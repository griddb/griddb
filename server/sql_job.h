/*
	Copyright (c) 2017 TOSHIBA Digital Solutions Corporation

	This program is free software: you can redistribute it and/or modify
	it under the terms of the GNU Affero General Public License as
	published by the Free Software Foundation, either version 3 of the
	License, or (at your option) any later version.

	This program is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU Affero General Public License for more details.

	You should have received a copy of the GNU Affero General Public License
	along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#ifndef SQL_JOB_H_
#define SQL_JOB_H_

#include "sql_resource_manager.h"
#include "cluster_common.h"
#include "event_engine.h"
#include "data_store_common.h"
#include "sql_type.h"
#include "sql_task_context.h"
#include "sql_job_common.h"
#include "sql_execution_common.h"

class SQLContext;
class Job;
class Task;
struct SQLJobRequestInfo;
class TaskInfo;
class SQLPreparedPlan;
class PartitionTable;
class JobManager;
struct TaskOption;
class SQLExecution;

#define TRACE_JOB_CONTROL_START(jobId, hostName, controlName) \
	GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK_DETAIL, \
			GS_TRACE_SQL_INTERNAL_DEBUG, \
			" " << jobId << " " << \
			" " << hostName << " " << \
			" TASK " << 0 << " START IN " << 0 << " " \
			<< controlName \
			<< " " << "PIPE");

#define TRACE_JOB_CONTROL_END(jobId, hostName, controlName) \
	GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK_DETAIL, \
			GS_TRACE_SQL_INTERNAL_DEBUG, \
			" " << jobId << " " << \
			" " << hostName << " " << \
			" TASK " << 0 << " END IN " << 0 << " " \
			<< controlName \
			<< " " << "PIPE");

#define TRACE_TASK_START(task, inputId, controlType) \
	GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK_DETAIL, \
			GS_TRACE_SQL_INTERNAL_DEBUG, \
			" " << task->job_->jobId_ << " " << \
			" " << task->job_->jobManager_->getHostName() << " " << \
			" TASK " << task->taskId_ << " START IN " << inputId << " " \
			<< getProcessorName(task->sqlType_) \
			<< " " << Job::getControlName(controlType));

#define TRACE_TASK_END(task, inputId, controlType) \
	GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK_DETAIL, \
			GS_TRACE_SQL_INTERNAL_DEBUG, \
			" " << task->job_->jobId_ << " " << \
			" " << task->job_->jobManager_->getHostName() << " " << \
			" TASK " << task->taskId_ << " END IN " << inputId << " " \
			<< getProcessorName(task->sqlType_) \
			<< " " << Job::getControlName(controlType));

#define TRACE_ADD_EVENT(str, jobId, taskId, inputId, controlType, pId) \
	GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK_DETAIL, \
			GS_TRACE_SQL_INTERNAL_DEBUG, \
			" " << str << \
			" " << jobId << " " << \
			" " << jobManager_->getHostName() << " " << \
			" TASK " << taskId << " EVENT IN " << inputId << " "  << \
			" " << Job::getControlName(controlType));

#define TRACE_ADD_EVENT2(str, jobId, taskId, inputId, controlType, count) \
	GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK_DETAIL, \
			GS_TRACE_SQL_INTERNAL_DEBUG, \
			" " << str << \
			" " << jobId << " " << \
			" " << jobManager_->getHostName() << " " << \
			" TASK " << taskId << " EVENT IN " << inputId << " "  << \
			" " << Job::getControlName(controlType) << " " << count);

#define TRACE_NEXT_EVENT(task, inputId, controlType) \
	GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK_DETAIL, \
			GS_TRACE_SQL_INTERNAL_DEBUG, \
			"CONT" << " " << task->job_->jobId_ << " " << \
			" " << task->job_->jobManager_->getHostName() << " " << \
			" TASK " << task->taskId_ << " EVENT IN " << inputId << " " \
			<< getProcessorName(task->sqlType_) \
			<< " " << Job::getControlName(controlType));

#define TRACE_CONTINUOUS_BLOCK(jobId, taskId, inputId, controlType) \
	GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK_DETAIL, \
			GS_TRACE_SQL_INTERNAL_DEBUG, \
			" " << jobId << " " << \
			" " << jobManager_->getHostName() << " " << \
			" TASK " << taskId << " EMPTY BLOCK IN " << inputId << " " \
			<< getProcessorName(task->sqlType_) \
			<< " " << Job::getControlName(controlType));

struct Dispatch {

	Dispatch() {}
	
	Dispatch(
			TaskId taskId,
			NodeId nodeId,
			InputId inputId,
			ServiceType type,
			AssignNo loadBalance) :
					taskId_(taskId),
					nodeId_(nodeId),
					inputId_(inputId),
					type_(type),
					loadBalance_(loadBalance) {}

	TaskId taskId_;
	NodeId nodeId_;
	InputId inputId_;
	ServiceType type_;
	AssignNo loadBalance_;
};

class TaskInterruptionHandler :
		public InterruptionChecker::CheckHandler {

public:

	TaskInterruptionHandler();

	virtual ~TaskInterruptionHandler();
	
	virtual bool operator()(
			InterruptionChecker &checker, int32_t flags);

	SQLContext *cxt_;
	EventMonotonicTime intervalMillis_;
};

class Job : public ResourceBase {

public:

	enum ExecutionStatus {
		FW_JOB_INIT,
		FW_JOB_DEPLOY,
		FW_JOB_EXECUTE,
		FW_JOB_COMPLETE
	};

	static const int32_t TASK_INPUT_STATUS_NORMAL = 0;
	static const int32_t TASK_INPUT_STATUS_PREPARE_FINISH = 1;
	static const int32_t TASK_INPUT_STATUS_FINISHED = 2;

	Job(SQLJobRequestInfo &jobRequestInfo);

	~Job();

	Task *getTask(TaskId taskId);

	void removeTask(TaskId taskId);

	void deploy(
			EventContext &ec,
			JobInfo *jobInfo,
			NodeId senderNodeId,
			int64_t waitInterval);

	void request(
			EventContext *ec,
			Event *ev,
			ControlType controlType,
			TaskId taskId,
			TaskId inputId,
			bool isEmpty,
			bool isComplete,
			TupleList::Block *resource,
			EventMonotonicTime emTime,
			int64_t waitTime = 0);

	void fetch(EventContext &ec, SQLExecution *execution);

	void cancel(EventContext &ec, bool clientCancel);

	void cancel(const Event::Source &sourcel);

	void handleError(EventContext &ec, std::exception *e);

	void checkNextEvent(
			EventContext &ec,
			ControlType controlType,
			TaskId taskId,
			TaskId inputId);


	typedef uint8_t StatementExecStatus;  

	void recieveNoSQLResponse(
			EventContext &ec,
			util::Exception &dest,
			StatementExecStatus status,
			int32_t pos);

	void getProfiler(
			util::StackAllocator &alloc,
			StatJobProfilerInfo &prof,
			int32_t mode);

	void recieveTaskBlock(
			EventContext &ec,
			TaskId taskId,InputId inputId,
			TupleList::Block *block,
			int64_t blockNo,
			ControlType controlType,
			bool isEmpty,
			bool isLocal);

	bool getTaskBlock(
			TaskId taskId,
			InputId inputId,
			Task *&task,
			TupleList::Block &block,
			bool &isExistBlock);

	void checkCancel(
			const char *str,
			bool partitionCheck,
			bool withNodeCheck = false);

	void setCancel() {
		cancelled_ = true;
	}

	PartitionId getPartitionId() {
		return connectedPId_;
	}

	void fowardRequest(
			EventContext &ec,
			ControlType controlType,
			Task *task,
			int outputId,
			bool isEmpty,
			bool isComplete,
			TupleList::Block *resource);

	void recvDeploySuccess(
			EventContext &ec,
			int64_t waitInterval = 0);

	void recvExecuteSuccess(
			EventContext &ec,
			EventByteInStream &in,
			bool isPending);

	void getProfiler(
			util::StackAllocator &alloc,
			TaskProfiler &profiler,
			size_t planId);

	TaskProfilerInfo *appendProfiler();


	bool isCoordinator() {
		return coordinator_;
	}

	bool isExplainAnalyze() {
		return isExplainAnalyze_;
	}

	JobId &getJobId() {
		return jobId_;
	}

	int64_t getStartTime() {
		return startTime_;
	}

	void setProfilerInfo(Task *task);


	const ResourceSet *getResourceSet() {
		return resourceSet_;
	}

	static const char *getControlName(ControlType type);

	static const char *getTaskExecStatusName(
		TaskExecutionStatus type);

	static const char *getProcessorName(SQLType::Id type);

	static bool isDQLProcessor(SQLType::Id type) {

		switch (type) {
			case SQLType::EXEC_RESULT:
			case SQLType::EXEC_INSERT:
			case SQLType::EXEC_UPDATE:
			case SQLType::EXEC_DELETE:
			case SQLType::EXEC_DDL:
				return false;
			default:
				return true;
		}
	}

	util::StackAllocator *assignStackAllocator();

	SQLVarSizeAllocator *assignLocalVarAllocator();

	void releaseStackAllocator(util::StackAllocator *alloc);

	void releaseLocalVarAllocator(
			SQLVarSizeAllocator *varAlloc);

	typedef util::AllocVector<Dispatch*> DispatchList;

	typedef util::AllocVector<NodeId> NodeIdList;

	typedef util::AllocVector<Task*> TaskList;

	typedef util::AllocVector<AssignNo> LimitAssignNoList;

	typedef util::AllocVector<uint8_t> ServiceList;

	AssignNo getAssignPId();

	bool isCancel() {
		return cancelled_;
	}

	util::StackAllocator &getAllocator() {
		return jobStackAlloc_;
	}

	SQLVariableSizeGlobalAllocator &getGlobalAllocator() {
		return globalVarAlloc_;
	}

	void setupContextTask(SQLContext *cxt);

	bool isSQL() {
			return isSQL_;
	}

private:

	void checkAsyncPartitionStatus();

	void checkDDLPartitionStatus();

	void ackDDLStatus(int32_t pos);

	void getFetchContext(SQLFetchContext &cxt);

	void setProfiler(
			JobProfilerInfo &profs,
			bool isRemote = false);

	void encodeProfiler(EventByteOutStream &out);

	util::Mutex &getLock() {
		return mutex_;
	}

	bool isDDL() {
		return (baseTaskId_ != UNDEF_TASKID
				&& taskList_.size() > static_cast<size_t>(baseTaskId_));
	}

	void executeJobStart(EventContext *ec);

	void executeTask(
			EventContext *ec,
			Event *ev,
			ControlType controlType,
			TaskId taskId,
			TaskId inputId,
			bool isEmpty,
			bool isComplete,
			TupleList::Block *resource,
			EventMonotonicTime emTime);

	void decodeOptional(EventByteInStream &in);

	bool isImmediateReply(ServiceType serviceType, uint32_t workerId);

	void sendJobInfo(EventContext &ec, JobInfo *jobInfo);

	NodeId resolveNodeId(JobInfo::GsNodeInfo *nodeInfo);

	void setLimitedAssignNoList();

	bool processNextEvent(
			Task *task,
			TupleList::Block *block,
			int64_t blockNo,
			size_t inputId,
			ControlType controlType,
			bool isEmpty,
			bool isLocal);

	void setupTaskInfo(
			Task *Task,
			EventContext &ec,
			Event &ev,
			TaskId inputId);

	bool pipe(
			Task *task,
			TaskId inputId,
			ControlType controlType,
			TupleList::Block *block);

	bool pipeOrFinish(
			Task *task,
			TaskId inputId,
			ControlType controlType,
			TupleList::Block *block);

	bool executeContinuous(
			EventContext &ec,
			Event &ev,
			Task *task,
			TaskId inputId,
			ControlType controlType,
			bool remaining,
			bool isEmpty,
			bool isComplete,
			EventMonotonicTime emTime);

	void doComplete(
			EventContext &ec, Task *task);

	void sendJobEvent(
			EventContext &ec,
			NodeId nodeId,
			ControlType controlType, 
			JobInfo *jobInfo,
			std::exception *e,
			TaskId taskId,
			InputId inputId,
			AssignNo loadBalance,
			ServiceType serviceType,
			bool isEmpty,
			bool isComplete,
			TupleList::Block *resource);

	void sendBlock(
			EventContext &ec,
			NodeId nodeId,
			ControlType controlType,
			TaskId taskId,
			InputId inputId,
			AssignNo loadBalance,
			bool isEmpty,
			bool isComplete,
			TupleList::Block *resource,
			int64_t blockNo);

	Task *createTask(
			EventContext &ec,
			TaskId taskId,
			JobInfo *jobInfo,
			TaskInfo *taskInfo);

	bool checkStart();

	void encodeProcessor(
			util::StackAllocator &alloc,
			JobInfo *jobInfo);

	void encodeOptional(
			util::ByteStream<util::XArrayOutStream<> > &out,
			JobInfo *jobInfo);

	util::Mutex mutex_;
	
	const ResourceSet *resourceSet_;

	JobManager *jobManager_;

	SQLVariableSizeGlobalAllocator &globalVarAlloc_;

	util::StackAllocator &jobStackAlloc_;

	JobId jobId_;

	util::StackAllocator::Scope *scope_;

	DispatchList noCondition_;

	NodeIdList nodeList_;

	TaskList taskList_;

	bool coordinator_;

	bool isSQL_;

	NodeId coordinatorNodeId_;

	util::Atomic<bool> cancelled_;
	ExecutionStatus status_;

	int64_t queryTimeout_;

	int64_t expiredTime_;

	int64_t startTime_;

	int64_t startMonotonicTime_;

	bool isSetJobTime_;

	int32_t selfTaskNum_;

	double storeMemoryAgingSwapRate_;

	util::TimeZone timezone_;

	util::Atomic<int32_t> deployCompletedCount_;

	util::Atomic<int32_t> executeCompletedCount_;

	util::Atomic<int32_t> taskCompletedCount_;

	TaskId resultTaskId_;

	PartitionId connectedPId_;

	NoSQLSyncContext *syncContext_;

	TaskId baseTaskId_;

	LimitAssignNoList limitedAssignNoList_;

	int32_t limitAssignNum_;

	int32_t currentAssignNo_;

	JobProfilerInfo profilerInfos_;

	bool isExplainAnalyze_;

	NodeIdList dispatchNodeIdList_;

	ServiceList dispatchServiceList_;

};

class Task {

	friend class Job;

public:

	Task(Job *job, TaskInfo *taskInfo);

	~Task();

	void createProcessor(
			util::StackAllocator &alloc,
			JobInfo *jobInfo,
			TaskId taskId,
			TaskInfo *taskInfo);

	std::string dump();

	bool isCompleted() {
		return completed_;
	}

	void setCompleted(bool completed) {
		completed_ = completed;
	}

	bool isImmediated() {
		return immediated_;
	}

	JobId &getJobId() {
		return job_->getJobId();
	}

	bool isResultCompleted() {
		return resultComplete_;
	}

	void setResultCompleted() {
		resultComplete_ = true;
	}

	ServiceType getServiceType() {
		return static_cast<ServiceType>(type_);
	}

	void startProcessor(TaskExecutionStatus status) {
		status_ = status;
		profilerInfo_->start();
	}

	void endProcessor() {
		profilerInfo_->end();
		status_ =  TASK_EXEC_IDLE;
		counter_++;
	}

	Job *getJob() {
		return job_;
	}

	TaskId getTaskId() {
		return taskId_;
	}

	SQLType::Id getSQLType() {
		return sqlType_;
	}

	void appendBlock(
			InputId inputId, TupleList::Block &block);
	
	bool getBlock(InputId inputId, TupleList::Block &block);

private:

	typedef util::AllocVector<Dispatch*> DispatchList;

	typedef std::vector<DispatchList, util::StdAllocator<
			DispatchList, SQLVariableSizeGlobalAllocator> > DispatchListArray;

	typedef util::AllocVector<TupleList*> TupleListArray;

	typedef util::AllocVector<TupleList::BlockReader*> BlockReaderArray;

	typedef util::AllocVector<int64_t> BlockCounterArray;

	typedef util::AllocVector<uint8_t> InputStatusList;

	void setupContextTask();
	void setupProfiler(TaskInfo *taskInfo);

	Job *job_;
	util::StackAllocator &jobStackAlloc_;
	util::StackAllocator *processorStackAlloc_;
	SQLVarSizeAllocator *processorVarAlloc_;
	SQLVariableSizeGlobalAllocator &globalVarAlloc_;
	DispatchListArray outputDispatchList_;

	bool completed_;
	bool resultComplete_;
	bool immediated_;
	int64_t counter_;
	TaskExecutionStatus status_;
	int64_t sendBlockCounter_;

	TupleListArray tempTupleList_;
	BlockReaderArray blockReaderList_;
	BlockCounterArray recvBlockCounterList_;
	InputStatusList inputStatusList_;
	InputId inputNo_;
	int64_t startTime_;
	TaskId taskId_;
	NodeId nodeId_;
	SQLContext *cxt_;
	uint8_t type_;
	SQLType::Id sqlType_;
	AssignNo loadBalance_;
	bool isDml_;
	SQLProcessor *processor_;
	TupleList::Group group_;
	TaskProfilerInfo *profilerInfo_;
	util::StackAllocator::Scope *scope_;

	TaskInterruptionHandler interruptionHandler_;
};


#endif
