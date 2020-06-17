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
/*!
	@file
	@brief Definition of job manager
*/
#ifndef SQL_JOB_MANAGER_H_
#define SQL_JOB_MANAGER_H_

#include "sql_type.h"
#include "sql_common.h"
#include "sql_tuple.h"
#include "cluster_event_type.h"
#include "cluster_common.h"
#include "transaction_context.h"
#include "data_store_common.h"
#include "sql_resource_manager.h"
#include "sql_task_context.h"
#include "sql_job_common.h"

class SQLProcessor;
class PartitionTable;
class ClusterService;
class SQLExecutionManager;
class TransactionService;
class SQLService;
class DataStore;
class JobManager;
class TaskContext;
class SQLContext;
class SQLExecution;
class SQLPreparedPlan;
struct NoSQLSyncContext;
struct SQLProcessorConfig;
class ResourceSet;
class ResourceSetReceiver;

struct StatJobProfilerInfo;
struct StatJobProfiler;
class Job;

#include "sql_execution_common.h"

typedef int32_t TaskId;


static const uint8_t UNDEF_JOB_VERSIONID = UINT8_MAX;
static const uint8_t MAX_JOB_VERSIONID = UINT8_MAX;

struct CancelOption;

struct StatJobIdListInfo {
	StatJobIdListInfo(util::StackAllocator &alloc) :
			jobId_(alloc),
			startTime_(alloc) {}

	util::String jobId_;
	util::String startTime_;
	
	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(jobId_, startTime_);
};

struct StatJobIdList {
	StatJobIdList(util::StackAllocator &alloc) :
			alloc_(alloc),
			infoList_(alloc) {}

	util::StackAllocator &alloc_;
	util::Vector<StatJobIdListInfo> infoList_;
	
	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(infoList_);
};

struct SQLJobRequestInfo {

	SQLJobRequestInfo(
			JobId &jobId,
			JobInfo *jobInfo,
			const ResourceSet &resourceSet) :
					jobId_(jobId),
					jobInfo_(jobInfo),
					resourceSet_(resourceSet) {}

	JobId &jobId_;
	JobInfo *jobInfo_;
	const ResourceSet &resourceSet_;
};

class JobManager {

	typedef ResourceManager<
			JobId,
			Job,
			SQLJobRequestInfo,
			SQLVariableSizeGlobalAllocator> SQLJobResourceManager;

public:

	static const int32_t
			DEFAULT_SERVICE = static_cast<int32_t>(SQL_SERVICE);

	static const int32_t
			DEFAULT_STACK_ALLOCATOR_POOL_MEMORY_LIMIT = 128;

	static const int64_t
			DEFAULT_RESOURCE_CHECK_TIME = 60 * 5 * 1000;
	
	static const int64_t
			DEFAULT_TASK_INTERRUPTION_INTERVAL = 100;
	
	static const int32_t
			JOB_TIMEOUT_SLACK_TIME = 10 * 1000;

	typedef uint32_t ResourceId;
	static const int32_t UNIT_CHECK_COUNT = 127;

	SQLJobResourceManager *getResourceManager() {
		return resourceManager_;
	}

	typedef util::AllocMap<JobId, Job*> JobMap;
	typedef JobMap::iterator JobMapItr;

	JobManager(
			SQLVariableSizeGlobalAllocator &globalVarAlloc,
			LocalTempStore &store,
			const ConfigTable &config);

	~JobManager();

	void initialize(
			const ResourceSet &resourceSet);

	void beginJob(
			EventContext &ec,
			JobId &jobId,
			JobInfo *jobInfo,
			int64_t waitInterval);

	void removeExecution(
			EventContext *ec,
			JobId &jobId,
			SQLExecution *execution,
			bool withCheck);

	void cancel(
			EventContext &ec,
			JobId &jobId,
			bool clientCancel);

	void cancel(
			const Event::Source &eventSource,
			JobId &jobId,
			CancelOption &option);

	void cancelAll(
			const Event::Source &eventSource,
			util::Vector<JobId> &jobIdList,
			CancelOption &option);

	void getCurrentJobList(util::Vector<JobId> &jobIdList);

	void checkAlive(
			EventContext &ec,
			JobId &jobId);

	void checkTimeout(EventContext &ec);

	void getProfiler(
			util::StackAllocator &alloc,
			StatJobProfiler &jobProfiler,
			int32_t mode);

	void getJobIdList(
			util::StackAllocator &alloc,
			StatJobIdList &infoList);

	void executeStatementError(
			EventContext &ec,
			std::exception *e,
			JobId &jobId);

	void executeStatementSuccess(
			EventContext &ec,
			Job *job,
			bool isExplainAnalyze,
			bool withResponse);

	EventEngine *getEE(ServiceType type) {
		return eeList_[static_cast<size_t>(type)];
	}

	LocalTempStore &getStore() {
		return *resourceSet_->getTempolaryStore();
	}

	SQLVariableSizeGlobalAllocator &getGlobalAllocator() {
		return globalVarAlloc_;
	}

	EventEngine *getDefaultEE() {
		return eeList_[DEFAULT_SERVICE];
	}

	void remove(JobId &jobId);

	std::string getHostName() {
		return hostName_;
	}

	std::string getHostAddress() {
		return hostAddress_;
	}

	uint16_t getHostPort() {
		return hostPort_;
	}

	int32_t getAssignPId();

	PartitionGroupId getSQLPgId(PartitionId pId);

	bool checkExecutableTask(
			EventContext &ec,
			Event &ev,
			int64_t startTime,
			int64_t limitInterval,
			bool pending);

	void sendEvent(
			Event &ev,
			ServiceType type,
			NodeId nodeId,
			int64_t waitInterval = 0);

	void addEvent(Event &ev, ServiceType type);

	const SQLProcessorConfig* getProcessorConfig() const;
	
	EventType getEventType(ControlType type);

	void rethrowJobException(
			util::StackAllocator &alloc,
			std::exception &e,
			JobId &jobId,
			ControlType controlType,
			TaskId taskId = UNDEF_TASKID,
			const char *taskName = NULL);

private:

	void getHostInfo();

	util::Mutex mutex_;

	SQLVariableSizeGlobalAllocator &globalVarAlloc_;
	const ResourceSet *resourceSet_;
	util::Atomic<int32_t> currentCnt_;
	SQLJobResourceManager *resourceManager_;
	int32_t txnConcurrency_;
	int32_t sqlConcurrency_;
	PartitionGroupConfig txnPgConfig_;
	PartitionGroupConfig sqlPgConfig_;
	std::vector<EventEngine *> eeList_;
	std::vector<PartitionId> sqlStartPIdList_;
	std::string hostName_;
	std::string hostAddress_;
	uint16_t hostPort_;
};

class SQLJobHandler : public EventHandler, public ResourceSetReceiver {

public:

	typedef uint8_t StatementExecStatus;

	SQLJobHandler() {};

	virtual ~SQLJobHandler() {};

	void initialize(const ResourceSet &resourceSet);
	
	ServiceType getServiceType() {
		return type_;
	}

	static void decodeRequestInfo(
			EventByteInStream &in,
			JobId &jobId,
			ControlType &controlType,
			int32_t &sqlVersionId);

	static void decodeTaskInfo(
			EventByteInStream &in,
			TaskId &taskId,
			InputId &inputId,
			bool &isEmpty,
			bool &isComplete,
			int64_t &blockNo);

	static void encodeRequestInfo(
			EventByteOutStream &out,
			JobId &jobId,
			ControlType controlType);

	static void encodeTaskInfo(
			EventByteOutStream &out,
			TaskId taskId,
			TaskId inputId,
			bool isEmpty,
			bool isComplete);
	
	static void encodeErrorInfo(
			EventByteOutStream &out,
			std::exception &e,
			StatementExecStatus status);

	void handleError(
			EventContext &ec,
			std::exception &e,
			JobId &jobId);

protected:

	const ResourceSet *resourceSet_;
	ServiceType type_;
};

class ExecuteJobHandler : public SQLJobHandler {

public:
	void operator()(EventContext &ec, Event &ev);

	void setServiceType(ServiceType serviceType) {
		type_ = serviceType;
	}

private:
	
	void executeRequest(
			EventContext &ec,
			Event &ev,
			EventByteInStream &in,
			JobId &jobId,
			ControlType controlType);
	
	void executeDeploy(
			EventContext &ec,
			Event &ev,
			EventByteInStream &in,
			JobId &jobId,
			PartitionId &recvPartitionId);

	void executeError(
			EventContext &ec,
			EventByteInStream &in,
			JobId &jobId);

	void executeCancel(
			EventContext &ec,
			JobId &jobId);

	void executeRecvAck(
			EventContext &ec,
			Event &ev,
			EventByteInStream &in,
			JobId &jobId);

	void request(
			Job *job,
			EventContext *ec,
			Event *ev,
			ControlType controlType,
			TaskId taskId,
			TaskId inputId,
			bool isEmpty,
			bool isComplete,
			EventMonotonicTime emTime,
			int64_t waitTime = 0);

};

class ControlJobHandler : public SQLJobHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

class SQLResourceCheckHandler : public SQLJobHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

class DispatchJobHandler : public SQLJobHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

std::ostream& operator<<(
		std::ostream &stream, const JobId &id);

struct CancelOption {
	
	CancelOption() : startTime_(0) {};
	
	CancelOption(util::String &startTimeStr);
	
	int64_t startTime_;
};

struct TaskProf {

	TaskProf() : id_(0), inputId_(0) {}

	int64_t id_;
	int64_t inputId_;
	std::string name_;
	std::string status_;
	std::string startTime_;
	UTIL_OBJECT_CODER_MEMBERS(
			id_, inputId_, name_, status_, startTime_);
};

struct StatJobProfiler {

	StatJobProfiler(util::StackAllocator &alloc) :
			jobProfs_(alloc),
			currentTime_(alloc) {}
	
	util::Vector<StatJobProfilerInfo> jobProfs_;
	util::String currentTime_;
	
	UTIL_OBJECT_CODER_MEMBERS(currentTime_, jobProfs_);
};

typedef ResourceLatch<
		JobId,
		Job,
		SQLJobRequestInfo,
		ResourceManager<
				JobId,
				Job,
				SQLJobRequestInfo,
				SQLVariableSizeGlobalAllocator> > JobLatch;

#endif
