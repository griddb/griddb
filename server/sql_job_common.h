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
#ifndef SQL_JOB_COMMON_H_
#define SQL_JOB_COMMON_H_

#include "sql_task_context.h"
#include "data_store_common.h"
#include "cluster_common.h"
#include "sql_type.h"

typedef int32_t TaskId;
typedef int32_t InputId;
typedef int32_t AssignNo;

class SQLProcessor;
class TaskInfo;
class PartitionTable;
class SQLPreparedPlan;
class JobManager;

enum TaskExecutionStatus {
	TASK_EXEC_PIPE,
	TASK_EXEC_FINISH,
	TASK_EXEC_NEXT,
	TASK_EXEC_IDLE
};

static const int32_t UNDEF_TASKID = -1;

enum ControlType {
	FW_CONTROL_DEPLOY,
	FW_CONTROL_DEPLOY_SUCCESS,
	FW_CONTROL_PIPE0,
	FW_CONTROL_PIPE,
	FW_CONTROL_PIPE_FINISH,
	FW_CONTROL_FINISH,
	FW_CONTROL_NEXT,
	FW_CONTROL_EXECUTE_SUCCESS,
	FW_CONTROL_CANCEL,
	FW_CONTROL_CLOSE,
	FW_CONTROL_ERROR,
	FW_CONTROL_HEARTBEAT,
	FW_CONTROL_UNDEF
};

struct JobId {

	JobId() :
			execId_(UNDEF_STATEMENTID),
			versionId_(0) {}

	JobId(ClientId &clientId, JobExecutionId execId) :
			clientId_(clientId),
			execId_(execId),
			versionId_(0) {}

	bool operator==(const JobId &id) const;
	
	bool isValid() {
		return (execId_ != UNDEF_STATEMENTID);
	}

	bool operator < (const JobId& id) const;

	ClientId clientId_;
	JobExecutionId execId_;
	uint8_t versionId_;

	void parse(
			util::StackAllocator &alloc,
			util::String &jobIdStr);

	void toString(
			util::StackAllocator &alloc,
			util::String &str,
			bool isClientIdOnly = false);

	void set(
			ClientId &clientId, 
			JobExecutionId execId,
			uint8_t versionId = 0) {

		clientId_ = clientId;
		execId_ = execId;
		versionId_ = versionId;
	}

	std::string dump(
			util::StackAllocator &alloc,
			bool isClientIdOnly = false);
};

class JobInfo {

public:
	class Job;
	typedef int32_t NodePosition;

	struct GsNodeInfo {

		GsNodeInfo(util::StackAllocator &alloc) :
				alloc_(alloc),
				address_(alloc),
				port_(0),
				nodeId_(0) {}

		util::StackAllocator &alloc_;
		util::String address_;
		uint16_t port_;
		NodeId nodeId_;

		UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
		UTIL_OBJECT_CODER_MEMBERS(address_, port_);
	};

	typedef util::Vector<GsNodeInfo*> GsNodeInfoList;
	typedef util::Vector<TaskInfo*> TaskInfoList;

	JobInfo(util::StackAllocator &alloc);

	~JobInfo();

	void setup(
			int64_t expiredTime,
			int64_t elapsedTime,
			int64_t currentClockTime,
			bool isSetJobTime,
			bool isCoordinator);

	TaskInfo *createTaskInfo(
			SQLPreparedPlan *plan, size_t pos);

	GsNodeInfo *createAssignInfo(
			PartitionTable *pt,
			NodeId nodeId,
			bool isLocal,
			JobManager *jobManager);

	util::StackAllocator &alloc_;
	GsNodeInfoList gsNodeInfoList_;
	bool coordinator_;
	int64_t queryTimeout_;
	int64_t expiredTime_;
	int64_t startTime_;
	int64_t startMonotonicTime_;
	bool isSetJobTime_;
	TaskInfoList taskInfoList_;
	TaskId resultTaskId_;
	PartitionId pId_;
	TaskOption option_;
	util::XArray<uint8_t> outBuffer_;
	util::XArrayOutStream<> outStream_;
	Job *job_;
	EventByteInStream *inStream_;
	NoSQLSyncContext *syncContext_;
	int32_t limitAssignNum_;
	bool isExplainAnalyze_;
	bool isRetry_;
	ContainerType containerType_;
	bool isSQL_;
	double storeMemoryAgingSwapRate_;

	util::TimeZone timezone_;
	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;

	UTIL_OBJECT_CODER_MEMBERS(
			gsNodeInfoList_,
			taskInfoList_,
			queryTimeout_,
			expiredTime_,
			startTime_,
			isSetJobTime_, 
			resultTaskId_,
			pId_,
			limitAssignNum_,
			isExplainAnalyze_,
			isRetry_,
			UTIL_OBJECT_CODER_ENUM(containerType_),
			isSQL_);
	};

class TaskInfo {

public:

	typedef util::Vector<uint32_t> ResourceList;

	TaskInfo(util::StackAllocator &alloc);

	void setSQLType(SQLType::Id sqlType);

	void setPlanInfo(
			SQLPreparedPlan *pPlan, size_t pos);

	util::StackAllocator &alloc_;
	TaskId taskId_;
	AssignNo loadBalance_;
	uint8_t type_;
	int32_t nodePos_;
	ResourceList inputList_;
	ResourceList outputList_;
	util::Vector< util::Vector<TupleList::TupleColumnType> > outColumnTypeList_;
	util::Vector< util::Vector<TupleList::TupleColumnType> > inputTypeList_;
	NodeId nodeId_;
	SQLType::Id sqlType_;
	bool isDml_;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(
			taskId_,
			loadBalance_,
			type_,
			nodePos_,
			inputList_,
			outputList_,
			inputTypeList_,
			UTIL_OBJECT_CODER_ENUM(sqlType_));
};

struct StatTaskProfilerInfo {

	StatTaskProfilerInfo(
			util::StackAllocator &alloc) :
					id_(0),
					inputId_(0),
					name_(alloc),
					status_(alloc),
					startTime_(alloc) {}

	int64_t id_;
	int64_t inputId_;
	util::String name_;
	util::String status_;
	util::String startTime_;

	UTIL_OBJECT_CODER_MEMBERS(
			id_, inputId_, name_, status_, startTime_);
};

struct StatJobProfilerInfo {

	StatJobProfilerInfo(
			util::StackAllocator &alloc,
			JobId &targetJobId);

	int64_t id_;
	util::String startTime_;
	util::String jobId_;
	util::Vector<StatTaskProfilerInfo> taskProfs_;

	UTIL_OBJECT_CODER_MEMBERS(
			id_, jobId_, startTime_, taskProfs_);
};

struct TaskProfilerInfo {

	TaskProfilerInfo() :
			taskId_(0),
			startTime_(0),
			endTime_(0),
			enableTimer_(false),
			completed_(false) {}

	int32_t taskId_;
	TaskProfiler profiler_;
	uint64_t startTime_;
	uint64_t endTime_;
	util::Stopwatch watch_;
	bool enableTimer_;
	bool completed_;

	void init(
			util::StackAllocator &alloc,
			int32_t taskId,
			int32_t size,			
			int32_t workerId,
			bool enableTimer = false);

	void incInputCount(int32_t inputId, size_t size);
	
	void start();
	
	void end();
	
	void complete();
	
	void setCustomProfile(SQLProcessor *processor);

	UTIL_OBJECT_CODER_MEMBERS(taskId_, profiler_);
};

struct JobProfilerInfo {
	
	JobProfilerInfo(util::StackAllocator &alloc) :
			alloc_(alloc),
			profilerList_(alloc) {}

	util::StackAllocator &alloc_;
	util::Vector<TaskProfilerInfo*> profilerList_;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(profilerList_);
};

typedef JobInfo::GsNodeInfo GsNodeInfo;

#endif
