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
class SQLProcessor;
struct ManagerSet;
class PartitionTable;
class ClusterService;
class SQLExecutionManager;
class TransactionService;
class SQLService;
class DataStoreV4;
class JobManager;
class TaskContext;
class SQLContext;
class SQLExecution;
class SQLPreparedPlan;
struct NoSQLSyncContext;
struct SQLProcessorConfig;
struct SQLFetchContext;
struct PartitionListStats;

struct StatJobProfilerInfo;
struct StatJobProfiler;
struct SQLProfilerInfo;
struct DatabaseStats;
typedef int32_t TaskId;


static const uint8_t UNDEF_JOB_VERSIONID = UINT8_MAX;
static const uint8_t MAX_JOB_VERSIONID = UINT8_MAX;

struct CancelOption;

struct ChunkBufferStatsSQL {
	enum Param {
		BUFFER_STATS_SQL_HIT_COUNT,
		BUFFER_STATS_SQL_MISS_HIT_COUNT,
		BUFFER_STATS_SQL_SWAP_READ_SIZE,
		BUFFER_STATS_SQL_SWAP_WRITE_SIZE,
		BUFFER_STATS_SQL_END
	};
	ChunkBufferStatsSQL() : table_(NULL) {}
	ChunkBufferStatsSQL(const ChunkBufferStatsSQL& stats) : table_(NULL) {
		table_.assign(stats.table_);
	}

	typedef LocalStatTable<Param, BUFFER_STATS_SQL_END> Table;
	Table table_;
};

class ChunkBufferStatsSQLScope;

struct StatTaskProfilerInfo {
	StatTaskProfilerInfo(util::StackAllocator& alloc) :
		id_(0),
		inputId_(0),
		name_(alloc),
		status_(alloc),
		counter_(0),
		sendPendingCount_(0),
		dispatchCount_(0),
		worker_(0),
		actualTime_(0),
		leadTime_(0),
		dispatchTime_(0),
		startTime_(alloc),
		sendEventCount_(0),
		sendEventSize_(0),
		allocateMemory_(0)
		,
		inputSwapRead_(0),
		inputSwapWrite_(0),
		inputActiveBlockCount_(0),
		swapRead_(0),
		swapWrite_(0),
		activeBlockCount_(0)
		,
		scanBufferHitCount_(0),
		scanBufferMissHitCount_(0),
		scanBufferSwapReadSize_(0),
		scanBufferSwapWriteSize_(0)
	{}

	int64_t getMemoryUse() const;
	int64_t getSqlStoreUse() const;
	int64_t getDataStoreAccess(SQLExecutionManager &executionManager) const;

	int64_t id_;
	int64_t inputId_;
	util::String name_;
	util::String status_;
	int64_t counter_;
	int64_t sendPendingCount_;
	int64_t dispatchCount_;
	int64_t worker_;
	int64_t actualTime_;
	int64_t leadTime_;
	int64_t dispatchTime_;
	util::String startTime_;
	int64_t sendEventCount_;
	int64_t sendEventSize_;
	int64_t allocateMemory_;
	int64_t inputSwapRead_;
	int64_t inputSwapWrite_;
	int64_t inputActiveBlockCount_;
	int64_t swapRead_;
	int64_t swapWrite_;
	int64_t activeBlockCount_;
	int64_t scanBufferHitCount_;
	int64_t scanBufferMissHitCount_;
	int64_t scanBufferSwapReadSize_;
	int64_t scanBufferSwapWriteSize_;

	UTIL_OBJECT_CODER_MEMBERS(
		id_,
		inputId_,
		name_,
		status_,
		counter_,
		sendPendingCount_,
		dispatchCount_,
		worker_,
		actualTime_,
		UTIL_OBJECT_CODER_OPTIONAL(dispatchTime_, 0),
		startTime_,
		UTIL_OBJECT_CODER_OPTIONAL(allocateMemory_, 0),
		UTIL_OBJECT_CODER_OPTIONAL(inputSwapRead_, 0),
		UTIL_OBJECT_CODER_OPTIONAL(inputSwapWrite_, 0),
		UTIL_OBJECT_CODER_OPTIONAL(inputActiveBlockCount_, 0),
		UTIL_OBJECT_CODER_OPTIONAL(swapRead_, 0),
		UTIL_OBJECT_CODER_OPTIONAL(swapWrite_, 0),
		UTIL_OBJECT_CODER_OPTIONAL(activeBlockCount_, 0),
		UTIL_OBJECT_CODER_OPTIONAL(sendEventCount_, 0),
		UTIL_OBJECT_CODER_OPTIONAL(sendEventSize_, 0),
		UTIL_OBJECT_CODER_OPTIONAL(scanBufferHitCount_, 0),
		UTIL_OBJECT_CODER_OPTIONAL(scanBufferMissHitCount_, 0),
		UTIL_OBJECT_CODER_OPTIONAL(scanBufferSwapReadSize_, 0),
		UTIL_OBJECT_CODER_OPTIONAL(scanBufferSwapWriteSize_, 0)
	);
};

struct StatJobIdListInfo {
	StatJobIdListInfo(util::StackAllocator& alloc) : jobId_(alloc), startTime_(alloc) {}
	util::String jobId_;
	util::String startTime_;
	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(jobId_, startTime_);
};

struct StatJobIdList {
	StatJobIdList(util::StackAllocator& alloc) : alloc_(alloc), infoList_(alloc) {}
	util::StackAllocator& alloc_;
	util::Vector<StatJobIdListInfo> infoList_;
	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(infoList_);
};

struct TaskProfilerInfo {
	TaskProfilerInfo() : taskId_(0), startTime_(0), endTime_(0),
		enableTimer_(false), completed_(false) {}

	int32_t taskId_;
	TaskProfiler profiler_;
	uint64_t startTime_;
	uint64_t endTime_;
	util::Stopwatch watch_;
	bool enableTimer_;
	bool completed_;

	void init(util::StackAllocator& alloc, size_t size, bool enableTimer = false);

	void incInputCount(int32_t inputId, int32_t size);
	void start();
	void end();
	int64_t lap();
	void complete();
	void setCustomProfile(SQLProcessor* processor);

	UTIL_OBJECT_CODER_MEMBERS(taskId_, profiler_);
};

struct JobProfilerInfo {
	JobProfilerInfo(util::StackAllocator& alloc) : alloc_(alloc), profilerList_(alloc) {}
	util::StackAllocator& alloc_;
	util::Vector<TaskProfilerInfo*> profilerList_;
	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(profilerList_);
};

struct TaskOption {
public:
	typedef EventByteInStream ByteInStream;
	typedef EventByteOutStream ByteOutStream;

	explicit TaskOption(util::StackAllocator& alloc);

	const SQLPreparedPlan* plan_;
	uint32_t planNodeId_;
	util::ObjectInStream<ByteInStream>* byteInStream_;
	util::AbstractObjectInStream* inStream_;
	EventByteInStream* baseStream_;
	const picojson::value* jsonValue_;
};

typedef StatementId ExecId;
typedef int32_t InputId;
typedef int32_t AssignNo;

class JobManager {
public:

	static const int32_t DEFAULT_SERVICE = static_cast<int32_t>(SQL_SERVICE);
	static const int32_t DEFAULT_ALLOCATOR_BLOCK_SIZE = 20;
	static const int32_t DEFAULT_STACK_ALLOCATOR_POOL_MEMORY_LIMIT = 128;
	static const int64_t DEFAULT_RESOURCE_CHECK_TIME = 60 * 5 * 1000;
	static const int64_t DEFAULT_TASK_INTERRUPTION_INTERVAL = 100;

	static const int32_t JOB_TIMEOUT_SLACK_TIME = 10 * 1000;
	typedef uint32_t ResourceId;
	static const int32_t UNIT_CHECK_COUNT = 127;
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
		FW_CONTROL_CHECK_CONTAINER,
		FW_CONTROL_UNDEF
	};

	enum TaskExecutionStatus {
		TASK_EXEC_PIPE,
		TASK_EXEC_FINISH,
		TASK_EXEC_NEXT,
		TASK_EXEC_IDLE
	};

	/*!
		@brief ジョブID
	*/
	struct JobId {

		JobId() : execId_(UNDEF_STATEMENTID), versionId_(0) {}
		JobId(ClientId& clientId, ExecId execId) :
			clientId_(clientId), execId_(execId), versionId_(0) {}

		JobId& operator=(const JobId& another) {
			if (this == &another) {
				return *this;
			}
			clientId_ = another.clientId_;
			execId_ = another.execId_;
			versionId_ = another.versionId_;
			return *this;
		}

		bool operator==(const JobId& id) const {
			if (execId_ == id.execId_
				&& clientId_.sessionId_ == id.clientId_.sessionId_
				&& versionId_ == id.versionId_
				&& !memcmp(clientId_.uuid_,
					id.clientId_.uuid_, TXN_CLIENT_UUID_BYTE_SIZE)) {
				return true;
			}
			else {
				return false;
			}
		}

		bool isValid() {
			return (execId_ != UNDEF_STATEMENTID);
		}

		bool operator < (const JobId& id) const {
			if (execId_ < id.execId_) {
				return true;
			}
			else if (execId_ > id.execId_) {
				return false;
			}
			else {
				if (versionId_ < id.versionId_) {
					return true;
				}
				else if (versionId_ > id.versionId_) {
					return false;
				}
				else {
					if (clientId_.sessionId_ < id.clientId_.sessionId_) {
						return true;
					}
					else if (clientId_.sessionId_ > id.clientId_.sessionId_) {
						return false;
					}
					else {
						int32_t ret = memcmp(clientId_.uuid_,
							id.clientId_.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
						if (ret < 0) {
							return true;
						}
						else {
							return false;
						}
					}
				}
			}
		}

		ClientId clientId_;
		ExecId execId_;
		uint8_t versionId_;
		void parse(util::StackAllocator& alloc, util::String& jobIdStr, bool isJobId = true);
		void toString(util::StackAllocator& alloc, util::String& str, bool isClientIdOnly = false);
		void set(ClientId& clientId, ExecId execId, uint8_t versionId = 0) {
			clientId_ = clientId;
			execId_ = execId;
			versionId_ = versionId;
		}
		std::string dump(util::StackAllocator& alloc, bool isClientIdOnly = false);
	};

	class TaskInfo {
	public:
		typedef util::Vector<ResourceId> ResourceList;

		TaskInfo(util::StackAllocator& alloc) :
			alloc_(alloc), taskId_(0), loadBalance_(0),
			type_(static_cast<uint8_t>(SQL_SERVICE)),
			nodePos_(0), inputList_(alloc), outputList_(alloc),
			outColumnTypeList_(alloc),
			inputTypeList_(alloc), nodeId_(0),
			sqlType_(SQLType::START_EXEC), isDml_(false) {}

		void setSQLType(SQLType::Id sqlType) {
			switch (sqlType) {
			case SQLType::EXEC_INSERT:
			case SQLType::EXEC_UPDATE:
			case SQLType::EXEC_DELETE:
				isDml_ = true;
				break;
			default:
				break;
			}
			sqlType_ = sqlType;
		}

		void setPlanInfo(SQLPreparedPlan* pPlan, size_t pos);

		util::StackAllocator& alloc_;
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
		UTIL_OBJECT_CODER_MEMBERS(taskId_, loadBalance_,
			type_, nodePos_, inputList_, outputList_,
			inputTypeList_, UTIL_OBJECT_CODER_ENUM(sqlType_));
	};

	class JobInfo {
	public:
		class Job;
		typedef int32_t NodePosition;
		struct GsNodeInfo {
			GsNodeInfo(util::StackAllocator& alloc) : alloc_(alloc),
				address_(alloc), port_(0), nodeId_(0) {}

			util::StackAllocator& alloc_;
			util::String address_;
			uint16_t port_;
			NodeId nodeId_;

			UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
			UTIL_OBJECT_CODER_MEMBERS(address_, port_);
		};

		typedef util::Vector<GsNodeInfo*> GsNodeInfoList;
		typedef util::Vector<TaskInfo*> TaskInfoList;

		JobInfo(util::StackAllocator& alloc) :
			eventStackAlloc_(alloc),
			gsNodeInfoList_(alloc),
			coordinator_(true),
			queryTimeout_(INT64_MAX),
			expiredTime_(INT64_MAX),
			startTime_(0),
			startMonotonicTime_(INT64_MAX),
			isSetJobTime_(false),
			taskInfoList_(alloc),
			resultTaskId_(0), pId_(0),
			option_(eventStackAlloc_),
			outBuffer_(eventStackAlloc_),
			outStream_(outBuffer_),
			job_(NULL),
			inStream_(NULL),
			syncContext_(NULL),
			limitAssignNum_(0),
			isExplainAnalyze_(false),
			isRetry_(false),
			containerType_(COLLECTION_CONTAINER),
			isSQL_(true),
			coordinatorNodeId_(0),
			storeMemoryAgingSwapRate_(TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE),
			timezone_(util::TimeZone()),
			dbId_(UNDEF_DBID),
			dbName_(NULL),
			appName_(NULL),
			userName_(NULL),
			sql_(NULL),
			isAdministrator_(false),
			delayTime_(0)
		{}

		~JobInfo() {
			for (size_t pos = 0; pos < gsNodeInfoList_.size(); pos++) {
				ALLOC_DELETE(eventStackAlloc_, gsNodeInfoList_[pos]);
			}
			for (size_t pos = 0; pos < taskInfoList_.size(); pos++) {
				ALLOC_DELETE(eventStackAlloc_, taskInfoList_[pos]);
			}
		}

		void setup(int64_t expiredTime, int64_t elapsedTime,
			int64_t currentClockTime, bool isSetJobTime, bool isCoordinator);

		TaskInfo* createTaskInfo(SQLPreparedPlan* plan, size_t pos);

		GsNodeInfo* createAssignInfo(PartitionTable* pt, NodeId nodeId,
			bool isLocal, JobManager* jobManager);

		util::StackAllocator& eventStackAlloc_;
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
		Job* job_;
		EventByteInStream* inStream_;
		NoSQLSyncContext* syncContext_;
		int32_t limitAssignNum_;
		bool isExplainAnalyze_;
		bool isRetry_;
		ContainerType containerType_;
		bool isSQL_;
		NodeId coordinatorNodeId_;
		double storeMemoryAgingSwapRate_;
		util::TimeZone timezone_;
		DatabaseId dbId_;
		const char* dbName_;
		const char* appName_;
		const char* userName_;
		const char* sql_;

		bool isAdministrator_;
		uint32_t delayTime_;

		UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;

		UTIL_OBJECT_CODER_MEMBERS(gsNodeInfoList_, taskInfoList_,
			queryTimeout_, expiredTime_, startTime_, isSetJobTime_,
			resultTaskId_, pId_, limitAssignNum_,
			isExplainAnalyze_, isRetry_,
			UTIL_OBJECT_CODER_ENUM(containerType_), isSQL_);
	};

	/*!
		@brief ジョブ
*/
	class Job {

		friend class JobThreadTest;
	public:
		enum ExecutionStatus {
			FW_JOB_INIT,
			FW_JOB_DEPLOY,
			FW_JOB_EXECUTE,
			FW_JOB_COMPLETE
		};

		struct Dispatch {
			Dispatch() {}
			Dispatch(TaskId taskId, NodeId nodeId, InputId inputId,
				ServiceType type, AssignNo loadBalance) :
				taskId_(taskId), nodeId_(nodeId), inputId_(inputId),
				type_(type), loadBalance_(loadBalance) {}
			TaskId taskId_;
			NodeId nodeId_;
			InputId inputId_;
			ServiceType type_;
			AssignNo loadBalance_;
		};

		static const int32_t OPTION_AGING_SWAP_RATE = 0;
		static const int32_t OPTION_TIME_ZONE = 1;
		static const int32_t OPTION_DB_ID = 2;
		static const int32_t OPTION_DB_NAME = 3;
		static const int32_t OPTION_ADMIN = 4;
		static const int32_t OPTION_APPLICATION_NAME = 5;
		static const int32_t OPTION_USER_NAME = 6;
		static const int32_t OPTION_SQL = 7;
		static const int32_t OPTION_DELAY_TIME = 8;
		static const int32_t OPTION_MAX = 9;

		static const int32_t TASK_INPUT_STATUS_NORMAL = 0;
		static const int32_t TASK_INPUT_STATUS_PREPARE_FINISH = 1;
		static const int32_t TASK_INPUT_STATUS_FINISHED = 2;

		class TaskInterruptionHandler :
			public InterruptionChecker::CheckHandler {
		public:
			TaskInterruptionHandler();
			virtual ~TaskInterruptionHandler();
			virtual bool operator()(InterruptionChecker& checker, int32_t flags);

			SQLContext* cxt_;
			EventMonotonicTime intervalMillis_;
		};

		class Task {

			friend class Job;
			friend class TaskContext;

		public:

			Task(Job* job, TaskInfo* taskInfo);
			~Task();

			template<typename T> T* getProcessor() {
				return static_cast<T*>(processor_);
			}
			void createProcessor(util::StackAllocator& alloc, JobInfo* jobInfo,
				TaskId taskId, TaskInfo* taskInfo);
			static void setupContextBase(
					SQLContext* cxt, JobManager* jobManager,
					util::AllocatorLimitter *limitter);
			std::string  dump();

			bool getProfiler(util::StackAllocator& alloc, StatJobProfilerInfo& jobProf,
				StatTaskProfilerInfo& taskProf, int32_t mode);

			bool isCompleted() {
				return completed_;
			}

			void setCompleted(bool completed) {
				completed_ = completed;
			}

			bool isImmediate() {
				return immediate_;
			}

			JobId& getJobId() {
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

			void setStoreProfiler(
					uint32_t unit, int64_t bufferHitCount,
					int64_t bufferMissHitCount, int64_t bufferSwapReadSize,
					int64_t bufferSwapWriteSize) {
				UNUSED_VARIABLE(unit);

				scanBufferHitCount_ += bufferHitCount;
				scanBufferMissHitCount_ += bufferMissHitCount;
				scanBufferSwapReadSize_ += bufferSwapReadSize;
				scanBufferSwapWriteSize_ += bufferSwapWriteSize;
			}

			void getStoreProfiler(int64_t& bufferHitCount, int64_t& bufferMissHitCount, int64_t& bufferSwapReadSize, int64_t& bufferSwapWriteSize) {
				bufferHitCount = scanBufferHitCount_;
				bufferMissHitCount = scanBufferMissHitCount_;
				bufferSwapReadSize = scanBufferSwapReadSize_;
				bufferSwapWriteSize = scanBufferSwapWriteSize_;
			}

			void startProcessor(TaskExecutionStatus status) {
				status_ = status;
				profilerInfo_->start();
			}

			void endProcessor() {
				profilerInfo_->end();
				status_ = JobManager::TASK_EXEC_IDLE;
				counter_++;
			}

			Job* getJob() {
				return job_;
			}

			TaskId getTaskId() {
				return taskId_;
			}

			SQLType::Id getSQLType() {
				return sqlType_;
			}

			void appendBlock(InputId inputId, TupleList::Block& block);
			bool getBlock(InputId inputId, TupleList::Block& block);

			static const int32_t DEFAULT_NEXT_READY_COUNT = 2;

			void sendReady() {
				sendRequestCount_++;
			}

			void sendComplete() {
				if (sendRequestCount_ > 0) {
					sendRequestCount_--;
				}
			}

			bool isReadyExecute();

			int64_t getExecutionCount() {
				return profilerInfo_->profiler_.executionCount_;
			}

			bool isRemote() {
				if (outputDispatchList_.size() > 0
					&& outputDispatchList_[0].size() > 0) {
					if (outputDispatchList_[0].size() == 1) {
						return (outputDispatchList_[0][0]->nodeId_ > 0);
					}
					else {
						return true;
					}
				}
				return false;
			}

			int64_t getAllocateMemory();
			
		private:

			typedef std::vector<Dispatch*, util::StdAllocator<
				Dispatch*, SQLVariableSizeGlobalAllocator> > DispatchList;

			typedef std::vector<DispatchList, util::StdAllocator<
				DispatchList, SQLVariableSizeGlobalAllocator> > DispatchListArray;

			typedef std::vector<TupleList*, util::StdAllocator<
				TupleList*, SQLVariableSizeGlobalAllocator> > TupleListArray;

			typedef std::vector<TupleList::BlockReader*, util::StdAllocator<
				TupleList::BlockReader*, SQLVariableSizeGlobalAllocator> > BlockReaderArray;

			typedef std::vector<int64_t, util::StdAllocator<
				int64_t, SQLVariableSizeGlobalAllocator> > BlockCounterArray;

			typedef std::vector<uint8_t, util::StdAllocator<
				int64_t, SQLVariableSizeGlobalAllocator> > InputStatusList;

			Job* job_;
			JobManager* jobManager_;
			util::StackAllocator& jobStackAlloc_;
			util::StackAllocator* processorStackAlloc_;
			SQLVarSizeAllocator* processorVarAlloc_;
			SQLVariableSizeGlobalAllocator& globalVarAlloc_;
			DispatchListArray outputDispatchList_;

			bool completed_;
			bool resultComplete_;
			bool immediate_;
			int64_t counter_;
			util::Atomic<int64_t> dispatchTime_;
			util::Atomic<int64_t> dispatchCount_;
			TaskExecutionStatus status_;
			TupleListArray tempTupleList_;
			BlockReaderArray blockReaderList_;
			BlockCounterArray recvBlockCounterList_;
			InputId inputNo_;
			int64_t startTime_;
			int64_t sendBlockCounter_;
			InputStatusList inputStatusList_;
			TaskId taskId_;
			NodeId nodeId_;
			SQLContext* cxt_;
			uint8_t type_;
			SQLType::Id sqlType_;
			AssignNo loadBalance_;
			bool isDml_;
			SQLProcessor* processor_;
			TupleList::Group group_;
			LocalTempStore::GroupId processorGroupId_;
			bool setGroup_;
			TaskProfilerInfo* profilerInfo_;
			util::StackAllocator::Scope* scope_;
			util::Atomic<int64_t> sendEventCount_;
			util::Atomic<int64_t> sendEventSize_;
			util::Atomic<int64_t> sendRequestCount_;
			int64_t scanBufferHitCount_;
			int64_t scanBufferMissHitCount_;
			int64_t scanBufferSwapReadSize_;
			int64_t scanBufferSwapWriteSize_;

			TaskInterruptionHandler interruptionHandler_;
			util::AllocatorLimitter memoryLimitter_;
			void setupContextTask();
			void setupProfiler(TaskInfo* taskInfo);
		};

		Job(JobId& jobId, JobInfo* jobInfo, JobManager* jobManager,
			SQLVariableSizeGlobalAllocator& globalVarAlloc);
		~Job();

		Task* getTask(TaskId taskId) {
			util::LockGuard<util::Mutex> guard(mutex_);
			if (taskId < 0 || taskId >= static_cast<TaskId>(taskList_.size())) {
				return NULL;
			}
			return taskList_[taskId];
		}

		Task* getTaskWithNoLock(TaskId taskId) {
			if (taskId < 0 || taskId >= static_cast<TaskId>(taskList_.size())) {
				return NULL;
			}
			return taskList_[taskId];
		}

		void getStats();

		util::Mutex &getLock() {
			return mutex_;
		}

		int64_t getTaskCount() {
			return taskList_.size();
		}

		void removeTask(TaskId taskId);

		void getSummary(std::string& summary);

		bool checkLimitTime(int64_t &startTime, uint32_t &elapsedMills);

		std::string getLongQueryForAudit(SQLExecution* execution);
		std::string getLongQueryForEventLog(SQLExecution* execution);

		void traceLongQuery(SQLExecution* execution);
		static void traceLongQueryCore(SQLExecution* execution, int64_t startTime, int64_t executionTime);

		void deploy(EventContext& ec, util::StackAllocator& alloc,
			JobInfo* jobInfo, NodeId senderNodeId, int64_t waitInterval);

		void request(EventContext* ec, Event* ev, JobManager::ControlType controlType,
			TaskId taskId, TaskId inputId, bool isEmpty, bool isComplete,
			TupleList::Block* resource, EventMonotonicTime emTime, int64_t waitTime = 0);

		void fetch(EventContext& ec, SQLExecution* execution);

		void cancel(EventContext& ec, bool clientCancel);

		void cancel(const Event::Source& sourcel, CancelOption& option);

		void checkSchema(EventContext& ec, Task* task);
		void ackResult(EventContext& ec);
		bool checkAndSetPending();
		bool isEnableFetch();

		void handleError(EventContext& ec, std::exception* e);

		void checkNextEvent(EventContext& ec, ControlType controlType,
			TaskId taskId, TaskId inputId);

		void setJobProfiler(SQLProfilerInfo& profs);


		typedef uint8_t StatementExecStatus;  

		void receiveNoSQLResponse(EventContext& ec,
			util::Exception& dest, StatementExecStatus status, int32_t pos);

		void getProfiler(util::StackAllocator& alloc, StatJobProfilerInfo& prof, int32_t mode);
		int64_t getVarTotalSize(TaskId taskId, bool isDeploy);
		int64_t getVarTotalSize(bool force = false);

		void checkMemoryLimit();

		void receiveTaskBlock(EventContext& ec, TaskId taskId, InputId inputId,
			TupleList::Block* block, int64_t blockNo, ControlType controlType,
			bool isEmpty, bool isLocal);

		bool getTaskBlock(TaskId taskId, InputId inputId, Task*& task, TupleList::Block& block, bool& isExistBlock);

		PartitionId getDispatchId(TaskId taskId, ServiceType& type);


		void checkCancel(const char* str, bool partitionCheck, bool withNodeCheck = false);

		void checkAsyncPartitionStatus();


		void checkDDLPartitionStatus();
		void ackDDLStatus(int32_t pos);

		bool getFetchContext(SQLFetchContext& cxt);

		void setCancel(bool clientCancelled = false) {
			cancelled_ = true;
			clientCancelled_ = clientCancelled;
		}

		PartitionId getPartitionId() {
			return connectedPId_;
		}

		JobManager* getJobManager() {
			return jobManager_;
		}

		void forwardRequest(EventContext& ec, ControlType controlType,
			Task* task, int outputId, bool isEmpty,
			bool isComplete, TupleList::Block* resource);

		void recvDeploySuccess(EventContext& ec, int64_t waitInterval = 0);

		void recvExecuteSuccess(EventContext& ec, EventByteInStream& in, bool isPending);

		void getProfiler(util::StackAllocator& eventStackAlloc, TaskProfiler& profiler, size_t planId);

		void setProfiler(JobProfilerInfo& profs, bool isRemote = false);

		void encodeProfiler(EventByteOutStream& out);

		TaskProfilerInfo* appendProfiler();


		bool isCoordinator() {
			return coordinator_;
		}

		bool isExplainAnalyze() {
			return isExplainAnalyze_;
		}

		bool isSQL() {
			return isSQL_;
		}

		JobId& getJobId() {
			return jobId_;
		}

		TaskId getResultTaskId() {
			return resultTaskId_;
		}

		PartitionId getConnectedPId() {
			return connectedPId_;
		}

		int64_t getStartTime() {
			return startTime_;
		}

		void incReference() {
			refCount_++;
		}
		bool decReference() {
			refCount_--;
			if (refCount_ == 0) {
				return true;
			}
			else {
				return false;
			}
		}

		void appendProfilerInfo(Task* Task);
		void setProfilerInfo(Task* task);

		bool isDDL() {
			return (baseTaskId_ != UNDEF_TASKID
				&& taskList_.size() > static_cast<size_t>(baseTaskId_));
		}

		void sendReady(TaskId taskId);
		void resetSendCounter();

		void sendReady() {
			sendRequestCount_++;
		}

		void sendComplete() {
			if (sendRequestCount_ > 0) {
				sendRequestCount_--;
			}
		}
		bool checkTaskInterrupt(TaskId taskId);
		void recvSendComplete(TaskId taskId);
		util::StackAllocator* getLocalStackAllocator() {
			return &jobStackAlloc_;
		}

		bool isDeployCompleted() {
			return deployCompleted_;
		}
		DatabaseId getDbId() {
			return dbId_;
		}

		const char* getDbName(util::String& dbName) {
			util::LockGuard<util::Mutex> guard(mutex_);
			dbName = getDbName();
			return dbName.c_str();
		}

		const char* getDbName() {
			return (dbName_.empty() ? GS_PUBLIC : dbName_.c_str());
		}
		bool isAdministrator() {
			return isAdministrator_;
		}

		bool getAppName(util::String &appName) {
			util::LockGuard<util::Mutex> guard(mutex_);
			if (appName_.empty()) {
				appName = appName_;
				return true;
			}
			return false;
		}

		const char* getAppName() {
			return (appName_.empty() ? NULL : appName_.c_str());
		}

		bool getUserName(util::String& userName) {
			util::LockGuard<util::Mutex> guard(mutex_);
			if (userName.empty()) {
				userName = userName_;
				return true;
			}
			return false;
		}

		const char* getUserName() {
			return (userName_.empty() ? NULL : userName_.c_str());
		}

		const char* getSQL() {
			return (sql_.empty() ? NULL : sql_.c_str());
		}

	private:

		util::StackAllocator* getStackAllocator(
				util::AllocatorLimitter *limitter);
		void executeJobStart(EventContext* ec, int64_t waitTime);
		void executeTask(EventContext* ec, Event* ev, JobManager::ControlType controlType,
			TaskId taskId, TaskId inputId, bool isEmpty, bool isComplete,
			TupleList::Block* resource,
			EventMonotonicTime emTime, int64_t waitTime);
		void setCustomProfile(SQLProcessor* processor);
		void decodeOptional(EventByteInStream& in);

		static bool isDQLProcessor(SQLType::Id type);
		SQLVarSizeAllocator* getLocalVarAllocator(
				util::AllocatorLimitter &limitter);
		void releaseStackAllocator(util::StackAllocator* alloc);
		void releaseLocalVarAllocator(SQLVarSizeAllocator* varAlloc);
		void encodeHeader(util::ByteStream<util::XArrayOutStream<> >& out, int32_t type, uint32_t size);
		void decodeHeader(EventByteInStream& in, int32_t &type, uint32_t &size);
		bool checkTotalMemorySize();
		int64_t getTotalMemorySizeInternal(TaskId taskId);

		JobManager* jobManager_;
		util::Mutex mutex_;
		util::StackAllocator& jobStackAlloc_;
		util::StackAllocator::Scope* scope_;

		SQLVariableSizeGlobalAllocator& globalVarAlloc_;
		typedef std::vector<Dispatch*, util::StdAllocator<
			Dispatch*, SQLVariableSizeGlobalAllocator> > DispatchList;
		DispatchList noCondition_;

		typedef std::vector<NodeId, util::StdAllocator<
			NodeId, SQLVariableSizeGlobalAllocator> > NodeIdList;
		NodeIdList nodeList_;

		JobId jobId_;
		int64_t refCount_;
		bool coordinator_;
		NodeId coordinatorNodeId_;
		util::Atomic<bool> cancelled_;
		util::Atomic<bool> clientCancelled_;
		ExecutionStatus status_;
		int64_t queryTimeout_;
		int64_t expiredTime_;
		int64_t startTime_;
		int64_t startMonotonicTime_;
		bool isSetJobTime_;
		int32_t selfTaskNum_;
		bool deployCompleted_;

		util::Stopwatch watch_;

		typedef std::vector<Task*, util::StdAllocator<
			Task*, SQLVariableSizeGlobalAllocator> > TaskList;
		TaskList taskList_;
		typedef std::vector<int64_t, util::StdAllocator<
			int64_t, SQLVariableSizeGlobalAllocator> > TaskStatList;

		TaskStatList taskStatList_;

		util::Atomic<int64_t> jobAllocateMemory_;

	public:
		util::Atomic<int64_t> executionCount_;
	private:
		util::Atomic<int32_t> deployCompletedCount_;
		util::Atomic<int32_t> executeCompletedCount_;
		util::Atomic<int32_t> taskCompletedCount_;

		TaskId resultTaskId_;
		PartitionId connectedPId_;
		NoSQLSyncContext* syncContext_;
		TaskId baseTaskId_;

		typedef std::vector<AssignNo, util::StdAllocator<
			AssignNo, SQLVariableSizeGlobalAllocator> > LimitAssignNoList;
		LimitAssignNoList limitedAssignNoList_;
		int32_t limitAssignNum_;
		int32_t currentAssignNo_;
		JobProfilerInfo profilerInfos_;
		bool isExplainAnalyze_;

		typedef std::vector<uint8_t, util::StdAllocator<
			uint8_t, SQLVariableSizeGlobalAllocator> > ServiceList;
		NodeIdList dispatchNodeIdList_;
		ServiceList dispatchServiceList_;

	public:
		util::Atomic<int64_t> sendRequestCount_;

	private:
		int64_t limitPendingTime_;
		int64_t sendVersion_;
		int32_t sendJobCount_;

	public:
		util::Atomic<int64_t> sendEventCount_;
		util::Atomic<int64_t> sendEventSize_;

	private:
		util::Atomic<int64_t> dispatchTime_;
		util::Atomic<int64_t> dispatchCount_;

	public:
		int64_t deployCompleteTime_;
		int64_t execStartTime_;

	private:
		bool isSQL_;
		double storeMemoryAgingSwapRate_;
		util::TimeZone timezone_;

		DatabaseId dbId_;
		util::String dbName_;
		util::String appName_;
		util::String userName_;
		util::String sql_;
		bool  isAdministrator_;
		uint32_t delayTime_;
		uint32_t chunkSize_;

		bool isImmediateReply(EventContext* ec, ServiceType serviceType);

		void sendJobInfo(EventContext& ec, JobInfo* jobInfo, int64_t waitInterval);

		NodeId resolveNodeId(JobInfo::GsNodeInfo* nodeInfo);

		void setLimitedAssignNoList();

		AssignNo getAssignPId();

		bool isCancel() {
			return cancelled_;
		}

		bool processNextEvent(Task* task, TupleList::Block* block,
			int64_t blockNo, int64_t inputId, ControlType controlType,
			bool isEmpty, bool isLocal);

		void setupTaskInfo(Task* Task, EventContext& ec, Event& ev, TaskId inputId);

		bool pipe(Task* task, TaskId inputId, ControlType controlType, TupleList::Block* block);
		bool pipeOrFinish(Task* task, TaskId inputId, ControlType controlType, TupleList::Block* block, ChunkBufferStatsSQLScope *scope);
		bool executeContinuous(EventContext& ec, Event& ev,
			Task* task, TaskId inputId, ControlType controlType, bool remaining,
			bool isEmpty, bool isComplete, EventMonotonicTime emTime, ChunkBufferStatsSQLScope* scope);

		void doComplete(EventContext& ec, Task* task, TaskId inputId,
			bool isEmpty, bool isComplete);

		void sendJobEvent(EventContext& ec, NodeId nodeId,
			ControlType controlType,
			JobInfo* jobInfo, std::exception* e,
			TaskId taskId, InputId inputId, AssignNo loadBalance,
			ServiceType serviceType, bool isEmpty,
			bool isComplete, TupleList::Block* resource, int64_t waitInterval = 0);

		void sendBlock(EventContext& ec, NodeId nodeId,
			ControlType controlType,
			JobInfo* jobInfo, std::exception* e,
			TaskId taskId, InputId inputId, AssignNo loadBalance, ServiceType type,
			bool isEmpty, bool isComplete, TupleList::Block* resource, int64_t blockNo, Task* task);

		Task* createTask(EventContext& ec, TaskId taskId, JobInfo* jobInfo, TaskInfo* taskInfo);
		bool checkStart();
		void encodeProcessor(EventContext& ec, util::StackAllocator& alloc, JobInfo* jobInfo);

		void encodeOptional(util::ByteStream<util::XArrayOutStream<> >& out);





		class SchemaCheckCounter {

		public:
			SchemaCheckCounter() : localCounter_(0), globalCounter_(0),
				localTargetCount_(0), globalTargetCount_(0),
				enableFetch_(true), pendingFetch_(false) {}

			void initLocal(int32_t targetSize);

			void initGlobal(int32_t targetSize);

			bool notifyLocal();

			bool notifyGlobal();


			bool isEnableFetch();

			bool checkAndSetPending();

			bool isPendingFetch() {
				return pendingFetch_;
			}

			void dump(int32_t pos) {
				std::cout << pos << "," << localCounter_ << "," << globalCounter_
					<< "," << localTargetCount_ << "," << globalTargetCount_
					<< "," << enableFetch_ << "," << pendingFetch_ << std::endl;
			}

		private:

			util::Mutex mutex_;

			int32_t localCounter_;

			int32_t globalCounter_;

			int32_t localTargetCount_;

			int32_t globalTargetCount_;

			bool enableFetch_;

			bool pendingFetch_;
		};

		SchemaCheckCounter checkCounter_;

		class JobResultRequest {

		public:
			JobResultRequest(EventContext& ec, Job* job);

			Event& getEvent() {
				return ev_;
			}

		private:
			Event ev_;
		};
	};

	/*!
		@brief ラッチ
	*/
	class Latch {
	public:

		enum LatchMode {
			LATCH_CREATE,
			LATCH_GET
		};

		/*!
			@brief コンストラクタ
		*/
		Latch(JobId& jobId, const char* str, JobManager* jobManager,
			LatchMode mode = LATCH_GET, JobInfo* jobInfo = NULL);

		/*!
			@brief デストラクタ
		*/
		~Latch();

		/*!
			@brief Job取得
		*/
		Job* get();

	private:

		JobId& jobId_;
		JobManager* jobManager_;
		Job* job_;
		const char* str_;
	};


		/*!
			@brief 制御種別
		*/

	typedef std::pair<JobId, Job*> JobMapEntry;
	typedef std::map<JobId, Job*, std::less<JobId>,
		util::StdAllocator<JobMapEntry,
		SQLVariableSizeGlobalAllocator> > JobMap;
	typedef JobMap::iterator JobMapItr;

	/*!
		@brief コンストラクタ
	*/
	JobManager(SQLVariableSizeGlobalAllocator& globalVarAlloc, LocalTempStore& store,
		const ConfigTable& config);

	/*!
		@brief デストラクタ
	*/
	~JobManager();

	void initialize(const ManagerSet& mgrSet);
	void beginJob(EventContext& ec, JobId& jobId, JobInfo* jobInfo, int64_t waitInterval);
	void removeExecution(
		EventContext* ec, JobId& jobId, SQLExecution* execution, bool withCheck);
	void cancel(EventContext& ec, JobId& jobId, bool clientCancel);
	void cancel(const Event::Source& eventSource, JobId& jobId, CancelOption& option);
	void cancelAll(const Event::Source& eventSource,
		util::StackAllocator& alloc, util::Vector<JobId>& jobIdList, CancelOption& option);
	void getCurrentJobList(util::Vector<JobId>& jobIdList);
	size_t getCurrentJobListSize();
	void checkAlive(EventContext& ec, util::StackAllocator& alloc,
		JobId& jobId, EventMonotonicTime checkTime);
	void checkTimeout(EventContext& ec, int64_t checkTime);

	Job* create(JobId& jobId, JobInfo* jobInfo);
	Job* get(JobId& jobId);
	void resetSendCounter(util::StackAllocator& alloc);

	void getProfiler(util::StackAllocator& alloc,
		StatJobProfiler& jobProfiler, int32_t mode, JobId* jobId);

	void getJobIdList(util::StackAllocator& alloc, StatJobIdList& infoList);
	void executeStatementError(EventContext& ec, std::exception* e, JobId& jobId);
	void executeStatementSuccess(EventContext& ec, Job* job,
		bool isExplainAnalyze, bool withResponse);

	void getDatabaseStats(util::StackAllocator& alloc,
		DatabaseId dbId, util::Map<DatabaseId, DatabaseStats*>& statsMap, bool isAdministrator);

	EventEngine* getEE(ServiceType type) {
		return eeList_[static_cast<size_t>(type)];
	}

	ClusterService* getClusterService() {
		return clsSvc_;
	}

	SQLService* getSQLService() {
		return sqlSvc_;
	}

	LocalTempStore& getStore() {
		return store_;
	}

	DataStoreV4* getDataStore() {
		return dataStore_;
	}

	TransactionService* getTransactionService() {
		return txnSvc_;
	}

	PartitionTable* getPartitionTable() {
		return pt_;
	}

	SQLVariableSizeGlobalAllocator& getGlobalAllocator() {
		return globalVarAlloc_;
	}

	EventEngine* getDefaultEE() {
		return eeList_[DEFAULT_SERVICE];
	}

	SQLExecutionManager* getExecutionManager() {
		return executionManager_;
	}

	void remove(JobId& jobId);

	std::string& getHostName() {
		return hostName_;
	}

	std::string& getHostAddress() {
		return hostAddress_;
	}

	uint16_t getHostPort() {
		return hostPort_;
	}

	int32_t getAssignPId();

	PartitionGroupId getSQLPgId(PartitionId pId);

	bool checkExecutableTask(EventContext& ec,
		Event& ev, int64_t startTime, int64_t limitInterval, bool pending);

	void sendEvent(Event& ev, ServiceType type,
		NodeId nodeId, int64_t waitInterval, Event* sendEvent, bool isSecondary);
	void addEvent(Event& ev, ServiceType type);

	const SQLProcessorConfig* getProcessorConfig() const;

	void sendReady() {
		sendRequestCount_++;
	}

	void sendComplete() {
		if (sendRequestCount_ > 0) {
			sendRequestCount_--;
		}
	}

	int64_t getPendingCount() {
		return sendRequestCount_;
	}

private:

	std::string hostName_;
	std::string hostAddress_;
	uint16_t hostPort_;

	void getHostInfo();

	EventType getEventType(ControlType type);

	void release(Job* job, const char* str);

	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	util::Mutex mutex_;
	util::Mutex stackAllocatorLock_;
	util::Mutex varAllocatorLock_;
	JobMap jobMap_;
	LocalTempStore& store_;
	std::vector<EventEngine*> eeList_;
	ClusterService* clsSvc_;
	PartitionTable* pt_;
	TransactionService* txnSvc_;
	SQLService* sqlSvc_;
	DataStoreV4* dataStore_;
	SQLExecutionManager* executionManager_;
	std::vector<PartitionId> sqlStartPIdList_;
	util::Atomic<int32_t> currentCnt_;
	int32_t sqlConcurrency_;
	int32_t txnConcurrency_;
	PartitionGroupConfig sqlPgConfig_;
	PartitionGroupConfig txnPgConfig_;
	util::Atomic<int64_t> sendRequestCount_;
};

class SQLJobHandler : public EventHandler {
public:
	typedef JobManager::JobId JobId;
	typedef JobManager::ControlType ControlType;
	typedef uint8_t StatementExecStatus;

	SQLJobHandler() : pt_(NULL), jobManager_(NULL),
		clsSvc_(NULL), sqlSvc_(NULL), executionManager_(NULL) {}

	void initialize(const ManagerSet& mgrSet, ServiceType type = SQL_SERVICE);
	ServiceType getServiceType() {
		return type_;
	}
	
	void executeRequest(EventContext& ec, Event& ev, EventByteInStream& in,
		JobId& jobId, JobManager::ControlType controlType);

	static void decodeRequestInfo(EventByteInStream& in,
		JobId& jobId, JobManager::ControlType& controlType,
		int32_t& sqlVersionId);

	static void decodeTaskInfo(EventByteInStream& in,
		TaskId& taskId, InputId& inputId,
		bool& isEmpty, bool& isComplete, int64_t& blockNo);

	static void encodeRequestInfo(EventByteOutStream& out,
		JobId& jobId, ControlType controlType);

	static void encodeTaskInfo(EventByteOutStream& out,
		TaskId taskId, TaskId inputId, bool isEmpty, bool isComplete);

	static void encodeErrorInfo(EventByteOutStream& out,
		std::exception& e, StatementExecStatus status);

	void handleError(EventContext& ec, Event& ev, std::exception& e,
		JobId& jobId, ControlType controlType);

protected:

	PartitionTable* pt_;
	JobManager* jobManager_;
	ClusterService* clsSvc_;
	SQLService* sqlSvc_;
	SQLExecutionManager* executionManager_;
	ServiceType type_;
};

typedef JobManager::Job Job;
typedef JobManager::JobInfo JobInfo;
typedef JobManager::JobId JobId;
typedef JobManager::JobInfo::GsNodeInfo GsNodeInfo;
typedef JobManager::TaskInfo TaskInfo;
typedef JobManager::Job::Task Task;
typedef JobManager::Job::Dispatch Dispatch;

class ExecuteJobHandler : public SQLJobHandler {
public:
	void operator()(EventContext& ec, Event& ev);
private:
	void executeDeploy(EventContext& ec, Event& ev, EventByteInStream& in,
		JobId& jobId, int32_t sqlVersionId, PartitionId& recvPartitionId);
	void executeError(EventContext& ec, EventByteInStream& in, JobId& jobId);
	void executeCancel(EventContext& ec, EventByteInStream& in, JobId& jobId);
	void executeRecvAck(EventContext& ec,
		Event& ev, EventByteInStream& in, JobId& jobId);
	void request(Job* job, EventContext* ec, Event* ev,
		JobManager::ControlType controlType,
		TaskId taskId, TaskId inputId, bool isEmpty, bool isComplete,
		EventMonotonicTime emTime, int64_t waitTime = 0);
};

class ControlJobHandler : public SQLJobHandler {
public:
	void operator()(EventContext& ec, Event& ev);
};

class SQLResourceCheckHandler : public SQLJobHandler {
public:
	void operator()(EventContext& ec, Event& ev);
};

class DispatchJobHandler : public SQLJobHandler {
public:
	void operator()(EventContext& ec, Event& ev);
};


struct SendEventInfo {

	SendEventInfo(EventByteOutStream& out,
		JobId& jobId, TaskId taskId);

	SendEventInfo(EventByteInStream& in);

	JobId jobId_;
	TaskId taskId_;
	int64_t sendVersion_;
};

class SendEventHandler : public ExecuteJobHandler {
public:
	void operator()(EventContext& ec, Event& ev);
};

std::ostream& operator<<(std::ostream& stream, const JobId& id);

class TaskContext {

public:
	class Factory;
	typedef int32_t InputId;

	TaskContext();
	NoSQLSyncContext* getSyncContext() const;
	void setSyncContext(NoSQLSyncContext* syncContext);
	util::StackAllocator& getAllocator();
	void setAllocator(util::StackAllocator* alloc);
	EventContext* getEventContext() const;
	void setEventContext(EventContext* eventContext);
	void setJobManager(JobManager* jobManager);
	Task* getTask();
	void setTask(Task* task);
	void checkCancelRequest();
	void transfer(TupleList::Block& block);
	void transfer(TupleList::Block& block, bool completed_, int32_t outputId);
	void finish(int32_t outputId);
	void finish();

	bool isTransactionService() {
		return (task_->type_ == TRANSACTION_SERVICE);
	}

	void setGlobalAllocator(SQLVariableSizeGlobalAllocator* globalVarAlloc) {
		globalVarAlloc_ = globalVarAlloc;
	}
	SQLVariableSizeGlobalAllocator* getGlobalAllocator() {
		return globalVarAlloc_;
	}
	int64_t getMonotonicTime();

private:

	util::StackAllocator* alloc_;
	SQLVariableSizeGlobalAllocator* globalVarAlloc_;
	JobManager* jobManager_;
	Task* task_;
	EventContext* eventContext_;
	int64_t counter_;
	NoSQLSyncContext* syncContext_;
};

class TaskProcessor {
public:
	typedef int32_t InputId;
	typedef TaskOption::ByteInStream ByteInStream;
	typedef TaskOption::ByteOutStream ByteOutStream;
	typedef util::Vector<TupleList::TupleColumnType> TupleInfo;
	typedef util::Vector<TupleInfo> TupleInfoList;

	TaskProcessor() {}
	~TaskProcessor() {}
};


struct CancelOption {
	CancelOption() : startTime_(0), forced_(false), allocateMemory_(0), limitStartTime_(0) {};
	CancelOption(util::String& startTimeStr);
	int64_t startTime_;
	bool forced_;
	int64_t allocateMemory_;
	int64_t limitStartTime_;
};

struct TaskProf {
	TaskProf() : id_(0), inputId_(0) {}
	int64_t id_;
	int64_t inputId_;
	std::string name_;
	std::string status_;
	std::string startTime_;
	UTIL_OBJECT_CODER_MEMBERS(id_, inputId_, name_, status_, startTime_);
};

struct StatJobProfilerInfo {
	StatJobProfilerInfo(util::StackAllocator& alloc, JobId& targetJobId) :
		id_(0),
		startTime_(alloc),
		deployCompleteTime_(alloc),
		execStartTime_(alloc),
		jobId_(alloc),
		address_(alloc),
		sendPendingCount_(0),
		sendEventCount_(0),
		sendEventSize_(0),
		allocateMemory_(0),
		taskProfs_(alloc) {
		jobId_ = targetJobId.dump(alloc).c_str();
	}

	int64_t getMemoryUse() const;
	int64_t getSqlStoreUse() const;
	int64_t getDataStoreAccess(SQLExecutionManager &executionManager) const;

	int64_t id_;
	util::String startTime_;
	util::String deployCompleteTime_;
	util::String execStartTime_;
	util::String jobId_;
	util::String address_;
	int64_t sendPendingCount_;
	int64_t sendEventCount_;
	int64_t sendEventSize_;
	int64_t allocateMemory_;
	util::Vector<StatTaskProfilerInfo> taskProfs_;

	UTIL_OBJECT_CODER_MEMBERS(
		id_,
		jobId_,
		startTime_,
		deployCompleteTime_,
		execStartTime_,
		sendPendingCount_,
		allocateMemory_,
		address_,
		UTIL_OBJECT_CODER_OPTIONAL(sendEventCount_, 0),
		UTIL_OBJECT_CODER_OPTIONAL(sendEventSize_, 0),
		taskProfs_);
};

struct StatJobProfiler {
	StatJobProfiler(util::StackAllocator& alloc) : jobProfs_(alloc), currentTime_(alloc) {}
	util::Vector<StatJobProfilerInfo> jobProfs_;
	util::String currentTime_;
	UTIL_OBJECT_CODER_MEMBERS(currentTime_, jobProfs_);
};

struct JobExecutionLatch {

	JobExecutionLatch(Job* job, Task* task) : job_(job), remote_(false) {
		if (task && (task->isRemote())) {
			remote_ = true;
			job_->executionCount_++;
		}
	}

	~JobExecutionLatch() {
		if (remote_) {
			job_->executionCount_--;
		}
	}

	bool isRemoteSend() {
		return remote_;
	}

	Job* job_;
	bool remote_;
};

#endif
