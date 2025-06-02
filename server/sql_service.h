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
	@brief Definition of sql service
*/

#ifndef SQL_SERVICE_H_
#define SQL_SERVICE_H_

#include "transaction_service.h"
#include "sql_common.h"
#include "sql_execution.h"

class SQLExecutionManager;
struct SQLTableInfo;

/*!
	@brief SQLサービスハンドラ共通
*/
class SQLServiceHandler : public StatementHandler {
public:
	/*!
		@brief コンストラクタ
	*/
	SQLServiceHandler() : StatementHandler(), globalVarAlloc_(NULL),
		localAlloc_(NULL), sqlSvc_(NULL),
		clsSvc_(NULL), txnSvc_(NULL), sysSvc_(NULL),
		pt_(NULL), executionManager_(NULL) {}

	/*!
		@brief デストラクタ
	*/
	~SQLServiceHandler() {}

	/*!
		@brief 初期化
	*/
	void initialize(const ManagerSet& mgrSet);

	void replyError(
		EventType eventType,
		PartitionId pId, EventContext& ec,
		const NodeDescriptor& nd, StatementId stmtId, const std::exception& e);

protected:

	SQLVariableSizeGlobalAllocator* globalVarAlloc_;

	SQLVariableSizeLocalAllocator** localAlloc_;

	SQLService* sqlSvc_;

	ClusterService* clsSvc_;

	TransactionService* txnSvc_;

	SystemService* sysSvc_;

	PartitionTable* pt_;

	JobManager* jobManager_;

	SQLExecutionManager* executionManager_;
};

class UpdateTableCacheHandler : public SQLServiceHandler {
public:
	void operator()(EventContext& ec, Event& ev);
private:
};

class SQLRequestNoSQLClientHandler : public SQLServiceHandler {
public:
	void operator()(EventContext& ec, Event& ev);
private:
	void executeRepair(EventContext& ec, SQLExecution* execution,
		util::String& containerName, ContainerId largeContainerId,
		SchemaVersionId versionId, NodeAffinityNumber affinity);
};

/*!
	@brief マスタ通知SQL接続先決定ハンドラ
*/
class SQLGetConnectionAddressHandler : public SQLServiceHandler {
public:
	void operator()(EventContext& ec, Event& ev);
};

/*!
	@brief バインド情報
*/
struct BindParam {
	/*!
		@brief コンストラクタ
	*/
	BindParam(util::StackAllocator& alloc, ColumnType type) : alloc_(alloc), type_(type) {}

	BindParam(util::StackAllocator& alloc, ColumnType type, TupleValue& value) :
		alloc_(alloc), type_(type), value_(value) {}

	/*!
		@brief デストラクタ
	*/
	~BindParam() {}

	void dump(util::StackAllocator& alloc);

	util::StackAllocator& alloc_;

	ColumnType type_;

	TupleValue value_;
};

/*!
	@brief SQLリクエスト処理ハンドラ
*/
class SQLRequestHandler : public SQLServiceHandler {

	static const char8_t* const querySeparator;


	template <typename T1, typename T2>
	void setValue(util::StackAllocator& alloc, T2 value,
		int32_t& size, char*& data) {
		size = sizeof(T1);
		data = reinterpret_cast<char*>(ALLOC_NEW(alloc) T1);
		*reinterpret_cast<T1*>(data) = static_cast<T1>(value);
	}

public:
	explicit SQLRequestHandler(util::AllocatorLimitter &workMemoryLimitter);

	/*!
		@brief ステートメント種別
	*/


	typedef util::Vector<TupleList::TupleColumnType> ValueTypeList;
	typedef util::ByteStream< util::ArrayInStream > InStream;

	void operator()(EventContext& ec, Event& ev);

	static void encodeEnvList(
		EventByteOutStream& out, util::StackAllocator& alloc,
		SQLExecutionManager& executionManager,
		ConnectionOption& connOption);

	static void decodeBindInfo(util::ByteStream<util::ArrayInStream>& in,
		RequestInfo& requestInfo);

	static void decodeBindColumnInfo(util::ByteStream<util::ArrayInStream>& in,
		RequestInfo& requestInfo, InStream& fixedPartIn, InStream& varPartIn);

private:

	/*!
		@brief オプションをセットする
	*/
	void setDefaultOptionValue(OptionSet& optionSet);

	/*!
		@brief クライアントリクエストデコード
	*/
	void decode(util::ByteStream<util::ArrayInStream>& in,
		RequestInfo& requestInfo);
	void replySuccess(Event& ev, NodeDescriptor& nd);

	util::AllocatorLimitter &workMemoryLimitter_;
};

/*!
	@brief SQLキャンセルハンドラ
*/
class SQLCancelHandler : public SQLServiceHandler {
public:
	void operator()(EventContext& ec, Event& ev);
};

/*!
	@brief ソケット切断ハンドラ
*/
class SQLSocketDisconnectHandler : public SQLServiceHandler {
public:
	void operator()(EventContext& ec, Event& ev);
};

/*!
	@brief NoSQL同期処理を行うハンドラ
*/
class NoSQLSyncReceiveHandler : public SQLServiceHandler {
public:
	void operator()(EventContext& ec, Event& ev);
};

/*!
	@brief コンストラクタ
*/
class SQLService {
public:
	static const int32_t SQL_V1_1_X_CLIENT_VERSION;
	static const int32_t SQL_V1_5_X_CLIENT_VERSION;
	static const int32_t SQL_V2_9_X_CLIENT_VERSION;
	static const int32_t SQL_V3_0_X_CLIENT_VERSION;
	static const int32_t SQL_V3_1_X_CLIENT_VERSION;
	static const int32_t SQL_V3_2_X_CLIENT_VERSION;
	static const int32_t SQL_V3_5_X_CLIENT_VERSION;
	static const int32_t SQL_V4_0_0_CLIENT_VERSION;
	static const int32_t SQL_V4_0_1_CLIENT_VERSION;
	static const int32_t SQL_CLIENT_VERSION;

	static const int32_t SQL_V4_0_MSG_VERSION;
	static const int32_t SQL_V4_1_MSG_VERSION;
	static const int32_t SQL_V4_1_0_MSG_VERSION;
	static const int32_t SQL_V5_5_MSG_VERSION;
	static const int32_t SQL_V5_6_MSG_VERSION;
	static const int32_t SQL_V5_8_MSG_VERSION;
	static const int32_t SQL_MSG_VERSION;
	static const bool SQL_MSG_BACKWARD_COMPATIBLE;

	static const int32_t DEFAULT_RESOURCE_CONTROL_LEVEL;

	/*!
		@brief コンストラクタ
	*/
	SQLService(
		ConfigTable& config, const EventEngine::Config& eeConfig,
		EventEngine::Source& eeSource, const char* name, PartitionTable* pt);

	/*!
		@brief デストラクタ
	*/
	~SQLService();

	static void checkVersion(int32_t versionId);

	/*!
		@brief コンフィグセットアップ
	*/
	static class ConfigSetUpHandler : public ConfigTable::SetUpHandler {
		virtual void operator()(ConfigTable& config);
	} configSetUpHandler_;

	/*!
		@brief 初期化
	*/
	void initialize(const ManagerSet& mgrSet);

	/*!
		@brief サービス開始
	*/
	void start();

	/*!
		@brief サービス停止
	*/
	void shutdown();

	/*!
		@brief サービス待ち合わせ
	*/
	void waitForShutdown();

	/*!
		@brief EventEngine取得
	*/
	EventEngine* getEE() {
		return &ee_;
	}

	struct Config : public ConfigTable::ParamHandler {
		Config() : sqlSvc_(NULL) {};
		void setUpConfigHandler(SQLService* sqlSvc, ConfigTable& configTable);
		virtual void operator()(
			ConfigTable::ParamId id, const ParamValue& value);
		SQLService* sqlSvc_;
	};

	void setTraceLimitTime(int32_t traceLimitTime) {
		if (traceLimitTime * 1000 > INT32_MAX) {
			traceLimitTime_ = INT32_MAX;
		}
		else {
			traceLimitTime_ = traceLimitTime * 1000;
		}
	};

	void setTraceLimitQuerySize(int32_t querySize) {
		traceLimitQuerySize_ = querySize;
	};

	int32_t getTraceLimitTime() {
		return traceLimitTime_;
	};

	int32_t getTraceLimitQuerySize() {
		return traceLimitQuerySize_;
	};

	void setSendPendingInterval(int32_t interval) {
		sendPendingInterval_ = interval;
	}

	int32_t getSendPendingInterval() {
		return sendPendingInterval_;
	}

	void setSendPendingTaskLimit(int32_t limit) {
		sendPendingTaskLimit_ = limit;
	}

	int32_t getSendPendingTaskLimit() {
		return sendPendingTaskLimit_;
	}

	void setSendPendingJobLimit(int32_t limit) {
		sendPendingJobLimit_ = limit;
	}

	int32_t getSendPendingJobLimit() {
		return sendPendingJobLimit_;
	}

	void setSendPendingTaskConcurrency(int32_t limit) {
		sendPendingTaskConcurrency_ = limit;
	}

	int32_t getSendPendingTaskConcurrency() {
		return sendPendingTaskConcurrency_;
	}

	void setAddBatchMaxCount(int32_t count) {
		addBatchMaxCount_ = count;
	}

	int32_t getAddBatchMaxCount() {
		return addBatchMaxCount_;
	}

	void setTablePartitioningMaxAssignedNum(int32_t count) {
		tablePartitioningMaxAssignedNum_ = count;
	}

	int32_t getTablePartitioningMaxAssignedNum() {
		return tablePartitioningMaxAssignedNum_;
	}

	void setTablePartitioningMaxAssignedEntryNum(int32_t count) {
		tablePartitioningMaxAssignedEntryNum_ = count;
	}

	int32_t getTablePartitioningMaxAssignedEntryNum() {
		return tablePartitioningMaxAssignedEntryNum_;
	}

	void setJobMemoryLimit(int64_t limit) {
		jobMemoryLimit_ = limit;
	}

	int64_t getJobMemoryLimit() {
		return jobMemoryLimit_;
	}

	void enableProfiler(bool isProfiler) {
		isProfiler_ = isProfiler;
	}

	bool isEnableProfiler() {
		return isProfiler_;
	}

	int32_t getMemoryCheckCount() {
		return checkCounter_;
	}

	void setMemoryCheckCount(int32_t count) {
		checkCounter_ = count;
	}

	void setNoSQLFailoverTimeout(int32_t interval) {
		if (interval * 1000 > INT32_MAX) {
			nosqlFailoverTimeout_ = INT32_MAX;
		}
		else {
			nosqlFailoverTimeout_ = interval * 1000;
		}
	}

	int32_t getNoSQLFailoverTimeout() {
		return nosqlFailoverTimeout_;
	}

	void setTableSchemaExpiredTime(int32_t interval) {
		if (interval * 1000 > INT32_MAX) {
			tableSchemaExpiredTime_ = INT32_MAX;
		}
		else {
			tableSchemaExpiredTime_ = interval * 1000;
		}
	}

	int32_t getTableSchemaExpiredTime() {
		return tableSchemaExpiredTime_;
	}

	bool getScanMetaTableByAdmin() {
		return scanMetaTableByAdmin_;
	}

	void setScanMetaTableByAdmin(bool flag) {
		scanMetaTableByAdmin_ = flag;
	}

	/*!
		@brief パーティショングループ番号の取得
	*/
	PartitionGroupId getPartitionGroupId(PartitionId pId) {
		return pgIdList_[pId];
	}

	PartitionId getBeginPId(PartitionGroupId pgId) {
		return startPIdList_[pgId];
	}

	PartitionTable* getPartitionTable() {
		return pt_;
	}
	/*!
		@brief コンカレンシを取得
	*/
	int32_t getConcurrency() {
		return pgConfig_.getPartitionGroupCount();
	}

	/*!
		@brief グローバルアロケータの取得
	*/
	SQLVariableSizeGlobalAllocator* getAllocator() {
		return globalVarAlloc_;
	}

	/*!
		@brief ローカルアロケータの取得
	*/
	SQLVariableSizeLocalAllocator** getLocalAllocator() {
		return localAlloc_;
	}

	/*!
		@brief ローカルアロケータの取得
	*/
	SQLVariableSizeLocalAllocator* getLocalAllocator(int32_t nth) {
		assert(localAlloc_ != NULL && nth < getConcurrency());
		return localAlloc_[nth];
	}

	TransactionManager* getTransactionManager() {
		return &txnMgr_;
	}

	SQLExecutionManager* getExecutionManager() {
		return executionManager_;
	}

	ClusterService* getClusterService() {
		return clsSvc_;
	}

	int64_t getTotalInternalConnectionCount() {
		return totalInternalConnectionCount_;
	}

	int64_t getTotalExternalConnectionCount() {
		return totalExternalConnectionCount_;
	}

	void setTotalInternalConnectionCount() {
		totalInternalConnectionCount_++;
	}

	void setTotalExternalConnectionCount() {
		totalExternalConnectionCount_++;
	}

	void requestCancel(EventContext& ec);
	void sendCancel(EventContext& ec, NodeId nodeId);

	/*!
		@brief エンコード
	*/
	template <class T> void encode(Event& ev, T& t);
	template <class T> void decode(Event& ev, T& t);

	static SQLTableInfo* decodeTableInfo(
		util::StackAllocator& alloc, util::ArrayByteInStream& in);
	static void applyClusterPartitionCount(
		SQLTableInfo& tableInfo, uint32_t clusterPartitionCount);

	uint64_t incClientSequenceNo() {
		return ++clientSequenceNo_;
	}

	PartitionGroupConfig& getConfig() {
		return pgConfig_;
	}

	uint64_t getClientSequenceNo() {
		return clientSequenceNo_;
	}

	void checkNodeStatus() {
		try {
			getClusterService()->getManager()->checkNodeStatus();
		}
		catch (std::exception& e) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_CLUSTER_ROLE_UNMATCH,
				GS_EXCEPTION_MERGE_MESSAGE(e, ""));
		}
	}

	void checkActiveStatus();

	int32_t getTableSchemaCacheSize() {
		return tableSchemaCacheSize_;
	}

	EventMonitor& getEventMonitor() {
		return eventMonitor_;
	}

	util::AllocatorLimitter* getTotalMemoryLimitter() {
		return totalMemoryLimitter_;
	}

	util::AllocatorLimitter& getWorkMemoryLimitter() {
		return workMemoryLimitter_;
	}

	bool isFailOnTotalMemoryLimit() {
		const int32_t level = resolveResourceControlLevel();
		return failOnTotalMemoryLimit_ && isTotalMemoryLimittable(level);
	}

private:
	int32_t resolveResourceControlLevel() {
		return (resourceControlLevel_ == 0 ?
				DEFAULT_RESOURCE_CONTROL_LEVEL : resourceControlLevel_);
	}

	static bool isTotalMemoryLimittable(int32_t resourceControlLevel) {
		return resourceControlLevel > 1;
	}

	void setTotalMemoryLimit(uint64_t limit);
	void setWorkMemoryLimitRate(double rate);
	void setFailOnTotalMemoryLimit(bool enabled);
	void setResourceControlLevel(int32_t level);

	void setUpMemoryLimits();

	void applyTotalMemoryLimit();
	void applyFailOnTotalMemoryLimit();

	size_t resolveTotalMemoryLimit();

	util::AllocatorLimitter* resolveTotalMemoryLimitter();

	static uint64_t doubleToBits(double src);
	static double bitsToDouble(uint64_t src);

	EventEngine ee_;
	const EventEngine::Source eeSource_;

	SQLVariableSizeGlobalAllocator* globalVarAlloc_;
	SQLVariableSizeLocalAllocator** localAlloc_;
	PartitionGroupConfig pgConfig_;
	int32_t notifyClientInterval_;
	SQLExecutionManager* executionManager_;
	TransactionService* txnSvc_;
	ClusterService* clsSvc_;
	TransactionManager txnMgr_;

	std::vector<uint8_t> statusList_;

	PartitionIdList startPIdList_;
	PartitionIdList pgIdList_;

	PartitionTable* pt_;

	util::Atomic <uint64_t> clientSequenceNo_;

	util::Atomic <int64_t> nosqlSyncId_;

	util::AllocatorLimitter *totalMemoryLimitter_;
	util::AllocatorLimitter workMemoryLimitter_;

	/*!
		@brief EventEngine config設定
	*/
	EventEngine::Config createEEConfig(
		const ConfigTable& config, const EventEngine::Config& src);

	ServiceThreadErrorHandler serviceThreadErrorHandler_;

	ConnectHandler connectHandler_;

	LoginHandler loginHandler_;

	DisconnectHandler disconnectHandler_;

	SQLGetConnectionAddressHandler getConnectionAddressHandler_;

	SQLTimerNotifyClientHandler timerNotifyClientHandler_;

	SQLRequestHandler requestHandler_;

	SQLCancelHandler cancelHandler_;

	NoSQLSyncReceiveHandler nosqlSyncReceiveHandler_;

	SQLSocketDisconnectHandler socketDisconnectHandler_;

	UnknownStatementHandler unknownStatementHandler_;

	IgnorableStatementHandler ignorableStatementHandler_;

	UpdateTableCacheHandler updateTableCacheHandler_;
	SQLRequestNoSQLClientHandler requestNoSQLClientHandler_;
	AuthenticationAckHandler authenticationAckHandler_;
	CheckTimeoutHandler checkTimeoutHandler_;

	ExecuteJobHandler executeHandler_;
	ControlJobHandler controlHandler_;
	SQLResourceCheckHandler resourceCheckHandler_;

	DispatchJobHandler dispatchJobHandler_;

	SendEventHandler sendEventHandler_;

	int32_t traceLimitTime_;
	int32_t traceLimitQuerySize_;
	int32_t nosqlFailoverTimeout_;
	int32_t tableSchemaExpiredTime_;
	int32_t tableSchemaCacheSize_;

	int64_t jobMemoryLimit_;

	uint64_t storeMemoryLimit_;
	uint64_t workMemoryLimit_;

	util::Atomic<uint64_t> totalMemoryLimit_;
	util::Atomic<uint64_t> workMemoryLimitRate_;
	util::Atomic<bool> failOnTotalMemoryLimit_;
	util::Atomic<int32_t> resourceControlLevel_;

	bool isProfiler_;
	int32_t checkCounter_;

	int32_t sendPendingInterval_;
	int32_t sendPendingTaskLimit_;
	int32_t sendPendingJobLimit_;
	int32_t sendPendingTaskConcurrency_;
	int32_t addBatchMaxCount_;

	int32_t tablePartitioningMaxAssignedNum_;
	int32_t tablePartitioningMaxAssignedEntryNum_;

	int64_t totalInternalConnectionCount_;
	int64_t totalExternalConnectionCount_;
	bool scanMetaTableByAdmin_;

	EventMonitor eventMonitor_;

	Config config_;

	static class StatSetUpHandler : public StatTable::SetUpHandler {
		virtual void operator()(StatTable& stat);
	} statSetUpHandler_;

	class StatUpdator : public StatTable::StatUpdator {
		virtual bool operator()(StatTable& stat);

	public:
		StatUpdator();
		SQLService* service_;
	} statUpdator_;

	static class MemoryLimitErrorHandler : public util::AllocationErrorHandler {
	public:
		virtual ~MemoryLimitErrorHandler();
		virtual void operator()(util::Exception &e);
	} memoryLimitErrorHandler_;

};




#define RAISE_RETURN(targetType)
#define RAISE_EXCEPTION(targetType)
#define RAISE_EXCEPTION2(targetType)
#define RAISE_SLEEP(targetType)
#define RAISE_SLEEP2(targetType)
#define RAISE_RETURN_BOOL(targetType)
#define RAISE_OPERATION(targetType)
#define RAISE_OPERATION_BOOL(targetType)
#define RAISE_OPERATION_TASK(targetType, task)
#define RAISE_OPERATION_CHANGE_VALUE(targetType, value1, value2)
#define RAISE_CHANGE_VALUE(targetType, value1, value2)
#define RAISE_OPERATION_NULL(targetType)
#define RAISE_RETURN_TASK(targetType, task)
#define RAISE_EXCEPTION_TASK(targetType, task)
#define RAISE_SLEEP_TASK(targetType, task)
#define RAISE_CONTINUE(targetType, pos1, pos2)




#endif
