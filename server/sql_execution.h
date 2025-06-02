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
	@brief Definition of sql execution
*/
#ifndef SQL_EXECUTION_H_
#define SQL_EXECUTION_H_

#include "sql_parser.h"
#include "transaction_context.h"
#include "sql_job_manager.h"
#include "transaction_service.h"
#include "sql_result_set.h"

class SQLExecution;
class SQLTableInfoList;
class DBConnection;
class NoSQLStore;
struct BindParam;
struct SQLProfilerInfo;
struct SQLProfiler;
struct NoSQLSyncContext;
struct ExecutionProfilerInfo;
class DataStoreV4;
struct DatabaseStats;
class SQLResultManager;

enum SQLRequestType {
	REQUEST_TYPE_EXECUTE,
	REQUEST_TYPE_PREPARE,
	REQUEST_TYPE_QUERY,
	REQUEST_TYPE_UPDATE,
	REQUEST_TYPE_PRAGMA,
	REQUEST_TYPE_FETCH,
	REQUEST_TYPE_CLOSE,
	REQUEST_TYPE_CANCEL,
	UNDEF_REQUEST_TYPE
};

typedef uint8_t SQLGetMode;
static const SQLGetMode SQL_CREATE = 1 << 0;
static const SQLGetMode SQL_GET = 1 << 1;
static const SQLGetMode SQL_PUT = (SQL_CREATE | SQL_GET);

static const int32_t SQL_DEFAULT_QUERY_TIMEOUT_INTERVAL = INT32_MAX;
static const int64_t SQL_MAX_ROWS = INT64_MAX;
static const int64_t SQL_DEFAULT_FETCH_SIZE = 65535;

class BindParamSet {
public:
	BindParamSet(util::StackAllocator &alloc) :
		alloc_(alloc),
		bindParamList_(alloc),
		currentPos_(0),
		columnSize_(0),
		rowCount_(0) {}

	void prepare(int32_t columnNum, int64_t rowCount) {
		clear();
		columnSize_ = columnNum;
		rowCount_ = rowCount;
		for (int64_t pos = 0; pos < rowCount; pos++) {
			util::Vector<BindParam*> bindParamList(alloc_);
			bindParamList_.push_back(bindParamList);
		}
	}

	void append(BindParam *bindParam) {
		if (static_cast<ptrdiff_t>(bindParamList_[currentPos_].size()) >=
				columnSize_) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_EXECUTION_INTERNAL,
				"(pos=" << currentPos_ << ", size1=" << bindParamList_[currentPos_].size() <<",size2=" << columnSize_);
			GS_THROW_USER_ERROR(GS_ERROR_SQL_EXECUTION_INTERNAL, "");
		}
		bindParamList_[currentPos_].push_back(bindParam);
	}

	util::Vector<BindParam*> &getParamList(size_t pos) {
		if (bindParamList_.size() == 0) {
			util::Vector<BindParam*> bindParamList(alloc_);
			bindParamList_.push_back(bindParamList);
		}
		if (pos >= bindParamList_.size()) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_EXECUTION_INTERNAL, 
				"(pos=" << pos << ", size=" << bindParamList_.size());
		}
		return bindParamList_[pos];
	}

	void reset() {
		currentPos_ = 0;
	}

	bool next() {
		if (static_cast<ptrdiff_t>(currentPos_) == rowCount_) {
			return false;
		}
		currentPos_++;
		return true;
	}

	void clear();

	void copyBindParamSet(const BindParamSet& rhs);

	int64_t getRowCount() {
		return rowCount_;
	}

private:
	util::StackAllocator &alloc_;
	util::Vector<util::Vector<BindParam*>> bindParamList_;
	size_t currentPos_;
	int32_t columnSize_;
	int64_t rowCount_;
};

/*!
	@brief クライアントリクエスト情報
*/
struct RequestInfo {
public:
	/*!
		@brief コンストラクタ
	*/
	RequestInfo(util::StackAllocator& eventStackAlloc,
		PartitionId pId, PartitionGroupId pgId) :
		eventStackAlloc_(eventStackAlloc),
		pId_(pId), pgId_(pgId), stmtId_(UNDEF_STATEMENTID),
		requestType_(UNDEF_REQUEST_TYPE),
		closeQueryId_(UNDEF_SESSIONID),
		isAutoCommit_(true), transactionStarted_(false), retrying_(false),
		queryTimeout_(SQL_DEFAULT_QUERY_TIMEOUT_INTERVAL),
		maxRows_(SQL_MAX_ROWS),
		inTableExists_(false), inTableSchema_(eventStackAlloc_),
		inTableData_(eventStackAlloc_),
		sqlCount_(0), sqlString_(eventStackAlloc_),
		option_(eventStackAlloc_),
		paramCount_(0),
		sessionMode_(SQL_CREATE),
		eventType_(UNDEF_EVENT_TYPE),
		bindParamSet_(eventStackAlloc),
		serializedBindInfo_(eventStackAlloc_),
		serialized_(true) {
	}

	RequestInfo(util::StackAllocator& eventStackAlloc, bool serialized) :
		eventStackAlloc_(eventStackAlloc),
		pId_(0), pgId_(0), stmtId_(UNDEF_STATEMENTID),
		requestType_(UNDEF_REQUEST_TYPE),
		closeQueryId_(UNDEF_SESSIONID),
		isAutoCommit_(true),
		transactionStarted_(false),
		retrying_(false),
		queryTimeout_(SQL_DEFAULT_QUERY_TIMEOUT_INTERVAL),
		maxRows_(SQL_MAX_ROWS),
		inTableExists_(false), inTableSchema_(eventStackAlloc_),
		inTableData_(eventStackAlloc_),
		sqlCount_(0), sqlString_(eventStackAlloc_),
		option_(eventStackAlloc_),
		paramCount_(0),
		sessionMode_(SQL_CREATE),
		eventType_(UNDEF_EVENT_TYPE),
		bindParamSet_(eventStackAlloc),
		serializedBindInfo_(eventStackAlloc_),
		serialized_(serialized) {
	}

	/*!
		@brief デストラクタ
	*/
	~RequestInfo() {}

	/*!
		@brief バインド要求判定
	*/
	bool isBind() {
		return (inTableExists_ == true);
	}

	util::StackAllocator& eventStackAlloc_;

	PartitionId pId_;

	PartitionGroupId pgId_;

	ClientId clientId_;

	StatementId stmtId_;

	SQLRequestType requestType_;

	SessionId closeQueryId_;

	bool isAutoCommit_;

	bool transactionStarted_;

	bool retrying_;

	int32_t queryTimeout_;

	int64_t maxRows_;

	bool inTableExists_;

	util::XArray<uint8_t> inTableSchema_;

	util::XArray<uint8_t> inTableData_;

	int32_t sqlCount_;

	util::String sqlString_;

	StatementMessage::OptionSet option_;

	int32_t paramCount_;

	SQLGetMode sessionMode_;

	EventType eventType_;

	BindParamSet bindParamSet_;

	util::XArray<uint8_t> serializedBindInfo_;
	bool serialized_;
};

/*!
	@brief クライアントリクエスト情報
*/
struct ResponseInfo {
	ResponseInfo() : stmtId_(UNDEF_STATEMENTID),
		transactionStarted_(false),
		isAutoCommit_(true), isRetrying_(false),
		resultCount_(1), updateCount_(0),
		parameterCount_(0), existsResult_(false),
		isContinue_(false),
		resultSet_(NULL),
		prepareBinded_(false) {}
	void reset();
	void set(RequestInfo& request, SQLResultSet* resultSet);
	StatementId stmtId_;
	bool transactionStarted_;
	bool isAutoCommit_;
	bool isRetrying_;
	int64_t resultCount_;
	int64_t updateCount_;
	int32_t parameterCount_;
	bool existsResult_;
	bool isContinue_;
	SQLResultSet* resultSet_;
	bool prepareBinded_;
};

struct SQLConfigParam {
public:
	SQLConfigParam(const ConfigTable& config);

	void setMultiIndexScan(bool value) {
		multiIndexScan_ = value;
	}

	void setPartitioningRowKeyConstraint(bool value) {
		partitioningRowKeyConstraint_ = value;
	}

	void setCostBasedJoin(bool value) {
		costBasedJoin_ = value;
	}

	void setCostBasedJoinDriving(bool value) {
		costBasedJoinDriving_ = value;
	}

	bool isMultiIndexScan() const {
		return multiIndexScan_;
	}

	bool isPartitioningRowKeyConstraint() const {
		return partitioningRowKeyConstraint_;
	}

	bool isCostBasedJoin() const {
		return costBasedJoin_;
	}

	bool isCostBasedJoinDriving() const {
		return costBasedJoinDriving_;
	}

	const SQLPlanningVersion& getPlanningVersion() const {
		return planningVersion_;
	}

private:
	SQLConfigParam(const SQLConfigParam&);
	const SQLConfigParam& operator=(const SQLConfigParam&);

	SQLPlanningVersion getPlanningVersion(
			const ConfigTable& config, SQLConfigTableParamId paramId);

	bool multiIndexScan_;
	bool partitioningRowKeyConstraint_;
	bool costBasedJoin_;
	bool costBasedJoinDriving_;
	SQLPlanningVersion planningVersion_;
};

class SQLConnectionControl {
public:
	SQLConnectionControl(
		SQLExecutionManager* executionMgr, SQLExecution* execution);
	void setEnv(uint32_t paramId, std::string& key);
	bool getEnv(uint32_t paramId, std::string& key);
	const SQLConfigParam& getConfig() const {
		return config_;
	}

private:
	SQLExecutionManager* executionMgr_;
	SQLExecution* execution_;
	bool hasData_;
	const SQLConfigParam& config_;
};

struct SQLExpandViewContext {

	SQLExpandViewContext() : eventStackAlloc_(NULL), control_(NULL),
		parsedInfo_(NULL), defaultDbName_(NULL), ec_(NULL),
		tableInfoList_(NULL), tableSet_(NULL),
		liveNodeIdList_(NULL), viewSet_(NULL),
		useCache_(true), withVersion_(true),
		forceMetaDataQuery_(false), isMetaDataQuery_(false),
		metaVisible_(false), internalMode_(false), forDriver_(false) {}

	util::StackAllocator* eventStackAlloc_;
	SQLConnectionControl* control_;
	SQLParsedInfo* parsedInfo_;
	const char8_t* defaultDbName_;
	EventContext* ec_;
	SQLTableInfoList* tableInfoList_;
	util::Set<util::String>* tableSet_;
	util::Vector<NodeId>* liveNodeIdList_;
	util::Set<util::String>* viewSet_;
	void setAllocator(util::StackAllocator* eventStackAlloc) {
		eventStackAlloc_ = eventStackAlloc;
	}
	util::StackAllocator& getAllocator() {
		return *eventStackAlloc_;
	}

	bool useCache_;
	bool withVersion_;
	bool forceMetaDataQuery_;
	bool isMetaDataQuery_;
	bool metaVisible_;
	bool internalMode_;
	bool forDriver_;
};

struct SQLFetchContext {

	SQLFetchContext() {}
	SQLFetchContext(TupleList::Reader* reader, TupleList::Column* columnList,
		size_t columnSize, bool isCompleted) :
		reader_(reader), columnList_(columnList),
		columnSize_(columnSize), isCompleted_(isCompleted) {
	}
	TupleList::Reader* reader_;
	TupleList::Column* columnList_;
	size_t columnSize_;
	bool isCompleted_;
};

class SQLExecutionManager {

	typedef std::pair<ClientId, SQLExecution*> SQLExecutionEntry;
	typedef std::map<ClientId, SQLExecution*, std::less<ClientId>,
		util::StdAllocator<SQLExecutionEntry,
		SQLVariableSizeGlobalAllocator> > SQLExecutionMap;
	typedef SQLExecutionMap::iterator SQLExecutionMapItr;

	static const size_t DEFAULT_ALLOCATOR_BLOCK_SIZE;
	static const size_t EXECUTION_FREE_LIMIT_SIZE;
	static const int32_t DEFAULT_WORK_CACHE_MEMORY_MB;
	static const int32_t DEFAULT_TABLE_CACHE_SIZE;

public:

	class Latch {
	public:

		Latch(ClientId& clientId, SQLExecutionManager* execMgr);
		Latch(ClientId& clientId, SQLExecutionManager* execMgr,
			const NodeDescriptor* clientNd,
			RequestInfo* request);
		~Latch();
		SQLExecution* get();

	private:
		Latch(const Latch&);
		Latch& operator=(const Latch&);

		ClientId& clientId_;
		SQLExecutionManager* executionManager_;
		SQLExecution* execution_;
	};

	SQLExecutionManager(ConfigTable& config,
		SQLVariableSizeGlobalAllocator& globalVarAlloc);
	~SQLExecutionManager();

	void initialize(const ManagerSet& mgrSet);
	void remove(EventContext& ec, ClientId& clientId, bool withCheck, int32_t point, JobId* jobId);
	void remove(SQLExecution* execution);
	void closeConnection(EventContext& ec, const NodeDescriptor& nd);
	void setRequestedConnectionEnv(
		const RequestInfo& request,
		StatementHandler::ConnectionOption& connOption);
	void setConnectionEnv(
		SQLExecution* execution, uint32_t paramId, const char8_t* value);
	void setConnectionEnv(
		StatementHandler::ConnectionOption& connOption,
		uint32_t paramId, const char8_t* value);
	bool getConnectionEnv(
		SQLExecution* execution, uint32_t paramId, std::string& value,
		bool& hasData);
	bool getConnectionEnv(
		StatementHandler::ConnectionOption& connOption,
		uint32_t paramId, std::string& value, bool& hasData);
	void completeConnectionEnvUpdates(
		StatementHandler::ConnectionOption& connOption);
	uint32_t getConnectionEnvUpdates(
		const StatementHandler::ConnectionOption& connOption);
	void dump();
	void getDatabaseStats(util::StackAllocator& alloc, DatabaseId dbId, util::Map<DatabaseId, DatabaseStats*>& statsMap, bool isAdministrator);

	void dumpTableCache(util::NormalOStringStream& oss, const char* dbName);
	void resizeTableCache(const char* dbName, int32_t cacheSize);

	void getProfiler(util::StackAllocator& alloc, SQLProfiler& profs);
	void getCurrentClientIdList(util::Vector<ClientId>& clientIdList);
	size_t getCurrentClientIdListSize();
	void cancelAll(const Event::Source& eventSource,
		util::StackAllocator& alloc, util::Vector<ClientId>& clientIdList,
		CancelOption& cancelOption);

	void clearConnection(Event& ev, bool isNewSQL);
	void checkTimeout(EventContext& ec);

	SQLService* getSQLService();
	util::FixedSizeAllocator<util::Mutex>& getFixedAllocator();
	SQLVariableSizeGlobalAllocator& getVarAllocator();
	JobManager* getJobManager();

	void refreshSchemaCache();

	int64_t getFetchSizeLimit() {
		return fetchSizeLimit_;
	}
	int64_t getNoSQLSizeLimit() {
		return nosqlSizeLimit_;
	}

	DBConnection* getDBConnection();

	static class StatSetUpHandler : public StatTable::SetUpHandler {
		virtual void operator()(StatTable& stat);
	} statSetUpHandler_;

	void updateStat(StatTable& stat);
	void updateStatNormal(StatTable& stat);

	void incOperation(bool isSQL, int32_t workerId, int32_t count) {
		if (isSQL) {
			totalReadOperationList_[workerId] += count;
		}
		else {
			totalWriteOperationList_[workerId] += count;
		}
	}

	uint64_t getTotalReadOperation() {
		uint64_t total = 0;
		for (size_t pos = 0; pos < totalReadOperationList_.size(); pos++) {
			total += totalReadOperationList_[pos];
		}
		return total;
	}

	uint64_t getTotalWriteOperation() {
		uint64_t total = 0;
		for (size_t pos = 0; pos < totalWriteOperationList_.size(); pos++) {
			total += totalWriteOperationList_[pos];
		}
		return total;
	}

	uint64_t incNodeCounter() {
		return ++nodeCounter_;
	}

	void incInternalRetry() {
		++internalRetryCount_;
	}

	uint64_t getInternalRetry() {
		return internalRetryCount_;
	}

	int64_t incCacheHit() {
		tableCacheHitCount_++;
		return tableCacheHitCount_;
	}

	double getTableCacheRatio() {
		return (double)tableCacheHitCount_ / (double)(tableCacheHitCount_ + tableCacheMissHitCount_);
	}

	int64_t incCacheMissHit() {
		tableCacheMissHitCount_++;
		return tableCacheMissHitCount_;
	}

	void getCacheStat(int64_t& hit, int64_t& miss) {
		hit = tableCacheHitCount_;
		miss = tableCacheMissHitCount_;
	}

	void dumpCache(bool hit) {
		const char* str = "hit";
		if (!hit) {
			str = " miss";
		}
		std::cout << str << tableCacheHitCount_ << "," << tableCacheMissHitCount_ << std::endl;
	}

	void setCacheLoadStat(int32_t searchCount, int32_t skipCount, uint32_t loadTime) {
		tableCacheSearchCount_ += searchCount;
		tableCacheSkipCount_ += skipCount;
		tableCacheLoadTime_ += loadTime;
	}

	util::StackAllocator* getStackAllocator(util::AllocatorLimitter *limitter);
	void releaseStackAllocator(util::StackAllocator* alloc);
	const SQLProcessorConfig* getProcessorConfig() const;
	SQLConfigParam& getSQLConfig();

	PartitionTable* getPartitionTable();
	SQLIndexStatsCache& getIndexStats();

	int32_t getTableCacheDumpLimitTime() {
		return tableCacheDumpLimitTime_;
	}

	void setTableCacheDumpLimitTime(int32_t time) {
		tableCacheDumpLimitTime_ = time;
	}

	const ManagerSet* getManagerSet() {
		return mgrSet_;
	}

private:

	SQLExecution* create(ClientId& clientId, const NodeDescriptor* clientNd,
		RequestInfo* request);
	SQLExecution* get(ClientId& clientId);
	void release(SQLExecution* execution);

	util::Mutex lock_;
	util::Mutex stackAllocatorLock_;
	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	JobManager* jobManager_;
	DBConnection* dbConnection_;
	PartitionTable* pt_;
	SQLExecutionMap executionMap_;
	SQLIndexStatsCache indexStats_;
	util::FixedSizeAllocator<util::Mutex> fixedAllocator_;
	SQLProcessorConfig* processorConfig_;
	int64_t fetchSizeLimit_;
	int64_t nosqlSizeLimit_;
	int32_t tableCacheDumpLimitTime_;

	const ManagerSet* mgrSet_;
	SQLConfigParam config_;

	util::Atomic<uint64_t> nodeCounter_;
	std::vector<uint64_t> totalReadOperationList_;
	std::vector<uint64_t> totalWriteOperationList_;

	util::Atomic<uint64_t> internalRetryCount_;
	util::Atomic<uint64_t> queueSizeMax_;
	util::Atomic<uint64_t> tableCacheHitCount_;
	util::Atomic<uint64_t> tableCacheMissHitCount_;

	util::Atomic<int64_t> tableCacheSearchCount_;
	util::Atomic<int64_t> tableCacheSkipCount_;
	util::Atomic<int64_t> tableCacheLoadTime_;

	static class ConfigSetUpHandler : public ConfigTable::SetUpHandler {
		virtual void operator()(ConfigTable& config);
	} configSetUpHandler_;
};

typedef StatementId ExecutionId;

class SQLExecution {

	friend class DDLProcessor;
	struct ExecutionProfilerInfo;

	typedef SQLParsedInfo::BindInfo BindInfo;
	typedef StatementHandler::StatementExecStatus StatementExecStatus;
	typedef SyntaxTree::CreateTableOption CreateTableOption;
	typedef util::Vector<TupleList::TupleColumnType> ValueTypeList;
	typedef int32_t CommandId;
	struct Config;
	static const uint32_t CACHE_EXPIRED_MAX_TIME = 60 * 1000 * 10;
	static const int32_t FETCH_LIMIT_SIZE = 65535;

	enum SQLExecutionType {
		SQL_EXEC_INIT,
		SQL_EXEC_PREPARE,
		SQL_EXEC_BIND,
		SQL_EXEC_PREPARE_BIND,
		SQL_EXEC_FETCH,
		SQL_EXEC_CLOSE,
		SQL_EXEC_CANCEL
	};

public:

	enum SQLReplyType {
		SQL_REPLY_DENY,
		SQL_REPLY_ERROR,
		SQL_REPLY_INTERNAL,
	};

	class ErrorFormatter {
	public:
		ErrorFormatter(
			SQLExecution& execution, const std::exception& cause);
		void format(std::ostream& os) const;
	private:
		SQLExecution& execution_;
		const std::exception& cause_;
	};

	SQLExecution(JobManager* jobManager,
		SQLExecutionManager* execManager,
		SQLVariableSizeGlobalAllocator& globalVarAlloc,
		ClientId& clientId,
		const NodeDescriptor& clientNd,
		StatementHandler::ConnectionOption& connOption,
		RequestInfo& request,
		DBConnection* commandMgr);

	~SQLExecution();
	
	bool isBatch() {
		return isBatch_;
	}
	void setBatch() {
		isBatch_ = true;
	}
	
	void setBatchCount(int64_t count) {
		batchCountList_.push_back(count);
		batchCurrentPos_++;
	}

	bool isBatchComplete() {
		return (batchCount_ == batchCurrentPos_);
	}

	void execute(EventContext& ec,
		RequestInfo& request, bool prepareBinded,
		std::exception* e, uint8_t versionId, JobId* jobId);

	void cancel(EventContext& ec, ExecutionId execId = MAX_STATEMENTID);
	void cancel(const Event::Source& eventSource);
	bool fetch(EventContext& ec, SQLFetchContext& cxt,
		ExecutionId execId, uint8_t versionId);
	void fetch(EventContext& ec, Event& ev, RequestInfo& request);
	void executeRepair(EventContext& ec, util::String& containerName,
		ContainerId largeContainerId,
		SchemaVersionId versionId, NodeAffinityNumber affinity);
	void close(EventContext& ec, RequestInfo& request);
	SQLExecutionManager* getExecutionManager() {
		return executionManager_;
	}

	class PreparedInfo {
	public:
		PreparedInfo(SQLVariableSizeGlobalAllocator& globalVarAllocator) :
			globalVarAlloc_(globalVarAllocator),
			serializedPlan_(NULL), serializedPlanSize_(0),
			serializedBindInfo_(NULL), serializedBindInfoSize_(0) {}
		void cleanup();
		void copySerialized(util::XArray<uint8_t>& binary);
		void copyBind(util::XArray<uint8_t>& binary);
		void restoreBind(RequestInfo& request);
		void restorePlan(SQLPreparedPlan*& plan,
			util::StackAllocator& eventStackAlloc, TupleValue::VarContext& varCxt);
	private:

		SQLVariableSizeGlobalAllocator& globalVarAlloc_;
		char* serializedPlan_;
		int32_t serializedPlanSize_;
		char* serializedBindInfo_;
		int32_t serializedBindInfoSize_;
	};

	class ClientInfo {

		friend class SQLExecution;
		friend class SQLExecutionContext;
	public:
		ClientInfo(util::StackAllocator& alloc, SQLVariableSizeGlobalAllocator& globalVarAlloc,
			const NodeDescriptor& clientNd, ClientId& clientId,
			StatementHandler::ConnectionOption& connOption);
		void init(util::StackAllocator& alloc);

	private:
		SQLString userName_;
		SQLString dbName_;
		DatabaseId dbId_;
		SQLString normalizedDbName_;
		SQLString applicationName_;
		bool isAdministrator_;
		const NodeDescriptor clientNd_;
		ClientId& clientId_;
		double storeMemoryAgingSwapRate_;
		util::TimeZone timezone_;
		StatementHandler::ConnectionOption& connOption_;
		int32_t acceptableFeatureVersion_;
	};

	class QueryAnalyzedInfo {
		friend class SQLExecution;
		friend class JobManager;
	public:
		QueryAnalyzedInfo(SQLVariableSizeGlobalAllocator& globalVarAlloc,
			util::StackAllocator& eventStackAlloc) : parsedInfo_(eventStackAlloc),
			mergeSelectList_(eventStackAlloc),
			prepared_(false), fastInserted_(false),
			isAnalyzed_(false), isTimeSeriesIncluded_(false), query_(globalVarAlloc),
			notSelect_(false), isDDL_(false) {}

			SQLParsedInfo& getParsedInfo() {
				return parsedInfo_;
			}
	private:
		bool isPragmaStatement() {
			return (parsedInfo_.pragmaType_ != SQLPragma::PRAGMA_NONE);
		}
		bool isExplain() {
			return (parsedInfo_.explainType_ == SyntaxTree::EXPLAIN_PLAN);
		}
		bool isExplainAnalyze() {
			return (parsedInfo_.explainType_ == SyntaxTree::EXPLAIN_ANALYZE);
		}
		bool isExplainStatement() {
			return (isExplain() || isExplainAnalyze());
		}
		const std::string getTableNameList();

		SQLParsedInfo parsedInfo_;
		util::Vector<SyntaxTree::ExprList*> mergeSelectList_;
		bool prepared_;
		bool fastInserted_;
		bool isAnalyzed_;
		bool isTimeSeriesIncluded_;
		SQLString query_;
		bool notSelect_;
		bool isDDL_;
	};

	struct ExecutionStatus {
		ExecutionStatus(RequestInfo& request) :
			requested_(false), responsed_(false),
			currentType_(SQL_EXEC_INIT), execId_(0),
			pendingCancelId_(MAX_STATEMENTID), preparedPlan_(NULL),
			fetchStatus_(request.maxRows_),
			sqlCount_(0), connectedPId_(0), connectedPgId_(0),
			requestType_(UNDEF_REQUEST_TYPE) {
			startTime_ = util::DateTime::now(false).getUnixTime();
			endTime_ = 0;
		}

		void reset() {
			requested_ = false;
		}

		bool isFetched() {
			return (fetchStatus_.fetchRemain_ != fetchStatus_.origMaxRows_);
		}

		void setFetchRemain(int64_t remainNum) {
			fetchStatus_.fetchRemain_ = remainNum;
		}

		bool declFetchRemain(int64_t fetchCount) {
			fetchStatus_.fetchRemain_ -= fetchCount;
			return (fetchStatus_.fetchRemain_ == 0);
		}

		int64_t getFetchRemain() {
			return fetchStatus_.fetchRemain_;
		}

		void setMaxRows(int64_t maxRows) {
			fetchStatus_.origMaxRows_ = maxRows;
			reset();
		}

		bool requested_;
		bool responsed_;
		SQLExecutionType currentType_;
		ExecutionId execId_;
		ExecId pendingCancelId_;
		int64_t startTime_;
		int64_t endTime_;
		SQLPreparedPlan* preparedPlan_;

		void set(RequestInfo& request) {
			sqlCount_ = request.sqlCount_;
			connectedPId_ = request.pId_;
			connectedPgId_ = request.pgId_;
			requestType_ = request.requestType_;
		}

		struct FetchStatus {
			FetchStatus(int64_t maxRows) : origMaxRows_(maxRows),
				fetchRemain_(origMaxRows_) {}
			int64_t origMaxRows_;
			int64_t fetchRemain_;
			void reset() {
				fetchRemain_ = origMaxRows_;
			}
		};

		FetchStatus fetchStatus_;

		int32_t sqlCount_;
		PartitionId connectedPId_;
		PartitionGroupId connectedPgId_;
		SQLRequestType requestType_;
	};

	class SQLExecutionContext {
		friend class SQLExecution;
	public:
		SQLExecutionContext(util::Mutex& lock,
			ClientInfo& clientInfo,
			QueryAnalyzedInfo& analyzedQueryInfo,
			ExecutionStatus& executionStatus,
			ResponseInfo& response,
			ClientId& clientId,
			JobId& currentJobId) :
			lock_(lock),
			clientInfo_(clientInfo),
			analyzedQueryInfo_(analyzedQueryInfo),
			executionStatus_(executionStatus),
			response_(response),
			clientId_(clientId),
			syncContext_(NULL),
			currentJobId_(currentJobId),
			pendingExceptionExecId_(UNDEF_STATEMENTID)
			{}

		const char* getDBName() const;
		const char* getApplicationName() const;
		bool isSetApplicationName() const;

		const char* getQuery() const;
		bool isSetQuery() const;

		void getQuery(util::String& str, size_t size);
		void setQuery(util::String& query) const;
		DatabaseId getDBId() const;
		bool isAdministrator() const;
		const SQLString& getUserName() const;
		int64_t getStartTime() const;
		void setStartTime(int64_t startTime);

		int64_t getEndTime() const;
		void setEndTime(int64_t endTime);

		double getStoreMemoryAgingSwapRate() const;
		const util::TimeZone& getTimezone() const;

		ClientId& getId() const;
		const char* getNormalizedDBName();
		bool isRetryStatement() const;
		bool isPreparedStatement() const;
		
		const std::string getTableNameList() const;

		const NodeDescriptor& getClientNd() const;
		const std::string getClientAddress() const;
		ExecutionId getExecId();
		StatementHandler::ConnectionOption& getConnectionOption();
		NoSQLSyncContext& getSyncContext();
		SessionId incCurrentSessionId();
		SessionId getCurrentSessionId();
		void setCurrentJobId(JobId& jobId);
		void getCurrentJobId(JobId& jobId, bool isLocal = true);
		uint8_t getCurrentJobVersionId();
		void setPendingException(std::exception& e);
		void resetPendingException();
		bool checkPendingException(bool flag);

	private:

		util::Mutex& lock_;
		ClientInfo& clientInfo_;
		QueryAnalyzedInfo& analyzedQueryInfo_;
		ExecutionStatus& executionStatus_;
		ResponseInfo& response_;
		ClientId& clientId_;
		NoSQLSyncContext* syncContext_;
		JobId& currentJobId_;
		ExecId pendingExceptionExecId_;
		util::Exception pendingException_;
	};

	SQLExecutionContext& getContext() {
		return context_;
	}
	DataStoreV4* getDataStore(PartitionId pId);
	util::StackAllocator* getStackAllocator() { return &executionStackAlloc_; };

	struct SQLReplyContext {
		SQLReplyContext() : ec_(NULL), job_(NULL), typeList_(NULL),
			versionId_(0), responseJobId_(NULL),
			replyType_(SQL_REPLY_UNDEF), countList_(NULL) {}
		void setExplainAnalyze(EventContext* ec, Job* job,
			uint8_t versionId, JobId* responseJobId) {
			assert(ec);
			ec_ = ec;
			job_ = job;
			versionId_ = versionId;
			responseJobId_ = responseJobId;
			replyType_ = SQL_REPLY_SUCCESS_EXPLAIN_ANALYZE;
		}
		void setExplain(EventContext* ec) {
			assert(ec);
			ec_ = ec;
			replyType_ = SQL_REPLY_SUCCESS_EXPLAIN;
		}
		void setBatch(EventContext *ec, util::Vector<int64_t> *countList) {
			assert(ec);
			ec_ = ec;
			countList_ = countList;
			replyType_ = SQL_REPLY_SUCCESS_EXPLAIN;
		}
		void set(EventContext* ec,
			ValueTypeList* typeList, uint8_t versionId, JobId* responseJobId) {
			assert(ec);
			ec_ = ec;
			typeList_ = typeList;
			versionId_ = versionId;
			responseJobId_ = responseJobId;
			replyType_ = SQL_REPLY_SUCCESS;
		}
		EventContext* ec_;
		Job* job_;
		ValueTypeList* typeList_;
		uint8_t versionId_;
		JobId* responseJobId_;
		int32_t replyType_;
		util::Vector<int64_t> *countList_;
	};

	bool replyClient(SQLReplyContext& cxt);

	void latch() {
		latchCount_++;
	}

	bool unlatch() {
		latchCount_--;
		return (latchCount_ == 0);
	}

	bool isCancelled() {
		return isCancelled_;
	}

	static void updateRemoteCache(EventContext& ec, EventEngine* ee,
		PartitionTable* pt, DatabaseId dbId, const char* dbName,
		const char* tableName, TablePartitioningVersionId versionId);

	bool parseViewSelect(
		SQLExpandViewContext& cxt,
		SQLParsedInfo& viewParsedInfo,
		uint32_t viewDepth, int64_t viewNsId, int64_t maxViewNsId,
		util::String& viewSelectString);

	const SQLTableInfo* getTableInfo(
		SQLExpandViewContext& cxt,
		SyntaxTree::Expr* expr,
		uint32_t viewDepth, bool viewOnly = false);

	static SQLTableInfo* decodeTableInfo(
		util::StackAllocator& alloc, util::ArrayByteInStream& in);

	static void applyClusterPartitionCount(
		SQLTableInfo& tableInfo, uint32_t clusterPartitionCount);

	static void assignDistributedTarget(
		TransactionContext& txn,
		const util::Vector< std::pair<
		TupleList::TupleColumnType, util::String> >& columnInfoList,
		const Query& query, ResultSet& resultSet);

	static FullContainerKey* predicateToContainerKeyByTQL(
			TransactionContext &txn, DataStoreV4 &dataStore,
			const Query &query, DatabaseId dbId, ContainerId metaContainerId,
			PartitionId partitionCount, util::String &dbNameStr,
			bool &fullReduced, PartitionId &reducedPartitionId);

	void setRequest(RequestInfo& request);

	ExecutionProfilerInfo& getProfilerInfo() {
		return profilerInfo_;
	}
	
	SQLParsedInfo& getParsedInfo() {
		return analyzedQueryInfo_.parsedInfo_;
	}

	int64_t getTotalSize() {
		return executionStackAlloc_.getTotalSize();
	}

private:

	typedef TupleColumnTypeUtils ColumnTypeUtils;

	static SQLReplyType checkReplyType(int32_t errorCode, bool& clearCache);

	bool isDDL() {
		return analyzedQueryInfo_.isDDL_;
	}

	void setDDL() {
		analyzedQueryInfo_.isDDL_ = true;
	}

	void setNotSelect(bool value) {
		analyzedQueryInfo_.notSelect_ = value;
	}

	bool isNotSelect() {
		return analyzedQueryInfo_.notSelect_;
	}

	void setPrepared() {
		analyzedQueryInfo_.prepared_ = true;
	}

	void setFastInserted() {
		analyzedQueryInfo_.fastInserted_ = true;
	}

	bool isFastInserted() {
		return analyzedQueryInfo_.fastInserted_;
	}

	bool isTimeSeriesIncluded() {
		return analyzedQueryInfo_.isTimeSeriesIncluded_;
	}

	void setTimeSeriesIncluded(bool flag) {
		analyzedQueryInfo_.isTimeSeriesIncluded_ = flag;
	}

	util::Vector<SyntaxTree::ExprList*>& getMergeSelectList() {
		return analyzedQueryInfo_.mergeSelectList_;
	}

	bool isAnalyzed() {
		return analyzedQueryInfo_.fastInserted_;
	}

	void setAnalyzed() {
		analyzedQueryInfo_.fastInserted_ = true;
	}

	void resetCurrentJobInfo() {
		executionStatus_.fetchStatus_.reset();
	}
	void setFetchComplete() {
		response_.isContinue_ = false;
	}

	void updateJobVersion(JobId& jobId);
	void updateJobVersion();
	void resetJobVersion();

	bool parse(
		SQLExpandViewContext& cxt,
		RequestInfo& request,
		uint32_t viewDepth, int64_t viewNsId, int64_t maxViewNsId);

	bool checkFastInsert(util::Vector<BindParam*>& bindParamInfos);

	struct ReplyOption {
		ReplyOption() : typeList_(NULL), job_(NULL) {};
		ReplyOption(ValueTypeList* typeList, Job* job) : typeList_(typeList), job_(job) {};
		ValueTypeList* typeList_;
		Job* job_;
	};
	static const int32_t SQL_REPLY_UNDEF = -1;
	static const int32_t SQL_REPLY_SUCCESS = 0;
	static const int32_t SQL_REPLY_SUCCESS_EXPLAIN = 1;
	static const int32_t SQL_REPLY_SUCCESS_EXPLAIN_ANALYZE = 2;

	void setAnyTypeData(util::StackAllocator& alloc, const TupleValue& value);
	void generateBlobValue(const TupleValue& value, util::XArray<uint8_t>& blobs);
	void setNullValue(const TupleValue& value,
		util::StackAllocator& alloc, TupleList::Column& column);

	NoSQLStore* getNoSQLStore();

	bool handleError(EventContext& ec, std::exception& e,
		int32_t currentVersionId, int64_t& delayTime, RequestInfo &request);
	int32_t preparedExecute(int32_t versionId,
		bool isException, RequestInfo& request);

	void setupViewContext(SQLExpandViewContext& cxt,
		EventContext& ec, SQLConnectionControl& connectionControl,
		util::Set<util::String>& tableSet, util::Set<util::String>& viewSet,
		util::Vector<NodeId>& liveNodeIdList, bool useCache, bool withVersion);

	bool checkSelectList(SyntaxTree::ExprList* selectList,
		util::Vector<SyntaxTree::ExprList*>& mergeSelectList,
		int32_t& placeHolderCount);

	void updateConnection();

	bool replySuccess(EventContext& ec, ValueTypeList* typeList,
		uint8_t versionId, JobId* responseJobId);
	bool replySuccessExplain(EventContext& ec,
		uint8_t versionId, JobId* responseJobId);
	bool replyError(EventContext& ec,
		StatementHandler::StatementExecStatus status, std::exception& e);
	void encodeSuccess(util::StackAllocator& alloc,
		EventByteOutStream& out, ValueTypeList* typeList);
	void encodeSuccessExplain(util::StackAllocator& alloc,
		EventByteOutStream& out);
	void encodeSuccessExplainAnalyze(util::StackAllocator& alloc,
		EventByteOutStream& out, Job* job);

	void compile(
			EventContext &ec, SQLConnectionControl &connectionControl,
			ValueTypeList &parameterTypeList, SQLTableInfoList *tableInfoList,
			TupleValue::VarContext &varCxt, int64_t startTime,
			RequestInfo& request, bool prepared, bool first,
			bool isPreparedRetry);
	void compileSub(
			util::StackAllocator &alloc, SQLConnectionControl &connectionControl,
			ValueTypeList &parameterTypeList, SQLTableInfoList *tableInfoList,
			util::Vector<BindParam*> &bindParamInfos,
			TupleValue::VarContext &varCxt, int64_t startTime);

	bool replySuccessExplainAnalyze(
		EventContext& ec, Job* job, uint8_t versionId, JobId* responseJobId);

	bool replySuccessInternal(EventContext& ec, int32_t type,
		uint8_t versionId, ReplyOption& option, JobId* responseJobId);
	void cleanupPrevJob(EventContext& ec);
	void cleanupSerializedData();
	void copySerializedData(util::XArray<uint8_t>& binary);
	void copySerializedBindInfo(util::XArray<uint8_t>& binary);

	void restoreBindInfo(RequestInfo& request);
	void restorePlanInfo(SQLPreparedPlan*& plan,
		util::StackAllocator& eventStackAlloc, TupleValue::VarContext& varCxt);
	void cancelPrevJob(EventContext& ec);
	bool fastInsert(EventContext& ec, util::Vector<BindParam*>& setInfo, bool useCache);
	void checkExecutableJob(ExecId targetStatementId);
	void checkWritableTable(SQLConnectionControl& control,
		TableSchemaInfo* tableSchema, SQLTableInfo& tableInfo);

	PartitionId getConnectedPId() {
		return executionStatus_.connectedPId_;
	}
	PartitionGroupId getConnectedPgId() {
		return executionStatus_.connectedPgId_;
	}

	void sendClient(EventContext& ec, Event& ev);
	void sendServer(EventContext& ec, Event& ev);

	bool checkJobVersionId(uint8_t versionId, bool isFetch, JobId* jobId);

	util::StackAllocator* wrapStackAllocator();

	void executeCancel(EventContext& ec, bool clientCancel = false);

	SQLTableInfoList* generateTableInfo(SQLExpandViewContext& cxt);
	SQLTableInfoList* createEmptyTableInfo(util::StackAllocator &alloc);

	void clearIndexStatsRequester();
	bool checkIndexStatsResolved();
	void resolveIndexStats(EventContext &ec);
	SQLIndexStatsCache& getIndexStats();

	void generateJobInfo(util::StackAllocator& alloc,
		SQLTableInfoList* tableInfoList,
		SQLPreparedPlan* pPlan,
		JobInfo* jobInfo, CommandId commandId,
		EventMonotonicTime emNow,
		SQLConnectionControl& control,
		util::Vector<NodeId>& liveNodeIdList, int32_t maxNodeNum);

	void generateJobId(JobId& jobId, ExecId execId);

	void setPreparedOption(RequestInfo& info, bool isRetry);

	bool fetchBatch(
		EventContext &ec, std::vector<int64_t> *countList);

	bool fetchNotSelect(EventContext& ec,
		TupleList::Reader* reader, TupleList::Column* columnList);

	bool fetchSelect(EventContext& ec,
		TupleList::Reader* reader, TupleList::Column* columnList,
		size_t columnSize, bool isCompleted);

	void setResponse();
	void checkCancel();
	void checkClientResponse();
	bool isCurrentTimeRequired();
	bool executeFastInsert(EventContext& ec,
		util::Vector<BindParam*>& bindParamInfos, bool useCache);

	void cleanupTableCache();
	void checkConcurrency(EventContext& ec, std::exception* e);
	bool doAfterParse(RequestInfo& request);
	void checkBatchRequest(RequestInfo& request);

	bool checkRetryError(SQLReplyType replyType, int32_t currentVersionId) {
		return (replyType == SQL_REPLY_INTERNAL && currentVersionId >= 1);
	}

	bool checkExecutionConstraint(uint32_t& delayStartTime, uint32_t& delayExecutionTime);

	void setupSyncContext();
	bool metaDataQueryCheck(SQLConnectionControl* control);
	bool checkRefreshTable(EventContext& ec, NameWithCaseSensitivity& name,
		util::StackAllocator& alloc,
		TableSchemaInfo* tableSchema, EventMonotonicTime currentEmTime);

	TupleValue getTupleValue(TupleValue::VarContext& varCxt,
		SyntaxTree::Expr* targetExpr,
		TupleList::TupleColumnType tupleType,
		util::Vector<BindParam*>& bindParamInfos);

	void checkColumnTypeFeatureVersion(ColumnType columnType);

	struct Config {
		Config(RequestInfo& request) {
			typedef StatementMessage::Options Options;
			fetchCount_ = request.option_.get<Options::FETCH_SIZE>();
			maxRows_ = request.option_.get<Options::MAX_ROWS>();
			queryTimeout_ = request.option_.get<
				Options::STATEMENT_TIMEOUT_INTERVAL>();
			txnTimeout_ = request.option_.get<
				Options::TXN_TIMEOUT_INTERVAL>();
		}
		int64_t fetchCount_;
		int64_t maxRows_;
		int32_t queryTimeout_;
		int32_t txnTimeout_;
	};

	util::Mutex lock_;
	int64_t latchCount_;
	ClientId clientId_;

	JobManager* jobManager_;
	SQLExecutionManager* executionManager_;
	DBConnection* dbConnection_;

	util::StackAllocator& executionStackAlloc_;
	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	SQLVariableSizeLocalAllocator& localVarAlloc_;
	util::StackAllocator::Scope* scope_;

	LocalTempStore::Group group_;
	SQLResultSet* resultSet_;
	ExecutionStatus executionStatus_;
	Config config_;
	NoSQLSyncContext* syncContext_;
	ResponseInfo response_;

	struct ExecutionProfilerInfo {
		ExecutionProfilerInfo() : parseAndGetTableTime_(0), compileTime_(0), nosqlPendingTime_(0) {}
		void init() {
			parseAndGetTableTime_ = 0;
			compileTime_ = 0;
			nosqlPendingTime_ = 0;
		}
		void incPendingTime(int64_t lap) {
			nosqlPendingTime_ += lap;
		}

		int64_t parseAndGetTableTime_;
		int64_t compileTime_;
		int64_t nosqlPendingTime_;
	};

	class ExecutionProfilerWatcher {
	public:
		static const int32_t PARSE = 0;
		static const int32_t COMPILE = 1;

		ExecutionProfilerWatcher(SQLExecution* execution, bool isNormal, bool trace) :
			profilerInfo_(execution->getProfilerInfo()), execution_(execution), isNormal_(isNormal), trace_(trace) {
			profilerInfo_.init();
			if (isNormal) {
				watch_.reset();
				watch_.start();
			}
		}

		~ExecutionProfilerWatcher();

		void lap(int32_t point) {
			int64_t elapsed = watch_.elapsedMillis();
			watch_.reset();
			watch_.start();
			switch (point) {
			case PARSE:
				profilerInfo_.parseAndGetTableTime_ = elapsed;
				break;
			case COMPILE:
				profilerInfo_.compileTime_ = elapsed;
				break;
			default: break;
			}
		}

	private:
		ExecutionProfilerInfo& profilerInfo_;
		SQLExecution* execution_;
		bool isNormal_;
		bool trace_;
		util::Stopwatch watch_;
	};

	ExecutionProfilerInfo profilerInfo_;
	util::Atomic<bool> isCancelled_;

	JobId currentJobId_;
	PreparedInfo preparedInfo_;
	ClientInfo clientInfo_;
	QueryAnalyzedInfo analyzedQueryInfo_;
	SQLExecutionContext context_;

	bool isBatch_;
	std::vector<int64_t> batchCountList_;
	int64_t batchCount_;
	int64_t batchCurrentPos_;
	BindParamSet bindParamSet_;
	SQLIndexStatsCache::RequesterHolder indexStatsRequester_;
};

struct SQLProfilerInfo {
	SQLProfilerInfo(util::StackAllocator& alloc) :
		startTime_(alloc), endTime_(alloc), sql_(alloc), clientId_(alloc),
		jobId_(alloc), queryType_(alloc),
		deployCompletedCount_(0),
		executeCompletedCount_(0), applicationName_(alloc), dbName_(alloc),
		parseAndGetTableTime_(0), compileTime_(0), nosqlPendingTime_(0) {}

	util::String startTime_;
	util::String endTime_;
	util::String sql_;
	util::String clientId_;
	util::String jobId_;
	util::String queryType_;
	int32_t deployCompletedCount_;
	int32_t executeCompletedCount_;
	util::String applicationName_;
	util::String dbName_;
	int64_t parseAndGetTableTime_;
	int64_t compileTime_;
	int64_t nosqlPendingTime_;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(startTime_, endTime_, sql_,
		clientId_, jobId_, queryType_, deployCompletedCount_,
		executeCompletedCount_, applicationName_, dbName_,
		parseAndGetTableTime_, compileTime_, nosqlPendingTime_);
};

struct SQLProfiler {
	SQLProfiler(util::StackAllocator& alloc) : alloc_(alloc), sqlProfiler_(alloc) {}
	util::StackAllocator& alloc_;
	util::Vector<SQLProfilerInfo> sqlProfiler_;
	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(sqlProfiler_);
};

#endif
