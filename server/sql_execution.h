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
#include "sql_result_set.h"
#include "sql_request_info.h"
#include "cluster_common.h"
#include "sql_job_common.h"
#include "sql_resource_manager.h"

class SQLExecution;
class SQLTableInfoList;
class NoSQLDB;
class NoSQLStore;
struct BindParam;
struct SQLProfilerInfo;
struct SQLProfiler;
struct NoSQLSyncContext;
class SQLPreparedPlan;
struct TableSchemaInfo;
class Job;
struct SQLFetchContext;
class SQLExecutionManager;

class Query;
class ResultSet;
class ResourceSet;

struct ResponseInfo {

	ResponseInfo() :
			stmtId_(UNDEF_STATEMENTID),
			transactionStarted_(false),
			isAutoCommit_(true),
			isRetrying_(false),
			resultCount_(1),
			updateCount_(0),
			parameterCount_(0),
			existsResult_(false),
			isContinue_(false),
			resultSet_(NULL),
			prepareBinded_(false) {}

	void reset();
	
	void set(RequestInfo &request, SQLResultSet *resultSet);
	
	StatementId stmtId_;
	bool transactionStarted_;
	bool isAutoCommit_;
	bool isRetrying_;
	int64_t resultCount_;
	int64_t updateCount_;
	int32_t parameterCount_;
	bool existsResult_;
	bool isContinue_;
	SQLResultSet *resultSet_;
	bool prepareBinded_;
};

struct SQLExpandViewContext {

	SQLExpandViewContext() :
			alloc_(NULL),
			control_(NULL),
			parsedInfo_(NULL),
			defaultDbName_(NULL),
			ec_(NULL),
			tableInfoList_(NULL),
			tableSet_(NULL),
			liveNodeIdList_(NULL),
			viewSet_(NULL),
			useCache_(true),
			withVersion_(true),
			forceMetaDataQuery_(false),
			isMetaDataQuery_(false),
			metaVisible_(false),
			internalMode_(false),
			forDriver_(false) {}

	util::StackAllocator *alloc_;
	SQLConnectionEnvironment  *control_;
	SQLParsedInfo *parsedInfo_;
	const char8_t *defaultDbName_;
	EventContext *ec_;
	SQLTableInfoList *tableInfoList_;
	util::Set<util::String> *tableSet_;
	util::Vector<NodeId> *liveNodeIdList_;
	util::Set<util::String> *viewSet_;

	void setAllocator(util::StackAllocator *alloc) {
		alloc_ = alloc;
	}
	
	util::StackAllocator &getAllocator() {
		return *alloc_;
	}

	bool useCache_;
	bool withVersion_;
	bool forceMetaDataQuery_;
	bool isMetaDataQuery_;
	bool metaVisible_;
	bool internalMode_;
	bool forDriver_;
};

class SQLExecution  : public ResourceBase {

	friend class DDLProcessor;
	typedef uint8_t StatementExecStatus;  

	typedef SQLParsedInfo::BindInfo BindInfo;
	typedef SyntaxTree::CreateTableOption CreateTableOption;
	typedef util::Vector<TupleList::TupleColumnType> ValueTypeList;
	struct Config;
	static const uint32_t CACHE_EXPIRED_MAX_TIME = 60*1000*10;
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
				SQLExecution &execution, const std::exception &cause);
		void format(std::ostream &os) const;

	private:
		
		SQLExecution &execution_;
		const std::exception &cause_;
	};

	SQLExecution(SQLExecutionRequestInfo &executionRequest);

	~SQLExecution();

	void execute(
			EventContext &ec,
			RequestInfo &request,
			bool prepareBinded,
			std::exception *e,
			uint8_t versionId,
			JobId *jobId);

	void cancel(JobExecutionId execId = MAX_STATEMENTID);

	void cancel(const Event::Source &eventSource);
	
	bool fetch(EventContext &ec, SQLFetchContext &cxt,
			JobExecutionId execId, uint8_t versionId);
	
	void fetch(EventContext &ec, RequestInfo &request);
	
	void executeRepair(
			EventContext &ec,
			util::String &containerName,
			ContainerId largeContainerId,
			SchemaVersionId versionId,
			NodeAffinityNumber affinity);
	
	void close(EventContext &ec, RequestInfo &request);
	
	class PreparedInfo {

	public:
		
		PreparedInfo(SQLVariableSizeGlobalAllocator &globalVarAllocator) :
				globalVarAlloc_(globalVarAllocator),
				serializedPlan_(NULL),
				serializedPlanSize_(0),
				serializedBindInfo_(NULL),
				serializedBindInfoSize_(0) {}

		void cleanup();
		
		void copySerialized(util::XArray<uint8_t> &binary);
		
		void copyBind(util::XArray<uint8_t> &binary);
		
		void restoreBind(RequestInfo &request);
		
		void restorePlan(SQLPreparedPlan *&plan,
				util::StackAllocator &alloc,
				TupleValue::VarContext &varCxt);

	private:

		SQLVariableSizeGlobalAllocator &globalVarAlloc_;
		
		char *serializedPlan_;
		size_t serializedPlanSize_;
		char *serializedBindInfo_;
		size_t serializedBindInfoSize_;
	};


	class QueryAnalyzedInfo {
	friend class SQLExecution;
	
	public:

		QueryAnalyzedInfo(
				SQLVariableSizeGlobalAllocator &globalVarAlloc,
				util::StackAllocator &alloc) :
						parsedInfo_(alloc),
						mergeSelectList_(alloc),
						prepared_(false),
						fastInserted_(false),
						isAnalyzed_(false),
						isTimeSeriesIncluded_(false),
						query_(globalVarAlloc),
						notSelect_(false),
						isDDL_(false) {}

	private:

		bool isPragmaStatement() {
			return (parsedInfo_.pragmaType_
					!= SQLPragma::PRAGMA_NONE);
		}

		bool isExplain() {
			return (parsedInfo_.explainType_
					== SyntaxTree::EXPLAIN_PLAN);
		}

		bool isExplainAnalyze() {
			return (parsedInfo_.explainType_
					== SyntaxTree::EXPLAIN_ANALYZE);
		}

		bool isExplainStatement() {
			return (isExplain() || isExplainAnalyze());
		}

		SQLParsedInfo parsedInfo_;
		util::Vector<SyntaxTree::ExprList *> mergeSelectList_;
		bool prepared_;
		bool fastInserted_;
		bool isAnalyzed_;
		bool isTimeSeriesIncluded_;
		SQLString query_;
		bool notSelect_;
		bool isDDL_;
	};

	struct ExecutionStatus {

		ExecutionStatus(RequestInfo &request) :
				requested_(false),
				responsed_(false), 
				currentType_(SQL_EXEC_INIT),
				execId_(0),
				pendingCancelId_(MAX_STATEMENTID),
				preparedPlan_(NULL),
				sqlCount_(0),
				connectedPId_(0),
				connectedPgId_(0),
				requestType_(UNDEF_REQUEST_TYPE),
				fetchStatus_(request.maxRows_) {

				startTime_ = util::DateTime::now(false).getUnixTime();
		}
		
		void reset() {
			requested_ = false;
		}
		
		bool isFetched() {
			return (fetchStatus_.fetchRemain_
					!= fetchStatus_.origMaxRows_);
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
		JobExecutionId execId_;
		JobExecutionId pendingCancelId_;
		int64_t startTime_;
		SQLPreparedPlan *preparedPlan_;

		int32_t sqlCount_;
		PartitionId connectedPId_;
		PartitionGroupId connectedPgId_;
		SQLRequestType requestType_;

		void set(RequestInfo &request) {
			sqlCount_ = request.sqlCount_;
			connectedPId_ = request.pId_;
			connectedPgId_ = request.pgId_;
			requestType_ = request.requestType_;
		}

		struct FetchStatus {
			
			FetchStatus(int64_t maxRows) :
					origMaxRows_(maxRows),
					fetchRemain_(origMaxRows_) {}

			void reset() {
				fetchRemain_ = origMaxRows_;
			}

			int64_t origMaxRows_;
			int64_t fetchRemain_;
		};

		FetchStatus fetchStatus_;
	};

	class SQLExecutionContext {
	friend class SQLExecution;
	public:
		
		SQLExecutionContext(
				const ResourceSet *resourceSet,
				util::Mutex &lock,
				ClientInfo &clientInfo,
				QueryAnalyzedInfo &analyedQueryInfo,
				ExecutionStatus &executionStatus,
				ResponseInfo &response,
				ClientId &clientId,
				JobId &currentJobId):
						resourceSet_(resourceSet),
						lock_(lock),
						clientInfo_(clientInfo),
						analyedQueryInfo_(analyedQueryInfo),
						executionStatus_(executionStatus),
						response_(response),
						clientId_(clientId),
						syncContext_(NULL),
						currentJobId_(currentJobId) {}

	const char *getDBName() const;

	const char *getApplicationName() const;

	const char *getQuery() const;

	void setQuery(util::String &query) const;

	DatabaseId getDBId() const;

	bool isAdministrator() const;

	const SQLString &getUserName() const;

	int64_t getStartTime() const;

	void setStartTime(int64_t startTime);

	double getStoreMemoryAgingSwapRate() const;

	const util::TimeZone &getTimezone() const;

	ClientId &getId() const;

	const char *getNormalizedDBName();

	bool isRetryStatement() const;

	bool isPreparedStatement() const;

	const NodeDescriptor &getClientNd() const;

	JobExecutionId getExecId();

	template<typename T>
	T &getConnectionOption();

	NoSQLSyncContext &getSyncContext();

	SessionId incCurrentSessionId();

	SessionId getCurrentSessionId();
	
	void setCurrentJobId(JobId &jobId);
	
	void getCurrentJobId(JobId &jobId, bool isLocal = true);
	
	uint8_t getCurrentJobVersionId();

	private:
	
		const ResourceSet *resourceSet_;
		util::Mutex &lock_;

		ClientInfo &clientInfo_;
		QueryAnalyzedInfo &analyedQueryInfo_;
		ExecutionStatus &executionStatus_;
		ResponseInfo &response_;
		ClientId &clientId_;
		NoSQLSyncContext *syncContext_;
		JobId &currentJobId_;
	};
	SQLExecutionContext &getContext()  {
		return context_;
	}

	const ResourceSet *getResourceSet() {
		return resourceSet_;
	}

	struct SQLReplyContext {

		SQLReplyContext() :
				ec_(NULL),
				job_(NULL),
				typeList_(NULL),
				versionId_(0),
				responseJobId_(NULL),
				replyType_(SQL_REPLY_UNDEF) {}

		void setExplainAnalyze(
				EventContext *ec,
				Job *job,
				uint8_t versionId,
				JobId *responseJobId) {

			assert(ec);
			ec_ = ec;
			job_ = job;
			versionId_ = versionId;
			responseJobId_ = responseJobId;
			replyType_ = SQL_REPLY_SUCCESS_EXPLAIN_ANALYZE;
		}

		void setExplain(EventContext *ec) {
			assert(ec);
			ec_ = ec;
			replyType_ = SQL_REPLY_SUCCESS_EXPLAIN;
		}

		void set(EventContext *ec,
				ValueTypeList *typeList,
				uint8_t versionId,
				JobId *responseJobId) {

			assert(ec);
			ec_ = ec;
			typeList_ = typeList;
			versionId_ = versionId;
			responseJobId_ = responseJobId;
			replyType_ = SQL_REPLY_SUCCESS;
		}

		EventContext *ec_;
		Job *job_;
		ValueTypeList *typeList_;
		uint8_t versionId_;
		JobId *responseJobId_;
		int32_t replyType_;
	};

	bool replyClient(SQLReplyContext &cxt);

	bool isCancelled() {
		return isCancelled_;
	}

	static void updateRemoteCache(
			EventContext &ec,
			EventEngine *ee,
			const ResourceSet *resourceSet,
			DatabaseId dbId,
			const char *dbName,
			const char *tableName,
			TablePartitioningVersionId versionId);

	bool parseViewSelect(
			SQLExpandViewContext &cxt,
			SQLParsedInfo &viewParsedInfo,
			uint32_t viewDepth,
			int64_t viewNsId,
			int64_t maxViewNsId,
			util::String &viewSelectString);

	const SQLTableInfo* getTableInfo(
			SQLExpandViewContext &cxt,
			SyntaxTree::Expr *expr,
			uint32_t viewDepth,
			bool viewOnly = false);

	static SQLTableInfo* decodeTableInfo(
			util::StackAllocator &alloc,
			util::ArrayByteInStream &in);

	static void applyClusterPartitionCount(
			SQLTableInfo &tableInfo,
			uint32_t clusterPartitionCount);

	void setRequest(RequestInfo &request);

	SQLExecutionManager *getExecutionManager() {
		return executionManager_;
	}

private:

	static SQLReplyType checkReplyType(
			int32_t errorCode, bool &clearCache);

	bool isDDL() {
		return analyedQueryInfo_.isDDL_;
	}
	
	void setDDL() {
		analyedQueryInfo_.isDDL_ = true;
	}

	void setNotSelect(bool value) {
		analyedQueryInfo_.notSelect_ = value;
	}

	bool isNotSelect() {
		return analyedQueryInfo_.notSelect_;
	}

	void setPrepared() {
		analyedQueryInfo_.prepared_ = true;
	}

	void setFastInserted() {
		analyedQueryInfo_.fastInserted_ = true;
	}

	bool isFastInserted() {
		return analyedQueryInfo_.fastInserted_;
	}

	bool isTimeSeriesIncluded() {
		return analyedQueryInfo_.isTimeSeriesIncluded_;
	}

	void setTimeSeriesIncluded(bool flag) {
		analyedQueryInfo_.isTimeSeriesIncluded_ = flag;
	}

	SQLParsedInfo &getParsedInfo() {
		return analyedQueryInfo_.parsedInfo_;
	}
	
	util::Vector<SyntaxTree::ExprList *> &getMergeSelectList() {
		return analyedQueryInfo_.mergeSelectList_;
	}

	bool isAnalyzed() {
		return analyedQueryInfo_.fastInserted_;
	}

	void setAnalyzed() {
		analyedQueryInfo_.fastInserted_ = true;
	}

	void resetCurrentJobInfo() {
		executionStatus_.fetchStatus_.reset();
	}

	void setFetchComplete() {
		response_.isContinue_ = false;
	}

	void updateJobVersion(JobId &jobId);

	void updateJobVersion();
	
	void resetJobVersion();

	bool parse(
			SQLExpandViewContext &cxt,
			RequestInfo &request,
			uint32_t viewDepth,
			int64_t viewNsId,
			int64_t maxViewNsId);

	bool checkFastInsert(
			util::Vector<BindParam*> &bindParamInfos);

	struct ReplyOption {

		ReplyOption() :
				typeList_(NULL),
				job_(NULL) {};

		ReplyOption(ValueTypeList *typeList, Job *job) :
				typeList_(typeList),
				job_(job) {};

		ValueTypeList *typeList_;
		Job *job_;
	};

	static const int32_t SQL_REPLY_UNDEF = -1;
	static const int32_t SQL_REPLY_SUCCESS = 0;
	static const int32_t SQL_REPLY_SUCCESS_EXPLAIN = 1;
	static const int32_t SQL_REPLY_SUCCESS_EXPLAIN_ANALYZE = 2;

	void setAnyTypeData(
			util::StackAllocator &alloc, const TupleValue &value);

	void generateBlobValue(
			const TupleValue &value, util::XArray<uint8_t> &blobs);

	void setNullValue(const TupleValue &value,
			util::StackAllocator &alloc, TupleList::Column &column);

	NoSQLStore *getNoSQLStore();

	bool handleError(
			EventContext &ec, std::exception &e,
			int32_t currentVersionId, int64_t &delayTime);

	void setupViewContext(
			SQLExpandViewContext &cxt,
			EventContext &ec,
			SQLConnectionEnvironment &connectionControl,
			util::Set<util::String> &tableSet,
			util::Set<util::String> &viewSet,
			util::Vector<NodeId> &liveNodeIdList,
			bool useCache,
			bool withVersion);

	bool checkSelectList(
			SyntaxTree::ExprList *selectList,
			util::Vector<SyntaxTree::ExprList *> &mergeSelectList,
			int32_t &placeHolderCount);

	void updateConnection();

	bool replySuccess(
			EventContext &ec,
			ValueTypeList *typeList,
			uint8_t versionId,
			JobId *responseJobId);

	bool replySuccessExplain(
			EventContext &ec,
			uint8_t versionId,
			JobId *responseJobId);
	
	bool replyError(
			EventContext &ec,
			StatementExecStatus status,
			std::exception &e);
	
	void encodeSuccess(
			util::StackAllocator &alloc,
			EventByteOutStream &out,
			ValueTypeList *typeList);
	
	void encodeSuccessExplain(
			util::StackAllocator &alloc,
			EventByteOutStream &out);
	
	void encodeSuccessExplainAnalyze(
			util::StackAllocator &alloc,
			EventByteOutStream &out,
			Job *job);

	void compile(
			util::StackAllocator &alloc,
			SQLConnectionEnvironment  &contol,
			ValueTypeList &parameterTypeList,
			SQLTableInfoList *tableInfoList,
			util::Vector<BindParam*> &bindParamInfos,
			TupleValue::VarContext &varCxt);

	bool replySuccessExplainAnalyze(
			EventContext &ec,
			Job *job,
			uint8_t versionId,
			JobId *responseJobId);

	bool replySuccessInternal(
			EventContext &ec,
			int32_t type,
			uint8_t versionId,
			ReplyOption &option,
			JobId *responseJobId);

	void cleanupPrevJob();
	
	void cleanupSerializedData();
	
	void copySerializedData(util::XArray<uint8_t> &binary);

	void copySerializedBindInfo(util::XArray<uint8_t> &binary);

	void restoreBindInfo(RequestInfo &request);
	
	void restorePlanInfo(
			SQLPreparedPlan *&plan,
			util::StackAllocator &alloc,
			TupleValue::VarContext &varCxt);

	void cancelPrevJob(EventContext &ec);

	bool fastInsert(
			EventContext &ec,
			util::Vector<BindParam*> &setInfo,
			bool useCache);

	void checkExecutableJob(JobExecutionId targetStatementId);

	void checkWritableTable(
			SQLConnectionEnvironment  &control,
			TableSchemaInfo *tableSchema,
			SQLTableInfo &tableInfo);

	PartitionId getConnectedPId() {
		return executionStatus_.connectedPId_;
	}

	PartitionGroupId getConnectedPgId() {
		return executionStatus_.connectedPgId_;
	}

	void sendClient(EventContext &ec, Event &ev);
	
	void sendServer(EventContext &ec, Event &ev);

	bool checkJobVersionId(
			uint8_t versionId, bool isFetch, JobId *jobId);

	util::StackAllocator *wrapStackAllocator();

	void executeCancel();

	SQLTableInfoList* generateTableInfo(
			SQLExpandViewContext &cxt);

	void generateJobInfo(
			util::StackAllocator &alloc,
			SQLTableInfoList *tableInfoList,
			SQLPreparedPlan *pPlan,
			JobInfo *jobInfo,
			EventMonotonicTime emNow, 
			util::Vector<NodeId> &liveNodeIdList,
			int32_t maxNodeNum);

	void generateJobId(JobId &jobId, ExecId execId);

	void setPreparedOption(RequestInfo &info);

	bool fetchNotSelect(
			EventContext &ec,
			TupleList::Reader *reader,
			TupleList::Column *columnList);

	bool fetchSelect(
			EventContext &ec,
			TupleList::Reader *reader,
			TupleList::Column *columnList,
			size_t columnSize,
			bool isCompleted);

	void setResponse();

	void checkCancel();

	void checkClientResponse();

	bool isCurrentTimeRequired();

	bool executeFastInsert(
			EventContext &ec,
			util::Vector<BindParam*> &bindParamInfos,
			bool useCache);

	void cleanupTableCache();

	void checkConcurrency(
			EventContext &ec, std::exception *e);
	
	bool doAfterParse(RequestInfo &request);

	bool checkRetryError(
			SQLReplyType replyType,
			int32_t currentVersionId) {

		return (replyType == SQL_REPLY_INTERNAL
				&& currentVersionId >= 1);
	}

	void setupSyncContext();

	bool metaDataQueryCheck(SQLConnectionEnvironment  *control);

	bool checkRefreshTable(
			EventContext &ec,
			NameWithCaseSensitivity &name,
			TableSchemaInfo *tableSchema,
			EventMonotonicTime currentEmTime);

	TupleValue getTupleValue(
			TupleValue::VarContext &varCxt,
			SyntaxTree::Expr *targetExpr,
			TupleList::TupleColumnType tupleType,
			util::Vector<BindParam*> &bindParamInfos);

	util::Mutex lock_;
	ClientId clientId_;
	const ResourceSet *resourceSet_;

	JobManager *jobManager_;
	SQLExecutionManager *executionManager_;
	NoSQLDB *db_;

	util::StackAllocator &executionStackAlloc_;
	SQLVariableSizeGlobalAllocator &globalVarAlloc_;
	SQLVariableSizeLocalAllocator &localVarAlloc_;
	util::StackAllocator::Scope *scope_;

	LocalTempStore::Group group_;
	SQLResultSet *resultSet_;
	ResponseInfo response_;

	util::Atomic<bool> isCancelled_;

	JobId currentJobId_;

	PreparedInfo preparedInfo_;
	ClientInfo clientInfo_;
	QueryAnalyzedInfo analyedQueryInfo_;
	ExecutionStatus executionStatus_;

	struct Config {

		Config(RequestInfo &request)  {

			typedef StatementMessage::Options Options;
			fetchCount_ = request.option_.get<Options::FETCH_SIZE>();
			maxRows_ =  request.option_.get<Options::MAX_ROWS>();
			queryTimeout_ = request.option_.get<
					Options::STATEMENT_TIMEOUT_INTERVAL>();
			txnTimeout_ = request.option_.get<
					Options::TXN_TIMEOUT_INTERVAL>();
			if (txnTimeout_ == -1) {
				txnTimeout_ = TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL * 1000;
			}
			else {
				txnTimeout_ = txnTimeout_ * 1000;
			}
		}

		int64_t fetchCount_;
		int64_t maxRows_;
		int32_t queryTimeout_;
		int32_t txnTimeout_;
	};
	Config config_;

	SQLExecutionContext context_;

};

struct SQLProfilerInfo {

	SQLProfilerInfo(util::StackAllocator &alloc) :
			startTime_(alloc),
			sql_(alloc),
			clientId_(alloc),
			queryType_(alloc) {}

	util::String startTime_;
	util::String sql_;
	util::String clientId_;
	util::String queryType_;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(
			startTime_, sql_, clientId_, queryType_);
};

struct SQLProfiler {

	SQLProfiler(util::StackAllocator &alloc) :
			alloc_(alloc),
			sqlProfiler_(alloc) {}

	util::StackAllocator &alloc_;
	util::Vector<SQLProfilerInfo> sqlProfiler_;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(sqlProfiler_);
};

#endif
