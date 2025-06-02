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
#include "sql_execution.h"
#include "sql_job_manager.h"
#include "sql_compiler.h"
#include "sql_processor.h"
#include "sql_processor_dml.h"
#include "sql_processor_result.h"
#include "sql_command_manager.h"
#include "sql_service.h"
#include "sql_utils.h"
#include "database_manager.h"

UTIL_TRACER_DECLARE(SQL_SERVICE);
UTIL_TRACER_DECLARE(DISTRIBUTED_FRAMEWORK);
UTIL_TRACER_DECLARE(SQL_DETAIL);
UTIL_TRACER_DECLARE(DISTRIBUTED_FRAMEWORK_DETAIL);
UTIL_TRACER_DECLARE(SQL_INTERNAL);
AUDIT_TRACER_DECLARE(AUDIT_CONNECT);


const size_t SQLExecutionManager::DEFAULT_ALLOCATOR_BLOCK_SIZE = 20;
const size_t SQLExecutionManager::EXECUTION_FREE_LIMIT_SIZE = 32 * 1024 * 1024;
const int32_t SQLExecutionManager::DEFAULT_WORK_CACHE_MEMORY_MB = 128;
const int32_t SQLExecutionManager::DEFAULT_TABLE_CACHE_SIZE = 512;

SQLExecutionManager::ConfigSetUpHandler
SQLExecutionManager::configSetUpHandler_;

#define TRACE_SQL_START(jobId, hostName, processorName) \
	GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK_DETAIL, \
			GS_TRACE_SQL_INTERNAL_DEBUG, \
			" " << jobId << " " << \
			" " << hostName << " " << \
			" SQL 0 START IN 0 " << \
			processorName \
			<< " PIPE");

#define TRACE_SQL_END(jobId, hostName, processorName) \
	GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK_DETAIL, \
			GS_TRACE_SQL_INTERNAL_DEBUG, \
			" " << jobId << " " << \
			" " << hostName << " " << \
			" SQL 0 END IN 0 " << \
			processorName \
			<< " PIPE");

AUDIT_TRACER_DECLARE(AUDIT_SQL_READ);
AUDIT_TRACER_DECLARE(AUDIT_SQL_WRITE);
AUDIT_TRACER_DECLARE(AUDIT_DDL);
AUDIT_TRACER_DECLARE(AUDIT_DCL);

#define AUDIT_TRACE_INFO_INTERNAL_EXECUTE(tracer, statementType) { \
}

#define AUDIT_TRACE_INFO_EXECUTE() { \
}

#define AUDIT_TRACE_WARNING_EXECUTE(execution, message) { \
}

#define AUDIT_TRACE_ERROR_INTERNAL_HANDLEERROR(tracer, statementType) { \
}

#define AUDIT_TRACE_ERROR_HANDLEERROR() { \
}

void BindParamSet::clear() {
	for (size_t pos1 = 0; pos1 < bindParamList_.size(); pos1++) {
		for (size_t pos2 = 0; pos2 < bindParamList_[pos1].size(); pos2++) {
			ALLOC_DELETE(alloc_, bindParamList_[pos1][pos2]);
			bindParamList_[pos1].clear();
		}
	}
	bindParamList_.clear();
	currentPos_ = 0;
	columnSize_ = 0;
	rowCount_ = 0;
}

void BindParamSet::copyBindParamSet(const BindParamSet& rhs) {
	try {
		reset();
		prepare(rhs.columnSize_, rhs.rowCount_);
		for (int32_t pos = 0; pos < rhs.rowCount_; pos++) {
			for (int32_t columnNo = 0; columnNo < rhs.columnSize_; columnNo++) {
				ColumnType type = rhs.bindParamList_[pos][columnNo]->type_;
				TupleValue& value = rhs.bindParamList_[pos][columnNo]->value_;
				BindParam* param = ALLOC_NEW(alloc_) BindParam(alloc_, type);
				switch (type) {
				case COLUMN_TYPE_NULL: 
					param->value_ = TupleValue(&SyntaxTree::NULL_VALUE_RAW_DATA,
						TupleList::TYPE_ANY);
					 break;
				case COLUMN_TYPE_BOOL: 
				case COLUMN_TYPE_BYTE: 
				case COLUMN_TYPE_SHORT: 
				case COLUMN_TYPE_INT: 
				case COLUMN_TYPE_LONG: 
				case COLUMN_TYPE_FLOAT: 
				case COLUMN_TYPE_DOUBLE: 
				case COLUMN_TYPE_TIMESTAMP: 
				case COLUMN_TYPE_MICRO_TIMESTAMP: {
					TupleList::TupleColumnType tupleType = convertNoSQLTypeToTupleType(type);
					param->value_ = TupleValue(value.fixedData(), tupleType);
					break;
				}
				case COLUMN_TYPE_NANO_TIMESTAMP: {
					NanoTimestamp tsValue = value.get<TupleNanoTimestamp>();
					param->value_ = SyntaxTree::makeNanoTimestampValue(alloc_, tsValue);
					break;
				}
				case COLUMN_TYPE_STRING: 
				case COLUMN_TYPE_GEOMETRY:
				case COLUMN_TYPE_BLOB: {
					TupleList::TupleColumnType tupleType = convertNoSQLTypeToTupleType(type);
					switch (tupleType) {
					case TupleList::TYPE_STRING: {
						int32_t size = static_cast<int32_t>(value.varSize());
						const char8_t* data = reinterpret_cast<const char*>(value.varData());
						param->value_ = SyntaxTree::makeStringValue(alloc_, data, size);
						break;
					}
					case TupleList::TYPE_GEOMETRY:
					case TupleList::TYPE_BLOB: {
						TupleValue::VarContext cxt;
						cxt.setStackAllocator(&alloc_);
						TupleValue::StackAllocLobBuilder lobBuilder(
							cxt, TupleList::TYPE_BLOB, 0);
						{
							TupleValue::LobReader reader(value);
							const void *data;
							size_t size;
							while (reader.next(data, size)) {
								lobBuilder.append(data, size);
							}
						}
						param->value_ = lobBuilder.build();
						break;
					}
					default:
						GS_THROW_USER_ERROR(GS_ERROR_SQL_VALUETYPE_UNSUPPORTED, "");
					}
					break;
				}
				case COLUMN_TYPE_STRING_ARRAY:
				case COLUMN_TYPE_BOOL_ARRAY:
				case COLUMN_TYPE_BYTE_ARRAY:
				case COLUMN_TYPE_SHORT_ARRAY:
				case COLUMN_TYPE_INT_ARRAY:
				case COLUMN_TYPE_LONG_ARRAY:
				case COLUMN_TYPE_FLOAT_ARRAY:
				case COLUMN_TYPE_DOUBLE_ARRAY:
				case COLUMN_TYPE_TIMESTAMP_ARRAY: 
					GS_THROW_USER_ERROR(GS_ERROR_SQL_VALUETYPE_UNSUPPORTED,
						"Unsupported bind type = " << static_cast<int32_t>(type));
				default:
					GS_THROW_USER_ERROR(GS_ERROR_SQL_INTERNAL,
						"Invalid bind type = " << static_cast<int32_t>(type));
				}
				bindParamList_[pos].push_back(param);
			}
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

SQLConfigParam::SQLConfigParam(const ConfigTable& config) :
		multiIndexScan_(config.get<bool>(CONFIG_TABLE_SQL_MULTI_INDEX_SCAN)),
		partitioningRowKeyConstraint_(config.get<bool>(
				CONFIG_TABLE_SQL_PARTITIONING_ROWKEY_CONSTRAINT)),
		costBasedJoin_(config.get<bool>(CONFIG_TABLE_SQL_COST_BASED_JOIN)),
		costBasedJoinDriving_(
				config.get<bool>(CONFIG_TABLE_SQL_COST_BASED_JOIN_DRIVING)),
		planningVersion_(
				getPlanningVersion(config, CONFIG_TABLE_SQL_PLAN_VERSION)) {
}

SQLPlanningVersion SQLConfigParam::getPlanningVersion(
		const ConfigTable& config, SQLConfigTableParamId paramId) {
	SQLPlanningVersion version;
	const char8_t *str = config.get<const char8_t*>(paramId);
	if (strlen(str) != 0) {
		version.parse(str);
	}
	return version;
}

SQLExecutionManager::SQLExecutionManager(
	ConfigTable& config, SQLVariableSizeGlobalAllocator& globalVarAlloc) try :
	globalVarAlloc_(globalVarAlloc), jobManager_(NULL), dbConnection_(NULL),
	executionMap_(SQLExecutionMap::key_compare(), globalVarAlloc_),
	indexStats_(globalVarAlloc, SQLIndexStatsCache::Option()),
	fixedAllocator_(
		util::AllocatorInfo(ALLOCATOR_GROUP_SQL_WORK, "ExecutionAllocator"),
		1 << DEFAULT_ALLOCATOR_BLOCK_SIZE),
	processorConfig_(NULL),
	fetchSizeLimit_(ConfigTable::megaBytesToBytes(config.get<int32_t>(
		CONFIG_TABLE_SQL_FETCH_SIZE_LIMIT))),
	nosqlSizeLimit_(config.get<int32_t>(
		CONFIG_TABLE_SQL_TRANSACTION_MESSAGE_SIZE_LIMIT)),
	tableCacheDumpLimitTime_(config.get<int32_t>(
		CONFIG_TABLE_SQL_TABLE_CACHE_LOAD_DUMP_LIMIT_TIME)),
	mgrSet_(NULL),
	config_(config),
	tableCacheHitCount_(1),
	tableCacheMissHitCount_(1),
	tableCacheSearchCount_(0),
	tableCacheSkipCount_(0),
	tableCacheLoadTime_(0)
{
	processorConfig_ = UTIL_NEW SQLProcessorConfig();
	processorConfig_->workMemoryLimitBytes_ = ConfigTable::megaBytesToBytes(
		config.getUInt32(CONFIG_TABLE_SQL_WORK_MEMORY_LIMIT));
	processorConfig_->workMemoryCacheBytes_ = ConfigTable::megaBytesToBytes(
		config.getUInt32(CONFIG_TABLE_SQL_WORK_CACHE_MEMORY));

	uint64_t maxCount = processorConfig_->workMemoryCacheBytes_
		/ static_cast<uint32_t>(1 << DEFAULT_ALLOCATOR_BLOCK_SIZE);
	fixedAllocator_.setFreeElementLimit(static_cast<size_t>(maxCount));

	nodeCounter_ = 0;
	int32_t concurrency = config.get<int32_t>(CONFIG_TABLE_SQL_CONCURRENCY);
	totalReadOperationList_.assign(concurrency, 0);
	totalWriteOperationList_.assign(concurrency, 0);
}
catch (...) {
	delete processorConfig_;
}

SQLExecutionManager::~SQLExecutionManager() {

	util::LockGuard<util::Mutex> guard(lock_);
	for (SQLExecutionMap::iterator it = executionMap_.begin();
		it != executionMap_.end(); it++) {
		SQLExecution* execution = (*it).second;
		if (execution != NULL) {
			ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, execution);
		}
	}
	delete processorConfig_;
	ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, dbConnection_);
}

void SQLExecutionManager::initialize(const ManagerSet& mgrSet) {
	mgrSet_ = &mgrSet;
	jobManager_ = mgrSet.jobMgr_;
	pt_ = mgrSet.pt_;

	int32_t cacheSize = mgrSet.sqlSvc_->getTableSchemaCacheSize();
	dbConnection_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
		DBConnection(*mgrSet.config_, globalVarAlloc_,
			cacheSize, mgrSet.pt_, mgrSet.partitionList_, mgrSet.sqlSvc_);
}

void SQLExecutionManager::remove(
		EventContext& ec,
		ClientId& targetClientId, bool withCheck, int32_t point, JobId* jobId) {
	UNUSED_VARIABLE(point);

	SQLExecution* execution = NULL;
	try {
		JobId cancelJobId;
		bool isRemove = false;
		ClientId clientId = targetClientId;
		{
			Latch latch(clientId, this);
			SQLExecution* execution = latch.get();
			if (execution) {
				isRemove = true;
				execution->getContext().getCurrentJobId(cancelJobId);
				execution->cancel(ec);
			}
		}
		if (isRemove) {
			if (jobId == NULL || (jobId != NULL && cancelJobId == *jobId)) {
				jobManager_->cancel(ec, cancelJobId, false);
			}
		}
		{
			util::LockGuard<util::Mutex> guard(lock_);
			SQLExecutionMapItr it = executionMap_.find(clientId);
			if (it != executionMap_.end()) {
				execution = (*it).second;
				if (withCheck) {
					if (!execution->getContext().isPreparedStatement()) {
						executionMap_.erase(clientId);
						execution->unlatch();
					}
				}
				else {
					executionMap_.erase(clientId);
					execution->unlatch();
				}
			}
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

SQLExecution* SQLExecutionManager::get(ClientId& clientId) {
	try {
		util::LockGuard<util::Mutex> guard(lock_);
		SQLExecutionMapItr it = executionMap_.find(clientId);
		if (it != executionMap_.end()) {
			SQLExecution* execution = (*it).second;
			execution->latch();
			return execution;
		}
		else {
			return NULL;
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

SQLExecution* SQLExecutionManager::create(ClientId& clientId,
	const NodeDescriptor* clientNd, RequestInfo* request) {
	try {
		util::LockGuard<util::Mutex> guard(lock_);
		SQLExecutionMapItr it = executionMap_.find(clientId);
		if (it != executionMap_.end()) {
			GS_THROW_USER_ERROR(
				GS_ERROR_SQL_EXECUTION_INTERNAL_ALREADY_EXIST,
				"ClientId=" << clientId << " is already exists");
		}
		else {
			SQLExecution* execution = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
				SQLExecution(jobManager_, this, globalVarAlloc_, clientId, *clientNd,
					clientNd->getUserData<StatementHandler::ConnectionOption>(),
					*request, dbConnection_);
			executionMap_.insert(std::make_pair(clientId, execution));
			return execution;
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void SQLExecutionManager::release(SQLExecution* execution) {
	try {
		util::LockGuard<util::Mutex> guard(lock_);
		if (execution->unlatch()) {
			util::StackAllocator* alloc = execution->getStackAllocator();
			ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, execution);
			releaseStackAllocator(alloc);
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void SQLExecutionManager::closeConnection(EventContext& ec,
	const NodeDescriptor& nd) {
	try {
		ClientId clientId;
		util::StackAllocator& alloc = ec.getAllocator();
		util::XArray<SessionId> sessionIdList(alloc);
		{
			util::LockGuard<util::Mutex> guard(lock_);
			StatementHandler::ConnectionOption& connOption =
				nd.getUserData<StatementHandler::ConnectionOption>();
			connOption.getSessionIdList(clientId, sessionIdList);
		}
		if (sessionIdList.size() > 0) {
			GS_TRACE_INFO(SQL_DETAIL, 0, "Connection close. nd=" << nd);
		}
		for (size_t pos = 0; pos < sessionIdList.size(); pos++) {
			clientId.sessionId_ = sessionIdList[pos];
			SQLExecutionManager::Latch latch(clientId, this);
			SQLExecution* execution = latch.get();
			if (execution) {
				try {
					remove(ec, clientId, false, 50, NULL);
				}
				catch (std::exception& e) {
					UTIL_TRACE_EXCEPTION(SQL_SERVICE, e,
						"Remove SQL execution failed, but continue, clientId=" << clientId);
				}
			}
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void SQLExecutionManager::ConfigSetUpHandler::operator()(ConfigTable& config) {
	CONFIG_TABLE_RESOLVE_GROUP(config, CONFIG_TABLE_SQL, "sql");

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_WORK_MEMORY_LIMIT, INT32).
		setUnit(ConfigTable::VALUE_UNIT_SIZE_MB).
		setMin(1).
		setDefault(SQLProcessorConfig::DEFAULT_WORK_MEMORY_MB);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_STORE_MEMORY_LIMIT, INT32).
		setUnit(ConfigTable::VALUE_UNIT_SIZE_MB).
		setMin(1).
		setDefault(LocalTempStore::DEFAULT_STORE_MEMORY_LIMIT_MB);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_STORE_SWAP_FILE_SIZE_LIMIT, INT32).
		setUnit(ConfigTable::VALUE_UNIT_SIZE_MB).
		setMin(1).
		setDefault(LocalTempStore::DEFAULT_STORE_SWAP_FILE_SIZE_LIMIT_MB);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_STORE_SWAP_FILE_PATH, STRING).
		setDefault(LocalTempStore::DEFAULT_SWAP_FILES_TOP_DIR);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_WORK_CACHE_MEMORY, INT32).
		setUnit(ConfigTable::VALUE_UNIT_SIZE_MB).
		setMin(1).
		setDefault(DEFAULT_WORK_CACHE_MEMORY_MB);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_FETCH_SIZE_LIMIT, INT32).
		setUnit(ConfigTable::VALUE_UNIT_SIZE_MB).
		setMin(1).
		setDefault(2);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_TRANSACTION_MESSAGE_SIZE_LIMIT, INT32).
		setMin(1).
		setDefault(1024 * 1024 * 2);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_SUB_CONTAINER_ASSIGN_TYPE, INT32).
		setDefault(0);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_STORE_SWAP_SYNC_SIZE, INT32).
		setUnit(ConfigTable::VALUE_UNIT_SIZE_MB).
		setMin(0).
		setDefault(LocalTempStore::DEFAULT_STORE_SWAP_SYNC_SIZE_MB);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_STORE_SWAP_SYNC_INTERVAL, INT32).
		setMin(0).
		setDefault(LocalTempStore::DEFAULT_STORE_SWAP_SYNC_INTERVAL);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_STORE_SWAP_RELEASE_INTERVAL, INT32).
		setMin(0).
		setDefault(0);
}

SQLExecutionManager::Latch::Latch(ClientId& clientId,
	SQLExecutionManager* executionManager, const NodeDescriptor* clientNd,
	RequestInfo* request) : clientId_(clientId),
	executionManager_(executionManager), execution_(NULL) {
	execution_ = executionManager_->create(clientId, clientNd, request);
}

SQLExecutionManager::Latch::Latch(ClientId& clientId,
	SQLExecutionManager* executionManager) : clientId_(clientId),
	executionManager_(executionManager), execution_(NULL) {
	execution_ = executionManager_->get(clientId);
}

SQLExecution* SQLExecutionManager::Latch::get() {
	return execution_;
}

SQLExecutionManager::Latch::~Latch() {
	if (execution_) executionManager_->release(execution_);
}

SQLExecution::SQLExecution(JobManager* jobManager,
	SQLExecutionManager* execManager,
	SQLVariableSizeGlobalAllocator& globalVarAlloc, ClientId& clientId,
	const NodeDescriptor& clientNd,
	StatementHandler::ConnectionOption& connOption,
	RequestInfo& request,
	DBConnection* dbConnection)  try :
	latchCount_(1),
	clientId_(clientId),
	jobManager_(jobManager),
	executionManager_(execManager),
	dbConnection_(dbConnection),
	executionStackAlloc_(*wrapStackAllocator()),
	globalVarAlloc_(globalVarAlloc),
	localVarAlloc_(*jobManager_->
		getSQLService()->getLocalAllocator(request.pgId_)),
	scope_(NULL),
	group_(jobManager_->getStore()),
	resultSet_(NULL),
	executionStatus_(request),
	config_(request),
	syncContext_(NULL),
	isCancelled_(false),
	currentJobId_(JobId()),
	preparedInfo_(globalVarAlloc_),
	clientInfo_(executionStackAlloc_, globalVarAlloc_,
		clientNd, clientId, connOption),
	analyzedQueryInfo_(globalVarAlloc_, executionStackAlloc_),
	context_(lock_, clientInfo_, analyzedQueryInfo_,
		executionStatus_, response_, clientId_, currentJobId_),
	isBatch_(false),
	batchCount_(0),
	batchCurrentPos_(0),
	bindParamSet_(executionStackAlloc_)
{
	executionStatus_.set(request);
	scope_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
		util::StackAllocator::Scope(executionStackAlloc_);
	setupSyncContext();
	resultSet_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_) SQLResultSet(globalVarAlloc_);
	latch();
	currentJobId_.set(clientId_, 0);
}
catch (...) {
	ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, resultSet_);
	ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, syncContext_);
	ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, scope_);
	ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, &executionStackAlloc_);
	updateConnection();
}

SQLExecution::~SQLExecution()  try {

	ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, resultSet_);
	try {
		syncContext_->cancel();
	}
	catch (std::exception&) {
	}
	ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, syncContext_);
	ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, scope_);
	clearIndexStatsRequester();
	updateConnection();
	cleanupSerializedData();

}
catch (...) {
	assert(false);
}

void SQLExecution::setRequest(RequestInfo& request) {
	executionStatus_.execId_ = request.stmtId_;
	executionStatus_.connectedPId_ = request.pId_;
	executionStatus_.connectedPgId_ = request.pgId_;
	executionStatus_.requestType_ = request.requestType_;
	context_.setQuery(request.sqlString_);
	context_.resetPendingException();
	response_.set(request, resultSet_);
	executionStatus_.responsed_ = false;

}

SQLTableInfoList* SQLExecution::generateTableInfo(SQLExpandViewContext& cxt) {
	SQLParsedInfo& parsedInfo = *cxt.parsedInfo_;
	SQLConnectionControl* control = cxt.control_;
	SQLTableInfoList* tableInfoList = cxt.tableInfoList_;

	std::pair<util::Set<util::String>::iterator, bool> outResult;
	cxt.forceMetaDataQuery_ = false;
	std::string value;
	if (control->getEnv(
		SQLPragma::PRAGMA_INTERNAL_COMPILER_EXECUTE_AS_META_DATA_QUERY, value)) {
		if (value == SQLPragma::VALUE_TRUE) {
			cxt.forceMetaDataQuery_ = true;
		}
	}
	cxt.isMetaDataQuery_ = (
		executionStatus_.requestType_ == REQUEST_TYPE_PRAGMA || cxt.forceMetaDataQuery_);
	std::string pragmaValue;
	cxt.metaVisible_ = true;
	if (control->getEnv(
		SQLPragma::PRAGMA_INTERNAL_COMPILER_META_TABLE_VISIBLE,
		pragmaValue)) {
		cxt.metaVisible_ = (pragmaValue == SQLPragma::VALUE_TRUE);
	}
	cxt.internalMode_ = false;
	if (control->getEnv(
		SQLPragma::PRAGMA_INTERNAL_COMPILER_INTERNAL_META_TABLE_VISIBLE,
		pragmaValue)) {
		cxt.internalMode_ = (pragmaValue == SQLPragma::VALUE_TRUE);
	}
	cxt.forDriver_ = cxt.isMetaDataQuery_;
	if (control->getEnv(
		SQLPragma::PRAGMA_INTERNAL_COMPILER_DRIVER_META_TABLE_VISIBLE,
		pragmaValue)) {
		cxt.forDriver_ |= (pragmaValue == SQLPragma::VALUE_TRUE);
	}
	if (parsedInfo.syntaxTreeList_.size() > 0) {
		if (!parsedInfo.createForceView_) {
			for (util::Vector<SyntaxTree::Expr*>::iterator
				it = parsedInfo.tableList_.begin();
				it != parsedInfo.tableList_.end(); it++) {
				SyntaxTree::Expr* expr = *it;
				getTableInfo(cxt, expr, 0);
			}
		}
	}
	return tableInfoList;
}

SQLTableInfoList* SQLExecution::createEmptyTableInfo(
		util::StackAllocator &alloc) {
	return ALLOC_NEW(alloc) SQLTableInfoList(alloc, &getIndexStats());
}

const SQLTableInfo* SQLExecution::getTableInfo(
		SQLExpandViewContext& cxt,
		SyntaxTree::Expr* expr,
		uint32_t viewDepth,
		bool viewOnly) {
	UNUSED_VARIABLE(viewDepth);

	util::StackAllocator& alloc = cxt.getAllocator();
	const char8_t* defaultDbName = cxt.defaultDbName_;
	SQLConnectionControl* control = cxt.control_;
	EventContext& ec = *cxt.ec_;
	SQLTableInfoList* tableInfoList = cxt.tableInfoList_;
	bool useCache = cxt.useCache_;
	bool withVersion = cxt.withVersion_;
	util::Vector<NodeId>& liveNodeIdList = *cxt.liveNodeIdList_;
	std::pair<util::Set<util::String>::iterator, bool> outResult;

	TableSchemaInfo* tableSchema = NULL;
	const SyntaxTree::QualifiedName* srcName = expr->qName_;
	if (srcName == NULL) {
		return NULL;
	}
	const util::String* tableName = srcName->table_;
	if (tableName == NULL) {
		return NULL;
	}
	NameWithCaseSensitivity currentTableName(
		tableName->c_str(), srcName->tableCaseSensitive_);
	if (SQLCompiler::Meta::isSystemTable(*srcName)) {
		if (viewOnly) {
			return NULL;
		}
		SQLTableInfo tableInfo(alloc);
		tableInfo.tableName_ = *tableName;
		tableInfo.dbName_ = defaultDbName;
		tableInfo.idInfo_.dbId_ = context_.getDBId();
		tableInfo.hasRowKey_ = false;

		std::string pragmaValue;

		bool metaVisible = true;
		if (control->getEnv(
			SQLPragma::PRAGMA_INTERNAL_COMPILER_META_TABLE_VISIBLE,
			pragmaValue)) {
			metaVisible = (pragmaValue == SQLPragma::VALUE_TRUE);
		}

		bool internalMode = false;
		if (control->getEnv(
			SQLPragma::PRAGMA_INTERNAL_COMPILER_INTERNAL_META_TABLE_VISIBLE,
			pragmaValue)) {
			internalMode = (pragmaValue == SQLPragma::VALUE_TRUE);
		}

		bool forDriver = cxt.isMetaDataQuery_;
		if (control->getEnv(
			SQLPragma::PRAGMA_INTERNAL_COMPILER_DRIVER_META_TABLE_VISIBLE,
			pragmaValue)) {
			forDriver |= (pragmaValue == SQLPragma::VALUE_TRUE);
		}

		const SQLTableInfo::IdInfo& idInfo =
			SQLCompiler::Meta::setSystemTableColumnInfo(
				alloc, *srcName, tableInfo, metaVisible, internalMode,
				forDriver);
		bool isNodeExpansion = SQLCompiler::Meta::isNodeExpansion(idInfo.containerId_);

		const SQLTableInfo& sqlTableInfo = tableInfoList->add(tableInfo);
		PartitionTable* pt = executionManager_->getPartitionTable();
		uint32_t partitionCount = pt->getPartitionNum();
		if (isNodeExpansion) {
			partitionCount = static_cast<uint32_t>(liveNodeIdList.size());
		}
		tableInfoList->prepareMeta(
			*srcName, idInfo, context_.getDBId(), partitionCount);
		return &sqlTableInfo;
	}

	bool dbCaseSensitive = expr->qName_->dbCaseSensitive_;
	util::String currentDb(alloc);
	if (expr->qName_->db_) {
		if (dbCaseSensitive) {
			checkConnectedDbName(alloc, context_.getDBName(),
				expr->qName_->db_->c_str(), dbCaseSensitive);
		}
		else {
			checkConnectedDbName(alloc, context_.getNormalizedDBName(),
				expr->qName_->db_->c_str(), dbCaseSensitive);
		}
		currentDb = *(expr->qName_->db_);
	}
	else {
		currentDb = util::String(defaultDbName, alloc);
	}
	NameWithCaseSensitivity currentDbName(currentDb.c_str(), dbCaseSensitive);

	if (!currentTableName.isCaseSensitive_) {
		const util::String& normalizeTableName = normalizeName(
			alloc, tableName->c_str());
		outResult = cxt.tableSet_->insert(normalizeTableName);
	}
	else {
		util::String currentTableNameStr(currentTableName.name_, alloc);
		outResult = cxt.tableSet_->insert(currentTableNameStr);
	}
	if (!outResult.second) {
		const SQLTableInfo* sqlTableInfo = tableInfoList->find(
			currentDb.c_str(), tableName->c_str(),
			dbCaseSensitive, currentTableName.isCaseSensitive_);
		return sqlTableInfo;
	}
	CreateTableOption option(alloc);
	DBConnection* conn = executionManager_->getDBConnection();
	bool isFirst = true;
	try {
		SQLTableInfo tableInfo(alloc);
		tableInfo.dbName_ = currentDb;
		TableLatch latch(ec, conn, this, currentDbName,
			currentTableName, useCache);
		bool withoutCache = !cxt.useCache_;
		tableSchema = latch.get();
		if (tableSchema != NULL) {
			if (tableSchema->containerInfoList_.size() == 0) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_INVALID_SCHEMA_INFO,
					"Invalid scheme info, container list is empty");
			}
			setTimeSeriesIncluded(tableSchema->isTimeSeriesContainer());
			EventMonotonicTime currentEmTime = ec.getEngine().getMonotonicTime();
			checkRefreshTable(ec, currentTableName, alloc, tableSchema, currentEmTime);
			tableSchema->checkSubContainer(0);
			checkWritableTable(*control, tableSchema, tableInfo);
			const uint32_t clusterPartitionCount =
				executionManager_->getPartitionTable()->getPartitionNum();
			tableSchema->setupSQLTableInfo(alloc, tableInfo,
				currentTableName.name_, context_.getDBId(),
				withVersion, clusterPartitionCount, isFirst, withoutCache
				, ec.getHandlerStartTime().getUnixTime()
				, tableSchema->sqlString_.c_str()
			);
			const SQLTableInfo& sqlTableInfo = tableInfoList->add(tableInfo);
			return &sqlTableInfo;
		}
		return NULL;
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void SQLExecution::clearIndexStatsRequester() {
	indexStatsRequester_.reset();
}

bool SQLExecution::checkIndexStatsResolved() {
	const uint64_t *requester = indexStatsRequester_.get();
	if (requester == NULL) {
		return true;
	}

	SQLIndexStatsCache &indexStats = getIndexStats();
	return indexStats.checkKeyResolved(*requester);
}

void SQLExecution::resolveIndexStats(EventContext &ec) {
	const uint64_t *requester = indexStatsRequester_.get();
	if (requester == NULL) {
		return;
	}

	SQLIndexStatsCache &indexStats = getIndexStats();

	DBConnection *conn = executionManager_->getDBConnection();
	NoSQLStore *store = conn->getNoSQLStore(
			getContext().getDBId(), getContext().getDBName());

	store->estimateIndexSearchSize(ec, *this, indexStats, *requester);
}

SQLIndexStatsCache& SQLExecution::getIndexStats() {
	return executionManager_->getIndexStats();
}

void SQLExecution::checkCancel() {
	if (isCancelled_) {
		isCancelled_ = false;
	}
}

void SQLExecution::execute(EventContext& ec,
	RequestInfo& request, bool prepareBinded,
	std::exception* e, uint8_t versionId, JobId* responseJobId) {

	util::StackAllocator& alloc = ec.getAllocator();
	if (e == NULL && !isBatch_) {
		setRequest(request);
	}

	checkConcurrency(ec, e);

	if (e == NULL) {
		GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK, GS_TRACE_SQL_INTERNAL_DEBUG,
			"SQL=" << request.sqlString_.c_str() << ", clientId=" << clientId_);
		GS_TRACE_DEBUG(SQL_DETAIL,
			GS_TRACE_SQL_EXECUTION_INFO, "sql=" << request.sqlString_.c_str());
	}

	if ((request.requestType_ == REQUEST_TYPE_PREPARE)
		|| (request.isBind() && request.retrying_
			&& request.sqlString_.size() > 0)) {
		setPrepared();
	}

	bool isException = (e != NULL);
	response_.prepareBinded_ = prepareBinded;
	SQLParsedInfo& parsedInfo = getParsedInfo();
	checkCancel();
	if (!isException) {
		cleanupPrevJob(ec);
		if (!isBatch_) {
			cleanupSerializedData();
			copySerializedBindInfo(request.serializedBindInfo_);
			if (request.bindParamSet_.getRowCount() > 0) {
				bindParamSet_.copyBindParamSet(request.bindParamSet_);
			}
		}
	}

	if (!isBatch_ && request.bindParamSet_.getRowCount() > 1) {
		isBatch_ = true;
		batchCount_ = request.bindParamSet_.getRowCount();
		SQLService* sqlSvc = executionManager_->getSQLService();
		int32_t addBatchMaxCount = sqlSvc->getAddBatchMaxCount();
		if (addBatchMaxCount != 0 && batchCount_ > addBatchMaxCount) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_ADD_BATCH_LIMIT_EXCEEDED, 
					"addBatch limit exceeded (limit=" << addBatchMaxCount <<
					", requested=" << batchCount_ << ")");
		}
	}
	else {
		if (!isBatch_) {
			isBatch_ = false;
		}
	}

	SQLConnectionControl connectionControl(executionManager_, this);
	JobId currentJobId;
	uint32_t currentVersionId = versionId;
	context_.getCurrentJobId(currentJobId);
	if (versionId == UNDEF_JOB_VERSIONID) {
		currentVersionId = currentJobId.versionId_;
	}
	int64_t delayTime = 0;
	uint32_t delayStartTime = 0;
	uint32_t delayExecutionTime = 0;
	bool isPreparedRetry = false;
	bool checkConst = false;
	bool isDbConstraint = false;

	if (context_.isRetryStatement()) {
		JobId jobId(clientId_, context_.getExecId());
		currentVersionId = 1;
		jobId.versionId_ = static_cast<uint8_t>(currentVersionId);
		context_.setCurrentJobId(jobId);
	}

	PartitionTable* pt = executionManager_->getPartitionTable();
	util::Vector<NodeId> liveNodeIdList(alloc);
	pt->getLiveNodeIdList(liveNodeIdList);
	int32_t maxNodeNum = pt->getNodeNum();

	bool first = true;
	for (;currentVersionId < MAX_JOB_VERSIONID + 1; currentVersionId++) {
		try {

			if (e != NULL) {
				GS_RETHROW_USER_OR_SYSTEM(*e, "");
			}

			ExecutionProfilerWatcher watcher(this, (e == NULL), false);

			bool useCache = (!context_.isRetryStatement() && currentVersionId == 0);
			resultSet_->resetSchema();

			SQLPreparedPlan::ValueTypeList parameterTypeList(alloc);
			SQLTableInfoList* tableInfoList = NULL;
			util::Set<util::String> tableSet(alloc);
			util::Set<util::String> viewSet(alloc);

			if (!request.isAutoCommit_) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_UNSUPPORTED,
					"Self commit mode is not supported");
			}

			if (request.transactionStarted_) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_UNSUPPORTED,
					"Transaction is not supported");
			}

			if (isPreparedRetry) {
				request.bindParamSet_.clear();
				restoreBindInfo(request);
				bindParamSet_.copyBindParamSet(request.bindParamSet_);
			}


			TupleValue::VarContext varCxt;
			varCxt.setStackAllocator(&alloc);
			varCxt.setVarAllocator(&localVarAlloc_);
			varCxt.setGroup(&group_);
			TupleValue::VarContext::Scope varScope(varCxt);

			SQLExpandViewContext cxt;
			setupViewContext(cxt, ec, connectionControl,
				tableSet, viewSet, liveNodeIdList, useCache,
				(currentVersionId == 0));

			int64_t startTime = util::DateTime::now(false).getUnixTime();
			if (request.requestType_ == REQUEST_TYPE_PREPARE || prepareBinded) {
				TRACE_SQL_START(clientId_, jobManager_->getHostName(), "PARSE");
				tableInfoList = createEmptyTableInfo(alloc);
				tableInfoList->setDefaultDBName(context_.getDBName());

				cxt.tableInfoList_ = tableInfoList;
				if (parsedInfo.getCommandNum() == 0 && !parse(cxt, request, 0, 0, 0)) {
					AUDIT_TRACE_INFO_EXECUTE();
					SQLReplyContext replyCxt;
					replyCxt.set(&ec, NULL, UNDEF_JOB_VERSIONID, NULL);
					replyClient(replyCxt);
					if (analyzedQueryInfo_.isPragmaStatement()) {
						executionManager_->remove(ec, clientId_, false, 51, NULL);
					}
					return;
				}
				TRACE_SQL_END(clientId_, jobManager_->getHostName(), "PARSE");
				AUDIT_TRACE_INFO_EXECUTE();

				getContext().getConnectionOption().checkSelect(parsedInfo);

				checkClientResponse();

				isDbConstraint = checkExecutionConstraint(delayStartTime, delayExecutionTime);
				checkConst = true;

				if (!isBatch() && !isDbConstraint && 
					fastInsert(ec, bindParamSet_.getParamList(batchCurrentPos_), useCache)) {
					return;
				}
				tableInfoList = generateTableInfo(cxt);

				if (request.requestType_ == REQUEST_TYPE_PREPARE) {
					const bool prepared = true;
					compile(
							ec, connectionControl, parameterTypeList,
							tableInfoList, varCxt, startTime, request, prepared,
							first, isPreparedRetry);

					SQLReplyContext replyCxt;
					replyCxt.set(&ec, &parameterTypeList, UNDEF_JOB_VERSIONID, NULL);
					replyClient(replyCxt);
					return;
				}
			}

			getContext().getConnectionOption().checkSelect(getParsedInfo());
			checkClientResponse();

			DatabaseManager::Option option;
			if (parsedInfo.syntaxTreeList_.size() > 0) {
				option.dropOnly_ = parsedInfo.syntaxTreeList_[0]->isDropOnlyCommand();
			}
			if (!checkConst) {
				isDbConstraint = checkExecutionConstraint(delayStartTime, delayExecutionTime);
			}
			checkBatchRequest(request);
			if (!isBatch() && !isDbConstraint && 
				fastInsert(ec, bindParamSet_.getParamList(batchCurrentPos_), useCache)) {
				return;
			}

			if (tableInfoList == NULL) {
				tableInfoList = createEmptyTableInfo(alloc);
				tableInfoList->setDefaultDBName(context_.getDBName());
				cxt.tableInfoList_ = tableInfoList;
				tableInfoList = generateTableInfo(cxt);
			}
			parameterTypeList.clear();

			watcher.lap(ExecutionProfilerWatcher::PARSE);

			{
				const bool prepared = false;
				compile(
						ec, connectionControl, parameterTypeList,
						tableInfoList, varCxt, startTime, request, prepared,
						first, isPreparedRetry);
			}

			if (analyzedQueryInfo_.isExplain()) {
				SQLReplyContext replyCxt;
				replyCxt.setExplain(&ec);
				replyClient(replyCxt);
				return;
			}

			TRACE_SQL_START(clientId_, jobManager_->getHostName(), "ASSIGN");
			JobInfo jobInfo(alloc);
			jobInfo.startTime_ = startTime;
			generateJobInfo(alloc, tableInfoList, executionStatus_.preparedPlan_,
				&jobInfo, 0, ec.getEngine().getMonotonicTime(),
				connectionControl, liveNodeIdList, maxNodeNum);
			TRACE_SQL_END(clientId_, jobManager_->getHostName(), "ASSIGN");

			jobInfo.delayTime_ = delayExecutionTime;

			JobId jobId;
			generateJobId(jobId, request.stmtId_);

			watcher.lap(ExecutionProfilerWatcher::COMPILE);

			jobManager_->beginJob(ec, jobId, &jobInfo, delayStartTime);
			getContext().setStartTime(jobInfo.startTime_);
			return;
		}
		catch (std::exception& e1) {
			if (context_.isPreparedStatement()) {
				isPreparedRetry = true;
			}
			if (!checkJobVersionId(
					static_cast<uint8_t>(currentVersionId), false,
					responseJobId)) {
				return;
			}
			if (handleError(ec, e1, currentVersionId, delayTime, request)) {
				return;
			}
		}
		isException = false;
		e = NULL;
		first = false;
	}
	GS_THROW_USER_ERROR(GS_ERROR_SQL_EXECUTION_RETRY_QUERY_LIMIT,
		"Sql statement execution is reached retry max count="
		<< static_cast<int32_t>(MAX_JOB_VERSIONID));
}

void SQLExecution::generateJobInfo(
		util::StackAllocator& alloc,
		SQLTableInfoList* tableInfoList, SQLPreparedPlan* pPlan,
		JobInfo* jobInfo, CommandId commandId,
		EventMonotonicTime emNow, SQLConnectionControl& control,
		util::Vector<NodeId>& liveNodeIdList, int32_t maxNodeNum) {
	UNUSED_VARIABLE(commandId);
	UNUSED_VARIABLE(control);

	try {
		jobInfo->syncContext_ = &context_.getSyncContext();
		jobInfo->option_.plan_ = executionStatus_.preparedPlan_;
		if (isTimeSeriesIncluded()) {
			jobInfo->containerType_ = TIME_SERIES_CONTAINER;
		}
		jobInfo->isSQL_ = !isNotSelect();
		jobInfo->isExplainAnalyze_ = analyzedQueryInfo_.isExplainAnalyze();
		size_t pos = 0;
		PartitionTable* pt = executionManager_->getPartitionTable();
		util::Vector<NodeId> rootAssignTaskList(pPlan->nodeList_.size(), 0, alloc);
		for (pos = 0; pos < pPlan->nodeList_.size(); pos++) {
			jobInfo->createTaskInfo(pPlan, pos);
		}
		for (pos = 0; pos < pPlan->nodeList_.size(); pos++) {
			TaskInfo* taskInfo = jobInfo->taskInfoList_[pos];
			for (TaskInfo::ResourceList::iterator it = taskInfo->inputList_.begin();
				it != taskInfo->inputList_.end(); ++it) {
				jobInfo->taskInfoList_[*it]->outputList_.push_back(static_cast<uint32_t>(pos));
			}
			if (taskInfo->isDml_) {
				SQLProcessor::planToInputInfoForDml(
					*pPlan, static_cast<uint32_t>(pos), *tableInfoList,
					taskInfo->inputTypeList_);
				rootAssignTaskList[pos] = 1;
			}
			else {
				if (taskInfo->sqlType_ == SQLType::EXEC_DDL) {
					rootAssignTaskList[pos] = 1;
				}
				SQLProcessor::planToInputInfo(
					*pPlan, static_cast<uint32_t>(pos), *tableInfoList,
					taskInfo->inputTypeList_);
			}
			SQLProcessor::planToOutputInfo(
				*pPlan, static_cast<uint32_t>(pos), taskInfo->outColumnTypeList_);
			if (taskInfo->sqlType_ == SQLType::EXEC_RESULT) {
				jobInfo->resultTaskId_ = static_cast<int32_t>(pos);
				jobInfo->pId_ = getConnectedPId();
				rootAssignTaskList[pos] = 1;
				util::Vector<util::String> columnNameList(alloc);
				resultSet_->getColumnNameList(alloc, columnNameList);
				if (columnNameList.size() == 0) {
					pPlan->nodeList_[pos].getColumnNameList(columnNameList);
					resultSet_->setColumnNameList(columnNameList);
				}
				resultSet_->setColumnTypeList(taskInfo->inputTypeList_[0]);
			}
		}
		int32_t nodeNum = static_cast<int32_t>(liveNodeIdList.size());
		if (nodeNum == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INVALID_NODE_ASSIGN,
				"Invalid node assigned, active node is none");
		}
		util::XArray<NodeId> activeNodeListMap(alloc);
		activeNodeListMap.assign(static_cast<size_t>(maxNodeNum), -1);
		util::Vector<int32_t> assignNodes(nodeNum, 0, alloc);
		util::Vector<bool> checkNodes(nodeNum, false, alloc);
		if (nodeNum > 1) {
			for (size_t i = 0;i < liveNodeIdList.size(); i++) {
				activeNodeListMap[liveNodeIdList[i]] = static_cast<NodeId>(i);
			}
			TaskInfo* resultTask = jobInfo->taskInfoList_[jobInfo->resultTaskId_];
			assert(resultTask->inputList_.size() > 0);
			TaskInfo* targetTask = jobInfo->taskInfoList_[resultTask->inputList_[0]];
			if (targetTask->sqlType_ == SQLType::EXEC_SELECT) {
				rootAssignTaskList[targetTask->taskId_] = 1;
				if (targetTask->inputList_.size() > 0) {
					TaskInfo* unionTask = jobInfo->taskInfoList_[targetTask->inputList_[0]];
					if (unionTask->sqlType_ == SQLType::EXEC_UNION) {
						rootAssignTaskList[unionTask->taskId_] = 1;
					}
				}
			}
			else if (targetTask->sqlType_ == SQLType::EXEC_DDL
				|| targetTask->sqlType_ == SQLType::EXEC_UNION) {
				rootAssignTaskList[targetTask->taskId_] = 1;
			}
			else if (targetTask->isDml_) {
				assert(resultTask->inputList_.size() > 0);
				TaskInfo* currentTask = jobInfo->taskInfoList_[resultTask->inputList_[0]];
				if (currentTask->sqlType_ == SQLType::EXEC_SELECT) {
					rootAssignTaskList[currentTask->taskId_] = 1;
					if (currentTask->inputList_.size() > 0) {
						TaskInfo* unionTask = jobInfo->taskInfoList_[currentTask->inputList_[0]];
						if (unionTask->sqlType_ == SQLType::EXEC_UNION) {
							rootAssignTaskList[unionTask->taskId_] = 1;
						}
					}
				}
				else if (currentTask->sqlType_ == SQLType::EXEC_UNION) {
					rootAssignTaskList[targetTask->taskId_] = 1;
				}
			}
		}
		if (nodeNum == 1) {
			bool forceAssign = false;
			if (pPlan->nodeList_.size() == 2) {
				forceAssign = true;
			}
			for (size_t pos = 0; pos < pPlan->nodeList_.size(); pos++) {
				TaskInfo* taskInfo = jobInfo->taskInfoList_[pos];
				if (taskInfo->sqlType_ == SQLType::EXEC_RESULT
					|| taskInfo->sqlType_ == SQLType::EXEC_DDL
					|| taskInfo->isDml_) {
					taskInfo->loadBalance_ = getConnectedPId();
					jobInfo->pId_ = getConnectedPId();
					taskInfo->nodePos_ = 0;
				}
				else if (taskInfo->sqlType_ == SQLType::EXEC_SCAN) {
					taskInfo->type_ = static_cast<uint8_t>(TRANSACTION_SERVICE);
					SQLPreparedPlan::Node& node = pPlan->nodeList_[pos];
					taskInfo->loadBalance_ = node.tableIdInfo_.partitionId_;
					if (node.tableIdInfo_.isNodeExpansion_) {
						taskInfo->type_ = static_cast<uint8_t>(SQL_SERVICE);
						taskInfo->nodePos_ = 0;
						continue;
					}
					taskInfo->nodePos_ = pt->getNewSQLOwner(node.tableIdInfo_.partitionId_);
					if (taskInfo->nodePos_ != 0) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INVALID_NODE_ASSIGN,
							"Invalid node assign (pId=" << node.tableIdInfo_.partitionId_
							<< ", nodeId=" << taskInfo->nodePos_ << ")");
					}
				}
				else {
					taskInfo->nodePos_ = 0;
					if (forceAssign) {
						taskInfo->loadBalance_ = getConnectedPId();
					}
					else {
						taskInfo->loadBalance_ = -1;
					}
				}
			}
		}
		else {
			util::Vector<TaskInfo*> notAssignTaskList(alloc);
			for (size_t pos = 0; pos < pPlan->nodeList_.size(); pos++) {
				TaskInfo* taskInfo = jobInfo->taskInfoList_[pos];
				if (taskInfo->sqlType_ == SQLType::EXEC_RESULT
					|| taskInfo->isDml_ || taskInfo->sqlType_ == SQLType::EXEC_DDL) {
					taskInfo->loadBalance_ = getConnectedPId();
					taskInfo->nodePos_ = 0;
					jobInfo->pId_ = getConnectedPId();
				}
				else if (taskInfo->sqlType_ == SQLType::EXEC_SCAN) {
					taskInfo->type_ = static_cast<uint8_t>(TRANSACTION_SERVICE);
					SQLPreparedPlan::Node& node = pPlan->nodeList_[pos];
					taskInfo->loadBalance_ = node.tableIdInfo_.partitionId_;
					if (node.tableIdInfo_.isNodeExpansion_) {
						taskInfo->type_ = static_cast<uint8_t>(SQL_SERVICE);
						taskInfo->nodePos_ = activeNodeListMap[liveNodeIdList[node.tableIdInfo_.subContainerId_]];
						continue;
					}
					NodeId ownerNodeId = UNDEF_NODEID;
					ownerNodeId = pt->getNewSQLOwner(node.tableIdInfo_.partitionId_);
					if (ownerNodeId == UNDEF_NODEID
						|| static_cast<int32_t>(activeNodeListMap.size()) <= ownerNodeId) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INVALID_NODE_ASSIGN,
							"Invalid node assign (pId=" << node.tableIdInfo_.partitionId_
							<< ", nodeId=" << ownerNodeId
							<< ", nodeNum=" << nodeNum << ")");
					}
					taskInfo->nodePos_ = activeNodeListMap[ownerNodeId];
					if (taskInfo->nodePos_ == -1 || taskInfo->nodePos_ >= nodeNum) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INVALID_NODE_ASSIGN,
							"Invalid node assign (pId=" << node.tableIdInfo_.partitionId_
							<< ", nodeId=" << ownerNodeId
							<< ", nodePos=" << taskInfo->nodePos_ << ", nodeNum=" << nodeNum << ")");
					}
					checkNodes[taskInfo->nodePos_] = true;
				}
				else {
					if (rootAssignTaskList[pos] == 1) {
						taskInfo->nodePos_ = 0;
						taskInfo->loadBalance_ = -1;
					}
					else {
						notAssignTaskList.push_back(taskInfo);
					}
				}
			}
			for (size_t i = 0; i < notAssignTaskList.size(); i++) {
				TaskInfo* taskInfo = notAssignTaskList[i];
				if (taskInfo->inputList_.size() == 0) {
					int32_t min = INT32_MAX;
					size_t pos = 0;
					for (size_t j = 0; j < assignNodes.size(); j++) {
						if (min > assignNodes[j]) {
							min = assignNodes[j];
							pos = j;
						}
					}
					taskInfo->nodePos_ = static_cast<int32_t>(pos);
					if (taskInfo->nodePos_ == -1 || taskInfo->nodePos_ >= nodeNum) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INVALID_NODE_ASSIGN,
							"Invalid node assign (nodePos=" << taskInfo->nodePos_ << ", nodeNum=" << nodeNum << ")");
					}
					assignNodes[taskInfo->nodePos_]++;
					checkNodes[taskInfo->nodePos_] = true;
				}
				else if (taskInfo->inputList_.size() == 1) {
					TaskInfo* taskInfo1 = jobInfo->taskInfoList_[taskInfo->inputList_[0]];
					taskInfo->nodePos_ = taskInfo1->nodePos_;
					if (taskInfo->nodePos_ == -1 || taskInfo->nodePos_ >= nodeNum) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INVALID_NODE_ASSIGN,
							"Invalid node assign (nodePos=" << taskInfo->nodePos_ << ", nodeNum=" << nodeNum << ")");
					}
					assignNodes[taskInfo->nodePos_]++;
					checkNodes[taskInfo->nodePos_] = true;
				}
				else {
					int32_t nodePos = static_cast<int32_t>(executionManager_->incNodeCounter() % nodeNum);
					taskInfo->nodePos_ = nodePos;
					if (taskInfo->nodePos_ == -1 || taskInfo->nodePos_ >= nodeNum) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INVALID_NODE_ASSIGN,
							"Invalid node assign (nodePos=" << taskInfo->nodePos_ << ", nodeNum=" << nodeNum << ")");
					}
					assignNodes[nodePos]++;
					checkNodes[nodePos] = true;
				}
				taskInfo->loadBalance_ = -1;
			}
		}
		for (size_t nodeId = 0; nodeId < liveNodeIdList.size(); nodeId++) {
			jobInfo->createAssignInfo(pt, liveNodeIdList[nodeId],
				(nodeNum == 1), jobManager_);
		}
		executionStatus_.setMaxRows(config_.maxRows_);
		jobInfo->storeMemoryAgingSwapRate_ = context_.getStoreMemoryAgingSwapRate();
		jobInfo->timezone_ = context_.getTimezone();
		jobInfo->dbId_ = context_.getDBId();
		jobInfo->dbName_ = context_.getDBName();
		jobInfo->appName_ = context_.getApplicationName();
		jobInfo->userName_ = context_.getUserName().c_str();

		jobInfo->isAdministrator_ = context_.isAdministrator();
		jobInfo->setup(config_.queryTimeout_, emNow, jobInfo->startTime_,
			isCurrentTimeRequired(), true);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e,
			"Create job information failed, clientId=" << clientId_
			<< ", executionId=" << context_.getExecId() << ", reason=" << GS_EXCEPTION_MESSAGE(e));
	}
}

/*!
	@brief フェッチ処理を実行する
	@param [in] ec イベントコンテキスト
	@param [in] request リクエスト
	@return なし
*/
void SQLExecution::fetch(
		EventContext& ec, Event& ev, RequestInfo& request) {
	UNUSED_VARIABLE(ev);

	try {
		setRequest(request);
		checkCancel();

		if (executionStatus_.execId_ != request.stmtId_) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_INVALID_QUERY_EXECUTION_ID,
				"Invalid statement execution number, expected="
				<< request.stmtId_ << ", current=" << executionStatus_.execId_);
		}
		CommandId requestCommandNo = request.sqlCount_;
		if (requestCommandNo > 1) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_CLIENT_REQUEST_PROTOCOL_ERROR,
				"Invalid statement command number, expected="
				<< requestCommandNo << ", current = 1");
		}
		JobId jobId;
		context_.getCurrentJobId(jobId);
		JobManager::Latch latch(jobId, "SQLExecution::fetch", jobManager_);
		Job* job = latch.get();
		if (job) {
			getContext().checkPendingException(true);
			job->fetch(ec, this);
		}
		else {
			try {
				getContext().checkPendingException(true);
				GS_THROW_USER_ERROR(GS_ERROR_JOB_CANCELLED,
					"Cancel job, jobId=" << jobId << ", (fetch)");
			}
			catch (std::exception& e) {
				replyError(ec, StatementHandler::TXN_STATEMENT_ERROR, e);
				if (!context_.isPreparedStatement()) {
					executionManager_->remove(ec, clientId_, false, 52, &jobId);
				}
			}
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "Fetch failed, clientId= " << clientId_
			<< ", executionId=" << request.stmtId_ << ",reason="
			<< GS_EXCEPTION_MESSAGE(e));
	}
}

bool SQLExecution::fetch(EventContext& ec, SQLFetchContext& cxt,
	ExecutionId execId, uint8_t versionId) {
	try {
		checkCancel();
		checkJobVersionId(versionId, true, NULL);
		if (executionStatus_.responsed_) {
			return false;
		}
		util::StackAllocator& alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);

		TupleValue::VarContext varCxt;
		varCxt.setStackAllocator(&alloc);
		varCxt.setVarAllocator(&localVarAlloc_);
		varCxt.setGroup(&group_);
		cxt.reader_->setVarContext(varCxt);

		if (isBatch()) {
			if (!isNotSelect()) {
				setBatchCount(0);
			}
			else if (cxt.reader_->exists()) {
				TupleList::Column& column = cxt.columnList_[0];
				int64_t updateCount = cxt.reader_->get().getAs<int64_t>(column);
				setBatchCount(updateCount);
			}
			else {
				setBatchCount(0);
			}
			if (isBatchComplete()) {
				return fetchBatch(
							ec, &batchCountList_);
			}
			else {
				RequestInfo request(alloc, false);
				execute(ec, request, true, NULL, 0, NULL);
				return false;
			}
		}
		else {
			if (isNotSelect()) {
				return fetchNotSelect(ec, cxt.reader_, cxt.columnList_);
			}
			else {
				return fetchSelect(ec, cxt.reader_, cxt.columnList_,
					cxt.columnSize_, cxt.isCompleted_);
			}
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "Fetch failed, clientId=" << clientId_
			<< ", executionId=" << execId << ", reason="
			<< GS_EXCEPTION_MESSAGE(e));
	}
}

void SQLExecution::setAnyTypeData(util::StackAllocator& alloc, const TupleValue& value) {
	switch (value.getType()) {
	case TupleList::TYPE_STRING: {
		resultSet_->setVariable(COLUMN_TYPE_STRING,
			reinterpret_cast<const char*>(value.varData()), value.varSize());
		break;
	}
	case TupleList::TYPE_BLOB: {
		util::XArray<uint8_t> blobs(alloc);
		generateBlobValue(value, blobs);
		resultSet_->setVariable(COLUMN_TYPE_BLOB,
			reinterpret_cast<const char*>(blobs.data()), blobs.size());
		break;
	}
	case TupleList::TYPE_BYTE: {
		resultSet_->setFixedValueWithPadding<int8_t>(
			COLUMN_TYPE_BYTE, value.fixedData());
		break;
	}
	case TupleList::TYPE_SHORT: {
		resultSet_->setFixedValueWithPadding<int16_t>(
			COLUMN_TYPE_SHORT, value.fixedData());
		break;
	}
	case TupleList::TYPE_INTEGER: {
		resultSet_->setFixedValueWithPadding<int32_t>(
			COLUMN_TYPE_INT, value.fixedData());
		break;
	}
	case TupleList::TYPE_LONG: {
		resultSet_->setFixedValue<int64_t>(
			COLUMN_TYPE_LONG, value.fixedData());
		break;
	}
	case TupleList::TYPE_FLOAT: {
		resultSet_->setFixedValueWithPadding<float>(
			COLUMN_TYPE_FLOAT, value.fixedData());
		break;
	}
	case TupleList::TYPE_NUMERIC:
	case TupleList::TYPE_DOUBLE: {
		resultSet_->setFixedValue<double>(
			COLUMN_TYPE_DOUBLE, value.fixedData());
		break;
	}
	case TupleList::TYPE_TIMESTAMP: {
		resultSet_->setFixedValue<int64_t>(
			COLUMN_TYPE_TIMESTAMP, value.fixedData());
		break;
	}
	case TupleList::TYPE_MICRO_TIMESTAMP: {
		resultSet_->setFixedValue<MicroTimestamp>(
			COLUMN_TYPE_MICRO_TIMESTAMP, value.fixedData());
		break;
	}
	case TupleList::TYPE_NANO_TIMESTAMP: {
		resultSet_->setLargeFixedFieldValueWithPadding(
			COLUMN_TYPE_NANO_TIMESTAMP,
			value.get<TupleNanoTimestamp>().get());
		break;
	}
	case TupleList::TYPE_BOOL: {
		resultSet_->setFixedValueWithPadding<bool>(
			COLUMN_TYPE_BOOL, value.fixedData());
		break;
	}
	case TupleList::TYPE_NULL: {
		resultSet_->setFixedNull();
		break;
	}
	default:
		GS_THROW_USER_ERROR(GS_ERROR_SQL_OPTYPE_UNSUPPORTED, "");
	}
}

void SQLExecution::cancel(EventContext& ec, ExecutionId execId) {
	try {
		isCancelled_ = true;
		if (executionStatus_.execId_ > execId) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_INVALID_QUERY_EXECUTION_ID,
				"Invalid statement execution number, expected="
				<< execId << ", min=" << executionStatus_.execId_);
		}
		else if (execId != MAX_STATEMENTID && executionStatus_.execId_ < execId) {
			executionStatus_.pendingCancelId_ = execId;
		}
		else {
			executeCancel(ec, true);
		}
	}
	catch (std::exception& e) {
		UTIL_TRACE_EXCEPTION_INFO(SQL_SERVICE, e, "");
	}
}

void SQLExecution::close(EventContext& ec, RequestInfo& request) {
	try {
		setRequest(request);
		executionStatus_.currentType_ = SQL_EXEC_CLOSE;
		SQLReplyContext replyCxt;
		replyCxt.set(&ec, NULL, UNDEF_JOB_VERSIONID, NULL);
		replyClient(replyCxt);
		executeCancel(ec);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "Close SQL execution failed, reason = "
			<< GS_EXCEPTION_MESSAGE(e));
	}
}

void SQLExecution::executeCancel(EventContext& ec, bool clientCancel) {
	UNUSED_VARIABLE(ec);
	UNUSED_VARIABLE(clientCancel);

	GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK, GS_TRACE_SQL_INTERNAL_DEBUG,
		"Execute cancel, clientId=" << clientId_);
	std::exception occurredException;
	bool errored = false;
	isCancelled_ = true;
	syncContext_->cancel();
	if (errored) {
		GS_RETHROW_USER_OR_SYSTEM(occurredException, "");
	}
}

bool SQLExecution::replySuccessInternal(EventContext& ec, int32_t type,
	uint8_t versionId, ReplyOption& option, JobId* responseJobId) {
	util::StackAllocator& alloc = ec.getAllocator();

	bool responsed = executionStatus_.responsed_;
	if (executionStatus_.currentType_ != SQL_EXEC_CLOSE && responsed) {
		GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK, GS_TRACE_SQL_INTERNAL_DEBUG,
			"Call reply success, but not executed because of already responded,"
			"clientId=" << clientId_);
		return false;
	}
	if (!checkJobVersionId(versionId, false, responseJobId)) {
		return false;
	}
	setResponse();

	Event replyEv(ec, SQL_EXECUTE_QUERY, getConnectedPId());
	replyEv.setPartitionIdSpecified(false);
	EventByteOutStream out = replyEv.getOutStream();

	switch (type) {
	case SQL_REPLY_SUCCESS: {
		ValueTypeList* typeList = option.typeList_;
		if (typeList && typeList->size() == 0) {
			response_.existsResult_ = false;
		}
		encodeSuccess(alloc, out, typeList);
	}
						  break;
	case SQL_REPLY_SUCCESS_EXPLAIN: {
		encodeSuccessExplain(alloc, out);
	}
								  break;
	case SQL_REPLY_SUCCESS_EXPLAIN_ANALYZE: {
		Job* job = option.job_;
		encodeSuccessExplainAnalyze(alloc, out, job);
		break;
	}
	default:
		GS_THROW_USER_ERROR(GS_ERROR_SQL_UNSUPPORTED, "");
	}
	sendClient(ec, replyEv);
	if (context_.isPreparedStatement()) {
		executionStatus_.reset();
		if (!response_.isContinue_) {
			response_.reset();
			resetJobVersion();
		}
	}
	return true;
}

bool SQLExecution::replySuccess(EventContext& ec,
	ValueTypeList* typeList, uint8_t versionId, JobId* responseJobId) {
	ReplyOption option;
	option.typeList_ = typeList;
	return replySuccessInternal(
		ec, SQL_REPLY_SUCCESS, versionId, option, responseJobId);
}

bool SQLExecution::replySuccessExplain(
	EventContext& ec, uint8_t versionId, JobId* responseJobId) {
	ReplyOption option;
	bool retVal = replySuccessInternal(ec, SQL_REPLY_SUCCESS_EXPLAIN,
		versionId, option, responseJobId);
	if (!context_.isPreparedStatement()) {
		executionManager_->remove(ec, clientId_, false, 53, responseJobId);
	}
	return retVal;
}

void SQLExecution::encodeSuccess(util::StackAllocator& alloc,
	EventByteOutStream& out, ValueTypeList* typeList) {

	out << response_.stmtId_;
	out << StatementHandler::TXN_STATEMENT_SUCCESS;

	StatementHandler::encodeBooleanData<EventByteOutStream>(out, response_.transactionStarted_);
	StatementHandler::encodeBooleanData<EventByteOutStream>(out, response_.isAutoCommit_);
	StatementHandler::encodeIntData<EventByteOutStream, int32_t>(
		out, static_cast<int32_t>(response_.resultCount_));

	if (response_.resultCount_ > 0) {
		StatementHandler::encodeIntData<EventByteOutStream, int32_t>(
			out, static_cast<int32_t>(response_.updateCount_));
		StatementHandler::encodeIntData<EventByteOutStream, int32_t>(out, response_.parameterCount_);
		StatementHandler::encodeBooleanData<EventByteOutStream>(out, response_.existsResult_);
		if (response_.existsResult_) {
			StatementHandler::encodeBooleanData<EventByteOutStream>(out, response_.isContinue_);
			out << static_cast<int32_t>(response_.resultSet_->getRowCount());
			if (!typeList) {
				response_.resultSet_->exportSchema(alloc, out);
			}
			else {
				size_t placeHolderCount = typeList->size();
				if (placeHolderCount > 0) {
					const size_t startPos = out.base().position();
					StatementHandler::encodeIntData<EventByteOutStream, uint32_t>(out, 0);
					StatementHandler::encodeIntData<EventByteOutStream, uint32_t>(
						out, static_cast<uint32_t>(placeHolderCount));
					uint32_t placeHolderPos;
					for (placeHolderPos = 0; placeHolderPos < placeHolderCount; placeHolderPos++) {
						const util::String emptyColumnName("", alloc);
						StatementHandler::encodeStringData<EventByteOutStream>(
							out, emptyColumnName);
						const bool withAny = true;
						const ColumnType columnType = convertTupleTypeToNoSQLType(
								(*typeList)[placeHolderPos]);
						checkColumnTypeFeatureVersion(columnType);
						const int8_t typeOrdinal =
								ValueProcessor::getPrimitiveColumnTypeOrdinal(
										columnType, withAny);
						out << typeOrdinal;
						const uint8_t flags = MessageSchema::makeColumnFlags(
								ValueProcessor::isArray(columnType), false, false);
						out << flags;
					}
					const int16_t keyCount = 0;
					StatementHandler::encodeIntData<EventByteOutStream>(out, keyCount);
					for (placeHolderPos = 0; placeHolderPos < placeHolderCount; placeHolderPos++) {
						const util::String emptyColumnName("", alloc);
						StatementHandler::encodeStringData<EventByteOutStream>(
							out, emptyColumnName);
					}
					const size_t endPos = out.base().position();
					out.base().position(startPos);
					StatementHandler::encodeIntData<EventByteOutStream, uint32_t>(
						out, static_cast<uint32_t>(endPos - startPos - sizeof(uint32_t)));
					out.base().position(endPos);
				}
			}
			const size_t startPos = out.base().position();
			StatementHandler::encodeIntData<EventByteOutStream, uint32_t>(out, 0);
			const uint64_t varDataBaseOffset = 0;
			out << varDataBaseOffset;
			if (response_.resultSet_->getRowCount() != 0) {
				response_.resultSet_->exportData(out);
			}
			const size_t endPos = out.base().position();
			out.base().position(startPos);
			StatementHandler::encodeIntData<EventByteOutStream, uint32_t>(
				out, static_cast<uint32_t>(endPos - startPos - sizeof(uint32_t)));
			out.base().position(endPos);
		}
	}
	SQLRequestHandler::encodeEnvList(out, alloc,
		*executionManager_,
		context_.getClientNd().getUserData<StatementHandler::ConnectionOption>());
}

void SQLExecution::encodeSuccessExplain(util::StackAllocator& alloc,
	EventByteOutStream& out) {
	SQLResultSet resultSet(globalVarAlloc_);
	util::Vector<TupleList::TupleColumnType> typeList(alloc);
	typeList.push_back(TupleList::TYPE_STRING);
	util::Vector<util::String> columnNameList(alloc);
	resultSet.getColumnNameList(alloc, columnNameList);
	columnNameList.push_back(util::String(alloc));
	resultSet.setColumnNameList(columnNameList);
	resultSet.setColumnTypeList(typeList);
	resultSet.init(alloc);
	TupleValue::VarContext varCxt;
	varCxt.setStackAllocator(&alloc);
	varCxt.setVarAllocator(&localVarAlloc_);
	varCxt.setGroup(&group_);
	SQLPreparedPlan* plan = executionStatus_.preparedPlan_;
	for (size_t planId = 0; planId < plan->nodeList_.size(); planId++) {
		resultSet.next(alloc);
		picojson::value jsonOutValue;
		JsonUtils::OutStream out(jsonOutValue);
		TupleValue::coder(
			util::ObjectCoder(), &varCxt).encode(out, (*plan).nodeList_[planId]);
		util::String tmpStr(alloc);
		tmpStr = jsonOutValue.serialize().c_str();
		resultSet.setVarFieldValue(
			reinterpret_cast<const char*>(tmpStr.data()),
			static_cast<uint32_t>(tmpStr.size()));
	}
	out << response_.stmtId_;
	out << StatementHandler::TXN_STATEMENT_SUCCESS;
	StatementHandler::encodeBooleanData<EventByteOutStream>(out, response_.transactionStarted_);
	StatementHandler::encodeBooleanData<EventByteOutStream>(out, response_.isAutoCommit_);
	out << static_cast<int32_t>(response_.resultCount_);
	out << static_cast<int32_t>(0);
	out << static_cast<int32_t>(0);
	StatementHandler::encodeBooleanData<EventByteOutStream>(out, true);
	StatementHandler::encodeBooleanData<EventByteOutStream>(out, false);
	out << static_cast<int32_t>(resultSet.getRowCount());
	resultSet.exportSchema(alloc, out);
	const size_t startPos = out.base().position();
	out << static_cast<uint32_t>(0);
	const uint64_t varDataBaseOffset = 0;
	out << varDataBaseOffset;
	resultSet.exportData(out);
	const size_t endPos = out.base().position();
	out.base().position(startPos);
	out << static_cast<uint32_t>(endPos - startPos - sizeof(uint32_t));
	out.base().position(endPos);
}

void SQLExecution::encodeSuccessExplainAnalyze(
	util::StackAllocator& eventStackAlloc,
	EventByteOutStream& out, Job* job) {

	SQLResultSet resultSet(globalVarAlloc_);
	util::Vector<TupleList::TupleColumnType> typeList(eventStackAlloc);
	typeList.push_back(TupleList::TYPE_STRING);
	util::Vector<util::String> columnNameList(eventStackAlloc);
	resultSet.getColumnNameList(eventStackAlloc, columnNameList);
	columnNameList.push_back(util::String(eventStackAlloc));
	resultSet.setColumnNameList(columnNameList);
	resultSet.setColumnTypeList(typeList);
	resultSet.init(eventStackAlloc);

	TupleValue::VarContext varCxt;
	varCxt.setStackAllocator(&eventStackAlloc);
	varCxt.setVarAllocator(&localVarAlloc_);
	varCxt.setGroup(&group_);

	SQLPreparedPlan* plan = NULL;
	restorePlanInfo(plan, eventStackAlloc, varCxt);
	cleanupSerializedData();
	if (plan) {
		for (size_t planId = 0; planId < plan->nodeList_.size(); planId++) {
			resultSet.next(eventStackAlloc);
			picojson::value jsonPlanOutValue;
			JsonUtils::OutStream planOut(jsonPlanOutValue);
			TaskProfiler profile;
			job->getProfiler(eventStackAlloc, profile, planId);
			SQLPreparedPlan::Node& node = (*plan).nodeList_[planId];
			TupleValue::VarContext::Scope varScope(varCxt);
			try {
				TupleValue::coder(util::ObjectCoder(), &varCxt).encode(planOut, node);
			}
			catch (std::exception& e) {
			}
			{
				SQLProcessor::Profiler::mergeResultToPlan(
						eventStackAlloc, profile, jsonPlanOutValue);
				const u8string &tmpStr = jsonPlanOutValue.serialize();

				resultSet.setVarFieldValue(tmpStr.data(), tmpStr.size());
			}
			ALLOC_DELETE(eventStackAlloc, profile.rows_);
			ALLOC_DELETE(eventStackAlloc, profile.address_);
		}
	}
	executionStatus_.preparedPlan_ = NULL;

	out << response_.stmtId_;
	out << StatementHandler::TXN_STATEMENT_SUCCESS;
	StatementHandler::encodeBooleanData<EventByteOutStream>(out, response_.transactionStarted_);
	StatementHandler::encodeBooleanData<EventByteOutStream>(out, response_.isAutoCommit_);
	out << static_cast<int32_t>(response_.resultCount_);
	out << static_cast<int32_t>(0);
	out << static_cast<int32_t>(0);
	StatementHandler::encodeBooleanData<EventByteOutStream>(out, true);
	StatementHandler::encodeBooleanData<EventByteOutStream>(out, false);
	out << static_cast<int32_t>(resultSet.getRowCount());
	resultSet.exportSchema(eventStackAlloc, out);
	const size_t startPos = out.base().position();
	out << static_cast<uint32_t>(0);
	const uint64_t varDataBaseOffset = 0;
	out << varDataBaseOffset;
	resultSet.exportData(out);
	const size_t endPos = out.base().position();
	out.base().position(startPos);
	out << static_cast<uint32_t>(endPos - startPos - sizeof(uint32_t));
	out.base().position(endPos);
}

bool SQLExecution::replySuccessExplainAnalyze(
	EventContext& ec, Job* job, uint8_t versionId, JobId* responseJobId) {
	ReplyOption option;
	option.job_ = job;
	bool retVal = replySuccessInternal(ec,
		SQL_REPLY_SUCCESS_EXPLAIN_ANALYZE, versionId, option, responseJobId);
	if (!context_.isPreparedStatement()) {
		executionManager_->remove(ec, clientId_, false, 54, responseJobId);
	}
	return retVal;
}


void SQLExecution::sendClient(EventContext& ec, Event& ev) {
	ec.getEngine().send(ev, context_.getClientNd());
	executionStatus_.responsed_ = true;
}

bool SQLExecution::replyError(EventContext& ec,
	StatementHandler::StatementExecStatus status, std::exception& e) {
	bool responsed = executionStatus_.responsed_;
	if (executionStatus_.currentType_ != SQL_EXEC_CLOSE && responsed) {
		GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK, GS_TRACE_SQL_INTERNAL_DEBUG,
			"Call reply success, but not executed because of already responded,"
			"clientId=" << clientId_);
		context_.setPendingException(e);
		return false;
	}
	UTIL_TRACE_EXCEPTION(SQL_SERVICE, e, "");
	Event replyEv(ec, SQL_EXECUTE_QUERY, getConnectedPId());
	StatementHandler::setErrorReply(replyEv, response_.stmtId_,
		status, e, context_.getClientNd());
	sendClient(ec, replyEv);
	return true;
}

void SQLExecution::setResponse() {
	if (isNotSelect()) {
		response_.existsResult_ = false;
		return;
	}
	switch (executionStatus_.requestType_) {
	case REQUEST_TYPE_EXECUTE:
	case REQUEST_TYPE_QUERY:
	case REQUEST_TYPE_FETCH:
	case REQUEST_TYPE_PRAGMA:
		response_.existsResult_ = true;
		break;
	case REQUEST_TYPE_UPDATE:
		response_.existsResult_ = true;
		break;
	case REQUEST_TYPE_PREPARE:
		response_.existsResult_ = true;
		break;
	case REQUEST_TYPE_CLOSE:
		response_.existsResult_ = false;
	default:
		break;
	}
}

NoSQLStore* SQLExecution::getNoSQLStore() {
	return dbConnection_->getNoSQLStore(
		context_.getDBId(), context_.getDBName());
}

SQLExecution::ErrorFormatter::ErrorFormatter(
	SQLExecution& execution, const std::exception& cause) :
	execution_(execution),
	cause_(cause) {
}

void SQLExecution::ErrorFormatter::format(std::ostream& os) const {
	os << GS_EXCEPTION_MESSAGE(cause_);
	os << " on ";
	bool hasQuery = false;
	switch (execution_.executionStatus_.requestType_) {
	case REQUEST_TYPE_EXECUTE:
		os << "executing statement";
		hasQuery = true;
		break;
	case REQUEST_TYPE_PREPARE:
		os << "preparing statement";
		hasQuery = true;
		break;
	case REQUEST_TYPE_QUERY:
		os << "executing query";
		hasQuery = true;
		break;
	case REQUEST_TYPE_UPDATE:
		os << "updating";
		hasQuery = true;
		break;
	case REQUEST_TYPE_PRAGMA:
		os << "executing statement(metadata)";
		hasQuery = true;
		break;
	case REQUEST_TYPE_FETCH:
		os << "fetching";
		break;
	case REQUEST_TYPE_CLOSE:
		os << "closing";
		break;
	case REQUEST_TYPE_CANCEL:
		os << "cancelling";
		break;
	default:
		break;
	}
	if (execution_.getContext().isPreparedStatement()) {
		os << " (prepared) ";
	}
	if (hasQuery) {
		os << " (sql=\"" << execution_.getContext().getQuery() << "\")";
	}
	os << " (db=\'" << execution_.getContext().getDBName() << "\')";
	os << " (user=\'" << execution_.getContext().getUserName() << "\')";

	const char* appName = execution_.getContext().getApplicationName();
	if (appName && strlen(appName) > 0) {
		os << " (appName=\'" << appName << "\')";
	}
	os << " (clientId=\'" << execution_.getContext().getId() << "\')";
	os << " (source=" << execution_.getContext().getClientNd() << ")";
	if (execution_.getContext().getConnectionOption().isPublicConnection()) {
		os << " (connection=PUBLIC)";
	}
}

std::ostream& operator<<(
	std::ostream& os, const SQLExecution::ErrorFormatter& formatter) {
	formatter.format(os);
	return os;
}

void SQLExecutionManager::dump() {
	GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK, GS_TRACE_SQL_INTERNAL_DEBUG,
		"ExecutionManager : execution entry=" << executionMap_.size());
	for (SQLExecutionMap::iterator it = executionMap_.begin();
		it != executionMap_.end();it++) {
		SQLExecution* execution = (*it).second;
		GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK, GS_TRACE_SQL_INTERNAL_DEBUG,
			"Execution : ExecId=" << execution->getContext().getId());
	}
}

void SQLExecutionManager::dumpTableCache(
	util::NormalOStringStream& oss, const char* dbName) {
	dbConnection_->dump(oss, dbName);
}

void SQLExecutionManager::resizeTableCache(
	const char* dbName, int32_t cacheSize) {
	dbConnection_->resizeTableCache(dbName, cacheSize);
}

void SQLExecution::checkClientResponse() {
	if (analyzedQueryInfo_.isExplain() || analyzedQueryInfo_.isExplainAnalyze()) {
		return;
	}
	if (!isNotSelect()) {
		if (executionStatus_.requestType_ == REQUEST_TYPE_UPDATE) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_STATEMENT_CATEGORY_UNMATCHED,
				"ExecuteUpdate() can not use for query with result set, use executeQuery() or execute()");
		}
	}
	else {
		if (executionStatus_.requestType_ == REQUEST_TYPE_QUERY) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_STATEMENT_CATEGORY_UNMATCHED,
				"ExecuteQuery() can not use for query without result set, use executeUpdate() or execute()");
		}
	}
}

TupleValue SQLExecution::getTupleValue(TupleValue::VarContext& varCxt,
	SyntaxTree::Expr* targetExpr,
	TupleList::TupleColumnType type,
	util::Vector<BindParam*>& bindParamInfos) {
	const bool implicit = true;
	if (targetExpr->op_ == SQLType::EXPR_CONSTANT) {
		return SQLCompiler::ProcessorUtils::convertType(
			varCxt, targetExpr->value_, type, implicit);
	}
	else {
		assert(bindParamInfos.size() > targetExpr->placeHolderPos_);
		return SQLCompiler::ProcessorUtils::convertType(
			varCxt, bindParamInfos[targetExpr->placeHolderPos_]->value_, type, implicit);
	}
}

void SQLExecution::checkColumnTypeFeatureVersion(ColumnType columnType) {
	StatementHandler::checkSchemaFeatureVersion(
			ValueProcessor::getSchemaFeatureLevel(columnType),
			context_.clientInfo_.acceptableFeatureVersion_);
}

struct BulkEntry {

	BulkEntry(util::StackAllocator& eventStackAlloc,
		const DataStoreConfig& dsConfig,
		ColumnInfo* columnInfoList,
		uint32_t columnCount,
		TargetContainerInfo& targetInfo) :
		eventStackAlloc_(eventStackAlloc), fixedPart_(NULL), varPart_(NULL),
		columnInfoList_(NULL),
		columnCount_(columnCount),
		dsConfig_(dsConfig), rowStore_(NULL),
		containerId_(targetInfo.containerId_), pId_(targetInfo.pId_),
		versionId_(targetInfo.versionId_), affinity_(targetInfo.affinity_) {
		init(columnInfoList, columnCount);
	}

	void init(ColumnInfo* columnInfoList, uint32_t columnCount) {
		ALLOC_DELETE(eventStackAlloc_, fixedPart_);
		ALLOC_DELETE(eventStackAlloc_, varPart_);
		ALLOC_DELETE(eventStackAlloc_, rowStore_);
		fixedPart_ = ALLOC_NEW(eventStackAlloc_) util::XArray<uint8_t>(eventStackAlloc_);
		varPart_ = ALLOC_NEW(eventStackAlloc_) util::XArray<uint8_t>(eventStackAlloc_);
		if (!columnInfoList_) {
			ALLOC_DELETE(eventStackAlloc_, columnInfoList_);
			columnInfoList_ = ALLOC_NEW(eventStackAlloc_) ColumnInfo[columnCount];
			memcpy(columnInfoList_, columnInfoList, sizeof(ColumnInfo) * columnCount);
		}
		rowStore_ = ALLOC_NEW(eventStackAlloc_) OutputMessageRowStore(
			dsConfig_, columnInfoList_, columnCount_, *fixedPart_, *varPart_, false);
	}

	util::StackAllocator& eventStackAlloc_;
	util::XArray<uint8_t>* fixedPart_;
	util::XArray<uint8_t>* varPart_;
	ColumnInfo* columnInfoList_;
	uint32_t columnCount_;
	const DataStoreConfig& dsConfig_;
	OutputMessageRowStore* rowStore_;
	ContainerId containerId_;
	PartitionId pId_;
	SchemaVersionId versionId_;
	NodeAffinityNumber affinity_;
	int64_t getSize() {
		return (fixedPart_->size() + varPart_->size());
	}
};

class BulkEntryManager {
public:
	BulkEntryManager(util::StackAllocator& eventStackAlloc, uint8_t partitioningType,
		int32_t partitioningNum, const DataStoreConfig& config,
		ColumnInfo* columnInfoList, uint32_t columnCount) :
		eventStackAlloc_(eventStackAlloc), storeList_(eventStackAlloc_),
		storeMapList_(eventStackAlloc_),
		partitioningType_(partitioningType), partitioningNum_(partitioningNum),
		config_(config), columnInfoList_(columnInfoList),
		columnCount_(columnCount), currentPos_(0), prevPos_(0), prevStore_(NULL) {
		switch (partitioningType_) {
		case SyntaxTree::TABLE_PARTITION_TYPE_UNDEF:
		case SyntaxTree::TABLE_PARTITION_TYPE_HASH:
			storeList_.assign(partitioningNum, static_cast<BulkEntry*>(NULL));
			break;
		default:
			break;
		}
	}

	BulkEntry* add(NodeAffinityNumber affinity,
		TableSchemaInfo* schema, TargetContainerInfo& targetInfo) {
		switch (partitioningType_) {
		case SyntaxTree::TABLE_PARTITION_TYPE_UNDEF:
		case SyntaxTree::TABLE_PARTITION_TYPE_HASH: {
			size_t targetPos = static_cast<size_t>(affinity);
			if (targetPos == static_cast<size_t>(-1) ||
					storeList_.size() <= targetPos) {
				GS_THROW_USER_ERROR(GS_ERROR_TXN_CONTAINER_SCHEMA_UNMATCH, "");
			}
			if (storeList_[targetPos] != NULL) {
				return storeList_[targetPos];
			}
			else {
				storeList_[targetPos] = ALLOC_NEW(eventStackAlloc_) BulkEntry(
					eventStackAlloc_, config_,
					schema->nosqlColumnInfoList_,
					static_cast<uint32_t>(schema->columnInfoList_.size()), targetInfo);
				return storeList_[targetPos];
			}
		}
												  break;
		case SyntaxTree::TABLE_PARTITION_TYPE_RANGE:
		case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH: {
			util::Map<NodeAffinityNumber, BulkEntry*>::iterator it = storeMapList_.find(affinity);
			if (it == storeMapList_.end()) {
				BulkEntry* store = ALLOC_NEW(eventStackAlloc_) BulkEntry(
					eventStackAlloc_, config_,
					schema->nosqlColumnInfoList_,
					static_cast<uint32_t>(schema->columnInfoList_.size()), targetInfo);
				storeMapList_.insert(std::make_pair(affinity, store));
				storeList_.push_back(store);
				return store;
			}
			else {
				return (*it).second;
			}
		}
		}
		return NULL;
	}

	void begin() {
		currentPos_ = 0;
		prevPos_ = 0;
		prevStore_ = NULL;
	}

	BulkEntry* next() {
		switch (partitioningType_) {
		case SyntaxTree::TABLE_PARTITION_TYPE_UNDEF:
		case SyntaxTree::TABLE_PARTITION_TYPE_HASH: {
			if (prevStore_) {
				ALLOC_DELETE(eventStackAlloc_, prevStore_);
				storeList_[prevPos_] = NULL;
			}
			while (currentPos_ < storeList_.size()) {
				if (storeList_[currentPos_] != NULL) {
					prevPos_ = currentPos_;
					prevStore_ = storeList_[currentPos_];
					return storeList_[currentPos_];
				}
				currentPos_++;
			}
			return NULL;
		}
												  break;
		case SyntaxTree::TABLE_PARTITION_TYPE_RANGE:
		case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH: {
			if (prevStore_) {
				ALLOC_DELETE(eventStackAlloc_, storeList_[prevPos_]);
				storeList_[prevPos_] = static_cast<BulkEntry*>(NULL);
			}
			while (currentPos_ < storeList_.size()) {
				if (storeList_[currentPos_] != NULL) {
					prevPos_ = currentPos_;
					prevStore_ = storeList_[currentPos_];
					return storeList_[currentPos_];
				}
				currentPos_++;
			}
		}
														break;
		}
		return NULL;
	}

	util::StackAllocator& eventStackAlloc_;
	util::Vector<BulkEntry*> storeList_;
	util::Map<NodeAffinityNumber, BulkEntry*> storeMapList_;
	uint8_t partitioningType_;
	uint32_t partitioningNum_;
	const DataStoreConfig& config_;
	ColumnInfo* columnInfoList_;
	uint32_t columnCount_;
	size_t currentPos_;
	size_t prevPos_;
	BulkEntry* prevStore_;
};

bool SQLExecution::executeFastInsert(EventContext& ec,
	util::Vector<BindParam*>& bindParamInfos, bool useCache) {
	TableLatch* latch = NULL;
	util::StackAllocator& eventStackAlloc = ec.getAllocator();
	util::Stopwatch watch;
	watch.start();
	SQLParsedInfo& parsedInfo = getParsedInfo();
	PartitionTable* pt = executionManager_->getPartitionTable();
	const uint32_t partitionNum =
		executionManager_->getPartitionTable()->getPartitionNum();
	bool releaseCache = false;
	TableSchemaInfo* tableSchema = NULL;
	const char8_t* currentDBName = NULL;

	const DataStoreConfig* dsConfig = executionManager_->getManagerSet()->dsConfig_;
	try {
		int64_t startTime = util::DateTime::now(false).getUnixTime();
		getContext().setStartTime(startTime);
		ExecutionProfilerWatcher watcher(this, true, true);
		JobId jobId;
		generateJobId(jobId, getContext().executionStatus_.execId_);

		util::Vector<SyntaxTree::ExprList*>& mergeSelectList = getMergeSelectList();
		assert(mergeSelectList.size() > 0);
		SyntaxTree::ExprList* selectList = mergeSelectList[0];
		NameWithCaseSensitivity tableName(
			parsedInfo.tableList_[0]->qName_->table_->c_str(),
			parsedInfo.tableList_[0]->qName_->tableCaseSensitive_);
		const char* dbName = NULL;
		bool isAdjust = false;
		bool dbCaseSensitive = false;
		if (parsedInfo.tableList_[0]->qName_->db_ == NULL) {
			isAdjust = true;
		}
		else {
			dbName = parsedInfo.tableList_[0]->qName_->db_->c_str();
			dbCaseSensitive = parsedInfo.tableList_[0]->qName_->dbCaseSensitive_;
			if (dbCaseSensitive) {
				checkConnectedDbName(
					eventStackAlloc, context_.getDBName(), dbName, dbCaseSensitive);
			}
			else {
				checkConnectedDbName(eventStackAlloc,
					context_.getNormalizedDBName(), dbName, dbCaseSensitive);
			}
		}
		NameWithCaseSensitivity dbNameWithCaseSensitive(dbName, dbCaseSensitive);
		CreateTableOption option(eventStackAlloc);
		DBConnection* conn = executionManager_->getDBConnection();
		if (isAdjust) {
			currentDBName = NULL;
		}
		else {
			currentDBName = dbName;
		}
		bool remoteUpdateCache = false;
		bool needSchemaRefresh = false;
		int64_t totalRowCount = 0;

		DatabaseId dbId = context_.getDBId();
		jobManager_->getTransactionService()->getManager()->getDatabaseManager().incrementRequest(dbId, true);

		try {
			latch = ALLOC_NEW(eventStackAlloc) TableLatch(ec,
				conn, this, currentDBName, tableName, useCache);
			tableSchema = latch->get();
			if (tableSchema == NULL) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_TABLE_NOT_FOUND,
					"Table name '" << tableName.name_ << "' not found");
			}
			if (!NoSQLCommonUtils::isWritableContainer(
				tableSchema->containerAttr_, COLLECTION_CONTAINER)) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DML_INVALID_CONTAINER_ATTRIBUTE,
					"Target table '" << tableName.name_ << "' is read-only table");
			}
			if (tableSchema->containerAttr_ == CONTAINER_ATTR_VIEW) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
					"Target table '" << tableName.name_ << "' is VIEW");
			}
			tableSchema->checkWritableContainer();
			if (tableSchema->partitionInfo_.partitionType_
				== SyntaxTree::TABLE_PARTITION_TYPE_HASH
				&& (tableSchema->containerInfoList_.size()
					!= tableSchema->partitionInfo_.partitioningNum_ + 1)) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_TABLE_NOT_FOUND,
					"Table name '" << tableName.name_
					<< "' is not found or already removed or under removing");
			}
			NoSQLStoreOption cmdOption(this);
			int32_t keyColumnId = tableSchema->partitionInfo_.partitioningColumnId_;
			int32_t subKeyColumnId = tableSchema->partitionInfo_.subPartitioningColumnId_;
			int32_t partitioningCount = tableSchema->partitionInfo_.getCurrentPartitioningCount();
			if (partitioningCount == 0) {
				partitioningCount = 1;
			}

			TupleValue::VarContext varCxt;
			varCxt.setStackAllocator(&eventStackAlloc);
			varCxt.setVarAllocator(&localVarAlloc_);
			varCxt.setGroup(&group_);
			BulkEntryManager bulkManager(eventStackAlloc,
				tableSchema->partitionInfo_.partitionType_,
				partitioningCount, *dsConfig,
				tableSchema->nosqlColumnInfoList_,
				static_cast<uint32_t>(tableSchema->columnInfoList_.size()));

			int64_t currentSize = 0;
			int64_t sizeLimit = executionManager_->getNoSQLSizeLimit();
			BulkEntry* currentStore;
			if (parsedInfo.syntaxTreeList_[0]->cmdOptionValue_
				== SyntaxTree::RESOLVETYPE_REPLACE) {
				cmdOption.putRowOption_ = PUT_INSERT_OR_UPDATE;
			}
			watcher.lap(ExecutionProfilerWatcher::PARSE);

			conn->getNoSQLStore(context_.getDBId(), context_.getDBName());
			for (size_t rowPos = 0; rowPos < mergeSelectList.size(); rowPos++) {
				if (tableSchema->columnInfoList_.size() != mergeSelectList[rowPos]->size()) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_MISMATCH_SCHEMA,
						"Specified column list count is unmatch with target table, expected="
						<< tableSchema->columnInfoList_.size()
						<< ", actual=" << mergeSelectList[rowPos]->size());
				}
				const TupleValue* value1 = NULL;
				const TupleValue* value2 = NULL;
				if (keyColumnId != -1) {
					value1 = ALLOC_NEW(eventStackAlloc) TupleValue(
						getTupleValue(varCxt, (*mergeSelectList[rowPos])[keyColumnId],
							tableSchema->columnInfoList_[keyColumnId].tupleType_, bindParamInfos));
				}
				if (subKeyColumnId != -1) {
					value2 = ALLOC_NEW(eventStackAlloc) TupleValue(
						getTupleValue(varCxt, (*mergeSelectList[rowPos])[subKeyColumnId],
							tableSchema->columnInfoList_[subKeyColumnId].tupleType_, bindParamInfos));
				}
				NameWithCaseSensitivity dbName(context_.getDBName(), false);
				TargetContainerInfo targetInfo;
				bool needRefresh = false;
				resolveTargetContainer(ec, value1, value2, conn, tableSchema,
					this, dbNameWithCaseSensitive, tableName, targetInfo, needRefresh);

				if (needRefresh) {
					ALLOC_DELETE(eventStackAlloc, latch);
					latch = NULL;
					latch = ALLOC_NEW(eventStackAlloc) TableLatch(ec,
						conn, this, currentDBName, tableName, false, true);
					tableSchema = latch->get();
					remoteUpdateCache = true;
					if (bulkManager.partitioningType_ != tableSchema->partitionInfo_.partitionType_) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL_INVALID_OPTION, "");
					}
				}
				NodeAffinityNumber pos;
				if (keyColumnId != -1) {
					if (SyntaxTree::isRangePartitioningType(
						tableSchema->partitionInfo_.partitionType_)) {
						pos = targetInfo.affinity_;
					}
					else {
						pos = targetInfo.pos_;
					}
				}
				else {
					TableContainerInfo* containerInfo;
					containerInfo = &tableSchema->containerInfoList_[0];
					targetInfo.affinity_ = 0;
					targetInfo.pId_ = containerInfo->pId_;
					targetInfo.containerId_ = containerInfo->containerId_;
					targetInfo.versionId_ = containerInfo->versionId_;
					pos = 0;
				}
				BulkEntry* store = bulkManager.add(pos, tableSchema, targetInfo);
				int64_t beforeSize = store->getSize();
				store->rowStore_->beginRow();

				for (size_t columnId = 0; columnId < selectList->size(); columnId++) {
					const TupleValue& value = getTupleValue(varCxt,
						(*mergeSelectList[rowPos])[columnId],
						tableSchema->columnInfoList_[columnId].tupleType_, bindParamInfos);
					DMLProcessor::setField(value.getType(), static_cast<ColumnId>(columnId),
						value, *store->rowStore_,
						tableSchema->nosqlColumnInfoList_[columnId].getColumnType());
				}
				store->rowStore_->next();
				currentSize += (store->getSize() - beforeSize);

				if (currentSize > sizeLimit) {
					BulkEntry* currentStore;
					bulkManager.begin();
					while ((currentStore = bulkManager.next()) != NULL) {
						NoSQLContainer container(ec, currentStore->containerId_,
							currentStore->versionId_, currentStore->pId_, *syncContext_, this);
						try {
							container.putRowSet(*currentStore->fixedPart_,
								*currentStore->varPart_, currentStore->rowStore_->getRowCount(), cmdOption);
						}
						catch (std::exception& e) {

							checkException(e);

							NoSQLContainer* newContainer = recoverySubContainer(
								eventStackAlloc, ec, getNoSQLStore(), this,
								tableName.name_, partitionNum, currentStore->affinity_);
							if (newContainer == NULL) {
								GS_RETHROW_USER_OR_SYSTEM(e, "");
							}
							container.updateInfo(newContainer->getContainerId(),
								newContainer->getVersionId());
							container.putRowSet(*currentStore->fixedPart_,
								*currentStore->varPart_,
								currentStore->rowStore_->getRowCount(), cmdOption);
							releaseCache = true;
						}

						totalRowCount += currentStore->rowStore_->getRowCount();
						currentStore->init(
							currentStore->columnInfoList_, currentStore->columnCount_);
					}
				}
			}
			bulkManager.begin();
			while ((currentStore = bulkManager.next()) != NULL) {
				NoSQLContainer container(ec, currentStore->containerId_,
					currentStore->versionId_, currentStore->pId_, *syncContext_, this);
				try {
					container.putRowSet(*currentStore->fixedPart_,
						*currentStore->varPart_,
						currentStore->rowStore_->getRowCount(), cmdOption);
				}
				catch (std::exception& e) {

					checkException(e);
					NoSQLContainer* newContainer = recoverySubContainer(
						eventStackAlloc, ec, getNoSQLStore(), this,
						tableName.name_, partitionNum, currentStore->affinity_);
					if (newContainer == NULL) {
						GS_RETHROW_USER_OR_SYSTEM(e, "");
					}

					container.updateInfo(newContainer->getContainerId(),
						newContainer->getVersionId());
					container.putRowSet(*currentStore->fixedPart_,
						*currentStore->varPart_,
						currentStore->rowStore_->getRowCount(), cmdOption);
					releaseCache = true;
				}
				totalRowCount += currentStore->rowStore_->getRowCount();
			}
			response_.updateCount_ = totalRowCount;
			executionManager_->incOperation(
					false, ec.getWorkerId(),
					static_cast<int32_t>(totalRowCount));
		}
		catch (std::exception& e) {
			needSchemaRefresh = true;
			const util::Exception checkException = GS_EXCEPTION_CONVERT(e, "");
			int32_t errorCode = checkException.getErrorCode();
			bool clearCache = false;
			checkReplyType(errorCode, clearCache);
			if (tableSchema && clearCache) {
				updateRemoteCache(ec, jobManager_->getEE(SQL_SERVICE),
					pt,
					context_.getDBId(), context_.getDBName(), tableName.name_,
					tableSchema->getTablePartitioningVersionId());
				remoteUpdateCache = true;
			}
			GS_RETHROW_USER_OR_SYSTEM(e, "");
		}
		if (latch && (needSchemaRefresh || remoteUpdateCache)) {
			TableSchemaInfo* schemaInfo = latch->get();
			if (schemaInfo) {
				TablePartitioningVersionId currentVersionId = MAX_TABLE_PARTITIONING_VERSIONID;
				if (needSchemaRefresh) {
				}
				else {
					if (remoteUpdateCache) {
						currentVersionId = schemaInfo->partitionInfo_.partitioningVersionId_;
					}
				}
				updateRemoteCache(ec, jobManager_->getEE(SQL_SERVICE),
					pt,
					context_.getDBId(), context_.getDBName(), tableName.name_,
					currentVersionId);
			}
		}
		SQLReplyContext replyCxt;
		replyCxt.set(&ec, NULL, UNDEF_JOB_VERSIONID, NULL);
		replyClient(replyCxt);
		if (!context_.isPreparedStatement()) {
			executionManager_->remove(ec, clientId_, false, 55, NULL);
		}
		if (latch) {
			ALLOC_DELETE(eventStackAlloc, latch);
			latch = NULL;
		}

		if (releaseCache) {
			getNoSQLStore()->removeCache(
				parsedInfo.tableList_[0]->qName_->table_->c_str());
		}

		getContext().setEndTime(util::DateTime::now(false).getUnixTime());
		return true;
	}
	catch (std::exception& e) {
		ALLOC_DELETE(eventStackAlloc, latch);
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

SQLExecutionManager::StatSetUpHandler SQLExecutionManager::statSetUpHandler_;

#define STAT_ADD(id) STAT_TABLE_ADD_PARAM(stat, parentId, id)

void SQLExecutionManager::StatSetUpHandler::operator()(StatTable& stat) {
	StatTable::ParamId parentId;
	parentId = STAT_TABLE_ROOT;
	stat.resolveGroup(parentId, STAT_TABLE_PERF, "performance");
	parentId = STAT_TABLE_PERF;
	STAT_ADD(STAT_TABLE_PERF_SQL_STORE_SWAP_FILE_SIZE_TOTAL);
	STAT_ADD(STAT_TABLE_PERF_SQL_TOTAL_READ_OPERATION);
	STAT_ADD(STAT_TABLE_PERF_SQL_TOTAL_WRITE_OPERATION);
	STAT_ADD(STAT_TABLE_PERF_SQL_SEND_PENDING_COUNT);
	STAT_ADD(STAT_TABLE_PERF_SQL_SQL_COUNT);
	STAT_ADD(STAT_TABLE_PERF_SQL_SQL_RETRY_COUNT);
	STAT_ADD(STAT_TABLE_PERF_SQL_JOB_COUNT);
	STAT_ADD(STAT_TABLE_PERF_SQL_QUEUE_MAX);
	STAT_ADD(STAT_TABLE_PERF_SQL_TABLE_CACHE_RATIO);

	STAT_ADD(STAT_TABLE_PERF_SQL_TABLE_CACHE_SEARCH_COUNT);
	STAT_ADD(STAT_TABLE_PERF_SQL_TABLE_CACHE_SKIP_COUNT);
	STAT_ADD(STAT_TABLE_PERF_SQL_TABLE_CACHE_LOAD_TIME);

	STAT_ADD(STAT_TABLE_PERF_SQL_STORE_SWAP_READ);
	STAT_ADD(STAT_TABLE_PERF_SQL_STORE_SWAP_READ_SIZE);
	STAT_ADD(STAT_TABLE_PERF_SQL_STORE_SWAP_READ_TIME);
	STAT_ADD(STAT_TABLE_PERF_SQL_STORE_SWAP_WRITE);
	STAT_ADD(STAT_TABLE_PERF_SQL_STORE_SWAP_WRITE_SIZE);
	STAT_ADD(STAT_TABLE_PERF_SQL_STORE_SWAP_WRITE_TIME);

	STAT_ADD(STAT_TABLE_PERF_SQL_TOTAL_INTERNAL_CONNECTION_COUNT);
	STAT_ADD(STAT_TABLE_PERF_SQL_TOTAL_EXTERNAL_CONNECTION_COUNT);
}

void SQLExecutionManager::updateStat(StatTable& stat) {
	LocalTempStore& store = jobManager_->getStore();

	stat.set(STAT_TABLE_PERF_SQL_STORE_SWAP_READ,
		store.getTotalSwapReadOperation());
	stat.set(STAT_TABLE_PERF_SQL_STORE_SWAP_READ_SIZE,
		store.getTotalSwapReadSize());
	stat.set(STAT_TABLE_PERF_SQL_STORE_SWAP_READ_TIME,
		store.getTotalSwapReadTime());
	stat.set(STAT_TABLE_PERF_SQL_STORE_SWAP_WRITE,
		store.getTotalSwapWriteOperation());
	stat.set(STAT_TABLE_PERF_SQL_STORE_SWAP_WRITE_SIZE,
		store.getTotalSwapWriteSize());
	stat.set(STAT_TABLE_PERF_SQL_STORE_SWAP_WRITE_TIME,
		store.getTotalSwapWriteTime());
	if (!stat.getDisplayOption(STAT_TABLE_DISPLAY_WEB_ONLY) ||
		!stat.getDisplayOption(STAT_TABLE_DISPLAY_OPTIONAL_MEM)) {
		return;
	}
	stat.set(STAT_TABLE_PERF_SQL_TOTAL_READ_OPERATION,
		getTotalReadOperation());
	stat.set(STAT_TABLE_PERF_SQL_TOTAL_WRITE_OPERATION,
		getTotalWriteOperation());
	stat.set(STAT_TABLE_PERF_SQL_SEND_PENDING_COUNT,
		jobManager_->getPendingCount());
	stat.set(STAT_TABLE_PERF_SQL_JOB_COUNT,
		jobManager_->getCurrentJobListSize());
	stat.set(STAT_TABLE_PERF_SQL_SQL_COUNT,
		getCurrentClientIdListSize());
	stat.set(STAT_TABLE_PERF_SQL_SQL_RETRY_COUNT, getInternalRetry());
	EventEngine::Stats eeStats;
	SQLService* sqlService = jobManager_->getSQLService();
	sqlService->getEE()->getStats(eeStats);
	const int64_t activeQueueSize = eeStats.get(
		EventEngine::Stats::EVENT_ACTIVE_QUEUE_SIZE_CURRENT);
	stat.set(STAT_TABLE_PERF_SQL_QUEUE_MAX,
		activeQueueSize);
	stat.set(STAT_TABLE_PERF_SQL_TABLE_CACHE_RATIO,
		getTableCacheRatio());
	stat.set(STAT_TABLE_PERF_SQL_TABLE_CACHE_SEARCH_COUNT,
		tableCacheSearchCount_);
	stat.set(STAT_TABLE_PERF_SQL_TABLE_CACHE_SKIP_COUNT,
		tableCacheSkipCount_);
	stat.set(STAT_TABLE_PERF_SQL_TABLE_CACHE_LOAD_TIME,
		tableCacheLoadTime_);
	stat.set(STAT_TABLE_PERF_SQL_TOTAL_INTERNAL_CONNECTION_COUNT,
		sqlService->getTotalInternalConnectionCount());
	stat.set(STAT_TABLE_PERF_SQL_TOTAL_EXTERNAL_CONNECTION_COUNT,
		sqlService->getTotalExternalConnectionCount());
}

void SQLExecutionManager::setRequestedConnectionEnv(
	const RequestInfo& request,
	StatementHandler::ConnectionOption& connOption) {
	typedef StatementMessage::PragmaList PragmaList;
	const PragmaList* pragmaList =
		request.option_.get<StatementMessage::Options::PRAGMA_LIST>();

	if (pragmaList == NULL) {
		return;
	}

	for (PragmaList::List::const_iterator it = pragmaList->list_.begin();
		it != pragmaList->list_.end(); ++it) {
		const SQLPragma::PragmaType paramId =
			SQLPragma::getPragmaType(it->first.c_str());
		if (paramId != SQLPragma::PRAGMA_NONE) {
			setConnectionEnv(connOption, paramId, it->second.c_str());
		}
	}
}

void SQLExecutionManager::setConnectionEnv(
	SQLExecution* execution, uint32_t paramId, const char8_t* value) {
	setConnectionEnv(execution->getContext().getConnectionOption(), paramId, value);
}

void SQLExecutionManager::setConnectionEnv(
	StatementHandler::ConnectionOption& connOption,
	uint32_t paramId, const char8_t* value) {
	if (paramId >= SQLPragma::PRAGMA_TYPE_MAX) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
			"ParamId=" << paramId << " is invalid.");
	}
	connOption.setConnectionEnv(paramId, value);
}

bool SQLExecutionManager::getConnectionEnv(
	SQLExecution* execution, uint32_t paramId, std::string& value,
	bool& hasData) {
	return getConnectionEnv(
		execution->getContext().getConnectionOption(), paramId, value, hasData);
}

bool SQLExecutionManager::getConnectionEnv(
	StatementHandler::ConnectionOption& connOption,
	uint32_t paramId, std::string& value, bool& hasData) {
	return connOption.getConnectionEnv(paramId, value, hasData);
}

void SQLExecutionManager::completeConnectionEnvUpdates(
	StatementHandler::ConnectionOption& connOption) {
	util::LockGuard<util::Mutex> guard(lock_);
	connOption.updatedEnvBits_ = 0;
}

uint32_t SQLExecutionManager::getConnectionEnvUpdates(
	const StatementHandler::ConnectionOption& connOption) {
	util::LockGuard<util::Mutex> guard(lock_);
	return connOption.updatedEnvBits_;
}

SQLConnectionControl::SQLConnectionControl(
	SQLExecutionManager* executionMgr, SQLExecution* execution) :
	executionMgr_(executionMgr), execution_(execution)
	, hasData_(true), config_(executionMgr->getSQLConfig()) {}

void SQLConnectionControl::setEnv(uint32_t paramId, std::string& value) {
	hasData_ = true;
	executionMgr_->setConnectionEnv(execution_, paramId, value.c_str());
}

bool SQLConnectionControl::getEnv(uint32_t paramId, std::string& key) {
	if (!hasData_) return false;
	return executionMgr_->getConnectionEnv(execution_, paramId, key, hasData_);
}

void SQLExecutionManager::getProfiler(
	util::StackAllocator& alloc, SQLProfiler& profs) {

	util::Vector<ClientId> clientIdList(alloc);
	getCurrentClientIdList(clientIdList);

	for (size_t pos = 0; pos < clientIdList.size(); pos++) {
		Latch latch(clientIdList[pos], this);
		SQLExecution* execution = latch.get();

		if (execution) {
			SQLProfilerInfo prof(alloc);
			prof.startTime_ = CommonUtility::getTimeStr(execution->getContext().getStartTime()).c_str();
			prof.sql_ = execution->getContext().getQuery();
			util::NormalOStringStream strstrm;

			char tmpBuffer[37];
			UUIDUtils::unparse(execution->getContext().getId().uuid_, tmpBuffer);
			util::String tmpUUIDStr(tmpBuffer, 36, alloc);
			strstrm << tmpUUIDStr.c_str();
			strstrm << ":" << execution->getContext().getId().sessionId_;
			prof.clientId_ = strstrm.str().c_str();

			JobId jobId;
			execution->getContext().getCurrentJobId(jobId);
			prof.jobId_ = jobId.dump(alloc).c_str();
			prof.endTime_ = CommonUtility::getTimeStr(execution->getContext().getEndTime()).c_str();
			prof.dbName_ = execution->getContext().getDBName();
			prof.applicationName_ = execution->getContext().getApplicationName();
			if (!execution->getContext().isPreparedStatement()) {
				prof.queryType_ = "NORMAL";
			}
			else {
				prof.queryType_ = "PREPARED";
			}
			prof.parseAndGetTableTime_ = execution->getProfilerInfo().parseAndGetTableTime_;
			prof.compileTime_ = execution->getProfilerInfo().compileTime_;
			prof.nosqlPendingTime_ = execution->getProfilerInfo().nosqlPendingTime_;

			JobManager::Latch latch(jobId, "SQLExecutionManager::getProfiler", jobManager_);
			Job* job = latch.get();
			if (job) {
				job->setJobProfiler(prof);
			}

			profs.sqlProfiler_.push_back(prof);
		}
	}
}

size_t SQLExecutionManager::getCurrentClientIdListSize() {

	util::LockGuard<util::Mutex> guard(lock_);
	return executionMap_.size();
}

void SQLExecutionManager::getCurrentClientIdList(
	util::Vector<ClientId>& clientIdList) {
	util::LockGuard<util::Mutex> guard(lock_);
	for (SQLExecutionMap::iterator it = executionMap_.begin();
		it != executionMap_.end(); it++) {
		SQLExecution* execution = (*it).second;
		if (execution != NULL) {
			clientIdList.push_back(execution->getContext().getId());
		}
	}
}

void SQLExecutionManager::cancelAll(
		const Event::Source& eventSource,
		util::StackAllocator& alloc, util::Vector<ClientId>& clientIdList,
		CancelOption& option) {
	UNUSED_VARIABLE(alloc);

	try {
		if (clientIdList.empty()) {
			getCurrentClientIdList(clientIdList);
		}
		for (size_t pos = 0; pos < clientIdList.size(); pos++) {
			Latch latch(clientIdList[pos], this);
			SQLExecution* execution = latch.get();
			if (execution) {
				JobId jobId;
				execution->getContext().getCurrentJobId(jobId);
				jobManager_->cancel(eventSource, jobId, option);
				execution->cancel(eventSource);
			}
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void SQLExecution::cancel(const Event::Source& eventSource) {
	try {
		JobId jobId;
		isCancelled_ = true;
		context_.getCurrentJobId(jobId);
		CancelOption option;
		jobManager_->cancel(eventSource, jobId, option);
	}
	catch (std::exception& e) {
		UTIL_TRACE_EXCEPTION(SQL_SERVICE, e, "");
		syncContext_->cancel();
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
	resetCurrentJobInfo();
	syncContext_->cancel();
}

SQLTableInfo* SQLExecution::decodeTableInfo(
	util::StackAllocator& alloc, util::ArrayByteInStream& in) {

	SQLTableInfo* info = ALLOC_NEW(alloc) SQLTableInfo(alloc);
	SQLTableInfo::PartitioningInfo* partitioning =
		ALLOC_NEW(alloc) SQLTableInfo::PartitioningInfo(alloc);
	info->partitioning_ = partitioning;

	int32_t distributedPolicy;
	in >> distributedPolicy;
	if (distributedPolicy != 1 && distributedPolicy != 2) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
	}

	int32_t partitioningType;
	in >> partitioningType;
	switch (partitioningType) {
	case SyntaxTree::TABLE_PARTITION_TYPE_HASH:
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE:
	case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH:
		partitioning->partitioningType_ = static_cast<uint8_t>(partitioningType);
		break;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
	}

	int32_t distributedMethod;
	in >> distributedMethod;

	if (SyntaxTree::isIncludeHashPartitioningType(partitioningType)) {
		int32_t partitioningCount;
		in >> partitioningCount;
		if (partitioningCount <= 0) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
		}
		partitioning->partitioningCount_ =
			static_cast<uint32_t>(partitioningCount);
	}

	ColumnType partitionColumnType;
	if (SyntaxTree::isRangePartitioningType(partitioningType)) {
		int8_t typeOrdinal;
		in >> typeOrdinal;
		if (!ValueProcessor::findColumnTypeByPrimitiveOrdinal(
				typeOrdinal, false, false, partitionColumnType)) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
		}

		int64_t intervalValue;
		switch (partitionColumnType) {
		case COLUMN_TYPE_BYTE: {
			int8_t value;
			in >> value;
			intervalValue = value;
			break;
		}
		case COLUMN_TYPE_SHORT: {
			int16_t value;
			in >> value;
			intervalValue = value;
			break;
		}
		case COLUMN_TYPE_INT: {
			int32_t value;
			in >> value;
			intervalValue = value;
			break;
		}
		case COLUMN_TYPE_LONG: {
			int64_t value;
			in >> value;
			intervalValue = value;
			break;
		}
		case COLUMN_TYPE_TIMESTAMP: {
			int64_t value;
			in >> value;
			intervalValue = value;
			break;
		}
		case COLUMN_TYPE_MICRO_TIMESTAMP: {
			MicroTimestamp value;
			in.readAll(&value, sizeof(value));
			intervalValue = ValueProcessor::getTimestamp(value);
			break;
		}
		case COLUMN_TYPE_NANO_TIMESTAMP: {
			NanoTimestamp value;
			in.readAll(&value, sizeof(value));
			intervalValue = ValueProcessor::getTimestamp(value);
			break;
		}
		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
		}
		if (intervalValue <= 0) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
		}
		partitioning->intervalValue_ = intervalValue;

		int8_t unitValue;
		in >> unitValue;
	}
	else {
		partitionColumnType = COLUMN_TYPE_ANY;
	}

	int64_t partitioningVersionId;
	in >> partitioningVersionId;

	{
		int32_t count;
		in >> count;
		for (int32_t i = 0; i < count; i++) {
			int64_t nodeAffinity;
			in >> nodeAffinity;
			if (nodeAffinity < 0) {
				GS_THROW_USER_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
			}
			partitioning->nodeAffinityList_.push_back(nodeAffinity);
		}
	}

	for (size_t i = 0; i < 2; i++) {
		int32_t count;
		in >> count;
		if (count != 1) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
		}

		int32_t columnId;
		in >> columnId;
		if (columnId < 0) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
		}

		(i == 0 ?
			partitioning->partitioningColumnId_ :
			partitioning->subPartitioningColumnId_) = columnId;

		if (partitioningType != SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH) {
			break;
		}
	}

	for (size_t i = 0; i < 2; i++) {
		int32_t count;
		in >> count;
		if (count < 0) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
		}

		for (int32_t j = 0; j < count; j++) {
			int64_t interval;
			int64_t intervalCount;
			in >> interval;
			in >> intervalCount;
			if (interval < 0 || intervalCount <= 0) {
				GS_THROW_USER_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
			}
			if (i == 0) {
				partitioning->availableList_.push_back(
					std::make_pair(interval, intervalCount));
			}
		}
	}

	int8_t currentStatus;
	in >> currentStatus;
	if (currentStatus == PARTITION_STATUS_CREATE_START ||
		currentStatus == PARTITION_STATUS_DROP_START) {
		int64_t nodeAffinity;
		in >> nodeAffinity;
		if (nodeAffinity < 0) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
		}
	}
	else if (currentStatus == INDEX_STATUS_CREATE_START ||
		currentStatus == INDEX_STATUS_DROP_START) {
		IndexInfo indexInfo(alloc);
		StatementHandler::decodeIndexInfo<util::ArrayByteInStream>(in, indexInfo);
	}
	else if (currentStatus != PARTITION_STATUS_NONE) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
	}

	if (distributedPolicy >= 2) {
		int64_t currentTime;
		int32_t expirationTime;
		int8_t expirationTimeUnit;

		in >> currentTime;
		in >> expirationTime;
		in >> expirationTimeUnit;

		if (currentTime < 0) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
		}

		if (expirationTime > 0) {
			if (partitionColumnType != COLUMN_TYPE_TIMESTAMP) {
				GS_THROW_USER_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
			}
		}
		else {
			if (expirationTime < -1) {
				GS_THROW_USER_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
			}
		}

		switch (expirationTimeUnit) {
		case TIME_UNIT_DAY:
		case TIME_UNIT_HOUR:
		case TIME_UNIT_MINUTE:
		case TIME_UNIT_SECOND:
		case TIME_UNIT_MILLISECOND:
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
		}
	}

	return info;
}

void SQLExecution::applyClusterPartitionCount(
		SQLTableInfo& tableInfo, uint32_t clusterPartitionCount) {
	if (tableInfo.partitioning_ != NULL) {
		const SQLTableInfo::PartitioningInfo *src = tableInfo.partitioning_;
		SQLTableInfo::PartitioningInfo *dest =
				ALLOC_NEW(src->alloc_) SQLTableInfo::PartitioningInfo(*src);

		dest->clusterPartitionCount_ = clusterPartitionCount;
		tableInfo.partitioning_ = dest;
	}
}

void SQLExecution::assignDistributedTarget(
	TransactionContext& txn,
	const util::Vector< std::pair<
	TupleList::TupleColumnType, util::String> >& columnInfoList,
	const Query& query, ResultSet& resultSet) {

	util::StackAllocator& alloc = txn.getDefaultAllocator();

	SQLTableInfo tableInfo = *resultSet.getLargeInfo();
	resultSet.setLargeInfo(NULL);

	tableInfo.columnInfoList_ = columnInfoList;

	bool uncovered;
	const bool reduced = SQLCompiler::reducePartitionedTargetByTQL(
		alloc, tableInfo, query, uncovered,
		resultSet.getDistributedTarget(), NULL);
	resultSet.setDistributedTargetStatus(uncovered, reduced);
}

FullContainerKey* SQLExecution::predicateToContainerKeyByTQL(
		TransactionContext &txn, DataStoreV4 &dataStore,
		const Query &query, DatabaseId dbId, ContainerId metaContainerId,
		PartitionId partitionCount, util::String &dbNameStr,
		bool &fullReduced, PartitionId &reducedPartitionId) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	return SQLCompiler::predicateToContainerKeyByTQL(
			alloc, txn, dataStore, query, dbId, metaContainerId,
			partitionCount, dbNameStr, fullReduced, reducedPartitionId);
}

bool SQLExecution::checkJobVersionId(
		uint8_t versionId, bool isFetch, JobId* jobId) {
	UNUSED_VARIABLE(jobId);

	uint8_t currentVersionId = context_.getCurrentJobVersionId();
	if (versionId != UNDEF_JOB_VERSIONID && versionId != currentVersionId) {
		if (isFetch) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_EXECUTION_INVALID_FETCH_STATEMENT,
				"Current fetch statement is changed, clientId=" << clientId_
				<< ", expected versionId=" << (int32_t)currentVersionId
				<< " current versionId=" << (int32_t)versionId);
		}
		else {
			GS_TRACE_WARNING(SQL_INTERNAL, GS_TRACE_SQL_INTERNAL_DEBUG,
				"Already internal execution, clientId=" << clientId_ << ", expected versionId="
				<< (int32_t)currentVersionId << ", current versionId=" << (int32_t)versionId);
			return false;
		}
	}
	return true;
}

void SQLExecution::compile(
		EventContext &ec, SQLConnectionControl &connectionControl,
		ValueTypeList &parameterTypeList, SQLTableInfoList *tableInfoList,
		TupleValue::VarContext &varCxt, int64_t startTime,
		RequestInfo& request, bool prepared, bool first,
		bool isPreparedRetry) {
	if (first) {
		clearIndexStatsRequester();
	}

	util::Vector<BindParam*> &bindParamInfos =
			bindParamSet_.getParamList(batchCurrentPos_);

	for (size_t i = 0; i < 2; i++) {
		TRACE_SQL_START(clientId_, jobManager_->getHostName(), "COMPILE");
		compileSub(
				ec.getAllocator(), connectionControl, parameterTypeList,
				tableInfoList, bindParamInfos, varCxt, startTime);

		if (!prepared) {
			setPreparedOption(request, isPreparedRetry);
		}
		TRACE_SQL_END(clientId_, jobManager_->getHostName(), "COMPILE");

		if (i > 0 || checkIndexStatsResolved()) {
			break;
		}
		resolveIndexStats(ec);
	}
}

void SQLExecution::compileSub(
		util::StackAllocator &alloc, SQLConnectionControl &connectionControl,
		ValueTypeList &parameterTypeList, SQLTableInfoList *tableInfoList,
		util::Vector<BindParam*> &bindParamInfos,
		TupleValue::VarContext &varCxt, int64_t startTime) {

	SQLPreparedPlan* plan;
	SQLParsedInfo& parsedInfo = getParsedInfo();

	SQLCompiler::CompileOption option;
	{
		const SQLConfigParam &param = executionManager_->getSQLConfig();
		option.setTimeZone(getContext().getTimezone());
		option.setPlanningVersion(param.getPlanningVersion());
		option.setCostBasedJoin(param.isCostBasedJoin());
		option.setCostBasedJoinDriving(param.isCostBasedJoinDriving());
		option.setIndexStatsRequester(&indexStatsRequester_);
	}

	SQLCompiler compiler(varCxt, &option);
	compiler.setMetaDataQueryFlag(executionStatus_.requestType_ == REQUEST_TYPE_PRAGMA);
	compiler.setExplainType(parsedInfo.explainType_);
	compiler.setInputSql(parsedInfo.inputSql_);
	compiler.setQueryStartTime(startTime);
	if (parsedInfo.placeHolderCount_ > 0) {
		compiler.setParameterTypeList(&parameterTypeList);
	}
	plan = ALLOC_NEW(alloc) SQLPreparedPlan(alloc);
	assert(parsedInfo.syntaxTreeList_.size() == 1);
	plan->setUpHintInfo(parsedInfo.syntaxTreeList_[0]->hintList_, *tableInfoList);
	compiler.setTableInfoList(tableInfoList);
	plan->parameterList_.clear();
	for (size_t bindPos = 0; bindPos < bindParamInfos.size(); bindPos++) {
		plan->parameterList_.push_back(bindParamInfos[bindPos]->value_);
	}
	compiler.compile(*parsedInfo.syntaxTreeList_[0], *plan, &connectionControl);
	executionStatus_.preparedPlan_ = plan;
	response_.parameterCount_ = parsedInfo.placeHolderCount_;
	if (bindParamInfos.size() > 0) {
		compiler.bindParameterTypes(*plan);
	}
	if (analyzedQueryInfo_.isExplainAnalyze()) {
		util::XArray<uint8_t> binary(alloc);
		util::XArrayOutStream<> arrayOut(binary);
		util::ByteStream< util::XArrayOutStream<> > out(arrayOut);
		TupleValue::coder(util::ObjectCoder(), NULL).encode(out, plan);
		cleanupSerializedData();
		copySerializedData(binary);
	}
}

void SQLExecutionManager::clearConnection(Event& ev, bool isNewSQL) {
	if (isNewSQL) {
		util::LockGuard<util::Mutex> guard(lock_);
		StatementHandler::ConnectionOption& connOption =
			ev.getSenderND().getUserData<StatementHandler::ConnectionOption>();
		connOption.clear();
	}
	else {
		StatementHandler::ConnectionOption& connOption =
			ev.getSenderND().getUserData<StatementHandler::ConnectionOption>();
		connOption.clear();
	}
}

void SQLExecution::cleanupPrevJob(EventContext& ec) {
	UNUSED_VARIABLE(ec);

	if (context_.isPreparedStatement()) {
		JobId currentJobId;
		context_.getCurrentJobId(currentJobId);
		jobManager_->remove(currentJobId);
	}
}

void SQLExecution::cancelPrevJob(EventContext& ec) {
	JobId currentJobId;
	context_.getCurrentJobId(currentJobId);
	jobManager_->cancel(ec, currentJobId, false);
}

bool SQLExecution::fastInsert(EventContext& ec,
	util::Vector<BindParam*>& bindParamInfos, bool useCache) {
	if (isAnalyzed() && !isFastInserted()) {
		return false;
	}
	if (executionStatus_.requestType_ == REQUEST_TYPE_PREPARE) {
		return false;
	}
	if (!isAnalyzed()) {
		if (!checkFastInsert(bindParamInfos)) {
			return false;
		}
	}
	return executeFastInsert(ec, bindParamInfos, useCache);
}

bool SQLExecution::parseViewSelect(
	SQLExpandViewContext& cxt,
	SQLParsedInfo& viewParsedInfo,
	uint32_t viewDepth, int64_t viewNsId, int64_t maxViewNsId,
	util::String& viewSelectString)
{
	GenSyntaxTree genSyntaxTree(executionStackAlloc_);
	SQLParsedInfo* orgParsedInfo = cxt.parsedInfo_;
	cxt.parsedInfo_ = &viewParsedInfo;
	genSyntaxTree.parseAll(&cxt, this, viewSelectString, viewDepth, viewNsId, maxViewNsId, true);
	cxt.parsedInfo_ = orgParsedInfo;
	if (viewParsedInfo.getCommandNum() > 1) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_UNSUPPORTED,
			"Multiple SQL statement is not supported");
	}
	if (viewParsedInfo.pragmaType_ != SQLPragma::PRAGMA_NONE) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_UNSUPPORTED,
			"Pragma is not supported");
	}
	if (viewParsedInfo.syntaxTreeList_[0]->calcCommandType()
		!= SyntaxTree::COMMAND_SELECT) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_UNSUPPORTED,
			"View definition must be select statement");
	}
	if (viewParsedInfo.syntaxTreeList_[0]->isDDL()) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_UNSUPPORTED,
			"DDL is not supported");
	}
	if (viewParsedInfo.placeHolderCount_ > 0) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
			"PlaceHolder is not supported");
	}
	if (viewParsedInfo.tableList_.size() > 0) {
		orgParsedInfo->tableList_.insert(
			orgParsedInfo->tableList_.end(), viewParsedInfo.tableList_.begin(),
			viewParsedInfo.tableList_.end());
	}
	return true;
}

bool SQLExecution::parse(
	SQLExpandViewContext& cxt,
	RequestInfo& request,
	uint32_t viewDepth, int64_t viewNsId, int64_t maxViewNsId)
{
	GenSyntaxTree genSyntaxTree(executionStackAlloc_);
	SQLParsedInfo& parsedInfo = getParsedInfo();
	try {
		genSyntaxTree.parseAll(&cxt, this, request.sqlString_,
			viewDepth, viewNsId, maxViewNsId, false);
		return doAfterParse(request);
	}
	catch (std::exception& e) {
		if (!analyzedQueryInfo_.isPragmaStatement() && parsedInfo.getCommandNum() == 0) {
			GS_RETHROW_USER_OR_SYSTEM(e, "");
		}
		doAfterParse(request);
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

bool SQLExecution::doAfterParse(RequestInfo& request) {
	SQLParsedInfo& parsedInfo = getParsedInfo();
	if (parsedInfo.getCommandNum() > 1) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_UNSUPPORTED,
			"Multiple SQL statement is not supported");
	}
	if (analyzedQueryInfo_.isPragmaStatement()) {
		if (request.requestType_ == REQUEST_TYPE_UPDATE) {
			setNotSelect(true);
		}
		return false;
	}
	if (parsedInfo.getCommandNum() == 0) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_INTERNAL,
			"Not supported query syntax");
	}
	executionStatus_.setFetchRemain(config_.maxRows_);
	if (parsedInfo.syntaxTreeList_[0]->calcCommandType()
		!= SyntaxTree::COMMAND_SELECT) {
		setNotSelect(true);
	}
	if (parsedInfo.syntaxTreeList_[0]->isDDL()) {
		setDDL();
	}
	if (context_.isPreparedStatement() && request.requestType_ == REQUEST_TYPE_EXECUTE) {
		if (response_.parameterCount_
			!= static_cast<int32_t>(bindParamSet_.getParamList(batchCurrentPos_).size())) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_EXECUTION_BIND_FAILED,
				"Unmatch bind parameter count, expect="
				<< response_.parameterCount_
				<< ", actual=" << bindParamSet_.getParamList(batchCurrentPos_).size());
		}
	}
	if (!context_.isPreparedStatement() && parsedInfo.placeHolderCount_ > 0) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
			"PlaceHolder must be specified in prepared statement");
	}
	return true;
}

bool SQLExecution::checkFastInsert(util::Vector<BindParam*>& setInfo) {
	SQLParsedInfo& parsedInfo = getParsedInfo();
	if (!isFastInserted()) {
		if (parsedInfo.syntaxTreeList_.size() == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_INVALID_SYNTAX_TREE,
				"Syntax tree is not found");
		}
		if (parsedInfo.syntaxTreeList_[0]->cmdType_ == SyntaxTree::CMD_INSERT
			&& parsedInfo.syntaxTreeList_[0]->insertSet_
			&& !parsedInfo.syntaxTreeList_[0]->insertList_) {
		}
		else {
			return false;
		}
		if (context_.isPreparedStatement() && response_.parameterCount_ != static_cast<int32_t>(setInfo.size())) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_EXECUTION_BIND_FAILED,
				"Unmatch bind parameter count, expect="
				<< response_.parameterCount_ << ", actual=" << setInfo.size());
		}
		SyntaxTree::Set* targetSet
			= parsedInfo.syntaxTreeList_[0]->insertSet_;
		util::Vector<SyntaxTree::ExprList*>& mergeSelectList = getMergeSelectList();
		int32_t placeHolderCount = 0;
		if (targetSet->right_ && targetSet->right_->selectList_) {
			if (targetSet->right_->limitList_) {
				return false;
			}
			if (!checkSelectList(targetSet->right_->selectList_, mergeSelectList,
				placeHolderCount)) {
				return false;
			}
		}
		else {
			if (targetSet->unionAllList_ && targetSet->unionAllList_->size() > 0) {
				for (size_t pos = 0; pos < targetSet->unionAllList_->size(); pos++) {
					if ((*targetSet->unionAllList_)[pos]->limitList_) {
						return false;
					}
					if (!checkSelectList((*targetSet->unionAllList_)[pos]->selectList_,
						mergeSelectList, placeHolderCount)) {
						return false;
					}
				}
			}
			else {
				return false;
			}
		}
		assert(mergeSelectList.size() > 0);
		if (parsedInfo.tableList_.size() == 1 &&
			parsedInfo.tableList_[0]->qName_ &&
			parsedInfo.tableList_[0]->qName_->table_) {
		}
		else {
			return false;
		}
	}
	if (parsedInfo.tableList_[0]->qName_ &&
		SQLCompiler::Meta::isSystemTable(*parsedInfo.tableList_[0]->qName_)) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_METATABLE_NOT_UPDATABLE,
			"Meta table '" << parsedInfo.tableList_[0]->qName_->table_->c_str()
			<< "' is not updatable");
	}
	setFastInserted();
	return true;
}

bool SQLExecution::metaDataQueryCheck(SQLConnectionControl* control) {
	bool forceMetaDataQuery = false;
	std::string value;
	if (control->getEnv(
		SQLPragma::PRAGMA_INTERNAL_COMPILER_EXECUTE_AS_META_DATA_QUERY, value)) {
		if (value == SQLPragma::VALUE_TRUE) {
			forceMetaDataQuery = true;
		}
	}
	return (executionStatus_.requestType_ == REQUEST_TYPE_PRAGMA
		|| forceMetaDataQuery);
}

bool SQLExecution::checkRefreshTable(
		EventContext& ec, NameWithCaseSensitivity& tableName,
		util::StackAllocator& alloc, TableSchemaInfo* tableSchema,
		EventMonotonicTime currentEmTime) {
	UNUSED_VARIABLE(alloc);

	if (tableSchema->partitionInfo_.partitionType_
		== SyntaxTree::TABLE_PARTITION_TYPE_UNDEF) {
		if (tableSchema->containerAttr_ != CONTAINER_ATTR_VIEW) {
			return false;
		}
	}

	SQLService* sqlSvc = executionManager_->getSQLService();
	int32_t expiredTime = sqlSvc->getTableSchemaExpiredTime();

	if ((currentEmTime - tableSchema->lastExecutedTime_) >= expiredTime) {
		NoSQLStore* command = getNoSQLStore();
		NoSQLContainer targetContainer(ec, tableName, context_.getSyncContext(), this);
		NoSQLStoreOption option(this);
		if (tableName.isCaseSensitive_) {
			option.caseSensitivity_.setContainerNameCaseSensitive();
		}
		command->getContainer(targetContainer, false, option);
		if (targetContainer.isExists() && (targetContainer.isLargeContainer()
			|| targetContainer.isViewContainer())) {
			tableSchema->disableCache_ = true;
		}
		tableSchema->lastExecutedTime_ = currentEmTime;
	}
	return false;
}

void SQLExecution::setPreparedOption(RequestInfo& request, bool isRetry) {
	if (context_.isPreparedStatement() && !isRetry) {
		typedef StatementMessage::Options Options;
		if (request.option_.get<Options::MAX_ROWS>() != INT64_MAX) {
			config_.maxRows_ = request.option_.get<Options::MAX_ROWS>();
			executionStatus_.setFetchRemain(config_.maxRows_);
		}
		int32_t queryTimeout = request.option_.get<Options::STATEMENT_TIMEOUT_INTERVAL>();
		if (queryTimeout != INT32_MAX && queryTimeout >= 1 * 1000) {
			config_.queryTimeout_ = queryTimeout;
		}
	}
}

void SQLExecution::checkExecutableJob(ExecId targetStatementId) {
	if (executionStatus_.pendingCancelId_ == targetStatementId) {
		JobId jobId(clientId_, targetStatementId);
		GS_THROW_USER_ERROR(GS_ERROR_JOB_CANCELLED,
			"Cancel job, jobId = " << jobId);
	}
}

bool SQLExecution::isCurrentTimeRequired() {
	return (executionStatus_.preparedPlan_->currentTimeRequired_);
}

void SQLExecution::cleanupTableCache() {
	DBConnection* conn = executionManager_->getDBConnection();
	if (conn != NULL) {
		SQLParsedInfo& parsedInfo = getParsedInfo();
		std::pair<util::Set<util::String>::iterator, bool> outResult;
		for (util::Vector<SyntaxTree::Expr*>::iterator
			it = parsedInfo.tableList_.begin();
			it != parsedInfo.tableList_.end(); it++) {
			const SyntaxTree::QualifiedName* qName = (*it)->qName_;
			if (qName == NULL || qName->table_ == NULL) {
				continue;
			}
			if (SQLCompiler::Meta::isSystemTable(*qName)) {
				continue;
			}
			NoSQLStore* command = getNoSQLStore();
			command->removeCache(qName->table_->c_str());
		}
	}
}

void SQLExecution::checkWritableTable(SQLConnectionControl& control,
	TableSchemaInfo* tableSchema, SQLTableInfo& tableInfo) {
	if (!NoSQLCommonUtils::isAccessibleContainer(
		tableSchema->containerAttr_, tableInfo.writable_)) {
		std::string pragmaValue;
		if (control.getEnv(SQLPragma::PRAGMA_EXPERIMENTAL_SHOW_SYSTEM, pragmaValue)) {
			if (!pragmaValue.empty()) {
				if (!SQLProcessor::ValueUtils::strICmp(pragmaValue.c_str(), "1")) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_INVALID_CONTAINER_ATTRIBUTE,
						"Target table '" << tableSchema->tableName_ << "' is not accessible, attribute=" <<
						static_cast<int32_t>(tableSchema->containerAttr_));
				}
			}
		}
	}
}

void SQLExecutionManager::checkTimeout(EventContext& ec) {
	SQLService* sqlSvc = getJobManager()->getSQLService();
	util::StackAllocator& alloc = ec.getAllocator();
	int64_t currentTime = sqlSvc->getEE()->getMonotonicTime();
	util::Vector<ClientId> clientIdList(alloc);
	getCurrentClientIdList(clientIdList);
	for (util::Vector<ClientId>::iterator it = clientIdList.begin();
		it != clientIdList.end(); it++) {
		SQLExecutionManager::Latch latch((*it), this);
		SQLExecution* execution = latch.get();
		if (execution) {
			JobId jobId;
			execution->getContext().getCurrentJobId(jobId);
			jobManager_->checkAlive(ec, alloc, jobId, currentTime);
			if (execution->isCancelled()) {
				execution->cancel(ec, jobId.execId_);
			}
		}
	}
}

void SQLExecution::PreparedInfo::cleanup() {
	if (serializedPlan_) {
		globalVarAlloc_.deallocate(serializedPlan_);
		serializedPlan_ = NULL;
		serializedPlanSize_ = 0;
	}
	if (serializedBindInfo_) {
		globalVarAlloc_.deallocate(serializedBindInfo_);
		serializedBindInfo_ = NULL;
		serializedBindInfoSize_ = 0;
	}
}

void SQLExecution::PreparedInfo::copySerialized(util::XArray<uint8_t>& binary) {
	if (binary.size() > 0) {
		serializedPlan_ = static_cast<char8_t*>(globalVarAlloc_.allocate(binary.size()));
		serializedPlanSize_ = static_cast<int32_t>(binary.size());
		memcpy(serializedPlan_, binary.data(), binary.size());
	}
}

void SQLExecution::PreparedInfo::copyBind(util::XArray<uint8_t>& binary) {
	if (binary.size() > 0) {
		serializedBindInfo_ = static_cast<char8_t*>(globalVarAlloc_.allocate(binary.size()));
		serializedBindInfoSize_ = static_cast<int32_t>(binary.size());
		memcpy(serializedBindInfo_, binary.data(), binary.size());
	}
}

void SQLExecution::PreparedInfo::restoreBind(
	RequestInfo& request) {
	if (serializedBindInfo_ && serializedBindInfoSize_ > 0) {
		util::ArrayByteInStream in = util::ArrayByteInStream(
			util::ArrayInStream(serializedBindInfo_, serializedBindInfoSize_));
		SQLRequestHandler::decodeBindInfo(in, request);
	}
}

void SQLExecution::PreparedInfo::restorePlan(SQLPreparedPlan*& plan,
	util::StackAllocator& eventStackAlloc, TupleValue::VarContext& varCxt) {
	if (serializedPlan_ && serializedPlanSize_ > 0) {
		util::ArrayByteInStream in = util::ArrayByteInStream(
			util::ArrayInStream(serializedPlan_, serializedPlanSize_));
		TupleValue::coder(util::ObjectCoder::withAllocator(
			eventStackAlloc), &varCxt).decode(in, plan);
	}
}

void SQLExecution::cleanupSerializedData() {
	preparedInfo_.cleanup();
}

void SQLExecution::copySerializedData(util::XArray<uint8_t>& binary) {
	preparedInfo_.copySerialized(binary);
}

void SQLExecution::copySerializedBindInfo(util::XArray<uint8_t>& binary) {
	preparedInfo_.copyBind(binary);
}

void SQLExecution::restoreBindInfo(RequestInfo& request) {
	preparedInfo_.restoreBind(request);
}

void SQLExecution::restorePlanInfo(SQLPreparedPlan*& plan,
	util::StackAllocator& eventStackAlloc, TupleValue::VarContext& varCxt) {
	preparedInfo_.restorePlan(plan, eventStackAlloc, varCxt);
}

bool SQLExecution::fetchBatch(
		EventContext &ec, std::vector<int64_t> *countList) {
	UNUSED_VARIABLE(countList);

	bool responsed = executionStatus_.responsed_;
	if (executionStatus_.currentType_
			!= SQL_EXEC_CLOSE && responsed) {
		
		GS_TRACE_INFO(
				DISTRIBUTED_FRAMEWORK,
				GS_TRACE_SQL_INTERNAL_DEBUG,
				"Call reply success, but not executed because of already responded,"
				"clientId=" << clientId_);
		return false;
	}

	Event replyEv(
			ec, SQL_EXECUTE_QUERY, getConnectedPId());
	replyEv.setPartitionIdSpecified(false);
	EventByteOutStream out = replyEv.getOutStream();

	out << response_.stmtId_;
	out << StatementHandler::TXN_STATEMENT_SUCCESS;
	StatementHandler::encodeBooleanData(
			out, response_.transactionStarted_);

	StatementHandler::encodeBooleanData(
			out, response_.isAutoCommit_);
	
	StatementHandler::encodeIntData<EventByteOutStream, uint32_t>(
			out, static_cast<uint32_t>(
					batchCountList_.size()));

	for (size_t i = 0; i < batchCountList_.size(); i++) {

		executionManager_->incOperation(
				false, ec.getWorkerId(),
				static_cast<int32_t>(batchCountList_[i]));

		StatementHandler::encodeIntData<EventByteOutStream, uint32_t>(
				out, static_cast<uint32_t>(
						batchCountList_[i]));

		StatementHandler::encodeIntData<EventByteOutStream, uint32_t>(
				out, response_.parameterCount_);

		bool existsResult = false;
		StatementHandler::encodeBooleanData(
				out, existsResult);
	}

	SQLRequestHandler::encodeEnvList(
			out,
			ec.getAllocator(),
			*executionManager_,
			context_.getClientNd().getUserData<StatementHandler::ConnectionOption>());

	sendClient(ec, replyEv);

	resultSet_->clear();
	executionStatus_.reset();
	response_.reset();
	resetJobVersion();

	isBatch_ = false;
	batchCountList_.clear();
	batchCount_ = 0;
	batchCurrentPos_ = 0;
	bindParamSet_.clear();

	return true;
}

bool SQLExecution::fetchNotSelect(EventContext& ec,
	TupleList::Reader* reader, TupleList::Column* columnList) {
	if (reader->exists()) {
		TupleValue::VarContext::Scope varScope(reader->getVarContext());
		assert(columnList != NULL);
		TupleList::Column& column = columnList[0];
		response_.updateCount_ = reader->get().getAs<int64_t>(column);
		if (isDDL()) {
			executionManager_->incOperation(false, ec.getWorkerId(), 1);
		}
		else {
			executionManager_->incOperation(false, ec.getWorkerId(),
				static_cast<int32_t>(response_.updateCount_));
		}
		SQLReplyContext replyCxt;
		replyCxt.set(&ec, NULL, UNDEF_JOB_VERSIONID, NULL);
		if (replyClient(replyCxt)) {
			resultSet_->clear();
			return true;
		}
		else {
			return false;
		}
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_EXECUTION_INVALID_STATUS,
			"Not select sql must be only one result");
	}
}

bool SQLExecution::fetchSelect(EventContext& ec, TupleList::Reader* reader,
	TupleList::Column* columnList, size_t columnSize, bool isCompleted) {

	util::StackAllocator& alloc = ec.getAllocator();
	bool retFlag = false;
	int64_t actualFetchCount = 0;
	int64_t currentFetchNum = executionStatus_.getFetchRemain();
	if (currentFetchNum > SQLExecution::FETCH_LIMIT_SIZE) {
		currentFetchNum = SQLExecution::FETCH_LIMIT_SIZE;
	}
	int64_t sizeLimit = executionManager_->getFetchSizeLimit();
	resultSet_->clear();
	resultSet_->init(alloc);

	while (reader->exists() && actualFetchCount < currentFetchNum
		&& resultSet_->getSize() < sizeLimit) {
		resultSet_->next(alloc);
		TupleValue::VarContext::Scope varScope(reader->getVarContext());
		for (int32_t columnPos = 0;
			columnPos < static_cast<int32_t>(columnSize); columnPos++) {
			TupleList::Column& column = columnList[columnPos];
			const TupleValue& value = reader->get().get(column);
			if (ColumnTypeUtils::isNull(value.getType())) {
				setNullValue(value, alloc, column);
				continue;
			}
			switch (column.type_ & ~TupleList::TYPE_MASK_NULLABLE) {
			case TupleList::TYPE_ANY: {
				setAnyTypeData(alloc, value);
			}
									break;
			case TupleList::TYPE_BYTE:
			case TupleList::TYPE_SHORT:
			case TupleList::TYPE_INTEGER:
			case TupleList::TYPE_LONG:
			case TupleList::TYPE_FLOAT:
			case TupleList::TYPE_NUMERIC:
			case TupleList::TYPE_DOUBLE:
			case TupleList::TYPE_TIMESTAMP:
			case TupleList::TYPE_MICRO_TIMESTAMP:
			case TupleList::TYPE_BOOL: {
				resultSet_->setFixedFieldValue(value.fixedData(),
					ColumnTypeUtils::getFixedSize(value.getType()));
			}
									 break;
			case TupleList::TYPE_NANO_TIMESTAMP:
				resultSet_->setFixedFieldValue(
						value.get<TupleNanoTimestamp>().data(),
						ColumnTypeUtils::getFixedSize(value.getType()));
				break;
			case TupleList::TYPE_STRING: {
				resultSet_->setVarFieldValue(
					value.varData(), value.varSize());
			}
									   break;
			case TupleList::TYPE_BLOB: {
				util::XArray<uint8_t> blobs(alloc);
				generateBlobValue(value, blobs);
				resultSet_->setVarFieldValue(
					blobs.data(), blobs.size());
			}
									 break;
			default:
				GS_THROW_USER_ERROR(GS_ERROR_SQL_OPTYPE_UNSUPPORTED, "");
			}
		}
		actualFetchCount++;
		reader->next();
	}
	bool isFetchSizeLimit = executionStatus_.declFetchRemain(actualFetchCount);
	if ((!reader->exists() && isCompleted) || isFetchSizeLimit) {
		response_.isContinue_ = false;
		retFlag = true;
	}
	else {
		response_.isContinue_ = true;
	}
	if (actualFetchCount > 0 || !response_.isContinue_) {
		SQLReplyContext replyCxt;
		replyCxt.set(&ec, NULL, UNDEF_JOB_VERSIONID, NULL);
		if (replyClient(replyCxt)) {
			resultSet_->clear();
			if (isFetchSizeLimit) {
				executionStatus_.setMaxRows(config_.maxRows_);
			}
		}
	}
	return retFlag;
}

void SQLExecution::generateBlobValue(
	const TupleValue& value, util::XArray<uint8_t>& blobs) {
	TupleValue::LobReader lobReader(value);
	const void* data;
	size_t size;
	while (lobReader.next(data, size)) {
		blobs.push_back(
			static_cast<const uint8_t*>(data), size);
	}
}

void SQLExecution::setNullValue(const TupleValue& value,
	util::StackAllocator& alloc, TupleList::Column& column) {
	if (column.type_ == TupleList::TYPE_ANY) {
		setAnyTypeData(alloc, value);
	}
	else {
		if (ColumnTypeUtils::isSomeFixed(column.type_)) {
			const size_t paddingSize =
					ColumnTypeUtils::getFixedSize(column.type_);
			resultSet_->setFixedFieldPadding(paddingSize);
		}
		else {
			resultSet_->setVarFieldValue(NULL, 0);
		}
		resultSet_->setNull(column.pos_, true);
	}
}

void SQLExecution::checkConcurrency(EventContext& ec, std::exception* e) {
	if (ec.getWorkerId() != getConnectedPgId()) {
		if (e != NULL) {
			try {
				throw* e;
			}
			catch (std::exception& e1) {
				GS_RETHROW_USER_OR_SYSTEM(e1, "");
			}
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_EXECUTION_INTERNAL,
				"Thread concurrency error, current="
				<< ec.getWorkerId() << ", expected=" << getConnectedPgId());
		}
	}
}

bool SQLExecution::handleError(
		EventContext& ec,
		std::exception& e, int32_t currentVersionId, int64_t& delayTime,
		RequestInfo &request) {
	UNUSED_VARIABLE(request);

	const util::Exception checkException = GS_EXCEPTION_CONVERT(e, "");
	int32_t errorCode = checkException.getErrorCode();
	bool cacheClear = false;
	delayTime = 0;
	SQLReplyType replyType = checkReplyType(errorCode, cacheClear);
	if (checkRetryError(replyType, currentVersionId)) {
		replyType = SQL_REPLY_ERROR;
		executionManager_->incInternalRetry();
	}
	try {
		SQLParsedInfo& parsedInfo = getParsedInfo();
		AUDIT_TRACE_ERROR_HANDLEERROR();
		GS_RETHROW_USER_OR_SYSTEM(e, ErrorFormatter(*this, e));
	}
	catch (std::exception& e2) {
		bool removed = false;
		switch (replyType) {
		case SQL_REPLY_INTERNAL: {
			UTIL_TRACE_EXCEPTION_WARNING(SQL_INTERNAL, e2, "");
			if (isDenyException(errorCode)) {
				delayTime = DEFAULT_NOSQL_FAILOVER_WAIT_TIME;
				jobManager_->getClusterService()->requestRefreshPartition(ec);
			}
			try {
				if (cacheClear) {
					cleanupTableCache();
				}
				resultSet_->resetSchema();
				cancelPrevJob(ec);
				updateJobVersion();
				return false;
			}
			catch (std::exception& e2) {
				replyError(ec, StatementHandler::TXN_STATEMENT_ERROR, e2);
				removed = true;
			}
		}
							   break;
		case SQL_REPLY_DENY: {
			if (replyError(ec, StatementHandler::TXN_STATEMENT_DENY, e2)) {
				removed = true;
			}
			jobManager_->getClusterService()->requestRefreshPartition(ec);
		}
						   break;
		case SQL_REPLY_ERROR: {
			if (replyError(ec, StatementHandler::TXN_STATEMENT_ERROR, e2)) {
				removed = true;
			}
		}
		}
		if (removed && !context_.isPreparedStatement()) {
			executionManager_->remove(ec, clientId_, false, 56, NULL);
		}
	}
	return true;
}

void SQLExecution::setupViewContext(SQLExpandViewContext& cxt,
	EventContext& ec, SQLConnectionControl& connectionControl,
	util::Set<util::String>& tableSet, util::Set<util::String>& viewSet,
	util::Vector<NodeId>& liveNodeIdList, bool useCache, bool withVersion) {
	cxt.eventStackAlloc_ = &ec.getAllocator();
	cxt.control_ = &connectionControl;
	cxt.parsedInfo_ = &getParsedInfo();
	cxt.defaultDbName_ = context_.getDBName();
	cxt.ec_ = &ec;
	cxt.tableInfoList_ = NULL;
	cxt.tableSet_ = &tableSet;
	cxt.liveNodeIdList_ = &liveNodeIdList;
	cxt.viewSet_ = &viewSet;
	cxt.useCache_ = useCache;
	cxt.withVersion_ = withVersion;
}

void SQLExecution::generateJobId(JobId& jobId, ExecId execId) {
	if (isBatch_) {
		context_.getCurrentJobId(jobId);
		jobId.clientId_ = clientId_;
		jobId.execId_ = INT64_MAX - batchCurrentPos_;
	}
	else {
		checkExecutableJob(execId);
		context_.getCurrentJobId(jobId);
		jobId.clientId_ = clientId_;
		jobId.execId_ = context_.getExecId();
	}
	context_.setCurrentJobId(jobId);
}

void SQLExecution::setupSyncContext() {
	syncContext_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_) NoSQLSyncContext(
		clientId_,
		executionManager_->getPartitionTable(),
		jobManager_->getTransactionService(),
		jobManager_->getSQLService(),
		jobManager_->getGlobalAllocator());
	syncContext_->userType_ = context_.getConnectionOption().userType_;
	syncContext_->replyPId_ = getConnectedPId();
	syncContext_->txnSvc_ = jobManager_->getTransactionService();
	syncContext_->dbName_ = context_.getDBName();
	syncContext_->dbId_ = context_.getDBId();
	syncContext_->timeoutInterval_ = config_.queryTimeout_;
	syncContext_->txnTimeoutInterval_ = config_.txnTimeout_;
	context_.syncContext_ = syncContext_;
}

DBConnection* SQLExecutionManager::getDBConnection() {
	return dbConnection_;
}

SQLExecution::ClientInfo::ClientInfo(util::StackAllocator& alloc,
	SQLVariableSizeGlobalAllocator& globalVarAlloc,
	const NodeDescriptor& clientNd, ClientId& clientId,
	StatementHandler::ConnectionOption& connOption)
	: userName_(globalVarAlloc), dbName_(globalVarAlloc), dbId_(0),
	normalizedDbName_(globalVarAlloc),
	applicationName_(globalVarAlloc), isAdministrator_(false),
	clientNd_(clientNd), clientId_(clientId), connOption_(connOption),
	acceptableFeatureVersion_(0) {
	init(alloc);
}

void SQLExecution::ClientInfo::init(util::StackAllocator& alloc) {
	util::StackAllocator::Scope scope(alloc);
	dbId_ = connOption_.dbId_;
	isAdministrator_ = (connOption_.userType_ != StatementMessage::USER_NORMAL);
	connOption_.getLoginInfo(userName_, dbName_, applicationName_);
	normalizedDbName_ = normalizeName(alloc, dbName_.c_str()).c_str();
	storeMemoryAgingSwapRate_ = connOption_.storeMemoryAgingSwapRate_;
	timezone_ = connOption_.timeZone_;
	acceptableFeatureVersion_ = connOption_.acceptableFeatureVersion_;
	connOption_.setHandlingClientId(clientId_);
}

util::StackAllocator* SQLExecutionManager::getStackAllocator(
		util::AllocatorLimitter *limitter) {
	util::StackAllocator *alloc;
	{
		util::LockGuard<util::Mutex> guard(stackAllocatorLock_);
		alloc = ALLOC_VAR_SIZE_NEW(globalVarAlloc_) util::StackAllocator(
			util::AllocatorInfo(ALLOCATOR_GROUP_SQL_WORK,
				"SQLExecutionAllocator"), &fixedAllocator_);
	}
	if (limitter != NULL) {
		alloc->setLimit(util::AllocatorStats::STAT_GROUP_TOTAL_LIMIT, limitter);
	}
	return alloc;
}

void SQLExecutionManager::releaseStackAllocator(util::StackAllocator* alloc) {
	if (alloc == NULL) return;
	util::LockGuard<util::Mutex> guard(stackAllocatorLock_);
	util::StackAllocator::Tool::forceReset(*alloc);
	alloc->setFreeSizeLimit(1);
	alloc->trim();
	ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, alloc);
}

SessionId SQLExecution::SQLExecutionContext::incCurrentSessionId() {
	return getConnectionOption().currentSessionId_++;
}

SessionId SQLExecution::SQLExecutionContext::getCurrentSessionId() {
	return getConnectionOption().currentSessionId_;
}

SQLService* SQLExecutionManager::getSQLService() {
	return jobManager_->getSQLService();
}

util::FixedSizeAllocator<util::Mutex>& SQLExecutionManager::getFixedAllocator() {
	return fixedAllocator_;
}


SQLVariableSizeGlobalAllocator& SQLExecutionManager::getVarAllocator() {
	return globalVarAlloc_;
}

JobManager* SQLExecutionManager::getJobManager() {
	return jobManager_;
}

const SQLProcessorConfig* SQLExecutionManager::getProcessorConfig() const {
	return processorConfig_;
}

SQLConfigParam& SQLExecutionManager::getSQLConfig() {
	return config_;
}

void SQLExecution::updateJobVersion(JobId& jobId) {
	util::LockGuard<util::Mutex> guard(lock_);
	currentJobId_.versionId_++;
	jobId = currentJobId_;
}

void SQLExecution::updateJobVersion() {
	util::LockGuard<util::Mutex> guard(lock_);
	currentJobId_.versionId_++;
}

void SQLExecution::resetJobVersion() {
	util::LockGuard<util::Mutex> guard(lock_);
	currentJobId_.versionId_ = 0;
}

void SQLExecution::updateConnection() {
	context_.getConnectionOption().removeSessionId(clientId_);
}

bool SQLExecution::checkSelectList(SyntaxTree::ExprList* selectList,
	util::Vector<SyntaxTree::ExprList*>& mergeSelectList,
	int32_t& placeHolderCount) {
	SyntaxTree::ExprList::iterator it;
	if (selectList == NULL) {
		return false;
	}
	for (it = selectList->begin(); it != selectList->end(); it++) {
		if ((*it)->op_ != SQLType::EXPR_CONSTANT) {
			if ((*it)->op_ != SQLType::EXPR_PLACEHOLDER) {
				return false;
			}
			else {
				if (!context_.isPreparedStatement()) {
					return false;
				}
				(*it)->placeHolderPos_ = placeHolderCount;
				placeHolderCount++;
			}
		}
	}
	mergeSelectList.push_back(selectList);
	return true;
};

/*!
	@brief レスポンス情報をリセットする
*/
void ResponseInfo::reset() {

	stmtId_ = UNDEF_STATEMENTID;
	transactionStarted_ = false;
	isAutoCommit_ = true;
	isRetrying_ = false;
	resultCount_ = 1;
	updateCount_ = 0;
	existsResult_ = false;
	isContinue_ = false;
}

void ResponseInfo::set(
	RequestInfo& request, SQLResultSet* resultSet) {
	resultSet_ = resultSet;
	reset();
	stmtId_ = request.stmtId_;
	transactionStarted_ = request.transactionStarted_;
	isAutoCommit_ = request.isAutoCommit_;
	isRetrying_ = request.retrying_;
}

void SQLExecution::updateRemoteCache(EventContext& ec, EventEngine* ee,
	PartitionTable* pt, DatabaseId dbId, const char* dbName,
	const char* tableName, TablePartitioningVersionId versionId) {
	if (pt->getNodeNum() == 1) return;
	Event request(ec, UPDATE_TABLE_CACHE, IMMEDIATE_PARTITION_ID);
	EventByteOutStream out = request.getOutStream();
	out << dbId;
	if (dbName != NULL && tableName != NULL) {
		StatementHandler::encodeStringData<EventByteOutStream>(out, dbName);
		StatementHandler::encodeStringData<EventByteOutStream>(out, tableName);
		out << versionId;
	}
	for (int32_t pos = 1; pos < pt->getNodeNum(); pos++) {
		const NodeDescriptor& nd = ee->getServerND(pos);
		try {
			ee->send(request, nd);
		}
		catch (std::exception& e) {
			UTIL_TRACE_EXCEPTION_WARNING(SQL_SERVICE, e, "");
		}
	}
}

PartitionTable* SQLExecutionManager::getPartitionTable() {
	return pt_;
}

SQLIndexStatsCache& SQLExecutionManager::getIndexStats() {
	return indexStats_;
}

SQLExecution::SQLReplyType SQLExecution::checkReplyType(
	int32_t errorCode, bool& clearCache) {
	switch (errorCode) {
	case GS_ERROR_TXN_CLUSTER_ROLE_UNMATCH:
	case GS_ERROR_DS_CON_LOCK_CONFLICT:
	case GS_ERROR_JOB_INVALID_RECV_BLOCK_NO:
		clearCache = true;
		return SQL_REPLY_DENY;
	case GS_ERROR_TXN_PARTITION_ROLE_UNMATCH:
	case GS_ERROR_TXN_PARTITION_STATE_UNMATCH:
	case GS_ERROR_SQL_COMPILE_INVALID_NODE_ASSIGN:
	case GS_ERROR_NOSQL_FAILOVER_TIMEOUT:
	case GS_ERROR_DS_DS_CONTAINER_ID_INVALID:
	case GS_ERROR_DS_CONTAINER_UNEXPECTEDLY_REMOVED:
	case GS_ERROR_DS_DS_CONTAINER_EXPIRED:
	case GS_ERROR_TXN_CONTAINER_NOT_FOUND:
	case GS_ERROR_TXN_CONTAINER_SCHEMA_UNMATCH:
	case GS_ERROR_SQL_PROC_INVALID_CONSTRAINT_NULL:
	case GS_ERROR_SQL_PROC_INTERNAL_INDEX_UNMATCH:
	case GS_ERROR_SQL_DML_EXPIRED_SUB_CONTAINER_VERSION:
	case GS_ERROR_SQL_DML_CONTAINER_STAT_GAP_TOO_LARGE:
	case GS_ERROR_SQL_PROC_UNSUPPORTED_TYPE_CONVERSION:
	case GS_ERROR_SQL_PROC_VALUE_SYNTAX_ERROR:
	case GS_ERROR_CM_INTERNAL_ERROR:
	case GS_ERROR_SQL_PROC_INTERNAL_INVALID_OPTION:
	case GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH:
		clearCache = true;
		return SQL_REPLY_INTERNAL;
	case GS_ERROR_SQL_COMPILE_COLUMN_NOT_FOUND:
	case GS_ERROR_SQL_COMPILE_COLUMN_NOT_RESOLVED:
	case GS_ERROR_SQL_COMPILE_COLUMN_LIST_UNMATCH:
	case GS_ERROR_SQL_COMPILE_MISMATCH_SCHEMA:
		clearCache = true;
		return SQL_REPLY_INTERNAL;
	default:
		return SQL_REPLY_ERROR;
	}
}


const char* SQLExecution::SQLExecutionContext::getDBName() const {
	return clientInfo_.dbName_.c_str();
}

const char* SQLExecution::SQLExecutionContext::getApplicationName() const {
	return clientInfo_.applicationName_.c_str();
}

bool SQLExecution::SQLExecutionContext::isSetApplicationName() const {
	return !clientInfo_.applicationName_.empty();
}

const char* SQLExecution::SQLExecutionContext::getQuery() const {
	return analyzedQueryInfo_.query_.c_str();
}

bool SQLExecution::SQLExecutionContext::isSetQuery() const {
	return !analyzedQueryInfo_.query_.empty();
}

void SQLExecution::SQLExecutionContext::getQuery(
	util::String& str, size_t size) {
	const char* ptr = analyzedQueryInfo_.query_.c_str();
	if (ptr) {
		str.append(analyzedQueryInfo_.query_.c_str(), size);
	}
}

void SQLExecution::SQLExecutionContext::setQuery(util::String& query) const {
	if (query.size() > 0) {
		analyzedQueryInfo_.query_ = query.c_str();
	}
}

DatabaseId SQLExecution::SQLExecutionContext::getDBId() const {
	return clientInfo_.dbId_;
}

bool SQLExecution::SQLExecutionContext::isAdministrator() const {
	return clientInfo_.isAdministrator_;
}

const SQLString& SQLExecution::SQLExecutionContext::getUserName() const {
	return clientInfo_.userName_;
}

int64_t SQLExecution::SQLExecutionContext::getStartTime() const {
	return executionStatus_.startTime_;
}

void SQLExecution::SQLExecutionContext::setStartTime(int64_t startTime) {
	executionStatus_.startTime_ = startTime;
}

int64_t SQLExecution::SQLExecutionContext::getEndTime() const {
	return executionStatus_.endTime_;
}

void SQLExecution::SQLExecutionContext::setEndTime(int64_t endTime) {
	executionStatus_.endTime_ = endTime;
}

double SQLExecution::SQLExecutionContext::getStoreMemoryAgingSwapRate() const {
	return clientInfo_.storeMemoryAgingSwapRate_;
}

const util::TimeZone& SQLExecution::SQLExecutionContext::getTimezone() const {
	return clientInfo_.timezone_;
}

ClientId& SQLExecution::SQLExecutionContext::getId() const {
	return clientId_;
}

const char* SQLExecution::SQLExecutionContext::getNormalizedDBName() {
	return clientInfo_.normalizedDbName_.c_str();
}

bool SQLExecution::SQLExecutionContext::isRetryStatement() const {
	return response_.isRetrying_;
}

bool SQLExecution::SQLExecutionContext::isPreparedStatement() const {
	return analyzedQueryInfo_.prepared_;
}

const NodeDescriptor& SQLExecution::SQLExecutionContext::getClientNd() const {
	return clientInfo_.clientNd_;
}

const std::string SQLExecution::SQLExecutionContext::getClientAddress() const {
	util::NormalOStringStream oss;
	std::string work;
	oss << clientInfo_.clientNd_;
	work = oss.str();
	return work.substr(work.find("address=")+8,work.length()-work.find("address=")-9);
}

ExecutionId SQLExecution::SQLExecutionContext::getExecId() {
	return executionStatus_.execId_;
}

NoSQLSyncContext& SQLExecution::SQLExecutionContext::getSyncContext() {
	return *syncContext_;
}

StatementHandler::ConnectionOption
& SQLExecution::SQLExecutionContext::getConnectionOption() {
	return clientInfo_.connOption_;
}

bool SQLExecution::replyClient(SQLExecution::SQLReplyContext& cxt) {
	switch (cxt.replyType_) {
	case SQL_REPLY_SUCCESS:
		return replySuccess(*cxt.ec_, cxt.typeList_, cxt.versionId_, cxt.responseJobId_);
		break;
	case SQL_REPLY_SUCCESS_EXPLAIN:
		return replySuccessExplain(*cxt.ec_, UNDEF_JOB_VERSIONID, NULL);
		break;
	case SQL_REPLY_SUCCESS_EXPLAIN_ANALYZE:
		return replySuccessExplainAnalyze(*cxt.ec_, cxt.job_,
			cxt.versionId_, cxt.responseJobId_);
		break;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_SQL_INTERNAL, "");
	}
}

void SQLExecution::SQLExecutionContext::getCurrentJobId(JobId& jobId, bool isLocal) {
	UNUSED_VARIABLE(isLocal);

	util::LockGuard<util::Mutex> guard(lock_);
	jobId = currentJobId_;
}

void SQLExecution::SQLExecutionContext::setCurrentJobId(JobId& jobId) {
	util::LockGuard<util::Mutex> guard(lock_);
	currentJobId_ = jobId;
	pendingExceptionExecId_ = UNDEF_STATEMENTID;
}

void SQLExecution::SQLExecutionContext::setPendingException(std::exception& e) {
	util::LockGuard<util::Mutex> guard(lock_);
	pendingExceptionExecId_ = currentJobId_.execId_;
	pendingException_ = GS_EXCEPTION_CONVERT(e, "");
}

void SQLExecution::SQLExecutionContext::resetPendingException() {
	util::LockGuard<util::Mutex> guard(lock_);
	pendingExceptionExecId_ = UNDEF_STATEMENTID;
}

bool SQLExecution::SQLExecutionContext::checkPendingException(bool flag) {
	util::LockGuard<util::Mutex> guard(lock_);
	if (pendingExceptionExecId_ != UNDEF_STATEMENTID) {
		if (flag) {
			try {
				pendingExceptionExecId_ = UNDEF_STATEMENTID;
				throw pendingException_;
			}
			catch (std::exception& e) {
				GS_RETHROW_USER_ERROR(e, "");
			}
		}
		return true;
	}
	return false;
}

uint8_t SQLExecution::SQLExecutionContext::getCurrentJobVersionId() {
	util::LockGuard<util::Mutex> guard(lock_);
	return currentJobId_.versionId_;
}

util::StackAllocator* SQLExecution::wrapStackAllocator() {
	return executionManager_->getStackAllocator(NULL);
}

void SQLExecutionManager::refreshSchemaCache() {
	try {
		DBConnection* conn = getDBConnection();
		conn->refreshSchemaCache();
	}
	catch (std::exception& e) {
	}
}

const std::string SQLExecution::QueryAnalyzedInfo::getTableNameList()  {
	util::NormalOStringStream oss;
	if (parsedInfo_.tableList_.size() > 0) {
		oss << "'tableName'=[";

		for (util::Vector<SyntaxTree::Expr*>::iterator it = parsedInfo_.tableList_.begin();
			it != parsedInfo_.tableList_.end(); it++) {
			std::string dbName;
			if ((*it)->qName_ && (*it)->qName_->db_) {
				dbName = (*it)->qName_->db_->c_str();
			}
			const SyntaxTree::QualifiedName* srcName = (*it)->qName_;
			if (srcName == NULL) {
				continue;
			}
			if (srcName->table_ == NULL) {
				continue;
			}
			oss << "'";
			if (!dbName.empty()) {
				oss << dbName;
				oss << ":";
			}
			oss << srcName->table_->c_str();
			oss << "'";
			if (it != parsedInfo_.tableList_.end() - 1) {
				oss << ",";
			}
		}
		oss << "] ";
	}
	else if (isDDL_) {
		if (parsedInfo_.syntaxTreeList_.size() > 0) {
			SyntaxTree::QualifiedName* targetName_ = parsedInfo_.syntaxTreeList_[0]->targetName_;
			if (targetName_ && targetName_->table_) {
				std::string dbName;
				if (targetName_->db_) {
					dbName = targetName_->db_->c_str();
				}
				oss << "'tableName'=['";
				if (!dbName.empty()) {
					oss << dbName;
					oss << ":";
				}
				oss << targetName_->table_->c_str();
				oss << "'] ";
			}
		}
	}
	return oss.str().c_str();
}

const std::string SQLExecution::SQLExecutionContext::getTableNameList() const {
	return analyzedQueryInfo_.getTableNameList();
}

void SQLExecutionManager::getDatabaseStats(util::StackAllocator& alloc,
	DatabaseId dbId, util::Map<DatabaseId, DatabaseStats*>& statsMap, bool isAdministrator) {
	util::Vector<ClientId> clientIdList(alloc);
	getCurrentClientIdList(clientIdList);
	for (size_t pos = 0; pos < clientIdList.size(); pos++) {
		SQLExecutionManager::Latch latch(clientIdList[pos], this);
		SQLExecution* execution = latch.get();
		if (execution) {
			const SQLExecution::SQLExecutionContext& sqlContext = execution->getContext();
			DatabaseId currentDbId = sqlContext.getDBId();
			if (isAdministrator || currentDbId == dbId) {
				auto stats = statsMap.find(currentDbId);
				if (stats == statsMap.end()) {
					statsMap[currentDbId] = ALLOC_NEW(alloc) DatabaseStats;
				}
				statsMap[currentDbId]->connectionStats_.sqlRequestCount_++;
				statsMap[currentDbId]->jobStats_.allocateMemorySize_ += execution->getTotalSize();
			}
		}
	}
}

void JobManager::remove(JobId& jobId) {
	try {
		util::LockGuard<util::Mutex> guard(mutex_);
		JobMapItr it = jobMap_.find(jobId);
		if (it != jobMap_.end()) {
			Job* job = (*it).second;
			jobMap_.erase(it);
			if (job->isCoordinator()) {
				job->setCancel();
				SQLExecutionManager::Latch latch(jobId.clientId_, executionManager_);
				SQLExecution* execution = latch.get();
				if (execution) {
					job->traceLongQuery(execution);
				}
			}
			if (job->decReference()) {
				util::StackAllocator* alloc = job->getLocalStackAllocator();
				ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, job);
				executionManager_->releaseStackAllocator(alloc);
			}
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void JobManager::Job::traceLongQuery(SQLExecution* execution) {
	traceLongQueryCore(execution, startTime_, watch_.elapsedMillis());
}

void JobManager::Job::traceLongQueryCore(SQLExecution* execution, int64_t startTime, int64_t execTime) {
	try {
		SQLService* sqlSvc = execution->getExecutionManager()->getSQLService();
		int64_t traceLimitTime = sqlSvc->getTraceLimitTime();
		if (execTime < traceLimitTime) {
			return;
		}
		util::NormalOStringStream ss;
		ss << "startTime=" << CommonUtility::getTimeStr(startTime) << ", executionTime=" << execTime;
		int32_t orgLength = static_cast<int32_t>(strlen(execution->getContext().getQuery()));
		int32_t queryLength = orgLength;
		int32_t queryLimitSize = sqlSvc->getTraceLimitQuerySize();
		if (queryLength > queryLimitSize) {
			queryLength = queryLimitSize;
			ss << ", query=";
			ss.write(execution->getContext().getQuery(), queryLength);
			ss << "(ommited, limit=" << queryLimitSize << ", size=" << orgLength << ")";
		}
		else if (queryLength > 0) {
			ss << ", query=" << execution->getContext().getQuery();
		}
		AUDIT_TRACE_WARNING_EXECUTE(execution, ss.str().c_str());
		ss << ", dbName=" << execution->getContext().getDBName()
			<< ", appName=" << execution->getContext().getApplicationName();
		GS_TRACE_WARNING(SQL_DETAIL, GS_TRACE_SQL_LONG_QUERY, ss.str().c_str());
	}
	catch (std::exception& e) {
		UTIL_TRACE_EXCEPTION(SQL_SERVICE, e, "");
	}
}

SQLExecution::ExecutionProfilerWatcher::~ExecutionProfilerWatcher() {
	if (trace_) {
		JobManager::Job::traceLongQueryCore(execution_, execution_->getContext().getStartTime(), watch_.elapsedMillis());
	}
}

bool SQLExecution::checkExecutionConstraint(uint32_t& delayStartTime, uint32_t& delayExecutionTime) {
	if (jobManager_->getClusterService()->getManager()->getStandbyInfo().isStandby() && isNotSelect()) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_DENY_REQUEST,
			"Deny updatable query (standby mode)");
	}
	DatabaseId dbId = context_.getDBId();
	DatabaseManager& dbManager = jobManager_->getTransactionService()->getManager()->getDatabaseManager();

	dbManager.incrementRequest(getContext().getDBId(), true);

	if (!dbManager.isEnableRequestConstraint()) {
		return false;
	}
	DatabaseManager::Option option;
	SQLParsedInfo& parsedInfo = getParsedInfo();
	if (parsedInfo.syntaxTreeList_.size() > 0) {
		option.dropOnly_ = parsedInfo.syntaxTreeList_[0]->isDropOnlyCommand();
	}
	return dbManager.getExecutionConstraint(dbId, delayStartTime, delayExecutionTime, true, &option);
}

void SQLExecution::checkBatchRequest(RequestInfo& request) {
	if (request.option_.get<StatementMessage::Options::FEATURE_VERSION>()
		== StatementMessage::FEATURE_V5_5 && !isNotSelect()) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_INVALID_BATCH_STATEMENT, 
			"Query with result set can not be used in batch method");
	}
}
