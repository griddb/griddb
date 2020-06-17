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
#include "sql_execution_manager.h"
#include "resource_set.h"
#include "sql_execution.h"
#include "sql_processor_config.h"
#include "sql_allocator_manager.h"
#include "sql_service.h"
#include "sql_resource_manager.h"
#include "nosql_db.h"
#include "uuid_utils.h"

UTIL_TRACER_DECLARE(SQL_SERVICE);

const int32_t SQLExecutionManager::
		DEFAULT_TABLE_CACHE_SIZE = 512;

typedef StatementHandler::ConnectionOption ConnectionOption;

SQLExecutionManager::ConfigSetUpHandler
		SQLExecutionManager::configSetUpHandler_;

SQLExecutionManager::StatSetUpHandler
		SQLExecutionManager::statSetUpHandler_;

SQLExecutionManager::SQLExecutionManager(
		ConfigTable &config,
		SQLAllocatorManager &allocatorManager) :
				allocatorManager_(&allocatorManager),
				globalVarAlloc_(
						*allocatorManager.getGlobalAllocator()),
				resourceManager_(NULL),
				resourceSet_(NULL),
				db_(NULL),
				processorConfig_(NULL),
				stats_(
						config.get<int32_t>(CONFIG_TABLE_SQL_CONCURRENCY)),
				config_(config) {

	processorConfig_ = UTIL_NEW SQLProcessorConfig();
	getProcessorConfig();

	resourceManager_ = UTIL_NEW
			SQLExecutionResourceManager(globalVarAlloc_);


	config_.setUpConfigHandler(config);
}

SQLExecutionManager::~SQLExecutionManager() {

	delete processorConfig_;
	delete db_;
	delete resourceManager_;
}

void SQLExecutionManager::initialize(
		const ResourceSet &resourceSet) {

	resourceSet_ = &resourceSet;
	
	int32_t cacheSize = DEFAULT_TABLE_CACHE_SIZE;
	db_ = UTIL_NEW
			NoSQLDB(&resourceSet, *resourceSet.config_,
					globalVarAlloc_, cacheSize);
}

bool SQLExecutionManager::cancel(
		EventContext &ec, ClientId &clientId) {

	JobId cancelJobId;
	bool isExists = false;
	bool isPreparedStatemnt = false;
	{
		ExecutionLatch latch(
				clientId, resourceManager_, NULL);
		SQLExecution *execution = latch.get();
		if (execution) {
			execution->getContext().getCurrentJobId(cancelJobId);
			execution->cancel(ec);
			isExists = true;
			isPreparedStatemnt
					= execution->getContext().isPreparedStatement();
		}
	}

	if (isExists) {
		JobManager *jobManager = resourceSet_->getJobManager();
		jobManager->cancel(ec, cancelJobId, false);
	}
	return isPreparedStatemnt;
}

void SQLExecutionManager::remove(
		EventContext &ec,
		ClientId &clientId,
		bool withCheck) {

	try {
		bool isPreparedStatemnt = cancel(ec, clientId);
		if (!withCheck || (withCheck && !isPreparedStatemnt)) {
			resourceManager_->remove(clientId);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void SQLExecutionManager::closeConnection(
		EventContext &ec,
		const NodeDescriptor &nd) {

	try {
		ClientId clientId;
		util::StackAllocator &alloc = ec.getAllocator();
		util::XArray<SessionId> sessionIdList(alloc);
		{
			util::LockGuard<util::Mutex> guard(lock_);
			ConnectionOption &connOption =
					nd.getUserData<ConnectionOption>();
			connOption.getSessionIdList(
					clientId, sessionIdList);
		}
	
		for (size_t pos = 0;
				pos < sessionIdList.size(); pos++) {
			
			clientId.sessionId_ = sessionIdList[pos];
		
			ExecutionLatch latch(clientId, resourceManager_, NULL);
			SQLExecution *execution = latch.get();
			if (execution) {
				try {
					remove(ec, clientId);
				}
				catch(std::exception &e) {
					UTIL_TRACE_EXCEPTION(SQL_SERVICE, e,
							"Remove SQL execution failed, "
							"but continue, clientId=" << clientId);
				}
			}
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

#define STAT_ADD(id) STAT_TABLE_ADD_PARAM(stat, parentId, id)

void SQLExecutionManager::StatSetUpHandler::operator()(StatTable &stat) {

	StatTable::ParamId parentId;
	parentId = STAT_TABLE_ROOT;
	stat.resolveGroup(parentId, STAT_TABLE_PERF, "performance");
	parentId = STAT_TABLE_PERF;
	STAT_ADD(STAT_TABLE_PERF_SQL_STORE_SWAP_FILE_SIZE_TOTAL);
	STAT_ADD(STAT_TABLE_PERF_SQL_TOTAL_READ_OPERATION);
	STAT_ADD(STAT_TABLE_PERF_SQL_TOTAL_WRITE_OPERATION);
	STAT_ADD(STAT_TABLE_PERF_SQL_STORE_SWAP_READ);
	STAT_ADD(STAT_TABLE_PERF_SQL_STORE_SWAP_READ_SIZE);
	STAT_ADD(STAT_TABLE_PERF_SQL_STORE_SWAP_READ_TIME);
	STAT_ADD(STAT_TABLE_PERF_SQL_STORE_SWAP_WRITE);
	STAT_ADD(STAT_TABLE_PERF_SQL_STORE_SWAP_WRITE_SIZE);
	STAT_ADD(STAT_TABLE_PERF_SQL_STORE_SWAP_WRITE_TIME);
}

void SQLExecutionManager::updateStat(StatTable &stat) {


	LocalTempStore &store = *resourceSet_->getTempolaryStore();

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

	if (!stat.getDisplayOption(
				STAT_TABLE_DISPLAY_WEB_ONLY) ||
		!stat.getDisplayOption(
				STAT_TABLE_DISPLAY_OPTIONAL_MEM)) {
		return;
	}
	stat.set(STAT_TABLE_PERF_SQL_TOTAL_READ_OPERATION,
			getTotalReadOperation());
	stat.set(STAT_TABLE_PERF_SQL_TOTAL_WRITE_OPERATION,
			getTotalWriteOperation());
}

void SQLExecutionManager::setRequestedConnectionEnv(
		const RequestInfo &request,
		const NodeDescriptor &nd) {

	typedef StatementMessage::PragmaList PragmaList;
	const PragmaList *pragmeList =
			request.option_.get<
					StatementMessage::Options::PRAGMA_LIST>();

	if (pragmeList == NULL) {
		return;
	}

	for (PragmaList::List::const_iterator
			it = pragmeList->list_.begin();
			it != pragmeList->list_.end(); ++it) {

		const SQLPragma::PragmaType paramId =
				SQLPragma::getPragmaType(it->first.c_str());
		
		if (paramId != SQLPragma::PRAGMA_NONE) {
			setConnectionEnv(
					nd, paramId, it->second.c_str());
		}
	}
}

void SQLExecutionManager::setConnectionEnv(
		SQLExecution *execution,
		uint32_t paramId,
		const char8_t *value) {
			
	setConnectionEnv(
			execution->getContext().getClientNd(),
			paramId, value);
}

void SQLExecutionManager::setConnectionEnv(
		const NodeDescriptor &nd,
		uint32_t paramId,
		const char8_t *value) {

	ConnectionOption &connOption
			= nd.getUserData<ConnectionOption>();

	if (paramId >= SQLPragma::PRAGMA_TYPE_MAX) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				"ParamId=" << paramId << " is invalid.");
	}
	connOption.setConnectionEnv(paramId, value);
}

bool SQLExecutionManager::getConnectionEnv(
		SQLExecution *execution,
		uint32_t paramId,
		std::string &value,
		bool &hasData) {

	return getConnectionEnv(
			execution->getContext().getClientNd(),
			paramId,
			value,
			hasData);
}

bool SQLExecutionManager::getConnectionEnv(
		const NodeDescriptor &nd,
		uint32_t paramId,
		std::string &value,
		bool &hasData) {

	ConnectionOption &connOption
			= nd.getUserData<ConnectionOption>();
	
	return connOption.getConnectionEnv(
			paramId, value, hasData);
}

void SQLExecutionManager::completeConnectionEnvUpdates(
		const NodeDescriptor &nd) {

	util::LockGuard<util::Mutex> guard(lock_);
	
	ConnectionOption &connOption
			= nd.getUserData<ConnectionOption>();
	connOption.updatedEnvBits_ = 0;
}

uint32_t SQLExecutionManager::getConnectionEnvUpdates(
		const NodeDescriptor &nd) {

	util::LockGuard<util::Mutex> guard(lock_);
	ConnectionOption &connOption
			= nd.getUserData<ConnectionOption>();

	return connOption.updatedEnvBits_;
}

SQLConnectionEnvironment ::SQLConnectionEnvironment (
		SQLExecutionManager *executionMgr,
		SQLExecution *execution) :
				executionMgr_(executionMgr),
				execution_(execution),
				hasData_(true) {}

void SQLConnectionEnvironment ::setEnv(
		uint32_t paramId, std::string &value) {

	hasData_ = true;
	executionMgr_->setConnectionEnv(
			execution_, paramId, value.c_str());
}

bool SQLConnectionEnvironment ::getEnv(
		uint32_t paramId, std::string &key) {

	if (!hasData_) return false;
	return executionMgr_->getConnectionEnv(
			execution_, paramId, key, hasData_);
}

void SQLExecutionManager::getProfiler(
		util::StackAllocator &alloc,
		SQLProfiler &profs) {

	util::LockGuard<util::Mutex> guard(
			resourceManager_->getLock());
	
	for (SQLExecutionMapItr
			it = resourceManager_->begin();
			it != resourceManager_->end(); it++) {

		SQLExecution *execution = (*it).second;

		if (execution != NULL) {

			SQLProfilerInfo prof(alloc);
			prof.startTime_ = getTimeStr(
					execution->getContext().getStartTime()).c_str();
			prof.sql_ = execution->getContext().getQuery();
			util::NormalOStringStream strstrm;

			char tmpBuffer[UUID_STRING_SIZE];
			UUIDUtils::unparse(
					execution->getContext().getId().uuid_, tmpBuffer);

			util::String tmpUUIDStr(tmpBuffer, 36, alloc);	
			strstrm << tmpUUIDStr.c_str();
			strstrm << ":" << execution->getContext().getId().sessionId_;
			prof.clientId_ = strstrm.str().c_str();
			
			if (!execution->getContext().isPreparedStatement()) {
				prof.queryType_ = "NORMAL";
			}
			else {
				prof.queryType_ = "PREPARED";
			}
			profs.sqlProfiler_.push_back(prof);
		}
	}
}

void SQLExecutionManager::getCurrentClientIdList(
		util::Vector<ClientId> &clientIdList) {

	resourceManager_->getKeyList(clientIdList);
}

void SQLExecutionManager::cancelAll(
		const Event::Source &eventSource,
		util::Vector<ClientId> &clientIdList,
		CancelOption &option) {

	try {

		JobManager *jobManager
				= resourceSet_->getJobManager();
		
		if (clientIdList.empty()) {
			getCurrentClientIdList(clientIdList);
		}

		for (size_t pos = 0;
				pos < clientIdList.size(); pos++) {

			ExecutionLatch latch(
					clientIdList[pos], resourceManager_, NULL);

			SQLExecution *execution = latch.get();
			if (execution) {

				JobId jobId;
				execution->getContext().getCurrentJobId(jobId);

				jobManager->cancel(eventSource, jobId, option);
				execution->cancel(eventSource);
			}
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

SQLVariableSizeGlobalAllocator
		&SQLExecutionManager::getVarAllocator() {
	return globalVarAlloc_;
}

const SQLProcessorConfig*
		SQLExecutionManager::getProcessorConfig() const {

	processorConfig_->workMemoryLimitBytes_
			= allocatorManager_->getWorkMemoryLimitSize();
	
	processorConfig_->workMemoryCacheBytes_
			= allocatorManager_->getWorkCacheMemorySize();

	return processorConfig_;
}

util::StackAllocator*
		SQLExecutionManager::getStackAllocator() {

	return allocatorManager_->getStackAllocator();
}

void SQLExecutionManager::releaseStackAllocator(
		util::StackAllocator *alloc) {

	allocatorManager_->releaseStackAllocator(alloc);
}

SQLExecutionManager::Stats::Stats(int32_t concurrency) {

	totalWriteOperationList_.assign(concurrency, 0);
	totalReadOperationList_.assign(concurrency, 0);
};

void SQLExecutionManager::ConfigSetUpHandler::operator()(
		ConfigTable &config) {

	CONFIG_TABLE_RESOLVE_GROUP(config, CONFIG_TABLE_SQL, "sql");

	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_SQL_TRACE_LIMIT_EXECUTION_TIME, INT32)
			.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
			.setMin(1)
			.setDefault(300);

	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_SQL_TRACE_LIMIT_QUERY_SIZE, INT32)
			.setMin(1)
			.setDefault(1000);

	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_SQL_PARTITIONING_ROWKEY_CONSTRAINT, BOOL)
			.setExtendedType(ConfigTable::EXTENDED_TYPE_LAX_BOOL)
			.setDefault(true);

	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_SQL_MULTI_INDEX_SCAN, BOOL)
			.setExtendedType(ConfigTable::EXTENDED_TYPE_LAX_BOOL)
			.setDefault(false);

	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_SQL_STORE_MEMORY_LIMIT, INT32).
			setUnit(ConfigTable::VALUE_UNIT_SIZE_MB).
			setMin(1).
			setDefault(LocalTempStore::DEFAULT_STORE_MEMORY_LIMIT_MB);

	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_SQL_FETCH_SIZE_LIMIT, INT32).
			setUnit(ConfigTable::VALUE_UNIT_SIZE_MB).
			setMin(1).
			setDefault(8);

	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_SQL_STORE_SWAP_FILE_SIZE_LIMIT, INT32).
			setUnit(ConfigTable::VALUE_UNIT_SIZE_MB).
			setMin(1).
			setDefault(
					LocalTempStore::DEFAULT_STORE_SWAP_FILE_SIZE_LIMIT_MB);

	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_SQL_STORE_SWAP_FILE_PATH, STRING).
			setDefault(LocalTempStore::DEFAULT_SWAP_FILES_TOP_DIR);

	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_SQL_TRANSACTION_MESSAGE_SIZE_LIMIT, INT32).
			setUnit(ConfigTable::VALUE_UNIT_SIZE_MB).
			setMin(1).
			setDefault(2);

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

/*!
	@brief Configuration of execution manager
*/
SQLExecutionManager::Config::Config(
		ConfigTable &configTable) {
	
	traceLimitTime_ = configTable.get<int32_t>(
			CONFIG_TABLE_SQL_TRACE_LIMIT_EXECUTION_TIME);

	traceLimitQuerySize_ = configTable.get<int32_t>(
			CONFIG_TABLE_SQL_TRACE_LIMIT_QUERY_SIZE);

	fetchSizeLimit_ = ConfigTable::megaBytesToBytes(
			static_cast<size_t>(configTable.get<int32_t>(
					CONFIG_TABLE_SQL_FETCH_SIZE_LIMIT)));

	nosqlSizeLimit_ = ConfigTable::megaBytesToBytes(
			static_cast<size_t>(configTable.get<int32_t>(
					CONFIG_TABLE_SQL_TRANSACTION_MESSAGE_SIZE_LIMIT)));

	multiIndexScan_ = configTable.get<bool>(
			CONFIG_TABLE_SQL_MULTI_INDEX_SCAN);

	partitioningRowKeyConstraint_ 
			= configTable.get<bool>(
					CONFIG_TABLE_SQL_PARTITIONING_ROWKEY_CONSTRAINT);
}

/*!
	@brief Setup configuration
*/
void SQLExecutionManager::Config::setUpConfigHandler(
		ConfigTable &configTable) {
	
	configTable.setParamHandler(
			CONFIG_TABLE_SQL_TRACE_LIMIT_EXECUTION_TIME, *this);
	
	configTable.setParamHandler(
			CONFIG_TABLE_SQL_TRACE_LIMIT_QUERY_SIZE, *this);

	configTable.setParamHandler(
			CONFIG_TABLE_SQL_MULTI_INDEX_SCAN, *this);

	configTable.setParamHandler(
			CONFIG_TABLE_SQL_PARTITIONING_ROWKEY_CONSTRAINT, *this);
}

/*!
	@brief Configuration operator
*/
void SQLExecutionManager::Config::operator()(
		ConfigTable::ParamId id, const ParamValue &value) {

	switch (id) {

	case CONFIG_TABLE_SQL_TRACE_LIMIT_EXECUTION_TIME:
		traceLimitTime_ = value.get<int32_t>();
		break;

	case CONFIG_TABLE_SQL_TRACE_LIMIT_QUERY_SIZE:
		traceLimitQuerySize_ = value.get<int32_t>();
		break;

	case CONFIG_TABLE_SQL_MULTI_INDEX_SCAN:
		multiIndexScan_ = value.get<bool>();
		break;

	case CONFIG_TABLE_SQL_PARTITIONING_ROWKEY_CONSTRAINT:
		partitioningRowKeyConstraint_ = value.get<bool>();
		break;

	default:
		break;
	}
}

SQLExecutionManager::Config::~Config() {
}

void SQLExecutionManager::dump() {

	util::LockGuard<util::Mutex> guard(
			resourceManager_->getLock());

	for (SQLExecutionMapItr
			it = resourceManager_->begin();
			it != resourceManager_->end(); it++) {

		SQLExecution *execution = (*it).second;
		if (execution) {
			GS_TRACE_INFO(
					SQL_SERVICE, GS_TRACE_SQL_INTERNAL_DEBUG,
					"Execution : ExecId="
					<< execution->getContext().getId());
		}
	}
}

void SQLExecutionManager::dumpTableCache(
		util::NormalOStringStream &oss, const char *dbName) {

	db_->dump(oss, dbName);
}

void SQLExecutionManager::resizeTableCache(
		const char *dbName, int32_t cacheSize) {

	db_->resizeTableCache(dbName, cacheSize);
}

void SQLExecutionManager::clearConnection(
		Event &ev, bool isNewSQL) {
	
	if (isNewSQL) {
		util::LockGuard<util::Mutex> guard(lock_);
		
		StatementHandler::ConnectionOption &connOption =
				ev.getSenderND().getUserData<
						StatementHandler::ConnectionOption>();

		connOption.clear();
	}
	else {
		StatementHandler::ConnectionOption &connOption =
				ev.getSenderND().getUserData<
						StatementHandler::ConnectionOption>();

		connOption.clear();
	}
}

void SQLExecutionManager::checkTimeout(EventContext &ec) {

	JobManager *jobManager
			= resourceSet_->getJobManager();

	util::StackAllocator &alloc = ec.getAllocator();
	util::Vector<ClientId> clientIdList(alloc);
	getCurrentClientIdList(clientIdList);
	
	for (util::Vector<ClientId>::iterator
			it = clientIdList.begin();
			it != clientIdList.end(); it++) {

		ExecutionLatch latch(
				(*it), resourceManager_, NULL);
		
		SQLExecution *execution = latch.get();
		if (execution) {
		
			JobId jobId;
			execution->getContext().getCurrentJobId(jobId);
			jobManager->checkAlive(ec, jobId);

			if (execution->isCancelled()) {
				execution->cancel(jobId.execId_);
			}
		}
	}
}

NoSQLDB *SQLExecutionManager::getDB() {
	return db_;
}

void SQLExecutionManager::incOperation(
		bool isSQL, int32_t workerId, int64_t count) {

	if (isSQL) {
		stats_.totalReadOperationList_[workerId] += count;
	}
	else {
		stats_.totalWriteOperationList_[workerId] += count;
	}
}

uint64_t SQLExecutionManager::getTotalReadOperation() {

	uint64_t total = 0;
	for (size_t pos = 0;
			pos < stats_.totalReadOperationList_.size(); pos++) {
		total += stats_.totalReadOperationList_[pos];
	}
	return total;
}

uint64_t SQLExecutionManager::getTotalWriteOperation() {

	uint64_t total = 0;
	for (size_t pos = 0;
			pos < stats_.totalWriteOperationList_.size(); pos++) {
		total += stats_.totalWriteOperationList_[pos];
	}
	return total;
}