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
	@brief sql service
*/

#include "sql_service.h"
#include "sql_command_manager.h"
#include "sql_utils.h"
#include "query_processor.h"
#include "query_collection.h"
#include "data_store_v4.h"
#include "database_manager.h"

template<typename T>
void decodeLargeRow(
		const char* key, util::StackAllocator& alloc, TransactionContext& txn,
		DataStoreV4* dataStore, const char8_t* dbName, BaseContainer* container,
		T& record, const EventMonotonicTime emNow);

#ifndef _WIN32
#include <signal.h>  
#endif
UTIL_TRACER_DECLARE(SQL_SERVICE);
UTIL_TRACER_DECLARE(DISTRIBUTED_FRAMEWORK);
UTIL_TRACER_DECLARE(DATA_EXPIRATION_DETAIL);

SQLService::ConfigSetUpHandler
SQLService::configSetUpHandler_;

const int32_t SQLService::SQL_V1_1_X_CLIENT_VERSION = -1;
const int32_t SQLService::SQL_V1_5_X_CLIENT_VERSION = -2;
const int32_t SQLService::SQL_V2_9_X_CLIENT_VERSION = -3;
const int32_t SQLService::SQL_V3_0_X_CLIENT_VERSION = -4;
const int32_t SQLService::SQL_V3_1_X_CLIENT_VERSION = -5;
const int32_t SQLService::SQL_V3_2_X_CLIENT_VERSION = -6;
const int32_t SQLService::SQL_V3_5_X_CLIENT_VERSION = -7;
const int32_t SQLService::SQL_V4_0_0_CLIENT_VERSION = -8;
const int32_t SQLService::SQL_V4_0_1_CLIENT_VERSION = -9;
const int32_t SQLService::SQL_CLIENT_VERSION = SQL_V4_0_1_CLIENT_VERSION;

const int32_t SQLService::SQL_V4_0_MSG_VERSION = 1;
const int32_t SQLService::SQL_V4_1_MSG_VERSION = 2;
const int32_t SQLService::SQL_V4_1_0_MSG_VERSION = 3;
const int32_t SQLService::SQL_V5_5_MSG_VERSION = 4;
const int32_t SQLService::SQL_V5_6_MSG_VERSION = 5;
const int32_t SQLService::SQL_V5_8_MSG_VERSION = 6;
const int32_t SQLService::SQL_MSG_VERSION = SQL_V5_8_MSG_VERSION;
const bool SQLService::SQL_MSG_BACKWARD_COMPATIBLE = false;

static const int32_t ACCEPTABLE_NEWSQL_CLIENT_VERSIONS[] = {
		SQLService::SQL_CLIENT_VERSION,
		SQLService::SQL_V4_0_0_CLIENT_VERSION,
		0 /* sentinel */
};

const int32_t SQLService::DEFAULT_RESOURCE_CONTROL_LEVEL = 2;

template<typename S>
void SQLGetContainerHandler::InMessage::decode(S &in) {
	SimpleInputMessage::decode(in);
	checkAcceptableVersion(request_);
	subCmd_ = decodeSubCommand(in);

	TransactionContext txn;
	txn.replaceDefaultAllocator(alloc_);

	switch (subCmd_) {
	case SUB_CMD_GET_CONTAINER:
		decodeContainerCondition(in, *this, connOption_);
		break;
	case SUB_CMD_ESTIMATE_INDEX_SEARCH_SIZE:
		decodeSearchList(txn, in, searchList_);
		break;
	default:
		assert(false);
	}
}

template<typename S>
void SQLGetContainerHandler::OutMessage::encode(S &out) {
	SimpleOutputMessage::encode(out);
	encodeResult(out, *globalVarAlloc_, info_, estimationList_);
}

const int32_t SQLGetContainerHandler::LATEST_FEATURE_VERSION =
		MessageSchema::V5_8_VERSION;

void SQLGetContainerHandler::operator()(
	EventContext& ec, Event& ev) {

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow
		= ec.getHandlerStartMonotonicTime();
	PartitionId pId = ev.getPartitionId();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {
		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		EVENT_START(ec, ev, transactionManager_, connOption.dbId_, false);

		if (inMes.subCmd_ == SUB_CMD_ESTIMATE_INDEX_SEARCH_SIZE) {
			estimateIndexSearchSize(ec, ev, inMes);
			return;
		}
		else {
			assert(inMes.subCmd_ == SUB_CMD_GET_CONTAINER);
		}

		util::XArray<uint8_t>& containerNameBinary = inMes.containerNameBinary_;

		util::String containerName(alloc);

		TableSchemaInfo info(*sqlService_->getAllocator());
		CompositeIndexInfos* compIndex = NULL;

		if (connOption.clientVersion_
			== TXN_V2_5_X_CLIENT_VERSION) {

			request.optional_.set<Options::DB_NAME>(GS_PUBLIC);
		}

		checkTransactionTimeout(
			emNow,
			ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_,
			ev.getQueueingCount());

		checkExecutable(
			pId,
			(CROLE_MASTER | CROLE_FOLLOWER),
			(PROLE_OWNER | PROLE_BACKUP),
			PSTATE_ON,
			partitionTable_);

		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn
			= transactionManager_->put(
				alloc,
				request.fixed_.pId_,
				TXN_EMPTY_CLIENTID,
				request.fixed_.cxtSrc_,
				now,
				emNow);
		txn.setAuditInfo(&ev, &ec, NULL);

		const FullContainerKey containerKey(
			alloc,
			getKeyConstraint(CONTAINER_ATTR_ANY, false),
			containerNameBinary.data(),
			containerNameBinary.size());

		bool isNewSql = isNewSQL(request);
		checkLoggedInDatabase(
			connOption.dbId_,
			connOption.dbName_.c_str(),
			containerKey.getComponents(alloc).dbId_,
			request.optional_.get<Options::DB_NAME>(),
			isNewSql);

		bool isCaseSensitive = getCaseSensitivity(request).isContainerNameCaseSensitive();
		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(txn,
			containerKey, isCaseSensitive);

		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);

		DSInputMes input(alloc, DS_GET_CONTAINER_OBJECT, ANY_CONTAINER, true);
		StackAllocAutoPtr<DSContainerOutputMes> ret(alloc, static_cast<DSContainerOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		BaseContainer* container = ret.get()->container_;

		if (container) {
			const FullContainerKey
				currentContainerKey = container->getContainerKey(txn);

			util::String* containerNameString =
				ALLOC_NEW(txn.getDefaultAllocator())
				util::String(txn.getDefaultAllocator());

			currentContainerKey.toString(
				txn.getDefaultAllocator(), *containerNameString);

			if (getCaseSensitivity(request).
				isContainerNameCaseSensitive()) {

				util::String targetContainerName(
					txn.getDefaultAllocator());

				containerKey.toString(
					txn.getDefaultAllocator(),
					targetContainerName);

				if (containerNameString->compare(
					targetContainerName)) {

					GS_THROW_USER_ERROR(
						GS_ERROR_SQL_TABLE_PARTITION_PARTITIONING_TABLE_NOT_FOUND,
						"Target table = " << targetContainerName
						<< " is not found");
				}
			}
		}

		if (container != NULL &&
			!checkPrivilege(
				ev.getType(),
				connOption.userType_,
				connOption.requestType_,
				request.optional_.get<Options::SYSTEM_MODE>(),
				request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>(),
				container->getAttribute())) {

			container = NULL;
		}

		info.founded_ = (container != NULL);

		if (info.founded_) {

			SQLVariableSizeGlobalAllocator& sqlAllocator
				= *sqlService_->getAllocator();

			containerKey.toString(alloc, containerName);
			TableContainerInfo containerInfo;

			OptionSet optionSet(alloc);
			info.containerType_ = container->getContainerType();
			info.containerAttr_ = container->getAttribute();
			containerInfo.versionId_ = container->getVersionId();
			containerInfo.containerId_ = container->getContainerId();
			containerInfo.approxSize_ = static_cast<int64_t>(std::min(
					container->getRowNum(),
					static_cast<uint64_t>(
							std::numeric_limits<int64_t>::max())));
			info.containerInfoList_.push_back(containerInfo);

			uint32_t columnNum = container->getColumnNum();

			for (uint32_t i = 0; i < columnNum; i++) {

				TableColumnInfo columnInfo(sqlAllocator);

				info.columnInfoList_.push_back(columnInfo);

				TableColumnInfo& current
					= info.columnInfoList_.back();
				ColumnInfo& noSqlColumn = container->getColumnInfo(i);

				current.name_ = noSqlColumn.getColumnName(
					txn, *container->getObjectManager(), container->getMetaAllocateStrategy());
				current.type_ = noSqlColumn.getColumnType();

				TupleList::TupleColumnType currentType
					= convertNoSQLTypeToTupleType(current.type_);

				bool notNull = noSqlColumn.isNotNull();
				current.tupleType_ = setColumnTypeNullable(
					currentType, !notNull);

				if (notNull) {
					ColumnInfo::setNotNull(current.option_);
				}
			}

			if (container->getColumnInfo(
				ColumnInfo::ROW_KEY_COLUMN_ID).isKey()) {
				info.hasRowKey_ = true;
			}
			else {
				info.hasRowKey_ = false;
			}

			typedef util::Vector<IndexInfo> IndexInfoList;

			IndexInfoList indexInfoList(alloc);
			container->getIndexInfoList(txn, indexInfoList);

			if (indexInfoList.size() > 0) {
				compIndex = ALLOC_NEW(alloc) CompositeIndexInfos(alloc);
				compIndex->setIndexInfos(indexInfoList);
			}

			for (IndexInfoList::iterator it = indexInfoList.begin();
				it != indexInfoList.end(); ++it) {

				TableColumnIndexInfoEntry entry(sqlAllocator);

				entry.indexName_ = it->indexName_.c_str();
				entry.indexType_ = it->mapType;
				info.columnInfoList_[it->columnIds_[0]].
					indexInfo_.push_back(entry);

				const int32_t flags
					= NoSQLCommonUtils::mapTypeToSQLIndexFlags(
						it->mapType,
						info.columnInfoList_[it->columnIds_[0]].type_);

				if (flags != 0) {

					if (info.indexInfoList_.empty()) {
						info.indexInfoList_.resize(columnNum, 0);
					}

					info.indexInfoList_[it->columnIds_[0]] |= flags;
				}
			}

			if (container->getAttribute() == CONTAINER_ATTR_VIEW) {

				ViewInfo viewInfo(alloc);
				const int64_t emNow = ec.getHandlerStartMonotonicTime();

				try {

					decodeLargeRow(
						NoSQLUtils::LARGE_CONTAINER_KEY_VIEW_INFO,
						alloc,
						txn,
						container->getDataStore(),
						connOption.dbName_.c_str(),
						container,
						viewInfo,
						emNow);

					info.sqlString_ = viewInfo.sqlString_.c_str();

				}
				catch (std::exception&) {
				}
			}
		}

		EstimationList estimationList(alloc);
		OutMessage outMes(
				request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
				getReplyOption(alloc, request, compIndex, false),
				&info, sqlService_->getAllocator(), estimationList);
		replySuccess(
				ec, alloc, ev.getSenderND(), ev.getType(), request, outMes,
				false);
	}
	catch (std::exception& e) {
		try {
			handleError(ec, alloc, ev, request, e);
		}
		catch (std::exception& e2) {
			UTIL_TRACE_EXCEPTION(SQL_SERVICE, e2, "");
		}
	}
}

template<typename S>
void SQLGetContainerHandler::encodeResult(
		S &out, SQLVariableSizeGlobalAllocator &varAlloc,
		TableSchemaInfo *info, const EstimationList &estimationList) {
	encodeTableSchemaInfo(out, varAlloc, info);
	encodeEstimationList(out, estimationList);
}

template<typename S>
void SQLGetContainerHandler::decodeResult(
		S &in, util::StackAllocator &alloc,
		SQLVariableSizeGlobalAllocator &varAlloc,
		util::AllocUniquePtr<TableSchemaInfo> &info,
		EstimationList &estimationList) {
	info = decodeTableSchemaInfo(in, alloc, varAlloc);
	decodeEstimationList(in, estimationList);
}

template
void SQLGetContainerHandler::decodeResult<EventByteInStream>(
		EventByteInStream &in, util::StackAllocator &alloc,
		SQLVariableSizeGlobalAllocator &varAlloc,
		util::AllocUniquePtr<TableSchemaInfo> &info,
		EstimationList &estimationList);

template<typename S>
void SQLGetContainerHandler::encodeTableSchemaInfo(
		S &out, SQLVariableSizeGlobalAllocator &varAlloc,
		TableSchemaInfo *info) {
	uint32_t bodySize = 0;

	const size_t headPos = out.base().position();
	encodeIntData(out, bodySize);

	const size_t bodyPos = out.base().position();
	try {
		util::ObjectCoder::withAllocator(varAlloc).encode(out, info);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_ERROR(
				e, GS_EXCEPTION_MERGE_MESSAGE(e, "Failed to encode message"));
	}
	const size_t endPos = out.base().position();

	out.base().position(headPos);
	bodySize = static_cast<uint32_t>(endPos - bodyPos);
	encodeIntData(out, bodySize);
	out.base().position(endPos);
}

template<typename S>
util::AllocUniquePtr<TableSchemaInfo>::ReturnType
SQLGetContainerHandler::decodeTableSchemaInfo(
		S &in, util::StackAllocator &alloc,
		SQLVariableSizeGlobalAllocator &varAlloc) {
	uint32_t bodySize;
	decodeIntData(in, bodySize);

	util::XArray<uint8_t> body(alloc);
	body.resize(bodySize);
	in >> std::make_pair(body.data(), bodySize);

	TableSchemaInfo *schemaInfoRef = NULL;
	try {
		util::ArrayByteInStream bodyStream(util::ArrayInStream(
				body.data(), body.size()));
		util::ObjectCoder::withAllocator(varAlloc).decode(
				bodyStream, schemaInfoRef);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(
			e, "Failed to decode schema message"));
	}

	util::AllocUniquePtr<TableSchemaInfo> schemaInfoPtr(
			schemaInfoRef, varAlloc);
	return schemaInfoPtr;
}

template<typename S>
void SQLGetContainerHandler::encodeEstimationList(
		S &out, const EstimationList &estimationList) {
	const uint32_t count = static_cast<uint32_t>(estimationList.size());
	encodeIntData(out, count);

	for (uint32_t i = 0; i < count; i++) {
		encodeLongData(out, estimationList[i]);
	}
}

template<typename S>
void SQLGetContainerHandler::decodeEstimationList(
		S &in, EstimationList &estimationList) {
	uint32_t count;
	decodeIntData(in, count);

	for (uint32_t i = 0; i < count; i++) {
		int64_t estimation;
		decodeLongData(in, estimation);

		estimationList.push_back(estimation);
	}
}

void SQLGetContainerHandler::estimateIndexSearchSize(
		EventContext &ec, Event &ev, InMessage &inMes) {
	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
	const PartitionId pId = ev.getPartitionId();

	Request &request = inMes.request_;

	try {
		checkTransactionTimeout(
				emNow,
				ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_,
				ev.getQueueingCount());

		checkExecutable(
				pId,
				(CROLE_MASTER | CROLE_FOLLOWER),
				(PROLE_OWNER | PROLE_BACKUP),
				PSTATE_ON,
				partitionTable_);

		util::LockGuard<util::Mutex> guard(
				partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext &txn = transactionManager_->put(
				alloc,
				request.fixed_.pId_,
				TXN_EMPTY_CLIENTID,
				request.fixed_.cxtSrc_,
				now,
				emNow);
		txn.setAuditInfo(&ev, &ec, NULL);

		const SearchList &searchList = inMes.searchList_;

		EstimationList estimationList(alloc);

		for (SearchList::const_iterator it = searchList.begin();
				it != searchList.end(); ++it) {
			const ContainerId containerId = it->first;
			BtreeMap::SearchContext *sc = it->second;

			KeyDataStoreValue keyStoreValue = getKeyDataStore(
					txn.getPartitionId())->get(alloc, containerId);

			DataStoreBase *ds = getDataStore(
					txn.getPartitionId(), keyStoreValue.storeType_);
			const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);

			DSInputMes input(
					alloc, DS_GET_CONTAINER_OBJECT, ANY_CONTAINER, true);
			StackAllocAutoPtr<DSContainerOutputMes> ret(
					alloc, static_cast<DSContainerOutputMes*>(
							ds->exec(&txn, &keyStoreValue, &input)));

			BaseContainer *container = ret.get()->container_;
			const int64_t estimation = (container == NULL ?
					-1 : container->estimateIndexSearchSize(txn, *sc));

			estimationList.push_back(estimation);
		}

		CompositeIndexInfos *compIndex = NULL;
		OutMessage outMes(
				request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
				getReplyOption(alloc, request, compIndex, false),
				NULL, sqlService_->getAllocator(), estimationList);

		replySuccess(
				ec, alloc, ev.getSenderND(), ev.getType(), request, outMes,
				false);
	}
	catch (std::exception &e) {
		try {
			handleError(ec, alloc, ev, request, e);
		}
		catch (std::exception &e2) {
			UTIL_TRACE_EXCEPTION(SQL_SERVICE, e2, "");
		}
	}
}

void SQLGetContainerHandler::checkAcceptableVersion(
		const StatementMessage::Request &request) {
	const int32_t acceptableVersion =
			request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>();
	if (acceptableVersion < LATEST_FEATURE_VERSION) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_MSG_VERSION_NOT_ACCEPTABLE,
				"SQL container access version unmatch ("
				"expected=" << LATEST_FEATURE_VERSION <<
				", actual=" << acceptableVersion << ")");
	}
}

SQLGetContainerHandler::SubCommand SQLGetContainerHandler::checkSubCommand(
		int32_t cmdNum) {
	switch (cmdNum) {
	case SUB_CMD_GET_CONTAINER:
	case SUB_CMD_ESTIMATE_INDEX_SEARCH_SIZE:
		break;
	default:
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_MSG_VERSION_NOT_ACCEPTABLE,
				"Unknown sub command (value=" << cmdNum << ")");
	}
	return static_cast<SubCommand>(cmdNum);
}

template<typename S>
void SQLGetContainerHandler::decodeContainerCondition(
		S &in, InMessage &msg, const ConnectionOption &connOption) {
	decodeVarSizeBinaryData<S>(in, msg.containerNameBinary_);

	decodeBooleanData<S>(in, msg.isContainerLock_);

	decodeUUID<S>(
		in, msg.clientId_.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);

	in >> msg.queryId_;

	if (connOption.clientVersion_ == TXN_V2_5_X_CLIENT_VERSION) {
		msg.request_.optional_.set<Options::DB_NAME>(GS_PUBLIC);
	}
}

template<typename S>
void SQLGetContainerHandler::decodeSearchList(
		TransactionContext &txn, S &in, SearchList &searchList) {
	uint32_t count;
	decodeIntData(in, count);

	for (uint32_t i = 0; i < count; i++) {
		SearchEntry entry;
		decodeSearchEntry(txn, in, entry);
		searchList.push_back(entry);
	}
}

template<typename S>
void SQLGetContainerHandler::decodeSearchEntry(
		TransactionContext &txn, S &in, SearchEntry &entry) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	util::Vector<ColumnId> columnIdList(alloc);
	BtreeMap::SearchContext *sc =
			ALLOC_NEW(alloc) BtreeMap::SearchContext(alloc, columnIdList);

	ContainerId containerId;
	decodeLongData(in, containerId);

	decodeColumnIdList(in, columnIdList);
	sc->setColumnIds(columnIdList);

	uint32_t count;
	decodeIntData(in, count);
	for (uint32_t i = 0; i < count; i++) {
		decodeTermCondition(txn, in, *sc, columnIdList);
	}

	entry = SearchEntry(containerId, sc);
}

template<typename S>
void SQLGetContainerHandler::decodeTermCondition(
		TransactionContext &txn, S &in, BtreeMap::SearchContext &sc,
		const util::Vector<ColumnId> &columnIdList) {
	assert(!columnIdList.empty());

	util::StackAllocator &alloc = txn.getDefaultAllocator();

	int32_t opType;
	ColumnType columnType;
	util::XArray<uint8_t> *valueData =
			ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);

	decodeIntData(in, opType);
	decodeIntData(in, columnType);
	decodeBinaryData(in, *valueData, false);

	const uint32_t valueSize = static_cast<uint32_t>(valueData->size());
	const bool opExtra = true;

	TermCondition *cond = ALLOC_NEW(alloc) TermCondition(
			columnType, columnType,
			static_cast<DSExpression::Operation>(opType),
			columnIdList.front(), valueData->data(), valueSize, opExtra);

	const bool forKey = true;
	sc.addCondition(txn, *cond, forKey);
}

template<typename S>
void SQLGetContainerHandler::decodeColumnIdList(
		S &in, util::Vector<ColumnId> &columnIdList) {
	uint32_t count;
	decodeIntData(in, count);

	for (uint32_t i = 0; i < count; i++) {
		ColumnId columnId;
		decodeIntData(in, columnId);
		columnIdList.push_back(columnId);
	}
}

/*!
	@brief コンストラクタ
	@param [in] config サーバコンフィグ
	@param [in] eeConfig イベントエンジンコンフィグ
	@param [in] eeSource イベントエンジンソース
	@param [in] name サービス名称
*/
SQLService::SQLService(
	ConfigTable& config, const EventEngine::Config& eeConfig,
	EventEngine::Source& eeSource, const char* name, PartitionTable* pt) :
	ee_(createEEConfig(config, eeConfig), eeSource, name), eeSource_(eeSource),
	globalVarAlloc_(NULL),
	localAlloc_(NULL),
	pgConfig_(KeyDataStore::MAX_PARTITION_NUM,
		config.get<int32_t>(CONFIG_TABLE_SQL_CONCURRENCY)),
	notifyClientInterval_(CommonUtility::changeTimeSecondToMilliSecond(config.get<int32_t>(
		CONFIG_TABLE_SQL_NOTIFICATION_INTERVAL))), executionManager_(NULL),
	txnSvc_(NULL), clsSvc_(NULL), txnMgr_(config, true),
	pt_(pt),
	clientSequenceNo_(0), nosqlSyncId_(0),
	totalMemoryLimitter_(resolveTotalMemoryLimitter()),
	workMemoryLimitter_(
			util::AllocatorInfo(ALLOCATOR_GROUP_SQL_WORK, "sqlWorkLimitter"),
			totalMemoryLimitter_),
	connectHandler_(SQL_CLIENT_VERSION,
		ACCEPTABLE_NEWSQL_CLIENT_VERSIONS),
	requestHandler_(workMemoryLimitter_),
	traceLimitTime_(config.get<int32_t>(
		CONFIG_TABLE_SQL_TRACE_LIMIT_EXECUTION_TIME) * 1000),
	traceLimitQuerySize_(config.get<int32_t>(
		CONFIG_TABLE_SQL_TRACE_LIMIT_QUERY_SIZE)),
	tableSchemaCacheSize_(config.get<int32_t>(
		CONFIG_TABLE_SQL_TABLE_CACHE_SIZE)),
	jobMemoryLimit_(ConfigTable::megaBytesToBytes(
		config.getUInt32(CONFIG_TABLE_SQL_JOB_TOTAL_MEMORY_LIMIT))),
	storeMemoryLimit_(ConfigTable::megaBytesToBytes(
			config.getUInt32(CONFIG_TABLE_SQL_STORE_MEMORY_LIMIT))),
	workMemoryLimit_(ConfigTable::megaBytesToBytes(config.getUInt32(
			CONFIG_TABLE_SQL_WORK_MEMORY_LIMIT))),
	totalMemoryLimit_(ConfigTable::megaBytesToBytes(
			config.getUInt32(CONFIG_TABLE_SQL_TOTAL_MEMORY_LIMIT))),
	workMemoryLimitRate_(doubleToBits(
			config.get<double>(CONFIG_TABLE_SQL_WORK_MEMORY_RATE))),
	failOnTotalMemoryLimit_(
			config.get<bool>(CONFIG_TABLE_SQL_FAIL_ON_TOTAL_MEMORY_LIMIT)),
	resourceControlLevel_(config.get<int32_t>(
			CONFIG_TABLE_SQL_RESOURCE_CONTROL_LEVEL)),
	isProfiler_(config.get<bool>(CONFIG_TABLE_SQL_ENABLE_PROFILER)),
	checkCounter_(config.get<int32_t>(
		CONFIG_TABLE_SQL_ENABLE_JOB_MEMORY_CHECK_INTERVAL_COUNT)),
	sendPendingInterval_(config.get<int32_t>(
		CONFIG_TABLE_SQL_SEND_PENDING_INTERVAL)),
	sendPendingTaskLimit_(config.get<int32_t>(
		CONFIG_TABLE_SQL_SEND_PENDING_TASK_LIMIT)),
	sendPendingJobLimit_(config.get<int32_t>(
		CONFIG_TABLE_SQL_SEND_PENDING_JOB_LIMIT)),
	sendPendingTaskConcurrency_(config.get<int32_t>(
		CONFIG_TABLE_SQL_SEND_PENDING_TASK_CONCURRENCY)),
	addBatchMaxCount_(config.get<int32_t>(
		CONFIG_TABLE_SQL_ADD_BATCH_MAX_COUNT)),
	tablePartitioningMaxAssignedNum_(config.get<int32_t>(
		CONFIG_TABLE_SQL_TABLE_PARTITIONING_MAX_ASSIGN_NUM)),
	tablePartitioningMaxAssignedEntryNum_(config.get<int32_t>(
		CONFIG_TABLE_SQL_TABLE_PARTITIONING_MAX_ASSIGN_ENTRY_NUM)),
	totalInternalConnectionCount_(0),
	totalExternalConnectionCount_(0),
	eventMonitor_(pgConfig_)
{
	try {

		setNoSQLFailoverTimeout(config.get<int32_t>(
			CONFIG_TABLE_SQL_NOSQL_FAILOVER_TIMEOUT));
		setTableSchemaExpiredTime(config.get<int32_t>(
			CONFIG_TABLE_SQL_TABLE_CACHE_EXPIRED_TIME));
		setScanMetaTableByAdmin(config.get<bool>(
			CONFIG_TABLE_SQL_ENABLE_SCAN_META_TABLE_ADMINISTRATOR_PRIVILEGES));

		ee_.setHandler(CONNECT, connectHandler_);
		ee_.setHandlingMode(CONNECT, EventEngine::HANDLING_IMMEDIATE);
		ee_.setHandler(DISCONNECT, disconnectHandler_);
		ee_.setHandlingMode(DISCONNECT, EventEngine::HANDLING_IMMEDIATE);
		ee_.setHandler(LOGIN, loginHandler_);
		ee_.setHandler(GET_PARTITION_ADDRESS, getConnectionAddressHandler_);
		ee_.setHandler(SQL_EXECUTE_QUERY, requestHandler_);
		ee_.setHandler(SQL_CANCEL_QUERY, cancelHandler_);
		ee_.setHandlingMode(SQL_CANCEL_QUERY, EventEngine::HANDLING_IMMEDIATE);
		ee_.setHandler(SQL_RECV_SYNC_REQUEST, nosqlSyncReceiveHandler_);
		ee_.setHandlingMode(SQL_RECV_SYNC_REQUEST,
			EventEngine::HANDLING_IMMEDIATE);
		ee_.setDisconnectHandler(socketDisconnectHandler_);
		ee_.setHandler(SQL_NOTIFY_CLIENT, ignorableStatementHandler_);
		ee_.setHandlingMode(SQL_NOTIFY_CLIENT,
			EventEngine::HANDLING_IMMEDIATE);
		ee_.setUnknownEventHandler(unknownStatementHandler_);
		ee_.setThreadErrorHandler(serviceThreadErrorHandler_);
		ee_.setUserDataType<StatementHandler::ConnectionOption>();
		ee_.setHandler(AUTHENTICATION_ACK, authenticationAckHandler_);

		ee_.setHandler(TXN_COLLECT_TIMEOUT_RESOURCE, checkTimeoutHandler_);
		ee_.setHandlingMode(TXN_COLLECT_TIMEOUT_RESOURCE,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		ee_.setHandler(EXECUTE_JOB, executeHandler_);
		ee_.setHandler(CONTROL_JOB, controlHandler_);
		ee_.setHandlingMode(CONTROL_JOB,
			EventEngine::HANDLING_IMMEDIATE);
		ee_.setHandler(CHECK_TIMEOUT_JOB, resourceCheckHandler_);
		ee_.setHandler(REQUEST_CANCEL, resourceCheckHandler_);
		ee_.setHandlingMode(CHECK_TIMEOUT_JOB,
			EventEngine::HANDLING_IMMEDIATE);
		ee_.setHandlingMode(REQUEST_CANCEL,
			EventEngine::HANDLING_IMMEDIATE);

		ee_.setHandler(DISPATCH_JOB, dispatchJobHandler_);
		ee_.setHandlingMode(DISPATCH_JOB,
			EventEngine::HANDLING_IMMEDIATE);

		ee_.setHandler(UPDATE_TABLE_CACHE, updateTableCacheHandler_);
		ee_.setHandlingMode(UPDATE_TABLE_CACHE,
			EventEngine::HANDLING_IMMEDIATE);
		ee_.setHandler(SQL_REQUEST_NOSQL_CLIENT,
			requestNoSQLClientHandler_);

		ee_.setHandler(SEND_EVENT, sendEventHandler_);
		ee_.setHandlingMode(SEND_EVENT,
			EventEngine::HANDLING_IMMEDIATE);

		globalVarAlloc_ = UTIL_NEW SQLVariableSizeGlobalAllocator(
			util::AllocatorInfo(ALLOCATOR_GROUP_SQL_WORK, "sql common"));

		ConfigTable* tmpTable = const_cast<ConfigTable*>(&config);
		config_.setUpConfigHandler(this, *tmpTable);

		setUpMemoryLimits();

	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "SQL service initialize failed");
	}
}

/*!
	@brief デストラクタ
*/
SQLService::~SQLService() {
	ee_.shutdown();
	ee_.waitForShutdown();

	for (int32_t i = 0; i < getConcurrency(); i++) {
		if (localAlloc_ && localAlloc_[i]) delete localAlloc_[i];
	}
	if (localAlloc_) {
		delete[] localAlloc_;
	}
	if (globalVarAlloc_) {
		delete globalVarAlloc_;
	}
}

/*!
	@brief EventEngineコンフィグセット
	@param [in] config サーバコンフィグ
	@param [in] eeConfig EventEngineコンフィグ
	@return Config EEコンフィグ
*/
EventEngine::Config SQLService::createEEConfig(
	const ConfigTable& config, const EventEngine::Config& src) {

	EventEngine::Config eeConfig = src;
	eeConfig.setServerNDAutoNumbering(false);
	eeConfig.setClientNDEnabled(true);
	eeConfig.setConcurrency(
		config.get<int32_t>(CONFIG_TABLE_SQL_CONCURRENCY));
	eeConfig.setPartitionCount(KeyDataStore::MAX_PARTITION_NUM);

	ClusterAdditionalServiceConfig addConfig(config);
	const char* targetAddress = addConfig.getServiceAddress(SQL_SERVICE);
	eeConfig.setServerAddress(targetAddress,
		config.getUInt16(CONFIG_TABLE_SQL_SERVICE_PORT));

	if (ClusterService::isMulticastMode(config)) {
		eeConfig.setMulticastAddress(
			config.get<const char8_t*>(CONFIG_TABLE_SQL_NOTIFICATION_ADDRESS),
			config.getUInt16(CONFIG_TABLE_SQL_NOTIFICATION_PORT));
		if (strlen(config.get<const char8_t*>(
			CONFIG_TABLE_SQL_NOTIFICATION_INTERFACE_ADDRESS)) != 0) {
			eeConfig.setMulticastInterfaceAddress(
				config.get<const char8_t*>(
					CONFIG_TABLE_SQL_NOTIFICATION_INTERFACE_ADDRESS),
				config.getUInt16(CONFIG_TABLE_SQL_SERVICE_PORT));
		}
	}

	eeConfig.connectionCountLimit_ =
		config.get<int32_t>(CONFIG_TABLE_SQL_CONNECTION_LIMIT);
	eeConfig.keepaliveEnabled_ =
		config.get<bool>(CONFIG_TABLE_SQL_USE_KEEPALIVE);
	eeConfig.keepaliveIdle_ =
		config.get<int32_t>(CONFIG_TABLE_SQL_KEEPALIVE_IDLE);
	eeConfig.keepaliveCount_ =
		config.get<int32_t>(CONFIG_TABLE_SQL_KEEPALIVE_COUNT);
	eeConfig.keepaliveInterval_ =
		config.get<int32_t>(CONFIG_TABLE_SQL_KEEPALIVE_INTERVAL);

	const size_t cacheSizeBase = ConfigTable::megaBytesToBytes(
			config.getUInt32(CONFIG_TABLE_SQL_TOTAL_MEMORY_LIMIT));
	if (resolveResourceControlLevel() > 1 || cacheSizeBase == 0) {
		eeConfig.eventBufferSizeLimit_ = ConfigTable::megaBytesToBytes(
			config.getUInt32(CONFIG_TABLE_SQL_EVENT_BUFFER_CACHE_SIZE));
	}
	else {
		eeConfig.eventBufferSizeLimit_ = cacheSizeBase;
	}

	eeConfig.setAllAllocatorGroup(ALLOCATOR_GROUP_SQL_MESSAGE);
	eeConfig.workAllocatorGroupId_ = ALLOCATOR_GROUP_SQL_WORK;

	return eeConfig;
}

/*!
	@brief SQLサービスを初期化して、GS共通情報をセットする
	@param [in] mgrSet GS共通情報
*/
void SQLService::initialize(const ManagerSet& mgrSet) {
	try {
		serviceThreadErrorHandler_.initialize(mgrSet);
		connectHandler_.initialize(mgrSet, true);
		disconnectHandler_.initialize(mgrSet, true);
		loginHandler_.initialize(mgrSet, true);
		getConnectionAddressHandler_.initialize(mgrSet);
		timerNotifyClientHandler_.initialize(mgrSet);
		requestHandler_.initialize(mgrSet);
		cancelHandler_.initialize(mgrSet);
		nosqlSyncReceiveHandler_.initialize(mgrSet);
		socketDisconnectHandler_.initialize(mgrSet);

		authenticationAckHandler_.initialize(mgrSet, true);
		checkTimeoutHandler_.initialize(mgrSet, true);
		int64_t checkSQLAuthInterval = 60 * 5;
		for (int32_t pgId = 0;
				static_cast<uint32_t>(pgId) < pgConfig_.getPartitionGroupCount();
				pgId++) {
			const PartitionId beginPId =
				pgConfig_.getGroupBeginPartitionId(pgId);
			Event timeoutCheckEvent(
				eeSource_, TXN_COLLECT_TIMEOUT_RESOURCE, beginPId);
			ee_.addPeriodicTimer(
				timeoutCheckEvent,
				static_cast<int32_t>(checkSQLAuthInterval * 1000));
		}

		executeHandler_.initialize(mgrSet);
		controlHandler_.initialize(mgrSet);
		resourceCheckHandler_.initialize(mgrSet);
		dispatchJobHandler_.initialize(mgrSet);
		updateTableCacheHandler_.initialize(mgrSet);
		requestNoSQLClientHandler_.initialize(mgrSet);
		sendEventHandler_.initialize(mgrSet);

		unknownStatementHandler_.initialize(mgrSet);
		ignorableStatementHandler_.initialize(mgrSet);

		clsSvc_ = mgrSet.clsSvc_;
		txnSvc_ = mgrSet.txnSvc_;
		executionManager_ = mgrSet.execMgr_;
		int32_t concurrency = getConcurrency();
		localAlloc_ = UTIL_NEW SQLVariableSizeLocalAllocator * [concurrency];

		txnMgr_.initialize(mgrSet);

		statusList_.assign(concurrency, 0);
		for (int32_t i = 0; i < concurrency; i++) {
			localAlloc_[i] = UTIL_NEW SQLVariableSizeLocalAllocator(util::AllocatorInfo(
				ALLOCATOR_GROUP_SQL_WORK, "localAlloc"));
		}
		startPIdList_.assign(concurrency, 0);
		for (int32_t pgId = 0; pgId < concurrency; pgId++) {
			startPIdList_[pgId] = pgConfig_.getGroupBeginPartitionId(pgId);
		}
		PartitionId partitionNum = KeyDataStore::MAX_PARTITION_NUM;
		pgIdList_.assign(partitionNum, 0);
		for (int32_t pId = 0; pId < static_cast<int32_t>(partitionNum); pId++) {
			pgIdList_[pId] = pgConfig_.getPartitionGroupId(pId);
		}
		statUpdator_.service_ = this;
		mgrSet.stats_->addUpdator(&statUpdator_);

	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "SQL service initialize failed");
	}
}

/*!
	@brief SQLサービスを開始する
*/
void SQLService::start() {
	try {
		ee_.start();
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "SQL service start failed");
	}
}

/*!
	@brief SQLサービスを停止要求する
*/
void SQLService::shutdown() {
	ee_.shutdown();
}

/*!
	@brief SQLサービスの停止を待ち合わせる
*/
void SQLService::waitForShutdown() {
	ee_.waitForShutdown();
}

/*!
	@brief ハンドラ情報を初期化する
	@param [in] mgrSet GS共通情報
*/
void SQLServiceHandler::initialize(const ManagerSet& mgrSet) {
	try {
		StatementHandler::initialize(mgrSet);
		sqlSvc_ = mgrSet.sqlSvc_;
		clsSvc_ = mgrSet.clsSvc_;
		txnSvc_ = mgrSet.txnSvc_;
		sysSvc_ = mgrSet.sysSvc_;
		pt_ = mgrSet.pt_;
		globalVarAlloc_ = sqlSvc_->getAllocator();
		localAlloc_ = sqlSvc_->getLocalAllocator();
		transactionManager_ = sqlService_->getTransactionManager();
		executionManager_ = mgrSet.execMgr_;
		jobManager_ = mgrSet.jobMgr_;
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "SQL service handler initialize failed");
	}
};

/*!
	@brief SQLサービス通知ハンドラ
	@param [in] ec イベントコンテキスト
	@param [in] ev イベント
*/
void SQLTimerNotifyClientHandler::operator ()(EventContext& ec, Event& ev) {
	UNUSED_VARIABLE(ev);

	if (clsSvc_->getNotificationManager().getMode()
		!= NOTIFICATION_MULTICAST) {
		return;
	}

	util::StackAllocator& alloc = ec.getAllocator();
	util::StackAllocator::Scope scope(alloc);

	try {
		if (!pt_->isMaster()) {
			return;
		}
		clsMgr_->checkNodeStatus();

		Event notifySqlClientEvent(ec, SQL_NOTIFY_CLIENT,
			CS_HANDLER_PARTITION_ID);
		EventByteOutStream out = notifySqlClientEvent.getOutStream();
		encode(out, ec);

		const NodeDescriptor& multicastND = sqlSvc_->getEE()->getMulticastND();

		if (!multicastND.isEmpty()) {
			notifySqlClientEvent.setPartitionIdSpecified(false);
			sqlSvc_->getEE()->send(notifySqlClientEvent, multicastND);
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_INVALID_SENDER_ND,
				"Multicast send failed");
		}
	}
	catch (EncodeDecodeException& e) {
		UTIL_TRACE_EXCEPTION(SQL_SERVICE, e, "");
	}
	catch (std::exception& e) {
		UTIL_TRACE_EXCEPTION(SQL_SERVICE, e, "");
		if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
			clsSvc_->setError(ec, &e);
		}
	}
}

/*!
	@brief SQLサービス通知ハンドラエンコード関数
	@param [in] out イベントストリーム
	@param [in] ec イベントコンテキスト

	- フォーマット <address, port, partitionNum, hashType>
	- パーティション数は特に使わない
*/
void SQLTimerNotifyClientHandler::encode(
		EventByteOutStream& out, EventContext& ec) {
	UNUSED_VARIABLE(ec);

	try {
		const uint8_t hashType = 0;
		util::SocketAddress::Inet address;
		uint16_t port;

		const NodeDescriptor& nd = sqlSvc_->getEE()->getSelfServerND();
		if (nd.isEmpty()) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_INVALID_SENDER_ND,
				"Invalid Node descriptor");
		}

		nd.getAddress().getIP(&address, &port);
		out.writeAll(&address, sizeof(address));
		out << static_cast<uint32_t>(port);
		out << static_cast<PartitionId>(KeyDataStore::MAX_PARTITION_NUM);
		out << hashType;
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, GS_EXCEPTION_MERGE_MESSAGE(e,
			"SQL client notification encode failed"));
	}
}

/*!
	@brief SQLコネクションの接続アドレス(スレッド番号)決定ハンドラ
	@param [in] ec イベントコンテキスト
	@param [in] ev イベント
	@note NOSQLのクライアントからコール
	@note 実行:SQLサービス
	@note 入力:通常クライアントメッセージ
	@note 出力:通常クライアントメッセージ
*/
void SQLGetConnectionAddressHandler::operator ()(EventContext& ec, Event& ev) {

	util::StackAllocator& alloc = ec.getAllocator();
	util::StackAllocator::Scope scope(alloc);
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
	Request request(alloc, getRequestSource(ev));
	Response response(alloc);

	try {
		ConnectionOption& connOption =
			ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);

		bool masterResolving = false;
		if (in.base().remaining() > 0) {
			decodeBooleanData(in, masterResolving);
		}
		bool usePublic = usePublicConection(request);

		const ClusterRole clusterRole =
			CROLE_MASTER | (masterResolving ? CROLE_FOLLOWER : 0);

		checkAuthentication(ev.getSenderND(), emNow);
		checkConsistency(ev.getSenderND(), ANY_CONSISTENCY);
		checkExecutable(clusterRole);

		util::XArrayOutStream<> arrayOut(response.binaryData_);
		util::ByteStream< util::XArrayOutStream<> > out(arrayOut);

		out << static_cast<PartitionId>(KeyDataStore::MAX_PARTITION_NUM);

		util::XArray<NodeId> liveNodeList(alloc);
		partitionTable_->getLiveNodeIdList(liveNodeList);

		assert(liveNodeList.size() > 0);
		uint32_t targetNodePos;
		if (!connOption.connected_) {
			targetNodePos = static_cast<uint32_t>(
				sqlSvc_->getClientSequenceNo() % static_cast<int32_t>(liveNodeList.size()));
			connOption.connected_ = true;
		}
		else {
			targetNodePos = static_cast<uint32_t>(
				sqlSvc_->incClientSequenceNo() % static_cast<int32_t>(liveNodeList.size()));
		}

		if (masterResolving) {
			out << static_cast<uint8_t>(0);
		}
		else {
			NodeAddress& address = partitionTable_->getNodeAddress(
				liveNodeList[targetNodePos], SQL_SERVICE, usePublic);
			out << static_cast<uint8_t>(1);
			out << std::pair<const uint8_t*, size_t>(
				reinterpret_cast<const uint8_t*>(&address.address_),
				sizeof(AddressType));
			out << static_cast<uint32_t>(address.port_);
		}


		out << static_cast<uint8_t>(0);

		if (masterResolving) {
			GetPartitionAddressHandler::encodeClusterInfo(
				out, ec.getEngine(), *partitionTable_,
				CONTAINER_HASH_MODE_CRC32, SQL_SERVICE, usePublic);
		}

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(), TXN_STATEMENT_SUCCESS,
			request, response, NO_REPLICATION);
	}
	catch (std::exception& e) {
		replyError(ev.getType(), ev.getPartitionId(),
			ec, ev.getSenderND(), request.fixed_.cxtSrc_.stmtId_, e);
	}
}

SQLRequestHandler::SQLRequestHandler(
		util::AllocatorLimitter &workMemoryLimitter) :
		workMemoryLimitter_(workMemoryLimitter) {
}

#include "nosql_command.h"
/*!
	@brief リクエストハンドラ
	@param [in] ec イベントコンテキスト
	@param [in] ev イベント
*/
void SQLRequestHandler::operator ()(EventContext& ec, Event& ev) {

	util::StackAllocator& alloc = ec.getAllocator();
	util::StackAllocator::Scope scope(alloc);
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	RequestInfo request(alloc, ev.getPartitionId(), ec.getWorkerId());
	request.eventType_ = ev.getType();

	try {

		EventByteInStream in(ev.getInStream());
		decode(in, request);

		ConnectionOption& connOption = ev.getSenderND().getUserData<ConnectionOption>();
		checkAuthentication(ev.getSenderND(), emNow);

		const StatementHandler::ClusterRole
			clusterRole = (StatementHandler::CROLE_MASTER | StatementHandler::CROLE_FOLLOWER);
		StatementHandler::checkExecutable(clusterRole, pt_);

		if (request.closeQueryId_ != UNDEF_SESSIONID) {
			ClientId closeClientId(request.clientId_.uuid_, request.closeQueryId_);
			SQLExecutionManager::Latch latch(closeClientId, executionManager_);
			SQLExecution* execution = latch.get();
			if (execution != NULL) {
				executionManager_->remove(ec, closeClientId, false, 58, NULL);
			}
		}

		executionManager_->setRequestedConnectionEnv(request, connOption);

		util::AllocatorLimitter::Scope limitterScope(
				workMemoryLimitter_, alloc);
		util::AllocatorLimitter::Scope varLimitterScope(
				workMemoryLimitter_, ec.getVariableSizeAllocator());

		switch (request.requestType_) {
		case REQUEST_TYPE_QUERY:
		case REQUEST_TYPE_PRAGMA: 
		case REQUEST_TYPE_EXECUTE:
		case REQUEST_TYPE_UPDATE: {
			if (request.isBind()) {
				if (request.retrying_ && request.sqlString_.size() > 0) {
					SQLExecutionManager::Latch latch(request.clientId_,
						executionManager_, &ev.getSenderND(), &request);
					SQLExecution* execution = latch.get();
					if (execution == NULL) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_CANCELLED,
							"Cancel SQL, clientId=" << request.clientId_ << ", location=clientRetry");
					}
					execution->execute(ec, request, true, NULL, UNDEF_JOB_VERSIONID, NULL);
				}
				else {
					SQLExecutionManager::Latch latch(request.clientId_, executionManager_);
					SQLExecution* execution = latch.get();
					if (execution == NULL) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_CANCELLED,
							"Cancel SQL, clientId=" << request.clientId_ << ", location=Bind");
					}
					execution->execute(ec, request, false, NULL, UNDEF_JOB_VERSIONID, NULL);
				}
			}
			else {
				SQLExecutionManager::Latch latch(request.clientId_,
					executionManager_, &ev.getSenderND(), &request);
				SQLExecution* execution = latch.get();
				if (execution == NULL) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_CANCELLED,
						"Cancel SQL, clientId=" << request.clientId_ << ", location=Query");
				}
				execution->execute(ec, request, true, NULL, UNDEF_JOB_VERSIONID, NULL);
			}
		}
								break;
		case REQUEST_TYPE_PREPARE: {
			SQLExecutionManager::Latch latch(request.clientId_,
				executionManager_, &ev.getSenderND(), &request);
			SQLExecution* execution = latch.get();
			if (execution == NULL) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_CANCELLED,
					"Cancel SQL, clientId=" << request.clientId_ << ", location=Prepared statement");
			}
			execution->execute(ec, request, false, NULL, UNDEF_JOB_VERSIONID, NULL);
		}
								 break;
		case REQUEST_TYPE_FETCH: {
			if (request.retrying_) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_CLIENT_FAILOVER_FAILED,
					"Failover fetch operation is not supported");
			}
			SQLExecutionManager::Latch latch(request.clientId_, executionManager_);
			SQLExecution* execution = latch.get();
			if (execution == NULL) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_CANCELLED,
					"Cancel SQL, clientId=" << request.clientId_ << ", location=fetch");
			}
			execution->fetch(ec, ev, request);
		}
							   break;
		case REQUEST_TYPE_CLOSE: {
			SQLExecutionManager::Latch latch(request.clientId_, executionManager_);
			SQLExecution* execution = latch.get();
			if (execution == NULL) {

				Event replyEv(ec, SQL_EXECUTE_QUERY, 0);
				replyEv.setPartitionIdSpecified(false);
				EventByteOutStream out = replyEv.getOutStream();

				out << request.stmtId_;
				out << TXN_STATEMENT_SUCCESS;

				encodeBooleanData(out, request.transactionStarted_);
				encodeBooleanData(out, request.isAutoCommit_);
				encodeIntData<EventByteOutStream, int32_t>(out, 0);
				encodeEnvList(out, alloc, *executionManager_, connOption);
				ec.getEngine().send(replyEv, ev.getSenderND());

				return;
			}
			JobId jobId;
			execution->getContext().getCurrentJobId(jobId);
			jobManager_->cancel(ec, jobId, false);
			execution->close(ec, request);
			executionManager_->remove(ec, request.clientId_, false, 59, NULL);
		}
							   break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_SQL_OPTYPE_UNSUPPORTED,
				"Invalid requestType = " << static_cast<int32_t>(request.requestType_));
		}
	}
	catch (std::exception& e) {

		try {
			SQLExecutionManager::Latch latch(request.clientId_, executionManager_);
			SQLExecution* execution = latch.get();
			if (execution) {
				execution->setRequest(request);
				execution->execute(ec, request, true, &e, UNDEF_JOB_VERSIONID, NULL);
			}
			else {
				replyError(ev.getType(),
					ev.getPartitionId(), ec,
					ev.getSenderND(), request.stmtId_, e);
			}
		}
		catch (std::exception& e2) {
			replyError(ev.getType(),
				ev.getPartitionId(), ec,
				ev.getSenderND(), request.stmtId_, e);
		}
	}
}

void SQLServiceHandler::replyError(EventType eventType,
	PartitionId pId, EventContext& ec, const NodeDescriptor& nd,
	StatementId stmtId, const std::exception& e) {

	EventRequestOption sendOption;
	StatementExecStatus status = TXN_STATEMENT_ERROR;

	try {
		const util::Exception checkException = GS_EXCEPTION_CONVERT(e, "");
		int32_t errorCode = checkException.getErrorCode();
		if (isDenyException(errorCode)) {
			status = TXN_STATEMENT_DENY;
		}
		Event replyEv(ec, eventType, pId);
		setErrorReply(replyEv, stmtId, status, e, nd);
		if (!nd.isEmpty()) {
			ec.getEngine().send(replyEv, nd, &sendOption);
		}
	}
	catch (std::exception& e) {
		UTIL_TRACE_EXCEPTION_WARNING(SQL_SERVICE, e, "");
	}
}

/*!
	@brief クライアントリクエストのデコード
	@param [in] in 入力ストリーム
	@param [out] request リクエスト情報
*/
void SQLRequestHandler::decode(util::ByteStream<util::ArrayInStream>& in,
	RequestInfo& request) {

	try {
		decodeLongData<util::ByteStream<util::ArrayInStream>, StatementId>(in, request.stmtId_);
		decodeUUID<util::ByteStream<util::ArrayInStream>>(in, request.clientId_.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
		decodeEnumData<util::ByteStream<util::ArrayInStream>, SQLGetMode>(in, request.sessionMode_);
		int32_t tmpStatement;
		decodeIntData<util::ByteStream<util::ArrayInStream>, int32_t>(in, tmpStatement);
		request.requestType_ = static_cast<SQLRequestType>(tmpStatement);

		decodeLongData<util::ByteStream<util::ArrayInStream>, SessionId>(in, request.clientId_.sessionId_);
		decodeLongData<util::ByteStream<util::ArrayInStream>, SessionId>(in, request.closeQueryId_);
		decodeBooleanData(in, request.transactionStarted_);
		decodeBooleanData(in, request.retrying_);

		decodeBindInfo(in, request);

		decodeOptionPart(in, request.option_);
		setDefaultOptionValue(request.option_);
		decodeIntData<util::ByteStream<util::ArrayInStream>, int32_t>(in, request.sqlCount_);

		util::String tmpString(request.eventStackAlloc_);
		for (int32_t pos = 0; pos < request.sqlCount_; pos++) {
			decodeStringData<util::ByteStream<util::ArrayInStream>, util::String>(in, tmpString);
			request.sqlString_ += tmpString;
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "Client request decode failed, reason="
			<< GS_EXCEPTION_MESSAGE(e));
	}
}

/*!
	@brief バインド情報のデコード
	@param [in] in 入力ストリーム
	@param [out] request リクエスト情報
*/
void SQLRequestHandler::decodeBindInfo(util::ByteStream<util::ArrayInStream>& in,
	RequestInfo& request) {

	try {
		size_t startPos = in.base().position();
		decodeBooleanData(in, request.inTableExists_);
		if (request.inTableExists_) {

			int64_t rowCount = 0;
			in >> rowCount;
			decodeBinaryData<util::ByteStream<util::ArrayInStream>>(in, request.inTableSchema_, false);
			InStream fixedSchemaIn(util::ArrayInStream(request.inTableSchema_.data(),
				request.inTableSchema_.size()));
			int32_t columnCount;
			fixedSchemaIn >> columnCount;
			decodeBinaryData<util::ByteStream<util::ArrayInStream>>(in, request.inTableData_, false);

			if (columnCount > 0) {
				const char* fixedPartData
					= reinterpret_cast<const char*>(request.inTableData_.data());
				size_t fixedPartDataSize = request.inTableData_.size();
				int32_t nullsBytes = ValueProcessor::calcNullsByteSize(columnCount);
				int32_t columnDataSize = sizeof(uint8_t)/* type */ + sizeof(uint64_t); 

				InStream fixedPartIn(util::ArrayInStream(fixedPartData, fixedPartDataSize));
				uint64_t headerPos;
				fixedPartIn >> headerPos;

				size_t varPos = sizeof(uint64_t) + (sizeof(uint64_t) + nullsBytes + columnDataSize * columnCount) * rowCount;

				InStream varPartIn(util::ArrayInStream(fixedPartData, fixedPartDataSize));

				request.bindParamSet_.prepare(columnCount, rowCount);
				for (int64_t i = 0; i < rowCount; i++) {
					uint64_t baseVarOffset;
					fixedPartIn >> baseVarOffset;

					fixedPartIn.base().position(
						fixedPartIn.base().position() + nullsBytes);

					varPartIn.base().position(varPos + baseVarOffset);

					ValueProcessor::getVarSize(varPartIn);

					for (int32_t columnNo = 0; columnNo < columnCount; columnNo++) {
						decodeBindColumnInfo(in, request, fixedPartIn, varPartIn);
					}
					request.bindParamSet_.next();
				}
			}
			else {
				request.bindParamSet_.prepare(columnCount, rowCount);
			}
			size_t endPos = in.base().position();
			if (request.serialized_) {
				size_t serializedDataSize = endPos - startPos;
				request.serializedBindInfo_.resize(serializedDataSize);
				in.base().position(startPos);
				in >> std::make_pair(request.serializedBindInfo_.data(), serializedDataSize);
				in.base().position(endPos);
			}
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief バインド変数のデコード
	@param [in] in 入力ストリーム
	@param [out] request リクエスト情報
	@param [in] fixedPartIn 固定長ストリーム
	@param [in] varPartIn 可変長ストリーム
*/
void SQLRequestHandler::decodeBindColumnInfo(
		util::ByteStream<util::ArrayInStream>& in,
		RequestInfo& request, InStream& fixedPartIn,
		InStream& varPartIn) {
	UNUSED_VARIABLE(in);

	try {
		int64_t numValue;
		int32_t size;
		char8_t* data;

		int8_t typeOrdinal;
		fixedPartIn >> typeOrdinal;

		const bool forArray = false;
		const bool withAny = true;
		ColumnType type;
		if (!ValueProcessor::findColumnTypeByPrimitiveOrdinal(
				typeOrdinal, forArray, withAny, type)) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_VALUETYPE_UNSUPPORTED, "");
		}
		BindParam* param = ALLOC_NEW(request.eventStackAlloc_) BindParam(request.eventStackAlloc_, type);

		switch (type) {
		case COLUMN_TYPE_BOOL: {
			fixedPartIn >> numValue;
			param->value_ = TupleValue(reinterpret_cast<const char*>(
				&numValue), TupleList::TYPE_BOOL);
		}
							 break;
		case COLUMN_TYPE_BYTE: {
			fixedPartIn >> numValue;
			param->value_ = TupleValue(reinterpret_cast<const char*>(
				&numValue), TupleList::TYPE_BYTE);
		}
							 break;
		case COLUMN_TYPE_SHORT: {
			fixedPartIn >> numValue;
			param->value_ = TupleValue(reinterpret_cast<const char*>(
				&numValue), TupleList::TYPE_SHORT);
		}
							  break;
		case COLUMN_TYPE_INT: {
			fixedPartIn >> numValue;
			param->value_ = TupleValue(reinterpret_cast<const char*>(
				&numValue), TupleList::TYPE_INTEGER);
		}
							break;
		case COLUMN_TYPE_LONG: {
			fixedPartIn >> numValue;
			param->value_ = TupleValue(reinterpret_cast<const char*>(
				&numValue), TupleList::TYPE_LONG);
		}
							 break;
		case COLUMN_TYPE_TIMESTAMP: {
			fixedPartIn >> numValue;
			param->value_ = TupleValue(reinterpret_cast<const char*>(
				&numValue), TupleList::TYPE_TIMESTAMP);
			break;
		}
		case COLUMN_TYPE_MICRO_TIMESTAMP: {
			fixedPartIn >> numValue;
			param->value_ = TupleValue(reinterpret_cast<const char*>(
				&numValue), TupleList::TYPE_MICRO_TIMESTAMP);
			break;
		}
		case COLUMN_TYPE_NANO_TIMESTAMP: {
			int64_t padding;
			fixedPartIn >> padding;
			NanoTimestamp tsValue;
			varPartIn.readAll(&tsValue, sizeof(tsValue));
			param->value_ = SyntaxTree::makeNanoTimestampValue(
					request.eventStackAlloc_, tsValue);
			break;
		}
		case COLUMN_TYPE_NULL: {
			fixedPartIn >> numValue;
			param->value_ = TupleValue(&SyntaxTree::NULL_VALUE_RAW_DATA,
				TupleList::TYPE_ANY);
		}
							 break;
		case COLUMN_TYPE_FLOAT: {
			float floatValue;
			fixedPartIn >> floatValue;
			int32_t dummy;
			fixedPartIn >> dummy;
			param->value_ = TupleValue(reinterpret_cast<const char*>(
				&floatValue), TupleList::TYPE_FLOAT);
		}
							  break;
		case COLUMN_TYPE_DOUBLE: {
			double doubleValue;
			fixedPartIn >> doubleValue;
			param->value_ = TupleValue(reinterpret_cast<const char*>(
				&doubleValue), TupleList::TYPE_DOUBLE);
		}
							   break;
		case COLUMN_TYPE_STRING:
		case COLUMN_TYPE_GEOMETRY:
		case COLUMN_TYPE_BLOB: {
			TupleList::TupleColumnType tupleType = convertNoSQLTypeToTupleType(type);
			int64_t fixedOffset;
			fixedPartIn >> fixedOffset;
			uint32_t varColumnDataSize = ValueProcessor::getVarSize(varPartIn);
			data = static_cast<char*>(
				request.eventStackAlloc_.allocate(varColumnDataSize));
			varPartIn.readAll(data, varColumnDataSize);
			size = static_cast<int32_t>(varColumnDataSize);

			switch (tupleType) {
			case TupleList::TYPE_STRING: {
				param->value_ = SyntaxTree::makeStringValue(
					request.eventStackAlloc_, data, size);
			}
									   break;
			case TupleList::TYPE_GEOMETRY:
			case TupleList::TYPE_BLOB: {
				TupleValue::VarContext cxt;
				cxt.setStackAllocator(&request.eventStackAlloc_);
				param->value_ = SyntaxTree::makeBlobValue(
					cxt, data, size);
			}
									 break;
			default:
				GS_THROW_USER_ERROR(GS_ERROR_SQL_VALUETYPE_UNSUPPORTED, "");
			}
		}
							 break;
		case COLUMN_TYPE_STRING_ARRAY:
		case COLUMN_TYPE_BOOL_ARRAY:
		case COLUMN_TYPE_BYTE_ARRAY:
		case COLUMN_TYPE_SHORT_ARRAY:
		case COLUMN_TYPE_INT_ARRAY:
		case COLUMN_TYPE_LONG_ARRAY:
		case COLUMN_TYPE_FLOAT_ARRAY:
		case COLUMN_TYPE_DOUBLE_ARRAY:
		case COLUMN_TYPE_TIMESTAMP_ARRAY: {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_VALUETYPE_UNSUPPORTED,
				"Unsupported bind type = " << static_cast<int32_t>(type));
		}
		default:
			GS_THROW_USER_ERROR(GS_ERROR_SQL_INTERNAL,
				"Invalid bind type = " << static_cast<int32_t>(type));
		}
		request.bindParamSet_.append(param);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief デフォルトオプションをセットする
	@param [in, out] option オプション
*/
void SQLRequestHandler::setDefaultOptionValue(OptionSet& optionSet) {
	if (optionSet.get<Options::STATEMENT_TIMEOUT_INTERVAL>() == 0) {
		const int32_t value = SQL_DEFAULT_QUERY_TIMEOUT_INTERVAL;
		optionSet.set<Options::STATEMENT_TIMEOUT_INTERVAL>(value);
		optionSet.set<Options::TXN_TIMEOUT_INTERVAL>(
			TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL);
	}
	else {
		optionSet.set<Options::TXN_TIMEOUT_INTERVAL>(
			optionSet.get<Options::STATEMENT_TIMEOUT_INTERVAL>() / 1000);
	}

	if (optionSet.get<Options::MAX_ROWS>() == 0) {
		const int64_t value = SQL_MAX_ROWS;
		optionSet.set<Options::MAX_ROWS>(value);
	}

	if (optionSet.get<Options::FETCH_SIZE>() == 0) {
		const int64_t value = SQL_DEFAULT_FETCH_SIZE;
		optionSet.set<Options::FETCH_SIZE>(value);
	}
}


template<typename T> void decodeValue(
	util::ByteStream<util::ArrayInStream>& in, T& value) {
	try {
		in >> value;
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "Decode numeric value failed");
	}
}

void SQLService::requestCancel(EventContext& ec) {
	util::StackAllocator& alloc = ec.getAllocator();
	util::Vector<ClientId> clientIdList(alloc);
	getExecutionManager()->getCurrentClientIdList(clientIdList);
	for (size_t pos = 0; pos < clientIdList.size(); pos++) {
		Event request(ec, SQL_CANCEL_QUERY, IMMEDIATE_PARTITION_ID);
		EventByteOutStream out = request.getOutStream();
		out << MAX_STATEMENTID;
		StatementHandler::encodeUUID<EventByteOutStream>(out, clientIdList[pos].uuid_,
			TXN_CLIENT_UUID_BYTE_SIZE);
		out << clientIdList[pos].sessionId_;
		uint8_t flag = 1;
		out << flag;
		ee_.add(request);
	}
	util::Vector<JobId> jobIdList(alloc);
	JobManager* jobManager = getExecutionManager()->getJobManager();
	CancelOption option;
	jobManager->cancelAll(ec, alloc, jobIdList, option);
}

void SQLService::sendCancel(EventContext& ec, NodeId nodeId) {
	try {
		Event request(ec, REQUEST_CANCEL, IMMEDIATE_PARTITION_ID);
		const NodeDescriptor& nd = ee_.getServerND(nodeId);
		if (nodeId == 0) {
			request.setSenderND(nd);
		}
		ee_.send(request, nd);
	}
	catch (std::exception& e) {
	}
}


#include "sql_processor_result.h"

/*!
	@brief キャンセルハンドラ
	@param [in] ec イベントコンテキスト
	@param [in] ev イベント
	@note immediateで実行
*/
void SQLCancelHandler::operator ()(EventContext& ec, Event& ev) {

	util::StackAllocator& alloc = ec.getAllocator();
	util::StackAllocator::Scope scope(alloc);

	try {

		bool isClientCancel = true;
		if (ev.getSenderND().isEmpty() || (!ev.getSenderND().isEmpty() && ev.getSenderND().getId() >= 0)) {
			isClientCancel = false;
		}

		ClientId cancelClientId;
		ExecutionId execId;
		EventByteInStream in = ev.getInStream();

		decodeValue(in, execId);
		StatementHandler::decodeUUID<util::ByteStream<util::ArrayInStream>>(in, cancelClientId.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
		decodeValue(in, cancelClientId.sessionId_);

		SQLExecutionManager::Latch latch(cancelClientId, executionManager_);
		SQLExecution* execution = latch.get();
		if (execution) {
			JobId jobId;
			execution->getContext().getCurrentJobId(jobId);
			if (!isClientCancel) {
				GS_TRACE_WARNING(SQL_SERVICE, GS_TRACE_SQL_CANCEL,
					"Call cancel by node failure, jobId=" << jobId);
			}
			else {
				GS_TRACE_WARNING(SQL_SERVICE, GS_TRACE_SQL_CANCEL,
					"Call cancel by client, jobId=" << jobId);
			}
			jobManager_->cancel(ec, jobId, true);
			execution->cancel(ec, execId);
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_CANCELLED,
				"Cancel SQL, clientId=" << cancelClientId << ", location=cancel");
		}
	}
	catch (std::exception& e) {
		UTIL_TRACE_EXCEPTION_INFO(SQL_SERVICE, e, "");
	}
}

/*!
	@brief ソケット切断ハンドラ
	@param [in] ec イベントコンテキスト
	@param [in] ev イベント
	@note immediateで実行
*/
void SQLSocketDisconnectHandler::operator ()(EventContext& ec, Event& ev) {

	util::StackAllocator& alloc = ec.getAllocator();
	util::StackAllocator::Scope scope(alloc);

	try {
		executionManager_->closeConnection(ec, ev.getSenderND());
	}
	catch (std::exception& e) {
		UTIL_TRACE_EXCEPTION(SQL_SERVICE, e, "");
	}
}

/*!
	@brief 同期処理受信処理部を実行する
	@param [in] ec イベントコンテキスト
	@param [in] ev イベント
	@note immediateで実行
*/
void NoSQLSyncReceiveHandler::operator ()(EventContext& ec, Event& ev) {

	util::StackAllocator& alloc = ec.getAllocator();
	util::StackAllocator::Scope scope(alloc);

	try {
		Request request(alloc, getRequestSource(ev));

		EventByteInStream in(ev.getInStream());
		StatementId stmtId;
		StatementExecStatus status;

		in >> stmtId;
		in >> status;

		util::Exception dest;
		if (status != TXN_STATEMENT_SUCCESS) {
			SQLErrorUtils::decodeException(alloc, in, dest);
		}
		ConnectionOption& connOption =
			ev.getSenderND().getUserData<ConnectionOption>();
		decodeRequestOptionPart(in, request, connOption);

		ClientId targetClientId;
		request.optional_.get<Options::UUID>().get(targetClientId.uuid_);
		targetClientId.sessionId_ = request.optional_.get<Options::QUERY_ID>();

		if (!request.optional_.get<Options::FOR_SYNC>()) {
			JobId jobId;
			jobId.clientId_ = targetClientId;
			jobId.execId_ = request.optional_.get<Options::JOB_EXEC_ID>();
			jobId.versionId_ = request.optional_.get<Options::JOB_VERSION>();
			JobManager::Latch latch(jobId, "NoSQLSyncReceiveHandler::operator", jobManager_);
			Job* job = latch.get();
			if (job) {
				job->receiveNoSQLResponse(
					ec, dest, status, request.optional_.get<Options::SUB_CONTAINER_ID>());
			}
			else {
				return;
			}
		}
		else {
			SQLExecutionManager::Latch latch(targetClientId, executionManager_);
			SQLExecution* execution = latch.get();
			if (execution) {
				if (status == TXN_STATEMENT_SUCCESS && execution->isCancelled()) {
					status = TXN_STATEMENT_ERROR;
					try {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_CANCELLED,
							"Cancel SQL, clientId=" << targetClientId << ", location=NoSQL Receive");
					}
					catch (std::exception& e) {
						dest = GS_EXCEPTION_CONVERT(e, "");
					}
				}
				NoSQLSyncContext& syncContext = execution->getContext().getSyncContext();
				const EventType eventType =
					request.optional_.get<Options::ACK_EVENT_TYPE>();
				const int64_t requestId =
					request.optional_.get<Options::NOSQL_SYNC_ID>();
				syncContext.put(eventType, requestId, status, in, dest, request);
			}
		}
	}
	catch (std::exception& e) {
		UTIL_TRACE_EXCEPTION(SQL_SERVICE, e,
			"NoSQL sync receive failed, reason=" << GS_EXCEPTION_MESSAGE(e));
	}
}

/*!
	@brief コンフィグセットアップ
	@param [in] config コンフィグ
*/
void SQLService::ConfigSetUpHandler::operator()(ConfigTable& config) {
	CONFIG_TABLE_RESOLVE_GROUP(config, CONFIG_TABLE_SQL, "sql");

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_NOTIFICATION_ADDRESS, STRING).
		inherit(CONFIG_TABLE_ROOT_NOTIFICATION_ADDRESS);
	CONFIG_TABLE_ADD_PORT_PARAM(
		config, CONFIG_TABLE_SQL_NOTIFICATION_PORT, 41999);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_NOTIFICATION_INTERVAL, INT32).
		setUnit(ConfigTable::VALUE_UNIT_DURATION_S).
		setMin(1).
		setDefault(5);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_SERVICE_ADDRESS, STRING).
		inherit(CONFIG_TABLE_ROOT_SERVICE_ADDRESS);
	CONFIG_TABLE_ADD_PORT_PARAM(
		config, CONFIG_TABLE_SQL_SERVICE_PORT, 20001);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_CONCURRENCY, INT32).
		setMin(1).
		setMax(128).
		setDefault(4);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_CONNECTION_LIMIT, INT32).
		setMin(3).
		setMax(65536).
		setDefault(5000);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_EVENT_BUFFER_CACHE_SIZE, INT32).
		setUnit(ConfigTable::VALUE_UNIT_SIZE_MB).
		setMin(1).
		setDefault(1024);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_USE_KEEPALIVE, BOOL).
		setExtendedType(ConfigTable::EXTENDED_TYPE_LAX_BOOL).
		setDefault(true);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_KEEPALIVE_IDLE, INT32).
		setUnit(ConfigTable::VALUE_UNIT_DURATION_S).
		setMin(0).
		setDefault(600);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_KEEPALIVE_INTERVAL, INT32).
		setUnit(ConfigTable::VALUE_UNIT_DURATION_S).
		setMin(0).
		setDefault(60);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_KEEPALIVE_COUNT, INT32).
		setMin(0).
		setDefault(5);
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
		config, CONFIG_TABLE_SQL_SEND_PENDING_INTERVAL, INT32)
		.setMin(0)
		.setDefault(500);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_SEND_PENDING_TASK_LIMIT, INT32)
		.setMin(1)
		.setDefault(5);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_SEND_PENDING_JOB_LIMIT, INT32)
		.setMin(1)
		.setDefault(50);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_SEND_PENDING_TASK_CONCURRENCY, INT32)
		.setMin(1)
		.setDefault(30);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_JOB_TOTAL_MEMORY_LIMIT, INT32).
		setUnit(ConfigTable::VALUE_UNIT_SIZE_MB).
		setMin(0).
		setDefault(0);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_NOSQL_FAILOVER_TIMEOUT, INT32).
		setUnit(ConfigTable::VALUE_UNIT_DURATION_S).
		setMin(0).
		setDefault(60 * 5);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_TABLE_CACHE_EXPIRED_TIME, INT32).
		setUnit(ConfigTable::VALUE_UNIT_DURATION_S).
		setMin(0).
		setDefault(600);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_TABLE_CACHE_SIZE, INT32).
		setMin(1).
		setDefault(512);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_MULTI_INDEX_SCAN, BOOL).
		setExtendedType(ConfigTable::EXTENDED_TYPE_LAX_BOOL).
		setDefault(false);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_ENABLE_PROFILER, BOOL).
		setExtendedType(ConfigTable::EXTENDED_TYPE_LAX_BOOL).
		setDefault(false);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_ENABLE_JOB_MEMORY_CHECK_INTERVAL_COUNT, INT32).
		setMin(0).
		setDefault(10);

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_SQL_LOCAL_SERVICE_ADDRESS, STRING)
		.setDefault("");

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_SQL_PUBLIC_SERVICE_ADDRESS, STRING)
		.setDefault("");

	CONFIG_TABLE_ADD_PARAM(config,
		CONFIG_TABLE_SQL_NOTIFICATION_INTERFACE_ADDRESS, STRING)
		.setDefault("");

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_PARTITIONING_ROWKEY_CONSTRAINT, BOOL).
		setExtendedType(ConfigTable::EXTENDED_TYPE_LAX_BOOL).
		setDefault(true);

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_SQL_PLAN_VERSION, STRING).
		setDefault("");

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_SQL_COST_BASED_JOIN, BOOL).
		setDefault(true);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_COST_BASED_JOIN_DRIVING, BOOL).
		inherit(CONFIG_TABLE_SQL_COST_BASED_JOIN);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_COST_BASED_JOIN_EXPLICIT, BOOL).
		setDefault(true);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_TABLE_CACHE_LOAD_DUMP_LIMIT_TIME, INT32).
		setMin(0).
		setDefault(5000);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_ENABLE_SCAN_META_TABLE_ADMINISTRATOR_PRIVILEGES, BOOL).
		setExtendedType(ConfigTable::EXTENDED_TYPE_LAX_BOOL).
		setDefault(true);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_ADD_BATCH_MAX_COUNT, INT32)
		.setMin(0)
		.setDefault(1000);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_TABLE_PARTITIONING_MAX_ASSIGN_NUM, INT32)
		.setMin(TABLE_PARTITIONING_MAX_HASH_PARTITIONING_NUM)
		.setMax(TABLE_PARTITIONING_MAX_ASSIGN_ENTRY_NUM)
		.setDefault(TABLE_PARTITIONING_DEFAULT_MAX_ASSIGN_NUM);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_TABLE_PARTITIONING_MAX_ASSIGN_ENTRY_NUM, INT32)
		.setMin(1)
		.setMax(TABLE_PARTITIONING_MAX_ASSIGN_ENTRY_NUM)
		.setDefault(TABLE_PARTITIONING_MAX_ASSIGN_ENTRY_NUM);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_FAIL_ON_TOTAL_MEMORY_LIMIT, BOOL)
		.setDefault(false);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_WORK_MEMORY_RATE, DOUBLE)
		.setMin(0)
		.setDefault(4);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_RESOURCE_CONTROL_LEVEL, INT32)
		.setMin(0)
		.setMax(2)
		.setDefault(0);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SQL_TOTAL_MEMORY_LIMIT, INT32).
		setUnit(ConfigTable::VALUE_UNIT_SIZE_MB).
		setMin(0).
		setDefault(0);
}

/*!
	@brief メッセージをエンコードする
	@param [in] ev イベント
	@param [in] t エンコード対象
*/
template <class T> void SQLService::encode(Event& ev, T& t) {
	try {
		try {
			EventByteOutStream out = ev.getOutStream();
			util::ObjectCoder::withAllocator(globalVarAlloc_).encode(out, t);
		}
		catch (std::exception& e) {
			GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(
				e, "Failed to encode message"));
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
}
template void SQLService::encode(Event& ev, TableSchemaInfo& t);

template <class T> void SQLService::decode(Event& ev, T& t) {
	try {
		try {
			EventByteInStream in = ev.getInStream();
			util::ObjectCoder::withAllocator(*globalVarAlloc_).decode(in, t);
		}
		catch (std::exception& e) {
			GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(
				e, "Failed to encode message"));
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
}

template void SQLService::decode(Event& ev, TableSchemaInfo& t);

SQLTableInfo* SQLService::decodeTableInfo(
	util::StackAllocator& alloc, util::ArrayByteInStream& in) {
	return SQLExecution::decodeTableInfo(alloc, in);
}

void SQLService::applyClusterPartitionCount(
	SQLTableInfo& tableInfo, uint32_t clusterPartitionCount) {
	SQLExecution::applyClusterPartitionCount(tableInfo, clusterPartitionCount);
}

void SQLRequestHandler::encodeEnvList(
	EventByteOutStream& out, util::StackAllocator& alloc,
	SQLExecutionManager& executionManager,
	ConnectionOption& connOption) {
	const size_t startPos = out.base().position();
	int32_t count = 0;
	encodeIntData<EventByteOutStream>(out, count);

	uint32_t bits = executionManager.getConnectionEnvUpdates(connOption);
	if (bits == 0) {
		return;
	}

	UTIL_STATIC_ASSERT(sizeof(bits) * CHAR_BIT > SQLPragma::PRAGMA_TYPE_MAX);
	for (uint32_t i = 0; i < SQLPragma::PRAGMA_TYPE_MAX; i++) {
		if ((bits & (1 << i)) != 0) {
			std::string value;
			bool hasData;
			if (executionManager.getConnectionEnv(
				connOption, i, value, hasData)) {
				const util::String name(SQLPragma::getPragmaTypeName(
					static_cast<SQLPragma::PragmaType>(i)), alloc);

				StatementHandler::encodeStringData<EventByteOutStream>(out, name);
				StatementHandler::encodeStringData<EventByteOutStream>(out, value);

				count++;
			}
			bits &= ~(1 << i);

			if (bits == 0) {
				break;
			}
		}
	}

	const size_t endPos = out.base().position();
	out.base().position(startPos);
	encodeIntData<EventByteOutStream>(out, count);
	out.base().position(endPos);

	executionManager.completeConnectionEnvUpdates(connOption);
}

template<typename T>
void decodeLargeRow(
	const char* key, util::StackAllocator& alloc, TransactionContext& txn,
	DataStoreV4* dataStore, const char8_t* dbName, BaseContainer* container,
	T& record, const EventMonotonicTime emNow);


SQLService::StatSetUpHandler SQLService::statSetUpHandler_;

#define STAT_ADD(id) STAT_TABLE_ADD_PARAM(stat, parentId, id)

void SQLService::StatSetUpHandler::operator()(StatTable& stat) {
	StatTable::ParamId parentId;

	parentId = STAT_TABLE_ROOT;
	stat.resolveGroup(parentId, STAT_TABLE_PERF, "performance");

	parentId = STAT_TABLE_PERF;
	STAT_ADD(STAT_TABLE_PERF_SQL_NUM_CONNECTION);
	STAT_ADD(STAT_TABLE_PERF_SQL_TOTAL_MEMORY);
	STAT_ADD(STAT_TABLE_PERF_SQL_PEAK_TOTAL_MEMORY);
	STAT_ADD(STAT_TABLE_PERF_SQL_TOTAL_MEMORY_LIMIT);
}

bool SQLService::StatUpdator::operator()(StatTable& stat) {
	if (!stat.getDisplayOption(STAT_TABLE_DISPLAY_SELECT_PERF)) {
		return true;
	}

	SQLService& svc = *service_;

	EventEngine::Stats sqlSvcStats;
	svc.getEE()->getStats(sqlSvcStats);

	const uint64_t numClientTCPConnection =
		sqlSvcStats.get(EventEngine::Stats::ND_CLIENT_CREATE_COUNT) -
		sqlSvcStats.get(EventEngine::Stats::ND_CLIENT_REMOVE_COUNT);
	const uint64_t numReplicationTCPConnection =
		sqlSvcStats.get(EventEngine::Stats::ND_SERVER_CREATE_COUNT) -
		sqlSvcStats.get(EventEngine::Stats::ND_SERVER_REMOVE_COUNT);
	const uint64_t numClientUDPConnection =
		sqlSvcStats.get(EventEngine::Stats::ND_MCAST_CREATE_COUNT) -
		sqlSvcStats.get(EventEngine::Stats::ND_MCAST_REMOVE_COUNT);
	const uint64_t numConnection = numClientTCPConnection +
		numReplicationTCPConnection +
		numClientUDPConnection;
	stat.set(STAT_TABLE_PERF_SQL_NUM_CONNECTION, numConnection);

	util::AllocatorLimitter::Stats stats = svc.totalMemoryLimitter_->getStats();
	if (!stats.failOnExcess_) {
		stats.limit_ = 0;
	}
	stat.set(STAT_TABLE_PERF_SQL_TOTAL_MEMORY, stats.usage_);
	stat.set(STAT_TABLE_PERF_SQL_PEAK_TOTAL_MEMORY, stats.peakUsage_);
	stat.set(STAT_TABLE_PERF_SQL_TOTAL_MEMORY_LIMIT, stats.limit_);

	return true;
}

SQLService::StatUpdator::StatUpdator() : service_(NULL) {}

SQLService::MemoryLimitErrorHandler::~MemoryLimitErrorHandler() {
}

void SQLService::MemoryLimitErrorHandler::operator()(util::Exception &e) {
	GS_RETHROW_USER_ERROR_CODED(GS_ERROR_SQL_TOTAL_MEMORY_EXCEEDED, e, "");
}

SQLService::MemoryLimitErrorHandler SQLService::memoryLimitErrorHandler_;

template<typename T>
void putLargeRows(
		const char* key,
		util::StackAllocator& alloc,
		TransactionContext& txn,
		DataStoreV4* dataStore,
		StatementHandler* statementHandler,
		PartitionTable* partitionTable,
		TransactionManager* transactionManager,
		StatementHandler::Request& request,
		util::XArray<ColumnInfo>& columnInfoList,
		util::XArray<const util::XArray<uint8_t>*>& logRecordList,
		KeyDataStoreValue& keyStoreValue,
		bool isNoClient, T& record) {
	UNUSED_VARIABLE(partitionTable);

	util::XArray<uint8_t> fixedPart(alloc);
	util::XArray<uint8_t> varPart(alloc);

	OutputMessageRowStore outputMrs(
		dataStore->getConfig(),
		&columnInfoList[0],
		static_cast<uint32_t>(columnInfoList.size()),
		fixedPart,
		varPart,
		false);

	NoSQLUtils::makeLargeContainerRow(
		alloc, key, outputMrs, record);

	fixedPart.push_back(varPart.data(), varPart.size());

	DSInputMes input(alloc, DS_PUT_ROW, &fixedPart, PUT_INSERT_OR_UPDATE,
		MAX_SCHEMAVERSIONID);
	StackAllocAutoPtr<DSOutputMes> ret(alloc, static_cast<DSOutputMes*>(dataStore->exec(&txn, &keyStoreValue, &input)));

	ClientId clientId;
	if (isNoClient) {
		clientId = TXN_EMPTY_CLIENTID;
	}
	else {
		clientId = txn.getClientId();
	}

	util::XArray<uint8_t>* logBinary = statementHandler->appendDataStoreLog(alloc, txn,
		clientId,
		request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
		keyStoreValue.storeType_, ret.get()->dsLog_);

	logRecordList.push_back(logBinary);

	transactionManager->update(
		txn, request.fixed_.cxtSrc_.stmtId_);
}

void putLargeBinaryRows(
		const char* key, util::StackAllocator& alloc,
		TransactionContext& txn,
		DataStoreV4* dataStore,
		StatementHandler* statementHandler,
		PartitionTable* partitionTable,
		TransactionManager* transactionManager,
		StatementHandler::Request& request,
		util::XArray<ColumnInfo>& columnInfoList,
		util::XArray<const util::XArray<uint8_t>*>& logRecordList,
		KeyDataStoreValue& keyStoreValue,
		util::XArray<uint8_t>& record) {
	UNUSED_VARIABLE(partitionTable);

	util::XArray<uint8_t> fixedPart(alloc);
	util::XArray<uint8_t> varPart(alloc);

	OutputMessageRowStore outputMrs(
		dataStore->getConfig(),
		&columnInfoList[0],
		static_cast<uint32_t>(columnInfoList.size()),
		fixedPart,
		varPart,
		false);

	NoSQLUtils::makeLargeContainerRowBinary(
		alloc, key, outputMrs, record);
	fixedPart.push_back(varPart.data(), varPart.size());

	DSInputMes input(alloc, DS_PUT_ROW, &fixedPart, PUT_INSERT_OR_UPDATE,
		MAX_SCHEMAVERSIONID);
	StackAllocAutoPtr<DSOutputMes> ret(alloc, static_cast<DSOutputMes*>(dataStore->exec(&txn, &keyStoreValue, &input)));

	util::XArray<uint8_t>* logBinary = statementHandler->appendDataStoreLog(alloc, txn,
		request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
		keyStoreValue.storeType_, ret.get()->dsLog_);

	logRecordList.push_back(logBinary);

	transactionManager->update(
		txn, request.fixed_.cxtSrc_.stmtId_);
};

template<typename T>
void decodeLargeRow(
		const char* key, util::StackAllocator& alloc, TransactionContext& txn,
		DataStoreV4* dataStore, const char* dbName, BaseContainer* container,
		T& record, const EventMonotonicTime emNow) {
	UNUSED_VARIABLE(dbName);

	if (container->getAttribute() != CONTAINER_ATTR_LARGE
		&& container->getAttribute() != CONTAINER_ATTR_VIEW) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH, "");
	}

	util::NormalOStringStream queryStr;
	queryStr << "select * where key='"
		<< NoSQLUtils::LARGE_CONTAINER_KEY_VERSION
		<< "' OR key='" << key << "'";
	util::String query(queryStr.str().c_str(), alloc);
	ResultSet* rs = dataStore->getResultSetManager()->create(
		txn, container->getContainerId(), container->getVersionId(), emNow, NULL);
	const ResultSetGuard rsGuard(txn, *dataStore, *rs);
	QueryProcessor::executeTQL(
		txn, *container, MAX_RESULT_SIZE, TQLInfo(GS_SYSTEM, NULL, query.c_str()), *rs);
	rs->setResultType(RESULT_ROWSET);
	OutputMessageRowStore outputMessageRowStore(
		dataStore->getConfig(), container->getColumnInfoList(),
		container->getColumnNum(), *(rs->getRowDataFixedPartBuffer()),
		*(rs->getRowDataVarPartBuffer()), false /*isRowIdIncluded*/);
	ResultSize resultNum;
	container->getRowList(txn, *(rs->getOIdList()), rs->getResultNum(), resultNum,
		&outputMessageRowStore, false /*isRowIdIncluded*/, 0);

	const uint8_t* data1;
	uint32_t size1;
	outputMessageRowStore.getAllFixedPart(data1, size1);
	const uint8_t* data2;
	uint32_t size2;
	outputMessageRowStore.getAllVariablePart(data2, size2);

	InputMessageRowStore inputMessageRowStore(dataStore->getConfig(),
		container->getColumnInfoList(), container->getColumnNum(),
		reinterpret_cast<void*>(const_cast<uint8_t*>(data1)), size1,
		reinterpret_cast<void*>(const_cast<uint8_t*>(data2)), size2,
		rs->getResultNum(), false, false);

	VersionInfo versionInfo;
	decodeRow<T>(
		inputMessageRowStore, record, versionInfo, key);
};

template
void decodeLargeRow(
	const char* key, util::StackAllocator& alloc, TransactionContext& txn,
	DataStoreV4* dataStore, const char* dbName, BaseContainer* container,
	TablePartitioningInfo<util::StackAllocator>& record, const EventMonotonicTime emNow);

template
void decodeLargeRow(
	const char* key, util::StackAllocator& alloc, TransactionContext& txn,
	DataStoreV4* dataStore, const char* dbName, BaseContainer* container,
	TablePartitioningIndexInfo& record, const EventMonotonicTime emNow);

template
void decodeLargeRow(
	const char* key, util::StackAllocator& alloc, TransactionContext& txn,
	DataStoreV4* dataStore, const char* dbName, BaseContainer* container,
	TableProperty& record, const EventMonotonicTime emNow);

template
void decodeLargeRow(
	const char* key, util::StackAllocator& alloc, TransactionContext& txn,
	DataStoreV4* dataStore, const char* dbName, BaseContainer* container,
	ViewInfo& record, const EventMonotonicTime emNow);

void decodeLargeBinaryRow(
	const char* key, util::StackAllocator& alloc, TransactionContext& txn,
	DataStoreV4* dataStore, const char* dbName, BaseContainer* container,
	util::XArray<uint8_t>& record, const EventMonotonicTime emNow) {

	if (container->getAttribute() != CONTAINER_ATTR_LARGE) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH, "");
	}

	util::NormalOStringStream queryStr;
	queryStr << "select * where key='"
		<< NoSQLUtils::LARGE_CONTAINER_KEY_VERSION
		<< "' OR key='" << key << "'";
	util::String query(queryStr.str().c_str(), alloc);
	ResultSet* rs = dataStore->getResultSetManager()->create(
		txn, container->getContainerId(), container->getVersionId(), emNow,
		NULL);
	const ResultSetGuard rsGuard(txn, *dataStore, *rs);
	QueryProcessor::executeTQL(
		txn, *container, MAX_RESULT_SIZE, TQLInfo(dbName, NULL, query.c_str()), *rs);
	rs->setResultType(RESULT_ROWSET);
	OutputMessageRowStore outputMessageRowStore(
		dataStore->getConfig(), container->getColumnInfoList(),
		container->getColumnNum(), *(rs->getRowDataFixedPartBuffer()),
		*(rs->getRowDataVarPartBuffer()), false /*isRowIdIncluded*/);
	ResultSize resultNum;
	container->getRowList(txn, *(rs->getOIdList()), rs->getResultNum(), resultNum,
		&outputMessageRowStore, false /*isRowIdIncluded*/, 0);

	const uint8_t* data1;
	uint32_t size1;
	outputMessageRowStore.getAllFixedPart(data1, size1);
	const uint8_t* data2;
	uint32_t size2;
	outputMessageRowStore.getAllVariablePart(data2, size2);

	InputMessageRowStore inputMessageRowStore(dataStore->getConfig(),
		container->getColumnInfoList(), container->getColumnNum(),
		reinterpret_cast<void*>(const_cast<uint8_t*>(data1)), size1,
		reinterpret_cast<void*>(const_cast<uint8_t*>(data2)), size2,
		rs->getResultNum(), false, false);

	VersionInfo versionInfo;
	decodeBinaryRow<util::XArray<uint8_t> >(
		inputMessageRowStore, record, versionInfo, key);
};

template<typename S>
void PutLargeContainerHandler::InMessage::decode(S& in) {
	SimpleInputMessage::decode(in);
	decodeVarSizeBinaryData<S>(in, containerNameBinary_);
	decodeEnumData<S, ContainerType>(in, containerType_);
	decodeBooleanData<S>(in, modifiable_);
	decodeBinaryData<S>(in, containerInfo_, false);
	decodeBinaryData<EventByteInStream>(in, normalContainerInfo_, false);
	if (request_.optional_.get<Options::CONTAINER_ATTRIBUTE>() >= CONTAINER_ATTR_ANY) {
		request_.optional_.set<Options::CONTAINER_ATTRIBUTE>(CONTAINER_ATTR_SINGLE);
	}
	isCaseSensitive_ = getCaseSensitivity(request_).isContainerNameCaseSensitive();
	featureVersion_ = request_.optional_.get<Options::FEATURE_VERSION>();

	if (in.base().remaining() != 0) {

		util::XArray<uint8_t> binary(alloc_);
		decodeBinaryData<EventByteInStream>(in, binary, false);

		util::ArrayByteInStream inStream(
			util::ArrayInStream(
				binary.data(), binary.size()));

		partitioningInfo_ = ALLOC_NEW(alloc_) TablePartitioningInfo<util::StackAllocator>(alloc_);
		util::ObjectCoder::withAllocator(alloc_).
			decode(inStream, *partitioningInfo_);
	}
}


void PutLargeContainerHandler::operator()(
	EventContext& ec, Event& ev) {

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);
	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {
		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		EVENT_START(ec, ev, transactionManager_, connOption.dbId_, false);

		util::XArray<uint8_t>& containerNameBinary = inMes.containerNameBinary_;
		util::String containerName(alloc); 
		ContainerType& containerType = inMes.containerType_;
		bool& modifiable = inMes.modifiable_;
		util::XArray<uint8_t>& containerInfo = inMes.containerInfo_;
		util::XArray<uint8_t>& normalContainerInfo = inMes.normalContainerInfo_;
		TablePartitioningInfo<util::StackAllocator>& partitioningInfo = *(inMes.partitioningInfo_);


		if (connOption.requestType_ == Message::REQUEST_NOSQL) {
			connOption.keepaliveTime_
				= ec.getHandlerStartMonotonicTime();
		}

		request.fixed_.clientId_
			= request.optional_.get<Options::CLIENT_ID>();

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;

		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkExecutable(
			request.fixed_.pId_,
			clusterRole,
			partitionRole,
			partitionStatus,
			partitionTable_);

		GS_TRACE_INFO(
			DATASTORE_BACKGROUND, GS_ERROR_DS_BACKGROUND_TASK_INVALID,
			"[PutContainerHandler PartitionId = " << request.fixed_.pId_ <<
			", stmtId = " << request.fixed_.cxtSrc_.stmtId_);

		request.fixed_.cxtSrc_.getMode_ = TransactionManager::PUT; 
		request.fixed_.cxtSrc_.txnMode_ = TransactionManager::NO_AUTO_COMMIT_BEGIN;

		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn = transactionManager_->putNoExpire(
			alloc,
			request.fixed_.pId_,
			request.fixed_.clientId_,
			request.fixed_.cxtSrc_,
			now,
			emNow);
		txn.setAuditInfo(&ev, &ec, NULL);

		if (txn.getPartitionId() != request.fixed_.pId_) {

			GS_THROW_SYSTEM_ERROR(
				GS_ERROR_TM_TRANSACTION_MODE_INVALID,
				"Invalid context exist request pId=" << request.fixed_.pId_ <<
				", txn pId=" << txn.getPartitionId());
		}

		if (!checkPrivilege(
			ev.getType(),
			connOption.userType_,
			connOption.requestType_,
			request.optional_.get<Options::SYSTEM_MODE>(),
			request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>(),
			static_cast<ContainerAttribute>(
				request.optional_.get<Options::CONTAINER_ATTRIBUTE>()))) {

			GS_THROW_USER_ERROR(
				GS_ERROR_DS_CON_ACCESS_INVALID,
				"Can not create container attribute : " <<
				static_cast<int32_t>(request.optional_.get<
					Options::CONTAINER_ATTRIBUTE>()));
		}

		const FullContainerKey containerKey(
			alloc,
			getKeyConstraint(request.optional_),
			containerNameBinary.data(), containerNameBinary.size());

		bool isNewSql = isNewSQL(request);
		checkLoggedInDatabase(
			connOption.dbId_,
			connOption.dbName_.c_str(),
			containerKey.getComponents(alloc).dbId_,
			request.optional_.get<Options::DB_NAME>(),
			isNewSql);

		{

			if (containerName.compare(GS_USERS) == 0 ||
				containerName.compare(GS_DATABASES) == 0) {

				GS_THROW_USER_ERROR(
					GS_ERROR_DS_CON_ACCESS_INVALID,
					"Can not create container name : " <<
					request.optional_.get<Options::DB_NAME>());
			}
		}

		KeyDataStore* keyStore = getKeyDataStore(txn.getPartitionId());
		KeyDataStoreValue keyStoreValue = keyStore->get(txn, containerKey, inMes.isCaseSensitive_);

		DataStoreV4* ds = static_cast<DataStoreV4*>(getDataStore(txn.getPartitionId(), keyStoreValue.storeType_));
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);

		DSInputMes input(alloc, DS_PUT_CONTAINER, &containerKey, containerType, &containerInfo,
			modifiable, inMes.featureVersion_, inMes.isCaseSensitive_);
		StackAllocAutoPtr<DSPutContainerOutputMes> ret(alloc, static_cast<DSPutContainerOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		PutStatus putStatus = ret.get()->status_;
		KeyDataStoreValue outputStoreValue = ret.get()->storeValue_;

		DSPutContainerOutputMes* successMes = ret.get();

		request.fixed_.cxtSrc_.containerId_ = outputStoreValue.containerId_;
		txn.setContainerId(outputStoreValue.containerId_);

		util::XArray<const util::XArray<uint8_t>*> logRecordList(alloc);
		util::XArray<ClientId> closedResourceIds(alloc);
		if (putStatus != PutStatus::NOT_EXECUTED) {
			util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
				request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
				outputStoreValue.storeType_, ret.get()->dsLog_);
			logRecordList.push_back(logBinary);
		}

		bool isAlterExecuted = false;
		if (putStatus == PutStatus::UPDATE) {
			DSInputMes input(alloc, DS_CONTINUE_ALTER_CONTAINER);
			StackAllocAutoPtr<DSPutContainerOutputMes> retAlter(alloc, static_cast<DSPutContainerOutputMes*>(ds->exec(&txn, &outputStoreValue, &input)));
			isAlterExecuted = retAlter.get()->isExecuted_;

			util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
				request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
				outputStoreValue.storeType_, retAlter.get()->dsLog_);
			logRecordList.push_back(logBinary);
		}

		int32_t replicationMode
			= transactionManager_->getReplicationMode();
		ReplicationContext::TaskStatus taskStatus
			= ReplicationContext::TASK_FINISHED;

		if (putStatus == PutStatus::CREATE || putStatus == PutStatus::CHANGE_PROPERTY ||
			isAlterExecuted) {
			DSInputMes input(alloc, DS_COMMIT);
			StackAllocAutoPtr<DSOutputMes> retCommit(alloc, static_cast<DSOutputMes*>(ds->exec(&txn, &outputStoreValue, &input)));
			util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
				request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
				outputStoreValue.storeType_, retCommit.get()->dsLog_);
			logRecordList.push_back(logBinary);

			transactionManager_->remove(request.fixed_.pId_, request.fixed_.clientId_);
			closedResourceIds.push_back(request.fixed_.clientId_);

			request.fixed_.cxtSrc_.getMode_ = TransactionManager::AUTO;
			request.fixed_.cxtSrc_.txnMode_ = TransactionManager::AUTO_COMMIT;
			txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
			txn.setAuditInfo(&ev, &ec, NULL);

			bool noClient = false;
			util::XArray<ColumnInfo> columnInfoList(alloc);
			NoSQLUtils::makeLargeContainerColumn(columnInfoList);

			VersionInfo versionInfo;
			putLargeRows(
				NoSQLUtils::LARGE_CONTAINER_KEY_VERSION,
				alloc,
				txn,
				ds,
				this,
				partitionTable_,
				transactionManager_,
				request,
				columnInfoList,
				logRecordList,
				outputStoreValue,
				noClient,
				versionInfo);

			partitioningInfo.largeContainerId_ = txn.getContainerId();
			putLargeRows(
				NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
				alloc,
				txn,
				ds,
				this,
				partitionTable_,
				transactionManager_,
				request,
				columnInfoList,
				logRecordList,
				outputStoreValue,
				noClient,
				partitioningInfo);

			putLargeBinaryRows(
				NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
				alloc,
				txn,
				ds,
				this,
				partitionTable_,
				transactionManager_,
				request,
				columnInfoList,
				logRecordList,
				outputStoreValue,
				normalContainerInfo);

			util::ArrayByteInStream normalIn = util::ArrayByteInStream(
				util::ArrayInStream(
					normalContainerInfo.data(),
					normalContainerInfo.size()));

			MessageSchema messageSchema(
				alloc,
				ds->getConfig(),
				containerName.c_str(),
				normalIn,
				request.optional_.get<Options::FEATURE_VERSION>());

			util::Vector<ColumnId> rowKeys
				= messageSchema.getRowKeyColumnIdList();

			if (rowKeys.size() > 0
				&& partitioningInfo.containerType_ == COLLECTION_CONTAINER) {

				TablePartitioningIndexInfo
					tablePartitioningIndexInfo(alloc);

				const NameWithCaseSensitivity indexName("", false);
				util::Vector<util::String> columnNameList(alloc);
				util::Vector<ColumnType> columnTypeList(alloc);
				util::Vector<uint8_t> columnOptionList(alloc);

				NoSQLUtils::makeContainerColumns(
					alloc,
					normalContainerInfo,
					columnNameList,
					columnTypeList,
					columnOptionList);

				TablePartitioningIndexInfoEntry* entry;

				if (rowKeys.size() == 1) {

					ColumnType targetColumnType = columnTypeList[rowKeys[0]];
					const DataStoreConfig* dsConfig = sqlService_->
						getExecutionManager()->getManagerSet()->dsConfig_;
					MapType targetIndexType = getAvailableIndex(
						*dsConfig,
						indexName.name_,
						targetColumnType,
						partitioningInfo.containerType_,
						true);

					entry = tablePartitioningIndexInfo.add(
						indexName,
						rowKeys,
						targetIndexType);
				}
				else {
					entry = tablePartitioningIndexInfo.add(
						indexName,
						rowKeys,
						MAP_TYPE_BTREE);
				}

				entry->status_ = INDEX_STATUS_CREATE_END;
				putLargeRows(
					NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
					alloc,
					txn,
					ds,
					this,
					partitionTable_,
					transactionManager_,
					request,
					columnInfoList,
					logRecordList,
					outputStoreValue,
					noClient,
					tablePartitioningIndexInfo);
			}
		}
		else if (putStatus == PutStatus::NOT_EXECUTED) {
			DSInputMes input(alloc, DS_COMMIT);
			ds->exec(&txn, &outputStoreValue, &input);

			transactionManager_->remove(request.fixed_.pId_, request.fixed_.clientId_);
			closedResourceIds.push_back(request.fixed_.clientId_);

			request.fixed_.cxtSrc_.getMode_ = TransactionManager::AUTO;
			request.fixed_.cxtSrc_.txnMode_ = TransactionManager::AUTO_COMMIT;
			txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
			txn.setAuditInfo(&ev, &ec, NULL);
		}
		else {
			replicationMode = TransactionManager::REPLICATION_SEMISYNC;
			taskStatus = ReplicationContext::TASK_CONTINUE;

			transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);
		}

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false),
			successMes->mes_);
		const bool ackWait = executeReplication(
			request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
			replicationMode, taskStatus, closedResourceIds.data(),
			closedResourceIds.size(), logRecordList.data(),
			logRecordList.size(), request.fixed_.cxtSrc_.stmtId_, 0,
			&outMes);

		if (putStatus != PutStatus::UPDATE || isAlterExecuted) {
			replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
				request, outMes, ackWait);
		}
		else {
			continueEvent(
				ec, alloc, ev.getSenderND(), ev.getType(),
				request.fixed_.cxtSrc_.stmtId_, request, response, ackWait);
		}
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void UpdateContainerStatusHandler::decode(EventByteInStream& in,
	Request& request, ConnectionOption& conn,
	util::XArray<uint8_t>& containerNameBinary,
	ContainerCategory& category, NodeAffinityNumber& affinity,
	TablePartitioningVersionId& versionId, ContainerId& largeContainerId,
	LargeContainerStatusType& status, IndexInfo& indexInfo) {

	int32_t dataSize = 0;
	decodeRequestCommonPart(in, request, conn);
	decodeVarSizeBinaryData<EventByteInStream>(in, containerNameBinary);
	in >> category;
	in >> dataSize;
	if (category == TABLE_PARTITIONING_CHECK_EXPIRED) {
		largeContainerId = UNDEF_CONTAINERID;
	}
	else
		if (category == TABLE_PARTITIONING_VERSION) {
			in >> versionId;
		}
		else {
			in >> largeContainerId;
			in >> affinity;
			if (category != TABLE_PARTITIONING_RECOVERY) {
				in >> status;
			}
			if (in.base().remaining() != 0 && category != TABLE_PARTITIONING_RECOVERY) {
				if (status != PARTITION_STATUS_NONE) {
					decodeIndexInfo<EventByteInStream>(in, indexInfo);
				}
				else {
					if (isNewSQL(request)) {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_TABLE_PARTITION_INVALID_MESSAGE,
							"Invalid message from sql server, affinity="
							<< affinity << ", status=" << getPartitionStatusRowName(status));
					}
					else {
						GS_THROW_USER_ERROR(GS_ERROR_SQL_TABLE_PARTITION_INVALID_MESSAGE,
							"Invalid message from java client, affinity=" << affinity
							<< ", status=" << getPartitionStatusRowName(status));
					}
				}
			}
		}
}

bool checkExpiredContainer(
		util::StackAllocator& alloc,
		EventContext& ec,
		const FullContainerKey& containerKey,
		TransactionContext& txn,
		ContainerId containerId,
		TablePartitioningInfo<util::StackAllocator>& partitioningInfo,
		PartitionTable* pt,
		const DataStoreConfig& dsConfig) {
	UNUSED_VARIABLE(dsConfig);

	if (!partitioningInfo.isTableExpiration()) {
		return false;
	}

	int64_t currentTime = ec.getHandlerStartTime().getUnixTime();

	util::Vector<NodeAffinityNumber> affinityNumberList(alloc);
	util::Vector<size_t> affinityPosList(alloc);

	Timestamp currentErasableTimestamp
		= txn.getStatementStartTime().getUnixTime();

	int64_t duration = MessageSchema::getTimestampDuration(
		partitioningInfo.timeSeriesProperty_.elapsedTime_,
		partitioningInfo.timeSeriesProperty_.timeUnit_);

	partitioningInfo.checkExpirableInterval(
		currentTime,
		currentErasableTimestamp,
		duration,
		affinityNumberList,
		affinityPosList);

	if (affinityNumberList.size() > 0) {

		FullContainerKeyComponents component
			= containerKey.getComponents(alloc, true);
		DatabaseId dbId = component.dbId_;

		util::String containerNameStr(alloc);
		containerKey.toString(alloc, containerNameStr);

		ContainerId largeContainerId = containerId;
		for (size_t pos = 0;
			pos < affinityNumberList.size(); pos++) {

			FullContainerKeyComponents subComponents;
			subComponents.dbId_ = dbId;
			subComponents.affinityNumber_ = affinityNumberList[pos];
			subComponents.baseName_ = containerNameStr.c_str();
			subComponents.baseNameSize_ = static_cast<int32_t>(
				strlen(containerNameStr.c_str()));
			subComponents.largeContainerId_ = largeContainerId;

			KeyConstraint keyConstraint(
				KeyConstraint::getNoLimitKeyConstraint());
			FullContainerKey* subContainerKey
				= ALLOC_NEW(alloc) FullContainerKey(
					alloc, keyConstraint, subComponents);

			PartitionId targetPId = NoSQLUtils::resolvePartitionId(alloc,
				pt->getPartitionNum(), *subContainerKey);

			Event sendRequest(ec, DROP_CONTAINER, targetPId);
			EventEngine& ee = ec.getEngine();

			EventByteOutStream out = sendRequest.getOutStream();
			StatementMessage::FixedRequest::Source
				source(targetPId, DROP_CONTAINER);

			StatementMessage::FixedRequest request2(source);
			request2.cxtSrc_.stmtId_ = 0;

			StatementMessage::OptionSet option(alloc);
			option.set<StatementMessage::Options::REPLY_PID>(targetPId);

			bool systemMode = true;
			option.set<StatementMessage::Options::SYSTEM_MODE>(systemMode);

			bool forSync = true;
			option.set<StatementMessage::Options::FOR_SYNC>(forSync);

			ContainerId containerId = -1;
			option.set<StatementMessage::Options::SUB_CONTAINER_ID>(
				static_cast<int32_t>(containerId));

			request2.encode(out);
			ClientId clientId;
			UUIDUtils::generate(clientId.uuid_);

			option.set<StatementMessage::Options::CLIENT_ID>(clientId);

			option.set<StatementMessage::Options::CONTAINER_ATTRIBUTE>(
				CONTAINER_ATTR_SUB);

			option.set<StatementMessage::Options::FEATURE_VERSION>(
				MessageSchema::V4_1_VERSION);

			option.encode(out);

			StatementHandler::encodeContainerKey(out, *subContainerKey);
			out << partitioningInfo.containerType_;

			NodeId ownerId = pt->getOwner(targetPId);
			if (ownerId == UNDEF_NODEID) {
				continue;
			}

			const NodeDescriptor& nd = ee.getServerND(ownerId);
			if (ownerId == 0) {
				sendRequest.setSenderND(nd);
				ee.add(sendRequest);
			}
			else {
				ee.send(sendRequest, nd);
			}

			util::String subContainerNameStr(alloc);
			subContainerKey->toString(alloc, subContainerNameStr);
			GS_TRACE_WARNING(DATA_EXPIRATION_DETAIL, GS_TRACE_DS_EXPIRED_CONTAINER_INFO,
				"Detect expired container, dbId=" << dbId << ", pId=" << targetPId << ", name="
				<< subContainerNameStr.c_str() << ", to=" << nd);

			for (size_t pos = 0; pos < affinityPosList.size(); pos++) {

				if (partitioningInfo.assignStatusList_
					[affinityPosList[pos]] != PARTITION_STATUS_DROP_START) {

					partitioningInfo.assignStatusList_[affinityPosList[pos]]
						= PARTITION_STATUS_DROP_START;
					partitioningInfo.activeContainerCount_--;
				}
			}
		}
		return true;
	}
	else {
		return false;
	}
}

void UpdateContainerStatusHandler::operator()(
	EventContext& ec, Event& ev) {

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);
	try {
		ConnectionOption& connOption =
			ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		util::XArray<uint8_t> containerNameBinary(alloc);

		bool updatePartitioningInfo = false;
		bool updateIndexInfo = false;

		TablePartitioningVersionId targetVersionId;
		IndexInfo indexInfo(alloc);

		int64_t baseValue = 0;

		ContainerId largeContainerId = UNDEF_CONTAINERID;
		LargeContainerStatusType targetStatus = PARTITION_STATUS_NONE;
		NodeAffinityNumber affinityNumber = UNDEF_NODE_AFFINITY_NUMBER;
		ContainerCategory category = UNDEF_LARGE_CONTAINER_CATEGORY;

		decode(
			in,
			request,
			connOption,
			containerNameBinary,
			category,
			affinityNumber,
			targetVersionId,
			largeContainerId,
			targetStatus,
			indexInfo);
		EVENT_START(ec, ev, transactionManager_, connOption.dbId_, false);

		const ClusterRole clusterRole
			= (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;

		if (category != TABLE_PARTITIONING_CHECK_EXPIRED) {
			checkAuthentication(ev.getSenderND(), emNow);  
			checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
			checkTransactionTimeout(
				emNow,
				ev.getQueuedMonotonicTime(),
				request.fixed_.cxtSrc_.txnTimeoutInterval_,
				ev.getQueueingCount());
		}

		checkExecutable(
			request.fixed_.pId_,
			clusterRole,
			partitionRole,
			partitionStatus,
			partitionTable_);

		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn = transactionManager_->put(
			alloc,
			request.fixed_.pId_ /*pId*/,
			TXN_EMPTY_CLIENTID,
			request.fixed_.cxtSrc_,
			now,
			emNow);
		txn.setAuditInfo(&ev, &ec, NULL);

		bool noClient = true;

		const FullContainerKey containerKey(
			alloc,
			getKeyConstraint(CONTAINER_ATTR_ANY, false),
			containerNameBinary.data(),
			containerNameBinary.size());

		bool caseSensitive = getCaseSensitivity(request).isContainerNameCaseSensitive();
		KeyDataStore* keyStore = getKeyDataStore(txn.getPartitionId());
		KeyDataStoreValue keyStoreValue = keyStore->get(txn, containerKey, caseSensitive);
		DataStoreV4* ds = static_cast<DataStoreV4*>(getDataStore(txn.getPartitionId(), keyStoreValue.storeType_));
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);

		txn.setContainerId(keyStoreValue.containerId_);

		if (category == TABLE_PARTITIONING_VERSION) {
			bool ackWait = false;

			DSInputMes input(alloc, DS_UPDATE_TABLE_PARTITIONING_ID, &containerKey, targetVersionId);
			txn.setAuditInfo(&ev, &ec, NULL);
			StackAllocAutoPtr<DSPutContainerOutputMes> ret(alloc, static_cast<DSPutContainerOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
			PutStatus putStatus = ret.get()->status_;

			DSMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
				getReplyOption(alloc, request, NULL, false),
				ret.get()->mes_);
			if (putStatus != PutStatus::NOT_EXECUTED) {
				request.fixed_.cxtSrc_.getMode_
					= TransactionManager::PUT; 
				request.fixed_.cxtSrc_.txnMode_
					= TransactionManager::NO_AUTO_COMMIT_BEGIN;

				txn.setContainerId(ret.get()->storeValue_.containerId_);
				util::XArray<const util::XArray<uint8_t>*> logRecordList(alloc);
				util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
					request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
					keyStoreValue.storeType_, ret.get()->dsLog_);
				logRecordList.push_back(logBinary);

				ackWait = executeReplication(
					request,
					ec,
					alloc,
					ev.getSenderND(),
					txn, ev.getType(),
					request.fixed_.cxtSrc_.stmtId_,
					transactionManager_->getReplicationMode(),
					NULL,
					0,
					logRecordList.data(),
					logRecordList.size(),
					&outMes);
			}

			replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
				request, outMes, ackWait);

			return;
		}



		if (keyStoreValue.containerId_ == UNDEF_CONTAINERID
			|| (keyStoreValue.containerId_ != largeContainerId
				&& largeContainerId != UNDEF_CONTAINERID)) {

			util::String containerNameStr(alloc);
			containerKey.toString(
				alloc, containerNameStr);

			GS_THROW_USER_ERROR(
				GS_ERROR_SQL_TABLE_PARTITION_PARTITIONING_TABLE_NOT_FOUND,
				"Target large container '"
				<< containerNameStr << "' is already removed");
		}

		DSInputMes input(alloc, DS_GET_CONTAINER_OBJECT, ANY_CONTAINER);
		StackAllocAutoPtr<DSContainerOutputMes> ret(alloc, static_cast<DSContainerOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		BaseContainer* container = ret.get()->container_;

		TablePartitioningInfo<util::StackAllocator>
			partitioningInfo(alloc);
		{
			TransactionContext& newTxn = transactionManager_->put(
				alloc,
				request.fixed_.pId_ /*pId*/,
				TXN_EMPTY_CLIENTID,
				request.fixed_.cxtSrc_,
				now,
				emNow);
			txn.setAuditInfo(&ev, &ec, NULL);

			decodeLargeRow(
				NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
				alloc,
				newTxn,
				ds,
				request.optional_.get<Options::DB_NAME>(),
				container,
				partitioningInfo,
				emNow);
		}
		partitioningInfo.init();

		if (category == TABLE_PARTITIONING_CHECK_EXPIRED) {
			const DataStoreConfig* dsConfig = sqlService_->getExecutionManager()->getManagerSet()->dsConfig_;
			updatePartitioningInfo = checkExpiredContainer(
				alloc,
				ec,
				containerKey,
				txn,
				keyStoreValue.containerId_,
				partitioningInfo,
				partitionTable_,
				*dsConfig);

			if (!updatePartitioningInfo) {
				return;
			}
		}
		if (category == TABLE_PARTITIONING_RECOVERY) {

			ClientId tmpClientId;
			UUIDUtils::generate(tmpClientId.uuid_);
			SQLExecutionManager* executionManager
				= resourceSet_->execMgr_;
			util::Random random(
				util::DateTime::now(false).getUnixTime());

			PartitionId targetPId
				= random.nextInt32(KeyDataStore::MAX_PARTITION_NUM);
			PartitionGroupId targetPgId
				= resourceSet_->sqlSvc_->getPartitionGroupId(targetPId);

			RequestInfo txmRequest(alloc, targetPId, targetPgId);
			SQLExecutionManager::Latch tmpLatch(tmpClientId,
				executionManager, &ev.getSenderND(), &txmRequest);
			Event tmpRequest(ec, SQL_REQUEST_NOSQL_CLIENT, targetPId);
			EventByteOutStream out = tmpRequest.getOutStream();
			StatementHandler::encodeUUID(out, tmpClientId.uuid_,
				TXN_CLIENT_UUID_BYTE_SIZE);
			const FullContainerKey containerKey = container->getContainerKey(txn);
			util::String containerName(txn.getDefaultAllocator());
			containerKey.toString(txn.getDefaultAllocator(), containerName);
			StatementHandler::encodeStringData(out, containerName);
			out << container->getContainerId();
			out << container->getVersionId();
			out << affinityNumber;
			out << ev.getPartitionId();
			out << request.fixed_.cxtSrc_.stmtId_;
			EventEngine* ee = sqlService_->getEE();
			ee->add(tmpRequest);
			return;

		}

		TablePartitioningIndexInfo tablePartitioningIndexInfo(alloc);

		bool isOptionSetValue = false;
		if (request.optional_.get<
			Options::INTERVAL_BASE_VALUE>().enabled_) {

			baseValue = request.optional_.get<
				Options::INTERVAL_BASE_VALUE>().baseValue_;
			isOptionSetValue = true;
		}

		int32_t partitioningNum = (partitioningInfo.partitionType_
			== SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH
			? partitioningInfo.partitioningNum_ : 1);

		switch (targetStatus) {

		case PARTITION_STATUS_CREATE_START: {

			if (SyntaxTree::isRangePartitioningType(
				partitioningInfo.partitionType_)) {

				if (!isOptionSetValue) {
					baseValue = partitioningInfo.calcValueFromAffinity(
						partitionTable_->getPartitionNum(),
						affinityNumber);
				}
				if (TupleColumnTypeUtils::isTimestampFamily(partitioningInfo.partitionColumnType_) 
					&&  !ValueProcessor::validateTimestamp(baseValue)) {
					GS_THROW_USER_ERROR(GS_ERROR_QP_TIMESTAMP_RANGE_INVALID,
						"Timestamp of partitioning key is out of range (value=" << baseValue
						<< ")");
				}

				size_t entryPos
					= partitioningInfo.findEntry(affinityNumber);

				if (entryPos == SIZE_MAX) {
					const FullContainerKey containerKey
						= container->getContainerKey(txn);
					partitioningInfo.checkMaxAssigned(
						txn,
						containerKey,
						partitioningNum, sqlService_->getTablePartitioningMaxAssignedNum());

					partitioningInfo.newEntry(
						affinityNumber,
						PARTITION_STATUS_CREATE_END,
						partitioningNum,
						baseValue, sqlService_->getTablePartitioningMaxAssignedEntryNum());

					partitioningInfo.incrementTablePartitioningVersionId();
					partitioningInfo.activeContainerCount_ += partitioningNum;
					updatePartitioningInfo = true;
				}
			}
		}
										  break;

		case PARTITION_STATUS_DROP_START: {

			if (SyntaxTree::isRangePartitioningType(
				partitioningInfo.partitionType_)) {

				if (!isOptionSetValue) {
					baseValue = partitioningInfo.calcValueFromAffinity(
						partitionTable_->getPartitionNum(),
						affinityNumber);
				}

				size_t entryPos = partitioningInfo.findEntry(
					affinityNumber);

				if (entryPos == SIZE_MAX) {

					partitioningInfo.newEntry(
						affinityNumber,
						PARTITION_STATUS_DROP_START,
						partitioningNum,
						baseValue, sqlService_->getTablePartitioningMaxAssignedEntryNum());

					updatePartitioningInfo = true;
				}
				else {
					for (size_t currentPos = 0;
						currentPos < static_cast<size_t>(partitioningNum);
						currentPos++) {

						if (partitioningInfo.assignStatusList_
							[entryPos + currentPos] != PARTITION_STATUS_DROP_START) {

							partitioningInfo.assignStatusList_
								[entryPos + currentPos] = targetStatus;

							partitioningInfo.activeContainerCount_--;
							updatePartitioningInfo = true;
						}
					}
				}
			}
		}
										break;

		case INDEX_STATUS_DROP_START:
		case INDEX_STATUS_DROP_END: {

			decodeLargeRow(
				NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
				alloc,
				txn,
				ds,
				request.optional_.get<Options::DB_NAME>(),
				container,
				tablePartitioningIndexInfo,
				emNow);

			TablePartitioningIndexInfoEntry* entry;
			const NameWithCaseSensitivity indexName(
				indexInfo.indexName_.c_str(),
				getCaseSensitivity(request).isIndexNameCaseSensitive());

			entry = tablePartitioningIndexInfo.find(indexName);

			if (targetStatus == INDEX_STATUS_DROP_START &&
				request.optional_.get<Options::CREATE_DROP_INDEX_MODE>() !=
				INDEX_MODE_SQL_EXISTS) {

				if (entry == NULL) {
					const FullContainerKey containerKey
						= container->getContainerKey(txn);
					util::String* containerNameString =
						ALLOC_NEW(txn.getDefaultAllocator())
						util::String(txn.getDefaultAllocator());

					containerKey.toString(
						txn.getDefaultAllocator(), *containerNameString);

					GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_INDEX_NOT_EXISTS,
						"Specified table '" << *containerNameString <<
						"', index '" << indexInfo.indexName_ << "' not exists");
				}
			}
			else {
				if (entry != NULL) {
					const int32_t removePos = static_cast<int32_t>(entry->pos_);
					tablePartitioningIndexInfo.remove(removePos);

					partitioningInfo.incrementTablePartitioningVersionId();
					updatePartitioningInfo = true;
					updateIndexInfo = true;
				}
			}
		}
								  break;
		}

		util::XArray<ColumnInfo> columnInfoList(alloc);
		NoSQLUtils::makeLargeContainerColumn(columnInfoList);

		if (updatePartitioningInfo || updateIndexInfo) {
			util::XArray<const util::XArray<uint8_t>*>
				logRecordList(alloc);

			if (updatePartitioningInfo) {

				putLargeRows(
					NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
					alloc,
					txn,
					ds,
					this,
					partitionTable_,
					transactionManager_,
					request,
					columnInfoList,
					logRecordList,
					keyStoreValue,
					noClient,
					partitioningInfo);
			}

			if (updateIndexInfo) {
				putLargeRows(
					NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
					alloc,
					txn,
					ds,
					this,
					partitionTable_,
					transactionManager_,
					request,
					columnInfoList,
					logRecordList,
					keyStoreValue,
					noClient,
					tablePartitioningIndexInfo);
			}

			SimpleOutputMessage outMes(request.fixed_.cxtSrc_.stmtId_,
				TXN_STATEMENT_SUCCESS, getReplyOption(alloc, request, NULL, false), NULL);
			const bool ackWait = executeReplication(
				request,
				ec,
				alloc,
				ev.getSenderND(),
				txn,
				ev.getType(),
				request.fixed_.cxtSrc_.stmtId_,
				transactionManager_->getReplicationMode(),
				NULL,
				0,
				logRecordList.data(),
				logRecordList.size(),
				&outMes);

			replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
				request, outMes, ackWait);

			if (!isNewSQL(request)) {

				ConnectionOption& connOption
					= ev.getSenderND().getUserData<ConnectionOption>();

				Event request(
					ec, UPDATE_TABLE_CACHE, IMMEDIATE_PARTITION_ID);
				EventByteOutStream out = request.getOutStream();
				out << connOption.dbId_;
				StatementHandler::encodeStringData<EventByteOutStream>(
					out, connOption.dbName_.c_str());

				const FullContainerKey containerKey
					= container->getContainerKey(txn);
				util::String containerName(txn.getDefaultAllocator());
				containerKey.toString(
					txn.getDefaultAllocator(), containerName);

				StatementHandler::encodeStringData<EventByteOutStream>(
					out, containerName.c_str());
				int64_t versionId = MAX_TABLE_PARTITIONING_VERSIONID;
				out << versionId;

				EventEngine* ee = sqlService_->getEE();
				for (int32_t pos = 0;
					pos < partitionTable_->getNodeNum(); pos++) {

					const NodeDescriptor& nd = ee->getServerND(pos);
					try {
						ee->send(request, nd);
					}
					catch (std::exception&) {
					}
				}
			}
		}
		else {
			replySuccess(
				ec,
				alloc,
				ev.getSenderND(),
				ev.getType(),
				TXN_STATEMENT_SUCCESS,
				request,
				response,
				false);
		}
	}
	catch (std::exception& e) {
		try {
			handleError(ec, alloc, ev, request, e);
		}
		catch (std::exception& e2) {
			UTIL_TRACE_EXCEPTION(SQL_SERVICE, e2, "");
		}
	}
}

void CreateLargeIndexHandler::operator()(
	EventContext& ec, Event& ev) {

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow
		= ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {
		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		EVENT_START(ec, ev, transactionManager_, connOption.dbId_, false);

		const uint64_t numRow = 1;
		RowData rowData(alloc);
		IndexInfo indexInfo = inMes.indexInfo_;
		bool isComposite = inMes.isComposite_;

		const NameWithCaseSensitivity indexName(
			indexInfo.indexName_.c_str(), false);

		util::Vector<util::String>& indexColumnNameList = inMes.indexColumnNameList_;
		util::Vector<ColumnId>& indexColumnIdList
			= indexInfo.columnIds_;
		util::String& indexColumnNameStr = inMes.indexColumnNameStr_;

		const ClusterRole clusterRole
			= (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;

		checkAuthentication(ev.getSenderND(), emNow);  

		checkConsistency(
			ev.getSenderND(), IMMEDIATE_CONSISTENCY);

		checkTransactionTimeout(
			emNow,
			ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_,
			ev.getQueueingCount());

		checkExecutable(
			request.fixed_.pId_,
			clusterRole, partitionRole,
			partitionStatus,
			partitionTable_);

		EmptyAllowedKey::validate(
			KeyConstraint::getNoLimitKeyConstraint(),
			indexInfo.indexName_.c_str(),
			static_cast<uint32_t>(indexInfo.indexName_.size()),
			"indexName");

		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn = transactionManager_->put(
			alloc,
			request.fixed_.pId_,
			request.fixed_.clientId_,
			request.fixed_.cxtSrc_,
			now,
			emNow);
		txn.setAuditInfo(&ev, &ec, NULL);

		uint8_t existIndex = 0;
		bool noClient = false;

		KeyDataStore* keyStore = getKeyDataStore(txn.getPartitionId());
		KeyDataStoreValue keyStoreValue = keyStore->get(alloc, txn.getContainerId());
		DataStoreV4* ds = static_cast<DataStoreV4*>(getDataStore(txn.getPartitionId(), keyStoreValue.storeType_));
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);

		DSInputMes input(alloc, DS_GET_CONTAINER_OBJECT, ANY_CONTAINER);
		StackAllocAutoPtr<DSContainerOutputMes> ret(alloc, static_cast<DSContainerOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		BaseContainer* container = ret.get()->container_;
		checkContainerSchemaVersion(
			container, request.fixed_.schemaVersionId_);

		VersionInfo versionInfo;
		TablePartitioningInfo<util::StackAllocator>
			partitioningInfo(alloc);

		decodeLargeRow(
			NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
			alloc,
			txn,
			ds,
			request.optional_.get<Options::DB_NAME>(),
			container,
			partitioningInfo,
			emNow);

		util::XArray<uint8_t> containerSchema(alloc);
		decodeLargeBinaryRow(
			NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
			alloc,
			txn,
			ds,
			request.optional_.get<Options::DB_NAME>(),
			container,
			containerSchema,
			emNow);

		util::Vector<ColumnType> indexColumnTypeList(alloc);
		util::Vector<uint8_t> indexColumnOptionList(alloc);
		NoSQLUtils::makeContainerColumns(
			alloc,
			containerSchema,
			indexColumnNameList,
			indexColumnTypeList,
			indexColumnOptionList);

		const NameWithCaseSensitivity columnName(
			indexColumnNameStr.c_str(),
			getCaseSensitivity(request).isColumnNameCaseSensitive());

		TablePartitioningIndexInfoEntry* entry, * checkEntry;
		TablePartitioningIndexInfo tablePartitioningIndexInfo(alloc);
		bool clientSuccess = false;

		decodeLargeRow(
			NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
			alloc,
			txn,
			ds,
			request.optional_.get<Options::DB_NAME>(),
			container,
			tablePartitioningIndexInfo,
			emNow);

		const FullContainerKey containerKey
			= container->getContainerKey(txn);
		util::String* containerNameString =
			ALLOC_NEW(txn.getDefaultAllocator())
			util::String(txn.getDefaultAllocator());

		containerKey.toString(
			txn.getDefaultAllocator(), *containerNameString);

		MapType targetIndexType;
		if (isComposite) {
			targetIndexType = MAP_TYPE_BTREE;
		}
		else {
			const DataStoreConfig* dsConfig = sqlService_->
				getExecutionManager()->getManagerSet()->dsConfig_;
			targetIndexType = getAvailableIndex(
				*dsConfig,
				indexInfo.indexName_.c_str(),
				indexColumnTypeList[indexColumnIdList[0]],
				partitioningInfo.containerType_, false);
		}

		if (partitioningInfo.containerType_ == TIME_SERIES_CONTAINER) {

			for (size_t pos = 0;
				pos < indexColumnIdList.size(); pos++) {

				if (indexColumnIdList[pos] == 0) {
					GS_THROW_USER_ERROR(
						GS_ERROR_DS_TIM_CREATEINDEX_ON_ROWKEY, "");
				}
			}
		}

		entry = tablePartitioningIndexInfo.find(indexName);
		const bool isExistsOrNotExistsFlag =
			(request.optional_.get<
				Options::CREATE_DROP_INDEX_MODE>() ==
				INDEX_MODE_SQL_EXISTS);

		if (entry != NULL && !isExistsOrNotExistsFlag) {
			switch (entry->status_) {

			case INDEX_STATUS_CREATE_START: {

				entry = tablePartitioningIndexInfo.find(
					indexName,
					indexColumnIdList,
					targetIndexType);

				if (entry == NULL) {
					GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_INDEX_ALREADY_EXISTS,
						"Specified table '"
						<< containerNameString->c_str() << "', column '"
						<< columnName.name_
						<< "' already exists, and specifed column condition "
						"is not same to original");
				}
				else {
					existIndex = 1;
				}
			}
										  break;
			case INDEX_STATUS_CREATE_END:
			case INDEX_STATUS_DROP_START:
			case INDEX_STATUS_DROP_END: {

				entry = NULL;
				GS_THROW_USER_ERROR(
					GS_ERROR_SQL_DDL_INDEX_ALREADY_EXISTS,
					"Specified table '" << containerNameString->c_str()
					<< "', indexName '" << indexName.name_ << "' already exists");
			}
									  break;
			}
		}

		checkEntry = tablePartitioningIndexInfo.check(
			indexColumnIdList, targetIndexType);
		if (checkEntry) {

			if (checkEntry->status_ == INDEX_STATUS_CREATE_END) {

				if (!isExistsOrNotExistsFlag) {
					GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_INDEX_ALREADY_EXISTS,
						"Specified table '" << containerNameString->c_str() << "', column '"
						<< columnName.name_ << "' already exists");
				}
				else {
					clientSuccess = true;
				}
			}
			else if (checkEntry->status_ == INDEX_STATUS_CREATE_START) {

				entry = tablePartitioningIndexInfo.find(
					indexName,
					indexColumnIdList,
					targetIndexType);

				if (!entry) {

					if (!isExistsOrNotExistsFlag) {
						GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INDEX_ALREADY_EXISTS,
							"Specified table '" << containerNameString->c_str() << "', column '"
							<< columnName.name_ << "' already exists");
					}
					else {
						clientSuccess = true;
						existIndex = 1;
					}
				}
			}
		}

		if (existIndex != 1) {

			entry = tablePartitioningIndexInfo.add(
				indexName,
				indexColumnIdList,
				targetIndexType);

			entry->status_ = INDEX_STATUS_CREATE_START;
			partitioningInfo.incrementTablePartitioningVersionId();
		}

		util::XArray<const util::XArray<uint8_t>*> logRecordList(alloc);

		if (!clientSuccess) {

			util::XArray<ColumnInfo> columnInfoList(alloc);
			NoSQLUtils::makeLargeContainerColumn(columnInfoList);

			if (existIndex != 1) {

				putLargeRows(
					NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
					alloc,
					txn,
					ds,
					this,
					partitionTable_,
					transactionManager_,
					request,
					columnInfoList,
					logRecordList,
					keyStoreValue,
					noClient,
					partitioningInfo);
			}

			putLargeRows(
				NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
				alloc,
				txn,
				ds,
				this,
				partitionTable_,
				transactionManager_,
				request,
				columnInfoList,
				logRecordList,
				keyStoreValue,
				noClient,
				tablePartitioningIndexInfo);
		}

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false), existIndex);

		const bool ackWait = executeReplication(
			request,
			ec,
			alloc,
			ev.getSenderND(),
			txn,
			ev.getType(),
			request.fixed_.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(),
			NULL,
			0,
			logRecordList.data(),
			logRecordList.size(),
			&outMes);

		replySuccess(
			ec,
			alloc,
			ev.getSenderND(), ev.getType(),
			request,
			outMes,
			ackWait);


		transactionService_->incrementWriteOperationCount(
			ev.getPartitionId());
		transactionService_->addRowWriteCount(
			ev.getPartitionId(), numRow);
	}
	catch (std::exception& e) {
		try {
			handleError(ec, alloc, ev, request, e);
		}
		catch (std::exception& e2) {
			UTIL_TRACE_EXCEPTION(SQL_SERVICE, e2, "");
		}
	}
}

void UpdateTableCacheHandler::operator()(EventContext& ec, Event& ev) {
	try {
		util::StackAllocator& alloc = ec.getAllocator();
		EventByteInStream in(ev.getInStream());
		DatabaseId dbId;
		util::String dbName(alloc);
		util::String tableName(alloc);
		TablePartitioningVersionId versionId;
		in >> dbId;
		if (in.base().remaining() == 0) {
			transactionManager_->getDatabaseManager().drop(dbId);
			return;
		}
		decodeStringData<util::ByteStream<util::ArrayInStream>, util::String>(in, dbName);
		decodeStringData<util::ByteStream<util::ArrayInStream>, util::String>(in, tableName);
		in >> versionId;
		DBConnection* conn = executionManager_->getDBConnection();
		if (conn) {
			NoSQLStore* store = conn->getNoSQLStore(dbId, dbName.c_str());
			if (store) {
				if (versionId == UNDEF_TABLE_PARTITIONING_VERSIONID) {
					store->removeCache(tableName.c_str());
				}
				else {
					SQLString key(executionManager_->getVarAllocator());
					NoSQLUtils::normalizeString(key, tableName.c_str());
					store->setDiffLoad<SQLString>(key);
				}
			}
		}
	}
	catch (std::exception& e) {
		UTIL_TRACE_EXCEPTION(SQL_SERVICE, e, "");
	}
}

void SQLRequestNoSQLClientHandler::executeRepair(EventContext& ec,
	SQLExecution* execution, util::String& tableNameStr,
	ContainerId largeContainerId, SchemaVersionId versionId,
	NodeAffinityNumber affinity) {
	util::StackAllocator& alloc = ec.getAllocator();
	NameWithCaseSensitivity tableName(tableNameStr.c_str(), false);
	DBConnection* conn = executionManager_->getDBConnection();
	NoSQLStore* command = conn->getNoSQLStore(
		execution->getContext().getDBId(), execution->getContext().getDBName());
	NoSQLContainer largeContainer(ec, tableName, execution->getContext().getSyncContext(), execution);
	NoSQLStoreOption option(execution);
	command->getContainer(largeContainer, false, option);
	if (!largeContainer.isExists()) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_TABLE_NOT_EXISTS,
			"Specified table '" << tableName.name_ << "' is already dropped");
	}
	if (largeContainer.getContainerId() == largeContainerId
		&& largeContainer.getSchemaVersionId() == versionId) {
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_DDL_TABLE_NOT_EXISTS,
			"Specified table '" << tableName.name_ << "' is already dropped");
	}

	TablePartitioningInfo<util::StackAllocator> partitioningInfo(alloc);
	util::XArray<ColumnInfo> largeColumnInfoList(alloc);
	NoSQLUtils::makeLargeContainerColumn(largeColumnInfoList);
	command->getLargeRecord<TablePartitioningInfo<util::StackAllocator> >(
		alloc, NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
		largeContainer, largeColumnInfoList, partitioningInfo, option);
	TablePartitioningIndexInfo tablePartitioningIndexInfo(alloc);
	command->getLargeRecord<TablePartitioningIndexInfo>(
		alloc, NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
		largeContainer, largeColumnInfoList, tablePartitioningIndexInfo, option);
	util::XArray<uint8_t> containerSchema(alloc);
	command->getLargeBinaryRecord(
		alloc, NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
		largeContainer, largeColumnInfoList, containerSchema, option);
	NoSQLContainer* subContainer = createNoSQLContainer(ec,
		tableName, largeContainerId, affinity, execution);
	TableExpirationSchemaInfo info;
	partitioningInfo.checkTableExpirationSchema(info, affinity, pt_->getPartitionNum());
	createSubContainer(info, alloc, command, largeContainer,
		subContainer, containerSchema, partitioningInfo, tablePartitioningIndexInfo,
		tableName, pt_->getPartitionNum(), affinity);
}


void SQLRequestNoSQLClientHandler::operator()(EventContext& ec, Event& ev) {
	util::StackAllocator& alloc = ec.getAllocator();
	util::StackAllocator::Scope scope(alloc);
	PartitionId replyPId = UNDEF_PARTITIONID;
	ClientId clientId;
	StatementId stmtId = UNDEF_STATEMENTID;
	bool isSetted = false;

	try {
		EventByteInStream in(ev.getInStream());
		util::String tableName(alloc);
		ContainerId largeContainerId;
		SchemaVersionId versionId;
		NodeAffinityNumber affinityNumber;
		in.readAll(clientId.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
		StatementHandler::decodeStringData<util::ByteStream<util::ArrayInStream>, util::String>(in, tableName);
		in >> largeContainerId;
		in >> versionId;
		in >> affinityNumber;
		in >> replyPId;
		in >> stmtId;
		isSetted = true;
		SQLExecutionManager::Latch latch(clientId, executionManager_);
		SQLExecution* execution = latch.get();
		if (execution == NULL) {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_CANCELLED, "");
		}
		executeRepair(ec, execution, tableName, largeContainerId, versionId, affinityNumber);

		Event response(ec, UPDATE_CONTAINER_STATUS, replyPId);
		EventByteOutStream out = encodeCommonPart(response, stmtId, TXN_STATEMENT_SUCCESS);
		out << static_cast<LargeContainerStatusType>(PARTITION_STATUS_NONE);
		out << static_cast<NodeAffinityNumber>(UNDEF_NODE_AFFINITY_NUMBER);
		EventEngine* ee = txnSvc_->getEE();
		ee->send(response, execution->getContext().getClientNd());

		executionManager_->remove(ec, clientId, false, 60, NULL);
	}
	catch (std::exception& e) {

		try {
			if (isSetted) {
				const util::Exception checkException = GS_EXCEPTION_CONVERT(e, "");

				Event response(ec, UPDATE_CONTAINER_STATUS, replyPId);
				encodeCommonPart(response, stmtId, TXN_STATEMENT_SUCCESS);
				EventEngine* ee = txnSvc_->getEE();
				SQLExecutionManager::Latch latch(clientId, executionManager_);
				SQLExecution* execution = latch.get();
				if (execution) {
					ee->send(response, execution->getContext().getClientNd());
					executionManager_->remove(ec, clientId, false, 61, NULL);
				}
			}
		}
		catch (std::exception& e) {
			UTIL_TRACE_EXCEPTION_WARNING(SQL_SERVICE, e, "");
		}
	}
}

void SQLService::checkVersion(int32_t versionId) {
	if (!(SQL_MSG_BACKWARD_COMPATIBLE ?
			versionId <= SQL_MSG_VERSION :
			versionId == SQL_MSG_VERSION)) {
		GS_THROW_CUSTOM_ERROR(DenyException,
			GS_ERROR_SQL_MSG_VERSION_NOT_ACCEPTABLE,
			"(receiveVersion=" << versionId << ", acceptableVersion=" << SQL_MSG_VERSION);
	}
}


void QueryProcessor::assignDistributedTarget(
	TransactionContext& txn, BaseContainer& container,
	const Query& query, ResultSet& resultSet) {
	if (resultSet.getLargeInfo() == NULL || static_cast<const ResultSet&>(
		resultSet).getDistributedTarget() != NULL) {
		return;
	}

	util::StackAllocator& alloc = txn.getDefaultAllocator();

	util::Vector< std::pair<
		TupleList::TupleColumnType, util::String> > columnInfoList(alloc);

	const util::String emptyColumnName(alloc);
	const uint32_t columnCount = container.getColumnNum();
	for (uint32_t i = 0; i < columnCount; i++) {
		const ColumnInfo& columnInfo = container.getColumnInfo(i);
		TupleList::TupleColumnType columnType =
			convertNoSQLTypeToTupleType(
				columnInfo.getColumnType());
		if (!TupleColumnTypeUtils::isAny(columnType) && !columnInfo.isNotNull()) {
			columnType |= TupleList::TYPE_MASK_NULLABLE;
		}
		columnInfoList.push_back(std::make_pair(
			columnType, emptyColumnName));
	}

	SQLExecution::assignDistributedTarget(
		txn, columnInfoList, query, resultSet);
}

FullContainerKey* QueryForMetaContainer::predicateToContainerKey(
		TransactionContext &txn, DataStoreV4 &dataStore,
		const Query &query, DatabaseId dbId, ContainerId metaContainerId,
		PartitionId partitionCount, util::String &dbNameStr,
		bool &fullReduced, PartitionId &reducedPartitionId) {
	return SQLExecution::predicateToContainerKeyByTQL(
			txn, dataStore, query, dbId, metaContainerId, partitionCount,
			dbNameStr, fullReduced, reducedPartitionId);
}

void SQLService::Config::setUpConfigHandler(SQLService* sqlSvc, ConfigTable& configTable) {
	sqlSvc_ = sqlSvc;
	configTable.setParamHandler(
		CONFIG_TABLE_SQL_TRACE_LIMIT_EXECUTION_TIME, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SQL_TRACE_LIMIT_QUERY_SIZE, *this);

	configTable.setParamHandler(
		CONFIG_TABLE_SQL_SEND_PENDING_INTERVAL, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SQL_SEND_PENDING_TASK_LIMIT, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SQL_SEND_PENDING_JOB_LIMIT, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SQL_JOB_TOTAL_MEMORY_LIMIT, *this);

	configTable.setParamHandler(
		CONFIG_TABLE_SQL_NOSQL_FAILOVER_TIMEOUT, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SQL_MULTI_INDEX_SCAN, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SQL_PARTITIONING_ROWKEY_CONSTRAINT, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SQL_COST_BASED_JOIN_DRIVING, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SQL_COST_BASED_JOIN_EXPLICIT, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SQL_ENABLE_PROFILER, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SQL_ENABLE_JOB_MEMORY_CHECK_INTERVAL_COUNT, *this);

	configTable.setParamHandler(
		CONFIG_TABLE_SQL_TABLE_CACHE_EXPIRED_TIME, *this);

	configTable.setParamHandler(
		CONFIG_TABLE_SQL_TABLE_CACHE_SIZE, *this);

	configTable.setParamHandler(
		CONFIG_TABLE_SQL_TABLE_CACHE_LOAD_DUMP_LIMIT_TIME, *this);

	configTable.setParamHandler(
		CONFIG_TABLE_SQL_ENABLE_SCAN_META_TABLE_ADMINISTRATOR_PRIVILEGES, *this);

	configTable.setParamHandler(
		CONFIG_TABLE_SQL_ADD_BATCH_MAX_COUNT, *this);

	configTable.setParamHandler(
		CONFIG_TABLE_SQL_TABLE_PARTITIONING_MAX_ASSIGN_NUM, *this);

	configTable.setParamHandler(
		CONFIG_TABLE_SQL_TABLE_PARTITIONING_MAX_ASSIGN_ENTRY_NUM, *this);

	configTable.setParamHandler(
			CONFIG_TABLE_SQL_TOTAL_MEMORY_LIMIT, *this);
	configTable.setParamHandler(
			CONFIG_TABLE_SQL_WORK_MEMORY_RATE, *this);
	configTable.setParamHandler(
			CONFIG_TABLE_SQL_FAIL_ON_TOTAL_MEMORY_LIMIT, *this);
	configTable.setParamHandler(
			CONFIG_TABLE_SQL_RESOURCE_CONTROL_LEVEL, *this);
}

void SQLService::Config::operator()(
	ConfigTable::ParamId id, const ParamValue& value) {
	switch (id) {
	case CONFIG_TABLE_SQL_TRACE_LIMIT_EXECUTION_TIME:
		sqlSvc_->setTraceLimitTime(value.get<int32_t>());
		break;
	case CONFIG_TABLE_SQL_TRACE_LIMIT_QUERY_SIZE:
		sqlSvc_->setTraceLimitQuerySize(value.get<int32_t>());
		break;
	case CONFIG_TABLE_SQL_SEND_PENDING_INTERVAL:
		sqlSvc_->setSendPendingInterval(value.get<int32_t>());
		break;
	case CONFIG_TABLE_SQL_SEND_PENDING_TASK_LIMIT:
		sqlSvc_->setSendPendingTaskLimit(value.get<int32_t>());
		break;
	case CONFIG_TABLE_SQL_SEND_PENDING_JOB_LIMIT:
		sqlSvc_->setSendPendingJobLimit(value.get<int32_t>());
		break;
	case CONFIG_TABLE_SQL_ENABLE_PROFILER:
		sqlSvc_->enableProfiler(value.get<bool>());
		break;

	case CONFIG_TABLE_SQL_ENABLE_JOB_MEMORY_CHECK_INTERVAL_COUNT:
		sqlSvc_->setMemoryCheckCount(value.get<int32_t>());
		break;

	case CONFIG_TABLE_SQL_JOB_TOTAL_MEMORY_LIMIT:
		sqlSvc_->setJobMemoryLimit(ConfigTable::megaBytesToBytes(
			static_cast<uint32_t>(value.get<int32_t>())));
		break;
	case CONFIG_TABLE_SQL_NOSQL_FAILOVER_TIMEOUT:
		sqlSvc_->setNoSQLFailoverTimeout(value.get<int32_t>());
		break;
	case CONFIG_TABLE_SQL_PARTITIONING_ROWKEY_CONSTRAINT:
		sqlSvc_->getExecutionManager()->getSQLConfig().
			setPartitioningRowKeyConstraint(value.get<bool>());
		break;
	case CONFIG_TABLE_SQL_COST_BASED_JOIN_DRIVING:
		sqlSvc_->getExecutionManager()->getSQLConfig().setCostBasedJoinDriving(
				value.get<bool>());
		break;
	case CONFIG_TABLE_SQL_COST_BASED_JOIN_EXPLICIT:
		sqlSvc_->getExecutionManager()->getSQLConfig().setCostBasedJoin(
				value.get<bool>());
		break;
	case CONFIG_TABLE_SQL_TABLE_CACHE_EXPIRED_TIME:
		sqlSvc_->setTableSchemaExpiredTime(value.get<int32_t>());
		break;
	case CONFIG_TABLE_SQL_TABLE_CACHE_LOAD_DUMP_LIMIT_TIME:
		sqlSvc_->getExecutionManager()->setTableCacheDumpLimitTime(value.get<int32_t>());
		break;
	case CONFIG_TABLE_SQL_ADD_BATCH_MAX_COUNT:
		sqlSvc_->setAddBatchMaxCount(value.get<int32_t>());
		break;
	case CONFIG_TABLE_SQL_TABLE_PARTITIONING_MAX_ASSIGN_NUM:
		sqlSvc_->setTablePartitioningMaxAssignedNum(value.get<int32_t>());
		break;
	case CONFIG_TABLE_SQL_TABLE_PARTITIONING_MAX_ASSIGN_ENTRY_NUM:
		sqlSvc_->setTablePartitioningMaxAssignedEntryNum(value.get<int32_t>());
		break;

	case CONFIG_TABLE_SQL_TOTAL_MEMORY_LIMIT:
		sqlSvc_->setTotalMemoryLimit(ConfigTable::megaBytesToBytes(
			static_cast<uint32_t>(std::max(value.get<int32_t>(), 0))));
		break;
	case CONFIG_TABLE_SQL_WORK_MEMORY_RATE:
		sqlSvc_->setWorkMemoryLimitRate(value.get<double>());
		break;
	case CONFIG_TABLE_SQL_FAIL_ON_TOTAL_MEMORY_LIMIT:
		sqlSvc_->setFailOnTotalMemoryLimit(value.get<bool>());
		break;
	case CONFIG_TABLE_SQL_RESOURCE_CONTROL_LEVEL:
		sqlSvc_->setResourceControlLevel(value.get<int32_t>());
		break;
	}
}

void SQLService::checkActiveStatus() {
	const StatementHandler::ClusterRole clusterRole =
		(StatementHandler::CROLE_MASTER | StatementHandler::CROLE_FOLLOWER);
	StatementHandler::checkExecutable(clusterRole, pt_);
	checkNodeStatus();
}

void SQLService::setTotalMemoryLimit(uint64_t limit) {
	totalMemoryLimit_ = limit;
	applyTotalMemoryLimit();
}

void SQLService::setWorkMemoryLimitRate(double rate) {
	workMemoryLimitRate_ = doubleToBits(rate);
	applyTotalMemoryLimit();
}

void SQLService::setFailOnTotalMemoryLimit(bool enabled) {
	failOnTotalMemoryLimit_ = enabled;
	applyFailOnTotalMemoryLimit();
}

void SQLService::setResourceControlLevel(int32_t level) {
	resourceControlLevel_ = level;
	applyFailOnTotalMemoryLimit();
}

void SQLService::setUpMemoryLimits() {
	applyTotalMemoryLimit();
	applyFailOnTotalMemoryLimit();
}

void SQLService::applyTotalMemoryLimit() {
	const uint64_t totalLimit = resolveTotalMemoryLimit();

	util::AllocatorLimitter *limitter = getTotalMemoryLimitter();
	assert(limitter != NULL);
	limitter->setLimit(totalLimit);
}

void SQLService::applyFailOnTotalMemoryLimit() {
	const int32_t level = resolveResourceControlLevel();
	const bool enabled = failOnTotalMemoryLimit_;
	const bool enabledActual = enabled && isTotalMemoryLimittable(level);
	workMemoryLimitter_.setFailOnExcess(enabledActual);

	if (enabled && !enabledActual) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_CONFIG_INVALID,
				"Unaccptable resource control level "
				"to fail on total memory limit (" <<
				"failOnTotalMemoryLimit=true"
				", resourceControlLevel=" << level << ")");
	}
}

size_t SQLService::resolveTotalMemoryLimit() {
	const uint64_t baseTotalLimit = totalMemoryLimit_;

	uint64_t totalLimit;
	if (baseTotalLimit == 0) {
		const uint64_t storeLimit = storeMemoryLimit_;

		const uint64_t uint64Max = std::numeric_limits<uint64_t>::max();
		const uint64_t workMax = uint64Max - storeLimit;

		const uint32_t conc = pgConfig_.getPartitionGroupCount();
		const uint64_t workLimit = std::min(workMemoryLimit_, workMax / conc);

		const double workTotalRate = std::max(std::min(
				bitsToDouble(workMemoryLimitRate_),
				static_cast<double>(workMax / (conc * workLimit))), 0.0);

		const uint64_t workTotalLimit = static_cast<uint64_t>(
				static_cast<double>(workLimit) * workTotalRate *
				static_cast<double>(conc));
		totalLimit = storeLimit + workTotalLimit;
	}
	else {
		totalLimit = baseTotalLimit;
	}

	const uint64_t sizeMax = std::numeric_limits<size_t>::max();
	return static_cast<uint64_t>(std::min(totalLimit, sizeMax));
}

util::AllocatorLimitter* SQLService::resolveTotalMemoryLimitter() {
	util::AllocatorManager &manager = util::AllocatorManager::getDefaultInstance();

	const util::AllocatorGroupId mainGroup = ALLOCATOR_GROUP_SQL_WORK;
	util::AllocatorLimitter *limitter = manager.getGroupLimitter(mainGroup);

	if (limitter == NULL) {
		const util::AllocatorGroupId rootGroup =
				util::AllocatorManager::GROUP_ID_ROOT;
		const util::AllocatorGroupId relatedGroups[] = {
			ALLOCATOR_GROUP_SQL_MESSAGE,
			ALLOCATOR_GROUP_SQL_LTS,
			ALLOCATOR_GROUP_SQL_JOB,
			rootGroup
		};

		const util::AllocatorManager::LimitType limitType =
				util::AllocatorManager::LIMIT_GROUP_TOTAL_SIZE;

		util::AllocatorManager &manager =
				util::AllocatorManager::getDefaultInstance();

		manager.setLimit(
				mainGroup, limitType, std::numeric_limits<size_t>::max());
		for (const util::AllocatorGroupId *it = relatedGroups;
				*it != rootGroup; it++) {
			manager.setSharingLimitterGroup(*it, mainGroup);
		}
		limitter = manager.getGroupLimitter(mainGroup);
		assert(limitter != NULL);

		limitter->setErrorHandler(&memoryLimitErrorHandler_);
	}

	return limitter;
}

uint64_t SQLService::doubleToBits(double src) {
	uint64_t dest;
	memcpy(&dest, &src, sizeof(uint64_t));
	return dest;
}

double SQLService::bitsToDouble(uint64_t src) {
	double dest;
	memcpy(&dest, &src, sizeof(uint64_t));
	return dest;
}

void BindParam::dump(util::StackAllocator& alloc) {
	util::NormalOStringStream os;
	switch (value_.getType()) {
	case TupleList::TYPE_LONG:
		os << value_.get<int64_t>();
		break;
	case TupleList::TYPE_DOUBLE:
		os << value_.get<double>();
		break;
	case TupleList::TYPE_STRING:
		os << "'";
		os.write(
			static_cast<const char8_t*>(value_.varData()),
			static_cast<std::streamsize>(value_.varSize()));
		os << "'";
		break;
	case TupleList::TYPE_INTEGER:
	{
		int32_t i;
		memcpy(&i, value_.fixedData(), sizeof(int32_t));
		os << i;
		break;
	}
	case TupleList::TYPE_SHORT:
	{
		int16_t i;
		memcpy(&i, value_.fixedData(), sizeof(int16_t));
		os << i;
		break;
	}
	case TupleList::TYPE_BYTE:
	{
		int8_t i;
		memcpy(&i, value_.fixedData(), sizeof(int8_t));
		os << static_cast<int32_t>(i);
		break;
	}
	case TupleList::TYPE_BOOL:
	{
		int8_t i;
		memcpy(&i, value_.fixedData(), sizeof(int8_t));
		os << "(" << static_cast<int32_t>(i) << ")";
		break;
	}
	case TupleList::TYPE_FLOAT:
	{
		float f;
		memcpy(&f, value_.fixedData(), sizeof(float));
		os << f;
		break;
	}
	case TupleList::TYPE_TIMESTAMP:
	{
		os << value_.get<int64_t>();
		break;
	}
	case TupleList::TYPE_MICRO_TIMESTAMP:
	{
		os << value_.get<MicroTimestamp>().value_;
		break;
	}
	case TupleList::TYPE_NANO_TIMESTAMP:
	{
		NanoTimestamp tsValue = value_.get<TupleNanoTimestamp>();
		os << static_cast<uint32_t>(tsValue.getHigh()) << " " <<
				static_cast<uint32_t>(tsValue.getLow());
		break;
	}
	case TupleList::TYPE_BLOB:
	{
		const char temp = 0;
		const size_t MAX_DUMP_BLOB_SIZE = 10;
		util::NormalIStringStream iss;
		TupleValue::LobReader reader(value_);
		const uint64_t count = reader.getPartCount();
		os << "[" << count << "]0x";
		const void* data;
		size_t size;
		if (reader.next(data, size)) {
			const size_t dumpSize = std::min(size, MAX_DUMP_BLOB_SIZE);
			util::XArray<char> buffer(alloc);
			buffer.assign(dumpSize * 2 + 1, temp);
			const char* in = static_cast<const char*>(data);
			size_t strSize = util::HexConverter::encode(
				buffer.data(), in, dumpSize, false);
			util::String hexStr(buffer.data(), strSize, alloc);
			os << hexStr.c_str();
			if (dumpSize < size || count > 1) {
				os << " ...";
			}
		}
		break;
	}
	case TupleList::TYPE_NULL:
		os << "NULL";
		break;
	default:
		os << "(other type)";
		break;
	}
	std::cout << os.str().c_str() << std::endl;
}

