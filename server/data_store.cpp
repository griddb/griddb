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
	@brief Implementation of DataStore
*/
#include "data_store.h"
#include "collection.h"
#include "data_store_common.h"
#include "message_schema.h"
#include "object_manager.h"
#include "query_processor.h"
#include "result_set.h"
#include "time_series.h"
#include "transaction_manager.h"

#include "cluster_service.h"

#include "resource_set.h"

#include "picojson.h"
#include <fstream>


#define TEST_PRINT(s)
#define TEST_PRINT1(s, d)





const uint32_t DataStoreValueLimitConfig::LIMIT_SMALL_SIZE_LIST[12] = {
	15 * 1024, 31 * 1024, 63 * 1024, 127 * 1024, 128 * 1024, 128 * 1024,
	128 * 1024, 128 * 1024, 128 * 1024, 128 * 1024, 128 * 1024, 128 * 1024};
const uint32_t DataStoreValueLimitConfig::LIMIT_BIG_SIZE_LIST[12] = {
	1024 * 1024 * 1024 - 1, 1024 * 1024 * 1024 - 1, 1024 * 1024 * 1024 - 1,
	1024 * 1024 * 1024 - 1, 1024 * 1024 * 1024 - 1, 1024 * 1024 * 1024 - 1,
	1024 * 1024 * 1024 - 1, 1024 * 1024 * 1024 - 1, 1024 * 1024 * 1024 - 1,
	1024 * 1024 * 1024 - 1, 1024 * 1024 * 1024 - 1, 1024 * 1024 * 1024 - 1};
const uint32_t DataStoreValueLimitConfig::LIMIT_ARRAY_NUM_LIST[12] = {
	2000, 4000, 8000, 16000, 32000, 65000,
	65000, 65000, 65000, 65000, 65000, 65000};
const uint32_t DataStoreValueLimitConfig::LIMIT_COLUMN_NUM_LIST[12] = {
	512, 1024, 1024, 1000*8, 1000*16, 1000*32,
	1000*32, 1000*32, 1000*32, 1000*32, 1000*32, 1000*32};
const uint32_t DataStoreValueLimitConfig::LIMIT_INDEX_NUM_LIST[12] = {
	512, 1024, 1024, 1000*4, 1000*8, 1000*16,
	1000*16, 1000*16, 1000*16, 1000*16, 1000*16, 1000*16};
const uint32_t DataStoreValueLimitConfig::LIMIT_CONTAINER_NAME_SIZE_LIST[12] = {
	8 * 1024, 16 * 1024, 32 * 1024, 64 * 1024, 128 * 1024, 128 * 1024,
	128 * 1024, 128 * 1024, 128 * 1024, 128 * 1024, 128 * 1024, 128 * 1024};

DataStoreValueLimitConfig::DataStoreValueLimitConfig(
	const ConfigTable &configTable) {
	int32_t chunkExpSize = util::nextPowerBitsOf2(
		configTable.getUInt32(CONFIG_TABLE_DS_STORE_BLOCK_SIZE));
	int32_t nth = chunkExpSize - ChunkManager::MIN_CHUNK_EXP_SIZE_;
	assert(nth < (sizeof(LIMIT_SMALL_SIZE_LIST) / sizeof(LIMIT_SMALL_SIZE_LIST[0])));

	limitSmallSize_ = LIMIT_SMALL_SIZE_LIST[nth];
	limitBigSize_ = LIMIT_BIG_SIZE_LIST[nth];
	limitArrayNumSize_ = LIMIT_ARRAY_NUM_LIST[nth];
	limitColumnNumSize_ = LIMIT_COLUMN_NUM_LIST[nth];
	limitContainerNameSize_ = LIMIT_CONTAINER_NAME_SIZE_LIST[nth];
	limitIndexNumSize_ = LIMIT_INDEX_NUM_LIST[nth];
}



/*!
	@brief Create ResultSet
*/

ResultSet *DataStore::createResultSet(TransactionContext &txn,
	ContainerId containerId, SchemaVersionId schemaVersionId, int64_t emNow,
	ResultSetOption *queryOption, bool noExpire) {
	PartitionGroupId pgId = calcPartitionGroupId(txn.getPartitionId());
	ResultSetId rsId = ++resultSetIdList_[pgId];
	int32_t rsTimeout = txn.getTransationTimeoutInterval();
	const int64_t timeout = emNow + static_cast<int64_t>(rsTimeout) * 1000;
	ResultSet &rs = (noExpire ?
			resultSetMap_[pgId]->createNoExpire(rsId) :
			resultSetMap_[pgId]->create(rsId, timeout));
	rs.setTxnAllocator(&txn.getDefaultAllocator());
	if (resultSetAllocator_[pgId]) {
		rs.setRSAllocator(resultSetAllocator_[pgId]);
		resultSetAllocator_[pgId] = NULL;
	}
	else {
		util::StackAllocator *allocator = UTIL_NEW util::StackAllocator(
			util::AllocatorInfo(ALLOCATOR_GROUP_TXN_RESULT, "resultSet"),
			&resultSetPool_);
		rs.setRSAllocator(allocator);
	}
	if (resultSetRowIdAllocator_[pgId]) {
		rs.setRSRowIdAllocator(resultSetRowIdAllocator_[pgId]);
		resultSetRowIdAllocator_[pgId] = NULL;
	}
	else {
		util::StackAllocator *allocatorRowId = UTIL_NEW util::StackAllocator(
			util::AllocatorInfo(ALLOCATOR_GROUP_TXN_RESULT, "resultSetRowId"),
			&resultSetPool_);
		rs.setRSRowIdAllocator(allocatorRowId);
	}
	if (resultSetSwapAllocator_[pgId]) {
		rs.setRSSwapAllocator(resultSetSwapAllocator_[pgId]);
		resultSetSwapAllocator_[pgId] = NULL;
	}
	else {
		util::StackAllocator *allocatorSwap = UTIL_NEW util::StackAllocator(
			util::AllocatorInfo(ALLOCATOR_GROUP_TXN_RESULT, "resultSetSwap"),
			&resultSetPool_);
		rs.setRSSwapAllocator(allocatorSwap);
	}
	rs.getRSAllocator()->setTotalSizeLimit(rs.getTxnAllocator()->getTotalSizeLimit());
	rs.setMemoryLimit(rs.getTxnAllocator()->getTotalSizeLimit());
	rs.setContainerId(containerId);
	rs.setSchemaVersionId(schemaVersionId);
	rs.setId(rsId);
	rs.setStartLsn(0);
	rs.setTimeoutTime(rsTimeout);
	rs.setPartitionId(txn.getPartitionId());
	rs.setQueryOption(queryOption);
	return &rs;
}

/*!
	@brief Get ResultSet
*/
ResultSet *DataStore::getResultSet(TransactionContext &txn, ResultSetId rsId) {
	ResultSet *rs =
		resultSetMap_[calcPartitionGroupId(txn.getPartitionId())]->get(rsId);
	if (rs) {
		rs->setTxnAllocator(&txn.getDefaultAllocator());
		ResultSetOption &queryOption = rs->getQueryOption();
		getObjectManager()->setSwapOutCounter(txn.getPartitionId(), queryOption.getSwapOutNum());
	}
	return rs;
}

ResultSet* DataStore::updateResultSetTimeout(
		TransactionContext &txn, ResultSetId rsId, int64_t emNow) {
	const int32_t rsTimeout = txn.getTransationTimeoutInterval();
	const int64_t timeout = emNow + static_cast<int64_t>(rsTimeout) * 1000;

	ResultSet* rs = resultSetMap_[
			calcPartitionGroupId(txn.getPartitionId())]->update(rsId, timeout);

	if (rs) {
		rs->setTxnAllocator(&txn.getDefaultAllocator());
	}

	return rs;
}

/*!
	@brief Close ResultSet
*/
void DataStore::closeResultSet(PartitionId pId, ResultSetId rsId) {
	if (rsId == UNDEF_RESULTSETID || pId == UNDEF_PARTITIONID) {
		return;
	}
	PartitionGroupId pgId = calcPartitionGroupId(pId);
	ResultSet *rs = resultSetMap_[pgId]->get(rsId);
	if (rs) {
		closeResultSetInternal(pgId, *rs);
	}
}

/*!
	@brief If ResultSet is no need, then clse , othewise clear temporary memory
*/
void DataStore::closeOrClearResultSet(PartitionId pId, ResultSetId rsId) {
	if (rsId == UNDEF_RESULTSETID || pId == UNDEF_PARTITIONID) {
		return;
	}
	PartitionGroupId pgId = calcPartitionGroupId(pId);
	ResultSet *rs = resultSetMap_[pgId]->get(rsId);
	if (rs) {
		rs->releaseTxnAllocator();
		ResultSetOption &queryOption = rs->getQueryOption();
		queryOption.setSwapOutNum(getObjectManager()->getSwapOutCounter(pId));
	}
	if (rs && (rs->isRelease())) {
		closeResultSetInternal(pgId, *rs);
	}
}

void DataStore::closeResultSetInternal(PartitionGroupId pgId, ResultSet &rs) {
	ResultSetId removeRSId = rs.getId();
	util::StackAllocator *allocator = rs.getRSAllocator();
	util::StackAllocator *allocatorRowId = rs.getRSRowIdAllocator();
	util::StackAllocator *allocatorSwap = rs.getRSSwapAllocator();
	rs.clear();				  
	rs.setRSAllocator(NULL);  
	rs.setRSRowIdAllocator(NULL);  
	rs.setRSSwapAllocator(NULL);   

	util::StackAllocator::Tool::forceReset(*allocator);
	util::StackAllocator::Tool::forceReset(*allocatorRowId);
	util::StackAllocator::Tool::forceReset(*allocatorSwap);
	allocator->setFreeSizeLimit(allocator->base().getElementSize());
	allocatorRowId->setFreeSizeLimit(allocatorRowId->base().getElementSize());
	allocatorSwap->setFreeSizeLimit(allocatorSwap->base().getElementSize());
	allocator->trim();
	allocatorRowId->trim();
	allocatorSwap->trim();

	delete resultSetAllocator_[pgId];
	resultSetAllocator_[pgId] = allocator;
	delete resultSetRowIdAllocator_[pgId];
	resultSetRowIdAllocator_[pgId] = allocatorRowId;
	delete resultSetSwapAllocator_[pgId];
	resultSetSwapAllocator_[pgId] = allocatorSwap;

	resultSetMap_[pgId]->remove(removeRSId);
}

/*!
	@brief Check if ResultSet is timeout
*/
void DataStore::checkTimeoutResultSet(
	PartitionGroupId pgId, int64_t checkTime) {
	ResultSetId rsId = UNDEF_RESULTSETID;
	ResultSetId *rsIdPtr = &rsId;
	ResultSet *rs = resultSetMap_[pgId]->refresh(checkTime, rsIdPtr);
	while (rs) {
		closeResultSetInternal(pgId, *rs);
		rs = resultSetMap_[pgId]->refresh(checkTime, rsIdPtr);
	}
}

void DataStore::addUpdatedRow(PartitionId pId, ContainerId containerId, RowId rowId, OId oId, bool isMvccRow) {
	PartitionGroupId pgId = calcPartitionGroupId(pId);
	util::ExpirableMap<ResultSetId, ResultSet, int64_t, ResultSetIdHash>::Cursor
		rsCursor = resultSetMap_[pgId]->getCursor();
	ResultSet *rs = rsCursor.next();
	while (rs) {
		if (rs->getPartitionId() == pId &&
			rs->getContainerId() == containerId && rs->isPartialExecuteMode()) {
			if (isMvccRow) {
				rs->addUpdatedMvccRow(rowId, oId);
			} else {
				rs->addUpdatedRow(rowId, oId);
			}
		}
		rs = rsCursor.next();
	}
}
void DataStore::addRemovedRow(PartitionId pId, ContainerId containerId, RowId rowId, OId oId) {
	PartitionGroupId pgId = calcPartitionGroupId(pId);
	util::ExpirableMap<ResultSetId, ResultSet, int64_t, ResultSetIdHash>::Cursor
		rsCursor = resultSetMap_[pgId]->getCursor();
	ResultSet *rs = rsCursor.next();
	while (rs) {
		if (rs->getPartitionId() == pId &&
			rs->getContainerId() == containerId && rs->isPartialExecuteMode()) {
			rs->addRemovedRow(rowId, oId);
		}
		rs = rsCursor.next();
	}
}
void DataStore::addRemovedRowArray(PartitionId pId, ContainerId containerId, OId oId) {
	PartitionGroupId pgId = calcPartitionGroupId(pId);
	util::ExpirableMap<ResultSetId, ResultSet, int64_t, ResultSetIdHash>::Cursor
		rsCursor = resultSetMap_[pgId]->getCursor();
	ResultSet *rs = rsCursor.next();
	while (rs) {
		if (rs->getPartitionId() == pId &&
			rs->getContainerId() == containerId && rs->isPartialExecuteMode()) {
			rs->addRemovedRowArray(oId);
		}
		rs = rsCursor.next();
	}
}

void DataStore::addRemovedChunk(PartitionId pId, OId oId) {
	PartitionGroupId pgId = calcPartitionGroupId(pId);
	util::ExpirableMap<ResultSetId, ResultSet, int64_t, ResultSetIdHash>::Cursor
		rsCursor = resultSetMap_[pgId]->getCursor();
	ResultSet *rs = rsCursor.next();
	while (rs) {
		if (rs->getPartitionId() == pId &&
			rs->isPartialExecuteMode()) {
			rs->addRemovedChunk(oId);
		}
		rs = rsCursor.next();
	}
}

void DataStore::forceCloseAllResultSet(PartitionId pId) {
	PartitionGroupId pgId = calcPartitionGroupId(pId);
	util::ExpirableMap<ResultSetId, ResultSet, int64_t, ResultSetIdHash>::Cursor
		rsCursor = resultSetMap_[pgId]->getCursor();
	ResultSet *rs = rsCursor.next();
	while (rs) {
		if (rs->getPartitionId() == pId) {
			closeResultSetInternal(pgId, *rs);
		}
		rs = rsCursor.next();
	}
}

DataStore::DataStore(ConfigTable &configTable, ChunkManager *chunkManager)
	: config_(configTable),
	  PARTITION_HEADER_OID(ObjectManager::getOId(
		  util::nextPowerBitsOf2(
		  configTable.getUInt32(CONFIG_TABLE_DS_STORE_BLOCK_SIZE)),
		  ALLOCATE_META_CHUNK, INITIAL_CHUNK_ID, FIRST_OBJECT_OFFSET)),
	  pgConfig_(configTable),
	  dsValueLimitConfig_(configTable),
	  allocateStrategy_(AllocateStrategy(ALLOCATE_META_CHUNK)),
	  resultSetPool_(
		  util::AllocatorInfo(ALLOCATOR_GROUP_TXN_RESULT, "resultSetPool"),
		  1 << RESULTSET_POOL_BLOCK_SIZE_BITS),
	  resultSetAllocator_(
		  UTIL_NEW util::StackAllocator * [pgConfig_.getPartitionGroupCount()]),
	  resultSetRowIdAllocator_(
		  UTIL_NEW util::StackAllocator * [pgConfig_.getPartitionGroupCount()]),
	  resultSetSwapAllocator_(
		  UTIL_NEW util::StackAllocator * [pgConfig_.getPartitionGroupCount()]),
	  resultSetMapManager_(NULL),
	  resultSetMap_(NULL),
	  containerIdTable_(NULL)
{
	resultSetPool_.setTotalElementLimit(
		ConfigTable::megaBytesToBytes(
			configTable.getUInt32(CONFIG_TABLE_DS_RESULT_SET_MEMORY_LIMIT)) /
		(1 << RESULTSET_POOL_BLOCK_SIZE_BITS));

	const uint32_t rsCacheSize =
			configTable.getUInt32(CONFIG_TABLE_DS_RESULT_SET_CACHE_MEMORY);
	if (rsCacheSize > 0) {
		resultSetPool_.setLimit(
				util::AllocatorStats::STAT_STABLE_LIMIT,
				ConfigTable::megaBytesToBytes(rsCacheSize));
	}
	else {
		resultSetPool_.setFreeElementLimit(0);
	}

	try {
		affinityGroupSize_ =
			configTable.get<int32_t>(CONFIG_TABLE_DS_AFFINITY_GROUP_SIZE);

		uint32_t partitionNum =
			configTable.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM);
		uint32_t partitionGroupNum =
			configTable.getUInt32(CONFIG_TABLE_DS_CONCURRENCY);

		const std::string cpFilePath =
			configTable.get<const char8_t *>(CONFIG_TABLE_DS_DB_PATH);

		objectManager_ = UTIL_NEW ObjectManager(configTable, chunkManager);

		containerIdTable_ = UTIL_NEW ContainerIdTable(partitionNum);

		cpFilePath_ = cpFilePath;
		eventLogPath_ =
			configTable.get<const char8_t *>(CONFIG_TABLE_SYS_EVENT_LOG_PATH);

		dbState_.assign(partitionNum, UNRESTORED);

		resultSetIdList_.reserve(partitionGroupNum);
		resultSetMapManager_ =
			UTIL_NEW util::ExpirableMap<ResultSetId, ResultSet, int64_t,
				ResultSetIdHash>::Manager *
			[partitionGroupNum];
		resultSetMap_ = UTIL_NEW util::ExpirableMap<ResultSetId, ResultSet,
							int64_t, ResultSetIdHash> *
						[partitionGroupNum];
		for (uint32_t i = 0; i < partitionGroupNum; ++i) {
			resultSetIdList_.push_back(1);
			resultSetAllocator_[i] = UTIL_NEW util::StackAllocator(
				util::AllocatorInfo(ALLOCATOR_GROUP_TXN_RESULT, "resultSet"),
				&resultSetPool_);
			resultSetRowIdAllocator_[i] = UTIL_NEW util::StackAllocator(
				util::AllocatorInfo(
					ALLOCATOR_GROUP_TXN_RESULT, "resultSetRowId"),
				&resultSetPool_);
			resultSetSwapAllocator_[i] = UTIL_NEW util::StackAllocator(
				util::AllocatorInfo(
					ALLOCATOR_GROUP_TXN_RESULT, "resultSetSwap"),
				&resultSetPool_);
			resultSetMapManager_[i] = UTIL_NEW util::ExpirableMap<ResultSetId,
				ResultSet, int64_t, ResultSetIdHash>::
				Manager(util::AllocatorInfo(
					ALLOCATOR_GROUP_TXN_RESULT, "resultSetMapManager"));
			resultSetMapManager_[i]->setFreeElementLimit(
				RESULTSET_FREE_ELEMENT_LIMIT);
			resultSetMap_[i] =
				resultSetMapManager_[i]->create(RESULTSET_MAP_HASH_SIZE,
					DS_MAX_RESULTSET_TIMEOUT_INTERVAL * 1000, 1000);
		}
		currentBackgroundList_.assign(partitionGroupNum, BGTask());
		activeBackgroundCount_.assign(partitionNum, 0);
		expirationStat_.assign(partitionNum, ExpirationStat());

		statUpdator_.dataStore_ = this;
	}
	catch (std::exception &e) {
		for (uint32_t i = 0; i < pgConfig_.getPartitionGroupCount(); i++) {
			resultSetMapManager_[i]->remove(resultSetMap_[i]);
			delete resultSetMapManager_[i];
			delete resultSetAllocator_[i];
			delete resultSetRowIdAllocator_[i];
			delete resultSetSwapAllocator_[i];
		}
		delete[] resultSetMapManager_;
		delete[] resultSetMap_;
		delete[] resultSetAllocator_;
		delete[] resultSetRowIdAllocator_;
		delete[] resultSetSwapAllocator_;
		delete objectManager_;

		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

DataStore::~DataStore() {
	for (uint32_t i = 0; i < pgConfig_.getPartitionCount(); ++i) {
		objectManager_->validateRefCounter(i);
		forceCloseAllResultSet(i);
	}

	for (uint32_t i = 0; i < pgConfig_.getPartitionGroupCount(); ++i) {
		resultSetMapManager_[i]->remove(resultSetMap_[i]);
		delete resultSetMapManager_[i];
		delete resultSetAllocator_[i];
		delete resultSetRowIdAllocator_[i];
		delete resultSetSwapAllocator_[i];
	}
	delete[] resultSetMapManager_;
	delete[] resultSetMap_;
	delete[] resultSetAllocator_;
	delete[] resultSetRowIdAllocator_;
	delete[] resultSetSwapAllocator_;
	delete objectManager_;
	delete containerIdTable_;
}

/*!
	@brief Initializer of DataStore
*/
void DataStore::initialize(ResourceSet &resourceSet) {
	try {
		resourceSet.stats_->addUpdator(&statUpdator_);
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(
				e, "Initialize failed. (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Creates or Updates Container
*/
BaseContainer *DataStore::putContainer(TransactionContext &txn, PartitionId pId,
	const FullContainerKey &containerKey, uint8_t containerType,
	uint32_t schemaSize, const uint8_t *containerSchema, bool isEnable,
	int32_t featureVersion,
	PutStatus &status, bool isCaseSensitive) {
	BaseContainer *container = NULL;
	util::StackAllocator &alloc = txn.getDefaultAllocator();
	try {
		util::ArrayByteInStream in = util::ArrayByteInStream(
			util::ArrayInStream(containerSchema, schemaSize));
		MessageSchema *messageSchema = NULL;

		util::String containerName(alloc);
		containerKey.toString(alloc, containerName);
		switch (containerType) {
		case COLLECTION_CONTAINER:
			messageSchema = ALLOC_NEW(alloc)
				MessageCollectionSchema(alloc,
					getValueLimitConfig(), containerName.c_str(),
					in, featureVersion);
			break;
		case TIME_SERIES_CONTAINER:
			messageSchema = ALLOC_NEW(alloc)
				MessageTimeSeriesSchema(alloc,
					getValueLimitConfig(), containerName.c_str(),
					in, featureVersion);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_TYPE_INVALID, "");
			break;
		}

		bool isAllowExpiration = true;
		container = getContainer(txn, pId, containerKey, containerType, false, isAllowExpiration);
		if (container == NULL) {
			container = createContainer(
				txn, pId, containerKey, containerType, messageSchema);
			status = CREATE;
			GS_TRACE_INFO(DATA_STORE, GS_TRACE_DS_DS_CREATE_CONTAINER,
				"name = " << containerName
						  << " Id = " << container->getContainerId());
		}
		else {
			if (isCaseSensitive) {
				FullContainerKey existContainerKey = container->getContainerKey(txn);
				if (containerKey.compareTo(alloc, existContainerKey, isCaseSensitive) != 0) {
					util::String inputStr(alloc);
					util::String existStr(alloc);
					containerKey.toString(alloc, inputStr);
					existContainerKey.toString(alloc, existStr);
					GS_THROW_USER_ERROR(
						GS_ERROR_TXN_INDEX_ALREADY_EXISTS, 
						"Case sensitivity mismatch, existed container name = "
							<< existStr
							<< ", input container name = " << inputStr);
				}
			}

			util::XArray<uint32_t> copyColumnMap(alloc);
			SchemaState schemaState;
			container->makeCopyColumnMap(txn, messageSchema, copyColumnMap, 
				schemaState);
			if (schemaState != DataStore::SAME_SCHEMA) {
				if (!isEnable) {  
					if (schemaState == DataStore::ONLY_TABLE_PARTITIONING_VERSION_DIFFERENCE) {
						status = NOT_EXECUTED;
						return container;
					}
					else {
						GS_THROW_USER_ERROR(
							GS_ERROR_DS_DS_CHANGE_SCHEMA_DISABLE, "");
					}
				}
				if (container->isInvalid()) {  
					GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
						"can not change schema. container's status is "
						"invalid.");
				}
				if (container->isExpired(txn)) {
					setLastExpiredTime(txn.getPartitionId(), txn.getStatementStartTime().getUnixTime());
					status = NOT_EXECUTED;
					return container;
				}
				if (container->hasUncommitedTransaction(txn)) {
					try {
						container->getContainerCursor(txn);
						GS_TRACE_INFO(
							DATASTORE_BACKGROUND, GS_TRACE_DS_DS_UPDATE_CONTAINER, "Continue to change shema");
						status = UPDATE;
					} 
					catch (UserException &e) {
						DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_COL_LOCK_CONFLICT,
							"change schema(pId=" << txn.getPartitionId()
								<< ", containerId=" << container->getContainerId()
								<< ", txnId=" << txn.getId() << ")");
					}
				} else {
					GS_TRACE_INFO(DATA_STORE, GS_TRACE_DS_DS_UPDATE_CONTAINER,
						"Start name = " << containerName
										<< " Id = " << container->getContainerId());
					if (schemaState == DataStore::COLUMNS_DIFFERENCE) {
						changeContainerSchema(
							txn, pId, containerKey, container, messageSchema, copyColumnMap);
						GS_TRACE_INFO(
							DATA_STORE, GS_TRACE_DS_DS_UPDATE_CONTAINER, "Change shema");
						status = UPDATE;
					} else if (schemaState == DataStore::PROPERY_DIFFERENCE) {
						changeContainerProperty(
							txn, pId, container, messageSchema);
						GS_TRACE_INFO(
							DATA_STORE, GS_TRACE_DS_DS_UPDATE_CONTAINER, "Change property");
						status = CHANGE_PROPERY;
					} else if (schemaState == DataStore::ONLY_TABLE_PARTITIONING_VERSION_DIFFERENCE) {
						changeTablePartitioningVersion(
							txn, pId, container, messageSchema);
						GS_TRACE_INFO(
							DATA_STORE, GS_TRACE_DS_DS_UPDATE_CONTAINER, "Change table partitioning version");
						status = CHANGE_PROPERY;
					} else if (schemaState == DataStore::COLUMNS_ADD) {
						addContainerSchema(
							txn, pId, container, messageSchema);
						GS_TRACE_INFO(
							DATA_STORE, GS_TRACE_DS_DS_UPDATE_CONTAINER, "Column add");
						status = CHANGE_PROPERY; 
					}
				}
			}
			else {
				status = NOT_EXECUTED;
			}
		}
		return container;
	}
	catch (std::exception &e) {
		if (container != NULL) {
			ALLOC_DELETE(alloc, container);
		}
		handleUpdateError(e, GS_ERROR_DS_DS_CREATE_COLLECTION_FAILED);
		return NULL;
	}
}

/*!
	@brief Drop Container
*/
void DataStore::dropContainer(TransactionContext &txn, PartitionId pId,
	const FullContainerKey &containerKey, uint8_t containerType,
	bool isCaseSensitive) {
	try {
		if (!objectManager_->existPartition(pId)) {
			return;
		}
		bool isAllowExpiration = true;
		ContainerAutoPtr containerAutoPtr(
			txn, this, pId, containerKey, containerType, isCaseSensitive, isAllowExpiration);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		if (container == NULL) {
			return;
		}

		if (!container->isExpired(txn) && !container->isInvalid() && container->hasUncommitedTransaction(txn)) {
			DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_COL_LOCK_CONFLICT,
				"drop container(pId=" << txn.getPartitionId()
					<< ", containerId=" << container->getContainerId()
					<< ", txnId=" << txn.getId() << ")");
		}

		ContainerId containerId = container->getContainerId();
		containerIdTable_->remove(pId, containerId);

		DataStorePartitionHeaderObject partitionHeadearObject(
			txn.getPartitionId(), *getObjectManager(), PARTITION_HEADER_OID);
		BtreeMap containerMap(txn, *getObjectManager(),
			partitionHeadearObject.getMetaMapOId(), allocateStrategy_, NULL);

		OId oId = UNDEF_OID;
		FullContainerKeyCursor keyCursor(const_cast<FullContainerKey *>(&containerKey));
		containerMap.search<FullContainerKeyCursor, OId, OId>(
			txn, keyCursor, oId, isCaseSensitive);
		if (oId == UNDEF_OID) {
			return;
		}

		int32_t status = containerMap.remove<FullContainerKeyCursor, OId>(txn, keyCursor, oId, isCaseSensitive);
		if ((status & BtreeMap::ROOT_UPDATE) != 0) {
			partitionHeadearObject.setMetaMapOId(containerMap.getBaseOId());
		}

		if (!container->isInvalid()) {  
			finalizeContainer(txn, container);
		}

	}
	catch (std::exception &e) {
		handleUpdateError(e, GS_ERROR_DS_DS_DROP_COLLECTION_FAILED);
	}
}

/*!
	@brief Get Container by name
*/
BaseContainer *DataStore::getContainer(TransactionContext &txn, PartitionId pId,
	const FullContainerKey &containerKey, uint8_t containerType,
	bool isCaseSensitive, bool allowExpiration) {
	try {
		if (!objectManager_->existPartition(pId)) {
			return NULL;
		}
		DataStorePartitionHeaderObject partitionHeadearObject(
			txn.getPartitionId(), *getObjectManager(), PARTITION_HEADER_OID);
		BtreeMap containerMap(txn, *getObjectManager(),
			partitionHeadearObject.getMetaMapOId(), allocateStrategy_, NULL);

		OId oId = UNDEF_OID;
		FullContainerKeyCursor keyCursor(const_cast<FullContainerKey *>(&containerKey));
		containerMap.search<FullContainerKeyCursor, OId, OId>(
			txn, keyCursor, oId, isCaseSensitive);
		if (oId == UNDEF_OID) {
			return NULL;
		}
		BaseContainer *container = getBaseContainer(txn, pId, oId, containerType, allowExpiration);
		return container;
	}
	catch (std::exception &e) {
		handleSearchError(e, GS_ERROR_DS_DS_GET_COLLECTION_FAILED);
		return NULL;
	}
}

/*!
	@brief Get Container by ContainerId
*/
BaseContainer *DataStore::getContainer(TransactionContext &txn, PartitionId pId,
	ContainerId containerId, uint8_t containerType, bool allowExpiration) {
	try {
		OId oId = containerIdTable_->get(pId, containerId);
		BaseContainer *container = 
			getBaseContainer(txn, pId, oId, containerType, allowExpiration);
		return container;
	}
	catch (std::exception &e) {
		handleSearchError(e, GS_ERROR_DS_DS_GET_COLLECTION_FAILED);
		return NULL;
	}
}

/*!
	@brief Get Collection by ObjectId at recovery phase
*/
BaseContainer *DataStore::getContainerForRestore(
	TransactionContext &txn, PartitionId, OId oId,
	ContainerId containerId, uint8_t containerType) {
	UNUSED_VARIABLE(containerId);
	try {
		if (UNDEF_OID == oId) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_CONTAINER_UNEXPECTEDLY_REMOVED, "");
		}
		if (containerType != COLLECTION_CONTAINER &&
			containerType != TIME_SERIES_CONTAINER
		) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_TYPE_INVALID, "");
		}
		BaseContainer *container = NULL;
		switch (containerType) {
		case COLLECTION_CONTAINER: {
			container =
				ALLOC_NEW(txn.getDefaultAllocator()) Collection(txn, this, oId);
		} break;
		case TIME_SERIES_CONTAINER: {
			container =
				ALLOC_NEW(txn.getDefaultAllocator()) TimeSeries(txn, this, oId);
		} break;
		default:
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_CONTAINER_TYPE_UNKNOWN, "");
			break;
		}

		return container;
	}
	catch (std::exception &e) {
		handleSearchError(e, GS_ERROR_DS_DS_GET_COLLECTION_FAILED);
		return NULL;
	}
}

/*!
	@brief Remove Container Schema in Container Schema Map
*/
void DataStore::removeColumnSchema(
	TransactionContext &txn, PartitionId pId, OId schemaOId) {
	ShareValueList commonContainerSchema(txn, *getObjectManager(), schemaOId);
	commonContainerSchema.decrement();
	int64_t schemaHashKey = commonContainerSchema.getHashVal();
	if (commonContainerSchema.getReferenceCounter() == 0) {
		StackAllocAutoPtr<BtreeMap> schemaMap(
			txn.getDefaultAllocator(), getSchemaMap(txn, pId));

		bool isCaseSensitive = true;
		int32_t status =
			schemaMap.get()->remove(txn, schemaHashKey, schemaOId, isCaseSensitive);
		if ((status & BtreeMap::ROOT_UPDATE) != 0) {
			DataStorePartitionHeaderObject partitionHeadearObject(
				txn.getPartitionId(), *getObjectManager(),
				PARTITION_HEADER_OID);
			partitionHeadearObject.setSchemaMapOId(
				schemaMap.get()->getBaseOId());
		}
		BaseContainer::finalizeSchema(
			txn, *getObjectManager(), &commonContainerSchema);
		commonContainerSchema.finalize();
	}
}

/*!
	@brief Remove Trigger in Trigger Map
*/
void DataStore::removeTrigger(
	TransactionContext &txn, PartitionId pId, OId triggerOId) {
	ShareValueList triggerSchema(txn, *getObjectManager(), triggerOId);
	triggerSchema.decrement();
	int64_t schemaHashKey = triggerSchema.getHashVal();
	if (triggerSchema.getReferenceCounter() == 0) {
		StackAllocAutoPtr<BtreeMap> triggerMap(
			txn.getDefaultAllocator(), getTriggerMap(txn, pId));

		bool isCaseSensitive = true;
		int32_t status =
			triggerMap.get()->remove(txn, schemaHashKey, triggerOId, isCaseSensitive);
		if ((status & BtreeMap::ROOT_UPDATE) != 0) {
			DataStorePartitionHeaderObject partitionHeadearObject(
				txn.getPartitionId(), *getObjectManager(),
				PARTITION_HEADER_OID);
			partitionHeadearObject.setTriggerMapOId(
				triggerMap.get()->getBaseOId());
		}
		BaseContainer::finalizeTrigger(
			txn, *getObjectManager(), &triggerSchema);
		triggerSchema.finalize();
	}
}

/*!
	@brief Get Finally ChunkKey when Chunk was expired
*/
ChunkKey DataStore::getLastChunkKey(TransactionContext &txn) {
	DataStorePartitionHeaderObject partitionHeadearObject(
		txn.getPartitionId(), *getObjectManager(), PARTITION_HEADER_OID);
	return partitionHeadearObject.getChunkKey();
}

/*!
	@brief Set Finally ChunkKey when Chunk was expired
*/
Timestamp DataStore::getLatestBatchFreeTime(TransactionContext &txn) {
	DataStorePartitionHeaderObject partitionHeadearObject(
		txn.getPartitionId(), *getObjectManager(), PARTITION_HEADER_OID);
	return partitionHeadearObject.getLatestBatchFreeTime();
}

void DataStore::finalizeContainer(TransactionContext &txn, BaseContainer *container) {
	bool isFinished = container->finalize(txn);
	if (isFinished) {
		GS_TRACE_INFO(
			DATASTORE_BACKGROUND, GS_ERROR_DS_BACKGROUND_TASK_INVALID, 
			"[DropContainer immediately finished, PartitionId = " << txn.getPartitionId()
				<< ", containerId = " << container->getContainerId());
	} else {
		BackgroundData bgData;
		bgData.setDropContainerData(container->getBaseOId());
		insertBGTask(txn, txn.getPartitionId(), bgData);

		GS_TRACE_INFO(
			DATASTORE_BACKGROUND, GS_ERROR_DS_BACKGROUND_TASK_INVALID, 
			"[DropContainer start, PartitionId = " << txn.getPartitionId()
				<< ", containerId = " << container->getContainerId());
	}
}

void DataStore::finalizeMap(TransactionContext &txn, const AllocateStrategy &allcateStrategy, BaseIndex *index) {
	bool isFinished = index->finalize(txn);
	if (isFinished) {
		GS_TRACE_INFO(
			DATASTORE_BACKGROUND, GS_ERROR_DS_BACKGROUND_TASK_INVALID,
			"[DropIndex immediately finished, PartitionId = " << txn.getPartitionId()
				<< ", mapType = " << (int)index->getMapType()
				<< ", oId = " << index->getBaseOId());
	} else {
		BackgroundData bgData;
		bgData.setDropIndexData(index->getMapType(), allcateStrategy.chunkKey_, index->getBaseOId());
		insertBGTask(txn, txn.getPartitionId(), bgData);

		GS_TRACE_INFO(
			DATASTORE_BACKGROUND, GS_ERROR_DS_BACKGROUND_TASK_INVALID,
			"[DropIndex start, PartitionId = " << txn.getPartitionId()
				<< ", mapType = " << (int)index->getMapType()
				<< ", oId = " << index->getBaseOId());
	}
}

template<>
DataStore::BackgroundData BtreeMap::getMaxValue() {
	return DataStore::BackgroundData();
}
template<>
DataStore::BackgroundData BtreeMap::getMinValue() {
	return DataStore::BackgroundData();
}

template int32_t BtreeMap::getAll(TransactionContext &txn, ResultSize limit,
	util::XArray< std::pair<BackgroundId, DataStore::BackgroundData> > &keyValueList);
template int32_t BtreeMap::getAll(TransactionContext &txn, ResultSize limit,
	util::XArray<std::pair<BackgroundId, DataStore::BackgroundData> > &keyValueList,
	BtreeMap::BtreeCursor &cursor);
bool DataStore::searchBGTask(TransactionContext &txn, PartitionId pId, BGTask &bgTask) {
	bool isFound = false;

	if (isRestored(pId) && getObjectManager()->existPartition(pId) && getBGTaskCount(pId) > 0) {
		StackAllocAutoPtr<BtreeMap> map(txn.getDefaultAllocator(),
			getBackgroundMap(txn, pId));
		BtreeMap::BtreeCursor btreeCursor;
		ResultSize limit = 10;
		while (1) {
			util::StackAllocator::Scope scope(txn.getDefaultAllocator());
			util::XArray< std::pair<BackgroundId, BackgroundData> > idList(
				txn.getDefaultAllocator());
			util::XArray< std::pair<BackgroundId, BackgroundData> >::iterator itr;
			int32_t getAllStatus = map.get()->getAll<BackgroundId, BackgroundData>(
				txn, limit, idList, btreeCursor);
			for (itr = idList.begin(); itr != idList.end(); itr++) {
				BackgroundData bgData = itr->second;
				if (!bgData.isInvalid()) {
					bgTask.pId_ = pId;
					bgTask.bgId_ = itr->first;
					isFound = true;
					break;
				}
			}
			if (isFound || getAllStatus == GS_SUCCESS) {
				break;
			}
		}
	}

	return isFound;
}

bool DataStore::executeBGTask(TransactionContext &txn, const BackgroundId bgId) {
	bool isFinished = false;
	try {
		if (!isRestored(txn.getPartitionId()) || 
			!getObjectManager()->existPartition(txn.getPartitionId())) {
			isFinished = true;
			return isFinished;
		}

		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		util::XArray<BackgroundData> list(txn.getDefaultAllocator());
		{
			StackAllocAutoPtr<BtreeMap> bgMap(txn.getDefaultAllocator(),
				getBackgroundMap(txn, txn.getPartitionId()));
			TermCondition cond(COLUMN_TYPE_LONG, COLUMN_TYPE_LONG, 
				DSExpression::EQ, UNDEF_COLUMNID, &bgId, sizeof(BackgroundId));
			BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, MAX_RESULT_SIZE);
			bgMap.get()->search
				<BackgroundId, BackgroundData, BackgroundData>(txn, sc, list);
		}

		if (list.size() != 1) {
			isFinished = true;
			return isFinished;
		}
		BackgroundData bgData = list[0];
		if (bgData.isInvalid()) {
			isFinished = true;
			return isFinished;
		}
		try {
			isFinished = executeBGTaskInternal(txn, bgData);
		}
		catch (UserException &) {
			isFinished = false;
			BackgroundData afterBgData = bgData;
			bgData.incrementError();
			updateBGTask(txn, txn.getPartitionId(), bgId, bgData, afterBgData);
		}
		if (isFinished) {
			removeBGTask(txn, txn.getPartitionId(), bgId, bgData);
		}
	}
	catch (std::exception &e) {
		handleUpdateError(e, GS_ERROR_DS_BACKGROUND_TASK_INVALID);
	}
	return isFinished;
}

void DataStore::clearAllBGTask(TransactionContext &txn) {
	if (!isRestored(txn.getPartitionId()) || 
		!getObjectManager()->existPartition(txn.getPartitionId())) {
		return;
	}
	try {
		bool isAllFinish = false;
		while (!isAllFinish) {
			util::StackAllocator::Scope scope(txn.getDefaultAllocator());
			util::XArray< std::pair<BackgroundId, BackgroundData> > list(txn.getDefaultAllocator());
			util::XArray< std::pair<BackgroundId, BackgroundData> >::iterator itr;
			{
				StackAllocAutoPtr<BtreeMap> bgMap(txn.getDefaultAllocator(),
					getBackgroundMap(txn, txn.getPartitionId()));
				bgMap.get()->getAll<BackgroundId, BackgroundData>(txn, MAX_RESULT_SIZE, list);
			}
			if (list.empty()) {
				break;
			}
			for (itr = list.begin(); itr != list.end(); itr++) {
				BackgroundId bgId = itr->first;
				BackgroundData bgData = itr->second;
				if (bgData.isInvalid()) {
					continue;
				}
				while (!executeBGTaskInternal(txn, bgData)) {}
				removeBGTask(txn, txn.getPartitionId(), bgId, bgData);
			}
		}
	}
	catch (std::exception &e) {
		handleUpdateError(e, GS_ERROR_DS_BACKGROUND_TASK_INVALID);
	}
}

bool DataStore::executeBGTaskInternal(TransactionContext &txn, BackgroundData &bgData) {
	bool isFinished = false;
	if (!isRestored(txn.getPartitionId()) || 
		!getObjectManager()->existPartition(txn.getPartitionId())) {
		isFinished = true;
		return isFinished;
	}

	const double FORCE_COLD_SWAP_RATE = 0;
	getObjectManager()->setStoreMemoryAgingSwapRate(txn.getPartitionId(), FORCE_COLD_SWAP_RATE);

	switch (bgData.getEventType()) {
	case BackgroundData::DROP_CONTAINER:
		{
			OId containerOId;
			bgData.getDropContainerData(containerOId);
			StackAllocAutoPtr<BaseContainer> container(txn.getDefaultAllocator(),
				getBaseContainer(txn, txn.getPartitionId(), containerOId, 
				ANY_CONTAINER, true));
			if (container.get() == NULL || container.get()->isInvalid()) {
				isFinished = true;
			} else {
				isFinished = container.get()->finalize(txn);
				if (isFinished) {
					GS_TRACE_INFO(
						DATASTORE_BACKGROUND, GS_ERROR_DS_BACKGROUND_TASK_INVALID, 
						"[DropContainer End PartitionId = " << txn.getPartitionId()
							<< ", containerId = " << container.get()->getContainerId());
				}
			}
		}
		break;
	case BackgroundData::DROP_INDEX:
		{
			MapType mapType;
			ChunkKey chunkKey;
			OId mapOId;

			bgData.getDropIndexData(mapType, chunkKey, mapOId);
			AllocateStrategy strategy;	

			Timestamp timestamp = txn.getStatementStartTime().getUnixTime();
			Timestamp chunkKeyTime = DataStore::convertChunkKey2Timestamp(chunkKey);

			if (chunkKey != UNDEF_CHUNK_KEY && (chunkKeyTime <= getLatestBatchFreeTime(txn) || chunkKeyTime <= timestamp)) {
				isFinished = true;
			}
			else {
				strategy.chunkKey_ = chunkKey;
				StackAllocAutoPtr<BaseIndex> map(txn.getDefaultAllocator(),
					getIndex(txn, *getObjectManager(), mapType, mapOId, 
					strategy, NULL, NULL));
				isFinished = map.get()->finalize(txn);
			}
			if (isFinished) {
				GS_TRACE_INFO(
					DATASTORE_BACKGROUND, GS_ERROR_DS_BACKGROUND_TASK_INVALID,
					"[DropIndex End PartitionId = " << txn.getPartitionId()
						<< ", mapType = " << (int)mapType
						<< ", oId = " << mapOId);
			}
		}
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
		break;
	}
	return isFinished;
}

BaseIndex *DataStore::getIndex(TransactionContext &txn, 
	ObjectManager &objectManager, MapType mapType, OId mapOId, 
	const AllocateStrategy &strategy, BaseContainer *container,
	TreeFuncInfo *funcInfo) {
	BaseIndex *map = NULL;
	switch (mapType) {
	case MAP_TYPE_BTREE:
		map = ALLOC_NEW(txn.getDefaultAllocator()) BtreeMap(
			txn, objectManager, mapOId, strategy, container, funcInfo);
		break;
	case MAP_TYPE_HASH:
		map = ALLOC_NEW(txn.getDefaultAllocator()) HashMap(
			txn, objectManager, mapOId, strategy, container, funcInfo);
		break;
	case MAP_TYPE_SPATIAL:
		map = ALLOC_NEW(txn.getDefaultAllocator()) RtreeMap(
			txn, objectManager, mapOId, strategy, container, funcInfo);
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
	}
	return map;
}

std::ostream &operator<<(
	std::ostream &output, const DataStore::BackgroundData &bgData) {
	switch (bgData.getEventType()) {
	case DataStore::BackgroundData::DROP_CONTAINER:
		{
			output << "[ eventType=DROP_CONTAINER,";
			OId containerOId;
			bgData.getDropContainerData(containerOId);
			output << " oId=" << containerOId << ",";
		}
		break;
	case DataStore::BackgroundData::DROP_INDEX:
		{
			output << "[ eventType=DROP_INDEX,";
			MapType mapType;
			ChunkKey chunkKey;
			OId mapOId;
			bgData.getDropIndexData(mapType, chunkKey, mapOId);
			output << " mapType=" << (uint32_t)mapType << ",";
			output << " chunkKey=" << chunkKey << ",";
			output << " oId=" << mapOId << ",";
		}
		break;
	default:
		output << "[ eventType=UNKNOWN,";
		break;
	}
	output << " status=" << (uint32_t)bgData.isInvalid() << ",";
	output << " errorCounter=" << (uint32_t)bgData.getErrorCount() << ",";
	output << "]";
	return output;
}

void DataStore::dumpTraceBGTask(TransactionContext &txn, PartitionId pId) {
	util::NormalOStringStream stream;
	stream << "dumpTraceBGTask PartitionId = " << pId << std::endl;
	if (getObjectManager()->existPartition(pId)) {
		stream << "BGTaskCount= " << getBGTaskCount(pId) << std::endl;

		StackAllocAutoPtr<BtreeMap> map(txn.getDefaultAllocator(),
			getBackgroundMap(txn, pId));
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		util::XArray< std::pair<BackgroundId, BackgroundData> > idList(
			txn.getDefaultAllocator());
		util::XArray< std::pair<BackgroundId, BackgroundData> >::iterator itr;
		map.get()->getAll<BackgroundId, BackgroundData>(
			txn, MAX_RESULT_SIZE, idList);
		stream << "BGTaskRealCount= " << idList.size() << std::endl;
		for (itr = idList.begin(); itr != idList.end(); itr++) {
			BackgroundData bgData = itr->second;
			stream << "BGTask= " << bgData << std::endl;
		}
	}

	GS_TRACE_ERROR(
		DATA_STORE, GS_ERROR_DS_BACKGROUND_TASK_INVALID, stream.str().c_str());
}


template <>
int32_t BtreeMap::getInitialItemSizeThreshold<BackgroundId, DataStore::BackgroundData>() {
	return INITIAL_MVCC_ITEM_SIZE_THRESHOLD;
}

/*!
	@brief Allocate DataStorePartitionHeader Object and BtreeMap Objects for
   Containers, Schemas and Triggers
*/
void DataStore::DataStorePartitionHeaderObject::initialize(
	TransactionContext &txn, const AllocateStrategy &allocateStrategy) {
	BaseObject::allocate<DataStorePartitionHeader>(
		sizeof(DataStorePartitionHeader), allocateStrategy, getBaseOId(),
		OBJECT_TYPE_CONTAINER_ID);
	memset(get(), 0, sizeof(DataStorePartitionHeader));

	setChunkKey(MIN_CHUNK_KEY);

	BtreeMap metaMap(txn, *getObjectManager(), allocateStrategy, NULL);
	metaMap.initialize(
		txn, COLUMN_TYPE_STRING, true, BtreeMap::TYPE_SINGLE_KEY);
	setMetaMapOId(metaMap.getBaseOId());

	BtreeMap schemaMap(txn, *getObjectManager(), allocateStrategy, NULL);
	schemaMap.initialize(
		txn, COLUMN_TYPE_LONG, false, BtreeMap::TYPE_SINGLE_KEY);
	setSchemaMapOId(schemaMap.getBaseOId());

	BtreeMap triggerMap(txn, *getObjectManager(), allocateStrategy, NULL);
	triggerMap.initialize(
		txn, COLUMN_TYPE_LONG, false, BtreeMap::TYPE_SINGLE_KEY);
	setTriggerMapOId(triggerMap.getBaseOId());

	BtreeMap backgroundMap(txn, *getObjectManager(), allocateStrategy, NULL);
	backgroundMap.initialize<BackgroundId, DataStore::BackgroundData>(
		txn, COLUMN_TYPE_LONG, false, BtreeMap::TYPE_SINGLE_KEY);
	setBackgroundMapOId(backgroundMap.getBaseOId());

	get()->maxContainerId_ = 0;
	get()->maxBackgroundId_ = 0;

	get()->latestCheckTime_ = 0;
}

/*!
	@brief Free DataStorePartitionHeader Object and BtreeMap Objects for
   Containers, Schemas and Triggers
*/
void DataStore::DataStorePartitionHeaderObject::finalize(
	TransactionContext &txn, const AllocateStrategy &allocateStrategy) {
	BtreeMap metaMap(
		txn, *getObjectManager(), getMetaMapOId(), allocateStrategy, NULL);
	metaMap.finalize(txn);
	BtreeMap schemaMap(
		txn, *getObjectManager(), getSchemaMapOId(), allocateStrategy, NULL);
	schemaMap.finalize(txn);
	BtreeMap triggerMap(
		txn, *getObjectManager(), getTriggerMapOId(), allocateStrategy, NULL);
	triggerMap.finalize(txn);
	BtreeMap backgroundMap(
		txn, *getObjectManager(), getBackgroundMapOId(), allocateStrategy, NULL);
	backgroundMap.finalize(txn);
}

BaseContainer *DataStore::createContainer(TransactionContext &txn,
	PartitionId pId, const FullContainerKey &containerKey,
	ContainerType containerType, MessageSchema *messageSchema) {
	DataStorePartitionHeaderObject partitionHeadearObject(
		pId, *getObjectManager());
	OId partitionHeaderOId = UNDEF_OID;

	if (!objectManager_->existPartition(pId)) {
		partitionHeadearObject.initialize(txn, allocateStrategy_);
		if (partitionHeadearObject.getBaseOId() != PARTITION_HEADER_OID) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_DS_CHUNK_OFFSET_INVALID, "");
		}
		if (!isRestored(pId)) {
			setRestored(pId);
		}
	}
	else {
		partitionHeaderOId = PARTITION_HEADER_OID;
		partitionHeadearObject.load(partitionHeaderOId);
		partitionHeadearObject.incrementMaxContainerId();
	}
	if (partitionHeadearObject.getMaxContainerId() == UNDEF_CONTAINERID) {
		GS_THROW_USER_ERROR(
			GS_ERROR_CM_LIMITS_EXCEEDED, "container num over limit");
	}


	int64_t schemaHashKey = BaseContainer::calcSchemaHashKey(messageSchema);
	OId schemaOId = getColumnSchemaId(txn, pId, messageSchema, schemaHashKey);
	insertColumnSchema(txn, pId, messageSchema, schemaHashKey, schemaOId);

	const ContainerId containerId = partitionHeadearObject.getMaxContainerId();

	BaseContainer *container;
	switch (containerType) {
	case COLLECTION_CONTAINER: {
		container = ALLOC_NEW(txn.getDefaultAllocator()) Collection(txn, this);
	} break;
	case TIME_SERIES_CONTAINER: {
		container = ALLOC_NEW(txn.getDefaultAllocator()) TimeSeries(txn, this);
	} break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_CONTAINER_TYPE_UNKNOWN, "");
		break;
	}

	container->initialize(txn);
	container->set(txn, containerKey,
		containerId, schemaOId, messageSchema);
	ContainerAttribute attribute = container->getAttribute();
	containerIdTable_->set(pId, container->getContainerId(),
		container->getBaseOId(), containerKey.getComponents(txn.getDefaultAllocator()).dbId_, attribute);
	TEST_PRINT("create container \n");
	TEST_PRINT1("pId %d\n", pId);
	TEST_PRINT1("databaseVersionId %d\n", containerKey.getComponemts().dbId_);
	TEST_PRINT1("attribute %d\n", (int32_t)attribute);

	FullContainerKeyCursor keyCursor(
		txn, *getObjectManager(), container->getContainerKeyOId());

	bool isCaseSensitive = false;
	BtreeMap containerMap(txn, *getObjectManager(),
		partitionHeadearObject.getMetaMapOId(), allocateStrategy_, NULL);
	int32_t status = containerMap.insert<FullContainerKeyCursor, OId>(
		txn, keyCursor, container->getBaseOId(), isCaseSensitive);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		partitionHeadearObject.setMetaMapOId(containerMap.getBaseOId());
	}

	return container;
}

void DataStore::changeContainerSchema(TransactionContext &txn, PartitionId pId,
	const FullContainerKey &containerKey,
	BaseContainer *&container, MessageSchema *messageSchema,
	util::XArray<uint32_t> &copyColumnMap) {

	int64_t schemaHashKey = BaseContainer::calcSchemaHashKey(messageSchema);
	OId schemaOId = getColumnSchemaId(txn, pId, messageSchema, schemaHashKey);
	insertColumnSchema(txn, pId, messageSchema, schemaHashKey, schemaOId);
	BaseContainer *newContainer;
	switch (container->getContainerType()) {
	case COLLECTION_CONTAINER: {
		newContainer =
			ALLOC_NEW(txn.getDefaultAllocator()) Collection(txn, this);
	} break;
	case TIME_SERIES_CONTAINER: {
		newContainer =
			ALLOC_NEW(txn.getDefaultAllocator()) TimeSeries(txn, this);
	} break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_CONTAINER_TYPE_UNKNOWN, "");
		break;
	}

	newContainer->initialize(txn);
	newContainer->set(txn, containerKey,
		container->getContainerId(), schemaOId, messageSchema);
	newContainer->setVersionId(
		container->getVersionId() + 1);  

	try {
		container->changeSchema(txn, *newContainer, copyColumnMap);
	}
	catch (UserException &e) {  
		finalizeContainer(txn, newContainer);
		ALLOC_DELETE(txn.getDefaultAllocator(), newContainer);
		GS_RETHROW_USER_ERROR(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "Failed to change schema"));
	}

	TEST_PRINT("changeSchema \n");
	TEST_PRINT1("pId %d\n", pId);
	TEST_PRINT1("databaseVersionId %d\n", containerKey.getComponents().dbId_);
	TEST_PRINT1("attribute %d\n", (int32_t)attribute);

	if (txn.isAutoCommit()) {
		updateContainer(txn, container, newContainer->getBaseOId());
		finalizeContainer(txn, container);
		ALLOC_DELETE(txn.getDefaultAllocator(), container);
		container = newContainer;
	} else {
		ALLOC_DELETE(txn.getDefaultAllocator(), newContainer);
	}
}

void DataStore::changeContainerProperty(TransactionContext &txn, PartitionId pId,
	BaseContainer *&container, MessageSchema *messageSchema) {

	ContainerId containerId = container->getContainerId();
	ContainerType containerType = container->getContainerType();
	int64_t schemaHashKey = BaseContainer::calcSchemaHashKey(messageSchema);
	OId schemaOId = getColumnSchemaId(txn, pId, messageSchema, schemaHashKey);
	insertColumnSchema(txn, pId, messageSchema, schemaHashKey, schemaOId);
	container->changeProperty(txn, schemaOId);
	container->setTablePartitioningVersionId(messageSchema->getTablePartitioningVersionId());
	ALLOC_DELETE(txn.getDefaultAllocator(), container);
	container = getContainer(txn, pId, containerId, containerType);
}

void DataStore::changeTablePartitioningVersion(TransactionContext &txn, PartitionId pId,
	BaseContainer *&container, MessageSchema *messageSchema) {
	UNUSED_VARIABLE(txn);
	UNUSED_VARIABLE(pId);
	container->setTablePartitioningVersionId(messageSchema->getTablePartitioningVersionId());
}
void DataStore::addContainerSchema(TransactionContext &txn, PartitionId pId,
	BaseContainer *&container, MessageSchema *messageSchema) {

	uint32_t oldColumnNum = container->getColumnNum();

	ContainerId containerId = container->getContainerId();
	ContainerType containerType = container->getContainerType();
	int64_t schemaHashKey = BaseContainer::calcSchemaHashKey(messageSchema);
	OId schemaOId = getColumnSchemaId(txn, pId, messageSchema, schemaHashKey);
	insertColumnSchema(txn, pId, messageSchema, schemaHashKey, schemaOId);

	OId oldSchmaOId = container->getColumnSchemaId();
	container->changeProperty(txn, schemaOId);
	container->setTablePartitioningVersionId(messageSchema->getTablePartitioningVersionId());
	ALLOC_DELETE(txn.getDefaultAllocator(), container);
	if (oldSchmaOId != UNDEF_OID) {
		removeColumnSchema(
			txn, txn.getPartitionId(), oldSchmaOId);
	}

	container = getContainer(txn, pId, containerId, containerType);
	container->changeNullStats(txn, oldColumnNum);
}

OId DataStore::getColumnSchemaId(TransactionContext &txn, PartitionId pId,
	MessageSchema *messageSchema, int64_t schemaHashKey) {
	StackAllocAutoPtr<BtreeMap> schemaMap(
		txn.getDefaultAllocator(), getSchemaMap(txn, pId));
	OId schemaOId = UNDEF_OID;
	util::XArray<OId> schemaList(txn.getDefaultAllocator());
	TermCondition cond(COLUMN_TYPE_LONG, COLUMN_TYPE_LONG, 
		DSExpression::EQ, UNDEF_COLUMNID, &schemaHashKey, sizeof(schemaHashKey));
	BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, MAX_RESULT_SIZE);
	schemaMap.get()->search(txn, sc, schemaList);
	for (size_t i = 0; i < schemaList.size(); i++) {
		ShareValueList commonContainerSchema(
			txn, *getObjectManager(), schemaList[i]);
		if (BaseContainer::schemaCheck(txn, *getObjectManager(),
				&commonContainerSchema, messageSchema)) {
			schemaOId = schemaList[i];
			break;
		}
	}
	return schemaOId;
}

OId DataStore::getTriggerId(TransactionContext &txn, PartitionId pId,
	util::XArray<const uint8_t *> &binary, int64_t triggerHashKey) {
	StackAllocAutoPtr<BtreeMap> triggerMap(
		txn.getDefaultAllocator(), getTriggerMap(txn, pId));
	OId triggerOId = UNDEF_OID;
	util::XArray<OId> schemaList(txn.getDefaultAllocator());
	TermCondition cond(COLUMN_TYPE_LONG, COLUMN_TYPE_LONG, 
		DSExpression::EQ, UNDEF_COLUMNID, &triggerHashKey, sizeof(triggerHashKey));
	BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, MAX_RESULT_SIZE);
	triggerMap.get()->search(txn, sc, schemaList);
	for (size_t i = 0; i < schemaList.size(); i++) {
		ShareValueList commonContainerSchema(
			txn, *getObjectManager(), schemaList[i]);
		if (BaseContainer::triggerCheck(
				txn, *getObjectManager(), &commonContainerSchema, binary)) {
			triggerOId = schemaList[i];
			break;
		}
	}
	return triggerOId;
}

void DataStore::insertColumnSchema(TransactionContext &txn, PartitionId pId,
	MessageSchema *messageSchema, int64_t schemaHashKey, OId &schemaOId) {
	if (schemaOId == UNDEF_OID) {
		StackAllocAutoPtr<BtreeMap> schemaMap(
			txn.getDefaultAllocator(), getSchemaMap(txn, pId));
		uint32_t bodyAllocateSize;
		util::XArray<ShareValueList::ElemData> inputList(
			txn.getDefaultAllocator());
		bool onMemory = false;
		BaseContainer::initializeSchema(txn, *getObjectManager(), messageSchema,
			allocateStrategy_, inputList, bodyAllocateSize, onMemory);
		uint32_t allocateSize = ShareValueList::getHeaderSize(
									static_cast<int32_t>(inputList.size())) +
								bodyAllocateSize;

		ShareValueList commonContainerSchema(txn, *getObjectManager());
		commonContainerSchema.initialize(txn, allocateSize, allocateStrategy_, onMemory);
		commonContainerSchema.set(schemaHashKey, inputList);
		schemaOId = commonContainerSchema.getBaseOId();

		bool isCaseSensitive = true;
		int32_t status =
			schemaMap.get()->insert(txn, schemaHashKey, schemaOId, isCaseSensitive);
		if ((status & BtreeMap::ROOT_UPDATE) != 0) {
			DataStorePartitionHeaderObject partitionHeadearObject(
				txn.getPartitionId(), *getObjectManager(),
				PARTITION_HEADER_OID);
			partitionHeadearObject.setSchemaMapOId(
				schemaMap.get()->getBaseOId());
		}
	}
	else {
		ShareValueList commonContainerSchema(
			txn, *getObjectManager(), schemaOId);
		commonContainerSchema.increment();
	}
}

void DataStore::insertTrigger(TransactionContext &txn, PartitionId pId,
	util::XArray<const uint8_t *> &binary, int64_t triggerHashKey,
	OId &triggerOId) {
	if (triggerOId == UNDEF_OID) {
		StackAllocAutoPtr<BtreeMap> triggerMap(
			txn.getDefaultAllocator(), getTriggerMap(txn, pId));

		uint32_t bodyAllocateSize = static_cast<uint32_t>(binary.size());
		util::XArray<ShareValueList::ElemData> inputList(
			txn.getDefaultAllocator());
		BaseContainer::initializeTrigger(txn, *getObjectManager(), binary,
			allocateStrategy_, inputList, bodyAllocateSize);

		uint32_t allocateSize = ShareValueList::getHeaderSize(
									static_cast<int32_t>(inputList.size())) +
								bodyAllocateSize;
		ShareValueList triggerSchema(txn, *getObjectManager());
		bool onMemory = false;
		triggerSchema.initialize(txn, allocateSize, allocateStrategy_,
			onMemory);
		triggerSchema.set(triggerHashKey, inputList);
		triggerOId = triggerSchema.getBaseOId();

		bool isCaseSensitive = true;
		int32_t status =
			triggerMap.get()->insert(txn, triggerHashKey, triggerOId, isCaseSensitive);
		if ((status & BtreeMap::ROOT_UPDATE) != 0) {
			DataStorePartitionHeaderObject partitionHeadearObject(
				txn.getPartitionId(), *getObjectManager(),
				PARTITION_HEADER_OID);
			partitionHeadearObject.setTriggerMapOId(
				triggerMap.get()->getBaseOId());
		}
	}
	else {
		ShareValueList triggerSchema(txn, *getObjectManager(), triggerOId);
		triggerSchema.increment();
	}
}

/*!
	@brief Get Container Schema Map
*/
BtreeMap *DataStore::getSchemaMap(TransactionContext &txn, PartitionId pId) {
	UNUSED_VARIABLE(pId);
	BtreeMap *schemaMap = NULL;

	DataStorePartitionHeaderObject partitionHeadearObject(
		txn.getPartitionId(), *getObjectManager(), PARTITION_HEADER_OID);
	schemaMap =
		ALLOC_NEW(txn.getDefaultAllocator()) BtreeMap(txn, *getObjectManager(),
			partitionHeadearObject.getSchemaMapOId(), allocateStrategy_);
	return schemaMap;
}

/*!
	@brief Get Trigger Map
*/
BtreeMap *DataStore::getTriggerMap(TransactionContext &txn, PartitionId pId) {
	UNUSED_VARIABLE(pId);
	BtreeMap *schemaMap = NULL;

	DataStorePartitionHeaderObject partitionHeadearObject(
		txn.getPartitionId(), *getObjectManager(), PARTITION_HEADER_OID);
	schemaMap =
		ALLOC_NEW(txn.getDefaultAllocator()) BtreeMap(txn, *getObjectManager(),
			partitionHeadearObject.getTriggerMapOId(), allocateStrategy_);
	return schemaMap;
}

template int32_t BtreeMap::insert(
	TransactionContext &txn, BackgroundId &key, DataStore::BackgroundData &value, bool isCaseSensitive);
template int32_t BtreeMap::remove(
	TransactionContext &txn, BackgroundId &key, DataStore::BackgroundData &value, bool isCaseSensitive);
template int32_t BtreeMap::update(TransactionContext &txn, BackgroundId &key,
	DataStore::BackgroundData &oldValue, DataStore::BackgroundData &newValue, bool isCaseSensitive);

/*!
	@brief Get Container by ContainerId
*/
BaseContainer *DataStore::getBaseContainer(TransactionContext &txn, PartitionId pId,
	OId oId, ContainerType containerType, bool allowExpiration) {
	if (!objectManager_->existPartition(pId)) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_CONTAINER_UNEXPECTEDLY_REMOVED, "Partition " << pId << " not exist");
	}
	if (UNDEF_OID == oId) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_CONTAINER_UNEXPECTEDLY_REMOVED, "");
	}
	BaseObject baseContainerImageObject(
		txn.getPartitionId(), *getObjectManager(), oId);
	BaseContainer::BaseContainerImage *baseContainerImage =
		baseContainerImageObject
			.getBaseAddr<BaseContainer::BaseContainerImage *>();
	ContainerType realContainerType = baseContainerImage->containerType_;
	if (containerType != ANY_CONTAINER &&
		realContainerType != containerType) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_TYPE_INVALID, 
			"Container type is invalid, or partitioned container already exists");
	}

	BaseContainer *container = NULL;
	switch (realContainerType) {
	case COLLECTION_CONTAINER: {
		container =
			ALLOC_NEW(txn.getDefaultAllocator()) Collection(txn, this, oId);
	} break;
	case TIME_SERIES_CONTAINER: {
		container =
			ALLOC_NEW(txn.getDefaultAllocator()) TimeSeries(txn, this, oId);
	} break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_CONTAINER_TYPE_UNKNOWN, "");
		break;
	}
	if (container->isExpired(txn) && !allowExpiration) {
		util::String containerName(txn.getDefaultAllocator());
		container->getContainerKey(txn).toString(txn.getDefaultAllocator(), containerName);
		ALLOC_DELETE(txn.getDefaultAllocator(), container);
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_EXPIRED, "\"" 
			<< containerName
			<< "\" is already expired");
	}

	return container;
}

/*!
	@brief Update objcet of Container
*/
void DataStore::updateContainer(TransactionContext &txn, BaseContainer *container, OId newContainerOId) {
	DataStorePartitionHeaderObject partitionHeadearObject(
		txn.getPartitionId(), *getObjectManager(), PARTITION_HEADER_OID);
	BtreeMap containerMap(txn, *getObjectManager(),
		partitionHeadearObject.getMetaMapOId(), allocateStrategy_, NULL);

	FullContainerKeyCursor keyCursor(
		txn, *getObjectManager(), container->getContainerKeyOId());

	OId oId = UNDEF_OID;
	bool isCaseSensitive = false;
	containerMap.search<FullContainerKeyCursor, OId, OId>(
		txn, keyCursor, oId, isCaseSensitive);
	if (oId == UNDEF_OID) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_CONTAINER_NOT_FOUND, "");
	}

	int32_t status = containerMap.remove<FullContainerKeyCursor, OId>(
		txn, keyCursor, oId, isCaseSensitive);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		partitionHeadearObject.setMetaMapOId(containerMap.getBaseOId());
	}

	StackAllocAutoPtr<BaseContainer> newContainer(txn.getDefaultAllocator(),
		getBaseContainer(txn, txn.getPartitionId(), newContainerOId, 
		container->getContainerType(), true));
	if (newContainer.get() == NULL || newContainer.get()->isInvalid()) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_CONTAINER_NOT_FOUND, "");
	}

	FullContainerKeyCursor newKeyCursor(
		txn, *getObjectManager(), newContainer.get()->getContainerKeyOId());
	status = containerMap.insert<FullContainerKeyCursor, OId>(
		txn, newKeyCursor, newContainerOId, isCaseSensitive);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		partitionHeadearObject.setMetaMapOId(containerMap.getBaseOId());
	}

	ContainerId containerId = container->getContainerId();

	containerIdTable_->remove(txn.getPartitionId(), containerId);
	ContainerAttribute attribute = container->getAttribute();

	containerIdTable_->set(txn.getPartitionId(), containerId, newContainerOId,
		newKeyCursor.getKey().getComponents(txn.getDefaultAllocator()).dbId_, attribute);
}

BtreeMap *DataStore::getBackgroundMap(TransactionContext &txn, PartitionId pId) {
	UNUSED_VARIABLE(pId);
	BtreeMap *schemaMap = NULL;

	DataStorePartitionHeaderObject partitionHeadearObject(
		txn.getPartitionId(), *getObjectManager(), PARTITION_HEADER_OID);
	schemaMap =
		ALLOC_NEW(txn.getDefaultAllocator()) BtreeMap(txn, *getObjectManager(),
			partitionHeadearObject.getBackgroundMapOId(), allocateStrategy_);
	return schemaMap;
}
BackgroundId DataStore::insertBGTask(TransactionContext &txn, PartitionId pId, 
	BackgroundData &bgData) {

	DataStorePartitionHeaderObject partitionHeadearObject(
		txn.getPartitionId(), *getObjectManager(), PARTITION_HEADER_OID);

	partitionHeadearObject.incrementBackgroundId();
	BackgroundId bgId = partitionHeadearObject.getMaxBackgroundId();

	{
		StackAllocAutoPtr<BtreeMap> bgMap(
			txn.getDefaultAllocator(), getBackgroundMap(txn, pId));

		bool isCaseSensitive = true;
		int32_t status =
			bgMap.get()->insert<BackgroundId, BackgroundData>(txn, bgId, bgData, isCaseSensitive);
		if ((status & BtreeMap::ROOT_UPDATE) != 0) {
			partitionHeadearObject.setBackgroundMapOId(
				bgMap.get()->getBaseOId());
		}
	}

	activeBackgroundCount_[pId]++;

	return bgId;
}
void DataStore::removeBGTask(TransactionContext &txn, PartitionId pId, BackgroundId bgId,
	BackgroundData &bgData) {
	StackAllocAutoPtr<BtreeMap> bgMap(
		txn.getDefaultAllocator(), getBackgroundMap(txn, pId));

	bool isCaseSensitive = true;
	int32_t status =
		bgMap.get()->remove<BackgroundId, BackgroundData>(txn, bgId, bgData, isCaseSensitive);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		DataStorePartitionHeaderObject partitionHeadearObject(
			txn.getPartitionId(), *getObjectManager(), PARTITION_HEADER_OID);
		partitionHeadearObject.setBackgroundMapOId(
			bgMap.get()->getBaseOId());
	}
	activeBackgroundCount_[pId]--;
}

void DataStore::updateBGTask(TransactionContext &txn, PartitionId pId, BackgroundId bgId, 
	BackgroundData &beforBgData, BackgroundData &afterBgData) {
	StackAllocAutoPtr<BtreeMap> bgMap(
		txn.getDefaultAllocator(), getBackgroundMap(txn, pId));

	bool isCaseSensitive = true;
	int32_t status =
		bgMap.get()->update<BackgroundId, BackgroundData>(txn, bgId, beforBgData, afterBgData, isCaseSensitive);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		DataStorePartitionHeaderObject partitionHeadearObject(
			txn.getPartitionId(), *getObjectManager(), PARTITION_HEADER_OID);
		partitionHeadearObject.setBackgroundMapOId(
			bgMap.get()->getBaseOId());
	}
}

/*!
	@brief Check if Partition is ready for redo log
*/
bool DataStore::isRestored(PartitionId pId) const {
	return (dbState_[pId] >= RESTORED);
}

/*!
	@brief Check if Partition already finished undo log
*/
bool DataStore::isUndoCompleted(PartitionId pId) const {
	return (dbState_[pId] >= UNDO_COMPLETED);
}

/*!
	@brief Set information that Partition already finished undo log
*/
void DataStore::setUndoCompleted(PartitionId pId) {
	if (dbState_[pId] >= RESTORED) {
		dbState_[pId] = UNDO_COMPLETED;
	}
}

void DataStore::setUnrestored(PartitionId pId) {
	dbState_[pId] = UNRESTORED;
}

void DataStore::setRestored(PartitionId pId) {
	if (dbState_[pId] == UNRESTORED) {
		dbState_[pId] = RESTORED;
	}
}

/*!
	@brief Prepare for redo log
*/
void DataStore::restartPartition(
	TransactionContext &txn, ClusterService *clusterService) {
	restoreContainerIdTable(txn, clusterService);
	restoreBackground(txn, clusterService);
	setRestored(txn.getPartitionId());

	restoreLastExpiredTime(txn, clusterService);

}

void DataStore::restoreLastExpiredTime(TransactionContext &txn, ClusterService *clusterService) {
	PartitionId pId = txn.getPartitionId();
	if (!objectManager_->existPartition(pId)) {
		return;
	}

	const DataStore::Latch latch(
		txn, pId, this, clusterService);

	DataStorePartitionHeaderObject partitionHeadearObject(
		txn.getPartitionId(), *getObjectManager(), PARTITION_HEADER_OID);

	Timestamp lastCheckTime = partitionHeadearObject.getLatestCheckTime();
	Timestamp lastChunkKeyTime = DataStore::convertChunkKey2Timestamp(
		partitionHeadearObject.getChunkKey());
	if (lastChunkKeyTime > lastCheckTime) {
		partitionHeadearObject.setLatestCheckTime(lastChunkKeyTime);
	}
	expirationStat_[pId].expiredTime_ = partitionHeadearObject.getLatestCheckTime();
}


/*!
	@brief Restore ContainerIdTable in the partition
*/
bool DataStore::restoreContainerIdTable(
	TransactionContext &txn, ClusterService *clusterService) {
	bool isGsContainersExist = false;
	if (!objectManager_->existPartition(txn.getPartitionId())) {
		return isGsContainersExist;
	}

	const DataStore::Latch latch(
		txn, txn.getPartitionId(), this, clusterService);

	DataStorePartitionHeaderObject partitionHeadearObject(
		txn.getPartitionId(), *getObjectManager(), PARTITION_HEADER_OID);
	BtreeMap containerMap(txn, *getObjectManager(),
		partitionHeadearObject.getMetaMapOId(), allocateStrategy_, NULL);


	size_t containerListSize = 0;
	BtreeMap::BtreeCursor btreeCursor;
	while (1) {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		util::XArray<OId> idList(txn.getDefaultAllocator());  
		int32_t getAllStatus =
			containerMap.getAll(txn, PARTIAL_RESULT_SIZE, idList, btreeCursor);

		for (size_t i = 0; i < idList.size(); ++i) {
			OId oId = idList[i];
			BaseObject baseContainerImageObject(
				txn.getPartitionId(), *getObjectManager(), oId);
			BaseContainer::BaseContainerImage *baseContainerImage =
				baseContainerImageObject
					.getBaseAddr<BaseContainer::BaseContainerImage *>();
			ContainerId containerId = baseContainerImage->containerId_;
			PartitionId pId = txn.getPartitionId();
			ContainerForRestoreAutoPtr containerAutoPtr(
				txn, this, pId, oId,
				containerId,
				baseContainerImage->containerType_);
			BaseContainer *container = containerAutoPtr.getBaseContainer();

			const FullContainerKey containerKey = container->getContainerKey(txn);
			util::String containerName(txn.getDefaultAllocator());
			containerKey.toString(txn.getDefaultAllocator(), containerName);
			const DatabaseId databaseVersionId =
				containerKey.getComponents(txn.getDefaultAllocator()).dbId_;
			ContainerAttribute attribute = container->getAttribute();
			containerIdTable_->set(
				pId, containerId, oId, databaseVersionId, attribute);
			TEST_PRINT("changeSchema \n");
			TEST_PRINT1("pId %d\n", pId);
			TEST_PRINT1("databaseVersionId %d\n", databaseVersionId);
			TEST_PRINT1("attribute %d\n", (int32_t)attribute);

			containerListSize++;
		}
		if (getAllStatus == GS_SUCCESS) {
			break;
		}
	}

	GS_TRACE_INFO(DATA_STORE, GS_TRACE_DS_DS_CONTAINER_ID_TABLE_STATUS,
		"restore pId," << txn.getPartitionId() << ",containerListSize,"
					   << containerListSize);

	return isGsContainersExist;
}

/*!
	@brief Restore BackgroundCounter in the partition
*/
void DataStore::restoreBackground(
	TransactionContext &txn, ClusterService *clusterService) {
	PartitionId pId = txn.getPartitionId();
	if (!objectManager_->existPartition(pId)) {
		return;
	}

	const DataStore::Latch latch(
		txn, pId, this, clusterService);

	StackAllocAutoPtr<BtreeMap> bgMap(
		txn.getDefaultAllocator(), getBackgroundMap(txn, pId));

	BtreeMap::BtreeCursor btreeCursor;
	while (1) {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		util::XArray< std::pair<BackgroundId, BackgroundData> > idList(
			txn.getDefaultAllocator());
		util::XArray< std::pair<BackgroundId, BackgroundData> >::iterator itr;
		int32_t getAllStatus = bgMap.get()->getAll<BackgroundId, BackgroundData>
			(txn, PARTIAL_RESULT_SIZE, idList, btreeCursor);

		for (itr = idList.begin(); itr != idList.end(); itr++) {
			BackgroundData bgData = itr->second;
			if (!bgData.isInvalid()) {
				activeBackgroundCount_[pId]++;
			} else {
				GS_TRACE_WARNING(
					DATA_STORE, GS_ERROR_DS_BACKGROUND_TASK_INVALID, 
					"Invalid BGTask= " << bgData);
			}
		}
		if (getAllStatus == GS_SUCCESS) {
			break;
		}
	}
}

/*!
	@brief Create Partition
*/
void DataStore::createPartition(PartitionId) {
}

/*!
	@brief Drop Partition
*/
void DataStore::dropPartition(PartitionId pId) {
	try {
		GS_TRACE_INFO(
			DATA_STORE, GS_TRACE_DS_DS_DROP_PARTITION, "pId = " << pId);
		if (objectManager_->existPartition(pId)) {
			objectManager_->dropPartition(pId);
			containerIdTable_->dropPartition(pId);
			activeBackgroundCount_[pId] = 0;
			expirationStat_[pId] = ExpirationStat();
		}

		setUnrestored(pId);
	}
	catch (std::exception &e) {
		handleUpdateError(e, GS_ERROR_DS_DS_DROP_PARTITION_FAILED);
	}
}

DataStore::ContainerIdTable::ContainerIdTable(uint32_t partitionNum)
	: partitionNum_(partitionNum) {
	try {
		containerIdMap_ = UTIL_NEW ContainerIdMap[partitionNum];
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

DataStore::ContainerIdTable::~ContainerIdTable() {
	delete[] containerIdMap_;
}

/*!
	@brief Get Container ObjectId by ContainerId
*/
OId DataStore::ContainerIdTable::get(PartitionId pId, ContainerId containerId) {
	ContainerIdMap::const_iterator itr = containerIdMap_[pId].find(containerId);
	if (itr == containerIdMap_[pId].end()) {
		return UNDEF_OID;
	}
	else {
		return itr->second.oId_;
	}
}


/*!
	@brief Set value(ContainerId, ContainerInfoCache)
*/
void DataStore::ContainerIdTable::set(PartitionId pId, ContainerId containerId,
	OId oId, int64_t databaseVersionId, ContainerAttribute attribute) {
	try {
		std::pair<ContainerIdMap::iterator, bool> itr;
		ContainerInfoCache containerInfoCache(
			oId, databaseVersionId, attribute);
		itr = containerIdMap_[pId].insert(
			std::make_pair(
				containerId, containerInfoCache));
		if (!itr.second) {
			GS_THROW_SYSTEM_ERROR(
				GS_ERROR_DS_DS_CONTAINER_ID_INVALID, "duplicate container id");
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Remove value by ContainerId key
*/
void DataStore::ContainerIdTable::remove(
	PartitionId pId, ContainerId containerId) {
	ContainerIdMap::size_type result = containerIdMap_[pId].erase(containerId);
	if (result == 0) {
		GS_TRACE_WARNING(DATA_STORE, GS_TRACE_DS_DS_CONTAINER_ID_TABLE_STATUS,
			"DataStore::ContainerIdTable::remove: out of bounds");
	}
}

/*!
	@brief Remove all values
*/
void DataStore::ContainerIdTable::dropPartition(PartitionId pId) {
	containerIdMap_[pId].clear();
}

/*!
	@brief Get list of all ContainerId in the map
*/
void DataStore::ContainerIdTable::getList(
	PartitionId pId, int64_t start, ResultSize limit, ContainerIdList &list) {
	try {
		list.clear();
		if (static_cast<uint64_t>(start) > size(pId)) {
			return;
		}
		int64_t skipCount = 0;
		ResultSize listCount = 0;
		bool inRange = false;
		ContainerIdMap::const_iterator itr;
		for (itr = containerIdMap_[pId].begin();
			 itr != containerIdMap_[pId].end(); itr++) {
			++skipCount;
			if (!inRange && skipCount > start) {
				inRange = true;
			}
			if (inRange) {
				if (listCount >= limit) {
					break;
				}
				if (listCount >
					DataStore::CONTAINER_NAME_LIST_NUM_UPPER_LIMIT) {
					GS_THROW_USER_ERROR(
						GS_ERROR_DS_DS_GET_CONTAINER_LIST_FAILED,
						"Numbers of containers exceed an upper limit level.");
				}
				list.push_back(*itr);
				++listCount;
			}
		}
		return;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "Failed to list container"));
	}
}

bool DataStore::ContainerIdTable::getListOrdered(
		PartitionId pId, ContainerId startId, uint64_t limit,
		const DatabaseId *dbId, ContainerCondition &condition,
		ContainerIdRefList &list) const {
	list.clear();
	list.reserve(std::min<uint64_t>(containerIdMap_[pId].size(), limit));

	const util::Set<ContainerAttribute> &attributes = condition.getAttributes();
	containerIdMapAsc pred;

	bool followingFound = false;
	for (ContainerIdMap::const_iterator itr = containerIdMap_[pId].begin();
			itr != containerIdMap_[pId].end(); ++itr) {

		const ContainerId id = itr->first;
		if (id < startId) {
			continue;
		}

		if (dbId != NULL && itr->second.databaseVersionId_ != *dbId) {
			continue;
		}

		if (attributes.find(itr->second.attribute_) == attributes.end()) {
			continue;
		}

		const ContainerIdRefList::value_type entry(itr->first, &itr->second);

		if (list.size() >= limit) {
			followingFound = true;

			if (list.empty()) {
				break;
			}

			std::pop_heap(list.begin(), list.end(), pred);
			if (pred(entry, list.back())) {
				list.back() = entry;
			}
		}
		else {
			list.push_back(entry);
		}

		std::push_heap(list.begin(), list.end(), pred);
	}

	std::sort_heap(list.begin(), list.end(), pred);
	return followingFound;
}

void DataStore::dumpPartition(TransactionContext &txn, PartitionId pId,
	DumpType type, const char *fileName) {
	try {
		util::NamedFile *file;
		file = UTIL_NEW util::NamedFile();
		file->open(fileName, util::FileFlag::TYPE_CREATE |
								 util::FileFlag::TYPE_TRUNCATE |
								 util::FileFlag::TYPE_WRITE_ONLY);

		if (!objectManager_->existPartition(pId)) {
			file->close();
			return;
		}

		util::NormalOStringStream stream;
		stream << "PartitionId = " << pId << std::endl;
		if (type == CONTAINER_SUMMARY) {
			stream << "ContainerId, ContainerName, ContainerType, RowNum(, "
					  "Map, ColumnNo, RowNum)*"
				   << std::endl;
		}
		file->write(stream.str().c_str(), stream.str().size());
		stream.str("");  

		ContainerIdTable::ContainerIdList list(txn.getDefaultAllocator());
		containerIdTable_->getList(pId, 0, INT64_MAX, list);

		for (size_t i = 0; i < list.size(); ++i) {
			ContainerAutoPtr containerAutoPtr(
				txn, this, pId, list[i].first, ANY_CONTAINER);
			BaseContainer *container = containerAutoPtr.getBaseContainer();

			const FullContainerKey &containerKey = container->getContainerKey(txn);
			util::String containerName(txn.getDefaultAllocator());
			containerKey.toString(txn.getDefaultAllocator(), containerName);

			stream << "" << container->getContainerId() << ","
				   << (int32_t)container->getContainerType() << ","
				   << containerName;




			stream << std::endl;
			file->write(stream.str().c_str(), stream.str().size());
			stream.str("");  

			if (type != CONTAINER_SUMMARY) {
				stream << container->dump(txn) << std::endl;
				stream << std::endl;
				file->write(stream.str().c_str(), stream.str().size());
				stream.str("");  
			}
			else {
				stream << "row num = " << container->getRowNum() << std::endl;
				stream << std::endl;
				file->write(stream.str().c_str(), stream.str().size());
				stream.str("");  
			}
		}

		file->close();
		delete file;
	}
	catch (SystemException &) {
		throw;
	}
	catch (std::exception &) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_OM_OPEN_DUMP_FILE_FAILED, "");
	}
}



#include <cstdlib>

#ifdef _WIN32
#define CATDLL_API extern "C" __declspec(dllimport)
#include <windows.h>
#else
#include <dlfcn.h>
#endif

#ifdef _WIN32
LPCWSTR createLStr(char *str) {
	std::string path = str;
  std::wstring wpath = std::wstring(path.begin(), path.end());
  LPCWSTR lpath = wpath.c_str();
  return lpath;
}

void *LoadFuncOrDie(HINSTANCE *lib, const std::string& func_name) {

  void *func = GetProcAddress(*lib, func_name.c_str());
  printf("%d\n",GetLastError()); 
  if (!func) {
	std::cerr << "Cannot load symbol create: " << func_name.c_str() << std::endl;
    exit(EXIT_FAILURE);
  }
  return func;
}

HINSTANCE *LoadLibOrDie(const std::string& path) {
  HINSTANCE *lib = new HINSTANCE;

  std::wstring wpath = std::wstring(path.begin(), path.end());
  LPCWSTR lpath = wpath.c_str();
  printf("%d\n",GetLastError()); 
  *lib = LoadLibrary(lpath);
  printf("%d\n",GetLastError()); 
  if (!lib) {
    std::cerr << "Cannot load library: " << path.c_str() << std::endl;
    exit(EXIT_FAILURE);
  }
  return lib;
}
#else
void *LoadFuncOrDie(void *lib, const std::string& func_name) {
	void *func = dlsym(lib, func_name.c_str());
	const char *dlsym_error = dlerror();
	if (dlsym_error) {
		std::cerr << "Cannot load symbol create: " << dlsym_error << std::endl;
		dlclose(lib);
		exit(EXIT_FAILURE);
	}
	return func;
}

void *LoadLibOrDie(const std::string& path) {
	void *lib = dlopen(path.c_str(), RTLD_LAZY);
	if (!lib) {
		std::cerr << "Cannot load library: " << dlerror() << std::endl;
		exit(EXIT_FAILURE);
	}
	return lib;
}
#endif

void DataStore::archive(util::StackAllocator &alloc, TransactionManager &txnMgr, BibInfo &bibInfo) {
	try {


		std::string fileType = bibInfo.option_.containerFileType_;
		int64_t flushThreshold = bibInfo.option_.flushThreshold_;
		int64_t blockSize = bibInfo.option_.blockSize_;
		ResultSize preReadNum = bibInfo.option_.preReadNum_;
#ifdef _WIN32
		std::string archiveHandler = "..\\lib\\longArchive\\" + fileType + "\\debug\\" + fileType + ".dll";
		HINSTANCE *archiveLib = LoadLibOrDie(archiveHandler);
#else
		std::string archiveHandler = "lib" + fileType + ".so";
		void *archiveLib = LoadLibOrDie(archiveHandler);
#endif
		std::vector< BibInfo::Container >::iterator itr;
		for (itr = bibInfo.containers_.begin(); itr != bibInfo.containers_.end(); itr++) {
			std::string fileBase = itr->containerFileBase_;
			std::string dbName = itr->database_;
			std::string containerName = itr->container_;
			PartitionId pId = static_cast<PartitionId>(itr->partitionNo_);

			if (!objectManager_->existPartition(pId)) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_OM_OPEN_DUMP_FILE_FAILED, "not exist pId = " << pId);
			}

			util::TimeZone timeZone;
			ClientId clientId;
			TransactionManager::ContextSource cxtSrc;
			util::DateTime now(0);
			EventMonotonicTime emNow = 0;
			TransactionContext &txn = txnMgr.put(alloc, pId, clientId, cxtSrc, now, emNow);
			StackAllocAutoPtr<BaseContainer> stackAutoPtr(txn.getDefaultAllocator(),
				getBaseContainer(txn, pId, *itr));
			BaseContainer *container = stackAutoPtr.get();

			{
				util::Set<TransactionId> activTxnSet(alloc);
				util::Set<TransactionId>::iterator txnItr;
				container->getActiveTxnList(txn, activTxnSet);

				ClientId mvccClientId;
				const bool isRedo = true;
				for (txnItr = activTxnSet.begin(); txnItr != activTxnSet.end(); txnItr++) {
					mvccClientId.sessionId_++;
					const TransactionManager::ContextSource src(ABORT_TRANSACTION,
						INITIAL_STATEMENTID, container->getContainerId(),
						TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL, 
						TransactionManager::PUT,
						TransactionManager::NO_AUTO_COMMIT_BEGIN_OR_CONTINUE,
						false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE, timeZone);
					TransactionContext &txn =
						txnMgr.put(alloc, pId, mvccClientId, src,
							now, emNow, isRedo, *txnItr);
					UNUSED_VARIABLE(txn);
				}
			}

			ArchiveHandler *(*CreateArchiveHandler)(const void *output, const void *threshold, const void *blockSize) = reinterpret_cast<ArchiveHandler *(*)(const void *output, const void *threshold, const void *blockSize)>(LoadFuncOrDie(archiveLib, "Create"));
			void (*DropArchiveHandler)(ArchiveHandler *) = reinterpret_cast<void (*)(ArchiveHandler *)>(LoadFuncOrDie(archiveLib, "Drop"));
			ArchiveHandler* handler = CreateArchiveHandler(fileBase.c_str(), &flushThreshold, &blockSize);
			try {
				int32_t version = handler->getVersion();
				std::cout << "version = " << version << std::endl;
				handler->setServerVersion("");
				handler->setDbName(dbName.c_str());
				handler->setContainerName(containerName.c_str());
				container->archive(txn, handler, preReadNum);
				DropArchiveHandler(handler);
			} catch (std::exception &) {
				DropArchiveHandler(handler);
				throw;
			}
		}
	}
	catch (std::exception &e) {
		handleSearchError(e, GS_ERROR_DS_DS_ARCHIVE_FAILED);
		return;
	}
}


MessageSchema *DataStore::makeMessageSchema(TransactionContext &txn, ContainerType containerType, const BibInfo::Container &bibInfo) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	MessageSchema *messageSchema = NULL;

	switch (containerType) {
	case COLLECTION_CONTAINER:
		messageSchema = ALLOC_NEW(alloc)
			MessageCollectionSchema(alloc,
				getValueLimitConfig(), bibInfo);
		break;
	case TIME_SERIES_CONTAINER:
		messageSchema = ALLOC_NEW(alloc)
			MessageTimeSeriesSchema(alloc,
				getValueLimitConfig(), bibInfo);
		break;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_TYPE_INVALID, "");
		break;
	}
	return messageSchema;
}

ShareValueList *DataStore::makeCommonContainerSchema(TransactionContext &txn, int64_t schemaHashKey, MessageSchema *messageSchema)
{
	uint32_t bodyAllocateSize;
	util::XArray<ShareValueList::ElemData> inputList(
		txn.getDefaultAllocator());

	bool onMemory = true;

	BaseContainer::initializeSchema(txn, *getObjectManager(), messageSchema,
		allocateStrategy_, inputList, bodyAllocateSize, onMemory);
	uint32_t allocateSize = ShareValueList::getHeaderSize(
								static_cast<int32_t>(inputList.size())) +
							bodyAllocateSize;
	ShareValueList *commonContainerSchema =
		ALLOC_NEW(txn.getDefaultAllocator()) ShareValueList(txn,
			*getObjectManager());
	commonContainerSchema->initialize(txn, allocateSize, allocateStrategy_, onMemory);
	commonContainerSchema->set(schemaHashKey, inputList);

	return commonContainerSchema;
}

BaseContainer *DataStore::getBaseContainer(TransactionContext &txn, PartitionId pId,
	const BibInfo::Container &bibInfo) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();
	if (!objectManager_->existPartition(pId)) {
		return NULL;
	}

	BaseContainer::BaseContainerImage *containerImage = BaseContainer::makeBaseContainerImage(txn, bibInfo);
	MessageSchema *messageSchema = makeMessageSchema(txn, containerImage->containerType_, bibInfo);
	int64_t schemaHashKey = BaseContainer::calcSchemaHashKey(messageSchema);
	ShareValueList *commonContainerSchema = makeCommonContainerSchema(txn, schemaHashKey, messageSchema);

	ContainerType containerType = containerImage->containerType_;
	BaseContainer *container = NULL;
	switch (containerType) {
	case COLLECTION_CONTAINER: {
		container =
			ALLOC_NEW(alloc) Collection(txn, this, containerImage, commonContainerSchema);
	} break;
	case TIME_SERIES_CONTAINER: {
		container =
			ALLOC_NEW(alloc) TimeSeries(txn, this, containerImage, commonContainerSchema);
	} break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_CONTAINER_TYPE_UNKNOWN, "");
		break;
	}

	return container;
}

/*!
	@brief Returns number of Container in the partition
*/
uint64_t DataStore::getContainerCount(TransactionContext &txn, PartitionId pId,
	const DatabaseId dbId, ContainerCondition &condition) {
	uint64_t count = 0;
	try {
		ContainerIdTable::ContainerIdList list(txn.getDefaultAllocator());
		containerIdTable_->getList(pId, 0, INT64_MAX, list);
		const int64_t currentDatabaseVersionId = dbId;
		TEST_PRINT("getContainerCount ---------\n");
		TEST_PRINT1("partitionId %d\n", pId);
		TEST_PRINT1("listSize %d\n", list.size());

		for (size_t i = 0; i < list.size(); i++) {
			const ContainerAttribute attribute = list[i].second.attribute_;
			const int64_t databaseVersionId = list[i].second.databaseVersionId_;
			bool isDbMatch = databaseVersionId == currentDatabaseVersionId;

			const util::Set<ContainerAttribute> &conditionAttributes =
				condition.getAttributes();
			bool isAttributeMatch = conditionAttributes.find(attribute) !=
									conditionAttributes.end();
			TEST_PRINT1("list index %d ---------\n", i);
			TEST_PRINT1("partitionId %d\n", pId);
			TEST_PRINT1("databaseVersionId %d\n", databaseVersionId);
			TEST_PRINT1("attribute %d\n", (int32_t)attribute);
			TEST_PRINT1("currentDatabaseName %s\n", dbNameStr.c_str());
			TEST_PRINT1(
				"currentDatabaseVersionId %d\n", currentDatabaseVersionId);
			TEST_PRINT1("isAttributeMatch %d\n", isAttributeMatch);
			if (isDbMatch && isAttributeMatch) {
				count++;
			}
		}
	}
	catch (std::exception &e) {
		handleSearchError(e, GS_ERROR_DS_DS_GET_CONTAINER_LIST_FAILED);
	}
	return count;
}

bool DataStore::containerIdMapAsc::operator()(
	const std::pair<ContainerId, ContainerInfoCache> &left,
	const std::pair<ContainerId, ContainerInfoCache> &right) const {
	return left.first < right.first;
}

bool DataStore::containerIdMapAsc::operator()(
	const std::pair<ContainerId, const ContainerInfoCache*> &left,
	const std::pair<ContainerId, const ContainerInfoCache*> &right) const {
	return left.first < right.first;
}


/*!
	@brief Returns names of Container to meet a given condition in the partition
*/
void DataStore::getContainerNameList(TransactionContext &txn, PartitionId pId,
	int64_t start, ResultSize limit, const DatabaseId dbId,
	ContainerCondition &condition, util::XArray<FullContainerKey> &nameList) {
	nameList.clear();
	if (pId >= pgConfig_.getPartitionCount()) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_GET_CONTAINER_LIST_FAILED,
			"Illeagal parameter. PartitionId is out of range");
	}
	if (start < 0) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_GET_CONTAINER_LIST_FAILED,
			"Illeagal parameter. start < 0");
	}
	if (!objectManager_->existPartition(pId)) {
		return;
	}
	try {
		ContainerIdTable::ContainerIdList list(txn.getDefaultAllocator());
		containerIdTable_->getList(pId, 0, INT64_MAX, list);
		std::sort(list.begin(), list.end(), containerIdMapAsc());
		int64_t currentDatabaseVersionId = dbId;

		TEST_PRINT("getContainerName ---------\n");
		TEST_PRINT1("partitionId %d\n", pId);
		TEST_PRINT1("listSize %d\n", list.size());
		TEST_PRINT1("currentDatabaseName %s\n", dbNameStr.c_str());
		TEST_PRINT1("currentDatabaseVersionId %d\n", currentDatabaseVersionId);

		int64_t count = 0;
		nameList.clear();
		for (size_t i = 0; i < list.size() && nameList.size() < limit; i++) {
			const ContainerAttribute attribute = list[i].second.attribute_;
			const int64_t databaseVersionId = list[i].second.databaseVersionId_;
			if (currentDatabaseVersionId == UNDEF_DBID) {
				currentDatabaseVersionId = databaseVersionId;
			}
			bool isDbMatch = databaseVersionId == currentDatabaseVersionId;
			const util::Set<ContainerAttribute> &conditionAttributes =
				condition.getAttributes();
			bool isAttributeMatch = conditionAttributes.find(attribute) !=
									conditionAttributes.end();
			TEST_PRINT1("list index %d ---------\n", i);
			TEST_PRINT1("databaseVersionId %d\n", databaseVersionId);
			TEST_PRINT1("attribute %d\n", (int32_t)attribute);
			TEST_PRINT1("isAttributeMatch %d\n", isAttributeMatch);
			if (isDbMatch && isAttributeMatch) {
				if (count >= start) {
					ContainerAutoPtr containerAutoPtr(
						txn, this, pId, list[i].first, ANY_CONTAINER);
					util::StackAllocator &alloc = txn.getDefaultAllocator();
					const void *srcBody;
					size_t bodySize = 0;
					containerAutoPtr.getBaseContainer()->getContainerKey(txn).toBinary(srcBody, bodySize);
					void *destBody = alloc.allocate(bodySize);
					memcpy(destBody, srcBody, bodySize);
					nameList.push_back(
						FullContainerKey(alloc, 
							KeyConstraint::getNoLimitKeyConstraint(), destBody, bodySize));
				}
				count++;
			}
		}
	}
	catch (std::exception &e) {
		handleSearchError(e, GS_ERROR_DS_DS_GET_CONTAINER_LIST_FAILED);
	}
}

DataStore::ContainerListHandler::~ContainerListHandler() {
}

bool DataStore::scanContainerList(
		TransactionContext &txn, PartitionId pId, ContainerId startContainerId,
		uint64_t limit, const DatabaseId *dbId, ContainerCondition &condition,
		const ContainerListHandler &handler) {

	util::StackAllocator &alloc = txn.getDefaultAllocator();
	util::StackAllocator::Scope scope(alloc);

	typedef ContainerIdTable::ContainerIdRefList ContainerIdRefList;
	ContainerIdRefList list(alloc);

	const bool followingFound = containerIdTable_->getListOrdered(
			pId, startContainerId, limit, dbId, condition, list);

	for (ContainerIdRefList::iterator itr = list.begin();
			itr != list.end(); ++itr) {
		util::StackAllocator::Scope subScope(alloc);

		bool isAllowExpiration = true;
		ContainerAutoPtr containerAutoPtr(txn, this, pId, itr->first, ANY_CONTAINER,
			isAllowExpiration);
		BaseContainer *baseContainer = containerAutoPtr.getBaseContainer();
		if (baseContainer == NULL) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_CONTAINER_UNEXPECTEDLY_REMOVED, "");
		}
		handler(
				txn,
				itr->first,
				itr->second->databaseVersionId_,
				itr->second->attribute_,
				*baseContainer);
	}

	return followingFound;
}

DataStore::Latch::Latch(TransactionContext &txn, PartitionId pId,
	DataStore *dataStore, ClusterService *clusterService)
	: pId_(pId),
	  txn_(txn),
	  dataStore_(dataStore),
	  clusterService_(clusterService) {
	dataStore_->getObjectManager()->checkDirtyFlag(
		txn.getPartitionId());  
	ObjectManager &objectManager = *(dataStore_->getObjectManager());
	if (dataStore_ != NULL && objectManager.existPartition(pId_)) {
		const double HOT_MODE_RATE = 1.0;
		objectManager.setStoreMemoryAgingSwapRate(pId_, HOT_MODE_RATE);
	}
}

DataStore::Latch::~Latch() {
	try {
		ObjectManager &objectManager = *(dataStore_->getObjectManager());
		if (dataStore_ != NULL && objectManager.existPartition(pId_)) {
			objectManager.checkDirtyFlag(
				txn_.getPartitionId());  
			objectManager.resetRefCounter(pId_);
			objectManager.freeLastLatchPhaseMemory(pId_);
			objectManager.setSwapOutCounter(pId_, 0);

		}
	}
	catch (std::exception &e) {
		GS_TRACE_ERROR(
			DATA_STORE, GS_TRACE_DS_DS_LATCH_STATUS, "UnLatch Failed");
		if (clusterService_ != NULL) {
			EventEngine::VariableSizeAllocator varSizeAlloc(
				util::AllocatorInfo(ALLOCATOR_GROUP_STORE, "latch"));
			Event::Source eventSource(varSizeAlloc);
			clusterService_->setSystemError(&e);
		}
	}
}


/*!
	@brief Handle Exception of update phase
*/
void DataStore::handleUpdateError(std::exception &, ErrorCode) {
	try {
		throw;
	}
	catch (SystemException &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
	catch (UserException &e) {
		if (e.getErrorCode() == GS_ERROR_CM_NO_MEMORY ||
			e.getErrorCode() == GS_ERROR_CM_MEMORY_LIMIT_EXCEEDED ||
			e.getErrorCode() == GS_ERROR_CM_SIZE_LIMIT_EXCEEDED) {
			GS_RETHROW_SYSTEM_ERROR(e, "");
		}
		else {
			GS_RETHROW_USER_ERROR(e, "");
		}
	}
	catch (LockConflictException &e) {
		DS_RETHROW_LOCK_CONFLICT_ERROR(e, "");
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Handle Exception of search phase
*/
void DataStore::handleSearchError(std::exception &, ErrorCode) {
	try {
		throw;
	}
	catch (SystemException &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
	catch (UserException &e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
	catch (LockConflictException &e) {
		DS_RETHROW_LOCK_CONFLICT_ERROR(e, "");
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

PartitionId DataStore::resolvePartitionId(
		util::StackAllocator &alloc, const FullContainerKey &containerKey,
		PartitionId partitionCount, ContainerHashMode hashMode) {
	assert(partitionCount > 0);

	const FullContainerKeyComponents normalizedComponents =
			containerKey.getComponents(alloc, false);

	if (normalizedComponents.affinityNumber_ != UNDEF_NODE_AFFINITY_NUMBER) {
		return static_cast<PartitionId>(
				normalizedComponents.affinityNumber_ % partitionCount);
	}
	else if (normalizedComponents.affinityStringSize_ > 0) {
		const uint32_t crcValue = util::CRC32::calculate(
				normalizedComponents.affinityString_,
				normalizedComponents.affinityStringSize_);
		return (crcValue % partitionCount);
	}
	else {
		const char8_t *baseContainerName =
				(normalizedComponents.baseNameSize_ == 0 ?
						"" : normalizedComponents.baseName_);
		const uint32_t crcValue = util::CRC32::calculate(
				baseContainerName,
				normalizedComponents.baseNameSize_);
		return (crcValue % partitionCount);
	}
}


/*!
	@brief Frees all Objects on the Chunks, older than timestamp of ChunkKey.
*/
bool DataStore::executeBatchFree(util::StackAllocator &alloc, PartitionId pId, 
	Timestamp timestamp, uint64_t maxScanNum, uint64_t &scanNum) {
	try {
		if (!isRestored(pId) || !getObjectManager()->existPartition(pId)) {
			bool isTail = true;
			return isTail;
		}
		util::Stopwatch timer;
		timer.start();
		Timestamp expireTime = timestamp;
		if (!getConfig().isAutoExpire() && timestamp > getConfig().getErasableExpiredTime()) {
			expireTime = getConfig().getErasableExpiredTime();
		}
		uint64_t roundingBitNum = ChunkManager::DataAffinityUtils::getExpireTimeRoundingBitNum(DEFAULT_EXPIRE_CATEGORY_ID);
		ChunkKey chunkKey = ChunkManager::DataAffinityUtils::convertTimestamp2ChunkKey(
			expireTime,	roundingBitNum, false);

		util::XArray<OId> freeList(alloc);
		uint64_t simulateFreeNum = 0;
		ChunkKey simulateChunkKey = ChunkManager::DataAffinityUtils::convertTimestamp2ChunkKey(
			getConfig().getSimulateErasableExpiredTime(), roundingBitNum, false);
		bool isTail = objectManager_->batchFree(
			pId, chunkKey, maxScanNum, scanNum, freeList,
			simulateChunkKey, simulateFreeNum);
		if (!freeList.empty()) {
			setLastExpiredTime(pId, DataStore::convertChunkKey2Timestamp(chunkKey));
			setLatestBatchFreeTime(pId, DataStore::convertChunkKey2Timestamp(chunkKey));
		}
		int64_t scanTime = timer.elapsedNanos() / 1000;
		updateExpirationStat(pId, freeList.size(), 
			simulateFreeNum, scanNum, isTail, scanTime);
		util::XArray<OId>::iterator itr;
		for (itr = freeList.begin(); itr != freeList.end(); itr++) {
			addRemovedChunk(pId, *itr);
		}
		return isTail;
	}
	catch (std::exception &e) {
		handleUpdateError(e, GS_ERROR_DS_DS_CREATE_COLLECTION_FAILED);
		return false;
	}
}

void DataStore::setLastExpiredTime(PartitionId pId, Timestamp time, bool force) {
	DataStorePartitionHeaderObject partitionHeadearObject(
		pId, *objectManager_, PARTITION_HEADER_OID);
	partitionHeadearObject.setLatestCheckTime(time, force);

	uint64_t roundingBitNum = ChunkManager::DataAffinityUtils::getExpireTimeRoundingBitNum(DEFAULT_EXPIRE_CATEGORY_ID);
	ChunkKey chunkKey = ChunkManager::DataAffinityUtils::convertTimestamp2ChunkKey(
		time, roundingBitNum, false);
	partitionHeadearObject.setChunkKey(chunkKey, force);
	if (force || expirationStat_[pId].expiredTime_ < time) {
		expirationStat_[pId].expiredTime_ = time;
		GS_TRACE_INFO(
			DATA_STORE, GS_ERROR_DS_BACKGROUND_TASK_INVALID, "setLastExpiredTime = " << time);
	}
}

void DataStore::setLatestBatchFreeTime(PartitionId pId, Timestamp time, bool force) {
	DataStorePartitionHeaderObject partitionHeadearObject(
		pId, *objectManager_, PARTITION_HEADER_OID);
	partitionHeadearObject.setLatestBatchFreeTime(time, force);

	GS_TRACE_INFO(
		DATA_STORE, GS_ERROR_DS_BACKGROUND_TASK_INVALID, "setLatestBatchFreeTime = " << partitionHeadearObject.getLatestBatchFreeTime());
}



DataStore::ConfigSetUpHandler DataStore::configSetUpHandler_;

void DataStore::ConfigSetUpHandler::operator()(ConfigTable &config) {
	CONFIG_TABLE_RESOLVE_GROUP(config, CONFIG_TABLE_DS, "dataStore");

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_PARTITION_NUM, INT32)
		.setMin(1)
		.setMax(static_cast<int32_t>(DataStore::MAX_PARTITION_NUM))
		.setDefault(128);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_STORE_BLOCK_SIZE, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_SIZE_B, true)
		.add("32KB")
		.add("64KB")
		.add("1MB")
		.add("4MB")
		.add("8MB")
		.add("16MB")
		.add("32MB")
		.setDefault("64KB");

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_DB_PATH, STRING)
		.setDefault("data");
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_BACKUP_PATH, STRING)
		.setDefault("backup");
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_SYNC_TEMP_PATH, STRING)
		.setDefault("sync");  
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_ARCHIVE_TEMP_PATH, STRING)
		.setDefault("archive");  
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_CHUNK_MEMORY_LIMIT, INT32)
		.deprecate();
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_STORE_MEMORY_LIMIT, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_SIZE_MB)
		.alternate(CONFIG_TABLE_DS_CHUNK_MEMORY_LIMIT)
		.setMin(1)
		.setDefault(1024)
		.setMax("128TB");
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_CONCURRENCY, INT32)
		.setMin(1)
		.setMax(128)
		.setDefault(4);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_LOG_WRITE_MODE, INT32)
		.setMin(-1)
		.setDefault(1);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_STORE_WARM_START, BOOL)
		.setExtendedType(ConfigTable::EXTENDED_TYPE_LAX_BOOL)
		.setDefault(false);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_AFFINITY_GROUP_SIZE, INT32)
		.setMin(1)
		.setMax(10000)
		.setDefault(7);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_DS_STORE_COMPRESSION_MODE, INT32)
		.setExtendedType(ConfigTable::EXTENDED_TYPE_ENUM)
		.addEnum(ChunkManager::NO_BLOCK_COMPRESSION, "NO_COMPRESSION")
		.addEnum(ChunkManager::BLOCK_COMPRESSION, "COMPRESSION")
		.setDefault(ChunkManager::NO_BLOCK_COMPRESSION);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_DS_IO_WARNING_THRESHOLD_MILLIS, INT32)
		.deprecate();
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_DS_IO_WARNING_THRESHOLD_TIME, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_MS)
		.alternate(CONFIG_TABLE_DS_IO_WARNING_THRESHOLD_MILLIS)
		.setMin(1)
		.setDefault(
			static_cast<int32_t>(IO_MONITOR_DEFAULT_WARNING_THRESHOLD_MILLIS));
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_DS_RESULT_SET_MEMORY_LIMIT, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_SIZE_MB)
		.setMin(1)
		.setDefault(10240);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_DS_RESULT_SET_CACHE_MEMORY, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_SIZE_MB)
		.setMin(0)
		.setDefault(0);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_RECOVERY_LEVEL, INT32)
		.add(0)
		.add(1)
		.setDefault(0);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_BACKGROUND_MIN_RATE, DOUBLE)
		.setMin(0.1)
		.setDefault(0.1)
		.setMax(1.0);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_ERASABLE_EXPIRED_TIME, STRING)
		.setDefault("1970-01-01T00:00:00.000Z");
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_AUTO_EXPIRE, BOOL)
		.setDefault(true);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_ESTIMATED_ERASABLE_EXPIRED_TIME, STRING)
		.setDefault("1970-01-01T00:00:00.000Z");
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_BATCH_SCAN_NUM, INT32)
		.setMin(500)
		.setDefault(2000)
		.setMax(100000);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_ROW_ARRAY_RATE_EXPONENT, INT32)
		.setMin(1)
		.setDefault(4)
		.setMax(10);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_ROW_ARRAY_SIZE_CONTROL_MODE, BOOL)
		.setDefault(true);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_PARTITION_BATCH_FREE_CHECK_INTERVAL, INT32)
		.setMin(60)
		.setDefault(60*60*6)
		.setMax(INT32_MAX);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_STORE_MEMORY_REDISTRIBUTE_SHIFTABLE_MEMORY_RATE, INT32)
		.setMin(0)
		.setMax(100)
		.setDefault(80);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_STORE_MEMORY_REDISTRIBUTE_EMA_HALF_LIFE_PERIOD, INT32)
		.setMin(2)
		.setMax(10000)
		.setDefault(240); 
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_STORE_MEMORY_AGING_SWAP_RATE, DOUBLE)
		.setMin(0.0)
		.setDefault(0.0005)
		.setMax(1.0);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_STORE_MEMORY_COLD_RATE, DOUBLE)
		.setMin(0.0)
		.setDefault(0.375)
		.setMax(1.0);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_DS_CHECKPOINT_FILE_FLUSH_SIZE, INT64)
		.setUnit(ConfigTable::VALUE_UNIT_SIZE_B)  
		.setMin(0)
		.setMax("128TB")
		.setDefault(0);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_DS_CHECKPOINT_FILE_AUTO_CLEAR_CACHE, BOOL)
		.setDefault(true);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_DS_STORE_BUFFER_TABLE_SIZE_RATE, DOUBLE)
		.setMin(0.0)
		.setDefault(0.01)
		.setMax(1.0);
	picojson::value defaultValue;

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_DS_DB_FILE_PATH_LIST, JSON)
		.setDefault(defaultValue);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_DS_DB_FILE_SPLIT_COUNT, INT32)
		.setMin(1)
		.setDefault(0)
		.setMax(128);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_DS_DB_FILE_SPLIT_STRIPE_SIZE, INT32)
		.setMin(1)
		.setDefault(1)
		.setMax(INT32_MAX);
}

DataStore::StatSetUpHandler DataStore::statSetUpHandler_;

#define STAT_ADD_SUB(id) STAT_TABLE_ADD_PARAM_SUB(stat, parentId, id)
#define STAT_ADD_SUB_SUB(id) STAT_TABLE_ADD_PARAM_SUB_SUB(stat, parentId, id)
#define CHUNK_STAT_ADD_PARAM(id)                           \
	stat.addParam(parentId, id, \
		STAT_TABLE_EXTRACT_SYMBOL(id, 5));

void DataStore::StatSetUpHandler::operator()(StatTable &stat) {
	StatTable::ParamId parentId;

	parentId = STAT_TABLE_ROOT;
	stat.resolveGroup(parentId, STAT_TABLE_PERF, "performance");

	parentId = STAT_TABLE_PERF;
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_NUM_BACKGROUND);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_BACKGROUND_MIN_RATE);
	stat.resolveGroup(parentId, STAT_TABLE_PERF_DS_EXP, "expirationDetail");

	parentId = STAT_TABLE_PERF_DS_EXP;
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_DS_EXP_ERASABLE_EXPIRED_TIME);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_DS_EXP_LATEST_EXPIRATION_CHECK_TIME);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_DS_EXP_AUTO_EXPIRE);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_DS_EXP_ESTIMATED_ERASABLE_EXPIRED_TIME);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_DS_EXP_ESTIMATED_BATCH_FREE);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_DS_EXP_LAST_BATCH_FREE);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_DS_EXP_BATCH_SCAN_TOTAL_NUM);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_DS_EXP_BATCH_SCAN_TOTAL_TIME);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_DS_EXP_BATCH_SCAN_NUM);
}

bool DataStore::StatUpdator::operator()(StatTable &stat) {
	if (!stat.getDisplayOption(STAT_TABLE_DISPLAY_SELECT_PERF)) {
		return true;
	}

	uint64_t numBGTask = 0;
	for (PartitionId pId = 0; pId < dataStore_->pgConfig_.getPartitionCount(); pId++) {
		numBGTask += dataStore_->getBGTaskCount(pId);
	}

	stat.set(STAT_TABLE_PERF_TXN_NUM_BACKGROUND, numBGTask);
	if (stat.getDisplayOption(STAT_TABLE_DISPLAY_WEB_ONLY) &&
		stat.getDisplayOption(STAT_TABLE_DISPLAY_OPTIONAL_TXN)) {
		stat.set(STAT_TABLE_PERF_TXN_BACKGROUND_MIN_RATE, 
			dataStore_->getConfig().getBackgroundMinRate());
	}
	stat.set(STAT_TABLE_PERF_DS_EXP_AUTO_EXPIRE, dataStore_->getConfig().isAutoExpire());
	{
		util::DateTime dateTime(dataStore_->getConfig().getErasableExpiredTime());
		util::NormalOStringStream strstrm;
		dateTime.format(strstrm, false);

		stat.set(STAT_TABLE_PERF_DS_EXP_ERASABLE_EXPIRED_TIME, strstrm.str());
	}
	{
		uint64_t lastExpiredNum = 0, simulateExpiredNum = 0, 
			scanBatchTotalTime = 0, scanBatchTotalNum = 0;
		int64_t diffExpiredNum = 0;
		Timestamp maxExpiredTime = 0;
		Timestamp simulateExpiredTime = 0;
		bool isMatch = true;
		for (PartitionId pId = 0; pId < dataStore_->pgConfig_.getPartitionCount(); pId++) {
			ExpirationStat curStat = dataStore_->getExpirationStat(pId);
			if (pId == 0) {
				simulateExpiredTime = curStat.preSimulateTime_;
			} else if (curStat.preSimulateTime_ != simulateExpiredTime)  {
				isMatch = false;
			}
			Timestamp expiredTime = curStat.expiredTime_;
			if (expiredTime > maxExpiredTime) {
				maxExpiredTime = expiredTime;
			}
			simulateExpiredNum += curStat.preSimulateCount_;
			lastExpiredNum += curStat.preExpiredNum_;
			diffExpiredNum += curStat.diffExpiredNum_;
			scanBatchTotalTime += curStat.totalScanTime_;
			scanBatchTotalNum += curStat.totalScanNum_;
		}
		util::NormalOStringStream strstrm;
		{
			util::DateTime dateTime(maxExpiredTime);
			dateTime.format(strstrm, false);
			stat.set(STAT_TABLE_PERF_DS_EXP_LATEST_EXPIRATION_CHECK_TIME, strstrm.str());
		}
		if (stat.getDisplayOption(STAT_TABLE_DISPLAY_WEB_ONLY) &&
			stat.getDisplayOption(STAT_TABLE_DISPLAY_OPTIONAL_TXN)) {
			strstrm.str("");
			if (!isMatch) {
				strstrm << "Under measurement";
				stat.set(STAT_TABLE_PERF_DS_EXP_ESTIMATED_ERASABLE_EXPIRED_TIME, strstrm.str());
				stat.set(STAT_TABLE_PERF_DS_EXP_ESTIMATED_BATCH_FREE, 0);
			} else {
				util::DateTime dateTime(simulateExpiredTime);
				dateTime.format(strstrm, false);
				stat.set(STAT_TABLE_PERF_DS_EXP_ESTIMATED_ERASABLE_EXPIRED_TIME, strstrm.str());
				uint64_t simulateChunkByteSize = simulateExpiredNum * dataStore_->getObjectManager()->getChunkSize();
				stat.set(STAT_TABLE_PERF_DS_EXP_ESTIMATED_BATCH_FREE, simulateChunkByteSize);
			}
			uint64_t lastChunkByteSize = lastExpiredNum * dataStore_->getObjectManager()->getChunkSize();
			stat.set(STAT_TABLE_PERF_DS_EXP_LAST_BATCH_FREE, lastChunkByteSize);
			stat.set(STAT_TABLE_PERF_DS_EXP_BATCH_SCAN_TOTAL_NUM, scanBatchTotalNum);
			stat.set(STAT_TABLE_PERF_DS_EXP_BATCH_SCAN_TOTAL_TIME, scanBatchTotalTime);
			stat.set(STAT_TABLE_PERF_DS_EXP_BATCH_SCAN_NUM, dataStore_->getConfig().getBatchScanNum());
		}
	}
	if (stat.getDisplayOption(STAT_TABLE_DISPLAY_WEB_ONLY)) {
	}

	return true;
}

DataStore::Config::Config(ConfigTable &configTable) :
		isAutoExpire_(true),
		erasableExpiredTime_(0),
		simulateErasableExpiredTime_(0),
		batchScanNum_(2000),
		rowArrayRateExponent_(4),
		isRowArraySizeControlMode_(true),
		backgroundMinRate_(10), backgroundWaitWeight_(9)
		, checkErasableExpiredInterval_(60*60*24),
		concurrencyNum_(1)
		, storeMemoryAgingSwapRate_(0)
		{
	setUpConfigHandler(configTable);
	setBackgroundMinRate(configTable.get<double>(CONFIG_TABLE_DS_BACKGROUND_MIN_RATE));
	setErasableExpiredTime(configTable.get<const char8_t *>(CONFIG_TABLE_DS_ERASABLE_EXPIRED_TIME));
	setAutoExpire(configTable.get<bool>(CONFIG_TABLE_DS_AUTO_EXPIRE));
	setSimulateErasableExpiredTime(configTable.get<const char8_t *>(CONFIG_TABLE_DS_ESTIMATED_ERASABLE_EXPIRED_TIME));
	setBatchScanNum(configTable.get<int32_t>(CONFIG_TABLE_DS_BATCH_SCAN_NUM));
	setRowArrayRateExponent(configTable.get<int32_t>(CONFIG_TABLE_DS_ROW_ARRAY_RATE_EXPONENT));
	setRowArraySizeControlMode(configTable.get<bool>(CONFIG_TABLE_DS_ROW_ARRAY_SIZE_CONTROL_MODE));
	setCheckErasableExpiredInterval(
		configTable.get<int32_t>(CONFIG_TABLE_DS_PARTITION_BATCH_FREE_CHECK_INTERVAL),
		configTable.get<int32_t>(CONFIG_TABLE_DS_CONCURRENCY));
	setStoreMemoryAgingSwapRate(configTable.get<double>(CONFIG_TABLE_DS_STORE_MEMORY_AGING_SWAP_RATE));
}

void DataStore::Config::setUpConfigHandler(ConfigTable &configTable) {
	configTable.setParamHandler(CONFIG_TABLE_DS_BACKGROUND_MIN_RATE, *this);
	configTable.setParamHandler(CONFIG_TABLE_DS_ERASABLE_EXPIRED_TIME, *this);
	configTable.setParamHandler(CONFIG_TABLE_DS_AUTO_EXPIRE, *this);
	configTable.setParamHandler(CONFIG_TABLE_DS_ESTIMATED_ERASABLE_EXPIRED_TIME, *this);
	configTable.setParamHandler(CONFIG_TABLE_DS_BATCH_SCAN_NUM, *this);
	configTable.setParamHandler(CONFIG_TABLE_DS_ROW_ARRAY_RATE_EXPONENT, *this);
	configTable.setParamHandler(CONFIG_TABLE_DS_ROW_ARRAY_SIZE_CONTROL_MODE, *this);
	configTable.setParamHandler(CONFIG_TABLE_DS_PARTITION_BATCH_FREE_CHECK_INTERVAL, *this);
	configTable.setParamHandler(CONFIG_TABLE_DS_STORE_MEMORY_AGING_SWAP_RATE, *this);
}

void DataStore::Config::setBackgroundMinRate(double rate) {
	backgroundMinRate_ = static_cast<int64_t>(rate * 100);
	backgroundWaitWeight_ = 100.0 / backgroundMinRate_ - 1;
}

double DataStore::Config::getBackgroundMinRate() const {
	return static_cast<double>(backgroundMinRate_) / 100;
}

void DataStore::Config::operator()(
		ConfigTable::ParamId id, const ParamValue &value) {
	switch (id) {
	case CONFIG_TABLE_DS_BACKGROUND_MIN_RATE:
		setBackgroundMinRate(value.get<double>());
		break;
	case CONFIG_TABLE_DS_AUTO_EXPIRE:
		setAutoExpire(value.get<bool>());
		break;
	case CONFIG_TABLE_DS_ERASABLE_EXPIRED_TIME:
		setErasableExpiredTime(value.get<const char8_t *>());
		break;
	case CONFIG_TABLE_DS_ESTIMATED_ERASABLE_EXPIRED_TIME:
		setSimulateErasableExpiredTime(value.get<const char8_t *>());
		break;
	case CONFIG_TABLE_DS_BATCH_SCAN_NUM:
		setBatchScanNum(value.get<int32_t>());
		break;
	case CONFIG_TABLE_DS_ROW_ARRAY_SIZE_CONTROL_MODE:
		setRowArraySizeControlMode(value.get<bool>());
		break;
	case CONFIG_TABLE_DS_ROW_ARRAY_RATE_EXPONENT:
		setRowArrayRateExponent(value.get<int32_t>());
		break;
	case CONFIG_TABLE_DS_PARTITION_BATCH_FREE_CHECK_INTERVAL:
		setCheckErasableExpiredInterval(value.get<int32_t>());
		break;
	case CONFIG_TABLE_DS_STORE_MEMORY_AGING_SWAP_RATE:
		setStoreMemoryAgingSwapRate(value.get<double>());
		break;
	}
}
