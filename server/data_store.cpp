/*
	Copyright (c) 2012 TOSHIBA CORPORATION.

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

#include "cluster_service.h"


#define TEST_PRINT(s)
#define TEST_PRINT1(s, d)


const OId DataStore::PARTITION_HEADER_OID = ObjectManager::getOId(
	ALLOCATE_META_CHUNK, INITIAL_CHUNK_ID, FIRST_OBJECT_OFFSET);



const uint32_t DataStoreValueLimitConfig::LIMIT_SMALL_SIZE_LIST[6] = {
	15 * 1024, 31 * 1024, 63 * 1024, 127 * 1024, 128 * 1024, 128 * 1024};
const uint32_t DataStoreValueLimitConfig::LIMIT_BIG_SIZE_LIST[6] = {
	31 * 1024 * 1024, 127 * 1024 * 1024, 511 * 1024 * 1024, 1024 * 1024 * 1024,
	1024 * 1024 * 1024, 1024 * 1024 * 1024};
const uint32_t DataStoreValueLimitConfig::LIMIT_ARRAY_NUM_LIST[6] = {
	2000, 4000, 8000, 16000, 32000, 65000};
const uint32_t DataStoreValueLimitConfig::LIMIT_COLUMN_NUM_LIST[6] = {
	512, 1024, 1024, 1024, 1024, 1024};
const uint32_t DataStoreValueLimitConfig::LIMIT_CONTAINER_NAME_SIZE_LIST[6] = {
	8 * 1024, 16 * 1024, 32 * 1024, 64 * 1024, 128 * 1024, 128 * 1024};

DataStoreValueLimitConfig::DataStoreValueLimitConfig(
	const ConfigTable &configTable) {
	int32_t chunkExpSize = util::nextPowerBitsOf2(
		configTable.getUInt32(CONFIG_TABLE_DS_STORE_BLOCK_SIZE));
	int32_t nth = chunkExpSize - DS_CHUNK_EXP_SIZE_MIN;

	limitSmallSize_ = LIMIT_SMALL_SIZE_LIST[nth];
	limitBigSize_ = LIMIT_BIG_SIZE_LIST[nth];
	limitArrayNumSize_ = LIMIT_ARRAY_NUM_LIST[nth];
	limitColumnNumSize_ = LIMIT_COLUMN_NUM_LIST[nth];
	limitContainerNameSize_ = LIMIT_CONTAINER_NAME_SIZE_LIST[nth];
}

ExtendedContainerName::ExtendedContainerName(util::StackAllocator &alloc,
	const char *extContainerName, size_t extContainerNameLen)
	: alloc_(alloc),
	  databaseId_(UNDEF_DBID),
	  databaseName_(NULL),
	  containerName_(NULL) {
	fullPathName_ = ALLOC_NEW(alloc) char[extContainerNameLen + 1];
	memcpy(fullPathName_, extContainerName, extContainerNameLen);
	fullPathName_[extContainerNameLen] = '\0';
}

ExtendedContainerName::ExtendedContainerName(util::StackAllocator &alloc,
	DatabaseId databaseId, const char *databaseName, const char *containerName)
	: alloc_(alloc) {
	size_t len = strlen(databaseName) + 1;
	databaseName_ = ALLOC_NEW(alloc) char[len];
	memcpy(databaseName_, databaseName, len);
	len = strlen(containerName) + 1;
	containerName_ = ALLOC_NEW(alloc) char[len];
	memcpy(containerName_, containerName, len);
	databaseId_ = databaseId;

	createFullPathName(databaseId, databaseName, containerName);
}



/*!
	@brief Create ResultSet
*/

ResultSet *DataStore::createResultSet(TransactionContext &txn,
	ContainerId containerId, SchemaVersionId schemaVersionId, int64_t emNow) {
	PartitionGroupId pgId = calcPartitionGroupId(txn.getPartitionId());
	ResultSetId rsId = ++resultSetIdList_[pgId];
	int32_t rsTimeout = txn.getTransationTimeoutInterval();
	const int64_t timeout = emNow + static_cast<int64_t>(rsTimeout) * 1000;
	ResultSet &rs = resultSetMap_[pgId]->create(rsId, timeout);
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
	rs.getRSAllocator()->setTotalSizeLimit(resultSetMemoryLimit_);
	rs.setMemoryLimit(resultSetMemoryLimit_);
	rs.setContainerId(containerId);
	rs.setSchemaVersionId(schemaVersionId);
	rs.setId(rsId);
	rs.setStartLsn(0);
	rs.setTimeoutTime(rsTimeout);
	rs.setPartitionId(txn.getPartitionId());
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
	}
	if (rs && (rs->getResultType() != PARTIAL_RESULT_ROWSET)) {
		closeResultSetInternal(pgId, *rs);
	}
}

void DataStore::closeResultSetInternal(PartitionGroupId pgId, ResultSet &rs) {
	ResultSetId removeRSId = rs.getId();
	util::StackAllocator *allocator = rs.getRSAllocator();
	rs.clear();				  
	rs.setRSAllocator(NULL);  

	util::StackAllocator::Tool::forceReset(*allocator);
	allocator->setFreeSizeLimit(allocator->base().getElementSize());
	allocator->trim();

	delete resultSetAllocator_[pgId];
	resultSetAllocator_[pgId] = allocator;

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

DataStore::DataStore(const ConfigTable &configTable, ChunkManager *chunkManager)
	: pgConfig_(configTable),
	  dsValueLimitConfig_(configTable),
	  allocateStrategy_(AllocateStrategy(ALLOCATE_META_CHUNK)),
	  resultSetMemoryLimit_(ConfigTable::megaBytesToBytes(
		  configTable.getUInt32(CONFIG_TABLE_TXN_TOTAL_MEMORY_LIMIT))),
	  resultSetPool_(
		  util::AllocatorInfo(ALLOCATOR_GROUP_TXN_RESULT, "resultSetPool"),
		  1 << RESULTSET_POOL_BLOCK_SIZE_BITS),
	  resultSetAllocator_(
		  UTIL_NEW util::StackAllocator * [pgConfig_.getPartitionGroupCount()]),
	  resultSetMapManager_(NULL),
	  resultSetMap_(NULL),
	  containerIdTable_(NULL) {
	resultSetPool_.setTotalElementLimit(
		ConfigTable::megaBytesToBytes(
			configTable.getUInt32(CONFIG_TABLE_DS_RESULT_SET_MEMORY_LIMIT)) /
		(1 << RESULTSET_POOL_BLOCK_SIZE_BITS));
	resultSetPool_.setFreeElementLimit(0);

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
	}
	catch (std::exception &e) {
		for (uint32_t i = 0; i < pgConfig_.getPartitionGroupCount(); i++) {
			resultSetMapManager_[i]->remove(resultSetMap_[i]);
			delete resultSetMapManager_[i];
			delete resultSetAllocator_[i];
		}
		delete[] resultSetMapManager_;
		delete[] resultSetMap_;
		delete[] resultSetAllocator_;
		delete objectManager_;

		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

DataStore::~DataStore() {
	for (uint32_t i = 0; i < pgConfig_.getPartitionCount(); ++i) {
		objectManager_->validateRefCounter(i);
	}

	for (uint32_t i = 0; i < pgConfig_.getPartitionGroupCount(); ++i) {
		resultSetMapManager_[i]->remove(resultSetMap_[i]);
		delete resultSetMapManager_[i];
		delete resultSetAllocator_[i];
	}
	delete[] resultSetMapManager_;
	delete[] resultSetMap_;
	delete[] resultSetAllocator_;
	delete objectManager_;
	delete containerIdTable_;
}

/*!
	@brief Creates or Updates Container
*/
BaseContainer *DataStore::putContainer(TransactionContext &txn, PartitionId pId,
	ExtendedContainerName &extContainerName, uint8_t containerType,
	uint32_t schemaSize, const uint8_t *containerSchema, bool isEnable,
	PutStatus &status) {
	BaseContainer *container = NULL;
	try {
		util::ArrayByteInStream in = util::ArrayByteInStream(
			util::ArrayInStream(containerSchema, schemaSize));
		MessageSchema *messageSchema = NULL;
		switch (containerType) {
		case COLLECTION_CONTAINER:
			messageSchema = ALLOC_NEW(txn.getDefaultAllocator())
				MessageCollectionSchema(txn.getDefaultAllocator(),
					getValueLimitConfig(), extContainerName.getContainerName(),
					in);
			break;
		case TIME_SERIES_CONTAINER:
			messageSchema = ALLOC_NEW(txn.getDefaultAllocator())
				MessageTimeSeriesSchema(txn.getDefaultAllocator(),
					getValueLimitConfig(), extContainerName.getContainerName(),
					in);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_TYPE_INVALID, "");
			break;
		}

		container = getContainer(txn, pId, extContainerName, containerType);
		if (container == NULL) {
			container = createContainer(
				txn, pId, extContainerName, containerType, messageSchema);
			status = CREATE;
			GS_TRACE_INFO(DATA_STORE, GS_TRACE_DS_DS_CREATE_CONTAINER,
				"name = " << extContainerName.getContainerName()
						  << " Id = " << container->getContainerId());
		}
		else {
			util::XArray<uint32_t> copyColumnMap(txn.getDefaultAllocator());
			bool isCompletelySameSchema;
			container->makeCopyColumnMap(
				txn, messageSchema, copyColumnMap, isCompletelySameSchema);
			if (!isCompletelySameSchema) {
				if (!isEnable) {  
					GS_THROW_USER_ERROR(
						GS_ERROR_DS_DS_CHANGE_SCHEMA_DISABLE, "");
				}
				if (container->isInvalid()) {  
					GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
						"can not change schema. container's status is "
						"invalid.");
				}
				GS_TRACE_INFO(DATA_STORE, GS_TRACE_DS_DS_UPDATE_CONTAINER,
					"Start name = " << extContainerName.getContainerName()
									<< " Id = " << container->getContainerId());
				changeContainerSchema(
					txn, pId, container, messageSchema, copyColumnMap);
				GS_TRACE_INFO(
					DATA_STORE, GS_TRACE_DS_DS_UPDATE_CONTAINER, "End");
				status = UPDATE;
			}
			else {
				status = NOT_EXECUTED;
			}
		}
		return container;
	}
	catch (std::exception &e) {
		if (container != NULL) {
			ALLOC_DELETE(txn.getDefaultAllocator(), container);
		}
		handleUpdateError(e, GS_ERROR_DS_DS_CREATE_COLLECTION_FAILED);
		return NULL;
	}
}

/*!
	@brief Drop Container
*/
void DataStore::dropContainer(TransactionContext &txn, PartitionId pId,
	ExtendedContainerName &extContainerName, uint8_t containerType) {
	try {
		if (!objectManager_->existPartition(pId)) {
			return;
		}
		ContainerAutoPtr containerAutoPtr(
			txn, this, pId, extContainerName, containerType);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		if (container == NULL) {
			return;
		}

		ContainerId containerId = container->getContainerId();
		containerIdTable_->remove(pId, containerId);

		DataStorePartitionHeaderObject partitionHeadearObject(
			txn.getPartitionId(), *getObjectManager(), PARTITION_HEADER_OID);
		BtreeMap containerMap(txn, *getObjectManager(),
			partitionHeadearObject.getMetaMapOId(), allocateStrategy_, NULL);

		uint32_t containerNameSize =
			static_cast<uint32_t>(strlen(extContainerName.c_str()));
		util::XArray<char8_t> containerUpperName(txn.getDefaultAllocator());
		containerUpperName.resize(containerNameSize + 1);
		ValueProcessor::convertUpperCase(extContainerName.c_str(),
			containerNameSize + 1, containerUpperName.data());
		StringCursor keyStringCursor(
			txn, reinterpret_cast<const char *>(containerUpperName.data()));

		OId oId = UNDEF_OID;
		containerMap.search(
			txn, keyStringCursor.str(), keyStringCursor.stringLength(), oId);
		if (oId == UNDEF_OID) {
			return;
		}

		int32_t status = containerMap.remove(txn, keyStringCursor.data(), oId);
		if ((status & BtreeMap::ROOT_UPDATE) != 0) {
			partitionHeadearObject.setMetaMapOId(containerMap.getBaseOId());
		}


		if (!container->isInvalid()) {  
			container->finalize(txn);
		}
		ALLOC_DELETE(txn.getDefaultAllocator(), container);

		GS_TRACE_INFO(DATA_STORE, GS_TRACE_DS_DS_DROP_CONTAINER,
			"name = " << extContainerName.getContainerName()
					  << " Id = " << containerId);
	}
	catch (std::exception &e) {
		handleUpdateError(e, GS_ERROR_DS_DS_DROP_COLLECTION_FAILED);
	}
}

/*!
	@brief Get Container by name
*/
BaseContainer *DataStore::getContainer(TransactionContext &txn, PartitionId pId,
	ExtendedContainerName &extContainerName, uint8_t containerType) {
	try {
		if (!objectManager_->existPartition(pId)) {
			return NULL;
		}
		DataStorePartitionHeaderObject partitionHeadearObject(
			txn.getPartitionId(), *getObjectManager(), PARTITION_HEADER_OID);
		BtreeMap containerMap(txn, *getObjectManager(),
			partitionHeadearObject.getMetaMapOId(), allocateStrategy_, NULL);

		uint32_t containerNameSize =
			static_cast<uint32_t>(strlen(extContainerName.c_str()));
		util::XArray<char8_t> containerUpperName(txn.getDefaultAllocator());
		containerUpperName.resize(containerNameSize + 1);
		ValueProcessor::convertUpperCase(extContainerName.c_str(),
			containerNameSize + 1, containerUpperName.data());

		OId oId = UNDEF_OID;
		containerMap.search(
			txn, containerUpperName.data(), containerNameSize, oId);
		if (oId == UNDEF_OID) {
			return NULL;
		}
		BaseObject baseContainerImageObject(
			txn.getPartitionId(), *getObjectManager(), oId);
		BaseContainer::BaseContainerImage *baseContainerImage =
			baseContainerImageObject
				.getBaseAddr<BaseContainer::BaseContainerImage *>();
		ContainerType realContainerType = baseContainerImage->containerType_;
		if (containerType != ANY_CONTAINER &&
			realContainerType != containerType) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_TYPE_INVALID, "");
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
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_DS_CONTAINER_TYPE_INVALID, "");
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
	@brief Get Container by ContainerId
*/
BaseContainer *DataStore::getContainer(TransactionContext &txn, PartitionId pId,
	ContainerId containerId, uint8_t containerType) {
	try {
		if (!objectManager_->existPartition(pId)) {
			return NULL;
		}
		OId oId = containerIdTable_->get(pId, containerId);
		if (UNDEF_OID == oId) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_ID_INVALID, "");
		}
		ContainerType realContainerType = getContainerType(txn, containerId);
		if (containerType != ANY_CONTAINER &&
			realContainerType != containerType) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_TYPE_INVALID, "");
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
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_DS_CONTAINER_TYPE_INVALID, "");
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
	@brief Get Collection by ContainerId
*/
Collection *DataStore::getCollection(
	TransactionContext &txn, PartitionId pId, ContainerId containerId) {
	BaseContainer *container =
		getContainer(txn, pId, containerId, COLLECTION_CONTAINER);
	Collection *collection = reinterpret_cast<Collection *>(container);
	return collection;
}

/*!
	@brief Get TimeSeries by ContainerId
*/
TimeSeries *DataStore::getTimeSeries(
	TransactionContext &txn, PartitionId pId, ContainerId containerId) {
	BaseContainer *container =
		getContainer(txn, pId, containerId, TIME_SERIES_CONTAINER);
	TimeSeries *timeSeries = reinterpret_cast<TimeSeries *>(container);
	return timeSeries;
}

/*!
	@brief Get Collection by ObjectId at recovery phase
*/
BaseContainer *DataStore::getContainerForRestore(
	TransactionContext &txn, PartitionId, OId oId, uint8_t containerType) {
	try {
		if (UNDEF_OID == oId) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_ID_INVALID, "");
		}
		if (containerType != COLLECTION_CONTAINER &&
			containerType != TIME_SERIES_CONTAINER) {
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
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_DS_CONTAINER_TYPE_INVALID, "");
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

		int32_t status =
			schemaMap.get()->remove(txn, &schemaHashKey, schemaOId);
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

		int32_t status =
			triggerMap.get()->remove(txn, &schemaHashKey, triggerOId);
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
void DataStore::setLastChunkKey(PartitionId pId, ChunkKey chunkKey) {
	DataStorePartitionHeaderObject partitionHeadearObject(
		pId, *objectManager_, PARTITION_HEADER_OID);
	partitionHeadearObject.setChunkKey(chunkKey);
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

	get()->maxContainerId_ = 0;
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
}

BaseContainer *DataStore::createContainer(TransactionContext &txn,
	PartitionId pId, ExtendedContainerName &extContainerName,
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
	BtreeMap containerMap(txn, *getObjectManager(),
		partitionHeadearObject.getMetaMapOId(), allocateStrategy_, NULL);

	int64_t schemaHashKey = BaseContainer::calcSchemaHashKey(messageSchema);
	OId schemaOId = getColumnSchemaId(txn, pId, messageSchema, schemaHashKey);
	insertColumnSchema(txn, pId, messageSchema, schemaHashKey, schemaOId);

	BaseContainer *container;
	switch (containerType) {
	case COLLECTION_CONTAINER: {
		container = ALLOC_NEW(txn.getDefaultAllocator()) Collection(txn, this);
	} break;
	case TIME_SERIES_CONTAINER: {
		container = ALLOC_NEW(txn.getDefaultAllocator()) TimeSeries(txn, this);
	} break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_DS_CONTAINER_TYPE_INVALID, "");
		break;
	}

	container->initialize(txn);
	container->set(txn, extContainerName.c_str(),
		partitionHeadearObject.getMaxContainerId(), schemaOId, messageSchema);
	ContainerAttribute attribute = container->getAttribute();
	containerIdTable_->set(pId, partitionHeadearObject.getMaxContainerId(),
		container->getBaseOId(), extContainerName.getDatabaseId(), attribute);
	TEST_PRINT("create container \n");
	TEST_PRINT1("pId %d\n", pId);
	TEST_PRINT1("databaseName %s\n", extContainerName.getDatabaseName());
	TEST_PRINT1("databaseVersionId %d\n", extContainerName.getDatabaseId());
	TEST_PRINT1("attribute %d\n", (int32_t)attribute);

	uint32_t size = static_cast<uint32_t>(strlen(extContainerName.c_str()));
	util::XArray<char8_t> containerUpperName(txn.getDefaultAllocator());
	containerUpperName.resize(size + 1);
	ValueProcessor::convertUpperCase(
		extContainerName.c_str(), size + 1, containerUpperName.data());
	StringCursor keyStringCursor(
		txn, reinterpret_cast<const char *>(containerUpperName.data()));

	int32_t status = containerMap.insert(
		txn, keyStringCursor.data(), container->getBaseOId());
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		partitionHeadearObject.setMetaMapOId(containerMap.getBaseOId());
	}

	return container;
}

void DataStore::changeContainerSchema(TransactionContext &txn, PartitionId pId,
	BaseContainer *&container, MessageSchema *messageSchema,
	util::XArray<uint32_t> &copyColumnMap) {
	ExtendedContainerName *extContainerName;
	container->getExtendedContainerName(txn, extContainerName);

	util::XArray<const util::String *> oldColumnNameList(
		txn.getDefaultAllocator());
	{
		ColumnInfo *oldSchema = container->getColumnInfoList();
		const uint32_t columnCount = container->getColumnNum();
		getColumnNameList(txn, oldSchema, columnCount, oldColumnNameList);
	}

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
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_DS_CONTAINER_TYPE_INVALID, "");
		break;
	}
	newContainer->initialize(txn);
	newContainer->set(txn, extContainerName->c_str(),
		container->getContainerId(), schemaOId, messageSchema);
	newContainer->setVersionId(
		container->getVersionId() + 1);  

	try {
		container->changeSchema(txn, *newContainer, copyColumnMap);
	}
	catch (UserException &e) {  
		newContainer->finalize(txn);
		GS_RETHROW_USER_ERROR(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "Failed to change schema"));
	}

	DataStorePartitionHeaderObject partitionHeadearObject(
		txn.getPartitionId(), *getObjectManager(), PARTITION_HEADER_OID);
	BtreeMap containerMap(txn, *getObjectManager(),
		partitionHeadearObject.getMetaMapOId(), allocateStrategy_, NULL);

	uint32_t size = static_cast<uint32_t>(strlen(extContainerName->c_str()));
	util::XArray<char8_t> containerUpperName(txn.getDefaultAllocator());
	containerUpperName.resize(size + 1);
	ValueProcessor::convertUpperCase(
		extContainerName->c_str(), size + 1, containerUpperName.data());
	StringCursor keyStringCursor(
		txn, reinterpret_cast<const char *>(containerUpperName.data()));

	OId oId = UNDEF_OID;
	containerMap.search(
		txn, keyStringCursor.str(), keyStringCursor.stringLength(), oId);
	if (oId == UNDEF_OID) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_CONTAINER_NOT_FOUND, "");
	}

	int32_t status = containerMap.update(
		txn, keyStringCursor.data(), oId, newContainer->getBaseOId());
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		partitionHeadearObject.setMetaMapOId(containerMap.getBaseOId());
	}

	ContainerId containerId = container->getContainerId();

	containerIdTable_->remove(pId, containerId);
	ContainerAttribute attribute = container->getAttribute();
	containerIdTable_->set(pId, containerId, newContainer->getBaseOId(),
		extContainerName->getDatabaseId(), attribute);
	TEST_PRINT("changeSchema \n");
	TEST_PRINT1("pId %d\n", pId);
	TEST_PRINT1("databaseName %s\n", extContainerName.getDatabaseName());
	TEST_PRINT1("databaseVersionId %d\n", extContainerName.getDatabaseId());
	TEST_PRINT1("attribute %d\n", (int32_t)attribute);

	container->finalize(txn);
	container = newContainer;

	util::XArray<const util::String *> newColumnNameList(
		txn.getDefaultAllocator());
	{
		ColumnInfo *newSchema = container->getColumnInfoList();
		const uint32_t columnCount = container->getColumnNum();
		getColumnNameList(txn, newSchema, columnCount, newColumnNameList);
	}
	container->updateTrigger(txn, oldColumnNameList, newColumnNameList);
}


OId DataStore::getColumnSchemaId(TransactionContext &txn, PartitionId pId,
	MessageSchema *messageSchema, int64_t schemaHashKey) {
	StackAllocAutoPtr<BtreeMap> schemaMap(
		txn.getDefaultAllocator(), getSchemaMap(txn, pId));
	OId schemaOId = UNDEF_OID;
	util::XArray<OId> schemaList(txn.getDefaultAllocator());
	BtreeMap::SearchContext sc(
		UNDEF_COLUMNID, &schemaHashKey, 0, 0, NULL, MAX_RESULT_SIZE);
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
	BtreeMap::SearchContext sc(
		UNDEF_COLUMNID, &triggerHashKey, 0, 0, NULL, MAX_RESULT_SIZE);
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
		BaseContainer::initializeSchema(txn, *getObjectManager(), messageSchema,
			allocateStrategy_, inputList, bodyAllocateSize);
		uint32_t allocateSize = ShareValueList::getHeaderSize(
									static_cast<int32_t>(inputList.size())) +
								bodyAllocateSize;

		ShareValueList commonContainerSchema(txn, *getObjectManager());
		commonContainerSchema.initialize(txn, allocateSize, allocateStrategy_);
		commonContainerSchema.set(schemaHashKey, inputList);
		schemaOId = commonContainerSchema.getBaseOId();

		int32_t status =
			schemaMap.get()->insert(txn, &schemaHashKey, schemaOId);
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
		triggerSchema.initialize(txn, allocateSize, allocateStrategy_);
		triggerSchema.set(triggerHashKey, inputList);
		triggerOId = triggerSchema.getBaseOId();

		int32_t status =
			triggerMap.get()->insert(txn, &triggerHashKey, triggerOId);
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
	BtreeMap *schemaMap = NULL;

	DataStorePartitionHeaderObject partitionHeadearObject(
		txn.getPartitionId(), *getObjectManager(), PARTITION_HEADER_OID);
	schemaMap =
		ALLOC_NEW(txn.getDefaultAllocator()) BtreeMap(txn, *getObjectManager(),
			partitionHeadearObject.getTriggerMapOId(), allocateStrategy_);
	return schemaMap;
}

/*!
	@brief Get PartitionGroupId from PartitionId
*/
UTIL_FORCEINLINE PartitionGroupId DataStore::calcPartitionGroupId(
	PartitionId pId) {
	return pgConfig_.getPartitionGroupId(pId);
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

void DataStore::getColumnNameList(TransactionContext &txn, ColumnInfo *schema,
	uint32_t columnCount, util::XArray<const util::String *> &columnNameList) {
	for (uint32_t i = 0; i < columnCount; i++) {
		util::String *columnName = ALLOC_NEW(txn.getDefaultAllocator())
			util::String(txn.getDefaultAllocator());
		columnName->append(reinterpret_cast<const char *>(
			schema[i].getColumnName(txn, *getObjectManager())));
		columnNameList.push_back(columnName);
	}
}

/*!
	@brief Prepare for redo log
*/
void DataStore::restartPartition(
	TransactionContext &txn, ClusterService *clusterService) {
	restoreContainerIdTable(txn, clusterService);
	setRestored(txn.getPartitionId());

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
				txn, this, pId, oId, baseContainerImage->containerType_);
			BaseContainer *container = containerAutoPtr.getBaseContainer();

			ExtendedContainerName *extContainerName;
			container->getExtendedContainerName(txn, extContainerName);
			const DatabaseId databaseVersionId =
				extContainerName->getDatabaseId();

			ContainerAttribute attribute = container->getAttribute();
			containerIdTable_->set(
				pId, containerId, oId, databaseVersionId, attribute);
			TEST_PRINT("changeSchema \n");
			TEST_PRINT1("pId %d\n", pId);
			TEST_PRINT1("databaseName %s\n", container->getDatabaseName(txn));
			TEST_PRINT1("databaseVersionId %d\n", databaseVersionId);
			TEST_PRINT1("attribute %d\n", (int32_t)attribute);

			containerListSize++;
		}
		if (getAllStatus == GS_SUCCESS) {
			break;
		}
	}

	GS_TRACE_INFO(DATA_STORE, GS_TRACE_DS_DS_CONTAINTER_ID_TABLE_STATUS,
		"restore pId," << txn.getPartitionId() << ",containerListSize,"
					   << containerListSize);

	return isGsContainersExist;
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
		}

		setUnrestored(pId);
	}
	catch (std::exception &e) {
		handleUpdateError(e, GS_ERROR_DS_DS_DROP_PARTITION_FAILED);
	}
}

/*!
	@brief Get Container type by ContainerId
*/
ContainerType DataStore::getContainerType(
	TransactionContext &txn, ContainerId containerId) const {
	OId oId = containerIdTable_->get(txn.getPartitionId(), containerId);
	if (oId == UNDEF_OID) {
		return UNDEF_CONTAINER;  
	}
	else {
		BaseObject baseContainerImageObject(
			txn.getPartitionId(), *getObjectManager(), oId);
		BaseContainer::BaseContainerImage *baseContainerImage =
			baseContainerImageObject
				.getBaseAddr<BaseContainer::BaseContainerImage *>();
		return baseContainerImage->containerType_;
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
			std::make_pair<ContainerId, ContainerInfoCache>(
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
		GS_TRACE_WARNING(DATA_STORE, GS_TRACE_DS_DS_CONTAINTER_ID_TABLE_STATUS,
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

			ExtendedContainerName *extContainerName;
			container->getExtendedContainerName(txn, extContainerName);
			const char *containerName = extContainerName->getContainerName();

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

/*!
	@brief Returns names of Container to meet a given condition in the partition
*/
void DataStore::getContainerNameList(TransactionContext &txn, PartitionId pId,
	int64_t start, ResultSize limit, const DatabaseId dbId,
	ContainerCondition &condition, util::XArray<util::String *> &nameList) {
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
		const int64_t currentDatabaseVersionId = dbId;
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

					ExtendedContainerName *extContainerName;
					containerAutoPtr.getBaseContainer()
						->getExtendedContainerName(txn, extContainerName);
					const char *containerName =
						extContainerName->getContainerName();

					TEST_PRINT1("getContainerNameList:containerName %s\n",
						containerName);
					util::String *containerNameString =
						ALLOC_NEW(txn.getDefaultAllocator()) util::String(
							containerName, txn.getDefaultAllocator());
					nameList.push_back(containerNameString);
				}
				count++;
			}
		}
	}
	catch (std::exception &e) {
		handleSearchError(e, GS_ERROR_DS_DS_GET_CONTAINER_LIST_FAILED);
	}
}

DataStore::Latch::Latch(TransactionContext &txn, PartitionId pId,
	DataStore *dataStore, ClusterService *clusterService)
	: pId_(pId),
	  txn_(txn),
	  dataStore_(dataStore),
	  clusterService_(clusterService) {
	dataStore_->getObjectManager()->checkDirtyFlag(
		txn.getPartitionId());  
}

DataStore::Latch::~Latch() {
	try {
		ObjectManager &objectManager = *(dataStore_->getObjectManager());
		if (dataStore_ != NULL && objectManager.existPartition(pId_)) {
			objectManager.checkDirtyFlag(
				txn_.getPartitionId());  
			objectManager.resetRefCounter(pId_);
			objectManager.freeLastLatchPhaseMemory(pId_);
		}
	}
	catch (std::exception &e) {
		GS_TRACE_ERROR(
			DATA_STORE, GS_TRACE_DS_DS_LATCH_STATUS, "UnLatch Failed");
		if (clusterService_ != NULL) {
			EventEngine::VariableSizeAllocator varSizeAlloc(
				util::AllocatorInfo(ALLOCATOR_GROUP_STORE, "latch"));
			Event::Source eventSource(varSizeAlloc);
			clusterService_->setError(eventSource, &e);
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


/*!
	@brief Frees all Objects on the Chunks, older than timestamp of ChunkKey.
*/
bool DataStore::executeBatchFree(PartitionId pId, Timestamp timestamp,
	uint64_t maxScanNum, uint64_t &scanNum) {
	ChunkKey chunkKey = DataStore::convertTimestamp2ChunkKey(timestamp, false);

	uint64_t freeChunkNum;
	bool isTail = objectManager_->batchFree(
		pId, chunkKey, maxScanNum, scanNum, freeChunkNum);
	if (freeChunkNum > 0) {
		setLastChunkKey(pId, chunkKey);
	}
	return isTail;
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
		.add("64KB")
		.add("1MB")
		.setDefault("64KB");

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_DB_PATH, STRING)
		.setDefault("data");
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
		.setDefault(1);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_LOG_WRITE_MODE, INT32)
		.setMin(-1)
		.setDefault(1);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_STORE_WARM_START, BOOL)
		.setExtendedType(ConfigTable::EXTENDED_TYPE_LAX_BOOL)
		.setDefault(true);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_AFFINITY_GROUP_SIZE, INT32)
		.setMin(1)
		.setMax(10000)
		.setDefault(4);

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
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_RECOVERY_LEVEL, INT32)
		.add(0)
		.add(1)
		.setDefault(0);
}
