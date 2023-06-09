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
	@brief Implementation of DataStoreV4
*/
#include "data_store_v4.h"
#include "collection.h"
#include "data_store_common.h"
#include "message_schema.h"
#include "object_manager_v4.h"
#include "query_processor.h"
#include "result_set.h"
#include "time_series.h"
#include "cluster_service.h"
#include "key_data_store.h"

#include "picojson.h"
#include <fstream>


#include "event_engine.h"
#include "transaction_service.h"


#define TEST_PRINT(s)
#define TEST_PRINT1(s, d)



AUDIT_TRACER_DECLARE(AUDIT_NOSQL_READ);
AUDIT_TRACER_DECLARE(AUDIT_NOSQL_WRITE);

std::string DataStoreV4::getClientAddress(const Event *ev) {
	util::NormalOStringStream oss;
	std::string work;
	if (ev->getSenderND().isEmpty()){
		return "";
	}
	oss << ev->getSenderND();
	work = oss.str();
	return work.substr(work.find("address=")+8,work.length()-work.find("address=")-9);
}

#define AUDIT_TRACE_CHECK   0

#define AUDIT_TRACE_INFO_EXECUTE(category,type,containername)  { \
}
#define AUDIT_TRACE_ERROR_EXECUTE()  { \
}


class TransactionContextScope {
public:

	TransactionContextScope(TransactionContext& txn) :
		txn_(txn), containerNameStr_(txn.getDefaultAllocator()) {
		txn_.setContainerNameStr(&containerNameStr_);
	}

	~TransactionContextScope() {
		txn_.setContainerNameStr(NULL);
	}
	
	static const char* getContainerName(TransactionContext& txn, const FullContainerKey& containerKey) {
		if (txn.getContainerNameStr()) {
			containerKey.toString(txn.getDefaultAllocator(), *txn.getContainerNameStr());
			return txn.getContainerNameStr()->c_str();
		}
		return "";
	}

	static const char* getContainerName(TransactionContext& txn, BaseContainer* container) {
		return getContainerName(txn, container->getContainerKey(txn));
	}

	const char* getContainerName() {
		return txn_.getContainerName();
	}

private:
	TransactionContext& txn_;
	util::String containerNameStr_;
};
const uint32_t DataStoreConfig::LIMIT_SMALL_SIZE_LIST[12] = {
	15 * 1024, 31 * 1024, 63 * 1024, 127 * 1024, 128 * 1024, 128 * 1024,
	128 * 1024, 128 * 1024, 128 * 1024, 128 * 1024, 128 * 1024, 128 * 1024 };
const uint32_t DataStoreConfig::LIMIT_BIG_SIZE_LIST[12] = {
	1024 * 1024 * 1024 - 1, 1024 * 1024 * 1024 - 1, 1024 * 1024 * 1024 - 1,
	1024 * 1024 * 1024 - 1, 1024 * 1024 * 1024 - 1, 1024 * 1024 * 1024 - 1,
	1024 * 1024 * 1024 - 1, 1024 * 1024 * 1024 - 1, 1024 * 1024 * 1024 - 1,
	1024 * 1024 * 1024 - 1, 1024 * 1024 * 1024 - 1, 1024 * 1024 * 1024 - 1 };
const uint32_t DataStoreConfig::LIMIT_ARRAY_NUM_LIST[12] = {
	2000, 4000, 8000, 16000, 32000, 65000,
	65000, 65000, 65000, 65000, 65000, 65000 };
const uint32_t DataStoreConfig::LIMIT_COLUMN_NUM_LIST[12] = {
	512, 1024, 1024, 1000 * 8, 1000 * 16, 1000 * 32,
	1000 * 32, 1000 * 32, 1000 * 32, 1000 * 32, 1000 * 32, 1000 * 32 };
const uint32_t DataStoreConfig::LIMIT_INDEX_NUM_LIST[12] = {
	512, 1024, 1024, 1000 * 4, 1000 * 8, 1000 * 16,
	1000 * 16, 1000 * 16, 1000 * 16, 1000 * 16, 1000 * 16, 1000 * 16 };
const uint32_t DataStoreConfig::LIMIT_CONTAINER_NAME_SIZE_LIST[12] = {
	8 * 1024, 16 * 1024, 32 * 1024, 64 * 1024, 128 * 1024, 128 * 1024,
	128 * 1024, 128 * 1024, 128 * 1024, 128 * 1024, 128 * 1024, 128 * 1024 };

DataStoreConfig::DataStoreConfig(ConfigTable& configTable) :
	erasableExpiredTime_(0),
	estimateErasableExpiredTime_(0),
	batchScanNum_(2000),
	rowArrayRateExponent_(4),
	isRowArraySizeControlMode_(true),
	backgroundMinRate_(10), backgroundWaitWeight_(9)
	, checkErasableExpiredInterval_(60 * 60 * 24),
	concurrencyNum_(1)
	, storeMemoryAgingSwapRate_(0)
{
	setUpConfigHandler(configTable);
	setBackgroundMinRate(configTable.get<double>(CONFIG_TABLE_DS_BACKGROUND_MIN_RATE));
	setErasableExpiredTime(configTable.get<const char8_t*>(CONFIG_TABLE_DS_ERASABLE_EXPIRED_TIME));
	setEstimateErasableExpiredTime(configTable.get<const char8_t*>(CONFIG_TABLE_DS_ESTIMATED_ERASABLE_EXPIRED_TIME));
	setBatchScanNum(configTable.get<int32_t>(CONFIG_TABLE_DS_BATCH_SCAN_NUM));
	setRowArrayRateExponent(configTable.get<int32_t>(CONFIG_TABLE_DS_ROW_ARRAY_RATE_EXPONENT));
	setRowArraySizeControlMode(configTable.get<bool>(CONFIG_TABLE_DS_ROW_ARRAY_SIZE_CONTROL_MODE));
	setCheckErasableExpiredInterval(
		configTable.get<int32_t>(CONFIG_TABLE_DS_PARTITION_BATCH_FREE_CHECK_INTERVAL),
		configTable.get<int32_t>(CONFIG_TABLE_DS_CONCURRENCY));
	setExpiredCheckCount(configTable.get<int32_t>(
		CONFIG_TABLE_DS_PARTITION_BATCH_FREE_CHECK_CONTAINER_COUNT));
	setStoreMemoryAgingSwapRate(configTable.get<double>(CONFIG_TABLE_DS_STORE_MEMORY_AGING_SWAP_RATE));

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

void DataStoreConfig::setUpConfigHandler(ConfigTable& configTable) {
	configTable.setParamHandler(CONFIG_TABLE_DS_BACKGROUND_MIN_RATE, *this);
	configTable.setParamHandler(CONFIG_TABLE_DS_ERASABLE_EXPIRED_TIME, *this);
	configTable.setParamHandler(CONFIG_TABLE_DS_ESTIMATED_ERASABLE_EXPIRED_TIME, *this);
	configTable.setParamHandler(CONFIG_TABLE_DS_BATCH_SCAN_NUM, *this);
	configTable.setParamHandler(CONFIG_TABLE_DS_ROW_ARRAY_RATE_EXPONENT, *this);
	configTable.setParamHandler(CONFIG_TABLE_DS_ROW_ARRAY_SIZE_CONTROL_MODE, *this);
	configTable.setParamHandler(CONFIG_TABLE_DS_PARTITION_BATCH_FREE_CHECK_INTERVAL, *this);
	configTable.setParamHandler(CONFIG_TABLE_DS_PARTITION_BATCH_FREE_CHECK_CONTAINER_COUNT, *this);
	configTable.setParamHandler(CONFIG_TABLE_DS_STORE_MEMORY_AGING_SWAP_RATE, *this);
}

void DataStoreConfig::setBackgroundMinRate(double rate) {
	backgroundMinRate_ = static_cast<int64_t>(rate * 100);
	backgroundWaitWeight_ = 100.0 / backgroundMinRate_ - 1;
}

int64_t DataStoreConfig::getBackgroundMinRatePct() const {
	return backgroundMinRate_;
}

double DataStoreConfig::getBackgroundMinRate() const {
	return static_cast<double>(getBackgroundMinRatePct()) / 100;
}

void DataStoreConfig::operator()(
	ConfigTable::ParamId id, const ParamValue& value) {
	switch (id) {
	case CONFIG_TABLE_DS_BACKGROUND_MIN_RATE:
		setBackgroundMinRate(value.get<double>());
		break;
	case CONFIG_TABLE_DS_ERASABLE_EXPIRED_TIME:
		setErasableExpiredTime(value.get<const char8_t*>());
		break;
	case CONFIG_TABLE_DS_ESTIMATED_ERASABLE_EXPIRED_TIME:
		setEstimateErasableExpiredTime(value.get<const char8_t*>());
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
	case CONFIG_TABLE_DS_PARTITION_BATCH_FREE_CHECK_CONTAINER_COUNT:
		setExpiredCheckCount(value.get<int32_t>());
		break;
	case CONFIG_TABLE_DS_STORE_MEMORY_AGING_SWAP_RATE:
		setStoreMemoryAgingSwapRate(value.get<double>());
		break;
	}
}


DataStoreStats::DataStoreStats(const Mapper *mapper) :
		table_(mapper),
		timeTable_(table_) {
}

void DataStoreStats::setUpMapper(Mapper &mapper, uint32_t chunkSize) {
	{
		Mapper sub(mapper.getSource());
		setUpBasicMapper(sub);
		mapper.addSub(sub);
	}

	{
		Mapper sub(mapper.getSource());
		setUpTransactionMapper(sub, chunkSize);
		mapper.addSub(sub);
	}

	{
		Mapper sub(mapper.getSource());
		setUpDetailMapper(sub, chunkSize);
		mapper.addSub(sub);
	}
}

void DataStoreStats::setUpBasicMapper(Mapper &mapper) {
	mapper.addFilter(STAT_TABLE_DISPLAY_SELECT_PERF);

	mapper.bind(
			DS_STAT_EXPIRED_TIME,
			STAT_TABLE_PERF_DS_EXP_LATEST_EXPIRATION_CHECK_TIME)
			.setDateTimeType()
			.setDefaultValue(ParamValue("Under measurement"));
	mapper.bind(DS_STAT_BG_NUM, STAT_TABLE_PERF_TXN_NUM_BACKGROUND);
}

void DataStoreStats::setUpTransactionMapper(
		Mapper &mapper, uint32_t chunkSize) {
	const ConfigTable::ValueUnit timeUnit = ConfigTable::VALUE_UNIT_NONE;

	mapper.addFilter(STAT_TABLE_DISPLAY_SELECT_PERF);
	mapper.addFilter(STAT_TABLE_DISPLAY_WEB_ONLY);
	mapper.addFilter(STAT_TABLE_DISPLAY_OPTIONAL_TXN);

	mapper.bind(
			DS_STAT_ESTIMATE_BATCH_FREE_TIME,
			STAT_TABLE_PERF_DS_EXP_ESTIMATED_ERASABLE_EXPIRED_TIME)
			.setDateTimeType();
	mapper.bind(
			DS_STAT_ESTIMATE_BATCH_FREE_NUM,
			STAT_TABLE_PERF_DS_EXP_ESTIMATED_BATCH_FREE)
			.setConversionUnit(chunkSize, false);

	mapper.bind(
			DS_STAT_BATCH_FREE_NUM,
			STAT_TABLE_PERF_DS_EXP_LAST_BATCH_FREE)
			.setConversionUnit(chunkSize, false);
	mapper.bind(
			DS_STAT_SCAN_NUM,
			STAT_TABLE_PERF_DS_EXP_BATCH_SCAN_TOTAL_NUM);
	mapper.bind(
			DS_STAT_SCAN_TIME,
			STAT_TABLE_PERF_DS_EXP_BATCH_SCAN_TOTAL_TIME)
			.setTimeUnit(timeUnit);
}

void DataStoreStats::setUpDetailMapper(Mapper &mapper, uint32_t chunkSize) {
	mapper.addFilter(STAT_TABLE_DISPLAY_SELECT_PERF);
	mapper.addFilter(STAT_TABLE_DISPLAY_WEB_OR_DETAIL_TRACE);

	const int32_t groupStatCount = CHUNK_GROUP_STAT_COUNT;
	const int32_t withUnit = 1;
	const int32_t groupStatsList[groupStatCount][2] = {
		{ ChunkGroupStats::GROUP_STAT_USE_BUFFER_COUNT, withUnit },
		{ ChunkGroupStats::GROUP_STAT_USE_BLOCK_COUNT, withUnit },
		{ ChunkGroupStats::GROUP_STAT_SWAP_READ_COUNT, 0 },
		{ ChunkGroupStats::GROUP_STAT_SWAP_WRITE_COUNT, 0 }
	};

	for (int32_t c = 0; c < CHUNK_CATEGORY_COUNT; c++) {
		const int32_t destId = getStoreParamId(
				STAT_TABLE_PERF_DS_DETAIL_CATEGORY_START + 1 + c,
				STAT_TABLE_PERF_DS_DETAIL_PARAM_START) + 1;
		for (int32_t  i = 0; i < groupStatCount; i++) {
			const int32_t srcId =
					DS_STAT_CHUNK_STATS_START +
					groupStatCount * c + groupStatsList[i][0];

			const BasicLocalStatMapper::Options &options = mapper.bind(
					static_cast<DataStoreStats::Param>(srcId), destId + i);
			if (groupStatsList[i][1] == withUnit) {
				options.setConversionUnit(chunkSize, false);
			}
		}
	}
}

StatTableParamId DataStoreStats::getStoreParamId(
		int32_t parentId, StatTableParamId id) {
	const int32_t categoryIndex =
		(parentId - STAT_TABLE_PERF_DS_DETAIL_CATEGORY_START - 1);
	const int32_t groupStatCount =
			(STAT_TABLE_PERF_DS_DETAIL_PARAM_END -
			STAT_TABLE_PERF_DS_DETAIL_PARAM_START - 1);
	UTIL_STATIC_ASSERT(groupStatCount == CHUNK_GROUP_STAT_COUNT);

	return static_cast<StatTableParamId>(
			(STAT_TABLE_PERF_DS_DETAIL_ALL_START + 1) +
			groupStatCount * categoryIndex +
			(id - STAT_TABLE_PERF_DS_DETAIL_PARAM_START - 1));
}

void DataStoreStats::setChunkStats(
		ChunkCategoryId categoryId, const ChunkGroupStats::Table &stats) {
	const int32_t groupStatCount = CHUNK_GROUP_STAT_COUNT;
	const int32_t baseId =
			DS_STAT_CHUNK_STATS_START + groupStatCount * categoryId;
	for (int32_t i = 0; i < groupStatCount; i++) {
		table_(static_cast<DataStoreStats::Param>(baseId + i)).assign(
				stats(static_cast<ChunkGroupStats::Param>(i)));
	}
}

void DataStoreStats::getChunkStats(
		ChunkCategoryId categoryId, ChunkGroupStats::Table &stats) const {
	const int32_t groupStatCount = CHUNK_GROUP_STAT_COUNT;
	const int32_t baseId =
			DS_STAT_CHUNK_STATS_START + groupStatCount * categoryId;
	for (int32_t i = 0; i < groupStatCount; i++) {
		stats(static_cast<ChunkGroupStats::Param>(i)).assign(
				table_(static_cast<DataStoreStats::Param>(baseId + i)));
	}
}

void DataStoreStats::setTimestamp(Param param, Timestamp ts) {
	table_(param).set(static_cast<uint64_t>(ts));
}

DataStoreStats::PlainChunkGroupStats::PlainChunkGroupStats() :
		subTable_(NULL) {
}



DataStoreV4::DataStoreV4(
		util::StackAllocator* stAlloc,
		util::FixedSizeAllocator<util::Mutex>* resultSetPool,
		ConfigTable* configTable, TransactionManager* txnMgr,
		ChunkManager* chunkmanager, LogManager<MutexLocker>* logmanager,
		KeyDataStore* keyStore, const StatsSet &stats) :
		DataStoreBase(stAlloc, resultSetPool, configTable, txnMgr, chunkmanager, logmanager, keyStore),
		config_(NULL),
		allocateStrategy_(),
		dsStats_(stats.dsStats_),
		objectManager_(NULL),
		clusterService_(NULL),
		keyStore_(keyStore),
		latch_(NULL),
		headerOId_(UNDEF_OID),
		rsManager_(NULL),
		activeBackgroundCount_(0),
		blockSize_(0)
{
	try {
		affinityGroupSize_ =
			configTable->get<int32_t>(CONFIG_TABLE_DS_AFFINITY_GROUP_SIZE);

		objectManager_ = UTIL_NEW ObjectManagerV4(
				*configTable, chunkmanager, stats.objMgrStats_);
		allocateStrategy_.set(META_GROUP_ID, objectManager_);

		uint32_t chunkExpSize = util::nextPowerBitsOf2(static_cast<uint32_t>(
			configTable->getUInt32(CONFIG_TABLE_DS_STORE_BLOCK_SIZE)));
		blockSize_ = 1 << chunkExpSize;

		const uint32_t rsCacheSize =
			configTable->getUInt32(CONFIG_TABLE_DS_RESULT_SET_CACHE_MEMORY);
		rsManager_ = UTIL_NEW ResultSetManager(stAlloc, resultSetPool, 
			objectManager_, rsCacheSize);


		clearTimeStats();
	}
	catch (std::exception& e) {
		delete rsManager_;
		delete objectManager_;

		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

DataStoreV4::~DataStoreV4() {
	delete rsManager_;

	delete objectManager_;
}

/*!
	@brief Initializer of DataStoreV4
*/
void DataStoreV4::initialize(ManagerSet& resourceSet) {
	config_ = resourceSet.dsConfig_;
	clusterService_ = resourceSet.clsSvc_;
}


/*!
	@brief Creates or Updates Container
*/
BaseContainer *DataStoreV4::putContainer(TransactionContext &txn,
	const FullContainerKey &containerKey, uint8_t containerType,
	uint32_t schemaSize, const uint8_t *containerSchema, bool isEnable,
	int32_t featureVersion,
	PutStatus &status, bool isCaseSensitive, KeyDataStoreValue &keyStoreValue) {
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
					getConfig(), containerName.c_str(),
					in, featureVersion);
			break;
		case TIME_SERIES_CONTAINER:
			messageSchema = ALLOC_NEW(alloc)
				MessageTimeSeriesSchema(alloc,
					getConfig(), containerName.c_str(),
					in, featureVersion);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_TYPE_INVALID, "");
			break;
		}

		if (keyStoreValue.containerId_ != UNDEF_CONTAINERID) {
			bool isAllowExpiration = true;
			container = getBaseContainer(txn, keyStoreValue.oId_, containerType, isAllowExpiration);
		}
		if (container == NULL) {
			container = createContainer(
				txn, containerKey, containerType, messageSchema);
			status = PutStatus::CREATE;
			GS_TRACE_INFO(DATA_STORE, GS_TRACE_DS_DS_CREATE_CONTAINER,
				"name = " << containerName
						  << " Id = " << container->getContainerId()
						  << " pId = " << txn.getPartitionId());
		}
		else {
			if (isCaseSensitive) {
				FullContainerKey existContainerKey = container->getContainerKey(txn);
				if (containerKey.compareTo(alloc, existContainerKey, isCaseSensitive) != 0) {
					util::String inputStr(alloc), existStr(alloc);
					containerKey.toString(alloc, inputStr);
					existContainerKey.toString(alloc, existStr);
					GS_THROW_USER_ERROR(
						GS_ERROR_TXN_INDEX_ALREADY_EXISTS,
						"Case sensitivity mismatch, existed container name = "
						<< existStr
						<< ", input container name = " << inputStr);
				}
			}
			status = changeContainer(txn, containerKey, container,
				messageSchema, isEnable, isCaseSensitive);
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
ContainerId DataStoreV4::dropContainer(TransactionContext &txn,
	uint8_t containerType,
	bool isCaseSensitive, KeyDataStoreValue &keyStoreValue) {
	try {
		if (!isActive()) {
			return UNDEF_CONTAINERID;
		}
		util::StackAllocator& alloc = txn.getDefaultAllocator();
		BaseContainer* container = NULL;
		if (keyStoreValue.containerId_ != UNDEF_CONTAINERID) {
			bool isAllowExpiration = true;
			container = getBaseContainer(txn, keyStoreValue.oId_, containerType, isAllowExpiration);
		}
		StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, container);
		container = containerAutoPtr.get();

		if (container == NULL) {
			return UNDEF_CONTAINERID;
		}
		if (!container->isExpired(txn) && !container->isInvalid() && container->hasUncommitedTransaction(txn)) {
			DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_CON_LOCK_CONFLICT,
				"drop container(pId=" << txn.getPartitionId()
					<< ", containerId=" << container->getContainerId()
					<< ", txnId=" << txn.getId() << ")");
		}

		ContainerId containerId = container->getContainerId();

		keyStore_->remove(txn, container->getContainerKeyOId());

		if (!container->isInvalid()) {  
			finalizeContainer(txn, container);
		}

		return containerId;
	}
	catch (std::exception &e) {
		handleUpdateError(e, GS_ERROR_DS_DS_DROP_COLLECTION_FAILED);
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Remove Container Schema in Container Schema Map
*/
void DataStoreV4::removeColumnSchema(
	TransactionContext &txn, OId schemaOId) {
	ShareValueList commonContainerSchema(*getObjectManager(), allocateStrategy_, schemaOId);
	commonContainerSchema.decrement();
	int64_t schemaHashKey = commonContainerSchema.getHashVal();
	if (commonContainerSchema.getReferenceCounter() == 0) {
		StackAllocAutoPtr<BtreeMap> schemaMap(
			txn.getDefaultAllocator(), getSchemaMap(txn));

		bool isCaseSensitive = true;
		int32_t status =
			schemaMap.get()->remove(txn, schemaHashKey, schemaOId, isCaseSensitive);
		if ((status & BtreeMap::ROOT_UPDATE) != 0) {
			DataStorePartitionHeaderObject partitionHeaderObject(
				*getObjectManager(),
				allocateStrategy_, headerOId_);
			partitionHeaderObject.setSchemaMapOId(
				schemaMap.get()->getBaseOId());
		}
		finalizeSchema(
			*getObjectManager(), allocateStrategy_ , 
			&commonContainerSchema);
		commonContainerSchema.finalize();
	}
}

void DataStoreV4::finalizeContainer(TransactionContext &txn, BaseContainer *container) {
	if (container == NULL) {
		return;
	}
	util::StackAllocator& alloc = txn.getDefaultAllocator();
	OId schemaOId = container->getColumnSchemaId();
	FullContainerKey containerKey = container->getContainerKey(txn);
	DatabaseId dbId = containerKey.getComponents(alloc, false).dbId_;
	util::String affinityStr(alloc);
	container->getAffinityStr(affinityStr);
	ChunkKey chunkKey = container->getChunkKey();
	bool isGroupEmpty = removeGroupId(dbId, affinityStr, chunkKey, container->getBaseGroupId());
	bool isUnique = false;
	if (strcmp(affinityStr.c_str(), UNIQUE_GROUP_ID_KEY) == 0) {
		isUnique = true;
	}
	bool isFinished = container->finalize(txn, isUnique);
	if (isFinished) {
		removeColumnSchema(txn, schemaOId);
		GS_TRACE_INFO(
			DATASTORE_BACKGROUND, GS_ERROR_DS_BACKGROUND_TASK_INVALID, 
			"[DropContainer immediately finished, PartitionId = " << txn.getPartitionId()
				<< ", containerId = " << container->getContainerId());
	} else {
		BackgroundData bgData;
		bgData.setDropContainerData(container->getMetaAllocateStrategy().getGroupId(), 
			container->getBaseOId(), container->getContainerExpirationTime());
		insertBGTask(txn, bgData);

		GS_TRACE_INFO(
			DATASTORE_BACKGROUND, GS_ERROR_DS_BACKGROUND_TASK_INVALID, 
			"[DropContainer start, PartitionId = " << txn.getPartitionId()
				<< ", containerId = " << container->getContainerId());
	}
}

void DataStoreV4::finalizeMap(TransactionContext &txn, const AllocateStrategy &allocateStrategy, 
	BaseIndex *index, Timestamp expirationTime) {
	if (index == NULL) {
		return;
	}
	bool isFinished = index->finalize(txn);
	if (isFinished) {
		GS_TRACE_INFO(
			DATASTORE_BACKGROUND, GS_ERROR_DS_BACKGROUND_TASK_INVALID,
			"[DropIndex immediately finished, PartitionId = " << txn.getPartitionId()
				<< ", mapType = " << (int)index->getMapType()
				<< ", oId = " << index->getBaseOId());
	} else {
		BackgroundData bgData;
		bgData.setDropIndexData(index->getMapType(), allocateStrategy.getGroupId(), index->getBaseOId(), expirationTime);
		insertBGTask(txn, bgData);

		GS_TRACE_INFO(
			DATASTORE_BACKGROUND, GS_ERROR_DS_BACKGROUND_TASK_INVALID,
			"[DropIndex start, PartitionId = " << txn.getPartitionId()
				<< ", mapType = " << (int)index->getMapType()
				<< ", oId = " << index->getBaseOId());
	}
}

template<>
DataStoreV4::BackgroundData BtreeMap::getMaxValue() {
	return DataStoreV4::BackgroundData();
}
template<>
DataStoreV4::BackgroundData BtreeMap::getMinValue() {
	return DataStoreV4::BackgroundData();
}

template int32_t BtreeMap::getAll(TransactionContext &txn, ResultSize limit,
	util::XArray< std::pair<BackgroundId, DataStoreV4::BackgroundData> > &keyValueList);
template int32_t BtreeMap::getAll(TransactionContext &txn, ResultSize limit,
	util::XArray<std::pair<BackgroundId, DataStoreV4::BackgroundData> > &keyValueList,
	BtreeMap::BtreeCursor &cursor);
BGTask DataStoreV4::searchBGTask(TransactionContext &txn) {
	BGTask bgTask;
	bool isFound = false;

	if (isActive() && getBGTaskCount() > 0) {
		StackAllocAutoPtr<BtreeMap> map(txn.getDefaultAllocator(),
			getBackgroundMap(txn));
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

	return bgTask;
}

bool DataStoreV4::executeBGTask(TransactionContext &txn, const BGTask bgTask) {
	bool isFinished = false;
	try {
		if (!isActive()) {
			isFinished = true;
			return isFinished;
		}

		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		util::XArray<BackgroundData> list(txn.getDefaultAllocator());
		{
			StackAllocAutoPtr<BtreeMap> bgMap(txn.getDefaultAllocator(),
				getBackgroundMap(txn));
			TermCondition cond(COLUMN_TYPE_LONG, COLUMN_TYPE_LONG, 
				DSExpression::EQ, UNDEF_COLUMNID, &bgTask.bgId_, sizeof(BackgroundId));
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
		isFinished = executeBGTaskInternal(txn, bgTask.bgId_, bgData);
		if (isFinished) {
			removeBGTask(txn, bgTask.bgId_, bgData);
		}
	}
	catch (std::exception &e) {
		handleUpdateError(e, GS_ERROR_DS_BACKGROUND_TASK_INVALID);
	}
	return isFinished;
}

bool DataStoreV4::executeBGTaskInternal(TransactionContext &txn, BackgroundId bgId, BackgroundData &bgData) {
	const double FORCE_COLD_SWAP_RATE = 0;
	getObjectManager()->setStoreMemoryAgingSwapRate(FORCE_COLD_SWAP_RATE);
	try {
		switch (bgData.getEventType()) {
		case BackgroundData::DROP_CONTAINER:
		{
			DSGroupId groupId = UNDEF_DS_GROUPID;
			Timestamp expirationTime = UNDEF_TIMESTAMP;
			OId oId = UNDEF_OID;
			bgData.getDropContainerData(groupId, oId, expirationTime);
			bool isAllowExpiration = true;
			StackAllocAutoPtr<BaseContainer> container(txn.getDefaultAllocator(),
				getBaseContainer(txn, oId,
					ANY_CONTAINER, isAllowExpiration));
			if (container.get() == NULL || container.get()->isInvalid() || container.get()->isExpired(txn)) {
				return true;
			}
			else {
				return container.get()->finalize(txn);
			}
		}
		break;
		case BackgroundData::DROP_INDEX:
		{
			MapType mapType;
			DSGroupId groupId;
			OId mapOId;
			Timestamp expirationTime;

			bgData.getDropIndexData(mapType, groupId, mapOId, expirationTime);

			Timestamp timestamp = txn.getStatementStartTime().getUnixTime();

			if (!objectManager_->isActive(groupId) ||
					expirationTime <= stats().getTimestamp(
							DataStoreStats::DS_STAT_BATCH_FREE_TIME) ||
					expirationTime <= timestamp) {
				return true;
			}
			else {
				AllocateStrategy strategy(groupId, getObjectManager());
				StackAllocAutoPtr<BaseIndex> map(txn.getDefaultAllocator(),
					getIndex(txn, *getObjectManager(), mapType, mapOId,
						strategy, NULL, NULL));
				return map.get()->finalize(txn);
			}
		}
		break;
		default:
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
			break;
		}
	}
	catch (UserException&) {
		BackgroundData afterBgData = bgData;
		afterBgData.incrementError();
		updateBGTask(txn, bgId, bgData, afterBgData);
		return false;
	}
}

BaseIndex *DataStoreV4::getIndex(TransactionContext &txn, 
	ObjectManagerV4 &objectManager, MapType mapType, OId mapOId, 
	AllocateStrategy &strategy, BaseContainer *container,
	TreeFuncInfo *funcInfo) {
	BaseIndex *map = NULL;
	switch (mapType) {
	case MAP_TYPE_BTREE:
		map = ALLOC_NEW(txn.getDefaultAllocator()) BtreeMap(
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



template <>
int32_t BtreeMap::getInitialItemSizeThreshold<BackgroundId, DataStoreV4::BackgroundData>() {
	return INITIAL_MVCC_ITEM_SIZE_THRESHOLD;
}

/*!
	@brief Allocate DataStorePartitionHeader Object and BtreeMap Objects for
   Containers and Schemas
*/
void DataStoreV4::DataStorePartitionHeaderObject::initialize(
	TransactionContext& txn, AllocateStrategy& allocateStrategy, OId oId) {
	load(oId, true);
	memset(get(), 0, sizeof(DataStorePartitionHeader));

	BtreeMap schemaMap(txn, *getObjectManager(), allocateStrategy, NULL);
	schemaMap.initialize(
		txn, COLUMN_TYPE_LONG, false, BtreeMap::TYPE_SINGLE_KEY);
	setSchemaMapOId(schemaMap.getBaseOId());

	setTriggerMapOId(UNDEF_OID);

	BtreeMap backgroundMap(txn, *getObjectManager(), allocateStrategy, NULL);
	backgroundMap.initialize<BackgroundId, DataStoreV4::BackgroundData>(
		txn, COLUMN_TYPE_LONG, false, BtreeMap::TYPE_SINGLE_KEY);
	setBackgroundMapOId(backgroundMap.getBaseOId());

	BtreeMap v4chunkIdMap(txn, *getObjectManager(), allocateStrategy, NULL);
	v4chunkIdMap.initialize<DSGroupId, ChunkId>(
		txn, COLUMN_TYPE_LONG, false, BtreeMap::TYPE_SINGLE_KEY);
	setV4chunkIdMapOId(v4chunkIdMap.getBaseOId());


	get()->maxBackgroundId_ = 0;

	get()->latestCheckTime_ = 0;
}

/*!
	@brief Free DataStorePartitionHeader Object and BtreeMap Objects for
   Containers and Schemas
*/
void DataStoreV4::DataStorePartitionHeaderObject::finalize(
	TransactionContext &txn, AllocateStrategy &allocateStrategy) {
	BtreeMap schemaMap(
		txn, *getObjectManager(), getSchemaMapOId(), allocateStrategy, NULL);
	schemaMap.finalize(txn);
	BtreeMap backgroundMap(
		txn, *getObjectManager(), getBackgroundMapOId(), allocateStrategy, NULL);
	backgroundMap.finalize(txn);
	BtreeMap v4chunkIdMap(
		txn, *getObjectManager(), getV4chunkIdMapOId(), allocateStrategy, NULL);
	backgroundMap.finalize(txn);
}

void DataStoreV4::initializeHeader(TransactionContext& txn) {
	DSObjectSize requestSize = sizeof(DataStorePartitionHeader);
	headerOId_ = keyStore_->put(txn, getStoreType(), requestSize);

	DataStorePartitionHeaderObject partitionHeaderObject(
		*getObjectManager(), allocateStrategy_);
	partitionHeaderObject.initialize(txn, allocateStrategy_, headerOId_);
}



BaseContainer *DataStoreV4::createContainer(TransactionContext &txn,
	const FullContainerKey &containerKey,
	ContainerType containerType, MessageSchema *messageSchema) {
	if (!isActive()) {
		initializeHeader(txn);
	}

	OId schemaOId = insertColumnSchema(txn, messageSchema);
	const ContainerId containerId = keyStore_->allocateContainerId();

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

	DatabaseId dbId = containerKey.getComponents(txn.getDefaultAllocator(), false).dbId_;
	const util::String &affinityStr = messageSchema->getAffinityStr();
	ChunkKey chunkKey = BaseContainer::calcChunkKey(
		messageSchema->getContainerExpirationStartTime(),
		messageSchema->getContainerExpirationInfo());
	DSGroupId baseGroupId = getGroupId(dbId, affinityStr, chunkKey);
	container->set(txn, containerKey, containerId, schemaOId, messageSchema, baseGroupId);

	KeyDataStoreValue storeValue(containerId, container->getBaseOId(), getStoreType(),
		container->getAttribute());
	keyStore_->put(txn, container->getContainerKeyOId(), storeValue);

	return container;
}

PutStatus DataStoreV4::changeContainer(TransactionContext& txn,
	const FullContainerKey& containerKey, BaseContainer*& container,
	MessageSchema* messageSchema, bool isEnable,
	bool isCaseSensitive) {
	util::StackAllocator& alloc = txn.getDefaultAllocator();
	PutStatus status = PutStatus::NOT_EXECUTED;

	util::XArray<uint32_t> copyColumnMap(alloc);
	SchemaState schemaState;
	container->makeCopyColumnMap(txn, messageSchema, copyColumnMap,
		schemaState);

	if (!isEnable && schemaState != DataStoreV4::SAME_SCHEMA) {
		if (schemaState == DataStoreV4::ONLY_TABLE_PARTITIONING_VERSION_DIFFERENCE) {
			status = PutStatus::NOT_EXECUTED;
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
		setLastExpiredTime(txn.getStatementStartTime().getUnixTime());
		return PutStatus::NOT_EXECUTED;
	}
	if (container->hasUncommitedTransaction(txn)) {
		try {
			container->getContainerCursor(txn);
			GS_TRACE_INFO(
				DATASTORE_BACKGROUND, GS_TRACE_DS_DS_UPDATE_CONTAINER, "Continue to change shema");
			status = PutStatus::UPDATE;
		}
		catch (UserException& e) {
			DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_CON_LOCK_CONFLICT,
				"change schema(pId=" << txn.getPartitionId()
				<< ", containerId=" << container->getContainerId()
				<< ", txnId=" << txn.getId() << ")");
		}
		return status;
	}
	switch (schemaState) {
	case DataStoreV4::COLUMNS_DIFFERENCE:
		changeContainerSchema(
			txn, containerKey, container, messageSchema, copyColumnMap);
		GS_TRACE_INFO(
			DATA_STORE, GS_TRACE_DS_DS_UPDATE_CONTAINER, "Change shema");
		status = PutStatus::UPDATE;
		break;
	case DataStoreV4::COLUMNS_RENAME:
		changeContainerProperty(
			txn, container, messageSchema);
		GS_TRACE_INFO(
			DATA_STORE, GS_TRACE_DS_DS_UPDATE_CONTAINER, "Column rename");
		status = PutStatus::CHANGE_PROPERLY;
		break;
	case DataStoreV4::PROPERLY_DIFFERENCE:
		changeContainerProperty(
			txn, container, messageSchema);
		GS_TRACE_INFO(
			DATA_STORE, GS_TRACE_DS_DS_UPDATE_CONTAINER, "Change property");
		status = PutStatus::CHANGE_PROPERLY;
		break;
	case DataStoreV4::ONLY_TABLE_PARTITIONING_VERSION_DIFFERENCE:
		changeTablePartitioningVersion(
			txn, container, messageSchema);
		GS_TRACE_INFO(
			DATA_STORE, GS_TRACE_DS_DS_UPDATE_CONTAINER, "Change table partitioning version");
		status = PutStatus::CHANGE_PROPERLY;
		break;
	case DataStoreV4::COLUMNS_ADD:
		addContainerSchema(
			txn, container, messageSchema);
		GS_TRACE_INFO(
			DATA_STORE, GS_TRACE_DS_DS_UPDATE_CONTAINER, "Column add");
		status = PutStatus::CHANGE_PROPERLY;
		break;
	default:
		assert(schemaState == DataStoreV4::SAME_SCHEMA);
		break;
	}

	return status;
}

void DataStoreV4::changeContainerSchema(TransactionContext &txn,
	const FullContainerKey &containerKey,
	BaseContainer *&container, MessageSchema *messageSchema,
	util::XArray<uint32_t> &copyColumnMap) {

	OId schemaOId = insertColumnSchema(txn, messageSchema);
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

	DatabaseId dbId = containerKey.getComponents(txn.getDefaultAllocator(), false).dbId_;
	DSGroupId baseGroupId = getGroupId(dbId, messageSchema->getAffinityStr(), container->getChunkKey());
	newContainer->initialize(txn);
	newContainer->set(txn, containerKey,
		container->getContainerId(), schemaOId, messageSchema, baseGroupId);
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
	TEST_PRINT1("databaseVersionId %d\n", containerKey.getComponents().dbId_);
	TEST_PRINT1("attribute %d\n", (int32_t)attribute);

	if (txn.isAutoCommit()) {
		container->updateContainer(txn, newContainer);
		finalizeContainer(txn, container);
		ALLOC_DELETE(txn.getDefaultAllocator(), container);
		container = newContainer;
	} else {
		ALLOC_DELETE(txn.getDefaultAllocator(), newContainer);
	}
}

void DataStoreV4::changeContainerProperty(TransactionContext &txn,
	BaseContainer *&container, MessageSchema *messageSchema) {

	ContainerId containerId = container->getContainerId();
	ContainerType containerType = container->getContainerType();
	OId schemaOId = insertColumnSchema(txn, messageSchema);
	container->changeProperty(txn, schemaOId);
	container->setTablePartitioningVersionId(messageSchema->getTablePartitioningVersionId());
	OId containerOId = container->getBaseOId();
	ALLOC_DELETE(txn.getDefaultAllocator(), container);
	container = getBaseContainer(txn, containerOId, containerType);
}

void DataStoreV4::changeTablePartitioningVersion(TransactionContext &txn,
	BaseContainer *&container, MessageSchema *messageSchema) {
	UNUSED_VARIABLE(txn);
	container->setTablePartitioningVersionId(messageSchema->getTablePartitioningVersionId());
}

void DataStoreV4::addContainerSchema(TransactionContext &txn,
	BaseContainer *&container, MessageSchema *messageSchema) {

	uint32_t oldColumnNum = container->getColumnNum();

	ContainerId containerId = container->getContainerId();
	ContainerType containerType = container->getContainerType();
	OId schemaOId = insertColumnSchema(txn, messageSchema);

	OId oldSchemaOId = container->getColumnSchemaId();
	container->changeProperty(txn, schemaOId);
	container->setTablePartitioningVersionId(messageSchema->getTablePartitioningVersionId());

	OId containerOId = container->getBaseOId();
	ALLOC_DELETE(txn.getDefaultAllocator(), container);
	if (oldSchemaOId != UNDEF_OID) {
		removeColumnSchema(
			txn, oldSchemaOId);
	}

	container = getBaseContainer(txn, containerOId, containerType);
	container->changeNullStats(txn, oldColumnNum);
}

OId DataStoreV4::getColumnSchemaId(TransactionContext &txn,
	MessageSchema *messageSchema, int64_t schemaHashKey) {
	StackAllocAutoPtr<BtreeMap> schemaMap(
		txn.getDefaultAllocator(), getSchemaMap(txn));
	OId schemaOId = UNDEF_OID;
	util::XArray<OId> schemaList(txn.getDefaultAllocator());
	TermCondition cond(COLUMN_TYPE_LONG, COLUMN_TYPE_LONG, 
		DSExpression::EQ, UNDEF_COLUMNID, &schemaHashKey, sizeof(schemaHashKey));
	BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, MAX_RESULT_SIZE);
	schemaMap.get()->search(txn, sc, schemaList);
	for (size_t i = 0; i < schemaList.size(); i++) {
		ShareValueList commonContainerSchema(
			*getObjectManager(), allocateStrategy_, schemaList[i]);
		if (schemaCheck(txn, *getObjectManager(), allocateStrategy_,
				&commonContainerSchema, messageSchema)) {
			schemaOId = schemaList[i];
			break;
		}
	}
	return schemaOId;
}

OId DataStoreV4::insertColumnSchema(TransactionContext &txn,
	MessageSchema *messageSchema) {
	int64_t schemaHashKey = calcSchemaHashKey(messageSchema);
	OId schemaOId = getColumnSchemaId(txn, messageSchema, schemaHashKey);
	if (schemaOId == UNDEF_OID) {
		StackAllocAutoPtr<BtreeMap> schemaMap(
			txn.getDefaultAllocator(), getSchemaMap(txn));
		uint32_t bodyAllocateSize;
		util::XArray<ShareValueList::ElemData> inputList(
			txn.getDefaultAllocator());
		bool onMemory = false;
		initializeSchema(txn, *getObjectManager(), messageSchema,
			allocateStrategy_, inputList, bodyAllocateSize, onMemory);
		uint32_t allocateSize = ShareValueList::getHeaderSize(
									static_cast<int32_t>(inputList.size())) +
								bodyAllocateSize;

		ShareValueList commonContainerSchema(*getObjectManager(), allocateStrategy_);
		commonContainerSchema.initialize(txn, allocateSize, allocateStrategy_, onMemory);
		commonContainerSchema.set(schemaHashKey, inputList);
		schemaOId = commonContainerSchema.getBaseOId();

		bool isCaseSensitive = true;
		int32_t status =
			schemaMap.get()->insert(txn, schemaHashKey, schemaOId, isCaseSensitive);
		if ((status & BtreeMap::ROOT_UPDATE) != 0) {
			DataStorePartitionHeaderObject partitionHeaderObject(
				*getObjectManager(),
				allocateStrategy_, headerOId_);
			partitionHeaderObject.setSchemaMapOId(
				schemaMap.get()->getBaseOId());
		}
	}
	else {
		ShareValueList commonContainerSchema(
			*getObjectManager(), allocateStrategy_, schemaOId);
		commonContainerSchema.increment();
	}
	return schemaOId;
}

/*!
	@brief Get Container Schema Map
*/
BtreeMap *DataStoreV4::getSchemaMap(TransactionContext &txn) {
	BtreeMap *schemaMap = NULL;

	DataStorePartitionHeaderObject partitionHeaderObject(
		*getObjectManager(), allocateStrategy_, headerOId_);
	schemaMap =
		ALLOC_NEW(txn.getDefaultAllocator()) BtreeMap(txn, *getObjectManager(),
			partitionHeaderObject.getSchemaMapOId(), allocateStrategy_);
	return schemaMap;
}

template int32_t BtreeMap::insert(
	TransactionContext &txn, BackgroundId &key, DataStoreV4::BackgroundData &value, bool isCaseSensitive);
template int32_t BtreeMap::remove(
	TransactionContext &txn, BackgroundId &key, DataStoreV4::BackgroundData &value, bool isCaseSensitive);
template int32_t BtreeMap::update(TransactionContext &txn, BackgroundId &key,
	DataStoreV4::BackgroundData &oldValue, DataStoreV4::BackgroundData &newValue, bool isCaseSensitive);

/*!
	@brief Get Container by ContainerId
*/
BaseContainer *DataStoreV4::getBaseContainer(TransactionContext &txn,
	OId oId, ContainerType containerType, bool allowExpiration) {
	util::StackAllocator& alloc = txn.getDefaultAllocator();
	if (!isActive()) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_CONTAINER_UNEXPECTEDLY_REMOVED, "Partition " << txn.getPartitionId() << " not exist");
	}
	if (UNDEF_OID == oId) {
		return NULL;
	}
	BaseObject baseContainerImageObject(
		*getObjectManager(), allocateStrategy_, oId);
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


BtreeMap *DataStoreV4::getBackgroundMap(TransactionContext &txn) {
	BtreeMap *schemaMap = NULL;

	DataStorePartitionHeaderObject partitionHeaderObject(
		*getObjectManager(), allocateStrategy_, headerOId_);
	schemaMap =
		ALLOC_NEW(txn.getDefaultAllocator()) BtreeMap(txn, *getObjectManager(),
			partitionHeaderObject.getBackgroundMapOId(), allocateStrategy_);
	return schemaMap;
}
BackgroundId DataStoreV4::insertBGTask(TransactionContext &txn, 
	BackgroundData &bgData) {

	DataStorePartitionHeaderObject partitionHeaderObject(
		*getObjectManager(), allocateStrategy_, headerOId_);

	BackgroundId bgId = partitionHeaderObject.allocateBackgroundId();

	{
		StackAllocAutoPtr<BtreeMap> bgMap(
			txn.getDefaultAllocator(), getBackgroundMap(txn));

		bool isCaseSensitive = true;
		int32_t status =
			bgMap.get()->insert<BackgroundId, BackgroundData>(txn, bgId, bgData, isCaseSensitive);
		if ((status & BtreeMap::ROOT_UPDATE) != 0) {
			partitionHeaderObject.setBackgroundMapOId(
				bgMap.get()->getBaseOId());
		}
	}

	activeBackgroundCount_++;

	return bgId;
}
void DataStoreV4::removeBGTask(TransactionContext &txn, BackgroundId bgId,
	BackgroundData &bgData) {
	StackAllocAutoPtr<BtreeMap> bgMap(
		txn.getDefaultAllocator(), getBackgroundMap(txn));

	bool isCaseSensitive = true;
	int32_t status =
		bgMap.get()->remove<BackgroundId, BackgroundData>(txn, bgId, bgData, isCaseSensitive);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		DataStorePartitionHeaderObject partitionHeaderObject(
			*getObjectManager(), allocateStrategy_, headerOId_);
		partitionHeaderObject.setBackgroundMapOId(
			bgMap.get()->getBaseOId());
	}
	activeBackgroundCount_--;
}

void DataStoreV4::updateBGTask(TransactionContext &txn, BackgroundId bgId, 
	BackgroundData &beforBgData, BackgroundData &afterBgData) {
	StackAllocAutoPtr<BtreeMap> bgMap(
		txn.getDefaultAllocator(), getBackgroundMap(txn));

	bool isCaseSensitive = true;
	int32_t status =
		bgMap.get()->update<BackgroundId, BackgroundData>(txn, bgId, beforBgData, afterBgData, isCaseSensitive);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		DataStorePartitionHeaderObject partitionHeaderObject(
			*getObjectManager(), allocateStrategy_, headerOId_);
		partitionHeaderObject.setBackgroundMapOId(
			bgMap.get()->getBaseOId());
	}
}

/*!
	@brief Prepare for redo log
*/
void DataStoreV4::activate(
	TransactionContext &txn, ClusterService *clusterService) {
	headerOId_ = keyStore_->get(txn, getStoreType());

	restoreBackground(txn, clusterService);
	restoreGroupId(txn, clusterService);
	restoreV4ChunkIdMap(txn, clusterService);

	restoreLastExpiredTime(txn, clusterService);
}

StoreType DataStoreV4::getStoreType() {
	return V4_COMPATIBLE;
}

void DataStoreV4::restoreLastExpiredTime(TransactionContext &txn, ClusterService *clusterService) {
	if (!isActive()) {
		return;
	}

	const DataStoreV4::Latch latch(
		txn, this, clusterService);

	DataStorePartitionHeaderObject partitionHeaderObject(
		*getObjectManager(), allocateStrategy_, headerOId_);

	dsStats_.setTimestamp(
			DataStoreStats::DS_STAT_EXPIRED_TIME,
			partitionHeaderObject.getLatestCheckTime());
	dsStats_.setTimestamp(
			DataStoreStats::DS_STAT_BATCH_FREE_TIME,
			partitionHeaderObject.getLatestBatchFreeTime());
}

/*!
	@brief Restore BackgroundCounter in the partition
*/
void DataStoreV4::restoreBackground(
	TransactionContext &txn, ClusterService *clusterService) {
	if (!isActive()) {
		return;
	}

	const DataStoreV4::Latch latch(
		txn, this, clusterService);

	StackAllocAutoPtr<BtreeMap> bgMap(
		txn.getDefaultAllocator(), getBackgroundMap(txn));

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
				activeBackgroundCount_++;
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
	@brief Restore GroupIdMap in the partition
*/
void DataStoreV4::restoreGroupId(
	TransactionContext& txn, ClusterService* clusterService) {
	if (!isActive()) {
		return;
	}
	util::StackAllocator& alloc = txn.getDefaultAllocator();
	util::StackAllocator::Scope scope(alloc);

	const DataStoreV4::Latch latch(
		txn, this, clusterService);

	ContainerId nextContainerId = 0;
	KeyDataStore::ContainerCondition condition(alloc);
	condition.setStoreType(getStoreType());
	condition.insertAttribute(CONTAINER_ATTR_SINGLE);
	condition.insertAttribute(CONTAINER_ATTR_SINGLE_SYSTEM);
	condition.insertAttribute(CONTAINER_ATTR_LARGE);
	condition.insertAttribute(CONTAINER_ATTR_SUB);
	condition.insertAttribute(CONTAINER_ATTR_VIEW);

	util::String affinityStr(alloc);
	while (1) {
		util::XArray< KeyDataStoreValue* > storeValueList(alloc);
		bool followingFound = keyStore_->scanContainerList(
			txn, nextContainerId,
			PARTIAL_RESULT_SIZE, UNDEF_DBID, condition, storeValueList);
		util::XArray< KeyDataStoreValue* >::iterator itr;
		for (itr = storeValueList.begin(); itr != storeValueList.end(); ++itr) {
			bool isAllowExpiration = true;
			StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, getBaseContainer(txn, (*itr)->oId_, ANY_CONTAINER, isAllowExpiration));
			BaseContainer* container = containerAutoPtr.get();
			FullContainerKey containerKey = container->getContainerKey(txn);
			DatabaseId dbId = containerKey.getComponents(alloc, false).dbId_;

			container->getAffinityStr(affinityStr);
			int32_t affinityId = calcAffinityGroupId(affinityStr);

			ChunkKey chunkKey = container->getChunkKey();
			DSGroupId groupId = container->getBaseGroupId();

			GroupKey groupKey(dbId, groupId, affinityId, chunkKey);
			std::map<GroupKey, int64_t>::iterator itr = groupKeyMap_.find(groupKey);
			if (itr != groupKeyMap_.end()) {
				itr->second++;
			}
			else {
				groupKeyMap_.insert(std::make_pair(GroupKey(dbId, groupId, affinityId, chunkKey), 1));
			}

			nextContainerId = container->getContainerId() + 1;
		}
		if (!followingFound) {
			break;
		}
	}
}

void DataStoreV4::restoreV4ChunkIdMap(
	TransactionContext& txn, ClusterService* clusterService) {
	if (!isActive()) {
		return;
	}

	const DataStoreV4::Latch latch(
		txn, this, clusterService);

	DataStorePartitionHeaderObject partitionHeaderObject(
		*getObjectManager(), allocateStrategy_, headerOId_);

	BtreeMap v4chunkIdMap(
		txn, *getObjectManager(), partitionHeaderObject.getV4chunkIdMapOId(), allocateStrategy_, NULL);

	BtreeMap::BtreeCursor btreeCursor;
	while (1) {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		util::XArray< std::pair<DSGroupId, ChunkId> > idList(
			txn.getDefaultAllocator());
		util::XArray< std::pair<DSGroupId, ChunkId> >::iterator itr;
		int32_t getAllStatus = v4chunkIdMap.getAll<DSGroupId, ChunkId>
			(txn, PARTIAL_RESULT_SIZE, idList, btreeCursor);

		std::deque<int64_t> chunkIdList;
		DSGroupId maxGroupId = UNDEF_DS_GROUPID;
		for (itr = idList.begin(); itr != idList.end(); itr++) {
			DSGroupId groupId = itr->first;
			ChunkId chunkId = itr->second;
			if (groupId > maxGroupId) {
				maxGroupId = groupId;
				if (chunkIdList.size() != maxGroupId + 1) {
					chunkIdList.resize(maxGroupId + 1, 0);
				}
			}
			chunkIdList[groupId] = chunkId;
		}
		getObjectManager()->initializeV5StartChunkIdList(maxGroupId, chunkIdList);
		if (getAllStatus == GS_SUCCESS) {
			break;
		}
	}
}



ShareValueList *DataStoreV4::makeCommonContainerSchema(TransactionContext &txn, int64_t schemaHashKey, MessageSchema *messageSchema)
{
	uint32_t bodyAllocateSize;
	util::XArray<ShareValueList::ElemData> inputList(
		txn.getDefaultAllocator());

	bool onMemory = true;

	initializeSchema(txn, *getObjectManager(), messageSchema,
		allocateStrategy_, inputList, bodyAllocateSize, onMemory);
	uint32_t allocateSize = ShareValueList::getHeaderSize(
								static_cast<int32_t>(inputList.size())) +
							bodyAllocateSize;
	ShareValueList *commonContainerSchema =
		ALLOC_NEW(txn.getDefaultAllocator()) ShareValueList(
			*getObjectManager(), allocateStrategy_);
	commonContainerSchema->initialize(txn, allocateSize, allocateStrategy_, onMemory);
	commonContainerSchema->set(schemaHashKey, inputList);

	return commonContainerSchema;
}

DataStoreV4::ContainerListHandler::~ContainerListHandler() {
}

DataStoreV4::Latch::Latch(TransactionContext &txn,
	DataStoreV4 *dataStore, ClusterService *clusterService)
	: txn_(txn),
	  dataStore_(dataStore),
	  clusterService_(clusterService) {
	if (dataStore_ != NULL) {
		dataStore_->getObjectManager()->checkDirtyFlag();
		ObjectManagerV4 &objectManager = *(dataStore_->getObjectManager());
		const double HOT_MODE_RATE = 1.0;
		objectManager.setStoreMemoryAgingSwapRate(HOT_MODE_RATE);
	}
}

DataStoreV4::Latch::~Latch() {
	try {
		if (dataStore_ != NULL) {
			ObjectManagerV4 &objectManager = *(dataStore_->getObjectManager());
			objectManager.checkDirtyFlag();
			objectManager.resetRefCounter();
			objectManager.freeLastLatchPhaseMemory();
			objectManager.setSwapOutCounter(0);
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
void DataStoreV4::handleUpdateError(std::exception &, ErrorCode) {
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
void DataStoreV4::handleSearchError(std::exception &, ErrorCode) {
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
uint64_t DataStoreV4::scanChunkGroup(TransactionContext& txn, Timestamp timestamp) {
	uint64_t scanNum = 0;
	try {
		if (!isActive()) {
			return scanNum;
		}

		const Timestamp expireTime = timestamp;
		const uint64_t roundingBitNum =
				DataStoreV4::DataAffinityUtils::getExpireTimeRoundingBitNum(
						DEFAULT_EXPIRE_CATEGORY_ID);
		const ChunkKey expireChunkKey =
				DataStoreV4::DataAffinityUtils::convertTimestamp2ChunkKey(
						expireTime, roundingBitNum, false);

		const Timestamp estimateBatchFreeTime =
				getConfig().getEstimateErasableExpiredTime();
		dsStats_.setTimestamp(
				DataStoreStats::DS_STAT_ESTIMATE_BATCH_FREE_TIME,
				estimateBatchFreeTime);

		const ChunkKey estimateBatchFreeChunkKey =
				DataStoreV4::DataAffinityUtils::convertTimestamp2ChunkKey(
						estimateBatchFreeTime, roundingBitNum, false);

		dsStats_.table_(DataStoreStats::DS_STAT_BG_NUM).set(getBGTaskCount());

		const uint32_t categoryCount = DS_CHUNK_CATEGORY_SIZE;
		DataStoreStats::PlainChunkGroupStats groupList[categoryCount];
		groupList[ALLOCATE_META_CHUNK].subTable_.assign(
				chunkManager_->groupStats(META_GROUP_ID));


		const Timestamp batchFreeTime =
				dsStats_.getTimestamp(DataStoreStats::DS_STAT_BATCH_FREE_TIME);
		const ChunkKey chunkKeyLowerLimit =
				DataStoreV4::DataAffinityUtils::convertTimestamp2ChunkKey(
						batchFreeTime, roundingBitNum, false);
		const GroupKey groupKey(0, 0, 0, chunkKeyLowerLimit);

		uint64_t estimateBatchFreeNum = 0;

		for (std::map<GroupKey, int64_t>::iterator itr =
				groupKeyMap_.upper_bound(groupKey);
				itr != groupKeyMap_.end(); itr++) {
			const DSGroupId baseGroupId = itr->first.groupId_;
			const ChunkKey chunkKey = itr->first.chunkKey_;

			uint64_t blockNum = 0;
			for (uint32_t i = 0; i < 2; i++) {
				const DSGroupId groupId = baseGroupId + i;
				const ChunkCategoryId categoryId =
						getCategoryId(groupId, chunkKey);
				const ChunkGroupStats::Table &subStats =
						chunkManager_->groupStats(groupId);

				groupList[categoryId].subTable_.merge(subStats);

				blockNum += subStats(
						ChunkGroupStats::GROUP_STAT_USE_BLOCK_COUNT).get();
			}

 			if (chunkKey <= estimateBatchFreeChunkKey) {
 				dsStats_.table_(
 						DataStoreStats::DS_STAT_ESTIMATE_BATCH_FREE_NUM)
 						.add(blockNum);
			}

			if (chunkKey <= expireChunkKey) {
				StatStopwatch &watch = dsStats_.timeTable_(
						DataStoreStats::DS_STAT_SCAN_TIME);
				watch.start();
				for (uint32_t i = 0; i < 2; i++) {
					chunkManager_->removeGroup(baseGroupId + i);
				}
				watch.stop();

				scanNum += blockNum;
 				dsStats_.table_(DataStoreStats::DS_STAT_SCAN_NUM)
 						.add(blockNum);
 				dsStats_.table_(DataStoreStats::DS_STAT_BATCH_FREE_NUM)
 						.add(blockNum);

				setLastExpiredTime(
						DataAffinityUtils::convertChunkKey2Timestamp(chunkKey));
				setLatestBatchFreeTime(
						DataAffinityUtils::convertChunkKey2Timestamp(chunkKey));
			}
		}

		for (uint32_t i = 0; i < categoryCount; i++) {
			const ChunkCategoryId categoryId = static_cast<ChunkCategoryId>(i);
			dsStats_.setChunkStats(
					categoryId, groupList[categoryId].subTable_);
		}
	}
	catch (std::exception& e) {
		handleUpdateError(e, GS_ERROR_DS_DS_CREATE_COLLECTION_FAILED);
	}
	return scanNum;
}

void DataStoreV4::setLastExpiredTime(Timestamp time, bool force) {
	if (!isActive()) {
		return;
	}

	DataStorePartitionHeaderObject partitionHeaderObject(
		*objectManager_, allocateStrategy_, headerOId_);
	partitionHeaderObject.setLatestCheckTime(time, force);

	if (setLatestTimeToStats(
			time, force, DataStoreStats::DS_STAT_BATCH_FREE_TIME)) {
		GS_TRACE_INFO(
				DATA_STORE, GS_ERROR_DS_BACKGROUND_TASK_INVALID,
				"setLastExpiredTime = " << time);
	}
}

void DataStoreV4::setLatestBatchFreeTime(Timestamp time, bool force) {
	if (!isActive()) {
		return;
	}
	DataStorePartitionHeaderObject partitionHeaderObject(
		*objectManager_, allocateStrategy_, headerOId_);
	partitionHeaderObject.setLatestBatchFreeTime(time, force);

	if (setLatestTimeToStats(
			time, force, DataStoreStats::DS_STAT_BATCH_FREE_TIME)) {
		GS_TRACE_INFO(
				DATA_STORE, GS_ERROR_DS_BACKGROUND_TASK_INVALID,
				"setLatestBatchFreeTime = " << time);
	}
}

bool DataStoreV4::setLatestTimeToStats(
		Timestamp time, bool force, DataStoreStats::Param param) {
	if (force || dsStats_.getTimestamp(param) < time) {
		dsStats_.setTimestamp(param, time);
		return true;
	}
	return false;
}

void DataStoreV4::clearTimeStats() {
	dsStats_.setTimestamp(DataStoreStats::DS_STAT_EXPIRED_TIME, 0);
	dsStats_.setTimestamp(DataStoreStats::DS_STAT_BATCH_FREE_TIME, 0);
	dsStats_.setTimestamp(DataStoreStats::DS_STAT_ESTIMATE_BATCH_FREE_TIME, 0);
}

void DataStoreV4::initializeSchema(TransactionContext &txn,
	ObjectManagerV4 &objectManager, MessageSchema *messageSchema,
	AllocateStrategy &allocateStrategy,
	util::XArray<ShareValueList::ElemData> &list, uint32_t &allocateSize,
	bool onMemory) {
	allocateSize = 0;
	{
		ShareValueList::ElemData elem(META_TYPE_COLUMN_SCHEMA);
		elem.size_ =
			ColumnSchema::getAllocateSize(messageSchema->getColumnCount(), 
			messageSchema->getRowKeyNum());
		elem.binary_ = ALLOC_NEW(txn.getDefaultAllocator()) uint8_t[elem.size_];
		memset(elem.binary_, 0, elem.size_);

		ColumnSchema *columnSchema =
			reinterpret_cast<ColumnSchema *>(elem.binary_);
		columnSchema->initialize(messageSchema->getColumnCount());
		columnSchema->set(txn, objectManager, messageSchema, allocateStrategy, onMemory);

		allocateSize += elem.size_;
		list.push_back(elem);
	}

	{
		ShareValueList::ElemData elem(META_TYPE_AFFINITY);
		elem.size_ = sizeof(char[AFFINITY_STRING_MAX_LENGTH]);
		elem.binary_ = ALLOC_NEW(txn.getDefaultAllocator()) uint8_t[elem.size_];
		memset(elem.binary_, 0, elem.size_);

		char *affinityStr = reinterpret_cast<char *>(elem.binary_);
		const util::String &affinityString = messageSchema->getAffinityStr();
		memcpy(affinityStr, affinityString.c_str(), affinityString.length());

		allocateSize += elem.size_;
		list.push_back(elem);
	}

	{
		ShareValueList::ElemData elem(META_TYPE_ATTRIBUTES);
		elem.size_ = sizeof(ContainerAttribute);
		elem.binary_ = ALLOC_NEW(txn.getDefaultAllocator()) uint8_t[elem.size_];
		memset(elem.binary_, 0, elem.size_);

		ContainerAttribute *attribute =
			reinterpret_cast<ContainerAttribute *>(elem.binary_);
		*attribute = messageSchema->getContainerAttribute();

		allocateSize += elem.size_;
		list.push_back(elem);
	}
	{
		ShareValueList::ElemData elem(META_TYPE_CONTAINER_DURATION);
		elem.size_ = sizeof(BaseContainer::ContainerExpirationInfo);
		elem.binary_ =
			ALLOC_NEW(txn.getDefaultAllocator()) uint8_t[elem.size_];
		memset(elem.binary_, 0, elem.size_);

		BaseContainer::ContainerExpirationInfo *containerExpirationInfo =
			reinterpret_cast<BaseContainer::ContainerExpirationInfo *>(elem.binary_);
		*containerExpirationInfo = messageSchema->getContainerExpirationInfo();

		allocateSize += elem.size_;
		list.push_back(elem);
	}
}


void DataStoreV4::finalizeSchema(
	ObjectManagerV4 &objectManager, AllocateStrategy& allocateStrategy, ShareValueList *commonContainerSchema) {
	ColumnSchema *columnSchema =
		commonContainerSchema->get<ColumnSchema>(META_TYPE_COLUMN_SCHEMA);
	if (columnSchema != NULL) {
		columnSchema->finalize(objectManager, allocateStrategy);
	}



}

bool DataStoreV4::schemaCheck(TransactionContext &txn,
	ObjectManagerV4 &objectManager, AllocateStrategy& allocateStrategy, ShareValueList *commonContainerSchema,
	MessageSchema *messageSchema) {
	ColumnSchema *columnSchema =
		commonContainerSchema->get<ColumnSchema>(META_TYPE_COLUMN_SCHEMA);
	if (!columnSchema->schemaCheck(txn, objectManager, allocateStrategy, messageSchema)) {
		return false;
	}

	const uint8_t *affinityBinary = commonContainerSchema->get<uint8_t>(META_TYPE_AFFINITY);

	char inputAffinityStr[AFFINITY_STRING_MAX_LENGTH];
	memset(inputAffinityStr, 0, sizeof(char[AFFINITY_STRING_MAX_LENGTH]));

	const util::String &affinityString = messageSchema->getAffinityStr();
	memcpy(inputAffinityStr, affinityString.c_str(), affinityString.length());

	if (memcmp(affinityBinary, inputAffinityStr, AFFINITY_STRING_MAX_LENGTH) !=
		0) {
		return false;
	}

	ContainerAttribute *attribute =
		commonContainerSchema->get<ContainerAttribute>(META_TYPE_ATTRIBUTES);
	if (*attribute != messageSchema->getContainerAttribute()) {
		return false;
	}

	BaseContainer::ContainerExpirationInfo *containerExpirationInfo =
		commonContainerSchema->get<BaseContainer::ContainerExpirationInfo>(
			META_TYPE_CONTAINER_DURATION);
	if (containerExpirationInfo != NULL) {
		if (!(*containerExpirationInfo ==
				messageSchema->getContainerExpirationInfo())) {
			return false;
		}
	}
	else {
		BaseContainer::ContainerExpirationInfo defaultExpirationInfo;
		if (!(defaultExpirationInfo ==
				messageSchema->getContainerExpirationInfo())) {
			return false;
		}
	}
	return true;
}

int64_t DataStoreV4::calcSchemaHashKey(MessageSchema *messageSchema) {
	int64_t hashVal = 0;
	hashVal += ColumnSchema::calcSchemaHashKey(messageSchema);

	const char *affinityStr = messageSchema->getAffinityStr().c_str();
	for (size_t i = 0; i < strlen(affinityStr); i++) {
		hashVal += static_cast<int64_t>(affinityStr[i]) + 1000000;
	}

	ContainerAttribute attribute = messageSchema->getContainerAttribute();
	hashVal += static_cast<int64_t>(attribute) + 100000000;

	return hashVal;
}

ChunkCategoryId DataStoreV4::getCategoryId(DSGroupId groupId, ChunkKey chunkKey) const {
	if (groupId == META_GROUP_ID) {
		return ALLOCATE_META_CHUNK;
	}
	else if (isMapGroup(groupId) && chunkKey == MAX_CHUNK_KEY) {
		return ALLOCATE_NO_EXPIRE_MAP;
	}
	else if (isMapGroup(groupId) && chunkKey != MAX_CHUNK_KEY) {
		return ALLOCATE_EXPIRE_MAP;
	}
	else if (!isMapGroup(groupId) && chunkKey == MAX_CHUNK_KEY) {
		return ALLOCATE_NO_EXPIRE_ROW;
	}
	else {
		return ALLOCATE_EXPIRE_ROW;
	}
}

bool DataStoreV4::isMapGroup(DSGroupId groupId) const {
	return groupId % 2 == 1;
}

#ifdef WIN32
static inline uint32_t __builtin_clz(uint32_t x) {
	return 31 - util::nlz(x);
}
static inline uint32_t u32(uint64_t x) {
	return static_cast<uint32_t>(x >> 32);
}
static inline uint32_t l32(uint64_t x) {
	return static_cast<uint32_t>(x & 0xFFFFFFFF);
}
static inline uint64_t __builtin_clzll(uint64_t x) {
	return u32(x) ? __builtin_clz(u32(x)) : __builtin_clz(l32(x)) + 32;
}
#endif

uint32_t DataStoreV4::DataAffinityUtils::ilog2(uint64_t x) {
	assert(x > 0);
	return (x > 0) ?
		static_cast<uint32_t>(8 * sizeof(uint64_t) - __builtin_clzll((x)) - 1)
		: static_cast<uint32_t>(8 * sizeof(uint64_t));
}


ChunkKey DataStoreV4::DataAffinityUtils::convertTimestamp2ChunkKey(
	Timestamp time, uint64_t roundingBitNum, bool isRoundUp) {
	assert(roundingBitNum >= CHUNKKEY_BIT_NUM);
	if (isRoundUp) {
		assert(time > 0);
		ChunkKey chunkKey = static_cast<ChunkKey>((time - 1) >> roundingBitNum) + 1;
		return static_cast<ChunkKey>(chunkKey << (roundingBitNum - CHUNKKEY_BIT_NUM));
	}
	else {
		ChunkKey chunkKey = static_cast<ChunkKey>(time >> roundingBitNum);
		return static_cast<ChunkKey>(chunkKey << (roundingBitNum - CHUNKKEY_BIT_NUM));
	}
}

ExpireIntervalCategoryId DataStoreV4::DataAffinityUtils::calcExpireIntervalCategoryId(
	uint64_t expireIntervalMillis) {
	if (expireIntervalMillis > 0) {
		uint64_t log2Value = ilog2(expireIntervalMillis);
		if (log2Value < MIN_EXPIRE_INTERVAL_CATEGORY_TERM_BITS) {
			return 0;
		}
		else {
			return static_cast<ExpireIntervalCategoryId>(
				(log2Value > MAX_EXPIRE_INTERVAL_CATEGORY_TERM_BITS) ?
				(EXPIRE_INTERVAL_CATEGORY_COUNT - 1)
				: ((log2Value - MIN_EXPIRE_INTERVAL_CATEGORY_TERM_BITS) / 2 + 1)
				);
		}
	}
	else {
		assert(false); 
		return 0;
	}
}

uint64_t DataStoreV4::DataAffinityUtils::getExpireTimeRoundingBitNum(
	ExpireIntervalCategoryId expireCategory) {
	assert(expireCategory < EXPIRE_INTERVAL_CATEGORY_COUNT);
	return expireCategory * 2 + MIN_EXPIRE_INTERVAL_CATEGORY_ROUNDUP_BITS;
}


DataStoreV4::ConfigSetUpHandler DataStoreV4::configSetUpHandler_;

void DataStoreV4::ConfigSetUpHandler::operator()(ConfigTable &config) {
	CONFIG_TABLE_RESOLVE_GROUP(config, CONFIG_TABLE_DS, "dataStore");

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_PARTITION_NUM, INT32)
		.setMin(1)
		.setMax(static_cast<int32_t>(KeyDataStore::MAX_PARTITION_NUM))
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

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_STORE_BLOCK_EXTENT_SIZE, INT32)
		.setMin(1)
		.setMax(1024)
		.setDefault(64);

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_DB_PATH, STRING)
		.setDefault("data");
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_BACKUP_PATH, STRING)
		.setDefault("backup");
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_SYNC_TEMP_PATH, STRING)
		.setDefault("sync");  
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_ARCHIVE_TEMP_PATH, STRING)
		.setDefault("archive");  
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_TRANSACTION_LOG_PATH, STRING)
		.setDefault("txnlog"); 
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
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_AFFINITY_GROUP_SIZE, INT32)
		.setMin(1)
		.setMax(10000)
		.setDefault(7);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_DS_STORE_COMPRESSION_MODE, INT32)
		.setExtendedType(ConfigTable::EXTENDED_TYPE_ENUM)
		.addEnum(ChunkCompressionTypes::NO_BLOCK_COMPRESSION, "NO_COMPRESSION")
		.addEnum(ChunkCompressionTypes::BLOCK_COMPRESSION, "COMPRESSION")
		.setDefault(ChunkCompressionTypes::NO_BLOCK_COMPRESSION);
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
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_PARTITION_BATCH_FREE_CHECK_CONTAINER_COUNT, INT32)
		.setDefault(5000)
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
		config, CONFIG_TABLE_DS_DB_FILE_SPLIT_COUNT, INT32)
		.setMin(1)
		.setDefault(1)
		.setMax(128);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_DS_DB_FILE_SPLIT_STRIPE_SIZE, INT32)
		.setMin(1)
		.setDefault(1)
		.setMax(INT32_MAX);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DS_LOG_MEMORY_LIMIT, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_SIZE_MB)
		.setMin(1)
		.setDefault(128)
		.setMax("128TB");
}



std::ostream &operator<<(
	std::ostream &output, const DataStoreV4::BackgroundData &bgData) {
	MapType mapType = MAP_TYPE_DEFAULT;
	DSGroupId groupId = UNDEF_DS_GROUPID;
	Timestamp expirationTime = UNDEF_TIMESTAMP;
	OId oId = UNDEF_OID;
	switch (bgData.getEventType()) {
	case DataStoreV4::BackgroundData::DROP_CONTAINER: {
		output << "[ eventType=DROP_CONTAINER,";
		bgData.getDropContainerData(groupId, oId, expirationTime);
	} break;
	case DataStoreV4::BackgroundData::DROP_INDEX: {
		output << "[ eventType=DROP_INDEX,";
		bgData.getDropIndexData(mapType, groupId, oId, expirationTime);
		output << " mapType=" << (uint32_t)mapType << ",";
	} break;
	default:
		output << "[ eventType=UNKNOWN,";
		break;
	}
	output << " groupId=" << groupId << ",";
	output << " oId=" << oId << ",";
	output << " expirationTime=" << expirationTime << ",";
	output << " status=" << (uint32_t)bgData.isInvalid() << ",";
	output << " errorCounter=" << (uint32_t)bgData.getErrorCount() << ",";
	output << "]";
	return output;
}

void DataStoreV4::dumpTraceBGTask(TransactionContext &txn) {
	util::NormalOStringStream stream;
	stream << "dumpTraceBGTask " << txn.getPartitionId() << std::endl;
	if (isActive()) {
		stream << "BGTaskCount= " << getBGTaskCount() << std::endl;

		StackAllocAutoPtr<BtreeMap> map(txn.getDefaultAllocator(),
			getBackgroundMap(txn));
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


void DataStoreV4::dumpPartition(TransactionContext &txn,
	DumpType type, const char *fileName) {
	try {
		util::NamedFile *file;
		file = UTIL_NEW util::NamedFile();
		file->open(fileName, util::FileFlag::TYPE_CREATE |
								 util::FileFlag::TYPE_TRUNCATE |
								 util::FileFlag::TYPE_WRITE_ONLY);

		if (!isActive()) {
			file->close();
			return;
		}

		util::NormalOStringStream stream;
		if (type == CONTAINER_SUMMARY) {
			stream << "ContainerId, ContainerName, ContainerType, RowNum(, "
					  "Map, ColumnNo, RowNum)*"
				   << std::endl;
		}
		file->write(stream.str().c_str(), stream.str().size());
		stream.str("");  

		util::StackAllocator& alloc = txn.getDefaultAllocator();
		util::StackAllocator::Scope scope(alloc);

		const DataStoreV4::Latch latch(
			txn, this, NULL);

		ContainerId nextContainerId = 0;
		KeyDataStore::ContainerCondition condition(alloc);
		condition.setStoreType(getStoreType());
		condition.insertAttribute(CONTAINER_ATTR_SINGLE);
		condition.insertAttribute(CONTAINER_ATTR_SINGLE_SYSTEM);
		condition.insertAttribute(CONTAINER_ATTR_LARGE);
		condition.insertAttribute(CONTAINER_ATTR_SUB);
		condition.insertAttribute(CONTAINER_ATTR_VIEW);

		util::String affinityStr;
		while (1) {
			util::XArray< KeyDataStoreValue* > storeValueList(alloc);
			bool followingFound = keyStore_->scanContainerList(
				txn, nextContainerId,
				PARTIAL_RESULT_SIZE, UNDEF_DBID, condition, storeValueList);
			util::XArray< KeyDataStoreValue* >::iterator itr;
			for (itr = storeValueList.begin(); itr != storeValueList.end(); ++itr) {
				bool isAllowExpiration = true;
				StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, getBaseContainer(txn, (*itr)->oId_, ANY_CONTAINER, isAllowExpiration));
				BaseContainer* container = containerAutoPtr.get();
				const FullContainerKey& containerKey = container->getContainerKey(txn);
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
				nextContainerId = container->getContainerId() + 1;
			}
			if (!followingFound) {
				break;
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

void DataStoreV4::finalize() {}

Serializable* DataStoreV4::exec(TransactionContext* txn, KeyDataStoreValue* storeValue, Serializable* message) {
	DSInputMes* input = static_cast<DSInputMes*>(message);
	TransactionContextScope scope(*txn);
	try {
		util::StackAllocator& alloc = txn->getDefaultAllocator();
		switch (input->type_) {
		case DS_COMMIT: {
			return execEvent(txn, storeValue, input->value_.commit_);
		} break;
		case DS_ABORT: {
			return execEvent(txn, storeValue, input->value_.abort_);
		} break;
		case DS_PUT_CONTAINER: {
			return execEvent(txn, storeValue, input->value_.putContainer_);
		} break;
		case DS_UPDATE_TABLE_PARTITIONING_ID: {
			return execEvent(txn, storeValue, input->value_.updateTablePartitioningId_);
		} break;
		case DS_DROP_CONTAINER: {
			return execEvent(txn, storeValue, input->value_.dropContainer_);
		} break;
		case DS_GET_CONTAINER: {
			return execEvent(txn, storeValue, input->value_.getContainer_);
		} break;
		case DS_CREATE_INDEX: {
			return execEvent(txn, storeValue, input->value_.createIndex_);
		} break;
		case DS_DROP_INDEX: {
			return execEvent(txn, storeValue, input->value_.dropIndex_);
		} break;
		case DS_CONTINUE_CREATE_INDEX: {
			return execEvent(txn, storeValue, input->value_.continueCreateIndex_);
		} break;
		case DS_CONTINUE_ALTER_CONTAINER: {
			return execEvent(txn, storeValue, input->value_.continueAlterContainer_);
		} break;
		case DS_PUT_ROW: {
			return execEvent(txn, storeValue, input->value_.putRow_);
		} break;
		case DS_APPEND_ROW: {
			return execEvent(txn, storeValue, input->value_.appendRow_);
		} break;
		case DS_UPDATE_ROW_BY_ID: {
			return execEvent(txn, storeValue, input->value_.updateRowById_);
		} break;
		case DS_REMOVE_ROW: {
			return execEvent(txn, storeValue, input->value_.removeRow_);
		} break;
		case DS_REMOVE_ROW_BY_ID: {
			return execEvent(txn, storeValue, input->value_.removeRowById_);
		} break;
		case DS_GET_ROW: {
			return execEvent(txn, storeValue, input->value_.getRow_);
		} break;
		case DS_GET_ROW_SET: {
			return execEvent(txn, storeValue, input->value_.getRowSet_);
		} break;
		case DS_QUERY_TQL: {
			return execEvent(txn, storeValue, input->value_.queryTQL_);
		} break;
		case DS_QUERY_GEOMETRY_WITH_EXCLUSION: {
			return execEvent(txn, storeValue, input->value_.queryGeometryWithExclusion_);
		} break;
		case DS_QUERY_GEOMETRY_RELATED: {
			return execEvent(txn, storeValue, input->value_.queryGeometryRelated_);
		} break;
		case DS_GET_TIME_RELATED: {
			return execEvent(txn, storeValue, input->value_.getRowTimeRelated_);
		} break;
		case DS_GET_INTERPOLATE: {
			return execEvent(txn, storeValue, input->value_.queryInterPolate_);
		} break;
		case DS_QUERY_AGGREGATE: {
			return execEvent(txn, storeValue, input->value_.queryAggregate_);
		} break;
		case DS_QUERY_TIME_RANGE: {
			return execEvent(txn, storeValue, input->value_.queryTimeRange_);
		} break;
		case DS_QUERY_SAMPLE: {
			return execEvent(txn, storeValue, input->value_.queryTimeSampling_);
		} break;
		case DS_QUERY_FETCH_RESULT_SET: {
			return execEvent(txn, storeValue, input->value_.queryFetchResultSet_);
		} break;
		case DS_QUERY_CLOSE_RESULT_SET: {
			return execEvent(txn, storeValue, input->value_.queryCloseResultSet_);
		} break;
		case DS_GET_CONTAINER_OBJECT: {
			return execEvent(txn, storeValue, input->value_.getContainerObject_);
		} break;
		case DS_SCAN_CHUNK_GROUP: {
			return execEvent(txn, storeValue, input->value_.scanChunkGroup_);
		} break;
		case DS_SEARCH_BACKGROUND_TASK: {
			return execEvent(txn, storeValue, input->value_.searchBackgroundTask_);
		} break;
		case DS_EXECUTE_BACKGROUND_TASK: {
			return execEvent(txn, storeValue, input->value_.executeBackgroundTask_);
		} break;
		case DS_CHECK_TIMEOUT_RESULT_SET: {
			return execEvent(txn, storeValue, input->value_.checkTimeoutResultSet_);
		} break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_MESSAGE_INVALID, "Unknown DataStore Command");
			break;
		}
		assert(false);
		return NULL;
	}
	catch (std::exception& e) {
		Event* ev=txn->getEvent();
		if ( AUDIT_TRACE_CHECK ) {
			if ( AuditCategoryType(input) != 0 ) {
				AUDIT_TRACE_ERROR_EXECUTE();
			}
		}
		handleSearchError(e, GS_ERROR_DS_DS_STATEMENT_FAILED);
		return NULL;
	}
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::Commit* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, getBaseContainer(*txn, storeValue->oId_, ANY_CONTAINER,
		opMes->allowExpiration_));
	BaseContainer* container = containerAutoPtr.get();
	Event* ev=txn->getEvent();
	if ( AUDIT_TRACE_CHECK ) {
		AUDIT_TRACE_INFO_EXECUTE(2,"COMMIT",
			(TransactionContextScope::getContainerName(*txn, container)));
	}
	checkContainerExistence(container);

	txnMgr_->commit(*txn, *container);

	DataStoreLogV4* dsLog = ALLOC_NEW(alloc) DataStoreLogV4(alloc,
		COMMIT_TRANSACTION, storeValue->containerId_,
		ALLOC_NEW(alloc) DataStoreLogV4::Commit());
	ResponsMesV4* outMes = ALLOC_NEW(alloc) ResponsMesV4(alloc);
	DSOutputMes* out = ALLOC_NEW(alloc) DSOutputMes(alloc, true, 0, outMes, dsLog);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::Abort* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, getBaseContainer(*txn, storeValue->oId_, ANY_CONTAINER,
		opMes->allowExpiration_));
	BaseContainer* container = containerAutoPtr.get();
	Event* ev=txn->getEvent();
	if ( AUDIT_TRACE_CHECK ) {
		AUDIT_TRACE_INFO_EXECUTE(2,"ABORT",
			(TransactionContextScope::getContainerName(*txn, container)));
	}

	checkContainerExistence(container);

	txnMgr_->abort(*txn, *container);

	DataStoreLogV4* dsLog = ALLOC_NEW(alloc) DataStoreLogV4(alloc,
		ABORT_TRANSACTION, storeValue->containerId_,
		ALLOC_NEW(alloc) DataStoreLogV4::Abort());
	ResponsMesV4* outMes = ALLOC_NEW(alloc) ResponsMesV4(alloc);
	DSOutputMes* out = ALLOC_NEW(alloc) DSOutputMes(alloc, true, 0, outMes, dsLog);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::PutContainer* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();

	PutStatus status;

	BaseContainer* orgContainer =
		putContainer(*txn, *(opMes->key_), opMes->containerType_,
			opMes->containerInfo_->size(), opMes->containerInfo_->data(),
			opMes->modifiable_, opMes->featureVersion_,
			status, opMes->isCaseSensitive_, *storeValue);
	Event* ev=txn->getEvent();
	if ( AUDIT_TRACE_CHECK ) {
		AUDIT_TRACE_INFO_EXECUTE(2,"PUT_CONTAINER",
			(TransactionContextScope::getContainerName(*txn, orgContainer)));
	}
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, orgContainer);
	ContainerId containerId = orgContainer->getContainerId();

	SchemaMessage* outMes = NULL;
	KeyDataStoreValue outputKeyValueStore;
	outputKeyValueStore = KeyDataStoreValue(
		orgContainer->getContainerId(),
		orgContainer->getBaseOId(),
		getStoreType(),
		orgContainer->getAttribute());
	if (status != PutStatus::UPDATE) {
		outMes = createSchemaMessage(*txn, orgContainer);
	}
	else {
		bool isImmediate = false;
		ContainerCursor containerCursor(isImmediate);
		containerCursor = orgContainer->getContainerCursor(*txn);
		StackAllocAutoPtr<BaseContainer> newContainerAutoPtr(alloc, getBaseContainer(*txn, containerCursor.getContainerOId(), ANY_CONTAINER));
		BaseContainer* newContainer = newContainerAutoPtr.get();
		outMes = createSchemaMessage(*txn, newContainer);
	}
	const void* keyBinary;
	size_t keySize;
	opMes->key_->toBinary(keyBinary, keySize);
	DataStoreLogV4::PutContainer* log = ALLOC_NEW(alloc) DataStoreLogV4::PutContainer(
		alloc, static_cast<uint8_t*>(const_cast<void*>(keyBinary)), keySize,
		opMes->containerInfo_, opMes->containerType_);
	bool executed = status != PutStatus::UPDATE;
	DataStoreLogV4* dsLog = ALLOC_NEW(alloc) DataStoreLogV4(alloc,
		PUT_CONTAINER, containerId, log);
	DSPutContainerOutputMes* out = ALLOC_NEW(alloc) DSPutContainerOutputMes(alloc, status, executed,
		outputKeyValueStore, outMes, dsLog);
	*storeValue = outputKeyValueStore;
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::UpdateTablePartitioningId* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	bool allowExpiration = true;
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, getBaseContainer(*txn, storeValue->oId_, ANY_CONTAINER, allowExpiration));
	BaseContainer* container = containerAutoPtr.get();

	PutStatus status = PutStatus::NOT_EXECUTED;
	DataStoreLogV4* dsLog = NULL;

	SchemaMessage* outMes = ALLOC_NEW(alloc) SchemaMessage(alloc);
	KeyDataStoreValue outputKeyValueStore(
		container->getContainerId(),
		container->getBaseOId(),
		getStoreType(),
		container->getAttribute());

	if (container->getTablePartitioningVersionId() < opMes->id_) {
		container->setTablePartitioningVersionId(opMes->id_);
		PutStatus status = PutStatus::UPDATE;

		outMes = createSchemaMessage(*txn, container);

		DataStoreLogV4::PutContainer* log = ALLOC_NEW(alloc) DataStoreLogV4::PutContainer(
			alloc, outMes->containerKeyBin_.data(), outMes->containerKeyBin_.size(),
			&(outMes->containerSchemaBin_), container->getContainerType());
		dsLog = ALLOC_NEW(alloc) DataStoreLogV4(alloc,
			PUT_CONTAINER, outMes->containerId_, log);
	}
	bool executed = status == PutStatus::UPDATE;
	DSPutContainerOutputMes* out = ALLOC_NEW(alloc) DSPutContainerOutputMes(alloc, status, executed,
		outputKeyValueStore, outMes, dsLog);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::DropContainer* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	Event* ev=txn->getEvent();
	if ( AUDIT_TRACE_CHECK ) {
		AUDIT_TRACE_INFO_EXECUTE(2,"DROP_CONTAINER",
			(TransactionContextScope::getContainerName(*txn, *opMes->key_)));
	}
	ContainerId containerId = dropContainer(*txn, opMes->containerType_,
		opMes->isCaseSensitive_, *storeValue);

	const void* keyBinary;
	size_t keySize;
	opMes->key_->toBinary(keyBinary, keySize);
	DataStoreLogV4::DropContainer* log = ALLOC_NEW(alloc) DataStoreLogV4::DropContainer(
		alloc, static_cast<uint8_t*>(const_cast<void*>(keyBinary)), keySize,
		opMes->containerType_);
	DataStoreLogV4* dsLog = ALLOC_NEW(alloc) DataStoreLogV4(alloc,
		DROP_CONTAINER, containerId, log);
	ResponsMesV4* outMes = ALLOC_NEW(alloc) ResponsMesV4(alloc);
	DSOutputMes* out = ALLOC_NEW(alloc) DSOutputMes(alloc, true, 0, outMes, dsLog);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::GetContainer* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	bool isExist = false;
	bool allowExpiration = false;
	SchemaMessage* schemaMes = NULL;
	Event* ev=txn->getEvent();
	if ( AUDIT_TRACE_CHECK ) {
		AUDIT_TRACE_INFO_EXECUTE(1,"GET_CONTAINER",
			(TransactionContextScope::getContainerName(*txn, *opMes->key_)));
	}
	if (storeValue->oId_ != UNDEF_OID) {
		isExist = true;
		StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, getBaseContainer(*txn, storeValue->oId_, opMes->containerType_, allowExpiration));
		schemaMes = createSchemaMessage(*txn, containerAutoPtr.get());
	}
	WithExistMessage* outMes = ALLOC_NEW(alloc) WithExistMessage(alloc, isExist, schemaMes);
	DataStoreLogV4* dsLog = NULL;
	DSOutputMes* out = ALLOC_NEW(alloc) DSOutputMes(alloc, isExist, 0, outMes, dsLog);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::CreateIndex* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	bool allowExpiration = true;
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, getBaseContainer(*txn, storeValue->oId_, ANY_CONTAINER, allowExpiration));
	BaseContainer* container = containerAutoPtr.get();
	Event* ev=txn->getEvent();
	if ( AUDIT_TRACE_CHECK ) {
		AUDIT_TRACE_INFO_EXECUTE(2,"CREATE_INDEX",
			TransactionContextScope::getContainerName(*txn, container));
	}
	checkContainerSchemaVersion(container, opMes->verId_);

	bool isImmediate = false;
	IndexCursor indexCursor = IndexCursor(isImmediate);
	bool skippedByMode = false;
	container->createIndex(*txn, *(opMes->info_), indexCursor,
		opMes->isCaseSensitive_, opMes->mode_, &skippedByMode);

	DataStoreLogV4* dsLog = NULL;
	if (!skippedByMode) {
		dsLog = ALLOC_NEW(alloc) DataStoreLogV4(alloc,
			CREATE_INDEX, storeValue->containerId_,
			ALLOC_NEW(alloc) DataStoreLogV4::CreateIndex(alloc, opMes->info_));
	}
	ResponsMesV4* outMes = ALLOC_NEW(alloc) ResponsMesV4(alloc);
	DSOutputMes* out = ALLOC_NEW(alloc) DSOutputMes(alloc, indexCursor.isFinished(), 0, outMes, dsLog);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::DropIndex* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	bool allowExpiration = true;
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, getBaseContainer(*txn, storeValue->oId_, ANY_CONTAINER, allowExpiration));
	BaseContainer* container = containerAutoPtr.get();
	Event* ev=txn->getEvent();
	if ( AUDIT_TRACE_CHECK ) {
		AUDIT_TRACE_INFO_EXECUTE(2,"DROP_INDEX",
			TransactionContextScope::getContainerName(*txn, container));
	}
	checkContainerSchemaVersion(container, opMes->verId_);

	bool skippedByMode = false;
	container->dropIndex(*txn, *(opMes->info_),
		opMes->isCaseSensitive_, opMes->mode_, &skippedByMode);

	DataStoreLogV4* dsLog = NULL;
	if (!skippedByMode) {
		dsLog = ALLOC_NEW(alloc) DataStoreLogV4(alloc,
			DELETE_INDEX, storeValue->containerId_,
			ALLOC_NEW(alloc) DataStoreLogV4::DropIndex(alloc, opMes->info_));
	}
	ResponsMesV4* outMes = ALLOC_NEW(alloc) ResponsMesV4(alloc);
	DSOutputMes* out = ALLOC_NEW(alloc) DSOutputMes(alloc, true, 0, outMes, dsLog);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::ContinueCreateIndex* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	bool allowExpiration = true;
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, getBaseContainer(*txn, storeValue->oId_, ANY_CONTAINER, allowExpiration));
	BaseContainer* container = containerAutoPtr.get();
	checkContainerExistence(container);

	IndexCursor indexCursor = container->getIndexCursor(*txn);
	container->continueCreateIndex(*txn, indexCursor);
	bool isFinished = indexCursor.isFinished();

	DataStoreLogV4* dsLog = ALLOC_NEW(alloc) DataStoreLogV4(alloc,
		CONTINUE_CREATE_INDEX, storeValue->containerId_,
		ALLOC_NEW(alloc) DataStoreLogV4::ContinueCreateIndex());
	ResponsMesV4* outMes = NULL;
	if (isFinished) {
		outMes = ALLOC_NEW(alloc) ResponsMesV4(alloc);
	}
	DSOutputMes* out = ALLOC_NEW(alloc) DSOutputMes(alloc, isFinished, 0, outMes, dsLog);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::ContinueAlterContainer* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	bool allowExpiration = true;
	StackAllocAutoPtr<BaseContainer> orgContainerAutoPtr(alloc, getBaseContainer(*txn, storeValue->oId_, ANY_CONTAINER, allowExpiration));
	BaseContainer* orgContainer = orgContainerAutoPtr.get();
	checkContainerExistence(orgContainer);

	ContainerCursor containerCursor = orgContainer->getContainerCursor(*txn);
	orgContainer->continueChangeSchema(*txn, containerCursor);
	bool isFinished = containerCursor.isFinished();

	DSPutContainerOutputMes* out = NULL;
	DataStoreLogV4* dsLog = ALLOC_NEW(alloc) DataStoreLogV4(alloc,
		CONTINUE_ALTER_CONTAINER, storeValue->containerId_,
		ALLOC_NEW(alloc) DataStoreLogV4::ContinueAlterContainer());
	if (isFinished) {
		StackAllocAutoPtr<BaseContainer> newContainerAutoPtr(alloc, getBaseContainer(*txn, containerCursor.getContainerOId(), ANY_CONTAINER, allowExpiration));
		BaseContainer* newContainer = newContainerAutoPtr.get();
		checkContainerExistence(newContainer);

		SchemaMessage* outMes = ALLOC_NEW(alloc) SchemaMessage(alloc);
		outMes = createSchemaMessage(*txn, newContainer);

		KeyDataStoreValue outputKeyValueStore = KeyDataStoreValue(
			newContainer->getContainerId(),
			newContainer->getBaseOId(),
			getStoreType(),
			newContainer->getAttribute());
		out = ALLOC_NEW(alloc) DSPutContainerOutputMes(alloc, PutStatus::UPDATE, isFinished,
			outputKeyValueStore, outMes, dsLog);
	}
	else {
		KeyDataStoreValue outputKeyValueStore = *storeValue;
		out = ALLOC_NEW(alloc) DSPutContainerOutputMes(alloc, PutStatus::UPDATE, isFinished,
			outputKeyValueStore, NULL, dsLog);
	}
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::PutRow* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	const uint64_t numRow = 1;
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, getBaseContainer(*txn, storeValue->oId_, ANY_CONTAINER));
	BaseContainer* container = containerAutoPtr.get();
	Event* ev=txn->getEvent();
	if ( AUDIT_TRACE_CHECK ) {
		AUDIT_TRACE_INFO_EXECUTE(2,"PUT_ROW",
			TransactionContextScope::getContainerName(*txn, container));
	}

	checkContainerSchemaVersion(container, opMes->verId_);
	RowId rowId = UNDEF_ROWID;
	PutStatus status;
	container->putRow(*txn, opMes->rowData_->size(), opMes->rowData_->data(),
		rowId, status, opMes->putRowOption_);

	DataStoreLogV4* dsLog = ALLOC_NEW(alloc) DataStoreLogV4(alloc,
		PUT_ROW, storeValue->containerId_,
		ALLOC_NEW(alloc) DataStoreLogV4::PutRow(alloc,
			(status != PutStatus::NOT_EXECUTED) ? rowId : UNDEF_ROWID, opMes->rowData_));
	RowExistMessage* existMes = ALLOC_NEW(alloc) RowExistMessage(alloc, status == PutStatus::UPDATE);
	DSOutputMes* out = ALLOC_NEW(alloc) DSOutputMes(alloc, status, numRow, existMes, dsLog);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::AppendRow* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	const uint64_t numRow = 1;
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, getBaseContainer(*txn, storeValue->oId_, TIME_SERIES_CONTAINER));
	BaseContainer* container = containerAutoPtr.get();
	Event* ev=txn->getEvent();
	if ( AUDIT_TRACE_CHECK ) {
		AUDIT_TRACE_INFO_EXECUTE(2,"APPEND_ROW",
			TransactionContextScope::getContainerName(*txn, container));
	}

	checkContainerSchemaVersion(container, opMes->verId_);
	RowId rowId = UNDEF_ROWID;
	PutStatus status;
	static_cast<TimeSeries*>(container)->appendRow(*txn, opMes->rowData_->size(), opMes->rowData_->data(),
		rowId, status);

	DataStoreLogV4* dsLog = ALLOC_NEW(alloc) DataStoreLogV4(alloc,
		PUT_ROW, storeValue->containerId_,
		ALLOC_NEW(alloc) DataStoreLogV4::PutRow(alloc,
			(status != PutStatus::NOT_EXECUTED) ? rowId : UNDEF_ROWID, opMes->rowData_));
	RowExistMessage* existMes = ALLOC_NEW(alloc) RowExistMessage(alloc, status == PutStatus::UPDATE);
	DSOutputMes* out = ALLOC_NEW(alloc) DSOutputMes(alloc, status, numRow, existMes, dsLog);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::UpdateRowById* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	const uint64_t numRow = 1;
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, getBaseContainer(*txn, storeValue->oId_, ANY_CONTAINER));
	BaseContainer* container = containerAutoPtr.get();
	Event* ev=txn->getEvent();
	if ( AUDIT_TRACE_CHECK ) {
		AUDIT_TRACE_INFO_EXECUTE(2,"UPDATE_ROW_BY_ID",
			TransactionContextScope::getContainerName(*txn, container));
	}

	checkContainerSchemaVersion(container, opMes->verId_);
	PutStatus status;
	container->updateRow(*txn, opMes->rowData_->size(), opMes->rowData_->data(),
		opMes->rowId_, status);

	DataStoreLogV4* dsLog = ALLOC_NEW(alloc) DataStoreLogV4(alloc,
		UPDATE_ROW_BY_ID, storeValue->containerId_,
		ALLOC_NEW(alloc) DataStoreLogV4::UpdateRow(alloc,
			(status != PutStatus::NOT_EXECUTED) ? opMes->rowId_ : UNDEF_ROWID, opMes->rowData_));
	ResponsMesV4* outMes = ALLOC_NEW(alloc) ResponsMesV4(alloc);
	DSOutputMes* out = ALLOC_NEW(alloc) DSOutputMes(alloc, status, numRow, outMes, dsLog);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::RemoveRow* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	const uint64_t numRow = 1;
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, getBaseContainer(*txn, storeValue->oId_, ANY_CONTAINER));
	BaseContainer* container = containerAutoPtr.get();
	Event* ev=txn->getEvent();
	if ( AUDIT_TRACE_CHECK ) {
		AUDIT_TRACE_INFO_EXECUTE(2,"REMOVE_ROW",
			TransactionContextScope::getContainerName(*txn, container));
	}

	checkContainerSchemaVersion(container, opMes->verId_);
	bool exist;
	RowId rowId = UNDEF_ROWID;
	container->deleteRow(*txn, opMes->keyData_->size(), opMes->keyData_->data(),
		rowId, exist);
	DataStoreLogV4* dsLog = ALLOC_NEW(alloc) DataStoreLogV4(alloc,
		REMOVE_ROW_BY_ID, storeValue->containerId_,
		ALLOC_NEW(alloc) DataStoreLogV4::RemoveRow(alloc, exist ? rowId : UNDEF_ROWID));
	RowExistMessage* existMes = ALLOC_NEW(alloc) RowExistMessage(alloc, exist);
	DSOutputMes* out = ALLOC_NEW(alloc) DSOutputMes(alloc, exist, numRow, existMes, dsLog);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::RemoveRowById* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	const uint64_t numRow = 1;
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, getBaseContainer(*txn, storeValue->oId_, ANY_CONTAINER));
	BaseContainer* container = containerAutoPtr.get();
	Event* ev=txn->getEvent();
	if ( AUDIT_TRACE_CHECK ) {
		AUDIT_TRACE_INFO_EXECUTE(2,"REMOVE_ROW_BY_ID",
			TransactionContextScope::getContainerName(*txn, container));
	}

	checkContainerSchemaVersion(container, opMes->verId_);
	bool exist;
	container->deleteRow(*txn, opMes->rowId_, exist);

	DataStoreLogV4* dsLog = ALLOC_NEW(alloc) DataStoreLogV4(alloc,
		REMOVE_ROW_BY_ID, storeValue->containerId_,
		ALLOC_NEW(alloc) DataStoreLogV4::RemoveRow(alloc, exist ? opMes->rowId_ : UNDEF_ROWID));
	ResponsMesV4* outMes = ALLOC_NEW(alloc) ResponsMesV4(alloc);
	DSOutputMes* out = ALLOC_NEW(alloc) DSOutputMes(alloc, exist, numRow, outMes, dsLog);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::GetRow* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, getBaseContainer(*txn, storeValue->oId_, ANY_CONTAINER));
	BaseContainer* container = containerAutoPtr.get();
	Event* ev=txn->getEvent();
	if ( AUDIT_TRACE_CHECK ) {
		AUDIT_TRACE_INFO_EXECUTE(1,"GET_ROW",
			TransactionContextScope::getContainerName(*txn, container));
	}

	checkContainerSchemaVersion(container, opMes->verId_);
	ResultSet* rs = getResultSetManager()->create(
		*txn, storeValue->containerId_, container->getVersionId(), opMes->emNow_, NULL);
	ResultSetGuard *rsGuard = ALLOC_NEW(alloc) ResultSetGuard(*txn, *this, *rs);

	util::XArray<RowId>* lockedRowId = NULL;
	bool exist;
	DataStoreLogV4* dsLog = NULL;
	{
		QueryProcessor::get(*txn, *container,
			static_cast<uint32_t>(opMes->keyData_->size()), opMes->keyData_->data(), *rs);
		if (opMes->forUpdate_) {
			lockedRowId = ALLOC_NEW(alloc) util::XArray<RowId>(alloc);
			container->getLockRowIdList(*txn, *rs, *lockedRowId);
		}
		QueryProcessor::fetch(
			*txn, *container, 0, MAX_RESULT_SIZE, rs);
		exist = (rs->getResultNum() > 0);
		if (lockedRowId != NULL) {
			dsLog = ALLOC_NEW(alloc) DataStoreLogV4(alloc,
				GET_ROW, storeValue->containerId_,
				ALLOC_NEW(alloc) DataStoreLogV4::LockRow(alloc, lockedRowId));
		}
	}
	SimpleQueryMessage* existMes = ALLOC_NEW(alloc) SimpleQueryMessage(alloc, rsGuard, rs, exist);
	DSOutputMes* out = ALLOC_NEW(alloc) DSOutputMes(alloc, exist, rs->getFetchNum(), existMes, dsLog);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::GetRowSet* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, getBaseContainer(*txn, storeValue->oId_, ANY_CONTAINER));
	BaseContainer* container = containerAutoPtr.get();
	Event* ev=txn->getEvent();
	if ( AUDIT_TRACE_CHECK ) {
		AUDIT_TRACE_INFO_EXECUTE(1,"GET_ROW_SET",
			TransactionContextScope::getContainerName(*txn, container));
	}

	checkContainerSchemaVersion(container, opMes->verId_);
	ResultSet* rs = getResultSetManager()->create(
		*txn, storeValue->containerId_, container->getVersionId(), opMes->emNow_, NULL);
	ResultSetGuard *rsGuard = ALLOC_NEW(alloc) ResultSetGuard(*txn, *this, *rs);

	RowId last = UNDEF_ROWID;
	{
		QueryProcessor::get(*txn, *container, opMes->position_, opMes->limit_,
			last, *rs);
		QueryProcessor::fetch(
			*txn, *container, 0, opMes->size_, rs);
	}
	DataStoreLogV4* dsLog = NULL; 
	ResponsMesV4* queryMes = ALLOC_NEW(alloc) ResponsMesV4(alloc);
	DSRowSetOutputMes* out = ALLOC_NEW(alloc) DSRowSetOutputMes(alloc, rsGuard, rs, last, queryMes, dsLog);
	return out;

}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::QueryTQL* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, getBaseContainer(*txn, storeValue->oId_, ANY_CONTAINER));
	BaseContainer* container = containerAutoPtr.get();
	Event* ev=txn->getEvent();
	if ( AUDIT_TRACE_CHECK ) {
		AUDIT_TRACE_INFO_EXECUTE(1,"QUERY_TQL",
			TransactionContextScope::getContainerName(*txn, container));
	}

	checkContainerSchemaVersion(container, opMes->verId_);
	ResultSet* rs = getResultSetManager()->create(
		*txn, storeValue->containerId_, container->getVersionId(), opMes->emNow_,
		opMes->rsQueryOption_);
	ResultSetGuard *rsGuard = ALLOC_NEW(alloc) ResultSetGuard(*txn, *this, *rs);

	util::XArray<RowId>* lockedRowId = NULL;
	bool exist;
	DataStoreLogV4* dsLog = NULL;
	{
		QueryProcessor::executeTQL(
			*txn, *container, opMes->limit_,
			TQLInfo(
				opMes->dbName_,
				opMes->containerKey_, opMes->query_.c_str()),
			*rs);
		if (opMes->forUpdate_) {
			lockedRowId = ALLOC_NEW(alloc) util::XArray<RowId>(alloc);
			container->getLockRowIdList(*txn, *rs, *lockedRowId);
		}
		QueryProcessor::fetch(
			*txn, *container, 0, opMes->size_, rs);
		exist = (rs->getResultNum() > 0);
		if (lockedRowId != NULL) {
			dsLog = ALLOC_NEW(alloc) DataStoreLogV4(alloc,
				GET_ROW, storeValue->containerId_,
				ALLOC_NEW(alloc) DataStoreLogV4::LockRow(alloc, lockedRowId));
		}
	}
	ResponsMesV4* queryMes = ALLOC_NEW(alloc) ResponsMesV4(alloc);
	DSTQLOutputMes* out = ALLOC_NEW(alloc) DSTQLOutputMes(alloc, rsGuard, rs, queryMes, dsLog);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::QueryGeometryWithExclusion* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, getBaseContainer(*txn, storeValue->oId_, COLLECTION_CONTAINER));
	BaseContainer* container = containerAutoPtr.get();
	Event* ev=txn->getEvent();
	if ( AUDIT_TRACE_CHECK ) {
		AUDIT_TRACE_INFO_EXECUTE(1,"QUERY_GEOMETRY_WITH_EXCLUSION",
			TransactionContextScope::getContainerName(*txn, container));
	}

	checkContainerSchemaVersion(container, opMes->verId_);
	ResultSet* rs = getResultSetManager()->create(
		*txn, storeValue->containerId_, container->getVersionId(), opMes->emNow_, NULL);
	ResultSetGuard *rsGuard = ALLOC_NEW(alloc) ResultSetGuard(*txn, *this, *rs);

	util::XArray<RowId>* lockedRowId = NULL;
	bool exist;
	DataStoreLogV4* dsLog = NULL;
	{
		QueryProcessor::searchGeometry(*txn, *static_cast<Collection*>(container),
			opMes->limit_, opMes->query_.columnId_,
			static_cast<uint32_t>(opMes->query_.intersection_.size()),
			opMes->query_.intersection_.data(),
			static_cast<uint32_t>(opMes->query_.disjoint_.size()),
			opMes->query_.disjoint_.data(),
			*rs);
		if (opMes->forUpdate_) {
			lockedRowId = ALLOC_NEW(alloc) util::XArray<RowId>(alloc);
			container->getLockRowIdList(*txn, *rs, *lockedRowId);
		}
		QueryProcessor::fetch(
			*txn, *container, 0, opMes->size_, rs);
		exist = (rs->getResultNum() > 0);
		if (lockedRowId != NULL) {
			dsLog = ALLOC_NEW(alloc) DataStoreLogV4(alloc,
				GET_ROW, storeValue->containerId_,
				ALLOC_NEW(alloc) DataStoreLogV4::LockRow(alloc, lockedRowId));
		}
	}
	ResponsMesV4* queryMes = ALLOC_NEW(alloc) ResponsMesV4(alloc);
	DSTQLOutputMes* out = ALLOC_NEW(alloc) DSTQLOutputMes(alloc, rsGuard, rs, queryMes, dsLog);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::QueryGeometryRelated* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, getBaseContainer(*txn, storeValue->oId_, COLLECTION_CONTAINER));
	BaseContainer* container = containerAutoPtr.get();
	Event* ev=txn->getEvent();
	if ( AUDIT_TRACE_CHECK ) {
		AUDIT_TRACE_INFO_EXECUTE(1,"QUERY_GEOMETRY_RELATED",
			TransactionContextScope::getContainerName(*txn, container));
	}

	checkContainerSchemaVersion(container, opMes->verId_);
	ResultSet* rs = getResultSetManager()->create(
		*txn, storeValue->containerId_, container->getVersionId(), opMes->emNow_, NULL);
	ResultSetGuard *rsGuard = ALLOC_NEW(alloc) ResultSetGuard(*txn, *this, *rs);

	util::XArray<RowId>* lockedRowId = NULL;
	bool exist;
	DataStoreLogV4* dsLog = NULL;
	{
		QueryProcessor::searchGeometryRelated(*txn, *static_cast<Collection*>(container),
			opMes->limit_, opMes->query_.columnId_,
			static_cast<uint32_t>(opMes->query_.intersection_.size()),
			opMes->query_.intersection_.data(),
			opMes->query_.operator_, *rs);
		if (opMes->forUpdate_) {
			lockedRowId = ALLOC_NEW(alloc) util::XArray<RowId>(alloc);
			container->getLockRowIdList(*txn, *rs, *lockedRowId);
		}
		QueryProcessor::fetch(
			*txn, *container, 0, opMes->size_, rs);
		exist = (rs->getResultNum() > 0);
		if (lockedRowId != NULL) {
			dsLog = ALLOC_NEW(alloc) DataStoreLogV4(alloc,
				GET_ROW, storeValue->containerId_,
				ALLOC_NEW(alloc) DataStoreLogV4::LockRow(alloc, lockedRowId));
		}
	}
	ResponsMesV4* queryMes = ALLOC_NEW(alloc) ResponsMesV4(alloc);
	DSTQLOutputMes* out = ALLOC_NEW(alloc) DSTQLOutputMes(alloc, rsGuard, rs, queryMes, dsLog);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::GetRowTimeRelated* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, getBaseContainer(*txn, storeValue->oId_, TIME_SERIES_CONTAINER));
	BaseContainer* container = containerAutoPtr.get();
	Event* ev=txn->getEvent();
	if ( AUDIT_TRACE_CHECK ) {
		AUDIT_TRACE_INFO_EXECUTE(1,"GET_TIME_RELATED",
			TransactionContextScope::getContainerName(*txn, container));
	}

	checkContainerSchemaVersion(container, opMes->verId_);
	ResultSet* rs = getResultSetManager()->create(
		*txn, storeValue->containerId_, container->getVersionId(), opMes->emNow_, NULL);
	ResultSetGuard *rsGuard = ALLOC_NEW(alloc) ResultSetGuard(*txn, *this, *rs);

	bool exist;
	{
		QueryProcessor::get(*txn, *static_cast<TimeSeries*>(container),
			opMes->condition_.rowKey_, opMes->condition_.operator_, *rs);
		QueryProcessor::fetch(
			*txn, *container, 0, MAX_RESULT_SIZE, rs);
		exist = (rs->getResultNum() > 0);
	}
	DataStoreLogV4* dsLog = NULL;
	SimpleQueryMessage* existMes = ALLOC_NEW(alloc) SimpleQueryMessage(alloc, rsGuard, rs, exist);
	DSOutputMes* out = ALLOC_NEW(alloc) DSOutputMes(alloc, exist, rs->getFetchNum(), existMes, dsLog);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::QueryInterPolate* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, getBaseContainer(*txn, storeValue->oId_, TIME_SERIES_CONTAINER));
	BaseContainer* container = containerAutoPtr.get();
	Event* ev=txn->getEvent();
	if ( AUDIT_TRACE_CHECK ) {
		AUDIT_TRACE_INFO_EXECUTE(1,"GET_INTERPOLATE",
			TransactionContextScope::getContainerName(*txn, container));
	}

	checkContainerSchemaVersion(container, opMes->verId_);
	ResultSet* rs = getResultSetManager()->create(
		*txn, storeValue->containerId_, container->getVersionId(), opMes->emNow_, NULL);
	ResultSetGuard *rsGuard = ALLOC_NEW(alloc) ResultSetGuard(*txn, *this, *rs);

	bool exist;
	{
		QueryProcessor::interpolate(*txn, *static_cast<TimeSeries*>(container), opMes->condition_.rowKey_,
			opMes->condition_.columnId_, *rs);

		QueryProcessor::fetch(
			*txn, *container, 0, MAX_RESULT_SIZE, rs);
		exist = (rs->getResultNum() > 0);
	}
	DataStoreLogV4* dsLog = NULL;
	SimpleQueryMessage* existMes = ALLOC_NEW(alloc) SimpleQueryMessage(alloc, rsGuard, rs, exist);
	DSOutputMes* out = ALLOC_NEW(alloc) DSOutputMes(alloc, exist, rs->getFetchNum(), existMes, dsLog);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::QueryAggregate* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, getBaseContainer(*txn, storeValue->oId_, TIME_SERIES_CONTAINER));
	BaseContainer* container = containerAutoPtr.get();
	Event* ev=txn->getEvent();
	if ( AUDIT_TRACE_CHECK ) {
		AUDIT_TRACE_INFO_EXECUTE(1,"QUERY_AGGREGATE",
			TransactionContextScope::getContainerName(*txn, container));
	}

	checkContainerSchemaVersion(container, opMes->verId_);
	ResultSet* rs = getResultSetManager()->create(
		*txn, storeValue->containerId_, container->getVersionId(), opMes->emNow_, NULL);
	ResultSetGuard *rsGuard = ALLOC_NEW(alloc) ResultSetGuard(*txn, *this, *rs);

	bool exist;
	{
		QueryProcessor::aggregate(*txn, *static_cast<TimeSeries*>(container), opMes->query_.start_, opMes->query_.end_,
			opMes->query_.columnId_, opMes->query_.aggregationType_, *rs);

		QueryProcessor::fetch(
			*txn, *container, 0, MAX_RESULT_SIZE, rs);
		exist = (rs->getResultNum() > 0);
	}
	DataStoreLogV4* dsLog = NULL;
	SimpleQueryMessage* existMes = ALLOC_NEW(alloc) SimpleQueryMessage(alloc, rsGuard, rs, exist);
	DSOutputMes* out = ALLOC_NEW(alloc) DSOutputMes(alloc, exist, rs->getFetchNum(), existMes, dsLog);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::QueryTimeRange* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, getBaseContainer(*txn, storeValue->oId_, TIME_SERIES_CONTAINER));
	BaseContainer* container = containerAutoPtr.get();
	Event* ev=txn->getEvent();
	if ( AUDIT_TRACE_CHECK ) {
		AUDIT_TRACE_INFO_EXECUTE(1,"QUERY_TIME_RANGE",
			TransactionContextScope::getContainerName(*txn, container));
	}

	checkContainerSchemaVersion(container, opMes->verId_);
	ResultSet* rs = getResultSetManager()->create(
		*txn, storeValue->containerId_, container->getVersionId(), opMes->emNow_, NULL);
	ResultSetGuard *rsGuard = ALLOC_NEW(alloc) ResultSetGuard(*txn, *this, *rs);

	util::XArray<RowId>* lockedRowId = NULL;
	DataStoreLogV4* dsLog = NULL;
	bool exist;
	{
		QueryProcessor::search(*txn, *static_cast<TimeSeries*>(container), opMes->query_.order_,
			opMes->limit_, opMes->query_.start_, opMes->query_.end_, *rs);
		if (opMes->forUpdate_) {
			lockedRowId = ALLOC_NEW(alloc) util::XArray<RowId>(alloc);
			container->getLockRowIdList(*txn, *rs, *lockedRowId);
		}
		QueryProcessor::fetch(
			*txn, *container, 0, opMes->size_, rs);
		exist = (rs->getResultNum() > 0);
		if (lockedRowId != NULL) {
			dsLog = ALLOC_NEW(alloc) DataStoreLogV4(alloc,
				GET_ROW, storeValue->containerId_,
				ALLOC_NEW(alloc) DataStoreLogV4::LockRow(alloc, lockedRowId));
		}
	}
	ResponsMesV4* queryMes = ALLOC_NEW(alloc) ResponsMesV4(alloc);
	DSTQLOutputMes* out = ALLOC_NEW(alloc) DSTQLOutputMes(alloc, rsGuard, rs, queryMes, dsLog);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::QueryTimeSampling* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, getBaseContainer(*txn, storeValue->oId_, TIME_SERIES_CONTAINER));
	BaseContainer* container = containerAutoPtr.get();
	Event* ev=txn->getEvent();
	if ( AUDIT_TRACE_CHECK ) {
		AUDIT_TRACE_INFO_EXECUTE(1,"QUERY_SAMPLE",
			TransactionContextScope::getContainerName(*txn, container));
	}

	checkContainerSchemaVersion(container, opMes->verId_);
	ResultSet* rs = getResultSetManager()->create(
		*txn, storeValue->containerId_, container->getVersionId(), opMes->emNow_, NULL);
	ResultSetGuard *rsGuard = ALLOC_NEW(alloc) ResultSetGuard(*txn, *this, *rs);

	util::XArray<RowId>* lockedRowId = NULL;
	bool exist;
	DataStoreLogV4* dsLog = NULL;
	{
		QueryProcessor::sample(*txn, *static_cast<TimeSeries*>(container), opMes->limit_,
			opMes->query_.start_, opMes->query_.end_, opMes->query_.toSamplingOption(), *rs);
		if (opMes->forUpdate_) {
			lockedRowId = ALLOC_NEW(alloc) util::XArray<RowId>(alloc);
			container->getLockRowIdList(*txn, *rs, *lockedRowId);
		}
		QueryProcessor::fetch(
			*txn, *container, 0, opMes->size_, rs);
		exist = (rs->getResultNum() > 0);
		if (lockedRowId != NULL) {
			dsLog = ALLOC_NEW(alloc) DataStoreLogV4(alloc,
				GET_ROW, storeValue->containerId_,
				ALLOC_NEW(alloc) DataStoreLogV4::LockRow(alloc, lockedRowId));
		}
	}
	ResponsMesV4* queryMes = ALLOC_NEW(alloc) ResponsMesV4(alloc);
	DSTQLOutputMes* out = ALLOC_NEW(alloc) DSTQLOutputMes(alloc, rsGuard, rs, queryMes, dsLog);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::QueryFetchResultSet* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, getBaseContainer(*txn, storeValue->oId_, ANY_CONTAINER));
	BaseContainer* container = containerAutoPtr.get();

	checkContainerSchemaVersion(container, opMes->verId_);

	ResultSet* rs = getResultSetManager()->get(*txn, opMes->rsId_);
	if (rs == NULL) {
		GS_THROW_USER_ERROR(
			GS_ERROR_DS_DS_RESULT_ID_INVALID, "(rsId=" << opMes->rsId_ << ")");
	}
	ResultSetGuard *rsGuard = ALLOC_NEW(alloc) ResultSetGuard(*txn, *this, *rs);

	bool exist;
	{
		QueryProcessor::fetch(
			*txn, *container, opMes->startPos_, opMes->fetchNum_, rs);
		exist = (rs->getResultNum() > 0);
	}
	DataStoreLogV4* dsLog = NULL;
	ResponsMesV4* queryMes = ALLOC_NEW(alloc) ResponsMesV4(alloc);
	DSTQLOutputMes* out = ALLOC_NEW(alloc) DSTQLOutputMes(alloc, rsGuard, rs, queryMes, dsLog);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::QueryCloseResultSet* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	ResultSet* rs = getResultSetManager()->get(*txn, opMes->rsId_);
	getResultSetManager()->close(opMes->rsId_);
	DataStoreLogV4* dsLog = NULL;
	ResponsMesV4* queryMes = ALLOC_NEW(alloc) ResponsMesV4(alloc);
	DSOutputMes* out = ALLOC_NEW(alloc) DSOutputMes(alloc, true, 0, queryMes, dsLog);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::GetContainerObject* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	BaseContainer* container = NULL;
	if (storeValue->oId_ != UNDEF_OID) {
		container = getBaseContainer(*txn, storeValue->oId_, opMes->containerType_, opMes->allowExpiration_);
	}
	Event* ev=txn->getEvent();
	if ( AUDIT_TRACE_CHECK ) {
		AUDIT_TRACE_INFO_EXECUTE(1,"GET_CONTAINER_OBJECT",
			TransactionContextScope::getContainerName(*txn, container));
	}
	DSContainerOutputMes* out = ALLOC_NEW(alloc) DSContainerOutputMes(alloc, container);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::ScanChunkGroup* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	uint64_t scanNum = scanChunkGroup(*txn, opMes->currentTime_);
	DSScanChunkGroupOutputMes* out = ALLOC_NEW(alloc) DSScanChunkGroupOutputMes(alloc, scanNum);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::SearchBackgroundTask* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	BGTask bgTask = searchBGTask(*txn);
	DSBackgroundTaskOutputMes* out = ALLOC_NEW(alloc) DSBackgroundTaskOutputMes(alloc, bgTask);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::ExecuteBackgroundTask* opMes) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	bool isFinish = executeBGTask(*txn, opMes->bgTask_);
	BGTask nextTask = isFinish ? BGTask() : opMes->bgTask_;
	DSBackgroundTaskOutputMes* out = ALLOC_NEW(alloc) DSBackgroundTaskOutputMes(alloc, nextTask);
	return out;
}

Serializable* DataStoreV4::execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::CheckTimeoutResultSet* opMes) {
	getResultSetManager()->checkTimeout(opMes->currentTime_);
	return NULL;
}

TransactionContext& DataStoreV4::getTxn(util::StackAllocator& alloc, TxnLog& txnLog, const util::DateTime& redoStartTime,
	Timestamp redoStartEmTime) {
	PartitionId pId = chunkManager_->getPId();
	const bool REDO = true;
	util::TimeZone timeZone;

	GSEventType eventType = txnLog.dsLog_->type_;
	TransactionManager::GetMode getMode;
	TransactionManager::TransactionMode txnMode;
	bool withBegin = (txnLog.txnMode_ ==
					TransactionManager::NO_AUTO_COMMIT_BEGIN);
	bool isAutoCommit = (txnLog.txnMode_ == TransactionManager::AUTO_COMMIT);
	switch (eventType) {
	case PUT_CONTAINER:
	case CONTINUE_ALTER_CONTAINER:
	case CONTINUE_CREATE_INDEX:
	case CREATE_INDEX:
	case PUT_ROW:
	case UPDATE_ROW_BY_ID:
	case REMOVE_ROW_BY_ID:
	case GET_ROW:
		getMode = txnMgr_->getContextGetModeForRecovery(
			static_cast<TransactionManager::GetMode>(
				txnLog.getMode_));
		txnMode = txnMgr_->getTransactionModeForRecovery(
			withBegin, isAutoCommit);
		break;
	case COMMIT_TRANSACTION:
	case ABORT_TRANSACTION:
		getMode = TransactionManager::PUT;
		txnMode = TransactionManager::NO_AUTO_COMMIT_BEGIN_OR_CONTINUE;
		break;
	default:
		getMode = TransactionManager::AUTO;
		txnMode = TransactionManager::AUTO_COMMIT;
		break;
	}
		
	const TransactionManager::ContextSource src(eventType,
		txnLog.stmtId_, txnLog.dsLog_->containerId_,
		txnLog.txnTimeoutInterval_,
		getMode,
		txnMode,
		false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE, timeZone);

	if (isTimeoutEvent(eventType)) {
		TransactionContext& txn =
			txnMgr_->putNoExpire(alloc, pId, txnLog.clientId_, src,
				redoStartTime, redoStartEmTime, REDO, txnLog.txnId_);
		assert(txn.getId() == txnLog.txnId_ || txnLog.txnId_ == UNDEF_TXNID);
		assert(txn.getContainerId() == txnLog.dsLog_->containerId_);
		return txn;
	}
	else {
		TransactionContext& txn =
			txnMgr_->put(alloc, pId, txnLog.clientId_, src,
				redoStartTime, redoStartEmTime, REDO, txnLog.txnId_);
		if ((eventType != DROP_CONTAINER) && (eventType != DELETE_INDEX)) {
			assert(txn.getId() == txnLog.txnId_);
			assert(txn.getContainerId() == txnLog.dsLog_->containerId_);
		}
		return txn;
	}
}

bool DataStoreV4::isTimeoutEvent(GSEventType eventType) {
	bool isTimeout = false;
	switch (eventType) {
	case PUT_CONTAINER:
	case CREATE_INDEX:
		isTimeout = true;
		break;
	default:
		break;
	}
	return isTimeout;
}

void DataStoreV4::redo(util::StackAllocator& alloc, RedoMode mode, const util::DateTime& redoStartTime,
	Timestamp redoStartEmTime, Serializable* message) {
	util::ArrayByteInStream in = util::ArrayByteInStream(
		util::ArrayInStream(message->data_, message->size_));

	TxnLog txnLog(alloc);
	txnLog.decode<util::ArrayByteInStream>(in);
	GSEventType eventType = txnLog.dsLog_->type_;
	TransactionContext& txn = getTxn(alloc, txnLog, redoStartTime, redoStartEmTime);
	const DataStoreBase::Scope dsScope(&txn, this, clusterService_);

	switch (eventType) {
	case PUT_CONTAINER: {
		redoEvent(txn, txnLog.dsLog_, txnLog.dsLog_->value_.putContainer_);
	} break;
	case DROP_CONTAINER: {
		redoEvent(txn, txnLog.dsLog_, txnLog.dsLog_->value_.dropContainer_);
	} break;
	case COMMIT_TRANSACTION: {
		redoEvent(txn, txnLog.dsLog_, txnLog.dsLog_->value_.commit_);
	} break;
	case ABORT_TRANSACTION: {
		redoEvent(txn, txnLog.dsLog_, txnLog.dsLog_->value_.abort_);
	} break;
	case CREATE_INDEX: {
		redoEvent(txn, txnLog.dsLog_, txnLog.dsLog_->value_.createIndex_);
	} break;
	case DELETE_INDEX: {
		redoEvent(txn, txnLog.dsLog_, txnLog.dsLog_->value_.dropIndex_);
	} break;
	case CONTINUE_CREATE_INDEX: {
		redoEvent(txn, txnLog.dsLog_, txnLog.dsLog_->value_.continueCreateIndex_);
	} break;
	case CONTINUE_ALTER_CONTAINER: {
		redoEvent(txn, txnLog.dsLog_, txnLog.dsLog_->value_.continueAlterContainer_);
	} break;
	case PUT_ROW: {
		redoEvent(txn, txnLog.dsLog_, txnLog.dsLog_->value_.putRow_);
	} break;
	case UPDATE_ROW_BY_ID: {
		redoEvent(txn, txnLog.dsLog_, txnLog.dsLog_->value_.updateRow_);
	} break;
	case REMOVE_ROW_BY_ID: {
		redoEvent(txn, txnLog.dsLog_, txnLog.dsLog_->value_.removeRow_);
	} break;
	case GET_ROW: {
		redoEvent(txn, txnLog.dsLog_, txnLog.dsLog_->value_.lockRow_);
	} break;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
		break;
	}
	txnMgr_->update(txn, txnLog.stmtId_);

	removeTransaction(mode, txn);
}



void DataStoreV4::redoEvent(TransactionContext& txn, DataStoreLogV4* dsLog, DataStoreLogV4::PutContainer* opeLog) {
	util::StackAllocator& alloc = txn.getDefaultAllocator();
	bool isAllowExpiration = true;
	bool isCaseSensitive = false;
	KeyDataStoreValue keyStoreValue = keyStore_->get(txn, opeLog->getContainerKey(), isCaseSensitive);

	PutStatus dummy;
	const bool isModifiable = true;
	BaseContainer* container =
		putContainer(txn, opeLog->getContainerKey(), opeLog->type_,
			opeLog->info_->size(), opeLog->info_->data(),
			isModifiable, MessageSchema::V4_1_VERSION, dummy,
			isCaseSensitive, keyStoreValue);

	StackAllocAutoPtr<BaseContainer> containerAutoPtr(alloc, container);

	checkContainerExistence(container);
	if (container->getContainerId() != dsLog->containerId_ ||
		txn.getContainerId() != dsLog->containerId_) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_DS_CONTAINER_ID_INVALID,
			"log containerId : " << dsLog->containerId_
			<< ", redo containerId : "
			<< container->getContainerId());
	}
}

void DataStoreV4::redoEvent(TransactionContext& txn, DataStoreLogV4* dsLog, DataStoreLogV4::DropContainer* opeLog) {
	bool isCaseSensitive = false;
	KeyDataStoreValue keyStoreValue = keyStore_->get(txn, opeLog->getContainerKey(), isCaseSensitive);
	dropContainer(txn, opeLog->type_, false, keyStoreValue);
}

void DataStoreV4::redoEvent(TransactionContext& txn, DataStoreLogV4* dsLog, DataStoreLogV4::Commit* opeLog) {
	bool isAllowExpiration = true;
	KeyDataStoreValue keyStoreValue = keyStore_->get(txn.getDefaultAllocator(), txn.getContainerId());
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(txn.getDefaultAllocator(), getBaseContainer(txn, keyStoreValue.oId_, ANY_CONTAINER, isAllowExpiration));
	checkContainerExistence(containerAutoPtr.get());
	txnMgr_->commit(txn, *containerAutoPtr.get());
}

void DataStoreV4::redoEvent(TransactionContext& txn, DataStoreLogV4* dsLog, DataStoreLogV4::Abort* opeLog) {
	bool isAllowExpiration = true;
	KeyDataStoreValue keyStoreValue = keyStore_->get(txn.getDefaultAllocator(), txn.getContainerId());
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(txn.getDefaultAllocator(), getBaseContainer(txn, keyStoreValue.oId_, ANY_CONTAINER, isAllowExpiration));
	checkContainerExistence(containerAutoPtr.get());
	txnMgr_->abort(txn, *containerAutoPtr.get());
}

void DataStoreV4::redoEvent(TransactionContext& txn, DataStoreLogV4* dsLog, DataStoreLogV4::CreateIndex* opeLog) {
	bool isAllowExpiration = true;
	KeyDataStoreValue keyStoreValue = keyStore_->get(txn.getDefaultAllocator(), txn.getContainerId());
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(txn.getDefaultAllocator(), getBaseContainer(txn, keyStoreValue.oId_, ANY_CONTAINER, isAllowExpiration));
	checkContainerExistence(containerAutoPtr.get());
	IndexCursor indexCursor;
	containerAutoPtr.get()->createIndex(txn, *(opeLog->info_), indexCursor);
}

void DataStoreV4::redoEvent(TransactionContext& txn, DataStoreLogV4* dsLog, DataStoreLogV4::DropIndex* opeLog) {
	bool isAllowExpiration = true;
	KeyDataStoreValue keyStoreValue = keyStore_->get(txn.getDefaultAllocator(), txn.getContainerId());
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(txn.getDefaultAllocator(), getBaseContainer(txn, keyStoreValue.oId_, ANY_CONTAINER, isAllowExpiration));
	checkContainerExistence(containerAutoPtr.get());
	containerAutoPtr.get()->dropIndex(txn, *(opeLog->info_));
}
void DataStoreV4::redoEvent(TransactionContext& txn, DataStoreLogV4* dsLog, DataStoreLogV4::ContinueCreateIndex* opeLog) {
	bool isAllowExpiration = true;
	KeyDataStoreValue keyStoreValue = keyStore_->get(txn.getDefaultAllocator(), txn.getContainerId());
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(txn.getDefaultAllocator(), getBaseContainer(txn, keyStoreValue.oId_, ANY_CONTAINER, isAllowExpiration));
	checkContainerExistence(containerAutoPtr.get());
	IndexCursor indexCursor = containerAutoPtr.get()->getIndexCursor(txn);
	containerAutoPtr.get()->continueCreateIndex(txn, indexCursor);
}
void DataStoreV4::redoEvent(TransactionContext& txn, DataStoreLogV4* dsLog, DataStoreLogV4::ContinueAlterContainer* opeLog) {
	bool isAllowExpiration = true;
	KeyDataStoreValue keyStoreValue = keyStore_->get(txn.getDefaultAllocator(), txn.getContainerId());
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(txn.getDefaultAllocator(), getBaseContainer(txn, keyStoreValue.oId_, ANY_CONTAINER, isAllowExpiration));
	BaseContainer* container = containerAutoPtr.get();
	checkContainerExistence(container);
	ContainerCursor containerCursor(txn.isAutoCommit());
	containerCursor = container->getContainerCursor(txn);
	container->continueChangeSchema(txn, containerCursor);
}

void DataStoreV4::redoEvent(TransactionContext& txn, DataStoreLogV4* dsLog, DataStoreLogV4::PutRow* opeLog) {
	bool isAllowExpiration = true;
	KeyDataStoreValue keyStoreValue = keyStore_->get(txn.getDefaultAllocator(), txn.getContainerId());
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(txn.getDefaultAllocator(), getBaseContainer(txn, keyStoreValue.oId_, ANY_CONTAINER, isAllowExpiration));
	BaseContainer* container = containerAutoPtr.get();
	checkContainerExistence(container);
	if (opeLog->rowId_ != UNDEF_ROWID) {
		PutStatus status;
		container->redoPutRow(txn, opeLog->rowData_->size(), opeLog->rowData_->data(), opeLog->rowId_, status, PUT_INSERT_OR_UPDATE);
		if (status == PutStatus::NOT_EXECUTED) {
			GS_TRACE_INFO(
				DATA_STORE, GS_ERROR_DS_BACKGROUND_TASK_INVALID,
				"(pId=" << txn.getPartitionId()
				<< ", logType = PUT_ROW"
				<< ", containerId = " << container->getContainerId()
				<< ", containerType =" << (int32_t)container->getContainerType()
				<< ", log rowId = " << opeLog->rowId_
				<< ", put row not executed"
			);
		}
	}
}

void DataStoreV4::redoEvent(TransactionContext& txn, DataStoreLogV4* dsLog, DataStoreLogV4::UpdateRow* opeLog) {
	bool isAllowExpiration = true;
	KeyDataStoreValue keyStoreValue = keyStore_->get(txn.getDefaultAllocator(), txn.getContainerId());
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(txn.getDefaultAllocator(), getBaseContainer(txn, keyStoreValue.oId_, ANY_CONTAINER, isAllowExpiration));
	BaseContainer* container = containerAutoPtr.get();
	checkContainerExistence(container);
	if (opeLog->rowId_ != UNDEF_ROWID) {
		PutStatus status;
		container->updateRow(txn, opeLog->rowData_->size(), opeLog->rowData_->data(), opeLog->rowId_, status);
		if (status != PutStatus::UPDATE) {
			GS_TRACE_INFO(
				DATA_STORE, GS_ERROR_DS_BACKGROUND_TASK_INVALID,
				"(pId=" << txn.getPartitionId()
				<< ", logType = UPDATE_ROW_BY_ID"
				<< ", containerId = " << container->getContainerId()
				<< ", containerType =" << (int32_t)container->getContainerType()
				<< ", log rowId = " << opeLog->rowId_
				<< ", update row not executed"
			);
		}
	}
}
void DataStoreV4::redoEvent(TransactionContext& txn, DataStoreLogV4* dsLog, DataStoreLogV4::RemoveRow* opeLog) {
	bool isAllowExpiration = true;
	KeyDataStoreValue keyStoreValue = keyStore_->get(txn.getDefaultAllocator(), txn.getContainerId());
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(txn.getDefaultAllocator(), getBaseContainer(txn, keyStoreValue.oId_, ANY_CONTAINER, isAllowExpiration));
	BaseContainer* container = containerAutoPtr.get();
	checkContainerExistence(container);
	if (opeLog->rowId_ != UNDEF_ROWID) {
		bool dummy;
		container->redoDeleteRow(txn, opeLog->rowId_, dummy);
		if (!dummy) {
			GS_TRACE_INFO(
				DATA_STORE, GS_ERROR_DS_BACKGROUND_TASK_INVALID,
				"(pId=" << txn.getPartitionId()
				<< ", logType = REMOVE_ROW_BY_ID"
				<< ", containerId = " << container->getContainerId()
				<< ", containerType =" << (int32_t)container->getContainerType()
				<< ", log rowId = " << opeLog->rowId_
				<< ", remove row not executed"
			);
		}
	}
}

void DataStoreV4::redoEvent(TransactionContext& txn, DataStoreLogV4* dsLog, DataStoreLogV4::LockRow* opeLog) {
	bool isAllowExpiration = true;
	KeyDataStoreValue keyStoreValue = keyStore_->get(txn.getDefaultAllocator(), txn.getContainerId());
	StackAllocAutoPtr<BaseContainer> containerAutoPtr(txn.getDefaultAllocator(), getBaseContainer(txn, keyStoreValue.oId_, ANY_CONTAINER, isAllowExpiration));
	checkContainerExistence(containerAutoPtr.get());
	containerAutoPtr.get()->lockRowList(txn, *(opeLog->rowIds_));
}

/*!
	@brief Removes transactions for after redoing.
*/
void DataStoreV4::removeTransaction(RedoMode mode, TransactionContext& txn) {
	switch (mode) {
	case REDO_MODE_REPLICATION:
	case REDO_MODE_SHORT_TERM_SYNC:
		break;

	case REDO_MODE_LONG_TERM_SYNC:
	case REDO_MODE_RECOVERY:
		if (!txn.isActive()) {
			txnMgr_->remove(txn.getPartitionId(), txn.getClientId());
		}
		break;

	default:
		GS_THROW_USER_ERROR(
			GS_ERROR_RM_REDO_MODE_INVALID, "(mode=" << mode << ")");
	}
}

OId DataStoreV4::getHeadOId(DSGroupId groupId) {
	ChunkId headChunkId = objectManager_->getHeadChunkId(groupId);
	OId partitionHeaderOId = objectManager_->getOId(groupId, headChunkId, FIRST_OBJECT_OFFSET);
	return partitionHeaderOId;
}

PartitionId DataStoreV4::getPId() {
	return objectManager_->getPId();
}


SchemaMessage* DataStoreV4::createSchemaMessage(TransactionContext&txn, BaseContainer *container) {
	util::StackAllocator& alloc = txn.getDefaultAllocator();
	SchemaMessage* outMes = ALLOC_NEW(alloc) SchemaMessage(alloc);
	outMes->schemaVersionId_ = container->getVersionId();
	outMes->containerId_ = container->getContainerId();
	const FullContainerKey containerKey =
		container->getContainerKey(txn);
	const void* keyBinary;
	size_t keySize;
	containerKey.toBinary(keyBinary, keySize);
	outMes->containerKeyBin_.assign(
		static_cast<const uint8_t*>(keyBinary),
		static_cast<const uint8_t*>(keyBinary) + keySize);

	const bool optionIncluded = true;
	bool internalOptionIncluded = true;
	container->getContainerInfo(txn, outMes->containerSchemaBin_, optionIncluded, internalOptionIncluded);
	return outMes;
}

bool DataStoreV4::support(Support type) {
	bool isSupport = false;
	switch(type) {
	case Support::TRIGGER:
		isSupport = false;
		break;
	default:
		assert(false);
		break;
	}
	return isSupport;
}

void DataStoreV4::preProcess(TransactionContext* txn, ClusterService* clusterService) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	if (latch_ != NULL) {
		ALLOC_DELETE(alloc, latch_);
		latch_ = NULL;
	}
	latch_ = ALLOC_NEW(alloc) Latch(*txn, this, clusterService);
}

void DataStoreV4::postProcess(TransactionContext* txn) {
	util::StackAllocator& alloc = txn->getDefaultAllocator();
	if (latch_ != NULL) {
		ALLOC_DELETE(alloc, latch_);
		latch_ = NULL;
	}
}

void DataStoreV4::checkContainerExistence(BaseContainer* container) {
	if (container == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_CONTAINER_NOT_FOUND, "");
	}
}

void DataStoreV4::checkContainerSchemaVersion(
	BaseContainer* container, SchemaVersionId schemaVersionId) {
	checkContainerExistence(container);
	if (schemaVersionId != MAX_SCHEMAVERSIONID && container->getVersionId() != schemaVersionId) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_CONTAINER_SCHEMA_UNMATCH,
			"(containerId=" << container->getContainerId()
			<< ", expectedVer=" << schemaVersionId
			<< ", actualVer=" << container->getVersionId()
			<< ")");
	}
}

DSGroupId DataStoreV4::getGroupId(DatabaseId dbId, const util::String& affinityStr, ChunkKey chunkKey) {
	const int32_t groupNum = 2;

	int32_t affinityId = calcAffinityGroupId(affinityStr);
	if (strcmp(affinityStr.c_str(), UNIQUE_GROUP_ID_KEY) == 0) {
		DSGroupId groupId = keyStore_->allocateGroupId(groupNum);
		groupKeyMap_.insert(std::make_pair(GroupKey(dbId, groupId, affinityId, chunkKey), 1));
		return groupId;
	}

	GroupKey groupKey(dbId, UNDEF_DS_GROUPID, affinityId, chunkKey);
	std::map<GroupKey, int64_t>::iterator itr = groupKeyMap_.upper_bound(groupKey);
	if (itr != groupKeyMap_.end() && 
		itr->first.isMatch(groupKey.dbId_, groupKey.affinityId_, groupKey.chunkKey_)) {
		itr->second++;
		return itr->first.groupId_;
	}
	else {
		DSGroupId groupId = keyStore_->allocateGroupId(groupNum);
		groupKeyMap_.insert(std::make_pair(GroupKey(dbId, groupId, affinityId, chunkKey), 1));
		return groupId;
	}
}

bool DataStoreV4::removeGroupId(DatabaseId dbId, const util::String& affinityStr, ChunkKey chunkKey, DSGroupId groupId) {
	int32_t affinityId = calcAffinityGroupId(affinityStr);

	bool isGroupEmpty = false;
	GroupKey groupKey(dbId, groupId, affinityId, chunkKey);
	std::map<GroupKey, int64_t>::iterator itr = groupKeyMap_.find(groupKey);
	if (itr != groupKeyMap_.end()) {
		itr->second--;
		if (itr->second == 0) {
			isGroupEmpty = true;
		}
	}
	else {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	return isGroupEmpty;
}


const char* DataStoreV4::AuditEventType(DSInputMes *input)
{
	switch (input->type_) {
	case DS_COMMIT:
		return "COMMIT";
	case DS_ABORT:
		return "ABORT";
	case DS_PUT_CONTAINER:
		return "PUT_CONTAINER";
	case DS_UPDATE_TABLE_PARTITIONING_ID:
		return "";
	case DS_DROP_CONTAINER:
		return "DROP_CONTAINER";
	case DS_GET_CONTAINER:
		return "GET_CONTAINER";
	case DS_CREATE_INDEX:
		return "CREATE_INDEX";
	case DS_DROP_INDEX:
		return "DROP_INDEX";
	case DS_CONTINUE_CREATE_INDEX:
	case DS_CONTINUE_ALTER_CONTAINER:
		return "";
	case DS_PUT_ROW:
		return "PUT_ROW";
	case DS_APPEND_ROW:
		return "APPEND_ROW";
	case DS_UPDATE_ROW_BY_ID:
		return "UPDATE_ROW_BY_ID";
	case DS_REMOVE_ROW:
		return "REMOVE_ROW";
	case DS_REMOVE_ROW_BY_ID:
		return "REMOVE_ROW_BY_ID";
	case DS_GET_ROW:
		return "GET_ROW";
	case DS_GET_ROW_SET:
		return "GET_ROW_SET";
	case DS_QUERY_TQL:
		return "QUERY_TQL";
	case DS_QUERY_GEOMETRY_WITH_EXCLUSION:
		return "QUERY_GEOMETRY_WITH_EXCLUSION";
	case DS_QUERY_GEOMETRY_RELATED:
		return "QUERY_GEOMETRY_RELATED";
	case DS_GET_TIME_RELATED:
		return "GET_TIME_RELATED";
	case DS_GET_INTERPOLATE:
		return "GET_INTERPOLATE";
	case DS_QUERY_AGGREGATE:
		return "QUERY_AGGREGATE";
	case DS_QUERY_TIME_RANGE:
		return "QUERY_TIME_RANGE";
	case DS_QUERY_SAMPLE:
		return "QUERY_SAMPLE";
	case DS_QUERY_FETCH_RESULT_SET:
	case DS_QUERY_CLOSE_RESULT_SET:
		return "";
	case DS_GET_CONTAINER_OBJECT:
		return "GET_CONTAINER_OBJECT";
	case DS_SCAN_CHUNK_GROUP:
	case DS_SEARCH_BACKGROUND_TASK:
	case DS_EXECUTE_BACKGROUND_TASK:
	case DS_CHECK_TIMEOUT_RESULT_SET:
		return "";
	}
	return "";
}
const int32_t DataStoreV4::AuditCategoryType(DSInputMes *input)
{
	switch (input->type_) {
	case DS_COMMIT:
		return 2;
	case DS_ABORT:
		return 2;
	case DS_PUT_CONTAINER:
		return 2;
	case DS_UPDATE_TABLE_PARTITIONING_ID:
		return 0;
	case DS_DROP_CONTAINER:
		return 2;
	case DS_GET_CONTAINER:
		return 1;
	case DS_CREATE_INDEX:
		return 2;
	case DS_DROP_INDEX:
		return 2;
	case DS_CONTINUE_CREATE_INDEX:
	case DS_CONTINUE_ALTER_CONTAINER:
		return 0;
	case DS_PUT_ROW:
		return 2;
	case DS_APPEND_ROW:
		return 2;
	case DS_UPDATE_ROW_BY_ID:
		return 2;
	case DS_REMOVE_ROW:
		return 2;
	case DS_REMOVE_ROW_BY_ID:
		return 2;
	case DS_GET_ROW:
		return 1;
	case DS_GET_ROW_SET:
		return 1;
	case DS_QUERY_TQL:
		return 1;
	case DS_QUERY_GEOMETRY_WITH_EXCLUSION:
		return 1;
	case DS_QUERY_GEOMETRY_RELATED:
		return 1;
	case DS_GET_TIME_RELATED:
		return 1;
	case DS_GET_INTERPOLATE:
		return 1;
	case DS_QUERY_AGGREGATE:
		return 1;
	case DS_QUERY_TIME_RANGE:
		return 1;
	case DS_QUERY_SAMPLE:
		return 1;
	case DS_QUERY_FETCH_RESULT_SET:
	case DS_QUERY_CLOSE_RESULT_SET:
		return 0;
	case DS_GET_CONTAINER_OBJECT:
		return 1;
	case DS_SCAN_CHUNK_GROUP:
	case DS_SEARCH_BACKGROUND_TASK:
	case DS_EXECUTE_BACKGROUND_TASK:
	case DS_CHECK_TIMEOUT_RESULT_SET:
		return 0;
	}
	return 0;
}
