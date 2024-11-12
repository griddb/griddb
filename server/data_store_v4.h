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
	@brief Definition of DataStoreV4
*/
#ifndef DATA_STORE_H_
#define DATA_STORE_H_

#include "util/container.h"
#include "util/trace.h"
#include "config_table.h"
#include "data_type.h"
#include "expirable_map.h"
#include "schema.h"
#include "object_manager_v4.h"
#include "container_key.h"
#include "partition.h" 
#include "utility_v5.h" 
#include "container_message_v4.h"

#include <map>
#include <utility>

#if UTIL_CXX11_SUPPORTED
#include <unordered_map>
#endif

UTIL_TRACER_DECLARE(DATA_STORE);
UTIL_TRACER_DECLARE(DATASTORE_BACKGROUND);

class BaseContainer;
class Collection;
class TimeSeries;
class Collection;
class TimeSeries;
class CheckpointBuffer;
class CheckpointFile;
class TransactionContext;
class BaseIndexStorage;
class BtreeMap;
class MessageSchema;
class ObjectManagerV4;
class ChunkManager;
class ClusterService;
class ResourceSet;
class ResultSetGuard;
struct ManagerSet;

struct SchemaMessage;
class KeyDataStore;
class ShareValueList;

class ObjectManagerV4;
class ChunktManager;
class ColumnInfo;
class ResultSet;
class ResultSetOption;
class TreeFuncInfo;
class ResultSetManager;
struct DataStoreConfig;
struct TxnLog;



/*!
	@brief Statistics of DataStoreV4
*/
class DataStoreStats {
public:
	enum {
		CHUNK_GROUP_STAT_COUNT = ChunkGroupStats::GROUP_STAT_END,
		CHUNK_CATEGORY_COUNT = DS_CHUNK_CATEGORY_SIZE,
		CHUNK_STAT_COUNT = CHUNK_GROUP_STAT_COUNT * CHUNK_CATEGORY_COUNT
	};

	enum Param {
		DS_STAT_EXPIRED_TIME,
		DS_STAT_BATCH_FREE_TIME,
		DS_STAT_ESTIMATE_BATCH_FREE_TIME,

		DS_STAT_BATCH_FREE_NUM,
		DS_STAT_ESTIMATE_BATCH_FREE_NUM,
		DS_STAT_SCAN_TIME,
		DS_STAT_SCAN_NUM,
		DS_STAT_BG_NUM,


		DS_STAT_CHUNK_STATS_START,
		DS_STAT_CHUNK_STATS_FINISH =
				DS_STAT_CHUNK_STATS_START + CHUNK_STAT_COUNT - 1,

		DS_STAT_END
	};

	typedef TimeStatTable<Param, DS_STAT_END> TimeTable;
	typedef TimeTable::BaseTable Table;
	typedef Table::Mapper Mapper;

	struct PlainChunkGroupStats {
		PlainChunkGroupStats();
		ChunkGroupStats::Table subTable_;
	};

	explicit DataStoreStats(const Mapper *mapper);

	static void setUpMapper(Mapper &mapper, uint32_t chunkSize);
	static void setUpBasicMapper(Mapper &mapper);
	static void setUpTransactionMapper(Mapper &mapper, uint32_t chunkSize);
	static void setUpDetailMapper(Mapper &mapper, uint32_t chunkSize);

	static StatTableParamId getStoreParamId(
			int32_t parentId, StatTableParamId id);

	void setChunkStats(
			ChunkCategoryId categoryId, const ChunkGroupStats::Table &stats);
	void getChunkStats(
			ChunkCategoryId categoryId, ChunkGroupStats::Table &stats) const;

	Timestamp getExpiredTime() const {
		return getTimestamp(DS_STAT_EXPIRED_TIME);
	}

	Timestamp getBatchFreeTime() const {
		return getTimestamp(DS_STAT_BATCH_FREE_TIME);
	}

	Timestamp getTimestamp(Param param) const {
		return static_cast<Timestamp>(table_(param).get());
	}

	void setTimestamp(Param param, Timestamp ts);

	Table table_;
	TimeTable timeTable_;
};

/*!
	@brief Configuration of DataStore
*/
struct DataStoreConfig : public ConfigTable::ParamHandler {
	DataStoreConfig(ConfigTable& configTable);

	void setUpConfigHandler(ConfigTable& configTable);
	virtual void operator()(
		ConfigTable::ParamId id, const ParamValue& value);

	int64_t getBackgroundMinRatePct() const;
	double getBackgroundMinRate() const;
	double getBackgroundWaitWeight() const {
		return backgroundWaitWeight_;
	}
	void setBackgroundMinRate(double rate);

	Timestamp getErasableExpiredTime() const {
		return erasableExpiredTime_;
	}
	void setErasableExpiredTime(const char8_t* timeStr) {
		util::DateTime dateTime(timeStr, false);
		erasableExpiredTime_ = dateTime.getUnixTime();
	}
	Timestamp getEstimateErasableExpiredTime() const {
		return estimateErasableExpiredTime_;
	}
	void setEstimateErasableExpiredTime(const char8_t* timeStr) {
		util::DateTime dateTime(timeStr, false);
		estimateErasableExpiredTime_ = dateTime.getUnixTime();
	}
	int32_t getBatchScanNum() const {
		return batchScanNum_;
	}
	void setBatchScanNum(int32_t scanNum) {
		batchScanNum_ = scanNum;
	}
	int32_t getRowArrayRateExponent() const {
		return rowArrayRateExponent_;
	}
	void setRowArrayRateExponent(int32_t rowArrayRateExponent) {
		rowArrayRateExponent_ = rowArrayRateExponent;
	}
	bool isRowArraySizeControlMode() const {
		return isRowArraySizeControlMode_;
	}
	void setRowArraySizeControlMode(bool state) {
		isRowArraySizeControlMode_ = state;
	}

	void setCheckErasableExpiredInterval(int32_t val, int32_t concurrency) {
		concurrencyNum_ = concurrency;
		setCheckErasableExpiredInterval(val);
	}

	void setCheckErasableExpiredInterval(int32_t val) {
		if (val == 0) {
			checkErasableExpiredInterval_ = 1;
		}
		else {
			checkErasableExpiredInterval_ = val;
		}
	}

	void setExpiredCheckCount(int32_t val) {
		if (val == 0) {
			checkExpiredContainerCount_ = 1;
		}
		else {
			checkExpiredContainerCount_ = val;
		}
	}

	int32_t getExpiredCheckCount() {
		return checkExpiredContainerCount_;
	}

	int32_t getCheckErasableExpiredInterval() const {
		return checkErasableExpiredInterval_;
	}

	void setStoreMemoryAgingSwapRate(double val) {
		if (val < 0) {
			storeMemoryAgingSwapRate_ = 0;
		}
		else {
			storeMemoryAgingSwapRate_ = val;
		}
	}

	double getStoreMemoryAgingSwapRate() const {
		return storeMemoryAgingSwapRate_;
	}

	uint32_t getLimitSmallSize() const {
		return limitSmallSize_;
	}
	uint32_t getLimitBigSize() const {
		return limitBigSize_;
	}
	uint32_t getLimitArrayNum() const {
		return limitArrayNumSize_;
	}
	uint32_t getLimitColumnNum() const {
		return limitColumnNumSize_;
	}
	uint32_t getLimitContainerNameSize() const {
		return limitContainerNameSize_;
	}
	uint32_t getLimitIndexNum() const {
		return limitIndexNumSize_;
	}
private:
	static const uint32_t LIMIT_SMALL_SIZE_LIST[12];
	static const uint32_t LIMIT_BIG_SIZE_LIST[12];
	static const uint32_t LIMIT_ARRAY_NUM_LIST[12];
	static const uint32_t LIMIT_COLUMN_NUM_LIST[12];
	static const uint32_t LIMIT_CONTAINER_NAME_SIZE_LIST[12];
	static const uint32_t LIMIT_INDEX_NUM_LIST[12];
	util::Atomic<Timestamp> erasableExpiredTime_;
	util::Atomic<Timestamp> estimateErasableExpiredTime_;
	util::Atomic<int32_t> batchScanNum_;
	util::Atomic<int32_t> rowArrayRateExponent_;
	util::Atomic<bool> isRowArraySizeControlMode_;
	util::Atomic<int64_t> backgroundMinRate_;
	double backgroundWaitWeight_;
	int32_t checkErasableExpiredInterval_;
	int32_t concurrencyNum_;
	double storeMemoryAgingSwapRate_;
	int32_t checkExpiredContainerCount_;

	uint32_t limitSmallSize_;
	uint32_t limitBigSize_;
	uint32_t limitArrayNumSize_;
	uint32_t limitColumnNumSize_;
	uint32_t limitContainerNameSize_;
	uint32_t limitIndexNumSize_;
};


/*!
	@brief DataStoreV4
*/
class DataStoreV4 : public DataStoreBase {
	friend class StoreV5Impl;	
public:  
	/*!
		@brief Result of comparing two schema
	*/
	enum SchemaState {
		SAME_SCHEMA,
		PROPERTY_DIFFERENCE,
		COLUMNS_DIFFERENCE,
		ONLY_TABLE_PARTITIONING_VERSION_DIFFERENCE,
		COLUMNS_ADD,
		COLUMNS_RENAME
	};

	/*!
		@brief Represents the type(s) of dump
	*/
	enum DumpType { CONTAINER_SUMMARY, CONTAINER_ALL };

	/*!
		@brief Pre/postprocess before/after DataStore operation
	*/
	class Latch {
	public:
		Latch(TransactionContext &txn, DataStoreV4 *dataStore,
			ClusterService *clusterService);
		~Latch();

	private:
		TransactionContext &txn_;
		DataStoreV4 *dataStore_;
		ClusterService *clusterService_;
	};

	typedef uint8_t BGEventType;
	/*!
		@brief BackgroundData base format
	*/
	class BackgroundData {
	public:
		static const BGEventType UNDEF_TYPE = 0;
		static const BGEventType DROP_INDEX = 1;
		static const BGEventType DROP_CONTAINER = 2;
	public:
		BackgroundData() {
			eventType_ = UNDEF_TYPE;
			status_ = 0;
			errorCounter_ = 0;
			mapType_ = MAP_TYPE_DEFAULT;
			padding_ = 0;
			groupId_ = UNDEF_DS_GROUPID;
			oId_ = UNDEF_OID;
			expirationTime_ = MAX_TIMESTAMP;
		}
		void setDropIndexData(MapType mapType, DSGroupId groupId, OId oId, Timestamp expirationTime) {
			eventType_ = DROP_INDEX;
			mapType_ = mapType;
			groupId_ = groupId;
			oId_ = oId;
			expirationTime_ = expirationTime;
		}
		void setDropContainerData(DSGroupId groupId, OId &oId, Timestamp expirationTime) {
			eventType_ = DROP_CONTAINER;
			groupId_ = groupId;
			oId_ = oId;
			expirationTime_ = expirationTime;
		}
		void getDropIndexData(MapType &mapType, DSGroupId& groupId, OId &oId, Timestamp &expirationTime) const {
			if (eventType_ != DROP_INDEX) {
				assert(true);
			}
			mapType = mapType_;
			groupId = groupId_;
			oId = oId_;
			expirationTime = expirationTime_;
		}
		void getDropContainerData(DSGroupId& groupId, OId &oId, Timestamp& expirationTime) const {
			if (eventType_ != DROP_CONTAINER) {
				assert(true);
			}
			groupId = groupId_;
			oId = oId_;
			expirationTime = expirationTime_;
		}
		bool isInvalid() const {
			return (errorCounter_ > 1);
		}
		void incrementError() {
			errorCounter_++;
		}
		uint8_t getErrorCount() const {
			return errorCounter_;
		}
		BGEventType getEventType() const {
			return eventType_;
		}

		bool operator==(const BackgroundData &b) const {
			if (memcmp(this, &b, sizeof(BackgroundData)) == 0) {
				return true;
			}
			else {
				return false;
			}
		}
		bool operator<(const BackgroundData &b) const {
			if (memcmp(this, &b, sizeof(BackgroundData)) < 0) {
				return true;
			}
			else {
				return false;
			}
		}

	private:
		BGEventType eventType_;
		uint8_t status_;  
		uint8_t errorCounter_;  
		MapType mapType_;
		int32_t padding_;
		DSGroupId groupId_;
		OId oId_;
		Timestamp expirationTime_;
	};

	class DataAffinityUtils;

	static const size_t CHUNKKEY_BIT_NUM = ChunkManager::CHUNKKEY_BIT_NUM;
	static const size_t CHUNKKEY_BITS = ChunkManager::CHUNKKEY_BITS;

public:  
public:  
	DataStoreV4(
			util::StackAllocator *stAlloc,
			util::FixedSizeAllocator<util::Mutex> *resultSetPool,
			ConfigTable *configTable, TransactionManager *txnMgr,
			ChunkManager *chunkManager, LogManager<MutexLocker> *logManager,
			KeyDataStore *keyStore, ResultSetManager &rsManager,
			const StatsSet &stats);
	~DataStoreV4();

	void initialize(ManagerSet& resourceSet);
	static void createResultSetManager(
			util::FixedSizeAllocator<util::Mutex> &resultSetPool,
			ConfigTable &configTable, ChunkBuffer &chunkBuffer,
			UTIL_UNIQUE_PTR<ResultSetManager> &rsManager) ;

	Serializable* exec(TransactionContext* txn, KeyDataStoreValue* storeValue, Serializable* message);

	bool support(Support type);
	void preProcess(TransactionContext* txn, ClusterService* clsService);
	void postProcess(TransactionContext* txn);
	void finalize();
	void redo(util::StackAllocator &alloc, RedoMode mode, const util::DateTime &redoStartTime,
			  Timestamp redoStartEmTime, Serializable* message);
	void activate(
		TransactionContext& txn, ClusterService* clusterService);
	StoreType getStoreType();


	class ContainerListHandler {
	public:
		virtual ~ContainerListHandler();
		virtual void operator()(
				TransactionContext &txn,
				ContainerId id, DatabaseId dbId, ContainerAttribute attribute,
				BaseContainer *container) const = 0;
	};

	KeyDataStore* getKeyDataStore() {
		return keyStore_;
	}

	const DataStoreConfig& getConfig() {
		return *config_;
	}

	ObjectManagerV4* getObjectManager() const {
		return objectManager_;
	}

	const DataStoreStats& stats() const { return dsStats_; };
	ResultSetManager* getResultSetManager() { return &rsManager_; }

	void finalizeContainer(TransactionContext &txn, BaseContainer *container);
	void finalizeMap(TransactionContext &txn, 
		const AllocateStrategy &allocateStrategy, BaseIndex *index, Timestamp ExpirationTime);

	static BaseIndex *getIndex(
			TransactionContext &txn, ObjectManagerV4 &objectManager,
			MapType mapType, OId mapOId, AllocateStrategy &strategy,
			BaseContainer *container, TreeFuncInfo *funcInfo,
			BaseIndexStorage *&indexStorage);
	static void releaseIndex(BaseIndexStorage &indexStorage) throw();

	void setLastExpiredTime(Timestamp time, bool force = false);

	uint64_t scanChunkGroup(TransactionContext& txn, Timestamp timestamp);


	void dumpPartition(TransactionContext& txn, DumpType type,
		const char* fileName);
	friend std::ostream& operator<<(
		std::ostream& output, const DataStoreV4::BackgroundData& bgData);
	void dumpTraceBGTask(TransactionContext& txn);





	static const char* AuditEventType(DSInputMes *input);

	int32_t AuditCategoryType(DSInputMes *input);
	
	static std::string getClientAddress(const Event *ev);

private:  
	static const bool LOCK_FORCE = true;

	struct GroupKey {
		DatabaseId dbId_;
		DSGroupId groupId_;
		int32_t affinityId_;
		ChunkKey chunkKey_; 
		GroupKey() : dbId_(UNDEF_DBID), groupId_(UNDEF_DS_GROUPID), 
			affinityId_(-1), chunkKey_(UNDEF_CHUNK_KEY) {}
		GroupKey(DatabaseId dbId, DSGroupId groupId, int32_t affinityId, ChunkKey chunkKey)
			: dbId_(dbId), groupId_(groupId), affinityId_(affinityId), chunkKey_(chunkKey) {}
		bool operator<(const GroupKey& b) const {
			if (chunkKey_ < b.chunkKey_) {
				return true;
			}
			else if (chunkKey_ > b.chunkKey_) {
				return false;
			}
			else if (dbId_ < b.dbId_) {
				return true;
			}
			else if (dbId_ > b.dbId_) {
				return false;
			}
			else if (affinityId_ < b.affinityId_) {
				return true;
			}
			else if (affinityId_ > b.affinityId_) {
				return false;
			}
			else {
				return groupId_ < b.groupId_;
			}
		}

		bool operator==(const GroupKey& b) const {
			return ((chunkKey_ == b.chunkKey_) && (dbId_ == b.dbId_) &&
				(affinityId_ == b.affinityId_) && (groupId_ == b.groupId_));
		}
		bool isMatch(DatabaseId dbId, int32_t affinityId, ChunkKey chunkKey) const {
			return ((chunkKey_ == chunkKey) && (dbId_ == dbId) &&
				(affinityId_ == affinityId));
		}
	};

	std::map<GroupKey, int64_t> groupKeyMap_;

	int32_t calcAffinityGroupId(const uint8_t* affinityBinary) {
		uint64_t hash = 0;
		const uint8_t* key = affinityBinary;
		uint32_t keylen = AFFINITY_STRING_MAX_LENGTH;
		for (hash = 0; --keylen != UINT32_MAX; hash = hash * 37 + *(key)++)
			;

		int32_t groupId = static_cast<int32_t>(hash % affinityGroupSize_);
		return groupId;
	}

	int32_t calcAffinityGroupId(const util::String &affinityStr) {
		uint8_t affinityBinary[AFFINITY_STRING_MAX_LENGTH];
		memset(affinityBinary, 0, AFFINITY_STRING_MAX_LENGTH);
		memcpy(affinityBinary, affinityStr.c_str(), affinityStr.length());
		int32_t affinityId = calcAffinityGroupId(affinityBinary);
		return affinityId;
	}

	DSGroupId getGroupId(DatabaseId dbId, const util::String &affinityStr, ChunkKey chunkKey);
	bool removeGroupId(DatabaseId dbId, const util::String& affinityStr, ChunkKey chunkKey, DSGroupId gorupId);
	bool isActive() const {
		return headerOId_ != UNDEF_OID;
	}

	/*!
		@brief DataStoreV4 meta data format
	*/
	struct DataStorePartitionHeader {
		static const int32_t PADDING_SIZE = 448;
		OId schemaMapOId_;
		OId triggerMapOId_;
		OId backgroundMapOId_;
		OId v4chunkIdMapOId_;
		uint64_t maxBackgroundId_;
		Timestamp latestCheckTime_;
		Timestamp latestBatchFreeTime_;
		uint8_t padding_[PADDING_SIZE];
	};

	/*!
		@brief DataStoreV4 meta data
	*/
	class DataStorePartitionHeaderObject : public BaseObject {
	public:
		DataStorePartitionHeaderObject(
			ObjectManagerV4 &objectManager, AllocateStrategy& strategy)
			: BaseObject(objectManager, strategy) {}
		DataStorePartitionHeaderObject(
			ObjectManagerV4 &objectManager, AllocateStrategy &strategy, OId oId)
			: BaseObject(objectManager, strategy, oId) {}

		void setSchemaMapOId(OId oId) {
			setDirty();
			get()->schemaMapOId_ = oId;
		}
		void setTriggerMapOId(OId oId) {
			setDirty();
			get()->triggerMapOId_ = oId;
		}
		void setBackgroundMapOId(OId oId) {
			setDirty();
			get()->backgroundMapOId_ = oId;
		}
		void setV4chunkIdMapOId(OId oId) {
			setDirty();
			get()->v4chunkIdMapOId_ = oId;
		}
		uint64_t allocateBackgroundId() {
			setDirty();
			get()->maxBackgroundId_++;
			return get()->maxBackgroundId_;
		}
		void setLatestCheckTime(Timestamp timestamp, bool force = false) {
			if (force || get()->latestCheckTime_  < timestamp) {
				setDirty();
				get()->latestCheckTime_ = timestamp;
			}
		}
		void setLatestBatchFreeTime(Timestamp timestamp, bool force = false) {
			if (force || get()->latestBatchFreeTime_  < timestamp) {
				setDirty();
				get()->latestBatchFreeTime_ = timestamp;
			}
		}

		OId getSchemaMapOId() const {
			return get()->schemaMapOId_;
		}
		OId getTriggerMapOId() const {
			return get()->triggerMapOId_;
		}
		OId getBackgroundMapOId() const {
			return get()->backgroundMapOId_;
		}
		OId getV4chunkIdMapOId() const {
			return get()->v4chunkIdMapOId_;
		}

		Timestamp getLatestCheckTime() const {
			return get()->latestCheckTime_;
		}
		Timestamp getLatestBatchFreeTime() const {
			return get()->latestBatchFreeTime_;
		}

		void initialize(
			TransactionContext &txn, AllocateStrategy& allocateStrategy, OId oId);
		void finalize(
			TransactionContext& txn, AllocateStrategy& allocateStrategy);

	private:
		DataStorePartitionHeader *get() const {
			return getBaseAddr<DataStorePartitionHeader *>();
		}
	};

	void updateStat(DataStoreStats& stat);
	ChunkCategoryId getCategoryId(DSGroupId groupId, ChunkKey chunkKey) const;
	bool isMapGroup(DSGroupId groupId) const;

	static const ChunkId INITIAL_CHUNK_ID = 0;
	static const uint32_t FIRST_OBJECT_OFFSET =
		ObjectManagerV4::CHUNK_HEADER_BLOCK_SIZE * 2 + 4;

private:					  
	DataStoreConfig *config_;
	AllocateStrategy allocateStrategy_;
	int32_t affinityGroupSize_;  
	DataStoreStats &dsStats_;
	ObjectManagerV4 *objectManager_;
	ClusterService *clusterService_;
	KeyDataStore *keyStore_;
	Latch *latch_;
	OId headerOId_;
	BaseContainer *container_;
	ResultSetManager &rsManager_;
	uint64_t activeBackgroundCount_;
	uint32_t blockSize_;

private:  
	void initializeHeader(TransactionContext& txn);
	void restoreBackground(TransactionContext &txn, 
		ClusterService *clusterService);
	void restoreGroupId(TransactionContext& txn,
		ClusterService* clusterService);
	void restoreV4ChunkIdMap(
		TransactionContext& txn, ClusterService* clusterService);

	void restoreLastExpiredTime(TransactionContext &txn, ClusterService *clusterService);

	BtreeMap *getSchemaMap(TransactionContext &txn);
	OId insertColumnSchema(TransactionContext &txn,
		MessageSchema *messageSchema);
	void removeColumnSchema(
		TransactionContext& txn, OId schemaOId);
	OId getColumnSchemaId(TransactionContext &txn,
		MessageSchema *messageSchema, int64_t schemaHashKey);

	BaseContainer* putContainer(TransactionContext& txn,
		const FullContainerKey& containerKey, ContainerType containerType,
		uint32_t schemaSize, const uint8_t* containerSchema, bool isEnable,
		int32_t featureVersion,
		PutStatus& status, bool isCaseSensitive, KeyDataStoreValue& keyStoreValue);

	ContainerId dropContainer(TransactionContext& txn,
		ContainerType containerType,
		bool isCaseSensitive, KeyDataStoreValue& keyStoreValue);

	BaseContainer *createContainer(TransactionContext &txn,
		const FullContainerKey &containerKey, ContainerType containerType,
		MessageSchema *messageSchema);
	PutStatus changeContainer(TransactionContext& txn,
		const FullContainerKey& containerKey, BaseContainer*& container,
		MessageSchema* messageSchema, bool isEnable,
		bool isCaseSensitive);
		
	void changeContainerSchema(TransactionContext &txn,
		const FullContainerKey &containerKey,
		BaseContainer *&container, MessageSchema *messageSchema,
		util::XArray<uint32_t> &copyColumnMap);
	void changeContainerProperty(TransactionContext &txn,
		BaseContainer *&container, MessageSchema *messageSchema);
	void changeTablePartitioningVersion(TransactionContext &txn,
		BaseContainer *&container, MessageSchema *messageSchema);

	void addContainerSchema(TransactionContext &txn,
		BaseContainer *&container, MessageSchema *messageSchema);

	BtreeMap *getBackgroundMap(TransactionContext &txn);
	BGTask searchBGTask(TransactionContext& txn);
	bool executeBGTask(TransactionContext& txn, const BGTask bgTask);
	uint64_t getBGTaskCount() {
		return activeBackgroundCount_;
	}
	BackgroundId insertBGTask(TransactionContext &txn, 
		BackgroundData &bgData);
	void removeBGTask(TransactionContext &txn, BackgroundId bgId,
		BackgroundData &bgData);
	void updateBGTask(TransactionContext &txn, BackgroundId bgId, 
		BackgroundData &beforBgData, BackgroundData &afterBgData);
	bool executeBGTaskInternal(TransactionContext &txn, BackgroundId bgId,
		BackgroundData &bgData);

	void setLatestBatchFreeTime(Timestamp time, bool force = false);

	bool setLatestTimeToStats(
			Timestamp time, bool force, DataStoreStats::Param param);
	void clearTimeStats();

	BaseContainer* getBaseContainer(TransactionContext& txn,
		OId oId, ContainerType containerType, bool allowExpiration = false);

	ShareValueList *makeCommonContainerSchema(TransactionContext &txn, int64_t schemaHashKey, MessageSchema *messageSchema);

	void removeTransaction(RedoMode mode, TransactionContext& txn);
	OId getHeadOId(DSGroupId groupId);

	PartitionId getPId();

	static class ConfigSetUpHandler : public ConfigTable::SetUpHandler {
		virtual void operator()(ConfigTable &config);
	} configSetUpHandler_;

	static int64_t calcSchemaHashKey(MessageSchema *messageSchema);
	static bool schemaCheck(TransactionContext &txn,
		ObjectManagerV4 &objectManager, AllocateStrategy& allocateStrategy, ShareValueList *commonContainerSchema,
		MessageSchema *messageSchema);
	static void finalizeSchema(
		ObjectManagerV4&objectManager, AllocateStrategy& allocateStrategy, 
		ShareValueList *commonContainerSchema);
	static void initializeSchema(TransactionContext &txn,
		ObjectManagerV4&objectManager, MessageSchema *messageSchema,
		AllocateStrategy &allocateStrategy,
		util::XArray<ShareValueList::ElemData> &list, uint32_t &allocateSize,
		bool onMemory);

	void checkContainerExistence(BaseContainer* container);
	void checkContainerSchemaVersion(
		BaseContainer* container, SchemaVersionId schemaVersionId);
	SchemaMessage* createSchemaMessage(TransactionContext& txn, BaseContainer* container);

	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::Commit* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::Abort* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::PutContainer* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::UpdateTablePartitioningId* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::DropContainer* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::GetContainer* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::CreateIndex* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::DropIndex* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::ContinueCreateIndex* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::ContinueAlterContainer* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::PutRow* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::AppendRow* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::UpdateRowById* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::RemoveRow* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::RemoveRowById* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::GetRow* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::GetRowSet* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::QueryTQL* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::QueryGeometryWithExclusion* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::QueryGeometryRelated* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::GetRowTimeRelated* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::QueryInterPolate* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::QueryAggregate* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::QueryTimeRange* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::QueryTimeSampling* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::QueryFetchResultSet* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::QueryCloseResultSet* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::GetContainerObject* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::ScanChunkGroup* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::SearchBackgroundTask* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::ExecuteBackgroundTask* opMes);
	Serializable* execEvent(TransactionContext* txn, KeyDataStoreValue* storeValue, DSInputMes::CheckTimeoutResultSet* opMes);

	TransactionContext& getTxn(util::StackAllocator& alloc, TxnLog& txnLog, const util::DateTime& redoStartTime,
		Timestamp redoStartEmTime);
	bool isTimeoutEvent(GSEventType eventType);
	void redoEvent(TransactionContext& txn, DataStoreLogV4 *dsLog, DataStoreLogV4::PutContainer* opeLog);
	void redoEvent(TransactionContext& txn, DataStoreLogV4* dsLog, DataStoreLogV4::DropContainer* opeLog);
	void redoEvent(TransactionContext& txn, DataStoreLogV4 *dsLog, DataStoreLogV4::Commit* opeLog);
	void redoEvent(TransactionContext& txn, DataStoreLogV4 *dsLog, DataStoreLogV4::Abort* opeLog);
	void redoEvent(TransactionContext& txn, DataStoreLogV4 *dsLog, DataStoreLogV4::CreateIndex* opeLog);
	void redoEvent(TransactionContext& txn, DataStoreLogV4 *dsLog, DataStoreLogV4::DropIndex* opeLog);
	void redoEvent(TransactionContext& txn, DataStoreLogV4 *dsLog, DataStoreLogV4::ContinueCreateIndex* opeLog);
	void redoEvent(TransactionContext& txn, DataStoreLogV4 *dsLog, DataStoreLogV4::ContinueAlterContainer* opeLog);
	void redoEvent(TransactionContext& txn, DataStoreLogV4 *dsLog, DataStoreLogV4::PutRow* opeLog);
	void redoEvent(TransactionContext& txn, DataStoreLogV4 *dsLog, DataStoreLogV4::UpdateRow* opeLog);
	void redoEvent(TransactionContext& txn, DataStoreLogV4 *dsLog, DataStoreLogV4::RemoveRow* opeLog);
	void redoEvent(TransactionContext& txn, DataStoreLogV4 *dsLog, DataStoreLogV4::LockRow* opeLog);

	void handleUpdateError(std::exception& e, ErrorCode errorCode);
	void handleSearchError(std::exception& e, ErrorCode errorCode);
};

class DataStoreV4::DataAffinityUtils {
public:
	static const uint64_t MIN_EXPIRE_INTERVAL_CATEGORY_ROUNDUP_BITS = 22;
	static const uint64_t MIN_EXPIRE_INTERVAL_CATEGORY_TERM_BITS = 28;
	static const uint64_t MAX_EXPIRE_INTERVAL_CATEGORY_TERM_BITS = 36;
	static const uint64_t MIN_UPDATE_INTERVAL_CATEGORY_TERM_BITS = 9;
	static const uint64_t MAX_UPDATE_INTERVAL_CATEGORY_TERM_BITS = 33;

	static const size_t EXPIRE_INTERVAL_CATEGORY_COUNT = 6;

	static uint32_t ilog2(uint64_t x);

	static ExpireIntervalCategoryId calcExpireIntervalCategoryId(
		uint64_t expireIntervalMillis);

	static uint64_t getExpireTimeRoundingBitNum(
		ExpireIntervalCategoryId expireCategory);

	static ChunkKey convertTimestamp2ChunkKey(
		Timestamp time, uint64_t roundingBitNum, bool isRoundUp);

	static Timestamp convertChunkKey2Timestamp(ChunkKey chunkKey) {
		Timestamp time;
		if (chunkKey == UNDEF_CHUNK_KEY) {
			time = INT64_MAX;
		}
		else if (chunkKey == MAX_CHUNK_KEY) {
			time = MAX_TIMESTAMP;
		}
		else {
			time = (static_cast<Timestamp>(chunkKey) << CHUNKKEY_BIT_NUM);
		}
		return time;
	}
};

#endif
