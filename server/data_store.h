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
	@brief Definition of DataStore
*/
#ifndef DATA_STORE_H_
#define DATA_STORE_H_

#include "util/container.h"
#include "util/trace.h"
#include "config_table.h"
#include "data_type.h"
#include "expirable_map.h"  
#include "message_row_store.h"
#include "object_manager.h"
#include "container_key.h"
#include "archive.h"

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
class BtreeMap;
struct ManagerSet;
class MessageSchema;
class ObjectManager;
class ChunkManager;
class ClusterService;

/*!
	@brief Exception for lock conflict
*/
class LockConflictException : public util::Exception {
public:
	explicit LockConflictException(
			UTIL_EXCEPTION_CONSTRUCTOR_ARGS_DECL) throw() :
			Exception(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_SET) {}
	virtual ~LockConflictException() throw() {}
};
#define DS_RETHROW_LOCK_CONFLICT_ERROR(cause, message) \
	GS_RETHROW_CUSTOM_ERROR(                           \
		LockConflictException, GS_ERROR_DEFAULT, cause, message)

#define DS_THROW_LOCK_CONFLICT_EXCEPTION(errorCode, message) \
	GS_THROW_CUSTOM_ERROR(LockConflictException, errorCode, message)

class ObjectManager;
class ChunktManager;
class ColumnInfo;
class ResultSet;  
class ResultSetOption;
class TreeFuncInfo;

struct BGTask {
	BGTask() {
		pId_ = UNDEF_PARTITIONID;
		bgId_ = UNDEF_BACKGROUND_ID;
	}
	PartitionId pId_;
	BackgroundId bgId_;
};


/*!
	@brief Estimate of Conainer resource size
*/
struct DataStoreEstimateSize {
	uint64_t metaSize_;
	uint64_t rowSize_;
	uint64_t btreeSize_;
	uint64_t hashSize_;
	uint64_t rtreeSize_;
	DataStoreEstimateSize() {
		metaSize_ = 0;
		rowSize_ = 0;
		btreeSize_ = 0;
		hashSize_ = 0;
		rtreeSize_ = 0;
	}
	DataStoreEstimateSize operator+(const DataStoreEstimateSize &b) {
		DataStoreEstimateSize c;
		c.metaSize_ = metaSize_ + b.metaSize_;
		c.rowSize_ = rowSize_ + b.rowSize_;
		c.btreeSize_ = btreeSize_ + b.btreeSize_;
		c.hashSize_ = hashSize_ + b.hashSize_;
		c.rtreeSize_ = rtreeSize_ + b.rtreeSize_;
		return c;
	}
	uint64_t getTotalSize() {
		return metaSize_ + rowSize_ + btreeSize_ + hashSize_ + rtreeSize_;
	}
	uint64_t getSeqTotalSize() {
		return metaSize_ + rowSize_ + getSeqBtreeSize() + hashSize_ +
			   rtreeSize_;
	}
	uint64_t getRowSize() {
		return rowSize_;
	}
	uint64_t getMetaSize() {
		return metaSize_;
	}
	uint64_t getBtreeSize() {
		return btreeSize_;
	}
	uint64_t getSeqBtreeSize() {
		return static_cast<uint64_t>(
			static_cast<double>(btreeSize_) * (32.0 / 24.0));
	}
	uint64_t getHashSize() {
		return hashSize_;
	}
	uint64_t getRtreeSize() {
		return rtreeSize_;
	}
	uint64_t getMaxRowSize() {
		return rowSize_ * 2;
	}
	uint64_t getMaxTotalSize() {
		return metaSize_ + getMaxRowSize() + btreeSize_ + hashSize_ +
			   rtreeSize_;
	}
};

/*!
	@brief Limit of DataStore Value
*/
class DataStoreValueLimitConfig {
public:
	DataStoreValueLimitConfig(const ConfigTable &configTable);
	~DataStoreValueLimitConfig() {}

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
	uint32_t limitSmallSize_;
	uint32_t limitBigSize_;
	uint32_t limitArrayNumSize_;
	uint32_t limitColumnNumSize_;
	uint32_t limitContainerNameSize_;
	uint32_t limitIndexNumSize_;
};

/*!
	@brief DataStore
*/
class DataStore {
public:  
	/*!
		@brief Status of DDL
	*/
	enum PutStatus {
		NOT_EXECUTED,
		CREATE,
		UPDATE,
		CHANGE_PROPERY,
	};
	/*!
		@brief Result of comparing two schema
	*/
	enum SchemaState {
		SAME_SCHEMA,
		PROPERY_DIFFERENCE,
		COLUMNS_DIFFERENCE,
		COLUMNS_ADD,
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
		Latch(TransactionContext &txn, PartitionId pId, DataStore *dataStore,
			ClusterService *clusterService);
		~Latch();

	private:
		const PartitionId pId_;
		TransactionContext &txn_;
		DataStore *dataStore_;
		ClusterService *clusterService_;
		uint32_t startChunkNum_;
	};

	/*!
		@brief Condition of Container search
	*/
	class ContainerCondition {
	public:
		ContainerCondition(util::StackAllocator &alloc) : attributes_(alloc) {}
		/*!
			@brief Insert list of ContainerAttribute condition
		*/
		void setAttributes(const util::Set<ContainerAttribute> &sourceSet) {
			for (util::Set<ContainerAttribute>::const_iterator itr =
					 sourceSet.begin();
				 itr != sourceSet.end(); itr++) {
				attributes_.insert(*itr);
			}
		}

		/*!
			@brief Insert ContainerAttribute condition
		*/
		void insertAttribute(ContainerAttribute attribute) {
			attributes_.insert(attribute);
		}

		/*!
			@brief Get list of ContainerAttribute condition
		*/
		const util::Set<ContainerAttribute> &getAttributes() {
			return attributes_;
		}

	private:
		util::Set<ContainerAttribute> attributes_;
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
			oId_ = UNDEF_OID;
		}
		void setDropIndexData(MapType mapType, ChunkKey chunkKey, OId oId) {
			eventType_ = DROP_INDEX;
			mapType_ = mapType;
			chunkKey_ = chunkKey;
			oId_ = oId;
		}
		void setDropContainerData(OId &oId) {
			eventType_ = DROP_CONTAINER;
			oId_ = oId;
		}
		void getDropIndexData(MapType &mapType, ChunkKey &chunkKey, OId &oId) const {
			if (eventType_ != DROP_INDEX) {
				assert(true);
			}
			mapType = mapType_;
			chunkKey = chunkKey_;
			oId = oId_;
		}
		void getDropContainerData(OId &oId) const {
			if (eventType_ != DROP_CONTAINER) {
				assert(true);
			}
			oId = oId_;
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
		void updateOId(OId oId) {
			oId_ = oId;
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
		union {
			ChunkKey chunkKey_;
			uint32_t padding_;
		};
		OId oId_;
	};

	static const ResultSize CONTAINER_NAME_LIST_NUM_UPPER_LIMIT = INT32_MAX;

	static const size_t RESULTSET_POOL_BLOCK_SIZE_BITS = 20;

	static const size_t CHUNKKEY_BIT_NUM = ChunkManager::CHUNKKEY_BIT_NUM;
	static const size_t CHUNKKEY_BITS = ChunkManager::CHUNKKEY_BITS;

	static const uint32_t MAX_PARTITION_NUM = 10000;
	static const uint32_t MAX_CONCURRENCY = 128;

public:  
public:  
	DataStore(ConfigTable &configTable, ChunkManager *chunkManager);
	~DataStore();

	void initialize(ManagerSet &mgrSet);

	bool isRestored(PartitionId pId) const;

	bool isUndoCompleted(PartitionId pId) const;

	void setUndoCompleted(PartitionId pId);


	uint64_t getContainerCount(TransactionContext &txn, PartitionId pId,
		const DatabaseId dbId, ContainerCondition &condition);
	void getContainerNameList(TransactionContext &txn, PartitionId pId,
		int64_t start, ResultSize limit, const DatabaseId dbId,
		ContainerCondition &condition, util::XArray<FullContainerKey> &nameList);
	class ContainerListHandler {
	public:
		virtual ~ContainerListHandler();
		virtual void operator()(
				TransactionContext &txn,
				ContainerId id, DatabaseId dbId, ContainerAttribute attribute,
				BaseContainer &container) const = 0;
	};
	bool scanContainerList(
		TransactionContext &txn, PartitionId pId, ContainerId startContainerId,
		uint64_t limit, const DatabaseId *dbId, ContainerCondition &condition,
		const ContainerListHandler &handler);

	BaseContainer *putContainer(TransactionContext &txn, PartitionId pId,
		const FullContainerKey &containerKey, ContainerType containerType,
		uint32_t schemaSize, const uint8_t *containerSchema, bool isEnable,
		int32_t featureVersion,
		PutStatus &status, bool isCaseSensitive = false);

	void dropContainer(TransactionContext &txn, PartitionId pId,
		const FullContainerKey &containerKey, ContainerType containerType,
		bool isCaseSensitive = false);
	BaseContainer *getContainer(TransactionContext &txn, PartitionId pId,
		const FullContainerKey &containerKey, ContainerType containerType,
		bool isCaseSensitive = false, bool allowExpiration = false);
	BaseContainer *getContainer(TransactionContext &txn, PartitionId pId,
		ContainerId containerId,
		ContainerType containerType, bool allowExpiration = false);  
	BaseContainer *getContainer(TransactionContext &txn, PartitionId pId,
		const ContainerCursor &containerCursor, bool allowExpiration = false) {
		return getBaseContainer(txn, pId, containerCursor.getContainerOId(), 
			ANY_CONTAINER, allowExpiration);
	}

	BaseContainer *getContainerForRestore(TransactionContext &txn,
		PartitionId pId, OId oId,
		ContainerId containerId, uint8_t containerType);  

	void updateContainer(TransactionContext &txn, BaseContainer *container, OId newContainerOId);

	void removeColumnSchema(
		TransactionContext &txn, PartitionId pId, OId schemaOId);

	void removeTrigger(
		TransactionContext &txn, PartitionId pId, OId triggerOId);
	void insertTrigger(TransactionContext &txn, PartitionId pId,
		util::XArray<const uint8_t *> &binary, int64_t triggerHashKey,
		OId &triggerOId);
	OId getTriggerId(TransactionContext &txn, PartitionId pId,
		util::XArray<const uint8_t *> &binary, int64_t triggerHashKey);

	ChunkKey getLastChunkKey(TransactionContext &txn);

	const DataStoreValueLimitConfig &getValueLimitConfig() {
		return dsValueLimitConfig_;
	}

	ObjectManager *getObjectManager() const {
		return objectManager_;
	}

	void createPartition(PartitionId pId);
	void dropPartition(PartitionId pId);
	/*!
		@brief Get PartitionGroupId from PartitionId
	*/
	inline PartitionGroupId calcPartitionGroupId(PartitionId pId) {
		return pgConfig_.getPartitionGroupId(pId);
	}

	void setCurrentBGTask(PartitionGroupId pgId, const BGTask &bgTask) {
		currentBackgroundList_[pgId] = bgTask;
	}
	void resetCurrentBGTask(PartitionGroupId pgId) {
		currentBackgroundList_[pgId] = BGTask();
	}
	bool getCurrentBGTask(PartitionGroupId pgId, BGTask &bgTask) const {
		bgTask = currentBackgroundList_[pgId];
		return bgTask.bgId_ != UNDEF_BACKGROUND_ID;
	}
	bool searchBGTask(TransactionContext &txn, PartitionId pId, BGTask &bgTask);
	void finalizeContainer(TransactionContext &txn, BaseContainer *container);
	void finalizeMap(TransactionContext &txn, 
		const AllocateStrategy &allcateStrategy, BaseIndex *index);
	bool executeBGTask(TransactionContext &txn, BackgroundId bgId);
	void clearAllBGTask(TransactionContext &txn);
	static BaseIndex *getIndex(TransactionContext &txn, 
		ObjectManager &objectManager, MapType mapType, OId mapOId, 
		const AllocateStrategy &strategy, BaseContainer *container,
		TreeFuncInfo *funcInfo);

	friend std::ostream &operator<<(
		std::ostream &output, const DataStore::BackgroundData &bgData);
	void dumpTraceBGTask(TransactionContext &txn, PartitionId pId);

	void restartPartition(
		TransactionContext &txn, ClusterService *clusterService);

	void dumpPartition(TransactionContext &txn, PartitionId pId, DumpType type,
		const char *fileName);
	std::string dump(TransactionContext &txn, PartitionId pId);
	void dumpContainerIdTable(PartitionId pId);
	void archive(util::StackAllocator &alloc, TransactionManager &txnMgr, BibInfo &bibInfo);
	void setLastExpiredTime(PartitionId pId, Timestamp time, bool force = false);
	void setLatestBatchFreeTime(PartitionId pId, Timestamp time, bool force = false);

	ResultSet *createResultSet(TransactionContext &txn, ContainerId containerId,
		SchemaVersionId versionId, int64_t emNow, 
		ResultSetOption *rsOption, bool noExpire = false);

	ResultSet *getResultSet(TransactionContext &txn, ResultSetId resultSetId);



	void closeResultSet(PartitionId pId, ResultSetId resultSetId);

	void closeOrClearResultSet(PartitionId pId, ResultSetId resultSetId);

	void checkTimeoutResultSet(PartitionGroupId pgId, int64_t checkTime);

	void handleUpdateError(std::exception &e, ErrorCode errorCode);
	void handleSearchError(std::exception &e, ErrorCode errorCode);

	static ChunkKey convertTimestamp2ChunkKey(Timestamp time, bool isRoundUp) {
		ChunkKey chunkKey = static_cast<ChunkKey>(time >> CHUNKKEY_BIT_NUM);
		if (isRoundUp && ((time & CHUNKKEY_BITS) != 0)) {
			chunkKey++;
		}
		return chunkKey;
	}

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

	bool executeBatchFree(PartitionId pId, Timestamp timestamp,
		uint64_t maxScanNum, uint64_t &scanNum);
	Timestamp getLatestExpirationCheckTime(PartitionId pId) {
		return expirationStat_[pId].expiredTime_;
	}
	Timestamp getLatestBatchFreeTime(TransactionContext &txn);

	struct Config;
	Config& getConfig() { return config_; }

	static PartitionId resolvePartitionId(
			util::StackAllocator &alloc, const FullContainerKey &containerKey,
			PartitionId partitionCount, ContainerHashMode hashMode);




private:  
	static const bool LOCK_FORCE = true;

	/*!
		@brief Status of DataStore, when service is starting
	*/
	enum DBState {
		UNRESTORED,  
		RESTORED,	
		CP_COMPLETED,   
		UNDO_COMPLETED  
	};


	/*!
		@brief Compare method for ResultSetMap
	*/
	struct ResultSetIdHash {
		ResultSetId operator()(const ResultSetId &key) {
			return key;
		}
	};

	/*!
		@brief Cache of ContainerInfo
	*/
	struct ContainerInfoCache {
		OId oId_;
		int64_t databaseVersionId_;
		ContainerAttribute attribute_;
		ContainerInfoCache(
			OId oId, int64_t databaseVersionId, ContainerAttribute attribute)
			: oId_(oId),
			  databaseVersionId_(databaseVersionId),
			  attribute_(attribute) {}
	};

	/*!
		@brief Compare method for ContainerIdTable
	*/
	struct containerIdMapAsc {
		bool operator()(const std::pair<ContainerId, ContainerInfoCache> &left,
			const std::pair<ContainerId, ContainerInfoCache> &right) const;
		bool operator()(const std::pair<ContainerId, const ContainerInfoCache*> &left,
			const std::pair<ContainerId, const ContainerInfoCache*> &right) const;
	};

	/*!
		@brief Map of (ContainerId, ContainerInfoCache)
	*/
	class ContainerIdTable {
#if UTIL_CXX11_SUPPORTED
		typedef std::unordered_map<ContainerId, ContainerInfoCache>
			ContainerIdMap;
#else
		typedef std::map<ContainerId, ContainerInfoCache> ContainerIdMap;
#endif
	public:
		typedef util::XArray<std::pair<ContainerId, ContainerInfoCache> >
			ContainerIdList;
		typedef util::XArray< std::pair<ContainerId, const ContainerInfoCache*> > ContainerIdRefList;
		ContainerIdTable(uint32_t partitionNum);
		~ContainerIdTable();
		void set(PartitionId pId, ContainerId containerId, OId oId,
			int64_t databaseVersionId, ContainerAttribute attribute);
		void remove(PartitionId pId, ContainerId containerId);
		OId get(PartitionId pId, ContainerId containerId);
		void dropPartition(PartitionId pId);
		/*!
			@brief Returns number of Container in the partition
		*/
		inline uint64_t size(
			PartitionId pId) const {  
			return containerIdMap_[pId].size();
		}
		void getList(PartitionId pId, int64_t start, ResultSize limit,
			ContainerIdList &containerInfoCacheList);
		bool getListOrdered(
			PartitionId pId, ContainerId startId, uint64_t limit,
			const DatabaseId *dbId, ContainerCondition &condition,
			ContainerIdRefList &list) const;

	private:
		ContainerIdMap *containerIdMap_;
		uint32_t partitionNum_;
	};


	/*!
		@brief DataStore meta data format
	*/
	struct DataStorePartitionHeader {
		static const int32_t PADDING_SIZE = 432;
		OId metaMapOId_;
		OId schemaMapOId_;
		OId triggerMapOId_;
		uint64_t maxContainerId_;
		ChunkKey chunkKey_;
		uint32_t padding0_;
		OId backgroundMapOId_;
		uint64_t maxBackgroundId_;
		Timestamp latestCheckTime_;
		Timestamp latestBatchFreeTime_;
		uint8_t padding_[PADDING_SIZE];
	};

	/*!
		@brief DataStore meta data
	*/
	class DataStorePartitionHeaderObject : public BaseObject {
	public:
		DataStorePartitionHeaderObject(
			PartitionId pId, ObjectManager &objectManager)
			: BaseObject(pId, objectManager) {}
		DataStorePartitionHeaderObject(
			PartitionId pId, ObjectManager &objectManager, OId oId)
			: BaseObject(pId, objectManager, oId) {}

		void setMetaMapOId(OId oId) {
			getObjectManager()->setDirty(getPartitionId(), getBaseOId());
			get()->metaMapOId_ = oId;
		}
		void setSchemaMapOId(OId oId) {
			getObjectManager()->setDirty(getPartitionId(), getBaseOId());
			get()->schemaMapOId_ = oId;
		}
		void setTriggerMapOId(OId oId) {
			getObjectManager()->setDirty(getPartitionId(), getBaseOId());
			get()->triggerMapOId_ = oId;
		}
		void setBackgroundMapOId(OId oId) {
			getObjectManager()->setDirty(getPartitionId(), getBaseOId());
			get()->backgroundMapOId_ = oId;
		}
		void incrementBackgroundId() {
			getObjectManager()->setDirty(getPartitionId(), getBaseOId());
			get()->maxBackgroundId_++;
		}
		void incrementMaxContainerId() {
			getObjectManager()->setDirty(getPartitionId(), getBaseOId());
			get()->maxContainerId_++;
		}
		void setChunkKey(ChunkKey chunkKey, bool force = false) {
			if (force || get()->chunkKey_  < chunkKey) {
				getObjectManager()->setDirty(getPartitionId(), getBaseOId());
				get()->chunkKey_ = chunkKey;
			}
		}
		void setLatestCheckTime(Timestamp timestamp, bool force = false) {
			if (force || get()->latestCheckTime_  < timestamp) {
				getObjectManager()->setDirty(getPartitionId(), getBaseOId());
				get()->latestCheckTime_ = timestamp;
			}
		}
		void setLatestBatchFreeTime(Timestamp timestamp, bool force = false) {
			if (force || get()->latestBatchFreeTime_  < timestamp) {
				getObjectManager()->setDirty(getPartitionId(), getBaseOId());
				get()->latestBatchFreeTime_ = timestamp;
			}
		}
		OId getMetaMapOId() const {
			return get()->metaMapOId_;
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
		uint64_t getMaxBackgroundId() const {
			return get()->maxBackgroundId_;
		}
		uint64_t getMaxContainerId() const {
			return get()->maxContainerId_;
		}
		ChunkKey getChunkKey() const {
			return get()->chunkKey_;
		}
		Timestamp getLatestCheckTime() const {
			return get()->latestCheckTime_;
		}
		Timestamp getLatestBatchFreeTime() const {
			return get()->latestBatchFreeTime_;
		}
		void initialize(
			TransactionContext &txn, const AllocateStrategy &allocateStrategy);
		void finalize(
			TransactionContext &txn, const AllocateStrategy &allocateStrategy);

	private:
		DataStorePartitionHeader *get() const {
			return getBaseAddr<DataStorePartitionHeader *>();
		}
	};

	static class StatSetUpHandler : public StatTable::SetUpHandler {
		virtual void operator()(StatTable &stat);
	} statSetUpHandler_;

	/*!
		@brief Updates Stat.
	*/
	class StatUpdator : public StatTable::StatUpdator {
		virtual bool operator()(StatTable &stat);

	public:
		DataStore *dataStore_;
	} statUpdator_;

	/*!
		@brief Configuration of DataStore
	*/
	struct Config : public ConfigTable::ParamHandler {
		Config(ConfigTable &configTable);

		void setUpConfigHandler(ConfigTable& configTable);
		virtual void operator()(
				ConfigTable::ParamId id, const ParamValue &value);

		double getBackgroundMinRate() const;
		double getBackgroundWaitWeight() const {
			return backgroundWaitWeight_;
		}
		void setBackgroundMinRate(double rate);

		util::Atomic<bool> isAutoExpire_;
		bool isAutoExpire() const {
			return isAutoExpire_;
		}
		void setAutoExpire(bool state) {
			isAutoExpire_ = state;
		}
		util::Atomic<Timestamp> erasableExpiredTime_;
		Timestamp getErasableExpiredTime() const {
			return erasableExpiredTime_;
		}
		void setErasableExpiredTime(const char8_t *timeStr) {
			util::DateTime dateTime(timeStr, false);
			erasableExpiredTime_ = dateTime.getUnixTime();
		}
		util::Atomic<Timestamp> simulateErasableExpiredTime_;
		Timestamp getSimulateErasableExpiredTime() const {
			return simulateErasableExpiredTime_;
		}
		void setSimulateErasableExpiredTime(const char8_t *timeStr) {
			util::DateTime dateTime(timeStr, false);
			simulateErasableExpiredTime_ = dateTime.getUnixTime();
		}
		util::Atomic<int32_t> batchScanNum_;
		int32_t getBatchScanNum() const {
			return batchScanNum_;
		}
		void setBatchScanNum(int32_t scanNum) {
			batchScanNum_ = scanNum;
		}
		util::Atomic<int32_t> rowArrayRateExponent_;
		int32_t getRowArrayRateExponent() const {
			return rowArrayRateExponent_;
		}
		void setRowArrayRateExponent(int32_t rowArrayRateExponent) {
			rowArrayRateExponent_ = rowArrayRateExponent;
		}
		util::Atomic<bool> isRowArraySizeControlMode_;
		bool isRowArraySizeControlMode() const {
			return isRowArraySizeControlMode_;
		}
		void setRowArraySizeControlMode(bool state) {
			isRowArraySizeControlMode_ = state;
		}
		util::Atomic<int64_t> backgroundMinRate_;
		double backgroundWaitWeight_;
		int32_t checkErasableExpiredInterval_;
		int32_t concurrencyNum_;

		void setCheckErasableExpiredInterval(int32_t val, int32_t coucurrency) {
			concurrencyNum_ = coucurrency;
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

		int32_t getCheckErasableExpiredInterval() {
			return checkErasableExpiredInterval_;
		}
		double storeMemoryAgingSwapRate_;

		void setStoreMemoryAgingSwapRate(double val) {
			if (val < 0) {
				storeMemoryAgingSwapRate_ = 0;
			}
			else {
				storeMemoryAgingSwapRate_ = val;
			}
		}

		double getStoreMemoryAgingSwapRate() {
			return storeMemoryAgingSwapRate_;
		}
	} config_;

	struct ExpirationStat {
		ExpirationStat() : expiredTime_(0), simulateTime_(0), preSimulateTime_(0),
			simulateCount_(0), preSimulateCount_(0), preExpiredNum_(0), 
			expiredNum_(0), diffExpiredNum_(0), totalScanTime_(0), totalScanNum_(0) {}

		Timestamp expiredTime_;
		Timestamp simulateTime_;
		Timestamp preSimulateTime_;
		size_t simulateCount_;
		size_t preSimulateCount_;
		size_t preExpiredNum_;
		size_t expiredNum_;
		int64_t diffExpiredNum_;
		int64_t totalScanTime_;
		int64_t totalScanNum_;
	};
	void updateExpirationStat(PartitionId pId, size_t expiredNum,
		size_t erasableNum, size_t scanNum, bool isTail, int64_t scanTime)
	{
		ExpirationStat &stat = expirationStat_[pId];

		stat.expiredNum_ += expiredNum;
		stat.simulateCount_ += erasableNum;
		stat.totalScanTime_ += scanTime;
		stat.totalScanNum_ += scanNum;
		if (isTail) {
			stat.diffExpiredNum_ = static_cast<int64_t>(stat.expiredNum_) - 
				static_cast<int64_t>(stat.preExpiredNum_);
			stat.preExpiredNum_ = stat.expiredNum_;
			stat.expiredNum_ = 0;
			stat.preSimulateCount_ = stat.simulateCount_;
			stat.preSimulateTime_ = stat.simulateTime_;
			stat.simulateTime_ = config_.getSimulateErasableExpiredTime();
			stat.simulateCount_ = 0;
			stat.totalScanTime_ = 0;
			stat.totalScanNum_ = 0;
		}
	}

	static const ChunkId INITIAL_CHUNK_ID = 0;
	static const uint32_t FIRST_OBJECT_OFFSET =
		ObjectManager::CHUNK_HEADER_BLOCK_SIZE * 2 + 4;
	const OId PARTITION_HEADER_OID;

private:					  
	std::string cpFilePath_;  
	std::vector<DBState> dbState_;
	std::string eventLogPath_;
	PartitionGroupConfig pgConfig_;
	DataStoreValueLimitConfig dsValueLimitConfig_;
	AllocateStrategy allocateStrategy_;
	int32_t affinityGroupSize_;  

	std::vector<ResultSetId> resultSetIdList_;
	util::FixedSizeAllocator<util::Mutex> resultSetPool_;
	util::StackAllocator **resultSetAllocator_;  
	util::ExpirableMap<ResultSetId, ResultSet, int64_t,
		ResultSetIdHash>::Manager **resultSetMapManager_;
	util::ExpirableMap<ResultSetId, ResultSet, int64_t, ResultSetIdHash> *
		*resultSetMap_;
	std::vector<BGTask> currentBackgroundList_;
	std::vector<uint64_t> activeBackgroundCount_;
	std::vector<ExpirationStat> expirationStat_;

	static const int32_t RESULTSET_MAP_HASH_SIZE = 100;
	static const size_t RESULTSET_FREE_ELEMENT_LIMIT =
		10 * RESULTSET_MAP_HASH_SIZE;
	static const int32_t DS_MAX_RESULTSET_TIMEOUT_INTERVAL = 60;

	ContainerIdTable *containerIdTable_;
	ObjectManager *objectManager_;
	ClusterService *clusterService_;



private:  
	void forceCloseAllResultSet(PartitionId pId);

	void closeResultSetInternal(PartitionGroupId pgId, ResultSet &rs);

	bool restoreContainerIdTable(
		TransactionContext &txn, ClusterService *clusterService);
	void restoreBackground(TransactionContext &txn, 
		ClusterService *clusterService);
	void restoreLastExpiredTime(TransactionContext &txn, ClusterService *clusterService);
	void setUnrestored(PartitionId pId);
	void setRestored(PartitionId pId);

	BtreeMap *getSchemaMap(TransactionContext &txn, PartitionId pId);
	void insertColumnSchema(TransactionContext &txn, PartitionId pId,
		MessageSchema *messageSchema, int64_t schemaHashKey, OId &schemaOId);
	OId getColumnSchemaId(TransactionContext &txn, PartitionId pId,
		MessageSchema *messageSchema, int64_t schemaHashKey);
	BtreeMap *getTriggerMap(TransactionContext &txn, PartitionId pId);

	BaseContainer *createContainer(TransactionContext &txn, PartitionId pId,
		const FullContainerKey &containerKey, ContainerType containerType,
		MessageSchema *messageSchema);
	void changeContainerSchema(TransactionContext &txn, PartitionId pId,
		const FullContainerKey &containerKey,
		BaseContainer *&container, MessageSchema *messageSchema,
		util::XArray<uint32_t> &copyColumnMap);
	void changeContainerProperty(TransactionContext &txn, PartitionId pId,
		BaseContainer *&container, MessageSchema *messageSchema);
	void addContainerSchema(TransactionContext &txn, PartitionId pId,
		BaseContainer *&container, MessageSchema *messageSchema);

	BaseContainer *getBaseContainer(TransactionContext &txn, PartitionId pId,
		OId oId, ContainerType containerType, bool allowExpiration = false);

	BtreeMap *getBackgroundMap(TransactionContext &txn, PartitionId pId);
	uint64_t getBGTaskCount(PartitionId pId) {
		return activeBackgroundCount_[pId];
	}
	BackgroundId insertBGTask(TransactionContext &txn, PartitionId pId, 
		BackgroundData &bgData);
	void removeBGTask(TransactionContext &txn, PartitionId pId, BackgroundId bgId,
		BackgroundData &bgData);
	void updateBGTask(TransactionContext &txn, PartitionId pId, BackgroundId bgId, 
		BackgroundData &beforBgData, BackgroundData &afterBgData);
	bool executeBGTaskInternal(TransactionContext &txn, BackgroundData &bgData);

	ExpirationStat getExpirationStat(PartitionId pId) {
		return expirationStat_[pId];
	}
	BaseContainer *getBaseContainer(TransactionContext &txn, PartitionId pId, 
		const BibInfo::Container &bibInfo);
	MessageSchema *makeMessageSchema(TransactionContext &txn, ContainerType containerType, const BibInfo::Container &bibInfo);
	ShareValueList *makeCommonContainerSchema(TransactionContext &txn, int64_t schemaHashKey, MessageSchema *messageSchema);


	static class ConfigSetUpHandler : public ConfigTable::SetUpHandler {
		virtual void operator()(ConfigTable &config);
	} configSetUpHandler_;
};

#endif
