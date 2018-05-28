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
	LockConflictException(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_DECL) throw()
		: Exception(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_SET) {}
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

class Cursor {
	virtual bool isFinished() const = 0;
public:
	static const uint64_t NUM_PER_EXEC = 500;
};

class IndexCursor : Cursor {
public:
	IndexCursor() {};
	IndexCursor(const MvccRowImage &image) {
		setMvccImage(image);
	};
	IndexCursor(bool isImmediate) {
		if (isImmediate) {
			setImmediateMode();
		}
	};

	bool isFinished() const {
		return data_.rowId_ == MAX_ROWID;
	}
	bool isImmediateMode() const {
		return data_.type_ == MVCC_UNDEF;
	}

	void setImmediateMode() {
		data_.type_ = MVCC_UNDEF;
	}
	void setMvccImage(const MvccRowImage &image) {
		memcpy(&data_, &image, sizeof(Data));
	}
	MvccRowImage getMvccImage() const {
		MvccRowImage image;
		memcpy(&image, &data_, sizeof(Data));
		return image;
	}
	MapType getMapType() const {return data_.mapType_;}
	ColumnId getColumnId() const {return data_.columnId_;}
	RowId getRowId() const {return data_.rowId_;}
	static uint64_t getNum() {return NUM_PER_EXEC;}
	void setMapType(MapType mapType) {data_.mapType_ = mapType;}
	void setColumnId(ColumnId columnId) {data_.columnId_ = columnId;}
	void setRowId(RowId rowId) {data_.rowId_ = rowId;}
private:
	struct Data {
		Data() {
			rowId_ = INITIAL_ROWID;
			padding1_ = 0;
			type_ = MVCC_INDEX;
			mapType_ = MAP_TYPE_DEFAULT;
			padding2_ = 0;
			columnId_ = UNDEF_COLUMNID;
		}

		RowId rowId_; 
		uint64_t padding1_; 
		MVCC_IMAGE_TYPE type_;
		MapType mapType_;
		uint16_t padding2_;
		ColumnId columnId_;
	};
	Data data_;
};

class ContainerCursor : Cursor {
public:
	ContainerCursor() {};
	ContainerCursor(const MvccRowImage &image) {
		setMvccImage(image);
	};
	ContainerCursor(bool isImmediate) {
		if (isImmediate) {
			setImmediateMode();
		}
	};

	ContainerCursor(bool isImmediate, OId oId) {
		if (isImmediate) {
			setImmediateMode();
		}
		setContainerOId(oId);
	};

	bool isFinished() const {
		return data_.rowId_ == MAX_ROWID;
	}
	bool isImmediateMode() const {
		return data_.type_ == MVCC_UNDEF;
	}

	void setImmediateMode() {
		data_.type_ = MVCC_UNDEF;
	}
	void setMvccImage(const MvccRowImage &image) {
		memcpy(&data_, &image, sizeof(Data));
	}
	MvccRowImage getMvccImage() const {
		MvccRowImage image;
		memcpy(&image, &data_, sizeof(Data));
		return image;
	}
	static uint64_t getNum() {return NUM_PER_EXEC;}
	RowId getRowId() const {return data_.rowId_;}
	void setRowId(RowId rowId) {data_.rowId_ = rowId;}
	OId getContainerOId() const {return data_.containerOId_;}
	void setContainerOId(OId oId) {data_.containerOId_ = oId;}
private:
	struct Data {
		Data() {
			rowId_ = INITIAL_ROWID;
			containerOId_ = UNDEF_OID;
			type_ = MVCC_CONTAINER;
			padding1_ = 0;
			padding2_ = 0;
			padding3_ = 0;
		}

		RowId rowId_; 
		OId containerOId_; 
		MVCC_IMAGE_TYPE type_;
		uint8_t padding1_;
		uint16_t padding2_;
		uint32_t padding3_;
	};
	Data data_;
};

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

private:
	static const int32_t DS_CHUNK_EXP_SIZE_MIN = 15;
	static const uint32_t LIMIT_SMALL_SIZE_LIST[6];
	static const uint32_t LIMIT_BIG_SIZE_LIST[6];
	static const uint32_t LIMIT_ARRAY_NUM_LIST[6];
	static const uint32_t LIMIT_COLUMN_NUM_LIST[6];
	static const uint32_t LIMIT_CONTAINER_NAME_SIZE_LIST[6];

	uint32_t limitSmallSize_;
	uint32_t limitBigSize_;
	uint32_t limitArrayNumSize_;
	uint32_t limitColumnNumSize_;
	uint32_t limitContainerNameSize_;
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

	static const size_t CHUNKKEY_BIT_NUM =
		22;  
	static const size_t CHUNKKEY_BITS =
		0x3FFFFF;  

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

	BaseContainer *putContainer(TransactionContext &txn, PartitionId pId,
		const FullContainerKey &containerKey, ContainerType containerType,
		uint32_t schemaSize, const uint8_t *containerSchema, bool isEnable,
		PutStatus &status, bool isCaseSensitive = false);

	void dropContainer(TransactionContext &txn, PartitionId pId,
		const FullContainerKey &containerKey, ContainerType containerType,
		bool isCaseSensitive = false);
	BaseContainer *getContainer(TransactionContext &txn, PartitionId pId,
		const FullContainerKey &containerKey, ContainerType containerType,
		bool isCaseSensitive = false);
	BaseContainer *getContainer(TransactionContext &txn, PartitionId pId,
		ContainerId containerId,
		ContainerType containerType);  
	BaseContainer *getContainer(TransactionContext &txn, PartitionId pId,
		const ContainerCursor &containerCursor) {
		return getBaseContainer(txn, pId, containerCursor.getContainerOId(), 
			ANY_CONTAINER);
	}

	Collection *getCollection(
		TransactionContext &txn, PartitionId pId, ContainerId containerId);
	TimeSeries *getTimeSeries(
		TransactionContext &txn, PartitionId pId, ContainerId containerId);

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
	void setLastChunkKey(PartitionId pId, ChunkKey chunkKey);

	const DataStoreValueLimitConfig &getValueLimitConfig() {
		return dsValueLimitConfig_;
	}

	ObjectManager *getObjectManager() const {
		return objectManager_;
	}

	void createPartition(PartitionId pId);
	void dropPartition(PartitionId pId);
	UTIL_FORCEINLINE PartitionGroupId calcPartitionGroupId(PartitionId pId);

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
		const AllocateStrategy &strategy, BaseContainer *container);

	friend std::ostream &operator<<(
		std::ostream &output, const DataStore::BackgroundData &bgData);
	void dumpTraceBGTask(TransactionContext &txn, PartitionId pId);

	void restartPartition(
		TransactionContext &txn, ClusterService *clusterService);

	void dumpPartition(TransactionContext &txn, PartitionId pId, DumpType type,
		const char *fileName);
	std::string dump(TransactionContext &txn, PartitionId pId);
	void dumpContainerIdTable(PartitionId pId);

	ResultSet *createResultSet(TransactionContext &txn, ContainerId containerId,
		SchemaVersionId versionId, int64_t emNow, bool noExpire = false);

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

	struct Config;
	Config& getConfig() { return config_; }




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

	private:
		ContainerIdMap *containerIdMap_;
		uint32_t partitionNum_;
	};


	/*!
		@brief DataStore meta data format
	*/
	struct DataStorePartitionHeader {
		static const int32_t PADDING_SIZE = 448;
		OId metaMapOId_;
		OId schemaMapOId_;
		OId triggerMapOId_;
		uint64_t maxContainerId_;
		ChunkKey chunkKey_;
		uint32_t padding0_;
		OId backgroundMapOId_;
		uint64_t maxBackgroundId_;
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
		void setChunkKey(ChunkKey chunkKey) {
			getObjectManager()->setDirty(getPartitionId(), getBaseOId());
			get()->chunkKey_ = chunkKey;
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

		util::Atomic<int64_t> backgroundMinRate_;
		double backgroundWaitWeight_;
	} config_;

	static const ChunkId INITIAL_CHUNK_ID = 0;
	static const uint32_t FIRST_OBJECT_OFFSET =
		ObjectManager::CHUNK_HEADER_BLOCK_SIZE * 2 + 4;
	static const OId PARTITION_HEADER_OID;

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
	BaseContainer *getBaseContainer(TransactionContext &txn, PartitionId pId,
		OId oId, ContainerType containerType);

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


	static class ConfigSetUpHandler : public ConfigTable::SetUpHandler {
		virtual void operator()(ConfigTable &config);
	} configSetUpHandler_;
};

#endif
