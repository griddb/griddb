
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
#include <map>
#include <utility>

#if defined(__GXX_EXPERIMENTAL_CXX0X__) || __cplusplus >= 201103L
#include <unordered_map>
#endif

UTIL_TRACER_DECLARE(DATA_STORE);

class BaseContainer;
class Collection;
class TimeSeries;
class Collection;
class TimeSeries;
class CheckpointBuffer;
class CheckpointFile;
class TransactionContext;
class BtreeMap;
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

/*!
	@brief Extended Container name
*/
class ExtendedContainerName {
public:
	ExtendedContainerName(util::StackAllocator &alloc,
		const char *extContainerName, size_t extContainerNameLen);
	ExtendedContainerName(util::StackAllocator &alloc, DatabaseId databaseId,
		const char *databaseName, const char *containerName);

	const char *c_str() {
		return reinterpret_cast<const char *>(fullPathName_);
	}

	DatabaseId getDatabaseId() {
		if (databaseId_ == UNDEF_DBID) {
			const char *undefName = "#Unknown";
			if (strlen(fullPathName_) != strlen(undefName) ||
				strncmp(fullPathName_, undefName, strlen(undefName)) != 0) {
				size_t startDbNamePos = 0;
				size_t startContainerNamePos = 0;
				getFullPathNamePosition(fullPathName_, strlen(fullPathName_),
					startDbNamePos, startContainerNamePos);
				if (startContainerNamePos == 0) {
					databaseId_ = GS_PUBLIC_DB_ID;
				}
				else if (startDbNamePos == 0) {
					size_t len = startContainerNamePos - startDbNamePos - 1;
					size_t publicDbNameLen = strlen(GS_PUBLIC);
					size_t systemDbNameLen = strlen(GS_SYSTEM);
					if (len >= publicDbNameLen &&
						memcmp(fullPathName_, GS_PUBLIC, publicDbNameLen) ==
							0) {
						databaseId_ = GS_PUBLIC_DB_ID;
					}
					else if (len >= systemDbNameLen &&
							 memcmp(fullPathName_, GS_SYSTEM,
								 systemDbNameLen) == 0) {
						databaseId_ = GS_SYSYTEM_DB_ID;
					}
					else {
						GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR,
							"Unknown dbname" << getDatabaseName());
					}
				}
				else {
					size_t len = startDbNamePos - 1;
					char *name = ALLOC_NEW(alloc_) char[len + 1];
					memcpy(name, fullPathName_, len);
					name[len] = '\0';
					databaseId_ = atol(name);
				}
			}
		}

		return databaseId_;
	}
	const char *getDatabaseName() {
		if (databaseName_ == NULL) {
			const char *undefName = "#Unknown";
			if (strlen(fullPathName_) != strlen(undefName) ||
				strncmp(fullPathName_, undefName, strlen(undefName)) != 0) {
				size_t startDbNamePos = 0;
				size_t startContainerNamePos = 0;
				getFullPathNamePosition(fullPathName_, strlen(fullPathName_),
					startDbNamePos, startContainerNamePos);

				if (startContainerNamePos == 0) {
					size_t len = strlen(GS_PUBLIC) + 1;
					databaseName_ = ALLOC_NEW(alloc_) char[len];
					memcpy(databaseName_, GS_PUBLIC, len);
				}
				else {
					assert(startDbNamePos <= startContainerNamePos);
					size_t len = startContainerNamePos - startDbNamePos - 1;
					databaseName_ = ALLOC_NEW(alloc_) char[len + 1];
					memcpy(databaseName_, fullPathName_ + startDbNamePos, len);
					databaseName_[len] = '\0';
				}
			}
			else {
				size_t len = strlen(undefName) + 1;
				databaseName_ = ALLOC_NEW(alloc_) char[len];
				memcpy(databaseName_, undefName, len);
			}
		}

		return reinterpret_cast<const char *>(databaseName_);
	}
	const char *getContainerName() {
		if (containerName_ == NULL) {
			const char *undefName = "#Unknown";
			if (strlen(fullPathName_) != strlen(undefName) ||
				strncmp(fullPathName_, undefName, strlen(undefName)) != 0) {
				size_t startDbNamePos = 0;
				size_t startContainerNamePos = 0;
				getFullPathNamePosition(fullPathName_, strlen(fullPathName_),
					startDbNamePos, startContainerNamePos);

				size_t len = strlen(fullPathName_) - startContainerNamePos;
				containerName_ = ALLOC_NEW(alloc_) char[len + 1];
				memcpy(
					containerName_, fullPathName_ + startContainerNamePos, len);
				containerName_[len] = '\0';
			}
			else {
				size_t len = strlen(undefName) + 1;
				containerName_ = ALLOC_NEW(alloc_) char[len];
				memcpy(containerName_, undefName, len);
			}
		}
		return reinterpret_cast<const char *>(containerName_);
	}

private:
	void createFullPathName(
		const DatabaseId dbId, const char *dbName, const char *containerName) {
		assert(strcmp(dbName, "") != 0);
		util::NormalOStringStream stream;
		if (strcmp(dbName, GS_PUBLIC) != 0) {
			if (strcmp(dbName, GS_SYSTEM) != 0) {
				stream << dbId << ":";
			}
			stream << dbName << ".";
		}
		stream << containerName;

		size_t len = strlen(stream.str().c_str()) + 1;
		fullPathName_ = ALLOC_NEW(alloc_) char[len];
		memcpy(fullPathName_, stream.str().c_str(), len);
	}

	static void getFullPathNamePosition(const char *fullPathName,
		const size_t fullPathLen, size_t &startDbNamePos,
		size_t &startContainerNamePos) {
		startDbNamePos = 0;
		for (size_t i = 0; i < fullPathLen; i++) {
			if (fullPathName[i] == ':' && i != fullPathLen - 1) {
				startDbNamePos = i + 1;
				break;
			}
		}
		startContainerNamePos = 0;
		for (size_t i = 0; i < fullPathLen; i++) {
			if (fullPathName[i] == '.' && i != fullPathLen - 1) {
				startContainerNamePos = i + 1;
				break;
			}
		}
	}

	util::StackAllocator &alloc_;
	char *fullPathName_;
	DatabaseId databaseId_;
	char *databaseName_;
	char *containerName_;
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
	DataStore(const ConfigTable &configTable, ChunkManager *chunkManager);
	~DataStore();

	bool isRestored(PartitionId pId) const;

	bool isUndoCompleted(PartitionId pId) const;

	void setUndoCompleted(PartitionId pId);


	uint64_t getContainerCount(TransactionContext &txn, PartitionId pId,
		const DatabaseId dbId, ContainerCondition &condition);
	void getContainerNameList(TransactionContext &txn, PartitionId pId,
		int64_t start, ResultSize limit, const DatabaseId dbId,
		ContainerCondition &condition, util::XArray<util::String *> &nameList);
	BaseContainer *putContainer(TransactionContext &txn, PartitionId pId,
		ExtendedContainerName &extContainerName, ContainerType containerType,
		uint32_t schemaSize, const uint8_t *containerSchema, bool isEnable,
		PutStatus &status);

	void dropContainer(TransactionContext &txn, PartitionId pId,
		ExtendedContainerName &extContainerName, ContainerType containerType);
	BaseContainer *getContainer(TransactionContext &txn, PartitionId pId,
		ExtendedContainerName &extContainerName, ContainerType containerType);
	BaseContainer *getContainer(TransactionContext &txn, PartitionId pId,
		ContainerId containerId,
		ContainerType containerType);  
	Collection *getCollection(
		TransactionContext &txn, PartitionId pId, ContainerId containerId);
	TimeSeries *getTimeSeries(
		TransactionContext &txn, PartitionId pId, ContainerId containerId);

	BaseContainer *getContainerForRestore(TransactionContext &txn,
		PartitionId pId, OId oId, uint8_t containerType);  

	ContainerType getContainerType(
		TransactionContext &txn, ContainerId containerId) const;

	BtreeMap *getSchemaMap(TransactionContext &txn, PartitionId pId);
	void removeColumnSchema(
		TransactionContext &txn, PartitionId pId, OId schemaOId);

	BtreeMap *getTriggerMap(TransactionContext &txn, PartitionId pId);
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

	void restartPartition(
		TransactionContext &txn, ClusterService *clusterService);

	void dumpPartition(TransactionContext &txn, PartitionId pId, DumpType type,
		const char *fileName);
	std::string dump(TransactionContext &txn, PartitionId pId);
	void dumpContainerIdTable(PartitionId pId);

	ResultSet *createResultSet(TransactionContext &txn, ContainerId containerId,
		SchemaVersionId versionId, int64_t emNow);

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
#if defined(__GXX_EXPERIMENTAL_CXX0X__) || __cplusplus >= 201103L
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
		static const int32_t PADDING_SIZE = 464;
		OId metaMapOId_;
		OId schemaMapOId_;
		OId triggerMapOId_;
		uint64_t maxContainerId_;
		ChunkKey chunkKey_;
		uint32_t padding0_;
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

	size_t resultSetMemoryLimit_;
	std::vector<ResultSetId> resultSetIdList_;
	util::FixedSizeAllocator<util::Mutex> resultSetPool_;
	util::StackAllocator **resultSetAllocator_;  
	util::ExpirableMap<ResultSetId, ResultSet, int64_t,
		ResultSetIdHash>::Manager **resultSetMapManager_;
	util::ExpirableMap<ResultSetId, ResultSet, int64_t, ResultSetIdHash> *
		*resultSetMap_;
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


	void setUnrestored(PartitionId pId);
	void setRestored(PartitionId pId);

	void getColumnNameList(TransactionContext &txn, ColumnInfo *schema,
		uint32_t columnCount,
		util::XArray<const util::String *> &columnNameList);
	void insertColumnSchema(TransactionContext &txn, PartitionId pId,
		MessageSchema *messageSchema, int64_t schemaHashKey, OId &schemaOId);
	OId getColumnSchemaId(TransactionContext &txn, PartitionId pId,
		MessageSchema *messageSchema, int64_t schemaHashKey);

	BaseContainer *createContainer(TransactionContext &txn, PartitionId pId,
		ExtendedContainerName &extContainerName, ContainerType containerType,
		MessageSchema *messageSchema);
	void changeContainerSchema(TransactionContext &txn, PartitionId pId,
		BaseContainer *&container, MessageSchema *messageSchema,
		util::XArray<uint32_t> &copyColumnMap);


	static class ConfigSetUpHandler : public ConfigTable::SetUpHandler {
		virtual void operator()(ConfigTable &config);
	} configSetUpHandler_;
};

#endif
