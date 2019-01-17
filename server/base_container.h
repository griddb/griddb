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
	@brief Definition of Container base class
*/
#ifndef BASE_CONTAINER_H_
#define BASE_CONTAINER_H_

#include "util/container.h"
#include "util/trace.h"
#include "schema.h"
#include "data_store.h"
#include "btree_map.h"
#include "hash_map.h"
#include "rtree_map.h"


UTIL_TRACER_DECLARE(BASE_CONTAINER);

class MessageSchema;
class ResultSet;
class ContainerRowScanner;
class ArchiveHandler;


/*!
	@brief Container base class
*/
class BaseContainer : public BaseObject {
	friend class ValueMap;
public:  
	/*!
		@brief Container base format
	*/
	struct BaseContainerImage {
		ContainerType containerType_;
		uint8_t status_;  
		uint16_t normalRowArrayNum_;  
		uint32_t versionId_;		  
		TablePartitioningVersionId tablePartitioningVersionId_;
		ContainerId containerId_;
		OId containerNameOId_;  
		union {
			OId rowIdMapOId_;		   
			OId subContainerListOId_;  
		};
		OId mvccMapOId_;  
		OId columnSchemaOId_;
		OId indexSchemaOId_;
		OId triggerListOId_;
		OId lastLsn_;  
		uint64_t rowNum_;
		Timestamp startTime_;	
	};


	class ValueMap {
	public:
		ValueMap(TransactionContext &txn, BaseContainer *container, IndexData &indexData)
			: container_(container), valueMap_(NULL), nullMap_(NULL),
			alloc_(txn.getDefaultAllocator()), indexData_(indexData) {
		}
		~ValueMap() {
			ALLOC_DELETE((alloc_), valueMap_);
			ALLOC_DELETE((alloc_), nullMap_);
		}
		BaseIndex *getValueMap(TransactionContext &txn, bool forNull) {
			if (forNull) {
				if (nullMap_ == NULL) {
					nullMap_ = container_->getIndex(txn, indexData_, forNull);
				}
				return nullMap_;
			} else {
				if (valueMap_ == NULL) {
					valueMap_ = container_->getIndex(txn, indexData_, forNull);
				}
				return valueMap_;
			}
		};
		BaseIndex *putValueMap(TransactionContext &txn, bool forNull) {
			if (forNull) {
				if (nullMap_ == NULL) {
					if (indexData_.oIds_.nullOId_ == UNDEF_OID) {
						container_->createNullIndexData(txn, indexData_);
					}
					nullMap_ = container_->getIndex(txn, indexData_, forNull);
				}
				return nullMap_;
			} else {
				if (valueMap_ == NULL) {
					valueMap_ = container_->getIndex(txn, indexData_, forNull);
				}
				return valueMap_;
			}
		};
		void updateIndexData(TransactionContext &txn) {
			if (valueMap_ != NULL) {
				indexData_.oIds_.mainOId_ = valueMap_->getBaseOId();
			}
			if (nullMap_ != NULL) {
				indexData_.oIds_.nullOId_ = nullMap_->getBaseOId();
			}
			container_->updateIndexData(txn, indexData_);
		}
		MapType getMapType() {
			return indexData_.mapType_;
		}
		template <typename T>
		int32_t search(TransactionContext &txn, typename T::SearchContext &sc,
			util::XArray<OId> &oIdList, OutputOrder outputOrder);

	private:
		BaseContainer *container_;
		BaseIndex *valueMap_;
		BaseIndex *nullMap_;
		util::StackAllocator &alloc_;
		IndexData &indexData_;
	};

	static const Timestamp EXPIRE_MARGIN = 1000;  
	static const int32_t EXPIRE_DIVIDE_UNDEFINED_NUM = -1;
	static const uint16_t EXPIRE_DIVIDE_DEFAULT_NUM = 8;
	/*!
		@brief ExpirationInfo format
	*/
	struct ExpirationInfo {
		int64_t duration_;	 
		int32_t elapsedTime_;  
		uint16_t dividedNum_;  
		TimeUnit timeUnit_;	
		uint8_t padding_;	  
		ExpirationInfo() {
			duration_ = INT64_MAX;  
			elapsedTime_ = -1;		
			timeUnit_ = TIME_UNIT_DAY;
			dividedNum_ = EXPIRE_DIVIDE_DEFAULT_NUM;
			padding_ = 0;
		}
		bool operator==(const ExpirationInfo &b) const {
			if (memcmp(this, &b, sizeof(ExpirationInfo)) == 0) {
				return true;
			}
			else {
				return false;
			}
		}
		int64_t getHashVal() const {
			int64_t hashVal = 0;
			hashVal += duration_;
			hashVal += elapsedTime_;
			hashVal += dividedNum_;
			hashVal += timeUnit_;
			return hashVal;
		}
	};

	/*!
		@brief ContainerExpirationInfo format
	*/
	struct ContainerExpirationInfo {
		ExpirationInfo info_;
		int64_t interval_;
		ContainerExpirationInfo() : info_(), interval_(0) {}
		bool operator==(const ContainerExpirationInfo &b) const {
			if (interval_ == b.interval_ && info_ == b.info_) {
				return true;
			}
			else {
				return false;
			}
		}
		int64_t getHashVal() const {
			int64_t hashVal = interval_ + info_.getHashVal();
			return hashVal;
		}
	};

	/*!
		@brief Information for archive
	*/
	struct ArchiveInfo {
		ArchiveInfo() : start_(0), end_(0), expired_(0), erasable_(0), 
			rowIdMapOId_(UNDEF_OID), mvccMapOId_(UNDEF_OID) {}
		Timestamp start_;
		Timestamp end_;
		Timestamp expired_;
		Timestamp erasable_;
		OId rowIdMapOId_;
		OId mvccMapOId_;
	};

	enum RowArrayType {
		ROW_ARRAY_GENERAL,
		ROW_ARRAY_PLAIN,
	};
	template<RowArrayType> struct RowArrayAccessType {};
	typedef RowArrayAccessType<ROW_ARRAY_GENERAL> RowArrayGeneralType;
	typedef RowArrayAccessType<ROW_ARRAY_PLAIN> RowArrayPlainType;

	class RowArrayStorage : public BaseObject {
	public:
		RowArrayStorage(PartitionId pId, ObjectManager &objectManager) : BaseObject(pId, objectManager) {};
	};

	class RowCache {
	public:
		RowCache();
		RowCache(TransactionContext &txn, BaseContainer *container);
		~RowCache();
		BaseObject &getFrontFieldObject() {
			return frontFieldCache_.get()->baseObject_;
		}

	public:
		struct FieldCache {
			explicit FieldCache(PartitionId partitionId, ObjectManager &objectManager) :
				baseObject_(partitionId, objectManager),
				addr_(NULL) {
			}

			BaseObject baseObject_;
			const uint8_t *addr_;
		};

		typedef util::XArray<FieldCache> FieldCacheList;
		union FieldCacheStorage {
			inline FieldCache* get() { return static_cast<FieldCache*>(addr()); }
			inline void* addr() { return this; }

			uint8_t bytesValue_[sizeof(FieldCache)];
			void *ptrValue_;
			uint64_t longValue_;
		} frontFieldCache_;
		size_t lastCachedField_;
		FieldCacheList fieldCacheList_;
	};

	class RowArray;

	template<typename Container, RowArrayType rowArrayType>
	class RowArrayImpl;

public:  
public:  
	virtual ~BaseContainer() {
		ALLOC_DELETE((alloc_), commonContainerSchema_);
		ALLOC_DELETE((alloc_), columnSchema_);
		ALLOC_DELETE((alloc_), indexSchema_);
		ALLOC_DELETE((alloc_), rowArrayCache_);
	}
	virtual void initialize(TransactionContext &txn) = 0;
	virtual bool finalize(TransactionContext &txn) = 0;

	virtual void set(TransactionContext &txn, const FullContainerKey &containerKey,
		ContainerId containerId, OId columnSchemaOId,
		MessageSchema *containerSchema) = 0;

	virtual void createIndex(TransactionContext &txn, const IndexInfo &indexInfo,
		IndexCursor& indexCursor,
		bool isIndexNameCaseSensitive = false) = 0;
	virtual void continueCreateIndex(TransactionContext& txn, 
		IndexCursor& indexCursor) = 0;
	IndexCursor getIndexCursor(TransactionContext& txn);
	ContainerCursor getContainerCursor(TransactionContext& txn);
	virtual void dropIndex(TransactionContext &txn, IndexInfo &indexInfo,
		bool isIndexNameCaseSensitive = false) = 0;
	void getIndexInfoList(
		TransactionContext &txn, util::Vector<IndexInfo> &indexInfoList);
	void getContainerInfo(TransactionContext &txn,
		util::XArray<uint8_t> &containerSchema, bool optionIncluded = true, bool internalOptionIncluded = true);
	virtual util::String getBibInfo(TransactionContext &txn, const char* dbName) = 0;
	virtual void getErasableList(TransactionContext &txn, Timestamp erasableTimeLimit, util::XArray<ArchiveInfo> &list) = 0;
	virtual ExpireType getExpireType() const = 0;
	void putRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowData, RowId &rowId, DataStore::PutStatus &status,
		PutRowOption putRowOption) {
		bool rowIdSecified = false;
		putRow(txn, rowSize, rowData, rowId, rowIdSecified, status,
			putRowOption);
	}
	void redoPutRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowData, RowId &rowId, DataStore::PutStatus &status,
		PutRowOption putRowOption) {
		bool rowIdSecified = true;
		putRow(txn, rowSize, rowData, rowId, rowIdSecified, status,
			putRowOption);
	}
	void putRowList(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowData, uint64_t numRow, DataStore::PutStatus &status);
	virtual void deleteRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowKey, RowId &rowId, bool &existing) = 0;
	virtual void deleteRow(
		TransactionContext &txn, RowId rowId, bool &existing) = 0;
	virtual void updateRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowData, RowId rowId, DataStore::PutStatus &status) = 0;
	virtual void redoDeleteRow(
		TransactionContext &txn, RowId rowId, bool &existing) = 0;
	virtual void abort(TransactionContext &txn) = 0;
	virtual void commit(TransactionContext &txn) = 0;
	void changeSchema(TransactionContext &txn,
		BaseContainer &newContainer, util::XArray<uint32_t> &copyColumnMap);
	void changeProperty(TransactionContext& txn, OId columnSchemaOId);
	void makeCopyColumnMap(TransactionContext &txn,
		MessageSchema *messageSchema, util::XArray<uint32_t> &copyColumnMap,
		DataStore::SchemaState &schemaState);
	void changeNullStats(TransactionContext& txn, uint32_t oldColumnNum);

	virtual bool hasUncommitedTransaction(TransactionContext &txn) = 0;

	virtual void searchRowIdIndex(TransactionContext &txn,
		BtreeMap::SearchContext &sc, util::XArray<OId> &resultList,
		OutputOrder order) = 0;
	virtual void searchRowIdIndex(TransactionContext &txn, uint64_t start,
		uint64_t limit, util::XArray<RowId> &rowIdList,
		util::XArray<OId> &resultList, uint64_t &skipped) = 0;

	void searchColumnIdIndex(TransactionContext &txn,
		BtreeMap::SearchContext &sc, util::XArray<OId> &oIdList,
		OutputOrder outputOrder);
	void searchColumnIdIndex(TransactionContext &txn,
		HashMap::SearchContext &sc, util::XArray<OId> &resultList);
	void searchColumnIdIndex(TransactionContext &txn,
		RtreeMap::SearchContext &sc, util::XArray<OId> &resultList);
	void searchColumnIdIndex(TransactionContext &txn,
		BtreeMap::SearchContext &sc, util::XArray<OId> &normalRowList,
		util::XArray<OId> &mvccRowList);

	void getRowList(TransactionContext &txn, util::XArray<OId> &oIdList,
		ResultSize limit, ResultSize &resultNum,
		MessageRowStore *messageRowStore, bool isWithRowId,
		ResultSize startPos);

	void putTrigger(TransactionContext &txn, const TriggerInfo &info);
	void deleteTrigger(TransactionContext &txn, const char *name);
	void getTriggerList(
		TransactionContext &txn, util::XArray<const uint8_t *> &triggerList);
	void updateTrigger(TransactionContext &txn,  OId oId,
		const util::XArray<const util::String *> &oldColumnNameList,
		const util::XArray<const util::String *> &newColumnNameList);

	void getLockRowIdList(TransactionContext &txn, ResultSet &resultSet,
		util::XArray<RowId> &idList);
	virtual void lockRowList(
		TransactionContext &txn, util::XArray<RowId> &rowIdList) = 0;

	void getRowIdList(TransactionContext &txn, util::XArray<OId> &oIdList,
		util::XArray<RowId> &idList);
	void getOIdList(TransactionContext &txn, uint64_t start, uint64_t limit,
		uint64_t &skipped, util::XArray<RowId> &idList,
		util::XArray<OId> &oIdList);

	virtual RowId getMaxRowId(TransactionContext &txn) = 0;

	OId getContainerKeyOId() {
		return baseContainerImage_->containerNameOId_;
	}

	FullContainerKey getContainerKey(TransactionContext &txn) {
		if (containerKeyCursor_ .getBaseOId() == UNDEF_OID) {
			if (baseContainerImage_->containerNameOId_ != UNDEF_OID) {
				containerKeyCursor_.load(baseContainerImage_->containerNameOId_);
			}
			else {
				GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_NAME_INVALID, "container key not exist");
			}
		}
		return containerKeyCursor_.getKey();
	}

	BaseIndex *getIndex(
		TransactionContext &txn, const IndexData &indexData, bool forNull = false) {
		return indexSchema_->getIndex(txn, indexData, forNull, this);
	}
	BaseIndex *getIndex(
		TransactionContext &txn, MapType mapType, ColumnId columnId, bool forNull = false) {
		bool withUncommitted = true;
		IndexData indexData;
		if (getIndexData(txn, columnId, mapType, withUncommitted, indexData)) {
			return getIndex(txn, indexData, forNull);
		}
		else {
			return NULL;
		}
	}

	uint32_t getColumnNum() const {
		return columnSchema_->getColumnNum();
	}
	uint32_t getRowFixedColumnSize() const {
		return columnSchema_->getRowFixedColumnSize();
	}
	ColumnInfo *getColumnInfoList() const {
		return columnSchema_->getColumnInfoList();
	}
	ColumnInfo &getColumnInfo(uint32_t columnId) const {
		return columnSchema_->getColumnInfo(columnId);
	}
	void getColumnInfo(TransactionContext &txn, ObjectManager &objectManager,
		const char *name, uint32_t &columnId, ColumnInfo *&columnInfo,
		bool isCaseSensitive) const {
		return columnSchema_->getColumnInfo(
			txn, objectManager, name, columnId, columnInfo, isCaseSensitive);
	}
	void getKeyColumnIdList(util::XArray<ColumnId> &keyColumnIdList) {
		columnSchema_->getKeyColumnIdList(keyColumnIdList);
	}
	uint32_t getVariableColumnNum() const {
		return columnSchema_->getVariableColumnNum();
	}
	bool definedRowKey() const {
		return columnSchema_->definedRowKey();
	}
	bool hasIndex(
		TransactionContext &txn, ColumnId columnId, MapType mapType) const {
		return indexSchema_->hasIndex(txn, columnId, mapType);
	}
	bool hasIndex(IndexTypes indexType, MapType mapType) const {
		return indexSchema_->hasIndex(indexType, mapType);
	}
	IndexTypes getIndexTypes(TransactionContext &txn, ColumnId columnId) {
		return indexSchema_->getIndexTypes(txn, columnId);
	}
	bool isFirstColumnAdd() {
		return columnSchema_->isFirstColumnAdd();
	}
	ContainerAttribute getAttribute() const {
		ContainerAttribute *attribute =
			commonContainerSchema_->get<ContainerAttribute>(
				META_TYPE_ATTRIBUTES);
		if (attribute == NULL) {
			return CONTAINER_ATTR_SINGLE;
		}
		else {
			return *attribute;
		}
	}
	DataStore *getDataStore() {
		return dataStore_;
	}


	void getNullsStats(util::XArray<uint8_t> &nullsList) const {
		uint16_t limitSize = sizeof(int64_t);
		if (!isNullsStatsStatus() && indexSchema_->getNullbitsSize() > limitSize) {
			uint16_t diffSize = indexSchema_->getNullbitsSize() - limitSize;
			nullsList.push_back(indexSchema_->getNullsStats(), limitSize);
			for (uint32_t i = 0; i < diffSize; i++) {
				nullsList.push_back(0xFF);
			}
			for (ColumnId columnId = limitSize * 8; columnId < getColumnNum(); columnId++) {
				if (getColumnInfo(columnId).isNotNull()) {
					RowNullBits::setNotNull(nullsList.data(), columnId);
				}
			}
		} else {
			nullsList.push_back(indexSchema_->getNullsStats(), 
				indexSchema_->getNullbitsSize());
		}
	}

	virtual AllocateStrategy calcMapAllocateStrategy() const = 0;
	virtual AllocateStrategy calcRowAllocateStrategy() const = 0;
	AllocateStrategy calcMetaAllocateStrategy() const {
		return AllocateStrategy(ALLOCATE_META_CHUNK);
	}

	void handleUpdateError(
		TransactionContext &txn, std::exception &e, ErrorCode errorCode);
	void handleSearchError(
		TransactionContext &txn, std::exception &e, ErrorCode errorCode);
	void handleInvalidError(
		TransactionContext &txn, SystemException &e, ErrorCode errorCode);

	virtual void getActiveTxnList(
		TransactionContext &txn, util::Set<TransactionId> &txnList) = 0;
	void archive(TransactionContext &txn, ArchiveHandler *handler, ResultSize preReadNum);

	virtual bool validate(TransactionContext &txn, std::string &errorMessage);
	virtual std::string dump(TransactionContext &txn);
	std::string dump(TransactionContext &txn, util::XArray<OId> &oIdList);

public:  
	ContainerId getContainerId() const {
		return baseContainerImage_->containerId_;
	}

	uint32_t getVersionId() const {
		return baseContainerImage_->versionId_;
	}

	int64_t getInitSchemaStatus() const {
		uint32_t columnNum, varColumnNum, rowFixedColumnSize;
		columnSchema_->getFirstSchema(columnNum, varColumnNum, rowFixedColumnSize);
		int64_t status = ColumnSchema::convertToInitSchemaStatus(columnNum, varColumnNum, rowFixedColumnSize);
		return status;
	}

	ColumnSchemaId getColumnSchemaId() const {
		return baseContainerImage_->columnSchemaOId_;
	}

	uint32_t getRowSize() const {
		return rowImageSize_;
	}
	uint32_t getRowFixedDataSize() const {
		return rowFixedDataSize_;
	}
	ContainerType getContainerType() const {
		return baseContainerImage_->containerType_;
	}
	uint64_t getRowNum() const {
		return (0x7FFFFFFFFFFFFFFFULL & baseContainerImage_->rowNum_);
	}
	uint16_t getNormalRowArrayNum() const {
		return baseContainerImage_->normalRowArrayNum_;
	}

	uint16_t getSmallRowArrayNum() const {
		if (getNormalRowArrayNum() < SMALL_ROW_ARRAY_MAX_SIZE) {
			return getNormalRowArrayNum();
		} else {
			return SMALL_ROW_ARRAY_MAX_SIZE;
		}
	}

	const char *getAffinity() const {
		return commonContainerSchema_->get<char>(META_TYPE_AFFINITY);
	}

	bool isInvalid() const {
		return (baseContainerImage_->status_ & CONTAINER_INVALID_BIT) != 0;
	}

	void setContainerInvalid() {
		baseContainerImage_->status_ |= CONTAINER_INVALID_BIT;
	}

	void setCompressionErrorMode(bool isError) {
		isCompressionErrorMode_ = isError;
	}
	bool isCompressionErrorMode() {
		return isCompressionErrorMode_;
	}
	void setVersionId(uint32_t versionId) {
		baseContainerImage_->versionId_ = versionId;
	}
	OId getTriggerOId() const {
		return baseContainerImage_->triggerListOId_;
	}
	void setTriggerOId(OId oId) {
		baseContainerImage_->triggerListOId_ = oId;
	}

	void updateNullsStats(const uint8_t *nullbits) {
		indexSchema_->updateNullsStats(nullbits);
	}
	uint32_t getNullbitsSize() const {
		return indexSchema_->getNullbitsSize();
	}
	void setNullStats(ColumnId columnId) {
		indexSchema_->setNullStats(columnId);
	}


	virtual uint32_t getRealColumnNum(TransactionContext &txn) = 0;
	virtual ColumnInfo* getRealColumnInfoList(TransactionContext &txn) = 0;
	virtual uint32_t getRealRowSize(TransactionContext &txn) = 0;
	virtual uint32_t getRealRowFixedDataSize(TransactionContext &txn) = 0;


	static util::String getContainerName(util::StackAllocator &alloc,
		const FullContainerKey &containerKey) {
		util::String containerName(alloc);
		containerKey.toString(alloc, containerName);
		return containerName;
	}
	bool isExpired(TransactionContext &txn) {
		Timestamp currentTime = txn.getStatementStartTime().getUnixTime();
		Timestamp lastExpiredTime = getDataStore()->getLatestExpirationCheckTime(txn.getPartitionId());

		if (currentTime < lastExpiredTime) {
			currentTime = lastExpiredTime;
		}

		Timestamp expirationTime = getContainerExpirationTime();
		if (currentTime > expirationTime) {
			return true;
		}
		return false;
	}
	Timestamp getContainerExpirationTime() const {
		int64_t duration = getContainerExpirationDutation();
		if (duration != INT64_MAX) {
			Timestamp endTime = getContainerExpirationEndTime();
			Timestamp expirationTime = getContainerExpirationEndTime() + duration;
			if (endTime < expirationTime) {
				return expirationTime;
			} else {
				return MAX_TIMESTAMP;
			}
		}
		return MAX_TIMESTAMP;
	}
	static Timestamp calcErasableTime(Timestamp baseTime, int64_t duration) {
		ExpireIntervalCategoryId expireCategoryId = DEFAULT_EXPIRE_CATEGORY_ID;
		ChunkKey chunkKey = UNDEF_CHUNK_KEY;
		calcChunkKey(baseTime, duration, expireCategoryId, chunkKey);
		Timestamp erasableTime = DataStore::convertChunkKey2Timestamp(chunkKey);
		return erasableTime;
	}

	RowArray *getCacheRowArray(TransactionContext &txn);

protected:  
	/*!
		@brief Mode of operation of put
	*/
	enum PutMode { UNDEFINED_STATUS, APPEND, UPDATE, INSERT, NOT_EXECUTED };
	/*!
		@brief Status of Mvcc map, with regard to the transactions
	*/
	enum MvccStatus {
		NO_ROW_TRANSACTION = 0,			
		EXCLUSIVE = 1,					
		NOT_EXCLUSIVE_CREATE_EXIST = 2, 
		NOT_EXCLUSIVE = 3,				
		UNKNOWN
	};
	static const uint16_t ROW_ARRAY_LIMIT_SIZE = 1024;
	static const uint16_t ROW_ARRAY_MAX_ESTIMATE_SIZE = ROW_ARRAY_LIMIT_SIZE / 2;
	static const uint16_t ROW_ARRAY_MAX_SIZE = 50;  
	static const uint16_t SMALL_ROW_ARRAY_MAX_SIZE =
		10;  
	static const uint16_t MVCC_ROW_ARRAY_MAX_SIZE = 1;

	static const uint8_t CONTAINER_INVALID_BIT =
		0x1;  
	static const uint8_t ALTER_CONTAINER_BIT =
		0x2; 
	static const uint8_t NULLS_STATS_BIT =
		0x4; 

	/*!
		@brief Data for sort
	*/
	struct SortKey {
	public:
		uint8_t *data() const {
			return data_;
		}
		OId getOId() const {
			return oId_;
		}
		void set(TransactionContext &txn, const ColumnType columnType,
			uint8_t *srcData, OId oId) {
			oId_ = oId;
			if (srcData == NULL) {
				data_ = NULL;
			} else {
			switch (columnType) {
			case COLUMN_TYPE_BOOL:
			case COLUMN_TYPE_BYTE:
			case COLUMN_TYPE_SHORT:
			case COLUMN_TYPE_INT:
			case COLUMN_TYPE_LONG:
			case COLUMN_TYPE_FLOAT:
			case COLUMN_TYPE_DOUBLE:
			case COLUMN_TYPE_TIMESTAMP:
			case COLUMN_TYPE_OID: {
				data_ = reinterpret_cast<uint8_t *>(
					txn.getDefaultAllocator().allocate(
						FixedSizeOfColumnType[columnType]));
				memcpy(data_, srcData, FixedSizeOfColumnType[columnType]);
			} break;
			case COLUMN_TYPE_STRING: {
				StringCursor stringCursor(srcData);
				data_ = reinterpret_cast<uint8_t *>(
					txn.getDefaultAllocator().allocate(
						stringCursor.getObjectSize()));
				memcpy(data_, srcData, stringCursor.getObjectSize());
			} break;
			default:
				GS_THROW_SYSTEM_ERROR(
					GS_ERROR_DS_TYPE_INVALID, "Invalid sort key");
				break;
			}
			}
		}

	private:
		uint8_t *data_;
		OId oId_;
	};

	/*!
		@brief Compare method for sort
	*/
	struct SortPred {
		const Operator *op_;
		TransactionContext *txn_;
		const ColumnType columnType_;
		const bool isNullLast_;
		SortPred(TransactionContext &txn, const Operator *op,
			const ColumnType columnType, bool isNullLast)
			: op_(op), txn_(&txn), columnType_(columnType),
			isNullLast_(isNullLast) {}
		bool operator()(const SortKey &v1, const SortKey &v2) const {
			if (v1.data() == NULL) {
				if (v2.data() == NULL) {
					return false;
				} else {
					return !isNullLast_;
				}
			}
			else if (v2.data() == NULL) {
				return isNullLast_;
			} else {
				Value value1_;
				Value value2_;
				value1_.set(v1.data(), columnType_);
				value2_.set(v2.data(), columnType_);
				return (*op_)(*txn_, value1_.data(), value1_.size(), value2_.data(),
					value2_.size());
			}
		}
	};

	enum ToRowMode {
		TO_MVCC,
		TO_NORMAL,
		TO_UNDEF
	};

	void setRsNotifier(ToRowMode mode) {
		rsNotifier_.set(getContainerId(), mode);
	}
	void resetRsNotifier() {
		rsNotifier_.reset();
	}
	class RsNotifier {
	public:
		RsNotifier(DataStore &dataStore) : dataStore_(dataStore), containerId_(UNDEF_CONTAINERID), mode_(TO_UNDEF) {
		}
		void set(ContainerId containerId, ToRowMode mode) {
			assert(mode != TO_UNDEF);
			assert(containerId != UNDEF_CONTAINERID);
			assert(mode_ == TO_UNDEF);
			assert(containerId_ == UNDEF_CONTAINERID);
			mode_ = mode;
			containerId_ = containerId;
		}
		void reset() {
			assert(mode_ != TO_UNDEF);
			assert(containerId_ != UNDEF_CONTAINERID);
			mode_ = TO_UNDEF;
			containerId_ = UNDEF_CONTAINERID;
		}
		void addUpdatedRow(PartitionId pId, RowId rowId, OId oId);
	protected:
		DataStore &dataStore_;
		ContainerId containerId_;
		ToRowMode mode_;
	};

	class RowScope {
	public:
		explicit RowScope(BaseContainer &container, ToRowMode mode) : container_(container) {
			container_.setRsNotifier(mode);
		}
		~RowScope() {
			container_.resetRsNotifier();
		}
	private:
		BaseContainer &container_;
	};
protected:  
	BaseContainerImage *baseContainerImage_;
	ColumnSchema *columnSchema_;
	ShareValueList *commonContainerSchema_;
	IndexSchema *indexSchema_;
	uint32_t rowImageSize_;
	uint32_t rowFixedDataSize_;
	AllocateStrategy metaAllocateStrategy_;
	AllocateStrategy mapAllocateStrategy_;
	AllocateStrategy rowAllocateStrategy_;
	MvccStatus exclusiveStatus_;
	util::StackAllocator &alloc_;
	DataStore *dataStore_;
	RsNotifier rsNotifier_;
	FullContainerKeyCursor containerKeyCursor_;
	bool isCompressionErrorMode_;  
	RowArray *rowArrayCache_;

	static const int8_t NULL_VALUE;
protected:  
	BaseContainer(TransactionContext &txn, DataStore *dataStore, OId oId)
		: BaseObject(
			  txn.getPartitionId(), *(dataStore->getObjectManager()), oId),
		  exclusiveStatus_(UNKNOWN),
		  alloc_(txn.getDefaultAllocator()),
		  dataStore_(dataStore),
		  rsNotifier_(*dataStore),
		  containerKeyCursor_(txn.getPartitionId(), *(dataStore->getObjectManager())),
		  isCompressionErrorMode_(false), rowArrayCache_(NULL) {
		baseContainerImage_ = getBaseAddr<BaseContainerImage *>();
		commonContainerSchema_ =
			ALLOC_NEW(txn.getDefaultAllocator()) ShareValueList(txn,
				*getObjectManager(), baseContainerImage_->columnSchemaOId_);
		columnSchema_ =
			commonContainerSchema_->get<ColumnSchema>(META_TYPE_COLUMN_SCHEMA);
		indexSchema_ = ALLOC_NEW(txn.getDefaultAllocator())
			IndexSchema(txn, *getObjectManager(),
				baseContainerImage_->indexSchemaOId_, getMetaAllcateStrategy());
	}
	BaseContainer(TransactionContext &txn, DataStore *dataStore)
		: BaseObject(txn.getPartitionId(), *(dataStore->getObjectManager())),
		  baseContainerImage_(NULL),
		  columnSchema_(NULL),
		  commonContainerSchema_(NULL),
		  indexSchema_(NULL),
		  rowImageSize_(0),
		  rowFixedDataSize_(0),
		  exclusiveStatus_(UNKNOWN),
		  alloc_(txn.getDefaultAllocator()),
		  dataStore_(dataStore),
		  rsNotifier_(*dataStore),
		  containerKeyCursor_(txn.getPartitionId(), *(dataStore->getObjectManager())),
		  isCompressionErrorMode_(false), rowArrayCache_(NULL) {}

	BaseContainer(TransactionContext &txn, DataStore *dataStore, BaseContainerImage *containerImage, ShareValueList *commonContainerSchema);

	void replaceIndexSchema(TransactionContext &txn) {
		setDirty();
		baseContainerImage_->indexSchemaOId_ = indexSchema_->getBaseOId();
	}

	BtreeMap *getRowIdMap(TransactionContext &txn) const {
		return ALLOC_NEW(txn.getDefaultAllocator())
			BtreeMap(txn, *getObjectManager(),
				baseContainerImage_->rowIdMapOId_, mapAllocateStrategy_, NULL);
	}

	BtreeMap *getMvccMap(TransactionContext &txn) const {
		return ALLOC_NEW(txn.getDefaultAllocator())
			BtreeMap(txn, *getObjectManager(), baseContainerImage_->mvccMapOId_,
				mapAllocateStrategy_, NULL);
	}

	void setCreateRowId(TransactionContext &txn, RowId rowId);

	static int64_t calcSchemaHashKey(MessageSchema *messageSchema);
	static bool schemaCheck(TransactionContext &txn,
		ObjectManager &objectManager, ShareValueList *commonContainerSchema,
		MessageSchema *messageSchema);
	static void finalizeSchema(TransactionContext &txn,
		ObjectManager &objectManager, ShareValueList *commonContainerSchema);
	static void initializeSchema(TransactionContext &txn,
		ObjectManager &objectManager, MessageSchema *messageSchema,
		const AllocateStrategy &allocateStrategy,
		util::XArray<ShareValueList::ElemData> &list, uint32_t &allocateSize,
		bool onMemory);

	static int64_t calcTriggerHashKey(util::XArray<const uint8_t *> &binary);
	static bool triggerCheck(TransactionContext &txn,
		ObjectManager &objectManager, ShareValueList *commonContainerSchema,
		util::XArray<const uint8_t *> &binary);
	static void finalizeTrigger(TransactionContext &txn,
		ObjectManager &objectManager, ShareValueList *commonContainerSchema);
	static void initializeTrigger(TransactionContext &txn,
		ObjectManager &objectManager, util::XArray<const uint8_t *> &binary,
		const AllocateStrategy &allocateStrategy,
		util::XArray<ShareValueList::ElemData> &list, uint32_t &allocateSize);

	static BaseContainerImage *makeBaseContainerImage(TransactionContext &txn, const BibInfo::Container &bibInfo);

	bool isSupportIndex(const IndexInfo &indexInfo) const;
	IndexCursor createCursor(TransactionContext &txn, const MvccRowImage &mvccImage);

	virtual void putRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowData, RowId &rowId, bool rowIdSpecified,
		DataStore::PutStatus &status, PutRowOption putRowOption) = 0;
	virtual void putRow(TransactionContext &txn,
		InputMessageRowStore *inputMessageRowStore, RowId &rowId,
		bool rowIdSpecified,
		DataStore::PutStatus &status, PutRowOption putRowOption) = 0;
	virtual void getIdList(TransactionContext &txn,
		util::XArray<uint8_t> &serializedRowList,
		util::XArray<RowId> &idList) = 0;
	virtual void lockIdList(TransactionContext &txn, util::XArray<OId> &oIdList,
		util::XArray<RowId> &idList) = 0;
	void getCommonContainerOptionInfo(util::XArray<uint8_t> &containerSchema);
	virtual void getContainerOptionInfo(
		TransactionContext &txn, util::XArray<uint8_t> &containerSchema) = 0;
	virtual util::String getBibContainerOptionInfo(TransactionContext &txn) = 0;
	virtual void checkContainerOption(MessageSchema *messageSchema,
		util::XArray<uint32_t> &copyColumnMap,
		bool &isCompletelySameSchema) = 0;
	virtual uint32_t calcRowImageSize(uint32_t rowFixedSize) = 0;
	virtual uint32_t calcRowFixedDataSize() = 0;
	uint16_t calcRowArrayNumBySize(uint32_t binarySize, uint32_t nullbitsSize);
	uint16_t calcRowArrayNum(TransactionContext& txn, bool sizeControlMode, uint16_t baseRowNum);
	uint16_t calcRowArrayNumByBaseRowNum(TransactionContext& txn, uint16_t baseRowNum, uint32_t rowArrayHeaderSize);
	uint16_t calcRowArrayNumByConfig(TransactionContext &txn, uint32_t rowArrayHeaderSize);
	template <typename R>
	void indexInsertImpl(TransactionContext &txn, IndexData &indexData,
		bool isImmediate);

	bool getKeyCondition(TransactionContext &txn, BtreeMap::SearchContext &sc,
		const Operator *&op1, const Operator *&op2) const;
	bool getKeyCondition(TransactionContext &txn, HashMap::SearchContext &sc,
		const Operator *&op1, const Operator *&op2) const;
	bool getKeyCondition(TransactionContext &txn, RtreeMap::SearchContext &sc,
		const Operator *&op1, const Operator *&op2) const;
	void getInitialSchemaStatus(uint32_t &columnNum, uint32_t &varColumnNum, uint32_t &rowFixedColumnSize) {
		return columnSchema_->getFirstSchema(columnNum, varColumnNum, rowFixedColumnSize);
	}
public:
	virtual void continueChangeSchema(TransactionContext &txn,
		ContainerCursor &containerCursor) = 0;
protected:
	template <typename R>
	void changeSchemaRecord(TransactionContext &txn,
		BaseContainer &newContainer, util::XArray<uint32_t> &copyColumnMap,
		RowId &cursor, bool isImmediate);
	void makeCopyColumnMap(TransactionContext &txn,
		BaseContainer &newContainer, util::XArray<uint32_t> &copyColumnMap);
	template <typename R>
	void getRowListImpl(TransactionContext &txn, util::XArray<OId> &oIdList,
		ResultSize limit, ResultSize &resultNum,
		MessageRowStore *messageRowStore, bool isWithRowId,
		ResultSize startPos);
	template <typename R, typename T>
	void searchColumnIdIndex(TransactionContext &txn, MapType mapType,
		typename T::SearchContext &sc, util::XArray<OId> &resultList, OutputOrder outputOrder);
	template <typename R, typename T>
	void searchColumnIdIndex(TransactionContext &txn, MapType mapType,
		typename T::SearchContext &sc, util::XArray<OId> &normalRowList,
		util::XArray<OId> &mvccRowList, OutputOrder outputOrder,
		bool ignoreTxnCheck);
	template <class R, class S>
	void searchMvccMap(TransactionContext &txn,
		S &sc, util::XArray<OId> &resultList, bool isCheckOnly, bool ignoreTxnCheck);
	template <class R, class S>
	void searchColumnId(TransactionContext &txn, S &sc,
		util::XArray<OId> &oIdList, util::XArray<OId> &mvccList,
		util::XArray<OId> &resultList, OutputOrder outputOrder);
	template <class R>
	void mergeRowList(TransactionContext &txn,
		const ColumnInfo &targetColumnInfo, util::XArray<OId> &inputList1,
		const bool isList1Sorted, util::XArray<OId> &inputList2,
		const bool isList2Sorted, util::XArray<OId> &mergeList,
		OutputOrder outputOrder);
	bool checkScColumnKey(TransactionContext &txn, BtreeMap::SearchContext &sc,
		const Value &value, const Operator *op1, const Operator *op2);
	bool checkScColumnKey(TransactionContext &txn, HashMap::SearchContext &sc,
		const Value &value, const Operator *op1, const Operator *op2);
	bool checkScColumnKey(TransactionContext &txn, RtreeMap::SearchContext &sc,
		const Value &value, const Operator *op1, const Operator *op2);
	util::String getBibInfoImpl(TransactionContext &txn, const char* dbName, uint64_t pos, bool isEmptyId);
	void getActiveTxnListImpl(
		TransactionContext &txn, util::Set<TransactionId> &txnList);
	template <typename R>
	std::string dumpImpl(TransactionContext &txn);
	template <typename R>
	bool validateImpl(TransactionContext &txn, std::string &errorMessage,
		RowId &preRowId, uint64_t &countRowNum, bool isCheckRowRate = false);

	template <typename R>
	void getRowIdListImpl(TransactionContext &txn, util::XArray<OId> &oIdList,
		util::XArray<RowId> &rowIdList);
	virtual void setDummyMvccImage(TransactionContext &txn) = 0;

	virtual void checkExclusive(TransactionContext &txn) = 0;

	inline bool isNoRowTransaction() const {
		return exclusiveStatus_ == NO_ROW_TRANSACTION;
	}
	inline bool isExclusive() const {
		return exclusiveStatus_ <= EXCLUSIVE;
	}
	inline bool isExclusiveUpdate() const {
		return (isExclusive() ||
				exclusiveStatus_ == NOT_EXCLUSIVE_CREATE_EXIST);
	}

	inline void setExclusiveStatus(MvccStatus status) {
		exclusiveStatus_ = status;
	}
	inline MvccStatus getExclusiveStatus() {
		return exclusiveStatus_;
	}

	virtual bool getIndexData(TransactionContext &txn, ColumnId columnId,
		MapType mapType, bool withUncommitted, IndexData &indexData) const = 0;
	virtual void getIndexList(
		TransactionContext &txn, bool withUncommitted, util::XArray<IndexData> &list) const = 0;
	virtual void createNullIndexData(TransactionContext &txn, 
		IndexData &indexData) = 0;
	virtual void finalizeIndex(TransactionContext &txn) = 0;

	static AffinityGroupId calcAffnityGroupId(const char *affinityStr) {
		uint64_t hash = 0;
		const char *key = affinityStr;
		uint32_t keylen = AFFINITY_STRING_MAX_LENGTH;
		for (hash = 0; --keylen != UINT32_MAX; hash = hash * 37 + *(key)++)
			;

		AffinityGroupId groupId = static_cast<AffinityGroupId>(hash);
		return groupId;
	}

	bool isAlterContainer() const {
		return (baseContainerImage_->status_ & ALTER_CONTAINER_BIT) != 0;
	}
	void setAlterContainer() {
		baseContainerImage_->status_ |= ALTER_CONTAINER_BIT;
	}
	void resetAlterContainer() {
		baseContainerImage_->status_ &= ~ALTER_CONTAINER_BIT;
	}
	bool isNullsStatsStatus() const {
		return (baseContainerImage_->status_ & NULLS_STATS_BIT) != 0;
	}
	void setNullsStatsStatus() {
		baseContainerImage_->status_ |= NULLS_STATS_BIT;
	}

	void convertRowArraySchema(TransactionContext &txn, RowArray &rowArray, bool isMvcc);	

	void setContainerExpirationStartTime(Timestamp startTime) {
		baseContainerImage_->startTime_ = startTime;
	}
	Timestamp getContainerExpirationStartTime() const {
		return baseContainerImage_->startTime_;
	}
	Timestamp getContainerExpirationEndTime() const {
		ContainerExpirationInfo *info = getContainerExpirationInfo();
		if (info == NULL) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR,
				"Invalid container expiration.");
		}
		Timestamp endTime = baseContainerImage_->startTime_ + info->interval_ - 1;
		if (baseContainerImage_->startTime_ <= endTime) {
			return endTime;
		} else {
			return MAX_TIMESTAMP;
		}
	}
	ContainerExpirationInfo* getContainerExpirationInfo() const {
		return commonContainerSchema_->get<ContainerExpirationInfo>(META_TYPE_CONTAINER_DURATION);
	}
	int64_t getContainerExpirationDutation() const {
		ContainerExpirationInfo *info =  commonContainerSchema_->get<ContainerExpirationInfo>(META_TYPE_CONTAINER_DURATION);
		if (info != NULL) {
			return info->info_.duration_;
		} else {
			return INT64_MAX;
		}
	}
	static void calcChunkKey(Timestamp baseTime, int64_t duration,
		ExpireIntervalCategoryId &expireCategoryId, ChunkKey &chunkKey) {
		if (duration <= 0) {
			GS_THROW_USER_ERROR(
				GS_ERROR_DS_CM_EXPIRATION_TIME_INVALID, "Invalid duration : " << duration);
		}
		expireCategoryId =  ChunkManager::DataAffinityUtils::calcExpireIntervalCategoryId(duration);
		if (baseTime + duration + EXPIRE_MARGIN >
			baseTime) {
			uint64_t roundingBitNum = ChunkManager::DataAffinityUtils::getExpireTimeRoundingBitNum(expireCategoryId);
			chunkKey = ChunkManager::DataAffinityUtils::convertTimestamp2ChunkKey(
				baseTime + duration + EXPIRE_MARGIN,
				roundingBitNum, true);
		}
		else {
			chunkKey = MAX_CHUNK_KEY;
		}
	}

public:  
	AllocateStrategy getMapAllcateStrategy() const {
		return mapAllocateStrategy_;
	}
	AllocateStrategy getRowAllcateStrategy() const {
		return rowAllocateStrategy_;
	}
	AllocateStrategy getMetaAllcateStrategy() const {
		return metaAllocateStrategy_;
	}

protected:  
	virtual void incrementRowNum() = 0;
	virtual void decrementRowNum() = 0;

	virtual void insertRowIdMap(TransactionContext &txn, BtreeMap *map,
		const void *constKey, OId oId) = 0;
	virtual void insertMvccMap(TransactionContext &txn, BtreeMap *map,
		TransactionId tId, MvccRowImage &mvccImage) = 0;
	virtual void insertValueMap(TransactionContext &txn, ValueMap &valueMap,
		const void *constKey, OId oId, bool isNull) = 0;
	virtual void updateRowIdMap(TransactionContext &txn, BtreeMap *map,
		const void *constKey, OId oldOId, OId newOId) = 0;
	virtual void updateMvccMap(TransactionContext &txn, BtreeMap *map,
		TransactionId tId, MvccRowImage &oldMvccImage,
		MvccRowImage &newMvccImage) = 0;
	virtual void updateValueMap(TransactionContext &txn, ValueMap &valueMap,
		const void *constKey, OId oldOId, OId newOId, bool isNull) = 0;
	virtual void removeRowIdMap(TransactionContext &txn, BtreeMap *map,
		const void *constKey, OId oId) = 0;
	virtual void removeMvccMap(TransactionContext &txn, BtreeMap *map,
		TransactionId tId, MvccRowImage &mvccImage) = 0;
	virtual void removeValueMap(TransactionContext &txn, ValueMap &valueMap,
		const void *constKey, OId oId, bool isNull) = 0;
	virtual void updateIndexData(
		TransactionContext &txn, const IndexData &indexData) = 0;
	void updateValueMaps(TransactionContext &txn, 
		const util::XArray<std::pair<OId, OId> > &moveList);

};

/*!
	@brief Auto_ptr for Container object
*/
class ContainerAutoPtr {
public:
	ContainerAutoPtr(TransactionContext &txn, DataStore *dataStore,
		PartitionId pId, const FullContainerKey &containerKey,
		uint8_t containerType, uint32_t schemaSize,
		const uint8_t *containerSchema, bool isEnable,
		int32_t featureVersion,
		DataStore::PutStatus &status, bool isCaseSensitive = false)
		: stackAutoPtr_(txn.getDefaultAllocator()) {
		BaseContainer *container =
			dataStore->putContainer(txn, pId, containerKey, containerType,
				schemaSize, containerSchema, isEnable, featureVersion,
				status, isCaseSensitive);
		stackAutoPtr_.set(container);
	}

	ContainerAutoPtr(TransactionContext &txn, DataStore *dataStore,
		PartitionId pId, const FullContainerKey &containerKey,
		uint8_t containerType, bool isCaseSensitive = false, bool allowExpiration = false)
		: stackAutoPtr_(txn.getDefaultAllocator()) {
		BaseContainer *container =
			dataStore->getContainer(txn, pId, containerKey, containerType, isCaseSensitive, allowExpiration);
		stackAutoPtr_.set(container);
	}
	ContainerAutoPtr(TransactionContext &txn, DataStore *dataStore,
		PartitionId pId, const ContainerCursor &containerCursor)
		: stackAutoPtr_(txn.getDefaultAllocator()) {
		bool allowExpiration = true;
		BaseContainer *newContainer =
			dataStore->getContainer(txn, pId, containerCursor, allowExpiration);
		stackAutoPtr_.set(newContainer);
	}

	ContainerAutoPtr(TransactionContext &txn, DataStore *dataStore,
		PartitionId pId, ContainerId containerId, uint8_t containerType, bool allowExpiration = false)
		: stackAutoPtr_(txn.getDefaultAllocator()) {
		BaseContainer *container =
			dataStore->getContainer(txn, pId, containerId, containerType, allowExpiration);
		stackAutoPtr_.set(container);
	}
	~ContainerAutoPtr() {
	}

	BaseContainer *getBaseContainer() {
		return stackAutoPtr_.get();
	}
	Collection *getCollection() {
		BaseContainer *container = stackAutoPtr_.get();
		if (container == NULL) {
			return NULL;
		}
		if (container->getContainerType() != COLLECTION_CONTAINER) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_TYPE_INVALID, "");
		}
		return reinterpret_cast<Collection *>(container);
	}
	TimeSeries *getTimeSeries() {
		BaseContainer *container = stackAutoPtr_.get();
		if (container == NULL) {
			return NULL;
		}
		if (container->getContainerType() != TIME_SERIES_CONTAINER) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_TYPE_INVALID, "");
		}
		return reinterpret_cast<TimeSeries *>(container);
	}

protected:
	ContainerAutoPtr(TransactionContext &txn)
		: stackAutoPtr_(txn.getDefaultAllocator()) {}
	StackAllocAutoPtr<BaseContainer> stackAutoPtr_;
};

/*!
	@brief Auto_ptr for Container object during recovery phase
*/
class ContainerForRestoreAutoPtr : public ContainerAutoPtr {
public:
	ContainerForRestoreAutoPtr(TransactionContext &txn, DataStore *dataStore,
		PartitionId pId, OId oId, ContainerId containerId, uint8_t containerType)
		: ContainerAutoPtr(txn) {
		BaseContainer *container =
			dataStore->getContainerForRestore(txn, pId, oId, containerId, containerType);
		stackAutoPtr_.set(container);
	}

private:
};

class BaseContainer::RowArray {
public:  
	class Row;
	struct Column;
	struct RowPram {
		uint32_t rowFixedColumnSize_;	
		uint32_t nullsOffset_;	
		uint32_t nullbitsSize_;	
		uint32_t rowSize_;		
		uint32_t rowIdOffset_;	
		uint32_t rowDataOffset_; 
		uint32_t rowHeaderOffset_; 
		uint32_t columnNum_;		
		uint32_t varColumnNum_;		
		uint32_t nullOffsetDiff_;	
		uint32_t columnOffsetDiff_;	
		uint32_t varHeaderSize_; 

		RowPram() {
			rowFixedColumnSize_ = 0;
			nullsOffset_ = 0;
			nullbitsSize_ = 0;
			rowSize_ = 0;
			rowIdOffset_ = 0;
			rowDataOffset_ = 0;
			rowHeaderOffset_ = 0;
			columnNum_ = 0xFFFF;
			varColumnNum_ = 0;
			nullOffsetDiff_ = 0;
			columnOffsetDiff_ = 0;
			varHeaderSize_ = 0;
		}
	};
	RowPram latestParam_;

	RowArray(TransactionContext &txn, BaseContainer *container);

	bool load(TransactionContext &txn, OId oId, BaseContainer *container,
		uint8_t getOption);
	bool setDirty(TransactionContext &txn);
	void initialize(
		TransactionContext &txn, RowId baseRowId, uint16_t maxRowNum);
	void finalize(TransactionContext &txn);

	void append(TransactionContext &txn, MessageRowStore *messageRowStore,
		RowId rowId);
	void insert(TransactionContext &txn, MessageRowStore *messageRowStore,
		RowId rowId);
	void update(TransactionContext &txn, MessageRowStore *messageRowStore);
	void remove(TransactionContext &txn);
	void move(TransactionContext &txn, RowArray &dest);
	void copy(TransactionContext &txn, RowArray &dest);
	void updateNullsStats(const uint8_t *nullbits);

	void copyRowArray(
		TransactionContext &txn, RowArray &dest); 
	void moveRowArray(
		TransactionContext &txn); 
	bool nextRowArray(
		TransactionContext &, RowArray &neighbor, bool &isOldSchema, uint8_t getOption);
	bool prevRowArray(
		TransactionContext &, RowArray &neighbor, bool &isOldSchema, uint8_t getOption);
	void shift(TransactionContext &txn, bool isForce,
		util::XArray<std::pair<OId, OId> > &moveList);
	void split(TransactionContext &txn, RowId insertRowId,
		RowArray &splitRowArray, RowId splitRowId,
		util::XArray<std::pair<OId, OId> > &moveList);
	void merge(TransactionContext &txn, RowArray &nextRowArray,
		util::XArray<std::pair<OId, OId> > &moveList);

	bool searchRowId(RowId rowId);
	bool searchNextRowId(RowId rowId);
	bool searchPrevRowId(RowId rowId);

	bool next();
	bool prev();
	bool begin();
	bool end();
	bool tail();
	bool hasNext() const;
	RowId getMidRowId();
	bool isFull();
	bool isNotInitialized() const;
	bool isTailPos() const;
	OId getOId() const;
	OId getBaseOId() const;
	uint8_t *getNewRow();
	uint8_t *getRow();
	uint16_t getRowNum() const;
	uint16_t getActiveRowNum(uint16_t limit = UINT16_MAX);
	uint16_t getMaxRowNum() const;
	uint32_t getHeaderSize() const;
	static uint32_t calcHeaderSize(uint32_t nullbitsSize);
	uint8_t *getRowIdAddr() const;
	void setRowId(RowId rowId);
	RowId getRowId() const;
	uint8_t *getNullsStats();
	uint32_t getNullbitsSize() const;
	void setContainerId(ContainerId containerId);
	ContainerId getContainerId() const;
	void setColumnNum(uint16_t columnNum);
	uint16_t getColumnNum() const;
	void setVarColumnNum(uint16_t columnNum);
	uint16_t getVarColumnNum() const;
	void setRowFixedColumnSize(uint16_t fixedSize);
	uint16_t getRowFixedColumnSize() const;

	RowArrayType getRowArrayType() const;

	template<typename Container, RowArrayType rowArrayType>
	BaseContainer::RowArrayImpl<Container, rowArrayType> *getImpl();
	template<typename Container, RowArrayType rowArrayType>
	BaseContainer::RowArrayImpl<Container, rowArrayType> *getImpl() const;

	BaseContainer::RowArrayImpl<BaseContainer, BaseContainer::ROW_ARRAY_GENERAL> *getDefaultImpl();
	BaseContainer::RowArrayImpl<BaseContainer, BaseContainer::ROW_ARRAY_GENERAL> *getDefaultImpl() const;

	bool validate();
	std::string dump(TransactionContext &txn);

	void lock(TransactionContext &txn);
	void setFirstUpdate();
	void resetFirstUpdate();
	bool isFirstUpdate() const;
	TransactionId getTxnId() const;

	void reset();

	bool isLatestSchema() const;
	bool convertSchema(TransactionContext &txn, 
		util::XArray< std::pair<RowId, OId> > &splitRAList,
		util::XArray< std::pair<OId, OId> > &moveOIdList);					
	static size_t getTmpColFixedOffset() {
		return COL_FIXED_DATA_OFFSET;
	}
	static size_t getTmpColRowIdOffset() {
		return COL_ROWID_OFFSET;
	}
	uint32_t getTmpVariableArrayOffset() {
		return latestParam_.rowDataOffset_;
	}
	RowId getCurrentRowId();
	template<typename Container>
	static Column getColumn(const ColumnInfo &info);
	template<typename Container>
	static Column getRowIdColumn(const BaseContainer &container);
private:								 
	static const size_t COL_ROWID_OFFSET =
		sizeof(TransactionId);  
	static const size_t COL_FIXED_DATA_OFFSET =
		COL_ROWID_OFFSET + sizeof(RowId);  
	static const size_t COL_ROW_HEADER_OFFSET = 7;  
	static const size_t TIM_ROW_HEADER_OFFSET = 0;  
public:
	static size_t getColFixedOffset() {
		return COL_FIXED_DATA_OFFSET;
	}
	static size_t getColRowIdOffset() {
		return COL_ROWID_OFFSET;
	}
private:  
	util::XArray< void * > rowArrayImplList_;
	RowArrayStorage rowArrayStorage_;
	RowCache rowCache_;
	BaseContainer::RowArrayImpl<BaseContainer, BaseContainer::ROW_ARRAY_GENERAL>* defaultImpl_;
};

class BaseContainer::RowArray::Row {
public:
	Row(uint8_t *rowImage, RowArray *rowArrayCursor);
	void initialize();
	void finalize(TransactionContext &txn);
	void setFields(
		TransactionContext &txn, MessageRowStore *messageRowStore);
	void updateFields(
		TransactionContext &txn, MessageRowStore *messageRowStore);
	void getField(TransactionContext &txn, const ColumnInfo &columnInfo,
		BaseObject &baseObject);
	void getField(TransactionContext &txn, const ColumnInfo &columnInfo,
		ContainerValue &containerValue);
	void remove(TransactionContext &txn);
	void move(TransactionContext &txn, Row &dest);
	void copy(TransactionContext &txn, Row &dest);
	void reset();
	void getImage(TransactionContext &txn,
		MessageRowStore *messageRowStore, bool isWithRowId);
	void getFieldImage(TransactionContext &txn,
		ColumnInfo &srcColumnInfo, uint32_t destColumnInfo,
		MessageRowStore *messageRowStore);								
	void setRowId(RowId rowId);
	RowId getRowId() const;
	bool isRemoved() const;
	uint8_t *getVariableArrayAddr() const;
	const uint8_t *getNullsAddr() const;
	bool isMatch(TransactionContext &txn, TermCondition &cond,
		ContainerValue &tmpValue);
	bool isNullValue(const ColumnInfo &columnInfo) const;

	void lock(TransactionContext &txn);
	void setFirstUpdate();
	void resetFirstUpdate();
	bool isFirstUpdate() const;
	TransactionId getTxnId() const;
	static bool isRemoved(RowHeader *binary);

	void archive(TransactionContext &txn, ArchiveHandler *handler);
	std::string dump(TransactionContext &txn);

	BaseContainer::RowArray *getRowArray();
private:  
	RowArray *rowArrayCursor_;
};



typedef BaseContainer::RowArrayType RowArrayType;

template<typename  Container, RowArrayType rowArrayType>
class BaseContainer::RowArrayImpl {
	friend class Row;
	typedef BaseContainer::RowArray::Column Column;
public:  
	class Row;

	RowArrayImpl(TransactionContext &txn, BaseContainer *container, 
		RowArrayStorage &rowArrayStrage, RowCache &rowCache, 
		const BaseContainer::RowArray::RowPram &latestParam);

	bool load(TransactionContext &txn, OId oId, BaseContainer *container,
		uint8_t getOption);											
	bool setDirty(TransactionContext &txn);							
	void initialize(
		TransactionContext &txn, RowId baseRowId, uint16_t maxRowNum);
	void finalize(TransactionContext &txn);							

	void append(TransactionContext &txn, MessageRowStore *messageRowStore,
		RowId rowId);
	void insert(TransactionContext &txn, MessageRowStore *messageRowStore,
		RowId rowId);
	void update(TransactionContext &txn, MessageRowStore *messageRowStore);
	void remove(TransactionContext &txn);
	void move(TransactionContext &txn, RowArrayImpl &dest);
	void copy(TransactionContext &txn, RowArrayImpl &dest);
	void updateNullsStats(const uint8_t *nullbits);

	void copyRowArray(
		TransactionContext &txn, RowArrayImpl &dest); 
	void moveRowArray(
		TransactionContext &txn); 
	bool nextRowArray(
		TransactionContext &, RowArrayImpl &neighbor, bool &isOldSchema, uint8_t getOption);
	bool prevRowArray(
		TransactionContext &, RowArrayImpl &neighbor, bool &isOldSchema, uint8_t getOption);
	void shift(TransactionContext &txn, bool isForce,
		util::XArray<std::pair<OId, OId> > &moveList);
	void split(TransactionContext &txn, RowId insertRowId,
		RowArrayImpl &splitRowArray, RowId splitRowId,
		util::XArray<std::pair<OId, OId> > &moveList);
	void merge(TransactionContext &txn, RowArrayImpl &nextRowArray,
		util::XArray<std::pair<OId, OId> > &moveList);

	bool searchRowId(RowId rowId);
	bool searchNextRowId(RowId rowId);
	bool searchPrevRowId(RowId rowId);

	bool next();
	bool prev();
	bool begin();
	bool end();
	bool tail();
	bool hasNext() const;
	RowId getMidRowId();
	bool isFull();
	bool isNotInitialized() const;
	bool isTailPos() const;
	OId getOId() const;
	OId getBaseOId() const;
	uint8_t *getNewRow();
	uint8_t *getRow();
	uint16_t getRowNum() const;
	uint16_t getActiveRowNum(uint16_t limit = UINT16_MAX);
	uint16_t getMaxRowNum() const;
	uint32_t getHeaderSize() const;
	static uint32_t calcHeaderSize(uint32_t nullbitsSize);
	void setRowId(RowId rowId);
	RowId getRowId() const;
	uint8_t *getNullsStats();
	uint32_t getNullbitsSize() const;
	void setContainerId(ContainerId containerId);
	ContainerId getContainerId() const;
	void setColumnNum(uint16_t columnNum);
	uint16_t getColumnNum() const;
	void setVarColumnNum(uint16_t columnNum);
	uint16_t getVarColumnNum() const;
	void setRowFixedColumnSize(uint16_t fixedSize);
	uint16_t getRowFixedColumnSize() const;
	RowArrayType getRowArrayType() const; 

	bool validate();												
	std::string dump(TransactionContext &txn);						

	void lock(TransactionContext &txn);
	void setFirstUpdate();
	void resetFirstUpdate();
	bool isFirstUpdate() const;
	TransactionId getTxnId() const;

	Row &getRowCursor() {
		return row_;
	};
	bool isLatestSchema() const;
	bool convertSchema(TransactionContext &txn, 
		util::XArray< std::pair<RowId, OId> > &splitRAList,
		util::XArray< std::pair<OId, OId> > &moveOIdList);					
	RowId getCurrentRowId() {
		return row_.getRowId();
	}
	static Column getColumn(const ColumnInfo &info);
	static Column getRowIdColumn(const BaseContainer &container);

private:  
	static const size_t MAX_ROW_NUM_OFFSET = 0;									
	static const size_t ROW_NUM_OFFSET = MAX_ROW_NUM_OFFSET + sizeof(uint16_t);	
	static const size_t ROWID_OFFSET = ROW_NUM_OFFSET + sizeof(uint16_t);		

	static const size_t CONTAINER_ID_OFFSET = ROWID_OFFSET + sizeof(RowId);		
	static const size_t COLUMN_NUM_OFFSET = CONTAINER_ID_OFFSET + sizeof(ContainerId);	
	static const size_t STATUS_OFFSET =	COLUMN_NUM_OFFSET + sizeof(uint16_t);  
	static const size_t ROWARRAY_TYPE_OFFSET = STATUS_OFFSET + sizeof(uint8_t);	
	static const size_t SPARSE_DATA_OFFSET = ROWARRAY_TYPE_OFFSET + sizeof(uint8_t);	

	static const size_t TIM_TID_OFFSET = SPARSE_DATA_OFFSET + sizeof(OId);				
	static const size_t TIM_BITS_OFFSET = TIM_TID_OFFSET + 7;							
	static const size_t VAR_COLUMN_NUM_OFFSET = TIM_TID_OFFSET +  sizeof(TransactionId);	
	static const size_t ROW_FIXED_SIZE_OFFSET = VAR_COLUMN_NUM_OFFSET +  sizeof(uint16_t); 

	static const size_t HEADER_AREA_SIZE = 48;
	static const size_t HEADER_FREE_AREA_SIZE = HEADER_AREA_SIZE - TIM_TID_OFFSET - sizeof(TransactionId);	
	enum Status {
		MAX_STATUS = 7			
	};

	struct Header {
		uint16_t elemCursor_;
	};

private:  
	BaseContainer *container_;
	RowArrayStorage &rowArrayStorage_;
	RowCache &rowCache_;

	Row row_;
	const BaseContainer::RowArray::RowPram &latestParam_;
	BaseContainer::RowArray::RowPram currentParam_;
	uint16_t elemCursor_;
	
private:  
	uint8_t *getTIdAddr() const;
	uint8_t *getBitsAddr() const;
	void setLockTId(TransactionId tId);

	uint8_t *getRowIdAddr() const;
	uint8_t *getRow(uint16_t elem) const;
	void setMaxRowNum(uint16_t num);
	void setRowNum(uint16_t num);
	uint8_t *getAddr() const;
	uint16_t getElemCursor(OId oId) const;
	void updateCursor();
	OId getBaseOId(OId oId) const;
	uint32_t getBinarySize(uint16_t maxRowNum) const;
	BaseContainer &getContainer() const;
	RowArrayImpl(const RowArrayImpl &);
	RowArrayImpl &operator=(const RowArrayImpl &);

	void initializeParam();
	bool isNotExistColumn(ColumnInfo columnInfo) const;
	bool hasVariableColumn() const;
	OId calcOId(uint16_t cursor) const;
	void reset(
		TransactionContext &txn, RowId baseRowId, uint16_t maxRowNum);
	void convert(TransactionContext &txn, RowArrayImpl &dest);
	void setStatus(Status status) {
		uint8_t *statusAddr = 
			reinterpret_cast<uint8_t *>(getAddr() + STATUS_OFFSET);
		*statusAddr |= (1 << status);
	}
	uint8_t getStatus(Status status) const {
		uint8_t *statusAddr = 
			reinterpret_cast<uint8_t *>(getAddr() + STATUS_OFFSET);
		return *statusAddr & (1 << status);
	}
	void resetCursor();
	void moveCursor(uint16_t elem);
	void nextCursor();
	void prevCursor();
};

template<typename Container, RowArrayType rowArrayType>
class BaseContainer::RowArrayImpl< Container, rowArrayType>::Row {
	typedef BaseContainer::RowArray::Column Column;
public:
	static const RowHeader FIRST_UPDATE_BIT =
		0x40;  
	static const TransactionId TID_FIELD =
		0x00ffffffffffffffLL;  
	static const TransactionId BITS_FIELD =
		0xff00000000000000LL;  

public:
	Row(uint8_t *rowImage, RowArrayImpl *rowArrayCursor);
	void initialize();
	void finalize(TransactionContext &txn);								
	void setFields(
		TransactionContext &txn, MessageRowStore *messageRowStore);
	void updateFields(
		TransactionContext &txn, MessageRowStore *messageRowStore);
	void getField(TransactionContext &txn, const ColumnInfo &columnInfo,
		BaseObject &baseObject);										
	void getField(TransactionContext &txn, const ColumnInfo &columnInfo,
		ContainerValue &containerValue);								

	const void *getField(const Column &column);
	const void *getFixedField(const Column &column);
	const void *getVariableField(const Column &column);

	void remove(TransactionContext &txn);
	void move(TransactionContext &txn, Row &dest);
	void copy(TransactionContext &txn, Row &dest);
	void reset();
	void getImage(TransactionContext &txn,
		MessageRowStore *messageRowStore, bool isWithRowId);			
	void getFieldImage(TransactionContext &txn,
		ColumnInfo &srcColumnInfo, uint32_t destColumnInfo,
		MessageRowStore *messageRowStore);								
	void setRowId(RowId rowId);
	RowId getRowId() const;
	bool isRemoved() const;
	uint8_t *getVariableArrayAddr() const;
	const uint8_t *getNullsAddr() const;
	bool isMatch(TransactionContext &txn, TermCondition &cond,
		ContainerValue &tmpValue);
	bool isNullValue(const ColumnInfo &columnInfo) const;
	void archive(TransactionContext &txn, ArchiveHandler *handler);

	void lock(TransactionContext &txn);
	void setFirstUpdate();
	void resetFirstUpdate();
	bool isFirstUpdate() const;
	TransactionId getTxnId() const;
	static bool isRemoved(RowHeader *binary);
	std::string dump(TransactionContext &txn);

	static RowHeader getFirstUpdateBit() {
		return FIRST_UPDATE_BIT;
	}
	static TransactionId getTIdField() {
		return TID_FIELD;
	}
	static TransactionId getBitsField() {
		return BITS_FIELD;
	}
	static size_t getBitsOffset() {
		return COL_TID_OFFSET + 7;
	}

	void convert(TransactionContext &txn, Row &dest);
	void setBinary(uint8_t *binary) {
		binary_ = binary;
	}
	void moveAddBinary(uint32_t offset) {
		binary_ += offset;
	}
	void moveSubBinary(uint32_t offset) {
		binary_ -= offset;
	}
	uint8_t *getBinary() const {
		return binary_;
	}
private:								 
	static const size_t COL_TID_OFFSET = 0;  
	static const RowHeader REMOVE_BIT =
		0x80;  

private:  
	RowArrayImpl *rowArrayCursor_;
	uint8_t *binary_;

private:
	uint8_t *getRowHeaderAddr() const;

	uint8_t *getRowIdAddr() const;
	void setRemoved();
	void setLockTId(TransactionId tId);
	void checkVarDataSize(TransactionContext &txn,
		const util::XArray< std::pair<uint8_t *, uint32_t> > &varList,
		const util::XArray<uint32_t> &varColumnIdList,
		bool isConvertSpecialType,
		util::XArray<uint32_t> &varDataObjectSizeList,
		util::XArray<uint32_t> &varDataObjectPosList);
	void setVariableFields(TransactionContext &txn,
		const util::XArray< std::pair<uint8_t *, uint32_t> > &varList,
		const util::XArray<uint32_t> &varColumnIdList,
		bool isConvertSpecialType,
		const util::XArray<uint32_t> &varDataObjectSizeList,
		const util::XArray<uint32_t> &varDataObjectPosList,
		const util::XArray<OId> &oldVarDataOIdList);
	uint8_t *getTIdAddr() const;
	uint8_t *getFixedAddr() const;
	void setVariableArray(OId oId);
	OId getVariableArray() const;
	uint8_t *getAddr() const;
};

struct BaseContainer::RowArray::Column {
public:
	Column() : fixedFieldOffset_(0), info_() {};
	size_t getFixedOffset() const {
		return fixedFieldOffset_;
	}
	const ColumnInfo &getColumnInfo() const {
		return info_;
	}
	void setFixedOffset(size_t offset) {
		fixedFieldOffset_ = offset;
	}
	void setColumnInfo(const ColumnInfo &info) {
		info_ = info;
	}
private:
	size_t fixedFieldOffset_;
	ColumnInfo info_;
};


#include "row.h"


#endif
