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
#include "data_store_v4.h"
#include "btree_map.h"
#include "rtree_map.h"
#include "container_message_v4.h"
#include "result_set.h"

UTIL_TRACER_DECLARE(BASE_CONTAINER);

class InputMessageRowStore;
class MessageSchema;
class ResultSet;
class ContainerRowScanner;
class ArchiveHandler;
class TreeFuncInfo;


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
		OId rowIdMapOId_;		   
		OId mvccMapOId_;  
		OId columnSchemaOId_;
		OId indexSchemaOId_;
		OId triggerListOId_;
		OId baseGroupId_;
		uint64_t rowNum_;
		Timestamp startTime_;	
	};

	class ValueMap {
	public:
		ValueMap(
				TransactionContext &txn, BaseContainer *container,
				IndexData &indexData) :
				container_(container),
				alloc_(txn.getDefaultAllocator()),
				indexData_(indexData) {
		}

		BaseIndex *getValueMap(TransactionContext &txn, bool forNull) {
			if (forNull) {
				if (nullMap_.get() == NULL) {
					container_->getIndex(txn, indexData_, forNull, nullMap_);
				}
				return nullMap_.get();
			}
			else {
				if (valueMap_.get() == NULL) {
					container_->getIndex(txn, indexData_, forNull, valueMap_);
				}
				assert(valueMap_.get() != NULL);
				return valueMap_.get();
			}
		};
		BaseIndex *putValueMap(TransactionContext &txn, bool forNull) {
			if (forNull) {
				if (nullMap_.get() == NULL) {
					if (indexData_.oIds_.nullOId_ == UNDEF_OID) {
						container_->createNullIndexData(txn, indexData_);
					}
					container_->getIndex(txn, indexData_, forNull, nullMap_);
				}
				assert(nullMap_.get() != NULL);
				return nullMap_.get();
			}
			else {
				if (valueMap_.get() == NULL) {
					container_->getIndex(txn, indexData_, forNull, valueMap_);
				}
				assert(valueMap_.get() != NULL);
				return valueMap_.get();
			}
		};
		void updateIndexData(TransactionContext &txn) {
			if (valueMap_.get() != NULL) {
				indexData_.oIds_.mainOId_ = valueMap_.get()->getBaseOId();
			}
			if (nullMap_.get() != NULL) {
				indexData_.oIds_.nullOId_ = nullMap_.get()->getBaseOId();
			}
			container_->updateIndexData(txn, indexData_);
		}
		MapType getMapType() {
			return indexData_.mapType_;
		}

		template <typename T>
		int32_t search(
				TransactionContext &txn, typename T::SearchContext &sc,
				util::XArray<OId> &oIdList, OutputOrder outputOrder);

		template <typename T>
		uint64_t estimate(
				TransactionContext &txn, typename T::SearchContext &sc);

		TreeFuncInfo *getFuncInfo(TransactionContext &txn) {
			return getValueMap(txn, false)->getFuncInfo();
		}

	private:
		BaseContainer *container_;
		IndexAutoPtr valueMap_;
		IndexAutoPtr nullMap_;
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
		RowArrayStorage(ObjectManagerV4 &objectManager, AllocateStrategy &strategy) : BaseObject(objectManager, strategy) {};
	};

	class RowCache {
	public:
		RowCache();
		RowCache(TransactionContext &txn, BaseContainer *container);
		~RowCache();
		BaseObject &getFrontFieldObject() {
			return frontFieldCache_.get()->baseObject_;
		}
		void reset() {
			lastCachedField_ = 0;
			BaseObject &frontObject = getFrontFieldObject();
			frontObject.reset();
			for (FieldCacheList::iterator it = fieldCacheList_.begin(); it != fieldCacheList_.end(); ++it) {
				it->reset();
			}
		}
	public:
		struct FieldCache {
			explicit FieldCache(ObjectManagerV4 &objectManager, AllocateStrategy &strategy) :
				baseObject_(objectManager, strategy),
				addr_(NULL) {
			}
			void reset() {
				baseObject_.reset();
				addr_ = NULL;
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

	struct RowArrayCheckerResult {
		RowArrayCheckerResult() :
				latestColumnCount_(0),
				initialColumnCount_(0),
				rowArrayCount_(0),
				columnMismatchCount_(0) {
		}

		uint32_t latestColumnCount_;
		uint32_t initialColumnCount_;
		int64_t rowArrayCount_;
		int64_t columnMismatchCount_;
	};

	class RowArray;

	template<typename Container, RowArrayType rowArrayType>
	class RowArrayImpl;

	typedef bool IndexMapTable[COLUMN_TYPE_PRIMITIVE_COUNT][MAP_TYPE_NUM];

public:  
public:  
	virtual ~BaseContainer() {
		ALLOC_DELETE((alloc_), commonContainerSchema_);
		ALLOC_DELETE((alloc_), columnSchema_);
		ALLOC_DELETE((alloc_), indexSchema_);
		ALLOC_DELETE((alloc_), rowArrayCache_);
	}
	virtual void initialize(TransactionContext &txn) = 0;
	virtual bool finalize(TransactionContext &txn, bool isRemoveGroup = false) = 0;
	virtual void set(TransactionContext& txn, const FullContainerKey& containerKey,
		ContainerId containerId, OId columnSchemaOId,
		MessageSchema* containerSchema, DSGroupId groupId) = 0;

	virtual void createIndex(
		TransactionContext &txn, const IndexInfo &indexInfo,
		IndexCursor& indexCursor,
		bool isIndexNameCaseSensitive = false,
		CreateDropIndexMode mode = INDEX_MODE_NOSQL,
		bool *skippedByMode = NULL) = 0;
	virtual void continueCreateIndex(TransactionContext& txn, 
		IndexCursor& indexCursor) = 0;
	IndexCursor getIndexCursor(TransactionContext& txn);
	ContainerCursor getContainerCursor(TransactionContext& txn);
	virtual void dropIndex(
		TransactionContext &txn, IndexInfo &indexInfo,
		bool isIndexNameCaseSensitive = false,
		CreateDropIndexMode mode = INDEX_MODE_NOSQL,
		bool *skippedByMode = NULL) = 0;

	void changeSchema(TransactionContext &txn,
		BaseContainer &newContainer, util::XArray<uint32_t> &copyColumnMap);
	void continueChangeSchema(TransactionContext &txn,
		ContainerCursor &containerCursor);
	void changeProperty(TransactionContext& txn, OId columnSchemaOId);
	void makeCopyColumnMap(TransactionContext &txn,
		MessageSchema *messageSchema, util::XArray<uint32_t> &copyColumnMap,
		DataStoreV4::SchemaState &schemaState);
	void changeNullStats(TransactionContext& txn, uint32_t oldColumnNum);
	void updateContainer(TransactionContext& txn, BaseContainer* newContainer);

	void getIndexInfoList(
		TransactionContext &txn, util::Vector<IndexInfo> &indexInfoList);
	void getIndexDataList(
		TransactionContext &txn, MapType mapType, 
		util::Vector<ColumnId> &columnIds,
		util::Vector<IndexData> &indexDataList,
		bool withPartialMatch);
	TreeFuncInfo *createTreeFuncInfo(TransactionContext &txn, const util::Vector<ColumnId> &columnIds);

	bool checkRowKeySchema(util::XArray<ColumnType> &columnTypeList);
	void getContainerInfo(TransactionContext &txn,
		util::XArray<uint8_t> &containerSchema, bool optionIncluded = true, bool internalOptionIncluded = true, bool isRenameColumn = false);

	virtual SchemaFeatureLevel getSchemaFeatureLevel() const;
	static SchemaFeatureLevel resolveColumnSchemaFeatureLevel(
			const ColumnInfo &info);

	void getErasableList(TransactionContext &txn, Timestamp erasableTimeLimit, util::XArray<ArchiveInfo> &list);

	bool checkRowArray(
			TransactionContext &txn, RowId startRowId, uint32_t limitMillis,
			util::Stopwatch &watch, RowId &lastRowId,
			RowArrayCheckerResult &result);

	void putRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowData, RowId &rowId, PutStatus &status,
		PutRowOption putRowOption) {
		bool rowIdSpecified = false;
		putRow(txn, rowSize, rowData, rowId, rowIdSpecified, status,
			putRowOption);
	}
	void redoPutRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowData, RowId &rowId, PutStatus &status,
		PutRowOption putRowOption) {
		bool rowIdSpecified = true;
		putRow(txn, rowSize, rowData, rowId, rowIdSpecified, status,
			putRowOption);
	}
	virtual void deleteRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowKey, RowId &rowId, bool &existing) = 0;
	virtual void deleteRow(
		TransactionContext &txn, RowId rowId, bool &existing) = 0;
	virtual void updateRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowData, RowId rowId, PutStatus &status) = 0;
	virtual void redoDeleteRow(
		TransactionContext &txn, RowId rowId, bool &existing) = 0;
	virtual void abort(TransactionContext &txn) = 0;
	virtual void commit(TransactionContext &txn) = 0;
	void validateForRename(TransactionContext &txn,
		MessageSchema *messageSchema, DataStoreV4::SchemaState &schemaState);
	bool hasUncommitedTransaction(TransactionContext &txn);

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
		RtreeMap::SearchContext &sc, util::XArray<OId> &resultList);
	void searchColumnIdIndex(TransactionContext &txn,
		BtreeMap::SearchContext &sc, util::XArray<OId> &normalRowList,
		util::XArray<OId> &mvccRowList);

	int64_t estimateIndexSearchSize(
			TransactionContext &txn, BtreeMap::SearchContext &sc);
	void estimateRowIdIndexSearchSize(
			TransactionContext &txn, BtreeMap::SearchContext &sc,
			uint64_t &estimationSize);
	bool estimateColumnIdIndexSearchSize(
			TransactionContext &txn, BtreeMap::SearchContext &sc,
			uint64_t &estimationSize);
	uint64_t toEstimationSize(uint64_t base);

	void getRowList(TransactionContext &txn, util::XArray<OId> &oIdList,
		ResultSize limit, ResultSize &resultNum,
		MessageRowStore *messageRowStore, bool isWithRowId,
		ResultSize startPos);

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
	/*!
		@brief Get FullContainerKey
	*/
	FullContainerKey getContainerKey(TransactionContext &txn) {
		UNUSED_VARIABLE(txn);
		if (containerKeyCursor_ .getBaseOId() == UNDEF_OID) {
			if (baseContainerImage_->containerNameOId_ != UNDEF_OID) {
				containerKeyCursor_.load(baseContainerImage_->containerNameOId_, false);
			}
			else {
				GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_NAME_INVALID, "container key not exist");
			}
		}
		return containerKeyCursor_.getKey();
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
	void getColumnInfo(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy& strategy,
		const char *name, uint32_t &columnId, ColumnInfo *&columnInfo,
		bool isCaseSensitive) const {
		return columnSchema_->getColumnInfo(
			txn, objectManager, strategy, name, columnId, columnInfo, isCaseSensitive);
	}
	void getKeyColumnIdList(util::Vector<ColumnId> &keyColumnIdList) {
		columnSchema_->getKeyColumnIdList(keyColumnIdList);
	}
	uint32_t getVariableColumnNum() const {
		return columnSchema_->getVariableColumnNum();
	}
	bool definedRowKey() const {
		return columnSchema_->definedRowKey();
	}
	bool hasIndex(
		TransactionContext &txn, util::Vector<ColumnId> &columnIds, 
		MapType mapType, bool withPartialMatch) const {
		return indexSchema_->hasIndex(txn, columnIds, mapType, withPartialMatch);
	}
	bool hasIndex(IndexTypes indexType, MapType mapType) const {
		return indexSchema_->hasIndex(indexType, mapType);
	}
	IndexTypes getIndexTypes(TransactionContext &txn, 
		util::Vector<ColumnId> &columnIds, bool withPartialMatch) const {
		return indexSchema_->getIndexTypes(txn, columnIds, withPartialMatch);
	}
	void getNullsStats(util::XArray<uint8_t> &nullsList) const {
		uint16_t limitSize = sizeof(int64_t);
		if (!isNullsStatsStatus() && indexSchema_->getNullbitsSize() > limitSize) {
			const uint32_t diffSize = indexSchema_->getNullbitsSize() - limitSize;
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

	void handleSearchError(
		TransactionContext &txn, std::exception &e, ErrorCode errorCode);

	bool validate(TransactionContext &txn, std::string &errorMessage);
	std::string dump(TransactionContext &txn);
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
	bool isFirstColumnAdd() {
		return columnSchema_->isFirstColumnAdd();
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

	void getAffinityStr(util::String &affinityStr) {
		char temporary[AFFINITY_STRING_MAX_LENGTH + 1];
		memcpy(temporary, getAffinityBinary(), AFFINITY_STRING_MAX_LENGTH);
		temporary[AFFINITY_STRING_MAX_LENGTH] =
			'\0';  
		affinityStr = temporary;
	}

	DSGroupId getBaseGroupId() const {
		return baseContainerImage_->baseGroupId_;
	}

	bool isInvalid() const {
		return (baseContainerImage_->status_ & CONTAINER_INVALID_BIT) != 0;
	}

	void setVersionId(uint32_t versionId) {
		baseContainerImage_->versionId_ = versionId;
	}

	void updateNullsStats(const uint8_t *nullbits) {
		indexSchema_->updateNullsStats(nullbits);
	}
	uint32_t getNullbitsSize() const {
		return indexSchema_->getNullbitsSize();
	}

	ExpireType getExpireType() const;

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

	DataStoreV4 *getDataStore() {
		return dataStore_;
	}

	virtual ColumnId getRowIdColumnId() = 0;
	virtual ColumnType getRowIdColumnType() = 0;

	bool checkRunTime(TransactionContext &txn);

	TablePartitioningVersionId getTablePartitioningVersionId();
	void setTablePartitioningVersionId(
		TablePartitioningVersionId versionId);

	bool isExpired(TransactionContext &txn) {
		Timestamp currentTime = txn.getStatementStartTime().getUnixTime();
		Timestamp lastExpiredTime = getDataStore()->stats().getExpiredTime();

		if (currentTime < lastExpiredTime) {
			currentTime = lastExpiredTime;
		}

		Timestamp expirationTime = getContainerExpirationTime();
		if (currentTime > expirationTime) {
			return true;
		}
		return false;
	}

	static Timestamp calcErasableTime(Timestamp baseTime, int64_t duration) {
		ChunkKey chunkKey = calcChunkKey(baseTime, duration);
		Timestamp erasableTime = DataStoreV4::DataAffinityUtils::convertChunkKey2Timestamp(chunkKey);
		return erasableTime;
	}
	static ChunkKey calcChunkKey(Timestamp startTime, const ContainerExpirationInfo& info) {
		Timestamp baseTime = getContainerExpirationEndTime(startTime, info);
		int64_t duration = info.info_.duration_;
		return calcChunkKey(baseTime, duration);
	}
	ChunkKey getChunkKey() {
		return calcChunkKey(getContainerExpirationEndTime(), getContainerExpirationDuration());
	}

	uint16_t getRowKeyColumnNum() const {
		return columnSchema_->getRowKeyColumnNum();
	}
	uint32_t getRowKeyFixedDataSize(util::StackAllocator &alloc) const;
	void getRowKeyFields(TransactionContext &txn, uint32_t rowKeySize, const uint8_t *rowKey, util::XArray<KeyData> &fields);
	ColumnInfo *getRowKeyColumnInfoList(TransactionContext &txn);

	AllocateStrategy& getMapAllocateStrategy() {
		return mapAllocateStrategy_;
	}
	AllocateStrategy& getRowAllocateStrategy() {
		return rowAllocateStrategy_;
	}
	AllocateStrategy& getMetaAllocateStrategy() {
		return metaAllocateStrategy_;
	}

	Timestamp getContainerExpirationTime() const {
		int64_t duration = getContainerExpirationDuration();
		if (duration != INT64_MAX) {
			Timestamp endTime = getContainerExpirationEndTime();
			Timestamp expirationTime = getContainerExpirationEndTime() + duration;
			if (endTime < expirationTime) {
				return expirationTime;
			}
			else {
				return MAX_TIMESTAMP;
			}
		}
		return MAX_TIMESTAMP;
	}

	void validateIndexInfo(const IndexInfo &info) const;
	static void validateIndexInfo(
			ContainerType type, ColumnType columnType, MapType mapType);

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
			case COLUMN_TYPE_MICRO_TIMESTAMP:
			case COLUMN_TYPE_NANO_TIMESTAMP:
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
		Operator op_;
		TransactionContext *txn_;
		const ColumnType columnType_;
		const bool isNullLast_;

		SortPred(
				TransactionContext &txn, Operator op,
				const ColumnType columnType, bool isNullLast) :
				op_(op), txn_(&txn), columnType_(columnType),
				isNullLast_(isNullLast) {
		}

		bool operator()(const SortKey &v1, const SortKey &v2) const {
			if (v1.data() == NULL) {
				if (v2.data() == NULL) {
					return false;
				}
				else {
					return !isNullLast_;
				}
			}
			else if (v2.data() == NULL) {
				return isNullLast_;
			}
			else {
				Value value1_;
				Value value2_;
				value1_.set(v1.data(), columnType_);
				value2_.set(v2.data(), columnType_);
				return op_(
						*txn_,
						value1_.data(), value1_.size(),
						value2_.data(), value2_.size());
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
		RsNotifier(DataStoreV4 &dataStore) : dataStore_(dataStore), containerId_(UNDEF_CONTAINERID), mode_(TO_UNDEF) {
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
		void addUpdatedRow(RowId rowId, OId oId);
	protected:
		DataStoreV4 &dataStore_;
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
	AllocateStrategy keyStoreAllocateStrategy_;
	AllocateStrategy metaAllocateStrategy_;
	AllocateStrategy mapAllocateStrategy_;
	AllocateStrategy rowAllocateStrategy_;
	MvccStatus exclusiveStatus_;
	util::StackAllocator &alloc_;
	DataStoreV4 *dataStore_;
	RsNotifier rsNotifier_;
	FullContainerKeyCursor containerKeyCursor_;
	RowArray *rowArrayCache_;
	TreeFuncInfo *rowIdFuncInfo_;
	TreeFuncInfo *mvccFuncInfo_;

	static const int8_t NULL_VALUE;
protected:  
	BaseContainer(TransactionContext &txn, DataStoreV4 *dataStore, OId oId)
		: BaseObject(
			  *(dataStore->getObjectManager())),
		  exclusiveStatus_(UNKNOWN),
		  alloc_(txn.getDefaultAllocator()),
		  dataStore_(dataStore),
		  rsNotifier_(*dataStore),
		  containerKeyCursor_(*(dataStore->getObjectManager())),
		  rowArrayCache_(NULL),
		  rowIdFuncInfo_(NULL), mvccFuncInfo_(NULL)
		{
		resetMetaAllocateStrategy(getObjectManager(), metaAllocateStrategy_);

		BaseObject::reset(*(getObjectManager()), getMetaAllocateStrategy());
		BaseObject::load(oId, false);

		containerKeyCursor_.reset(*(getObjectManager()), getMetaAllocateStrategy());

		baseContainerImage_ = getBaseAddr<BaseContainerImage *>();
		commonContainerSchema_ =
			ALLOC_NEW(txn.getDefaultAllocator()) ShareValueList(
				*getObjectManager(), getMetaAllocateStrategy(), baseContainerImage_->columnSchemaOId_);
		columnSchema_ =
			commonContainerSchema_->get<ColumnSchema>(META_TYPE_COLUMN_SCHEMA);
		indexSchema_ = ALLOC_NEW(txn.getDefaultAllocator())
			IndexSchema(txn, *getObjectManager(),
				baseContainerImage_->indexSchemaOId_, getMetaAllocateStrategy());
		
		resetRowAllocateStrategy(getObjectManager(), rowAllocateStrategy_);
		resetMapAllocateStrategy(getObjectManager(), mapAllocateStrategy_);
	}
	BaseContainer(TransactionContext &txn, DataStoreV4 *dataStore)
		: BaseObject(*(dataStore->getObjectManager())),
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
		  containerKeyCursor_(*(dataStore->getObjectManager())),
		  rowArrayCache_(NULL),
		  rowIdFuncInfo_(NULL), mvccFuncInfo_(NULL)
	{
		resetMetaAllocateStrategy(getObjectManager(), metaAllocateStrategy_);

		BaseObject::reset(*(getObjectManager()), getMetaAllocateStrategy());
		containerKeyCursor_.reset(*(getObjectManager()), getMetaAllocateStrategy());
	}

	void resetMetaAllocateStrategy(ObjectManagerV4* objMgr, AllocateStrategy &strategy) const {
		strategy.set(META_GROUP_ID, objMgr);
	}
	/*!
		@brief Calculate AllocateStrategy of Map Object
	*/
	void resetMapAllocateStrategy(ObjectManagerV4* objMgr, AllocateStrategy &strategy) const {
		strategy.set(baseContainerImage_->baseGroupId_, objMgr);
	}
	/*!
		@brief Calculate AllocateStrategy of Row Object
	*/
	void resetRowAllocateStrategy(ObjectManagerV4* objMgr, AllocateStrategy &strategy) const {
		strategy.set(baseContainerImage_->baseGroupId_ + 1, objMgr);
	}

	void setAllocateStrategy(ObjectManagerV4 *objMgr) {
		resetMetaAllocateStrategy(objMgr, metaAllocateStrategy_);
		resetRowAllocateStrategy(objMgr, rowAllocateStrategy_);
		resetMapAllocateStrategy(objMgr, mapAllocateStrategy_);
	}

	void calcGroupId();

	void replaceIndexSchema(TransactionContext &txn) {
		UNUSED_VARIABLE(txn);
		setDirty();
		baseContainerImage_->indexSchemaOId_ = indexSchema_->getBaseOId();
	}

	bool checkIndexConstraint(
		TransactionContext& txn, bool isCreate,
		CreateDropIndexMode mode, const IndexInfo& info,
		bool isCaseSensitive);

	void putRowList(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowData, uint64_t numRow, PutStatus &status);
	void getIndex(
			TransactionContext &txn, const IndexData &indexData, bool forNull,
			IndexAutoPtr &indexPtr) {
		return indexSchema_->getIndex(txn, indexData, forNull, this, indexPtr);
	}

	BtreeMap *getRowIdMap(TransactionContext &txn) const {
		return ALLOC_NEW(txn.getDefaultAllocator())
			BtreeMap(txn, *getObjectManager(),
				baseContainerImage_->rowIdMapOId_, *const_cast<AllocateStrategy*>(&mapAllocateStrategy_), 
				NULL, rowIdFuncInfo_);
	}

	void getMvccMap(
			TransactionContext &txn, const IndexData &indexData,
			BaseIndexStorage::AutoPtr<BtreeMap> &mvccMap) const {
		IndexStorageSet *storageSet = indexData.storageSet_;
		assert(storageSet != NULL);

		BaseIndexStorage *&indexStorage = storageSet->mvccIndexStorage_;
		mvccMap.initialize(indexStorage, getMvccMap(txn, &indexStorage));
	}

	BtreeMap* getMvccMap(
			TransactionContext &txn, BaseIndexStorage **indexStorage = NULL) const {
		return BaseIndexStorage::create<BtreeMap>(
				txn, *getObjectManager(), MAP_TYPE_BTREE,
				baseContainerImage_->mvccMapOId_,
				*const_cast<AllocateStrategy*>(&mapAllocateStrategy_), NULL,
				mvccFuncInfo_, indexStorage);
	}

	void setCreateRowId(TransactionContext &txn, RowId rowId);

	IndexCursor createCursor(TransactionContext &txn, const MvccRowImage &mvccImage);

	virtual void putRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowData, RowId &rowId, bool rowIdSpecified,
		PutStatus &status, PutRowOption putRowOption) = 0;
	virtual void putRowInternal(TransactionContext &txn,
		InputMessageRowStore *inputMessageRowStore, RowId &rowId,
		bool rowIdSpecified,
		PutStatus &status, PutRowOption putRowOption) = 0;
	virtual void getIdList(TransactionContext &txn,
		util::XArray<uint8_t> &serializedRowList,
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

	void getFields(TransactionContext &txn, 
		MessageRowStore* messageRowStore, 
		util::Vector<ColumnId> &columnIdList, util::XArray<KeyData> &fields);
	bool getKeyCondition(TransactionContext &txn, BtreeMap::SearchContext &sc,
		const Operator *&op1, const Operator *&op2) const;
	bool getKeyCondition(TransactionContext &txn, RtreeMap::SearchContext &sc,
		const Operator *&op1, const Operator *&op2) const;
	void getInitialSchemaStatus(uint32_t &columnNum, uint32_t &varColumnNum, uint32_t &rowFixedColumnSize) {
		return columnSchema_->getFirstSchema(columnNum, varColumnNum, rowFixedColumnSize);
	}

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
	template <typename R, typename S>
	void searchMvccMap(TransactionContext& txn, S &sc, ContainerRowScanner &scanner);
	template<typename R>
	void resolveExclusiveStatus(TransactionContext& txn);
	static bool getRowIdRangeCondition(
			util::StackAllocator &alloc,
			BtreeMap::SearchContext &sc, ContainerType containerType,
			RowId *startRowId, RowId *endRowId);
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
	template <typename R>
	std::string dumpImpl(TransactionContext &txn);
	template <typename R>
	bool validateImpl(TransactionContext &txn, std::string &errorMessage,
		RowId &preRowId, uint64_t &countRowNum, bool isCheckRowRate = false);


	template <typename R>
	void getRowIdListImpl(TransactionContext &txn, util::XArray<OId> &oIdList,
		util::XArray<RowId> &rowIdList);
	void addUpdatedRow(TransactionContext &txn, RowId rowId, OId oId) {
		rsNotifier_.addUpdatedRow(rowId, oId);
		UNUSED_VARIABLE(txn);
	}
	void addRemovedRow(TransactionContext &txn, RowId rowId, OId oId) {
		UNUSED_VARIABLE(txn);
		getDataStore()->getResultSetManager()->addRemovedRow(getContainerId(),
			rowId, oId);
	}
	void addRemovedRowArray(TransactionContext &txn, OId oId) {
		UNUSED_VARIABLE(txn);
		getDataStore()->getResultSetManager()->addRemovedRowArray(
			getContainerId(), oId);
	}
	virtual void setDummyMvccImage(TransactionContext &txn) = 0;

	void checkExclusive(TransactionContext &txn);

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

	bool getIndexData(TransactionContext &txn, const util::Vector<ColumnId> &columnIds,
		MapType mapType, bool withUncommitted, IndexData &indexData,
		bool withPartialMatch = false) const;
	bool getIndexData(TransactionContext &txn, IndexCursor &indexCursor,
		IndexData &indexData) const {
		return indexSchema_->getIndexData( 
			txn, indexCursor, indexData);
	}
	const void *getIndexValue(TransactionContext &txn, util::Vector<ColumnId> &columnIds,
		TreeFuncInfo *funcInfo, util::XArray<KeyData> &keyFieldList) {
		const void *value = NULL;
		if (keyFieldList.size() == 1) {
			ColumnInfo &columnInfo = getColumnInfo(columnIds[0]);
			KeyData keyData = keyFieldList[0];
			if (columnInfo.isVariable()) {
				uint32_t encodeSize = ValueProcessor::getEncodedVarSize(keyData.size_);
				value = static_cast<const uint8_t *>(keyData.data_) - encodeSize;
			} else {
				value = keyData.data_;
			}
		} else {
			value = 
				funcInfo->createCompositeInfo(txn.getDefaultAllocator(), keyFieldList);
		}
		return value;
	}

	void getIndexList(
		TransactionContext &txn, bool withUncommitted, util::XArray<IndexData> &list) const;

	void createNullIndexData(TransactionContext &txn, IndexData &indexData) {
		indexSchema_->createNullIndexData(txn, indexData, 
			this);
	}

	void finalizeIndex(TransactionContext &txn, bool alreadyRemoved) {
		if (!isExpired(txn) && !alreadyRemoved) {
			indexSchema_->dropAll(txn, this, true);
		}
		indexSchema_->finalize(txn);
	}

	bool isAlterContainer() const {
		return (baseContainerImage_->status_ & ALTER_CONTAINER_BIT) != 0;
	}
	void setAlterContainer() {
		uint8_t &dest = baseContainerImage_->status_;
		dest = static_cast<uint8_t>(dest | ALTER_CONTAINER_BIT);
	}
	void resetAlterContainer() {
		uint8_t &dest = baseContainerImage_->status_;
		dest = static_cast<uint8_t>(dest & ALTER_CONTAINER_BIT);
	}
	bool isNullsStatsStatus() const {
		return (baseContainerImage_->status_ & NULLS_STATS_BIT) != 0;
	}
	void setNullsStatsStatus() {
		baseContainerImage_->status_ |= NULLS_STATS_BIT;
	}

	void convertRowArraySchema(TransactionContext &txn, RowArray &rowArray, bool isMvcc,
		RowId searchRowId = UNDEF_ROWID, BtreeMap::SearchContext* sc = NULL);	

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
		return getContainerExpirationEndTime(baseContainerImage_->startTime_, *info);
	}
	ContainerExpirationInfo* getContainerExpirationInfo() const {
		return commonContainerSchema_->get<ContainerExpirationInfo>(META_TYPE_CONTAINER_DURATION);
	}
	int64_t getContainerExpirationDuration() const {
		ContainerExpirationInfo *info =  commonContainerSchema_->get<ContainerExpirationInfo>(META_TYPE_CONTAINER_DURATION);
		if (info != NULL) {
			return info->info_.duration_;
		} else {
			return INT64_MAX;
		}
	}
	static Timestamp getContainerExpirationEndTime(Timestamp startTime, const ContainerExpirationInfo& info) {
		Timestamp endTime = startTime + info.interval_ - 1;
		if (startTime <= endTime) {
			return endTime;
		}
		else {
			return MAX_TIMESTAMP;
		}
	}

	static ChunkKey calcChunkKey(Timestamp baseTime, int64_t duration) {
		if (duration <= 0) {
			GS_THROW_USER_ERROR(
				GS_ERROR_DS_CM_EXPIRATION_TIME_INVALID, "Invalid duration : " << duration);
		}
		ChunkKey chunkKey = MAX_CHUNK_KEY;
		ExpireIntervalCategoryId expireCategoryId = DataStoreV4::DataAffinityUtils::calcExpireIntervalCategoryId(duration);
		if (baseTime + duration + EXPIRE_MARGIN >
			baseTime) {
			uint64_t roundingBitNum = DataStoreV4::DataAffinityUtils::getExpireTimeRoundingBitNum(expireCategoryId);
			chunkKey = DataStoreV4::DataAffinityUtils::convertTimestamp2ChunkKey(
				baseTime + duration + EXPIRE_MARGIN,
				roundingBitNum, true);
		}
		return chunkKey;
	}

	const uint8_t *getAffinityBinary() const {
		return commonContainerSchema_->get<uint8_t>(META_TYPE_AFFINITY);
	}

	template<typename C>
	static SchemaFeatureLevel resolveSchemaFeatureLevel(const C &container);

protected:  
	void setContainerInvalid() {
		baseContainerImage_->status_ |= CONTAINER_INVALID_BIT;
	}

	uint16_t getSmallRowArrayNum() const {
		if (getNormalRowArrayNum() < SMALL_ROW_ARRAY_MAX_SIZE) {
			return getNormalRowArrayNum();
		} else {
			return SMALL_ROW_ARRAY_MAX_SIZE;
		}
	}

	void incrementRowNum() {
		baseContainerImage_->rowNum_++;
	}
	void decrementRowNum() {
		baseContainerImage_->rowNum_--;
	}

	void insertRowIdMap(TransactionContext &txn, BtreeMap *map,
		const void *constKey, OId oId);
	void insertMvccMap(TransactionContext &txn, BtreeMap *map,
		TransactionId tId, MvccRowImage &mvccImage);
	void insertValueMap(TransactionContext &txn, ValueMap &valueMap,
		const void *constKey, OId oId, bool isNull);
	void updateRowIdMap(TransactionContext &txn, BtreeMap *map,
		const void *constKey, OId oldOId, OId newOId);
	void updateMvccMap(TransactionContext &txn, BtreeMap *map,
		TransactionId tId, MvccRowImage &oldMvccImage,
		MvccRowImage &newMvccImage);
	void updateValueMap(TransactionContext &txn, ValueMap &valueMap,
		const void *constKey, OId oldOId, OId newOId, bool isNull);
	void removeRowIdMap(TransactionContext &txn, BtreeMap *map,
		const void *constKey, OId oId);
	void removeMvccMap(TransactionContext &txn, BtreeMap *map,
		TransactionId tId, MvccRowImage &mvccImage);
	void removeValueMap(TransactionContext &txn, ValueMap &valueMap,
		const void *constKey, OId oId, bool isNull);

	void updateIndexData(
		TransactionContext &txn, const IndexData &indexData);
	void updateValueMaps(TransactionContext &txn, 
		const util::XArray<std::pair<OId, OId> > &moveList);

	void handleUpdateError(
		TransactionContext &txn, std::exception &e, ErrorCode errorCode);
	void handleInvalidError(
		TransactionContext &txn, SystemException &e, ErrorCode errorCode);

	virtual void lockIdList(TransactionContext &txn, util::XArray<OId> &oIdList,
		util::XArray<RowId> &idList) = 0;

	static bool hasTermConditionUpdator(BtreeMap::SearchContext &sc);
	static bool hasTermConditionUpdator(RtreeMap::SearchContext &sc);
};

/*!
	@brief Auto_ptr for Container object
*/
class ContainerAutoPtr {
public:
	ContainerAutoPtr(TransactionContext& txn, DataStoreV4* dataStore,
		OId oId, uint8_t containerType, bool allowExpiration)
		: stackAutoPtr_(txn.getDefaultAllocator()) {
		if (oId == UNDEF_OID) {
			stackAutoPtr_.set(NULL);
			return;
		}
		util::StackAllocator& alloc = txn.getDefaultAllocator();
		KeyDataStoreValue keyStoreValue(UNDEF_CONTAINERID, oId, dataStore->getStoreType(), CONTAINER_ATTR_ANY);
		DSInputMes input(alloc, DS_GET_CONTAINER_OBJECT, containerType, allowExpiration);
		StackAllocAutoPtr<DSContainerOutputMes> ret(alloc, 
			static_cast<DSContainerOutputMes*>(dataStore->exec(&txn, &keyStoreValue, &input)));
		stackAutoPtr_.set(ret.get()->releaseContainerPtr());
	}
	ContainerAutoPtr(TransactionContext& txn, BaseContainer* container)
		: stackAutoPtr_(txn.getDefaultAllocator()) {
		stackAutoPtr_.set(container);
	}

	ContainerAutoPtr(TransactionContext &txn, DataStoreV4 *dataStore,
		const ContainerCursor &containerCursor)
		: stackAutoPtr_(txn.getDefaultAllocator()) {
		bool allowExpiration = true;

		util::StackAllocator& alloc = txn.getDefaultAllocator();
		KeyDataStoreValue keyStoreValue(UNDEF_CONTAINERID, containerCursor.getContainerOId(),
			dataStore->getStoreType(), CONTAINER_ATTR_ANY);
		DSInputMes input(alloc, DS_GET_CONTAINER_OBJECT, ANY_CONTAINER, allowExpiration);
		StackAllocAutoPtr<DSContainerOutputMes> ret(alloc,
			static_cast<DSContainerOutputMes*>(dataStore->exec(&txn, &keyStoreValue, &input)));
		stackAutoPtr_.set(ret.get()->releaseContainerPtr());
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
	template<typename Container>
	const void *getFields(TransactionContext& txn, 
		TreeFuncInfo *funcInfo, bool &isNullValue);
	template<typename Container>
	void getFields(TransactionContext& txn, util::Vector<ColumnId> &columnIds,
		util::XArray<KeyData> &fields);

	void lock(TransactionContext &txn);
	void setFirstUpdate();
	void resetFirstUpdate();
	bool isFirstUpdate() const;
	TransactionId getTxnId() const;
	static bool isRemoved(RowHeader *binary);

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
	friend class ContainerRowScanner;

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
	const void *getFields(TransactionContext& txn, 
		TreeFuncInfo *funcInfo, bool &isNullValue);
	void getFields(TransactionContext& txn, util::Vector<ColumnId> &columnIds,
		util::XArray<KeyData> &fields);

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

class ContainerRowScanner {
private:
	typedef BaseContainer::RowArray RowArray;
	typedef BaseContainer::RowArrayType RowArrayType;

	typedef bool (*HandlerFunc)(
			TransactionContext&, BaseContainer&, RowArray&,
			const OId*, const OId*, void*);

public:
	struct HandlerEntry {
	public:
		HandlerEntry();
		bool isAvailable() const;

	private:
		friend class ContainerRowScanner;
		template<bool ForRowArray> HandlerFunc getHandler();

		HandlerFunc rowHandler_;
		HandlerFunc rowArrayHandler_;
		void *handlerValue_;
	};

	struct HandlerSet {
	public:
		template<RowArrayType T> HandlerEntry& getEntry();
		HandlerEntry& getEntry(RowArrayType type);

	private:
		HandlerEntry generalEntry_;
		HandlerEntry plainEntry_;
	};

	typedef util::Vector<Value> VirtualValueList;

	ContainerRowScanner(
			const HandlerSet &handlerSet, RowArray &rowArray,
			VirtualValueList *virtualValueList);

	bool scanRowUnchecked(
			TransactionContext &txn, BaseContainer &container,
			RowArray *loadedRowArray, const OId *begin, const OId *end);

	bool scanRowArrayUnchecked(
			TransactionContext &txn, BaseContainer &container,
			RowArray *loadedRowArray, const OId *begin, const OId *end);

	RowArray& getRowArray();
	Value& getVirtualValue(size_t index);

	template<typename Handler>
	static HandlerEntry createHandlerEntry(Handler &handler);

private:
	ContainerRowScanner(const ContainerRowScanner&);
	ContainerRowScanner& operator=(const ContainerRowScanner&);

	template<bool ForRowArray> bool scanUnchecked(
			TransactionContext &txn, BaseContainer &container,
			RowArray *loadedRowArray, const OId *begin, const OId *end);

	template<typename Handler, RowArrayType T>
	static bool scanRowSpecific(
			TransactionContext &txn, BaseContainer &container,
			RowArray &rowArray, const OId *begin, const OId *end,
			void *handlerValue);

	template<typename Handler, RowArrayType T>
	static bool scanRowArraySpecific(
			TransactionContext &txn, BaseContainer &container,
			RowArray &rowArray, const OId *begin, const OId *end,
			void *handlerValue);

	HandlerSet handlerSet_;
	RowArray &rowArray_;
	VirtualValueList *virtualValueList_;
};

template<typename C>
SchemaFeatureLevel BaseContainer::resolveSchemaFeatureLevel(
		const C &container) {
	const uint32_t columnNum = container.getColumnNum();

	SchemaFeatureLevel level = 1;
	for (uint32_t i = 0; i < columnNum; i++) {
		level = std::max(
				level,
				resolveColumnSchemaFeatureLevel(container.getColumnInfo(i)));
	}

	return level;
}

inline bool ContainerRowScanner::scanRowUnchecked(
		TransactionContext &txn, BaseContainer &container,
		RowArray *loadedRowArray, const OId *begin, const OId *end) {
	const bool forRowArray = false;
	return scanUnchecked<forRowArray>(
			txn, container, loadedRowArray, begin, end);
}

inline bool ContainerRowScanner::scanRowArrayUnchecked(
		TransactionContext &txn, BaseContainer &container,
		RowArray *loadedRowArray, const OId *begin, const OId *end) {
	const bool forRowArray = true;
	return scanUnchecked<forRowArray>(
			txn, container, loadedRowArray, begin, end);
}

template<bool ForRowArray>
bool ContainerRowScanner::scanUnchecked(
		TransactionContext &txn, BaseContainer &container,
		RowArray *loadedRowArray, const OId *begin, const OId *end) {
	assert(loadedRowArray == NULL || loadedRowArray == &rowArray_);

	if (loadedRowArray == NULL) {
		for (const OId *it = begin; it != end; ++it) {
			rowArray_.load(txn, *it, &container, OBJECT_READ_ONLY);

			HandlerEntry &entry =
					handlerSet_.getEntry(rowArray_.getRowArrayType());

			if (!entry.getHandler<ForRowArray>()(
					txn, container, rowArray_, it, it + 1,
					entry.handlerValue_)) {
				return false;
			}
		}
	}
	else {
		HandlerEntry &entry =
				handlerSet_.getEntry(rowArray_.getRowArrayType());
		if (begin != end) {
			rowArray_.load(txn, *begin, &container, OBJECT_READ_ONLY);
		}

		if (!entry.getHandler<ForRowArray>()(
				txn, container, rowArray_, begin, end, entry.handlerValue_)) {
			return false;
		}
	}

	return true;
}

inline BaseContainer::RowArray& ContainerRowScanner::getRowArray() {
	return rowArray_;
}

inline Value& ContainerRowScanner::getVirtualValue(size_t index) {
	assert(virtualValueList_ != NULL);
	return (*virtualValueList_)[index];
}

template<typename Handler>
ContainerRowScanner::HandlerEntry ContainerRowScanner::createHandlerEntry(
		Handler &handler) {
	const RowArrayType rowArrayType = Handler::ROW_ARRAY_TYPE;
	HandlerEntry entry;
	entry.rowHandler_ = &scanRowSpecific<Handler, rowArrayType>;
	entry.rowArrayHandler_ = &scanRowArraySpecific<Handler, rowArrayType>;
	entry.handlerValue_ = &handler;
	return entry;
}

template<typename Handler, RowArrayType T>
bool ContainerRowScanner::scanRowSpecific(
		TransactionContext &txn, BaseContainer &container,
		RowArray &rowArray, const OId *begin, const OId *end,
		void *handlerValue) {
	Handler &handler = *static_cast<Handler*>(handlerValue);

	typename BaseContainer::RowArrayImpl<BaseContainer, T> *impl =
			rowArray.getImpl<BaseContainer, T>();

	for (const OId *it = begin;;) {
		assert(impl->getOId() == *it);
		assert(impl->getRowArrayType() == T ||
				T == BaseContainer::ROW_ARRAY_GENERAL);

		handler(txn, *impl);

		if (++it == end) {
			break;
		}
		impl->load(txn, *it, &container, OBJECT_READ_ONLY);
	}

	return handler.update(txn);
}

template<typename Handler, RowArrayType T>
bool ContainerRowScanner::scanRowArraySpecific(
		TransactionContext &txn, BaseContainer &container,
		RowArray &rowArray, const OId *begin, const OId *end,
		void *handlerValue) {
	Handler &handler = *static_cast<Handler*>(handlerValue);

	typename BaseContainer::RowArrayImpl<BaseContainer, T> *impl =
			rowArray.getImpl<BaseContainer, T>();

	for (const OId *it = begin;;) {
		assert(impl->getRowArrayType() == T ||
				T == BaseContainer::ROW_ARRAY_GENERAL);

		do {
			impl->resetCursor();
			if (impl->row_.isRemoved() && !impl->next()) {
				break;
			}

			uint32_t rest = impl->getRowNum() - impl->elemCursor_;
			for (;;) {
				handler(txn, *impl);

				if (--rest <= 0) {
					break;
				}
				impl->nextCursor();
				if (impl->row_.isRemoved()) {
					if (!impl->next()) {
						break;
					}
					rest = impl->getRowNum() - impl->elemCursor_;
				}
			}

			if (!handler.update(txn)) {
				return false;
			}
		}
		while (false);

		if (++it == end) {
			break;
		}
		impl->load(txn, *it, &container, OBJECT_READ_ONLY);
	}

	return true;
}

template<bool ForRowArray>
inline ContainerRowScanner::HandlerFunc
ContainerRowScanner::HandlerEntry::getHandler() {
	return (ForRowArray ? rowArrayHandler_ : rowHandler_);
}

template<>
inline ContainerRowScanner::HandlerEntry&
ContainerRowScanner::HandlerSet::getEntry<BaseContainer::ROW_ARRAY_GENERAL>() {
	return generalEntry_;
}

template<>
inline ContainerRowScanner::HandlerEntry&
ContainerRowScanner::HandlerSet::getEntry<BaseContainer::ROW_ARRAY_PLAIN>() {
	return plainEntry_;
}

inline ContainerRowScanner::HandlerEntry&
ContainerRowScanner::HandlerSet::getEntry(RowArrayType type) {
	if (type == BaseContainer::ROW_ARRAY_GENERAL) {
		return getEntry<BaseContainer::ROW_ARRAY_GENERAL>();
	}
	else {
		assert(type == BaseContainer::ROW_ARRAY_PLAIN);
		return getEntry<BaseContainer::ROW_ARRAY_PLAIN>();
	}
}

#endif
