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
#include "btree_map.h"
#include "data_type.h"
#include "hash_map.h"
#include "value_operator.h"
#include "value_processor.h"
#include "util/trace.h"
#include "data_store.h"
#include "message_row_store.h"
#include "schema.h"
#include "container_key.h"


UTIL_TRACER_DECLARE(BASE_CONTAINER);

class MessageSchema;
class ResultSet;
class ContainerRowScanner;


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
		Timestamp expiredTime_;	
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

public:  
public:  
	virtual ~BaseContainer() {
		ALLOC_DELETE((alloc_), commonContainerSchema_);
		ALLOC_DELETE((alloc_), columnSchema_);
		ALLOC_DELETE((alloc_), indexSchema_);
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
		util::XArray<uint8_t> &containerSchema, bool optionIncluded = true);

	virtual void putRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowData, RowId &rowId, DataStore::PutStatus &status,
		PutRowOption putRowOption) = 0;
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
		return columnSchema_->getRowFixedSize();
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
		nullsList.push_back(indexSchema_->getNullsStats(), 
			indexSchema_->getNullbitsSize());
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


	ColumnSchemaId getColumnSchemaId() const {
		return baseContainerImage_->columnSchemaOId_;
	}

	uint32_t getRowSize() const {
		return rowImageSize_;
	}
	uint32_t getRowFixedDataSize() const {
		return rowFixedDataSize_;
	}
	uint32_t getRowFixedSize() const {
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

protected:  
	/*!
		@brief Mode of operation of put
	*/
	enum PutMode { UNDEFINED_STATUS, APPEND, UPDATE, INSERT, NOT_EXECUTED };
	/*!
		@brief Status of Mvcc map, with regard to the transactions
	*/
	enum MvccStatus {
		EXCLUSIVE,					
		NOT_EXCLUSIVE_CREATE_EXIST,  
		NOT_EXCLUSIVE,  
		UNKNOWN
	};
	static const uint16_t ROW_ARRAY_MAX_SIZE = 50;  
	static const uint16_t SMALL_ROW_ARRAY_MAX_SIZE =
		10;  
	static const uint16_t MVCC_ROW_ARRAY_MAX_SIZE = 1;

	static const uint8_t CONTAINER_INVALID_BIT =
		0x1;  
	static const uint8_t ALTER_CONTAINER_BIT =
		0x2; 

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
	FullContainerKeyCursor containerKeyCursor_;
	bool isCompressionErrorMode_;  
	static const int8_t NULL_VALUE;
protected:  
	BaseContainer(TransactionContext &txn, DataStore *dataStore, OId oId)
		: BaseObject(
			  txn.getPartitionId(), *(dataStore->getObjectManager()), oId),
		  exclusiveStatus_(UNKNOWN),
		  alloc_(txn.getDefaultAllocator()),
		  dataStore_(dataStore),
		  containerKeyCursor_(txn.getPartitionId(), *(dataStore->getObjectManager())),
		  isCompressionErrorMode_(false) {
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
		  containerKeyCursor_(txn.getPartitionId(), *(dataStore->getObjectManager())),
		  isCompressionErrorMode_(false) {}

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
		util::XArray<ShareValueList::ElemData> &list, uint32_t &allocateSize);

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

	bool isSupportIndex(const IndexInfo &indexInfo) const;
	IndexCursor createCursor(TransactionContext &txn, const MvccRowImage &mvccImage);

	virtual void putRow(TransactionContext &txn,
		InputMessageRowStore *inputMessageRowStore, RowId &rowId,
		DataStore::PutStatus &status, PutRowOption putRowOption) = 0;
	virtual void getIdList(TransactionContext &txn,
		util::XArray<uint8_t> &serializedRowList,
		util::XArray<RowId> &idList) = 0;
	virtual void lockIdList(TransactionContext &txn, util::XArray<OId> &oIdList,
		util::XArray<RowId> &idList) = 0;
	void getCommonContainerOptionInfo(util::XArray<uint8_t> &containerSchema);
	virtual void getContainerOptionInfo(
		TransactionContext &txn, util::XArray<uint8_t> &containerSchema) = 0;
	virtual void checkContainerOption(MessageSchema *messageSchema,
		util::XArray<uint32_t> &copyColumnMap,
		bool &isCompletelySameSchema) = 0;
	virtual uint32_t calcRowImageSize(uint32_t rowFixedSize) = 0;
	virtual uint32_t calcRowFixedDataSize() = 0;
	virtual uint16_t calcRowArrayNum(uint16_t baseRowNum) = 0;

	template <typename R>
	void indexInsertImpl(TransactionContext &txn, IndexData &indexData,
		bool isImmediate);

	bool getKeyCondition(TransactionContext &txn, BtreeMap::SearchContext &sc,
		const Operator *&op1, const Operator *&op2) const;
	bool getKeyCondition(TransactionContext &txn, HashMap::SearchContext &sc,
		const Operator *&op1, const Operator *&op2) const;
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
	template <class R, class S>
	void searchMvccMap(TransactionContext &txn,
		S &sc, util::XArray<OId> &resultList, bool isCheckOnly);
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

	inline bool isExclusive() const {
		return exclusiveStatus_ == EXCLUSIVE;
	}
	inline bool isExclusiveUpdate() const {
		return (exclusiveStatus_ == EXCLUSIVE ||
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
		DataStore::PutStatus &status, bool isCaseSensitive = false)
		: stackAutoPtr_(txn.getDefaultAllocator()) {
		BaseContainer *container =
			dataStore->putContainer(txn, pId, containerKey, containerType,
				schemaSize, containerSchema, isEnable, status, isCaseSensitive);
		stackAutoPtr_.set(container);
	}

	ContainerAutoPtr(TransactionContext &txn, DataStore *dataStore,
		PartitionId pId, const FullContainerKey &containerKey,
		uint8_t containerType, bool isCaseSensitive = false)
		: stackAutoPtr_(txn.getDefaultAllocator()) {
		BaseContainer *container =
			dataStore->getContainer(txn, pId, containerKey, containerType, isCaseSensitive);
		stackAutoPtr_.set(container);
	}
	ContainerAutoPtr(TransactionContext &txn, DataStore *dataStore,
		PartitionId pId, const ContainerCursor &containerCursor)
		: stackAutoPtr_(txn.getDefaultAllocator()) {
		BaseContainer *newContainer =
			dataStore->getContainer(txn, pId, containerCursor);
		stackAutoPtr_.set(newContainer);
	}

	ContainerAutoPtr(TransactionContext &txn, DataStore *dataStore,
		PartitionId pId, ContainerId containerId, uint8_t containerType)
		: stackAutoPtr_(txn.getDefaultAllocator()) {
		BaseContainer *container =
			dataStore->getContainer(txn, pId, containerId, containerType);
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


#endif
