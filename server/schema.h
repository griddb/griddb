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
	@brief Definition of Schemas for container
*/
#ifndef SCHEMA_H_
#define SCHEMA_H_

#include "data_type.h"
#include <float.h>
#include "base_object.h"
#include "object_manager.h"  
#include "value_operator.h"
#include "value_processor.h"

class TransactionContext;
class BaseIndex;
class BtreeMap;
class HashMap;
class RtreeMap;
class MessageSchema;
class MessageTimeSeriesSchema;
class BaseContainer;
typedef uint64_t ColumnSchemaId;
typedef uint8_t IndexTypes;

const ColumnSchemaId UNDEF_COLUMN_SCHEMAID =
	std::numeric_limits<ColumnSchemaId>::max();

const uint64_t UNDEF_CONTAINER_POS = UINT64_MAX;
const uint16_t UNDEF_INDEX_POS = 0xffff;


/*!
	@brief Object for Container field value
*/
class ContainerValue {
public:
	ContainerValue(TransactionContext &txn, ObjectManager &objectManager)
		: baseObj_(txn.getPartitionId(), objectManager) {}
	BaseObject &getBaseObject() {
		return baseObj_;
	}
	const Value &getValue() {
		return value_;
	}
	void set(const void *data, ColumnType type) {
		value_.set(data, type);
	}
	void set(RowId rowId) {
		value_.set(rowId);
	}

private:
	BaseObject baseObj_;
	Value value_;

private:
	ContainerValue(const ContainerValue &);  
	ContainerValue &operator=(
		const ContainerValue &);  
};

/*!
	@brief link-based array object
*/
template <typename H, typename V>
class LinkArray : public BaseObject {
public:
	static const uint64_t DEFALUT_RESERVE_NUM = 1;

public:
	LinkArray(TransactionContext &txn, ObjectManager &objectManager)
		: BaseObject(txn.getPartitionId(), objectManager) {}
	LinkArray(TransactionContext &txn, ObjectManager &objectManager, OId oId)
		: BaseObject(txn.getPartitionId(), objectManager, oId) {}
	void initialize(TransactionContext &txn, uint64_t reserveNum,
		const AllocateStrategy &allocateStrategy) {
		BaseObject::allocate<uint8_t>(getAllocateSize(reserveNum),
			allocateStrategy, getBaseOId(), OBJECT_TYPE_CONTAINER_ID);
		setReserveNum(reserveNum);
		setNum(0);
	}

	/*!
		@brief Free Objects related to LinkArray
	*/
	void finalize(TransactionContext &txn) {
		setDirty();
		V *head = getElemHead();
		if (getReserveNum() >= MAX_LOCAL_ELEMENT_NUM) {
			OId nextOId = getNextOId(head);
			while (nextOId != UNDEF_OID) {
				BaseObject chainObj(
					txn.getPartitionId(), *getObjectManager(), nextOId);

				nextOId = getNextOId(chainObj.getBaseAddr<V *>());
				chainObj.finalize();
			}
		}
		BaseObject::finalize();
	}

	/*!
		@brief Get array length
	*/
	uint64_t getNum() const {
		return *getNumPtr();
	}
	/*!
		@brief Get the element
	*/
	const V *get(TransactionContext &txn, uint64_t pos);
	/*!
		@brief Insert the element
	*/
	void insert(TransactionContext &txn, uint64_t pos, const V *value,
		const AllocateStrategy &allocateStrategy);
	/*!
		@brief Update the element
	*/
	void update(TransactionContext &txn, uint64_t pos, const V *value);
	/*!
		@brief Remove the element
	*/
	void remove(TransactionContext &txn, uint64_t pos);
	/*!
		@brief Get header value
	*/
	const H *getHeader() {
		return reinterpret_cast<H *>(getBaseAddr());
	}
	/*!
		@brief Set header value
	*/
	void setHeader(H *header) {
		setDirty();
		*reinterpret_cast<H *>(getBaseAddr()) = *header;
	}
	void dump(TransactionContext &txn);

private:
	static const int32_t LIST_EXP_SIZE = 13;  
	static const int32_t CHAIN_BIT;
	static const int32_t SUB_ELEM_BIT_MASK;
	static const uint64_t MAX_LOCAL_ELEMENT_NUM;

private:
	static uint32_t getAllocateSize(uint64_t reserveNum) {
		return static_cast<uint32_t>(getHeaderSize() + (sizeof(uint64_t) * 2) +
									 (sizeof(V) * reserveNum));
	}
	/*!
		@brief Check if reserved size is full to capacity
	*/
	bool isFull() const {
		return (getNum() == getReserveNum());
	}
	uint64_t getReserveNum() const {
		return *getReserveNumPtr();
	}
	uint64_t *getNumPtr() const {
		return reinterpret_cast<uint64_t *>(getBaseAddr() + getHeaderSize());
	}
	uint64_t *getReserveNumPtr() const {
		return reinterpret_cast<uint64_t *>(
			getBaseAddr() + getHeaderSize() + sizeof(uint64_t));
	}
	void increment() {
		(*getNumPtr())++;
	}
	void decrement() {
		(*getNumPtr())--;
	}

	void setNum(uint64_t num) {
		*getNumPtr() = num;
	}
	void setReserveNum(uint64_t reserverNum) {
		*getReserveNumPtr() = reserverNum;
	}

	uint64_t getElementHeaderOffset() const {
		return getHeaderSize() + sizeof(uint64_t) * 2;
	}
	V *getElemHead() const {
		return reinterpret_cast<V *>(getBaseAddr() + getElementHeaderOffset());
	}

	void getChainList(
		TransactionContext &txn, uint64_t chainNo, BaseObject &chain);
	void getChainList(
		TransactionContext &txn, uint64_t chainNo, UpdateBaseObject &chain);

	uint64_t getChainElemNum(uint64_t chainNo) const {
		if (getNum() == 0 || chainNo >= getChainNum()) {
			return 0;
		}
		uint64_t rest = (getNum() & SUB_ELEM_BIT_MASK);
		if (getChainNum() == chainNo + 1 && rest != 0) {
			return rest;
		}
		else {
			return MAX_LOCAL_ELEMENT_NUM;
		}
	}
	uint64_t getChainNum() const;
	OId getNextOId(V *head) const {
		return *reinterpret_cast<OId *>(head + MAX_LOCAL_ELEMENT_NUM);
	}
	void setNextOId(V *head, OId nextOId) {
		OId *addr = reinterpret_cast<OId *>(head + MAX_LOCAL_ELEMENT_NUM);
		*addr = nextOId;
	}

	static uint64_t getHeaderSize();
	OId expand(
		TransactionContext &txn, const AllocateStrategy &allocateStrategy);
};

typedef uint16_t CONTAINER_META_TYPE;
const CONTAINER_META_TYPE META_TYPE_COLUMN_SCHEMA = 0;  
const CONTAINER_META_TYPE META_TYPE_RESERVED = 1;
const CONTAINER_META_TYPE META_TYPE_DURATION = 2;	
const CONTAINER_META_TYPE META_TYPE_AFFINITY = 3;	
const CONTAINER_META_TYPE META_TYPE_TRIGGER = 4;	 
const CONTAINER_META_TYPE META_TYPE_ATTRIBUTES = 5;  
const CONTAINER_META_TYPE META_TYPE_MAX = 6;		 

/*!
	@brief Key value list Object
*/
class ShareValueList : public BaseObject {
public:
	struct ElemData {
		CONTAINER_META_TYPE type_;
		uint32_t size_;
		uint8_t *binary_;
		ElemData() : type_(META_TYPE_MAX), size_(0), binary_(NULL) {}
		ElemData(CONTAINER_META_TYPE type)
			: type_(type), size_(0), binary_(NULL) {}
	};

public:
	ShareValueList(TransactionContext &txn, ObjectManager &objectManager)
		: BaseObject(txn.getPartitionId(), objectManager) {}
	ShareValueList(
		TransactionContext &txn, ObjectManager &objectManager, OId oId)
		: BaseObject(txn.getPartitionId(), objectManager, oId) {}
	void initialize(TransactionContext &txn, uint32_t allocateSize,
		const AllocateStrategy &allocateStrategy);
	void set(int64_t hashVal, util::XArray<ElemData> &list);

	void finalize();

	template <typename K>
	K *get(CONTAINER_META_TYPE type) const;

	int32_t getNum() const {
		return *getNumPtr();
	}
	void increment() {
		setDirty();
		(*getRefCounterPtr())++;
	}
	void decrement() {
		setDirty();
		(*getRefCounterPtr())--;
	}
	int64_t getReferenceCounter() const {
		return *getRefCounterPtr();
	}

	int64_t getHashVal() const {
		return *getHashValPtr();
	}

	static uint32_t getHeaderSize(int32_t num) {
		return sizeof(int64_t) * 2 + sizeof(int32_t) +
			   num * (sizeof(CONTAINER_META_TYPE) + sizeof(uint32_t));
	}

private:
	int64_t *getHashValPtr() const {
		return reinterpret_cast<int64_t *>(getBaseAddr());
	}
	int64_t *getRefCounterPtr() const {
		return reinterpret_cast<int64_t *>(getBaseAddr() + sizeof(int64_t));
	}
	int32_t *getNumPtr() const {
		return reinterpret_cast<int32_t *>(getBaseAddr() + sizeof(int64_t) * 2);
	}

	uint8_t *put(int32_t pos, CONTAINER_META_TYPE type, uint32_t valueOffset);

	uint8_t *getElemHeader(int32_t pos) const {
		return getBaseAddr() + getHeaderSize(pos);
	}
};

/*!
	@brief Information of an index
*/
struct IndexInfo {
	uint32_t columnId;  
	MapType mapType;	
	IndexInfo() : columnId(0), mapType(MAP_TYPE_BTREE) {}
	IndexInfo(uint32_t columnID, MapType mapTYPE)
		: columnId(columnID), mapType(mapTYPE) {}
};

/*!
	@brief Information of column
*/
class ColumnInfo  
{
public:
	static const ColumnId ROW_KEY_COLUMN_ID = 0;  

	ColumnInfo() {}
	~ColumnInfo() {}

	ColumnId getColumnId() const {
		return columnId_;
	}

	const char *getColumnName(
		TransactionContext &txn, ObjectManager &objectManager) {
		BaseObject nameObject(
			txn.getPartitionId(), objectManager, columnNameOId_);
		size_t len = strlen(nameObject.getBaseAddr<const char *>()) + 1;
		char *name = ALLOC_NEW(txn.getDefaultAllocator()) char[len];
		memcpy(name, nameObject.getBaseAddr<const char *>(), len);
		return reinterpret_cast<const char *>(name);
	}

	ColumnType getColumnType() const {
		return columnType_;
	}

	/*!
		@brief Get element Column type from Array Column type
	*/
	ColumnType getSimpleColumnType() const {
		return ValueProcessor::getSimpleColumnType(getColumnType());
	}

	uint32_t getColumnSize() const {
		return columnSize_;
	}

	uint32_t getColumnOffset() const {
		return offset_;
	}

	bool isKey() const {
		return isKey_ != 0;
	}

	bool isArray() const {
		return ValueProcessor::isArray(getColumnType());
	}

	bool isVariable() const {
		return (getColumnType() == COLUMN_TYPE_STRING ||
				getColumnType() == COLUMN_TYPE_BLOB ||
				ValueProcessor::isArray(getColumnType()));
	}

	/*!
		@brief Check if column type is special(Blob or StringArray)
	*/
	bool isSpecialVariable() const {
		return (getColumnType() == COLUMN_TYPE_BLOB ||
				getColumnType() == COLUMN_TYPE_STRING_ARRAY);
	}

	void initialize();
	void set(TransactionContext &txn, ObjectManager &objectManager,
		uint32_t toColumnId, uint32_t fromColumnId,
		MessageSchema *messageSchema, const AllocateStrategy &allocateStrategy);

	void setType(ColumnType type, bool isArray) {
		if (isArray) {
			columnType_ = static_cast<ColumnType>(type + COLUMN_TYPE_OID + 1);
		}
		else {
			columnType_ = type;
		}
		columnSize_ = FixedSizeOfColumnType[columnType_];
	}

	void finalize(TransactionContext &txn, ObjectManager &objectManager);

	void getSchema(TransactionContext &txn, ObjectManager &objectManager,
		util::XArray<uint8_t> &schema);

	std::string dump(TransactionContext &txn, ObjectManager &objectManager);

	void setColumnId(uint16_t columnId) {
		columnId_ = columnId;
	}
	void setOffset(uint16_t offset) {
		offset_ = offset;
	}

private:
	OId columnNameOId_;	
	uint16_t columnId_;	
	uint16_t columnSize_;  
	uint16_t
		offset_;			 
	ColumnType columnType_;  
	uint8_t isKey_;			 
};

/*!
	@brief Information of Duration
*/
struct DurationInfo {
	int64_t timestampDuration_;  
	int32_t timeDuration_;		 
	TimeUnit timeUnit_;			 
	uint8_t padding1_;
	uint8_t padding2_;
	uint8_t padding3_;
	DurationInfo() {
		timestampDuration_ = INT64_MAX;  
		timeDuration_ = -1;				 
		timeUnit_ = TIME_UNIT_DAY;
		padding1_ = 0;
		padding2_ = 0;
		padding3_ = 0;
	}
	/*!
		@brief Calculate hash value
	*/
	int64_t getHashVal() const {
		int64_t hashVal = 0;
		hashVal += timestampDuration_;
		hashVal += timeDuration_;
		hashVal += timeUnit_;
		return hashVal;
	}
};


/*!
	@brief Information of trigger
*/
class TriggerInfo {
public:
	static const int32_t TRIGGER_VERSION = 1;

	TriggerInfo(util::StackAllocator &alloc);
	TriggerInfo(const TriggerInfo &info);

	static void encode(
		const TriggerInfo &info, util::XArray<uint8_t> &binaryTrigger);
	static void decode(const uint8_t *binaryTrigger, TriggerInfo &info);
	static void getSizeAndVersion(
		const uint8_t *binaryTrigger, uint32_t &size, int32_t &version);
	static bool compare(
		const uint8_t *binaryTrigger1, const uint8_t *binaryTrigger2);

	util::StackAllocator &alloc_;

	int32_t version_;

	int8_t type_;
	util::String name_;
	util::String uri_;
	int32_t operation_;
	util::XArray<ColumnId> columnIds_;

	int32_t jmsProviderType_;
	util::String jmsProviderTypeName_;

	int32_t jmsDestinationType_;
	util::String jmsDestinationTypeName_;

	util::String jmsDestinationName_;

	util::String jmsUser_;
	util::String jmsPassword_;
};

/*!
	@brief List of trigger
*/
class TriggerList {
public:
	static const uint32_t MAX_TRIGGER_LIST_NUM = 30;

	void initialize(uint32_t num);

	void set(TransactionContext &txn, ObjectManager &objectManager,
		util::XArray<const uint8_t *> &triggerList,
		const AllocateStrategy &allocateStrategy);

	void createImage(TransactionContext &txn, ObjectManager &objectManager,
		const TriggerInfo &info, util::XArray<const uint8_t *> &binary,
		size_t limit);

	bool updateImage(TransactionContext &txn, ObjectManager &objectManager,
		const util::XArray<const util::String *> &oldColumnNameList,
		const util::XArray<const util::String *> &newColumnNameList,
		util::XArray<const uint8_t *> &binary);

	bool removeImage(TransactionContext &txn, ObjectManager &objectManager,
		const char *name, util::XArray<const uint8_t *> &binary);

	void getList(TransactionContext &txn, ObjectManager &objectManager,
		util::XArray<const uint8_t *> &triggerList);

	void finalize(TransactionContext &txn, ObjectManager &objectManager);

	/*!
		@brief Calculate the size needed
	*/
	static uint32_t getAllocateSize(uint32_t num) {
		return sizeof(uint32_t) + (sizeof(OId) * num);
	}

private:
	uint32_t num_;

private:
	OId *getOIdList() {
		uint8_t *addr =
			reinterpret_cast<uint8_t *>(const_cast<TriggerList *>(this)) +
			sizeof(uint32_t);
		return reinterpret_cast<OId *>(addr);
	}
};

/*!
	@brief Information of Column layout
*/
class ColumnSchema  
{
public:
	void initialize(uint32_t columnNum);
	void set(TransactionContext &txn, ObjectManager &objectManager,
		MessageSchema *collectionSchema,
		const AllocateStrategy &allocateStrategy);
	void finalize(TransactionContext &txn, ObjectManager &objectManager);

	uint32_t getColumnNum() const {
		return columnNum_;
	}

	uint32_t getRowFixedSize() const {
		return rowFixedSize_;
	}

	ColumnInfo *getColumnInfoList() const {
		return reinterpret_cast<ColumnInfo *>(
			reinterpret_cast<uint8_t *>(const_cast<ColumnSchema *>(this)) +
			COLUMN_INFO_OFFSET);
	}

	ColumnInfo &getColumnInfo(uint32_t columnId) const {
		return getColumnInfoList()[columnId];
	}

	void getColumnInfo(TransactionContext &txn, ObjectManager &objectManager,
		const char *name, uint32_t &columnId, ColumnInfo *&columnInfo) const;

	uint32_t getVariableColumnNum() const {
		return variableColumnNum_;
	}

	/*!
		@brief Check if Row key is defined
	*/
	bool definedRowKey() const {
		return getColumnInfo(ColumnInfo::ROW_KEY_COLUMN_ID).isKey();
	}

	static int64_t calcSchemaHashKey(MessageSchema *messageSchema);
	bool schemaCheck(TransactionContext &txn, ObjectManager &objectManager,
		MessageSchema *messageSchema);
	/*!
		@brief Calculate the size needed
	*/
	static uint32_t getAllocateSize(uint32_t columnNum) {
		return COLUMN_INFO_OFFSET + (sizeof(ColumnInfo) * columnNum);
	}

private:
	static const uint32_t COLUMN_INFO_OFFSET =
		sizeof(int64_t) + sizeof(int32_t) + sizeof(uint32_t) +
		sizeof(uint16_t) + sizeof(uint16_t);
	uint32_t rowFixedSize_;  
	uint16_t columnNum_;
	uint16_t variableColumnNum_;
};

/*!
	@brief Information of an index
*/
struct IndexData {
	OId oId_;
	ColumnId columnId_;
	MapType mapType_;
};

/*!
	@brief Information of Index layout
*/
class IndexSchema : public BaseObject {
public:
	static const uint32_t INITIALIZE_RESERVE_NUM = 2;

public:
	IndexSchema(TransactionContext &txn, ObjectManager &objectManager,
		const AllocateStrategy &strategy)
		: BaseObject(txn.getPartitionId(), objectManager),
		  allocateStrategy_(strategy) {}
	IndexSchema(TransactionContext &txn, ObjectManager &objectManager, OId oId,
		const AllocateStrategy &strategy)
		: BaseObject(txn.getPartitionId(), objectManager, oId),
		  allocateStrategy_(strategy) {}

	void initialize(
		TransactionContext &txn, uint16_t reserveNum, uint16_t indexNum = 0);

	bool createIndexInfo(
		TransactionContext &txn, ColumnId columnId, MapType mapType);
	void dropIndexInfo(
		TransactionContext &txn, ColumnId columnId, MapType mapType);
	void createIndexData(TransactionContext &txn, ColumnId columnId,
		MapType mapType, ColumnType columnType, BaseContainer *container,
		uint64_t containerPos, bool isUnique);
	void dropIndexData(TransactionContext &txn, ColumnId columnId,
		MapType mapType, BaseContainer *container, uint64_t containerPos,
		bool isMapFinalize);
	void dropAll(TransactionContext &txn, BaseContainer *container,
		uint64_t containerPos, bool isMapFinalize);
	void finalize(TransactionContext &txn);

	uint16_t getIndexNum() const {
		return *getNumPtr();
	}
	void getIndexList(TransactionContext &txn, uint64_t containerPos,
		util::XArray<IndexData> &list) const;
	bool getIndexData(TransactionContext &txn, ColumnId columnId,
		MapType mapType, uint64_t containerPos, IndexData &indexData) const;

	bool hasIndex(
		TransactionContext &txn, ColumnId columnId, MapType mapType) const;
	static bool hasIndex(IndexTypes indexType, MapType mapType);
	IndexTypes getIndexTypes(TransactionContext &txn, ColumnId columnId) const;

	BaseIndex *getIndex(TransactionContext &txn, MapType mapType,
		ColumnId columnId, BaseContainer *container,
		uint64_t containerPos) const;
	void updateIndexData(
		TransactionContext &txn, IndexData indexData, uint64_t containerPos);

private:
/*!
	@brief Calculate the size needed
*/
	static uint32_t getAllocateSize(uint32_t reserveNum) {
		return INDEX_DATA_OFFSET + (getIndexDataSize() * reserveNum);
	}
	/*!
		@brief Check if reserved size is full to capacity
	*/
	bool isFull() const {
		return (getIndexNum() == getReserveNum());
	}
	/*!
		@brief Get reserved size
	*/
	uint16_t getReserveNum() const {
		return *getReserveNumPtr();
	}
	void expand(TransactionContext &txn);
	uint16_t *getNumPtr() const {
		return reinterpret_cast<uint16_t *>(getBaseAddr());
	}
	uint16_t *getReserveNumPtr() const {
		return reinterpret_cast<uint16_t *>(getBaseAddr() + sizeof(uint16_t));
	}
	void increment() {
		(*getNumPtr())++;
	}
	void decrement() {
		(*getNumPtr())--;
	}

	void setNum(uint16_t num) {
		*getNumPtr() = num;
	}
	void setReserveNum(uint16_t reserverNum) {
		*getReserveNumPtr() = reserverNum;
	}

	uint8_t *getElemHead() const {
		return reinterpret_cast<uint8_t *>(getBaseAddr() + INDEX_DATA_OFFSET);
	}

	static uint32_t getIndexDataSize() {
		return sizeof(OId) + sizeof(uint16_t) + sizeof(int8_t) + sizeof(int8_t);
	}

	uint16_t getNth(ColumnId columnId, MapType mapType) const {
		for (uint16_t i = 0; i < getIndexNum(); i++) {
			uint8_t *indexDataPos = getElemHead() + (getIndexDataSize() * i);
			if (getColumnId(indexDataPos) == columnId &&
				getMapType(indexDataPos) == mapType) {
				return i;
			}
		}
		return UNDEF_INDEX_POS;
	}

	void getIndexData(TransactionContext &txn, uint16_t nth,
		uint64_t containerPos, IndexData &indexData) const {
		uint8_t *indexDataPos = getElemHead() + (getIndexDataSize() * nth);
		indexData.oId_ = getOId(txn, indexDataPos, containerPos);
		indexData.columnId_ = getColumnId(indexDataPos);
		indexData.mapType_ = getMapType(indexDataPos);
	}

	void setIndexData(TransactionContext &txn, uint16_t nth,
		uint64_t containerPos, IndexData &indexData) {
		uint8_t *indexDataPos =
			const_cast<uint8_t *>(getElemHead()) + (getIndexDataSize() * nth);
		setOId(txn, indexDataPos, containerPos, indexData.oId_);
		setColumnId(indexDataPos, indexData.columnId_);
		setMapType(indexDataPos, indexData.mapType_);
	}

	void setIndexInfo(TransactionContext &txn, uint16_t nth, ColumnId columnId,
		MapType mapType) {
		IndexData indexData;
		indexData.columnId_ = columnId;
		indexData.mapType_ = mapType;
		indexData.oId_ = UNDEF_OID;
		setIndexData(txn, nth, UNDEF_CONTAINER_POS, indexData);
	}

	OId getOId(
		TransactionContext &txn, uint8_t *cursor, uint64_t containerPos) const {
		if (containerPos == UNDEF_CONTAINER_POS) {
			return *reinterpret_cast<OId *>(cursor);
		}
		else {
			OId baseOId = getOId(txn, cursor, UNDEF_CONTAINER_POS);
			if (baseOId == UNDEF_OID) {
				return UNDEF_OID;
			}
			else {
				LinkArray<void, OId> linkArray(
					txn, *getObjectManager(), baseOId);
				return *linkArray.get(txn, containerPos);
			}
		}
	}
	ColumnId getColumnId(uint8_t *cursor) const {
		return *reinterpret_cast<uint16_t *>(cursor + sizeof(OId));
	}
	MapType getMapType(uint8_t *cursor) const {
		return static_cast<MapType>(*reinterpret_cast<int8_t *>(
			cursor + sizeof(OId) + sizeof(uint16_t)));
	}

	void setOId(TransactionContext &txn, uint8_t *cursor, uint64_t containerPos,
		OId oId) {
		if (containerPos == UNDEF_CONTAINER_POS) {
			*reinterpret_cast<OId *>(cursor) = oId;
		}
		else {
			OId baseOId = getOId(txn, cursor, UNDEF_CONTAINER_POS);
			if (baseOId == UNDEF_OID) {
				LinkArray<void, OId> linkArray(txn, *getObjectManager());
				linkArray.initialize(txn,
					LinkArray<void, OId>::DEFALUT_RESERVE_NUM,
					allocateStrategy_);
				*reinterpret_cast<OId *>(cursor) = linkArray.getBaseOId();
				linkArray.insert(txn, containerPos, &oId, allocateStrategy_);
			}
			else {
				LinkArray<void, OId> linkArray(
					txn, *getObjectManager(), baseOId);
				linkArray.insert(txn, containerPos, &oId, allocateStrategy_);
				if (baseOId != linkArray.getBaseOId()) {
					*reinterpret_cast<OId *>(cursor) = linkArray.getBaseOId();
				}
			}
		}
	}

	void updateOId(TransactionContext &txn, uint8_t *cursor,
		uint64_t containerPos, OId oId) {
		if (containerPos == UNDEF_CONTAINER_POS) {
			*reinterpret_cast<OId *>(cursor) = oId;
		}
		else {
			OId baseOId = getOId(txn, cursor, UNDEF_CONTAINER_POS);
			if (baseOId == UNDEF_OID) {
				GS_THROW_SYSTEM_ERROR(
					GS_ERROR_DS_UNEXPECTED_ERROR, "update index not found");
			}
			else {
				LinkArray<void, OId> linkArray(
					txn, *getObjectManager(), baseOId);
				linkArray.update(txn, containerPos, &oId);
			}
		}
	}

	void removeOId(
		TransactionContext &txn, uint8_t *cursor, uint64_t containerPos) {
		if (containerPos == UNDEF_CONTAINER_POS) {
			*reinterpret_cast<OId *>(cursor) = UNDEF_OID;
		}
		else {
			OId baseOId = getOId(txn, cursor, UNDEF_CONTAINER_POS);
			if (baseOId != UNDEF_OID) {
				LinkArray<void, OId> linkArray(
					txn, *getObjectManager(), baseOId);
				linkArray.remove(txn, containerPos);
				if (linkArray.getNum() == 0) {
					linkArray.finalize(txn);
					*reinterpret_cast<OId *>(cursor) = UNDEF_OID;
				}
			}
		}
	}

	void setColumnId(uint8_t *cursor, ColumnId columnId) {
		*reinterpret_cast<uint16_t *>(cursor + sizeof(OId)) =
			static_cast<uint16_t>(columnId);
	}
	void setMapType(uint8_t *cursor, MapType mapType) {
		*reinterpret_cast<int8_t *>(cursor + sizeof(OId) + sizeof(uint16_t)) =
			static_cast<int8_t>(mapType);
	}

private:
	static const uint32_t INDEX_DATA_OFFSET =
		sizeof(uint16_t) + sizeof(uint16_t);

private:
	AllocateStrategy allocateStrategy_;
};

#endif
