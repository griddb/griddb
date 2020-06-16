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
	@brief Definition of Schemas for container
*/
#ifndef SCHEMA_H_
#define SCHEMA_H_

#include "data_type.h"
#include "base_object.h"
#include "object_manager.h"  
#include "transaction_context.h"
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

typedef int8_t COMPRESSION_TYPE;
const int8_t NO_COMPRESSION = 0;
const int8_t SS_COMPRESSION = 1;
const int8_t HI_COMPRESSION = 2;

enum SSCompressionStatus {
	SS_IS_READRY = 0,
	SS_IS_NOT_READRY = 1,
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
	void initializeOnMemory(TransactionContext &txn, const V *value) {
		uint64_t reserveNum = 1;
		void *binary = txn.getDefaultAllocator().allocate(getAllocateSize(reserveNum));
		setBaseAddr(static_cast<uint8_t *>(binary));
		setReserveNum(reserveNum);
		if (value != NULL) {
			setNum(1);
			V *headElem = getElemHead();
			*headElem = *value;
		} else {
			setNum(0);
		}
	}
	void initialize(TransactionContext &txn, uint64_t reserveNum,
		const AllocateStrategy &allocateStrategy) {
		UNUSED_VARIABLE(txn);
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
									 (sizeof(V) * reserveNum) + sizeof(OId));
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
const CONTAINER_META_TYPE META_TYPE_COMPRESSION_SCHEMA = 1;  
const CONTAINER_META_TYPE META_TYPE_DURATION = 2;	
const CONTAINER_META_TYPE META_TYPE_AFFINITY = 3;	
const CONTAINER_META_TYPE META_TYPE_TRIGGER = 4;	 
const CONTAINER_META_TYPE META_TYPE_ATTRIBUTES = 5;  
const CONTAINER_META_TYPE META_TYPE_CONTAINER_DURATION = 6;	
const CONTAINER_META_TYPE META_TYPE_MAX = 7;		 

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
		const AllocateStrategy &allocateStrategy, bool onMemory);
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
	MapType mapType;	

	util::StackAllocator &alloc_;
	util::String indexName_;
	util::String extensionName_;
	util::XArray<ColumnType> extensionOptionSchema_;
	util::XArray<uint8_t> extensionOptionFixedPart_;
	util::XArray<uint8_t> extensionOptionVarPart_;

	util::Vector<uint32_t> columnIds_;

	uint8_t anyNameMatches_;
	uint8_t anyTypeMatches_;

	IndexInfo(util::StackAllocator &alloc) :
		mapType(MAP_TYPE_BTREE), alloc_(alloc),
		indexName_(alloc_), extensionName_(alloc_),
		extensionOptionSchema_(alloc_),
		extensionOptionFixedPart_(alloc_), extensionOptionVarPart_(alloc_),
		columnIds_(alloc_),
		anyNameMatches_(0), anyTypeMatches_(0) {}
	IndexInfo(util::StackAllocator &alloc, const util::Vector<uint32_t> &columnIds, MapType mapTYPE) :
		mapType(mapTYPE), alloc_(alloc),
		indexName_(alloc_), extensionName_(alloc_),
		extensionOptionSchema_(alloc_),
		extensionOptionFixedPart_(alloc_), extensionOptionVarPart_(alloc_),
		columnIds_(alloc_),
		anyNameMatches_(0), anyTypeMatches_(0) {
			columnIds_.assign(columnIds.begin(), columnIds.end());
		}
	IndexInfo(util::StackAllocator &alloc, const util::String &indexName, 
		util::Vector<uint32_t> &columnIds, MapType mapTYPE,
		uint8_t anyNameMatches = 0, uint8_t anyTypeMatches = 0) :
		mapType(mapTYPE), alloc_(alloc),
		indexName_(alloc_), extensionName_(alloc_),
		extensionOptionSchema_(alloc_),
		extensionOptionFixedPart_(alloc_), extensionOptionVarPart_(alloc_),
		columnIds_(alloc_),
		anyNameMatches_(anyNameMatches), anyTypeMatches_(anyTypeMatches) {
			indexName_ = indexName;
			columnIds_.assign(columnIds.begin(), columnIds.end());
		}
	IndexInfo(const IndexInfo &info) :
		mapType(info.mapType), alloc_(info.alloc_),
		indexName_(alloc_), extensionName_(alloc_),
		extensionOptionSchema_(alloc_),
		extensionOptionFixedPart_(alloc_), extensionOptionVarPart_(alloc_),
		columnIds_(alloc_),
		anyNameMatches_(info.anyNameMatches_), anyTypeMatches_(info.anyTypeMatches_) {
		indexName_.append(info.indexName_);

		extensionName_.append(info.extensionName_);

		extensionOptionSchema_.assign(info.extensionOptionSchema_.begin(), info.extensionOptionSchema_.end());

		extensionOptionFixedPart_.assign(info.extensionOptionFixedPart_.begin(), info.extensionOptionFixedPart_.end());

		extensionOptionVarPart_.assign(info.extensionOptionVarPart_.begin(), info.extensionOptionVarPart_.end());

		columnIds_.assign(info.columnIds_.begin(), info.columnIds_.end());
	}

	IndexInfo &operator=(const IndexInfo &info) {
		if (this == &info) {
			return *this;
		}

		mapType = info.mapType;

		indexName_.clear();
		indexName_.append(info.indexName_);

		extensionName_.clear();
		extensionName_.append(info.extensionName_);

		extensionOptionSchema_.clear();
		extensionOptionSchema_.assign(info.extensionOptionSchema_.begin(), info.extensionOptionSchema_.end());

		extensionOptionFixedPart_.clear();
		extensionOptionFixedPart_.assign(info.extensionOptionFixedPart_.begin(), info.extensionOptionFixedPart_.end());

		extensionOptionVarPart_.clear();
		extensionOptionVarPart_.assign(info.extensionOptionVarPart_.begin(), info.extensionOptionVarPart_.end());

		columnIds_.clear();
		columnIds_.assign(info.columnIds_.begin(), info.columnIds_.end());

		anyNameMatches_ = info.anyNameMatches_;
		anyTypeMatches_ = info.anyTypeMatches_;

		return *this;
	}
	bool isComposite() {
		return columnIds_.size() > 1;
	}
};

/*!
	@brief Information of column
*/
class ColumnInfo  
{
public:
	static const ColumnId ROW_KEY_COLUMN_ID = 0;  
	static const uint8_t COLUMN_FLAG_KEY = 0x01;
	static const uint8_t COLUMN_FLAG_VIRTUAL = 0x02;
	static const uint8_t COLUMN_FLAG_NOT_NULL = 0x04;

	ColumnInfo() {}
	~ColumnInfo() {}

	ColumnId getColumnId() const {
		return columnId_;
	}

	const char *getColumnName(
		TransactionContext &txn, ObjectManager &objectManager,
		bool onMemory = false) const {
		if (onMemory) {
			return columnNameOnMemory_;
		}
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
		return (flags_ & COLUMN_FLAG_KEY) != 0;
	}

	bool isVirtual() const {
		return (flags_ & COLUMN_FLAG_VIRTUAL) != 0;
	}

	bool isArray() const {
		return ValueProcessor::isArray(getColumnType());
	}

	bool isVariable() const {
		return (getColumnType() == COLUMN_TYPE_STRING ||
				getColumnType() == COLUMN_TYPE_GEOMETRY ||
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
		MessageSchema *messageSchema, const AllocateStrategy &allocateStrategy,
		bool onMemory);

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

	static void setVirtual(uint8_t &flag) {
		flag |= COLUMN_FLAG_VIRTUAL;
	}

	static void setNotNull(uint8_t &flag) {
		flag |= COLUMN_FLAG_NOT_NULL;
	}

	void setNotNull() {
		flags_ |= COLUMN_FLAG_NOT_NULL;
	}
	/*!
		@brief Check if column type is not null
	*/
	bool isNotNull() const {
		return (flags_ & COLUMN_FLAG_NOT_NULL) != 0;
	}

	static bool isNotNull(uint8_t flag) {
		return (flag & COLUMN_FLAG_NOT_NULL) != 0;
	}

private:

	union {
		OId columnNameOId_;	
		char *columnNameOnMemory_;	
	};
	uint16_t columnId_;	
	uint16_t columnSize_;  
	uint16_t
		offset_;			 
	ColumnType columnType_;  
	uint8_t flags_;			 
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
	@brief Compression Infomation for message format
*/
class MessageCompressionInfo {
public:
	enum CompressionType {
		NONE = 0,
		DSDC = 1,  
	};

	void initialize();
	void set(CompressionType type, bool threshholdRelative, double threshhold,
		double rate, double span);
	void finalize();

	CompressionType getType() const {
		return type_;
	}
	double getThreshhold() const {
		return threshhold_;
	}
	bool getThreshholdRelative() const {
		return threshholdRelative_;
	}
	double getRate() const {
		return rate_;
	}
	double getSpan() const {
		return span_;
	}

	MessageCompressionInfo() {}
	~MessageCompressionInfo() {}

private:
	MessageCompressionInfo(const MessageCompressionInfo &) {}

	double threshhold_;		   
	double rate_;			   
	double span_;			   
	CompressionType type_;	 
	bool threshholdRelative_;  
};

struct DSDCVal {
	double upperAngle_;  
	double lowerAngle_;  
	double upperError_;
	double lowerError_;

	void initialize(double error) {
		upperAngle_ = DBL_MAX;
		lowerAngle_ = -DBL_MAX;
		upperError_ = error;
		lowerError_ = error;
	}

	std::string dump() {
		util::NormalOStringStream strstrm;
		strstrm << "DSDCVal" << std::endl;
		strstrm << "upperAngle_=" << upperAngle_ << ", "
				<< "lowerAngle_=" << lowerAngle_ << ", "
				<< "upperError_=" << upperError_ << ", "
				<< "lowerError_=" << lowerError_ << ", " << std::endl;
		return strstrm.str();
	}
};

class HiCompressionData {
public:
	void initialize(ColumnId columnId, double threshhold, double rate,
		double span, bool threshholdRelative) {
		columnId_ = static_cast<uint16_t>(columnId);
		threshhold_ = threshhold;
		rate_ = rate;
		span_ = span;
		threshholdRelative_ = threshholdRelative;
		padding1_ = 0;
		padding2_ = 0;
	}
	void finalize() {}

	ColumnId getColumnId() {
		return columnId_;
	}
	void getProperty(double &threshhold, double &rate, double &span,
		bool &threshholdRelative) const {
		threshhold = threshhold_;
		rate = rate_;
		span = span_;
		threshholdRelative = threshholdRelative_;
	}

private:
	double threshhold_;		   
	double rate_;			   
	double span_;			   
	uint16_t columnId_;		   
	bool threshholdRelative_;  
	uint8_t padding1_;
	uint32_t padding2_;
};

class CompressionSchema {
public:
	void initialize(
		COMPRESSION_TYPE compressionType, DurationInfo &compressionWindowInfo) {
		compressionWindowInfo_ = compressionWindowInfo;
		hiCompressionColumnNum_ = 0;
		compressionType_ = compressionType;
		padding1_ = 0;
		padding2_ = 0;
	}
	void finalize() {}

	const DurationInfo &getDurationInfo() const {
		return compressionWindowInfo_;
	}

	COMPRESSION_TYPE getCompressionType() const {
		return compressionType_;
	}
	void setCompressionType(COMPRESSION_TYPE type) {
		compressionType_ = type;
	}

	void addHiCompression(uint16_t pos, ColumnId columnId, double threshhold,
		double rate, double span, bool threshholdRelative);
	bool isHiCompression(ColumnId columnId) const;
	void getHiCompressionColumnList(util::Vector<ColumnId> &list) const;
	void getHiCompressionProperty(ColumnId columnId, double &threshhold,
		double &rate, double &span, bool &threshholdRelative,
		uint16_t &compressionPos) const;

	uint16_t getHiCompressionNum() const {
		return hiCompressionColumnNum_;
	}
	HiCompressionData *getHiCompressionDataList() const {
		return reinterpret_cast<HiCompressionData *>(
			reinterpret_cast<uint8_t *>(const_cast<CompressionSchema *>(this)) +
			HI_COMPRESSION_DATA_OFFSET);
	}
	bool schemaCheck(
		TransactionContext &txn, MessageTimeSeriesSchema *messageSchema);
	static int64_t calcHash(MessageTimeSeriesSchema *messageSchema);

	/*!
		@brief Calculate the size needed
	*/
	static uint32_t getAllocateSize(uint32_t hiCompressionColumnNum) {
		return HI_COMPRESSION_DATA_OFFSET +
			   (sizeof(HiCompressionData) * hiCompressionColumnNum);
	}

private:
	static const uint32_t HI_COMPRESSION_DATA_OFFSET =
		sizeof(DurationInfo) + sizeof(uint16_t) + sizeof(bool) + sizeof(int8_t);
	DurationInfo compressionWindowInfo_;  
	uint16_t hiCompressionColumnNum_;
	COMPRESSION_TYPE compressionType_;
	uint8_t padding1_;
	uint32_t padding2_;
};

class DSDCValList : public BaseObject {
public:
	DSDCValList(TransactionContext &txn, ObjectManager &objectManager)
		: BaseObject(txn.getPartitionId(), objectManager) {}
	DSDCValList(TransactionContext &txn, ObjectManager &objectManager, OId oId)
		: BaseObject(txn.getPartitionId(), objectManager, oId) {}
	void initialize(TransactionContext &txn,
		const CompressionSchema &compressionSchema,
		uint16_t hiCompressionColumnNum,
		const AllocateStrategy &allocateStrategy) {
		UNUSED_VARIABLE(txn);
		BaseObject::allocate<DSDCVal>(sizeof(DSDCVal) * hiCompressionColumnNum,
			allocateStrategy, getBaseOId(), OBJECT_TYPE_COMPRESSIONINFO);

		reset(compressionSchema, hiCompressionColumnNum);
	}
	void finalize() {
		BaseObject::finalize();
	}
	void reset(const CompressionSchema &compressionSchema,
		uint16_t hiCompressionColumnNum) {
		setDirty();
		DSDCVal *dsdcValList = reinterpret_cast<DSDCVal *>(getBaseAddr());
		const HiCompressionData *hiCompressionDataList =
			compressionSchema.getHiCompressionDataList();
		for (uint16_t i = 0; i < hiCompressionColumnNum; i++) {
			double threshhold, rate, span;
			bool threshholdRelative;
			hiCompressionDataList[i].getProperty(
				threshhold, rate, span, threshholdRelative);
			dsdcValList[i].initialize(threshhold);
		}
	}
	const DSDCVal *get(uint64_t pos) const {
		DSDCVal *dsdcValList = getBaseAddr<DSDCVal *>();
		return &dsdcValList[pos];
	}
	void update(uint64_t pos, const DSDCVal *dsdcVal) {
		setDirty();
		DSDCVal *dsdcValList = getBaseAddr<DSDCVal *>();
		dsdcValList[pos] = *dsdcVal;
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
	util::Vector<ColumnId> columnIds_;

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
	@brief Operation of Row null bit field
*/
class RowNullBits {
private:
	static const uint8_t BITE_POS_FILTER = 0x3;
	static const uint8_t BIT_POS_FILTER = 0x7;
public:
	static inline uint32_t getBytePos(ColumnId columnId) {
		return columnId >> BITE_POS_FILTER;
	}
	static inline uint32_t getBitPos(ColumnId columnId) {
		return columnId & BIT_POS_FILTER;
	}
	static inline void setBit(uint8_t &byte, uint32_t bitPos) {
		byte |= (0x1 << bitPos);
	}
	static inline void resetBit(uint8_t &byte, uint32_t bitPos) {
		byte &= ~(0x1 << bitPos);
	}
	/*!
		@brief Check if value is null
	*/
	static inline bool isNullValue(const uint8_t *nullbits, ColumnId columnId) {
		uint8_t nullbyte = *(nullbits + getBytePos(columnId));
		return ((nullbyte >> getBitPos(columnId)) & 0x1) != 0;
	}
	static inline void setNull(uint8_t *nullbits, ColumnId columnId) {
		uint8_t *nullbytePos = nullbits + getBytePos(columnId);
		setBit(*nullbytePos, getBitPos(columnId));
	}
	static inline void setNotNull(uint8_t *nullbits, ColumnId columnId) {
		uint8_t *nullbytePos = nullbits + getBytePos(columnId);
		resetBit(*nullbytePos, getBitPos(columnId));
	}
	static inline void unionNullsStats(const uint8_t *srcNullbits, uint8_t *destNullbits,
		size_t nullbitsSize) {
		size_t size64bit = nullbitsSize >> BITE_POS_FILTER; 
		size_t size8bit = nullbitsSize & BIT_POS_FILTER;
		for (size_t i = 0; i < size64bit; i++) {
			const uint64_t *src = reinterpret_cast<const uint64_t *>(srcNullbits) + i;
			uint64_t *dest = reinterpret_cast<uint64_t *>(destNullbits) + i;
			*dest |= *src;
		}
		for (size_t i = 0; i < size8bit; i++) {
			const uint8_t *src = srcNullbits + (size64bit << BITE_POS_FILTER) + i;
			uint8_t *dest = destNullbits + (size64bit << BITE_POS_FILTER) + i;
			*dest |= *src;
		}
	}
	static uint16_t calcBitsSize(uint32_t columnNum) {
		uint16_t nullBitsSize = columnNum >> BITE_POS_FILTER;
		if ((columnNum & BIT_POS_FILTER) != 0) {
			nullBitsSize++;
		}
		return nullBitsSize;
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
		const AllocateStrategy &allocateStrategy, bool onMemory);
	void set(util::StackAllocator &alloc,
		ColumnSchema *srcSchema,
		const util::Vector<ColumnId> &columnIds);
	void finalize(TransactionContext &txn, ObjectManager &objectManager);

	uint32_t getColumnNum() const {
		return columnNum_;
	}

	uint16_t getRowKeyColumnNum() const {
		uint16_t *keyNumAddr = reinterpret_cast<uint16_t *>(getRowKeyPtr());
		uint16_t keyNum = *keyNumAddr;
		return keyNum;
	}

	uint32_t getRowFixedColumnSize() const {
		return rowFixedColumnSize_;
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
		const char *name, uint32_t &columnId, ColumnInfo *&columnInfo,
		bool isCaseSensitive) const;

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
	static uint32_t getAllocateSize(uint32_t columnNum, uint32_t rowKeyNum) {
		return COLUMN_INFO_OFFSET + (sizeof(ColumnInfo) * columnNum) + 
			sizeof(uint16_t) + sizeof(uint16_t) * rowKeyNum;
	}
	void getKeyColumnIdList(util::Vector<ColumnId> &keyColumnIdList) {
		uint16_t *keyNumAddr = reinterpret_cast<uint16_t *>(getRowKeyPtr());
		uint16_t keyNum = *keyNumAddr;
		for (uint16_t i = 0; i <keyNum; i++) {
			keyNumAddr++;
			ColumnId columnId = *keyNumAddr;
			keyColumnIdList.push_back(columnId);
		}
	}

	bool isFirstColumnAdd() const {
		uint32_t columnNum, varColumnNum, rowFixedColumnSize;
		getFirstSchema(columnNum, varColumnNum, rowFixedColumnSize);
		if (columnNum == 0) {
			return true;
		} else {
			return false;
		}
	}
	void getFirstSchema(uint32_t &columnNum, uint32_t &varColumnNum, uint32_t &rowFixedColumnSize) const {
		columnNum = firstColumnNum_;
		varColumnNum = firstVarColumnNum_;
		rowFixedColumnSize = firstRowFixedColumnSize_;
	}

	static const uint8_t INIT_STATUS_COLUMN_NUM_BITS = 48;
	static const uint8_t INIT_STATUS_VAR_NUM_BITS = 32;
	static const uint8_t BIT_POS_FILTER = 0x7;
	static void convertFromInitSchemaStatus(int64_t orgStatus, uint32_t &columnNum, uint32_t &varColumnNum, uint32_t &rowFixedColumnSize) {
		uint64_t status = static_cast<uint64_t>(orgStatus);
		columnNum = static_cast<uint32_t>((status >> INIT_STATUS_COLUMN_NUM_BITS));
		varColumnNum = static_cast<uint32_t>(((status >> INIT_STATUS_VAR_NUM_BITS) & 0xFFFF));
		rowFixedColumnSize = static_cast<uint32_t>((status & 0xFFFFFFFF));
	}

	static int64_t convertToInitSchemaStatus(uint32_t orgColumnNum, uint32_t orgVarColumnNum, uint32_t orgRowFixedColumnSize) {
		uint64_t columnNum = orgColumnNum;
		uint64_t varColumnNum = orgVarColumnNum;
		uint64_t rowFixedColumnSize = orgRowFixedColumnSize;
		uint64_t orgStatus = ((columnNum << INIT_STATUS_COLUMN_NUM_BITS) | (varColumnNum << INIT_STATUS_VAR_NUM_BITS) | rowFixedColumnSize);
		int64_t status = static_cast<int64_t>(orgStatus);
		return status;
	}
	std::string dump(TransactionContext &txn, ObjectManager &objectManager);

private:
	uint8_t *getRowKeyPtr() const {
		ColumnInfo *columnInfoList = getColumnInfoList();
		uint8_t *rowKeyPtr = reinterpret_cast<uint8_t *>(
			columnInfoList + columnNum_);
		return rowKeyPtr;
	}
	void setFirstSchema(uint32_t columnNum, uint32_t varColumnNum, uint32_t rowFixedColumnSize) {
		firstColumnNum_ = static_cast<uint16_t>(columnNum);
		firstVarColumnNum_ = static_cast<uint16_t>(varColumnNum);
		firstRowFixedColumnSize_ = static_cast<uint16_t>(rowFixedColumnSize);
	}
private:
	static const uint32_t COLUMN_INFO_OFFSET =
		sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint16_t) + sizeof(uint16_t) +
		sizeof(uint16_t) + sizeof(uint16_t) +
		sizeof(int32_t) + sizeof(int16_t); 
	uint32_t rowFixedColumnSize_;  
	uint16_t columnNum_;
	uint16_t variableColumnNum_;
	uint16_t firstColumnNum_; 
	uint16_t firstVarColumnNum_; 
	uint16_t firstRowFixedColumnSize_; 
};

struct MapOIds {
	OId mainOId_;
	OId nullOId_;
	MapOIds() : mainOId_(UNDEF_OID), nullOId_(UNDEF_OID) {
	}
	bool operator==(const MapOIds &another) const {
		return mainOId_ == another.mainOId_ &&  nullOId_ == another.nullOId_;
	}
	bool operator!=(const MapOIds &another) const {
		return mainOId_ != another.mainOId_ ||  nullOId_ != another.nullOId_;
	}
	friend std::ostream &operator<<(std::ostream &output, const MapOIds &v) {
		output << "(" << v.mainOId_ << "," << v.nullOId_ << ")";
		return output;
	}
};

static const MapOIds UNDEF_MAP_OIDS;

typedef uint8_t DDLStatus;
static const DDLStatus DDL_READY = 0;
static const DDLStatus DDL_UNDER_CONSTRUCTION = 1;

/*!
	@brief Information of an index
*/
struct IndexData {
	MapOIds oIds_;
	OId optionOId_;
	util::Vector<ColumnId> *columnIds_;
	MapType mapType_;
	DDLStatus status_;
	RowId cursor_;
	IndexData(util::StackAllocator &alloc) {
		oIds_ = UNDEF_MAP_OIDS;
		optionOId_ = UNDEF_OID;
		columnIds_ = ALLOC_NEW(alloc) util::Vector<ColumnId>(alloc);
		mapType_ = MAP_TYPE_DEFAULT;
		status_ = DDL_READY;
		cursor_ = MAX_ROWID;
	}
	bool isComposite() {
		return columnIds_->size() > 1;
	}
};

/*!
	@brief Information of Index layout
*/
class IndexSchema : public BaseObject {
public:
	static const uint32_t INITIALIZE_RESERVE_NUM = 1;

public:
	IndexSchema(TransactionContext &txn, ObjectManager &objectManager,
		const AllocateStrategy &strategy)
		: BaseObject(txn.getPartitionId(), objectManager),
		  allocateStrategy_(strategy) {}
	IndexSchema(TransactionContext &txn, ObjectManager &objectManager, OId oId,
		const AllocateStrategy &strategy)
		: BaseObject(txn.getPartitionId(), objectManager, oId),
		  allocateStrategy_(strategy) {}

	void initialize(TransactionContext &txn, uint16_t reserveNum, 
		uint16_t indexNum, uint32_t columnNum, bool onMemory);

	bool createIndexInfo(
		TransactionContext &txn, const IndexInfo &indexInfo);
	void dropIndexInfo(
		TransactionContext &txn, util::Vector<ColumnId> &columnIds, MapType mapType);
	IndexData createIndexData(TransactionContext &txn, 
		const util::Vector<ColumnId> &columnIds,
		MapType mapType, const util::Vector<ColumnType> &columnTypes, 
		BaseContainer *container, uint64_t containerPos, bool isUnique);
	void createDummyIndexData(TransactionContext &txn, IndexInfo &indexInfo,
		uint64_t containerPos);
	void dropIndexData(TransactionContext &txn, util::Vector<ColumnId> &columnIds,
		MapType mapType, BaseContainer *container, uint64_t containerPos,
		bool isMapFinalize);

	void dropAll(TransactionContext &txn, BaseContainer *container,
		uint64_t containerPos, bool isMapFinalize);
	void finalize(TransactionContext &txn);

	uint16_t getIndexNum() const {
		return *getNumPtr();
	}
	void getIndexList(TransactionContext &txn, uint64_t containerPos,
		bool withUncommitted, util::XArray<IndexData> &list) const;
	bool getIndexData(TransactionContext &txn, const util::Vector<ColumnId> &columnIds,
		MapType mapType, uint64_t containerPos, bool withUncommitted, 
		bool withPartialMatch, IndexData &indexData) const;
	bool getIndexData(TransactionContext &txn, const IndexCursor &indexCursor,
		uint64_t containerPos, IndexData &indexData) const;
	void getIndexInfoList(TransactionContext &txn, BaseContainer *container,
		const IndexInfo &indexInfo, bool withUncommitted, 
		util::Vector<IndexInfo> &matchList, 
		util::Vector<IndexInfo> &mismatchList,
		bool isIndexNameCaseSensitive,
		bool withPartialMatch = false);
	void getIndexDataList(TransactionContext &txn, util::Vector<ColumnId> &columnIds,
		MapType mapType, bool withUncommitted, 
		util::Vector<IndexData> &indexDataList, bool withPartialMatch = false);
	void createNullIndexData(TransactionContext &txn, uint64_t containerPos,
		IndexData &indexData, BaseContainer *container);

	bool hasIndex(
		TransactionContext &txn, util::Vector<ColumnId> &columnIds, 
		MapType mapType, bool withPartialMatch) const;
	static bool hasIndex(IndexTypes indexType, MapType mapType);
	IndexTypes getIndexTypes(TransactionContext &txn, 
		util::Vector<ColumnId> &columnIds, bool withPartialMatch) const;
	IndexTypes getIndexTypes(TransactionContext &txn, ColumnId columnId) const;

	BaseIndex *getIndex(
		TransactionContext &txn, const IndexData &indexData, bool forNull,
		BaseContainer *container) const;
	void updateIndexData(
		TransactionContext &txn, const IndexData &indexData, uint64_t containerPos);
	void commit(TransactionContext &txn, IndexCursor &indexCursor);
	uint8_t *getNullsStats() const {
		return getBaseAddr() + NULL_STAT_OFFSET;
	}
	uint16_t getNullbitsSize() const {
		uint8_t *upperAddr = getBaseAddr() + NULL_BIT_SIZE_UPPER_OFFSET;
		uint8_t *lowerAddr = getBaseAddr() + NULL_BIT_SIZE_LOWER_OFFSET;
		uint16_t upper = static_cast<uint16_t>(*upperAddr);
		uint16_t lower = static_cast<uint16_t>(*lowerAddr);
		uint16_t nullBitsSize = ((upper << NULL_BIT_UPPER_SHIFT) | lower);
		return nullBitsSize;
	}
	void updateNullsStats(const uint8_t *nullbits) {
		setDirty();
		RowNullBits::unionNullsStats(nullbits, getNullsStats(), 
			getNullbitsSize());
	}
	void setNullStats(ColumnId columnId) {
		setDirty();
		RowNullBits::setNull(getNullsStats(), columnId);
	}
	bool expandNullStats(TransactionContext &txn, uint32_t oldColumnNum, uint32_t newColumnNum);
private:
/*!
	@brief Calculate the size needed
*/
	static uint32_t getAllocateSize(uint32_t reserveNum, uint16_t bitsSize) {
		return NULL_STAT_OFFSET + bitsSize + INDEX_HEADER_SIZE + (getIndexDataSize() * reserveNum);
	}
	void setNullbitsSize(uint16_t bitsSize) {
		uint8_t *upperAddr = getBaseAddr() + NULL_BIT_SIZE_UPPER_OFFSET;
		*upperAddr = static_cast<uint8_t>(bitsSize >> NULL_BIT_UPPER_SHIFT);
		uint8_t *lowerAddr = getBaseAddr() + NULL_BIT_SIZE_LOWER_OFFSET;
		*lowerAddr = static_cast<uint8_t>(bitsSize & NULL_BIT_LOWER_FILTER);
	}
	static uint32_t getOptionSize(const util::String &name, util::Vector<ColumnId> &columnIds) {
		size_t offset = ValueProcessor::getEncodedVarSize(name.length());
		size_t columnIdSize = 0;
		columnIdSize = sizeof(uint16_t) * (columnIds.size() + 1);
		return static_cast<uint32_t>(OPTION_NAME_OFFSET + offset + name.length() + columnIdSize);
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
		return reinterpret_cast<uint16_t *>(getNullsStats() + getNullbitsSize());
	}
	uint16_t *getReserveNumPtr() const {
		return reinterpret_cast<uint16_t *>(getNullsStats() + getNullbitsSize() + sizeof(uint16_t));
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
		return getNullsStats() + getNullbitsSize() + INDEX_HEADER_SIZE;
	}

	static uint32_t getIndexDataSize() {
		return INDEX_DATA_SIZE;
	}

	uint16_t getNth(TransactionContext &txn, 
		const util::Vector<ColumnId> &columnIds, MapType mapType, bool withPartialMatch) const {
		for (uint16_t i = 0; i < getIndexNum(); i++) {
			uint8_t *indexDataPos = getElemHead() + (getIndexDataSize() * i);
			if (getMapType(indexDataPos) == mapType && 
				compareColumnIds(txn, indexDataPos, columnIds, 
				getOptionOId(indexDataPos), withPartialMatch)) {
				return i;
			}
		}
		return UNDEF_INDEX_POS;
	}
	uint16_t getNth(ColumnId columnId, MapType mapType, OId optionOId) const {
		for (uint16_t i = 0; i < getIndexNum(); i++) {
			uint8_t *indexDataPos = getElemHead() + (getIndexDataSize() * i);
			if (getColumnId(indexDataPos) == columnId &&
				getMapType(indexDataPos) == mapType &&
				getOptionOId(indexDataPos) == optionOId) {
				return i;
			}
		}
		return UNDEF_INDEX_POS;
	}

	void getIndexData(TransactionContext &txn, uint16_t nth,
		uint64_t containerPos, IndexData &indexData) const {
		uint8_t *indexDataPos = getElemHead() + (getIndexDataSize() * nth);
		indexData.oIds_ = getMapOIds(txn, indexDataPos, containerPos);
		indexData.mapType_ = getMapType(indexDataPos);
		indexData.optionOId_ = getOptionOId(indexDataPos);
		indexData.columnIds_->clear();
		if (isComposite(indexDataPos)) {
			BaseObject option(txn.getPartitionId(), *getObjectManager(),
				getOptionOId(indexDataPos));
			getCompositeColumnIds(option.getBaseAddr(), *(indexData.columnIds_));
		} else {
			indexData.columnIds_->push_back(getColumnId(indexDataPos));
		}
		indexData.status_ = getStatus(indexDataPos);
		if (indexData.status_!= DDL_READY) {
			BaseObject option(txn.getPartitionId(), *getObjectManager(),
				getOptionOId(indexDataPos));
			indexData.cursor_ = getRowId(option.getBaseAddr());
		} else {
			indexData.cursor_ = MAX_ROWID;
		}
	}

	void setIndexData(TransactionContext &txn, uint16_t nth,
		uint64_t containerPos, IndexData &indexData,
		const util::String *name = NULL) {
		uint8_t *indexDataPos =	getElemHead() + (getIndexDataSize() * nth);
		setMapOIds(txn, indexDataPos, containerPos, indexData.oIds_);
		setMapType(indexDataPos, indexData.mapType_, indexData.isComposite());
		setColumnId(indexDataPos, indexData.columnIds_->at(0));
		setStatus(indexDataPos, indexData.status_);
		if (name != NULL) {
			OId optionOId;
			BaseObject option(txn.getPartitionId(), *getObjectManager());
			uint32_t allocateSize = IndexSchema::getOptionSize(*name, *(indexData.columnIds_));
			uint8_t *optionAddr = option.allocate<uint8_t>(allocateSize,
				allocateStrategy_, optionOId, OBJECT_TYPE_COLUMNINFO);
			memset(optionAddr, 0, allocateSize);
			setOptionHeader(optionAddr, 0);
			setRowId(optionAddr, indexData.cursor_);
			setName(optionAddr, *name);
			setCompositeColumnIds(optionAddr, *(indexData.columnIds_));
			setOptionOId(indexDataPos, optionOId);
		}
	}

	void getIndexInfo(TransactionContext &txn, uint16_t nth, 
		IndexInfo &indexInfo) {
		uint8_t *indexDataPos = getElemHead() + (getIndexDataSize() * nth);
		indexInfo.mapType = getMapType(indexDataPos);
		BaseObject option(txn.getPartitionId(), *getObjectManager(),
			getOptionOId(indexDataPos));
		if (isComposite(indexDataPos)) {
			getCompositeColumnIds(option.getBaseAddr(), indexInfo.columnIds_);
		} else {
			indexInfo.columnIds_.push_back(getColumnId(indexDataPos));
		}
		getName(option.getBaseAddr(), indexInfo.indexName_);
	}

	void setIndexInfo(TransactionContext &txn, uint16_t nth, 
		const IndexInfo &indexInfo) {
		IndexData indexData(txn.getDefaultAllocator());
		indexData.columnIds_->clear();
		indexData.columnIds_->assign(indexInfo.columnIds_.begin(), indexInfo.columnIds_.end());
		indexData.mapType_ = indexInfo.mapType;
		indexData.status_ = DDL_UNDER_CONSTRUCTION;
		indexData.cursor_ = INITIAL_ROWID;
		indexData.oIds_ = UNDEF_MAP_OIDS;
		setIndexData(txn, nth, UNDEF_CONTAINER_POS, indexData, &indexInfo.indexName_);
	}

	ColumnId getColumnId(uint8_t *cursor) const {
		return *reinterpret_cast<uint16_t *>(cursor + COLUMNID_OFFSET);
	}
	MapType getMapType(uint8_t *cursor) const {
		uint8_t type = *(cursor + MAPTYPE_OFFSET);
		return static_cast<MapType>(type & MAP_TYPE_MASK);
	}
	bool isComposite(uint8_t *cursor) const {
		uint8_t type = *(cursor + MAPTYPE_OFFSET);
		return (type & COMPOSITE_FLAG_MASK) != 0;
	}
	DDLStatus getStatus(uint8_t *cursor) const {
		uint8_t type = *(cursor + STATUS_OFFSET);
		return static_cast<DDLStatus>(type);
	}

	void setLinkArrayOId(uint8_t *cursor, OId oId) {
		MapOIds *mapOIds = reinterpret_cast<MapOIds *>(cursor);
		mapOIds->mainOId_ = oId;
	}

	OId getLinkArrayOId(uint8_t *cursor) const {
		MapOIds *mapOIds = reinterpret_cast<MapOIds *>(cursor);
		return mapOIds->mainOId_;
	}

	MapOIds getMapOIds(
		TransactionContext &txn, uint8_t *cursor, uint64_t containerPos) const {
		if (containerPos == UNDEF_CONTAINER_POS) {
			return *reinterpret_cast<MapOIds *>(cursor);
		}
		else {
			OId baseOId = getLinkArrayOId(cursor);
			if (baseOId == UNDEF_OID) {
				return UNDEF_MAP_OIDS;
			}
			else {
				LinkArray<void, MapOIds> linkArray(
					txn, *getObjectManager(), baseOId);
				return *linkArray.get(txn, containerPos);
			}
		}
	}


	void setMapOIds(TransactionContext &txn, uint8_t *cursor, uint64_t containerPos,
		const MapOIds oIds) {
		if (containerPos == UNDEF_CONTAINER_POS) {
			*reinterpret_cast<MapOIds *>(cursor) = oIds;
		}
		else {
			OId baseOId = getLinkArrayOId(cursor);
			if (baseOId == UNDEF_OID) {
				LinkArray<void, MapOIds> linkArray(txn, *getObjectManager());
				linkArray.initialize(txn,
					LinkArray<void, MapOIds>::DEFALUT_RESERVE_NUM,
					allocateStrategy_);
				setLinkArrayOId(cursor, linkArray.getBaseOId());
				linkArray.insert(txn, containerPos, &oIds, allocateStrategy_);
			}
			else {
				LinkArray<void, MapOIds> linkArray(
					txn, *getObjectManager(), baseOId);
				linkArray.insert(txn, containerPos, &oIds, allocateStrategy_);
				if (baseOId != linkArray.getBaseOId()) {
					setLinkArrayOId(cursor, linkArray.getBaseOId());
				}
			}
		}
	}

	void updateMapOIds(TransactionContext &txn, uint8_t *cursor,
		uint64_t containerPos, const MapOIds &oIds) {
		if (containerPos == UNDEF_CONTAINER_POS) {
			*reinterpret_cast<MapOIds *>(cursor) = oIds;
		}
		else {
			OId baseOId = getLinkArrayOId(cursor);
			if (baseOId == UNDEF_OID) {
				GS_THROW_SYSTEM_ERROR(
					GS_ERROR_DS_UNEXPECTED_ERROR, "update index not found");
			}
			else {
				LinkArray<void, MapOIds> linkArray(
					txn, *getObjectManager(), baseOId);
				linkArray.update(txn, containerPos, &oIds);
			}
		}
	}

	void removeMapOIds(
		TransactionContext &txn, uint8_t *cursor, uint64_t containerPos) {
		if (containerPos == UNDEF_CONTAINER_POS) {
			*reinterpret_cast<MapOIds *>(cursor) = UNDEF_MAP_OIDS;
		}
		else {
			OId baseOId = getLinkArrayOId(cursor);
			if (baseOId != UNDEF_OID) {
				LinkArray<void, MapOIds> linkArray(
					txn, *getObjectManager(), baseOId);
				linkArray.remove(txn, containerPos);
				if (linkArray.getNum() == 0) {
					linkArray.finalize(txn);
					setLinkArrayOId(cursor, UNDEF_OID);
				}
			}
		}
	}
	bool compareColumnIds(TransactionContext &txn, uint8_t *cursor, 
		const util::Vector<ColumnId> &columnIds, OId optionOId, bool withPartialMatch) const {
		bool ret = (getColumnId(cursor) == columnIds[0]);
		if (ret && (isComposite(cursor) || columnIds.size() > 1)) {
			BaseObject option(txn.getPartitionId(), *getObjectManager(), optionOId);
			util::Vector<ColumnId> targetColumnIds(txn.getDefaultAllocator());
			getCompositeColumnIds(option.getBaseAddr(), targetColumnIds);
			if (!withPartialMatch) {
				ret = (columnIds.size() == targetColumnIds.size()) 
					&& std::equal(columnIds.begin(), columnIds.end(), targetColumnIds.begin());
			}
			return ret;
		} else {
			return ret;
		}
	}
	void setColumnId(uint8_t *cursor, ColumnId columnId) {
		*reinterpret_cast<uint16_t *>(cursor + COLUMNID_OFFSET) =
			static_cast<uint16_t>(columnId);
	}
	void setMapType(uint8_t *cursor, MapType mapType, bool isComposite) {
		uint8_t value = isComposite ? (COMPOSITE_FLAG_MASK | mapType) : mapType;
		*(cursor + MAPTYPE_OFFSET) = value;
	}
	void setStatus(uint8_t *cursor, DDLStatus status) const {
		uint8_t *type = cursor + STATUS_OFFSET;
		*type = status;
	}

	void setOptionOId(uint8_t *cursor, OId oId) {
		uint8_t *optionOIdPos = cursor + OPTION_OFFSET;
		*reinterpret_cast<OId *>(optionOIdPos) = oId;
	}

	void setOptionHeader(uint8_t *cursor, uint8_t val) {
		*cursor = val;
	}
	void setRowId(uint8_t *cursor, RowId rowId) {
		uint8_t *rowIdPos = cursor + OPTION_ROWID_OFFSET;
		*reinterpret_cast<RowId *>(rowIdPos) = rowId;
	}
	void setName(uint8_t *optionCursor, 
		const util::String &name) {
		uint8_t *namePos = optionCursor + OPTION_NAME_OFFSET;
		uint64_t length = name.length();
		uint32_t offset = ValueProcessor::getEncodedVarSize(length);
		uint64_t encodedLength = ValueProcessor::encodeVarSize(length);
		memcpy(namePos, &encodedLength, offset);
		memcpy(namePos + offset, name.c_str(), length);
	}
	uint8_t *getCompositeColumnIdsPos(uint8_t *optionCursor) const {
		uint8_t *namePos = optionCursor + OPTION_NAME_OFFSET;
		StringCursor strCursor(namePos);
		uint8_t *columnIdsPos = strCursor.str() + strCursor.stringLength();
		return columnIdsPos;
	}
	void setCompositeColumnIds(uint8_t *optionCursor, const util::Vector<ColumnId> &columnIds) {
		uint8_t *columnIdsPos = getCompositeColumnIdsPos(optionCursor);
	    uint16_t *numPos = reinterpret_cast<uint16_t *>(columnIdsPos);
		*numPos = static_cast<uint16_t>(columnIds.size());
		if (columnIds.size() > 1) {
			uint16_t *idPos = ++numPos;
			for (size_t i = 0; i < columnIds.size(); i++) { 
				*idPos = static_cast<uint16_t>(columnIds[i]);
				idPos++;
			}
		}
	}
	void getCompositeColumnIds(uint8_t *optionCursor, util::Vector<ColumnId> &columnIds) const {
		uint8_t *columnIdsPos = getCompositeColumnIdsPos(optionCursor);
	    uint16_t *numPos = reinterpret_cast<uint16_t *>(columnIdsPos);
		size_t columnNum = static_cast<size_t>(*numPos);
		uint16_t *idPos = ++numPos;
		for (size_t i = 0; i < columnNum; i++) { 
			columnIds.push_back(*idPos);
			idPos++;
		}
	}

	OId getOptionOId(uint8_t *cursor) const {
		uint8_t *optionOIdPos = cursor + OPTION_OFFSET;
		return *reinterpret_cast<OId *>(optionOIdPos);
	}

	uint8_t getOptionHeader(uint8_t *optionCursor) const {
		return *optionCursor;
	}
	TransactionId getRowId(uint8_t *optionCursor) const {
		uint8_t *rowIdPos = optionCursor + OPTION_ROWID_OFFSET;
		return *reinterpret_cast<RowId *>(rowIdPos);
	}
	void getName(uint8_t *optionCursor, 
		util::String &name) const {
		uint8_t *namePos = optionCursor + OPTION_NAME_OFFSET;
		StringCursor strCursor(namePos);
		name.assign(reinterpret_cast<char *>(strCursor.str()), strCursor.stringLength());
	}

private:
	static const uint16_t NULL_BIT_UPPER_SHIFT = 0x8;
	static const uint16_t NULL_BIT_LOWER_FILTER = 0x00FF;
	static const uint32_t NULL_BIT_SIZE_UPPER_OFFSET = 0;
	static const uint32_t NULL_BIT_SIZE_LOWER_OFFSET = sizeof(uint8_t);
	static const uint32_t NULL_STAT_OFFSET = NULL_BIT_SIZE_LOWER_OFFSET + sizeof(uint8_t);
	static const uint32_t INDEX_HEADER_SIZE =
		sizeof(uint16_t) + sizeof(uint16_t);
	static const uint32_t OPTION_OFFSET = sizeof(MapOIds);
	static const uint32_t COLUMNID_OFFSET = OPTION_OFFSET + sizeof(OId);
	static const uint32_t MAPTYPE_OFFSET = COLUMNID_OFFSET + sizeof(uint16_t);
	static const uint32_t STATUS_OFFSET = MAPTYPE_OFFSET + sizeof(int8_t);
	static const uint32_t INDEX_DATA_SIZE = STATUS_OFFSET + sizeof(int8_t);

	static const uint32_t OPTION_ROWID_OFFSET = sizeof(uint8_t);
	static const uint32_t OPTION_NAME_OFFSET = OPTION_ROWID_OFFSET + sizeof(RowId);

	static const uint8_t COMPOSITE_FLAG_MASK = 0x80; 
	static const uint8_t MAP_TYPE_MASK = static_cast<const uint8_t>(~COMPOSITE_FLAG_MASK); 

private:
	AllocateStrategy allocateStrategy_;
};

class BibInfoUtil {
public:
	static TimeUnit getTimeUnit(const char *str);
	static const char *getTimeUnitStr(TimeUnit unit);
	static COMPRESSION_TYPE getCompressionType(const char *str);
	static const char *getCompressionTypeStr(COMPRESSION_TYPE type);
	static ColumnType getColumnType(const char *str);
	static const char *getColumnTypeStr(const ColumnType type);
	static ContainerType getContainerType(const char *str);
	static const char *getContainerTypeStr(ContainerType type);
public:
	static const char *CONTAINER_TYPE_COLLECTION_STR;
	static const char *CONTAINER_TYPE_TIMESERIES_STR;

	static const char *COLUMN_TYPE_BOOL_STR;
	static const char *COLUMN_TYPE_BYTE_STR;
	static const char *COLUMN_TYPE_SHORT_STR;
	static const char *COLUMN_TYPE_INT_STR;
	static const char *COLUMN_TYPE_LONG_STR;
	static const char *COLUMN_TYPE_FLOAT_STR;
	static const char *COLUMN_TYPE_DOUBLE_STR;
	static const char *COLUMN_TYPE_TIMESTAMP_STR;
	static const char *COLUMN_TYPE_STRING_STR;
	static const char *COLUMN_TYPE_GEOMETRY_STR;
	static const char *COLUMN_TYPE_BLOB_STR;
	static const char *COLUMN_TYPE_STRING_ARRAY_STR;
	static const char *COLUMN_TYPE_BOOL_ARRAY_STR;
	static const char *COLUMN_TYPE_BYTE_ARRAY_STR;
	static const char *COLUMN_TYPE_SHORT_ARRAY_STR;
	static const char *COLUMN_TYPE_INT_ARRAY_STR;
	static const char *COLUMN_TYPE_LONG_ARRAY_STR;
	static const char *COLUMN_TYPE_FLOAT_ARRAY_STR;
	static const char *COLUMN_TYPE_DOUBLE_ARRAY_STR;
	static const char *COLUMN_TYPE_TIMESTAMP_ARRAY_STR;


	static const char *TIME_UNIT_DAY_STR;
	static const char *TIME_UNIT_HOUR_STR;
	static const char *TIME_UNIT_MINUTE_STR;
	static const char *TIME_UNIT_SECOND_STR;
	static const char *TIME_UNIT_MILLISECOND_STR;

	static const char *NO_COMPRESSION_STR;
	static const char *SS_COMPRESSION_STR;
	static const char *HI_COMPRESSION_STR;
};

namespace picojson {
class value;
}
struct BibInfo {
	BibInfo() {};
	virtual ~BibInfo() {};
	void load(std::string &jsonString);
	struct Option {
		Option();
		void load(const picojson::value &json);

		static const int64_t DEFAULT_FLUSH_THRESHOLD;
		static const int64_t LIMIT_FLUSH_THRESHOLD;
		static const int64_t DEFAULT_BLOCK_SIZE;
		static const int64_t LIMIT_BLOCK_SIZE;

		std::string containerFileType_;
		std::string storeMemoryLimit_;
		std::string logDirectory_;
		int64_t flushThreshold_;
		int64_t blockSize_;
		int64_t preReadNum_;
	};
	struct Container {
		Container();
		void load(const picojson::value &json);

		std::string containerFileBase_;

		DatabaseId databaseId_;
		ContainerId containerId_;
		OId rowIndexOId_;
		OId mvccIndexOId_;
		int64_t initSchemaStatus_;
		SchemaVersionId schemaVersion_;
		std::string database_;
		std::string container_;
		std::string dataAffinity_;
		PartitionId partitionNo_;
		std::string containerType_;
		bool rowKeyAssigned_;
		struct Column {
			Column();
			void load(const picojson::value &json);

			std::string columnName_;
			std::string type_;
			bool notNull_;
		};
		struct TimeSeriesProperties {
			TimeSeriesProperties();
			void load(const picojson::value &json);

			bool isExist_;
			int32_t rowExpirationElapsedTime_;
			std::string rowExpirationTimeUnit_;
			int32_t expirationDivisionCount_;
			std::string compressionMethod_;
			int32_t compressionWindowSize_;
			std::string compressionWindowSizeUnit_;
		};
		struct CompressionInfo {
			CompressionInfo();
			void load(const picojson::value &json);

			std::string columnName_;
			std::string compressionType_;
			double rate_;
			double span_;
			double width_;
		};
		std::vector< Column > columnSet_;
		TimeSeriesProperties timeSeriesProperties_;
		std::vector< CompressionInfo > compressionInfoSet_;
		std::vector< std::string > rowKeySet_;
	};
	Option option_;
	std::vector< Container > containers_;

	template<typename T> static bool setKey(
		const picojson::value &json, const char *key, T &output, bool isOption);
	template<typename T> static bool setIntegerKey(
		const picojson::value &json, const char *key, T &output, bool isOption);
	template<typename T> static bool setStringIntegerKey(
		const picojson::value &json, const char *key, T &output, bool isOption);
private:
};
#endif
