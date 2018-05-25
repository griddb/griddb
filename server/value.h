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
	@brief Definition of Value
*/
#ifndef VALUE_H_
#define VALUE_H_

#include "value_processor.h"
#include <math.h>
#include <vector>

class RowArray;
class Collection;
class Row;
class TimeSeries;
class ColumnInfo;
class MessageRowStore;
class Value;


/*!
	@brief Object for blob
*/
class BinaryObject : public BaseObject {
public:
	BinaryObject(TransactionContext &txn, ObjectManager &objectManager)
		: BaseObject(txn.getPartitionId(), objectManager) {}
	BinaryObject(TransactionContext &txn, ObjectManager &objectManager, OId oId)
		: BaseObject(txn.getPartitionId(), objectManager, oId) {}
	BinaryObject(uint8_t *addr) : BaseObject(addr) {}

	/*!
		@brief Get size of data
		@note contain variable size area
	*/
	static uint32_t getObjectSize(uint32_t size) {
		return ValueProcessor::getEncodedVarSize(size) + size;  
	}

	void setData(uint32_t size, const uint8_t *data) {
		uint32_t sizeLen = ValueProcessor::getEncodedVarSize(size);
		uint32_t encodedSize = ValueProcessor::encodeVarSize(size);
		memcpy(getBaseAddr(), &encodedSize, sizeLen);
		memcpy(getBaseAddr() + sizeLen, data, size);  
	}
	/*!
		@brief Get size of blob
	*/
	uint32_t size() const {
		return ValueProcessor::decodeVarSize(getBaseAddr());
	}
	/*!
		@brief Get value pointer
		@note return head of blob-bainary
	*/
	const uint8_t *data() const {
		uint32_t sizeLen = ValueProcessor::getEncodedVarSize(getBaseAddr());
		return reinterpret_cast<const uint8_t *>(getBaseAddr() + sizeLen);
	}

	/*!
		@brief Get value pointer
		@note all return head of data
	*/
	uint8_t *getImage() {
		return getBaseAddr();
	}
};

/*!
	@brief Object for Fixed-type array
*/
class ArrayObject : public BaseObject {
public:
	ArrayObject(TransactionContext &txn, ObjectManager &objectManager)
		: BaseObject(txn.getPartitionId(), objectManager) {}
	ArrayObject(TransactionContext &txn, ObjectManager &objectManager, OId oId)
		: BaseObject(txn.getPartitionId(), objectManager, oId) {}
	ArrayObject(uint8_t *addr) : BaseObject(addr) {}
	/*!
		@brief Get size of data
		@note contain variable size area
	*/
	static uint32_t getObjectSize(uint32_t arrayLength, ColumnType type) {
		assert(FixedSizeOfColumnType[type] > 0);
		uint32_t arrayLengthLen = ValueProcessor::getEncodedVarSize(
			arrayLength);  
		uint32_t totalSize =
			arrayLengthLen +
			arrayLength * FixedSizeOfColumnType
							  [type];  
		uint32_t totalSizeLen = ValueProcessor::getEncodedVarSize(totalSize);
		uint32_t objectSize = totalSizeLen + totalSize;
		return objectSize;
	}

	/*!
		@brief Get array length
	*/
	uint32_t getArrayLength() const {
		uint32_t totalSizeLen = ValueProcessor::getEncodedVarSize(
			getBaseAddr());  
		return ValueProcessor::decodeVarSize(getBaseAddr() + totalSizeLen);
	}

	/*!
		@brief Get element of array
	*/
	const uint8_t *getArrayElement(uint32_t arrayIndex, ColumnType type) const {
		assert(FixedSizeOfColumnType[type] > 0);
		assert(arrayIndex < getArrayLength());
		uint32_t totalSizeLen =
			ValueProcessor::getEncodedVarSize(getBaseAddr());  
		uint32_t elemCountLen = ValueProcessor::getEncodedVarSize(
			getBaseAddr() + totalSizeLen);  
		return reinterpret_cast<const uint8_t *>(
			getBaseAddr() + totalSizeLen + elemCountLen +
			(arrayIndex * FixedSizeOfColumnType[type]));
	}

	/*!
		@brief Set array length
	*/
	void setArrayLength(uint32_t arrayLength, ColumnType type) {
		assert(FixedSizeOfColumnType[type] > 0);
		uint32_t arrayLengthLen =
			ValueProcessor::getEncodedVarSize(arrayLength);
		uint32_t totalSize =
			arrayLengthLen + arrayLength * FixedSizeOfColumnType[type];

		uint32_t encodedTotalSize =
			ValueProcessor::encodeVarSize(totalSize);  
		uint32_t totalSizeLen = ValueProcessor::getEncodedVarSize(totalSize);
		uint32_t encodedArrayLength =
			ValueProcessor::encodeVarSize(arrayLength);  
		memcpy(getBaseAddr(), &encodedTotalSize, totalSizeLen);
		memcpy(
			getBaseAddr() + totalSizeLen, &encodedArrayLength, arrayLengthLen);
	}

	/*!
		@brief Set element of array
	*/
	void setArrayElement(
		uint32_t arrayIndex, ColumnType type, const uint8_t *data) {
		assert(FixedSizeOfColumnType[type] > 0);
		assert(arrayIndex < getArrayLength());
		memcpy(getArrayElementForUpdate(arrayIndex, type), data,
			FixedSizeOfColumnType[type]);
	}

	/*!
		@brief Get value pointer
		@note all return head of data
	*/
	uint8_t *getImage() {
		return getBaseAddr();
	}

private:
	uint8_t *getArrayElementForUpdate(uint32_t arrayIndex, ColumnType type) {
		assert(FixedSizeOfColumnType[type] > 0);
		assert(arrayIndex < getArrayLength());
		uint32_t totalSizeLen =
			ValueProcessor::getEncodedVarSize(getBaseAddr());  
		uint32_t elemCountLen = ValueProcessor::getEncodedVarSize(
			getBaseAddr() + totalSizeLen);  
		return reinterpret_cast<uint8_t *>(
			getBaseAddr() + totalSizeLen + elemCountLen +
			(arrayIndex * FixedSizeOfColumnType[type]));
	}
};

/*!
	@brief Cursor for blob/string array
*/
class MatrixCursor {
public:
	/*!
		@brief Get variable size
	*/
	uint32_t getTotalSize() const {
		const uint8_t *cursor = &variant_;
		uint32_t elemSizeLen = ValueProcessor::getEncodedVarSize(
			&variant_);  
		cursor += elemSizeLen;
		uint32_t totalSize = *reinterpret_cast<const uint32_t *>(cursor);
		return totalSize;
	}

	/*!
		@brief Get linked OId
	*/
	OId getHeaderOId() const {
		const uint8_t *cursor = &variant_;
		uint32_t elemSizeLen = ValueProcessor::getEncodedVarSize(&variant_);
		cursor += elemSizeLen;		 
		cursor += sizeof(uint32_t);  
		return *reinterpret_cast<const OId *>(cursor);
	}

	void initialize() {
		variant_ = 0;
	}

	/*!
		@brief Set variable size
	*/
	void setTotalSize(uint32_t totalSize) {
		uint32_t encodedArrayLength = ValueProcessor::encodeVarSize(
			LINK_VARIABLE_COLUMN_DATA_SIZE);  
		uint32_t arrayLengthLen =
			ValueProcessor::getEncodedVarSize(LINK_VARIABLE_COLUMN_DATA_SIZE);
		memcpy(&variant_, &encodedArrayLength, arrayLengthLen);
		memcpy(&variant_ + arrayLengthLen, &totalSize, sizeof(uint32_t));
	}

	/*!
		@brief Set linked OId
	*/
	void setHeaderOId(OId headerOId) {
		uint8_t *cursor = &variant_;
		uint32_t elemSizeLen = ValueProcessor::getEncodedVarSize(&variant_);
		cursor += elemSizeLen;		 
		cursor += sizeof(uint32_t);  

		uint32_t arrayLengthLen =
			ValueProcessor::getEncodedVarSize(LINK_VARIABLE_COLUMN_DATA_SIZE);
		uint32_t totalSizeLen = sizeof(uint32_t);
		memcpy(
			&variant_ + arrayLengthLen + totalSizeLen, &headerOId, sizeof(OId));
	}

	/*!
		@brief Set variable size and linked OId
	*/
	static void setVariableDataInfo(
		void *objectRowField, OId linkOId, uint32_t blobDataSize) {
		uint8_t *addr = reinterpret_cast<uint8_t *>(objectRowField);
		uint32_t encodedElemNum =
			ValueProcessor::encodeVarSize(LINK_VARIABLE_COLUMN_DATA_SIZE);
		uint32_t elemNumLen =
			ValueProcessor::getEncodedVarSize(LINK_VARIABLE_COLUMN_DATA_SIZE);
		memcpy(addr, &encodedElemNum, elemNumLen);
		addr += elemNumLen;
		memcpy(addr, &blobDataSize, sizeof(uint32_t));
		addr += sizeof(uint32_t);
		memcpy(addr, &linkOId, sizeof(OId));
	}

	/*!
		@brief Get value pointer
	*/
	uint8_t *getImage() {
		return reinterpret_cast<uint8_t *>(&variant_);
	}

private:
	uint8_t variant_;
};

/*!
	@brief Field Value Wrapper
*/
class Value {
private:
	ColumnType type_;  
	struct Object {
		const void *value_;  
		uint32_t totalSize_;  
		bool onDataStore_;	
		void set(const void *value, bool onDataStore) {
			value_ = value;
			totalSize_ = UINT32_MAX;
			onDataStore_ = onDataStore;
		}
		void set(const void *value, uint32_t size, bool onDataStore) {
			value_ = value;
			totalSize_ = size;
			onDataStore_ = onDataStore;
		}
	};
	union Member {  
		bool bool_;
		int8_t byte_;
		int16_t short_;
		int32_t int_;
		int64_t long_;
		float float_;
		double double_;
		Timestamp timestamp_;
		Object object_;  
	};
	Member data_;

public:
	Value() : type_(COLUMN_TYPE_STRING) {
		data_.object_.set(NULL, false);
	}
	explicit Value(bool b) : type_(COLUMN_TYPE_BOOL) {
		data_.bool_ = b;
	}
	explicit Value(int8_t i) : type_(COLUMN_TYPE_BYTE) {
		data_.byte_ = i;
	}
	explicit Value(int16_t i) : type_(COLUMN_TYPE_SHORT) {
		data_.short_ = i;
	}
	explicit Value(int32_t i) : type_(COLUMN_TYPE_INT) {
		data_.int_ = i;
	}
	explicit Value(int64_t i) : type_(COLUMN_TYPE_LONG) {
		data_.long_ = i;
	}
	explicit Value(float f) : type_(COLUMN_TYPE_FLOAT) {
		data_.float_ = f;
	}
	explicit Value(double d) : type_(COLUMN_TYPE_DOUBLE) {
		data_.double_ = d;
	}
	/*!
		@brief Set string value
		@note convert to RowMessage/RowObject string format
	*/
	explicit Value(util::StackAllocator &alloc, const char *s)
		: type_(COLUMN_TYPE_STRING) {
		uint32_t strSize = static_cast<uint32_t>(strlen(s));
		uint32_t encodedSize = ValueProcessor::encodeVarSize(strSize);
		uint32_t sizeLen = ValueProcessor::getEncodedVarSize(strSize);
		char *target =
			reinterpret_cast<char *>(alloc.allocate(sizeLen + strSize));
		memcpy(target, &encodedSize, sizeLen);
		memcpy(target + sizeLen, s, strSize);
		data_.object_.set(target, false);
	}
	/*!
		@brief Set string value
		@note convert to RowMessage/RowObject string format
	*/
	explicit Value(util::StackAllocator &alloc, const char *s, uint32_t strSize)
		: type_(COLUMN_TYPE_STRING) {  
		uint32_t encodedSize = ValueProcessor::encodeVarSize(strSize);
		uint32_t sizeLen = ValueProcessor::getEncodedVarSize(strSize);
		char *target =
			reinterpret_cast<char *>(alloc.allocate(sizeLen + strSize));
		memcpy(target, &encodedSize, sizeLen);
		memcpy(target + sizeLen, s, strSize);
		data_.object_.set(target, false);
	}

	inline void set(bool b) {
		type_ = COLUMN_TYPE_BOOL;
		data_.bool_ = b;
	}
	inline void set(int8_t i) {
		type_ = COLUMN_TYPE_BYTE;
		data_.byte_ = i;
	}
	inline void set(int16_t i) {
		type_ = COLUMN_TYPE_SHORT;
		data_.short_ = i;
	}
	inline void set(int32_t i) {
		type_ = COLUMN_TYPE_INT;
		data_.int_ = i;
	}
	inline void set(int64_t i) {
		type_ = COLUMN_TYPE_LONG;
		data_.long_ = i;
	}
	inline void set(float f) {
		type_ = COLUMN_TYPE_FLOAT;
		data_.float_ = f;
	}
	inline void set(double d) {
		type_ = COLUMN_TYPE_DOUBLE;
		data_.double_ = d;
	}

	/*!
		@brief Set string value
		@note convert to RowMessage/RowObject string format
	*/
	inline void set(util::StackAllocator &alloc, char *s) {
		type_ = COLUMN_TYPE_STRING;
		uint32_t strSize = static_cast<uint32_t>(strlen(s));
		uint32_t encodedSize = ValueProcessor::encodeVarSize(strSize);
		uint32_t sizeLen = ValueProcessor::getEncodedVarSize(strSize);
		char *target =
			reinterpret_cast<char *>(alloc.allocate(sizeLen + strSize));
		memcpy(target, &encodedSize, sizeLen);
		memcpy(target + sizeLen, s, strSize);
		data_.object_.set(target, false);
	}
	/*!
		@brief Set string value
		@note convert to RowMessage/RowObject string format
	*/
	inline void set(util::StackAllocator &alloc, char *s,
		uint32_t strSize) {  
		type_ = COLUMN_TYPE_STRING;
		uint32_t encodedSize = ValueProcessor::encodeVarSize(strSize);
		uint32_t sizeLen = ValueProcessor::getEncodedVarSize(strSize);
		char *target =
			reinterpret_cast<char *>(alloc.allocate(sizeLen + strSize));
		memcpy(target, &encodedSize, sizeLen);
		memcpy(target + sizeLen, s, strSize);
		data_.object_.set(target, false);
	}
	inline void setTimestamp(Timestamp i) {
		type_ = COLUMN_TYPE_TIMESTAMP;
		data_.timestamp_ = i;
	}

	void copy(TransactionContext &txn, ObjectManager &objectManager,
		const Value &srcValue);


	/*!
		@brief Get value pointer
		@note string and geometry type return head of string and head of
	   geometry-bainary
	*/
	UTIL_FORCEINLINE const uint8_t *data() const {
		switch (type_) {
		case COLUMN_TYPE_BOOL:
		case COLUMN_TYPE_BYTE:
		case COLUMN_TYPE_SHORT:
		case COLUMN_TYPE_INT:
		case COLUMN_TYPE_LONG:
		case COLUMN_TYPE_FLOAT:
		case COLUMN_TYPE_DOUBLE:
		case COLUMN_TYPE_TIMESTAMP:
		case COLUMN_TYPE_OID:
			return reinterpret_cast<const uint8_t *>(&data_);
			break;
		case COLUMN_TYPE_STRING: {
			const uint8_t *stringObject =
				reinterpret_cast<const uint8_t *>(data_.object_.value_);
			if (stringObject) {
				uint32_t size = ValueProcessor::getEncodedVarSize(stringObject);
				return (stringObject + size);  
			}
			else {
				return NULL;
			}
		} break;
		case COLUMN_TYPE_BLOB:
		case COLUMN_TYPE_STRING_ARRAY:
		case COLUMN_TYPE_BOOL_ARRAY:
		case COLUMN_TYPE_BYTE_ARRAY:
		case COLUMN_TYPE_SHORT_ARRAY:
		case COLUMN_TYPE_INT_ARRAY:
		case COLUMN_TYPE_LONG_ARRAY:
		case COLUMN_TYPE_FLOAT_ARRAY:
		case COLUMN_TYPE_DOUBLE_ARRAY:
		case COLUMN_TYPE_TIMESTAMP_ARRAY:
			return reinterpret_cast<const uint8_t *>(data_.object_.value_);
			break;
		default:
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
			break;
		}
	}
	/*!
		@brief Get value size
		@note string and geometry type return string-length and geometry-bainary
	   length
	*/
	UTIL_FORCEINLINE uint32_t size() const {
		uint32_t size = 0;
		switch (type_) {
		case COLUMN_TYPE_BOOL:
		case COLUMN_TYPE_BYTE:
		case COLUMN_TYPE_SHORT:
		case COLUMN_TYPE_INT:
		case COLUMN_TYPE_LONG:
		case COLUMN_TYPE_FLOAT:
		case COLUMN_TYPE_DOUBLE:
		case COLUMN_TYPE_TIMESTAMP:
		case COLUMN_TYPE_OID:
			size = FixedSizeOfColumnType[type_];
			break;
		case COLUMN_TYPE_STRING: {
			if (data_.object_.value_ != NULL) {
				size = ValueProcessor::decodeVarSize(data_.object_.value_);
			}
		} break;
		case COLUMN_TYPE_BLOB:
			if (data_.object_.value_ != NULL) {
				return data_.object_.totalSize_;
			}
			break;
		case COLUMN_TYPE_STRING_ARRAY:
			if (data_.object_.value_ != NULL) {
				return data_.object_.totalSize_;
			}
			break;
		case COLUMN_TYPE_BOOL_ARRAY:
		case COLUMN_TYPE_BYTE_ARRAY:
		case COLUMN_TYPE_SHORT_ARRAY:
		case COLUMN_TYPE_INT_ARRAY:
		case COLUMN_TYPE_LONG_ARRAY:
		case COLUMN_TYPE_FLOAT_ARRAY:
		case COLUMN_TYPE_DOUBLE_ARRAY:
		case COLUMN_TYPE_TIMESTAMP_ARRAY: {
			if (data_.object_.value_ != NULL) {
				const ArrayObject arrayObject(reinterpret_cast<uint8_t *>(
					const_cast<void *>(data_.object_.value_)));
				size = arrayObject.getObjectSize(arrayObject.getArrayLength(),
					ValueProcessor::getSimpleColumnType(type_));
			}
		} break;
		default:
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
			break;
		}
		return size;
	}

	/*!
		@brief Get value pointer
		@note all data type return head of data
	*/
	UTIL_FORCEINLINE const uint8_t *getImage() const {
		switch (type_) {
		case COLUMN_TYPE_BOOL:
		case COLUMN_TYPE_BYTE:
		case COLUMN_TYPE_SHORT:
		case COLUMN_TYPE_INT:
		case COLUMN_TYPE_LONG:
		case COLUMN_TYPE_FLOAT:
		case COLUMN_TYPE_DOUBLE:
		case COLUMN_TYPE_TIMESTAMP:
		case COLUMN_TYPE_OID:
			return reinterpret_cast<const uint8_t *>(&data_);
			break;
		case COLUMN_TYPE_STRING:
			return reinterpret_cast<const uint8_t *>(
				data_.object_.value_);  
			break;
		case COLUMN_TYPE_BLOB:
		case COLUMN_TYPE_STRING_ARRAY:
		case COLUMN_TYPE_BOOL_ARRAY:
		case COLUMN_TYPE_BYTE_ARRAY:
		case COLUMN_TYPE_SHORT_ARRAY:
		case COLUMN_TYPE_INT_ARRAY:
		case COLUMN_TYPE_LONG_ARRAY:
		case COLUMN_TYPE_FLOAT_ARRAY:
		case COLUMN_TYPE_DOUBLE_ARRAY:
		case COLUMN_TYPE_TIMESTAMP_ARRAY:
			return reinterpret_cast<const uint8_t *>(data_.object_.value_);
			break;
		default:
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
			break;
		}
	}

	/*!
		@brief Initialize default value
	*/
	void init(ColumnType type) {
		type_ = type;
		switch (type_) {
		case COLUMN_TYPE_BOOL:
		case COLUMN_TYPE_BYTE:
		case COLUMN_TYPE_SHORT:
		case COLUMN_TYPE_INT:
		case COLUMN_TYPE_LONG:
		case COLUMN_TYPE_FLOAT:
		case COLUMN_TYPE_DOUBLE:
		case COLUMN_TYPE_TIMESTAMP:
		case COLUMN_TYPE_OID:
			memset(&data_, 0, FixedSizeOfColumnType[type_]);
			break;
		case COLUMN_TYPE_STRING:
		case COLUMN_TYPE_BLOB:
		case COLUMN_TYPE_STRING_ARRAY:
		case COLUMN_TYPE_BOOL_ARRAY:
		case COLUMN_TYPE_BYTE_ARRAY:
		case COLUMN_TYPE_SHORT_ARRAY:
		case COLUMN_TYPE_INT_ARRAY:
		case COLUMN_TYPE_LONG_ARRAY:
		case COLUMN_TYPE_FLOAT_ARRAY:
		case COLUMN_TYPE_DOUBLE_ARRAY:
		case COLUMN_TYPE_TIMESTAMP_ARRAY:
			data_.object_.onDataStore_ = false;
			data_.object_.value_ = NULL;
			break;
		default:
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
			break;
		}
	}
	bool isArray() const {
		return ValueProcessor::isArray(type_);
	}
	bool onDataStore() const {
		if (isSimple()) {
			return false;
		}
		else {
			return data_.object_.onDataStore_;
		}
	}
	ColumnType getType() const {
		return type_;
	}

	inline bool isNumerical() const {
		return ValueProcessor::isNumerical(type_);
	}
	inline bool isInteger() const {
		return ValueProcessor::isInteger(type_);
	}
	inline bool isFloat() const {
		return ValueProcessor::isFloat(type_);
	}

	inline bool isSimple() const {
		return ValueProcessor::isSimple(type_);
	}

	/*!
		@brief Cast boolean value
	*/
	bool getBool() const {
		return ValueProcessor::getBool(type_, data());
	}
	/*!
		@brief Cast byte value
	*/
	int8_t getByte() const {
		return ValueProcessor::getByte(type_, data());
	}
	/*!
		@brief Cast short value
	*/
	int16_t getShort() const {
		return ValueProcessor::getShort(type_, data());
	}
	/*!
		@brief Cast int value
	*/
	int32_t getInt() const {
		return ValueProcessor::getInt(type_, data());
	}
	/*!
		@brief Cast long value
	*/
	int64_t getLong() const {
		return ValueProcessor::getLong(type_, data());
	}
	/*!
		@brief Cast float value
	*/
	float getFloat() const {
		return ValueProcessor::getFloat(type_, data());
	}
	/*!
		@brief Cast double value
	*/
	double getDouble() const {
		return ValueProcessor::getDouble(type_, data());
	}
	/*!
		@brief Cast Timestamp value
	*/
	Timestamp getTimestamp() const {
		return ValueProcessor::getTimestamp(type_, data());
	}


	/*!
		@brief Get array length
	*/
	uint32_t getArrayLength(
		TransactionContext &txn, ObjectManager &objectManager) const {
		if ((!isArray()) || (data_.object_.value_ == NULL)) return 0;
		switch (type_) {
		case COLUMN_TYPE_STRING_ARRAY: {
			const uint8_t *addr =
				reinterpret_cast<const uint8_t *>(data_.object_.value_);
			uint32_t varDataSize = ValueProcessor::decodeVarSize(addr);
			addr += ValueProcessor::getEncodedVarSize(varDataSize);
			addr += sizeof(uint32_t);
			const OId *oId = reinterpret_cast<const OId *>(addr);
			if (*oId != UNDEF_OID) {
				VariableArrayCursor arrayCursor(
					txn, objectManager, *oId, OBJECT_READ_ONLY);
				return arrayCursor.getArrayLength();
			}
			else {
				return 0;
			}
		} break;
		case COLUMN_TYPE_BOOL_ARRAY:
		case COLUMN_TYPE_BYTE_ARRAY:
		case COLUMN_TYPE_SHORT_ARRAY:
		case COLUMN_TYPE_INT_ARRAY:
		case COLUMN_TYPE_LONG_ARRAY:
		case COLUMN_TYPE_FLOAT_ARRAY:
		case COLUMN_TYPE_DOUBLE_ARRAY:
		case COLUMN_TYPE_TIMESTAMP_ARRAY: {
			const ArrayObject arrayObject(reinterpret_cast<uint8_t *>(
				const_cast<void *>(data_.object_.value_)));
			if (arrayObject.getBaseAddr() != NULL) {
				return arrayObject.getArrayLength();
			}
			else {
				return 0;
			}
		} break;
		default:
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
			break;
		}
	}
	void getArrayElement(TransactionContext &txn, ObjectManager &objectManager,
		uint32_t i, const uint8_t *&data, uint32_t &size) const;


	void serialize(util::XArray<uint8_t> &serializedRowList)
		const;  


	void get(TransactionContext &txn, ObjectManager &objectManager,
		MessageRowStore *messageRowStore,
		ColumnId columnId);  

	std::string dump(
		TransactionContext &txn, ObjectManager &objectManager) const;

public:
	inline void set(const void *data, ColumnType type) {
		type_ = type;

		if (isSimple()) {
			memcpy(&data_, data, FixedSizeOfColumnType[type_]);
		}
		else if ((isArray() && type != COLUMN_TYPE_STRING_ARRAY)
				 || type == COLUMN_TYPE_STRING) {
			data_.object_.set(data, true);
		}
		else {
			if (data != NULL) {
				uint32_t varDataSize = ValueProcessor::decodeVarSize(data);
				const uint8_t *objectData =
					reinterpret_cast<const uint8_t *>(data);  
				const uint8_t *addr =
					objectData + ValueProcessor::getEncodedVarSize(varDataSize);
				uint32_t totalSize = *reinterpret_cast<const uint32_t *>(addr);
				addr += sizeof(uint32_t);
				const OId *oId = reinterpret_cast<const OId *>(addr);
				if (*oId != UNDEF_OID) {
					data_.object_.set(objectData, totalSize, true);
				}
				else {
					data_.object_.set(NULL, 0, true);
				}
			}
			else {
				data_.object_.set(NULL, 0, true);
			}
		}
	}
	inline void set(const uint8_t *value, uint32_t, ColumnType type) {
		type_ = type;
		data_.object_.set(const_cast<uint8_t *>(value), true);
	}

};

typedef bool (*Operator)(TransactionContext &txn, uint8_t const *p,
	uint32_t size1, uint8_t const *q, uint32_t size2);
typedef int32_t (*Comparator)(TransactionContext &txn, uint8_t const *p,
	uint32_t size1, uint8_t const *q, uint32_t size2);
typedef bool (*Operator)(TransactionContext &txn, uint8_t const *p,
	uint32_t size1, uint8_t const *q, uint32_t size2);

typedef void (*Calculator1)(
	TransactionContext &txn, uint8_t const *p, uint32_t size1, Value &value);
typedef void (*Calculator2)(TransactionContext &txn, uint8_t const *p,
	uint32_t size1, uint8_t const *q, uint32_t size2, Value &value);

/*!
	@brief Term Condition
*/
struct TermCondition {
	Operator operator_;		 
	uint32_t columnId_;		 
	uint32_t columnOffset_;  
	const uint8_t *value_;   
	uint32_t valueSize_;  

	TermCondition()
		: operator_(NULL),
		  columnId_(0),
		  columnOffset_(0),
		  value_(NULL),
		  valueSize_(0) {}
	TermCondition(Operator operatorFunction, uint32_t columnId,
		uint32_t columnOffset, const uint8_t *value, uint32_t valueSize)
		: operator_(operatorFunction),
		  columnId_(columnId),
		  columnOffset_(columnOffset),
		  value_(value),
		  valueSize_(valueSize) {}
};

#endif
