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
class Geometry;
class ArchiveHandler;

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
	static const uint8_t defalutFixedValue_[8];
	static const uint8_t defalutStringValue_[1];
	static const uint8_t defalutFixedArrayValue_[2];
	static const uint8_t defalutStringArrayValue_[13];
	static const uint8_t defalutBlobValue_[2];
	static const uint8_t defalutGeometryValue_[7];

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
		uint64_t encodedSize = ValueProcessor::encodeVarSize(strSize);
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
		uint64_t encodedSize = ValueProcessor::encodeVarSize(strSize);
		uint32_t sizeLen = ValueProcessor::getEncodedVarSize(strSize);
		char *target =
			reinterpret_cast<char *>(alloc.allocate(sizeLen + strSize));
		memcpy(target, &encodedSize, sizeLen);
		memcpy(target + sizeLen, s, strSize);
		data_.object_.set(target, false);
	}
	explicit Value(Geometry *geom) : type_(COLUMN_TYPE_GEOMETRY) {
		data_.object_.set(geom, false);
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
	inline void setNull() {
		type_ = COLUMN_TYPE_NULL;
		data_.object_.value_ = NULL;
	}

	/*!
		@brief Set string value
		@note convert to RowMessage/RowObject string format
	*/
	inline void set(util::StackAllocator &alloc, char *s) {
		type_ = COLUMN_TYPE_STRING;
		uint32_t strSize = static_cast<uint32_t>(strlen(s));
		uint64_t encodedSize = ValueProcessor::encodeVarSize(strSize);
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
		uint64_t encodedSize = ValueProcessor::encodeVarSize(strSize);
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
		case COLUMN_TYPE_GEOMETRY: {
			if (data_.object_.value_ != NULL) {
				const uint8_t *object =
					reinterpret_cast<const uint8_t *>(data_.object_.value_);
				uint32_t size = ValueProcessor::getEncodedVarSize(object);
				return (object + size);  
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
		case COLUMN_TYPE_NULL:
			return NULL;
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
		case COLUMN_TYPE_GEOMETRY: {
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
					FixedSizeOfColumnType[ValueProcessor::getSimpleColumnType(type_)]);
			}
		} break;
		case COLUMN_TYPE_NULL:
			size = 0;
			break;
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
		case COLUMN_TYPE_GEOMETRY:
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
		case COLUMN_TYPE_NULL:
			return NULL;
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
		case COLUMN_TYPE_GEOMETRY:
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
		case COLUMN_TYPE_NULL:
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
	inline bool isNullValue() const {
		return type_ == COLUMN_TYPE_NULL;
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

	void archive(TransactionContext &txn, ObjectManager &objectManager, ArchiveHandler *handler) const;
	void dump(
		TransactionContext &txn, ObjectManager &objectManager, 
		util::NormalOStringStream &stream, bool forExport = false) const;

public:
	inline void set(const void *data, ColumnType type) {
		type_ = type;

		if (isSimple()) {
			memcpy(&data_, data, FixedSizeOfColumnType[type_]);
		}
		else if ((isArray() && type != COLUMN_TYPE_STRING_ARRAY)
				 || type == COLUMN_TYPE_GEOMETRY
				 || type == COLUMN_TYPE_STRING) {
			data_.object_.set(data, true);
		}
		else if (type == COLUMN_TYPE_BLOB) {
			if (data != NULL) {
				const uint8_t *objectData =
					reinterpret_cast<const uint8_t *>(data);  
				uint32_t totalSize = static_cast<uint32_t>(BlobCursor::getTotalSize(objectData));
				data_.object_.set(objectData, totalSize, true);
			}
			else {
				data_.object_.set(NULL, 0, true);
			}
		} else {
			if (data != NULL) {
				uint32_t varDataSize = ValueProcessor::decodeVarSize(data);
				const uint8_t *objectData =
					reinterpret_cast<const uint8_t *>(data);  
				const uint8_t *addr =
					objectData + ValueProcessor::getEncodedVarSize(varDataSize);
				uint32_t totalSize = *reinterpret_cast<const uint32_t *>(addr);
				addr += sizeof(uint32_t);
				data_.object_.set(objectData, totalSize, true);
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


	static const void *getDefaultFixedValue(ColumnType type) {
		UNUSED_VARIABLE(type);
		return defalutFixedValue_;
	}
	static const void *getDefaultVariableValue(ColumnType type) {
		switch (type) {
		case COLUMN_TYPE_STRING:
			return defalutStringValue_;
			break;
		case COLUMN_TYPE_GEOMETRY:
			return defalutGeometryValue_;
			break;
		case COLUMN_TYPE_BLOB:
			return defalutBlobValue_;
			break;
		case COLUMN_TYPE_STRING_ARRAY:
			return defalutStringArrayValue_;
			break;
		case COLUMN_TYPE_BOOL_ARRAY:
		case COLUMN_TYPE_BYTE_ARRAY:
		case COLUMN_TYPE_SHORT_ARRAY:
		case COLUMN_TYPE_INT_ARRAY:
		case COLUMN_TYPE_LONG_ARRAY:
		case COLUMN_TYPE_FLOAT_ARRAY:
		case COLUMN_TYPE_DOUBLE_ARRAY:
		case COLUMN_TYPE_TIMESTAMP_ARRAY:
			return defalutFixedArrayValue_;
			break;
		default:
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
			break;
		}
		return NULL;
	}
};

/*!
	@brief Object for Container field value
*/
class ContainerValue {
public:
	ContainerValue(PartitionId pId, ObjectManager &objectManager)
		: baseObj_(pId, objectManager) {}
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
	void setNull() {
		value_.setNull();
	}
	void init(ColumnType type) {
		value_.init(type);
	}
private:
	BaseObject baseObj_;
	Value value_;

private:
	ContainerValue(const ContainerValue &);  
	ContainerValue &operator=(
		const ContainerValue &);  
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
*	@brief Represents the type of Operation
*/
class DSExpression {
public:
	enum Operation {
		ADD,
		SUB,
		MUL,
		DIV,
		REM,
		IS,
		ISNOT,
		BITNOT,
		BITAND,
		BITOR,
		LSHIFT,
		RSHIFT,
		NE,
		EQ,
		LT,
		LE,
		GT,
		GE,
		PLUS,
		MINUS,
		BITMINUS,
		BETWEEN,
		NOTBETWEEN,
		NONE,
		IS_NULL,
		IS_NOT_NULL
		,
		GEOM_OP
	};

	static const char *getOperationStr(Operation op) {
		switch (op) {
		case ADD: return "+"; break;
		case SUB: return "-"; break;
		case MUL: return "*"; break;
		case DIV: return "/"; break;
		case REM: return "REM"; break;
		case IS: return "IS"; break;
		case ISNOT: return "ISNOT"; break;
		case BITNOT: return "BITNOT"; break;
		case BITAND: return "BITAND"; break;
		case BITOR: return "BITOR"; break;
		case LSHIFT: return "LSHIFT"; break;
		case RSHIFT: return "RSHIFT"; break;
		case NE: return "!="; break;
		case EQ: return "="; break;
		case LT: return "<"; break;
		case LE: return "<="; break;
		case GT: return ">"; break;
		case GE: return ">="; break;
		case PLUS: return "PLUS"; break;
		case MINUS: return "MINUS"; break;
		case BITMINUS: return "BITMINUS"; break;
		case BETWEEN: return "BETWEEN"; break;
		case NOTBETWEEN: return "NOTBETWEEN"; break;
		case NONE: return "NONE"; break;
		case IS_NULL: return "IS_NULL"; break;
		case IS_NOT_NULL: return "IS_NOT_NULL"; break;
		case GEOM_OP: return "GEOM_OP"; break;
		default: assert(false);
		}
		return NULL;
	}

	static bool isIncluded(Operation op) {
		if (op == EQ || op == LE || op == GE) {
			return true;
		}
		return false;
	}
	static bool isStartCondition(Operation op) {
		if (op == GT || op == GE || op == EQ) {
			return true;
		}
		return false;
	}
	static bool isEndCondition(Operation op) {
		if (op == LT || op == LE) {
			return true;
		}
		return false;
	}
};


#endif
