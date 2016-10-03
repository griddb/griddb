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
	@brief Definition of ValueProcessor
*/
#ifndef VALUE_PROCESSOR_H_
#define VALUE_PROCESSOR_H_

#include "util/container.h"
#include "base_object.h"
#include "data_store_common.h"
#include "data_type.h"
#include "gs_error.h"
#include "object_manager.h"
#include "transaction_context.h"
#include <math.h>
#include <vector>

class RowArray;
class Collection;
class Row;
class TimeSeries;
class ColumnInfo;
class MessageRowStore;
class Value;
class BaseObject;

/*!
	@brief Processes Field value
*/
class ValueProcessor {
public:
	static int32_t compare(TransactionContext &txn,
		ObjectManager &objectManager, ColumnId columnId,
		MessageRowStore *messageRowStore, uint8_t *objectRowField);

	static int32_t compare(TransactionContext &txn,
		ObjectManager &objectManager, ColumnType type,
		uint8_t *srcObjectRowField, uint8_t *targetObjectRowField);

	static void getField(TransactionContext &txn, ObjectManager &objectManager,
		ColumnId columnId, Value *objectValue,
		MessageRowStore *outputMessageRowStore);

	static void getField(TransactionContext &txn, ObjectManager &objectManager,
		ColumnId columnId, uint32_t recordNth, Value *objectValue,
		MessageRowStore *outputMessageRowStore);  

	static void initField(ColumnType type, void *objectField);

	/*
	 * VarSize format
	 * first 1byte
	 * xxxxxx00 4byte(to 2^30=1G-1)
	 * xxxxxx10 8byte(OID)
	 * xxxxxxx1 1byte(to 127)
	*/

	/*!
		@brief Check if variable size is 1 byte
	*/
	static inline bool varSizeIs1Byte(void *ptr) {
		return ((*reinterpret_cast<uint8_t *>(ptr) & 0x01) == 0x01);
	}
	static inline bool varSizeIs1Byte(const void *ptr) {
		return ((*reinterpret_cast<const uint8_t *>(ptr) & 0x01) == 0x01);
	}
	/*!
		@brief Check if variable size is 4 byte
	*/
	static inline bool varSizeIs4Byte(void *ptr) {
		return ((*reinterpret_cast<uint8_t *>(ptr) & 0x03) == 0x00);
	}
	static inline bool varSizeIs4Byte(const void *ptr) {
		return ((*reinterpret_cast<const uint8_t *>(ptr) & 0x03) == 0x00);
	}
	/*!
		@brief Check if variable size is 8 byte
	*/
	static inline bool varSizeIs8Byte(void *ptr) {
		return ((*reinterpret_cast<uint8_t *>(ptr) & 0x03) == 0x02);
	}
	static inline bool varSizeIs8Byte(const void *ptr) {
		return ((*reinterpret_cast<const uint8_t *>(ptr) & 0x03) == 0x02);
	}

	/*!
		@brief Check if variable size is 1 byte
	*/
	static inline bool varSizeIs1Byte(uint8_t val) {
		return ((val & 0x01) == 0x01);
	}
	/*!
		@brief Check if variable size is 4 byte
	*/
	static inline bool varSizeIs4Byte(uint8_t val) {
		return ((val & 0x03) == 0x00);
	}
	/*!
		@brief Check if variable size is 8 byte
	*/
	static inline bool varSizeIs8Byte(uint8_t val) {
		return ((val & 0x03) == 0x02);
	}

	/*!
		@brief Decode variable size (1byte)
	*/
	static inline uint32_t decode1ByteVarSize(uint8_t val) {
		assert(val != 0);
		return val >> 1;
	}
	/*!
		@brief Decode variable size (4byte)
	*/
	static inline uint32_t decode4ByteVarSize(uint32_t val) {
		assert(val != 0);
		return val >> 2;
	}
	/*!
		@brief Decode variable size (8byte)
	*/
	static inline uint64_t decode8ByteVarSize(uint64_t val) {
		assert(val != 0);
		return val >> 2;
	}

	/*!
		@brief Decode variable size (1byte)
	*/
	static inline uint32_t get1ByteVarSize(void *ptr) {
		assert(*reinterpret_cast<uint8_t *>(ptr) != 0);
		return (*reinterpret_cast<uint8_t *>(ptr) >> 1);
	}
	static inline uint32_t get1ByteVarSize(const void *ptr) {
		assert(*reinterpret_cast<const uint8_t *>(ptr) != 0);
		return (*reinterpret_cast<const uint8_t *>(ptr) >> 1);
	}
	/*!
		@brief Decode variable size (4byte)
	*/
	static inline uint32_t get4ByteVarSize(void *ptr) {
		assert(*reinterpret_cast<uint32_t *>(ptr) != 0);
		return (*reinterpret_cast<uint32_t *>(ptr) >> 2);
	}
	static inline uint32_t get4ByteVarSize(const void *ptr) {
		assert(*reinterpret_cast<const uint32_t *>(ptr) != 0);
		return (*reinterpret_cast<const uint32_t *>(ptr) >> 2);
	}
	/*!
		@brief Decode variable size (8byte)
	*/
	static inline uint64_t getOIdVarSize(void *ptr) {
		assert(*reinterpret_cast<uint64_t *>(ptr) != 0);
		return (*reinterpret_cast<uint64_t *>(ptr) >> 2);
	}
	static inline uint64_t getOIdVarSize(const void *ptr) {
		assert(*reinterpret_cast<const uint64_t *>(ptr) != 0);
		return (*reinterpret_cast<const uint64_t *>(ptr) >> 2);
	}

	/*!
		@brief Decode variable size (1 or 4byte)
	*/
	static inline uint32_t decodeVarSize(void *ptr) {
		if (varSizeIs1Byte(ptr)) {
			return get1ByteVarSize(ptr);
		}
		else {
			assert(varSizeIs4Byte(ptr));
			return get4ByteVarSize(ptr);
		}
	}
	static inline uint32_t decodeVarSize(const void *ptr) {
		if (varSizeIs1Byte(ptr)) {
			return get1ByteVarSize(ptr);
		}
		else {
			assert(varSizeIs4Byte(ptr));
			return get4ByteVarSize(ptr);
		}
	}

	/*!
		@brief Decode variable size or OId (1 or 4 or 8byte)
	*/
	static inline uint64_t decodeVarSizeOrOId(void *ptr, bool &isOId) {
		isOId = false;
		if (varSizeIs1Byte(ptr)) {
			return get1ByteVarSize(ptr);
		}
		else if (varSizeIs4Byte(ptr)) {
			return get4ByteVarSize(ptr);
		}
		else {
			assert(varSizeIs8Byte(ptr));
			isOId = true;
			return getOIdVarSize(ptr);
		}
	}
	static inline uint64_t decodeVarSizeOrOId(const void *ptr, bool &isOId) {
		isOId = false;
		if (varSizeIs1Byte(ptr)) {
			return get1ByteVarSize(ptr);
		}
		else if (varSizeIs4Byte(ptr)) {
			return get4ByteVarSize(ptr);
		}
		else {
			assert(varSizeIs8Byte(ptr));
			isOId = true;
			return getOIdVarSize(ptr);
		}
	}

	/*!
		@brief get Encode Size(1 or 4 or 8 byte)
	*/
	static inline uint32_t getEncodedVarSize(void *ptr) {
		if (varSizeIs1Byte(ptr)) {
			return 1;
		}
		else if (varSizeIs4Byte(ptr)) {
			return 4;
		}
		else {
			assert(varSizeIs8Byte(ptr));
			return 8;
		}
	}
	static inline uint32_t getEncodedVarSize(const void *ptr) {
		if (varSizeIs1Byte(ptr)) {
			return 1;
		}
		else if (varSizeIs4Byte(ptr)) {
			return 4;
		}
		else {
			assert(varSizeIs8Byte(ptr));
			return 8;
		}
	}

	/*!
		@brief get Encode Size(1 or 4 byte)
	*/
	static inline uint32_t getEncodedVarSize(uint32_t val) {
		return (val <= OBJECT_MAX_1BYTE_LENGTH_VALUE ? 1 : 4);
	}

	static inline uint32_t encodeVarSize(uint32_t val) {
		assert(val < 0x40000000L);
		if (val <= OBJECT_MAX_1BYTE_LENGTH_VALUE) {
			return ((val << 1L) | 0x01);
		}
		else {
			return (val << 2L);
		}
	}

	/*!
		@brief get Encode Size(for OId)
	*/
	static inline uint64_t encodeVarSizeOId(uint64_t val) {
		return (val << 2L) | 0x02;
	}

	/*!
		@brief Calculate Null byte size
	*/
	inline static uint32_t calcNullsByteSize(uint32_t columnNum) {
		return (columnNum + 7) / 8;
	}

	/*!
		@brief Validate Column type for array type
	*/
	static bool isValidArrayAndType(bool isArray, ColumnType type) {
		if (isArray && (type <= COLUMN_TYPE_TIMESTAMP)) {
			return true;
		}
		else if (!isArray && (type < COLUMN_TYPE_OID) &&
				 type != COLUMN_TYPE_RESERVED) {
			return true;
		}
		else {
			return false;
		}
	}

	/*!
		@brief Validate Row key type
	*/
	static bool validateRowKeyType(bool isArray, ColumnType type) {
		bool isValid = false;
		if (!isArray) {
			switch (type) {
			case COLUMN_TYPE_STRING:
			case COLUMN_TYPE_INT:
			case COLUMN_TYPE_LONG:
			case COLUMN_TYPE_TIMESTAMP:
				isValid = true;
				break;
			default:
				break;
			}
		}
		return isValid;
	}

	/*!
		@brief Validate Column type except for array typte
	*/
	static bool isValidColumnType(int8_t type) {
		return (type >= COLUMN_TYPE_STRING && type < COLUMN_TYPE_RESERVED) ||
			   (type > COLUMN_TYPE_RESERVED && type <= COLUMN_TYPE_BLOB);
	}
	/*!
		@brief Check if Column type is simple (except variable type)
	*/
	static bool isSimple(ColumnType type) {
		return (type >= COLUMN_TYPE_BOOL && type <= COLUMN_TYPE_TIMESTAMP);
	}
	/*!
		@brief Check if Column type is numerical
	*/
	static bool isNumerical(ColumnType type) {
		return (type >= COLUMN_TYPE_BYTE && type <= COLUMN_TYPE_DOUBLE);
	}
	/*!
		@brief Check if Column type is integer(byte, short, int, long)
	*/
	static bool isInteger(ColumnType type) {
		return (type >= COLUMN_TYPE_BYTE && type <= COLUMN_TYPE_LONG);
	}
	/*!
		@brief Check if Column type is floating-point(float, double)
	*/
	static bool isFloat(ColumnType type) {
		return (type >= COLUMN_TYPE_FLOAT && type <= COLUMN_TYPE_DOUBLE);
	}
	/*!
		@brief Check if Column type is array
	*/
	static bool isArray(ColumnType type) {
		return (type >= COLUMN_TYPE_STRING_ARRAY &&
				type <= COLUMN_TYPE_TIMESTAMP_ARRAY);
	}

	/*!
		@brief Check if Column type is nest format(blob, string array)
	*/
	static bool isNestStructure(ColumnType type) {
		return (type == COLUMN_TYPE_BLOB || type == COLUMN_TYPE_STRING_ARRAY);
	}
	/*!
		@brief Get element Column type from Array Column type
	*/
	static ColumnType getSimpleColumnType(ColumnType type) {
		if (isArray(type)) {
			return static_cast<ColumnType>(type - COLUMN_TYPE_OID - 1);
		}
		else {
			return type;
		}
	}
	/*!
		@brief Cast boolean value
	*/
	static bool getBool(ColumnType type, const void *field) {
		if (type == COLUMN_TYPE_BOOL) {
			return (*reinterpret_cast<const bool *>(field));
		}
		else {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
		}
	}
	/*!
		@brief Cast byte value
	*/
	static int8_t getByte(ColumnType type, const void *field) {
		switch (type) {
		case COLUMN_TYPE_BYTE:
			return static_cast<int8_t>(
				*reinterpret_cast<const int8_t *>(field));
		case COLUMN_TYPE_SHORT:
			return static_cast<int8_t>(
				*reinterpret_cast<const int16_t *>(field));
		case COLUMN_TYPE_INT:
			return static_cast<int8_t>(
				*reinterpret_cast<const int32_t *>(field));
		case COLUMN_TYPE_LONG:
			return static_cast<int8_t>(
				*reinterpret_cast<const int64_t *>(field));
		case COLUMN_TYPE_FLOAT:
			return static_cast<int8_t>(*reinterpret_cast<const float *>(field));
		case COLUMN_TYPE_DOUBLE:
			return static_cast<int8_t>(
				*reinterpret_cast<const double *>(field));
			break;
		default:
			break;
		}
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
	}
	/*!
		@brief Cast short value
	*/
	static int16_t getShort(ColumnType type, const void *field) {
		switch (type) {
		case COLUMN_TYPE_BYTE:
			return static_cast<int16_t>(
				*reinterpret_cast<const int8_t *>(field));
		case COLUMN_TYPE_SHORT:
			return static_cast<int16_t>(
				*reinterpret_cast<const int16_t *>(field));
		case COLUMN_TYPE_INT:
			return static_cast<int16_t>(
				*reinterpret_cast<const int32_t *>(field));
		case COLUMN_TYPE_LONG:
			return static_cast<int16_t>(
				*reinterpret_cast<const int64_t *>(field));
		case COLUMN_TYPE_FLOAT:
			return static_cast<int16_t>(
				*reinterpret_cast<const float *>(field));
		case COLUMN_TYPE_DOUBLE:
			return static_cast<int16_t>(
				*reinterpret_cast<const double *>(field));
			break;
		default:
			break;
		}
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
	}
	/*!
		@brief Cast int value
	*/
	static int32_t getInt(ColumnType type, const void *field) {
		switch (type) {
		case COLUMN_TYPE_BYTE:
			return static_cast<int32_t>(
				*reinterpret_cast<const int8_t *>(field));
		case COLUMN_TYPE_SHORT:
			return static_cast<int32_t>(
				*reinterpret_cast<const int16_t *>(field));
		case COLUMN_TYPE_INT:
			return static_cast<int32_t>(
				*reinterpret_cast<const int32_t *>(field));
		case COLUMN_TYPE_LONG:
			return static_cast<int32_t>(
				*reinterpret_cast<const int64_t *>(field));
		case COLUMN_TYPE_FLOAT:
			return static_cast<int32_t>(
				*reinterpret_cast<const float *>(field));
		case COLUMN_TYPE_DOUBLE:
			return static_cast<int32_t>(
				*reinterpret_cast<const double *>(field));
			break;
		default:
			break;
		}
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
	}
	/*!
		@brief Cast long value
	*/
	static int64_t getLong(ColumnType type, const void *field) {
		switch (type) {
		case COLUMN_TYPE_BYTE:
			return static_cast<int64_t>(
				*reinterpret_cast<const int8_t *>(field));
		case COLUMN_TYPE_SHORT:
			return static_cast<int64_t>(
				*reinterpret_cast<const int16_t *>(field));
		case COLUMN_TYPE_INT:
			return static_cast<int64_t>(
				*reinterpret_cast<const int32_t *>(field));
		case COLUMN_TYPE_LONG:
			return static_cast<int64_t>(
				*reinterpret_cast<const int64_t *>(field));
		case COLUMN_TYPE_FLOAT:
			return static_cast<int64_t>(
				*reinterpret_cast<const float *>(field));
		case COLUMN_TYPE_DOUBLE:
			return static_cast<int64_t>(
				*reinterpret_cast<const double *>(field));
			break;
		default:
			break;
		}
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
	}
	/*!
		@brief Cast float value
	*/
	static float getFloat(ColumnType type, const void *field) {
		switch (type) {
		case COLUMN_TYPE_BYTE:
			return static_cast<float>(*reinterpret_cast<const int8_t *>(field));
		case COLUMN_TYPE_SHORT:
			return static_cast<float>(
				*reinterpret_cast<const int16_t *>(field));
		case COLUMN_TYPE_INT:
			return static_cast<float>(
				*reinterpret_cast<const int32_t *>(field));
		case COLUMN_TYPE_LONG:
			return static_cast<float>(
				*reinterpret_cast<const int64_t *>(field));
		case COLUMN_TYPE_FLOAT:
			return static_cast<float>(*reinterpret_cast<const float *>(field));
		case COLUMN_TYPE_DOUBLE:
			return static_cast<float>(*reinterpret_cast<const double *>(field));
			break;
		default:
			break;
		}
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
	}
	/*!
		@brief Cast double value
	*/
	static double getDouble(ColumnType type, const void *field) {
		switch (type) {
		case COLUMN_TYPE_BYTE:
			return static_cast<double>(
				*reinterpret_cast<const int8_t *>(field));
		case COLUMN_TYPE_SHORT:
			return static_cast<double>(
				*reinterpret_cast<const int16_t *>(field));
		case COLUMN_TYPE_INT:
			return static_cast<double>(
				*reinterpret_cast<const int32_t *>(field));
		case COLUMN_TYPE_LONG:
			return static_cast<double>(
				*reinterpret_cast<const int64_t *>(field));
		case COLUMN_TYPE_FLOAT:
			return static_cast<double>(*reinterpret_cast<const float *>(field));
		case COLUMN_TYPE_DOUBLE:
			return static_cast<double>(
				*reinterpret_cast<const double *>(field));
			break;
		default:
			break;
		}
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
	}
	/*!
		@brief Cast Timestamp value
	*/
	static Timestamp getTimestamp(ColumnType type, const void *field) {
		if (type == COLUMN_TYPE_TIMESTAMP) {
			return (*reinterpret_cast<const Timestamp *>(field));
		}
		else {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
		}
	}

	/*!
		@brief Validatet Timestamp value
	*/
	static bool validateTimestamp(Timestamp val) {
		if ((val < 0) || (val > SUPPORT_MAX_TIMESTAMP)) {
			return false;
		}
		return true;
	}
	/*!
		@brief Convert to Upper case
	*/
	static void convertUpperCase(char8_t const *p, size_t size, char8_t *out) {
		char c;
		for (size_t i = 0; i < size; i++) {
			c = *(p + i);
			if ((c >= 'a') && (c <= 'z')) {
				*(out + i) = c - 32;
			}
			else {
				*(out + i) = c;
			}
		}
	}

	/*!
		@brief Get Typed-name
	*/
	static std::string getTypeName(ColumnType type);

	static const Timestamp
		SUPPORT_MAX_TIMESTAMP;  

	static std::string dumpMemory(
		const std::string &name, const uint8_t *addr, uint64_t size);
};

/*!
	@brief Represents the type of interpolation of Rows
*/
enum InterpolationMode {
	INTERP_MODE_LINEAR_OR_PREVIOUS = 0,
	INTERP_MODE_EMPTY = 1,
};

/*!
	@brief Conditions of Sampling search
*/
struct Sampling {
	uint32_t interval_;  
	TimeUnit timeUnit_;  
	std::vector<uint32_t>
		interpolatedColumnIdList_;  
	InterpolationMode mode_;		
};

/*!
	@brief Provides a cursor in Variable-type array value(in Row Message and Row
   Object)
*/
class VariableArrayCursor : public BaseObject {
public:  
public:  
public:  
	VariableArrayCursor(TransactionContext &txn, ObjectManager &objectManager,
		OId oId, AccessMode accessMode);
	VariableArrayCursor(uint8_t *addr);

	void finalize();

	/*!
		@brief Get current element
	*/
	uint8_t *getElement(uint32_t &elemSize, uint32_t &elemCount);

	bool nextElement(bool forRemove = false);

	OId getElementOId();

	/*!
		@brief Move to first position
	*/
	void reset() {
		if (getBaseOId() != rootOId_) {
			curObject_.load(rootOId_);
		}
		else {
			resetCursor();
		}
		curObject_.moveCursor(
			ValueProcessor::getEncodedVarSize(elemNum_));  
		elemCursor_ = UNDEF_CURSOR_POS;
	}

	OId clone(TransactionContext &txn, const AllocateStrategy &allocateStrategy,
		OId neighborOId);

	/*!
		@brief Get array length
	*/
	uint32_t getArrayLength() {
		return elemNum_;
	}

	void setArrayLength(uint32_t length);


	void getField(const ColumnInfo &columnInfo, BaseObject &baseObject);

	static uint32_t divisionThreshold(uint32_t size) {
		return size + (size >> 1);
	}

private:  
	struct VariableColumnInfo {
		OId oId_;  
		uint64_t pos_;  
		uint32_t len_;  
	};
	static const uint32_t UNDEF_CURSOR_POS = UINT32_MAX;

private:  
	BaseObject &curObject_;
	OId rootOId_;
	uint32_t elemNum_;
	uint32_t currentSize_;
	uint32_t
		elemCursor_;  
	AccessMode accessMode_;
private:			  
	uint8_t *data() {
		return curObject_.getBaseAddr();
	}
};

/*!
	@brief Provides a cursor in String-type value(in Row Message and Row Object)
*/
class StringCursor : public BaseObject {
public:  
public:  
public:  
	StringCursor(
		TransactionContext &txn, ObjectManager &objectManager, OId oId);

	StringCursor(uint8_t *binary);

	StringCursor(TransactionContext &txn, const uint8_t *str,
		uint32_t strLength);  

	StringCursor(TransactionContext &txn, const char *str);

	/*!
		@brief Free StringCursor Object
	*/
	void finalize() {
		BaseObject::finalize();
	}

	uint32_t getObjectSize();

	/*!
		@brief Get string length
	*/
	uint32_t stringLength() {
		return length_;
	}

	/*!
		@brief Get string pointer
	*/
	uint8_t *str() {
		return getCursor<uint8_t>();
	}

	/*!
		@brief Get data pointer
	*/
	uint8_t *data() {
		return getBaseAddr();
	}

private:  
	static const uint8_t ZERO_LENGTH_STR_BINARY_ = 0x01;

private:  
	uint32_t length_;
	uint8_t zeroLengthStr_;

private:  
};

#endif
