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
	@brief Definition of ValueProcessor
*/
#ifndef VALUE_PROCESSOR_H_
#define VALUE_PROCESSOR_H_

#include "util/container.h"
#include "base_object.h"
#include "data_store_common.h"
#include "data_type.h"
#include "gs_error.h"
#include "object_manager_v4.h"
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
	template<typename T> class RawTimestampFormatter;

	static int32_t compare(TransactionContext &txn,
		ObjectManagerV4 &objectManager, AllocateStrategy &strategy, ColumnId columnId,
		MessageRowStore *messageRowStore, uint8_t *objectRowField);

	static int32_t compare(TransactionContext &txn,
		ObjectManagerV4 &objectManager, AllocateStrategy &strategy, ColumnType type,
		uint8_t *srcObjectRowField, uint8_t *targetObjectRowField);

	static void getField(TransactionContext &txn, ObjectManagerV4 &objectManager,
		AllocateStrategy& strategy, ColumnId columnId, const Value *objectValue,
		MessageRowStore *outputMessageRowStore);

	static void getField(TransactionContext &txn, ObjectManagerV4 &objectManager,
		AllocateStrategy& strategy, ColumnId columnId, uint32_t recordNth, Value *objectValue,
		MessageRowStore *outputMessageRowStore);  

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
		@brief Decode variable size (1 or 4byte)
	*/
	static inline uint32_t decodeVarSize(const void *ptr) {
		uint8_t val1byte = *static_cast<const uint8_t *>(ptr);
		if (varSizeIs1Byte(val1byte)) {
			return decode1ByteVarSize(val1byte);
		}
		else if (varSizeIs4Byte(val1byte)) {
			uint32_t val4byte = *static_cast<const uint32_t *>(ptr);
			return decode4ByteVarSize(val4byte);
		}
		else {
			assert(varSizeIs8Byte(val1byte));
			uint64_t val8byte = *static_cast<const uint64_t *>(ptr);
			if (val8byte > static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
				UTIL_THROW_ERROR(GS_ERROR_DS_OUT_OF_RANGE,
						"Decoded size = " << val8byte);
			}
			return static_cast<uint32_t>(decode8ByteVarSize(val8byte));
		}
	}

	/*!
		@brief Decode variable size (1 or 4 or 8byte)
	*/
	static inline uint64_t decodeVarSize64(const void *ptr) {
		bool isOId;
		return decodeVarSizeOrOId(ptr, isOId);
	}

	/*!
		@brief Decode variable size or OId (1 or 4 or 8byte)
	*/
	static inline uint64_t decodeVarSizeOrOId(const void *ptr, bool &isOId) {
		isOId = false;
		uint8_t val1byte = *static_cast<const uint8_t *>(ptr);
		if (varSizeIs1Byte(val1byte)) {
			return decode1ByteVarSize(val1byte);
		}
		else if (varSizeIs4Byte(val1byte)) {
			uint32_t val4byte = *static_cast<const uint32_t *>(ptr);
			return decode4ByteVarSize(val4byte);
		}
		else {
			assert(varSizeIs8Byte(val1byte));
			uint64_t val8byte = *static_cast<const uint64_t *>(ptr);
			isOId = true;
			return decode8ByteVarSize(val8byte);
		}
	}

	/*!
		@brief Get variable size
	*/
	static inline uint32_t getVarSize(util::ByteStream<util::ArrayInStream> &in) {
		uint64_t currentPos = in.base().position();
		uint8_t byteData;
		in >> byteData;
		if (ValueProcessor::varSizeIs1Byte(byteData)) {
			return ValueProcessor::decode1ByteVarSize(byteData);
		} else if (ValueProcessor::varSizeIs4Byte(byteData)) {
			in.base().position(static_cast<size_t>(currentPos));
			uint32_t rawData;
			in >> rawData;
			return ValueProcessor::decode4ByteVarSize(rawData);
		} else {
			in.base().position(static_cast<size_t>(currentPos));
			uint64_t rawData;
			in >> rawData;
			uint64_t decodedVal = ValueProcessor::decode8ByteVarSize(rawData);
			if (decodedVal > static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
				GS_THROW_USER_ERROR(GS_ERROR_DS_OUT_OF_RANGE,
						"Decoded size = " << decodedVal);
			}
			return static_cast<uint32_t>(decodedVal);
		}
	}

	/*!
		@brief get Encode Size(1 or 4 or 8 byte)
	*/
	static inline uint32_t getEncodedVarSize(const void *ptr) {
		uint8_t val = *static_cast<const uint8_t *>(ptr);
		if (varSizeIs1Byte(val)) {
			return 1;
		}
		else if (varSizeIs4Byte(val)) {
			return 4;
		}
		else {
			assert(varSizeIs8Byte(val));
			return 8;
		}
	}

	/*!
		@brief get Encode Size(1 or 4 or 8byte)
	*/
	static inline uint32_t getEncodedVarSize(uint64_t val) {
		if (val < VAR_SIZE_1BYTE_THRESHOLD) {
			return 1;
		}
		else if (val < VAR_SIZE_4BYTE_THRESHOLD) {
			return 4;
		}
		else {
			return 8;
		}
	}

	static inline uint64_t encodeVarSize(uint64_t val) {
		assert(val < 0x4000000000000000L);
		if (val < VAR_SIZE_1BYTE_THRESHOLD) {
			return encode1ByteVarSize(static_cast<uint8_t>(val));
		}
		else if (val < VAR_SIZE_4BYTE_THRESHOLD) {
			return encode4ByteVarSize(static_cast<uint32_t>(val));
		}
		else {
			return encode8ByteVarSize(val);
		}
	}

	/*!
		@brief get Encode Size
	*/
	static inline uint8_t encode1ByteVarSize(uint8_t val) {
		return static_cast<uint8_t>(((val << 1) | 0x01));
	}

	/*!
		@brief get Encode Size
	*/
	static inline uint32_t encode4ByteVarSize(uint32_t val) {
		return (val << 2);
	}

	/*!
		@brief get Encode Size
	*/
	static inline uint64_t encode8ByteVarSize(uint64_t val) {
		return (val << 2) | 0x02;
	}

	/*!
		@brief get Encode Size(for OId)
	*/
	static inline uint64_t encodeVarSizeOId(uint64_t val) {
		return encode8ByteVarSize(val);
	}

	/*!
		@brief Calculate Null byte size
	*/
	inline static uint32_t calcNullsByteSize(uint32_t columnNum) {
		return (columnNum + 7) / 8;
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
			case COLUMN_TYPE_MICRO_TIMESTAMP:
			case COLUMN_TYPE_NANO_TIMESTAMP:
				isValid = true;
				break;
			default:
				break;
			}
		}
		return isValid;
	}

	/*!
		@brief Check if Column type is simple (except variable type)
	*/
	static bool isSimple(ColumnType type) {
		return (type >= COLUMN_TYPE_SIMPLE_BEGIN1 &&
				type <= COLUMN_TYPE_SIMPLE_TAIL1) ||
				(type >= COLUMN_TYPE_SIMPLE_BEGIN2 &&
				type <= COLUMN_TYPE_SIMPLE_TAIL2);
	}
	/*!
		@brief Check if Column type is numerical
	*/
	static bool isNumerical(ColumnType type) {
		return (type >= COLUMN_TYPE_NUMERIC_BEGIN &&
				type <= COLUMN_TYPE_NUMERIC_TAIL);
	}
	/*!
		@brief Check if Column type is integer(byte, short, int, long)
	*/
	static bool isInteger(ColumnType type) {
		return (type >= COLUMN_TYPE_INTEGRAL_BEGIN &&
				type <= COLUMN_TYPE_INTEGRAL_TAIL);
	}
	/*!
		@brief Check if Column type is floating-point(float, double)
	*/
	static bool isFloat(ColumnType type) {
		return (type >= COLUMN_TYPE_FLOATING_BEGIN &&
				type <= COLUMN_TYPE_FLOATING_TAIL);
	}
	/*!
		@brief Check if Column type is array
	*/
	static bool isArray(ColumnType type) {
		return (type >= COLUMN_TYPE_ARRAY_BEGIN &&
				type <= COLUMN_TYPE_ARRAY_TAIL);
	}

	static bool isVariable(ColumnType type) {
		return (type <= COLUMN_TYPE_PRIMITIVE_TAIL1 &&
				!(type >= COLUMN_TYPE_SIMPLE_BEGIN1 &&
				type <= COLUMN_TYPE_SIMPLE_TAIL1)) || isArray(type);
	}

	/*!
		@brief Check if Column type is nest format(blob, string array)
	*/
	static bool isNestStructure(ColumnType type) {
		return (type == COLUMN_TYPE_BLOB || type == COLUMN_TYPE_STRING_ARRAY);
	}

	/*!
		@brief Check if Column type is timestamp family(milli, micro, nano)
	*/
	static bool isTimestampFamily(ColumnType type) {
		return (type == COLUMN_TYPE_TIMESTAMP ||
				type == COLUMN_TYPE_MICRO_TIMESTAMP ||
				type == COLUMN_TYPE_NANO_TIMESTAMP);
	}

	static SchemaFeatureLevel getSchemaFeatureLevel(ColumnType type) {
		if (type >= COLUMN_TYPE_PRIMITIVE_BEGIN2 &&
				type <= COLUMN_TYPE_TOTAL_TAIL) {
			return 2;
		}
		return 1;
	}

	/*!
		@brief Get element Column type from Array Column type
	*/
	static ColumnType getArrayElementType(ColumnType type) {
		if (isArray(type)) {
			return static_cast<ColumnType>(type - COLUMN_TYPE_ARRAY_BEGIN);
		}
		else {
			return type;
		}
	}

	static bool findArrayTypeByElement(
			ColumnType type, ColumnType &outType) {
		if (type > COLUMN_TYPE_SIMPLE_TAIL1) {
			outType = COLUMN_TYPE_ANY;
			return false;
		}
		else {
			outType = static_cast<ColumnType>(type + COLUMN_TYPE_ARRAY_BEGIN);
			return true;
		}
	}

	static int8_t getPrimitiveColumnTypeOrdinal(
			ColumnType type, bool withAny) {
		int8_t ordinal;
		if (!findPrimitiveColumnTypeOrdinal(type, withAny, ordinal)) {
			assert(false);
		}
		return ordinal;
	}

	static bool findPrimitiveColumnTypeOrdinal(
			ColumnType type, bool withAny, int8_t &ordinal) {
		if (type > COLUMN_TYPE_PRIMITIVE_TAIL1) {
			if (isArray(type)) {
				ordinal = static_cast<int8_t>(type - COLUMN_TYPE_ARRAY_BEGIN);
				return true;
			}
			else if (type < COLUMN_TYPE_PRIMITIVE_BEGIN2 ||
					type > COLUMN_TYPE_PRIMITIVE_TAIL2) {
				ordinal = static_cast<int8_t>(COLUMN_TYPE_ANY);
				if (withAny && type == COLUMN_TYPE_ANY) {
					return true;
				}
				return false;
			}
			ordinal = static_cast<int8_t>(type - COLUMN_TYPE_PRIMITIVE_BEGIN2 +
					COLUMN_TYPE_PRIMITIVE_TAIL1 + 1);
			return true;
		}
		ordinal = static_cast<int8_t>(type);
		return true;
	}

	static bool findColumnTypeByPrimitiveOrdinal(
			int8_t ordinal, bool forArray, bool withAny, ColumnType &outType) {

		ColumnType elemType;
		if (withAny && ordinal == static_cast<int8_t>(COLUMN_TYPE_ANY)) {
			elemType = COLUMN_TYPE_ANY;
		}
		else if (ordinal < 0 || static_cast<ColumnType>(ordinal) >=
				COLUMN_TYPE_PRIMITIVE_COUNT) {
			outType = COLUMN_TYPE_ANY;
			return false;
		}
		else if (static_cast<ColumnType>(ordinal) > COLUMN_TYPE_PRIMITIVE_TAIL1) {
			elemType = static_cast<ColumnType>(
					static_cast<ColumnType>(ordinal) -
					COLUMN_TYPE_PRIMITIVE_TAIL1 - 1 +
					COLUMN_TYPE_PRIMITIVE_BEGIN2);
		}
		else {
			elemType = static_cast<ColumnType>(ordinal);
		}

		if (forArray) {
			return findArrayTypeByElement(elemType, outType);
		}
		else {
			outType = elemType;
			return true;
		}
	}

	/*!
		@brief Cast boolean value
	*/
	static bool getBool(ColumnType type, const void *field) {
		if (type == COLUMN_TYPE_BOOL) {
			return (*static_cast<const bool *>(field));
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
				*static_cast<const int8_t *>(field));
		case COLUMN_TYPE_SHORT:
			return static_cast<int8_t>(
				*static_cast<const int16_t *>(field));
		case COLUMN_TYPE_INT:
			return static_cast<int8_t>(
				*static_cast<const int32_t *>(field));
		case COLUMN_TYPE_LONG:
			return static_cast<int8_t>(
				*static_cast<const int64_t *>(field));
		case COLUMN_TYPE_FLOAT:
			return static_cast<int8_t>(*static_cast<const float *>(field));
		case COLUMN_TYPE_DOUBLE:
			return static_cast<int8_t>(
				*static_cast<const double *>(field));
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
				*static_cast<const int8_t *>(field));
		case COLUMN_TYPE_SHORT:
			return static_cast<int16_t>(
				*static_cast<const int16_t *>(field));
		case COLUMN_TYPE_INT:
			return static_cast<int16_t>(
				*static_cast<const int32_t *>(field));
		case COLUMN_TYPE_LONG:
			return static_cast<int16_t>(
				*static_cast<const int64_t *>(field));
		case COLUMN_TYPE_FLOAT:
			return static_cast<int16_t>(
				*static_cast<const float *>(field));
		case COLUMN_TYPE_DOUBLE:
			return static_cast<int16_t>(
				*static_cast<const double *>(field));
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
				*static_cast<const int8_t *>(field));
		case COLUMN_TYPE_SHORT:
			return static_cast<int32_t>(
				*static_cast<const int16_t *>(field));
		case COLUMN_TYPE_INT:
			return static_cast<int32_t>(
				*static_cast<const int32_t *>(field));
		case COLUMN_TYPE_LONG:
			return static_cast<int32_t>(
				*static_cast<const int64_t *>(field));
		case COLUMN_TYPE_FLOAT:
			return static_cast<int32_t>(
				*static_cast<const float *>(field));
		case COLUMN_TYPE_DOUBLE:
			return static_cast<int32_t>(
				*static_cast<const double *>(field));
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
				*static_cast<const int8_t *>(field));
		case COLUMN_TYPE_SHORT:
			return static_cast<int64_t>(
				*static_cast<const int16_t *>(field));
		case COLUMN_TYPE_INT:
			return static_cast<int64_t>(
				*static_cast<const int32_t *>(field));
		case COLUMN_TYPE_LONG:
			return static_cast<int64_t>(
				*static_cast<const int64_t *>(field));
		case COLUMN_TYPE_FLOAT:
			return static_cast<int64_t>(
				*static_cast<const float *>(field));
		case COLUMN_TYPE_DOUBLE:
			return static_cast<int64_t>(
				*static_cast<const double *>(field));
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
			return static_cast<float>(*static_cast<const int8_t *>(field));
		case COLUMN_TYPE_SHORT:
			return static_cast<float>(
				*static_cast<const int16_t *>(field));
		case COLUMN_TYPE_INT:
			return static_cast<float>(
				*static_cast<const int32_t *>(field));
		case COLUMN_TYPE_LONG:
			return static_cast<float>(
				*static_cast<const int64_t *>(field));
		case COLUMN_TYPE_FLOAT:
			return static_cast<float>(*static_cast<const float *>(field));
		case COLUMN_TYPE_DOUBLE:
			return static_cast<float>(*static_cast<const double *>(field));
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
				*static_cast<const int8_t *>(field));
		case COLUMN_TYPE_SHORT:
			return static_cast<double>(
				*static_cast<const int16_t *>(field));
		case COLUMN_TYPE_INT:
			return static_cast<double>(
				*static_cast<const int32_t *>(field));
		case COLUMN_TYPE_LONG:
			return static_cast<double>(
				*static_cast<const int64_t *>(field));
		case COLUMN_TYPE_FLOAT:
			return static_cast<double>(*static_cast<const float *>(field));
		case COLUMN_TYPE_DOUBLE:
			return static_cast<double>(
				*static_cast<const double *>(field));
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
		switch (type) {
		case COLUMN_TYPE_TIMESTAMP:
			return *static_cast<const Timestamp*>(field);
		case COLUMN_TYPE_MICRO_TIMESTAMP:
			return getTimestamp(*static_cast<const MicroTimestamp*>(field));
		case COLUMN_TYPE_NANO_TIMESTAMP:
			return getTimestamp(*static_cast<const NanoTimestamp*>(field));
		default:
			return errorTimestampType();
		}
	}

	static Timestamp getTimestamp(const MicroTimestamp &src) {
		return src.value_ / 1000;
	}

	static Timestamp getTimestamp(const NanoTimestamp &src) {
		const int64_t unit = NanoTimestamp::HIGH_MICRO_UNIT;
		return src.getHigh() / (1000 * unit);
	}

	static Timestamp getTimestamp(const util::DateTime &src) {
		return src.getUnixTime();
	}

	static MicroTimestamp getMicroTimestamp(ColumnType type, const void *field) {
		switch (type) {
		case COLUMN_TYPE_TIMESTAMP:
			return getMicroTimestamp(*static_cast<const Timestamp*>(field));
		case COLUMN_TYPE_MICRO_TIMESTAMP:
			return*static_cast<const MicroTimestamp*>(field);
		case COLUMN_TYPE_NANO_TIMESTAMP:
			return getMicroTimestamp(*static_cast<const NanoTimestamp*>(field));
		default:
			return errorMicroTimestampType();
		}
	}

	static MicroTimestamp getMicroTimestamp(const Timestamp &src) {
		MicroTimestamp dest;
		dest.value_ = src * 1000;
		return dest;
	}

	static MicroTimestamp getMicroTimestamp(const NanoTimestamp &src) {
		const int64_t unit = NanoTimestamp::HIGH_MICRO_UNIT;
		MicroTimestamp dest;
		dest.value_ = src.getHigh() / unit;
		return dest;
	}

	static MicroTimestamp getMicroTimestamp(const util::PreciseDateTime &src) {
		MicroTimestamp dest;
		dest.value_ =
				src.getBase().getUnixTime() * 1000 +
				src.getNanoSeconds() / 1000;
		return dest;
	}

	static NanoTimestamp getNanoTimestamp(ColumnType type, const void *field) {
		switch (type) {
		case COLUMN_TYPE_TIMESTAMP:
			return getNanoTimestamp(*static_cast<const Timestamp*>(field));
		case COLUMN_TYPE_MICRO_TIMESTAMP:
			return getNanoTimestamp(*static_cast<const MicroTimestamp*>(field));
		case COLUMN_TYPE_NANO_TIMESTAMP:
			return *static_cast<const NanoTimestamp*>(field);
		default:
			return errorNanoTimestampType();
		}
	}

	static NanoTimestamp getNanoTimestamp(const Timestamp &src) {
		const int64_t unit = NanoTimestamp::HIGH_MICRO_UNIT;
		NanoTimestamp dest;
		dest.assign(src * (1000 * unit), 0);
		return dest;
	}

	static NanoTimestamp getNanoTimestamp(const MicroTimestamp &src) {
		const int64_t unit = NanoTimestamp::HIGH_MICRO_UNIT;
		NanoTimestamp dest;
		dest.assign(src.value_ * unit, 0);
		return dest;
	}

	static NanoTimestamp getNanoTimestamp(const util::PreciseDateTime &src) {
		const int64_t unit = NanoTimestamp::HIGH_MICRO_UNIT;
		const int64_t revUnit = 1000 / unit;
		const int64_t base = src.getBase().getUnixTime();
		const int64_t nanos = static_cast<int64_t>(src.getNanoSeconds());
		NanoTimestamp dest;
		dest.assign(
				base * (1000 * unit) + nanos / revUnit,
				static_cast<uint8_t>(nanos % revUnit));
		return dest;
	}

	static util::DateTime toDateTime(const Timestamp &src) {
		return util::DateTime(src);
	}

	static util::PreciseDateTime toDateTime(const MicroTimestamp &src) {
		return util::PreciseDateTime::ofNanoSeconds(
				util::DateTime(getTimestamp(src)),
				static_cast<uint32_t>(src.value_ % 1000) * 1000);
	}

	static util::PreciseDateTime toDateTime(const NanoTimestamp &src) {
		const int64_t unit = NanoTimestamp::HIGH_MICRO_UNIT;
		const int64_t revUnit = 1000 / unit;
		return util::PreciseDateTime::ofNanoSeconds(
				util::DateTime(getTimestamp(src)),
				static_cast<uint32_t>(
						src.getHigh() % (1000 * unit) * revUnit) +
				src.getLow());
	}

	static int32_t compareTimestamp(
			const Timestamp &ts1, const Timestamp &ts2) {
		if (ts1 != ts2) {
			return (ts1 < ts2 ? -1 : 1);
		}
		return 0;
	}

	static int32_t compareTimestamp(
			const MicroTimestamp &ts1, const MicroTimestamp &ts2) {
		return compareTimestamp(ts1.value_, ts2.value_);
	}

	static int32_t compareTimestamp(
			const NanoTimestamp &ts1, const NanoTimestamp &ts2) {
		if (ts1.getHigh() != ts2.getHigh()) {
			return (ts1.getHigh() < ts2.getHigh() ? -1 : 1);
		}
		if (ts1.getLow() != ts2.getLow()) {
			return (ts1.getLow() < ts2.getLow() ? -1 : 1);
		}
		return 0;
	}

	static RawTimestampFormatter<Timestamp> getRawTimestampFormatter(
			const Timestamp &ts);
	static RawTimestampFormatter<MicroTimestamp> getRawTimestampFormatter(
			const MicroTimestamp &ts);
	static RawTimestampFormatter<NanoTimestamp> getRawTimestampFormatter(
			const NanoTimestamp &ts);

	template<typename T>
	static util::DateTime::Formatter getTimestampFormatter(
			const T &ts, const util::DateTime::ZonedOption &option) {
		util::DateTime::Formatter formatter =
				toDateTime(ts).getFormatter(option);
		return formatter.withDefaultPrecision(getTimestampPrecision(ts));
	}

	static util::DateTime::FieldType getTimestampPrecision(
			const Timestamp&) {
		return util::DateTime::FIELD_MILLISECOND;
	}

	static util::DateTime::FieldType getTimestampPrecision(
			const MicroTimestamp&) {
		return util::DateTime::FIELD_MICROSECOND;
	}

	static util::DateTime::FieldType getTimestampPrecision(
			const NanoTimestamp&) {
		return util::DateTime::FIELD_NANOSECOND;
	}

	static void dumpRawTimestamp(std::ostream &os, const Timestamp &ts);
	static void dumpRawTimestamp(std::ostream &os, const MicroTimestamp &ts);
	static void dumpRawTimestamp(std::ostream &os, const NanoTimestamp &ts);

	/*!
		@brief Validate Timestamp value
	*/
	static bool validateTimestamp(const Timestamp &val) {
		if ((val < 0) || (val > SUPPORT_MAX_TIMESTAMP)) {
			return false;
		}
		return true;
	}

	static bool validateTimestamp(const MicroTimestamp &val) {
		if (val.value_ < 0 || compareTimestamp(
				val, SUPPORT_MAX_MICRO_TIMESTAMP) > 0) {
			return false;
		}
		return true;
	}

	static bool validateTimestamp(const NanoTimestamp &val) {
		if (val.getHigh() < 0 ||
				val.getLow() >= static_cast<uint32_t>(
						1000 / NanoTimestamp::HIGH_MICRO_UNIT) ||
				compareTimestamp(
						val, SUPPORT_MAX_NANO_TIMESTAMP) > 0) {
			return false;
		}
		return true;
	}

	static Timestamp makeMaxTimestamp(bool fractionTrimming);
	static MicroTimestamp makeMaxMicroTimestamp(bool fractionTrimming);
	static NanoTimestamp makeMaxNanoTimestamp(bool fractionTrimming);

	static Timestamp errorTimestampType();
	static MicroTimestamp errorMicroTimestampType();
	static NanoTimestamp errorNanoTimestampType();

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
		@brief Convert to Lower case
	*/
	static void convertLowerCase(char8_t const *p, size_t size, char8_t *out) {
		char c;
		for (size_t i = 0; i < size; i++) {
			c = *(p + i);
			if ((c >= 'A') && (c <= 'Z')) {
				*(out + i) = c + 32;
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
	static const char8_t* getTypeNameChars(
			ColumnType type, bool precisionIgnorable = false);

	static void dumpSimpleValue(util::NormalOStringStream &stream, 
		ColumnType columnType, const void *data, uint32_t size, bool withType = false);

	static int32_t getValuePrecision(ColumnType type);
	static int32_t getValueStringLength(ColumnType type);

	static const Timestamp
		SUPPORT_MAX_TIMESTAMP;  
	static const MicroTimestamp SUPPORT_MAX_MICRO_TIMESTAMP;
	static const NanoTimestamp SUPPORT_MAX_NANO_TIMESTAMP;

	static std::string dumpMemory(
		const std::string &name, const uint8_t *addr, uint64_t size);
};

template<typename T>
class ValueProcessor::RawTimestampFormatter {
public:
	explicit RawTimestampFormatter(const T &value) : value_(value) {
	}

	std::ostream& operator()(std::ostream &os) const {
		dumpRawTimestamp(os, value_);
		return os;
	}

private:
	T value_;
};

template<typename T>
inline std::ostream& operator<<(
		std::ostream &os,
		const ValueProcessor::RawTimestampFormatter<T> &formatter) {
	return formatter(os);
}

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
	VariableArrayCursor(ObjectManagerV4 &objectManager,
		AllocateStrategy &strategy, OId oId, AccessMode accessMode);
	VariableArrayCursor(uint8_t *addr);

	void finalize();

	/*!
		@brief Get current element
	*/
	uint8_t *getElement(uint32_t &elemSize, uint32_t &elemCount);

	bool nextElement(bool forRemove = false);
	bool moveElement(uint32_t pos);

	OId getElementOId();

	/*!
		@brief Move to first position
	*/
	void reset() {
		if (getBaseOId() != rootOId_) {
			curObject_.load(rootOId_, false);
		}
		else {
			resetCursor();
		}
		curObject_.moveCursor(
			ValueProcessor::getEncodedVarSize(elemNum_));  
		elemCursor_ = UNDEF_CURSOR_POS;
	}

	OId clone(AllocateStrategy &allocateStrategy,
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

	static void checkVarDataSize(TransactionContext &txn,
		ObjectManagerV4 &objectManager,
		const util::XArray< std::pair<uint8_t *, uint32_t> > &varList,
		const util::XArray<ColumnType> &columnTypeList,
		bool isConvertSpecialType,
		util::XArray<uint32_t> &varDataObjectSizeList,
		util::XArray<uint32_t> &varDataObjectPosList);

	static OId createVariableArrayCursor(TransactionContext &txn,
		ObjectManagerV4 &objectManager,
		AllocateStrategy &allocateStrategy,
		const util::XArray< std::pair<uint8_t *, uint32_t> > &varList,
		const util::XArray<ColumnType> &columnTypeList,
		bool isConvertSpecialType,
		const util::XArray<uint32_t> &varDataObjectSizeList,
		const util::XArray<uint32_t> &varDataObjectPosList,
		const util::XArray<OId> &oldVarDataOIdList,
		OId neighborOId);

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
		ObjectManagerV4 &objectManager, AllocateStrategy &strategy, OId oId);

	StringCursor(uint8_t *binary);

	StringCursor(util::StackAllocator& alloc, const uint8_t *str,
		uint32_t strLength);  

	StringCursor(util::StackAllocator& alloc, const char *str);

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


/*!
	@brief Object for blob
*/
class BinaryObject : public BaseObject {
public:
	BinaryObject(ObjectManagerV4 &objectManager, AllocateStrategy &strategy)
		: BaseObject(objectManager, strategy) {}
	BinaryObject(ObjectManagerV4 &objectManager, AllocateStrategy& strategy, OId oId)
		: BaseObject(objectManager, strategy, oId) {}
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
		uint64_t encodedSize = ValueProcessor::encodeVarSize(size);
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
		@note return head of blob-binary
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
	ArrayObject(ObjectManagerV4 &objectManager, AllocateStrategy& strategy)
		: BaseObject(objectManager, strategy) {}
	ArrayObject(ObjectManagerV4 &objectManager, AllocateStrategy& strategy, OId oId)
		: BaseObject(objectManager, strategy, oId) {}
	ArrayObject(uint8_t *addr) : BaseObject(addr) {}

	/*!
		@brief Get size of data
		@note contain variable size area
	*/
	static uint32_t getObjectSize(uint32_t arrayLength, uint16_t elemSize) {
		assert(elemSize > 0);
		uint32_t arrayLengthLen = ValueProcessor::getEncodedVarSize(
			arrayLength);  
		uint32_t totalSize =
			arrayLengthLen +
			arrayLength * elemSize;  
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
	const uint8_t *getArrayElement(uint32_t arrayIndex, uint16_t elemSize) const {
		assert(elemSize > 0);
		assert(arrayIndex < getArrayLength());
		uint32_t totalSizeLen =
			ValueProcessor::getEncodedVarSize(getBaseAddr());  
		uint32_t elemCountLen = ValueProcessor::getEncodedVarSize(
			getBaseAddr() + totalSizeLen);  
		return reinterpret_cast<const uint8_t *>(
			getBaseAddr() + totalSizeLen + elemCountLen +
			(arrayIndex * elemSize));
	}

	/*!
		@brief Set array length
	*/
	void setArrayLength(uint32_t arrayLength, uint16_t elemSize) {
		assert(elemSize > 0);
		uint32_t arrayLengthLen =
			ValueProcessor::getEncodedVarSize(arrayLength);
		uint32_t totalSize =
			arrayLengthLen + arrayLength * elemSize;

		uint64_t encodedTotalSize =
			ValueProcessor::encodeVarSize(totalSize);  
		uint32_t totalSizeLen = ValueProcessor::getEncodedVarSize(totalSize);
		uint64_t encodedArrayLength =
			ValueProcessor::encodeVarSize(arrayLength);  
		memcpy(getBaseAddr(), &encodedTotalSize, totalSizeLen);
		memcpy(
			getBaseAddr() + totalSizeLen, &encodedArrayLength, arrayLengthLen);
	}

	/*!
		@brief Set element of array
	*/
	void setArrayElement(
		uint32_t arrayIndex, uint16_t elemSize, const uint8_t *data) {
		assert(elemSize > 0);
		assert(arrayIndex < getArrayLength());
		memcpy(getArrayElementForUpdate(arrayIndex, elemSize), data,
			elemSize);
	}
	/*!
		@brief Get value pointer
		@note all return head of data
	*/
	uint8_t *getImage() {
		return getBaseAddr();
	}
private:
	uint8_t *getArrayElementForUpdate(uint32_t arrayIndex, uint16_t elemSize) {
		assert(elemSize > 0);
		assert(arrayIndex < getArrayLength());
		uint32_t totalSizeLen =
			ValueProcessor::getEncodedVarSize(getBaseAddr());  
		uint32_t elemCountLen = ValueProcessor::getEncodedVarSize(
			getBaseAddr() + totalSizeLen);  
		return reinterpret_cast<uint8_t *>(
			getBaseAddr() + totalSizeLen + elemCountLen +
			(arrayIndex * elemSize));
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
		uint64_t encodedArrayLength = ValueProcessor::encodeVarSize(
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
		uint64_t encodedElemNum =
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

class LogDevide {
public:
	static const int32_t MAX_DIVIDED_NUM = 3;
	LogDevide(ObjectManagerV4 &objectManager) 
		: objectManager_(objectManager), constElemNum_(0), dividedElemNum_(0),
		sizeList_{ 0, 0, 0 },
		blobSubBlockUnitSize_(objectManager.getRecommendedLimitObjectSize()) {
	}
	void initialize(uint64_t inputSize);
	uint32_t getElemNum() {
		return static_cast<uint32_t>(constElemNum_ + dividedElemNum_);
	}
	uint32_t getAllocateSize(int32_t currentElemNum) {
		if (currentElemNum < static_cast<int32_t>(constElemNum_)) {
			return blobSubBlockUnitSize_;
		} else {
			return sizeList_[currentElemNum - constElemNum_];
		}
	}


private:
	static const double EFFICIENCY_THRESHOLD;
	static const uint32_t	DIVIDED_SIZE_LIMIT = ((1 << 7) - ObjectManagerV4::OBJECT_HEADER_SIZE); 

	ObjectManagerV4 &objectManager_;
	uint32_t constElemNum_;
	uint32_t dividedElemNum_;
	uint32_t sizeList_[MAX_DIVIDED_NUM];
	const uint32_t blobSubBlockUnitSize_;

	uint32_t calcSizeOfBuddy(uint32_t size) {
		uint32_t buddySize = objectManager_.estimateAllocateSize(size);
		return buddySize + ObjectManagerV4::OBJECT_HEADER_SIZE;
	}
	void initializeDevide(uint32_t restSize);
};

class BlobCursor {
public:
	enum CURSOR_MODE {
		READ,
		REMOVE,
		CREATE
	};
	static uint64_t getTotalSize(const uint8_t *addr);
	static uint32_t getPrefixDataSize(ObjectManagerV4 &objectManager, uint64_t totalSize);
public:
	BlobCursor(ObjectManagerV4 &objectManager, AllocateStrategy &allocateStrategy, const uint8_t * const ptr);
	BlobCursor(ObjectManagerV4 &objectManager, AllocateStrategy &allocateStrategy, const uint8_t *ptr, OId neighborOId);
	~BlobCursor() {}
	uint32_t initialize(uint8_t *destAddr,  uint64_t totalSize);
	void finalize();
	uint64_t getTotalSize() const;
	bool next(CURSOR_MODE mode = READ);
	bool hasNext();
	void reset();  
	void getCurrentBinary(const uint8_t *&ptr, uint32_t &size);
	void setBinary(const uint8_t *addr, uint64_t size);
	void addBinary(const uint8_t *addr, uint32_t size);
	void dump(util::NormalOStringStream &ss, bool forExport = false);
	uint8_t *getBinary(util::StackAllocator &alloc);
private:
	struct BlobArrayElement {
		BlobArrayElement(uint64_t size, OId oId) : size_(size), oId_(oId) {
		}
		uint64_t size_;
		OId oId_;
	};
	class BlobArrayObject : public ArrayObject {
	public:
		BlobArrayObject() : ArrayObject(NULL), curPos_(-1) {
		}
		void load(OId oId) {
			BaseObject::load(oId, false);
			resetArrayCursor();
		}
		inline void loadNeighbor(OId oId, AccessMode mode) {
			BaseObject::loadNeighbor(oId, mode);
			resetArrayCursor();
		}
		template <class T>
		T *allocate(DSObjectSize requestSize,
			OId &oId, ObjectType objectType) {
			T *addr = BaseObject::allocate<T>(requestSize,
					oId, objectType);
			resetArrayCursor();
			return addr;
		}
		template <class T>
		T *allocateNeighbor(DSObjectSize requestSize,
			OId &oId, OId neighborOId,
			ObjectType objectType) {
			T *addr = BaseObject::allocateNeighbor<T>(requestSize,
					oId, neighborOId, objectType);
			resetArrayCursor();
			return addr;
		}
		static uint32_t getObjectSize(uint32_t arrayLength) {
			return ArrayObject::getObjectSize(arrayLength, sizeof(BlobArrayElement));
		}
		void setArrayLength(uint32_t arrayLength) {
			ArrayObject::setArrayLength(arrayLength, sizeof(BlobArrayElement));
		}
		const BlobArrayElement *getCurrentElement() const {
			return reinterpret_cast<const BlobArrayElement *>(
				ArrayObject::getArrayElement(curPos_, sizeof(BlobArrayElement)));
		}
		void setCurrentElement(const BlobArrayElement *data) {
			ArrayObject::setArrayElement(curPos_, sizeof(BlobArrayElement), 
				reinterpret_cast<const uint8_t *>(data));
		}
		bool next() {
			uint32_t length = getArrayLength();
			if (length != 0  && curPos_ + 1 < static_cast<int32_t>(length)) {
				curPos_++;
				return true;
			} else {
				return false;
			}
		}

		void resetArrayCursor() {
			curPos_ = -1;
		}
	private:
		int32_t curPos_;
	};

	uint32_t calcArrayNum(int32_t currentElem, size_t depth) {
		uint32_t lowerArrayNum = 0, lowerArrayPos = 0;
		uint32_t upperArrayNum = maxElem_, upperArrayPos = currentElem;
		size_t currentDepth = maxDepth_;
		uint32_t maxArrayNum = getMaxArrayNum(objectManager_);
		while (currentDepth-- != depth) {
			lowerArrayNum = upperArrayNum;
			lowerArrayPos = upperArrayPos;
			upperArrayNum = static_cast<uint32_t>(ceil(static_cast<double>(upperArrayNum) / maxArrayNum));
			upperArrayPos = upperArrayPos / maxArrayNum;
		}

		uint32_t arrayNum = maxArrayNum;
		if (upperArrayPos == upperArrayNum - 1) {
			arrayNum = lowerArrayNum % maxArrayNum;
			if (arrayNum == 0) {
				arrayNum = maxArrayNum;	
			}
		}
		UNUSED_VARIABLE(lowerArrayPos);
		return arrayNum;
	}
	static bool isDivided(uint32_t depth) {
		return depth != 0;
	}
	bool isDivided() {
		return isDivided(maxDepth_);
	}
	static uint32_t calcDepth(ObjectManagerV4 &objectManager, uint64_t totalSize, uint32_t elemNum, uint32_t &topArrayNum);
	static uint32_t getMaxArrayNum(ObjectManagerV4 &objectManager);
	void nextBlock(CURSOR_MODE mode);
	void down(CURSOR_MODE mode);
private:
	static const uint32_t MAX_DEPTH = 4;  
	static const uint32_t MIN_DIVIDED_SIZE = 119;
	ObjectManagerV4 &objectManager_;
	AllocateStrategy &allocateStrategy_;
	const uint8_t *baseAddr_;
	const uint8_t *topArrayAddr_;
	BaseObject curObj_;
	BlobArrayObject *arrayCursor_;
	BlobArrayObject stackCursor_[MAX_DEPTH];
	int32_t currentElem_;
	int32_t maxElem_;
	uint32_t currentDepth_;
	uint32_t maxDepth_;
	LogDevide logDevide_;
	OId neighborOId_;
};


#endif
