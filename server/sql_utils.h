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
#ifndef SQL_UTILS_H_
#define SQL_UTILS_H_

#include "util/container.h"
#include "data_store_common.h"
#include "sql_tuple.h"
#include "sql_tuple.h"
#include "gs_error.h"

static void convertUpper(char8_t const *p, size_t size, char8_t *out) {
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

static util::String normalizeName(
		util::StackAllocator &alloc, const char8_t *src) {
	util::XArray<char8_t> dest(alloc);
	dest.resize(strlen(src) + 1);
	convertUpper(
			src, static_cast<uint32_t>(dest.size()), &dest[0]);
	return util::String(&dest[0], alloc);
}

static bool checkAcceptableTupleType(TupleList::TupleColumnType type) {
	switch (type & ~TupleList::TYPE_MASK_NULLABLE) {
		case TupleList::TYPE_BOOL :
		case TupleList::TYPE_BYTE :
		case TupleList::TYPE_SHORT:
		case TupleList::TYPE_INTEGER :
		case TupleList::TYPE_LONG :
		case TupleList::TYPE_FLOAT :
		case TupleList::TYPE_NUMERIC :
		case TupleList::TYPE_DOUBLE :
		case TupleList::TYPE_TIMESTAMP :
		case TupleList::TYPE_NULL :
		case TupleList::TYPE_STRING :
		case TupleList::TYPE_BLOB :
		case TupleList::TYPE_ANY: 
			return true;
		default:
			return false;
	}
}

static ColumnType convertTupleTypeToNoSQLType(TupleList::TupleColumnType type) {
	switch (type & ~TupleList::TYPE_MASK_NULLABLE) {
	case TupleList::TYPE_BOOL: return COLUMN_TYPE_BOOL;
	case TupleList::TYPE_BYTE: return COLUMN_TYPE_BYTE;
	case TupleList::TYPE_SHORT: return COLUMN_TYPE_SHORT;
	case TupleList::TYPE_INTEGER: return COLUMN_TYPE_INT;
	case TupleList::TYPE_LONG: return COLUMN_TYPE_LONG;
	case TupleList::TYPE_FLOAT: return COLUMN_TYPE_FLOAT;
	case TupleList::TYPE_NUMERIC: return COLUMN_TYPE_DOUBLE;
	case TupleList::TYPE_DOUBLE: return COLUMN_TYPE_DOUBLE;
	case TupleList::TYPE_TIMESTAMP: return COLUMN_TYPE_TIMESTAMP;
	case TupleList::TYPE_NULL: return COLUMN_TYPE_NULL;
	case TupleList::TYPE_STRING: return COLUMN_TYPE_STRING;
	case TupleList::TYPE_GEOMETRY: return COLUMN_TYPE_GEOMETRY;
	case TupleList::TYPE_BLOB: return COLUMN_TYPE_BLOB;
	case TupleList::TYPE_ANY: return COLUMN_TYPE_ANY;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_NOSQL_INTERNAL,
				"Unsupported type, type=" << static_cast<int32_t>(type));
	}
}

static bool checkNoSQLTypeToTupleType(ColumnType type) {
	switch (type & ~TupleList::TYPE_MASK_NULLABLE) {
		case COLUMN_TYPE_BOOL :
		case COLUMN_TYPE_BYTE :
		case COLUMN_TYPE_SHORT:
		case COLUMN_TYPE_INT :
		case COLUMN_TYPE_LONG :
		case COLUMN_TYPE_FLOAT :
		case COLUMN_TYPE_DOUBLE :
		case COLUMN_TYPE_TIMESTAMP :
		case COLUMN_TYPE_NULL :
		case COLUMN_TYPE_STRING :
		case COLUMN_TYPE_BLOB :
			return true;
		case COLUMN_TYPE_GEOMETRY:
		case COLUMN_TYPE_STRING_ARRAY:
		case COLUMN_TYPE_BOOL_ARRAY:
		case COLUMN_TYPE_BYTE_ARRAY:
		case COLUMN_TYPE_SHORT_ARRAY:
		case COLUMN_TYPE_INT_ARRAY:
		case COLUMN_TYPE_LONG_ARRAY:
		case COLUMN_TYPE_FLOAT_ARRAY:
		case COLUMN_TYPE_DOUBLE_ARRAY:
		case COLUMN_TYPE_TIMESTAMP_ARRAY:
			return false;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_NOSQL_INTERNAL,
					"Unsupported type, type=" << static_cast<int32_t>(type));
	}
}

static TupleList::TupleColumnType
	convertNoSQLTypeToTupleType(ColumnType type) {
	switch (type) {
	case COLUMN_TYPE_BOOL : return TupleList::TYPE_BOOL;
	case COLUMN_TYPE_BYTE : return TupleList::TYPE_BYTE;
	case COLUMN_TYPE_SHORT: return TupleList::TYPE_SHORT;
	case COLUMN_TYPE_INT : return TupleList::TYPE_INTEGER;
	case COLUMN_TYPE_LONG : return TupleList::TYPE_LONG;
	case COLUMN_TYPE_FLOAT : return TupleList::TYPE_FLOAT;
	case COLUMN_TYPE_DOUBLE : return TupleList::TYPE_DOUBLE;
	case COLUMN_TYPE_TIMESTAMP : return TupleList::TYPE_TIMESTAMP;
	case COLUMN_TYPE_NULL : return TupleList::TYPE_NULL;
	case COLUMN_TYPE_STRING : return TupleList::TYPE_STRING;
	case COLUMN_TYPE_BLOB : return  TupleList::TYPE_BLOB;
	case COLUMN_TYPE_GEOMETRY:
	case COLUMN_TYPE_STRING_ARRAY:
	case COLUMN_TYPE_BOOL_ARRAY:
	case COLUMN_TYPE_BYTE_ARRAY:
	case COLUMN_TYPE_SHORT_ARRAY:
	case COLUMN_TYPE_INT_ARRAY:
	case COLUMN_TYPE_LONG_ARRAY:
	case COLUMN_TYPE_FLOAT_ARRAY:
	case COLUMN_TYPE_DOUBLE_ARRAY:
	case COLUMN_TYPE_TIMESTAMP_ARRAY:
		return TupleList::TYPE_ANY;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_NOSQL_INTERNAL,
				"Unsupported type, type=" << static_cast<int32_t>(type));
	}
}


#endif