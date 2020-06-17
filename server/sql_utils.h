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

#include "data_type.h"
#include "util/container.h"
#include "data_store_common.h"
#include "sql_tuple.h"

class SQLUtils {
public:
	static void getDatabaseVersion(int32_t &major, int32_t &minor);
	
	static int64_t convertToTime(util::String &timeStr);
	
	static bool checkNoSQLTypeToTupleType(ColumnType type);
	
	static ColumnType convertTupleTypeToNoSQLType(
			TupleList::TupleColumnType type);

	static util::String normalizeName(
			util::StackAllocator &alloc,
			const char8_t *src,
			bool isCaseSensitive = false);

	static void convertUpper(
			char8_t const *p, size_t size, char8_t *out);
};

#endif
