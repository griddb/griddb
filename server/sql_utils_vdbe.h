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

#ifndef SQL_UTILS_VDBE_H_
#define SQL_UTILS_VDBE_H_

#include "sql_value.h"

struct SQLVdbeUtils {
	typedef SQLValues::ValueContext ValueContext;
	typedef SQLValues::StringBuilder StringBuilder;
	typedef SQLValues::TypeUtils TypeUtils;
	typedef SQLValues::ValueUtils ValueUtils;

	struct VdbeUtils;
};

struct SQLVdbeUtils::VdbeUtils {
	struct Impl;

	enum {
		NUMERIC_BUFFER_SIZE = 128
	};

	static TupleValue sqlPrintf(
			ValueContext &cxt, const char8_t *format,
			const TupleValue *args, size_t argCount);

	static size_t numericToString(
			const TupleValue &value, char8_t *buf, size_t size);
	static StringBuilder& numericToString(
			StringBuilder &builder, const TupleValue &value);

	static TupleValue toString(ValueContext &cxt, const int64_t &value);
	static TupleValue toString(ValueContext &cxt, const double &value);

	static bool toLong(const char8_t *buffer, size_t length, int64_t &result);
	static bool toDouble(const char8_t *buffer, size_t length, double &result);

	static int strICmp(const char8_t *value1, const char8_t *value2);
	static bool matchGlob(const char8_t *str, const char8_t *pattern);

	static bool isSpaceChar(util::CodePoint c);
	static bool isSpaceChar(char8_t c);

	static void toLower(char8_t *buf, size_t size);
	static void toUpper(char8_t *buf, size_t size);

	static void generateRandom(void *buf, size_t size);
};

#endif
