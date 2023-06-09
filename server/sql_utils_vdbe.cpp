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

#include "sql_utils_vdbe.h"
#include "util/numeric.h"

#include "c_utility_cpp.h"

#include "sql_rewrite.h"
#include "newsql_interface.h"

namespace {
struct ConstantsChecker {
	ConstantsChecker() {
		UTIL_STATIC_ASSERT(GS_TYPE_STRING_NULL == GS_TYPE_TIMESTAMP_ARRAY + 1);
	}
};
}

extern "C" {
#include "sqliteInt.h"
#include "vdbeInt.h"
}

struct VdbeInitializer {
private:
	static VdbeInitializer INSTANCE;
	VdbeInitializer();
};

VdbeInitializer VdbeInitializer::INSTANCE;

VdbeInitializer::VdbeInitializer() {
	int rc;
	static_cast<void>(rc);

	static CUtilRawVariableSizeAllocator varAlloc(util::AllocatorInfo(
			ALLOCATOR_GROUP_SQL_WORK, "vdbeLocal"));
	rc = sqlite3gs_allocator_set(
			NULL, CUtilVariableSizeAllocatorWrapper::wrap(&varAlloc));
	assert(rc == SQLITE_OK);

	sqlite3gsMemSet();

	rc = sqlite3_initialize();
	assert(rc == SQLITE_OK);
}

struct SQLVdbeUtils::VdbeUtils::Impl {
	static const TupleString::BufferInfo CONSTANT_P_INF_STR;
	static const TupleString::BufferInfo CONSTANT_P_INFINITY_STR;
	static const TupleString::BufferInfo CONSTANT_N_INF_STR;
	static const TupleString::BufferInfo CONSTANT_N_INFINITY_STR;
	static const TupleString::BufferInfo CONSTANT_NAN_STR;

	static size_t numericToStringInternal(
			const TupleValue &value, char8_t *buf, size_t size);
	static StringBuilder& numericToStringInternal(
			StringBuilder &builder, const TupleValue &value);

	static bool specialNumericToString(
			double value, char8_t *buf, size_t size, size_t &retSize);
	static const char8_t* specialNumericToString(double value);

	static bool stringToSpecialNumeric(
			const char8_t *buffer, size_t length, double &result);

	static bool fitToLexicalFloatingBoundary(
			const char8_t *buffer, size_t length, double &rawValue);
	static bool checkLexicalFloatingBoundary(
			const char8_t *buffer, size_t length, double rawValue,
			bool &infinite, bool &nonCanonical);

	static int32_t compareLexicalFloatingDigits(
			std::pair<const char8_t*, const char8_t*> &digits1,
			std::pair<const char8_t*, const char8_t*> &digits2);
	static bool parseFloatingComponents(
			const char8_t *buffer, size_t length,
			std::pair<const char8_t*, const char8_t*> &digits,
			std::pair<const char8_t*, const char8_t*> &exponent,
			int32_t &digitSign, int32_t &exponentSign);

	static std::pair<const char8_t*, const char8_t*> tokenizeFloatingDigits(
			const char8_t *&it, const char8_t *end, bool withPoint);
	static bool tokenizeExponentSeparator(
			const char8_t *&it, const char8_t *end);

	static int32_t tokenizeNumericSign(
			const char8_t *&it, const char8_t *end);
	static std::pair<const char8_t*, const char8_t*> tokenizeSpace(
			const char8_t *&it, const char8_t *end);
	static void trimSpace(const char8_t *&begin, const char8_t *&end);
	static bool isEmptyToken(
			const std::pair<const char8_t*, const char8_t*> &token);

	static void setUpAllocator(ValueContext &cxt);
};

const TupleString::BufferInfo
SQLVdbeUtils::VdbeUtils::Impl::CONSTANT_P_INF_STR =
		ValueUtils::toStringBuffer("Inf");

const TupleString::BufferInfo
SQLVdbeUtils::VdbeUtils::Impl::CONSTANT_P_INFINITY_STR =
		ValueUtils::toStringBuffer("Infinity");

const TupleString::BufferInfo
SQLVdbeUtils::VdbeUtils::Impl::CONSTANT_N_INF_STR =
		ValueUtils::toStringBuffer("-Inf");

const TupleString::BufferInfo
SQLVdbeUtils::VdbeUtils::Impl::CONSTANT_N_INFINITY_STR =
		ValueUtils::toStringBuffer("-Infinity");

const TupleString::BufferInfo
SQLVdbeUtils::VdbeUtils::Impl::CONSTANT_NAN_STR =
		ValueUtils::toStringBuffer("NaN");

size_t SQLVdbeUtils::VdbeUtils::Impl::numericToStringInternal(
		const TupleValue &value, char8_t *buf, size_t size) {
	typedef int64_t Alignment;
	uint8_t argValueStorage[sizeof(sqlite3_value) + sizeof(Alignment)];

	sqlite3_value &argValue = *reinterpret_cast<sqlite3_value*>(
			reinterpret_cast<uintptr_t>(
					argValueStorage + sizeof(Alignment) - 1) /
					sizeof(Alignment) * sizeof(Alignment));

	assert(argValueStorage <= reinterpret_cast<uint8_t*>(&argValue));
	assert(reinterpret_cast<uint8_t*>(&argValue) <
			argValueStorage + sizeof(Alignment));

	memset(&argValue, 0, sizeof(argValue));
	sqlite3VdbeMemInit(&argValue, NULL, MEM_Null);

	const char8_t *format;
	switch (value.getType()) {
	case TupleTypes::TYPE_LONG:
		sqlite3VdbeMemSetInt64(&argValue, value.get<int64_t>());
		format = "%lld";
		break;
	case TupleTypes::TYPE_DOUBLE:
		{
			const double doubleValue = value.get<double>();
			size_t retSize;
			if (specialNumericToString(doubleValue, buf, size, retSize)) {
				return retSize;
			}

			sqlite3VdbeMemSetDouble(&argValue, doubleValue);
			format = "%!.15g";
		}
		break;
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, "");
	}

	sqlite3_value *argList[] = { &argValue };

	PrintfArguments printfArgs;
	printfArgs.nArg = static_cast<int>(sizeof(argList) / sizeof(*argList));
	printfArgs.nUsed = 0;
	printfArgs.apArg = argList;

	StrAccum str;
	const int bufSize = static_cast<int>(std::min<size_t>(
			size, static_cast<size_t>(SQLITE_MAX_LENGTH)));
	sqlite3StrAccumInit(&str, buf, bufSize, bufSize);
	str.useMalloc = 0;

	sqlite3XPrintf(&str, SQLITE_PRINTF_SQLFUNC, format, &printfArgs);

	if (str.accError == 0 && str.zText != NULL) {
		return static_cast<size_t>(str.nChar);
	}

	if (str.accError == STRACCUM_TOOBIG &&
			size < static_cast<size_t>(SQLITE_MAX_LENGTH)) {
		return 0;
	}

	GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL,
			"Unexpected printf result (code=" << str.accError << ")");
}

SQLValues::StringBuilder&
SQLVdbeUtils::VdbeUtils::Impl::numericToStringInternal(
		StringBuilder &builder, const TupleValue &value) {

	const size_t offset = builder.size();

	assert(offset <= builder.capacity() && builder.capacity() > 0);
	builder.resize(builder.capacity());

	size_t size;
	while ((size = numericToStringInternal(
			value, builder.data() + offset, builder.size())) == 0) {
		builder.resize(offset + (builder.capacity() - offset) * 2);
	}

	builder.resize(offset + size);
	return builder;
}

bool SQLVdbeUtils::VdbeUtils::Impl::specialNumericToString(
		double value, char8_t *buf, size_t size, size_t &retSize) {

	const char8_t *str = specialNumericToString(value);
	if (str == NULL) {
		return false;
	}

	retSize = strlen(str);
	if (size < retSize) {
		retSize = 0;
	}
	else {
		memcpy(buf, str, retSize);
	}

	return true;
}

const char8_t* SQLVdbeUtils::VdbeUtils::Impl::specialNumericToString(
		double value) {
	if (!ValueUtils::CONFIG_NAN_AS_NULL && util::isNaN(value)) {
		return CONSTANT_NAN_STR.first;
	}
	else if (value == std::numeric_limits<double>::infinity()) {
		return CONSTANT_P_INFINITY_STR.first;
	}
	else if (value == -std::numeric_limits<double>::infinity()) {
		return CONSTANT_N_INFINITY_STR.first;
	}
	else {
		return NULL;
	}
}

bool SQLVdbeUtils::VdbeUtils::Impl::stringToSpecialNumeric(
		const char8_t *buffer, size_t length, double &result) {

	std::pair<const char8_t*, const char8_t*> trimmed(buffer, buffer + length);
	trimSpace(trimmed.first, trimmed.second);

	const int32_t sign = tokenizeNumericSign(trimmed.first, trimmed.second);

	const TupleString::BufferInfo bufInfo(
			trimmed.first, static_cast<size_t>(trimmed.second - trimmed.first));

	if (!ValueUtils::CONFIG_NAN_AS_NULL &&
			ValueUtils::orderPartNoCase(bufInfo, CONSTANT_NAN_STR) == 0) {
		result = std::numeric_limits<double>::quiet_NaN();
		return true;
	}
	else if (ValueUtils::orderPartNoCase(bufInfo, CONSTANT_P_INF_STR) == 0 ||
			ValueUtils::orderPartNoCase(
					bufInfo, CONSTANT_P_INFINITY_STR) == 0) {
		if (sign >= 0) {
			result = std::numeric_limits<double>::infinity();
		}
		else {
			result = -std::numeric_limits<double>::infinity();
		}
		return true;
	}

	return false;
}

bool SQLVdbeUtils::VdbeUtils::Impl::fitToLexicalFloatingBoundary(
		const char8_t *buffer, size_t length, double &rawValue) {
	if (!util::isInf(rawValue)) {
		return true;
	}

	bool infinite;
	bool nonCanonical;
	if (!checkLexicalFloatingBoundary(
			buffer, length, rawValue, infinite, nonCanonical)) {
		return false;
	}

	if (infinite) {
		return false;
	}

	if (rawValue < 0) {
		rawValue = -std::numeric_limits<double>::max();
	}
	else {
		rawValue = std::numeric_limits<double>::max();
	}

	return true;
}

bool SQLVdbeUtils::VdbeUtils::Impl::checkLexicalFloatingBoundary(
		const char8_t *buffer, size_t length, double rawValue,
		bool &infinite, bool &nonCanonical) {
	infinite = false;
	nonCanonical = false;

	if (!util::isInf(rawValue)) {
		return true;
	}

	std::pair<const char8_t*, const char8_t*> digits;
	std::pair<const char8_t*, const char8_t*> exponent;
	int32_t digitSign;
	int32_t exponentSign;
	if (!parseFloatingComponents(
			buffer, length, digits, exponent, digitSign, exponentSign)) {
		return false;
	}

	char8_t boundBuf[std::numeric_limits<double>::digits10 * 2];

	std::pair<const char8_t*, const char8_t*> boundDigits;
	std::pair<const char8_t*, const char8_t*> boundExponent;
	int32_t boundDigitSign;
	int32_t boundExponentSign;
	{
		const double boundValue = (rawValue < 0 ?
				-std::numeric_limits<double>::max() :
				std::numeric_limits<double>::max());
		const size_t boundBufSize = numericToStringInternal(
				TupleValue(boundValue), boundBuf, sizeof(boundBuf));
		if (boundBufSize == 0) {
			assert(false);
			return false;
		}
		if (!parseFloatingComponents(
				boundBuf, boundBufSize, boundDigits, boundExponent,
				boundDigitSign, boundExponentSign)) {
			return false;
		}
	}

	if ((digitSign >= 0) != (boundDigitSign >= 0) ||
			(exponentSign >= 0) != (boundExponentSign >= 0) ||
			compareLexicalFloatingDigits(exponent, boundExponent) != 0) {
		nonCanonical = true;
		return false;
	}

	if (compareLexicalFloatingDigits(digits, boundDigits) > 0) {
		infinite = true;
	}

	return true;
}

int32_t SQLVdbeUtils::VdbeUtils::Impl::compareLexicalFloatingDigits(
		std::pair<const char8_t*, const char8_t*> &digits1,
		std::pair<const char8_t*, const char8_t*> &digits2) {
	const char8_t *it1 = digits1.first;
	const char8_t *it2 = digits2.first;
	for (;; ++it1, ++it2) {
		const bool inRange1 = (it1 < digits1.second);
		const bool inRange2 = (it2 < digits2.second);
		if (!inRange1 && !inRange2) {
			return 0;
		}

		const char8_t elem1 = (inRange1 ? *it1 : '0');
		const char8_t elem2 = (inRange2 ? *it2 : '0');

		const int32_t value1 = (sqlite3Isdigit(elem1) ? elem1 - '0' : -1);
		const int32_t value2 = (sqlite3Isdigit(elem2) ? elem2 - '0' : -1);

		const int32_t diff = value1 - value2;
		if (diff != 0) {
			return (diff > 0 ? 1 : -1);
		}
	}
}

bool SQLVdbeUtils::VdbeUtils::Impl::parseFloatingComponents(
		const char8_t *buffer, size_t length,
		std::pair<const char8_t*, const char8_t*> &digits,
		std::pair<const char8_t*, const char8_t*> &exponent,
		int32_t &digitSign, int32_t &exponentSign) {
	do {
		const char8_t *it = buffer;
		const char8_t *end = it + length;

		tokenizeSpace(it, end);

		{
			digitSign = tokenizeNumericSign(it, end);

			const bool withPoint = true;
			digits = tokenizeFloatingDigits(it, end, withPoint);
			if (isEmptyToken(digits)) {
				break;
			}
		}

		if (tokenizeExponentSeparator(it, end)) {
			exponentSign = tokenizeNumericSign(it, end);

			const bool withPoint = false;
			exponent = tokenizeFloatingDigits(it, end, withPoint);
			if (isEmptyToken(exponent)) {
				break;
			}
		}

		tokenizeSpace(it, end);

		if (it != end) {
			break;
		}

		return true;
	}
	while (false);

	digits = std::pair<const char8_t*, const char8_t*>();
	exponent = std::pair<const char8_t*, const char8_t*>();
	digitSign = 0;
	exponentSign = 0;
	return false;
}

std::pair<const char8_t*, const char8_t*>
SQLVdbeUtils::VdbeUtils::Impl::tokenizeFloatingDigits(
		const char8_t *&it, const char8_t *end, bool withPoint) {
	const char8_t *top = it;
	bool nonZeroFound = false;
	for (; it != end; ++it) {
		if (!(sqlite3Isdigit(*it) || (withPoint && *it == '.'))) {
			break;
		}
		if (!nonZeroFound) {
			if (*it == '0') {
				const char8_t *next = it + 1;
				if (next != end && sqlite3Isdigit(*next)) {
					++top;
				}
			}
			else {
				nonZeroFound = true;
			}
		}
	}
	return std::make_pair(top, it);
}

bool SQLVdbeUtils::VdbeUtils::Impl::tokenizeExponentSeparator(
		const char8_t *&it, const char8_t *end) {
	if (it != end && (*it == 'e' || *it == 'E')) {
		++it;
		return true;
	}
	return false;
}

int32_t SQLVdbeUtils::VdbeUtils::Impl::tokenizeNumericSign(
		const char8_t *&it, const char8_t *end) {
	if (it != end && (*it == '+' || *it == '-')) {
		const char8_t sign = (*it == '+' ? 1 : -1);
		++it;
		return sign;
	}
	return 0;
}

std::pair<const char8_t*, const char8_t*>
SQLVdbeUtils::VdbeUtils::Impl::tokenizeSpace(
		const char8_t *&it, const char8_t *end) {
	const char8_t *begin = it;
	for (; it != end; ++it) {
		if (!sqlite3Isspace(*it)) {
			break;
		}
	}
	return std::make_pair(begin, it);
}

void SQLVdbeUtils::VdbeUtils::Impl::trimSpace(
		const char8_t *&begin, const char8_t *&end) {
	for (; begin != end; ++begin) {
		if (!sqlite3Isspace(*begin)) {
			break;
		}
	}
	for (; end != begin; --end) {
		if (!sqlite3Isspace(*(end - 1))) {
			break;
		}
	}
}

bool SQLVdbeUtils::VdbeUtils::Impl::isEmptyToken(
		const std::pair<const char8_t*, const char8_t*> &token) {
	return (token.first == token.second);
}

void SQLVdbeUtils::VdbeUtils::Impl::setUpAllocator(ValueContext &cxt) {
	SQLVarSizeAllocator *varAlloc = &cxt.getVarAllocator();
	assert(varAlloc != NULL);

	const int rc = sqlite3gs_allocator_set(
			NULL, CUtilVariableSizeAllocatorWrapper::wrap(varAlloc));
	assert(rc == SQLITE_OK);
	static_cast<void>(rc);
}

#define GS_EXECUTE_SQL_FUNCTION(db, funcStmt, extraMessage) \
	do { \
		if ((funcStmt) != SQLITE_OK) { \
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL, ""); \
		} \
	} \
	while (false)

TupleValue SQLVdbeUtils::VdbeUtils::sqlPrintf(
		ValueContext &cxt, const char8_t *format,
		const TupleValue *args, size_t argCount) {

	typedef util::Vector<sqlite3_value> ValueList;
	typedef util::Vector<sqlite3_value*> ValuePtrList;

	util::StackAllocator &alloc = cxt.getAllocator();

	ValueList valueList(alloc);
	ValuePtrList valuePtrList(alloc);

	valueList.reserve(argCount);
	valuePtrList.reserve(argCount);

	Impl::setUpAllocator(cxt);

	struct MemCleaner {
		~MemCleaner() {
			for (ValueList::iterator it = valueList_->begin();
					it != valueList_->end(); ++it) {
				sqlite3VdbeMemRelease(&(*it));
			}
			sqlite3gs_allocator_set(NULL, NULL);
		}
		ValueList *valueList_;
	} memCleaner = { &valueList };
	static_cast<void>(memCleaner);

	for (size_t i = 0; i < argCount; i++) {
		const TupleValue &arg = args[i];

		sqlite3_value value;
		memset(&value, 0, sizeof(value));
		sqlite3VdbeMemInit(&value, NULL, MEM_Null);

		const TupleColumnType argType = arg.getType();
		if (TypeUtils::isIntegral(argType)) {
			sqlite3VdbeMemSetInt64(&value, ValueUtils::toLong(arg));
		}
		else if (TypeUtils::isFloating(argType)) {
			sqlite3VdbeMemSetDouble(&value, ValueUtils::toDouble(arg));
		}
		else if (argType == TupleTypes::TYPE_STRING ||
				argType == TupleTypes::TYPE_BLOB ||
				argType == TupleTypes::TYPE_BOOL ||
				TypeUtils::isTimestampFamily(argType)) {
			const TupleValue &strArg = ValueUtils::toString(cxt, arg);
			const size_t strSize = strArg.varSize();
			if (strSize >
					static_cast<unsigned>(std::numeric_limits<int>::max())) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_LIMIT_EXCEEDED,
						"Too long printf arg string size");
			}

			const char8_t *strBuffer = static_cast<const char8_t*>(strArg.varData());
			GS_EXECUTE_SQL_FUNCTION(NULL, sqlite3VdbeMemSetStr(
					&value, strBuffer, static_cast<int>(strSize),
					SQLITE_UTF8, SQLITE_STATIC), "");
		}
		else {
			assert(TypeUtils::isNull(argType));
			sqlite3VdbeMemSetNull(&value);
		}

		valueList.push_back(value);
		valuePtrList.push_back(&valueList.back());
	}

	if (argCount > static_cast<unsigned>(std::numeric_limits<int>::max())) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_LIMIT_EXCEEDED,
				"Too many printf args");
	}

	StringBuilder builder(cxt);

	const size_t limit =
			std::min<size_t>(cxt.getMaxStringLength(), SQLITE_MAX_LENGTH);
	const size_t printfLimit = limit + 1;
	builder.setLimit(printfLimit);
	builder.resize(builder.capacity());

	for (;;) {
		PrintfArguments printfArgs;
		printfArgs.nArg = static_cast<int>(argCount);
		printfArgs.nUsed = 0;
		printfArgs.apArg = (valuePtrList.empty() ? NULL : &valuePtrList[0]);

		StrAccum str;
		const int bufSize = static_cast<int>(builder.size());
		sqlite3StrAccumInit(&str, builder.data(), bufSize, bufSize);
		str.useMalloc = 0;

		sqlite3XPrintf(&str, SQLITE_PRINTF_SQLFUNC, format, &printfArgs);

		if (str.accError == 0 && str.zText != NULL) {
			const size_t resultSize = static_cast<size_t>(str.nChar);
			builder.resize(resultSize);
			builder.setLimit(limit);

			return builder.build(cxt);
		}

		do {
			if (str.accError == STRACCUM_TOOBIG) {
				if (builder.size() < printfLimit) {
					break;
				}
				builder.setLimit(limit);
			}
			GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_INTERNAL,
					"Unexpected printf result (code=" << str.accError << ")");
		}
		while (false);

		const size_t newSize =
				std::min<size_t>(printfLimit, builder.capacity() * 2) +
				(builder.capacity() >= printfLimit ? 1 : 0);
		builder.resize(newSize);
	}
}

size_t SQLVdbeUtils::VdbeUtils::numericToString(
		const TupleValue &value, char8_t *buf, size_t size) {
	return Impl::numericToStringInternal(value, buf, size);
}

SQLValues::StringBuilder&
SQLVdbeUtils::VdbeUtils::numericToString(
		StringBuilder &builder, const TupleValue &value) {
	return Impl::numericToStringInternal(builder, value);
}

TupleValue SQLVdbeUtils::VdbeUtils::toString(
		ValueContext &cxt, const int64_t &value) {
	StringBuilder builder(cxt);
	return Impl::numericToStringInternal(
			builder, TupleValue(value)).build(cxt);
}

TupleValue SQLVdbeUtils::VdbeUtils::toString(
		ValueContext &cxt, const double &value) {
	StringBuilder builder(cxt);
	return Impl::numericToStringInternal(
			builder, TupleValue(value)).build(cxt);
}

bool SQLVdbeUtils::VdbeUtils::toLong(
		const char8_t *buffer, size_t length, int64_t &result) {
	const char8_t *tail = buffer + length;
	for (; buffer != tail; --tail) {
		if (!isSpaceChar(*(tail - 1))) {
			break;
		}
	}

	const size_t trimmedLen = static_cast<size_t>(tail - buffer);
	if (trimmedLen > static_cast<unsigned>(std::numeric_limits<int>::max())) {
		result = int64_t();
		return false;
	}

	i64 i64Result;
	UTIL_STATIC_ASSERT(sizeof(i64Result) == sizeof(result));

	const bool succeeded = sqlite3Atoi64(
			buffer, &i64Result, static_cast<int>(trimmedLen), SQLITE_UTF8) ==
			SQLITE_OK;
	result = i64Result;

	return succeeded;
}

bool SQLVdbeUtils::VdbeUtils::toDouble(
		const char8_t *buffer, size_t length, double &result) {
	if (length > static_cast<unsigned>(std::numeric_limits<int>::max())) {
		result = double();
		return false;
	}

	if (Impl::stringToSpecialNumeric(buffer, length, result)) {
		return true;
	}

	const bool succeeded = !!sqlite3AtoF(
			buffer, &result, static_cast<int>(length), SQLITE_UTF8);

	if (succeeded &&
			!Impl::fitToLexicalFloatingBoundary(buffer, length, result)) {
		result = double();
		return false;
	}

	return succeeded;
}

int SQLVdbeUtils::VdbeUtils::strICmp(
		const char8_t *value1, const char8_t *value2) {
	return sqlite3StrICmp(value1, value2);
}

bool SQLVdbeUtils::VdbeUtils::matchGlob(
		const char8_t *str, const char8_t *pattern) {
	return sqlite3_strglob(pattern, str) == 0;
}

bool SQLVdbeUtils::VdbeUtils::isSpaceChar(util::CodePoint c) {
	return (c < 0x80 && isSpaceChar(static_cast<char8_t>(c)));
}

bool SQLVdbeUtils::VdbeUtils::isSpaceChar(char8_t c) {
	return !!sqlite3Isspace(c);
}

void SQLVdbeUtils::VdbeUtils::toLower(char8_t *buf, size_t size) {
	char8_t *const end = buf + size;
	for (char8_t *it = buf; it != end; ++it) {
		*it = static_cast<char8_t>(sqlite3Tolower(*it));
	}
}

void SQLVdbeUtils::VdbeUtils::toUpper(char8_t *buf, size_t size) {
	char8_t *const end = buf + size;
	for (char8_t *it = buf; it != end; ++it) {
		*it = static_cast<char8_t>(sqlite3Toupper(*it));
	}
}

void SQLVdbeUtils::VdbeUtils::generateRandom(void *buf, size_t size) {
	sqlite3_randomness(static_cast<int>(size), buf);
}

Select* gssqlSetUpSelect(Parse *parse, Select *select) {
	static_cast<void>(parse);
	static_cast<void>(select);
	return NULL;
}

Select* gssqlSetUpDelete(Parse *parse, SrcList *tableList, Expr *where) {
	static_cast<void>(parse);
	static_cast<void>(tableList);
	static_cast<void>(where);
	return NULL;
}

Select* gssqlSetUpUpdate(Parse *parse, SrcList *tableList, ExprList *changes,
		Expr *where, int onError) {
	static_cast<void>(parse);
	static_cast<void>(tableList);
	static_cast<void>(changes);
	static_cast<void>(where);
	static_cast<void>(onError);
	return NULL;
}

Select* gssqlSetUpInsert(Parse *parse, SrcList *tableList, Select *select,
		IdList *columnList, int onError) {
	static_cast<void>(parse);
	static_cast<void>(tableList);
	static_cast<void>(select);
	static_cast<void>(columnList);
	static_cast<void>(onError);
	return NULL;
}

void gssqlSelect(Parse *parse, Select *select) {
	static_cast<void>(parse);
	static_cast<void>(select);
}

void gssqlCreateDatabase(Parse *parse, Token *name) {
	static_cast<void>(parse);
	static_cast<void>(name);
}

void gssqlDropDatabase(Parse *parse, Token *name) {
	static_cast<void>(parse);
	static_cast<void>(name);
}

void gssqlCreateTable(Parse *parse, Token *name1, Token *name2,
		RewriterTableColumnList *columnList, RewriterTableOption *option,
		int noErr) {
	static_cast<void>(parse);
	static_cast<void>(name1);
	static_cast<void>(name2);
	static_cast<void>(columnList);
	static_cast<void>(option);
	static_cast<void>(noErr);
}

RewriterTableColumnList* gssqlAppendTableColumn(
		Parse *parse, RewriterTableColumnList *columnList,
		RewriterTableColumn *column) {
	static_cast<void>(parse);
	static_cast<void>(columnList);
	static_cast<void>(column);
	return NULL;
}

void gssqlDeleteTableColumnList(
		sqlite3 *db, RewriterTableColumnList *columnList) {
	static_cast<void>(db);
	static_cast<void>(columnList);
}

RewriterTableColumn* gssqlCreateTableColumn(
		Parse *parse, Token *name, Token *type, int option) {
	static_cast<void>(parse);
	static_cast<void>(name);
	static_cast<void>(type);
	static_cast<void>(option);
	return NULL;
}

void gssqlDeleteTableColumn(sqlite3 *db, RewriterTableColumn *column) {
	static_cast<void>(db);
	static_cast<void>(column);
}

int gssqlUpdateTableColumnOption(Parse *parse, int base, int extra) {
	static_cast<void>(parse);
	static_cast<void>(base);
	static_cast<void>(extra);
	return 0;
}

RewriterTableOption* gssqlCreateTableOption(
		Parse *parse, Token *partitionColumn, Token *partitionCount) {
	static_cast<void>(parse);
	static_cast<void>(partitionColumn);
	static_cast<void>(partitionCount);
	return NULL;
}

void gssqlDeleteTableOption(sqlite3 *db, RewriterTableOption *option) {
	static_cast<void>(db);
	static_cast<void>(option);
}

void gssqlDropTable(Parse *parse, SrcList *name, int noErr) {
	static_cast<void>(parse);
	static_cast<void>(name);
	static_cast<void>(noErr);
}

void gssqlCreateIndex(Parse *parse,
		Token *indexName, Token *tableName1, Token *tableName2,
		ExprList *columnList, int ifNotExist) {
	static_cast<void>(parse);
	static_cast<void>(indexName);
	static_cast<void>(tableName1);
	static_cast<void>(tableName2);
	static_cast<void>(columnList);
	static_cast<void>(ifNotExist);
}

void gssqlDropIndex(Parse *parse, SrcList *name, int noErr) {
	static_cast<void>(parse);
	static_cast<void>(name);
	static_cast<void>(noErr);
}

void gssqlCreateUser(Parse *parse, Token *userName, Token *password) {
	static_cast<void>(parse);
	static_cast<void>(userName);
	static_cast<void>(password);
}

void gssqlSetPassword(Parse *parse, Token *userName, Token *password) {
	static_cast<void>(parse);
	static_cast<void>(userName);
	static_cast<void>(password);
}

void gssqlDropUser(Parse *parse, Token *userName) {
	static_cast<void>(parse);
	static_cast<void>(userName);
}

void gssqlGrant(Parse *parse, Token *dbName, Token *userName) {
	static_cast<void>(parse);
	static_cast<void>(dbName);
	static_cast<void>(userName);
}

void gssqlRevoke(Parse *parse, Token *dbName, Token *userName) {
	static_cast<void>(parse);
	static_cast<void>(dbName);
	static_cast<void>(userName);
}

void gssqlExecutePragma(
		Parse *parse, Token *key1, Token *key2, Token *value, int minusFlag) {
	static_cast<void>(parse);
	static_cast<void>(key1);
	static_cast<void>(key2);
	static_cast<void>(value);
	static_cast<void>(minusFlag);
}

int gsCursorCreate(SQLStatement *pSqlStatement, int iTable, SQLCursor **pCur) {
	static_cast<void>(pSqlStatement);
	static_cast<void>(iTable);
	static_cast<void>(pCur);
	return -1;
}

int gsCursorClose(SQLCursor *pCur) {
	static_cast<void>(pCur);
	return -1;
}

int gsCursorFirst(SQLCursor *pCur, int *pRes) {
	static_cast<void>(pCur);
	static_cast<void>(pRes);
	return -1;
}

int gsCursorNext(SQLCursor *pCur, int *pRes) {
	static_cast<void>(pCur);
	static_cast<void>(pRes);
	return -1;
}

int gsCursorLast(SQLCursor *pCur, int *pRes) {
	static_cast<void>(pCur);
	static_cast<void>(pRes);
	return -1;
}

int gsCursorPosition(SQLCursor *pCur, int64_t *pos) {
	static_cast<void>(pCur);
	static_cast<void>(pos);
	return -1;
}

int gsCursorPosition2(SQLCursor *pCur, int *blockNo, int64_t *pos) {
	static_cast<void>(pCur);
	static_cast<void>(blockNo);
	static_cast<void>(pos);
	return -1;
}

int gsCursorMove(SQLCursor *pCur, int64_t *pos, int *pRes) {
	static_cast<void>(pCur);
	static_cast<void>(pos);
	static_cast<void>(pRes);
	return -1;
}

int gsCursorMove2(SQLCursor *pCur, int blockNo, int64_t *pos, int *pRes) {
	static_cast<void>(pCur);
	static_cast<void>(blockNo);
	static_cast<void>(pos);
	static_cast<void>(pRes);
	return -1;
}

int gsColumnType(SQLStatement *pSqlStatement, int iTable, int iCol) {
	static_cast<void>(pSqlStatement);
	static_cast<void>(iTable);
	static_cast<void>(iCol);
	return -1;
}

int gsColumnCount(SQLStatement *pSqlStatement, int iTable) {
	static_cast<void>(pSqlStatement);
	static_cast<void>(iTable);
	return -1;
}

int64_t gsRowCount(SQLStatement *pSqlStatement, int iTable) {
	static_cast<void>(pSqlStatement);
	static_cast<void>(iTable);
	return -1;
}

int gsGetValueString(
		SQLCursor *pCur, int iCol, const char **value, size_t *size) {
	static_cast<void>(pCur);
	static_cast<void>(iCol);
	static_cast<void>(value);
	static_cast<void>(size);
	return -1;
}

int gsGetValueBool(SQLCursor *pCur, int iCol, int8_t *value) {
	static_cast<void>(pCur);
	static_cast<void>(iCol);
	static_cast<void>(value);
	return -1;
}

int gsGetValueByte(SQLCursor *pCur, int iCol, int8_t *value) {
	static_cast<void>(pCur);
	static_cast<void>(iCol);
	static_cast<void>(value);
	return -1;
}

int gsGetValueShort(SQLCursor *pCur, int iCol, int16_t *value) {
	static_cast<void>(pCur);
	static_cast<void>(iCol);
	static_cast<void>(value);
	return -1;
}

int gsGetValueInteger(SQLCursor *pCur, int iCol, int *value) {
	static_cast<void>(pCur);
	static_cast<void>(iCol);
	static_cast<void>(value);
	return -1;
}

int gsGetValueLong(SQLCursor *pCur, int iCol, int64_t *value) {
	static_cast<void>(pCur);
	static_cast<void>(iCol);
	static_cast<void>(value);
	return -1;
}

int gsGetValueFloat(SQLCursor *pCur, int iCol, float *value) {
	static_cast<void>(pCur);
	static_cast<void>(iCol);
	static_cast<void>(value);
	return -1;
}

int gsGetValueDouble(SQLCursor *pCur, int iCol, double *value) {
	static_cast<void>(pCur);
	static_cast<void>(iCol);
	static_cast<void>(value);
	return -1;
}

int gsGetValueTimestamp(SQLCursor *pCur, int iCol, int64_t *value) {
	static_cast<void>(pCur);
	static_cast<void>(iCol);
	static_cast<void>(value);
	return -1;
}

int gsGetValueBlob(SQLCursor *pCur, int iCol, GSBlobTag *value) {
	static_cast<void>(pCur);
	static_cast<void>(iCol);
	static_cast<void>(value);
	return -1;
}

int gsGetValueStringNull(SQLCursor *pCur, int iCol,
		const char **value, size_t *size, int *pRes) {
	static_cast<void>(pCur);
	static_cast<void>(iCol);
	static_cast<void>(value);
	static_cast<void>(size);
	static_cast<void>(pRes);
	return -1;
}

int gsGetValueBoolNull(SQLCursor *pCur, int iCol, int8_t *value, int *pRes) {
	static_cast<void>(pCur);
	static_cast<void>(iCol);
	static_cast<void>(value);
	static_cast<void>(pRes);
	return -1;
}

int gsGetValueByteNull(SQLCursor *pCur, int iCol, int8_t *value, int *pRes) {
	static_cast<void>(pCur);
	static_cast<void>(iCol);
	static_cast<void>(value);
	static_cast<void>(pRes);
	return -1;
}

int gsGetValueShortNull(SQLCursor *pCur, int iCol, int16_t *value, int *pRes) {
	static_cast<void>(pCur);
	static_cast<void>(iCol);
	static_cast<void>(value);
	static_cast<void>(pRes);
	return -1;
}

int gsGetValueIntegerNull(SQLCursor *pCur, int iCol, int *value, int *pRes) {
	static_cast<void>(pCur);
	static_cast<void>(iCol);
	static_cast<void>(value);
	static_cast<void>(pRes);
	return -1;
}

int gsGetValueLongNull(SQLCursor *pCur, int iCol, int64_t *value, int *pRes) {
	static_cast<void>(pCur);
	static_cast<void>(iCol);
	static_cast<void>(value);
	static_cast<void>(pRes);
	return -1;
}

int gsGetValueFloatNull(SQLCursor *pCur, int iCol, float *value, int *pRes) {
	static_cast<void>(pCur);
	static_cast<void>(iCol);
	static_cast<void>(value);
	static_cast<void>(pRes);
	return -1;
}

int gsGetValueDoubleNull(SQLCursor *pCur, int iCol, double *value, int *pRes) {
	static_cast<void>(pCur);
	static_cast<void>(iCol);
	static_cast<void>(value);
	static_cast<void>(pRes);
	return -1;
}

int gsGetValueTimestampNull(SQLCursor *pCur, int iCol, int64_t *value, int *pRes) {
	static_cast<void>(pCur);
	static_cast<void>(iCol);
	static_cast<void>(value);
	static_cast<void>(pRes);
	return -1;
}

int gsGetValueBlobNull(SQLCursor *pCur, int iCol, GSBlobTag *value, int *pRes) {
	static_cast<void>(pCur);
	static_cast<void>(iCol);
	static_cast<void>(value);
	static_cast<void>(pRes);
	return -1;
}

int gsSetConnectionEnv(SQLStatement *pSQLStatement, int type, int flag) {
	static_cast<void>(pSQLStatement);
	static_cast<void>(type);
	static_cast<void>(flag);
	return -1;
}

int gsGetConnectionEnv(SQLStatement *pSQLStatement, int type) {
	static_cast<void>(pSQLStatement);
	static_cast<void>(type);
	return -1;
}
