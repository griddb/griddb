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
	@brief Definition of String functions for TQL
*/

#ifndef FUNCTIONS_STRING_H_
#define FUNCTIONS_STRING_H_

#include <algorithm>
#include <cctype>
#include <string>

#include "expression.h"
#include "qp_def.h"
#include "utf8.h"


/*!
 * @brief STRING(x)
 *
 * @return x as string
 */
class FunctorString : public TqlFunc {
public:
	using TqlFunc::operator();
	Expr *operator()(ExprList &args, TransactionContext &txn, ObjectManager &) {
		if (args.size() != 1) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		return Expr::newStringValue(args[0]->getValueAsString(txn), txn);
	}
	virtual ~FunctorString() {}
};

/*!
 * @brief CHAR_LENGTH(str)
 *
 * @return length
 */
class FunctorCharLength : public TqlFunc {
public:
	using TqlFunc::operator();
	Expr *operator()(ExprList &args, TransactionContext &txn, ObjectManager &) {
		if (args.size() != 1) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		else if (!args[0]->isString()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 1 is not a string");
		}
		else {
			/* DO NOTHING */
		}
		const char *s = args[0]->getValueAsString(txn);
		return Expr::newNumericValue(static_cast<int32_t>(_utf8Strlen(s)), txn);
	}
	virtual ~FunctorCharLength() {}
};

/*!
 * @brief CONCAT(str1, str2, ...)
 *
 * @return concatinated string
 */
class FunctorConcat : public TqlFunc {
public:
	using TqlFunc::operator();
	Expr *operator()(ExprList &args, TransactionContext &txn, ObjectManager &) {
		if (args.empty() || args.size() < 2) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		size_t i;
		util::String s("", QP_ALLOCATOR);  
		for (i = 0; i < args.size(); i++) {
			if (!args[i]->isString()) {
				util::NormalOStringStream os;
				os << "Argument " << i + 1 << " is not a string";
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
					os.str().c_str());
			}
			s += args[i]->getValueAsString(txn);
		}
		return Expr::newStringValue(s.c_str(), txn);
	}
	virtual ~FunctorConcat() {}
};

#define USE_SQL92_LIKE 0 /* Case sensitive : 1=Ignore case */
/*!
 * @brief LIKE(str1, str2, [escape])
 *
 * @return whether str1 contains str2
 */
class FunctorLike : public TqlFunc {
#ifndef USE_SQL92_LIKE
	inline bool match(
		const char *text, int textlen, const char *pattern, int patternlen) {
		char first = pattern[0];
		char last = pattern[patternlen - 1];
		if (first == '%') {
			pattern = &pattern[1];
			if (last == '%') {
				return std::find(text, pattern, 0) != std::string::npos;
			}
			return text.find(pattern, 0, patternlen) != std::string::npos;
		}
		else if (last == '%') {
			std::string tmp("", pattern.c_str(), patternlen - 1);
			pattern = tmp.c_str();
			return text.find(pattern, text.length() - patternlen) !=
				   std::string::npos;
		}
		else {
			/* DO NOTHING */
		}
		return (text.compare(pattern) == 0);
	}
#else
	/*!
	 * @brief A structure defining how to do GLOB-style comparisons.
	 * Implementation is taken from SQLite
	 */
	struct compareInfo {
		uint8_t matchAll;
		uint8_t matchOne;
		uint8_t matchSet;
		uint8_t noCase;
	};

	/*!
	 * @brief SQL LIKE Pattern matcher
	 *
	 * @param zPattern Pattern string (described in SQL-LIKE grammer)
	 * @param zString Target string
	 * @param pInfo compareInfo
	 * @param esc Escape character
	 *
	 * @return 1 when matched, 0 otherwise
	 * @note cf.(Public Domain)http://www.sqlite.org
	 */
	int patternCompare(const char *zPattern, const char *zString,
		const struct compareInfo *pInfo, const int esc) {
		int c, c2;
		int invert;
		int seen;
		uint8_t matchOne = pInfo->matchOne;
		uint8_t matchAll = pInfo->matchAll;
		uint8_t matchSet = pInfo->matchSet;
		uint8_t noCase = pInfo->noCase;
		int prevEscape = 0; /* True if the previous character was 'escape' */

		while ((c = _READ_UTF8(zPattern, &zPattern)) != 0) {
			if (!prevEscape && c == matchAll) {
				while ((c = _READ_UTF8(zPattern, &zPattern)) == matchAll ||
					   c == matchOne) {
					if (c == matchOne && _READ_UTF8(zString, &zString) == 0) {
						return 0;
					}
				}
				if (c == 0) {
					return 1;
				}
				else if (c == esc) {
					c = _READ_UTF8(zPattern, &zPattern);
					if (c == 0) {
						return 0;
					}
				}
				else if (c == matchSet) {
					if (esc != 0) { /* This is GLOB, not LIKE */
						GS_THROW_USER_ERROR(
							GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
							"GLOB cannot take ESCAPE");
					}
					else if (matchSet >=
							 0x80) { /* '[' is a single-byte character */
						GS_THROW_USER_ERROR(
							GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
							"matchSet must be single byte.");
					}
					else {
						/* DO NOTHING */
					}
					while (*zString &&
						   patternCompare(&zPattern[-1], zString, pInfo, esc) ==
							   0) {
						_SKIP_UTF8(zString);
					}
					return *zString != 0;
				}
				else {
					/* DO NOTHING */
				}

				while ((c2 = _READ_UTF8(zString, &zString)) != 0) {
					if (noCase) {
						c2 = std::toupper(c2);
						c = std::toupper(c);
						while (c2 != 0 && c2 != c) {
							c2 = _READ_UTF8(zString, &zString);
							c2 = std::toupper(c2);
						}
					}
					else {
						while (c2 != 0 && c2 != c) {
							c2 = _READ_UTF8(zString, &zString);
						}
					}
					if (c2 == 0) return 0;
					if (patternCompare(zPattern, zString, pInfo, esc)) return 1;
				}
				return 0;
			}
			else if (!prevEscape && c == matchOne) {
				if (_READ_UTF8(zString, &zString) == 0) {
					return 0;
				}
			}
			else if (c == matchSet) {
				int prior_c = 0;
				if (esc != 0) { /* This only occurs for GLOB, not LIKE */
					GS_THROW_USER_ERROR(
						GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
						"GLOB cannot take ESCAPE");
				}
				seen = 0;
				invert = 0;
				c = _READ_UTF8(zString, &zString);
				if (c == 0) return 0;
				c2 = _READ_UTF8(zPattern, &zPattern);
				if (c2 == '^') {
					invert = 1;
					c2 = _READ_UTF8(zPattern, &zPattern);
				}
				if (c2 == ']') {
					if (c == ']') seen = 1;
					c2 = _READ_UTF8(zPattern, &zPattern);
				}
				while (c2 && c2 != ']') {
					if (c2 == '-' && zPattern[0] != ']' && zPattern[0] != 0 &&
						prior_c > 0) {
						c2 = _READ_UTF8(zPattern, &zPattern);
						if (c >= prior_c && c <= c2) seen = 1;
						prior_c = 0;
					}
					else {
						if (c == c2) {
							seen = 1;
						}
						prior_c = c2;
					}
					c2 = _READ_UTF8(zPattern, &zPattern);
				}
				if (c2 == 0 || (seen ^ invert) == 0) {
					return 0;
				}
			}
			else if (esc == c && !prevEscape) {
				prevEscape = 1;
			}
			else {
				c2 = _READ_UTF8(zString, &zString);
				if (noCase) {
					c = std::toupper(c);
					c2 = std::toupper(c2);
				}
				if (c != c2) {
					return 0;
				}
				prevEscape = 0;
			}
		}
		return *zString == 0;
	}

	/*!
	 * @brief Matcher entry function
	 *
	 * @param text Target text to search
	 * @param pattern Search pattern
	 * @param esc Escape character
	 *
	 * @return matched or not
	 */
	inline bool match(const char *text, const char *pattern, int esc) {
		/* The correct SQL-92 behavior is for the LIKE operator to ignore
		** case.  Thus  'a' LIKE 'A' would be true. */
		const struct compareInfo likeInfoNorm = {'%', '_', 0, USE_SQL92_LIKE};
		return (patternCompare(pattern, text, &likeInfoNorm, esc) != 0);
	}
#endif /* USE_SQL92_LIKE */

public:
	using TqlFunc::operator();
	Expr *operator()(ExprList &args, TransactionContext &txn, ObjectManager &) {
		if (args.size() < 3 || args.size() > 4) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		else if (!args[0]->isString()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 1 is not a string");
		}
		else if (!args[1]->isString()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 2 is not a string");
		}
		else if (!args[2]->isBoolean()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_INVALID_ARGUMENT,
				"Internal logic error: internal argument is not a boolean");
		}
		else if ((args.size() == 4 && !args[3]->isString())) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 3 is not a string");
		}
		else {
			/* DO NOTHING */
		}
		const char *s1 = args[0]->getValueAsString(txn);
		const char *s2 = args[1]->getValueAsString(txn);
		bool invert = args[2]->getValueAsBool();

		int esc = 0;
		if (args.size() == 4) {
			const char *escStr = args[3]->getValueAsString(txn);
			if (escStr[0] == '\0' || escStr[1] != '\0') {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE,
					"ESCAPE CHAR must be only 1 character");
			}
			esc = escStr[0];
		}

#ifndef USE_SQL92_LIKE
		return Expr::newBooleanValue(
			(invert) ? !match(s1, s2) : match(s1, s2), txn);
#else
		return Expr::newBooleanValue(
			(invert) ? !match(s1, s2, esc) : match(s1, s2, esc), txn);
#endif
	}

	virtual ~FunctorLike() {}
};

/*!
 * @brief SUBSTRING(str, offset, [length=inf])
 *
 * @return substring start from 'offset' whose length is 'length'
 */
class FunctorSubstring : public TqlFunc {
public:
	using TqlFunc::operator();
	Expr *operator()(ExprList &args, TransactionContext &txn, ObjectManager &) {
		if (args.size() > 3 || args.size() < 2) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		else if (!args[0]->isString()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 1 is not a string");
		}
		else if (!args[1]->isNumericInteger()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 2 is not an numeric value");
		}
		else if (args.size() == 3 && !args[2]->isNumericInteger()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 3 is not an numeric value");
		}
		else {
			/* DO NOTHING */
		}

		int offset = args[1]->getValueAsInt();
		if (offset <= 0) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE,
				"Argument 2 is invalid range");
		}
		size_t off = static_cast<size_t>(offset - 1);

		const char *str = args[0]->getValueAsString(txn);
		util::String newstr(
			"", QP_ALLOCATOR);  

		if (args.size() == 2) {
			_utf8Substring(str, static_cast<int>(off), -1, newstr);
		}
		else {
			int len = args[2]->getValueAsInt();
			if (len < 0) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE,
					"Argument 3 is invalid as length");
			}
			_utf8Substring(str, static_cast<int>(off), len, newstr);
		}

		return Expr::newStringValue(newstr.c_str(), txn);
	}

	virtual ~FunctorSubstring() {}
};

/*!
 * @brief UPPER(str) and LOWER(str) base
 *
 * @return string transformed
 */
template <int (*T)(int)>
class FunctorTrans : public TqlFunc {
public:
	Expr *operator()(ExprList &args, TransactionContext &txn, ObjectManager &) {
		if (args.empty() || args.size() > 1) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		else if (!args[0]->isString()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 1 is not a string");
		}
		else {
			/* DO NOTHING */
		}

		util::String transformed(args[0]->getValueAsString(txn),
			QP_ALLOCATOR);  
		std::transform(
			transformed.begin(), transformed.end(), transformed.begin(), T);
		return Expr::newStringValue(transformed.c_str(), txn);
	}

	virtual ~FunctorTrans() {}
};

/*!
 * @brief UPPER(str)
 */
typedef FunctorTrans<std::toupper> Functor_upper;
/*!
 * @brief LOWER(str)
 */
typedef FunctorTrans<std::tolower> Functor_lower;


#endif /*FUNCTIONS_STRING_HPP*/
