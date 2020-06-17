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

#ifndef SQL_LEXER_H
#define SQL_LEXER_H

#include <cstring>
#include <cctype>

#include "sql_parser.h" 
#include "sql_internal_parser_token.h"

#ifdef _WIN32
#define gsStrNICmp _strnicmp
#else
#define gsStrNICmp strncasecmp
#endif

/*!
 * @brief SQL lexer
 */
class SQLLexer{
public:
	static bool gsGetToken(const char8_t *str, SQLToken &t, bool &inHint);
	static const uint8_t gsCtypeMap[256];
	static const uint8_t gsUpperToLower[256];
	static const uint8_t gsLowerToUpper[256];

	static inline bool isIdChar(uint8_t x) {
		return (gsCtypeMap[x] & 0x46) != 0;
	}
	static inline bool isSpace(uint8_t x) {
		return (gsCtypeMap[x] & 0x01) != 0;
	}
	static inline bool isDigit(uint8_t x) {
		return (gsCtypeMap[x] & 0x04) != 0;
	}
	static inline bool isXDigit(uint8_t x) {
		return (gsCtypeMap[x] & 0x08) != 0;
	}
	static inline uint8_t charMap(uint8_t x) {
		return gsUpperToLower[x];
	}

private:
	static inline int32_t _gsGetToken(
			const char8_t *z, int32_t *type, bool &inHint);

};

#endif
