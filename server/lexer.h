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
	@brief Definition of Lexer for TQL
*/

#ifndef LEXER_H_
#define LEXER_H_

#include <cctype>
#include <cstring>

#include "expression.h"
#include "tql_token.h"

#ifdef _WIN32
#define gsStrNICmp _strnicmp
#else
#define gsStrNICmp strncasecmp
#endif

#define charMap(X) (Lexer::gsUpperToLower[(unsigned char)X])
#define IdChar(C) ((Lexer::gsCtypeMap[(unsigned char)C] & 0x46) != 0)
#define IsSpace(C) ((Lexer::gsCtypeMap[(unsigned char)C] & 0x01) != 0)
#define IsDigit(C) ((Lexer::gsCtypeMap[(unsigned char)C] & 0x04) != 0)


/*!
 * @brief  TQL and WKT tokenizer (Lexer)
 */
class Lexer {
public:
	static int gsGetToken(const char *str, Token &t);
	static const unsigned char gsCtypeMap[256];
	static const unsigned char gsUpperToLower[256];
	static const unsigned char gsLowerToUpper[256];

	static inline char ToUpper(char x) {
		if (x >= 'a' && x <= 'z') return x + 'A' - 'a';
		return x;
	}
	static inline char ToLower(char x) {
		if (x >= 'A' && x <= 'Z') return x + 'a' - 'A';
		return x;
	}

private:
	static inline int _gsGetToken(const char *z, int *type);
};

#endif
