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


#include "util/allocator.h"
#include "sql_parser_define.h"
#include "sql_internal_parser_token.h"
#include "sql_lexer.h"
#include "sql_lexer_keyword.h"
#include <cstring>
#include <cctype>

/*!
 * Cherset type map.
 *
 * Bit 0 (0x01): SPACE
 * Bit 1 (0x02): ALPHA
 * Bit 2 (0x04): DIGIT
 * Bit 3 (0x08): XDIGIT
 * Bit 4 (0x10): NOT USED
 * Bit 5 (0x20): UPPER
 * Bit 6 (0x40): Can be used as ID
 * BIT 7 (0x80): NOT USED
 */
const uint8_t SQLLexer::gsCtypeMap[256] = {
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* 00..07    ........ */
	0x00, 0x01, 0x01, 0x01, 0x01, 0x01, 0x00, 0x00,  /* 08..0f    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* 10..17    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* 18..1f    ........ */
	0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* 20..27     !"#$%&' */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* 28..2f    ()*+,-./ */
	0x0c, 0x0c, 0x0c, 0x0c, 0x0c, 0x0c, 0x0c, 0x0c,  /* 30..37    01234567 */
	0x0c, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* 38..3f    89:;<=>? */

	0x00, 0x0a, 0x0a, 0x0a, 0x0a, 0x0a, 0x0a, 0x02,  /* 40..47    @ABCDEFG */
	0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02,  /* 48..4f    HIJKLMNO */
	0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02,  /* 50..57    PQRSTUVW */
	0x02, 0x02, 0x02, 0x00, 0x00, 0x00, 0x00, 0x40,  /* 58..5f    XYZ[\]^_ */
	0x00, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x22,  /* 60..67    `abcdefg */
	0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22,  /* 68..6f    hijklmno */
	0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22,  /* 70..77    pqrstuvw */
	0x22, 0x22, 0x22, 0x00, 0x00, 0x00, 0x00, 0x00,  /* 78..7f    xyz{|}~. */

	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* 80..87    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* 88..8f    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* 90..97    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* 98..9f    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* a0..a7    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* a8..af    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* b0..b7    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* b8..bf    ........ */

	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* c0..c7    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* c8..cf    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* d0..d7    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* d8..df    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* e0..e7    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* e8..ef    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* f0..f7    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00   /* f8..ff    ........ */
  };

/*!
 * ToLower table
 *
 */
const uint8_t SQLLexer::gsUpperToLower[] = {
	0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15, 16, 17,
	18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35,
	36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53,
	54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 97, 98, 99,100,101,102,103,
	104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,
	122, 91, 92, 93, 94, 95, 96, 97, 98, 99,100,101,102,103,104,105,106,107,
	108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,
	126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,
	144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,
	162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,
	180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,
	198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,
	216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,
	234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,
	252,253,254,255
  };


/*!
 * ToUpper table
 *
 */
const uint8_t SQLLexer::gsLowerToUpper[] = {
	0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15, 16, 17,
	18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35,
	36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53,
	54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71,
	72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89,
	90, 91, 92, 93, 94, 95, 96, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75,
	76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 123,124,125,
	126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,
	144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,
	162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,
	180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,
	198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,
	216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,
	234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,
	252,253,254,255
  };

/*!
 * Return the length of the token that begins at z[0].
 * Store the token type in *tokenType before returning.
 *
 * @param[in] z string to get token from.
 * @param[out] tokenType returned token type.
 *
 * @return token length
 */
inline int32_t SQLLexer::_gsGetToken(
		const char8_t *z, int32_t *tokenType, bool &inHint) {
	int32_t i, c;
	uint8_t x = *z;

	if (x != 'X' && x != 'x' && x != '$' && !isDigit(x) && isIdChar(x)) {
	ALPHA:
		for (i=1; isIdChar(z[i]); i++) {}
		*tokenType = keywordCode(z, i);
		return i;
	}
	else if (isSpace(x)) {
		for (i = 1; isSpace(z[i]); i++) {}
		*tokenType = TK_SPACE;
		return i;
	}
	else if (x == '*') {
		if (!inHint) {
			*tokenType = TK_STAR;
			return 1;
		}
		else {
			if ( z[1]=='/' ) { 
				inHint = false;
				*tokenType = TK_HINT_END;
				return 2;
			}
			else {
				*tokenType = TK_STAR;
				return 1;
			}
		}
	}
	else if (x == '(') {
		*tokenType = TK_LP;
		return 1;
	}
	else if (x == ')') {
		*tokenType = TK_RP;
		return 1;
	}
	else {
		switch (x) {
		case '+': {
			*tokenType = TK_PLUS;
			return 1;
		}
		case '%': {
			*tokenType = TK_REM;
			return 1;
		}
		case ',': {
			*tokenType = TK_COMMA;
			return 1;
		}
		case '&': {
			*tokenType = TK_BITAND;
			return 1;
		}
		case '~': {
			*tokenType = TK_BITNOT;
			return 1;
		}
		case ';': {
			*tokenType = TK_SEMI;
			return 1;
		}
		case '-': {
			if ( z[1]=='-' ) { 
				for (i = 2; (c = z[i]) != 0 && c != '\n'; i++) {}
				*tokenType = TK_SPACE;
				return i;
			}
			*tokenType = TK_MINUS;
			return 1;
		}
		case '/': {
			if ( z[1] != '*' || z[2] == 0 ) {
				*tokenType = TK_SLASH;
				return 1;
			}
			/* Syntax diagram for comments */
			c = z[2];
			if (c != '+') {
				for (i = 3; (c != '*' || z[i] != '/') && (c = z[i]) != 0; i++) {}
				if ( c ) i++;
				*tokenType = TK_SPACE;
				return i;
			}
			else {
				if (inHint) {
					*tokenType = TK_ILLEGAL;
					return 3;
				}
				inHint = true;
				*tokenType = TK_HINT_START;
				return 3;
			}
		}
		case '=': {
			*tokenType = TK_EQ;
			return 1 + (z[1] == '=');
		}
		case '<': {
			c = z[1];
			if ( c == '=') {
				*tokenType = TK_LE;
				return 2;
			}
			else if (c == '>') {
				*tokenType = TK_NE;
				return 2;
			}
			else if (c == '<') {
				*tokenType = TK_LSHIFT;
				return 2;
			}
			else {
				*tokenType = TK_LT;
				return 1;
			}
		}
		case '>': {
			c = z[1];
			if (c == '=') {
				*tokenType = TK_GE;
				return 2;
			}
			else if (c == '>') {
				*tokenType = TK_RSHIFT;
				return 2;
			}
			else {
				*tokenType = TK_GT;
				return 1;
			}
		}
		case '!': {
			if (z[1] != '=') {
				*tokenType = TK_ILLEGAL;
				return 2;
			}
			else {
				*tokenType = TK_NE;
				return 2;
			}
		}
		case '|': {
			if (z[1] != '|') {
				*tokenType = TK_BITOR;
				return 1;
			}
			else {
				*tokenType = TK_CONCAT;
				return 2;
			}
		}

		case '\'':
		case '"': {
			int32_t delim = z[0];
			for (i = 1; (c = z[i]) != 0; i++) {
				if (c == delim) {
					if (z[i+1] == delim) {
						i++;
					}
					else {
						break;
					}
				}
			}
			if (c == '\'') {
				*tokenType = TK_STRING;
				return i+1;
			}
			else if (c != 0) {
				*tokenType = TK_ID;
				return i+1;
			}
			else {
				*tokenType = TK_ILLEGAL;
				if (c != 0) i++;
				return i;
			}
		}
		case '.': {
			if (!isDigit(z[1])) {
				*tokenType = TK_DOT;
				return 1;
			}
		}

		case '0': case '1': case '2': case '3': case '4':
		case '5': case '6': case '7': case '8': case '9': {
			*tokenType = TK_INTEGER;
			for (i = 0; isDigit(z[i]); i++) {}
			if (z[i] == '.') {
				i++;
				while (isDigit(z[i])) { i++; }
				*tokenType = TK_FLOAT;
			}
			if ((z[i] == 'e' || z[i] == 'E') &&
					( isDigit(z[i+1]) ||
							((z[i+1] == '+' || z[i+1] == '-') && isDigit(z[i+2]))
					)
				) {
				i += 2;
				while (isDigit(z[i])) { i++; }
				*tokenType = TK_FLOAT;
			}
			while (isIdChar(z[i])) {
				*tokenType = TK_ILLEGAL;
				i++;
			}
			return i;
		}

		case '[': { 
			c = z[0];
			for (i = 1; c != ']' && (c = z[i]) != 0; i++) {}
			*tokenType = (c == ']' ? TK_ID : TK_ILLEGAL);
			return i;
		}
		case '?': {
			*tokenType = TK_VARIABLE;
			for (i = 1; isDigit(z[i]); i++) {}
			return i;
		}
		case '@':  /* For compatibility with MS SQL Server */
		case ':': {
			int32_t n = 0;
			*tokenType = TK_VARIABLE;
			for (i = 1; (c = z[i]) != 0; i++) {
				if (isIdChar(z[i])) {
					n++;
				}
				else {
					break;
				}
			}
			if ( n==0 ) {
			}
			return i;
		}

		case 'x':
			/* FALLTHROUGH */
		case 'X': {
			if (z[1] == '\'') {
				*tokenType = TK_BLOB;
				for (i = 2; (c = z[i]) != 0 && c != '\''; i++) {
					if (!isXDigit(z[i])) {
						*tokenType = TK_ILLEGAL;
					}
				}
				if (i % 2 || !c) *tokenType = TK_ILLEGAL;
				if (c) i++;
				return i;
			}
			/* Otherwise fall through to the next case */
			goto ALPHA;
		}

		} 
	}

	*tokenType = TK_ILLEGAL;
	return 1;
}

/*!
 * Wrapper routine
 *
 * @param str string to get token from.
 * @param t returned token.
 *
 * @return true:token get is successful, false: not successful.
 */
bool SQLLexer::gsGetToken(const char8_t *str, SQLToken &t, bool &inHint) {

	if (str == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
				"Internal logic error: Token string is NULL");
	}
	if (str[0] == '\0') {
		t.id_ = TK_ILLEGAL;
		t.size_ = 0;
		t.value_ = str;
		return false;
	}
	t.value_ = str;
	t.pos_ += t.size_;
	t.size_ = _gsGetToken(str, &t.id_, inHint);

	return (t.size_ != 0);
}
