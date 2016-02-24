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
	@brief Implementation of Lexer for TQL
*/

#include "lexer.h"
#include "util/allocator.h"
#include "expression.h"
#include "qp_def.h"
#include "tql.h"
#include "tql_token.h"
#include <cctype>
#include <cstring>

#define testcase(...)

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
const unsigned char Lexer::gsCtypeMap[256] = {
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* 00..07    ........ */
	0x00, 0x01, 0x01, 0x01, 0x01, 0x01, 0x00, 0x00, /* 08..0f    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* 10..17    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* 18..1f    ........ */
	0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* 20..27     !"#$%&' */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* 28..2f    ()*+,-./ */
	0x0c, 0x0c, 0x0c, 0x0c, 0x0c, 0x0c, 0x0c, 0x0c, /* 30..37    01234567 */
	0x0c, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* 38..3f    89:;<=>? */

	0x00, 0x0a, 0x0a, 0x0a, 0x0a, 0x0a, 0x0a, 0x02, /* 40..47    @ABCDEFG */
	0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, /* 48..4f    HIJKLMNO */
	0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, /* 50..57    PQRSTUVW */
	0x02, 0x02, 0x02, 0x00, 0x00, 0x00, 0x00, 0x40, /* 58..5f    XYZ[\]^_ */
	0x00, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x2a, 0x22, /* 60..67    `abcdefg */
	0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, /* 68..6f    hijklmno */
	0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, /* 70..77    pqrstuvw */
	0x22, 0x22, 0x22, 0x00, 0x00, 0x00, 0x00, 0x00, /* 78..7f    xyz{|}~. */

	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* 80..87    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* 88..8f    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* 90..97    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* 98..9f    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* a0..a7    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* a8..af    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* b0..b7    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* b8..bf    ........ */

	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* c0..c7    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* c8..cf    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* d0..d7    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* d8..df    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* e0..e7    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* e8..ef    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* f0..f7    ........ */
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00  /* f8..ff    ........ */
};

/*!
 * ToLower table
 *
 */
const unsigned char Lexer::gsUpperToLower[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
	11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
	30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48,
	49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 97, 98, 99,
	100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114,
	115, 116, 117, 118, 119, 120, 121, 122, 91, 92, 93, 94, 95, 96, 97, 98, 99,
	100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114,
	115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129,
	130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144,
	145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159,
	160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174,
	175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189,
	190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204,
	205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219,
	220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234,
	235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249,
	250, 251, 252, 253, 254, 255};

/*!
 * ToUpper table
 *
 */
const unsigned char Lexer::gsLowerToUpper[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
	11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
	30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48,
	49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67,
	68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86,
	87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 65, 66, 67, 68, 69, 70, 71, 72, 73,
	74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 123,
	124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138,
	139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153,
	154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168,
	169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183,
	184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198,
	199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213,
	214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228,
	229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 241, 242, 243,
	244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255};

#include "kw.cpp"

/*!
 * Return the length of the token that begins at z[0].
 * Store the token type in *tokenType before returning.
 *
 * @param[in] z string to get token from.
 * @param[out] tokenType returned token type.
 *
 * @return token length
 */
inline int Lexer::_gsGetToken(const char *z, int *tokenType) {
	int i, c;
	unsigned char x = *z;

	if (x != 'X' && x != 'x' && x != '$' && !IsDigit(x) && IdChar(x)) {
	ALPHA:
		bool isExist = false;
		for (i = 1; IdChar(z[i]) || z[i] == '@' || z[i] == '#'; i++) {
			if (z[i] == '@' || z[i] == '#') {
				isExist = true;
			}
		}
		if (isExist) {
			*tokenType = TK_COLID;
		}
		else {
			*tokenType = keywordCode(reinterpret_cast<const char *>(z), i);
		}
		return i;
	}
	else if (IsSpace(x)) {
		for (i = 1; IsSpace(z[i]); i++) {
		}
		*tokenType = TK_SPACE;
		return i;
	}
	else if (x == '*') {
		*tokenType = TK_STAR;
		return 1;
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
			*tokenType = TK_SEMICOLON;
			return 1;
		}
		case '-': {
			if (z[1] == '-') {  
				for (i = 2; (c = z[i]) != 0 && c != '\n'; i++) {
				}
				*tokenType = TK_SPACE;
				return i;
			}
			*tokenType = TK_MINUS;
			return 1;
		}
		case '/': {
			if (z[1] != '*' || z[2] == 0) {
				*tokenType = TK_SLASH;
				return 1;
			}
			/* Syntax diagram for comments */
			for (i = 3, c = z[2]; (c != '*' || z[i] != '/') && (c = z[i]) != 0;
				 i++) {
			}
			if (c) i++;
			*tokenType = TK_SPACE;
			return i;
		}
		case '=': {
			*tokenType = TK_EQ;
			return 1 + (z[1] == '=');
		}
		case '<': {
			if ((c = z[1]) == '=') {
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
			if ((c = z[1]) == '=') {
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
			int delim = z[0];
			int idCharCheck = 0xFF;
			for (i = 1; (c = z[i]) != 0; i++) {
				if (c == delim) {
					if (z[i + 1] == delim) {
						i++;
					}
					else {
						break;
					}
				}
				else {
					idCharCheck &= IdChar(z[i]);
				}
			}
			if (c == '\'') {
				*tokenType = TK_STRING;
				return i + 1;
			}
			else if (c != 0 && idCharCheck) {
				*tokenType = TK_ID;
				return i + 1;
			}
			else {
				*tokenType = TK_ILLEGAL;
				if (c != 0) i++;
				return i;
			}
		}
		case '.': {
			if (!IsDigit(z[1])) {
				*tokenType = TK_DOT;
				return 1;
			}
		}

		case '0':
		case '1':
		case '2':
		case '3':
		case '4':
		case '5':
		case '6':
		case '7':
		case '8':
		case '9': {
			*tokenType = TK_INTEGER;
			for (i = 0; isdigit(z[i]); i++) {
			}
#ifndef SQLITE_OMIT_FLOATING_POINT
			if (z[i] == '.') {
				i++;
				while (isdigit(z[i])) {
					i++;
				}
				*tokenType = TK_FLOAT;
			}
			if ((z[i] == 'e' || z[i] == 'E') &&
				(isdigit(z[i + 1]) || ((z[i + 1] == '+' || z[i + 1] == '-') &&
										  isdigit(z[i + 2])))) {
				i += 2;
				while (isdigit(z[i])) {
					i++;
				}
				*tokenType = TK_FLOAT;
			}
#endif
			while (IdChar(z[i])) {
				*tokenType = TK_ILLEGAL;
				i++;
			}
			return i;
		}

#ifdef ADDITIONAL_FUNCTIONS_LEXER
		case '[': {
			for (i = 1, c = z[0]; c != ']' && (c = z[i]) != 0; i++) {
			}
			*tokenType = c == ']' ? TK_ID : TK_ILLEGAL;
			return i;
		}
		case '?': {
			*tokenType = TK_VARIABLE;
			for (i = 1; isdigit(z[i]); i++) {
			}
			return i;
		}
#ifndef SQLITE_OMIT_TCL_VARIABLE
		case '$':
#endif
		case '@': /* For compatibility with MS SQL Server */
		case ':': {
			int n = 0;
			testcase(z[0] == '$');
			testcase(z[0] == '@');
			testcase(z[0] == ':');
			*tokenType = TK_VARIABLE;
			for (i = 1; (c = z[i]) != 0; i++) {
				if (IdChar(c)) {
					n++;
#ifndef SQLITE_OMIT_TCL_VARIABLE
				}
				else if (c == '(' && n > 0) {
					do {
						i++;
					} while ((c = z[i]) != 0 && !isspace(c) && c != ')');
					if (c == ')') {
						i++;
					}
					else {
						*tokenType = TK_ILLEGAL;
					}
					break;
				}
				else if (c == ':' && z[i + 1] == ':') {
					i++;
#endif
				}
				else {
					break;
				}
			}
			if (n == 0) {
#ifdef TBE_OMIT_PLSQL
				*tokenType = TK_ILLEGAL;
#endif
			}
			return i;
		}
#endif

#ifndef SQLITE_OMIT_BLOB_LITERAL
		case 'x':
		case 'X': {
			testcase(z[0] == 'x');
			testcase(z[0] == 'X');
			if (z[1] == '\'') {
				*tokenType = TK_BLOB;
				for (i = 2; (c = z[i]) != 0 && c != '\''; i++) {
					if (!isxdigit(c)) {
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
#endif
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
 * @return token get is successful or not.
 */
int Lexer::gsGetToken(const char *str, Token &t) {
	if (str == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CRITICAL_LOGIC_ERROR,
			"Internal logic error: Token string is NULL");
	}
	t.minusflag = 0;
	if (str[0] == '\0') {
		t.id = TK_ILLEGAL;
		t.n = 0;
		t.z = str;
		return 0;
	}

	t.z = str;
	t.n = _gsGetToken(str, &t.id);

	return (t.n != 0);
}
