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
	@brief Defines for the lexer.
*/
#ifndef PARSERTOKEN_H
#define PARSERTOKEN_H
#define TK_SEMICOLON 1
#define TK_EXPLAIN 2
#define TK_ANALYZE 3
#define TK_ID 4
#define TK_ASC 5
#define TK_CAST 6
#define TK_COLUMNKW 7
#define TK_DESC 8
#define TK_EACH 9
#define TK_EXCLUSIVE 10
#define TK_LIKE_KW 11
#define TK_PRAGMA 12
#define TK_REPLACE 13
#define TK_RENAME 14
#define TK_IF 15
#define TK_ANY 16
#define TK_OR 17
#define TK_XOR 18
#define TK_AND 19
#define TK_NOT 20
#define TK_IS 21
#define TK_MATCH 22
#define TK_BETWEEN 23
#define TK_IN 24
#define TK_ISNULL 25
#define TK_NOTNULL 26
#define TK_NE 27
#define TK_EQ 28
#define TK_GT 29
#define TK_LE 30
#define TK_LT 31
#define TK_GE 32
#define TK_ESCAPE 33
#define TK_BITAND 34
#define TK_BITOR 35
#define TK_LSHIFT 36
#define TK_RSHIFT 37
#define TK_PLUS 38
#define TK_MINUS 39
#define TK_STAR 40
#define TK_SLASH 41
#define TK_REM 42
#define TK_CONCAT 43
#define TK_COLLATE 44
#define TK_BITNOT 45
#define TK_SELECT 46
#define TK_STRING 47
#define TK_COLID 48
#define TK_FROM 49
#define TK_LP 50
#define TK_RP 51
#define TK_DOT 52
#define TK_ORDER 53
#define TK_BY 54
#define TK_COMMA 55
#define TK_LIMIT 56
#define TK_INTEGER 57
#define TK_OFFSET 58
#define TK_WHERE 59
#define TK_NULL 60
#define TK_FLOAT 61
#define TK_BLOB 62
#define TK_NAN 63
#define TK_INF 64
#define TK_ON 65
#define TK_DELETE 66
#define TK_DEFAULT 67
#define TK_TO_TEXT 68
#define TK_TO_BLOB 69
#define TK_TO_NUMERIC 70
#define TK_TO_INT 71
#define TK_TO_REAL 72
#define TK_ISNOT 73
#define TK_END_OF_FILE 74
#define TK_ILLEGAL 75
#define TK_SPACE 76
#define TK_UNCLOSED_STRING 77
#define TK_FUNC 78
#define TK_COLUMN 79
#define TK_AGG_FUNCTION 80
#define TK_AGG_COLUMN 81
#define TK_CONST_FUNC 82
#define TK_UMINUS 83
#define TK_UPLUS 84
#define TK_GISFUNC 85
#endif
