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
	@brief C++ Driver template for the LEMON parser generator.
		   Just ported into C++ style.
*/
#include "tql.h"
/* The code below are written into .cpp file */

namespace lemon_tqlParser {
const tqlParser_YYACTIONTYPE tqlParser::yy_action[] = {
	/*     0 */ 104, 48, 98, 103, 128, 100, 111, 14, 14, 13,
	/*    10 */ 13, 13, 18, 19, 21, 68, 13, 13, 13, 59,
	/*    20 */ 126, 125, 16, 16, 17, 17, 17, 17, 5, 15,
	/*    30 */ 15, 15, 15, 14, 14, 13, 13, 13, 111, 127,
	/*    40 */ 74, 72, 96, 71, 18, 19, 21, 68, 99, 101,
	/*    50 */ 4, 27, 48, 64, 16, 16, 17, 17, 17, 17,
	/*    60 */ 123, 15, 15, 15, 15, 14, 14, 13, 13, 13,
	/*    70 */ 87, 63, 105, 111, 90, 89, 88, 43, 121, 18,
	/*    80 */ 19, 21, 68, 107, 106, 95, 45, 2, 8, 16,
	/*    90 */ 16, 17, 17, 17, 17, 12, 15, 15, 15, 15,
	/*   100 */ 14, 14, 13, 13, 13, 111, 92, 91, 96, 12,
	/*   110 */ 109, 87, 52, 21, 68, 62, 61, 88, 43, 3,
	/*   120 */ 29, 16, 16, 17, 17, 17, 17, 102, 15, 15,
	/*   130 */ 15, 15, 14, 14, 13, 13, 13, 111, 213, 23,
	/*   140 */ 1, 110, 78, 42, 108, 75, 68, 8, 12, 93,
	/*   150 */ 12, 95, 7, 16, 16, 17, 17, 17, 17, 69,
	/*   160 */ 15, 15, 15, 15, 14, 14, 13, 13, 13, 86,
	/*   170 */ 85, 84, 77, 76, 51, 20, 17, 17, 17, 17,
	/*   180 */ 28, 15, 15, 15, 15, 14, 14, 13, 13, 13,
	/*   190 */ 69, 50, 6, 9, 10, 58, 32, 122, 112, 67,
	/*   200 */ 66, 49, 115, 70, 124, 22, 20, 73, 97, 46,
	/*   210 */ 56, 94, 118, 47, 119, 120, 117, 116, 114, 113,
	/*   220 */ 83, 24, 122, 112, 9, 10, 60, 214, 65, 214,
	/*   230 */ 79, 214, 214, 115, 82, 214, 22, 214, 82, 119,
	/*   240 */ 81, 80, 214, 118, 214, 214, 120, 117, 116, 114,
	/*   250 */ 113, 11, 15, 15, 15, 15, 14, 14, 13, 13,
	/*   260 */ 13, 214, 214, 32, 122, 112, 214, 57, 15, 15,
	/*   270 */ 15, 15, 14, 14, 13, 13, 13, 214, 214, 214,
	/*   280 */ 214, 119, 30, 122, 112, 36, 122, 112, 37, 122,
	/*   290 */ 112, 35, 122, 112, 34, 122, 112, 214, 214, 214,
	/*   300 */ 119, 214, 214, 119, 214, 214, 119, 214, 214, 119,
	/*   310 */ 214, 214, 119, 40, 122, 112, 38, 122, 112, 41,
	/*   320 */ 122, 112, 44, 122, 112, 55, 122, 112, 26, 122,
	/*   330 */ 112, 119, 214, 214, 119, 214, 214, 119, 214, 214,
	/*   340 */ 119, 214, 214, 119, 214, 214, 119, 214, 214, 214,
	/*   350 */ 214, 39, 122, 112, 54, 122, 112, 53, 122, 112,
	/*   360 */ 33, 122, 112, 25, 122, 112, 31, 122, 112, 119,
	/*   370 */ 214, 214, 119, 214, 214, 119, 214, 214, 119, 214,
	/*   380 */ 214, 119, 214, 214, 119,
};
const tqlParser_YYCODETYPE tqlParser::yy_lookahead[] = {
	/*     0 */ 5, 89, 90, 8, 0, 4, 11, 38, 39, 40,
	/*    10 */ 41, 42, 17, 18, 19, 20, 40, 41, 42, 80,
	/*    20 */ 81, 82, 27, 28, 29, 30, 31, 32, 28, 34,
	/*    30 */ 35, 36, 37, 38, 39, 40, 41, 42, 11, 0,
	/*    40 */ 1, 57, 4, 58, 17, 18, 19, 20, 47, 48,
	/*    50 */ 50, 50, 89, 90, 27, 28, 29, 30, 31, 32,
	/*    60 */ 57, 34, 35, 36, 37, 38, 39, 40, 41, 42,
	/*    70 */ 88, 4, 74, 11, 92, 93, 94, 95, 51, 17,
	/*    80 */ 18, 19, 20, 51, 51, 47, 12, 55, 55, 27,
	/*    90 */ 28, 29, 30, 31, 32, 97, 34, 35, 36, 37,
	/*   100 */ 38, 39, 40, 41, 42, 11, 71, 40, 4, 97,
	/*   110 */ 98, 88, 77, 19, 20, 92, 93, 94, 95, 54,
	/*   120 */ 46, 27, 28, 29, 30, 31, 32, 74, 34, 35,
	/*   130 */ 36, 37, 38, 39, 40, 41, 42, 11, 78, 79,
	/*   140 */ 50, 11, 38, 39, 51, 3, 20, 55, 97, 51,
	/*   150 */ 97, 47, 55, 27, 28, 29, 30, 31, 32, 4,
	/*   160 */ 34, 35, 36, 37, 38, 39, 40, 41, 42, 65,
	/*   170 */ 66, 67, 51, 51, 83, 20, 29, 30, 31, 32,
	/*   180 */ 49, 34, 35, 36, 37, 38, 39, 40, 41, 42,
	/*   190 */ 4, 84, 59, 38, 39, 40, 69, 70, 71, 72,
	/*   200 */ 73, 85, 47, 53, 86, 50, 20, 56, 91, 52,
	/*   210 */ 91, 88, 57, 88, 87, 60, 61, 62, 63, 64,
	/*   220 */ 96, 69, 70, 71, 38, 39, 2, 99, 76, 99,
	/*   230 */ 96, 99, 99, 47, 57, 99, 50, 99, 61, 87,
	/*   240 */ 63, 64, 99, 57, 99, 99, 60, 61, 62, 63,
	/*   250 */ 64, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	/*   260 */ 42, 99, 99, 69, 70, 71, 99, 73, 34, 35,
	/*   270 */ 36, 37, 38, 39, 40, 41, 42, 99, 99, 99,
	/*   280 */ 99, 87, 69, 70, 71, 69, 70, 71, 69, 70,
	/*   290 */ 71, 69, 70, 71, 69, 70, 71, 99, 99, 99,
	/*   300 */ 87, 99, 99, 87, 99, 99, 87, 99, 99, 87,
	/*   310 */ 99, 99, 87, 69, 70, 71, 69, 70, 71, 69,
	/*   320 */ 70, 71, 69, 70, 71, 69, 70, 71, 69, 70,
	/*   330 */ 71, 87, 99, 99, 87, 99, 99, 87, 99, 99,
	/*   340 */ 87, 99, 99, 87, 99, 99, 87, 99, 99, 99,
	/*   350 */ 99, 69, 70, 71, 69, 70, 71, 69, 70, 71,
	/*   360 */ 69, 70, 71, 69, 70, 71, 69, 70, 71, 87,
	/*   370 */ 99, 99, 87, 99, 99, 87, 99, 99, 87, 99,
	/*   380 */ 99, 87, 99, 99, 87,
};
const short tqlParser::yy_shift_ofst[] = {
	/*     0 */ 224, 155, 186, 186, 104, 104, 186, 186, 186, 186,
	/*    10 */ 186, 186, 186, 186, 186, 186, 186, 186, 186, 186,
	/*    20 */ 186, 186, 186, 74, -5, -5, 218, 1, 1, 67,
	/*    30 */ 27, 62, 62, 62, 94, 94, 126, 126, 147, 234,
	/*    40 */ 234, -31, 177, 177, -24, 38, 38, 157, 157, 151,
	/*    50 */ 150, 133, 131, -32, -32, -32, 0, 33, 32, 39,
	/*    60 */ 142, 122, 121, 90, 98, 97, 92, 93, 130, 90,
	/*    70 */ 65, 3, -15, -16, 4,
};
const short tqlParser::yy_reduce_ofst[] = {
	/*     0 */ 60, 127, 194, 152, 23, -18, 297, 294, 291, 288,
	/*    10 */ 285, 282, 259, 256, 253, 250, 247, 244, 225, 222,
	/*    20 */ 219, 216, 213, -61, 53, -2, 12, -37, -88, 35,
	/*    30 */ 51, 51, 51, 51, 51, 51, 51, 51, 51, 51,
	/*    40 */ 51, 51, 134, 124, 51, 125, 123, 119, 117, 118,
	/*    50 */ 116, 107, 91, 51, 51, 51,
};
const tqlParser_YYACTIONTYPE tqlParser::yy_default[] = {
	/*     0 */ 130, 173, 212, 212, 204, 204, 212, 212, 212, 212,
	/*    10 */ 212, 212, 212, 212, 212, 212, 212, 212, 212, 212,
	/*    20 */ 212, 212, 212, 212, 155, 155, 210, 212, 212, 212,
	/*    30 */ 212, 160, 175, 174, 182, 181, 180, 179, 184, 209,
	/*    40 */ 183, 185, 212, 212, 186, 212, 212, 147, 147, 156,
	/*    50 */ 149, 159, 143, 206, 205, 187, 188, 212, 212, 212,
	/*    60 */ 132, 212, 212, 212, 212, 150, 172, 212, 212, 137,
	/*    70 */ 212, 212, 157, 212, 212, 131, 192, 190, 203, 199,
	/*    80 */ 202, 201, 200, 198, 197, 196, 195, 194, 193, 191,
	/*    90 */ 189, 136, 135, 146, 148, 139, 138, 145, 144, 142,
	/*   100 */ 141, 140, 152, 154, 153, 151, 178, 177, 171, 211,
	/*   110 */ 208, 207, 176, 170, 169, 168, 167, 166, 165, 164,
	/*   120 */ 163, 162, 161, 158, 134, 133, 129,
};

#ifdef tqlParser_YYFALLBACK
const tqlParser_YYCODETYPE tqlParser::yyFallback[] = {
	0, /*          $ => nothing */
	0, /*  SEMICOLON => nothing */
	4, /*    EXPLAIN => ID */
	0, /*    ANALYZE => nothing */
	0, /*         ID => nothing */
	4, /*        ASC => ID */
	4, /*       CAST => ID */
	4, /*   COLUMNKW => ID */
	4, /*       DESC => ID */
	4, /*       EACH => ID */
	4, /*  EXCLUSIVE => ID */
	4, /*    LIKE_KW => ID */
	4, /*     PRAGMA => ID */
	4, /*    REPLACE => ID */
	4, /*     RENAME => ID */
	4, /*         IF => ID */
};
#endif /* tqlParser_YYFALLBACK */

#ifndef NDEBUG
/* For tracing shifts, the names of all terminals and nonterminals
** are required.  The following table supplies these names */
const char *const tqlParser::yyTokenName[] = {
	"$", "SEMICOLON", "EXPLAIN", "ANALYZE", "ID", "ASC", "CAST", "COLUMNKW",
	"DESC", "EACH", "EXCLUSIVE", "LIKE_KW", "PRAGMA", "REPLACE", "RENAME", "IF",
	"ANY", "OR", "XOR", "AND", "NOT", "IS", "MATCH", "BETWEEN", "IN", "ISNULL",
	"NOTNULL", "NE", "EQ", "GT", "LE", "LT", "GE", "ESCAPE", "BITAND", "BITOR",
	"LSHIFT", "RSHIFT", "PLUS", "MINUS", "STAR", "SLASH", "REM", "CONCAT",
	"COLLATE", "BITNOT", "SELECT", "STRING", "COLID", "FROM", "LP", "RP", "DOT",
	"ORDER", "BY", "COMMA", "LIMIT", "INTEGER", "OFFSET", "WHERE", "NULL",
	"FLOAT", "BLOB", "NAN", "INF", "ON", "DELETE", "DEFAULT", "error", "expr",
	"term", "function", "exprlist", "nexprlist", "sortorder", "sortitem",
	"sortlist", "selcollist", "ecmd", "explain", "cmd", "select", "oneselect",
	"from", "where_opt", "orderby_opt", "limit_opt", "id", "nm", "colnm",
	"seltablist", "dbnm", "nmnum", "minus_num", "plus_num", "plus_opt",
	"number", "likeop", "escape",
};
#endif /* NDEBUG */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
 */
const char *const tqlParser::yyRuleName[] = {
 /*   0 */ "ecmd ::= explain cmd",
 /*   1 */ "ecmd ::= explain cmd SEMICOLON",
 /*   2 */ "cmd ::= select",
 /*   3 */ "explain ::=",
 /*   4 */ "explain ::= EXPLAIN ANALYZE",
 /*   5 */ "explain ::= EXPLAIN",
 /*   6 */ "select ::= oneselect",
 /*   7 */ "oneselect ::= SELECT selcollist from where_opt orderby_opt limit_opt",
 /*   8 */ "selcollist ::= function",
 /*   9 */ "selcollist ::= STAR",
 /*  10 */ "id ::= ID",
 /*  11 */ "nm ::= ID",
 /*  12 */ "nm ::= STRING",
 /*  13 */ "colnm ::= COLID",
 /*  14 */ "colnm ::= ID",
 /*  15 */ "colnm ::= STRING",
 /*  16 */ "from ::=",
 /*  17 */ "from ::= FROM seltablist",
 /*  18 */ "seltablist ::= colnm dbnm",
 /*  19 */ "seltablist ::= LP seltablist RP",
 /*  20 */ "dbnm ::=",
 /*  21 */ "dbnm ::= DOT nm",
 /*  22 */ "orderby_opt ::=",
 /*  23 */ "orderby_opt ::= ORDER BY sortlist",
 /*  24 */ "sortlist ::= sortlist COMMA expr sortorder",
 /*  25 */ "sortlist ::= expr sortorder",
 /*  26 */ "sortorder ::= ASC",
 /*  27 */ "sortorder ::= DESC",
 /*  28 */ "sortorder ::=",
 /*  29 */ "limit_opt ::=",
 /*  30 */ "limit_opt ::= LIMIT INTEGER",
 /*  31 */ "limit_opt ::= LIMIT INTEGER OFFSET INTEGER",
 /*  32 */ "where_opt ::=",
 /*  33 */ "where_opt ::= WHERE expr",
 /*  34 */ "expr ::= term",
 /*  35 */ "expr ::= LP expr RP",
 /*  36 */ "term ::= NULL",
 /*  37 */ "term ::= id",
 /*  38 */ "term ::= INTEGER",
 /*  39 */ "term ::= FLOAT",
 /*  40 */ "term ::= BLOB",
 /*  41 */ "term ::= STRING",
 /*  42 */ "term ::= NAN",
 /*  43 */ "term ::= INF",
 /*  44 */ "function ::= ID LP exprlist RP",
 /*  45 */ "exprlist ::= nexprlist",
 /*  46 */ "exprlist ::=",
 /*  47 */ "nexprlist ::= nexprlist COMMA expr",
 /*  48 */ "nexprlist ::= expr",
 /*  49 */ "expr ::= function",
 /*  50 */ "function ::= ID LP STAR RP",
 /*  51 */ "function ::= ID LP STAR COMMA nexprlist RP",
 /*  52 */ "expr ::= NOT expr",
 /*  53 */ "expr ::= expr AND expr",
 /*  54 */ "expr ::= expr XOR expr",
 /*  55 */ "expr ::= expr OR expr",
 /*  56 */ "expr ::= expr LT|GT|GE|LE expr",
 /*  57 */ "expr ::= expr EQ|NE expr",
 /*  58 */ "expr ::= expr BITAND|BITOR|LSHIFT|RSHIFT expr",
 /*  59 */ "expr ::= expr PLUS|MINUS expr",
 /*  60 */ "expr ::= expr STAR|SLASH|REM expr",
 /*  61 */ "cmd ::= PRAGMA nm dbnm",
 /*  62 */ "cmd ::= PRAGMA nm dbnm EQ nmnum",
 /*  63 */ "cmd ::= PRAGMA nm dbnm LP nmnum RP",
 /*  64 */ "cmd ::= PRAGMA nm dbnm EQ minus_num",
 /*  65 */ "cmd ::= PRAGMA nm dbnm LP minus_num RP",
 /*  66 */ "nmnum ::= plus_num",
 /*  67 */ "nmnum ::= nm",
 /*  68 */ "nmnum ::= ON",
 /*  69 */ "nmnum ::= DELETE",
 /*  70 */ "nmnum ::= DEFAULT",
 /*  71 */ "plus_num ::= plus_opt number",
 /*  72 */ "minus_num ::= MINUS number",
 /*  73 */ "number ::= INTEGER|FLOAT",
 /*  74 */ "number ::= NAN",
 /*  75 */ "number ::= INF",
 /*  76 */ "plus_opt ::= PLUS",
 /*  77 */ "plus_opt ::=",
 /*  78 */ "expr ::= MINUS expr",
 /*  79 */ "expr ::= PLUS expr",
 /*  80 */ "likeop ::= LIKE_KW",
 /*  81 */ "likeop ::= NOT LIKE_KW",
 /*  82 */ "escape ::= ESCAPE expr",
 /*  83 */ "escape ::=",
 /*  84 */ "expr ::= expr likeop expr escape",
  };
#endif /* NDEBUG */

const struct tqlParser::RULEINFO tqlParser::yyRuleInfo[] = {
	{78, 2}, {78, 3}, {80, 1}, {79, 0}, {79, 2}, {79, 1}, {81, 1}, {82, 6},
	{77, 1}, {77, 1}, {87, 1}, {88, 1}, {88, 1}, {89, 1}, {89, 1}, {89, 1},
	{83, 0}, {83, 2}, {90, 2}, {90, 3}, {91, 0}, {91, 2}, {85, 0}, {85, 3},
	{76, 4}, {76, 2}, {74, 1}, {74, 1}, {74, 0}, {86, 0}, {86, 2}, {86, 4},
	{84, 0}, {84, 2}, {69, 1}, {69, 3}, {70, 1}, {70, 1}, {70, 1}, {70, 1},
	{70, 1}, {70, 1}, {70, 1}, {70, 1}, {71, 4}, {72, 1}, {72, 0}, {73, 3},
	{73, 1}, {69, 1}, {71, 4}, {71, 6}, {69, 2}, {69, 3}, {69, 3}, {69, 3},
	{69, 3}, {69, 3}, {69, 3}, {69, 3}, {69, 3}, {80, 3}, {80, 5}, {80, 6},
	{80, 5}, {80, 6}, {92, 1}, {92, 1}, {92, 1}, {92, 1}, {92, 1}, {94, 2},
	{93, 2}, {96, 1}, {96, 1}, {96, 1}, {95, 1}, {95, 0}, {69, 2}, {69, 2},
	{97, 1}, {97, 2}, {98, 2}, {98, 0}, {69, 4},
};

/* APPENDIX */
}  
