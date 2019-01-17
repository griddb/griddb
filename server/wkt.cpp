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
/**
 * @file
 * @brief  C++ Driver template for the LEMON parser generator.
 *         Just ported into C++ style.
 *
 */
#include "wkt.h"
/* The code below are written into .cpp file */

namespace lemon_wktParser {
const wktParser_YYACTIONTYPE wktParser::yy_action[] = {
	/*     0 */ 21, 64, 67, 66, 64, 50, 46, 73, 33, 74,
	/*    10 */ 75, 37, 34, 76, 77, 39, 42, 61, 56, 72,
	/*    20 */ 28, 24, 1, 67, 66, 25, 71, 67, 66, 49,
	/*    30 */ 38, 44, 37, 29, 40, 41, 39, 42, 61, 56,
	/*    40 */ 27, 28, 69, 62, 24, 28, 67, 66, 3, 25,
	/*    50 */ 80, 67, 66, 27, 79, 45, 62, 40, 41, 39,
	/*    60 */ 42, 61, 56, 43, 28, 42, 61, 47, 31, 28,
	/*    70 */ 37, 23, 81, 67, 66, 23, 30, 67, 66, 23,
	/*    80 */ 78, 67, 66, 6, 35, 70, 39, 8, 40, 56,
	/*    90 */ 39, 28, 60, 56, 58, 28, 7, 57, 55, 28,
	/*   100 */ 36, 2, 85, 48, 5, 68, 27, 67, 66, 62,
	/*   110 */ 54, 44, 32, 11, 26, 67, 66, 22, 4, 67,
	/*   120 */ 66, 59, 52, 67, 66, 28, 65, 10, 51, 67,
	/*   130 */ 66, 117, 27, 28, 117, 62, 63, 28, 119, 120,
	/*   140 */ 120, 28, 120, 19, 120, 67, 66, 28, 120, 120,
	/*   150 */ 120, 18, 120, 67, 66, 17, 120, 67, 66, 16,
	/*   160 */ 120, 67, 66, 28, 120, 15, 120, 67, 66, 120,
	/*   170 */ 120, 28, 120, 120, 120, 28, 120, 120, 120, 28,
	/*   180 */ 120, 14, 120, 67, 66, 28, 120, 120, 120, 13,
	/*   190 */ 120, 67, 66, 12, 120, 67, 66, 53, 120, 67,
	/*   200 */ 66, 28, 120, 9, 120, 67, 66, 120, 120, 28,
	/*   210 */ 120, 120, 120, 28, 120, 120, 120, 28, 120, 20,
	/*   220 */ 120, 67, 66, 28, 120, 120, 120, 120, 120, 120,
	/*   230 */ 120, 120, 120, 120, 120, 120, 120, 120, 120, 28,
};
const wktParser_YYCODETYPE wktParser::yy_lookahead[] = {
	/*     0 */ 13, 7, 15, 16, 10, 18, 19, 20, 21, 22,
	/*    10 */ 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
	/*    20 */ 33, 13, 3, 15, 16, 13, 4, 15, 16, 4,
	/*    30 */ 22, 9, 24, 25, 26, 27, 28, 29, 30, 31,
	/*    40 */ 8, 33, 30, 11, 13, 33, 15, 16, 3, 13,
	/*    50 */ 0, 15, 16, 8, 7, 8, 11, 26, 27, 28,
	/*    60 */ 29, 30, 31, 27, 33, 29, 30, 4, 22, 33,
	/*    70 */ 24, 13, 0, 15, 16, 13, 6, 15, 16, 13,
	/*    80 */ 7, 15, 16, 3, 26, 4, 28, 9, 26, 31,
	/*    90 */ 28, 33, 4, 31, 4, 33, 9, 31, 4, 33,
	/*   100 */ 9, 3, 4, 5, 3, 13, 8, 15, 16, 11,
	/*   110 */ 4, 9, 9, 13, 3, 15, 16, 13, 3, 15,
	/*   120 */ 16, 13, 4, 15, 16, 33, 14, 13, 2, 15,
	/*   130 */ 16, 7, 8, 33, 10, 11, 14, 33, 17, 34,
	/*   140 */ 34, 33, 34, 13, 34, 15, 16, 33, 34, 34,
	/*   150 */ 34, 13, 34, 15, 16, 13, 34, 15, 16, 13,
	/*   160 */ 34, 15, 16, 33, 34, 13, 34, 15, 16, 34,
	/*   170 */ 34, 33, 34, 34, 34, 33, 34, 34, 34, 33,
	/*   180 */ 34, 13, 34, 15, 16, 33, 34, 34, 34, 13,
	/*   190 */ 34, 15, 16, 13, 34, 15, 16, 13, 34, 15,
	/*   200 */ 16, 33, 34, 13, 34, 15, 16, 34, 34, 33,
	/*   210 */ 34, 34, 34, 33, 34, 34, 34, 33, 34, 13,
	/*   220 */ 34, 15, 16, 33, 34, 34, 34, 34, 34, 34,
	/*   230 */ 34, 34, 34, 34, 34, 34, 34, 34, 34, 33,
};
const short wktParser::yy_shift_ofst[] = {
	/*     0 */ 126, 98, 45, 32, 32, 32, 32, 32, 32, 124,
	/*    10 */ 124, 124, 32, 32, 32, 32, 32, 32, 32, 32,
	/*    20 */ 32, 32, 32, 32, 32, 32, 115, -6, -6, 22,
	/*    30 */ 47, 118, 111, 103, 102, 106, 101, 91, 94, 87,
	/*    40 */ 90, 88, 78, 81, 80, 73, 70, 72, 63, 50,
	/*    50 */ 25, 19,
};
const short wktParser::yy_reduce_ofst[] = {
	/*     0 */ 121, -13, 8, 31, 62, 58, 36, 66, 12, 206,
	/*    10 */ 190, 108, 184, 180, 176, 168, 152, 146, 142, 138,
	/*    20 */ 130, 114, 108, 104, 100, 92, 46, 122, 112,
};
const wktParser_YYACTIONTYPE wktParser::yy_default[] = {
	/*     0 */ 118, 117, 117, 117, 117, 117, 117, 117, 117, 109,
	/*    10 */ 99, 99, 117, 117, 117, 117, 117, 117, 117, 117,
	/*    20 */ 117, 117, 117, 117, 117, 117, 118, 118, 118, 118,
	/*    30 */ 118, 118, 118, 100, 93, 118, 118, 103, 118, 106,
	/*    40 */ 118, 118, 96, 118, 118, 118, 82, 118, 118, 118,
	/*    50 */ 118, 118, 101, 110, 104, 102, 108, 107, 105, 109,
	/*    60 */ 95, 98, 116, 114, 115, 113, 112, 111, 99, 97,
	/*    70 */ 94, 92, 91, 90, 89, 88, 87, 86, 84, 83,
};

#ifdef wktParser_YYFALLBACK
const wktParser_YYCODETYPE wktParser::yyFallback[] = {};
#endif /* wktParser_YYFALLBACK */

#ifndef NDEBUG
/* For tracing shifts, the names of all terminals and nonterminals
** are required.  The following table supplies these names */
const char *const wktParser::yyTokenName[] = {
	"$", "ANY", "GISFUNC", "LP", "RP", "EMPTY", "SEMICOLON", "INTEGER", "MINUS",
	"COMMA", "FLOAT", "PLUS", "error", "signed", "number", "plus_num",
	"minus_num", "geom", "gisarg", "gisexpr", "gismultipolygon3d",
	"gisnpolygonlist3d", "gispolygon3d", "gispolygon2d", "gisnpointlist3d",
	"gisnpointlist2d", "gismultipoint3d", "gismultipoint2d", "gispointlist3d",
	"gispointlist2d", "gispoint2d", "gispoint3d", "gisqsf", "plus_opt",
};
#endif /* NDEBUG */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
 */
const char *const wktParser::yyRuleName[] = {
 /*   0 */ "geom ::= GISFUNC LP gisarg RP",
 /*   1 */ "geom ::= GISFUNC LP EMPTY RP",
 /*   2 */ "gisarg ::= gisexpr",
 /*   3 */ "gisarg ::= gisexpr SEMICOLON INTEGER",
 /*   4 */ "gisarg ::= gisexpr SEMICOLON MINUS INTEGER",
 /*   5 */ "gisarg ::=",
 /*   6 */ "gisexpr ::= gismultipoint2d",
 /*   7 */ "gisexpr ::= gismultipoint3d",
 /*   8 */ "gisexpr ::= gispolygon2d",
 /*   9 */ "gisexpr ::= gispolygon3d",
 /*  10 */ "gisexpr ::= gismultipolygon3d",
 /*  11 */ "gisexpr ::= gisqsf",
 /*  12 */ "gispolygon2d ::= LP gisnpointlist2d RP",
 /*  13 */ "gispolygon2d ::= gisnpointlist2d",
 /*  14 */ "gisnpointlist2d ::= gisnpointlist2d COMMA LP gismultipoint2d RP",
 /*  15 */ "gisnpointlist2d ::= LP gismultipoint2d RP",
 /*  16 */ "gismultipoint2d ::= gispointlist2d",
 /*  17 */ "gispointlist2d ::= gispointlist2d COMMA gispoint2d",
 /*  18 */ "gispointlist2d ::= gispoint2d",
 /*  19 */ "gispoint2d ::= signed signed",
 /*  20 */ "gismultipolygon3d ::= gisnpolygonlist3d",
 /*  21 */ "gisnpolygonlist3d ::= gisnpolygonlist3d COMMA LP gispolygon3d RP",
 /*  22 */ "gisnpolygonlist3d ::= LP gispolygon3d RP",
 /*  23 */ "gispolygon3d ::= gisnpointlist3d",
 /*  24 */ "gisnpointlist3d ::= gisnpointlist3d COMMA LP gismultipoint3d RP",
 /*  25 */ "gisnpointlist3d ::= LP gismultipoint3d RP",
 /*  26 */ "gismultipoint3d ::= gispointlist3d",
 /*  27 */ "gispointlist3d ::= gispointlist3d COMMA gispoint3d",
 /*  28 */ "gispointlist3d ::= gispoint3d",
 /*  29 */ "gispoint3d ::= signed signed signed",
 /*  30 */ "gisqsf ::= signed signed signed signed signed signed signed signed signed signed signed signed signed",
 /*  31 */ "signed ::= plus_num",
 /*  32 */ "signed ::= minus_num",
 /*  33 */ "plus_num ::= plus_opt number",
 /*  34 */ "minus_num ::= MINUS number",
 /*  35 */ "number ::= INTEGER|FLOAT",
 /*  36 */ "plus_opt ::= PLUS",
 /*  37 */ "plus_opt ::=",
  };
#endif /* NDEBUG */

const struct wktParser::RULEINFO wktParser::yyRuleInfo[] = {
	{17, 4}, {17, 4}, {18, 1}, {18, 3}, {18, 4}, {18, 0}, {19, 1}, {19, 1},
	{19, 1}, {19, 1}, {19, 1}, {19, 1}, {23, 3}, {23, 1}, {25, 5}, {25, 3},
	{27, 1}, {29, 3}, {29, 1}, {30, 2}, {20, 1}, {21, 5}, {21, 3}, {22, 1},
	{24, 5}, {24, 3}, {26, 1}, {28, 3}, {28, 1}, {31, 3}, {32, 13}, {13, 1},
	{13, 1}, {15, 2}, {16, 2}, {14, 1}, {33, 1}, {33, 0},
};

/* APPENDIX */
}  
