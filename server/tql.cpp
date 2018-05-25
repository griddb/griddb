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
	@brief C++ Driver template for the LEMON parser generator.
		   Just ported into C++ style.
*/
#include "tql.h"
/* The code below are written into .cpp file */

namespace lemon_tqlParser{
const tqlParser_YYACTIONTYPE tqlParser::yy_action[] = {
 /*     0 */   106,  129,   78,  105,   67,  130,  113,   16,   16,   15,
 /*    10 */    15,   15,   20,   21,   23,   72,   14,   15,   15,   15,
 /*    20 */     5,   76,   18,   18,   19,   19,   19,   19,  125,   17,
 /*    30 */    17,   17,   17,   16,   16,   15,   15,   15,  113,  109,
 /*    40 */    95,  102,    4,    2,   20,   21,   23,   72,   14,  108,
 /*    50 */    63,  128,  127,    8,   18,   18,   19,   19,   19,   19,
 /*    60 */    75,   17,   17,   17,   17,   16,   16,   15,   15,   15,
 /*    70 */     3,   91,    1,  113,   94,   93,   92,   47,  123,   20,
 /*    80 */    21,   23,   72,   14,  103,  101,   48,   29,  112,   18,
 /*    90 */    18,   19,   19,   19,   19,  102,   17,   17,   17,   17,
 /*   100 */    16,   16,   15,   15,   15,  113,   52,  100,  102,   52,
 /*   110 */    68,   96,   86,   23,   72,   14,   86,   56,   85,   84,
 /*   120 */    31,   18,   18,   19,   19,   19,   19,  107,   17,   17,
 /*   130 */    17,   17,   16,   16,   15,   15,   15,  113,  103,  101,
 /*   140 */    12,  111,   82,   46,  215,   25,   72,   14,  110,   12,
 /*   150 */   104,  103,  101,   18,   18,   19,   19,   19,   19,   73,
 /*   160 */    17,   17,   17,   17,   16,   16,   15,   15,   15,   90,
 /*   170 */    89,   88,   12,    8,    7,   22,   19,   19,   19,   19,
 /*   180 */    97,   17,   17,   17,   17,   16,   16,   15,   15,   15,
 /*   190 */    73,   81,   80,    9,   10,   62,   79,   12,   73,   30,
 /*   200 */    55,    6,   74,  117,   91,   24,   22,   66,   65,   92,
 /*   210 */    47,   54,  120,   53,   13,  122,  119,  118,  116,  115,
 /*   220 */   126,   32,  124,  114,    9,   10,   77,   34,  124,  114,
 /*   230 */    71,   70,    9,   10,  117,   99,   24,   49,   60,  121,
 /*   240 */    64,   98,  117,  120,   24,  121,  122,  119,  118,  116,
 /*   250 */   115,  120,   51,  216,  122,  119,  118,  116,  115,   11,
 /*   260 */    17,   17,   17,   17,   16,   16,   15,   15,   15,   17,
 /*   270 */    17,   17,   17,   16,   16,   15,   15,   15,   26,  124,
 /*   280 */   114,  216,   34,  124,  114,   69,   61,   38,  124,  114,
 /*   290 */    87,   39,  124,  114,   83,  216,  121,   37,  124,  114,
 /*   300 */   121,   36,  124,  114,  216,  121,   44,  124,  114,  121,
 /*   310 */   216,  216,   42,  124,  114,  121,   45,  124,  114,  121,
 /*   320 */    50,  124,  114,  216,  121,   59,  124,  114,  216,  216,
 /*   330 */   121,   40,  124,  114,  121,   41,  124,  114,  121,   28,
 /*   340 */   124,  114,  216,  121,   43,  124,  114,  216,  216,  121,
 /*   350 */    58,  124,  114,  121,   57,  124,  114,  121,   35,  124,
 /*   360 */   114,  216,  121,   27,  124,  114,  216,  216,  121,   33,
 /*   370 */   124,  114,  121,  216,  216,  216,  121,  216,  216,  216,
 /*   380 */   216,  121,  216,  216,  216,  216,  216,  121,
};
const tqlParser_YYCODETYPE tqlParser::yy_lookahead[] = {
 /*     0 */     5,    0,    1,    8,    4,    0,   11,   38,   39,   40,
 /*    10 */    41,   42,   17,   18,   19,   20,   21,   40,   41,   42,
 /*    20 */    28,   57,   27,   28,   29,   30,   31,   32,   57,   34,
 /*    30 */    35,   36,   37,   38,   39,   40,   41,   42,   11,   51,
 /*    40 */    40,    4,   50,   55,   17,   18,   19,   20,   21,   51,
 /*    50 */    80,   81,   82,   55,   27,   28,   29,   30,   31,   32,
 /*    60 */    58,   34,   35,   36,   37,   38,   39,   40,   41,   42,
 /*    70 */    54,   88,   50,   11,   91,   92,   93,   94,   51,   17,
 /*    80 */    18,   19,   20,   21,   47,   48,   12,   50,   11,   27,
 /*    90 */    28,   29,   30,   31,   32,    4,   34,   35,   36,   37,
 /*   100 */    38,   39,   40,   41,   42,   11,   88,   89,    4,   88,
 /*   110 */    89,   71,   57,   19,   20,   21,   61,   77,   63,   64,
 /*   120 */    46,   27,   28,   29,   30,   31,   32,   74,   34,   35,
 /*   130 */    36,   37,   38,   39,   40,   41,   42,   11,   47,   48,
 /*   140 */    96,   97,   38,   39,   78,   79,   20,   21,   51,   96,
 /*   150 */    74,   47,   48,   27,   28,   29,   30,   31,   32,    4,
 /*   160 */    34,   35,   36,   37,   38,   39,   40,   41,   42,   65,
 /*   170 */    66,   67,   96,   55,   55,   20,   29,   30,   31,   32,
 /*   180 */    51,   34,   35,   36,   37,   38,   39,   40,   41,   42,
 /*   190 */     4,   51,   51,   38,   39,   40,    3,   96,    4,   49,
 /*   200 */    83,   59,   53,   48,   88,   50,   20,   91,   92,   93,
 /*   210 */    94,   84,   57,   85,   20,   60,   61,   62,   63,   64,
 /*   220 */    86,   69,   70,   71,   38,   39,   56,   69,   70,   71,
 /*   230 */    72,   73,   38,   39,   48,   90,   50,   52,   90,   87,
 /*   240 */     2,   88,   48,   57,   50,   87,   60,   61,   62,   63,
 /*   250 */    64,   57,   88,   98,   60,   61,   62,   63,   64,   33,
 /*   260 */    34,   35,   36,   37,   38,   39,   40,   41,   42,   34,
 /*   270 */    35,   36,   37,   38,   39,   40,   41,   42,   69,   70,
 /*   280 */    71,   98,   69,   70,   71,   76,   73,   69,   70,   71,
 /*   290 */    95,   69,   70,   71,   95,   98,   87,   69,   70,   71,
 /*   300 */    87,   69,   70,   71,   98,   87,   69,   70,   71,   87,
 /*   310 */    98,   98,   69,   70,   71,   87,   69,   70,   71,   87,
 /*   320 */    69,   70,   71,   98,   87,   69,   70,   71,   98,   98,
 /*   330 */    87,   69,   70,   71,   87,   69,   70,   71,   87,   69,
 /*   340 */    70,   71,   98,   87,   69,   70,   71,   98,   98,   87,
 /*   350 */    69,   70,   71,   87,   69,   70,   71,   87,   69,   70,
 /*   360 */    71,   98,   87,   69,   70,   71,   98,   98,   87,   69,
 /*   370 */    70,   71,   87,   98,   98,   98,   87,   98,   98,   98,
 /*   380 */    98,   87,   98,   98,   98,   98,   98,   87,
};
const short tqlParser::yy_shift_ofst[] = {
 /*     0 */   238,  155,  186,  186,  104,  104,  186,  186,  186,  186,
 /*    10 */   186,  186,  186,  186,  194,  186,  186,  186,  186,  186,
 /*    20 */   186,  186,  186,  186,  186,   74,   -5,   -5,  226,   37,
 /*    30 */    37,    0,   27,   62,   62,   62,   94,   94,  126,  126,
 /*    40 */   147,  147,  147,  235,  235,  -31,   55,   55,   91,   91,
 /*    50 */   -23,  185,  185,  170,  149,  142,  150,  -37,  -37,  -37,
 /*    60 */    -8,   -2,  -12,    1,  193,  141,  140,   22,  129,  119,
 /*    70 */   118,   97,   77,   22,   16,  -29,    2,  -36,    5,
};
const short tqlParser::yy_reduce_ofst[] = {
 /*     0 */    66,  158,  213,  209,  116,  -17,  300,  294,  289,  285,
 /*    10 */   281,  275,  270,  266,  262,  256,  251,  247,  243,  237,
 /*    20 */   232,  228,  222,  218,  152,  -30,   76,   53,   44,   21,
 /*    30 */    18,   40,  101,  101,  101,  101,  101,  101,  101,  101,
 /*    40 */   101,  101,  101,  101,  101,  101,  199,  195,  164,  153,
 /*    50 */   101,  148,  145,  134,  128,  127,  117,  101,  101,  101,
};
const tqlParser_YYACTIONTYPE tqlParser::yy_default[] = {
 /*     0 */   132,  173,  214,  214,  204,  204,  214,  214,  214,  214,
 /*    10 */   214,  214,  214,  214,  214,  214,  214,  214,  214,  214,
 /*    20 */   214,  214,  214,  214,  214,  214,  155,  155,  212,  214,
 /*    30 */   214,  214,  214,  160,  175,  174,  182,  181,  180,  179,
 /*    40 */   205,  206,  184,  211,  183,  185,  214,  214,  214,  214,
 /*    50 */   186,  147,  147,  156,  149,  159,  143,  208,  207,  187,
 /*    60 */   188,  214,  214,  214,  134,  214,  214,  214,  214,  150,
 /*    70 */   172,  214,  214,  139,  214,  214,  157,  214,  214,  133,
 /*    80 */   192,  190,  203,  199,  202,  201,  200,  198,  197,  196,
 /*    90 */   195,  194,  193,  191,  189,  138,  137,  146,  148,  145,
 /*   100 */   144,  142,  141,  140,  152,  154,  153,  151,  178,  177,
 /*   110 */   171,  213,  210,  209,  176,  170,  169,  168,  167,  166,
 /*   120 */   165,  164,  163,  162,  161,  158,  136,  135,  131,
};

#ifdef tqlParser_YYFALLBACK
  const tqlParser_YYCODETYPE tqlParser::yyFallback[] = {
    0,  /*          $ => nothing */
    0,  /*  SEMICOLON => nothing */
    4,  /*    EXPLAIN => ID */
    0,  /*    ANALYZE => nothing */
    0,  /*         ID => nothing */
    4,  /*        ASC => ID */
    4,  /*       CAST => ID */
    4,  /*   COLUMNKW => ID */
    4,  /*       DESC => ID */
    4,  /*       EACH => ID */
    4,  /*  EXCLUSIVE => ID */
    4,  /*    LIKE_KW => ID */
    4,  /*     PRAGMA => ID */
    4,  /*    REPLACE => ID */
    4,  /*     RENAME => ID */
    4,  /*         IF => ID */
  };
#endif /* tqlParser_YYFALLBACK */

#ifndef NDEBUG
  /* For tracing shifts, the names of all terminals and nonterminals
  ** are required.  The following table supplies these names */
  const char *const tqlParser::yyTokenName[] = { 
  "$",             "SEMICOLON",     "EXPLAIN",       "ANALYZE",     
  "ID",            "ASC",           "CAST",          "COLUMNKW",    
  "DESC",          "EACH",          "EXCLUSIVE",     "LIKE_KW",     
  "PRAGMA",        "REPLACE",       "RENAME",        "IF",          
  "ANY",           "OR",            "XOR",           "AND",         
  "NOT",           "IS",            "MATCH",         "BETWEEN",     
  "IN",            "ISNULL",        "NOTNULL",       "NE",          
  "EQ",            "GT",            "LE",            "LT",          
  "GE",            "ESCAPE",        "BITAND",        "BITOR",       
  "LSHIFT",        "RSHIFT",        "PLUS",          "MINUS",       
  "STAR",          "SLASH",         "REM",           "CONCAT",      
  "COLLATE",       "BITNOT",        "SELECT",        "COLID",       
  "STRING",        "FROM",          "LP",            "RP",          
  "DOT",           "ORDER",         "BY",            "COMMA",       
  "LIMIT",         "INTEGER",       "OFFSET",        "WHERE",       
  "NULL",          "FLOAT",         "BLOB",          "NAN",         
  "INF",           "ON",            "DELETE",        "DEFAULT",     
  "error",         "expr",          "term",          "function",    
  "exprlist",      "nexprlist",     "sortorder",     "sortitem",    
  "sortlist",      "selcollist",    "ecmd",          "explain",     
  "cmd",           "select",        "oneselect",     "from",        
  "where_opt",     "orderby_opt",   "limit_opt",     "id",          
  "nm",            "seltablist",    "dbnm",          "nmnum",       
  "minus_num",     "plus_num",      "plus_opt",      "number",      
  "likeop",        "escape",      
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
 /*  11 */ "nm ::= COLID",
 /*  12 */ "nm ::= ID",
 /*  13 */ "nm ::= STRING",
 /*  14 */ "from ::=",
 /*  15 */ "from ::= FROM seltablist",
 /*  16 */ "seltablist ::= nm dbnm",
 /*  17 */ "seltablist ::= LP seltablist RP",
 /*  18 */ "dbnm ::=",
 /*  19 */ "dbnm ::= DOT nm",
 /*  20 */ "orderby_opt ::=",
 /*  21 */ "orderby_opt ::= ORDER BY sortlist",
 /*  22 */ "sortlist ::= sortlist COMMA expr sortorder",
 /*  23 */ "sortlist ::= expr sortorder",
 /*  24 */ "sortorder ::= ASC",
 /*  25 */ "sortorder ::= DESC",
 /*  26 */ "sortorder ::=",
 /*  27 */ "limit_opt ::=",
 /*  28 */ "limit_opt ::= LIMIT INTEGER",
 /*  29 */ "limit_opt ::= LIMIT INTEGER OFFSET INTEGER",
 /*  30 */ "where_opt ::=",
 /*  31 */ "where_opt ::= WHERE expr",
 /*  32 */ "expr ::= term",
 /*  33 */ "expr ::= LP expr RP",
 /*  34 */ "term ::= NULL",
 /*  35 */ "term ::= id",
 /*  36 */ "term ::= INTEGER",
 /*  37 */ "term ::= FLOAT",
 /*  38 */ "term ::= BLOB",
 /*  39 */ "term ::= STRING",
 /*  40 */ "term ::= NAN",
 /*  41 */ "term ::= INF",
 /*  42 */ "function ::= ID LP exprlist RP",
 /*  43 */ "exprlist ::= nexprlist",
 /*  44 */ "exprlist ::=",
 /*  45 */ "nexprlist ::= nexprlist COMMA expr",
 /*  46 */ "nexprlist ::= expr",
 /*  47 */ "expr ::= function",
 /*  48 */ "function ::= ID LP STAR RP",
 /*  49 */ "function ::= ID LP STAR COMMA nexprlist RP",
 /*  50 */ "expr ::= NOT expr",
 /*  51 */ "expr ::= expr AND expr",
 /*  52 */ "expr ::= expr XOR expr",
 /*  53 */ "expr ::= expr OR expr",
 /*  54 */ "expr ::= expr LT|GT|GE|LE expr",
 /*  55 */ "expr ::= expr EQ|NE expr",
 /*  56 */ "expr ::= expr BITAND|BITOR|LSHIFT|RSHIFT expr",
 /*  57 */ "expr ::= expr PLUS|MINUS expr",
 /*  58 */ "expr ::= expr STAR|SLASH|REM expr",
 /*  59 */ "cmd ::= PRAGMA nm dbnm",
 /*  60 */ "cmd ::= PRAGMA nm dbnm EQ nmnum",
 /*  61 */ "cmd ::= PRAGMA nm dbnm LP nmnum RP",
 /*  62 */ "cmd ::= PRAGMA nm dbnm EQ minus_num",
 /*  63 */ "cmd ::= PRAGMA nm dbnm LP minus_num RP",
 /*  64 */ "nmnum ::= plus_num",
 /*  65 */ "nmnum ::= nm",
 /*  66 */ "nmnum ::= ON",
 /*  67 */ "nmnum ::= DELETE",
 /*  68 */ "nmnum ::= DEFAULT",
 /*  69 */ "plus_num ::= plus_opt number",
 /*  70 */ "minus_num ::= MINUS number",
 /*  71 */ "number ::= INTEGER|FLOAT",
 /*  72 */ "number ::= NAN",
 /*  73 */ "number ::= INF",
 /*  74 */ "plus_opt ::= PLUS",
 /*  75 */ "plus_opt ::=",
 /*  76 */ "expr ::= expr IS expr",
 /*  77 */ "expr ::= expr IS NOT expr",
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
  { 78, 2 },
  { 78, 3 },
  { 80, 1 },
  { 79, 0 },
  { 79, 2 },
  { 79, 1 },
  { 81, 1 },
  { 82, 6 },
  { 77, 1 },
  { 77, 1 },
  { 87, 1 },
  { 88, 1 },
  { 88, 1 },
  { 88, 1 },
  { 83, 0 },
  { 83, 2 },
  { 89, 2 },
  { 89, 3 },
  { 90, 0 },
  { 90, 2 },
  { 85, 0 },
  { 85, 3 },
  { 76, 4 },
  { 76, 2 },
  { 74, 1 },
  { 74, 1 },
  { 74, 0 },
  { 86, 0 },
  { 86, 2 },
  { 86, 4 },
  { 84, 0 },
  { 84, 2 },
  { 69, 1 },
  { 69, 3 },
  { 70, 1 },
  { 70, 1 },
  { 70, 1 },
  { 70, 1 },
  { 70, 1 },
  { 70, 1 },
  { 70, 1 },
  { 70, 1 },
  { 71, 4 },
  { 72, 1 },
  { 72, 0 },
  { 73, 3 },
  { 73, 1 },
  { 69, 1 },
  { 71, 4 },
  { 71, 6 },
  { 69, 2 },
  { 69, 3 },
  { 69, 3 },
  { 69, 3 },
  { 69, 3 },
  { 69, 3 },
  { 69, 3 },
  { 69, 3 },
  { 69, 3 },
  { 80, 3 },
  { 80, 5 },
  { 80, 6 },
  { 80, 5 },
  { 80, 6 },
  { 91, 1 },
  { 91, 1 },
  { 91, 1 },
  { 91, 1 },
  { 91, 1 },
  { 93, 2 },
  { 92, 2 },
  { 95, 1 },
  { 95, 1 },
  { 95, 1 },
  { 94, 1 },
  { 94, 0 },
  { 69, 3 },
  { 69, 4 },
  { 69, 2 },
  { 69, 2 },
  { 96, 1 },
  { 96, 2 },
  { 97, 2 },
  { 97, 0 },
  { 69, 4 },
  };
  
  


  /* APPENDIX */
} 
