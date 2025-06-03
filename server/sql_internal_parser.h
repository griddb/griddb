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
 * @file   lemon-template.cpp
 *
 * @brief  C++ Driver template for the LEMON parser generator.
 *         Just ported into C++ style.
 *
 */

#ifndef LEMPAR_CPP_SQLParser_GENERATED_HPP
#define LEMPAR_CPP_SQLParser_GENERATED_HPP

#ifdef _WIN32
#pragma warning(disable:4065)
#endif
/* First off, code is included that follows the "include" declaration
 ** in the input grammar file. */

#include "sql_internal_parser_token.h"

#include "sql_parser_define.h"
#include "sql_parser.h"
#include "sql_processor.h" 
#include "util/container.h"
#include "util/time.h"

#include <vector>
#include <stdexcept>

#ifdef _WIN32
#define atoll atol
#endif

/*
** Disable all error recovery processing in the parser push-down
** automaton.
*/
#define YYNOERRORRECOVERY 1

#define PRINT(...)

#define SQLPARSER_CONTEXT (parser)
#define SQLPARSER_ALLOCATOR (SQLPARSER_CONTEXT->getSQLAllocator())
#define SQLPARSER_NEW SQL_PARSER_ALLOC_NEW(SQLPARSER_ALLOCATOR)
#define SQLPARSER_DELETE(x) SQL_PARSER_ALLOC_DELETE(SQLPARSER_ALLOCATOR, x)
#define SQLPARSER_SAFE_DELETE(x) SQL_PARSER_ALLOC_SAFE_DELETE(SQLPARSER_ALLOCATOR,x)

#define SQL_PARSER_THROW_ERROR(errorCode, token, message) \
	do { \
		size_t line = 0; \
		size_t column = 0; \
		SyntaxTree::countLineAndColumnFromToken(parser->inputSql_, token, line, column); \
		GS_THROW_USER_ERROR(errorCode, \
			message << " (line=" << line << ", column=" << column << ")"); \
	} \
	while (false)

typedef SyntaxTree::Expr Expr;
typedef SyntaxTree::ExprList ExprList;
typedef SyntaxTree::Select Select;
typedef SyntaxTree::QualifiedName QualifiedName;

struct LikeOp {
	SQLToken operator_;		/* "like"または"glob"または"regexp" */
	bool existNot_;			/* NOTがあればtrue */
};

struct IntervalOption {
	int64_t value_;			
	int64_t unit_;			
};

struct IntervalOptionDetail {
	int64_t offset_;
	int64_t timeZone_;
	bool withTimeZone_;
};

struct GroupIntervalOption {
	IntervalOption base_;
	IntervalOptionDetail detail_;
};

struct DateTimeField {
	static DateTimeField of(util::DateTime::FieldType unit, const SQLToken &token) {
		DateTimeField field;
		field.unit_ = unit;
		field.token_ = token;
		return field;
	}

	util::DateTime::FieldType unit_;
	SQLToken token_;
};

#include <iostream>

/* Next is all token values, in a form suitable for use by makeheaders.
 ** This section will be null unless lemon is run with the -m switch.
 */
/*
 ** These constants (all generated automatically by the parser generator)
 ** specify the various kinds of tokens (terminals) that the parser
 ** understands.
 **
 ** Each symbol here is a terminal symbol in the grammar.
 */

/* Make sure the INTERFACE macro is defined.
 */
#ifndef INTERFACE
# define INTERFACE 1
#endif
/* The next thing included is series of defines which control
 ** various aspects of the generated parser.
 **    SQLParser_YYCODETYPE         is the data type used for storing terminal
 **                       and nonterminal numbers.  "unsigned char" is
 **                       used if there are fewer than 250 terminals
 **                       and nonterminals.  "int" is used otherwise.
 **    SQLParser_YYNOCODE           is a number of type SQLParser_YYCODETYPE which corresponds
 **                       to no legal terminal or nonterminal number.  This
 **                       number is used to fill in empty slots of the hash
 **                       table.
 **    SQLParser_YYFALLBACK         If defined, this indicates that one or more tokens
 **                       have fall-back values which should be used if the
 **                       original value of the token will not parse.
 **    SQLParser_YYACTIONTYPE       is the data type used for storing terminal
 **                       and nonterminal numbers.  "unsigned char" is
 **                       used if there are fewer than 250 rules and
 **                       states combined.  "int" is used otherwise.
 **    SQLParserTOKENTYPE     is the data type used for minor tokens given
 **                       directly to the parser from the tokenizer.
 **    SQLParser_YYMINORTYPE        is the data type used for all minor tokens.
 **                       This is typically a union of many types, one of
 **                       which is SQLParserTOKENTYPE.  The entry in the union
 **                       for base tokens is called "yy0".
 **    SQLParser_YYSTACKDEPTH       is the maximum depth of the parser's stack.  If
 **                       zero the stack is dynamically sized using realloc()
 **    SQLParserARG_SDECL     A static variable declaration for the %extra_argument
 **    SQLParserARG_PDECL     A parameter declaration for the %extra_argument
 **    SQLParser_YYNSTATE           the combined number of states.
 **    SQLParser_YYNRULE            the number of rules in the grammar
 **    SQLParser_YYERRORSYMBOL      is the code number of the error symbol.  If not
 **                       defined, then do no error processing.
 */
#define SQLParser_YYCODETYPE unsigned short int
#define SQLParser_YYNOCODE 286
#define SQLParser_YYACTIONTYPE unsigned short int
#define SQLParser_YYWILDCARD 112
#define SQLParserTOKENTYPE SQLToken
typedef union {
  int yyinit;
  SQLParserTOKENTYPE yy0;
  SyntaxTree::WindowOption* yy19;
  ExprList* yy22;
  struct GroupIntervalOption yy61;
  SyntaxTree::SetOp yy67;
  SQLType::Id yy92;
  SyntaxTree::ColumnInfo* yy106;
  QualifiedName* yy142;
  TupleList::TupleColumnType yy161;
  SQLType::JoinType yy197;
  struct IntervalOptionDetail yy267;
  SyntaxTree::AggrOpt yy296;
  int32_t yy310;
  struct IntervalOption yy318;
  SyntaxTree::Select* yy336;
  SyntaxTree::WindowFrameBoundary* yy345;
  Expr* yy362;
  SyntaxTree::Set* yy372;
  DateTimeField yy380;
  SyntaxTree::PartitioningOption* yy441;
  SyntaxTree::TableColumnList* yy498;
  SyntaxTree::CreateIndexOption * yy515;
  SyntaxTree::TableColumn* yy516;
  SyntaxTree::WindowFrameOption* yy520;
  bool yy527;
  struct LikeOp yy528;
  int64_t yy549;
  uint8_t yy562;
} SQLParser_YYMINORTYPE;
#ifndef SQLParser_YYSTACKDEPTH
#define SQLParser_YYSTACKDEPTH 2000
#endif
#define SQLParserARG_SDECL SQLParserContext* parser
#define SQLParserARG_PDECL ,SQLParserContext* parser
#define SQLParserARG_STORE this->parser = parser
#define SQLParser_YYNSTATE 700
#define SQLParser_YYNRULE 322
#define SQLParser_YYFALLBACK 1
#define SQLParser_YY_NO_ACTION      (SQLParser_YYNSTATE+SQLParser_YYNRULE+2)
#define SQLParser_YY_ACCEPT_ACTION  (SQLParser_YYNSTATE+SQLParser_YYNRULE+1)
#define SQLParser_YY_ERROR_ACTION   (SQLParser_YYNSTATE+SQLParser_YYNRULE)

/* The yyzerominor constant is used to initialize instances of
 ** SQLParser_YYMINORTYPE objects to zero. */
static const SQLParser_YYMINORTYPE SQLParser_yyzerominor = { 0 };

/* Define the yytestcase() macro to be a no-op if is not already defined
 ** otherwise.
 **
 ** Applications can choose to define yytestcase() in the %include section
 ** to a macro that can assist in verifying code coverage.  For production
 ** code the yytestcase() macro should be turned off.  But it is useful
 ** for testing.
 */
#ifndef yytestcase
# define yytestcase(X)
#endif

/*!
 * @brief SQLParser
 *
 */
namespace lemon_SQLParser {

	/*!
	 * @brief SQLParser parser main
	 *
	 */
	class SQLParser {

		/* Next are the tables used to determine what action to take based on the
		 ** current state and lookahead token.  These tables are used to implement
		 ** functions that take a state number and lookahead value and return an
		 ** action integer.
		 **
		 ** Suppose the action integer is N.  Then the action is determined as
		 ** follows
		 **
		 **   0 <= N < SQLParser_YYNSTATE                  Shift N.  That is, push the lookahead
		 **                                      token onto the stack and goto state N.
		 **
		 **   SQLParser_YYNSTATE <= N < SQLParser_YYNSTATE+SQLParser_YYNRULE   Reduce by rule N-SQLParser_YYNSTATE.
		 **
		 **   N == SQLParser_YYNSTATE+SQLParser_YYNRULE              A syntax error has occurred.
		 **
		 **   N == SQLParser_YYNSTATE+SQLParser_YYNRULE+1            The parser accepts its input.
		 **
		 **   N == SQLParser_YYNSTATE+SQLParser_YYNRULE+2            No such action.  Denotes unused
		 **                                      slots in the yy_action[] table.
		 **
		 ** The action table is constructed as a single large table named yy_action[].
		 ** Given state S and lookahead X, the action is computed as
		 **
		 **      yy_action[ yy_shift_ofst[S] + X ]
		 **
		 ** If the index value yy_shift_ofst[S]+X is out of range or if the value
		 ** yy_lookahead[yy_shift_ofst[S]+X] is not equal to X or if yy_shift_ofst[S]
		 ** is equal to SQLParser_YY_SHIFT_USE_DFLT, it means that the action is not in the table
		 ** and that yy_default[S] should be used instead.
		 **
		 ** The formula above is for computing the action when the lookahead is
		 ** a terminal symbol.  If the lookahead is a non-terminal (as occurs after
		 ** a reduce action) then the yy_reduce_ofst[] array is used in place of
		 ** the yy_shift_ofst[] array and SQLParser_YY_REDUCE_USE_DFLT is used in place of
		 ** SQLParser_YY_SHIFT_USE_DFLT.
		 **
		 ** The following are the tables generated in this section:
		 **
		 **  yy_action[]        A single table containing all actions.
		 **  yy_lookahead[]     A table containing the lookahead for each entry in
		 **                     yy_action.  Used to detect hash collisions.
		 **  yy_shift_ofst[]    For each state, the offset into yy_action for
		 **                     shifting terminals.
		 **  yy_reduce_ofst[]   For each state, the offset into yy_action for
		 **                     shifting non-terminals after a reduce.
		 **  yy_default[]       Default action for each state.
		 */

#define SQLParser_YY_ACTTAB_COUNT (1806)
#define SQLParser_YY_SHIFT_USE_DFLT (-124)
#define SQLParser_YY_SHIFT_COUNT (490)
#define SQLParser_YY_SHIFT_MIN   (-123)
#define SQLParser_YY_SHIFT_MAX   (1620)
#define SQLParser_YY_REDUCE_USE_DFLT (-184)
#define SQLParser_YY_REDUCE_COUNT (310)
#define SQLParser_YY_REDUCE_MIN   (-183)
#define SQLParser_YY_REDUCE_MAX   (1314)
#define SQLParser_YYNFALLBACK  (112)

		static const SQLParser_YYACTIONTYPE yy_action[SQLParser_YY_ACTTAB_COUNT];
		static const SQLParser_YYCODETYPE yy_lookahead[SQLParser_YY_ACTTAB_COUNT];
		static const short yy_shift_ofst[SQLParser_YY_SHIFT_COUNT + 1];
		static const short yy_reduce_ofst[SQLParser_YY_REDUCE_COUNT + 1];
		static const SQLParser_YYACTIONTYPE yy_default[];




		/* The next table maps tokens into fallback tokens.  If a construct
		 ** like the following:
		 **
		 **      %fallback ID X Y Z.
		 **
		 ** appears in the grammar, then ID becomes a fallback token for X, Y,
		 ** and Z.  Whenever one of the tokens X, Y, or Z is input to the parser
		 ** but it does not parse, the type of the token is changed to ID and
		 ** the parse is retried before an error is thrown.
		 */
#ifdef SQLParser_YYFALLBACK
		static const SQLParser_YYCODETYPE yyFallback[SQLParser_YYNFALLBACK];
#endif /* SQLParser_YYFALLBACK */


		/* The following structure represents a single element of the
		 ** parser's stack.  Information stored includes:
		 **
		 **   +  The state number for the parser at this level of the stack.
		 **
		 **   +  The value of the token stored at this level of the stack.
		 **      (In other words, the "major" token.)
		 **
		 **   +  The semantic value stored at this level of the stack.  This is
		 **      the information used by the action routines in the grammar.
		 **      It is sometimes called the "minor" token.
		 */
		/*!
		 * @brief parser's stack
		 */
		struct yyStackEntry {
			SQLParser_YYACTIONTYPE stateno;  /* The state-number */
			SQLParser_YYCODETYPE major;      /* The major token value.  This is the code
										  ** number for the token at this stack level */
			SQLParser_YYMINORTYPE minor;     /* The user-supplied minor token value.  This
										  ** is the value of the token  */
		};
		typedef struct yyStackEntry yyStackEntry;

		/* The state of the parser is completely contained in an instance of
		 ** the following structure */

		int32_t yyidx;                    /* Index of top element in stack */
#ifdef SQLParser_YYTRACKMAXSTACKDEPTH
		int32_t yyidxMax;                 /* Maximum value of yyidx */
#endif
		int32_t yyerrcnt;                 /* Shifts left before out of the error */
		SQLParserARG_SDECL;               /* A place to hold %extra_argument */
#if SQLParser_YYSTACKDEPTH<=0
		int32_t yystksz;                  /* Current side of the stack */
		yyStackEntry *yystack;        /* The parser's stack */
#else
		yyStackEntry yystack[SQLParser_YYSTACKDEPTH];  /* The parser's stack */
#endif

#ifndef NDEBUG
		std::ostream *yyTraceFILE;
		const char* yyTracePrompt;
#endif /* NDEBUG */

#ifndef NDEBUG
		/*
		 ** Turn parser tracing on by giving a stream to which to write the trace
		 ** and a prompt to preface each trace message.  Tracing is turned off
		 ** by making either argument NULL
		 **
		 ** Inputs:
		 ** <ul>
		 ** <li> A FILE* to which trace output should be written.
		 **      If NULL, then tracing is turned off.
		 ** <li> A prefix string written at the beginning of every
		 **      line of trace output.  If NULL, then tracing is
		 **      turned off.
		 ** </ul>
		 **
		 ** Outputs:
		 ** None.
		 */
	public: void SQLParserSetTrace(std::ostream *TraceFILE, const char *zTracePrompt){
		yyTraceFILE = TraceFILE;
		yyTracePrompt = zTracePrompt;
		if( yyTraceFILE==0 ) yyTraceFILE = 0;
	}
#endif /* NDEBUG */


#ifndef NDEBUG
	protected:
		static const char *const yyTokenName[];
		static const char *const yyRuleName[SQLParser_YYNRULE];
#endif

#if SQLParser_YYSTACKDEPTH<=0
		/*
		 ** Try to increase the size of the parser stack.
		 */
		void yyGrowStack(){
			int32_t newSize;
			yyStackEntry *pNew;

			newSize = yystksz*2 + 100;
			pNew = new yyStackEntry[newSize];
			if( pNew ){
				memcpy(pNew, yystack, newSize*sizeof(pNew[0]));
				delete yystack;
				yystack = pNew;
				yystksz = newSize;
#ifndef NDEBUG
				if( yyTraceFILE ){
					*yyTraceFILE << yyTracePrompt << "Stack grows to " <<
					  yystksz <<" entries!" << std::endl;
				}
#endif
			}
		}
#endif

		/*
		 ** This function allocates a new parser.
		 ** The only argument is a pointer to a function which works like
		 ** malloc.
		 **
		 ** Inputs:
		 ** A pointer to the function used to allocate memory.
		 **
		 ** Outputs:
		 ** A pointer to a parser.  This pointer is used in subsequent calls
		 ** to SQLParser and SQLParserFree.
		 */
	public: SQLParser(){
		yyidx = -1;
#ifdef SQLParser_YYTRACKMAXSTACKDEPTH
		yidxMax = 0;
#endif
#if SQLParser_YYSTACKDEPTH<=0
		yystack = NULL;
		yystksz = 0;
		yyGrowStack();
#else
		memset(yystack, 0, sizeof(yystack));
#endif
#ifndef NDEBUG
		yyTraceFILE = NULL;
#endif
		yyerrcnt = -1;
	}

		/* The following function deletes the value associated with a
		 ** symbol.  The symbol can be either a terminal or nonterminal.
		 ** "yymajor" is the symbol code, and "yypminor" is a pointer to
		 ** the value.
		 */
		void yy_destructor(
			SQLParser_YYCODETYPE yymajor,     /* Type code for object to destroy */
			SQLParser_YYMINORTYPE *yypminor   /* The object to be destroyed */
			){
			switch( yymajor ){
				/* Here is inserted the actions which take place when a
				 ** terminal or non-terminal is destroyed.  This can happen
				 ** when the symbol is popped from the stack during a
				 ** reduce or during error processing or when a parser is
				 ** being destroyed before it is finished parsing.
				 **
				 ** Note: during a reduce, the only symbols destroyed are those
				 ** which appear on the RHS of the rule, but which are not used
				 ** inside the C code.
				 */
    case 202: /* columnlist */
    case 236: /* addcolumnlist */
{
SQLPARSER_SAFE_DELETE((yypminor->yy498));
}
      break;
    case 203: /* conslist_opt */
    case 205: /* createtable_opt */
    case 216: /* createtable_optlist */
    case 222: /* conslist */
    case 224: /* idxlist */
    case 233: /* exprlist */
    case 234: /* idxlist_opt */
    case 239: /* hint_opt */
    case 244: /* selcollist */
    case 249: /* orderby_opt */
    case 252: /* nexprlist */
    case 253: /* hintlist */
    case 254: /* hintlist1 */
    case 256: /* hintexprlist */
    case 257: /* hintexprlist1 */
    case 260: /* sclp */
    case 266: /* using_opt */
    case 267: /* idlist */
    case 268: /* sortlist */
    case 271: /* setlist */
    case 273: /* inscollist_opt */
    case 276: /* partitionby_opt */
    case 283: /* case_exprlist */
{
SQLPARSER_SAFE_DELETE((yypminor->yy22));
}
      break;
    case 206: /* partitioning_options */
{
SQLPARSER_SAFE_DELETE((yypminor->yy441));
}
      break;
    case 207: /* column */
    case 237: /* renamecolumn */
    case 238: /* addcolumn */
{
SQLPARSER_SAFE_DELETE((yypminor->yy516));
}
      break;
    case 209: /* carglist */
{
SQLPARSER_SAFE_DELETE((yypminor->yy106));
}
      break;
    case 210: /* expr */
    case 223: /* tcons */
    case 245: /* from */
    case 246: /* where_opt */
    case 248: /* having_opt */
    case 255: /* onehint */
    case 258: /* hintexpr */
    case 259: /* term */
    case 262: /* seltabtree */
    case 263: /* stl_prefix */
    case 265: /* on_opt */
    case 282: /* case_operand */
    case 284: /* case_else */
{
SQLPARSER_SAFE_DELETE((yypminor->yy362));
}
      break;
    case 228: /* fullname */
{
SQLPARSER_SAFE_DELETE((yypminor->yy142));
}
      break;
    case 231: /* select */
    case 240: /* selectnowith */
    case 251: /* valuelist */
{
SQLPARSER_SAFE_DELETE((yypminor->yy372));
}
      break;
    case 241: /* oneselect */
{
SQLPARSER_SAFE_DELETE((yypminor->yy336));
}
      break;
    case 247: /* groupby_opt */
{
SQLPARSER_SAFE_DELETE((yypminor->yy22));SQLPARSER_SAFE_DELETE((yypminor->yy22));
}
      break;
    case 275: /* window_opt */
{
SQLPARSER_SAFE_DELETE((yypminor->yy19));
}
      break;
    case 277: /* frame_opt */
{
SQLPARSER_SAFE_DELETE((yypminor->yy520));
}
      break;
    case 278: /* frame_boundary */
{
SQLPARSER_SAFE_DELETE((yypminor->yy345));
}
      break;
			default:  break;   /* If no destructor action specified: do nothing */
			}
		}

		/*
		 ** Pop the parser's stack once.
		 **
		 ** If there is a destructor routine associated with the token which
		 ** is popped from the stack, then call it.
		 **
		 ** Return the major token number for the symbol popped.
		 */
		int32_t yy_pop_parser_stack(){
			SQLParser_YYCODETYPE yymajor;
			yyStackEntry *yytos;

			if( yyidx<0 ) return 0;
			yytos = &yystack[yyidx];
#ifndef NDEBUG
			if( yyTraceFILE && yyidx>=0 ){
				*yyTraceFILE << yyTracePrompt << "Popping " << yyTokenName[yytos->major] << std::endl;
			}
#endif
			yymajor = yytos->major;
			yy_destructor(yymajor, &yytos->minor);
			yyidx--;
			return yymajor;
		}

		/*
		 ** Deallocate and destroy a parser.  Destructors are all called for
		 ** all stack elements before shutting the parser down.
		 **
		 ** Inputs:
		 ** <ul>
		 ** <li>  A pointer to the parser.  This should be a pointer
		 **       obtained from SQLParserAlloc.
		 ** <li>  A pointer to a function used to reclaim memory obtained
		 **       from malloc.
		 ** </ul>
		 */
	public: ~SQLParser(){
		while( yyidx>=0 ) yy_pop_parser_stack();
#if SQLParser_YYSTACKDEPTH<=0
		delete yystack;
		yystack = NULL;
#endif
	}

		/*
		 ** Return the peak depth of the stack for a parser.
		 */
#ifdef SQLParser_YYTRACKMAXSTACKDEPTH
	public: int32_t SQLParserStackPeak(void *p){
		return yyidxMax;
	}
#endif

		/*
		 ** Find the appropriate action for a parser given the terminal
		 ** look-ahead token iLookAhead.
		 **
		 ** If the look-ahead token is SQLParser_YYNOCODE, then check to see if the action is
		 ** independent of the look-ahead.  If it is, return the action, otherwise
		 ** return SQLParser_YY_NO_ACTION.
		 */
		int32_t yy_find_shift_action(
			SQLParser_YYCODETYPE iLookAhead     /* The look-ahead token */
			){
			int32_t i;
			int32_t stateno = this->yystack[this->yyidx].stateno;

			if( stateno>SQLParser_YY_SHIFT_COUNT
				|| (i = yy_shift_ofst[stateno])==SQLParser_YY_SHIFT_USE_DFLT ){
				return yy_default[stateno];
			}
			assert( iLookAhead!=SQLParser_YYNOCODE );
			i += iLookAhead;
			if( i<0 || i>=SQLParser_YY_ACTTAB_COUNT || yy_lookahead[i]!=iLookAhead ){
				if( iLookAhead>0 ){
#ifdef SQLParser_YYFALLBACK
					SQLParser_YYCODETYPE iFallback;            /* Fallback token */
					if( iLookAhead<SQLParser_YYNFALLBACK
						&& (iFallback = yyFallback[iLookAhead])!=0 ){
#ifndef NDEBUG
						if( yyTraceFILE ){
							*yyTraceFILE << yyTracePrompt << "FALLBACK " << yyTokenName[iLookAhead]
							  << " => " << yyTokenName[iFallback] << std::endl;
						}
#endif
						return yy_find_shift_action(iFallback);
					}
#endif
#ifdef SQLParser_YYWILDCARD
					{
						int32_t j = i - iLookAhead + SQLParser_YYWILDCARD;
						if(
#if SQLParser_YY_SHIFT_MIN+SQLParser_YYWILDCARD<0
							j>=0 &&
#endif
#if SQLParser_YY_SHIFT_MAX+SQLParser_YYWILDCARD>=SQLParser_YY_ACTTAB_COUNT
							j<SQLParser_YY_ACTTAB_COUNT &&
#endif
							yy_lookahead[j]==SQLParser_YYWILDCARD
							){
#ifndef NDEBUG
							if( yyTraceFILE ){
								*yyTraceFILE << yyTracePrompt << "WILDCARD "
								  << yyTokenName[iLookAhead]
									<< " => " << yyTokenName[SQLParser_YYWILDCARD]
									  << std::endl;
							}
#endif /* NDEBUG */
							return yy_action[j];
						}
					}
#endif /* SQLParser_YYWILDCARD */
				}
				return yy_default[stateno];
			}else{
				return yy_action[i];
			}
		}

		/*
		 ** Find the appropriate action for a parser given the non-terminal
		 ** look-ahead token iLookAhead.
		 **
		 ** If the look-ahead token is SQLParser_YYNOCODE, then check to see if the action is
		 ** independent of the look-ahead.  If it is, return the action, otherwise
		 ** return SQLParser_YY_NO_ACTION.
		 */
		int32_t yy_find_reduce_action(
			int32_t stateno,              /* Current state number */
			SQLParser_YYCODETYPE iLookAhead     /* The look-ahead token */
			){
			int32_t i;
#ifdef SQLParser_YYERRORSYMBOL
			if( stateno>SQLParser_YY_REDUCE_COUNT ){
				return yy_default[stateno];
			}
#else
			assert( stateno<=SQLParser_YY_REDUCE_COUNT );
#endif
			i = yy_reduce_ofst[stateno];
			assert( i!=SQLParser_YY_REDUCE_USE_DFLT );
			assert( iLookAhead!=SQLParser_YYNOCODE );
			i += iLookAhead;
#ifdef SQLParser_YYERRORSYMBOL
			if( i<0 || i>=SQLParser_YY_ACTTAB_COUNT || yy_lookahead[i]!=iLookAhead ){
				return yy_default[stateno];
			}
#else
			assert( i>=0 && i<SQLParser_YY_ACTTAB_COUNT );
			assert( yy_lookahead[i]==iLookAhead );
#endif
			return yy_action[i];
		}

		/*
		 ** The following routine is called if the stack overflows.
		 */
		void yyStackOverflow(SQLParser_YYMINORTYPE *yypMinor){
			UNUSED_VARIABLE(yypMinor);
			yyidx--;
#ifndef NDEBUG
			if( yyTraceFILE ){
				*yyTraceFILE << yyTracePrompt << "Stack Overflow!" << std::endl;
			}
#endif
			while( yyidx>=0 ) yy_pop_parser_stack();
			/* Here code is inserted which will execute if the parser
			 ** stack every overflows */

	parser->setError("parser stack overflow");
		}

		/*
		 ** Perform a shift action.
		 */
		void yy_shift(
			int32_t yyNewState,               /* The new state to shift in */
			int32_t yyMajor,                  /* The major token to shift in */
			SQLParser_YYMINORTYPE *yypMinor         /* Pointer to the minor token to shift in */
			){
			yyStackEntry *yytos;
			yyidx++;
#ifdef SQLParser_YYTRACKMAXSTACKDEPTH
			if( yyidx>yyidxMax ){
				yyidxMax = yyidx;
			}
#endif
#if SQLParser_YYSTACKDEPTH>0
			if( yyidx>=SQLParser_YYSTACKDEPTH ){
				yyStackOverflow(yypMinor);
				return;
			}
#else
			if( yyidx>=yystksz ){
				yyGrowStack();
				if( yyidx>=yystksz ){
					yyStackOverflow(yypMinor);
					return;
				}
			}
#endif
			yytos = &yystack[yyidx];
			yytos->stateno = (SQLParser_YYACTIONTYPE)yyNewState;
			yytos->major = (SQLParser_YYCODETYPE)yyMajor;
			yytos->minor = *yypMinor;
#ifndef NDEBUG
			if( yyTraceFILE && yyidx>0 ){
				int32_t i;
				*yyTraceFILE << yyTracePrompt << "Shift " << yyNewState << std::endl;
				*yyTraceFILE << yyTracePrompt << "Stack:";
				for(i=1; i<=yyidx; i++){
					*yyTraceFILE << " " << yyTokenName[yystack[i].major];
				}
				*yyTraceFILE << std::endl;
			}
#endif
		}

		/* The following table contains information about every rule that
		 ** is used during the reduce.
		 */
		/*!
      @brief Contains information about every rule
      that is used during the reduce
		 */
		static const struct RULEINFO{
			SQLParser_YYCODETYPE lhs;         /* Symbol on the left-hand side of the rule */
			unsigned char nrhs;     /* Number of right-hand side symbols in the rule */
		}yyRuleInfo[];

		/*
		 ** Perform a reduce action and the shift that must immediately
		 ** follow the reduce.
		 */
	private:
		void yy_reduce( int32_t yyruleno                 /* Number of the rule by which to reduce */
						){
			int32_t yygoto;                     /* The next state */
			int32_t yyact;                      /* The next action */
			SQLParser_YYMINORTYPE yygotominor;        /* The LHS of the rule reduced */
			yyStackEntry *yymsp;            /* The top of the parser's stack */
			int32_t yysize;                     /* Amount to pop the stack */
			yymsp = &yystack[yyidx];
#ifndef NDEBUG
			if( yyTraceFILE && yyruleno>=0
				&& yyruleno< SQLParser_YYNRULE ){
				*yyTraceFILE << yyTracePrompt << "Reduce ["
				  << yyRuleName[yyruleno] << "]." << std::endl;
			}
#endif /* NDEBUG */

			/* Silence complaints from purify about yygotominor being uninitialized
			 ** in some cases when it is copied into the stack after the following
			 ** switch.  yygotominor is uninitialized when a rule reduces that does
			 ** not set the value of its left-hand side nonterminal.  Leaving the
			 ** value of the nonterminal uninitialized is utterly harmless as long
			 ** as the value is never used.  So really the only thing this code
			 ** accomplishes is to quieten purify.
			 **
			 ** 2007-01-16:  The wireshark project (www.wireshark.org) reports that
			 ** without this code, their parser segfaults.  I'm not sure what there
			 ** parser is doing to make this happen.  This is the second bug report
			 ** from wireshark this week.  Clearly they are stressing Lemon in ways
			 ** that it has not been previously stressed...  (SQLite ticket #2172)
			 */
			/*memset(&yygotominor, 0, sizeof(yygotominor));*/
			yygotominor = SQLParser_yyzerominor;


			switch( yyruleno ){
				/* Beginning here are the reduction cases.  A typical example
				 ** follows:
				 **   case 0:
				 **  #line <lineno> <grammarfile>
				 **     { ... }           
				 **  #line <lineno> <thisfile>
				 **     break;
				 */
      case 5: /* explain ::= */
{ parser->beginParse(SyntaxTree::EXPLAIN_NONE); }
        break;
      case 6: /* explain ::= EXPLAIN */
{ parser->beginParse(SyntaxTree::EXPLAIN_PLAN); }
        break;
      case 7: /* explain ::= EXPLAIN ANALYZE */
{ parser->beginParse(SyntaxTree::EXPLAIN_ANALYZE); }
        break;
      case 8: /* cmdx ::= cmd */
{ parser->finishParse(); }
        break;
      case 9: /* cmd ::= BEGIN trans_opt */
{ parser->beginTransaction(0); }
        break;
      case 12: /* cmd ::= COMMIT trans_opt */
      case 13: /* cmd ::= END trans_opt */ yytestcase(yyruleno==13);
{ parser->commitTransaction(); }
        break;
      case 14: /* cmd ::= ROLLBACK trans_opt */
{ parser->rollbackTransaction(); }
        break;
      case 16: /* create_table ::= createkw TABLE ifnotexists nm dbnm LP columnlist conslist_opt RP is_timeseries createtable_opt partitioning_options */
{
	assert(!parser->topSelect_);
	SyntaxTree::Select *select = Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_CREATE_TABLE);

	SyntaxTree::QualifiedName* qName = NULL;
	SQLToken tokenName = yymsp[-8].minor.yy0;
	if (yymsp[-7].minor.yy0.size_ > 0) {
		qName = SyntaxTree::QualifiedName::makeQualifiedName(
				parser->getSQLAllocator(), parser->viewNsId_, &yymsp[-8].minor.yy0, &yymsp[-7].minor.yy0, NULL);
		tokenName = yymsp[-7].minor.yy0;
	}
	else {
		qName = SyntaxTree::QualifiedName::makeQualifiedName(
				parser->getSQLAllocator(), parser->viewNsId_, NULL, &yymsp[-8].minor.yy0, NULL);
	}
	if (!qName->table_) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, tokenName,
				"Table name must not be empty");
	}
	select->targetName_ = qName;

	select->cmdOptionValue_ = yymsp[-9].minor.yy310;

	if (yymsp[-4].minor.yy22) {
		SyntaxTree::checkTableConstraint(parser->getSQLAllocator(), yymsp[-5].minor.yy498, yymsp[-4].minor.yy22);
	}
	select->createTableOpt_ =
		SyntaxTree::makeCreateTableOption(
				parser->getSQLAllocator(), yymsp[-5].minor.yy498, yymsp[-4].minor.yy22, yymsp[0].minor.yy441, yymsp[-1].minor.yy22, yymsp[-9].minor.yy310, NULL, false, yymsp[-2].minor.yy527);

	parser->setTopSelect(select);
}
        break;
      case 18: /* ifnotexists ::= */
      case 72: /* ifexists ::= */ yytestcase(yyruleno==72);
      case 75: /* force ::= */ yytestcase(yyruleno==75);
      case 302: /* between_op ::= BETWEEN */ yytestcase(yyruleno==302);
      case 305: /* in_op ::= IN */ yytestcase(yyruleno==305);
{yygotominor.yy310 = 0;}
        break;
      case 19: /* ifnotexists ::= IF NOT EXISTS */
      case 71: /* ifexists ::= IF EXISTS */ yytestcase(yyruleno==71);
      case 76: /* force ::= FORCE */ yytestcase(yyruleno==76);
      case 303: /* between_op ::= NOT BETWEEN */ yytestcase(yyruleno==303);
      case 306: /* in_op ::= NOT IN */ yytestcase(yyruleno==306);
{yygotominor.yy310 = 1;}
        break;
      case 20: /* columnlist ::= column */
      case 106: /* addcolumnlist ::= addcolumn */ yytestcase(yyruleno==106);
{
	yygotominor.yy498 = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::TableColumnList(parser->getSQLAllocator());
	yygotominor.yy498->push_back(yymsp[0].minor.yy516);
}
        break;
      case 21: /* columnlist ::= columnlist COMMA column */
      case 107: /* addcolumnlist ::= addcolumnlist COMMA addcolumn */ yytestcase(yyruleno==107);
{
	yygotominor.yy498 = yymsp[-2].minor.yy498;
	if (!yygotominor.yy498) {
		yygotominor.yy498 = ALLOC_NEW(parser->getSQLAllocator())
				SyntaxTree::TableColumnList(parser->getSQLAllocator());
	}
	yygotominor.yy498->push_back(yymsp[0].minor.yy516);
}
        break;
      case 22: /* column ::= nm typetoken carglist */
      case 108: /* addcolumn ::= ADD nm typetoken carglist */ yytestcase(yyruleno==108);
      case 109: /* addcolumn ::= ADD COLUMNKW nm typetoken carglist */ yytestcase(yyruleno==109);
{
	yygotominor.yy516 = SyntaxTree::makeCreateTableColumn(parser->getSQLAllocator(), &yymsp[-2].minor.yy0, yymsp[-1].minor.yy161, yymsp[0].minor.yy106);
}
        break;
      case 23: /* carglist ::= */
{yygotominor.yy106 = 0;}
        break;
      case 24: /* carglist ::= carglist PRIMARY KEY */
{
	if (!yymsp[-2].minor.yy106) {
		yymsp[-2].minor.yy106 = ALLOC_NEW(parser->getSQLAllocator()) SyntaxTree::ColumnInfo;
	}
	int32_t flag = SyntaxTree::COLUMN_OPT_PRIMARY_KEY;
	if ((yymsp[-2].minor.yy106->option_ & flag) == flag) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-1].minor.yy0,
				"PRIMARY KEY option duplicated");
	}
	if ((yymsp[-2].minor.yy106->option_ & SyntaxTree::COLUMN_OPT_VIRTUAL) != 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-1].minor.yy0,
				"PRIMARY KEY and VIRTUAL options can not be specified at the same time");
	}
	if ((yymsp[-2].minor.yy106->option_ & SyntaxTree::COLUMN_OPT_NULLABLE) != 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-1].minor.yy0,
				"PRIMARY KEY and NULL options can not be specified at the same time");
	}
	yymsp[-2].minor.yy106->option_ |= static_cast<SyntaxTree::ColumnOption>(flag);
	yygotominor.yy106 = yymsp[-2].minor.yy106;
}
        break;
      case 25: /* carglist ::= carglist NOT NULL */
{
	if (!yymsp[-2].minor.yy106) {
		yymsp[-2].minor.yy106 = ALLOC_NEW(parser->getSQLAllocator()) SyntaxTree::ColumnInfo;
	}
	const int32_t flag = SyntaxTree::COLUMN_OPT_NOT_NULL;
	if ((yymsp[-2].minor.yy106->option_ & flag) == flag) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-1].minor.yy0,
				"NOT NULL option duplicated");
	}
	if ((yymsp[-2].minor.yy106->option_ & SyntaxTree::COLUMN_OPT_NULLABLE) != 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-1].minor.yy0,
				"NOT NULL and NULL options can not be specified at the same time");
	}
	yymsp[-2].minor.yy106->option_ |= static_cast<SyntaxTree::ColumnOption>(flag);
	yygotominor.yy106 = yymsp[-2].minor.yy106;
}
        break;
      case 26: /* carglist ::= carglist NULL */
{
	if (!yymsp[-1].minor.yy106) {
		yymsp[-1].minor.yy106 = ALLOC_NEW(parser->getSQLAllocator()) SyntaxTree::ColumnInfo;
	}
	const int32_t flag = SyntaxTree::COLUMN_OPT_NULLABLE;
	if ((yymsp[-1].minor.yy106->option_ & flag) == flag) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
				"NULL option duplicated");
	}
	if ((yymsp[-1].minor.yy106->option_ & SyntaxTree::COLUMN_OPT_PRIMARY_KEY) != 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
				"PRIMARY KEY and NULL options can not be specified at the same time");
	}
	if ((yymsp[-1].minor.yy106->option_ & SyntaxTree::COLUMN_OPT_NOT_NULL) != 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
				"NOT NULL and NULL options can not be specified at the same time");
	}
	yymsp[-1].minor.yy106->option_ |= static_cast<SyntaxTree::ColumnOption>(flag);
	yygotominor.yy106 = yymsp[-1].minor.yy106;
}
        break;
      case 27: /* carglist ::= carglist AS LP expr RP VIRTUAL */
{
	SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-4].minor.yy0,
			"Syntax error");
	yygotominor.yy106 = yymsp[-5].minor.yy106;
}
        break;
      case 28: /* partitioning_options ::= */
{yygotominor.yy441 = 0;}
        break;
      case 29: /* partitioning_options ::= PARTITION BY HASH singlecol PARTITIONS INTEGER */
{
	yygotominor.yy441 = ALLOC_NEW(parser->getSQLAllocator()) SyntaxTree::PartitioningOption;
	yygotominor.yy441->partitionType_ = SyntaxTree::TABLE_PARTITION_TYPE_HASH;
	yygotominor.yy441->partitionColumn_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[-2].minor.yy0);
	/* yymsp[0].minor.yy0: Token */
	int64_t int64Value;
	bool succeeded = SQLProcessor::ValueUtils::toLong(
			yymsp[0].minor.yy0.value_, yymsp[0].minor.yy0.size_, int64Value);
	if (!succeeded || int64Value <= 0 || int64Value > INT32_MAX) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
				"Table partitioning count must be a positive integer");
	}
	yygotominor.yy441->partitionCount_ = static_cast<int32_t>(int64Value);
	yygotominor.yy441->optInterval_ = 0;
}
        break;
      case 30: /* partitioning_options ::= PARTITION BY RANGE|INTERVAL singlecol intervalOption */
{
	yygotominor.yy441 = ALLOC_NEW(parser->getSQLAllocator()) SyntaxTree::PartitioningOption;
	yygotominor.yy441->partitionType_ = SyntaxTree::TABLE_PARTITION_TYPE_RANGE;
	yygotominor.yy441->partitionColumn_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[-1].minor.yy0);
	yygotominor.yy441->optInterval_ = yymsp[0].minor.yy318.value_;
	yygotominor.yy441->optIntervalUnit_ = yymsp[0].minor.yy318.unit_;
}
        break;
      case 31: /* partitioning_options ::= PARTITION BY RANGE|INTERVAL singlecol intervalOption SUBPARTITION BY HASH singlecol SUBPARTITIONS INTEGER */
{

	yygotominor.yy441 = ALLOC_NEW(parser->getSQLAllocator()) SyntaxTree::PartitioningOption;
	yygotominor.yy441->partitionType_ = SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH;
	yygotominor.yy441->partitionColumn_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[-7].minor.yy0);
	yygotominor.yy441->subPartitionColumn_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[-2].minor.yy0);
	/* yymsp[0].minor.yy0: Token */
	int64_t int64Value;
	bool succeeded = SQLProcessor::ValueUtils::toLong(
			yymsp[0].minor.yy0.value_, yymsp[0].minor.yy0.size_, int64Value);
	if (!succeeded || int64Value <= 0 || int64Value > INT32_MAX) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
				"Table partitioning count must be a positive integer");
	}
	yygotominor.yy441->partitionCount_ = static_cast<int32_t>(int64Value);
	yygotominor.yy441->optInterval_ = yymsp[-6].minor.yy318.value_;
	yygotominor.yy441->optIntervalUnit_ = yymsp[-6].minor.yy318.unit_;
}
        break;
      case 32: /* intervalOption ::= EVERY LP INTEGER RP */
{
	/* yymsp[-1].minor.yy0: Token */
	int64_t int64Value;
	bool succeeded = SQLProcessor::ValueUtils::toLong(
			yymsp[-1].minor.yy0.value_, yymsp[-1].minor.yy0.size_, int64Value);
	if (!succeeded || int64Value <= 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-1].minor.yy0,
				"Table partitioning interval must be a positive integer");
	}
	yygotominor.yy318.value_ = int64Value;
	yygotominor.yy318.unit_ = -1;
}
        break;
      case 33: /* intervalOption ::= EVERY LP INTEGER COMMA fieldType RP */
{
	const util::DateTime::FieldType unit = yymsp[-1].minor.yy380.unit_;
	const SQLToken &unitToken = yymsp[-1].minor.yy380.token_;
	if (!SyntaxTree::checkPartitioningIntervalTimeField(unit)) {
		SQL_PARSER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, unitToken,
				"Specified unit is not allowed");
	}

	/* yymsp[-3].minor.yy0: Token */
	int64_t int64Value;
	bool succeeded = SQLProcessor::ValueUtils::toLong(
			yymsp[-3].minor.yy0.value_, yymsp[-3].minor.yy0.size_, int64Value);
	if (!succeeded || int64Value <= 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-3].minor.yy0,
				"Table partitioning interval must be a positive integer");
	}

	yygotominor.yy318.value_ = int64Value;
	yygotominor.yy318.unit_ = static_cast<int64_t>(unit);
}
        break;
      case 34: /* groupIntervalOption ::= EVERY LP INTEGER COMMA fieldType intervalOptionDetail RP */
{
	const util::DateTime::FieldType unit = yymsp[-2].minor.yy380.unit_;
	const SQLToken &unitToken = yymsp[-2].minor.yy380.token_;
	if (!SyntaxTree::checkGroupIntervalTimeField(unit)) {
		SQL_PARSER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, unitToken,
				"Specified unit is not allowed");
	}

	/* yymsp[-4].minor.yy0: Token */
	int64_t int64Value;
	bool succeeded = SQLProcessor::ValueUtils::toLong(
			yymsp[-4].minor.yy0.value_, yymsp[-4].minor.yy0.size_, int64Value);
	if (!succeeded || int64Value <= 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-4].minor.yy0,
				"Range group interval must be a positive integer");
	}

	yygotominor.yy61.base_.value_ = int64Value;
	yygotominor.yy61.base_.unit_ = static_cast<int64_t>(unit);
	yygotominor.yy61.detail_ = yymsp[-1].minor.yy267;
}
        break;
      case 35: /* intervalOptionDetail ::= */
{
	yygotominor.yy267.offset_ = 0;
	yygotominor.yy267.timeZone_ = 0;
	yygotominor.yy267.withTimeZone_ = false;
}
        break;
      case 36: /* intervalOptionDetail ::= COMMA INTEGER */
{
	if (!SyntaxTree::resolveRangeGroupOffset(yymsp[0].minor.yy0, yygotominor.yy267.offset_)) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
				"Range group offset must be a non negetive integer");
	}
	yygotominor.yy267.timeZone_ = 0;
	yygotominor.yy267.withTimeZone_ = false;
}
        break;
      case 37: /* intervalOptionDetail ::= COMMA STRING */
{
	yygotominor.yy267.offset_ = 0;
	if (!SyntaxTree::resolveRangeGroupTimeZone(
			parser->getSQLAllocator(), yymsp[0].minor.yy0, yygotominor.yy267.timeZone_)) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
				"Syntax error at range group time zone");
	}
	yygotominor.yy267.withTimeZone_ = true;
}
        break;
      case 38: /* intervalOptionDetail ::= COMMA INTEGER COMMA STRING */
{
	if (!SyntaxTree::resolveRangeGroupOffset(yymsp[-2].minor.yy0, yygotominor.yy267.offset_)) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-2].minor.yy0,
				"Range group offset must be a non negetive integer");
	}
	if (!SyntaxTree::resolveRangeGroupTimeZone(
			parser->getSQLAllocator(), yymsp[0].minor.yy0, yygotominor.yy267.timeZone_)) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
				"Syntax error at range group time zone");
	}
	yygotominor.yy267.withTimeZone_ = true;
}
        break;
      case 39: /* is_timeseries ::= */
      case 97: /* sortascending ::= DESC */ yytestcase(yyruleno==97);
{yygotominor.yy527 = false;}
        break;
      case 40: /* is_timeseries ::= USING TIMESERIES */
      case 96: /* sortascending ::= ASC */ yytestcase(yyruleno==96);
      case 98: /* sortascending ::= */ yytestcase(yyruleno==98);
{yygotominor.yy527 = true;}
        break;
      case 41: /* singlecol ::= nm */
      case 49: /* plus_num ::= PLUS INTEGER|FLOAT */ yytestcase(yyruleno==49);
      case 50: /* plus_num ::= INTEGER|FLOAT */ yytestcase(yyruleno==50);
      case 51: /* minus_num ::= MINUS INTEGER|FLOAT */ yytestcase(yyruleno==51);
      case 52: /* nm ::= ID */ yytestcase(yyruleno==52);
      case 58: /* typename ::= ID */ yytestcase(yyruleno==58);
      case 150: /* as ::= AS nm */ yytestcase(yyruleno==150);
      case 151: /* as ::= ID */ yytestcase(yyruleno==151);
      case 162: /* dbnm ::= DOT nm */ yytestcase(yyruleno==162);
{yygotominor.yy0 = yymsp[0].minor.yy0;}
        break;
      case 42: /* singlecol ::= LP nm RP */
{yygotominor.yy0 = yymsp[-1].minor.yy0;}
        break;
      case 43: /* createtable_opt ::= */
      case 62: /* conslist_opt ::= */ yytestcase(yyruleno==62);
      case 125: /* hint_opt ::= */ yytestcase(yyruleno==125);
      case 127: /* hintlist ::= */ yytestcase(yyruleno==127);
      case 132: /* hintexprlist ::= */ yytestcase(yyruleno==132);
      case 185: /* using_opt ::= */ yytestcase(yyruleno==185);
      case 186: /* orderby_opt ::= */ yytestcase(yyruleno==186);
      case 190: /* groupby_opt ::= */ yytestcase(yyruleno==190);
      case 215: /* inscollist_opt ::= */ yytestcase(yyruleno==215);
      case 254: /* partitionby_opt ::= */ yytestcase(yyruleno==254);
      case 319: /* exprlist ::= */ yytestcase(yyruleno==319);
{yygotominor.yy22 = 0;}
        break;
      case 44: /* createtable_opt ::= WITH LP createtable_optlist RP */
{
	yygotominor.yy22 = yymsp[-1].minor.yy22;
}
        break;
      case 45: /* createtable_optlist ::= createtable_optlist COMMA nm EQ expr */
{
	assert(yymsp[0].minor.yy362);
	if (yymsp[0].minor.yy362->op_ != SQLType::EXPR_CONSTANT) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy362->startToken_,
				"Option value must be constant");
	}
	if (!yymsp[-4].minor.yy22) {
		yymsp[-4].minor.yy22 = ALLOC_NEW(parser->getSQLAllocator())
				SyntaxTree::ExprList(parser->getSQLAllocator());
	}
	util::String *keyStr = SyntaxTree::tokenToString(
			parser->getSQLAllocator(), yymsp[-2].minor.yy0, false);
	yymsp[0].minor.yy362->aliasName_ = keyStr;

	yygotominor.yy22 = yymsp[-4].minor.yy22;
	yygotominor.yy22->push_back(yymsp[0].minor.yy362);
}
        break;
      case 46: /* createtable_optlist ::= nm EQ expr */
{
	assert(yymsp[0].minor.yy362);
	if (yymsp[0].minor.yy362->op_ != SQLType::EXPR_CONSTANT) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy362->startToken_,
				"Option value must be constant");
	}
	yygotominor.yy22 = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());

	util::String *keyStr = SyntaxTree::tokenToString(
			parser->getSQLAllocator(), yymsp[-2].minor.yy0, false);
	yymsp[0].minor.yy362->aliasName_ = keyStr;

	yygotominor.yy22->push_back(yymsp[0].minor.yy362);
}
        break;
      case 48: /* create_virtual_table ::= createkw VIRTUAL TABLE ifnotexists nm dbnm LP columnlist conslist_opt RP USING nm createtable_opt partitioning_options */
{
	SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-12].minor.yy0,
			"Syntax error");
}
        break;
      case 53: /* typetoken ::= typename */
{
	yygotominor.yy161 = SyntaxTree::toColumnType(parser->getSQLAllocator(), yymsp[0].minor.yy0, NULL, NULL);
}
        break;
      case 54: /* typetoken ::= typename LP signed RP */
{
	yygotominor.yy161 = SyntaxTree::toColumnType(parser->getSQLAllocator(), yymsp[-3].minor.yy0, &yymsp[-1].minor.yy549, NULL);
}
        break;
      case 55: /* typetoken ::= typename LP signed RP typename */
{
	yygotominor.yy161 = SyntaxTree::toColumnType(parser->getSQLAllocator(), yymsp[-4].minor.yy0, &yymsp[-2].minor.yy549, NULL);
}
        break;
      case 56: /* typetoken ::= typename LP signed COMMA signed RP */
{
	yygotominor.yy161 = SyntaxTree::toColumnType(parser->getSQLAllocator(), yymsp[-5].minor.yy0, &yymsp[-3].minor.yy549, &yymsp[-1].minor.yy549);
}
        break;
      case 57: /* typetoken ::= typename LP signed COMMA signed RP typename */
{
	yygotominor.yy161 = SyntaxTree::toColumnType(parser->getSQLAllocator(), yymsp[-6].minor.yy0, &yymsp[-4].minor.yy549, &yymsp[-2].minor.yy549);
}
        break;
      case 59: /* typename ::= typename ID */
{
	yygotominor.yy0 = yymsp[-1].minor.yy0;
}
        break;
      case 60: /* signed ::= plus_num */
{
	if (!SyntaxTree::toSignedValue(yymsp[0].minor.yy0, false, yygotominor.yy549)) {
		SQL_PARSER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				yymsp[0].minor.yy0, "Invalid number for column type option");
	}
}
        break;
      case 61: /* signed ::= minus_num */
{
	if (!SyntaxTree::toSignedValue(yymsp[0].minor.yy0, true, yygotominor.yy549)) {
		SQL_PARSER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				yymsp[0].minor.yy0, "Invalid number for column type option");
	}
}
        break;
      case 63: /* conslist_opt ::= COMMA conslist */
      case 126: /* hintlist ::= hintlist1 */ yytestcase(yyruleno==126);
      case 131: /* hintexprlist ::= hintexprlist1 */ yytestcase(yyruleno==131);
      case 187: /* orderby_opt ::= ORDER BY sortlist */ yytestcase(yyruleno==187);
      case 191: /* groupby_opt ::= GROUP BY nexprlist */ yytestcase(yyruleno==191);
      case 255: /* partitionby_opt ::= PARTITION BY nexprlist */ yytestcase(yyruleno==255);
      case 318: /* exprlist ::= nexprlist */ yytestcase(yyruleno==318);
{yygotominor.yy22 = yymsp[0].minor.yy22;}
        break;
      case 64: /* conslist ::= tcons */
{
	if (yymsp[0].minor.yy362) {
		yygotominor.yy22 = ALLOC_NEW(parser->getSQLAllocator())
				SyntaxTree::ExprList(parser->getSQLAllocator());
		yygotominor.yy22->push_back(yymsp[0].minor.yy362);
	}
	else {
		yygotominor.yy22 = 0;
	}
}
        break;
      case 65: /* tcons ::= PRIMARY KEY LP idxlist RP */
{
	SyntaxTree::Expr* expr = SyntaxTree::Expr::makeExpr(
		parser->getSQLAllocator(), SQLType::EXPR_COLUMN);
	expr->next_ = yymsp[-1].minor.yy22;
	yygotominor.yy362 = expr;
}
        break;
      case 66: /* orconf ::= */
{yygotominor.yy310 = SyntaxTree::RESOLVETYPE_DEFAULT;}
        break;
      case 67: /* orconf ::= OR resolvetype */
{yygotominor.yy310 = yymsp[0].minor.yy310;}
        break;
      case 68: /* resolvetype ::= IGNORE */
{yygotominor.yy310 = SyntaxTree::RESOLVETYPE_IGNORE;}
        break;
      case 69: /* resolvetype ::= REPLACE */
{yygotominor.yy310 = SyntaxTree::RESOLVETYPE_REPLACE;}
        break;
      case 70: /* cmd ::= DROP TABLE ifexists fullname */
{
	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_DROP_TABLE);

	select->targetName_ = yymsp[0].minor.yy142;
	select->cmdOptionValue_ = yymsp[-1].minor.yy310;
	parser->setTopSelect(select);
}
        break;
      case 74: /* create_view ::= CREATE force VIEW nm dbnm AS select */
{
	assert(!parser->topSelect_);
	SyntaxTree::Select *select = Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_CREATE_VIEW);

	SyntaxTree::QualifiedName* qName = NULL;
	SQLToken tokenName = yymsp[-3].minor.yy0;
	if (yymsp[-2].minor.yy0.size_ > 0) {
		qName = SyntaxTree::QualifiedName::makeQualifiedName(
				parser->getSQLAllocator(), parser->viewNsId_, &yymsp[-3].minor.yy0, &yymsp[-2].minor.yy0, NULL);
		tokenName = yymsp[-2].minor.yy0;
	}
	else {
		qName = SyntaxTree::QualifiedName::makeQualifiedName(
				parser->getSQLAllocator(), parser->viewNsId_, NULL, &yymsp[-3].minor.yy0, NULL);
	}
	if (!qName->table_) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, tokenName,
				"View name must not be empty");
	}
	select->targetName_ = qName;

	assert(yymsp[0].minor.yy372);
	if (yymsp[0].minor.yy372 && yymsp[0].minor.yy372->right_ && yymsp[0].minor.yy372->right_->hintList_) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				yymsp[-1].minor.yy0, "Hint specified in View definition");
	}
	select->insertSet_ = yymsp[0].minor.yy372;
	SQLAllocator &alloc = parser->getSQLAllocator();

	SQLToken tokenAs = yymsp[-1].minor.yy0;
	size_t tailLen = strlen(tokenAs.value_ + tokenAs.size_);
	util::String* viewSelectStr =
		ALLOC_NEW(alloc) util::String(tokenAs.value_+ tokenAs.size_, tailLen, alloc);


	SyntaxTree::CreateTableOption *opt =
			ALLOC_NEW(alloc) SyntaxTree::CreateTableOption(alloc);
	opt->optionString_ = viewSelectStr;
	select->createTableOpt_ = opt;
	select->cmdOptionValue_ = yymsp[-5].minor.yy310;

	parser->setTopSelect(select);

	if (yymsp[-5].minor.yy310 == 1) {
		parser->createForceView_ = true;
	}

}
        break;
      case 77: /* cmd ::= DROP VIEW ifexists fullname */
{
	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_DROP_VIEW);

	select->targetName_ = yymsp[0].minor.yy142;
	select->cmdOptionValue_ = yymsp[-1].minor.yy310;
	parser->setTopSelect(select);
}
        break;
      case 78: /* cmd ::= CREATE DATABASE nm */
{
	/* yymsp[0].minor.yy0: Token */
	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_CREATE_DATABASE);

	select->cmdOptionList_ = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());

	SyntaxTree::Expr* nameExpr = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_ID);
	nameExpr->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[0].minor.yy0);

	if (!nameExpr->qName_->name_ || nameExpr->qName_->name_->size() == 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
				"Database name must not be empty");
	}
	select->cmdOptionList_->push_back(nameExpr);

	parser->setTopSelect(select);
}
        break;
      case 79: /* cmd ::= DROP DATABASE nm */
{
	/* yymsp[0].minor.yy0: Token */
	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_DROP_DATABASE);

	select->cmdOptionList_ = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());

	SyntaxTree::Expr* nameExpr = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_ID);
	nameExpr->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[0].minor.yy0);

	if (!nameExpr->qName_->name_ || nameExpr->qName_->name_->size() == 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
				"Database name must not be empty");
	}
	select->cmdOptionList_->push_back(nameExpr);
	parser->setTopSelect(select);
}
        break;
      case 80: /* cmd ::= CREATE USER nm IDENTIFIED BY STRING */
{
	/* yymsp[-3].minor.yy0: Token */
	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_CREATE_USER);

	select->cmdOptionList_ = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());

	SyntaxTree::Expr* nameExpr1 = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_ID);
	nameExpr1->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[-3].minor.yy0);

	if (!nameExpr1->qName_->name_ || nameExpr1->qName_->name_->size() == 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-3].minor.yy0,
				"User name must not be empty");
	}

	SyntaxTree::Expr* nameExpr2 = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_CONSTANT);
	util::String *nameStr = SyntaxTree::tokenToString(
			parser->getSQLAllocator(), yymsp[0].minor.yy0, true);
	if (nameStr) {
		nameExpr2->value_ = SyntaxTree::makeStringValue(
				parser->getSQLAllocator(), nameStr->c_str(), nameStr->size());
	}
	select->cmdOptionList_->push_back(nameExpr1);
	select->cmdOptionList_->push_back(nameExpr2);

	parser->setTopSelect(select);
}
        break;
      case 81: /* cmd ::= CREATE USER nm */
{
	/* yymsp[0].minor.yy0: Token */
	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_CREATE_USER);

	select->cmdOptionList_ = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());

	SyntaxTree::Expr* nameExpr = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_ID);
	nameExpr->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[0].minor.yy0);

	if (!nameExpr->qName_->name_ || nameExpr->qName_->name_->size() == 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
				"User name must not be empty");
	}
	select->cmdOptionList_->push_back(nameExpr);
	select->cmdOptionList_->push_back(NULL);

	parser->setTopSelect(select);
}
        break;
      case 82: /* cmd ::= SET PASSWORD FOR nm EQ STRING */
{
	/* yymsp[-2].minor.yy0, yymsp[0].minor.yy0: Token */
	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_SET_PASSWORD);

	select->cmdOptionList_ = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());

	SyntaxTree::Expr* nameExpr1 = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_ID);
	nameExpr1->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[-2].minor.yy0);
	if (!nameExpr1->qName_->name_ || nameExpr1->qName_->name_->size() == 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-2].minor.yy0,
				"User name must not be empty");
	}

	SyntaxTree::Expr* nameExpr2 = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_CONSTANT);
	util::String *nameStr = SyntaxTree::tokenToString(
			parser->getSQLAllocator(), yymsp[0].minor.yy0, true);
	if (nameStr) {
		nameExpr2->value_ = SyntaxTree::makeStringValue(
				parser->getSQLAllocator(), nameStr->c_str(), nameStr->size());
	}
	select->cmdOptionList_->push_back(nameExpr1);
	select->cmdOptionList_->push_back(nameExpr2);

	parser->setTopSelect(select);
}
        break;
      case 83: /* cmd ::= SET PASSWORD EQ STRING */
{
	/* yymsp[0].minor.yy0: Token */
	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_SET_PASSWORD);

	select->cmdOptionList_ = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());

	SyntaxTree::Expr* nameExpr2 = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_CONSTANT);
	util::String *nameStr = SyntaxTree::tokenToString(
			parser->getSQLAllocator(), yymsp[0].minor.yy0, true);
	if (nameStr) {
		nameExpr2->value_ = SyntaxTree::makeStringValue(
			parser->getSQLAllocator(), nameStr->c_str(), nameStr->size());
	}

	select->cmdOptionList_->push_back(NULL);
	select->cmdOptionList_->push_back(nameExpr2);

	parser->setTopSelect(select);
}
        break;
      case 84: /* cmd ::= DROP USER nm */
{
	/* yymsp[0].minor.yy0: Token */
	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_DROP_USER);

	select->cmdOptionList_ = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());

	SyntaxTree::Expr* nameExpr = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_ID);
	nameExpr->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[0].minor.yy0);

	if (!nameExpr->qName_->name_ || nameExpr->qName_->name_->size() == 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
				"User name must not be empty");
	}
	select->cmdOptionList_->push_back(nameExpr);

	parser->setTopSelect(select);
}
        break;
      case 85: /* cmd ::= CREATE ROLE nm */
{
	/* yymsp[0].minor.yy0: Token */
	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_CREATE_ROLE);

	select->cmdOptionList_ = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());

	SyntaxTree::Expr* nameExpr = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_ID);
	nameExpr->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[0].minor.yy0);

	if (!nameExpr->qName_->name_ || nameExpr->qName_->name_->size() == 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
				"Role name must not be empty");
	}
	select->cmdOptionList_->push_back(nameExpr);

	parser->setTopSelect(select);
}
        break;
      case 86: /* cmd ::= DROP ROLE nm */
{
	/* yymsp[0].minor.yy0: Token */
	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_DROP_USER);

	select->cmdOptionList_ = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());

	SyntaxTree::Expr* nameExpr = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_ID);
	nameExpr->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[0].minor.yy0);

	if (!nameExpr->qName_->name_ || nameExpr->qName_->name_->size() == 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
				"Role name must not be empty");
	}
	select->cmdOptionList_->push_back(nameExpr);

	parser->setTopSelect(select);
}
        break;
      case 87: /* cmd ::= GRANT ALL ON nm TO nm */
{
	/* yymsp[-2].minor.yy0, yymsp[0].minor.yy0: Token */
	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_GRANT);

	select->cmdOptionList_ = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());

	SyntaxTree::Expr* nameExpr1 = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_ID);
	nameExpr1->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[-2].minor.yy0);

	if (!nameExpr1->qName_->name_ || nameExpr1->qName_->name_->size() == 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-2].minor.yy0,
				"User name must not be empty");
	}

	SyntaxTree::Expr* nameExpr2 = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_ID);
	nameExpr2->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[0].minor.yy0);

	if (!nameExpr2->qName_->name_ || nameExpr2->qName_->name_->size() == 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
				"Object name must not be empty");
	}
	select->cmdOptionList_->push_back(nameExpr1);
	select->cmdOptionList_->push_back(nameExpr2);
	Expr* expr = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_CONSTANT);
	int32_t role = 0;
	expr->value_ = TupleValue(role);
	select->cmdOptionList_->push_back(expr);

	parser->setTopSelect(select);
}
        break;
      case 88: /* cmd ::= GRANT SELECT ON nm TO nm */
{
	/* yymsp[-2].minor.yy0, yymsp[0].minor.yy0: Token */
	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_GRANT);

	select->cmdOptionList_ = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());

	SyntaxTree::Expr* nameExpr1 = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_ID);
	nameExpr1->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[-2].minor.yy0);

	if (!nameExpr1->qName_->name_ || nameExpr1->qName_->name_->size() == 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-2].minor.yy0,
				"User name must not be empty");
	}

	SyntaxTree::Expr* nameExpr2 = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_ID);
	nameExpr2->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[0].minor.yy0);

	if (!nameExpr2->qName_->name_ || nameExpr2->qName_->name_->size() == 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
				"Object name must not be empty");
	}
	select->cmdOptionList_->push_back(nameExpr1);
	select->cmdOptionList_->push_back(nameExpr2);

	Expr* expr = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_CONSTANT);
	int32_t role = 1;
	expr->value_ = TupleValue(role);
	select->cmdOptionList_->push_back(expr);

	parser->setTopSelect(select);
}
        break;
      case 89: /* cmd ::= REVOKE ALL ON nm FROM nm */
{
	/* yymsp[-2].minor.yy0, yymsp[0].minor.yy0: Token */
	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_REVOKE);

	select->cmdOptionList_ = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());

	SyntaxTree::Expr* nameExpr1 = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_ID);
	nameExpr1->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[-2].minor.yy0);

	if (!nameExpr1->qName_->name_ || nameExpr1->qName_->name_->size() == 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-2].minor.yy0,
				"User name must not be empty");
	}

	SyntaxTree::Expr* nameExpr2 = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_ID);
	nameExpr2->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[0].minor.yy0);

	if (!nameExpr2->qName_->name_ || nameExpr2->qName_->name_->size() == 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
				"Object name must not be empty");
	}

	select->cmdOptionList_->push_back(nameExpr1);
	select->cmdOptionList_->push_back(nameExpr2);
	Expr* expr = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_CONSTANT);
	int32_t role = 0;
	expr->value_ = TupleValue(role);
	select->cmdOptionList_->push_back(expr);

	parser->setTopSelect(select);
}
        break;
      case 90: /* cmd ::= REVOKE SELECT ON nm FROM nm */
{
	/* yymsp[-2].minor.yy0, yymsp[0].minor.yy0: Token */
	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_REVOKE);

	select->cmdOptionList_ = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());

	SyntaxTree::Expr* nameExpr1 = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_ID);
	nameExpr1->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[-2].minor.yy0);

	if (!nameExpr1->qName_->name_ || nameExpr1->qName_->name_->size() == 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-2].minor.yy0,
				"User name must not be empty");
	}

	SyntaxTree::Expr* nameExpr2 = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_ID);
	nameExpr2->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[0].minor.yy0);

	if (!nameExpr2->qName_->name_ || nameExpr2->qName_->name_->size() == 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
				"Object name must not be empty");
	}

	select->cmdOptionList_->push_back(nameExpr1);
	select->cmdOptionList_->push_back(nameExpr2);

	Expr* expr = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_CONSTANT);
	int32_t role = 1;
	expr->value_ = TupleValue(role);
	select->cmdOptionList_->push_back(expr);

	parser->setTopSelect(select);
}
        break;
      case 91: /* cmd ::= CREATE INDEX ifnotexists nm ON nm dbnm LP idxlist RP using_options */
{
	/* yymsp[-7].minor.yy0, yymsp[-5].minor.yy0, yymsp[-4].minor.yy0: Token, yymsp[-2].minor.yy22: ExprList */
	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_CREATE_INDEX);

	select->cmdOptionList_ = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());

	SyntaxTree::Expr* nameExpr = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_ID);
	nameExpr->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[-7].minor.yy0);

	if (!nameExpr->qName_->name_ || nameExpr->qName_->name_->size() == 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-7].minor.yy0,
				"Index name must not be empty");
	}
	select->cmdOptionList_->push_back(nameExpr);

	SyntaxTree::QualifiedName* qName = NULL;
	if (yymsp[-4].minor.yy0.size_ > 0) {
		qName = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, &yymsp[-5].minor.yy0, &yymsp[-4].minor.yy0, &yymsp[-7].minor.yy0);
	}
	else {
		if (yymsp[-5].minor.yy0.size_ > 0) {
			qName = SyntaxTree::QualifiedName::makeQualifiedName(
					parser->getSQLAllocator(), parser->viewNsId_, NULL, &yymsp[-5].minor.yy0, &yymsp[-7].minor.yy0);
			
			if (!qName->table_ || qName->table_->size() == 0) {
				SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-5].minor.yy0,
						"Table name must not be empty");
			}
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
					"Table name must not be empty");
		}
	}
	if (!qName->table_ || qName->table_->size() == 0) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				"Table name must not be empty");
	}
	select->targetName_ = qName;

	select->insertList_ = yymsp[-2].minor.yy22;
	select->cmdOptionValue_ = yymsp[-8].minor.yy310;

	select->createIndexOpt_ =
		SyntaxTree::makeCreateIndexOption(
			parser->getSQLAllocator(), yymsp[-2].minor.yy22, yymsp[-8].minor.yy310, yymsp[0].minor.yy515);

	SyntaxTree::Expr* table = SyntaxTree::Expr::makeTable(
			parser->getSQLAllocator(), qName, NULL);
	parser->tableList_.push_back(table);

	parser->setTopSelect(select);
}
        break;
      case 92: /* using_options ::= */
{yygotominor.yy515 = 0;}
        break;
      case 93: /* using_options ::= USING nm LP exprlist RP */
{
	if (yymsp[-1].minor.yy22) {
		SyntaxTree::ExprList::iterator itr = yymsp[-1].minor.yy22->begin();
		for (; itr != yymsp[-1].minor.yy22->end(); ++itr) {
			if (((*itr)->op_ != SQLType::EXPR_CONSTANT)
				&& ((*itr)->op_ != SQLType::EXPR_PLACEHOLDER)) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
					"USING options must be CONSTANT or PLACE_HOLDER");
			}
		}
	}
	yygotominor.yy515 = ALLOC_NEW(parser->getSQLAllocator()) SyntaxTree::CreateIndexOption;
	yygotominor.yy515->extensionName_ = SyntaxTree::tokenToString(
			parser->getSQLAllocator(), yymsp[-3].minor.yy0, true);
	yygotominor.yy515->extensionOptionList_ = yymsp[-1].minor.yy22;
}
        break;
      case 94: /* idxlist ::= idxlist COMMA nm sortascending */
{
	SyntaxTree::Expr* expr = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_COLUMN);
	expr->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[-1].minor.yy0);
	expr->sortAscending_ = yymsp[0].minor.yy527;
	if (!expr->qName_->name_) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-1].minor.yy0,
				"Zero-length delimited identifier");
	}
	if (!yymsp[-3].minor.yy22) {
		yymsp[-3].minor.yy22 = ALLOC_NEW(parser->getSQLAllocator())
				SyntaxTree::ExprList(parser->getSQLAllocator());
	}
	yymsp[-3].minor.yy22->push_back(expr);
	yygotominor.yy22 = yymsp[-3].minor.yy22;
}
        break;
      case 95: /* idxlist ::= nm sortascending */
{
	SyntaxTree::Expr* expr = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_COLUMN);
	expr->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[-1].minor.yy0);
	if (!expr->qName_->name_) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-1].minor.yy0,
				"Zero-length delimited identifier");
	}
	expr->sortAscending_ = yymsp[0].minor.yy527;

	yygotominor.yy22 = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());
	yygotominor.yy22->push_back(expr);
}
        break;
      case 99: /* cmd ::= DROP INDEX ifexists nm ON nm dbnm */
{
	SyntaxTree::QualifiedName* qName = NULL;
	if (yymsp[0].minor.yy0.size_ > 0) {
		qName = SyntaxTree::QualifiedName::makeQualifiedName(
				parser->getSQLAllocator(), parser->viewNsId_, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, &yymsp[-3].minor.yy0);
	}
	else {
		qName = SyntaxTree::QualifiedName::makeQualifiedName(
				parser->getSQLAllocator(), parser->viewNsId_, NULL, &yymsp[-1].minor.yy0, &yymsp[-3].minor.yy0);
	}

	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_DROP_INDEX);

	select->targetName_ = qName;
	select->cmdOptionValue_ = yymsp[-4].minor.yy310;

	SyntaxTree::Expr* table = SyntaxTree::Expr::makeTable(
			parser->getSQLAllocator(), qName, NULL);
	parser->tableList_.push_back(table);

	parser->setTopSelect(select);
}
        break;
      case 100: /* cmd ::= DROP INDEX ifexists nm DOT nm dbnm */
{
	SyntaxTree::QualifiedName* qName = NULL;
	if (yymsp[0].minor.yy0.size_ > 0) {
		assert(yymsp[-1].minor.yy0.size_ > 0);
		qName = SyntaxTree::QualifiedName::makeQualifiedName(
				parser->getSQLAllocator(), parser->viewNsId_, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);
	}
	else {
		assert(yymsp[-1].minor.yy0.size_ > 0);
		qName = SyntaxTree::QualifiedName::makeQualifiedName(
				parser->getSQLAllocator(), parser->viewNsId_, NULL, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0);
	}
	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_DROP_INDEX);

	select->targetName_ = qName;
	select->cmdOptionValue_ = yymsp[-4].minor.yy310;

	parser->setTopSelect(select);
}
        break;
      case 101: /* cmd ::= DROP INDEX ifexists nm */
{
	SyntaxTree::QualifiedName* qName = NULL;
	SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
			"Table name is missing");
	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_DROP_INDEX);

	select->targetName_ = qName;
	select->cmdOptionValue_ = yymsp[-1].minor.yy310;

	parser->setTopSelect(select);
}
        break;
      case 102: /* cmd ::= ALTER TABLE nm DROP PARTITION FOR LP expr RP */
{
	SyntaxTree::QualifiedName* largeQName = NULL;
	largeQName = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, &yymsp[-6].minor.yy0, NULL);

	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_ALTER_TABLE_DROP_PARTITION);

	select->targetName_ = largeQName;

	select->cmdOptionList_ = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());

	if (yymsp[-1].minor.yy362 && yymsp[-1].minor.yy362->op_ == SQLType::EXPR_CONSTANT) {
		SQLToken T = yymsp[-1].minor.yy362->startToken_;
		if ((yymsp[-1].minor.yy362->value_.getType() == TupleList::TYPE_LONG)
				|| (yymsp[-1].minor.yy362->value_.getType() == TupleList::TYPE_STRING)) {
			select->cmdOptionList_->push_back(yymsp[-1].minor.yy362);
		}
		else {
			SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
					T, "Invalid value specified");
		}
	}
	else {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				yymsp[-1].minor.yy362->startToken_, "Parameter must be constant");
	}

	SyntaxTree::Expr* table = SyntaxTree::Expr::makeTable(
			parser->getSQLAllocator(), largeQName, NULL);
	parser->tableList_.push_back(table);

	parser->setTopSelect(select);
}
        break;
      case 103: /* cmd ::= ALTER TABLE nm addcolumnlist */
{
	SyntaxTree::QualifiedName* largeQName = NULL;
	largeQName = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, &yymsp[-1].minor.yy0, NULL);

	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_ALTER_TABLE_ADD_COLUMN);

	select->targetName_ = largeQName;

	select->cmdOptionList_ = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());

	SyntaxTree::Expr* table = SyntaxTree::Expr::makeTable(
			parser->getSQLAllocator(), largeQName, NULL);
	parser->tableList_.push_back(table);

	select->createTableOpt_ =
		SyntaxTree::makeAlterTableAddColumnOption(parser->getSQLAllocator(), yymsp[0].minor.yy498);

	parser->setTopSelect(select);
}
        break;
      case 104: /* cmd ::= ALTER TABLE nm RENAME COLUMNKW renamecolumn TO renamecolumn */
{
	SyntaxTree::QualifiedName* largeQName = NULL;
	largeQName = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, &yymsp[-5].minor.yy0, NULL);

	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_ALTER_TABLE_RENAME_COLUMN);

	select->targetName_ = largeQName;

	select->cmdOptionList_ = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());

	SyntaxTree::Expr* table = SyntaxTree::Expr::makeTable(
			parser->getSQLAllocator(), largeQName, NULL);
	parser->tableList_.push_back(table);

	SyntaxTree::TableColumnList* colList = ALLOC_NEW(parser->getSQLAllocator())
				SyntaxTree::TableColumnList(parser->getSQLAllocator());
	colList->push_back(yymsp[-2].minor.yy516);
	colList->push_back(yymsp[0].minor.yy516);
	
	select->createTableOpt_ =
		SyntaxTree::makeAlterTableAddColumnOption(parser->getSQLAllocator(), colList);

	parser->setTopSelect(select);
}
        break;
      case 105: /* renamecolumn ::= nm */
{
	SyntaxTree::ColumnInfo* colInfo = NULL;
	yygotominor.yy516 = SyntaxTree::makeCreateTableColumn(parser->getSQLAllocator(), &yymsp[0].minor.yy0, colInfo);
}
        break;
      case 110: /* cmd ::= PRAGMA nm DOT nm DOT nm EQ nm */
{
	parser->setPragma(yymsp[-6].minor.yy0, yymsp[-4].minor.yy0, yymsp[-2].minor.yy0, yymsp[0].minor.yy0, 0);
}
        break;
      case 111: /* cmd ::= PRAGMA nm DOT nm DOT nm EQ plus_num */
{
	parser->setPragma(yymsp[-6].minor.yy0, yymsp[-4].minor.yy0, yymsp[-2].minor.yy0, yymsp[0].minor.yy0, 1);
}
        break;
      case 112: /* cmd ::= PRAGMA nm DOT nm DOT nm EQ minus_num */
{
	parser->setPragma(yymsp[-6].minor.yy0, yymsp[-4].minor.yy0, yymsp[-2].minor.yy0, yymsp[0].minor.yy0, 2);
}
        break;
      case 113: /* cmd ::= hint_opt select */
{
	if (yymsp[0].minor.yy372) {
		if (!yymsp[0].minor.yy372->left_ && yymsp[0].minor.yy372->right_ && !yymsp[0].minor.yy372->unionAllList_) {
			if (yymsp[-1].minor.yy22) {
				yymsp[0].minor.yy372->right_->hintList_ = yymsp[-1].minor.yy22;
				const SyntaxTree::Select* hintSelect = parser->getHintSelect();
				if (hintSelect != NULL && hintSelect != yymsp[0].minor.yy372->right_) {
					SQLToken token;
					if (yymsp[-1].minor.yy22->size() > 0) {
						token = yymsp[-1].minor.yy22->at(0)->startToken_;
					}
					else if (hintSelect->hintList_ &&
							 hintSelect->hintList_->size() > 0) {
						token = hintSelect->hintList_->at(0)->startToken_;
					}
					else {
						token = SyntaxTree::makeEmptyToken();
					}
					SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
							token, "Hint specified more than once");
				}
				else {
					parser->setHintSelect(yymsp[0].minor.yy372->right_);
				}
			}
			parser->setTopSelect(yymsp[0].minor.yy372->right_);
		}
		else {
			SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
					parser->getSQLAllocator(), SyntaxTree::CMD_SELECT);

			SyntaxTree::Expr* star = SyntaxTree::Expr::makeExpr(
					parser->getSQLAllocator(), SQLType::EXPR_ALL_COLUMN);

			select->selectList_ = ALLOC_NEW(parser->getSQLAllocator())
					SyntaxTree::ExprList(parser->getSQLAllocator());
			select->selectList_->push_back(star);

			SyntaxTree::Expr* selectExpr = SyntaxTree::Expr::makeExpr(
					parser->getSQLAllocator(), SQLType::EXPR_SELECTION);
					selectExpr->subQuery_ = yymsp[0].minor.yy372;

			if (yymsp[-1].minor.yy22) {
				select->hintList_ = yymsp[-1].minor.yy22;
			}
			else {
				SyntaxTree::Set* leftMostSet = yymsp[0].minor.yy372;
				while(leftMostSet->left_) {
					leftMostSet = leftMostSet->left_;
				}
				assert(leftMostSet);
				if (leftMostSet->right_) {
					if (leftMostSet->right_->hintList_) {
						select->hintList_ = leftMostSet->right_->hintList_;
						leftMostSet->right_->hintList_ = NULL;
					}
				}
				else if (leftMostSet->unionAllList_ 
						&& leftMostSet->unionAllList_->at(0)
						&& leftMostSet->unionAllList_->at(0)->hintList_) {
					select->hintList_ = leftMostSet->unionAllList_->at(0)->hintList_;
					leftMostSet->unionAllList_->at(0)->hintList_ = NULL;
				}
			}
			select->from_ = selectExpr;

			parser->setTopSelect(select);

			const SyntaxTree::Select* hintSelect = parser->getHintSelect();
			if (hintSelect != NULL && hintSelect != select &&
					hintSelect->hintList_ != NULL) {
				SQLToken token;
				if (hintSelect->hintList_->size() > 0) {
					token = hintSelect->hintList_->at(0)->startToken_;
				}
				else if (select->hintList_ && select->hintList_->size() > 0) {
					token = select->hintList_->at(0)->startToken_;
				}
				else {
					token = SyntaxTree::makeEmptyToken();
				}
				SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
						token, "Invalid hint found");
			}
			if (select->hintList_ != NULL) {
				parser->setHintSelect(select);
			}
		}
	}
	else {
		parser->setTopSelect(NULL);
	}
}
        break;
      case 114: /* select ::= oneselect */
{
	yygotominor.yy372 = SyntaxTree::Set::makeSet(parser->getSQLAllocator(),
		SyntaxTree::SET_OP_NONE, yymsp[0].minor.yy336);
	parser->currentSelect_ = yymsp[0].minor.yy336;
}
        break;
      case 115: /* select ::= select multiselect_op oneselect */
{
	assert(yymsp[-2].minor.yy372);
	if (yymsp[0].minor.yy336) {
		SyntaxTree::Select* lastSelect = NULL;
		if (yymsp[-2].minor.yy372->right_) {
			lastSelect = yymsp[-2].minor.yy372->right_;
		}
		else {
			assert(yymsp[-2].minor.yy372->unionAllList_);
			lastSelect = yymsp[-2].minor.yy372->unionAllList_->back();
		}
		if (lastSelect->orderByList_ || lastSelect->limitList_) {
			util::String unionOpName(parser->getSQLAllocator());
			switch (yymsp[-1].minor.yy67) {
			case SyntaxTree::SET_OP_UNION:
				unionOpName = "UNION";
				break;
			case SyntaxTree::SET_OP_UNION_ALL:
				unionOpName = "UNION ALL";
				break;
			case SyntaxTree::SET_OP_EXCEPT:
				unionOpName = "EXCEPT";
				break;
			case SyntaxTree::SET_OP_INTERSECT:
				unionOpName = "INTERSECT";
				break;
			case SyntaxTree::SET_OP_NONE:
				break;
			}
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				"ORDER BY or LIMIT clause should come after " 
				<< unionOpName.c_str() << " not before");
		}
		if (yymsp[-1].minor.yy67 == SyntaxTree::SET_OP_UNION_ALL) {
			if (yymsp[-2].minor.yy372->type_ == SyntaxTree::SET_OP_UNION_ALL) {
				assert(yymsp[-2].minor.yy372->unionAllList_);
				yygotominor.yy372 = yymsp[-2].minor.yy372;
				yygotominor.yy372->unionAllList_->push_back(yymsp[0].minor.yy336);
			}
			else if (yymsp[-2].minor.yy372->type_ == SyntaxTree::SET_OP_NONE) {
				assert(!yymsp[-2].minor.yy372->unionAllList_);
				yymsp[-2].minor.yy372->unionAllList_ = ALLOC_NEW(parser->getSQLAllocator())
						SyntaxTree::SelectList(parser->getSQLAllocator());
				if (yymsp[-2].minor.yy372->right_) {
					yymsp[-2].minor.yy372->unionAllList_->push_back(yymsp[-2].minor.yy372->right_);
					yymsp[-2].minor.yy372->right_ = NULL;
				}
				yymsp[-2].minor.yy372->type_ = yymsp[-1].minor.yy67; 
				yygotominor.yy372 = yymsp[-2].minor.yy372;
				yygotominor.yy372->unionAllList_->push_back(yymsp[0].minor.yy336);
			}
			else {
				SyntaxTree::SelectList* unionAllList = ALLOC_NEW(parser->getSQLAllocator())
						SyntaxTree::SelectList(parser->getSQLAllocator());
				unionAllList->push_back(yymsp[0].minor.yy336);

				yygotominor.yy372 = SyntaxTree::Set::makeSet(
						parser->getSQLAllocator(), yymsp[-1].minor.yy67, yymsp[-2].minor.yy372, NULL, unionAllList);
			}
		}
		else {
			yygotominor.yy372 = SyntaxTree::Set::makeSet(parser->getSQLAllocator(), yymsp[-1].minor.yy67, yymsp[-2].minor.yy372, yymsp[0].minor.yy336, NULL);
		}
	}
	else {
		yygotominor.yy372 = yymsp[-2].minor.yy372;
	}
}
        break;
      case 116: /* multiselect_op ::= UNION */
{yygotominor.yy67 = SyntaxTree::SET_OP_UNION;}
        break;
      case 117: /* multiselect_op ::= UNION ALL */
{yygotominor.yy67 = SyntaxTree::SET_OP_UNION_ALL;}
        break;
      case 118: /* multiselect_op ::= EXCEPT */
{yygotominor.yy67 = SyntaxTree::SET_OP_EXCEPT;}
        break;
      case 119: /* multiselect_op ::= INTERSECT */
{yygotominor.yy67 = SyntaxTree::SET_OP_INTERSECT;}
        break;
      case 120: /* oneselect ::= SELECT hint_opt distinct selcollist from where_opt groupby_opt having_opt orderby_opt limit_opt */
{
	/* yymsp[-9].minor.yy0: Token */
	yygotominor.yy336 = SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_SELECT,
			yymsp[-6].minor.yy22, yymsp[-7].minor.yy296, yymsp[-5].minor.yy362, yymsp[-4].minor.yy362, yymsp[-3].minor.yy22, yymsp[-2].minor.yy362, yymsp[-1].minor.yy22, yymsp[0].minor.yy22, yymsp[-8].minor.yy22);

	if (yymsp[-8].minor.yy22 != NULL) {
		const SyntaxTree::Select* hintSelect = parser->getHintSelect();
		if (hintSelect != NULL && hintSelect->hintList_ != NULL &&
				hintSelect->hintList_ != yymsp[-8].minor.yy22) {
			SQLToken token;
			if (yymsp[-8].minor.yy22->size() > 0) {
				token = yymsp[-8].minor.yy22->at(0)->startToken_;
			}
			else if (hintSelect->hintList_->size() > 0) {
				token = hintSelect->hintList_->at(0)->startToken_;
			}
			else {
				token = SyntaxTree::makeEmptyToken();
			}
			SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
					token, "Hint specified more than once");
		}
		else {
			parser->setHintSelect(yygotominor.yy336);
		}
	}

	parser->currentSelect_ = yygotominor.yy336;
	if (parser->childrenList_.size() > 0) {
		for (size_t pos = 0; pos < parser->childrenList_.size(); ++pos) {
			parser->childrenList_[pos]->parent_ = yygotominor.yy336;
		}
		parser->childrenList_.clear();
	}
	parser->childrenList_.push_back(yygotominor.yy336);
}
        break;
      case 121: /* select ::= valuelist */
{yygotominor.yy372 = yymsp[0].minor.yy372;}
        break;
      case 122: /* valuelist ::= VALUES LP nexprlist RP */
{
	SyntaxTree::Select* select =  SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_SELECT,
			yymsp[-1].minor.yy22, SyntaxTree::AGGR_OPT_ALL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

	yygotominor.yy372 = SyntaxTree::Set::makeSet(parser->getSQLAllocator(),
			SyntaxTree::SET_OP_NONE, select);
	parser->currentSelect_ = select;
	if (parser->childrenList_.size() > 0) {
		for (size_t pos = 0; pos < parser->childrenList_.size(); ++pos) {
			parser->childrenList_[pos]->parent_ = select;
		}
		parser->childrenList_.clear();
	}
	parser->childrenList_.push_back(select);
}
        break;
      case 123: /* valuelist ::= valuelist COMMA LP exprlist RP */
{
	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_SELECT,
			yymsp[-1].minor.yy22, SyntaxTree::AGGR_OPT_ALL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

	assert(yymsp[-4].minor.yy372);
	if (yymsp[-4].minor.yy372->type_ == SyntaxTree::SET_OP_NONE) {
		assert(!yymsp[-4].minor.yy372->unionAllList_);
		yymsp[-4].minor.yy372->unionAllList_ = ALLOC_NEW(parser->getSQLAllocator())
				SyntaxTree::SelectList(parser->getSQLAllocator());
		if (yymsp[-4].minor.yy372->right_) {
			yymsp[-4].minor.yy372->unionAllList_->push_back(yymsp[-4].minor.yy372->right_);
			yymsp[-4].minor.yy372->right_ = NULL;
		}
		yymsp[-4].minor.yy372->type_ = SyntaxTree::SET_OP_UNION_ALL;
		yygotominor.yy372 = yymsp[-4].minor.yy372;
		yygotominor.yy372->unionAllList_->push_back(select);
	}
	else {
		assert(yymsp[-4].minor.yy372->type_ == SyntaxTree::SET_OP_UNION_ALL);
		yygotominor.yy372 = yymsp[-4].minor.yy372;
		yygotominor.yy372->unionAllList_->push_back(select);
	}
	parser->currentSelect_ = select;
	if (parser->childrenList_.size() > 0) {
		for (size_t pos = 0; pos < parser->childrenList_.size(); ++pos) {
			parser->childrenList_[pos]->parent_ = select;
		}
		parser->childrenList_.clear();
	}
	parser->childrenList_.push_back(select);
}
        break;
      case 124: /* hint_opt ::= HINT_START hintlist HINT_END */
      case 145: /* sclp ::= selcollist COMMA */ yytestcase(yyruleno==145);
{
	yygotominor.yy22 = yymsp[-1].minor.yy22;
}
        break;
      case 128: /* hintlist1 ::= hintlist1 onehint */
{
	if (!yymsp[-1].minor.yy22) {
		yymsp[-1].minor.yy22 = ALLOC_NEW(parser->getSQLAllocator())
				SyntaxTree::ExprList(parser->getSQLAllocator());
	}
	assert(yymsp[0].minor.yy362);
	yymsp[-1].minor.yy22->push_back(yymsp[0].minor.yy362);
	yygotominor.yy22 = yymsp[-1].minor.yy22;
}
        break;
      case 129: /* hintlist1 ::= onehint */
{
	assert(yymsp[0].minor.yy362);
	yygotominor.yy22 = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());
	assert(yymsp[0].minor.yy362);
	yygotominor.yy22->push_back(yymsp[0].minor.yy362);
}
        break;
      case 130: /* onehint ::= ID LP hintexprlist RP */
{
	if (!yymsp[-1].minor.yy22) {
		yymsp[-1].minor.yy22 = ALLOC_NEW(parser->getSQLAllocator())
				SyntaxTree::ExprList(parser->getSQLAllocator());
	}
	SyntaxTree::Expr* hintExpr = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_LIST);

	hintExpr->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[-3].minor.yy0);

	hintExpr->next_ = yymsp[-1].minor.yy22;
	hintExpr->startToken_ = yymsp[-2].minor.yy0;
	hintExpr->endToken_ = yymsp[0].minor.yy0;

	yygotominor.yy362 = hintExpr;
}
        break;
      case 133: /* hintexprlist1 ::= hintexprlist1 hintexpr */
{
	if (!yymsp[-1].minor.yy22) {
		yymsp[-1].minor.yy22 = ALLOC_NEW(parser->getSQLAllocator())
				SyntaxTree::ExprList(parser->getSQLAllocator());
	}
	yymsp[-1].minor.yy22->push_back(yymsp[0].minor.yy362);
	yygotominor.yy22 = yymsp[-1].minor.yy22;
}
        break;
      case 134: /* hintexprlist1 ::= hintexpr */
      case 203: /* limit_opt ::= LIMIT expr */ yytestcase(yyruleno==203);
      case 321: /* nexprlist ::= expr */ yytestcase(yyruleno==321);
{
	yygotominor.yy22 = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());
	yygotominor.yy22->push_back(yymsp[0].minor.yy362);
}
        break;
      case 135: /* hintexpr ::= term */
      case 154: /* from ::= FROM seltabtree */ yytestcase(yyruleno==154);
      case 219: /* expr ::= term */ yytestcase(yyruleno==219);
{
	yygotominor.yy362 = yymsp[0].minor.yy362;
}
        break;
      case 136: /* hintexpr ::= MINUS INTEGER */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_CONSTANT);
	yygotominor.yy362->startToken_ = yymsp[-1].minor.yy0;
	yygotominor.yy362->endToken_ = yymsp[0].minor.yy0;

	util::String str(yymsp[-1].minor.yy0.value_, yymsp[-1].minor.yy0.value_ + yymsp[-1].minor.yy0.size_, parser->getSQLAllocator());
	str.append(yymsp[0].minor.yy0.value_, yymsp[0].minor.yy0.size_);
	int64_t int64Value;
	bool succeeded = SQLProcessor::ValueUtils::toLong(
			str.c_str(), str.size(), int64Value);
	if (succeeded) {
		yygotominor.yy362->value_ = TupleValue(int64Value);
	}
	else {
		double doubleValue;
		bool succeeded = SQLProcessor::ValueUtils::toDouble(
				str.c_str(), str.size(), doubleValue);
		if (succeeded) {
			yygotominor.yy362->value_ = TupleValue(doubleValue);
		}
		else {
			SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
								   yymsp[0].minor.yy0, "Invalid constant value");
		}
	}
}
        break;
      case 137: /* hintexpr ::= MINUS FLOAT */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_CONSTANT);
	yygotominor.yy362->startToken_ = yymsp[-1].minor.yy0;
	yygotominor.yy362->endToken_ = yymsp[0].minor.yy0;

	util::String str(yymsp[-1].minor.yy0.value_, yymsp[-1].minor.yy0.value_ + yymsp[-1].minor.yy0.size_, parser->getSQLAllocator());
	str.append(yymsp[0].minor.yy0.value_, yymsp[0].minor.yy0.size_);
	double doubleValue;
	bool succeeded = SQLProcessor::ValueUtils::toDouble(
			str.c_str(), str.size(), doubleValue);
	if (succeeded) {
		yygotominor.yy362->value_ = TupleValue(doubleValue);
	}
	else {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
							   yymsp[0].minor.yy0, "Invalid constant value");
	}
}
        break;
      case 138: /* hintexpr ::= LP hintexprlist1 RP */
{
	/* yymsp[-2].minor.yy0, yymsp[0].minor.yy0: Token */
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_LIST);
	yygotominor.yy362->next_ = yymsp[-1].minor.yy22;
	yygotominor.yy362->startToken_ = yymsp[-2].minor.yy0;
	yygotominor.yy362->endToken_ = yymsp[0].minor.yy0;
}
        break;
      case 139: /* hintexpr ::= ID */
      case 222: /* expr ::= ID */ yytestcase(yyruleno==222);
{
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_COLUMN);
	yygotominor.yy362->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[0].minor.yy0);
	if (!yygotominor.yy362->qName_->name_) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
				"Zero-length delimited identifier");
	}
	yygotominor.yy362->startToken_ = yymsp[0].minor.yy0;
}
        break;
      case 140: /* hintexpr ::= nm DOT nm */
      case 223: /* expr ::= nm DOT nm */ yytestcase(yyruleno==223);
{
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_COLUMN);
	yygotominor.yy362->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
	if (!yygotominor.yy362->qName_->name_) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
				"Zero-length delimited identifier");
	}
	yygotominor.yy362->startToken_ = yymsp[-2].minor.yy0;
	yygotominor.yy362->endToken_ = yymsp[0].minor.yy0;
}
        break;
      case 141: /* hintexpr ::= nm DOT nm DOT nm */
      case 224: /* expr ::= nm DOT nm DOT nm */ yytestcase(yyruleno==224);
{
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_COLUMN);
	yygotominor.yy362->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, &yymsp[-4].minor.yy0, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
	if (!yygotominor.yy362->qName_->name_) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
				"Zero-length delimited identifier");
	}
	yygotominor.yy362->startToken_ = yymsp[-4].minor.yy0;
	yygotominor.yy362->endToken_ = yymsp[0].minor.yy0;
}
        break;
      case 142: /* distinct ::= DISTINCT */
{yygotominor.yy296 = SyntaxTree::AGGR_OPT_DISTINCT;}
        break;
      case 143: /* distinct ::= ALL */
      case 144: /* distinct ::= */ yytestcase(yyruleno==144);
{yygotominor.yy296 = SyntaxTree::AGGR_OPT_ALL;}
        break;
      case 146: /* sclp ::= */
      case 202: /* limit_opt ::= */ yytestcase(yyruleno==202);
{
	yygotominor.yy22 = 0;
}
        break;
      case 147: /* selcollist ::= sclp expr as */
{
	if (!yymsp[-2].minor.yy22) {
		yymsp[-2].minor.yy22 = ALLOC_NEW(parser->getSQLAllocator())
				SyntaxTree::ExprList(parser->getSQLAllocator());
	}
	if (yymsp[0].minor.yy0.size_ > 0) {
		assert(yymsp[-1].minor.yy362);
		yymsp[-1].minor.yy362->aliasName_ = SyntaxTree::tokenToString(
				parser->getSQLAllocator(), yymsp[0].minor.yy0, true);
	}
	yygotominor.yy22 = yymsp[-2].minor.yy22;
	yygotominor.yy22->push_back(yymsp[-1].minor.yy362);
}
        break;
      case 148: /* selcollist ::= sclp STAR */
{
	SyntaxTree::Expr* expr = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_ALL_COLUMN);

	if (!yymsp[-1].minor.yy22) {
		yymsp[-1].minor.yy22 = ALLOC_NEW(parser->getSQLAllocator())
				SyntaxTree::ExprList(parser->getSQLAllocator());
	}
	yygotominor.yy22 = yymsp[-1].minor.yy22;
	yygotominor.yy22->push_back(expr);
}
        break;
      case 149: /* selcollist ::= sclp nm DOT STAR */
{
	/* yymsp[-2].minor.yy0, yymsp[0].minor.yy0:Token */
	SyntaxTree::Expr* expr = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_ALL_COLUMN);

	expr->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, &yymsp[-2].minor.yy0, NULL);
	if (!yymsp[-3].minor.yy22) {
		yymsp[-3].minor.yy22 = ALLOC_NEW(parser->getSQLAllocator())
				SyntaxTree::ExprList(parser->getSQLAllocator());
	}
	yygotominor.yy22 = yymsp[-3].minor.yy22;
	yygotominor.yy22->push_back(expr);
}
        break;
      case 152: /* as ::= */
{yygotominor.yy0.size_ = 0;}
        break;
      case 153: /* from ::= */
{
	yygotominor.yy362 = 0;
}
        break;
      case 155: /* stl_prefix ::= seltabtree joinop */
{
	assert(yymsp[-1].minor.yy362);
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_JOIN);
	yygotominor.yy362->joinOp_ = yymsp[0].minor.yy197;
	yygotominor.yy362->left_ = yymsp[-1].minor.yy362;
	yygotominor.yy362->right_ = NULL;
}
        break;
      case 156: /* stl_prefix ::= */
{ yygotominor.yy362 = 0; }
        break;
      case 157: /* seltabtree ::= stl_prefix nm dbnm as on_opt using_opt */
{
	SyntaxTree::QualifiedName* qName = NULL;
	if (!yymsp[-5].minor.yy362 && (yymsp[-1].minor.yy362 || yymsp[0].minor.yy22)) {
		if (yymsp[-1].minor.yy362) {
			SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
					yymsp[-1].minor.yy362->startToken_, "JOIN clause is required before ON");
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
					"JOIN clause is required before USING");
		}
	}
	if (yymsp[-3].minor.yy0.size_ > 0) {
		qName = SyntaxTree::QualifiedName::makeQualifiedName(
				parser->getSQLAllocator(), parser->viewNsId_, &yymsp[-4].minor.yy0, &yymsp[-3].minor.yy0, NULL);
	}
	else {
		qName = SyntaxTree::QualifiedName::makeQualifiedName(
				parser->getSQLAllocator(), parser->viewNsId_, NULL, &yymsp[-4].minor.yy0, NULL);
	}
	SyntaxTree::Expr* table = SyntaxTree::Expr::makeTable(
			parser->getSQLAllocator(), qName, NULL);
	int64_t nsId = parser->checkAndGetViewSelectStr(table);
	if (yymsp[-2].minor.yy0.size_ > 0) {
		table->aliasName_ = SyntaxTree::tokenToString(
				parser->getSQLAllocator(), yymsp[-2].minor.yy0, true);
	}
	else if (nsId != SyntaxTree::QualifiedName::TOP_NS_ID) {
		table->aliasName_ = ALLOC_NEW(parser->getSQLAllocator()) util::String(
				qName->table_->c_str(), parser->getSQLAllocator());
	}
	if (nsId != SyntaxTree::QualifiedName::TOP_NS_ID) {
		if (!table->qName_) {
			table->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
					parser->getSQLAllocator());
		}
		table->qName_->nsId_ = nsId;
	}
	if (!yymsp[-5].minor.yy362) {
		yymsp[-5].minor.yy362 = table;
	}
	else {
		assert(yymsp[-5].minor.yy362->op_ == SQLType::EXPR_JOIN);
		assert(yymsp[-5].minor.yy362->right_ == NULL);
		yymsp[-5].minor.yy362->right_ = table;

		yymsp[-5].minor.yy362->next_ = ALLOC_NEW(parser->getSQLAllocator())
				SyntaxTree::ExprList(parser->getSQLAllocator());
		yymsp[-5].minor.yy362->next_->push_back(yymsp[-1].minor.yy362);  
		if (yymsp[0].minor.yy22) {
			yymsp[-5].minor.yy362->next_->reserve(1 + yymsp[0].minor.yy22->size());
			yymsp[-5].minor.yy362->next_->insert(yymsp[-5].minor.yy362->next_->end(), yymsp[0].minor.yy22->begin(), yymsp[0].minor.yy22->end());
		}
		else {
			yymsp[-5].minor.yy362->next_->push_back(NULL);
		}
	}
	yygotominor.yy362 = yymsp[-5].minor.yy362;
	parser->tableList_.push_back(table);
}
        break;
      case 158: /* seltabtree ::= stl_prefix nm dbnm LP exprlist RP as on_opt using_opt */
{
	SyntaxTree::QualifiedName* qName = NULL;
	if (!yymsp[-8].minor.yy362 && (yymsp[-1].minor.yy362 || yymsp[0].minor.yy22)) {
		if (yymsp[-1].minor.yy362) {
			SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
					yymsp[-1].minor.yy362->startToken_, "JOIN clause is required before ON");
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
					"JOIN clause is required before USING");
		}
	}
	if (yymsp[-6].minor.yy0.size_ > 0) {
		qName = SyntaxTree::QualifiedName::makeQualifiedName(
				parser->getSQLAllocator(), parser->viewNsId_, &yymsp[-7].minor.yy0, &yymsp[-6].minor.yy0, NULL);
	}
	else {
		qName = SyntaxTree::QualifiedName::makeQualifiedName(
				parser->getSQLAllocator(),
				parser->viewNsId_, NULL, &yymsp[-7].minor.yy0, NULL);
	}
	SyntaxTree::Expr* table = SyntaxTree::Expr::makeTable(
			parser->getSQLAllocator(), qName, NULL);
	int64_t nsId = parser->checkAndGetViewSelectStr(table);
	if (yymsp[-2].minor.yy0.size_ > 0) {
		table->aliasName_ = SyntaxTree::tokenToString(
				parser->getSQLAllocator(), yymsp[-2].minor.yy0, true);
	}
	else if (nsId != SyntaxTree::QualifiedName::TOP_NS_ID) {
		table->aliasName_ = ALLOC_NEW(parser->getSQLAllocator()) util::String(
				qName->table_->c_str(), parser->getSQLAllocator());
	}
	if (nsId != SyntaxTree::QualifiedName::TOP_NS_ID) {
		if (!table->qName_) {
			table->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
					parser->getSQLAllocator());
		}
		table->qName_->nsId_ = nsId;
	}
	if (yymsp[-4].minor.yy22) {
		table->next_ = yymsp[-4].minor.yy22;
	}
	if (!yymsp[-8].minor.yy362) {
		yymsp[-8].minor.yy362 = table;
	}
	else {
		assert(yymsp[-8].minor.yy362->op_ == SQLType::EXPR_JOIN);
		assert(yymsp[-8].minor.yy362->right_ == NULL);
		yymsp[-8].minor.yy362->right_ = table;

		yymsp[-8].minor.yy362->next_ = ALLOC_NEW(parser->getSQLAllocator())
				SyntaxTree::ExprList(parser->getSQLAllocator());
		yymsp[-8].minor.yy362->next_->push_back(yymsp[-1].minor.yy362);  
		if (yymsp[0].minor.yy22) {
			yymsp[-8].minor.yy362->next_->reserve(1 + yymsp[0].minor.yy22->size());
			yymsp[-8].minor.yy362->next_->insert(yymsp[-8].minor.yy362->next_->end(), yymsp[0].minor.yy22->begin(), yymsp[0].minor.yy22->end());
		}
		else {
			yymsp[-8].minor.yy362->next_->push_back(NULL);
		}
	}
	yygotominor.yy362 = yymsp[-8].minor.yy362;
	parser->tableList_.push_back(table);
}
        break;
      case 159: /* seltabtree ::= stl_prefix LP select RP as on_opt using_opt */
{

	if (!yymsp[-6].minor.yy362 && (yymsp[-1].minor.yy362 || yymsp[0].minor.yy22)) {
		if (yymsp[-1].minor.yy362) {
			SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
					yymsp[-1].minor.yy362->startToken_, "JOIN clause is required before ON");
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
					"JOIN clause is required before USING");
		}
	}
	SyntaxTree::Expr* table = SyntaxTree::Expr::makeTable(
		parser->getSQLAllocator(), NULL, yymsp[-4].minor.yy372);
	if (yymsp[-2].minor.yy0.size_ > 0) {
		table->aliasName_ = SyntaxTree::tokenToString(
				parser->getSQLAllocator(), yymsp[-2].minor.yy0, true);
	}
	else {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-3].minor.yy0,
				"Subquery in FROM must have an alias");
	}
	if (!yymsp[-6].minor.yy362) {
		yymsp[-6].minor.yy362 = table;
	}
	else {
		assert(yymsp[-6].minor.yy362->op_ == SQLType::EXPR_JOIN);
		assert(yymsp[-6].minor.yy362->right_ == NULL);
		yymsp[-6].minor.yy362->right_ = table;

		yymsp[-6].minor.yy362->next_ = ALLOC_NEW(parser->getSQLAllocator())
				SyntaxTree::ExprList(parser->getSQLAllocator());
		yymsp[-6].minor.yy362->next_->push_back(yymsp[-1].minor.yy362);  
		if (yymsp[0].minor.yy22) {
			yymsp[-6].minor.yy362->next_->reserve(1 + yymsp[0].minor.yy22->size());
			yymsp[-6].minor.yy362->next_->insert(yymsp[-6].minor.yy362->next_->end(), yymsp[0].minor.yy22->begin(), yymsp[0].minor.yy22->end());
		}
		else {
			yymsp[-6].minor.yy362->next_->push_back(NULL);
		}
	}
	yygotominor.yy362 = yymsp[-6].minor.yy362;
	parser->tableList_.push_back(table);

	if (yymsp[-4].minor.yy372 && yymsp[-4].minor.yy372->right_ && yymsp[-4].minor.yy372->right_->hintList_) {
		SQLToken token;
		if (yymsp[-4].minor.yy372->right_->hintList_->size() > 0) {
			token = yymsp[-4].minor.yy372->right_->hintList_->at(0)->startToken_;
		}
		else {
			token = SyntaxTree::makeEmptyToken();
		}
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				token, "Invalid hint found");
	}
}
        break;
      case 160: /* seltabtree ::= stl_prefix LP seltabtree RP as on_opt using_opt */
{

	if (!yymsp[-6].minor.yy362 && (yymsp[-1].minor.yy362 || yymsp[0].minor.yy22)) {
		if (yymsp[-1].minor.yy362) {
			SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
					yymsp[-1].minor.yy362->startToken_, "JOIN clause is required before ON");
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
					"JOIN clause is required before USING");
		}
	}
	if (yymsp[-6].minor.yy362 == 0 && (yymsp[-2].minor.yy0.size_ == 0) && yymsp[-1].minor.yy362==0 && yymsp[0].minor.yy22==0) {
		yygotominor.yy362 = yymsp[-4].minor.yy362;
	}
	else {
		if (yymsp[-2].minor.yy0.size_ > 0) {
			yymsp[-4].minor.yy362->aliasName_ = SyntaxTree::tokenToString(
					parser->getSQLAllocator(), yymsp[-2].minor.yy0, true);
		}

		if (!yymsp[-6].minor.yy362) {
			yymsp[-6].minor.yy362 = yymsp[-4].minor.yy362;
		}
		else {
			assert(yymsp[-6].minor.yy362->op_ == SQLType::EXPR_JOIN);
			assert(yymsp[-6].minor.yy362->right_ == NULL);
			yymsp[-6].minor.yy362->right_ = yymsp[-4].minor.yy362;

			yymsp[-6].minor.yy362->next_ = ALLOC_NEW(parser->getSQLAllocator())
					SyntaxTree::ExprList(parser->getSQLAllocator());
			yymsp[-6].minor.yy362->next_->push_back(yymsp[-1].minor.yy362);  
			if (yymsp[0].minor.yy22) {
				yymsp[-6].minor.yy362->next_->reserve(1 + yymsp[0].minor.yy22->size());
				yymsp[-6].minor.yy362->next_->insert(yymsp[-6].minor.yy362->next_->end(), yymsp[0].minor.yy22->begin(), yymsp[0].minor.yy22->end());
			}
			else {
				yymsp[-6].minor.yy362->next_->push_back(NULL);
			}
		}
		yygotominor.yy362 = yymsp[-6].minor.yy362;
	}
}
        break;
      case 161: /* dbnm ::= */
{yygotominor.yy0.value_ = 0; yygotominor.yy0.size_ = 0;}
        break;
      case 163: /* fullname ::= nm dbnm */
{
	SyntaxTree::QualifiedName* qName = NULL;
	if (yymsp[0].minor.yy0.size_ > 0) {
		qName = SyntaxTree::QualifiedName::makeQualifiedName(
				parser->getSQLAllocator(), parser->viewNsId_, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, NULL);
	}
	else {
		qName = SyntaxTree::QualifiedName::makeQualifiedName(
				parser->getSQLAllocator(), parser->viewNsId_, NULL, &yymsp[-1].minor.yy0, NULL);
	}
	yygotominor.yy142 = qName;
}
        break;
      case 164: /* joinop ::= COMMA|JOIN */
      case 165: /* joinop ::= INNER JOIN */ yytestcase(yyruleno==165);
      case 166: /* joinop ::= CROSS JOIN */ yytestcase(yyruleno==166);
{
	yygotominor.yy197 = SQLType::JOIN_INNER;
}
        break;
      case 167: /* joinop ::= LEFT JOIN */
      case 168: /* joinop ::= LEFT OUTER JOIN */ yytestcase(yyruleno==168);
{
	yygotominor.yy197 = SQLType::JOIN_LEFT_OUTER;
}
        break;
      case 169: /* joinop ::= RIGHT JOIN */
      case 170: /* joinop ::= RIGHT OUTER JOIN */ yytestcase(yyruleno==170);
{
	yygotominor.yy197 = SQLType::JOIN_RIGHT_OUTER;
}
        break;
      case 171: /* joinop ::= FULL JOIN */
      case 172: /* joinop ::= FULL OUTER JOIN */ yytestcase(yyruleno==172);
{
	yygotominor.yy197 = SQLType::JOIN_FULL_OUTER;
}
        break;
      case 173: /* joinop ::= NATURAL JOIN */
      case 174: /* joinop ::= NATURAL INNER JOIN */ yytestcase(yyruleno==174);
      case 175: /* joinop ::= NATURAL CROSS JOIN */ yytestcase(yyruleno==175);
{
	yygotominor.yy197 = SQLType::JOIN_NATURAL_INNER;
}
        break;
      case 176: /* joinop ::= NATURAL LEFT JOIN */
      case 177: /* joinop ::= NATURAL LEFT OUTER JOIN */ yytestcase(yyruleno==177);
{
	yygotominor.yy197 = SQLType::JOIN_NATURAL_LEFT_OUTER;
}
        break;
      case 178: /* joinop ::= NATURAL RIGHT JOIN */
      case 179: /* joinop ::= NATURAL RIGHT OUTER JOIN */ yytestcase(yyruleno==179);
{
	yygotominor.yy197 = SQLType::JOIN_NATURAL_RIGHT_OUTER;
}
        break;
      case 180: /* joinop ::= NATURAL FULL JOIN */
      case 181: /* joinop ::= NATURAL FULL OUTER JOIN */ yytestcase(yyruleno==181);
{
	yygotominor.yy197 = SQLType::JOIN_NATURAL_FULL_OUTER;
}
        break;
      case 182: /* on_opt ::= ON expr */
      case 201: /* having_opt ::= HAVING expr */ yytestcase(yyruleno==201);
      case 207: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==207);
{yygotominor.yy362 = yymsp[0].minor.yy362;}
        break;
      case 183: /* on_opt ::= */
      case 200: /* having_opt ::= */ yytestcase(yyruleno==200);
      case 206: /* where_opt ::= */ yytestcase(yyruleno==206);
      case 315: /* case_else ::= */ yytestcase(yyruleno==315);
      case 317: /* case_operand ::= */ yytestcase(yyruleno==317);
{yygotominor.yy362 = 0;}
        break;
      case 184: /* using_opt ::= USING LP idlist RP */
      case 216: /* inscollist_opt ::= LP idlist RP */ yytestcase(yyruleno==216);
{yygotominor.yy22 = yymsp[-1].minor.yy22;}
        break;
      case 188: /* sortlist ::= sortlist COMMA expr sortascending */
{
	if (!yymsp[-3].minor.yy22) {
		yymsp[-3].minor.yy22 = ALLOC_NEW(parser->getSQLAllocator())
				SyntaxTree::ExprList(parser->getSQLAllocator());
	}
	yymsp[-1].minor.yy362->sortAscending_ = yymsp[0].minor.yy527;
	yymsp[-3].minor.yy22->push_back(yymsp[-1].minor.yy362);
	yygotominor.yy22 = yymsp[-3].minor.yy22;
}
        break;
      case 189: /* sortlist ::= expr sortascending */
{
	yygotominor.yy22 = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());
	yymsp[-1].minor.yy362->sortAscending_ = yymsp[0].minor.yy527;
	yygotominor.yy22->push_back(yymsp[-1].minor.yy362);
}
        break;
      case 192: /* groupby_opt ::= GROUP BY RANGE LP expr RP groupIntervalOption groupFillOption */
{
	yygotominor.yy22 = SyntaxTree::makeRangeGroupOption(
			parser->getSQLAllocator(), yymsp[-3].minor.yy362, yymsp[0].minor.yy362, yymsp[-1].minor.yy61.base_.value_,
			yymsp[-1].minor.yy61.base_.unit_, yymsp[-1].minor.yy61.detail_.offset_, yymsp[-1].minor.yy61.detail_.timeZone_,
			yymsp[-1].minor.yy61.detail_.withTimeZone_);
}
        break;
      case 193: /* groupFillOption ::= */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_RANGE_FILL_NULL);
}
        break;
      case 194: /* groupFillOption ::= FILL LP groupFillOptionBase RP */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), yymsp[-1].minor.yy92);
}
        break;
      case 195: /* groupFillOptionBase ::= NULL */
{
	yygotominor.yy92 = SQLType::EXPR_RANGE_FILL_NULL;
}
        break;
      case 196: /* groupFillOptionBase ::= NONE */
{
	yygotominor.yy92 = SQLType::EXPR_RANGE_FILL_NONE;
}
        break;
      case 197: /* groupFillOptionBase ::= LINEAR */
{
	yygotominor.yy92 = SQLType::EXPR_RANGE_FILL_LINEAR;
}
        break;
      case 198: /* groupFillOptionBase ::= PREVIOUS */
{
	yygotominor.yy92 = SQLType::EXPR_RANGE_FILL_PREV;
}
        break;
      case 199: /* groupFillOptionBase ::= ID */
{
	SQL_PARSER_THROW_ERROR(
			GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
			"Unknown group fill option");
}
        break;
      case 204: /* limit_opt ::= LIMIT expr OFFSET expr */
{
	yygotominor.yy22 = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());
	yygotominor.yy22->push_back(yymsp[-2].minor.yy362);
	yygotominor.yy22->push_back(yymsp[0].minor.yy362);
}
        break;
      case 205: /* limit_opt ::= LIMIT expr COMMA expr */
{
	yygotominor.yy22 = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());
	yygotominor.yy22->push_back(yymsp[0].minor.yy362);  
	yygotominor.yy22->push_back(yymsp[-2].minor.yy362);  
}
        break;
      case 208: /* cmd ::= hint_opt DELETE hint_opt FROM fullname as where_opt */
{
	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
		parser->getSQLAllocator(), SyntaxTree::CMD_DELETE);
	select->targetName_ = yymsp[-2].minor.yy142;
	select->where_ = yymsp[0].minor.yy362;
	if (yymsp[-6].minor.yy22 || yymsp[-4].minor.yy22) {
		select->hintList_ = yymsp[-6].minor.yy22 ? yymsp[-6].minor.yy22 : yymsp[-4].minor.yy22;
	}
	SyntaxTree::Expr* table = SyntaxTree::Expr::makeTable(
			parser->getSQLAllocator(), select->targetName_, NULL);
	if (yymsp[-1].minor.yy0.size_ > 0) {
		table->aliasName_ = SyntaxTree::tokenToString(
				parser->getSQLAllocator(), yymsp[-1].minor.yy0, true);
	}
	parser->tableList_.push_back(table);

	select->from_ = table;

	parser->setTopSelect(select);

	parser->currentSelect_ = select;
	if (parser->childrenList_.size() > 0) {
		for (size_t pos = 0; pos < parser->childrenList_.size(); ++pos) {
			parser->childrenList_[pos]->parent_ = select;
		}
		parser->childrenList_.clear();
	}
}
        break;
      case 209: /* cmd ::= hint_opt UPDATE hint_opt orconf fullname as SET setlist where_opt */
{
	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
		parser->getSQLAllocator(), SyntaxTree::CMD_UPDATE);
	select->targetName_ = yymsp[-4].minor.yy142;
	select->updateSetList_ = yymsp[-1].minor.yy22;
	select->where_ = yymsp[0].minor.yy362;
	select->cmdOptionValue_ = yymsp[-5].minor.yy310;
	if (yymsp[-8].minor.yy22 || yymsp[-6].minor.yy22) {
		select->hintList_ = yymsp[-8].minor.yy22 ? yymsp[-8].minor.yy22 : yymsp[-6].minor.yy22;
	}
	SyntaxTree::Expr* table = SyntaxTree::Expr::makeTable(
			parser->getSQLAllocator(), select->targetName_, NULL);
	if (yymsp[-3].minor.yy0.size_ > 0) {
		table->aliasName_ = SyntaxTree::tokenToString(
				parser->getSQLAllocator(), yymsp[-3].minor.yy0, true);
	}
	parser->tableList_.push_back(table);

	select->from_ = table;

	parser->setTopSelect(select);

	parser->currentSelect_ = select;
	if (parser->childrenList_.size() > 0) {
		for (size_t pos = 0; pos < parser->childrenList_.size(); ++pos) {
			parser->childrenList_[pos]->parent_ = select;
		}
		parser->childrenList_.clear();
	}
}
        break;
      case 210: /* setlist ::= setlist COMMA nm EQ expr */
{
	if (!yymsp[-4].minor.yy22) {
		yymsp[-4].minor.yy22 = ALLOC_NEW(parser->getSQLAllocator())
				SyntaxTree::ExprList(parser->getSQLAllocator());
	}
	SyntaxTree::Expr* opExpr = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::OP_EQ);

	SyntaxTree::Expr* nameExpr = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_COLUMN);

	nameExpr->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[-2].minor.yy0);

	if (!nameExpr->qName_->name_ || nameExpr->qName_->name_->size() == 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-2].minor.yy0,
				"Column name must not be empty");
	}
	opExpr->left_ = nameExpr;
	opExpr->right_ = yymsp[0].minor.yy362;
	yygotominor.yy22 = yymsp[-4].minor.yy22;
	yygotominor.yy22->push_back(opExpr);
}
        break;
      case 211: /* setlist ::= nm EQ expr */
{
	yygotominor.yy22 = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());
	SyntaxTree::Expr* opExpr = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::OP_EQ);

	SyntaxTree::Expr* nameExpr = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_COLUMN);

	nameExpr->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[-2].minor.yy0);

	if (!nameExpr->qName_->name_ || nameExpr->qName_->name_->size() == 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-2].minor.yy0,
				"Column name must not be empty");
	}
	opExpr->left_ = nameExpr;
	opExpr->right_ = yymsp[0].minor.yy362;
	yygotominor.yy22->push_back(opExpr);
}
        break;
      case 212: /* cmd ::= hint_opt insert_cmd hint_opt INTO fullname inscollist_opt select */
{
	SyntaxTree::Select* select = SyntaxTree::Select::makeSelect(
			parser->getSQLAllocator(), SyntaxTree::CMD_INSERT);
	select->targetName_ = yymsp[-2].minor.yy142;
	select->insertSet_ = yymsp[0].minor.yy372;
	select->insertList_ = yymsp[-1].minor.yy22;
	select->cmdOptionValue_ = yymsp[-5].minor.yy562;
	if (yymsp[-6].minor.yy22 || yymsp[-4].minor.yy22) {
		select->hintList_ = yymsp[-6].minor.yy22 ? yymsp[-6].minor.yy22 : yymsp[-4].minor.yy22;
	}

	SyntaxTree::Expr* table = SyntaxTree::Expr::makeTable(
			parser->getSQLAllocator(), select->targetName_, NULL);
	parser->tableList_.push_back(table);

	parser->setTopSelect(select);

	parser->currentSelect_ = select;
	if (parser->childrenList_.size() > 0) {
		for (size_t pos = 0; pos < parser->childrenList_.size(); ++pos) {
			parser->childrenList_[pos]->parent_ = select;
		}
		parser->childrenList_.clear();
	}

	if (yymsp[0].minor.yy372 && yymsp[0].minor.yy372->right_ && yymsp[0].minor.yy372->right_->hintList_) {
		SQLToken token;
		if (yymsp[0].minor.yy372->right_->hintList_->size() > 0) {
			token = yymsp[0].minor.yy372->right_->hintList_->at(0)->startToken_;
		}
		else {
			token = SyntaxTree::makeEmptyToken();
		}
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				token, "Invalid hint found");
	}
}
        break;
      case 213: /* insert_cmd ::= INSERT orconf */
{yygotominor.yy562 = static_cast<uint8_t>(yymsp[0].minor.yy310);}
        break;
      case 214: /* insert_cmd ::= REPLACE */
{yygotominor.yy562 = SyntaxTree::RESOLVETYPE_REPLACE;}
        break;
      case 217: /* idlist ::= idlist COMMA nm */
{
	if (!yymsp[-2].minor.yy22) {
		yymsp[-2].minor.yy22 = ALLOC_NEW(parser->getSQLAllocator())
				SyntaxTree::ExprList(parser->getSQLAllocator());
	}
	SyntaxTree::Expr* nameExpr = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_COLUMN);

	nameExpr->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[0].minor.yy0);

	if (!nameExpr->qName_->name_ || nameExpr->qName_->name_->size() == 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
				"Column name must not be empty");
	}
	yygotominor.yy22 = yymsp[-2].minor.yy22;
	yygotominor.yy22->push_back(nameExpr);
}
        break;
      case 218: /* idlist ::= nm */
{
	yygotominor.yy22 = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());

	SyntaxTree::Expr* nameExpr = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_COLUMN);

	nameExpr->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), parser->viewNsId_, NULL, NULL, &yymsp[0].minor.yy0);

	if (!nameExpr->qName_->name_ || nameExpr->qName_->name_->size() == 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
				"Column name must not be empty");
	}
	yygotominor.yy22->push_back(nameExpr);
}
        break;
      case 220: /* expr ::= LP expr RP */
{
	/* yymsp[-2].minor.yy0, yymsp[0].minor.yy0: Token */
	yygotominor.yy362 = yymsp[-1].minor.yy362;
	yygotominor.yy362->startToken_ = yymsp[-2].minor.yy0;
	yygotominor.yy362->endToken_ = yymsp[0].minor.yy0;
}
        break;
      case 221: /* term ::= NULL */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_CONSTANT);
	yygotominor.yy362->value_ = TupleValue(&SyntaxTree::NULL_VALUE_RAW_DATA, TupleList::TYPE_ANY);
}
        break;
      case 225: /* term ::= INTEGER */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_CONSTANT);
	yygotominor.yy362->startToken_ = yymsp[0].minor.yy0;

	int64_t int64Value;
	bool succeeded = SQLProcessor::ValueUtils::toLong(
		yymsp[0].minor.yy0.value_, yymsp[0].minor.yy0.size_, int64Value);
	if (succeeded) {
		yygotominor.yy362->value_ = TupleValue(int64Value);
	}
	else {
		double doubleValue;
		bool succeeded = SQLProcessor::ValueUtils::toDouble(
				yymsp[0].minor.yy0.value_, yymsp[0].minor.yy0.size_, doubleValue);
		if (succeeded) {
			yygotominor.yy362->value_ = TupleValue(doubleValue);
		}
		else {
			SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
								   yymsp[0].minor.yy0, "Invalid constant value");
		}
	}
}
        break;
      case 226: /* term ::= FLOAT */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_CONSTANT);

	yygotominor.yy362->startToken_ = yymsp[0].minor.yy0;
	double doubleValue;
	bool succeeded = SQLProcessor::ValueUtils::toDouble(
			yymsp[0].minor.yy0.value_, yymsp[0].minor.yy0.size_, doubleValue);
	if (succeeded) {
		yygotominor.yy362->value_ = TupleValue(doubleValue);
	}
	else {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
							   yymsp[0].minor.yy0, "Invalid constant value");
	}
}
        break;
      case 227: /* term ::= TRUE */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_CONSTANT);
	yygotominor.yy362->value_ = TupleValue(true);
	yygotominor.yy362->startToken_ = yymsp[0].minor.yy0;
}
        break;
      case 228: /* term ::= FALSE */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_CONSTANT);
	yygotominor.yy362->value_ = TupleValue(false);
	yygotominor.yy362->startToken_ = yymsp[0].minor.yy0;
}
        break;
      case 229: /* term ::= BLOB */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_CONSTANT);
	assert(yymsp[0].minor.yy0.size_ >= 3); 
	util::String hexStr(yymsp[0].minor.yy0.value_ + 2, yymsp[0].minor.yy0.value_ + yymsp[0].minor.yy0.size_ - 1, parser->getSQLAllocator());
	util::XArray<char> buffer(parser->getSQLAllocator());
	char temp = 0;
	buffer.assign(hexStr.size() / 2 + 1, temp);
	size_t byteSize = util::HexConverter::decode(buffer.data(), hexStr.c_str(), hexStr.size());
	yygotominor.yy362->value_ = SyntaxTree::makeBlobValue(
		parser->getVarContext(), buffer.data(), byteSize);
	yygotominor.yy362->startToken_ = yymsp[0].minor.yy0;
}
        break;
      case 230: /* term ::= STRING */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_CONSTANT);
	util::String* str = SyntaxTree::tokenToString(
			parser->getSQLAllocator(), yymsp[0].minor.yy0, true);
	if (!str) {
		str = ALLOC_NEW(parser->getSQLAllocator())
				util::String("", parser->getSQLAllocator());
	}
	yygotominor.yy362->value_ = SyntaxTree::makeStringValue(
			parser->getSQLAllocator(), str->c_str(), str->size());
	yygotominor.yy362->startToken_ = yymsp[0].minor.yy0;
}
        break;
      case 231: /* expr ::= VARIABLE */
{
	/* yymsp[0].minor.yy0: Token */
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_PLACEHOLDER);
	yygotominor.yy362->placeHolderCount_ = ++parser->placeHolderCount_;
	yygotominor.yy362->startToken_ = yymsp[0].minor.yy0;
}
        break;
      case 232: /* expr ::= CAST LP expr AS typetoken RP */
{
	/* yymsp[-5].minor.yy0, yymsp[0].minor.yy0: Token */

	TupleList::TupleColumnType colType = yymsp[-1].minor.yy161;
	if (colType == TupleList::TYPE_NULL) {
		SQL_PARSER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-5].minor.yy0,
				"NULL type must not be specified on CAST expression");
	}
	SyntaxTree::Expr *typeExpr = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_TYPE);
	typeExpr->columnType_ = colType;

	yygotominor.yy362 = SyntaxTree::Expr::makeBinaryExpr(*parser, 
			SQLType::OP_CAST, yymsp[-3].minor.yy362, typeExpr);
	yygotominor.yy362->startToken_ = yymsp[-5].minor.yy0;
	yygotominor.yy362->endToken_ = yymsp[0].minor.yy0;
}
        break;
      case 233: /* expr ::= EXTRACT LP field COMMA exprlist RP */
      case 234: /* expr ::= TIMESTAMPADD LP field COMMA exprlist RP */ yytestcase(yyruleno==234);
      case 235: /* expr ::= TIMESTAMPDIFF LP field COMMA exprlist RP */ yytestcase(yyruleno==235);
      case 236: /* expr ::= TIMESTAMP_TRUNC LP field COMMA exprlist RP */ yytestcase(yyruleno==236);
{
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_FUNCTION);
	yygotominor.yy362->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), 0, NULL, NULL, &yymsp[-5].minor.yy0);

	assert(yymsp[-3].minor.yy362);
	yygotominor.yy362->next_ = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());
	yygotominor.yy362->next_->push_back(yymsp[-3].minor.yy362);
	yygotominor.yy362->next_->reserve(yygotominor.yy362->next_->size() + yymsp[-1].minor.yy22->size());
	std::copy(yymsp[-1].minor.yy22->begin(), yymsp[-1].minor.yy22->end(), std::back_inserter(*yygotominor.yy362->next_));
	yygotominor.yy362->startToken_ = yymsp[-5].minor.yy0;
	yygotominor.yy362->endToken_ = yymsp[0].minor.yy0;
}
        break;
      case 237: /* field ::= fieldType */
{
	const util::DateTime::FieldType unit = yymsp[0].minor.yy380.unit_;
	const SQLToken &unitToken = yymsp[0].minor.yy380.token_;

	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_CONSTANT);
	yygotominor.yy362->value_ = TupleValue(static_cast<int64_t>(unit));
	yygotominor.yy362->startToken_ = unitToken;
}
        break;
      case 238: /* fieldType ::= YEAR */
{
	yygotominor.yy380 = DateTimeField::of(util::DateTime::FIELD_YEAR, yymsp[0].minor.yy0);
}
        break;
      case 239: /* fieldType ::= MONTH */
{
	yygotominor.yy380 = DateTimeField::of(util::DateTime::FIELD_MONTH, yymsp[0].minor.yy0);
}
        break;
      case 240: /* fieldType ::= DAY */
{
	yygotominor.yy380 = DateTimeField::of(util::DateTime::FIELD_DAY_OF_MONTH, yymsp[0].minor.yy0);
}
        break;
      case 241: /* fieldType ::= HOUR */
{
	yygotominor.yy380 = DateTimeField::of(util::DateTime::FIELD_HOUR, yymsp[0].minor.yy0);
}
        break;
      case 242: /* fieldType ::= MINUTE */
{
	yygotominor.yy380 = DateTimeField::of(util::DateTime::FIELD_MINUTE, yymsp[0].minor.yy0);
}
        break;
      case 243: /* fieldType ::= SECOND */
{
	yygotominor.yy380 = DateTimeField::of(util::DateTime::FIELD_SECOND, yymsp[0].minor.yy0);
}
        break;
      case 244: /* fieldType ::= MILLISECOND */
{
	yygotominor.yy380 = DateTimeField::of(util::DateTime::FIELD_MILLISECOND, yymsp[0].minor.yy0);
}
        break;
      case 245: /* fieldType ::= MICROSECOND */
{
	yygotominor.yy380 = DateTimeField::of(util::DateTime::FIELD_MICROSECOND, yymsp[0].minor.yy0);
}
        break;
      case 246: /* fieldType ::= NANOSECOND */
{
	yygotominor.yy380 = DateTimeField::of(util::DateTime::FIELD_NANOSECOND, yymsp[0].minor.yy0);
}
        break;
      case 247: /* fieldType ::= DAY_OF_WEEK */
{
	yygotominor.yy380 = DateTimeField::of(util::DateTime::FIELD_DAY_OF_WEEK, yymsp[0].minor.yy0);
}
        break;
      case 248: /* fieldType ::= DAY_OF_YEAR */
{
	yygotominor.yy380 = DateTimeField::of(util::DateTime::FIELD_DAY_OF_YEAR, yymsp[0].minor.yy0);
}
        break;
      case 249: /* fieldType ::= ID */
{
	util::String* fieldStr = SyntaxTree::tokenToString(
			parser->getSQLAllocator(), yymsp[0].minor.yy0, false);
	SQL_PARSER_THROW_ERROR(
			GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[0].minor.yy0,
			"Unknown field: " << *fieldStr);
}
        break;
      case 250: /* expr ::= ID LP distinct exprlist RP window_opt */
{
	/* yymsp[-4].minor.yy0,yymsp[-1].minor.yy0: Token */
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_FUNCTION);
	yygotominor.yy362->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), 0, NULL, NULL, &yymsp[-5].minor.yy0);
	if (!yygotominor.yy362->qName_->name_) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-5].minor.yy0,
				"Zero-length delimited identifier");
	}
	yygotominor.yy362->next_ = yymsp[-2].minor.yy22;
	if (yymsp[-3].minor.yy296) {
		yygotominor.yy362->aggrOpts_ = SyntaxTree::AGGR_OPT_DISTINCT;
	}
	if (yymsp[0].minor.yy19) {
#if SQL_PARSER_ENABLE_WINDOW_FUNCTION
		if (yymsp[-3].minor.yy296) {
			SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-4].minor.yy0,
					"Use of DISTINCT is not allowed with the OVER clause");
		}
		yygotominor.yy362->windowOpt_ = yymsp[0].minor.yy19;
#else
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-1].minor.yy0,
				"Not supported");
#endif
	}
	yygotominor.yy362->startToken_ = yymsp[-5].minor.yy0;
	yygotominor.yy362->endToken_ = yymsp[-1].minor.yy0;
}
        break;
      case 251: /* expr ::= ID LP STAR RP window_opt */
{ 
	/* yymsp[-1].minor.yy0: Token */
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_FUNCTION);
	yygotominor.yy362->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), 0, NULL, NULL, &yymsp[-4].minor.yy0);

	yygotominor.yy362->next_ = ALLOC_NEW(parser->getSQLAllocator())
			ExprList(parser->getSQLAllocator());
	SyntaxTree::Expr* star = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_ALL_COLUMN);
	yygotominor.yy362->next_->push_back(star);
	if (yymsp[0].minor.yy19) {
#if SQL_PARSER_ENABLE_WINDOW_FUNCTION
		yygotominor.yy362->windowOpt_ = yymsp[0].minor.yy19;
#else
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-1].minor.yy0,
				"Not supported");
#endif
	}
	yygotominor.yy362->startToken_ = yymsp[-4].minor.yy0;
	yygotominor.yy362->endToken_ = yymsp[-1].minor.yy0;
}
        break;
      case 252: /* window_opt ::= */
{yygotominor.yy19 = 0;}
        break;
      case 253: /* window_opt ::= OVER LP partitionby_opt orderby_opt RP frame_opt */
{
	yygotominor.yy19 = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::WindowOption(parser->getSQLAllocator());

	yygotominor.yy19->partitionByList_ = yymsp[-3].minor.yy22;
	yygotominor.yy19->orderByList_ = yymsp[-2].minor.yy22;
	yygotominor.yy19->frameOption_ = yymsp[0].minor.yy520;
}
        break;
      case 256: /* frame_opt ::= */
{yygotominor.yy520 = 0;}
        break;
      case 257: /* frame_opt ::= ROWS frame_boundary */
{
	yygotominor.yy520 = ALLOC_NEW(parser->getSQLAllocator()) SyntaxTree::WindowFrameOption();
	yygotominor.yy520->frameMode_ = SyntaxTree::WindowFrameOption::FRAME_MODE_ROW;
	yygotominor.yy520->frameStartBoundary_ = yymsp[0].minor.yy345;
	yygotominor.yy520->frameFinishBoundary_ = 0;
}
        break;
      case 258: /* frame_opt ::= RANGE frame_boundary */
{
	yygotominor.yy520 = ALLOC_NEW(parser->getSQLAllocator()) SyntaxTree::WindowFrameOption();
	yygotominor.yy520->frameMode_ = SyntaxTree::WindowFrameOption::FRAME_MODE_RANGE;
	yygotominor.yy520->frameStartBoundary_ = yymsp[0].minor.yy345;
	yygotominor.yy520->frameFinishBoundary_ = 0;
}
        break;
      case 259: /* frame_opt ::= ROWS BETWEEN frame_boundary AND frame_boundary */
{
	yygotominor.yy520 = ALLOC_NEW(parser->getSQLAllocator()) SyntaxTree::WindowFrameOption();
	yygotominor.yy520->frameMode_ = SyntaxTree::WindowFrameOption::FRAME_MODE_ROW;
	yygotominor.yy520->frameStartBoundary_ = yymsp[-2].minor.yy345;
	yygotominor.yy520->frameFinishBoundary_ = yymsp[0].minor.yy345;
}
        break;
      case 260: /* frame_opt ::= RANGE BETWEEN frame_boundary AND frame_boundary */
{
	yygotominor.yy520 = ALLOC_NEW(parser->getSQLAllocator()) SyntaxTree::WindowFrameOption();
	yygotominor.yy520->frameMode_ = SyntaxTree::WindowFrameOption::FRAME_MODE_RANGE;
	yygotominor.yy520->frameStartBoundary_ = yymsp[-2].minor.yy345;
	yygotominor.yy520->frameFinishBoundary_ = yymsp[0].minor.yy345;
}
        break;
      case 261: /* frame_boundary ::= UNBOUNDED PRECEDING */
      case 268: /* frame_boundary ::= PRECEDING */ yytestcase(yyruleno==268);
{
	yygotominor.yy345 = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::WindowFrameBoundary(parser->getSQLAllocator());
	yygotominor.yy345->boundaryType_ = SyntaxTree::WindowFrameBoundary::BOUNDARY_UNBOUNDED_PRECEDING;
	yygotominor.yy345->boundaryValueExpr_ = 0;
}
        break;
      case 262: /* frame_boundary ::= UNBOUNDED FOLLOWING */
      case 269: /* frame_boundary ::= FOLLOWING */ yytestcase(yyruleno==269);
{
	yygotominor.yy345 = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::WindowFrameBoundary(parser->getSQLAllocator());
	yygotominor.yy345->boundaryType_ = SyntaxTree::WindowFrameBoundary::BOUNDARY_UNBOUNDED_FOLLOWING;
	yygotominor.yy345->boundaryValueExpr_ = 0;
}
        break;
      case 263: /* frame_boundary ::= CURRENT ROW */
{
	yygotominor.yy345 = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::WindowFrameBoundary(parser->getSQLAllocator());
	yygotominor.yy345->boundaryType_ = SyntaxTree::WindowFrameBoundary::BOUNDARY_CURRENT_ROW;
	yygotominor.yy345->boundaryValueExpr_ = 0;
}
        break;
      case 264: /* frame_boundary ::= expr PRECEDING */
{
	yygotominor.yy345 = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::WindowFrameBoundary(parser->getSQLAllocator());
	yygotominor.yy345->boundaryType_ = SyntaxTree::WindowFrameBoundary::BOUNDARY_N_PRECEDING;
	yygotominor.yy345->boundaryValueExpr_ = yymsp[-1].minor.yy362;
}
        break;
      case 265: /* frame_boundary ::= expr FOLLOWING */
{
	yygotominor.yy345 = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::WindowFrameBoundary(parser->getSQLAllocator());
	yygotominor.yy345->boundaryType_ = SyntaxTree::WindowFrameBoundary::BOUNDARY_N_FOLLOWING;
	yygotominor.yy345->boundaryValueExpr_ = yymsp[-1].minor.yy362;
}
        break;
      case 266: /* frame_boundary ::= LP INTEGER COMMA fieldType RP PRECEDING */
{
	yygotominor.yy345 = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::WindowFrameBoundary(parser->getSQLAllocator());
	yygotominor.yy345->boundaryType_ = SyntaxTree::WindowFrameBoundary::BOUNDARY_N_PRECEDING;

	const util::DateTime::FieldType unit = yymsp[-2].minor.yy380.unit_;
	const SQLToken &unitToken = yymsp[-2].minor.yy380.token_;
	if (!SyntaxTree::checkGroupIntervalTimeField(unit)) {
		SQL_PARSER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, unitToken,
				"Specified unit is not allowed");
	}
	/* yymsp[-4].minor.yy0: Token */
	int64_t int64Value;
	bool succeeded = SQLProcessor::ValueUtils::toLong(
			yymsp[-4].minor.yy0.value_, yymsp[-4].minor.yy0.size_, int64Value);
	if (!succeeded || int64Value <= 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-4].minor.yy0,
				"Frame range value must be a positive integer");
	}
	yygotominor.yy345->boundaryLongValue_ = int64Value;
	yygotominor.yy345->boundaryTimeUnit_ = static_cast<int64_t>(unit);
}
        break;
      case 267: /* frame_boundary ::= LP INTEGER COMMA fieldType RP FOLLOWING */
{
	yygotominor.yy345 = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::WindowFrameBoundary(parser->getSQLAllocator());
	yygotominor.yy345->boundaryType_ = SyntaxTree::WindowFrameBoundary::BOUNDARY_N_FOLLOWING;

	const util::DateTime::FieldType unit = yymsp[-2].minor.yy380.unit_;
	const SQLToken &unitToken = yymsp[-2].minor.yy380.token_;
	if (!SyntaxTree::checkGroupIntervalTimeField(unit)) {
		SQL_PARSER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, unitToken,
				"Specified unit is not allowed");
	}
	/* yymsp[-4].minor.yy0: Token */
	int64_t int64Value;
	bool succeeded = SQLProcessor::ValueUtils::toLong(
			yymsp[-4].minor.yy0.value_, yymsp[-4].minor.yy0.size_, int64Value);
	if (!succeeded || int64Value <= 0) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-4].minor.yy0,
				"Frame range value must be a positive integer");
	}
	yygotominor.yy345->boundaryLongValue_ = int64Value;
	yygotominor.yy345->boundaryTimeUnit_ = static_cast<int64_t>(unit);
}
        break;
      case 270: /* expr ::= ID LP distinct exprlist RP WITHIN GROUP LP ORDER BY sortlist RP */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_FUNCTION);
	yygotominor.yy362->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), 0, NULL, NULL, &yymsp[-11].minor.yy0);
	if (!yygotominor.yy362->qName_->name_) {
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-11].minor.yy0,
				"Zero-length delimited identifier");
	}
	yygotominor.yy362->next_ = yymsp[-8].minor.yy22;

	if (yymsp[-9].minor.yy296) {
		SQL_PARSER_THROW_ERROR(
				GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, yymsp[-10].minor.yy0,
				"Use of DISTINCT is not allowed with the WITHIN GROUP clause");
	}

	SyntaxTree::Expr *orderingExpr = SyntaxTree::Expr::makeExpr(
			parser->getSQLAllocator(), SQLType::EXPR_AGG_ORDERING);
	orderingExpr->next_ = yymsp[-1].minor.yy22;

	yygotominor.yy362->next_->insert(yygotominor.yy362->next_->begin(), orderingExpr);
	yygotominor.yy362->startToken_ = yymsp[-11].minor.yy0;
	yygotominor.yy362->endToken_ = yymsp[0].minor.yy0;
}
        break;
      case 271: /* expr ::= expr AND expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeBinaryExpr(
			*parser, SQLType::EXPR_AND, yymsp[-2].minor.yy362, yymsp[0].minor.yy362);
}
        break;
      case 272: /* expr ::= expr OR expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeBinaryExpr(
			*parser, SQLType::EXPR_OR, yymsp[-2].minor.yy362, yymsp[0].minor.yy362);
}
        break;
      case 273: /* expr ::= expr LT expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeBinaryExpr(
			*parser, SQLType::OP_LT, yymsp[-2].minor.yy362, yymsp[0].minor.yy362);
}
        break;
      case 274: /* expr ::= expr GT expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeBinaryExpr(
			*parser, SQLType::OP_GT, yymsp[-2].minor.yy362, yymsp[0].minor.yy362);
}
        break;
      case 275: /* expr ::= expr GE expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeBinaryExpr(
			*parser, SQLType::OP_GE, yymsp[-2].minor.yy362, yymsp[0].minor.yy362);
}
        break;
      case 276: /* expr ::= expr LE expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeBinaryExpr(
			*parser, SQLType::OP_LE, yymsp[-2].minor.yy362, yymsp[0].minor.yy362);
}
        break;
      case 277: /* expr ::= expr EQ expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeBinaryExpr(
			*parser, SQLType::OP_EQ, yymsp[-2].minor.yy362, yymsp[0].minor.yy362);
}
        break;
      case 278: /* expr ::= expr NE expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeBinaryExpr(
			*parser, SQLType::OP_NE, yymsp[-2].minor.yy362, yymsp[0].minor.yy362);
}
        break;
      case 279: /* expr ::= expr BITAND expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeBinaryExpr(
			*parser, SQLType::OP_BIT_AND, yymsp[-2].minor.yy362, yymsp[0].minor.yy362);
}
        break;
      case 280: /* expr ::= expr BITOR expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeBinaryExpr(
			*parser, SQLType::OP_BIT_OR, yymsp[-2].minor.yy362, yymsp[0].minor.yy362);
}
        break;
      case 281: /* expr ::= expr LSHIFT expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeBinaryExpr(
			*parser, SQLType::OP_SHIFT_LEFT, yymsp[-2].minor.yy362, yymsp[0].minor.yy362);
}
        break;
      case 282: /* expr ::= expr RSHIFT expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeBinaryExpr(
			*parser, SQLType::OP_SHIFT_RIGHT, yymsp[-2].minor.yy362, yymsp[0].minor.yy362);
}
        break;
      case 283: /* expr ::= expr PLUS expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeBinaryExpr(
			*parser, SQLType::OP_ADD, yymsp[-2].minor.yy362, yymsp[0].minor.yy362);
}
        break;
      case 284: /* expr ::= expr MINUS expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeBinaryExpr(
			*parser, SQLType::OP_SUBTRACT, yymsp[-2].minor.yy362, yymsp[0].minor.yy362);
}
        break;
      case 285: /* expr ::= expr STAR expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeBinaryExpr(
			*parser, SQLType::OP_MULTIPLY, yymsp[-2].minor.yy362, yymsp[0].minor.yy362);
}
        break;
      case 286: /* expr ::= expr SLASH expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeBinaryExpr(
			*parser, SQLType::OP_DIVIDE, yymsp[-2].minor.yy362, yymsp[0].minor.yy362);
}
        break;
      case 287: /* expr ::= expr REM expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeBinaryExpr(
			*parser, SQLType::OP_REMAINDER, yymsp[-2].minor.yy362, yymsp[0].minor.yy362);
}
        break;
      case 288: /* expr ::= expr CONCAT expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeBinaryExpr(
			*parser, SQLType::OP_CONCAT, yymsp[-2].minor.yy362, yymsp[0].minor.yy362);
}
        break;
      case 289: /* likeop ::= LIKE_KW|MATCH */
{
	yygotominor.yy528.operator_ = yymsp[0].minor.yy0;
	yygotominor.yy528.existNot_ = false;
}
        break;
      case 290: /* likeop ::= NOT LIKE_KW|MATCH */
{
	yygotominor.yy528.operator_ = yymsp[0].minor.yy0;
	yygotominor.yy528.existNot_ = true;
}
        break;
      case 291: /* expr ::= expr likeop expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_FUNCTION);
	yygotominor.yy362->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), 0, NULL, NULL, &yymsp[-1].minor.yy528.operator_);
	yygotominor.yy362->next_ = ALLOC_NEW(parser->getSQLAllocator())
			ExprList(parser->getSQLAllocator());
	yygotominor.yy362->next_->push_back(yymsp[0].minor.yy362); 
	yygotominor.yy362->next_->push_back(yymsp[-2].minor.yy362);
	if ( yymsp[-1].minor.yy528.existNot_ ) {
		SyntaxTree::Expr* likeBody = yygotominor.yy362;
		yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::OP_NOT);
		yygotominor.yy362->left_ = likeBody;
	}
	if (yymsp[-2].minor.yy362) { yygotominor.yy362->startToken_ = yymsp[-2].minor.yy362->startToken_; }
	if (yymsp[0].minor.yy362) { yygotominor.yy362->endToken_ = yymsp[0].minor.yy362->endToken_; }
}
        break;
      case 292: /* expr ::= expr likeop expr ESCAPE expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_FUNCTION);
	yygotominor.yy362->qName_ = SyntaxTree::QualifiedName::makeQualifiedName(
			parser->getSQLAllocator(), 0, NULL, NULL, &yymsp[-3].minor.yy528.operator_);

	yygotominor.yy362->next_ = ALLOC_NEW(parser->getSQLAllocator())
			ExprList(parser->getSQLAllocator());
	yygotominor.yy362->next_->push_back(yymsp[-2].minor.yy362); 
	yygotominor.yy362->next_->push_back(yymsp[-4].minor.yy362);
	yygotominor.yy362->next_->push_back(yymsp[0].minor.yy362);
	if ( yymsp[-3].minor.yy528.existNot_ ) {
		SyntaxTree::Expr* likeBody = yygotominor.yy362;
		yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::OP_NOT);
		yygotominor.yy362->left_ = likeBody;
	}
	if (yymsp[-4].minor.yy362) { yygotominor.yy362->startToken_ = yymsp[-4].minor.yy362->startToken_; }
	if (yymsp[0].minor.yy362) { yygotominor.yy362->endToken_ = yymsp[0].minor.yy362->endToken_; }
}
        break;
      case 293: /* expr ::= expr ISNULL */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeUnaryExpr(
			*parser, SQLType::OP_IS_NULL, yymsp[-1].minor.yy362);
}
        break;
      case 294: /* expr ::= expr NOTNULL */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeUnaryExpr(
			*parser, SQLType::OP_IS_NOT_NULL, yymsp[-1].minor.yy362);
}
        break;
      case 295: /* expr ::= expr NOT NULL */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeUnaryExpr(
			*parser, SQLType::OP_IS_NOT_NULL, yymsp[-2].minor.yy362);
}
        break;
      case 296: /* expr ::= expr IS expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeBinaryExpr(
			*parser, SQLType::OP_IS, yymsp[-2].minor.yy362, yymsp[0].minor.yy362);
}
        break;
      case 297: /* expr ::= expr IS NOT expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeBinaryExpr(
			*parser, SQLType::OP_IS_NOT, yymsp[-3].minor.yy362, yymsp[0].minor.yy362);
}
        break;
      case 298: /* expr ::= NOT expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeUnaryExpr(
			*parser, SQLType::OP_NOT, yymsp[0].minor.yy362);
}
        break;
      case 299: /* expr ::= BITNOT expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeUnaryExpr(
			*parser, SQLType::OP_BIT_NOT, yymsp[0].minor.yy362);
}
        break;
      case 300: /* expr ::= MINUS expr */
{
	if (yymsp[0].minor.yy362 && yymsp[0].minor.yy362->op_ == SQLType::EXPR_CONSTANT) {
		SQLToken T = yymsp[0].minor.yy362->startToken_;
		if (T.id_ == TK_INTEGER 
			&& (((yymsp[0].minor.yy362->value_.getType() == TupleList::TYPE_LONG)
					&& (yymsp[0].minor.yy362->value_.get<int64_t>() > 0))
				|| ((yymsp[0].minor.yy362->value_.getType() == TupleList::TYPE_DOUBLE)
					&& (yymsp[0].minor.yy362->value_.get<double>() > 0)))) {
			yygotominor.yy362 = yymsp[0].minor.yy362;
			util::String str(yymsp[-1].minor.yy0.value_, yymsp[-1].minor.yy0.value_ + yymsp[-1].minor.yy0.size_, parser->getSQLAllocator());
			str.append(T.value_, T.size_);
			int64_t int64Value;
			bool succeeded = SQLProcessor::ValueUtils::toLong(
				str.c_str(), str.size(), int64Value);
			if (succeeded) {
				yygotominor.yy362->value_ = TupleValue(int64Value);
			}
			else {
				double doubleValue;
				bool succeeded = SQLProcessor::ValueUtils::toDouble(
					str.c_str(), str.size(), doubleValue);
				if (succeeded) {
					yygotominor.yy362->value_ = TupleValue(doubleValue);
				}
				else {
					SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
										   T, "Invalid constant value");
				}
			}
		}
		else if (T.id_ == TK_FLOAT
				&& yymsp[0].minor.yy362->value_.getType() == TupleList::TYPE_DOUBLE
				&& yymsp[0].minor.yy362->value_.get<double>() > 0) {
			yygotominor.yy362 = yymsp[0].minor.yy362;
			util::String str(yymsp[-1].minor.yy0.value_, yymsp[-1].minor.yy0.value_ + yymsp[-1].minor.yy0.size_, parser->getSQLAllocator());
			str.append(T.value_, T.size_);
			double doubleValue;
			bool succeeded = SQLProcessor::ValueUtils::toDouble(
				str.c_str(), str.size(), doubleValue);
			if (succeeded) {
				yygotominor.yy362->value_ = TupleValue(doubleValue);
			}
			else {
				SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
									   T, "Invalid constant value");
			}
		}
		else {
			yygotominor.yy362 = SyntaxTree::Expr::makeUnaryExpr(
					*parser, SQLType::OP_MINUS, yymsp[0].minor.yy362);
		}
	}
	else {
		yygotominor.yy362 = SyntaxTree::Expr::makeUnaryExpr(
				*parser, SQLType::OP_MINUS, yymsp[0].minor.yy362);
	}
}
        break;
      case 301: /* expr ::= PLUS expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeUnaryExpr(
			*parser, SQLType::OP_PLUS, yymsp[0].minor.yy362);
}
        break;
      case 304: /* expr ::= expr between_op expr AND expr */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_BETWEEN);
	yygotominor.yy362->next_ = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());
	yygotominor.yy362->next_->reserve(3);
	yygotominor.yy362->next_->push_back(yymsp[-4].minor.yy362);
	yygotominor.yy362->next_->push_back(yymsp[-2].minor.yy362);
	yygotominor.yy362->next_->push_back(yymsp[0].minor.yy362);
	if (yymsp[-3].minor.yy310) {
		SyntaxTree::Expr* body = yygotominor.yy362;
		yygotominor.yy362 = SyntaxTree::Expr::makeUnaryExpr(
				*parser, SQLType::OP_NOT, body);
	}
	if (yymsp[-4].minor.yy362) { yygotominor.yy362->startToken_ = yymsp[-4].minor.yy362->startToken_; }
	if (yymsp[0].minor.yy362) { yygotominor.yy362->endToken_ = yymsp[0].minor.yy362->startToken_; }
}
        break;
      case 307: /* expr ::= expr in_op LP exprlist RP */
{
	SyntaxTree::Expr* listExpr = SyntaxTree::Expr::makeExpr(
		parser->getSQLAllocator(), SQLType::EXPR_LIST);
	listExpr->next_ = yymsp[-1].minor.yy22;

	yygotominor.yy362 = SyntaxTree::Expr::makeBinaryExpr(
			*parser, SQLType::EXPR_IN, yymsp[-4].minor.yy362, listExpr);
	if (yymsp[-3].minor.yy310) {
		SyntaxTree::Expr* body = yygotominor.yy362;
		yygotominor.yy362 = SyntaxTree::Expr::makeUnaryExpr(
				*parser, SQLType::OP_NOT, body);
	}
	if (yymsp[-4].minor.yy362) { yygotominor.yy362->startToken_ = yymsp[-4].minor.yy362->startToken_; }
	yygotominor.yy362->endToken_ = yymsp[0].minor.yy0;
}
        break;
      case 308: /* expr ::= LP select RP */
{
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_SELECTION);
	yygotominor.yy362->subQuery_ = yymsp[-1].minor.yy372;

	yygotominor.yy362->startToken_ = yymsp[-2].minor.yy0;
	yygotominor.yy362->endToken_ = yymsp[0].minor.yy0;

	if (yymsp[-1].minor.yy372 && yymsp[-1].minor.yy372->right_ && yymsp[-1].minor.yy372->right_->hintList_) {
		SQLToken token;
		if (yymsp[-1].minor.yy372->right_->hintList_->size() > 0) {
			token = yymsp[-1].minor.yy372->right_->hintList_->at(0)->startToken_;
		}
		else {
			token = SyntaxTree::makeEmptyToken();
		}
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				token, "Invalid hint found");
	}
}
        break;
      case 309: /* expr ::= expr in_op LP select RP */
{
	SyntaxTree::Expr* selectExpr = SyntaxTree::Expr::makeExpr(
		parser->getSQLAllocator(), SQLType::EXPR_SELECTION);
	selectExpr->subQuery_ = yymsp[-1].minor.yy372;

	yygotominor.yy362 = SyntaxTree::Expr::makeBinaryExpr(
			*parser, SQLType::EXPR_IN, yymsp[-4].minor.yy362, selectExpr);
	if (yymsp[-3].minor.yy310) {
		SyntaxTree::Expr* body = yygotominor.yy362;
		yygotominor.yy362 = SyntaxTree::Expr::makeUnaryExpr(
				*parser, SQLType::OP_NOT, body);
	}
	if (yymsp[-4].minor.yy362) { yygotominor.yy362->startToken_ = yymsp[-4].minor.yy362->startToken_; }
	yygotominor.yy362->endToken_ = yymsp[0].minor.yy0;

	if (yymsp[-1].minor.yy372 && yymsp[-1].minor.yy372->right_ && yymsp[-1].minor.yy372->right_->hintList_) {
		SQLToken token;
		if (yymsp[-1].minor.yy372->right_->hintList_->size() > 0) {
			token = yymsp[-1].minor.yy372->right_->hintList_->at(0)->startToken_;
		}
		else {
			token = SyntaxTree::makeEmptyToken();
		}
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				token, "Invalid hint found");
	}
}
        break;
      case 310: /* expr ::= EXISTS LP select RP */
{
	/* yymsp[-3].minor.yy0, yymsp[0].minor.yy0: Token */
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_EXISTS);
	yygotominor.yy362->subQuery_ = yymsp[-1].minor.yy372;

	yygotominor.yy362->startToken_ = yymsp[-3].minor.yy0;
	yygotominor.yy362->endToken_ = yymsp[0].minor.yy0;

	if (yymsp[-1].minor.yy372 && yymsp[-1].minor.yy372->right_ && yymsp[-1].minor.yy372->right_->hintList_) {
		SQLToken token;
		if (yymsp[-1].minor.yy372->right_->hintList_->size() > 0) {
			token = yymsp[-1].minor.yy372->right_->hintList_->at(0)->startToken_;
		}
		else {
			token = SyntaxTree::makeEmptyToken();
		}
		SQL_PARSER_THROW_ERROR(GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
				token, "Invalid hint found");
	}
}
        break;
      case 311: /* expr ::= CASE case_operand case_exprlist case_else END */
{


	/* yymsp[-4].minor.yy0, yymsp[0].minor.yy0: Token */
	yygotominor.yy362 = SyntaxTree::Expr::makeExpr(parser->getSQLAllocator(), SQLType::EXPR_CASE);
	yygotominor.yy362->next_ = yymsp[-2].minor.yy22;
	if (yymsp[-3].minor.yy362 && yygotominor.yy362->next_) {
		SyntaxTree::ExprList::iterator itr = yygotominor.yy362->next_->begin();
		size_t count = 0;
		for (; itr != yygotominor.yy362->next_->end(); ++itr, ++count) {
			if (((count % 2) == 0) && *itr) { 
				SyntaxTree::Expr* newExpr = SyntaxTree::Expr::makeExpr(
					parser->getSQLAllocator(), SQLType::OP_EQ);
				newExpr->left_ = yymsp[-3].minor.yy362;
				newExpr->right_ = (*itr);
				(*itr) = newExpr;
			}
		}
	}
	if (yymsp[-1].minor.yy362) {
		yygotominor.yy362->next_->push_back(yymsp[-1].minor.yy362);
	}
	yygotominor.yy362->startToken_ = yymsp[-4].minor.yy0;
	yygotominor.yy362->endToken_ = yymsp[0].minor.yy0;
}
        break;
      case 312: /* case_exprlist ::= case_exprlist WHEN expr THEN expr */
{
	if (!yymsp[-4].minor.yy22) {
		yymsp[-4].minor.yy22 = ALLOC_NEW(parser->getSQLAllocator())
				SyntaxTree::ExprList(parser->getSQLAllocator());
	}
	yymsp[-4].minor.yy22->push_back(yymsp[-2].minor.yy362);
	if (yymsp[0].minor.yy362) {
		yymsp[-4].minor.yy22->push_back(yymsp[0].minor.yy362);
	}
	yygotominor.yy22 = yymsp[-4].minor.yy22;
}
        break;
      case 313: /* case_exprlist ::= WHEN expr THEN expr */
{
	yygotominor.yy22 = ALLOC_NEW(parser->getSQLAllocator())
			SyntaxTree::ExprList(parser->getSQLAllocator());
	yygotominor.yy22->push_back(yymsp[-2].minor.yy362);
	if (yymsp[0].minor.yy362) {
		yygotominor.yy22->push_back(yymsp[0].minor.yy362);
	}
}
        break;
      case 314: /* case_else ::= ELSE expr */
      case 316: /* case_operand ::= expr */ yytestcase(yyruleno==316);
{
	yygotominor.yy362 = yymsp[0].minor.yy362;
	if (yymsp[0].minor.yy362) {
		yygotominor.yy362->startToken_ = yymsp[0].minor.yy362->startToken_;
		yygotominor.yy362->endToken_ = yymsp[0].minor.yy362->endToken_;
	}
}
        break;
      case 320: /* nexprlist ::= nexprlist COMMA expr */
{
	if (!yymsp[-2].minor.yy22) {
		yymsp[-2].minor.yy22 = ALLOC_NEW(parser->getSQLAllocator())
				SyntaxTree::ExprList(parser->getSQLAllocator());
	}
	yymsp[-2].minor.yy22->push_back(yymsp[0].minor.yy362);
	yygotominor.yy22 = yymsp[-2].minor.yy22;
}
        break;
      default:
      /* (0) input ::= cmdlist */ yytestcase(yyruleno==0);
      /* (1) cmdlist ::= cmdlist ecmd */ yytestcase(yyruleno==1);
      /* (2) cmdlist ::= ecmd */ yytestcase(yyruleno==2);
      /* (3) ecmd ::= SEMI */ yytestcase(yyruleno==3);
      /* (4) ecmd ::= explain cmdx SEMI */ yytestcase(yyruleno==4);
      /* (10) trans_opt ::= */ yytestcase(yyruleno==10);
      /* (11) trans_opt ::= TRANSACTION */ yytestcase(yyruleno==11);
      /* (15) cmd ::= create_table */ yytestcase(yyruleno==15);
      /* (17) createkw ::= CREATE */ yytestcase(yyruleno==17);
      /* (47) cmd ::= create_virtual_table */ yytestcase(yyruleno==47);
      /* (73) cmd ::= create_view */ yytestcase(yyruleno==73);
        break;
			};
			yygoto = yyRuleInfo[yyruleno].lhs;
			yysize = yyRuleInfo[yyruleno].nrhs;
			yyidx -= yysize;
			yyact = yy_find_reduce_action(yymsp[-yysize].stateno,(SQLParser_YYCODETYPE)yygoto);
			if( yyact < SQLParser_YYNSTATE ){
#ifdef NDEBUG
				/* If we are not debugging and the reduce action popped at least
				 ** one element off the stack, then we can push the new element back
				 ** onto the stack here, and skip the stack overflow test in yy_shift().
				 ** That gives a significant speed improvement. */
				if( yysize ){
					yyidx++;
					yymsp -= yysize-1;
					yymsp->stateno = (SQLParser_YYACTIONTYPE)yyact;
					yymsp->major = (SQLParser_YYCODETYPE)yygoto;
					yymsp->minor = yygotominor;
				}else
#endif
				{
					yy_shift(yyact,yygoto,&yygotominor);
				}
			}else{
				assert( yyact == SQLParser_YYNSTATE + SQLParser_YYNRULE + 1 );
				yy_accept();
			}
		}

		/*
		 ** The following code executes when the parse fails
		 */
#ifndef SQLParser_YYNOERRORRECOVERY
		void yy_parse_failed(){
#ifndef NDEBUG
			if( yyTraceFILE ){
				*yyTraceFILE << yyTracePrompt << "Fail!" << std::endl;
			}
#endif
			while( yyidx>=0 ) yy_pop_parser_stack();
			/* Here code is inserted which will be executed whenever the
			 ** parser fails */
		}
#endif /* SQLParser_YYNOERRORRECOVERY */

		/*
		 ** The following code executes when a syntax error first occurs.
		 */
		void yy_syntax_error(
			int32_t yymajor,                   /* The major type of the error token */
			SQLParser_YYMINORTYPE yyminor            /* The minor type of the error token */
			){
			UNUSED_VARIABLE(yymajor);
#define TOKEN (yyminor.yy0)

	if (TOKEN.value_[0]) {
		util::String token(TOKEN.value_, TOKEN.size_, parser->getSQLAllocator());
		assert(parser->inputSql_ != NULL);
		size_t line = 0;
		size_t column = 0;
		SyntaxTree::countLineAndColumnFromToken(
			parser->inputSql_, TOKEN, line, column);
		util::NormalOStringStream ss;
		ss << "Syntax error at or near \"" << token.c_str()
			<< "\" (line=" << line << ", column=" << column << ")";
		parser->setError(ss.str().c_str());
	} else {
		parser->setError("Syntax error before finishing parse");
	}
	parser->parseState_ = SQLParserContext::PARSESTATE_END;
		}

		/*
		 ** The following is executed when the parser accepts
		 */
		void yy_accept(){
#ifndef NDEBUG
			if( yyTraceFILE ){
				*yyTraceFILE << yyTracePrompt << "Accept!" << std::endl;
			}
#endif
			while( yyidx>=0 ) yy_pop_parser_stack();
			/* Here code is inserted which will be executed whenever the
			 ** parser accepts */
		}

		/* The main parser program.
		 ** The first argument is a pointer to a structure obtained from
		 ** "SQLParserAlloc" which describes the current state of the parser.
		 ** The second argument is the major token number.  The third is
		 ** the minor token.  The fourth optional argument is whatever the
		 ** user wants (and specified in the grammar) and is available for
		 ** use by the action routines.
		 **
		 ** Inputs:
		 ** <ul>
		 ** <li> A pointer to the parser (an opaque structure.)
		 ** <li> The major token number.
		 ** <li> The minor token number.
		 ** <li> An option argument of a grammar-specified type.
		 ** </ul>
		 **
		 ** Outputs:
		 ** None.
		 */
	public:
		void Execute(
			int32_t yymajor,                 /* The major token code number */
			SQLParserTOKENTYPE &yyminor       /* The value for the token */
			SQLParserARG_PDECL               /* Optional %extra_argument parameter */
			){
			SQLParser_YYMINORTYPE yyminorunion;
			int32_t yyact;            /* The parser action. */
			int32_t yyendofinput;     /* True if we are at the end of input */
#ifdef SQLParser_YYERRORSYMBOL
			int32_t yyerrorhit = 0;   /* True if yymajor has invoked an error */
#endif

			/* (re)initialize the parser, if necessary */
			if( yyidx<0 ){
#if SQLParser_YYSTACKDEPTH<=0
				if( yystksz <=0 ){
					/*memset(&yyminorunion, 0, sizeof(yyminorunion));*/
					yyminorunion = SQLParser_yyzerominor;
					yyStackOverflow(&yyminorunion);
					return;
				}
#endif
				yyidx = 0;
				yyerrcnt = -1;
				yystack[0].stateno = 0;
				yystack[0].major = 0;
			}
			yyminorunion.yy0 = yyminor;
			yyendofinput = (yymajor==0);
			SQLParserARG_STORE;

#ifndef NDEBUG
			if( yyTraceFILE ){
				*yyTraceFILE << yyTracePrompt <<
				  "Input " << yyTokenName[yymajor] << std::endl;
			}
#endif

			do{
				yyact = yy_find_shift_action((SQLParser_YYCODETYPE)yymajor);
				if( yyact<SQLParser_YYNSTATE ){
					assert( !yyendofinput );  /* Impossible to shift the $ token */
					yy_shift(yyact,yymajor,&yyminorunion);
					yyerrcnt--;
					yymajor = SQLParser_YYNOCODE;
				}else if( yyact < SQLParser_YYNSTATE + SQLParser_YYNRULE ){
					yy_reduce(yyact-SQLParser_YYNSTATE);
				}else{
					assert( yyact == SQLParser_YY_ERROR_ACTION );
#ifdef SQLParser_YYERRORSYMBOL
					int32_t yymx;
#endif
#ifndef NDEBUG
					if( yyTraceFILE ){
						*yyTraceFILE << yyTracePrompt <<
						  "Syntax Error!" << std::endl;
					}
#endif
#ifdef SQLParser_YYERRORSYMBOL
					/* A syntax error has occurred.
					 ** The response to an error depends upon whether or not the
					 ** grammar defines an error token "ERROR".
					 **
					 ** This is what we do if the grammar does define ERROR:
					 **
					 **  * Call the %syntax_error function.
					 **
					 **  * Begin popping the stack until we enter a state where
					 **    it is legal to shift the error symbol, then shift
					 **    the error symbol.
					 **
					 **  * Set the error count to three.
					 **
					 **  * Begin accepting and shifting new tokens.  No new error
					 **    processing will occur until three tokens have been
					 **    shifted successfully.
					 **
					 */
					if( yyerrcnt<0 ){
						yy_syntax_error(yymajor,yyminorunion);
					}
					yymx = yystack[yyidx].major;
					if( yymx==SQLParser_YYERRORSYMBOL || yyerrorhit ){
#ifndef NDEBUG
						if( yyTraceFILE ){
							*yyTraceFILE << yyTracePrompt <<
							  "Discard input token " <<
								yyTokenName[yymajor] << std::endl;
						}
#endif
						yy_destructor((SQLParser_YYCODETYPE)yymajor,&yyminorunion);
						yymajor = SQLParser_YYNOCODE;
					}else{
						while(
							yyidx >= 0 &&
							yymx != SQLParser_YYERRORSYMBOL &&
							(yyact = yy_find_reduce_action(
								yystack[yyidx].stateno,
								SQLParser_YYERRORSYMBOL)) >= SQLParser_YYNSTATE
							){
							yy_pop_parser_stack();
						}
						if( yyidx < 0 || yymajor==0 ){
							yy_destructor((SQLParser_YYCODETYPE)yymajor,&yyminorunion);
							yy_parse_failed();
							yymajor = SQLParser_YYNOCODE;
						}else if( yymx!=SQLParser_YYERRORSYMBOL ){
							SQLParser_YYMINORTYPE u2;
							u2.SQLParser_YYERRSYMDT = 0;
							yy_shift(yyact,SQLParser_YYERRORSYMBOL,&u2);
						}
					}
					yyerrcnt = 3;
					yyerrorhit = 1;
#elif defined(SQLParser_YYNOERRORRECOVERY)
					/* If the SQLParser_YYNOERRORRECOVERY macro is defined, then do not attempt to
					 ** do any kind of error recovery.  Instead, simply invoke the syntax
					 ** error routine and continue going as if nothing had happened.
					 **
					 ** Applications can set this macro (for example inside %include) if
					 ** they intend to abandon the parse upon the first syntax error seen.
					 */
					yy_syntax_error(yymajor,yyminorunion);
					yy_destructor((SQLParser_YYCODETYPE)yymajor,&yyminorunion);
					yymajor = SQLParser_YYNOCODE;

#else  /* SQLParser_YYERRORSYMBOL is not defined */
					/* This is what we do if the grammar does not define ERROR:
					 **
					 **  * Report an error message, and throw away the input token.
					 **
					 **  * If the input token is $, then fail the parse.
					 **
					 ** As before, subsequent error messages are suppressed until
					 ** three input tokens have been successfully shifted.
					 */
					if( yyerrcnt<=0 ){
						yy_syntax_error(yymajor,yyminorunion);
					}
					yyerrcnt = 3;
					yy_destructor((SQLParser_YYCODETYPE)yymajor,&yyminorunion);
					if( yyendofinput ){
						yy_parse_failed();
					}
					yymajor = SQLParser_YYNOCODE;
#endif
				}
			}while( yymajor!=SQLParser_YYNOCODE && yyidx>=0 );
			return;
		}

	};

}
#endif

