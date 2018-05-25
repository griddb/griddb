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
#ifndef LEMPAR_CPP_tqlParser_GENERATED_HPP
#define LEMPAR_CPP_tqlParser_GENERATED_HPP

#ifdef _WIN32
#pragma warning(disable : 4065)
#endif
/* First off, code is included that follows the "include" declaration
** in the input grammar file. */

#include "tql_token.h"

#include "util/container.h"
#include "qp_def.h"

#include <stdexcept>
#include <vector>

#include "expression.h"
#include "query.h"

#ifdef _WIN32
#define atoll atol
#endif

/*
** Disable all error recovery processing in the parser push-down
** automaton.
*/
#define YYNOERRORRECOVERY 1

#define PRINT(...)

#define TQLPARSER_CONTEXT (pQuery->getTransactionContext())
#define TQLPARSER_ALLOCATOR (TQLPARSER_CONTEXT.getDefaultAllocator())
#define TQLPARSER_NEW QP_ALLOC_NEW(TQLPARSER_ALLOCATOR)
#define TQLPARSER_DELETE(x) QP_ALLOC_DELETE(TQLPARSER_ALLOCATOR, x)
#define TQLPARSER_SAFE_DELETE(x) QP_ALLOC_SAFE_DELETE(TQLPARSER_ALLOCATOR, x)
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
#define INTERFACE 1
#endif
/* The next thing included is series of defines which control
** various aspects of the generated parser.
**    tqlParser_YYCODETYPE         is the data type used for storing terminal
**                       and nonterminal numbers.  "unsigned char" is
**                       used if there are fewer than 250 terminals
**                       and nonterminals.  "int" is used otherwise.
**    tqlParser_YYNOCODE           is a number of type tqlParser_YYCODETYPE
*which corresponds
**                       to no legal terminal or nonterminal number.  This
**                       number is used to fill in empty slots of the hash
**                       table.
**    tqlParser_YYFALLBACK         If defined, this indicates that one or more
*tokens
**                       have fall-back values which should be used if the
**                       original value of the token will not parse.
**    tqlParser_YYACTIONTYPE       is the data type used for storing terminal
**                       and nonterminal numbers.  "unsigned char" is
**                       used if there are fewer than 250 rules and
**                       states combined.  "int" is used otherwise.
**    tqlParserTOKENTYPE     is the data type used for minor tokens given
**                       directly to the parser from the tokenizer.
**    tqlParser_YYMINORTYPE        is the data type used for all minor tokens.
**                       This is typically a union of many types, one of
**                       which is tqlParserTOKENTYPE.  The entry in the union
**                       for base tokens is called "yy0".
**    tqlParser_YYSTACKDEPTH       is the maximum depth of the parser's stack.
*If
**                       zero the stack is dynamically sized using realloc()
**    tqlParserARG_SDECL     A static variable declaration for the
*%extra_argument
**    tqlParserARG_PDECL     A parameter declaration for the %extra_argument
**    tqlParser_YYNSTATE           the combined number of states.
**    tqlParser_YYNRULE            the number of rules in the grammar
**    tqlParser_YYERRORSYMBOL      is the code number of the error symbol.  If
*not
**                       defined, then do no error processing.
*/
#define tqlParser_YYCODETYPE unsigned char
#define tqlParser_YYNOCODE 100
#define tqlParser_YYACTIONTYPE unsigned char
#define tqlParser_YYWILDCARD 16
#define tqlParserTOKENTYPE Token
typedef union {
	int yyinit;
	tqlParserTOKENTYPE yy0;
	Expr *yy44;
	ExprList *yy124;
	SortExprList *yy138;
	SortOrder yy197;
} tqlParser_YYMINORTYPE;
#ifndef tqlParser_YYSTACKDEPTH
#define tqlParser_YYSTACKDEPTH 2000
#endif
#define tqlParserARG_SDECL Query *pQuery
#define tqlParserARG_PDECL , Query *pQuery
#define tqlParserARG_STORE this->pQuery = pQuery
#define tqlParser_YYNSTATE 127
#define tqlParser_YYNRULE 85
#define tqlParser_YYFALLBACK 1
#define tqlParser_YY_NO_ACTION (tqlParser_YYNSTATE + tqlParser_YYNRULE + 2)
#define tqlParser_YY_ACCEPT_ACTION (tqlParser_YYNSTATE + tqlParser_YYNRULE + 1)
#define tqlParser_YY_ERROR_ACTION (tqlParser_YYNSTATE + tqlParser_YYNRULE)

/* The yyzerominor constant is used to initialize instances of
** tqlParser_YYMINORTYPE objects to zero. */
static const tqlParser_YYMINORTYPE tqlParser_yyzerominor = {0};

/* Define the yytestcase() macro to be a no-op if is not already defined
** otherwise.
**
** Applications can choose to define yytestcase() in the %include section
** to a macro that can assist in verifying code coverage.  For production
** code the yytestcase() macro should be turned off.  But it is useful
** for testing.
*/
#ifndef yytestcase
#define yytestcase(X)
#endif

/*!
 * @brief tqlParser
 *
 */
namespace lemon_tqlParser {

/*!
 * @brief tqlParser parser main
 *
 */
class tqlParser {
/* Next are the tables used to determine what action to take based on the
** current state and lookahead token.  These tables are used to implement
** functions that take a state number and lookahead value and return an
** action integer.
**
** Suppose the action integer is N.  Then the action is determined as
** follows
**
**   0 <= N < tqlParser_YYNSTATE                  Shift N.  That is, push the
*lookahead
**                                      token onto the stack and goto state N.
**
**   tqlParser_YYNSTATE <= N < tqlParser_YYNSTATE+tqlParser_YYNRULE   Reduce by
*rule N-tqlParser_YYNSTATE.
**
**   N == tqlParser_YYNSTATE+tqlParser_YYNRULE              A syntax error has
*occurred.
**
**   N == tqlParser_YYNSTATE+tqlParser_YYNRULE+1            The parser accepts
*its input.
**
**   N == tqlParser_YYNSTATE+tqlParser_YYNRULE+2            No such action.
*Denotes unused
**                                      slots in the yy_action[] table.
**
** The action table is constructed as a single large table named yy_action[].
** Given state S and lookahead X, the action is computed as
**
**      yy_action[ yy_shift_ofst[S] + X ]
**
** If the index value yy_shift_ofst[S]+X is out of range or if the value
** yy_lookahead[yy_shift_ofst[S]+X] is not equal to X or if yy_shift_ofst[S]
** is equal to tqlParser_YY_SHIFT_USE_DFLT, it means that the action is not in
*the table
** and that yy_default[S] should be used instead.
**
** The formula above is for computing the action when the lookahead is
** a terminal symbol.  If the lookahead is a non-terminal (as occurs after
** a reduce action) then the yy_reduce_ofst[] array is used in place of
** the yy_shift_ofst[] array and tqlParser_YY_REDUCE_USE_DFLT is used in place
*of
** tqlParser_YY_SHIFT_USE_DFLT.
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

#define tqlParser_YY_ACTTAB_COUNT (385)
#define tqlParser_YY_SHIFT_USE_DFLT (-32)
#define tqlParser_YY_SHIFT_COUNT (74)
#define tqlParser_YY_SHIFT_MIN (-31)
#define tqlParser_YY_SHIFT_MAX (234)
#define tqlParser_YY_REDUCE_USE_DFLT (-89)
#define tqlParser_YY_REDUCE_COUNT (55)
#define tqlParser_YY_REDUCE_MIN (-88)
#define tqlParser_YY_REDUCE_MAX (297)
#define tqlParser_YYNFALLBACK (16)

	static const tqlParser_YYACTIONTYPE yy_action[tqlParser_YY_ACTTAB_COUNT];
	static const tqlParser_YYCODETYPE yy_lookahead[tqlParser_YY_ACTTAB_COUNT];
	static const short yy_shift_ofst[tqlParser_YY_SHIFT_COUNT + 1];
	static const short yy_reduce_ofst[tqlParser_YY_REDUCE_COUNT + 1];
	static const tqlParser_YYACTIONTYPE yy_default[];

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
#ifdef tqlParser_YYFALLBACK
	static const tqlParser_YYCODETYPE yyFallback[tqlParser_YYNFALLBACK];
#endif /* tqlParser_YYFALLBACK */

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
		tqlParser_YYACTIONTYPE stateno; /* The state-number */
		tqlParser_YYCODETYPE major; /* The major token value.  This is the code
			  ** number for the token at this stack level */
		tqlParser_YYMINORTYPE
			minor; /* The user-supplied minor token value.  This
** is the value of the token  */
	};
	typedef struct yyStackEntry yyStackEntry;

	/* The state of the parser is completely contained in an instance of
	** the following structure */

	int yyidx; /* Index of top element in stack */
#ifdef tqlParser_YYTRACKMAXSTACKDEPTH
	int yyidxMax; /* Maximum value of yyidx */
#endif
	int yyerrcnt;		/* Shifts left before out of the error */
	tqlParserARG_SDECL; /* A place to hold %extra_argument */
#if tqlParser_YYSTACKDEPTH <= 0
	int yystksz;		   /* Current side of the stack */
	yyStackEntry *yystack; /* The parser's stack */
#else
	yyStackEntry yystack[tqlParser_YYSTACKDEPTH]; /* The parser's stack */
#endif

#ifndef NDEBUG
	std::ostream *yyTraceFILE;
	const char *yyTracePrompt;
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
public:
	void tqlParserSetTrace(std::ostream *TraceFILE, const char *zTracePrompt) {
		yyTraceFILE = TraceFILE;
		yyTracePrompt = zTracePrompt;
		if (yyTraceFILE == 0) yyTraceFILE = 0;
	}
#endif /* NDEBUG */

#ifndef NDEBUG
protected:
	static const char *const yyTokenName[];
	static const char *const yyRuleName[tqlParser_YYNRULE];
#endif

#if tqlParser_YYSTACKDEPTH <= 0
	/*
	** Try to increase the size of the parser stack.
	*/
	void yyGrowStack() {
		int newSize;
		yyStackEntry *pNew;

		newSize = yystksz * 2 + 100;
		pNew = new yyStackEntry[newSize];
		if (pNew) {
			memcpy(pNew, yystack, newSize * sizeof(pNew[0]));
			delete yystack;
			yystack = pNew;
			yystksz = newSize;
#ifndef NDEBUG
			if (yyTraceFILE) {
				*yyTraceFILE << yyTracePrompt << "Stack grows to " << yystksz
							 << " entries!" << std::endl;
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
	** to tqlParser and tqlParserFree.
	*/
public:
	tqlParser() {
		yyidx = -1;
#ifdef tqlParser_YYTRACKMAXSTACKDEPTH
		yidxMax = 0;
#endif
#if tqlParser_YYSTACKDEPTH <= 0
		yystack = NULL;
		yystksz = 0;
		yyGrowStack();
#else
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
		tqlParser_YYCODETYPE yymajor,   /* Type code for object to destroy */
		tqlParser_YYMINORTYPE *yypminor /* The object to be destroyed */
		) {
		switch (yymajor) {
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
		case 69: /* expr */
		case 70: /* term */
		case 71: /* function */
		case 75: /* sortitem */
		case 98: /* escape */
		{
			TQLPARSER_SAFE_DELETE((yypminor->yy44));
		} break;
		default:
			break; /* If no destructor action specified: do nothing */
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
	int yy_pop_parser_stack() {
		tqlParser_YYCODETYPE yymajor;
		yyStackEntry *yytos;

		if (yyidx < 0) return 0;
		yytos = &yystack[yyidx];
#ifndef NDEBUG
		if (yyTraceFILE && yyidx >= 0) {
			*yyTraceFILE << yyTracePrompt << "Popping "
						 << yyTokenName[yytos->major] << std::endl;
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
	**       obtained from tqlParserAlloc.
	** <li>  A pointer to a function used to reclaim memory obtained
	**       from malloc.
	** </ul>
	*/
public:
	~tqlParser() {
		while (yyidx >= 0) yy_pop_parser_stack();
#if tqlParser_YYSTACKDEPTH <= 0
		delete yystack;
		yystack = NULL;
#endif
	}

/*
** Return the peak depth of the stack for a parser.
*/
#ifdef tqlParser_YYTRACKMAXSTACKDEPTH
public:
	int tqlParserStackPeak(void *p) {
		return yyidxMax;
	}
#endif

	/*
	** Find the appropriate action for a parser given the terminal
	** look-ahead token iLookAhead.
	**
	** If the look-ahead token is tqlParser_YYNOCODE, then check to see if the
	*action is
	** independent of the look-ahead.  If it is, return the action, otherwise
	** return tqlParser_YY_NO_ACTION.
	*/
	int yy_find_shift_action(
		tqlParser_YYCODETYPE iLookAhead /* The look-ahead token */
		) {
		int i;
		int stateno = this->yystack[this->yyidx].stateno;

		if (stateno > tqlParser_YY_SHIFT_COUNT ||
			(i = yy_shift_ofst[stateno]) == tqlParser_YY_SHIFT_USE_DFLT) {
			return yy_default[stateno];
		}
		assert(iLookAhead != tqlParser_YYNOCODE);
		i += iLookAhead;
		if (i < 0 || i >= tqlParser_YY_ACTTAB_COUNT ||
			yy_lookahead[i] != iLookAhead) {
			if (iLookAhead > 0) {
#ifdef tqlParser_YYFALLBACK
				tqlParser_YYCODETYPE iFallback; /* Fallback token */
				if (iLookAhead < tqlParser_YYNFALLBACK &&
					(iFallback = yyFallback[iLookAhead]) != 0) {
#ifndef NDEBUG
					if (yyTraceFILE) {
						*yyTraceFILE << yyTracePrompt << "FALLBACK "
									 << yyTokenName[iLookAhead] << " => "
									 << yyTokenName[iFallback] << std::endl;
					}
#endif
					return yy_find_shift_action(iFallback);
				}
#endif
#ifdef tqlParser_YYWILDCARD
				{
					int j = i - iLookAhead + tqlParser_YYWILDCARD;
					if (
#if tqlParser_YY_SHIFT_MIN + tqlParser_YYWILDCARD < 0
						j >= 0 &&
#endif
#if tqlParser_YY_SHIFT_MAX + tqlParser_YYWILDCARD >= tqlParser_YY_ACTTAB_COUNT
						j < tqlParser_YY_ACTTAB_COUNT &&
#endif
						yy_lookahead[j] == tqlParser_YYWILDCARD) {
#ifndef NDEBUG
						if (yyTraceFILE) {
							*yyTraceFILE << yyTracePrompt << "WILDCARD "
										 << yyTokenName[iLookAhead] << " => "
										 << yyTokenName[tqlParser_YYWILDCARD]
										 << std::endl;
						}
#endif /* NDEBUG */
						return yy_action[j];
					}
				}
#endif /* tqlParser_YYWILDCARD */
			}
			return yy_default[stateno];
		}
		else {
			return yy_action[i];
		}
	}

	/*
	** Find the appropriate action for a parser given the non-terminal
	** look-ahead token iLookAhead.
	**
	** If the look-ahead token is tqlParser_YYNOCODE, then check to see if the
	*action is
	** independent of the look-ahead.  If it is, return the action, otherwise
	** return tqlParser_YY_NO_ACTION.
	*/
	int yy_find_reduce_action(int stateno, /* Current state number */
		tqlParser_YYCODETYPE iLookAhead	/* The look-ahead token */
		) {
		int i;
#ifdef tqlParser_YYERRORSYMBOL
		if (stateno > tqlParser_YY_REDUCE_COUNT) {
			return yy_default[stateno];
		}
#else
		assert(stateno <= tqlParser_YY_REDUCE_COUNT);
#endif
		i = yy_reduce_ofst[stateno];
		assert(i != tqlParser_YY_REDUCE_USE_DFLT);
		assert(iLookAhead != tqlParser_YYNOCODE);
		i += iLookAhead;
#ifdef tqlParser_YYERRORSYMBOL
		if (i < 0 || i >= tqlParser_YY_ACTTAB_COUNT ||
			yy_lookahead[i] != iLookAhead) {
			return yy_default[stateno];
		}
#else
		assert(i >= 0 && i < tqlParser_YY_ACTTAB_COUNT);
		assert(yy_lookahead[i] == iLookAhead);
#endif
		return yy_action[i];
	}

	/*
	** The following routine is called if the stack overflows.
	*/
	void yyStackOverflow(tqlParser_YYMINORTYPE *yypMinor) {
		yyidx--;
#ifndef NDEBUG
		if (yyTraceFILE) {
			*yyTraceFILE << yyTracePrompt << "Stack Overflow!" << std::endl;
		}
#endif
		while (yyidx >= 0) yy_pop_parser_stack();
		/* Here code is inserted which will execute if the parser
		** stack every overflows */

		pQuery->setError("parser stack overflow");
	}

	/*
	** Perform a shift action.
	*/
	void yy_shift(int yyNewState, /* The new state to shift in */
		int yyMajor,			  /* The major token to shift in */
		tqlParser_YYMINORTYPE
			*yypMinor /* Pointer to the minor token to shift in */
		) {
		yyStackEntry *yytos;
		yyidx++;
#ifdef tqlParser_YYTRACKMAXSTACKDEPTH
		if (yyidx > yyidxMax) {
			yyidxMax = yyidx;
		}
#endif
#if tqlParser_YYSTACKDEPTH > 0
		if (yyidx >= tqlParser_YYSTACKDEPTH) {
			yyStackOverflow(yypMinor);
			return;
		}
#else
		if (yyidx >= yystksz) {
			yyGrowStack();
			if (yyidx >= yystksz) {
				yyStackOverflow(yypMinor);
				return;
			}
		}
#endif
		yytos = &yystack[yyidx];
		yytos->stateno = (tqlParser_YYACTIONTYPE)yyNewState;
		yytos->major = (tqlParser_YYCODETYPE)yyMajor;
		yytos->minor = *yypMinor;
#ifndef NDEBUG
		if (yyTraceFILE && yyidx > 0) {
			int i;
			*yyTraceFILE << yyTracePrompt << "Shift " << yyNewState
						 << std::endl;
			*yyTraceFILE << yyTracePrompt << "Stack:";
			for (i = 1; i <= yyidx; i++) {
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
	static const struct RULEINFO {
		tqlParser_YYCODETYPE lhs; /* Symbol on the left-hand side of the rule */
		unsigned char nrhs; /* Number of right-hand side symbols in the rule */
	} yyRuleInfo[];

	/*
	** Perform a reduce action and the shift that must immediately
	** follow the reduce.
	*/
private:
	void yy_reduce(int yyruleno /* Number of the rule by which to reduce */
		) {
		int yygoto;						   /* The next state */
		int yyact;						   /* The next action */
		tqlParser_YYMINORTYPE yygotominor; /* The LHS of the rule reduced */
		yyStackEntry *yymsp;			   /* The top of the parser's stack */
		int yysize;						   /* Amount to pop the stack */
		yymsp = &yystack[yyidx];
#ifndef NDEBUG
		if (yyTraceFILE && yyruleno >= 0 && yyruleno < tqlParser_YYNRULE) {
			*yyTraceFILE << yyTracePrompt << "Reduce [" << yyRuleName[yyruleno]
						 << "]." << std::endl;
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
		yygotominor = tqlParser_yyzerominor;

		switch (yyruleno) {
		/* Beginning here are the reduction cases.  A typical example
		** follows:
		**   case 0:
		**  #line <lineno> <grammarfile>
		**     { ... }           
		**  #line <lineno> <thisfile>
		**     break;
		*/
		case 0: /* ecmd ::= explain cmd */
		case 1: /* ecmd ::= explain cmd SEMICOLON */
			yytestcase(yyruleno == 1);
			{
				PRINT("ECMD");
				pQuery->parseState_ = Query::PARSESTATE_END;
			}
			break;
		case 2: /* cmd ::= select */
		{
			PRINT("CMD");
		} break;
		case 4: /* explain ::= EXPLAIN ANALYZE */
		{
			pQuery->enableExplain(true);
		} break;
		case 5: /* explain ::= EXPLAIN */
		{
			pQuery->enableExplain(false);
		} break;
		case 8: /* selcollist ::= function */
		{
			yygotominor.yy124 = TQLPARSER_NEW ExprList(TQLPARSER_ALLOCATOR);
			pQuery->setSelectionExpr(yygotominor.yy124);
			yygotominor.yy124->push_back(yymsp[0].minor.yy44);
		} break;
		case 9: /* selcollist ::= STAR */
		{
			yygotominor.yy124 = TQLPARSER_NEW ExprList(TQLPARSER_ALLOCATOR);
			pQuery->setSelectionExpr(yygotominor.yy124);
			Expr *x = Expr::newColumnNode(yymsp[0].minor.yy0, TQLPARSER_CONTEXT,
				pQuery->parseState_, pQuery->getCollection(),
				pQuery->getTimeSeries());
			yygotominor.yy124->push_back(x);
		} break;
		case 10: /* id ::= ID */
		case 11: /* nm ::= ID */
			yytestcase(yyruleno == 11);
		case 12: /* nm ::= STRING */
			yytestcase(yyruleno == 12);
		case 13: /* colnm ::= COLID */
			yytestcase(yyruleno == 13);
		case 14: /* colnm ::= ID */
			yytestcase(yyruleno == 14);
		case 15: /* colnm ::= STRING */
			yytestcase(yyruleno == 15);
		case 21: /* dbnm ::= DOT nm */
			yytestcase(yyruleno == 21);
			{ yygotominor.yy0 = yymsp[0].minor.yy0; }
			break;
		case 16: /* from ::= */
		{
			pQuery->parseState_ = Query::PARSESTATE_CONDITION;
		} break;
		case 17: /* from ::= FROM seltablist */
		{
			pQuery->parseState_ = Query::PARSESTATE_CONDITION;
		} break;
		case 18: /* seltablist ::= colnm dbnm */
		{
			pQuery->setFromCollection(
				yymsp[-1].minor.yy0, yymsp[0].minor.yy0, TQLPARSER_CONTEXT);
		} break;
		case 23: /* orderby_opt ::= ORDER BY sortlist */
		{
			pQuery->setSortList(yymsp[0].minor.yy138);
		} break;
		case 24: /* sortlist ::= sortlist COMMA expr sortorder */
		{
			SortItem s;
			s.order = yymsp[0].minor.yy197;
			s.expr = yymsp[-1].minor.yy44;
			yymsp[-3].minor.yy138->push_back(s);
			yygotominor.yy138 = yymsp[-3].minor.yy138;
		} break;
		case 25: /* sortlist ::= expr sortorder */
		{
			SortItem s;
			yygotominor.yy138 = TQLPARSER_NEW SortExprList(TQLPARSER_ALLOCATOR);
			s.order = yymsp[0].minor.yy197;
			s.expr = yymsp[-1].minor.yy44;
			yygotominor.yy138->push_back(s);
		} break;
		case 26: /* sortorder ::= ASC */
		case 28: /* sortorder ::= */
			yytestcase(yyruleno == 28);
			{ yygotominor.yy197 = ASC; }
			break;
		case 27: /* sortorder ::= DESC */
		{
			yygotominor.yy197 = DESC;
		} break;
		case 29: /* limit_opt ::= */
		{
			pQuery->setLimitExpr(NULL, NULL);
		} break;
		case 30: /* limit_opt ::= LIMIT INTEGER */
		{
			pQuery->setLimitExpr(
				Expr::newNumericValue(
					int64_t(atoll(yymsp[0].minor.yy0.z)), TQLPARSER_CONTEXT),
				NULL);
		} break;
		case 31: /* limit_opt ::= LIMIT INTEGER OFFSET INTEGER */
		{
			pQuery->setLimitExpr(
				Expr::newNumericValue(
					int64_t(atoll(yymsp[-2].minor.yy0.z)), TQLPARSER_CONTEXT),
				Expr::newNumericValue(
					int64_t(atoll(yymsp[0].minor.yy0.z)), TQLPARSER_CONTEXT));
		} break;
		case 33: /* where_opt ::= WHERE expr */
		{
			pQuery->setConditionExpr(yymsp[0].minor.yy44, TQLPARSER_CONTEXT);
		} break;
		case 34: /* expr ::= term */
		{
			PRINT("EXPR1");
			yygotominor.yy44 = yymsp[0].minor.yy44;
		} break;
		case 35: /* expr ::= LP expr RP */
		{
			yygotominor.yy44 = yymsp[-1].minor.yy44;
		} break;
		case 36: /* term ::= NULL */
		{
			PRINT("TERM1");
			yygotominor.yy44 = NULL;
		} break;
		case 37: /* term ::= id */
		{
			yygotominor.yy44 = Expr::newColumnOrIdNode(yymsp[0].minor.yy0,
				TQLPARSER_CONTEXT, pQuery->parseState_, pQuery->getCollection(),
				pQuery->getTimeSeries(), pQuery->getSpecialIdMap());
		} break;
		case 38: /* term ::= INTEGER */
		{
			PRINT("TERM2");
			{
				int64_t tmp = atoll(yymsp[0].minor.yy0.z);
				if (tmp == INT64_MAX &&
					(yymsp[0].minor.yy0.z[yymsp[0].minor.yy0.n - 1] !=
						(tmp % 10) + '0')) {
					tmp = INT64_MIN;
				}
				yygotominor.yy44 =
					Expr::newNumericValue(tmp, TQLPARSER_CONTEXT);
			}
		} break;
		case 39: /* term ::= FLOAT */
		{
			PRINT("TERM2");
			yygotominor.yy44 = Expr::newNumericValue(
				atof(yymsp[0].minor.yy0.z), TQLPARSER_CONTEXT);
		} break;
		case 40: /* term ::= BLOB */
		{
			PRINT("TERM2");
			yygotominor.yy44 =
				Expr::newBlobValue(yymsp[0].minor.yy0, TQLPARSER_CONTEXT);
		} break;
		case 41: /* term ::= STRING */
		{
			char buf[256];
			char *p;
			PRINT("TERM3");
			if (yymsp[0].minor.yy0.n == 2) {
				yygotominor.yy44 = Expr::newStringValue("", TQLPARSER_CONTEXT);
			}
			else {
				if (yymsp[0].minor.yy0.n < 255) {
					p = buf;
				}
				else {
					p = reinterpret_cast<char *>(
						TQLPARSER_ALLOCATOR.allocate(yymsp[0].minor.yy0.n));
				}
				memcpy(p, yymsp[0].minor.yy0.z + 1, yymsp[0].minor.yy0.n - 2);
				p[yymsp[0].minor.yy0.n - 2] = '\0';
				yygotominor.yy44 =
					Expr::newStringValueWithUnescape(p, TQLPARSER_CONTEXT);
			}
		} break;
		case 42: /* term ::= NAN */
		{
			PRINT("TERM3");
			yygotominor.yy44 = Expr::newNumericValue(
				std::numeric_limits<double>::quiet_NaN(), TQLPARSER_CONTEXT);
		} break;
		case 43: /* term ::= INF */
		{
			PRINT("TERM3");
			yygotominor.yy44 = Expr::newNumericValue(
				std::numeric_limits<double>::infinity(), TQLPARSER_CONTEXT);
		} break;
		case 44: /* function ::= ID LP exprlist RP */
		{
			if (pQuery->parseState_ == Query::PARSESTATE_SELECTION) {
				yygotominor.yy44 = Expr::newSelectionNode(yymsp[-3].minor.yy0,
					yymsp[-1].minor.yy124, TQLPARSER_CONTEXT,
					pQuery->getFunctionMap(), pQuery->getAggregationMap(),
					pQuery->getSelectionMap());
			}
			else {
				yygotominor.yy44 = Expr::newFunctionNode(yymsp[-3].minor.yy0,
					yymsp[-1].minor.yy124, TQLPARSER_CONTEXT,
					(*pQuery->getFunctionMap()));
			}
			if (yymsp[-1].minor.yy124 != NULL) {
				TQLPARSER_DELETE(yymsp[-1].minor.yy124);
			}
		} break;
		case 45: /* exprlist ::= nexprlist */
		{
			yygotominor.yy124 = yymsp[0].minor.yy124;
		} break;
		case 46: /* exprlist ::= */
		{
			yygotominor.yy124 = NULL;
		} break;
		case 47: /* nexprlist ::= nexprlist COMMA expr */
		{
			yygotominor.yy124 = yymsp[-2].minor.yy124;
			yygotominor.yy124->push_back(yymsp[0].minor.yy44);
		} break;
		case 48: /* nexprlist ::= expr */
		{
			yygotominor.yy124 = TQLPARSER_NEW ExprList(TQLPARSER_ALLOCATOR);
			yygotominor.yy124->push_back(yymsp[0].minor.yy44);
		} break;
		case 49: /* expr ::= function */
		{
			yygotominor.yy44 = yymsp[0].minor.yy44;
		} break;
		case 50: /* function ::= ID LP STAR RP */
		{
			Expr *s = Expr::newColumnNode(yymsp[-1].minor.yy0,
				TQLPARSER_CONTEXT, pQuery->parseState_, pQuery->getCollection(),
				pQuery->getTimeSeries());
			ExprList *v = TQLPARSER_NEW ExprList(TQLPARSER_ALLOCATOR);
			v->push_back(s);
			yygotominor.yy44 = Expr::newSelectionNode(yymsp[-3].minor.yy0, v,
				TQLPARSER_CONTEXT, pQuery->getFunctionMap(),
				pQuery->getAggregationMap(), pQuery->getSelectionMap());
			TQLPARSER_DELETE(v);
		} break;
		case 51: /* function ::= ID LP STAR COMMA nexprlist RP */
		{
			Expr *s = Expr::newColumnNode(yymsp[-3].minor.yy0,
				TQLPARSER_CONTEXT, pQuery->parseState_, pQuery->getCollection(),
				pQuery->getTimeSeries());
			ExprList *v = TQLPARSER_NEW ExprList(TQLPARSER_ALLOCATOR);
			v->push_back(s);
			v->insert(v->end(), yymsp[-1].minor.yy124->begin(),
				yymsp[-1].minor.yy124->end());
			yygotominor.yy44 = Expr::newSelectionNode(yymsp[-5].minor.yy0, v,
				TQLPARSER_CONTEXT, pQuery->getFunctionMap(),
				pQuery->getAggregationMap(), pQuery->getSelectionMap());
			TQLPARSER_DELETE(v);
			if (yymsp[-1].minor.yy124 != NULL) {
				TQLPARSER_DELETE(yymsp[-1].minor.yy124);
			}
		} break;
		case 52: /* expr ::= NOT expr */
		{
			BoolExpr *b;
			b = dynamic_cast<BoolExpr *>(yymsp[0].minor.yy44);
			if (b == NULL) {
				b = TQLPARSER_NEW BoolExpr(
					yymsp[0].minor.yy44, TQLPARSER_CONTEXT);
			}
			yygotominor.yy44 = TQLPARSER_NEW BoolExpr(
				BoolExpr::NOT, b, NULL, TQLPARSER_CONTEXT);
		} break;
		case 53: /* expr ::= expr AND expr */
		{
			BoolExpr *b1, *b2;
			b1 = dynamic_cast<BoolExpr *>(yymsp[-2].minor.yy44);
			b2 = dynamic_cast<BoolExpr *>(yymsp[0].minor.yy44);
			if (b1 == NULL) {
				b1 = TQLPARSER_NEW BoolExpr(
					yymsp[-2].minor.yy44, TQLPARSER_CONTEXT);
			}
			if (b2 == NULL) {
				b2 = TQLPARSER_NEW BoolExpr(
					yymsp[0].minor.yy44, TQLPARSER_CONTEXT);
			}
			yygotominor.yy44 = TQLPARSER_NEW BoolExpr(
				BoolExpr::AND, b1, b2, TQLPARSER_CONTEXT);
		} break;
		case 54: /* expr ::= expr XOR expr */
		{
			BoolExpr *na, *nb, *x1, *x2;
			BoolExpr *b1, *b2;
			b1 = dynamic_cast<BoolExpr *>(yymsp[-2].minor.yy44);
			b2 = dynamic_cast<BoolExpr *>(yymsp[0].minor.yy44);
			if (b1 == NULL) {
				b1 = TQLPARSER_NEW BoolExpr(
					yymsp[-2].minor.yy44, TQLPARSER_CONTEXT);
			}
			if (b2 == NULL) {
				b2 = TQLPARSER_NEW BoolExpr(
					yymsp[0].minor.yy44, TQLPARSER_CONTEXT);
			}
			na = TQLPARSER_NEW BoolExpr(
				BoolExpr::NOT, b1, NULL, TQLPARSER_CONTEXT);
			nb = TQLPARSER_NEW BoolExpr(
				BoolExpr::NOT, b2, NULL, TQLPARSER_CONTEXT);
			x1 = TQLPARSER_NEW BoolExpr(
				BoolExpr::AND, b1, nb, TQLPARSER_CONTEXT);
			x2 = TQLPARSER_NEW BoolExpr(
				BoolExpr::AND, na, b2, TQLPARSER_CONTEXT);
			yygotominor.yy44 =
				TQLPARSER_NEW BoolExpr(BoolExpr::OR, x1, x2, TQLPARSER_CONTEXT);
		} break;
		case 55: /* expr ::= expr OR expr */
		{
			BoolExpr *b1, *b2;
			b1 = dynamic_cast<BoolExpr *>(yymsp[-2].minor.yy44);
			b2 = dynamic_cast<BoolExpr *>(yymsp[0].minor.yy44);
			if (b1 == NULL) {
				b1 = TQLPARSER_NEW BoolExpr(
					yymsp[-2].minor.yy44, TQLPARSER_CONTEXT);
			}
			if (b2 == NULL) {
				b2 = TQLPARSER_NEW BoolExpr(
					yymsp[0].minor.yy44, TQLPARSER_CONTEXT);
			}
			yygotominor.yy44 =
				TQLPARSER_NEW BoolExpr(BoolExpr::OR, b1, b2, TQLPARSER_CONTEXT);
		} break;
		case 56: /* expr ::= expr LT|GT|GE|LE expr */
		{
			PRINT("EXPR7");
			switch (yymsp[-1].minor.yy0.id) {
			case TK_LT:
				yygotominor.yy44 =
					Expr::newOperationNode(Expr::LT, yymsp[-2].minor.yy44,
						yymsp[0].minor.yy44, TQLPARSER_CONTEXT);
				break;
			case TK_GT:
				yygotominor.yy44 =
					Expr::newOperationNode(Expr::GT, yymsp[-2].minor.yy44,
						yymsp[0].minor.yy44, TQLPARSER_CONTEXT);
				break;
			case TK_GE:
				yygotominor.yy44 =
					Expr::newOperationNode(Expr::GE, yymsp[-2].minor.yy44,
						yymsp[0].minor.yy44, TQLPARSER_CONTEXT);
				break;
			case TK_LE:
				yygotominor.yy44 =
					Expr::newOperationNode(Expr::LE, yymsp[-2].minor.yy44,
						yymsp[0].minor.yy44, TQLPARSER_CONTEXT);
				break;
			}
		} break;
		case 57: /* expr ::= expr EQ|NE expr */
		{
			yygotominor.yy44 = Expr::newOperationNode(
				(yymsp[-1].minor.yy0.id == TK_NE) ? (Expr::NE) : (Expr::EQ),
				yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TQLPARSER_CONTEXT);
		} break;
		case 58: /* expr ::= expr BITAND|BITOR|LSHIFT|RSHIFT expr */
		{
			PRINT("EXPR11");
			switch (yymsp[-1].minor.yy0.id) {
			case TK_BITAND:
				yygotominor.yy44 =
					Expr::newOperationNode(Expr::BITAND, yymsp[-2].minor.yy44,
						yymsp[0].minor.yy44, TQLPARSER_CONTEXT);
				break;
			case TK_BITOR:
				yygotominor.yy44 =
					Expr::newOperationNode(Expr::BITOR, yymsp[-2].minor.yy44,
						yymsp[0].minor.yy44, TQLPARSER_CONTEXT);
				break;
			case TK_LSHIFT:
				yygotominor.yy44 =
					Expr::newOperationNode(Expr::LSHIFT, yymsp[-2].minor.yy44,
						yymsp[0].minor.yy44, TQLPARSER_CONTEXT);
				break;
			case TK_RSHIFT:
				yygotominor.yy44 =
					Expr::newOperationNode(Expr::RSHIFT, yymsp[-2].minor.yy44,
						yymsp[0].minor.yy44, TQLPARSER_CONTEXT);
				break;
			}
		} break;
		case 59: /* expr ::= expr PLUS|MINUS expr */
		{
			yygotominor.yy44 = Expr::newOperationNode(
				(yymsp[-1].minor.yy0.id == TK_PLUS) ? (Expr::ADD) : (Expr::SUB),
				yymsp[-2].minor.yy44, yymsp[0].minor.yy44, TQLPARSER_CONTEXT);
		} break;
		case 60: /* expr ::= expr STAR|SLASH|REM expr */
		{
			switch (yymsp[-1].minor.yy0.id) {
			case TK_STAR:
				yygotominor.yy44 =
					Expr::newOperationNode(Expr::MUL, yymsp[-2].minor.yy44,
						yymsp[0].minor.yy44, TQLPARSER_CONTEXT);
				break;
			case TK_SLASH:
				yygotominor.yy44 =
					Expr::newOperationNode(Expr::DIV, yymsp[-2].minor.yy44,
						yymsp[0].minor.yy44, TQLPARSER_CONTEXT);
				break;
			case TK_REM:
				yygotominor.yy44 =
					Expr::newOperationNode(Expr::REM, yymsp[-2].minor.yy44,
						yymsp[0].minor.yy44, TQLPARSER_CONTEXT);
				break;
			}
		} break;
		case 61: /* cmd ::= PRAGMA nm dbnm */
		{
			pQuery->setPragma(&yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, 0, 0);
		} break;
		case 62: /* cmd ::= PRAGMA nm dbnm EQ nmnum */
		{
			pQuery->setPragma(&yymsp[-3].minor.yy0, &yymsp[-2].minor.yy0,
				&yymsp[0].minor.yy0, 0);
		} break;
		case 63: /* cmd ::= PRAGMA nm dbnm LP nmnum RP */
		{
			pQuery->setPragma(&yymsp[-4].minor.yy0, &yymsp[-3].minor.yy0,
				&yymsp[-1].minor.yy0, 0);
		} break;
		case 64: /* cmd ::= PRAGMA nm dbnm EQ minus_num */
		{
			pQuery->setPragma(&yymsp[-3].minor.yy0, &yymsp[-2].minor.yy0,
				&yymsp[0].minor.yy0, 1);
		} break;
		case 65: /* cmd ::= PRAGMA nm dbnm LP minus_num RP */
		{
			pQuery->setPragma(&yymsp[-4].minor.yy0, &yymsp[-3].minor.yy0,
				&yymsp[-1].minor.yy0, 1);
		} break;
		case 66: /* nmnum ::= plus_num */
		case 67: /* nmnum ::= nm */
			yytestcase(yyruleno == 67);
		case 68: /* nmnum ::= ON */
			yytestcase(yyruleno == 68);
		case 69: /* nmnum ::= DELETE */
			yytestcase(yyruleno == 69);
		case 70: /* nmnum ::= DEFAULT */
			yytestcase(yyruleno == 70);
			{ yygotominor.yy0 = yymsp[0].minor.yy0; }
			break;
		case 71: /* plus_num ::= plus_opt number */
		{
			yygotominor.yy0 = yymsp[0].minor.yy0;
		} break;
		case 72: /* minus_num ::= MINUS number */
		{
			yygotominor.yy0 = yymsp[0].minor.yy0;
			yygotominor.yy0.minusflag = 1;
		} break;
		case 73: /* number ::= INTEGER|FLOAT */
		{
			PRINT("NUMBER");
			yygotominor.yy0 = yymsp[0].minor.yy0;
		} break;
		case 74: /* number ::= NAN */
		case 75: /* number ::= INF */
			yytestcase(yyruleno == 75);
			{ yygotominor.yy0 = yymsp[0].minor.yy0; }
			break;
		case 76: /* plus_opt ::= PLUS */
		{
			PRINT("PLUS_OPT1");
		} break;
		case 77: /* plus_opt ::= */
		{
			PRINT("PLUS_OPT2");
		} break;
		case 78: /* expr ::= MINUS expr */
		{
			yygotominor.yy44 = Expr::newOperationNode(
				Expr::BITMINUS, yymsp[0].minor.yy44, NULL, TQLPARSER_CONTEXT);
		} break;
		case 79: /* expr ::= PLUS expr */
		case 82: /* escape ::= ESCAPE expr */
			yytestcase(yyruleno == 82);
			{ yygotominor.yy44 = yymsp[0].minor.yy44; }
			break;
		case 80: /* likeop ::= LIKE_KW */
		{
			yygotominor.yy0 = yymsp[0].minor.yy0;
			yygotominor.yy0.minusflag = 0;
		} break;
		case 81: /* likeop ::= NOT LIKE_KW */
		{
			yygotominor.yy0 = yymsp[0].minor.yy0;
			yygotominor.yy0.minusflag = 1;
		} break;
		case 83: /* escape ::= */
		{
			yygotominor.yy44 = NULL;
		} break;
		case 84: /* expr ::= expr likeop expr escape */
		{
			if (pQuery->parseState_ == Query::PARSESTATE_SELECTION) {
				UTIL_THROW_USER_ERROR(GS_ERROR_TQ_FUNCTION_NOT_FOUND,
					"Operation cannot use in this context");
			}
			else {
				ExprList arg(TQLPARSER_ALLOCATOR);
				arg.push_back(yymsp[-3].minor.yy44);
				arg.push_back(yymsp[-1].minor.yy44);
				arg.push_back(Expr::newBooleanValue(
					yymsp[-2].minor.yy0.minusflag == 1, TQLPARSER_CONTEXT));
				if (yymsp[0].minor.yy44 != NULL) {
					arg.push_back(yymsp[0].minor.yy44);
				}
				yygotominor.yy44 = TQLPARSER_NEW BoolExpr(
					Expr::newFunctionNode(yymsp[-2].minor.yy0, &arg,
						TQLPARSER_CONTEXT, (*pQuery->getFunctionMap())),
					TQLPARSER_CONTEXT);
			}
		} break;
		default:
			/* (3) explain ::= */ yytestcase(yyruleno == 3);
			/* (6) select ::= oneselect */ yytestcase(yyruleno == 6);
			/* (7) oneselect ::= SELECT selcollist from where_opt orderby_opt limit_opt */ yytestcase(
				yyruleno == 7);
			/* (19) seltablist ::= LP seltablist RP */ yytestcase(
				yyruleno == 19);
			/* (20) dbnm ::= */ yytestcase(yyruleno == 20);
			/* (22) orderby_opt ::= */ yytestcase(yyruleno == 22);
			/* (32) where_opt ::= */ yytestcase(yyruleno == 32);
			break;
		};
		yygoto = yyRuleInfo[yyruleno].lhs;
		yysize = yyRuleInfo[yyruleno].nrhs;
		yyidx -= yysize;
		yyact = yy_find_reduce_action(
			yymsp[-yysize].stateno, (tqlParser_YYCODETYPE)yygoto);
		if (yyact < tqlParser_YYNSTATE) {
#ifdef NDEBUG
			/* If we are not debugging and the reduce action popped at least
			** one element off the stack, then we can push the new element back
			** onto the stack here, and skip the stack overflow test in
			*yy_shift().
			** That gives a significant speed improvement. */
			if (yysize) {
				yyidx++;
				yymsp -= yysize - 1;
				yymsp->stateno = (tqlParser_YYACTIONTYPE)yyact;
				yymsp->major = (tqlParser_YYCODETYPE)yygoto;
				yymsp->minor = yygotominor;
			}
			else
#endif
			{
				yy_shift(yyact, yygoto, &yygotominor);
			}
		}
		else {
			assert(yyact == tqlParser_YYNSTATE + tqlParser_YYNRULE + 1);
			yy_accept();
		}
	}

/*
** The following code executes when the parse fails
*/
#ifndef tqlParser_YYNOERRORRECOVERY
	void yy_parse_failed() {
#ifndef NDEBUG
		if (yyTraceFILE) {
			*yyTraceFILE << yyTracePrompt << "Fail!" << std::endl;
		}
#endif
		while (yyidx >= 0) yy_pop_parser_stack();
		/* Here code is inserted which will be executed whenever the
		** parser fails */
	}
#endif /* tqlParser_YYNOERRORRECOVERY */

	/*
	** The following code executes when a syntax error first occurs.
	*/
	void yy_syntax_error(int yymajor, /* The major type of the error token */
		tqlParser_YYMINORTYPE yyminor /* The minor type of the error token */
		) {
#define TOKEN (yyminor.yy0)

		if (TOKEN.z[0]) {
			util::String str(
				"Syntax error during parse: ", TQLPARSER_ALLOCATOR);
			str += util::String(TOKEN.z, TOKEN.n, TQLPARSER_ALLOCATOR);
			pQuery->setError(str.c_str());
		}
		else {
			pQuery->setError("Syntax error before finishing parse");
		}
		pQuery->parseState_ = Query::PARSESTATE_END;
	}

	/*
	** The following is executed when the parser accepts
	*/
	void yy_accept() {
#ifndef NDEBUG
		if (yyTraceFILE) {
			*yyTraceFILE << yyTracePrompt << "Accept!" << std::endl;
		}
#endif
		while (yyidx >= 0) yy_pop_parser_stack();
		/* Here code is inserted which will be executed whenever the
		** parser accepts */
	}

	/* The main parser program.
	** The first argument is a pointer to a structure obtained from
	** "tqlParserAlloc" which describes the current state of the parser.
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
	void Execute(int yymajor,		/* The major token code number */
		tqlParserTOKENTYPE &yyminor /* The value for the token */
		tqlParserARG_PDECL			/* Optional %extra_argument parameter */
		) {
		tqlParser_YYMINORTYPE yyminorunion;
		int yyact;		  /* The parser action. */
		int yyendofinput; /* True if we are at the end of input */
#ifdef tqlParser_YYERRORSYMBOL
		int yyerrorhit = 0; /* True if yymajor has invoked an error */
#endif

		/* (re)initialize the parser, if necessary */
		if (yyidx < 0) {
#if tqlParser_YYSTACKDEPTH <= 0
			if (yystksz <= 0) {
				/*memset(&yyminorunion, 0, sizeof(yyminorunion));*/
				yyminorunion = tqlParser_yyzerominor;
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
		yyendofinput = (yymajor == 0);
		tqlParserARG_STORE;

#ifndef NDEBUG
		if (yyTraceFILE) {
			*yyTraceFILE << yyTracePrompt << "Input " << yyTokenName[yymajor]
						 << std::endl;
		}
#endif

		do {
			yyact = yy_find_shift_action((tqlParser_YYCODETYPE)yymajor);
			if (yyact < tqlParser_YYNSTATE) {
				assert(!yyendofinput); /* Impossible to shift the $ token */
				yy_shift(yyact, yymajor, &yyminorunion);
				yyerrcnt--;
				yymajor = tqlParser_YYNOCODE;
			}
			else if (yyact < tqlParser_YYNSTATE + tqlParser_YYNRULE) {
				yy_reduce(yyact - tqlParser_YYNSTATE);
			}
			else {
				assert(yyact == tqlParser_YY_ERROR_ACTION);
#ifdef tqlParser_YYERRORSYMBOL
				int yymx;
#endif
#ifndef NDEBUG
				if (yyTraceFILE) {
					*yyTraceFILE << yyTracePrompt << "Syntax Error!"
								 << std::endl;
				}
#endif
#ifdef tqlParser_YYERRORSYMBOL
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
				if (yyerrcnt < 0) {
					yy_syntax_error(yymajor, yyminorunion);
				}
				yymx = yystack[yyidx].major;
				if (yymx == tqlParser_YYERRORSYMBOL || yyerrorhit) {
#ifndef NDEBUG
					if (yyTraceFILE) {
						*yyTraceFILE << yyTracePrompt << "Discard input token "
									 << yyTokenName[yymajor] << std::endl;
					}
#endif
					yy_destructor((tqlParser_YYCODETYPE)yymajor, &yyminorunion);
					yymajor = tqlParser_YYNOCODE;
				}
				else {
					while (
						yyidx >= 0 && yymx != tqlParser_YYERRORSYMBOL &&
						(yyact = yy_find_reduce_action(yystack[yyidx].stateno,
							 tqlParser_YYERRORSYMBOL)) >= tqlParser_YYNSTATE) {
						yy_pop_parser_stack();
					}
					if (yyidx < 0 || yymajor == 0) {
						yy_destructor(
							(tqlParser_YYCODETYPE)yymajor, &yyminorunion);
						yy_parse_failed();
						yymajor = tqlParser_YYNOCODE;
					}
					else if (yymx != tqlParser_YYERRORSYMBOL) {
						tqlParser_YYMINORTYPE u2;
						u2.tqlParser_YYERRSYMDT = 0;
						yy_shift(yyact, tqlParser_YYERRORSYMBOL, &u2);
					}
				}
				yyerrcnt = 3;
				yyerrorhit = 1;
#elif defined(tqlParser_YYNOERRORRECOVERY)
				/* If the tqlParser_YYNOERRORRECOVERY macro is defined, then do
				*not attempt to
				** do any kind of error recovery.  Instead, simply invoke the
				*syntax
				** error routine and continue going as if nothing had happened.
				**
				** Applications can set this macro (for example inside %include)
				*if
				** they intend to abandon the parse upon the first syntax error
				*seen.
				*/
				yy_syntax_error(yymajor, yyminorunion);
				yy_destructor((tqlParser_YYCODETYPE)yymajor, &yyminorunion);
				yymajor = tqlParser_YYNOCODE;

#else /* tqlParser_YYERRORSYMBOL is not defined */
				/* This is what we do if the grammar does not define ERROR:
				**
				**  * Report an error message, and throw away the input token.
				**
				**  * If the input token is $, then fail the parse.
				**
				** As before, subsequent error messages are suppressed until
				** three input tokens have been successfully shifted.
				*/
				if (yyerrcnt <= 0) {
					yy_syntax_error(yymajor, yyminorunion);
				}
				yyerrcnt = 3;
				yy_destructor((tqlParser_YYCODETYPE)yymajor, &yyminorunion);
				if (yyendofinput) {
					yy_parse_failed();
				}
				yymajor = tqlParser_YYNOCODE;
#endif
			}
		} while (yymajor != tqlParser_YYNOCODE && yyidx >= 0);
		return;
	}
};
}
#endif
