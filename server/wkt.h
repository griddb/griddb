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

#ifndef LEMPAR_CPP_wktParser_GENERATED_HPP
#define LEMPAR_CPP_wktParser_GENERATED_HPP

#ifdef _WIN32
#pragma warning(disable : 4065)
#endif
/* First off, code is included that follows the "include" declaration
** in the input grammar file. */

#include "wkt_token.h"

#include "util/container.h"
#include "gs_error.h"
#include "qp_def.h"

#include <cassert>
#include <iostream>
#include <stdexcept>

#include "expression.h"
#include "gis_multipolygon.h"
#include "gis_pointgeom.h"
#include "gis_polygon.h"
#include "gis_polyhedralsurface.h"
#include "gis_quadraticsurface.h"

#ifdef _WIN32
#define atoll atol
#endif
/*
** Disable all error recovery processing in the parser push-down
** automaton.
*/
#define YYNOERRORRECOVERY 1

#define PRINT(...)

namespace lemon_wktParser {
struct wktParserArg {
	Expr **ev;
	TransactionContext *txn;
	ObjectManagerV4*objectManager;
	AllocateStrategy *strategy;
	FunctionMap *fmap;
	int err;
};
};

#define WKTPARSER_NEW QP_ALLOC_NEW(arg->txn->getDefaultAllocator())
#define WKTPARSER_DELETE(x) QP_ALLOC_DELETE(arg->txn->getDefaultAllocator(), x)
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
**    wktParser_YYCODETYPE         is the data type used for storing terminal
**                       and nonterminal numbers.  "unsigned char" is
**                       used if there are fewer than 250 terminals
**                       and nonterminals.  "int" is used otherwise.
**    wktParser_YYNOCODE           is a number of type wktParser_YYCODETYPE
*which corresponds
**                       to no legal terminal or nonterminal number.  This
**                       number is used to fill in empty slots of the hash
**                       table.
**    wktParser_YYFALLBACK         If defined, this indicates that one or more
*tokens
**                       have fall-back values which should be used if the
**                       original value of the token will not parse.
**    wktParser_YYACTIONTYPE       is the data type used for storing terminal
**                       and nonterminal numbers.  "unsigned char" is
**                       used if there are fewer than 250 rules and
**                       states combined.  "int" is used otherwise.
**    wktParserTOKENTYPE     is the data type used for minor tokens given
**                       directly to the parser from the tokenizer.
**    wktParser_YYMINORTYPE        is the data type used for all minor tokens.
**                       This is typically a union of many types, one of
**                       which is wktParserTOKENTYPE.  The entry in the union
**                       for base tokens is called "yy0".
**    wktParser_YYSTACKDEPTH       is the maximum depth of the parser's stack.
*If
**                       zero the stack is dynamically sized using realloc()
**    wktParserARG_SDECL     A static variable declaration for the
*%extra_argument
**    wktParserARG_PDECL     A parameter declaration for the %extra_argument
**    wktParser_YYNSTATE           the combined number of states.
**    wktParser_YYNRULE            the number of rules in the grammar
**    wktParser_YYERRORSYMBOL      is the code number of the error symbol.  If
*not
**                       defined, then do no error processing.
*/
#define wktParser_YYCODETYPE unsigned char
#define wktParser_YYNOCODE 35
#define wktParser_YYACTIONTYPE unsigned char
#define wktParser_YYWILDCARD 1
#define wktParserTOKENTYPE Token
typedef union {
	int yyinit;
	wktParserTOKENTYPE yy0;
	QuadraticSurface *yy2;
	double yy8;
	MultiPolygon *yy22;
	MultiPoint *yy28;
	QP_XArray<Expr *> *yy29;
	QP_XArray<Point *> *yy30;
	Polygon *yy35;
	Expr *yy46;
	QP_XArray<Polygon *> *yy47;
	QP_XArray<MultiPoint *> *yy59;
	Point *yy61;
} wktParser_YYMINORTYPE;
#ifndef wktParser_YYSTACKDEPTH
#define wktParser_YYSTACKDEPTH 2000
#endif
#define wktParserARG_SDECL lemon_wktParser::wktParserArg *arg
#define wktParserARG_PDECL , lemon_wktParser::wktParserArg *arg
#define wktParserARG_STORE this->arg = arg
#define wktParser_YYNSTATE 80
#define wktParser_YYNRULE 38
#define wktParser_YY_NO_ACTION (wktParser_YYNSTATE + wktParser_YYNRULE + 2)
#define wktParser_YY_ACCEPT_ACTION (wktParser_YYNSTATE + wktParser_YYNRULE + 1)
#define wktParser_YY_ERROR_ACTION (wktParser_YYNSTATE + wktParser_YYNRULE)

/* The yyzerominor constant is used to initialize instances of
** wktParser_YYMINORTYPE objects to zero. */
static const wktParser_YYMINORTYPE wktParser_yyzerominor = {0};

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
 * @brief wktParser
 *
 */
namespace lemon_wktParser {

/*!
 * @brief wktParser parser main
 *
 */
class wktParser {
/* Next are the tables used to determine what action to take based on the
** current state and lookahead token.  These tables are used to implement
** functions that take a state number and lookahead value and return an
** action integer.
**
** Suppose the action integer is N.  Then the action is determined as
** follows
**
**   0 <= N < wktParser_YYNSTATE                  Shift N.  That is, push the
*lookahead
**                                      token onto the stack and goto state N.
**
**   wktParser_YYNSTATE <= N < wktParser_YYNSTATE+wktParser_YYNRULE   Reduce by
*rule N-wktParser_YYNSTATE.
**
**   N == wktParser_YYNSTATE+wktParser_YYNRULE              A syntax error has
*occurred.
**
**   N == wktParser_YYNSTATE+wktParser_YYNRULE+1            The parser accepts
*its input.
**
**   N == wktParser_YYNSTATE+wktParser_YYNRULE+2            No such action.
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
** is equal to wktParser_YY_SHIFT_USE_DFLT, it means that the action is not in
*the table
** and that yy_default[S] should be used instead.
**
** The formula above is for computing the action when the lookahead is
** a terminal symbol.  If the lookahead is a non-terminal (as occurs after
** a reduce action) then the yy_reduce_ofst[] array is used in place of
** the yy_shift_ofst[] array and wktParser_YY_REDUCE_USE_DFLT is used in place
*of
** wktParser_YY_SHIFT_USE_DFLT.
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

#define wktParser_YY_ACTTAB_COUNT (240)
#define wktParser_YY_SHIFT_USE_DFLT (-7)
#define wktParser_YY_SHIFT_COUNT (51)
#define wktParser_YY_SHIFT_MIN (-6)
#define wktParser_YY_SHIFT_MAX (126)
#define wktParser_YY_REDUCE_USE_DFLT (-14)
#define wktParser_YY_REDUCE_COUNT (28)
#define wktParser_YY_REDUCE_MIN (-13)
#define wktParser_YY_REDUCE_MAX (206)
#define wktParser_YYNFALLBACK (1)

	static const wktParser_YYACTIONTYPE yy_action[wktParser_YY_ACTTAB_COUNT];
	static const wktParser_YYCODETYPE yy_lookahead[wktParser_YY_ACTTAB_COUNT];
	static const short yy_shift_ofst[wktParser_YY_SHIFT_COUNT + 1];
	static const short yy_reduce_ofst[wktParser_YY_REDUCE_COUNT + 1];
	static const wktParser_YYACTIONTYPE yy_default[];

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
#ifdef wktParser_YYFALLBACK
	static const wktParser_YYCODETYPE yyFallback[wktParser_YYNFALLBACK];
#endif /* wktParser_YYFALLBACK */

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
		wktParser_YYACTIONTYPE stateno; /* The state-number */
		wktParser_YYCODETYPE major; /* The major token value.  This is the code
			  ** number for the token at this stack level */
		wktParser_YYMINORTYPE
			minor; /* The user-supplied minor token value.  This
** is the value of the token  */
	};
	typedef struct yyStackEntry yyStackEntry;

	/* The state of the parser is completely contained in an instance of
	** the following structure */

	int yyidx; /* Index of top element in stack */
#ifdef wktParser_YYTRACKMAXSTACKDEPTH
	int yyidxMax; /* Maximum value of yyidx */
#endif
	int yyerrcnt;		/* Shifts left before out of the error */
	wktParserARG_SDECL; /* A place to hold %extra_argument */
#if wktParser_YYSTACKDEPTH <= 0
	int yystksz;		   /* Current side of the stack */
	yyStackEntry *yystack; /* The parser's stack */
#else
	yyStackEntry yystack[wktParser_YYSTACKDEPTH]; /* The parser's stack */
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
	void wktParserSetTrace(std::ostream *TraceFILE, const char *zTracePrompt) {
		yyTraceFILE = TraceFILE;
		yyTracePrompt = zTracePrompt;
		if (yyTraceFILE == 0) yyTraceFILE = 0;
	}
#endif /* NDEBUG */

#ifndef NDEBUG
protected:
	static const char *const yyTokenName[];
	static const char *const yyRuleName[wktParser_YYNRULE];
#endif

#if wktParser_YYSTACKDEPTH <= 0
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
	** to wktParser and wktParserFree.
	*/
public:
	wktParser() {
		yyidx = -1;
#ifdef wktParser_YYTRACKMAXSTACKDEPTH
		yidxMax = 0;
#endif
#if wktParser_YYSTACKDEPTH <= 0
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
		wktParser_YYCODETYPE yymajor,   /* Type code for object to destroy */
		wktParser_YYMINORTYPE *yypminor /* The object to be destroyed */
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
		wktParser_YYCODETYPE yymajor;
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
	**       obtained from wktParserAlloc.
	** <li>  A pointer to a function used to reclaim memory obtained
	**       from malloc.
	** </ul>
	*/
public:
	~wktParser() {
		while (yyidx >= 0) yy_pop_parser_stack();
#if wktParser_YYSTACKDEPTH <= 0
		delete yystack;
		yystack = NULL;
#endif
	}

/*
** Return the peak depth of the stack for a parser.
*/
#ifdef wktParser_YYTRACKMAXSTACKDEPTH
public:
	int wktParserStackPeak(void *p) {
		return yyidxMax;
	}
#endif

	/*
	** Find the appropriate action for a parser given the terminal
	** look-ahead token iLookAhead.
	**
	** If the look-ahead token is wktParser_YYNOCODE, then check to see if the
	*action is
	** independent of the look-ahead.  If it is, return the action, otherwise
	** return wktParser_YY_NO_ACTION.
	*/
	int yy_find_shift_action(
		wktParser_YYCODETYPE iLookAhead /* The look-ahead token */
		) {
		int i;
		int stateno = this->yystack[this->yyidx].stateno;

		if (stateno > wktParser_YY_SHIFT_COUNT ||
			(i = yy_shift_ofst[stateno]) == wktParser_YY_SHIFT_USE_DFLT) {
			return yy_default[stateno];
		}
		assert(iLookAhead != wktParser_YYNOCODE);
		i += iLookAhead;
		if (i < 0 || i >= wktParser_YY_ACTTAB_COUNT ||
			yy_lookahead[i] != iLookAhead) {
			if (iLookAhead > 0) {
#ifdef wktParser_YYFALLBACK
				wktParser_YYCODETYPE iFallback; /* Fallback token */
				if (iLookAhead < wktParser_YYNFALLBACK &&
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
#ifdef wktParser_YYWILDCARD
				{
					int j = i - iLookAhead + wktParser_YYWILDCARD;
					if (
#if wktParser_YY_SHIFT_MIN + wktParser_YYWILDCARD < 0
						j >= 0 &&
#endif
#if wktParser_YY_SHIFT_MAX + wktParser_YYWILDCARD >= wktParser_YY_ACTTAB_COUNT
						j < wktParser_YY_ACTTAB_COUNT &&
#endif
						yy_lookahead[j] == wktParser_YYWILDCARD) {
#ifndef NDEBUG
						if (yyTraceFILE) {
							*yyTraceFILE << yyTracePrompt << "WILDCARD "
										 << yyTokenName[iLookAhead] << " => "
										 << yyTokenName[wktParser_YYWILDCARD]
										 << std::endl;
						}
#endif /* NDEBUG */
						return yy_action[j];
					}
				}
#endif /* wktParser_YYWILDCARD */
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
	** If the look-ahead token is wktParser_YYNOCODE, then check to see if the
	*action is
	** independent of the look-ahead.  If it is, return the action, otherwise
	** return wktParser_YY_NO_ACTION.
	*/
	int yy_find_reduce_action(int stateno, /* Current state number */
		wktParser_YYCODETYPE iLookAhead	/* The look-ahead token */
		) {
		int i;
#ifdef wktParser_YYERRORSYMBOL
		if (stateno > wktParser_YY_REDUCE_COUNT) {
			return yy_default[stateno];
		}
#else
		assert(stateno <= wktParser_YY_REDUCE_COUNT);
#endif
		i = yy_reduce_ofst[stateno];
		assert(i != wktParser_YY_REDUCE_USE_DFLT);
		assert(iLookAhead != wktParser_YYNOCODE);
		i += iLookAhead;
#ifdef wktParser_YYERRORSYMBOL
		if (i < 0 || i >= wktParser_YY_ACTTAB_COUNT ||
			yy_lookahead[i] != iLookAhead) {
			return yy_default[stateno];
		}
#else
		assert(i >= 0 && i < wktParser_YY_ACTTAB_COUNT);
		assert(yy_lookahead[i] == iLookAhead);
#endif
		return yy_action[i];
	}

	/*
	** The following routine is called if the stack overflows.
	*/
	void yyStackOverflow(wktParser_YYMINORTYPE *yypMinor) {
		yyidx--;
#ifndef NDEBUG
		if (yyTraceFILE) {
			*yyTraceFILE << yyTracePrompt << "Stack Overflow!" << std::endl;
		}
#endif
		while (yyidx >= 0) yy_pop_parser_stack();
		/* Here code is inserted which will execute if the parser
		** stack every overflows */
	}

	/*
	** Perform a shift action.
	*/
	void yy_shift(int yyNewState, /* The new state to shift in */
		int yyMajor,			  /* The major token to shift in */
		wktParser_YYMINORTYPE
			*yypMinor /* Pointer to the minor token to shift in */
		) {
		yyStackEntry *yytos;
		yyidx++;
#ifdef wktParser_YYTRACKMAXSTACKDEPTH
		if (yyidx > yyidxMax) {
			yyidxMax = yyidx;
		}
#endif
#if wktParser_YYSTACKDEPTH > 0
		if (yyidx >= wktParser_YYSTACKDEPTH) {
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
		yytos->stateno = (wktParser_YYACTIONTYPE)yyNewState;
		yytos->major = (wktParser_YYCODETYPE)yyMajor;
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
		wktParser_YYCODETYPE lhs; /* Symbol on the left-hand side of the rule */
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
		wktParser_YYMINORTYPE yygotominor; /* The LHS of the rule reduced */
		yyStackEntry *yymsp;			   /* The top of the parser's stack */
		int yysize;						   /* Amount to pop the stack */
		yymsp = &yystack[yyidx];
#ifndef NDEBUG
		if (yyTraceFILE && yyruleno >= 0 && yyruleno < wktParser_YYNRULE) {
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
		yygotominor = wktParser_yyzerominor;

		switch (yyruleno) {
		/* Beginning here are the reduction cases.  A typical example
		** follows:
		**   case 0:
		**  #line <lineno> <grammarfile>
		**     { ... }           
		**  #line <lineno> <thisfile>
		**     break;
		*/
		case 0: /* geom ::= GISFUNC LP gisarg RP */
		{
			Expr *e = Expr::newFunctionNode(yymsp[-3].minor.yy0,
				yymsp[-1].minor.yy29, *(arg->txn), *(arg->fmap));
			*arg->ev = e->eval(*(arg->txn), *(arg->objectManager), *(arg->strategy), NULL,
				arg->fmap, EVAL_MODE_NORMAL);
			WKTPARSER_DELETE(e);
			if (yymsp[-1].minor.yy29) {
				WKTPARSER_DELETE(yymsp[-1].minor.yy29);
			}
		} break;
		case 1: /* geom ::= GISFUNC LP EMPTY RP */
		{
			Expr *emptyArg1 = Expr::newStringValue("EMPTY", *(arg->txn));
			QP_XArray<Expr *> emptyArg(arg->txn->getDefaultAllocator());
			emptyArg.push_back(emptyArg1);
			Expr *e = Expr::newFunctionNode(
				yymsp[-3].minor.yy0, &emptyArg, *(arg->txn), *(arg->fmap));
			*arg->ev = e->eval(*(arg->txn), *(arg->objectManager), *(arg->strategy), NULL,
				arg->fmap, EVAL_MODE_NORMAL);
			WKTPARSER_DELETE(e);
			WKTPARSER_DELETE(emptyArg1);
		} break;
		case 2: /* gisarg ::= gisexpr */
		{
			yygotominor.yy29 =
				WKTPARSER_NEW ExprList(arg->txn->getDefaultAllocator());
			yygotominor.yy29->push_back(yymsp[0].minor.yy46);
			yygotominor.yy29->push_back(
				Expr::newNumericValue(static_cast<int64_t>(-1), *(arg->txn)));
		} break;
		case 3: /* gisarg ::= gisexpr SEMICOLON INTEGER */
		{
			yygotominor.yy29 =
				WKTPARSER_NEW ExprList(arg->txn->getDefaultAllocator());
			yygotominor.yy29->push_back(yymsp[-2].minor.yy46);

			char **endptr = NULL;
			int64_t val = std::strtol(yymsp[0].minor.yy0.z, endptr, 0);
			yygotominor.yy29->push_back(Expr::newNumericValue(val,
				*(arg->txn)));
		} break;
		case 4: /* gisarg ::= gisexpr SEMICOLON MINUS INTEGER */
		{
			yygotominor.yy29 =
				WKTPARSER_NEW ExprList(arg->txn->getDefaultAllocator());
			yygotominor.yy29->push_back(yymsp[-3].minor.yy46);
			char **endptr = NULL;
			int64_t val = std::strtol(yymsp[0].minor.yy0.z, endptr, 0);
			yygotominor.yy29->push_back(Expr::newNumericValue(-val,
				*(arg->txn)));
		} break;
		case 5: /* gisarg ::= */
		{
			yygotominor.yy29 = NULL;
		} break;
		case 6: /* gisexpr ::= gismultipoint2d */
		{
			yygotominor.yy46 =
				Expr::newGeometryValue(yymsp[0].minor.yy28, *(arg->txn));
		} break;
		case 7: /* gisexpr ::= gismultipoint3d */
		{
			yygotominor.yy46 =
				Expr::newGeometryValue(yymsp[0].minor.yy28, *(arg->txn));
		} break;
		case 8: /* gisexpr ::= gispolygon2d */
		case 9: /* gisexpr ::= gispolygon3d */
			yytestcase(yyruleno == 9);
			{
				yygotominor.yy46 =
					Expr::newGeometryValue(yymsp[0].minor.yy35, *(arg->txn));
			}
			break;
		case 10: /* gisexpr ::= gismultipolygon3d */
		{
			yygotominor.yy46 =
				Expr::newGeometryValue(yymsp[0].minor.yy22, *(arg->txn));
		} break;
		case 11: /* gisexpr ::= gisqsf */
		{
			yygotominor.yy46 =
				Expr::newGeometryValue(yymsp[0].minor.yy2, *(arg->txn));
		} break;
		case 12: /* gispolygon2d ::= LP gisnpointlist2d RP */
		{
			yygotominor.yy35 = WKTPARSER_NEW Polygon(static_cast<int64_t>(-1),
				yymsp[-1].minor.yy59, *(arg->txn), *(arg->objectManager), *(arg->strategy));
			while (!yymsp[-1].minor.yy59->empty()) {
				WKTPARSER_DELETE(yymsp[-1].minor.yy59->back());
				yymsp[-1].minor.yy59->pop_back();
			}
			WKTPARSER_DELETE(yymsp[-1].minor.yy59);
		} break;
		case 13: /* gispolygon2d ::= gisnpointlist2d */
		{
			yygotominor.yy35 = WKTPARSER_NEW Polygon(
				-1, yymsp[0].minor.yy59, *(arg->txn), *(arg->objectManager), *(arg->strategy));
			while (!yymsp[0].minor.yy59->empty()) {
				WKTPARSER_DELETE(yymsp[0].minor.yy59->back());
				yymsp[0].minor.yy59->pop_back();
			}
			WKTPARSER_DELETE(yymsp[0].minor.yy59);
		} break;
		case 14: /* gisnpointlist2d ::= gisnpointlist2d COMMA LP gismultipoint2d
					RP */
		case 24: /* gisnpointlist3d ::= gisnpointlist3d COMMA LP gismultipoint3d
					RP */
			yytestcase(yyruleno == 24);
			{
				yygotominor.yy59 = yymsp[-4].minor.yy59;
				yymsp[-4].minor.yy59->push_back(yymsp[-1].minor.yy28);
			}
			break;
		case 15: /* gisnpointlist2d ::= LP gismultipoint2d RP */
		case 25: /* gisnpointlist3d ::= LP gismultipoint3d RP */
			yytestcase(yyruleno == 25);
			{
				yygotominor.yy59 = WKTPARSER_NEW QP_XArray<MultiPoint *>(
					arg->txn->getDefaultAllocator());
				yygotominor.yy59->push_back(yymsp[-1].minor.yy28);
			}
			break;
		case 16: /* gismultipoint2d ::= gispointlist2d */
		{
			yygotominor.yy28 = WKTPARSER_NEW MultiPoint(
				-1, *yymsp[0].minor.yy30, *(arg->txn), *(arg->objectManager), *(arg->strategy));
			while (!yymsp[0].minor.yy30->empty()) {
				WKTPARSER_DELETE(yymsp[0].minor.yy30->back());
				yymsp[0].minor.yy30->pop_back();
			}
			WKTPARSER_DELETE(yymsp[0].minor.yy30);
		} break;
		case 17: /* gispointlist2d ::= gispointlist2d COMMA gispoint2d */
		case 27: /* gispointlist3d ::= gispointlist3d COMMA gispoint3d */
			yytestcase(yyruleno == 27);
			{
				if (yymsp[-2].minor.yy30 == NULL) {
					yygotominor.yy30 = WKTPARSER_NEW QP_XArray<Point *>(
						arg->txn->getDefaultAllocator());
				}
				else {
					yygotominor.yy30 = yymsp[-2].minor.yy30;
				}
				yygotominor.yy30->push_back(yymsp[0].minor.yy61);
			}
			break;
		case 18: /* gispointlist2d ::= gispoint2d */
		case 28: /* gispointlist3d ::= gispoint3d */
			yytestcase(yyruleno == 28);
			{
				yygotominor.yy30 = WKTPARSER_NEW QP_XArray<Point *>(
					arg->txn->getDefaultAllocator());
				yygotominor.yy30->push_back(yymsp[0].minor.yy61);
			}
			break;
		case 19: /* gispoint2d ::= signed signed */
		{
			yygotominor.yy61 =
				WKTPARSER_NEW Point(-1, yymsp[-1].minor.yy8, yymsp[0].minor.yy8,
					std::numeric_limits<double>::quiet_NaN(), *(arg->txn));
		} break;
		case 20: /* gismultipolygon3d ::= gisnpolygonlist3d */
		{
			yygotominor.yy22 =
				WKTPARSER_NEW MultiPolygon(static_cast<int64_t>(-1),
					*yymsp[0].minor.yy47, *(arg->txn), *(arg->objectManager), *(arg->strategy));
			while (!yymsp[0].minor.yy47->empty()) {
				WKTPARSER_DELETE(yymsp[0].minor.yy47->back());
				yymsp[0].minor.yy47->pop_back();
			}
			WKTPARSER_DELETE(yymsp[0].minor.yy47);
		} break;
		case 21: /* gisnpolygonlist3d ::= gisnpolygonlist3d COMMA LP
					gispolygon3d RP */
		{
			yygotominor.yy47 = yymsp[-4].minor.yy47;
			yygotominor.yy47->push_back(yymsp[-1].minor.yy35);
		} break;
		case 22: /* gisnpolygonlist3d ::= LP gispolygon3d RP */
		{
			yygotominor.yy47 = WKTPARSER_NEW QP_XArray<Polygon *>(
				arg->txn->getDefaultAllocator());
			yygotominor.yy47->push_back(yymsp[-1].minor.yy35);
		} break;
		case 23: /* gispolygon3d ::= gisnpointlist3d */
		{
			yygotominor.yy35 = WKTPARSER_NEW Polygon(
				-1, yymsp[0].minor.yy59, *(arg->txn), *(arg->objectManager), *(arg->strategy));
			while (!yymsp[0].minor.yy59->empty()) {
				WKTPARSER_DELETE(yymsp[0].minor.yy59->back());
				yymsp[0].minor.yy59->pop_back();
			}
			WKTPARSER_DELETE(yymsp[0].minor.yy59);
		} break;
		case 26: /* gismultipoint3d ::= gispointlist3d */
		{
			yygotominor.yy28 = WKTPARSER_NEW MultiPoint(
				-1, *yymsp[0].minor.yy30, *(arg->txn), *(arg->objectManager), *(arg->strategy));
			while (!yymsp[0].minor.yy30->empty()) {
				WKTPARSER_DELETE(yymsp[0].minor.yy30->back());
				yymsp[0].minor.yy30->pop_back();
			}
			WKTPARSER_DELETE(yymsp[0].minor.yy30);
		} break;
		case 29: /* gispoint3d ::= signed signed signed */
		{
			yygotominor.yy61 = WKTPARSER_NEW Point(-1, yymsp[-2].minor.yy8,
				yymsp[-1].minor.yy8, yymsp[0].minor.yy8, *(arg->txn));
		} break;
		case 30: /* gisqsf ::= signed signed signed signed signed signed signed
					signed signed signed signed signed signed */
		{
			yygotominor.yy2 = WKTPARSER_NEW QuadraticSurface(*(arg->txn),
				TR_PV3KEY_NONE, 13, yymsp[-12].minor.yy8, yymsp[-11].minor.yy8,
				yymsp[-10].minor.yy8, yymsp[-9].minor.yy8, yymsp[-8].minor.yy8,
				yymsp[-7].minor.yy8, yymsp[-6].minor.yy8, yymsp[-5].minor.yy8,
				yymsp[-4].minor.yy8, yymsp[-3].minor.yy8, yymsp[-2].minor.yy8,
				yymsp[-1].minor.yy8, yymsp[0].minor.yy8);
		} break;
		case 31: /* signed ::= plus_num */
		case 32: /* signed ::= minus_num */
			yytestcase(yyruleno == 32);
			{ yygotominor.yy8 = yymsp[0].minor.yy8; }
			break;
		case 33: /* plus_num ::= plus_opt number */
		{
			yygotominor.yy8 = yymsp[0].minor.yy8;
		} break;
		case 34: /* minus_num ::= MINUS number */
		{
			yygotominor.yy8 = -yymsp[0].minor.yy8;
		} break;
		case 35: /* number ::= INTEGER|FLOAT */
		{
			yygotominor.yy8 = atof(yymsp[0].minor.yy0.z);
		} break;
		case 36: /* plus_opt ::= PLUS */
		{
			PRINT("PLUS_OPT1");
		} break;
		case 37: /* plus_opt ::= */
		{
			PRINT("PLUS_OPT2");
		} break;
		default:
			break;
		};
		yygoto = yyRuleInfo[yyruleno].lhs;
		yysize = yyRuleInfo[yyruleno].nrhs;
		yyidx -= yysize;
		yyact = yy_find_reduce_action(
			yymsp[-yysize].stateno, (wktParser_YYCODETYPE)yygoto);
		if (yyact < wktParser_YYNSTATE) {
#ifdef NDEBUG
			/* If we are not debugging and the reduce action popped at least
			** one element off the stack, then we can push the new element back
			** onto the stack here, and skip the stack overflow test in
			*yy_shift().
			** That gives a significant speed improvement. */
			if (yysize) {
				yyidx++;
				yymsp -= yysize - 1;
				yymsp->stateno = (wktParser_YYACTIONTYPE)yyact;
				yymsp->major = (wktParser_YYCODETYPE)yygoto;
				yymsp->minor = yygotominor;
			}
			else
#endif
			{
				yy_shift(yyact, yygoto, &yygotominor);
			}
		}
		else {
			assert(yyact == wktParser_YYNSTATE + wktParser_YYNRULE + 1);
			yy_accept();
		}
	}

/*
** The following code executes when the parse fails
*/
#ifndef wktParser_YYNOERRORRECOVERY
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
#endif /* wktParser_YYNOERRORRECOVERY */

	/*
	** The following code executes when a syntax error first occurs.
	*/
	void yy_syntax_error(int yymajor, /* The major type of the error token */
		wktParser_YYMINORTYPE yyminor /* The minor type of the error token */
		) {
#define TOKEN (yyminor.yy0)

		arg->err = 1;
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
	** "wktParserAlloc" which describes the current state of the parser.
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
		wktParserTOKENTYPE &yyminor /* The value for the token */
		wktParserARG_PDECL			/* Optional %extra_argument parameter */
		) {
		wktParser_YYMINORTYPE yyminorunion;
		int yyact;		  /* The parser action. */
		int yyendofinput; /* True if we are at the end of input */
#ifdef wktParser_YYERRORSYMBOL
		int yyerrorhit = 0; /* True if yymajor has invoked an error */
#endif

		/* (re)initialize the parser, if necessary */
		if (yyidx < 0) {
#if wktParser_YYSTACKDEPTH <= 0
			if (yystksz <= 0) {
				/*memset(&yyminorunion, 0, sizeof(yyminorunion));*/
				yyminorunion = wktParser_yyzerominor;
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
		wktParserARG_STORE;

#ifndef NDEBUG
		if (yyTraceFILE) {
			*yyTraceFILE << yyTracePrompt << "Input " << yyTokenName[yymajor]
						 << std::endl;
		}
#endif

		do {
			yyact = yy_find_shift_action((wktParser_YYCODETYPE)yymajor);
			if (yyact < wktParser_YYNSTATE) {
				assert(!yyendofinput); /* Impossible to shift the $ token */
				yy_shift(yyact, yymajor, &yyminorunion);
				yyerrcnt--;
				yymajor = wktParser_YYNOCODE;
			}
			else if (yyact < wktParser_YYNSTATE + wktParser_YYNRULE) {
				yy_reduce(yyact - wktParser_YYNSTATE);
			}
			else {
				assert(yyact == wktParser_YY_ERROR_ACTION);
#ifdef wktParser_YYERRORSYMBOL
				int yymx;
#endif
#ifndef NDEBUG
				if (yyTraceFILE) {
					*yyTraceFILE << yyTracePrompt << "Syntax Error!"
								 << std::endl;
				}
#endif
#ifdef wktParser_YYERRORSYMBOL
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
				if (yymx == wktParser_YYERRORSYMBOL || yyerrorhit) {
#ifndef NDEBUG
					if (yyTraceFILE) {
						*yyTraceFILE << yyTracePrompt << "Discard input token "
									 << yyTokenName[yymajor] << std::endl;
					}
#endif
					yy_destructor((wktParser_YYCODETYPE)yymajor, &yyminorunion);
					yymajor = wktParser_YYNOCODE;
				}
				else {
					while (
						yyidx >= 0 && yymx != wktParser_YYERRORSYMBOL &&
						(yyact = yy_find_reduce_action(yystack[yyidx].stateno,
							 wktParser_YYERRORSYMBOL)) >= wktParser_YYNSTATE) {
						yy_pop_parser_stack();
					}
					if (yyidx < 0 || yymajor == 0) {
						yy_destructor(
							(wktParser_YYCODETYPE)yymajor, &yyminorunion);
						yy_parse_failed();
						yymajor = wktParser_YYNOCODE;
					}
					else if (yymx != wktParser_YYERRORSYMBOL) {
						wktParser_YYMINORTYPE u2;
						u2.wktParser_YYERRSYMDT = 0;
						yy_shift(yyact, wktParser_YYERRORSYMBOL, &u2);
					}
				}
				yyerrcnt = 3;
				yyerrorhit = 1;
#elif defined(wktParser_YYNOERRORRECOVERY)
				/* If the wktParser_YYNOERRORRECOVERY macro is defined, then do
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
				yy_destructor((wktParser_YYCODETYPE)yymajor, &yyminorunion);
				yymajor = wktParser_YYNOCODE;

#else /* wktParser_YYERRORSYMBOL is not defined */
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
				yy_destructor((wktParser_YYCODETYPE)yymajor, &yyminorunion);
				if (yyendofinput) {
					yy_parse_failed();
				}
				yymajor = wktParser_YYNOCODE;
#endif
			}
		} while (yymajor != wktParser_YYNOCODE && yyidx >= 0);
		return;
	}
};
}
#endif
