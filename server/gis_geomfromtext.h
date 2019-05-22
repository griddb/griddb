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
	@brief GeomFromText Functor
*/
#ifndef GIS_GEOMFROMTEXT_H_
#define GIS_GEOMFROMTEXT_H_

#include "function_map.h"
#include "gis_geometry.h"
#include "gis_linestring.h"
#include "gis_multipolygon.h"
#include "gis_pointgeom.h"
#include "gis_polygon.h"
#include "gis_polyhedralsurface.h"
#include "gis_quadraticsurface.h"
#include "qp_def.h"

#include "lexer.h"
#include "tql_token.h"
#include "wkt.h"
#include "wkt_token.h"


/*!
 * @brief GeomFromText Functor
 *
 */
class FunctorGeomFromText : public TqlFunc {
public:
	using TqlFunc::operator();
	Expr *operator()(
		ExprList &args, TransactionContext &txn, ObjectManager &objectManager) {
		if (args.size() != 1) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
				"Invalid argument count");
		}
		FunctionMap *wktFunctionMap = FunctionMap::getInstanceForWkt();

		if (args[0]->isNullValue()) {
			return Expr::newNullValue(txn);
		}
		if (!args[0]->isString()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Argument 1 is not a string.");
		}

		lemon_wktParser::wktParser *p =
			QP_NEW_BY_TXN(txn) lemon_wktParser::wktParser();
		if (p == NULL) {
			GS_THROW_USER_ERROR(
				GS_ERROR_CM_NO_MEMORY, "Cannot allocate WKT parser");
		}
		lemon_wktParser::wktParserArg arg;
		int ret;
		const char *c_str = args[0]->getValueAsString(txn);
		Token t;
		Expr *e = NULL;
		arg.ev = &e;
		arg.txn = &txn;
		arg.objectManager = &objectManager;
		arg.fmap = wktFunctionMap;
		arg.err = 0;

#ifdef WKT_TRACE
		p->wktParserSetTrace(&std::cerr, "wkt_trace:");
#endif

		do {
			ret = getToken(txn, c_str, t);
			if (ret == 0 || ret == -1) {
				break;
			}
			if (t.id == TK_ILLEGAL) {
				break;
			}
			if (ret != -2) {
				p->Execute(t.id, t, &arg);
			}
			if (arg.err != 0) {
				QP_SAFE_DELETE(p);
				std::string str2 =
					"Argument 1 is not a WKT string: ";  
				str2 += c_str;
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE, str2.c_str());
			}
			c_str += t.n;
		} while (1);
		p->Execute(0, t, &arg);

		QP_SAFE_DELETE(p);
		if (*arg.ev == NULL) {
			std::string str2 = "Argument 1 is not a WKT string: ";  
			str2 += c_str;
			GS_THROW_USER_ERROR(
				GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE, str2.c_str());
		}
		return *arg.ev;
	}

private:
	/*!
	 * Get token from the main TQL lexer.
	 * (We do not use special lexer for maintainability.)
	 *
	 * @param str WKT string to parse
	 * @param t Token reference
	 *
	 * @return 1 if succeeded or 0 when failed
	 */
	int getToken(TransactionContext &txn, const char *str, Token &t) {

		int ret = Lexer::gsGetToken(str, t);

#define CASE_MAP_TOKEN(x)  \
	case TK_##x:           \
		t.id = TK_WKT_##x; \
		break
		switch (t.id) {
			CASE_MAP_TOKEN(GISFUNC);
			CASE_MAP_TOKEN(LP);
			CASE_MAP_TOKEN(RP);
			CASE_MAP_TOKEN(SEMICOLON);
			CASE_MAP_TOKEN(INTEGER);
			CASE_MAP_TOKEN(COMMA);
			CASE_MAP_TOKEN(MINUS);
			CASE_MAP_TOKEN(FLOAT);
			CASE_MAP_TOKEN(PLUS);
		case TK_ID: {
			char buf[256], *p;
			if (t.n < 256) {
				p = buf;
			}
			else {
				p = reinterpret_cast<char *>(QP_ALLOCATOR.allocate(t.n + 1));
			}
			memcpy(p, t.z, t.n);
			buf[t.n] = '\0';
			if (util::stricmp(p, "EMPTY") == 0) {
				t.id = TK_WKT_EMPTY;
			}
			else {
				t.id = TK_WKT_GISFUNC;
			}
		} break;
		case TK_SPACE:
			ret = -2;
			break;
		case TK_NAN:
		case TK_INF:
		case TK_ILLEGAL:
			ret = -1;
			break;
		default:
			t.id = TK_WKT_GISFUNC;
			break;
		}
#undef CASE_MAP_TOKEN

		return ret;
	}
};

#endif
