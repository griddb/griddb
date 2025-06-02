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
	@brief Implementation of TqlFunc and FunctionMap
*/

#include "function_map.h"
#include "expression.h"
#include "qp_def.h"
#include "transaction_context.h"
#include "function_gis.h"
#include "gis_generator.h"
#include "gis_geomfromtext.h"
#include "aggregation_func.h"
#include "function_array.h"
#include "function_float.h"
#include "function_string.h"
#include "function_timestamp.h"

FunctionMap FunctionMap::gismap_(true);
FunctionMap FunctionMap::map_(false);
SpecialIdMap SpecialIdMap::map_;
SelectionMap SelectionMap::map_;
AggregationMap AggregationMap::map_;

#include "selection_func_impl.h"
#include "special_id_map_impl.h"

/*!
 * @brief Function map constructor
 *
 * @param forgis Generation flag for GIS function
 *
 */
FunctionMap::FunctionMap(bool forgis) : OpenHash<TqlFunc>() {
	if (forgis) {
		RegisterFunction<PointGenerator>("POINT");
		RegisterFunction<LineStringGenerator>("LINESTRING");
		RegisterFunction<PolygonGenerator>("POLYGON");
		RegisterFunction<PolyhedronGenerator>("POLYHEDRALSURFACE");
		RegisterFunction<QuadraticSurfaceGenerator>("QUADRATICSURFACE");
		return;
	}
	else {
		RegisterFunction<FunctorString>("STRING");
		RegisterFunction<FunctorCharLength>("CHAR_LENGTH");
		RegisterFunction<FunctorConcat>("CONCAT");
		RegisterFunction<FunctorLike>("LIKE");
		RegisterFunction<FunctorSubstring>("SUBSTRING");
		RegisterFunction<Functor_upper>("UPPER");
		RegisterFunction<Functor_lower>("LOWER");

		RegisterFunction<FunctorNow>("NOW");
		RegisterFunction< FunctorTimestamp<
				util::DateTime::FIELD_MILLISECOND, false> >("TIMESTAMP");
		RegisterFunction< FunctorTimestamp<
				util::DateTime::FIELD_MILLISECOND, true> >("TIMESTAMP_MS");
		RegisterFunction< FunctorTimestamp<
				util::DateTime::FIELD_MICROSECOND, true> >("TIMESTAMP_US");
		RegisterFunction< FunctorTimestamp<
				util::DateTime::FIELD_NANOSECOND, true> >("TIMESTAMP_NS");

		RegisterFunction<FunctorTimestampadd>("TIMESTAMP_ADD");
		RegisterFunction<FunctorTimestampdiff>("TIMESTAMP_DIFF");
		RegisterFunction<FunctorTimestampadd>("TIMESTAMPADD");
		RegisterFunction<FunctorTimestampdiff>("TIMESTAMPDIFF");
		RegisterFunction<Functor_from_timestamp>("FROM_TIMESTAMP");
		RegisterFunction<FunctorToTimestampMS>("TO_TIMESTAMP_MS");
		RegisterFunction<FunctorToEpochMS>("TO_EPOCH_MS");

		RegisterFunction<FunctorArrayLength>("ARRAY_LENGTH");
		RegisterFunction<FunctorElement>("ELEMENT");

		RegisterFunction<FunctorRound>("ROUND");
		RegisterFunction<FunctorCeil>("CEILING");
		RegisterFunction<FunctorFloor>("FLOOR");

		RegisterFunction<FunctorGeomFromText>("ST_GEOMFROMTEXT");
		RegisterFunction<FunctorMakerect>("ST_MAKERECT");
		RegisterFunction<FunctorMakebox>("ST_MAKEBOX");

		RegisterFunction<FunctorMakeplane>("ST_MAKEPLANE");
		RegisterFunction<FunctorMakesphere>("ST_MAKESPHERE");
		RegisterFunction<FunctorMakecylinder>("ST_MAKECYLINDER");
		RegisterFunction<FunctorMakecone>("ST_MAKECONE");
		RegisterFunction<FunctorMakeqsf>("ST_MAKEQSF");
		RegisterFunction<FunctorGetsrid>("ST_GETSRID");

		RegisterFunction<FunctorMbrIntersects>("ST_MBRINTERSECTS");
		RegisterFunction<FunctorQsfmbrIntersects>("ST_QSFMBRINTERSECTS");
	}
}

/*!
 * @brief Default function call operator
 *
 * @param args Argument list
 * @param column_values Values bound to column
 * @param function_map Function map
 * @param mode Evaluation mode described in EvalMode
 * @param txn The transaction context
 * @param txn Object manager
 * @param argsAfterEval Evaluation result of args
 *
 * @return Result as expression
 */
Expr *TqlFunc::operator()(ExprList &args, ContainerRowWrapper *column_values,
	FunctionMap *function_map, EvalMode mode, TransactionContext &txn,
	ObjectManagerV4 &objectManager, AllocateStrategy &strategy, ExprList &argsAfterEval) {
	for (ExprList::const_iterator it = args.begin(); it != args.end(); it++) {
		argsAfterEval.insert(argsAfterEval.end(),
			(*it)->eval(txn, objectManager, strategy, column_values, function_map, mode));
	}

	Expr *ret = (*this)(argsAfterEval, txn, objectManager, strategy);

	for (ExprList::const_iterator it = argsAfterEval.begin();
		 it != argsAfterEval.end(); it++) {
		QP_DELETE(*it);
	}

	return ret;
}
