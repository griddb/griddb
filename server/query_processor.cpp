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
	@brief Implementation of QueryProcessor
*/

#include "query.h"
#include "base_container.h"
#include "data_store_v4.h"
#include "data_store_common.h"
#include "query_processor.h"
#include "result_set.h"
#include "value_operator.h"
#include "btree_map.h"
#include "gis_quadraticsurface.h"
#include "rtree_map.h"
#include "data_store_common.h"
#include "gs_error.h"
#include "picojson.h"
#include <sstream>

const char8_t *const QueryProcessor::ANALYZE_QUERY = "#analyze";


/*!
	@brief Execute TQL Query
*/
void QueryProcessor::executeTQL(TransactionContext &txn,
	BaseContainer &container, ResultSize limit, const TQLInfo &tqlInfo,
	ResultSet &resultSet) {
	const char *query = tqlInfo.query_;
	try {
		bool noop = (limit == 0);
		if (resultSet.getLargeInfo() != NULL) {
			noop = false;
		}

		if (noop) {
			resultSet.setResultType(RESULT_ROWSET, 0);
			return;
		}

		if (strcmp(query, ANALYZE_QUERY) == 0) {
			Query analyzeQuery(txn, *(container.getObjectManager()), container.getRowAllcateStrategy(), tqlInfo);
			analyzeQuery.enableExplain(true);
			assignDistributedTarget(txn, container, analyzeQuery, resultSet);
			if (container.isInvalid()) {
				analyzeQuery.addExplain(
					0, "CONTAINER", "STRING", "INVALID", "");
			}
			else {
				analyzeQuery.addExplain(0, "CONTAINER", "STRING", "VALID", "");
			}
			analyzeQuery.finishQuery(txn, resultSet, container);
			return;
		}

		switch (container.getContainerType()) {
		case COLLECTION_CONTAINER: {
			Collection *collection = reinterpret_cast<Collection *>(&container);
			QueryStopwatchHook *hook = NULL;
			QueryForCollection queryObj(txn, *collection, tqlInfo, limit, hook);
			assignDistributedTarget(txn, container, queryObj, resultSet);
			queryObj.setQueryOption(txn, resultSet);
			if (queryObj.getLimit() == 0) {
				ResultType resultType = RESULT_ROWSET;
				if (queryObj.hasAggregationClause()) {
					resultType = RESULT_AGGREGATE;
				}
				resultSet.setResultType(resultType, 0);

				if (queryObj.doExplain()) {
					queryObj.addExplain(0, "QUERY_RESULT_ROWS", "INTEGER", "0", "");
				}
				queryObj.finishQuery(txn, resultSet, container);
				return;
			}

			queryObj.doQuery(txn, *collection, resultSet);  

			if (queryObj.doExecute()) {
				queryObj.doSelection(txn, *collection, resultSet);
			}
			queryObj.finishQuery(txn, resultSet, container);

		} break;
		case TIME_SERIES_CONTAINER: {
			TimeSeries *timeSeries = reinterpret_cast<TimeSeries *>(&container);
			QueryStopwatchHook *hook = NULL;
			QueryForTimeSeries queryObj(txn, *timeSeries, tqlInfo, limit, hook);
			assignDistributedTarget(txn, container, queryObj, resultSet);
			queryObj.setQueryOption(txn, resultSet);

			if (queryObj.getLimit() == 0) {
				ResultType resultType = RESULT_ROWSET;
				if (queryObj.hasAggregationClause()) {
					resultType = RESULT_AGGREGATE;
				}
				resultSet.setResultType(resultType, 0);

				if (queryObj.doExplain()) {
					queryObj.addExplain(0, "QUERY_RESULT_ROWS", "INTEGER", "0", "");
				}
				queryObj.finishQuery(txn, resultSet, container);
				return;
			}

			queryObj.doQuery(txn, *timeSeries, resultSet);

			if (queryObj.doExecute()) {
				queryObj.doSelection(txn, *timeSeries, resultSet);
			}
			queryObj.finishQuery(txn, resultSet, container);
		} break;
		default:
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_CONTAINER_TYPE_UNKNOWN, "");	
			break;
		}
	}
	catch (std::exception &e) {
		container.handleSearchError(txn, e, GS_ERROR_QP_COL_QUERY_FAILED);
	}
}

void QueryProcessor::executeMetaTQL(
		TransactionContext &txn, MetaContainer &container,
		MetaProcessorSource &processorSource, ResultSize limit,
		const TQLInfo &tqlInfo, ResultSet &resultSet) {
	QueryForMetaContainer query(txn, container, tqlInfo, limit, NULL);
	query.doQuery(txn, processorSource, resultSet);
}

/*!
	@brief Obtain a set of Rows satisfying the specified spatial range condition
*/
void QueryProcessor::searchGeometryRelated(TransactionContext &txn,
	Collection &collection, ResultSize limit, uint32_t columnId,
	uint32_t geometrySize, uint8_t *geometry, GeometryOperator geometryOp,
	ResultSet &resultSet) {
	try {
		ObjectManagerV4 &objectManager = *(collection.getObjectManager());
		util::XArray<OId> &oIdList = *resultSet.getOIdList();
		Geometry *geom = Geometry::deserialize(txn, geometry, geometrySize);

		oIdList.clear();
		ColumnInfo *cInfoList = collection.getColumnInfoList();
		ColumnInfo *columnInfo = &cInfoList[columnId];

		if (columnInfo->getColumnType() != COLUMN_TYPE_GEOMETRY) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Cannot search not-geometry collection by spacial condition.");
		}
		bool withPartialMatch = false;
		util::Vector<ColumnId> columnIds(txn.getDefaultAllocator());
		columnIds.push_back(columnId);
		if (!collection.hasIndex(txn, columnIds, MAP_TYPE_SPATIAL, withPartialMatch)) {
			util::XArray<OId> tmpOIdList(txn.getDefaultAllocator());
			tmpOIdList.clear();
			BtreeMap::SearchContext sc (txn.getDefaultAllocator(), UNDEF_COLUMNID);
			collection.searchRowIdIndex(txn, sc, tmpOIdList, ORDER_UNDEFINED);
			util::XArray<OId>::iterator itr;
			BaseContainer::RowArray rowArray(txn, &collection);
			ContainerValue value(objectManager, collection.getRowAllcateStrategy());
			for (itr = tmpOIdList.begin(); itr != tmpOIdList.end(); itr++) {
				rowArray.load(txn, *itr, &collection, OBJECT_READ_ONLY);
				BaseContainer::RowArray::Row row(rowArray.getRow(), &rowArray);
				row.getField(txn, *columnInfo, value);
				Geometry *geom2 = Geometry::deserialize(
					txn, value.getValue().data(), value.getValue().size());
				bool conditionFlag;
				switch (geometryOp) {
				case GEOMETRY_INTERSECT:
					conditionFlag = geom->isBoundingRectIntersects(*geom2);
					break;
				case GEOMETRY_INCLUDE:
					conditionFlag = geom->isBoundingRectInclude(*geom2);
					break;
				default:
					GS_THROW_USER_ERROR(
						GS_ERROR_TQ_INTERNAL_GIS_UNKNOWN_RELATIONSHIP,
						"Invalid relationship of Geometry is specified.");
				}
				if (conditionFlag) {
					oIdList.push_back(*itr);
				}
			}
		}
		else {
			RtreeMap::SearchContext::GeomeryCondition geomCond;
			geomCond.valid_ = true;
			if (geom->getType() == Geometry::QUADRATICSURFACE) {
				if (geometryOp == GEOMETRY_INTERSECT ||
					geometryOp == GEOMETRY_QSF_INTERSECT) {
					QuadraticSurface *qsf =
						dynamic_cast<QuadraticSurface *>(geom);
					if (geom == NULL) {
						GS_THROW_USER_ERROR(
							GS_ERROR_TQ_CONSTRAINT_GIS_CANNOT_GET_VALUE,
							"Quadratic surface does not have pv3key");
					}
					geomCond.relation_ = GEOMETRY_QSF_INTERSECT;
					geomCond.pkey_ = qsf->getPv3Key();
				}
				else {
					GS_THROW_USER_ERROR(
						GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE,
						"Invalid operation for quadratic surface");
				}
			}
			else {
				geomCond.rect_[0] = geom->getBoundingRect();
				geomCond.relation_ = geometryOp;
			}
			TermCondition cond(columnInfo->getColumnType(), 
				columnInfo->getColumnType(), DSExpression::GEOM_OP, 
				columnInfo->getColumnId(),
				&geomCond, sizeof(RtreeMap::SearchContext::GeomeryCondition));
			RtreeMap::SearchContext sc (txn.getDefaultAllocator(), columnInfo->getColumnId());
			sc.addCondition(txn, cond, true);
			sc.setLimit(limit);
			collection.searchColumnIdIndex(txn, sc, oIdList);
		}
		resultSet.setResultType(RESULT_ROW_ID_SET, oIdList.size());
	}
	catch (std::exception &e) {
		collection.handleSearchError(
			txn, e, GS_ERROR_QP_SEARCH_GEOM_RELATED_FAILED);
	}
}

/*!
	@brief Obtain a set of Rows satisfying a spatial range condition specifying
   an excluded range
*/
void QueryProcessor::searchGeometry(TransactionContext &txn,
	Collection &collection, ResultSize limit, uint32_t columnId,
	uint32_t geometrySize1, uint8_t *geometry1, uint32_t geometrySize2,
	uint8_t *geometry2, ResultSet &resultSet) {
	try {
		ObjectManagerV4 &objectManager = *(collection.getObjectManager());
		util::XArray<OId> &oIdList = *resultSet.getOIdList();

		Geometry *geom1 = Geometry::deserialize(txn, geometry1, geometrySize1);
		Geometry *geom2 = Geometry::deserialize(txn, geometry2, geometrySize2);


		oIdList.clear();
		ColumnInfo *cInfoList = collection.getColumnInfoList();
		ColumnInfo *columnInfo = &cInfoList[columnId];
		if (columnInfo->getColumnType() != COLUMN_TYPE_GEOMETRY) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_MAP_NOT_FOUND,
				"Cannot search not-geometry collection by spacial condition.");
		}

		bool withPartialMatch = false;
		util::Vector<ColumnId> columnIds(txn.getDefaultAllocator());
		columnIds.push_back(columnId);
		if (!collection.hasIndex(txn, columnIds, MAP_TYPE_SPATIAL, withPartialMatch)) {
			util::XArray<OId> tmpOIdList(txn.getDefaultAllocator());
			tmpOIdList.clear();
			BtreeMap::SearchContext sc (txn.getDefaultAllocator(), UNDEF_COLUMNID);
			collection.searchRowIdIndex(txn, sc, tmpOIdList, ORDER_UNDEFINED);
			util::XArray<OId>::iterator itr;
			BaseContainer::RowArray rowArray(txn, &collection);
			ContainerValue value(objectManager, collection.getRowAllcateStrategy());
			for (itr = tmpOIdList.begin(); itr != tmpOIdList.end(); itr++) {
				rowArray.load(txn, *itr, &collection, OBJECT_READ_ONLY);
				BaseContainer::RowArray::Row row(rowArray.getRow(), &rowArray);
				row.getField(txn, *columnInfo, value);
				Geometry *geom = Geometry::deserialize(
					txn, value.getValue().data(), value.getValue().size());
			bool conditionFlag = geom1->isBoundingRectIntersects(*geom) &&
								 !geom2->isBoundingRectIntersects(*geom);

			if (conditionFlag) {
				oIdList.push_back(*itr);
			}
			}
		}
		else {
			RtreeMap::SearchContext::GeomeryCondition geomCond;
			geomCond.valid_ = true;
			geomCond.rect_[0] = geom1->getBoundingRect();
			geomCond.rect_[1] = geom2->getBoundingRect();
			geomCond.relation_ = GEOMETRY_DIFFERENTIAL;

			TermCondition cond(columnInfo->getColumnType(), 
				columnInfo->getColumnType(), DSExpression::GEOM_OP, 
				columnInfo->getColumnId(),
				&geomCond, sizeof(RtreeMap::SearchContext::GeomeryCondition));
			RtreeMap::SearchContext sc (txn.getDefaultAllocator(), columnInfo->getColumnId());
			sc.addCondition(txn, cond, true);
			sc.setLimit(limit);

			collection.searchColumnIdIndex(txn, sc, oIdList);
		}
		resultSet.setResultType(RESULT_ROW_ID_SET, oIdList.size());
	}
	catch (std::exception &e) {
		collection.handleSearchError(txn, e, GS_ERROR_QP_SEARCH_GEOM_FAILED);
	}
}

