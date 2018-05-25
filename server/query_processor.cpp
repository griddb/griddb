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
#include "data_store.h"
#include "data_store_common.h"
#include "query_processor.h"
#include "result_set.h"
#include "value_operator.h"
#include "transaction_manager.h"
#include "btree_map.h"
#include "data_store_common.h"
#include "gs_error.h"
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

		if (noop) {
			resultSet.setResultType(RESULT_ROWSET, 0);
			return;
		}

		if (strcmp(query, ANALYZE_QUERY) == 0) {
			Query analyzeQuery(txn, *(container.getObjectManager()), tqlInfo);
			analyzeQuery.enableExplain(true);
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



