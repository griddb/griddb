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
	@brief Definition of QueryProcessor
*/
#ifndef QUERY_PROCESSOR_H_
#define QUERY_PROCESSOR_H_

#include "collection.h"
#include "result_set.h"  
#include "time_series.h"
#include "value_processor.h"

class ResultSet;

/*!
	@brief QueryProcessor
*/
class QueryProcessor {
public:

	static void executeTQL(TransactionContext &txn, BaseContainer &container,
		ResultSize limit, const TQLInfo &tqlInfo, ResultSet &resultSet);

	static void get(TransactionContext &txn, BaseContainer &container,
		uint32_t rowkeySize, const uint8_t *rowkey, ResultSet &resultSet);

	static void get(TransactionContext &txn, BaseContainer &container,
		RowId preLast, ResultSize limit, RowId &last, ResultSet &rs);

	static void fetch(TransactionContext &txn, BaseContainer &container,
		uint64_t startPos, ResultSize fetchNum, ResultSet *&resultSet,
		LogSequentialNumber lsn = 0);



	static void search(TransactionContext &txn,
		BaseContainer &container, ResultSize limit,
		const util::XArray<uint8_t> *startKey,
		const util::XArray<uint8_t> *endKey, ResultSet &rs);


	static void search(TransactionContext &txn, TimeSeries &timeSeries,
		ResultSize limit, Timestamp start, Timestamp end,
		ResultSet &resultSet) {
		search(txn, timeSeries, ORDER_ASCENDING, limit, start, end, resultSet);
	}
	static void search(TransactionContext &txn, TimeSeries &timeSeries,
		OutputOrder order, ResultSize limit, Timestamp start, Timestamp end,
		ResultSet &resultSet);

	static void sample(TransactionContext &txn, TimeSeries &timeSeries,
		ResultSize limit, Timestamp start, Timestamp end,
		const Sampling &sampling, ResultSet &resultSet);

	static void aggregate(TransactionContext &txn, TimeSeries &timeSeries,
		Timestamp start, Timestamp end, uint32_t columnId,
		AggregationType aggregationType, ResultSet &resultSet);

	static void get(TransactionContext &txn, TimeSeries &timeSeries,
		Timestamp ts, TimeOperator rangeOp, ResultSet &resultSet);

	static void interpolate(TransactionContext &txn, TimeSeries &timeSeries,
		Timestamp ts, uint32_t columnId, ResultSet &resultSet);

	static void get(TransactionContext &txn, TimeSeries &timeSeries,
		RowId preLast, ResultSize limit, RowId &last, ResultSet &resultSet);



private:
	static const uint32_t EXPLAIN_COLUMN_NUM = 6;
	static ColumnInfo *makeExplainColumnInfo(TransactionContext &txn);
	static const char8_t *const ANALYZE_QUERY;  
};


#endif
