/*
	Copyright (c) 2012 TOSHIBA CORPORATION.

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
	@brief Definition of Select functions for TQL
*/

#ifndef SELECTION_FUNC_H_
#define SELECTION_FUNC_H_

#include "function_map.h"
#include "time_series.h"
#include "value_processor.h"

class OutputMessageRowStore;
/*!
 * @brief Represents the information of Sampling Row
 */
struct SamplingRow {
	PointRowId rowid;
	Timestamp key;
	const Value *value;
};

/*!
 * @brief Selection base class
 */
class TqlSelection {
public:
	/*!
	 * @brief Default selection caller
	 *
	 * @param txn Transaction Context
	 * @param timeSeries TimeSeries to search
	 * @param resultRowIdList Search result ID list
	 * @param isSorted True if the search scans rowkey.
	 * @param limit Limit of the size of result rowset
	 * @param args Argument list
	 * @param messageRowStore Output row image
	 * @param resultType ResultType
	 * @param notFiltered True if no where clause is written
	 *
	 * @return Number of results
	 */
	virtual int operator()(TransactionContext &txn, TimeSeries &timeSeries,
		util::XArray<PointRowId> &resultRowIdList, bool isSorted,
		OutputOrder apiOutputOrder, SortExprList *orderByExpr, uint64_t limit,
		uint64_t offset, FunctionMap &function_map, ExprList &args,
		OutputMessageRowStore *messageRowStore, ResultType &resultType) = 0;

	/*!
	 * @brief Default selection caller
	 *
	 * @param txn Transaction Context
	 * @param collection Collection collection to search
	 * @param resultRowIdList Search result ID list
	 * @param isSorted True if the search scans rowkey.
	 * @param limit Limit of the size of result rowset
	 * @param args Argument list
	 * @param messageRowStore Output row image
	 * @param resultType ResultType
	 * @param notFiltered True if no where clause is written
	 *
	 * @return Number of results
	 */
	virtual int operator()(TransactionContext &, Collection &,
		util::XArray<OId> &, bool, OutputOrder, SortExprList *, uint64_t,
		uint64_t, FunctionMap &, ExprList &, OutputMessageRowStore *,
		ResultType &) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_SELECTION_ABUSE,
			"Selection function for TimeSeries is used to collection");
	}

	/*!
	 * @brief If the selection must be treated as special?
	 *
	 * @return
	 */
	virtual QpPassMode getPassMode() {
		return QP_PASSMODE_NO_PASS;
	}
	virtual uint64_t apiPassThrough(TransactionContext &txn,
		TimeSeries &timeSeries, BtreeMap::SearchContext &sc,
		OutputOrder apiOutputOrder, uint64_t limit, uint64_t offset,
		ExprList &args, OutputMessageRowStore *messageRowStore,
		ResultType &resultType) = 0;

	/*!
	 * @brief If function has internal valuables, returns a clone object of
	 * "this"
	 * @return Cloned object or Constant object
	 */
	virtual TqlSelection *clone(util::StackAllocator &) {
		return this;
	}

	/*!
	 * @brief If clone() generates a new object, delete that
	 */
	virtual void destruct() { /* DO NOTHING */
	}

	virtual ~TqlSelection() {}
};

/*!
 * @brief time_find function(TIME_NEXT, TIME_NEXT_ONLY, TIME_PREV,
 * TIME_PREV_ONLY)
 *
 */
template <bool isJustInclude, bool isAccending>
class SelectionTimeFind : public TqlSelection {
public:
	using TqlSelection::operator();
	int operator()(TransactionContext &txn, TimeSeries &timeSeries,
		util::XArray<PointRowId> &resultRowIdList, bool isSorted,
		OutputOrder apiOutputOrder, SortExprList *orderByExpr, uint64_t limit,
		uint64_t offset, FunctionMap &function_map, ExprList &args,
		OutputMessageRowStore *messageRowStore, ResultType &resultType);
	QpPassMode getPassMode() {
		return QP_PASSMODE_PASS;
	}
	uint64_t apiPassThrough(TransactionContext &txn, TimeSeries &timeSeries,
		BtreeMap::SearchContext &sc, OutputOrder apiOutputOrder, uint64_t limit,
		uint64_t offset, ExprList &args, OutputMessageRowStore *messageRowStore,
		ResultType &resultType);
	virtual ~SelectionTimeFind() {}
};

/*!
 * @brief TIME_INTERPOLATED(column, timestamp)
 *
 */
class SelectionTimeInterpolated : public TqlSelection {
public:
	using TqlSelection::operator();
	int operator()(TransactionContext &txn, TimeSeries &timeSeries,
		util::XArray<PointRowId> &resultRowIdList, bool isSorted,
		OutputOrder apiOutputOrder, SortExprList *orderByExpr, uint64_t limit,
		uint64_t offset, FunctionMap &function_map, ExprList &args,
		OutputMessageRowStore *messageRowStore, ResultType &resultType);
	QpPassMode getPassMode() {
		return QP_PASSMODE_PASS;
	}
	uint64_t apiPassThrough(TransactionContext &txn, TimeSeries &timeSeries,
		BtreeMap::SearchContext &sc, OutputOrder apiOutputOrder, uint64_t limit,
		uint64_t offset, ExprList &args, OutputMessageRowStore *messageRowStore,
		ResultType &resultType);

	void getInterpolatedValue(TransactionContext &txn, Timestamp t,
		const Value &v1, const Value &v2, Timestamp t1, Timestamp t2, Value &v);
	virtual ~SelectionTimeInterpolated() {}
};

/*!
 * @brief TIME_SAMPLING(*|column, timestamp_start, timestamp_end, interval,
 * DAY|HOUR|MINUTE|SECOND|MILLISECOND)
 */
class SelectionTimeSampling : public SelectionTimeInterpolated {
public:
	using TqlSelection::operator();
	int operator()(TransactionContext &txn, TimeSeries &timeSeries,
		util::XArray<PointRowId> &resultRowIdList, bool isSorted,
		OutputOrder apiOutputOrder, SortExprList *orderByExpr, uint64_t limit,
		uint64_t offset, FunctionMap &function_map, ExprList &args,
		OutputMessageRowStore *messageRowStore, ResultType &resultType);
	QpPassMode getPassMode() {
		return QP_PASSMODE_PASS_IF_NO_WHERE;
	}
	uint64_t apiPassThrough(TransactionContext &txn, TimeSeries &timeSeries,
		BtreeMap::SearchContext &sc, OutputOrder apiOutputOrder, uint64_t limit,
		uint64_t offset, ExprList &args, OutputMessageRowStore *messageRowStore,
		ResultType &resultType);
	virtual ~SelectionTimeSampling() {}

private:
	void parseArgument(TransactionContext &txn, ObjectManager &objectManager,
		ExprList &args, uint32_t &columnId, ColumnType &columnType,
		util::DateTime::FieldType &fType, Timestamp &targetTs, Timestamp &endTs,
		int32_t &duration);
};

/*!
 * @brief MAX_ROWS(column)/MIN_ROWS(column)
 *
 */
template <AggregationType aggregateType>
class SelectionMaxMinRows : public TqlSelection {
public:
	using TqlSelection::operator();
	int operator()(TransactionContext &txn, TimeSeries &timeSeries,
		util::XArray<PointRowId> &resultRowIdList, bool isSorted,
		OutputOrder apiOutputOrder, SortExprList *orderByExpr, uint64_t limit,
		uint64_t offset, FunctionMap &function_map, ExprList &args,
		OutputMessageRowStore *messageRowStore, ResultType &resultType) {
		return execute<TimeSeries>(txn, &timeSeries, resultRowIdList, isSorted,
			apiOutputOrder, orderByExpr, limit, offset, function_map, args,
			messageRowStore, resultType);
	}
	int operator()(TransactionContext &txn, Collection &collection,
		util::XArray<PointRowId> &resultRowIdList, bool isSorted,
		OutputOrder apiOutputOrder, SortExprList *orderByExpr, uint64_t limit,
		uint64_t offset, FunctionMap &function_map, ExprList &args,
		OutputMessageRowStore *messageRowStore, ResultType &resultType) {
		return execute<Collection>(txn, &collection, resultRowIdList, isSorted,
			apiOutputOrder, orderByExpr, limit, offset, function_map, args,
			messageRowStore, resultType);
	}
	QpPassMode getPassMode() {
		return QP_PASSMODE_NO_PASS;
	}
	uint64_t apiPassThrough(TransactionContext &, TimeSeries &,
		BtreeMap::SearchContext &, OutputOrder, uint64_t, uint64_t, ExprList &,
		OutputMessageRowStore *, ResultType &) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_NOT_SUPPORTED, "Unsupported operation");
	}
	virtual ~SelectionMaxMinRows() {}

private:
	template <typename R>
	int execute(TransactionContext &txn, BaseContainer *container,
		util::XArray<PointRowId> &resultRowIdList, bool isSorted,
		OutputOrder apiOutputOrder, SortExprList *orderByExpr, uint64_t limit,
		uint64_t offset, FunctionMap &function_map, ExprList &args,
		OutputMessageRowStore *messageRowStore, ResultType &resultType);
};

/*!
 * @brief TIME_NEXT(*, timestamp)
 */
typedef SelectionTimeFind<true, true> Selection_time_next;
/*!
 * @brief TIME_NEXT_ONLY(*, timestamp)
 */
typedef SelectionTimeFind<false, true> Selection_time_next_only;

/*!
 * @brief TIME_PREV(*, timestamp)
 */
typedef SelectionTimeFind<true, false> Selection_time_prev;

/*!
 * @brief TIME_PREV_ONLY(*, timestamp)
 */
typedef SelectionTimeFind<false, false> Selection_time_prev_only;

/*!
 * @brief MAX_ROWS(column)
 */
typedef SelectionMaxMinRows<AGG_MAX> Selection_max_rows;

/*!
 * @brief MIN_ROWS(column)
 */
typedef SelectionMaxMinRows<AGG_MIN> Selection_min_rows;

/*!
 * @brief Hash based map for selection
 */
class SelectionMap : public OpenHash<TqlSelection> {
public:
	static SelectionMap *getInstance() {
		return &map_;
	}
	/*!
	 * @brief Register functions
	 *
	 */
	SelectionMap() : OpenHash<TqlSelection>() {
		RegisterFunction<Selection_time_next>("TIME_NEXT");
		RegisterFunction<Selection_time_next_only>("TIME_NEXT_ONLY");
		RegisterFunction<Selection_time_prev>("TIME_PREV");
		RegisterFunction<Selection_time_prev_only>("TIME_PREV_ONLY");
		RegisterFunction<SelectionTimeInterpolated>("TIME_INTERPOLATED");
		RegisterFunction<SelectionTimeSampling>("TIME_SAMPLING");
		RegisterFunction<Selection_max_rows>("MAX_ROWS");
		RegisterFunction<Selection_min_rows>("MIN_ROWS");
	}

private:
	static SelectionMap map_;
};

#endif
