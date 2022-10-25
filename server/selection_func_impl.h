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
	@brief Implementation of Select functions for TQL
*/

#ifndef SELECTION_FUNC_IMPL_H_
#define SELECTION_FUNC_IMPL_H_
#include "query_timeseries.h"
#include <algorithm>


#define time_sort(a, b) \
	while (0) {         \
	}

/*!
	@brief Binary search
*/
static size_t time_binary_search(TransactionContext &txn,
	TimeSeries &timeSeries, util::XArray<PointRowId> &resultRowIdList,
	Timestamp targetTs, bool &found, Timestamp &ts) {
	size_t resultRowIdListSize = resultRowIdList.size(), lowPos, midPos,
		   highPos;

	lowPos = 0;
	highPos = resultRowIdListSize - 1;
	midPos = highPos / 2;

	found = false;
	BaseContainer::RowArray rowArray(txn, &timeSeries);  
	while (lowPos <= highPos) {
		midPos = (lowPos + highPos) / 2;
		rowArray.load(txn, resultRowIdList[midPos], &timeSeries,
			OBJECT_READ_ONLY);  
		BaseContainer::RowArray::Row row(rowArray.getRow(), &rowArray);  

		ts = row.getRowId();
		if (ts < targetTs) {
			lowPos = midPos + 1;
		}
		else if (ts > targetTs) {
			if (midPos == 0) break;  
			highPos = midPos - 1;
		}
		else if (ts == targetTs) {
			found = true;
			return midPos;
		}
	}
	rowArray.load(txn, resultRowIdList[midPos], &timeSeries,
		OBJECT_READ_ONLY);  
	BaseContainer::RowArray::Row row(rowArray.getRow(), &rowArray);  

	ts = row.getRowId();
	return midPos;
}

/*!
 * @brief time_find function(TIME_NEXT, TIME_NEXT_ONLY, TIME_PREV,
 * TIME_PREV_ONLY)
 *
 */
template <bool isJustInclude, bool isAscending>
int SelectionTimeFind<isJustInclude, isAscending>::operator()(
	TransactionContext &txn, TimeSeries &timeSeries,
	util::XArray<PointRowId> &resultRowIdList, bool isSorted, OutputOrder,
	SortExprList *, uint64_t, uint64_t offset, FunctionMap &, ExprList &args,
	OutputMessageRowStore *messageRowStore, ResultType &resultType) {
	if (args.size() != 2) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
			"Invalid argument count for selection");
	}

	if (!args[0]->isColumn()) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
			"Invalid argument type for aggregation, first arg must be *");
	}
	uint32_t aggColumnId = args[0]->getColumnId();
	if (aggColumnId != UNDEF_COLUMNID) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_COLUMN_CANNOT_AGGREGATE,
			"Invalid column for aggregation, first arg must be *");
	}

	ObjectManagerV4 &objectManager = *(timeSeries.getObjectManager());
	AllocateStrategy &strategy = timeSeries.getRowAllocateStrategy();
	Expr *tsExpr =
		args[1]->eval(txn, objectManager, strategy, NULL, NULL, EVAL_MODE_NORMAL);
	Timestamp baseTs = tsExpr->getTimeStamp();
	Timestamp ts;
	bool found;
	size_t pos;

	QP_SAFE_DELETE(tsExpr);

	if (resultRowIdList.size() == 0 || offset > 0) {
		return 0;
	}
	if (!isSorted) {
		time_sort(txn, resultRowIdList);
	}
	pos =
		time_binary_search(txn, timeSeries, resultRowIdList, baseTs, found, ts);

	BaseContainer::RowArray rowArray(txn, &timeSeries);  
	if (isJustInclude && found) {
		rowArray.load(txn, resultRowIdList[pos], &timeSeries,
			OBJECT_READ_ONLY);  
		BaseContainer::RowArray::Row row(rowArray.getRow(), &rowArray);  
		row.getImage(txn, messageRowStore, false);					  
		messageRowStore->next();									  
		resultType = RESULT_ROWSET;
		return 1;
	}
	else if (baseTs <= ts && pos == 0) {
		if (!isAscending) {
			return 0;  
		}
		else {
			if (!isJustInclude && ts == baseTs) {
				if (resultRowIdList.size() == 1) return 0;  
				pos++;
			}
			rowArray.load(txn, resultRowIdList[pos], &timeSeries,
				OBJECT_READ_ONLY);  
			BaseContainer::RowArray::Row row(rowArray.getRow(), &rowArray);  
			row.getImage(txn, messageRowStore, false);					  
			messageRowStore->next();									  
			resultType = RESULT_ROWSET;
			return 1;
		}
	}
	else if (baseTs >= ts && pos == resultRowIdList.size() - 1) {
		if (isAscending) {
			return 0;  
		}
		else {
			if (!isJustInclude && ts == baseTs) {
				pos--;
			}
			rowArray.load(txn, resultRowIdList[pos], &timeSeries,
				OBJECT_READ_ONLY);  
			BaseContainer::RowArray::Row row(rowArray.getRow(), &rowArray);  
			row.getImage(txn, messageRowStore, false);					  
			messageRowStore->next();									  
			resultType = RESULT_ROWSET;
			return 1;
		}
	}

	if (found) {
		if (isJustInclude) {
		}
		else if (isAscending) {
			pos++;
		}
		else {  
			pos--;
		}
	}
	else {
		if (isAscending && baseTs > ts) {
			pos++;
		}
		else if (!isAscending && baseTs < ts) {
			pos--;
		}
	}

	rowArray.load(txn, resultRowIdList[pos], &timeSeries,
		OBJECT_READ_ONLY);  
	BaseContainer::RowArray::Row row(rowArray.getRow(), &rowArray);  
	row.getImage(txn, messageRowStore, false);					  
	messageRowStore->next();									  
	resultType = RESULT_ROWSET;

	return 1;
}

/*!
 * @brief time_find function(TIME_NEXT, TIME_NEXT_ONLY, TIME_PREV,
 * TIME_PREV_ONLY) by Container api
 *
 */
template <bool isJustInclude, bool isAscending>
uint64_t SelectionTimeFind<isJustInclude, isAscending>::apiPassThrough(
	TransactionContext &txn, TimeSeries &timeSeries, BtreeMap::SearchContext &sc,
	OutputOrder, uint64_t, uint64_t offset, ExprList &args,
	OutputMessageRowStore *messageRowStore, ResultType &resultType) {
	TimeOperator timeOp;
	uint64_t resultNum = 0;

	if (args.size() != 2) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
			"Invalid argument count for selection");
	}

	if (!args[0]->isColumn()) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
			"Invalid argument type for aggregation, first arg must be *");
	}
	uint32_t aggColumnId = args[0]->getColumnId();
	if (aggColumnId != UNDEF_COLUMNID) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_COLUMN_CANNOT_AGGREGATE,
			"Invalid column for aggregation, first arg must be *");
	}

	if (offset > 0) {
		return 0;
	}

	if (isJustInclude) {
		if (isAscending) {
			timeOp = TIME_NEXT;
		}
		else {
			timeOp = TIME_PREV;
		}
	}
	else {
		if (isAscending) {
			timeOp = TIME_NEXT_ONLY;
		}
		else {
			timeOp = TIME_PREV_ONLY;
		}
	}

	Value v;
	ObjectManagerV4 &objectManager = *(timeSeries.getObjectManager());
	AllocateStrategy &strategy = timeSeries.getRowAllocateStrategy();
	Expr *tsExpr =
		args[1]->eval(txn, objectManager, strategy, NULL, NULL, EVAL_MODE_NORMAL);
	if (tsExpr->isNullValue()) {
		resultNum = 0;
		return 0;
	}
	Timestamp ts = tsExpr->getTimeStamp();
	QP_SAFE_DELETE(tsExpr);

	OId targetOId;
	timeSeries.searchTimeOperator(txn, sc, ts, timeOp, targetOId);
	if (UNDEF_OID == targetOId) {
		resultNum = 0;
		return 0;
	}
	resultNum = 1;
	BaseContainer::RowArray rowArray(txn, &timeSeries);  
	rowArray.load(
		txn, targetOId, &timeSeries, OBJECT_READ_ONLY);  
	BaseContainer::RowArray::Row row(rowArray.getRow(), &rowArray);  
	row.getImage(txn, messageRowStore, false);					  
	messageRowStore->next();									  
	resultType = RESULT_ROWSET;
	return resultNum;
}

/*!
 * @brief return value at position in between the data points.
 *
 */
void SelectionTimeInterpolated::getInterpolatedValue(TransactionContext &txn,
	Timestamp t, const Value &v1, const Value &v2, Timestamp t1, Timestamp t2,
	Value &v) {
	if (v1.isNullValue() || v2.isNullValue()) {
		v.setNull();
		return;
	}
	v.set(0.0);  
	Value tmp1, tmp2, vdiff;
	double rate = static_cast<double>(t - t1) / static_cast<double>(t2 - t1);
	tmp1.set(rate);

	CalculatorTable::subTable_[v2.getType()][v1.getType()](txn, v2.data(), 0, v1.data(), 0,
		vdiff);  
	CalculatorTable::mulTable_[tmp1.getType()][vdiff.getType()](
		txn, tmp1.data(), 0, vdiff.data(), 0, tmp2);
	CalculatorTable::addTable_[tmp2.getType()][v1.getType()](
		txn, tmp2.data(), 0, v1.data(), 0, v);
	int64_t i = v.getLong();
	double d = v.getDouble();
	switch (v1.getType()) {
	case COLUMN_TYPE_BOOL:
		v.set(i != 0);
		break;
	case COLUMN_TYPE_BYTE:
		v.set(static_cast<char8_t>(i));
		break;
	case COLUMN_TYPE_SHORT:
		v.set(static_cast<int16_t>(i));
		break;
	case COLUMN_TYPE_INT:
		v.set(static_cast<int32_t>(i));
		break;
	case COLUMN_TYPE_LONG:
		v.set(static_cast<int64_t>(i));
		break;
	case COLUMN_TYPE_FLOAT:
		v.set(static_cast<float>(d));
		break;
	case COLUMN_TYPE_DOUBLE:
		break;  
	case COLUMN_TYPE_TIMESTAMP:
		v.setTimestamp(static_cast<int64_t>(i));
		break;
	default:
		assert(0);
		break;
	}
}

/*!
 * @brief TIME_INTERPOLATED(column, timestamp)
 *
 */
int SelectionTimeInterpolated::operator()(TransactionContext &txn,
	TimeSeries &timeSeries, util::XArray<PointRowId> &resultRowIdList,
	bool isSorted, OutputOrder, SortExprList *, uint64_t, uint64_t offset,
	FunctionMap &, ExprList &args, OutputMessageRowStore *messageRowStore,
	ResultType &resultType) {
	ObjectManagerV4 &objectManager = *(timeSeries.getObjectManager());
	AllocateStrategy &strategy = timeSeries.getRowAllocateStrategy();
	if (args.size() != 2) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
			"Invalid argument count for selection");
	}
	else if (!args[0]->isColumn()) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
			"Column required for interpolation");
	}
	else if (args[0]->isNullValue() || args[1]->isNullValue()) {
		return 0;
	}

	Value v;
	Expr *tsExpr =
		args[1]->eval(txn, objectManager, strategy, NULL, NULL, EVAL_MODE_NORMAL);

	if (tsExpr->isNullValue()) {
		return 0;
	}

	Timestamp baseTs = tsExpr->getTimeStamp();
	Timestamp ts;
	bool found;
	size_t pos;
	QP_SAFE_DELETE(tsExpr);

	uint32_t columnId = args[0]->getColumnId();
	const ColumnInfo *columnInfo = NULL;
	ColumnType type = COLUMN_TYPE_WITH_BEGIN;
	if (columnId == UNDEF_COLUMNID) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
			"Cannot interpolate '*'");
		return 0;
	}
	else {
		columnInfo = args[0]->getColumnInfo();
		type = columnInfo->getColumnType();
	}

	resultType = RESULT_ROWSET;
	if (!(type >= COLUMN_TYPE_BYTE && type <= COLUMN_TYPE_DOUBLE)) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
			"Cannot interpolate non-numeric column");
	}

	if (resultRowIdList.size() == 0) {
		return 0;
	}
	if (offset > 0) {
		return 0;
	}

	if (!isSorted) {
		time_sort(txn, resultRowIdList);
	}
	pos =
		time_binary_search(txn, timeSeries, resultRowIdList, baseTs, found, ts);

	if (found) {
		BaseContainer::RowArray rowArray(txn, &timeSeries);  
		rowArray.load(txn, resultRowIdList[pos], &timeSeries,
			OBJECT_READ_ONLY);  
		BaseContainer::RowArray::Row row(rowArray.getRow(), &rowArray);  
		row.getImage(txn, messageRowStore, false);					  
		messageRowStore->next();									  
		resultType = RESULT_ROWSET;
		return 1;
	}
	else if (pos == 0 && baseTs < ts) {
		return 0;  
	}
	else if (pos == resultRowIdList.size() - 1 && baseTs > ts) {
		return 0;  
	}

	if (ts < baseTs) {
		pos++;
	}

	Timestamp t1, t2;

	BaseContainer::RowArray rowArray(txn, &timeSeries);  
	rowArray.load(txn, resultRowIdList[pos - 1], &timeSeries,
		OBJECT_READ_ONLY);  
	BaseContainer::RowArray::Row row1(rowArray.getRow(), &rowArray);  
	rowArray.load(txn, resultRowIdList[pos], &timeSeries,
		OBJECT_READ_ONLY);  
	BaseContainer::RowArray::Row row2(rowArray.getRow(), &rowArray);  

	t1 = row1.getRowId();
	t2 = row2.getRowId();  
	if (ts > t2) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_TIM_SAMPLE_FAILED,
			"Specified time is later than all timestamp in resultset.");
	}
	ContainerValue v1(objectManager, timeSeries.getRowAllocateStrategy());
	ContainerValue v2(objectManager, timeSeries.getRowAllocateStrategy());
	ColumnInfo &interpolateColumnInfo =
		timeSeries.getColumnInfo(columnId);  
	rowArray.load(txn, resultRowIdList[pos - 1], &timeSeries,
		OBJECT_READ_ONLY);  
	BaseContainer::RowArray::Row row3(rowArray.getRow(), &rowArray);  
	row3.getField(txn, interpolateColumnInfo, v1);
	rowArray.load(txn, resultRowIdList[pos], &timeSeries,
		OBJECT_READ_ONLY);  
	BaseContainer::RowArray::Row row4(rowArray.getRow(), &rowArray);  
	row4.getField(txn, interpolateColumnInfo, v2);

	getInterpolatedValue(txn, baseTs, v1.getValue(), v2.getValue(), t1, t2, v);

	rowArray.load(txn, resultRowIdList[pos - 1], &timeSeries,
		OBJECT_READ_ONLY);  
	BaseContainer::RowArray::Row row(rowArray.getRow(), &rowArray);  
	row.getImage(txn, messageRowStore, false);					  
	messageRowStore->setField(columnId, v);
	Value timeVal(baseTs);
	messageRowStore->setField(0, timeVal);
	messageRowStore->next();  
	resultType = RESULT_ROWSET;
	return 1;
}

/*!
 * @brief TIME_INTERPOLATED(column, timestamp) by Container api
 *
 */
uint64_t SelectionTimeInterpolated::apiPassThrough(TransactionContext &txn,
	TimeSeries &timeSeries, BtreeMap::SearchContext &, OutputOrder, uint64_t,
	uint64_t offset, ExprList &args, OutputMessageRowStore *messageRowStore,
	ResultType &resultType) {
	uint64_t resultNum = 0;

	if (args.size() != 2) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
			"Invalid argument count for selection");
	}
	else if (!args[0]->isColumn()) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
			"Column required for interpolation");
	}
	else if (args[0]->isNullValue() || args[1]->isNullValue()) {
		return 0;
	}

	uint32_t columnId = args[0]->getColumnId();
	const ColumnInfo *columnInfo = NULL;
	ColumnType type = COLUMN_TYPE_WITH_BEGIN;
	if (columnId == UNDEF_COLUMNID) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
			"Cannot interpolate '*'");
	}
	else {
		columnInfo = args[0]->getColumnInfo();
		type = columnInfo->getColumnType();
	}
	resultType = RESULT_ROWSET;
	if (!(type >= COLUMN_TYPE_BYTE && type <= COLUMN_TYPE_DOUBLE)) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
			"Cannot interpolate non-numeric column");
	}

	Expr *tsExpr = args[1]->eval(
		txn, *(timeSeries.getObjectManager()), timeSeries.getRowAllocateStrategy(), NULL, NULL, EVAL_MODE_NORMAL);
	if (tsExpr->isNullValue()) {
		return 0;
	}
	Timestamp ts = tsExpr->getTimeStamp();
	QP_SAFE_DELETE(tsExpr);

	if (offset > 0) {
		return 0;
	}

	Sampling sampling;
	TermCondition cond(timeSeries.getRowIdColumnType(), timeSeries.getRowIdColumnType(),
		DSExpression::GE, timeSeries.getRowIdColumnId(), &ts,
		sizeof(ts));
	BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, 2);
	if (columnId != UNDEF_COLUMNID) {
		sampling.interpolatedColumnIdList_.push_back(columnId);
	}
	sampling.interval_ = 0;
	sampling.mode_ = INTERP_MODE_LINEAR_OR_PREVIOUS;

	timeSeries.sample(txn, sc, sampling, resultNum, messageRowStore);
	resultType = RESULT_ROWSET;
	return resultNum;
}

/*!
 * @brief TIME_SAMPLING(*|column, timestamp_start, timestamp_end, interval,
 * DAY|HOUR|MINUTE|SECOND|MILLISECOND)
 */
int SelectionTimeSampling::operator()(TransactionContext &txn,
	TimeSeries &timeSeries, util::XArray<PointRowId> &resultRowIdList,
	bool isSorted, OutputOrder, SortExprList *orderByExpr, uint64_t limit,
	uint64_t offset, FunctionMap &function_map, ExprList &args,
	OutputMessageRowStore *messageRowStore, ResultType &resultType) {
	Timestamp currentTs, targetTs, endTs, prevTs;
	util::DateTime::FieldType fType;
	int32_t duration;
	uint32_t columnId;
	ColumnType columnType;
	uint64_t nActualLimit = limit;

	bool doSort = orderByExpr &&
				  !((*orderByExpr)[0].expr->isColumn() &&
					  (*orderByExpr)[0].expr->getColumnId() == 0 &&
					  (*orderByExpr)[0].order == ASC);

	ObjectManagerV4 &objectManager = *(timeSeries.getObjectManager());
	AllocateStrategy &strategy = timeSeries.getRowAllocateStrategy();
	bool isNullValue = parseArgument(txn, objectManager, strategy, args, columnId, columnType, fType,
		targetTs, endTs, duration);
	if (isNullValue) {
		return 0;
	}

	SamplingRow tmpRow;
	tmpRow.value = NULL;

	util::XArray<SamplingRow> arRowList(txn.getDefaultAllocator());

	currentTs = 0;

	size_t i = 0;
	bool found;
	if (resultRowIdList.size() == 0) {
		return 0;
	}
	if (!isSorted) {
		time_sort(txn, resultRowIdList);
	}

	i = time_binary_search(
		txn, timeSeries, resultRowIdList, targetTs, found, currentTs);
	if (currentTs > targetTs && i >= 1) {
		i--;  
	}
	BaseContainer::RowArray rowArray(txn, &timeSeries);  
	if (i >= 1) {
		Value v;
		rowArray.load(txn, resultRowIdList[i - 1], &timeSeries,
			OBJECT_READ_ONLY);  
		BaseContainer::RowArray::Row row(rowArray.getRow(), &rowArray);  
		prevTs = row.getRowId();
	}
	else {
		prevTs = -1;
	}

	util::DateTime dt;
	dt.setUnixTime(targetTs);

	if (offset > 0 && limit != MAX_RESULT_SIZE) {
		limit += offset;
	}
	if (doSort) {
		limit = MAX_RESULT_SIZE;
	}

	util::DateTime::ZonedOption zonedOption = util::DateTime::ZonedOption::create(false, txn.getTimeZone());
	for (; i < resultRowIdList.size() && limit > arRowList.size();) {
		targetTs = dt.getUnixTime();
		rowArray.load(txn, resultRowIdList[i], &timeSeries,
			OBJECT_READ_ONLY);  
		BaseContainer::RowArray::Row row(rowArray.getRow(), &rowArray);  
		currentTs = row.getRowId();

		if (i == 0) {
			while (targetTs < currentTs) {
				dt.addField(duration, fType, zonedOption);  
				targetTs = dt.getUnixTime();
			}
		}

		if (targetTs > endTs) {
			break;
		}  

		if (targetTs == currentTs) {
			tmpRow.rowid = resultRowIdList[i];
			tmpRow.key = targetTs;
			if (columnId != UNDEF_COLUMNID) {
				Value *v = QP_NEW Value();
				ContainerValue currentContainerValue(objectManager, timeSeries.getRowAllocateStrategy());
				rowArray.load(txn, resultRowIdList[i], &timeSeries,
					OBJECT_READ_ONLY);  
				BaseContainer::RowArray::Row row(
					rowArray.getRow(), &rowArray);  
				row.getField(txn, timeSeries.getColumnInfo(columnId),
					currentContainerValue);  
				v->copy(txn, objectManager, strategy,
					currentContainerValue
						.getValue());  
				tmpRow.value = v;
			}
			arRowList.push_back(tmpRow);

			dt.addField(duration, fType, zonedOption);  

			prevTs = currentTs;
			i++;  
			continue;
		}
		else if (i > 0 && targetTs < currentTs) {
			tmpRow.rowid = resultRowIdList[i - 1];
			tmpRow.key = targetTs;
			if (columnId != UNDEF_COLUMNID) {
				Value *v = QP_NEW Value();
				ContainerValue v1(objectManager, timeSeries.getRowAllocateStrategy());
				ContainerValue v2(objectManager, timeSeries.getRowAllocateStrategy());
				rowArray.load(txn, resultRowIdList[i - 1], &timeSeries,
					OBJECT_READ_ONLY);  
				BaseContainer::RowArray::Row row1(
					rowArray.getRow(), &rowArray);  
				row1.getField(
					txn, timeSeries.getColumnInfo(columnId), v1);  
				rowArray.load(txn, resultRowIdList[i], &timeSeries,
					OBJECT_READ_ONLY);  
				BaseContainer::RowArray::Row row2(
					rowArray.getRow(), &rowArray);  
				row2.getField(
					txn, timeSeries.getColumnInfo(columnId), v2);  
				getInterpolatedValue(txn, targetTs, v1.getValue(),
					v2.getValue(), prevTs, currentTs, *v);
				tmpRow.value = v;
			}
			arRowList.push_back(tmpRow);
			dt.addField(duration, fType, zonedOption);  
			continue;
		}
		else {
			i++;  
			prevTs = currentTs;
		}
	}

	if (doSort) {
		TimeSeriesOrderByComparator comp(
			txn, timeSeries, function_map, *orderByExpr, columnId);
		std::sort(arRowList.begin(), arRowList.end(), comp);
	}
	if (offset > 0 && arRowList.size() > offset) {
		arRowList.erase(
			arRowList.begin(), arRowList.begin() + static_cast<size_t>(offset));
	}
	else if (offset > 0 && arRowList.size() <= offset) {
		resultType = RESULT_ROWSET;
		return 0;
	}

	if (arRowList.size() > nActualLimit) {
		arRowList.erase(arRowList.begin() + static_cast<size_t>(nActualLimit),
			arRowList.end());
	}

	util::XArray<PointRowId> starIdList(txn.getDefaultAllocator());
	for (uint32_t i = 0; i < arRowList.size(); i++) {
		starIdList.push_back(arRowList[i].rowid);
	}

	for (uint32_t i = 0; i < starIdList.size(); ++i) {
		rowArray.load(txn, starIdList[i], &timeSeries,
			OBJECT_READ_ONLY);  
		BaseContainer::RowArray::Row row(rowArray.getRow(), &rowArray);  
		row.getImage(txn, messageRowStore, false);					  
		Value v1, v2;
		v1.setTimestamp(arRowList[i].key);
		messageRowStore->setField(0, v1);
		if (columnId != UNDEF_COLUMNID) {
			const Value *pv = arRowList[i].value;
			messageRowStore->setField(columnId, *pv);
		}
		messageRowStore->next();  
	}

	resultType = RESULT_ROWSET;
	return static_cast<int>(arRowList.size());
}

bool SelectionTimeSampling::parseArgument(TransactionContext &txn,
	ObjectManagerV4 &objectManager, AllocateStrategy &strategy, ExprList &args, uint32_t &columnId,
	ColumnType &columnType, util::DateTime::FieldType &fType,
	Timestamp &targetTs, Timestamp &endTs, int32_t &duration) {
	if (args.size() != 5) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
			"Invalid argument count for selection");
	}
	else if (!args[0]->isColumn() ||
			 !(args[3]->isNullValue() || args[3]->isValue()) ||
			 !(args[4]->isColumn() || args[4]->isString())) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
			"Invalid arguments for selection");
	}

	columnId = args[0]->getColumnId();
	const ColumnInfo *columnInfo = NULL;
	columnType = COLUMN_TYPE_WITH_BEGIN;
	if (columnId != UNDEF_COLUMNID) {
		columnInfo = args[0]->getColumnInfo();
		columnType = columnInfo->getColumnType();

		if (!(columnType >= COLUMN_TYPE_BYTE &&
				columnType <= COLUMN_TYPE_DOUBLE)) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Cannot interpolate non-numeric column");
		}
	}

	const char *unitStr;
	Expr *e1, *e2, *e3;
	e1 = args[1]->eval(txn, objectManager, strategy, NULL, NULL, EVAL_MODE_NORMAL);
	e2 = args[2]->eval(txn, objectManager, strategy, NULL, NULL, EVAL_MODE_NORMAL);
	e3 = args[3]->eval(txn, objectManager, strategy, NULL, NULL, EVAL_MODE_NORMAL);
	if (e1->isNullValue() || e2->isNullValue() || e3->isNullValue()) {
		return true;
	}
	targetTs = e1->getTimeStamp();
	endTs = e2->getTimeStamp();
	duration = e3->getValueAsInt();
	QP_SAFE_DELETE(e1);
	QP_SAFE_DELETE(e2);
	QP_SAFE_DELETE(e3);

	if (duration <= 0) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE,
			"Invalid duration for TIME_SAMPLING()");
	}

	if (!args[4]->isColumn()) {
		Expr *e4 = args[4]->eval(txn, objectManager, strategy, NULL, NULL, EVAL_MODE_NORMAL);
		if (e4->isNullValue()) {
			return true;
		}
		unitStr = e4->getValueAsString(txn);
		QP_SAFE_DELETE(e4);
	}
	else {
		unitStr = args[4]->getValueAsString(txn);
	}

	if (util::stricmp(unitStr, "DAY") == 0) {
		fType = util::DateTime::FIELD_DAY_OF_MONTH;
	}
	else if (util::stricmp(unitStr, "HOUR") == 0) {
		fType = util::DateTime::FIELD_HOUR;
	}
	else if (util::stricmp(unitStr, "MINUTE") == 0) {
		fType = util::DateTime::FIELD_MINUTE;
	}
	else if (util::stricmp(unitStr, "SECOND") == 0) {
		fType = util::DateTime::FIELD_SECOND;
	}
	else if (util::stricmp(unitStr, "MILLISECOND") == 0) {
		fType = util::DateTime::FIELD_MILLISECOND;
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE,
			"Invalid time type for TIME_SAMPLING()");
	}
	return false;
}

/*!
 * @brief TIME_SAMPLING(*|column, timestamp_start, timestamp_end, interval,
 * DAY|HOUR|MINUTE|SECOND|MILLISECOND) by Container api
 */
uint64_t SelectionTimeSampling::apiPassThrough(TransactionContext &txn,
	TimeSeries &timeSeries, BtreeMap::SearchContext &sc, OutputOrder,
	uint64_t limit, uint64_t offset, ExprList &args,
	OutputMessageRowStore *messageRowStore, ResultType &resultType) {
	Timestamp startTs, endTs;
	util::DateTime::FieldType fType;
	TimeUnit unit;
	int32_t duration;
	uint32_t columnId;
	ColumnType columnType;
	Sampling sampling;
	int64_t duration_msec;

	ObjectManagerV4 &objectManager = *(timeSeries.getObjectManager());
	AllocateStrategy &strategy = timeSeries.getRowAllocateStrategy();
	bool isNullValue = parseArgument(txn, objectManager, strategy, args, columnId, columnType, fType,
		startTs, endTs, duration);
	if (isNullValue) {
		return 0;
	}

	if (columnId != UNDEF_COLUMNID) {
		sampling.interpolatedColumnIdList_.push_back(columnId);
	}
	sampling.interval_ = duration;
	duration_msec = duration;
	switch (fType) {
	case util::DateTime::FIELD_DAY_OF_MONTH:
		unit = TIME_UNIT_DAY;
		duration_msec *= 24 * 60 * 60 * 1000LL;
		break;
	case util::DateTime::FIELD_HOUR:
		unit = TIME_UNIT_HOUR;
		duration_msec *= 60 * 60 * 1000;
		break;
	case util::DateTime::FIELD_MINUTE:
		unit = TIME_UNIT_MINUTE;
		duration_msec *= 60 * 1000;
		break;
	case util::DateTime::FIELD_SECOND:
		unit = TIME_UNIT_SECOND;
		duration_msec *= 1000;
		break;
	case util::DateTime::FIELD_MILLISECOND:
		unit = TIME_UNIT_MILLISECOND;
		break;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE,
			"Invalid time type for TIME_SAMPLING()");
		break;
	}
	sampling.timeUnit_ = unit;
	sampling.mode_ = INTERP_MODE_LINEAR_OR_PREVIOUS;

	TermCondition *currentStartCond = sc.getStartKeyCondition();
	if (currentStartCond == NULL) {
		TermCondition startCond(
			COLUMN_TYPE_TIMESTAMP, COLUMN_TYPE_TIMESTAMP,
			DSExpression::GE,
			ColumnInfo::ROW_KEY_COLUMN_ID, reinterpret_cast<uint8_t *>(&startTs), sizeof(startTs));
		sc.addCondition(txn, startCond, true);
	}
	else {
		bool isReplace = false;
		Timestamp startTs2 =
			*reinterpret_cast<const Timestamp *>(currentStartCond->value_);
		if (startTs2 > startTs) {
			Timestamp diff = startTs2 - startTs;
			int64_t n = diff / duration_msec;
			startTs = startTs + n * duration_msec;

			isReplace = true;
		}
		else if (startTs2 < startTs) {
			isReplace = true;
		}
		else {
		}
		if (isReplace) {
			currentStartCond->value_ = reinterpret_cast<uint8_t *>(&startTs);
		}
	}

	TermCondition *currentEndCond = sc.getEndKeyCondition();
	if (currentEndCond == NULL) {
		TermCondition endCond(
			COLUMN_TYPE_TIMESTAMP, COLUMN_TYPE_TIMESTAMP,
			DSExpression::LE,
			ColumnInfo::ROW_KEY_COLUMN_ID, reinterpret_cast<uint8_t *>(&endTs), sizeof(endTs));
		sc.addCondition(txn, endCond, true);
	}
	else {
		Timestamp endTs2 =
			*reinterpret_cast<const Timestamp *>(currentEndCond->value_);
		if (endTs2 > endTs) {
			currentEndCond->value_ = reinterpret_cast<uint8_t *>(&endTs);
		}
		else {
			endTs = endTs2;
		}
	}
	if (startTs > endTs) {
		return 0;
	}

	sc.setLimit(limit + offset);

	uint64_t resultNum = 0;
	if (offset == 0) {
		timeSeries.sample(txn, sc, sampling, resultNum, messageRowStore);
	}
	else {
		uint32_t columnNum = timeSeries.getColumnNum();
		util::StackAllocator &alloc = txn.getDefaultAllocator();
		util::XArray<uint8_t> fixedData(alloc);
		util::XArray<uint8_t> variableData(alloc);
		const DataStoreConfig &dsConfig =
			timeSeries.getDataStore()->getConfig();

		OutputMessageRowStore omrs(dsConfig,
			timeSeries.getColumnInfoList(), columnNum, fixedData, variableData,
			false);
		timeSeries.sample(txn, sc, sampling, resultNum, &omrs);
		fixedData.insert(
			fixedData.end(), variableData.begin(), variableData.end());

		InputMessageRowStore imrs(dsConfig,
			timeSeries.getColumnInfoList(), columnNum, fixedData.data(),
			static_cast<uint32_t>(fixedData.size()), resultNum,
			timeSeries.getRowFixedDataSize());

		if (resultNum > offset) {
			resultNum -= offset;
			imrs.position(offset);
			for (size_t i = 0; i < resultNum; i++) {
				messageRowStore->setRow(imrs);
				messageRowStore->next();
				imrs.next();
			}
		}
		else {
			resultNum = 0;
		}
	}
	resultType = RESULT_ROWSET;

	return resultNum;
}

/*!
 * @brief TIME_SAMPLING(*|column, timestamp_start, timestamp_end, interval,
 * DAY|HOUR|MINUTE|SECOND|MILLISECOND)
 */
int SelectionTimeWindowAgg::operator()(TransactionContext &txn,
	TimeSeries &timeSeries, util::XArray<OId> &resultRowIdList,
	bool isSorted, OutputOrder, SortExprList *orderByExpr, uint64_t limit,
	uint64_t offset, FunctionMap &function_map, ExprList &args,
	OutputMessageRowStore *messageRowStore, ResultType &resultType) {
	UNUSED_VARIABLE(isSorted);
	UNUSED_VARIABLE(limit);
	UNUSED_VARIABLE(offset);
	UNUSED_VARIABLE(function_map);

	Timestamp startTime, endTime;
	util::DateTime::FieldType fType;
	int32_t duration;
	uint32_t columnId;
	ColumnType columnType;
	ResultSize resultNum;
	AggregationType aggType;

	if (orderByExpr) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_SELECTION_ABUSE,
			"TimeWindowAgg is not support with order by clause");
	}
	ObjectManagerV4 &objectManager = *(timeSeries.getObjectManager());
	AllocateStrategy &strategy = timeSeries.getRowAllocateStrategy();
	bool isNullValue = parseArgument(txn, objectManager, strategy, args, columnId, columnType, fType,
		startTime, endTime, duration, aggType);
	if (isNullValue) {
		return 0;
	}
	Sampling sampling;
	sampling.interval_ = duration;
	sampling.timeUnit_ = fType;
	timeSeries.aggregateByTimeWindow(txn, columnId, aggType, startTime, endTime,
		sampling, resultRowIdList, resultNum, messageRowStore);
	resultType = RESULT_ROWSET;
	return static_cast<int>(resultNum);
}

bool SelectionTimeWindowAgg::parseArgument(TransactionContext &txn,
	ObjectManagerV4 &objectManager, AllocateStrategy &strategy, ExprList &args, uint32_t &columnId,
	ColumnType &columnType, util::DateTime::FieldType &fType,
	Timestamp &targetTs, Timestamp &endTs, int32_t &duration,
	AggregationType &aggType) {
	if (args.size() != 6) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
			"Invalid argument count for selection");
	}

	columnId = args[0]->getColumnId();
	const ColumnInfo *columnInfo = NULL;
	columnType = COLUMN_TYPE_WITH_BEGIN;
	if (columnId != UNDEF_COLUMNID) {
		columnInfo = args[0]->getColumnInfo();
		columnType = columnInfo->getColumnType();

		if (!(columnType >= COLUMN_TYPE_BYTE &&
				columnType <= COLUMN_TYPE_DOUBLE)) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Cannot aggregate non-numeric column");
		}
	} else {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
			"column not found");
	}

	const char *aggStr, *unitStr;
	Expr *e1, *e2, *e3;

	aggStr = args[1]->getValueAsString(txn);
	if (util::stricmp(aggStr, "MAX") == 0) {
		aggType = AGG_MAX;
	}
	else if (util::stricmp(aggStr, "MIN") == 0) {
		aggType = AGG_MIN;
	}
	else if (util::stricmp(aggStr, "AVG") == 0) {
		aggType = AGG_AVG;
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE,
			"Invalid aggregation type for TIME_WINDOW_AGG() : " << aggStr);
	}

	e1 = args[2]->eval(txn, objectManager, strategy, NULL, NULL, EVAL_MODE_NORMAL);
	e2 = args[3]->eval(txn, objectManager, strategy, NULL, NULL, EVAL_MODE_NORMAL);
	e3 = args[4]->eval(txn, objectManager, strategy, NULL, NULL, EVAL_MODE_NORMAL);
	if (e1->isNullValue() || e2->isNullValue() || e3->isNullValue()) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
				"Cannot input null value");
	}
	targetTs = e1->getTimeStamp();
	endTs = e2->getTimeStamp();
	duration = e3->getValueAsInt();
	QP_SAFE_DELETE(e1);
	QP_SAFE_DELETE(e2);
	QP_SAFE_DELETE(e3);

	if (duration <= 0) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE,
			"Invalid duration for TIME_SAMPLING()");
	}

	unitStr = args[5]->getValueAsString(txn);

	if (util::stricmp(unitStr, "DAY") == 0) {
		fType = util::DateTime::FIELD_DAY_OF_MONTH;
	}
	else if (util::stricmp(unitStr, "HOUR") == 0) {
		fType = util::DateTime::FIELD_HOUR;
	}
	else if (util::stricmp(unitStr, "MINUTE") == 0) {
		fType = util::DateTime::FIELD_MINUTE;
	}
	else if (util::stricmp(unitStr, "SECOND") == 0) {
		fType = util::DateTime::FIELD_SECOND;
	}
	else if (util::stricmp(unitStr, "MILLISECOND") == 0) {
		fType = util::DateTime::FIELD_MILLISECOND;
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_RANGE,
			"Invalid time type for TIME_SAMPLING()");
	}
	return false;
}

/*!
 * @brief TIME_SAMPLING(*|column, timestamp_start, timestamp_end, interval,
 * DAY|HOUR|MINUTE|SECOND|MILLISECOND) by Container api
 */
uint64_t SelectionTimeWindowAgg::apiPassThrough(TransactionContext &txn,
	TimeSeries &timeSeries, BtreeMap::SearchContext &sc, OutputOrder,
	uint64_t limit, uint64_t offset, ExprList &args,
	OutputMessageRowStore *messageRowStore, ResultType &resultType) {
	UNUSED_VARIABLE(sc);
	UNUSED_VARIABLE(limit);
	UNUSED_VARIABLE(offset);

	Timestamp startTime, endTime;
	util::DateTime::FieldType fType;
	int32_t duration;
	uint32_t columnId;
	ColumnType columnType;
	ResultSize resultNum;
	AggregationType aggType;

	ObjectManagerV4 &objectManager = *(timeSeries.getObjectManager());
	AllocateStrategy &strategy = timeSeries.getRowAllocateStrategy();
	bool isNullValue = parseArgument(txn, objectManager, strategy, args, columnId, columnType, fType,
		startTime, endTime, duration, aggType);
	if (isNullValue) {
		return 0;
	}
	ColumnId rowColumnId = timeSeries.getRowIdColumnId();
	ColumnType rowColumnType = timeSeries.getRowIdColumnType();
	TermCondition startCond(rowColumnType, rowColumnType, 
		DSExpression::GE, rowColumnId, &startTime, sizeof(startTime));
	TermCondition endCond(rowColumnType, rowColumnType, 
		DSExpression::LE, rowColumnId, &endTime, sizeof(endTime));

	util::XArray<OId> resultRowIdList(txn.getDefaultAllocator());
	BtreeMap::SearchContext rangeSc(txn.getDefaultAllocator(), startCond, endCond, MAX_RESULT_SIZE);
		timeSeries.searchRowIdIndex(txn, rangeSc, resultRowIdList, ORDER_ASCENDING);

	Sampling sampling;
	sampling.interval_ = duration;
	sampling.timeUnit_ = fType;
	timeSeries.aggregateByTimeWindow(txn, columnId, aggType, startTime, endTime,
		sampling, resultRowIdList, resultNum, messageRowStore);
	resultType = RESULT_ROWSET;
	return static_cast<int>(resultNum);
}

/*!
 * @brief MAX_ROWS(column)/MIN_ROWS(column)
 *
 */
template <AggregationType aggregateType>
template <typename R>
int SelectionMaxMinRows<aggregateType>::execute(TransactionContext &txn,
	BaseContainer *container, util::XArray<PointRowId> &resultRowIdList, bool,
	OutputOrder, SortExprList *orderByExpr, uint64_t limit, uint64_t offset,
	FunctionMap &function_map, ExprList &args,
	OutputMessageRowStore *messageRowStore, ResultType &resultType) {
	uint64_t nActualLimit = limit;

	if (args.size() != 1) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_COUNT,
			"Invalid argument count for selection");
	}
	else if (!args[0]->isColumn()) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_CONSTRAINT_INVALID_ARGUMENT_TYPE,
			"Column required for MAX/MIN_ROWS");
	}

	uint32_t columnId = args[0]->getColumnId();
	if (columnId == UNDEF_COLUMNID) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_COLUMN_CANNOT_AGGREGATE,
			"Invalid column for aggregation");
	}

	ColumnInfo &columnInfo = container->getColumnInfo(columnId);
	ColumnType columnType = columnInfo.getColumnType();
	if (!ValueProcessor::isNumerical(columnType) &&
		columnType != COLUMN_TYPE_TIMESTAMP) {
		GS_THROW_USER_ERROR(GS_ERROR_TQ_COLUMN_CANNOT_AGGREGATE,
			"Invalid column for aggregation");
	}

	assert(aggregateType == AGG_MAX || aggregateType == AGG_MIN);

	if (resultRowIdList.size() == 0) {
		return 0;
	}

	bool doSort = orderByExpr &&
				((container->getContainerType() != TIME_SERIES_CONTAINER) ||
				 !((*orderByExpr)[0].expr->isColumn() &&
					 (*orderByExpr)[0].expr->getColumnId() == 0 &&
					 (*orderByExpr)[0].order == ASC));
	if (doSort) {
		if (container->getContainerType() == TIME_SERIES_CONTAINER) {  
			TimeSeriesOrderByComparator comp(txn,
				*reinterpret_cast<TimeSeries *>(container), function_map,
				*orderByExpr);
			std::sort(resultRowIdList.begin(), resultRowIdList.end(), comp);
		}
		else {
			CollectionOrderByComparator comp(txn,
				*reinterpret_cast<Collection *>(container), function_map,
				*orderByExpr);
			std::sort(resultRowIdList.begin(), resultRowIdList.end(), comp);
		}
	}

	if (offset > 0 && limit != MAX_RESULT_SIZE) {
		limit += offset;
	}

	assert(ComparatorTable::comparatorTable_[columnType][columnType] != NULL);

	bool isFirst = true;
	Value maxMinVal;
	util::XArray<PointRowId> maxMinPosList(
		txn.getDefaultAllocator());  
	ObjectManagerV4 &objectManager = *(container->getObjectManager());
	AllocateStrategy& strategy = container->getRowAllocateStrategy();
	BaseContainer::RowArray rowArray(txn, container);
	for (size_t i = 0; i < resultRowIdList.size(); i++) {
		rowArray.load(txn, resultRowIdList[i], container, OBJECT_READ_ONLY);  
		BaseContainer::RowArray::Row row(rowArray.getRow(), &rowArray);  
		ContainerValue currentContainerValue(objectManager, container->getRowAllocateStrategy());
		row.getField(txn, columnInfo, currentContainerValue);
		if (currentContainerValue.getValue().isNullValue()) {
		}
		else if (isFirst) {
			isFirst = false;
			maxMinVal.copy(
				txn, objectManager, strategy, currentContainerValue.getValue());
			maxMinPosList.push_back(resultRowIdList[i]);
		}
		else {
			int32_t ret =
				ComparatorTable::comparatorTable_[columnType][columnType](txn, maxMinVal.data(),
					maxMinVal.size(), currentContainerValue.getValue().data(),
					currentContainerValue.getValue().size());
			if (ret == 0) {
				maxMinPosList.push_back(resultRowIdList[i]);
			}
			else if ((ret < 0 && aggregateType == AGG_MAX) ||
					 (ret > 0 && aggregateType == AGG_MIN)) {
				maxMinVal.copy(
					txn, objectManager, strategy, currentContainerValue.getValue());
				maxMinPosList.clear();
				maxMinPosList.push_back(resultRowIdList[i]);
			}
			else {
			}
		}
	}

	if (offset > 0 && maxMinPosList.size() > offset) {
		maxMinPosList.erase(maxMinPosList.begin(),
			maxMinPosList.begin() + static_cast<size_t>(offset));
	}
	else if (offset > 0 && maxMinPosList.size() <= offset) {
		resultType = RESULT_ROWSET;
		return 0;
	}

	if (maxMinPosList.size() > nActualLimit) {
		maxMinPosList.erase(
			maxMinPosList.begin() + static_cast<size_t>(nActualLimit),
			maxMinPosList.end());
	}

	uint64_t resultNum = maxMinPosList.size();

	for (size_t i = 0; i < maxMinPosList.size(); ++i) {
		rowArray.load(txn, maxMinPosList[i], container, OBJECT_READ_ONLY);  
		BaseContainer::RowArray::Row row(rowArray.getRow(), &rowArray);  
		if (container->getContainerType() == TIME_SERIES_CONTAINER) {  
			row.getImage(txn, messageRowStore, false);				   
		}
		else {
			row.getImage(txn, messageRowStore, true);  
		}
		messageRowStore->next();  
	}

	resultType = RESULT_ROWSET;
	return static_cast<int>(resultNum);
}

#endif
