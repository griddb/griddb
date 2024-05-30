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
#include "data_store_v4.h"
#include "data_store_common.h"
#include "query_processor.h"
#include "result_set.h"
#include "btree_map.h"
#include "gs_error.h"
#include "value_processor.h"
#include "query.h"
#include "gis_quadraticsurface.h"
#include "message_row_store.h"
#include <sstream>

/*!
	@brief Returns the content of a Row corresponding to the Row key
*/
void QueryProcessor::get(TransactionContext &txn, BaseContainer &container,
	uint32_t rowKeySize, const uint8_t *rowKey, ResultSet &resultSet) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();
	try {
		if (!container.definedRowKey()) {  
			GS_THROW_USER_ERROR(GS_ERROR_QP_ROW_KEY_UNDEFINED, "");	
		}
		util::XArray<KeyData> keyFields(alloc);
		container.getRowKeyFields(txn, rowKeySize, rowKey, keyFields);
		util::Vector<ColumnId> keyColumnIdList(alloc);
		container.getKeyColumnIdList(keyColumnIdList);

		util::XArray<OId> &idList = *resultSet.getOIdList();
		idList.clear();
		if (container.getContainerType() == COLLECTION_CONTAINER) {
			bool withPartialMatch = false;
			IndexTypes indexBit =
				container.getIndexTypes(txn, keyColumnIdList, withPartialMatch);
			if (container.hasIndex(indexBit, MAP_TYPE_BTREE)) {
				BtreeMap::SearchContext sc(alloc, keyColumnIdList);
				sc.setLimit(1);
				for (ColumnId i = 0; i < static_cast<uint32_t>(keyFields.size()); i++) {
					ColumnInfo &columnInfo = container.getColumnInfo(i);
					TermCondition cond(columnInfo.getColumnType(), columnInfo.getColumnType(), 
						DSExpression::EQ, i, keyFields[i].data_, keyFields[i].size_);
					sc.addCondition(txn, cond, true);
				}
				container.searchColumnIdIndex(txn, sc, idList, ORDER_UNDEFINED);
			}
			else {
				BtreeMap::SearchContext sc(alloc, container.getRowIdColumnId());
				sc.setLimit(1);
				for (ColumnId i = 0; i < static_cast<uint32_t>(keyFields.size()); i++) {
					ColumnInfo &columnInfo = container.getColumnInfo(i);
					TermCondition cond(columnInfo.getColumnType(), columnInfo.getColumnType(), 
						DSExpression::EQ, i, keyFields[i].data_, keyFields[i].size_);
					sc.addCondition(txn, cond, false);
				}
				container.searchRowIdIndex(txn, sc, idList, ORDER_UNDEFINED);
			}
		}
		else {
			ColumnId columnId = ColumnInfo::ROW_KEY_COLUMN_ID;
			ColumnInfo &columnInfo = container.getColumnInfo(columnId);

			Timestamp rowKeyTimestamp =
				*(reinterpret_cast<const Timestamp *>(rowKey));
			TermCondition cond(columnInfo.getColumnType(), columnInfo.getColumnType(), 
				DSExpression::EQ, columnId, &rowKeyTimestamp, sizeof(rowKeyTimestamp));
			BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, 1);
			container.searchRowIdIndex(txn, sc, idList, ORDER_ASCENDING);
		}
		if (idList.size() == 1) {
			resultSet.setResultType(RESULT_ROW_ID_SET, 1);
		}
		else if (idList.size() > 1) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_QP_HIT_COUNT_INVALID,
				"hit num is more than 1. num = " << idList.size());
		}
	}
	catch (std::exception &e) {
		container.handleSearchError(txn, e, GS_ERROR_QP_COL_GET_FAILED);
	}
}

/*!
	@brief Obtain a set of Rows from start RowId with limitation of number
*/
void QueryProcessor::get(TransactionContext &txn, BaseContainer &container,
	RowId preLast, ResultSize limit, RowId &, ResultSet &resultSet) {
	try {
		if (limit == UNDEF_RESULT_SIZE) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_QP_UNDEFINED, "undefined limit");	
		}

		util::XArray<OId> &idList = *resultSet.getOIdList();
		idList.clear();
		ColumnId columnId = container.getRowIdColumnId();
		if (preLast != UNDEF_ROWID) {
			RowId start = preLast;
			ColumnType columnType = container.getRowIdColumnType();
			TermCondition cond(columnType, columnType,
				DSExpression::GT, columnId, &start,
				sizeof(start));
			BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, limit);
			container.searchRowIdIndex(txn, sc, idList, ORDER_ASCENDING);
		}
		else {
			BtreeMap::SearchContext sc(txn.getDefaultAllocator(), columnId);
			sc.setLimit(limit);
			container.searchRowIdIndex(txn, sc, idList, ORDER_ASCENDING);
		}
		resultSet.setResultType(RESULT_ROW_ID_SET, idList.size());
	}
	catch (std::exception &e) {
		container.handleSearchError(txn, e, GS_ERROR_QP_COL_GET_FAILED);
	}
}

/*!
	@brief Obtain a set of Rows within specific ranges between the specified
   start and end times
*/
void QueryProcessor::search(TransactionContext &txn, BaseContainer &container,
	ResultSize limit, const util::XArray<uint8_t> *startKey,
	const util::XArray<uint8_t> *endKey, ResultSet &resultSet) {
	UNUSED_VARIABLE(limit);
	util::StackAllocator &alloc = txn.getDefaultAllocator();
	try {
		util::XArray<OId> &idList = *resultSet.getOIdList();
		if (!container.definedRowKey()) {  
			GS_THROW_USER_ERROR(GS_ERROR_QP_ROW_KEY_UNDEFINED, "");
		}
		util::Vector<ColumnId> keyColumnIdList(alloc);
		container.getKeyColumnIdList(keyColumnIdList);

		if (keyColumnIdList.size() > 1 && (startKey != NULL || endKey != NULL)) {
			GS_THROW_USER_ERROR(GS_ERROR_QP_ROW_KEY_INVALID, "range for composite row key is not support");
		}

		util::XArray<KeyData> startKeyFields(alloc);
		util::XArray<KeyData> endKeyFields(alloc);
		if (startKey != NULL) {
			container.getRowKeyFields(
					txn, static_cast<uint32_t>(startKey->size()),
					startKey->data(), startKeyFields);
		}
		if (endKey != NULL) {
			container.getRowKeyFields(
					txn, static_cast<uint32_t>(endKey->size()),
					endKey->data(), endKeyFields);
		}
		util::Vector<ColumnId> rowIdColumnIdList(alloc);
		rowIdColumnIdList.push_back(container.getRowIdColumnId());

		for (util::Vector<ColumnId>::iterator itr = keyColumnIdList.begin();
			itr != keyColumnIdList.end(); itr++) {
			ColumnInfo &columnInfo = container.getColumnInfo(*itr);
			if ((startKey != NULL || endKey != NULL) && 
				columnInfo.getColumnType() == COLUMN_TYPE_STRING) {
				GS_THROW_USER_ERROR(GS_ERROR_QP_ROW_KEY_INVALID, "range for string is not support");
			}
		}

		util::Vector<ColumnId> *indexColumnIdList = &rowIdColumnIdList;
		bool isUseValueIndex = false;
		bool isKeyCondition = true;
		if (container.getContainerType() == COLLECTION_CONTAINER) {
			bool withPartialMatch = false;
			if (container.hasIndex(
					txn, keyColumnIdList, MAP_TYPE_BTREE, withPartialMatch)) {
				isUseValueIndex = true;
				indexColumnIdList = &keyColumnIdList;
			} else {
				isKeyCondition = false;
			}
		} else {
		}
		BtreeMap::SearchContext sc(txn.getDefaultAllocator(), *indexColumnIdList);
		for (ColumnId i = 0; i < static_cast<uint32_t>(startKeyFields.size()); i++) {
			ColumnInfo &columnInfo = container.getColumnInfo(i);
			TermCondition cond(columnInfo.getColumnType(), columnInfo.getColumnType(), 
				DSExpression::GE, i, startKeyFields[i].data_, startKeyFields[i].size_);
			sc.addCondition(txn, cond, isKeyCondition);
		}
		for (ColumnId i = 0; i < static_cast<uint32_t>(endKeyFields.size()); i++) {
			ColumnInfo &columnInfo = container.getColumnInfo(i);
			TermCondition cond(columnInfo.getColumnType(), columnInfo.getColumnType(), 
				DSExpression::LE, i, endKeyFields[i].data_, endKeyFields[i].size_);
			sc.addCondition(txn, cond, isKeyCondition);
		}
		OutputOrder order = (keyColumnIdList.size() > 1) ? ORDER_UNDEFINED : ORDER_ASCENDING;
		if (isUseValueIndex) {
			container.searchColumnIdIndex(txn, sc, idList, order);
		} else {
			container.searchRowIdIndex(txn, sc, idList, order);
		}
		resultSet.setResultType(RESULT_ROW_ID_SET, idList.size());
	}
	catch (std::exception &e) {
		container.handleSearchError(txn, e, GS_ERROR_QP_COL_RANGE_FAILED);
	}
}


/*!
	@brief Obtain a set of Rows within a specific range between the specified
   start and end times
*/
void QueryProcessor::search(TransactionContext &txn, TimeSeries &timeSeries,
	OutputOrder order, ResultSize limit, Timestamp start, Timestamp end,
	ResultSet &resultSet) {
	try {
		util::XArray<OId> &rowIdList = *resultSet.getOIdList();
		BtreeMap::SearchContext sc (txn.getDefaultAllocator(), ColumnInfo::ROW_KEY_COLUMN_ID);

		if (start != UNDEF_TIMESTAMP) {
			if (!ValueProcessor::validateTimestamp(start)) {
				GS_THROW_USER_ERROR(GS_ERROR_QP_TIMESTAMP_RANGE_INVALID,
					"Timestamp of start out of range (start=" << start << ")");
			}
		}
		if (end != UNDEF_TIMESTAMP) {
			if (!ValueProcessor::validateTimestamp(end)) {
				GS_THROW_USER_ERROR(GS_ERROR_QP_TIMESTAMP_RANGE_INVALID,
					"Timestamp of end out of range (end=" << end << ")");
			}
		}

		Timestamp rewriteStartTime = start;
		DSExpression::Operation startOpType = DSExpression::GE;
		if (rewriteStartTime != UNDEF_TIMESTAMP) {
			TermCondition cond(timeSeries.getRowIdColumnType(), timeSeries.getRowIdColumnType(),
				startOpType, timeSeries.getRowIdColumnId(),
				reinterpret_cast<uint8_t *>(&rewriteStartTime), sizeof(rewriteStartTime));
			sc.addCondition(txn, cond, true);
		}
		if (end != UNDEF_TIMESTAMP) {
			TermCondition cond(timeSeries.getRowIdColumnType(), timeSeries.getRowIdColumnType(),
				DSExpression::LE, timeSeries.getRowIdColumnId(),
				reinterpret_cast<uint8_t *>(&end), sizeof(end));
			sc.addCondition(txn, cond, true);
		}

		sc.setLimit(limit);
		timeSeries.searchRowIdIndex(txn, sc, rowIdList, order);
		resultSet.setResultType(RESULT_ROW_ID_SET, rowIdList.size());
	}
	catch (std::exception &e) {
		timeSeries.handleSearchError(txn, e, GS_ERROR_QP_TIM_SEARCH_FAILED);
	}
}

/*!
	@brief Take a sampling of Rows within a specific range
*/
void QueryProcessor::sample(TransactionContext &txn, TimeSeries &timeSeries,
	ResultSize limit, Timestamp start, Timestamp end, const Sampling &sampling,
	ResultSet &resultSet) {
	try {
		util::XArray<uint8_t> &serializedRowList =
			*resultSet.getRowDataFixedPartBuffer();
		util::XArray<uint8_t> &serializedVarDataList =
			*resultSet.getRowDataVarPartBuffer();
		serializedRowList.clear();
		serializedVarDataList.clear();
		if (!ValueProcessor::validateTimestamp(start)) {
			GS_THROW_USER_ERROR(GS_ERROR_QP_TIMESTAMP_RANGE_INVALID,
				"Timestamp of start out of range (start=" << start << ")");
		}
		if (!ValueProcessor::validateTimestamp(end)) {
			GS_THROW_USER_ERROR(GS_ERROR_QP_TIMESTAMP_RANGE_INVALID,
				"Timestamp of end out of range (end=" << end << ")");
		}
		if (sampling.interval_ == 0 || sampling.interval_ == UINT32_MAX) {
			GS_THROW_USER_ERROR(GS_ERROR_QP_INTERVAL_INVALID, "");
		}

		ResultSize resultNum = 0;
		TermCondition startCond(timeSeries.getRowIdColumnType(), timeSeries.getRowIdColumnType(),
			DSExpression::GE, timeSeries.getRowIdColumnId(), &start,
			sizeof(start));
		TermCondition endCond(timeSeries.getRowIdColumnType(), timeSeries.getRowIdColumnType(),
			DSExpression::LE, timeSeries.getRowIdColumnId(), &end,
			sizeof(end));
		BtreeMap::SearchContext sc(txn.getDefaultAllocator(), startCond, endCond, limit);

		OutputMessageRowStore outputMessageRowStore(
			timeSeries.getDataStore()->getConfig(),
			timeSeries.getColumnInfoList(), timeSeries.getColumnNum(),
			serializedRowList, serializedVarDataList, false);
		if (sampling.mode_ == INTERP_MODE_EMPTY) {
			timeSeries.sampleWithoutInterp(
				txn, sc, sampling, resultNum, &outputMessageRowStore);
		}
		else {
			timeSeries.sample(
				txn, sc, sampling, resultNum, &outputMessageRowStore);
		}
		resultSet.setResultType(RESULT_ROWSET, resultNum);
	}
	catch (std::exception &e) {
		timeSeries.handleSearchError(txn, e, GS_ERROR_QP_TIM_SAMPLE_FAILED);
	}
}

/*!
	@brief Performs an aggregation operation on a Row set or its specific
   Columns, based on the specified start and end times
*/
void QueryProcessor::aggregate(TransactionContext &txn, TimeSeries &timeSeries,
	Timestamp start, Timestamp end, uint32_t columnId,
	AggregationType aggregationType, ResultSet &resultSet) {
	try {
		util::XArray<uint8_t> &serializedRowList =
			*resultSet.getRowDataFixedPartBuffer();
		serializedRowList.clear();
		BtreeMap::SearchContext sc (txn.getDefaultAllocator(), ColumnInfo::ROW_KEY_COLUMN_ID);
		ResultSize resultNum;

		if (!ValueProcessor::validateTimestamp(start)) {
			GS_THROW_USER_ERROR(GS_ERROR_QP_TIMESTAMP_RANGE_INVALID,
				"Timestamp of start out of range (start=" << start << ")");
		}
		if (!ValueProcessor::validateTimestamp(end)) {
			GS_THROW_USER_ERROR(GS_ERROR_QP_TIMESTAMP_RANGE_INVALID,
				"Timestamp of end out of range (end=" << end << ")");
		}

		DSExpression::Operation opType = DSExpression::GE;
		Timestamp *rewriteStartTime = &start;
		TermCondition startCond(timeSeries.getRowIdColumnType(), timeSeries.getRowIdColumnType(),
			opType, timeSeries.getRowIdColumnId(),
			rewriteStartTime, sizeof(*rewriteStartTime));
		sc.addCondition(txn, startCond, true);
		TermCondition endCond(timeSeries.getRowIdColumnType(), timeSeries.getRowIdColumnType(),
			DSExpression::LE, timeSeries.getRowIdColumnId(),
			&end, sizeof(end));
		sc.addCondition(txn, endCond, true);

		Value value;
		timeSeries.aggregate(
			txn, sc, columnId, aggregationType, resultNum, value);
		if (resultNum > 0) {
			value.serialize(serializedRowList);
		}
		resultSet.setResultType(RESULT_AGGREGATE, resultNum);
	}
	catch (std::exception &e) {
		timeSeries.handleSearchError(txn, e, GS_ERROR_QP_TIM_AGGREGATE_FAILED);
	}
}

/*!
	@brief Returns one Row related with the specified time
*/
void QueryProcessor::get(TransactionContext &txn, TimeSeries &timeSeries,
	Timestamp ts, TimeOperator timeOp, ResultSet &resultSet) {
	try {
		if (!ValueProcessor::validateTimestamp(ts)) {
			GS_THROW_USER_ERROR(GS_ERROR_QP_TIMESTAMP_RANGE_INVALID,
				"Timestamp of rowKey out of range (rowKey=" << ts << ")");
		}

		OId oId = UNDEF_OID;
		timeSeries.searchTimeOperator(txn, ts, timeOp, oId);
		resultSet.getOIdList()->clear();
		if (UNDEF_OID == oId) {
			resultSet.setResultType(RESULT_ROWSET, 0);
		}
		else {
			resultSet.getOIdList()->push_back(oId);
			resultSet.setResultType(RESULT_ROW_ID_SET, 1);
		}
	}
	catch (std::exception &e) {
		timeSeries.handleSearchError(txn, e, GS_ERROR_QP_TIM_GET_FAILED);
	}
}

/*!
	@brief Performs linear interpolation etc. of a Row object corresponding to
   the specified time
*/
void QueryProcessor::interpolate(TransactionContext &txn,
	TimeSeries &timeSeries, Timestamp ts, uint32_t columnId,
	ResultSet &resultSet) {
	try {
		util::XArray<uint8_t> &serializedRowList =
			*resultSet.getRowDataFixedPartBuffer();
		util::XArray<uint8_t> &serializedVarDataList =
			*resultSet.getRowDataVarPartBuffer();
		serializedRowList.clear();
		serializedVarDataList.clear();

		if (!ValueProcessor::validateTimestamp(ts)) {
			GS_THROW_USER_ERROR(GS_ERROR_QP_TIMESTAMP_RANGE_INVALID,
				"Timestamp of rowKey out of range (rowKey=" << ts << ")");
		}
		if (columnId >= timeSeries.getColumnNum()) {
			GS_THROW_USER_ERROR(GS_ERROR_QP_COLUMN_ID_INVALID, "");
		}

		if (columnId >= timeSeries.getColumnNum()) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_COLUMN_ID_INVALID, "");
		}

		Sampling sampling;
		ResultSize resultNum;
		TermCondition cond(timeSeries.getRowIdColumnType(), timeSeries.getRowIdColumnType(),
			DSExpression::GE, timeSeries.getRowIdColumnId(), &ts,
			sizeof(ts));
		BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, 2);

		sampling.interpolatedColumnIdList_.push_back(columnId);
		sampling.interval_ = 0;

		OutputMessageRowStore outputMessageRowStore(
			timeSeries.getDataStore()->getConfig(),
			timeSeries.getColumnInfoList(), timeSeries.getColumnNum(),
			serializedRowList, serializedVarDataList, false);
		timeSeries.sample(txn, sc, sampling, resultNum, &outputMessageRowStore);
		resultSet.setResultType(RESULT_ROWSET, resultNum);
	}
	catch (std::exception &e) {
		timeSeries.handleSearchError(
			txn, e, GS_ERROR_QP_TIM_INTERPOLATE_FAILED);
	}
}

/*!
	@brief Obtain a set of Rows from start RowId with limitation of number
*/
void QueryProcessor::get(TransactionContext &txn, TimeSeries &timeSeries,
	Timestamp preLast, ResultSize limit, Timestamp &last,
	ResultSet &resultSet) {
	try {
		util::XArray<uint8_t> &serializedRowList =
			*resultSet.getRowDataFixedPartBuffer();
		util::XArray<uint8_t> &serializedVarDataList =
			*resultSet.getRowDataVarPartBuffer();
		serializedRowList.clear();
		serializedVarDataList.clear();

		if (limit == UNDEF_RESULT_SIZE) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_QP_UNDEFINED, "undefined limit");
		}

		util::XArray<OId> &rowIdList = *resultSet.getOIdList();

		if (preLast != UNDEF_TIMESTAMP) {
			Timestamp start = preLast;
			TermCondition cond(timeSeries.getRowIdColumnType(), timeSeries.getRowIdColumnType(),
				DSExpression::GT, timeSeries.getRowIdColumnId(), &start,
				sizeof(start));
			BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, limit);
			timeSeries.searchRowIdIndex(txn, sc, rowIdList, ORDER_ASCENDING);
		}
		else {
			Timestamp startTime = MINIMUM_EXPIRED_TIMESTAMP;
			TermCondition cond(timeSeries.getRowIdColumnType(), timeSeries.getRowIdColumnType(),
				DSExpression::GT, timeSeries.getRowIdColumnId(), &startTime,
				sizeof(startTime));
			BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, limit);
			timeSeries.searchRowIdIndex(txn, sc, rowIdList, ORDER_ASCENDING);
		}
		ResultSize resultNum = 0;
		if (!rowIdList.empty()) {
			resultNum = rowIdList.size();  
		}
		else {
			resultNum = 0;
			last = UNDEF_TIMESTAMP;
		}
		resultSet.setResultType(RESULT_ROW_ID_SET, resultNum);
	}
	catch (std::exception &e) {
		timeSeries.handleSearchError(txn, e, GS_ERROR_QP_TIM_GET_FAILED);
	}
}

/*!
	@brief Returns a set of Rows as an execution result(translate into Message
   format)
*/
void QueryProcessor::fetch(TransactionContext &txn, BaseContainer &container,
	uint64_t startPos, ResultSize fetchNum, ResultSet *&resultSet,
	LogSequentialNumber lastUpdateLsn) {
	try {
		if (resultSet->isPartialExecuteMode()) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_DS_FETCH_PARAMETER_INVALID,
				"Partial mode not suppot");
		}
		if (fetchNum < 1) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_FETCH_PARAMETER_INVALID,
				"Fetch Size = " << fetchNum
								<< ". Fetch size must be greater than 1");
		}
		if (!resultSet->getTxnAllocator()) {
			resultSet->setTxnAllocator(&txn.getDefaultAllocator());
		}
		if (resultSet->getResultNum() == 0) {
			resultSet->setDataPos(0, 0, 0, 0);
			resultSet->setFetchNum(0);
			if (resultSet->getResultNum() == 0 &&
				(resultSet->getResultType() == RESULT_ROW_ID_SET ||
					resultSet->getResultType() == RESULT_NONE)) {
				resultSet->setResultType(RESULT_ROWSET, 0);
			}
			return;
		}

		if (resultSet->getResultNum() != 0 &&
			startPos >= resultSet->getResultNum()) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_FETCH_START_POS_INVALID, "");
		}
		if (resultSet->getStartLsn() < lastUpdateLsn) {
			GS_THROW_USER_ERROR(GS_ERROR_QP_FETCH_FAILED, "");
		}
		switch (resultSet->getResultType()) {
		case RESULT_AGGREGATE:
		case RESULT_EXPLAIN:
			resultSet->setDataPos(
				0, static_cast<uint32_t>(
					   resultSet->getRowDataFixedPartBuffer()->size()),
				0, static_cast<uint32_t>(
					   resultSet->getRowDataVarPartBuffer()->size()));
			resultSet->setFetchNum(resultSet->getResultNum());
			break;
		default: {
			bool isRowIdIncluded = resultSet->isRowIdInclude();
			if (container.getContainerType() == TIME_SERIES_CONTAINER) {  
				isRowIdIncluded = false;
				resultSet->setRowIdInclude(false);
			}
			else {
				isRowIdIncluded = true;
				resultSet->setRowIdInclude(true);
			}
			if (resultSet->hasRowId()) {
				if (resultSet->getResultType() == PARTIAL_RESULT_ROWSET) {
					if (resultSet->getSchemaVersionId() !=
						container.getVersionId()) {
						GS_THROW_USER_ERROR(GS_ERROR_QP_FETCH_FAILED,
							"Schema version check failed.");
					}
					uint64_t skipped = resultSet->getSkipCount();
					container.getOIdList(txn, startPos, fetchNum, skipped,
						*resultSet->getRowIdList(), *resultSet->getOIdList());
					resultSet->setSkipCount(skipped);
					startPos += skipped;
					fetchNum = resultSet->getOIdList()->size();
					assert(startPos <= resultSet->getResultNum());
				}

				resultSet->resetSerializedData();
				{
					size_t reserveSize =
						(resultSet->getOIdList()->size() - startPos > fetchNum)
							? fetchNum
							: resultSet->getOIdList()->size() - startPos;
					resultSet->getRowDataFixedPartBuffer()->reserve(
						reserveSize * container.getRowFixedDataSize());
					if (container.getVariableColumnNum() > 0) {
						resultSet->getRowDataVarPartBuffer()->reserve(
							reserveSize * container.getVariableColumnNum() * 8);
					}
				}
				OutputMessageRowStore outputMessageRowStore(
					container.getDataStore()->getConfig(),
					container.getColumnInfoList(), container.getColumnNum(),
					*(resultSet->getRowDataFixedPartBuffer()),
					*(resultSet->getRowDataVarPartBuffer()), isRowIdIncluded);
				ResultSize resultNum;
				container.getRowList(txn, *(resultSet->getOIdList()), fetchNum,
					resultNum, &outputMessageRowStore, isRowIdIncluded, 0);
				resultSet->setDataPos(
					0, static_cast<uint32_t>(
						   resultSet->getRowDataFixedPartBuffer()->size()),
					0, static_cast<uint32_t>(
						   resultSet->getRowDataVarPartBuffer()->size()));
				resultSet->setResultType(
					RESULT_ROWSET, resultSet->getResultNum(), isRowIdIncluded);

				if (startPos + fetchNum >= resultSet->getResultNum()) {
					resultSet->setFetchNum(
						resultSet->getResultNum() - startPos);
					resultSet->setResultType(RESULT_ROWSET);
				}
				else {
					resultSet->setFetchNum(fetchNum);
					resultSet->setResultType(PARTIAL_RESULT_ROWSET);

					util::XArray<RowId> &rowIdList = *resultSet->getRowIdList();
					if (rowIdList.size() == 0) {
						container.getRowIdList(
							txn, *resultSet->getOIdList(), rowIdList);
						resultSet->getOIdList()->clear();
					}
				}
			}
			else {
				util::XArray<uint8_t> *serializedFixedDataList =
					resultSet->getRowDataFixedPartBuffer();
				util::XArray<uint8_t> *serializedVarDataList =
					resultSet->getRowDataVarPartBuffer();
				if (resultSet->useRSRowImage()) {
					serializedFixedDataList =
						resultSet->getRSRowDataFixedPartBuffer();
					serializedVarDataList =
						resultSet->getRSRowDataVarPartBuffer();
				}
				bool isValidate = false;
				InputMessageRowStore inputMessageRowStore(
					container.getDataStore()->getConfig(),
					container.getColumnInfoList(), container.getColumnNum(),
					serializedFixedDataList->data(),
					static_cast<uint32_t>(serializedFixedDataList->size()),
					serializedVarDataList->data(),
					static_cast<uint32_t>(serializedVarDataList->size()),
					resultSet->getResultNum(), isRowIdIncluded, isValidate);

				uint64_t startFixedOffset, fixedOffsetSize, startVarOffset,
					varOffsetSize;
				inputMessageRowStore.getPartialRowSet(startPos, fetchNum,
					startFixedOffset, fixedOffsetSize, startVarOffset,
					varOffsetSize);
				resultSet->setDataPos(startFixedOffset, fixedOffsetSize,
					startVarOffset, varOffsetSize);

				if (startPos + fetchNum >= resultSet->getResultNum()) {
					resultSet->setFetchNum(
						resultSet->getResultNum() - startPos);
					resultSet->setResultType(RESULT_ROWSET);
				}
				else {
					resultSet->setFetchNum(fetchNum);
					resultSet->setResultType(PARTIAL_RESULT_ROWSET);

					util::XArray<uint8_t> &keepSerializedRowList =
						*resultSet->getRSRowDataFixedPartBuffer();
					util::XArray<uint8_t> &keepSerializedVarDataList =
						*resultSet->getRSRowDataVarPartBuffer();
					if (keepSerializedRowList.size() == 0) {
						keepSerializedRowList.reserve(
							resultSet->getRowDataFixedPartBuffer()->size());
						keepSerializedRowList.push_back(
							resultSet->getRowDataFixedPartBuffer()->data(),
							resultSet->getRowDataFixedPartBuffer()->size());
						keepSerializedVarDataList.reserve(
							resultSet->getRowDataVarPartBuffer()->size());
						keepSerializedVarDataList.push_back(
							resultSet->getRowDataVarPartBuffer()->data(),
							resultSet->getRowDataVarPartBuffer()->size());
						resultSet->setUseRSRowImage(true);
					}
				}
			}
		}
		}
	}
	catch (std::exception &e) {
		container.handleSearchError(txn, e, GS_ERROR_QP_FETCH_FAILED);
	}
}

