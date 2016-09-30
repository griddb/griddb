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
	@brief Implementation of QueryProcessor
*/
#include "data_store.h"
#include "data_store_common.h"
#include "query_processor.h"
#include "result_set.h"
#include "transaction_manager.h"
#include "btree_map.h"
#include "gs_error.h"
#include "value_processor.h"
#include <sstream>

/*!
	@brief Returns the content of a Row corresponding to the Row key
*/
void QueryProcessor::get(TransactionContext &txn, BaseContainer &container,
	uint32_t rowKeySize, const uint8_t *rowKey, ResultSet &resultSet) {
	try {
		if (!container.definedRowKey()) {  
			GS_THROW_USER_ERROR(GS_ERROR_QP_ROW_KEY_UNDEFINED, "");
		}
		ColumnInfo &keyColumnInfo =
			container.getColumnInfo(ColumnInfo::ROW_KEY_COLUMN_ID);

		if (keyColumnInfo.getColumnType() == COLUMN_TYPE_STRING) {
			StringCursor stringCusor(const_cast<uint8_t *>(rowKey));
			rowKey = stringCusor.str();
			rowKeySize = stringCusor.stringLength();
			if (rowKeySize >
				container.getDataStore()
					->getValueLimitConfig()
					.getLimitSmallSize()) {  
				GS_THROW_USER_ERROR(GS_ERROR_QP_ROW_KEY_INVALID, "");
			}
		}
		else if (keyColumnInfo.getColumnType() == COLUMN_TYPE_TIMESTAMP) {
			if (!ValueProcessor::validateTimestamp(
					*reinterpret_cast<const Timestamp *>(rowKey))) {
				GS_THROW_USER_ERROR(GS_ERROR_QP_TIMESTAMP_RANGE_INVALID,
					"Timestamp of rowKey out of range (rowKey=" << rowKey
																<< ")");
			}
		}

		util::XArray<OId> &idList = *resultSet.getOIdList();
		idList.clear();
		if (container.getContainerType() == COLLECTION_CONTAINER) {
			IndexTypes indexBit =
				container.getIndexTypes(txn, ColumnInfo::ROW_KEY_COLUMN_ID);
			if (container.hasIndex(indexBit, MAP_TYPE_HASH)) {
				HashMap::SearchContext sc(ColumnInfo::ROW_KEY_COLUMN_ID, rowKey,
					rowKeySize, 0, NULL, MAX_RESULT_SIZE);
				container.searchColumnIdIndex(txn, sc, idList);
			}
			else if (container.hasIndex(indexBit, MAP_TYPE_BTREE)) {
				BtreeMap::SearchContext sc(ColumnInfo::ROW_KEY_COLUMN_ID,
					rowKey, rowKeySize, 0, NULL, 1);
				container.searchColumnIdIndex(txn, sc, idList, ORDER_UNDEFINED);
			}
			else {
				ColumnType type = keyColumnInfo.getColumnType();

				TermCondition cond(eqTable[type][type],
					ColumnInfo::ROW_KEY_COLUMN_ID,
					keyColumnInfo.getColumnOffset(), rowKey, rowKeySize);
				BtreeMap::SearchContext sc(
					0, NULL, 0, true, NULL, 0, true, 1, &cond, MAX_RESULT_SIZE);
				container.searchRowIdIndex(txn, sc, idList, ORDER_UNDEFINED);
			}
		}
		else {
			Timestamp rowKeyTimestamp =
				*(reinterpret_cast<const Timestamp *>(rowKey));
			BtreeMap::SearchContext sc(
				ColumnInfo::ROW_KEY_COLUMN_ID, &rowKeyTimestamp, 0, 0, NULL, 1);
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

		if (preLast != UNDEF_ROWID) {
			RowId start = preLast;
			BtreeMap::SearchContext sc(
				0, &start, 0, false, NULL, 0, true, 0, NULL, limit);
			container.searchRowIdIndex(txn, sc, idList, ORDER_ASCENDING);
		}
		else {
			BtreeMap::SearchContext sc(
				0, NULL, 0, true, NULL, 0, true, 0, NULL, limit);
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
	try {
		util::XArray<OId> &idList = *resultSet.getOIdList();
		if (!container.definedRowKey()) {  
			GS_THROW_USER_ERROR(GS_ERROR_QP_ROW_KEY_UNDEFINED, "");
		}

		ColumnInfo &keyColumnInfo =
			container.getColumnInfo(ColumnInfo::ROW_KEY_COLUMN_ID);
		if (keyColumnInfo.getColumnType() == COLUMN_TYPE_STRING &&
			(startKey != NULL || endKey != NULL)) {
			GS_THROW_USER_ERROR(GS_ERROR_QP_ROW_KEY_INVALID, "");
		}

		if (keyColumnInfo.getColumnType() == COLUMN_TYPE_TIMESTAMP) {
			if (startKey != NULL &&
				!ValueProcessor::validateTimestamp(
					*reinterpret_cast<const Timestamp *>(startKey->data()))) {
				GS_THROW_USER_ERROR(GS_ERROR_QP_ROW_KEY_INVALID, "");
			}
			if (endKey != NULL &&
				!ValueProcessor::validateTimestamp(
					*reinterpret_cast<const Timestamp *>(endKey->data()))) {
				GS_THROW_USER_ERROR(GS_ERROR_QP_ROW_KEY_INVALID, "");
			}
		}

		const util::XArray<uint8_t> *keyList[] = {startKey, endKey};
		const size_t keyCount = sizeof(keyList) / sizeof(*keyList);

		const uint8_t *keyData[keyCount];
		uint32_t keySize[keyCount];
		bool keyIncluded[keyCount];

		for (size_t i = 0; i < keyCount; i++) {
			const util::XArray<uint8_t> *key = keyList[i];
			if (key == NULL) {
				keyData[i] = NULL;
				keySize[i] = 0;
				keyIncluded[i] = false;
			}
			else {
				keyData[i] = key->data();
				keySize[i] = static_cast<uint32_t>(key->size());
				keyIncluded[i] = true;
			}
		}

		if (container.getContainerType() == COLLECTION_CONTAINER) {
			if (container.hasIndex(
					txn, keyColumnInfo.getColumnId(), MAP_TYPE_BTREE)) {
				BtreeMap::SearchContext sc(ColumnInfo::ROW_KEY_COLUMN_ID,
					keyData[0], keySize[0], keyIncluded[0], keyData[1],
					keySize[1], keyIncluded[1], 0, NULL, limit);
				container.searchColumnIdIndex(txn, sc, idList, ORDER_ASCENDING);
			}
			else {
				TermCondition condList[keyCount];
				uint32_t condCount = 0;

				const ColumnType type = keyColumnInfo.getColumnType();
				const Operator operatorList[] = {
					geTable[type][type], leTable[type][type]};

				for (size_t i = 0; i < keyCount; i++) {
					if (keyIncluded[i]) {
						condList[condCount] = TermCondition(operatorList[i],
							ColumnInfo::ROW_KEY_COLUMN_ID,
							keyColumnInfo.getColumnOffset(), keyData[i],
							keySize[i]);
						condCount++;
					}
				}

				BtreeMap::SearchContext sc(ColumnInfo::ROW_KEY_COLUMN_ID, NULL,
					0, false, NULL, 0, false, condCount, condList, limit);
				container.searchRowIdIndex(txn, sc, idList, ORDER_ASCENDING);
			}
		}
		else {
			BtreeMap::SearchContext sc(ColumnInfo::ROW_KEY_COLUMN_ID,
				keyData[0], keySize[0], keyIncluded[0], keyData[1], keySize[1],
				keyIncluded[1], 0, NULL, limit);
			container.searchRowIdIndex(txn, sc, idList, ORDER_ASCENDING);
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
		BtreeMap::SearchContext sc;

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

		Timestamp expiredTime =
			timeSeries.getCurrentExpiredTime(txn);  

		if (expiredTime > end) {
			resultSet.setResultNum(0);
			return;
		}

		sc.columnId_ = ColumnInfo::ROW_KEY_COLUMN_ID;
		if (start == UNDEF_TIMESTAMP) {
			sc.startKey_ = &expiredTime;
			sc.isStartKeyIncluded_ = false;
		}
		else {
			if (expiredTime > start) {
				sc.startKey_ = &expiredTime;
				sc.isStartKeyIncluded_ = false;
			}
			else {
				sc.startKey_ = &start;
				sc.isStartKeyIncluded_ = true;
			}
		}
		if (end == UNDEF_TIMESTAMP) {
			sc.endKey_ = 0;
		}
		else {
			sc.endKey_ = &end;
			sc.isEndKeyIncluded_ = true;
		}

		sc.limit_ = limit;
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

		Timestamp expiredTime = timeSeries.getCurrentExpiredTime(txn);
		ResultSize resultNum = 0;
		if (expiredTime > end) {
			resultSet.setResultType(RESULT_ROWSET, 0);
			return;
		}


		BtreeMap::SearchContext sc(ColumnInfo::ROW_KEY_COLUMN_ID, &start, 0,
			true, &end, 0, true, 0, NULL, limit);
		OutputMessageRowStore outputMessageRowStore(
			timeSeries.getDataStore()->getValueLimitConfig(),
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
		BtreeMap::SearchContext sc;
		ResultSize resultNum;

		if (!ValueProcessor::validateTimestamp(start)) {
			GS_THROW_USER_ERROR(GS_ERROR_QP_TIMESTAMP_RANGE_INVALID,
				"Timestamp of start out of range (start=" << start << ")");
		}
		if (!ValueProcessor::validateTimestamp(end)) {
			GS_THROW_USER_ERROR(GS_ERROR_QP_TIMESTAMP_RANGE_INVALID,
				"Timestamp of end out of range (end=" << end << ")");
		}

		Timestamp expiredTime =
			timeSeries.getCurrentExpiredTime(txn);  

		if (expiredTime > end) {
			if (aggregationType == AGG_COUNT) {
				Value value;
				int64_t count = 0;
				value.set(count);
				value.serialize(serializedRowList);
				resultNum = 1;
			}
			else {
				resultNum = 0;
			}
			resultSet.setResultType(RESULT_AGGREGATE, resultNum);
			return;
		}

		sc.columnId_ = ColumnInfo::ROW_KEY_COLUMN_ID;
		if (expiredTime > start) {
			sc.startKey_ = &expiredTime;
			sc.isStartKeyIncluded_ = false;
		}
		else {
			sc.startKey_ = &start;
			sc.isStartKeyIncluded_ = true;
		}
		sc.endKey_ = &end;
		sc.isEndKeyIncluded_ = true;

		timeSeries.aggregate(
			txn, sc, columnId, aggregationType, resultNum, serializedRowList);
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

		Timestamp expiredTime = timeSeries.getCurrentExpiredTime(txn);
		if (expiredTime > ts) {  
			return;
		}
		if (columnId >= timeSeries.getColumnNum()) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_COLUMN_ID_INVALID, "");
		}

		Sampling sampling;
		ResultSize resultNum;
		BtreeMap::SearchContext sc(ColumnInfo::ROW_KEY_COLUMN_ID, &ts, 0, true,
			NULL, 0, true, 0, NULL, 2);

		sampling.interpolatedColumnIdList_.push_back(columnId);
		sampling.interval_ = 0;

		OutputMessageRowStore outputMessageRowStore(
			timeSeries.getDataStore()->getValueLimitConfig(),
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

		Timestamp expiredTime =
			timeSeries.getCurrentExpiredTime(txn);  
		if (preLast != UNDEF_TIMESTAMP) {
			Timestamp start = preLast;
			if (expiredTime > start) {
				start = expiredTime;
			}
			BtreeMap::SearchContext sc(ColumnInfo::ROW_KEY_COLUMN_ID, &start, 0,
				false, NULL, 0, true, 0, NULL, limit);
			timeSeries.searchRowIdIndex(txn, sc, rowIdList, ORDER_ASCENDING);
		}
		else {
			BtreeMap::SearchContext sc(ColumnInfo::ROW_KEY_COLUMN_ID,
				&expiredTime, 0, false, NULL, 0, true, 0, NULL, limit);
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
#if defined(PARTIAL_EXE_SERVER_UNIT_TEST_MODE) && \
	defined(GD_ENABLE_NEWSQL_SERVER)
				ResultSize resultNum;
				if (resultSet->isPartialExecuteMode()) {
					if (!resultSet->isPartialExecuteSuspend()) {
						OutputMessageRowStore outputMessageRowStore(
							container.getDataStore()->getValueLimitConfig(),
							container.getColumnInfoList(),
							container.getColumnNum(),
							*(resultSet->getRowDataFixedPartBuffer()),
							*(resultSet->getRowDataVarPartBuffer()),
							isRowIdIncluded);

						util::XArray<OId> tmpList(txn.getDefaultAllocator());
						uint64_t skipped = 0;
						if (resultSet->getRowIdList()->size() == 0) {
							tmpList.insert(tmpList.end(),
								resultSet->getOIdList()->begin(),
								resultSet->getOIdList()->end());
						}
						else {
							container.getOIdList(txn, 0, MAX_RESULT_SIZE,
								skipped, *resultSet->getRowIdList(), tmpList);
						}
						container.getRowList(txn, tmpList, MAX_RESULT_SIZE,
							resultNum, &outputMessageRowStore, isRowIdIncluded,
							0);
						resultSet->setResultNum(tmpList.size());
					}
					else {
					}
				}
				else {
					OutputMessageRowStore outputMessageRowStore(
						container.getDataStore()->getValueLimitConfig(),
						container.getColumnInfoList(), container.getColumnNum(),
						*(resultSet->getRowDataFixedPartBuffer()),
						*(resultSet->getRowDataVarPartBuffer()),
						isRowIdIncluded);
					container.getRowList(txn, *(resultSet->getOIdList()),
						fetchNum, resultNum, &outputMessageRowStore,
						isRowIdIncluded, 0);
				}
#else
				OutputMessageRowStore outputMessageRowStore(
					container.getDataStore()->getValueLimitConfig(),
					container.getColumnInfoList(), container.getColumnNum(),
					*(resultSet->getRowDataFixedPartBuffer()),
					*(resultSet->getRowDataVarPartBuffer()), isRowIdIncluded);
				ResultSize resultNum;
				container.getRowList(txn, *(resultSet->getOIdList()), fetchNum,
					resultNum, &outputMessageRowStore, isRowIdIncluded, 0);
#endif
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
				InputMessageRowStore inputMessageRowStore(
					container.getDataStore()->getValueLimitConfig(),
					container.getColumnInfoList(), container.getColumnNum(),
					serializedFixedDataList->data(),
					static_cast<uint32_t>(serializedFixedDataList->size()),
					serializedVarDataList->data(),
					static_cast<uint32_t>(serializedVarDataList->size()),
					resultSet->getResultNum(), isRowIdIncluded);

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
