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
	@brief Definition of TimeSeries
*/
#ifndef TIME_SERIES_H_
#define TIME_SERIES_H_

#include "base_container.h"

UTIL_TRACER_DECLARE(TIME_SERIES);



/*!
	@brief TimeSeries
*/
class TimeSeries : public BaseContainer {
	friend class StoreV5Impl;	
protected:  
	/*!
		@brief TimeSeries format
	*/
	struct TimeSeriesImage : BaseContainerImage {
		uint64_t reserved_;  
		uint64_t padding1_;  
		uint64_t padding2_;  
	};

public:  
public:  
	TimeSeries(TransactionContext &txn, DataStoreV4 *dataStore, OId oId)
		: BaseContainer(txn, dataStore, oId) {
		rowFixedDataSize_ = calcRowFixedDataSize();
		rowImageSize_ = calcRowImageSize(rowFixedDataSize_);
		setAllocateStrategy(dataStore->getObjectManager());
		util::StackAllocator &alloc = txn.getDefaultAllocator();
		util::Vector<ColumnId> columnIds(alloc);
		columnIds.push_back(getRowIdColumnId());
		rowIdFuncInfo_ = ALLOC_NEW(alloc) TreeFuncInfo(alloc);
		rowIdFuncInfo_->initialize(columnIds, NULL);

		columnIds[0] = UNDEF_COLUMNID;
		mvccFuncInfo_ = ALLOC_NEW(alloc) TreeFuncInfo(alloc);
		mvccFuncInfo_->initialize(columnIds, NULL);
	}
	TimeSeries(TransactionContext &txn, DataStoreV4 *dataStore)
		: BaseContainer(txn, dataStore) {}

	~TimeSeries() {}
	void initialize(TransactionContext &txn);
	bool finalize(TransactionContext &txn, bool isRemoveGroup);
	void set(TransactionContext & txn, const FullContainerKey & containerKey,
		ContainerId containerId, OId columnSchemaOId,
		MessageSchema * containerSchema, DSGroupId groupId);

	void createIndex(
		TransactionContext &txn, const IndexInfo &indexInfo,
		IndexCursor& indexCursor,
		bool isIndexNameCaseSensitive = false,
		CreateDropIndexMode mode = INDEX_MODE_NOSQL,
		bool *skippedByMode = NULL);
	void continueCreateIndex(TransactionContext& txn, 
		IndexCursor& indexCursor);

	void dropIndex(
		TransactionContext &txn, IndexInfo &indexInfo,
		bool isIndexNameCaseSensitive = false,
		CreateDropIndexMode mode = INDEX_MODE_NOSQL,
		bool *skippedByMode = NULL);

	void putRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowData, RowId &rowId, bool rowIdSpecified,
		PutStatus &status, PutRowOption putRowOption);

	void appendRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowDataWOTimestamp, Timestamp &rowKey,
		PutStatus &status);
	void deleteRow(TransactionContext &txn, uint32_t rowKeySize,
		const uint8_t *rowKey, RowId &rowId, bool &existing);
	/*!
		@brief Deletes a Row corresponding to the specified RowId
	*/
	void deleteRow(TransactionContext &txn, RowId rowId, bool &existing) {
		bool isForceLock = false;
		deleteRow(txn, rowId, existing, isForceLock);
	}
	/*!
		@brief Updates a Row corresponding to the specified RowId
	*/
	void updateRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowData, RowId rowId, PutStatus &status) {
		bool isForceLock = false;
		updateRow(txn, rowSize, rowData, rowId, status, isForceLock);
	}
	/*!
		@brief Deletes a Row corresponding to the specified RowId at recovery
	   phase
	*/
	void redoDeleteRow(TransactionContext &txn, RowId rowId, bool &existing) {
		bool isForceLock = true;
		deleteRow(txn, rowId, existing, isForceLock);
	}
	void abort(TransactionContext &txn);
	void commit(TransactionContext &txn);

	void searchRowIdIndex(TransactionContext &txn, BtreeMap::SearchContext &sc,
		util::XArray<OId> &resultList, OutputOrder order);
	void searchRowIdIndex(TransactionContext &txn, uint64_t start,
		uint64_t limit, util::XArray<RowId> &rowIdList,
		util::XArray<OId> &resultList, uint64_t &skipped);
	void searchRowIdIndexAsRowArray(
			TransactionContext& txn, BtreeMap::SearchContext &sc,
			util::XArray<OId> &oIdList, util::XArray<OId> &mvccOIdList);
	void scanRowIdIndex(
			TransactionContext& txn, BtreeMap::SearchContext &sc,
			OutputOrder order, ContainerRowScanner &scanner);
	void scanRowArray(
			TransactionContext& txn, const OId *begin, const OId *end,
			bool onMvcc, ContainerRowScanner &scanner);
	void scanRow(
			TransactionContext& txn, const OId *begin, const OId *end,
			bool onMvcc, ContainerRowScanner &scanner);
	RowId getMaxRowId(TransactionContext &txn);

	void searchColumnIdIndex(
			TransactionContext &txn, BtreeMap::SearchContext &sc,
			util::XArray<OId> &resultList, OutputOrder order,
			bool neverOrdering = false);
	void searchColumnIdIndex(
			TransactionContext &txn, BtreeMap::SearchContext &sc,
			util::XArray<OId> &normalRowList, util::XArray<OId> &mvccRowList);

	void aggregate(TransactionContext &txn, BtreeMap::SearchContext &sc,
		uint32_t columnId, AggregationType type, ResultSize &resultNum,
		Value &value);  

	void aggregateByTimeWindow(TransactionContext &txn,
		uint32_t columnId, AggregationType type, 
		Timestamp startTime, Timestamp endTime, const Sampling &sampling, 
		util::XArray<OId> &oIdList, ResultSize &resultNum,
		MessageRowStore *messageRowStore);

	void sample(TransactionContext &txn, BtreeMap::SearchContext &sc,
		const Sampling &sampling, ResultSize &resultNum,
		MessageRowStore *messageRowStore);  
	void sampleWithoutInterp(TransactionContext &txn,
		BtreeMap::SearchContext &sc, const Sampling &sampling,
		ResultSize &resultNum,
		MessageRowStore *messageRowStore);  
	void searchTimeOperator(
		TransactionContext &txn, Timestamp ts, TimeOperator timeOp, OId &oId);
	void searchTimeOperator(
		TransactionContext &txn, BtreeMap::SearchContext &sc, 
		Timestamp ts, TimeOperator timeOp, OId &oId);

	void lockRowList(TransactionContext &txn, util::XArray<RowId> &rowIdList);

	ColumnId getRowIdColumnId() {
		return ColumnInfo::ROW_KEY_COLUMN_ID;
	}
	ColumnType getRowIdColumnType() {
		return COLUMN_TYPE_TIMESTAMP;
	}

	static const IndexMapTable& getIndexMapTable();

protected:  
protected:  
	void putRowInternal(TransactionContext &txn,
		InputMessageRowStore *inputMessageRowStore, RowId &rowId,
		bool rowIdSpecified,
		PutStatus &status, PutRowOption putRowOption);

private:  
	struct OpForSample {
		bool isInterpolated_;
		Calculator add_;
		Calculator sub_;
		Calculator mul_;
	};
	/*!
		@brief Status of RowArray, when row is deleted
	*/
	enum DeleteStatus {
		DELETE_SIMPLE,	 
		DELETE_MERGE,	  
		DELETE_ROW_ARRAY,  
	};

private:  

	void deleteRow(
		TransactionContext &txn, RowId rowId, bool &existing, bool isForceLock);
	void updateRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowData, RowId rowId, PutStatus &status,
		bool isForceLock);
	void appendRowInternal(TransactionContext &txn,
		MessageRowStore *messageRowStore, RowArray &rowArray, RowId &rowId);
	void deleteRowInternal(TransactionContext &txn, Timestamp rowKey,
		bool &existing, bool isForceLock);
	void updateRowInternal(TransactionContext &txn,
		MessageRowStore *messageRowStore, RowArray &rowArray, RowId &rowId);
	void insertRowInternal(TransactionContext &txn,
		MessageRowStore *messageRowStore, RowArray &destRowArray, RowId &rowId);
	void shift(TransactionContext &txn, RowArray &rowArray, bool isForce);
	void split(TransactionContext &txn, RowArray &rowArray, RowId insertRowId,
		RowArray &splitRowArray, RowId &splitRowId);
	void merge(
		TransactionContext &txn, RowArray &rowArray, RowArray &nextRowArray);
	void createMinimumRowArray(
		TransactionContext &txn, RowArray &rowArray, bool isExistEmptyRowArray);

	void abortInternal(TransactionContext &txn, TransactionId tId);
	void undoCreateRow(TransactionContext &txn, RowArray &rowArray);
	void undoUpdateRow(TransactionContext &txn, RowArray &beforeRowArray);

	template<bool Mvcc> static void scanRowArrayCheckedDirect(
			TransactionContext& txn, RowArray &rowArray, OutputOrder order,
			RowId startRowId, RowId endRowId, util::XArray<OId> &oIdList);
	template<bool Mvcc> static void scanRowCheckedDirect(
			TransactionContext& txn, RowArray &rowArray,
			RowId startRowId, RowId endRowId, util::XArray<OId> &oIdList);
	template<bool Mvcc> static bool filterActiveTransaction(
			TransactionContext& txn, RowArray::Row &row);
	static RowId getScanStartRowId();

	bool scanRowArrayChecked(
			TransactionContext& txn, RowArray *rowArray,
			const BtreeMap::SearchContext &sc, OutputOrder order,
			RowId startRowId, RowId endRowId, RowId lastCheckRowId,
			RowId &lastMergedRowId, size_t &restRowOIdCount,
			util::XArray< std::pair<RowId, OId> > &rowOIdList,
			util::XArray<OId> &mvccOIdList, util::XArray<OId> &mergedOIdList);
	void mergeScannedRowArray(
			TransactionContext& txn, OutputOrder order,
			util::XArray< std::pair<RowId, OId> > &rowOIdList,
			util::XArray<OId> &mvccOIdList, util::XArray<OId> &mergedOIdList);

	void scanRowIdIndexPrepare(
			TransactionContext& txn, BtreeMap::SearchContext &sc,
			OutputOrder order, util::XArray<OId> *oIdList);
	static void searchMvccMapPrepare(
			TransactionContext& txn, const BtreeMap::SearchContext &sc,
			BtreeMap::SearchContext &mvccSC, OutputOrder order,
			RowId startRowId, RowId endRowId,
			RowId lastCheckRowId, RowId lastMergedRowId,
			std::pair<RowId, RowId> *mvccRowIdRange);

	void getIdList(TransactionContext &txn,
		util::XArray<uint8_t> &serializedRowList, util::XArray<RowId> &idList);
	void lockIdList(TransactionContext &txn, util::XArray<OId> &oIdList,
		util::XArray<RowId> &idList);
	void setDummyMvccImage(TransactionContext &txn);
	void getContainerOptionInfo(
		TransactionContext &txn, util::XArray<uint8_t> &containerSchema);
	void checkContainerOption(MessageSchema *messageSchema,
		util::XArray<uint32_t> &copyColumnMap, bool &isCompletelySameSchema);
	util::String getBibContainerOptionInfo(TransactionContext &txn);

	uint32_t calcRowImageSize(uint32_t rowFixedSize) {
		uint32_t rowImageSize_ = sizeof(RowHeader) + rowFixedSize;
		return rowImageSize_;
	}
	uint32_t calcRowFixedDataSize() {
		uint32_t rowFixedDataSize =
			ValueProcessor::calcNullsByteSize(columnSchema_->getColumnNum()) +
			columnSchema_->getRowFixedColumnSize();
		if (columnSchema_->getVariableColumnNum()) {
			rowFixedDataSize += sizeof(OId);
		}
		return rowFixedDataSize;
	}

	void searchRowArrayList(TransactionContext &txn,
		BtreeMap::SearchContext &sc, util::XArray<OId> &normalOIdList,
		util::XArray<OId> &mvccOIdList);

private:  

	static const bool INDEX_MAP_TABLE[][MAP_TYPE_NUM];
};

/*!
	@brief Cursor for aggregate values
*/
struct AggregationCursor {

	Operator operator_;		   
	Calculator calculator1_;  
	Calculator calculator2_;  
	int32_t count_;			   
	Value value1_;
	Value value2_;

	AggregationType aggType_;
	ColumnType type_;

	AggregationCursor() : operator_(NULL), calculator1_(NULL), 
		calculator2_(NULL), count_(0), aggType_(AGG_UNSUPPORTED_TYPE),
		type_(COLUMN_TYPE_ANY) {
	}
	void set(AggregationType aggType, ColumnType type) {
		count_ = 0;
		aggType_ = aggType;
		type_ = type;
		switch (aggType) {
		case AGG_MIN:
			operator_ = ComparatorTable::Lt()(type, type);
			break;
		case AGG_MAX:
			operator_ = ComparatorTable::Gt()(type, type);
			break;
		case AGG_SUM:
			calculator1_ = CalculatorTable::Add()(type, type);
			break;
		case AGG_AVG:
			calculator1_ = CalculatorTable::Add()(type, type);
			break;
		case AGG_COUNT:
			break;
		case AGG_VARIANCE:
			calculator1_ = CalculatorTable::Add()(type, type);
			calculator2_ = CalculatorTable::Mul()(type, type);
			break;
		case AGG_STDDEV:
			calculator1_ = CalculatorTable::Add()(type, type);
			calculator2_ = CalculatorTable::Mul()(type, type);
			break;
		case AGG_TIME_AVG:
		default:
			break;
		}
	}
};

typedef void (*AggregatorForLoop)(
	TransactionContext &txn, const Value &value, AggregationCursor &cursor);

/*!
	@brief Execute min function for a Row
*/
static void minForLoop(
	TransactionContext &txn, const Value &value, AggregationCursor &cursor) {
	if (cursor.count_ == 0) {
		cursor.value1_ = value;
	}
	else {
		if (cursor.operator_(txn, value.data(), value.size(),
				cursor.value1_.data(), cursor.value1_.size())) {
			cursor.value1_ = value;
		}
	}

	cursor.count_++;
}
/*!
	@brief Execute max function for a Row
*/
static void maxForLoop(
	TransactionContext &txn, const Value &value, AggregationCursor &cursor) {
	if (cursor.count_ == 0) {
		cursor.value1_ = value;
	}
	else {
		if (cursor.operator_(txn, value.data(), value.size(),
				cursor.value1_.data(), cursor.value1_.size())) {
			cursor.value1_ = value;
		}
	}

	cursor.count_++;
}
/*!
	@brief Execute sum function for a Row
*/
static void sumForLoop(
	TransactionContext &txn, const Value &value, AggregationCursor &cursor) {
	if (cursor.count_ == 0) {
		cursor.value1_ = value;
	}
	else {
		cursor.calculator1_(txn, value.data(), value.size(),
			cursor.value1_.data(), cursor.value1_.size(), cursor.value1_);
	}

	cursor.count_++;
}
/*!
	@brief Execute avg function for a Row
*/
static void avgForLoop(
	TransactionContext &txn, const Value &value, AggregationCursor &cursor) {
	if (cursor.count_ == 0) {
		cursor.value1_ = value;
	}
	else {
		cursor.calculator1_(txn, value.data(), value.size(),
			cursor.value1_.data(), cursor.value1_.size(), cursor.value1_);
	}

	cursor.count_++;
}
/*!
	@brief Execute count function for a Row
*/
static void countForLoop(
	TransactionContext &, const Value &, AggregationCursor &cursor) {
	cursor.count_++;
}

/*!
	@brief Execute variance function for a Row
*/
static void varianceForLoop(
	TransactionContext &txn, const Value &value, AggregationCursor &cursor) {
	if (cursor.count_ == 0) {
		cursor.value1_ = value;
	}
	else {
		cursor.calculator1_(txn, value.data(), value.size(),
			cursor.value1_.data(), cursor.value1_.size(), cursor.value1_);
	}

	if (cursor.count_ == 0) {
		cursor.calculator2_(txn, value.data(), value.size(), value.data(),
			value.size(), cursor.value2_);
	}
	else {
		Value tmpValue;
		cursor.calculator2_(txn, value.data(), value.size(), value.data(),
			value.size(), tmpValue);
		cursor.calculator1_(txn, tmpValue.data(), tmpValue.size(),
			cursor.value2_.data(), cursor.value2_.size(), cursor.value2_);
	}

	cursor.count_++;
}

/*!
	@brief Execute stddev function for a Row
*/
static void stddevForLoop(
	TransactionContext &txn, const Value &value, AggregationCursor &cursor) {
	if (cursor.count_ == 0) {
		cursor.value1_ = value;
	}
	else {
		cursor.calculator1_(txn, value.data(), value.size(),
			cursor.value1_.data(), cursor.value1_.size(), cursor.value1_);
	}

	if (cursor.count_ == 0) {
		cursor.calculator2_(txn, value.data(), value.size(), value.data(),
			value.size(), cursor.value2_);
	}
	else {
		Value tmpValue;
		cursor.calculator2_(txn, value.data(), value.size(), value.data(),
			value.size(), tmpValue);
		cursor.calculator1_(txn, tmpValue.data(), tmpValue.size(),
			cursor.value2_.data(), cursor.value2_.size(), cursor.value2_);
	}

	cursor.count_++;
}

void aggregationPostProcess(AggregationCursor &cursor, Value &value);

static const AggregatorForLoop aggLoopTable[] = {&minForLoop, &maxForLoop,
	&sumForLoop, &avgForLoop, &varianceForLoop, &stddevForLoop, &countForLoop,
	NULL};

#endif
