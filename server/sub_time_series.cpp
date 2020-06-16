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
	@brief Implementation of SubTimeSeries
*/
#include "util/trace.h"
#include "btree_map.h"
#include "data_store.h"
#include "data_store_common.h"
#include "hash_map.h"
#include "time_series.h"
#include "transaction_manager.h"
#include "gs_error.h"
#include "message_schema.h"
#include "value_processor.h"


/*!
	@brief Initialize the area in SubTimeSeries
*/
void SubTimeSeries::initialize(uint64_t position, Timestamp startTime) {
	position_ = position;
	startTime_ = startTime;
	metaAllocateStrategy_ = AllocateStrategy();
	rowAllocateStrategy_ = AllocateStrategy();
	mapAllocateStrategy_ = AllocateStrategy();
}

/*!
	@brief Initialize the area in SubTimeSeries
*/
void SubTimeSeries::initialize(TransactionContext&) {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
}

/*!
	@brief Set SubTimeSeries Schema
*/
void SubTimeSeries::set(
	TransactionContext&, const FullContainerKey&, ContainerId, OId, MessageSchema*) {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
}

/*!
	@brief Set SubTimeSeries Schema
*/
void SubTimeSeries::set(TransactionContext& txn, TimeSeries* parent) {

	baseContainerImage_ = reinterpret_cast<BaseContainerImage*>(
		ALLOC_NEW(txn.getDefaultAllocator()) TimeSeriesImage);
	memcpy(baseContainerImage_, parent->baseContainerImage_,
		sizeof(TimeSeriesImage));
	baseContainerImage_->rowIdMapOId_ = UNDEF_OID;
	baseContainerImage_->mvccMapOId_ = UNDEF_OID;
	baseContainerImage_->rowNum_ = 0;
	rowImageSize_ = parent->rowImageSize_;
	commonContainerSchema_ = parent->commonContainerSchema_;
	columnSchema_ = parent->columnSchema_;
	indexSchema_ = parent->indexSchema_;
	rowArrayCache_ = parent_->rowArrayCache_;

	if (getExpirationInfo().duration_ == INT64_MAX) {
		endTime_ = MAX_TIMESTAMP;
	}
	else {
		endTime_ = startTime_ + parent->getDivideDuration() - 1;
	}

	setAllocateStrategy(parent);

	BtreeMap map(txn, *getObjectManager(), getMapAllcateStrategy(), this, rowIdFuncInfo_);
	map.initialize(
		txn, COLUMN_TYPE_TIMESTAMP, true, BtreeMap::TYPE_UNIQUE_RANGE_KEY);
	baseContainerImage_->rowIdMapOId_ = map.getBaseOId();

	BtreeMap mvccMap(txn, *getObjectManager(), getMapAllcateStrategy(), this, mvccFuncInfo_);
	mvccMap.initialize<TransactionId, MvccRowImage>(
		txn, COLUMN_TYPE_OID, false, BtreeMap::TYPE_SINGLE_KEY);
	baseContainerImage_->mvccMapOId_ = mvccMap.getBaseOId();

	util::XArray<IndexData> indexList(txn.getDefaultAllocator());
	bool withUncommitted = true;
	parent->getIndexList(txn, withUncommitted, indexList);
	for (size_t i = 0; i < indexList.size(); i++) {
		IndexInfo indexInfo(txn.getDefaultAllocator(), *(indexList[i].columnIds_), indexList[i].mapType_);
		IndexCursor indexCursor(true);
		createIndex(txn, indexInfo, indexCursor);
	}

	updateSubTimeSeriesImage(txn);
	setAllocateStrategy(parent);
}

/*!
	@brief Free Objects related to SubTimeSeries
*/
bool SubTimeSeries::finalize(TransactionContext& txn) {
	RowArray rowArray(txn, this);
	if (baseContainerImage_->rowIdMapOId_ != UNDEF_OID) {
		BtreeMap::BtreeCursor btreeCursor;
		StackAllocAutoPtr<BtreeMap> rowIdMap(
			txn.getDefaultAllocator(), getRowIdMap(txn));
		ResultSize limit = (ContainerCursor::getNum() / getNormalRowArrayNum() <= 2) ?
			2 : (ContainerCursor::getNum() / getNormalRowArrayNum());
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		util::XArray<OId> idList(txn.getDefaultAllocator());
		util::XArray<OId>::iterator itr;
		int32_t getAllStatus = rowIdMap.get()->getAll(
			txn, limit, idList, btreeCursor);

		for (itr = idList.begin(); itr != idList.end(); itr++) {
			bool isOldSchema = rowArray.load(txn, *itr, this, OBJECT_FOR_UPDATE);
			UNUSED_VARIABLE(isOldSchema);
			assert(isOldSchema || !isOldSchema);
			RowId rowArrayId = rowArray.getRowId();
			removeRowIdMap(txn, rowIdMap.get(),
				&rowArrayId, *itr); 
			rowArray.finalize(txn);
		}
		if (getAllStatus != GS_SUCCESS) {
			return false;
		}

		getDataStore()->finalizeMap(txn, getMapAllcateStrategy(), rowIdMap.get());
	}

	if (baseContainerImage_->mvccMapOId_ != UNDEF_OID) {
		BtreeMap::BtreeCursor btreeCursor;
		StackAllocAutoPtr<BtreeMap> mvccMap(
			txn.getDefaultAllocator(), getMvccMap(txn));
		while (1) {
			util::StackAllocator::Scope scope(txn.getDefaultAllocator());
			util::XArray<std::pair<TransactionId, MvccRowImage> > idList(
				txn.getDefaultAllocator());
			util::XArray<std::pair<TransactionId, MvccRowImage> >::iterator itr;
			int32_t getAllStatus =
				mvccMap.get()->getAll<TransactionId, MvccRowImage>(
					txn, PARTIAL_RESULT_SIZE, idList, btreeCursor);

			for (itr = idList.begin(); itr != idList.end(); itr++) {
					switch (itr->second.type_) {
					case MVCC_CREATE:
					case MVCC_SELECT:
					case MVCC_INDEX:
					case MVCC_CONTAINER:
						break;
					case MVCC_UPDATE:
					case MVCC_DELETE:
						{
							bool isOldSchema = rowArray.load(txn, itr->second.snapshotRowOId_, this,
								OBJECT_FOR_UPDATE);
							UNUSED_VARIABLE(isOldSchema);
							assert(isOldSchema || !isOldSchema);
							rowArray.finalize(txn);
						}
						break;
					default:
						GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
						break;
					}
			}
			if (getAllStatus == GS_SUCCESS) {
				break;
			}
		}
		getDataStore()->finalizeMap(txn, getMapAllcateStrategy(), mvccMap.get());
	}

	finalizeIndex(txn);

	baseContainerImage_->indexSchemaOId_ = UNDEF_OID;
	baseContainerImage_->rowIdMapOId_ = UNDEF_OID;
	baseContainerImage_->mvccMapOId_ = UNDEF_OID;
	baseContainerImage_->rowNum_ = 0;
	updateSubTimeSeriesImage(txn);

	return true;
}

/*!
	@brief Creates a specifed type of index on the specified Column
*/
void SubTimeSeries::createIndex(TransactionContext& txn,
	const IndexInfo& indexInfo, IndexCursor& indexCursor,
	bool isIndexNameCaseSensitive) {
	UNUSED_VARIABLE(indexCursor);
	UNUSED_VARIABLE(isIndexNameCaseSensitive);
	util::Vector<ColumnType> columnTypes(txn.getDefaultAllocator());
	for (size_t i = 0; i < indexInfo.columnIds_.size(); i++) {
		ColumnId inputColumnId = indexInfo.columnIds_[i];
		ColumnInfo& columnInfo = getColumnInfo(inputColumnId);
		columnTypes.push_back(columnInfo.getColumnType());
	}
	bool isUnique = false;
	indexSchema_->createIndexData(txn, indexInfo.columnIds_, 
		indexInfo.mapType, columnTypes, this, position_, isUnique);

	updateSubTimeSeriesImage(txn);
}

/*!
	@brief Continues to create a specifed type of index on the specified Column
*/
void SubTimeSeries::continueCreateIndex(TransactionContext& , 
	IndexCursor& ) {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
}

/*!
	@brief Continue to create a specifed type of index on the specified Column
*/
void SubTimeSeries::indexInsert(TransactionContext& txn, 
	IndexCursor& indexCursor) {

	IndexData indexData(txn.getDefaultAllocator());
	bool isExist = getIndexData(txn, indexCursor, indexData);

	if (!isExist) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	indexData.cursor_ = indexCursor.getRowId();
	indexInsertImpl<TimeSeries>(txn, indexData, indexCursor.isImmediateMode());
	updateIndexData(txn, indexData);
	indexCursor.setRowId(indexData.cursor_);
}

/*!
	@brief Creates a specifed type of index on the specified Column
*/
void SubTimeSeries::dropIndex(TransactionContext& txn, IndexInfo& indexInfo,
							  bool isIndexNameCaseSensitive) {
	UNUSED_VARIABLE(isIndexNameCaseSensitive);
	indexSchema_->dropIndexData(
		txn, indexInfo.columnIds_, indexInfo.mapType, this, position_, true);
}

/*!
	@brief Deletes a Row corresponding to the specified Row key
*/
void SubTimeSeries::deleteRow(TransactionContext& txn, uint32_t,
	const uint8_t* rowKey, RowId& rowId, bool& existing) {
	Timestamp rowKeyTimestamp = *(reinterpret_cast<const Timestamp*>(rowKey));
	bool isForceLock = true;
	deleteRowInternal(txn, rowKeyTimestamp, existing, isForceLock);
	if (existing) {
		rowId = rowKeyTimestamp;
		decrementRowNum();
	}
	else {
		rowId = UNDEF_ROWID;
	}
	updateSubTimeSeriesImage(txn);
}

/*!
	@brief Deletes a Row corresponding to the specified RowId
*/
void SubTimeSeries::deleteRow(
	TransactionContext& txn, RowId rowId, bool& existing, bool isForceLock) {
	util::StackAllocator::Scope scope(txn.getDefaultAllocator());
	deleteRowInternal(txn, rowId, existing, isForceLock);
	if (existing) {
		decrementRowNum();
	}
	updateSubTimeSeriesImage(txn);
}

/*!
	@brief Updates a Row corresponding to the specified RowId
*/
void SubTimeSeries::updateRow(TransactionContext& txn, uint32_t rowSize,
	const uint8_t* rowData, RowId rowId, DataStore::PutStatus& status,
	bool isForceLock) {
	util::StackAllocator::Scope scope(txn.getDefaultAllocator());

	InputMessageRowStore inputMessageRowStore(
		getDataStore()->getValueLimitConfig(), getColumnInfoList(),
		getColumnNum(), const_cast<uint8_t*>(rowData), rowSize, 1,
		rowFixedDataSize_);
	inputMessageRowStore.next();

	Timestamp rowKey = inputMessageRowStore.getField<COLUMN_TYPE_TIMESTAMP>(
		ColumnInfo::ROW_KEY_COLUMN_ID);
		TermCondition cond(getRowIdColumnType(), getRowIdColumnType(),
			DSExpression::LE, getRowIdColumnId(), &rowKey,
			sizeof(rowKey));
		BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, 1);
	util::XArray<OId> oIdList(txn.getDefaultAllocator());
	searchRowIdIndex(txn, sc, oIdList, ORDER_UNDEFINED);
	if (!oIdList.empty()) {
		RowArray rowArray(txn, this);
		bool isOldSchema = rowArray.load(txn, oIdList[0], this, OBJECT_FOR_UPDATE);
		if (isOldSchema) {
			RowScope rowScope(*this, TO_NORMAL);
			convertRowArraySchema(txn, rowArray, false);
		}
		RowArray::Row row(rowArray.getRow(), &rowArray);
		if (!isForceLock &&
			row.getTxnId() != txn.getId()) {  
			if (txn.getManager().isActiveTransaction(
				txn.getPartitionId(), row.getTxnId())) {
					DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_COL_LOCK_CONFLICT,
						"(txnId=" << txn.getId() << ", rowTxnId=" << row.getTxnId() << ")");
			}
		}
		updateRowInternal(txn, &inputMessageRowStore, rowArray, rowId);
		status = DataStore::UPDATE;
	}
	else {
		status = DataStore::NOT_EXECUTED;
	}
	updateSubTimeSeriesImage(txn);
}

/*!
	@brief Rolls back the result of transaction
*/
void SubTimeSeries::abort(TransactionContext& txn) {
	abortInternal(txn, txn.getId());
}

void SubTimeSeries::abortInternal(TransactionContext& txn, TransactionId tId) {
	util::StackAllocator::Scope scope(txn.getDefaultAllocator());

	StackAllocAutoPtr<BtreeMap> mvccMap(
		txn.getDefaultAllocator(), getMvccMap(txn));
	if (mvccMap.get()->isEmpty()) {
		return;
	}

	util::XArray<MvccRowImage> mvccList(txn.getDefaultAllocator());
	util::XArray<MvccRowImage>::iterator itr;
	TermCondition cond(COLUMN_TYPE_LONG, COLUMN_TYPE_LONG, 
		DSExpression::EQ, UNDEF_COLUMNID, &tId, sizeof(tId));
	BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, MAX_RESULT_SIZE);
	mvccMap.get()->search<TransactionId, MvccRowImage, MvccRowImage>(
		txn, sc, mvccList);

	if (!mvccList.empty()) {
		getObjectManager()->setDirty(
			txn.getPartitionId(), baseContainerImage_->mvccMapOId_);
		RowArray rowArray(txn, this);
		for (itr = mvccList.begin(); itr != mvccList.end(); itr++) {
			if (parent_->isAlterContainer() && itr->type_ != MVCC_CONTAINER) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_COL_LOCK_CONFLICT, 
					"abort : container already locked "
					<< ", partitionId = " << txn.getPartitionId()
					<< ", txnId = " << txn.getId()
					<< ", containerId = " << getContainerId()
					);
			}
			if (itr->type_ == MVCC_CREATE) {
				RowId startKey = itr->firstCreateRowId_;
				RowId endKey = itr->lastCreateRowId_;
				if (startKey != INITIAL_ROWID) {
					TermCondition startCond(COLUMN_TYPE_TIMESTAMP, COLUMN_TYPE_TIMESTAMP, 
						DSExpression::GE, ColumnInfo::ROW_KEY_COLUMN_ID, &startKey, sizeof(startKey));
					TermCondition endCond(COLUMN_TYPE_TIMESTAMP, COLUMN_TYPE_TIMESTAMP, 
						DSExpression::LE, ColumnInfo::ROW_KEY_COLUMN_ID, &endKey, sizeof(endKey));
					BtreeMap::SearchContext sc(txn.getDefaultAllocator(), startCond, endCond, MAX_RESULT_SIZE);
					util::XArray<OId> oIdList(txn.getDefaultAllocator());
					util::XArray<OId>::iterator rowItr;
					{
						StackAllocAutoPtr<BtreeMap> rowIdMap(
							txn.getDefaultAllocator(), getRowIdMap(txn));
						rowIdMap.get()->search(txn, sc, oIdList);
					}
					for (rowItr = oIdList.begin(); rowItr != oIdList.end();
						 rowItr++) {
						util::StackAllocator::Scope scope(txn.getDefaultAllocator());
						bool isOldSchema = rowArray.load(txn, *rowItr,
							reinterpret_cast<SubTimeSeries*>(this),
							OBJECT_READ_ONLY);
						UNUSED_VARIABLE(isOldSchema);
						assert(isOldSchema || !isOldSchema);
						if (rowArray.getTxnId() == tId && !rowArray.isFirstUpdate()) {
							bool isOldSchema = rowArray.setDirty(txn);
							UNUSED_VARIABLE(isOldSchema);
							assert(isOldSchema || !isOldSchema);
							undoCreateRow(txn, rowArray);
						}
					}
				}
			}
		}
		for (itr = mvccList.begin(); itr != mvccList.end(); itr++) {
				switch (itr->type_) {
				case MVCC_SELECT:
				case MVCC_CREATE: 
					removeMvccMap(txn, mvccMap.get(), tId, *itr);
					break;
				case MVCC_INDEX:
				case MVCC_CONTAINER:
					removeMvccMap(txn, mvccMap.get(), tId, *itr);
					GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "Type = " << (int)itr->type_
						<< "This type must not exist in SubTimeseries ");
					break;
				case MVCC_UPDATE:
				case MVCC_DELETE:
					{
						util::StackAllocator::Scope scope(txn.getDefaultAllocator());
						bool isOldSchema = rowArray.load(txn, itr->snapshotRowOId_, this, OBJECT_FOR_UPDATE);
						UNUSED_VARIABLE(isOldSchema);
						assert(isOldSchema || !isOldSchema);
						undoUpdateRow(txn, rowArray);
						removeMvccMap(txn, mvccMap.get(), tId, *itr);
					}
					break;
				default:
					GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
					break;
				}
		}

		const CompressionSchema& compressionSchema = getCompressionSchema();
		switch (compressionSchema.getCompressionType()) {
		case HI_COMPRESSION: {
			uint16_t hiCompressionColumnNum =
				compressionSchema.getHiCompressionNum();
			if (hiCompressionColumnNum > 0) {
				DSDCValList dsdcValList(txn, *getObjectManager(),
					reinterpret_cast<TimeSeriesImage*>(baseContainerImage_)
						->hiCompressionStatus_);
				dsdcValList.reset(compressionSchema, hiCompressionColumnNum);
			}
		} break;
		case SS_COMPRESSION: {
			setSsCompressionReady(SS_IS_NOT_READRY);
		} break;
		default:
			break;
		}
	}

	if (mvccMap.get()->isEmpty()) {
		resetRuntime();
	}
	updateSubTimeSeriesImage(txn);
}

/*!
	@brief Commits the result of transaction
*/
void SubTimeSeries::commit(TransactionContext& txn) {
	util::StackAllocator::Scope scope(txn.getDefaultAllocator());
	StackAllocAutoPtr<BtreeMap> mvccMap(
		txn.getDefaultAllocator(), getMvccMap(txn));
	if (mvccMap.get()->isEmpty()) {
		return;
	}

	TransactionId tId = txn.getId();
	util::XArray<MvccRowImage> mvccList(txn.getDefaultAllocator());
	util::XArray<MvccRowImage>::iterator itr;
	TermCondition cond(COLUMN_TYPE_LONG, COLUMN_TYPE_LONG, 
		DSExpression::EQ, UNDEF_COLUMNID, &tId, sizeof(tId));
	BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, MAX_RESULT_SIZE);
	mvccMap.get()->search<TransactionId, MvccRowImage, MvccRowImage>(
		txn, sc, mvccList);

	if (!mvccList.empty()) {
		getObjectManager()->setDirty(
			txn.getPartitionId(), baseContainerImage_->mvccMapOId_);
		RowArray rowArray(txn, this);
		for (itr = mvccList.begin(); itr != mvccList.end(); itr++) {
			if (parent_->isAlterContainer() && itr->type_ != MVCC_CONTAINER) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TIM_LOCK_CONFLICT, 
					"commit : container already locked "
					<< ", partitionId = " << txn.getPartitionId()
					<< ", txnId = " << txn.getId()
					<< ", containerId = " << getContainerId()
					);
			}
			switch (itr->type_) {
			case MVCC_CREATE:
			case MVCC_SELECT:
				{
					removeMvccMap(txn, mvccMap.get(), tId, *itr);
				}
				break;
			case MVCC_UPDATE:
			case MVCC_DELETE:
				{
					bool isOldSchema = rowArray.load(txn, itr->snapshotRowOId_, this, OBJECT_FOR_UPDATE);
					UNUSED_VARIABLE(isOldSchema);
					assert(isOldSchema || !isOldSchema);
					rowArray.finalize(txn);
					removeMvccMap(txn, mvccMap.get(), tId, *itr);
				}
				break;
			case MVCC_INDEX:
				{
					removeMvccMap(txn, mvccMap.get(), tId, *itr);
				}
				break;
			case MVCC_CONTAINER:
				{
					resetAlterContainer();
					removeMvccMap(txn, mvccMap.get(), tId, *itr);
				}
				break;
			default:
				GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
				break;
			}
		}
	}

	if (mvccMap.get()->isEmpty()) {
		resetRuntime();
	}
	updateSubTimeSeriesImage(txn);
}

/*!
	@brief Search Btree Index of RowId
*/
void SubTimeSeries::searchRowIdIndex(TransactionContext& txn,
	BtreeMap::SearchContext& sc, util::XArray<OId>& resultList,
	OutputOrder outputOrder) {

	util::XArray<OId> mvccList(txn.getDefaultAllocator());
	ResultSize limitBackup = sc.getLimit();
	sc.setLimit(MAX_RESULT_SIZE);
	bool isCheckOnly = sc.getResumeStatus() != BaseIndex::SearchContext::NOT_RESUME;
	const bool ignoreTxnCheck = false;
	searchMvccMap<TimeSeries, BtreeMap::SearchContext>(txn, sc, mvccList, isCheckOnly, ignoreTxnCheck);
	sc.setLimit(limitBackup);

	ContainerValue containerValue(txn.getPartitionId(), *getObjectManager());
	util::XArray<OId> oIdList(txn.getDefaultAllocator());
	util::XArray<OId>::iterator itr;

	limitBackup = sc.getLimit();
	if (sc.getRestConditionNum() > 0 || !isExclusive()) {
		sc.setLimit(MAX_RESULT_SIZE);
	}
	else if (sc.getLimit() < MAX_RESULT_SIZE) {
		sc.setLimit(sc.getLimit() + 1);
	}
	StackAllocAutoPtr<BtreeMap> rowIdMap(
		txn.getDefaultAllocator(), getRowIdMap(txn));
	rowIdMap.get()->search(txn, sc, oIdList, outputOrder);
	sc.setLimit(limitBackup);

	util::Vector<TermCondition> keyCondList(txn.getDefaultAllocator());
	sc.getConditionList(keyCondList, BaseIndex::SearchContext::COND_KEY);
	util::Vector<TermCondition> condList(txn.getDefaultAllocator());
	sc.getConditionList(condList, BaseIndex::SearchContext::COND_OTHER);
	const Timestamp *startKey = sc.getStartKey<Timestamp>();
	const Timestamp *endKey = sc.getEndKey<Timestamp>();

	RowId lastCheckRowId = UNDEF_ROWID;
	RowArray rowArray(txn, this);
	for (itr = oIdList.begin(); itr != oIdList.end(); itr++) {
		rowArray.load(txn, *itr, this, OBJECT_READ_ONLY);
		lastCheckRowId = rowArray.getRowId();
		if (!isExclusive() && txn.getId() != rowArray.getTxnId() &&
			!rowArray.isFirstUpdate() &&
			txn.getManager().isActiveTransaction(
				txn.getPartitionId(), rowArray.getTxnId())) {
			continue;
		}
		if (outputOrder != ORDER_DESCENDING) {
			if (startKey != NULL && *startKey > rowArray.getRowId()) {
				rowArray.searchNextRowId(*startKey);
			} else {
				rowArray.begin();
			}
			for (; !rowArray.end(); rowArray.next()) {
				RowArray::Row row(rowArray.getRow(), &rowArray);
				Timestamp rowId = row.getRowId();
				lastCheckRowId = rowId;
				bool isMatch = true;
				util::Vector<TermCondition>::iterator condItr;
				for (condItr = keyCondList.begin(); condItr != keyCondList.end(); condItr++) {
					isMatch = condItr->operator_(txn, reinterpret_cast<const uint8_t *>(&rowId),
						sizeof(rowId), static_cast<const uint8_t *>(condItr->value_),
						condItr->valueSize_);
					if (!isMatch) {
						break;
					}
				}
				if (!isMatch) {
					continue;
				}
				for (condItr = condList.begin(); condItr != condList.end(); condItr++) {
					if (!row.isMatch(txn, *condItr, containerValue)) {
						isMatch = false;
						break;
					}
				}
				if (isMatch) {
					resultList.push_back(rowArray.getOId());
					if (resultList.size() == sc.getLimit()) {
						break;
					}
				}
			}
		}
		else {
			bool isRend;
			if (endKey != NULL && *endKey > rowArray.getRowId()) {
				isRend = rowArray.searchPrevRowId(*endKey);
			} else {
				isRend = rowArray.tail();
			}
			for (; isRend; isRend = rowArray.prev()) {
				RowArray::Row row(rowArray.getRow(), &rowArray);
				Timestamp rowId = row.getRowId();
				lastCheckRowId = rowId;
				bool isMatch = true;
				util::Vector<TermCondition>::iterator condItr;
				for (condItr = keyCondList.begin(); condItr != keyCondList.end(); condItr++) {
					isMatch = condItr->operator_(txn, reinterpret_cast<const uint8_t *>(&rowId),
						sizeof(rowId), static_cast<const uint8_t *>(condItr->value_),
						condItr->valueSize_);
					if (!isMatch) {
						break;
					}
				}
				if (!isMatch) {
					continue;
				}
				for (condItr = condList.begin(); condItr != condList.end(); condItr++) {
					if (!row.isMatch(txn, *condItr, containerValue)) {
						isMatch = false;
						break;
					}
				}
				if (isMatch) {
					resultList.push_back(rowArray.getOId());
					if (resultList.size() == sc.getLimit()) {
						break;
					}
				}
			}
		}
		if (resultList.size() >= sc.getLimit()) {
			break;
		}
	}
	if (sc.isSuspended() && lastCheckRowId != UNDEF_ROWID) {
		sc.setSuspendPoint(txn, &(++lastCheckRowId), sizeof(RowId), 0);
	}

	if (!mvccList.empty() && outputOrder != ORDER_UNDEFINED) {
		util::XArray<OId> mergeList(txn.getDefaultAllocator());
		mergeList.reserve(
			resultList.size() + mvccList.size());  
		{
			util::StackAllocator::Scope scope(
				txn.getDefaultAllocator());  

			RowArray rowArray(txn, reinterpret_cast<SubTimeSeries*>(this));
			ColumnType targetType = COLUMN_TYPE_ROWID;
			util::XArray<SortKey> indexSortKeyList(txn.getDefaultAllocator());
			util::XArray<SortKey> mvccSortKeyList(txn.getDefaultAllocator());
			for (itr = mvccList.begin(); itr != mvccList.end(); itr++) {
				rowArray.load(txn, *itr, reinterpret_cast<SubTimeSeries*>(this),
					OBJECT_READ_ONLY);
				RowArray::Row row(rowArray.getRow(), &rowArray);
				RowId rowId = row.getRowId();
				SortKey sortKey;
				sortKey.set(txn, targetType, reinterpret_cast<uint8_t *>(&rowId), *itr);

				mvccSortKeyList.push_back(sortKey);
			}

			bool isNullLast = outputOrder == ORDER_ASCENDING;
			const Operator* sortOp;
			if (outputOrder == ORDER_ASCENDING) {
				sortOp = &ComparatorTable::ltTable_[targetType][targetType];
			}
			else {
				sortOp = &ComparatorTable::gtTable_[targetType][targetType];
			}
			std::sort(mvccSortKeyList.begin(), mvccSortKeyList.end(),
				SortPred(txn, sortOp, targetType, isNullLast));

			for (itr = resultList.begin(); itr != resultList.end(); itr++) {
				rowArray.load(txn, *itr, reinterpret_cast<SubTimeSeries*>(this),
					OBJECT_READ_ONLY);
				RowArray::Row row(rowArray.getRow(), &rowArray);
				RowId rowId = row.getRowId();

				SortKey sortKey;
				sortKey.set(txn, targetType, reinterpret_cast<uint8_t *>(&rowId), *itr);
				indexSortKeyList.push_back(sortKey);
			}

			util::XArray<SortKey>::iterator indexItr, mvccItr;
			Value value1, value2;
			for (indexItr = indexSortKeyList.begin(),
				mvccItr = mvccSortKeyList.begin();
				 indexItr != indexSortKeyList.end() &&
				 mvccItr != mvccSortKeyList.end();) {
				value1.set(indexItr->data(), targetType);
				value2.set(mvccItr->data(), targetType);
				if ((*sortOp)(txn, value1.data(), value1.size(), value2.data(),
						value2.size())) {
					mergeList.push_back(indexItr->getOId());
					indexItr++;
				}
				else {
					mergeList.push_back(mvccItr->getOId());
					mvccItr++;
				}
			}
			while (indexItr != indexSortKeyList.end()) {
				mergeList.push_back(indexItr->getOId());
				indexItr++;
			}
			while (mvccItr != mvccSortKeyList.end()) {
				mergeList.push_back(mvccItr->getOId());
				mvccItr++;
			}
		}
		resultList.clear();
		resultList.swap(mergeList);
	}
	else {
		resultList.push_back(mvccList.data(), mvccList.size());
	}
	if (resultList.size() > sc.getLimit()) {
		resultList.resize(sc.getLimit());
	}
}

/*!
	@brief Search Btree Index of RowId
*/
void SubTimeSeries::searchRowIdIndex(TransactionContext&, uint64_t, uint64_t,
	util::XArray<RowId>&, util::XArray<OId>&, uint64_t&) {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
}

void SubTimeSeries::searchRowIdIndexAsRowArray(
		TransactionContext& txn, BtreeMap::SearchContext &sc,
		util::XArray<OId> &oIdList, util::XArray<OId> &mvccOIdList) {
	const OutputOrder order = ORDER_UNDEFINED;

	RowId startRowId;
	RowId endRowId;
	if (!getRowIdRangeCondition(
			txn.getDefaultAllocator(),
			sc, getContainerType(), &startRowId, &endRowId)) {
		return;
	}

	scanRowIdIndexPrepare(txn, sc, order, &oIdList);

	if (!isExclusive()) {
		const RowId lastCheckRowId = UNDEF_ROWID;
		const RowId lastMergedRowId = UNDEF_ROWID;

		BtreeMap::SearchContext mvccSC (txn.getDefaultAllocator(), UNDEF_COLUMNID);
		std::pair<RowId, RowId> mvccRowIdRange;
		searchMvccMapPrepare(
				txn, sc, mvccSC, order, startRowId, endRowId,
				lastCheckRowId, lastMergedRowId, &mvccRowIdRange);

		const bool checkOnly = false;
		const bool ignoreTxnCheck = false;
		searchMvccMap<TimeSeries, BtreeMap::SearchContext>(
				txn, mvccSC, mvccOIdList, checkOnly, ignoreTxnCheck);
	}
}

bool SubTimeSeries::scanRowIdIndex(
		TransactionContext& txn, BtreeMap::SearchContext &sc,
		OutputOrder order, ContainerRowScanner &scanner,
		util::XArray<OId> *(&oIdListRef)[3],
		util::XArray< std::pair<RowId, OId> > &rowOIdList) {

	util::XArray<OId> &oIdList = *oIdListRef[0];
	util::XArray<OId> &mvccOIdList = *oIdListRef[1];
	util::XArray<OId> &mergedOIdList = *oIdListRef[2];

	RowId startRowId;
	RowId endRowId;
	if (!getRowIdRangeCondition(
			txn.getDefaultAllocator(),
			sc, getContainerType(), &startRowId, &endRowId)) {
		return false;
	}

	oIdList.clear();
	scanRowIdIndexPrepare(txn, sc, order, &oIdList);

	RowId lastCheckRowId = UNDEF_ROWID;
	RowId lastMergedRowId = UNDEF_ROWID;

	const size_t rowOIdListLimit =
			txn.getDefaultAllocator().base().getElementSize() /
			(sizeof(RowId) + sizeof(OId)) / 4;
	size_t restRowOIdCount = rowOIdListLimit;

	bool limitReached = false;
	RowArray &rowArray = scanner.getRowArray();

	for (util::XArray<OId>::iterator it = oIdList.begin();; ++it) {
		bool inRange = false;
		bool found = false;
		if (it != oIdList.end()) {
			rowArray.load(txn, *it, this, OBJECT_READ_ONLY);

			if (order != ORDER_DESCENDING) {
				found = rowArray.tail();
				if (found) {
					RowArray::Row row(rowArray.getRow(), &rowArray);
					lastCheckRowId = row.getRowId();

					if (rowArray.begin() && lastCheckRowId < endRowId) {
						inRange = (rowArray.getRowId() >= startRowId);
					}
				}
			}
			else {
				found = rowArray.begin();
				if (found) {
					lastCheckRowId = rowArray.getRowId();

					if (rowArray.tail() && lastCheckRowId >= startRowId) {
						RowArray::Row row(rowArray.getRow(), &rowArray);
						inRange = (row.getRowId() < endRowId);
					}
				}
			}
		}

		if (inRange && isExclusive()) {
			if (order != ORDER_DESCENDING) {
				if (!scanner.scanRowArrayUnchecked(
						txn, *this, &rowArray, &(*it), &(*it) + 1)) {
					limitReached = true;
					break;
				}
			}
			else {
				if (found && rowArray.tail()) {
					do {
						RowArray::Row row(rowArray.getRow(), &rowArray);
						mergedOIdList.push_back(rowArray.getOId());
					}
					while (rowArray.prev());
				}
			}
		}
		else {
			RowArray *foundRowArray = (found ? &rowArray : NULL);
			if (scanRowArrayChecked(
					txn, foundRowArray, sc, order, startRowId, endRowId,
					lastCheckRowId, lastMergedRowId, restRowOIdCount,
					rowOIdList, mvccOIdList, mergedOIdList)) {

				restRowOIdCount = rowOIdListLimit;
			}
		}

		if (!mergedOIdList.empty()) {
			if (!scanner.scanRowUnchecked(
					txn, *this, NULL,
					&mergedOIdList.front(), &mergedOIdList.back() + 1)) {
				limitReached = true;
				break;
			}
			mergedOIdList.clear();
		}

		if (it == oIdList.end()) {
			break;
		}
	}

	if (sc.isSuspended() && lastCheckRowId != UNDEF_ROWID) {
		if (order != ORDER_DESCENDING) {
			++lastCheckRowId;
		}
		else {
			--lastCheckRowId;
		}

		sc.setSuspendPoint(txn, &lastCheckRowId, sizeof(RowId), 0);

	}

	return !limitReached;
}

void SubTimeSeries::setDummyMvccImage(TransactionContext& txn) {
	try {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		StackAllocAutoPtr<BtreeMap> mvccMap(
			txn.getDefaultAllocator(), getMvccMap(txn));
		TransactionId tId = txn.getId();

		bool exists = false;
		if (!mvccMap.get()->isEmpty()) {
			util::XArray<MvccRowImage> mvccList(txn.getDefaultAllocator());
			util::XArray<MvccRowImage>::iterator itr;
			TermCondition cond(COLUMN_TYPE_LONG, COLUMN_TYPE_LONG, 
				DSExpression::EQ, UNDEF_COLUMNID, &tId, sizeof(tId));
			BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, MAX_RESULT_SIZE);
			mvccMap.get()->search<TransactionId, MvccRowImage, MvccRowImage>(
				txn, sc, mvccList);
			if (!mvccList.empty()) {
				for (itr = mvccList.begin(); itr != mvccList.end(); itr++) {
					if (itr->type_ == MVCC_SELECT) {
						exists = true;
						break;
					}
				}
			}
		}

		if (!exists) {
			MvccRowImage dummyMvccImage(MVCC_SELECT, INITIAL_ROWID);
			insertMvccMap(txn, mvccMap.get(), tId, dummyMvccImage);
			setRuntime();
			updateSubTimeSeriesImage(txn);
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void SubTimeSeries::getIndexList(TransactionContext &txn, bool withUncommitted, 
	util::XArray<IndexData> &list) const {
	indexSchema_->getIndexList(txn, position_, withUncommitted, list);
}

/*!
	@brief Newly creates or updates a Row, based on the specified Row object and
   also the Row key specified as needed
*/
void SubTimeSeries::putRow(TransactionContext& txn,
	InputMessageRowStore* inputMessageRowStore, RowId& rowId, bool,
	DataStore::PutStatus& status, PutRowOption putRowOption) {
	util::StackAllocator::Scope scope(txn.getDefaultAllocator());

	PutMode mode = UNDEFINED_STATUS;
	Timestamp rowKey = inputMessageRowStore->getField<COLUMN_TYPE_TIMESTAMP>(
		ColumnInfo::ROW_KEY_COLUMN_ID);
	RowArray rowArray(txn, this);

	OId tailOId = UNDEF_OID;
	Timestamp tailRowKey = UNDEF_TIMESTAMP;
	bool isOmittable = false;

	const CompressionSchema& compressionSchema = getCompressionSchema();

	if (compressionSchema.getCompressionType() == NO_COMPRESSION) {
		if (position_ == parent_->getBasePos()) {
			tailOId = BtreeMap::getTailDirect(
				txn, *getObjectManager(), parent_->getTailNodeOId());
		}
		else {
			StackAllocAutoPtr<BtreeMap> rowIdMap(
				txn.getDefaultAllocator(), getRowIdMap(txn));
			tailOId = rowIdMap.get()->getTail(txn);
		}
		if (tailOId == UNDEF_OID) {
			mode = APPEND;
			StackAllocAutoPtr<BtreeMap> mvccMap(
				txn.getDefaultAllocator(), getMvccMap(txn));
			if (!mvccMap.get()->isEmpty()) {
				if (txn.isAutoCommit()) {
					DS_THROW_LOCK_CONFLICT_EXCEPTION(
						GS_ERROR_DS_TIM_LOCK_CONFLICT,
						"(pId=" << txn.getPartitionId() << ", rowKey=" << rowKey
								<< ")");
				}
				else {
					util::XArray<std::pair<TransactionId, MvccRowImage> >
						mvccKeyValueList(txn.getDefaultAllocator());
					util::XArray<std::pair<TransactionId,
						MvccRowImage> >::iterator mvccItr;
					mvccMap.get()->getAll<TransactionId, MvccRowImage>(
						txn, MAX_RESULT_SIZE, mvccKeyValueList);
					for (mvccItr = mvccKeyValueList.begin();
						 mvccItr != mvccKeyValueList.end(); mvccItr++) {
						if (mvccItr->first != txn.getId()) {
							DS_THROW_LOCK_CONFLICT_EXCEPTION(
								GS_ERROR_DS_TIM_LOCK_CONFLICT,
								"(pId=" << txn.getPartitionId()
										<< ", rowKey=" << rowKey << ")");
						}
					}
				}
			}
		}
		if (mode == UNDEFINED_STATUS) {
			bool isOldSchema = rowArray.load(txn, tailOId, this, OBJECT_FOR_UPDATE);
			bool isExist = rowArray.tail();
			RowArray::Row row(rowArray.getRow(), &rowArray);
			tailRowKey = row.getRowId();
			if (!isExist) {
				rowArray.lock(txn);
				RowId rowArrayRowId = rowArray.getRowId();
				if (rowArrayRowId <= rowKey) {
					mode = APPEND;
				}
			}
			else if (tailRowKey < rowKey) {
				mode = APPEND;
			}
			else if (tailRowKey == rowKey) {
				mode = UPDATE;
			}
			else {
				rowArray.begin();
				Timestamp headRowKey = row.getRowId();
				if (headRowKey <= rowKey) {
					if (rowArray.searchNextRowId(rowKey)) {
						mode = UPDATE;
					}
					else {
						mode = INSERT;
					}
				}
			}
			if (isOldSchema) {
				RowScope rowScope(*this, TO_NORMAL);
				convertRowArraySchema(txn, rowArray, false); 
			}
		}
		if (mode == UNDEFINED_STATUS) {
			TermCondition cond(getRowIdColumnType(), getRowIdColumnType(),
				DSExpression::LE, getRowIdColumnId(), &rowKey,
				sizeof(rowKey));
			BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, 1);
			util::XArray<OId> oIdList(txn.getDefaultAllocator());
			{
				StackAllocAutoPtr<BtreeMap> rowIdMap(
					txn.getDefaultAllocator(), getRowIdMap(txn));
				rowIdMap.get()->search(txn, sc, oIdList, ORDER_DESCENDING);
			}
			if (!oIdList.empty()) {
				bool isOldSchema = rowArray.load(txn, oIdList[0], this, OBJECT_FOR_UPDATE);
				if (rowArray.searchNextRowId(rowKey)) {
					mode = UPDATE;
				}
				else {
					mode = INSERT;
				}
				if (isOldSchema) {
					RowScope rowScope(*this, TO_NORMAL);
					convertRowArraySchema(txn, rowArray, false); 
				}
			}
			else {
				createMinimumRowArray(txn, rowArray, false);
				mode = APPEND;
			}
		}
	}
	else {
		bool isPastTimeInsert = false;
		if (position_ == parent_->getBasePos()) {
			tailOId = BtreeMap::getTailDirect(
				txn, *getObjectManager(), parent_->getTailNodeOId());
		}
		else {
			isPastTimeInsert = true;
		}
		if (!isPastTimeInsert && tailOId == UNDEF_OID) {
			mode = APPEND;
		}
		else if (!isPastTimeInsert) {
			bool isOldSchema = rowArray.load(txn, tailOId, this, OBJECT_FOR_UPDATE);

			bool isExist = rowArray.tail();
			if (isOldSchema) {
				RowScope rowScope(*this, TO_NORMAL);
				convertRowArraySchema(txn, rowArray, false);
			}
			RowArray::Row row(rowArray.getRow(), &rowArray);
			tailRowKey = row.getRowId();
			RowId rowArrayRowId = rowArray.getRowId();
			if (!isExist && rowArrayRowId <= rowKey) {
				rowArray.lock(txn);
				mode = APPEND;
			}
			else if (tailRowKey < rowKey) {
				rowArray.lock(txn);


				const CompressionSchema& compressionSchema =
					getCompressionSchema();
				switch (compressionSchema.getCompressionType()) {
				case HI_COMPRESSION: {
					isOmittable =
						isRowOmittableByHiCompression(txn, compressionSchema,
							inputMessageRowStore, rowArray, rowKey);
				} break;
				case SS_COMPRESSION: {
					isOmittable =
						isRowOmittableBySsCompression(txn, compressionSchema,
							inputMessageRowStore, rowArray, rowKey);
				} break;
				default:
					GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
					break;
				}
				if (isOmittable) {
					mode = UPDATE;
				}
				else {
					mode = APPEND;
				}
			}
			else if (tailRowKey == rowKey) {
				mode = NOT_EXECUTED;
			}
			else {
				isPastTimeInsert = true;
			}
		}
		if (isPastTimeInsert) {
			if (isCompressionErrorMode()) {  
				GS_THROW_USER_ERROR(GS_ERROR_DS_TIM_UPDATE_INVALID,
					"insert(old time)/update not support on Compression Mode : "
					"rowKey("
						<< rowKey << ") < latest rowKey(" << tailRowKey << ")");
			}
			else {
				mode = NOT_EXECUTED;

				const FullContainerKey containerKey = getContainerKey(txn);
				util::String containerName(txn.getDefaultAllocator());
				containerKey.toString(txn.getDefaultAllocator(), containerName);
				GS_TRACE_ERROR(TIME_SERIES,
					GS_TRACE_DS_TIM_COMPRESSION_INVALID_WARNING,
					"insert(old time)/update not support on Compression Mode : "
					"Container("
						<< containerName << ") rowKey("
						<< rowKey << ") < latest rowKey(" << tailRowKey << ")");
			}
		}
	}

	rowId = rowKey;
	switch (mode) {
	case APPEND:
		if (putRowOption == PUT_UPDATE_ONLY) {
			GS_THROW_USER_ERROR(GS_ERROR_CON_PUT_ROW_OPTION_INVALID,
				"update row, but row not exists");
		}
		appendRowInternal(txn, inputMessageRowStore, rowArray, rowId);
		status = DataStore::CREATE;
		break;
	case INSERT:
		if (putRowOption == PUT_UPDATE_ONLY) {
			GS_THROW_USER_ERROR(GS_ERROR_CON_PUT_ROW_OPTION_INVALID,
				"update row, but row not exists");
		}
		insertRowInternal(txn, inputMessageRowStore, rowArray, rowId);
		status = DataStore::CREATE;
		break;
	case UPDATE:
		if (putRowOption == PUT_INSERT_ONLY) {
			GS_THROW_USER_ERROR(GS_ERROR_CON_PUT_ROW_OPTION_INVALID,
				"insert row, but row exists");
		}
		updateRowInternal(txn, inputMessageRowStore, rowArray, rowId);
		if (isOmittable) {
			status = DataStore::CREATE;
		}
		else {
			status = DataStore::UPDATE;
		}
		break;
	case NOT_EXECUTED:
		status = DataStore::NOT_EXECUTED;
		rowId = UNDEF_ROWID;
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TIM_PUT_ROW_ERROR, "");
	}
	if (mode == APPEND || mode == INSERT) {
		incrementRowNum();
	}
	updateSubTimeSeriesImage(txn);
}

void SubTimeSeries::appendRowInternal(TransactionContext& txn,
	MessageRowStore* messageRowStore, RowArray& rowArray, RowId& rowId) {
	if (rowArray.isNotInitialized() || rowArray.isFull()) {
		StackAllocAutoPtr<BtreeMap> rowIdMap(
			txn.getDefaultAllocator(), getRowIdMap(txn));
		rowArray.reset();
		if (rowIdMap.get()->isEmpty()) {
			bool isRowArraySizeControlMode = false;
			uint32_t smallRowArrayNum =
				calcRowArrayNum(txn, isRowArraySizeControlMode, getSmallRowArrayNum());
			rowArray.initialize(txn, rowId, smallRowArrayNum);
		}
		else {
			rowArray.initialize(txn, rowId, getNormalRowArrayNum());
		}
		insertRowIdMap(txn, rowIdMap.get(), &rowId, rowArray.getBaseOId());
		rowArray.lock(txn);
		rowArray.resetFirstUpdate();
	}
	else {
		rowArray.lock(txn);
		if (!txn.isAutoCommit() && rowArray.isFirstUpdate()) {
			RowScope rowScope(*this, TO_MVCC);
			rowArray.resetFirstUpdate();
			RowArray snapShotRowArray(txn, this);
			snapShotRowArray.initialize(
				txn, rowArray.getRowId(), rowArray.getMaxRowNum());
			rowArray.copyRowArray(txn, snapShotRowArray);

			TransactionId tId = txn.getId();
			MvccRowImage mvccRowImage(MVCC_UPDATE, snapShotRowArray.getOId());
			StackAllocAutoPtr<BtreeMap> mvccMap(
				txn.getDefaultAllocator(), getMvccMap(txn));
			insertMvccMap(txn, mvccMap.get(), tId, mvccRowImage);
			setRuntime();
		}
		if (rowArray.isTailPos()) {
			RowScope rowScope(*this, TO_NORMAL);
			shift(txn, rowArray, true);
			rowArray.tail();
		}
	}
	{
		RowScope rowScope(*this, TO_NORMAL);

		rowArray.append(txn, messageRowStore, rowId);
		RowArray::Row row(rowArray.getRow(), &rowArray);

		util::XArray<IndexData> indexList(txn.getDefaultAllocator());
		bool withUncommitted = true;
		getIndexList(txn, withUncommitted, indexList);
		for (size_t i = 0; i < indexList.size(); i++) {
			if (indexList[i].cursor_ < row.getRowId()) {
				continue;
			}
			ValueMap valueMap(txn, this, indexList[i]);
			bool isNullValue;
			const void *fieldValue = row.getFields<TimeSeries>(txn, 
				valueMap.getFuncInfo(txn), isNullValue);
			insertValueMap(txn, valueMap, fieldValue, rowArray.getOId(),
				isNullValue);
		}

		if (!txn.isAutoCommit()) {  
			setCreateRowId(txn, rowId);
			setRuntime();
		}
	}
}


void SubTimeSeries::updateRowInternal(TransactionContext& txn,
	MessageRowStore* messageRowStore, RowArray& rowArray, RowId& rowId) {
	RowArray::Row row(rowArray.getRow(), &rowArray);
	rowArray.lock(txn);

	if (!txn.isAutoCommit() && rowArray.isFirstUpdate()) {
		RowScope rowScope(*this, TO_MVCC);
		rowArray.resetFirstUpdate();
		RowArray snapShotRowArray(txn, this);
		snapShotRowArray.initialize(
			txn, rowArray.getRowId(), rowArray.getMaxRowNum());
		rowArray.copyRowArray(txn, snapShotRowArray);

		TransactionId tId = txn.getId();
		MvccRowImage mvccRowImage(MVCC_UPDATE, snapShotRowArray.getOId());
		StackAllocAutoPtr<BtreeMap> mvccMap(
			txn.getDefaultAllocator(), getMvccMap(txn));
		insertMvccMap(txn, mvccMap.get(), tId, mvccRowImage);
		setRuntime();
	}
	{
		RowScope rowScope(*this, TO_NORMAL);

		util::XArray<IndexData> indexList(txn.getDefaultAllocator());
		bool withUncommitted = true;
		getIndexList(txn, withUncommitted, indexList);
		for (size_t i = 0; i < indexList.size(); i++) {
			if (indexList[i].cursor_ < row.getRowId()) {
				continue;
			}
			util::XArray<KeyData> inputKeyFieldList(txn.getDefaultAllocator());
			getFields(txn, messageRowStore,
				*(indexList[i].columnIds_), inputKeyFieldList);
			util::XArray<KeyData> currentKeyFieldList(txn.getDefaultAllocator());
			row.getFields<TimeSeries>(txn, 
				*(indexList[i].columnIds_), currentKeyFieldList);

			bool isMatch = true;
			for (size_t pos = 0; pos < indexList[i].columnIds_->size(); pos++) {
				KeyData &input = inputKeyFieldList[pos];
				KeyData &current = currentKeyFieldList[pos];
				isMatch = ((input.data_ == NULL && current.data_ == NULL) || 
					(input.data_ != NULL && current.data_ != NULL &&
					input.size_ == current.size_ &&
					memcmp(input.data_, current.data_, current.size_) == 0));
				if (!isMatch) {
					break;
				}
			}
			if (!isMatch) {
				ValueMap valueMap(txn, this, indexList[i]);
				TreeFuncInfo *funcInfo = valueMap.getFuncInfo(txn);
				const void *currentValue = &NULL_VALUE;
				bool isCurrentNullValue = 
					(currentKeyFieldList.size() == 1 && currentKeyFieldList[0].data_ == NULL);
				if (!isCurrentNullValue) {
					currentValue = getIndexValue(txn, *(indexList[i].columnIds_),
						funcInfo, currentKeyFieldList);
				}
				removeValueMap(txn, valueMap, currentValue, rowArray.getOId(),
					isCurrentNullValue);
				const void *inputValue = &NULL_VALUE;
				bool isInputNullValue = 
					(inputKeyFieldList.size() == 1 && inputKeyFieldList[0].data_ == NULL);
				if (!isInputNullValue) {
					inputValue = getIndexValue(txn, *(indexList[i].columnIds_),
						funcInfo, inputKeyFieldList);
				}
				insertValueMap(txn, valueMap, inputValue,
					rowArray.getOId(), isInputNullValue);
			}
		}

		rowArray.update(txn, messageRowStore);
	}
	rowId = row.getRowId();
}

void SubTimeSeries::deleteRowInternal(TransactionContext& txn, Timestamp rowKey,
	bool& existing, bool isForceLock) {
	existing = true;
	TermCondition cond(getRowIdColumnType(), getRowIdColumnType(),
		DSExpression::GE, getRowIdColumnId(), &rowKey,
		sizeof(rowKey));
	BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, 2);
	util::XArray<OId> idList(txn.getDefaultAllocator());
	{
		StackAllocAutoPtr<BtreeMap> rowIdMap(
			txn.getDefaultAllocator(), getRowIdMap(txn));
		rowIdMap.get()->search(txn, sc, idList);
	}

	if (idList.size() == 0) {
		existing = false;
		return;
	}
	RowArray rowArray(txn, this);
	bool isOldSchema = rowArray.load(txn, idList[0], this, OBJECT_FOR_UPDATE);
	if (!rowArray.searchNextRowId(rowKey)) {
		existing = false;
		return;
	}
	if (!isForceLock &&
		rowArray.getTxnId() != txn.getId()) {  
		if (txn.getManager().isActiveTransaction(
			txn.getPartitionId(), rowArray.getTxnId())) {
				DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_COL_LOCK_CONFLICT,
					"(txnId=" << txn.getId() << ", rowTxnId=" << rowArray.getTxnId() << ")");
		}
	}

	if (isOldSchema) {
		RowScope rowScope(*this, TO_NORMAL);
		convertRowArraySchema(txn, rowArray, false);
		StackAllocAutoPtr<BtreeMap> rowIdMap(
			txn.getDefaultAllocator(), getRowIdMap(txn));
		idList.clear();
		rowIdMap.get()->search(txn, sc, idList);
	}

	uint16_t halfRowNum = rowArray.getMaxRowNum() / 2;
	uint16_t activeNum = rowArray.getActiveRowNum(halfRowNum + 1);

	DeleteStatus deleteStatus = DELETE_SIMPLE;
	RowArray nextRowArray(txn, this);
	if (activeNum == 1) {
		if (idList.size() > 1) {
			deleteStatus = DELETE_ROW_ARRAY;
		}
	}
	else if (activeNum < halfRowNum && !isOldSchema) {
		if (idList.size() > 1) {
			bool isOldSchema = nextRowArray.load(txn, idList[1], this, OBJECT_FOR_UPDATE);
			if (isForceLock || nextRowArray.getTxnId() == txn.getId() || 
				!txn.getManager().isActiveTransaction(
					txn.getPartitionId(), nextRowArray.getTxnId())) {  
				if (isOldSchema) {
					nextRowArray.begin();
					RowScope rowScope(*this, TO_NORMAL);
					convertRowArraySchema(txn, nextRowArray, false);
				}
				uint32_t nextActiveNum = nextRowArray.getActiveRowNum(rowArray.getMaxRowNum() - activeNum);
				if (activeNum + nextActiveNum + 1 <= rowArray.getMaxRowNum()) {
					deleteStatus = DELETE_MERGE;
				}
			}
		}
	}

	rowArray.lock(txn);
	if (deleteStatus == DELETE_MERGE) {
		nextRowArray.lock(txn);
		if (!txn.isAutoCommit() && nextRowArray.isFirstUpdate()) {
			RowScope rowScope(*this, TO_MVCC);
			nextRowArray.resetFirstUpdate();
			RowArray snapShotRowArray(txn, this);
			snapShotRowArray.initialize(
				txn, nextRowArray.getRowId(), nextRowArray.getMaxRowNum());
			nextRowArray.copyRowArray(txn, snapShotRowArray);

			TransactionId tId = txn.getId();
			MvccRowImage mvccRowImage(MVCC_DELETE, snapShotRowArray.getOId());
			StackAllocAutoPtr<BtreeMap> mvccMap(
				txn.getDefaultAllocator(), getMvccMap(txn));
			insertMvccMap(txn, mvccMap.get(), tId, mvccRowImage);
			setRuntime();
		}
	}

	if (!txn.isAutoCommit() && rowArray.isFirstUpdate()) {
		RowScope rowScope(*this, TO_MVCC);
		rowArray.resetFirstUpdate();
		RowArray snapShotRowArray(txn, this);
		snapShotRowArray.initialize(
			txn, rowArray.getRowId(), rowArray.getMaxRowNum());
		rowArray.copyRowArray(txn, snapShotRowArray);

		TransactionId tId = txn.getId();
		MVCC_IMAGE_TYPE mvccImageType;
		if (deleteStatus == DELETE_ROW_ARRAY) {
			mvccImageType = MVCC_DELETE;
		}
		else {
			mvccImageType = MVCC_UPDATE;
		}
		MvccRowImage mvccRowImage(mvccImageType, snapShotRowArray.getOId());
		StackAllocAutoPtr<BtreeMap> mvccMap(
			txn.getDefaultAllocator(), getMvccMap(txn));
		insertMvccMap(txn, mvccMap.get(), tId, mvccRowImage);
		setRuntime();
	}
	{
		RowScope rowScope(*this, TO_NORMAL);

		util::XArray<IndexData> indexList(txn.getDefaultAllocator());
		bool withUncommitted = true;
		getIndexList(txn, withUncommitted, indexList);
		if (!indexList.empty()) {
			RowArray::Row row(rowArray.getRow(), &rowArray);
			for (size_t i = 0; i < indexList.size(); i++) {
				if (indexList[i].cursor_ < row.getRowId()) {
					continue;
				}

				ValueMap valueMap(txn, this, indexList[i]);
				bool isNullValue;
				const void *fieldValue = row.getFields<TimeSeries>(txn, 
					valueMap.getFuncInfo(txn), isNullValue);
				removeValueMap(txn, valueMap, fieldValue, rowArray.getOId(),
					isNullValue);
			}
		}

		rowArray.remove(txn);

		switch (deleteStatus) {
		case DELETE_SIMPLE:
			break;
		case DELETE_ROW_ARRAY: {
			RowId removeRowId = rowArray.getRowId();
			StackAllocAutoPtr<BtreeMap> rowIdMap(
				txn.getDefaultAllocator(), getRowIdMap(txn));
			removeRowIdMap(
				txn, rowIdMap.get(), &removeRowId, rowArray.getBaseOId());
			rowArray.finalize(txn);
		} break;
		case DELETE_MERGE: {
			merge(txn, rowArray, nextRowArray);
		} break;
		default:
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TIM_PUT_ROW_ERROR, "");
			break;
		}
	}
}

void SubTimeSeries::insertRowInternal(TransactionContext& txn,
	MessageRowStore* messageRowStore, RowArray& rowArray, RowId& rowId) {
	rowArray.lock(txn);
	if (rowArray.end()) {
		RowArray nextRowArray(txn, this);
		bool isOldSchema = false;
		if (rowArray.nextRowArray(txn, nextRowArray, isOldSchema, OBJECT_FOR_UPDATE)) {
			nextRowArray.lock(txn);
			if (isOldSchema) {
				nextRowArray.begin();
				RowScope rowScope(*this, TO_NORMAL);
				convertRowArraySchema(txn, nextRowArray, false);
			}
			if (!txn.isAutoCommit() && nextRowArray.isFirstUpdate()) {
				RowScope rowScope(*this, TO_MVCC);
				nextRowArray.resetFirstUpdate();
				RowArray snapShotRowArray(txn, this);
				snapShotRowArray.initialize(
					txn, nextRowArray.getRowId(), nextRowArray.getMaxRowNum());
				nextRowArray.copyRowArray(txn, snapShotRowArray);

				TransactionId tId = txn.getId();
				MvccRowImage mvccRowImage(
					MVCC_DELETE, snapShotRowArray.getOId());
				StackAllocAutoPtr<BtreeMap> mvccMap(
					txn.getDefaultAllocator(), getMvccMap(txn));
				insertMvccMap(txn, mvccMap.get(), tId, mvccRowImage);
				setRuntime();
			}
		}
	}

	if (!txn.isAutoCommit() && rowArray.isFirstUpdate()) {
		RowScope rowScope(*this, TO_MVCC);
		rowArray.resetFirstUpdate();
		RowArray snapShotRowArray(txn, this);
		snapShotRowArray.initialize(
			txn, rowArray.getRowId(), rowArray.getMaxRowNum());
		rowArray.copyRowArray(txn, snapShotRowArray);

		TransactionId tId = txn.getId();
		MvccRowImage mvccRowImage(MVCC_UPDATE, snapShotRowArray.getOId());
		StackAllocAutoPtr<BtreeMap> mvccMap(
			txn.getDefaultAllocator(), getMvccMap(txn));
		insertMvccMap(txn, mvccMap.get(), tId, mvccRowImage);
		setRuntime();
	}
	{
		RowScope rowScope(*this, TO_NORMAL);

		RowId insertRowId = rowId;
		if (rowArray.getMaxRowNum() == 1) {
			if (rowArray.isFull()) {
				StackAllocAutoPtr<BtreeMap> rowIdMap(
					txn.getDefaultAllocator(), getRowIdMap(txn));
				rowArray.reset();
				rowArray.initialize(txn, insertRowId, getNormalRowArrayNum());
				insertRowIdMap(
					txn, rowIdMap.get(), &insertRowId, rowArray.getBaseOId());
				rowArray.lock(txn);
				rowArray.resetFirstUpdate();
			}
		}
		else {
			if (rowArray.isFull()) {
				RowId splitRowId;
				RowArray splitRowArray(txn, this);
				split(txn, rowArray, insertRowId, splitRowArray, splitRowId);
				if (splitRowId < insertRowId) {
					bool isOldSchema = rowArray.load(
						txn, splitRowArray.getOId(), this, OBJECT_FOR_UPDATE);
					UNUSED_VARIABLE(isOldSchema);
					assert(!isOldSchema);
				}
			}
			else {
				shift(txn, rowArray, false);
			}
		}

		rowArray.insert(txn, messageRowStore, insertRowId);
		RowArray::Row row(rowArray.getRow(), &rowArray);

		util::XArray<IndexData> indexList(txn.getDefaultAllocator());
		bool withUncommitted = true;
		getIndexList(txn, withUncommitted, indexList);
		for (size_t i = 0; i < indexList.size(); i++) {
			if (indexList[i].cursor_ < row.getRowId()) {
				continue;
			}

			ValueMap valueMap(txn, this, indexList[i]);
			bool isNullValue;
			const void *fieldValue = row.getFields<TimeSeries>(txn, 
				valueMap.getFuncInfo(txn), isNullValue);
			insertValueMap(txn, valueMap, fieldValue, rowArray.getOId(),
				isNullValue);
		}
	}
}

void SubTimeSeries::shift(
	TransactionContext& txn, RowArray& rowArray, bool isForce) {
	util::XArray<std::pair<OId, OId> > moveList(txn.getDefaultAllocator());
	util::XArray<std::pair<OId, OId> >::iterator itr;
	rowArray.shift(txn, isForce, moveList);

	updateValueMaps(txn, moveList);
}

void SubTimeSeries::split(TransactionContext& txn, RowArray& rowArray,
	RowId insertRowId, RowArray& splitRowArray, RowId& splitRowId) {
	util::XArray<std::pair<OId, OId> > moveList(txn.getDefaultAllocator());
	util::XArray<std::pair<OId, OId> >::iterator itr;

	StackAllocAutoPtr<BtreeMap> rowIdMap(
		txn.getDefaultAllocator(), getRowIdMap(txn));
	splitRowId = rowArray.getMidRowId();
	splitRowArray.initialize(txn, splitRowId, getNormalRowArrayNum());
	rowArray.split(txn, insertRowId, splitRowArray, splitRowId, moveList);
	splitRowArray.lock(txn);
	splitRowArray.resetFirstUpdate();

	insertRowIdMap(
		txn, rowIdMap.get(), &splitRowId, splitRowArray.getBaseOId());

	updateValueMaps(txn, moveList);
	if (!txn.isAutoCommit()) {  
		setCreateRowId(txn, splitRowId);
		setRuntime();
	}
}

void SubTimeSeries::merge(
	TransactionContext& txn, RowArray& rowArray, RowArray& nextRowArray) {
	util::XArray<std::pair<OId, OId> > moveList(txn.getDefaultAllocator());
	util::XArray<std::pair<OId, OId> >::iterator itr;
	rowArray.merge(txn, nextRowArray, moveList);

	updateValueMaps(txn, moveList);

	RowId rowId = nextRowArray.getRowId();
	StackAllocAutoPtr<BtreeMap> rowIdMap(
		txn.getDefaultAllocator(), getRowIdMap(txn));
	removeRowIdMap(txn, rowIdMap.get(), &rowId, nextRowArray.getBaseOId());
	nextRowArray.finalize(txn);
}

void SubTimeSeries::undoCreateRow(TransactionContext& txn, RowArray& rowArray) {
	util::XArray<IndexData> indexList(txn.getDefaultAllocator());
	bool withUncommitted = true;
	getIndexList(txn, withUncommitted, indexList);
	if (!indexList.empty()) {
		for (size_t i = 0; i < indexList.size(); i++) {
			ValueMap valueMap(txn, this, indexList[i]);
			for (rowArray.begin(); !rowArray.end(); rowArray.next()) {
				RowArray::Row row(rowArray.getRow(), &rowArray);
				if (indexList[i].cursor_ < row.getRowId()) {
					continue;
				}
				bool isNullValue;
				const void *fieldValue = row.getFields<TimeSeries>(txn, 
					valueMap.getFuncInfo(txn), isNullValue);
				removeValueMap(txn, valueMap, fieldValue, rowArray.getOId(),
					isNullValue);
			}
		}
	}
	for (size_t i = 0; i < rowArray.getActiveRowNum(); i++) {
		decrementRowNum();
	}

	RowId removeRowId = rowArray.getRowId();
	StackAllocAutoPtr<BtreeMap> rowIdMap(
		txn.getDefaultAllocator(), getRowIdMap(txn));
	removeRowIdMap(txn, rowIdMap.get(), &removeRowId, rowArray.getBaseOId());
	rowArray.finalize(txn);
}

void SubTimeSeries::undoUpdateRow(
	TransactionContext& txn, RowArray& beforeRowArray) {
	RowArray afterRowArray(txn, this);
	RowId rowId = beforeRowArray.getRowId();

	TermCondition cond(COLUMN_TYPE_TIMESTAMP, COLUMN_TYPE_TIMESTAMP, 
		DSExpression::EQ, ColumnInfo::ROW_KEY_COLUMN_ID, &rowId, sizeof(rowId));
	BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, 1);
	util::XArray<OId> oIdList(txn.getDefaultAllocator());
	{
		StackAllocAutoPtr<BtreeMap> rowIdMap(
			txn.getDefaultAllocator(), getRowIdMap(txn));
		rowIdMap.get()->search(txn, sc, oIdList);
	}
	bool isExistRowArray = false;
	if (!oIdList.empty()) {
		bool isOldSchema = afterRowArray.load(txn, oIdList[0], this, OBJECT_FOR_UPDATE);
		UNUSED_VARIABLE(isOldSchema);
		assert(isOldSchema || !isOldSchema);
		RowId targetRowId = afterRowArray.getRowId();
		if (rowId == targetRowId) {
			isExistRowArray = true;
		}
	}

	util::XArray<IndexData> indexList(txn.getDefaultAllocator());
	bool withUncommitted = true;
	getIndexList(txn, withUncommitted, indexList);
	if (!indexList.empty()) {
		for (size_t i = 0; i < indexList.size(); i++) {
			ValueMap valueMap(txn, this, indexList[i]);
			if (isExistRowArray) {
				for (afterRowArray.begin(); !afterRowArray.end();
					 afterRowArray.next()) {
					RowArray::Row row(afterRowArray.getRow(), &afterRowArray);
					if (indexList[i].cursor_ < row.getRowId()) {
						continue;
					}
					bool isNullValue;
					const void *fieldValue = row.getFields<TimeSeries>(txn, 
						valueMap.getFuncInfo(txn), isNullValue);
					removeValueMap(txn, valueMap, fieldValue,
						afterRowArray.getOId(), isNullValue);
				}
			}
			for (beforeRowArray.begin(); !beforeRowArray.end();
				 beforeRowArray.next()) {
				RowArray::Row row(beforeRowArray.getRow(), &beforeRowArray);
				if (indexList[i].cursor_ < row.getRowId()) {
					continue;
				}
				bool isNullValue;
				const void *fieldValue = row.getFields<TimeSeries>(txn, 
					valueMap.getFuncInfo(txn), isNullValue);
				insertValueMap(txn, valueMap, fieldValue,
					beforeRowArray.getOId(), isNullValue);
			}
		}
	}
	for (size_t i = 0; i < beforeRowArray.getActiveRowNum(); i++) {
		incrementRowNum();
	}

	RowScope rowScope(*this, TO_NORMAL);
	beforeRowArray.moveRowArray(txn);

	if (isExistRowArray) {
		{
			StackAllocAutoPtr<BtreeMap> rowIdMap(
				txn.getDefaultAllocator(), getRowIdMap(txn));
			updateRowIdMap(txn, rowIdMap.get(), &rowId, afterRowArray.getBaseOId(),
				beforeRowArray.getBaseOId());
		}
		if (!beforeRowArray.isLatestSchema()) {
			convertRowArraySchema(txn, beforeRowArray, false);
		}

		for (size_t i = 0; i < afterRowArray.getActiveRowNum(); i++) {
			decrementRowNum();
		}
		afterRowArray.finalize(txn);
	}
	else {
		{
			StackAllocAutoPtr<BtreeMap> rowIdMap(
				txn.getDefaultAllocator(), getRowIdMap(txn));
			insertRowIdMap(
				txn, rowIdMap.get(), &rowId, beforeRowArray.getBaseOId());
		}
	}
}

void SubTimeSeries::createMinimumRowArray(
	TransactionContext& txn, RowArray& rowArray, bool isExistEmptyRowArray) {
	StackAllocAutoPtr<BtreeMap> rowIdMap(
		txn.getDefaultAllocator(), getRowIdMap(txn));
	if (isExistEmptyRowArray) {
		assert(rowArray.getActiveRowNum() == 0);
		RowId rowArrayRowId = rowArray.getRowId();
		if (!txn.isAutoCommit() && rowArray.isFirstUpdate()) {
			RowScope rowScope(*this, TO_MVCC);
			rowArray.resetFirstUpdate();
			RowArray snapShotRowArray(txn, this);
			snapShotRowArray.initialize(
				txn, rowArrayRowId, rowArray.getMaxRowNum());
			rowArray.copyRowArray(txn, snapShotRowArray);

			TransactionId tId = txn.getId();
			MvccRowImage mvccRowImage(MVCC_UPDATE, snapShotRowArray.getOId());
			StackAllocAutoPtr<BtreeMap> mvccMap(
				txn.getDefaultAllocator(), getMvccMap(txn));
			insertMvccMap(txn, mvccMap.get(), tId, mvccRowImage);
			setRuntime();
		}
		removeRowIdMap(
			txn, rowIdMap.get(), &rowArrayRowId, rowArray.getBaseOId());

		rowArray.finalize(txn);
	}

	RowId minimumRowId = 0;
	rowArray.initialize(txn, minimumRowId, getNormalRowArrayNum());
	rowArray.lock(txn);
	rowArray.resetFirstUpdate();
	insertRowIdMap(txn, rowIdMap.get(), &minimumRowId, rowArray.getBaseOId());

	if (!txn.isAutoCommit()) {  
		setCreateRowId(txn, minimumRowId);
		setRuntime();
	}
}

bool SubTimeSeries::isRowOmittableBySsCompression(TransactionContext& txn,
	const CompressionSchema& compressionSchema,
	MessageRowStore* messageRowStore, RowArray& rowArray, Timestamp rowKey) {
	bool isOmittable = true;
	bool isSameValue = false;
	uint32_t columnNum = getColumnNum();

	RowArray checkRowArray(txn, this);
	checkRowArray.load(txn, rowArray.getOId(), this, OBJECT_READ_ONLY);

	if (checkRowArray.getRowNum() == 0) {
		isOmittable = false;
		isSameValue = false;
	}
	else {
		checkRowArray.tail();
		RowArray::Row prevRow(checkRowArray.getRow(), &checkRowArray);
		if (checkRowArray.getRowNum() == 1) {
			isOmittable = false;
		}
		else if (isSsCompressionReady()) {
			RowArray startRowArray(txn, this);
			startRowArray.load(txn, checkRowArray.getOId(), this, OBJECT_READ_ONLY);
			startRowArray.prev();
			RowArray::Row startRow(startRowArray.getRow(), &startRowArray);

			Timestamp startTime = startRow.getRowId();
			Timestamp nowTime = rowKey;
			if (nowTime - startTime >
				compressionSchema.getDurationInfo().timestampDuration_) {
				isOmittable = false;
			}
		}

		isSameValue = true;
		for (uint32_t i = 1; i < columnNum; i++) {
			ColumnInfo& columnInfo = getColumnInfo(i);
			BaseObject baseFieldObject(
				txn.getPartitionId(), *getObjectManager());
			bool inputIsNull = messageRowStore->isNullValue(columnInfo.getColumnId());
			bool prevIsNull = prevRow.isNullValue(columnInfo);
			if (!prevIsNull) {
				prevRow.getField(txn, columnInfo, baseFieldObject);
			}
			if (inputIsNull != prevIsNull || (!inputIsNull && !prevIsNull &&
				ValueProcessor::compare(txn, *getObjectManager(), i,
					messageRowStore,
					baseFieldObject.getCursor<uint8_t>()) != 0)) {
				isSameValue = false;
				break;
			}
		}
	}

	if (isSameValue) {
		if (!isSsCompressionReady()) {
			isOmittable = false;
		}
		setSsCompressionReady(SS_IS_READRY);
	}
	else {
		isOmittable = false;
		setSsCompressionReady(SS_IS_NOT_READRY);
	}

	return isOmittable;
}

bool SubTimeSeries::isRowOmittableByHiCompression(TransactionContext& txn,
	const CompressionSchema& compressionSchema,
	MessageRowStore* messageRowStore, RowArray& rowArray, Timestamp rowKey)

{
	bool isOmittable = true;
	uint32_t columnNum = getColumnNum();
	RowArray checkRowArray(txn, this);
	checkRowArray.load(txn, rowArray.getOId(), this, OBJECT_READ_ONLY);

	DSDCValList dsdcValList(txn, *getObjectManager());
	if (compressionSchema.getHiCompressionNum() > 0) {
		dsdcValList.load(reinterpret_cast<TimeSeriesImage*>(baseContainerImage_)
							 ->hiCompressionStatus_);
	}
	if (checkRowArray.getRowNum() > 1) {
		checkRowArray.tail();
		RowArray::Row prevRow(checkRowArray.getRow(), &checkRowArray);
		RowArray startRowArray(txn, this);
		startRowArray.load(txn, checkRowArray.getOId(), this, OBJECT_READ_ONLY);
		startRowArray.prev();
		RowArray::Row startRow(startRowArray.getRow(), &startRowArray);


		Timestamp startTime = startRow.getRowId();
		Timestamp nowTime = rowKey;
		if (nowTime - startTime >
			compressionSchema.getDurationInfo().timestampDuration_) {
			isOmittable = false;
		}
		else {
			for (uint32_t i = 1; i < columnNum; i++) {
				ColumnInfo& columnInfo = getColumnInfo(i);

				bool inputIsNull = messageRowStore->isNullValue(columnInfo.getColumnId());
				bool startIsNull = startRow.isNullValue(columnInfo);
				bool prevIsNull = prevRow.isNullValue(columnInfo);

				const void* rowStoreField;
				uint32_t size;
				if (!inputIsNull) {
					messageRowStore->getField(i, rowStoreField, size);
				}
				BaseObject baseStartFieldObject(
					txn.getPartitionId(), *getObjectManager());
				if (!startIsNull) {
					startRow.getField(txn, columnInfo, baseStartFieldObject);
				}
				BaseObject basePrevFieldObject(
					txn.getPartitionId(), *getObjectManager());
				if (!prevIsNull) {
					prevRow.getField(txn, columnInfo, basePrevFieldObject);
				}

				if (inputIsNull && startIsNull && prevIsNull) {
				} 
				else if (compressionSchema.isHiCompression(i)) {
					if (inputIsNull || startIsNull || prevIsNull) {
						isOmittable = false;
					} else {

						double startDoubleVal =
							ValueProcessor::getDouble(columnInfo.getColumnType(),
								baseStartFieldObject.getCursor<uint8_t>());
						double nowDoubleVal = ValueProcessor::getDouble(
							columnInfo.getColumnType(), rowStoreField);

						double threshhold, rate, span;
						bool threshholdRelative;
						uint16_t compressionPos;
						compressionSchema.getHiCompressionProperty(i, threshhold,
							rate, span, threshholdRelative, compressionPos);

						DSDCVal dsdcVal = *dsdcValList.get(compressionPos);
						isOmittable = isOmittableByDSDCCheck(threshhold, startTime,
							startDoubleVal, nowTime, nowDoubleVal, dsdcVal);
						dsdcValList.update(compressionPos, &dsdcVal);
					}
				}
				else {
					if (inputIsNull != prevIsNull || (!inputIsNull && !prevIsNull &&
						ValueProcessor::compare(txn, *getObjectManager(),
							columnInfo.getColumnType(),
							baseStartFieldObject.getCursor<uint8_t>(),
							basePrevFieldObject.getCursor<uint8_t>()) != 0)) {
						isOmittable = false;
					}
				}
				if (isOmittable == false) {
					break;
				}
			}
		}
	}
	else {
		isOmittable = false;
	}

	if (isOmittable == false && checkRowArray.getRowNum() > 0) {
		checkRowArray.tail();
		RowArray::Row prevRow(checkRowArray.getRow(), &checkRowArray);

		util::Vector<ColumnId> columnIdList(txn.getDefaultAllocator());
		compressionSchema.getHiCompressionColumnList(columnIdList);
		for (size_t i = 0; i < columnIdList.size(); i++) {
			ColumnId columnId = columnIdList[i];
			ColumnInfo& columnInfo = getColumnInfo(columnId);
			bool inputIsNull = messageRowStore->isNullValue(columnInfo.getColumnId());
			bool prevIsNull = prevRow.isNullValue(columnInfo);
			const void* rowStoreField;
			uint32_t size;
			if (!inputIsNull) {
				messageRowStore->getField(columnId, rowStoreField, size);
			}

			BaseObject basePrevFieldObject(
				txn.getPartitionId(), *getObjectManager());
			if (!prevIsNull) {
				prevRow.getField(txn, columnInfo, basePrevFieldObject);
			}

			if (!inputIsNull && !prevIsNull) {
				double rowStoreDoubleVal = ValueProcessor::getDouble(
					columnInfo.getColumnType(), rowStoreField);

				Timestamp prevTimestamp = prevRow.getRowId();

				double preDoubleVal =
					ValueProcessor::getDouble(columnInfo.getColumnType(),
						basePrevFieldObject.getCursor<uint8_t>());

				double threshhold, rate, span;
				bool threshholdRelative;
				uint16_t compressionPos;
				compressionSchema.getHiCompressionProperty(columnId, threshhold,
					rate, span, threshholdRelative, compressionPos);
				DSDCVal dsdcVal = *dsdcValList.get(compressionPos);
				resetDSDCVal(threshhold, prevTimestamp, preDoubleVal, rowKey,
					rowStoreDoubleVal, dsdcVal);
				dsdcValList.update(compressionPos, &dsdcVal);
			}
		}
	}

	return isOmittable;
}

bool SubTimeSeries::isOmittableByDSDCCheck(double threshold,
	Timestamp startTime, double startVal, Timestamp nowTime, double nowVal,
	DSDCVal& dsdcVal) {
	double timeNowDiff = static_cast<double>(nowTime - startTime);
	assert(timeNowDiff > 0);


	double nowMaxAngle =
		(nowVal + dsdcVal.upperError_ - startVal) / timeNowDiff;


	if (nowMaxAngle < dsdcVal.upperAngle_) {
		dsdcVal.upperAngle_ = nowMaxAngle;  
	}
	else {
		double nowUpperError =
			startVal + (dsdcVal.upperAngle_ * timeNowDiff) - nowVal;
		if (nowUpperError > 0 && nowUpperError < dsdcVal.upperError_) {
			dsdcVal.upperError_ = nowUpperError;  
		}
		else {
			dsdcVal.initialize(threshold);  
			return false;					
		}
	}

	double nowMinAngle =
		(nowVal - dsdcVal.lowerError_ - startVal) / timeNowDiff;


	if (nowMinAngle > dsdcVal.lowerAngle_) {
		dsdcVal.lowerAngle_ = nowMinAngle;  
	}
	else {
		double nowLowerError =
			nowVal - (dsdcVal.lowerAngle_ * timeNowDiff) - startVal;
		if (nowLowerError > 0 && nowLowerError < dsdcVal.lowerError_) {
			dsdcVal.lowerError_ = nowLowerError;  
		}
		else {
			dsdcVal.initialize(threshold);  
			return false;					
		}
	}

	return true;  
}

void SubTimeSeries::resetDSDCVal(double threshold, Timestamp prevTime,
	double prevVal, Timestamp nowTime, double nowVal, DSDCVal& dsdcVal) {
	double timeNowDiff = static_cast<double>(nowTime - prevTime);
	assert(timeNowDiff > 0);


	dsdcVal.upperError_ = threshold;
	dsdcVal.lowerError_ = dsdcVal.upperError_;

	double nowMaxAngle = (nowVal + dsdcVal.upperError_ - prevVal) / timeNowDiff;
	dsdcVal.upperAngle_ = nowMaxAngle;

	double nowMinAngle = (nowVal - dsdcVal.lowerError_ - prevVal) / timeNowDiff;
	dsdcVal.lowerAngle_ = nowMinAngle;

}

void SubTimeSeries::updateSubTimeSeriesImage(TransactionContext& txn) {
	SubTimeSeriesImage subTimeSeriesImage;
	subTimeSeriesImage.mvccMapOId_ = baseContainerImage_->mvccMapOId_;
	subTimeSeriesImage.rowIdMapOId_ = baseContainerImage_->rowIdMapOId_;
	subTimeSeriesImage.startTime_ = startTime_;
	parent_->updateSubTimeSeriesImage(
		txn, SubTimeSeriesInfo(subTimeSeriesImage, position_));
}

void SubTimeSeries::insertRowIdMap(
	TransactionContext& txn, BtreeMap* map, const void* constKey, OId oId) {
	int32_t status = map->insert(txn, constKey, oId);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		baseContainerImage_->rowIdMapOId_ = map->getBaseOId();
		updateSubTimeSeriesImage(txn);
	}
	if ((status & BtreeMap::TAIL_UPDATE) != 0 &&
		position_ == parent_->getBasePos()) {
		parent_->setTailNodeOId(map->getTailNodeOId());
	}
}

void SubTimeSeries::insertMvccMap(TransactionContext& txn, BtreeMap* map,
	TransactionId tId, MvccRowImage& mvccImage) {
	bool isCaseSensitive = true;
	int32_t status =
		map->insert<TransactionId, MvccRowImage>(txn, tId, mvccImage, isCaseSensitive);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		if (isRuntime()) {
			baseContainerImage_->mvccMapOId_ = map->getBaseOId();
			setRuntime();
		}
		else {
			baseContainerImage_->mvccMapOId_ = map->getBaseOId();
		}
		updateSubTimeSeriesImage(txn);
	}
}

void SubTimeSeries::insertValueMap(TransactionContext& txn, ValueMap &valueMap,
	const void* constKey, OId oId, bool isNullVal) {
	int32_t status = valueMap.putValueMap(txn, isNullVal)->insert(txn, constKey, oId);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		valueMap.updateIndexData(txn);
	}
}

void SubTimeSeries::updateRowIdMap(TransactionContext& txn, BtreeMap* map,
	const void* constKey, OId oldOId, OId newOId) {
	int32_t status = map->update(txn, constKey, oldOId, newOId);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		baseContainerImage_->rowIdMapOId_ = map->getBaseOId();
		updateSubTimeSeriesImage(txn);
	}
	if ((status & BtreeMap::TAIL_UPDATE) != 0 &&
		position_ == parent_->getBasePos()) {
		parent_->setTailNodeOId(map->getTailNodeOId());
	}
}

void SubTimeSeries::updateMvccMap(TransactionContext& txn, BtreeMap* map,
	TransactionId tId, MvccRowImage& oldMvccImage, MvccRowImage& newMvccImage) {
	bool isCaseSensitive = true;
	int32_t status = map->update<TransactionId, MvccRowImage>(
		txn, tId, oldMvccImage, newMvccImage, isCaseSensitive);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		if (isRuntime()) {
			baseContainerImage_->mvccMapOId_ = map->getBaseOId();
			setRuntime();
		}
		else {
			baseContainerImage_->mvccMapOId_ = map->getBaseOId();
		}
		updateSubTimeSeriesImage(txn);
	}
}

void SubTimeSeries::updateValueMap(TransactionContext& txn, ValueMap &valueMap,
	const void* constKey, OId oldOId, OId newOId, bool isNullVal) {
	int32_t status = valueMap.putValueMap(txn, isNullVal)->update(txn, constKey, oldOId, newOId);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		valueMap.updateIndexData(txn);
	}
}

void SubTimeSeries::removeRowIdMap(
	TransactionContext& txn, BtreeMap* map, const void* constKey, OId oId) {
	int32_t status = map->remove(txn, constKey, oId);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		baseContainerImage_->rowIdMapOId_ = map->getBaseOId();
		updateSubTimeSeriesImage(txn);
	}
	if ((status & BtreeMap::TAIL_UPDATE) != 0 &&
		position_ == parent_->getBasePos()) {
		parent_->setTailNodeOId(map->getTailNodeOId());
	}
}

void SubTimeSeries::removeMvccMap(TransactionContext& txn, BtreeMap* map,
	TransactionId tId, MvccRowImage& mvccImage) {
	bool isCaseSensitive = true;
	int32_t status =
		map->remove<TransactionId, MvccRowImage>(txn, tId, mvccImage, isCaseSensitive);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		if (isRuntime()) {
			baseContainerImage_->mvccMapOId_ = map->getBaseOId();
			setRuntime();
		}
		else {
			baseContainerImage_->mvccMapOId_ = map->getBaseOId();
		}
		updateSubTimeSeriesImage(txn);
	}
}

void SubTimeSeries::removeValueMap(TransactionContext& txn, ValueMap &valueMap,
	const void* constKey, OId oId, bool isNullVal) {
	int32_t status = valueMap.putValueMap(txn, isNullVal)->remove(txn, constKey, oId);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		valueMap.updateIndexData(txn);
	}
}

void SubTimeSeries::updateIndexData(
	TransactionContext& txn, const IndexData &indexData) {
	indexSchema_->updateIndexData(txn, indexData, position_);
}


util::String SubTimeSeries::getBibInfo(TransactionContext &txn, const char* dbName) {
	return getBibInfoImpl(txn, dbName, position_, false);
}

void SubTimeSeries::getActiveTxnList(
	TransactionContext &txn, util::Set<TransactionId> &txnList) {
	getActiveTxnListImpl(txn, txnList);
}

void SubTimeSeries::getErasableList(TransactionContext &, Timestamp , util::XArray<ArchiveInfo> &) {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
}

ExpireType SubTimeSeries::getExpireType() const {
	return parent_->getExpireType();
}

/*!
	@brief Validates Rows and Indexes
*/
bool SubTimeSeries::validate(TransactionContext&, std::string&) {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
	return false;
}

std::string SubTimeSeries::dump(TransactionContext& txn) {
	return dumpImpl<TimeSeries>(txn);
}


