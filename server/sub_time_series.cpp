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
	TransactionContext&, const char8_t*, ContainerId, OId, MessageSchema*) {
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

	if (getExpirationInfo().duration_ == INT64_MAX) {
		endTime_ = MAX_TIMESTAMP;
	}
	else {
		endTime_ = startTime_ + parent->getDivideDuration() - 1;
	}

	setAllocateStrategy(parent);

	BtreeMap map(txn, *getObjectManager(), getMapAllcateStrategy(), this);
	map.initialize(
		txn, COLUMN_TYPE_TIMESTAMP, true, BtreeMap::TYPE_UNIQUE_RANGE_KEY);
	baseContainerImage_->rowIdMapOId_ = map.getBaseOId();

	BtreeMap mvccMap(txn, *getObjectManager(), getMapAllcateStrategy(), this);
	mvccMap.initialize<TransactionId, MvccRowImage>(
		txn, COLUMN_TYPE_OID, false, BtreeMap::TYPE_SINGLE_KEY);
	baseContainerImage_->mvccMapOId_ = mvccMap.getBaseOId();

	util::XArray<IndexData> indexList(txn.getDefaultAllocator());
	parent->getIndexList(txn, indexList);
	for (size_t i = 0; i < indexList.size(); i++) {
		IndexInfo indexInfo(indexList[i].columnId_, indexList[i].mapType_);
		createIndex(txn, indexInfo);
	}

	updateSubTimeSeriesImage(txn);
	setAllocateStrategy(parent);
}

/*!
	@brief Free Objects related to SubTimeSeries
*/
void SubTimeSeries::finalize(TransactionContext& txn) {
	if (baseContainerImage_->rowIdMapOId_ != UNDEF_OID) {
		BtreeMap::BtreeCursor btreeCursor;
		StackAllocAutoPtr<BtreeMap> rowIdMap(
			txn.getDefaultAllocator(), getRowIdMap(txn));
		while (1) {
			util::StackAllocator::Scope scope(txn.getDefaultAllocator());
			util::XArray<OId> idList(txn.getDefaultAllocator());
			util::XArray<OId>::iterator itr;
			int32_t getAllStatus = rowIdMap.get()->getAll(
				txn, PARTIAL_RESULT_SIZE, idList, btreeCursor);

			for (itr = idList.begin(); itr != idList.end(); itr++) {
				RowArray rowArray(txn, *itr,
					reinterpret_cast<SubTimeSeries*>(this), OBJECT_FOR_UPDATE);
				rowArray.finalize(txn);
			}
			if (getAllStatus == GS_SUCCESS) {
				break;
			}
		}

		rowIdMap.get()->finalize(txn);
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
				if (itr->second.type_ == MVCC_UPDATE ||
					itr->second.type_ == MVCC_DELETE) {
					RowArray rowArray(txn, itr->second.snapshotRowOId_, this,
						OBJECT_FOR_UPDATE);
					rowArray.finalize(txn);
				}
			}
			if (getAllStatus == GS_SUCCESS) {
				break;
			}
		}
		mvccMap.get()->finalize(txn);
	}

	finalizeIndex(txn);

	baseContainerImage_->indexSchemaOId_ = UNDEF_OID;
	baseContainerImage_->rowIdMapOId_ = UNDEF_OID;
	baseContainerImage_->mvccMapOId_ = UNDEF_OID;
	baseContainerImage_->rowNum_ = 0;
	updateSubTimeSeriesImage(txn);
}

/*!
	@brief Creates a specifed type of index on the specified Column
*/
void SubTimeSeries::createIndex(TransactionContext& txn, IndexInfo& indexInfo) {
	ColumnInfo& columnInfo = getColumnInfo(indexInfo.columnId);
	bool isUnique = false;
	if (definedRowKey() &&
		indexInfo.columnId == ColumnInfo::ROW_KEY_COLUMN_ID) {
		isUnique = true;
	}
	indexSchema_->createIndexData(txn, indexInfo.columnId, indexInfo.mapType,
		columnInfo.getColumnType(), this, position_, isUnique);

	indexInsert(txn, indexInfo);
	updateSubTimeSeriesImage(txn);
}

/*!
	@brief Creates a specifed type of index on the specified Column
*/
void SubTimeSeries::dropIndex(TransactionContext& txn, IndexInfo& indexInfo) {
	if (indexInfo.columnId >= getColumnNum()) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_COLUMN_ID_INVALID, "");
	}

	if (hasIndex(txn, indexInfo.columnId, indexInfo.mapType)) {
		indexSchema_->dropIndexData(
			txn, indexInfo.columnId, indexInfo.mapType, this, position_, true);
	}
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
	BtreeMap::SearchContext sc(ColumnInfo::ROW_KEY_COLUMN_ID, NULL, 0, true,
		&rowKey, 0, true, 0, NULL, 1);
	util::XArray<OId> oIdList(txn.getDefaultAllocator());
	searchRowIdIndex(txn, sc, oIdList, ORDER_UNDEFINED);
	if (!oIdList.empty()) {
		RowArray rowArray(txn, oIdList[0], this, OBJECT_FOR_UPDATE);
		RowArray::Row row(rowArray.getRow(), &rowArray);
		if (!isForceLock &&
			row.getTxnId() != txn.getId()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_COL_NOLOCK, "");
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
	BtreeMap::SearchContext sc(
		UNDEF_COLUMNID, &tId, sizeof(tId), 0, NULL, MAX_RESULT_SIZE);
	mvccMap.get()->search<TransactionId, MvccRowImage, MvccRowImage>(
		txn, sc, mvccList);

	if (!mvccList.empty()) {
		getObjectManager()->setDirty(
			txn.getPartitionId(), baseContainerImage_->mvccMapOId_);
		for (itr = mvccList.begin(); itr != mvccList.end(); itr++) {
			if (itr->type_ == MVCC_CREATE) {
				RowId startKey = itr->firstCreateRowId_;
				RowId endKey = itr->lastCreateRowId_;
				if (startKey != INITIAL_ROWID) {
					BtreeMap::SearchContext sc(ColumnInfo::ROW_KEY_COLUMN_ID,
						&startKey, 0, true, &endKey, 0, true, 0, NULL,
						MAX_RESULT_SIZE);
					util::XArray<OId> oIdList(txn.getDefaultAllocator());
					util::XArray<OId>::iterator rowItr;
					{
						StackAllocAutoPtr<BtreeMap> rowIdMap(
							txn.getDefaultAllocator(), getRowIdMap(txn));
						rowIdMap.get()->search(txn, sc, oIdList);
					}
					for (rowItr = oIdList.begin(); rowItr != oIdList.end();
						 rowItr++) {
						RowArray rowArray(txn, *rowItr,
							reinterpret_cast<SubTimeSeries*>(this),
							OBJECT_FOR_UPDATE);
						if (rowArray.getTxnId() == tId &&
							!rowArray.isFirstUpdate()) {
							undoCreateRow(txn, rowArray);
						}
					}
				}
				removeMvccMap(txn, mvccMap.get(), tId, *itr);
			}
		}
		for (itr = mvccList.begin(); itr != mvccList.end(); itr++) {
			if (itr->type_ == MVCC_SELECT) {
				removeMvccMap(txn, mvccMap.get(), tId, *itr);
			}
			else if (itr->type_ != MVCC_CREATE) {
				RowArray rowArray(txn, itr->snapshotRowOId_,
					reinterpret_cast<SubTimeSeries*>(this), OBJECT_FOR_UPDATE);
				undoUpdateRow(txn, rowArray);
				removeMvccMap(txn, mvccMap.get(), tId, *itr);
			}
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
	BtreeMap::SearchContext sc(
		UNDEF_COLUMNID, &tId, sizeof(tId), 0, NULL, MAX_RESULT_SIZE);
	mvccMap.get()->search<TransactionId, MvccRowImage, MvccRowImage>(
		txn, sc, mvccList);

	if (!mvccList.empty()) {
		getObjectManager()->setDirty(
			txn.getPartitionId(), baseContainerImage_->mvccMapOId_);
		for (itr = mvccList.begin(); itr != mvccList.end(); itr++) {
			switch (itr->type_) {
			case MVCC_CREATE:
			case MVCC_SELECT:
				{ removeMvccMap(txn, mvccMap.get(), tId, *itr); }
				break;
			case MVCC_UPDATE:
			case MVCC_DELETE:
				{
					RowArray rowArray(txn, itr->snapshotRowOId_,
						reinterpret_cast<SubTimeSeries*>(this),
						OBJECT_FOR_UPDATE);
					rowArray.finalize(txn);
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
	@brief Change new Column layout
*/
void SubTimeSeries::changeSchema(TransactionContext& txn,
	BaseContainer& newContainer, util::XArray<uint32_t>& copyColumnMap) {
	changeSchemaRecord(txn, newContainer, copyColumnMap);
}

/*!
	@brief Search Btree Index of RowId
*/
void SubTimeSeries::searchRowIdIndex(TransactionContext& txn,
	BtreeMap::SearchContext& sc, util::XArray<OId>& resultList,
	OutputOrder outputOrder) {
	const Operator *op1, *op2;
	bool isValid = getKeyCondition(txn, sc, op1, op2);
	if (!isValid) {
		return;
	}

	util::XArray<OId> mvccList(txn.getDefaultAllocator());
	ResultSize limitBackup = sc.limit_;
	sc.limit_ = MAX_RESULT_SIZE;
	searchMvccMap<TimeSeries, BtreeMap::SearchContext>(txn, sc, mvccList);
	sc.limit_ = limitBackup;

	ContainerValue containerValue(txn, *getObjectManager());
	util::XArray<OId> oIdList(txn.getDefaultAllocator());
	util::XArray<OId>::iterator itr;

	limitBackup = sc.limit_;
	if (sc.conditionNum_ > 0 || !isExclusive()) {
		sc.limit_ = MAX_RESULT_SIZE;
	}
	else if (sc.limit_ < MAX_RESULT_SIZE) {
		sc.limit_ += 1;
	}
	StackAllocAutoPtr<BtreeMap> rowIdMap(
		txn.getDefaultAllocator(), getRowIdMap(txn));
	rowIdMap.get()->search(txn, sc, oIdList, outputOrder);
	sc.limit_ = limitBackup;

	for (itr = oIdList.begin(); itr != oIdList.end(); itr++) {
		RowArray rowArray(txn, *itr, reinterpret_cast<SubTimeSeries*>(this),
			OBJECT_READ_ONLY);
		if (!isExclusive() && txn.getId() != rowArray.getTxnId() &&
			!rowArray.isFirstUpdate() &&
			txn.getManager().isActiveTransaction(
				txn.getPartitionId(), rowArray.getTxnId())) {
			continue;
		}
		if (outputOrder != ORDER_DESCENDING) {
			for (rowArray.begin(); !rowArray.end(); rowArray.next()) {
				RowArray::Row row(rowArray.getRow(), &rowArray);
				Timestamp rowId = row.getRowId();
				if (((op1 != 0) &&
						!(*op1)(txn, reinterpret_cast<const uint8_t*>(&rowId),
							sizeof(Timestamp),
							reinterpret_cast<const uint8_t*>(sc.startKey_),
							sc.startKeySize_)) ||
					((op2 != 0) &&
						!(*op2)(txn, reinterpret_cast<const uint8_t*>(&rowId),
							sizeof(Timestamp),
							reinterpret_cast<const uint8_t*>(sc.endKey_),
							sc.endKeySize_))) {
					continue;
				}

				bool isMatch = true;
				for (uint32_t c = 0; c < sc.conditionNum_; c++) {
					if (!row.isMatch(
							txn, sc.conditionList_[c], containerValue)) {
						isMatch = false;
						break;
					}
				}
				if (isMatch) {
					resultList.push_back(rowArray.getOId());
					if (resultList.size() == sc.limit_) {
						break;
					}
				}
			}
		}
		else {
			for (bool isRend = rowArray.tail(); isRend;
				 isRend = rowArray.prev()) {
				RowArray::Row row(rowArray.getRow(), &rowArray);
				Timestamp rowId = row.getRowId();
				if (((op1 != 0) &&
						!(*op1)(txn, reinterpret_cast<const uint8_t*>(&rowId),
							sizeof(Timestamp),
							reinterpret_cast<const uint8_t*>(sc.startKey_),
							sc.startKeySize_)) ||
					((op2 != 0) &&
						!(*op2)(txn, reinterpret_cast<const uint8_t*>(&rowId),
							sizeof(Timestamp),
							reinterpret_cast<const uint8_t*>(sc.endKey_),
							sc.endKeySize_))) {
					continue;
				}
				bool isMatch = true;
				for (uint32_t c = 0; c < sc.conditionNum_; c++) {
					if (!row.isMatch(
							txn, sc.conditionList_[c], containerValue)) {
						isMatch = false;
						break;
					}
				}
				if (isMatch) {
					resultList.push_back(rowArray.getOId());
					if (resultList.size() == sc.limit_) {
						break;
					}
				}
			}
		}
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
				uint8_t* objectRowField;
				row.getRowIdField(objectRowField);

				SortKey sortKey;
				sortKey.set(txn, targetType, objectRowField, *itr);

				mvccSortKeyList.push_back(sortKey);
			}

			const Operator* sortOp;
			if (outputOrder == ORDER_ASCENDING) {
				sortOp = &ltTable[targetType][targetType];
			}
			else {
				sortOp = &gtTable[targetType][targetType];
			}
			std::sort(mvccSortKeyList.begin(), mvccSortKeyList.end(),
				SortPred(txn, sortOp, targetType));

			for (itr = resultList.begin(); itr != resultList.end(); itr++) {
				rowArray.load(txn, *itr, reinterpret_cast<SubTimeSeries*>(this),
					OBJECT_READ_ONLY);
				RowArray::Row row(rowArray.getRow(), &rowArray);
				uint8_t* objectRowField;
				row.getRowIdField(objectRowField);

				SortKey sortKey;
				sortKey.set(txn, targetType, objectRowField, *itr);
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
	if (resultList.size() > sc.limit_) {
		resultList.resize(sc.limit_);
	}
}

/*!
	@brief Search Btree Index of RowId
*/
void SubTimeSeries::searchRowIdIndex(TransactionContext&, uint64_t, uint64_t,
	util::XArray<RowId>&, util::XArray<OId>&, uint64_t&) {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
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
			BtreeMap::SearchContext sc(
				UNDEF_COLUMNID, &tId, sizeof(tId), 0, NULL, MAX_RESULT_SIZE);
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

/*!
	@brief Newly creates or updates a Row, based on the specified Row object and
   also the Row key specified as needed
*/
void SubTimeSeries::putRow(TransactionContext& txn,
	InputMessageRowStore* inputMessageRowStore, RowId& rowId,
	DataStore::PutStatus& status, PutRowOption putRowOption) {
	util::StackAllocator::Scope scope(txn.getDefaultAllocator());

	PutMode mode = UNDEFINED_STATUS;
	Timestamp rowKey = inputMessageRowStore->getField<COLUMN_TYPE_TIMESTAMP>(
		ColumnInfo::ROW_KEY_COLUMN_ID);
	RowArray rowArray(txn, this);

	OId tailOId = UNDEF_OID;
	Timestamp tailRowKey = UNDEF_TIMESTAMP;
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
			rowArray.load(txn, tailOId, this, OBJECT_FOR_UPDATE);
			bool isExist = rowArray.tail();
			RowArray::Row row(rowArray.getRow(), &rowArray);
			tailRowKey = row.getTime();
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
				Timestamp headRowKey = row.getTime();
				if (headRowKey <= rowKey) {
					if (rowArray.searchRowId(rowKey)) {
						mode = UPDATE;
					}
					else {
						mode = INSERT;
					}
				}
			}
		}
		if (mode == UNDEFINED_STATUS) {
			BtreeMap::SearchContext sc(ColumnInfo::ROW_KEY_COLUMN_ID, NULL, 0,
				true, &rowKey, 0, true, 0, NULL, 1);
			util::XArray<OId> oIdList(txn.getDefaultAllocator());
			StackAllocAutoPtr<BtreeMap> rowIdMap(
				txn.getDefaultAllocator(), getRowIdMap(txn));
			rowIdMap.get()->search(txn, sc, oIdList, ORDER_DESCENDING);
			if (!oIdList.empty()) {
				rowArray.load(txn, oIdList[0], this, OBJECT_FOR_UPDATE);
				if (rowArray.searchRowId(rowKey)) {
					mode = UPDATE;
				}
				else {
					mode = INSERT;
				}
			}
			else {
				createMinimumRowArray(txn, rowArray, false);
				mode = APPEND;
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
		status = DataStore::UPDATE;
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
			uint16_t smallRowArrayNum =
				calcRowArrayNum(SMALL_ROW_ARRAY_MAX_SIZE);
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
			shift(txn, rowArray, true);
			rowArray.tail();
		}
	}

	rowArray.append(txn, messageRowStore, rowId);
	RowArray::Row row(rowArray.getRow(), &rowArray);

	util::XArray<IndexData> indexList(txn.getDefaultAllocator());
	getIndexList(txn, indexList);
	for (size_t i = 0; i < indexList.size(); i++) {
		StackAllocAutoPtr<BaseIndex> map(txn.getDefaultAllocator(),
			getIndex(txn, indexList[i].mapType_, indexList[i].columnId_));
		if (map.get() == NULL) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
		}

		ColumnInfo& columnInfo = getColumnInfo(indexList[i].columnId_);
		BaseObject baseFieldObject(txn.getPartitionId(), *getObjectManager());
		row.getField(txn, columnInfo, baseFieldObject);
		insertValueMap(txn, map.get(), baseFieldObject.getCursor<uint8_t>(),
			rowArray.getOId(), indexList[i].columnId_, indexList[i].mapType_);
	}

	if (!txn.isAutoCommit()) {  
		setCreateRowId(txn, rowId);
		setRuntime();
	}
}


void SubTimeSeries::updateRowInternal(TransactionContext& txn,
	MessageRowStore* messageRowStore, RowArray& rowArray, RowId& rowId) {
	RowArray::Row row(rowArray.getRow(), &rowArray);
	rowId = row.getRowId();
	rowArray.lock(txn);

	if (!txn.isAutoCommit() && rowArray.isFirstUpdate()) {
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

	util::XArray<IndexData> indexList(txn.getDefaultAllocator());
	getIndexList(txn, indexList);
	if (!indexList.empty()) {
		for (size_t i = 0; i < indexList.size(); i++) {
			ColumnId columnId = indexList[i].columnId_;

			ColumnInfo& columnInfo = getColumnInfo(columnId);

			BaseObject baseFieldObject(
				txn.getPartitionId(), *getObjectManager());
			row.getField(txn, columnInfo, baseFieldObject);
			int32_t result =
				ValueProcessor::compare(txn, *getObjectManager(), columnId,
					messageRowStore, baseFieldObject.getCursor<uint8_t>());
			if (result != 0) {
				StackAllocAutoPtr<BaseIndex> map(txn.getDefaultAllocator(),
					getIndex(txn, indexList[i].mapType_,
													 indexList[i].columnId_));
				if (map.get() == NULL) {
					GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
				}
				ColumnInfo& columnInfo = getColumnInfo(indexList[i].columnId_);
				BaseObject baseFieldObject(
					txn.getPartitionId(), *getObjectManager());
				row.getField(txn, columnInfo, baseFieldObject);
				removeValueMap(txn, map.get(),
					baseFieldObject.getCursor<uint8_t>(), rowArray.getOId(),
					indexList[i].columnId_, indexList[i].mapType_);

				const uint8_t* messageRowField;
				uint32_t messageRowFieldSize;
				messageRowStore->getField(columnInfo.getColumnId(),
					messageRowField, messageRowFieldSize);
				insertValueMap(txn, map.get(), messageRowField,
					rowArray.getOId(), indexList[i].columnId_,
					indexList[i].mapType_);
			}
		}
	}

	rowArray.update(txn, messageRowStore);
}

void SubTimeSeries::deleteRowInternal(TransactionContext& txn, Timestamp rowKey,
	bool& existing, bool isForceLock) {
	existing = true;
	BtreeMap::SearchContext sc(ColumnInfo::ROW_KEY_COLUMN_ID, &rowKey, 0, true,
		NULL, 0, true, 0, NULL, 2);
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
	RowArray rowArray(txn, idList[0], reinterpret_cast<SubTimeSeries*>(this),
		OBJECT_FOR_UPDATE);
	if (!rowArray.searchRowId(rowKey)) {
		existing = false;
		return;
	}
	if (!isForceLock &&
		rowArray.getTxnId() != txn.getId()) {  
		GS_THROW_USER_ERROR(GS_ERROR_DS_COL_NOLOCK, "");
	}
	uint32_t activeNum = rowArray.getActiveRowNum();

	DeleteStatus deleteStatus = DELETE_SIMPLE;
	RowArray nextRowArray(txn, this);
	if (activeNum == 1) {
		if (idList.size() > 1) {
			deleteStatus = DELETE_ROW_ARRAY;
		}
	}
	else if (activeNum < rowArray.getMaxRowNum() / 2) {
		if (idList.size() > 1) {
			nextRowArray.load(txn, idList[1], this, OBJECT_FOR_UPDATE);
			uint32_t nextActiveNum = nextRowArray.getActiveRowNum();
			if (activeNum + nextActiveNum + 1 <= rowArray.getMaxRowNum()) {
				deleteStatus = DELETE_MERGE;
			}
		}
	}

	rowArray.lock(txn);
	if (deleteStatus == DELETE_MERGE) {
		nextRowArray.lock(txn);
		if (!txn.isAutoCommit() && nextRowArray.isFirstUpdate()) {
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

	util::XArray<IndexData> indexList(txn.getDefaultAllocator());
	getIndexList(txn, indexList);
	if (!indexList.empty()) {
		RowArray::Row row(rowArray.getRow(), &rowArray);
		for (size_t i = 0; i < indexList.size(); i++) {
			StackAllocAutoPtr<BaseIndex> map(txn.getDefaultAllocator(),
				getIndex(txn, indexList[i].mapType_, indexList[i].columnId_));
			if (map.get() == NULL) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
			}
			ColumnInfo& columnInfo = getColumnInfo(indexList[i].columnId_);
			BaseObject baseFieldObject(
				txn.getPartitionId(), *getObjectManager());
			row.getField(txn, columnInfo, baseFieldObject);
			removeValueMap(txn, map.get(), baseFieldObject.getCursor<uint8_t>(),
				rowArray.getOId(), indexList[i].columnId_,
				indexList[i].mapType_);
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

void SubTimeSeries::insertRowInternal(TransactionContext& txn,
	MessageRowStore* messageRowStore, RowArray& rowArray, RowId& rowId) {
	rowArray.lock(txn);
	if (rowArray.end()) {
		RowArray nextRowArray(txn, this);
		if (rowArray.nextRowArray(txn, nextRowArray, OBJECT_FOR_UPDATE)) {
			nextRowArray.lock(txn);
			if (!txn.isAutoCommit() && nextRowArray.isFirstUpdate()) {
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
				rowArray.load(
					txn, splitRowArray.getOId(), this, OBJECT_FOR_UPDATE);
			}
		}
		else {
			shift(txn, rowArray, false);
		}
	}

	rowArray.insert(txn, messageRowStore, insertRowId);
	RowArray::Row row(rowArray.getRow(), &rowArray);

	util::XArray<IndexData> indexList(txn.getDefaultAllocator());
	getIndexList(txn, indexList);
	for (size_t i = 0; i < indexList.size(); i++) {
		StackAllocAutoPtr<BaseIndex> map(txn.getDefaultAllocator(),
			getIndex(txn, indexList[i].mapType_, indexList[i].columnId_));
		if (map.get() == NULL) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
		}
		ColumnInfo& columnInfo = getColumnInfo(indexList[i].columnId_);
		BaseObject baseFieldObject(txn.getPartitionId(), *getObjectManager());
		row.getField(txn, columnInfo, baseFieldObject);
		insertValueMap(txn, map.get(), baseFieldObject.getCursor<uint8_t>(),
			rowArray.getOId(), indexList[i].columnId_, indexList[i].mapType_);
	}
}

void SubTimeSeries::shift(
	TransactionContext& txn, RowArray& rowArray, bool isForce) {
	util::XArray<std::pair<OId, OId> > moveList(txn.getDefaultAllocator());
	util::XArray<std::pair<OId, OId> >::iterator itr;
	rowArray.shift(txn, isForce, moveList);
	util::XArray<IndexData> indexList(txn.getDefaultAllocator());
	getIndexList(txn, indexList);
	if (!indexList.empty()) {
		for (size_t i = 0; i < indexList.size(); i++) {
			StackAllocAutoPtr<BaseIndex> map(txn.getDefaultAllocator(),
				getIndex(txn, indexList[i].mapType_, indexList[i].columnId_));
			if (map.get() == NULL) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
			}
			ColumnInfo& columnInfo = getColumnInfo(indexList[i].columnId_);

			RowArray moveRowArray(txn, this);
			for (itr = moveList.begin(); itr != moveList.end(); itr++) {
				moveRowArray.load(txn, itr->second, this, OBJECT_READ_ONLY);
				RowArray::Row row(moveRowArray.getRow(), &moveRowArray);
				BaseObject baseFieldObject(
					txn.getPartitionId(), *getObjectManager());
				row.getField(txn, columnInfo, baseFieldObject);
				updateValueMap(txn, map.get(),
					baseFieldObject.getCursor<uint8_t>(), itr->first,
					itr->second, indexList[i].columnId_, indexList[i].mapType_);
			}
		}
	}
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

	util::XArray<IndexData> indexList(txn.getDefaultAllocator());
	getIndexList(txn, indexList);
	if (!indexList.empty()) {
		for (size_t i = 0; i < indexList.size(); i++) {
			StackAllocAutoPtr<BaseIndex> map(txn.getDefaultAllocator(),
				getIndex(txn, indexList[i].mapType_, indexList[i].columnId_));
			if (map.get() == NULL) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
			}
			ColumnInfo& columnInfo = getColumnInfo(indexList[i].columnId_);

			RowArray moveRowArray(txn, this);
			for (itr = moveList.begin(); itr != moveList.end(); itr++) {
				moveRowArray.load(txn, itr->second, this, OBJECT_READ_ONLY);
				RowArray::Row row(moveRowArray.getRow(), &moveRowArray);
				BaseObject baseFieldObject(
					txn.getPartitionId(), *getObjectManager());
				row.getField(txn, columnInfo, baseFieldObject);
				updateValueMap(txn, map.get(),
					baseFieldObject.getCursor<uint8_t>(), itr->first,
					itr->second, indexList[i].columnId_, indexList[i].mapType_);
			}
		}
	}
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

	util::XArray<IndexData> indexList(txn.getDefaultAllocator());
	getIndexList(txn, indexList);
	if (!indexList.empty()) {
		for (size_t i = 0; i < indexList.size(); i++) {
			StackAllocAutoPtr<BaseIndex> map(txn.getDefaultAllocator(),
				getIndex(txn, indexList[i].mapType_, indexList[i].columnId_));
			if (map.get() == NULL) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
			}
			ColumnInfo& columnInfo = getColumnInfo(indexList[i].columnId_);

			RowArray moveRowArray(txn, this);
			for (itr = moveList.begin(); itr != moveList.end(); itr++) {
				moveRowArray.load(txn, itr->second, this, OBJECT_READ_ONLY);
				RowArray::Row row(moveRowArray.getRow(), &moveRowArray);
				BaseObject baseFieldObject(
					txn.getPartitionId(), *getObjectManager());
				row.getField(txn, columnInfo, baseFieldObject);
				updateValueMap(txn, map.get(),
					baseFieldObject.getCursor<uint8_t>(), itr->first,
					itr->second, indexList[i].columnId_, indexList[i].mapType_);
			}
		}
	}

	RowId rowId = nextRowArray.getRowId();
	StackAllocAutoPtr<BtreeMap> rowIdMap(
		txn.getDefaultAllocator(), getRowIdMap(txn));
	removeRowIdMap(txn, rowIdMap.get(), &rowId, nextRowArray.getBaseOId());
	nextRowArray.finalize(txn);
}

void SubTimeSeries::undoCreateRow(TransactionContext& txn, RowArray& rowArray) {
	util::XArray<IndexData> indexList(txn.getDefaultAllocator());
	getIndexList(txn, indexList);
	if (!indexList.empty()) {
		for (size_t i = 0; i < indexList.size(); i++) {
			StackAllocAutoPtr<BaseIndex> map(txn.getDefaultAllocator(),
				getIndex(txn, indexList[i].mapType_, indexList[i].columnId_));
			if (map.get() == NULL) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
			}
			ColumnInfo& columnInfo = getColumnInfo(indexList[i].columnId_);
			for (rowArray.begin(); !rowArray.end(); rowArray.next()) {
				RowArray::Row row(rowArray.getRow(), &rowArray);
				BaseObject baseFieldObject(
					txn.getPartitionId(), *getObjectManager());
				row.getField(txn, columnInfo, baseFieldObject);
				removeValueMap(txn, map.get(),
					baseFieldObject.getCursor<uint8_t>(), rowArray.getOId(),
					indexList[i].columnId_, indexList[i].mapType_);
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

	BtreeMap::SearchContext sc(UNDEF_COLUMNID, &rowId, 0, 0, NULL, 1);
	util::XArray<OId> oIdList(txn.getDefaultAllocator());
	StackAllocAutoPtr<BtreeMap> rowIdMap(
		txn.getDefaultAllocator(), getRowIdMap(txn));
	rowIdMap.get()->search(txn, sc, oIdList);
	bool isExistRowArray = false;
	if (!oIdList.empty()) {
		afterRowArray.load(txn, oIdList[0], this, OBJECT_FOR_UPDATE);
		RowId targetRowId = afterRowArray.getRowId();
		if (rowId == targetRowId) {
			isExistRowArray = true;
		}
	}

	util::XArray<IndexData> indexList(txn.getDefaultAllocator());
	getIndexList(txn, indexList);
	if (!indexList.empty()) {
		for (size_t i = 0; i < indexList.size(); i++) {
			StackAllocAutoPtr<BaseIndex> map(txn.getDefaultAllocator(),
				getIndex(txn, indexList[i].mapType_, indexList[i].columnId_));
			if (map.get() == NULL) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
			}
			ColumnInfo& columnInfo = getColumnInfo(indexList[i].columnId_);
			if (isExistRowArray) {
				for (afterRowArray.begin(); !afterRowArray.end();
					 afterRowArray.next()) {
					RowArray::Row row(afterRowArray.getRow(), &afterRowArray);
					BaseObject baseFieldObject(
						txn.getPartitionId(), *getObjectManager());
					row.getField(txn, columnInfo, baseFieldObject);
					removeValueMap(txn, map.get(),
						baseFieldObject.getCursor<uint8_t>(),
						afterRowArray.getOId(), indexList[i].columnId_,
						indexList[i].mapType_);
				}
			}
			for (beforeRowArray.begin(); !beforeRowArray.end();
				 beforeRowArray.next()) {
				RowArray::Row row(beforeRowArray.getRow(), &beforeRowArray);
				BaseObject baseFieldObject(
					txn.getPartitionId(), *getObjectManager());
				row.getField(txn, columnInfo, baseFieldObject);
				insertValueMap(txn, map.get(),
					baseFieldObject.getCursor<uint8_t>(),
					beforeRowArray.getOId(), indexList[i].columnId_,
					indexList[i].mapType_);
			}
		}
	}
	if (isExistRowArray) {
		updateRowIdMap(txn, rowIdMap.get(), &rowId, afterRowArray.getBaseOId(),
			beforeRowArray.getBaseOId());
		for (size_t i = 0; i < afterRowArray.getActiveRowNum(); i++) {
			decrementRowNum();
		}
		afterRowArray.finalize(txn);
	}
	else {
		insertRowIdMap(
			txn, rowIdMap.get(), &rowId, beforeRowArray.getBaseOId());
	}
	for (size_t i = 0; i < beforeRowArray.getActiveRowNum(); i++) {
		incrementRowNum();
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
	int32_t status =
		map->insert<TransactionId, MvccRowImage>(txn, tId, mvccImage);
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

void SubTimeSeries::insertValueMap(TransactionContext& txn, BaseIndex* map,
	const void* constKey, OId oId, ColumnId columnId, MapType mapType) {
	int32_t status = map->insert(txn, constKey, oId);
	if (mapType == MAP_TYPE_BTREE && (status & BtreeMap::ROOT_UPDATE) != 0) {
		IndexData indexData;
		indexData.columnId_ = columnId;
		indexData.mapType_ = mapType;
		indexData.oId_ = map->getBaseOId();
		updateIndexData(txn, indexData);
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
	int32_t status = map->update<TransactionId, MvccRowImage>(
		txn, tId, oldMvccImage, newMvccImage);
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

void SubTimeSeries::updateValueMap(TransactionContext& txn, BaseIndex* map,
	const void* constKey, OId oldOId, OId newOId, ColumnId columnId,
	MapType mapType) {
	int32_t status = map->update(txn, constKey, oldOId, newOId);
	if (mapType == MAP_TYPE_BTREE && (status & BtreeMap::ROOT_UPDATE) != 0) {
		IndexData indexData;
		indexData.columnId_ = columnId;
		indexData.mapType_ = mapType;
		indexData.oId_ = map->getBaseOId();
		updateIndexData(txn, indexData);
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
	int32_t status =
		map->remove<TransactionId, MvccRowImage>(txn, tId, mvccImage);
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

void SubTimeSeries::removeValueMap(TransactionContext& txn, BaseIndex* map,
	const void* constKey, OId oId, ColumnId columnId, MapType mapType) {
	int32_t status = map->remove(txn, constKey, oId);
	if (mapType == MAP_TYPE_BTREE && (status & BtreeMap::ROOT_UPDATE) != 0) {
		IndexData indexData;
		indexData.columnId_ = columnId;
		indexData.mapType_ = mapType;
		indexData.oId_ = map->getBaseOId();
		updateIndexData(txn, indexData);
	}
}

void SubTimeSeries::updateIndexData(
	TransactionContext& txn, IndexData indexData) {
	indexSchema_->updateIndexData(txn, indexData, position_);
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

