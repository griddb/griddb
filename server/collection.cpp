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
	@brief Implementation of Collection
*/
#include "collection.h"
#include "hash_map.h"
#include "util/trace.h"
#include "btree_map.h"
#include "data_store.h"
#include "data_store_common.h"
#include "transaction_manager.h"
#include "gs_error.h"
#include "message_schema.h"
#include "value_processor.h"

const bool Collection::indexMapTable[][MAP_TYPE_NUM] = {
	{true, true, false},  
	{true, true, false},  
	{true, true, false},  
	{true, true, false},  
	{true, true, false},  
	{true, true, false},  
	{true, true, false},  
	{true, true, false},  
	{true, true, false},  
	{false, false, false},  
	{false, false, false}  
};


/*!
	@brief Allocate Collection Object
*/
void Collection::initialize(TransactionContext& txn) {
	baseContainerImage_ =
		BaseObject::allocate<CollectionImage>(sizeof(CollectionImage),
			getMetaAllcateStrategy(), getBaseOId(), OBJECT_TYPE_COLLECTION);
	baseContainerImage_->containerType_ =
		static_cast<int8_t>(COLLECTION_CONTAINER);
	baseContainerImage_->status_ = 0;  
	baseContainerImage_->normalRowArrayNum_ = 0;
	baseContainerImage_->containerId_ = UNDEF_CONTAINERID;
	baseContainerImage_->containerNameOId_ = UNDEF_OID;
	baseContainerImage_->rowIdMapOId_ = UNDEF_OID;
	baseContainerImage_->mvccMapOId_ = UNDEF_OID;
	baseContainerImage_->columnSchemaOId_ = UNDEF_OID;
	baseContainerImage_->indexSchemaOId_ = UNDEF_OID;
	baseContainerImage_->triggerListOId_ = UNDEF_OID;
	baseContainerImage_->lastLsn_ = UNDEF_LSN;
	baseContainerImage_->rowNum_ = 0;
	baseContainerImage_->versionId_ = 0;

	reinterpret_cast<CollectionImage*>(baseContainerImage_)->maxRowId_ = -1;
	reinterpret_cast<CollectionImage*>(baseContainerImage_)->padding2_ = 0;
	reinterpret_cast<CollectionImage*>(baseContainerImage_)->padding3_ = 0;
	reinterpret_cast<CollectionImage*>(baseContainerImage_)->padding4_ = 0;
	reinterpret_cast<CollectionImage*>(baseContainerImage_)->padding5_ = 0;

	rowImageSize_ = 0;
	metaAllocateStrategy_ = AllocateStrategy();
	rowAllocateStrategy_ = AllocateStrategy();
	mapAllocateStrategy_ = AllocateStrategy();
}

/*!
	@brief Set Collection Schema
*/
void Collection::set(TransactionContext& txn, const char8_t* containerName,
	ContainerId containerId, OId columnSchemaOId,
	MessageSchema* containerSchema) {
	baseContainerImage_->containerId_ = containerId;

	uint32_t containerNameSize = static_cast<uint32_t>(strlen(containerName));

	BaseObject stringObject(txn.getPartitionId(), *getObjectManager());
	char* stringPtr = stringObject.allocate<char>(containerNameSize + 1,
		getMetaAllcateStrategy(), baseContainerImage_->containerNameOId_,
		OBJECT_TYPE_VARIANT);
	memcpy(stringPtr, containerName, containerNameSize + 1);

	baseContainerImage_->columnSchemaOId_ = columnSchemaOId;
	commonContainerSchema_ =
		ALLOC_NEW(txn.getDefaultAllocator()) ShareValueList(
			txn, *getObjectManager(), baseContainerImage_->columnSchemaOId_);

	columnSchema_ =
		commonContainerSchema_->get<ColumnSchema>(META_TYPE_COLUMN_SCHEMA);

	indexSchema_ = ALLOC_NEW(txn.getDefaultAllocator())
		IndexSchema(txn, *getObjectManager(), getMetaAllcateStrategy());
	indexSchema_->initialize(txn, IndexSchema::INITIALIZE_RESERVE_NUM);
	baseContainerImage_->indexSchemaOId_ = indexSchema_->getBaseOId();

	rowFixedDataSize_ = calcRowFixedDataSize();
	rowImageSize_ = calcRowImageSize(rowFixedDataSize_);

	baseContainerImage_->normalRowArrayNum_ =
		calcRowArrayNum(ROW_ARRAY_MAX_SIZE);

	setAllocateStrategy();

	BtreeMap map(txn, *getObjectManager(), getMapAllcateStrategy(), this);
	map.initialize(
		txn, COLUMN_TYPE_TIMESTAMP, true, BtreeMap::TYPE_UNIQUE_RANGE_KEY);
	baseContainerImage_->rowIdMapOId_ = map.getBaseOId();

	BtreeMap mvccMap(txn, *getObjectManager(), getMapAllcateStrategy(), this);
	mvccMap.initialize<TransactionId, MvccRowImage>(
		txn, COLUMN_TYPE_OID, false, BtreeMap::TYPE_SINGLE_KEY);
	baseContainerImage_->mvccMapOId_ = mvccMap.getBaseOId();

	if (containerSchema->getRowKeyColumnId() != UNDEF_COLUMNID) {
		IndexInfo indexInfo(ColumnInfo::ROW_KEY_COLUMN_ID, MAP_TYPE_BTREE);
		createIndex(txn, indexInfo);
	}

	GS_TRACE_INFO(BASE_CONTAINER, GS_TRACE_DS_CON_DATA_AFFINITY_DEFINED,
		"ContainerName = " << containerName << ", Affinity = "
						   << containerSchema->getAffinityStr()
						   << ", AffinityHashVal = "
						   << calcAffnityGroupId(getAffinity()));
}

/*!
	@brief Free Objects related to Collection
*/
void Collection::finalize(TransactionContext& txn) {
	try {
		setDirty();
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
					RowArray rowArray(txn, *itr, this, OBJECT_FOR_UPDATE);
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
				util::XArray<std::pair<TransactionId, MvccRowImage> >::iterator
					itr;
				int32_t getAllStatus =
					mvccMap.get()->getAll<TransactionId, MvccRowImage>(
						txn, PARTIAL_RESULT_SIZE, idList, btreeCursor);

				for (itr = idList.begin(); itr != idList.end(); itr++) {
					if (itr->second.type_ == MVCC_UPDATE ||
						itr->second.type_ == MVCC_DELETE) {
						RowArray rowArray(txn, itr->second.snapshotRowOId_,
							this, OBJECT_FOR_UPDATE);
						rowArray.finalize(txn);
					}
				}
				if (getAllStatus == GS_SUCCESS) {
					break;
				}
			}
			mvccMap.get()->finalize(txn);
		}

		commonContainerSchema_->reset();  
		getDataStore()->removeColumnSchema(
			txn, txn.getPartitionId(), baseContainerImage_->columnSchemaOId_);

		finalizeIndex(txn);

		OId triggerOId = getTriggerOId();
		if (triggerOId != UNDEF_OID) {
			getDataStore()->removeTrigger(
				txn, txn.getPartitionId(), triggerOId);
		}

		getObjectManager()->free(
			txn.getPartitionId(), baseContainerImage_->containerNameOId_);
		baseContainerImage_->containerNameOId_ = UNDEF_OID;

		BaseObject::finalize();
	}
	catch (std::exception& e) {
		handleUpdateError(txn, e, GS_ERROR_DS_DS_DROP_COLLECTION_FAILED);
	}
}

/*!
	@brief Creates a specifed type of index on the specified Column
*/
void Collection::createIndex(TransactionContext& txn, IndexInfo& indexInfo) {
	try {
		GS_TRACE_INFO(COLLECTION, GS_TRACE_DS_CON_CREATE_INDEX,
			"Collection Id = " << getContainerId()
							   << " columnId = " << indexInfo.columnId
							   << " type = " << indexInfo.mapType);

		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not create index. container's status is invalid.");
		}

		if (indexInfo.columnId >= getColumnNum()) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_COLUMN_ID_INVALID, "");
		}
		if (!isSupportIndex(indexInfo)) {
			GS_THROW_USER_ERROR(
				GS_ERROR_CM_NOT_SUPPORTED, "not support this index type");
		}

		if (hasIndex(txn, indexInfo.columnId, indexInfo.mapType)) {
			return;
		}

		ColumnInfo& columnInfo = getColumnInfo(indexInfo.columnId);

		bool isUnique = false;
		if (definedRowKey() &&
			indexInfo.columnId == ColumnInfo::ROW_KEY_COLUMN_ID) {
			isUnique = true;
		}
		if (indexSchema_->createIndexInfo(
				txn, indexInfo.columnId, indexInfo.mapType)) {
			replaceIndexSchema(txn);
		}
		indexSchema_->createIndexData(txn, indexInfo.columnId,
			indexInfo.mapType, columnInfo.getColumnType(), this,
			UNDEF_CONTAINER_POS, isUnique);

		indexInsert(txn, indexInfo);
	}
	catch (std::exception& e) {
		handleUpdateError(txn, e, GS_ERROR_DS_COL_CREATE_INDEX_FAILED);
	}
}

/*!
	@brief Creates a specifed type of index on the specified Column
*/
void Collection::dropIndex(TransactionContext& txn, IndexInfo& indexInfo) {
	try {
		GS_TRACE_DEBUG(COLLECTION, GS_TRACE_DS_CON_DROP_INDEX,
			"Collection Id = "
				<< getContainerId() << " columnId = " << indexInfo.columnId
				<< " type = " << static_cast<uint32_t>(indexInfo.mapType));

		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not delete index. container's status is invalid.");
		}

		if (indexInfo.columnId >= getColumnNum()) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_COLUMN_ID_INVALID, "");
		}

		if (hasIndex(txn, indexInfo.columnId, indexInfo.mapType)) {
			indexSchema_->dropIndexData(txn, indexInfo.columnId,
				indexInfo.mapType, this, UNDEF_CONTAINER_POS, true);
			indexSchema_->dropIndexInfo(
				txn, indexInfo.columnId, indexInfo.mapType);
		}
	}
	catch (std::exception& e) {
		handleUpdateError(txn, e, GS_ERROR_DS_COL_DROP_INDEX_FAILED);
	}
}

/*!
	@brief Deletes a Row corresponding to the specified Row key
*/
void Collection::deleteRow(TransactionContext& txn, uint32_t rowKeySize,
	const uint8_t* rowKey, RowId& rowId, bool& existing) {
	try {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		setDirty();

		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not delete. container's status is invalid.");
		}

		if (!definedRowKey()) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_COL_ROWKEY_UNDEFINED, "");
		}
		RowArray rowArray(txn, this);
		if (!isUnique(txn, rowKeySize, rowKey, rowArray)) {
			deleteRowInternal(txn, rowArray, rowId);
			existing = true;
			decrementRowNum();
		}
		else {
			rowId = UNDEF_ROWID;
			existing = false;
		}
	}
	catch (std::exception& e) {
		handleUpdateError(txn, e, GS_ERROR_DS_COL_DELETE_ROW_FAILED);
	}
}

void Collection::deleteRow(
	TransactionContext& txn, RowId rowId, bool& existing, bool isForceLock) {
	try {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		setDirty();

		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not delete. container's status is invalid.");
		}

		BtreeMap::SearchContext sc(UNDEF_COLUMNID, &rowId, 0, 0, NULL, 1);
		util::XArray<OId> oIdList(txn.getDefaultAllocator());
		searchRowIdIndex(txn, sc, oIdList, ORDER_UNDEFINED);
		if (!oIdList.empty()) {
			RowArray rowArray(txn, oIdList[0], this, OBJECT_FOR_UPDATE);
			RowArray::Row row(rowArray.getRow(), &rowArray);
			if (!isForceLock &&
				row.getTxnId() != txn.getId()) {  
				GS_THROW_USER_ERROR(GS_ERROR_DS_COL_NOLOCK, "");
			}
			RowId dummyRowId;
			deleteRowInternal(txn, rowArray, dummyRowId);
			existing = true;
			decrementRowNum();
		}
		else {
			existing = false;
		}
	}
	catch (std::exception& e) {
		handleUpdateError(txn, e, GS_ERROR_DS_COL_DELETE_ROW_FAILED);
	}
}

void Collection::updateRow(TransactionContext& txn, uint32_t rowSize,
	const uint8_t* rowData, RowId rowId, DataStore::PutStatus& status,
	bool isForceLock) {
	try {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		setDirty();

		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not delete. container's status is invalid.");
		}

		InputMessageRowStore inputMessageRowStore(
			getDataStore()->getValueLimitConfig(), getColumnInfoList(),
			getColumnNum(), const_cast<uint8_t*>(rowData), rowSize, 1,
			rowFixedDataSize_);
		inputMessageRowStore.next();

		BtreeMap::SearchContext sc(UNDEF_COLUMNID, &rowId, 0, 0, NULL, 1);
		util::XArray<OId> oIdList(txn.getDefaultAllocator());
		searchRowIdIndex(txn, sc, oIdList, ORDER_UNDEFINED);
		if (!oIdList.empty()) {
			RowArray rowArray(txn, oIdList[0], this, OBJECT_FOR_UPDATE);
			RowArray::Row row(rowArray.getRow(), &rowArray);

			if (definedRowKey()) {
				BaseObject baseFieldObject(
					txn.getPartitionId(), *getObjectManager());
				row.getField(txn, getColumnInfo(ColumnInfo::ROW_KEY_COLUMN_ID),
					baseFieldObject);
				if (ValueProcessor::compare(txn, *getObjectManager(),
						ColumnInfo::ROW_KEY_COLUMN_ID, &inputMessageRowStore,
						baseFieldObject.getCursor<uint8_t>()) != 0) {
					GS_THROW_USER_ERROR(GS_ERROR_DS_COL_ROWKEY_INVALID,
						"Row key is not same value");
				}
			}
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
	}
	catch (std::exception& e) {
		handleUpdateError(txn, e, GS_ERROR_DS_COL_UPDATE_ROW_FAILED);
	}
}

/*!
	@brief Rolls back the result of transaction
*/
void Collection::abort(TransactionContext& txn) {
	abortInternal(txn, txn.getId());
}

void Collection::abortInternal(TransactionContext& txn, TransactionId tId) {
	try {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());

		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not delete. container's status is invalid.");
		}

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
			setDirty();
			for (itr = mvccList.begin(); itr != mvccList.end(); itr++) {
				if (itr->type_ == MVCC_CREATE) {
					RowId startKey = itr->firstCreateRowId_;
					RowId endKey = itr->lastCreateRowId_;
					if (startKey != INITIAL_ROWID) {
						BtreeMap::SearchContext sc(UNDEF_COLUMNID, &startKey, 0,
							true, &endKey, 0, true, 0, NULL, MAX_RESULT_SIZE);
						util::XArray<OId> oIdList(txn.getDefaultAllocator());
						util::XArray<OId>::iterator rowItr;
						{
							StackAllocAutoPtr<BtreeMap> rowIdMap(
								txn.getDefaultAllocator(), getRowIdMap(txn));
							rowIdMap.get()->search(txn, sc, oIdList);
						}
						for (rowItr = oIdList.begin(); rowItr != oIdList.end();
							 rowItr++) {
							RowArray rowArray(
								txn, *rowItr, this, OBJECT_FOR_UPDATE);
							for (rowArray.begin(); !rowArray.end();
								 rowArray.next()) {
								RowArray::Row row(rowArray.getRow(), &rowArray);
								if (row.getTxnId() == tId &&
									!row.isFirstUpdate()) {
									undoCreateRow(txn, rowArray);
									if (rowArray.getActiveRowNum() == 0) {
										RowId rowId = rowArray.getRowId();
										StackAllocAutoPtr<BtreeMap> rowIdMap(
											txn.getDefaultAllocator(),
											getRowIdMap(txn));
										removeRowIdMap(txn, rowIdMap.get(),
											&rowId, rowArray.getBaseOId());
										rowArray.finalize(txn);
										break;
									}
								}
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
					RowArray rowArray(
						txn, itr->snapshotRowOId_, this, OBJECT_FOR_UPDATE);
					undoUpdateRow(txn, rowArray);
					removeMvccMap(txn, mvccMap.get(), tId, *itr);
				}
			}
		}
	}
	catch (std::exception& e) {
		handleUpdateError(txn, e, GS_ERROR_DS_COL_ABORT_FAILED);
	}
}

/*!
	@brief Commits the result of transaction
*/
void Collection::commit(TransactionContext& txn) {
	try {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());

		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not delete. container's status is invalid.");
		}

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
			setDirty();
			for (itr = mvccList.begin(); itr != mvccList.end(); itr++) {
				switch (itr->type_) {
				case MVCC_CREATE:
				case MVCC_SELECT:
					{ removeMvccMap(txn, mvccMap.get(), tId, *itr); }
					break;
				case MVCC_UPDATE:
				case MVCC_DELETE:
					{
						RowArray rowArray(
							txn, itr->snapshotRowOId_, this, OBJECT_FOR_UPDATE);
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
	}
	catch (std::exception& e) {
		handleUpdateError(txn, e, GS_ERROR_DS_COL_COMMIT_FAILED);
	}
}


/*!
	@brief Change new Column layout
*/
void Collection::changeSchema(TransactionContext& txn,
	BaseContainer& newContainer, util::XArray<uint32_t>& copyColumnMap) {
	try {
		setDirty();

		newContainer.setTriggerOId(getTriggerOId());
		setTriggerOId(UNDEF_OID);

		for (uint32_t columnId = 0; columnId < copyColumnMap.size();
			 columnId++) {
			uint32_t oldColumnId = copyColumnMap[columnId];
			if (oldColumnId != UNDEF_COLUMNID) {
				if (hasIndex(txn, oldColumnId, MAP_TYPE_BTREE)) {
					IndexInfo indexInfo(columnId, MAP_TYPE_BTREE);
					newContainer.createIndex(txn, indexInfo);
				}
				if (hasIndex(txn, oldColumnId, MAP_TYPE_HASH)) {
					IndexInfo indexInfo(columnId, MAP_TYPE_HASH);
					newContainer.createIndex(txn, indexInfo);
				}
			}
		}
		changeSchemaRecord(txn, newContainer, copyColumnMap);
	}
	catch (std::exception& e) {
		handleUpdateError(
			txn, e, GS_ERROR_DS_DS_CHANGE_COLLECTION_SCHEMA_FAILED);
	}
}

/*!
	@brief Check if Container has data of uncommited transaction
*/
bool Collection::hasUncommitedTransaction(TransactionContext& txn) {
	StackAllocAutoPtr<BtreeMap> mvccMap(
		txn.getDefaultAllocator(), getMvccMap(txn));
	if (mvccMap.get()->isEmpty()) {
		return false;
	}
	else {
		return true;
	}
}

/*!
	@brief Search Btree Index of RowId
*/
void Collection::searchRowIdIndex(TransactionContext& txn,
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
	searchMvccMap<Collection, BtreeMap::SearchContext>(txn, sc, mvccList);
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
		RowArray rowArray(txn, *itr, this, OBJECT_READ_ONLY);
		if (outputOrder != ORDER_DESCENDING) {
			for (rowArray.begin(); !rowArray.end(); rowArray.next()) {
				RowArray::Row row(rowArray.getRow(), &rowArray);
				if (!isExclusive() && txn.getId() != row.getTxnId() &&
					!row.isFirstUpdate() &&
					txn.getManager().isActiveTransaction(
						txn.getPartitionId(), row.getTxnId())) {
					continue;
				}
				containerValue.set(row.getRowId());
				if (((op1 != 0) &&
						!(*op1)(txn, containerValue.getValue().data(),
							containerValue.getValue().size(),
							reinterpret_cast<const uint8_t*>(sc.startKey_),
							sc.startKeySize_)) ||
					((op2 != 0) &&
						!(*op2)(txn, containerValue.getValue().data(),
							containerValue.getValue().size(),
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
				if (!isExclusive() && txn.getId() != row.getTxnId() &&
					!row.isFirstUpdate() &&
					txn.getManager().isActiveTransaction(
						txn.getPartitionId(), row.getTxnId())) {
					continue;
				}
				containerValue.set(row.getRowId());
				if (((op1 != 0) &&
						!(*op1)(txn, containerValue.getValue().data(),
							containerValue.getValue().size(),
							reinterpret_cast<const uint8_t*>(sc.startKey_),
							sc.startKeySize_)) ||
					((op2 != 0) &&
						!(*op2)(txn, containerValue.getValue().data(),
							containerValue.getValue().size(),
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

			RowArray rowArray(txn, this);
			ColumnType targetType = COLUMN_TYPE_ROWID;
			util::XArray<SortKey> indexSortKeyList(txn.getDefaultAllocator());
			util::XArray<SortKey> mvccSortKeyList(txn.getDefaultAllocator());
			for (itr = mvccList.begin(); itr != mvccList.end(); itr++) {
				rowArray.load(txn, *itr, this, OBJECT_READ_ONLY);
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
				rowArray.load(txn, *itr, this, OBJECT_READ_ONLY);
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
void Collection::searchRowIdIndex(TransactionContext& txn, uint64_t start,
	uint64_t limit, util::XArray<RowId>& rowIdList,
	util::XArray<OId>& resultList, uint64_t& skipped) {
	if (rowIdList.empty()) {
		return;
	}

	skipped = 0;
	if (limit > rowIdList.size() - start) {
		resultList.resize(rowIdList.size() - start, UNDEF_OID);
	}
	else {
		resultList.resize(limit, UNDEF_OID);
	}

	util::XArray<std::pair<RowId, int64_t> > sortRowIdList(
		txn.getDefaultAllocator());
	{
		for (size_t i = start; i < rowIdList.size() && i < start + limit; i++) {
			sortRowIdList.push_back(
				std::pair<RowId, int64_t>(rowIdList[i], i - start));
		}
		std::sort(sortRowIdList.begin(), sortRowIdList.end());
	}
	RowId minRowId = sortRowIdList.front().first;
	RowId maxRowId = sortRowIdList.back().first;

	BtreeMap::SearchContext sc(UNDEF_COLUMNID, &minRowId, 0, true, &maxRowId, 0,
		true, 0, NULL, MAX_RESULT_SIZE);
	util::XArray<KeyValue<RowId, OId> > keyValueList(txn.getDefaultAllocator());

	StackAllocAutoPtr<BtreeMap> rowIdMap(
		txn.getDefaultAllocator(), getRowIdMap(txn));
	rowIdMap.get()->search<RowId, OId, KeyValue<RowId, OId> >(
		txn, sc, keyValueList, ORDER_ASCENDING);

	util::XArray<OId> mvccList(txn.getDefaultAllocator());
	searchMvccMap<Collection, BtreeMap::SearchContext>(txn, sc, mvccList);
	util::Map<RowId, OId> mvccRowIdMap(txn.getDefaultAllocator());
	for (util::XArray<OId>::iterator mvccItr = mvccList.begin();
		 mvccItr != mvccList.end(); mvccItr++) {
		RowArray mvccRowArray(txn, *mvccItr, this, OBJECT_READ_ONLY);
		RowArray::Row mvccRow(mvccRowArray.getRow(), &mvccRowArray);
		mvccRowIdMap.insert(
			std::make_pair(mvccRow.getRowId(), mvccRowArray.getOId()));
	}

	util::XArray<std::pair<RowId, int64_t> >::iterator currentItr =
		sortRowIdList.begin();
	util::XArray<KeyValue<RowId, OId> >::iterator keyValueItr;
	util::Map<RowId, OId>::iterator mvccRowIdMapItr;
	for (currentItr = sortRowIdList.begin(), keyValueItr = keyValueList.begin();
		 currentItr != sortRowIdList.end() &&
		 keyValueItr != keyValueList.end();) {
		RowId startRowId = keyValueItr->key_;  
		RowId endRowId = (keyValueItr + 1 != keyValueList.end())
							 ? (keyValueItr + 1)->key_
							 : MAX_ROWID;  
		RowId currentRowId = currentItr->first;
		if (currentRowId >= startRowId && currentRowId < endRowId) {
			RowArray rowArray(txn, keyValueItr->value_, this, OBJECT_READ_ONLY);
			for (rowArray.begin();
				 !rowArray.end() && currentRowId < endRowId;) {
				RowArray::Row row(rowArray.getRow(), &rowArray);
				RowId rowId = row.getRowId();
				if (rowId < currentRowId) {
					rowArray.next();  
				}
				else {
					OId targetOId = UNDEF_OID;
					if (rowId == currentRowId) {
						if (!isExclusive() && txn.getId() != row.getTxnId() &&
							!row.isFirstUpdate() &&
							txn.getManager().isActiveTransaction(
								txn.getPartitionId(), row.getTxnId())) {
							mvccRowIdMapItr = mvccRowIdMap.find(currentRowId);
							if (mvccRowIdMapItr != mvccRowIdMap.end()) {
								targetOId = mvccRowIdMapItr->second;
							}
						}
						else {
							targetOId = rowArray.getOId();
							rowArray.next();  
						}
					}
					else {
						mvccRowIdMapItr = mvccRowIdMap.find(currentRowId);
						if (mvccRowIdMapItr != mvccRowIdMap.end()) {
							targetOId = mvccRowIdMapItr->second;
						}
					}
					resultList[currentItr->second] = targetOId;
					if (targetOId == UNDEF_OID) {
						skipped++;
					}

					currentItr++;
					if (currentItr == sortRowIdList.end()) {
						break;
					}
					currentRowId = currentItr->first;
				}
			}
			if (rowArray.end()) {
				keyValueItr++;
			}
		}
		else if (currentRowId < startRowId) {
			mvccRowIdMapItr = mvccRowIdMap.find(currentRowId);
			if (mvccRowIdMapItr == mvccRowIdMap.end()) {
				resultList[currentItr->second] = UNDEF_OID;
				skipped++;
			}
			else {
				resultList[currentItr->second] = mvccRowIdMapItr->second;
			}
			currentItr++;
		}
		else {
			keyValueItr++;
		}
	}
	for (; currentItr != sortRowIdList.end(); currentItr++) {
		RowId currentRowId = currentItr->first;
		mvccRowIdMapItr = mvccRowIdMap.find(currentRowId);
		if (mvccRowIdMapItr == mvccRowIdMap.end()) {
			resultList[currentItr->second] = UNDEF_OID;
			skipped++;
		}
		else {
			resultList[currentItr->second] = mvccRowIdMapItr->second;
		}
		currentItr++;
	}
	if (skipped > 0) {
		for (size_t outPos = 0, curPos = 0; curPos < resultList.size();
			 curPos++) {
			if (resultList[curPos] != UNDEF_OID) {
				resultList[outPos++] = resultList[curPos];
			}
		}
		resultList.resize(resultList.size() - skipped);
	}
}



/*!
	@brief Lock Rows
*/
void Collection::lockRowList(
	TransactionContext& txn, util::XArray<RowId>& rowIdList) {
	try {
		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not lock. container's status is invalid.");
		}
		for (size_t i = 0; i < rowIdList.size(); i++) {
			util::StackAllocator::Scope scope(txn.getDefaultAllocator());
			BtreeMap::SearchContext sc(
				UNDEF_COLUMNID, &rowIdList[i], 0, 0, NULL, 1);
			util::XArray<OId> oIdList(txn.getDefaultAllocator());
			searchRowIdIndex(txn, sc, oIdList, ORDER_UNDEFINED);
			if (!oIdList.empty()) {
				RowArray rowArray(txn, oIdList[0], this, OBJECT_FOR_UPDATE);
				RowArray::Row row(rowArray.getRow(), &rowArray);
				row.lock(txn);
			}
			else {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_COL_GET_LOCK_ID_INVALID,
					"rowId" << rowIdList[i] << "is not found,  ");
			}
		}
	}
	catch (std::exception& e) {
		handleSearchError(txn, e, GS_ERROR_DS_COL_GET_LOCK_ID_INVALID);
	}
}

/*!
	@brief Check if Row is locked
*/
bool Collection::isLocked(
	TransactionContext& txn, uint32_t rowKeySize, const uint8_t* rowKey) {
	try {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		if (!definedRowKey()) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_COL_ROWKEY_UNDEFINED, "");
		}

		RowArray rowArray(txn, this);
		if (isUnique(txn, rowKeySize, rowKey, rowArray)) {
			RowArray::Row row(rowArray.getRow(), &rowArray);
			if (txn.getManager().isActiveTransaction(
					txn.getPartitionId(), row.getTxnId())) {
				return true;
			}
			else {
				return false;
			}
		}
		else {
			GS_THROW_USER_ERROR(
				GS_ERROR_DS_COL_ROWKEY_INVALID, "rowKey not exist");
		}
	}
	catch (std::exception& e) {
		handleSearchError(txn, e, GS_ERROR_DS_COL_GET_LOCK_ID_INVALID);
	}

	return false;
}


void Collection::putRow(TransactionContext& txn,
	InputMessageRowStore* inputMessageRowStore, RowId& rowId,
	DataStore::PutStatus& status, PutRowOption putRowOption) {
	util::StackAllocator::Scope scope(txn.getDefaultAllocator());
	PutMode mode = UNDEFINED_STATUS;
	const uint8_t* rowKey;
	uint32_t rowKeySize;
	inputMessageRowStore->getField(
		ColumnInfo::ROW_KEY_COLUMN_ID, rowKey, rowKeySize);

	RowArray rowArray(txn, this);
	if (!definedRowKey() || isUnique(txn, rowKeySize, rowKey, rowArray)) {
		StackAllocAutoPtr<BtreeMap> rowIdMap(
			txn.getDefaultAllocator(), getRowIdMap(txn));
		OId tailOId = rowIdMap.get()->getTail(txn);
		if (tailOId != UNDEF_OID) {
			rowArray.load(txn, tailOId, this, OBJECT_FOR_UPDATE);
			rowArray.tail();
		}
		mode = APPEND;
	}
	else {
		mode = UPDATE;
	}
	switch (mode) {
	case APPEND:
		if (putRowOption == PUT_UPDATE_ONLY) {
			GS_THROW_USER_ERROR(GS_ERROR_CON_PUT_ROW_OPTION_INVALID,
				"update row, but row not exists");
		}
		appendRowInternal(txn, inputMessageRowStore, rowArray, rowId);
		status = DataStore::CREATE;
		incrementRowNum();
		break;
	case UPDATE:
		if (putRowOption == PUT_INSERT_ONLY) {
			GS_THROW_USER_ERROR(GS_ERROR_CON_PUT_ROW_OPTION_INVALID,
				"insert row, but row exists");
		}
		updateRowInternal(txn, inputMessageRowStore, rowArray, rowId);
		status = DataStore::UPDATE;
		break;
	default:
		break;
	}
}

bool Collection::isUnique(TransactionContext& txn, uint32_t rowKeySize,
	const uint8_t* rowKey, RowArray& rowArray) {
	bool isFound = false;

	ColumnInfo& keyColumnInfo = getColumnInfo(ColumnInfo::ROW_KEY_COLUMN_ID);
	if (keyColumnInfo.getColumnType() == COLUMN_TYPE_STRING) {
		StringCursor stringCusor(const_cast<uint8_t*>(rowKey));
		rowKey = stringCusor.str();
		rowKeySize = stringCusor.stringLength();
		if (rowKeySize >
			getDataStore()
				->getValueLimitConfig()
				.getLimitSmallSize()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_COL_ROWKEY_INVALID, "");
		}
	}
	else if (keyColumnInfo.getColumnType() == COLUMN_TYPE_TIMESTAMP) {
		if (!ValueProcessor::validateTimestamp(
				*reinterpret_cast<const Timestamp*>(rowKey))) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_COL_ROWKEY_INVALID,
				"Timestamp of rowKey out of range (rowKey="
					<< *reinterpret_cast<const Timestamp*>(rowKey) << ")");
		}
	}

	OId oId = UNDEF_OID;
	IndexTypes indexBit = getIndexTypes(txn, ColumnInfo::ROW_KEY_COLUMN_ID);
	if (hasIndex(indexBit, MAP_TYPE_HASH)) {
		StackAllocAutoPtr<HashMap> hmap(txn.getDefaultAllocator(),
			reinterpret_cast<HashMap*>(getIndex(
				txn, MAP_TYPE_HASH, ColumnInfo::ROW_KEY_COLUMN_ID)));
		hmap.get()->search(txn, rowKey, rowKeySize, oId);
		isFound = (oId != UNDEF_OID);
	}
	else if (hasIndex(indexBit, MAP_TYPE_BTREE)) {
		StackAllocAutoPtr<BtreeMap> bmap(txn.getDefaultAllocator(),
			reinterpret_cast<BtreeMap*>(getIndex(
				txn, MAP_TYPE_BTREE, ColumnInfo::ROW_KEY_COLUMN_ID)));
		bmap.get()->search(txn, rowKey, rowKeySize, oId);
		isFound = (oId != UNDEF_OID);
	}
	else {
		isFound = searchRowKeyWithRowIdMap(txn, rowKeySize, rowKey, oId);
	}

	if (!isFound && !isExclusive()) {
		isFound = searchRowKeyWithMvccMap(txn, rowKeySize, rowKey, oId);
	}
	if (isFound) {
		rowArray.load(txn, oId, this, OBJECT_FOR_UPDATE);
	}
	return !isFound;
}

void Collection::appendRowInternal(TransactionContext& txn,
	MessageRowStore* messageRowStore, RowArray& rowArray, RowId& rowId) {
	rowId = allocateRowId();

	if (rowArray.isNotInitialized() || rowArray.isFull()) {
		StackAllocAutoPtr<BtreeMap> rowIdMap(
			txn.getDefaultAllocator(), getRowIdMap(txn));
		rowArray.reset();
		if (getRowNum() == 0) {
			uint32_t smallRowArrayNum =
				calcRowArrayNum(SMALL_ROW_ARRAY_MAX_SIZE);
			rowArray.initialize(txn, rowId, smallRowArrayNum);
		}
		else {
			rowArray.initialize(txn, rowId, getNormalRowArrayNum());
		}
		insertRowIdMap(txn, rowIdMap.get(), &rowId, rowArray.getBaseOId());
	}
	else {
		if (rowArray.isTailPos()) {
			shift(txn, rowArray, true);
			rowArray.tail();
		}
	}

	rowArray.append(txn, messageRowStore, rowId);

	RowArray::Row row(rowArray.getRow(), &rowArray);

	row.lock(txn);
	row.resetFirstUpdate();

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
	}
}

void Collection::updateRowInternal(TransactionContext& txn,
	MessageRowStore* messageRowStore, RowArray& rowArray, RowId& rowId) {
	RowArray::Row row(rowArray.getRow(), &rowArray);
	rowId = row.getRowId();
	row.lock(txn);

	if (!txn.isAutoCommit() && row.isFirstUpdate()) {
		row.resetFirstUpdate();
		RowArray snapShotRowArray(txn, this);
		snapShotRowArray.initialize(
			txn, row.getRowId(), MVCC_ROW_ARRAY_MAX_SIZE);
		rowArray.copy(txn, snapShotRowArray);

		TransactionId tId = txn.getId();
		MvccRowImage mvccRowImage(MVCC_UPDATE, snapShotRowArray.getOId());
		StackAllocAutoPtr<BtreeMap> mvccMap(
			txn.getDefaultAllocator(), getMvccMap(txn));
		insertMvccMap(txn, mvccMap.get(), tId, mvccRowImage);
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

void Collection::deleteRowInternal(
	TransactionContext& txn, RowArray& rowArray, RowId& rowId) {
	RowArray::Row row(rowArray.getRow(), &rowArray);
	rowId = row.getRowId();
	row.lock(txn);

	if (!txn.isAutoCommit() && row.isFirstUpdate()) {
		row.resetFirstUpdate();
		RowArray snapShotRowArray(txn, this);
		snapShotRowArray.initialize(
			txn, row.getRowId(), MVCC_ROW_ARRAY_MAX_SIZE);
		rowArray.copy(txn, snapShotRowArray);

		TransactionId tId = txn.getId();
		MvccRowImage mvccRowImage(MVCC_DELETE, snapShotRowArray.getOId());
		StackAllocAutoPtr<BtreeMap> mvccMap(
			txn.getDefaultAllocator(), getMvccMap(txn));
		insertMvccMap(txn, mvccMap.get(), tId, mvccRowImage);
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
			BaseObject baseFieldObject(
				txn.getPartitionId(), *getObjectManager());
			row.getField(txn, columnInfo, baseFieldObject);
			removeValueMap(txn, map.get(), baseFieldObject.getCursor<uint8_t>(),
				rowArray.getOId(), indexList[i].columnId_,
				indexList[i].mapType_);
		}
	}

	rowArray.remove(txn);

	uint16_t activeNum = rowArray.getActiveRowNum();
	if (activeNum == 0) {
		RowId rowId = rowArray.getRowId();
		StackAllocAutoPtr<BtreeMap> rowIdMap(
			txn.getDefaultAllocator(), getRowIdMap(txn));
		removeRowIdMap(txn, rowIdMap.get(), &rowId, rowArray.getBaseOId());
		rowArray.finalize(txn);
	}
	else if (activeNum < rowArray.getMaxRowNum() / 2) {
		RowArray nextRowArray(txn, this);
		bool isExist =
			rowArray.nextRowArray(txn, nextRowArray, OBJECT_FOR_UPDATE);
		if (isExist &&
			activeNum + nextRowArray.getActiveRowNum() <=
				rowArray.getMaxRowNum()) {
			merge(txn, rowArray, nextRowArray);
		}
		else {
			RowArray prevRowArray(txn, this);
			isExist =
				rowArray.prevRowArray(txn, prevRowArray, OBJECT_FOR_UPDATE);
			if (isExist &&
				activeNum + prevRowArray.getActiveRowNum() <=
					prevRowArray.getMaxRowNum()) {
				merge(txn, prevRowArray, rowArray);
			}
		}
	}
}

void Collection::insertRowInternal(
	TransactionContext& txn, RowArray& srcRowArray, RowArray& destRowArray) {
	RowArray::Row srcRow(srcRowArray.getRow(), &srcRowArray);
	RowId insertRowId = srcRow.getRowId();
	if (destRowArray.getMaxRowNum() == 1) {
		if (destRowArray.isFull()) {
			StackAllocAutoPtr<BtreeMap> rowIdMap(
				txn.getDefaultAllocator(), getRowIdMap(txn));
			destRowArray.reset();
			destRowArray.initialize(txn, insertRowId, getNormalRowArrayNum());
			insertRowIdMap(
				txn, rowIdMap.get(), &insertRowId, destRowArray.getBaseOId());
		}
	}
	else {
		if (destRowArray.isFull()) {
			RowId splitRowId;
			RowArray splitRowArray(txn, this);
			split(txn, destRowArray, insertRowId, splitRowArray, splitRowId);
			if (splitRowId < srcRow.getRowId()) {
				destRowArray.load(
					txn, splitRowArray.getOId(), this, OBJECT_FOR_UPDATE);
			}
		}
		else {
			shift(txn, destRowArray, false);
		}
	}
	srcRowArray.move(txn, destRowArray);
	RowArray::Row row(destRowArray.getRow(), &destRowArray);

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
			destRowArray.getOId(), indexList[i].columnId_,
			indexList[i].mapType_);
	}
}

void Collection::shift(
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

void Collection::split(TransactionContext& txn, RowArray& rowArray,
	RowId insertRowId, RowArray& splitRowArray, RowId& splitRowId) {
	util::XArray<std::pair<OId, OId> > moveList(txn.getDefaultAllocator());
	util::XArray<std::pair<OId, OId> >::iterator itr;

	StackAllocAutoPtr<BtreeMap> rowIdMap(
		txn.getDefaultAllocator(), getRowIdMap(txn));
	splitRowId = rowArray.getMidRowId();
	splitRowArray.initialize(txn, splitRowId, getNormalRowArrayNum());
	rowArray.split(txn, insertRowId, splitRowArray, splitRowId, moveList);
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
}

void Collection::merge(
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

bool Collection::searchRowKeyWithRowIdMap(TransactionContext& txn,
	uint32_t rowKeySize, const uint8_t* rowKey, OId& oId) {
	oId = UNDEF_OID;

	ColumnInfo& columnInfo = getColumnInfo(ColumnInfo::ROW_KEY_COLUMN_ID);
	ColumnType type = columnInfo.getColumnType();
	const Operator eq = eqTable[type][type];
	ContainerValue containerValue(txn, *getObjectManager());

	BtreeMap::BtreeCursor btreeCursor;

	while (1) {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		util::XArray<OId> idList(txn.getDefaultAllocator());  
		util::XArray<OId>::iterator itr;
		StackAllocAutoPtr<BtreeMap> rowIdMap(
			txn.getDefaultAllocator(), getRowIdMap(txn));
		int32_t getAllStatus = rowIdMap.get()->getAll(
			txn, PARTIAL_RESULT_SIZE, idList, btreeCursor);

		for (itr = idList.begin(); itr != idList.end(); itr++) {
			RowArray rowArray(txn, *itr, this, OBJECT_READ_ONLY);
			for (rowArray.begin(); !rowArray.end(); rowArray.next()) {
				RowArray::Row row(rowArray.getRow(), &rowArray);
				row.getField(txn, columnInfo, containerValue);
				if (eq(txn, containerValue.getValue().data(),
						containerValue.getValue().size(), rowKey, rowKeySize)) {
					oId = rowArray.getOId();
					return true;
				}
			}
		}
		if (getAllStatus == GS_SUCCESS) {
			break;
		}
	}
	return false;
}

bool Collection::searchRowKeyWithMvccMap(TransactionContext& txn,
	uint32_t rowKeySize, const uint8_t* rowKey, OId& oId) {
	oId = UNDEF_OID;

	StackAllocAutoPtr<BtreeMap> mvccMap(
		txn.getDefaultAllocator(), getMvccMap(txn));
	if (mvccMap.get()->isEmpty()) {
		return false;
	}

	ColumnInfo& columnInfo = getColumnInfo(ColumnInfo::ROW_KEY_COLUMN_ID);
	ColumnType type = columnInfo.getColumnType();
	const Operator eq = eqTable[type][type];
	Value value;

	util::XArray<std::pair<TransactionId, MvccRowImage> > idList(
		txn.getDefaultAllocator());
	util::XArray<std::pair<TransactionId, MvccRowImage> >::iterator itr;
	mvccMap.get()->getAll<TransactionId, MvccRowImage>(
		txn, MAX_RESULT_SIZE, idList);
	for (itr = idList.begin(); itr != idList.end(); itr++) {
		if (itr->second.type_ == MVCC_SELECT) {
			continue;
		}
		else if (itr->second.type_ == MVCC_CREATE) {
			continue;
		}
		RowArray rowArray(
			txn, itr->second.snapshotRowOId_, this, OBJECT_READ_ONLY);
		RowArray::Row row(rowArray.getRow(), &rowArray);
		if (row.getTxnId() != txn.getId()) {
			BaseObject baseFieldObject(
				txn.getPartitionId(), *getObjectManager());
			row.getField(txn, columnInfo, baseFieldObject);
			value.set(baseFieldObject.getCursor<uint8_t>(),
				columnInfo.getColumnType());
			if (eq(txn, value.data(), value.size(), rowKey, rowKeySize)) {
				oId = itr->second.snapshotRowOId_;
				return true;
			}
		}
	}

	return false;
}

void Collection::undoCreateRow(TransactionContext& txn, RowArray& rowArray) {
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
	decrementRowNum();
}

void Collection::undoUpdateRow(
	TransactionContext& txn, RowArray& beforeRowArray) {
	AbortStatus status = ABORT_UNDEF_STATUS;

	util::XArray<IndexData> indexList(txn.getDefaultAllocator());
	getIndexList(txn, indexList);

	RowArray::Row beforeRow(beforeRowArray.getRow(), &beforeRowArray);
	RowArray rowArray(txn, this);
	RowId rowId = beforeRow.getRowId();
	util::XArray<OId> oIdList(txn.getDefaultAllocator());

	{
		BtreeMap::SearchContext sc(
			UNDEF_COLUMNID, NULL, 0, true, &rowId, 0, true, 0, NULL, 1);
		StackAllocAutoPtr<BtreeMap> rowIdMap(
			txn.getDefaultAllocator(), getRowIdMap(txn));
		rowIdMap.get()->search(txn, sc, oIdList, ORDER_DESCENDING);
		if (!oIdList.empty()) {
			rowArray.load(txn, oIdList[0], this, OBJECT_FOR_UPDATE);
			bool isFound = rowArray.searchRowId(rowId);
			if (isFound) {
				status = ABORT_UPDATE_ROW_ARRAY;
			}
			else {
				status = ABORT_INSERT_ROW_ARRAY;
			}
		}
		else {
			status = ABORT_CREATE_FIRST_ROW_ARRAY;
		}
	}

	switch (status) {
	case ABORT_INSERT_ROW_ARRAY:
		insertRowInternal(txn, beforeRowArray, rowArray);
		break;
	case ABORT_UPDATE_ROW_ARRAY: {
		RowArray::Row row(rowArray.getRow(), &rowArray);
		if (!row.isRemoved()) {
			for (size_t i = 0; i < indexList.size(); i++) {
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
			}
			row.finalize(txn);
		}
		beforeRowArray.move(txn, rowArray);

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
			insertValueMap(txn, map.get(), baseFieldObject.getCursor<uint8_t>(),
				rowArray.getOId(), indexList[i].columnId_,
				indexList[i].mapType_);
		}
	} break;
	case ABORT_CREATE_FIRST_ROW_ARRAY: {
		RowId baseRowId = 0;
		rowArray.initialize(txn, baseRowId, getNormalRowArrayNum());
		StackAllocAutoPtr<BtreeMap> rowIdMap(
			txn.getDefaultAllocator(), getRowIdMap(txn));
		insertRowIdMap(txn, rowIdMap.get(), &baseRowId, rowArray.getBaseOId());
		beforeRowArray.move(txn, rowArray);

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
			insertValueMap(txn, map.get(), baseFieldObject.getCursor<uint8_t>(),
				rowArray.getOId(), indexList[i].columnId_,
				indexList[i].mapType_);
		}
	} break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
		break;
	}

	beforeRowArray.finalize(txn);

	if (status != ABORT_UPDATE_ROW_ARRAY) {
		incrementRowNum();
	}
}

void Collection::getIdList(TransactionContext& txn,
	util::XArray<uint8_t>& serializedRowList, util::XArray<RowId>& idList) {
	try {
		uint8_t* in = serializedRowList.data();
		for (size_t i = 0; i < (serializedRowList.size() /
								   (sizeof(RowId) + rowFixedDataSize_));
			 i++) {  
			idList.push_back(*reinterpret_cast<RowId*>(in));
			in += sizeof(RowId) + rowFixedDataSize_;  
		}
	}
	catch (std::exception& e) {
		handleSearchError(txn, e, GS_ERROR_DS_CON_GET_ROW_ID_LIST_FAILED);
	}
}

void Collection::lockIdList(TransactionContext& txn, util::XArray<OId>& oIdList,
	util::XArray<RowId>& idList) {
	try {
		RowArray rowArray(txn, this);
		for (size_t i = 0; i < oIdList.size(); i++) {
			rowArray.load(txn, oIdList[i], this, OBJECT_FOR_UPDATE);
			RowArray::Row row(rowArray.getRow(), &rowArray);
			row.lock(txn);
			idList.push_back(row.getRowId());
		}
	}
	catch (std::exception& e) {
		handleSearchError(txn, e, GS_ERROR_DS_CON_GET_ROW_ID_LIST_FAILED);
	}
}

void Collection::setDummyMvccImage(TransactionContext& txn) {
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
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void Collection::getContainerOptionInfo(
	TransactionContext&, util::XArray<uint8_t>&) {
	return;
}

void Collection::checkContainerOption(
	MessageSchema*, util::XArray<uint32_t>&, bool&) {
	return;
}

void Collection::checkExclusive(TransactionContext& txn) {
	if (isExclusiveUpdate()) {  
		return;
	}
	setExclusiveStatus(IS_EXCLUSIVE);

	StackAllocAutoPtr<BtreeMap> mvccMap(
		txn.getDefaultAllocator(), getMvccMap(txn));
	if (mvccMap.get()->isEmpty()) {
		return;
	}

	Value value;
	util::XArray<std::pair<TransactionId, MvccRowImage> > idList(
		txn.getDefaultAllocator());
	util::XArray<std::pair<TransactionId, MvccRowImage> >::iterator itr;
	mvccMap.get()->getAll<TransactionId, MvccRowImage>(
		txn, MAX_RESULT_SIZE, idList);
	for (itr = idList.begin(); itr != idList.end(); itr++) {
		if (itr->second.type_ == MVCC_SELECT) {
			continue;
		}
		else if (itr->second.type_ == MVCC_CREATE) {
			if (getExclusiveStatus() == IS_EXCLUSIVE &&
				txn.getId() != itr->first) {  
				setExclusiveStatus(IS_NOT_EXCLUSIVE_CREATE_EXIST);
			}
		}
		else {
			if (txn.getId() !=
				itr->first) {  
				setExclusiveStatus(IS_NOT_EXCLUSIVE);
				break;
			}
		}
	}
}

void Collection::insertRowIdMap(
	TransactionContext& txn, BtreeMap* map, const void* constKey, OId oId) {
	int32_t status = map->insert(txn, constKey, oId);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		setDirty();
		baseContainerImage_->rowIdMapOId_ = map->getBaseOId();
	}
}

void Collection::insertMvccMap(TransactionContext& txn, BtreeMap* map,
	TransactionId tId, MvccRowImage& mvccImage) {
	int32_t status =
		map->insert<TransactionId, MvccRowImage>(txn, tId, mvccImage);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		setDirty();
		baseContainerImage_->mvccMapOId_ = map->getBaseOId();
	}
}

void Collection::insertValueMap(TransactionContext& txn, BaseIndex* map,
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

void Collection::updateRowIdMap(TransactionContext& txn, BtreeMap* map,
	const void* constKey, OId oldOId, OId newOId) {
	int32_t status = map->update(txn, constKey, oldOId, newOId);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		setDirty();
		baseContainerImage_->rowIdMapOId_ = map->getBaseOId();
	}
}

void Collection::updateMvccMap(TransactionContext& txn, BtreeMap* map,
	TransactionId tId, MvccRowImage& oldMvccImage, MvccRowImage& newMvccImage) {
	int32_t status = map->update<TransactionId, MvccRowImage>(
		txn, tId, oldMvccImage, newMvccImage);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		setDirty();
		baseContainerImage_->mvccMapOId_ = map->getBaseOId();
	}
}

void Collection::updateValueMap(TransactionContext& txn, BaseIndex* map,
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

void Collection::removeRowIdMap(
	TransactionContext& txn, BtreeMap* map, const void* constKey, OId oId) {
	int32_t status = map->remove(txn, constKey, oId);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		setDirty();
		baseContainerImage_->rowIdMapOId_ = map->getBaseOId();
	}
}

void Collection::removeMvccMap(TransactionContext& txn, BtreeMap* map,
	TransactionId tId, MvccRowImage& mvccImage) {
	int32_t status =
		map->remove<TransactionId, MvccRowImage>(txn, tId, mvccImage);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		setDirty();
		baseContainerImage_->mvccMapOId_ = map->getBaseOId();
	}
}

void Collection::removeValueMap(TransactionContext& txn, BaseIndex* map,
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

void Collection::updateIndexData(TransactionContext& txn, IndexData indexData) {
	indexSchema_->updateIndexData(txn, indexData, UNDEF_CONTAINER_POS);
}

uint16_t Collection::calcRowArrayNum(uint16_t baseRowNum) {
	uint32_t pointArrayObjectSize = static_cast<uint32_t>(
		RowArray::getHeaderSize() + rowImageSize_ * baseRowNum);
	Size_t estimateAllocateSize =
		getObjectManager()->estimateAllocateSize(pointArrayObjectSize);
	if (estimateAllocateSize > getObjectManager()->getHalfOfMaxObjectSize()) {
		uint32_t oneRowObjectSize = static_cast<uint32_t>(
			RowArray::getHeaderSize() + rowImageSize_ * 1);
		Size_t estimateOneRowAllocateSize =
			getObjectManager()->estimateAllocateSize(oneRowObjectSize);
		if (estimateOneRowAllocateSize >
			getObjectManager()->getHalfOfMaxObjectSize()) {
			estimateAllocateSize = getObjectManager()->getMaxObjectSize();
		}
		else {
			estimateAllocateSize = getObjectManager()->getHalfOfMaxObjectSize();
		}
	}
	return (estimateAllocateSize - RowArray::getHeaderSize()) / rowImageSize_;
}


/*!
	@brief Validates Rows and Indexes
*/
bool Collection::validate(TransactionContext& txn, std::string& errorMessage) {
	RowId preRowId = -1;
	bool isRowRateCheck = false;
	uint64_t countRowNum = 0;
	return validateImpl<Collection>(
		txn, errorMessage, preRowId, countRowNum, isRowRateCheck);
}

std::string Collection::dump(TransactionContext& txn) {
	return dumpImpl<Collection>(txn);
}

