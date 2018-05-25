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
	@brief Implementation of TimeSeries
*/
#include "time_series.h"
#include "util/trace.h"
#include "btree_map.h"
#include "data_store.h"
#include "data_store_common.h"
#include "hash_map.h"
#include "transaction_manager.h"
#include "gs_error.h"
#include "message_schema.h"
#include "value_processor.h"


const bool TimeSeries::indexMapTable[][MAP_TYPE_NUM] = {
	{true, false, false},   
	{true, false, false},   
	{true, false, false},   
	{true, false, false},   
	{true, false, false},   
	{true, false, false},   
	{true, false, false},   
	{true, false, false},   
	{true, false, false},   
	{false, false, false},  
	{false, false, false}   
};


/*!
	@brief Allocate TimeSeries Object
*/
void TimeSeries::initialize(TransactionContext &txn) {
	baseContainerImage_ =
		BaseObject::allocate<TimeSeriesImage>(sizeof(TimeSeriesImage),
			getMetaAllcateStrategy(), getBaseOId(), OBJECT_TYPE_TIME_SERIES);
	baseContainerImage_->containerType_ =
		static_cast<int8_t>(TIME_SERIES_CONTAINER);
	baseContainerImage_->status_ = 0;  
	baseContainerImage_->normalRowArrayNum_ = 0;
	baseContainerImage_->containerId_ = UNDEF_CONTAINERID;
	baseContainerImage_->containerNameOId_ = UNDEF_OID;
	baseContainerImage_->subContainerListOId_ = UNDEF_OID;
	baseContainerImage_->mvccMapOId_ = UNDEF_OID;
	baseContainerImage_->columnSchemaOId_ = UNDEF_OID;
	baseContainerImage_->indexSchemaOId_ = UNDEF_OID;
	baseContainerImage_->triggerListOId_ = UNDEF_OID;
	baseContainerImage_->lastLsn_ = UNDEF_LSN;
	baseContainerImage_->rowNum_ = 0;
	baseContainerImage_->versionId_ = 0;
	reinterpret_cast<TimeSeriesImage *>(baseContainerImage_)->reserved_ = 0;
	reinterpret_cast<TimeSeriesImage *>(baseContainerImage_)->padding1_ = 0;
	reinterpret_cast<TimeSeriesImage *>(baseContainerImage_)->padding2_ = 0;
	reinterpret_cast<TimeSeriesImage *>(baseContainerImage_)->padding3_ = 0;
	reinterpret_cast<TimeSeriesImage *>(baseContainerImage_)->padding4_ = 0;

	rowImageSize_ = 0;
	metaAllocateStrategy_ = AllocateStrategy();
	rowAllocateStrategy_ = AllocateStrategy();
	mapAllocateStrategy_ = AllocateStrategy();
}

/*!
	@brief Set TimeSeries Schema
*/
void TimeSeries::set(TransactionContext &txn, const char8_t *containerName,
	ContainerId containerId, OId columnSchemaOId,
	MessageSchema *orgContainerSchema) {
	MessageTimeSeriesSchema *containerSchema =
		reinterpret_cast<MessageTimeSeriesSchema *>(orgContainerSchema);
	baseContainerImage_->containerId_ = containerId;

	uint32_t containerNameSize = static_cast<uint32_t>(strlen(containerName));

	BaseObject stringObject(txn.getPartitionId(), *getObjectManager());
	char *stringPtr = stringObject.allocate<char>(containerNameSize + 1,
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

	int32_t initAllocateSubTimeSeriesNum;
	if (getExpirationInfo().duration_ == INT64_MAX) {
		initAllocateSubTimeSeriesNum = 1;
	}
	else {
		initAllocateSubTimeSeriesNum = getExpirationInfo().dividedNum_ + 1;
	}
	subTimeSeriesListHead_ = ALLOC_NEW(txn.getDefaultAllocator())
		SubTimeSeriesList(txn, *getObjectManager());
	subTimeSeriesListHead_->initialize(
		txn, initAllocateSubTimeSeriesNum, getMetaAllcateStrategy());
	setBaseTime(0);
	setTailNodeOId(UNDEF_OID);
	baseContainerImage_->subContainerListOId_ =
		subTimeSeriesListHead_->getBaseOId();

	baseContainerImage_->normalRowArrayNum_ =
		calcRowArrayNum(ROW_ARRAY_MAX_SIZE);

	setAllocateStrategy();

	GS_TRACE_INFO(BASE_CONTAINER, GS_TRACE_DS_CON_DATA_AFFINITY_DEFINED,
		"ContainerName = " << containerName << ", Affinity = "
						   << containerSchema->getAffinityStr()
						   << ", AffinityHashVal = "
						   << calcAffnityGroupId(getAffinity()));
}

/*!
	@brief Free Objects related to TimeSeries
*/
void TimeSeries::finalize(TransactionContext &txn) {
	try {
		setDirty();
		Timestamp expiredTime = getCurrentExpiredTime(txn);
		expireSubTimeSeries(txn, expiredTime);

		for (int64_t i = subTimeSeriesListHead_->getNum() - 1; i >= 0; i--) {
			const SubTimeSeriesImage &subTimeSeriesImage =
				*(subTimeSeriesListHead_->get(txn, i));
			util::StackAllocator::Scope scope(txn.getDefaultAllocator());
			SubTimeSeries *subContainer =
				ALLOC_NEW(txn.getDefaultAllocator()) SubTimeSeries(
					txn, SubTimeSeriesInfo(subTimeSeriesImage, i), this);
			subContainer->finalize(txn);
		}

		if (baseContainerImage_->subContainerListOId_ != UNDEF_OID) {
			subTimeSeriesListHead_->finalize(txn);
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
	catch (std::exception &e) {
		handleUpdateError(txn, e, GS_ERROR_DS_DS_DROP_TIME_SERIES_FAILED);
	}
}

/*!
	@brief Creates a specifed type of index on the specified Column
*/
void TimeSeries::createIndex(TransactionContext &txn, IndexInfo &indexInfo) {
	try {
		GS_TRACE_INFO(TIME_SERIES, GS_TRACE_DS_CON_CREATE_INDEX,
			"TimeSeries Id = " << getContainerId()
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
		if (indexInfo.columnId == ColumnInfo::ROW_KEY_COLUMN_ID) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_TIM_CREATEINDEX_ON_ROWKEY, "");
		}

		if (hasIndex(txn, indexInfo.columnId, indexInfo.mapType)) {
			return;
		}

		if (indexSchema_->createIndexInfo(
				txn, indexInfo.columnId, indexInfo.mapType)) {
			replaceIndexSchema(txn);
		}

		util::XArray<SubTimeSeriesInfo> subTimeSeriesList(
			txn.getDefaultAllocator());
		util::XArray<SubTimeSeriesInfo>::iterator itr;
		getSubTimeSeriesList(txn, subTimeSeriesList, true);
		for (itr = subTimeSeriesList.begin(); itr != subTimeSeriesList.end();
			 itr++) {
			util::StackAllocator::Scope scope(txn.getDefaultAllocator());
			SubTimeSeries *subContainer = ALLOC_NEW(txn.getDefaultAllocator())
				SubTimeSeries(txn, *itr, this);
			subContainer->createIndex(txn, indexInfo);
		}
	}
	catch (std::exception &e) {
		handleUpdateError(txn, e, GS_ERROR_DS_TIM_CREATE_INDEX_FAILED);
	}
}

/*!
	@brief Creates a specifed type of index on the specified Column
*/
void TimeSeries::dropIndex(TransactionContext &txn, IndexInfo &indexInfo) {
	try {
		GS_TRACE_DEBUG(TIME_SERIES, GS_TRACE_DS_CON_DROP_INDEX,
			"TimeSeries Id = "
				<< getContainerId() << " columnId = " << indexInfo.columnId
				<< " type = " << static_cast<uint32_t>(indexInfo.mapType));

		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not drop index. container's status is invalid.");
		}

		if (indexInfo.columnId >= getColumnNum()) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_COLUMN_ID_INVALID, "");
		}

		if (hasIndex(txn, indexInfo.columnId, indexInfo.mapType)) {
			util::XArray<SubTimeSeriesInfo> subTimeSeriesList(
				txn.getDefaultAllocator());
			util::XArray<SubTimeSeriesInfo>::reverse_iterator rItr;
			getSubTimeSeriesList(txn, subTimeSeriesList, true);
			for (rItr = subTimeSeriesList.rbegin();
				 rItr != subTimeSeriesList.rend(); rItr++) {
				util::StackAllocator::Scope scope(txn.getDefaultAllocator());
				SubTimeSeries *subContainer = ALLOC_NEW(
					txn.getDefaultAllocator()) SubTimeSeries(txn, *rItr, this);
				subContainer->dropIndex(txn, indexInfo);
			}
			indexSchema_->dropIndexInfo(
				txn, indexInfo.columnId, indexInfo.mapType);
		}
	}
	catch (std::exception &e) {
		handleUpdateError(txn, e, GS_ERROR_DS_TIM_DROP_INDEX_FAILED);
	}
}

/*!
	@brief Newly creates or updates a Row with a Row key of the current time on
   server
*/
void TimeSeries::appendRow(TransactionContext &txn, uint32_t rowSize,
	const uint8_t *rowDataWOTimestamp, Timestamp &rowKey,
	DataStore::PutStatus &status) {
	try {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		setDirty();

		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not put. container's status is invalid.");
		}

		rowKey = txn.getStatementStartTime().getUnixTime();
		if (!ValueProcessor::validateTimestamp(rowKey)) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_TIM_ROW_DATA_INVALID,
				"Timestamp of AppendTime out of range (rowKey=" << rowKey
																<< ")");
		}
		InputMessageRowStore inputMessageRowStore(
			getDataStore()->getValueLimitConfig(), getColumnInfoList(),
			getColumnNum(), const_cast<uint8_t *>(rowDataWOTimestamp), rowSize,
			1, rowFixedDataSize_);
		inputMessageRowStore.next();
		inputMessageRowStore.setField(
			ColumnInfo::ROW_KEY_COLUMN_ID, &rowKey, sizeof(Timestamp));
		putRow(
			txn, &inputMessageRowStore, rowKey, status, PUT_INSERT_OR_UPDATE);
	}
	catch (std::exception &e) {
		handleUpdateError(txn, e, GS_ERROR_DS_TIM_APPEND_ROW_FAILED);
	}
}

/*!
	@brief Deletes a Row corresponding to the specified Row key
*/
void TimeSeries::deleteRow(TransactionContext &txn, uint32_t rowKeySize,
	const uint8_t *rowKey, RowId &rowId, bool &existing) {
	try {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		setDirty();

		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not delete. container's status is invalid.");
		}

		Timestamp rowKeyTimestamp =
			*(reinterpret_cast<const Timestamp *>(rowKey));
		if (!ValueProcessor::validateTimestamp(rowKeyTimestamp)) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_COL_ROWKEY_INVALID,
				"Timestamp of rowKey out of range (rowKey=" << rowKeyTimestamp
															<< ")");
		}


		SubTimeSeries *subContainer =
			getSubContainer(txn, rowKeyTimestamp, true);
		if (subContainer == NULL) {
			existing = false;
			rowId = UNDEF_ROWID;
			return;
		}
		subContainer->deleteRow(txn, rowKeySize, rowKey, rowId, existing);
	}
	catch (std::exception &e) {
		handleUpdateError(txn, e, GS_ERROR_DS_TIM_DELETE_ROW_FAILED);
	}
}

void TimeSeries::deleteRow(
	TransactionContext &txn, RowId rowId, bool &existing, bool isForceLock) {
	try {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		setDirty();

		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not delete. container's status is invalid.");
		}

		Timestamp rowKeyTimestamp = static_cast<Timestamp>(rowId);
		if (!ValueProcessor::validateTimestamp(rowKeyTimestamp)) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_COL_ROWKEY_INVALID,
				"Timestamp of rowKey out of range (rowKey=" << rowKeyTimestamp
															<< ")");
		}


		SubTimeSeries *subContainer =
			getSubContainer(txn, rowKeyTimestamp, true);
		if (subContainer == NULL) {
			existing = false;
			rowId = UNDEF_ROWID;
			return;
		}
		subContainer->deleteRow(txn, rowId, existing, isForceLock);
	}
	catch (std::exception &e) {
		handleUpdateError(txn, e, GS_ERROR_DS_TIM_DELETE_ROW_FAILED);
	}
}

void TimeSeries::updateRow(TransactionContext &txn, uint32_t rowSize,
	const uint8_t *rowData, RowId rowId, DataStore::PutStatus &status,
	bool isForceLock) {
	try {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		setDirty();

		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not update. container's status is invalid.");
		}

		Timestamp rowKeyTimestamp = static_cast<Timestamp>(rowId);
		if (!ValueProcessor::validateTimestamp(rowKeyTimestamp)) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_COL_ROWKEY_INVALID,
				"Timestamp of rowKey out of range (rowKey=" << rowKeyTimestamp
															<< ")");
		}


		SubTimeSeries *subContainer =
			getSubContainer(txn, rowKeyTimestamp, true);
		if (subContainer == NULL) {
			status = DataStore::NOT_EXECUTED;
			rowId = UNDEF_ROWID;
			return;
		}
		subContainer->updateRow(
			txn, rowSize, rowData, rowId, status, isForceLock);
	}
	catch (std::exception &e) {
		handleUpdateError(txn, e, GS_ERROR_DS_TIM_UPDATE_ROW_INVALID);
	}
}

/*!
	@brief Rolls back the result of transaction
*/
void TimeSeries::abort(TransactionContext &txn) {
	try {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		setDirty();

		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not abort. container's status is invalid.");
		}

		util::XArray<SubTimeSeriesInfo> subTimeSeriesList(
			txn.getDefaultAllocator());
		util::XArray<SubTimeSeriesInfo>::iterator itr;
		getRuntimeSubTimeSeriesList(txn, subTimeSeriesList, true);
		for (itr = subTimeSeriesList.begin(); itr != subTimeSeriesList.end();
			 itr++) {
			util::StackAllocator::Scope scope(txn.getDefaultAllocator());
			SubTimeSeries *subContainer = ALLOC_NEW(txn.getDefaultAllocator())
				SubTimeSeries(txn, *itr, this);
			subContainer->abort(txn);
		}
	}
	catch (std::exception &e) {
		handleUpdateError(txn, e, GS_ERROR_DS_TIM_ABORT_FAILED);
	}
}

/*!
	@brief Commits the result of transaction
*/
void TimeSeries::commit(TransactionContext &txn) {
	try {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		setDirty();

		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not commit. container's status is invalid.");
		}

		util::XArray<SubTimeSeriesInfo> subTimeSeriesList(
			txn.getDefaultAllocator());
		util::XArray<SubTimeSeriesInfo>::iterator itr;
		getRuntimeSubTimeSeriesList(txn, subTimeSeriesList, true);
		for (itr = subTimeSeriesList.begin(); itr != subTimeSeriesList.end();
			 itr++) {
			util::StackAllocator::Scope scope(txn.getDefaultAllocator());
			SubTimeSeries *subContainer = ALLOC_NEW(txn.getDefaultAllocator())
				SubTimeSeries(txn, *itr, this);
			subContainer->commit(txn);
		}
	}
	catch (std::exception &e) {
		handleUpdateError(txn, e, GS_ERROR_DS_TIM_COMMIT_FAILED);
	}
}


/*!
	@brief Change new Column layout
*/
void TimeSeries::changeSchema(TransactionContext &txn,
	BaseContainer &newContainer, util::XArray<uint32_t> &copyColumnMap) {
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
		util::XArray<SubTimeSeriesInfo> subTimeSeriesList(
			txn.getDefaultAllocator());
		util::XArray<SubTimeSeriesInfo>::iterator itr;
		getSubTimeSeriesList(txn, subTimeSeriesList, true);
		for (itr = subTimeSeriesList.begin(); itr != subTimeSeriesList.end();
			 itr++) {
			util::StackAllocator::Scope scope(txn.getDefaultAllocator());
			SubTimeSeries *subContainer = ALLOC_NEW(txn.getDefaultAllocator())
				SubTimeSeries(txn, *itr, this);
			subContainer->changeSchema(txn, newContainer, copyColumnMap);
		}
	}
	catch (std::exception &e) {
		handleUpdateError(
			txn, e, GS_ERROR_DS_DS_CHANGE_TIME_SERIES_SCHEMA_FAILED);
	}
}

/*!
	@brief Check if Container has data of uncommited transaction
*/
bool TimeSeries::hasUncommitedTransaction(TransactionContext &txn) {
	util::XArray<SubTimeSeriesInfo> subTimeSeriesList(
		txn.getDefaultAllocator());
	getRuntimeSubTimeSeriesList(txn, subTimeSeriesList, false);
	if (subTimeSeriesList.empty()) {
		return false;
	}
	else {
		return true;
	}
}

/*!
	@brief Search Btree Index of RowId
*/
void TimeSeries::searchRowIdIndex(TransactionContext &txn,
	BtreeMap::SearchContext &sc, util::XArray<OId> &resultList,
	OutputOrder order) {
	Timestamp expiredTime = getCurrentExpiredTime(txn);
	const void *backupStartKey = NULL;
	const Timestamp *startKeyPtr =
		reinterpret_cast<const Timestamp *>(sc.startKey_);
	if (expiredTime != MINIMUM_EXPIRED_TIMESTAMP &&
		(startKeyPtr == NULL || *startKeyPtr < expiredTime)) {
		backupStartKey = sc.startKey_;
		sc.startKey_ = &expiredTime;
	}
	util::XArray<SubTimeSeriesInfo> subTimeSeriesImageList(
		txn.getDefaultAllocator());
	getSubTimeSeriesList(txn, sc, subTimeSeriesImageList, false);
	if (order != ORDER_DESCENDING) {
		util::XArray<SubTimeSeriesInfo>::iterator itr;
		for (itr = subTimeSeriesImageList.begin();
			 itr != subTimeSeriesImageList.end(); itr++) {
			SubTimeSeries *subContainer = ALLOC_NEW(txn.getDefaultAllocator())
				SubTimeSeries(txn, *itr, this);
			subContainer->searchRowIdIndex(txn, sc, resultList,
				ORDER_ASCENDING);  
			if (resultList.size() >= sc.limit_) {
				break;
			}
		}
	}
	else {
		util::XArray<SubTimeSeriesInfo>::reverse_iterator itr;
		for (itr = subTimeSeriesImageList.rbegin();
			 itr != subTimeSeriesImageList.rend(); itr++) {
			SubTimeSeries *subContainer = ALLOC_NEW(txn.getDefaultAllocator())
				SubTimeSeries(txn, *itr, this);
			subContainer->searchRowIdIndex(txn, sc, resultList, order);
			if (resultList.size() >= sc.limit_) {
				break;
			}
		}
	}
	if (backupStartKey != NULL) {
		sc.startKey_ = backupStartKey;
	}
}
/*!
	@brief Search Btree Index of RowId
*/
void TimeSeries::searchRowIdIndex(TransactionContext &txn, uint64_t start,
	uint64_t limit, util::XArray<RowId> &rowIdList,
	util::XArray<OId> &resultList, uint64_t &skipped) {
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
	util::XArray<OId> mvccList(txn.getDefaultAllocator());

	util::XArray<SubTimeSeriesInfo> subTimeSeriesImageList(
		txn.getDefaultAllocator());
	getSubTimeSeriesList(txn, sc, subTimeSeriesImageList, false);
	util::XArray<SubTimeSeriesInfo>::iterator itr;
	for (itr = subTimeSeriesImageList.begin();
		 itr != subTimeSeriesImageList.end(); itr++) {
		SubTimeSeries *subContainer =
			ALLOC_NEW(txn.getDefaultAllocator()) SubTimeSeries(txn, *itr, this);

		StackAllocAutoPtr<BtreeMap> rowIdMap(
			txn.getDefaultAllocator(), subContainer->getRowIdMap(txn));
		rowIdMap.get()->search<RowId, OId, KeyValue<RowId, OId> >(
			txn, sc, keyValueList, ORDER_ASCENDING);

		subContainer->searchMvccMap<TimeSeries, BtreeMap::SearchContext>(
			txn, sc, mvccList);
	}
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

void TimeSeries::searchColumnIdIndex(TransactionContext &txn,
	BtreeMap::SearchContext &sc, util::XArray<OId> &resultList,
	OutputOrder order)
{
	BtreeMap::SearchContext
		targetContainerSc;  
	util::XArray<SubTimeSeriesInfo> subTimeSeriesImageList(
		txn.getDefaultAllocator());
	getSubTimeSeriesList(txn, sc, subTimeSeriesImageList, false);
	util::XArray<SubTimeSeriesInfo>::iterator itr;
	ColumnId sortColumnId;
	if (order != ORDER_UNDEFINED) {
		sortColumnId = sc.columnId_;
	}
	else {
		sortColumnId = ColumnInfo::ROW_KEY_COLUMN_ID;
	}
	ColumnInfo &sortColumnInfo = getColumnInfo(sortColumnId);
	util::XArray<OId> mergeList(txn.getDefaultAllocator());
	for (itr = subTimeSeriesImageList.begin();
		 itr != subTimeSeriesImageList.end(); itr++) {
		util::XArray<OId> subList(txn.getDefaultAllocator());
		SubTimeSeries *subContainer =
			ALLOC_NEW(txn.getDefaultAllocator()) SubTimeSeries(txn, *itr, this);
		if (order != ORDER_UNDEFINED) {
			reinterpret_cast<BaseContainer *>(subContainer)
				->searchColumnIdIndex(txn, sc, subList, order);
			mergeRowList<TimeSeries>(txn, sortColumnInfo, resultList, true,
				subList, true, mergeList, order);
			resultList.swap(mergeList);
		}
		else {
			ResultSize limitBackup = sc.limit_;
			sc.limit_ = MAX_RESULT_SIZE;
			reinterpret_cast<BaseContainer *>(subContainer)
				->searchColumnIdIndex(txn, sc, subList, order);
			sc.limit_ = limitBackup;
			mergeRowList<TimeSeries>(txn, sortColumnInfo, resultList, true,
				subList, false, mergeList, ORDER_ASCENDING);
			resultList.swap(mergeList);
			if (resultList.size() >= sc.limit_) {
				break;
			}
		}
		mergeList.clear();
	}
	if (resultList.size() > sc.limit_) {
		resultList.resize(sc.limit_);
	}
}

/*!
	@brief Performs an aggregation operation on a Row set or its specific
   Columns, based on the specified start and end times
*/
void TimeSeries::aggregate(TransactionContext &txn, BtreeMap::SearchContext &sc,
	uint32_t columnId, AggregationType type, ResultSize &resultNum,
	util::XArray<uint8_t> &serializedRowList) {
	if (type == AGG_UNSUPPORTED_TYPE) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_AGGREGATED_COLUMN_TYPE_INVALID, "");
	}
	else if (type != AGG_COUNT) {
		if (columnId >= getColumnNum()) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_COLUMN_ID_INVALID, "");
		}
		ColumnType colType = getColumnInfo(columnId).getColumnType();
		if (type == AGG_MAX || type == AGG_MIN) {
			if (!ValueProcessor::isNumerical(colType) &&
				colType != COLUMN_TYPE_TIMESTAMP) {
				GS_THROW_USER_ERROR(
					GS_ERROR_DS_AGGREGATED_COLUMN_TYPE_INVALID, "");
			}
		}
		else {
			if (!ValueProcessor::isNumerical(colType)) {
				GS_THROW_USER_ERROR(
					GS_ERROR_DS_AGGREGATED_COLUMN_TYPE_INVALID, "");
			}
		}
	}
	util::XArray<OId> normalOIdList(txn.getDefaultAllocator());
	util::XArray<OId> mvccOIdList(txn.getDefaultAllocator());
	const Operator *op1, *op2;
	bool isValid = getKeyCondition(txn, sc, op1, op2);
	if (isValid) {
		searchRowArrayList(txn, sc, normalOIdList, mvccOIdList);
	}

	ContainerValue containerValue(txn, *getObjectManager());
	Value value;
	switch (type) {
	case AGG_TIME_AVG: {
		ContainerValue containerValueTs(txn, *getObjectManager());
		Timestamp beforeTs = UNDEF_TIMESTAMP;
		Timestamp beforeIntTs = UNDEF_TIMESTAMP;
		double beforeVal = 0;
		double weightedSum = 0;
		double weight = 0;
		bool isFirst = true;
		ColumnInfo &aggColumnInfo = getColumnInfo(columnId);
		ColumnInfo &keyColumnInfo =
			getColumnInfo(ColumnInfo::ROW_KEY_COLUMN_ID);

		util::XArray<OId>::iterator normalItr, mvccItr;
		RowArray normalRowArray(txn, this);
		RowArray mvccRowArray(txn, this);
		RowArray *normalRowArrayPtr = &normalRowArray;
		RowArray *mvccRowArrayPtr = &mvccRowArray;
		RowArray *rowArrayPtr = NULL;

		Timestamp normalBeginTime = -1, mvccBeginTime = -1;
		if (!normalOIdList.empty()) {
			normalRowArrayPtr->load(
				txn, normalOIdList[0], this, OBJECT_READ_ONLY);
			if (normalRowArrayPtr->begin()) {
				RowArray::Row row(
					normalRowArrayPtr->getRow(), normalRowArrayPtr);
				normalBeginTime = row.getRowId();
			}
		}
		if (!mvccOIdList.empty()) {
			mvccRowArrayPtr->load(txn, mvccOIdList[0], this, OBJECT_READ_ONLY);
			if (mvccRowArrayPtr->begin()) {
				RowArray::Row row(mvccRowArrayPtr->getRow(), mvccRowArrayPtr);
				mvccBeginTime = row.getRowId();
			}
		}

		normalItr = normalOIdList.begin();
		mvccItr = mvccOIdList.begin();
		while (
			normalItr != normalOIdList.end() || mvccItr != mvccOIdList.end()) {
			bool isNoramlExecute = false;
			if (mvccItr == mvccOIdList.end() ||
				(normalItr != normalOIdList.end() &&
					normalBeginTime < mvccBeginTime)) {
				rowArrayPtr = normalRowArrayPtr;
				isNoramlExecute = true;
			}
			else {
				rowArrayPtr = mvccRowArrayPtr;
				isNoramlExecute = false;
			}

			if ((isNoramlExecute &&
					(isExclusive() || txn.getId() == rowArrayPtr->getTxnId() ||
						rowArrayPtr->isFirstUpdate() ||
						!txn.getManager().isActiveTransaction(
							txn.getPartitionId(), rowArrayPtr->getTxnId()))) ||
				(!isNoramlExecute && txn.getId() != rowArrayPtr->getTxnId())) {
				for (rowArrayPtr->begin(); !rowArrayPtr->end();
					 rowArrayPtr->next()) {
					RowArray::Row row(rowArrayPtr->getRow(), rowArrayPtr);
					Timestamp rowId = row.getRowId();
					if (((op1 != 0) &&
							!(*op1)(txn,
								reinterpret_cast<const uint8_t *>(&rowId),
								sizeof(Timestamp),
								reinterpret_cast<const uint8_t *>(sc.startKey_),
								sc.startKeySize_)) ||
						((op2 != 0) &&
							!(*op2)(txn,
								reinterpret_cast<const uint8_t *>(&rowId),
								sizeof(Timestamp),
								reinterpret_cast<const uint8_t *>(sc.endKey_),
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
						row.getField(txn, keyColumnInfo, containerValueTs);
						row.getField(txn, aggColumnInfo, containerValue);
						if (isFirst) {
							beforeTs =
								containerValueTs.getValue().getTimestamp();
							beforeIntTs = beforeTs;
							beforeVal = containerValue.getValue().getDouble();
							isFirst = false;
						}
						else {
							Timestamp nextTs =
								containerValueTs.getValue().getTimestamp();
							Timestamp intTs =
								(nextTs - beforeTs) / 2 + beforeTs;  
							double currentWeight = static_cast<double>(
								intTs -
								beforeIntTs);  
							weightedSum += beforeVal * currentWeight;
							beforeTs = nextTs;
							beforeIntTs = intTs;
							beforeVal = containerValue.getValue().getDouble();
							weight += currentWeight;
						}
					}
				}
			}

			if (isNoramlExecute) {
				normalItr++;
			}
			else {
				mvccItr++;
			}
			if (normalItr != normalOIdList.end()) {
				normalRowArrayPtr->load(
					txn, *normalItr, this, OBJECT_READ_ONLY);
				if (normalRowArrayPtr->begin()) {
					RowArray::Row row(
						normalRowArrayPtr->getRow(), normalRowArrayPtr);
					normalBeginTime = row.getRowId();
				}
			}
			if (mvccItr != mvccOIdList.end()) {
				mvccRowArrayPtr->load(txn, *mvccItr, this, OBJECT_READ_ONLY);
				if (mvccRowArrayPtr->begin()) {
					RowArray::Row row(
						mvccRowArrayPtr->getRow(), mvccRowArrayPtr);
					mvccBeginTime = row.getRowId();
				}
			}
		}

		if (isFirst) {
			resultNum = 0;
		}
		else {
			resultNum = 1;
			if (beforeIntTs == beforeTs) {
				value.set(beforeVal);
			}
			else {
				double currentWeight =
					static_cast<double>(beforeTs - beforeIntTs);  
				weight += currentWeight;
				weightedSum += beforeVal * currentWeight;
				value.set(weightedSum / weight);
			}
		}
	} break;
	case AGG_COUNT: {
		int64_t counter = 0;
		RowArray rowArray(txn, this);
		util::XArray<OId>::iterator itr;
		for (itr = normalOIdList.begin(); itr != normalOIdList.end(); itr++) {
			rowArray.load(txn, *itr, this, OBJECT_READ_ONLY);
			if (!isExclusive() && txn.getId() != rowArray.getTxnId() &&
				!rowArray.isFirstUpdate() &&
				txn.getManager().isActiveTransaction(
					txn.getPartitionId(), rowArray.getTxnId())) {
				continue;
			}
			for (rowArray.begin(); !rowArray.end(); rowArray.next()) {
				RowArray::Row row(rowArray.getRow(), &rowArray);
				Timestamp rowId = row.getRowId();
				if (((op1 != 0) &&
						!(*op1)(txn, reinterpret_cast<const uint8_t *>(&rowId),
							sizeof(Timestamp),
							reinterpret_cast<const uint8_t *>(sc.startKey_),
							sc.startKeySize_)) ||
					((op2 != 0) &&
						!(*op2)(txn, reinterpret_cast<const uint8_t *>(&rowId),
							sizeof(Timestamp),
							reinterpret_cast<const uint8_t *>(sc.endKey_),
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
					counter++;
				}
			}
		}
		for (itr = mvccOIdList.begin(); itr != mvccOIdList.end(); itr++) {
			rowArray.load(txn, *itr, this, OBJECT_READ_ONLY);
			if (txn.getId() == rowArray.getTxnId()) {
				continue;
			}
			for (rowArray.begin(); !rowArray.end(); rowArray.next()) {
				RowArray::Row row(rowArray.getRow(), &rowArray);
				Timestamp rowId = row.getRowId();
				if (((op1 != 0) &&
						!(*op1)(txn, reinterpret_cast<const uint8_t *>(&rowId),
							sizeof(Timestamp),
							reinterpret_cast<const uint8_t *>(sc.startKey_),
							sc.startKeySize_)) ||
					((op2 != 0) &&
						!(*op2)(txn, reinterpret_cast<const uint8_t *>(&rowId),
							sizeof(Timestamp),
							reinterpret_cast<const uint8_t *>(sc.endKey_),
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
					counter++;
				}
			}
		}
		value.set(counter);
		resultNum = 1;
	} break;
	default: {
		AggregationCursor aggCursor;
		const AggregatorForLoop *aggLoop = &aggLoopTable[type];
		ColumnInfo &aggColumnInfo = getColumnInfo(columnId);
		aggCursor.set(type, aggColumnInfo.getColumnType());

		RowArray rowArray(txn, this);
		util::XArray<OId>::iterator itr;
		for (itr = normalOIdList.begin(); itr != normalOIdList.end(); itr++) {
			rowArray.load(txn, *itr, this, OBJECT_READ_ONLY);
			if (!isExclusive() && txn.getId() != rowArray.getTxnId() &&
				!rowArray.isFirstUpdate() &&
				txn.getManager().isActiveTransaction(
					txn.getPartitionId(), rowArray.getTxnId())) {
				continue;
			}
			for (rowArray.begin(); !rowArray.end(); rowArray.next()) {
				RowArray::Row row(rowArray.getRow(), &rowArray);
				Timestamp rowId = row.getRowId();
				if (((op1 != 0) &&
						!(*op1)(txn, reinterpret_cast<const uint8_t *>(&rowId),
							sizeof(Timestamp),
							reinterpret_cast<const uint8_t *>(sc.startKey_),
							sc.startKeySize_)) ||
					((op2 != 0) &&
						!(*op2)(txn, reinterpret_cast<const uint8_t *>(&rowId),
							sizeof(Timestamp),
							reinterpret_cast<const uint8_t *>(sc.endKey_),
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
					row.getField(txn, aggColumnInfo, containerValue);
					(*aggLoop)(txn, containerValue.getValue(), aggCursor);
				}
			}
		}
		for (itr = mvccOIdList.begin(); itr != mvccOIdList.end(); itr++) {
			rowArray.load(txn, *itr, this, OBJECT_READ_ONLY);
			if (txn.getId() == rowArray.getTxnId()) {
				continue;
			}
			for (rowArray.begin(); !rowArray.end(); rowArray.next()) {
				RowArray::Row row(rowArray.getRow(), &rowArray);
				Timestamp rowId = row.getRowId();
				if (((op1 != 0) &&
						!(*op1)(txn, reinterpret_cast<const uint8_t *>(&rowId),
							sizeof(Timestamp),
							reinterpret_cast<const uint8_t *>(sc.startKey_),
							sc.startKeySize_)) ||
					((op2 != 0) &&
						!(*op2)(txn, reinterpret_cast<const uint8_t *>(&rowId),
							sizeof(Timestamp),
							reinterpret_cast<const uint8_t *>(sc.endKey_),
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
					row.getField(txn, aggColumnInfo, containerValue);
					(*aggLoop)(txn, containerValue.getValue(), aggCursor);
				}
			}
		}

		if (aggCursor.count_ > 0) {
			aggregationPostProcess(aggCursor, value);
			resultNum = 1;
		}
		else {
			resultNum = 0;
		}
	} break;
	}

	if (resultNum > 0) {
		value.serialize(serializedRowList);
	}
}

/*!
	@brief Take a sampling of Rows within a specific range
*/
void TimeSeries::sample(TransactionContext &txn, BtreeMap::SearchContext &sc,
	const Sampling &sampling, ResultSize &resultNum,
	MessageRowStore *messageRowStore) {
	if (sc.startKey_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_KEY_RANGE_INVALID, "");
	}
	if (sc.conditionNum_ > 0) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_FILTERING_CONDITION_INVALID, "");
	}
	if (sc.columnId_ >= getColumnNum()) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_COLUMN_ID_INVALID, "");
	}
	if (sc.endKey_ == NULL) {
		if (sampling.interval_ != 0) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_KEY_RANGE_INVALID, "");
		}
	}
	if (sampling.interval_ != 0) {
		if ((sampling.timeUnit_ < TIME_UNIT_DAY) ||
			(sampling.timeUnit_ > TIME_UNIT_MILLISECOND)) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_TIM_SAMPLING_TIME_UNIT_INVALID, "");
		}
	}
	for (size_t i = 0; i < sampling.interpolatedColumnIdList_.size(); i++) {
		if (sampling.interpolatedColumnIdList_[i] >= getColumnNum()) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_COLUMN_ID_INVALID, "");
		}
		ColumnType type = getColumnInfo(sampling.interpolatedColumnIdList_[i])
							  .getColumnType();
		if (!ValueProcessor::isNumerical(type)) {
			GS_THROW_USER_ERROR(
				GS_ERROR_DS_TIM_INTERPORATED_COLUMN_TYPE_INVALID, "");
		}
	}

	Timestamp interval = 0;
	if (sampling.interval_ != 0) {
		switch (sampling.timeUnit_) {
		case TIME_UNIT_DAY:
			interval = static_cast<Timestamp>(sampling.interval_) * 24 * 60 *
					   60 * 1000LL;
			break;
		case TIME_UNIT_HOUR:
			interval =
				static_cast<Timestamp>(sampling.interval_) * 60 * 60 * 1000LL;
			break;
		case TIME_UNIT_MINUTE:
			interval = static_cast<Timestamp>(sampling.interval_) * 60 * 1000LL;
			break;
		case TIME_UNIT_SECOND:
			interval = static_cast<Timestamp>(sampling.interval_) * 1000LL;
			break;
		case TIME_UNIT_MILLISECOND:
			interval = static_cast<Timestamp>(sampling.interval_);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_DS_TIM_SAMPLING_TIME_UNIT_INVALID, "");
		}
	}

	const Operator *op1, *op2;
	bool isValid = getKeyCondition(txn, sc, op1, op2);
	if (!isValid) {
		resultNum = messageRowStore->getRowCount();
		return;
	}

	Timestamp targetT = *reinterpret_cast<const Timestamp *>(sc.startKey_);
	{
		if (interval != 0) {
			Timestamp expiredTime = getCurrentExpiredTime(txn);
			Timestamp oldStartTime =
				*reinterpret_cast<const Timestamp *>(sc.startKey_);
			if (oldStartTime < expiredTime) {
				Timestamp diffTime = expiredTime - oldStartTime;
				int64_t div = diffTime / interval;
				int64_t mod = diffTime % interval;
				Timestamp newStartTime;
				if (mod == 0) {
					newStartTime = oldStartTime + (interval * div);
				}
				else {
					newStartTime = oldStartTime + (interval * (div + 1));
				}
				targetT = newStartTime;
			}
		}
	}

	Timestamp expiredTime = getCurrentExpiredTime(txn);
	const Timestamp *startKeyPtr =
		reinterpret_cast<const Timestamp *>(sc.startKey_);
	if (expiredTime != MINIMUM_EXPIRED_TIMESTAMP &&
		(startKeyPtr == NULL || *startKeyPtr < expiredTime)) {
		sc.startKey_ = &expiredTime;
	}

	ResultSize limit = sc.limit_;
	if (interval != 0) {
		sc.limit_ = MAX_RESULT_SIZE;
	}
	util::XArray<OId> normalOIdList(txn.getDefaultAllocator());
	util::XArray<OId> mvccOIdList(txn.getDefaultAllocator());
	searchRowArrayList(txn, sc, normalOIdList, mvccOIdList);
	sc.limit_ = limit;

	util::XArray<OId>::iterator normalItr, mvccItr;
	RowArray normalRowArray(txn, this);
	RowArray mvccRowArray(txn, this);
	RowArray prevRowArray(txn, this);
	RowArray *normalRowArrayPtr = &normalRowArray;
	RowArray *mvccRowArrayPtr = &mvccRowArray;
	RowArray *rowArrayPtr = NULL;

	Timestamp endT = UNDEF_TIMESTAMP;
	if (sc.endKey_ != NULL) {
		endT = *reinterpret_cast<const Timestamp *>(sc.endKey_);
	}
	if (!sampling.interpolatedColumnIdList_.empty()) {
		Timestamp normalEndTime = -1, mvccEndTime = -1;
		RowArray rowArray(txn, this);
		util::XArray<OId>::reverse_iterator rItr;
		if (!normalOIdList.empty()) {
			for (rItr = normalOIdList.rbegin(); rItr != normalOIdList.rend();
				 rItr++) {
				rowArray.load(txn, *rItr, this, OBJECT_READ_ONLY);
				if (!isExclusive() && txn.getId() != rowArray.getTxnId() &&
					!rowArray.isFirstUpdate() &&
					txn.getManager().isActiveTransaction(
						txn.getPartitionId(), rowArray.getTxnId())) {
					continue;
				}
				if (rowArray.tail()) {
					RowArray::Row row(rowArray.getRow(), &rowArray);
					normalEndTime = row.getRowId();
					break;
				}
			}
		}
		if (normalEndTime < endT) {
			if (!mvccOIdList.empty()) {
				for (rItr = mvccOIdList.rbegin(); rItr != mvccOIdList.rend();
					 rItr++) {
					rowArray.load(txn, *rItr, this, OBJECT_READ_ONLY);
					if (txn.getId() == rowArray.getTxnId()) {
						continue;
					}
					if (rowArray.tail()) {
						RowArray::Row row(rowArray.getRow(), &rowArray);
						mvccEndTime = row.getRowId();
						break;
					}
				}
			}
		}
		{
			bool isNeedNext = false;
			if (normalEndTime >= mvccEndTime) {
				if (normalEndTime != -1 && normalEndTime < endT) {
					isNeedNext = true;
				}
			}
			else {
				if (mvccEndTime != -1 && mvccEndTime < endT) {
					isNeedNext = true;
				}
			}
			if (isNeedNext) {
				BtreeMap::SearchContext sc(ColumnInfo::ROW_KEY_COLUMN_ID, &endT,
					0, true, NULL, 0, true, 0, NULL, 1);
				util::XArray<OId> postOIdList(txn.getDefaultAllocator());
				searchRowIdIndex(txn, sc, postOIdList, ORDER_ASCENDING);
				if (!postOIdList.empty()) {
					normalOIdList.push_back(postOIdList[0]);
				}
			}
		}
	}

	Timestamp normalBeginTime = -1, mvccBeginTime = -1;
	{
		for (normalItr = normalOIdList.begin();
			 normalItr != normalOIdList.end(); normalItr++) {
			normalRowArrayPtr->load(txn, *normalItr, this, OBJECT_READ_ONLY);
			if (!isExclusive() &&
				txn.getId() != normalRowArrayPtr->getTxnId() &&
				!normalRowArrayPtr->isFirstUpdate() &&
				txn.getManager().isActiveTransaction(
					txn.getPartitionId(), normalRowArrayPtr->getTxnId())) {
				continue;
			}
			if (normalRowArrayPtr->begin()) {
				RowArray::Row row(
					normalRowArrayPtr->getRow(), normalRowArrayPtr);
				normalBeginTime = row.getRowId();
				break;
			}
		}
		for (mvccItr = mvccOIdList.begin(); mvccItr != mvccOIdList.end();
			 mvccItr++) {
			mvccRowArrayPtr->load(txn, *mvccItr, this, OBJECT_READ_ONLY);
			if (txn.getId() == mvccRowArrayPtr->getTxnId()) {
				continue;
			}
			if (mvccRowArrayPtr->begin()) {
				RowArray::Row row(mvccRowArrayPtr->getRow(), mvccRowArrayPtr);
				mvccBeginTime = row.getRowId();
				break;
			}
		}
	}

	Timestamp prevT = UNDEF_TIMESTAMP;
	OId prevOId = UNDEF_OID;
	{
		bool isNeedPrev = false;
		if (normalBeginTime != -1 && normalBeginTime <= mvccBeginTime &&
			normalBeginTime > targetT) {
			isNeedPrev = true;
		}
		else if (mvccBeginTime != -1 && mvccBeginTime > targetT) {
			isNeedPrev = true;
		}
		if (isNeedPrev) {
			BtreeMap::SearchContext sc(ColumnInfo::ROW_KEY_COLUMN_ID, NULL, 0,
				true, &targetT, 0, true, 0, NULL, 1);
			util::XArray<OId> prevOIdList(txn.getDefaultAllocator());
			searchRowIdIndex(txn, sc, prevOIdList, ORDER_DESCENDING);
			if (!prevOIdList.empty()) {
				prevRowArray.load(txn, prevOIdList[0], this, OBJECT_READ_ONLY);
				RowArray::Row prevRow(prevRowArray.getRow(), &prevRowArray);
				prevT = static_cast<Timestamp>(prevRow.getRowId());
				prevOId = prevOIdList[0];
			}
		}
	}
	ContainerValue containerValue(txn, *getObjectManager());
	ContainerValue prevContainerValue(txn, *getObjectManager());
	ContainerValue nextContainerValue(txn, *getObjectManager());
	Value subValue;
	Value mulValue;
	Value addValue;

	util::XArray<OpForSample> opList(txn.getDefaultAllocator());
	opList.resize(getColumnNum());
	for (uint32_t columnId = 0; columnId < getColumnNum(); columnId++) {
		opList[columnId].isInterpolated_ = false;
	}
	for (size_t i = 0; i < sampling.interpolatedColumnIdList_.size(); i++) {
		uint32_t columnId = sampling.interpolatedColumnIdList_[i];
		ColumnType type = getColumnInfo(columnId).getColumnType();
		opList[columnId].isInterpolated_ = true;
		opList[columnId].sub_ = subTable[type][type];
		opList[columnId].mul_ = mulTable[type][COLUMN_TYPE_DOUBLE];
		opList[columnId].add_ = addTable[type][COLUMN_TYPE_DOUBLE];
	}

	bool isWithRowId = false;  

	while (normalItr != normalOIdList.end() || mvccItr != mvccOIdList.end()) {
		bool isNoramlExecute = false;
		if (mvccItr == mvccOIdList.end() ||
			(normalItr != normalOIdList.end() &&
				normalBeginTime < mvccBeginTime)) {
			rowArrayPtr = normalRowArrayPtr;
			isNoramlExecute = true;
		}
		else {
			rowArrayPtr = mvccRowArrayPtr;
			isNoramlExecute = false;
		}

		if ((isNoramlExecute &&
				(isExclusive() || txn.getId() == rowArrayPtr->getTxnId() ||
					rowArrayPtr->isFirstUpdate() ||
					!txn.getManager().isActiveTransaction(
						txn.getPartitionId(), rowArrayPtr->getTxnId()))) ||
			(!isNoramlExecute && txn.getId() != rowArrayPtr->getTxnId())) {
			for (rowArrayPtr->begin(); !rowArrayPtr->end();
				 rowArrayPtr->next()) {
				RowArray::Row row(rowArrayPtr->getRow(), rowArrayPtr);
				Timestamp rowId = row.getRowId();
				if (rowId < expiredTime) {
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
				if (!isMatch) {
					continue;
				}
				if (targetT == rowId) {  

					row.getImage(txn, messageRowStore, isWithRowId);
					messageRowStore->next();

					if (interval == 0) {
						goto LABEL_FINISH;
					}
					if (messageRowStore->getRowCount() >= sc.limit_) {
						goto LABEL_FINISH;
					}

					targetT += interval;
					if (endT != UNDEF_TIMESTAMP && targetT > endT) {
						goto LABEL_FINISH;
					}
				}
				else if (targetT < rowId) {  
					while (true) {
						if (prevT != UNDEF_TIMESTAMP) {
							prevRowArray.load(
								txn, prevOId, this, OBJECT_READ_ONLY);
							RowArray::Row prevRow(
								prevRowArray.getRow(), &prevRowArray);
							messageRowStore->beginRow();

							messageRowStore->setField<COLUMN_TYPE_TIMESTAMP>(
								0, targetT);

							for (uint32_t columnId = 1;
								 columnId < getColumnNum(); columnId++) {
								ColumnInfo &columnInfo =
									getColumnInfo(columnId);
								prevRow.getField(
									txn, columnInfo, prevContainerValue);
								if (opList[columnId]
										.isInterpolated_) {  
									row.getField(
										txn, columnInfo, nextContainerValue);

									double rate =
										static_cast<double>(targetT - prevT) /
										static_cast<double>(rowId - prevT);
									opList[columnId].sub_(txn,
										nextContainerValue.getValue().data(), 0,
										prevContainerValue.getValue().data(), 0,
										subValue);
									opList[columnId].mul_(txn, subValue.data(),
										0, reinterpret_cast<uint8_t *>(&rate),
										0, mulValue);
									opList[columnId].add_(txn,
										prevContainerValue.getValue().data(), 0,
										mulValue.data(), 0, addValue);

									switch (prevContainerValue.getValue()
												.getType()) {
									case COLUMN_TYPE_BYTE: {
										int8_t val = static_cast<int8_t>(
											addValue.getDouble());
										messageRowStore
											->setField<COLUMN_TYPE_BYTE>(
												columnId, val);
									} break;
									case COLUMN_TYPE_SHORT: {
										int16_t val = static_cast<int16_t>(
											addValue.getDouble());
										messageRowStore
											->setField<COLUMN_TYPE_SHORT>(
												columnId, val);
									} break;
									case COLUMN_TYPE_INT: {
										int32_t val = static_cast<int32_t>(
											addValue.getDouble());
										messageRowStore
											->setField<COLUMN_TYPE_INT>(
												columnId, val);
									} break;
									case COLUMN_TYPE_LONG: {
										int64_t val = static_cast<int64_t>(
											addValue.getDouble());
										messageRowStore
											->setField<COLUMN_TYPE_LONG>(
												columnId, val);
									} break;
									case COLUMN_TYPE_FLOAT: {
										float val = static_cast<float>(
											addValue.getDouble());
										messageRowStore
											->setField<COLUMN_TYPE_FLOAT>(
												columnId, val);
									} break;
									case COLUMN_TYPE_DOUBLE: {
										double val = static_cast<float>(
											addValue.getDouble());
										messageRowStore
											->setField<COLUMN_TYPE_DOUBLE>(
												columnId, val);
									} break;
									default:
										GS_THROW_USER_ERROR(
											GS_ERROR_DS_TIM_INTERPORATED_COLUMN_TYPE_INVALID,
											"");
									}
								}
								else {  
									prevRow.getFieldImage(txn, columnInfo,
										columnInfo.getColumnId(),
										messageRowStore);
								}
							}
							messageRowStore->next();
						}
						else {  
						}

						if (interval == 0) {
							goto LABEL_FINISH;
						}
						if (messageRowStore->getRowCount() >= sc.limit_) {
							goto LABEL_FINISH;
						}
						targetT += interval;
						if (endT != UNDEF_TIMESTAMP && targetT > endT) {
							goto LABEL_FINISH;
						}

						if (targetT == rowId) {
							row.getImage(txn, messageRowStore, isWithRowId);
							messageRowStore->next();
							if (messageRowStore->getRowCount() >= sc.limit_) {
								goto LABEL_FINISH;
							}

							targetT += interval;
							if (endT != UNDEF_TIMESTAMP && targetT > endT) {
								goto LABEL_FINISH;
							}
						}

						if (targetT > rowId) {
							break;
						}
					}
				}

				prevT = rowId;
				prevOId = rowArrayPtr->getOId();
			}
		}
		if (isNoramlExecute) {
			normalItr++;
		}
		else {
			mvccItr++;
		}
		if (normalItr != normalOIdList.end()) {
			normalRowArrayPtr->load(txn, *normalItr, this, OBJECT_READ_ONLY);
			if (normalRowArrayPtr->begin()) {
				RowArray::Row row(
					normalRowArrayPtr->getRow(), normalRowArrayPtr);
				normalBeginTime = row.getRowId();
			}
		}
		if (mvccItr != mvccOIdList.end()) {
			mvccRowArrayPtr->load(txn, *mvccItr, this, OBJECT_READ_ONLY);
			if (mvccRowArrayPtr->begin()) {
				RowArray::Row row(mvccRowArrayPtr->getRow(), mvccRowArrayPtr);
				mvccBeginTime = row.getRowId();
			}
		}
	}

	if (prevT != UNDEF_TIMESTAMP && interval != 0 && targetT < endT &&
		sampling.interpolatedColumnIdList_.empty()) {
		while (targetT <= endT) {  
			prevRowArray.load(txn, prevOId, this, OBJECT_READ_ONLY);
			RowArray::Row prevRow(prevRowArray.getRow(), &prevRowArray);

			messageRowStore->beginRow();

			messageRowStore->setField<COLUMN_TYPE_TIMESTAMP>(0, targetT);

			for (uint32_t columnId = 1; columnId < getColumnNum(); columnId++) {
				ColumnInfo &columnInfo = getColumnInfo(columnId);
				prevRow.getFieldImage(
					txn, columnInfo, columnInfo.getColumnId(), messageRowStore);
			}
			messageRowStore->next();

			if (messageRowStore->getRowCount() >= sc.limit_) {
				goto LABEL_FINISH;
			}
			targetT += interval;
		}
	}
LABEL_FINISH:
	resultNum = messageRowStore->getRowCount();
}

/*!
	@brief Take a sampling of Rows within a specific range without interpolation
*/
void TimeSeries::sampleWithoutInterp(TransactionContext &txn,
	BtreeMap::SearchContext &sc, const Sampling &sampling,
	ResultSize &resultNum, MessageRowStore *messageRowStore) {
	if (sc.startKey_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_KEY_RANGE_INVALID, "");
	}
	if (sc.conditionNum_ > 0) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_FILTERING_CONDITION_INVALID, "");
	}
	if (sc.columnId_ >= getColumnNum()) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_COLUMN_ID_INVALID, "");
	}
	if (sc.endKey_ == NULL) {
		if (sampling.interval_ != 0) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_KEY_RANGE_INVALID, "");
		}
	}
	if (sampling.interval_ != 0) {
		if ((sampling.timeUnit_ < TIME_UNIT_DAY) ||
			(sampling.timeUnit_ > TIME_UNIT_MILLISECOND)) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_TIM_SAMPLING_TIME_UNIT_INVALID, "");
		}
	}
	if (sampling.interpolatedColumnIdList_.size() != 0) {
		GS_THROW_USER_ERROR(
			GS_ERROR_DS_TIM_INTERPORATED_COLUMN_IDLIST_INVALID, "");
	}

	Timestamp interval = 0;
	if (sampling.interval_ != 0) {
		switch (sampling.timeUnit_) {
		case TIME_UNIT_DAY:
			interval = static_cast<Timestamp>(sampling.interval_) * 24 * 60 *
					   60 * 1000LL;
			break;
		case TIME_UNIT_HOUR:
			interval =
				static_cast<Timestamp>(sampling.interval_) * 60 * 60 * 1000LL;
			break;
		case TIME_UNIT_MINUTE:
			interval = static_cast<Timestamp>(sampling.interval_) * 60 * 1000LL;
			break;
		case TIME_UNIT_SECOND:
			interval = static_cast<Timestamp>(sampling.interval_) * 1000LL;
			break;
		case TIME_UNIT_MILLISECOND:
			interval = static_cast<Timestamp>(sampling.interval_);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_DS_TIM_SAMPLING_TIME_UNIT_INVALID, "");
		}
	}

	const Operator *op1, *op2;
	bool isValid = getKeyCondition(txn, sc, op1, op2);
	if (!isValid) {
		resultNum = messageRowStore->getRowCount();
		return;
	}

	Timestamp targetT = *reinterpret_cast<const Timestamp *>(sc.startKey_);
	{
		if (interval != 0) {
			Timestamp expiredTime = getCurrentExpiredTime(txn);
			Timestamp oldStartTime =
				*reinterpret_cast<const Timestamp *>(sc.startKey_);
			if (oldStartTime < expiredTime) {
				Timestamp diffTime = expiredTime - oldStartTime;
				int64_t div = diffTime / interval;
				int64_t mod = diffTime % interval;
				Timestamp newStartTime;
				if (mod == 0) {
					newStartTime = oldStartTime + (interval * div);
				}
				else {
					newStartTime = oldStartTime + (interval * (div + 1));
				}
				targetT = newStartTime;
			}
		}
	}

	Timestamp expiredTime = getCurrentExpiredTime(txn);
	const Timestamp *startKeyPtr =
		reinterpret_cast<const Timestamp *>(sc.startKey_);
	if (expiredTime != MINIMUM_EXPIRED_TIMESTAMP &&
		(startKeyPtr == NULL || *startKeyPtr < expiredTime)) {
		sc.startKey_ = &expiredTime;
	}

	ResultSize limit = sc.limit_;
	if (interval != 0) {
		sc.limit_ = MAX_RESULT_SIZE;
	}
	util::XArray<OId> normalOIdList(txn.getDefaultAllocator());
	util::XArray<OId> mvccOIdList(txn.getDefaultAllocator());
	searchRowArrayList(txn, sc, normalOIdList, mvccOIdList);
	sc.limit_ = limit;

	util::XArray<OId>::iterator normalItr, mvccItr;
	RowArray normalRowArray(txn, this);
	RowArray mvccRowArray(txn, this);
	RowArray *normalRowArrayPtr = &normalRowArray;
	RowArray *mvccRowArrayPtr = &mvccRowArray;
	RowArray *rowArrayPtr = NULL;

	Timestamp normalBeginTime = -1, mvccBeginTime = -1;
	{
		for (normalItr = normalOIdList.begin();
			 normalItr != normalOIdList.end(); normalItr++) {
			normalRowArrayPtr->load(txn, *normalItr, this, OBJECT_READ_ONLY);
			if (!isExclusive() &&
				txn.getId() != normalRowArrayPtr->getTxnId() &&
				!normalRowArrayPtr->isFirstUpdate() &&
				txn.getManager().isActiveTransaction(
					txn.getPartitionId(), normalRowArrayPtr->getTxnId())) {
				continue;
			}
			if (normalRowArrayPtr->begin()) {
				RowArray::Row row(
					normalRowArrayPtr->getRow(), normalRowArrayPtr);
				normalBeginTime = row.getRowId();
				break;
			}
		}
		for (mvccItr = mvccOIdList.begin(); mvccItr != mvccOIdList.end();
			 mvccItr++) {
			mvccRowArrayPtr->load(txn, *mvccItr, this, OBJECT_READ_ONLY);
			if (txn.getId() == mvccRowArrayPtr->getTxnId()) {
				continue;
			}
			if (mvccRowArrayPtr->begin()) {
				RowArray::Row row(mvccRowArrayPtr->getRow(), mvccRowArrayPtr);
				mvccBeginTime = row.getRowId();
				break;
			}
		}
	}

	ContainerValue containerValue(txn, *getObjectManager());
	Value value;
	bool isWithRowId = false;  
	while (normalItr != normalOIdList.end() || mvccItr != mvccOIdList.end()) {
		bool isNoramlExecute = false;
		if (mvccItr == mvccOIdList.end() ||
			(normalItr != normalOIdList.end() &&
				normalBeginTime < mvccBeginTime)) {
			rowArrayPtr = normalRowArrayPtr;
			isNoramlExecute = true;
		}
		else {
			rowArrayPtr = mvccRowArrayPtr;
			isNoramlExecute = false;
		}

		if ((isNoramlExecute &&
				(isExclusive() || txn.getId() == rowArrayPtr->getTxnId() ||
					rowArrayPtr->isFirstUpdate() ||
					!txn.getManager().isActiveTransaction(
						txn.getPartitionId(), rowArrayPtr->getTxnId()))) ||
			(!isNoramlExecute && txn.getId() != rowArrayPtr->getTxnId())) {
			for (rowArrayPtr->begin(); !rowArrayPtr->end();
				 rowArrayPtr->next()) {
				RowArray::Row row(rowArrayPtr->getRow(), rowArrayPtr);
				Timestamp rowId = row.getRowId();
				if (rowId < expiredTime) {
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
				if (!isMatch) {
					continue;
				}

				if (sc.endKey_ != NULL &&
					rowId > *reinterpret_cast<const Timestamp *>(sc.endKey_)) {
					goto LABEL_FINISH;
				}

				if (targetT == rowId) {  

					row.getImage(txn, messageRowStore, isWithRowId);
					messageRowStore->next();

					if (interval == 0) {
						goto LABEL_FINISH;
					}
					if (messageRowStore->getRowCount() >= sc.limit_) {
						goto LABEL_FINISH;
					}

					targetT += interval;
				}
				else if (targetT < rowId) {  

					while (true) {
						messageRowStore->beginRow();

						messageRowStore->setField<COLUMN_TYPE_TIMESTAMP>(
							ColumnInfo::ROW_KEY_COLUMN_ID, targetT);
						for (uint32_t columnId = 1; columnId < getColumnNum();
							 columnId++) {
							ColumnInfo &columnInfo = getColumnInfo(columnId);
							value.init(columnInfo.getColumnType());
							ValueProcessor::getField(txn, *getObjectManager(),
								columnId, &value, messageRowStore);
						}
						messageRowStore->next();

						if (interval == 0) {
							goto LABEL_FINISH;
						}
						if (messageRowStore->getRowCount() >= sc.limit_) {
							goto LABEL_FINISH;
						}
						targetT += interval;


						if (targetT == rowId) {
							row.getImage(txn, messageRowStore, isWithRowId);
							messageRowStore->next();
							if (messageRowStore->getRowCount() >= sc.limit_) {
								goto LABEL_FINISH;
							}

							targetT += interval;
						}
						if (targetT > rowId) {
							break;
						}
					}
				}

				if (sc.endKey_ != NULL &&
					targetT >
						*reinterpret_cast<const Timestamp *>(sc.endKey_)) {
					goto LABEL_FINISH;
				}
			}
		}
		if (isNoramlExecute) {
			normalItr++;
		}
		else {
			mvccItr++;
		}
		if (normalItr != normalOIdList.end()) {
			normalRowArrayPtr->load(txn, *normalItr, this, OBJECT_READ_ONLY);
			if (normalRowArrayPtr->begin()) {
				RowArray::Row row(
					normalRowArrayPtr->getRow(), normalRowArrayPtr);
				normalBeginTime = row.getRowId();
			}
		}
		if (mvccItr != mvccOIdList.end()) {
			mvccRowArrayPtr->load(txn, *mvccItr, this, OBJECT_READ_ONLY);
			if (mvccRowArrayPtr->begin()) {
				RowArray::Row row(mvccRowArrayPtr->getRow(), mvccRowArrayPtr);
				mvccBeginTime = row.getRowId();
			}
		}
	}

LABEL_FINISH:
	resultNum = messageRowStore->getRowCount();
	if (resultNum < sc.limit_ && interval != 0) {
		size_t num =
			(size_t)((*reinterpret_cast<const Timestamp *>(sc.endKey_) -
						 *reinterpret_cast<const Timestamp *>(sc.startKey_)) /
					 interval) +
			1;

		for (ResultSize i = resultNum; i < num; i++) {
			targetT = *reinterpret_cast<const Timestamp *>(sc.startKey_) +
					  interval * i;

			messageRowStore->beginRow();

			messageRowStore->setField<COLUMN_TYPE_TIMESTAMP>(
				ColumnInfo::ROW_KEY_COLUMN_ID, targetT);
			for (uint32_t columnId = 1; columnId < getColumnNum(); columnId++) {
				ColumnInfo &columnInfo = getColumnInfo(columnId);
				value.init(columnInfo.getColumnType());
				ValueProcessor::getField(txn, *getObjectManager(), columnId,
					&value, messageRowStore);
			}
			messageRowStore->next();
			if (messageRowStore->getRowCount() >= sc.limit_) {
				break;
			}
		}
		resultNum = messageRowStore->getRowCount();
	}
}

/*!
	@brief Returns one Row related with the specified time
*/
void TimeSeries::searchTimeOperator(
	TransactionContext &txn, Timestamp ts, TimeOperator timeOp, OId &oId) {
	util::XArray<OId> oIdList(txn.getDefaultAllocator());
	switch (timeOp) {
	case TIME_PREV_ONLY: {
		BtreeMap::SearchContext sc(ColumnInfo::ROW_KEY_COLUMN_ID, NULL, 0, true,
			&ts, 0, false, 0, NULL, 1);
		searchRowIdIndex(txn, sc, oIdList, ORDER_DESCENDING);
	} break;
	case TIME_PREV: {
		BtreeMap::SearchContext sc(ColumnInfo::ROW_KEY_COLUMN_ID, NULL, 0, true,
			&ts, 0, true, 0, NULL, 1);
		searchRowIdIndex(txn, sc, oIdList, ORDER_DESCENDING);
	} break;
	case TIME_NEXT: {
		BtreeMap::SearchContext sc(ColumnInfo::ROW_KEY_COLUMN_ID, &ts, 0, true,
			NULL, 0, true, 0, NULL, 1);
		searchRowIdIndex(txn, sc, oIdList, ORDER_ASCENDING);
	} break;
	case TIME_NEXT_ONLY: {
		BtreeMap::SearchContext sc(ColumnInfo::ROW_KEY_COLUMN_ID, &ts, 0, false,
			NULL, 0, true, 0, NULL, 1);
		searchRowIdIndex(txn, sc, oIdList, ORDER_ASCENDING);
	} break;
	}
	if (oIdList.empty()) {
		oId = UNDEF_OID;
	}
	else {
		oId = oIdList[0];
	}
}

/*!
	@brief Lock Rows
*/
void TimeSeries::lockRowList(
	TransactionContext &txn, util::XArray<RowId> &rowIdList) {
	try {
		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not lock. container's status is invalid.");
		}

		for (size_t i = 0; i < rowIdList.size(); i++) {
			util::StackAllocator::Scope scope(txn.getDefaultAllocator());
			BtreeMap::SearchContext sc(UNDEF_COLUMNID, &rowIdList[i], 0, true,
				&rowIdList[i], 0, true, 0, NULL, 1);
			util::XArray<OId> oIdList(txn.getDefaultAllocator());
			searchRowIdIndex(txn, sc, oIdList, ORDER_UNDEFINED);
			if (!oIdList.empty()) {
				RowArray rowArray(txn, oIdList[0], this, OBJECT_FOR_UPDATE);
				RowArray::Row row(rowArray.getRow(), &rowArray);
				row.lock(txn);
			}
			else {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_COL_GET_LOCK_ID_INVALID,
					"rowId" << rowIdList[i] << "is not found");
			}
		}
	}
	catch (std::exception &e) {
		handleSearchError(txn, e, GS_ERROR_DS_TIM_GET_LOCK_ID_INVALID);
	}
}

/*!
	@brief Check if Row is locked
*/
bool TimeSeries::isLocked(
	TransactionContext &txn, uint32_t, const uint8_t *rowKey) {
	try {
		Timestamp rowKeyTimestamp =
			*(reinterpret_cast<const Timestamp *>(rowKey));
		if (!ValueProcessor::validateTimestamp(rowKeyTimestamp)) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_COL_ROWKEY_INVALID,
				"Timestamp of rowKey out of range (rowKey=" << rowKeyTimestamp
															<< ")");
		}

		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		util::XArray<OId> oIdList(txn.getDefaultAllocator());
		util::XArray<OId>::iterator itr;
		BtreeMap::SearchContext sc(
			ColumnInfo::ROW_KEY_COLUMN_ID, &rowKey, 0, 0, NULL, 1);
		searchRowIdIndex(txn, sc, oIdList, ORDER_ASCENDING);

		if (!oIdList.empty()) {
			RowArray rowArray(txn, oIdList[0], this, OBJECT_READ_ONLY);
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
	catch (std::exception &e) {
		handleSearchError(txn, e, GS_ERROR_DS_TIM_GET_LOCK_ID_INVALID);
	}

	return false;
}

/*!
	@brief Get max expired time
*/
Timestamp TimeSeries::getCurrentExpiredTime(TransactionContext &txn) {
	ExpirationInfo &expirationInfo = getExpirationInfo();
	Timestamp currentTime = txn.getStatementStartTime().getUnixTime();
	if (expirationInfo.duration_ == INT64_MAX) {
		return MINIMUM_EXPIRED_TIMESTAMP;  
	}
	else {
		Timestamp lastExpiredTime = DataStore::convertChunkKey2Timestamp(
			getDataStore()->getLastChunkKey(txn));
		Timestamp calcExpiredTime = currentTime;
		if (calcExpiredTime >= lastExpiredTime) {
			return calcExpiredTime - expirationInfo.duration_;
		}
		else {
			return lastExpiredTime - expirationInfo.duration_;
		}
	}
}


void TimeSeries::putRow(TransactionContext &txn,
	InputMessageRowStore *inputMessageRowStore, RowId &rowId,
	DataStore::PutStatus &status, PutRowOption putRowOption) {
	util::StackAllocator::Scope scope(txn.getDefaultAllocator());
	const uint8_t *rowKey;
	uint32_t rowKeySize;
	inputMessageRowStore->getField(
		ColumnInfo::ROW_KEY_COLUMN_ID, rowKey, rowKeySize);


	Timestamp rowKeyTimestamp = *(reinterpret_cast<const Timestamp *>(rowKey));
	SubTimeSeries *subContainer = putSubContainer(txn, rowKeyTimestamp);
	if (subContainer == NULL) {
		rowId = UNDEF_ROWID;
		status = DataStore::NOT_EXECUTED;
		return;
	}
	subContainer->putRow(
		txn, inputMessageRowStore, rowId, status, putRowOption);
}


SubTimeSeries *TimeSeries::putSubContainer(
	TransactionContext &txn, Timestamp rowKey) {
	SubTimeSeries *container = NULL;
	Timestamp expiredTime = getCurrentExpiredTime(txn);
	if (rowKey <= expiredTime) {
		return NULL;
	}
	expireSubTimeSeries(txn, expiredTime);

	{
		if (subTimeSeriesListHead_->getNum() == 0) {
			int64_t firstPos = 0;
			SubTimeSeriesImage defaultImage;
			subTimeSeriesListHead_->insert(
				txn, firstPos, &defaultImage, getMetaAllcateStrategy());
			replaceSubTimeSeriesList(txn);

			if (getExpirationInfo().duration_ == INT64_MAX) {
				setBaseTime(MINIMUM_EXPIRED_TIMESTAMP);
			}
			else {
				setBaseTime(rowKey);
			}
			setTailNodeOId(UNDEF_OID);

			container =
				ALLOC_NEW(txn.getDefaultAllocator()) SubTimeSeries(txn, this);
			container->initialize(firstPos, getBaseTime());
			container->set(txn, this);

			return container;
		}
	}

	if (rowKey >= getBaseTime() &&
		rowKey < getBaseTime() + getDivideDuration()) {
		const SubTimeSeriesImage &subTimeSeriesImage =
			*(subTimeSeriesListHead_->get(txn, getBasePos()));
		container = ALLOC_NEW(txn.getDefaultAllocator()) SubTimeSeries(
			txn, SubTimeSeriesInfo(subTimeSeriesImage, getBasePos()), this);
	}
	else {
		int64_t minPos = 0;
		findStartSubTimeSeriesPos(
			txn, rowKey, 0, subTimeSeriesListHead_->getNum() - 1, minPos);
		bool isNewSubTimeSeries = true;
		if (minPos >= 0) {
			const SubTimeSeriesImage &subTimeSeriesImage =
				*subTimeSeriesListHead_->get(txn, minPos);
			if (rowKey >= subTimeSeriesImage.startTime_ &&
				rowKey < subTimeSeriesImage.startTime_ + getDivideDuration()) {
				container = ALLOC_NEW(txn.getDefaultAllocator()) SubTimeSeries(
					txn, SubTimeSeriesInfo(subTimeSeriesImage, minPos), this);
				isNewSubTimeSeries = false;
			}
		}
		if (!isNewSubTimeSeries) {
			const SubTimeSeriesImage &subTimeSeriesImage =
				*subTimeSeriesListHead_->get(txn, minPos);
			container = ALLOC_NEW(txn.getDefaultAllocator()) SubTimeSeries(
				txn, SubTimeSeriesInfo(subTimeSeriesImage, minPos), this);
		}
		else {
			int64_t insertPos =
				minPos +
				1;  
			int64_t distance;
			if (rowKey >= getBaseTime()) {
				distance = (getBaseTime() - rowKey) / getDivideDuration();
			}
			else {
				distance =
					(getBaseTime() - rowKey - 1) / getDivideDuration() + 1;
			}
			Timestamp startTime =
				getBaseTime() - (distance * getDivideDuration());

			SubTimeSeriesImage defaultImage;
			subTimeSeriesListHead_->insert(
				txn, insertPos, &defaultImage, getMetaAllcateStrategy());
			replaceSubTimeSeriesList(txn);

			if (rowKey > getBaseTime()) {
				setBaseTime(startTime);
				setTailNodeOId(UNDEF_OID);
			}

			container =
				ALLOC_NEW(txn.getDefaultAllocator()) SubTimeSeries(txn, this);
			container->initialize(insertPos, startTime);
			container->set(txn, this);
		}
	}
	return container;
}

bool TimeSeries::findStartSubTimeSeriesPos(TransactionContext &txn,
	Timestamp targetBeginTime, int64_t lowPos, int64_t highPos,
	int64_t &midPos) {
	if (subTimeSeriesListHead_->getNum() == 0) {
		return false;
	}

	midPos = 0;
	while (lowPos <= highPos) {
		midPos = ((lowPos + highPos) >> 1);  
		const SubTimeSeriesImage &subTimeSeriesImage =
			*(subTimeSeriesListHead_->get(txn, midPos));
		if (subTimeSeriesImage.startTime_ == targetBeginTime) {
			return true;
		}
		else if (subTimeSeriesImage.startTime_ < targetBeginTime) {
			lowPos = midPos + 1;
		}
		else {
			highPos = midPos - 1;
		}
	}
	const SubTimeSeriesImage &subTimeSeriesImage =
		*(subTimeSeriesListHead_->get(txn, midPos));
	if (subTimeSeriesImage.startTime_ > targetBeginTime) {
		midPos--;
	}
	return false;
}

int32_t TimeSeries::expireSubTimeSeries(
	TransactionContext &txn, Timestamp expiredTime) {
	int32_t expireNum = 0;
	for (uint64_t i = 0; i < subTimeSeriesListHead_->getNum(); i++) {
		const SubTimeSeriesImage &subTimeSeriesImage =
			*(subTimeSeriesListHead_->get(txn, i));
		if (subTimeSeriesImage.startTime_ + getDivideDuration() <=
			expiredTime) {
			expireNum++;
		}
		else {
			break;
		}
	}
	if (expireNum > 0) {
		for (int32_t i = 0; i < expireNum; i++) {
			finalizeExpireIndex(txn, 0);
			subTimeSeriesListHead_->remove(txn, 0);
		}
	}
	return expireNum;
}

SubTimeSeries *TimeSeries::getSubContainer(
	TransactionContext &txn, Timestamp rowKey, bool forUpdate) {
	BtreeMap::SearchContext sc(
		ColumnInfo::ROW_KEY_COLUMN_ID, &rowKey, 0, 0, NULL, 1);
	util::XArray<SubTimeSeriesInfo> subTimeSeriesList(
		txn.getDefaultAllocator());
	util::XArray<SubTimeSeriesInfo>::iterator itr;
	getSubTimeSeriesList(txn, sc, subTimeSeriesList, forUpdate);
	if (subTimeSeriesList.empty()) {
		return NULL;
	}
	else {
		return ALLOC_NEW(txn.getDefaultAllocator())
			SubTimeSeries(txn, subTimeSeriesList.front(), this);
	}
}

void TimeSeries::getIdList(TransactionContext &txn,
	util::XArray<uint8_t> &serializedRowList, util::XArray<RowId> &idList) {
	try {
		uint8_t *in = serializedRowList.data();
		for (size_t i = 0; i < (serializedRowList.size() / rowFixedDataSize_);
			 i++) {  
			idList.push_back(*reinterpret_cast<RowId *>(
				in +
				getColumnInfo(ColumnInfo::ROW_KEY_COLUMN_ID)
					.getColumnOffset()));
			in += rowFixedDataSize_;
		}
	}
	catch (std::exception &e) {
		handleSearchError(txn, e, GS_ERROR_DS_CON_GET_ROW_ID_LIST_FAILED);
	}
}

void TimeSeries::lockIdList(TransactionContext &txn, util::XArray<OId> &oIdList,
	util::XArray<RowId> &idList) {
	try {
		RowArray rowArray(txn, this);
		for (size_t i = 0; i < oIdList.size(); i++) {
			rowArray.load(txn, oIdList[i], this, OBJECT_FOR_UPDATE);
			RowArray::Row row(rowArray.getRow(), &rowArray);
			row.lock(txn);
			idList.push_back(row.getRowId());
		}
	}
	catch (std::exception &e) {
		handleSearchError(txn, e, GS_ERROR_DS_CON_GET_ROW_ID_LIST_FAILED);
	}
}

void TimeSeries::setDummyMvccImage(TransactionContext &txn) {
	try {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());

		util::XArray<SubTimeSeriesInfo> subTimeSeriesList(
			txn.getDefaultAllocator());
		getSubTimeSeriesList(txn, subTimeSeriesList, true);
		if (!subTimeSeriesList.empty()) {
			const size_t lastPos = subTimeSeriesList.size() - 1;
			const SubTimeSeriesInfo &subTimeSeriesInfo =
				subTimeSeriesList[lastPos];
			SubTimeSeries *subContainer = ALLOC_NEW(txn.getDefaultAllocator())
				SubTimeSeries(txn, subTimeSeriesInfo, this);
			subContainer->setDummyMvccImage(txn);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void TimeSeries::getContainerOptionInfo(
	TransactionContext &txn, util::XArray<uint8_t> &containerSchema) {
	bool isExistTimeSeriesOption = true;
	containerSchema.push_back(
		reinterpret_cast<uint8_t *>(&isExistTimeSeriesOption), sizeof(bool));

	ExpirationInfo &expirationInfo = getExpirationInfo();
	containerSchema.push_back(
		reinterpret_cast<uint8_t *>(&(expirationInfo.elapsedTime_)),
		sizeof(int32_t));
	int8_t tmp = static_cast<int8_t>(expirationInfo.timeUnit_);
	containerSchema.push_back(
		reinterpret_cast<uint8_t *>(&tmp), sizeof(int8_t));
	int32_t tmpDivisionCount = expirationInfo.dividedNum_;
	containerSchema.push_back(
		reinterpret_cast<uint8_t *>(&tmpDivisionCount), sizeof(int32_t));

	DurationInfo defaultDurationInfo;
	int32_t tmpDuration =
		static_cast<int32_t>(defaultDurationInfo.timeDuration_);
	containerSchema.push_back(
		reinterpret_cast<uint8_t *>(&tmpDuration), sizeof(int32_t));
	int8_t tmpTimeUnit = static_cast<int8_t>(defaultDurationInfo.timeUnit_);
	containerSchema.push_back(
		reinterpret_cast<uint8_t *>(&tmpTimeUnit), sizeof(int8_t));
	int8_t tmpCompressionType = 0;
	containerSchema.push_back(
		reinterpret_cast<uint8_t *>(&tmpCompressionType), sizeof(int8_t));
	uint32_t compressionInfoNum = 0;
	containerSchema.push_back(
		reinterpret_cast<uint8_t *>(&compressionInfoNum), sizeof(uint32_t));
}

void TimeSeries::checkContainerOption(MessageSchema *orgMessageSchema,
	util::XArray<uint32_t> &copyColumnMap, bool &) {
	MessageTimeSeriesSchema *messageSchema =
		reinterpret_cast<MessageTimeSeriesSchema *>(orgMessageSchema);

	if (!messageSchema->isExistTimeSeriesOption()) {

		messageSchema->setExpirationInfo(getExpirationInfo());
	}
	else {
		ExpirationInfo &expirationInfo = getExpirationInfo();
		if (expirationInfo.elapsedTime_ !=
			messageSchema->getExpirationInfo().elapsedTime_) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_TIM_INVALID_SCHEMA_OPTION,
				"elapsed time of expiration is different. old = "
					<< expirationInfo.elapsedTime_ << ", new = "
					<< messageSchema->getExpirationInfo().elapsedTime_);
		}
		if (expirationInfo.timeUnit_ !=
			messageSchema->getExpirationInfo().timeUnit_) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_TIM_INVALID_SCHEMA_OPTION,
				"time unit of expiration is different. old = "
					<< (int32_t)expirationInfo.timeUnit_ << ", new = "
					<< (int32_t)messageSchema->getExpirationInfo().timeUnit_);
		}

		if (expirationInfo.dividedNum_ !=
			messageSchema->getExpirationInfo().dividedNum_) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_TIM_INVALID_SCHEMA_OPTION,
				"divisionCount of expiration is different. old = "
					<< expirationInfo.dividedNum_ << ", new = "
					<< messageSchema->getExpirationInfo().dividedNum_);
		}

	}
	return;
}

void TimeSeries::getSubTimeSeriesList(TransactionContext &txn,
	util::XArray<SubTimeSeriesInfo> &subTimeSeriesList, bool forUpdate) {
	BtreeMap::SearchContext sc(
		UNDEF_COLUMNID, NULL, 0, true, NULL, 0, true, 0, NULL, MAX_RESULT_SIZE);
	getSubTimeSeriesList(txn, sc, subTimeSeriesList, forUpdate);
}

void TimeSeries::getSubTimeSeriesList(TransactionContext &txn,
	BtreeMap::SearchContext &sc,
	util::XArray<SubTimeSeriesInfo> &subTimeSeriesList, bool forUpdate) {
	const Operator *op1, *op2;
	bool isValid = getKeyCondition(txn, sc, op1, op2);
	if (!isValid) {
		return;
	}

	Timestamp currentExpiredTime = getCurrentExpiredTime(txn);
	if (forUpdate) {
		expireSubTimeSeries(txn, currentExpiredTime);
	}

	Timestamp scBeginTime = -1;
	Timestamp scEndTime = MAX_TIMESTAMP;
	if (sc.columnId_ == ColumnInfo::ROW_KEY_COLUMN_ID) {
		if (sc.startKey_ != NULL) {
			scBeginTime = *reinterpret_cast<const Timestamp *>(sc.startKey_);
		}
		if (sc.endKey_ != NULL) {
			scEndTime = *reinterpret_cast<const Timestamp *>(sc.endKey_);
		}
	}
	else {
		for (uint32_t c = 0; c < sc.conditionNum_; c++) {
			if (sc.conditionList_[c].columnId_ ==
				ColumnInfo::ROW_KEY_COLUMN_ID) {
				if (sc.conditionList_[c].operator_ == &geTimestampTimestamp ||
					sc.conditionList_[c].operator_ == &gtTimestampTimestamp) {
					Timestamp condBeginTime =
						*reinterpret_cast<const Timestamp *>(
							sc.conditionList_[c].value_);
					if (scBeginTime < condBeginTime) {
						scBeginTime = condBeginTime;
					}
				}
				else if (sc.conditionList_[c].operator_ ==
							 &leTimestampTimestamp ||
						 sc.conditionList_[c].operator_ ==
							 &ltTimestampTimestamp) {
					Timestamp condEndTime =
						*reinterpret_cast<const Timestamp *>(
							sc.conditionList_[c].value_);
					if (scEndTime > condEndTime) {
						scEndTime = condEndTime;
					}
				}
			}
		}
	}
	if (scBeginTime < currentExpiredTime) {
		scBeginTime = currentExpiredTime;
	}


	int64_t minPos = 0;
	findStartSubTimeSeriesPos(
		txn, scBeginTime, 0, subTimeSeriesListHead_->getNum() - 1, minPos);
	if (minPos < 0) {
		minPos = 0;
	}
	for (int64_t i = minPos;
		 i < static_cast<int64_t>(subTimeSeriesListHead_->getNum()); i++) {
		const SubTimeSeriesImage &subTimeSeriesImage =
			*(subTimeSeriesListHead_->get(txn, i));
		Timestamp containerBeginTime = subTimeSeriesImage.startTime_;
		Timestamp containerEndTime =
			containerBeginTime + getDivideDuration() - 1;

		bool isMatch = false;
		if (containerBeginTime <= scBeginTime &&
			scBeginTime <= containerEndTime) {
			subTimeSeriesList.push_back(
				SubTimeSeriesInfo(subTimeSeriesImage, i));
			isMatch = true;
		}
		else if (scBeginTime <= containerBeginTime &&
				 containerBeginTime <= scEndTime) {
			subTimeSeriesList.push_back(
				SubTimeSeriesInfo(subTimeSeriesImage, i));
			isMatch = true;
		}
		if (i != minPos && !isMatch) {
			break;
		}
	}
}

void TimeSeries::getRuntimeSubTimeSeriesList(TransactionContext &txn,
	util::XArray<SubTimeSeriesInfo> &subTimeSeriesList, bool forUpdate) {
	Timestamp expiredTime = getCurrentExpiredTime(txn);
	if (forUpdate) {
		expireSubTimeSeries(txn, expiredTime);
	}

	int64_t divideDuration = getDivideDuration();
	for (uint64_t i = 0; i < subTimeSeriesListHead_->getNum(); i++) {
		const SubTimeSeriesImage &subTimeSeriesImage =
			*(subTimeSeriesListHead_->get(txn, i));
		if (subTimeSeriesImage.isRuntime() &&
			(subTimeSeriesImage.startTime_ + divideDuration > expiredTime)) {
			subTimeSeriesList.push_back(
				SubTimeSeriesInfo(subTimeSeriesImage, i));
		}
	}
}

void TimeSeries::updateSubTimeSeriesImage(
	TransactionContext &txn, const SubTimeSeriesInfo &subTimeSeriesInfo) {
	subTimeSeriesListHead_->update(
		txn, subTimeSeriesInfo.pos_, &subTimeSeriesInfo.image_);
}

void TimeSeries::searchRowArrayList(TransactionContext &txn,
	BtreeMap::SearchContext &sc, util::XArray<OId> &normalOIdList,
	util::XArray<OId> &mvccOIdList) {
	util::XArray<SubTimeSeriesInfo> subTimeSeriesList(
		txn.getDefaultAllocator());
	util::XArray<SubTimeSeriesInfo>::iterator itr;
	getSubTimeSeriesList(txn, sc, subTimeSeriesList, false);
	for (itr = subTimeSeriesList.begin(); itr != subTimeSeriesList.end();
		 itr++) {
		SubTimeSeries *subContainer =
			ALLOC_NEW(txn.getDefaultAllocator()) SubTimeSeries(txn, *itr, this);
		StackAllocAutoPtr<BtreeMap> rowIdMap(
			txn.getDefaultAllocator(), subContainer->getRowIdMap(txn));
		rowIdMap.get()->search(txn, sc, normalOIdList);
		{
			util::XArray<std::pair<TransactionId, MvccRowImage> >
				mvccKeyValueList(txn.getDefaultAllocator());
			util::XArray<std::pair<TransactionId, MvccRowImage> >::iterator
				mvccItr;
			StackAllocAutoPtr<BtreeMap> mvccMap(
				txn.getDefaultAllocator(), subContainer->getMvccMap(txn));
			mvccMap.get()->getAll<TransactionId, MvccRowImage>(
				txn, MAX_RESULT_SIZE, mvccKeyValueList);
			mvccOIdList.reserve(mvccKeyValueList.size());  
			{
				util::StackAllocator::Scope scope(
					txn.getDefaultAllocator());  

				ColumnType targetType = COLUMN_TYPE_ROWID;
				util::XArray<SortKey> mvccSortKeyList(
					txn.getDefaultAllocator());
				RowArray mvccRowArray(txn, this);
				for (mvccItr = mvccKeyValueList.begin();
					 mvccItr != mvccKeyValueList.end(); mvccItr++) {
					if (mvccItr->second.type_ == MVCC_SELECT) {
						continue;
					}
					if (mvccItr->first != txn.getId() &&
						mvccItr->second.type_ != MVCC_CREATE) {
						mvccRowArray.load(txn, mvccItr->second.snapshotRowOId_,
							this, OBJECT_READ_ONLY);
						if (mvccRowArray.begin()) {
							RowArray::Row row(
								mvccRowArray.getRow(), &mvccRowArray);

							uint8_t *objectRowField;
							row.getRowIdField(objectRowField);

							SortKey sortKey;
							sortKey.set(txn, targetType, objectRowField,
								mvccItr->second.snapshotRowOId_);
							mvccSortKeyList.push_back(sortKey);
						}
					}
				}
				const Operator *sortOp =
					&ltTable[COLUMN_TYPE_TIMESTAMP][COLUMN_TYPE_TIMESTAMP];
				std::sort(mvccSortKeyList.begin(), mvccSortKeyList.end(),
					SortPred(txn, sortOp, COLUMN_TYPE_TIMESTAMP));
				util::XArray<SortKey>::iterator mvccKeyItr;
				for (mvccKeyItr = mvccSortKeyList.begin();
					 mvccKeyItr != mvccSortKeyList.end(); mvccKeyItr++) {
					mvccOIdList.push_back(mvccKeyItr->getOId());
				}
			}
		}
	}
}

void TimeSeries::insertRowIdMap(
	TransactionContext &, BtreeMap *, const void *, OId) {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
}

void TimeSeries::insertMvccMap(
	TransactionContext &, BtreeMap *, TransactionId, MvccRowImage &) {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
}

void TimeSeries::insertValueMap(
	TransactionContext &, BaseIndex *, const void *, OId, ColumnId, MapType) {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
}

void TimeSeries::updateRowIdMap(
	TransactionContext &, BtreeMap *, const void *, OId, OId) {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
}

void TimeSeries::updateMvccMap(TransactionContext &, BtreeMap *, TransactionId,
	MvccRowImage &, MvccRowImage &) {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
}

void TimeSeries::updateValueMap(TransactionContext &, BaseIndex *, const void *,
	OId, OId, ColumnId, MapType) {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
}

void TimeSeries::removeRowIdMap(
	TransactionContext &, BtreeMap *, const void *, OId) {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
}

void TimeSeries::removeMvccMap(
	TransactionContext &, BtreeMap *, TransactionId, MvccRowImage &) {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
}

void TimeSeries::removeValueMap(
	TransactionContext &, BaseIndex *, const void *, OId, ColumnId, MapType) {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
}

void TimeSeries::updateIndexData(TransactionContext &, IndexData) {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
}

uint16_t TimeSeries::calcRowArrayNum(uint16_t baseRowNum) {
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
	return static_cast<uint16_t>(
		(estimateAllocateSize - RowArray::getHeaderSize()) / rowImageSize_);
}

/*!
	@brief Postprocess of aggregation operation
*/
void aggregationPostProcess(AggregationCursor &cursor, Value &value) {
	switch (cursor.aggType_) {
	case AGG_COUNT:
		value.set(cursor.count_);
		break;
	case AGG_AVG: {
		double doubleVal = cursor.value1_.getDouble();
		doubleVal /= cursor.count_;
		value.set(doubleVal);
	} break;
	case AGG_VARIANCE: {
		double doubleVal2 = cursor.value2_.getDouble();
		doubleVal2 /= cursor.count_;
		double doubleVal1 = cursor.value1_.getDouble();
		doubleVal1 /= cursor.count_;
		doubleVal1 *= doubleVal1;
		value.set(doubleVal2 - doubleVal1);
	} break;
	case AGG_STDDEV: {
		double doubleVal2 = cursor.value2_.getDouble();
		doubleVal2 /= cursor.count_;
		double doubleVal1 = cursor.value1_.getDouble();
		doubleVal1 /= cursor.count_;
		doubleVal1 *= doubleVal1;
		value.set(sqrt(doubleVal2 - doubleVal1));
	} break;
	default:
		value = cursor.value1_;
	}
}


/*!
	@brief Validates Rows and Indexes
*/
bool TimeSeries::validate(TransactionContext &txn, std::string &errorMessage) {
	bool isValid = true;
	RowId preRowId = -1;
	util::NormalOStringStream strstrm;
	bool isRowRateCheck =
		false;  
	uint64_t totalCountRowNum = 0;

	util::XArray<IndexData> parentIndexList(txn.getDefaultAllocator());
	getIndexList(txn, parentIndexList);

	util::XArray<SubTimeSeriesInfo> subTimeSeriesList(
		txn.getDefaultAllocator());
	util::XArray<SubTimeSeriesInfo>::iterator itr;
	getSubTimeSeriesList(txn, subTimeSeriesList, false);
	for (itr = subTimeSeriesList.begin(); itr != subTimeSeriesList.end();
		 itr++) {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		SubTimeSeries *subContainer =
			ALLOC_NEW(txn.getDefaultAllocator()) SubTimeSeries(txn, *itr, this);
		std::string subErrorMessage;
		uint64_t countRowNum = 0;
		isValid = subContainer->validateImpl<TimeSeries>(
			txn, subErrorMessage, preRowId, countRowNum, isRowRateCheck);
		if (!isValid) {
			isValid = false;
			strstrm << subErrorMessage;
			break;
		}
		totalCountRowNum += countRowNum;

		util::XArray<IndexData> childIndexList(txn.getDefaultAllocator());
		subContainer->getIndexList(txn, childIndexList);
		if (parentIndexList.size() != childIndexList.size()) {
			isValid = false;
			strstrm << "inValid IndexSize: parentIndexListSize("
					<< parentIndexList.size() << ") != childeIndexListSize("
					<< childIndexList.size() << ")" << std::endl;
			break;
		}
		for (size_t i = 0; i < parentIndexList.size(); i++) {
			if (parentIndexList[i].columnId_ != childIndexList[i].columnId_) {
				isValid = false;
				strstrm << "inValid columnId: parentIndex ColumnId("
						<< parentIndexList[i].columnId_
						<< ") != childeIndex ColumnId("
						<< childIndexList[i].columnId_ << ")" << std::endl;
				break;
			}
			if (parentIndexList[i].mapType_ != childIndexList[i].mapType_) {
				isValid = false;
				strstrm << "inValid columnId: parentIndex MapType("
						<< parentIndexList[i].mapType_
						<< ") != childeIndex MapType("
						<< childIndexList[i].mapType_ << ")" << std::endl;
				break;
			}
		}
		if (!isValid) {
			break;
		}
	}
	{
		if (getExpirationInfo().duration_ == INT64_MAX) {
			if (totalCountRowNum != getRowNum()) {
				isValid = false;
				strstrm << "inValid row num: container rowNum(" << getRowNum()
						<< ") != totalRowNum(" << totalCountRowNum << ")"
						<< std::endl;
			}
		}
	}

	errorMessage = strstrm.str();
	return isValid;
}

std::string TimeSeries::dump(TransactionContext &txn) {
	util::NormalOStringStream strstrm;
	util::XArray<SubTimeSeriesInfo> subTimeSeriesList(
		txn.getDefaultAllocator());
	util::XArray<SubTimeSeriesInfo>::iterator itr;
	getSubTimeSeriesList(txn, subTimeSeriesList, false);
	for (itr = subTimeSeriesList.begin(); itr != subTimeSeriesList.end();
		 itr++) {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		SubTimeSeries *subContainer =
			ALLOC_NEW(txn.getDefaultAllocator()) SubTimeSeries(txn, *itr, this);
		strstrm << "===SubTimeSeries Dump===" << std::endl;
		strstrm << subContainer->dump(txn);
	}
	return strstrm.str();
}

