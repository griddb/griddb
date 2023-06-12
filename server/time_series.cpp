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
#include "data_store_v4.h"
#include "data_store_common.h"
#include "gs_error.h"
#include "message_schema.h"
#include "value_processor.h"

const bool TimeSeries::INDEX_MAP_TABLE[][MAP_TYPE_NUM] = {
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
	{false, false, false},   
	{true, false, false},  
	{true, false, false}  
};


/*!
	@brief Allocate TimeSeries Object
*/
void TimeSeries::initialize(TransactionContext &txn) {
	UNUSED_VARIABLE(txn);
	baseContainerImage_ =
		BaseObject::allocate<TimeSeriesImage>(sizeof(TimeSeriesImage),
			getBaseOId(), OBJECT_TYPE_TIME_SERIES);
	memset(baseContainerImage_, 0, sizeof(TimeSeriesImage));
	baseContainerImage_->containerType_ =
		static_cast<int8_t>(TIME_SERIES_CONTAINER);
	baseContainerImage_->status_ = 0;  
	baseContainerImage_->normalRowArrayNum_ = 0;
	baseContainerImage_->containerId_ = UNDEF_CONTAINERID;
	baseContainerImage_->containerNameOId_ = UNDEF_OID;
	baseContainerImage_->rowIdMapOId_ = UNDEF_OID;
	baseContainerImage_->mvccMapOId_ = UNDEF_OID;
	baseContainerImage_->columnSchemaOId_ = UNDEF_OID;
	baseContainerImage_->indexSchemaOId_ = UNDEF_OID;
	baseContainerImage_->triggerListOId_ = UNDEF_OID;
	baseContainerImage_->baseGroupId_ = UNDEF_DS_GROUPID;
	baseContainerImage_->rowNum_ = 0;
	baseContainerImage_->versionId_ = 0;
	baseContainerImage_->tablePartitioningVersionId_ = UNDEF_TABLE_PARTITIONING_VERSIONID;
	baseContainerImage_->startTime_ = 0;

	reinterpret_cast<TimeSeriesImage *>(baseContainerImage_)->reserved_ = UNDEF_OID;
	reinterpret_cast<TimeSeriesImage *>(baseContainerImage_)->padding1_ = 0;
	reinterpret_cast<TimeSeriesImage *>(baseContainerImage_)->padding2_ = 0;

	rowImageSize_ = 0;
}

/*!
	@brief Set TimeSeries Schema
*/
void TimeSeries::set(TransactionContext & txn, const FullContainerKey & containerKey,
	ContainerId containerId, OId columnSchemaOId,
	MessageSchema * containerSchema, DSGroupId groupId) {
	baseContainerImage_->containerId_ = containerId;
	baseContainerImage_->baseGroupId_ = groupId;
	setTablePartitioningVersionId(containerSchema->getTablePartitioningVersionId());
	setContainerExpirationStartTime(containerSchema->getContainerExpirationStartTime());

	FullContainerKeyCursor keyCursor(*getObjectManager(), getMetaAllocateStrategy());
	keyCursor.initialize(txn, containerKey);
	baseContainerImage_->containerNameOId_ = keyCursor.getBaseOId();

	baseContainerImage_->columnSchemaOId_ = columnSchemaOId;
	commonContainerSchema_ =
		ALLOC_NEW(txn.getDefaultAllocator()) ShareValueList(
			*getObjectManager(), getMetaAllocateStrategy(), baseContainerImage_->columnSchemaOId_);
	columnSchema_ =
		commonContainerSchema_->get<ColumnSchema>(META_TYPE_COLUMN_SCHEMA);

	indexSchema_ = ALLOC_NEW(txn.getDefaultAllocator())
		IndexSchema(txn, *getObjectManager(), getMetaAllocateStrategy());
	bool onMemory = false;
	indexSchema_->initialize(txn, IndexSchema::INITIALIZE_RESERVE_NUM, 0, getColumnNum(), onMemory);
	baseContainerImage_->indexSchemaOId_ = indexSchema_->getBaseOId();

	rowFixedDataSize_ = calcRowFixedDataSize();
	rowImageSize_ = calcRowImageSize(rowFixedDataSize_);

	baseContainerImage_->normalRowArrayNum_ = calcRowArrayNum(
		txn, getDataStore()->getConfig().isRowArraySizeControlMode(),
		ROW_ARRAY_MAX_SIZE);

	resetRowAllocateStrategy(getObjectManager(), rowAllocateStrategy_);
	resetMapAllocateStrategy(getObjectManager(), mapAllocateStrategy_);

	if (isExpired(txn)) {
		dataStore_->setLastExpiredTime(txn.getStatementStartTime().getUnixTime());
	} else {

	util::StackAllocator &alloc = txn.getDefaultAllocator();
	util::Vector<ColumnId> columnIds(alloc);
	columnIds.push_back(getRowIdColumnId());
	rowIdFuncInfo_ = ALLOC_NEW(alloc) TreeFuncInfo(alloc);
	rowIdFuncInfo_->initialize(columnIds, NULL);

	columnIds[0] = UNDEF_COLUMNID;
	mvccFuncInfo_ = ALLOC_NEW(alloc) TreeFuncInfo(alloc);
	mvccFuncInfo_->initialize(columnIds, NULL);

	BtreeMap map(txn, *getObjectManager(), getMapAllocateStrategy(), this, rowIdFuncInfo_);
	map.initialize(
		txn, COLUMN_TYPE_TIMESTAMP, true, BtreeMap::TYPE_UNIQUE_RANGE_KEY);
	baseContainerImage_->rowIdMapOId_ = map.getBaseOId();

	BtreeMap mvccMap(txn, *getObjectManager(), getMapAllocateStrategy(), this, mvccFuncInfo_);
	mvccMap.initialize<TransactionId, MvccRowImage>(
		txn, COLUMN_TYPE_OID, false, BtreeMap::TYPE_SINGLE_KEY);
	baseContainerImage_->mvccMapOId_ = mvccMap.getBaseOId();
	}

	setNullsStatsStatus();

}


/*!
	@brief Free Objects related to TimeSeries
*/
bool TimeSeries::finalize(TransactionContext& txn, bool isRemoveGroup) {
	try {
		setDirty();

		DSGroupId baseGroupId = getBaseGroupId();
		if (isRemoveGroup) {
			getObjectManager()->chunkManager()->removeGroup(baseGroupId);
			getObjectManager()->chunkManager()->removeGroup(baseGroupId + 1);
		}
		bool alreadyRemoved = !objectManager_->isActive(baseGroupId);
		if (!alreadyRemoved && !isExpired(txn)) {

			RowArray rowArray(txn, this);
			if (baseContainerImage_->rowIdMapOId_ != UNDEF_OID && !isExpired(txn)) {
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

				getDataStore()->finalizeMap(txn, getMapAllocateStrategy(), rowIdMap.get(), getContainerExpirationTime());
			}

			if (baseContainerImage_->mvccMapOId_ != UNDEF_OID && !isExpired(txn)) {
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
						switch (itr->second.type_) {
						case MVCC_CREATE:
						case MVCC_SELECT:
						case MVCC_INDEX:
						case MVCC_CONTAINER:
							break;
						case MVCC_UPDATE:
						case MVCC_DELETE:
						{
							bool isOldSchema = rowArray.load(txn, itr->second.snapshotRowOId_,
								this, OBJECT_FOR_UPDATE);
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
				getDataStore()->finalizeMap(txn, getMapAllocateStrategy(), mvccMap.get(), getContainerExpirationTime());
			}

		}

		finalizeIndex(txn, alreadyRemoved);

		commonContainerSchema_->reset();

		FullContainerKey containerKey = getContainerKey(txn);
		containerKeyCursor_.finalize();
		baseContainerImage_->containerNameOId_ = UNDEF_OID;

		BaseObject::finalize();
	}
	catch (std::exception& e) {
		handleUpdateError(txn, e, GS_ERROR_DS_DS_DROP_CONTAINER_FAILED);
	}

	return true;
}

/*!
	@brief Creates a specifed type of index on the specified Column
*/
void TimeSeries::createIndex(
		TransactionContext& txn,
		const IndexInfo& indexInfo, IndexCursor& indexCursor,
		bool isIndexNameCaseSensitive, CreateDropIndexMode mode,
		bool *skippedByMode) {
	if (skippedByMode != NULL) {
		*skippedByMode = false;
	}
	try {
		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not create index. container's status is invalid.");
		}
		if (isExpired(txn)) {
			indexCursor.setRowId(MAX_ROWID);
			dataStore_->setLastExpiredTime(txn.getStatementStartTime().getUnixTime());
			return;
		}

		bool isExecute = checkIndexConstraint(txn, true, mode, indexInfo, isIndexNameCaseSensitive);
		if (!isExecute) {
			indexCursor.setRowId(MAX_ROWID);
			if (skippedByMode != NULL) {
				*skippedByMode = true;
			}
			return;
		}

		IndexInfo realIndexInfo = indexInfo;
		if ((realIndexInfo.columnIds_.size() < 1 ||
			realIndexInfo.columnIds_.size() > MAX_COMPOSITE_COLUMN_NUM) || 
			realIndexInfo.anyTypeMatches_ != 0 || realIndexInfo.anyNameMatches_ != 0) {
			GS_THROW_USER_ERROR(
				GS_ERROR_CM_NOT_SUPPORTED, 
				"Invalid parameter, number of columnName is invalid or anyType is specified or any index name is specified, "
					<< " number of columnName = " << realIndexInfo.columnIds_.size()
					<< ", anyTypeMatches = " << (uint32_t)realIndexInfo.anyTypeMatches_
					<< ", anyNameMatches = " << (uint32_t)realIndexInfo.anyNameMatches_);
		}
		EmptyAllowedKey::validate(
			KeyConstraint::getUserKeyConstraint(
				getDataStore()->getConfig().getLimitContainerNameSize()),
			realIndexInfo.indexName_.c_str(),
			static_cast<uint32_t>(realIndexInfo.indexName_.size()),
			"indexName");

		util::Set<ColumnId> columnIdSet(txn.getDefaultAllocator());
		for (size_t i = 0; i < realIndexInfo.columnIds_.size(); i++) {
			ColumnId inputColumnId = realIndexInfo.columnIds_[i];
			if (inputColumnId >= getColumnNum()) {
				GS_THROW_USER_ERROR(GS_ERROR_DS_COLUMN_ID_INVALID, 
					"invalid columnId : " << inputColumnId);
			}
			if (columnIdSet.find(inputColumnId) != columnIdSet.end()) {
				GS_THROW_USER_ERROR(GS_ERROR_CM_NOT_SUPPORTED, 
					"Invalid parameter, same column can not be specified more than one, "
					   << ", first columnNumber = " << inputColumnId);
			} else {
				columnIdSet.insert(inputColumnId);
			}

			ColumnInfo& columnInfo = getColumnInfo(inputColumnId);
			if (realIndexInfo.mapType == MAP_TYPE_DEFAULT) {
				IndexSchema::findDefaultIndexType(
						columnInfo.getColumnType(), realIndexInfo.mapType);
			}
		}
		MapType inputMapType = realIndexInfo.mapType;

		GS_TRACE_INFO(TIME_SERIES, GS_TRACE_DS_CON_CREATE_INDEX,
			"TimeSeries Id = " << getContainerId()
							   << ", first columnNumber = " << realIndexInfo.columnIds_[0]
							   << ", type = " << getMapTypeStr(inputMapType));

		validateIndexInfo(realIndexInfo);

		util::Vector<uint32_t>::iterator itr = 
			std::find(realIndexInfo.columnIds_.begin(), realIndexInfo.columnIds_.end(),
			ColumnInfo::ROW_KEY_COLUMN_ID);
		if (itr != realIndexInfo.columnIds_.end()) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_TIM_CREATEINDEX_ON_ROWKEY, "");
		}

		util::Vector<IndexInfo> matchList(txn.getDefaultAllocator());
		util::Vector<IndexInfo> mismatchList(txn.getDefaultAllocator());
		bool withUncommitted = true;

		indexSchema_->getIndexInfoList(txn, this, realIndexInfo, 
			withUncommitted, matchList, mismatchList, isIndexNameCaseSensitive);
		if (!mismatchList.empty()) {
			for (size_t i = 0; i < mismatchList.size(); i++) {
				IndexData indexData(txn.getDefaultAllocator());
				bool isExist = getIndexData(txn, 
					mismatchList[0].columnIds_, mismatchList[0].mapType, 
					withUncommitted, indexData);
				if (isExist && indexData.status_ != DDL_READY) {
					DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_CON_LOCK_CONFLICT,
						"createIndex(pId=" << txn.getPartitionId()
							<< ", index name = \"" << realIndexInfo.indexName_.c_str()
							<< ", type=" << getMapTypeStr(inputMapType)
							<< ", first columnNumber=" << (int32_t)realIndexInfo.columnIds_[0]
							<< ", txnId=" << txn.getId() << ")");
				} else {
					util::NormalOStringStream strstrm;
					strstrm << "The specified parameter caused inconsistency to the existing index"
							<< ", existing index name = \"" << mismatchList[0].indexName_.c_str()
							<< ", \" existing columnNumber = " << mismatchList[0].columnIds_[0]
							<< ", existing type = " << getMapTypeStr(mismatchList[0].mapType)
							<< ", input index name = \"" << realIndexInfo.indexName_.c_str()
							<< ", \" input columnNumbers = [";
					for (size_t j = 0; j < realIndexInfo.columnIds_.size(); j++) {
						if (j != 0) {
							strstrm << ",";
						}
						strstrm << realIndexInfo.columnIds_[j] << ",";
					}
					strstrm << "]";
					strstrm << ", input mapType = " << getMapTypeStr(realIndexInfo.mapType)
							<< ", input anyTypeMatches = " << (int32_t)realIndexInfo.anyTypeMatches_
							<< ", input anyNameMatches = " << (int32_t)realIndexInfo.anyNameMatches_
							<< ", input case sensitivity = " << (int32_t)isIndexNameCaseSensitive;
					GS_THROW_USER_ERROR(GS_ERROR_CM_NOT_SUPPORTED, strstrm.str().c_str());
				}
			}
		}

		if (isAlterContainer()) {
			DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_CON_LOCK_CONFLICT,
				"(pId=" << txn.getPartitionId() << ", containerId=" << getContainerId()
						<< ", txnId=" << txn.getId() << ")");
		}

		indexCursor.setColumnId(realIndexInfo.columnIds_[0]);
		indexCursor.setMapType(inputMapType);
		IndexData indexData(txn.getDefaultAllocator());
		bool isExist = getIndexData(txn, realIndexInfo.columnIds_, inputMapType, 
			withUncommitted, indexData);
		if (isExist) {
			indexCursor.setOption(indexData.optionOId_);
			if (indexData.status_ == DDL_READY) {
				indexCursor.setRowId(MAX_ROWID);
				return;
			}

			if (indexCursor.isImmediateMode()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_CM_NOT_SUPPORTED, 
					"Immediate mode of createIndex is forbidden under constructing, "
						<< " first columnNumber = " << realIndexInfo.columnIds_[0]
						<< " type = " << getMapTypeStr(inputMapType));
				return;
			}
		}

		uint32_t limitNum = getDataStore()->getConfig().getLimitIndexNum();
		if (!isExist && indexSchema_->getIndexNum() == limitNum) {
			GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED, "Num of index"
				<< " exceeds maximum num : " << limitNum);
		}
		setDirty();

		TransactionId tId = txn.getId();
		StackAllocAutoPtr<BtreeMap> mvccMap(
			txn.getDefaultAllocator(), getMvccMap(txn));
		MvccRowImage beforeImage;
		if (isExist) {
			util::XArray<MvccRowImage> mvccList(txn.getDefaultAllocator());
			util::XArray<MvccRowImage>::iterator itr;
			TermCondition cond(COLUMN_TYPE_LONG, COLUMN_TYPE_LONG, 
				DSExpression::EQ, UNDEF_COLUMNID, &tId, sizeof(tId));
			BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, MAX_RESULT_SIZE);
			mvccMap.get()->search<TransactionId, MvccRowImage, MvccRowImage>(
				txn, sc, mvccList);
			if (!mvccList.empty()) {
				for (itr = mvccList.begin(); itr != mvccList.end(); itr++) {
					if (itr->type_ == MVCC_INDEX) {
						beforeImage = *itr;
					} else {
						GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
					}
				}
			} else {
				DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_CON_LOCK_CONFLICT,
					"createIndex(pId=" << txn.getPartitionId()
						<< ", index name = \"" << realIndexInfo.indexName_.c_str()
						<< ", type=" << getMapTypeStr(inputMapType)
						<< ", first columnNumber=" << (int32_t)realIndexInfo.columnIds_[0]
						<< ", txnId=" << txn.getId() << ")");
			}
		} else {
			if (indexSchema_->createIndexInfo(txn, realIndexInfo)) {
				replaceIndexSchema(txn);
			}

			util::Vector<ColumnId> keyColumnIdList(txn.getDefaultAllocator());
			getKeyColumnIdList(keyColumnIdList);
			bool isUnique = definedRowKey() &&
				(keyColumnIdList.size() == realIndexInfo.columnIds_.size()) &&
				std::equal(keyColumnIdList.begin(), keyColumnIdList.end(), realIndexInfo.columnIds_.begin());

			util::Vector<ColumnType> columnTypes(txn.getDefaultAllocator());
			for (size_t i = 0; i < realIndexInfo.columnIds_.size(); i++) {
				ColumnId inputColumnId = realIndexInfo.columnIds_[i];
				ColumnInfo& columnInfo = getColumnInfo(inputColumnId);
				columnTypes.push_back(columnInfo.getColumnType());
			}
			indexData = indexSchema_->createIndexData(txn, realIndexInfo.columnIds_,
				inputMapType, columnTypes, this, isUnique);
			indexCursor.setOption(indexData.optionOId_);
			if (!indexCursor.isImmediateMode()) {
				beforeImage = indexCursor.getMvccImage();
				insertMvccMap(txn, mvccMap.get(), tId, beforeImage);
			} else {
				indexSchema_->commit(txn, indexCursor);
			}
		}

		indexInsertImpl<TimeSeries>(txn, indexData, indexCursor.isImmediateMode());

		indexCursor.setRowId(indexData.cursor_);
		updateIndexData(txn, indexData);

	}
	catch (std::exception& e) {
		handleUpdateError(txn, e, GS_ERROR_DS_CON_CREATE_INDEX_FAILED);
	}
}

/*!
	@brief Continues to create a specifed type of index on the specified Column
*/
void TimeSeries::continueCreateIndex(TransactionContext& txn, 
	IndexCursor& indexCursor) {
	try {
		setDirty();
		if (isExpired(txn)) {
			indexCursor.setRowId(MAX_ROWID);
			dataStore_->setLastExpiredTime(txn.getStatementStartTime().getUnixTime());
			return;
		}

		IndexData indexData(txn.getDefaultAllocator());
		bool isExist = getIndexData(txn, indexCursor, indexData);
		if (!isExist) {
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR,
				"can not continue to create index. index data does not existed.");
		}
		if (indexData.status_ == DDL_READY) {
			indexCursor.setRowId(MAX_ROWID);
			return;
		}
		indexInsertImpl<TimeSeries>(txn, indexData, indexCursor.isImmediateMode());

		indexCursor.setRowId(indexData.cursor_);
		updateIndexData(txn, indexData);
	}
	catch (std::exception& e) {
		handleUpdateError(txn, e, GS_ERROR_DS_COL_CREATE_INDEX_FAILED);
	}
}

/*!
	@brief Creates a specifed type of index on the specified Column
*/
void TimeSeries::dropIndex(
		TransactionContext& txn, IndexInfo& indexInfo,
		bool isIndexNameCaseSensitive, CreateDropIndexMode mode,
		bool *skippedByMode) {
	if (skippedByMode != NULL) {
		*skippedByMode = false;
	}
	try {
		GS_TRACE_DEBUG(TIME_SERIES, GS_TRACE_DS_CON_DROP_INDEX,
			"Container Id = " << getContainerId());

		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not drop index. container's status is invalid.");
		}
		if (isExpired(txn)) {
			dataStore_->setLastExpiredTime(txn.getStatementStartTime().getUnixTime());
			return;
		}

		bool isExecute = checkIndexConstraint(txn, false, mode, indexInfo, isIndexNameCaseSensitive);
		if (!isExecute) {
			if (skippedByMode != NULL) {
				*skippedByMode = true;
			}
			return;
		}

		util::Vector<IndexInfo> matchList(txn.getDefaultAllocator());
		util::Vector<IndexInfo> mismatchList(txn.getDefaultAllocator());
		bool withUncommitted = false;
		indexSchema_->getIndexInfoList(txn, this, indexInfo, withUncommitted,
			matchList, mismatchList, isIndexNameCaseSensitive);

		if (isAlterContainer() && !matchList.empty()) {
			DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_CON_LOCK_CONFLICT,
				"(pId=" << txn.getPartitionId() << ", containerId=" << getContainerId()
						<< ", txnId=" << txn.getId() << ")");
		}

		for (size_t i = 0; i < matchList.size(); i++) {
			indexSchema_->dropIndexData(txn, matchList[i].columnIds_,
				matchList[i].mapType, this, true);
			indexSchema_->dropIndexInfo(
				txn, matchList[i].columnIds_, matchList[i].mapType);
		}
	}
	catch (std::exception& e) {
		handleUpdateError(txn, e, GS_ERROR_DS_CON_DROP_INDEX_FAILED);
	}
}

/*!
	@brief Newly creates or updates a Row, based on the specified Row object and
   also the Row key specified as needed
*/
void TimeSeries::putRow(TransactionContext &txn, uint32_t rowSize,
	const uint8_t *rowData, RowId &rowId, bool rowIdSpecified, 
	PutStatus &status, PutRowOption putRowOption)
{
	try {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		setDirty();

		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not put. container's status is invalid.");
		}
		if (isExpired(txn)) {
			status = PutStatus::NOT_EXECUTED;
			return;
		}
		InputMessageRowStore inputMessageRowStore(
			getDataStore()->getConfig(), getColumnInfoList(),
			getColumnNum(), const_cast<uint8_t *>(rowData), rowSize, 1,
			getRowFixedDataSize(), false);
		inputMessageRowStore.next();

		if (isAlterContainer()) {
			DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_CON_LOCK_CONFLICT,
				"(pId=" << txn.getPartitionId() << ", containerId=" << getContainerId()
						<< ", txnId=" << txn.getId() << ")");
		}

		putRowInternal(txn, &inputMessageRowStore, rowId, rowIdSpecified, status,
			putRowOption);
	}
	catch (std::exception &e) {
		handleUpdateError(txn, e, GS_ERROR_DS_COL_PUT_ROW_FAILED);
	}
}

/*!
	@brief Deletes a Row corresponding to the specified Row key
*/
void TimeSeries::deleteRow(TransactionContext &txn, uint32_t rowKeySize,
	const uint8_t* rowKey, RowId& rowId, bool& existing) {
	Timestamp rowKeyTimestamp =
		*(reinterpret_cast<const Timestamp *>(rowKey));
	bool isForceLock = false;
	deleteRow(txn, rowKeyTimestamp, existing, isForceLock);
	if (existing) {
		rowId = rowKeyTimestamp;
	}
	else {
		rowId = UNDEF_ROWID;
	}
}

void TimeSeries::deleteRow(
	TransactionContext& txn, RowId rowId, bool& existing, bool isForceLock) {
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

		if (isExpired(txn)) {
			existing = false;
			return;
		}

		if (isAlterContainer() && !isForceLock) {
			DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_CON_LOCK_CONFLICT,
				"(pId=" << txn.getPartitionId() << ", containerId=" << getContainerId()
						<< ", txnId=" << txn.getId() << ")");
		}

		deleteRowInternal(txn, rowKeyTimestamp, existing, isForceLock);
		if (existing) {
			decrementRowNum();
		}
	}
	catch (std::exception& e) {
		handleUpdateError(txn, e, GS_ERROR_DS_CON_DELETE_ROW_FAILED);
	}
}

void TimeSeries::updateRow(TransactionContext &txn, uint32_t rowSize,
	const uint8_t* rowData, RowId rowId, PutStatus& status,
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

		if (isExpired(txn)) {
			status = PutStatus::NOT_EXECUTED;
			return;
		}

		if (isAlterContainer() && !isForceLock) {
			DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_CON_LOCK_CONFLICT,
				"(pId=" << txn.getPartitionId() << ", containerId=" << getContainerId()
						<< ", txnId=" << txn.getId() << ")");
		}

		InputMessageRowStore inputMessageRowStore(
			getDataStore()->getConfig(), getColumnInfoList(),
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
						DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_CON_LOCK_CONFLICT,
							"(txnId=" << txn.getId() << ", rowTxnId=" << row.getTxnId() << ")");
				}
			}
			updateRowInternal(txn, &inputMessageRowStore, rowArray, rowId);
			status = PutStatus::UPDATE;
		}
		else {
			status = PutStatus::NOT_EXECUTED;
		}
	}
	catch (std::exception& e) {
		handleUpdateError(txn, e, GS_ERROR_DS_CON_UPDATE_ROW_INVALID);
	}
}

/*!
	@brief Rolls back the result of transaction
*/
void TimeSeries::abort(TransactionContext& txn) {
	abortInternal(txn, txn.getId());
}

void TimeSeries::abortInternal(TransactionContext& txn, TransactionId tId) {
	try {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not abort. container's status is invalid.");
		}

		if (isExpired(txn)) {
			dataStore_->setLastExpiredTime(txn.getStatementStartTime().getUnixTime());
			return;
		}

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
			setDirty();
			RowArray rowArray(txn, this);
			for (itr = mvccList.begin(); itr != mvccList.end(); itr++) {
				if (isAlterContainer() && itr->type_ != MVCC_CONTAINER) {
					GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_CON_LOCK_CONFLICT, 
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
						TermCondition startCond(getRowIdColumnType(), getRowIdColumnType(), 
							DSExpression::GE, getRowIdColumnId(), &startKey, sizeof(startKey));
						TermCondition endCond(getRowIdColumnType(), getRowIdColumnType(), 
							DSExpression::LE, getRowIdColumnId(), &endKey, sizeof(endKey));
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
							bool isOldSchema = rowArray.load(
								txn, *rowItr, this, OBJECT_READ_ONLY);

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
					{
						IndexCursor indexCursor = IndexCursor(*itr);
						IndexData indexData(txn.getDefaultAllocator());
						bool isExist = getIndexData(txn, indexCursor,
							indexData);
						UNUSED_VARIABLE(isExist);
						indexSchema_->dropIndexData(txn, *(indexData.columnIds_),
							indexData.mapType_, this, true);
						indexSchema_->dropIndexInfo(
							txn, *(indexData.columnIds_), indexData.mapType_);
						removeMvccMap(txn, mvccMap.get(), tId, *itr);

						GS_TRACE_INFO(
							DATA_STORE, GS_ERROR_DS_BACKGROUND_TASK_INVALID, 
							"abort createIndex, " 
							<< "containerId = " << getContainerId()
							<< ", columnNumber =" << indexCursor.getColumnId()
							<< ", mapType=" << (int)indexCursor.getMapType());
					}
					break;
				case MVCC_CONTAINER:
					{
						ContainerCursor containerCursor(*itr);
						resetAlterContainer();
						ContainerAutoPtr containerAutoPtr(txn, dataStore_, 
							containerCursor);
						BaseContainer *container = containerAutoPtr.getBaseContainer();
						dataStore_->finalizeContainer(txn, container);
						removeMvccMap(txn, mvccMap.get(), tId, *itr);

						GS_TRACE_INFO(
							DATA_STORE, GS_ERROR_DS_BACKGROUND_TASK_INVALID, 
							"abort alter, "
							<< "containerId = " << getContainerId()
							<< ", txnId = " << txn.getId());
					}
					break;
				case MVCC_UPDATE:
				case MVCC_DELETE:
					{
						util::StackAllocator::Scope scope(txn.getDefaultAllocator());
						bool isOldSchema = rowArray.load(txn, itr->snapshotRowOId_, this,
							OBJECT_FOR_UPDATE);
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
		}
	}
	catch (std::exception& e) {
		handleUpdateError(txn, e, GS_ERROR_DS_CON_ABORT_FAILED);
	}
}

/*!
	@brief Commits the result of transaction
*/
void TimeSeries::commit(TransactionContext& txn) {
	try {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());

		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not commit. container's status is invalid.");
		}
		if (isExpired(txn)) {
			dataStore_->setLastExpiredTime(txn.getStatementStartTime().getUnixTime());
			return;
		}

		bool isFinalize = false;

		{
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
				setDirty();
				RowArray rowArray(txn, this);
				for (itr = mvccList.begin(); itr != mvccList.end(); itr++) {
					if (isAlterContainer() && itr->type_ != MVCC_CONTAINER) {
						GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_CON_LOCK_CONFLICT, 
							"commit : container already locked "
							<< ", partitionId = " << txn.getPartitionId()
							<< ", txnId = " << txn.getId()
							<< ", containerId = " << getContainerId()
							);
					}
					switch (itr->type_) {
					case MVCC_SELECT:
					case MVCC_CREATE:
						{ removeMvccMap(txn, mvccMap.get(), tId, *itr); }
						break;
					case MVCC_UPDATE:
					case MVCC_DELETE:
						{
							bool isOldSchema = rowArray.load(
								txn, itr->snapshotRowOId_, this, OBJECT_FOR_UPDATE);
							UNUSED_VARIABLE(isOldSchema);
							assert(isOldSchema || !isOldSchema);
							rowArray.finalize(txn);
							removeMvccMap(txn, mvccMap.get(), tId, *itr);
						}
						break;
					case MVCC_INDEX:
						{
							IndexCursor indexCursor = createCursor(txn, *itr);
							while (!indexCursor.isFinished()) {
								continueCreateIndex(txn, indexCursor);
							}
							indexSchema_->commit(txn, indexCursor);
							removeMvccMap(txn, mvccMap.get(), tId, *itr);
						}
						break;
					case MVCC_CONTAINER:
						{
							ContainerCursor containerCursor(*itr);
							while (!containerCursor.isFinished()) {
								continueChangeSchema(txn, containerCursor);
							}
							resetAlterContainer();
							MvccRowImage mvccImage = containerCursor.getMvccImage();
							removeMvccMap(txn, mvccMap.get(), tId, mvccImage);
							ContainerAutoPtr containerAutoPtr(txn, dataStore_,
								containerCursor);
							updateContainer(txn, containerAutoPtr.getBaseContainer());
							isFinalize = true; 
						}
						break;
					default:
						GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
						break;
					}
				}
			}
		}

		if (isFinalize) {
			dataStore_->finalizeContainer(txn, this);
		}
	}
	catch (std::exception& e) {
		handleUpdateError(txn, e, GS_ERROR_DS_CON_COMMIT_FAILED);
	}
}

/*!
	@brief Search Btree Index of RowId
*/
void TimeSeries::searchRowIdIndex(TransactionContext &txn,
	BtreeMap::SearchContext& sc, util::XArray<OId>& resultList,
	OutputOrder outputOrder) {

	util::XArray<OId> mvccList(txn.getDefaultAllocator());
	ResultSize limitBackup = sc.getLimit();
	sc.setLimit(MAX_RESULT_SIZE);
	bool isCheckOnly = sc.getResumeStatus() != BaseIndex::SearchContext::NOT_RESUME;
	const bool ignoreTxnCheck = false;
	searchMvccMap<TimeSeries, BtreeMap::SearchContext>(txn, sc, mvccList, isCheckOnly, ignoreTxnCheck);

	sc.setLimit(limitBackup);

	ContainerValue containerValue(*getObjectManager(), getRowAllocateStrategy());
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

			ColumnType targetType = COLUMN_TYPE_ROWID;
			util::XArray<SortKey> indexSortKeyList(txn.getDefaultAllocator());
			util::XArray<SortKey> mvccSortKeyList(txn.getDefaultAllocator());
			for (itr = mvccList.begin(); itr != mvccList.end(); itr++) {
				rowArray.load(txn, *itr, this, OBJECT_READ_ONLY);
				RowArray::Row row(rowArray.getRow(), &rowArray);
				RowId rowId = row.getRowId();
				SortKey sortKey;
				sortKey.set(txn, targetType, reinterpret_cast<uint8_t *>(&rowId), *itr);
				mvccSortKeyList.push_back(sortKey);
			}

			bool isNullLast = outputOrder == ORDER_ASCENDING;
			Operator sortOp;
			if (outputOrder == ORDER_ASCENDING) {
				sortOp = ComparatorTable::Lt()(targetType, targetType);
			}
			else {
				sortOp = ComparatorTable::Gt()(targetType, targetType);
			}
			std::sort(mvccSortKeyList.begin(), mvccSortKeyList.end(),
				SortPred(txn, sortOp, targetType, isNullLast));
			for (itr = resultList.begin(); itr != resultList.end(); itr++) {
				rowArray.load(txn, *itr, this, OBJECT_READ_ONLY);
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
void TimeSeries::searchRowIdIndex(TransactionContext& txn, uint64_t start,
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

	TermCondition startCond(getRowIdColumnType(), getRowIdColumnType(), 
		DSExpression::GE, getRowIdColumnId(), &minRowId, sizeof(minRowId));
	TermCondition endCond(getRowIdColumnType(), getRowIdColumnType(), 
		DSExpression::LE, getRowIdColumnId(), &maxRowId, sizeof(maxRowId));
	BtreeMap::SearchContext sc(txn.getDefaultAllocator(), startCond, endCond, MAX_RESULT_SIZE);
	util::XArray<KeyValue<RowId, OId> > keyValueList(txn.getDefaultAllocator());

	StackAllocAutoPtr<BtreeMap> rowIdMap(
		txn.getDefaultAllocator(), getRowIdMap(txn));
	rowIdMap.get()->search<RowId, OId, KeyValue<RowId, OId> >(
		txn, sc, keyValueList, ORDER_ASCENDING);

	bool isCheckOnly = false;
	util::XArray<OId> mvccList(txn.getDefaultAllocator());
	const bool ignoreTxnCheck = false;
	searchMvccMap<TimeSeries, BtreeMap::SearchContext>(txn, sc, mvccList, isCheckOnly, ignoreTxnCheck);
	util::Map<RowId, OId> mvccRowIdMap(txn.getDefaultAllocator());
	RowArray rowArray(txn, this);
	for (util::XArray<OId>::iterator mvccItr = mvccList.begin();
		 mvccItr != mvccList.end(); mvccItr++) {
		rowArray.load(txn, *mvccItr, this, OBJECT_READ_ONLY);
		RowArray::Row mvccRow(rowArray.getRow(), &rowArray);
		mvccRowIdMap.insert(
			std::make_pair(mvccRow.getRowId(), rowArray.getOId()));
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
			rowArray.load(txn, keyValueItr->value_, this, OBJECT_READ_ONLY);
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

void TimeSeries::searchRowIdIndexAsRowArray(
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

void TimeSeries::scanRowIdIndex(
		TransactionContext& txn, BtreeMap::SearchContext &sc,
		OutputOrder order, ContainerRowScanner &scanner) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();
	util::XArray<OId> oIdList(alloc);
	util::XArray<OId> mvccOIdList(alloc);
	util::XArray<OId> mergedOIdList(alloc);
	util::XArray< std::pair<RowId, OId> > rowOIdList(alloc);

	RowId startRowId;
	RowId endRowId;
	if (!getRowIdRangeCondition(
			alloc,
			sc, getContainerType(), &startRowId, &endRowId)) {
		return;
	}

	oIdList.clear();
	scanRowIdIndexPrepare(txn, sc, order, &oIdList);

	RowId lastCheckRowId = UNDEF_ROWID;
	RowId lastMergedRowId = UNDEF_ROWID;

	const size_t rowOIdListLimit =
			txn.getDefaultAllocator().base().getElementSize() /
			(sizeof(RowId) + sizeof(OId)) / 4;
	size_t restRowOIdCount = rowOIdListLimit;

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
}

void TimeSeries::scanRowArray(
		TransactionContext& txn, const OId *begin, const OId *end,
		bool onMvcc, ContainerRowScanner &scanner) {

	if (checkRunTime(txn)) {
		RowArray &rowArray = scanner.getRowArray();

		const RowId startRowId = getScanStartRowId();
		const RowId endRowId = UNDEF_ROWID;

		util::XArray<OId> oIdList(txn.getDefaultAllocator());

		for (const OId *it = begin; it != end; ++it) {
			rowArray.load(txn, *it, this, OBJECT_READ_ONLY);
			if (onMvcc) {
				scanRowArrayCheckedDirect<true>(
						txn, rowArray, ORDER_UNDEFINED,
						startRowId, endRowId, oIdList);
			}
			else {
				scanRowArrayCheckedDirect<false>(
						txn, rowArray, ORDER_UNDEFINED,
						startRowId, endRowId, oIdList);
			}

			if (!oIdList.empty() && !scanner.scanRowUnchecked(
					txn, *this, &rowArray,
					&oIdList.front(), &oIdList.back() + 1)) {
				break;
			}
			oIdList.clear();
		}
	}
	else {
		scanner.scanRowArrayUnchecked(txn, *this, NULL, begin, end);
	}
}

void TimeSeries::scanRow(
		TransactionContext& txn, const OId *begin, const OId *end,
		bool onMvcc, ContainerRowScanner &scanner) {

	if (checkRunTime(txn)) {
		RowArray &rowArray = scanner.getRowArray();

		const RowId startRowId = getScanStartRowId();
		const RowId endRowId = UNDEF_ROWID;

		util::XArray<OId> oIdList(txn.getDefaultAllocator());

		for (const OId *it = begin; it != end; ++it) {
			rowArray.load(txn, *it, this, OBJECT_READ_ONLY);
			if (onMvcc) {
				scanRowCheckedDirect<true>(
						txn, rowArray, startRowId, endRowId, oIdList);
			}
			else {
				scanRowCheckedDirect<false>(
						txn, rowArray, startRowId, endRowId, oIdList);
			}

			if (!oIdList.empty() && !scanner.scanRowUnchecked(
					txn, *this, &rowArray,
					&oIdList.front(), &oIdList.back() + 1)) {
				break;
			}
			oIdList.clear();
		}
	}
	else {
		scanner.scanRowUnchecked(txn, *this, NULL, begin, end);
	}
}

/*!
	@brief Get max RowId
*/
RowId TimeSeries::getMaxRowId(TransactionContext &txn) {
	OId oId = UNDEF_OID;
	searchTimeOperator(txn, MAX_TIMESTAMP, TIME_PREV, oId);
	if (UNDEF_OID == oId) {
		return UNDEF_ROWID;
	}
	else {
		RowArray rowArray(txn, this);
		rowArray.load(txn, oId, this, OBJECT_READ_ONLY);
		RowArray::Row row(rowArray.getRow(), &rowArray);
		return row.getRowId();
	}
}

void TimeSeries::searchColumnIdIndex(TransactionContext &txn,
	BtreeMap::SearchContext &sc, BtreeMap::SearchContext &orgSc,
	util::XArray<OId> &resultList, OutputOrder order, bool neverOrdering)
{
	assert(!(order != ORDER_UNDEFINED && neverOrdering));
	util::XArray<OId> mergeList(txn.getDefaultAllocator());
	if (order != ORDER_UNDEFINED) {
		reinterpret_cast<BaseContainer *>(this)
			->searchColumnIdIndex(txn, sc, resultList, order);
	}
	else {
		ColumnId sortColumnId = ColumnInfo::ROW_KEY_COLUMN_ID;
		ColumnInfo &sortColumnInfo = getColumnInfo(sortColumnId);
		ResultSize limitBackup = sc.getLimit();
		sc.setLimit(MAX_RESULT_SIZE);
		reinterpret_cast<BaseContainer *>(this)
			->searchColumnIdIndex(
				txn, sc, resultList, order);
		sc.setLimit(limitBackup);
		util::XArray<OId> dummyList(txn.getDefaultAllocator());
		mergeRowList<TimeSeries>(txn, sortColumnInfo, resultList, false,
			dummyList, false, mergeList, ORDER_ASCENDING);
		resultList.swap(mergeList);
	}

	if (resultList.size() > sc.getLimit()) {
		resultList.resize(sc.getLimit());
	}
}

void TimeSeries::searchColumnIdIndex(TransactionContext &txn,
	BtreeMap::SearchContext &sc, BtreeMap::SearchContext &orgSc,
	util::XArray<OId> &normalRowList, util::XArray<OId> &mvccRowList)
{
	{
		ResultSize limitBackup = sc.getLimit();
		sc.setLimit(MAX_RESULT_SIZE);
		reinterpret_cast<BaseContainer *>(this)
			->searchColumnIdIndex(
				txn, sc, normalRowList, mvccRowList);
		sc.setLimit(limitBackup);
	}
	if (normalRowList.size() + mvccRowList.size() > sc.getLimit()) {
		if (mvccRowList.size() > sc.getLimit()) {
			mvccRowList.resize(sc.getLimit());
			normalRowList.clear();
		} else {
			normalRowList.resize(sc.getLimit() - mvccRowList.size());
		}
	}
}

/*!
	@brief Lock Rows
*/
void TimeSeries::lockRowList(
	TransactionContext& txn, util::XArray<RowId>& rowIdList) {
	try {
		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not lock. container's status is invalid.");
		}

		if (isExpired(txn)) {
			dataStore_->setLastExpiredTime(txn.getStatementStartTime().getUnixTime());
			return;
		}

		if (isAlterContainer()) {
			DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_CON_LOCK_CONFLICT,
				"(pId=" << txn.getPartitionId() << ", containerId=" << getContainerId()
						<< ", txnId=" << txn.getId() << ")");
		}

		setDummyMvccImage(txn);
		RowArray rowArray(txn, this);
		for (size_t i = 0; i < rowIdList.size(); i++) {
			util::StackAllocator::Scope scope(txn.getDefaultAllocator());
			TermCondition cond(getRowIdColumnType(), getRowIdColumnType(),
				DSExpression::EQ, getRowIdColumnId(), &rowIdList[i],
				sizeof(rowIdList[i]));
			BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, 1);
			util::XArray<OId> oIdList(txn.getDefaultAllocator());
			searchRowIdIndex(txn, sc, oIdList, ORDER_UNDEFINED);
			if (!oIdList.empty()) {
				bool isOldSchema = rowArray.load(txn, oIdList[0], this, OBJECT_FOR_UPDATE);
				UNUSED_VARIABLE(isOldSchema);
				assert(isOldSchema || !isOldSchema);
				RowArray::Row row(rowArray.getRow(), &rowArray);
				row.lock(txn);
			}
			else {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_COL_GET_LOCK_ID_INVALID,
					"rowId" << rowIdList[i] << "is not found");
			}
		}
	}
	catch (std::exception& e) {
		handleSearchError(txn, e, GS_ERROR_DS_CON_GET_LOCK_ID_INVALID);
	}
}

const BaseContainer::IndexMapTable& TimeSeries::getIndexMapTable() {
	return INDEX_MAP_TABLE;
}


void TimeSeries::putRowInternal(TransactionContext& txn,
	InputMessageRowStore* inputMessageRowStore, RowId& rowId,
	bool rowIdSpecified,
	PutStatus& status, PutRowOption putRowOption) {
	util::StackAllocator::Scope scope(txn.getDefaultAllocator());
	PutMode mode = UNDEFINED_STATUS;
	Timestamp rowKey = inputMessageRowStore->getField<COLUMN_TYPE_TIMESTAMP>(
		ColumnInfo::ROW_KEY_COLUMN_ID);
	RowArray rowArray(txn, this);

	Timestamp tailRowKey = UNDEF_TIMESTAMP;
	OId tailOId = UNDEF_OID;
	{
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
					GS_ERROR_DS_CON_LOCK_CONFLICT,
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
							GS_ERROR_DS_CON_LOCK_CONFLICT,
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

	rowId = rowKey;
	switch (mode) {
	case APPEND:
		if (putRowOption == PUT_UPDATE_ONLY) {
			GS_THROW_USER_ERROR(GS_ERROR_CON_PUT_ROW_OPTION_INVALID,
				"update row, but row not exists");
		}
		appendRowInternal(txn, inputMessageRowStore, rowArray, rowId);
		status = PutStatus::CREATE;
		break;
	case INSERT:
		if (putRowOption == PUT_UPDATE_ONLY) {
			GS_THROW_USER_ERROR(GS_ERROR_CON_PUT_ROW_OPTION_INVALID,
				"update row, but row not exists");
		}
		insertRowInternal(txn, inputMessageRowStore, rowArray, rowId);
		status = PutStatus::CREATE;
		break;
	case UPDATE:
		if (putRowOption == PUT_INSERT_ONLY) {
			GS_THROW_USER_ERROR(GS_ERROR_CON_PUT_ROW_OPTION_INVALID,
				"insert row, but row exists");
		}
		updateRowInternal(txn, inputMessageRowStore, rowArray, rowId);
		status = PutStatus::UPDATE;
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TIM_PUT_ROW_ERROR, "");
	}
	if (mode == APPEND || mode == INSERT) {
		incrementRowNum();
	}

}

void TimeSeries::appendRowInternal(TransactionContext& txn,
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
		}
	}
}

void TimeSeries::updateRowInternal(TransactionContext& txn,
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

void TimeSeries::deleteRowInternal(TransactionContext& txn, Timestamp rowKey,
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
				DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_CON_LOCK_CONFLICT,
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

void TimeSeries::insertRowInternal(TransactionContext& txn,
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
				if (!txn.isAutoCommit()) {  
					setCreateRowId(txn, insertRowId);
				}

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

void TimeSeries::shift(
	TransactionContext& txn, RowArray& rowArray, bool isForce) {
	util::XArray<std::pair<OId, OId> > moveList(txn.getDefaultAllocator());
	util::XArray<std::pair<OId, OId> >::iterator itr;
	rowArray.shift(txn, isForce, moveList);
	updateValueMaps(txn, moveList);
}

void TimeSeries::split(TransactionContext& txn, RowArray& rowArray,
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
	}
}

void TimeSeries::merge(
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

void TimeSeries::undoCreateRow(TransactionContext& txn, RowArray& rowArray) {
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

void TimeSeries::undoUpdateRow(
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

void TimeSeries::getIdList(TransactionContext &txn,
	util::XArray<uint8_t>& serializedRowList, util::XArray<RowId>& idList) {
	try {
		uint8_t* in = serializedRowList.data();
		for (size_t i = 0; i < (serializedRowList.size() / rowFixedDataSize_);
			 i++) {  
			idList.push_back(*reinterpret_cast<RowId *>(
				in +
				getColumnInfo(ColumnInfo::ROW_KEY_COLUMN_ID)
					.getColumnOffset()));
			in += rowFixedDataSize_;
		}
	}
	catch (std::exception& e) {
		handleSearchError(txn, e, GS_ERROR_DS_CON_GET_ROW_ID_LIST_FAILED);
	}
}

void TimeSeries::lockIdList(TransactionContext& txn, util::XArray<OId>& oIdList,
	util::XArray<RowId>& idList) {
	try {
		RowArray rowArray(txn, this);
		for (size_t i = 0; i < oIdList.size(); i++) {
			bool isOldSchema = rowArray.load(txn, oIdList[i], this, OBJECT_FOR_UPDATE);
			UNUSED_VARIABLE(isOldSchema);
			assert(isOldSchema || !isOldSchema);
			RowArray::Row row(rowArray.getRow(), &rowArray);
			row.lock(txn);
			idList.push_back(row.getRowId());
		}
	}
	catch (std::exception& e) {
		handleSearchError(txn, e, GS_ERROR_DS_CON_GET_ROW_ID_LIST_FAILED);
	}
}

void TimeSeries::setDummyMvccImage(TransactionContext &txn) {
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
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void TimeSeries::getContainerOptionInfo(
	TransactionContext &txn, util::XArray<uint8_t> &containerSchema) {
	bool isExistTimeSeriesOption = true;
	containerSchema.push_back(
		reinterpret_cast<uint8_t *>(&isExistTimeSeriesOption), sizeof(bool));

	int32_t tmpElapsedTime = -1;
	containerSchema.push_back(
		reinterpret_cast<const uint8_t *>(&tmpElapsedTime),
		sizeof(int32_t));
	int8_t tmp = static_cast<int8_t>(TIME_UNIT_DAY);
	containerSchema.push_back(
		reinterpret_cast<const uint8_t *>(&tmp), sizeof(int8_t));
	int32_t tmpDivisionCount = EXPIRE_DIVIDE_UNDEFINED_NUM;
	containerSchema.push_back(
		reinterpret_cast<const uint8_t *>(&tmpDivisionCount), sizeof(int32_t));

	int32_t tmpDuration = -1;
	containerSchema.push_back(
		reinterpret_cast<uint8_t *>(&tmpDuration), sizeof(int32_t));
	int8_t tmpTimeUnit = static_cast<int8_t>(TIME_UNIT_DAY);
	containerSchema.push_back(
		reinterpret_cast<uint8_t *>(&tmpTimeUnit), sizeof(int8_t));
	int8_t tmpCompressionType = 0;
	containerSchema.push_back(
		reinterpret_cast<uint8_t *>(&tmpCompressionType), sizeof(int8_t));
	uint32_t compressionInfoNum = 0;
	containerSchema.push_back(
		reinterpret_cast<uint8_t *>(&compressionInfoNum), sizeof(uint32_t));
}

util::String TimeSeries::getBibContainerOptionInfo(TransactionContext &) {
	return "";
}

void TimeSeries::checkContainerOption(
	MessageSchema*, util::XArray<uint32_t>&, bool&) {
	return;
}

template<bool Mvcc>
void TimeSeries::scanRowArrayCheckedDirect(
		TransactionContext& txn, RowArray &rowArray, OutputOrder order,
		RowId startRowId, RowId endRowId, util::XArray<OId> &oIdList) {

	if (order != ORDER_DESCENDING) {
		if (rowArray.begin()) {
			do {
				RowArray::Row row(rowArray.getRow(), &rowArray);
				const RowId rowId = row.getRowId();
				if (rowId >= endRowId) {
					break;
				}
				else if (rowId < startRowId ||
						filterActiveTransaction<Mvcc>(txn, row)) {
					continue;
				}
				oIdList.push_back(rowArray.getOId());
			}
			while (rowArray.next());
		}
	}
	else {
		if (rowArray.tail()) {
			do {
				RowArray::Row row(rowArray.getRow(), &rowArray);
				const RowId rowId = row.getRowId();
				if (rowId < startRowId) {
					break;
				}
				else if (rowId >= endRowId ||
						filterActiveTransaction<Mvcc>(txn, row)) {
					continue;
				}
				oIdList.push_back(rowArray.getOId());
			}
			while (rowArray.prev());
		}
	}
}

template<bool Mvcc>
void TimeSeries::scanRowCheckedDirect(
		TransactionContext& txn, RowArray &rowArray,
		RowId startRowId, RowId endRowId, util::XArray<OId> &oIdList) {
	RowArray::Row row(rowArray.getRow(), &rowArray);

	const RowId rowId = row.getRowId();
	if (rowId < startRowId || rowId >= endRowId) {
		return;
	}

	if (filterActiveTransaction<Mvcc>(txn, row)) {
		return;
	}

	oIdList.push_back(rowArray.getOId());
}

template<>
bool TimeSeries::filterActiveTransaction<true>(
		TransactionContext &txn, RowArray::Row &row) {
	return txn.getId() == row.getTxnId();
}

template<>
bool TimeSeries::filterActiveTransaction<false>(
		TransactionContext& txn, RowArray::Row &row) {
	return txn.getId() != row.getTxnId() &&
			!row.isFirstUpdate() &&
			txn.getManager().isActiveTransaction(
					txn.getPartitionId(), row.getTxnId());
}

RowId TimeSeries::getScanStartRowId() {
	return std::numeric_limits<RowId>::min();
}

bool TimeSeries::scanRowArrayChecked(
		TransactionContext& txn, RowArray *rowArray,
		const BtreeMap::SearchContext &sc, OutputOrder order,
		RowId startRowId, RowId endRowId, RowId lastCheckRowId,
		RowId &lastMergedRowId, size_t &restRowOIdCount,
		util::XArray< std::pair<RowId, OId> > &rowOIdList,
		util::XArray<OId> &mvccOIdList, util::XArray<OId> &mergedOIdList) {

	const bool onMvcc = false;
	if (order != ORDER_DESCENDING) {
		if (rowArray != NULL && rowArray->begin()) {
			do {
				RowArray::Row row(rowArray->getRow(), rowArray);
				const RowId rowId = row.getRowId();
				if (rowId >= endRowId) {
					break;
				}
				else if (rowId < startRowId ||
						filterActiveTransaction<onMvcc>(txn, row)) {
					continue;
				}
				rowOIdList.push_back(
						std::make_pair(rowId, rowArray->getOId()));
			}
			while (rowArray->next());
		}
	}
	else {
		if (rowArray != NULL && rowArray->tail()) {
			do {
				RowArray::Row row(rowArray->getRow(), rowArray);
				const RowId rowId = row.getRowId();
				if (rowId < startRowId) {
					break;
				}
				else if (rowId >= endRowId ||
						filterActiveTransaction<onMvcc>(txn, row)) {
					continue;
				}
				rowOIdList.push_back(
						std::make_pair(rowId, rowArray->getOId()));
			}
			while (rowArray->prev());
		}
	}

	if (rowArray != NULL && rowArray->getRowNum() >= restRowOIdCount) {
		restRowOIdCount -= rowArray->getRowNum();
		return false;
	}

	if (!isExclusive()) {
		BtreeMap::SearchContext mvccSC (txn.getDefaultAllocator(), UNDEF_COLUMNID);
		std::pair<RowId, RowId> mvccRowIdRange;
		searchMvccMapPrepare(
				txn, sc, mvccSC, order, startRowId, endRowId,
				lastCheckRowId, lastMergedRowId, &mvccRowIdRange);

		const bool checkOnly = false;
		const bool ignoreTxnCheck = false;
		searchMvccMap<TimeSeries, BtreeMap::SearchContext>(
				txn, mvccSC, mvccOIdList, checkOnly, ignoreTxnCheck);
		lastMergedRowId = lastCheckRowId;
	}

	if (!rowOIdList.empty() || !mvccOIdList.empty()) {
		mergeScannedRowArray(
				txn, order, rowOIdList, mvccOIdList, mergedOIdList);

		rowOIdList.clear();
		mvccOIdList.clear();
	}
	restRowOIdCount = 0;

	return true;
}

void TimeSeries::mergeScannedRowArray(
		TransactionContext& txn, OutputOrder order,
		util::XArray< std::pair<RowId, OId> > &rowOIdList,
		util::XArray<OId> &mvccOIdList, util::XArray<OId> &mergedOIdList) {
	typedef util::XArray<OId> OIdList;
	typedef util::XArray< std::pair<RowId, OId> > RowOIdList;

	if (mvccOIdList.empty()) {
		mergedOIdList.reserve(rowOIdList.size());
		for (RowOIdList::iterator it = rowOIdList.begin();
				it != rowOIdList.end(); ++it) {
			mergedOIdList.push_back(it->second);
		}
	}
	else {
		const size_t rowOIdPos = rowOIdList.size();

		RowArray rowArray(txn, this);
		rowOIdList.reserve(rowOIdList.size() + mvccOIdList.size());

		for (OIdList::iterator it = mvccOIdList.begin();
				it != mvccOIdList.end(); ++it) {

			rowArray.load(txn, *it, this, OBJECT_READ_ONLY);
			rowOIdList.push_back(std::make_pair(
					rowArray.getRowId(), rowArray.getOId()));
		}

		if (order != ORDER_DESCENDING) {
			std::sort(rowOIdList.begin() + rowOIdPos, rowOIdList.end());
		}
		else {
			std::greater<RowOIdList::value_type> pred;
			std::sort(
					rowOIdList.begin() + rowOIdPos, rowOIdList.end(), pred);
		}

		RowOIdList::iterator it1 = rowOIdList.begin();
		RowOIdList::iterator end1 = it1 + rowOIdPos;
		RowOIdList::iterator it2 = end1;
		RowOIdList::iterator end2 = rowOIdList.end();

		if (it1 != end1 && it2 != end2) {
			if (order != ORDER_DESCENDING) {
				for (;;) {
					if (it1->first < it2->first) {
						mergedOIdList.push_back(it1->second);
						if (++it1 == end1) {
							break;
						}
					}
					else {
						mergedOIdList.push_back(it2->second);
						if (++it2 == end1) {
							break;
						}
					}
				}
			}
			else {
				for (;;) {
					if (it1->first > it2->first) {
						mergedOIdList.push_back(it1->second);
						if (++it1 == end1) {
							break;
						}
					}
					else {
						mergedOIdList.push_back(it2->second);
						if (++it2 == end1) {
							break;
						}
					}
				}
			}
		}
		for (; it1 != end1; ++it1) {
			mergedOIdList.push_back(it1->second);
		}
		for (; it2 != end2; ++it2) {
			mergedOIdList.push_back(it2->second);
		}
	}
}

void TimeSeries::scanRowIdIndexPrepare(
		TransactionContext& txn, BtreeMap::SearchContext &sc,
		OutputOrder order, util::XArray<OId> *oIdList) {
	resolveExclusiveStatus<TimeSeries>(txn);

	const ResultSize limitBackup = sc.getLimit();
	if (sc.getRestConditionNum() > 0 || !isExclusive()) {
		sc.setLimit(MAX_RESULT_SIZE);
	}
	else if (sc.getLimit() < MAX_RESULT_SIZE) {
		sc.setLimit(sc.getLimit() + 1);
	}
	StackAllocAutoPtr<BtreeMap> rowIdMap(txn.getDefaultAllocator(), getRowIdMap(txn));
	rowIdMap.get()->search(txn, sc, *oIdList, order);
	sc.setLimit(limitBackup);
}

void TimeSeries::searchMvccMapPrepare(
		TransactionContext& txn, const BtreeMap::SearchContext &sc,
		BtreeMap::SearchContext &mvccSC, OutputOrder order,
		RowId startRowId, RowId endRowId,
		RowId lastCheckRowId, RowId lastMergedRowId,
		std::pair<RowId, RowId> *mvccRowIdRange) {

	RowId *mvccStartRowId = &mvccRowIdRange->first;
	RowId *mvccEndRowId = &mvccRowIdRange->second;

	if (order != ORDER_DESCENDING) {
		if (lastMergedRowId == UNDEF_ROWID) {
			*mvccStartRowId = startRowId;
		}
		else {
			*mvccStartRowId = lastMergedRowId + 1;
		}

		if (lastCheckRowId != UNDEF_ROWID) {
			*mvccEndRowId = std::min(lastCheckRowId + 1, endRowId);
		}
		else {
			*mvccEndRowId = endRowId;
		}
	}
	else {
		if (lastCheckRowId != UNDEF_ROWID) {
			*mvccStartRowId = std::max(lastCheckRowId, startRowId);
		}
		else {
			*mvccStartRowId = startRowId;
		}

		*mvccEndRowId = std::min(lastMergedRowId, endRowId);
	}

	const RowId minRowId = std::numeric_limits<RowId>::min();

	if (*mvccStartRowId != minRowId) {
		TermCondition newCond(COLUMN_TYPE_TIMESTAMP, COLUMN_TYPE_TIMESTAMP, 
			DSExpression::GE, sc.getScColumnId(), mvccStartRowId, sizeof(*mvccStartRowId));
		mvccSC.updateCondition(txn, newCond);
	}

	if (*mvccEndRowId != UNDEF_ROWID) {
		TermCondition newCond(COLUMN_TYPE_TIMESTAMP, COLUMN_TYPE_TIMESTAMP, 
			DSExpression::LT, sc.getScColumnId(), mvccEndRowId, sizeof(*mvccEndRowId));
		mvccSC.updateCondition(txn, newCond);
	}
}



/*!
	@brief Newly creates or updates a Row with a Row key of the current time on
   server
*/
void TimeSeries::appendRow(TransactionContext &txn, uint32_t rowSize,
	const uint8_t *rowDataWOTimestamp, Timestamp &rowKey,
	PutStatus &status) {
	try {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		setDirty();

		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not put. container's status is invalid.");
		}

		if (isExpired(txn)) {
			status = PutStatus::NOT_EXECUTED;
			return;
		}
		rowKey = txn.getStatementStartTime().getUnixTime();
		if (!ValueProcessor::validateTimestamp(rowKey)) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_TIM_ROW_DATA_INVALID,
				"Timestamp of AppendTime out of range (rowKey=" << rowKey
																<< ")");
		}
		InputMessageRowStore inputMessageRowStore(
			getDataStore()->getConfig(), getColumnInfoList(),
			getColumnNum(), const_cast<uint8_t *>(rowDataWOTimestamp), rowSize,
			1, rowFixedDataSize_);
		inputMessageRowStore.next();
		inputMessageRowStore.setField(
			ColumnInfo::ROW_KEY_COLUMN_ID, &rowKey, sizeof(Timestamp));
		bool dummy = false;
		putRowInternal(txn, &inputMessageRowStore, rowKey, dummy, status, 
			PUT_INSERT_OR_UPDATE);
	}
	catch (std::exception &e) {
		handleUpdateError(txn, e, GS_ERROR_DS_TIM_APPEND_ROW_FAILED);
	}
}


void TimeSeries::createMinimumRowArray(
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
	}
}


void TimeSeries::searchRowArrayList(TransactionContext &txn,
	BtreeMap::SearchContext &sc, util::XArray<OId> &normalOIdList,
	util::XArray<OId> &mvccOIdList) {
	RowArray mvccRowArray(txn, this);

	StackAllocAutoPtr<BtreeMap> rowIdMap(
		txn.getDefaultAllocator(), getRowIdMap(txn));
	rowIdMap.get()->search(txn, sc, normalOIdList);
	{
		util::XArray<std::pair<TransactionId, MvccRowImage> >
			mvccKeyValueList(txn.getDefaultAllocator());
		util::XArray<std::pair<TransactionId, MvccRowImage> >::iterator
			mvccItr;
		StackAllocAutoPtr<BtreeMap> mvccMap(
			txn.getDefaultAllocator(), getMvccMap(txn));
		mvccMap.get()->getAll<TransactionId, MvccRowImage>(
			txn, MAX_RESULT_SIZE, mvccKeyValueList);
		mvccOIdList.reserve(mvccKeyValueList.size());  
		{
			ColumnType targetType = COLUMN_TYPE_ROWID;
			util::XArray<SortKey> mvccSortKeyList(
				txn.getDefaultAllocator());
			for (mvccItr = mvccKeyValueList.begin();
				 mvccItr != mvccKeyValueList.end(); mvccItr++) {
				switch (mvccItr->second.type_) {
				case MVCC_SELECT:
				case MVCC_INDEX:
				case MVCC_CONTAINER:
				case MVCC_CREATE:
					break;
				case MVCC_UPDATE:
				case MVCC_DELETE:
					{
						if (mvccItr->first != txn.getId()) {
							mvccRowArray.load(txn, mvccItr->second.snapshotRowOId_,
								this, OBJECT_READ_ONLY);
							if (mvccRowArray.begin()) {
								RowArray::Row row(
									mvccRowArray.getRow(), &mvccRowArray);

								RowId rowId = row.getRowId();

								SortKey sortKey;
								sortKey.set(txn, targetType, reinterpret_cast<uint8_t *>(&rowId),
									mvccItr->second.snapshotRowOId_);
								mvccSortKeyList.push_back(sortKey);
							}
						}
					}
					break;
				default:
					GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
					break;
				}
			}
			const Operator sortOp = ComparatorTable::Lt()(
					COLUMN_TYPE_TIMESTAMP, COLUMN_TYPE_TIMESTAMP);
			bool isNullLast = true;
			std::sort(mvccSortKeyList.begin(), mvccSortKeyList.end(),
				SortPred(txn, sortOp, COLUMN_TYPE_TIMESTAMP, isNullLast));
			util::XArray<SortKey>::iterator mvccKeyItr;
			for (mvccKeyItr = mvccSortKeyList.begin();
				 mvccKeyItr != mvccSortKeyList.end(); mvccKeyItr++) {
				mvccOIdList.push_back(mvccKeyItr->getOId());
			}
		}
	}
}



/*!
	@brief Performs an aggregation operation on a Row set or its specific
   Columns, based on the specified start and end times
*/
void TimeSeries::aggregateByTimeWindow(TransactionContext &txn,
	uint32_t columnId, AggregationType type, Timestamp startTime, Timestamp endTime, 
	const Sampling &sampling, 
	util::XArray<OId> &oIdList, ResultSize &resultNum,
	MessageRowStore *messageRowStore) {
	resultNum = 0;
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
	
	ContainerValue containerValue(*getObjectManager(), getRowAllocateStrategy());
	AggregationCursor aggCursor;
	const AggregatorForLoop *aggLoop = &aggLoopTable[type];
	ColumnInfo &aggColumnInfo = getColumnInfo(columnId);
	aggCursor.set(type, aggColumnInfo.getColumnType());

	RowArray rowArray(txn, this);
	Timestamp firstRowTime = -1;
	if (!oIdList.empty()) {
		rowArray.load(txn, oIdList[0], this, OBJECT_READ_ONLY);
		RowArray::Row row(rowArray.getRow(), &rowArray);
		firstRowTime = row.getRowId();
	}
	
	Timestamp groupStartTime = startTime;
	Timestamp groupEndTime = groupStartTime + interval;
	while (groupEndTime < firstRowTime) {
		messageRowStore->beginRow();
		messageRowStore->setField<COLUMN_TYPE_TIMESTAMP>(
			ColumnInfo::ROW_KEY_COLUMN_ID, groupStartTime);
		for (ColumnId i = ColumnInfo::ROW_KEY_COLUMN_ID + 1; i < getColumnNum(); i++) {
			messageRowStore->setNull(i);
		}
		messageRowStore->next();
		groupStartTime = groupEndTime;
		groupEndTime += interval;
		resultNum++;
	}
	
	Value value;
	util::XArray<OId>::iterator itr;
	for (itr = oIdList.begin(); itr != oIdList.end(); itr++) {
		rowArray.load(txn, *itr, this, OBJECT_READ_ONLY);
		RowArray::Row row(rowArray.getRow(), &rowArray);
		Timestamp rowId = row.getRowId();
		if (rowId >= groupEndTime) {
			aggregationPostProcess(aggCursor, value);
			messageRowStore->beginRow();
			messageRowStore->setField<COLUMN_TYPE_TIMESTAMP>(
				ColumnInfo::ROW_KEY_COLUMN_ID, groupStartTime);
			for (ColumnId i = ColumnInfo::ROW_KEY_COLUMN_ID + 1; i < getColumnNum(); i++) {
				if (columnId == i && aggCursor.count_ != 0) {
					aggregationPostProcess(aggCursor, value);
					messageRowStore
						->setField<COLUMN_TYPE_DOUBLE>(
							i, value.getDouble());
					aggCursor.count_ = 0;
					aggCursor.value1_ = Value();
					aggCursor.value2_ = Value();
				} else {
					messageRowStore->setNull(i);
				}
			}
			messageRowStore->next();

			groupStartTime = groupEndTime;
			groupEndTime += interval;
			resultNum++;

			if (groupEndTime > endTime) {
				break;
			}
		}
		
		row.getField(txn, aggColumnInfo, containerValue);
		if (!containerValue.getValue().isNullValue()) {
			(*aggLoop)(txn, containerValue.getValue(), aggCursor);
		}
	}
	while (groupStartTime <= endTime) {
		messageRowStore->beginRow();
		messageRowStore->setField<COLUMN_TYPE_TIMESTAMP>(
			ColumnInfo::ROW_KEY_COLUMN_ID, groupStartTime);
		for (ColumnId i = ColumnInfo::ROW_KEY_COLUMN_ID + 1; i < getColumnNum(); i++) {
			if (columnId == i && aggCursor.count_ != 0) {
				aggregationPostProcess(aggCursor, value);
				messageRowStore
					->setField<COLUMN_TYPE_DOUBLE>(
						i, value.getDouble());
				aggCursor.count_ = 0;
				aggCursor.value1_ = Value();
				aggCursor.value2_ = Value();
			} else {
				messageRowStore->setNull(i);
			}
		}
		messageRowStore->next();
		groupStartTime = groupEndTime;
		groupEndTime += interval;
		resultNum++;
	}
}

/*!
	@brief Take a sampling of Rows within a specific range
*/
void TimeSeries::sample(TransactionContext &txn, BtreeMap::SearchContext &sc,
	const Sampling &sampling, ResultSize &resultNum,
	MessageRowStore *messageRowStore) {
	PartitionId pId = txn.getPartitionId();
	if (sc.getStartKey<Timestamp>() == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_KEY_RANGE_INVALID, "");
	}
	util::Vector<TermCondition> otherList(txn.getDefaultAllocator());
	sc.getConditionList(otherList, BaseIndex::SearchContext::COND_OTHER);
	if (!otherList.empty()) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_FILTERING_CONDITION_INVALID, "");
	}
	if (sc.getScColumnId() >= getColumnNum()) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_COLUMN_ID_INVALID, "");
	}
	if (sc.getEndKey<Timestamp>() == NULL) {
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
				GS_ERROR_DS_TIM_INTERPOLATED_COLUMN_TYPE_INVALID, "");
		}
	}

	Timestamp interval = 0;
	if (sampling.interval_ != 0) {
		interval = 
			MessageSchema::getTimestampDuration(sampling.interval_,
			sampling.timeUnit_);
	}

	util::Vector<TermCondition> condList(txn.getDefaultAllocator());
	sc.getConditionList(condList, BaseIndex::SearchContext::COND_OTHER);

	Timestamp startT = *sc.getStartKey<Timestamp>();
	Timestamp targetT = startT;

	ResultSize limit = sc.getLimit();
	if (interval != 0) {
		sc.setLimit(MAX_RESULT_SIZE);
	}
	util::XArray<OId> normalOIdList(txn.getDefaultAllocator());
	util::XArray<OId> mvccOIdList(txn.getDefaultAllocator());
	searchRowArrayList(txn, sc, normalOIdList, mvccOIdList);
	sc.setLimit(limit);

	util::XArray<OId>::iterator normalItr, mvccItr;
	RowArray normalRowArray(txn, this);
	RowArray mvccRowArray(txn, this);
	RowArray prevRowArray(txn, this);
	RowArray *normalRowArrayPtr = &normalRowArray;
	RowArray *mvccRowArrayPtr = &mvccRowArray;
	RowArray *rowArrayPtr = NULL;

	Timestamp endT = UNDEF_TIMESTAMP;
	if (sc.getEndKey<Timestamp>() != NULL) {
		endT = *sc.getEndKey<Timestamp>();
	}
	if (startT > endT) {
		resultNum = messageRowStore->getRowCount();
		return;
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
						pId, rowArray.getTxnId())) {
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
				TermCondition cond(getRowIdColumnType(), getRowIdColumnType(),
					DSExpression::GE, getRowIdColumnId(), &endT,
					sizeof(endT));
				BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, 1);
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
					pId, normalRowArrayPtr->getTxnId())) {
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
			TermCondition cond(getRowIdColumnType(), getRowIdColumnType(),
				DSExpression::LE, getRowIdColumnId(), &targetT,
				sizeof(targetT));
			BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, 1);
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
	ContainerValue containerValue(*getObjectManager(), getRowAllocateStrategy());
	ContainerValue prevContainerValue(*getObjectManager(), getRowAllocateStrategy());
	ContainerValue nextContainerValue(*getObjectManager(), getRowAllocateStrategy());
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
		opList[columnId].sub_ = CalculatorTable::Sub()(type, type);
		opList[columnId].mul_ = CalculatorTable::Mul()(type, COLUMN_TYPE_DOUBLE);
		opList[columnId].add_ = CalculatorTable::Add()(type, COLUMN_TYPE_DOUBLE);
	}

	bool isWithRowId = false;  

	while (normalItr != normalOIdList.end() || mvccItr != mvccOIdList.end()) {
		bool isNormalExecute = false;
		if (mvccItr == mvccOIdList.end() ||
			(normalItr != normalOIdList.end() &&
				normalBeginTime < mvccBeginTime)) {
			rowArrayPtr = normalRowArrayPtr;
			isNormalExecute = true;
		}
		else {
			rowArrayPtr = mvccRowArrayPtr;
			isNormalExecute = false;
		}

		if ((isNormalExecute &&
				(isExclusive() || txn.getId() == rowArrayPtr->getTxnId() ||
					rowArrayPtr->isFirstUpdate() ||
					!txn.getManager().isActiveTransaction(
						pId, rowArrayPtr->getTxnId()))) ||
			(!isNormalExecute && txn.getId() != rowArrayPtr->getTxnId())) {
			for (rowArrayPtr->begin(); !rowArrayPtr->end();
				 rowArrayPtr->next()) {
				RowArray::Row row(rowArrayPtr->getRow(), rowArrayPtr);
				Timestamp rowId = row.getRowId();

				bool isMatch = true;
				util::Vector<TermCondition>::iterator condItr;
				for (condItr = condList.begin(); condItr != condList.end(); condItr++) {
					if (!row.isMatch(txn, *condItr, containerValue)) {
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
					if (messageRowStore->getRowCount() >= sc.getLimit()) {
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
									if (prevContainerValue.getValue().isNullValue() ||
										nextContainerValue.getValue().isNullValue()) {
										messageRowStore->setNull(columnId);
									} else {

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
											GS_ERROR_DS_TIM_INTERPOLATED_COLUMN_TYPE_INVALID,
											"");
									}
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
						if (messageRowStore->getRowCount() >= sc.getLimit()) {
							goto LABEL_FINISH;
						}
						targetT += interval;
						if (endT != UNDEF_TIMESTAMP && targetT > endT) {
							goto LABEL_FINISH;
						}

						if (targetT == rowId) {
							row.getImage(txn, messageRowStore, isWithRowId);
							messageRowStore->next();
							if (messageRowStore->getRowCount() >= sc.getLimit()) {
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
		if (isNormalExecute) {
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

			if (messageRowStore->getRowCount() >= sc.getLimit()) {
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
	PartitionId pId = txn.getPartitionId();
	if (sc.getStartKey<Timestamp>() == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_KEY_RANGE_INVALID, "");
	}
	util::Vector<TermCondition> otherList(txn.getDefaultAllocator());
	sc.getConditionList(otherList, BaseIndex::SearchContext::COND_OTHER);
	if (!otherList.empty()) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_FILTERING_CONDITION_INVALID, "");
	}
	if (sc.getScColumnId() >= getColumnNum()) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_COLUMN_ID_INVALID, "");
	}
	if (sc.getEndKey<Timestamp>() == NULL) {
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
			GS_ERROR_DS_TIM_INTERPOLATED_COLUMN_IDLIST_INVALID, "");
	}

	Timestamp interval = 0;
	if (sampling.interval_ != 0) {
		interval = 
			MessageSchema::getTimestampDuration(sampling.interval_,
			sampling.timeUnit_);
	}

	util::Vector<TermCondition> condList(txn.getDefaultAllocator());
	sc.getConditionList(condList, BaseIndex::SearchContext::COND_OTHER);

	Timestamp startT = *sc.getStartKey<Timestamp>();
	Timestamp targetT = startT;

	Timestamp endT = UNDEF_TIMESTAMP;
	if (sc.getEndKey<Timestamp>() != NULL) {
		endT = *sc.getEndKey<Timestamp>();
	}
	if (startT > endT) {
		resultNum = messageRowStore->getRowCount();
		return;
	}
	ResultSize limit = sc.getLimit();
	if (interval != 0) {
		sc.setLimit(MAX_RESULT_SIZE);
	}
	util::XArray<OId> normalOIdList(txn.getDefaultAllocator());
	util::XArray<OId> mvccOIdList(txn.getDefaultAllocator());
	searchRowArrayList(txn, sc, normalOIdList, mvccOIdList);
	sc.setLimit(limit);

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
					pId, normalRowArrayPtr->getTxnId())) {
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

	ContainerValue containerValue(*getObjectManager(), getRowAllocateStrategy());
	Value value;
	bool isWithRowId = false;  
	while (normalItr != normalOIdList.end() || mvccItr != mvccOIdList.end()) {
		bool isNormalExecute = false;
		if (mvccItr == mvccOIdList.end() ||
			(normalItr != normalOIdList.end() &&
				normalBeginTime < mvccBeginTime)) {
			rowArrayPtr = normalRowArrayPtr;
			isNormalExecute = true;
		}
		else {
			rowArrayPtr = mvccRowArrayPtr;
			isNormalExecute = false;
		}

		if ((isNormalExecute &&
				(isExclusive() || txn.getId() == rowArrayPtr->getTxnId() ||
					rowArrayPtr->isFirstUpdate() ||
					!txn.getManager().isActiveTransaction(
						pId, rowArrayPtr->getTxnId()))) ||
			(!isNormalExecute && txn.getId() != rowArrayPtr->getTxnId())) {
			for (rowArrayPtr->begin(); !rowArrayPtr->end();
				 rowArrayPtr->next()) {
				RowArray::Row row(rowArrayPtr->getRow(), rowArrayPtr);
				Timestamp rowId = row.getRowId();

				bool isMatch = true;
				util::Vector<TermCondition>::iterator condItr;
				for (condItr = condList.begin(); condItr != condList.end(); condItr++) {
					if (!row.isMatch(txn, *condItr, containerValue)) {
						isMatch = false;
						break;
					}
				}

				if (!isMatch) {
					continue;
				}

				if (endT != UNDEF_TIMESTAMP && rowId > endT) {
					goto LABEL_FINISH;
				}

				if (targetT == rowId) {  

					row.getImage(txn, messageRowStore, isWithRowId);
					messageRowStore->next();

					if (interval == 0) {
						goto LABEL_FINISH;
					}
					if (messageRowStore->getRowCount() >= sc.getLimit()) {
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
							value.get(txn, *getObjectManager(), getRowAllocateStrategy(),
								messageRowStore, columnId);
						}
						messageRowStore->next();

						if (interval == 0) {
							goto LABEL_FINISH;
						}
						if (messageRowStore->getRowCount() >= sc.getLimit()) {
							goto LABEL_FINISH;
						}
						targetT += interval;


						if (targetT == rowId) {
							row.getImage(txn, messageRowStore, isWithRowId);
							messageRowStore->next();
							if (messageRowStore->getRowCount() >= sc.getLimit()) {
								goto LABEL_FINISH;
							}

							targetT += interval;
						}
						if (targetT > rowId) {
							break;
						}
					}
				}

				if (endT != UNDEF_TIMESTAMP && targetT > endT) {
					goto LABEL_FINISH;
				}
			}
		}
		if (isNormalExecute) {
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
	if (resultNum < sc.getLimit() && interval != 0) {
		size_t num =
			(size_t)((endT - startT) / interval) + 1;

		for (ResultSize i = resultNum; i < num; i++) {
			targetT = startT + interval * i;

			messageRowStore->beginRow();

			messageRowStore->setField<COLUMN_TYPE_TIMESTAMP>(
				ColumnInfo::ROW_KEY_COLUMN_ID, targetT);
			for (uint32_t columnId = 1; columnId < getColumnNum(); columnId++) {
				ColumnInfo &columnInfo = getColumnInfo(columnId);
				value.init(columnInfo.getColumnType());
				value.get(txn, *getObjectManager(), getRowAllocateStrategy(),
					messageRowStore, columnId);
			}
			messageRowStore->next();
			if (messageRowStore->getRowCount() >= sc.getLimit()) {
				break;
			}
		}
		resultNum = messageRowStore->getRowCount();
	}
}

/*!
	@brief Performs an aggregation operation on a Row set or its specific
   Columns, based on the specified start and end times
*/
void TimeSeries::aggregate(TransactionContext &txn, BtreeMap::SearchContext &sc,
	uint32_t columnId, AggregationType type, ResultSize &resultNum,
	Value &value) {
	PartitionId pId = txn.getPartitionId();
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
					!ValueProcessor::isTimestampFamily(colType)) {
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

	util::Vector<TermCondition> keyCondList(txn.getDefaultAllocator());
	sc.getConditionList(keyCondList, BaseIndex::SearchContext::COND_KEY);
	util::Vector<TermCondition> condList(txn.getDefaultAllocator());
	sc.getConditionList(condList, BaseIndex::SearchContext::COND_OTHER);
	searchRowArrayList(txn, sc, normalOIdList, mvccOIdList);

	ContainerValue containerValue(*getObjectManager(), getRowAllocateStrategy());
	switch (type) {
	case AGG_TIME_AVG: {
		ContainerValue containerValueTs(*getObjectManager(), getRowAllocateStrategy());
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
			bool isNormalExecute = false;
			if (mvccItr == mvccOIdList.end() ||
				(normalItr != normalOIdList.end() &&
					normalBeginTime < mvccBeginTime)) {
				rowArrayPtr = normalRowArrayPtr;
				isNormalExecute = true;
			}
			else {
				rowArrayPtr = mvccRowArrayPtr;
				isNormalExecute = false;
			}

			if ((isNormalExecute &&
					(isExclusive() || txn.getId() == rowArrayPtr->getTxnId() ||
						rowArrayPtr->isFirstUpdate() ||
						!txn.getManager().isActiveTransaction(
							pId, rowArrayPtr->getTxnId()))) ||
				(!isNormalExecute && txn.getId() != rowArrayPtr->getTxnId())) {
				for (rowArrayPtr->begin(); !rowArrayPtr->end();
					 rowArrayPtr->next()) {
					RowArray::Row row(rowArrayPtr->getRow(), rowArrayPtr);
					Timestamp rowId = row.getRowId();
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
						row.getField(txn, keyColumnInfo, containerValueTs);
						row.getField(txn, aggColumnInfo, containerValue);
						if (containerValue.getValue().isNullValue()) {
						} else
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

			if (isNormalExecute) {
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
					pId, rowArray.getTxnId())) {
				continue;
			}
			for (rowArray.begin(); !rowArray.end(); rowArray.next()) {
				RowArray::Row row(rowArray.getRow(), &rowArray);
				Timestamp rowId = row.getRowId();
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
					pId, rowArray.getTxnId())) {
				continue;
			}
			for (rowArray.begin(); !rowArray.end(); rowArray.next()) {
				RowArray::Row row(rowArray.getRow(), &rowArray);
				Timestamp rowId = row.getRowId();
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
					row.getField(txn, aggColumnInfo, containerValue);
					if (!containerValue.getValue().isNullValue()) {
						(*aggLoop)(txn, containerValue.getValue(), aggCursor);
					}
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
					row.getField(txn, aggColumnInfo, containerValue);
					if (!containerValue.getValue().isNullValue()) {
						(*aggLoop)(txn, containerValue.getValue(), aggCursor);
					}
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
}

/*!
	@brief Returns one Row related with the specified time
*/
void TimeSeries::searchTimeOperator(
	TransactionContext &txn, Timestamp ts, TimeOperator timeOp, OId &oId) {
	BtreeMap::SearchContext sc (txn.getDefaultAllocator(), ColumnInfo::ROW_KEY_COLUMN_ID);
	sc.setLimit(1);
	searchTimeOperator(txn, sc, ts, timeOp, oId);
}

/*!
	@brief Returns one Row related with the specified time within range condition
*/
void TimeSeries::searchTimeOperator(
	TransactionContext &txn, BtreeMap::SearchContext &sc, Timestamp ts, TimeOperator timeOp, OId &oId) {
	util::XArray<OId> oIdList(txn.getDefaultAllocator());
	assert(sc.getScColumnId() == ColumnInfo::ROW_KEY_COLUMN_ID);
	assert(sc.getRestConditionNum() == 0);
	sc.setLimit(1);
	switch (timeOp) {
	case TIME_PREV_ONLY: {
		TermCondition newCond(COLUMN_TYPE_TIMESTAMP, COLUMN_TYPE_TIMESTAMP,
			DSExpression::LT, ColumnInfo::ROW_KEY_COLUMN_ID,
			&ts, sizeof(Timestamp));
		sc.updateCondition(txn, newCond);
		searchRowIdIndex(txn, sc, oIdList, ORDER_DESCENDING);
	} break;
	case TIME_PREV: {
		TermCondition newCond(COLUMN_TYPE_TIMESTAMP, COLUMN_TYPE_TIMESTAMP,
			DSExpression::LE, ColumnInfo::ROW_KEY_COLUMN_ID,
			&ts, sizeof(Timestamp));
		sc.updateCondition(txn, newCond);
		searchRowIdIndex(txn, sc, oIdList, ORDER_DESCENDING);
	} break;
	case TIME_NEXT: {
		TermCondition newCond(COLUMN_TYPE_TIMESTAMP, COLUMN_TYPE_TIMESTAMP,
			DSExpression::GE, ColumnInfo::ROW_KEY_COLUMN_ID,
			&ts, sizeof(Timestamp));
		sc.updateCondition(txn, newCond);
		searchRowIdIndex(txn, sc, oIdList, ORDER_ASCENDING);
	} break;
	case TIME_NEXT_ONLY: {
		TermCondition newCond(COLUMN_TYPE_TIMESTAMP, COLUMN_TYPE_TIMESTAMP,
			DSExpression::GT, ColumnInfo::ROW_KEY_COLUMN_ID,
			&ts, sizeof(Timestamp));
		sc.updateCondition(txn, newCond);
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


