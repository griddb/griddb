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
#include "rtree_map.h"
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
	{false, false, true},  
	{false, false, false}  
};


/*!
	@brief Allocate Collection Object
*/
void Collection::initialize(TransactionContext& txn) {
	UNUSED_VARIABLE(txn);
	baseContainerImage_ =
		BaseObject::allocate<CollectionImage>(sizeof(CollectionImage),
			getMetaAllcateStrategy(), getBaseOId(), OBJECT_TYPE_COLLECTION);
	memset(baseContainerImage_, 0, sizeof(CollectionImage));
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
	baseContainerImage_->tablePartitioningVersionId_ = UNDEF_TABLE_PARTITIONING_VERSIONID;
	baseContainerImage_->startTime_ = 0;

	reinterpret_cast<CollectionImage*>(baseContainerImage_)->maxRowId_ = -1;
	reinterpret_cast<CollectionImage *>(baseContainerImage_)->padding1_ = 0;
	reinterpret_cast<CollectionImage *>(baseContainerImage_)->padding2_ = 0;


	rowImageSize_ = 0;
	metaAllocateStrategy_ = AllocateStrategy();
	rowAllocateStrategy_ = AllocateStrategy();
	mapAllocateStrategy_ = AllocateStrategy();
}

/*!
	@brief Set Collection Schema
*/
void Collection::set(TransactionContext& txn, const FullContainerKey &containerKey,
	ContainerId containerId, OId columnSchemaOId,
	MessageSchema* containerSchema) {
	baseContainerImage_->containerId_ = containerId;
	setTablePartitioningVersionId(containerSchema->getTablePartitioningVersionId());
	setContainerExpirationStartTime(containerSchema->getContainerExpirationStartTime());

	FullContainerKeyCursor keyCursor(txn.getPartitionId(), *getObjectManager());
	keyCursor.initialize(txn, containerKey, getMetaAllcateStrategy());
	baseContainerImage_->containerNameOId_ = keyCursor.getBaseOId();

	baseContainerImage_->columnSchemaOId_ = columnSchemaOId;
	commonContainerSchema_ =
		ALLOC_NEW(txn.getDefaultAllocator()) ShareValueList(
			txn, *getObjectManager(), baseContainerImage_->columnSchemaOId_);

	columnSchema_ =
		commonContainerSchema_->get<ColumnSchema>(META_TYPE_COLUMN_SCHEMA);

	indexSchema_ = ALLOC_NEW(txn.getDefaultAllocator())
		IndexSchema(txn, *getObjectManager(), getMetaAllcateStrategy());
	bool onMemory = false;
	indexSchema_->initialize(txn, IndexSchema::INITIALIZE_RESERVE_NUM, 0, getColumnNum(), onMemory);
	baseContainerImage_->indexSchemaOId_ = indexSchema_->getBaseOId();

	rowFixedDataSize_ = calcRowFixedDataSize();
	rowImageSize_ = calcRowImageSize(rowFixedDataSize_);

	baseContainerImage_->normalRowArrayNum_ = calcRowArrayNum(
		txn, getDataStore()->getConfig().isRowArraySizeControlMode(),
		ROW_ARRAY_MAX_SIZE);

	setAllocateStrategy();

	if (isExpired(txn)) {
		dataStore_->setLastExpiredTime(txn.getPartitionId(), txn.getStatementStartTime().getUnixTime());
	} else {

	util::StackAllocator &alloc = txn.getDefaultAllocator();
	util::Vector<ColumnId> columnIds(alloc);
	columnIds.push_back(UNDEF_COLUMNID);
	rowIdFuncInfo_ = ALLOC_NEW(alloc) TreeFuncInfo(alloc);
	rowIdFuncInfo_->initialize(columnIds, NULL);
	mvccFuncInfo_ = rowIdFuncInfo_;

	BtreeMap map(txn, *getObjectManager(), getMapAllcateStrategy(), this, rowIdFuncInfo_);
	map.initialize(
		txn, COLUMN_TYPE_TIMESTAMP, true, BtreeMap::TYPE_UNIQUE_RANGE_KEY);
	baseContainerImage_->rowIdMapOId_ = map.getBaseOId();

	BtreeMap mvccMap(txn, *getObjectManager(), getMapAllcateStrategy(), this, mvccFuncInfo_);
	mvccMap.initialize<TransactionId, MvccRowImage>(
		txn, COLUMN_TYPE_OID, false, BtreeMap::TYPE_SINGLE_KEY);
	baseContainerImage_->mvccMapOId_ = mvccMap.getBaseOId();

	if (!containerSchema->getRowKeyColumnIdList().empty()) {
		const util::Vector<ColumnId> &columnIds = containerSchema->getRowKeyColumnIdList();
		IndexInfo indexInfo(txn.getDefaultAllocator(),
			columnIds, MAP_TYPE_BTREE);
		IndexCursor indexCursor(true);
		createIndex(txn, indexInfo, indexCursor);
	}
	}

	setNullsStatsStatus();

	rowArrayCache_ = ALLOC_NEW(txn.getDefaultAllocator())
		RowArray(txn, this);
}

/*!
	@brief Free Objects related to Collection
*/
bool Collection::finalize(TransactionContext& txn) {
	try {
		setDirty();
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

			getDataStore()->finalizeMap(txn, getMapAllcateStrategy(), rowIdMap.get());
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
			getDataStore()->finalizeMap(txn, getMapAllcateStrategy(), mvccMap.get());
		}

		finalizeIndex(txn);

		commonContainerSchema_->reset();  
		getDataStore()->removeColumnSchema(
			txn, txn.getPartitionId(), baseContainerImage_->columnSchemaOId_);

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

	return true;
}

/*!
	@brief Creates a specifed type of index on the specified Column
*/
void Collection::createIndex(TransactionContext& txn, 
	const IndexInfo& indexInfo, IndexCursor& indexCursor,
	bool isIndexNameCaseSensitive) {
	try {
		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not create index. container's status is invalid.");
		}
		if (isExpired(txn)) {
			indexCursor.setRowId(MAX_ROWID);
			dataStore_->setLastExpiredTime(txn.getPartitionId(), txn.getStatementStartTime().getUnixTime());
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
				getDataStore()->getValueLimitConfig().getLimitContainerNameSize()),
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
				realIndexInfo.mapType = defaultIndexType[columnInfo.getColumnType()];
			}
		}
		MapType inputMapType = realIndexInfo.mapType;

		GS_TRACE_INFO(COLLECTION, GS_TRACE_DS_CON_CREATE_INDEX,
			"Collection Id = " << getContainerId()
							   << ", first columnNumber = " << realIndexInfo.columnIds_[0]
							   << ", type = " << getMapTypeStr(inputMapType));

		if (!isSupportIndex(realIndexInfo)) {
			GS_THROW_USER_ERROR(
				GS_ERROR_CM_NOT_SUPPORTED, "not support this index type");
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
					DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_COL_LOCK_CONFLICT,
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
			DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_COL_LOCK_CONFLICT,
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

		uint32_t limitNum = getDataStore()->getValueLimitConfig().getLimitIndexNum();
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
				DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_COL_LOCK_CONFLICT,
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
				inputMapType, columnTypes, this,
				UNDEF_CONTAINER_POS, isUnique);
			indexCursor.setOption(indexData.optionOId_);
			if (!indexCursor.isImmediateMode()) {
				beforeImage = indexCursor.getMvccImage();
				insertMvccMap(txn, mvccMap.get(), tId, beforeImage);
			} else {
				indexSchema_->commit(txn, indexCursor);
			}
		}

		indexInsertImpl<Collection>(txn, indexData, indexCursor.isImmediateMode());

		indexCursor.setRowId(indexData.cursor_);
		updateIndexData(txn, indexData);

	}
	catch (std::exception& e) {
		handleUpdateError(txn, e, GS_ERROR_DS_COL_CREATE_INDEX_FAILED);
	}
}

/*!
	@brief Continues to create a specifed type of index on the specified Column
*/
void Collection::continueCreateIndex(TransactionContext& txn, 
	IndexCursor& indexCursor) {
	try {
		setDirty();
		if (isExpired(txn)) {
			indexCursor.setRowId(MAX_ROWID);
			dataStore_->setLastExpiredTime(txn.getPartitionId(), txn.getStatementStartTime().getUnixTime());
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
		indexInsertImpl<Collection>(txn, indexData, indexCursor.isImmediateMode());

		indexCursor.setRowId(indexData.cursor_);
		updateIndexData(txn, indexData);
	}
	catch (std::exception& e) {
		handleUpdateError(txn, e, GS_ERROR_DS_COL_CREATE_INDEX_FAILED);
	}
}

void Collection::continueChangeSchema(TransactionContext &txn,
	ContainerCursor &containerCursor) {

	try {
		setDirty();
		if (isExpired(txn)) {
			containerCursor.setRowId(MAX_ROWID);
			dataStore_->setLastExpiredTime(txn.getPartitionId(), txn.getStatementStartTime().getUnixTime());
			return;
		}

		MvccRowImage beforeImage = containerCursor.getMvccImage();
		ContainerAutoPtr containerAutoPtr(txn, dataStore_, 
			txn.getPartitionId(), containerCursor);
		BaseContainer *newContainer = containerAutoPtr.getBaseContainer();

		util::XArray<uint32_t> copyColumnMap(txn.getDefaultAllocator());
		makeCopyColumnMap(txn, *newContainer, copyColumnMap);

		RowId rowIdCursor = containerCursor.getRowId();
		changeSchemaRecord<Collection>(txn, *newContainer, copyColumnMap,
			rowIdCursor, containerCursor.isImmediateMode());

		newContainer->commit(txn);
		containerCursor.setRowId(rowIdCursor);
		if (!containerCursor.isImmediateMode()) {
			MvccRowImage mvccImage = containerCursor.getMvccImage();
			StackAllocAutoPtr<BtreeMap> mvccMap(
				txn.getDefaultAllocator(), getMvccMap(txn));
			updateMvccMap(txn, mvccMap.get(), txn.getId(), beforeImage, mvccImage);
		}
	}
	catch (std::exception& e) {
		handleUpdateError(txn, e, GS_ERROR_DS_COL_CREATE_INDEX_FAILED);
	}
}

/*!
	@brief Creates a specifed type of index on the specified Column
*/
void Collection::dropIndex(TransactionContext& txn, IndexInfo& indexInfo,
						   bool isIndexNameCaseSensitive) {
	try {
		GS_TRACE_DEBUG(COLLECTION, GS_TRACE_DS_CON_DROP_INDEX,
			"Collection Id = " << getContainerId());

		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not delete index. container's status is invalid.");
		}
		if (isExpired(txn)) {
			dataStore_->setLastExpiredTime(txn.getPartitionId(), txn.getStatementStartTime().getUnixTime());
			return;
		}

		util::Vector<IndexInfo> matchList(txn.getDefaultAllocator());
		util::Vector<IndexInfo> mismatchList(txn.getDefaultAllocator());
		bool withUncommitted = false;
		indexSchema_->getIndexInfoList(txn, this, indexInfo, withUncommitted,
			matchList, mismatchList, isIndexNameCaseSensitive);

		if (isAlterContainer() && !matchList.empty()) {
			DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_COL_LOCK_CONFLICT,
				"(pId=" << txn.getPartitionId() << ", containerId=" << getContainerId()
						<< ", txnId=" << txn.getId() << ")");
		}

		for (size_t i = 0; i < matchList.size(); i++) {
			indexSchema_->dropIndexData(txn, matchList[i].columnIds_,
				matchList[i].mapType, this, UNDEF_CONTAINER_POS, true);
			indexSchema_->dropIndexInfo(
				txn, matchList[i].columnIds_, matchList[i].mapType);
		}
	}
	catch (std::exception& e) {
		handleUpdateError(txn, e, GS_ERROR_DS_COL_DROP_INDEX_FAILED);
	}
}

/*!
	@brief Newly creates or updates a Row, based on the specified Row object and
   also the Row key specified as needed
*/
void Collection::putRow(TransactionContext &txn, uint32_t rowSize,
	const uint8_t *rowData, RowId &rowId, bool rowIdSpecified, 
	DataStore::PutStatus &status, PutRowOption putRowOption)
{
	try {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		setDirty();

		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not put. container's status is invalid.");
		}
		if (isExpired(txn)) {
			status = DataStore::NOT_EXECUTED;
			return;
		}
		InputMessageRowStore inputMessageRowStore(
			getDataStore()->getValueLimitConfig(), getRealColumnInfoList(txn),
			getRealColumnNum(txn), const_cast<uint8_t *>(rowData), rowSize, 1,
			getRealRowFixedDataSize(txn), false);
		inputMessageRowStore.next();

		if (isAlterContainer()) {
			DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_COL_LOCK_CONFLICT,
				"(pId=" << txn.getPartitionId() << ", containerId=" << getContainerId()
						<< ", txnId=" << txn.getId() << ")");
		}

		putRow(txn, &inputMessageRowStore, rowId, rowIdSpecified, status,
			putRowOption);
	}
	catch (std::exception &e) {
		handleUpdateError(txn, e, GS_ERROR_DS_COL_PUT_ROW_FAILED);
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

		if (isExpired(txn)) {
			rowId = UNDEF_ROWID;
			existing = false;
			return;
		}

		if (isAlterContainer()) {
			DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_COL_LOCK_CONFLICT,
				"(pId=" << txn.getPartitionId() << ", containerId=" << getContainerId()
						<< ", txnId=" << txn.getId() << ")");
		}

		if (!definedRowKey()) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_COL_ROWKEY_UNDEFINED, "");
		}

		util::Vector<ColumnId> keyColumnIdList(txn.getDefaultAllocator());
		getKeyColumnIdList(keyColumnIdList);

		util::XArray<KeyData> keyFields(txn.getDefaultAllocator());
		getRowKeyFields(txn, rowKeySize, rowKey, keyFields);
		OId targetOId = UNDEF_OID;
		if (!isUnique(txn, keyColumnIdList, keyFields, targetOId)) {
			RowArray rowArray(txn, this);
			bool isOldSchema = rowArray.load(txn, targetOId, this, OBJECT_FOR_UPDATE);
			if (isOldSchema) {
				RowScope rowScope(*this, TO_NORMAL);
				convertRowArraySchema(txn, rowArray, false);
			}
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

		if (isExpired(txn)) {
			existing = false;
			return;
		}

		if (isAlterContainer() && !isForceLock) {
			DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_COL_LOCK_CONFLICT,
				"(pId=" << txn.getPartitionId() << ", containerId=" << getContainerId()
						<< ", txnId=" << txn.getId() << ")");
		}

		RowArray rowArray(txn, this);
		TermCondition cond(getRowIdColumnType(), getRowIdColumnType(), 
			DSExpression::EQ, getRowIdColumnId(), &rowId, sizeof(rowId));
		BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, 1);
		util::XArray<OId> oIdList(txn.getDefaultAllocator());
		searchRowIdIndex(txn, sc, oIdList, ORDER_UNDEFINED);
		if (!oIdList.empty()) {
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

		if (isExpired(txn)) {
			status = DataStore::NOT_EXECUTED;
			return;
		}

		InputMessageRowStore inputMessageRowStore(
			getDataStore()->getValueLimitConfig(), getColumnInfoList(),
			getColumnNum(), const_cast<uint8_t*>(rowData), rowSize, 1,
			rowFixedDataSize_);
		inputMessageRowStore.next();

		if (isAlterContainer()) {
			DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_COL_LOCK_CONFLICT,
				"(pId=" << txn.getPartitionId() << ", containerId=" << getContainerId()
						<< ", txnId=" << txn.getId() << ")");
		}

		RowArray rowArray(txn, this);
		TermCondition cond(getRowIdColumnType(), getRowIdColumnType(), 
			DSExpression::EQ, getRowIdColumnId(), &rowId, sizeof(rowId));
		BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, 1);
		util::XArray<OId> oIdList(txn.getDefaultAllocator());
		searchRowIdIndex(txn, sc, oIdList, ORDER_UNDEFINED);
		if (!oIdList.empty()) {
			bool isOldSchema = rowArray.load(txn, oIdList[0], this, OBJECT_FOR_UPDATE);
			if (isOldSchema) {
				RowScope rowScope(*this, TO_NORMAL);
				convertRowArraySchema(txn, rowArray, false); 
			}
			RowArray::Row row(rowArray.getRow(), &rowArray);

			util::Vector<ColumnId> keyColumnIdList(txn.getDefaultAllocator());
			getKeyColumnIdList(keyColumnIdList);
			for (util::Vector<ColumnId>::iterator itr = keyColumnIdList.begin();
				itr != keyColumnIdList.end(); itr++) {
				BaseObject baseFieldObject(
					txn.getPartitionId(), *getObjectManager());
				row.getField(txn, getColumnInfo(*itr),
					baseFieldObject);
				if (ValueProcessor::compare(txn, *getObjectManager(),
						*itr, &inputMessageRowStore,
						baseFieldObject.getCursor<uint8_t>()) != 0) {
					GS_THROW_USER_ERROR(GS_ERROR_DS_COL_ROWKEY_INVALID,
						"Row key is not same value");
				}
			}	
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

		if (isExpired(txn)) {
			dataStore_->setLastExpiredTime(txn.getPartitionId(), txn.getStatementStartTime().getUnixTime());
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
							bool isUndo = false;
							for (rowArray.begin(); !rowArray.end();
								 rowArray.next()) {
								RowArray::Row row(rowArray.getRow(), &rowArray);
								if (row.getTxnId() == tId && !row.isFirstUpdate()) {
									if (!isUndo) {
										isUndo = true;
										bool isOldSchema = rowArray.setDirty(txn);
										UNUSED_VARIABLE(isOldSchema);
										assert(isOldSchema || !isOldSchema);
									}
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
							indexData.mapType_, this, UNDEF_CONTAINER_POS, true);
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
							txn.getPartitionId(), containerCursor);
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
		if (isExpired(txn)) {
			dataStore_->setLastExpiredTime(txn.getPartitionId(), txn.getStatementStartTime().getUnixTime());
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
						GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_COL_LOCK_CONFLICT, 
							"commit : container already locked "
							<< ", partitionId = " << txn.getPartitionId()
							<< ", txnId = " << txn.getId()
							<< ", containerId = " << getContainerId()
							);
					}
					switch (itr->type_) {
					case MVCC_CREATE:
					case MVCC_SELECT:
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
							dataStore_->updateContainer(txn, this, containerCursor.getContainerOId());
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
		handleUpdateError(txn, e, GS_ERROR_DS_COL_COMMIT_FAILED);
	}
}

/*!
	@brief Check if Container has data of uncommited transaction
*/
bool Collection::hasUncommitedTransaction(TransactionContext& txn) {
	if (isExpired(txn)) {
		return false;
	}
	StackAllocAutoPtr<BtreeMap> mvccMap(
		txn.getDefaultAllocator(), getMvccMap(txn));
	if (mvccMap.get()->isEmpty()) {
		return false;
	}
	else {
		return true;
	}
}

bool Collection::checkRunTime(TransactionContext &txn) {
	checkExclusive(txn);
	return !isNoRowTransaction();
}

/*!
	@brief Search Btree Index of RowId
*/
void Collection::searchRowIdIndex(TransactionContext& txn,
	BtreeMap::SearchContext& sc, util::XArray<OId>& resultList,
	OutputOrder outputOrder) {

	util::XArray<OId> mvccList(txn.getDefaultAllocator());
	ResultSize limitBackup = sc.getLimit();
	sc.setLimit(MAX_RESULT_SIZE);
	bool isCheckOnly = sc.getResumeStatus() != BaseIndex::SearchContext::NOT_RESUME;
	const bool ignoreTxnCheck = false;
	searchMvccMap<Collection, BtreeMap::SearchContext>(txn, sc, mvccList, isCheckOnly, ignoreTxnCheck);

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
	const RowId *startKey = sc.getStartKey<RowId>();
	const RowId *endKey = sc.getEndKey<RowId>();

	RowId lastCheckRowId = UNDEF_ROWID;
	RowArray rowArray(txn, this);
	for (itr = oIdList.begin(); itr != oIdList.end(); itr++) {
		rowArray.load(txn, *itr, this, OBJECT_READ_ONLY);
		lastCheckRowId = rowArray.getRowId();
		if (outputOrder != ORDER_DESCENDING) {
			if (startKey != NULL && *startKey > rowArray.getRowId()) {
				rowArray.searchNextRowId(*startKey);
			} else {
				rowArray.begin();
			}
			for (; !rowArray.end(); rowArray.next()) {
				RowArray::Row row(rowArray.getRow(), &rowArray);
				lastCheckRowId = row.getRowId();
				if (!isExclusive() && txn.getId() != row.getTxnId() &&
					!row.isFirstUpdate() &&
					txn.getManager().isActiveTransaction(
						txn.getPartitionId(), row.getTxnId())) {
					continue;
				}
				RowId rowId = row.getRowId();
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
				lastCheckRowId = row.getRowId();
				if (!isExclusive() && txn.getId() != row.getTxnId() &&
					!row.isFirstUpdate() &&
					txn.getManager().isActiveTransaction(
						txn.getPartitionId(), row.getTxnId())) {
					continue;
				}
				RowId rowId = row.getRowId();
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
	searchMvccMap<Collection, BtreeMap::SearchContext>(txn, sc, mvccList, isCheckOnly, ignoreTxnCheck);
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

void Collection::scanRowIdIndex(
		TransactionContext& txn, BtreeMap::SearchContext &sc,
		ContainerRowScanner &scanner) {

	RowId startRowId;
	RowId endRowId;
	if (!getRowIdRangeCondition(
		txn.getDefaultAllocator(),
		sc, getContainerType(), &startRowId, &endRowId)) {
		return;
	}

	util::XArray<OId> oIdList(txn.getDefaultAllocator());
	scanRowIdIndexPrepare(txn, sc, &oIdList);

	util::XArray<OId> checkedOIdList(txn.getDefaultAllocator());
	RowId lastCheckRowId = UNDEF_ROWID;

	bool limitReached = false;
	RowArray &rowArray = scanner.getRowArray();

	for (util::XArray<OId>::iterator it = oIdList.begin();
			it != oIdList.end(); ++it) {
		rowArray.load(txn, *it, this, OBJECT_READ_ONLY);

		{
			if (!rowArray.tail()) {
				continue;
			}

			lastCheckRowId = rowArray.getCurrentRowId();
			rowArray.begin();
		}

		if (isExclusive() &&
				startRowId <= rowArray.getRowId() &&
				lastCheckRowId < endRowId) {
			if (!scanner.scanRowArrayUnchecked(
					txn, *this, &rowArray, &(*it), &(*it) + 1)) {
				limitReached = true;
				break;
			}
		}
		else {
			const bool onMvcc = false;
			scanRowArrayCheckedDirect<onMvcc>(
					txn, rowArray, startRowId, endRowId, checkedOIdList);

			if (!checkedOIdList.empty()) {
				if (!scanner.scanRowUnchecked(
						txn, *this, &rowArray,
						&checkedOIdList.front(),
						&checkedOIdList.back() + 1)) {
					limitReached = true;
					break;
				}
				checkedOIdList.clear();
			}
		}
	}

	if (!limitReached && !isExclusive()) {
		BtreeMap::SearchContext mvccSC (txn.getDefaultAllocator(), UNDEF_COLUMNID);
		std::pair<RowId, RowId> mvccRowIdRange;
		searchMvccMapPrepare(
				txn, sc, mvccSC, startRowId, lastCheckRowId, &mvccRowIdRange);

		searchMvccMap<Collection, BtreeMap::SearchContext>(
				txn, mvccSC, scanner);
	}

	if (sc.isSuspended() && lastCheckRowId != UNDEF_ROWID) {
		++lastCheckRowId;

		sc.setSuspendPoint(txn, &lastCheckRowId, sizeof(RowId), 0);
	}
}

void Collection::scanRowArray(
		TransactionContext& txn, const OId *begin, const OId *end,
		bool onMvcc, ContainerRowScanner &scanner) {

	resolveExclusiveStatus<Collection>(txn);

	if (isExclusive()) {
		scanner.scanRowArrayUnchecked(txn, *this, NULL, begin, end);
	}
	else {
		RowArray &rowArray = scanner.getRowArray();

		const RowId startRowId = std::numeric_limits<RowId>::min();
		const RowId endRowId = UNDEF_ROWID;

		util::XArray<OId> oIdList(txn.getDefaultAllocator());

		for (const OId *it = begin; it != end; ++it) {
			rowArray.load(txn, *it, this, OBJECT_READ_ONLY);
			if (onMvcc) {
				scanRowArrayCheckedDirect<true>(
						txn, rowArray, startRowId, endRowId, oIdList);
			}
			else {
				scanRowArrayCheckedDirect<false>(
						txn, rowArray, startRowId, endRowId, oIdList);
			}

			if (!oIdList.empty()) {
				if (!scanner.scanRowUnchecked(
						txn, *this, &rowArray,
						&oIdList.front(), &oIdList.back() + 1)) {
					break;
				}
				oIdList.clear();
			}
		}
	}
}

void Collection::scanRow(
		TransactionContext& txn, const OId *begin, const OId *end,
		bool onMvcc, ContainerRowScanner &scanner) {

	resolveExclusiveStatus<Collection>(txn);

	if (isExclusive()) {
		scanner.scanRowUnchecked(txn, *this, NULL, begin, end);
	}
	else {
		RowArray &rowArray = scanner.getRowArray();
		util::XArray<OId> oIdList(txn.getDefaultAllocator());

		for (const OId *it = begin; it != end; ++it) {
			rowArray.load(txn, *it, this, OBJECT_READ_ONLY);
			if (onMvcc) {
				scanRowCheckedDirect<true>(txn, rowArray, oIdList);
			}
			else {
				scanRowCheckedDirect<false>(txn, rowArray, oIdList);
			}

			if (!oIdList.empty() && !scanner.scanRowUnchecked(
					txn, *this, &rowArray,
					&oIdList.front(), &oIdList.back() + 1)) {
				break;
			}
			oIdList.clear();
		}
	}
}

/*!
	@brief Get max RowId
*/
RowId Collection::getMaxRowId(TransactionContext& txn) {
	TermCondition cond(getRowIdColumnType(), getRowIdColumnType(), 
		DSExpression::LE, getRowIdColumnId(), &MAX_ROWID,
		sizeof(MAX_ROWID));
	BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, 1);
	util::XArray<OId> oIdList(txn.getDefaultAllocator());

	searchRowIdIndex(txn, sc, oIdList, ORDER_DESCENDING);
	if (oIdList.empty()) {
		return UNDEF_ROWID;
	}
	else {
		RowArray rowArray(txn, this);
		rowArray.load(txn, oIdList[0], this, OBJECT_READ_ONLY);
		RowArray::Row row(rowArray.getRow(), &rowArray);
		return row.getRowId();
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

		if (isExpired(txn)) {
			dataStore_->setLastExpiredTime(txn.getPartitionId(), txn.getStatementStartTime().getUnixTime());
			return;
		}

		if (isAlterContainer()) {
			DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_COL_LOCK_CONFLICT,
				"(pId=" << txn.getPartitionId() << ", containerId=" << getContainerId()
						<< ", txnId=" << txn.getId() << ")");
		}

		RowArray rowArray(txn, this);
		for (size_t i = 0; i < rowIdList.size(); i++) {
			util::StackAllocator::Scope scope(txn.getDefaultAllocator());
			TermCondition cond(getRowIdColumnType(), getRowIdColumnType(), 
				DSExpression::EQ, getRowIdColumnId(), &rowIdList[i], sizeof(RowId));
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
					"rowId" << rowIdList[i] << "is not found,  ");
			}
		}
	}
	catch (std::exception& e) {
		handleSearchError(txn, e, GS_ERROR_DS_COL_GET_LOCK_ID_INVALID);
	}
}


void Collection::putRow(TransactionContext& txn,
	InputMessageRowStore* inputMessageRowStore, RowId& rowId,
	bool rowIdSpecified,
	DataStore::PutStatus& status, PutRowOption putRowOption) {
	util::StackAllocator::Scope scope(txn.getDefaultAllocator());
	PutMode mode = UNDEFINED_STATUS;

	util::Vector<ColumnId> keyColumnIdList(txn.getDefaultAllocator());
	getKeyColumnIdList(keyColumnIdList);
	util::XArray<KeyData> keyFieldList(txn.getDefaultAllocator());
	getFields(txn, inputMessageRowStore, keyColumnIdList, keyFieldList);

	RowArray rowArray(txn, this);
	OId targetOId = UNDEF_OID;
	if (!definedRowKey() || isUnique(txn, keyColumnIdList, keyFieldList, targetOId)) {
		OId tailOId = UNDEF_OID;
		{
			StackAllocAutoPtr<BtreeMap> rowIdMap(
				txn.getDefaultAllocator(), getRowIdMap(txn));
			tailOId = rowIdMap.get()->getTail(txn);
		}
		if (tailOId != UNDEF_OID) {
			bool isOldSchema = rowArray.load(txn, tailOId, this, OBJECT_FOR_UPDATE);
			assert(isOldSchema || !isOldSchema);
			rowArray.tail();
			if (isOldSchema) {
				RowScope rowScope(*this, TO_NORMAL);
				convertRowArraySchema(txn, rowArray, false);
			}
		}
		mode = APPEND;
	}
	else {
		bool isOldSchema = rowArray.load(txn, targetOId, this, OBJECT_FOR_UPDATE);
		if (isOldSchema) {
			RowScope rowScope(*this, TO_NORMAL);
			convertRowArraySchema(txn, rowArray, false);
		}
		mode = UPDATE;
	}
	switch (mode) {
	case APPEND:
		if (putRowOption == PUT_UPDATE_ONLY) {
			GS_THROW_USER_ERROR(GS_ERROR_CON_PUT_ROW_OPTION_INVALID,
				"update row, but row not exists");
		}
		appendRowInternal(txn, inputMessageRowStore, rowArray, rowId, rowIdSpecified);
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

bool Collection::isUnique(TransactionContext& txn,
	util::Vector<ColumnId> &keyColumnIdList,
	util::XArray<KeyData> &keyFieldList, OId &oId) {
	bool isFound = false;

	oId = UNDEF_OID;
	bool withPartialMatch = false;
	IndexTypes indexBit = getIndexTypes(txn, keyColumnIdList, withPartialMatch);
	if (hasIndex(indexBit, MAP_TYPE_HASH)) {
		assert(keyColumnIdList.size() == 1);
		assert(keyFieldList.size() == 1);

		IndexData indexData(txn.getDefaultAllocator());
		getIndexData(txn, keyColumnIdList, MAP_TYPE_HASH, false, indexData, withPartialMatch);
		ValueMap valueMap(txn, this, indexData);
		const void *indexValue = getIndexValue(txn, *(indexData.columnIds_),
			valueMap.getFuncInfo(txn), keyFieldList);
		static_cast<HashMap *>(valueMap.getValueMap(txn, false))
			->search(txn, indexValue, oId);
		isFound = (oId != UNDEF_OID);
	}
	else if (hasIndex(indexBit, MAP_TYPE_BTREE)) {
		assert(keyColumnIdList.size() >= 1);
		assert(keyFieldList.size() >= 1);

		IndexData indexData(txn.getDefaultAllocator());
		getIndexData(txn, keyColumnIdList, MAP_TYPE_BTREE, false, indexData, withPartialMatch);
		ValueMap valueMap(txn, this, indexData);
		BtreeMap *map = static_cast<BtreeMap *>(valueMap.getValueMap(txn, false));
		const void *indexValue = getIndexValue(txn, *(indexData.columnIds_),
			map->getFuncInfo(), keyFieldList);
		map->search(txn, indexValue, oId);
		isFound = (oId != UNDEF_OID);
	}
	else {
		isFound = searchRowKeyWithRowIdMap(txn, keyFieldList, keyColumnIdList, oId);
	}

	if (!isFound && !isExclusive()) {
		isFound = searchRowKeyWithMvccMap(txn, keyFieldList, keyColumnIdList, oId);
	}

	return !isFound;
}

void Collection::appendRowInternal(TransactionContext& txn,
	MessageRowStore* messageRowStore, RowArray& rowArray, RowId& rowId,
	bool rowIdSpecified) {
	if (rowIdSpecified) {
		setMaxRowId(rowId);
	} else {
		rowId = allocateRowId();
	}

	if (rowArray.isNotInitialized() || rowArray.isFull()) {
		StackAllocAutoPtr<BtreeMap> rowIdMap(
			txn.getDefaultAllocator(), getRowIdMap(txn));
		rowArray.reset();
		if (getRowNum() == 0) {
			bool isRowArraySizeControlMode = false;
			uint32_t smallRowArrayNum =
				calcRowArrayNum(txn, isRowArraySizeControlMode, getSmallRowArrayNum());
			rowArray.initialize(txn, rowId, smallRowArrayNum);
		}
		else {
			rowArray.initialize(txn, rowId, getNormalRowArrayNum());
		}
		insertRowIdMap(txn, rowIdMap.get(), &rowId, rowArray.getBaseOId());
	}
	else {
		RowScope rowScope(*this, TO_NORMAL);
		if (rowArray.isTailPos()) {
			shift(txn, rowArray, true);
			rowArray.tail();
		}
	}
	{
		RowScope rowScope(*this, TO_NORMAL);

		rowArray.append(txn, messageRowStore, rowId);
	
		RowArray::Row row(rowArray.getRow(), &rowArray);
	
		row.lock(txn);
		row.resetFirstUpdate();
	
		util::XArray<IndexData> indexList(txn.getDefaultAllocator());
		bool withUncommitted = true;
		getIndexList(txn, withUncommitted, indexList);
		for (size_t i = 0; i < indexList.size(); i++) {
			if (indexList[i].cursor_ < row.getRowId()) {
				continue;
			}
			ValueMap valueMap(txn, this, indexList[i]);
			bool isNullValue;
			const void *fieldValue = row.getFields<Collection>(txn, 
				valueMap.getFuncInfo(txn), isNullValue);
			insertValueMap(txn, valueMap, fieldValue, rowArray.getOId(),
				isNullValue);
		}

		if (!txn.isAutoCommit()) {  
			setCreateRowId(txn, rowId);
		}
	}
}

void Collection::updateRowInternal(TransactionContext& txn,
	MessageRowStore* messageRowStore, RowArray& rowArray, RowId& rowId) {
	RowArray::Row row(rowArray.getRow(), &rowArray);
	rowId = row.getRowId();
	row.lock(txn);

	if (!txn.isAutoCommit() && row.isFirstUpdate()) {
		RowScope rowScope(*this, TO_MVCC);
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
			row.getFields<Collection>(txn, 
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
}

void Collection::deleteRowInternal(
	TransactionContext& txn, RowArray& rowArray, RowId& rowId) {
	RowArray::Row row(rowArray.getRow(), &rowArray);
	rowId = row.getRowId();
	row.lock(txn);

	if (!txn.isAutoCommit() && row.isFirstUpdate()) {
		RowScope rowScope(*this, TO_MVCC);
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
	{
		RowScope rowScope(*this, TO_NORMAL);

		util::XArray<IndexData> indexList(txn.getDefaultAllocator());
		bool withUncommitted = true;
		getIndexList(txn, withUncommitted, indexList);
		if (!indexList.empty()) {
			for (size_t i = 0; i < indexList.size(); i++) {
				if (indexList[i].cursor_ < row.getRowId()) {
					continue;
				}
				ValueMap valueMap(txn, this, indexList[i]);
				bool isNullValue;
				const void *fieldValue = row.getFields<Collection>(txn, 
					valueMap.getFuncInfo(txn), isNullValue);
				removeValueMap(txn, valueMap, fieldValue, rowArray.getOId(),
					isNullValue);
		}
		}

		rowArray.remove(txn);

		uint16_t halfRowNum = rowArray.getMaxRowNum() / 2;
		uint16_t activeNum = rowArray.getActiveRowNum(halfRowNum);
		if (activeNum == 0) {
			RowId rowId = rowArray.getRowId();
			StackAllocAutoPtr<BtreeMap> rowIdMap(
				txn.getDefaultAllocator(), getRowIdMap(txn));
			removeRowIdMap(txn, rowIdMap.get(), &rowId, rowArray.getBaseOId());
			rowArray.finalize(txn);
		}
		else if (activeNum < halfRowNum) {
			RowArray neighborRowArray(txn, this);
			bool isOldSchema = false;
			bool isExist =
				rowArray.nextRowArray(txn, neighborRowArray, isOldSchema, OBJECT_FOR_UPDATE);
			if (isOldSchema) {
				neighborRowArray.begin();
				convertRowArraySchema(txn, neighborRowArray, false); 
			}
			if (isExist &&
				activeNum + neighborRowArray.getActiveRowNum(rowArray.getMaxRowNum() - activeNum + 1) <=
					rowArray.getMaxRowNum()) {
				merge(txn, rowArray, neighborRowArray);
			}
			else {
				isExist =
					rowArray.prevRowArray(txn, neighborRowArray, isOldSchema, OBJECT_FOR_UPDATE);
				if (isOldSchema) {
					neighborRowArray.tail();
					convertRowArraySchema(txn, neighborRowArray, false); 
				}
				if (isExist &&
					activeNum + neighborRowArray.getActiveRowNum() <=
						neighborRowArray.getMaxRowNum()) {
					merge(txn, neighborRowArray, rowArray);
				}
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
		RowScope rowScope(*this, TO_NORMAL);
		if (destRowArray.isFull()) {
			RowId splitRowId;
			RowArray splitRowArray(txn, this);
			split(txn, destRowArray, insertRowId, splitRowArray, splitRowId);
			if (splitRowId < srcRow.getRowId()) {
				bool isOldSchema = destRowArray.load(
					txn, splitRowArray.getOId(), this, OBJECT_FOR_UPDATE);
				UNUSED_VARIABLE(isOldSchema);
				assert(!isOldSchema);
			}
		}
		else {
			shift(txn, destRowArray, false);
		}
	}
	{
		RowScope rowScope(*this, TO_NORMAL);
		srcRowArray.move(txn, destRowArray);
		RowArray::Row row(destRowArray.getRow(), &destRowArray);

		util::XArray<IndexData> indexList(txn.getDefaultAllocator());
		bool withUncommitted = true;
		getIndexList(txn, withUncommitted, indexList);
		for (size_t i = 0; i < indexList.size(); i++) {
			if (indexList[i].cursor_ < row.getRowId()) {
				continue;
			}
			ValueMap valueMap(txn, this, indexList[i]);
			bool isNullValue;
			const void *fieldValue = row.getFields<Collection>(txn, 
				valueMap.getFuncInfo(txn), isNullValue);
			insertValueMap(txn, valueMap, fieldValue, destRowArray.getOId(),
				isNullValue);

		}
	}
}

void Collection::shift(
	TransactionContext& txn, RowArray& rowArray, bool isForce) {
	util::XArray<std::pair<OId, OId> > moveList(txn.getDefaultAllocator());
	util::XArray<std::pair<OId, OId> >::iterator itr;
	rowArray.shift(txn, isForce, moveList);
	updateValueMaps(txn, moveList);
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

	updateValueMaps(txn, moveList);
}

void Collection::merge(
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

bool Collection::searchRowKeyWithRowIdMap(TransactionContext& txn,
	util::XArray<KeyData> &keyFieldList, util::Vector<ColumnId> &keyColumnIdList,
	OId& oId) {
	oId = UNDEF_OID;

	BtreeMap::SearchContext sc(txn.getDefaultAllocator(), keyColumnIdList);
	sc.setLimit(1);
	for (size_t i = 0; i < keyColumnIdList.size(); i++) {
		ColumnInfo &columnInfo = getColumnInfo(keyColumnIdList[i]);
		TermCondition cond(columnInfo.getColumnType(), columnInfo.getColumnType(), 
			DSExpression::EQ, i, keyFieldList[i].data_, keyFieldList[i].size_);
		sc.addCondition(cond, true);
	}

	util::Vector<TermCondition> condList(txn.getDefaultAllocator());
	sc.getConditionList(condList, BaseIndex::SearchContext::COND_ALL);
	util::Vector<TermCondition>::iterator condItr;
	ContainerValue containerValue(txn.getPartitionId(), *getObjectManager());

	BtreeMap::BtreeCursor btreeCursor;

	RowArray rowArray(txn, this);
	while (1) {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		util::XArray<OId> idList(txn.getDefaultAllocator());  
		util::XArray<OId>::iterator itr;
		StackAllocAutoPtr<BtreeMap> rowIdMap(
			txn.getDefaultAllocator(), getRowIdMap(txn));
		int32_t getAllStatus = rowIdMap.get()->getAll(
			txn, PARTIAL_RESULT_SIZE, idList, btreeCursor);

		for (itr = idList.begin(); itr != idList.end(); itr++) {
			rowArray.load(txn, *itr, this, OBJECT_READ_ONLY);
			for (rowArray.begin(); !rowArray.end(); rowArray.next()) {
				RowArray::Row row(rowArray.getRow(), &rowArray);
				bool isMatch = true;
				for (condItr = condList.begin(); condItr != condList.end(); condItr++) {
					if (!row.isMatch(txn, *condItr, containerValue)) {
						isMatch = false;
						break;
					}
				}
				if (isMatch) {
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
	BtreeMap::SearchContext &sc, OId& oId) {
	oId = UNDEF_OID;

	StackAllocAutoPtr<BtreeMap> mvccMap(
		txn.getDefaultAllocator(), getMvccMap(txn));
	if (mvccMap.get()->isEmpty()) {
		return false;
	}

	util::Vector<TermCondition> condList(txn.getDefaultAllocator());
	sc.getConditionList(condList, BaseIndex::SearchContext::COND_ALL);
	util::Vector<TermCondition>::iterator condItr;
	ContainerValue containerValue(txn.getPartitionId(), *getObjectManager());

	RowArray rowArray(txn, this);
	util::XArray<std::pair<TransactionId, MvccRowImage> > idList(
		txn.getDefaultAllocator());
	util::XArray<std::pair<TransactionId, MvccRowImage> >::iterator itr;
	mvccMap.get()->getAll<TransactionId, MvccRowImage>(
		txn, MAX_RESULT_SIZE, idList);
	for (itr = idList.begin(); itr != idList.end(); itr++) {
		switch (itr->second.type_) {
		case MVCC_SELECT:
		case MVCC_INDEX:
		case MVCC_CONTAINER:
		case MVCC_CREATE:
			break;
		case MVCC_UPDATE:
		case MVCC_DELETE:
			{
				rowArray.load(
					txn, itr->second.snapshotRowOId_, this, OBJECT_READ_ONLY);
				RowArray::Row row(rowArray.getRow(), &rowArray);
				if (row.getTxnId() != txn.getId()) {
					bool isMatch = true;
					for (condItr = condList.begin(); condItr != condList.end(); condItr++) {
						if (!row.isMatch(txn, *condItr, containerValue)) {
							isMatch = false;
							break;
						}
					}
					if (isMatch) {
						oId = itr->second.snapshotRowOId_;
						return true;
					}
				}
			}
			break;
		default:
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
			break;
		}
	}

	return false;
}

bool Collection::searchRowKeyWithMvccMap(TransactionContext& txn,
	util::XArray<KeyData> &keyFieldList, util::Vector<ColumnId> &keyColumnIdList,
	OId& oId) {
	oId = UNDEF_OID;

	StackAllocAutoPtr<BtreeMap> mvccMap(
		txn.getDefaultAllocator(), getMvccMap(txn));
	if (mvccMap.get()->isEmpty()) {
		return false;
	}

	BtreeMap::SearchContext sc(txn.getDefaultAllocator(), keyColumnIdList);
	sc.setLimit(1);
	for (size_t i = 0; i < keyColumnIdList.size(); i++) {
		ColumnInfo &columnInfo = getColumnInfo(i);
		TermCondition cond(columnInfo.getColumnType(), columnInfo.getColumnType(), 
			DSExpression::EQ, keyColumnIdList[i], keyFieldList[i].data_, keyFieldList[i].size_);
		sc.addCondition(cond, true);
	}

	util::Vector<TermCondition> condList(txn.getDefaultAllocator());
	sc.getConditionList(condList, BaseIndex::SearchContext::COND_ALL);
	util::Vector<TermCondition>::iterator condItr;
	ContainerValue containerValue(txn.getPartitionId(), *getObjectManager());

	RowArray rowArray(txn, this);
	util::XArray<std::pair<TransactionId, MvccRowImage> > idList(
		txn.getDefaultAllocator());
	util::XArray<std::pair<TransactionId, MvccRowImage> >::iterator itr;
	mvccMap.get()->getAll<TransactionId, MvccRowImage>(
		txn, MAX_RESULT_SIZE, idList);
	for (itr = idList.begin(); itr != idList.end(); itr++) {
		switch (itr->second.type_) {
		case MVCC_SELECT:
		case MVCC_INDEX:
		case MVCC_CONTAINER:
		case MVCC_CREATE:
			break;
		case MVCC_UPDATE:
		case MVCC_DELETE:
			{
				rowArray.load(
					txn, itr->second.snapshotRowOId_, this, OBJECT_READ_ONLY);
				RowArray::Row row(rowArray.getRow(), &rowArray);
				if (row.getTxnId() != txn.getId()) {
					bool isMatch = true;
					for (condItr = condList.begin(); condItr != condList.end(); condItr++) {
						if (!row.isMatch(txn, *condItr, containerValue)) {
							isMatch = false;
							break;
						}
					}
					if (isMatch) {
						oId = itr->second.snapshotRowOId_;
						return true;
					}
				}
			}
			break;
		default:
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
			break;
		}
	}

	return false;
}


void Collection::undoCreateRow(TransactionContext& txn, RowArray& rowArray) {
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
			const void *fieldValue = row.getFields<Collection>(txn, 
				valueMap.getFuncInfo(txn), isNullValue);
			removeValueMap(txn, valueMap, fieldValue, rowArray.getOId(),
				isNullValue);
		}
	}
	rowArray.remove(txn);
	decrementRowNum();
}

void Collection::undoUpdateRow(
	TransactionContext& txn, RowArray& beforeRowArray) {
	AbortStatus status = ABORT_UNDEF_STATUS;

	RowArray::Row beforeRow(beforeRowArray.getRow(), &beforeRowArray);
	RowArray rowArray(txn, this);
	RowId rowId = beforeRow.getRowId();
	util::XArray<OId> oIdList(txn.getDefaultAllocator());

	{
		TermCondition cond(getRowIdColumnType(), getRowIdColumnType(),
			DSExpression::LE, getRowIdColumnId(), &rowId,
			sizeof(rowId));
		BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, 1);
		{
			StackAllocAutoPtr<BtreeMap> rowIdMap(
				txn.getDefaultAllocator(), getRowIdMap(txn));
			rowIdMap.get()->search(txn, sc, oIdList, ORDER_DESCENDING);
		}
		if (!oIdList.empty()) {
			bool isOldSchema = rowArray.load(txn, oIdList[0], this, OBJECT_FOR_UPDATE);
			bool isFound = rowArray.searchNextRowId(rowId);
			if (isFound) {
				status = ABORT_UPDATE_ROW_ARRAY;
			}
			else {
				status = ABORT_INSERT_ROW_ARRAY;
			}
			if (isOldSchema) {
				RowScope rowScope(*this, TO_NORMAL);
				convertRowArraySchema(txn, rowArray, false); 
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
		RowScope rowScope(*this, TO_NORMAL);
		util::XArray<IndexData> indexList(txn.getDefaultAllocator());
		bool withUncommitted = true;
		getIndexList(txn, withUncommitted, indexList);
		RowArray::Row row(rowArray.getRow(), &rowArray);
		if (!row.isRemoved()) {
			for (size_t i = 0; i < indexList.size(); i++) {
				if (indexList[i].cursor_ < row.getRowId()) {
					continue;
				}
				ValueMap valueMap(txn, this, indexList[i]);
				bool isNullValue;
				const void *fieldValue = row.getFields<Collection>(txn, 
					valueMap.getFuncInfo(txn), isNullValue);
				removeValueMap(txn, valueMap, fieldValue, rowArray.getOId(),
					isNullValue);
			}
			row.finalize(txn);
		}
		beforeRowArray.move(txn, rowArray);

		for (size_t i = 0; i < indexList.size(); i++) {
			if (indexList[i].cursor_ < row.getRowId()) {
				continue;
			}
			ValueMap valueMap(txn, this, indexList[i]);
			bool isNullValue;
			const void *fieldValue = row.getFields<Collection>(txn, 
				valueMap.getFuncInfo(txn), isNullValue);
			insertValueMap(txn, valueMap, fieldValue, rowArray.getOId(),
				isNullValue);
		}
	} break;
	case ABORT_CREATE_FIRST_ROW_ARRAY: {
		RowScope rowScope(*this, TO_NORMAL);
		RowId baseRowId = 0;
		rowArray.initialize(txn, baseRowId, getNormalRowArrayNum());
		StackAllocAutoPtr<BtreeMap> rowIdMap(
			txn.getDefaultAllocator(), getRowIdMap(txn));
		insertRowIdMap(txn, rowIdMap.get(), &baseRowId, rowArray.getBaseOId());
		beforeRowArray.move(txn, rowArray);

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
			const void *fieldValue = row.getFields<Collection>(txn, 
				valueMap.getFuncInfo(txn), isNullValue);
			insertValueMap(txn, valueMap, fieldValue, rowArray.getOId(),
				isNullValue);
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

void Collection::getContainerOptionInfo(
	TransactionContext&, util::XArray<uint8_t>&) {
	return;
}

util::String Collection::getBibContainerOptionInfo(TransactionContext &) {
	return "";
}

void Collection::checkContainerOption(
	MessageSchema*, util::XArray<uint32_t>&, bool&) {
	return;
}

void Collection::checkExclusive(TransactionContext& txn) {
	if (isExclusiveUpdate()) {  
		return;
	}
	setExclusiveStatus(NO_ROW_TRANSACTION);

	StackAllocAutoPtr<BtreeMap> mvccMap(
		txn.getDefaultAllocator(), getMvccMap(txn));
	if (mvccMap.get()->isEmpty()) {
		return;
	}

	util::XArray<std::pair<TransactionId, MvccRowImage> > idList(
		txn.getDefaultAllocator());
	util::XArray<std::pair<TransactionId, MvccRowImage> >::iterator itr;
	mvccMap.get()->getAll<TransactionId, MvccRowImage>(
		txn, MAX_RESULT_SIZE, idList);
	for (itr = idList.begin(); itr != idList.end(); itr++) {
		switch (itr->second.type_) {
		case MVCC_SELECT:
			{
				if (isNoRowTransaction()) {
					setExclusiveStatus(EXCLUSIVE);
				}
			}
		case MVCC_INDEX:
		case MVCC_CONTAINER:
			break;
		case MVCC_CREATE:
			{
				if (isExclusive() && txn.getId() != itr->first) {
					setExclusiveStatus(NOT_EXCLUSIVE_CREATE_EXIST);
				}
				else if (isNoRowTransaction()) {
					setExclusiveStatus(EXCLUSIVE);
				} 
			}
			break;
		case MVCC_UPDATE:
		case MVCC_DELETE:
			{
				if (isNoRowTransaction()) {
					setExclusiveStatus(EXCLUSIVE);
				} 
				if (txn.getId() != itr->first) {
					setExclusiveStatus(NOT_EXCLUSIVE);
					break;
				}
			}
			break;
		default:
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
			break;
		}
	}
}

bool Collection::getIndexData(TransactionContext &txn, const util::Vector<ColumnId> &columnIds,
	MapType mapType, bool withUncommitted, IndexData &indexData, bool withPartialMatch) const {
	bool isExist = indexSchema_->getIndexData(
		txn, columnIds, mapType, UNDEF_CONTAINER_POS, withUncommitted, 
		withPartialMatch, indexData);
	return isExist;
}

void Collection::getIndexList(
		TransactionContext &txn, bool withUncommitted, 
		util::XArray<IndexData> &list) const
{
	indexSchema_->getIndexList(txn, UNDEF_CONTAINER_POS, withUncommitted, list);
}

template<bool Mvcc>
void Collection::scanRowArrayCheckedDirect(
		TransactionContext& txn, RowArray &rowArray,
		RowId startRowId, RowId endRowId, util::XArray<OId> &oIdList) {

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

template<bool Mvcc>
void Collection::scanRowCheckedDirect(
		TransactionContext& txn, RowArray &rowArray,
		util::XArray<OId> &oIdList) {
	RowArray::Row row(rowArray.getRow(), &rowArray);

	if (filterActiveTransaction<Mvcc>(txn, row)) {
		return;
	}

	oIdList.push_back(rowArray.getOId());
}

template<>
bool Collection::filterActiveTransaction<true>(
		TransactionContext &txn, RowArray::Row &row) {
	return txn.getId() == row.getTxnId();
}

template<>
bool Collection::filterActiveTransaction<false>(
		TransactionContext &txn, RowArray::Row &row) {
	return txn.getId() != row.getTxnId() &&
			!row.isFirstUpdate() &&
			txn.getManager().isActiveTransaction(
					txn.getPartitionId(), row.getTxnId());
}

void Collection::scanRowIdIndexPrepare(
		TransactionContext &txn, BtreeMap::SearchContext &sc,
		util::XArray<OId> *oIdList) {
	const OutputOrder order = ORDER_UNDEFINED;

	resolveExclusiveStatus<Collection>(txn);

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

void Collection::searchMvccMapPrepare(
		TransactionContext& txn, const BtreeMap::SearchContext &sc,
		BtreeMap::SearchContext &mvccSC,
		RowId startRowId, RowId lastCheckRowId,
		std::pair<RowId, RowId> *mvccRowIdRange) {

	RowId *mvccStartRowId = &mvccRowIdRange->first;
	RowId *mvccEndRowId = &mvccRowIdRange->second;

	*mvccStartRowId = startRowId;

	if (sc.isSuspended() && lastCheckRowId != UNDEF_ROWID) {
		*mvccEndRowId = lastCheckRowId + 1;
	}
	else {
		*mvccEndRowId = UNDEF_ROWID;
	}

	const RowId minRowId = std::numeric_limits<RowId>::min();

	if (*mvccStartRowId != minRowId) {
		TermCondition newCond(COLUMN_TYPE_LONG, COLUMN_TYPE_LONG, 
			DSExpression::GE, UNDEF_COLUMNID, &mvccStartRowId, sizeof(mvccStartRowId));
		mvccSC.updateCondition(txn, newCond);
	}

	if (*mvccEndRowId != UNDEF_ROWID) {
		TermCondition newCond(COLUMN_TYPE_LONG, COLUMN_TYPE_LONG, 
			DSExpression::LT, UNDEF_COLUMNID, &mvccEndRowId, sizeof(mvccEndRowId));
		mvccSC.updateCondition(txn, newCond);
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
	bool isCaseSensitive = true;
	int32_t status =
		map->insert<TransactionId, MvccRowImage>(txn, tId, mvccImage, isCaseSensitive);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		setDirty();
		baseContainerImage_->mvccMapOId_ = map->getBaseOId();
	}
}

void Collection::insertValueMap(TransactionContext& txn, ValueMap &valueMap,
	const void* constKey, OId oId, bool isNullVal) {
	int32_t status = valueMap.putValueMap(txn, isNullVal)->insert(txn, constKey, oId);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		valueMap.updateIndexData(txn);
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
	bool isCaseSensitive = true;
	int32_t status = map->update<TransactionId, MvccRowImage>(
		txn, tId, oldMvccImage, newMvccImage, isCaseSensitive);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		setDirty();
		baseContainerImage_->mvccMapOId_ = map->getBaseOId();
	}
}

void Collection::updateValueMap(TransactionContext& txn, ValueMap &valueMap,
	const void* constKey, OId oldOId, OId newOId, bool isNullVal) {
	int32_t status = valueMap.putValueMap(txn, isNullVal)->update(txn, constKey, oldOId, newOId);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		valueMap.updateIndexData(txn);
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
	bool isCaseSensitive = true;
	int32_t status =
		map->remove<TransactionId, MvccRowImage>(txn, tId, mvccImage, isCaseSensitive);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		setDirty();
		baseContainerImage_->mvccMapOId_ = map->getBaseOId();
	}
}

void Collection::removeValueMap(TransactionContext& txn, ValueMap &valueMap,
	const void* constKey, OId oId, bool isNullVal) {
	int32_t status = valueMap.putValueMap(txn, isNullVal)->remove(txn, constKey, oId);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		valueMap.updateIndexData(txn);
	}
}

void Collection::updateIndexData(TransactionContext& txn, const IndexData &indexData) {
	indexSchema_->updateIndexData(txn, indexData, UNDEF_CONTAINER_POS);
}


util::String Collection::getBibInfo(TransactionContext &txn, const char* dbName) {
	return getBibInfoImpl(txn, dbName, 0, false);
}

void Collection::getActiveTxnList(
	TransactionContext &txn, util::Set<TransactionId> &txnList) {
	getActiveTxnListImpl(txn, txnList);
}

void Collection::getErasableList(TransactionContext &txn, Timestamp erasableTimeUpperLimit, util::XArray<ArchiveInfo> &list) {
	Timestamp erasableTimeLowerLimit = getDataStore()->getLatestBatchFreeTime(txn);
	if (erasableTimeLowerLimit < dataStore_->getConfig().getErasableExpiredTime()) {
		erasableTimeLowerLimit = dataStore_->getConfig().getErasableExpiredTime();
	}
	ExpireType type = getExpireType();
	if (type == TABLE_EXPIRE && baseContainerImage_->rowIdMapOId_ != UNDEF_OID
		&& baseContainerImage_->mvccMapOId_ != UNDEF_OID) {
		ArchiveInfo info;
		info.rowIdMapOId_ = baseContainerImage_->rowIdMapOId_;
		info.mvccMapOId_ = baseContainerImage_->mvccMapOId_;
		info.start_ = getContainerExpirationStartTime();
		info.end_ = getContainerExpirationEndTime();
		info.expired_ = getContainerExpirationTime();
		info.erasable_ = DataStore::convertChunkKey2Timestamp(getRowAllcateStrategy().chunkKey_);
		if (erasableTimeLowerLimit >= info.erasable_ || info.erasable_ > erasableTimeUpperLimit) {
			return;
		}
		list.push_back(info);
	}
}

ExpireType Collection::getExpireType() const {
	if (getContainerExpirationDutation() != INT64_MAX) {
		return TABLE_EXPIRE;
	}
	return NO_EXPIRE;
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



