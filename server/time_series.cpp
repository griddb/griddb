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
	UNUSED_VARIABLE(txn);
	baseContainerImage_ =
		BaseObject::allocate<TimeSeriesImage>(sizeof(TimeSeriesImage),
			getMetaAllcateStrategy(), getBaseOId(), OBJECT_TYPE_TIME_SERIES);
	memset(baseContainerImage_, 0, sizeof(TimeSeriesImage));
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
	baseContainerImage_->tablePartitioningVersionId_ = UNDEF_TABLE_PARTITIONING_VERSIONID;
	baseContainerImage_->startTime_ = 0;
	reinterpret_cast<TimeSeriesImage *>(baseContainerImage_)
		->hiCompressionStatus_ = UNDEF_OID;
	reinterpret_cast<TimeSeriesImage *>(baseContainerImage_)->padding1_ = 0;
	reinterpret_cast<TimeSeriesImage *>(baseContainerImage_)->padding2_ = 0;

	rowImageSize_ = 0;
	metaAllocateStrategy_ = AllocateStrategy();
	rowAllocateStrategy_ = AllocateStrategy();
	mapAllocateStrategy_ = AllocateStrategy();
}

/*!
	@brief Set TimeSeries Schema
*/
void TimeSeries::set(TransactionContext &txn, const FullContainerKey &containerKey,
	ContainerId containerId, OId columnSchemaOId,
	MessageSchema *orgContainerSchema) {
	MessageTimeSeriesSchema *containerSchema =
		reinterpret_cast<MessageTimeSeriesSchema *>(orgContainerSchema);
	baseContainerImage_->containerId_ = containerId;
	setTablePartitioningVersionId(containerSchema->getTablePartitioningVersionId());
	setContainerExpirationStartTime(orgContainerSchema->getContainerExpirationStartTime());

	FullContainerKeyCursor keyCursor(txn.getPartitionId(), *getObjectManager());
	keyCursor.initialize(txn, containerKey, getMetaAllcateStrategy());
	baseContainerImage_->containerNameOId_ = keyCursor.getBaseOId();

	baseContainerImage_->columnSchemaOId_ = columnSchemaOId;
	commonContainerSchema_ =
		ALLOC_NEW(txn.getDefaultAllocator()) ShareValueList(
			txn, *getObjectManager(), baseContainerImage_->columnSchemaOId_);
	columnSchema_ =
		commonContainerSchema_->get<ColumnSchema>(META_TYPE_COLUMN_SCHEMA);

	const CompressionSchema &compressionSchema = getCompressionSchema();
	switch (compressionSchema.getCompressionType()) {
	case HI_COMPRESSION: {
		uint16_t hiCompressionColumnNum =
			compressionSchema.getHiCompressionNum();
		if (hiCompressionColumnNum > 0) {
			DSDCValList dsdcValList(txn, *getObjectManager());
			dsdcValList.initialize(txn, compressionSchema,
				hiCompressionColumnNum, getMetaAllcateStrategy());
			reinterpret_cast<TimeSeriesImage *>(baseContainerImage_)
				->hiCompressionStatus_ = dsdcValList.getBaseOId();
		}
	} break;
	case SS_COMPRESSION: {
		setSsCompressionReady(SS_IS_NOT_READRY);
	} break;
	default:
		break;
	}

	indexSchema_ = ALLOC_NEW(txn.getDefaultAllocator())
		IndexSchema(txn, *getObjectManager(), getMetaAllcateStrategy());
	bool onMemory = false;
	indexSchema_->initialize(txn, IndexSchema::INITIALIZE_RESERVE_NUM, 0, getColumnNum(), onMemory);
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

	baseContainerImage_->normalRowArrayNum_ = calcRowArrayNum(
		txn, getDataStore()->getConfig().isRowArraySizeControlMode(),
		ROW_ARRAY_MAX_SIZE);

	setAllocateStrategy();

	if (isExpired(txn)) {
		dataStore_->setLastExpiredTime(txn.getPartitionId(), txn.getStatementStartTime().getUnixTime());
	}

	util::StackAllocator &alloc = txn.getDefaultAllocator();
	util::Vector<ColumnId> columnIds(alloc);
	columnIds.push_back(ColumnInfo::ROW_KEY_COLUMN_ID);
	rowIdFuncInfo_ = ALLOC_NEW(alloc) TreeFuncInfo(alloc);
	rowIdFuncInfo_->initialize(columnIds, NULL);

	columnIds[0] = UNDEF_COLUMNID;
	mvccFuncInfo_ = ALLOC_NEW(alloc) TreeFuncInfo(alloc);
	mvccFuncInfo_->initialize(columnIds, NULL);

	BtreeMap mvccMap(txn, *getObjectManager(), getMapAllcateStrategy(), this, mvccFuncInfo_);
	mvccMap.initialize<TransactionId, MvccRowImage>(
		txn, COLUMN_TYPE_OID, false, BtreeMap::TYPE_SINGLE_KEY);
	baseContainerImage_->mvccMapOId_ = mvccMap.getBaseOId();

	setNullsStatsStatus();

	rowArrayCache_ = ALLOC_NEW(txn.getDefaultAllocator())
		RowArray(txn, this);
}

/*!
	@brief Free Objects related to TimeSeries
*/
bool TimeSeries::finalize(TransactionContext& txn) {
	try {
		setDirty();
		util::XArray<SubTimeSeriesInfo> subTimeSeriesList(
			txn.getDefaultAllocator());
		util::XArray<SubTimeSeriesInfo>::reverse_iterator rItr;
		getSubTimeSeriesList(txn, subTimeSeriesList, true);
		for (rItr = subTimeSeriesList.rbegin();
			 rItr != subTimeSeriesList.rend(); rItr++) {
			util::StackAllocator::Scope scope(txn.getDefaultAllocator());
			SubTimeSeries subContainer(txn, *rItr, this);
			bool isFinished = subContainer.finalize(txn);
			if (!isFinished) {
				return isFinished;
			}
			subTimeSeriesListHead_->remove(txn, rItr->pos_);
		}

		if (baseContainerImage_->subContainerListOId_ != UNDEF_OID) {
			subTimeSeriesListHead_->finalize(txn);
		}

		const CompressionSchema &compressionSchema = getCompressionSchema();
		if (compressionSchema.getCompressionType() == HI_COMPRESSION) {
			uint16_t hiCompressionColumnNum =
				compressionSchema.getHiCompressionNum();
			if (hiCompressionColumnNum > 0) {
				getObjectManager()->free(txn.getPartitionId(),
					reinterpret_cast<TimeSeriesImage *>(baseContainerImage_)
						->hiCompressionStatus_);
			}
		}

		if (baseContainerImage_->mvccMapOId_ != UNDEF_OID) {
			StackAllocAutoPtr<BtreeMap> mvccMap(
				txn.getDefaultAllocator(), getMvccMap(txn));
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
	catch (std::exception &e) {
		handleUpdateError(txn, e, GS_ERROR_DS_DS_DROP_TIME_SERIES_FAILED);
	}

	return true;
}

/*!
	@brief Creates a specifed type of index on the specified Column
*/
void TimeSeries::createIndex(TransactionContext &txn,
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

		GS_TRACE_INFO(TIME_SERIES, GS_TRACE_DS_CON_CREATE_INDEX,
			"TimeSeries Id = " << getContainerId()
							   << ", first columnNumber = " << realIndexInfo.columnIds_[0]
							   << ", type = " << getMapTypeStr(inputMapType));

		if (!isSupportIndex(realIndexInfo)) {
			GS_THROW_USER_ERROR(
				GS_ERROR_CM_NOT_SUPPORTED, "not support this index type");
		}
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
							<< ", input anyNameMatches = " << (int32_t)realIndexInfo.anyNameMatches_;

					GS_THROW_USER_ERROR(GS_ERROR_CM_NOT_SUPPORTED, strstrm.str().c_str());
				}
			}
		}

		if (isAlterContainer()) {
			DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_TIM_LOCK_CONFLICT,
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

			util::XArray<SubTimeSeriesInfo> subTimeSeriesList(
				txn.getDefaultAllocator());
			util::XArray<SubTimeSeriesInfo>::iterator itr;
			getSubTimeSeriesList(txn, subTimeSeriesList, true, true);

			{
				uint64_t expiredNum = subTimeSeriesListHead_->getNum() - subTimeSeriesList.size();
				for (uint64_t i = 0; i < expiredNum; i++) {
					indexSchema_->createDummyIndexData(txn, realIndexInfo, i);
				}
			}

			for (itr = subTimeSeriesList.begin(); itr != subTimeSeriesList.end();
				 itr++) {
				util::StackAllocator::Scope scope(txn.getDefaultAllocator());
				SubTimeSeries *subContainer = ALLOC_NEW(txn.getDefaultAllocator())
					SubTimeSeries(txn, *itr, this);
				subContainer->createIndex(txn, realIndexInfo, indexCursor);
			}
			getIndexData(txn, realIndexInfo.columnIds_, inputMapType, 
				withUncommitted, indexData);
			indexCursor.setOption(indexData.optionOId_);
			if (!indexCursor.isImmediateMode()) {
				beforeImage = indexCursor.getMvccImage();
				insertMvccMap(txn, mvccMap.get(), tId, beforeImage);
			} else {
				indexSchema_->commit(txn, indexCursor);
			}
		}
		{
			TermCondition cond(getRowIdColumnType(), getRowIdColumnType(),
				DSExpression::GT, getRowIdColumnId(), &indexData.cursor_,
				sizeof(indexData.cursor_));
			BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, MAX_RESULT_SIZE);
			util::XArray<SubTimeSeriesInfo> subTimeSeriesList(
				txn.getDefaultAllocator());
			util::XArray<SubTimeSeriesInfo>::iterator itr;
			getSubTimeSeriesList(txn, sc, subTimeSeriesList, true);
			if (subTimeSeriesList.empty()) {
				indexCursor.setRowId(MAX_ROWID);
			}
			else {
				indexCursor.setRowId(indexData.cursor_);
				for (itr = subTimeSeriesList.begin(); itr != subTimeSeriesList.end();
					 itr++) {
					if (!indexCursor.isImmediateMode() && indexCursor.isFinished()) {
						indexCursor.setRowId(itr->image_.startTime_ - 1);
					}
					util::StackAllocator::Scope scope(txn.getDefaultAllocator());
					SubTimeSeries subContainer(txn, *itr, this);
					subContainer.indexInsert(txn, indexCursor);
					if (!indexCursor.isFinished()) {
						break;
					}
				}
			}
		}
	}
	catch (std::exception &e) {
		handleUpdateError(txn, e, GS_ERROR_DS_TIM_CREATE_INDEX_FAILED);
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

		{
			TermCondition cond(getRowIdColumnType(), getRowIdColumnType(),
				DSExpression::GT, getRowIdColumnId(), &indexData.cursor_,
				sizeof(indexData.cursor_));
			BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, MAX_RESULT_SIZE);
			util::XArray<SubTimeSeriesInfo> subTimeSeriesList(
				txn.getDefaultAllocator());
			util::XArray<SubTimeSeriesInfo>::iterator itr;
			getSubTimeSeriesList(txn, sc, subTimeSeriesList, true, true);
			if (subTimeSeriesList.empty()) {
				indexCursor.setRowId(MAX_ROWID);
			}
			else {
				indexCursor.setRowId(indexData.cursor_);
				for (itr = subTimeSeriesList.begin(); itr != subTimeSeriesList.end();
					 itr++) {
					if (!indexCursor.isImmediateMode() && indexCursor.isFinished()) {
						indexCursor.setRowId(itr->image_.startTime_ - 1);
					}
					util::StackAllocator::Scope scope(txn.getDefaultAllocator());
					SubTimeSeries *subContainer = ALLOC_NEW(txn.getDefaultAllocator())
						SubTimeSeries(txn, *itr, this);
					subContainer->indexInsert(txn, indexCursor);
					if (!indexCursor.isFinished()) {
						break;
					}
				}
			}
		}

	}
	catch (std::exception& e) {
		handleUpdateError(txn, e, GS_ERROR_DS_COL_CREATE_INDEX_FAILED);
	}
}

void TimeSeries::continueChangeSchema(TransactionContext &txn,
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
		{
			TermCondition cond(getRowIdColumnType(), getRowIdColumnType(),
				DSExpression::GT, getRowIdColumnId(), &rowIdCursor,
				sizeof(rowIdCursor));
			BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, MAX_RESULT_SIZE);
			util::XArray<SubTimeSeriesInfo> subTimeSeriesList(
				txn.getDefaultAllocator());
			util::XArray<SubTimeSeriesInfo>::iterator itr;
			getSubTimeSeriesList(txn, sc, subTimeSeriesList, true);
			if (subTimeSeriesList.empty()) {
				containerCursor.setRowId(MAX_ROWID);
			}
			else {
				for (itr = subTimeSeriesList.begin(); itr != subTimeSeriesList.end();
					 itr++) {
					if (!containerCursor.isImmediateMode() && containerCursor.isFinished()) {
						rowIdCursor = itr->image_.startTime_ - 1;
					}
					util::StackAllocator::Scope scope(txn.getDefaultAllocator());
					SubTimeSeries *subContainer = ALLOC_NEW(txn.getDefaultAllocator())
						SubTimeSeries(txn, *itr, this);
					subContainer->changeSchemaRecord<TimeSeries>(txn, *newContainer, copyColumnMap,
						rowIdCursor, containerCursor.isImmediateMode());
					containerCursor.setRowId(rowIdCursor);
					if (!containerCursor.isFinished()) {
						break;
					}
				}
			}
		}

		newContainer->commit(txn);
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

void TimeSeries::dropIndex(TransactionContext &txn, IndexInfo &indexInfo,
						   bool isIndexNameCaseSensitive) {
	try {
		GS_TRACE_DEBUG(TIME_SERIES, GS_TRACE_DS_CON_DROP_INDEX,
			"TimeSeries Id = " << getContainerId());

		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not drop index. container's status is invalid.");
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
			DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_TIM_LOCK_CONFLICT,
				"(pId=" << txn.getPartitionId() << ", containerId=" << getContainerId()
						<< ", txnId=" << txn.getId() << ")");
		}

		for (size_t i = 0; i < matchList.size(); i++) {
			util::Vector<ColumnId> &inputColumnIds = matchList[i].columnIds_;
			MapType inputMapType = matchList[i].mapType;

			util::XArray<SubTimeSeriesInfo> subTimeSeriesList(
				txn.getDefaultAllocator());
			util::XArray<SubTimeSeriesInfo>::reverse_iterator rItr;
			getSubTimeSeriesList(txn, subTimeSeriesList, true, true);
			for (rItr = subTimeSeriesList.rbegin();
				 rItr != subTimeSeriesList.rend(); rItr++) {
				util::StackAllocator::Scope scope(txn.getDefaultAllocator());
				SubTimeSeries subContainer(txn, *rItr, this);
				subContainer.dropIndex(txn, matchList[i], isIndexNameCaseSensitive);
			}
			{
				uint64_t expiredNum = subTimeSeriesListHead_->getNum() - subTimeSeriesList.size();
				for (uint64_t i = 0; i < expiredNum; i++) {
					indexSchema_->dropIndexData(
						txn, inputColumnIds, inputMapType, this, expiredNum - i - 1, false);
				}
			}
			indexSchema_->dropIndexInfo(
				txn, matchList[i].columnIds_, matchList[i].mapType);
		}
	}
	catch (std::exception &e) {
		handleUpdateError(txn, e, GS_ERROR_DS_TIM_DROP_INDEX_FAILED);
	}
}

/*!
	@brief Newly creates or updates a Row, based on the specified Row object and
   also the Row key specified as needed
*/
void TimeSeries::putRow(TransactionContext &txn, uint32_t rowSize,
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

		InputMessageRowStore inputMessageRowStore(
			getDataStore()->getValueLimitConfig(), getRealColumnInfoList(txn),
			getRealColumnNum(txn), const_cast<uint8_t *>(rowData), rowSize, 1,
			getRealRowFixedDataSize(txn), false);
		inputMessageRowStore.next();
		putRow(txn, &inputMessageRowStore, rowId, rowIdSpecified, status,
			putRowOption);
	}
	catch (std::exception &e) {
		handleUpdateError(txn, e, GS_ERROR_DS_COL_PUT_ROW_FAILED);
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
		bool dummy = false;
		putRow(txn, &inputMessageRowStore, rowKey, dummy, status, 
			PUT_INSERT_OR_UPDATE);
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

		if (getCompressionSchema().getCompressionType() != NO_COMPRESSION) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_TIM_UPDATE_INVALID,
				"Delete not support on Compression Mode");
		}

		if (isAlterContainer()) {
			DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_TIM_LOCK_CONFLICT,
				"(pId=" << txn.getPartitionId() << ", containerId=" << getContainerId()
						<< ", txnId=" << txn.getId() << ")");
		}

		SubTimeSeries *subContainer =
			getSubContainer(txn, rowKeyTimestamp, true);
		if (subContainer == NULL) {
			existing = false;
			rowId = UNDEF_ROWID;
			return;
		}
		subContainer->deleteRow(txn, rowKeySize, rowKey, rowId, existing);
		ALLOC_DELETE(txn.getDefaultAllocator(), subContainer);
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

		if (getCompressionSchema().getCompressionType() != NO_COMPRESSION) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_TIM_UPDATE_INVALID,
				"Delete not support on Compression Mode");
		}

		if (isAlterContainer()) {
			DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_TIM_LOCK_CONFLICT,
				"(pId=" << txn.getPartitionId() << ", containerId=" << getContainerId()
						<< ", txnId=" << txn.getId() << ")");
		}

		SubTimeSeries *subContainer =
			getSubContainer(txn, rowKeyTimestamp, true);
		if (subContainer == NULL) {
			existing = false;
			rowId = UNDEF_ROWID;
			return;
		}
		subContainer->deleteRow(txn, rowId, existing, isForceLock);
		ALLOC_DELETE(txn.getDefaultAllocator(), subContainer);
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

		if (getCompressionSchema().getCompressionType() != NO_COMPRESSION) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_TIM_UPDATE_INVALID,
				"Update not support on Compression Mode");
		}

		if (isAlterContainer()) {
			DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_TIM_LOCK_CONFLICT,
				"(pId=" << txn.getPartitionId() << ", containerId=" << getContainerId()
						<< ", txnId=" << txn.getId() << ")");
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
		ALLOC_DELETE(txn.getDefaultAllocator(), subContainer);
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
		if (isExpired(txn)) {
			dataStore_->setLastExpiredTime(txn.getPartitionId(), txn.getStatementStartTime().getUnixTime());
			return;
		}

		util::XArray<SubTimeSeriesInfo> subTimeSeriesList(
			txn.getDefaultAllocator());
		util::XArray<SubTimeSeriesInfo>::iterator itr;
		getRuntimeSubTimeSeriesList(txn, subTimeSeriesList, true);
		for (itr = subTimeSeriesList.begin(); itr != subTimeSeriesList.end();
			 itr++) {
			util::StackAllocator::Scope scope(txn.getDefaultAllocator());
			SubTimeSeries subContainer(txn, *itr, this);
			subContainer.abort(txn);
		}
		{
			TransactionId tId = txn.getId();
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
				for (itr = mvccList.begin(); itr != mvccList.end(); itr++) {
					if (isAlterContainer() && itr->type_ != MVCC_CONTAINER) {
						GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_COL_LOCK_CONFLICT, 
							"abort : container already locked "
							<< ", partitionId = " << txn.getPartitionId()
							<< ", txnId = " << txn.getId()
							<< ", containerId = " << getContainerId()
							);
					}
					switch (itr->type_) {
					case MVCC_SELECT:
						{ removeMvccMap(txn, mvccMap.get(), tId, *itr); }
						break;
					case MVCC_INDEX:
						{
							IndexCursor indexCursor = IndexCursor(*itr);
							IndexData indexData(txn.getDefaultAllocator());
							bool isExist = getIndexData(txn, indexCursor, indexData);
							UNUSED_VARIABLE(isExist);
							IndexInfo indexInfo(txn.getDefaultAllocator(),
								*(indexData.columnIds_), indexData.mapType_);

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
					default:
						GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "Type = " << (int)itr->type_
							<< "This type must not exist in Timeseries ");
						break;
					}
				}
			}
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

		if (isExpired(txn)) {
			dataStore_->setLastExpiredTime(txn.getPartitionId(), txn.getStatementStartTime().getUnixTime());
			return;
		}

		util::XArray<SubTimeSeriesInfo> subTimeSeriesList(
			txn.getDefaultAllocator());
		util::XArray<SubTimeSeriesInfo>::iterator itr;
		getRuntimeSubTimeSeriesList(txn, subTimeSeriesList, true);
		for (itr = subTimeSeriesList.begin(); itr != subTimeSeriesList.end();
			 itr++) {
			util::StackAllocator::Scope scope(txn.getDefaultAllocator());
			SubTimeSeries subContainer(txn, *itr, this);
			subContainer.commit(txn);
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
				for (itr = mvccList.begin(); itr != mvccList.end(); itr++) {
					if (isAlterContainer() && itr->type_ != MVCC_CONTAINER) {
						GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TIM_LOCK_CONFLICT, 
							"commit : container already locked "
							<< ", partitionId = " << txn.getPartitionId()
							<< ", txnId = " << txn.getId()
							<< ", containerId = " << getContainerId()
							);
					}
					switch (itr->type_) {
					case MVCC_SELECT:
						{ removeMvccMap(txn, mvccMap.get(), tId, *itr); }
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
	catch (std::exception &e) {
		handleUpdateError(txn, e, GS_ERROR_DS_TIM_COMMIT_FAILED);
	}
}

/*!
	@brief Check if Container has data of uncommited transaction
*/
bool TimeSeries::hasUncommitedTransaction(TransactionContext &txn) {
	if (isExpired(txn)) {
		return false;
	}
	StackAllocAutoPtr<BtreeMap> mvccMap(
		txn.getDefaultAllocator(), getMvccMap(txn));
	if (!mvccMap.get()->isEmpty()) {
		return true;
	}

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
	if (expiredTime != MINIMUM_EXPIRED_TIMESTAMP) {
		TermCondition newCond(getRowIdColumnType(), getRowIdColumnType(), 
			DSExpression::GT, getRowIdColumnId(), &expiredTime, sizeof(expiredTime));
		sc.updateCondition(txn, newCond);
	}
	util::XArray<SubTimeSeriesInfo> subTimeSeriesImageList(
		txn.getDefaultAllocator());
	getSubTimeSeriesList(txn, sc, subTimeSeriesImageList, false);
	if (order != ORDER_DESCENDING) {
		util::XArray<SubTimeSeriesInfo>::iterator itr;
		for (itr = subTimeSeriesImageList.begin();
			 itr != subTimeSeriesImageList.end(); itr++) {
			SubTimeSeries subContainer(txn, *itr, this);
			subContainer.searchRowIdIndex(txn, sc, resultList,
				ORDER_ASCENDING);  
			if (resultList.size() >= sc.getLimit() || sc.isSuspended()) {
				break;
			}
		}
	}
	else {
		util::XArray<SubTimeSeriesInfo>::reverse_iterator itr;
		for (itr = subTimeSeriesImageList.rbegin();
			 itr != subTimeSeriesImageList.rend(); itr++) {
			SubTimeSeries subContainer(txn, *itr, this);
			subContainer.searchRowIdIndex(txn, sc, resultList, order);
			if (resultList.size() >= sc.getLimit() || sc.isSuspended()) {
				break;
			}
		}
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

	TermCondition startCond(COLUMN_TYPE_TIMESTAMP, COLUMN_TYPE_TIMESTAMP, 
		DSExpression::GE, ColumnInfo::ROW_KEY_COLUMN_ID, &minRowId, sizeof(minRowId));
	TermCondition endCond(COLUMN_TYPE_TIMESTAMP, COLUMN_TYPE_TIMESTAMP, 
		DSExpression::LE, ColumnInfo::ROW_KEY_COLUMN_ID, &maxRowId, sizeof(maxRowId));
	BtreeMap::SearchContext sc(txn.getDefaultAllocator(), startCond, endCond, MAX_RESULT_SIZE);
	util::XArray<KeyValue<RowId, OId> > keyValueList(txn.getDefaultAllocator());
	util::XArray<OId> mvccList(txn.getDefaultAllocator());

	util::XArray<SubTimeSeriesInfo> subTimeSeriesImageList(
		txn.getDefaultAllocator());
	getSubTimeSeriesList(txn, sc, subTimeSeriesImageList, false);
	util::XArray<SubTimeSeriesInfo>::iterator itr;
	for (itr = subTimeSeriesImageList.begin();
		 itr != subTimeSeriesImageList.end(); itr++) {
		SubTimeSeries subContainer(txn, *itr, this);

		StackAllocAutoPtr<BtreeMap> rowIdMap(
			txn.getDefaultAllocator(), subContainer.getRowIdMap(txn));
		rowIdMap.get()->search<RowId, OId, KeyValue<RowId, OId> >(
			txn, sc, keyValueList, ORDER_ASCENDING);

		bool isCheckOnly = false;
		const bool ignoreTxnCheck = false;
		subContainer.searchMvccMap<TimeSeries, BtreeMap::SearchContext>(
			txn, sc, mvccList, isCheckOnly, ignoreTxnCheck);
	}

	RowArray rowArray(txn, this);
	util::Map<RowId, OId> mvccRowIdMap(txn.getDefaultAllocator());
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

	Timestamp expiredTime = getCurrentExpiredTime(txn);
	if (expiredTime != MINIMUM_EXPIRED_TIMESTAMP) {
		TermCondition newCond(getRowIdColumnType(), getRowIdColumnType(), 
			DSExpression::GT, getRowIdColumnId(), &expiredTime, sizeof(expiredTime));
		sc.updateCondition(txn, newCond);
	}

	util::StackAllocator &alloc = txn.getDefaultAllocator();

	util::XArray<SubTimeSeriesInfo> subInfoList(alloc);
	getSubTimeSeriesList(txn, sc, subInfoList, false);
	for (util::XArray<SubTimeSeriesInfo>::iterator it =
			subInfoList.begin(); it != subInfoList.end(); ++it) {
		SubTimeSeries sub(txn, *it, this);

		sub.searchRowIdIndexAsRowArray(txn, sc, oIdList, mvccOIdList);
		if (sc.isSuspended()) {
			break;
		}
	}

}

void TimeSeries::scanRowIdIndex(
		TransactionContext& txn, BtreeMap::SearchContext &sc,
		OutputOrder order, ContainerRowScanner &scanner) {

	Timestamp expiredTime = getCurrentExpiredTime(txn);
	if (expiredTime != MINIMUM_EXPIRED_TIMESTAMP) {
		TermCondition newCond(getRowIdColumnType(), getRowIdColumnType(), 
			DSExpression::GT, getRowIdColumnId(), &expiredTime, sizeof(expiredTime));
		sc.updateCondition(txn, newCond);
	}

	util::StackAllocator &alloc = txn.getDefaultAllocator();

	util::XArray<OId> oIdList1(alloc);
	util::XArray<OId> oIdList2(alloc);
	util::XArray<OId> oIdList3(alloc);
	util::XArray<OId> *oIdListRef[] = { &oIdList1, &oIdList2, &oIdList3 };
	util::XArray< std::pair<RowId, OId> > rowOIdList(alloc);

	util::XArray<SubTimeSeriesInfo> subInfoList(alloc);
	getSubTimeSeriesList(txn, sc, subInfoList, false);
	if (order != ORDER_DESCENDING) {
		for (util::XArray<SubTimeSeriesInfo>::iterator it =
				subInfoList.begin(); it != subInfoList.end(); it++) {
			SubTimeSeries sub(txn, *it, this);

			if (!sub.scanRowIdIndex(
					txn, sc, ORDER_ASCENDING,
					scanner, oIdListRef, rowOIdList) ||
					sc.isSuspended()) {
				break;
			}
		}
	}
	else {
		util::XArray<SubTimeSeriesInfo>::reverse_iterator itr;
		for (util::XArray<SubTimeSeriesInfo>::reverse_iterator it =
				subInfoList.rbegin(); it != subInfoList.rend(); it++) {
			SubTimeSeries sub(txn, *it, this);

			if (sub.scanRowIdIndex(
					txn, sc, order, scanner, oIdListRef, rowOIdList) ||
					sc.isSuspended()) {
				break;
			}
		}
	}

}

void TimeSeries::scanRowArray(
		TransactionContext& txn, const OId *begin, const OId *end,
		bool onMvcc, ContainerRowScanner &scanner) {

	const Timestamp expiredTime = getCurrentExpiredTime(txn);

	if (expiredTime != MINIMUM_EXPIRED_TIMESTAMP || isRuntime(txn)) {
		RowArray &rowArray = scanner.getRowArray();

		const RowId startRowId = getScanStartRowId(expiredTime);
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

	const Timestamp expiredTime = getCurrentExpiredTime(txn);

	if (expiredTime != MINIMUM_EXPIRED_TIMESTAMP || isRuntime(txn)) {
		RowArray &rowArray = scanner.getRowArray();

		const RowId startRowId = getScanStartRowId(expiredTime);
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
	util::XArray<SubTimeSeriesInfo> subTimeSeriesImageList(
		txn.getDefaultAllocator());
	getSubTimeSeriesList(txn, sc, subTimeSeriesImageList, false);
	util::XArray<SubTimeSeriesInfo>::iterator itr;
	ColumnId sortColumnId;
	if (order != ORDER_UNDEFINED) {
		sortColumnId = sc.getScColumnId();
	}
	else {
		sortColumnId = ColumnInfo::ROW_KEY_COLUMN_ID;
	}
	ColumnInfo &sortColumnInfo = getColumnInfo(sortColumnId);
	util::XArray<OId> mergeList(txn.getDefaultAllocator());
	for (itr = subTimeSeriesImageList.begin();
		 itr != subTimeSeriesImageList.end(); itr++) {
		util::XArray<OId> subList(txn.getDefaultAllocator());
		SubTimeSeries subContainer(txn, *itr, this);
		if (order != ORDER_UNDEFINED) {
			reinterpret_cast<BaseContainer *>(&subContainer)
				->searchColumnIdIndex(txn, sc, subList, order);
			mergeRowList<TimeSeries>(txn, sortColumnInfo, resultList, true,
				subList, true, mergeList, order);
			resultList.swap(mergeList);
		}
		else {
			ResultSize limitBackup = sc.getLimit();
			sc.setLimit(MAX_RESULT_SIZE);
			reinterpret_cast<BaseContainer *>(&subContainer)
				->searchColumnIdIndex(
					txn, sc, subList, order);
			sc.setLimit(limitBackup);
			if (!neverOrdering) {
				mergeRowList<TimeSeries>(txn, sortColumnInfo, resultList, true,
					subList, false, mergeList, ORDER_ASCENDING);
				resultList.swap(mergeList);
			} else {
				resultList.insert(resultList.end(), subList.begin(), subList.end());
			}
			if (resultList.size() >= sc.getLimit() || sc.isSuspended()) {
				if (sc.isSuspended() && isExpireContainer()) {
					sc.setSuspendRowId(itr->image_.startTime_);
				}
				break;
			}
			if (!sc.isSuspended() && isExpireContainer()) {
				orgSc.setSuspendLimit(sc.getSuspendLimit());
				sc = orgSc;
			}
		}
		mergeList.clear();
	}
	if (resultList.size() > sc.getLimit()) {
		resultList.resize(sc.getLimit());
	}
}

void TimeSeries::searchColumnIdIndex(TransactionContext &txn,
	BtreeMap::SearchContext &sc, BtreeMap::SearchContext &orgSc,
	util::XArray<OId> &normalRowList, util::XArray<OId> &mvccRowList)
{
	util::XArray<SubTimeSeriesInfo> subTimeSeriesImageList(
		txn.getDefaultAllocator());
	getSubTimeSeriesList(txn, sc, subTimeSeriesImageList, false);
	util::XArray<SubTimeSeriesInfo>::iterator itr;
	util::XArray<OId> mergeList(txn.getDefaultAllocator());
	for (itr = subTimeSeriesImageList.begin();
		 itr != subTimeSeriesImageList.end(); itr++) {
		util::XArray<OId> subNormalList(txn.getDefaultAllocator());
		util::XArray<OId> subMvccList(txn.getDefaultAllocator());
		SubTimeSeries subContainer(txn, *itr, this);

		ResultSize limitBackup = sc.getLimit();
		sc.setLimit(MAX_RESULT_SIZE);
		reinterpret_cast<BaseContainer *>(&subContainer)
			->searchColumnIdIndex(
				txn, sc, subNormalList, subMvccList);
		sc.setLimit(limitBackup);
		normalRowList.insert(normalRowList.end(), subNormalList.begin(), subNormalList.end());
		mvccRowList.insert(mvccRowList.end(), subMvccList.begin(), subMvccList.end());
		if (normalRowList.size() + mvccRowList.size() >= sc.getLimit() || sc.isSuspended()) {
			if (sc.isSuspended() && isExpireContainer()) {
				sc.setSuspendRowId(itr->image_.startTime_);
			}
			break;
		}
		if (!sc.isSuspended() && isExpireContainer()) {
			orgSc.setSuspendLimit(sc.getSuspendLimit());
			sc = orgSc;
		}
		mergeList.clear();
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

	util::Vector<TermCondition> keyCondList(txn.getDefaultAllocator());
	sc.getConditionList(keyCondList, BaseIndex::SearchContext::COND_KEY);
	util::Vector<TermCondition> condList(txn.getDefaultAllocator());
	sc.getConditionList(condList, BaseIndex::SearchContext::COND_OTHER);
	searchRowArrayList(txn, sc, normalOIdList, mvccOIdList);

	ContainerValue containerValue(pId, *getObjectManager());
	switch (type) {
	case AGG_TIME_AVG: {
		ContainerValue containerValueTs(pId, *getObjectManager());
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
							pId, rowArrayPtr->getTxnId()))) ||
				(!isNoramlExecute && txn.getId() != rowArrayPtr->getTxnId())) {
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
	
	ContainerValue containerValue(txn.getPartitionId(), *getObjectManager());
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
	{
		if (interval != 0) {
			Timestamp expiredTime = getCurrentExpiredTime(txn);
			if (targetT < expiredTime) {
				Timestamp diffTime = expiredTime - targetT;
				int64_t div = diffTime / interval;
				int64_t mod = diffTime % interval;
				Timestamp newStartTime;
				if (mod == 0) {
					newStartTime = targetT + (interval * div);
				}
				else {
					newStartTime = targetT + (interval * (div + 1));
				}
				targetT = newStartTime;
			}
		}
	}

	Timestamp expiredTime = getCurrentExpiredTime(txn);
	if (expiredTime != MINIMUM_EXPIRED_TIMESTAMP) {
		TermCondition newCond(getRowIdColumnType(), getRowIdColumnType(), 
			DSExpression::GT, getRowIdColumnId(), &expiredTime, sizeof(expiredTime));
		sc.updateCondition(txn, newCond);
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
	ContainerValue containerValue(pId, *getObjectManager());
	ContainerValue prevContainerValue(pId, *getObjectManager());
	ContainerValue nextContainerValue(pId, *getObjectManager());
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
		opList[columnId].sub_ = CalculatorTable::subTable_[type][type];
		opList[columnId].mul_ = CalculatorTable::mulTable_[type][COLUMN_TYPE_DOUBLE];
		opList[columnId].add_ = CalculatorTable::addTable_[type][COLUMN_TYPE_DOUBLE];
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
						pId, rowArrayPtr->getTxnId()))) ||
			(!isNoramlExecute && txn.getId() != rowArrayPtr->getTxnId())) {
			for (rowArrayPtr->begin(); !rowArrayPtr->end();
				 rowArrayPtr->next()) {
				RowArray::Row row(rowArrayPtr->getRow(), rowArrayPtr);
				Timestamp rowId = row.getRowId();
				if (rowId < expiredTime) {
					continue;
				}

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
	{
		if (interval != 0) {
			Timestamp expiredTime = getCurrentExpiredTime(txn);
			if (targetT < expiredTime) {
				Timestamp diffTime = expiredTime - targetT;
				int64_t div = diffTime / interval;
				int64_t mod = diffTime % interval;
				Timestamp newStartTime;
				if (mod == 0) {
					newStartTime = targetT + (interval * div);
				}
				else {
					newStartTime = targetT + (interval * (div + 1));
				}
				targetT = newStartTime;
			}
		}
	}


	Timestamp expiredTime = getCurrentExpiredTime(txn);
	if (expiredTime != MINIMUM_EXPIRED_TIMESTAMP) {
		TermCondition newCond(getRowIdColumnType(), getRowIdColumnType(), 
			DSExpression::GT, getRowIdColumnId(), &expiredTime, sizeof(expiredTime));
		sc.updateCondition(txn, newCond);
	}
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

	ContainerValue containerValue(pId, *getObjectManager());
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
						pId, rowArrayPtr->getTxnId()))) ||
			(!isNoramlExecute && txn.getId() != rowArrayPtr->getTxnId())) {
			for (rowArrayPtr->begin(); !rowArrayPtr->end();
				 rowArrayPtr->next()) {
				RowArray::Row row(rowArrayPtr->getRow(), rowArrayPtr);
				Timestamp rowId = row.getRowId();
				if (rowId < expiredTime) {
					continue;
				}

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
							value.get(txn, *getObjectManager(),
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
				value.get(txn, *getObjectManager(),
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
	@brief Lock Rows
*/
void TimeSeries::lockRowList(
	TransactionContext &txn, util::XArray<RowId> &rowIdList) {
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
			DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_TIM_LOCK_CONFLICT,
				"(pId=" << txn.getPartitionId() << ", containerId=" << getContainerId()
						<< ", txnId=" << txn.getId() << ")");
		}

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
	catch (std::exception &e) {
		handleSearchError(txn, e, GS_ERROR_DS_TIM_GET_LOCK_ID_INVALID);
	}
}

/*!
	@brief Get max expired time
*/
Timestamp TimeSeries::getCurrentExpiredTime(TransactionContext &txn) {
	const ExpirationInfo &expirationInfo = getExpirationInfo();
	if (expirationInfo.duration_ == INT64_MAX) {
		return MINIMUM_EXPIRED_TIMESTAMP;  
	}
	else {
		Timestamp lastExpiredTime = getDataStore()->getLatestExpirationCheckTime(txn.getPartitionId());

		Timestamp currentTime = txn.getStatementStartTime().getUnixTime();
		if (currentTime >= lastExpiredTime) {
			return currentTime - expirationInfo.duration_;
		}
		else {
			return lastExpiredTime - expirationInfo.duration_;
		}
	}
}


void TimeSeries::putRow(TransactionContext &txn,
	InputMessageRowStore *inputMessageRowStore, RowId &rowId,
	bool rowIdSpecified,
	DataStore::PutStatus &status, PutRowOption putRowOption) {
	util::StackAllocator::Scope scope(txn.getDefaultAllocator());
	if (isAlterContainer()) {
		DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_TIM_LOCK_CONFLICT,
			"(pId=" << txn.getPartitionId() << ", containerId=" << getContainerId()
					<< ", txnId=" << txn.getId() << ")");
	}

	Timestamp rowKey = inputMessageRowStore->getField<COLUMN_TYPE_TIMESTAMP>(
		ColumnInfo::ROW_KEY_COLUMN_ID);

	const CompressionSchema &compressionSchema = getCompressionSchema();

	if (compressionSchema.getCompressionType() != NO_COMPRESSION &&
		putRowOption == PUT_UPDATE_ONLY) {
		if (isCompressionErrorMode()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_TIM_UPDATE_INVALID,
				"insert(old time)/update not support on Compression Mode : "
				"rowKey("
					<< rowKey << ")");
		}
		else {
			rowId = UNDEF_ROWID;
			status = DataStore::NOT_EXECUTED;
			const FullContainerKey containerKey = getContainerKey(txn);
			util::String containerName(txn.getDefaultAllocator());
			containerKey.toString(txn.getDefaultAllocator(), containerName);
			GS_TRACE_ERROR(TIME_SERIES,
				GS_TRACE_DS_TIM_COMPRESSION_INVALID_WARNING,
				"insert(old time)/update not support on Compression Mode : "
				"Container("
					<< containerName << ") rowKey("
					<< rowKey << ")");
			return;
		}
	}

	SubTimeSeries *subContainer = putSubContainer(txn, rowKey);
	if (subContainer == NULL) {
		rowId = UNDEF_ROWID;
		status = DataStore::NOT_EXECUTED;
		return;
	}
	subContainer->putRow(txn, inputMessageRowStore, rowId, rowIdSpecified,
		status, putRowOption);
	ALLOC_DELETE(txn.getDefaultAllocator(), subContainer);
}


SubTimeSeries *TimeSeries::putSubContainer(
	TransactionContext &txn, Timestamp rowKey) {
	SubTimeSeries *container = NULL;
	Timestamp expiredTime = getCurrentExpiredTime(txn);
	if (rowKey <= expiredTime || isExpired(txn)) {
		return NULL;
	}
	bool isIndexUpdate = false;
	expireSubTimeSeries(txn, expiredTime, isIndexUpdate);

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
	TransactionContext &txn, Timestamp expiredTime, bool indexUpdate) {

	Timestamp erasableTime = expiredTime;
	int32_t expireNum = 0;
	if (!dataStore_->getConfig().isAutoExpire()) {
		Timestamp configErasableTime = dataStore_->getConfig().getErasableExpiredTime();
		if (erasableTime > configErasableTime) {
			erasableTime = configErasableTime;
		}
	}
	for (uint64_t i = 0; i < subTimeSeriesListHead_->getNum(); i++) {
		const SubTimeSeriesImage &subTimeSeriesImage =
			*(subTimeSeriesListHead_->get(txn, i));
		SubTimeSeriesInfo subInfo(subTimeSeriesImage, i);
		SubTimeSeries subContainer(txn, subInfo, this);
		ChunkKey subChunkKey = subContainer.getRowAllcateStrategy().chunkKey_;
		Timestamp subTimeErasableTime = DataStore::convertChunkKey2Timestamp(subChunkKey);

		bool isFinish = true;
		if (subContainer.getEndTime() <= expiredTime && indexUpdate) {
			dataStore_->setLastExpiredTime(txn.getPartitionId(), txn.getStatementStartTime().getUnixTime());
			isFinish = false;
		}
		if (subTimeErasableTime <= erasableTime) {
			expireNum++;
			isFinish = false;
		}
		if (isFinish) {
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
	TermCondition cond(COLUMN_TYPE_TIMESTAMP, COLUMN_TYPE_TIMESTAMP, 
		DSExpression::EQ, ColumnInfo::ROW_KEY_COLUMN_ID, &rowKey, sizeof(rowKey));
	BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, 1);
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
			bool isOldSchema = rowArray.load(txn, oIdList[i], this, OBJECT_FOR_UPDATE);
			UNUSED_VARIABLE(isOldSchema);
			assert(isOldSchema || !isOldSchema);
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
			SubTimeSeries subContainer(txn, subTimeSeriesInfo, this);
			subContainer.setDummyMvccImage(txn);
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

	const ExpirationInfo &expirationInfo = getExpirationInfo();
	containerSchema.push_back(
		reinterpret_cast<const uint8_t *>(&(expirationInfo.elapsedTime_)),
		sizeof(int32_t));
	int8_t tmp = static_cast<int8_t>(expirationInfo.timeUnit_);
	containerSchema.push_back(
		reinterpret_cast<const uint8_t *>(&tmp), sizeof(int8_t));
	int32_t tmpDivisionCount = (expirationInfo.elapsedTime_ > 0 ?
		expirationInfo.dividedNum_ : EXPIRE_DIVIDE_UNDEFINED_NUM);
	containerSchema.push_back(
		reinterpret_cast<const uint8_t *>(&tmpDivisionCount), sizeof(int32_t));

	const CompressionSchema &compressionSchema = getCompressionSchema();
	if (compressionSchema.getCompressionType() != NO_COMPRESSION) {
		int32_t compressionDuration = static_cast<int32_t>(
			compressionSchema.getDurationInfo().timeDuration_);
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&compressionDuration), sizeof(int32_t));
		int8_t tmpTimeUnit =
			static_cast<int8_t>(compressionSchema.getDurationInfo().timeUnit_);
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&tmpTimeUnit), sizeof(int8_t));

		int8_t tmpCompressionType =
			static_cast<int8_t>(compressionSchema.getCompressionType());
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&tmpCompressionType), sizeof(int8_t));

		util::Vector<ColumnId> hiCompressionColumnList(
			txn.getDefaultAllocator());
		compressionSchema.getHiCompressionColumnList(hiCompressionColumnList);

		uint32_t compressionInfoNum =
			static_cast<uint32_t>(hiCompressionColumnList.size());
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&compressionInfoNum), sizeof(uint32_t));

		for (uint32_t i = 0; i < hiCompressionColumnList.size(); i++) {
			int32_t columnId = hiCompressionColumnList[i];
			double threshhold, rate, span;
			bool threshholdRelative;
			uint16_t compressionPos;
			compressionSchema.getHiCompressionProperty(columnId, threshhold,
				rate, span, threshholdRelative, compressionPos);

			containerSchema.push_back(
				reinterpret_cast<uint8_t *>(&columnId), sizeof(int32_t));
			containerSchema.push_back(
				reinterpret_cast<uint8_t *>(&threshholdRelative), sizeof(bool));
			if (threshholdRelative) {
				containerSchema.push_back(
					reinterpret_cast<uint8_t *>(&rate), sizeof(double));
				containerSchema.push_back(
					reinterpret_cast<uint8_t *>(&span), sizeof(double));
			}
			else {
				containerSchema.push_back(
					reinterpret_cast<uint8_t *>(&threshhold), sizeof(double));
			}
		}
	}
	else {
		DurationInfo defaultDurationInfo;
		int32_t compressionDuration =
			static_cast<int32_t>(defaultDurationInfo.timeDuration_);
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&compressionDuration), sizeof(int32_t));
		int8_t tmpTimeUnit = static_cast<int8_t>(defaultDurationInfo.timeUnit_);
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&tmpTimeUnit), sizeof(int8_t));

		int8_t tmpCompressionType =
			static_cast<int8_t>(compressionSchema.getCompressionType());
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&tmpCompressionType), sizeof(int8_t));

		uint32_t compressionInfoNum = 0;
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&compressionInfoNum), sizeof(uint32_t));
	}
}

util::String TimeSeries::getBibContainerOptionInfo(TransactionContext &txn) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();
	util::NormalOStringStream strstrm;

	strstrm << "," << std::endl;
	strstrm << "\"timeSeriesProperties\" : {" << std::endl;
	{
		const ExpirationInfo &expirationInfo = getExpirationInfo();
		strstrm << "\"rowExpirationElapsedTime\": " << expirationInfo.elapsedTime_ << "," << std::endl; 
		strstrm << "\"rowExpirationTimeUnit\": \"" << BibInfoUtil::getTimeUnitStr(expirationInfo.timeUnit_) << "\"," << std::endl; 
		int32_t tmpDivisionCount = (expirationInfo.elapsedTime_ > 0 ?
			expirationInfo.dividedNum_ : EXPIRE_DIVIDE_UNDEFINED_NUM);
		strstrm << "\"expirationDivisionCount\": " << tmpDivisionCount << std::endl; 
		const CompressionSchema &compressionSchema = getCompressionSchema();
		strstrm << "," << std::endl;
		strstrm << "\"compressionMethod\": \"" << BibInfoUtil::getCompressionTypeStr(compressionSchema.getCompressionType()) << "\"," << std::endl; 
		strstrm << "\"compressionWindowSize\": " << compressionSchema.getDurationInfo().timeDuration_ << "," << std::endl; 
		strstrm << "\"compressionWindowSizeUnit\": \"" << BibInfoUtil::getTimeUnitStr(compressionSchema.getDurationInfo().timeUnit_) << "\"" << std::endl; 
	}
	strstrm << "}," << std::endl;
	strstrm << "\"compressionInfoSet\" : [" << std::endl;
	const CompressionSchema &compressionSchema = getCompressionSchema();
	if (compressionSchema.getCompressionType() == HI_COMPRESSION) {
		util::Vector<ColumnId> hiCompressionColumnList(alloc);
		compressionSchema.getHiCompressionColumnList(hiCompressionColumnList);
		for (uint32_t i = 0; i < hiCompressionColumnList.size(); i++) {
			if (i != 0) {
				strstrm << "," << std::endl;
			}
			strstrm << "{" << std::endl;
			int32_t columnId = hiCompressionColumnList[i];
			strstrm << "\"columnName\": \"" << getColumnInfo(columnId).getColumnName(txn, *getObjectManager()) << "\"," << std::endl; 
			double threshhold, rate, span;
			bool threshholdRelative;
			uint16_t compressionPos;
			compressionSchema.getHiCompressionProperty(columnId, threshhold,
				rate, span, threshholdRelative, compressionPos);
			if (threshholdRelative) {
				strstrm << "\"compressionType\": \"RELATIVE\"," << std::endl; 
				strstrm << "\"rate\": " << rate << "," << std::endl; 
				strstrm << "\"span\": " << span << std::endl; 
			} else {
				strstrm << "\"compressionType\": \"ABSOLUTE\"," << std::endl; 
				strstrm << "\"width\": " << threshhold << std::endl; 
			}
			strstrm << "}" << std::endl;
		}
	}
	strstrm << "]" << std::endl;
	util::String result(alloc);
	result = strstrm.str().c_str();
	return result;
}

void TimeSeries::checkContainerOption(MessageSchema *orgMessageSchema,
	util::XArray<uint32_t> &copyColumnMap, bool &) {
	MessageTimeSeriesSchema *messageSchema =
		reinterpret_cast<MessageTimeSeriesSchema *>(orgMessageSchema);
	uint32_t columnNum = messageSchema->getColumnCount();
	const CompressionSchema &compressionSchema = getCompressionSchema();

	if (!messageSchema->isExistTimeSeriesOption()) {

		messageSchema->setExpirationInfo(getExpirationInfo());
		messageSchema->setCompressionType(
			compressionSchema.getCompressionType());

		if (compressionSchema.getCompressionType() != NO_COMPRESSION) {
			DurationInfo durationInfo = compressionSchema.getDurationInfo();
			messageSchema->setDurationInfo(durationInfo);

			for (uint32_t i = 0; i < columnNum; i++) {
				if (copyColumnMap[i] != UNDEF_COLUMNID) {
					ColumnId oldColumnId = copyColumnMap[i];

					if (compressionSchema.isHiCompression(oldColumnId)) {
						double threshhold, rate, span;
						bool threshholdRelative;
						uint16_t compressionPos;
						compressionSchema.getHiCompressionProperty(oldColumnId,
							threshhold, rate, span, threshholdRelative,
							compressionPos);
						messageSchema->setCompressionInfoNum(
							messageSchema->getCompressionInfoNum() + 1);
						messageSchema->getCompressionInfo(i).set(
							MessageCompressionInfo::DSDC, threshholdRelative,
							threshhold, rate, span);
					}
				}
			}
		}
	}
	else {
		const ExpirationInfo &expirationInfo = getExpirationInfo();
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

		if (messageSchema->getCompressionType() !=
			compressionSchema.getCompressionType()) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_TIM_INVALID_SCHEMA_OPTION,
				"Compression type is different. old = "
					<< (int32_t)compressionSchema.getCompressionType()
					<< ", new = "
					<< (int32_t)messageSchema->getCompressionType());
		}

		if (compressionSchema.getCompressionType() != NO_COMPRESSION) {
			if (compressionSchema.getDurationInfo().timeDuration_ !=
				messageSchema->getDurationInfo().timeDuration_) {
				GS_THROW_USER_ERROR(GS_ERROR_DS_TIM_INVALID_SCHEMA_OPTION,
					"compression window size is different. old = "
						<< compressionSchema.getDurationInfo().timeDuration_
						<< ", new = "
						<< messageSchema->getDurationInfo().timeDuration_);
			}
			if (compressionSchema.getDurationInfo().timeUnit_ !=
				messageSchema->getDurationInfo().timeUnit_) {
				GS_THROW_USER_ERROR(GS_ERROR_DS_TIM_INVALID_SCHEMA_OPTION,
					"time unit of compression window size is different. old = "
						<< (int32_t)compressionSchema.getDurationInfo()
							   .timeUnit_
						<< ", new = "
						<< (int32_t)messageSchema->getDurationInfo().timeUnit_);
			}

			if (messageSchema->getCompressionInfoNum() != 0) {
				for (uint32_t i = 0; i < columnNum; i++) {
					if (copyColumnMap[i] != UNDEF_COLUMNID) {
						ColumnId oldColumnId = copyColumnMap[i];

						MessageCompressionInfo::CompressionType type =
							messageSchema->getCompressionInfo(i).getType();
						bool threshholdRelative =
							messageSchema->getCompressionInfo(i)
								.getThreshholdRelative();
						double threshhold = messageSchema->getCompressionInfo(i)
												.getThreshhold();
						double rate =
							messageSchema->getCompressionInfo(i).getRate();
						double span =
							messageSchema->getCompressionInfo(i).getSpan();

						if (type == MessageCompressionInfo::NONE) {
							if (compressionSchema.isHiCompression(
									oldColumnId)) {
								messageSchema->setCompressionInfoNum(
									messageSchema->getCompressionInfoNum() + 1);
								double oldThreshhold, oldRate, oldSpan;
								bool oldThreshholdRelative;
								uint16_t compressionPos;
								compressionSchema.getHiCompressionProperty(
									oldColumnId, oldThreshhold, oldRate,
									oldSpan, oldThreshholdRelative,
									compressionPos);

								messageSchema->getCompressionInfo(i).set(
									MessageCompressionInfo::DSDC,
									oldThreshholdRelative, oldThreshhold,
									oldRate, oldSpan);
							}
						}
						else {
							if (compressionSchema.isHiCompression(
									oldColumnId)) {
								double oldThreshhold, oldRate, oldSpan;
								bool oldThreshholdRelative;
								uint16_t compressionPos;
								compressionSchema.getHiCompressionProperty(
									oldColumnId, oldThreshhold, oldRate,
									oldSpan, oldThreshholdRelative,
									compressionPos);

								if (oldThreshholdRelative !=
									threshholdRelative) {
									GS_THROW_USER_ERROR(
										GS_ERROR_DS_TIM_INVALID_SCHEMA_OPTION,
										"column = "
											<< messageSchema->getColumnName(i)
												   .c_str()
											<< " : "
											<< "threshholdRelative of "
											   "compression is different. old "
											   "= "
											<< (int32_t)oldThreshholdRelative
											<< ", new = "
											<< (int32_t)threshholdRelative);
								}
								if (oldThreshhold != threshhold) {
									GS_THROW_USER_ERROR(
										GS_ERROR_DS_TIM_INVALID_SCHEMA_OPTION,
										"column = "
											<< messageSchema->getColumnName(i)
												   .c_str()
											<< " : "
											<< "threshhold of compression is "
											   "different. old = "
											<< oldThreshhold
											<< ", new = " << threshhold);
								}
								if (oldRate != rate) {
									GS_THROW_USER_ERROR(
										GS_ERROR_DS_TIM_INVALID_SCHEMA_OPTION,
										"column = "
											<< messageSchema->getColumnName(i)
												   .c_str()
											<< " : "
											<< "rate of compression is "
											   "different. old = "
											<< oldRate << ", new = " << rate);
								}
								if (oldSpan != span) {
									GS_THROW_USER_ERROR(
										GS_ERROR_DS_TIM_INVALID_SCHEMA_OPTION,
										"column = "
											<< messageSchema->getColumnName(i)
												   .c_str()
											<< " : "
											<< "span of compression is "
											   "different. old = "
											<< oldSpan << ", new = " << span);
								}
							}
							else {
								GS_THROW_USER_ERROR(
									GS_ERROR_DS_TIM_INVALID_SCHEMA_OPTION,
									"column = "
										<< messageSchema->getColumnName(i)
											   .c_str()
										<< " : "
										<< "type of compression is different. "
										   "old = NONE , new = HiCompression");
							}
						}
					}
				}
			}
		}
	}
	if (messageSchema->getContainerExpirationInfo().info_.duration_ != INT64_MAX && 
		messageSchema->getExpirationInfo().duration_ != INT64_MAX) {
		GS_THROW_USER_ERROR(
			GS_ERROR_DS_DS_SCHEMA_INVALID, 
			"row and partiion expiration can not be defined at the same time");
	}
	return;
}

void TimeSeries::getSubTimeSeriesList(TransactionContext &txn,
	util::XArray<SubTimeSeriesInfo> &subTimeSeriesList, bool forUpdate, bool indexUpdate) {
	BtreeMap::SearchContext sc (txn.getDefaultAllocator(), ColumnInfo::ROW_KEY_COLUMN_ID);
	sc.setLimit(MAX_RESULT_SIZE);
	getSubTimeSeriesList(txn, sc, subTimeSeriesList, forUpdate, indexUpdate);
}

void TimeSeries::getSubTimeSeriesList(TransactionContext &txn,
	BtreeMap::SearchContext &sc,
	util::XArray<SubTimeSeriesInfo> &subTimeSeriesList, bool forUpdate, bool indexUpdate) {


	if (isExpired(txn)) {
		return;
	}

	Timestamp currentExpiredTime = getCurrentExpiredTime(txn);
	if (forUpdate) {
		expireSubTimeSeries(txn, currentExpiredTime, indexUpdate);
	}

	Timestamp scBeginTime = -1;
	Timestamp scEndTime = MAX_TIMESTAMP;
	if (sc.getKeyColumnNum() == 1 && sc.getScColumnId() == ColumnInfo::ROW_KEY_COLUMN_ID) {
		const Timestamp *startKey = sc.getStartKey<Timestamp>();
		if (startKey != NULL) {
			scBeginTime = *startKey;
		}
		const Timestamp *endKey = sc.getEndKey<Timestamp>();
		if (endKey != NULL) {
			scEndTime = *endKey;
		}
	}
	else {
		for (uint32_t c = 0; c < sc.getConditionNum(); c++) {
			if (sc.getCondition(c).columnId_ ==
				ColumnInfo::ROW_KEY_COLUMN_ID) {
				if (sc.getCondition(c).opType_ == DSExpression::GE ||
					sc.getCondition(c).opType_ == DSExpression::GT) {
					Timestamp condBeginTime =
						*reinterpret_cast<const Timestamp *>(
							sc.getCondition(c).value_);
					if (scBeginTime < condBeginTime) {
						scBeginTime = condBeginTime;
					}
				}
				else if (sc.getCondition(c).opType_ ==
							 DSExpression::LE ||
						 sc.getCondition(c).opType_ ==
							 DSExpression::LT) {
					Timestamp condEndTime =
						*reinterpret_cast<const Timestamp *>(
							sc.getCondition(c).value_);
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

	if (sc.getResumeStatus()  != BaseIndex::SearchContext::NOT_RESUME && 
		sc.getSuspendRowId() != UNDEF_ROWID &&
		scBeginTime < sc.getSuspendRowId()) {
		scBeginTime = sc.getSuspendRowId();
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

void TimeSeries::getExpirableSubTimeSeriesList(TransactionContext &txn, 
	Timestamp expirableTime, util::XArray<SubTimeSeriesInfo> &subTimeSeriesList) {

	for (int64_t i = 0;
		 i < static_cast<int64_t>(subTimeSeriesListHead_->getNum()); i++) {
		const SubTimeSeriesImage &subTimeSeriesImage =
			*(subTimeSeriesListHead_->get(txn, i));
		SubTimeSeriesInfo subInfo(subTimeSeriesImage, i);
		SubTimeSeries subContainer(txn, subInfo, this);
		ChunkKey chunkKey = subContainer.getRowAllcateStrategy().chunkKey_;
		Timestamp subExpireTime = DataStore::convertChunkKey2Timestamp(chunkKey);
		if (subExpireTime <= expirableTime) {
			subTimeSeriesList.push_back(subInfo);
		} else {
			break;
		}
	}
}

void TimeSeries::getRuntimeSubTimeSeriesList(TransactionContext &txn,
	util::XArray<SubTimeSeriesInfo> &subTimeSeriesList, bool forUpdate) {
	if (isExpired(txn)) {
		return;
	}

	Timestamp expiredTime = getCurrentExpiredTime(txn);
	if (forUpdate) {
		bool isIndexUpdate = false;
		expireSubTimeSeries(txn, expiredTime, isIndexUpdate);
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
	RowArray mvccRowArray(txn, this);
	for (itr = subTimeSeriesList.begin(); itr != subTimeSeriesList.end();
		 itr++) {
		SubTimeSeries subContainer(txn, *itr, this);
		StackAllocAutoPtr<BtreeMap> rowIdMap(
			txn.getDefaultAllocator(), subContainer.getRowIdMap(txn));
		rowIdMap.get()->search(txn, sc, normalOIdList);
		{
			util::XArray<std::pair<TransactionId, MvccRowImage> >
				mvccKeyValueList(txn.getDefaultAllocator());
			util::XArray<std::pair<TransactionId, MvccRowImage> >::iterator
				mvccItr;
			StackAllocAutoPtr<BtreeMap> mvccMap(
				txn.getDefaultAllocator(), subContainer.getMvccMap(txn));
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
				const Operator *sortOp =
					&ComparatorTable::ltTable_[COLUMN_TYPE_TIMESTAMP][COLUMN_TYPE_TIMESTAMP];
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
}

void TimeSeries::insertRowIdMap(
	TransactionContext &, BtreeMap *, const void *, OId) {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
}

void TimeSeries::insertMvccMap(TransactionContext& txn, BtreeMap* map,
	TransactionId tId, MvccRowImage& mvccImage) {
	bool isCaseSensitive = true;
	int32_t status =
		map->insert<TransactionId, MvccRowImage>(txn, tId, mvccImage, isCaseSensitive);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		setDirty();
		baseContainerImage_->mvccMapOId_ = map->getBaseOId();
	}
}

void TimeSeries::insertValueMap(TransactionContext& , ValueMap &,
	const void* , OId , bool ) {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
}

void TimeSeries::updateRowIdMap(
	TransactionContext &, BtreeMap *, const void *, OId, OId) {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
}

void TimeSeries::updateMvccMap(TransactionContext& txn, BtreeMap* map,
	TransactionId tId, MvccRowImage& oldMvccImage, MvccRowImage& newMvccImage) {
	bool isCaseSensitive = true;
	int32_t status = map->update<TransactionId, MvccRowImage>(
		txn, tId, oldMvccImage, newMvccImage, isCaseSensitive);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		setDirty();
		baseContainerImage_->mvccMapOId_ = map->getBaseOId();
	}
}

void TimeSeries::updateValueMap(TransactionContext& , ValueMap &,
	const void* , OId , OId , bool ) {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
}

void TimeSeries::removeRowIdMap(
	TransactionContext &, BtreeMap *, const void *, OId) {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
}

void TimeSeries::removeMvccMap(TransactionContext& txn, BtreeMap* map,
	TransactionId tId, MvccRowImage& mvccImage) {
	bool isCaseSensitive = true;
	int32_t status =
		map->remove<TransactionId, MvccRowImage>(txn, tId, mvccImage, isCaseSensitive);
	if ((status & BtreeMap::ROOT_UPDATE) != 0) {
		setDirty();
		baseContainerImage_->mvccMapOId_ = map->getBaseOId();
	}
}

void TimeSeries::removeValueMap(TransactionContext& , ValueMap &,
	const void* , OId , bool ) {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
}

void TimeSeries::updateIndexData(TransactionContext &, const IndexData &) {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
}

bool TimeSeries::isRuntime(TransactionContext& txn) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	util::XArray<SubTimeSeriesInfo> subInfoList(alloc);
	getRuntimeSubTimeSeriesList(txn, subInfoList, false);

	return !subInfoList.empty();
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
		TransactionContext& txn, RowArray::Row &row) {
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

RowId TimeSeries::getScanStartRowId(Timestamp expiredTime) {
	if (expiredTime != MINIMUM_EXPIRED_TIMESTAMP) {
		return expiredTime;
	}
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
	StackAllocAutoPtr<BtreeMap> rowIdMap(
			txn.getDefaultAllocator(), getRowIdMap(txn));
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


util::String TimeSeries::getBibInfo(TransactionContext &txn, const char* dbName) {
	util::NormalOStringStream strstrm;
	util::XArray<SubTimeSeriesInfo> subTimeSeriesList(
		txn.getDefaultAllocator());
	util::XArray<SubTimeSeriesInfo>::iterator itr;

	if (subTimeSeriesListHead_->getNum() == 0) {
		strstrm << getBibInfoImpl(txn, dbName, 0, true);
	}
	for (int64_t i = 0;
		 i < static_cast<int64_t>(subTimeSeriesListHead_->getNum()); i++) {
		if (i != 0) {
			strstrm << "," << std::endl;
		}
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		const SubTimeSeriesImage &subTimeSeriesImage =
			*(subTimeSeriesListHead_->get(txn, i));
		SubTimeSeriesInfo subInfo(subTimeSeriesImage, i);
		SubTimeSeries subContainer(txn, subInfo, this);
		strstrm << subContainer.getBibInfo(txn, dbName);
	}
	util::String result(txn.getDefaultAllocator());
	result = strstrm.str().c_str();
	return result;
}

void TimeSeries::getActiveTxnList(
	TransactionContext &txn, util::Set<TransactionId> &txnList) {

	getActiveTxnListImpl(txn, txnList);

	util::XArray<SubTimeSeriesInfo> subTimeSeriesList(
		txn.getDefaultAllocator());
	util::XArray<SubTimeSeriesInfo>::iterator itr;
	getSubTimeSeriesList(txn, subTimeSeriesList, false);
	for (itr = subTimeSeriesList.begin(); itr != subTimeSeriesList.end();
		 itr++) {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		SubTimeSeries subContainer(txn, *itr, this);
		subContainer.getActiveTxnListImpl(txn, txnList);
	}
}
void TimeSeries::getErasableList(TransactionContext &txn, Timestamp erasableTimeUpperLimit, util::XArray<ArchiveInfo> &list) {
	Timestamp erasableTimeLowerLimit = getDataStore()->getLatestBatchFreeTime(txn);
	if (erasableTimeLowerLimit < dataStore_->getConfig().getErasableExpiredTime()) {
		erasableTimeLowerLimit = dataStore_->getConfig().getErasableExpiredTime();
	}
	ExpireType type = getExpireType();
	if (type == TABLE_EXPIRE) {
		ArchiveInfo info;
		{
			ExpireIntervalCategoryId expireCategoryId = DEFAULT_EXPIRE_CATEGORY_ID;
			ChunkKey chunkKey = UNDEF_CHUNK_KEY;
			calcChunkKey(getContainerExpirationEndTime(), 
				getContainerExpirationDutation(),
				expireCategoryId, chunkKey);

			info.start_ = getContainerExpirationStartTime();
			info.end_ = getContainerExpirationEndTime();
			info.expired_ = getContainerExpirationTime();
			info.erasable_ = DataStore::convertChunkKey2Timestamp(chunkKey);
			if (erasableTimeLowerLimit >= info.erasable_ || info.erasable_ > erasableTimeUpperLimit) {
				return;
			}
			if (subTimeSeriesListHead_->getNum() == 0) {
				info.rowIdMapOId_ = UNDEF_OID;
				info.mvccMapOId_ = UNDEF_OID;
			} else {
				assert(subTimeSeriesListHead_->getNum() == 1);
				util::StackAllocator::Scope scope(txn.getDefaultAllocator());
				const SubTimeSeriesImage &subTimeSeriesImage =
					*(subTimeSeriesListHead_->get(txn, 0));
				info.rowIdMapOId_ = subTimeSeriesImage.rowIdMapOId_;
				info.mvccMapOId_ = subTimeSeriesImage.mvccMapOId_;
			}
		}
		list.push_back(info);
	} else if (type == ROW_EXPIRE) {
		const ExpirationInfo &expirationInfo = getExpirationInfo();
		for (int64_t i = 0;
			 i < static_cast<int64_t>(subTimeSeriesListHead_->getNum()); i++) {
			ArchiveInfo info;
			{
				util::StackAllocator::Scope scope(txn.getDefaultAllocator());
				const SubTimeSeriesImage &subTimeSeriesImage =
					*(subTimeSeriesListHead_->get(txn, i));
				SubTimeSeriesInfo subInfo(subTimeSeriesImage, i);
				SubTimeSeries subContainer(txn, subInfo, this);

				info.start_ = subContainer.getStartTime();
				info.end_ = subContainer.getEndTime();
				info.expired_ = info.end_ + expirationInfo.duration_;
				info.erasable_ = DataStore::convertChunkKey2Timestamp(subContainer.getRowAllcateStrategy().chunkKey_);
				info.rowIdMapOId_ = subTimeSeriesImage.rowIdMapOId_;
				info.mvccMapOId_ = subTimeSeriesImage.mvccMapOId_;
				if (erasableTimeLowerLimit >= info.erasable_) {
					continue;
				} else if (info.erasable_ > erasableTimeUpperLimit) {
					break;
				}
			}
			list.push_back(info);
		}
	}
}
ExpireType TimeSeries::getExpireType() const {
	if (getContainerExpirationDutation() != INT64_MAX) {
		assert(getExpirationInfo().duration_ == INT64_MAX);
		return TABLE_EXPIRE;
	} else if (getExpirationInfo().duration_ != INT64_MAX) {
		return ROW_EXPIRE;
	}
	return NO_EXPIRE;
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
	bool withUncommitted = true;
	getIndexList(txn, withUncommitted, parentIndexList);

	util::XArray<SubTimeSeriesInfo> subTimeSeriesList(
		txn.getDefaultAllocator());
	util::XArray<SubTimeSeriesInfo>::iterator itr;
	getSubTimeSeriesList(txn, subTimeSeriesList, false);
	for (itr = subTimeSeriesList.begin(); itr != subTimeSeriesList.end();
		 itr++) {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		SubTimeSeries subContainer(txn, *itr, this);
		std::string subErrorMessage;
		uint64_t countRowNum = 0;
		isValid = subContainer.validateImpl<TimeSeries>(
			txn, subErrorMessage, preRowId, countRowNum, isRowRateCheck);
		if (!isValid) {
			isValid = false;
			strstrm << subErrorMessage;
			break;
		}
		totalCountRowNum += countRowNum;

		util::XArray<IndexData> childIndexList(txn.getDefaultAllocator());
		subContainer.getIndexList(txn, withUncommitted, childIndexList);
		if (parentIndexList.size() != childIndexList.size()) {
			isValid = false;
			strstrm << "inValid IndexSize: parentIndexListSize("
					<< parentIndexList.size() << ") != childeIndexListSize("
					<< childIndexList.size() << ")" << std::endl;
			break;
		}
		for (size_t i = 0; i < parentIndexList.size(); i++) {
			if (!(parentIndexList[i].columnIds_->size() == childIndexList[i].columnIds_->size()) &&
				std::equal(parentIndexList[i].columnIds_->begin(), 
				parentIndexList[i].columnIds_->end(), childIndexList[i].columnIds_->begin())) {
				isValid = false;
				strstrm << "inValid columnId: parentIndex first ColumnId("
						<< parentIndexList[i].columnIds_->at(0)
						<< ") != childeIndex first ColumnId("
						<< childIndexList[i].columnIds_->at(0) << ")" << std::endl;
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
		SubTimeSeries subContainer(txn, *itr, this);
		strstrm << "===SubTimeSeries Dump===" << std::endl;
		strstrm << subContainer.dump(txn);
	}
	return strstrm.str();
}


