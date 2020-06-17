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
	@brief Implementation of Container base class
*/
#include "collection.h"
#include "hash_map.h"
#include "time_series.h"
#include "gis_geometry.h"
#include "rtree_map.h"
#include "util/trace.h"
#include "btree_map.h"
#include "data_store.h"
#include "data_store_common.h"
#include "transaction_context.h"
#include "transaction_manager.h"
#include "gs_error.h"
#include "message_schema.h"
#include "result_set.h"
#include "value_processor.h"

#include "picojson.h"

const int8_t BaseContainer::NULL_VALUE = 0;



/*!
	@brief Get list of IndexInfo
*/
void BaseContainer::getIndexInfoList(
	TransactionContext &txn, util::Vector<IndexInfo> &indexInfoList) {
	try {
		bool withUncommitted = false;
		IndexInfo indexInfo(txn.getDefaultAllocator());
		indexInfo.anyNameMatches_ = 1;
		indexInfo.columnIds_.clear();
		indexInfo.anyTypeMatches_ = 1;
		bool isCaseSensitive = false;

		util::Vector<IndexInfo> mismatchList(txn.getDefaultAllocator());
		indexSchema_->getIndexInfoList(txn, this, indexInfo, withUncommitted,
			indexInfoList, mismatchList, isCaseSensitive);

		assert(mismatchList.empty());
	}
	catch (std::exception &e) {
		handleSearchError(txn, e, GS_ERROR_DS_COL_GET_INDEX_INFO_LIST_FAILED);
	}
}

void BaseContainer::getIndexInfoList(
	TransactionContext &txn, MapType mapType, 
	util::Vector<ColumnId> &columnIds,
	util::Vector<IndexInfo> &indexInfoList,
	bool withPartialMatch) {
	try {
		bool withUncommitted = false;
		IndexInfo indexInfo(txn.getDefaultAllocator());
		indexInfo.mapType = mapType;
		indexInfo.anyNameMatches_ = 1;
		indexInfo.columnIds_.clear();
		indexInfo.columnIds_.assign(columnIds.begin(), columnIds.end()); 
		indexInfo.anyTypeMatches_ = 0;
		bool isCaseSensitive = false;

		util::Vector<IndexInfo> mismatchList(txn.getDefaultAllocator());
		indexSchema_->getIndexInfoList(txn, this, indexInfo, withUncommitted,
			indexInfoList, mismatchList, isCaseSensitive, withPartialMatch);
	}
	catch (std::exception &e) {
		handleSearchError(txn, e, GS_ERROR_DS_COL_GET_INDEX_INFO_LIST_FAILED);
	}
}
void BaseContainer::getIndexDataList(
	TransactionContext &txn, MapType mapType, 
	util::Vector<ColumnId> &columnIds,
	util::Vector<IndexData> &indexDataList,
	bool withPartialMatch) {
	try {
		bool withUncommitted = false;
		indexSchema_->getIndexDataList(txn, columnIds, mapType, 
			withUncommitted, indexDataList, withPartialMatch);
	}
	catch (std::exception &e) {
		handleSearchError(txn, e, GS_ERROR_DS_COL_GET_INDEX_INFO_LIST_FAILED);
	}
}
bool BaseContainer::checkRowKeySchema(util::XArray<ColumnType> &columnTypeList) {
	if (columnTypeList.size() != getRowKeyColumnNum()) {
		return false;
	}
	for (size_t i = 0; i < columnTypeList.size(); i++) {
		ColumnInfo &columnInfo = getColumnInfo(static_cast<ColumnId>(i));
		if (columnInfo.getColumnType() != columnTypeList[i]) {
			return false;
		}
	}
	return true;
}

/*!
	@brief Get Container Schema
*/
void BaseContainer::getContainerInfo(TransactionContext &txn,
	util::XArray<uint8_t> &containerSchema, bool optionIncluded, bool internalOptionIncluded) {
	try {
		uint32_t columnNum = getColumnNum();
		containerSchema.push_back(
			reinterpret_cast<uint8_t *>(&columnNum), sizeof(uint32_t));

		for (uint32_t i = 0; i < columnNum; i++) {
			getColumnInfo(i).getSchema(
				txn, *getObjectManager(), containerSchema);
		}

		{
			util::Vector<ColumnId> keyColumnIdList(txn.getDefaultAllocator());
			getKeyColumnIdList(keyColumnIdList);
			int16_t rowKeyNum = static_cast<int16_t>(keyColumnIdList.size());
			containerSchema.push_back(
				reinterpret_cast<uint8_t *>(&rowKeyNum), sizeof(int16_t));
			util::Vector<ColumnId>::iterator itr;
			for (itr = keyColumnIdList.begin(); itr != keyColumnIdList.end(); itr++) {
				int16_t rowKeyColumnId = static_cast<int16_t>(*itr);
				containerSchema.push_back(
					reinterpret_cast<uint8_t *>(&rowKeyColumnId), sizeof(int16_t));
			}
		}

		if (optionIncluded) {
			getCommonContainerOptionInfo(containerSchema);
			getContainerOptionInfo(txn, containerSchema);
			if (internalOptionIncluded) {
				int32_t containerAttribute = getAttribute();
				containerSchema.push_back(
					reinterpret_cast<uint8_t *>(&containerAttribute), sizeof(int32_t));
				TablePartitioningVersionId tablePartitioningVersionId = getTablePartitioningVersionId();
				containerSchema.push_back(
					reinterpret_cast<const uint8_t *>(&tablePartitioningVersionId),
					sizeof(TablePartitioningVersionId));

				const ContainerExpirationInfo *containerExpirationInfo = getContainerExpirationInfo();
				if (getContainerExpirationDutation() != INT64_MAX) {
					int32_t optionType = static_cast<int32_t>(MessageSchema::PARTITION_EXPIRATION);
					containerSchema.push_back(
						reinterpret_cast<const uint8_t *>(&optionType),
						sizeof(int32_t));
					int32_t optionSize = sizeof(Timestamp) + sizeof(int64_t) + sizeof(int64_t);
					containerSchema.push_back(
						reinterpret_cast<const uint8_t *>(&optionSize),
						sizeof(int32_t));
					Timestamp startTime = getContainerExpirationStartTime();
					containerSchema.push_back(
						reinterpret_cast<const uint8_t *>(&startTime),
						sizeof(Timestamp));
					containerSchema.push_back(
						reinterpret_cast<const uint8_t *>(&(containerExpirationInfo->interval_)),
						sizeof(int64_t));
					containerSchema.push_back(
						reinterpret_cast<const uint8_t *>(&(containerExpirationInfo->info_.duration_)),
						sizeof(int64_t));
				}
				{
					int32_t optionType = static_cast<int32_t>(MessageSchema::OPTION_END);
					containerSchema.push_back(
						reinterpret_cast<const uint8_t *>(&optionType),
						sizeof(int32_t));
				}
			}
		}
	}
	catch (std::exception &e) {
		handleSearchError(txn, e, GS_ERROR_DS_COL_GET_COLINFO_FAILED);
	}
}

IndexCursor BaseContainer::getIndexCursor(TransactionContext& txn) {
	util::XArray<MvccRowImage> mvccList(txn.getDefaultAllocator());
	try {
		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not get index cursor. container's status is invalid.");
		}
		TransactionId tId = txn.getId();
		if (baseContainerImage_->mvccMapOId_ != UNDEF_OID) {
			StackAllocAutoPtr<BtreeMap> mvccMap(
				txn.getDefaultAllocator(), getMvccMap(txn));
			TermCondition cond(COLUMN_TYPE_LONG, COLUMN_TYPE_LONG, 
				DSExpression::EQ, UNDEF_COLUMNID, &tId, sizeof(tId));
			BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, MAX_RESULT_SIZE);
			mvccMap.get()->search<TransactionId, MvccRowImage, MvccRowImage>(
				txn, sc, mvccList);
		}

		if (mvccList.empty() ||  mvccList[0].type_  != MVCC_INDEX) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"Index cursor not exist, already finished.");
		} else if (mvccList.size() > 1) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, 
				"index mvcc invalid size = " << mvccList.size()
				<< ", partitionId = " << txn.getPartitionId()
				<< ", txnId = " << txn.getId()
				<< ", containerId = " << getContainerId()
				);
		}

		if (isAlterContainer()) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_COL_LOCK_CONFLICT, 
				"continueCreateIndex : container already locked "
				<< ", partitionId = " << txn.getPartitionId()
				<< ", txnId = " << txn.getId()
				<< ", containerId = " << getContainerId()
				);
		}
	}
	catch (std::exception& e) {
		handleSearchError(txn, e, GS_ERROR_DS_COL_CREATE_INDEX_FAILED);
	}
	return createCursor(txn, mvccList[0]);
}

ContainerCursor BaseContainer::getContainerCursor(TransactionContext& txn) {
	util::XArray<MvccRowImage> mvccList(txn.getDefaultAllocator());
	try {
		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not get changeSchema cursor. container's status is invalid.");
		}
		if (isExpired(txn)) {
			return ContainerCursor();
		}
		TransactionId tId = txn.getId();
		if (baseContainerImage_->mvccMapOId_ != UNDEF_OID) {
			StackAllocAutoPtr<BtreeMap> mvccMap(
				txn.getDefaultAllocator(), getMvccMap(txn));
			TermCondition cond(COLUMN_TYPE_LONG, COLUMN_TYPE_LONG, 
				DSExpression::EQ, UNDEF_COLUMNID, &tId, sizeof(tId));
			BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, MAX_RESULT_SIZE);
			mvccMap.get()->search<TransactionId, MvccRowImage, MvccRowImage>(
				txn, sc, mvccList);
		}

		if (mvccList.empty() ||  mvccList[0].type_  != MVCC_CONTAINER) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"changeSchema cursor not exist, already finished.");
		} else if (mvccList.size() > 1) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, 
				"changeSchema mvcc invalid size = " << mvccList.size()
				<< ", partitionId = " << txn.getPartitionId()
				<< ", txnId = " << txn.getId()
				<< ", containerId = " << getContainerId()
				);
		}
	}
	catch (std::exception& e) {
		handleSearchError(txn, e, GS_ERROR_DS_COL_CREATE_INDEX_FAILED);
	}
	return ContainerCursor(mvccList[0]);
}

/*!
	@brief Newly creates or updates Rows, based on the specified Row object and
   also the Row key specified as needed
*/
void BaseContainer::putRowList(TransactionContext &txn, uint32_t rowSize,
	const uint8_t *rowData, uint64_t numRow, DataStore::PutStatus &status) {
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
			getDataStore()->getValueLimitConfig(), getColumnInfoList(),
			getColumnNum(), const_cast<uint8_t *>(rowData), rowSize, numRow,
			rowFixedDataSize_);
		inputMessageRowStore.reset();

		checkExclusive(txn);

		RowId rowId;
		status = DataStore::NOT_EXECUTED;
		for (uint64_t i = 0; i < numRow; i++) {
			util::StackAllocator::Scope scope(txn.getDefaultAllocator());

			inputMessageRowStore.next();
			DataStore::PutStatus rowStatus;
			bool rowIdSecified = false;
			putRow(txn, &inputMessageRowStore, rowId, rowIdSecified, rowStatus,
				PUT_INSERT_OR_UPDATE);
			if (status != DataStore::UPDATE &&
				rowStatus != DataStore::NOT_EXECUTED) {
				status = rowStatus;
			}
		}
	}
	catch (std::exception &e) {
		handleUpdateError(txn, e, GS_ERROR_DS_COL_PUT_ROWLIST_FAILED);
	}
}

/*!
	@brief Newly creates or updates Trigger
*/
void BaseContainer::putTrigger(
	TransactionContext &txn, const TriggerInfo &info) {
	try {
		setDirty();

		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not put trigger. container's status is invalid.");
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

		TriggerList *trigger = NULL;

		util::XArray<const uint8_t *> newTrigerBinary(
			txn.getDefaultAllocator());

		OId oId = getTriggerOId();
		if (oId == UNDEF_OID) {
			util::XArray<uint8_t> *binaryTrigger =
				ALLOC_NEW(txn.getDefaultAllocator())
					util::XArray<uint8_t>(txn.getDefaultAllocator());
			TriggerInfo::encode(info, *binaryTrigger);
			newTrigerBinary.push_back(
				reinterpret_cast<const uint8_t *>(binaryTrigger->data()));
		}
		else {
			ShareValueList commonContainerSchema(txn, *getObjectManager(), oId);
			trigger = commonContainerSchema.get<TriggerList>(META_TYPE_TRIGGER);
			trigger->createImage(txn, *getObjectManager(), info,
				newTrigerBinary,
				dataStore_->getValueLimitConfig().getLimitSmallSize());
		}

		if (newTrigerBinary.size() > TriggerList::MAX_TRIGGER_LIST_NUM) {
			GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
				"exceeded maximum number of triggers. (num="
					<< newTrigerBinary.size() << ")");
		}

		int64_t triggerHashKey = calcTriggerHashKey(newTrigerBinary);
		OId storeTriggerOId = getDataStore()->getTriggerId(
			txn, txn.getPartitionId(), newTrigerBinary, triggerHashKey);
		getDataStore()->insertTrigger(txn, txn.getPartitionId(),
			newTrigerBinary, triggerHashKey, storeTriggerOId);
		setTriggerOId(storeTriggerOId);

		if (oId != UNDEF_OID) {
			getDataStore()->removeTrigger(txn, txn.getPartitionId(), oId);
		}
	}
	catch (SystemException &e) {
		GS_RETHROW_SYSTEM_ERROR(e,
			"Failed to put trigger "
			"(triggerName="
				<< info.name_ << ", triggerType=" << info.type_
				<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
	catch (UserException &e) {
		try {
			GS_RETHROW_USER_ERROR(e,
				"Failed to put trigger "
				"(triggerName="
					<< info.name_ << ", triggerType=" << info.type_
					<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
		}
		catch (std::exception &e) {
			handleUpdateError(txn, e, GS_ERROR_DS_PUT_TRIGGER_FAILED);
		}
	}
	catch (LockConflictException &e) {
		DS_RETHROW_LOCK_CONFLICT_ERROR(e,
			"Failed to put trigger "
			"(triggerName="
				<< info.name_ << ", triggerType=" << info.type_
				<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e,
			"Failed to put trigger "
			"(triggerName="
				<< info.name_ << ", triggerType=" << info.type_
				<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Update Trigger
*/
void BaseContainer::updateTrigger(TransactionContext &txn, OId oId,
	const util::XArray<const util::String *> &oldColumnNameList,
	const util::XArray<const util::String *> &newColumnNameList) {
	try {
		setDirty();

		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not update trigger. container's status is invalid.");
		}

		if (oId == UNDEF_OID) {
			return;
		}

		if (isAlterContainer()) {
			DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_COL_LOCK_CONFLICT,
				"(pId=" << txn.getPartitionId() << ", containerId=" << getContainerId()
						<< ", txnId=" << txn.getId() << ")");
		}

		util::XArray<const uint8_t *> newTrigerBinary(
			txn.getDefaultAllocator());
		bool isChange = false;
		{
			ShareValueList commonContainerSchema(txn, *getObjectManager(), oId);
			TriggerList *trigger =
				commonContainerSchema.get<TriggerList>(META_TYPE_TRIGGER);

			isChange = trigger->updateImage(txn, *getObjectManager(),
				oldColumnNameList, newColumnNameList, newTrigerBinary);
		}
		if (isChange) {
			int64_t triggerHashKey = calcTriggerHashKey(newTrigerBinary);
			OId storeTriggerOId = getDataStore()->getTriggerId(
				txn, txn.getPartitionId(), newTrigerBinary, triggerHashKey);
			getDataStore()->insertTrigger(txn, txn.getPartitionId(),
				newTrigerBinary, triggerHashKey, storeTriggerOId);
			setTriggerOId(storeTriggerOId);

		}
	}
	catch (std::exception &e) {
		handleUpdateError(txn, e, GS_ERROR_DS_UPDATE_TRIGGER_FAILED);
	}
}

/*!
	@brief Delete Trigger
*/
void BaseContainer::deleteTrigger(TransactionContext &txn, const char *name) {
	try {
		setDirty();

		if (isInvalid()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_STATUS_INVALID,
				"can not delete trigger. container's status is invalid.");
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

		OId oId = getTriggerOId();
		if (oId == UNDEF_OID) {
			return;
		}

		util::XArray<const uint8_t *> newTrigerBinary(
			txn.getDefaultAllocator());
		bool isExist = false;
		{
			ShareValueList commonContainerSchema(txn, *getObjectManager(), oId);
			TriggerList *trigger =
				commonContainerSchema.get<TriggerList>(META_TYPE_TRIGGER);

			isExist = trigger->removeImage(
				txn, *getObjectManager(), name, newTrigerBinary);
		}
		if (isExist) {
			OId storeTriggerOId = UNDEF_OID;
			if (!newTrigerBinary.empty()) {
				int64_t triggerHashKey = calcTriggerHashKey(newTrigerBinary);
				storeTriggerOId = getDataStore()->getTriggerId(
					txn, txn.getPartitionId(), newTrigerBinary, triggerHashKey);
				getDataStore()->insertTrigger(txn, txn.getPartitionId(),
					newTrigerBinary, triggerHashKey, storeTriggerOId);
			}
			setTriggerOId(storeTriggerOId);

			getDataStore()->removeTrigger(txn, txn.getPartitionId(), oId);
		}
	}
	catch (SystemException &e) {
		GS_RETHROW_SYSTEM_ERROR(e,
			"Failed to delete trigger "
			"(triggerName="
				<< name << ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
	catch (UserException &e) {
		try {
			GS_RETHROW_USER_ERROR(e,
				"Failed to delete trigger "
				"(triggerName="
					<< name << ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
		}
		catch (std::exception &e) {
			handleUpdateError(txn, e, GS_ERROR_DS_DELETE_TRIGGER_FAILED);
		}
	}
	catch (LockConflictException &e) {
		DS_RETHROW_LOCK_CONFLICT_ERROR(e,
			"Failed to delete trigger "
			"(triggerName="
				<< name << ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e,
			"Failed to delete trigger "
			"(triggerName="
				<< name << ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Get list of Trigger
*/
void BaseContainer::getTriggerList(
	TransactionContext &txn, util::XArray<const uint8_t *> &triggerList) {
	try {
		OId triggerOId = getTriggerOId();
		if (triggerOId == UNDEF_OID) {
			return;
		}

		ShareValueList commonContainerSchema(
			txn, *getObjectManager(), triggerOId);
		TriggerList *trigger =
			commonContainerSchema.get<TriggerList>(META_TYPE_TRIGGER);

		trigger->getList(txn, *getObjectManager(), triggerList);
	}
	catch (std::exception &e) {
		handleSearchError(txn, e, GS_ERROR_DS_GET_TRIGGER_FAILED);
	}
}

/*!
	@brief Get list of locked RowId
*/
void BaseContainer::getLockRowIdList(TransactionContext &txn,
	ResultSet &resultSet, util::XArray<RowId> &idList) {
	if (isAlterContainer()) {
		DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_COL_LOCK_CONFLICT,
			"(pId=" << txn.getPartitionId() << ", containerId=" << getContainerId()
					<< ", txnId=" << txn.getId() << ")");
	}

	setDummyMvccImage(txn);

	if (resultSet.getResultNum() > 0) {
		if (resultSet.hasRowId()) {
			lockIdList(txn, *(resultSet.getOIdList()), idList);
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_CANNOT_LOCK_ROW,
				"Cannot lock rows for update in aggregation/selection");
		}
	}
}

template <typename R, typename T>
void BaseContainer::searchColumnIdIndex(TransactionContext &txn, MapType mapType, 
	typename T::SearchContext &sc, util::XArray<OId> &resultList, 
	OutputOrder outputOrder) {

	bool ignoreTxnCheck = false;
	util::XArray<OId> normalRowList(txn.getDefaultAllocator());
	util::XArray<OId> mvccRowList(txn.getDefaultAllocator());
	searchColumnIdIndex<R, T>(txn, mapType, sc, normalRowList, mvccRowList, outputOrder, ignoreTxnCheck);

	if (!mvccRowList.empty() && outputOrder != ORDER_UNDEFINED) {
		util::XArray<OId> mergeList(txn.getDefaultAllocator());
		ColumnInfo &targetColumnInfo = getColumnInfo(sc.getScColumnId());
		mergeRowList<R>(txn, targetColumnInfo, mvccRowList, false, normalRowList,
			true, resultList, outputOrder);
	}
	else {
		resultList.swap(normalRowList);
		resultList.insert(resultList.begin(), mvccRowList.begin(), mvccRowList.end());
	}
	if (resultList.size() > sc.getLimit()) {
		resultList.resize(sc.getLimit());
	}
}


template <typename R, typename T>
void BaseContainer::searchColumnIdIndex(TransactionContext &txn, MapType mapType, 
	typename T::SearchContext &sc, util::XArray<OId> &normalRowList, 
	util::XArray<OId> &mvccRowList, OutputOrder outputOrder, bool ignoreTxnCheck) {


	{
		ResultSize limitBackup = sc.getLimit();
		if (outputOrder != ORDER_UNDEFINED) {
			sc.setLimit(MAX_RESULT_SIZE);
		}
		bool isCheckOnly = sc.getResumeStatus() != BaseIndex::SearchContext::NOT_RESUME;
		searchMvccMap<R, typename T::SearchContext>(txn, sc, mvccRowList, isCheckOnly, ignoreTxnCheck);

		sc.setLimit(limitBackup);

		if (outputOrder == ORDER_UNDEFINED && mvccRowList.size() >= sc.getLimit()) {
			mvccRowList.resize(sc.getLimit());
			return;
		}
	}

	{
		util::XArray<OId> oIdList(txn.getDefaultAllocator());
		ResultSize limitBackup = sc.getLimit();
		if (sc.getRestConditionNum() > 0 || !isExclusive()) {
			sc.setLimit(MAX_RESULT_SIZE);
		} else {
			sc.setLimit(sc.getLimit() - mvccRowList.size());
		}
		bool withUncommitted = false;
		const util::Vector<ColumnId> &columnIds = sc.getColumnIds();
		IndexData indexData(txn.getDefaultAllocator());
		getIndexData(txn, columnIds, mapType, withUncommitted, indexData);
		ValueMap valueMap(txn, this, indexData);
		valueMap.search<T>(txn, sc, oIdList, outputOrder);
		sc.setLimit(limitBackup);

		ContainerValue containerValue(txn.getPartitionId(), *getObjectManager());
		util::XArray<OId>::iterator itr;
		if (sc.getRestConditionNum() > 0 || (!isExclusive() && !ignoreTxnCheck)) {
			util::Vector<TermCondition> condList(txn.getDefaultAllocator());
			sc.getConditionList(condList, BaseIndex::SearchContext::COND_OTHER);
			RowArray rowArray(txn, reinterpret_cast<R *>(this));
			for (itr = oIdList.begin(); itr != oIdList.end(); itr++) {
				rowArray.load(
					txn, *itr, reinterpret_cast<R *>(this), OBJECT_READ_ONLY);
				RowArray::Row row(rowArray.getRow(), &rowArray);

				if (!ignoreTxnCheck && txn.getId() != row.getTxnId() && !row.isFirstUpdate() &&
					txn.getManager().isActiveTransaction(
						txn.getPartitionId(), row.getTxnId())) {
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
				if (isMatch) {
					normalRowList.push_back(rowArray.getOId());
					if (normalRowList.size() + mvccRowList.size() == sc.getLimit()) {
						break;
					}
				}
			}
		}
		else {
			normalRowList.swap(oIdList);
		}
	}
}

/*!
	@brief Search Index of column
*/
void BaseContainer::searchColumnIdIndex(TransactionContext &txn,
	BtreeMap::SearchContext &sc, util::XArray<OId> &normalRowList,
	util::XArray<OId> &mvccRowList) {

	bool ignoreTxnCheck = true;
	OutputOrder outputOrder = ORDER_UNDEFINED;

	MapType mapType = MAP_TYPE_BTREE;
	switch (getContainerType()) {
	case COLLECTION_CONTAINER:
		searchColumnIdIndex<Collection, BtreeMap>(
			txn, mapType, sc, normalRowList, mvccRowList, outputOrder, ignoreTxnCheck);
		break;
	case TIME_SERIES_CONTAINER:
		searchColumnIdIndex<TimeSeries, BtreeMap>(
			txn, mapType, sc, normalRowList, mvccRowList, outputOrder, ignoreTxnCheck);
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_CONTAINER_TYPE_UNKNOWN, "");
		break;
	}
}

/*!
	@brief Search Index of column
*/
void BaseContainer::searchColumnIdIndex(TransactionContext &txn,
	BtreeMap::SearchContext &sc, util::XArray<OId> &resultList,
	OutputOrder outputOrder) {

	MapType mapType = MAP_TYPE_BTREE;
	switch (getContainerType()) {
	case COLLECTION_CONTAINER:
		searchColumnIdIndex<Collection, BtreeMap>(
			txn, mapType, sc, resultList, outputOrder);
		break;
	case TIME_SERIES_CONTAINER:
		searchColumnIdIndex<TimeSeries, BtreeMap>(
			txn, mapType, sc, resultList, outputOrder);
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_CONTAINER_TYPE_UNKNOWN, "");
		break;
	}
}


/*!
	@brief Search Hash Index of column
*/
void BaseContainer::searchColumnIdIndex(TransactionContext &txn,
	HashMap::SearchContext &sc, util::XArray<OId> &resultList) {
	MapType mapType = MAP_TYPE_HASH;
	switch (getContainerType()) {
	case COLLECTION_CONTAINER:
		searchColumnIdIndex<Collection, HashMap>(
			txn, mapType, sc, resultList, ORDER_UNDEFINED);
		break;
	case TIME_SERIES_CONTAINER:
		searchColumnIdIndex<TimeSeries, HashMap>(
			txn, mapType, sc, resultList, ORDER_UNDEFINED);
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_CONTAINER_TYPE_UNKNOWN, "");
		break;
	}
}

/*!
	@brief Search Rtree Index of column
*/
void BaseContainer::searchColumnIdIndex(TransactionContext &txn,
	RtreeMap::SearchContext &sc, util::XArray<OId> &resultList) {
	MapType mapType = MAP_TYPE_SPATIAL;
	switch (getContainerType()) {
	case COLLECTION_CONTAINER:
		searchColumnIdIndex<Collection, RtreeMap>(
			txn, mapType, sc, resultList, ORDER_UNDEFINED);
		break;
	case TIME_SERIES_CONTAINER:
		searchColumnIdIndex<TimeSeries, RtreeMap>(
			txn, mapType, sc, resultList, ORDER_UNDEFINED);
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_CONTAINER_TYPE_UNKNOWN, "");
		break;
	}
}

/*!
	@brief Get list of Row in Message format
*/
void BaseContainer::getRowList(TransactionContext &txn,
	util::XArray<OId> &oIdList, ResultSize limit, ResultSize &resultNum,
	MessageRowStore *messageRowStore, bool isWithRowId, ResultSize startPos) {
	try {
		BaseContainer *container = const_cast<BaseContainer *>(this);
		switch (getContainerType()) {
		case COLLECTION_CONTAINER:
			container->getRowListImpl<Collection>(txn, oIdList, limit,
				resultNum, messageRowStore, isWithRowId, startPos);
			break;
		case TIME_SERIES_CONTAINER:
			container->getRowListImpl<TimeSeries>(txn, oIdList, limit,
				resultNum, messageRowStore, isWithRowId, startPos);
			break;
		default:
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_CONTAINER_TYPE_UNKNOWN, "");
			break;
		}
	}
	catch (std::exception &e) {
		handleSearchError(txn, e, GS_ERROR_DS_CON_GET_ROW_LIST_FAILED);
	}
}

/*!
	@brief Get Row OId from RowId
*/
void BaseContainer::getRowIdList(TransactionContext &txn,
	util::XArray<OId> &oIdList, util::XArray<RowId> &rowIdList) {
	try {
		BaseContainer *container = const_cast<BaseContainer *>(this);
		switch (getContainerType()) {
		case COLLECTION_CONTAINER:
			container->getRowIdListImpl<Collection>(txn, oIdList, rowIdList);
			break;
		case TIME_SERIES_CONTAINER:
			container->getRowIdListImpl<TimeSeries>(txn, oIdList, rowIdList);
			break;
		default:
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_CONTAINER_TYPE_UNKNOWN, "");
			break;
		}
	}
	catch (std::exception &e) {
		handleSearchError(txn, e, GS_ERROR_DS_CON_GET_ROW_ID_LIST_FAILED);
	}
}

/*!
	@brief Get RowId from Row OId
*/
void BaseContainer::getOIdList(TransactionContext &txn, uint64_t start,
	uint64_t limit, uint64_t &skipCount, util::XArray<RowId> &rowIdList,
	util::XArray<OId> &oIdList) {
	try {
		searchRowIdIndex(txn, start, limit, rowIdList, oIdList, skipCount);
	}
	catch (std::exception &e) {
		handleSearchError(txn, e, GS_ERROR_DS_CON_GET_OID_LIST_FAILED);
	}
}

/*!
	@brief Free Trigger Object
*/
void BaseContainer::finalizeTrigger(TransactionContext &txn,
	ObjectManager &objectManager, ShareValueList *commonContainerSchema) {
	TriggerList *trigger =
		commonContainerSchema->get<TriggerList>(META_TYPE_TRIGGER);
	if (trigger != NULL) {
		trigger->finalize(txn, objectManager);
	}
}

BaseContainer::RowArray *BaseContainer::getCacheRowArray(TransactionContext &txn) {
	if (rowArrayCache_ == NULL) {
		rowArrayCache_ = ALLOC_NEW(txn.getDefaultAllocator())
			RowArray(txn, this);
	}
	return rowArrayCache_;
}

uint32_t BaseContainer::getRowKeyFixedDataSize(util::StackAllocator &alloc) const {
	util::Vector<ColumnId> columnIds(alloc);
	for (ColumnId i = 0; i < getRowKeyColumnNum(); i++) {
		columnIds.push_back(i);
	}
	TreeFuncInfo funcInfo(alloc);
	funcInfo.initialize(columnIds, columnSchema_);

	ColumnSchema *rowKeySchema = funcInfo.getColumnSchema();
	uint32_t rowFixedDataSize =
		ValueProcessor::calcNullsByteSize(rowKeySchema->getColumnNum()) +
		rowKeySchema->getRowFixedColumnSize();
	if (rowKeySchema->getVariableColumnNum()) {
		rowFixedDataSize += sizeof(OId);
	}
	return rowFixedDataSize;
}

void BaseContainer::getFields(TransactionContext &txn, 
	MessageRowStore* messageRowStore, 
	util::Vector<ColumnId> &columnIdList, util::XArray<KeyData> &fields) {
	UNUSED_VARIABLE(txn);
	for (util::Vector<ColumnId>::iterator itr = columnIdList.begin();
		itr != columnIdList.end(); itr++) {
		ColumnInfo &columnInfo = getColumnInfo(*itr);
		const uint8_t* data = NULL;
		uint32_t size = 0;
		if (!messageRowStore->isNullValue(columnInfo.getColumnId())) {
			messageRowStore->getField(columnInfo.getColumnId(), data, size);
			if (columnInfo.isVariable()) {
				uint32_t encodeSize = ValueProcessor::getEncodedVarSize(data);
				uint32_t varSize = ValueProcessor::decodeVarSize(data);
				data = const_cast<uint8_t *>(data) + encodeSize;
				size = varSize;
			}
		}
		KeyData keyData(data, size);
		fields.push_back(keyData);
	}
}

ColumnInfo *BaseContainer::getRowKeyColumnInfoList(TransactionContext &txn) {
	if (getRowKeyColumnNum() > 1) {
		util::StackAllocator &alloc = txn.getDefaultAllocator();

		util::Vector<ColumnId> keyColumnIdList(alloc);
		getKeyColumnIdList(keyColumnIdList);

		TreeFuncInfo *funcInfo = ALLOC_NEW(alloc) TreeFuncInfo(alloc);
		funcInfo->initialize(keyColumnIdList, columnSchema_, false);
		return funcInfo->getColumnSchema()->getColumnInfoList();
	} else {
		return getColumnInfoList();
	}
}

void BaseContainer::getRowKeyFields(TransactionContext &txn, uint32_t rowKeySize, const uint8_t *rowKey, util::XArray<KeyData> &fields) {
	if (getRowKeyColumnNum() > 1) {
		const bool VALIDATE_ROW_IMAGE = true;
		InputMessageRowStore inputMessageRowStore(
			getDataStore()->getValueLimitConfig(), getRowKeyColumnInfoList(txn),
			getRowKeyColumnNum(),
			const_cast<uint8_t *>(rowKey),
			rowKeySize, 
			1, getRowKeyFixedDataSize(txn.getDefaultAllocator()), 
			VALIDATE_ROW_IMAGE);
		for (ColumnId i = 0; i < getRowKeyColumnNum(); i++) {
			ColumnInfo &columnInfo = getColumnInfo(i);
			const uint8_t *data;
			uint32_t size;
			inputMessageRowStore.getField(i, data, size);
			if (columnInfo.isVariable()) {
				uint32_t encodeSize = ValueProcessor::getEncodedVarSize(data);
				uint32_t varSize = ValueProcessor::decodeVarSize(data);
				data = const_cast<uint8_t *>(data) + encodeSize;
				size = varSize;
			}
			KeyData keyData(data, size);
			fields.push_back(keyData);
		}
	} else {
		ColumnInfo &columnInfo = getColumnInfo(ColumnInfo::ROW_KEY_COLUMN_ID);
		if (columnInfo.getColumnType() == COLUMN_TYPE_STRING) {
			StringCursor stringCusor(const_cast<uint8_t *>(rowKey));
			rowKey = stringCusor.str();
			rowKeySize = stringCusor.stringLength();
			if (rowKeySize >
				getDataStore()->getValueLimitConfig()
					.getLimitSmallSize()) {  
				GS_THROW_USER_ERROR(GS_ERROR_QP_ROW_KEY_INVALID, "");
			}
		}
		else if (columnInfo.getColumnType() == COLUMN_TYPE_TIMESTAMP) {
			if (!ValueProcessor::validateTimestamp(
					*reinterpret_cast<const Timestamp *>(rowKey))) {
				GS_THROW_USER_ERROR(GS_ERROR_QP_TIMESTAMP_RANGE_INVALID,
					"Timestamp of rowKey out of range (rowKey=" << rowKey
																<< ")");
			}
		}
		KeyData keyData(rowKey, rowKeySize);
		fields.push_back(keyData);
	}
}
/*!
	@brief Handle Exception of update phase
*/
void BaseContainer::handleUpdateError(
	TransactionContext &txn, std::exception &, ErrorCode errorCode) {
	try {
		throw;
	}
	catch (SystemException &e) {
		handleInvalidError(txn, e, errorCode);
	}
	catch (UserException &e) {
		if (e.getErrorCode() == GS_ERROR_CM_NO_MEMORY ||
			e.getErrorCode() == GS_ERROR_CM_MEMORY_LIMIT_EXCEEDED ||
			e.getErrorCode() == GS_ERROR_CM_SIZE_LIMIT_EXCEEDED) {
			GS_RETHROW_SYSTEM_ERROR(e, "");
		}
		else if (e.getErrorCode(0) == GS_ERROR_DS_CON_STATUS_INVALID) {
			GS_RETHROW_USER_ERROR(e, "can not operate invalid container");
		}
		else {
			GS_RETHROW_USER_ERROR(e, "");
		}
	}
	catch (LockConflictException &e) {
		DS_RETHROW_LOCK_CONFLICT_ERROR(e, "");
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Handle Exception of search phase
*/
void BaseContainer::handleSearchError(
	TransactionContext &txn, std::exception &, ErrorCode errorCode) {
	try {
		throw;
	}
	catch (SystemException &e) {
		handleInvalidError(txn, e, errorCode);
	}
	catch (UserException &e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
	catch (LockConflictException &e) {
		DS_RETHROW_LOCK_CONFLICT_ERROR(e, "");
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Handle SystemException
*/
void BaseContainer::handleInvalidError(
	TransactionContext &txn, SystemException &e, ErrorCode) {
	bool isContainerInvalid = false;
	try {
		int32_t errorCode = e.getErrorCode(e.getMaxDepth());
		if ((errorCode >= GS_ERROR_DS_UNDEFINED &&
				errorCode < GS_ERROR_OM_UNDEFINED) ||
			(errorCode >= GS_ERROR_QP_UNDEFINED &&
				errorCode < GS_ERROR_LM_WRITE_LOG_FAILED) ||
			(errorCode >= GS_ERROR_TQ_NOT_DEFINED_ERROR &&
				errorCode < GS_ERROR_RM_UNDEFINED) ||
			(errorCode == GS_ERROR_OM_INVALID_OID ||
				errorCode == GS_ERROR_OM_INVALID_OBJECT)) {
			isContainerInvalid = true;
		}
	}
	catch (std::exception &ex) {
		GS_RETHROW_SYSTEM_ERROR(ex,
			GS_EXCEPTION_MERGE_MESSAGE(ex, "Invalid Check Operation Error"));
	}
	if (isContainerInvalid) {
		try {
			if (getBaseOId() != UNDEF_OID) {
				setDirty();
				setContainerInvalid();
			}

			const FullContainerKey containerKey = getContainerKey(txn);
			util::String containerName(txn.getDefaultAllocator());
			containerKey.toString(txn.getDefaultAllocator(), containerName);
			UTIL_TRACE_EXCEPTION(
				BASE_CONTAINER, e,
				GS_EXCEPTION_MERGE_MESSAGE(
					e, "Container '" << containerName
									 << "' is Invalid Status"));
		}
		catch (std::exception &ex) {
			GS_RETHROW_USER_ERROR(ex, GS_EXCEPTION_MERGE_MESSAGE(
										  ex, "Invalid Check Operation Error"));
		}
		GS_RETHROW_USER_ERROR(e, "");
	}
	else {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

TablePartitioningVersionId BaseContainer::getTablePartitioningVersionId() {
	return baseContainerImage_->tablePartitioningVersionId_;
}
void BaseContainer::setTablePartitioningVersionId(
	TablePartitioningVersionId versionId) {
	setDirty();
	baseContainerImage_->tablePartitioningVersionId_ = versionId;
}

struct CompareCharI {
public:
	bool operator()(const char * const right, const char * const left) const {
		return (compareStringStringI(
					right,
					static_cast<uint32_t>(strlen(right)),
					left,
					static_cast<uint32_t>(strlen(left))) < 0);
	}
};
/*!
	@brief Make map to convert new Column layout
*/
void BaseContainer::makeCopyColumnMap(TransactionContext &txn,
	MessageSchema *messageSchema, util::XArray<uint32_t> &copyColumnMap,
	DataStore::SchemaState &schemaState) {
	bool isCompletelySameSchema = false;
	bool isNullableChange = false;
	bool isVersionChange = false;

	{
		if (getAttribute() != messageSchema->getContainerAttribute()) {
			if (getAttribute() != CONTAINER_ATTR_SINGLE_SYSTEM &&
				messageSchema->getContainerAttribute() != CONTAINER_ATTR_SINGLE_SYSTEM) {
				GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
					"Specified container already exists. "
						<< "Partitioned and not partitioned container"
						<< " are not mutually convertible");
			} else {
				GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
					"Specified container already exists. "
						<< "Attribute is unexpectedly different. : new = ("
						<< messageSchema->getContainerAttribute()
						<< "), current = ("
						<< getAttribute() << ")");
			}
		}
	}
		
	uint32_t matchCount = 0;
	uint32_t posMatchCount = 0;
	uint32_t columnNum = messageSchema->getColumnCount();

	util::Vector<ColumnId> keyColumnIdList(txn.getDefaultAllocator());
	getKeyColumnIdList(keyColumnIdList);
	const util::Vector<ColumnId> &schemaKeyColumnIdList =  messageSchema->getRowKeyColumnIdList();
	if (keyColumnIdList.size() != schemaKeyColumnIdList.size()) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_CHANGE_INVALID,
			"RowKey of new schema does not match RowKey of current schema");
	}
	for (size_t i = 0; i < keyColumnIdList.size(); i++) {
		ColumnId schemaKeyColumnId = schemaKeyColumnIdList[i];
		ColumnInfo &keyColumnInfo = getColumnInfo(keyColumnIdList[i]);

		const util::String &newColumnName =
			messageSchema->getColumnName(schemaKeyColumnId);
		const char *columnName =
			keyColumnInfo.getColumnName(txn, *getObjectManager());
		uint32_t columnNameSize = static_cast<uint32_t>(strlen(columnName));

		bool isCaseSensitive = false;	
		if (!eqCaseStringString(txn, columnName, 
				columnNameSize, newColumnName.c_str(),
				static_cast<uint32_t>(newColumnName.length()),
				isCaseSensitive)) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_CHANGE_INVALID,
				"RowKey column name is different");
		}
	}

	ColumnInfo *columnInfoList = getColumnInfoList();
	util::Map<const char *, ColumnId, CompareCharI> columnNameMap(txn.getDefaultAllocator());
	for (uint32_t j = 0; j < getColumnNum(); j++) {
		const char *columnName =
			columnInfoList[j].getColumnName(txn, *getObjectManager());
		columnNameMap.insert(std::make_pair(columnName, j));
	}

	for (uint32_t i = 0; i < columnNum; i++) {
		const util::String &newColumnName = messageSchema->getColumnName(i);
		ColumnType columnType = messageSchema->getColumnType(i);
		bool isArray = messageSchema->getIsArray(i);
		bool isNotNull = messageSchema->getIsNotNull(i);

		bool isMatch = false;
		util::Map<const char *, ColumnId, CompareCharI>::iterator columnItr;
		columnItr = columnNameMap.find(newColumnName.c_str());
		if (columnItr != columnNameMap.end()) {
			uint32_t j = columnItr->second;

			bool isNotNull = messageSchema->getIsNotNull(i);
			if (columnInfoList[j].isNotNull() && !isNotNull) {
				isNullableChange = true;
			} else if (columnInfoList[j].isNotNull() != isNotNull) {
				GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_CHANGE_INVALID,
					newColumnName.c_str()
						<< " is different nullable property: new is not nullable"
						<< ", current is nullable");
			}

			bool isSame = false;
			if (columnInfoList[j].getSimpleColumnType() == columnType &&
				columnInfoList[j].isArray() == isArray) {
				isSame = true;
			}
			if (isSame) {  
				copyColumnMap.push_back(j);
				isMatch = true;
				matchCount++;
				if (i == j) {
					posMatchCount++;
				}
			}
			else {
				GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_CHANGE_INVALID,
					newColumnName.c_str()
						<< " is different type. (type, array): new = ("
						<< (int32_t)columnType << "," << isArray
						<< "), current = ("
						<< (int32_t)columnInfoList[j].getSimpleColumnType()
						<< "," << (int32_t)columnInfoList[j].isArray()
						<< ")");
			}
		}
		if (isMatch == false) {  
			if (columnType == COLUMN_TYPE_GEOMETRY) {
				GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_CHANGE_INVALID, "To add Geometry's column is not supported");
			}
			copyColumnMap.push_back(UNDEF_COLUMNID);
		}
	}

	if (matchCount > 0) {  
		if (getColumnNum() == columnNum && matchCount == getColumnNum()) {
			isCompletelySameSchema = true;
		}
		else {
			isCompletelySameSchema = false;
		}
	}
	else {  
		if (!keyColumnIdList.empty()) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_CHANGE_INVALID,
				"all column name is different");
		}
		isCompletelySameSchema = false;
	}

	{
		const util::String &changeAffinityString =
			messageSchema->getAffinityStr();

		char affinityStr[AFFINITY_STRING_MAX_LENGTH + 1];
		memcpy(affinityStr, getAffinityBinary(), AFFINITY_STRING_MAX_LENGTH);
		affinityStr[AFFINITY_STRING_MAX_LENGTH] =
			'\0';  
		if (strcmp(changeAffinityString.c_str(), DEFAULT_AFFINITY_STRING) ==
			0) {
			messageSchema->setAffinityStr(affinityStr);
		}
		else {
			if (strcmp(affinityStr, changeAffinityString.c_str()) != 0) {
				GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
					"affinity is different. : new = ("
						<< changeAffinityString << "), current = (" << affinityStr
						<< ")");
			}
		}
	}
	ContainerExpirationInfo *containerExpirationInfo = getContainerExpirationInfo();
	if (containerExpirationInfo != NULL) {
		if (!(*containerExpirationInfo ==
				messageSchema->getContainerExpirationInfo())) {
			GS_THROW_USER_ERROR(
				GS_ERROR_DS_DS_SCHEMA_INVALID, 
				"partition expiration is different");
		}
	}
	else {
		ContainerExpirationInfo defaultExpirationInfo;
		if (!(defaultExpirationInfo ==
				messageSchema->getContainerExpirationInfo())) {
			GS_THROW_USER_ERROR(
				GS_ERROR_DS_DS_SCHEMA_INVALID, 
				"partition expiration is different");
		}
	}

	if (getTablePartitioningVersionId() < messageSchema->getTablePartitioningVersionId()) {
		isVersionChange = true;	
	} else if (getTablePartitioningVersionId() > messageSchema->getTablePartitioningVersionId()) {
		if (getAttribute() == CONTAINER_ATTR_SUB) {
		}
	}

	checkContainerOption(messageSchema, copyColumnMap, isCompletelySameSchema);

	if (isCompletelySameSchema && !isNullableChange && !isVersionChange) {
		schemaState = DataStore::SAME_SCHEMA;
	} else if (isCompletelySameSchema && !isNullableChange && isVersionChange) {
		schemaState = DataStore::ONLY_TABLE_PARTITIONING_VERSION_DIFFERENCE;
	} else if (isCompletelySameSchema) {
		schemaState = DataStore::PROPERY_DIFFERENCE;
	} else if (matchCount == getColumnNum() && posMatchCount == matchCount) {
		schemaState = DataStore::COLUMNS_ADD;
		uint32_t columnNum, varColumnNum, rowFixedColumnSize;
		if (isFirstColumnAdd()) {
			columnNum = getColumnNum();
			varColumnNum = getVariableColumnNum();
			rowFixedColumnSize = getRowFixedColumnSize();
		} else {
			getInitialSchemaStatus(columnNum, varColumnNum, rowFixedColumnSize);
		}
		messageSchema->setFirstSchema(columnNum, varColumnNum, rowFixedColumnSize);
	} else {
		schemaState = DataStore::COLUMNS_DIFFERENCE;
	}
}

void BaseContainer::setCreateRowId(TransactionContext &txn, RowId rowId) {
	util::StackAllocator::Scope scope(txn.getDefaultAllocator());
	StackAllocAutoPtr<BtreeMap> mvccMap(
		txn.getDefaultAllocator(), getMvccMap(txn));
	TransactionId tId = txn.getId();

	bool isUpdate = false;
	MvccRowImage preMvccImage;
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
				if (itr->type_ == MVCC_CREATE) {
					preMvccImage = *itr;
					isUpdate = true;
					break;
				}
			}
		}
	}
	if (isUpdate) {
		MvccRowImage postMvccImage = preMvccImage;
		postMvccImage.updateRowId(rowId);
		updateMvccMap(txn, mvccMap.get(), tId, preMvccImage, postMvccImage);
	}
	else {
		MvccRowImage postMvccImage(rowId, rowId);
		insertMvccMap(txn, mvccMap.get(), tId, postMvccImage);
	}
}

bool BaseContainer::isSupportIndex(const IndexInfo &indexInfo) const {
	MapType inputMapType = indexInfo.mapType;
	if (inputMapType < 0 || inputMapType >= MAP_TYPE_NUM) {
		return false;
	}
	if (indexInfo.columnIds_.size() > 1 && inputMapType != MAP_TYPE_BTREE) {
		return false;
	}
	bool isSuport = true;
	for (size_t i = 0; i < indexInfo.columnIds_.size(); i++) {
		ColumnId inputColumnId = indexInfo.columnIds_[i];

		ColumnInfo &columnInfo = getColumnInfo(inputColumnId);
		if (columnInfo.isArray()) {
			return false;
		}
		switch (getContainerType()) {
		case COLLECTION_CONTAINER:
			isSuport = Collection::indexMapTable[columnInfo.getColumnType()]
												[inputMapType];
			break;
		case TIME_SERIES_CONTAINER:
			isSuport = TimeSeries::indexMapTable[columnInfo.getColumnType()]
												[inputMapType];
			break;
		default:
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_CONTAINER_TYPE_UNKNOWN, "");
			break;
		}
		if (!isSuport) {
			break;
		}
	}
	if (isSuport) {
		return true;
	}
	else {
		return false;
	}
}

template void BaseContainer::indexInsertImpl<Collection>(
	TransactionContext &txn, IndexData &indexData, bool isImmediate);
template void BaseContainer::indexInsertImpl<TimeSeries>(
	TransactionContext &txn, IndexData &indexData, bool isImmediate);

template <typename R>
void BaseContainer::indexInsertImpl(
	TransactionContext &txn, IndexData &indexData, bool isImmediate) {
	if (!isImmediate) {
		ValueMap valueMap(txn, this, indexData);

		RowId lastRowId = INITIAL_ROWID;
		ResultSize limit = (IndexCursor::getNum() / getNormalRowArrayNum() <= 2) ?
			2 : (IndexCursor::getNum() / getNormalRowArrayNum());

		TermCondition cond(getRowIdColumnType(), getRowIdColumnType(), 
			DSExpression::GT, getRowIdColumnId(), &indexData.cursor_,
			sizeof(indexData.cursor_));
		BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, limit);

		util::XArray<OId> oIdList(txn.getDefaultAllocator());
		util::XArray<OId>::iterator itr;

		StackAllocAutoPtr<BtreeMap> rowIdMap(
			txn.getDefaultAllocator(), getRowIdMap(txn));
		rowIdMap.get()->search(txn, sc, oIdList, ORDER_UNDEFINED);
		RowArray rowArray(txn, this);
		for (itr = oIdList.begin(); itr != oIdList.end(); itr++) {
			rowArray.load(txn, *itr, this, OBJECT_READ_ONLY);
			lastRowId = rowArray.getRowId();
			for (rowArray.begin(); !rowArray.end(); rowArray.next()) {
				RowArray::Row row(rowArray.getRow(), &rowArray);
				if (indexData.cursor_ >= row.getRowId()) {
					continue;
				}
				bool isNullValue;
				const void *fieldValue = row.getFields<R>(txn, 
					valueMap.getFuncInfo(txn), isNullValue);
				insertValueMap(txn, valueMap, fieldValue, rowArray.getOId(),
					isNullValue);
				lastRowId = row.getRowId();
			}
			indexData.cursor_ = lastRowId;
		}
		if (oIdList.size() < limit) {
			indexData.cursor_ = MAX_ROWID;
		} else {
			assert(lastRowId > INITIAL_ROWID);
		}
	} else {
		BtreeMap::BtreeCursor btreeCursor;
		RowArray rowArray(txn, this);
		while (1) {
			util::StackAllocator::Scope scope(txn.getDefaultAllocator());
			util::XArray<OId> oIdList(txn.getDefaultAllocator());
			util::XArray<OId>::iterator itr;
			StackAllocAutoPtr<BtreeMap> rowIdMap(
				txn.getDefaultAllocator(), getRowIdMap(txn));
			int32_t getAllStatus = rowIdMap.get()->getAll(
				txn, PARTIAL_RESULT_SIZE, oIdList, btreeCursor);
			for (itr = oIdList.begin(); itr != oIdList.end(); itr++) {
				rowArray.load(txn, *itr, this, OBJECT_READ_ONLY);
				ValueMap valueMap(txn, this, indexData);
				for (rowArray.begin(); !rowArray.end(); rowArray.next()) {
					RowArray::Row row(rowArray.getRow(), &rowArray);
					bool isNullValue;
					const void *fieldValue = row.getFields<R>(txn, 
						valueMap.getFuncInfo(txn), isNullValue);
					insertValueMap(txn, valueMap, fieldValue, rowArray.getOId(),
						isNullValue);
				}

			}
			if (getAllStatus == GS_SUCCESS) {
				indexData.cursor_ = MAX_ROWID;
				break;
			}
		}
	}
}


/*!
	@brief Change new Column layout
*/
void BaseContainer::changeSchema(TransactionContext& txn,
	BaseContainer& newContainer, util::XArray<uint32_t>& copyColumnMap) {
	try {
		setDirty();

		if (getTriggerOId() != UNDEF_OID) {
			util::XArray<const util::String *> oldColumnNameList(
				txn.getDefaultAllocator());
			ColumnInfo *oldSchema = this->getColumnInfoList();
			for (uint32_t i = 0; i < this->getColumnNum(); i++) {
				util::String *columnName = ALLOC_NEW(txn.getDefaultAllocator())
					util::String(txn.getDefaultAllocator());
				columnName->append(reinterpret_cast<const char *>(
					oldSchema[i].getColumnName(txn, *getObjectManager())));
				oldColumnNameList.push_back(columnName);
			}
			util::XArray<const util::String *> newColumnNameList(
				txn.getDefaultAllocator());
			ColumnInfo *newSchema = newContainer.getColumnInfoList();
			for (uint32_t i = 0; i < newContainer.getColumnNum(); i++) {
				util::String *columnName = ALLOC_NEW(txn.getDefaultAllocator())
					util::String(txn.getDefaultAllocator());
				columnName->append(reinterpret_cast<const char *>(
					newSchema[i].getColumnName(txn, *getObjectManager())));
				newColumnNameList.push_back(columnName);
			}
			newContainer.updateTrigger(txn, getTriggerOId(), 
				oldColumnNameList, newColumnNameList);
		}

		util::Vector<IndexInfo> oldIndexInfoList(txn.getDefaultAllocator());
		getIndexInfoList(txn, oldIndexInfoList);
		for (size_t i = 0; i < oldIndexInfoList.size(); i++) {
			const IndexInfo &oldIndexInfo = oldIndexInfoList[i];
			util::Vector<uint32_t> newColumnIds(txn.getDefaultAllocator());
			bool isExists = true;
			for (size_t j = 0; j < oldIndexInfo.columnIds_.size(); j++) {
				ColumnId oldColumnId = oldIndexInfo.columnIds_[j];
				util::XArray<uint32_t>::iterator itr = std::find(copyColumnMap.begin(), copyColumnMap.end(), oldColumnId);
				size_t index = std::distance(copyColumnMap.begin(), itr);
				if (index == copyColumnMap.size()) {
					isExists = false;
					break;
				}
				ColumnId newColumnId = static_cast<ColumnId>(index);
				newColumnIds.push_back(newColumnId);
			}
			if (isExists) {
				IndexInfo newIndexInfo(txn.getDefaultAllocator(),
					oldIndexInfo.indexName_, newColumnIds,
					oldIndexInfo.mapType);

				bool isImmediate = true;
				IndexCursor indexCusror(isImmediate);

				newContainer.createIndex(txn, newIndexInfo, indexCusror);
			}
		}

		setAlterContainer();

		bool isImmediate = txn.isAutoCommit();
		ContainerCursor containerCursor(isImmediate, newContainer.getBaseOId());
		if (!containerCursor.isImmediateMode()) {
			TransactionId tId = txn.getId();
			StackAllocAutoPtr<BtreeMap> mvccMap(
				txn.getDefaultAllocator(), getMvccMap(txn));
			MvccRowImage image = containerCursor.getMvccImage();
			insertMvccMap(txn, mvccMap.get(), tId, image);
		} else {
			continueChangeSchema(txn, containerCursor);
		}
	}
	catch (std::exception& e) {
		handleUpdateError(
			txn, e, GS_ERROR_DS_DS_CHANGE_COLLECTION_SCHEMA_FAILED);
	}
}

template void BaseContainer::changeSchemaRecord<Collection>(TransactionContext &txn,
	BaseContainer &newContainer, util::XArray<uint32_t> &copyColumnMap,
	RowId &cursor, bool isImmediate);
template void BaseContainer::changeSchemaRecord<TimeSeries>(TransactionContext &txn,
	BaseContainer &newContainer, util::XArray<uint32_t> &copyColumnMap,
	RowId &cursor, bool isImmediate);

template <typename R>
void BaseContainer::changeSchemaRecord(TransactionContext &txn,
	BaseContainer &newContainer, util::XArray<uint32_t> &copyColumnMap,
	RowId &cursor, bool isImmediate) {
	ColumnInfo *newColumnInfoList = newContainer.getColumnInfoList();

	RowArray rowArray(txn, this);
	if (!isImmediate) {
		util::StackAllocator::Scope scope(txn.getDefaultAllocator());
		util::XArray<uint8_t> serializedRow(txn.getDefaultAllocator());
		util::XArray<uint8_t> serializedVarDataList(txn.getDefaultAllocator());
		util::XArray<OId> idList(txn.getDefaultAllocator());  
		size_t counter = 0;
		Value value;

		RowId lastRowId = INITIAL_ROWID;
		ResultSize limit = (ContainerCursor::getNum() / getNormalRowArrayNum() <= 2) ?
			2 : (ContainerCursor::getNum() / getNormalRowArrayNum());
		TermCondition cond(getRowIdColumnType(), getRowIdColumnType(), 
			DSExpression::GT, getRowIdColumnId(), &cursor,
			sizeof(cursor));
		BtreeMap::SearchContext sc(txn.getDefaultAllocator(), cond, limit);

		util::XArray<OId> oIdList(txn.getDefaultAllocator());
		util::XArray<OId>::iterator itr;

		StackAllocAutoPtr<BtreeMap> rowIdMap(
			txn.getDefaultAllocator(), getRowIdMap(txn));
		rowIdMap.get()->search(txn, sc, oIdList, ORDER_UNDEFINED);
		for (itr = oIdList.begin(); itr != oIdList.end(); itr++) {
			size_t rowNum = 0;
			serializedRow.clear();
			serializedVarDataList.clear();
			OutputMessageRowStore outputMessageRowStore(
				getDataStore()->getValueLimitConfig(), newColumnInfoList,
				newContainer.getColumnNum(), serializedRow,
				serializedVarDataList, false);
			rowArray.load(txn, *itr, this, OBJECT_READ_ONLY);
			lastRowId = rowArray.getRowId();
			for (rowArray.begin(); !rowArray.end(); rowArray.next()) {
				RowArray::Row row(rowArray.getRow(), &rowArray);
				if (cursor >= row.getRowId()) {
					continue;
				}

				outputMessageRowStore.beginRow();  

				for (uint32_t columnId = 0; columnId < copyColumnMap.size();
					 columnId++) {
					uint32_t oldColumnId = copyColumnMap[columnId];
					ColumnInfo &oldColumnInfo = getColumnInfo(oldColumnId);
					if (oldColumnId != UNDEF_COLUMNID) {
						row.getFieldImage(txn, oldColumnInfo, columnId,
							&outputMessageRowStore);
					}
					else {
						value.init(newColumnInfoList[columnId].getColumnType());
						ValueProcessor::getField(txn, *getObjectManager(),
							columnId, &value, &outputMessageRowStore);
					}
				}

				outputMessageRowStore.next();
				counter++;
				rowNum++;
				lastRowId = row.getRowId();
			}
			{
				serializedRow.push_back(
					serializedVarDataList.data(), serializedVarDataList.size());
				InputMessageRowStore inputMessageRowStore(
					getDataStore()->getValueLimitConfig(), newColumnInfoList,
					newContainer.getColumnNum(), serializedRow.data(),
					static_cast<uint32_t>(serializedRow.size()), rowNum,
					static_cast<uint32_t>(newContainer.getRowFixedDataSize()));
				inputMessageRowStore.next();

				DataStore::PutStatus status;
				newContainer.putRowList(txn,
					static_cast<uint32_t>(serializedRow.size()),
					serializedRow.data(), rowNum, status);
			}
			cursor = lastRowId;
		}
		if (oIdList.size() < limit) {
			cursor = MAX_ROWID;
		} else {
			assert(lastRowId > INITIAL_ROWID);
		}
	} else {
		BtreeMap::BtreeCursor btreeCursor;

		size_t counter = 0;
		Value value;
		while (1) {
			util::StackAllocator::Scope scope(txn.getDefaultAllocator());
			util::XArray<uint8_t> serializedRow(txn.getDefaultAllocator());
			util::XArray<uint8_t> serializedVarDataList(txn.getDefaultAllocator());
			util::XArray<OId> idList(txn.getDefaultAllocator());  
			StackAllocAutoPtr<BtreeMap> rowIdMap(
				txn.getDefaultAllocator(), getRowIdMap(txn));
			int32_t getAllStatus = rowIdMap.get()->getAll(
				txn, PARTIAL_RESULT_SIZE, idList, btreeCursor);
			util::XArray<OId>::iterator itr;
			for (itr = idList.begin(); itr != idList.end(); itr++) {
				size_t rowNum = 0;
				serializedRow.clear();
				serializedVarDataList.clear();
				OutputMessageRowStore outputMessageRowStore(
					getDataStore()->getValueLimitConfig(), newColumnInfoList,
					newContainer.getColumnNum(), serializedRow,
					serializedVarDataList, false);
				rowArray.load(txn, *itr, this, OBJECT_READ_ONLY);
				for (rowArray.begin(); !rowArray.end(); rowArray.next()) {
					RowArray::Row row(rowArray.getRow(), &rowArray);

					outputMessageRowStore.beginRow();  

					for (uint32_t columnId = 0; columnId < copyColumnMap.size();
						 columnId++) {
						uint32_t oldColumnId = copyColumnMap[columnId];
						ColumnInfo &oldColumnInfo = getColumnInfo(oldColumnId);
						if (oldColumnId != UNDEF_COLUMNID) {
							row.getFieldImage(txn, oldColumnInfo, columnId,
								&outputMessageRowStore);
						}
						else {
							value.init(newColumnInfoList[columnId].getColumnType());
							ValueProcessor::getField(txn, *getObjectManager(),
								columnId, &value, &outputMessageRowStore);
						}
					}

					outputMessageRowStore.next();
					counter++;
					rowNum++;
				}
				{
					serializedRow.push_back(
						serializedVarDataList.data(), serializedVarDataList.size());
					InputMessageRowStore inputMessageRowStore(
						getDataStore()->getValueLimitConfig(), newColumnInfoList,
						newContainer.getColumnNum(), serializedRow.data(),
						static_cast<uint32_t>(serializedRow.size()), rowNum,
						static_cast<uint32_t>(newContainer.getRowFixedDataSize()));
					inputMessageRowStore.next();

					DataStore::PutStatus status;
					newContainer.putRowList(txn,
						static_cast<uint32_t>(serializedRow.size()),
						serializedRow.data(), rowNum, status);
				}
			}
			if (getAllStatus == GS_SUCCESS) {
				break;
			}
		}
	}
}
void BaseContainer::makeCopyColumnMap(TransactionContext &txn,
	BaseContainer &newContainer, util::XArray<uint32_t> &copyColumnMap) {

	ColumnInfo *newColumnInfoList = newContainer.getColumnInfoList();
	for (uint32_t i = 0; i < newContainer.getColumnNum(); i++) {
		const char *newColumnName =
			newColumnInfoList[i].getColumnName(txn, *getObjectManager());
		uint32_t newColumnNameSize =
			static_cast<uint32_t>(strlen(newColumnName));

		ColumnInfo *columnInfoList = getColumnInfoList();
		bool isMatch = false;
		for (uint32_t j = 0; j < getColumnNum(); j++) {
			const char *columnName =
				columnInfoList[j].getColumnName(txn, *getObjectManager());
			uint32_t columnNameSize =
				static_cast<uint32_t>(strlen(columnName));
			bool isCaseSensitive = false;	
			if (eqCaseStringString(
					txn, columnName, columnNameSize,
					newColumnName, newColumnNameSize,
					isCaseSensitive)) {  

				copyColumnMap.push_back(j);
				isMatch = true;
				break;
			}
		}
		if (isMatch == false) {  
			copyColumnMap.push_back(UNDEF_COLUMNID);
		}
	}
}


void BaseContainer::changeProperty(TransactionContext& txn, OId columnSchemaOId) {
	setDirty();
	baseContainerImage_->columnSchemaOId_ = columnSchemaOId;
	setVersionId(getVersionId() + 1);  
}

void BaseContainer::changeNullStats(TransactionContext& txn, uint32_t oldColumnNum) {
	setDirty();
	baseContainerImage_->normalRowArrayNum_ = calcRowArrayNum(
		txn, getDataStore()->getConfig().isRowArraySizeControlMode(),
		ROW_ARRAY_MAX_SIZE);

	bool isExpand = indexSchema_->expandNullStats(txn, oldColumnNum, getColumnNum());
	if (isExpand) {
		replaceIndexSchema(txn);
	}
	for (ColumnId columnId = oldColumnNum; columnId < getColumnNum(); columnId++) {
		ColumnInfo columnInfo = getColumnInfo(columnId);
		if (!columnInfo.isNotNull()) {
			setNullStats(columnId);
		}
	}
}

template <typename R>
void BaseContainer::getRowListImpl(TransactionContext &txn,
	util::XArray<OId> &oIdList, ResultSize limit, ResultSize &resultNum,
	MessageRowStore *messageRowStore, bool isWithRowId, ResultSize startPos) {
	if (oIdList.size() - startPos > limit) {
		resultNum = limit;
	}
	else {
		resultNum = oIdList.size() - startPos;
	}
	RowArray rowArray(txn, reinterpret_cast<R *>(this));
	for (size_t i = static_cast<size_t>(startPos);
		 i < static_cast<size_t>(startPos + resultNum); i++) {
		rowArray.load(
			txn, oIdList[i], reinterpret_cast<R *>(this), OBJECT_READ_ONLY);
		RowArray::Row row(rowArray.getRow(), &rowArray);
		row.getImage(txn, messageRowStore, isWithRowId);
		messageRowStore->next();
	}
}

template <class R, class S>
void BaseContainer::searchMvccMap(
	TransactionContext &txn, S &sc, util::XArray<OId> &resultList, bool isCheckOnly, bool ignoreTxnCheck) {
	if (isExclusiveUpdate()) {  
		return;
	}


	setExclusiveStatus(NO_ROW_TRANSACTION);

	StackAllocAutoPtr<BtreeMap> mvccMap(
		txn.getDefaultAllocator(), getMvccMap(txn));
	if (mvccMap.get()->isEmpty()) {
		return;
	}

	RowArray rowArray(txn, this);
	ContainerValue containerValue(txn.getPartitionId(), *getObjectManager());
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
			break;
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
				util::Vector<TermCondition> condList(txn.getDefaultAllocator());
				sc.getConditionList(condList, BaseIndex::SearchContext::COND_ALL);
				rowArray.load(txn, itr->second.snapshotRowOId_,
					this, OBJECT_READ_ONLY);
				for (rowArray.begin(); !rowArray.end(); rowArray.next()) {
					RowArray::Row row(rowArray.getRow(), &rowArray);
					if (txn.getId() != row.getTxnId()) {
						setExclusiveStatus(NOT_EXCLUSIVE);
					} else if (!ignoreTxnCheck) {
						continue;
					}

					if (!isCheckOnly) {
						bool isMatch = true;
						util::Vector<TermCondition>::iterator condItr;
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

			}
			break;
		default:
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
			break;
		}
	}
	if (resultList.size() > sc.getLimit()) {
		resultList.resize(sc.getLimit());
	}
}

template<typename R, typename S> 
void BaseContainer::searchMvccMap(
		TransactionContext& txn, S &sc, ContainerRowScanner &scanner) {
	if (isExclusiveUpdate()) { 
		return;
	}


	setExclusiveStatus(NO_ROW_TRANSACTION);

	StackAllocAutoPtr<BtreeMap> mvccMap(
			txn.getDefaultAllocator(), getMvccMap(txn));
	if (mvccMap.get()->isEmpty()) {
		return;
	}

	RowArray &rowArray = scanner.getRowArray();
	ContainerValue containerValue(txn.getPartitionId(), *getObjectManager());
	util::XArray< std::pair<TransactionId, MvccRowImage> > idList(
			txn.getDefaultAllocator());
	util::XArray< std::pair<TransactionId, MvccRowImage> >::iterator itr;
	mvccMap.get()->getAll<TransactionId, MvccRowImage>(
			txn, MAX_RESULT_SIZE, idList);
	for (itr = idList.begin(); itr != idList.end(); itr++) {
		switch (itr->second.type_) {
		case MVCC_SELECT:
			if (isNoRowTransaction()) {
				setExclusiveStatus(EXCLUSIVE);
			}
			break;
		case MVCC_INDEX:
		case MVCC_CONTAINER:
			break;
		case MVCC_CREATE:
			if (isExclusive() && txn.getId() != itr->first) { 
				setExclusiveStatus(NOT_EXCLUSIVE_CREATE_EXIST);
			}
			else if (isNoRowTransaction()) {
				setExclusiveStatus(EXCLUSIVE);
			}
			break;
		case MVCC_UPDATE:
		case MVCC_DELETE:
			{
				if (isNoRowTransaction()) {
					setExclusiveStatus(EXCLUSIVE);
				} 
				util::Vector<TermCondition> condList(txn.getDefaultAllocator());
				sc.getConditionList(condList, BaseIndex::SearchContext::COND_ALL);
				rowArray.load(txn, itr->second.snapshotRowOId_, this,
						OBJECT_READ_ONLY);
				for (rowArray.begin(); !rowArray.end(); rowArray.next()) {
					RowArray::Row row(rowArray.getRow(), &rowArray);
					if (txn.getId() == row.getTxnId()) {
						continue;
					}
					setExclusiveStatus(NOT_EXCLUSIVE);

					bool isMatch = true;
					util::Vector<TermCondition>::iterator condItr;
					for (condItr = condList.begin(); condItr != condList.end(); condItr++) {
						if (!row.isMatch(txn, *condItr, containerValue)) {
							isMatch = false;
							break;
						}
					}
					if (isMatch) {
						const OId oId = rowArray.getOId();
						if (!scanner.scanRowUnchecked(
								txn, *this, &rowArray, &oId, &oId + 1)) {
							return;
						}
					}
				}
			}
			break;
		default:
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
			break;
		}
	}
}

template void BaseContainer::searchMvccMap<Collection, BtreeMap::SearchContext>(
		TransactionContext& txn, BtreeMap::SearchContext &sc, ContainerRowScanner &scanner);

template<typename R>
void BaseContainer::resolveExclusiveStatus(TransactionContext& txn) {
	if (BaseContainer::getExclusiveStatus() != UNKNOWN) {
		return;
	}

	BtreeMap::SearchContext sc (txn.getDefaultAllocator(), UNDEF_COLUMNID);
	const bool checkOnly = true;
	util::XArray<OId> dummyList(txn.getDefaultAllocator());
	const bool ignoreTxnCheck = false;
	searchMvccMap<R, BtreeMap::SearchContext>(txn, sc, dummyList, checkOnly, ignoreTxnCheck);
}

template void BaseContainer::resolveExclusiveStatus<Collection>(
		TransactionContext& txn);
template void BaseContainer::resolveExclusiveStatus<TimeSeries>(
		TransactionContext& txn);

bool BaseContainer::getRowIdRangeCondition(
		util::StackAllocator &alloc,
		BtreeMap::SearchContext &sc, ContainerType containerType,
		RowId *startRowId, RowId *endRowId) {

	const RowId minRowId = std::numeric_limits<RowId>::min();
	TermCondition *currentStartCond = sc.getStartKeyCondition();

	if (currentStartCond == NULL) {
		*startRowId = minRowId;
	}
	else {
		assert(currentStartCond->valueSize_ == sizeof(*startRowId));

		*startRowId = *reinterpret_cast<const RowId*>(currentStartCond->value_);

		if (currentStartCond->opType_ == DSExpression::GT && *startRowId != minRowId) {
			--*startRowId;
		}
	}

	TermCondition *currentEndCond = sc.getEndKeyCondition();
	if (currentEndCond == NULL) {
		*endRowId = UNDEF_ROWID;
	}
	else {
		assert(currentEndCond->valueSize_ == sizeof(*endRowId));

		*endRowId = *reinterpret_cast<const RowId*>(currentEndCond->value_);

		if (currentEndCond->opType_ == DSExpression::LE && *endRowId != UNDEF_ROWID) {
			++*endRowId;
		}
	}

	if (sc.getRestConditionNum() != 0) {
		const DSExpression::Operation opType = DSExpression::LE;

		uint32_t columnId;
		if (containerType == TIME_SERIES_CONTAINER) {
			columnId = ColumnInfo::ROW_KEY_COLUMN_ID;
		}
		else {
			columnId = UNDEF_COLUMNID;
		}

		UTIL_STATIC_ASSERT(sizeof(RowId) == sizeof(Timestamp));
		const uint32_t valueSize = sizeof(RowId);

		util::Vector<TermCondition> condList(alloc);
		sc.getConditionList(condList, BaseIndex::SearchContext::COND_OTHER);
		util::Vector<TermCondition>::iterator condItr;
		for (condItr = condList.begin(); condItr != condList.end(); condItr++) {
			const TermCondition &condition = *condItr;
			if (condition.value_ != NULL &&
					condition.opType_ == opType &&
					condition.columnId_ == columnId &&
					condition.valueSize_ == valueSize) {
				RowId rowId =
						*reinterpret_cast<const RowId*>(condition.value_);
				if (rowId != UNDEF_ROWID) {
					++rowId;
				}
				*endRowId = std::min(*endRowId, rowId);
			}
		}
	}
	return (*startRowId < *endRowId);
}


template
void BaseContainer::mergeRowList<Collection>(TransactionContext &txn,
	const ColumnInfo &targetColumnInfo, util::XArray<OId> &inputList1,
	const bool isList1Sorted, util::XArray<OId> &inputList2,
	const bool isList2Sorted, util::XArray<OId> &mergeList,
	OutputOrder outputOrder);
template
void BaseContainer::mergeRowList<TimeSeries>(TransactionContext &txn,
	const ColumnInfo &targetColumnInfo, util::XArray<OId> &inputList1,
	const bool isList1Sorted, util::XArray<OId> &inputList2,
	const bool isList2Sorted, util::XArray<OId> &mergeList,
	OutputOrder outputOrder);

template <class R>
void BaseContainer::mergeRowList(TransactionContext &txn,
	const ColumnInfo &targetColumnInfo, util::XArray<OId> &inputList1,
	const bool isList1Sorted, util::XArray<OId> &inputList2,
	const bool isList2Sorted, util::XArray<OId> &mergeList,
	OutputOrder outputOrder) {
	mergeList.reserve(
		inputList1.size() + inputList2.size());  
	{
		util::StackAllocator::Scope scope(
			txn.getDefaultAllocator());  

		RowArray rowArray(txn, reinterpret_cast<R *>(this));
		ColumnType targetType = targetColumnInfo.getColumnType();
		util::XArray<SortKey> sortKeyList1(txn.getDefaultAllocator());
		util::XArray<SortKey> sortKeyList2(txn.getDefaultAllocator());

		util::XArray<OId>::iterator itr;
		for (itr = inputList1.begin(); itr != inputList1.end(); itr++) {
			rowArray.load(
				txn, *itr, reinterpret_cast<R *>(this), OBJECT_READ_ONLY);
			RowArray::Row row(rowArray.getRow(), &rowArray);
			SortKey sortKey;
			if (row.isNullValue(targetColumnInfo)) {
				sortKey.set(
					txn, targetType, NULL, *itr);
			} else {
				BaseObject baseFieldObject(
					txn.getPartitionId(), *getObjectManager());
				row.getField(txn, targetColumnInfo, baseFieldObject);

				sortKey.set(
					txn, targetType, baseFieldObject.getCursor<uint8_t>(), *itr);
			}
			sortKeyList1.push_back(sortKey);
		}

		const Operator *sortOp;
		const Operator stringOp[] = {&ltStringString, &gtStringString,
			&ltStringString};  
		const Operator boolOp[] = {&ltBoolBool, &gtBoolBool,
			&ltBoolBool};  
		bool isNullLast = outputOrder == ORDER_ASCENDING;
		if (targetType == COLUMN_TYPE_STRING) {
			sortOp = &stringOp[outputOrder];
		}
		else if (targetType == COLUMN_TYPE_BOOL) {
			sortOp = &boolOp[outputOrder];
		}
		else {
			if (outputOrder == ORDER_ASCENDING) {
				sortOp = &ComparatorTable::ltTable_[targetType][targetType];
			}
			else {
				sortOp = &ComparatorTable::gtTable_[targetType][targetType];
			}
		}
		if (!isList1Sorted) {
			std::sort(sortKeyList1.begin(), sortKeyList1.end(),
				SortPred(txn, sortOp, targetType, isNullLast));

		}

		for (itr = inputList2.begin(); itr != inputList2.end(); itr++) {
			rowArray.load(
				txn, *itr, reinterpret_cast<R *>(this), OBJECT_READ_ONLY);
			RowArray::Row row(rowArray.getRow(), &rowArray);
			SortKey sortKey;
			if (row.isNullValue(targetColumnInfo)) {
				sortKey.set(
					txn, targetType, NULL, *itr);
			} else {
				BaseObject baseFieldObject(
					txn.getPartitionId(), *getObjectManager());
				row.getField(txn, targetColumnInfo, baseFieldObject);

				sortKey.set(
					txn, targetType, baseFieldObject.getCursor<uint8_t>(), *itr);
			}
			sortKeyList2.push_back(sortKey);
		}
		if (!isList2Sorted) {
			std::sort(sortKeyList2.begin(), sortKeyList2.end(),
				SortPred(txn, sortOp, targetType, isNullLast));
		}

		util::XArray<SortKey>::iterator itr1, itr2;
		SortPred pred(txn, sortOp, targetType, isNullLast);
		for (itr1 = sortKeyList1.begin(), itr2 = sortKeyList2.begin();
			 itr1 != sortKeyList1.end() && itr2 != sortKeyList2.end();) {

			bool result = pred(*itr1, *itr2);
			if (result) {
				mergeList.push_back(itr1->getOId());
				itr1++;
			}
			else {
				mergeList.push_back(itr2->getOId());
				itr2++;
			}

		}
		while (itr2 != sortKeyList2.end()) {
			mergeList.push_back(itr2->getOId());
			itr2++;
		}
		while (itr1 != sortKeyList1.end()) {
			mergeList.push_back(itr1->getOId());
			itr1++;
		}
	}
}


template <typename R>
void BaseContainer::getRowIdListImpl(TransactionContext &txn,
	util::XArray<OId> &oIdList, util::XArray<RowId> &rowIdList) {
	rowIdList.clear();
	rowIdList.reserve(oIdList.size());
	RowArray rowArray(txn, reinterpret_cast<R *>(this));
	for (size_t i = 0; i < oIdList.size(); i++) {
		rowArray.load(
			txn, oIdList[i], reinterpret_cast<R *>(this), OBJECT_READ_ONLY);
		RowArray::Row row(rowArray.getRow(), &rowArray);
		rowIdList.push_back(row.getRowId());
	}
}

void BaseContainer::getCommonContainerOptionInfo(
	util::XArray<uint8_t> &containerSchema) {
	char affinityStr[AFFINITY_STRING_MAX_LENGTH + 1];
	memcpy(affinityStr, getAffinityBinary(), AFFINITY_STRING_MAX_LENGTH);
	affinityStr[AFFINITY_STRING_MAX_LENGTH] =
		'\0';  
	int32_t affinityStrLen =
		static_cast<int32_t>(strlen(reinterpret_cast<char *>(affinityStr)));
	containerSchema.push_back(
		reinterpret_cast<uint8_t *>(&affinityStrLen), sizeof(int32_t));
	containerSchema.push_back(
		reinterpret_cast<uint8_t *>(affinityStr), affinityStrLen);
}

void BaseContainer::initializeSchema(TransactionContext &txn,
	ObjectManager &objectManager, MessageSchema *messageSchema,
	const AllocateStrategy &allocateStrategy,
	util::XArray<ShareValueList::ElemData> &list, uint32_t &allocateSize,
	bool onMemory) {
	allocateSize = 0;
	{
		ShareValueList::ElemData elem(META_TYPE_COLUMN_SCHEMA);
		elem.size_ =
			ColumnSchema::getAllocateSize(messageSchema->getColumnCount(), 
			messageSchema->getRowKeyNum());
		elem.binary_ = ALLOC_NEW(txn.getDefaultAllocator()) uint8_t[elem.size_];
		memset(elem.binary_, 0, elem.size_);

		ColumnSchema *columnSchema =
			reinterpret_cast<ColumnSchema *>(elem.binary_);
		columnSchema->initialize(messageSchema->getColumnCount());
		columnSchema->set(txn, objectManager, messageSchema, allocateStrategy, onMemory);

		allocateSize += elem.size_;
		list.push_back(elem);
	}

	if (messageSchema->getContainerType() == TIME_SERIES_CONTAINER) {
		MessageTimeSeriesSchema *messageTimeSeriesSchema =
			reinterpret_cast<MessageTimeSeriesSchema *>(messageSchema);

		{
			ShareValueList::ElemData elem(META_TYPE_COMPRESSION_SCHEMA);
			elem.size_ = CompressionSchema::getAllocateSize(
				messageTimeSeriesSchema->getCompressionInfoNum());
			elem.binary_ =
				ALLOC_NEW(txn.getDefaultAllocator()) uint8_t[elem.size_];
			memset(elem.binary_, 0, elem.size_);
			CompressionSchema *compressionSchema =
				reinterpret_cast<CompressionSchema *>(elem.binary_);
			compressionSchema->initialize(
				messageTimeSeriesSchema->getCompressionType(),
				messageTimeSeriesSchema->getDurationInfo());
			if (compressionSchema->getCompressionType() == HI_COMPRESSION) {
				uint16_t pos = 0;
				for (uint32_t i = 0; i < messageSchema->getColumnCount(); i++) {
					if (messageTimeSeriesSchema->getCompressionInfo(i)
							.getType() != MessageCompressionInfo::NONE) {
						compressionSchema->addHiCompression(pos, i,
							messageTimeSeriesSchema->getCompressionInfo(i)
								.getThreshhold(),
							messageTimeSeriesSchema->getCompressionInfo(i)
								.getRate(),
							messageTimeSeriesSchema->getCompressionInfo(i)
								.getSpan(),
							messageTimeSeriesSchema->getCompressionInfo(i)
								.getThreshholdRelative());
						pos++;
					}
				}
			}

			allocateSize += elem.size_;
			list.push_back(elem);
		}

		{
			ShareValueList::ElemData elem(META_TYPE_DURATION);
			elem.size_ = sizeof(ExpirationInfo);
			elem.binary_ =
				ALLOC_NEW(txn.getDefaultAllocator()) uint8_t[elem.size_];
			memset(elem.binary_, 0, elem.size_);

			ExpirationInfo *expirationInfo =
				reinterpret_cast<ExpirationInfo *>(elem.binary_);
			*expirationInfo = messageTimeSeriesSchema->getExpirationInfo();

			allocateSize += elem.size_;
			list.push_back(elem);
		}
	}

	{
		ShareValueList::ElemData elem(META_TYPE_AFFINITY);
		elem.size_ = sizeof(char[AFFINITY_STRING_MAX_LENGTH]);
		elem.binary_ = ALLOC_NEW(txn.getDefaultAllocator()) uint8_t[elem.size_];
		memset(elem.binary_, 0, elem.size_);

		char *affinityStr = reinterpret_cast<char *>(elem.binary_);
		const util::String &affinityString = messageSchema->getAffinityStr();
		memcpy(affinityStr, affinityString.c_str(), affinityString.length());

		allocateSize += elem.size_;
		list.push_back(elem);
	}

	{
		ShareValueList::ElemData elem(META_TYPE_ATTRIBUTES);
		elem.size_ = sizeof(ContainerAttribute);
		elem.binary_ = ALLOC_NEW(txn.getDefaultAllocator()) uint8_t[elem.size_];
		memset(elem.binary_, 0, elem.size_);

		ContainerAttribute *attribute =
			reinterpret_cast<ContainerAttribute *>(elem.binary_);
		*attribute = messageSchema->getContainerAttribute();

		allocateSize += elem.size_;
		list.push_back(elem);
	}
	{
		ShareValueList::ElemData elem(META_TYPE_CONTAINER_DURATION);
		elem.size_ = sizeof(ContainerExpirationInfo);
		elem.binary_ =
			ALLOC_NEW(txn.getDefaultAllocator()) uint8_t[elem.size_];
		memset(elem.binary_, 0, elem.size_);

		ContainerExpirationInfo *containerExpirationInfo =
			reinterpret_cast<ContainerExpirationInfo *>(elem.binary_);
		*containerExpirationInfo = messageSchema->getContainerExpirationInfo();

		allocateSize += elem.size_;
		list.push_back(elem);
	}
}

void BaseContainer::finalizeSchema(TransactionContext &txn,
	ObjectManager &objectManager, ShareValueList *commonContainerSchema) {
	ColumnSchema *columnSchema =
		commonContainerSchema->get<ColumnSchema>(META_TYPE_COLUMN_SCHEMA);
	if (columnSchema != NULL) {
		columnSchema->finalize(txn, objectManager);
	}

	CompressionSchema *compressionSchema =
		commonContainerSchema->get<CompressionSchema>(
			META_TYPE_COMPRESSION_SCHEMA);
	if (compressionSchema != NULL) {
		compressionSchema->finalize();
	}



}

bool BaseContainer::schemaCheck(TransactionContext &txn,
	ObjectManager &objectManager, ShareValueList *commonContainerSchema,
	MessageSchema *messageSchema) {
	ColumnSchema *columnSchema =
		commonContainerSchema->get<ColumnSchema>(META_TYPE_COLUMN_SCHEMA);
	if (!columnSchema->schemaCheck(txn, objectManager, messageSchema)) {
		return false;
	}

	CompressionSchema *compressionSchema =
		commonContainerSchema->get<CompressionSchema>(
			META_TYPE_COMPRESSION_SCHEMA);
	if (compressionSchema != NULL) {
		if (messageSchema->getContainerType() != TIME_SERIES_CONTAINER) {
			return false;
		}
		MessageTimeSeriesSchema *messageTimeSeriesSchema =
			reinterpret_cast<MessageTimeSeriesSchema *>(messageSchema);

		if (!compressionSchema->schemaCheck(txn, messageTimeSeriesSchema)) {
			return false;
		}
	}
	else {
		if (messageSchema->getContainerType() != COLLECTION_CONTAINER) {
			return false;
		}
	}

	ExpirationInfo *expirationInfo =
		commonContainerSchema->get<ExpirationInfo>(
			META_TYPE_DURATION);
	if (expirationInfo != NULL) {
		if (messageSchema->getContainerType() != TIME_SERIES_CONTAINER) {
			return false;
		}
		MessageTimeSeriesSchema *messageTimeSeriesSchema =
			reinterpret_cast<MessageTimeSeriesSchema *>(messageSchema);

		if (!(*expirationInfo ==
				messageTimeSeriesSchema->getExpirationInfo())) {
			return false;
		}
	}
	else {
		if (messageSchema->getContainerType() != COLLECTION_CONTAINER) {
			return false;
		}
	}

	const uint8_t *affinityBinary = commonContainerSchema->get<uint8_t>(META_TYPE_AFFINITY);

	char inputAffinityStr[AFFINITY_STRING_MAX_LENGTH];
	memset(inputAffinityStr, 0, sizeof(char[AFFINITY_STRING_MAX_LENGTH]));

	const util::String &affinityString = messageSchema->getAffinityStr();
	memcpy(inputAffinityStr, affinityString.c_str(), affinityString.length());

	if (memcmp(affinityBinary, inputAffinityStr, AFFINITY_STRING_MAX_LENGTH) !=
		0) {
		return false;
	}

	ContainerAttribute *attribute =
		commonContainerSchema->get<ContainerAttribute>(META_TYPE_ATTRIBUTES);
	if (*attribute != messageSchema->getContainerAttribute()) {
		return false;
	}

	ContainerExpirationInfo *containerExpirationInfo =
		commonContainerSchema->get<ContainerExpirationInfo>(
			META_TYPE_CONTAINER_DURATION);
	if (containerExpirationInfo != NULL) {
		if (!(*containerExpirationInfo ==
				messageSchema->getContainerExpirationInfo())) {
			return false;
		}
	}
	else {
		ContainerExpirationInfo defaultExpirationInfo;
		if (!(defaultExpirationInfo ==
				messageSchema->getContainerExpirationInfo())) {
			return false;
		}
	}
	return true;
}

int64_t BaseContainer::calcSchemaHashKey(MessageSchema *messageSchema) {
	int64_t hashVal = 0;
	hashVal += ColumnSchema::calcSchemaHashKey(messageSchema);

	if (messageSchema->getContainerType() == TIME_SERIES_CONTAINER) {
		MessageTimeSeriesSchema *messageTimeSeriesSchema =
			reinterpret_cast<MessageTimeSeriesSchema *>(messageSchema);
		hashVal += CompressionSchema::calcHash(messageTimeSeriesSchema);
	}

	if (messageSchema->getContainerType() == TIME_SERIES_CONTAINER) {
		MessageTimeSeriesSchema *messageTimeSeriesSchema =
			reinterpret_cast<MessageTimeSeriesSchema *>(messageSchema);
		const ExpirationInfo &expirationInfo =
			messageTimeSeriesSchema->getExpirationInfo();
		hashVal += expirationInfo.getHashVal();
	}

	const char *affinityStr = messageSchema->getAffinityStr().c_str();
	for (size_t i = 0; i < strlen(affinityStr); i++) {
		hashVal += static_cast<int64_t>(affinityStr[i]) + 1000000;
	}

	ContainerAttribute attribute = messageSchema->getContainerAttribute();
	hashVal += static_cast<int64_t>(attribute) + 100000000;

	return hashVal;
}

int64_t BaseContainer::calcTriggerHashKey(
	util::XArray<const uint8_t *> &binary) {
	int64_t hashVal = 0;
	util::XArray<const uint8_t *>::iterator itr;
	for (itr = binary.begin(); itr != binary.end(); itr++) {
		uint32_t size;
		int32_t version;
		TriggerInfo::getSizeAndVersion(*itr, size, version);
		for (size_t i = 0; i < size + sizeof(size); i++) {
			hashVal += (*itr)[i];
		}
	}
	return hashVal;
}

bool BaseContainer::triggerCheck(TransactionContext &txn,
	ObjectManager &objectManager, ShareValueList *commonContainerSchema,
	util::XArray<const uint8_t *> &binary) {
	TriggerList *trigger =
		commonContainerSchema->get<TriggerList>(META_TYPE_TRIGGER);
	if (trigger != NULL) {
		util::XArray<const uint8_t *> triggerList(txn.getDefaultAllocator());
		trigger->getList(txn, objectManager, triggerList);
		if (triggerList.size() != binary.size()) {
			return false;
		}

		util::XArray<const uint8_t *>::iterator binaryItr, commonItr;
		for (binaryItr = binary.begin(), commonItr = triggerList.begin();
			 binaryItr != binary.end() && commonItr != triggerList.end();
			 binaryItr++, commonItr++) {
			if (!TriggerInfo::compare(*binaryItr, (*commonItr))) {
				return false;
			}
		}
	}
	else {
		return false;
	}
	return true;
}

void BaseContainer::initializeTrigger(TransactionContext &txn,
	ObjectManager &objectManager, util::XArray<const uint8_t *> &binary,
	const AllocateStrategy &allocateStrategy,
	util::XArray<ShareValueList::ElemData> &list, uint32_t &allocateSize) {
	allocateSize = 0;
	{
		ShareValueList::ElemData elem(META_TYPE_TRIGGER);
		elem.size_ =
			TriggerList::getAllocateSize(static_cast<uint32_t>(binary.size()));
		elem.binary_ = ALLOC_NEW(txn.getDefaultAllocator()) uint8_t[elem.size_];

		TriggerList *trigger = reinterpret_cast<TriggerList *>(elem.binary_);
		trigger->initialize(static_cast<uint32_t>(binary.size()));
		trigger->set(txn, objectManager, binary, allocateStrategy);

		allocateSize += elem.size_;
		list.push_back(elem);
	}
}

IndexCursor BaseContainer::createCursor(TransactionContext &txn, const MvccRowImage &mvccImage) {
	IndexCursor indexCursor(mvccImage);

	IndexData indexData(txn.getDefaultAllocator());
	bool isExist = getIndexData(txn, indexCursor, indexData);
	if (!isExist) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR,
			"can not continue to create index. index data does not existed.");
	}
	indexCursor.setRowId(indexData.cursor_);
	return indexCursor;
}

void BaseContainer::updateValueMaps(TransactionContext &txn, 
									const util::XArray<std::pair<OId, OId> > &moveList) {
	util::StackAllocator::Scope scope(txn.getDefaultAllocator());
	util::XArray<IndexData> indexList(txn.getDefaultAllocator());
	bool withUncommitted = true;
	getIndexList(txn, withUncommitted, indexList);

	if (!indexList.empty() && !moveList.empty()) {
		RowArray moveRowArray(txn, this);
		for (size_t i = 0; i < indexList.size(); i++) {
			ValueMap valueMap(txn, this, indexList[i]);

			util::XArray<std::pair<OId, OId> >::const_iterator itr;
			for (itr = moveList.begin(); itr != moveList.end(); itr++) {
				moveRowArray.load(txn, itr->second, this, OBJECT_READ_ONLY);
				RowArray::Row row(moveRowArray.getRow(), &moveRowArray);
				if (indexList[i].cursor_ < row.getRowId()) {
					continue;
				}
				bool isNullValue;
				const void *fieldValue = row.getFields<TimeSeries>(txn, 
					valueMap.getFuncInfo(txn), isNullValue);
				updateValueMap(txn, valueMap, fieldValue, itr->first,
					itr->second, isNullValue);
			}
		}
	}
}

void BaseContainer::convertRowArraySchema(TransactionContext &txn, RowArray &rowArray, bool isMvcc) {
	RowId orgRowId = rowArray.getRowId();
	OId orgOId = rowArray.getBaseOId();
	util::XArray<std::pair<RowId, OId> > splitRAList(txn.getDefaultAllocator());
	util::XArray<std::pair<OId, OId> > moveOIdList(txn.getDefaultAllocator());
	bool isRelease = rowArray.convertSchema(txn, splitRAList, moveOIdList);
	if (!isMvcc) {
		{
			StackAllocAutoPtr<BtreeMap> rowIdMap(
				txn.getDefaultAllocator(), getRowIdMap(txn));
			if (isRelease) {
				removeRowIdMap(txn, rowIdMap.get(), &orgRowId, orgOId);
			}
			for (size_t i = 0; i < splitRAList.size(); i++) {
				RowId rowId = splitRAList[i].first;
				OId baseOId = splitRAList[i].second;
				insertRowIdMap(txn, rowIdMap.get(), &rowId, baseOId);
			}
		}
		updateValueMaps(txn, moveOIdList);
	}
}

uint16_t BaseContainer::calcRowArrayNumBySize(uint32_t binarySize, uint32_t nullbitsSize) {
	if (binarySize > RowArray::calcHeaderSize(nullbitsSize)) {
		return (binarySize - RowArray::calcHeaderSize(nullbitsSize)) / rowImageSize_;
	} else {
		return 0;
	}
}

uint16_t BaseContainer::calcRowArrayNum(TransactionContext& txn, bool sizeControlMode, uint16_t baseRowNum) {
	uint32_t headerSize = RowArray::calcHeaderSize(getNullbitsSize());
	uint16_t rowArrayNum;	
	if (sizeControlMode) {
		rowArrayNum = calcRowArrayNumByConfig(txn, headerSize);
	} else {
		rowArrayNum = calcRowArrayNumByBaseRowNum(txn, baseRowNum, headerSize);
	}
	return rowArrayNum;
}

uint16_t BaseContainer::calcRowArrayNumByConfig(TransactionContext &txn, uint32_t rowArrayHeaderSize) {
	UNUSED_VARIABLE(txn);
	uint32_t maxObjectSize = getObjectManager()->getMaxObjectSize();
	uint16_t estimateRowNum = 0;
	for (int32_t i = dataStore_->getConfig().getRowArrayRateExponent(); ; i--) {
		uint32_t estimateRowAreaSize =
			getObjectManager()->getAllocateSize(i) - rowArrayHeaderSize;
		if (estimateRowAreaSize > maxObjectSize) {
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_CM_FAILED, "can not calc row array size ");
		}
		if (estimateRowAreaSize >= rowImageSize_) {
			estimateRowNum = estimateRowAreaSize / rowImageSize_;
			if (estimateRowNum > ROW_ARRAY_LIMIT_SIZE) {
				uint32_t requestObjectSize = rowArrayHeaderSize + rowImageSize_ * ROW_ARRAY_MAX_ESTIMATE_SIZE;
				estimateRowAreaSize =
					getObjectManager()->estimateAllocateSize(requestObjectSize) - rowArrayHeaderSize;
				estimateRowNum = estimateRowAreaSize / rowImageSize_;
			}
			break;
		}
	}
	assert(estimateRowNum != 0);

	return estimateRowNum;
}

uint16_t BaseContainer::calcRowArrayNumByBaseRowNum(TransactionContext& txn, uint16_t baseRowNum, uint32_t rowArrayHeaderSize) {
	UNUSED_VARIABLE(txn);
	uint32_t pointArrayObjectSize = static_cast<uint32_t>(
		rowArrayHeaderSize + rowImageSize_ * baseRowNum);
	Size_t estimateAllocateSize =
		getObjectManager()->estimateAllocateSize(pointArrayObjectSize);
	if (estimateAllocateSize > getObjectManager()->getHalfOfMaxObjectSize()) {
		uint32_t oneRowObjectSize = static_cast<uint32_t>(
			rowArrayHeaderSize + rowImageSize_ * 1);
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
		(estimateAllocateSize - rowArrayHeaderSize) / rowImageSize_);
}

util::String BaseContainer::getBibInfoImpl(TransactionContext &txn, const char* dbName, uint64_t pos, bool isEmptyId) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();
	util::NormalOStringStream strstrm;
	try {
		FullContainerKey containerKey = getContainerKey(txn);
		FullContainerKeyComponents keyComponents = containerKey.getComponents(txn.getDefaultAllocator());
		util::String containerName(alloc);
		getContainerKey(txn).toString(alloc, containerName);

		strstrm << "{" << std::endl;
		strstrm << "\"containerFileBase\": \"" << containerName << "." << pos << "\"," << std::endl; 

		strstrm << "\"containerAttribute\": {" << std::endl;

		strstrm << "\"databaseId\": \"" << keyComponents.dbId_ << "\"," << std::endl; 
		strstrm << "\"containerId\": \"" << getContainerId() << "\"," << std::endl; 
		if (isEmptyId) {
			strstrm << "\"rowIndexOId\": \"" << UNDEF_OID << "\"," << std::endl; 
			strstrm << "\"mvccIndexOId\": \"" << UNDEF_OID << "\"," << std::endl; 
		} else {
			strstrm << "\"rowIndexOId\": \"" << baseContainerImage_->rowIdMapOId_ << "\"," << std::endl; 
			strstrm << "\"mvccIndexOId\": \"" << baseContainerImage_->mvccMapOId_ << "\"," << std::endl; 
		}
		strstrm << "\"initSchemaStatus\": " << "null" << "," << std::endl; 
		strstrm << "\"schemaVersion\": " << getVersionId() << "," << std::endl; 

		strstrm << "\"database\": \"" << dbName << "\"," << std::endl; 
		strstrm << "\"container\": \"" << containerName << "\"," << std::endl; 
		strstrm << "\"partitionNo\": " << txn.getPartitionId() << "," << std::endl; 

		strstrm << "\"expirableType\": " << "null" << "," << std::endl; 
		strstrm << "\"startTime\": " << "null" << "," << std::endl; 
		strstrm << "\"endTime\": " << "null" << "," << std::endl; 
		strstrm << "\"expiredTime\": " << "null" << "," << std::endl; 
		strstrm << "\"erasableTime\": " << "null" << "," << std::endl; 

		strstrm << "\"largeContainerId\": \"" << keyComponents.largeContainerId_ << "\"," << std::endl; 

		strstrm << "\"tableExpirationElapsedTime\": " << "null" << "," << std::endl; 
		strstrm << "\"tableExpirationTimeUnit\": " << "null" << "," << std::endl; 
		strstrm << "\"interval\": " << "null" << std::endl; 


		strstrm << "," << std::endl;
		strstrm << "\"containerType\": \"" << BibInfoUtil::getContainerTypeStr(getContainerType()) << "\"," << std::endl;
		strstrm << "\"columnSet\": [" << std::endl;
		for (uint32_t i = 0; i < getColumnNum(); i++) {
			if (i != 0) {
				strstrm << "," << std::endl;
			}
			strstrm << "{" << std::endl;
			ColumnInfo &columnInfo = getColumnInfo(i);
			strstrm << "\"columnName\": \"" << columnInfo.getColumnName(txn, *getObjectManager()) << "\"," << std::endl; 
			strstrm << "\"type\": \"" << BibInfoUtil::getColumnTypeStr(columnInfo.getColumnType()) << "\"," << std::endl;
			if (columnInfo.isNotNull()) {
				strstrm << "\"notNull\": true" << std::endl;
			} else {
				strstrm << "\"notNull\": false" << std::endl;
			}
			strstrm << "}" << std::endl;
		}
		strstrm << "]" << std::endl;

		strstrm << "," << std::endl;
		util::Vector<ColumnId> keyColumnIdList(txn.getDefaultAllocator());
		getKeyColumnIdList(keyColumnIdList);
		strstrm << "\"rowKeySet\": [" << std::endl;
		for (uint32_t i = 0; i < keyColumnIdList.size(); i++) {
			ColumnId columnId = keyColumnIdList[i];
			if (i != 0) {
				strstrm << "," << std::endl;
			}
			ColumnInfo &columnInfo = getColumnInfo(columnId);
			strstrm << "\"" << columnInfo.getColumnName(txn, *getObjectManager()) << "\"" << std::endl; 
		}
		strstrm << "]" << std::endl;

		strstrm << "," << std::endl;
		char affinityStr[AFFINITY_STRING_MAX_LENGTH + 1];
		memcpy(affinityStr, getAffinityBinary(), AFFINITY_STRING_MAX_LENGTH);
		affinityStr[AFFINITY_STRING_MAX_LENGTH] =
			'\0';  
		strstrm << "\"dataAffinity\": \"" << affinityStr << "\"" << std::endl;

		strstrm << getBibContainerOptionInfo(txn) << std::endl;
		strstrm << "}" << std::endl;
		strstrm << "}" << std::endl;
	}
	catch (std::exception &e) {
		handleSearchError(txn, e, GS_ERROR_DS_COL_GET_COLINFO_FAILED);
	}
	util::String result(alloc);
	result = strstrm.str().c_str();
	return result;
}

template <typename T>
int32_t BaseContainer::ValueMap::search(TransactionContext &txn, 
	typename T::SearchContext &sc, util::XArray<OId> &oIdList,
	OutputOrder outputOrder) {

	bool isNullLast = outputOrder == ORDER_ASCENDING;
	if (sc.getNullCond() != BaseIndex::SearchContext::NOT_IS_NULL && !isNullLast) {
		BtreeMap::SearchContext nullSc (txn.getDefaultAllocator(), UNDEF_COLUMNID);
		nullSc.setLimit(sc.getLimit());
		BtreeMap *nullMap = static_cast<BtreeMap *>(getValueMap(txn, true));
		if (nullMap != NULL) {
			nullMap->search(txn, nullSc, oIdList, outputOrder);
		}
		sc.setLimit(sc.getLimit() - oIdList.size());
	}

	if (sc.getNullCond() != BaseIndex::SearchContext::IS_NULL) {
		T *valueMap = static_cast<T *>(getValueMap(txn, false));
		if (valueMap != NULL) {
			valueMap->search(txn, sc, oIdList, outputOrder);
		}
		sc.setLimit(sc.getLimit() - oIdList.size());
	}

	if (sc.getNullCond() != BaseIndex::SearchContext::NOT_IS_NULL && isNullLast) {
		BtreeMap::SearchContext nullSc (txn.getDefaultAllocator(), UNDEF_COLUMNID);
		nullSc.setLimit(sc.getLimit());
		BtreeMap *nullMap = static_cast<BtreeMap *>(getValueMap(txn, true));
		if (nullMap != NULL) {
			nullMap->search(txn, nullSc, oIdList, outputOrder);
		}
		sc.setLimit(sc.getLimit() - oIdList.size());
	}

	return GS_SUCCESS;
}

template <>
int32_t BaseContainer::ValueMap::search<BtreeMap>(TransactionContext &txn, 
	BtreeMap::SearchContext &sc, util::XArray<OId> &oIdList,
	OutputOrder outputOrder) {

	bool isNullLast = outputOrder == ORDER_ASCENDING;
	switch (sc.getNullCond()) {
	case BaseIndex::SearchContext::IS_NULL: {
			BtreeMap *nullMap = static_cast<BtreeMap *>(getValueMap(txn, true));
			if (nullMap != NULL) {
				nullMap->search(txn, sc, oIdList, outputOrder);
			}
			sc.setLimit(sc.getLimit() - oIdList.size());
		}
		break;
	case BaseIndex::SearchContext::NOT_IS_NULL:
		static_cast<BtreeMap *>(getValueMap(txn, false))->search(txn, sc, oIdList, outputOrder);
		sc.setLimit(sc.getLimit() - oIdList.size());
		break;
	case BaseIndex::SearchContext::ALL:
		if (!isNullLast) {
			if (sc.getResumeStatus() == BaseIndex::SearchContext::NOT_RESUME) {
				BtreeMap::SearchContext nullSc (txn.getDefaultAllocator(), UNDEF_COLUMNID);
				nullSc.setLimit(sc.getLimit());
				BtreeMap *nullMap = static_cast<BtreeMap *>(getValueMap(txn, true));
				if (nullMap != NULL) {
					nullMap->search(txn, nullSc, oIdList, outputOrder);
				}
				sc.setNullSuspended(nullSc.isSuspended());
			} else if (sc.getResumeStatus() == BaseIndex::SearchContext::NULL_RESUME) {
				BtreeMap::SearchContext nullSc = sc;
				BtreeMap *nullMap = static_cast<BtreeMap *>(getValueMap(txn, true));
				if (nullMap != NULL) {
					nullMap->search(txn, nullSc, oIdList, outputOrder);
				}
				sc.setNullSuspended(nullSc.isSuspended());
			} else {
			}
			sc.setLimit(sc.getLimit() - oIdList.size());
			sc.setSuspendLimit(sc.getSuspendLimit() - oIdList.size());
		}

		if (!sc.isNullSuspended()) {
			if (sc.getResumeStatus() != BaseIndex::SearchContext::NULL_RESUME) {
				static_cast<BtreeMap *>(getValueMap(txn, false))->search(txn, sc, oIdList, outputOrder);
			} else if (isNullLast) {
				BtreeMap::SearchContext nullSc (txn.getDefaultAllocator(), UNDEF_COLUMNID);
				nullSc.setLimit(sc.getLimit());
				static_cast<BtreeMap *>(getValueMap(txn, false))->search(txn, nullSc, oIdList, outputOrder);
				sc.setSuspended(nullSc.isSuspended());
			} else {
			}
			sc.setLimit(sc.getLimit() - oIdList.size());
			sc.setSuspendLimit(sc.getSuspendLimit() - oIdList.size());
		}

		if (!sc.isSuspended() && isNullLast) {
			if (sc.getResumeStatus() != BaseIndex::SearchContext::NULL_RESUME) {
				BtreeMap::SearchContext nullSc (txn.getDefaultAllocator(), UNDEF_COLUMNID);
				nullSc.setLimit(sc.getLimit());
				BtreeMap *nullMap = static_cast<BtreeMap *>(getValueMap(txn, true));
				if (nullMap != NULL) {
					nullMap->search(txn, nullSc, oIdList, outputOrder);
				}
				sc.setNullSuspended(nullSc.isSuspended());
			} else {
				BtreeMap::SearchContext nullSc = sc;
				BtreeMap *nullMap = static_cast<BtreeMap *>(getValueMap(txn, true));
				if (nullMap != NULL) {
					nullMap->search(txn, nullSc, oIdList, outputOrder);
				}
				sc.setNullSuspended(nullSc.isSuspended());
			}
			sc.setLimit(sc.getLimit() - oIdList.size());
			sc.setSuspendLimit(sc.getSuspendLimit() - oIdList.size());
		}
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
	}
	return 0;
}

ContainerRowScanner::ContainerRowScanner(
		const HandlerSet &handlerSet, RowArray &rowArray,
		VirtualValueList *virtualValueList) :
		handlerSet_(handlerSet),
		rowArray_(rowArray),
		virtualValueList_(virtualValueList) {
	assert(handlerSet_.getEntry<
			BaseContainer::ROW_ARRAY_GENERAL>().isAvalilable());
	assert(handlerSet_.getEntry<
			BaseContainer::ROW_ARRAY_PLAIN>().isAvalilable());
}

ContainerRowScanner::HandlerEntry::HandlerEntry() :
		rowHandler_(NULL),
		rowArrayHandler_(NULL),
		handlerValue_(NULL) {
}

bool ContainerRowScanner::HandlerEntry::isAvalilable() const {
	return (handlerValue_ != NULL);
}

void BaseContainer::RsNotifier::addUpdatedRow(PartitionId pId, RowId rowId, OId oId) {
	assert(mode_ != TO_UNDEF);
	dataStore_.addUpdatedRow(pId, containerId_, rowId, oId, mode_ == TO_MVCC);
}


