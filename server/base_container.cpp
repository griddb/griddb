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
			util::XArray<ColumnId> keyColumnIdList(txn.getDefaultAllocator());
			getKeyColumnIdList(keyColumnIdList);
			int16_t rowKeyNum = static_cast<int16_t>(keyColumnIdList.size());
			containerSchema.push_back(
				reinterpret_cast<uint8_t *>(&rowKeyNum), sizeof(int16_t));
			util::XArray<ColumnId>::iterator itr;
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
			BtreeMap::SearchContext sc(
				UNDEF_COLUMNID, &tId, sizeof(tId), 0, NULL, MAX_RESULT_SIZE);
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
			BtreeMap::SearchContext sc(
				UNDEF_COLUMNID, &tId, sizeof(tId), 0, NULL, MAX_RESULT_SIZE);
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
		ColumnInfo &targetColumnInfo = getColumnInfo(sc.columnId_);
		mergeRowList<R>(txn, targetColumnInfo, mvccRowList, false, normalRowList,
			true, resultList, outputOrder);
	}
	else {
		resultList.swap(normalRowList);
		resultList.insert(resultList.begin(), mvccRowList.begin(), mvccRowList.end());
	}
	if (resultList.size() > sc.limit_) {
		resultList.resize(sc.limit_);
	}
}


template <typename R, typename T>
void BaseContainer::searchColumnIdIndex(TransactionContext &txn, MapType mapType, 
	typename T::SearchContext &sc, util::XArray<OId> &normalRowList, 
	util::XArray<OId> &mvccRowList, OutputOrder outputOrder, bool ignoreTxnCheck) {

	const Operator *op1, *op2;
	bool isValid = getKeyCondition(txn, sc, op1, op2);
	if (!isValid) {
		return;
	}

	{
		ResultSize limitBackup = sc.limit_;
		if (outputOrder != ORDER_UNDEFINED) {
			sc.limit_ = MAX_RESULT_SIZE;
		}
		bool isCheckOnly = false;
		searchMvccMap<R, typename T::SearchContext>(txn, sc, mvccRowList, isCheckOnly, ignoreTxnCheck);

		sc.limit_ = limitBackup;

		if (outputOrder == ORDER_UNDEFINED && mvccRowList.size() >= sc.limit_) {
			mvccRowList.resize(sc.limit_);
			return;
		}
	}

	{
		util::XArray<OId> oIdList(txn.getDefaultAllocator());
		ResultSize limitBackup = sc.limit_;
		if (sc.conditionNum_ > 0 || !isExclusive()) {
			sc.limit_ = MAX_RESULT_SIZE;
		} else {
			sc.limit_ -= mvccRowList.size();
		}
		bool withUncommitted = false;
		IndexData indexData;
		getIndexData(txn, sc.columnId_, mapType, withUncommitted, indexData);
		ValueMap valueMap(txn, this, indexData);
		valueMap.search<T>(txn, sc, oIdList, outputOrder);
		sc.limit_ = limitBackup;

		ContainerValue containerValue(txn.getPartitionId(), *getObjectManager());
		util::XArray<OId>::iterator itr;
		if (sc.conditionNum_ > 0 || (!isExclusive() && !ignoreTxnCheck)) {
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
				for (uint32_t c = 0; c < sc.conditionNum_; c++) {
					if (!row.isMatch(txn, sc.conditionList_[c], containerValue)) {
						isMatch = false;
						break;
					}
				}
				if (isMatch) {
					normalRowList.push_back(rowArray.getOId());
					if (normalRowList.size() + mvccRowList.size() == sc.limit_) {
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

	util::XArray<ColumnId> keyColumnIdList(txn.getDefaultAllocator());
	getKeyColumnIdList(keyColumnIdList);
	const util::XArray<ColumnId> &schemaKeyColumnIdList =  messageSchema->getRowKeyColumnIdList();
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
//		bool isNotNull = messageSchema->getIsNotNull(i);

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
		memcpy(affinityStr, getAffinity(), AFFINITY_STRING_MAX_LENGTH);
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


	checkContainerOption(messageSchema, copyColumnMap, isCompletelySameSchema);

	if (isCompletelySameSchema && !isNullableChange && !isVersionChange) {
		schemaState = DataStore::SAME_SCHEMA;
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
		BtreeMap::SearchContext sc(
			UNDEF_COLUMNID, &tId, sizeof(tId), 0, NULL, MAX_RESULT_SIZE);
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
	ColumnId inputColumnId = indexInfo.columnIds_[0];
	MapType inputMapType = indexInfo.mapType;

	ColumnInfo &columnInfo = getColumnInfo(inputColumnId);
	if (columnInfo.isArray()) {
		return false;
	}
	if (inputMapType < 0 || inputMapType >= MAP_TYPE_NUM) {
		return false;
	}
	bool isSuport;
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

		ColumnInfo &columnInfo = getColumnInfo(indexData.columnId_);
		BtreeMap::SearchContext sc(UNDEF_COLUMNID, &indexData.cursor_, 0, false,
			NULL, 0, false, 0, NULL, limit);

		util::XArray<OId> oIdList(txn.getDefaultAllocator());
		util::XArray<OId>::iterator itr;

		StackAllocAutoPtr<BtreeMap> rowIdMap(
			txn.getDefaultAllocator(), getRowIdMap(txn));
		rowIdMap.get()->search(txn, sc, oIdList, ORDER_UNDEFINED);
		RowArray rowArray(txn, this);
		for (itr = oIdList.begin(); itr != oIdList.end(); itr++) {
			rowArray.load(txn, *itr, this, OBJECT_READ_ONLY);
			for (rowArray.begin(); !rowArray.end(); rowArray.next()) {
				RowArray::Row row(rowArray.getRow(), &rowArray);
				if (indexData.cursor_ >= row.getRowId()) {
					continue;
				}
				bool isNullValue = row.isNullValue(columnInfo);
				BaseObject baseFieldObject(
					txn.getPartitionId(), *getObjectManager());
				const void *fieldValue = &NULL_VALUE;
				if (!isNullValue) {
					row.getField(txn, columnInfo, baseFieldObject);
					fieldValue = baseFieldObject.getCursor<void>();
				}
				insertValueMap(txn, valueMap, fieldValue, rowArray.getOId(),
					isNullValue);
				lastRowId = row.getRowId();
			}
			indexData.cursor_ = lastRowId;
		}
		if (oIdList.size() < limit) {
			indexData.cursor_ = MAX_ROWID;
		}
	} else {
		ColumnInfo &columnInfo = getColumnInfo(indexData.columnId_);
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
					bool isNullValue = row.isNullValue(columnInfo);
					BaseObject baseFieldObject(
						txn.getPartitionId(), *getObjectManager());
					const void *fieldValue = &NULL_VALUE;
					if (!isNullValue) {
						row.getField(txn, columnInfo, baseFieldObject);
						fieldValue = baseFieldObject.getCursor<void>();
					}
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

bool BaseContainer::getKeyCondition(TransactionContext &txn,
	BtreeMap::SearchContext &sc, const Operator *&op1,
	const Operator *&op2) const {
	op1 = NULL;
	op2 = NULL;
	bool isValid = true;

	if (sc.nullCond_ != BaseIndex::SearchContext::NOT_IS_NULL) {
	} else
	if (sc.startKey_ != NULL || sc.endKey_ != NULL) {
		if (sc.columnId_ >= getColumnNum() && sc.columnId_ != UNDEF_COLUMNID) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_COLUMN_ID_INVALID, "");
		}
		ColumnType targetType;
		if (sc.columnId_ != UNDEF_COLUMNID) {
			ColumnInfo &columnInfo = getColumnInfo(sc.columnId_);
			targetType = columnInfo.getColumnType();
		}
		else {
			targetType = COLUMN_TYPE_ROWID;
		}
		if (ComparatorTable::eqTable_[targetType][targetType] == NULL) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_DATA_CANNOT_COMPARE, "");
		}
		if (sc.startKey_ != NULL && sc.endKey_ != NULL) {
			if (ComparatorTable::eqTable_[targetType][targetType](txn,
					reinterpret_cast<const uint8_t *>(sc.startKey_),
					sc.startKeySize_,
					reinterpret_cast<const uint8_t *>(sc.endKey_),
					sc.endKeySize_)) {
				if (!sc.isStartKeyIncluded_ && !sc.isEndKeyIncluded_) {
					isValid = false;
				}
				op1 = &ComparatorTable::eqTable_[targetType][targetType];
			}
			else {
				if (sc.isStartKeyIncluded_) {
					op1 = &ComparatorTable::geTable_[targetType][targetType];
				}
				else {
					op1 = &ComparatorTable::gtTable_[targetType][targetType];
				}
				if (sc.isEndKeyIncluded_) {
					op2 = &ComparatorTable::leTable_[targetType][targetType];
				}
				else {
					op2 = &ComparatorTable::ltTable_[targetType][targetType];
				}
				if ((*op1)(txn, reinterpret_cast<const uint8_t *>(sc.startKey_),
						sc.startKeySize_,
						reinterpret_cast<const uint8_t *>(sc.endKey_),
						sc.endKeySize_)) {
					isValid = false;
				}
			}
		}
		else if (sc.startKey_ != NULL) {
			if (sc.isStartKeyIncluded_) {
				op1 = &ComparatorTable::geTable_[targetType][targetType];
			}
			else {
				op1 = &ComparatorTable::gtTable_[targetType][targetType];
			}
			if (*op1 == NULL) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_INTERNAL_DATA_CANNOT_COMPARE, "");
			}
		}
		else if (sc.endKey_ != NULL) {
			if (sc.isEndKeyIncluded_) {
				op2 = &ComparatorTable::leTable_[targetType][targetType];
			}
			else {
				op2 = &ComparatorTable::ltTable_[targetType][targetType];
			}
			if (*op2 == NULL) {
				GS_THROW_USER_ERROR(
					GS_ERROR_TQ_INTERNAL_DATA_CANNOT_COMPARE, "");
			}
		}
	}
	return isValid;
}

bool BaseContainer::getKeyCondition(TransactionContext &,
	HashMap::SearchContext &sc, const Operator *&op1,
	const Operator *&op2) const {
	op1 = NULL;
	op2 = NULL;
	bool isValid = true;
	if (sc.nullCond_ != BaseIndex::SearchContext::NOT_IS_NULL) {
	} else
	if (sc.key_ != NULL) {
		if (sc.columnId_ >= getColumnNum() && sc.columnId_ != UNDEF_COLUMNID) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_COLUMN_ID_INVALID, "");
		}
		ColumnType targetType;
		if (sc.columnId_ != UNDEF_COLUMNID) {
			ColumnInfo &columnInfo = getColumnInfo(sc.columnId_);
			targetType = columnInfo.getColumnType();
		}
		else {
			targetType = COLUMN_TYPE_ROWID;
		}
		op1 = &ComparatorTable::eqTable_[targetType][targetType];
		if (op1 == NULL) {
			GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_DATA_CANNOT_COMPARE, "");
		}
	}
	return isValid;
}

bool BaseContainer::getKeyCondition(TransactionContext &,
	RtreeMap::SearchContext &, const Operator *&op1,
	const Operator *&op2) const {
	op1 = NULL;
	op2 = NULL;
	bool isValid = true;
	return isValid;
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
		for (uint32_t columnId = 0; columnId < copyColumnMap.size();
			 columnId++) {
			uint32_t oldColumnId = copyColumnMap[columnId];
			if (oldColumnId != UNDEF_COLUMNID) {
				for (size_t i = 0; i < oldIndexInfoList.size(); i++) {
					const IndexInfo &oldIndexInfo = oldIndexInfoList[i];
					
					
					if (!oldIndexInfo.columnIds_.empty()
						&& oldIndexInfo.columnIds_[0] == oldColumnId) {
						IndexInfo newIndexInfo(txn.getDefaultAllocator(),
							oldIndexInfo.indexName_, columnId,
							oldIndexInfo.mapType);

						bool isImmediate = true;
						IndexCursor indexCusror(isImmediate);

						newContainer.createIndex(txn, newIndexInfo, indexCusror);
					}
				}
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
		BtreeMap::SearchContext sc(UNDEF_COLUMNID, &cursor, 0, false,
			NULL, 0, false, 0, NULL, limit);

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
	if (baseContainerImage_->columnSchemaOId_ != UNDEF_OID) { 
		getDataStore()->removeColumnSchema(
			txn, txn.getPartitionId(), baseContainerImage_->columnSchemaOId_);
	}
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

	const Operator *op1, *op2;
	bool isValid = getKeyCondition(txn, sc, op1, op2);
	if (!isValid) {
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
						if (sc.columnId_ == UNDEF_COLUMNID) {
							containerValue.set(row.getRowId());
						}
						else {
							row.getField(txn, getColumnInfo(sc.columnId_), containerValue);
						}
						if (!checkScColumnKey(
								txn, sc, containerValue.getValue(), op1, op2)) {
							continue;
						}
						bool isMatch = true;
						for (uint32_t c = 0; c < sc.conditionNum_; c++) {
							if (!row.isMatch(txn, sc.conditionList_[c], containerValue)) {
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
			break;
		default:
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
			break;
		}
	}
	if (resultList.size() > sc.limit_) {
		resultList.resize(sc.limit_);
	}
}


template <class R, class S>
void BaseContainer::searchColumnId(TransactionContext &txn, S &sc,
	util::XArray<OId> &oIdList, util::XArray<OId> &mvccList,
	util::XArray<OId> &resultList, OutputOrder outputOrder) {

	ContainerValue containerValue(txn, *getObjectManager());
	util::XArray<OId>::iterator itr;
	if (sc.conditionNum_ > 0 || !isExclusive()) {
		RowArray rowArray(txn, reinterpret_cast<R *>(this));
		for (itr = oIdList.begin(); itr != oIdList.end(); itr++) {
			rowArray.load(
				txn, *itr, reinterpret_cast<R *>(this), OBJECT_READ_ONLY);
			RowArray::Row row(rowArray.getRow(), &rowArray);

			if (txn.getId() != row.getTxnId() && !row.isFirstUpdate() &&
				txn.getManager().isActiveTransaction(
					txn.getPartitionId(), row.getTxnId())) {
				continue;
			}
			bool isMatch = true;
			for (uint32_t c = 0; c < sc.conditionNum_; c++) {
				if (!row.isMatch(txn, sc.conditionList_[c], containerValue)) {
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
		resultList.swap(oIdList);
	}

	if (!mvccList.empty() && outputOrder != ORDER_UNDEFINED) {
		util::XArray<OId> mergeList(txn.getDefaultAllocator());
		ColumnInfo &targetColumnInfo = getColumnInfo(sc.columnId_);
		mergeRowList<R>(txn, targetColumnInfo, mvccList, false, resultList,
			true, mergeList, outputOrder);
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

bool BaseContainer::checkScColumnKey(TransactionContext &txn,
	BtreeMap::SearchContext &sc, const Value &value, const Operator *op1,
	const Operator *op2) {
	bool isMatch = false;
	if (value.isNullValue()) {
		isMatch = sc.nullCond_ != BaseIndex::SearchContext::NOT_IS_NULL;
	} else
	if (((op1 == 0) || (*op1)(txn, value.data(), value.size(),
						   reinterpret_cast<const uint8_t *>(sc.startKey_),
						   sc.startKeySize_)) &&
		((op2 == 0) || (*op2)(txn, value.data(), value.size(),
						   reinterpret_cast<const uint8_t *>(sc.endKey_),
						   sc.endKeySize_))) {
		isMatch = true;
	}
	else {
		isMatch = false;
	}
	return isMatch;
}

bool BaseContainer::checkScColumnKey(TransactionContext &txn,
	HashMap::SearchContext &sc, const Value &value, const Operator *op1,
	const Operator *) {
	bool isMatch = false;
	if (value.isNullValue()) {
		isMatch = sc.nullCond_ != BaseIndex::SearchContext::NOT_IS_NULL;
	} else
	if ((op1 == 0) ||
		(*op1)(txn, value.data(), value.size(),
			reinterpret_cast<const uint8_t *>(sc.key_), sc.keySize_)) {
		isMatch = true;
	}
	else {
		isMatch = false;
	}
	return isMatch;
}

bool BaseContainer::checkScColumnKey(TransactionContext &txn,
	RtreeMap::SearchContext &sc, const Value &value, const Operator *,
	const Operator *) {

	bool isMatch = false;
	if (value.isNullValue()) {
		isMatch = sc.nullCond_ != BaseIndex::SearchContext::NOT_IS_NULL;
	} else
	{
	Geometry *geom = Geometry::deserialize(txn, value.data(), value.size());
	switch (sc.relation_) {
	case GEOMETRY_INTERSECT:
		if (geom->isEmpty()) {
			isMatch = false;
		}
		else {
			const TrRectTag &a = sc.rect_[0];
			const TrRectTag &b = geom->getBoundingRect();
			isMatch = ((a.xmin <= b.xmax) && (b.xmin <= a.xmax) &&
					   ((a.ymin <= b.ymax) && (b.ymin <= a.ymax)) &&
					   (geom->getDimension() <= 2 ||
						   ((a.zmin <= b.zmax) && (b.zmin <= a.zmax))));
		}
		break;
	case GEOMETRY_INCLUDE:
		if (geom->isEmpty()) {
			isMatch = false;
		}
		else {
			const TrRectTag &out = sc.rect_[0];
			const TrRectTag &in = geom->getBoundingRect();
			isMatch = (out.xmin <= in.xmin && out.xmax >= in.xmax &&
					   out.ymin <= in.ymin && out.ymax >= in.ymax &&
					   (geom->getDimension() <= 2 ||
						   (out.zmin <= in.zmin && out.zmax >= in.zmax)));
		}
		break;
	case GEOMETRY_DIFFERENTIAL:
		if (geom->isEmpty()) {
			isMatch = false;
		}
		else {
			const TrRectTag &a = sc.rect_[0];
			const TrRectTag &a2 = sc.rect_[1];
			const TrRectTag &b = geom->getBoundingRect();
			bool conditionFlag1 =
				((a.xmin <= b.xmax) && (b.xmin <= a.xmax) &&
					((a.ymin <= b.ymax) && (b.ymin <= a.ymax)) &&
					(geom->getDimension() <= 2 ||
						((a.zmin <= b.zmax) && (b.zmin <= a.zmax))));
			bool conditionFlag2 =
				((a2.xmin <= b.xmax) && (b.xmin <= a2.xmax) &&
					((a2.ymin <= b.ymax) && (b.ymin <= a2.ymax)) &&
					(geom->getDimension() <= 2 ||
						((a2.zmin <= b.zmax) && (b.zmin <= a2.zmax))));
			isMatch = conditionFlag1 && !conditionFlag2;
		}
		break;
	case GEOMETRY_QSF_INTERSECT:

		if (geom->isEmpty()) {
			isMatch = false;
		}
		else {
			const TrRectTag r = geom->getBoundingRect();
			TrPv3Box box;
			box.p0[0] = r.xmin;
			box.p0[1] = r.ymin;
			box.p0[2] = r.zmin;
			box.p1[0] = r.xmax - r.xmin;
			box.p1[1] = r.ymax - r.ymin;
			box.p1[2] = r.zmax - r.zmin;
			isMatch =
				(TrPv3Test2(&box, const_cast<TrPv3Key *>(&sc.pkey_)) != 0);
		}
		break;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_TQ_INTERNAL_GIS_UNKNOWN_RELATIONSHIP,
			"Invalid relationship of Geometry is specified.");
	}
	}
	return isMatch;
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
	memcpy(affinityStr, getAffinity(), AFFINITY_STRING_MAX_LENGTH);
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

	char *affinityStr = commonContainerSchema->get<char>(META_TYPE_AFFINITY);

	char inputAffinityStr[AFFINITY_STRING_MAX_LENGTH];
	memset(inputAffinityStr, 0, sizeof(char[AFFINITY_STRING_MAX_LENGTH]));

	const util::String &affinityString = messageSchema->getAffinityStr();
	memcpy(inputAffinityStr, affinityString.c_str(), affinityString.length());

	if (memcmp(affinityStr, inputAffinityStr, AFFINITY_STRING_MAX_LENGTH) !=
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

	IndexData indexData;
	bool withUncommitted = true;
	bool isExist = getIndexData(txn, indexCursor.getColumnId(), indexCursor.getMapType(), 
		withUncommitted, indexData);
	if (!isExist) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR,
			"can not continue to create index. index data does not existed.");
	}
	indexCursor.setRowId(indexData.cursor_);
	return indexCursor;
}

void BaseContainer::updateValueMaps(TransactionContext &txn, 
									const util::XArray<std::pair<OId, OId> > &moveList) {
	util::XArray<IndexData> indexList(txn.getDefaultAllocator());
	bool withUncommitted = true;
	getIndexList(txn, withUncommitted, indexList);

	if (!indexList.empty() && !moveList.empty()) {
		RowArray moveRowArray(txn, this);
		for (size_t i = 0; i < indexList.size(); i++) {
			ValueMap valueMap(txn, this, indexList[i]);
			ColumnInfo& columnInfo = getColumnInfo(indexList[i].columnId_);

			util::XArray<std::pair<OId, OId> >::const_iterator itr;
			for (itr = moveList.begin(); itr != moveList.end(); itr++) {
				moveRowArray.load(txn, itr->second, this, OBJECT_READ_ONLY);
				RowArray::Row row(moveRowArray.getRow(), &moveRowArray);
				if (indexList[i].cursor_ < row.getRowId()) {
					continue;
				}
				bool isNullValue = row.isNullValue(columnInfo);
				BaseObject baseFieldObject(
					txn.getPartitionId(), *getObjectManager());
				const void *fieldValue = &NULL_VALUE;
				if (!isNullValue) {
					row.getField(txn, columnInfo, baseFieldObject);
					fieldValue = baseFieldObject.getCursor<void>();
				}
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
		if (definedRowKey()) {
			strstrm << "\"rowKeyAssigned\": true" << "," << std::endl;
		} else {
			strstrm << "\"rowKeyAssigned\": false" << "," << std::endl;
		}
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
		char affinityStr[AFFINITY_STRING_MAX_LENGTH + 1];
		memcpy(affinityStr, getAffinity(), AFFINITY_STRING_MAX_LENGTH);
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
	if (sc.nullCond_ != BaseIndex::SearchContext::NOT_IS_NULL && !isNullLast) {
		BtreeMap::SearchContext nullSc(sc.columnId_, NULL, 0, false, NULL, 
			0, false, 0, NULL, sc.limit_, COLUMN_TYPE_NULL);
		BtreeMap *nullMap = static_cast<BtreeMap *>(getValueMap(txn, true));
		if (nullMap != NULL) {
			nullMap->search(txn, nullSc, oIdList, outputOrder);
		}
		sc.limit_ -= oIdList.size();
	}

	if (sc.nullCond_ != BaseIndex::SearchContext::IS_NULL) {
		T *valueMap = static_cast<T *>(getValueMap(txn, false));
		if (valueMap != NULL) {
			valueMap->search(txn, sc, oIdList, outputOrder);
		}
		sc.limit_ -= oIdList.size();
	}

	if (sc.nullCond_ != BaseIndex::SearchContext::NOT_IS_NULL && isNullLast) {
		BtreeMap::SearchContext nullSc(sc.columnId_, NULL, 0, false, NULL, 
			0, false, 0, NULL, sc.limit_, COLUMN_TYPE_NULL);
		BtreeMap *nullMap = static_cast<BtreeMap *>(getValueMap(txn, true));
		if (nullMap != NULL) {
			nullMap->search(txn, nullSc, oIdList, outputOrder);
		}
		sc.limit_ -= oIdList.size();
	}

	return GS_SUCCESS;
}





