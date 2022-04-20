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
	@brief Definition of Collection
*/
#ifndef COLLECTION_H_
#define COLLECTION_H_

#include "base_container.h"

UTIL_TRACER_DECLARE(COLLECTION);



/*!
	@brief Collection
*/
class Collection : public BaseContainer {
	friend class StoreV5Impl;
protected:  
	/*!
		@brief Collection format
	*/
	struct CollectionImage : BaseContainerImage {
		RowId maxRowId_;	 
		uint64_t padding1_;  
		uint64_t padding2_;  
	};

public:  
	static const bool indexMapTable[][MAP_TYPE_NUM];

public:  
public:  
	Collection(TransactionContext &txn, DataStoreV4 *dataStore, OId oId)
		: BaseContainer(txn, dataStore, oId) {
		rowFixedDataSize_ = calcRowFixedDataSize();
		rowImageSize_ = calcRowImageSize(rowFixedDataSize_);
		util::StackAllocator &alloc = txn.getDefaultAllocator();
		util::Vector<ColumnId> columnIds(alloc);
		columnIds.push_back(getRowIdColumnId());
		rowIdFuncInfo_ = ALLOC_NEW(alloc) TreeFuncInfo(alloc);
		rowIdFuncInfo_->initialize(columnIds, NULL);

		columnIds[0] = UNDEF_COLUMNID;
		mvccFuncInfo_ = ALLOC_NEW(alloc) TreeFuncInfo(alloc);
		mvccFuncInfo_->initialize(columnIds, NULL);
	}
	Collection(TransactionContext &txn, DataStoreV4 *dataStore)
		: BaseContainer(txn, dataStore) {}
	~Collection() {}
	void initialize(TransactionContext &txn);
	bool finalize(TransactionContext &txn, bool isRemoveGroup);
	void set(TransactionContext &txn, const FullContainerKey &containerKey,
		ContainerId containerId, OId columnSchemaOId,
		MessageSchema *containerSchema, DSGroupId groupId);

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
	void scanRowIdIndex(
			TransactionContext& txn, BtreeMap::SearchContext &sc,
			ContainerRowScanner &scanner);
	void scanRowArray(
			TransactionContext& txn, const OId *begin, const OId *end,
			bool onMvcc, ContainerRowScanner &scanner);
	void scanRow(
			TransactionContext& txn, const OId *begin, const OId *end,
			bool onMvcc, ContainerRowScanner &scanner);
	RowId getMaxRowId(TransactionContext &txn);

	void lockRowList(TransactionContext &txn, util::XArray<RowId> &rowIdList);

	ColumnId getRowIdColumnId() {
		return UNDEF_COLUMNID;
	}
	ColumnType getRowIdColumnType() {
		return COLUMN_TYPE_LONG;
	}

protected:  
protected:  
	void putRowInternal(TransactionContext &txn,
		InputMessageRowStore *inputMessageRowStore, RowId &rowId,
		bool rowIdSpecified,
		PutStatus &status, PutRowOption putRowOption);

private:  
	/*!
		@brief Status of rowArray, when the transaction is aborted
	*/
	enum AbortStatus {
		ABORT_CREATE_FIRST_ROW_ARRAY,
		ABORT_UPDATE_ROW_ARRAY,
		ABORT_INSERT_ROW_ARRAY,
		ABORT_APPEND_ROW_ARRAY,
		ABORT_SPLIT_ROW_ARRAY,
		ABORT_UNDEF_STATUS
	};

private:  
private:  
	bool isUnique(TransactionContext &txn, 
		util::Vector<ColumnId> &keyColumnIdList, 
		util::XArray<KeyData> &keyFieldList, OId &oId);
	bool searchRowKeyWithRowIdMap(TransactionContext &txn, 
		util::XArray<KeyData> &keyFieldList, util::Vector<ColumnId> &keyColumnIdList,
		OId &oId);
	bool searchRowKeyWithMvccMap(TransactionContext &txn,
		BtreeMap::SearchContext &sc, OId &oId);
	bool searchRowKeyWithMvccMap(TransactionContext &txn,
		util::XArray<KeyData> &keyFieldList, util::Vector<ColumnId> &keyColumnIdList,
		OId &oId);

	void deleteRow(
		TransactionContext &txn, RowId rowId, bool &existing, bool isForceLock);
	void updateRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowData, RowId rowId, PutStatus &status,
		bool isForceLock);
	void appendRowInternal(TransactionContext &txn,
		MessageRowStore *messageRowStore, RowArray &rowArray, RowId &rowId,
		bool rowIdSpecified);
	void deleteRowInternal(
		TransactionContext &txn, RowArray &rowArray, RowId &rowId);
	void updateRowInternal(TransactionContext &txn,
		MessageRowStore *messageRowStore, RowArray &rowArray, RowId &rowId);
	void insertRowInternal(
		TransactionContext &txn, RowArray &srcRowArray, RowArray &destRowArray);
	void shift(TransactionContext &txn, RowArray &rowArray, bool isForce);
	void split(TransactionContext &txn, RowArray &rowArray, RowId insertRowId,
		RowArray &splitRowArray, RowId &splitRowId);
	void merge(
		TransactionContext &txn, RowArray &rowArray, RowArray &nextRowArray);

	void abortInternal(TransactionContext &txn, TransactionId tId);
	void undoCreateRow(TransactionContext &txn, RowArray &rowArray);
	void undoUpdateRow(TransactionContext &txn, RowArray &beforeRowArray);

	RowId allocateRowId() {
		return ++reinterpret_cast<CollectionImage *>(baseContainerImage_)
					 ->maxRowId_;
	}

	void setMaxRowId(RowId newRowId) {
		RowId &maxRowId = reinterpret_cast<CollectionImage *>
			(baseContainerImage_)->maxRowId_;
		if (maxRowId < newRowId) {
			maxRowId = newRowId;
		} else {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_COL_GET_LOCK_ID_INVALID,
				"specified rowId is not acceptable, "
				<< "current maxowId = " << maxRowId
				<< ", specified rowId = " << newRowId);
		}
	}

	template<bool Mvcc> static void scanRowArrayCheckedDirect(
			TransactionContext& txn, RowArray &rowArray,
			RowId startRowId, RowId endRowId, util::XArray<OId> &oIdList);
	template<bool Mvcc> static void scanRowCheckedDirect(
			TransactionContext& txn, RowArray &rowArray,
			util::XArray<OId> &oIdList);
	template<bool Mvcc> static bool filterActiveTransaction(
			TransactionContext& txn, RowArray::Row &row);

	void scanRowIdIndexPrepare(
			TransactionContext& txn, BtreeMap::SearchContext &sc,
			util::XArray<OId> *oIdList);
	static void searchMvccMapPrepare(
			TransactionContext& txn, const BtreeMap::SearchContext &sc,
			BtreeMap::SearchContext &mvccSC,
			RowId startRowId, RowId lastCheckRowId,
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
		uint32_t rowImageSize_ =
			sizeof(TransactionId) + sizeof(RowId) + rowFixedSize;
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

};

#endif
