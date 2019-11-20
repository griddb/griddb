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
class RowArray;



/*!
	@brief Collection
*/
class Collection : public BaseContainer {
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
	Collection(TransactionContext &txn, DataStore *dataStore, OId oId)
		: BaseContainer(txn, dataStore, oId) {
		rowFixedDataSize_ = calcRowFixedDataSize();
		rowImageSize_ = calcRowImageSize(rowFixedDataSize_);
		setAllocateStrategy();
		rowArrayCache_ = ALLOC_NEW(txn.getDefaultAllocator())
			RowArray(txn, this);
		util::StackAllocator &alloc = txn.getDefaultAllocator();
		util::Vector<ColumnId> columnIds(alloc);
		columnIds.push_back(UNDEF_COLUMNID);
		rowIdFuncInfo_ = ALLOC_NEW(alloc) TreeFuncInfo(alloc);
		rowIdFuncInfo_->initialize(columnIds, NULL);
		mvccFuncInfo_ = rowIdFuncInfo_;
	}
	Collection(TransactionContext &txn, DataStore *dataStore)
		: BaseContainer(txn, dataStore) {
	}
	Collection(TransactionContext &txn, DataStore *dataStore, BaseContainerImage *containerImage, ShareValueList *commonContainerSchema)
		: BaseContainer(txn, dataStore, containerImage, commonContainerSchema) {
		rowFixedDataSize_ = calcRowFixedDataSize();
		rowImageSize_ = calcRowImageSize(rowFixedDataSize_);
		rowArrayCache_ = ALLOC_NEW(txn.getDefaultAllocator())
			RowArray(txn, this);
		util::StackAllocator &alloc = txn.getDefaultAllocator();
		util::Vector<ColumnId> columnIds(alloc);
		columnIds.push_back(UNDEF_COLUMNID);
		rowIdFuncInfo_ = ALLOC_NEW(alloc) TreeFuncInfo(alloc);
		rowIdFuncInfo_->initialize(columnIds, NULL);
		mvccFuncInfo_ = rowIdFuncInfo_;
	}

	~Collection() {}
	void initialize(TransactionContext &txn);
	bool finalize(TransactionContext &txn);
	void set(TransactionContext &txn, const FullContainerKey &containerKey,
		ContainerId containerId, OId columnSchemaOId,
		MessageSchema *containerSchema);

	void createIndex(TransactionContext &txn, const IndexInfo &indexInfo, 
		IndexCursor& indexCursor,
		bool isIndexNameCaseSensitive = false);
	void continueCreateIndex(TransactionContext& txn, 
		IndexCursor& indexCursor);

	void dropIndex(TransactionContext &txn, IndexInfo &indexInfo,
		bool isIndexNameCaseSensitive = false);


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
		const uint8_t *rowData, RowId rowId, DataStore::PutStatus &status) {
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

	void continueChangeSchema(TransactionContext &txn,
		ContainerCursor &containerCursor);

	bool hasUncommitedTransaction(TransactionContext &txn);

	void searchRowIdIndex(TransactionContext &txn, BtreeMap::SearchContext &sc,
		util::XArray<OId> &resultList, OutputOrder order);
	void searchRowIdIndex(TransactionContext &txn, uint64_t start,
		uint64_t limit, util::XArray<RowId> &rowIdList,
		util::XArray<OId> &resultList, uint64_t &skipped);
	RowId getMaxRowId(TransactionContext &txn);

	void lockRowList(TransactionContext &txn, util::XArray<RowId> &rowIdList);

	/*!
		@brief Calculate AllocateStrategy of Map Object
	*/
	AllocateStrategy calcMapAllocateStrategy() const {
		ChunkCategoryId chunkCategoryId;
		ExpireIntervalCategoryId expireCategoryId = DEFAULT_EXPIRE_CATEGORY_ID;
		ChunkKey chunkKey = UNDEF_CHUNK_KEY;
		int64_t duration = getContainerExpirationDutation();
		if (duration != INT64_MAX) {
			chunkCategoryId = ALLOCATE_EXPIRE_MAP;
			calcChunkKey(getContainerExpirationEndTime(), duration,
				expireCategoryId, chunkKey);
		}
		else {
			chunkCategoryId = ALLOCATE_NO_EXPIRE_MAP;
		}
		AffinityGroupId groupId = calcAffnityGroupId(getAffinity());
		return AllocateStrategy(chunkCategoryId, groupId, chunkKey, expireCategoryId);
	}
	/*!
		@brief Calculate AllocateStrategy of Row Object
	*/
	AllocateStrategy calcRowAllocateStrategy() const {
		ChunkCategoryId chunkCategoryId;
		ExpireIntervalCategoryId expireCategoryId = DEFAULT_EXPIRE_CATEGORY_ID;
		ChunkKey chunkKey = UNDEF_CHUNK_KEY;
		int64_t duration = getContainerExpirationDutation();
		if (duration != INT64_MAX) {
			chunkCategoryId = ALLOCATE_EXPIRE_ROW;
			calcChunkKey(getContainerExpirationEndTime(), duration,
				expireCategoryId, chunkKey);
		}
		else {
			chunkCategoryId = ALLOCATE_NO_EXPIRE_ROW;
		}
		AffinityGroupId groupId = calcAffnityGroupId(getAffinity());
		return AllocateStrategy(chunkCategoryId, groupId, chunkKey, expireCategoryId);
	}

	ColumnId getRowIdColumnId() {
		return UNDEF_COLUMNID;
	}
	ColumnType getRowIdColumnType() {
		return COLUMN_TYPE_LONG;
	}

	uint32_t getRealColumnNum(TransactionContext &txn) {
		return getColumnNum();
	}
	ColumnInfo* getRealColumnInfoList(TransactionContext &txn) {
		return getColumnInfoList();
	}
	uint32_t getRealRowSize(TransactionContext &txn) {
		return getRowSize();
	}
	uint32_t getRealRowFixedDataSize(TransactionContext &txn) {
		return getRowFixedDataSize();
	}

	util::String getBibInfo(TransactionContext &txn, const char* dbName);
	void getActiveTxnList(
		TransactionContext &txn, util::Set<TransactionId> &txnList);
	void getErasableList(TransactionContext &txn, Timestamp erasableTimeLimit, util::XArray<ArchiveInfo> &list);
	ExpireType getExpireType() const;
	bool validate(TransactionContext &txn, std::string &errorMessage);
	std::string dump(TransactionContext &txn);

protected:  
protected:  
	void putRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowData, RowId &rowId, bool rowIdSpecified,
		DataStore::PutStatus &status, PutRowOption putRowOption);
	void putRow(TransactionContext &txn,
		InputMessageRowStore *inputMessageRowStore, RowId &rowId,
		bool rowIdSpecified,
		DataStore::PutStatus &status, PutRowOption putRowOption);

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
		const uint8_t *rowData, RowId rowId, DataStore::PutStatus &status,
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

	void insertRowIdMap(TransactionContext &txn, BtreeMap *map,
		const void *constKey, OId oId);
	void insertMvccMap(TransactionContext &txn, BtreeMap *map,
		TransactionId tId, MvccRowImage &mvccImage);
	void insertValueMap(TransactionContext &txn, ValueMap &valueMap,
		const void *constKey, OId oId, bool isNull);
	void updateRowIdMap(TransactionContext &txn, BtreeMap *map,
		const void *constKey, OId oldOId, OId newOId);
	void updateMvccMap(TransactionContext &txn, BtreeMap *map,
		TransactionId tId, MvccRowImage &oldMvccImage,
		MvccRowImage &newMvccImage);
	void updateValueMap(TransactionContext &txn, ValueMap &valueMap,
		const void *constKey, OId oldOId, OId newOId, bool isNull);
	void removeRowIdMap(TransactionContext &txn, BtreeMap *map,
		const void *constKey, OId oId);
	void removeMvccMap(TransactionContext &txn, BtreeMap *map,
		TransactionId tId, MvccRowImage &mvccImage);
	void removeValueMap(TransactionContext &txn, ValueMap &valueMap,
		const void *constKey, OId oId, bool isNull);
	void updateIndexData(
		TransactionContext &txn, const IndexData &indexData);


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

	void setAllocateStrategy() {
		metaAllocateStrategy_ = calcMetaAllocateStrategy();
		rowAllocateStrategy_ = calcRowAllocateStrategy();
		mapAllocateStrategy_ = calcMapAllocateStrategy();
	}
	void checkExclusive(TransactionContext &txn);

	bool getIndexData(TransactionContext &txn, const util::Vector<ColumnId> &columnIds,
		MapType mapType, bool withUncommitted, IndexData &indexData, 
		bool withPartialMatch = false) const;
	bool getIndexData(TransactionContext &txn, IndexCursor &indexCursor,
		IndexData &indexData) const {
		return indexSchema_->getIndexData( 
			txn, indexCursor, UNDEF_CONTAINER_POS, indexData);
	}
	void createNullIndexData(TransactionContext &txn, IndexData &indexData) {
		indexSchema_->createNullIndexData(txn, UNDEF_CONTAINER_POS, indexData, 
			this);
	}
	void getIndexList(TransactionContext &txn,  
		bool withUncommitted, util::XArray<IndexData> &list) const;



	void finalizeIndex(TransactionContext &txn) {
		if (!isExpired(txn)) {
			indexSchema_->dropAll(txn, this, UNDEF_CONTAINER_POS, true);
		}
		indexSchema_->finalize(txn);
	}

	void incrementRowNum() {
		baseContainerImage_->rowNum_++;
	}
	void decrementRowNum() {
		baseContainerImage_->rowNum_--;
	}

};

#endif
