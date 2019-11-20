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
	@brief Definition of TimeSeries and SubTimeSeries
*/
#ifndef TIME_SERIES_H_
#define TIME_SERIES_H_

#include "base_container.h"

class ColumnInfo;
class MessageTimeSeriesSchema;

UTIL_TRACER_DECLARE(TIME_SERIES);

static const OId RUNTIME_RESET_FILTER =
	0x0ULL;  
static const OId RUNTIME_SET_FILTER =
	0x1ULL;  



class SubTimeSeries;
/*!
	@brief TimeSeries
*/
class TimeSeries : public BaseContainer {
	friend class DataStore;
	friend class SubTimeSeries;

public:  
	static const bool indexMapTable[][MAP_TYPE_NUM];

	/*!
		@brief TimeSeries format
	*/
	struct TimeSeriesImage : BaseContainerImage {
		union {
			OId hiCompressionStatus_;	  
			int64_t ssCompressionStatus_;  
		};
		uint64_t padding1_;  
		uint64_t padding2_;  
	};
	/*!
		@brief SubTimeSeries format
	*/
	struct SubTimeSeriesImage {
		OId rowIdMapOId_;
		OId mvccMapOId_;
		Timestamp startTime_;  
		SubTimeSeriesImage() {
			rowIdMapOId_ = UNDEF_OID;
			mvccMapOId_ = UNDEF_OID;
			startTime_ = 0;  
		}
		bool isRuntime() const {
			return ObjectManager::getUserArea(mvccMapOId_) != 0;
		}
		bool operator!=(const SubTimeSeriesImage &target) const {
			if (rowIdMapOId_ != target.rowIdMapOId_ ||
				mvccMapOId_ != target.mvccMapOId_ ||
				startTime_ != target.startTime_) {
				return true;
			}
			else {
				return false;
			}
		}
		friend std::ostream &operator<<(
			std::ostream &output, const SubTimeSeriesImage &val) {
			output << "[" << val.startTime_ << "," << val.rowIdMapOId_ << ","
				   << val.mvccMapOId_;
			output << "]";
			return output;
		}
	};

protected:  
	struct SubTimeSeriesInfo {
		SubTimeSeriesImage image_;
		uint64_t pos_;  
		SubTimeSeriesInfo(const SubTimeSeriesImage &image, uint64_t pos)
			: image_(image), pos_(pos) {}
	};
	struct BaseSubTimeSeriesData {
		Timestamp baseTimestamp_;  
		OId tailNodeOId_;		   
	};
	typedef LinkArray<BaseSubTimeSeriesData, SubTimeSeriesImage>
		SubTimeSeriesList;

public:  

public:  
public:  
	TimeSeries(TransactionContext &txn, DataStore *dataStore, OId oId)
		: BaseContainer(txn, dataStore, oId), subTimeSeriesListHead_(NULL) {
		if (baseContainerImage_->subContainerListOId_ != UNDEF_OID) {
			subTimeSeriesListHead_ = ALLOC_NEW(txn.getDefaultAllocator())
				SubTimeSeriesList(txn, *getObjectManager(),
					baseContainerImage_->subContainerListOId_);
		}
		rowFixedDataSize_ = calcRowFixedDataSize();
		rowImageSize_ = calcRowImageSize(rowFixedDataSize_);
		setAllocateStrategy();
		rowArrayCache_ = ALLOC_NEW(txn.getDefaultAllocator())
			RowArray(txn, this);
		util::StackAllocator &alloc = txn.getDefaultAllocator();
		util::Vector<ColumnId> columnIds(alloc);
		columnIds.push_back(ColumnInfo::ROW_KEY_COLUMN_ID);
		rowIdFuncInfo_ = ALLOC_NEW(alloc) TreeFuncInfo(alloc);
		rowIdFuncInfo_->initialize(columnIds, NULL);

		columnIds[0] = UNDEF_COLUMNID;
		mvccFuncInfo_ = ALLOC_NEW(alloc) TreeFuncInfo(alloc);
		mvccFuncInfo_->initialize(columnIds, NULL);
	}
	TimeSeries(TransactionContext &txn, DataStore *dataStore)
		: BaseContainer(txn, dataStore), subTimeSeriesListHead_(NULL) {}
	TimeSeries(TransactionContext &txn, DataStore *dataStore, BaseContainerImage *containerImage, ShareValueList *commonContainerSchema)
		: BaseContainer(txn, dataStore, containerImage, commonContainerSchema), subTimeSeriesListHead_(NULL) {

		subTimeSeriesListHead_ = ALLOC_NEW(txn.getDefaultAllocator())
			SubTimeSeriesList(txn, *getObjectManager());

		if (containerImage->rowIdMapOId_ != UNDEF_OID) {
			SubTimeSeriesImage image;
			image.rowIdMapOId_ = containerImage->rowIdMapOId_;
			image.mvccMapOId_ = containerImage->mvccMapOId_;
			image.startTime_ = 0;
			subTimeSeriesListHead_->initializeOnMemory(txn, &image);
		} else {
			subTimeSeriesListHead_->initializeOnMemory(txn, NULL);
		}

		containerImage->rowIdMapOId_ = UNDEF_OID;
		containerImage->mvccMapOId_ = UNDEF_OID;

		rowFixedDataSize_ = calcRowFixedDataSize();
		rowImageSize_ = calcRowImageSize(rowFixedDataSize_);
		rowArrayCache_ = ALLOC_NEW(txn.getDefaultAllocator())
			RowArray(txn, this);
		util::StackAllocator &alloc = txn.getDefaultAllocator();
		util::Vector<ColumnId> columnIds(alloc);
		columnIds.push_back(ColumnInfo::ROW_KEY_COLUMN_ID);
		rowIdFuncInfo_ = ALLOC_NEW(alloc) TreeFuncInfo(alloc);
		rowIdFuncInfo_->initialize(columnIds, NULL);

		columnIds[0] = UNDEF_COLUMNID;
		mvccFuncInfo_ = ALLOC_NEW(alloc) TreeFuncInfo(alloc);
		mvccFuncInfo_->initialize(columnIds, NULL);
	}

	~TimeSeries() {
		ALLOC_DELETE((alloc_), subTimeSeriesListHead_);
	}
	void initialize(TransactionContext &txn);
	bool finalize(TransactionContext &txn);
	void set(TransactionContext &txn, const FullContainerKey &containerKey,
		ContainerId containerId, OId columnSchemaOId,
		MessageSchema *orgContainerSchema);

	void createIndex(TransactionContext &txn, const IndexInfo& indexInfo, 
		IndexCursor& indexCursor,
		bool isIndexNameCaseSensitive = false);
	void continueCreateIndex(TransactionContext& txn, 
		IndexCursor& indexCursor);

	void dropIndex(TransactionContext &txn, IndexInfo &indexInfo,
		bool isIndexNameCaseSensitive = false);

	void putRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowData, RowId &rowId, bool rowIdSpecified,
		DataStore::PutStatus &status, PutRowOption putRowOption);

	void appendRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowDataWOTimestamp, Timestamp &rowKey,
		DataStore::PutStatus &status);
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
	void searchColumnIdIndex(TransactionContext &txn,
		BtreeMap::SearchContext &sc, util::XArray<OId> &resultList,
		OutputOrder order, bool neverOrdering = false);
	void searchColumnIdIndex(TransactionContext &txn,
		BtreeMap::SearchContext &sc, BtreeMap::SearchContext &orgSc,
		util::XArray<OId> &normalRowList, util::XArray<OId> &mvccRowList);
	void searchColumnIdIndex(TransactionContext &txn,
		BtreeMap::SearchContext &sc, util::XArray<OId> &normalRowList,
		util::XArray<OId> &mvccRowList) {
		searchColumnIdIndex(txn, sc, sc, normalRowList, mvccRowList);
	}
	void aggregate(TransactionContext &txn, BtreeMap::SearchContext &sc,
		uint32_t columnId, AggregationType type, ResultSize &resultNum,
		Value &value);  

	void aggregateByTimeWindow(TransactionContext &txn,
		uint32_t columnId, AggregationType type, 
		Timestamp startTime, Timestamp endTime, const Sampling &sampling, 
		util::XArray<OId> &oIdList, ResultSize &resultNum,
		MessageRowStore *messageRowStore);

	void sample(TransactionContext &txn, BtreeMap::SearchContext &sc,
		const Sampling &sampling, ResultSize &resultNum,
		MessageRowStore *messageRowStore);  
	void sampleWithoutInterp(TransactionContext &txn,
		BtreeMap::SearchContext &sc, const Sampling &sampling,
		ResultSize &resultNum,
		MessageRowStore *messageRowStore);  
	void searchTimeOperator(
		TransactionContext &txn, Timestamp ts, TimeOperator timeOp, OId &oId);
	void searchTimeOperator(
		TransactionContext &txn, BtreeMap::SearchContext &sc, 
		Timestamp ts, TimeOperator timeOp, OId &oId);

	void lockRowList(TransactionContext &txn, util::XArray<RowId> &rowIdList);
	void setDummyMvccImage(TransactionContext &txn);

	Timestamp getCurrentExpiredTime(TransactionContext &txn);

	/*!
		@brief Calculate AllocateStrategy of Map Object
	*/
	AllocateStrategy calcMapAllocateStrategy() const {
		AffinityGroupId groupId = calcAffnityGroupId(getAffinity());
		return AllocateStrategy(ALLOCATE_NO_EXPIRE_MAP, groupId);
	}
	/*!
		@brief Calculate AllocateStrategy of Row Object
	*/
	AllocateStrategy calcRowAllocateStrategy() const {
		AffinityGroupId groupId = calcAffnityGroupId(getAffinity());
		return AllocateStrategy(ALLOCATE_NO_EXPIRE_ROW, groupId);
	}

	/*!
		@brief Calculate AllocateStrategy of Map Object
	*/
	AllocateStrategy calcSubMapAllocateStrategy() const {
		ChunkCategoryId chunkCategoryId;
		ExpireIntervalCategoryId expireCategoryId = DEFAULT_EXPIRE_CATEGORY_ID;
		ChunkKey chunkKey = UNDEF_CHUNK_KEY;
		if (getExpireType() != NO_EXPIRE) {
			chunkCategoryId = ALLOCATE_EXPIRE_MAP;
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
	AllocateStrategy calcSubRowAllocateStrategy() const {
		ChunkCategoryId chunkCategoryId;
		ExpireIntervalCategoryId expireCategoryId = DEFAULT_EXPIRE_CATEGORY_ID;
		ChunkKey chunkKey = UNDEF_CHUNK_KEY;

		if (getExpireType() != NO_EXPIRE) {
			chunkCategoryId = ALLOCATE_EXPIRE_ROW;
		}
		else {
			chunkCategoryId = ALLOCATE_NO_EXPIRE_ROW;
		}
		AffinityGroupId groupId = calcAffnityGroupId(getAffinity());
		return AllocateStrategy(chunkCategoryId, groupId, chunkKey, expireCategoryId);
	}
	/*!
		@brief Check if expired option is defined
	*/
	bool isExpireContainer() {
		return getExpirationInfo().duration_ != INT64_MAX;
	}

	ColumnId getRowIdColumnId() {
		return ColumnInfo::ROW_KEY_COLUMN_ID;
	}
	ColumnType getRowIdColumnType() {
		return COLUMN_TYPE_TIMESTAMP;
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

	const ExpirationInfo& getExpirationInfo() const {
		ExpirationInfo *expirationInfo =
			commonContainerSchema_->get<ExpirationInfo>(META_TYPE_DURATION);
		return *expirationInfo;
	}

	const CompressionSchema &getCompressionSchema() const {
		CompressionSchema *compressionSchema =
			commonContainerSchema_->get<CompressionSchema>(
				META_TYPE_COMPRESSION_SCHEMA);
		return *compressionSchema;
	}

	util::String getBibInfo(TransactionContext &txn, const char* dbName);
	void getActiveTxnList(
		TransactionContext &txn, util::Set<TransactionId> &txnList);
	void getErasableList(TransactionContext &txn, Timestamp erasableTimeLimit, util::XArray<ArchiveInfo> &list);
	ExpireType getExpireType() const;
	bool validate(TransactionContext &txn, std::string &errorMessage);
	std::string dump(TransactionContext &txn);

private:  
	struct OpForSample {
		bool isInterpolated_;
		Calculator2 add_;
		Calculator2 sub_;
		Calculator2 mul_;
	};

private:										
	AllocateStrategy subRowAllocateStrategy_;
	AllocateStrategy subMapAllocateStrategy_;
	SubTimeSeriesList *subTimeSeriesListHead_;  
private:										
	SubTimeSeries *putSubContainer(TransactionContext &txn, Timestamp rowKey);
	SubTimeSeries *getSubContainer(
		TransactionContext &txn, Timestamp rowKey, bool forUpdate);
	void putRow(TransactionContext &txn,
		InputMessageRowStore *inputMessageRowStore, RowId &rowId,
		bool rowIdSpecified,
		DataStore::PutStatus &status, PutRowOption putRowOption);
	void deleteRow(
		TransactionContext &txn, RowId rowId, bool &existing, bool isForceLock);
	void updateRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowData, RowId rowId, DataStore::PutStatus &status,
		bool isForceLock);

	void getIdList(TransactionContext &txn,
		util::XArray<uint8_t> &serializedRowList, util::XArray<RowId> &idList);
	void lockIdList(TransactionContext &txn, util::XArray<OId> &oIdList,
		util::XArray<RowId> &idList);
	void getContainerOptionInfo(
		TransactionContext &txn, util::XArray<uint8_t> &containerSchema);
	util::String getBibContainerOptionInfo(TransactionContext &txn);
	void checkContainerOption(MessageSchema *orgMessageSchema,
		util::XArray<uint32_t> &copyColumnMap, bool &isCompletelySameSchema);

	uint32_t calcRowImageSize(uint32_t rowFixedSize) {
		uint32_t rowImageSize_ = sizeof(RowHeader) + rowFixedSize;
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
		subRowAllocateStrategy_ = calcSubRowAllocateStrategy();
		mapAllocateStrategy_ = calcMapAllocateStrategy();
		subMapAllocateStrategy_ = calcSubMapAllocateStrategy();
	}

	void checkExclusive(TransactionContext &) {}

	void getSubTimeSeriesList(TransactionContext &txn,
		util::XArray<SubTimeSeriesInfo> &subTimeSeriesList, bool forUpdate, bool indexUpdate = false);
	void getSubTimeSeriesList(TransactionContext &txn,
		BtreeMap::SearchContext &sc,
		util::XArray<SubTimeSeriesInfo> &subTimeSeriesList, bool forUpdate, bool indexUpdate = false);
	void getRuntimeSubTimeSeriesList(TransactionContext &txn,
		util::XArray<SubTimeSeriesInfo> &subTimeSeriesList, bool forUpdate);
	void getExpirableSubTimeSeriesList(TransactionContext &txn,
		Timestamp expirableTime, util::XArray<SubTimeSeriesInfo> &subTimeSeriesList);
	bool findStartSubTimeSeriesPos(TransactionContext &txn,
		Timestamp targetBeginTime, int64_t lowPos, int64_t highPos,
		int64_t &midPos);  
	void searchRowArrayList(TransactionContext &txn,
		BtreeMap::SearchContext &sc, util::XArray<OId> &normalOIdList,
		util::XArray<OId> &mvccOIdList);

	bool isSsCompressionReady() const {
		return (reinterpret_cast<TimeSeriesImage *>(baseContainerImage_)
					->ssCompressionStatus_ == SS_IS_READRY);
	}
	void setSsCompressionReady(SSCompressionStatus status) {
		reinterpret_cast<TimeSeriesImage *>(baseContainerImage_)
			->ssCompressionStatus_ = status;
	}

	void setBaseTime(Timestamp time) {
		BaseSubTimeSeriesData header = *subTimeSeriesListHead_->getHeader();
		header.baseTimestamp_ = time;
		subTimeSeriesListHead_->setHeader(&header);
	}

	void setTailNodeOId(OId oId) {
		BaseSubTimeSeriesData header = *subTimeSeriesListHead_->getHeader();
		header.tailNodeOId_ = oId;
		subTimeSeriesListHead_->setHeader(&header);
	}

	uint16_t getDivideNum() {
		return getExpirationInfo().dividedNum_;
	}

	Timestamp getBaseTime() {
		return subTimeSeriesListHead_->getHeader()->baseTimestamp_;
	}

	OId getTailNodeOId() {
		return subTimeSeriesListHead_->getHeader()->tailNodeOId_;
	}

	uint64_t getBasePos() {
		return subTimeSeriesListHead_->getNum() - 1;
	}

	void replaceSubTimeSeriesList(TransactionContext &txn) {
		setDirty();
		if (baseContainerImage_->subContainerListOId_ !=
			subTimeSeriesListHead_->getBaseOId()) {
			baseContainerImage_->subContainerListOId_ =
				subTimeSeriesListHead_->getBaseOId();
		}
	}
	int32_t expireSubTimeSeries(TransactionContext &txn, Timestamp expiredTime, bool indexUpdate);

	int64_t getDivideDuration() {
		const ExpirationInfo &expirationInfo = getExpirationInfo();
		return expirationInfo.duration_ / expirationInfo.dividedNum_;
	}

	void updateSubTimeSeriesImage(
		TransactionContext &txn, const SubTimeSeriesInfo &subTimeSeriesInfo);

	bool getIndexData(TransactionContext &txn, const util::Vector<ColumnId> &columnIds,
		MapType mapType, bool withUncommitted, IndexData &indexData,
		bool withPartialMatch = false) const {
		return indexSchema_->getIndexData(
			txn, columnIds, mapType, UNDEF_CONTAINER_POS, withUncommitted, 
			withPartialMatch, indexData);
	}
	bool getIndexData(TransactionContext &txn, IndexCursor &indexCursor,
		IndexData &indexData) const {
		return indexSchema_->getIndexData(
			txn, indexCursor, UNDEF_CONTAINER_POS, indexData);
	}
	void createNullIndexData(TransactionContext &txn, IndexData &indexData) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
	}
	void getIndexList(TransactionContext &txn,  
		bool withUncommitted, util::XArray<IndexData> &list) const {
		indexSchema_->getIndexList(txn, UNDEF_CONTAINER_POS, withUncommitted, list);
	}


	void finalizeIndex(TransactionContext &txn) {
		indexSchema_->finalize(txn);
	}
	void finalizeExpireIndex(TransactionContext &txn, uint64_t pos) {
		indexSchema_->dropAll(txn, this, pos, false);
	}

	void incrementRowNum() {
		baseContainerImage_->rowNum_++;
	}
	void decrementRowNum() {
		baseContainerImage_->rowNum_--;
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


};

/*!
	@brief SubTimeSeries
	@note TimeSeries is consist of SubTimeSeries. If Expiration is deined,
		  there are SubTimeSeries more than one.
*/
class SubTimeSeries : public TimeSeries {
public:  
public:  
public:  
	SubTimeSeries(TransactionContext &txn, const SubTimeSeriesInfo &info,
		TimeSeries *parent)
		: TimeSeries(txn, parent->getDataStore()),
		  parent_(parent),
		  position_(info.pos_),
		  startTime_(info.image_.startTime_) {
		baseContainerImage_ = reinterpret_cast<BaseContainerImage *>(
			ALLOC_NEW(txn.getDefaultAllocator()) TimeSeriesImage);
		memcpy(baseContainerImage_, parent->baseContainerImage_,
			sizeof(TimeSeriesImage));
		baseContainerImage_->rowIdMapOId_ = info.image_.rowIdMapOId_;
		baseContainerImage_->mvccMapOId_ = info.image_.mvccMapOId_;
		rowImageSize_ = parent->rowImageSize_;
		rowFixedDataSize_ = parent->rowFixedDataSize_;

		commonContainerSchema_ = parent->commonContainerSchema_;
		columnSchema_ = parent->columnSchema_;
		indexSchema_ = parent_->indexSchema_;
		rowArrayCache_ = parent_->rowArrayCache_;

		rowIdFuncInfo_ = parent_->rowIdFuncInfo_;
		mvccFuncInfo_ = parent_->mvccFuncInfo_;

		if (getExpirationInfo().duration_ == INT64_MAX) {
			endTime_ = MAX_TIMESTAMP;
		}
		else {
			endTime_ = startTime_ + parent->getDivideDuration() - 1;
		}
		setAllocateStrategy(parent);
	}
	SubTimeSeries(TransactionContext &txn, TimeSeries *parent)
		: TimeSeries(txn, parent->getDataStore()),
		  parent_(parent),
		  position_(0),
		  startTime_(0) {
		rowIdFuncInfo_ = parent_->rowIdFuncInfo_;
		mvccFuncInfo_ = parent_->mvccFuncInfo_;
	}
	~SubTimeSeries() {
		commonContainerSchema_ = NULL;
		columnSchema_ = NULL;
		indexSchema_ = NULL;
		rowArrayCache_ = NULL;
	}
	void initialize(uint64_t position, Timestamp startTime);
	void initialize(TransactionContext &txn);
	bool finalize(TransactionContext &txn);
	void set(TransactionContext &txn, const FullContainerKey &containerKey,
		ContainerId containerId, OId columnSchemaOId,
		MessageSchema *orgContainerSchema);
	void set(TransactionContext &txn, TimeSeries *parent);

	void createIndex(TransactionContext &txn, const IndexInfo& indexInfo, 
		IndexCursor& indexCursor,
		bool isIndexNameCaseSensitive = false);
	void continueCreateIndex(TransactionContext& txn, 
		IndexCursor& indexCursor);
	void indexInsert(TransactionContext& txn, IndexCursor& indexCursor);

	void dropIndex(TransactionContext &txn, IndexInfo &indexInfo,
		bool isIndexNameCaseSensitive = false);


	void putRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowData, RowId &rowId, bool rowIdSpecified, 
		DataStore::PutStatus &status, PutRowOption putRowOption) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "unsupport operation");
	}

	void putRow(TransactionContext &txn,
		InputMessageRowStore *inputMessageRowStore, RowId &rowId,
		bool rowIdSpecified,
		DataStore::PutStatus &status, PutRowOption putRowOption);

	void deleteRow(TransactionContext &txn, uint32_t rowKeySize,
		const uint8_t *rowKey, RowId &rowId, bool &existing);
	/*!
		@brief Deletes a Row corresponding to the specified RowId
	*/
	void deleteRow(TransactionContext &, RowId, bool &) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "unsupport operation");
	}
	/*!
		@brief Updates a Row corresponding to the specified RowId
	*/
	void updateRow(TransactionContext &, uint32_t, const uint8_t *, RowId,
		DataStore::PutStatus &) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "unsupport operation");
	}
	/*!
		@brief Deletes a Row corresponding to the specified RowId at recovery
	   phase
	*/
	void redoDeleteRow(TransactionContext &, RowId, bool &) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "unsupport operation");
	}
	void deleteRow(
		TransactionContext &txn, RowId rowId, bool &existing, bool isForceLock);
	void updateRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowData, RowId rowId, DataStore::PutStatus &status,
		bool isForceLock);
	void abort(TransactionContext &txn);
	void commit(TransactionContext &txn);

	void searchRowIdIndex(TransactionContext &txn, BtreeMap::SearchContext &sc,
		util::XArray<OId> &resultList, OutputOrder order);
	void searchRowIdIndex(TransactionContext &txn, uint64_t start,
		uint64_t limit, util::XArray<RowId> &rowIdList,
		util::XArray<OId> &resultList, uint64_t &skipped);





	void setDummyMvccImage(TransactionContext &txn);

	void getIndexList(
		TransactionContext &txn, bool withUncommitted, util::XArray<IndexData> &list) const;

	util::String getBibInfo(TransactionContext &txn, const char* dbName);
	void getActiveTxnList(
		TransactionContext &txn, util::Set<TransactionId> &txnList);
	void getErasableList(TransactionContext &txn, Timestamp erasableTimeLimit, util::XArray<ArchiveInfo> &list);
	ExpireType getExpireType() const;
	Timestamp getStartTime() const {
		return startTime_;
	}
	Timestamp getEndTime() const {
		return endTime_;
	}

	bool validate(TransactionContext &txn, std::string &errorMessage);
	std::string dump(TransactionContext &txn);

private:  
	/*!
		@brief Status of RowArray, when row is deleted
	*/
	enum DeleteStatus {
		DELETE_SIMPLE,	 
		DELETE_MERGE,	  
		DELETE_ROW_ARRAY,  
	};

private:  
	TimeSeries *parent_;
	uint64_t position_;
	Timestamp startTime_;
	Timestamp endTime_;  
private:				 
	void appendRowInternal(TransactionContext &txn,
		MessageRowStore *messageRowStore, RowArray &rowArray, RowId &rowId);
	void deleteRowInternal(TransactionContext &txn, Timestamp rowKey,
		bool &existing, bool isForceLock);
	void updateRowInternal(TransactionContext &txn,
		MessageRowStore *messageRowStore, RowArray &rowArray, RowId &rowId);
	void insertRowInternal(TransactionContext &txn,
		MessageRowStore *messageRowStore, RowArray &destRowArray, RowId &rowId);
	void shift(TransactionContext &txn, RowArray &rowArray, bool isForce);
	void split(TransactionContext &txn, RowArray &rowArray, RowId insertRowId,
		RowArray &splitRowArray, RowId &splitRowId);
	void merge(
		TransactionContext &txn, RowArray &rowArray, RowArray &nextRowArray);
	void createMinimumRowArray(
		TransactionContext &txn, RowArray &rowArray, bool isExistEmptyRowArray);
	void abortInternal(TransactionContext &txn, TransactionId tId);
	void undoCreateRow(TransactionContext &txn, RowArray &rowArray);
	void undoUpdateRow(TransactionContext &txn, RowArray &beforeRowArray);

	void setRuntime() {
		baseContainerImage_->mvccMapOId_ =
			ObjectManager::setUserArea(baseContainerImage_->mvccMapOId_, RUNTIME_SET_FILTER);
	}
	void resetRuntime() {
		baseContainerImage_->mvccMapOId_ =
			ObjectManager::setUserArea(baseContainerImage_->mvccMapOId_, RUNTIME_RESET_FILTER);
	}
	bool isRuntime() {
		return ObjectManager::getUserArea(baseContainerImage_->mvccMapOId_) != 0;
	}

	bool isRowOmittableBySsCompression(TransactionContext &txn,
		const CompressionSchema &compressionSchema,
		MessageRowStore *messageRowStore, RowArray &rowArray, Timestamp rowKey);
	bool isRowOmittableByHiCompression(TransactionContext &txn,
		const CompressionSchema &compressionSchema,
		MessageRowStore *messageRowStore, RowArray &rowArray, Timestamp rowKey);

	bool isSsCompressionReady() const {
		return parent_->isSsCompressionReady();
	}
	void setSsCompressionReady(SSCompressionStatus status) {
		return parent_->setSsCompressionReady(status);
	}
	bool isOmittableByDSDCCheck(double threshold, Timestamp startTime,
		double startVal, Timestamp nowTime, double nowVal, DSDCVal &dsdcVal);
	void resetDSDCVal(double threshold, Timestamp prevTime, double prevVal,
		Timestamp nowTime, double nowVal, DSDCVal &dsdcVal);

	void updateSubTimeSeriesImage(TransactionContext &txn);
	void setAllocateStrategy() {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
	}
	void setAllocateStrategy(TimeSeries *parent) {
		metaAllocateStrategy_ = parent->metaAllocateStrategy_;
		rowAllocateStrategy_ = parent->subRowAllocateStrategy_;
		mapAllocateStrategy_ = parent->subMapAllocateStrategy_;
		ExpireType expireType = getExpireType();
		if (expireType != NO_EXPIRE) {
			int64_t duration = getExpirationInfo().duration_;
			Timestamp baseTime = endTime_;
			if (expireType == TABLE_EXPIRE) {
				duration = getContainerExpirationDutation();
				baseTime = getContainerExpirationEndTime();
			}
			ExpireIntervalCategoryId expireCategoryId = DEFAULT_EXPIRE_CATEGORY_ID;
			ChunkKey chunkKey = MAX_CHUNK_KEY;
			if (baseTime + duration + EXPIRE_MARGIN > baseTime) {
				calcChunkKey(baseTime, duration, expireCategoryId, chunkKey);
			}
			rowAllocateStrategy_.chunkKey_ = chunkKey;
			rowAllocateStrategy_.expireCategoryId_ = expireCategoryId;
			mapAllocateStrategy_.chunkKey_ = chunkKey;
			mapAllocateStrategy_.expireCategoryId_ = expireCategoryId;
		}
	}

	void checkExclusive(TransactionContext &) {}

	bool getIndexData(TransactionContext &txn, const util::Vector<ColumnId> &columnIds,
		MapType mapType, bool withUncommitted, IndexData &indexData,
		bool withPartialMatch = false) const {
		return indexSchema_->getIndexData(
			txn, columnIds, mapType, position_, withUncommitted, 
			withPartialMatch, indexData);
	}
	bool getIndexData(TransactionContext &txn, IndexCursor &indexCursor,
		IndexData &indexData) const {
		return indexSchema_->getIndexData(
			txn, indexCursor, position_, indexData);
	}
	void createNullIndexData(TransactionContext &txn, IndexData &indexData) {
		indexSchema_->createNullIndexData(txn, position_, indexData, this);
	}

	void finalizeIndex(TransactionContext &txn) {
		if (!isExpired(txn)) {
			indexSchema_->dropAll(txn, this, position_, true);
		}
	}

	bool isCompressionErrorMode() {
		return parent_->isCompressionErrorMode();
	}

	void incrementRowNum() {
		parent_->incrementRowNum();
	}
	void decrementRowNum() {
		parent_->decrementRowNum();
	}

	bool isAlterContainer() const {
		return parent_->isAlterContainer();
	}
	void setAlterContainer() {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
	}
	void resetAlterContainer() {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
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
};

/*!
	@brief Cursor for aggregate values
*/
struct AggregationCursor {

	Operator operator_;		   
	Calculator2 calculator1_;  
	Calculator2 calculator2_;  
	int32_t count_;			   
	Value value1_;
	Value value2_;

	AggregationType aggType_;
	ColumnType type_;

	AggregationCursor() {
		count_ = 0;
	}
	void set(AggregationType aggType, ColumnType type) {
		count_ = 0;
		aggType_ = aggType;
		type_ = type;
		switch (aggType) {
		case AGG_MIN:
			operator_ = ComparatorTable::ltTable_[type][type];
			break;
		case AGG_MAX:
			operator_ = ComparatorTable::gtTable_[type][type];
			break;
		case AGG_SUM:
			calculator1_ = CalculatorTable::addTable_[type][type];
			break;
		case AGG_AVG:
			calculator1_ = CalculatorTable::addTable_[type][type];
			break;
		case AGG_COUNT:
			break;
		case AGG_VARIANCE:
			calculator1_ = CalculatorTable::addTable_[type][type];
			calculator2_ = CalculatorTable::mulTable_[type][type];
			break;
		case AGG_STDDEV:
			calculator1_ = CalculatorTable::addTable_[type][type];
			calculator2_ = CalculatorTable::mulTable_[type][type];
			break;
		case AGG_TIME_AVG:
		default:
			break;
		}
	}
};

typedef void (*AggregatorForLoop)(
	TransactionContext &txn, const Value &value, AggregationCursor &cursor);

/*!
	@brief Execute min function for a Row
*/
static void minForLoop(
	TransactionContext &txn, const Value &value, AggregationCursor &cursor) {
	if (cursor.count_ == 0) {
		cursor.value1_ = value;
	}
	else {
		if (cursor.operator_(txn, value.data(), value.size(),
				cursor.value1_.data(), cursor.value1_.size())) {
			cursor.value1_ = value;
		}
	}

	cursor.count_++;
}
/*!
	@brief Execute max function for a Row
*/
static void maxForLoop(
	TransactionContext &txn, const Value &value, AggregationCursor &cursor) {
	if (cursor.count_ == 0) {
		cursor.value1_ = value;
	}
	else {
		if (cursor.operator_(txn, value.data(), value.size(),
				cursor.value1_.data(), cursor.value1_.size())) {
			cursor.value1_ = value;
		}
	}

	cursor.count_++;
}
/*!
	@brief Execute sum function for a Row
*/
static void sumForLoop(
	TransactionContext &txn, const Value &value, AggregationCursor &cursor) {
	if (cursor.count_ == 0) {
		cursor.value1_ = value;
	}
	else {
		cursor.calculator1_(txn, value.data(), value.size(),
			cursor.value1_.data(), cursor.value1_.size(), cursor.value1_);
	}

	cursor.count_++;
}
/*!
	@brief Execute avg function for a Row
*/
static void avgForLoop(
	TransactionContext &txn, const Value &value, AggregationCursor &cursor) {
	if (cursor.count_ == 0) {
		cursor.value1_ = value;
	}
	else {
		cursor.calculator1_(txn, value.data(), value.size(),
			cursor.value1_.data(), cursor.value1_.size(), cursor.value1_);
	}

	cursor.count_++;
}
/*!
	@brief Execute count function for a Row
*/
static void countForLoop(
	TransactionContext &, const Value &, AggregationCursor &cursor) {
	cursor.count_++;
}

/*!
	@brief Execute variance function for a Row
*/
static void varianceForLoop(
	TransactionContext &txn, const Value &value, AggregationCursor &cursor) {
	if (cursor.count_ == 0) {
		cursor.value1_ = value;
	}
	else {
		cursor.calculator1_(txn, value.data(), value.size(),
			cursor.value1_.data(), cursor.value1_.size(), cursor.value1_);
	}

	if (cursor.count_ == 0) {
		cursor.calculator2_(txn, value.data(), value.size(), value.data(),
			value.size(), cursor.value2_);
	}
	else {
		Value tmpValue;
		cursor.calculator2_(txn, value.data(), value.size(), value.data(),
			value.size(), tmpValue);
		cursor.calculator1_(txn, tmpValue.data(), tmpValue.size(),
			cursor.value2_.data(), cursor.value2_.size(), cursor.value2_);
	}

	cursor.count_++;
}

/*!
	@brief Execute stddev function for a Row
*/
static void stddevForLoop(
	TransactionContext &txn, const Value &value, AggregationCursor &cursor) {
	if (cursor.count_ == 0) {
		cursor.value1_ = value;
	}
	else {
		cursor.calculator1_(txn, value.data(), value.size(),
			cursor.value1_.data(), cursor.value1_.size(), cursor.value1_);
	}

	if (cursor.count_ == 0) {
		cursor.calculator2_(txn, value.data(), value.size(), value.data(),
			value.size(), cursor.value2_);
	}
	else {
		Value tmpValue;
		cursor.calculator2_(txn, value.data(), value.size(), value.data(),
			value.size(), tmpValue);
		cursor.calculator1_(txn, tmpValue.data(), tmpValue.size(),
			cursor.value2_.data(), cursor.value2_.size(), cursor.value2_);
	}

	cursor.count_++;
}

void aggregationPostProcess(AggregationCursor &cursor, Value &value);

static const AggregatorForLoop aggLoopTable[] = {&minForLoop, &maxForLoop,
	&sumForLoop, &avgForLoop, &varianceForLoop, &stddevForLoop, &countForLoop,
	NULL};

#endif
