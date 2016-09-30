/*
	Copyright (c) 2012 TOSHIBA CORPORATION.

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

#include "util/trace.h"
#include "base_container.h"
#include "btree_map.h"
#include "data_store.h"
#include "data_store_common.h"  
#include "data_type.h"
#include "hash_map.h"
#include "message_row_store.h"
#include "schema.h"
#include "value_operator.h"
#include "value_processor.h"
#include <float.h>

class ColumnInfo;
class MessageTimeSeriesSchema;

UTIL_TRACER_DECLARE(TIME_SERIES);

static const uint8_t EXPIRE_DIVIDE_DEFAULT_NUM = 8;
static const Timestamp EXPIRE_MARGIN =
	1000;  
static const OId RUNTIME_BASE_FILTER =
	0x7FFFFFFFFFFFFFFFLL;  
static const OId RUNTIME_SET_FILTER =
	0x8000000000000000ULL;  



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
		@brief ExpirationInfo format
	*/
	struct ExpirationInfo {
		int64_t duration_;	 
		int32_t elapsedTime_;  
		uint16_t dividedNum_;  
		TimeUnit timeUnit_;	
		uint8_t padding_;	  
		ExpirationInfo() {
			duration_ = INT64_MAX;  
			elapsedTime_ = -1;		
			timeUnit_ = TIME_UNIT_DAY;
			dividedNum_ = EXPIRE_DIVIDE_DEFAULT_NUM;
			padding_ = 0;
		}
		bool operator==(const ExpirationInfo &b) const {
			if (memcmp(this, &b, sizeof(ExpirationInfo)) == 0) {
				return true;
			}
			else {
				return false;
			}
		}
		int64_t getHashVal() const {
			int64_t hashVal = 0;
			hashVal += duration_;
			hashVal += elapsedTime_;
			hashVal += dividedNum_;
			hashVal += timeUnit_;
			return hashVal;
		}
	};
	/*!
		@brief TimeSeries format
	*/
	struct TimeSeriesImage : BaseContainerImage {
		uint64_t reserved_;  
		uint64_t padding1_;  
		uint64_t padding2_;  
		uint64_t padding3_;  
		uint64_t padding4_;  
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
			return (mvccMapOId_ & RUNTIME_SET_FILTER) != 0;
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
	/*!
		@brief RowArray for TimeSeries
	*/
	class RowArray : public BaseObject {
	public:  
		/*!
			@brief Row for TimeSeries
		*/
		class Row {
		public:
			Row(uint8_t *rowImage, RowArray *rowArrayCursor);
			void initialize();
			void finalize(TransactionContext &txn);
			void setFields(
				TransactionContext &txn, MessageRowStore *messageRowStore);
			void updateFields(
				TransactionContext &txn, MessageRowStore *messageRowStore);
			void getField(TransactionContext &txn, const ColumnInfo &columnInfo,
				BaseObject &baseObject);
			void getField(TransactionContext &txn, const ColumnInfo &columnInfo,
				ContainerValue &containerValue);
			void getRowIdField(uint8_t *&data);
			void remove(TransactionContext &txn);
			void move(TransactionContext &txn, RowArray::Row &dest);
			void copy(TransactionContext &txn, RowArray::Row &dest);
			/*!
				@brief Lock this Row
			*/
			void lock(TransactionContext &txn) {
				rowArrayCursor_->lock(txn);
			}
			/*!
				@brief Set flag that this Row is already updated in the
			   transaction
			*/
			void setFirstUpdate() {
				rowArrayCursor_->setFirstUpdate();
			}
			/*!
				@brief Reset flag that this Row is already updated in the
			   transaction
			*/
			void resetFirstUpdate() {
				rowArrayCursor_->resetFirstUpdate();
			}
			/*!
				@brief Check if this Row is already updated in the transaction
			*/
			bool isFirstUpdate() {
				return rowArrayCursor_->isFirstUpdate();
			}

			/*!
				@brief Set RowId
			*/
			void setRowId(RowId rowId) {
				memcpy(getRowIdAddr(), &rowId, sizeof(RowId));
			}
			/*!
				@brief Get TransactionId when finally updated
			*/
			TransactionId getTxnId() {
				return rowArrayCursor_->getTxnId();
			}
			/*!
				@brief Get RowId
			*/
			RowId getRowId() {
				return *reinterpret_cast<RowId *>(getRowIdAddr());
			}
			/*!
				@brief Check if this Row is already removed
			*/
			static bool isRemoved(uint8_t *binary) {
				return (*reinterpret_cast<RowHeader *>(binary) & REMOVE_BIT) !=
					   0;
			}
			/*!
				@brief Check if this Row is already removed
			*/
			bool isRemoved() {
				return isRemoved(binary_);
			}
			/*!
				@brief Set removed flag
			*/
			void reset() {
				setRemoved();
			}

			void getImage(TransactionContext &txn,
				MessageRowStore *messageRowStore, bool isWithRowId);
			void getFieldImage(TransactionContext &txn,
				ColumnInfo &srcColumnInfo, uint32_t destColumnInfo,
				MessageRowStore *messageRowStore);
			/*!
				@brief Get address of variable OId
			*/
			uint8_t *getVariableArrayAddr() {
				return getFixedAddr();
			}

			/*!
				@brief Get Row key(Timestamp)
			*/
			Timestamp getTime() {
				return *reinterpret_cast<Timestamp *>(getRowIdAddr());
			}
			/*!
				@brief Check if this Row meet a condition
			*/
			bool isMatch(TransactionContext &txn, TermCondition &cond,
				ContainerValue &tmpValue) {
				bool isMatch = false;
				if (cond.columnId_ == UNDEF_COLUMNID) {
					RowId rowId = getRowId();
					isMatch =
						cond.operator_(txn, reinterpret_cast<uint8_t *>(&rowId),
							sizeof(RowId), cond.value_, cond.valueSize_);
				}
				else {
					getField(txn, rowArrayCursor_->getContainer().getColumnInfo(
									  cond.columnId_),
						tmpValue);
					isMatch = cond.operator_(txn, tmpValue.getValue().data(),
						tmpValue.getValue().size(), cond.value_,
						cond.valueSize_);
				}
				return isMatch;
			}
			std::string dump(TransactionContext &txn);

		private:  
			static const size_t FIXED_ADDR_OFFSET =
				sizeof(RowHeader);  
			static const size_t VARIABLE_ARRAY_OFFSET =
				sizeof(RowHeader);  
			static const RowHeader REMOVE_BIT =
				0x80;  

		private:  
			RowArray *rowArrayCursor_;
			uint8_t *binary_;
			size_t nullsOffset_;
		private:
			void checkVarDataSize(TransactionContext &txn, uint32_t columnNum,
				uint32_t variableColumnNum, uint8_t *varTopAddr,
				util::XArray<uint32_t> &varColumnIdList,
				util::XArray<uint32_t> &varDataObjectSizeList,
				util::XArray<uint32_t> &varDataObjectPosList);
			uint8_t *getRowIdAddr() {
				return getFixedAddr() +
					   rowArrayCursor_->getContainer()
						   .getColumnInfo(ColumnInfo::ROW_KEY_COLUMN_ID)
						   .getColumnOffset();
			}
			uint8_t *getTIdAddr() {
				return binary_ + TID_OFFSET;
			}
			uint8_t *getFixedAddr() {
				return binary_ + FIXED_ADDR_OFFSET;
			}
			void setVariableArray(OId oId) {
				memcpy(getVariableArrayAddr(), &oId, sizeof(OId));
			}
			OId getVariableArray() {
				return *reinterpret_cast<OId *>(getVariableArrayAddr());
			}
			uint8_t *getNullsAddr() {
				return binary_ + FIXED_ADDR_OFFSET + nullsOffset_;
			}
			void setRemoved() {
				RowHeader *val = reinterpret_cast<RowHeader *>(binary_);
				*val |= REMOVE_BIT;
			}
			uint8_t *getAddr() {
				return binary_;
			}
		};

	public:
		RowArray(TransactionContext &txn, OId oId, TimeSeries *container,
			uint8_t getOption);
		RowArray(TransactionContext &txn, TimeSeries *container);
		void load(TransactionContext &txn, OId oId, TimeSeries *container,
			uint8_t getOption);
		void initialize(
			TransactionContext &txn, RowId baseRowId, uint16_t maxRowNum);
		void finalize(TransactionContext &txn);

		void append(TransactionContext &txn, MessageRowStore *messageRowStore,
			RowId rowId);
		void insert(TransactionContext &txn, MessageRowStore *messageRowStore,
			RowId rowId);
		void update(TransactionContext &txn, MessageRowStore *messageRowStore);
		void remove(TransactionContext &txn);
		void move(TransactionContext &txn, RowArray &dest);
		void copy(TransactionContext &txn, RowArray &dest);
		void copyRowArray(TransactionContext &txn, RowArray &dest);

		bool nextRowArray(
			TransactionContext &, RowArray &neighbor, uint8_t getOption);
		bool prevRowArray(
			TransactionContext &, RowArray &neighbor, uint8_t getOption);
		void shift(TransactionContext &txn, bool isForce,
			util::XArray<std::pair<OId, OId> > &moveList);
		void split(TransactionContext &txn, RowId insertRowId,
			RowArray &splitRowArray, RowId splitRowId,
			util::XArray<std::pair<OId, OId> > &moveList);
		void merge(TransactionContext &txn, RowArray &nextRowArray,
			util::XArray<std::pair<OId, OId> > &moveList);

		/*!
			@brief Move to next Row, and Check if Row exists
		*/
		bool next() {
			while (elemCursor_ + 1 < getRowNum()) {
				elemCursor_++;
				if (!RowArray::Row::isRemoved(getRow())) {
					return true;
				}
			}
			elemCursor_++;
			return false;
		}

		/*!
			@brief Move to prev Row, and Check if Row exists
		*/
		bool prev() {
			while (elemCursor_ > 0) {
				elemCursor_--;
				if (!RowArray::Row::isRemoved(getRow())) {
					return true;
				}
			}
			return false;
		}

		/*!
			@brief Move to first Row, and Check if Row exists
		*/
		bool begin() {
			elemCursor_ = 0;
			if (Row::isRemoved(getRow())) {
				return next();
			}
			return true;
		}

		/*!
			@brief Check if cursor reached to end
		*/
		bool end() {
			if (elemCursor_ >= getRowNum()) {
				return true;
			}
			else {
				return false;
			}
		}

		/*!
			@brief Move to last Row, and Check if Row exists
		*/
		bool tail() {
			if (getRowNum() != 0) {
				elemCursor_ = getRowNum() - 1;
			}
			else {
				elemCursor_ = 0;
			}
			if (RowArray::Row::isRemoved(getRow())) {
				return prev();
			}
			return true;
		}

		/*!
			@brief Check if next Row exists
		*/
		bool hasNext() {
			if (elemCursor_ + 1 < getRowNum()) {
				return true;
			}
			else {
				return false;
			}
		}

		bool searchRowId(RowId rowId);
		/*!
			@brief Get Middle RowId of RowArray
		*/
		RowId getMidRowId() {
			uint16_t midPos = getMaxRowNum() / 2;
			Row midRow(getRow(midPos), this);
			return midRow.getRowId();
		}

		/*!
			@brief Check if reserved Row area is full to capacity
		*/
		bool isFull() {
			return getActiveRowNum() == getMaxRowNum() ? true : false;
		}
		/*!
			@brief Check if RowArray is initialized
		*/
		bool isNotInitialized() {
			return BaseObject::getBaseOId() == UNDEF_OID ? true : false;
		}
		/*!
			@brief Check if cursor is last Row
		*/
		bool isTailPos() {
			return elemCursor_ >= getMaxRowNum() - 1 ? true : false;
		}
		/*!
			@brief Get OId of Row(RowArray OId + row offset)
		*/
		OId getOId() {
			return ((static_cast<OId>(elemCursor_) << (64 - ELEM_BIT)) |
					getBaseOId());
		}
		/*!
			@brief Get OId of RowArray
		*/
		OId getBaseOId() {
			return getBaseOId(BaseObject::getBaseOId());
		}
		/*!
			@brief Return address of new Row
		*/
		uint8_t *getNewRow() {
			elemCursor_ = getRowNum();
			return getRow();
		}
		/*!
			@brief Get address of current Row
		*/
		uint8_t *getRow() {
			return getRow(elemCursor_);
		}
		/*!
			@brief Get number of Rows(include removed Rows)
		*/
		uint16_t getRowNum() {
			return *reinterpret_cast<uint16_t *>(getAddr() + ROW_NUM_OFFSET);
		}
		/*!
			@brief Get number of existed Rows
		*/
		uint16_t getActiveRowNum() {
			updateCursor();
			uint16_t activeRowNum = 0;
			for (uint16_t i = 0; i < getRowNum(); i++) {
				if (!RowArray::Row::isRemoved(getRow(i))) {
					activeRowNum++;
				}
			}
			return activeRowNum;
		}
		/*!
			@brief Get maximum that it can store
		*/
		uint16_t getMaxRowNum() {
			return *reinterpret_cast<uint16_t *>(
				getAddr() + MAX_ROW_NUM_OFFSET);
		}
		/*!
			@brief Get header size of RowArray Object
		*/
		static uint32_t getHeaderSize() {
			return HEADER_SIZE;
		}

		/*!
			@brief Get RowId address of current Row
		*/
		uint8_t *getRowIdAddr() {
			return getAddr() + ROWID_OFFSET;
		}
		uint8_t *getTIdAddr() {
			return getAddr() + TID_OFFSET;
		}
		void lock(TransactionContext &txn);
		/*!
			@brief Set flag that this RowArray is already updated in the
		   transaction
		*/
		void setFirstUpdate() {
			uint8_t *val = reinterpret_cast<uint8_t *>(getBitsAddr());
			*val |= FIRST_UPDATE_BIT;
		}
		/*!
			@brief Reset flag that this RowArray is already updated in the
		   transaction
		*/
		void resetFirstUpdate() {
			uint8_t *val = reinterpret_cast<uint8_t *>(getBitsAddr());
			*val ^= FIRST_UPDATE_BIT;
		}
		/*!
			@brief Check if this RowArray is already updated in the transaction
		*/
		bool isFirstUpdate() {
			return (*reinterpret_cast<uint8_t *>(getBitsAddr()) &
					   FIRST_UPDATE_BIT) != 0;
		}
		/*!
			@brief Set RowId to current Row
		*/
		void setRowId(RowId rowId) {
			memcpy(getRowIdAddr(), &rowId, sizeof(RowId));
		}
		/*!
			@brief Get TransactionId when finally updated
		*/
		TransactionId getTxnId() {
			TransactionId tId =
				*reinterpret_cast<TransactionId *>(getTIdAddr());
			return (tId & TID_FIELD);
		}
		/*!
			@brief Get RowId of current Row
		*/
		RowId getRowId() {
			return *reinterpret_cast<RowId *>(getRowIdAddr());
		}
		std::string dump(TransactionContext &txn);

	private:  
		static const size_t HEADER_FREE_AREA_SIZE =
			20;  
		static const size_t HEADER_SIZE =
			sizeof(uint16_t) * 2 + sizeof(RowId) + sizeof(TransactionId) +
			HEADER_FREE_AREA_SIZE;  
		static const size_t MAX_ROW_NUM_OFFSET = 0;  
		static const size_t ROW_NUM_OFFSET =
			sizeof(uint16_t);  
		static const size_t ROWID_OFFSET =
			sizeof(uint16_t) * 2;  
		static const size_t TID_OFFSET =
			ROWID_OFFSET + sizeof(RowId);  
		static const size_t BITS_OFFSET =
			TID_OFFSET + 7;  
		static const size_t ROW_AREA_OFFSET =
			TID_OFFSET + sizeof(TransactionId);  
		static const uint8_t FIRST_UPDATE_BIT =
			0x40;  
		static const TransactionId TID_FIELD =
			0x00ffffffffffffffLL;  
		static const TransactionId BITS_FIELD =
			0xff00000000000000LL;  

		struct Header {
			uint16_t elemCursor_;
		};
		static const size_t ELEM_BIT = 10;
		static const OId BASE_FILTER = 0x003FFFFFFFFFFFFFLL;

	private:  
		TimeSeries *container_;
		size_t rowSize_;
		uint16_t elemCursor_;

	private:  
		uint8_t *getBitsAddr() {
			return getAddr() + BITS_OFFSET;
		}
		uint8_t *getRow(uint16_t elem) {
			return getAddr() + HEADER_SIZE + elem * rowSize_;
		}
		void setMaxRowNum(uint16_t num) {
			uint16_t *addr =
				reinterpret_cast<uint16_t *>(getAddr() + MAX_ROW_NUM_OFFSET);
			*addr = num;
		}
		void setRowNum(uint16_t num) {
			uint16_t *addr =
				reinterpret_cast<uint16_t *>(getAddr() + ROW_NUM_OFFSET);
			*addr = num;
		}
		uint8_t *getAddr() {
			return getBaseAddr();
		}
		TimeSeries &getContainer() {
			return *container_;
		}
		uint16_t getElemCursor(OId oId) {
			return static_cast<uint16_t>((oId >> (64 - ELEM_BIT)));
		}
		void setLockTId(TransactionId tId) {
			TransactionId *targetAddr =
				reinterpret_cast<TransactionId *>(getTIdAddr());
			TransactionId filterTId = (tId & TID_FIELD);
			TransactionId header = ((*targetAddr) & BITS_FIELD);
			*targetAddr = (header | filterTId);
			setFirstUpdate();
		}
		void updateCursor() {
			uint16_t currentCursor = -1;
			for (uint16_t i = 0; i < getMaxRowNum(); i++) {
				if (!RowArray::Row::isRemoved(getRow(i))) {
					currentCursor = i;
				}
			}
			setRowNum(currentCursor + 1);
		}
		OId getBaseOId(OId oId) {
			return (oId & BASE_FILTER);
		}
		uint32_t getBinarySize(uint16_t maxRowNum) {
			return static_cast<uint32_t>(
				getHeaderSize() + rowSize_ * maxRowNum);
		}
		RowArray(const RowArray &);  
		RowArray &operator=(const RowArray &);  
	};

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
	}
	TimeSeries(TransactionContext &txn, DataStore *dataStore)
		: BaseContainer(txn, dataStore), subTimeSeriesListHead_(NULL) {}
	~TimeSeries() {
		ALLOC_DELETE((alloc_), subTimeSeriesListHead_);
	}
	void initialize(TransactionContext &txn);
	void finalize(TransactionContext &txn);
	void set(TransactionContext &txn, const char8_t *containerName,
		ContainerId containerId, OId columnSchemaOId,
		MessageSchema *orgContainerSchema);

	void createIndex(TransactionContext &txn, IndexInfo &indexInfo);
	void dropIndex(TransactionContext &txn, IndexInfo &indexInfo);


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
	/*!
		@brief Updates a Row corresponding to the specified RowId at recovery
	   phase
	*/
	void redoUpdateRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowData, RowId rowId, DataStore::PutStatus &status) {
		bool isForceLock = true;
		updateRow(txn, rowSize, rowData, rowId, status, isForceLock);
	}
	void abort(TransactionContext &txn);
	void commit(TransactionContext &txn);
	void changeSchema(TransactionContext &txn, BaseContainer &newContainer,
		util::XArray<uint32_t> &copyColumnMap);
	bool hasUncommitedTransaction(TransactionContext &txn);

	void searchRowIdIndex(TransactionContext &txn, BtreeMap::SearchContext &sc,
		util::XArray<OId> &resultList, OutputOrder order);
	void searchRowIdIndex(TransactionContext &txn, uint64_t start,
		uint64_t limit, util::XArray<RowId> &rowIdList,
		util::XArray<OId> &resultList, uint64_t &skipped);
	void searchColumnIdIndex(TransactionContext &txn,
		BtreeMap::SearchContext &sc, util::XArray<OId> &resultList,
		OutputOrder order);
	void aggregate(TransactionContext &txn, BtreeMap::SearchContext &sc,
		uint32_t columnId, AggregationType type, ResultSize &resultNum,
		util::XArray<uint8_t> &serializedRowList);  
	void sample(TransactionContext &txn, BtreeMap::SearchContext &sc,
		const Sampling &sampling, ResultSize &resultNum,
		MessageRowStore *messageRowStore);  
	void sampleWithoutInterp(TransactionContext &txn,
		BtreeMap::SearchContext &sc, const Sampling &sampling,
		ResultSize &resultNum,
		MessageRowStore *messageRowStore);  
	void searchTimeOperator(
		TransactionContext &txn, Timestamp ts, TimeOperator timeOp, OId &oId);

	void lockRowList(TransactionContext &txn, util::XArray<RowId> &rowIdList);
	void setDummyMvccImage(TransactionContext &txn);
	bool isLocked(
		TransactionContext &txn, uint32_t rowKeySize, const uint8_t *rowKey);

	Timestamp getCurrentExpiredTime(TransactionContext &txn);

	/*!
		@brief Calculate AllocateStrategy of Map Object
	*/
	AllocateStrategy calcMapAllocateStrategy() const {
		AllocateStrategy allocateStrategy;
		ChunkCategoryId chunkCategoryId;
		if (getExpirationInfo().duration_ == INT64_MAX) {
			chunkCategoryId = ALLOCATE_NO_EXPIRE_MAP;
		}
		else {
			chunkCategoryId = ALLOCATE_EXPIRE_MAP;
		}
		AffinityGroupId groupId = calcAffnityGroupId(getAffinity());
		return AllocateStrategy(chunkCategoryId, groupId);
	}
	/*!
		@brief Calculate AllocateStrategy of Row Object
	*/
	AllocateStrategy calcRowAllocateStrategy() const {
		AllocateStrategy allocateStrategy;
		ChunkCategoryId chunkCategoryId;
		if (getExpirationInfo().duration_ == INT64_MAX) {
			chunkCategoryId = ALLOCATE_NO_EXPIRE_ROW;
		}
		else {
			chunkCategoryId = ALLOCATE_EXPIRE_ROW;
		}
		AffinityGroupId groupId = calcAffnityGroupId(getAffinity());
		return AllocateStrategy(chunkCategoryId, groupId);
	}
	/*!
		@brief Check if expired option is defined
	*/
	bool isExpireContainer() {
		return getExpirationInfo().duration_ != INT64_MAX;
	}

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
	SubTimeSeriesList *subTimeSeriesListHead_;  
private:										
	SubTimeSeries *putSubContainer(TransactionContext &txn, Timestamp rowKey);
	SubTimeSeries *getSubContainer(
		TransactionContext &txn, Timestamp rowKey, bool forUpdate);
	void putRow(TransactionContext &txn,
		InputMessageRowStore *inputMessageRowStore, RowId &rowId,
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
	void checkContainerOption(MessageSchema *orgMessageSchema,
		util::XArray<uint32_t> &copyColumnMap, bool &isCompletelySameSchema);

	uint32_t calcRowImageSize(uint32_t rowFixedSize) {
		uint32_t rowImageSize_ = sizeof(RowHeader) + rowFixedSize;
		return rowImageSize_;
	}
	uint32_t calcRowFixedDataSize() {
		uint32_t rowFixedDataSize =
			ValueProcessor::calcNullsByteSize(columnSchema_->getColumnNum()) +
			columnSchema_->getRowFixedSize();
		if (columnSchema_->getVariableColumnNum()) {
			rowFixedDataSize += sizeof(OId);
		}
		return rowFixedDataSize;
	}
	uint16_t calcRowArrayNum(uint16_t baseRowNum);

	void setAllocateStrategy() {
		metaAllocateStrategy_ = calcMetaAllocateStrategy();
		rowAllocateStrategy_ = calcRowAllocateStrategy();
		mapAllocateStrategy_ = calcMapAllocateStrategy();
	}

	void checkExclusive(TransactionContext &) {}

	ExpirationInfo &getExpirationInfo() const {
		ExpirationInfo *expirationInfo =
			commonContainerSchema_->get<ExpirationInfo>(META_TYPE_DURATION);
		return *expirationInfo;
	}
	void getSubTimeSeriesList(TransactionContext &txn,
		util::XArray<SubTimeSeriesInfo> &subTimeSeriesList, bool forUpdate);
	void getSubTimeSeriesList(TransactionContext &txn,
		BtreeMap::SearchContext &sc,
		util::XArray<SubTimeSeriesInfo> &subTimeSeriesList, bool forUpdate);
	void getRuntimeSubTimeSeriesList(TransactionContext &txn,
		util::XArray<SubTimeSeriesInfo> &subTimeSeriesList, bool forUpdate);
	bool findStartSubTimeSeriesPos(TransactionContext &txn,
		Timestamp targetBeginTime, int64_t lowPos, int64_t highPos,
		int64_t &midPos);  
	void searchRowArrayList(TransactionContext &txn,
		BtreeMap::SearchContext &sc, util::XArray<OId> &normalOIdList,
		util::XArray<OId> &mvccOIdList);


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
	int32_t expireSubTimeSeries(TransactionContext &txn, Timestamp expiredTime);

	int64_t getDivideDuration() {
		const ExpirationInfo &expirationInfo = getExpirationInfo();
		return expirationInfo.duration_ / expirationInfo.dividedNum_;
	}

	void updateSubTimeSeriesImage(
		TransactionContext &txn, const SubTimeSeriesInfo &subTimeSeriesInfo);

	bool getIndexData(TransactionContext &txn, ColumnId columnId,
		MapType mapType, IndexData &indexData) const {
		return indexSchema_->getIndexData(
			txn, columnId, mapType, UNDEF_CONTAINER_POS, indexData);
	}
	void getIndexList(
		TransactionContext &txn, util::XArray<IndexData> &list) const {
		indexSchema_->getIndexList(txn, UNDEF_CONTAINER_POS, list);
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

	void insertRowIdMap(
		TransactionContext &txn, BtreeMap *map, const void *constKey, OId oId);
	void insertMvccMap(TransactionContext &txn, BtreeMap *map,
		TransactionId tId, MvccRowImage &mvccImage);
	void insertValueMap(TransactionContext &txn, BaseIndex *map,
		const void *constKey, OId oId, ColumnId columnId, MapType mapType);
	void updateRowIdMap(TransactionContext &txn, BtreeMap *map,
		const void *constKey, OId oldOId, OId newOId);
	void updateMvccMap(TransactionContext &txn, BtreeMap *map,
		TransactionId tId, MvccRowImage &oldMvccImage,
		MvccRowImage &newMvccImage);
	void updateValueMap(TransactionContext &txn, BaseIndex *map,
		const void *constKey, OId oldOId, OId newOId, ColumnId columnId,
		MapType mapType);
	void removeRowIdMap(
		TransactionContext &txn, BtreeMap *map, const void *constKey, OId oId);
	void removeMvccMap(TransactionContext &txn, BtreeMap *map,
		TransactionId tId, MvccRowImage &mvccImage);
	void removeValueMap(TransactionContext &txn, BaseIndex *map,
		const void *constKey, OId oId, ColumnId columnId, MapType mapType);
	void updateIndexData(TransactionContext &txn, IndexData indexData);

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
		  startTime_(0) {}
	~SubTimeSeries() {}
	void initialize(uint64_t position, Timestamp startTime);
	void initialize(TransactionContext &txn);
	void finalize(TransactionContext &txn);
	void set(TransactionContext &txn, const char8_t *containerName,
		ContainerId containerId, OId columnSchemaOId,
		MessageSchema *orgContainerSchema);
	void set(TransactionContext &txn, TimeSeries *parent);

	void createIndex(TransactionContext &txn, IndexInfo &indexInfo);
	void dropIndex(TransactionContext &txn, IndexInfo &indexInfo);


	void putRow(TransactionContext &txn,
		InputMessageRowStore *inputMessageRowStore, RowId &rowId,
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
	/*!
		@brief Updates a Row corresponding to the specified RowId at recovery
	   phase
	*/
	void redoUpdateRow(TransactionContext &, uint32_t, const uint8_t *, RowId,
		DataStore::PutStatus &) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "unsupport operation");
	}
	void deleteRow(
		TransactionContext &txn, RowId rowId, bool &existing, bool isForceLock);
	void updateRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowData, RowId rowId, DataStore::PutStatus &status,
		bool isForceLock);
	void abort(TransactionContext &txn);
	void commit(TransactionContext &txn);
	void changeSchema(TransactionContext &txn, BaseContainer &newContainer,
		util::XArray<uint32_t> &copyColumnMap);

	void searchRowIdIndex(TransactionContext &txn, BtreeMap::SearchContext &sc,
		util::XArray<OId> &resultList, OutputOrder order);
	void searchRowIdIndex(TransactionContext &txn, uint64_t start,
		uint64_t limit, util::XArray<RowId> &rowIdList,
		util::XArray<OId> &resultList, uint64_t &skipped);




	void setDummyMvccImage(TransactionContext &txn);

	void getIndexList(
		TransactionContext &txn, util::XArray<IndexData> &list) const {
		indexSchema_->getIndexList(txn, position_, list);
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
			(baseContainerImage_->mvccMapOId_ | RUNTIME_SET_FILTER);
	}
	void resetRuntime() {
		baseContainerImage_->mvccMapOId_ =
			(baseContainerImage_->mvccMapOId_ & RUNTIME_BASE_FILTER);
	}
	bool isRuntime() {
		return (baseContainerImage_->mvccMapOId_ & RUNTIME_SET_FILTER) != 0;
	}


	void updateSubTimeSeriesImage(TransactionContext &txn);
	ChunkKey getSetChunkKey() const {
		if (getExpirationInfo().duration_ == INT64_MAX) {
			return UNDEF_CHUNK_KEY;
		}
		else {
			if (endTime_ + getExpirationInfo().duration_ + EXPIRE_MARGIN >
				endTime_) {
				return DataStore::convertTimestamp2ChunkKey(
					endTime_ + getExpirationInfo().duration_ + EXPIRE_MARGIN,
					true);
			}
			else {
				return MAX_CHUNK_KEY;
			}
		}
	}
	void setAllocateStrategy() {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
	}
	void setAllocateStrategy(TimeSeries *parent) {
		metaAllocateStrategy_ = parent->metaAllocateStrategy_;
		rowAllocateStrategy_ = parent->rowAllocateStrategy_;
		mapAllocateStrategy_ = parent->mapAllocateStrategy_;
		if (getExpirationInfo().duration_ != INT64_MAX) {
			ChunkKey chunkKey = getSetChunkKey();
			rowAllocateStrategy_.chunkKey_ = chunkKey;
			mapAllocateStrategy_.chunkKey_ = chunkKey;
		}
	}

	void checkExclusive(TransactionContext &) {}

	bool getIndexData(TransactionContext &txn, ColumnId columnId,
		MapType mapType, IndexData &indexData) const {
		return indexSchema_->getIndexData(
			txn, columnId, mapType, position_, indexData);
	}
	void finalizeIndex(TransactionContext &txn) {
		indexSchema_->dropAll(txn, this, position_, true);
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

	void insertRowIdMap(
		TransactionContext &txn, BtreeMap *map, const void *constKey, OId oId);
	void insertMvccMap(TransactionContext &txn, BtreeMap *map,
		TransactionId tId, MvccRowImage &mvccImage);
	void insertValueMap(TransactionContext &txn, BaseIndex *map,
		const void *constKey, OId oId, ColumnId columnId, MapType mapType);
	void updateRowIdMap(TransactionContext &txn, BtreeMap *map,
		const void *constKey, OId oldOId, OId newOId);
	void updateMvccMap(TransactionContext &txn, BtreeMap *map,
		TransactionId tId, MvccRowImage &oldMvccImage,
		MvccRowImage &newMvccImage);
	void updateValueMap(TransactionContext &txn, BaseIndex *map,
		const void *constKey, OId oldOId, OId newOId, ColumnId columnId,
		MapType mapType);
	void removeRowIdMap(
		TransactionContext &txn, BtreeMap *map, const void *constKey, OId oId);
	void removeMvccMap(TransactionContext &txn, BtreeMap *map,
		TransactionId tId, MvccRowImage &mvccImage);
	void removeValueMap(TransactionContext &txn, BaseIndex *map,
		const void *constKey, OId oId, ColumnId columnId, MapType mapType);
	void updateIndexData(TransactionContext &txn, IndexData indexData);
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
			operator_ = ltTable[type][type];
			break;
		case AGG_MAX:
			operator_ = gtTable[type][type];
			break;
		case AGG_SUM:
			calculator1_ = addTable[type][type];
			break;
		case AGG_AVG:
			calculator1_ = addTable[type][type];
			break;
		case AGG_COUNT:
			break;
		case AGG_VARIANCE:
			calculator1_ = addTable[type][type];
			calculator2_ = mulTable[type][type];
			break;
		case AGG_STDDEV:
			calculator1_ = addTable[type][type];
			calculator2_ = mulTable[type][type];
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
