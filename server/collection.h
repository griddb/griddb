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
	@brief Definition of Collection
*/
#ifndef COLLECTION_H_
#define COLLECTION_H_

#include "util/container.h"
#include "btree_map.h"
#include "data_type.h"
#include "hash_map.h"
#include "value_operator.h"
#include "value_processor.h"
#include "util/trace.h"
#include "base_container.h"
#include "data_store.h"
#include "message_row_store.h"
#include "schema.h"

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
		uint64_t padding2_;  
		uint64_t padding3_;  
		uint64_t padding4_;  
		uint64_t padding5_;  
	};

public:  
	static const bool indexMapTable[][MAP_TYPE_NUM];

	/*!
		@brief RowArray for Collection
	*/
	class RowArray : public BaseObject {
		friend class Row;
	public:  
		/*!
			@brief Row for Collection
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
			void move(TransactionContext &txn, Row &dest);
			void copy(TransactionContext &txn, Row &dest);
			void lock(TransactionContext &txn);
			/*!
				@brief Set flag that this Row is already updated in the
			   transaction
			*/
			void setFirstUpdate() {
				RowHeader *val = reinterpret_cast<RowHeader *>(getBitsAddr());
				*val |= FIRST_UPDATE_BIT;
			}
			/*!
				@brief Reset flag that this Row is already updated in the
			   transaction
			*/
			void resetFirstUpdate() {
				RowHeader *val = reinterpret_cast<RowHeader *>(getBitsAddr());
				*val ^= FIRST_UPDATE_BIT;
			}
			/*!
				@brief Check if this Row is already updated in the transaction
			*/
			bool isFirstUpdate() {
				return (*reinterpret_cast<RowHeader *>(getBitsAddr()) &
						   FIRST_UPDATE_BIT) != 0;
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
				TransactionId tId =
					*reinterpret_cast<TransactionId *>(getTIdAddr());
				return (tId & TID_FIELD);
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
			static bool isRemoved(RowHeader *binary) {
				return ((*reinterpret_cast<RowHeader *>(binary + BITS_OFFSET)) &
						   REMOVE_BIT) != 0;
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
			static const size_t TID_OFFSET = 0;  
			static const size_t BITS_OFFSET =
				TID_OFFSET + 7;  
			static const size_t ROWID_OFFSET =
				sizeof(TransactionId);  
			static const size_t FIXED_ADDR_OFFSET =
				ROWID_OFFSET + sizeof(RowId);  
			static const size_t VARIABLE_ARRAY_OFFSET =
				ROWID_OFFSET + sizeof(RowId);  
			static const RowHeader REMOVE_BIT =
				0x80;  
			static const RowHeader FIRST_UPDATE_BIT =
				0x40;  
			static const TransactionId TID_FIELD =
				0x00ffffffffffffffLL;  
			static const TransactionId BITS_FIELD =
				0xff00000000000000LL;  

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
			uint8_t *getBitsAddr() {
				return binary_ + BITS_OFFSET;
			}
			uint8_t *getRowIdAddr() {
				return binary_ + ROWID_OFFSET;
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
				RowHeader *val = reinterpret_cast<RowHeader *>(getBitsAddr());
				*val |= REMOVE_BIT;
			}
			void setLockTId(TransactionId tId) {
				TransactionId *targetAddr =
					reinterpret_cast<TransactionId *>(getTIdAddr());
				TransactionId filterTId = (tId & TID_FIELD);
				TransactionId header = ((*targetAddr) & BITS_FIELD);
				*targetAddr = (header | filterTId);
				setFirstUpdate();
			}
			uint8_t *getAddr() {
				return binary_;
			}
		};

	public:
		RowArray(TransactionContext &txn, OId oId, Collection *container,
			uint8_t getOption);
		RowArray(TransactionContext &txn, Collection *container);
		void load(TransactionContext &txn, OId oId, Collection *container,
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
				if (!Row::isRemoved(getRow())) {
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
				if (!Row::isRemoved(getRow())) {
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
				if (!Row::isRemoved(getRow(i))) {
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
		/*!
			@brief Set RowId to current Row
		*/
		void setRowId(RowId rowId) {
			memcpy(getRowIdAddr(), &rowId, sizeof(RowId));
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
			28;  
		static const size_t HEADER_SIZE =
			sizeof(uint16_t) * 2 + sizeof(RowId) +
			HEADER_FREE_AREA_SIZE;  
		static const size_t MAX_ROW_NUM_OFFSET = 0;  
		static const size_t ROW_NUM_OFFSET =
			sizeof(uint16_t);  
		static const size_t ROW_AREA_OFFSET =
			HEADER_SIZE;  
		static const size_t ROWID_OFFSET =
			sizeof(uint16_t) * 2;  
		struct Header {
			uint16_t elemCursor_;
		};
		static const size_t ELEM_BIT = 10;
		static const OId BASE_FILTER = 0x003FFFFFFFFFFFFFLL;

	private:  
		Collection *container_;
		size_t rowSize_;
		uint16_t elemCursor_;

	private:  
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
		Collection &getContainer() {
			return *container_;
		}
		uint16_t getElemCursor(OId oId) {
			return static_cast<uint16_t>((oId >> (64 - ELEM_BIT)));
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
	Collection(TransactionContext &txn, DataStore *dataStore, OId oId)
		: BaseContainer(txn, dataStore, oId) {
		rowFixedDataSize_ = calcRowFixedDataSize();
		rowImageSize_ = calcRowImageSize(rowFixedDataSize_);
		setAllocateStrategy();
	}
	Collection(TransactionContext &txn, DataStore *dataStore)
		: BaseContainer(txn, dataStore) {
	}
	~Collection() {}
	void initialize(TransactionContext &txn);
	void finalize(TransactionContext &txn);
	void set(TransactionContext &txn, const char8_t *containerName,
		ContainerId containerId, OId columnSchemaOId,
		MessageSchema *containerSchema);

	void createIndex(TransactionContext &txn, IndexInfo &indexInfo);
	void dropIndex(TransactionContext &txn, IndexInfo &indexInfo);



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
		@brief Update a Row corresponding to the specified RowId
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
		@brief Update a Row corresponding to the specified RowId at recovery
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

	void lockRowList(TransactionContext &txn, util::XArray<RowId> &rowIdList);
	bool isLocked(
		TransactionContext &txn, uint32_t rowKeySize, const uint8_t *rowKey);

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

	bool validate(TransactionContext &txn, std::string &errorMessage);
	std::string dump(TransactionContext &txn);

protected:  
protected:  
	void putRow(TransactionContext &txn,
		InputMessageRowStore *inputMessageRowStore, RowId &rowId,
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
	bool isUnique(TransactionContext &txn, uint32_t rowKeySize,
		const uint8_t *rowKey, RowArray &rowArray);
	bool searchRowKeyWithRowIdMap(TransactionContext &txn, uint32_t rowKeySize,
		const uint8_t *rowKey, OId &oId);
	bool searchRowKeyWithMvccMap(TransactionContext &txn, uint32_t rowKeySize,
		const uint8_t *rowKey, OId &oId);

	void deleteRow(
		TransactionContext &txn, RowId rowId, bool &existing, bool isForceLock);
	void updateRow(TransactionContext &txn, uint32_t rowSize,
		const uint8_t *rowData, RowId rowId, DataStore::PutStatus &status,
		bool isForceLock);
	void appendRowInternal(TransactionContext &txn,
		MessageRowStore *messageRowStore, RowArray &rowArray, RowId &rowId);
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
		return ++reinterpret_cast<CollectionImage *>(this->baseContainerImage_)
					 ->maxRowId_;
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


	void getIdList(TransactionContext &txn,
		util::XArray<uint8_t> &serializedRowList, util::XArray<RowId> &idList);
	void lockIdList(TransactionContext &txn, util::XArray<OId> &oIdList,
		util::XArray<RowId> &idList);
	void setDummyMvccImage(TransactionContext &txn);
	void getContainerOptionInfo(
		TransactionContext &txn, util::XArray<uint8_t> &containerSchema);
	void checkContainerOption(MessageSchema *messageSchema,
		util::XArray<uint32_t> &copyColumnMap, bool &isCompletelySameSchema);
	uint32_t calcRowImageSize(uint32_t rowFixedSize) {
		uint32_t rowImageSize_ =
			sizeof(TransactionId) + sizeof(RowId) + rowFixedSize;
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
	void checkExclusive(TransactionContext &txn);

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
		indexSchema_->dropAll(txn, this, UNDEF_CONTAINER_POS, true);
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
