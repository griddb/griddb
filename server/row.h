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
	@brief Implementation of RowArrayImpl and RowArrayImpl::Row
*/
#ifndef ROW_H_
#define ROW_H_

#include "blob_processor.h"
#include "message_row_store.h"
#include "string_array_processor.h"
#include "transaction_manager.h"
/*!
	@brief Move to next Row, and Check if Row exists
*/
inline bool BaseContainer::RowArray::next() {
	return defaultImpl_->next();
}

/*!
	@brief Move to prev Row, and Check if Row exists
*/
inline bool BaseContainer::RowArray::prev() {
	return defaultImpl_->prev();
}

/*!
	@brief Move to first Row, and Check if Row exists
*/
inline bool BaseContainer::RowArray::begin() {
	return defaultImpl_->begin();
}

/*!
	@brief Check if cursor reached to end
*/
inline bool BaseContainer::RowArray::end() {
	return defaultImpl_->end();
}

/*!
	@brief Move to last Row, and Check if Row exists
*/
inline bool BaseContainer::RowArray::tail() {
	return defaultImpl_->tail();
}

/*!
	@brief Check if next Row exists
*/
inline bool BaseContainer::RowArray::hasNext() const {
	return defaultImpl_->hasNext();
}

/*!
	@brief Get Object from Chunk
*/
inline bool BaseContainer::RowArray::load(TransactionContext &txn, OId oId,
	BaseContainer *container, uint8_t getOption) {
	return defaultImpl_->load(txn, oId, container, getOption);
}

/*!
	@brief Get Middle RowId of RowArray
*/
inline RowId BaseContainer::RowArray::getMidRowId() {
	return defaultImpl_->getMidRowId();
}

/*!
	@brief Check if reserved Row area is full to capacity
*/
inline bool BaseContainer::RowArray::isFull() {
	return defaultImpl_->isFull();
}
/*!
	@brief Check if RowArray is initialized
*/
inline bool BaseContainer::RowArray::isNotInitialized() const {
	return defaultImpl_->isNotInitialized();
}
/*!
	@brief Check if cursor is last Row
*/
inline bool BaseContainer::RowArray::isTailPos() const {
	return defaultImpl_->isTailPos();
}

/*!
	@brief Get OId of Row(RowArray OId + row offset)
*/
inline OId BaseContainer::RowArray::getOId() const {
	return defaultImpl_->getOId();
}

/*!
	@brief Get OId of RowArray
*/
inline OId BaseContainer::RowArray::getBaseOId() const {
	return defaultImpl_->getBaseOId();
}

/*!
	@brief Return address of new Row
*/
inline uint8_t *BaseContainer::RowArray::getNewRow() {
	return defaultImpl_->getNewRow();
}

/*!
	@brief Get address of current Row
*/
inline uint8_t *BaseContainer::RowArray::getRow() {
	return defaultImpl_->getRow();
}

/*!
	@brief Get number of Rows(include removed Rows)
*/
inline uint16_t BaseContainer::RowArray::getRowNum() const {
	return defaultImpl_->getRowNum();
}

/*!
	@brief Get number of existed Rows
*/
inline uint16_t BaseContainer::RowArray::getActiveRowNum(uint16_t limit) {
	return defaultImpl_->getActiveRowNum(limit);
}

/*!
	@brief Get maximum that it can store
*/
inline uint16_t BaseContainer::RowArray::getMaxRowNum() const {
	return defaultImpl_->getMaxRowNum();
}

/*!
	@brief Get header size of RowArray Object
*/
inline uint32_t BaseContainer::RowArray::getHeaderSize() const {
	return defaultImpl_->getHeaderSize();
}

/*!
	@brief Set RowId to current Row
*/
inline void BaseContainer::RowArray::setRowId(RowId rowId) {
	defaultImpl_->setRowId(rowId);
}

/*!
	@brief Get RowId of current Row
*/
inline RowId BaseContainer::RowArray::getRowId() const {
	return defaultImpl_->getRowId();
}

inline RowArrayType BaseContainer::RowArray::getRowArrayType() const {
	return defaultImpl_->getRowArrayType();
}

inline RowId BaseContainer::RowArray::getCurrentRowId() {
	return defaultImpl_->getCurrentRowId();
}

template<typename Container>
BaseContainer::RowArray::Column BaseContainer::RowArray::getColumn(const ColumnInfo &info) {
	return BaseContainer::RowArrayImpl<Container, BaseContainer::ROW_ARRAY_GENERAL>::getColumn(info);
}
template<typename Container>
BaseContainer::RowArray::Column BaseContainer::RowArray::getRowIdColumn(const BaseContainer &container) {
	return BaseContainer::RowArrayImpl<Container, BaseContainer::ROW_ARRAY_GENERAL>::getRowIdColumn(container);
}

inline BaseContainer::RowArrayImpl<BaseContainer, BaseContainer::ROW_ARRAY_GENERAL>* BaseContainer::RowArray::getDefaultImpl() {
	return defaultImpl_;
}

inline BaseContainer::RowArrayImpl<BaseContainer, BaseContainer::ROW_ARRAY_GENERAL>* BaseContainer::RowArray::getDefaultImpl() const {
	return defaultImpl_;
}


inline BaseContainer::RowArray::Row::Row(uint8_t *rowImage, RowArray *rowArrayCursor)
	: rowArrayCursor_(rowArrayCursor) {
}

/*!
	@brief Set flag that this Row is already updated in the
   transaction
*/
inline void BaseContainer::RowArray::Row::setFirstUpdate() {
	rowArrayCursor_->getDefaultImpl()->getRowCursor().setFirstUpdate();
}
/*!
	@brief Reset flag that this Row is already updated in the
   transaction
*/
inline void BaseContainer::RowArray::Row::resetFirstUpdate() {
	rowArrayCursor_->getDefaultImpl()->getRowCursor().resetFirstUpdate();
}
/*!
	@brief Check if this Row is already updated in the transaction
*/
inline bool BaseContainer::RowArray::Row::isFirstUpdate() const {
	return rowArrayCursor_->getDefaultImpl()->getRowCursor().isFirstUpdate();
}

/*!
	@brief Set RowId
*/
inline void BaseContainer::RowArray::Row::setRowId(RowId rowId) {
	rowArrayCursor_->getDefaultImpl()->getRowCursor().setRowId(rowId);
}
/*!
	@brief Get TransactionId when finally updated
*/
inline TransactionId BaseContainer::RowArray::Row::getTxnId() const {
	return rowArrayCursor_->getDefaultImpl()->getRowCursor().getTxnId();
}

/*!
	@brief Get RowId
*/
inline RowId BaseContainer::RowArray::Row::getRowId() const {
	return rowArrayCursor_->getDefaultImpl()->getRowCursor().getRowId();
}
/*!
	@brief Check if this Row is already removed
*/

/*!
	@brief Check if this Row is already removed
*/
inline bool BaseContainer::RowArray::Row::isRemoved() const {
	return rowArrayCursor_->getDefaultImpl()->getRowCursor().isRemoved();
}
/*!
	@brief Set removed flag
*/
inline void BaseContainer::RowArray::Row::reset() {
	rowArrayCursor_->getDefaultImpl()->getRowCursor().reset();
}

/*!
	@brief Get address of variable OId
*/
inline uint8_t *BaseContainer::RowArray::Row::getVariableArrayAddr() const {
	return rowArrayCursor_->getDefaultImpl()->getRowCursor().getVariableArrayAddr();
}

inline const uint8_t *BaseContainer::RowArray::Row::getNullsAddr() const {
	return rowArrayCursor_->getDefaultImpl()->getRowCursor().getNullsAddr();
}

/*!
	@brief Check if this Row meet a condition
*/
inline bool BaseContainer::RowArray::Row::isMatch(TransactionContext &txn, TermCondition &cond,
	ContainerValue &tmpValue) {
	return rowArrayCursor_->getDefaultImpl()->getRowCursor().isMatch(txn, cond, tmpValue);
}
inline bool BaseContainer::RowArray::Row::isNullValue(const ColumnInfo &columnInfo) const {
	return rowArrayCursor_->getDefaultImpl()->getRowCursor().isNullValue(columnInfo);
}


inline BaseContainer::RowArray* BaseContainer::RowArray::Row::getRowArray() {
	return rowArrayCursor_;
}


template<typename Container, RowArrayType rowArrayType>
inline BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::Row(uint8_t *rowImage, RowArrayImpl *rowArrayCursor)
	: rowArrayCursor_(rowArrayCursor), binary_(rowImage) {
}

/*!
	@brief Initialize the area in Row
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::initialize() {
	memset(binary_, 0, rowArrayCursor_->getContainer().getRowSize());
	if (rowArrayCursor_->hasVariableColumn()) {
		setVariableArray(UNDEF_OID);
	}
}

/*!
	@brief Free Objects related to Row
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::finalize(TransactionContext &txn) {
	if (rowArrayCursor_->hasVariableColumn()) {
		if (getVariableArray() != UNDEF_OID) {
			BaseObject &frontObject = rowArrayCursor_->rowCache_.getFrontFieldObject();
			frontObject.reset();

			ObjectManager &objectManager =
				*(rowArrayCursor_->getContainer().getObjectManager());
			VariableArrayCursor cursor(
				txn, objectManager, getVariableArray(), OBJECT_FOR_UPDATE);
			for (uint32_t columnId = 0;
				 columnId < rowArrayCursor_->getColumnNum();
				 columnId++) {
				ColumnInfo &columnInfo =
					rowArrayCursor_->getContainer().getColumnInfo(columnId);
				if (columnInfo.isVariable()) {
					bool nextFound = cursor.nextElement();
					if (!nextFound) {
						assert(columnInfo.getColumnOffset() >= rowArrayCursor_->currentParam_.varColumnNum_);
						break;
					}
					if (columnInfo.isSpecialVariable()) {
						uint32_t elemSize;
						uint32_t elemCount;
						uint8_t *elemData = cursor.getElement(
							elemSize, elemCount);  
						switch (columnInfo.getColumnType()) {
						case COLUMN_TYPE_STRING_ARRAY:
							StringArrayProcessor::remove(txn, objectManager,
								columnInfo.getColumnType(), elemData);
							break;
						case COLUMN_TYPE_BLOB:
							BlobProcessor::remove(txn, objectManager,
								columnInfo.getColumnType(), elemData);
							break;
						}
					}
				}
			}
			cursor.finalize();
			setVariableArray(UNDEF_OID);
		}
	}
}

/*!
	@brief Set flag that this Row is already updated in the
   transaction
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::setFirstUpdate() {
	if (rowArrayCursor_->getContainer().getContainerType() == TIME_SERIES_CONTAINER) {
		rowArrayCursor_->setFirstUpdate();
	} else {
		RowHeader *val = getRowHeaderAddr();
		*val |= FIRST_UPDATE_BIT;
	}
}
/*!
	@brief Reset flag that this Row is already updated in the
   transaction
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::resetFirstUpdate() {
	if (rowArrayCursor_->getContainer().getContainerType() == TIME_SERIES_CONTAINER) {
		rowArrayCursor_->resetFirstUpdate();
	} else {
		RowHeader *val = getRowHeaderAddr();
		*val ^= FIRST_UPDATE_BIT;
	}
}
/*!
	@brief Check if this Row is already updated in the transaction
*/
template<typename Container, RowArrayType rowArrayType>
bool BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::isFirstUpdate() const {
	if (rowArrayCursor_->getContainer().getContainerType() == TIME_SERIES_CONTAINER) {
		return rowArrayCursor_->isFirstUpdate();
	} else {
		return (*getRowHeaderAddr() & FIRST_UPDATE_BIT) != 0;
	}
}

/*!
	@brief Set RowId
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::setRowId(RowId rowId) {
	memcpy(getRowIdAddr(), &rowId, sizeof(RowId));
}
/*!
	@brief Get TransactionId when finally updated
*/
template<typename Container, RowArrayType rowArrayType>
TransactionId BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::getTxnId() const {
	TransactionId *tIdAddr;
	if (rowArrayCursor_->getContainer().getContainerType() == TIME_SERIES_CONTAINER) {
		tIdAddr = reinterpret_cast<TransactionId *>(rowArrayCursor_->getTIdAddr());
	} else {
		tIdAddr = reinterpret_cast<TransactionId *>(getTIdAddr());
	}
	return (*tIdAddr & TID_FIELD);
}

/*!
	@brief Get RowId
*/
template<typename Container, RowArrayType rowArrayType>
inline RowId BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::getRowId() const {
	return *reinterpret_cast<RowId *>(getRowIdAddr());
}

/*!
	@brief Check if this Row is already removed
*/
template<typename Container, RowArrayType rowArrayType>
inline bool BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::isRemoved() const {
	return (*getRowHeaderAddr() & REMOVE_BIT) != 0;
}
/*!
	@brief Set removed flag
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::reset() {
	setRemoved();
}

/*!
	@brief Get address of variable OId
*/
template<typename Container, RowArrayType rowArrayType>
uint8_t *BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::getVariableArrayAddr() const {
	return getFixedAddr();
}

template<typename Container, RowArrayType rowArrayType>
const uint8_t *BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::getNullsAddr() const {
	return binary_ + rowArrayCursor_->currentParam_.nullsOffset_;
}

/*!
	@brief Check if this Row meet a condition
*/
template<typename Container, RowArrayType rowArrayType>
bool BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::isMatch(TransactionContext &txn, TermCondition &cond,
	ContainerValue &tmpValue) {
	bool isMatch = false;
	if (cond.columnId_ == UNDEF_COLUMNID) {
		RowId rowId = getRowId();
		isMatch =
			cond.operator_(txn, reinterpret_cast<uint8_t *>(&rowId),
				sizeof(RowId), cond.value_, cond.valueSize_);
	}
	else {
		ColumnInfo &columnInfo = rowArrayCursor_->getContainer().getColumnInfo(cond.columnId_);
		if (isNullValue(columnInfo)) {
			isMatch = cond.operator_ == ComparatorTable::isNull_;
		} else {
			getField(txn, columnInfo, tmpValue);
			isMatch = cond.operator_(txn, tmpValue.getValue().data(),
				tmpValue.getValue().size(), cond.value_,
				cond.valueSize_);
		}
	}
	return isMatch;
}

template<typename Container, RowArrayType rowArrayType>
bool BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::isNullValue(const ColumnInfo &columnInfo) const {
	if (util::IsSame<RowArrayAccessType<rowArrayType>, RowArrayGeneralType>::VALUE) {
		if (rowArrayCursor_->isNotExistColumn(columnInfo)) {
			if (columnInfo.isNotNull()) {
				return false;
			} else {
				return true;
			}
		}
	}
	return RowNullBits::isNullValue(getNullsAddr(), columnInfo.getColumnId());
}


template<typename Container, RowArrayType rowArrayType>
inline RowHeader *BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::getRowHeaderAddr() const {
	return getAddr() + rowArrayCursor_->currentParam_.rowHeaderOffset_;
}

template<typename Container, RowArrayType rowArrayType>
inline uint8_t *BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::getRowIdAddr() const {
	return binary_ + rowArrayCursor_->currentParam_.rowIdOffset_;
}

template<typename Container, RowArrayType rowArrayType>
inline uint8_t *BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::getTIdAddr() const {
	return binary_ + COL_TID_OFFSET;
}
template<typename Container, RowArrayType rowArrayType>
inline uint8_t *BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::getFixedAddr() const {
	return binary_ + rowArrayCursor_->currentParam_.rowDataOffset_;
}

template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::setVariableArray(OId oId) {
	memcpy(getVariableArrayAddr(), &oId, sizeof(OId));
}

template<typename Container, RowArrayType rowArrayType>
OId BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::getVariableArray() const {
	return *reinterpret_cast<OId *>(getVariableArrayAddr());
}

template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::setRemoved() {
	RowHeader *val = getRowHeaderAddr();
	*val |= REMOVE_BIT;
}

template<typename Container, RowArrayType rowArrayType>
uint8_t *BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::getAddr() const {
	return binary_;
}


template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::setLockTId(TransactionId tId) {
	TransactionId *targetAddr =
		reinterpret_cast<TransactionId *>(getTIdAddr());
	TransactionId filterTId = (tId & Row::TID_FIELD);
	TransactionId header = ((*targetAddr) & Row::BITS_FIELD);
	*targetAddr = (header | filterTId);
	setFirstUpdate();
}

/*!
	@brief Calculate Object's size of variable field values
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::checkVarDataSize(TransactionContext &txn,
	const util::XArray< std::pair<uint8_t *, uint32_t> > &varList,
	const util::XArray<uint32_t> &varColumnIdList,
	bool isConvertSpecialType,
	util::XArray<uint32_t> &varDataObjectSizeList,
	util::XArray<uint32_t> &varDataObjectPosList) {
	ObjectManager &objectManager =
		*(rowArrayCursor_->getContainer().getObjectManager());
	uint32_t variableColumnNum = varList.size();

	uint32_t currentObjectSize = ValueProcessor::getEncodedVarSize(
		variableColumnNum);  
	varDataObjectSizeList.push_back(currentObjectSize);  
	uint32_t varColumnObjectCount = 0;
	util::XArray<uint32_t> accumulateSizeList(txn.getDefaultAllocator());
	for (uint32_t elemNth = 0; elemNth < varList.size(); elemNth++) {
		ColumnInfo &columnInfo =
			rowArrayCursor_->getContainer().getColumnInfo(varColumnIdList[elemNth]);
		uint32_t elemSize = varList[elemNth].second;
		if (isConvertSpecialType) {
			if (columnInfo.getColumnType() == COLUMN_TYPE_STRING_ARRAY) {
				elemSize = LINK_VARIABLE_COLUMN_DATA_SIZE;
			} else if (columnInfo.getColumnType() == COLUMN_TYPE_BLOB) {
				elemSize = BlobCursor::getPrefixDataSize(objectManager, elemSize);
			}
		}
		for (size_t checkCount = 0; !accumulateSizeList.empty() && 
			(currentObjectSize + elemSize +
				ValueProcessor::getEncodedVarSize(elemSize) +
				NEXT_OBJECT_LINK_INFO_SIZE) >
			static_cast<uint32_t>(objectManager.getRecommendtLimitObjectSize());
			checkCount++) {
			uint32_t dividedObjectSize = currentObjectSize;
			uint32_t dividedElemNth = elemNth - 1;
			Size_t estimateAllocateSize =
				objectManager.estimateAllocateSize(currentObjectSize) + NEXT_OBJECT_LINK_INFO_SIZE;
			if (checkCount == 0 && VariableArrayCursor::divisionThreshold(currentObjectSize) < estimateAllocateSize) {
				for (size_t i = 0; i < accumulateSizeList.size(); i++) {
					uint32_t accumulateSize = accumulateSizeList[accumulateSizeList.size() - 1 - i];
					if (accumulateSize == currentObjectSize) {
						continue;
					}
					Size_t estimateAllocateSizeFront =
						objectManager.estimateAllocateSize(accumulateSize + NEXT_OBJECT_LINK_INFO_SIZE) + ObjectAllocator::BLOCK_HEADER_SIZE;
					Size_t estimateAllocateSizeBack =
						objectManager.estimateAllocateSize(currentObjectSize - accumulateSize);
					if (estimateAllocateSizeFront + estimateAllocateSizeBack < estimateAllocateSize && 
						(VariableArrayCursor::divisionThreshold(accumulateSize + ObjectAllocator::BLOCK_HEADER_SIZE) >= estimateAllocateSizeFront)) {
						dividedObjectSize = accumulateSize;
						dividedElemNth -= static_cast<uint32_t>(i);
						break;
					}
				}
			}
			varDataObjectPosList.push_back(dividedElemNth);
			varDataObjectSizeList[varColumnObjectCount] = dividedObjectSize + NEXT_OBJECT_LINK_INFO_SIZE;
			++varColumnObjectCount;
			currentObjectSize = currentObjectSize - dividedObjectSize;	 
			varDataObjectSizeList.push_back(currentObjectSize);  
			accumulateSizeList.erase(accumulateSizeList.begin(), accumulateSizeList.end() - (elemNth - 1 - dividedElemNth));
		}
		currentObjectSize +=
			elemSize + ValueProcessor::getEncodedVarSize(elemSize);
		accumulateSizeList.push_back(currentObjectSize);
	}

	Size_t estimateAllocateSize =
		objectManager.estimateAllocateSize(currentObjectSize);
	if (VariableArrayCursor::divisionThreshold(currentObjectSize) < estimateAllocateSize) {
		uint32_t dividedObjectSize = currentObjectSize;
		uint32_t dividedElemNth = variableColumnNum - 1;
		for (size_t i = 0; i < accumulateSizeList.size(); i++) {
			uint32_t accumulateSize = accumulateSizeList[accumulateSizeList.size() - 1 - i];
			if (accumulateSize == currentObjectSize) {
				continue;
			}
			Size_t estimateAllocateSizeFront =
				objectManager.estimateAllocateSize(accumulateSize + NEXT_OBJECT_LINK_INFO_SIZE) + ObjectAllocator::BLOCK_HEADER_SIZE;
			Size_t estimateAllocateSizeBack =
				objectManager.estimateAllocateSize(currentObjectSize - accumulateSize);
			if (estimateAllocateSizeFront + estimateAllocateSizeBack < estimateAllocateSize && 
				(VariableArrayCursor::divisionThreshold(accumulateSize + ObjectAllocator::BLOCK_HEADER_SIZE) >= estimateAllocateSizeFront)) {
				dividedObjectSize = accumulateSize;
				dividedElemNth -= static_cast<uint32_t>(i);
				break;
			}
		}
		if (dividedObjectSize != currentObjectSize) {
			varDataObjectPosList.push_back(dividedElemNth);
			varDataObjectSizeList[varColumnObjectCount] = dividedObjectSize + NEXT_OBJECT_LINK_INFO_SIZE;
			++varColumnObjectCount;
			currentObjectSize = currentObjectSize - dividedObjectSize;
			varDataObjectSizeList.push_back(currentObjectSize);  
		}
	}
	varDataObjectSizeList[varColumnObjectCount] = currentObjectSize;
	varDataObjectPosList.push_back(
		static_cast<uint32_t>(variableColumnNum - 1));
}

template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::setVariableFields(TransactionContext &txn,
	const util::XArray< std::pair<uint8_t *, uint32_t> > &varList,
	const util::XArray<uint32_t> &varColumnIdList,
	bool isConvertSpecialType,
	const util::XArray<uint32_t> &varDataObjectSizeList,
	const util::XArray<uint32_t> &varDataObjectPosList,
	const util::XArray<OId> &oldVarDataOIdList) {

	ObjectManager &objectManager =
		*(rowArrayCursor_->getContainer().getObjectManager());
	const AllocateStrategy allocateStrategy =
		rowArrayCursor_->getContainer().getRowAllcateStrategy();
	const uint32_t variableColumnNum =
		rowArrayCursor_->getContainer().getVariableColumnNum();

	uint32_t elemNth = 0;
	uint8_t *destAddr = NULL;
	OId variableOId = UNDEF_OID;
	OId oldVarDataOId = UNDEF_OID;
	BaseObject oldVarObj(txn.getPartitionId(), objectManager);
	Size_t oldVarObjSize = 0;
	uint8_t *nextLinkAddr = NULL;
	OId neighborOId = rowArrayCursor_->getBaseOId();
	for (size_t i = 0; i < varDataObjectSizeList.size(); ++i) {
		if (i < oldVarDataOIdList.size()) {
			oldVarDataOId = oldVarDataOIdList[i];
			oldVarObj.load(oldVarDataOId, OBJECT_FOR_UPDATE);
			oldVarObjSize =
				objectManager.getSize(oldVarObj.getBaseAddr());
		}
		else {
			oldVarDataOId = UNDEF_OID;
			oldVarObjSize = 0;
		}
		if (oldVarObjSize >= varDataObjectSizeList[i]) {
			destAddr = oldVarObj.getBaseAddr();
			variableOId = oldVarDataOId;
		}
		else {
			if (UNDEF_OID != oldVarDataOId) {
				oldVarObj.finalize();
			}
			destAddr = oldVarObj.allocateNeighbor<uint8_t>(
				varDataObjectSizeList[i], allocateStrategy, variableOId,
				neighborOId,
				OBJECT_TYPE_ROW);  
			neighborOId = variableOId;
			assert(destAddr);
		}
		if (i == 0) {
			setVariableArray(variableOId);
			uint64_t encodedVariableColumnNum =
				ValueProcessor::encodeVarSize(variableColumnNum);
			uint32_t encodedVariableColumnNumLen =
				ValueProcessor::getEncodedVarSize(variableColumnNum);
			memcpy(destAddr, &encodedVariableColumnNum,
				encodedVariableColumnNumLen);
			destAddr += encodedVariableColumnNumLen;
		}
		else {
			assert(nextLinkAddr);
			uint64_t encodedOId =
				ValueProcessor::encodeVarSizeOId(variableOId);
			memcpy(nextLinkAddr, &encodedOId, sizeof(uint64_t));
			nextLinkAddr = NULL;
		}
		for (; elemNth < varList.size(); elemNth++) {
			uint32_t elemSize = varList[elemNth].second;
			uint8_t *data = varList[elemNth].first;
			uint32_t headerSize =
				ValueProcessor::getEncodedVarSize(elemSize);
			ColumnInfo &columnInfo =
				rowArrayCursor_->getContainer().getColumnInfo(
				varColumnIdList[elemNth]);
			if (isConvertSpecialType && columnInfo.getColumnType() == COLUMN_TYPE_STRING_ARRAY) {				
				uint32_t linkHeaderValue =
					ValueProcessor::encodeVarSize(
						LINK_VARIABLE_COLUMN_DATA_SIZE);
				uint32_t linkHeaderSize =
					ValueProcessor::getEncodedVarSize(
						LINK_VARIABLE_COLUMN_DATA_SIZE);
				memcpy(destAddr, &linkHeaderValue, linkHeaderSize);
				destAddr += linkHeaderSize;
				memcpy(destAddr, &elemSize,
					sizeof(uint32_t));  
				destAddr += sizeof(uint32_t);
				OId linkOId = UNDEF_OID;
				if (elemSize > 0) {
					linkOId = StringArrayProcessor::putToObject(txn,
						objectManager, data, elemSize,
						allocateStrategy,
						variableOId);  
				}
				memcpy(destAddr, &linkOId, sizeof(OId));
				destAddr += sizeof(OId);
			} else if (isConvertSpecialType && columnInfo.getColumnType() == COLUMN_TYPE_BLOB) {
				uint32_t destSize;
				BlobProcessor::setField(txn, objectManager,
					data, elemSize,
					destAddr, destSize,
					allocateStrategy, variableOId);
				destAddr += destSize;
			} else {
				memcpy(destAddr, data, headerSize + elemSize);
				destAddr += headerSize + elemSize;
			}
			nextLinkAddr = destAddr;
			if (elemNth == varDataObjectPosList[i]) {
				elemNth++;
				break;
			}
		}
	}
	for (size_t i = varDataObjectSizeList.size();
		 i < oldVarDataOIdList.size(); ++i) {
		assert(UNDEF_OID != oldVarDataOIdList[i]);
		if (UNDEF_OID != oldVarDataOIdList[i]) {
			objectManager.free(txn.getPartitionId(), oldVarDataOIdList[i]);
		}
	}
}

/*!
	@brief Set field values to RowArrayImpl Object
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::setFields(
	TransactionContext &txn, MessageRowStore *messageRowStore) {
//	ObjectManager &objectManager =
//		*(rowArrayCursor_->getContainer().getObjectManager());
	util::StackAllocator &alloc = txn.getDefaultAllocator();
	const void *source;
	uint32_t size;
	messageRowStore->getRowFixedPart(source, size);

	uint8_t *dest = getFixedAddr();

	memcpy(dest, source, size);

	const uint32_t variableColumnNum =
		rowArrayCursor_->getContainer().getVariableColumnNum();
	if (variableColumnNum > 0) {
		setVariableArray(UNDEF_OID);
		util::XArray<uint32_t> varColumnIdList(alloc);
		varColumnIdList.reserve(variableColumnNum);
		for (uint32_t columnId = 0; columnId < rowArrayCursor_->getColumnNum(); columnId++) {
			ColumnInfo &columnInfo =
				rowArrayCursor_->getContainer().getColumnInfo(columnId);
			if (columnInfo.isVariable()) {
				varColumnIdList.push_back(columnId);
			}
		}
		util::XArray< std::pair<uint8_t *, uint32_t> > varList(alloc);
		VariableArrayCursor variableArrayCursor(messageRowStore->getRowVariablePart());
		while (variableArrayCursor.nextElement()) {
			uint32_t elemSize;
			uint32_t elemNth;
			uint8_t *data =
				variableArrayCursor.getElement(elemSize, elemNth);
			varList.push_back(std::make_pair(data, elemSize));
		}

		bool isConvertSpecialType = true;
		util::XArray<uint32_t> varDataObjectSizeList(
			alloc);  
		util::XArray<uint32_t> varDataObjectPosList(
			alloc);  
		checkVarDataSize(txn, varList, varColumnIdList,
			isConvertSpecialType, varDataObjectSizeList,
			varDataObjectPosList);
		util::XArray<OId> oldVarDataOIdList(alloc);
		setVariableFields(txn, varList, varColumnIdList,
			isConvertSpecialType, varDataObjectSizeList,
			varDataObjectPosList, oldVarDataOIdList);

	}
}

/*!
	@brief Updates field values on RowArrayImpl Object
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::updateFields(
	TransactionContext &txn, MessageRowStore *messageRowStore) {
	ObjectManager &objectManager =
		*(rowArrayCursor_->getContainer().getObjectManager());
	util::StackAllocator &alloc = txn.getDefaultAllocator();
	const uint8_t *source;
	uint32_t size;
	messageRowStore->getRowFixedPart(source, size);

	uint8_t *dest = getFixedAddr();
	OId oldVarDataOId = UNDEF_OID;
	if (rowArrayCursor_->hasVariableColumn()) {
		BaseObject &frontObject = rowArrayCursor_->rowCache_.getFrontFieldObject();
		frontObject.reset();
		oldVarDataOId = getVariableArray();
	}
	memcpy(dest, source, size);

	const uint32_t variableColumnNum =
		rowArrayCursor_->getContainer().getVariableColumnNum();
	if (variableColumnNum > 0) {
//		const AllocateStrategy allocateStrategy =
//			rowArrayCursor_->getContainer().getRowAllcateStrategy();

		setVariableArray(UNDEF_OID);
		util::XArray<uint32_t> varColumnIdList(alloc);
		varColumnIdList.reserve(variableColumnNum);
		for (uint32_t columnId = 0; columnId < rowArrayCursor_->getColumnNum(); columnId++) {
			ColumnInfo &columnInfo =
				rowArrayCursor_->getContainer().getColumnInfo(columnId);
			if (columnInfo.isVariable()) {
				varColumnIdList.push_back(columnId);
			}
		}
		util::XArray< std::pair<uint8_t *, uint32_t> > varList(alloc);
		VariableArrayCursor variableArrayCursor(messageRowStore->getRowVariablePart());
		while (variableArrayCursor.nextElement()) {
			uint32_t elemSize;
			uint32_t elemNth;
			uint8_t *data =
				variableArrayCursor.getElement(elemSize, elemNth);
			varList.push_back(std::make_pair(data, elemSize));
		}

		bool isConvertSpecialType = true;
		util::XArray<uint32_t> varDataObjectSizeList(
			alloc);  
		util::XArray<uint32_t> varDataObjectPosList(
			alloc);  
		checkVarDataSize(txn, varList, varColumnIdList,
			isConvertSpecialType, varDataObjectSizeList,
			varDataObjectPosList);

		util::XArray<OId> oldVarDataOIdList(alloc);
		{
			assert(oldVarDataOId != UNDEF_OID);
			VariableArrayCursor srcArrayCursor(
				txn, objectManager, oldVarDataOId, OBJECT_FOR_UPDATE);
			OId prevOId = UNDEF_OID;
			for (uint32_t columnId = 0;
				 columnId < rowArrayCursor_->getContainer().getColumnNum();
				 columnId++) {
				ColumnInfo &columnInfo =
					rowArrayCursor_->getContainer().getColumnInfo(columnId);
				if (columnInfo.isVariable()) {
					bool srcNext = srcArrayCursor.nextElement();
					UNUSED_VARIABLE(srcNext);
					assert(srcNext);
					if (prevOId != srcArrayCursor.getElementOId()) {
						oldVarDataOIdList.push_back(
							srcArrayCursor.getElementOId());
						prevOId = srcArrayCursor.getElementOId();
					}
					if (columnInfo.isSpecialVariable()) {
						uint32_t elemSize;
						uint32_t elemNth;
						uint8_t *data =
							srcArrayCursor.getElement(elemSize, elemNth);
						switch (columnInfo.getColumnType()) {
						case COLUMN_TYPE_STRING_ARRAY:
							StringArrayProcessor::remove(txn, objectManager,
								columnInfo.getColumnType(), data);
							break;
						case COLUMN_TYPE_BLOB:
							BlobProcessor::remove(txn, objectManager,
								columnInfo.getColumnType(), data);
							break;
						default:
							GS_THROW_USER_ERROR(GS_ERROR_CM_NOT_SUPPORTED,
								"unknown columnType:"
									<< (int32_t)columnInfo.getColumnType());
						}
					}
				}
			}
		}

		setVariableFields(txn, varList, varColumnIdList,
			isConvertSpecialType, varDataObjectSizeList,
			varDataObjectPosList, oldVarDataOIdList);

	}
}

/*!
	@brief Get field value
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::getField(TransactionContext &txn,
	const ColumnInfo &columnInfo, BaseObject &baseObject) {
	assert(!isNullValue(columnInfo));
	if (ValueProcessor::isSimple(columnInfo.getColumnType())) {
		if (rowArrayCursor_->isNotExistColumn(columnInfo)) {
			void *valueAddr = const_cast<void *>(Value::getDefaultFixedValue(columnInfo.getColumnType()));
			baseObject.setBaseAddr(reinterpret_cast<uint8_t *>(valueAddr));
		} else {
			baseObject.copyReference(this->rowArrayCursor_->getBaseOId(),
				this->getFixedAddr() - rowArrayCursor_->currentParam_.columnOffsetDiff_ + columnInfo.getColumnOffset());
		}
	}
	else {
		OId variableOId = this->getVariableArray();
		if (rowArrayCursor_->isNotExistColumn(columnInfo) || variableOId == UNDEF_OID) {
			void *valueAddr = const_cast<void *>(Value::getDefaultVariableValue(columnInfo.getColumnType()));
			baseObject.setBaseAddr(reinterpret_cast<uint8_t *>(valueAddr));
		} else {
			ObjectManager &objectManager =
				*(rowArrayCursor_->getContainer().getObjectManager());
			VariableArrayCursor variableArrayCursor(
				txn, objectManager, variableOId, OBJECT_READ_ONLY);
			if (columnInfo.getColumnOffset() < variableArrayCursor.getArrayLength()) {
				variableArrayCursor.getField(columnInfo, baseObject);
			} else {
				void *valueAddr = const_cast<void *>(Value::getDefaultVariableValue(columnInfo.getColumnType()));
				baseObject.setBaseAddr(reinterpret_cast<uint8_t *>(valueAddr));
			}
		}
	}
}

/*!
	@brief Get field value
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::getField(TransactionContext &txn,
	const ColumnInfo &columnInfo, ContainerValue &containerValue) {
	if (isNullValue(columnInfo)) {
		containerValue.setNull();
	} else {
		getField(txn, columnInfo, containerValue.getBaseObject());
		containerValue.set(containerValue.getBaseObject().getCursor<uint8_t>(),
			columnInfo.getColumnType());
	}
}

/*!
	@brief Get field value
*/
template<typename Container, RowArrayType rowArrayType>
const void * BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::getField(
	const BaseContainer::RowArray::Column &column) {
	ColumnInfo &columnInfo = column.getColumnInfo();
	if (ValueProcessor::isSimple(columnInfo.getColumnType())) {
		return getFixedField(column);
	}
	else {
		return getVariableField(column);
	}
}

template<typename Container, RowArrayType rowArrayType>
const void *BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::getFixedField(const BaseContainer::RowArray::Column &column) {
	if (util::IsSame<RowArrayAccessType<rowArrayType>, RowArrayGeneralType>::VALUE) {
		const ColumnInfo &columnInfo = column.getColumnInfo();
		if (columnInfo.getColumnId() == std::numeric_limits<uint16_t>::max()) {
			assert(getRowId() == *getRowIdAddr());
			return reinterpret_cast<const void *>(getRowIdAddr());
		}
		if (rowArrayCursor_->isNotExistColumn(columnInfo)) {
			return Value::getDefaultFixedValue(columnInfo.getColumnType());
		}
		return getFixedAddr() - rowArrayCursor_->currentParam_.columnOffsetDiff_ + column.getColumnInfo().getColumnOffset();
	}
	assert((column.getColumnInfo().getColumnId() == std::numeric_limits<uint16_t>::max()) ? 
		(column.getFixedOffset() == rowArrayCursor_->currentParam_.rowIdOffset_) :
		column.getFixedOffset() == (rowArrayCursor_->currentParam_.rowDataOffset_ - rowArrayCursor_->currentParam_.columnOffsetDiff_ + column.getColumnInfo().getColumnOffset()));
	return binary_ + column.getFixedOffset();
}

template<typename Container, RowArrayType rowArrayType>
const void * BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::getVariableField(
	const BaseContainer::RowArray::Column &column) {
	const ColumnInfo &columnInfo = column.getColumnInfo();
	const OId baseOId = getVariableArray();
	uint32_t varHeaderSize = rowArrayCursor_->currentParam_.varHeaderSize_;
	if (util::IsSame<RowArrayAccessType<rowArrayType>, RowArrayGeneralType>::VALUE) {
		if (rowArrayCursor_->isNotExistColumn(columnInfo) || baseOId == UNDEF_OID) {
			return Value::getDefaultVariableValue(columnInfo.getColumnType());
		}
	}
	BaseObject &frontObject = rowArrayCursor_->rowCache_.getFrontFieldObject();
	do {
		if (baseOId == frontObject.getBaseOId()) {
			break;
		}

		rowArrayCursor_->rowCache_.lastCachedField_ = 0;
		frontObject.loadNeighbor(baseOId, OBJECT_READ_ONLY);
	}
	while (false);

	if (util::IsSame<RowArrayAccessType<rowArrayType>, RowArrayGeneralType>::VALUE) {
		varHeaderSize = ValueProcessor::getEncodedVarSize(frontObject.getBaseAddr());
		uint32_t elemNum = ValueProcessor::decodeVarSize(frontObject.getBaseAddr());
		if (columnInfo.getColumnOffset() >= elemNum) {
			return Value::getDefaultVariableValue(columnInfo.getColumnType());
		}
	}
	assert(ValueProcessor::getEncodedVarSize(frontObject.getBaseAddr()) ==
			varHeaderSize);

	const uint8_t *addr = frontObject.getBaseAddr() + varHeaderSize;

	if (columnInfo.getColumnOffset() > 0) {
		typedef util::XArray<BaseContainer::RowCache::FieldCache> FieldCacheList;
		typedef FieldCacheList::iterator Iterator;
		Iterator it = rowArrayCursor_->rowCache_.fieldCacheList_.begin() + rowArrayCursor_->rowCache_.lastCachedField_;
		Iterator tail = rowArrayCursor_->rowCache_.fieldCacheList_.begin() +
				columnInfo.getColumnOffset();

		if (it < tail) {
			if (it != rowArrayCursor_->rowCache_.fieldCacheList_.begin()) {
				addr = it->addr_;
			}
			do {
				++it;

				uint8_t val1byte = *reinterpret_cast<const uint8_t *>(addr);
				if (ValueProcessor::varSizeIs4Byte(val1byte)) {
					uint32_t val4byte = *reinterpret_cast<const uint32_t *>(addr);
					addr += 4 + ValueProcessor::decode4ByteVarSize(val4byte);
				}
				else {
					addr += 1 + ValueProcessor::decode1ByteVarSize(val1byte);
				}

				val1byte = *reinterpret_cast<const uint8_t *>(addr);
				if (ValueProcessor::varSizeIs8Byte(val1byte)) {
					uint64_t val8byte = *reinterpret_cast<const uint64_t *>(addr);
					it->baseObject_.loadNeighbor(
							ValueProcessor::decodeVarSize64(addr), OBJECT_READ_ONLY);
					addr = it->baseObject_.getBaseAddr();
				}

				it->addr_ = addr;
			}
			while (it != tail);
			rowArrayCursor_->rowCache_.lastCachedField_ = columnInfo.getColumnOffset();
		}

		return tail->addr_;
	}

	return addr;
}


/*!
	@brief Delete this Row
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::remove(TransactionContext &txn) {
	finalize(txn);
	setRemoved();
}

/*!
	@brief Move this Row to another RowArrayImpl
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::move(
	TransactionContext &txn, Row &dest) {
	assert(dest.rowArrayCursor_->isLatestSchema());
	memcpy(dest.getAddr(), getAddr(), rowArrayCursor_->latestParam_.rowSize_);
	setVariableArray(UNDEF_OID);
	remove(txn);
}

/*!
	@brief Convert this Row to another latset schemaRowArrayImpl
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::convert(
	TransactionContext &txn, Row &dest) {
	assert(!rowArrayCursor_->isLatestSchema());
	assert(dest.rowArrayCursor_->isLatestSchema());

	memset(dest.getAddr(), 0, rowArrayCursor_->latestParam_.rowSize_);
	memcpy(dest.getAddr(), getAddr(), rowArrayCursor_->currentParam_.nullsOffset_);
	memcpy(dest.getAddr() + rowArrayCursor_->latestParam_.nullsOffset_,
		getAddr() + rowArrayCursor_->currentParam_.nullsOffset_,
		rowArrayCursor_->currentParam_.nullbitsSize_);

	for (ColumnId columnId = rowArrayCursor_->currentParam_.columnNum_; 
		columnId < rowArrayCursor_->latestParam_.columnNum_; columnId++) {
		ColumnInfo &columnInfo =
			rowArrayCursor_->getContainer().getColumnInfo(columnId);
		if (!columnInfo.isNotNull()) {
			RowNullBits::setNull(dest.getAddr() + rowArrayCursor_->latestParam_.nullsOffset_, columnId);
		}
	}

	memcpy(dest.getAddr() + rowArrayCursor_->latestParam_.nullsOffset_ + rowArrayCursor_->latestParam_.nullbitsSize_,
		getAddr() + rowArrayCursor_->currentParam_.nullsOffset_ + rowArrayCursor_->currentParam_.nullbitsSize_,
		rowArrayCursor_->currentParam_.rowFixedColumnSize_);
	if (dest.rowArrayCursor_->hasVariableColumn()) {
		if (rowArrayCursor_->getVarColumnNum() < rowArrayCursor_->latestParam_.varColumnNum_) {
			ObjectManager &objectManager =
				*(rowArrayCursor_->getContainer().getObjectManager());
			util::StackAllocator &alloc = txn.getDefaultAllocator();
			util::StackAllocator::Scope scope(alloc);

			util::XArray<uint32_t> varColumnIdList(alloc);
			varColumnIdList.reserve(rowArrayCursor_->latestParam_.varColumnNum_);
			for (uint32_t columnId = 0; columnId < rowArrayCursor_->latestParam_.columnNum_; columnId++) {
				ColumnInfo &columnInfo =
					rowArrayCursor_->getContainer().getColumnInfo(columnId);
				if (columnInfo.isVariable()) {
					varColumnIdList.push_back(columnId);
				}
			}

			util::XArray< std::pair<uint8_t *, uint32_t> > varList(alloc);
			util::XArray<OId> oldVarDataOIdList(alloc);
			OId dummyVarOId = UNDEF_OID;
			if (rowArrayCursor_->hasVariableColumn()) {
				dummyVarOId = getVariableArray();
			}
			if (dummyVarOId != UNDEF_OID) {
				OId prevOId = UNDEF_OID;
				VariableArrayCursor variableArrayCursor(txn, objectManager, dummyVarOId, OBJECT_FOR_UPDATE);
				while (variableArrayCursor.nextElement()) {
					uint32_t elemSize;
					uint32_t elemNth;
					uint8_t *data =
						variableArrayCursor.getElement(elemSize, elemNth);

					uint8_t *swap = static_cast<uint8_t *>(alloc.allocate(elemSize + ValueProcessor::getEncodedVarSize(elemSize)));
					memcpy(swap, data, elemSize + ValueProcessor::getEncodedVarSize(elemSize));
					varList.push_back(std::make_pair(swap, elemSize));

					OId currentOId = variableArrayCursor.getElementOId();
					if (prevOId != currentOId) {
						oldVarDataOIdList.push_back(currentOId);
						prevOId = currentOId;
					}
				}
			}
			for (uint32_t nth = rowArrayCursor_->getVarColumnNum(); nth < rowArrayCursor_->latestParam_.varColumnNum_; nth++) {
				ColumnInfo &columnInfo =
					rowArrayCursor_->getContainer().getColumnInfo(varColumnIdList[nth]);
				void *dummy = const_cast<void *>(Value::getDefaultVariableValue(columnInfo.getColumnType()));
				uint32_t elemSize = ValueProcessor::decodeVarSize(dummy);
				varList.push_back(std::make_pair(static_cast<uint8_t *>(dummy), elemSize));
			}
			bool isConvertSpecialType = false;
			util::XArray<uint32_t> varDataObjectSizeList(
				alloc);  
			util::XArray<uint32_t> varDataObjectPosList(
				alloc);  
			dest.checkVarDataSize(txn, varList, varColumnIdList,
				isConvertSpecialType, varDataObjectSizeList,
				varDataObjectPosList);
			dest.setVariableFields(txn, varList, varColumnIdList,
				isConvertSpecialType, varDataObjectSizeList,
				varDataObjectPosList, oldVarDataOIdList);

		} else {
			dest.setVariableArray(getVariableArray());
		}
	}
	setVariableArray(UNDEF_OID);
	remove(txn);
}

/*!
	@brief Copy this Row to another RowArrayImpl
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::copy(
	TransactionContext &txn, Row &dest) {
	assert(dest.rowArrayCursor_->isLatestSchema());
	memcpy(dest.getAddr(), getAddr(), rowArrayCursor_->latestParam_.rowSize_);
	if (rowArrayCursor_->hasVariableColumn() && getVariableArray() != UNDEF_OID) {
		ObjectManager &objectManager =
			*(rowArrayCursor_->getContainer().getObjectManager());
		const AllocateStrategy &allocateStrategy =
			rowArrayCursor_->getContainer().getRowAllcateStrategy();
		OId srcTopOId = getVariableArray();

		VariableArrayCursor srcCursor(txn, objectManager, srcTopOId, OBJECT_READ_ONLY);
		OId destTopOId = srcCursor.clone(txn, allocateStrategy,
			dest.rowArrayCursor_->getBaseOId());  
		dest.setVariableArray(destTopOId);

		VariableArrayCursor destCursor(txn, objectManager, destTopOId, OBJECT_FOR_UPDATE);
		srcCursor.reset();
		for (uint32_t columnId = 0;
			 columnId < rowArrayCursor_->getContainer().getColumnNum();
			 ++columnId) {
			ColumnInfo &columnInfo =
				rowArrayCursor_->getContainer().getColumnInfo(columnId);
			if (columnInfo.isVariable()) {
				bool exist = destCursor.nextElement();
				if (!exist) {
					break;
				}
				srcCursor.nextElement();
				if (columnInfo.isSpecialVariable()) {
					uint32_t destElemSize, srcElemSize;
					uint32_t destElemCount, srcElemCount;
					uint8_t *srcElem = srcCursor.getElement(srcElemSize, srcElemCount);
					uint8_t *destElem = destCursor.getElement(destElemSize, destElemCount);
					switch (columnInfo.getColumnType()) {
					case COLUMN_TYPE_STRING_ARRAY:
						StringArrayProcessor::clone(txn, objectManager,
							columnInfo.getColumnType(), srcElem, destElem,
							allocateStrategy,
							destTopOId);  
						break;
					case COLUMN_TYPE_BLOB:
						BlobProcessor::clone(txn, objectManager,
							columnInfo.getColumnType(), srcElem, destElem,
							allocateStrategy,
							destTopOId);  
						break;
					default:
						GS_THROW_USER_ERROR(GS_ERROR_CM_NOT_SUPPORTED,
							"unknown columnType:"
								<< (int32_t)columnInfo.getColumnType());
					}
				}
			}
		}
	}
}

/*!
	@brief Lock this Row
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::lock(TransactionContext &txn) {

	if (rowArrayCursor_->getContainer().getContainerType() == TIME_SERIES_CONTAINER) {
		rowArrayCursor_->lock(txn);
	} else {
		if (getTxnId() == txn.getId()) {
		}
		else if (txn.getManager().isActiveTransaction(
					 txn.getPartitionId(), getTxnId())) {
			DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_COL_LOCK_CONFLICT,
				"(pId=" << txn.getPartitionId() << ", rowTxnId=" << getTxnId()
						<< ", txnId=" << txn.getId() << ")");
		}
		else {
			setLockTId(txn.getId());
		}
	}
}

/*!
	@brief translate into Message format
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::getImage(TransactionContext &txn,
	MessageRowStore *messageRowStore, bool isWithRowId) {
	ObjectManager &objectManager =
		*(rowArrayCursor_->getContainer().getObjectManager());
	if (isWithRowId) {
		messageRowStore->setRowId(getRowId());
	}
	{
		uint8_t *fixedAddr = getFixedAddr();
		uint32_t fixedSize = static_cast<uint32_t>(
			rowArrayCursor_->getContainer().getRowFixedDataSize());
		if (!rowArrayCursor_->isLatestSchema()) {
			uint32_t nullOffset = rowArrayCursor_->latestParam_.nullsOffset_ - rowArrayCursor_->latestParam_.rowDataOffset_;
			util::StackAllocator &alloc = txn.getDefaultAllocator();
			uint8_t *binary = static_cast<uint8_t *>(alloc.allocate(fixedSize));
			memset(binary, 0, fixedSize);
			memcpy(binary + nullOffset, getNullsAddr(), rowArrayCursor_->getNullbitsSize());
			for (ColumnId columnId = rowArrayCursor_->currentParam_.columnNum_; 
				columnId < rowArrayCursor_->latestParam_.columnNum_; columnId++) {
				ColumnInfo &columnInfo =
					rowArrayCursor_->getContainer().getColumnInfo(columnId);
				if (!columnInfo.isNotNull()) {
					RowNullBits::setNull(binary + nullOffset, columnId);
				}
			}
			memcpy(binary + nullOffset + rowArrayCursor_->latestParam_.nullbitsSize_,
				getNullsAddr() + rowArrayCursor_->currentParam_.nullbitsSize_, rowArrayCursor_->currentParam_.rowFixedColumnSize_);
			fixedAddr = binary;
		}
		messageRowStore->setRowFixedPart(fixedAddr, fixedSize);
	}

	if (rowArrayCursor_->hasVariableColumn()) {
		messageRowStore->setVarSize(
			rowArrayCursor_->getContainer().getVariableColumnNum());  
		OId variablePartOId = getVariableArray();
		if (variablePartOId == UNDEF_OID) {
			for (uint32_t columnId = 0;
				 columnId < rowArrayCursor_->getContainer().getColumnNum();
				 columnId++) {
				ColumnInfo &columnInfo =
					rowArrayCursor_->getContainer().getColumnInfo(columnId);
				if (columnInfo.isVariable()) {
					if (columnInfo.getColumnType() == COLUMN_TYPE_STRING_ARRAY) {
						messageRowStore->setVarDataHeaderField(columnId, 1);
						messageRowStore->setVarSize(0);  
					} else if (columnInfo.getColumnType() == COLUMN_TYPE_BLOB) {
						messageRowStore->setVarDataHeaderField(columnId, 0);
					} else {
						const void *elemData = Value::getDefaultVariableValue(columnInfo.getColumnType());
						uint32_t elemSize = ValueProcessor::decodeVarSize(elemData);
						messageRowStore->setField(columnId, elemData, elemSize);
					}
				}
			}
		} else {
			VariableArrayCursor cursor(txn, objectManager, variablePartOId, OBJECT_READ_ONLY);
			for (uint32_t columnId = 0;
				 columnId < rowArrayCursor_->getContainer().getColumnNum();
				 columnId++) {
				ColumnInfo &columnInfo =
					rowArrayCursor_->getContainer().getColumnInfo(columnId);
				if (columnInfo.isVariable()) {
					bool nextFound = cursor.nextElement();
					uint32_t elemSize;
					uint32_t elemCount;
					if (!nextFound) {
						if (columnInfo.getColumnType() == COLUMN_TYPE_STRING_ARRAY) {
							messageRowStore->setVarDataHeaderField(columnId, 1);
							messageRowStore->setVarSize(0);  
						} else if (columnInfo.getColumnType() == COLUMN_TYPE_BLOB) {
							messageRowStore->setVarDataHeaderField(columnId, 0);
						} else {
							const void *elemData = Value::getDefaultVariableValue(columnInfo.getColumnType());
							uint32_t elemSize = ValueProcessor::decodeVarSize(elemData);
							messageRowStore->setField(columnId, elemData, elemSize);
						}
						continue;
					}
					uint8_t *elemData = cursor.getElement(elemSize, elemCount);
					switch (columnInfo.getColumnType()) {
					case COLUMN_TYPE_STRING_ARRAY: {
						uint32_t totalSize = 0;
						if (elemSize > 0) {
							assert(elemSize == LINK_VARIABLE_COLUMN_DATA_SIZE);
							elemData += ValueProcessor::getEncodedVarSize(elemSize);
							memcpy(&totalSize, elemData, sizeof(uint32_t));
							elemData += sizeof(uint32_t);
						}
						if (totalSize > 0) {
							OId linkOId;
							memcpy(&linkOId, elemData, sizeof(OId));
							messageRowStore->setVarDataHeaderField(
								columnId, totalSize);
							VariableArrayCursor arrayCursor(
								txn, objectManager, linkOId, OBJECT_READ_ONLY);
							messageRowStore->setVarSize(
								arrayCursor.getArrayLength());  
							while (arrayCursor.nextElement()) {
								uint32_t elemSize, elemCount;
								uint8_t *addr =
									arrayCursor.getElement(elemSize, elemCount);
								messageRowStore->addArrayElement(
									addr, elemSize +
											  ValueProcessor::getEncodedVarSize(
												  elemSize));
							}
						}
						else {
							messageRowStore->setVarDataHeaderField(columnId, 1);
							messageRowStore->setVarSize(0);  
						}
					} break;
					case COLUMN_TYPE_BLOB: {
						Value value;
						value.set(elemData, COLUMN_TYPE_BLOB);
						BlobProcessor::getField(txn, objectManager, columnId,
						&value, messageRowStore);
					} break;
					default:
						messageRowStore->setField(columnId, elemData, elemSize);
					}
				}
			}
		}
	}
}

/*!
	@brief translate the field into Message format
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::getFieldImage(TransactionContext &txn,
	ColumnInfo &columnInfo, uint32_t newColumnId,
	MessageRowStore *messageRowStore) {
	ObjectManager &objectManager =
		*(rowArrayCursor_->getContainer().getObjectManager());
	ContainerValue containerValue(txn.getPartitionId(), objectManager);
	getField(txn, columnInfo, containerValue);
	ValueProcessor::getField(
		txn, objectManager, newColumnId, &containerValue.getValue(), messageRowStore);
}

template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::archive(TransactionContext &txn, ArchiveHandler *handler) {
	BaseContainer &container = rowArrayCursor_->getContainer();
	ObjectManager &objectManager = *(container.getObjectManager());
	handler->initializeRow();
	ContainerValue containerValue(txn.getPartitionId(), objectManager);
	for (uint32_t i = 0; i < container.getColumnNum(); i++) {
		handler->initializeField(i);
		getField(txn, container.getColumnInfo(i),
			containerValue);
		containerValue.getValue().archive(txn, objectManager, handler);
		handler->finalizeField();
	}
	handler->finalizeRow();
}



template<typename Container, RowArrayType rowArrayType>
BaseContainer::RowArrayImpl<Container, rowArrayType>::RowArrayImpl(
	TransactionContext &txn, BaseContainer *container, 
	RowArrayStorage &rowArrayStrage, RowCache &rowCache,
	const BaseContainer::RowArray::RowPram &latestParam)
	: container_(container),
	  rowArrayStorage_(rowArrayStrage),
	  rowCache_(rowCache),
	  row_(NULL, this),
	  latestParam_(latestParam),
	  currentParam_(latestParam),
	  elemCursor_(0) {
}

template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::initializeParam() {
	currentParam_ = latestParam_;
	currentParam_.columnNum_ = getColumnNum();
	currentParam_.varColumnNum_ = getVarColumnNum();
	uint32_t rowFixedColumnSize = getRowFixedColumnSize();
	if (currentParam_.columnNum_ == 0) {
		container_->getInitialSchemaStatus(currentParam_.columnNum_, currentParam_.varColumnNum_, rowFixedColumnSize);
	}
	if (!isLatestSchema()) {
		currentParam_.varHeaderSize_ = ValueProcessor::getEncodedVarSize(currentParam_.varColumnNum_);
		if (currentParam_.varColumnNum_ == 0 && latestParam_.varColumnNum_ != 0) {
			currentParam_.nullOffsetDiff_ += sizeof(OId);
			currentParam_.columnOffsetDiff_ = currentParam_.nullOffsetDiff_;
		}

		currentParam_.nullbitsSize_ = RowNullBits::calcBitsSize(currentParam_.columnNum_);
		if (currentParam_.nullbitsSize_ != latestParam_.nullbitsSize_) {
			currentParam_.columnOffsetDiff_ += (latestParam_.nullbitsSize_ - currentParam_.nullbitsSize_);
		}
		currentParam_.rowDataOffset_ = latestParam_.rowDataOffset_;
		currentParam_.nullsOffset_ = latestParam_.nullsOffset_ - currentParam_.nullOffsetDiff_;
		currentParam_.rowFixedColumnSize_ = rowFixedColumnSize;
		currentParam_.rowSize_ = currentParam_.nullsOffset_ + currentParam_.nullbitsSize_ + currentParam_.rowFixedColumnSize_;

		{
//			uint32_t oldRowSize_ = currentParam_.nullsOffset_ + currentParam_.nullbitsSize_;
			for (uint32_t i = currentParam_.columnNum_; i != 0; i--) {
				ColumnInfo &columnInfo =
					getContainer().getColumnInfo(i - 1);
				if (!columnInfo.isVariable()) {
//					oldRowSize_ = currentParam_.rowDataOffset_ + columnInfo.getColumnOffset() + columnInfo.getColumnSize() - currentParam_.columnOffsetDiff_;
					break;
				}
			}
//			uint32_t oldRrowFixedColumnSize_ = oldRowSize_ - currentParam_.nullsOffset_ - currentParam_.nullbitsSize_;
//			assert(oldRrowFixedColumnSize_ == currentParam_.rowFixedColumnSize_);
//			assert(oldRowSize_ == currentParam_.rowSize_);
		}

		if (container_->getContainerType() == TIME_SERIES_CONTAINER) {
			currentParam_.rowIdOffset_ = currentParam_.rowSize_ - currentParam_.rowFixedColumnSize_;
		}
	}
	assert(currentParam_.varColumnNum_ <= latestParam_.varColumnNum_);
}

/*!
	@brief Get Object from Chunk
*/
template<typename Container, RowArrayType rowArrayType>
inline bool BaseContainer::RowArrayImpl<Container, rowArrayType>::load(TransactionContext &txn, OId oId,
	BaseContainer *container, uint8_t getOption) {
	if (getOption != OBJECT_READ_ONLY || rowArrayStorage_.getPartitionId() != txn.getPartitionId()) {
		rowArrayStorage_.load(oId, getOption);
	}
	else {
		rowArrayStorage_.loadNeighbor(oId, OBJECT_READ_ONLY);
		rowArrayStorage_.resetCursor();
	}


	container_ = container;
	if (!container->isFirstColumnAdd() && 
		currentParam_.columnNum_ != getColumnNum()) {
		initializeParam();
	}
	moveCursor(getElemCursor(oId));

	return !isLatestSchema();
}

/*!
	@brief Allocate RowArrayImpl Object
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::initialize(
	TransactionContext &txn, RowId baseRowId, uint16_t maxRowNum) {
	OId oId;
	currentParam_ = latestParam_;
	rowArrayStorage_.allocate<uint8_t>(getBinarySize(maxRowNum),
		container_->getRowAllcateStrategy(), oId, OBJECT_TYPE_ROW_ARRAY);
	reset(txn, baseRowId, maxRowNum);
}

/*!
	@brief Free Objects related to RowArrayImpl
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::finalize(TransactionContext &txn) {
	setDirty(txn);
	for (begin(); !end(); next()) {
		row_.finalize(txn);
	}
	rowArrayStorage_.finalize();
}

/*!
	@brief Append Row to current cursor
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::append(
	TransactionContext &txn, MessageRowStore *messageRowStore, RowId rowId) {
	Row row(getNewRow(), this);
	row.initialize();
	row.setRowId(rowId);
	row.setFields(txn, messageRowStore);
	setRowNum(getRowNum() + 1);
	updateNullsStats(row.getNullsAddr());
	container_->updateNullsStats(row.getNullsAddr());
}

template<typename Container, RowArrayType rowArrayType>
inline BaseContainer::RowArrayImpl<Container, rowArrayType>* BaseContainer::RowArray::getImpl() {
	return static_cast<BaseContainer::RowArrayImpl<Container, rowArrayType> *>(rowArrayImplList_[rowArrayType]);
}

template<typename Container, RowArrayType rowArrayType>
inline BaseContainer::RowArrayImpl<Container, rowArrayType>* BaseContainer::RowArray::getImpl() const {
	return static_cast<BaseContainer::RowArrayImpl<Container, rowArrayType> *>(rowArrayImplList_[rowArrayType]);
}

template<typename Container, RowArrayType rowArrayType>
BaseContainer::RowArray::Column BaseContainer::RowArrayImpl<Container, rowArrayType>::getColumn(const ColumnInfo &info) {
	UTIL_STATIC_ASSERT((!util::IsSame<Container, BaseContainer>::VALUE));

	BaseContainer::RowArray::Column column;
	column.setColumnInfo(info);
	if (util::IsSame<Container, Collection>::VALUE) {
		column.setFixedOffset(BaseContainer::RowArray::getColFixedOffset() + info.getColumnOffset());
	} else if (util::IsSame<Container, TimeSeries>::VALUE) {
		column.setFixedOffset(sizeof(RowHeader) + info.getColumnOffset());
	} else {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_TYPE_INVALID, "");
	}

	return column;
}

template<typename Container, RowArrayType rowArrayType>
BaseContainer::RowArray::Column BaseContainer::RowArrayImpl<Container, rowArrayType>::getRowIdColumn(const BaseContainer &container) {
	UTIL_STATIC_ASSERT((!util::IsSame<Container, BaseContainer>::VALUE));

	BaseContainer::RowArray::Column column;
	if (util::IsSame<Container, Collection>::VALUE) {
		ColumnInfo info;
		info.initialize();
		info.setColumnId(std::numeric_limits<uint16_t>::max());
		info.setType(COLUMN_TYPE_LONG, false);
		info.setOffset(static_cast<uint16_t>(BaseContainer::RowArray::getColRowIdOffset()));
		column.setColumnInfo(info);
		column.setFixedOffset(info.getColumnOffset());
	} else if (util::IsSame<Container, TimeSeries>::VALUE) {
		column.setColumnInfo(container.getColumnInfo(ColumnInfo::ROW_KEY_COLUMN_ID));
		column.setFixedOffset(sizeof(RowHeader) + column.getColumnInfo().getColumnOffset());
	} else {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_TYPE_INVALID, "");
	}
	return column;
}

/*!
	@brief Move to next Row, and Check if Row exists
*/
template<typename Container, RowArrayType rowArrayType>
inline bool BaseContainer::RowArrayImpl<Container, rowArrayType>::next() {
	while (elemCursor_ + 1 < getRowNum()) {
		nextCursor();
		if (!row_.isRemoved()) {
			return true;
		}
	}
	nextCursor();
	return false;
}

/*!
	@brief Move to prev Row, and Check if Row exists
*/
template<typename Container, RowArrayType rowArrayType>
bool BaseContainer::RowArrayImpl<Container, rowArrayType>::prev() {
	assert(elemCursor_ != UINT16_MAX);
	while (elemCursor_ > 0) {
		prevCursor();
		if (!row_.isRemoved()) {
			return true;
		}
	}
	return false;
}

/*!
	@brief Move to first Row, and Check if Row exists
*/
template<typename Container, RowArrayType rowArrayType>
inline bool BaseContainer::RowArrayImpl<Container, rowArrayType>::begin() {
	resetCursor();
	if (row_.isRemoved()) {
		return next();
	}
	return true;
}

/*!
	@brief Check if cursor reached to end
*/
template<typename Container, RowArrayType rowArrayType>
inline bool BaseContainer::RowArrayImpl<Container, rowArrayType>::end() {
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
template<typename Container, RowArrayType rowArrayType>
inline bool BaseContainer::RowArrayImpl<Container, rowArrayType>::tail() {
	if (getRowNum() != 0) {
		moveCursor(getRowNum() - 1);
	}
	else {
		resetCursor();
	}
	if (row_.isRemoved()) {
		return prev();
	}
	return true;
}

/*!
	@brief Check if next Row exists
*/
template<typename Container, RowArrayType rowArrayType>
inline bool BaseContainer::RowArrayImpl<Container, rowArrayType>::hasNext() const {
	if (elemCursor_ + 1 < getRowNum()) {
		return true;
	}
	else {
		return false;
	}
}

/*!
	@brief Get Middle RowId of RowArray
*/
template<typename Container, RowArrayType rowArrayType>
RowId BaseContainer::RowArrayImpl<Container, rowArrayType>::getMidRowId() {
	uint16_t midPos = getMaxRowNum() / 2;
	Row midRow(getRow(midPos), this);
	return midRow.getRowId();
}
/*!
	@brief Check if reserved Row area is full to capacity
*/
template<typename Container, RowArrayType rowArrayType>
bool BaseContainer::RowArrayImpl<Container, rowArrayType>::isFull() {
	return (getRowNum() == getMaxRowNum() && getActiveRowNum() == getMaxRowNum()) ? true : false;
}
/*!
	@brief Check if RowArray is initialized
*/
template<typename Container, RowArrayType rowArrayType>
bool BaseContainer::RowArrayImpl<Container, rowArrayType>::isNotInitialized() const {
	return rowArrayStorage_.getBaseOId() == UNDEF_OID ? true : false;
}
/*!
	@brief Check if cursor is last Row
*/
template<typename Container, RowArrayType rowArrayType>
bool BaseContainer::RowArrayImpl<Container, rowArrayType>::isTailPos() const {
	return elemCursor_ >= getMaxRowNum() - 1 ? true : false;
}

/*!
	@brief Get OId of Row(RowArray OId + row offset)
*/
template<typename Container, RowArrayType rowArrayType>
inline OId BaseContainer::RowArrayImpl<Container, rowArrayType>::getOId() const {
	return ObjectManager::setUserArea(getBaseOId(), static_cast<OId>(elemCursor_));
}

/*!
	@brief Get OId of RowArray
*/
template<typename Container, RowArrayType rowArrayType>
OId BaseContainer::RowArrayImpl<Container, rowArrayType>::getBaseOId() const {
	return getBaseOId(rowArrayStorage_.getBaseOId());
}

/*!
	@brief Return address of new Row
*/
template<typename Container, RowArrayType rowArrayType>
uint8_t *BaseContainer::RowArrayImpl<Container, rowArrayType>::getNewRow() {
	moveCursor(getRowNum());
	return row_.getBinary();
}

/*!
	@brief Get address of current Row
*/
template<typename Container, RowArrayType rowArrayType>
inline uint8_t *BaseContainer::RowArrayImpl<Container, rowArrayType>::getRow() {
	return row_.getBinary();
}

/*!
	@brief Get number of Rows(include removed Rows)
*/
template<typename Container, RowArrayType rowArrayType>
inline uint16_t BaseContainer::RowArrayImpl<Container, rowArrayType>::getRowNum() const {
	return *reinterpret_cast<uint16_t *>(getAddr() + ROW_NUM_OFFSET);
}

/*!
	@brief Get number of existed Rows
*/
template<typename Container, RowArrayType rowArrayType>
inline uint16_t BaseContainer::RowArrayImpl<Container, rowArrayType>::getActiveRowNum(uint16_t limit) {
	uint16_t currentCursor = elemCursor_;
	updateCursor();
	uint16_t activeRowNum = 0;
	resetCursor();
	for (uint16_t i = 0; i < getRowNum(); i++) {
		if (!row_.isRemoved()) {
			activeRowNum++;
			if (activeRowNum >= limit) {
				break;
			}
		}
		nextCursor();
	}
	moveCursor(currentCursor);
	return activeRowNum;
}

/*!
	@brief Get maximum that it can store
*/
template<typename Container, RowArrayType rowArrayType>
inline uint16_t BaseContainer::RowArrayImpl<Container, rowArrayType>::getMaxRowNum() const {
	return *reinterpret_cast<uint16_t *>(
		getAddr() + MAX_ROW_NUM_OFFSET);
}

/*!
	@brief Get header size of RowArray Object
*/
template<typename Container, RowArrayType rowArrayType>
inline uint32_t BaseContainer::RowArrayImpl<Container, rowArrayType>::getHeaderSize() const {
	uint32_t headerSize = HEADER_AREA_SIZE + getNullbitsSize(); 
	assert(headerSize == calcHeaderSize(getNullbitsSize()));
	return headerSize;
}
template<typename Container, RowArrayType rowArrayType>
inline uint32_t BaseContainer::RowArrayImpl<Container, rowArrayType>::calcHeaderSize(uint32_t nullbitsSize) {
	return HEADER_AREA_SIZE + nullbitsSize;
}

template<typename Container, RowArrayType rowArrayType>
inline TransactionId BaseContainer::RowArrayImpl<Container, rowArrayType>::getTxnId() const {
	TransactionId *tIdAddr;
	if (getContainer().getContainerType() == TIME_SERIES_CONTAINER) {
		tIdAddr = reinterpret_cast<TransactionId *>(getTIdAddr());
	} else {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_TYPE_INVALID, "lock for block not supported");
	}
	return (*tIdAddr & Row::TID_FIELD);
}

template<typename Container, RowArrayType rowArrayType>
inline RowArrayType BaseContainer::RowArrayImpl<Container, rowArrayType>::getRowArrayType() const {
	if (isLatestSchema()) {
		return BaseContainer::ROW_ARRAY_PLAIN;
	} else {
		return BaseContainer::ROW_ARRAY_GENERAL;
	}
}


template<typename Container, RowArrayType rowArrayType>
bool BaseContainer::RowArrayImpl<Container, rowArrayType>::setDirty(TransactionContext &txn) {
	rowArrayStorage_.setDirty();
	return !isLatestSchema();
}

/*!
	@brief Get RowId address of current Row
*/
template<typename Container, RowArrayType rowArrayType>
inline uint8_t *BaseContainer::RowArrayImpl<Container, rowArrayType>::getRowIdAddr() const {
	return getAddr() + ROWID_OFFSET;
}

/*!
	@brief Set RowId to current Row
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::setRowId(RowId rowId) {
	memcpy(getRowIdAddr(), &rowId, sizeof(RowId));
}

/*!
	@brief Get RowId of current Row
*/
template<typename Container, RowArrayType rowArrayType>
inline RowId BaseContainer::RowArrayImpl<Container, rowArrayType>::getRowId() const {
	return *reinterpret_cast<RowId *>(getRowIdAddr());
}
template<typename Container, RowArrayType rowArrayType>
inline uint8_t *BaseContainer::RowArrayImpl<Container, rowArrayType>::getNullsStats() {
	return getAddr() + HEADER_AREA_SIZE;
}
template<typename Container, RowArrayType rowArrayType>
inline uint32_t BaseContainer::RowArrayImpl<Container, rowArrayType>::getNullbitsSize() const {
	return currentParam_.nullbitsSize_;
}
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::setContainerId(ContainerId containerId) {
	ContainerId *containerIdAddr = 
		reinterpret_cast<ContainerId *>(rowArrayStorage_.getBaseAddr() + CONTAINER_ID_OFFSET);
	*containerIdAddr = containerId;
}
template<typename Container, RowArrayType rowArrayType>
inline ContainerId BaseContainer::RowArrayImpl<Container, rowArrayType>::getContainerId() const {
	ContainerId *containerIdAddr = 
		reinterpret_cast<ContainerId *>(rowArrayStorage_.getBaseAddr() + CONTAINER_ID_OFFSET);
	return *containerIdAddr;
}
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::setColumnNum(uint16_t columnNum) {
	uint16_t *columnNumAddr = 
		reinterpret_cast<uint16_t *>(rowArrayStorage_.getBaseAddr() + COLUMN_NUM_OFFSET);
	*columnNumAddr = columnNum;
}
template<typename Container, RowArrayType rowArrayType>
inline uint16_t BaseContainer::RowArrayImpl<Container, rowArrayType>::getColumnNum() const {
	uint16_t *columnNumAddr = 
		reinterpret_cast<uint16_t *>(rowArrayStorage_.getBaseAddr() + COLUMN_NUM_OFFSET);
	return *columnNumAddr;
}

template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::setVarColumnNum(uint16_t columnNum) {
	uint16_t *columnNumAddr = 
		reinterpret_cast<uint16_t *>(rowArrayStorage_.getBaseAddr() + VAR_COLUMN_NUM_OFFSET);
	*columnNumAddr = columnNum;
}
template<typename Container, RowArrayType rowArrayType>
inline uint16_t BaseContainer::RowArrayImpl<Container, rowArrayType>::getVarColumnNum() const {
	uint16_t *columnNumAddr = 
		reinterpret_cast<uint16_t *>(rowArrayStorage_.getBaseAddr() + VAR_COLUMN_NUM_OFFSET);
	return *columnNumAddr;
}

template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::setRowFixedColumnSize(uint16_t fixedSize) {
	uint16_t *fixedSizeAddr = 
		reinterpret_cast<uint16_t *>(rowArrayStorage_.getBaseAddr() + ROW_FIXED_SIZE_OFFSET);
	*fixedSizeAddr = fixedSize;
}
template<typename Container, RowArrayType rowArrayType>
inline uint16_t BaseContainer::RowArrayImpl<Container, rowArrayType>::getRowFixedColumnSize() const {
	uint16_t *fixedSizeAddr = 
		reinterpret_cast<uint16_t *>(rowArrayStorage_.getBaseAddr() + ROW_FIXED_SIZE_OFFSET);
	return *fixedSizeAddr;
}

template<typename Container, RowArrayType rowArrayType>
inline uint8_t *BaseContainer::RowArrayImpl<Container, rowArrayType>::getTIdAddr() const {
	return getAddr() + TIM_TID_OFFSET;
}
/*!
	@brief Set flag that this RowArray is already updated in the
   transaction
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::setFirstUpdate() {
	uint8_t *val = getBitsAddr();
	*val |= Row::getFirstUpdateBit();
}
/*!
	@brief Reset flag that this RowArray is already updated in the
   transaction
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::resetFirstUpdate() {
	uint8_t *val = getBitsAddr();
	*val ^= Row::getFirstUpdateBit();
}
/*!
	@brief Check if this RowArray is already updated in the transaction
*/
template<typename Container, RowArrayType rowArrayType>
bool BaseContainer::RowArrayImpl<Container, rowArrayType>::isFirstUpdate() const {
	return (*getBitsAddr() & Row::getFirstUpdateBit()) != 0;
}

template<typename Container, RowArrayType rowArrayType>
inline uint8_t *BaseContainer::RowArrayImpl<Container, rowArrayType>::getRow(uint16_t elem) const {
	return getAddr() + getHeaderSize() + elem * currentParam_.rowSize_;
}

template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::setMaxRowNum(uint16_t num) {
	uint16_t *addr =
		reinterpret_cast<uint16_t *>(getAddr() + MAX_ROW_NUM_OFFSET);
	*addr = num;
}
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::setRowNum(uint16_t num) {
	uint16_t *addr =
		reinterpret_cast<uint16_t *>(getAddr() + ROW_NUM_OFFSET);
	*addr = num;
}
template<typename Container, RowArrayType rowArrayType>
inline uint8_t *BaseContainer::RowArrayImpl<Container, rowArrayType>::getAddr() const {
	return rowArrayStorage_.getBaseAddr();
}
template<typename Container, RowArrayType rowArrayType>
inline uint16_t BaseContainer::RowArrayImpl<Container, rowArrayType>::getElemCursor(OId oId) const {
	return static_cast<uint16_t>(ObjectManager::getUserArea(oId));
}
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::updateCursor() {
	uint16_t maxRowPos = -1;
	uint16_t currentCursor = elemCursor_;
	resetCursor();
	for (uint16_t i = 0; i < getMaxRowNum(); i++) {
		if (!row_.isRemoved()) {
			maxRowPos = i;
		}
		nextCursor();
	}
	setRowNum(maxRowPos + 1);
	moveCursor(currentCursor);
}
template<typename Container, RowArrayType rowArrayType>
OId BaseContainer::RowArrayImpl<Container, rowArrayType>::getBaseOId(OId oId) const {
	return ObjectManager::getBaseArea(oId);
}
template<typename Container, RowArrayType rowArrayType>
uint32_t BaseContainer::RowArrayImpl<Container, rowArrayType>::getBinarySize(uint16_t maxRowNum) const {
	return static_cast<uint32_t>(
		getHeaderSize() + currentParam_.rowSize_ * maxRowNum);
}



template<typename Container, RowArrayType rowArrayType>
inline BaseContainer &BaseContainer::RowArrayImpl<Container, rowArrayType>::getContainer() const {
	return *container_;
}

template<typename Container, RowArrayType rowArrayType>
inline uint8_t *BaseContainer::RowArrayImpl<Container, rowArrayType>::getBitsAddr() const {
	return getAddr() + TIM_BITS_OFFSET;
}
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::setLockTId(TransactionId tId) {
	TransactionId *targetAddr =
		reinterpret_cast<TransactionId *>(getTIdAddr());
	TransactionId filterTId = (tId & Row::TID_FIELD);
	TransactionId header = ((*targetAddr) & Row::BITS_FIELD);
	*targetAddr = (header | filterTId);
	setFirstUpdate();
}


/*!
	@brief Insert Row to current cursor
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::insert(
	TransactionContext &txn, MessageRowStore *messageRowStore, RowId rowId) {
	row_.initialize();
	row_.setRowId(rowId);
	row_.setFields(txn, messageRowStore);
	if (elemCursor_ >= getRowNum()) {
		setRowNum(elemCursor_ + 1);
	}
	updateNullsStats(row_.getNullsAddr());
	container_->updateNullsStats(row_.getNullsAddr());
}

/*!
	@brief Update Row on current cursor
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::update(
	TransactionContext &txn, MessageRowStore *messageRowStore) {
	row_.updateFields(txn, messageRowStore);
	updateNullsStats(messageRowStore->getNullsAddr());
	container_->updateNullsStats(messageRowStore->getNullsAddr());
}

/*!
	@brief Delete this Row on current cursor
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::remove(TransactionContext &txn) {
	row_.remove(txn);
	if (elemCursor_ == getRowNum() - 1) {
		setRowNum(elemCursor_);
	}
}

/*!
	@brief Move Row on current cursor to Another RowArrayImpl
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::move(TransactionContext &txn, RowArrayImpl &dest) {
	Row row(getRow(), this);
	Row destRow(dest.getRow(), &dest);
	row.move(txn, destRow);
	updateCursor();
	dest.updateCursor();
	dest.updateNullsStats(destRow.getNullsAddr());
}

template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::convert(TransactionContext &txn, RowArrayImpl &dest) {
	Row row(getRow(), this);
	Row destRow(dest.getRow(), &dest);
	row.convert(txn, destRow);
	updateCursor();
	dest.updateCursor();
	dest.updateNullsStats(destRow.getNullsAddr());
}

/*!
	@brief Copy Row on current cursor to Another RowArrayImpl
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::copy(TransactionContext &txn, RowArrayImpl &dest) {
	Row row(getRow(), this);
	Row destRow(dest.getRow(), &dest);
	row.copy(txn, destRow);
	updateCursor();
	dest.updateCursor();
	dest.updateNullsStats(destRow.getNullsAddr());
}

/*!
	@brief Update stats of nullbits
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::updateNullsStats(const uint8_t *nullbits) {
	RowNullBits::unionNullsStats(nullbits, getNullsStats(), getNullbitsSize());
}

template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::copyRowArray(
	TransactionContext &txn, RowArrayImpl &dest) {

	uint16_t currentCursor = elemCursor_;
	if (hasVariableColumn()) {
		memcpy(dest.getAddr(), getAddr(), RowArrayImpl::getHeaderSize());
		for (begin(); !end(); next()) {
			copy(txn, dest);
			dest.next();
		}
	}
	else {
		memcpy(dest.getAddr(), getAddr(), getBinarySize(getMaxRowNum()));
	}
	moveCursor(currentCursor);
}

template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::moveRowArray(TransactionContext &txn) {
	uint16_t currentCursor = elemCursor_;
	moveCursor(currentCursor);
}

/*!
	@brief Move to next RowArrayImpl, and Check if RowArrayImpl exists
*/
template<typename Container, RowArrayType rowArrayType>
bool BaseContainer::RowArrayImpl<Container, rowArrayType>::nextRowArray(
	TransactionContext &txn, RowArrayImpl &neighbor, bool &isOldSchema, uint8_t getOption) {
	RowId rowId = getRowId();
	BtreeMap::SearchContext sc(
		0, &rowId, 0, false, NULL, 0, false, 0, NULL, 2);  
	util::XArray<OId> oIdList(txn.getDefaultAllocator());
	util::XArray<OId>::iterator itr;
	StackAllocAutoPtr<BtreeMap> rowIdMap(
		txn.getDefaultAllocator(), container_->getRowIdMap(txn));
	rowIdMap.get()->search(txn, sc, oIdList);
	for (itr = oIdList.begin(); itr != oIdList.end(); itr++) {
		if (*itr != getBaseOId()) {
			isOldSchema = neighbor.load(txn, *itr, container_, getOption);
			return true;
		}
	}
	return false;
}

/*!
	@brief Move to prev RowArrayImpl, and Check if RowArrayImpl exists
*/
template<typename Container, RowArrayType rowArrayType>
bool BaseContainer::RowArrayImpl<Container, rowArrayType>::prevRowArray(
	TransactionContext &txn, RowArrayImpl &neighbor, bool &isOldSchema, uint8_t getOption) {
	RowId rowId = getRowId();
	BtreeMap::SearchContext sc(
		0, NULL, 0, false, &rowId, 0, false, 0, NULL, 2);  
	util::XArray<OId> oIdList(txn.getDefaultAllocator());
	util::XArray<OId>::iterator itr;
	StackAllocAutoPtr<BtreeMap> rowIdMap(
		txn.getDefaultAllocator(), container_->getRowIdMap(txn));
	rowIdMap.get()->search(txn, sc, oIdList, ORDER_DESCENDING);
	for (itr = oIdList.begin(); itr != oIdList.end(); itr++) {
		if (*itr != getBaseOId()) {
			isOldSchema = neighbor.load(txn, *itr, container_, getOption);
			return true;
		}
	}
	return false;
}

/*!
	@brief Search Row corresponding to RowId
*/
template<typename Container, RowArrayType rowArrayType>
bool BaseContainer::RowArrayImpl<Container, rowArrayType>::searchRowId(RowId rowId) {
	resetCursor();
	for (uint16_t i = 0; i < getRowNum(); i++) {
		if (!row_.isRemoved()) {
			RowId currentRowId = row_.getRowId();
			if (currentRowId == rowId) {
				return true;
			}
			else if (currentRowId > rowId) {
				return false;
			}
		}
		nextCursor();
	}
	moveCursor(getRowNum());
	return false;
}

template<typename Container, RowArrayType rowArrayType>
bool BaseContainer::RowArrayImpl<Container, rowArrayType>::searchNextRowId(RowId rowId) {
	uint16_t rowNum = getRowNum();
	if (!begin() || rowId < row_.getRowId()) {
		return false;
	}
	uint16_t lowPos = elemCursor_;
	uint16_t highPos = rowNum - 1;
	uint16_t midPos = 0;
	while (lowPos <= highPos) {
		midPos = ((lowPos + highPos) >> 1);
		moveCursor(midPos);
		bool isExist = !row_.isRemoved();
		if (!isExist) {
			isExist = next();
		}
		if (isExist) {
			RowId currentRowId = row_.getRowId();
			if (currentRowId == rowId) {
				return true;
			}
			else if (currentRowId < rowId) {
				if (!next()) {
					break;
				}
				lowPos = elemCursor_;
			}
			else {
				if (!prev()) {
					next();
					break;
				}
				highPos = elemCursor_;
			}
		} else {
			if (!prev()) {
				next();
				break;
			}
			highPos = elemCursor_;
		}
	}
	if (elemCursor_ < rowNum && rowId > row_.getRowId()) {
		next();
	}
	return false;
}

template<typename Container, RowArrayType rowArrayType>
bool BaseContainer::RowArrayImpl<Container, rowArrayType>::searchPrevRowId(RowId rowId) {
	if (searchNextRowId(rowId)) {
		return true;
	} else {
		return prev();
	}
}

/*!
	@brief Lock this RowArrayImpl
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::lock(TransactionContext &txn) {
	if (getTxnId() == txn.getId()) {
	}
	else if (txn.getManager().isActiveTransaction(
				 txn.getPartitionId(), getTxnId())) {
		DS_THROW_LOCK_CONFLICT_EXCEPTION(GS_ERROR_DS_COL_LOCK_CONFLICT,
			"(pId=" << txn.getPartitionId() << ", rowTxnId=" << getTxnId()
					<< ", txnId=" << txn.getId() << ")");
	}
	else {
		setLockTId(txn.getId());
	}
}


/*!
	@brief Shift Rows to next position
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::shift(TransactionContext &txn, bool isForce,
	util::XArray<std::pair<OId, OId> > &moveList) {
	if (isForce) {
		moveCursor(getMaxRowNum());
	}
	uint16_t insertPos = elemCursor_;
	uint16_t targetPos = getMaxRowNum();

	for (uint16_t i = insertPos; i < getMaxRowNum(); i++) {
		moveCursor(i);
		if (row_.isRemoved()) {
			targetPos = i;
			break;
		}
	}
	if (targetPos != getMaxRowNum()) {
		for (uint16_t i = targetPos; i > insertPos; i--) {
			moveCursor(i - 1);
			Row row(getRow(), this);
			OId oldOId = getOId();
			moveCursor(i);
			Row destRow(getRow(), this);
			OId newOId = getOId();
			row.move(txn, destRow);
			if (destRow.isRemoved()) {
				break;
			}
			moveList.push_back(std::make_pair(oldOId, newOId));
		}
		moveCursor(insertPos);
	}
	else {
		for (uint16_t i = insertPos + 1; i > 0; i--) {
			if (i >= getMaxRowNum()) {
				continue;
			}
			moveCursor(i - 1);
			if (row_.isRemoved()) {
				targetPos = i - 1;
				break;
			}
		}
		for (uint16_t i = targetPos; i < insertPos; i++) {
			moveCursor(i);
			Row row(getRow(), this);
			if (!row.isRemoved()) {
				OId oldOId = getOId();
				moveCursor(i - 1);
				Row destRow(getRow(), this);
				OId newOId = getOId();
				row.move(txn, destRow);
				moveList.push_back(std::make_pair(oldOId, newOId));
			}
		}
		moveCursor(insertPos - 1);
	}
	updateCursor();
}

/*!
	@brief Split this RowArrayImpl
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::split(TransactionContext &txn, RowId insertRowId,
	RowArrayImpl &splitRowArray, RowId splitRowId,
	util::XArray<std::pair<OId, OId> > &moveList) {
	uint16_t insertPos = elemCursor_;
	uint16_t midPos = getMaxRowNum() / 2;
	if (insertRowId < splitRowId) {
		for (uint16_t i = midPos; i < getMaxRowNum(); i++) {
			moveCursor(i);
			move(txn, splitRowArray);
			OId oldOId = getOId();
			OId newOId = splitRowArray.getOId();
			moveList.push_back(std::make_pair(oldOId, newOId));
			splitRowArray.next();
		}
		for (uint16_t i = midPos; i > insertPos; i--) {
			moveCursor(i - 1);
			Row row(getRow(), this);
			OId oldOId = getOId();
			moveCursor(i);
			Row destRow(getRow(), this);
			OId newOId = getOId();
			row.move(txn, destRow);
			moveList.push_back(std::make_pair(oldOId, newOId));
		}
		moveCursor(insertPos);
	}
	else {
		uint16_t destCursor = getMaxRowNum() - midPos;
		for (uint16_t i = midPos; i < getMaxRowNum(); i++) {
			moveCursor(i);
			if (i == insertPos) {
				destCursor = splitRowArray.elemCursor_;
				splitRowArray.updateCursor();
				splitRowArray.next();
			}
			move(txn, splitRowArray);
			OId oldOId = getOId();
			OId newOId = splitRowArray.getOId();
			moveList.push_back(std::make_pair(oldOId, newOId));
			splitRowArray.next();
		}
		splitRowArray.moveCursor(destCursor);
		moveCursor(midPos - 1);
	}
	updateCursor();
	splitRowArray.updateCursor();
}

/*!
	@brief Merge this RowArrayImpl and another RowArrayImpl
*/
template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::merge(TransactionContext &txn,
	RowArrayImpl &nextRowArray, util::XArray<std::pair<OId, OId> > &moveList) {
	uint16_t pos = 0;
	for (uint16_t i = 0; i < getRowNum(); i++) {
		moveCursor(i);
		Row row(getRow(), this);
		if (!row.isRemoved()) {
			if (pos != i) {
				OId oldOId = getOId();
				moveCursor(pos);
				Row destRow(getRow(), this);
				OId newOId = getOId();
				row.move(txn, destRow);
				moveList.push_back(std::make_pair(oldOId, newOId));
			}
			pos++;
		}
	}
	for (nextRowArray.begin(); !nextRowArray.end(); nextRowArray.next()) {
		moveCursor(pos);
		OId oldOId = nextRowArray.getOId();
		nextRowArray.move(txn, *this);
		OId newOId = getOId();
		moveList.push_back(std::make_pair(oldOId, newOId));
		pos++;
	}
	updateCursor();
	nextRowArray.updateCursor();
}



template<typename Container, RowArrayType rowArrayType>
bool BaseContainer::RowArrayImpl<Container, rowArrayType>::convertSchema(TransactionContext &txn,
	util::XArray< std::pair<RowId, OId> > &splitRAList,
	util::XArray< std::pair<OId, OId> > &moveOIdList) {

	BaseObject &frontObject = rowCache_.getFrontFieldObject();
	frontObject.reset();

	bool isRelease = false;
		
//	int64_t maxRowNum = getMaxRowNum();
	int64_t activeRowNum = getActiveRowNum();
	ObjectManager &objectManager =
		*(getContainer().getObjectManager());
	uint32_t binarySize = objectManager.getSize(rowArrayStorage_.getBaseAddr());
	int64_t thisRowArrayMaxRowNum = getContainer().calcRowArrayNumBySize(binarySize, getContainer().getNullbitsSize());

	int64_t restRowNum = 0, thisRowArrayRowNum = thisRowArrayMaxRowNum;
	if (activeRowNum > thisRowArrayMaxRowNum) {
		restRowNum = activeRowNum - thisRowArrayMaxRowNum;
	} else {
		thisRowArrayRowNum = activeRowNum;
	}
	int64_t currentMaxRowNum = getContainer().getNormalRowArrayNum();
//	uint16_t currentCursor = elemCursor_;
	OId currentOId = getOId();
	OId newCurrentOId = UNDEF_OID;

	if (restRowNum > 0) {
		begin();
		for (size_t i = 0; i < static_cast<size_t>(thisRowArrayRowNum); i++) {
			next();
		}
	}
	while (restRowNum >= currentMaxRowNum) {
		RowId baseRowId = row_.getRowId();
		RowArray splitRowArray(txn, container_);
		splitRowArray.initialize(txn, baseRowId, currentMaxRowNum);
		for (size_t i = 0; i < static_cast<size_t>(currentMaxRowNum); i++) {
			OId oldOId = getOId();
			convert(txn, *(splitRowArray.getDefaultImpl()));	
			OId newOId = splitRowArray.getOId();
			moveOIdList.push_back(std::make_pair(oldOId, newOId));
			if (currentOId == oldOId) {
				newCurrentOId = newOId;
			}
			splitRowArray.next();
			next();
		}
		restRowNum -= currentMaxRowNum;
		splitRAList.push_back(std::make_pair( splitRowArray.getRowId(), splitRowArray.getBaseOId()));
	}

	if (restRowNum > 0) {
		bool isRowArraySizeControlMode = false;
		int64_t restMaxRowNum = getContainer().calcRowArrayNum(txn, isRowArraySizeControlMode, restRowNum);
		RowId baseRowId = row_.getRowId();
		RowArray splitRowArray(txn, container_);
		splitRowArray.initialize(txn, baseRowId, restMaxRowNum);
		for (size_t i = 0; i < static_cast<size_t>(restRowNum); i++) {
			OId oldOId = getOId();
			convert(txn, *(splitRowArray.getDefaultImpl()));	
			OId newOId = splitRowArray.getOId();
			moveOIdList.push_back(std::make_pair(oldOId, newOId));
			if (currentOId == oldOId) {
				newCurrentOId = newOId;
			}
			splitRowArray.next();
			next();
		}

		splitRAList.push_back(std::make_pair( splitRowArray.getRowId(), splitRowArray.getBaseOId()));
	}

	if (thisRowArrayRowNum > 0) {
		begin();
		RowArray splitRowArray(txn, container_);
		splitRowArray.initialize(txn, getRowId(), thisRowArrayMaxRowNum);
		for (size_t i = 0; i < static_cast<size_t>(thisRowArrayRowNum); i++) {
			OId oldOId = getOId();
			convert(txn, *(splitRowArray.getDefaultImpl()));	
			OId newOId = ObjectManager::setUserArea(getBaseOId(), static_cast<OId>(i));
			moveOIdList.push_back(std::make_pair(oldOId, newOId));
			if (currentOId == oldOId) {
				newCurrentOId = newOId;
			}
			splitRowArray.next();
			next();
		}

		reset(txn, splitRowArray.getRowId(), splitRowArray.getMaxRowNum());
		splitRowArray.begin();
		for (size_t i = 0; i < static_cast<size_t>(thisRowArrayRowNum); i++) {
			splitRowArray.getDefaultImpl()->move(txn, *this);
			splitRowArray.next();
			next();
		}
		splitRowArray.finalize(txn);
	} else {
		if (activeRowNum == 0) {
			RowId baseRowId = getRowId();
			RowArray splitRowArray(txn, container_);
			splitRowArray.initialize(txn, baseRowId, currentMaxRowNum);
			splitRAList.push_back(std::make_pair( splitRowArray.getRowId(), splitRowArray.getBaseOId()));
			newCurrentOId = splitRowArray.getBaseOId();
		}
		finalize(txn);
		isRelease = true;
	}

	if (newCurrentOId != UNDEF_OID) {
		load(txn, newCurrentOId, container_, OBJECT_FOR_UPDATE); 
	} else if (isRelease) {
		assert(!splitRAList.empty());
		load(txn, splitRAList.back().second, container_, OBJECT_FOR_UPDATE); 
	} else {
		begin();
	}

	return isRelease;
}

template<typename Container, RowArrayType rowArrayType>
inline bool BaseContainer::RowArrayImpl<Container, rowArrayType>::isLatestSchema() const {
	return currentParam_.columnNum_ == latestParam_.columnNum_;
}

template<typename Container, RowArrayType rowArrayType>
inline bool BaseContainer::RowArrayImpl<Container, rowArrayType>::isNotExistColumn(ColumnInfo columnInfo) const {
	return !isLatestSchema() && columnInfo.getColumnId() >= getColumnNum();
}

template<typename Container, RowArrayType rowArrayType>
inline bool BaseContainer::RowArrayImpl<Container, rowArrayType>::hasVariableColumn() const {
	return getVarColumnNum() != 0;
}

template<typename Container, RowArrayType rowArrayType>
inline OId BaseContainer::RowArrayImpl<Container, rowArrayType>::calcOId(uint16_t cursor) const {
	return ObjectManager::setUserArea(getBaseOId(), static_cast<OId>(cursor));
}


template<typename Container, RowArrayType rowArrayType>
void BaseContainer::RowArrayImpl<Container, rowArrayType>::reset(
	TransactionContext &txn, RowId baseRowId, uint16_t maxRowNum) {
	currentParam_ = latestParam_;
	memset(rowArrayStorage_.getBaseAddr(), 0, getHeaderSize());
	setMaxRowNum(maxRowNum);
	setRowNum(0);
	setRowId(baseRowId);

	setContainerId(container_->getContainerId());
	setColumnNum(container_->getColumnNum());
	setVarColumnNum(container_->getVariableColumnNum());
	setRowFixedColumnSize(container_->getRowFixedColumnSize());

	uint32_t nullBitSize = getNullbitsSize();
	uint8_t *nullsList = getNullsStats();
	memset(nullsList, 0, nullBitSize);

	resetCursor();
	for (uint16_t i = 0; i < getMaxRowNum(); i++) {
		row_.reset();
		nextCursor();
	}
	resetCursor();
}

template<typename Container, RowArrayType rowArrayType>
inline void BaseContainer::RowArrayImpl<Container, rowArrayType>::resetCursor() {
	elemCursor_ = 0;
	row_.setBinary(getAddr() + getHeaderSize());
}

template<typename Container, RowArrayType rowArrayType>
inline void BaseContainer::RowArrayImpl<Container, rowArrayType>::moveCursor(uint16_t elem) {
	elemCursor_ = elem;
	row_.setBinary(getAddr() + getHeaderSize() + elem * currentParam_.rowSize_);
}

template<typename Container, RowArrayType rowArrayType>
inline void BaseContainer::RowArrayImpl<Container, rowArrayType>::nextCursor() {
	elemCursor_++;
	row_.setBinary(row_.getBinary() + currentParam_.rowSize_);
}

template<typename Container, RowArrayType rowArrayType>
inline void BaseContainer::RowArrayImpl<Container, rowArrayType>::prevCursor() {
	elemCursor_--;
	row_.setBinary(row_.getBinary() - currentParam_.rowSize_);
}


template<typename Container, RowArrayType rowArrayType>
std::string BaseContainer::RowArrayImpl<Container, rowArrayType>::dump(TransactionContext &txn) {
	uint16_t pos = elemCursor_;
	util::NormalOStringStream strstrm;
	strstrm << "RowId," << getRowId() << ",MaxRowNum," << getMaxRowNum()
			<< ",RowNum," << getActiveRowNum();
	strstrm << ",ContainerId," << getContainerId();
	strstrm << ",ColumnNum," << getColumnNum();
	if (getColumnNum() != getContainer().getColumnNum()) {
		strstrm << "(Latest " << getContainer().getColumnNum() <<")";
	}
	strstrm << ",VarColumnNum," << getVarColumnNum() << ",";
	if (getContainer().getContainerType() == TIME_SERIES_CONTAINER) {
		strstrm << ",TxnId=" << getTxnId();
	}
	strstrm << std::endl;
	strstrm << "ChunkId,Offset,ElemNum" << std::endl;
	for (begin(); !end(); next()) {
		Row row(getRow(), this);
		OId oId = getOId();
		strstrm << ObjectManager::getChunkId(oId) << ","
				<< ObjectManager::getOffset(oId) << ","
				<< ObjectManager::getChunkId(oId) << "," << getElemCursor(oId)
				<< ",";
		strstrm << row.dump(txn) << std::endl;
	}
	moveCursor(pos);
	return strstrm.str();
}


template<typename Container, RowArrayType rowArrayType>
bool BaseContainer::RowArrayImpl<Container, rowArrayType>::validate() {
	return true;
}

template<typename Container, RowArrayType rowArrayType>
std::string BaseContainer::RowArrayImpl<Container, rowArrayType>::Row::dump(TransactionContext &txn) {
	ObjectManager &objectManager =
		*(rowArrayCursor_->getContainer().getObjectManager());
	util::NormalOStringStream strstrm;
	strstrm << "(";
	if (rowArrayCursor_->getContainer().getContainerType() == COLLECTION_CONTAINER) {
		strstrm << ", RowId=" << getRowId() << ", TxnId=" << getTxnId() << ", ";
	}
	ContainerValue containerValue(txn.getPartitionId(), objectManager);
	for (uint32_t i = 0; i < rowArrayCursor_->getContainer().getColumnNum();
		 i++) {
		if (i != 0) {
			strstrm << ",";
		}
		getField(txn, rowArrayCursor_->getContainer().getColumnInfo(i),
			containerValue);
		containerValue.getValue().dump(txn, objectManager, strstrm);
	}
	strstrm << ")";
	return strstrm.str();
}


#endif
