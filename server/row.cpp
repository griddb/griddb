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
#include "util/trace.h"
#include "btree_map.h"
#include "collection.h"
#include "data_store_v4.h"
#include "data_store_common.h"
#include "blob_processor.h"
#include "gs_error.h"
#include "message_schema.h"
#include "string_array_processor.h"
#include "value_processor.h"



#include "row.h"

BaseContainer::RowArray::RowArray(TransactionContext &txn, BaseContainer *container) 
	: rowArrayImplList_(txn.getDefaultAllocator()),
	rowArrayStorage_(*container->getObjectManager(), container->getRowAllocateStrategy()),
	rowCache_(txn, container), defaultImpl_(NULL) {

	if (container->getContainerType() == COLLECTION_CONTAINER) {
		latestParam_.rowDataOffset_ = getColFixedOffset();
		latestParam_.rowIdOffset_ = getColRowIdOffset();
		latestParam_.rowHeaderOffset_ = COL_ROW_HEADER_OFFSET;
	} else if (container->getContainerType() == TIME_SERIES_CONTAINER) {
		latestParam_.rowDataOffset_ = sizeof(RowHeader);
		latestParam_.rowIdOffset_ = latestParam_.rowDataOffset_ +
		   container->getColumnInfo(ColumnInfo::ROW_KEY_COLUMN_ID)
		   .getColumnOffset();
		latestParam_.rowHeaderOffset_ = TIM_ROW_HEADER_OFFSET;
	} else {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_TYPE_INVALID, "");
	}

	latestParam_.varColumnNum_ = container->getVariableColumnNum();
	latestParam_.varHeaderSize_ = ValueProcessor::getEncodedVarSize(latestParam_.varColumnNum_);
	latestParam_.columnNum_ = container->getColumnNum();
	latestParam_.nullOffsetDiff_ = 0;
	latestParam_.columnOffsetDiff_ = 0;

	latestParam_.rowSize_ = container->getRowSize();
	latestParam_.rowFixedColumnSize_ = container->getRowFixedColumnSize();
	latestParam_.nullbitsSize_ = container->getNullbitsSize();
	latestParam_.nullsOffset_ = latestParam_.rowSize_ - (latestParam_.nullbitsSize_ + latestParam_.rowFixedColumnSize_);

	typedef BaseContainer::RowArrayImpl<BaseContainer, BaseContainer::ROW_ARRAY_GENERAL> GeneralRowArrayImpl;
	typedef BaseContainer::RowArrayImpl<BaseContainer, BaseContainer::ROW_ARRAY_PLAIN> PlainRowArrayImpl;

	util::StackAllocator &alloc = txn.getDefaultAllocator();
	GeneralRowArrayImpl *generalImpl = ALLOC_NEW(alloc) GeneralRowArrayImpl(
		txn, container, rowArrayStorage_, rowCache_, latestParam_);
	PlainRowArrayImpl *plainImpl = reinterpret_cast<PlainRowArrayImpl *>(generalImpl);
	rowArrayImplList_.push_back(generalImpl);
	rowArrayImplList_.push_back(plainImpl);
	defaultImpl_ = getImpl<BaseContainer, BaseContainer::ROW_ARRAY_GENERAL>();
}


/*!
	@brief Allocate RowArrayImpl Object
*/
void BaseContainer::RowArray::initialize(
	TransactionContext &txn, RowId baseRowId, uint16_t maxRowNum) {
	getDefaultImpl()->initialize(txn, baseRowId, maxRowNum);
}

/*!
	@brief Free Objects related to RowArrayImpl
*/
void BaseContainer::RowArray::finalize(TransactionContext &txn) {
	getDefaultImpl()->finalize(txn);
}

/*!
	@brief Append Row to current cursor
*/
void BaseContainer::RowArray::append(
	TransactionContext &txn, MessageRowStore *messageRowStore, RowId rowId) {
	getDefaultImpl()->append(txn, messageRowStore, rowId);
}

/*!
	@brief Insert Row to current cursor
*/
void BaseContainer::RowArray::insert(
	TransactionContext &txn, MessageRowStore *messageRowStore, RowId rowId) {
	getDefaultImpl()->insert(txn, messageRowStore, rowId);
}

/*!
	@brief Update Row on current cursor
*/
void BaseContainer::RowArray::update(
	TransactionContext &txn, MessageRowStore *messageRowStore) {
	getDefaultImpl()->update(txn, messageRowStore);
}

/*!
	@brief Delete this Row on current cursor
*/
void BaseContainer::RowArray::remove(TransactionContext &txn) {
	getDefaultImpl()->remove(txn);
}

/*!
	@brief Move Row on current cursor to Another RowArrayImpl
*/
void BaseContainer::RowArray::move(TransactionContext &txn, RowArray &dest) {
	getDefaultImpl()->move(txn, *dest.getDefaultImpl());
}

/*!
	@brief Copy Row on current cursor to Another RowArrayImpl
*/
void BaseContainer::RowArray::copy(TransactionContext &txn, RowArray &dest) {
	getDefaultImpl()->copy(txn, *dest.getDefaultImpl());
}

/*!
	@brief Update stats of nullbits
*/
void BaseContainer::RowArray::updateNullsStats(const uint8_t *nullbits) {
	getDefaultImpl()->updateNullsStats(nullbits);
}

void BaseContainer::RowArray::copyRowArray(
	TransactionContext &txn, RowArray &dest) {
	getDefaultImpl()->copyRowArray(txn, *dest.getDefaultImpl());
}

void BaseContainer::RowArray::moveRowArray(TransactionContext &txn) {
	getDefaultImpl()->moveRowArray(txn);
}

/*!
	@brief Move to next RowArrayImpl, and Check if RowArrayImpl exists
*/
bool BaseContainer::RowArray::nextRowArray(
	TransactionContext &txn, RowArray &neighbor, bool &isOldSchema, uint8_t getOption) {
	return getDefaultImpl()->nextRowArray(txn, *neighbor.getDefaultImpl(), isOldSchema, getOption);
}

/*!
	@brief Move to prev RowArrayImpl, and Check if RowArrayImpl exists
*/
bool BaseContainer::RowArray::prevRowArray(
	TransactionContext &txn, RowArray &neighbor, bool &isOldSchema, uint8_t getOption) {
	return getDefaultImpl()->prevRowArray(txn, *neighbor.getDefaultImpl(), isOldSchema, getOption);
}

/*!
	@brief Search Row corresponding to RowId
*/
bool BaseContainer::RowArray::searchRowId(RowId rowId) {
	return getDefaultImpl()->searchRowId(rowId);
}

bool BaseContainer::RowArray::searchNextRowId(RowId rowId) {
	return getDefaultImpl()->searchNextRowId(rowId);
}

bool BaseContainer::RowArray::searchPrevRowId(RowId rowId) {
	return getDefaultImpl()->searchPrevRowId(rowId);
}

/*!
	@brief Lock this RowArrayImpl
*/
void BaseContainer::RowArray::lock(TransactionContext &txn) {
	getDefaultImpl()->lock(txn);
}


/*!
	@brief Shift Rows to next position
*/
void BaseContainer::RowArray::shift(TransactionContext &txn, bool isForce,
	util::XArray<std::pair<OId, OId> > &moveList) {
	getDefaultImpl()->shift(txn, isForce, moveList);
}

/*!
	@brief Split this RowArrayImpl
*/
void BaseContainer::RowArray::split(TransactionContext &txn, RowId insertRowId,
	RowArray &splitRowArray, RowId splitRowId,
	util::XArray<std::pair<OId, OId> > &moveList) {
	getDefaultImpl()->split(txn, insertRowId, *splitRowArray.getDefaultImpl(), splitRowId, moveList);
}

/*!
	@brief Merge this RowArrayImpl and another RowArrayImpl
*/
void BaseContainer::RowArray::merge(TransactionContext &txn,
	RowArray &nextRowArray, util::XArray<std::pair<OId, OId> > &moveList) {
	getDefaultImpl()->merge(txn, *nextRowArray.getDefaultImpl(), moveList);
}


uint8_t *BaseContainer::RowArray::getNullsStats() {
	return getDefaultImpl()->getNullsStats();
}
uint32_t BaseContainer::RowArray::getNullbitsSize() const {
	return getDefaultImpl()->getNullbitsSize();
}

void BaseContainer::RowArray::setContainerId(ContainerId containerId) {
	getDefaultImpl()->setContainerId(containerId);
}
ContainerId BaseContainer::RowArray::getContainerId() const {
	return getDefaultImpl()->getContainerId();
}

void BaseContainer::RowArray::setColumnNum(uint16_t columnNum) {
	getDefaultImpl()->setColumnNum(columnNum);
}
uint16_t BaseContainer::RowArray::getColumnNum() const {
	return getDefaultImpl()->getColumnNum();
}

void BaseContainer::RowArray::setVarColumnNum(uint16_t columnNum) {
	getDefaultImpl()->setVarColumnNum(columnNum);
}
uint16_t BaseContainer::RowArray::getVarColumnNum() const {
	return getDefaultImpl()->getVarColumnNum();
}
void BaseContainer::RowArray::setRowFixedColumnSize(uint16_t fixedSize) {
	getDefaultImpl()->setRowFixedColumnSize(fixedSize);
}
uint16_t BaseContainer::RowArray::getRowFixedColumnSize() const {
	return getDefaultImpl()->getRowFixedColumnSize();
}


/*!
	@brief Set flag that this RowArray is already updated in the
   transaction
*/
void BaseContainer::RowArray::setFirstUpdate() {
	getDefaultImpl()->setFirstUpdate();
}

/*!
	@brief Reset flag that this RowArray is already updated in the
   transaction
*/
void BaseContainer::RowArray::resetFirstUpdate() {
	getDefaultImpl()->resetFirstUpdate();
}
/*!
	@brief Check if this RowArray is already updated in the transaction
*/
bool BaseContainer::RowArray::isFirstUpdate() const {
	return getDefaultImpl()->isFirstUpdate();
}

uint32_t BaseContainer::RowArray::calcHeaderSize(uint32_t nullbitsSize) {
	return BaseContainer::RowArrayImpl<BaseContainer, BaseContainer::ROW_ARRAY_GENERAL>::calcHeaderSize(nullbitsSize);
}

TransactionId BaseContainer::RowArray::getTxnId() const {
	return getDefaultImpl()->getTxnId();
}

bool BaseContainer::RowArray::setDirty(TransactionContext &txn) {
	return getDefaultImpl()->setDirty(txn);
}
void BaseContainer::RowArray::reset() {
	rowArrayStorage_.reset();
}

bool BaseContainer::RowArray::isLatestSchema() const {
	return getDefaultImpl()->isLatestSchema();
}

bool BaseContainer::RowArray::convertSchema(TransactionContext &txn,
	util::XArray< std::pair<RowId, OId> > &splitRAList,
	util::XArray< std::pair<OId, OId> > &moveOIdList) {
	return getDefaultImpl()->convertSchema(txn, splitRAList, moveOIdList);
}

/*!
	@brief Initialize the area in Row
*/
void BaseContainer::RowArray::Row::initialize() {
	rowArrayCursor_->getDefaultImpl()->getRowCursor().initialize();
}

/*!
	@brief Free Objects related to Row
*/
void BaseContainer::RowArray::Row::finalize(TransactionContext &txn) {
	rowArrayCursor_->getDefaultImpl()->getRowCursor().finalize(txn);
}


/*!
	@brief Set field values to RowArrayImpl Object
*/
void BaseContainer::RowArray::Row::setFields(
	TransactionContext &txn, MessageRowStore *messageRowStore) {
	rowArrayCursor_->getDefaultImpl()->getRowCursor().setFields(txn, messageRowStore);
}

/*!
	@brief Updates field values on RowArrayImpl Object
*/
void BaseContainer::RowArray::Row::updateFields(
	TransactionContext &txn, MessageRowStore *messageRowStore) {
	rowArrayCursor_->getDefaultImpl()->getRowCursor().updateFields(txn, messageRowStore);
}

/*!
	@brief Get field value
*/
void BaseContainer::RowArray::Row::getField(TransactionContext &txn,
	const ColumnInfo &columnInfo, BaseObject &baseObject) {
	rowArrayCursor_->getDefaultImpl()->getRowCursor().getField(txn, columnInfo, baseObject);
}

/*!
	@brief Get field value
*/
void BaseContainer::RowArray::Row::getField(TransactionContext &txn,
	const ColumnInfo &columnInfo, ContainerValue &containerValue) {
	rowArrayCursor_->getDefaultImpl()->getRowCursor().getField(txn, columnInfo, containerValue);
}

/*!
	@brief Delete this Row
*/
void BaseContainer::RowArray::Row::remove(TransactionContext &txn) {
	rowArrayCursor_->getDefaultImpl()->getRowCursor().remove(txn);
}

/*!
	@brief Move this Row to another RowArrayImpl
*/
void BaseContainer::RowArray::Row::move(
	TransactionContext &txn, Row &dest) {
	rowArrayCursor_->getDefaultImpl()->getRowCursor().move(txn, dest.getRowArray()->getDefaultImpl()->getRowCursor());
}

/*!
	@brief Copy this Row to another RowArrayImpl
*/
void BaseContainer::RowArray::Row::copy(
	TransactionContext &txn, Row &dest) {
	rowArrayCursor_->getDefaultImpl()->getRowCursor().copy(txn, dest.getRowArray()->getDefaultImpl()->getRowCursor());
}

/*!
	@brief Lock this Row
*/
void BaseContainer::RowArray::Row::lock(TransactionContext &txn) {
	rowArrayCursor_->getDefaultImpl()->getRowCursor().lock(txn);
}

/*!
	@brief translate into Message format
*/
void BaseContainer::RowArray::Row::getImage(TransactionContext &txn,
	MessageRowStore *messageRowStore, bool isWithRowId) {
	rowArrayCursor_->getDefaultImpl()->getRowCursor().getImage(txn, messageRowStore, isWithRowId);
}

/*!
	@brief translate the field into Message format
*/
void BaseContainer::RowArray::Row::getFieldImage(TransactionContext &txn,
	ColumnInfo &columnInfo, uint32_t newColumnId,
	MessageRowStore *messageRowStore) {
	rowArrayCursor_->getDefaultImpl()->getRowCursor().getFieldImage(txn, columnInfo, newColumnId, messageRowStore);
}

std::string BaseContainer::RowArray::dump(TransactionContext &txn) {
	return getDefaultImpl()->dump(txn);
}

bool BaseContainer::RowArray::validate() {
	return getDefaultImpl()->validate();
}

std::string BaseContainer::RowArray::Row::dump(TransactionContext &txn) {
	return rowArrayCursor_->getDefaultImpl()->getRowCursor().dump(txn);
}




BaseContainer::RowCache::RowCache(TransactionContext &txn, BaseContainer *container)
	: fieldCacheList_(txn.getDefaultAllocator()) {
	ObjectManagerV4 &objectManager = *(container->getObjectManager());
	new (frontFieldCache_.addr())
			FieldCache(objectManager, container->getRowAllocateStrategy());

	const uint32_t varCount = container->getVariableColumnNum();

	lastCachedField_ = 0;

	if (varCount > 0) {
		assert(fieldCacheList_.empty());
		fieldCacheList_.resize(varCount);
		for (uint32_t i = varCount; i > 0; i--) {
			void *addr = &fieldCacheList_[i - 1];
			new (addr) FieldCache(objectManager, container->getRowAllocateStrategy());
		}
	}
}

BaseContainer::RowCache::~RowCache() {
	frontFieldCache_.get()->~FieldCache();

	for (FieldCacheList::iterator it = fieldCacheList_.begin(); it != fieldCacheList_.end(); ++it) {
		it->~FieldCache();
	}
	fieldCacheList_.clear();
}

