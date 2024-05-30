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
	@brief Implementation of BlobProcessor
*/
#include "blob_processor.h"
#include "util/time.h"
#include "gs_error.h"
#include "message_row_store.h"
#include "schema.h"
#include "value_operator.h"

const double LogDevide::EFFICIENCY_THRESHOLD = 1 / (1 / static_cast<double>(1 << MAX_DIVIDED_NUM));

void LogDevide::initialize(uint64_t inputSize) {
	uint32_t restSize;
	if (inputSize > blobSubBlockUnitSize_) {
		constElemNum_ = static_cast<uint32_t>(inputSize / blobSubBlockUnitSize_);
		restSize = static_cast<uint32_t>(inputSize - (constElemNum_ * blobSubBlockUnitSize_));
	} else {
		restSize = static_cast<uint32_t>(inputSize);
	}
	if (restSize > 0) {
		if (restSize <= DIVIDED_SIZE_LIMIT) {
			sizeList_[dividedElemNum_++] = restSize;
		} else {
			initializeDevide(restSize);
		}
	}
}

void LogDevide::initializeDevide(uint32_t restSize) {
	uint32_t sizeOfBuddy = calcSizeOfBuddy(restSize);

	uint32_t halfSize = (sizeOfBuddy >> 1) - ObjectManagerV4::OBJECT_HEADER_SIZE;
	uint32_t quarterSize = (sizeOfBuddy >> 2) - ObjectManagerV4::OBJECT_HEADER_SIZE;
	uint32_t oneEightSize = (sizeOfBuddy >> 3) - ObjectManagerV4::OBJECT_HEADER_SIZE;
	uint32_t oneSixteenSize = (sizeOfBuddy >> 4) - ObjectManagerV4::OBJECT_HEADER_SIZE;
	if (restSize > halfSize + quarterSize) {
		if (restSize > halfSize + quarterSize + oneEightSize) {
			sizeList_[dividedElemNum_++] = restSize;
		}
		else {
			sizeList_[dividedElemNum_++] = halfSize;
			sizeList_[dividedElemNum_++] = quarterSize;
			sizeList_[dividedElemNum_++] = restSize - halfSize - quarterSize;
		}
	}
	else if (restSize > halfSize + oneEightSize) {
		if (restSize > halfSize + oneEightSize + oneSixteenSize) {
			sizeList_[dividedElemNum_++] = halfSize;
			sizeList_[dividedElemNum_++] = restSize - halfSize;
		}
		else {
			sizeList_[dividedElemNum_++] = halfSize;
			sizeList_[dividedElemNum_++] = oneEightSize;
			sizeList_[dividedElemNum_++] = restSize - halfSize - oneEightSize;
		}
	}
	else {
		sizeList_[dividedElemNum_++] = halfSize;
		sizeList_[dividedElemNum_++] = restSize - halfSize;
	}
}

BlobCursor::BlobCursor(ObjectManagerV4 &objectManager, AllocateStrategy &allocateStrategy,
					   const uint8_t * const ptr)
					   : objectManager_(objectManager),
					   allocateStrategy_(allocateStrategy),
					   baseAddr_(ptr), topArrayAddr_(NULL),
					   curObj_(objectManager, allocateStrategy_), arrayCursor_(NULL),
					   currentElem_(-1), maxElem_(0), currentDepth_(0),
					   maxDepth_(0),  logDevide_(objectManager), neighborOId_(UNDEF_OID) {
	for (uint32_t i = 0; i < MAX_DEPTH; i++) {
		stackCursor_[i].reset(objectManager, allocateStrategy_);
	}
	uint32_t headerLen = ValueProcessor::getEncodedVarSize(baseAddr_);  
	uint64_t size = ValueProcessor::decodeVarSize64(baseAddr_ + headerLen);  
	if (size != 0) {
		uint32_t blobSizeLen = ValueProcessor::getEncodedVarSize(baseAddr_ + headerLen);
		maxElem_ = ValueProcessor::decodeVarSize(baseAddr_ + headerLen + blobSizeLen);  
		uint32_t maxElemLen = ValueProcessor::getEncodedVarSize(baseAddr_ + headerLen + blobSizeLen);
		maxDepth_ = ValueProcessor::decodeVarSize(baseAddr_ + headerLen + blobSizeLen + maxElemLen);  
		uint32_t maxDepthLen = ValueProcessor::getEncodedVarSize(baseAddr_ + headerLen + blobSizeLen + maxElemLen);
		topArrayAddr_ = baseAddr_ + headerLen + blobSizeLen + maxElemLen + maxDepthLen;  
	}
}

BlobCursor::BlobCursor(ObjectManagerV4 &objectManager,
					   AllocateStrategy &allocateStrategy,
					   const uint8_t *ptr, OId neighborOId)
					   : objectManager_(objectManager),
					   allocateStrategy_(allocateStrategy),
					   baseAddr_(ptr), topArrayAddr_(NULL),
					   curObj_(objectManager, allocateStrategy_), arrayCursor_(NULL),
					   currentElem_(-1), maxElem_(0), currentDepth_(0),
					   maxDepth_(0), logDevide_(objectManager), neighborOId_(neighborOId) {
	for (uint32_t i = 0; i < MAX_DEPTH; i++) {
		stackCursor_[i].reset(objectManager, allocateStrategy_);
	}
}


uint32_t BlobCursor::getPrefixDataSize(ObjectManagerV4 &objectManager, uint64_t totalSize) {
	LogDevide logDevide(objectManager);
	logDevide.initialize(totalSize);
	uint32_t elemNum = logDevide.getElemNum();
	uint32_t topArrayNum = 0;
	uint32_t depth = calcDepth(objectManager, totalSize, elemNum, topArrayNum);
	uint32_t encodeBlobSizeLen = ValueProcessor::getEncodedVarSize(totalSize);
	uint32_t encodeElemNumSizeLen = 0, encodeDepthLen = 0, topArraySize = 0;
	if (totalSize != 0) {
		encodeElemNumSizeLen = ValueProcessor::getEncodedVarSize(elemNum);
		encodeDepthLen = ValueProcessor::getEncodedVarSize(depth);
		if (isDivided(depth)) {
			topArraySize = BlobArrayObject::getObjectSize(topArrayNum);
		} else {
			topArraySize = static_cast<uint32_t>(totalSize);
		}
	}

	return encodeBlobSizeLen + encodeElemNumSizeLen + encodeDepthLen + topArraySize;
}

uint32_t BlobCursor::getMaxArrayNum(ObjectManagerV4 &objectManager) {
	uint32_t headerSize = sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint64_t);
	uint32_t allocateSize = objectManager.getRecommendedLimitObjectSize() - headerSize;
	return static_cast<uint32_t>(allocateSize / sizeof(BlobArrayElement));
}

uint32_t BlobCursor::calcDepth(ObjectManagerV4 &objectManager, uint64_t totalSize, uint32_t elemNum, uint32_t &topArrayNum) {
	uint32_t depth = 0;
	if (totalSize >= MIN_DIVIDED_SIZE) {
		depth++;
		topArrayNum = elemNum;
		uint32_t maxArrayNum = getMaxArrayNum(objectManager);
		while (topArrayNum > maxArrayNum) {
			topArrayNum = static_cast<uint32_t>(ceil(static_cast<double>(topArrayNum) / maxArrayNum));
			depth++;
		}
	}
	return depth;
}

uint32_t BlobCursor::initialize(uint8_t *destAddr, uint64_t totalSize) {
	logDevide_.initialize(totalSize);
	maxElem_ = logDevide_.getElemNum();

	uint32_t topArrayNum = 0;
	maxDepth_ = calcDepth(objectManager_, totalSize, maxElem_, topArrayNum);

	uint32_t encodeBlobSizeLen = ValueProcessor::getEncodedVarSize(totalSize);
	uint32_t encodeElemNumSizeLen = 0, encodeDepthLen = 0, topArraySize = 0;
	if (totalSize != 0) {
		encodeElemNumSizeLen = ValueProcessor::getEncodedVarSize(maxElem_);
		encodeDepthLen = ValueProcessor::getEncodedVarSize(maxDepth_);
		if (isDivided()) {
			topArraySize = BlobArrayObject::getObjectSize(topArrayNum);
		} else {
			topArraySize = static_cast<uint32_t>(totalSize);
		}
	}
	uint32_t totalTopSize = encodeBlobSizeLen + encodeElemNumSizeLen + encodeDepthLen + topArraySize;
	uint32_t encodeTotalSizeLen = ValueProcessor::getEncodedVarSize(totalTopSize);

	uint64_t encodeTotalSize = ValueProcessor::encodeVarSize(totalTopSize);
	uint64_t encodeBlobSize = ValueProcessor::encodeVarSize(totalSize);
	memcpy(destAddr, &encodeTotalSize, encodeTotalSizeLen);
	memcpy(destAddr + encodeTotalSizeLen, &encodeBlobSize, encodeBlobSizeLen);


	if (totalSize != 0) {
		uint64_t encodeElumNum = ValueProcessor::encodeVarSize(maxElem_);
		uint64_t encodeDepth = ValueProcessor::encodeVarSize(maxDepth_);

		memcpy(destAddr + encodeTotalSizeLen + encodeBlobSizeLen, &encodeElumNum, encodeElemNumSizeLen);
		memcpy(destAddr + encodeTotalSizeLen + encodeBlobSizeLen + encodeElemNumSizeLen, &encodeDepth, encodeDepthLen);
	}
	topArrayAddr_ = destAddr + encodeTotalSizeLen + encodeBlobSizeLen + encodeElemNumSizeLen + encodeDepthLen;
	return encodeTotalSizeLen + totalTopSize;
}

void BlobCursor::finalize() {
	reset();
	if (isDivided()) {
		ChunkAccessor ca;
		while (next(REMOVE)) {
			const BlobArrayElement *element = arrayCursor_->getCurrentElement();
			objectManager_.free(ca, allocateStrategy_.getGroupId(), element->oId_);
		}
	}
}

/*!
	@brief Get variable size
*/
uint64_t BlobCursor::getTotalSize(const uint8_t *addr) {
	uint32_t prefixSize = ValueProcessor::getEncodedVarSize(addr);
	uint64_t totalSize = ValueProcessor::decodeVarSize64(addr + 
		prefixSize);
	return totalSize;
}

/*!
	@brief Get variable size
*/
uint64_t BlobCursor::getTotalSize() const {
	return getTotalSize(baseAddr_);
}

bool BlobCursor::next(CURSOR_MODE mode) {
	if (!hasNext()) {
		if (mode == REMOVE) {
			for (uint32_t i = 0; i < currentDepth_ + 1; i++) {
				stackCursor_[i].finalize();
			}
			arrayCursor_ = NULL;
		}
		return false;
	}
	currentElem_++;
	if (arrayCursor_ == NULL) {
		if (isDivided()) {
			stackCursor_[currentDepth_].setBaseAddr(const_cast<uint8_t *>(topArrayAddr_));
			arrayCursor_ = &(stackCursor_[currentDepth_]);
			if (mode == CREATE) {
				uint32_t arrayNum = calcArrayNum(currentElem_, currentDepth_);
				arrayCursor_->setArrayLength(arrayNum);
			}
			arrayCursor_->next();
			down(mode);
		} else {
			curObj_.setBaseAddr(const_cast<uint8_t *>(topArrayAddr_));
		}
	} else if (arrayCursor_->next()) {
	} else {
		nextBlock(mode);
	}
	return true;
}

void BlobCursor::nextBlock(CURSOR_MODE mode) {
	if (mode == REMOVE) {
		arrayCursor_->finalize();
	}
	bool isExist = (currentDepth_ > 0);
	while (currentDepth_ > 0) {
		currentDepth_--;
		arrayCursor_ = &(stackCursor_[currentDepth_]);
		if (arrayCursor_->next()) {
			isExist = true;
			break;
		}
		else if (mode == REMOVE) {
			arrayCursor_->finalize();
		}
	}
	UNUSED_VARIABLE(isExist);
	assert(isExist);
	down(mode);
}


void BlobCursor::down(CURSOR_MODE mode) {
	while (currentDepth_ + 1 < maxDepth_) {
		currentDepth_++;
		if (mode == CREATE) {
			uint32_t arrayNum = calcArrayNum(currentElem_, currentDepth_);
			OId newOId;
			if (neighborOId_ == UNDEF_OID) {
				stackCursor_[currentDepth_].allocate<BlobArrayObject>(
					BlobArrayObject::getObjectSize(arrayNum),
					newOId, OBJECT_TYPE_VARIANT);
			}
			else {
				stackCursor_[currentDepth_].allocateNeighbor<BlobArrayObject>(
					BlobArrayObject::getObjectSize(arrayNum),
					newOId, neighborOId_,
					OBJECT_TYPE_VARIANT);
			}
			neighborOId_ = newOId;
			stackCursor_[currentDepth_].setArrayLength(arrayNum);
			BlobArrayElement newElement(0, newOId);
			arrayCursor_->setCurrentElement(&newElement);
			arrayCursor_ = &(stackCursor_[currentDepth_]);
		} else {
			const BlobArrayElement *element = arrayCursor_->getCurrentElement();
			OId oId = element->oId_;
			arrayCursor_ = &(stackCursor_[currentDepth_]);
			if (arrayCursor_->getBaseOId() != UNDEF_OID) {
				arrayCursor_->loadNeighbor(oId, OBJECT_READ_ONLY);
			} else {
				arrayCursor_->load(oId);
			}
		}
		arrayCursor_->next();
	}
}

bool BlobCursor::hasNext() {
	return currentElem_ + 1 < maxElem_;
}

void BlobCursor::reset() {
	currentElem_ = -1;
	currentDepth_ = 0;
	arrayCursor_ = NULL;
	for (uint32_t i = 0; i < MAX_DEPTH; i++) {
		stackCursor_[i].resetArrayCursor();
	}
}

void BlobCursor::getCurrentBinary(const uint8_t *&ptr, uint32_t &size) {
	if (isDivided()) {
		const BlobArrayElement *element = arrayCursor_->getCurrentElement();
		OId oId = element->oId_;
		if (curObj_.getBaseOId() != UNDEF_OID) {
			curObj_.loadNeighbor(oId, OBJECT_READ_ONLY);
		} else {
			curObj_.load(oId, false);
		}
		size = static_cast<uint32_t>(element->size_);
		ptr = curObj_.getBaseAddr();
	} else {
		size = static_cast<uint32_t>(getTotalSize(baseAddr_));
		ptr = curObj_.getBaseAddr();
	}
}

void BlobCursor::setBinary(const uint8_t *addr, uint64_t size) {
	if (size > 0) {
		const uint8_t *currentAddr = addr;
		uint64_t restSize = size;
		while (next(CREATE)) {
			uint32_t allocSize = logDevide_.getAllocateSize(currentElem_);
			addBinary(currentAddr, allocSize);
			restSize -= allocSize;
			currentAddr += allocSize;
		}
		assert(restSize == 0);
	}
}

void BlobCursor::addBinary(const uint8_t *addr, uint32_t size) {
	if (isDivided()) {
		OId oId;
		if (neighborOId_ == UNDEF_OID) {
			curObj_.allocate<BaseObject>(
				size, oId, OBJECT_TYPE_VARIANT);
		}
		else {
			curObj_.allocateNeighbor<BaseObject>(
				size, oId, neighborOId_, OBJECT_TYPE_VARIANT);
		}
		neighborOId_ = oId;
		BlobArrayElement newElement(size, oId);
		arrayCursor_->setCurrentElement(&newElement);
		for (uint32_t i = 0; i < currentDepth_; i++) {
			const BlobArrayElement *element = stackCursor_[i].getCurrentElement();
			BlobArrayElement updateElement = BlobArrayElement(element->size_ + size, element->oId_);
			stackCursor_[i].setCurrentElement(&updateElement);
		}
	}
	memcpy(curObj_.getBaseAddr(), addr, size);
}

void BlobCursor::dump(util::NormalOStringStream &ss, bool forExport) {
	uint64_t blobSize = getTotalSize();
	if (!forExport) {
		ss << "(BLOB)length=" << blobSize << "'";
	} else {
		ss << "x'";
	}
	while (next()) {
		uint32_t srcDataSize = 0;
		const uint8_t *srcData = NULL;
		getCurrentBinary(srcData, srcDataSize);
		if (!forExport) {
			ss << ",(" << currentElem_ << ",size=" << srcDataSize << ")";
		} else {
			util::NormalIStringStream iss(
					u8string(reinterpret_cast<const char8_t*>(srcData), srcDataSize));
			util::HexConverter::encode(ss, iss);
		}
	}
	ss << "'";
}



uint8_t *BlobCursor::getBinary(util::StackAllocator &alloc) {
	uint64_t blobSize = getTotalSize();
	uint8_t *destAddr = static_cast<uint8_t *>(alloc.allocate(blobSize));
	uint8_t *current = destAddr;
	while (next()) {
		uint32_t srcDataSize = 0;
		const uint8_t *srcData = NULL;
		getCurrentBinary(srcData, srcDataSize);
		memcpy(current, srcData, srcDataSize);
		current += srcDataSize;
	}
	return destAddr;
}


/*!
	@brief Compare message field value with object field value
*/
int32_t BlobProcessor::compare(TransactionContext &txn,
	ObjectManagerV4 &objectManager, AllocateStrategy &strategy, ColumnId columnId,
	MessageRowStore *messageRowStore, uint8_t *objectRowField) {
	const uint8_t *inputField;
	uint32_t inputFieldSize;
	messageRowStore->getField(columnId, inputField, inputFieldSize);
	inputField +=
		ValueProcessor::getEncodedVarSize(inputFieldSize);  

	BlobCursor blobCursor(objectManager, strategy, objectRowField);
	while (blobCursor.next()) {
		uint32_t targetDataSize = 0;
		const uint8_t *targetData = NULL;
		blobCursor.getCurrentBinary(targetData, targetDataSize);
		uint32_t compareInputSize = 
			inputFieldSize < targetDataSize ? inputFieldSize : targetDataSize;
		int32_t result = compareBinaryBinary(
			txn, inputField, compareInputSize, targetData, targetDataSize);
		if (result != 0) {
			return result;
		}
		inputField += compareInputSize;
		inputFieldSize -= compareInputSize;
	}
	if (blobCursor.hasNext()) {
		return -1;
	} else if (inputFieldSize != 0) {
		return 1;
	} else {
		return 0;
	}
}

/*!
	@brief Compare object field values
*/
int32_t BlobProcessor::compare(TransactionContext &txn,
	ObjectManagerV4 &objectManager, AllocateStrategy &strategy, ColumnType, uint8_t *srcObjectRowField,
	uint8_t *targetObjectRowField) {

	BlobCursor srcBlobCursor(objectManager, strategy, srcObjectRowField);
	BlobCursor targetBlobCursor(objectManager, strategy, targetObjectRowField);
	uint64_t restSrcTotalSize = srcBlobCursor.getTotalSize();
	uint64_t restTargetTotalSize = targetBlobCursor.getTotalSize();

	uint32_t srcDataSize = 0, targetDataSize = 0;
	uint32_t restSrcDataSize = 0, restTargetDataSize = 0;
	const uint8_t *srcData = NULL, *targetData = NULL;
	while (restSrcTotalSize != 0 && restTargetTotalSize != 0) {
		if (restSrcDataSize == 0) {
			srcBlobCursor.next();
			srcBlobCursor.getCurrentBinary(srcData, srcDataSize);
			restSrcDataSize = srcDataSize;
		}
		if (restTargetDataSize == 0) {
			targetBlobCursor.next();
			targetBlobCursor.getCurrentBinary(targetData, targetDataSize);
			restTargetDataSize = targetDataSize;
		}
		uint32_t compareSize = 
			restSrcDataSize < restTargetDataSize ? restSrcDataSize : restTargetDataSize;

		int32_t result = compareBinaryBinary(
			txn, srcData, compareSize, targetData, compareSize);
		if (result != 0) {
			return result;
		}
		restSrcDataSize -= compareSize;
		restTargetDataSize -= compareSize;
		srcData += compareSize;
		targetData += compareSize;

		restSrcTotalSize -= compareSize;
		restTargetTotalSize -= compareSize;
	}
	if (restTargetTotalSize != 0) {
		return -1;
	} else if (restSrcTotalSize != 0) {
		return 1;
	} else {
		return 0;
	}
}

/*!
	@brief Set field value to message
*/
void BlobProcessor::getField(
		TransactionContext &txn, ObjectManagerV4 &objectManager,
		AllocateStrategy &strategy, ColumnId columnId, const Value *objectValue,
		MessageRowStore *messageRowStore) {
	UNUSED_VARIABLE(txn);

	if (objectValue->data() == NULL) {
		messageRowStore->setVarDataHeaderField(columnId, 0);
		return;
	}

	BlobCursor blobCursor(objectManager, strategy, const_cast<uint8_t *>(objectValue->data()));
	uint64_t blobSize = blobCursor.getTotalSize();
	messageRowStore->setVarDataHeaderField(columnId, static_cast<uint32_t>(blobSize));

	assert(objectValue->onDataStore());
	uint64_t destBlobSize = 0;
	while (blobCursor.next()) {
		uint32_t srcDataSize = 0;
		const uint8_t *srcData = NULL;
		blobCursor.getCurrentBinary(srcData, srcDataSize);
		messageRowStore->addVariableFieldPart(srcData, srcDataSize);
		destBlobSize += srcDataSize;
	}
	assert(blobSize == destBlobSize);
}

/*!
	@brief Clone field value
*/
void BlobProcessor::clone(
		TransactionContext &txn, ObjectManagerV4 &objectManager,
		ColumnType, const uint8_t *srcObjectField, uint8_t *destObjectField,
		AllocateStrategy &allocateStrategy, OId neighborOId) {
	UNUSED_VARIABLE(txn);

	BlobCursor srcBlobCursor(objectManager, allocateStrategy, const_cast<uint8_t *>(srcObjectField));
	BlobCursor destBlobCursor(objectManager, allocateStrategy, destObjectField, neighborOId);

	destBlobCursor.initialize(destObjectField, srcBlobCursor.getTotalSize());
	while (srcBlobCursor.next()) {
		uint32_t srcDataSize = 0;
		const uint8_t *srcData = NULL;
		srcBlobCursor.getCurrentBinary(srcData, srcDataSize);

		destBlobCursor.next(BlobCursor::CREATE);
		destBlobCursor.addBinary(srcData, srcDataSize);
	}
	assert(!srcBlobCursor.hasNext() && !destBlobCursor.hasNext());
}

/*!
	@brief Remove field value
*/
void BlobProcessor::remove(
		TransactionContext &txn, ObjectManagerV4 &objectManager,
		AllocateStrategy &strategy, ColumnType, uint8_t *objectField) {
	UNUSED_VARIABLE(txn);

	BlobCursor blobCursor(objectManager, strategy, objectField);
	blobCursor.finalize();
}

/*!
	@brief Set field value
*/
void BlobProcessor::setField(
		TransactionContext &txn,
		ObjectManagerV4 &objectManager, const uint8_t *srcAddr, uint32_t srcSize,
		uint8_t *destAddr, uint32_t &destSize,
		AllocateStrategy &allocateStrategy, OId neighborOId) {
	UNUSED_VARIABLE(txn);


	const uint8_t *currentAddr = srcAddr + 
		ValueProcessor::getEncodedVarSize(srcSize);  
	BlobCursor blobCursor(objectManager, allocateStrategy, destAddr, neighborOId);
	destSize = blobCursor.initialize(destAddr, srcSize);
	blobCursor.setBinary(currentAddr, srcSize);
}

