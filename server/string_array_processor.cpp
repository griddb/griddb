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
	@brief Implementation of StringArrayProcessor
*/
#include "string_array_processor.h"
#include "util/time.h"
#include "gs_error.h"
#include "message_row_store.h"
#include "object_manager.h"
#include "schema.h"
#include "value_operator.h"



/*!
	@brief Compare message field value with object field value
*/
int32_t StringArrayProcessor::compare(TransactionContext &txn,
	ObjectManager &objectManager, ColumnId columnId,
	MessageRowStore *messageRowStore, uint8_t *objectRowField) {
	const uint8_t *inputField;
	uint32_t inputFieldSize;
	messageRowStore->getField(columnId, inputField, inputFieldSize);

	VariableArrayCursor inputArrayCursor(const_cast<uint8_t *>(inputField));
	uint32_t inputArrayLength = inputArrayCursor.getArrayLength();

	uint32_t targetArrayLength = 0;

	MatrixCursor *strArrayObject =
		reinterpret_cast<MatrixCursor *>(objectRowField);
	assert(strArrayObject);
	OId headerOId = strArrayObject->getHeaderOId();

	int32_t result = 0;
	if (headerOId != UNDEF_OID) {
		VariableArrayCursor targetArrayCursor(
			txn, objectManager, headerOId, OBJECT_READ_ONLY);
		targetArrayLength = targetArrayCursor.getArrayLength();

		if (inputArrayLength < targetArrayLength) {
			result = -1;
		}
		else if (inputArrayLength > targetArrayLength) {
			result = 1;
		}
		else {
			uint32_t inputElemSize;
			uint32_t inputCount;
			uint8_t *targetElem;
			uint32_t targetElemSize;
			uint32_t targetCount;

			result = 0;
			while (inputArrayCursor.nextElement()) {
				inputArrayCursor.getElement(inputElemSize, inputCount);
				if (targetArrayCursor.nextElement()) {
					targetElem = targetArrayCursor.getElement(
						targetElemSize, targetCount);
				}
				else {
					targetElem = NULL;
					targetElemSize = 0;
				}
				result = compareStringString(txn, inputField, inputFieldSize,
					targetElem, targetElemSize);
				if (result != 0) {
					break;
				}
			}
		}
	}
	else {
		result = (inputArrayLength > 0) ? 1 : 0;
	}
	return result;
}

/*!
	@brief Compare object field values
*/
int32_t StringArrayProcessor::compare(TransactionContext &txn,
	ObjectManager &objectManager, ColumnType, uint8_t *srcObjectRowField,
	uint8_t *targetObjectRowField) {
	MatrixCursor *srcArrayObject =
		reinterpret_cast<MatrixCursor *>(srcObjectRowField);
	assert(srcArrayObject);
	OId srcHeaderOId = srcArrayObject->getHeaderOId();

	MatrixCursor *targetArrayObject =
		reinterpret_cast<MatrixCursor *>(targetObjectRowField);
	assert(targetArrayObject);
	OId targetHeaderOId = targetArrayObject->getHeaderOId();

	uint32_t srcArrayLength = 0;
	uint32_t targetArrayLength = 0;

	int32_t result = 0;
	if (srcHeaderOId != UNDEF_OID) {
		VariableArrayCursor srcArrayCursor(
			txn, objectManager, srcHeaderOId, OBJECT_READ_ONLY);
		srcArrayLength = srcArrayCursor.getArrayLength();

		if (targetHeaderOId != UNDEF_OID) {
			VariableArrayCursor targetArrayCursor(
				txn, objectManager, targetHeaderOId, OBJECT_READ_ONLY);
			targetArrayLength = targetArrayCursor.getArrayLength();

			if (srcArrayLength < targetArrayLength) {
				result = -1;
			}
			else if (srcArrayLength > targetArrayLength) {
				result = 1;
			}
			else {
				uint8_t *srcElem;
				uint32_t srcElemSize;
				uint32_t srcCount;
				uint8_t *targetElem;
				uint32_t targetElemSize;
				uint32_t targetCount;

				result = 0;
				while (srcArrayCursor.nextElement()) {
					srcElem = srcArrayCursor.getElement(srcElemSize, srcCount);
					if (targetArrayCursor.nextElement()) {
						targetElem = targetArrayCursor.getElement(
							targetElemSize, targetCount);
					}
					else {
						targetElem = NULL;
						targetElemSize = 0;
					}
					result = compareStringString(
						txn, srcElem, srcElemSize, targetElem, targetElemSize);
					if (result != 0) {
						break;
					}
				}
			}
		}
		else {
			result = (srcArrayLength > 0) ? 1 : 0;
		}
	}
	else {
		if (targetHeaderOId != UNDEF_OID) {
			VariableArrayCursor targetArrayCursor(
				txn, objectManager, targetHeaderOId, OBJECT_READ_ONLY);
			targetArrayLength = targetArrayCursor.getArrayLength();
			result = (targetArrayLength > 0) ? -1 : 0;
		}
		else {
			result = 0;
		}
	}
	return result;
}

/*!
	@brief Set field value to message
*/
void StringArrayProcessor::getField(TransactionContext &txn,
	ObjectManager &objectManager, ColumnId columnId, const Value *objectValue,
	MessageRowStore *messageRowStore) {

	const MatrixCursor *arrayObject =
		reinterpret_cast<const MatrixCursor *>(objectValue->data());
	if (arrayObject != NULL) {
		uint32_t stringArraySize = arrayObject->getTotalSize();
		messageRowStore->setVarDataHeaderField(columnId, stringArraySize);

		uint32_t copySize = 0;
		assert(objectValue->onDataStore());
		const OId headerOId = arrayObject->getHeaderOId();
		if (headerOId != UNDEF_OID) {
			VariableArrayCursor arrayCursor(
				txn, objectManager, headerOId, OBJECT_READ_ONLY);
			uint32_t arrayLength = arrayCursor.getArrayLength();
			messageRowStore->setVarSize(arrayLength);
			copySize += ValueProcessor::getEncodedVarSize(arrayLength);

			uint8_t *srcElem;
			uint32_t elemSize;
			uint32_t elemCount;
			while (arrayCursor.nextElement()) {  
				srcElem = arrayCursor.getElement(elemSize, elemCount);
				messageRowStore->addVariableFieldPart(srcElem,
					elemSize + ValueProcessor::getEncodedVarSize(elemSize));
				copySize +=
					elemSize + ValueProcessor::getEncodedVarSize(elemSize);
			}
		}
		assert(copySize == stringArraySize);
	}
	else {
		messageRowStore->setVarDataHeaderField(
			columnId, 1);  
		messageRowStore->setVarSize(0);  
	}
}

/*!
	@brief Clone field value
*/
void StringArrayProcessor::clone(TransactionContext &txn,
	ObjectManager &objectManager, ColumnType, const void *srcObjectField,
	void *destObjectField, const AllocateStrategy &allocateStrategy,
	OId neighborOId) {
	const MatrixCursor *srcArrayObject =
		reinterpret_cast<const MatrixCursor *>(srcObjectField);
	assert(srcArrayObject);
	OId srcHeaderOId = srcArrayObject->getHeaderOId();
	uint32_t srcTotalSize = srcArrayObject->getTotalSize();

	OId linkOId = UNDEF_OID;
	if (srcHeaderOId != UNDEF_OID) {
		VariableArrayCursor srcArrayCursor(
			txn, objectManager, srcHeaderOId, OBJECT_READ_ONLY);
		linkOId = srcArrayCursor.clone(txn, allocateStrategy, neighborOId);
	}
	else {
		linkOId = UNDEF_OID;
	}
	MatrixCursor::setVariableDataInfo(destObjectField, linkOId, srcTotalSize);
}

/*!
	@brief Remove field value
*/
void StringArrayProcessor::remove(TransactionContext &txn,
	ObjectManager &objectManager, ColumnType, uint8_t *objectField) {

	uint8_t *addr = objectField;
	uint32_t varDataElemSize = ValueProcessor::decodeVarSize(addr);
	addr += ValueProcessor::getEncodedVarSize(varDataElemSize);
	assert(varDataElemSize == LINK_VARIABLE_COLUMN_DATA_SIZE);
	addr += sizeof(uint32_t);
	OId *oId = reinterpret_cast<OId *>(addr);
	if (*oId != UNDEF_OID) {
		VariableArrayCursor variableArrayCursor(txn, objectManager, *oId, OBJECT_FOR_UPDATE);
		variableArrayCursor.finalize();
		*oId = UNDEF_OID;
	}
}

/*!
	@brief Set field value to object
*/
OId StringArrayProcessor::putToObject(TransactionContext &txn,
	ObjectManager &objectManager, const uint8_t *srcAddr, uint32_t size,
	const AllocateStrategy &allocateStrategy, OId neighborOId) {

	const uint32_t MAX_STRING_ARRAY_OBJECT_SIZE =
		objectManager.getMaxObjectSize() - 4 - 8 - 8;  
	OId headerOId = UNDEF_OID;
	OId currentNeighborOId = neighborOId;
	uint32_t remain = size;
	srcAddr += ValueProcessor::getEncodedVarSize(size);
	util::XArray<uint32_t> accumulateSizeList(txn.getDefaultAllocator());
	VariableArrayCursor variableArrayCursor(const_cast<uint8_t *>(srcAddr));
	uint32_t currentObjectSize = ValueProcessor::getEncodedVarSize(
			variableArrayCursor.getArrayLength());
	const uint8_t *copyStartAddr = srcAddr;
	BaseObject prevDestObj(txn.getPartitionId(), objectManager);
	if (remain <= MAX_STRING_ARRAY_OBJECT_SIZE) {

		while (variableArrayCursor.nextElement()) {
			uint32_t elemSize;
			uint32_t elemNth;
			variableArrayCursor.getElement(elemSize, elemNth);
			currentObjectSize +=
				elemSize + ValueProcessor::getEncodedVarSize(elemSize);
			accumulateSizeList.push_back(currentObjectSize);
		}
	}
	else {
		while (variableArrayCursor.nextElement()) {
			uint32_t elemSize;
			uint32_t elemNth;
			variableArrayCursor.getElement(elemSize, elemNth);
			for (size_t checkCount = 0; 
				(currentObjectSize + elemSize +
					ValueProcessor::getEncodedVarSize(elemSize) +
					NEXT_OBJECT_LINK_INFO_SIZE) >
				MAX_STRING_ARRAY_OBJECT_SIZE;
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

				uint32_t allocateSize =
					dividedObjectSize + NEXT_OBJECT_LINK_INFO_SIZE;
				OId targetOId = UNDEF_OID;
				BaseObject destObj(txn.getPartitionId(), objectManager);
				if (currentNeighborOId == UNDEF_OID) {
					destObj.allocate<uint8_t>(allocateSize, allocateStrategy,
						targetOId, OBJECT_TYPE_ROW);
				}
				else {
					destObj.allocateNeighbor<uint8_t>(allocateSize,
						allocateStrategy, targetOId, currentNeighborOId,
						OBJECT_TYPE_ROW);
				}
				currentNeighborOId = targetOId;  
				memcpy(destObj.getBaseAddr(), copyStartAddr, dividedObjectSize);
				remain -= dividedObjectSize;
				copyStartAddr += dividedObjectSize;

				if (headerOId == UNDEF_OID) {
					headerOId = targetOId;
				}
				else {
					uint64_t encodedOId =
						ValueProcessor::encodeVarSizeOId(targetOId);
					memcpy(prevDestObj.getCursor<uint8_t>(), &encodedOId,
						sizeof(uint64_t));
				}
				prevDestObj.copyReference(destObj);
				prevDestObj.moveCursor(dividedObjectSize);
				currentObjectSize = currentObjectSize - dividedObjectSize;
				accumulateSizeList.erase(accumulateSizeList.begin(), accumulateSizeList.end() - (elemNth - 1 - dividedElemNth));
			}
			currentObjectSize +=
				elemSize + ValueProcessor::getEncodedVarSize(elemSize);
			accumulateSizeList.push_back(currentObjectSize);
		}
	}
	Size_t estimateAllocateSize =
		objectManager.estimateAllocateSize(currentObjectSize);
	if (VariableArrayCursor::divisionThreshold(currentObjectSize) < estimateAllocateSize) {
		uint32_t dividedObjectSize = currentObjectSize;
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
				break;
			}
		}
		if (dividedObjectSize != currentObjectSize) {
			uint32_t allocateSize =
				dividedObjectSize + NEXT_OBJECT_LINK_INFO_SIZE;
			OId targetOId = UNDEF_OID;
			BaseObject destObj(txn.getPartitionId(), objectManager);
			if (currentNeighborOId == UNDEF_OID) {
				destObj.allocate<uint8_t>(allocateSize, allocateStrategy,
					targetOId, OBJECT_TYPE_ROW);
			}
			else {
				destObj.allocateNeighbor<uint8_t>(allocateSize,
					allocateStrategy, targetOId, currentNeighborOId,
					OBJECT_TYPE_ROW);
			}
			currentNeighborOId = targetOId;  
			memcpy(destObj.getBaseAddr(), copyStartAddr, dividedObjectSize);
			remain -= dividedObjectSize;
			copyStartAddr += dividedObjectSize;

			if (headerOId == UNDEF_OID) {
				headerOId = targetOId;
			}
			else {
				uint64_t encodedOId =
					ValueProcessor::encodeVarSizeOId(targetOId);
				memcpy(prevDestObj.getCursor<uint8_t>(), &encodedOId,
					sizeof(uint64_t));
			}
			prevDestObj.copyReference(destObj);
			prevDestObj.moveCursor(dividedObjectSize);
			currentObjectSize = currentObjectSize - dividedObjectSize;
		}
	}

	assert(currentObjectSize <= MAX_STRING_ARRAY_OBJECT_SIZE);
	OId targetOId = UNDEF_OID;
	BaseObject destObj(txn.getPartitionId(), objectManager);
	if (currentNeighborOId == UNDEF_OID) {
		destObj.allocate<uint8_t>(
			size, allocateStrategy, headerOId, OBJECT_TYPE_ROW);
	}
	else {
		destObj.allocateNeighbor<uint8_t>(currentObjectSize, allocateStrategy,
			targetOId, currentNeighborOId, OBJECT_TYPE_ROW);
	}
	memcpy(destObj.getBaseAddr(), copyStartAddr, currentObjectSize);
	remain -= currentObjectSize;
	if (headerOId == UNDEF_OID) {
		headerOId = targetOId;
	}
	else {
		uint64_t encodedOId = ValueProcessor::encodeVarSizeOId(targetOId);
		memcpy(prevDestObj.getCursor<uint8_t>(), &encodedOId,
			sizeof(uint64_t));
	}
	assert(remain == 0);
	return headerOId;
}
