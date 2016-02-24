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
	@brief Implementation of BlobProcessor
*/
#include "blob_processor.h"
#include "util/time.h"
#include "gs_error.h"
#include "message_row_store.h"
#include "schema.h"
#include "value_operator.h"

static const uint32_t BLOB_SUB_BLOCK_SIZE_DEFAULT =
	128 * 1024 - OBJECT_BLOCK_HEADER_SIZE - 8;  



/*!
	@brief Compare message field value with object field value
*/
int32_t BlobProcessor::compare(TransactionContext &txn,
	ObjectManager &objectManager, ColumnId columnId,
	MessageRowStore *messageRowStore, uint8_t *objectRowField) {
	const uint8_t *inputField;
	uint32_t inputFieldSize;
	messageRowStore->getField(columnId, inputField, inputFieldSize);

	int32_t result;
	uint32_t objectRowFieldSize;

	MatrixCursor *blobObject = reinterpret_cast<MatrixCursor *>(objectRowField);
	if (blobObject != NULL) {
		util::XArray<uint8_t> target(txn.getDefaultAllocator());

		const OId headerOId = blobObject->getHeaderOId();
		if (headerOId != UNDEF_OID) {
			ArrayObject oIdArrayObject(txn, objectManager, headerOId);
			uint32_t num = oIdArrayObject.getArrayLength();
			for (uint32_t i = 0; i < num; i++) {
				const uint8_t *elemData =
					oIdArrayObject.getArrayElement(i, COLUMN_TYPE_OID);
				const OId elemOId = *reinterpret_cast<const OId *>(elemData);
				if (elemOId != UNDEF_OID) {
					BinaryObject elemObject(txn, objectManager, elemOId);
					target.push_back(elemObject.data(), elemObject.size());
				}
			}
		}
		objectRowField = target.data();
		objectRowFieldSize = static_cast<uint32_t>(target.size());
	}
	else {
		objectRowField = NULL;
		objectRowFieldSize = 0;
	}

	inputField +=
		ValueProcessor::getEncodedVarSize(inputFieldSize);  
	result = compareBinaryBinary(
		txn, inputField, inputFieldSize, objectRowField, objectRowFieldSize);
	return result;
}

/*!
	@brief Compare object field values
*/
int32_t BlobProcessor::compare(TransactionContext &txn,
	ObjectManager &objectManager, ColumnType, uint8_t *srcObjectRowField,
	uint8_t *targetObjectRowField) {
	MatrixCursor *srcBlobObject =
		reinterpret_cast<MatrixCursor *>(srcObjectRowField);
	OId srcOId = srcBlobObject->getHeaderOId();

	MatrixCursor *targetBlobObject =
		reinterpret_cast<MatrixCursor *>(targetObjectRowField);
	OId targetOId = targetBlobObject->getHeaderOId();

	if (srcOId == UNDEF_OID || targetOId == UNDEF_OID) {
		if (srcOId == UNDEF_OID && targetOId == UNDEF_OID) {
			return 0;
		}
		else if (srcOId == UNDEF_OID) {
			return -1;
		}
		else {
			return 1;
		}
	}

	ArrayObject srcVariant(txn, objectManager, srcOId);
	uint32_t srcArrayLength = srcVariant.getArrayLength();

	ArrayObject targetVariant(txn, objectManager, targetOId);
	uint32_t targetArrayLength = targetVariant.getArrayLength();

	int32_t result = 0;
	if (srcArrayLength < targetArrayLength) {
		result = -1;
	}
	else if (srcArrayLength > targetArrayLength) {
		result = 1;
	}
	else {
		result = 0;
		for (uint32_t i = 0; i < srcArrayLength; i++) {
			const uint8_t *srcOIdPtr =
				srcVariant.getArrayElement(i, COLUMN_TYPE_OID);
			BinaryObject srcElem(txn, objectManager,
				(*reinterpret_cast<const OId *>(srcOIdPtr)));
			uint32_t srcElemSize = static_cast<uint32_t>(srcElem.size());

			const uint8_t *targetOIdPtr =
				targetVariant.getArrayElement(i, COLUMN_TYPE_OID);
			BinaryObject targetElem(txn, objectManager,
				(*reinterpret_cast<const OId *>(targetOIdPtr)));
			uint32_t targetElemSize = static_cast<uint32_t>(targetElem.size());

			result = compareBinaryBinary(txn, srcElem.data(), srcElemSize,
				targetElem.data(), targetElemSize);
			if (result != 0) {
				break;
			}
		}
	}
	return result;
}

/*!
	@brief Set field value to message
*/
void BlobProcessor::getField(TransactionContext &txn,
	ObjectManager &objectManager, ColumnId columnId, Value *objectValue,
	MessageRowStore *messageRowStore) {
	const MatrixCursor *blobObject =
		reinterpret_cast<const MatrixCursor *>(objectValue->data());
	if (blobObject != NULL) {
		uint32_t blobSize = blobObject->getTotalSize();
		messageRowStore->setVarDataHeaderField(columnId, blobSize);

		bool onDataStore = objectValue->onDataStore();
		size_t destBlobSize = 0;
		if (onDataStore) {
			const OId headerOId = blobObject->getHeaderOId();
			if (headerOId != UNDEF_OID) {
				ArrayObject oIdArrayObject(txn, objectManager, headerOId);
				uint32_t num = oIdArrayObject.getArrayLength();
				for (uint32_t i = 0; i < num; i++) {
					const uint8_t *elemData =
						oIdArrayObject.getArrayElement(i, COLUMN_TYPE_OID);
					const OId elemOId =
						*reinterpret_cast<const OId *>(elemData);
					if (elemOId != UNDEF_OID) {
						BinaryObject elemObject(txn, objectManager, elemOId);
						messageRowStore->addVariableFieldPart(
							elemObject.data(), elemObject.size());
						destBlobSize += elemObject.size();
					}
				}
			}
		}
		else {
			assert(false);
		}
		assert(blobSize == destBlobSize);
	}
	else {
		messageRowStore->setVarDataHeaderField(columnId, 0);
	}
}

/*!
	@brief Clone field value
*/
void BlobProcessor::clone(TransactionContext &txn, ObjectManager &objectManager,
	ColumnType, const void *srcObjectField, void *destObjectField,
	const AllocateStrategy &allocateStrategy, OId neighborOId) {
	OId destOId;
	const MatrixCursor *srcBlobObject =
		reinterpret_cast<const MatrixCursor *>(srcObjectField);
	uint32_t totalSize = srcBlobObject->getTotalSize();
	OId srcOId = srcBlobObject->getHeaderOId();
	OId currentNeighborOId = neighborOId;
	uint32_t cloneDataSize = 0;
	if (srcOId != UNDEF_OID) {
		ArrayObject srcArray(txn, objectManager, srcOId);
		uint32_t num = srcArray.getArrayLength();

		ArrayObject destArray(txn, objectManager);
		if (currentNeighborOId == UNDEF_OID) {
			destArray.allocate<ArrayObject>(
				ArrayObject::getObjectSize(num, COLUMN_TYPE_OID),
				allocateStrategy, destOId, OBJECT_TYPE_VARIANT);
		}
		else {
			destArray.allocateNeighbor<ArrayObject>(
				ArrayObject::getObjectSize(num, COLUMN_TYPE_OID),
				allocateStrategy, destOId, currentNeighborOId,
				OBJECT_TYPE_VARIANT);
		}
		currentNeighborOId = destOId;  

		destArray.setArrayLength(num, COLUMN_TYPE_OID);

		OId destElmOId;
		for (uint32_t i = 0; i < num; i++) {
			OId srcElemOId = *reinterpret_cast<const OId *>(
				srcArray.getArrayElement(i, COLUMN_TYPE_OID));

			if (srcElemOId != UNDEF_OID) {
				BinaryObject srcElemVariant(txn, objectManager, srcElemOId);

				BinaryObject destElemVariant(txn, objectManager);
				destElemVariant.allocateNeighbor<BinaryObject>(
					BinaryObject::getObjectSize(srcElemVariant.size()),
					allocateStrategy, destElmOId, currentNeighborOId,
					OBJECT_TYPE_VARIANT);
				currentNeighborOId = destElmOId;  
				destElemVariant.setData(
					srcElemVariant.size(), srcElemVariant.data());
				cloneDataSize += srcElemVariant.size();
			}
			else {
				destElmOId = UNDEF_OID;
			}

			destArray.setArrayElement(i, COLUMN_TYPE_OID,
				reinterpret_cast<const uint8_t *>(&destElmOId));
		}
	}
	else {
		destOId = UNDEF_OID;
	}
	assert(cloneDataSize == totalSize);
	MatrixCursor::setVariableDataInfo(destObjectField, destOId, totalSize);
}

/*!
	@brief Remove field value
*/
void BlobProcessor::remove(TransactionContext &txn,
	ObjectManager &objectManager, ColumnType, uint8_t *objectField) {
	MatrixCursor *targetBlobObject =
		reinterpret_cast<MatrixCursor *>(objectField);
	OId oId = targetBlobObject->getHeaderOId();

	if (oId != UNDEF_OID) {
		ArrayObject blobArray(txn, objectManager, oId);
		uint32_t blobElemCount = blobArray.getArrayLength();

		for (uint32_t i = 0; i < blobElemCount; i++) {
			OId blobDataOId = *reinterpret_cast<const OId *>(
				blobArray.getArrayElement(i, COLUMN_TYPE_OID));
			assert(blobDataOId != UNDEF_OID);
			objectManager.free(txn.getPartitionId(), blobDataOId);
		}
		blobArray.finalize();
		targetBlobObject->setHeaderOId(UNDEF_OID);
	}
}

/*!
	@brief Set field value to object
*/
OId BlobProcessor::putToObject(TransactionContext &txn,
	ObjectManager &objectManager, const uint8_t *srcAddr, uint32_t size,
	const AllocateStrategy &allocateStrategy, OId neighborOId) {
	const uint32_t BLOB_SUB_BLOCK_SIZE_FROM_CHUNK_SIZE =
		objectManager.getMaxObjectSize() - 8;
	const uint32_t BLOB_SUB_BLOCK_SIZE = std::min(
		BLOB_SUB_BLOCK_SIZE_FROM_CHUNK_SIZE, BLOB_SUB_BLOCK_SIZE_DEFAULT);
	srcAddr += ValueProcessor::getEncodedVarSize(size);  

	OId vOId = UNDEF_OID;
	OId currentNeighborOId = neighborOId;
	if (size > 0) {
		uint32_t num = (size - 1) / BLOB_SUB_BLOCK_SIZE + 1;

		ArrayObject blobArray(txn, objectManager);
		if (currentNeighborOId == UNDEF_OID) {
			blobArray.allocate<ArrayObject>(
				ArrayObject::getObjectSize(num, COLUMN_TYPE_OID),
				allocateStrategy, vOId, OBJECT_TYPE_VARIANT);
		}
		else {
			blobArray.allocateNeighbor<ArrayObject>(
				ArrayObject::getObjectSize(num, COLUMN_TYPE_OID),
				allocateStrategy, vOId, currentNeighborOId,
				OBJECT_TYPE_VARIANT);
		}
		currentNeighborOId = vOId;  

		blobArray.setArrayLength(num, COLUMN_TYPE_OID);

		OId elmOId;

		const uint8_t *current = srcAddr;
		uint32_t restSize = size;
		for (uint32_t i = 0; i < num - 1; i++) {
			BinaryObject binary(txn, objectManager);
			binary.allocateNeighbor<BinaryObject>(
				BinaryObject::getObjectSize(BLOB_SUB_BLOCK_SIZE),
				allocateStrategy, elmOId, currentNeighborOId,
				OBJECT_TYPE_VARIANT);
			currentNeighborOId = elmOId;  
			binary.setData(BLOB_SUB_BLOCK_SIZE, current);

			blobArray.setArrayElement(
				i, COLUMN_TYPE_OID, reinterpret_cast<const uint8_t *>(&elmOId));
			restSize -= BLOB_SUB_BLOCK_SIZE;
			current += BLOB_SUB_BLOCK_SIZE;
		}
		{
			BinaryObject binary(txn, objectManager);
			binary.allocateNeighbor<BinaryObject>(
				BinaryObject::getObjectSize(restSize), allocateStrategy, elmOId,
				currentNeighborOId, OBJECT_TYPE_VARIANT);
			binary.setData(restSize, current);

			blobArray.setArrayElement(num - 1, COLUMN_TYPE_OID,
				reinterpret_cast<const uint8_t *>(&elmOId));
		}
	}
	return vOId;
}
