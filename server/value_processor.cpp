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
	@brief Implementation of ValueProcessor
*/
#include "value_processor.h"
#include "message_row_store.h"
#include "schema.h"
#include "value_operator.h"
#include "geometry_processor.h"
#include "util/time.h"
#include "array_processor.h"
#include "blob_processor.h"
#include "gs_error.h"
#include "string_array_processor.h"
#include "string_processor.h"
#include <iomanip>  

const Timestamp ValueProcessor::SUPPORT_MAX_TIMESTAMP =
		makeMaxTimestamp(TRIM_MILLISECONDS);
const MicroTimestamp ValueProcessor::SUPPORT_MAX_MICRO_TIMESTAMP =
		makeMaxMicroTimestamp(TRIM_MILLISECONDS);
const NanoTimestamp ValueProcessor::SUPPORT_MAX_NANO_TIMESTAMP =
		makeMaxNanoTimestamp(TRIM_MILLISECONDS);

/*!
	@brief Compare message field value with object field value
*/
int32_t ValueProcessor::compare(TransactionContext &txn,
	ObjectManagerV4 &objectManager, AllocateStrategy &strategy, ColumnId columnId,
	MessageRowStore *messageRowStore, uint8_t *objectRowField) {
	const uint8_t *inputField;
	uint32_t inputFieldSize;
	messageRowStore->getField(columnId, inputField, inputFieldSize);

	ColumnType type =
		messageRowStore->getColumnInfoList()[columnId].getColumnType();

	int32_t result;
	switch (type) {
	case COLUMN_TYPE_BOOL:
	case COLUMN_TYPE_BYTE:
	case COLUMN_TYPE_SHORT:
	case COLUMN_TYPE_INT:
	case COLUMN_TYPE_LONG:
	case COLUMN_TYPE_FLOAT:
	case COLUMN_TYPE_DOUBLE:
	case COLUMN_TYPE_TIMESTAMP:
	case COLUMN_TYPE_MICRO_TIMESTAMP:
	case COLUMN_TYPE_NANO_TIMESTAMP: {
		uint32_t objectRowFieldSize = 0;
		result = ComparatorTable::getComparator(type, type)(
				txn, inputField, inputFieldSize,
				objectRowField, objectRowFieldSize);
	} break;
	case COLUMN_TYPE_STRING:
		result = StringProcessor::compare(
			txn, objectManager, strategy, columnId, messageRowStore, objectRowField);
		break;
	case COLUMN_TYPE_GEOMETRY:
		result = GeometryProcessor::compare(
			txn, objectManager, strategy, columnId, messageRowStore, objectRowField);
		break;
	case COLUMN_TYPE_BLOB:
		result = BlobProcessor::compare(
			txn, objectManager, strategy, columnId, messageRowStore, objectRowField);
		break;
	case COLUMN_TYPE_STRING_ARRAY:
		result = StringArrayProcessor::compare(
			txn, objectManager, strategy, columnId, messageRowStore, objectRowField);
		break;
	case COLUMN_TYPE_BOOL_ARRAY:
	case COLUMN_TYPE_BYTE_ARRAY:
	case COLUMN_TYPE_SHORT_ARRAY:
	case COLUMN_TYPE_INT_ARRAY:
	case COLUMN_TYPE_LONG_ARRAY:
	case COLUMN_TYPE_FLOAT_ARRAY:
	case COLUMN_TYPE_DOUBLE_ARRAY:
	case COLUMN_TYPE_TIMESTAMP_ARRAY:
		result = ArrayProcessor::compare(
			txn, objectManager, strategy, columnId, messageRowStore, objectRowField);
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
	}
	return result;
}

/*!
	@brief Compare object field values
*/
int32_t ValueProcessor::compare(TransactionContext &txn,
	ObjectManagerV4 &objectManager, AllocateStrategy &strategy, ColumnType type, uint8_t *srcObjectRowField,
	uint8_t *targetObjectRowField) {
	int32_t result;
	switch (type) {
	case COLUMN_TYPE_BOOL:
	case COLUMN_TYPE_BYTE:
	case COLUMN_TYPE_SHORT:
	case COLUMN_TYPE_INT:
	case COLUMN_TYPE_LONG:
	case COLUMN_TYPE_FLOAT:
	case COLUMN_TYPE_DOUBLE:
	case COLUMN_TYPE_TIMESTAMP:
	case COLUMN_TYPE_MICRO_TIMESTAMP:
	case COLUMN_TYPE_NANO_TIMESTAMP: {
		uint32_t srcObjectRowFieldSize = 0;
		uint32_t targetObjectRowFieldSize = 0;
		result = ComparatorTable::getComparator(type, type)(
				txn, srcObjectRowField,
				srcObjectRowFieldSize, targetObjectRowField,
				targetObjectRowFieldSize);
	} break;
	case COLUMN_TYPE_STRING:
		result = StringProcessor::compare(
			txn, objectManager, strategy, type, srcObjectRowField, targetObjectRowField);
		break;
	case COLUMN_TYPE_GEOMETRY:
		result = GeometryProcessor::compare(
			txn, objectManager, strategy, type, srcObjectRowField, targetObjectRowField);
		break;
	case COLUMN_TYPE_BLOB:
		result = BlobProcessor::compare(
			txn, objectManager, strategy, type, srcObjectRowField, targetObjectRowField);
		break;
	case COLUMN_TYPE_STRING_ARRAY:
		result = StringArrayProcessor::compare(
			txn, objectManager, strategy, type, srcObjectRowField, targetObjectRowField);
		break;
	case COLUMN_TYPE_BOOL_ARRAY:
	case COLUMN_TYPE_BYTE_ARRAY:
	case COLUMN_TYPE_SHORT_ARRAY:
	case COLUMN_TYPE_INT_ARRAY:
	case COLUMN_TYPE_LONG_ARRAY:
	case COLUMN_TYPE_FLOAT_ARRAY:
	case COLUMN_TYPE_DOUBLE_ARRAY:
	case COLUMN_TYPE_TIMESTAMP_ARRAY:
		result = ArrayProcessor::compare(
			txn, objectManager, strategy, type, srcObjectRowField, targetObjectRowField);
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
	}
	return result;
}

/*!
	@brief Set field value to message
*/
void ValueProcessor::getField(TransactionContext &txn,
	ObjectManagerV4 &objectManager, AllocateStrategy &strategy, ColumnId columnId, const Value *objectValue,
	MessageRowStore *messageRowStore) {

	if (objectValue->isNullValue()) {
		messageRowStore->setNull(columnId);
		return;
	}

	ColumnType type =
		messageRowStore->getColumnInfoList()[columnId].getColumnType();

	switch (type) {
	case COLUMN_TYPE_BOOL:
	case COLUMN_TYPE_BYTE:
	case COLUMN_TYPE_SHORT:
	case COLUMN_TYPE_INT:
	case COLUMN_TYPE_LONG:
	case COLUMN_TYPE_FLOAT:
	case COLUMN_TYPE_DOUBLE:
	case COLUMN_TYPE_TIMESTAMP:
	case COLUMN_TYPE_MICRO_TIMESTAMP:
	case COLUMN_TYPE_NANO_TIMESTAMP:
		messageRowStore->setField(
			columnId, objectValue->data(), FixedSizeOfColumnType[type]);
		break;
	case COLUMN_TYPE_STRING:
		StringProcessor::getField(
			txn, objectManager, strategy, columnId, objectValue, messageRowStore);
		break;
	case COLUMN_TYPE_GEOMETRY:
		GeometryProcessor::getField(
			txn, objectManager, strategy, columnId, objectValue, messageRowStore);
		break;
	case COLUMN_TYPE_BLOB:
		BlobProcessor::getField(
			txn, objectManager, strategy, columnId, objectValue, messageRowStore);
		break;
	case COLUMN_TYPE_STRING_ARRAY:
		StringArrayProcessor::getField(
			txn, objectManager, strategy, columnId, objectValue, messageRowStore);
		break;
	case COLUMN_TYPE_BOOL_ARRAY:
	case COLUMN_TYPE_BYTE_ARRAY:
	case COLUMN_TYPE_SHORT_ARRAY:
	case COLUMN_TYPE_INT_ARRAY:
	case COLUMN_TYPE_LONG_ARRAY:
	case COLUMN_TYPE_FLOAT_ARRAY:
	case COLUMN_TYPE_DOUBLE_ARRAY:
	case COLUMN_TYPE_TIMESTAMP_ARRAY:
		ArrayProcessor::getField(
			txn, objectManager, strategy, columnId, objectValue, messageRowStore);
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
	}
}

std::string ValueProcessor::dumpMemory(
	const std::string &name, const uint8_t *addr, uint64_t size) {
	util::NormalOStringStream ss;

	ss << "[dump]" << name << ", size=" << size << std::endl;
	for (uint64_t i = 0; i < size; ++i) {
		uint16_t val = *addr;
		ss << std::hex << std::setw(2) << std::setfill('0') << val << " ";
		++addr;
		if (((i + 1) % 16) == 0) {
			ss << std::endl;
		}
	}
	ss << std::endl;
	return ss.str();
}

int8_t ValueProcessor::errorForPrimitiveColumnTypeOrdinal() {
	assert(false);
	GS_THROW_USER_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
}

VariableArrayCursor::VariableArrayCursor(
	ObjectManagerV4 &objectManager, AllocateStrategy &strategy, OId oId, AccessMode accessMode)
	: BaseObject(objectManager, strategy),
	  curObject_(*this),
	  elemCursor_(UNDEF_CURSOR_POS),
	  accessMode_(accessMode) {
	BaseObject::load(oId, accessMode_);
	rootOId_ = getBaseOId();
	elemNum_ = ValueProcessor::decodeVarSize(curObject_.getBaseAddr());
	curObject_.moveCursor(
		ValueProcessor::getEncodedVarSize(elemNum_));  
}

VariableArrayCursor::VariableArrayCursor(uint8_t *addr)
	: BaseObject(addr), curObject_(*this), elemCursor_(UNDEF_CURSOR_POS), 
	  accessMode_(OBJECT_READ_ONLY) {
	rootOId_ = getBaseOId();
	elemNum_ = ValueProcessor::decodeVarSize(curObject_.getBaseAddr());
	curObject_.moveCursor(
		ValueProcessor::getEncodedVarSize(elemNum_));  
}

/*!
	@brief Get current element
*/
uint8_t *VariableArrayCursor::getElement(
	uint32_t &elemSize, uint32_t &elemCount) {
	if (elemCursor_ >= elemNum_) {
		assert(false);
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_DS_PARAMETER_INVALID, "");  
	}
	elemCount = elemCursor_;
	elemSize = ValueProcessor::decodeVarSize(curObject_.getCursor<uint8_t>());
	return curObject_.getCursor<uint8_t>();  
}

/*!
	@brief Move to next element
*/
bool VariableArrayCursor::nextElement(bool forRemove) {
	if (elemCursor_ + 1 >= elemNum_ || elemNum_ == 0) {
		if (forRemove) {
			curObject_.finalize();
		}
		return false;
	}
	if (elemCursor_ == UNDEF_CURSOR_POS) {
		elemCursor_ = 0;
		return true;
	}
	uint32_t elemSize =
		ValueProcessor::decodeVarSize(curObject_.getCursor<uint8_t>());
	curObject_.moveCursor(
		elemSize + ValueProcessor::getEncodedVarSize(elemSize));  
	++elemCursor_;
	bool isOId = false;
	uint64_t elemSize64 = ValueProcessor::decodeVarSizeOrOId(
		curObject_.getCursor<uint8_t>(), isOId);
	if (isOId) {
		OId oId = static_cast<OId>(elemSize64);
		if (forRemove) {
			curObject_.finalize();
		}
		curObject_.loadNeighbor(oId, accessMode_);
	}
	return true;
}


bool VariableArrayCursor::moveElement(uint32_t pos) {
	if (pos >= elemNum_) {
		return false;
	}
	if (elemCursor_ == UNDEF_CURSOR_POS || elemCursor_ > pos) {
		if (elemCursor_ > pos) {
			reset();
		}
		for (uint32_t i = 0; i <= pos; i++) {
			bool isExist = nextElement();
			if (!isExist) {
				return false;
			}
		}
	} else {
		for (uint32_t i = elemCursor_; i < pos; i++) {
			bool isExist = nextElement();
			if (!isExist) {
				return false;
			}
		}
	}
	return true;
}


/*!
	@brief Clone
*/
OId VariableArrayCursor::clone(
	AllocateStrategy &allocateStrategy, OId neighborOId) {
	if (UNDEF_OID == getBaseOId()) {
		return UNDEF_OID;
	}
	OId currentNeighborOId = neighborOId;
	OId linkOId = UNDEF_OID;

	uint32_t srcDataLength = getArrayLength();
	if (srcDataLength == 0) {
		uint8_t *srcObj = getBaseAddr();
		DSObjectSize srcObjSize = getSize();
		BaseObject destObject(*getObjectManager(), allocateStrategy);
		OId destOId = UNDEF_OID;
		if (currentNeighborOId != UNDEF_OID) {
			destObject.allocateNeighbor<uint8_t>(srcObjSize,
				destOId, currentNeighborOId, OBJECT_TYPE_VARIANT);
		}
		else {
			destObject.allocate<uint8_t>(
				srcObjSize, destOId, OBJECT_TYPE_VARIANT);
		}
		memcpy(destObject.getBaseAddr(), srcObj, srcObjSize);
		linkOId = destOId;
	}
	else {
		OId srcOId = UNDEF_OID;
		OId destOId = UNDEF_OID;
		OId prevOId = UNDEF_OID;
		uint8_t *srcObj = NULL;
		BaseObject destObject(*getObjectManager(), allocateStrategy);

		uint8_t *srcElem;
		uint32_t srcElemSize;
		uint32_t srcCount;
		uint8_t *lastElem = NULL;
		uint32_t lastElemSize = 0;
		while (nextElement()) {
			srcObj = data();
			srcOId = getElementOId();
			srcElem = getElement(srcElemSize, srcCount);
			if (srcOId != prevOId) {
				DSObjectSize srcObjSize = getSize();
				if (currentNeighborOId != UNDEF_OID) {
					destObject.allocateNeighbor<uint8_t>(srcObjSize,
						destOId, currentNeighborOId,
						OBJECT_TYPE_VARIANT);
				}
				else {
					destObject.allocate<uint8_t>(srcObjSize,
						destOId, OBJECT_TYPE_VARIANT);
				}
				currentNeighborOId =
					destOId;  

				memcpy(destObject.getBaseAddr(), srcObj, srcObjSize);
				if (UNDEF_OID == linkOId) {
					linkOId = destOId;
				}
				else {
					assert(lastElem != NULL);
					uint8_t *linkOIdAddr =
						lastElem + lastElemSize +
						ValueProcessor::getEncodedVarSize(lastElemSize);
					uint64_t encodedOId =
						ValueProcessor::encodeVarSizeOId(destOId);
					memcpy(linkOIdAddr, &encodedOId, sizeof(uint64_t));
				}
				prevOId = srcOId;
			}
			lastElem = destObject.getBaseAddr() + (srcElem - srcObj);
			lastElemSize = srcElemSize;
		}
	}
	return linkOId;
}

/*!
	@brief Get ObjectId of current element
*/
OId VariableArrayCursor::getElementOId() {
	return curObject_.getBaseOId();
}

/*!
	@brief Set array length
*/
void VariableArrayCursor::setArrayLength(uint32_t length) {
	if (elemNum_ == 0) {
		elemNum_ = length;
	}
	else {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_DS_PARAMETER_INVALID, "");  
	}
}

/*!
	@brief Get field value Object
*/
void VariableArrayCursor::getField(
	const ColumnInfo &columnInfo, BaseObject &baseObject) {
	uint32_t variableColumnNum =
		columnInfo.getColumnOffset();  
	uint32_t varColumnNth = 0;
	while (nextElement()) {
		;  
		if (varColumnNth == variableColumnNum) {
			break;
		}
		++varColumnNth;
	}
	if (varColumnNth != variableColumnNum) {
		assert(false);
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_DS_PARAMETER_INVALID,
			"varColumnNth(" << varColumnNth << ") != variableColumnNum"
							<< variableColumnNum);
	}
	baseObject.copyReference(curObject_.getBaseOId(), curObject_.getBaseAddr());
	baseObject.moveCursor(
		curObject_.getCursor<uint8_t>() - curObject_.getBaseAddr());
}

/*!
	@brief Free Objects related to VariableArray
*/
void VariableArrayCursor::finalize() {
	reset();  
	if (getBaseOId() != UNDEF_OID) {
		while (nextElement(true)) {  
			;
		}
	}
}

void VariableArrayCursor::checkVarDataSize(TransactionContext &txn,
	ObjectManagerV4 &objectManager,
	const util::XArray< std::pair<uint8_t *, uint32_t> > &varList,
	const util::XArray<ColumnType> &columnTypeList,
	bool isConvertSpecialType,
	util::XArray<uint32_t> &varDataObjectSizeList,
	util::XArray<uint32_t> &varDataObjectPosList) {
	uint32_t variableColumnNum = static_cast<uint32_t>(varList.size());

	uint32_t currentObjectSize = ValueProcessor::getEncodedVarSize(
		variableColumnNum);  
	varDataObjectSizeList.push_back(currentObjectSize);  
	uint32_t varColumnObjectCount = 0;
	util::XArray<uint32_t> accumulateSizeList(txn.getDefaultAllocator());
	for (uint32_t elemNth = 0; elemNth < varList.size(); elemNth++) {
		ColumnType columnType = columnTypeList[elemNth];
		uint32_t elemSize = varList[elemNth].second;
		if (isConvertSpecialType) {
			if (columnType == COLUMN_TYPE_STRING_ARRAY) {
				elemSize = LINK_VARIABLE_COLUMN_DATA_SIZE;
			} else if (columnType == COLUMN_TYPE_BLOB) {
				elemSize = BlobCursor::getPrefixDataSize(objectManager, elemSize);
			}
		}
		for (size_t checkCount = 0; !accumulateSizeList.empty() && 
			(currentObjectSize + elemSize +
				ValueProcessor::getEncodedVarSize(elemSize) +
				NEXT_OBJECT_LINK_INFO_SIZE) >
			static_cast<uint32_t>(objectManager.getRecommendedLimitObjectSize());
			checkCount++) {
			uint32_t dividedObjectSize = currentObjectSize;
			uint32_t dividedElemNth = elemNth - 1;
			DSObjectSize estimateAllocateSize =
				objectManager.estimateAllocateSize(currentObjectSize) + NEXT_OBJECT_LINK_INFO_SIZE;
			if (checkCount == 0 && VariableArrayCursor::divisionThreshold(currentObjectSize) < estimateAllocateSize) {
				for (size_t i = 0; i < accumulateSizeList.size(); i++) {
					uint32_t accumulateSize = accumulateSizeList[accumulateSizeList.size() - 1 - i];
					if (accumulateSize == currentObjectSize) {
						continue;
					}
					DSObjectSize estimateAllocateSizeFront =
						objectManager.estimateAllocateSize(accumulateSize + NEXT_OBJECT_LINK_INFO_SIZE) + ObjectManagerV4::OBJECT_HEADER_SIZE;
					DSObjectSize estimateAllocateSizeBack =
						objectManager.estimateAllocateSize(currentObjectSize - accumulateSize);
					if (estimateAllocateSizeFront + estimateAllocateSizeBack < estimateAllocateSize && 
						(VariableArrayCursor::divisionThreshold(accumulateSize + ObjectManagerV4::OBJECT_HEADER_SIZE) >= estimateAllocateSizeFront)) {
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

	DSObjectSize estimateAllocateSize =
		objectManager.estimateAllocateSize(currentObjectSize);
	if (VariableArrayCursor::divisionThreshold(currentObjectSize) < estimateAllocateSize) {
		uint32_t dividedObjectSize = currentObjectSize;
		uint32_t dividedElemNth = variableColumnNum - 1;
		for (size_t i = 0; i < accumulateSizeList.size(); i++) {
			uint32_t accumulateSize = accumulateSizeList[accumulateSizeList.size() - 1 - i];
			if (accumulateSize == currentObjectSize) {
				continue;
			}
			DSObjectSize estimateAllocateSizeFront =
				objectManager.estimateAllocateSize(accumulateSize + NEXT_OBJECT_LINK_INFO_SIZE) + ObjectManagerV4::OBJECT_HEADER_SIZE;
			DSObjectSize estimateAllocateSizeBack =
				objectManager.estimateAllocateSize(currentObjectSize - accumulateSize);
			if (estimateAllocateSizeFront + estimateAllocateSizeBack < estimateAllocateSize && 
				(VariableArrayCursor::divisionThreshold(accumulateSize + ObjectManagerV4::OBJECT_HEADER_SIZE) >= estimateAllocateSizeFront)) {
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

OId VariableArrayCursor::createVariableArrayCursor(TransactionContext &txn,
	ObjectManagerV4 &objectManager,
	AllocateStrategy &allocateStrategy,
	const util::XArray< std::pair<uint8_t *, uint32_t> > &varList,
	const util::XArray<ColumnType> &columnTypeList,
	bool isConvertSpecialType,
	const util::XArray<uint32_t> &varDataObjectSizeList,
	const util::XArray<uint32_t> &varDataObjectPosList,
	const util::XArray<OId> &oldVarDataOIdList,
	OId neighborOId) {

	const uint32_t variableColumnNum = static_cast<uint32_t>(columnTypeList.size());

	OId topOId = UNDEF_OID;
	uint32_t elemNth = 0;
	uint8_t *destAddr = NULL;
	OId variableOId = UNDEF_OID;
	OId oldVarDataOId = UNDEF_OID;
	BaseObject oldVarObj(objectManager, allocateStrategy);
	DSObjectSize oldVarObjSize = 0;
	uint8_t *nextLinkAddr = NULL;
	BaseObject refVarObj(objectManager, allocateStrategy);
	for (size_t i = 0; i < varDataObjectSizeList.size(); ++i) {
		if (i < oldVarDataOIdList.size()) {
			oldVarDataOId = oldVarDataOIdList[i];
			oldVarObj.load(oldVarDataOId, OBJECT_FOR_UPDATE);
			oldVarObjSize =
				oldVarObj.getSize();
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
				varDataObjectSizeList[i], variableOId,
				neighborOId,
				OBJECT_TYPE_ROW);  
			neighborOId = variableOId;
			assert(destAddr);
		}
		if (i == 0) {
			topOId = variableOId;
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
		refVarObj.copyReference(oldVarObj);
		for (; elemNth < varList.size(); elemNth++) {
			uint32_t elemSize = varList[elemNth].second;
			uint8_t *data = varList[elemNth].first;
			uint32_t headerSize =
				ValueProcessor::getEncodedVarSize(elemSize);
			ColumnType columnType = columnTypeList[elemNth];
			if (isConvertSpecialType && columnType == COLUMN_TYPE_STRING_ARRAY) {				
				uint32_t linkHeaderValue = static_cast<uint32_t>(
						ValueProcessor::encodeVarSize(
							LINK_VARIABLE_COLUMN_DATA_SIZE));
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
			} else if (isConvertSpecialType && columnType == COLUMN_TYPE_BLOB) {
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
	ChunkAccessor ca;
	for (size_t i = varDataObjectSizeList.size();
		 i < oldVarDataOIdList.size(); ++i) {
		assert(UNDEF_OID != oldVarDataOIdList[i]);
		if (UNDEF_OID != oldVarDataOIdList[i]) {
			objectManager.free(ca, allocateStrategy.getGroupId(), oldVarDataOIdList[i]);
		}
	}

	return topOId;
}

StringCursor::StringCursor(
	ObjectManagerV4 &objectManager, AllocateStrategy &strategy, OId oId)
	: BaseObject(objectManager, strategy, oId),
	  zeroLengthStr_(ZERO_LENGTH_STR_BINARY_) {
	length_ = ValueProcessor::decodeVarSize(getBaseAddr());
	moveCursor(ValueProcessor::getEncodedVarSize(length_));  
}

StringCursor::StringCursor(uint8_t *binary)
	: BaseObject(binary), zeroLengthStr_(ZERO_LENGTH_STR_BINARY_) {
	setBaseOId(UNDEF_OID);
	if (binary == NULL) {
		setBaseAddr(&zeroLengthStr_);
		length_ = 0;
	}
	else {
		setBaseAddr(binary);
		length_ = ValueProcessor::decodeVarSize(getBaseAddr());
		moveCursor(ValueProcessor::getEncodedVarSize(length_));  
	}
}

StringCursor::StringCursor(
	util::StackAllocator& alloc, const uint8_t *str, uint32_t strLength)
	: BaseObject(NULL), zeroLengthStr_(ZERO_LENGTH_STR_BINARY_) {
	setBaseOId(UNDEF_OID);
	if (str == NULL) {
		setBaseAddr(&zeroLengthStr_);
		length_ = 0;
	}
	else {
		length_ = strLength;
		size_t offset = ValueProcessor::getEncodedVarSize(length_);  
		setBaseAddr(
			ALLOC_NEW(alloc) uint8_t[offset + length_]);

		uint64_t encodedLength = ValueProcessor::encodeVarSize(length_);
		memcpy(getBaseAddr(), &encodedLength, offset);
		memcpy(getBaseAddr() + offset, str, length_);
		moveCursor(offset);
	}
}

StringCursor::StringCursor(util::StackAllocator &alloc, const char *str)
	: BaseObject(NULL), zeroLengthStr_(ZERO_LENGTH_STR_BINARY_) {
	setBaseOId(UNDEF_OID);
	if (str == NULL) {
		setBaseAddr(&zeroLengthStr_);
		length_ = 0;
	}
	else {
		length_ = static_cast<uint32_t>(strlen(str));
		size_t offset = ValueProcessor::getEncodedVarSize(length_);  
		setBaseAddr(
			ALLOC_NEW(alloc) uint8_t[offset + length_]);

		uint64_t encodedLength = ValueProcessor::encodeVarSize(length_);
		memcpy(getBaseAddr(), &encodedLength, offset);
		memcpy(getBaseAddr() + offset, str, length_);
		moveCursor(offset);
	}
}

StringCursor::StringCursor(
		util::XArray<uint8_t> &buf, const uint8_t *str, uint32_t strLength) :
		BaseObject(NULL), zeroLengthStr_(ZERO_LENGTH_STR_BINARY_) {
	setBaseOId(UNDEF_OID);
	if (str == NULL) {
		setBaseAddr(&zeroLengthStr_);
		length_ = 0;
	}
	else {
		length_ = strLength;
		size_t offset = ValueProcessor::getEncodedVarSize(length_);  

		buf.resize(offset + length_);
		setBaseAddr(buf.data());

		uint64_t encodedLength = ValueProcessor::encodeVarSize(length_);
		memcpy(getBaseAddr(), &encodedLength, offset);
		memcpy(getBaseAddr() + offset, str, length_);
		moveCursor(offset);
	}
}

/*!
	@brief Get data size
*/
uint32_t StringCursor::getObjectSize() {
	return length_ + ValueProcessor::getEncodedVarSize(length_);  
}

ValueProcessor::RawTimestampFormatter<Timestamp>
ValueProcessor::getRawTimestampFormatter(const Timestamp &ts) {
	return ValueProcessor::RawTimestampFormatter<Timestamp>(ts);
}

ValueProcessor::RawTimestampFormatter<MicroTimestamp>
ValueProcessor::getRawTimestampFormatter(const MicroTimestamp &ts) {
	return ValueProcessor::RawTimestampFormatter<MicroTimestamp>(ts);
}

ValueProcessor::RawTimestampFormatter<NanoTimestamp>
ValueProcessor::getRawTimestampFormatter(const NanoTimestamp &ts) {
	return ValueProcessor::RawTimestampFormatter<NanoTimestamp>(ts);
}

void ValueProcessor::dumpRawTimestamp(std::ostream &os, const Timestamp &ts) {
	os << ts;
}

void ValueProcessor::dumpRawTimestamp(
		std::ostream &os, const MicroTimestamp &ts) {
	os << ts.value_;
}

void ValueProcessor::dumpRawTimestamp(
		std::ostream &os, const NanoTimestamp &ts) {
	os << "[";
	os << ts.getHigh();
	os << ",";
	os << static_cast<uint32_t>(ts.getLow());
	os << "]";
}

Timestamp ValueProcessor::makeMaxTimestamp(bool fractionTrimming) {
	if (util::DateTime::max(fractionTrimming) >
			static_cast<int64_t>(std::numeric_limits<int32_t>::max()) * 1000) {
		return util::DateTime(9999, 12, 31, 23, 59, 59, 999, false).getUnixTime();
	}
	else {
		return util::DateTime::max(fractionTrimming).getUnixTime();
	}
}

MicroTimestamp ValueProcessor::makeMaxMicroTimestamp(bool fractionTrimming) {
	return getMicroTimestamp(makeMaxNanoTimestamp(fractionTrimming));
}

NanoTimestamp ValueProcessor::makeMaxNanoTimestamp(bool fractionTrimming) {
	return getNanoTimestamp(util::PreciseDateTime::ofNanoSeconds(
			util::DateTime(makeMaxTimestamp(fractionTrimming)),
			(fractionTrimming ? 0 : 1000 * 1000 - 1)));
}

Timestamp ValueProcessor::errorTimestampType() {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
}

MicroTimestamp ValueProcessor::errorMicroTimestampType() {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
}

NanoTimestamp ValueProcessor::errorNanoTimestampType() {
	GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
}

std::string ValueProcessor::getTypeName(ColumnType type) {
	return getTypeNameChars(type);
}

const char8_t* ValueProcessor::getTypeNameChars(
		ColumnType type, bool precisionIgnorable) {
	switch (type) {
	case COLUMN_TYPE_BOOL:
		return "BOOL";
	case COLUMN_TYPE_BYTE:
		return "BYTE";
	case COLUMN_TYPE_SHORT:
		return "SHORT";
	case COLUMN_TYPE_INT:
		return "INTEGER";
	case COLUMN_TYPE_LONG:
		return "LONG";
	case COLUMN_TYPE_FLOAT:
		return "FLOAT";
	case COLUMN_TYPE_DOUBLE:
		return "DOUBLE";
	case COLUMN_TYPE_TIMESTAMP:
		return "TIMESTAMP";
	case COLUMN_TYPE_STRING:
		return "STRING";
	case COLUMN_TYPE_GEOMETRY:
		return "GEOMETRY";
	case COLUMN_TYPE_BLOB:
		return "BLOB";
	case COLUMN_TYPE_STRING_ARRAY:
		return "STRING_ARRAY";
	case COLUMN_TYPE_BOOL_ARRAY:
		return "BOOL_ARRAY";
	case COLUMN_TYPE_BYTE_ARRAY:
		return "BYTE_ARRAY";
	case COLUMN_TYPE_SHORT_ARRAY:
		return "SHORT_ARRAY";
	case COLUMN_TYPE_INT_ARRAY:
		return "INTEGER_ARRAY";
	case COLUMN_TYPE_LONG_ARRAY:
		return "LONG_ARRAY";
	case COLUMN_TYPE_FLOAT_ARRAY:
		return "FLOAT_ARRAY";
	case COLUMN_TYPE_DOUBLE_ARRAY:
		return "DOUBLE_ARRAY";
	case COLUMN_TYPE_TIMESTAMP_ARRAY:
		return "TIMESTAMP_ARRAY";
	case COLUMN_TYPE_MICRO_TIMESTAMP:
		if (precisionIgnorable) {
			return "TIMESTAMP";
		}
		return "TIMESTAMP(6)";
	case COLUMN_TYPE_NANO_TIMESTAMP:
		if (precisionIgnorable) {
			return "TIMESTAMP";
		}
		return "TIMESTAMP(9)";
	default:
		return "UNKNOWN_TYPE";
	}
}

void ValueProcessor::dumpSimpleValue(util::NormalOStringStream &stream, ColumnType columnType, const void *data, uint32_t size, bool withType) {
	if (isArray(columnType)) {
		stream << "(support only simple type";
	} else {
		if (withType) {
			stream << "(" << getTypeName(columnType) << ")";
		}
		if (data == NULL && columnType != COLUMN_TYPE_NULL) {
			stream << "NULL";
		} else {
		switch (columnType) {
		case COLUMN_TYPE_STRING :
			{
				util::NormalXArray<char> binary;
				binary.push_back(static_cast<const char *>(data) + ValueProcessor::getEncodedVarSize(size), size);
				binary.push_back('\0');
				stream << "'" << binary.data() << "'";
			}
			break;
		case COLUMN_TYPE_TIMESTAMP :
			{
				Timestamp val = *static_cast<const Timestamp *>(data);
				util::DateTime dateTime(val);
				dateTime.format(stream, false, false);
			}
			break;
		case COLUMN_TYPE_GEOMETRY :
		case COLUMN_TYPE_BLOB:
			{
				stream << "x'";
				util::NormalIStringStream iss(
						u8string(static_cast<const char8_t*>(data) + ValueProcessor::getEncodedVarSize(size), size));
				util::HexConverter::encode(stream, iss);
				stream << "'";
			}
			break;
		case COLUMN_TYPE_BOOL:
			if (*static_cast<const bool *>(data)) {
				stream << "TRUE";
			} else {
				stream << "FALSE";
			}
			break;
		case COLUMN_TYPE_BYTE: 
			{
				int16_t byteVal = 
					*static_cast<const int8_t *>(data);
				stream << byteVal;
			}
			break;
		case COLUMN_TYPE_SHORT:
			stream << *static_cast<const int16_t *>(data);
			break;
		case COLUMN_TYPE_INT:
			stream << *static_cast<const int32_t *>(data);
			break;
		case COLUMN_TYPE_LONG:
			stream << *static_cast<const int64_t *>(data);
			break;
		case COLUMN_TYPE_FLOAT:
			stream << *static_cast<const float *>(data);
			break;
		case COLUMN_TYPE_DOUBLE:
			stream << *static_cast<const double *>(data);
			break;
		case COLUMN_TYPE_NULL:
			stream << "NULL";
			break;
		case COLUMN_TYPE_MICRO_TIMESTAMP:
			stream << toDateTime(
					*static_cast<const MicroTimestamp*>(data)).getFormatter(
					util::DateTime::ZonedOption()).withDefaultPrecision(
					util::DateTime::FIELD_MICROSECOND);
			break;
		case COLUMN_TYPE_NANO_TIMESTAMP:
			stream << toDateTime(
					*static_cast<const NanoTimestamp*>(data)).getFormatter(
					util::DateTime::ZonedOption());
			break;
		default:
			stream << "(not implement)";
			break;
		}
		}
	}
}

int32_t ValueProcessor::getValuePrecision(ColumnType type) {
	switch (type) {
	case COLUMN_TYPE_TIMESTAMP:
		return 3;
	case COLUMN_TYPE_MICRO_TIMESTAMP:
		return 6;
	case COLUMN_TYPE_NANO_TIMESTAMP:
		return 9;
	default:
		return -1;
	}
}

int32_t ValueProcessor::getValueStringLength(ColumnType type) {
	switch (type) {
	case COLUMN_TYPE_TIMESTAMP:
	case COLUMN_TYPE_MICRO_TIMESTAMP:
	case COLUMN_TYPE_NANO_TIMESTAMP:
		return 27 + getValuePrecision(type);
	default:
		return -1;
	}
}

