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
	(util::DateTime::max(TRIM_MILLISECONDS) >
		static_cast<int64_t>(std::numeric_limits<int32_t>::max()) * 1000)
		? util::DateTime(9999, 12, 31, 23, 59, 59, 999, false).getUnixTime()
		: util::DateTime::max(TRIM_MILLISECONDS).getUnixTime();

/*!
	@brief Compare message field value with object field value
*/
int32_t ValueProcessor::compare(TransactionContext &txn,
	ObjectManager &objectManager, ColumnId columnId,
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
	case COLUMN_TYPE_TIMESTAMP: {
		uint32_t objectRowFieldSize = 0;
		result = ComparatorTable::comparatorTable_[type][type](txn, inputField, inputFieldSize,
			objectRowField, objectRowFieldSize);
	} break;
	case COLUMN_TYPE_STRING:
		result = StringProcessor::compare(
			txn, objectManager, columnId, messageRowStore, objectRowField);
		break;
	case COLUMN_TYPE_GEOMETRY:
		result = GeometryProcessor::compare(
			txn, objectManager, columnId, messageRowStore, objectRowField);
		break;
	case COLUMN_TYPE_BLOB:
		result = BlobProcessor::compare(
			txn, objectManager, columnId, messageRowStore, objectRowField);
		break;
	case COLUMN_TYPE_STRING_ARRAY:
		result = StringArrayProcessor::compare(
			txn, objectManager, columnId, messageRowStore, objectRowField);
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
			txn, objectManager, columnId, messageRowStore, objectRowField);
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
	ObjectManager &objectManager, ColumnType type, uint8_t *srcObjectRowField,
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
	case COLUMN_TYPE_TIMESTAMP: {
		uint32_t srcObjectRowFieldSize = 0;
		uint32_t targetObjectRowFieldSize = 0;
		result = ComparatorTable::comparatorTable_[type][type](txn, srcObjectRowField,
			srcObjectRowFieldSize, targetObjectRowField,
			targetObjectRowFieldSize);
	} break;
	case COLUMN_TYPE_STRING:
		result = StringProcessor::compare(
			txn, objectManager, type, srcObjectRowField, targetObjectRowField);
		break;
	case COLUMN_TYPE_GEOMETRY:
		result = GeometryProcessor::compare(
			txn, objectManager, type, srcObjectRowField, targetObjectRowField);
		break;
	case COLUMN_TYPE_BLOB:
		result = BlobProcessor::compare(
			txn, objectManager, type, srcObjectRowField, targetObjectRowField);
		break;
	case COLUMN_TYPE_STRING_ARRAY:
		result = StringArrayProcessor::compare(
			txn, objectManager, type, srcObjectRowField, targetObjectRowField);
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
			txn, objectManager, type, srcObjectRowField, targetObjectRowField);
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
	ObjectManager &objectManager, ColumnId columnId, const Value *objectValue,
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
		messageRowStore->setField(
			columnId, objectValue->data(), FixedSizeOfColumnType[type]);
		break;
	case COLUMN_TYPE_STRING:
		StringProcessor::getField(
			txn, objectManager, columnId, objectValue, messageRowStore);
		break;
	case COLUMN_TYPE_GEOMETRY:
		GeometryProcessor::getField(
			txn, objectManager, columnId, objectValue, messageRowStore);
		break;
	case COLUMN_TYPE_BLOB:
		BlobProcessor::getField(
			txn, objectManager, columnId, objectValue, messageRowStore);
		break;
	case COLUMN_TYPE_STRING_ARRAY:
		StringArrayProcessor::getField(
			txn, objectManager, columnId, objectValue, messageRowStore);
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
			txn, objectManager, columnId, objectValue, messageRowStore);
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

VariableArrayCursor::VariableArrayCursor(TransactionContext &txn,
	ObjectManager &objectManager, OId oId, AccessMode accessMode)
	: BaseObject(txn.getPartitionId(), objectManager),
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
	: BaseObject(addr), curObject_(*this), elemCursor_(UNDEF_CURSOR_POS) {
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

/*!
	@brief Clone
*/
OId VariableArrayCursor::clone(TransactionContext &txn,
	const AllocateStrategy &allocateStrategy, OId neighborOId) {
	if (UNDEF_OID == getBaseOId()) {
		return UNDEF_OID;
	}
	OId currentNeighborOId = neighborOId;
	OId linkOId = UNDEF_OID;

	uint32_t srcDataLength = getArrayLength();
	if (srcDataLength == 0) {
		uint8_t *srcObj = getBaseAddr();
		Size_t srcObjSize = getObjectManager()->getSize(srcObj);
		BaseObject destObject(txn.getPartitionId(), *getObjectManager());
		OId destOId = UNDEF_OID;
		if (currentNeighborOId != UNDEF_OID) {
			destObject.allocateNeighbor<uint8_t>(srcObjSize, allocateStrategy,
				destOId, currentNeighborOId, OBJECT_TYPE_VARIANT);
		}
		else {
			destObject.allocate<uint8_t>(
				srcObjSize, allocateStrategy, destOId, OBJECT_TYPE_VARIANT);
		}
		memcpy(destObject.getBaseAddr(), srcObj, srcObjSize);
		linkOId = destOId;
	}
	else {
		OId srcOId = UNDEF_OID;
		OId destOId = UNDEF_OID;
		OId prevOId = UNDEF_OID;
		uint8_t *srcObj = NULL;
		BaseObject destObject(txn.getPartitionId(), *getObjectManager());

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
				Size_t srcObjSize = getObjectManager()->getSize(srcObj);
				if (currentNeighborOId != UNDEF_OID) {
					destObject.allocateNeighbor<uint8_t>(srcObjSize,
						allocateStrategy, destOId, currentNeighborOId,
						OBJECT_TYPE_VARIANT);
				}
				else {
					destObject.allocate<uint8_t>(srcObjSize, allocateStrategy,
						destOId, OBJECT_TYPE_VARIANT);
				}
				currentNeighborOId =
					destOId;  

				memcpy(destObject.getBaseAddr(), srcObj, srcObjSize);
				if (UNDEF_OID == linkOId) {
					linkOId = destOId;
				}
				else {
					assert(lastElem > 0);
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

StringCursor::StringCursor(
	TransactionContext &txn, ObjectManager &objectManager, OId oId)
	: BaseObject(txn.getPartitionId(), objectManager, oId),
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
	TransactionContext &txn, const uint8_t *str, uint32_t strLength)
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
			ALLOC_NEW(txn.getDefaultAllocator()) uint8_t[offset + length_]);

		uint64_t encodedLength = ValueProcessor::encodeVarSize(length_);
		memcpy(getBaseAddr(), &encodedLength, offset);
		memcpy(getBaseAddr() + offset, str, length_);
		moveCursor(offset);
	}
}

StringCursor::StringCursor(TransactionContext &txn, const char *str)
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
			ALLOC_NEW(txn.getDefaultAllocator()) uint8_t[offset + length_]);

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

std::string ValueProcessor::getTypeName(ColumnType type) {
	return getTypeNameChars(type);
}

const char8_t* ValueProcessor::getTypeNameChars(ColumnType type) {
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
		return "INT_ARRAY";
	case COLUMN_TYPE_LONG_ARRAY:
		return "LONG_ARRAY";
	case COLUMN_TYPE_FLOAT_ARRAY:
		return "FLOAT_ARRAY";
	case COLUMN_TYPE_DOUBLE_ARRAY:
		return "DOUBLE_ARRAY";
	case COLUMN_TYPE_TIMESTAMP_ARRAY:
		return "TIMESTAMP_ARRAY";
	default:
		return "UNKNON_TYPE";
	}
}

void ValueProcessor::dumpSimpleValue(util::NormalOStringStream &stream, ColumnType columnType, const void *data, uint32_t size, bool withType) {
	if (isArray(columnType)) {
		stream << "(support only simple type";
	} else {
		if (withType) {
			stream << "(" << getTypeName(columnType) << ")";
		}
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
		default:
			stream << "(not implement)";
			break;
		}
	}
}
