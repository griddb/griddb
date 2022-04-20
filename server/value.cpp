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
	@brief Implementation of Value
*/
#include "value.h"
#include "util/time.h"
#include "gs_error.h"
#include "schema.h"
#include "message_row_store.h"

#include "gis_point.h"

const uint8_t Value::defalutFixedValue_[8] = {0, 0, 0, 0, 0, 0, 0, 0};
const uint8_t Value::defalutStringValue_[1] = {0x01};
const uint8_t Value::defalutFixedArrayValue_[2] = {(0x01 | (0x01 << 1)), 0x01};
const uint8_t Value::defalutStringArrayValue_[13] = {(0x01 | (0x0c << 1)), 0x00, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};
const uint8_t Value::defalutBlobValue_[2] = {(0x01 | (0x01 << 1)), 0x01};
const uint8_t Value::defalutGeometryValue_[7] = {0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0x00};

void Value::dump(
	TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
	util::NormalOStringStream &ss, bool forExport) const {
	if (isArray()) {
		if (!forExport) {
			ss << "[Array]length=";
		}
		if (data() != NULL) {
			ColumnType simpleType =
				ValueProcessor::getSimpleColumnType(getType());
			if (simpleType != COLUMN_TYPE_STRING) {
				ss << getArrayLength(txn, objectManager, strategy);
				ss << "[";
				for (uint32_t k = 0; k < getArrayLength(txn, objectManager, strategy);
					 k++) {
					if (k != 0) {
						ss << ",";
					}
					uint32_t size;
					const uint8_t *data;
					getArrayElement(txn, objectManager, strategy, k, data, size);
					ValueProcessor::dumpSimpleValue(ss, simpleType, data, size, !forExport);
				}
				ss << "]";
			}
			else {
				const uint8_t *blobHeaderData =
					reinterpret_cast<const uint8_t *>(data_.object_.value_);
				uint32_t elemSize =
					ValueProcessor::decodeVarSize(blobHeaderData);
				blobHeaderData += ValueProcessor::getEncodedVarSize(elemSize);
				uint32_t totalSize =
					*reinterpret_cast<const uint32_t *>(blobHeaderData);
				blobHeaderData += sizeof(uint32_t);

				OId headerOId = *reinterpret_cast<const OId *>(blobHeaderData);
				if (headerOId != UNDEF_OID) {
					VariableArrayCursor arrayCursor(
						objectManager, strategy, headerOId, OBJECT_READ_ONLY);
					uint32_t num = arrayCursor.getArrayLength();
					if (!forExport) {
						ss << num << ",";
						ss << "totalSize=" << totalSize;
					}
					ss << "[";
					uint32_t count = 0;
					uint8_t *elem;
					uint32_t elemSize;
					uint32_t elemCount;
					while (arrayCursor.nextElement()) {
						if (count != 0) {
							ss << ",";
						}
						elem = arrayCursor.getElement(elemSize, elemCount);
						ValueProcessor::dumpSimpleValue(ss, simpleType, elem, elemSize, !forExport);
						++count;
					}
					ss << "]";
					assert(num == count);
				}
			}
		}
		else {
			if (!forExport) {
				ss << "0,totalSize=0";
			}
			ss << "[]";
		}
	}
	else {
		switch (getType()) {
		case COLUMN_TYPE_BLOB: {
			BlobCursor blobCursor(objectManager, strategy, data());
			blobCursor.dump(ss, forExport);
		} break;
		default:
			ValueProcessor::dumpSimpleValue(ss, getType(), getImage(), size(), !forExport);
			break;
		}
	}
}



/*!
	@brief Get element of array
*/
void Value::getArrayElement(TransactionContext &txn,
	ObjectManagerV4 &objectManager, AllocateStrategy &strategy, uint32_t i, const uint8_t *&data,
	uint32_t &size) const {
	data = NULL;
	size = 0;
	if (isArray() && data_.object_.value_ != 0) {
		uint32_t num = getArrayLength(txn, objectManager, strategy);
		if (i < num) {
			ColumnType simpleType =
				ValueProcessor::getSimpleColumnType(getType());
			switch (simpleType) {
			case COLUMN_TYPE_BOOL:
			case COLUMN_TYPE_BYTE:
			case COLUMN_TYPE_SHORT:
			case COLUMN_TYPE_INT:
			case COLUMN_TYPE_LONG:
			case COLUMN_TYPE_FLOAT:
			case COLUMN_TYPE_DOUBLE:
			case COLUMN_TYPE_TIMESTAMP:
			case COLUMN_TYPE_OID: {
				const ArrayObject arrayObject(reinterpret_cast<uint8_t *>(
					const_cast<void *>(data_.object_.value_)));
				const uint8_t *elementObject =
					arrayObject.getArrayElement(i, FixedSizeOfColumnType[simpleType]);
				data = elementObject;
				size = FixedSizeOfColumnType[simpleType];
			} break;
			case COLUMN_TYPE_STRING: {
				const MatrixCursor *arrayObject =
					reinterpret_cast<const MatrixCursor *>(
						data_.object_.value_);
				if (data_.object_.onDataStore_) {
					VariableArrayCursor arrayCursor(
						objectManager, strategy, arrayObject->getHeaderOId(), OBJECT_READ_ONLY);

					uint32_t elemCount;
					while (arrayCursor
							   .nextElement()) {  
						data = arrayCursor.getElement(size, elemCount);
						if (elemCount == i) {
							break;
						}
					}
					data += ValueProcessor::getEncodedVarSize(
						size);  
				}
				else {
					size = 0;
					data =
						reinterpret_cast<const uint8_t *>(data_.object_.value_);
					if (data) {
						size = ValueProcessor::getEncodedVarSize(data);
						data += size;
					}
				}
			} break;
			default:
				GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
				break;
			}
		}
		else {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_ARRAY_INDEX_INVALID,
				"array num = " << num << ", index = " << i);
		}
	}
}

/*!
	@brief Serialize value (only numlic or Timestamp)
*/
void Value::serialize(util::XArray<uint8_t> &serializedRowList) const {
	if (!isArray() && (isNumerical() || type_ == COLUMN_TYPE_TIMESTAMP)) {
		int8_t tmp = type_;
		serializedRowList.push_back(
			reinterpret_cast<uint8_t *>(&tmp), sizeof(int8_t));
		serializedRowList.push_back(data(), FixedSizeOfColumnType[type_]);
	}
}

/*!
	@brief Copy value
*/
void Value::copy(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
	const Value &srcValue) {
	PartitionId pId = txn.getPartitionId();
	util::StackAllocator &alloc = txn.getDefaultAllocator();
	switch (srcValue.getType()) {
	case COLUMN_TYPE_STRING:
	case COLUMN_TYPE_GEOMETRY:
	{
		char *destObject;
		if (srcValue.size() > 0) {
			const char *srcObject =
				reinterpret_cast<const char *>(srcValue.data());
			uint32_t srcObjectLen = srcValue.size();
			uint32_t headerLen =
				ValueProcessor::getEncodedVarSize(srcObjectLen);

			destObject = reinterpret_cast<char *>(
				alloc.allocate(headerLen + srcObjectLen));
			memcpy(destObject, srcObject, headerLen + srcObjectLen);
		}
		else {
			destObject = NULL;
		}

		type_ = srcValue.getType();
		data_.object_.set(destObject, false);
	} break;
	case COLUMN_TYPE_BOOL:
		set(srcValue.data_.bool_);
		break;
	case COLUMN_TYPE_BYTE:
		set(srcValue.data_.byte_);
		break;
	case COLUMN_TYPE_SHORT:
		set(srcValue.data_.short_);
		break;
	case COLUMN_TYPE_INT:
		set(srcValue.data_.int_);
		break;
	case COLUMN_TYPE_LONG:
		set(srcValue.data_.long_);
		break;
	case COLUMN_TYPE_FLOAT:
		set(srcValue.data_.float_);
		break;
	case COLUMN_TYPE_DOUBLE:
		set(srcValue.data_.double_);
		break;
	case COLUMN_TYPE_TIMESTAMP:
		setTimestamp(srcValue.data_.timestamp_);
		break;
	case COLUMN_TYPE_OID:
		break;
	case COLUMN_TYPE_BLOB: {
		type_ = srcValue.getType();
		if (srcValue.data_.object_.value_ != NULL) {
			BlobCursor blobCursor(objectManager, strategy, reinterpret_cast<const uint8_t *>(srcValue.data_.object_.value_));
			uint64_t blobSize = blobCursor.getTotalSize();
			uint8_t *destAddr = reinterpret_cast<uint8_t *>(alloc.allocate(blobSize));
			while (blobCursor.next()) {
				uint32_t srcDataSize = 0;
				const uint8_t *srcData = NULL;
				blobCursor.getCurrentBinary(srcData, srcDataSize);
				memcpy(destAddr, srcData, srcDataSize);
				destAddr += srcDataSize;
			}
			data_.object_.set(destAddr, false);
		} else {
			data_.object_.set(NULL, false);
		}
	} break;
	case COLUMN_TYPE_STRING_ARRAY: {
		ArrayObject destObject(objectManager, strategy);
		if (srcValue.data_.object_.value_ != NULL) {
			const ArrayObject srcObject(reinterpret_cast<uint8_t *>(
				const_cast<void *>(srcValue.data_.object_.value_)));
			ColumnType simpleType =
				ValueProcessor::getSimpleColumnType(srcValue.getType());
			uint32_t objectSize = ArrayObject::getObjectSize(
				srcObject.getArrayLength(), FixedSizeOfColumnType[COLUMN_TYPE_OID]);

			uint32_t num = srcObject.getArrayLength();

			destObject.setBaseAddr(
				reinterpret_cast<uint8_t *>(alloc.allocate(objectSize)));

			destObject.setArrayLength(num, FixedSizeOfColumnType[simpleType]);
			for (uint32_t i = 0; i < num; i++) {
				const char *srcElemVariant;
				char *destElemVariant;
				BaseObject baseObject(objectManager, strategy);
				if (srcValue.onDataStore()) {
					const OId *srcElemOId = reinterpret_cast<const OId *>(
						srcObject.getArrayElement(i, FixedSizeOfColumnType[simpleType]));

					if (*srcElemOId != UNDEF_OID) {
						if (pId == UNDEF_PARTITIONID) {
							GS_THROW_USER_ERROR(GS_ERROR_CM_NOT_SUPPORTED,
								"not support copy type");
						}
						baseObject.load(*srcElemOId, false);
						srcElemVariant = baseObject.getBaseAddr<const char *>();
					}
					else {
						srcElemVariant = NULL;
					}
				}
				else {
					srcElemVariant = reinterpret_cast<const char *>(
						srcObject.getArrayElement(i, FixedSizeOfColumnType[simpleType]));
				}
				if (srcElemVariant != NULL) {
					uint32_t srcElemVariantLen =
						static_cast<uint32_t>(strlen(srcElemVariant));
					destElemVariant = reinterpret_cast<char *>(
						alloc.allocate(srcElemVariantLen + 1));
					memcpy(
						destElemVariant, srcElemVariant, srcElemVariantLen + 1);
				}
				else {
					destElemVariant = NULL;
				}

				destObject.setArrayElement(i, FixedSizeOfColumnType[simpleType],
					reinterpret_cast<const uint8_t *>(destElemVariant));
			}
		}

		type_ = srcValue.getType();
		data_.object_.set(destObject.getImage(), false);
	} break;
	case COLUMN_TYPE_BOOL_ARRAY:
	case COLUMN_TYPE_BYTE_ARRAY:
	case COLUMN_TYPE_SHORT_ARRAY:
	case COLUMN_TYPE_INT_ARRAY:
	case COLUMN_TYPE_LONG_ARRAY:
	case COLUMN_TYPE_FLOAT_ARRAY:
	case COLUMN_TYPE_DOUBLE_ARRAY:
	case COLUMN_TYPE_TIMESTAMP_ARRAY: {
		void *destObject;
		if (srcValue.data_.object_.value_ != NULL) {
			ArrayObject srcObject(reinterpret_cast<uint8_t *>(
				const_cast<void *>(srcValue.data_.object_.value_)));
			uint32_t objectSize = ArrayObject::getObjectSize(
				srcObject.getArrayLength(), FixedSizeOfColumnType[COLUMN_TYPE_OID]);

			destObject = alloc.allocate(objectSize);
			memcpy(destObject, srcObject.getImage(), objectSize);
		}
		else {
			destObject = NULL;
		}

		type_ = srcValue.getType();
		data_.object_.set(destObject, false);
	} break;
	case COLUMN_TYPE_NULL:
		*this = srcValue;
		break;
	default:
		break;
	}
}


void Value::get(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
	MessageRowStore *messageRowStore, ColumnId columnId) {
	if (isNullValue()) {
		messageRowStore->setNull(columnId);
	} else {
		ValueProcessor::getField(
			txn, objectManager, strategy, columnId, this, messageRowStore);
	}
}

