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

std::string Value::dump(
	TransactionContext &txn, ObjectManager &objectManager) const {
	util::NormalOStringStream ss;
	if (isArray()) {
		ss << "[Array]length=";
		if (data() != NULL) {
			ColumnType simpleType =
				ValueProcessor::getSimpleColumnType(getType());
			if (simpleType != COLUMN_TYPE_STRING) {
				ss << getArrayLength(txn, objectManager);
				ss << "[";
				for (uint32_t k = 0; k < getArrayLength(txn, objectManager);
					 k++) {
					if (k != 0) {
						ss << ",";
					}
					uint32_t size;
					const uint8_t *data;
					getArrayElement(txn, objectManager, k, data, size);
					switch (simpleType) {
					case COLUMN_TYPE_BOOL:
						ss << "(BOOL)" << *reinterpret_cast<const bool *>(data);
						break;
					case COLUMN_TYPE_BYTE: {
						ss << "(BYTE)";
						int16_t byteVal =
							*reinterpret_cast<const int8_t *>(data);
						ss << byteVal;
					} break;
					case COLUMN_TYPE_SHORT:
						ss << "(SHORT)"
						   << *reinterpret_cast<const int16_t *>(data);
						break;
					case COLUMN_TYPE_INT:
						ss << "(INT)"
						   << *reinterpret_cast<const int32_t *>(data);
						break;
					case COLUMN_TYPE_LONG:
						ss << "(LONG)"
						   << *reinterpret_cast<const int64_t *>(data);
						break;
					case COLUMN_TYPE_FLOAT:
						ss << "(FLOAT)"
						   << *reinterpret_cast<const float *>(data);
						break;
					case COLUMN_TYPE_DOUBLE:
						ss << "(DOUBLE)"
						   << *reinterpret_cast<const double *>(data);
						break;
					case COLUMN_TYPE_TIMESTAMP:
						ss << "(TIMESTAMP)";
						ss << *(reinterpret_cast<const Timestamp *>(data));
						break;
					case COLUMN_TYPE_BLOB:
					case COLUMN_TYPE_OID:
					case COLUMN_TYPE_STRING_ARRAY:
					case COLUMN_TYPE_BOOL_ARRAY:
					case COLUMN_TYPE_BYTE_ARRAY:
					case COLUMN_TYPE_SHORT_ARRAY:
					case COLUMN_TYPE_INT_ARRAY:
					case COLUMN_TYPE_LONG_ARRAY:
					case COLUMN_TYPE_FLOAT_ARRAY:
					case COLUMN_TYPE_DOUBLE_ARRAY:
					case COLUMN_TYPE_TIMESTAMP_ARRAY:
					case COLUMN_TYPE_WITH_BEGIN:
					default:
						ss << "(unknown type)";
						break;
					}
				}
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
						txn, objectManager, headerOId, OBJECT_READ_ONLY);
					uint32_t num = arrayCursor.getArrayLength();
					ss << num << ",";
					ss << "totalSize=" << totalSize << "[";
					uint32_t count = 0;
					uint8_t *elem;
					uint32_t elemSize;
					uint32_t elemCount;
					while (arrayCursor.nextElement()) {
						if (count != 0) {
							ss << ",";
						}
						util::NormalXArray<uint8_t> binary;
						elem = arrayCursor.getElement(elemSize, elemCount);
						if (elemSize > 0) {
							elem += ValueProcessor::getEncodedVarSize(elemSize);
							binary.push_back(elem, elemSize);
						}
						binary.push_back('\0');
						ss << "(STRING)"
						   << reinterpret_cast<char *>(binary.data());
						++count;
					}
					assert(num == count);
				}
			}
		}
		else {
			ss << "0[";
		}
		ss << "]";
	}
	else {
		switch (getType()) {
		case COLUMN_TYPE_STRING: {
			util::NormalXArray<uint8_t> binary;
			const uint8_t *str = reinterpret_cast<const uint8_t *>(data());
			ss << "(STRING)";
			if (str != NULL) {
				binary.push_back(str, size());
				binary.push_back('\0');
				ss << reinterpret_cast<char *>(binary.data());
			}
			else {
				ss << "NULL";
			}
		} break;
		case COLUMN_TYPE_BOOL:
			ss << "(BOOL)";
			ss << getBool();
			break;
		case COLUMN_TYPE_BYTE: {
			ss << "(BYTE)";
			int16_t byteVal = getByte();
			ss << byteVal;
		} break;
		case COLUMN_TYPE_SHORT:
			ss << "(SHORT)";
			ss << getShort();
			break;
		case COLUMN_TYPE_INT:
			ss << "(INT)";
			ss << getInt();
			break;
		case COLUMN_TYPE_LONG:
			ss << "(LONG)";
			ss << getLong();
			break;
		case COLUMN_TYPE_FLOAT:
			ss << "(FLOAT)";
			ss << getFloat();
			break;
		case COLUMN_TYPE_DOUBLE:
			ss << "(DOUBLE)";
			ss << getDouble();
			break;
		case COLUMN_TYPE_TIMESTAMP:
			ss << "(TIMESTAMP)";
			ss << getTimestamp();
			break;
		case COLUMN_TYPE_BLOB: {
			ss << "(BLOB)length=" << data_.object_.totalSize_;
			uint64_t length = 0;
			if (data() != NULL) {
				const uint8_t *blobHeaderData =
					reinterpret_cast<const uint8_t *>(data());
				uint32_t elemSize =
					ValueProcessor::decodeVarSize(blobHeaderData);
				blobHeaderData += ValueProcessor::getEncodedVarSize(elemSize);
				uint32_t totalSize =
					*reinterpret_cast<const uint32_t *>(blobHeaderData);
				blobHeaderData += sizeof(uint32_t);
				ss << ", totalSize=" << totalSize;

				OId headerOId = *reinterpret_cast<const OId *>(blobHeaderData);
				if (headerOId != UNDEF_OID) {
					ArrayObject arrayObject(txn, objectManager, headerOId);
					uint32_t num = arrayObject.getArrayLength();
					for (uint32_t i = 0; i < num; i++) {
						const uint8_t *elementObject =
							arrayObject.getArrayElement(i, COLUMN_TYPE_OID);
						OId srcElemOId =
							*reinterpret_cast<const OId *>(elementObject);
						if (srcElemOId != UNDEF_OID) {
							BinaryObject srcElemVariant(
								txn, objectManager, srcElemOId);
							length += srcElemVariant.size();
							ss << ",(" << i << ",OID=" << srcElemOId
							   << ",size=" << srcElemVariant.size() << ")";
						}
						else {
							ss << ",(" << i << ",OID=UNDEF_OID)";
						}
					}
				}
			}
			assert(length == data_.object_.totalSize_);
		} break;
		case COLUMN_TYPE_OID:
		case COLUMN_TYPE_STRING_ARRAY:
		case COLUMN_TYPE_BOOL_ARRAY:
		case COLUMN_TYPE_BYTE_ARRAY:
		case COLUMN_TYPE_SHORT_ARRAY:
		case COLUMN_TYPE_INT_ARRAY:
		case COLUMN_TYPE_LONG_ARRAY:
		case COLUMN_TYPE_FLOAT_ARRAY:
		case COLUMN_TYPE_DOUBLE_ARRAY:
		case COLUMN_TYPE_TIMESTAMP_ARRAY:
		case COLUMN_TYPE_WITH_BEGIN:
		default:
			ss << "(unknown type)";
			break;
		}
	}
	return ss.str();
}

/*!
	@brief Get element of array
*/
void Value::getArrayElement(TransactionContext &txn,
	ObjectManager &objectManager, uint32_t i, const uint8_t *&data,
	uint32_t &size) const {
	data = NULL;
	size = 0;
	if (isArray() && data_.object_.value_ != 0) {
		uint32_t num = getArrayLength(txn, objectManager);
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
					arrayObject.getArrayElement(i, simpleType);
				data = elementObject;
				size = FixedSizeOfColumnType[simpleType];
			} break;
			case COLUMN_TYPE_STRING: {
				const MatrixCursor *arrayObject =
					reinterpret_cast<const MatrixCursor *>(
						data_.object_.value_);
				if (data_.object_.onDataStore_) {
					VariableArrayCursor arrayCursor(
						txn, objectManager, arrayObject->getHeaderOId(), OBJECT_READ_ONLY);

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
void Value::copy(TransactionContext &txn, ObjectManager &objectManager,
	const Value &srcValue) {
	PartitionId pId = txn.getPartitionId();
	util::StackAllocator &alloc = txn.getDefaultAllocator();
	switch (srcValue.getType()) {
	case COLUMN_TYPE_STRING:
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
		ArrayObject destObject(txn, objectManager);
		if (srcValue.data_.object_.value_ != NULL) {
			const ArrayObject srcObject(reinterpret_cast<uint8_t *>(
				const_cast<void *>(srcValue.data_.object_.value_)));
			ColumnType simpleType =
				ValueProcessor::getSimpleColumnType(srcValue.getType());
			uint32_t objectSize = ArrayObject::getObjectSize(
				srcObject.getArrayLength(), COLUMN_TYPE_OID);

			uint32_t num = srcObject.getArrayLength();

			destObject.setBaseAddr(
				reinterpret_cast<uint8_t *>(alloc.allocate(objectSize)));

			destObject.setArrayLength(num, simpleType);

			for (uint32_t i = 0; i < num; i++) {
				BinaryObject srcElemVariant(txn, objectManager);
				BinaryObject destElemVariant(txn, objectManager);
				if (srcValue.onDataStore()) {
					const OId *srcElemOId = reinterpret_cast<const OId *>(
						srcObject.getArrayElement(i, simpleType));

					if (*srcElemOId != UNDEF_OID) {
						if (pId == UNDEF_PARTITIONID) {
							GS_THROW_USER_ERROR(GS_ERROR_CM_NOT_SUPPORTED,
								"not support copy type");
						}
						srcElemVariant.load(*srcElemOId);
					}
				}
				else {
					srcElemVariant.setBaseAddr(const_cast<uint8_t *>(
						srcObject.getArrayElement(i, simpleType)));
				}
				if (srcElemVariant.getBaseAddr() != NULL) {
					destElemVariant.setBaseAddr(reinterpret_cast<uint8_t *>(
						alloc.allocate(BinaryObject::getObjectSize(
							srcElemVariant.size()))));
					destElemVariant.setData(
						srcElemVariant.size(), srcElemVariant.data());
				}

				destObject.setArrayElement(
					i, simpleType, destElemVariant.getBaseAddr());
			}
		}

		type_ = srcValue.getType();
		data_.object_.set(destObject.getImage(), false);
	} break;
	case COLUMN_TYPE_STRING_ARRAY: {
		ArrayObject destObject(txn, objectManager);
		if (srcValue.data_.object_.value_ != NULL) {
			const ArrayObject srcObject(reinterpret_cast<uint8_t *>(
				const_cast<void *>(srcValue.data_.object_.value_)));
			ColumnType simpleType =
				ValueProcessor::getSimpleColumnType(srcValue.getType());
			uint32_t objectSize = ArrayObject::getObjectSize(
				srcObject.getArrayLength(), COLUMN_TYPE_OID);

			uint32_t num = srcObject.getArrayLength();

			destObject.setBaseAddr(
				reinterpret_cast<uint8_t *>(alloc.allocate(objectSize)));

			destObject.setArrayLength(num, simpleType);
			for (uint32_t i = 0; i < num; i++) {
				const char *srcElemVariant;
				char *destElemVariant;
				BaseObject baseObject(pId, objectManager);
				if (srcValue.onDataStore()) {
					const OId *srcElemOId = reinterpret_cast<const OId *>(
						srcObject.getArrayElement(i, simpleType));

					if (*srcElemOId != UNDEF_OID) {
						if (pId == UNDEF_PARTITIONID) {
							GS_THROW_USER_ERROR(GS_ERROR_CM_NOT_SUPPORTED,
								"not support copy type");
						}
						baseObject.load(*srcElemOId);
						srcElemVariant = baseObject.getBaseAddr<const char *>();
					}
					else {
						srcElemVariant = NULL;
					}
				}
				else {
					srcElemVariant = reinterpret_cast<const char *>(
						srcObject.getArrayElement(i, simpleType));
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

				destObject.setArrayElement(i, simpleType,
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
				srcObject.getArrayLength(), COLUMN_TYPE_OID);

			destObject = alloc.allocate(objectSize);
			memcpy(destObject, srcObject.getImage(), objectSize);
		}
		else {
			destObject = NULL;
		}

		type_ = srcValue.getType();
		data_.object_.set(destObject, false);
	} break;
	case COLUMN_TYPE_WITH_BEGIN:
	default:
		break;
	}
}


void Value::get(TransactionContext &txn, ObjectManager &objectManager,
	MessageRowStore *messageRowStore, ColumnId columnId) {
	ValueProcessor::getField(
		txn, objectManager, columnId, this, messageRowStore);
}
