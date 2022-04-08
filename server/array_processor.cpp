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
	@brief Implementation of ArrayProcessor
*/
#include "array_processor.h"
#include "util/time.h"
#include "gs_error.h"
#include "message_row_store.h"
#include "schema.h"
#include "value_operator.h"

/*!
	@brief Compare message field value with object field value
*/
int32_t ArrayProcessor::compare(TransactionContext &txn, ObjectManagerV4 &, AllocateStrategy &,
	ColumnId columnId, MessageRowStore *messageRowStore,
	uint8_t *objectRowField) {
	const uint8_t *inputField;
	uint32_t inputFieldSize;
	messageRowStore->getField(columnId, inputField, inputFieldSize);
	inputField += ValueProcessor::getEncodedVarSize(inputFieldSize);


	int32_t result;
	uint32_t objectRowFieldSize = ValueProcessor::decodeVarSize(objectRowField);
	objectRowField += ValueProcessor::getEncodedVarSize(objectRowFieldSize);

	result = compareBinaryBinary(
		txn, inputField, inputFieldSize, objectRowField, objectRowFieldSize);
	return result;
}

/*!
	@brief Compare object field values
*/
int32_t ArrayProcessor::compare(TransactionContext &txn, ObjectManagerV4 &, AllocateStrategy &,
	ColumnType, uint8_t *srcObjectRowField, uint8_t *targetObjectRowField) {
	int32_t result;
	uint32_t srcObjectRowFieldSize = 0;
	if (srcObjectRowField) {
		srcObjectRowFieldSize =
			ValueProcessor::decodeVarSize(srcObjectRowField);
		srcObjectRowField +=
			ValueProcessor::getEncodedVarSize(srcObjectRowFieldSize);
	}

	uint32_t targetObjectRowFieldSize = 0;
	if (targetObjectRowField) {
		targetObjectRowFieldSize =
			ValueProcessor::decodeVarSize(targetObjectRowField);
		targetObjectRowField +=
			ValueProcessor::getEncodedVarSize(targetObjectRowFieldSize);
	}

	result = compareBinaryBinary(txn, srcObjectRowField, srcObjectRowFieldSize,
		targetObjectRowField, targetObjectRowFieldSize);
	return result;
}

/*!
	@brief Set field value to message
*/
void ArrayProcessor::getField(TransactionContext &, ObjectManagerV4 &, AllocateStrategy &,
	ColumnId columnId, const Value *objectValue, MessageRowStore *messageRowStore) {
	messageRowStore->setArrayField(columnId);
	if (objectValue->data() != NULL) {
		const ArrayObject arrayObject(
			const_cast<uint8_t *>(objectValue->data()));
		ColumnType simpleType = messageRowStore->getColumnInfoList()[columnId]
									.getSimpleColumnType();
		uint32_t num = arrayObject.getArrayLength();
		uint32_t totalSize = ValueProcessor::getEncodedVarSize(num) +
							 FixedSizeOfColumnType[simpleType] * num;
		messageRowStore->setVarDataHeaderField(
			columnId, totalSize);		   
		messageRowStore->setVarSize(num);  
		for (uint32_t i = 0; i < num; i++) {
			const uint8_t *elemData =
				arrayObject.getArrayElement(i, FixedSizeOfColumnType[simpleType]);
			messageRowStore->addArrayElement(
				elemData, FixedSizeOfColumnType[simpleType]);
		}
	}
	else {
		messageRowStore->setVarDataHeaderField(
			columnId, 1);  
		messageRowStore->setVarSize(0);  
	}
}
