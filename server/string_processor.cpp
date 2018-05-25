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
	@brief Implementation of StringProcessor
*/
#include "string_processor.h"
#include "util/time.h"
#include "gs_error.h"
#include "message_row_store.h"
#include "schema.h"
#include "value_operator.h"

/*!
	@brief Compare message field value with object field value
*/
int32_t StringProcessor::compare(TransactionContext &txn, ObjectManager &,
	ColumnId columnId, MessageRowStore *messageRowStore,
	uint8_t *objectRowField) {
	const uint8_t *inputField;
	uint32_t inputFieldSize;
	messageRowStore->getField(columnId, inputField, inputFieldSize);
	inputField += ValueProcessor::getEncodedVarSize(inputFieldSize);

	uint32_t objectRowFieldSize = ValueProcessor::decodeVarSize(objectRowField);
	objectRowField += ValueProcessor::getEncodedVarSize(objectRowFieldSize);
	if (objectRowFieldSize == 0) {
		objectRowField = NULL;
	}
	return compareStringString(
		txn, inputField, inputFieldSize, objectRowField, objectRowFieldSize);
}

/*!
	@brief Compare object field values
*/
int32_t StringProcessor::compare(TransactionContext &txn, ObjectManager &,
	ColumnType, uint8_t *srcObjectRowField, uint8_t *targetObjectRowField) {
	int32_t result;

	uint32_t srcObjectRowFieldSize = 0;
	if (srcObjectRowField) {
		srcObjectRowFieldSize =
			ValueProcessor::decodeVarSize(srcObjectRowField);
		srcObjectRowField +=
			ValueProcessor::getEncodedVarSize(srcObjectRowFieldSize);
		if (srcObjectRowFieldSize == 0) {
			srcObjectRowField = NULL;
		}
	}

	uint32_t targetObjectRowFieldSize = 0;
	if (targetObjectRowField) {
		targetObjectRowFieldSize =
			ValueProcessor::decodeVarSize(targetObjectRowField);
		targetObjectRowField +=
			ValueProcessor::getEncodedVarSize(targetObjectRowFieldSize);
		if (targetObjectRowFieldSize == 0) {
			targetObjectRowField = NULL;
		}
	}

	result = compareStringString(txn, srcObjectRowField, srcObjectRowFieldSize,
		targetObjectRowField, targetObjectRowFieldSize);
	return result;
}

/*!
	@brief Set field value to message
*/
void StringProcessor::getField(TransactionContext &, ObjectManager &,
	ColumnId columnId, Value *objectValue, MessageRowStore *messageRowStore) {
	const uint8_t *data = objectValue->getImage();
	if (data != NULL) {
		uint32_t size = objectValue->size();
		messageRowStore->setField(columnId, data, size);
	}
	else {
		messageRowStore->setField(columnId, NULL, 0);
	}
}
