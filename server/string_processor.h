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
	@brief Definition of StringProcessor
*/
#ifndef STRING_PROCESSOR_H_
#define STRING_PROCESSOR_H_

#include "value_processor.h"

class MessageRowStore;

/*!
	@brief Processes Field value of String-type
*/
class StringProcessor {
public:
	static int32_t compare(TransactionContext &txn,
		ObjectManager &objectManager, ColumnId columnId,
		MessageRowStore *messageRowStore, uint8_t *objectRowField);

	static int32_t compare(TransactionContext &txn,
		ObjectManager &objectManager, ColumnType type,
		uint8_t *srcObjectRowField, uint8_t *targetObjectRowField);

	static void getField(TransactionContext &txn, ObjectManager &objectManager,
		ColumnId columnId, Value *objectValue,
		MessageRowStore *outputMessageRowStore);
};

#endif
