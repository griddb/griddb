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
	@brief Implementation of Schemas for container
*/
#include "schema.h"
#include "btree_map.h"
#include "data_store.h"
#include "hash_map.h"
#include "rtree_map.h"
#include "message_schema.h"
#include "value_processor.h"

TriggerInfo::TriggerInfo(util::StackAllocator &alloc)
	: alloc_(alloc),
	  version_(TRIGGER_VERSION),
	  type_(-1),
	  name_(alloc_),
	  uri_(alloc_),
	  operation_(0),
	  columnIds_(alloc_),
	  jmsProviderType_(-1),
	  jmsProviderTypeName_(alloc_),
	  jmsDestinationType_(-1),
	  jmsDestinationTypeName_(alloc_),
	  jmsDestinationName_(alloc_),
	  jmsUser_(alloc_),
	  jmsPassword_(alloc_) {}

TriggerInfo::TriggerInfo(const TriggerInfo &info)
	: alloc_(info.alloc_),
	  version_(info.version_),
	  type_(info.type_),
	  name_(info.name_),
	  uri_(info.uri_),
	  operation_(info.operation_),
	  columnIds_(alloc_),
	  jmsProviderType_(info.jmsProviderType_),
	  jmsProviderTypeName_(info.jmsProviderTypeName_),
	  jmsDestinationType_(info.jmsDestinationType_),
	  jmsDestinationTypeName_(info.jmsDestinationTypeName_),
	  jmsDestinationName_(info.jmsDestinationName_),
	  jmsUser_(info.jmsUser_),
	  jmsPassword_(info.jmsPassword_) {
	columnIds_.assign(info.columnIds_.begin(), info.columnIds_.end());
}

/*!
	@brief Encode as Message format
*/
void TriggerInfo::encode(
	const TriggerInfo &info, util::XArray<uint8_t> &binaryTrigger) {
	try {
		util::XArrayOutStream<> arrayOut(binaryTrigger);
		util::ByteStream<util::XArrayOutStream<> > out(arrayOut);


		const size_t sizePos = out.base().position();
		uint32_t totalSize = 0;
		out << totalSize;

		const size_t bodyPos = out.base().position();

		out << info.version_;
		out << info.type_;
		out << info.name_;
		out << info.uri_;
		out << info.operation_;

		out << static_cast<uint32_t>(info.columnIds_.size());
		for (util::XArray<ColumnId>::const_iterator it =
				 info.columnIds_.begin();
			 it != info.columnIds_.end(); ++it) {
			out << *it;
		}

		out << info.jmsProviderType_;
		out << info.jmsDestinationType_;
		out << info.jmsDestinationName_;
		out << info.jmsUser_;
		out << info.jmsPassword_;

		const size_t endPos = out.base().position();

		totalSize = static_cast<uint32_t>(endPos - bodyPos);

		out.base().position(sizePos);
		out << totalSize;
		out.base().position(endPos);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e,
			"Trigger encode Failed(triggerName="
				<< info.name_ << ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Decode Message format
*/
void TriggerInfo::decode(const uint8_t *binaryTrigger, TriggerInfo &info) {
	try {
		uint32_t totalSize = 0;
		getSizeAndVersion(binaryTrigger, totalSize, info.version_);

		{
			util::ArrayInStream arrayIn(
				binaryTrigger + sizeof(totalSize), totalSize);
			util::ArrayByteInStream in(arrayIn);

			in >> info.version_;
			in >> info.type_;
			in >> info.name_;
			in >> info.uri_;
			in >> info.operation_;

			uint32_t columnIdCount;
			in >> columnIdCount;
			info.columnIds_.resize(columnIdCount);
			for (uint32_t i = 0; i < columnIdCount; i++) {
				in >> info.columnIds_[i];
			}

			in >> info.jmsProviderType_;
			in >> info.jmsDestinationType_;
			in >> info.jmsDestinationName_;
			in >> info.jmsUser_;
			in >> info.jmsPassword_;
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e,
			"Trigger decode Failed(triggerName="
				<< info.name_ << ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Get data size and version of Trigger
*/
void TriggerInfo::getSizeAndVersion(
	const uint8_t *binaryTrigger, uint32_t &size, int32_t &version) {
	try {
		if (binaryTrigger == NULL) {
			size = 0;
			version = -1;
			return;
		}

		util::ArrayInStream arrayIn(
			binaryTrigger, sizeof(size) + sizeof(version));
		util::ArrayByteInStream in(arrayIn);
		in >> size;
		in >> version;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e,
			"Trigger decode Failed(reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Decode Trigers
*/
bool TriggerInfo::compare(
	const uint8_t *binaryTrigger1, const uint8_t *binaryTrigger2) {
	uint32_t size1, size2;
	int32_t version1, version2;

	getSizeAndVersion(binaryTrigger1, size1, version1);
	getSizeAndVersion(binaryTrigger2, size2, version2);

	if (size1 != size2) {
		return false;
	}
	else {
		return (memcmp(binaryTrigger1 + sizeof(size1),
					binaryTrigger2 + sizeof(size2), size1) == 0);
	}
}

/*!
	@brief Initialize the area in TriggerList
*/
void TriggerList::initialize(uint32_t num) {
	num_ = num;
	if (num_ > 0) {
		OId *list = getOIdList();
		for (uint32_t i = 0; i < num_; ++i) {
			list[i] = UNDEF_OID;
		}
	}
}

void TriggerList::set(TransactionContext &txn, ObjectManager &objectManager,
	util::XArray<const uint8_t *> &triggerList,
	const AllocateStrategy &allocateStrategy) {
	if (triggerList.size() != num_) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_UNEXPECTED_ERROR, "");
	}
	if (num_ > 0) {
		OId *list = getOIdList();
		for (uint32_t i = 0; i < num_; ++i) {
			uint32_t size;
			int32_t version;
			TriggerInfo::getSizeAndVersion(triggerList[i], size, version);

			BaseObject triggerBinaryObject(txn.getPartitionId(), objectManager);
			uint8_t *obj =
				triggerBinaryObject.allocate<uint8_t>(size + sizeof(size),
					allocateStrategy, list[i], OBJECT_TYPE_EVENTLIST);
			memcpy(obj, triggerList[i], size + sizeof(size));
		}
	}
}

/*!
	@brief Newly creates or updates a Trigger
*/
void TriggerList::createImage(TransactionContext &txn,
	ObjectManager &objectManager, const TriggerInfo &info,
	util::XArray<const uint8_t *> &triggerList, size_t limit) {
	bool isExist = false;

	util::StackAllocator &alloc = txn.getDefaultAllocator();

	util::XArray<uint8_t> *binaryTrigger =
		ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
	TriggerInfo::encode(info, *binaryTrigger);

	if (binaryTrigger->size() > limit) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
			"trigger name, uri, or destinationName is too long. (size="
				<< binaryTrigger->size() << ")");
	}

	OId *list = getOIdList();

	for (uint32_t i = 0; i < num_; ++i) {
		BaseObject encodeObject(txn.getPartitionId(), objectManager, list[i]);
		const uint8_t *encodedTrigger = encodeObject.getBaseAddr<uint8_t *>();
		TriggerInfo trigger(alloc);
		TriggerInfo::decode(encodedTrigger, trigger);

		if (info.name_.compare(trigger.name_) == 0) {
			triggerList.push_back(binaryTrigger->data());
			isExist = true;
		}
		else {
			Size_t objectSize =
				objectManager.getSize(encodeObject.getBaseAddr());
			uint8_t *existEncodedTrigger = reinterpret_cast<uint8_t *>(
				txn.getDefaultAllocator().allocate(objectSize));
			memcpy(existEncodedTrigger, encodeObject.getBaseAddr(), objectSize);
			triggerList.push_back(existEncodedTrigger);
		}
	}
	if (!isExist) {
		triggerList.push_back(binaryTrigger->data());
	}

	GS_TRACE_DEBUG(DATA_STORE, GS_TRACE_DS_CON_CREATE_TRIGGER,
		"numTrigger=" << num_ << ", triggerName=" << info.name_
					  << ", triggerType=" << info.type_
					  << ", isExist=" << isExist);
}

/*!
	@brief Updates ColumnId of TriggerInfo
*/
bool TriggerList::updateImage(TransactionContext &txn,
	ObjectManager &objectManager,
	const util::XArray<const util::String *> &oldColumnNameList,
	const util::XArray<const util::String *> &newColumnNameList,
	util::XArray<const uint8_t *> &triggerList) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	bool isExist = false;

	if (num_ > 0) {
		isExist = true;
		OId *list = getOIdList();

		for (uint32_t i = 0; i < num_; i++) {
			BaseObject triggerObject(
				txn.getPartitionId(), objectManager, list[i]);
			Size_t objectSize =
				objectManager.getSize(triggerObject.getBaseAddr());
			uint8_t *encodedTrigger = reinterpret_cast<uint8_t *>(
				txn.getDefaultAllocator().allocate(objectSize));
			memcpy(encodedTrigger, triggerObject.getBaseAddr(), objectSize);

			TriggerInfo before(alloc);
			TriggerInfo::decode(encodedTrigger, before);

			util::XArray<ColumnId> afterColumnIds(alloc);
			for (size_t j = 0; j < before.columnIds_.size(); j++) {
				const ColumnId oldColumnId = before.columnIds_[j];
				const util::String *oldColumName = oldColumnNameList[oldColumnId];

				for (uint32_t newColumnId = 0;
					 newColumnId <
					 static_cast<uint32_t>(newColumnNameList.size());
					 newColumnId++) {
					const util::String *newColumnName =
						newColumnNameList[newColumnId];
					if (newColumnName->compare(*oldColumName) == 0) {
						afterColumnIds.push_back(newColumnId);
						break;
					}
				}
			}

			{
				TriggerInfo after = before;
				after.columnIds_.clear();
				after.columnIds_.assign(
					afterColumnIds.begin(), afterColumnIds.end());

				util::XArray<uint8_t> *binaryTrigger =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
				TriggerInfo::encode(after, *binaryTrigger);

				triggerList.push_back(binaryTrigger->data());

				GS_TRACE_DEBUG(DATA_STORE, GS_TRACE_DS_CON_UPDATE_TRIGGER,
					"numTrigger=" << num_ << ", triggerName=" << after.name_
								  << ", triggerType=" << after.type_);
			}
		}
	}
	return isExist;
}

/*!
	@brief Deletes Trigger
*/
bool TriggerList::removeImage(TransactionContext &txn,
	ObjectManager &objectManager, const char *name,
	util::XArray<const uint8_t *> &triggerList) {
	util::StackAllocator &alloc = *triggerList.get_allocator().base();

	bool isExist = false;
	if (num_ > 0) {
		OId *list = getOIdList();
		for (uint32_t i = 0; i < num_; ++i) {
			BaseObject encodeObject(
				txn.getPartitionId(), objectManager, list[i]);
			const uint8_t *encodedTrigger =
				encodeObject.getBaseAddr<uint8_t *>();

			TriggerInfo trigger(alloc);
			TriggerInfo::decode(encodedTrigger, trigger);

			if (trigger.name_.compare(name) == 0) {
				isExist = true;
			}
			else {
				Size_t objectSize =
					objectManager.getSize(encodeObject.getBaseAddr());
				uint8_t *otherEncodedTrigger = reinterpret_cast<uint8_t *>(
					txn.getDefaultAllocator().allocate(objectSize));
				memcpy(otherEncodedTrigger, encodeObject.getBaseAddr(),
					objectSize);
				triggerList.push_back(otherEncodedTrigger);
			}
		}
	}
	return isExist;
}

/*!
	@brief Get list of Trigger
*/
void TriggerList::getList(TransactionContext &txn, ObjectManager &objectManager,
	util::XArray<const uint8_t *> &triggerList) {
	if (num_ > 0) {
		OId *list = getOIdList();
		for (uint32_t i = 0; i < num_; ++i) {
			BaseObject triggerListObject(
				txn.getPartitionId(), objectManager, list[i]);
			Size_t objectSize =
				objectManager.getSize(triggerListObject.getBaseAddr());
			uint8_t *encodedTrigger = reinterpret_cast<uint8_t *>(
				txn.getDefaultAllocator().allocate(objectSize));
			memcpy(encodedTrigger, triggerListObject.getBaseAddr(), objectSize);
			triggerList.push_back(encodedTrigger);
		}
	}
}

/*!
	@brief Free Objects related to TriggerList
*/
void TriggerList::finalize(
	TransactionContext &txn, ObjectManager &objectManager) {
	if (num_ > 0) {
		OId *list = getOIdList();
		for (uint32_t i = 0; i < num_; ++i) {
			objectManager.free(txn.getPartitionId(), list[i]);
		}
	}
}

/*!
	@brief Initialize the area in ColumnInfo
*/
void ColumnInfo::initialize() {
	columnNameOId_ = UNDEF_OID;
	columnType_ = COLUMN_TYPE_WITH_BEGIN;
	offset_ = 0;
	flags_ = 0;
}

void ColumnInfo::set(TransactionContext &txn, ObjectManager &objectManager,
	uint32_t toColumnId, uint32_t fromColumnId, MessageSchema *messageSchema,
	const AllocateStrategy &allocateStrategy, bool onMemory) {
	columnId_ = static_cast<uint16_t>(toColumnId);

	const util::String &columnName = messageSchema->getColumnName(fromColumnId);
	uint32_t size = static_cast<uint32_t>(columnName.size());

	char *stringPtr = NULL;
	if (onMemory) {
		columnNameOnMemory_ = static_cast<char *>(txn.getDefaultAllocator().allocate(size + 1));
		strcpy(columnNameOnMemory_, columnName.c_str());
	} else {
		BaseObject stringObject(txn.getPartitionId(), objectManager);
		stringPtr = stringObject.allocate<char>(
			size + 1, allocateStrategy, columnNameOId_, OBJECT_TYPE_VARIANT);
		memcpy(stringPtr, columnName.c_str(), size + 1);
	}

	setType(messageSchema->getColumnType(fromColumnId),
		messageSchema->getIsArray(fromColumnId));

	flags_ = 0;

//	int64_t hashVal = messageSchema->getColumnCount();
	const util::XArray<ColumnId> &keyColumnIds =
		messageSchema->getRowKeyColumnIdList();
	util::XArray<ColumnId>::const_iterator itr = 
		std::find(keyColumnIds.begin(), keyColumnIds.end(), fromColumnId);
	if (itr != keyColumnIds.end()) {
		flags_ |= COLUMN_FLAG_KEY;
	}

	if (messageSchema->getIsVirtual(fromColumnId)) {
		flags_ |= COLUMN_FLAG_VIRTUAL;
	}
	if (messageSchema->getIsNotNull(fromColumnId)) {
		flags_ |= COLUMN_FLAG_NOT_NULL;
	}
}

/*!
	@brief Free Objects related to ColumnInfo
*/
void ColumnInfo::finalize(
	TransactionContext &txn, ObjectManager &objectManager) {
	if (columnNameOId_ != UNDEF_OID) {
		objectManager.free(txn.getPartitionId(), columnNameOId_);
		columnNameOId_ = UNDEF_OID;
	}
}

/*!
	@brief translate into Message format
*/
void ColumnInfo::getSchema(TransactionContext &txn,
	ObjectManager &objectManager, util::XArray<uint8_t> &schema) {
	char *columnName = const_cast<char *>(getColumnName(txn, objectManager));
	int32_t columnNameLen = static_cast<int32_t>(strlen(columnName));
	schema.push_back(
		reinterpret_cast<uint8_t *>(&columnNameLen), sizeof(int32_t));
	schema.push_back(reinterpret_cast<uint8_t *>(columnName), columnNameLen);

	int8_t tmp = static_cast<int8_t>(getSimpleColumnType());
	schema.push_back(reinterpret_cast<uint8_t *>(&tmp), sizeof(int8_t));

	uint8_t flag = (isArray() ? 1 : 0);
	flag |= (isVirtual() ? COLUMN_FLAG_VIRTUAL : 0);
	flag |= (isNotNull() ? COLUMN_FLAG_NOT_NULL : 0);
	schema.push_back(reinterpret_cast<uint8_t *>(&flag), sizeof(uint8_t));
}

void MessageCompressionInfo::initialize() {
	type_ = NONE;
	threshhold_ = 0;
	threshholdRelative_ = 0;
	rate_ = 0;
	span_ = 0;
}

void MessageCompressionInfo::set(CompressionType type, bool threshholdRelative,
	double threshhold, double rate, double span) {
	type_ = type;
	threshholdRelative_ = threshholdRelative;
	threshhold_ = threshhold;
	rate_ = rate;
	span_ = span;
}

void MessageCompressionInfo::finalize() {
}

std::string ColumnInfo::dump(
	TransactionContext &txn, ObjectManager &objectManager) {
	util::NormalOStringStream strstrm;
	const char *name = this->getColumnName(txn, objectManager);
	strstrm << "columnId_=" << columnId_ << ", "
			<< "columnName=" << name << ", "
			<< "columnType_=" << columnType_ << ", "
			<< "columnSize_=" << columnSize_ << ", "
			<< "offset_=" << offset_ << ", "
			<< "isKey=" << isKey() << ", "
			<< "isArray=" << isArray() << ", " << std::endl;

	return strstrm.str();
}

/*!
	@brief Initialize the area in ColumnSchema
*/
void ColumnSchema::initialize(uint32_t columnNum) {
	columnNum_ = columnNum;
	rowFixedColumnSize_ = 0;
	firstColumnNum_ = 0;
	firstVarColumnNum_ = 0;
	firstRowFixedColumnSize_ = 0;
}

/*!
	@brief Free Objects related to ColumnSchema
*/
void ColumnSchema::finalize(
	TransactionContext &txn, ObjectManager &objectManager) {
	for (uint32_t i = 0; i < columnNum_; i++) {
		ColumnInfo &columnInfo = getColumnInfo(i);
		columnInfo.finalize(txn, objectManager);
	}
}

/*!
	@brief Set Column layout
*/
void ColumnSchema::set(TransactionContext &txn, ObjectManager &objectManager,
	MessageSchema *messageSchema, const AllocateStrategy &allocateStrategy,
	bool onMemory) {
	ColumnInfo *columnInfoList = getColumnInfoList();
	if (!messageSchema->getRowKeyColumnIdList().empty()) {
		ColumnId messageRowKeyColumnId = messageSchema->getRowKeyColumnIdList().front();
		bool hasVariableColumn = false;
		uint32_t columnId = 1;
		for (uint32_t i = 0; i < columnNum_; i++) {
			if (i == messageRowKeyColumnId) {
				columnInfoList[0].initialize();
				columnInfoList[0].set(
					txn, objectManager, 0, i, messageSchema, allocateStrategy, onMemory);
				if (!hasVariableColumn && columnInfoList[0].isVariable()) {
					hasVariableColumn = true;
				}
			}
			else {
				columnInfoList[columnId].initialize();
				columnInfoList[columnId].set(txn, objectManager, columnId, i,
					messageSchema, allocateStrategy, onMemory);
				if (!hasVariableColumn &&
					columnInfoList[columnId].isVariable()) {
					hasVariableColumn = true;
				}
				columnId++;
			}
		}
		uint32_t nullsAndVarOffset =
			ValueProcessor::calcNullsByteSize(columnNum_) +
			(hasVariableColumn ? sizeof(OId) : 0);
		rowFixedColumnSize_ = 0;
		uint16_t variableColumnIndex = 0;  
		for (uint16_t i = 0; i < columnNum_; i++) {
			if (columnInfoList[i].isVariable()) {
				columnInfoList[i].setOffset(
					variableColumnIndex);  
				++variableColumnIndex;
			}
			else {
				columnInfoList[i].setOffset(nullsAndVarOffset + rowFixedColumnSize_);
				rowFixedColumnSize_ += columnInfoList[i].getColumnSize();
			}
		}
		variableColumnNum_ = variableColumnIndex;
	}
	else {
		uint16_t variableColumnIndex = 0;  
		uint32_t nullsAndVarOffset =
			ValueProcessor::calcNullsByteSize(columnNum_);  
		for (uint16_t i = 0; i < columnNum_; i++) {
			columnInfoList[i].initialize();
			columnInfoList[i].set(
				txn, objectManager, i, i, messageSchema, allocateStrategy,
				onMemory);
			if (columnInfoList[i].isVariable()) {
				columnInfoList[i].setOffset(
					variableColumnIndex);  
				++variableColumnIndex;
			}
			else {
				columnInfoList[i].setOffset(nullsAndVarOffset + rowFixedColumnSize_);
				rowFixedColumnSize_ += columnInfoList[i].getColumnSize();
			}
		}
		variableColumnNum_ = variableColumnIndex;

		if (variableColumnIndex > 0) {
			nullsAndVarOffset =
				ValueProcessor::calcNullsByteSize(columnNum_) + sizeof(OId);
			rowFixedColumnSize_ = 0;
			for (uint16_t i = 0; i < columnNum_; i++) {
				if (!columnInfoList[i].isVariable()) {
					columnInfoList[i].setOffset(
						nullsAndVarOffset + rowFixedColumnSize_);
					rowFixedColumnSize_ += columnInfoList[i].getColumnSize();
				}
			}
		}
	}
	{
		uint16_t *rowKeyNumPtr = reinterpret_cast<uint16_t *>(getRowKeyPtr());
		const util::XArray<ColumnId> &schemaKeyColumnIdList = messageSchema->getRowKeyColumnIdList();
		uint16_t rowKeyNum = static_cast<uint16_t>(schemaKeyColumnIdList.size());
		*rowKeyNumPtr = rowKeyNum;
		rowKeyNumPtr++;
		for (size_t i = 0; i < schemaKeyColumnIdList.size(); i++) {
			uint16_t columnId = static_cast<uint16_t>(schemaKeyColumnIdList[i]);
			*rowKeyNumPtr = columnId;
			rowKeyNumPtr++;
		}
	}
	uint32_t columnNum, varColumnNum, rowFixedColumnSize;
	messageSchema->getFirstSchema(columnNum, varColumnNum, rowFixedColumnSize);
	if (columnNum != 0) {
		setFirstSchema(columnNum, varColumnNum, rowFixedColumnSize);
	}
}

void ColumnSchema::getColumnInfo(TransactionContext &txn,
	ObjectManager &objectManager, const char *name, uint32_t &columnId,
	ColumnInfo *&columnInfo, bool isCaseSensitive) const {
	for (uint32_t i = 0; i < columnNum_; i++) {
		ColumnInfo &checkColumnInfo = getColumnInfo(i);
		const char *columnName =
			checkColumnInfo.getColumnName(txn, objectManager);
		uint32_t columnNameSize = static_cast<uint32_t>(strlen(columnName));
		bool isExist = 
			eqCaseStringString(txn, name, static_cast<uint32_t>(strlen(name)),
				columnName, columnNameSize, isCaseSensitive);
		if (isExist) {
			columnId = i;
			columnInfo = &checkColumnInfo;
			return;
		}
	}
	columnInfo = NULL;
}

/*!
	@brief Calculate hash value
*/
int64_t ColumnSchema::calcSchemaHashKey(MessageSchema *messageSchema) {
	int64_t hashVal = messageSchema->getColumnCount();
	const util::XArray<ColumnId> &keyColumnIds = messageSchema->getRowKeyColumnIdList();
	util::XArray<ColumnId>::const_iterator itr;
	for (itr = keyColumnIds.begin(); itr != keyColumnIds.end(); itr++) {
		hashVal += *itr;
	}
	hashVal = hashVal << 32;
	for (uint32_t i = 0; i < messageSchema->getColumnCount(); i++) {
		int32_t columnHashVal = 0;
		columnHashVal += messageSchema->getColumnType(i);
		const util::String &columnName = messageSchema->getColumnName(i);
		for (uint32_t j = 0; j < columnName.size(); j++) {
			columnHashVal += static_cast<int32_t>(columnName[j]);
		}
		columnHashVal = columnHashVal << i;  

		hashVal += columnHashVal;
	}
	return hashVal;
}

/*!
	@brief Check if Column layout is same
*/
bool ColumnSchema::schemaCheck(TransactionContext &txn,
	ObjectManager &objectManager, MessageSchema *messageSchema) {
	bool isSameSchema = true;
	util::XArray<ColumnId> columnMap(txn.getDefaultAllocator());

	uint32_t columnNum = messageSchema->getColumnCount();

	if (columnNum != getColumnNum()) {
		isSameSchema = false;
	}
	if (isSameSchema) {
		uint32_t firstColumnNum, newFirstColumnNum;
		uint32_t firstVarColumnNum, newFirstVarColumnNum;
		uint32_t firstRowFixedColumnSize, newFirstRowFixedColumnSize;
		messageSchema->getFirstSchema(newFirstColumnNum, newFirstVarColumnNum, newFirstRowFixedColumnSize);
		getFirstSchema(firstColumnNum, firstVarColumnNum, firstRowFixedColumnSize);
		if (newFirstColumnNum != firstColumnNum || 
			firstVarColumnNum != newFirstVarColumnNum || 
			firstRowFixedColumnSize != newFirstRowFixedColumnSize) {
			isSameSchema = false;
		}
	}
	if (isSameSchema) {
		util::XArray<ColumnId> keyColumnIdList(txn.getDefaultAllocator());
		getKeyColumnIdList(keyColumnIdList);
		const util::XArray<ColumnId> &schemaKeyColumnIdList = messageSchema->getRowKeyColumnIdList();
		if (keyColumnIdList.size() != schemaKeyColumnIdList.size()) {
			isSameSchema = false;
		}
		if (isSameSchema) {
			for (uint16_t i = 0; i < columnNum; i++) {
				columnMap.push_back(i);
			}
			for (size_t i = 0; i < keyColumnIdList.size(); i++) {
				if (keyColumnIdList[i] != schemaKeyColumnIdList[i]) {
					isSameSchema = false;
					break;
				}
			}
		}
	}

	if (isSameSchema) {
		for (uint32_t i = 0; i < columnNum; i++) {
			const util::String &newColumnName = messageSchema->getColumnName(i);
			ColumnType columnType = messageSchema->getColumnType(i);
			bool isArray = messageSchema->getIsArray(i);
			bool isNotNull = messageSchema->getIsNotNull(i);

			ColumnInfo &columnInfo = getColumnInfo(columnMap[i]);
			const char *stringObject =
				columnInfo.getColumnName(txn, objectManager);
			const uint8_t *columnName =
				reinterpret_cast<const uint8_t *>(stringObject);
			uint32_t columnNameSize =
				static_cast<uint32_t>(strlen(stringObject));
			if (eqStringString(
					txn, columnName, columnNameSize,
					reinterpret_cast<const uint8_t *>(newColumnName.c_str()),
					static_cast<uint32_t>(
						newColumnName.length()))) {  

				if (columnInfo.getSimpleColumnType() != columnType ||
					columnInfo.isArray() != isArray ||
					columnInfo.isNotNull() != isNotNull) {
					isSameSchema = false;
				}
			}
			else {
				isSameSchema = false;
			}
			if (!isSameSchema) {
				break;
			}
		}
	}
	return isSameSchema;
}

/*!
	@brief Allocate IndexSchema Object
*/
void IndexSchema::initialize(
	TransactionContext &txn, uint16_t reserveNum, uint16_t indexNum, uint32_t columnNum, 
	bool onMemory) {
	uint16_t bitsSize = RowNullBits::calcBitsSize(columnNum);
	if (onMemory) {
		void *binary = txn.getDefaultAllocator().allocate(IndexSchema::getAllocateSize(reserveNum, bitsSize));
		setBaseAddr(static_cast<uint8_t *>(binary));
	} else {
		BaseObject::allocate<uint8_t>(IndexSchema::getAllocateSize(reserveNum, bitsSize),
			allocateStrategy_, getBaseOId(), OBJECT_TYPE_COLUMNINFO);
	}
	memset(getBaseAddr(), 0, IndexSchema::getAllocateSize(reserveNum, bitsSize));
	setNullbitsSize(bitsSize);

	setNum(indexNum);
	setReserveNum(reserveNum);
}

/*!
	@brief Appends Index Info to Index Schema
*/
bool IndexSchema::createIndexInfo(
	TransactionContext &txn, const IndexInfo &indexInfo) {
	setDirty();

	bool isDuplicate = false;
	if (isFull()) {
		expand(txn);
		isDuplicate = true;
	}
	setIndexInfo(txn, getIndexNum(), indexInfo);
	increment();

	return isDuplicate;
}

/*!
	@brief Deletes Index Info from Index Schema
*/
void IndexSchema::dropIndexInfo(
	TransactionContext &txn, ColumnId columnId, MapType mapType) {
	setDirty();

	bool isFound = false;
	IndexData indexData;
	AllocateStrategy allocateStrategy;
	for (uint16_t i = 0; i < getIndexNum(); i++) {
		getIndexData(txn, i, UNDEF_CONTAINER_POS, indexData);
		if (indexData.columnId_ == columnId && indexData.mapType_ == mapType) {
			isFound = true;
			OId optionOId = getOptionOId(getElemHead() + (getIndexDataSize() * i));
			getObjectManager()->free(txn.getPartitionId(), optionOId);
			for (uint16_t j = i; j < getIndexNum(); j++) {
				if (j + 1 < getIndexNum()) {
					uint8_t *src = getElemHead() + (getIndexDataSize() * (j + 1));
					uint8_t *dest = getElemHead() + (getIndexDataSize() * (j));
					memcpy(dest, src, getIndexDataSize());
				}
			}
			break;
		}
	}
	if (isFound) {
		decrement();
	}
}

/*!
	@brief Creates Index Object
*/
IndexData IndexSchema::createIndexData(TransactionContext &txn, ColumnId columnId,
	MapType mapType, ColumnType columnType, BaseContainer *container,
	uint64_t containerPos, bool isUnique) {
	setDirty();

	OId oId;
	switch (mapType) {
	case MAP_TYPE_BTREE: {
		BtreeMap map(txn, *getObjectManager(),
			container->getMapAllcateStrategy(), container);
		map.initialize(txn, columnType, isUnique, BtreeMap::TYPE_SINGLE_KEY);
		oId = map.getBaseOId();
	} break;
	case MAP_TYPE_HASH: {
		HashMap map(txn, *getObjectManager(),
			container->getMapAllcateStrategy(), container);
		map.initialize(txn, columnType, columnId,
			container->getMetaAllcateStrategy(), isUnique);
		oId = map.getBaseOId();
	} break;
	case MAP_TYPE_SPATIAL: {
		RtreeMap map(txn, *getObjectManager(),
			container->getMapAllcateStrategy(), container);
		map.initialize(txn, columnType, columnId,
			container->getMetaAllcateStrategy(), isUnique);
		oId = map.getBaseOId();
	} break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
	}
	IndexData indexData;
	uint16_t nth = getNth(columnId, mapType);
	getIndexData(txn, nth, UNDEF_CONTAINER_POS, indexData);
	indexData.oIds_.mainOId_ = oId;
	setIndexData(txn, nth, containerPos, indexData);

	return indexData;
}

void IndexSchema::createDummyIndexData(TransactionContext &txn,
	ColumnId columnId, MapType mapType, uint64_t containerPos) {
	setDirty();

	OId oId = UNDEF_OID;
	IndexData indexData;
	uint16_t nth = getNth(columnId, mapType);
	getIndexData(txn, nth, UNDEF_CONTAINER_POS, indexData);
	indexData.oIds_.mainOId_ = oId;
	uint8_t *indexDataPos =	getElemHead() + (getIndexDataSize() * nth);
	setMapOIds(txn, indexDataPos, containerPos, indexData.oIds_);
}

/*!
	@brief Free Index Object
*/
void IndexSchema::dropIndexData(TransactionContext &txn, ColumnId columnId,
	MapType mapType, BaseContainer *container, uint64_t containerPos,
	bool isMapFinalize) {
	setDirty();
	if (isMapFinalize) {
		bool withUncommitted = true; 
		IndexData indexData;
		if (getIndexData(txn, columnId, mapType, containerPos, withUncommitted,
			indexData)) {
			StackAllocAutoPtr<BaseIndex> mainMap(txn.getDefaultAllocator(),
				getIndex(txn, indexData, false, container));
			container->getDataStore()->finalizeMap
				(txn, container->getMapAllcateStrategy(), mainMap.get());
			StackAllocAutoPtr<BaseIndex> nullMap(txn.getDefaultAllocator(),
				getIndex(txn, indexData, true, container));
			if (nullMap.get() != NULL) {
				container->getDataStore()->finalizeMap
					(txn, container->getMapAllcateStrategy(), nullMap.get());
			}
		}
	}
	uint16_t nth = getNth(columnId, mapType);
	uint8_t *indexDataPos = getElemHead() + (getIndexDataSize() * nth);
	removeMapOIds(txn, indexDataPos, containerPos);
}

/*!
	@brief Updates OId of Index Object
*/
void IndexSchema::updateIndexData(
	TransactionContext &txn, const IndexData &updateIndexData, uint64_t containerPos) {
	setDirty();
	uint16_t nth = getNth(updateIndexData.columnId_, updateIndexData.mapType_);
	uint8_t *indexDataPos = getElemHead() + (getIndexDataSize() * nth);
	updateMapOIds(txn, indexDataPos, containerPos, updateIndexData.oIds_);
	if (updateIndexData.status_!= DDL_READY) {
		BaseObject option(txn.getPartitionId(), *getObjectManager(),
			getOptionOId(indexDataPos));
		option.setDirty();
		setRowId(option.getBaseAddr(), updateIndexData.cursor_);
	}
}

void IndexSchema::commit(TransactionContext &txn, ColumnId columnId, MapType mapType) {
	setDirty();
	uint16_t nth = getNth(columnId, mapType);
	uint8_t *indexDataPos = getElemHead() + (getIndexDataSize() * nth);
	if (nth == UNDEF_INDEX_POS || getStatus(indexDataPos) == DDL_READY) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "nth=" << nth
			<< "columnId=" << columnId << ", mapType=" << (int32_t)mapType);
	}
	setStatus(indexDataPos, DDL_READY);
}

/*!
	@brief Check if Index is defined
*/
bool IndexSchema::hasIndex(
	TransactionContext &txn, ColumnId columnId, MapType mapType) const {
	IndexData indexData;
	bool withUncommitted = false;
	return getIndexData(txn, columnId, mapType, UNDEF_CONTAINER_POS, 
		withUncommitted, indexData);
}

/*!
	@brief Check if Index is defined
*/
bool IndexSchema::hasIndex(IndexTypes indexType, MapType mapType) {
	return (indexType & (1 << mapType)) != 0;
}

/*!
	@brief Get list of Index type defined on Column
*/
IndexTypes IndexSchema::getIndexTypes(
	TransactionContext &txn, ColumnId columnId) const {
	IndexTypes indexType = 0;
	IndexData indexData;
	bool withUncommitted = false;
	if (getIndexData(txn, columnId, MAP_TYPE_BTREE, UNDEF_CONTAINER_POS,
		withUncommitted, indexData)) {
		indexType |= (1 << MAP_TYPE_BTREE);
	}
	if (getIndexData(txn, columnId, MAP_TYPE_HASH, UNDEF_CONTAINER_POS,
		withUncommitted, indexData)) {
		indexType |= (1 << MAP_TYPE_HASH);
	}
	if (getIndexData(txn, columnId, MAP_TYPE_SPATIAL, UNDEF_CONTAINER_POS, 
		withUncommitted, indexData)) {
		indexType |= (1 << MAP_TYPE_SPATIAL);
	}
	return indexType;
}

/*!
	@brief Get list of Index Data defined on Columns
*/
void IndexSchema::getIndexList(TransactionContext &txn, uint64_t containerPos,
	bool withUncommitted, util::XArray<IndexData> &list) const {
	for (uint16_t i = 0; i < getIndexNum(); i++) {
		IndexData indexData;
		getIndexData(txn, i, containerPos, indexData);
		if (withUncommitted || indexData.status_ == DDL_READY) {
			list.push_back(indexData);
		}
	}
}

/*!
	@brief Get Index Data defined on Column
*/
bool IndexSchema::getIndexData(TransactionContext &txn, ColumnId columnId,
	MapType mapType, uint64_t containerPos, bool withUncommitted, 
	IndexData &indexData) const {
	bool isFound = false;
	for (uint16_t i = 0; i < getIndexNum(); i++) {
		uint8_t *indexDataPos = getElemHead() + (getIndexDataSize() * i);
		DDLStatus status = getStatus(indexDataPos);
		if (getColumnId(indexDataPos) == columnId &&
			getMapType(indexDataPos) == mapType &&
			(withUncommitted || status == DDL_READY)) {
			indexData.oIds_ = getMapOIds(txn, indexDataPos, containerPos);
			indexData.columnId_ = getColumnId(indexDataPos);
			indexData.mapType_ = getMapType(indexDataPos);
			indexData.status_ = getStatus(indexDataPos);
			if (status != DDL_READY) {
				BaseObject option(txn.getPartitionId(), *getObjectManager(),
					getOptionOId(indexDataPos));
				indexData.cursor_ = getRowId(option.getBaseAddr());
			} else {
				indexData.cursor_ = MAX_ROWID;
			}
			isFound = true;
			break;
		}
	}
	return isFound;
}

void IndexSchema::createNullIndexData(TransactionContext &txn, 
	uint64_t containerPos, IndexData &indexData, BaseContainer *container) {
	setDirty();

	bool isUnique = false;
	ColumnType nullType = COLUMN_TYPE_NULL;
	BtreeMap map(txn, *getObjectManager(),
		container->getMapAllcateStrategy(), container);
	map.initialize(txn, nullType, isUnique, BtreeMap::TYPE_SINGLE_KEY);
	indexData.oIds_.nullOId_ = map.getBaseOId();

	uint16_t nth = getNth(indexData.columnId_, indexData.mapType_);
	uint8_t *indexDataPos = getElemHead() + (getIndexDataSize() * nth);
	updateMapOIds(txn, indexDataPos, containerPos, indexData.oIds_);
}

void IndexSchema::getIndexInfoList(TransactionContext &txn, 
	BaseContainer *container, const IndexInfo &indexInfo, 
	bool withUncommitted, util::Vector<IndexInfo> &matchList, 
	util::Vector<IndexInfo> &mismatchList,
	bool isIndexNameCaseSensitive) {
	for (uint16_t i = 0; i < getIndexNum(); i++) {
		IndexData indexData;
		getIndexData(txn, i, UNDEF_CONTAINER_POS, indexData);
		if (!withUncommitted && indexData.status_ != DDL_READY) {
			continue;
		}

		IndexInfo currentIndexInfo(txn.getDefaultAllocator());
		getIndexInfo(txn, i, currentIndexInfo);
		bool isColumnIdsMatch = 
			std::find(indexInfo.columnIds_.begin(), indexInfo.columnIds_.end(), currentIndexInfo.columnIds_[0])
			!= indexInfo.columnIds_.end();

		bool isMapTypeMatch;
		if (indexInfo.mapType == MAP_TYPE_DEFAULT) {
			ColumnInfo& columnInfo = container->getColumnInfo(currentIndexInfo.columnIds_[0]);
			isMapTypeMatch = defaultIndexType[columnInfo.getColumnType()] == currentIndexInfo.mapType;
		} else {
			isMapTypeMatch = indexInfo.mapType == currentIndexInfo.mapType;
		}

		if (indexInfo.anyNameMatches_ == 0 && indexInfo.indexName_.length() != 0) {
			bool isCaseSensitive = false;	
			bool isNameEqWithCaseInsensitive = 
				eqCaseStringString(txn, 
					indexInfo.indexName_.c_str(),
					static_cast<uint32_t>(indexInfo.indexName_.length()),
					currentIndexInfo.indexName_.c_str(),
					static_cast<uint32_t>(currentIndexInfo.indexName_.length()),
					isCaseSensitive);
			if (isNameEqWithCaseInsensitive) {
				bool isCaseSensitiveMismatch = false;
				if (isIndexNameCaseSensitive) {
					isCaseSensitiveMismatch = 
						!eqCaseStringString(txn, 
							indexInfo.indexName_.c_str(),
							static_cast<uint32_t>(indexInfo.indexName_.length()),
							currentIndexInfo.indexName_.c_str(),
							static_cast<uint32_t>(currentIndexInfo.indexName_.length()),
							isIndexNameCaseSensitive);
				}
				if (isCaseSensitiveMismatch ||
					(!indexInfo.columnIds_.empty() && !isColumnIdsMatch) ||
					(indexInfo.anyTypeMatches_ == 0 && !isMapTypeMatch)) {
					mismatchList.push_back(currentIndexInfo);
				} else {
					matchList.push_back(currentIndexInfo);
				}
			} else {
				if (!indexInfo.columnIds_.empty() && isColumnIdsMatch &&
					indexInfo.anyTypeMatches_ == 0 && isMapTypeMatch) {
					mismatchList.push_back(currentIndexInfo);
				}
			}
		} else {
			if ((indexInfo.columnIds_.empty() || isColumnIdsMatch) &&
				(indexInfo.anyTypeMatches_ != 0 || isMapTypeMatch)) {
				matchList.push_back(currentIndexInfo);
			}
		}
	}
}

/*!
	@brief Free all Index Objects
*/
void IndexSchema::dropAll(TransactionContext &txn, BaseContainer *container,
	uint64_t containerPos, bool isMapFinalize) {
	setDirty();
	{
		util::XArray<IndexData> list(txn.getDefaultAllocator());
		bool withUncommitted = true;
		getIndexList(txn, containerPos, withUncommitted, list);
		for (size_t i = 0; i < list.size(); i++) {
			dropIndexData(txn, list[i].columnId_, list[i].mapType_, container,
				containerPos, isMapFinalize);
		}
	}
}

/*!
	@brief Free Objects related to IndexSchema
*/
void IndexSchema::finalize(TransactionContext &txn) {
	setDirty();
	{
		util::XArray<IndexData> list(txn.getDefaultAllocator());
		bool withUncommitted = true;
		getIndexList(txn, UNDEF_CONTAINER_POS, withUncommitted, list);
		for (size_t i = 0; i < list.size(); i++) {
			dropIndexInfo(txn, list[i].columnId_, list[i].mapType_);
		}
		BaseObject::finalize();
	}
}

/*!
	@brief Get Index Object
*/
BaseIndex *IndexSchema::getIndex(TransactionContext &txn, const IndexData &indexData,
	bool forNull, BaseContainer *container) const {
	const AllocateStrategy strategy = container->getMapAllcateStrategy();

	OId mapOId;
	MapType mapType;
	if (forNull) {
		mapOId = indexData.oIds_.nullOId_;
		mapType = MAP_TYPE_BTREE;
	} else {
		mapOId = indexData.oIds_.mainOId_;
		mapType = indexData.mapType_;
	}
	if (mapOId == UNDEF_OID) {
		return NULL;
	}
	BaseIndex *map = DataStore::getIndex(txn, *getObjectManager(), mapType, 
		mapOId, strategy, container);
	return map;
}


void IndexSchema::expand(TransactionContext &txn) {
	setDirty();
	uint16_t orgIndexNum = getIndexNum();
	uint16_t newReserveIndexNum = getReserveNum() * 2;
	OId duplicateOId;

	uint8_t *toIndexSchema = getObjectManager()->allocate<uint8_t>(
		txn.getPartitionId(), getAllocateSize(newReserveIndexNum, getNullbitsSize()),
		allocateStrategy_, duplicateOId, OBJECT_TYPE_COLUMNINFO);
	memset(toIndexSchema, 0, getAllocateSize(newReserveIndexNum, getNullbitsSize()));
	memcpy(toIndexSchema, getBaseAddr(), getAllocateSize(orgIndexNum, getNullbitsSize()));

	BaseObject::finalize();

	setBaseOId(duplicateOId);
	setBaseAddr(toIndexSchema);
	setNum(orgIndexNum);
	setReserveNum(newReserveIndexNum);
}

bool IndexSchema::expandNullStats(TransactionContext &txn, uint32_t oldColumnNum, uint32_t newColumnNum) {
	uint16_t oldNullBitsSize = RowNullBits::calcBitsSize(oldColumnNum);
	uint16_t newNullBitsSize = RowNullBits::calcBitsSize(newColumnNum);
	if (oldNullBitsSize == newNullBitsSize) {
		return false;
	}

	setDirty();
	uint16_t reserveIndexNum = getReserveNum();
	OId duplicateOId;

	uint8_t *toIndexSchema = getObjectManager()->allocate<uint8_t>(
		txn.getPartitionId(), getAllocateSize(reserveIndexNum, newNullBitsSize),
		allocateStrategy_, duplicateOId, OBJECT_TYPE_COLUMNINFO);
	memset(toIndexSchema, 0, getAllocateSize(reserveIndexNum, newNullBitsSize));
	memcpy(toIndexSchema + NULL_STAT_OFFSET, getBaseAddr() + NULL_STAT_OFFSET, oldNullBitsSize);
	memcpy(toIndexSchema + NULL_STAT_OFFSET + newNullBitsSize, 
		getBaseAddr() + NULL_STAT_OFFSET + oldNullBitsSize, 
		INDEX_HEADER_SIZE + getIndexDataSize() * reserveIndexNum);

	BaseObject::finalize();

	setBaseOId(duplicateOId);
	setBaseAddr(toIndexSchema);
	setNullbitsSize(newNullBitsSize);

	return true;
}

void CompressionSchema::addHiCompression(uint16_t pos, ColumnId columnId,
	double threshhold, double rate, double span, bool threshholdRelative) {
	HiCompressionData *hiCompressionDataList = getHiCompressionDataList();
	hiCompressionDataList[pos].initialize(
		columnId, threshhold, rate, span, threshholdRelative);
	hiCompressionColumnNum_++;
}

bool CompressionSchema::isHiCompression(ColumnId columnId) const {
	bool isFound = false;
	HiCompressionData *hiCompressionDataList = getHiCompressionDataList();
	for (uint16_t i = 0; i < hiCompressionColumnNum_; i++) {
		if (hiCompressionDataList[i].getColumnId() == columnId) {
			isFound = true;
			break;
		}
	}
	return isFound;
}

void CompressionSchema::getHiCompressionColumnList(
	util::XArray<ColumnId> &list) const {
	HiCompressionData *hiCompressionDataList = getHiCompressionDataList();
	for (uint16_t i = 0; i < hiCompressionColumnNum_; i++) {
		list.push_back(hiCompressionDataList[i].getColumnId());
	}
}

void CompressionSchema::getHiCompressionProperty(ColumnId columnId,
	double &threshhold, double &rate, double &span, bool &threshholdRelative,
	uint16_t &compressionPos) const {
	bool isFound = false;
	uint16_t i = 0;
	HiCompressionData *hiCompressionDataList = getHiCompressionDataList();
	for (; i < hiCompressionColumnNum_; i++) {
		if (hiCompressionDataList[i].getColumnId() == columnId) {
			isFound = true;
			break;
		}
	}
	if (!isFound) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_HICOMPRESSION_INVALID, "");
	}
	compressionPos = i;
	hiCompressionDataList[compressionPos].getProperty(
		threshhold, rate, span, threshholdRelative);
}

bool CompressionSchema::schemaCheck(
	TransactionContext &, MessageTimeSeriesSchema *messageSchema) {
	uint32_t columnNum = messageSchema->getColumnCount();

	if (messageSchema->getCompressionType() != getCompressionType()) {
		return false;
	}

	if (getCompressionType() != NO_COMPRESSION) {
		if (getDurationInfo().timeDuration_ !=
			messageSchema->getDurationInfo().timeDuration_) {
			return false;
		}
		if (getDurationInfo().timeUnit_ !=
			messageSchema->getDurationInfo().timeUnit_) {
			return false;
		}

		if (messageSchema->getCompressionInfoNum() != 0) {
			for (uint32_t i = 0; i < columnNum; i++) {
				MessageCompressionInfo::CompressionType type =
					messageSchema->getCompressionInfo(i).getType();
				bool threshholdRelative = messageSchema->getCompressionInfo(i)
											  .getThreshholdRelative();
				double threshhold =
					messageSchema->getCompressionInfo(i).getThreshhold();
				double rate = messageSchema->getCompressionInfo(i).getRate();
				double span = messageSchema->getCompressionInfo(i).getSpan();

				if (type != MessageCompressionInfo::NONE) {
					if (isHiCompression(i)) {
						double oldThreshhold, oldRate, oldSpan;
						bool oldThreshholdRelative;
						uint16_t compressionPos;
						getHiCompressionProperty(i, oldThreshhold, oldRate,
							oldSpan, oldThreshholdRelative, compressionPos);

						if (oldThreshholdRelative != threshholdRelative) {
							return false;
						}
						if (oldThreshhold != threshhold) {
							return false;
						}
						if (oldRate != rate) {
							return false;
						}
						if (oldSpan != span) {
							return false;
						}
					}
					else {
						return false;
					}
				}
			}
		}
	}
	return true;
}

int64_t CompressionSchema::calcHash(MessageTimeSeriesSchema *messageSchema) {
	int64_t hashVal = 0;
	uint32_t columnNum = messageSchema->getColumnCount();

	hashVal += messageSchema->getCompressionType();
	if (messageSchema->getCompressionType() != NO_COMPRESSION) {
		hashVal += messageSchema->getDurationInfo().getHashVal();

		if (messageSchema->getCompressionInfoNum() != 0) {
			for (uint32_t i = 0; i < columnNum; i++) {
				MessageCompressionInfo::CompressionType type =
					messageSchema->getCompressionInfo(i).getType();
				bool threshholdRelative = messageSchema->getCompressionInfo(i)
											  .getThreshholdRelative();
				double threshhold =
					messageSchema->getCompressionInfo(i).getThreshhold();
				double rate = messageSchema->getCompressionInfo(i).getRate();
				double span = messageSchema->getCompressionInfo(i).getSpan();

				if (type != MessageCompressionInfo::NONE) {
					hashVal += static_cast<int64_t>(i);
					hashVal += static_cast<int64_t>(threshholdRelative);
					hashVal += static_cast<int64_t>(threshhold * 100);
					hashVal += static_cast<int64_t>(rate * 100);
					hashVal += static_cast<int64_t>(span * 100);
				}
			}
		}
	}
	return hashVal;
}

/*!
	@brief Allocate ShareValueList Object
*/
void ShareValueList::initialize(TransactionContext &txn, uint32_t allocateSize,
	const AllocateStrategy &allocateStrategy, bool onMemory) {
	if (onMemory) {
		void *binary = txn.getDefaultAllocator().allocate(allocateSize);
		setBaseAddr(static_cast<uint8_t *>(binary));
	} else {
		BaseObject::allocate<uint8_t>(
			allocateSize, allocateStrategy, getBaseOId(), OBJECT_TYPE_CONTAINER_ID);
	}
	memset(getBaseAddr(), 0, allocateSize);

	*getHashValPtr() = 0;
	*getRefCounterPtr() = 1;
	*getNumPtr() = 0;
}

void ShareValueList::set(int64_t hashVal, util::XArray<ElemData> &list) {
	util::XArray<ElemData>::const_iterator itr;

	*getHashValPtr() = hashVal;
	*getNumPtr() = static_cast<int32_t>(list.size());

	uint32_t valueOffset = 0;

	uint32_t i = 0;
	for (i = 0, itr = list.begin(); itr != list.end(); i++, itr++) {
		uint8_t *addr = put(i, itr->type_, valueOffset);
		memcpy(addr, itr->binary_, itr->size_);
		valueOffset += itr->size_;
	}
}

/*!
	@brief Free Objects related to ShareValueList
*/
void ShareValueList::finalize() {
	BaseObject::finalize();
}

uint8_t *ShareValueList::put(
	int32_t pos, CONTAINER_META_TYPE type, uint32_t valueOffset) {
	uint8_t *typeAddr = getElemHeader(pos);
	uint8_t *offsetAddr = typeAddr + sizeof(CONTAINER_META_TYPE);

	*reinterpret_cast<CONTAINER_META_TYPE *>(typeAddr) = type;
	*reinterpret_cast<uint32_t *>(offsetAddr) =
		getHeaderSize(getNum()) + valueOffset;

	uint8_t *valueAddr =
		getBaseAddr() + *reinterpret_cast<uint32_t *>(offsetAddr);

	return valueAddr;
}

template ColumnSchema *ShareValueList::get(CONTAINER_META_TYPE type) const;
template CompressionSchema *ShareValueList::get(CONTAINER_META_TYPE type) const;
template BaseContainer::ExpirationInfo *ShareValueList::get(
	CONTAINER_META_TYPE type) const;
template char *ShareValueList::get(CONTAINER_META_TYPE type) const;
template TriggerList *ShareValueList::get(CONTAINER_META_TYPE type) const;
template ContainerAttribute *ShareValueList::get(
	CONTAINER_META_TYPE type) const;
template BaseContainer::ContainerExpirationInfo *ShareValueList::get(
	CONTAINER_META_TYPE type) const;

template <typename K>
K *ShareValueList::get(CONTAINER_META_TYPE type) const {
	for (int32_t i = 0; i < getNum(); ++i) {
		uint8_t *typeAddr = getElemHeader(i);
		CONTAINER_META_TYPE currentType =
			*reinterpret_cast<CONTAINER_META_TYPE *>(typeAddr);
		if (currentType == type) {
			uint8_t *offsetAddr = typeAddr + sizeof(CONTAINER_META_TYPE);
			uint8_t *valueAddr =
				getBaseAddr() + *reinterpret_cast<uint32_t *>(offsetAddr);
			return reinterpret_cast<K *>(valueAddr);
		}
	}
	return NULL;
}

template <typename H, typename V>
const int32_t LinkArray<H, V>::CHAIN_BIT =
	LIST_EXP_SIZE - ((int32_t)(log(double(sizeof(V))) / log(double(2))) + 1);
template <typename H, typename V>
const int32_t LinkArray<H, V>::SUB_ELEM_BIT_MASK =
	(1 << (LIST_EXP_SIZE -
		   ((int32_t)(log(double(sizeof(V))) / log(double(2))) + 1))) -
	1;

template const uint64_t LinkArray<TimeSeries::BaseSubTimeSeriesData,
	TimeSeries::SubTimeSeriesImage>::MAX_LOCAL_ELEMENT_NUM;
template <typename H, typename V>
const uint64_t LinkArray<H, V>::MAX_LOCAL_ELEMENT_NUM =
	(1 << (LIST_EXP_SIZE -
		   ((int32_t)(log(double(sizeof(V))) / log(double(2))) + 1)));

template <>
uint64_t LinkArray<void, MapOIds>::getHeaderSize() {
	return 0;
}

template uint64_t LinkArray<TimeSeries::BaseSubTimeSeriesData,
	TimeSeries::SubTimeSeriesImage>::getHeaderSize();
template <typename H, typename V>
uint64_t LinkArray<H, V>::getHeaderSize() {
	return sizeof(H);
}

template const MapOIds *LinkArray<void, MapOIds>::get(
	TransactionContext &txn, uint64_t pos);
template const TimeSeries::SubTimeSeriesImage *
LinkArray<TimeSeries::BaseSubTimeSeriesData,
	TimeSeries::SubTimeSeriesImage>::get(TransactionContext &txn, uint64_t pos);
template <typename H, typename V>
const V *LinkArray<H, V>::get(TransactionContext &txn, uint64_t pos) {
	if (pos >= getNum()) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_OUT_OF_RANGE,
			"Invalid Access pos :" << pos << ", arraySize :" << getNum());
	}
	uint64_t chainNo = (pos >> CHAIN_BIT);
	uint64_t startPos = pos & SUB_ELEM_BIT_MASK;
	if (chainNo == 0) {
		const V *chainHead = getElemHead();
		const V *elem = chainHead + startPos;
		return elem;
	}
	else {
		BaseObject chainObject(txn.getPartitionId(), *getObjectManager());
		getChainList(txn, chainNo, chainObject);
		const V *chainHead = chainObject.getCursor<V>();
		const V *elem = chainHead + startPos;
		return elem;
	}
}

template void LinkArray<void, MapOIds>::insert(TransactionContext &txn,
	uint64_t pos, const MapOIds *value, const AllocateStrategy &allocateStrategy);
template void LinkArray<TimeSeries::BaseSubTimeSeriesData,
	TimeSeries::SubTimeSeriesImage>::insert(TransactionContext &txn,
	uint64_t pos, const TimeSeries::SubTimeSeriesImage *value,
	const AllocateStrategy &allocateStrategy);
template <typename H, typename V>
void LinkArray<H, V>::insert(TransactionContext &txn, uint64_t pos,
	const V *value, const AllocateStrategy &allocateStrategy) {
	setDirty();
	if (isFull()) {
		expand(txn, allocateStrategy);
	}

	uint64_t chainNo = (pos >> CHAIN_BIT);
	uint64_t startPos = pos & SUB_ELEM_BIT_MASK;
	uint64_t elemNum = getChainElemNum(chainNo);
	UpdateBaseObject chain(txn.getPartitionId(), *getObjectManager());
	getChainList(txn, chainNo, chain);
	V insertImage = *value;
	while (1) {
		uint64_t maxPos;
		if (elemNum < MAX_LOCAL_ELEMENT_NUM) {
			maxPos = elemNum;
			if (elemNum > 0) {
				for (uint64_t i = maxPos; i > startPos; i--) {
					V *fromValue = chain.getCursor<V>() + i - 1;
					V *toValue = chain.getCursor<V>() + i;
					*toValue = *fromValue;
				}
			}
			V *insertValue = chain.getCursor<V>() + startPos;
			*insertValue = insertImage;
			break;
		}
		else {
			V tailImage = *(chain.getCursor<V>() + elemNum - 1);

			maxPos = elemNum - 1;
			if (elemNum > 0) {
				for (uint64_t i = maxPos; i > startPos; i--) {
					V *fromValue = chain.getCursor<V>() + i - 1;
					V *toValue = chain.getCursor<V>() + i;
					*toValue = *fromValue;
				}
			}
			V *insertValue = chain.getCursor<V>() + startPos;
			*insertValue = insertImage;

			insertImage = tailImage;

			chainNo++;
			startPos = 0;
			elemNum = getChainElemNum(chainNo);
			chain.load(getNextOId(chain.getCursor<V>()));
		}
	}
	increment();
}

template void LinkArray<void, MapOIds>::update(
	TransactionContext &txn, uint64_t pos, const MapOIds *value);
template void LinkArray<TimeSeries::BaseSubTimeSeriesData,
	TimeSeries::SubTimeSeriesImage>::update(TransactionContext &txn,
	uint64_t pos, const TimeSeries::SubTimeSeriesImage *value);
template <typename H, typename V>
void LinkArray<H, V>::update(
	TransactionContext &txn, uint64_t pos, const V *value) {
	setDirty();

	if (pos >= getNum()) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_OUT_OF_RANGE,
			"Invalid Access pos :" << pos << ", arraySize :" << getNum());
	}
	uint64_t chainNo = (pos >> CHAIN_BIT);
	uint64_t startPos = pos & SUB_ELEM_BIT_MASK;
	if (chainNo == 0) {
		V *chainHead = getElemHead();
		V *elem = chainHead + startPos;
		*elem = *value;
	}
	else {
		UpdateBaseObject chain(txn.getPartitionId(), *getObjectManager());
		getChainList(txn, chainNo, chain);
		V *elem = chain.getCursor<V>() + startPos;
		*elem = *value;
	}
}

template void LinkArray<void, MapOIds>::remove(
	TransactionContext &txn, uint64_t pos);
template void LinkArray<TimeSeries::BaseSubTimeSeriesData,
	TimeSeries::SubTimeSeriesImage>::remove(TransactionContext &txn,
	uint64_t pos);
template <typename H, typename V>
void LinkArray<H, V>::remove(TransactionContext &txn, uint64_t pos) {
	setDirty();
	if (pos >= getNum()) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_OUT_OF_RANGE,
			"Invalid Access pos :" << pos << ", arraySize :" << getNum());
	}

	uint64_t chainNo = (pos >> CHAIN_BIT);
	uint64_t startPos = pos & SUB_ELEM_BIT_MASK;
	uint64_t elemNum = getChainElemNum(chainNo);
	UpdateBaseObject chain(txn.getPartitionId(), *getObjectManager());
	getChainList(txn, chainNo, chain);
	while (1) {
		for (uint64_t i = startPos; i < elemNum - 1; i++) {
			V *fromValue = chain.getCursor<V>() + i + 1;
			V *toValue = chain.getCursor<V>() + i;
			*toValue = *fromValue;
		}
		if (getChainNum() == chainNo + 1 ||
			getNextOId(chain.getCursor<V>()) == UNDEF_OID) {
			break;
		}
		else {
			V *tailValue = chain.getCursor<V>() + elemNum - 1;

			chainNo++;
			startPos = 0;
			elemNum = getChainElemNum(chainNo);

			UpdateBaseObject nextChain(txn.getPartitionId(),
				*getObjectManager(), getNextOId(chain.getCursor<V>()));
			V *nextHeadValue = nextChain.getCursor<V>();

			*tailValue = *nextHeadValue;
			chain.copyReference(nextChain);
		}
	}
	decrement();
}

template OId LinkArray<void, MapOIds>::expand(
	TransactionContext &txn, const AllocateStrategy &allocateStrategy);
template OId LinkArray<TimeSeries::BaseSubTimeSeriesData,
	TimeSeries::SubTimeSeriesImage>::expand(TransactionContext &txn,
	const AllocateStrategy &allocateStrategy);
template <typename H, typename V>
OId LinkArray<H, V>::expand(
	TransactionContext &txn, const AllocateStrategy &allocateStrategy) {
	setDirty();

	OId retOId = UNDEF_OID;
	if (getReserveNum() < MAX_LOCAL_ELEMENT_NUM) {
		uint64_t newReserveNum = getReserveNum() * 2;
		if (newReserveNum > MAX_LOCAL_ELEMENT_NUM) {
			newReserveNum = MAX_LOCAL_ELEMENT_NUM;
		}

		OId newOId;
		{
			BaseObject newObj(txn.getPartitionId(), *getObjectManager());
			newObj.allocate<uint8_t>(getAllocateSize(newReserveNum),
				allocateStrategy, newOId, OBJECT_TYPE_CONTAINER_ID);
			memcpy(newObj.getBaseAddr(), getBaseAddr(),
				getElementHeaderOffset() + sizeof(V) * getNum());
		}
		BaseObject::finalize();

		load(newOId, OBJECT_FOR_UPDATE);

		setReserveNum(newReserveNum);
		if (newReserveNum == MAX_LOCAL_ELEMENT_NUM) {
			V *chainHead = getElemHead();
			setNextOId(chainHead, UNDEF_OID);
		}
		retOId = getBaseOId();
	}
	else {
		uint64_t lastChainNo = getChainNum() - 1;
		UpdateBaseObject chain(txn.getPartitionId(), *getObjectManager());
		getChainList(txn, lastChainNo, chain);

		OId nextOId;
		BaseObject nextChain(txn.getPartitionId(), *getObjectManager());
		nextChain.allocate<uint8_t>(getAllocateSize(MAX_LOCAL_ELEMENT_NUM),
			allocateStrategy, nextOId, OBJECT_TYPE_CONTAINER_ID);

		setNextOId(chain.getCursor<V>(), nextOId);
		setNextOId(nextChain.getCursor<V>(), UNDEF_OID);
		setReserveNum(getReserveNum() + MAX_LOCAL_ELEMENT_NUM);
	}
	return retOId;
}

template <typename H, typename V>
uint64_t LinkArray<H, V>::getChainNum() const {
	uint64_t num = getNum();
	uint64_t chainNum = (num >> CHAIN_BIT);

	uint64_t bitNum = (1 << CHAIN_BIT) - 1;
	if (num == 0 || (num & bitNum) != 0) {
		chainNum++;
	}
	return chainNum;
}

template <typename H, typename V>
void LinkArray<H, V>::getChainList(
	TransactionContext &txn, uint64_t chainNo, BaseObject &chain) {
	if (chainNo == 0) {
		chain.copyReference(*reinterpret_cast<BaseObject *>(this));
		chain.moveCursor(getElementHeaderOffset());
	}
	else {
		V *head = getElemHead();
		for (uint64_t i = 0; i < chainNo; i++) {
			OId nextOId = getNextOId(head);
			chain.load(nextOId);
			head = reinterpret_cast<V *>(chain.getBaseAddr());
		}
	}
	return;
}

template <typename H, typename V>
void LinkArray<H, V>::getChainList(
	TransactionContext &txn, uint64_t chainNo, UpdateBaseObject &chain) {
	if (chainNo == 0) {
		chain.copyReference(*reinterpret_cast<BaseObject *>(this));
		chain.moveCursor(getElementHeaderOffset());
	}
	else {
		V *head = getElemHead();
		for (uint64_t i = 0; i < chainNo; i++) {
			OId nextOId = getNextOId(head);
			chain.load(nextOId);
			head = reinterpret_cast<V *>(chain.getBaseAddr());
		}
	}
	return;
}

template void LinkArray<void, MapOIds>::dump(TransactionContext &txn);
template void LinkArray<TimeSeries::BaseSubTimeSeriesData,
	TimeSeries::SubTimeSeriesImage>::dump(TransactionContext &txn);
template <typename H, typename V>
void LinkArray<H, V>::dump(TransactionContext &txn) {
	std::cout << "num = " << getNum() << ", reverve = " << getReserveNum()
			  << std::endl;

	uint64_t counter = 0;
	for (uint64_t chainNo = 0; chainNo < getChainNum(); chainNo++) {
		uint64_t elemNum = getChainElemNum(chainNo);
		BaseObject chain(txn.getPartitionId(), *getObjectManager());
		getChainList(txn, chainNo, chain);
		for (uint64_t i = 0; i < elemNum; i++) {
			const V *value = chain.getCursor<V>() + i;
			std::cout << "[" << counter << "]" << *value << std::endl;
			counter++;
		}
	}
}


const char *BibInfoUtil::CONTAINER_TYPE_COLLECTION_STR = "COLLECTION";
const char *BibInfoUtil::CONTAINER_TYPE_TIMESERIES_STR = "TIME_SERIES";

const char *BibInfoUtil::COLUMN_TYPE_BOOL_STR = "BOOL";
const char *BibInfoUtil::COLUMN_TYPE_BYTE_STR = "BYTE";
const char *BibInfoUtil::COLUMN_TYPE_SHORT_STR = "SHORT";
const char *BibInfoUtil::COLUMN_TYPE_INT_STR = "INTEGER";
const char *BibInfoUtil::COLUMN_TYPE_LONG_STR = "LONG";
const char *BibInfoUtil::COLUMN_TYPE_FLOAT_STR = "FLOAT";
const char *BibInfoUtil::COLUMN_TYPE_DOUBLE_STR = "DOUBLE";
const char *BibInfoUtil::COLUMN_TYPE_TIMESTAMP_STR = "TIMESTAMP";
const char *BibInfoUtil::COLUMN_TYPE_STRING_STR = "STRING";
const char *BibInfoUtil::COLUMN_TYPE_GEOMETRY_STR = "GEOMETRY";
const char *BibInfoUtil::COLUMN_TYPE_BLOB_STR = "BLOB";
const char *BibInfoUtil::COLUMN_TYPE_STRING_ARRAY_STR = "STRING[]";
const char *BibInfoUtil::COLUMN_TYPE_BOOL_ARRAY_STR = "BOOL[]";
const char *BibInfoUtil::COLUMN_TYPE_BYTE_ARRAY_STR = "BYTE[]";
const char *BibInfoUtil::COLUMN_TYPE_SHORT_ARRAY_STR = "SHORT[]";
const char *BibInfoUtil::COLUMN_TYPE_INT_ARRAY_STR = "INTEGER[]";
const char *BibInfoUtil::COLUMN_TYPE_LONG_ARRAY_STR = "LONG[]";
const char *BibInfoUtil::COLUMN_TYPE_FLOAT_ARRAY_STR = "FLOAT[]";
const char *BibInfoUtil::COLUMN_TYPE_DOUBLE_ARRAY_STR = "DOUBLE[]";
const char *BibInfoUtil::COLUMN_TYPE_TIMESTAMP_ARRAY_STR = "TIMESTAMP[]";

const char *BibInfoUtil::TIME_UNIT_DAY_STR = "DAY";
const char *BibInfoUtil::TIME_UNIT_HOUR_STR = "HOUR";
const char *BibInfoUtil::TIME_UNIT_MINUTE_STR = "MINUTE";
const char *BibInfoUtil::TIME_UNIT_SECOND_STR = "SECOND";
const char *BibInfoUtil::TIME_UNIT_MILLISECOND_STR = "MILLISECOND";

const char *BibInfoUtil::NO_COMPRESSION_STR = "NO";
const char *BibInfoUtil::SS_COMPRESSION_STR = "SS";
const char *BibInfoUtil::HI_COMPRESSION_STR = "HI";

TimeUnit BibInfoUtil::getTimeUnit(const char *str) {
	TimeUnit unit = TIME_UNIT_DAY;
	if (strcmp(str, TIME_UNIT_DAY_STR) == 0) {
		unit = TIME_UNIT_DAY;
	}
	else if (strcmp(str, TIME_UNIT_HOUR_STR) == 0) {
		unit = TIME_UNIT_HOUR;
	}
	else if (strcmp(str, TIME_UNIT_MINUTE_STR) == 0) {
		unit = TIME_UNIT_MINUTE;
	}
	else if (strcmp(str, TIME_UNIT_SECOND_STR) == 0) {
		unit = TIME_UNIT_SECOND;
	}
	else if (strcmp(str, TIME_UNIT_MILLISECOND_STR) == 0) {
		unit = TIME_UNIT_MILLISECOND;
	}
	else {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
			"Unknown Timeunit : " << str);
	}
	return unit;
}

const char *BibInfoUtil::getTimeUnitStr(TimeUnit unit) {
	const char *unitStr = NULL;
	switch (unit) {
	case TIME_UNIT_DAY:
		unitStr = TIME_UNIT_DAY_STR;
		break;
	case TIME_UNIT_HOUR:
		unitStr = TIME_UNIT_HOUR_STR;
		break;
	case TIME_UNIT_MINUTE:
		unitStr = TIME_UNIT_MINUTE_STR;
		break;
	case TIME_UNIT_SECOND:
		unitStr = TIME_UNIT_SECOND_STR;
		break;
	case TIME_UNIT_MILLISECOND:
		unitStr = TIME_UNIT_MILLISECOND_STR;
		break;
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
			"Unknown Timeunit : " << unit);
		break;
	}
	return unitStr;
}

COMPRESSION_TYPE BibInfoUtil::getCompressionType(const char *str) {
	COMPRESSION_TYPE type = TIME_UNIT_DAY;
	if (strcmp(str, NO_COMPRESSION_STR) == 0) {
		type = NO_COMPRESSION;
	}
	else if (strcmp(str, SS_COMPRESSION_STR) == 0) {
		type = SS_COMPRESSION;
	}
	else if (strcmp(str, HI_COMPRESSION_STR) == 0) {
		type = HI_COMPRESSION;
	}
	else {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
			"Unknown CompressionType : " << str);
	}
	return type;
}

const char *BibInfoUtil::getCompressionTypeStr(COMPRESSION_TYPE type) {
	const char *typeStr = NULL;
	switch (type) {
	case NO_COMPRESSION:
		typeStr = NO_COMPRESSION_STR;
		break;
	case SS_COMPRESSION:
		typeStr = SS_COMPRESSION_STR;
		break;
	case HI_COMPRESSION:
		typeStr = HI_COMPRESSION_STR;
		break;
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
			"Unknown CompressionType : " << type);
		break;
	}
	return typeStr;
}

ColumnType BibInfoUtil::getColumnType(const char *str) {
	ColumnType type = COLUMN_TYPE_ANY;
	if (strcmp(str, COLUMN_TYPE_BOOL_STR) == 0) {
		type = COLUMN_TYPE_BOOL;
	}
	else if (strcmp(str, COLUMN_TYPE_BYTE_STR) == 0) {
		type = COLUMN_TYPE_BYTE;
	}
	else if (strcmp(str, COLUMN_TYPE_SHORT_STR) == 0) {
		type = COLUMN_TYPE_SHORT;
	}
	else if (strcmp(str, COLUMN_TYPE_INT_STR) == 0) {
		type = COLUMN_TYPE_INT;
	}
	else if (strcmp(str, COLUMN_TYPE_LONG_STR) == 0) {
		type = COLUMN_TYPE_LONG;
	}
	else if (strcmp(str, COLUMN_TYPE_FLOAT_STR) == 0) {
		type = COLUMN_TYPE_FLOAT;
	}
	else if (strcmp(str, COLUMN_TYPE_DOUBLE_STR) == 0) {
		type = COLUMN_TYPE_DOUBLE;
	}
	else if (strcmp(str, COLUMN_TYPE_TIMESTAMP_STR) == 0) {
		type = COLUMN_TYPE_TIMESTAMP;
	}
	else if (strcmp(str, COLUMN_TYPE_STRING_STR) == 0) {
		type = COLUMN_TYPE_STRING;
	}
	else if (strcmp(str, COLUMN_TYPE_GEOMETRY_STR) == 0) {
		type = COLUMN_TYPE_GEOMETRY;
	}
	else if (strcmp(str, COLUMN_TYPE_BLOB_STR) == 0) {
		type = COLUMN_TYPE_BLOB;
	}
	else if (strcmp(str, COLUMN_TYPE_STRING_ARRAY_STR) == 0) {
		type = COLUMN_TYPE_STRING_ARRAY;
	}
	else if (strcmp(str, COLUMN_TYPE_BOOL_ARRAY_STR) == 0) {
		type = COLUMN_TYPE_BOOL_ARRAY;
	}
	else if (strcmp(str, COLUMN_TYPE_BYTE_ARRAY_STR) == 0) {
		type = COLUMN_TYPE_BYTE_ARRAY;
	}
	else if (strcmp(str, COLUMN_TYPE_SHORT_ARRAY_STR) == 0) {
		type = COLUMN_TYPE_SHORT_ARRAY;
	}
	else if (strcmp(str, COLUMN_TYPE_INT_ARRAY_STR) == 0) {
		type = COLUMN_TYPE_INT_ARRAY;
	}
	else if (strcmp(str, COLUMN_TYPE_LONG_ARRAY_STR) == 0) {
		type = COLUMN_TYPE_LONG_ARRAY;
	}
	else if (strcmp(str, COLUMN_TYPE_FLOAT_ARRAY_STR) == 0) {
		type = COLUMN_TYPE_FLOAT_ARRAY;
	}
	else if (strcmp(str, COLUMN_TYPE_DOUBLE_ARRAY_STR) == 0) {
		type = COLUMN_TYPE_DOUBLE_ARRAY;
	}
	else if (strcmp(str, COLUMN_TYPE_TIMESTAMP_ARRAY_STR) == 0) {
		type = COLUMN_TYPE_TIMESTAMP_ARRAY;
	}
	else {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
			"Unknown ColumnType : " << str);
	}
	return type;
}

const char *BibInfoUtil::getColumnTypeStr(const ColumnType type) {
	const char *str = NULL;
	switch (type) {
	case COLUMN_TYPE_BOOL:
		str = COLUMN_TYPE_BOOL_STR;
		break;
	case COLUMN_TYPE_BYTE:
		str = COLUMN_TYPE_BYTE_STR;
		break;
	case COLUMN_TYPE_SHORT:
		str = COLUMN_TYPE_SHORT_STR;
		break;
	case COLUMN_TYPE_INT:
		str = COLUMN_TYPE_INT_STR;
		break;
	case COLUMN_TYPE_LONG:
		str = COLUMN_TYPE_LONG_STR;
		break;
	case COLUMN_TYPE_FLOAT:
		str = COLUMN_TYPE_FLOAT_STR;
		break;
	case COLUMN_TYPE_DOUBLE:
		str = COLUMN_TYPE_DOUBLE_STR;
		break;
	case COLUMN_TYPE_TIMESTAMP:
		str = COLUMN_TYPE_TIMESTAMP_STR;
		break;
	case COLUMN_TYPE_STRING:
		str = COLUMN_TYPE_STRING_STR;
		break;
	case COLUMN_TYPE_GEOMETRY:
		str = COLUMN_TYPE_GEOMETRY_STR;
		break;
	case COLUMN_TYPE_BLOB:
		str = COLUMN_TYPE_BLOB_STR;
		break;
	case COLUMN_TYPE_STRING_ARRAY:
		str = COLUMN_TYPE_STRING_ARRAY_STR;
		break;
	case COLUMN_TYPE_BOOL_ARRAY:
		str = COLUMN_TYPE_BOOL_ARRAY_STR;
		break;
	case COLUMN_TYPE_BYTE_ARRAY:
		str = COLUMN_TYPE_BYTE_ARRAY_STR;
		break;
	case COLUMN_TYPE_SHORT_ARRAY:
		str = COLUMN_TYPE_SHORT_ARRAY_STR;
		break;
	case COLUMN_TYPE_INT_ARRAY:
		str = COLUMN_TYPE_INT_ARRAY_STR;
		break;
	case COLUMN_TYPE_LONG_ARRAY:
		str = COLUMN_TYPE_LONG_ARRAY_STR;
		break;
	case COLUMN_TYPE_FLOAT_ARRAY:
		str = COLUMN_TYPE_FLOAT_ARRAY_STR;
		break;
	case COLUMN_TYPE_DOUBLE_ARRAY:
		str = COLUMN_TYPE_DOUBLE_ARRAY_STR;
		break;
	case COLUMN_TYPE_TIMESTAMP_ARRAY:
		str = COLUMN_TYPE_TIMESTAMP_ARRAY_STR;
		break;
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
			"Unknown ColumnType : " << type);
		break;
	}
	return str;
}

ContainerType BibInfoUtil::getContainerType(const char *str) {
	ContainerType type = UNDEF_CONTAINER;
	if (strcmp(str, CONTAINER_TYPE_COLLECTION_STR) == 0) {
		type = COLLECTION_CONTAINER;
	}
	else if (strcmp(str, CONTAINER_TYPE_TIMESERIES_STR) == 0) {
		type = TIME_SERIES_CONTAINER;
	}
	else {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
			"Unknown ContainerType : " << str);
	}
	return type;
}

const char *BibInfoUtil::getContainerTypeStr(ContainerType type) {
	const char *typeStr = NULL;
	switch (type) {
	case COLLECTION_CONTAINER:
		typeStr = CONTAINER_TYPE_COLLECTION_STR;
		break;
	case TIME_SERIES_CONTAINER:
		typeStr = CONTAINER_TYPE_TIMESERIES_STR;
		break;
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
			"Unknown ContainerType : " << type);
		break;
	}
	return typeStr;
}


#include "gs_error_common.h"
#include "json.h"
#include "picojson.h"

const int64_t BibInfo::Option::DEFAULT_FLUSH_THRESHOLD = 1024 * 1024; 
const int64_t BibInfo::Option::DEFAULT_BLOCK_SIZE = 2 * 1024 * 1024 * 1024LL + 1024 * 1024LL; 
const int64_t BibInfo::Option::LIMIT_BLOCK_SIZE = 4 * 1024 * 1024 * 1024LL; 
const int64_t BibInfo::Option::LIMIT_FLUSH_THRESHOLD = LIMIT_BLOCK_SIZE;
BibInfo::Option::Option() : flushThreshold_(DEFAULT_FLUSH_THRESHOLD),
	blockSize_(DEFAULT_BLOCK_SIZE), preReadNum_(PARTIAL_RESULT_SIZE)
{}

BibInfo::Container::Container() : databaseId_(UNDEF_DBID),containerId_(UNDEF_CONTAINERID),
	rowIndexOId_(UNDEF_OID),mvccIndexOId_(UNDEF_OID),initSchemaStatus_(0),
	schemaVersion_(UNDEF_SCHEMAVERSIONID),database_(GS_PUBLIC),
	partitionNo_(UNDEF_PARTITIONID),rowKeyAssigned_(false) {}
BibInfo::Container::Column::Column() : notNull_(false) {}
BibInfo::Container::TimeSeriesProperties::TimeSeriesProperties() : isExist_(false),
	rowExpirationElapsedTime_(-1),
	expirationDivisionCount_(BaseContainer::EXPIRE_DIVIDE_DEFAULT_NUM),
	compressionMethod_("NO"), compressionWindowSize_(-1) {}
BibInfo::Container::CompressionInfo::CompressionInfo() : rate_(0),span_(0),width_(0) {}

template<typename T>
bool BibInfo::setKey(const picojson::value &json, const char *key, T &output, bool isOption) {
//	const picojson::value &existKey = json.get(key);
	const T *jsonValue = 
		JsonUtils::find< T >(json, key);
	if (jsonValue == NULL && !json.get(key).is<picojson::null>()) {
		GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_UNEXPECTED_TYPE,
			"Json key type unmatched , key = " << key);
	}
	if (jsonValue == NULL && !isOption) {
		GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_UNEXPECTED_TYPE,
			"Json key not found, key = " << key);
	}
	bool exist = false;
	if (jsonValue != NULL) {
		output = *jsonValue;
		exist = true;
	}
	return exist;
}

template<typename T>
bool BibInfo::setIntegerKey(const picojson::value &json, const char *key, T &output, bool isOption) {
	UTIL_STATIC_ASSERT(std::numeric_limits<T>::is_integer);
//	const picojson::value &existKey = json.get(key);
	const double *jsonValue = 
		JsonUtils::find< double >(json, key);
	if (jsonValue == NULL && !json.get(key).is<picojson::null>()) {
		GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_UNEXPECTED_TYPE,
			"Json key type unmatched , key = " << key);
	}
	if (jsonValue == NULL && !isOption) {
		GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_UNEXPECTED_TYPE,
			"Json key not found, key = " << key);
	}
	bool exist = false;
	if (jsonValue != NULL) {
		output = JsonUtils::asInt<T>(*jsonValue);
		exist = true;
	}
	return exist;
}

template<typename T>
bool BibInfo::setStringIntegerKey(const picojson::value &json, const char *key, T &output, bool isOption) {
	UTIL_STATIC_ASSERT(std::numeric_limits<T>::is_integer);
//	const picojson::value &existKey = json.get(key);
	const std::string *jsonValue = 
		JsonUtils::find< std::string >(json, key);
	if (jsonValue == NULL && !json.get(key).is<picojson::null>()) {
		GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_UNEXPECTED_TYPE,
			"Json key type unmatched , key = " << key);
	}
	if (jsonValue == NULL && !isOption) {
		GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_UNEXPECTED_TYPE,
			"Json key not found, key = " << key);
	}
	bool exist = false;
	if (jsonValue != NULL) {
		util::StrictLexicalConverter<T>()(jsonValue->c_str(), output);
		exist = true;
	}
	return exist;
}

void BibInfo::Option::load(const picojson::value &json) {
	BibInfo::setKey<std::string>(json, "containerFileType", containerFileType_, false);
	BibInfo::setKey<std::string>(json, "storeMemoryLimit", storeMemoryLimit_, false);
	BibInfo::setKey<std::string>(json, "logDirectory", logDirectory_, false);
	BibInfo::setIntegerKey<int64_t>(json, "flushThreshold", flushThreshold_, true);
	if (flushThreshold_ < 0) {
		GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_UNEXPECTED_TYPE,
			"Value of flushThreshold is invalid, flushThreshold = " << flushThreshold_);
	}
	if (flushThreshold_ > LIMIT_FLUSH_THRESHOLD) {
		GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_UNEXPECTED_TYPE,
			"Value of flushThreshold is over, limit =  " << LIMIT_FLUSH_THRESHOLD);
	}
	BibInfo::setIntegerKey<int64_t>(json, "blockSize", blockSize_, true);
	if (blockSize_ < 0) {
		GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_UNEXPECTED_TYPE,
			"Value of blockSize is invalid, blockSize = " << blockSize_);
	}
	if (blockSize_ > LIMIT_BLOCK_SIZE) {
		GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_UNEXPECTED_TYPE,
			"Value of blockSize is over, limit =  " << LIMIT_BLOCK_SIZE);
	}
	BibInfo::setIntegerKey<int64_t>(json, "preReadNum", preReadNum_, true);
	if (preReadNum_ < 0) {
		GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_UNEXPECTED_TYPE,
			"Value of preReadNum is invalid, preReadNum = " << preReadNum_);
	}
}

void BibInfo::Container::load(const picojson::value &json) {
	BibInfo::setKey<std::string>(json, "containerFileBase", containerFileBase_, false);
	const picojson::value *attrJson = JsonUtils::find<picojson::value>(json, "containerAttribute");
	if (attrJson == NULL) {
		GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_UNEXPECTED_TYPE,
			"Json key not found or type unmatched , key = " << "containerAttribute");
	}

	BibInfo::setKey<std::string>(*attrJson, "database", database_, true);
	BibInfo::setKey<std::string>(*attrJson, "container", container_, false);
	BibInfo::setKey<std::string>(*attrJson, "dataAffinity", dataAffinity_, true);
	BibInfo::setKey<std::string>(*attrJson, "containerType", containerType_, false);

	BibInfo::setStringIntegerKey<DatabaseId>(*attrJson, "databaseId", databaseId_, false);
	BibInfo::setStringIntegerKey<ContainerId>(*attrJson, "containerId", containerId_, false);
	BibInfo::setStringIntegerKey<OId>(*attrJson, "rowIndexOId", rowIndexOId_, false);
	BibInfo::setStringIntegerKey<OId>(*attrJson, "mvccIndexOId", mvccIndexOId_, false);
	BibInfo::setStringIntegerKey<int64_t>(*attrJson, "initSchemaStatus", initSchemaStatus_, true);
	BibInfo::setIntegerKey<SchemaVersionId>(*attrJson, "schemaVersion", schemaVersion_, false);
	BibInfo::setIntegerKey<PartitionId>(*attrJson, "partitionNo", partitionNo_, false);

	BibInfo::setKey<bool>(*attrJson, "rowKeyAssigned", rowKeyAssigned_, true);

	const picojson::value *timeSeriesProperties = JsonUtils::find<picojson::value>(*attrJson, "timeSeriesProperties");
	if (timeSeriesProperties != NULL) {
		if (!timeSeriesProperties->is<picojson::object>()) {
			GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_UNEXPECTED_TYPE,
				"Json key type unmatched , key = " << "timeSeriesProperties");
		}
		timeSeriesProperties_.load(*timeSeriesProperties);
	}

	const std::vector<picojson::value> *columnSetValue = 
		JsonUtils::find< std::vector<picojson::value> >(*attrJson, "columnSet");
	{
		if (columnSetValue == NULL) {
			GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_UNEXPECTED_TYPE,
				"Json key not found or type unmatched , key = " << "columnSet");
		}
		if (columnSetValue->empty()) {
			GS_THROW_USER_ERROR(GS_ERROR_JSON_UNEXPECTED_TYPE,
				"empty array, key = " << "columnSet");
		}
		for (uint32_t i = 0; i < columnSetValue->size(); i++) {
			Column columnValue;
			columnValue.load((*columnSetValue)[i]);
			columnSet_.push_back(columnValue);
		}
	}

	const picojson::value *compressionInfoSetValue = 
		JsonUtils::find< picojson::value >(*attrJson, "compressionInfoSet");
	{
		if (compressionInfoSetValue != NULL) {
			if (!compressionInfoSetValue->is<picojson::array>()) {
				GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_UNEXPECTED_TYPE,
					"Json key type unmatched , key = " << "compressionInfoSet");
			}
			const picojson::array &arrayValue = compressionInfoSetValue->get<picojson::array>();
			for (uint32_t i = 0; i < arrayValue.size(); i++) {
				CompressionInfo compressionInfoValue;
				compressionInfoValue.load(arrayValue[i]);
				compressionInfoSet_.push_back(compressionInfoValue);
			}
		}
	}
}

void BibInfo::Container::Column::load(const picojson::value &json) {
	BibInfo::setKey<std::string>(json, "columnName", columnName_, false);
	BibInfo::setKey<std::string>(json, "type", type_, false);
	BibInfo::setKey<bool>(json, "notNull", notNull_, true);
}

void BibInfo::Container::TimeSeriesProperties::load(const picojson::value &json) {
	isExist_ = true;
	BibInfo::setKey<std::string>(json, "rowExpirationTimeUnit", rowExpirationTimeUnit_, true);
	BibInfo::setKey<std::string>(json, "compressionMethod", compressionMethod_, true);
	BibInfo::setKey<std::string>(json, "compressionWindowSizeUnit", compressionWindowSizeUnit_, true);
	BibInfo::setIntegerKey<int32_t>(json, "rowExpirationElapsedTime", rowExpirationElapsedTime_, true);
	BibInfo::setIntegerKey<int32_t>(json, "expirationDivisionCount", expirationDivisionCount_, true);
	BibInfo::setIntegerKey<int32_t>(json, "compressionWindowSize", compressionWindowSize_, true);
}

void BibInfo::Container::CompressionInfo::load(const picojson::value &json) {
	BibInfo::setKey<std::string>(json, "columnName", columnName_, false);
	BibInfo::setKey<std::string>(json, "compressionType", compressionType_, false);
	bool existRate = BibInfo::setKey<double>(json, "rate", rate_, true);
	bool existSpan = BibInfo::setKey<double>(json, "span", span_, true);
	bool existWidth = BibInfo::setKey<double>(json, "width", width_, true);
	if (compressionType_.compare("ABSOLUTE") == 0 && !existWidth) {
		GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_UNEXPECTED_TYPE,
			"Json key 'width' must be defined, if Json key 'compressionType_' is 'ABSOLUTE'");
	} else if (compressionType_.compare("RELATIVE") == 0  && (!existWidth || !existSpan)) {
		GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_UNEXPECTED_TYPE,
			"Json key 'width' and 'span' must be defined, if Json key 'compressionType_' is 'RELATIVE'");
	}
	if (existWidth && (existRate || existSpan)) {
		GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_UNEXPECTED_TYPE,
			"Json key 'width' must not be present at the same time as 'span' or 'width'");
	}
}

void BibInfo::load(std::string &jsonString) {
	picojson::value bibValue;
	std::string error;
	picojson::parse(bibValue, jsonString.begin(), jsonString.end(), &error);
	if (!error.empty()) {
		GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_INVALID_SYNTAX,
			"Json parse error : " << error);
	}
	try {
		const picojson::value *optionValue = JsonUtils::find<picojson::value>(bibValue, "option");
		if (optionValue == NULL || !optionValue->is<picojson::object>()) {
			GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_UNEXPECTED_TYPE,
				"Json key not found or type unmatched , key = " << "option");
		}
		option_.load(*optionValue);
		const std::vector<picojson::value> *containersValue = 
			JsonUtils::find< std::vector<picojson::value> >(bibValue, "containers");
		if (containersValue == NULL) {
			GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_UNEXPECTED_TYPE,
				"Json key not found or type unmatched , key = " << "containers");
		}
		if (containersValue->empty()) {
			GS_THROW_USER_ERROR(GS_ERROR_JSON_UNEXPECTED_TYPE,
				"empty array, key = " << "containers");
		}
		for (uint32_t i = 0; i < containersValue->size(); i++) {
			Container containerValue;
			containerValue.load((*containersValue)[i]);
			containers_.push_back(containerValue);
		}
	} catch (UserException &e) {
		std::cerr << GS_EXCEPTION_MESSAGE(e) << std::endl;
		throw;
	}
}
