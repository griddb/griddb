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
				const util::String *oldColumName = oldColumnNameList[j];

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

			if (afterColumnIds.size() > 0) {
				isExist = true;

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
			else {
				triggerList.push_back(encodedTrigger);
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
	isKey_ = false;
}

void ColumnInfo::set(TransactionContext &txn, ObjectManager &objectManager,
	uint32_t toColumnId, uint32_t fromColumnId, MessageSchema *messageSchema,
	const AllocateStrategy &allocateStrategy) {
	columnId_ = static_cast<uint16_t>(toColumnId);

	const util::String &columnName = messageSchema->getColumnName(fromColumnId);
	uint32_t size = static_cast<uint32_t>(columnName.size());

	BaseObject stringObject(txn.getPartitionId(), objectManager);
	char *stringPtr = stringObject.allocate<char>(
		size + 1, allocateStrategy, columnNameOId_, OBJECT_TYPE_VARIANT);
	memcpy(stringPtr, columnName.c_str(), size + 1);

	setType(messageSchema->getColumnType(fromColumnId),
		messageSchema->getIsArray(fromColumnId));

	if (fromColumnId == messageSchema->getRowKeyColumnId()) {
		isKey_ = true;
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

	bool isArrayVal = isArray();
	schema.push_back(reinterpret_cast<uint8_t *>(&isArrayVal), sizeof(bool));
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
	rowFixedSize_ = 0;
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
	MessageSchema *messageSchema, const AllocateStrategy &allocateStrategy) {
	ColumnInfo *columnInfoList = getColumnInfoList();
	if (messageSchema->getRowKeyColumnId() != UNDEF_COLUMNID) {
		bool hasVariableColumn = false;
		uint32_t columnId = 1;
		for (uint32_t i = 0; i < columnNum_; i++) {
			if (i == messageSchema->getRowKeyColumnId()) {
				columnInfoList[0].initialize();
				columnInfoList[0].set(
					txn, objectManager, 0, i, messageSchema, allocateStrategy);
				if (!hasVariableColumn && columnInfoList[0].isVariable()) {
					hasVariableColumn = true;
				}
			}
			else {
				columnInfoList[columnId].initialize();
				columnInfoList[columnId].set(txn, objectManager, columnId, i,
					messageSchema, allocateStrategy);
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
		rowFixedSize_ = 0;
		uint16_t variableColumnIndex = 0;  
		for (uint16_t i = 0; i < columnNum_; i++) {
			if (columnInfoList[i].isVariable()) {
				columnInfoList[i].setOffset(variableColumnIndex);  
				++variableColumnIndex;
			}
			else {
				columnInfoList[i].setOffset(nullsAndVarOffset + rowFixedSize_);
				rowFixedSize_ += columnInfoList[i].getColumnSize();
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
				txn, objectManager, i, i, messageSchema, allocateStrategy);
			if (columnInfoList[i].isVariable()) {
				columnInfoList[i].setOffset(variableColumnIndex);  
				++variableColumnIndex;
			}
			else {
				columnInfoList[i].setOffset(nullsAndVarOffset + rowFixedSize_);
				rowFixedSize_ += columnInfoList[i].getColumnSize();
			}
		}
		variableColumnNum_ = variableColumnIndex;

		if (variableColumnIndex > 0) {
			nullsAndVarOffset =
				ValueProcessor::calcNullsByteSize(columnNum_) + sizeof(OId);
			rowFixedSize_ = 0;
			for (uint16_t i = 0; i < columnNum_; i++) {
				if (!columnInfoList[i].isVariable()) {
					columnInfoList[i].setOffset(
						nullsAndVarOffset + rowFixedSize_);
					rowFixedSize_ += columnInfoList[i].getColumnSize();
				}
			}
		}
	}
}

void ColumnSchema::getColumnInfo(TransactionContext &txn,
	ObjectManager &objectManager, const char *name, uint32_t &columnId,
	ColumnInfo *&columnInfo) const {
	for (uint32_t i = 0; i < columnNum_; i++) {
		ColumnInfo &checkColumnInfo = getColumnInfo(i);
		const char *stringObject =
			checkColumnInfo.getColumnName(txn, objectManager);
		const uint8_t *columnName =
			reinterpret_cast<const uint8_t *>(stringObject);
		uint32_t columnNameSize = static_cast<uint32_t>(strlen(stringObject));
		if (eqCaseStringStringI(txn, reinterpret_cast<const uint8_t *>(name),
				static_cast<uint32_t>(strlen(name)), columnName,
				columnNameSize)) {
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
	int64_t hashVal =
		messageSchema->getColumnCount() + messageSchema->getRowKeyColumnId();
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
	ColumnId keyColumnId = messageSchema->getRowKeyColumnId();

	if (columnNum != getColumnNum()) {
		isSameSchema = false;
	}
	if (isSameSchema) {
		ColumnInfo &keyColumnInfo = getColumnInfo(0);
		if (keyColumnInfo.isKey()) {  
			if (keyColumnId == UNDEF_COLUMNID) {
				isSameSchema = false;
			}
			else {
				for (uint16_t i = 0; i < keyColumnId; i++) {
					columnMap.push_back(i + 1);
				}
				columnMap.push_back(0);
				for (uint16_t i = keyColumnId + 1; i < columnNum; i++) {
					columnMap.push_back(i);
				}
			}
		}
		else {  
			if (keyColumnId != UNDEF_COLUMNID) {
				isSameSchema = false;
			}
			else {
				for (uint16_t i = 0; i < columnNum; i++) {
					columnMap.push_back(i);
				}
			}
		}
	}

	if (isSameSchema) {
		for (uint32_t i = 0; i < columnNum; i++) {
			const util::String &newColumnName = messageSchema->getColumnName(i);
			ColumnType columnType = messageSchema->getColumnType(i);
			bool isArray = messageSchema->getIsArray(i);

			ColumnInfo &columnInfo = getColumnInfo(columnMap[i]);
			const char *stringObject =
				columnInfo.getColumnName(txn, objectManager);
			const uint8_t *columnName =
				reinterpret_cast<const uint8_t *>(stringObject);
			uint32_t columnNameSize =
				static_cast<uint32_t>(strlen(stringObject));
			if (eqTable[COLUMN_TYPE_STRING][COLUMN_TYPE_STRING](
					txn, columnName, columnNameSize,
					reinterpret_cast<const uint8_t *>(newColumnName.c_str()),
					static_cast<uint32_t>(
						newColumnName.length()))) {  

				if (columnInfo.getSimpleColumnType() != columnType ||
					columnInfo.isArray() != isArray) {
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
	TransactionContext &txn, uint16_t reserveNum, uint16_t indexNum) {
	BaseObject::allocate<uint8_t>(IndexSchema::getAllocateSize(reserveNum),
		allocateStrategy_, getBaseOId(), OBJECT_TYPE_COLUMNINFO);

	setNum(indexNum);
	setReserveNum(reserveNum);
}

/*!
	@brief Appends Index Info to Index Schema
*/
bool IndexSchema::createIndexInfo(
	TransactionContext &txn, ColumnId columnId, MapType mapType) {
	setDirty();

	bool isDuplicate = false;
	if (isFull()) {
		expand(txn);
		isDuplicate = true;
	}
	setIndexInfo(txn, getIndexNum(), columnId, mapType);
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
			for (uint16_t j = i; j < getIndexNum(); j++) {
				if (j + 1 < getIndexNum()) {
					IndexData nextIndexData;
					getIndexData(
						txn, j + 1, UNDEF_CONTAINER_POS, nextIndexData);
					setIndexData(txn, j, UNDEF_CONTAINER_POS, nextIndexData);
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
void IndexSchema::createIndexData(TransactionContext &txn, ColumnId columnId,
	MapType mapType, ColumnType columnType, BaseContainer *container,
	uint64_t containerPos, bool isUnique) {
	setDirty();

	IndexData indexData;
	indexData.columnId_ = columnId;
	indexData.mapType_ = mapType;
	switch (mapType) {
	case MAP_TYPE_BTREE: {
		BtreeMap map(txn, *getObjectManager(),
			container->getMapAllcateStrategy(), container);
		map.initialize(txn, columnType, isUnique, BtreeMap::TYPE_SINGLE_KEY);
		indexData.oId_ = map.getBaseOId();
	} break;
	case MAP_TYPE_HASH: {
		HashMap map(txn, *getObjectManager(),
			container->getMapAllcateStrategy(), container);
		map.initialize(txn, columnType, columnId,
			container->getMetaAllcateStrategy(), isUnique);
		indexData.oId_ = map.getBaseOId();
	} break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
	}
	uint16_t nth = getNth(columnId, mapType);
	setIndexData(txn, nth, containerPos, indexData);
}

/*!
	@brief Free Index Object
*/
void IndexSchema::dropIndexData(TransactionContext &txn, ColumnId columnId,
	MapType mapType, BaseContainer *container, uint64_t containerPos,
	bool isMapFinalize) {
	setDirty();
	if (isMapFinalize) {
		StackAllocAutoPtr<BaseIndex> map(txn.getDefaultAllocator(),
			getIndex(txn, mapType, columnId, container, containerPos));
		map.get()->finalize(txn);
	}
	uint16_t nth = getNth(columnId, mapType);
	uint8_t *indexDataPos = getElemHead() + (getIndexDataSize() * nth);
	removeOId(txn, indexDataPos, containerPos);
}

/*!
	@brief Updates OId of Index Object
*/
void IndexSchema::updateIndexData(
	TransactionContext &txn, IndexData updateIndexData, uint64_t containerPos) {
	setDirty();
	uint16_t nth = getNth(updateIndexData.columnId_, updateIndexData.mapType_);
	uint8_t *indexDataPos = getElemHead() + (getIndexDataSize() * nth);
	updateOId(txn, indexDataPos, containerPos, updateIndexData.oId_);
}

/*!
	@brief Check if Index is defined
*/
bool IndexSchema::hasIndex(
	TransactionContext &txn, ColumnId columnId, MapType mapType) const {
	IndexData indexData;
	return getIndexData(txn, columnId, mapType, UNDEF_CONTAINER_POS, indexData);
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
	if (getIndexData(
			txn, columnId, MAP_TYPE_BTREE, UNDEF_CONTAINER_POS, indexData)) {
		indexType |= (1 << MAP_TYPE_BTREE);
	}
	if (getIndexData(
			txn, columnId, MAP_TYPE_HASH, UNDEF_CONTAINER_POS, indexData)) {
		indexType |= (1 << MAP_TYPE_HASH);
	}

	return indexType;
}

/*!
	@brief Get list of Index Data defined on Columns
*/
void IndexSchema::getIndexList(TransactionContext &txn, uint64_t containerPos,
	util::XArray<IndexData> &list) const {
	for (uint16_t i = 0; i < getIndexNum(); i++) {
		IndexData indexData;
		getIndexData(txn, i, containerPos, indexData);
		list.push_back(indexData);
	}
}

/*!
	@brief Get Index Data defined on Column
*/
bool IndexSchema::getIndexData(TransactionContext &txn, ColumnId columnId,
	MapType mapType, uint64_t containerPos, IndexData &indexData) const {
	bool isFound = false;
	for (uint16_t i = 0; i < getIndexNum(); i++) {
		uint8_t *indexDataPos = getElemHead() + (getIndexDataSize() * i);
		if (getColumnId(indexDataPos) == columnId &&
			getMapType(indexDataPos) == mapType) {
			indexData.oId_ = getOId(txn, indexDataPos, containerPos);
			indexData.columnId_ = getColumnId(indexDataPos);
			indexData.mapType_ = getMapType(indexDataPos);

			isFound = true;
			break;
		}
	}
	return isFound;
}

/*!
	@brief Free all Index Objects
*/
void IndexSchema::dropAll(TransactionContext &txn, BaseContainer *container,
	uint64_t containerPos, bool isMapFinalize) {
	setDirty();
	{
		util::XArray<IndexData> list(txn.getDefaultAllocator());
		getIndexList(txn, containerPos, list);
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
		getIndexList(txn, UNDEF_CONTAINER_POS, list);
		for (size_t i = 0; i < list.size(); i++) {
			dropIndexInfo(txn, list[i].columnId_, list[i].mapType_);
		}
		BaseObject::finalize();
	}
}

/*!
	@brief Get Index Object
*/
BaseIndex *IndexSchema::getIndex(TransactionContext &txn, MapType mapType,
	ColumnId columnId, BaseContainer *container, uint64_t containerPos) const {
	IndexData indexData;
	AllocateStrategy strategy = container->getMapAllcateStrategy();
	if (getIndexData(txn, columnId, mapType, containerPos, indexData)) {
		BaseIndex *map = NULL;
		switch (mapType) {
		case MAP_TYPE_BTREE:
			map = ALLOC_NEW(txn.getDefaultAllocator()) BtreeMap(
				txn, *getObjectManager(), indexData.oId_, strategy, container);
			break;
		case MAP_TYPE_HASH:
			map = ALLOC_NEW(txn.getDefaultAllocator()) HashMap(
				txn, *getObjectManager(), indexData.oId_, strategy, container);
			break;
		default:
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
		}
		return map;
	}
	else {
		return NULL;
	}
}

void IndexSchema::expand(TransactionContext &txn) {
	setDirty();
	uint16_t orgIndexNum = getIndexNum();
	uint16_t newReserveIndexNum = getReserveNum() * 2;
	OId duplicateOId;
	uint8_t *toIndexSchema = getObjectManager()->allocate<uint8_t>(
		txn.getPartitionId(), getAllocateSize(newReserveIndexNum),
		allocateStrategy_, duplicateOId, OBJECT_TYPE_COLUMNINFO);
	memcpy(toIndexSchema, getBaseAddr(), getAllocateSize(orgIndexNum));
	finalize(txn);

	setBaseOId(duplicateOId);
	setBaseAddr(toIndexSchema);
	setNum(orgIndexNum);
	setReserveNum(newReserveIndexNum);
}


/*!
	@brief Allocate ShareValueList Object
*/
void ShareValueList::initialize(TransactionContext &txn, uint32_t allocateSize,
	const AllocateStrategy &allocateStrategy) {
	BaseObject::allocate<uint8_t>(
		allocateSize, allocateStrategy, getBaseOId(), OBJECT_TYPE_CONTAINER_ID);
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
template TimeSeries::ExpirationInfo *ShareValueList::get(
	CONTAINER_META_TYPE type) const;
template char *ShareValueList::get(CONTAINER_META_TYPE type) const;
template TriggerList *ShareValueList::get(CONTAINER_META_TYPE type) const;
template ContainerAttribute *ShareValueList::get(
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
uint64_t LinkArray<void, OId>::getHeaderSize() {
	return 0;
}

template uint64_t LinkArray<TimeSeries::BaseSubTimeSeriesData,
	TimeSeries::SubTimeSeriesImage>::getHeaderSize();
template <typename H, typename V>
uint64_t LinkArray<H, V>::getHeaderSize() {
	return sizeof(H);
}

template const OId *LinkArray<void, OId>::get(
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

template void LinkArray<void, OId>::insert(TransactionContext &txn,
	uint64_t pos, const OId *value, const AllocateStrategy &allocateStrategy);
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
		V tailImage = *(chain.getCursor<V>() + elemNum - 1);

		uint64_t maxPos;
		if (elemNum < MAX_LOCAL_ELEMENT_NUM) {
			maxPos = elemNum;
		}
		else {
			maxPos = elemNum - 1;
		}

		if (elemNum > 0) {
			for (uint64_t i = maxPos; i > startPos; i--) {
				V *fromValue = chain.getCursor<V>() + i - 1;
				V *toValue = chain.getCursor<V>() + i;
				*toValue = *fromValue;
			}
		}
		V *insertValue = chain.getCursor<V>() + startPos;
		*insertValue = insertImage;

		if (elemNum < MAX_LOCAL_ELEMENT_NUM) {
			break;
		}
		else {
			insertImage = tailImage;

			chainNo++;
			startPos = 0;
			elemNum = getChainElemNum(chainNo);
			chain.load(getNextOId(chain.getCursor<V>()));
		}
	}
	increment();
}

template void LinkArray<void, OId>::update(
	TransactionContext &txn, uint64_t pos, const OId *value);
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

template void LinkArray<void, OId>::remove(
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
		for (uint64_t i = startPos; i < elemNum; i++) {
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

template OId LinkArray<void, OId>::expand(
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

template void LinkArray<void, OId>::dump(TransactionContext &txn);
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
