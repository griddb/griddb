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
#include "data_store_v4.h"
#include "rtree_map.h"
#include "message_schema.h"
#include "value_processor.h"

/*!
	@brief Initialize the area in ColumnInfo
*/
void ColumnInfo::initialize() {
	columnNameOId_ = UNDEF_OID;
	columnType_ = COLUMN_TYPE_WITH_BEGIN;
	offset_ = 0;
	flags_ = 0;
}

void ColumnInfo::set(TransactionContext &txn, ObjectManagerV4 &objectManager,
	uint32_t toColumnId, uint32_t fromColumnId, MessageSchema *messageSchema,
	AllocateStrategy &allocateStrategy, bool onMemory) {
	columnId_ = static_cast<uint16_t>(toColumnId);

	const util::String &columnName = messageSchema->getColumnName(fromColumnId);
	uint32_t size = static_cast<uint32_t>(columnName.size());

	char *stringPtr = NULL;
	if (onMemory) {
		columnNameOnMemory_ = static_cast<char *>(txn.getDefaultAllocator().allocate(size + 1));
		strcpy(columnNameOnMemory_, columnName.c_str());
	} else {
		BaseObject stringObject(objectManager, allocateStrategy);
		stringPtr = stringObject.allocate<char>(
			size + 1, columnNameOId_, OBJECT_TYPE_VARIANT);
		memcpy(stringPtr, columnName.c_str(), size + 1);
	}

	setType(messageSchema->getColumnFullType(fromColumnId));

	flags_ = 0;

	const util::Vector<ColumnId> &keyColumnIds =
		messageSchema->getRowKeyColumnIdList();
	util::Vector<ColumnId>::const_iterator itr = 
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
	ObjectManagerV4 &objectManager, AllocateStrategy& strategy) {
	if (columnNameOId_ != UNDEF_OID) {
		ChunkAccessor ca;
		objectManager.free(ca, strategy.getGroupId(), columnNameOId_);
		columnNameOId_ = UNDEF_OID;
	}
}

/*!
	@brief translate into Message format
*/
void ColumnInfo::getSchema(
		TransactionContext &txn, ObjectManagerV4 &objectManager,
		AllocateStrategy &strategy, util::XArray<uint8_t> &schema) {
	char *columnName = const_cast<char *>(getColumnName(txn, objectManager, strategy));
	int32_t columnNameLen = static_cast<int32_t>(strlen(columnName));
	schema.push_back(
		reinterpret_cast<uint8_t *>(&columnNameLen), sizeof(int32_t));
	schema.push_back(reinterpret_cast<uint8_t *>(columnName), columnNameLen);

	const int8_t typeOrdinal = ValueProcessor::getPrimitiveColumnTypeOrdinal(
			getColumnType(), false);
	schema.push_back(
			reinterpret_cast<const uint8_t *>(&typeOrdinal),
			sizeof(typeOrdinal));

	const uint8_t flags = MessageSchema::makeColumnFlags(
			isArray(), isVirtual(), isNotNull());
	schema.push_back(&flags, sizeof(flags));
}

std::string ColumnInfo::dump(
	TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy& strategy) {
	util::NormalOStringStream strstrm;
	const char *name = this->getColumnName(txn, objectManager, strategy);
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
	ObjectManagerV4 &objectManager, AllocateStrategy& allocateStrategy) {
	for (uint32_t i = 0; i < columnNum_; i++) {
		ColumnInfo &columnInfo = getColumnInfo(i);
		columnInfo.finalize(objectManager, allocateStrategy);
	}
}

/*!
	@brief Set Column layout
*/
void ColumnSchema::set(TransactionContext &txn, ObjectManagerV4 &objectManager,
	MessageSchema *messageSchema, AllocateStrategy &allocateStrategy,
	bool onMemory) {
	ColumnInfo *columnInfoList = getColumnInfoList();
	const util::Vector<ColumnId> &keyColumnIds = messageSchema->getRowKeyColumnIdList();
	uint16_t *rowKeyNumPtr = reinterpret_cast<uint16_t *>(getRowKeyPtr());
	uint16_t rowKeyNum = static_cast<uint16_t>(keyColumnIds.size());
	*rowKeyNumPtr = rowKeyNum;
	rowKeyNumPtr++;
	bool hasVariableColumn = false;

	uint32_t columnId = 0;
	util::Vector<ColumnId>::const_iterator itr;
	for (itr = keyColumnIds.begin(); itr != keyColumnIds.end(); itr++) {
		uint32_t i = *itr;
		columnInfoList[columnId].initialize();
		columnInfoList[columnId].set(txn, objectManager, columnId, i,
			messageSchema, allocateStrategy, onMemory);
		if (!hasVariableColumn &&
			columnInfoList[columnId].isVariable()) {
			hasVariableColumn = true;
		}

		*rowKeyNumPtr = columnId;
		rowKeyNumPtr++;

		columnId++;
	}
	for (uint32_t i = 0; i < columnNum_; i++) {
		util::Vector<ColumnId>::const_iterator itr = 
			std::find(keyColumnIds.begin(), keyColumnIds.end(), i);
		if (itr == keyColumnIds.end()) {
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
	{
		uint16_t *rowKeyNumPtr = reinterpret_cast<uint16_t *>(getRowKeyPtr());
		const util::Vector<ColumnId> &schemaKeyColumnIdList = messageSchema->getRowKeyColumnIdList();
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

void ColumnSchema::set(util::StackAllocator &alloc,
		const ColumnSchema *srcSchema,
		const util::Vector<ColumnId> &columnIds) {
	UNUSED_VARIABLE(alloc);
	ColumnInfo *columnInfoList = getColumnInfoList();
	uint16_t variableColumnIndex = 0;  
	uint32_t nullsAndVarOffset =
		ValueProcessor::calcNullsByteSize(columnNum_);  
	for (uint16_t i = 0; i < columnNum_; i++) {
		ColumnId oldColumnId = columnIds[i];
		columnInfoList[i] = srcSchema->getColumnInfo(oldColumnId);
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

void ColumnSchema::getColumnInfo(TransactionContext &txn,
	ObjectManagerV4 &objectManager, AllocateStrategy& strategy, const char *name, uint32_t &columnId,
	ColumnInfo *&columnInfo, bool isCaseSensitive) const {
	for (uint32_t i = 0; i < columnNum_; i++) {
		ColumnInfo &checkColumnInfo = getColumnInfo(i);
		const char *columnName =
			checkColumnInfo.getColumnName(txn, objectManager, strategy);
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
	const util::Vector<ColumnId> &keyColumnIds = messageSchema->getRowKeyColumnIdList();
	util::Vector<ColumnId>::const_iterator itr;
	for (itr = keyColumnIds.begin(); itr != keyColumnIds.end(); itr++) {
		hashVal += *itr;
	}
	hashVal = hashVal << 32;
	for (uint32_t i = 0; i < messageSchema->getColumnCount(); i++) {
		int32_t columnHashVal = 0;
		columnHashVal += messageSchema->getColumnFullType(i);
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
	ObjectManagerV4 &objectManager, AllocateStrategy& strategy, MessageSchema *messageSchema) {
	bool isSameSchema = true;
	util::Vector<ColumnId> columnMap(txn.getDefaultAllocator());

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
		util::Vector<ColumnId> keyColumnIdList(txn.getDefaultAllocator());
		getKeyColumnIdList(keyColumnIdList);
		const util::Vector<ColumnId> &schemaKeyColumnIdList = messageSchema->getRowKeyColumnIdList();
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
			ColumnType columnType = messageSchema->getColumnFullType(i);
			bool isNotNull = messageSchema->getIsNotNull(i);

			ColumnInfo &columnInfo = getColumnInfo(columnMap[i]);
			const char *stringObject =
				columnInfo.getColumnName(txn, objectManager, strategy);
			const uint8_t *columnName =
				reinterpret_cast<const uint8_t *>(stringObject);
			uint32_t columnNameSize =
				static_cast<uint32_t>(strlen(stringObject));
			if (eqStringString(
					txn, columnName, columnNameSize,
					reinterpret_cast<const uint8_t *>(newColumnName.c_str()),
					static_cast<uint32_t>(
						newColumnName.length()))) {  

				if (columnInfo.getColumnType() != columnType ||
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

std::string ColumnSchema::dump(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy& strategy) {
	util::NormalOStringStream ss;
	for (size_t i = 0; i < getColumnNum(); i++) {
		ss << "[" << i << "]" << getColumnInfo(i).dump(txn, objectManager, strategy) << std::endl;
	}
	return ss.str();
}

void IndexAutoPtr::clear() throw() {
	if (index_ != NULL) {
		assert(storage_ != NULL && index_ != NULL);
		DataStoreV4::releaseIndex(**storage_);
	}
	index_ = NULL;
	storage_ = NULL;
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
			getBaseOId(), OBJECT_TYPE_COLUMNINFO);
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
	TransactionContext &txn, util::Vector<ColumnId> &columnIds, MapType mapType) {
	setDirty();

	bool isFound = false;
	IndexData indexData(txn.getDefaultAllocator());
	ChunkAccessor ca;
	for (uint16_t i = 0; i < getIndexNum(); i++) {
		getIndexData(txn, i, indexData);
		if (indexData.mapType_ == mapType &&
			(columnIds.size() == indexData.columnIds_->size()) &&
			std::equal(columnIds.begin(), columnIds.end(), indexData.columnIds_->begin())) {
			isFound = true;
			OId optionOId = getOptionOId(getElemHead() + (getIndexDataSize() * i));
			getObjectManager()->free(ca, allocateStrategy_.getGroupId(), optionOId);
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
IndexData IndexSchema::createIndexData(TransactionContext &txn, const util::Vector<ColumnId> &columnIds,
	MapType mapType, const util::Vector<ColumnType> &columnTypes, BaseContainer *container,
	bool isUnique) {
	setDirty();

	TreeFuncInfo *funcInfo = container->createTreeFuncInfo(txn, columnIds);
	OId oId;
	switch (mapType) {
	case MAP_TYPE_BTREE: {
		uint32_t elemSize = BtreeMap::getDefaultElemSize();
		ColumnType columntype = columnTypes[0];
		if (columnIds.size() > 1) {
			columntype = COLUMN_TYPE_COMPOSITE;
			ColumnSchema *columnSchema = funcInfo->getColumnSchema();
			uint32_t fixedColumnsSize = columnSchema->getRowFixedColumnSize();
			bool hasVariable = columnSchema->getVariableColumnNum() > 0;
			elemSize = CompositeInfoObject::calcSize(
				columnSchema->getColumnNum(),
				fixedColumnsSize, hasVariable) + sizeof(OId);
		}
		BtreeMap map(txn, *getObjectManager(),
			container->getMapAllocateStrategy(), container, funcInfo);
		map.initialize(txn, columntype, isUnique, BtreeMap::TYPE_SINGLE_KEY, elemSize);
		oId = map.getBaseOId();
	} break;
	case MAP_TYPE_SPATIAL: {
		RtreeMap map(txn, *getObjectManager(),
			container->getMapAllocateStrategy(), container, funcInfo);
		map.initialize(txn, columnTypes[0], columnIds[0],
			container->getMapAllocateStrategy(), isUnique);
		oId = map.getBaseOId();
	} break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TYPE_INVALID, "");
	}
	IndexData indexData(txn.getDefaultAllocator());
	bool withPartialMatch = false;
	uint16_t nth = getNth(txn, columnIds, mapType, withPartialMatch);
	getIndexData(txn, nth, indexData);
	indexData.oIds_.mainOId_ = oId;
	setIndexData(txn, nth, indexData);

	return indexData;
}

/*!
	@brief Free Index Object
*/
void IndexSchema::dropIndexData(TransactionContext &txn, util::Vector<ColumnId> &columnIds,
	MapType mapType, BaseContainer *container,
	bool isMapFinalize) {
	bool withPartialMatch = false;
	setDirty();
	if (isMapFinalize) {
		bool withUncommitted = true; 
		IndexData indexData(txn.getDefaultAllocator());
		if (getIndexData(txn, columnIds, mapType, withUncommitted,
			withPartialMatch, indexData)) {
			IndexAutoPtr mainMap;
			getIndex(txn, indexData, false, container, mainMap);
			if (mainMap.get() != NULL) {
				container->getDataStore()->finalizeMap
					(txn, container->getMapAllocateStrategy(), mainMap.get(), 
						container->getContainerExpirationTime());
			}
			IndexAutoPtr nullMap;
			getIndex(txn, indexData, true, container, nullMap);
			if (nullMap.get() != NULL) {
				container->getDataStore()->finalizeMap
					(txn, container->getMapAllocateStrategy(), nullMap.get(), 
						container->getContainerExpirationTime());
			}
		}
	}
	uint16_t nth = getNth(txn, columnIds, mapType, withPartialMatch);
	uint8_t *indexDataPos = getElemHead() + (getIndexDataSize() * nth);
	removeMapOIds(txn, indexDataPos);
}

/*!
	@brief Updates OId of Index Object
*/
void IndexSchema::updateIndexData(
	TransactionContext &txn, const IndexData &updateIndexData) {
	setDirty();
	bool withPartialMatch = false;
	uint16_t nth = getNth(txn, *(updateIndexData.columnIds_), updateIndexData.mapType_, withPartialMatch);
	uint8_t *indexDataPos = getElemHead() + (getIndexDataSize() * nth);
	updateMapOIds(txn, indexDataPos, updateIndexData.oIds_);
	if (updateIndexData.status_!= DDL_READY) {
		BaseObject option(*getObjectManager(),
			allocateStrategy_, getOptionOId(indexDataPos));
		option.setDirty();
		setRowId(option.getBaseAddr(), updateIndexData.cursor_);
	}
}

void IndexSchema::commit(TransactionContext &txn, IndexCursor &indexCursor) {
	UNUSED_VARIABLE(txn);
	setDirty();
	uint16_t nth = getNth(indexCursor.getColumnId(), indexCursor.getMapType(), indexCursor.getOId());
	uint8_t *indexDataPos = getElemHead() + (getIndexDataSize() * nth);
	if (nth == UNDEF_INDEX_POS || getStatus(indexDataPos) == DDL_READY) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "nth=" << nth
			<< ", optionOId=" << indexCursor.getOId() 
			<< ", columnId=" << indexCursor.getColumnId() 
			<< ", mapType=" << (int32_t)indexCursor.getMapType());
	}
	setStatus(indexDataPos, DDL_READY);
}

/*!
	@brief Check if Index is defined
*/
bool IndexSchema::hasIndex(
	TransactionContext &txn, util::Vector<ColumnId> &columnIds,
	MapType mapType, bool withPartialMatch) const {
	IndexData indexData(txn.getDefaultAllocator());
	bool withUncommitted = false;
	return getIndexData(txn, columnIds, mapType, 
		withUncommitted, withPartialMatch, indexData);
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
	TransactionContext &txn, util::Vector<ColumnId> &columnIds, 
	bool withPartialMatch) const {
	IndexTypes indexType = 0;
	bool withUncommitted = false;
	for (uint16_t i = 0; i < getIndexNum(); i++) {
		uint8_t *indexDataPos = getElemHead() + (getIndexDataSize() * i);
		DDLStatus status = getStatus(indexDataPos);
		if (compareColumnIds(txn, indexDataPos, columnIds, 
				getOptionOId(indexDataPos), withPartialMatch) &&
			(withUncommitted || status == DDL_READY)) {
			MapType mapType = getMapType(indexDataPos);
			indexType |= (1 << mapType);
		}
	}

	return indexType;
}

IndexTypes IndexSchema::getIndexTypes(
	TransactionContext &txn, ColumnId columnId) const {
	IndexTypes indexType = 0;
	IndexData indexData(txn.getDefaultAllocator());
	util::Vector<ColumnId> columnIds(txn.getDefaultAllocator());
	columnIds.push_back(columnId);
	bool withUncommitted = false;
	bool withPartialMatch = true;
	if (getIndexData(txn, columnIds, MAP_TYPE_BTREE,
		withUncommitted, withPartialMatch, indexData)) {
		indexType |= (1 << MAP_TYPE_BTREE);
	}
	if (getIndexData(txn, columnIds, MAP_TYPE_SPATIAL, 
		withUncommitted, withPartialMatch, indexData)) {
		indexType |= (1 << MAP_TYPE_SPATIAL);
	}
	return indexType;
}

/*!
	@brief Get list of Index Data defined on Columns
*/
void IndexSchema::getIndexList(TransactionContext &txn,
	bool withUncommitted, util::XArray<IndexData> &list) const {
	for (uint16_t i = 0; i < getIndexNum(); i++) {
		IndexData indexData(txn.getDefaultAllocator());
		getIndexData(txn, i, indexData);
		if (withUncommitted || indexData.status_ == DDL_READY) {
			list.push_back(indexData);
		}
	}
}

/*!
	@brief Get Index Data defined on Column
*/
bool IndexSchema::getIndexData(TransactionContext &txn, const util::Vector<ColumnId> &columnIds,
	MapType mapType, bool withUncommitted, bool withPartialMatch, 
	IndexData &indexData) const {
	bool isFound = false;
	for (uint16_t i = 0; i < getIndexNum(); i++) {
		uint8_t *indexDataPos = getElemHead() + (getIndexDataSize() * i);
		DDLStatus status = getStatus(indexDataPos);
		if (getMapType(indexDataPos) == mapType &&
		    compareColumnIds(txn, indexDataPos, columnIds, 
				getOptionOId(indexDataPos), withPartialMatch) &&
			(withUncommitted || status == DDL_READY)) {
			indexData.oIds_ = getMapOIds(txn, indexDataPos);
			indexData.optionOId_ = getOptionOId(indexDataPos);
			indexData.mapType_ = getMapType(indexDataPos);
			indexData.columnIds_->clear();
			if (isComposite(indexDataPos)) {
				BaseObject option(*getObjectManager(),
					*const_cast<AllocateStrategy* >(&allocateStrategy_), getOptionOId(indexDataPos));
				getCompositeColumnIds(option.getBaseAddr(), *(indexData.columnIds_));
			} else {
				indexData.columnIds_->push_back(getColumnId(indexDataPos));
			}

			indexData.status_ = getStatus(indexDataPos);
			if (status != DDL_READY) {
				BaseObject option(*getObjectManager(),
					*const_cast<AllocateStrategy*>(&allocateStrategy_), getOptionOId(indexDataPos));
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

bool IndexSchema::getIndexData(TransactionContext &txn, const IndexCursor &indexCursor,
	IndexData &indexData) const {

	ColumnId firstColumnId = indexCursor.getColumnId();
	MapType mapType = indexCursor.getMapType();
	OId optionOId = indexCursor.getOId();

	bool isFound = false;
	for (uint16_t i = 0; i < getIndexNum(); i++) {
		uint8_t *indexDataPos = getElemHead() + (getIndexDataSize() * i);
		DDLStatus status = getStatus(indexDataPos);
		if (getMapType(indexDataPos) == mapType &&
			getColumnId(indexDataPos) == firstColumnId &&
			getOptionOId(indexDataPos) == optionOId) {
			indexData.oIds_ = getMapOIds(txn, indexDataPos);
			indexData.optionOId_ = getOptionOId(indexDataPos);
			indexData.mapType_ = getMapType(indexDataPos);
			indexData.columnIds_->clear();
			if (isComposite(indexDataPos)) {
				BaseObject option(*getObjectManager(),
					*const_cast<AllocateStrategy*>(&allocateStrategy_), getOptionOId(indexDataPos));
				getCompositeColumnIds(option.getBaseAddr(), *(indexData.columnIds_));
			} else {
				indexData.columnIds_->push_back(getColumnId(indexDataPos));
			}

			indexData.status_ = getStatus(indexDataPos);
			if (status != DDL_READY) {
				BaseObject option(*getObjectManager(),
					*const_cast<AllocateStrategy*>(&allocateStrategy_), getOptionOId(indexDataPos));
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
	IndexData &indexData, BaseContainer *container) {
	setDirty();

	bool isUnique = false;
	ColumnType nullType = COLUMN_TYPE_NULL;
	BtreeMap map(txn, *getObjectManager(),
		container->getMapAllocateStrategy(), container);
	map.initialize(txn, nullType, isUnique, BtreeMap::TYPE_SINGLE_KEY);
	indexData.oIds_.nullOId_ = map.getBaseOId();

	bool withPartialMatch = false;
	uint16_t nth = getNth(txn, *(indexData.columnIds_), indexData.mapType_, withPartialMatch);
	uint8_t *indexDataPos = getElemHead() + (getIndexDataSize() * nth);
	updateMapOIds(txn, indexDataPos, indexData.oIds_);
}

void IndexSchema::getIndexInfoList(TransactionContext &txn, 
	BaseContainer *container, const IndexInfo &indexInfo, 
	bool withUncommitted, util::Vector<IndexInfo> &matchList, 
	util::Vector<IndexInfo> &mismatchList,
	bool isIndexNameCaseSensitive,
	bool withPartialMatch) {
	for (uint16_t i = 0; i < getIndexNum(); i++) {
		IndexData indexData(txn.getDefaultAllocator());
		getIndexData(txn, i, indexData);
		if (!withUncommitted && indexData.status_ != DDL_READY) {
			continue;
		}

		IndexInfo currentIndexInfo(txn.getDefaultAllocator());
		getIndexInfo(txn, i, currentIndexInfo);
		bool isColumnIdsMatch = false;
		if ((!withPartialMatch && indexInfo.columnIds_.size() == currentIndexInfo.columnIds_.size()) ||
			(withPartialMatch && indexInfo.columnIds_.size() <= currentIndexInfo.columnIds_.size())) {
			isColumnIdsMatch = std::equal(indexInfo.columnIds_.begin(), indexInfo.columnIds_.end(), currentIndexInfo.columnIds_.begin());
		}

		bool isMapTypeMatch;
		if (indexInfo.mapType == MAP_TYPE_DEFAULT) {
			ColumnInfo& columnInfo = container->getColumnInfo(currentIndexInfo.columnIds_[0]);
			MapType defaultType;
			findDefaultIndexType(columnInfo.getColumnType(), defaultType);
			isMapTypeMatch = (defaultType == currentIndexInfo.mapType);
		} else {
			isMapTypeMatch = (indexInfo.mapType == currentIndexInfo.mapType);
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

void IndexSchema::getIndexDataList(TransactionContext &txn, util::Vector<ColumnId> &columnIds,
	MapType mapType, bool withUncommitted, 
	util::Vector<IndexData> &indexDataList, bool withPartialMatch) {
	for (uint16_t i = 0; i < getIndexNum(); i++) {
		IndexData indexData(txn.getDefaultAllocator());
		getIndexData(txn, i, indexData);
		if (!withUncommitted && indexData.status_ != DDL_READY) {
			continue;
		}
		if (mapType != indexData.mapType_) {
			continue;
		}
		bool isColumnIdsMatch = false;
		if ((!withPartialMatch && columnIds.size() == columnIds.size()) ||
			(withPartialMatch && columnIds.size() <= columnIds.size())) {
			isColumnIdsMatch = std::equal(columnIds.begin(), columnIds.end(), indexData.columnIds_->begin());
		}
		if (isColumnIdsMatch) {
			indexDataList.push_back(indexData);
		}
	}
}

/*!
	@brief Free all Index Objects
*/
void IndexSchema::dropAll(TransactionContext &txn, BaseContainer *container,
	bool isMapFinalize) {
	setDirty();
	{
		util::XArray<IndexData> list(txn.getDefaultAllocator());
		bool withUncommitted = true;
		getIndexList(txn, withUncommitted, list);
		for (size_t i = 0; i < list.size(); i++) {
			dropIndexData(txn, *(list[i].columnIds_), list[i].mapType_, container,
				isMapFinalize);
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
		getIndexList(txn, withUncommitted, list);
		for (size_t i = 0; i < list.size(); i++) {
			dropIndexInfo(txn, *(list[i].columnIds_), list[i].mapType_);
		}
		BaseObject::finalize();
	}
}

/*!
	@brief Get Index Object
*/
void IndexSchema::getIndex(
		TransactionContext &txn, const IndexData &indexData, bool forNull,
		BaseContainer *container, IndexAutoPtr &indexPtr) const {
	AllocateStrategy& strategy = container->getMapAllocateStrategy();

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
		indexPtr.clear();
		return;
	}

	IndexStorageSet *storageSet = indexData.storageSet_;
	assert(storageSet != NULL);

	TreeFuncInfo *&funcInfo =
			(forNull ? storageSet->nullFuncInfo_ : storageSet->funcInfo_);
	funcInfo = container->createTreeFuncInfo(txn, *(indexData.columnIds_));

	BaseIndexStorage *&indexStorage =
			(forNull ? storageSet->nullIndexStorage_ : storageSet->indexStorage_);
	indexPtr.initialize(indexStorage, DataStoreV4::getIndex(
			txn, *getObjectManager(), mapType, mapOId, strategy, container,
			funcInfo, indexStorage));
}

void IndexSchema::expand(TransactionContext &txn) {
	setDirty();
	const uint16_t orgIndexNum = getIndexNum();
	const uint16_t newReserveIndexNum = getReserveNum() * 2;

	BaseObject duplicateObj(*getObjectManager(), strategy_);
	OId duplicateOId;
	uint8_t* toIndexSchema = duplicateObj.allocate<uint8_t>(
			getAllocateSize(newReserveIndexNum, getNullbitsSize()),
			duplicateOId, OBJECT_TYPE_COLUMNINFO);

	memset(toIndexSchema, 0, getAllocateSize(newReserveIndexNum, getNullbitsSize()));
	memcpy(toIndexSchema, getBaseAddr(), getAllocateSize(orgIndexNum, getNullbitsSize()));

	BaseObject::finalize();

	copyReference(duplicateObj);
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
	const uint16_t reserveIndexNum = getReserveNum();

	BaseObject duplicateObj(*getObjectManager(), strategy_);
	OId duplicateOId;
	uint8_t* toIndexSchema = duplicateObj.allocate<uint8_t>(
			getAllocateSize(reserveIndexNum, newNullBitsSize),
			duplicateOId, OBJECT_TYPE_COLUMNINFO);

	memset(toIndexSchema, 0, getAllocateSize(reserveIndexNum, newNullBitsSize));
	memcpy(toIndexSchema + NULL_STAT_OFFSET, getBaseAddr() + NULL_STAT_OFFSET, oldNullBitsSize);
	memcpy(toIndexSchema + NULL_STAT_OFFSET + newNullBitsSize, 
		getBaseAddr() + NULL_STAT_OFFSET + oldNullBitsSize, 
		INDEX_HEADER_SIZE + getIndexDataSize() * reserveIndexNum);

	BaseObject::finalize();

	copyReference(duplicateObj);
	setNullbitsSize(newNullBitsSize);

	return true;
}

bool IndexSchema::findDefaultIndexType(ColumnType columnType, MapType &type) {
	UTIL_STATIC_ASSERT(
			sizeof(DEFAULT_INDEX_TYPE) / sizeof(*DEFAULT_INDEX_TYPE) ==
			COLUMN_TYPE_PRIMITIVE_COUNT);

	int8_t columnTypeOrdinal;
	if (ValueProcessor::isArray(columnType) ||
			!ValueProcessor::findPrimitiveColumnTypeOrdinal(
					columnType, false, columnTypeOrdinal)) {
		type = MAP_TYPE_DEFAULT;
		return false;
	}

	type = DEFAULT_INDEX_TYPE[columnTypeOrdinal];
	return (type != MAP_TYPE_DEFAULT);
}

/*!
	@brief Allocate ShareValueList Object
*/
void ShareValueList::initialize(TransactionContext &txn, uint32_t allocateSize,
	AllocateStrategy &allocateStrategy, bool onMemory) {
	if (onMemory) {
		void *binary = txn.getDefaultAllocator().allocate(allocateSize);
		setBaseAddr(static_cast<uint8_t *>(binary));
	} else {
		BaseObject::allocate<uint8_t>(
			allocateSize, getBaseOId(), OBJECT_TYPE_CONTAINER_ID);
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
template BaseContainer::ExpirationInfo *ShareValueList::get(
	CONTAINER_META_TYPE type) const;
template uint8_t *ShareValueList::get(CONTAINER_META_TYPE type) const;
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

