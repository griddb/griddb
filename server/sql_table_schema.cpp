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
#include "sql_utils.h"
#include "collection.h"
#include "sql_compiler.h"
#include "nosql_container.h"
#include "sql_table_schema.h"
#include "transaction_service.h"
#include "nosql_utils.h"
#include "time_series.h"
#include "message_schema.h"
#include "picojson.h"
#include "json.h"
#include "sql_processor.h"

TableContainerInfo *TableSchemaInfo::setContainerInfo(
		size_t pos, NoSQLContainer &container) {

	util::AllocVector<ColumnId> *columnIdList = NULL;
	
	try {
		
		TableContainerInfo containerInfo(
				container.getGlobalAllocator());
		containerInfo.versionId_ = container.getSchemaVersionId();
		containerInfo.containerId_ = container.getContainerId();
		containerInfo.pId_ = container.getPartitionId();
		containerInfo.containerAttribute_ = container.getContainerAttribute();
		containerInfo.containerType_ = container.getContainerType();
		containerInfo.containerName_ = container.getName();
		containerInfo.pos_ = pos;
		
		containerInfoList_.push_back(containerInfo);

		if (pos == 0) {
			setColumnInfo(container);
			util::Vector<IndexInfo>
					&indexInfoLists = container.getCompositeIndex();
			for (size_t pos = 0;
					pos  < indexInfoLists.size(); pos++) {

				columnIdList = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
						util::AllocVector<ColumnId>(globalVarAlloc_);

				for (size_t columnPos = 0;
						columnPos < indexInfoLists[pos].columnIds_.size();
						columnPos++) {

					columnIdList->push_back(
							indexInfoLists[pos].columnIds_[columnPos]);
				}
				compositeColumnIdList_.push_back(columnIdList);
			}
		}
		hasRowKey_ = container.hasRowKey();

		if (indexInfoList_.empty()) {
				indexInfoList_.resize(container.getColumnSize(), 0);
		}
		if (indexInfoList_.size() < container.getIndexInfoList().size()) {
			indexInfoList_.resize(container.getIndexInfoList().size(), 0);
		}

		if (pos == 0) {
			for (size_t idx = 0;
					idx < container.getIndexInfoList().size(); idx++) {
				indexInfoList_[idx] = container.getIndexInfoList()[idx];
			}
		}

		TableIndexInfoList &currentIndexInfoList
				= containerInfoList_.back().indexInfoList_;
		currentIndexInfoList.resize(
				container.getColumnSize(), 0);
		
		for (size_t idx = 0;
				idx < container.getIndexInfoList().size(); idx++) {
			currentIndexInfoList[idx] = container.getIndexInfoList()[idx];
		}

		if (currentIndexInfoList.size() >= indexInfoList_.size()) {
			indexInfoList_.resize(
					currentIndexInfoList.size());
		}

		if (pos > 0) {
			for (size_t idx = 0;
					idx < container.getIndexInfoList().size(); idx++) {
				indexInfoList_[idx] &= currentIndexInfoList[idx];
			}
		}

		if (nullStatisticsList_.empty()) {
			NoSQLUtils::initializeBits(
					nullStatisticsList_, container.getColumnSize());
		}

		const util::Vector<uint8_t>
				&nullList = container.getHasNullValueList();
		if (nullList.size() > nullStatisticsList_.size()) {
			nullStatisticsList_.resize(nullList.size());
		}

		for (size_t i = 0;i < nullStatisticsList_.size(); i++) {
			if (nullList.size() > i) {
				nullStatisticsList_[i] |= nullList[i];
			}
		}
		return &containerInfoList_.back();
	}
	catch (std::exception &e) {
		
		if (columnIdList) {
			ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, columnIdList);
		}
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void TableSchemaInfo::setColumnInfo(NoSQLContainer &container) {

	uint16_t fixDataOffset = 0;
	uint16_t varDataPos = 0;
	if (nosqlColumnInfoList_ == NULL) {
		nosqlColumnInfoList_ = static_cast<ColumnInfo*>(
				globalVarAlloc_.allocate(
				sizeof(ColumnInfo) * container.getColumnSize()));
		memset(nosqlColumnInfoList_, 0,
				sizeof(ColumnInfo) * container.getColumnSize());
	}
	fixDataOffset = static_cast<uint16_t>(
			ValueProcessor::calcNullsByteSize(
			static_cast<uint32_t>(
					container.getColumnSize())));
	
	size_t pos = 0;
	for (pos = 0; pos < container.getColumnSize(); pos++) {
		if (NoSQLUtils::isVariableType(
				container.getColumnType(pos))) {
			fixDataOffset = static_cast<int16_t>(
					fixDataOffset + static_cast<uint16_t>(sizeof(OId)));

			break;
		}
	}

	for (pos = 0; pos < container.getColumnSize(); pos++) {

		TableColumnInfo info(globalVarAlloc_);
		info.name_ = container.getColumnName(pos);
		info.type_ = container.getColumnType(pos);
		info.option_ = container.getColumnOption(pos);
		info.tupleType_ = container.getTupleColumnType(pos);

		columnInfoList_.push_back(info);
		ColumnInfo &columnInfo = nosqlColumnInfoList_[pos];
		columnInfo.setColumnId(static_cast<uint16_t>(pos));
		columnInfo.setType(info.type_, false);

		if (NoSQLUtils::isVariableType(info.type_)) {
			columnInfo.setOffset(varDataPos);
			varDataPos++;
		}
		else {
			columnInfo.setOffset(fixDataOffset);
			fixDataOffset = static_cast<int16_t>(
					fixDataOffset + static_cast<int16_t>(
							NoSQLUtils::getFixedSize(info.type_)));
		}
	}
	columnSize_ = static_cast<uint32_t>(
			container.getColumnSize());
}

void TableSchemaInfo::setOptionList(
		util::XArray<uint8_t> &optionList) {

	assert(columnInfoList_.size() == optionList.size());
	if (columnInfoList_.size() != optionList.size()) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_DDL_INVALID_COLUMN, "");
	}

	for (size_t pos = 0;
			pos < columnInfoList_.size(); pos++) {

		columnInfoList_[pos].option_ = optionList[pos];
		
		TupleList::TupleColumnType currentType
				= NoSQLUtils::convertNoSQLTypeToTupleType(
						columnInfoList_[pos].type_);
		
		columnInfoList_[pos].tupleType_ 
			= NoSQLUtils::setColumnTypeNullable(
					currentType, !ColumnInfo::isNotNull(optionList[pos]));
	}
}

TableSchemaInfo::~TableSchemaInfo() {
	
	if (nosqlColumnInfoList_) {
		globalVarAlloc_.deallocate(nosqlColumnInfoList_);
		nosqlColumnInfoList_ = NULL;
	}
	
	for (size_t pos = 0;
			pos < compositeColumnIdList_.size(); pos++) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, compositeColumnIdList_[pos]);
	}
}

void TableSchemaInfo::setPrimaryKeyIndex(
		int32_t primaryKeyColumnId) {

	if (indexInfoList_.empty()) {
			indexInfoList_.resize(columnInfoList_.size(), 0);
	}

	if (primaryKeyColumnId == -1) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_INTERNAL, "");
	}

	if (columnInfoList_.size() <= primaryKeyColumnId
			|| indexInfoList_.size() <= primaryKeyColumnId) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_INTERNAL, "");
	}

	const int32_t flags
			= NoSQLUtils::mapTypeToSQLIndexFlags(
					MAP_TYPE_DEFAULT,
					columnInfoList_[primaryKeyColumnId].type_);

	for (size_t pos = 0;
			pos < containerInfoList_.size(); pos++) {

		if (pos == 0) {
			indexInfoList_[primaryKeyColumnId] = flags;
		}

		TableIndexInfoList &currentIndexInfoList
				= containerInfoList_[pos].indexInfoList_;
		currentIndexInfoList.resize(
				columnInfoList_.size(), 0);
		currentIndexInfoList[primaryKeyColumnId] = flags;
	}
}

TablePartitioningIndexInfoEntry *TablePartitioningIndexInfo::find(
		const NameWithCaseSensitivity &indexName,
		util::Vector<ColumnId> &columnIdList,
		MapType indexType) {

	TablePartitioningIndexInfoEntry *retEntry = NULL;
	bool isCaseSensitive = indexName.isCaseSensitive_;
	const util::String &normalizeSpecifiedIndexName =
			SQLUtils::normalizeName(
					alloc_, indexName.name_, isCaseSensitive);

	for (size_t pos = 0;
			pos < indexEntryList_.size(); pos++) {

		const util::String &normalizeTargetIndexName =
				SQLUtils::normalizeName(
						alloc_, 
						indexEntryList_[pos]->indexName_.c_str(),
						isCaseSensitive);

		if (!strcmp(normalizeSpecifiedIndexName.c_str(),
					normalizeTargetIndexName.c_str())) {

			if (isSame(pos, columnIdList, indexType)) {
				retEntry = indexEntryList_[pos];
				retEntry->pos_ = static_cast<int32_t>(pos);
				break;
			}
		}
	}
	return retEntry;
}

TablePartitioningIndexInfoEntry *TablePartitioningIndexInfo::check(
		util::Vector<ColumnId> &columnIdList, MapType indexType) {

	for (size_t pos = 0; pos < indexEntryList_.size(); pos++) {
		if (isSame(pos, columnIdList, indexType)) {
			return indexEntryList_[pos];
		}
	}
	return NULL;
}

TablePartitioningIndexInfoEntry *TablePartitioningIndexInfo::find(
		const NameWithCaseSensitivity &indexName) {
	
	TablePartitioningIndexInfoEntry *retEntry = NULL;
	bool isCaseSensitive = indexName.isCaseSensitive_;
	const util::String &normalizeSpecifiedIndexName =
			SQLUtils::normalizeName(
					alloc_,
					indexName.name_,
					isCaseSensitive);

	for (size_t pos = 0; pos < indexEntryList_.size(); pos++) {
		const util::String &normalizeTargetIndexName =
				SQLUtils::normalizeName(
						alloc_,
						indexEntryList_[pos]->indexName_.c_str(),
						isCaseSensitive);

		if (!strcmp(normalizeSpecifiedIndexName.c_str(),
				normalizeTargetIndexName.c_str())) {

			retEntry = indexEntryList_[pos];
			retEntry->pos_ = static_cast<int32_t>(pos);
			break;
		}
	}
	return retEntry;
}

TablePartitioningIndexInfoEntry *TablePartitioningIndexInfo::add(
		const NameWithCaseSensitivity &indexName,
		util::Vector<ColumnId> &columnIdList,
		MapType indexType) {

	util::String indexNameStr(
			indexName.name_, alloc_);

	TablePartitioningIndexInfoEntry *entry
			= ALLOC_NEW(alloc_)
					TablePartitioningIndexInfoEntry(
							alloc_,
							indexNameStr,
							columnIdList,
							indexType);

	indexEntryList_.push_back(entry);
	entry->pos_ = indexEntryList_.size();

	return entry;
}

void TablePartitioningIndexInfo::remove(size_t pos) {

	if (pos >= indexEntryList_.size()) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_TABLE_PARTITION_INTERNAL,
				"Target pos=" << pos 
				<< " is out of range, max=" << indexEntryList_.size());
	}

	indexEntryList_.erase(
			indexEntryList_.begin() + pos);
}

TableContainerInfo *TableSchemaInfo::getTableContainerInfo(
		uint32_t partitionNum, 
		const TupleValue *value1,
		const TupleValue *value2,
		NodeAffinityNumber &affinity,
		int64_t &baseValue,
		NodeAffinityNumber &baseAffinity) {

	affinity = partitionInfo_.getAffinityNumber(
			partitionNum,
			value1,
			value2,
			-1,
			baseValue,
			baseAffinity);

	switch (partitionInfo_.partitionType_) {
	
		case SyntaxTree::TABLE_PARTITION_TYPE_HASH: {

			if (affinity >= containerInfoList_.size()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_SQL_TABLE_PARTITION_INTERNAL,
						"Target pos=" << static_cast<int32_t>(affinity) 
						<< " is out of range, max="
						<< containerInfoList_.size());
			}
			return &containerInfoList_[
					static_cast<size_t>(affinity)];
		}
		break;

		case SyntaxTree::TABLE_PARTITION_TYPE_RANGE: 
		case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH: {

			AffinityContainerMap::iterator it = affinityMap_.find(affinity);
			if (it == affinityMap_.end()) {
				return NULL;
			}
			else {
				if (containerInfoList_.size() <= (*it).second) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_TABLE_PARTITION_INTERNAL, "");
				}
				return &containerInfoList_[(*it).second];
			}
		}
		break;
		default:  {
			if (containerInfoList_.size() == 0) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_TABLE_PARTITION_INTERNAL, "");
			}
			return &containerInfoList_[0];
		}
	}
}

void TargetContainerInfo::set(
		TableContainerInfo *containerInfo,
		NodeAffinityNumber affinity) {

	containerId_  = containerInfo->containerId_;
	pId_  = containerInfo->pId_;
	versionId_  = containerInfo->versionId_;
	affinity_ = affinity;
	pos_ = containerInfo->pos_;	
}

template<typename Alloc>
NodeAffinityNumber TablePartitioningInfo<Alloc>::getAffinityNumber(
		uint32_t partitionNum,
		const TupleValue *value1,
		const TupleValue *value2,
		int32_t position, 
		int64_t &baseValue,
		NodeAffinityNumber &baseAffinity) {

	NodeAffinityNumber affinity = UNDEF_NODE_AFFINITY_NUMBER;
	int64_t tmpValue;
	uint64_t shiftValue = 0;

	switch (partitionType_) {
		case SyntaxTree::TABLE_PARTITION_TYPE_HASH: {

			affinity = (SQLProcessor::ValueUtils::hashValue(*value1)
					% partitioningNum_) + 1;
			baseValue = affinity;
		}
		break;
		case SyntaxTree::TABLE_PARTITION_TYPE_RANGE: 
		case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH: {

			TupleList::TupleColumnType type = value1->getType();
			size_t size = TupleColumnTypeUtils::getFixedSize(type);
			
			switch (type) {
				
				case TupleList::TYPE_BYTE: {
					int8_t byteValue;
					memcpy(&byteValue, value1->fixedData(), size);
					tmpValue = byteValue;
				}
				break;
				
				case TupleList::TYPE_SHORT: {
					int16_t shortValue;
					memcpy(&shortValue, value1->fixedData(), size);
					tmpValue = shortValue;
				}
				break;
				
				case TupleList::TYPE_INTEGER: {
					int32_t intValue;
					memcpy(&intValue, value1->fixedData(), size);
					tmpValue = intValue;
				}
				break;
				
				case TupleList::TYPE_TIMESTAMP:
				case TupleList::TYPE_LONG: {
					int64_t longValue;
					memcpy(&longValue, value1->fixedData(), size);
					tmpValue = longValue;
				}
				break;
				
				default:
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_TABLE_PARTITION_INTERNAL, "");
			}
			baseValue = tmpValue;
			if (tmpValue < 0) {
				shiftValue = 1;
				tmpValue++;
			}

			tmpValue = tmpValue / intervalValue_;
			if (shiftValue  == 1) {
				tmpValue *= -1;
			}

			uint64_t rangeBase = ((tmpValue << 1) | shiftValue);
			if (partitionType_ == SyntaxTree::TABLE_PARTITION_TYPE_RANGE) {
				affinity = 
					((rangeBase / (partitionNum*2)) * (partitionNum * 2)
					+ condensedPartitionIdList_[
							static_cast<size_t>(tmpValue % (partitionNum))]
					+ shiftValue * partitionNum
					+ partitionNum);
					baseAffinity = affinity;
			}
			else {
				if (position == -1 && value2 == NULL) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH, "");
				}
				if (position == -1) {
					position = 
					(SQLProcessor::ValueUtils::hashValue(*value2) % partitioningNum_);
				}

				affinity = 
					(rangeBase / (partitionNum * 2)
					* partitionNum * 2 * partitioningNum_ 
					+ condensedPartitionIdList_[
							static_cast<size_t>(tmpValue % partitionNum)]
					+ shiftValue * partitionNum * partitioningNum_
					+ position 
					+ partitionNum);

				baseAffinity = 
					(rangeBase / (partitionNum * 2) * partitionNum * 2 * partitioningNum_  
					+ condensedPartitionIdList_[
							static_cast<size_t>(tmpValue % partitionNum)]
					+ shiftValue * partitionNum * partitioningNum_
					+ partitionNum);
			}
		}
	}
	return affinity;
}

template<typename Alloc>
int64_t TablePartitioningInfo<Alloc>::calcValueFromAffinity(
		uint32_t partitionNum, NodeAffinityNumber affinity) {

	int64_t decodeValue = -1;
	int32_t partitioningNum = 1;
	if (partitionType_ == SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH) {
		partitioningNum = partitioningNum_;
	}
	
	NodeAffinityNumber beforeAffinity = affinity;
	beforeAffinity -= partitionNum;
	int64_t start = 
			((beforeAffinity / (partitionNum * 2 * partitioningNum))
			* (partitionNum * 2 *partitioningNum));
	int64_t startPos = start  / (
			partitionNum * partitioningNum * 2) *partitionNum;
	bool isFind = false;

	for (size_t pos = 0; pos < condensedPartitionIdList_.size(); pos++) {

		if (beforeAffinity
				== static_cast<NodeAffinityNumber>(
						start + condensedPartitionIdList_[pos])) {
			
			decodeValue = (startPos + pos) * intervalValue_;
			isFind = true;
			break;
		}
		else if (beforeAffinity
				== static_cast<NodeAffinityNumber>(
						start + condensedPartitionIdList_[pos]
						+ partitionNum*partitioningNum)) {

			if ((-startPos - (static_cast<int64_t>(pos) + 1)) < INT64_MIN / intervalValue_) {
				decodeValue = INT64_MIN;
			}
			else {
				decodeValue = (-startPos - (pos + 1)) * intervalValue_;
			}
			
			isFind = true;
			break;
		}
	}
	return decodeValue;
}

template <typename Alloc>
void TablePartitioningInfo<Alloc>::findNeighbor(
		util::StackAllocator &alloc,
		int64_t value,
		util::Set<NodeAffinityNumber> &neighborAssignList) {

	util::Map<int64_t, NodeAffinityNumber> tmpMap(alloc);
	int32_t divideCount = 1;
	size_t pos;
	int64_t currentValue;
	
	if (intervalValue_ == 0) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_TABLE_PARTITION_INTERNAL, "");
	}
	
	if (value >= 0) {
		currentValue = (value / intervalValue_) * intervalValue_;
	}
	else {
		if (((value + 1) / intervalValue_ - 1)
				< (INT64_MIN /  intervalValue_)) {
			currentValue = INT64_MIN;
		}
		else {
		currentValue
				= ((value + 1) / intervalValue_ - 1) * intervalValue_;
		}
	}

	if (partitionType_
			== SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH) {
		divideCount = partitioningNum_;
	}
	for (pos = 1; pos < assignNumberList_.size(); pos++) {
		
		if (assignStatusList_[pos] == PARTITION_STATUS_DROP_START) continue;
		
		if (divideCount == 1
				|| (divideCount > 1 && (pos % divideCount) == 1)) {
			tmpMap.insert(std::make_pair(
					assignValueList_[pos], assignNumberList_[pos]));
		}
	}

	util::Map<int64_t, NodeAffinityNumber>::iterator
			lowItr = tmpMap.lower_bound(currentValue );

	if (lowItr == tmpMap.end() && !tmpMap.empty()) {
		lowItr = tmpMap.begin();
	}
	if (lowItr != tmpMap.end()) {

		if (lowItr != tmpMap.begin()) {
			lowItr--;
			typename PartitionAssignNumberMapList::iterator
					it = assignNumberMapList_.find((*lowItr).second);
		
			if (it != assignNumberMapList_.end()) {

				neighborAssignList.insert(
						assignNumberList_[(*it).second]);

				for (pos = 1;
						pos < static_cast<size_t>(divideCount); pos++) {
					neighborAssignList.insert(
							assignNumberList_[(*it).second] + pos);
				}
			}
			lowItr++;
		}
		lowItr++;

		if (lowItr != tmpMap.end()) {
			typename PartitionAssignNumberMapList::iterator
					it = assignNumberMapList_.find((*lowItr).second);
		
			if (it != assignNumberMapList_.end()) {
				
				neighborAssignList.insert(
						assignNumberList_[(*it).second]);

				for (pos = 1; pos < static_cast<size_t>(divideCount); pos++) {
					neighborAssignList.insert(
							assignNumberList_[(*it).second] + pos);
				}
			}
		}
	}
}


MapType NoSQLUtils::getAvailableIndex(
		DataStore *dataStore,
		const char *indexName,
		ColumnType targetColumnType,
		ContainerType targetContainerType,
		bool primaryCheck) {

	int32_t isArray = 0;

	try {
		if (strlen(indexName) > 0) {
			EmptyAllowedKey::validate(
				KeyConstraint::getUserKeyConstraint(
						dataStore->getValueLimitConfig().getLimitContainerNameSize()),
						indexName, static_cast<uint32_t>(strlen(indexName)),
						"indexName");
		}

		if (ValueProcessor::isArray(targetColumnType)) {
			isArray = 1;
			GS_THROW_USER_ERROR(
					GS_ERROR_CM_NOT_SUPPORTED, "not support this index type");
		}

		MapType realMapType = defaultIndexType[targetColumnType];
		if (realMapType < 0 || realMapType >= MAP_TYPE_NUM) {
			GS_THROW_USER_ERROR(
				GS_ERROR_CM_NOT_SUPPORTED, "not support this index type");
		}
		bool isSuport;

		switch (targetContainerType) {

		case COLLECTION_CONTAINER:
			isSuport = Collection::indexMapTable[targetColumnType]
					[realMapType];
			break;

		case TIME_SERIES_CONTAINER:
			isSuport = TimeSeries::indexMapTable[targetColumnType]
					[realMapType];
			break;

		default:
			GS_THROW_USER_ERROR(
					GS_ERROR_DS_CONTAINER_TYPE_UNKNOWN, "");
			break;
		}

		if (isSuport) {
			return realMapType;
		}
		else {
			GS_THROW_USER_ERROR(
					GS_ERROR_CM_NOT_SUPPORTED,
					"not support this index type");
		}
	}
	catch (std::exception &e) {
		if (primaryCheck) {
			GS_THROW_USER_ERROR(
					GS_ERROR_DS_DS_SCHEMA_INVALID,
					"Unsupported Rowkey Column type = "
					<< static_cast<int32_t>(targetColumnType)
					<< ", array=" <<  isArray);
		}
		else {
			GS_RETHROW_USER_ERROR(e, "");
		}
	}
}

void TableExpirationSchemaInfo::encode(
		EventByteOutStream &out) {

	if (isTableExpiration_) {
		if (startValue_ != -1 && limitValue_ != -1
				&& elapsedTime_ != -1) {

			out << static_cast<int32_t>(
					MessageSchema::PARTITION_EXPIRATION);
			size_t startPos = out.base().position();
			
			out << static_cast<int32_t>(0);
			size_t startDataPos = out.base().position();

			assert(startValue_ != -1);
			assert(limitValue_ != -1);
			out << startValue_;
			out << limitValue_;

			int64_t duration = MessageSchema::getTimestampDuration(
					elapsedTime_, timeUnit_);
			out << duration;
			
			size_t endDataPos = out.base().position();
			size_t encodeSize = endDataPos - startDataPos;
			out.base().position(startPos);
			out << static_cast<int32_t>(encodeSize);
			assert(encodeSize == 24);
			
			out.base().position(endDataPos);
		}
	}
	out << static_cast<int32_t>(MessageSchema::OPTION_END);
}

void TableSchemaInfo::checkWritableContainer() {

	for (size_t pos = 0; pos < getColumnSize(); pos++) {
		if (!SQLUtils::checkNoSQLTypeToTupleType(
				nosqlColumnInfoList_[pos].getColumnType())) {

			GS_THROW_USER_ERROR(GS_ERROR_SQL_COMPILE_UNSUPPORTED,
					"Target column type='" << ValueProcessor::getTypeName(
					nosqlColumnInfoList_[pos].getColumnType())
					<< "' is not writable in current version");
		}
	}
}
 void TableSchemaInfo::checkSubContainer(size_t nth) {

	 if (partitionInfo_.isPartitioning()) {

		 if (containerInfoList_.size() <= nth
					|| partitionInfo_.assignNumberList_.size() <= nth) {
			 GS_THROW_USER_ERROR(GS_ERROR_SQL_INTERNAL, "");
		 }

		if (containerInfoList_[nth].affinity_
				!= partitionInfo_.assignNumberList_[nth]) {

			util::NormalOStringStream strstrm;
			strstrm << tableName_ << "@"
					<< partitionInfo_.largeContainerId_ 
					<< "@" << partitionInfo_.assignNumberList_[nth];

			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_COMPILE_TABLE_NOT_FOUND,
					"Table '" << tableName_
					<< "', partition '" << strstrm.str().c_str()
					<< "' is not found");
		}
	}
}

 void TableSchemaInfo::setupSQLTableInfo(
		util::StackAllocator &alloc,
		SQLTableInfo &tableInfo,
		const char *tableName,
		DatabaseId dbId,
		bool withVersion,
		int32_t clusterPartitionCount,
		bool withoutCache,
		int64_t startTime,
		const char *viewSqlString) {

	tableInfo.tableName_ = tableName;
	tableInfo.idInfo_.dbId_ = dbId;
	tableInfo.hasRowKey_ = hasRowKey_;
	
	tableInfo.indexInfoList_.assign(
			indexInfoList_.begin(), indexInfoList_.end());
	tableInfo.nullsStats_.assign(
			nullStatisticsList_.begin(), nullStatisticsList_.end());
	tableInfo.sqlString_ = viewSqlString;
	
	if (containerAttr_ == CONTAINER_ATTR_VIEW) {
		tableInfo.isView_ = true;
	}
	
	for (size_t columnId = 0;
			columnId < columnInfoList_.size(); columnId++) {
	
		const TableColumnInfo &columnInfo =
				columnInfoList_[columnId];
		SQLTableInfo::SQLColumnInfo column(
				columnInfo.tupleType_,
				util::String(columnInfo.name_.c_str(), alloc));
		tableInfo.columnInfoList_.push_back(column);
		
		tableInfo.nosqlColumnInfoList_.push_back(columnInfo.type_);
		tableInfo.nosqlColumnOptionList_.push_back(columnInfo.option_);
	}

	int32_t partitioningCount
			= partitionInfo_.getCurrentPartitioningCount();
	if (partitioningCount > 0) {
	
		if (SyntaxTree::isRangePartitioningType(
				partitionInfo_.partitionType_)) {
			if (withoutCache) {
				tableInfo.isExpirable_ = true;
			}
		}

		if (withVersion) {
			tableInfo.idInfo_.partitioningVersionId_ =
					partitionInfo_.partitioningVersionId_;
		}
		else {
			tableInfo.idInfo_.partitioningVersionId_ =
					MAX_TABLE_PARTITIONING_VERSIONID;
		}
		SQLTableInfo::PartitioningInfo *partitioning =
				ALLOC_NEW(alloc) SQLTableInfo::PartitioningInfo(alloc);
		tableInfo.partitioning_ = partitioning;
		
		partitioning->partitioningColumnId_ =
				partitionInfo_.partitioningColumnId_;
		partitioning->subPartitioningColumnId_ =
				partitionInfo_.subPartitioningColumnId_;
		partitioning->partitioningType_ = partitionInfo_.partitionType_;
		int32_t startSubContainerId = 0;
		
		if (containerInfoList_.size() == 1) {
			startSubContainerId = 0;
		}
		else {
			startSubContainerId = 1;
		}
				
		SQLTableInfo::SubInfoList &subInfoList = partitioning->subInfoList_;
		
		for (size_t subContainerId = startSubContainerId;
				subContainerId < containerInfoList_.size();
				subContainerId++) {

			const TableContainerInfo
					&containerInfo = containerInfoList_[subContainerId];

			SQLTableInfo::SubInfo subInfo;
			subInfo.partitionId_ = containerInfo.pId_,
			subInfo.containerId_ = containerInfo.containerId_,
			subInfo.schemaVersionId_ = containerInfo.versionId_;
			
			subInfo.indexInfoList_
					= ALLOC_NEW(alloc) util::Vector<int32_t>(alloc);
			
			subInfo.indexInfoList_->assign(
					containerInfoList_[subContainerId].indexInfoList_.begin(),
					containerInfoList_[subContainerId].indexInfoList_.end());

			if (partitionInfo_.partitionType_ ==
					SyntaxTree::TABLE_PARTITION_TYPE_HASH) {
				
				const size_t idListSize
						= partitionInfo_.condensedPartitionIdList_.size();
				
				if (startSubContainerId == 0) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_COMPILE_TABLE_NOT_FOUND,
							"Table name '" << tableName << 
							"' is not found or already removed or under removing");
				}

				assert(startSubContainerId > 0);
				subInfo.nodeAffinity_ =
						(subContainerId - 1) / clusterPartitionCount *
								clusterPartitionCount +
								partitionInfo_.condensedPartitionIdList_[
								(subContainerId - 1) % idListSize] +
								clusterPartitionCount;

				if (containerInfo.affinity_ == UNDEF_NODE_AFFINITY_NUMBER ||
					containerInfo.affinity_
							== static_cast<NodeAffinityNumber>(
									subInfo.nodeAffinity_)) {
				}
				else {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH, "");
				}
			} 
			else {
				subInfo.nodeAffinity_ = containerInfo.affinity_;
			}

			assert(startSubContainerId == 0 ||
					subInfo.nodeAffinity_ >= clusterPartitionCount);
			subInfoList.push_back(subInfo);
		}

		partitioning->partitioningCount_
				= partitionInfo_.partitioningNum_;
		partitioning->clusterPartitionCount_
				= clusterPartitionCount;
		partitioning->intervalValue_
				= partitionInfo_.intervalValue_;
		
		partitioning->nodeAffinityList_.assign(
				partitionInfo_.condensedPartitionIdList_.begin(),
				partitionInfo_.condensedPartitionIdList_.end());

		if (partitionInfo_.partitionType_ !=
				SyntaxTree::TABLE_PARTITION_TYPE_HASH) {

			util::Vector<int64_t> availableList(alloc);
			util::Vector<int64_t> availableCountList(alloc);
			util::Vector<int64_t> unavailableList(alloc);
			util::Vector<int64_t> unavailableCountList(alloc);
			
			partitionInfo_.getSubIdList(
					alloc,
					availableList,
					availableCountList,
					unavailableList,
					unavailableCountList,
					startTime);

			util::Vector<int64_t>::iterator listIt
					= availableList.begin();
			util::Vector<int64_t>::iterator listEnd
					= availableList.end();
			util::Vector<int64_t>::iterator countIt
					= availableCountList.begin();
			util::Vector<int64_t>::iterator countEnd
					= availableCountList.end();
			
			while (listIt != listEnd && countIt != countEnd) {
				partitioning->availableList_.push_back(
						std::make_pair(*listIt, *countIt));
				++listIt;
				++countIt;
			}
		}
	}
	else {
		assert(containerInfoList_.size() > 0);
		const TableContainerInfo &containerInfo
				= containerInfoList_[0];

		tableInfo.idInfo_.partitionId_ = containerInfo.pId_;
		tableInfo.idInfo_.containerId_ = containerInfo.containerId_;
		tableInfo.idInfo_.schemaVersionId_ = containerInfo.versionId_;
	}

	for (size_t pos = 0;
			pos < compositeColumnIdList_.size(); pos++) {

		util::Vector<ColumnId> columnIdList(alloc);
		for (size_t columnPos = 0;
				columnPos < compositeColumnIdList_[pos]->size();
				columnPos++) {

			columnIdList.push_back(
					(*compositeColumnIdList_[pos])[columnPos]);
		}

		tableInfo.compositeIndexInfoList_.push_back(
				columnIdList);
	}
}

void TableSchemaInfo::copyPartitionInfo(
		TableSchemaInfo &info) {

	partitionInfo_.partitionType_ =
			info.partitionInfo_.partitionType_;
	partitionInfo_.partitioningNum_ = 
			info.partitionInfo_.partitioningNum_;
	partitionInfo_.partitionColumnName_ =
			info.partitionInfo_.partitionColumnName_;
	partitionInfo_.partitioningColumnId_ =
			info.partitionInfo_.partitioningColumnId_;
	partitionInfo_.partitionColumnType_ =
			info.partitionInfo_.partitionColumnType_;
	partitionInfo_.subPartitioningColumnName_ =
			info.partitionInfo_.subPartitioningColumnName_;
	partitionInfo_.subPartitioningColumnName_ =
			info.partitionInfo_.subPartitioningColumnName_;
	partitionInfo_.subPartitioningColumnId_ =
			info.partitionInfo_.subPartitioningColumnId_;
	partitionInfo_.intervalUnit_ =
			info.partitionInfo_.intervalUnit_;
	partitionInfo_.intervalValue_ =
			info.partitionInfo_.intervalValue_;
	partitionInfo_.largeContainerId_ =
			info.partitionInfo_.largeContainerId_;
	partitionInfo_.largeAffinityNumber_ =
			info.partitionInfo_.largeAffinityNumber_;
	partitionInfo_.condensedPartitionIdList_ =
			info.partitionInfo_.condensedPartitionIdList_;
	partitionInfo_.assignNumberList_ =
			info.partitionInfo_.assignNumberList_;
	partitionInfo_.assignStatusList_ =
			info.partitionInfo_.assignStatusList_;
	partitionInfo_.assignValueList_ =
			info.partitionInfo_.assignValueList_;
	partitionInfo_.assignNumberMapList_ =
			info.partitionInfo_.assignNumberMapList_;
	partitionInfo_.assignCountMax_ =
			info.partitionInfo_.assignCountMax_;
	partitionInfo_.assignCountMax_ =
			info.partitionInfo_.assignCountMax_;
	partitionInfo_.tableStatus_ =
			info.partitionInfo_.tableStatus_;
	partitionInfo_.dividePolicy_ =
			info.partitionInfo_.dividePolicy_;
	partitionInfo_.distributedfPolicy_ =
			info.partitionInfo_.distributedfPolicy_;
	partitionInfo_.partitioningVersionId_ =
			info.partitionInfo_.partitioningVersionId_;
	partitionInfo_.containerType_ =
			info.partitionInfo_.containerType_;
	partitionInfo_.currentStatus_ =
			info.partitionInfo_.currentStatus_;
	partitionInfo_.currentAffinityNumber_ =
			info.partitionInfo_.currentAffinityNumber_;
	partitionInfo_.currentIndexName_ =
			info.partitionInfo_.currentIndexName_;
	partitionInfo_.currentIndexType_ =
			info.partitionInfo_.currentIndexType_;
	partitionInfo_.currentIndexColumnId_ =
			info.partitionInfo_.currentIndexColumnId_;
	partitionInfo_.anyNameMatches_ = 
			info.partitionInfo_.anyNameMatches_;
	partitionInfo_.anyTypeMatches_ =
			info.partitionInfo_.anyTypeMatches_;
	partitionInfo_.currentIndexCaseSensitive_ =
			info.partitionInfo_.currentIndexCaseSensitive_;
	partitionInfo_.subPartitionColumnType_ =
			info.partitionInfo_.subPartitionColumnType_;
	partitionInfo_.activeContainerCount_ =
			info.partitionInfo_.activeContainerCount_;
	partitionInfo_.timeSeriesProperty_ =
			info.partitionInfo_.timeSeriesProperty_;
	partitionInfo_.subIdListCached_ =
			info.partitionInfo_.subIdListCached_;
	partitionInfo_.availableList_ =
			info.partitionInfo_.availableList_;
	partitionInfo_.availableCountList_ =
			info.partitionInfo_.availableCountList_;
	partitionInfo_.disAvailableList_ =
			info.partitionInfo_.disAvailableList_;
	partitionInfo_.disAvailableCountList_ =
			info.partitionInfo_.disAvailableCountList_;
	partitionInfo_.currentIntervalValue_ =
			info.partitionInfo_.currentIntervalValue_;
}

void TableSchemaInfo::copy(TableSchemaInfo &info) {

	columnInfoList_ = info.columnInfoList_;
	containerInfoList_ = info.containerInfoList_;
	founded_ = info.founded_;
	hasRowKey_ = info.hasRowKey_;
	containerType_ = info.containerType_;
	containerAttr_ = info.containerAttr_;
	columnSize_ = info.columnSize_;
	
	nosqlColumnInfoList_ = static_cast<ColumnInfo*>(
			globalVarAlloc_.allocate(sizeof(ColumnInfo) * columnSize_));
	
	memset(
			reinterpret_cast<void*>(nosqlColumnInfoList_),
			0,
			sizeof(ColumnInfo) * columnSize_);

	memcpy(
			reinterpret_cast<void *>(nosqlColumnInfoList_),
			reinterpret_cast<void*>(info.nosqlColumnInfoList_),
			sizeof(ColumnInfo) * columnSize_);

	indexInfoList_ = info.indexInfoList_;
	nullStatisticsList_ = info.nullStatisticsList_;
	tableName_ = info.tableName_;
	affinityMap_ = info.affinityMap_;
	lastExecutedTime_ = info.lastExecutedTime_;
	sqlString_ = info.sqlString_;
	compositeColumnIdList_ = info.compositeColumnIdList_;

	copyPartitionInfo(info);
}

template<typename Alloc>
TablePartitioningInfo<Alloc>::TablePartitioningInfo(Alloc &alloc) :
		alloc_(alloc),
		partitionType_(0),
		containerType_(COLLECTION_CONTAINER),
		partitioningNum_(0),
		partitionColumnName_(alloc),
		primaryColumnId_(-1),
		partitioningColumnId_(UNDEF_COLUMNID),
		partitionColumnType_(0),
		subPartitioningColumnName_(alloc),
		subPartitioningColumnId_(UNDEF_COLUMNID),
		subPartitionColumnType_(0),
		largeContainerId_(UNDEF_CONTAINERID),
		largeAffinityNumber_(UNDEF_NODE_AFFINITY_NUMBER),
		intervalValue_(0),
		intervalUnit_(UINT8_MAX),
		dividePolicy_(0),
		distributedfPolicy_(0),
		condensedPartitionIdList_(alloc),
		assignCountMax_(TABLE_PARTITIONING_MAX_ASSIGN_NUM),
		assignNumberList_(alloc), 
		assignStatusList_(alloc), 
		assignValueList_(alloc),
		assignNumberMapList_(alloc),
		activeContainerCount_(0),
		tableStatus_(0),
		partitioningVersionId_(0),
		currentStatus_(PARTITION_STATUS_NONE),
		currentAffinityNumber_(UNDEF_NODE_AFFINITY_NUMBER),
		currentIndexName_(alloc),
		currentIndexType_(MAP_TYPE_DEFAULT),
		currentIndexColumnId_(-1),
		currentIndexCaseSensitive_(false),
		anyNameMatches_(0),
		anyTypeMatches_(0),
		subIdListCached_(false),
		availableList_(alloc),
		availableCountList_(alloc),
		disAvailableList_(alloc),
		disAvailableCountList_(alloc),
		currentIntervalValue_(-1),
		opt_(NULL) {

		if (partitionType_ != SyntaxTree::TABLE_PARTITION_TYPE_HASH) {
			for (size_t i = 0;
					i < assignNumberList_.size(); i++) {
				assignNumberMapList_.insert(
						std::make_pair(assignNumberList_[i], i));
			}
		}
	}

template<typename Alloc>
void TablePartitioningInfo<Alloc>::init() {

	if (partitionType_
			!= SyntaxTree::TABLE_PARTITION_TYPE_HASH) {
		for (size_t i = 0;
				i < assignNumberList_.size(); i++) {

			assignNumberMapList_.insert(
					std::make_pair(assignNumberList_[i], i));
		}
	}
}

template<typename Alloc>
void TablePartitioningInfo<Alloc>::checkIntervalList(
		int64_t interval,
		int64_t currentValue,
		util::Vector<int64_t> &intervalList,
		util::Vector<int64_t> &intervalCountList,
		int64_t &prevInterval) {

	int64_t offset;
	if (currentValue < 0) {				
		offset = -CURRENT_INTERVAL_UNIT;
	}
	else {
		if (interval == MIN_PLUS_INTERVAL
				&& prevInterval == MAX_MINUS_INTERVAL) {
			offset = -CURRENT_BOUNDARY_UNIT;
		}
		else {
			offset = CURRENT_INTERVAL_UNIT;
		}
	}
	if (intervalList.size() == 0) {
		intervalList.push_back(interval);
		intervalCountList.push_back(1);
	}
	else {
		if (interval == prevInterval) {
			return;
		}
		if (interval == prevInterval + offset) {
			intervalCountList.back()++;
		}
		else {
			intervalList.push_back(interval);
			intervalCountList.push_back(1);
		}
	}
	prevInterval = interval;
}

template<typename Alloc>
void TablePartitioningInfo<Alloc>::setAssignInfo(
		util::StackAllocator &alloc,
		ClusterStatistics &stat,
		SyntaxTree::TablePartitionType partitioningType,
		PartitionId largeContainerPId) {

	util::Vector<PartitionId> condensedPartitionIdList(alloc);
	util::Vector<NodeAffinityNumber> assignNumberList(alloc);

	stat.generateStatistics(
			partitioningType,largeContainerPId,
			condensedPartitionIdList,
			assignNumberList);

	setCondensedPartitionIdList(condensedPartitionIdList);
	setSubContainerPartitionIdList(assignNumberList);
	activeContainerCount_ = assignNumberList.size();

	if (partitioningType
			!= SyntaxTree::TABLE_PARTITION_TYPE_HASH) {
		assignValueList_.push_back(0);
	}
}

template<typename Alloc>
void TablePartitioningInfo<Alloc>::checkExpireableInterval(
		int64_t currentTime,
		int64_t currentErasableTimestamp,
		int64_t duration,
		util::Vector<NodeAffinityNumber> &expiredAffinityNumbers,
		util::Vector<size_t> &expiredAffinityPos) {

	if (isTableExpiration()) {
	
		if (assignValueList_.size() < 2) return;

		for (size_t pos = 1;
				pos < assignValueList_.size(); pos++) {

			if (assignStatusList_[pos]
				!= PARTITION_STATUS_CREATE_END) continue;

			int64_t startTime, endTime;
			if (assignValueList_[pos] - 1
					> INT64_MAX - intervalValue_) {
				endTime = INT64_MAX;
			}
			else {
				endTime = assignValueList_[pos] + intervalValue_ - 1;
			}

			startTime = (
				(currentTime - duration) / intervalValue_
						* intervalValue_ - intervalValue_);
			int64_t erasableTime
					= BaseContainer::calcErasableTime(endTime, duration);
			
			if (erasableTime < currentErasableTimestamp
					&& currentTime > startTime) {
			
				expiredAffinityNumbers.push_back(
						assignNumberList_[pos]);
				expiredAffinityPos.push_back(pos);
			}
		}
	}
}

template<typename Alloc>
void TablePartitioningInfo<Alloc>::getSubIdList(
		util::StackAllocator &alloc,
		util::Vector<int64_t> &availableList,
		util::Vector<int64_t> &availableCountList, 
		util::Vector<int64_t> &disAvailableList,
		util::Vector<int64_t> &disAvailableCountList,
		int64_t currentTime) {
	if (assignNumberList_.size() == 1) {
		return;
	}
	bool calculated = false;
	
	if (subIdListCached_) {
		bool useCache = true;
	
		if (isTableExpiration()) {
	
			int64_t tmpCurrentIntervalValue = 
					((currentTime - MessageSchema::getTimestampDuration(
							timeSeriesProperty_.elapsedTime_,
							timeSeriesProperty_.timeUnit_)) / intervalValue_)
					* intervalValue_ - intervalValue_;
			
			calculated = true;
			
			if (tmpCurrentIntervalValue != currentIntervalValue_) {
				useCache = false;
				currentIntervalValue_ = tmpCurrentIntervalValue;
			}
		}
		if (useCache) {
			size_t pos;
			availableList.resize(availableList_.size());
			availableCountList.resize(
					availableCountList_.size());
			disAvailableList.resize(
					disAvailableList_.size());
			disAvailableCountList.resize(
					disAvailableCountList_.size());

			for (pos = 0; pos < availableList_.size(); pos++) {
				availableList[pos] = availableList_[pos];
				availableCountList[pos] = availableCountList_[pos];
			}

			for (pos = 0; pos < disAvailableList_.size(); pos++) {
				disAvailableList[pos] = disAvailableList_[pos];
				disAvailableCountList[pos] = disAvailableCountList_[pos];
			}

			return;
		}
	}

	util::Vector<std::pair<int64_t, LargeContainerStatusType>>
			tmpList(alloc);
	size_t pos;
	
	for (pos = 1; pos < assignNumberList_.size(); pos++) {
		tmpList.push_back(std::make_pair(
				assignValueList_[pos], assignStatusList_[pos]));
	}

	std::sort(tmpList.begin(), tmpList.end());
	
	int64_t prevAvailableValue = INT64_MAX;
	int64_t prevDisAvailableValue = INT64_MAX;
	int64_t prevAvailableInterval = INT64_MAX;
	int64_t prevDisAvailableInterval = INT64_MAX;
	
	if (isTableExpiration()) {
		if (!calculated) {
			currentIntervalValue_
					= ((currentTime - MessageSchema::getTimestampDuration(	
							timeSeriesProperty_.elapsedTime_,
							timeSeriesProperty_.timeUnit_)) / intervalValue_)
					* intervalValue_ - intervalValue_;
		}
	}
	for (size_t pos = 0; pos < tmpList.size(); pos++) {
	
		int64_t tmpValue = tmpList[pos].first;
		int64_t baseValue = tmpValue;
		uint64_t shiftValue;
		shiftValue = 1;
		
		if (baseValue < 0) {
			baseValue = -(baseValue / intervalValue_);
			shiftValue = 1;
		}
		else {
			baseValue = baseValue / intervalValue_;
			shiftValue = 0;
		}
		baseValue = (baseValue << 1) | shiftValue;
		
		checkMaxIntervalValue(tmpValue, baseValue);
		
		if (isTableExpiration()
				&& tmpList[pos].first <= currentIntervalValue_) {

			checkIntervalList(
					baseValue,
					tmpValue, 
					disAvailableList,
					disAvailableCountList,
					prevDisAvailableInterval);

			prevDisAvailableValue = tmpValue;
		}
		else if (
				tmpList[pos].second == PARTITION_STATUS_CREATE_END) {

			checkIntervalList(
					baseValue,
					tmpValue,
					availableList,
					availableCountList,
					prevAvailableInterval);
			
			prevAvailableValue = tmpValue;
		}

		else if (tmpList[pos].second == PARTITION_STATUS_DROP_START) {
			
			checkIntervalList(
					baseValue,
					tmpValue,
					disAvailableList,
					disAvailableCountList,
					prevDisAvailableInterval);
			
			prevDisAvailableValue = tmpValue;
		}
	}

	if (!subIdListCached_) {
	
		util::LockGuard<util::Mutex> guard(mutex_);
		if (subIdListCached_) return;

		availableList_.resize(availableList.size());
		availableCountList_.resize(
				availableCountList.size());
		disAvailableList_.resize(
				disAvailableList.size());
		disAvailableCountList_.resize(
				disAvailableCountList.size());
		assert(availableList.size()
				== availableCountList.size());
		assert(disAvailableList.size()
				== disAvailableCountList.size());
		
		size_t pos;
		for (pos = 0; pos < availableList.size(); pos++) {
			availableList_[pos] = availableList[pos];
			availableCountList_[pos] = availableCountList[pos];
		}
		
		for (pos = 0; pos < disAvailableList.size(); pos++) {
			disAvailableList_[pos] = disAvailableList[pos];
			disAvailableCountList_[pos] = disAvailableCountList[pos];
		}
		
		subIdListCached_ = true;
	}
}

template<typename Alloc>
void TablePartitioningInfo<Alloc>::setSubContainerPartitionIdList(
		util::Vector<NodeAffinityNumber> &assignNumberList) {
	
	if (static_cast<size_t>(assignCountMax_) < assignNumberList.size()) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_TABLE_PARTITION_INTERNAL,
						"Max partitioning assign count");
	}

	assignNumberList_.assign(assignNumberList.size(), 0);
	assignStatusList_.assign(assignNumberList.size(), 0);
	
	if (partitionType_ == SyntaxTree::TABLE_PARTITION_TYPE_HASH) {
	
		for (size_t i = 0; i < assignNumberList.size(); i++) {
			assignNumberList_[i] = assignNumberList[i];
			assignStatusList_[i] = PARTITION_STATUS_CREATE_END;
		}
	}
	else {

		for (size_t i = 0; i < assignNumberList.size(); i++) {
			assignNumberList_[i] = assignNumberList[i];
			assignStatusList_[i] = PARTITION_STATUS_CREATE_END;

			assignNumberMapList_.insert (
					std::make_pair(assignNumberList_[i], i));
		}
	}
}

template<typename Alloc>
size_t TablePartitioningInfo<Alloc>::findEntry(
		NodeAffinityNumber affinity) {

	typename PartitionAssignNumberMapList::iterator
			it = assignNumberMapList_.find(affinity);

	if (it == assignNumberMapList_.end()) {
		return SIZE_MAX;
	}
	else {
		return (*it).second;
	}
}

template<typename Alloc>
size_t TablePartitioningInfo<Alloc>::newEntry(
		NodeAffinityNumber affinity,
		LargeContainerStatusType status,
		int32_t partitioingNum,
		int64_t baseValue) {

	if (assignNumberList_.size() >
			static_cast<size_t>(TABLE_PARTITIONING_MAX_ASSIGN_ENTRY_NUM)) {
		
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_TABLE_PARTITION_MAX_ASSIGN_COUNT, "");
	}

	size_t pos, retPos = 0;
	for (pos = 0;
			pos < static_cast<size_t>(partitioingNum); pos++) {

		assignNumberList_.push_back(affinity + pos);
		assignStatusList_.push_back(status);
		retPos = assignNumberList_.size() - 1;
		
		assignNumberMapList_.insert(
				std::make_pair(affinity, retPos));
	}

	if (assignValueList_.size() == 0) {
		assignValueList_.push_back(0);
	}

	int64_t tmpValue;
	if (baseValue >= 0) {
		tmpValue = (baseValue / intervalValue_) * intervalValue_;
	}
	else {
		if (((baseValue + 1) / intervalValue_ - 1)
				< (INT64_MIN / intervalValue_)){
			tmpValue = INT64_MIN;
		}
		else {
			tmpValue = ((baseValue + 1) / intervalValue_ - 1)
					* intervalValue_;
		}
	}

	for (pos = 0; pos < static_cast<size_t>(
			partitioingNum); pos++) {

		assignValueList_.push_back(tmpValue);
	}

	return retPos;
}

template<typename Alloc>
void TablePartitioningInfo<Alloc>::checkTableExpirationSchema(
		TableExpirationSchemaInfo &info,
		NodeAffinityNumber affinity,
		uint32_t partitionNum) {

	if (isTableExpiration()
			&& assignNumberList_[0] != affinity) {
		
		info.isTableExpiration_ = true;
		info.elapsedTime_ =  timeSeriesProperty_.elapsedTime_;
		info.timeUnit_ =  timeSeriesProperty_.timeUnit_;
		info.startValue_ = calcValueFromAffinity(
				partitionNum, affinity);
		assert(info.startValue_ != -1);
		info.limitValue_ = intervalValue_;
	}
}

template<typename Alloc>
void TablePartitioningInfo<Alloc>::checkTableExpirationSchema(
		TableExpirationSchemaInfo &info, size_t pos) {

	if (isTableExpiration()
			&& pos != 0
			&& pos < assignValueList_.size()) {

		info.isTableExpiration_ = true;
		info.elapsedTime_ =  timeSeriesProperty_.elapsedTime_;
		info.timeUnit_ =  timeSeriesProperty_.timeUnit_;
		info.startValue_ = assignValueList_[pos];
		assert(info.startValue_ != -1);
		info.limitValue_ =  intervalValue_;
	}
}

template<typename Alloc>
bool TablePartitioningInfo<Alloc>::checkSchema(
		TablePartitioningInfo &target,
		util::XArray<uint8_t> &targetSchema,
		util::XArray<uint8_t> &currentSchema) {

	if (partitionType_ == target.partitionType_
			&& partitioningNum_ == target.partitioningNum_
			&& partitioningColumnId_ == target.partitioningColumnId_
			&& subPartitioningColumnId_ == target.subPartitioningColumnId_
			&& intervalUnit_ == target.intervalUnit_
			&& intervalValue_ == target.intervalValue_
			&& dividePolicy_ == target.dividePolicy_
			&& distributedfPolicy_ == target.distributedfPolicy_
			&& containerType_ == target.containerType_
			&& subPartitionColumnType_ == target.subPartitionColumnType_
			&& timeSeriesProperty_ == target.timeSeriesProperty_
			&& targetSchema.size() == currentSchema.size()
			&& !memcmp(
					targetSchema.data(),
					currentSchema.data(),
					targetSchema.size())) {

		return true;
	}
	else {
		return false;
	}
}

void TablePartitioningIndexInfo::getIndexInfoList(
		util::StackAllocator &alloc,
		util::Vector<IndexInfo> &indexInfoList) {
	
	for (size_t pos = 0;
			pos < indexEntryList_.size(); pos++) {
		
		IndexInfo indexInfo(
				alloc,
				indexEntryList_[pos]->indexName_, 
				indexEntryList_[pos]->columnIds_,
				indexEntryList_[pos]->indexType_);
		
		indexInfoList.push_back(indexInfo);
	}
}

bool TablePartitioningIndexInfo::isSame(
		size_t pos,
		util::Vector<ColumnId> &columnIdList,
		MapType indexType) {

	if (columnIdList.size()
			!= indexEntryList_[pos]->columnIds_.size()) {
		return false;
	}

	for (size_t columnId = 0;
			columnId < columnIdList.size(); columnId++) {

		if (columnIdList[columnId]
			!= indexEntryList_[pos]->columnIds_[columnId]) {
			return false;
		}
	}
	return (
			indexEntryList_[pos]->indexType_ == indexType);
}

template <typename Alloc>
void TablePartitioningInfo<Alloc>::checkMaxAssigned(
		TransactionContext &txn,
		BaseContainer *container,
		int32_t partitioningNum) {

	if (activeContainerCount_ + partitioningNum >
			TABLE_PARTITIONING_MAX_ASSIGN_NUM + 1) {

		const FullContainerKey containerKey =
				container->getContainerKey(txn);

		util::String containerName(txn.getDefaultAllocator());
		containerKey.toString(
				txn.getDefaultAllocator(), containerName);

		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_TABLE_PARTITION_MAX_ASSIGN_COUNT,
				"Number of table partitions exceeds maximum limit("
				<< TABLE_PARTITIONING_MAX_ASSIGN_NUM << ")");
	}
}

TablePartitioningInfo<SQLVariableSizeGlobalAllocator>
		&TableSchemaInfo::getTablePartitioningInfo() {
	return partitionInfo_;
}

template <typename Alloc>
TablePartitioningInfo<Alloc>::TimeSeriesProperty::
		TimeSeriesProperty() :
				timeUnit_(TIME_UNIT_DAY),
				dividedNum_(
						BaseContainer::EXPIRE_DIVIDE_DEFAULT_NUM),
				elapsedTime_(-1) {
}

template NodeAffinityNumber TablePartitioningInfo<
		util::StackAllocator>::getAffinityNumber(
		uint32_t partitionNum,
		const TupleValue *value1,
		const TupleValue *value2,
		int32_t position, 
		int64_t &baseValue,
		NodeAffinityNumber &baseAffinity);

template void TablePartitioningInfo<util::StackAllocator>::init();

template void TablePartitioningInfo<
		util::StackAllocator>::checkIntervalList(
				int64_t interval, int64_t currentValue,
				util::Vector<int64_t> &intervalList,
				util::Vector<int64_t> &intervalCountList,
				int64_t &prevInterval);

template void TablePartitioningInfo<
		util::StackAllocator>::checkExpireableInterval(
				int64_t currentTime,
				int64_t currentErasableTimestamp,
				int64_t duration,
				util::Vector<NodeAffinityNumber> &expiredAffinityNumbers,
				util::Vector<size_t> &expiredAffinityPos);

template void TablePartitioningInfo<
		util::StackAllocator>::getSubIdList(
				util::StackAllocator &alloc, util::Vector<int64_t> &availableList,
				util::Vector<int64_t> &availableCountList, 
				util::Vector<int64_t> &disAvailableList,
				util::Vector<int64_t> &disAvailableCountList,
				int64_t currentTime);

template void TablePartitioningInfo<
		util::StackAllocator>::setSubContainerPartitionIdList(
				util::Vector<NodeAffinityNumber> &assignNumberList);

template size_t TablePartitioningInfo<
		util::StackAllocator>::newEntry(
				NodeAffinityNumber affinity,
				LargeContainerStatusType status,
				int32_t partitioingNum,
				int64_t baseValue);

template size_t TablePartitioningInfo<
		SQLVariableSizeGlobalAllocator>::findEntry(
				NodeAffinityNumber affinity);

template size_t TablePartitioningInfo<
		util::StackAllocator>::findEntry(
				NodeAffinityNumber affinity);

template bool TablePartitioningInfo<
		util::StackAllocator>::checkSchema(
				TablePartitioningInfo &target,
				util::XArray<uint8_t> &targetSchema,
				util::XArray<uint8_t> &currentSchema);

template void TablePartitioningInfo<
		SQLVariableSizeGlobalAllocator>::checkTableExpirationSchema(
				TableExpirationSchemaInfo &info,
				NodeAffinityNumber affinity,
				uint32_t partitionNum);

template void TablePartitioningInfo<
		SQLVariableSizeGlobalAllocator>::checkTableExpirationSchema(
				TableExpirationSchemaInfo &info,
				size_t pos);

template void TablePartitioningInfo<
		util::StackAllocator>::checkTableExpirationSchema(
				TableExpirationSchemaInfo &info,
				NodeAffinityNumber affinity,
				uint32_t partitionNum);

template void TablePartitioningInfo<
		util::StackAllocator>::checkTableExpirationSchema(
				TableExpirationSchemaInfo &info,
				size_t pos);

template int64_t TablePartitioningInfo<
		util::StackAllocator>::calcValueFromAffinity(
				uint32_t partitionNum,
				NodeAffinityNumber affinity);

template TablePartitioningInfo<
		util::StackAllocator>::TablePartitioningInfo(
				util::StackAllocator&);	

template TablePartitioningInfo<
		SQLVariableSizeGlobalAllocator>::TablePartitioningInfo(
				SQLVariableSizeGlobalAllocator&);

template void TablePartitioningInfo<
		util::StackAllocator>::checkMaxAssigned(
		TransactionContext &txn, 
		BaseContainer *container,
		int32_t partitioningNum);

template void TablePartitioningInfo<
		util::StackAllocator>::findNeighbor(
				util::StackAllocator &alloc,
				int64_t value,
				util::Set<NodeAffinityNumber> &neighborAssignList);

template void TablePartitioningInfo<
		util::StackAllocator>::setAssignInfo(
				util::StackAllocator &alloc,
				ClusterStatistics &stat,
				SyntaxTree::TablePartitionType partitioningType,
				PartitionId largeContainerPId);