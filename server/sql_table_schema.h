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
#ifndef SQL_TABLE_SCHEMA_H_
#define SQL_TABLE_SCHEMA_H_

#include "container_key.h"
#include "schema.h"
#include "sql_parser.h"
#include "sql_cluster_stat.h"

class NoSQLContainer;
class PartitionTable;

typedef util::AllocVector<uint8_t> NullStatisticsList;
typedef util::AllocVector<int32_t> TableIndexInfoList;

struct TableExpirationSchemaInfo {

	TableExpirationSchemaInfo() :
			elapsedTime_(-1),
			timeUnit_(UINT8_MAX), 
			startValue_(-1),
			limitValue_(-1),
			isTableExpiration_(false) {}

	void encode(EventByteOutStream &out);

	bool isTableExpiration() {
		return isTableExpiration_;
	}

	int32_t elapsedTime_;
	TimeUnit timeUnit_;
	int64_t startValue_;
	int64_t limitValue_;
	bool isTableExpiration_;
};

struct TableContainerInfo;

struct TargetContainerInfo {

	TargetContainerInfo() :
			pId_(0),
			versionId_(UNDEF_SCHEMAVERSIONID),
			containerId_(UNDEF_CONTAINERID),
			affinity_(UNDEF_NODE_AFFINITY_NUMBER),
			pos_(0) {
	}

	void set(
			TableContainerInfo *info, NodeAffinityNumber affinity);

	PartitionId pId_;
	SchemaVersionId versionId_;
	ContainerId containerId_;
	NodeAffinityNumber affinity_;
	size_t pos_;
};

template<typename Alloc>
struct TablePartitioningOptionInfo {

	typedef util::BasicString<
			char8_t, std::char_traits<char8_t>,
			util::StdAllocator<char8_t, Alloc> > PString;

	typedef std::vector<int64_t, util::StdAllocator<
		int64_t, Alloc> > PartitionColumnNameList;

	typedef std::vector<ColumnId, util::StdAllocator<
		ColumnId, Alloc> > PartitionColumnIdList;

	typedef std::vector<int32_t, util::StdAllocator<
		int32_t, Alloc> > PartitionColumnTypeList;

	typedef std::vector<int32_t, util::StdAllocator<
		int32_t, Alloc> > OptionKeyNameIdList;

	typedef std::vector<PString, util::StdAllocator<
		PString, Alloc> > OptionKeyValueList;

	typedef std::vector<int32_t, util::StdAllocator<
		int32_t, Alloc> > OptionKeyPositionList;

	TablePartitioningOptionInfo(Alloc &alloc) :
			alloc_(alloc),
			partitionColumnNameList_(alloc),
			partitionColumnIdList_(alloc),
			subPartitionColumnNameList_(alloc),
			subPartitionColumnIdList_(alloc),
			keyNameIdList_(alloc), keyValueList_(alloc), keyPositionList_(alloc),
			primaryColumnIdList_(alloc), primaryColumnNameList_(alloc),
			primaryColumnTypeList_(alloc) {}	
	
	Alloc &alloc_;
	PartitionColumnNameList partitionColumnNameList_;
	PartitionColumnIdList partitionColumnIdList_;
	PartitionColumnNameList subPartitionColumnNameList_;
	PartitionColumnIdList subPartitionColumnIdList_;

	OptionKeyNameIdList keyNameIdList_;
	OptionKeyValueList keyValueList_;
	OptionKeyPositionList keyPositionList_;
	PartitionColumnNameList primaryColumnNameList_;
	PartitionColumnIdList primaryColumnIdList_;
	PartitionColumnTypeList primaryColumnTypeList_;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(partitionColumnNameList_,
			partitionColumnIdList_, subPartitionColumnNameList_,
			subPartitionColumnIdList_, keyNameIdList_, keyValueList_,
			keyPositionList_, primaryColumnNameList_,
			primaryColumnIdList_, primaryColumnTypeList_);
};


struct TableContainerInfo {

	TableContainerInfo(
			SQLVariableSizeGlobalAllocator &globalVarAlloc) :
					globalVarAlloc_(globalVarAlloc),
					containerName_(globalVarAlloc),
					containerId_(0),
					versionId_(0),
					containerType_(COLLECTION_CONTAINER),
					containerAttribute_(CONTAINER_ATTR_SINGLE),
					isRowKey_(false),
					pId_(0),
					hasNullValue_(0),
					nullStatisticsList_(globalVarAlloc_),
					indexInfoList_(globalVarAlloc_),
					pos_(0),
					affinity_(UNDEF_NODE_AFFINITY_NUMBER) {}

	TableContainerInfo& operator=(
			const TableContainerInfo &right) {

		containerName_ = right.containerName_;
		containerId_ = right.containerId_;
		versionId_ = right.versionId_;
		containerType_ = right.containerType_;
		containerAttribute_ = right.containerAttribute_;
		isRowKey_ = right.isRowKey_;
		pId_ = right.pId_;
		hasNullValue_ = right.hasNullValue_;
		nullStatisticsList_ = right.nullStatisticsList_;
		indexInfoList_ = right.indexInfoList_;
		pos_ = right.pos_;
		affinity_ = right.affinity_;

		return *this;
	}

	SQLVariableSizeGlobalAllocator &globalVarAlloc_;
	SQLString containerName_;
	ContainerId containerId_;
	uint32_t versionId_;
	uint8_t containerType_;
	uint8_t containerAttribute_;
	bool isRowKey_;
	PartitionId pId_;
	uint8_t hasNullValue_;
	NullStatisticsList nullStatisticsList_;
	TableIndexInfoList indexInfoList_;
	size_t pos_;
	NodeAffinityNumber affinity_;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(
			containerName_,
			containerId_,
			versionId_,
			containerType_,
			containerAttribute_,
			isRowKey_,
			pId_,
			hasNullValue_,
			nullStatisticsList_);
};

static const uint32_t OLD_LARGE_CONTAINER_VERSION_ID =  0;
static const uint32_t LARGE_CONTAINER_VERSION_ID_1 =  1;

struct VersionInfo {

	VersionInfo() : version_(LARGE_CONTAINER_VERSION_ID_1) {}

	uint32_t version_;
	UTIL_OBJECT_CODER_MEMBERS(version_);
};

class ClusterStatistics;

template<typename Alloc>
struct TablePartitioningInfo {

	typedef SyntaxTree::CreateTableOption CreateTableOption;
	typedef SyntaxTree::CreateIndexOption CreateIndexOption;

	typedef util::BasicString<
			char8_t, std::char_traits<char8_t>,
			util::StdAllocator<char8_t, Alloc> > PString;

	typedef util::AllocVector<NodeAffinityNumber>
			PartitionAssignNumberList;
	
	typedef util::AllocVector<int64_t>
			PartitionAssignValueList;

	typedef util::AllocVector<LargeContainerStatusType>
			PartitionAssignStatusList;

	typedef util::AllocVector<PartitionId>
			CondensedPartitionIdList;

	typedef util::AllocVector<int64_t>
			PartitionAssignVersionList;

	typedef util::AllocMap<NodeAffinityNumber, size_t>
			PartitionAssignNumberMapList; 

	static const int32_t CURRENT_INTERVAL_UNIT = 2;
	static const int32_t CURRENT_BOUNDARY_UNIT = (CURRENT_INTERVAL_UNIT + 1);
	static const int64_t MAX_MINUS_INTERVAL = 3;
	static const int64_t MIN_PLUS_INTERVAL = 0;

	TablePartitioningInfo(Alloc &alloc);

	void init();

	bool isPartitioning() {
		return (partitionType_
				!= SyntaxTree::TABLE_PARTITION_TYPE_UNDEF);
	}

	void setAssignInfo(
			util::StackAllocator &alloc,
			ClusterStatistics &stat,
			SyntaxTree::TablePartitionType type,
			PartitionId largeContainerPId);

	Alloc &getAllocator() {
		return alloc_;
	}

	void checkMaxIntervalValue(
			int64_t tmpValue, int64_t &interval) {

		if (tmpValue == INT64_MIN) {
			interval = interval + 2;
		}	
	}

	void checkIntervalList(
			int64_t interval,
			int64_t currentValue,
			util::Vector<int64_t> &intervalList,
			util::Vector<int64_t> &intervalCountList,
			int64_t &prevInterval);

	void checkExpireableInterval(int64_t currentTime,
			int64_t currentErasableTimestamp, int64_t duration,
			util::Vector<NodeAffinityNumber> &expiredAffinityNumbers,
			util::Vector<size_t> &expiredAffinityPos);

	void getSubIdList(util::StackAllocator &alloc,
			util::Vector<int64_t> &availableList,
			util::Vector<int64_t> &availableCountList, 
			util::Vector<int64_t> &disAvailableList,
			util::Vector<int64_t> &disAvailableCountList,
			int64_t currentTime);

	void findNeighbor(
			util::StackAllocator &alloc,
			int64_t value,
			util::Set<NodeAffinityNumber> &neighborAssignList);

	void setSubContainerPartitionIdList(
			util::Vector<NodeAffinityNumber> &assignNumberList);

	size_t findEntry(NodeAffinityNumber affinity);

	size_t newEntry(
			NodeAffinityNumber affinity,
			LargeContainerStatusType status,
			int32_t partitioingNum, int64_t baseValue);

	TablePartitioningVersionId incrementTablePartitioningVersionId() {
		partitioningVersionId_++;
		return partitioningVersionId_;
	}

	NodeAffinityNumber getAffinityNumber(
			uint32_t partitionNum,
			const TupleValue *value1,
			const TupleValue *value2,
			int32_t position,
			int64_t &baseValue,
			NodeAffinityNumber &baseAffinity);

	int64_t calcValueFromAffinity(
			uint32_t partitionNum,
			NodeAffinityNumber affinity);

	void setCondensedPartitionIdList(
			util::Vector<PartitionId> &pIdList) {
		
		condensedPartitionIdList_.assign(
				pIdList.size(), 0);
		
		for (size_t i = 0; i < pIdList.size(); i++) {
			condensedPartitionIdList_[i] = pIdList[i];
		}
	}

	int32_t getCurrentPartitioningCount() {
		return static_cast<int32_t>(
				assignNumberList_.size());
	}

	size_t getActiveContainerCount() {
		return activeContainerCount_;
	}

	struct TimeSeriesProperty;

	bool isTableExpiration() {

		return (
				timeSeriesProperty_.elapsedTime_ != -1
						&& tableStatus_ == static_cast<uint8_t>(
							EXPIRATION_TYPE_PARTITION));
	}

	bool isRowExpiration() {

		return (
				timeSeriesProperty_.elapsedTime_ != -1
						&& tableStatus_ == static_cast<uint8_t>(
								EXPIRATION_TYPE_ROW));
	}

	bool isExpiration() {
		return (isTableExpiration() || isRowExpiration());
	}

	void checkTableExpirationSchema(
			TableExpirationSchemaInfo &info,
			NodeAffinityNumber affinity,
			uint32_t partitionNum);

	void checkTableExpirationSchema(
			TableExpirationSchemaInfo &info,
			size_t pos);

	bool checkSchema(
			TablePartitioningInfo &target,
			util::XArray<uint8_t> &targetSchema,
			util::XArray<uint8_t> &currentSchema);

	void checkMaxAssigned(
			TransactionContext &txn,
			BaseContainer *container,
			int32_t partitioningNum);

	bool checkInterval(int32_t pos) {
	
		if (partitionType_
				== SyntaxTree::TABLE_PARTITION_TYPE_HASH) {
			return false;
		}

		if (pos == 1) {
			return false;
		}

		return true;
	}

	struct TimeSeriesProperty {

		TimeSeriesProperty();

		TimeSeriesProperty& operator=(
				const TimeSeriesProperty &right) {

			timeUnit_ = right.timeUnit_;
			dividedNum_ = right.dividedNum_;
			elapsedTime_ = right.elapsedTime_;

			return *this;
		}

		bool operator==(
				const TimeSeriesProperty &op) const {

			return (
					timeUnit_ == op.timeUnit_ 
							&& dividedNum_ == op.dividedNum_
							&& elapsedTime_ == op.elapsedTime_);
		}

		TimeUnit timeUnit_;
		uint16_t dividedNum_;
		int32_t elapsedTime_;

		UTIL_OBJECT_CODER_MEMBERS(
				timeUnit_, dividedNum_, elapsedTime_);
	};

	std::string getIntervalValue() {

		util::NormalOStringStream oss;
		if (SyntaxTree::isRangePartitioningType(
				partitionType_)) {

			if (partitionColumnType_
					== TupleList::TYPE_TIMESTAMP) {
				int64_t orgValue
						= intervalValue_ / 24 / 3600 / 1000;
				oss << orgValue;
			}
			else {
				oss << intervalValue_;
			}
		}

		return oss.str().c_str();
	}

	bool checkHashPartitioning(int32_t pos) {

		switch (
				static_cast<SyntaxTree::TablePartitionType>(
						partitionType_)) {

			case SyntaxTree::TABLE_PARTITION_TYPE_HASH:
				return (pos == 0);

			case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH:
				return (pos == 1);
			
			default:
				return false;
		}
	}

	int32_t getHashPartitioningCount() {

		switch (
				static_cast<SyntaxTree::TablePartitionType>(
							partitionType_)) {

			case SyntaxTree::TABLE_PARTITION_TYPE_HASH:
				return partitioningNum_;

			case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH:
				return partitioningNum_;

			default:
				return -1;
		}
	}

	util::Mutex mutex_;

	Alloc &alloc_;
	uint8_t partitionType_;
	uint8_t containerType_;
	PartitionId partitioningNum_;
	int32_t primaryColumnId_;
	PString partitionColumnName_;
	ColumnId partitioningColumnId_;
	uint16_t partitionColumnType_;
	PString subPartitioningColumnName_;
	ColumnId subPartitioningColumnId_;
	uint16_t subPartitionColumnType_;
	ContainerId largeContainerId_;
	NodeAffinityNumber largeAffinityNumber_;
	int64_t intervalValue_;
	uint8_t intervalUnit_;
	int32_t dividePolicy_;
	int32_t distributedfPolicy_;
	CondensedPartitionIdList
			condensedPartitionIdList_;
	int64_t assignCountMax_;
	PartitionAssignNumberList assignNumberList_;
	PartitionAssignStatusList assignStatusList_;
	PartitionAssignValueList assignValueList_;
	PartitionAssignNumberMapList
			assignNumberMapList_;
	int64_t activeContainerCount_;
	int8_t tableStatus_;

	TablePartitioningVersionId partitioningVersionId_;
	LargeContainerStatusType currentStatus_;
	NodeAffinityNumber currentAffinityNumber_;
	PString currentIndexName_;
	int8_t currentIndexType_;
	int32_t currentIndexColumnId_;
	bool currentIndexCaseSensitive_;
	uint8_t anyNameMatches_;
	uint8_t anyTypeMatches_;
	util::Atomic<bool> subIdListCached_;
	PartitionAssignValueList availableList_;
	PartitionAssignValueList availableCountList_;
	PartitionAssignValueList disAvailableList_;
	PartitionAssignValueList disAvailableCountList_;
	int64_t currentIntervalValue_;
	TimeSeriesProperty timeSeriesProperty_;
	TablePartitioningOptionInfo<Alloc> *opt_;


	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	
	UTIL_OBJECT_CODER_MEMBERS(
			partitionType_,
			partitionColumnName_,
			partitioningNum_,
			partitioningColumnId_,
			largeContainerId_,
			largeAffinityNumber_,
			condensedPartitionIdList_,
			intervalUnit_,
			intervalValue_,
			subPartitioningColumnName_,
			subPartitioningColumnId_,
			assignNumberList_,
			assignStatusList_,
			assignValueList_,
			assignCountMax_,
			tableStatus_,
			partitioningVersionId_, 
			primaryColumnId_,
			containerType_,
			currentStatus_,
			currentAffinityNumber_,
			partitionColumnType_,
			subPartitionColumnType_,
			activeContainerCount_,
			currentIndexName_,
			currentIndexType_,
			currentIndexColumnId_,
			currentIndexCaseSensitive_,
			anyNameMatches_,
			anyTypeMatches_,
			timeSeriesProperty_,
			opt_
		);
		
};

struct TablePartitioningIndexInfoEntry {

	TablePartitioningIndexInfoEntry(
			util::StackAllocator &alloc) :	
					alloc_(alloc),
					indexName_(alloc),
					columnIds_(alloc),
					indexType_(-1),
					status_(PARTITION_STATUS_CREATE_START),
					pos_(0) {}

	TablePartitioningIndexInfoEntry(
			util::StackAllocator &alloc,
			util::String &indexName,
			util::Vector<uint32_t> &columnIds,
			MapType indexType) :	
					alloc_(alloc),
					indexName_(indexName),
					columnIds_(columnIds),
					indexType_(indexType),
					status_(PARTITION_STATUS_CREATE_START),
					pos_(0) {}

	util::StackAllocator &alloc_;
	util::String indexName_;
	util::Vector<uint32_t> columnIds_;
	MapType indexType_;
	LargeContainerStatusType status_;
	size_t pos_;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(
			indexName_, columnIds_, indexType_, status_);
};

struct TablePartitioningIndexInfo {

	TablePartitioningIndexInfo(
			util::StackAllocator &alloc) :
					alloc_(alloc),
					indexEntryList_(alloc),
					indexEntryMap_(alloc),
					partitioningVersionId_(0) {}
	
	void getIndexInfoList(util::StackAllocator &alloc,
			util::Vector<IndexInfo> &indexInfoList);

	bool isEmpty() {
		return (indexEntryList_.size() == 0);
	}

	bool isSame(
			size_t pos,
			util::Vector<ColumnId> &columnIdList,
			MapType indexType);

	TablePartitioningIndexInfoEntry *find(
			const NameWithCaseSensitivity &indexName,
			util::Vector<ColumnId> &columnIdList,
			MapType indexType);

	TablePartitioningIndexInfoEntry *check(
			util::Vector<ColumnId> &columnIdList,
			MapType indexType);

	TablePartitioningIndexInfoEntry *find(
			const NameWithCaseSensitivity &indexName);

	TablePartitioningIndexInfoEntry *add(
			const NameWithCaseSensitivity &indexName,
			util::Vector<ColumnId> &columnIdList,
			MapType indexType);

	util::StackAllocator &getAllocator() {
		return alloc_;
	}

	void remove(size_t pos);

	util::StackAllocator &alloc_;
	util::Vector<TablePartitioningIndexInfoEntry*> indexEntryList_;
	util::Map<util::String,
			TablePartitioningIndexInfoEntry*> indexEntryMap_;
	TablePartitioningVersionId partitioningVersionId_;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(
			indexEntryList_, partitioningVersionId_);
};

struct ViewInfo {

	ViewInfo(util::StackAllocator &alloc) :
			alloc_(alloc),
			sqlString_(alloc) {}

	util::StackAllocator &getAllocator() {
		return alloc_;
	}

	util::StackAllocator &alloc_;
	util::String sqlString_; 
	
	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(sqlString_);
};

struct TableColumnOptionInfo {
	
	TableColumnOptionInfo() : hasNull_(0) {}
	
	uint8_t hasNull_;
	UTIL_OBJECT_CODER_MEMBERS(hasNull_);
};

struct TableColumnIndexInfoEntry {

	TableColumnIndexInfoEntry(
			SQLVariableSizeGlobalAllocator &globalVarAlloc) :
					globalVarAlloc_(globalVarAlloc),
					indexName_(globalVarAlloc),
					indexType_(0) {}

	TableColumnIndexInfoEntry& operator=(
				const TableColumnIndexInfoEntry &right) {

		indexName_ = right.indexName_;
		indexType_ = right.indexType_;
		return *this;
	}

	SQLVariableSizeGlobalAllocator &globalVarAlloc_;
	SQLString indexName_;
	MapType indexType_;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(indexName_, indexType_);
};

struct TableColumnInfo {

	typedef util::AllocVector<TableColumnIndexInfoEntry>
			TableColumnIndexInfoList;

	TableColumnInfo(
			SQLVariableSizeGlobalAllocator &globalVarAlloc) :
					globalVarAlloc_(globalVarAlloc), 
					name_(globalVarAlloc),
					type_(0),
					option_(0),
					tupleType_(TupleList::TYPE_NULL),
					indexInfo_(globalVarAlloc_) {}

	TableColumnInfo& operator=(
			const TableColumnInfo &right) {

		name_ = right.name_;
		type_ = right.type_;
		option_ = right.option_;
		tupleType_ = right.tupleType_;
		columnOption_ = right.columnOption_;
		indexInfo_ = right.indexInfo_;

		return *this;
	}

	SQLVariableSizeGlobalAllocator &globalVarAlloc_;
	SQLString name_;
	uint8_t type_;
	uint8_t option_;
	TupleList::TupleColumnType tupleType_;
	TableColumnOptionInfo columnOption_;
	TableColumnIndexInfoList indexInfo_;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(
			name_,
			type_,
			option_,
			columnOption_,
			indexInfo_,
			tupleType_);
};

struct TableSchemaInfo {

	typedef util::AllocVector<TableColumnInfo>
			TableColumnInfoList;

	typedef util::AllocVector<TableContainerInfo>
			TableContainerInfoList;

	typedef util::AllocMap<
			NodeAffinityNumber, size_t>
					AffinityContainerMap;

	TableSchemaInfo(
			SQLVariableSizeGlobalAllocator &globalVarAlloc) :
					globalVarAlloc_(globalVarAlloc),
					partitionInfo_(globalVarAlloc),
					columnInfoList_(globalVarAlloc_),
					containerInfoList_(globalVarAlloc_),
					refCount_(1),
					founded_(false),
					hasRowKey_(false),
					containerType_(COLLECTION_CONTAINER),
					containerAttr_(CONTAINER_ATTR_SINGLE),
					nosqlColumnInfoList_(NULL),
					indexInfoList_(globalVarAlloc_),
					nullStatisticsList_(globalVarAlloc_),
					columnSize_(0),
					tableName_(globalVarAlloc_),
					affinityMap_(globalVarAlloc_),
					lastExecutedTime_(0),
					disableCache_(false),
					sqlString_(globalVarAlloc_),
					compositeColumnIdList_(globalVarAlloc_) {}

	~TableSchemaInfo();

	TablePartitioningInfo<SQLVariableSizeGlobalAllocator>
			&getTablePartitioningInfo();

	void copy(TableSchemaInfo &info);

	void copyPartitionInfo(TableSchemaInfo &info);

	void checkSubContainer(size_t nth);
	
	void setupSQLTableInfo(
			util::StackAllocator &alloc, 
			SQLTableInfo &tableInfo,
			const char *tableName,
			DatabaseId dbId,
			bool withVersion,
			int32_t clusterPartitionCount,
			bool withoutCache,
			int64_t startTime,
			const char *viewSqlString);

	bool isTimeSeriesContainer() {
		return (containerInfoList_.size() > 0
				&& containerInfoList_[0].containerType_
						== TIME_SERIES_CONTAINER);
	}

	void checkWritableContainer();

	TableContainerInfo *setContainerInfo(
			size_t pos, NoSQLContainer &container);

	void setPrimaryKeyIndex(int32_t primaryKeyCokumnId);

	TableContainerInfo *getTableContainerInfo(
			uint32_t partitionNum,
			const TupleValue *value1,
			const TupleValue *value2,
			NodeAffinityNumber &affinity,
			int64_t &baseValue,
			NodeAffinityNumber &baseAffinity);

	void setColumnInfo(NoSQLContainer &container);

	void setOptionList(util::XArray<uint8_t> &optionList);

	uint32_t getColumnSize() {
		return columnSize_;
	}

	const char *getSQLString() {
		return sqlString_.c_str();
	}

	SQLVariableSizeGlobalAllocator &globalVarAlloc_;
	TablePartitioningInfo<SQLVariableSizeGlobalAllocator> partitionInfo_;
	TableColumnInfoList columnInfoList_;
	TableContainerInfoList containerInfoList_;
	int64_t refCount_;
	bool founded_;
	bool hasRowKey_;
	ContainerType containerType_;
	ContainerAttribute containerAttr_;
	ColumnInfo *nosqlColumnInfoList_;
	TableIndexInfoList indexInfoList_;

	NullStatisticsList nullStatisticsList_;
	uint32_t columnSize_;

	SQLString tableName_;
	AffinityContainerMap affinityMap_;
	EventMonotonicTime lastExecutedTime_;
	bool disableCache_;
	SQLString sqlString_;
	util::AllocVector<util::AllocVector<ColumnId> *>
			compositeColumnIdList_;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(
			columnInfoList_,
			containerInfoList_,
			partitionInfo_,
			founded_,
			hasRowKey_,
			indexInfoList_,
			nullStatisticsList_,
			sqlString_);
};
#endif
