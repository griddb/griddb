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
	@brief Definition of nosql command
*/

#ifndef NOSQL_COMMAND_H_
#define NOSQL_COMMAND_H_

#include "sql_common.h"
#include "nosql_client_session.h"
#include "transaction_service.h"
#include "sql_tuple.h"
#include "sql_parser.h"
#include "sql_processor.h"
#include "time_series.h"
#include "message_schema.h"
#include "message_row_store.h"
#include "json.h"
#include "picojson.h"

class NoSQLContainer;
typedef uint8_t ColumnOption;
typedef StatementId ExecId;
class DBConnection;
struct DDLBaseInfo;

static int32_t DEFAULT_NOSQL_COMMAND_TIMEOUT = 60 * 5 * 1000;

const char8_t* getPartitionStatusRowName(LargeContainerStatusType type);
const char8_t* getTablePartitionTypeName(uint8_t type);
const char8_t* getTablePartitionTypeName(
		SyntaxTree::TablePartitionType partitionType);
const char8_t* getContainerTypeName(uint8_t type);
const char8_t* getDateTimeName(uint8_t type);
const char8_t* getPartitionStatusName(LargeContainerStatusType type);

struct TableProperty {

	struct TablePropertyElem {
		TablePropertyElem(util::StackAllocator& alloc) : key_(0), value_(alloc) {}
		int32_t key_;
		util::String value_;
		UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
		UTIL_OBJECT_CODER_MEMBERS(key_, value_);
	};

	TableProperty(util::StackAllocator& alloc) : alloc_(alloc), list_(alloc), map_(alloc) {}
	util::StackAllocator& getAllocator() {
		return alloc_;
	}
	typedef util::Vector< TablePropertyElem* > TableList;
	typedef util::Map<int32_t, TablePropertyElem*> TableMap;
	void init() {
		for (TableList::iterator it = list_.begin(); it != list_.end(); it++) {
			map_.insert(std::make_pair((*it)->key_, (*it)));
		}
	}
	void put(int32_t key, const char* value) {
		TablePropertyElem* elem = ALLOC_NEW(alloc_) TablePropertyElem(alloc_);
		elem->key_ = key;
		elem->value_ = value;
		list_.push_back(elem);
		map_.insert(std::make_pair(key, elem));
	}
	const char* get(int32_t key) {
		TableMap::iterator it = map_.find(key);
		if (it != map_.end()) {
			return (*it).second->value_.c_str();
		}
		else {
			return NULL;
		}
	}
	util::StackAllocator& alloc_;
	TableList list_;
	TableMap map_;
	UTIL_OBJECT_CODER_MEMBERS(list_);
};

class NoSQLRequest {

	typedef StatementHandler::StatementExecStatus StatementExecStatus;

public:

	NoSQLRequest(SQLVariableSizeGlobalAllocator& varAlloc,
		SQLService* sqlSvc, TransactionService* txnSvc, ClientId* clientId);
	~NoSQLRequest();
	NoSQLRequestId start(Event& request);
	void get(Event& request, util::StackAllocator& alloc, NoSQLContainer* container,
		int32_t waitInterval, int32_t timeoutInterval, util::XArray<uint8_t>& response);
	void wait(int32_t waitInterval);
	void put(NoSQLRequestId requestId, StatementExecStatus status,
		EventByteInStream& in, util::Exception* exception, StatementMessage::Request& request);
	void cancel();
	void getResponse(util::XArray<uint8_t>& buffer);
	bool isRunning();

	NoSQLRequestId getRequestId() {
		util::LockGuard<util::Condition> guard(condition_);
		return requestId_;
	}

private:

	NoSQLRequest(const NoSQLRequest&);
	NoSQLRequest& operator=(const NoSQLRequest&);

	enum State {
		STATE_NONE,
		STATE_SUCCEEDED,
		STATE_FAILED
	};

	util::Condition condition_;
	State state_;
	util::Exception exception_;
	SQLVariableSizeGlobalAllocator& varAlloc_;
	NoSQLRequestId requestId_;
	uint8_t* syncBinaryData_;
	size_t syncBinarySize_;
	SQLService* sqlSvc_;
	TransactionService* txnSvc_;
	EventType eventType_;
	NoSQLContainer* container_;
	ClientId* clientId_;
	void clear();
};

typedef std::vector<uint8_t, util::StdAllocator<
	uint8_t, SQLVariableSizeGlobalAllocator> > NullStatisticsList;
typedef std::vector<int32_t, util::StdAllocator<
	int32_t, SQLVariableSizeGlobalAllocator> > TableIndexInfoList;

struct LargeExecStatus {

	LargeExecStatus(util::StackAllocator& eventStackAlloc) :
		eventStackAlloc_(eventStackAlloc),
		currentStatus_(PARTITION_STATUS_NONE),
		affinityNumber_(UNDEF_NODE_AFFINITY_NUMBER),
		indexInfo_(eventStackAlloc) {}

	util::StackAllocator& eventStackAlloc_;
	LargeContainerStatusType currentStatus_;
	NodeAffinityNumber affinityNumber_;
	IndexInfo indexInfo_;
	void reset() {
		currentStatus_ = PARTITION_STATUS_NONE;
		affinityNumber_ = UNDEF_NODE_AFFINITY_NUMBER;
		indexInfo_.indexName_.clear();
		indexInfo_.mapType = MAP_TYPE_DEFAULT;
		indexInfo_.columnIds_.clear();
	}
};

struct RenameColumnSchemaInfo {

	RenameColumnSchemaInfo() :
		isRenameColumn_(false) {}

	void encode(EventByteOutStream& out);

	bool isRenameColumn() {
		return isRenameColumn_;
	}

	bool isRenameColumn_; 
};

struct TableExpirationSchemaInfo {
	TableExpirationSchemaInfo() : elapsedTime_(-1),
		timeUnit_(UINT8_MAX), startValue_(-1),
		limitValue_(-1), isTableExpiration_(false)
		, subContainerAffinity_(-1)
		, dataAffinityPos_(SIZE_MAX)
	{}
	int32_t elapsedTime_;
	TimeUnit timeUnit_;
	int64_t startValue_;
	int64_t limitValue_;
	bool isTableExpiration_;
	int64_t subContainerAffinity_;
	size_t dataAffinityPos_;
	bool updateSchema(util::StackAllocator& alloc,
		util::XArray<uint8_t>& binarySchemaInfo,
		util::XArray<uint8_t>& newBinarySchemaInfo);

	void encode(EventByteOutStream& out);

	bool isTableExpiration() {
		return isTableExpiration_;
	}
};

struct TableContainerInfo;

struct TargetContainerInfo {
	TargetContainerInfo() : pId_(0), versionId_(UNDEF_SCHEMAVERSIONID),
		containerId_(-1), affinity_(UNDEF_NODE_AFFINITY_NUMBER), pos_(0) {
	}
	PartitionId pId_;
	SchemaVersionId versionId_;
	ContainerId containerId_;
	NodeAffinityNumber affinity_;
	size_t pos_;
	void set(TableContainerInfo* info, NodeAffinityNumber affinity);
};

/*!
	@brief コンテナ情報
*/
struct TableContainerInfo {
public:
	TableContainerInfo() :
			containerId_(0),
			versionId_(0),
			pId_(0), pos_(0),
			affinity_(UNDEF_NODE_AFFINITY_NUMBER),
			approxSize_(-1) {}

	UTIL_OBJECT_CODER_MEMBERS(
			containerId_, versionId_, pId_, approxSize_);

	ContainerId containerId_;
	uint32_t versionId_;
	PartitionId pId_;
	size_t pos_;
	NodeAffinityNumber affinity_;
	int64_t approxSize_;
};

static const uint32_t OLD_LARGE_CONTAINER_VERSION_ID = 0;
static const uint32_t LARGE_CONTAINER_VERSION_ID_1 = 1;

static const uint32_t LARGE_CONTAINER_VERSION_COLUMN_ID = 0;
static const uint32_t LARGE_CONTAINER_PARTITION_INFO_COLUMN_ID = 1;
static const uint32_t LARGE_CONTAINER_PARTITION_OPTION_COLUMN_ID = 2;

struct VersionInfo {
	VersionInfo() : version_(LARGE_CONTAINER_VERSION_ID_1) {}
	uint32_t version_;
	UTIL_OBJECT_CODER_MEMBERS(version_);
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

	TablePartitioningOptionInfo(Alloc& alloc) :
		alloc_(alloc),
		partitionColumnNameList_(alloc),
		partitionColumnIdList_(alloc),
		subPartitionColumnNameList_(alloc),
		subPartitionColumnIdList_(alloc),
		keyNameIdList_(alloc), keyValueList_(alloc), keyPositionList_(alloc),
		primaryColumnNameList_(alloc),
		primaryColumnIdList_(alloc),
		primaryColumnTypeList_(alloc) {}

	Alloc& alloc_;
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

template<typename Alloc>
struct TablePartitioningInfo {

	typedef SyntaxTree::CreateTableOption CreateTableOption;
	typedef SyntaxTree::CreateIndexOption CreateIndexOption;

	typedef util::BasicString<
		char8_t, std::char_traits<char8_t>,
		util::StdAllocator<char8_t, Alloc> > PString;

	typedef std::vector<PString, util::StdAllocator<
		PString, Alloc> > SubContainerNameList;

	typedef std::vector<NodeAffinityNumber, util::StdAllocator<
		NodeAffinityNumber, Alloc> > PartitionAssignNumberList;

	typedef std::vector<int64_t, util::StdAllocator<
		int64_t, Alloc> > PartitionAssignValueList;

	typedef std::vector<LargeContainerStatusType, util::StdAllocator<
		LargeContainerStatusType, Alloc> > PartitionAssignStatusList;

	typedef std::vector<PartitionId, util::StdAllocator<
		PartitionId, Alloc> > CondensedPartitionIdList;

	typedef std::vector<int64_t, util::StdAllocator<
		int64_t, Alloc> > PartitionAssignVersionList;

	typedef std::pair<NodeAffinityNumber, size_t>
		PartitionAssignNumberMapListEntry;
	typedef std::map<NodeAffinityNumber, size_t, std::less<NodeAffinityNumber>,
		util::StdAllocator<PartitionAssignNumberMapListEntry, Alloc> >
		PartitionAssignNumberMapList;

	typedef std::map<NodeAffinityNumber, size_t, std::less<NodeAffinityNumber>,
		util::StdAllocator<PartitionAssignNumberMapListEntry, util::StackAllocator> >
		PartitionAssignNumberStackMapList;

	TablePartitioningInfo(Alloc& alloc);

	void init();

	bool isPartitioning() {
		return (partitionType_ != SyntaxTree::TABLE_PARTITION_TYPE_UNDEF);
	}

	Alloc& getAllocator() {
		return alloc_;
	}

	void checkMaxIntervalValue(int64_t tmpValue, int64_t& interval) {
		if (tmpValue == INT64_MIN) {
			interval = interval + 2;
		}
	}

	ContainerId getLargeContainerId() {
		return largeContainerId_;
	}

	uint8_t getPartitioningType() {
		return partitionType_;
	}

	bool checkRecoveryPartitioningType(int32_t subContainerId) {
		if (partitionType_ == SyntaxTree::TABLE_PARTITION_TYPE_HASH
			|| ((SyntaxTree::isRangePartitioningType(partitionType_)
				&& (subContainerId == 0)))) {
			return true;
		}
		return false;
	}

	template<typename T>
	void copy(TablePartitioningInfo<T>& info) {
		partitionType_ = info.partitionType_;
		partitioningNum_ = info.partitioningNum_;
		partitionColumnName_ = info.partitionColumnName_.c_str();
		partitioningColumnId_ = info.partitioningColumnId_;
		partitionColumnType_ = info.partitionColumnType_;
		subPartitioningColumnName_ = info.subPartitioningColumnName_.c_str();
		subPartitioningColumnId_ = info.subPartitioningColumnId_;
		intervalUnit_ = info.intervalUnit_;
		intervalValue_ = info.intervalValue_;
		largeContainerId_ = info.largeContainerId_;
		intervalAffinity_ = info.intervalAffinity_;
		for (size_t pos = 0; pos < subContainerNameList_.size(); pos++) {
			subContainerNameList_[pos] = subContainerNameList_[pos].c_str();
		}
		for (size_t pos = 0; pos < subContainerNameList_.size(); pos++) {
			condensedPartitionIdList_[pos] = condensedPartitionIdList_[pos];
		}
		for (size_t pos = 0; pos < subContainerNameList_.size(); pos++) {
			assignNumberList_[pos] = assignNumberList_[pos];
		}
		for (size_t pos = 0; pos < subContainerNameList_.size(); pos++) {
			assignStatusList_[pos] = assignStatusList_[pos];
		}
		for (size_t pos = 0; pos < subContainerNameList_.size(); pos++) {
			assignValueList_[pos] = assignValueList_[pos];
		}
		for (
			typename PartitionAssignNumberStackMapList::iterator it = info.assignNumberMapList_.begin();
			it != info.assignNumberMapList_.end(); it++) {
			assignNumberMapList_.insert(std::make_pair((*it).first, (*it).second));
		}
		assignCountMax_ = info.assignCountMax_;
		tableStatus_ = info.tableStatus_;
		dividePolicy_ = info.dividePolicy_;
		distributedfPolicy_ = info.distributedfPolicy_;
		partitioningVersionId_ = info.partitioningVersionId_;
		primaryColumnId_ = info.primaryColumnId_;
		containerType_ = info.containerType_;
		currentStatus_ = info.currentStatus_;
		currentAffinityNumber_ = info.currentAffinityNumber_;
		currentIndexName_ = info.currentIndexName_.c_str();
		currentIndexType_ = info.currentIndexType_;
		currentIndexColumnId_ = info.currentIndexColumnId_;
		anyNameMatches_ = info.anyNameMatches_;
		anyTypeMatches_ = info.anyTypeMatches_;
		currentIndexCaseSensitive_ = info.currentIndexCaseSensitive_;
		subPartitionColumnType_ = info.subPartitionColumnType_;
		activeContainerCount_ = info.activeContainerCount_;
		timeSeriesProperty_.timeUnit_ = info.timeSeriesProperty_.timeUnit_;
		timeSeriesProperty_.dividedNum_ = info.timeSeriesProperty_.dividedNum_;
		timeSeriesProperty_.elapsedTime_ = info.timeSeriesProperty_.elapsedTime_;
		subIdListCached_ = info.subIdListCached_;
		for (size_t pos = 0; pos < availableCountList_.size(); pos++) {
			availableCountList_[pos] = info.availableCountList_[pos];
		}
		for (size_t pos = 0; pos < disAvailableList_.size(); pos++) {
			disAvailableList_[pos] = info.disAvailableList_[pos];
		}
		for (size_t pos = 0; pos < disAvailableCountList_.size(); pos++) {
			disAvailableCountList_[pos] = info.disAvailableCountList_[pos];
		}
		for (size_t pos = 0; pos < disAvailableCountList_.size(); pos++) {
			disAvailableCountList_[pos] = info.disAvailableCountList_[pos];
		}
		currentIntervalValue_ = info.currentIntervalValue_;
	}

	static const int32_t CURRENT_INTERVAL_UNIT = 2;
	static const int32_t CURRENT_BOUNDARY_UNIT = (CURRENT_INTERVAL_UNIT + 1);
	static const int64_t MAX_MINUS_INTERVAL = 3;
	static const int64_t MIN_PLUS_INTERVAL = 0;

	void checkIntervalList(int64_t interval, int64_t currentValue,
		util::Vector<int64_t>& intervalList,
		util::Vector<int64_t>& intervalCountList, int64_t& prevInterval);

	void checkExpirableInterval(int64_t currentTime,
		int64_t currentErasableTimestamp, int64_t duration,
		util::Vector<NodeAffinityNumber>& expiredAffinityNumbers,
		util::Vector<size_t>& expiredAffinityPos);

	void getIntervalInfo(int64_t currentTime, size_t pos,
		int64_t& erasableTime, int64_t& startTime);

	void getSubIdList(util::StackAllocator& alloc,
		util::Vector<int64_t>& availableList,
		util::Vector<int64_t>& availableCountList,
		util::Vector<int64_t>& disAvailableList,
		util::Vector<int64_t>& disAvailableCountList,
		int64_t currentTime);

	void findNeighbor(util::StackAllocator& alloc,
		int64_t value,
		util::Set<NodeAffinityNumber>& neighborAssignList);

	void setSubContainerPartitionIdList(
		util::Vector<NodeAffinityNumber>& assignNumberList);

	size_t findEntry(NodeAffinityNumber affinity);

	size_t newEntry(NodeAffinityNumber affinity,
		LargeContainerStatusType status,
		int32_t partitioningNum, int64_t baseValue, size_t maxAssignedEntryNum);

	TablePartitioningVersionId incrementTablePartitioningVersionId() {
		partitioningVersionId_++;
		return partitioningVersionId_;
	}

	TablePartitioningVersionId getTablePartitioningVersionId() {
		return partitioningVersionId_;
	}

	NodeAffinityNumber getAffinity(size_t pos) {
		return assignNumberList_[pos];
	}

	LargeContainerStatusType getPartitionStatus(size_t pos) {
		return assignStatusList_[pos];
	}

	NodeAffinityNumber getAffinityNumber(uint32_t partitionNum,
		const TupleValue* value1, const TupleValue* value2,
		int32_t position, int64_t& baseValue,
		NodeAffinityNumber& baseAffinity);

	int64_t calcValueFromAffinity(
		uint32_t partitionNum, NodeAffinityNumber affinity);

	void setCondensedPartitionIdList(util::Vector<PartitionId>& pIdList) {
		condensedPartitionIdList_.assign(pIdList.size(), 0);
		for (size_t i = 0; i < pIdList.size(); i++) {
			condensedPartitionIdList_[i] = pIdList[i];
		}
	}

	int32_t getCurrentPartitioningCount() {
		return static_cast<int32_t>(assignNumberList_.size());
	}

	int64_t getActiveContainerCount() {
		return activeContainerCount_;
	}

	struct TimeSeriesProperty {
	public:
		TimeSeriesProperty() : timeUnit_(TIME_UNIT_DAY),
			dividedNum_(BaseContainer::EXPIRE_DIVIDE_DEFAULT_NUM), elapsedTime_(-1) {
		}

		bool operator==(const TimeSeriesProperty& op) const {
			return (
				timeUnit_ == op.timeUnit_
				&& dividedNum_ == op.dividedNum_
				&& elapsedTime_ == op.elapsedTime_);
		}

		UTIL_OBJECT_CODER_MEMBERS(timeUnit_, dividedNum_, elapsedTime_);

		TimeUnit timeUnit_;
		uint16_t dividedNum_;
		int32_t elapsedTime_;
	};

	void setIntervalWorkerGroup(uint32_t group) {
		intervalAffinity_ &= 0xFFFFFFFF00000000;
		intervalAffinity_ |= static_cast<uint32_t>(group);
	}

	void setIntervalWorkerGroupPosition(int32_t pos) {
		intervalAffinity_ &= 0x00000000FFFFFFFF;
		intervalAffinity_ |= static_cast<uint64_t>(static_cast<uint64_t>(pos + 1) << 32);
	}

	bool isSetSetIntervalWorkerGroupPosition() {
		return (!((intervalAffinity_ & 0xFFFFFFFF00000000) == 0xFFFFFFFF00000000));
	}

	uint32_t getIntervalWorkerGroup() {
		return static_cast<uint32_t>(intervalAffinity_ & 0x00000000FFFFFFFF);
	}

	int32_t getIntervalWorkerGroupPosition() {
		return (isSetSetIntervalWorkerGroupPosition() ? static_cast<uint32_t>((intervalAffinity_ >> 32) - 1) : 0);
	}

	Alloc& alloc_;
	uint8_t partitionType_;
	PartitionId partitioningNum_;
	PString partitionColumnName_;
	ColumnId partitioningColumnId_;
	uint16_t partitionColumnType_;
	PString subPartitioningColumnName_;
	ColumnId subPartitioningColumnId_;
	uint8_t intervalUnit_;
	int64_t intervalValue_;

	ContainerId largeContainerId_;
	uint64_t intervalAffinity_;

	SubContainerNameList subContainerNameList_;
	CondensedPartitionIdList condensedPartitionIdList_;

	PartitionAssignNumberList assignNumberList_;
	PartitionAssignStatusList assignStatusList_;
	PartitionAssignValueList assignValueList_;
	PartitionAssignNumberMapList assignNumberMapList_;

	int64_t assignCountMax_;
	int8_t tableStatus_;
	int32_t dividePolicy_;
	int32_t distributedfPolicy_;
	TablePartitioningVersionId partitioningVersionId_;
	int32_t primaryColumnId_;
	uint8_t containerType_;
	LargeContainerStatusType currentStatus_;
	NodeAffinityNumber currentAffinityNumber_;
	PString currentIndexName_;
	int8_t currentIndexType_;
	int32_t currentIndexColumnId_;
	uint8_t anyNameMatches_;
	uint8_t anyTypeMatches_;
	bool currentIndexCaseSensitive_;
	uint16_t subPartitionColumnType_;
	int64_t activeContainerCount_;
	TimeSeriesProperty timeSeriesProperty_;
	util::Atomic<bool> subIdListCached_;
	PartitionAssignValueList availableList_;
	PartitionAssignValueList availableCountList_;
	PartitionAssignValueList disAvailableList_;
	PartitionAssignValueList disAvailableCountList_;

	int64_t currentIntervalValue_;
	std::string dumpExpirationInfo(const char* containerName,
		TableExpirationSchemaInfo& info);

	bool isTableExpiration() {
		return (timeSeriesProperty_.elapsedTime_ != -1
			&& tableStatus_ == static_cast<uint8_t>(
				EXPIRATION_TYPE_PARTITION));
	}

	bool isRowExpiration() {
		return (timeSeriesProperty_.elapsedTime_ != -1
			&& tableStatus_ == static_cast<uint8_t>(EXPIRATION_TYPE_ROW));
	}

	bool isExpiration() {
		return (isTableExpiration() || isRowExpiration());
	}

	void checkTableExpirationSchema(TableExpirationSchemaInfo& info,
		NodeAffinityNumber affinity, uint32_t partitionNum);
	void checkTableExpirationSchema(
		TableExpirationSchemaInfo& info, size_t pos);

	util::Mutex mutex_;

	bool checkSchema(TablePartitioningInfo& target,
		util::XArray<uint8_t>& targetSchema,
		util::XArray<uint8_t>& currentSchema);

	void checkMaxAssigned(
		TransactionContext& txn,
		const FullContainerKey& containerKey,
		int32_t partitioningNum, size_t maxAssignedNum);

	bool checkInterval(int32_t pos) {
		if (partitionType_ == SyntaxTree::TABLE_PARTITION_TYPE_HASH) {
			return false;
		}
		if (pos == 1) {
			return false;
		}
		return true;
	}

	std::string getIntervalValue() {
		util::NormalOStringStream oss;
		if (SyntaxTree::isRangePartitioningType(partitionType_)) {
			if (TupleColumnTypeUtils::isTimestampFamily(partitionColumnType_)) {
				util::DateTime::FieldType dateType = static_cast<util::DateTime::FieldType>(intervalUnit_);
				switch (dateType) {
				case util::DateTime::FIELD_DAY_OF_MONTH: {
					int64_t orgValue = intervalValue_ / 24 / 3600 / 1000;
					oss << orgValue;
					break;
				}
				case util::DateTime::FIELD_HOUR: {
					int64_t orgValue = intervalValue_ / 1 / 3600 / 1000;
					oss << orgValue;
					break;
				}
				default:
					break;
				}
			}
			else {
				oss << intervalValue_;
			}
		}
		return oss.str().c_str();
	}

	std::string getIntervalUnit() {
		util::NormalOStringStream oss;
		if (intervalUnit_ != UINT8_MAX) {
			oss << getDateTimeName(intervalUnit_);
		}
		else {
		}
		return oss.str().c_str();
	}

	bool checkHashPartitioning(int32_t pos) {
		switch (static_cast<SyntaxTree::TablePartitionType>(partitionType_)) {
		case SyntaxTree::TABLE_PARTITION_TYPE_HASH:
			return (pos == 0);
		case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH:
			return (pos == 1);
		default:
			return false;
		}
	}

	int32_t getHashPartitioningCount(int32_t pos) {
		UNUSED_VARIABLE(pos);

		switch (static_cast<SyntaxTree::TablePartitionType>(partitionType_)) {
		case SyntaxTree::TABLE_PARTITION_TYPE_HASH:
			return partitioningNum_;
		case SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH:
			return partitioningNum_;
		default:
			return -1;
		}
	}

	std::string getPartitioningCount() {
		util::NormalOStringStream oss;
		if (partitionType_ == SyntaxTree::TABLE_PARTITION_TYPE_HASH) {
			oss << partitioningNum_;
		}
		return oss.str().c_str();
	}

	std::string getSubPartitioningCount() {
		util::NormalOStringStream oss;
		if (partitionType_ == SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH) {
			oss << partitioningNum_;
		}
		return oss.str().c_str();
	}

	std::string getTimeSeriesExpirationTime() {
		util::NormalOStringStream oss;
		if (timeSeriesProperty_.elapsedTime_ != -1) {
			oss << timeSeriesProperty_.elapsedTime_;
		}
		return oss.str().c_str();
	}

	std::string getTimeSeriesExpirationTimeUnit() {
		util::NormalOStringStream oss;
		if (timeSeriesProperty_.timeUnit_ != UINT8_MAX) {
			oss << getDateTimeName(timeSeriesProperty_.timeUnit_);
		}
		return oss.str().c_str();
	}

	std::string getTimeSeriesExpirationDivisionCount() {
		util::NormalOStringStream oss;
		if (timeSeriesProperty_.dividedNum_ != UINT16_MAX) {
			oss << timeSeriesProperty_.dividedNum_;
		}
		return oss.str().c_str();
	}

	std::string dump(int32_t level = 0) {
		util::NormalOStringStream oss;
		oss << "TABLE_OPTIONAL_TYPE :" << getContainerTypeName(containerType_) << std::endl;
		oss << "PARTITIONING_TYPE :" << getTablePartitionTypeName(partitionType_) << std::endl;
		oss << "PARTITIONING_COLUMN :" << partitionColumnName_ << std::endl;
		oss << "PARTITION_DIVISION_COUNT :" << getPartitioningCount() << std::endl;
		oss << "SUB_PARTITIONING_COLUMN :" << subPartitioningColumnName_ << std::endl;
		oss << "SUBPARTITION_DIVISION_COUNT :" << getSubPartitioningCount() << std::endl;
		oss << "PARTITION_INTERVAL_VALUE :" << getIntervalValue() << std::endl;
		oss << "PARTITION_INTERVAL_UNIT :" << getIntervalUnit() << std::endl;
		oss << "EXPIRATION_TIME :" << getTimeSeriesExpirationTime() << std::endl;
		oss << "EXPIRATION_TIME_UNIT :" << getTimeSeriesExpirationTimeUnit() << std::endl;
		oss << "EXPIRATION_DIVISION_COUNT :" << getTimeSeriesExpirationDivisionCount() << std::endl;
		return oss.str().c_str();
	};

	TablePartitioningOptionInfo<Alloc>* opt_;
	TableProperty* property_;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(
		partitionType_,
		partitionColumnName_,
		partitioningNum_,
		partitioningColumnId_,
		largeContainerId_,
		intervalAffinity_,
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

	explicit TablePartitioningIndexInfoEntry(util::StackAllocator& alloc) :
			alloc_(alloc), indexName_(alloc),
			columnIds_(alloc), indexType_(-1),
			status_(PARTITION_STATUS_CREATE_START), pos_(0) {}

	TablePartitioningIndexInfoEntry(
			util::StackAllocator& alloc,
			util::String& indexName, util::Vector<uint32_t>& columnIds,
			MapType indexType) :
			alloc_(alloc), indexName_(indexName),
			columnIds_(columnIds), indexType_(indexType),
			status_(PARTITION_STATUS_CREATE_START), pos_(0) {}

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(
		indexName_, columnIds_, indexType_, status_);

	util::StackAllocator& alloc_;
	util::String indexName_;
	util::Vector<uint32_t> columnIds_;
	MapType indexType_;
	LargeContainerStatusType status_;
	int32_t pos_;
};

struct TablePartitioningIndexInfo {

	explicit TablePartitioningIndexInfo(
			util::StackAllocator& eventStackAlloc) :
			eventStackAlloc_(eventStackAlloc),
			indexEntryList_(eventStackAlloc_),
			indexEntryMap_(eventStackAlloc_),
			partitioningVersionId_(0) {}

	util::StackAllocator& eventStackAlloc_;
	util::Vector<TablePartitioningIndexInfoEntry*> indexEntryList_;
	util::Map<util::String, TablePartitioningIndexInfoEntry*> indexEntryMap_;
	TablePartitioningVersionId partitioningVersionId_;

	void getIndexInfoList(util::StackAllocator& alloc,
		util::Vector<IndexInfo>& indexInfoList);

	bool isEmpty() {
		return (indexEntryList_.size() == 0);
	}

	bool isSame(size_t pos,
		util::Vector<ColumnId>& columnIdList, MapType indexType);

	TablePartitioningIndexInfoEntry* find(
		const NameWithCaseSensitivity& indexName,
		util::Vector<ColumnId>& columnIdList, MapType indexType);

	TablePartitioningIndexInfoEntry* check(
		util::Vector<ColumnId>& columnIdList, MapType indexType);

	TablePartitioningIndexInfoEntry* find(
		const NameWithCaseSensitivity& indexName);

	TablePartitioningIndexInfoEntry* add(
		const NameWithCaseSensitivity& indexName,
		util::Vector<ColumnId>& columnIdList, MapType indexType);

	util::StackAllocator& getAllocator() {
		return eventStackAlloc_;
	}

	void remove(int32_t pos);

	std::string dumpStatus(util::StackAllocator& alloc);

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(indexEntryList_, partitioningVersionId_);
};


struct IndexInfoEntry {
	IndexInfoEntry() : columnId_(UNDEF_COLUMNID), indexType_(MAP_TYPE_DEFAULT) {}
	ColumnId columnId_;
	MapType indexType_;
	UTIL_OBJECT_CODER_MEMBERS(columnId_, indexType_);
};

struct ViewInfo {
	ViewInfo(util::StackAllocator& alloc) : alloc_(alloc), sqlString_(alloc) {}
	util::StackAllocator& getAllocator() {
		return alloc_;
	}
	util::StackAllocator& alloc_;
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
	explicit TableColumnIndexInfoEntry(
			SQLVariableSizeGlobalAllocator& globalVarAlloc) :
			indexName_(globalVarAlloc),
			indexType_(0) {}

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(indexName_, indexType_);

	SQLString indexName_;
	MapType indexType_;
};

struct TableColumnInfo {
	typedef std::vector<TableColumnIndexInfoEntry, util::StdAllocator<
			TableColumnIndexInfoEntry, SQLVariableSizeGlobalAllocator> >
			TableColumnIndexInfoList;

	explicit TableColumnInfo(SQLVariableSizeGlobalAllocator& globalVarAlloc) :
			name_(globalVarAlloc),
			type_(0), option_(0),
			tupleType_(TupleList::TYPE_NULL),
			indexInfo_(globalVarAlloc) {}

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(
			name_, type_, option_,
			columnOption_, indexInfo_, tupleType_);

	SQLString name_;
	uint8_t type_;
	uint8_t option_;

	TupleList::TupleColumnType tupleType_;

	TableColumnOptionInfo columnOption_;
	TableColumnIndexInfoList indexInfo_;
};

struct TableSchemaInfo {

	typedef std::vector<TableColumnInfo, util::StdAllocator<
		TableColumnInfo, SQLVariableSizeGlobalAllocator> >
		TableColumnInfoList;

	typedef std::vector<TableContainerInfo, util::StdAllocator<
		TableContainerInfo, SQLVariableSizeGlobalAllocator> >
		TableContainerInfoList;

	typedef std::vector<TableIndexInfoList, util::StdAllocator<
		TableIndexInfoList, SQLVariableSizeGlobalAllocator> >
		TableIndexInfoAllList;

	typedef std::pair<NodeAffinityNumber, NodeAffinityNumber>
		ContainerEntry;

	typedef std::map<NodeAffinityNumber, NodeAffinityNumber,
		std::less<NodeAffinityNumber>,
		util::StdAllocator<ContainerEntry,
		SQLVariableSizeGlobalAllocator> > AffinityContainerMap;

	typedef std::vector<ColumnId, util::StdAllocator<
		ColumnId, SQLVariableSizeGlobalAllocator> > ColumnIdList;
	typedef std::vector<ColumnIdList*, util::StdAllocator<
		ColumnIdList*, SQLVariableSizeGlobalAllocator> > CompositeColumnIdList;

	TableSchemaInfo(SQLVariableSizeGlobalAllocator& globalVarAlloc) :
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
		columnSize_(0),
		tableName_(globalVarAlloc_),
		affinityMap_(AffinityContainerMap::key_compare(), globalVarAlloc_),
		lastExecutedTime_(0),
		disableCache_(false),
		diffLoad_(false),
		sqlString_(globalVarAlloc_),
		compositeColumnIdList_(globalVarAlloc_),
		property_(NULL)
	{}

	~TableSchemaInfo();

	TablePartitioningInfo<SQLVariableSizeGlobalAllocator>
		& getTablePartitioningInfo() {
		return partitionInfo_;
	}

	void copy(TableSchemaInfo& info);
	void copyPartitionInfo(TableSchemaInfo& info);

	TablePartitioningVersionId getTablePartitioningVersionId() {
		return partitionInfo_.getTablePartitioningVersionId();
	}
	void checkSubContainer(size_t nth);
	void setupSQLTableInfo(util::StackAllocator& alloc,
		SQLTableInfo& tableInfo,
		const char* tableName,
		DatabaseId dbId,
		bool withVersion,
		int32_t clusterPartitionCount,
		bool isFirst,
		bool withoutCache
		, int64_t startTime
		, const char* viewSqlString
	);

	const char* getTableName() {
		return tableName_.c_str();
	}
	bool isTimeSeriesContainer() {
		return (containerInfoList_.size() > 0
			&& containerType_ == TIME_SERIES_CONTAINER);
	}

	void checkWritableContainer();

	void setAffinity(NodeAffinityNumber affinity, int32_t subContainerId) {
		if (SyntaxTree::isRangePartitioningType(partitionInfo_.partitionType_)) {
			affinityMap_.insert(
				std::make_pair(affinity, subContainerId));
		}
	}

	void setContainerInfo(
		int32_t pos, TableContainerInfo& containerInfo,
		NodeAffinityNumber = UNDEF_NODE_AFFINITY_NUMBER);

	TableContainerInfo* setContainerInfo(
		int32_t pos, NoSQLContainer& container,
		NodeAffinityNumber = UNDEF_NODE_AFFINITY_NUMBER);

	void setPrimaryKeyIndex(int32_t primaryKeyColumnId);

	TableContainerInfo* getTableContainerInfo(
		uint32_t partitionNum,
		const TupleValue* value1,
		const TupleValue* value2,
		NodeAffinityNumber& affinity,
		int64_t& baseValue,
		NodeAffinityNumber& baseAffinity);

	TableContainerInfo* getTableContainerInfo(NodeAffinityNumber);

	void setColumnInfo(NoSQLContainer& container);
	void setOptionList(util::XArray<uint8_t>& optionList);

	uint32_t getColumnSize() {
		return columnSize_;
	}

	const char* getSQLString() {
		return sqlString_.c_str();
	}

	bool isDisableCache() {
		return disableCache_;
	}

	void setDisableCache(bool flag) {
		disableCache_ = flag;
	}

	void setDiffLoad() {
		diffLoad_ = true;
	}

	void resetDiffLoad() {
		diffLoad_ = false;
	}

	bool isDiffLoad() {
		bool current = diffLoad_;
		diffLoad_ = false;
		return current;
	}

	int64_t getLastExecutedTime() {
		return lastExecutedTime_;
	}
	void setLastExecutedTime(int64_t time) {
		lastExecutedTime_ = time;
	}

	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	TablePartitioningInfo<SQLVariableSizeGlobalAllocator> partitionInfo_;
	TableColumnInfoList columnInfoList_;
	TableContainerInfoList containerInfoList_;
	int64_t refCount_;
	bool founded_;
	bool hasRowKey_;
	ContainerType containerType_;
	ContainerAttribute containerAttr_;
	ColumnInfo* nosqlColumnInfoList_;
	TableIndexInfoList indexInfoList_;
	uint32_t columnSize_;

	SQLString tableName_;
	AffinityContainerMap affinityMap_;
	EventMonotonicTime lastExecutedTime_;
	bool disableCache_;
	bool diffLoad_;

	SQLString sqlString_;
	CompositeColumnIdList compositeColumnIdList_;

	TableProperty* property_;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(
		columnInfoList_,
		containerInfoList_,
		partitionInfo_,
		founded_,
		containerType_,
		UTIL_OBJECT_CODER_ENUM(
			containerAttr_, CONTAINER_ATTR_SINGLE),
		hasRowKey_,
		indexInfoList_,
		sqlString_);
};

struct NoSQLStoreOption {

	NoSQLStoreOption() {
		clear();
	}

	NoSQLStoreOption(SQLExecution* execution,
		const NameWithCaseSensitivity* dbName = NULL,
		const NameWithCaseSensitivity* tableName = NULL);

	void setAsyncOption(DDLBaseInfo* baseInfo,
		StatementMessage::CaseSensitivity caseSensitive);
	bool isSystemDb_;
	ContainerType containerType_;
	bool ifNotExistsOrIfExists_;
	ContainerAttribute containerAttr_;
	PutRowOption putRowOption_;
	bool isSync_;
	int32_t subContainerId_;
	ExecId execId_;
	uint8_t jobVersionId_;
	int64_t baseValue_;
	bool sendBaseValue_;
	const char* indexName_;
	SessionId currentSessionId_;
	StatementMessage::CaseSensitivity caseSensitivity_;
	int32_t featureVersion_;
	int32_t acceptableFeatureVersion_;
	const char* applicationName_;
	double storeMemoryAgingSwapRate_;
	util::TimeZone timezone_;

	void clear() {
		isSystemDb_ = false;
		containerType_ = COLLECTION_CONTAINER;
		ifNotExistsOrIfExists_ = false;
		containerAttr_ = CONTAINER_ATTR_SINGLE;
		putRowOption_ = PUT_INSERT_ONLY;
		isSync_ = true;
		subContainerId_ = -1;
		execId_ = 0;
		jobVersionId_ = 0;
		sendBaseValue_ = false;
		baseValue_ = 0;
		indexName_ = NULL;
		caseSensitivity_.clear();
		featureVersion_ = -1;
		acceptableFeatureVersion_ = -1;
		applicationName_ = NULL;
		storeMemoryAgingSwapRate_ = TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE;
		timezone_ = util::TimeZone();
	}
};

/*!
	@brief	NoSQL実行のためのコンテキスト
*/
struct NoSQLSyncContext {
	typedef StatementHandler::StatementExecStatus StatementExecStatus;

	NoSQLSyncContext(
		ClientId& clientId, PartitionTable* pt, TransactionService* txnSvc,
		SQLService* sqlSvc, SQLVariableSizeGlobalAllocator& globalVarAlloc) :
		globalVarAlloc_(globalVarAlloc),
		clientId_(clientId),
		nosqlRequest_(globalVarAlloc, sqlSvc, txnSvc, &clientId_),
		replyPId_(UNDEF_PARTITIONID),
		dbName_(NULL),
		dbId_(0),
		timeoutInterval_(-1),
		userType_(StatementHandler::UserType::USER_ADMIN),
		txnSvc_(txnSvc),
		sqlSvc_(sqlSvc),
		pt_(pt),
		txnTimeoutInterval_(-1) {}

	~NoSQLSyncContext() {}
	SQLVariableSizeGlobalAllocator& getGlobalAllocator() {
		return globalVarAlloc_;
	}

	void get(Event& request, util::StackAllocator& alloc,
		NoSQLContainer* container, int32_t waitInterval,
		int32_t timeoutInterval, util::XArray<uint8_t>& response);
	void wait(int32_t waitInterval);
	void put(EventType eventType, int64_t syncId, StatementExecStatus status,
		EventByteInStream& in, util::Exception& exception, StatementMessage::Request& request);

	void cancel();
	bool isRunning();

	int64_t getRequestId() {
		return nosqlRequest_.getRequestId();
	}
	util::Mutex lock_;
	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	ClientId clientId_;
	NoSQLRequest nosqlRequest_;
	PartitionId replyPId_;
	const char* dbName_;
	DatabaseId dbId_;
	int32_t timeoutInterval_;
	StatementHandler::UserType userType_;
	TransactionService* txnSvc_;
	SQLService* sqlSvc_;
	PartitionTable* pt_;
	int32_t txnTimeoutInterval_;
};

class NoSQLContainer {

	typedef int32_t AccessControlType;
	static const int32_t DEFAULT_WAIT_INTERVAL = 1000;
	static const int32_t DEFAULT_RETRY_MAX = DEFAULT_WAIT_INTERVAL * 60;
	static const int32_t DEFAULT_NOSQL_FAILOVER_LIMIT_TIME = 120 * 1000;

public:
	struct OptionalRequest;

	NoSQLContainer(EventContext& ec,
		const NameWithCaseSensitivity& containerName,
		NoSQLSyncContext& context, SQLExecution* execution);

	NoSQLContainer(EventContext& ec,
		const NameWithCaseSensitivity& containerName,
		FullContainerKey* containerKey,
		NoSQLSyncContext& context,
		SQLExecution* execution);

	NoSQLContainer(EventContext& ec,
		ContainerId containerId, SchemaVersionId versionId,
		PartitionId pId, NoSQLSyncContext& context,
		SQLExecution* execution);

	NoSQLContainer(EventContext& ec,
		PartitionId pId, NoSQLSyncContext& context,
		SQLExecution* execution);

	~NoSQLContainer();
	void encodeRequestId(
		util::StackAllocator& alloc, Event& request,
		NoSQLRequestId requestId);

	bool isLargeContainer() {
		return (getContainerAttribute() == CONTAINER_ATTR_LARGE);
	}

	bool isViewContainer() {
		return (getContainerAttribute() == CONTAINER_ATTR_VIEW);
	}

	void getContainerInfo(NoSQLStoreOption& option);

	void estimateIndexSearchSize(
			const SQLIndexStatsCache::KeyList &keyList,
			NoSQLStoreOption &option, util::Vector<int64_t> &estimationList);

	void getContainerBinary(NoSQLStoreOption& option,
		util::XArray<uint8_t>& response);

	const char* getSQLString() {
		return sqlString_.c_str();
	}

	void getContainerCount(uint64_t& count, NoSQLStoreOption& option);

	void putContainer(TableExpirationSchemaInfo& info,
		RenameColumnSchemaInfo& renameColInfo, 
		util::XArray<uint8_t>& binarySchemaInfo,
		bool modifiable, NoSQLStoreOption& option);

	void putLargeContainer(util::XArray<uint8_t>& binarySchemaInfo,
		bool modifiable, NoSQLStoreOption& option,
		util::XArray<uint8_t>* containerSchema = NULL,
		util::XArray<uint8_t>* binaryData = NULL);

	void updateLargeContainerStatus(
		LargeContainerStatusType partitionStatus,
		NodeAffinityNumber pos,
		NoSQLStoreOption& option,
		LargeExecStatus& execStatus, IndexInfo* indexInfo);

	bool createLargeIndex(const char* indexName,
		const char* columnName,
		MapType mapType, uint32_t columnId,
		NoSQLStoreOption& option);

	bool createLargeIndex(const char* indexName, util::Vector<util::String>& columnNameList,
		MapType mapType, util::Vector<ColumnId>&, NoSQLStoreOption& option);

	void updateContainerProperty(TablePartitioningVersionId versionId,
		NoSQLStoreOption& option);

	void dropContainer(NoSQLStoreOption& option);

	void putRow(util::XArray<uint8_t>& fixedPart,
		util::XArray<uint8_t>& varPart,
		RowId rowId, NoSQLStoreOption& option);

	void putRowSet(util::XArray<uint8_t>& fixedPart,
		util::XArray<uint8_t>& varPart,
		uint64_t numRows, NoSQLStoreOption& option);

	void updateRow(RowId rowId,
		util::XArray<uint8_t>& rowData, NoSQLStoreOption& option);

	void deleteRowSet(util::XArray<RowId>& rowIdList,
		NoSQLStoreOption& option);

	void updateRowSet(util::XArray<uint8_t>& fixedPart,
		util::XArray<uint8_t>& varPart,
		util::XArray<RowId>& rowIdList,
		NoSQLStoreOption& option);

	void executeSyncQuery(const char* query,
		NoSQLStoreOption& option,
		util::XArray<uint8_t>& fixedPart,
		util::XArray<uint8_t>& varPart,
		int64_t& rowCount,
		bool& hasRowKey);

	void createIndex(const char* indexName,
		MapType mapType, uint32_t columnId, NoSQLStoreOption& option);

	void createIndex(const char* indexName, MapType mapType,
		util::Vector<uint32_t>& columnIds, NoSQLStoreOption& option);

	void dropIndex(const char* indexName,
		NoSQLStoreOption& option);

	void createDatabase(const char* dbName,
		NoSQLStoreOption& option);

	void dropDatabase(const char* dbName,
		NoSQLStoreOption& option);

	void grant(const char* userName, const char* dbName,
		AccessControlType type, NoSQLStoreOption& option);

	void revoke(const char* userName, const char* dbName,
		AccessControlType type, NoSQLStoreOption& option);

	void putUser(const char* userName, const char* password,
		bool modifiable, NoSQLStoreOption& option, bool isRole);

	void dropUser(const char* userName, NoSQLStoreOption& option);

	void createTransactionContext(NoSQLStoreOption& option);

	void commit(NoSQLStoreOption& option);

	void abort(NoSQLStoreOption& option);

	void closeContainer(NoSQLStoreOption& option);

	bool isExists() {
		return founded_;
	}

	const char* getName() {
		return containerName_.c_str();
	}

	ContainerAttribute getContainerAttribute() {
		return attribute_;
	}

	ContainerType getContainerType() {
		return containerType_;
	}

	ContainerId getContainerId() {
		return containerId_;
	}

	SchemaVersionId getSchemaVersionId() {
		return versionId_;
	}

	PartitionId getPartitionId() {
		return txnPId_;
	}

	bool hasRowKey() {
		return hasRowKey_;
	}

	const util::XArray<int32_t>& getIndexInfoList() {
		return indexInfoList_;
	}

	void setContainerType(ContainerType containerType) {
		containerType_ = containerType;
	}

	int32_t getColumnId(util::StackAllocator& alloc,
		const NameWithCaseSensitivity& columnName);

	bool getIndexEntry(util::StackAllocator& alloc,
		const NameWithCaseSensitivity& indexName, ColumnId columnId,
		MapType indexType, util::Vector<IndexInfoEntry>& entryList);

	size_t getColumnSize() {
		return columnInfoList_.size();
	}

	const char* getColumnName(size_t pos) {
		return columnInfoList_[pos]->name_.c_str();
	}

	uint8_t getColumnOption(size_t pos) {
		return columnInfoList_[pos]->option_;
	}

	uint8_t getColumnType(size_t pos) {
		return columnInfoList_[pos]->type_;
	}

	TupleList::TupleColumnType getTupleColumnType(size_t pos) {
		return columnInfoList_[pos]->tupleType_;
	}

	NoSQLSyncContext& getSyncContext() {
		return *context_;
	}

	void setSyncContext(NoSQLSyncContext* context) {
		context_ = context;
	}

	bool isTransactionMode() {
		return (txnMode_ != TransactionManager::AUTO_COMMIT);
	}

	void setTransactionMode() {
		txnMode_ = TransactionManager::NO_AUTO_COMMIT_BEGIN;
		getMode_ = TransactionManager::GET;
	}

	void setAutoCommitMode() {
		txnMode_ = TransactionManager::AUTO_COMMIT;
		getMode_ = TransactionManager::AUTO;
	}

	void setBeginTransaction() {
		isBegin_ = true;
	}

	void setMode(TransactionManager::TransactionMode txnMode,
		TransactionManager::GetMode getMode) {
		txnMode_ = txnMode;
		getMode_ = getMode;
	}

	StatementId getStatementId() {
		return stmtId_;
	}

	void setCurrentSesisonId(SessionId sessionId) {
		currentSessionId_ = sessionId;
	}

	void setStatementId(StatementId stmtId) {
		stmtId_ = stmtId;
	}

	const FullContainerKey* getContainerKey() {
		return containerKey_;
	}

	bool enableCommitOrAbort() {
		return (txnMode_ == TransactionManager::NO_AUTO_COMMIT_BEGIN
			&& isBegin_ == true);
	}

	int32_t getProtocolVersion() const {
		return StatementHandler::TXN_CLIENT_VERSION;
	}

	void getSessionUUID(UUIDValue &sessionUUID) const {
		memcpy(&sessionUUID, context_->clientId_.uuid_,
			sizeof(sessionUUID));
	}

	int64_t generateSessionId() {
		int64_t newId = stmtId_ + 1;
		while (newId == 0) {
			newId++;
		}
		stmtId_ = newId;
		return stmtId_;
	}

	SQLVariableSizeGlobalAllocator& getGlobalAllocator() {
		return context_->getGlobalAllocator();
	}

	void checkCondition(uint32_t currentTimer,
		int32_t timeoutInterval = DEFAULT_NOSQL_COMMAND_TIMEOUT);

	void updateCondition();

	void updateInfo(ContainerId containerId, SchemaVersionId versionId);

	void setForceAbort() {
		forceAbort_ = true;
	}

	void setExecution(SQLExecution* execution) {
		execution_ = execution;
	}

	void setEventContext(EventContext* ec) {
		ec_ = ec;
	}

	SQLExecution* getExecution() {
		return  execution_;
	}

	util::Vector<IndexInfo>& getIndexEntryInfo() {
		return indexEntryList_;
	}

	util::StackAllocator& getAllocator() {
		return eventStackAlloc_;
	}

	bool isCaseSensitive() {
		return isCaseSensitive_;
	}

	ClientId& getNoSQLClientId() {
		return nosqlClientId_;
	}
	void setNoSQLClientId(ClientId& clientId) {
		nosqlClientId_ = clientId;
	}
	void createNoSQLClientId();
	bool isSetClientId() {
		return (nosqlClientId_.sessionId_ != 0);
	}

	void sendMessage(EventEngine& ee, Event& request);

	void setRequestOption(StatementMessage::Request& request);
	util::Vector<IndexInfo>& getCompositeIndex();
	SchemaVersionId getVersionId() {
		return versionId_;
	}

	NodeId getNodeId() {
		return nodeId_;
	}

	int64_t containerApproxSize() {
		return approxSize_;
	}

private:

	void start(ClientSession::Builder& sessionBuilder);
	bool executeCommand(Event& request, bool isSync, util::XArray<uint8_t>& response);
	void encodeFixedPart(EventByteOutStream& out, EventType type);
	void encodeOptionPart(EventByteOutStream& out, EventType type, NoSQLStoreOption& option);
	void execPrivilegeProcessInternal(EventType eventType,
		const char* userName, const char* dbName,
		AccessControlType type, NoSQLStoreOption& option);
	void commitOrRollbackInternal(EventType eventType,
		NoSQLStoreOption& option);
	void execDatabaseProcInternal(EventType eventType,
		const char* dbName, NoSQLStoreOption& option);
	void setInterval(int32_t interval = DEFAULT_WAIT_INTERVAL) {
		waitInterval_ = interval;
	}

	bool isRetryEvent() {
		return (statementType_ == SQL_GET_CONTAINER
			|| statementType_ == QUERY_TQL
			|| statementType_ == CREATE_TRANSACTION_CONTEXT
			|| statementType_ == CLOSE_SESSION
			|| statementType_ == GET_PARTITION_CONTAINER_NAMES);
	}

	bool checkTimeoutEvent(uint32_t currentTimer,
		uint32_t timeoutInterval = DEFAULT_NOSQL_COMMAND_TIMEOUT) {
		switch (statementType_) {
		case SQL_GET_CONTAINER:
		case GET_CONTAINER:
		case GET_CONTAINER_PROPERTIES:
		case PUT_CONTAINER:
		case PUT_LARGE_CONTAINER:
		case PUT_USER:
		case DROP_USER:
		case QUERY_TQL:
		case UPDATE_CONTAINER_STATUS: {
			if (currentTimer >= timeoutInterval) {
				return true;
			}
			else {
				return false;
			}
		}
		default:
			return false;
		}
	}

	static void encodeIndexEstimationKeyList(
			EventByteOutStream &out,
			const SQLIndexStatsCache::KeyList &keyList);
	static void encodeIndexEstimationKey(
			EventByteOutStream &out, const SQLIndexStatsCache::Key &key);
	static void encodeIndexEstimationKeyColumns(
			EventByteOutStream &out, const SQLIndexStatsCache::Key &key);
	static void encodeIndexEstimationKeyConditions(
			EventByteOutStream &out, const SQLIndexStatsCache::Key &key);
	static void encodeIndexEstimationKeyCondition(
			EventByteOutStream &out, const SQLIndexStatsCache::Key &key,
			bool upper);

	util::StackAllocator& eventStackAlloc_;

	NodeId nodeId_;
	ContainerId containerId_;
	SchemaVersionId versionId_;
	PartitionId txnPId_;
	util::String containerName_;
	StatementId stmtId_;
	NoSQLSyncContext* context_;
	ClientSession clientSession_;
	EventType statementType_;
	TransactionManager::TransactionMode txnMode_;
	TransactionManager::GetMode getMode_;
	int32_t waitInterval_;
	bool isBegin_;
	ContainerAttribute attribute_;
	ContainerType containerType_;
	bool hasRowKey_;
	bool founded_;
	bool setted_;

	struct NoSQLColumn {
		NoSQLColumn(util::StackAllocator& alloc) :
			alloc_(alloc), name_(alloc), type_(0), option_(0),
			tupleType_(TupleList::TYPE_NULL) {}

		util::StackAllocator& alloc_;
		util::String name_;
		uint8_t type_;
		uint8_t option_;
		TupleList::TupleColumnType tupleType_;
	};

	util::XArray<NoSQLColumn*> columnInfoList_;
	util::XArray<int32_t> indexInfoList_;
	util::Vector<uint8_t> nullStatisticsList_;
	util::Vector<IndexInfo> indexEntryList_;
	util::Vector<IndexInfo> compositeIndexEntryList_;
	PartitionRevisionNo  partitionRevNo_;
	bool forceAbort_;
	SQLExecution* execution_;
	SessionId currentSessionId_;
	bool isCaseSensitive_;
	const KeyConstraint keyConstraint_;
	const FullContainerKey* containerKey_;
	EventMonotonicTime startTime_;
	EventContext* ec_;
	util::String sqlString_;
	ClientId nosqlClientId_;
	int64_t approxSize_;
};

struct NoSQLContainer::OptionalRequest {

	static const StatementMessage::OptionType
		OPTION_TYPE_HUGE_NOSQL = 11000;

	static const StatementMessage::OptionType
		OPTION_TYPE_HUGE_NEWSQL = 12000;

	OptionalRequest(util::StackAllocator& alloc) :
		alloc_(alloc),
		optionSet_(alloc),
		transactionTimeout_(-1),
		forUpdate_(false),
		containerLockRequired_(false),
		systemMode_(false),
		dbName_(alloc),
		containerAttribute_(-1),
		putRowOption_(-1),
		sqlStatementTimeoutInterval_(-1),
		sqlFetchLimit_(-1),
		sqlFetchSize_(-1),
		replyPartitionId_(UNDEF_PARTITIONID),
		replyEventType_(UNDEF_EVENT_TYPE),
		uuid_(NULL),
		queryId_(UNDEF_SESSIONID),
		userType_(StatementMessage::USER_ADMIN),
		dbVersionId_(0),
		isSync_(false),
		subContainerId_(-1),
		execId_(0),
		jobVersionId_(0),
		ackEventType_(UNDEF_EVENT_TYPE),
		indexName_(NULL),
		createDropIndexMode_(INDEX_MODE_SQL_DEFAULT),
		currentSessionId_(0),
		isRetry_(false),
		baseValue_(0),
		requestId_(UNDEF_NOSQL_SYNCID) {
	}
	~OptionalRequest() {}
	void format(EventByteOutStream& reqOut);
	static void putType(EventByteOutStream& reqOut, StatementHandler::OptionType type);
	void set(StatementHandler::OptionType type);
	void clear();

	util::StackAllocator& alloc_;
	util::Set<StatementHandler::OptionType> optionSet_;
	int32_t transactionTimeout_;
	bool forUpdate_;
	bool containerLockRequired_;
	bool systemMode_;
	util::String dbName_;
	int32_t containerAttribute_;
	uint8_t putRowOption_;
	int32_t sqlStatementTimeoutInterval_;
	int64_t sqlFetchLimit_;
	int64_t sqlFetchSize_;
	PartitionId replyPartitionId_;
	int32_t replyEventType_;
	ClientId clientId_;
	uint8_t* uuid_;
	SessionId queryId_;
	StatementHandler::UserType userType_;
	DatabaseId dbVersionId_;
	bool isSync_;
	int32_t subContainerId_;
	ExecId execId_;
	uint8_t jobVersionId_;
	EventType ackEventType_;
	const char* indexName_;
	CreateDropIndexMode createDropIndexMode_;
	SessionId currentSessionId_;
	bool isRetry_;
	StatementMessage::CaseSensitivity caseSensitivity_;
	int64_t baseValue_;
	int64_t requestId_;
};

struct NoSQLUtils {
	typedef SyntaxTree::CreateTableOption CreateTableOption;
	typedef SyntaxTree::CreateIndexOption CreateIndexOption;

	static const uint8_t INDEX_META_INDEX_TYPE_DEFAULT = MAP_TYPE_HASH;
	static const ColumnId INDEX_META_COLID_INDEX_KEY_NAME = 0;
	static const ColumnId INDEX_META_COLID_INDEX_NAME = 1;
	static const ColumnId INDEX_META_COLID_TABLE_NAME = 2;
	static const ColumnId INDEX_META_COLID_TABLE_ID = 3;
	static const ColumnId INDEX_META_COLID_SCHEMA_VERSION = 4;
	static const ColumnId INDEX_META_COLID_COLUMN_ID = 5;
	static const ColumnId INDEX_META_COLID_COLUMN_NAME = 6;
	static const ColumnId INDEX_META_COLID_INDEX_TYPE = 7;
	static const ColumnId INDEX_META_COLID_OPTION = 8;
	static const ColumnId INDEX_META_COLID_MAX = 9;

	static const ColumnId DATABASE_META_COLID_DB_NAME = 0;
	static const ColumnId DATABASE_META_COLID_DB_ID = 1;
	static const ColumnId DATABASE_META_COLID_CONTAINER_NAME = 2;
	static const ColumnId DATABASE_META_COLID_CONTAINER_TYPE = 3;
	static const uint8_t NULLABLE_MASK = 0x80;
	static const char8_t* const LARGE_CONTAINER_KEY_VERSION;
	static const char8_t* const LARGE_CONTAINER_KEY_PARTITIONING_INFO;
	static const char8_t* const LARGE_CONTAINER_KEY_EXECUTING;
	static const char8_t* const LARGE_CONTAINER_KEY_INDEX;
	static const char8_t* const LARGE_CONTAINER_KEY_PARTITIONING_ASSIGN_INFO;
	static const char8_t* const LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA;
	static const char8_t* const LARGE_CONTAINER_KEY_VIEW_INFO;
	static const char8_t* const LARGE_CONTAINER_KEY_TABLE_PROPERTY;
	static const char8_t* const DATA_AFFINITY_POLICY_ALL;
	static const char8_t* const DATA_AFFINITY_POLICY_DISTRIBUTED;


	void checkWritableContainer();

	static PartitionId resolvePartitionId(
		util::StackAllocator& alloc,
		PartitionId partitionCount,
		const FullContainerKey& containerKey,
		ContainerHashMode hashMode = CONTAINER_HASH_MODE_CRC32);

	static uint32_t resolveSubContainerId(
		uint32_t hashValue, uint32_t tablePartitionCount);

	template<typename T>
	static void normalizeString(T& str, const char* src) {
		str.resize(strlen(src) + 1);
		char c;
		for (uint32_t i = 0; i < strlen(src); i++) {
			c = *(src + i);
			if ((c >= 'a') && (c <= 'z')) {
				str[i] = static_cast<char>(c - 32);
			}
			else {
				str[i] = static_cast<char>(c);
			}
		}
	}

	static void getAffinityValue(util::StackAllocator& alloc,
		const CreateTableOption& createOption, util::String& value);

	static void getAffinityValue(util::StackAllocator& alloc,
		util::XArray<uint8_t>& containerSchema,
		util::String& value);

	static void getAffinitySubValue(
		util::StackAllocator& alloc, NodeAffinityNumber affinity,
		util::String& affinityValue);

	static void makeLargeContainerColumn(
		util::XArray<ColumnInfo>& columnInfoList);

	template<typename Alloc>
	static void makeNormalContainerSchema(
		util::StackAllocator& alloc, const char* containerName,
		const CreateTableOption& createOption, util::XArray<uint8_t>& containerSchema,
		util::XArray<uint8_t>& optionList,
		TablePartitioningInfo<Alloc>& partitioningInfo);

	static void makeContainerColumns(
		util::StackAllocator& alloc, util::XArray<uint8_t>& containerSchema,
		util::Vector<util::String>& columnNameList,
		util::Vector<ColumnType>& columnTypeList,
		util::Vector<uint8_t>& columnOptionList);

	static void makeLargeContainerSchema(util::StackAllocator& alloc,
		util::XArray<uint8_t>& binarySchema,
		bool isView, util::String& affinityValue);

	static void checkSchemaValidation(util::StackAllocator& alloc,
		const DataStoreConfig& config, const char* containerName,
		util::XArray<uint8_t>& binarySchema, ContainerType containerType);

	static int32_t getColumnId(util::StackAllocator& alloc,
		util::Vector<util::String> columnNameList,
		const NameWithCaseSensitivity& columnName);

	template<typename T>
	static void makeLargeContainerRow(util::StackAllocator& alloc,
		const char* key,
		OutputMessageRowStore& outputMrs, T& targetValue);

	static void makeLargeContainerRowBinary(util::StackAllocator& alloc,
		const char* key,
		OutputMessageRowStore& outputMrs,
		util::XArray<uint8_t>& targetValue);

	static void makeLargeContainerIndexRow(util::StackAllocator& alloc,
		const char* key, OutputMessageRowStore& outputMrs,
		util::Vector<IndexInfo>& indexInfoList);

	static int32_t checkPrimaryKey(const CreateTableOption& option);
	static void checkPrimaryKey(
		util::StackAllocator& alloc, const CreateTableOption& option,
		util::Vector<ColumnId>& columnIds);
	static void decodePartitioningTableIndexInfo(
		util::StackAllocator& alloc,
		InputMessageRowStore& rowStore,
		util::Vector<IndexInfo>& indexInfoList);

	static bool checkAcceptableTupleType(TupleList::TupleColumnType type);

	/*!
		@brief カラム種別が可変長かどうか
		@note 内部でNoSQL系共通関数を呼び出し
	*/
	static bool isVariableType(ColumnType type);

	static uint32_t getFixedSize(ColumnType type);
};

template<typename T>
void decodeRow(InputMessageRowStore& rowStore, T& record,
	VersionInfo& versionInfo, const char* rowKey);

template<typename T>
void decodeBinaryRow(InputMessageRowStore& rowStore, T& record,
	VersionInfo& versionInfo, const char* rowKey);

template<typename T>
void getTablePartitioningInfo(util::StackAllocator& alloc, const DataStoreConfig& dsConfig,
	NoSQLContainer& container, util::XArray<ColumnInfo>& columnInfoList,
	T& partitioningInfo, NoSQLStoreOption& option);

template<typename T>
void getLargeContainerInfo(util::StackAllocator& alloc, const char* key,
	const DataStoreConfig& dsConfig, NoSQLContainer& container,
	util::XArray<ColumnInfo>& columnInfoList, T& partitioningInfo);

template<typename T>
void getLargeContainerInfoBinary(util::StackAllocator& alloc,
	const char* key, const DataStoreConfig& dsConfig, NoSQLContainer& container,
	util::XArray<ColumnInfo>& columnInfoList, T& partitioningInfo);

NoSQLContainer* createNoSQLContainer(EventContext& ec,
	const NameWithCaseSensitivity tableName, ContainerId largeContainerId,
	NodeAffinityNumber affinityNumber, SQLExecution* execution);

void resolveTargetContainer(EventContext& ec,
	const TupleValue* value1, const TupleValue* value2,
	DBConnection* conn, TableSchemaInfo* origTableSchema, SQLExecution* execution,
	NameWithCaseSensitivity& dbName, NameWithCaseSensitivity& tableName,
	TargetContainerInfo& targetInfo, bool& needRefresh);

template<typename Alloc>
bool execPartitioningOperation(EventContext& ec,
	DBConnection* conn, LargeContainerStatusType targetOperation,
	LargeExecStatus& status, NoSQLContainer& container,
	NodeAffinityNumber baseAffinity, NodeAffinityNumber targetAffinity,
	NoSQLStoreOption& option, TablePartitioningInfo<Alloc>& partitioningInfo,
	const NameWithCaseSensitivity& tableName, SQLExecution* execution,
	TargetContainerInfo& targetContainerInfo);

MapType getAvailableIndex(const DataStoreConfig& dsConfig, const char* indexName,
	ColumnType targetColumnType,
	ContainerType targetContainerType, bool primaryCheck);

bool isDenyException(int32_t errorCode);

void dumpRecoverContainer(util::StackAllocator& alloc,
	const char* tableName, NoSQLContainer* container);

void checkConnectedDbName(
	util::StackAllocator& alloc, const char* connectedDb,
	const char* currentDb, bool isCaseSensitive);

TupleList::TupleColumnType setColumnTypeNullable(
	TupleList::TupleColumnType type, bool nullable);

class NoSQLStore;

NoSQLContainer* recoverySubContainer(util::StackAllocator& alloc,
	EventContext& ec,
	NoSQLStore* store,
	SQLExecution* execution,
	const NameWithCaseSensitivity& tableName,
	int32_t partitionNum,
	NodeAffinityNumber affinity);

void checkException(std::exception& e);
void checkException(EventType type, std::exception& e);

#endif
