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
#ifndef NOSQL_CONTAINER_H_
#define NOSQL_CONTAINER_H_

#include "sql_common.h"
#include "container_key.h"
#include "sql_tuple.h"
#include "transaction_statement_message.h"
#include "cluster_common.h"
#include "nosql_common.h"

struct LargeExecStatus;
struct NameWithCaseSensitivity;
struct NoSQLSyncContext;
class SQLExecution;
struct NoSQLStoreOption;
struct TableExpirationSchemaInfo;

struct IndexInfoEntry {

	IndexInfoEntry() :
			columnId_(UNDEF_COLUMNID),
			indexType_(MAP_TYPE_DEFAULT) {}

	ColumnId columnId_;
	MapType indexType_;

	UTIL_OBJECT_CODER_MEMBERS(columnId_, indexType_);
};

class NoSQLContainerBase {

public:

	NoSQLContainerBase(
			EventContext &ec,
			NoSQLSyncContext &context,
			SQLExecution *execution);

	void setEventContext(EventContext *ec) {
		ec_ = ec;
	}

	void setBeginTransaction() {
		isBegin_ = true;
	}

	bool isExists() {	
		return founded_;
	}

	const char *getName() {
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

	const util::XArray<int32_t> &getIndexInfoList() {
		return indexInfoList_;
	}

	const util::Vector<uint8_t> &getHasNullValueList() {
		return nullStatisticsList_;
	}

	void setContainerType(ContainerType containerType) {
		containerType_ = containerType;
	}
	
	bool isLargeContainer() {
		return (getContainerAttribute() == CONTAINER_ATTR_LARGE);
	}
	
	bool isViewContainer() {
		return (getContainerAttribute() == CONTAINER_ATTR_VIEW);
	}

	const char *getSQLString() {
		return sqlString_.c_str();
	}

	int32_t getColumnId(
			util::StackAllocator &alloc,
			const NameWithCaseSensitivity &columnName);

	size_t getColumnSize() {
		return columnInfoList_.size();
	}
	
	const char *getColumnName(size_t pos) {
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
	
	NoSQLSyncContext &getSyncContext() {
		return *context_;
	}
	
	void setMode(
			TransactionManager::TransactionMode txnMode,
			TransactionManager::GetMode getMode) {

		txnMode_ = txnMode;
		getMode_ = getMode;
	}

	StatementId getStatementId() {
		return stmtId_;
	}

	void setCurrentSesionId(SessionId sessionId) {
		currentSessionId_ = sessionId;
	}

	const FullContainerKey *getContainerKey() {
		return containerKey_;
	}

	int32_t getProtocolVersion() const;

	SQLVariableSizeGlobalAllocator &getGlobalAllocator();

	void checkCondition(
			uint32_t currentTimer,
			int32_t timeoutInterval = DEFAULT_TIMEOUT_INTERVAL);

	void setForceAbort() {
		forceAbort_ = true;
	}

	void setExecution(SQLExecution *execution) {
		execution_ = execution;
	}

	SQLExecution *getExecution() {
		return  execution_;
	}

	util::StackAllocator &getAllocator() {
		return alloc_;
	}

	ClientId &getNoSQLClientId() {
		return nosqlClientId_;
	}
	
	void setNoSQLClientId(ClientId &clientId) {
		nosqlClientId_ = clientId;
	}

	void setNoSQLAbort(SessionId sessionId);

	void createNoSQLClientId();
	
	void sendMessage(EventEngine &ee, Event &request);

	EventContext *getEvetnContext() {
		return ec_;
	}
	void setRequestOption(
			StatementMessage::Request &requset);

	util::Vector<IndexInfo> &getCompositeIndex();

	SchemaVersionId getVersionId() {
		return versionId_;
	}

protected:

	static const int32_t DEFAULT_WAIT_INTERVAL = 15 * 1000;
	static const int32_t DEFAULT_TIMEOUT_INTERVAL = 60 * 5 * 1000;

	bool enableCommitOrAbort() {
		return (txnMode_ == TransactionManager::NO_AUTO_COMMIT_BEGIN
			&& isBegin_ == true);
	}

	void initializeBits(size_t columnCount) {
		if (nullStatisticsList_.empty()) {
			const uint8_t unitBits = 0;
			nullStatisticsList_.assign(
					(columnCount + CHAR_BIT - 1) / CHAR_BIT, unitBits);
		}
	}

	bool checkTimeoutEvent(
			uint32_t currentTimer,
			uint32_t timeoutInterval = DEFAULT_TIMEOUT_INTERVAL) {

		switch (eventType_) {
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

	void updateCondition();
	
	bool isSetClientId() {
		return (nosqlClientId_.sessionId_ != 0);
	}

	bool isRetryEvent() {
		return (eventType_ == SQL_GET_CONTAINER
			|| eventType_ == QUERY_TQL
			|| eventType_ == CREATE_TRANSACTION_CONTEXT
			|| eventType_ == CLOSE_SESSION
			|| eventType_ == GET_PARTITION_CONTAINER_NAMES);
	}

	void checkException(EventType type, std::exception &e);

	EventContext *ec_;
	util::StackAllocator &alloc_;
	NoSQLSyncContext *context_;
	EventType eventType_;
	TransactionManager::TransactionMode txnMode_;
	TransactionManager::GetMode getMode_;
	int32_t waitInterval_;
	bool isBegin_;

	ContainerAttribute attribute_;
	ContainerType containerType_;
	bool hasRowKey_;
	bool founded_;
	bool setted_;
	util::String sqlString_;
	NodeId nodeId_;
	ContainerId containerId_;
	SchemaVersionId versionId_;
	PartitionId txnPId_;
	StatementId stmtId_;
	PartitionRevisionNo partitionRevNo_;
	bool forceAbort_;
	SQLExecution *execution_;
	SessionId currentSessionId_;

	bool isCaseSensitive_;
	const KeyConstraint keyConstraint_;
	EventMonotonicTime startTime_;
	ClientId nosqlClientId_;
	util::String containerName_;
	FullContainerKey *containerKey_;

	struct NoSQLColumn {

		NoSQLColumn(util::StackAllocator &alloc) :
				alloc_(alloc),
				name_(alloc),
				type_(0),
				option_(0),
				tupleType_(TupleList::TYPE_NULL) {}

		util::StackAllocator &alloc_;
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
};

class NoSQLContainer : public NoSQLContainerBase {

	typedef int32_t AccessControlType;
	static const int32_t DEFAULT_DUMP_INTERVAL;

public:
	struct OptionalRequest;

	NoSQLContainer(EventContext &ec,
			const NameWithCaseSensitivity &containerName,
			NoSQLSyncContext &context,
			SQLExecution *execution);

	NoSQLContainer(EventContext &ec,
			const NameWithCaseSensitivity &containerName,
			FullContainerKey *containerKey,
			NoSQLSyncContext &context,
			SQLExecution *execution);

	NoSQLContainer(EventContext &ec,
			ContainerId containerId,
			SchemaVersionId versionId,
			PartitionId pId,
			NoSQLSyncContext &context,
			SQLExecution *execution);

	NoSQLContainer(EventContext &ec,
			PartitionId pId,
			NoSQLSyncContext &context,
			SQLExecution *execution);

	~NoSQLContainer();

	void encodeRequestId(
			util::StackAllocator &alloc,
			Event &request,
			NoSQLRequestId requestId);
	
	void getContainerInfo(NoSQLStoreOption &option);

	void getContainerBinary(NoSQLStoreOption &option,
			util::XArray<uint8_t> &response);

	void getContainerCount(
			uint64_t &count, NoSQLStoreOption &option);

	void putContainer(
			TableExpirationSchemaInfo &info,
			util::XArray<uint8_t> &binarySchemaInfo, 
			bool modifiable,
			NoSQLStoreOption &option);
	
	void putLargeContainer(
			util::XArray<uint8_t> &binarySchemaInfo,
			bool modifiable, NoSQLStoreOption &option, 
			util::XArray<uint8_t> *containerSchema = NULL,
			util::XArray<uint8_t> *binaryData = NULL);

	void updateLargeContainerStatus(
			LargeContainerStatusType partitionStatus,
			NodeAffinityNumber pos,
			NoSQLStoreOption &option,
			LargeExecStatus &execStatus,
			IndexInfo *indexInfo);
	
	bool createLargeIndex(
			const char *indexName,
			const char *columnName,
			MapType mapType,
			uint32_t columnId,
			NoSQLStoreOption &option);

	bool createLargeIndex(
			const char *indexName,
			util::Vector<util::String> &columnNameList,
			MapType mapType,
			util::Vector<ColumnId> &columneIdList,
			NoSQLStoreOption &option);

	void updateContainerProperty(
			TablePartitioningVersionId versionId,
			NoSQLStoreOption &option);

	void dropContainer(NoSQLStoreOption &option);
	
	void putRow(
			util::XArray<uint8_t> &fixedPart,
			util::XArray<uint8_t> &varPart,
			RowId rowId,
			NoSQLStoreOption &option);

	void putRowSet(
			util::XArray<uint8_t> &fixedPart,
			util::XArray<uint8_t> &varPart,
			uint64_t numRows,
			NoSQLStoreOption &option);

	void updateRow(
			RowId rowId,
			util::XArray<uint8_t> &rowData,
			NoSQLStoreOption &option);

	void deleteRowSet(
			util::XArray<RowId> &rowIdList,
			NoSQLStoreOption &option);

	void updateRowSet(
			util::XArray<uint8_t> &fixedPart,
			util::XArray<uint8_t> &varPart,
			util::XArray<RowId> &rowIdList,
			NoSQLStoreOption &option);

	void executeSyncQuery(
			const char *query,
			NoSQLStoreOption &option,
			util::XArray<uint8_t> &fixedPart,
			util::XArray<uint8_t> &varPart,
			int64_t &rowCount,
			bool &hasRowKey);

	void createIndex(
			const char *indexName,
			MapType mapType,
			uint32_t columnId,
			NoSQLStoreOption &option);

	void createIndex(
			const char *indexName,
			MapType mapType,
			util::Vector<uint32_t> &columnIds,
			NoSQLStoreOption &option);

	void dropIndex(const char *indexName,
			NoSQLStoreOption &option);

	void createDatabase(const char *dbName,
			NoSQLStoreOption &option);

	void dropDatabase(const char *dbName,
			NoSQLStoreOption &option);

	void grant(
			const char *userName,
			const char *dbName,
			AccessControlType type,
			NoSQLStoreOption &option);
	
	void revoke(
			const char *userName,
			const char *dbName,
			AccessControlType type,
			NoSQLStoreOption &option);
	
	void putUser(
			const char *userName,
			const char *password,
			bool modifiable,
			NoSQLStoreOption &option);
	
	void dropUser(const char *userName, NoSQLStoreOption &option);
	
	void createTransactionContext(NoSQLStoreOption &option);
	
	void commit(NoSQLStoreOption &option);
	
	void abort(NoSQLStoreOption &option);
	
	void closeContainer(NoSQLStoreOption &option);
	
private:

	void start();

	bool executeCommand(
			Event &request,
			bool isSync,
			util::XArray<uint8_t> &response);

	void encodeFixedPart(
			EventByteOutStream &out,
			EventType type);

	void encodeOptionPart(
			EventByteOutStream &out,
			EventType type,
			NoSQLStoreOption &option);

	void execPrivilegeProcessInternal(
			EventType eventType,
			const char *userName,
			const char *dbName,
			AccessControlType type,
			NoSQLStoreOption &option);

	void commitOrRollbackInternal(
			EventType eventType,
			NoSQLStoreOption &option);

	void execDatabaseProcInternal(
			const char *dbName,
			NoSQLStoreOption &option);

	void refreshPartition();

	bool withAllocate_;
};

struct NoSQLContainer::OptionalRequest {
	
	static const StatementMessage::OptionType
			OPTION_TYPE_HUGE_NOSQL = 11000;
	
	static const StatementMessage::OptionType
			OPTION_TYPE_HUGE_NEWSQL = 12000;
	
	OptionalRequest(util::StackAllocator &alloc) :
			alloc_(alloc),
			optionSet_(alloc),
			transactionTimeout_(-1),
			forUpdate_(false),
			systemMode_(false),
			dbName_(alloc),
			containerAttribute_(-1),
			putRowOption_(0),
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
			createDropIndexMode_(
					StatementMessage::INDEX_MODE_SQL_DEFAULT),
			currentSessionId_(0),
			isRetry_(false),
			baseValue_(0),
			requestId_(UNDEF_NOSQL_REQUESTID) {
	}
	~OptionalRequest() {}

	void format(EventByteOutStream &reqOut);

	static void putType(
			EventByteOutStream &reqOut,
			StatementMessage::OptionType type);

	void set(StatementMessage::OptionType type);
	void clear();

	util::StackAllocator &alloc_;
	util::Set<StatementMessage::OptionType> optionSet_;
	int32_t transactionTimeout_;
	bool forUpdate_;
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
	uint8_t *uuid_;
	SessionId queryId_;
	StatementMessage::UserType userType_;
	DatabaseId dbVersionId_;
	bool isSync_;
	int32_t subContainerId_;
	JobExecutionId execId_;
	uint8_t jobVersionId_;
	EventType ackEventType_;
	const char *indexName_;
	StatementMessage::CreateDropIndexMode createDropIndexMode_;
	SessionId currentSessionId_;
	bool isRetry_;
	StatementMessage::CaseSensitivity caseSensitivity_;
	int64_t baseValue_;
	NoSQLRequestId requestId_;
};

#endif
