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
	@brief Implementation of TransactionService
*/
#include "transaction_service.h"

#include "gs_error.h"

#include "log_manager.h"
#include "transaction_context.h"
#include "transaction_manager.h"
#include "cluster_manager.h"
#include "database_manager.h"

#include "util/container.h"
#include "sql_execution.h"
#include "sql_utils.h"

#include "key_data_store.h"
#include "interchangeable.h"

template<>
void StatementMessage::OptionSet::encode<>(EventByteOutStream& out) const;
template<>
void StatementMessage::OptionSet::encode<>(OutStream& out) const;

template<typename T>
void decodeLargeRow(
	const char* key, util::StackAllocator& alloc, TransactionContext& txn,
	DataStoreV4* dataStore, const char8_t* dbName, BaseContainer* container,
	T& record, const EventMonotonicTime emNow);

void decodeLargeBinaryRow(
	const char* key, util::StackAllocator& alloc, TransactionContext& txn,
	DataStoreV4* dataStore, const char* dbName, BaseContainer* container,
	util::XArray<uint8_t>& record, const EventMonotonicTime emNow);


#include "sql_service.h"
#include "nosql_command.h"

#define TEST_PRINT(s)
#define TEST_PRINT1(s, d)

#define TRANSACTION_EVENT_START(ec, ev, mgr, conn, clientRequest) \
	EVENT_START(ec, ev, mgr,conn.dbId_, clientRequest); \
	if (checkConstraint(ev, conn)) return;


#define TRANSACTION_EVENT_START_FOR_UPDATE(ec, ev, mgr, conn, clientRequest, isUpdate) \
	EVENT_START(ec, ev, mgr,conn.dbId_, clientRequest); \
	if (checkConstraint(ev, conn, isUpdate)) return;

#include "base_container.h"
#include "data_store_v4.h"
#include "message_row_store.h"  
#include "message_schema.h"
#include "query_processor.h"
#include "result_set.h"
#include "meta_store.h"

#include "cluster_service.h"
#include "partition_table.h"
#include "recovery_manager.h"
#include "system_service.h"

extern std::string dumpUUID(uint8_t* uuid);

#ifndef _WIN32
#include <signal.h>  
#endif



#define TXN_THROW_DENY_ERROR(errorCode, message) \
	GS_THROW_CUSTOM_ERROR(DenyException, errorCode, message)

#define TXN_TRACE_HANDLER_CALLED(ev)               \
	UTIL_TRACE_DEBUG(TRANSACTION_SERVICE,          \
		"handler called. (type=" <<  EventTypeUtility::getEventTypeName(ev.getType()) \
							<< "[" << ev.getType() << "]" \
							<< ", nd=" << ev.getSenderND() \
							<< ", pId=" << ev.getPartitionId() << ")")

#define TXN_TRACE_HANDLER_CALLED_END(ev)               \
	UTIL_TRACE_DEBUG(TRANSACTION_SERVICE,          \
		"handler finished. (type=" << EventTypeUtility::getEventTypeName(ev.getType()) \
							<< "[" << ev.getType() << "]" \
							<< ", nd=" << ev.getSenderND() \
							<< ", pId=" << ev.getPartitionId() << ")")

UTIL_TRACER_DECLARE(IO_MONITOR);
UTIL_TRACER_DECLARE(TRANSACTION_DETAIL);
UTIL_TRACER_DECLARE(DISTRIBUTED_FRAMEWORK);
UTIL_TRACER_DECLARE(DATA_EXPIRATION_DETAIL);

AUDIT_TRACER_DECLARE(AUDIT_NOSQL_READ);
AUDIT_TRACER_DECLARE(AUDIT_NOSQL_WRITE);
AUDIT_TRACER_DECLARE(AUDIT_CONNECT);

#define AUDIT_TRACE_CHECK   0

#define AUDIT_TRACE_INFO_OPERATOR(objectname)  { \
}
#define AUDIT_TRACE_ERROR_OPERATOR(objectname)  { \
}
#define AUDIT_TRACE_ERROR_EXECUTE(objectname)  { \
}

template<>
template<typename S>
SQLTableInfo* StatementMessage::CustomOptionCoder<SQLTableInfo*, 0>::decode(
	S& in, util::StackAllocator& alloc) const {
	return SQLExecution::decodeTableInfo(alloc, in);
}

template<>
template<typename S>
inline void StatementMessage::CustomOptionCoder<
	StatementMessage::CompositeIndexInfos*, 0>::encode(
		S& out, const ValueType& value) const {
	assert(value != NULL);
	const size_t size = value->indexInfoList_.size();
	out << static_cast<uint64_t>(size);
	for (size_t pos = 0; pos < size; pos++) {
		StatementHandler::encodeIndexInfo(out, value->indexInfoList_[pos]);
	}
}

template<>
template<typename S>
inline StatementMessage::CompositeIndexInfos*
StatementMessage::CustomOptionCoder<
	StatementMessage::CompositeIndexInfos*, 0>::decode(
		S& in, util::StackAllocator& alloc) const {
	CompositeIndexInfos* value = ALLOC_NEW(alloc) CompositeIndexInfos(alloc);
	uint64_t size;
	in >> size;
	value->indexInfoList_.resize(static_cast<size_t>(size), IndexInfo(alloc));
	for (size_t pos = 0; pos < size; pos++) {
		StatementHandler::decodeIndexInfo(in, value->indexInfoList_[pos]);
	}
	return value;
}


const StatementHandler::ClusterRole StatementHandler::CROLE_UNKNOWN = 0;
const StatementHandler::ClusterRole StatementHandler::CROLE_SUBMASTER = 1 << 2;
const StatementHandler::ClusterRole StatementHandler::CROLE_MASTER = 1 << 0;
const StatementHandler::ClusterRole StatementHandler::CROLE_FOLLOWER = 1 << 1;
const StatementHandler::ClusterRole StatementHandler::CROLE_ANY = ~0;
const char8_t* const StatementHandler::clusterRoleStr[8] = { "UNKNOWN",
	"MASTER", "FOLLOWER", "MASTER, FOLLOWER", "SUB_CLUSTER",
	"MASTER, SUB_CLUSTER", "FOLLOWER, SUB_CLUSTER",
	"MASTER, FOLLOWER, SUB_CLUSTER"
};

const StatementHandler::PartitionRoleType StatementHandler::PROLE_UNKNOWN = 0;
const StatementHandler::PartitionRoleType StatementHandler::PROLE_OWNER = 1
<< 0;
const StatementHandler::PartitionRoleType StatementHandler::PROLE_BACKUP = 1
<< 1;
const StatementHandler::PartitionRoleType StatementHandler::PROLE_CATCHUP =
1 << 2;
const StatementHandler::PartitionRoleType StatementHandler::PROLE_NONE = 1 << 3;
const StatementHandler::PartitionRoleType StatementHandler::PROLE_ANY = ~0;
const char8_t* const StatementHandler::partitionRoleTypeStr[16] = { "UNKNOWN",
	"OWNER", "BACKUP", "OWNER, BACKUP", "CATCHUP", "OWNER, CATCHUP",
	"BACKUP, CATCHUP", "OWNER, BACKUP, CATCHUP", "NONE", "OWNER, NONE",
	"BACKUP, NONE", "OWNER, BACKUP, NONE", "CATCHUP, NONE",
	"OWNER, CATCHUP, NONE", "BACKUP, CATCHUP, NONE",
	"OWNER, BACKUP, CATCHUP, NONE"
};

const StatementHandler::PartitionStatus StatementHandler::PSTATE_UNKNOWN = 0;
const StatementHandler::PartitionStatus StatementHandler::PSTATE_ON = 1 << 0;
const StatementHandler::PartitionStatus StatementHandler::PSTATE_SYNC = 1 << 1;
const StatementHandler::PartitionStatus StatementHandler::PSTATE_OFF = 1 << 2;
const StatementHandler::PartitionStatus StatementHandler::PSTATE_STOP = 1 << 3;
const StatementHandler::PartitionStatus StatementHandler::PSTATE_ANY = ~0;
const char8_t* const StatementHandler::partitionStatusStr[16] = { "UNKNOWN",
	"ON", "SYNC", "ON, SYNC", "OFF", "ON, OFF", "SYNC, OFF", "ON, SYNC, OFF",
	"STOP", "ON, STOP", "SYNC, STOP", "ON, SYNC, STOP", "OFF, STOP",
	"ON, OFF, STOP", "SYNC, OFF, STOP", "ON, SYNC, OFF, STOP"
};

const StatementHandler::StatementExecStatus
StatementHandler::TXN_STATEMENT_SUCCESS = 0;
const StatementHandler::StatementExecStatus
StatementHandler::TXN_STATEMENT_ERROR = 1;
const StatementHandler::StatementExecStatus
StatementHandler::TXN_STATEMENT_NODE_ERROR = 2;
const StatementHandler::StatementExecStatus
StatementHandler::TXN_STATEMENT_DENY = 3;
const StatementHandler::StatementExecStatus
StatementHandler::TXN_STATEMENT_SUCCESS_BUT_REPL_TIMEOUT =
TXN_STATEMENT_SUCCESS;

const StatementHandler::ProtocolVersion
StatementHandler::PROTOCOL_VERSION_UNDEFINED = 0;
const StatementHandler::ProtocolVersion
StatementHandler::TXN_V1_0_X_CLIENT_VERSION = 1;
const StatementHandler::ProtocolVersion
StatementHandler::TXN_V1_1_X_CLIENT_VERSION = TXN_V1_0_X_CLIENT_VERSION;
const StatementHandler::ProtocolVersion
StatementHandler::TXN_V1_5_X_CLIENT_VERSION = 2;
const StatementHandler::ProtocolVersion
StatementHandler::TXN_V2_0_X_CLIENT_VERSION = 3;
const StatementHandler::ProtocolVersion
StatementHandler::TXN_V2_1_X_CLIENT_VERSION = 4;
const StatementHandler::ProtocolVersion
StatementHandler::TXN_V2_5_X_CLIENT_VERSION = 5;
const StatementHandler::ProtocolVersion
StatementHandler::TXN_V2_7_X_CLIENT_VERSION = 6;
const StatementHandler::ProtocolVersion
StatementHandler::TXN_V2_8_X_CLIENT_VERSION = 7;
const StatementHandler::ProtocolVersion
StatementHandler::TXN_V2_9_X_CLIENT_VERSION = 8;
const StatementHandler::ProtocolVersion
StatementHandler::TXN_V3_0_X_CLIENT_VERSION = 9;
const StatementHandler::ProtocolVersion
StatementHandler::TXN_V3_0_X_CE_CLIENT_VERSION = 10;
const StatementHandler::ProtocolVersion
StatementHandler::TXN_V3_1_X_CLIENT_VERSION = 11;
const StatementHandler::ProtocolVersion
StatementHandler::TXN_V3_2_X_CLIENT_VERSION = 12;
const StatementHandler::ProtocolVersion
StatementHandler::TXN_V3_5_X_CLIENT_VERSION = 13;
const StatementHandler::ProtocolVersion
StatementHandler::TXN_V4_0_0_CLIENT_VERSION = 14;
const StatementHandler::ProtocolVersion
StatementHandler::TXN_V4_0_1_CLIENT_VERSION = 15;
const StatementHandler::ProtocolVersion StatementHandler::TXN_CLIENT_VERSION =
TXN_V4_0_1_CLIENT_VERSION;

static const StatementHandler::ProtocolVersion
ACCEPTABLE_NOSQL_CLIENT_VERSIONS[] = {
	StatementHandler::TXN_CLIENT_VERSION,
	StatementHandler::PROTOCOL_VERSION_UNDEFINED /* sentinel */
};

StatementHandler::StatementHandler()
	: clusterService_(NULL),
	clusterManager_(NULL),
	partitionList_(NULL),
	partitionTable_(NULL),
	transactionService_(NULL),
	transactionManager_(NULL),
	systemService_(NULL),
	recoveryManager_(NULL)
	,
	sqlService_(NULL),
	isNewSQL_(false)
{}

StatementHandler::~StatementHandler() {}

void StatementHandler::initialize(const ManagerSet& resourceSet, bool isNewSQL) {

	resourceSet_ = &resourceSet;

	clusterService_ = resourceSet.clsSvc_;
	clusterManager_ = resourceSet.clsMgr_;
	partitionList_ = resourceSet.partitionList_;
	partitionTable_ = resourceSet.pt_;
	transactionService_ = resourceSet.txnSvc_;
	transactionManager_ = resourceSet.txnMgr_;
	systemService_ = resourceSet.sysSvc_;
	recoveryManager_ = resourceSet.recoveryMgr_;
	sqlService_ = resourceSet.sqlSvc_;
	isNewSQL_ = isNewSQL;
	if (isNewSQL) {
		transactionManager_ = sqlService_->getTransactionManager();
	}
	dsConfig_ = resourceSet.dsConfig_;

	const size_t forNonSystem = 0;
	const size_t forSystem = 1;
	const size_t forNonPartitioning = 0;
	const size_t forPartitioning = 1;
	const size_t noCheckLength = 0;
	const size_t checkLength = 1;

	for (size_t i = forNonSystem; i <= forSystem; i++) {
		for (size_t j = forNonPartitioning; j <= forPartitioning; j++) {
			for (size_t k = noCheckLength; k <= checkLength; k++) {
				keyConstraint_[i][j][k].systemPartAllowed_ = (i == forSystem);
				keyConstraint_[i][j][k].largeContainerIdAllowed_ = (j == forPartitioning);
				keyConstraint_[i][j][k].maxTotalLength_ = (k == checkLength ?
					dsConfig_->getLimitContainerNameSize() : std::numeric_limits<uint32_t>::max());
			}
		}
	}
}

/*!
	@brief Sets the information about success reply to an client
*/
void StatementHandler::setSuccessReply(
	util::StackAllocator& alloc, Event& ev, StatementId stmtId,
	StatementExecStatus status, const Response& response,
	const Request& request)
{
	try {
		EventByteOutStream out = encodeCommonPart(ev, stmtId, status);
		EventType eventType = ev.getType();
		if (isNewSQL(request)) {
			OptionSet optionSet(alloc);
			if (response.compositeIndexInfos_) {
				optionSet.set<Options::COMPOSITE_INDEX>(response.compositeIndexInfos_);
			}
			setReplyOption(optionSet, request);
			optionSet.encode(out);
			eventType = request.fixed_.stmtType_;
		}

		switch (eventType) {
		case CONNECT:
		case DISCONNECT:
		case LOGOUT:
		case DROP_CONTAINER:
		case CREATE_TRANSACTION_CONTEXT:
		case CLOSE_TRANSACTION_CONTEXT:
		case CREATE_INDEX:
		case CONTINUE_CREATE_INDEX:
		case DELETE_INDEX:
		case CREATE_TRIGGER:
		case DELETE_TRIGGER:
		case FLUSH_LOG:
		case UPDATE_ROW_BY_ID:
		case REMOVE_ROW_BY_ID:
		case REMOVE_MULTIPLE_ROWS_BY_ID_SET:
		case UPDATE_MULTIPLE_ROWS_BY_ID_SET:
		case COMMIT_TRANSACTION:
		case ABORT_TRANSACTION:
		case CLOSE_MULTIPLE_TRANSACTION_CONTEXTS:
		case PUT_MULTIPLE_CONTAINER_ROWS:
		case CLOSE_RESULT_SET:
		case PUT_USER:
		case DROP_USER:
		case PUT_DATABASE:
		case DROP_DATABASE:
		case PUT_PRIVILEGE:
		case DROP_PRIVILEGE:
		case SQL_GET_CONTAINER:
		case SQL_RECV_SYNC_REQUEST:
			if (response.schemaMessage_) {
				sqlService_->encode(ev, *response.schemaMessage_);
			}
			break;
		case LOGIN:
			if (response.connectionOption_ != NULL) {
				ConnectionOption& connOption = *response.connectionOption_;
				encodeIntData<EventByteOutStream, int8_t>(out, connOption.authMode_);
				encodeIntData<EventByteOutStream, DatabaseId>(out, connOption.dbId_);
			}
			break;
		case SQL_ACK_GET_CONTAINER:
			break;
		case GET_PARTITION_ADDRESS:
			encodeBinaryData<EventByteOutStream>(
				out, response.binaryData_.data(), response.binaryData_.size());
			break;
		case GET_USERS: {
			out << static_cast<uint32_t>(response.userInfoList_.size());
			for (size_t i = 0; i < response.userInfoList_.size(); i++) {
				encodeStringData<EventByteOutStream>(out, response.userInfoList_[i]->userName_);
				out << response.userInfoList_[i]->property_;
				encodeBooleanData<EventByteOutStream>(out, response.userInfoList_[i]->withDigest_);
				encodeStringData<EventByteOutStream>(out, response.userInfoList_[i]->digest_);
			}
		} break;
		case GET_DATABASES: {
			out << static_cast<uint32_t>(response.databaseInfoList_.size());
			for (size_t i = 0; i < response.databaseInfoList_.size(); i++) {
				encodeStringData<EventByteOutStream>(out, response.databaseInfoList_[i]->dbName_);
				out << response.databaseInfoList_[i]->property_;
				out << static_cast<uint32_t>(
					response.databaseInfoList_[i]
					->privilegeInfoList_.size());  
				for (size_t j = 0;
					j <
					response.databaseInfoList_[i]->privilegeInfoList_.size();
					j++) {
					encodeStringData<EventByteOutStream>(
						out, response.databaseInfoList_[i]
						->privilegeInfoList_[j]
						->userName_);
					encodeStringData<EventByteOutStream>(
						out, response.databaseInfoList_[i]
						->privilegeInfoList_[j]
						->privilege_);
				}
			}
		} break;
		case GET_PARTITION_CONTAINER_NAMES: {
			out << response.containerNum_;
			out << static_cast<uint32_t>(response.containerNameList_.size());
			for (size_t i = 0; i < response.containerNameList_.size(); i++) {
				encodeContainerKey(out, response.containerNameList_[i]);
			}
		} break;

		case GET_CONTAINER_PROPERTIES:
			ev.addExtraMessage(
				response.binaryData_.data(), response.binaryData_.size());
			break;

		case GET_CONTAINER:
			encodeBooleanData<EventByteOutStream>(out, response.existFlag_);
			if (response.existFlag_) {
				out << response.schemaVersionId_;
				out << response.containerId_;
				encodeVarSizeBinaryData<EventByteOutStream>(out, response.binaryData2_.data(),
					response.binaryData2_.size());
				encodeBinaryData<EventByteOutStream>(out, response.binaryData_.data(),
					response.binaryData_.size());
				{
					int32_t containerAttribute =
						static_cast<int32_t>(response.containerAttribute_);
					out << containerAttribute;
				}
			}
			break;
		case PUT_LARGE_CONTAINER:
		case PUT_CONTAINER:
		case CONTINUE_ALTER_CONTAINER:
			out << response.schemaVersionId_;
			out << response.containerId_;
			encodeVarSizeBinaryData<EventByteOutStream>(out, response.binaryData2_.data(),
				response.binaryData2_.size());
			encodeBinaryData<EventByteOutStream>(
				out, response.binaryData_.data(), response.binaryData_.size());
			{
				int32_t containerAttribute =
					static_cast<int32_t>(response.containerAttribute_);
				out << containerAttribute;
			}
			break;

		case FETCH_RESULT_SET:
			assert(response.rs_ != NULL);
			{
				encodeBooleanData<EventByteOutStream>(out, response.rs_->isRelease());
				out << response.rs_->getVarStartPos();
				out << response.rs_->getFetchNum();
				ev.addExtraMessage(response.rs_->getFixedStartData(),
					response.rs_->getFixedOffsetSize());
				ev.addExtraMessage(response.rs_->getVarStartData(),
					response.rs_->getVarOffsetSize());
			}
			break;

		case GET_ROW:
		case GET_TIME_SERIES_ROW_RELATED:
		case INTERPOLATE_TIME_SERIES_ROW:
			assert(response.rs_ != NULL);
			encodeBooleanData<EventByteOutStream>(out, response.existFlag_);
			if (response.existFlag_) {
				encodeBinaryData<EventByteOutStream>(out, response.rs_->getFixedStartData(),
					response.rs_->getFixedOffsetSize());
				encodeBinaryData<EventByteOutStream>(out, response.rs_->getVarStartData(),
					response.rs_->getVarOffsetSize());
			}
			break;

		case QUERY_TQL:
		case QUERY_COLLECTION_GEOMETRY_RELATED:
		case QUERY_COLLECTION_GEOMETRY_WITH_EXCLUSION:
		case QUERY_TIME_SERIES_RANGE:
		case QUERY_TIME_SERIES_SAMPLING:
			assert(response.rs_ != NULL);
			if (isNewSQL(request)) {
				encodeEnumData<EventByteOutStream, ResultType>(out, response.rs_->getResultType());
				out << response.rs_->getResultNum();
				out << response.rs_->getFixedOffsetSize();
				encodeBinaryData<EventByteOutStream>(out, response.rs_->getFixedStartData(), static_cast<size_t>(response.rs_->getFixedOffsetSize()));
				out << response.rs_->getVarOffsetSize();
				encodeBinaryData<EventByteOutStream>(out, response.rs_->getVarStartData(), static_cast<size_t>(response.rs_->getVarOffsetSize()));
				break;
			}
			switch (response.rs_->getResultType()) {
			case RESULT_ROWSET: {
				PartialQueryOption partialQueryOption(alloc);
				response.rs_->getQueryOption().encodePartialQueryOption(alloc, partialQueryOption);

				const size_t entryNumPos = out.base().position();
				int32_t entryNum = 0;
				out << entryNum;

				if (!partialQueryOption.empty()) {
					encodeEnumData<EventByteOutStream, QueryResponseType>(out, PARTIAL_EXECUTION_STATE);

					const size_t entrySizePos = out.base().position();
					int32_t entrySize = 0;
					out << entrySize;

					encodeBooleanData<EventByteOutStream>(out, true);
					entrySize += static_cast<int32_t>(sizeof(int8_t));

					int32_t partialEntryNum = static_cast<int32_t>(partialQueryOption.size());
					out << partialEntryNum;
					entrySize += static_cast<int32_t>(sizeof(int32_t));

					PartialQueryOption::const_iterator itr;
					for (itr = partialQueryOption.begin(); itr != partialQueryOption.end(); itr++) {
						encodeIntData<EventByteOutStream, int8_t>(out, itr->first);
						entrySize += static_cast<int32_t>(sizeof(int8_t));

						int32_t partialEntrySize =
								static_cast<int32_t>(itr->second->size());
						out << partialEntrySize;
						entrySize += static_cast<int32_t>(sizeof(int32_t));
						encodeBinaryData<EventByteOutStream>(out, itr->second->data(), itr->second->size());
						entrySize += static_cast<int32_t>(itr->second->size());
					}
					const size_t entryLastPos = out.base().position();
					out.base().position(entrySizePos);
					out << entrySize;
					out.base().position(entryLastPos);
					entryNum++;
				}

				entryNum += encodeDistributedResult<EventByteOutStream>(out, *response.rs_, NULL);

				{
					encodeEnumData<EventByteOutStream, QueryResponseType>(out, ROW_SET);

					const int32_t entrySize = static_cast<int32_t>(
							sizeof(ResultSize) +
							response.rs_->getFixedOffsetSize() +
							response.rs_->getVarOffsetSize());
					out << entrySize;
					out << response.rs_->getFetchNum();
					ev.addExtraMessage(response.rs_->getFixedStartData(),
						response.rs_->getFixedOffsetSize());
					ev.addExtraMessage(response.rs_->getVarStartData(),
						response.rs_->getVarOffsetSize());
					entryNum++;
				}

				const size_t lastPos = out.base().position();
				out.base().position(entryNumPos);
				out << entryNum;
				out.base().position(lastPos);
			} break;
			case RESULT_EXPLAIN: {
				const size_t entryNumPos = out.base().position();
				int32_t entryNum = 0;
				out << entryNum;

				entryNum += encodeDistributedResult<EventByteOutStream>(out, *response.rs_, NULL);

				encodeEnumData<EventByteOutStream, QueryResponseType>(out, QUERY_ANALYSIS);
				const int32_t entrySize = static_cast<int32_t>(
						sizeof(ResultSize) +
						response.rs_->getFixedOffsetSize() +
						response.rs_->getVarOffsetSize());
				out << entrySize;

				out << response.rs_->getResultNum();
				ev.addExtraMessage(response.rs_->getFixedStartData(),
					response.rs_->getFixedOffsetSize());
				ev.addExtraMessage(response.rs_->getVarStartData(),
					response.rs_->getVarOffsetSize());
				entryNum++;

				const size_t lastPos = out.base().position();
				out.base().position(entryNumPos);
				out << entryNum;
				out.base().position(lastPos);
			} break;
			case RESULT_AGGREGATE: {
				int32_t entryNum = 1;
				out << entryNum;

				encodeEnumData<EventByteOutStream, QueryResponseType>(out, AGGREGATION);
				const util::XArray<uint8_t>* rowDataFixedPart =
					response.rs_->getRowDataFixedPartBuffer();
				const int32_t entrySize = static_cast<int32_t>(
						sizeof(ResultSize) + rowDataFixedPart->size());
				out << entrySize;

				out << response.rs_->getResultNum();
				encodeBinaryData<EventByteOutStream>(
					out, rowDataFixedPart->data(), rowDataFixedPart->size());
			} break;
			case PARTIAL_RESULT_ROWSET: {
				int32_t entryNum = 2;
				out << entryNum;
				{
					encodeEnumData<EventByteOutStream, QueryResponseType>(out, PARTIAL_FETCH_STATE);
					int32_t entrySize = sizeof(ResultSize) + sizeof(ResultSetId);
					out << entrySize;

					out << response.rs_->getResultNum();
					out << response.rs_->getId();
				}
				{
					encodeEnumData<EventByteOutStream, QueryResponseType>(out, ROW_SET);
					const int32_t entrySize = static_cast<int32_t>(
							sizeof(ResultSize) +
							response.rs_->getFixedOffsetSize() +
							response.rs_->getVarOffsetSize());
					out << entrySize;

					out << response.rs_->getFetchNum();
					ev.addExtraMessage(response.rs_->getFixedStartData(),
						response.rs_->getFixedOffsetSize());
					ev.addExtraMessage(response.rs_->getVarStartData(),
						response.rs_->getVarOffsetSize());
				}
			} break;
			default:
				GS_THROW_USER_ERROR(GS_ERROR_TXN_RESULT_TYPE_INVALID,
						"(resultType=" <<
						static_cast<int32_t>(response.rs_->getResultType()) << ")");
			}
			break;
		case UPDATE_CONTAINER_STATUS:
			out << response.currentStatus_;
			out << response.currentAffinity_;
			if (!response.indexInfo_.indexName_.empty()) {
				encodeIndexInfo(out, response.indexInfo_);
			}
			break;
		case CREATE_LARGE_INDEX:
			out << response.existIndex_;
			break;
		case DROP_LARGE_INDEX:
			break;
		case PUT_ROW:
		case PUT_MULTIPLE_ROWS:
		case REMOVE_ROW:
		case APPEND_TIME_SERIES_ROW:
			encodeBooleanData<EventByteOutStream>(out, response.existFlag_);
			break;

		case GET_MULTIPLE_ROWS:
			assert(response.rs_ != NULL);
			{
				out << response.last_;
				ev.addExtraMessage(response.rs_->getFixedStartData(),
					response.rs_->getFixedOffsetSize());
				ev.addExtraMessage(response.rs_->getVarStartData(),
					response.rs_->getVarOffsetSize());
			}
			break;

		case AGGREGATE_TIME_SERIES:
			assert(response.rs_ != NULL);
			encodeBooleanData<EventByteOutStream>(out, response.existFlag_);
			if (response.existFlag_) {
				const util::XArray<uint8_t>* rowDataFixedPart =
					response.rs_->getRowDataFixedPartBuffer();
				encodeBinaryData<EventByteOutStream>(
					out, rowDataFixedPart->data(), rowDataFixedPart->size());
			}
			break;

		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN,
				"(stmtType=" << ev.getType() << ")");
		}
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief Sets the information about error reply to an client
*/
void StatementHandler::setErrorReply(
	Event& ev, StatementId stmtId,
	StatementExecStatus status, const std::exception& exception,
	const NodeDescriptor& nd) {
	try {
		EventByteOutStream out = encodeCommonPart(ev, stmtId, status);

		encodeException(out, exception, nd);
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

StatementHandler::OptionSet* StatementHandler::getReplyOption(util::StackAllocator& alloc,
	const Request& request, CompositeIndexInfos* infos, bool isContinue) {
	bool isSQL = isNewSQL(request);
	if (!isSQL) {
		return NULL;
	}

	OptionSet* optionSet = ALLOC_NEW(alloc) OptionSet(alloc);

	optionSet->set<Options::UUID>(request.optional_.get<Options::UUID>());

	optionSet->set<Options::QUERY_ID>(
		request.optional_.get<Options::QUERY_ID>());
	optionSet->set<Options::FOR_SYNC>(
		request.optional_.get<Options::FOR_SYNC>());

	if (isContinue) {
		optionSet->set<Options::REPLY_PID>(
			request.optional_.get<Options::REPLY_PID>());
		optionSet->set<Options::REPLY_EVENT_TYPE>(
			request.optional_.get<Options::REPLY_EVENT_TYPE>());
	}

	if (!request.optional_.get<Options::FOR_SYNC>()) {
		optionSet->set<Options::ACK_EVENT_TYPE>(
			request.optional_.get<Options::ACK_EVENT_TYPE>());
		optionSet->set<Options::SUB_CONTAINER_ID>(
			request.optional_.get<Options::SUB_CONTAINER_ID>());
		optionSet->set<Options::JOB_EXEC_ID>(
			request.optional_.get<Options::JOB_EXEC_ID>());
		optionSet->set<Options::JOB_VERSION>(
			request.optional_.get<Options::JOB_VERSION>());
	}
	else {
		optionSet->set<Options::ACK_EVENT_TYPE>(request.fixed_.stmtType_);
		if (request.optional_.get<Options::NOSQL_SYNC_ID>() !=
			UNDEF_NOSQL_REQUESTID) {
			optionSet->set<Options::NOSQL_SYNC_ID>(
				request.optional_.get<Options::NOSQL_SYNC_ID>());
		}
	}
	optionSet->set<StatementMessage::Options::FEATURE_VERSION>(MessageSchema::V4_1_VERSION);
	if (infos != NULL) {
		optionSet->set<Options::COMPOSITE_INDEX>(infos);
	}
	return optionSet;
}

StatementHandler::OptionSet* StatementHandler::getReplyOption(
		util::StackAllocator& alloc, const ReplicationContext& replContext,
		CompositeIndexInfos* infos, bool isContinue) {
	UNUSED_VARIABLE(alloc);
	UNUSED_VARIABLE(replContext);
	UNUSED_VARIABLE(infos);
	UNUSED_VARIABLE(isContinue);

	return NULL;
}

void StatementHandler::setReplyOption(
	OptionSet& optionSet, const Request& request) {
	optionSet.set<Options::UUID>(request.optional_.get<Options::UUID>());

	optionSet.set<Options::QUERY_ID>(
		request.optional_.get<Options::QUERY_ID>());
	optionSet.set<Options::FOR_SYNC>(
		request.optional_.get<Options::FOR_SYNC>());

	if (!request.optional_.get<Options::FOR_SYNC>()) {
		optionSet.set<Options::ACK_EVENT_TYPE>(
			request.optional_.get<Options::ACK_EVENT_TYPE>());
		optionSet.set<Options::SUB_CONTAINER_ID>(
			request.optional_.get<Options::SUB_CONTAINER_ID>());
		optionSet.set<Options::JOB_EXEC_ID>(
			request.optional_.get<Options::JOB_EXEC_ID>());
		optionSet.set<Options::JOB_VERSION>(
			request.optional_.get<Options::JOB_VERSION>());
	}
	else {
		optionSet.set<Options::ACK_EVENT_TYPE>(request.fixed_.stmtType_);
		if (request.optional_.get<Options::NOSQL_SYNC_ID>() !=
			UNDEF_NOSQL_REQUESTID) {
			optionSet.set<Options::NOSQL_SYNC_ID>(
				request.optional_.get<Options::NOSQL_SYNC_ID>());
		}
	}
	optionSet.set<StatementMessage::Options::FEATURE_VERSION>(MessageSchema::V4_1_VERSION);
}

void StatementHandler::setReplyOption(
	OptionSet& optionSet, const ReplicationContext& replContext) {
	ClientId ackClientId;
	replContext.copyAckClientId(ackClientId);

	optionSet.set<Options::UUID>(UUIDObject(ackClientId.uuid_));

	optionSet.set<Options::QUERY_ID>(replContext.getQueryId());
	optionSet.set<Options::FOR_SYNC>(replContext.getSyncFlag());
	if (replContext.getSyncFlag()) {
		if (replContext.getSyncId() != UNDEF_NOSQL_REQUESTID) {
			optionSet.set<Options::NOSQL_SYNC_ID>(
				replContext.getSyncId());
		}
	}

	optionSet.set<Options::REPLY_PID>(replContext.getReplyPId());

	optionSet.set<Options::SUB_CONTAINER_ID>(
		replContext.getSubContainerId());
	optionSet.set<Options::JOB_EXEC_ID>(replContext.getExecId());
	optionSet.set<Options::JOB_VERSION>(replContext.getJobVersionId());
}

void StatementHandler::setReplyOptionForContinue(
	OptionSet& optionSet, const Request& request) {
	optionSet.set<Options::UUID>(request.optional_.get<Options::UUID>());

	optionSet.set<Options::QUERY_ID>(
		request.optional_.get<Options::QUERY_ID>());
	optionSet.set<Options::FOR_SYNC>(
		request.optional_.get<Options::FOR_SYNC>());

	optionSet.set<Options::REPLY_PID>(
		request.optional_.get<Options::REPLY_PID>());
	optionSet.set<Options::REPLY_EVENT_TYPE>(
		request.optional_.get<Options::REPLY_EVENT_TYPE>());
	optionSet.set<Options::DB_VERSION_ID>(
		request.optional_.get<Options::DB_VERSION_ID>());

	if (!request.optional_.get<Options::FOR_SYNC>()) {
		optionSet.set<Options::ACK_EVENT_TYPE>(
			request.optional_.get<Options::ACK_EVENT_TYPE>());
		optionSet.set<Options::SUB_CONTAINER_ID>(
			request.optional_.get<Options::SUB_CONTAINER_ID>());
		optionSet.set<Options::JOB_EXEC_ID>(
			request.optional_.get<Options::JOB_EXEC_ID>());
		optionSet.set<Options::JOB_VERSION>(
			request.optional_.get<Options::JOB_VERSION>());
	}
	else {
		optionSet.set<Options::ACK_EVENT_TYPE>(request.fixed_.stmtType_);
		if (request.optional_.get<Options::NOSQL_SYNC_ID>() !=
			UNDEF_NOSQL_REQUESTID) {
			optionSet.set<Options::NOSQL_SYNC_ID>(
				request.optional_.get<Options::NOSQL_SYNC_ID>());
		}
	}
}

void StatementHandler::setReplyOptionForContinue(
	OptionSet& optionSet, const ReplicationContext& replContext) {
	ClientId ackClientId;
	replContext.copyAckClientId(ackClientId);

	optionSet.set<Options::UUID>(UUIDObject(ackClientId.uuid_));

	optionSet.set<Options::QUERY_ID>(replContext.getQueryId());
	optionSet.set<Options::FOR_SYNC>(replContext.getSyncFlag());

	optionSet.set<Options::REPLY_PID>(replContext.getReplyPId());
	optionSet.set<Options::REPLY_EVENT_TYPE>(replContext.getReplyEventType());
	optionSet.set<Options::DB_VERSION_ID>(
		replContext.getConnectionND().isEmpty() ? 0 : replContext.getConnectionND().getUserData<ConnectionOption>().dbId_);

	if (!replContext.getSyncFlag()) {
		optionSet.set<Options::SUB_CONTAINER_ID>(
			replContext.getSubContainerId());
		optionSet.set<Options::JOB_EXEC_ID>(replContext.getExecId());
		optionSet.set<Options::JOB_VERSION>(replContext.getJobVersionId());
	}
}

void StatementHandler::setSQLResponseInfo(
	ReplicationContext& replContext, const Request& request) {
	ClientId ackClientId;
	request.optional_.get<Options::UUID>().get(ackClientId.uuid_);

	replContext.setSQLResponseInfo(
		request.optional_.get<Options::REPLY_PID>(),
		request.optional_.get<Options::REPLY_EVENT_TYPE>(),
		request.optional_.get<Options::QUERY_ID>(),
		request.fixed_.clientId_,
		request.optional_.get<Options::FOR_SYNC>(),
		request.optional_.get<Options::JOB_EXEC_ID>(),
		request.optional_.get<Options::SUB_CONTAINER_ID>(),
		ackClientId,
		request.optional_.get<Options::NOSQL_SYNC_ID>(),
		request.optional_.get<Options::JOB_VERSION>()
	);
}

EventType StatementHandler::resolveReplyEventType(
	EventType stmtType, const OptionSet& optionSet) {
	const EventType replyEventType =
		optionSet.get<Options::REPLY_EVENT_TYPE>();
	if (replyEventType == UNDEF_EVENT_TYPE) {
		return stmtType;
	}
	else {
		return replyEventType;
	}
}

/*!
	@brief Checks if a user is authenticated
*/
void StatementHandler::checkAuthentication(
	const NodeDescriptor& ND, EventMonotonicTime emNow) {
	TEST_PRINT("checkAuthentication() S\n");

	const ConnectionOption& connOption = ND.getUserData<ConnectionOption>();

	if (!connOption.isAuthenticated_) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_REQUIRED, "");
	}
	if (!connOption.isAdminAndPublicDB_) {
		EventMonotonicTime interval =
			transactionService_->getManager()->getReauthenticationInterval();
		if (interval != 0) {
			TEST_PRINT1("ReauthenticationInterval=%d\n", interval);

			TEST_PRINT1("emNow=%d\n", emNow);
			TEST_PRINT1(
				"authenticationTime=%d\n", connOption.authenticationTime_);
			TEST_PRINT1("diff=%d\n", emNow - connOption.authenticationTime_);
			if (emNow - connOption.authenticationTime_ >
				interval * 1000) {  
				TEST_PRINT("ReAuth fired\n");
				TXN_THROW_DENY_ERROR(GS_ERROR_TXN_REAUTHENTICATION_FIRED, "");
			}
		}
	}
	TEST_PRINT("checkAuthentication() E\n");
}

/*!
	@brief Checks if immediate consistency is required
*/
void StatementHandler::checkConsistency(
	const NodeDescriptor& ND, bool requireImmediate) {
	const ConnectionOption& connOption = ND.getUserData<ConnectionOption>();
	if (requireImmediate && (connOption.isImmediateConsistency_ != true)) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_CONSISTENCY_TYPE_UNMATCH,
			"immediate consistency required");
	}
}

/*!
	@brief Checks executable status
*/
void StatementHandler::checkExecutable(
	PartitionId pId,
	ClusterRole requiredClusterRole, PartitionRoleType requiredPartitionRole,
	PartitionStatus requiredPartitionStatus, PartitionTable* pt) {
#define TXN_THROW_PT_STATE_UNMATCH_ERROR(                           \
	pId,errorCode, requiredPartitionStatus, actualPartitionStatus,pt)       \
	TXN_THROW_DENY_ERROR(errorCode,                                 \
		"(pId=" << pId << ", required={" << partitionStatusToStr(requiredPartitionStatus) << "}" \
					<< ", actual="                                  \
					<< partitionStatusToStr(actualPartitionStatus) \
					<< ", revision=" << pt->getPartitionRevision(pId, 0) \
					<< ", LSN=" << pt->getLSN(pId) \
					<< ")")

	const PartitionTable::PartitionStatus actualPartitionStatus =
		partitionTable_->getPartitionStatus(pId);
	switch (actualPartitionStatus) {
	case PartitionTable::PT_ON:
		if (!(requiredPartitionStatus & PSTATE_ON)) {
			TXN_THROW_PT_STATE_UNMATCH_ERROR(pId,
				GS_ERROR_TXN_PARTITION_STATE_UNMATCH, requiredPartitionStatus,
				PSTATE_ON, pt);
		}
		break;
	case PartitionTable::PT_SYNC:
		if (!(requiredPartitionStatus & PSTATE_SYNC)) {
			TXN_THROW_PT_STATE_UNMATCH_ERROR(pId,
				GS_ERROR_TXN_PARTITION_STATE_UNMATCH, requiredPartitionStatus,
				PSTATE_SYNC, pt);
		}
		break;
	case PartitionTable::PT_OFF:
		if (!(requiredPartitionStatus & PSTATE_OFF)) {
			TXN_THROW_PT_STATE_UNMATCH_ERROR(pId,
				GS_ERROR_TXN_PARTITION_STATE_UNMATCH, requiredPartitionStatus,
				PSTATE_OFF, pt);
		}
		break;
	case PartitionTable::PT_STOP:
		if (!(requiredPartitionStatus & PSTATE_STOP)) {
			TXN_THROW_PT_STATE_UNMATCH_ERROR(pId,
				GS_ERROR_TXN_PARTITION_STATE_UNMATCH, requiredPartitionStatus,
				PSTATE_STOP, pt);
		}
		break;
	default:
		TXN_THROW_PT_STATE_UNMATCH_ERROR(pId, GS_ERROR_TXN_PARTITION_STATE_INVALID,
			requiredPartitionStatus, PSTATE_UNKNOWN, pt);
	}

	const NodeId myself = 0;
	const bool isOwner = partitionTable_->isOwner(pId, myself);
	const bool isBackup = partitionTable_->isBackup(pId, myself);
	const bool isCatchup = partitionTable_->isCatchup(pId, myself);

	assert((isOwner && isBackup) == false);
	assert((isOwner && isCatchup) == false);
	assert((isBackup && isCatchup) == false);
	PartitionRoleType actualPartitionRole = PROLE_UNKNOWN;
	actualPartitionRole |= isOwner ? PROLE_OWNER : PROLE_UNKNOWN;
	actualPartitionRole |= isBackup ? PROLE_BACKUP : PROLE_UNKNOWN;
	actualPartitionRole |= isCatchup ? PROLE_CATCHUP : PROLE_UNKNOWN;
	actualPartitionRole |=
		(!isOwner && !isBackup && !isCatchup) ? PROLE_NONE : PROLE_UNKNOWN;
	assert(actualPartitionRole != PROLE_UNKNOWN);
	if (!(actualPartitionRole & requiredPartitionRole)) {
		TXN_THROW_DENY_ERROR(GS_ERROR_TXN_PARTITION_ROLE_UNMATCH,
			"(pId=" << pId << ", required={" << partitionRoleTypeToStr(requiredPartitionRole) << "}"
			<< ", actual="
			<< partitionRoleTypeToStr(actualPartitionRole)
			<< ", revision=" << pt->getPartitionRevision(pId, 0) << ", LSN=" << pt->getLSN(pId) << ")");
	}

	const bool isMaster = partitionTable_->isMaster();
	const bool isFollower = partitionTable_->isFollower();
	const bool isSubmaster = (!isMaster && !isFollower);
	assert((isMaster && isFollower) == false);
	ClusterRole actualClusterRole = CROLE_UNKNOWN;
	actualClusterRole |= isMaster ? CROLE_MASTER : CROLE_UNKNOWN;
	actualClusterRole |= isFollower ? CROLE_FOLLOWER : CROLE_UNKNOWN;
	actualClusterRole |= isSubmaster ? CROLE_SUBMASTER : CROLE_UNKNOWN;
	assert(actualClusterRole != CROLE_UNKNOWN);
	if (!(actualClusterRole & requiredClusterRole)) {
		TXN_THROW_DENY_ERROR(GS_ERROR_TXN_CLUSTER_ROLE_UNMATCH,
			"(pId=" << pId << ", required={" << clusterRoleToStr(requiredClusterRole) << "}"
			<< ", actual="
			<< clusterRoleToStr(actualClusterRole) << ", LSN=" << pt->getLSN(pId) << ")");
	}

#undef TXN_THROW_PT_STATE_UNMATCH_ERROR
}

void StatementHandler::checkExecutable(ClusterRole requiredClusterRole) {
#define TXN_THROW_PT_STATE_UNMATCH_ERROR(                           \
	errorCode, requiredPartitionStatus, actualPartitionStatus)       \
	TXN_THROW_DENY_ERROR(errorCode,                                 \
		"(required={" << partitionStatusToStr(requiredPartitionStatus) << "}" \
					<< ", actual="                                  \
					<< partitionStatusToStr(actualPartitionStatus) << ")")
	const bool isMaster = partitionTable_->isMaster();
	const bool isFollower = partitionTable_->isFollower();
	const bool isSubmaster = (!isMaster && !isFollower);
	assert((isMaster && isFollower) == false);
	ClusterRole actualClusterRole = CROLE_UNKNOWN;
	actualClusterRole |= isMaster ? CROLE_MASTER : CROLE_UNKNOWN;
	actualClusterRole |= isFollower ? CROLE_FOLLOWER : CROLE_UNKNOWN;
	actualClusterRole |= isSubmaster ? CROLE_SUBMASTER : CROLE_UNKNOWN;
	assert(actualClusterRole != CROLE_UNKNOWN);
	if (!(actualClusterRole & requiredClusterRole)) {
		TXN_THROW_DENY_ERROR(GS_ERROR_TXN_CLUSTER_ROLE_UNMATCH,
			"(required={" << clusterRoleToStr(requiredClusterRole) << "}"
			<< ", actual="
			<< clusterRoleToStr(actualClusterRole) << ")");
	}

#undef TXN_THROW_PT_STATE_UNMATCH_ERROR
}

void StatementHandler::checkExecutable(ClusterRole requiredClusterRole, PartitionTable* pt) {
	const bool isMaster = pt->isMaster();
	const bool isFollower = pt->isFollower();
	const bool isSubmaster = (!isMaster && !isFollower);
	assert((isMaster && isFollower) == false);
	ClusterRole actualClusterRole = CROLE_UNKNOWN;
	actualClusterRole |= isMaster ? CROLE_MASTER : CROLE_UNKNOWN;
	actualClusterRole |= isFollower ? CROLE_FOLLOWER : CROLE_UNKNOWN;
	actualClusterRole |= isSubmaster ? CROLE_SUBMASTER : CROLE_UNKNOWN;
	assert(actualClusterRole != CROLE_UNKNOWN);
	if (!(actualClusterRole & requiredClusterRole)) {
		TXN_THROW_DENY_ERROR(GS_ERROR_TXN_CLUSTER_ROLE_UNMATCH,
			"(required={" << clusterRoleToStr(requiredClusterRole) << "}"
			<< ", actual="
			<< clusterRoleToStr(actualClusterRole) << ")");
	}
}

/*!
	@brief Checks if transaction timeout
*/
void StatementHandler::checkTransactionTimeout(
	EventMonotonicTime now,
	EventMonotonicTime queuedTime, int32_t txnTimeoutIntervalSec,
	uint32_t queueingCount) {
	static_cast<void>(now);
	static_cast<void>(queuedTime);
	static_cast<void>(txnTimeoutIntervalSec);
	static_cast<void>(queueingCount);
}

/*!
	@brief Checks if a specified container exists
*/
void StatementHandler::checkContainerExistence(BaseContainer* container) {
	if (container == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_CONTAINER_NOT_FOUND, "");
	}
}
/*!
	@brief Checks if a specified container exists
*/
void StatementHandler::checkContainerExistence(KeyDataStoreValue& keyStoreValue) {
	if (keyStoreValue.containerId_ == UNDEF_CONTAINERID) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_CONTAINER_NOT_FOUND, "");
	}
}

/*!
	@brief Checks the schema version of container
*/
void StatementHandler::checkContainerSchemaVersion(
	BaseContainer* container, SchemaVersionId schemaVersionId) {
	checkContainerExistence(container);

	if (container->getVersionId() != schemaVersionId) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_CONTAINER_SCHEMA_UNMATCH,
			"(containerId=" << container->getContainerId()
			<< ", expectedVer=" << schemaVersionId
			<< ", actualVer=" << container->getVersionId()
			<< ")");
	}
}

void StatementHandler::checkSchemaFeatureVersion(
		const BaseContainer &container, int32_t acceptableFeatureVersion) {
	if (acceptableFeatureVersion < StatementMessage::FEATURE_V5_3) {
		checkSchemaFeatureVersion(
				container.getSchemaFeatureLevel(), acceptableFeatureVersion);
	}
}

void StatementHandler::checkSchemaFeatureVersion(
		int32_t schemaFetureLevel, int32_t acceptableFeatureVersion) {
	if (acceptableFeatureVersion < StatementMessage::FEATURE_V5_3 &&
			schemaFetureLevel > 1) {
		GS_THROW_USER_ERROR(
				GS_ERROR_TXN_CLIENT_VERSION_NOT_ACCEPTABLE,
				"Requested client does not support extended schema feature "
				"(e.g. precise timestamp type) (clientVersion=" <<
				acceptableFeatureVersion << ")");
	}
}

void StatementHandler::checkMetaTableFeatureVersion(
		uint8_t unit, int32_t acceptableFeatureVersion) {
	if (unit != static_cast<uint8_t>(util::DateTime::FIELD_DAY_OF_MONTH) &&
			acceptableFeatureVersion < StatementMessage::FEATURE_V5_6) {
		GS_THROW_USER_ERROR(
				GS_ERROR_TXN_CLIENT_VERSION_NOT_ACCEPTABLE,
				"Requested client does not support extended table partitioning type feature "
				"(clientVersion=" << acceptableFeatureVersion << ")");
	}
}

/*!
	@brief Checks fetch option
*/
void StatementHandler::checkFetchOption(FetchOption fetchOption) {
	int64_t inputFetchSize = static_cast<int64_t>(fetchOption.size_);
	if (inputFetchSize < 1) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_FETCH_PARAMETER_INVALID,
			"Fetch Size = " << inputFetchSize
			<< ". Fetch size must be greater than 1");
	}
	checkSizeLimit(fetchOption.limit_);
}

/*!
	@brief Checks result size limit
*/
void StatementHandler::checkSizeLimit(ResultSize limit) {
	int64_t inputLimit = static_cast<int64_t>(limit);
	if (inputLimit < 0) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_FETCH_PARAMETER_INVALID,
			"Limit Size = " << inputLimit
			<< ". Limit size must be greater than 0");
	}
}

void StatementHandler::checkLoggedInDatabase(
	DatabaseId loginDbId, const char8_t* loginDbName,
	DatabaseId specifiedDbId, const char8_t* specifiedDbName
	, bool isNewSQL
) {
	if (isNewSQL) {
		return;
	}
	if (specifiedDbId != loginDbId ||
		strcmp(specifiedDbName, loginDbName) != 0) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_DATABASE_UNMATCH,
			"Specified db and logged in db are different. " <<
			"specified = " << specifiedDbName << "(" << specifiedDbId << ")" <<
			", logged in = " << loginDbName << "(" << loginDbId << ")");
	}
}

void StatementHandler::checkQueryOption(
	const OptionSet& optionSet,
	const FetchOption& fetchOption, bool isPartial, bool isTQL) {

	checkFetchOption(fetchOption);

	const QueryContainerKey* distInfo = optionSet.get<Options::DIST_QUERY>();
	const bool distributed = (distInfo != NULL && distInfo->enabled_);

	if (optionSet.get<Options::FETCH_BYTES_SIZE>() < 1) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_FETCH_PARAMETER_INVALID,
			"Fetch byte size = " << optionSet.get<Options::FETCH_BYTES_SIZE>() <<
			". Fetch byte size must be greater than 1");
	}
	if (!isTQL && (distributed || isPartial)) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_NOT_EXECUTABLE,
			"Partial query option is only applicable in TQL");
	}
	if (fetchOption.size_ != INT64_MAX && (distributed || isPartial)) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_NOT_EXECUTABLE,
			"partial fetch is not applicable in distribute / partial execute mode");
	}
	if (optionSet.get<Options::FOR_UPDATE>() && (distributed || isPartial)) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_NOT_EXECUTABLE,
			"row lock (forUpdate) is not applicable in distribute / partial execute mode");
	}
}

void StatementHandler::createResultSetOption(
	const OptionSet& optionSet,
	const int32_t* fetchBytesSize, bool partial,
	const PartialQueryOption& partialOption,
	ResultSetOption& queryOption) {

	const QueryContainerKey* distInfo = optionSet.get<Options::DIST_QUERY>();
	const bool distributed = (distInfo != NULL && distInfo->enabled_);

	queryOption.set(
		(fetchBytesSize == NULL ?
			optionSet.get<Options::FETCH_BYTES_SIZE>() :
			*fetchBytesSize),
		distributed, partial, partialOption);

	SQLTableInfo* largeInfo = optionSet.get<Options::LARGE_INFO>();
	if (largeInfo != NULL) {
		SQLService::applyClusterPartitionCount(
			*largeInfo, partitionTable_->getPartitionNum());
		queryOption.setLargeInfo(largeInfo);
	}
}

void StatementHandler::checkLogVersion(uint16_t logFormatVersion) {
	if (!LogManagerConst::isAcceptableFormatVersion(logFormatVersion)) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_REPLICATION_LOG_VERSION_NOT_ACCEPTABLE,
			"(receiveFormatVersion=" << static_cast<uint32_t>(logFormatVersion) <<
			", baseFormatVersion=" << static_cast<uint32_t>(LogManagerConst::getBaseFormatVersion()) <<
			")" );
	}
}

StatementMessage::FixedRequest::Source StatementHandler::getRequestSource(
	const Event & ev) {
	return FixedRequest::Source(ev.getPartitionId(), ev.getType());
}

template<typename S>
void StatementHandler::decodeRequestCommonPart(
	S & in, Request & request,
	ConnectionOption & connOption) {
	try {
		request.fixed_.decode<S>(in);
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
	decodeRequestOptionPart(in, request, connOption);
}

/*!
	@brief Decodes option part of message
*/
template<typename S>
void StatementHandler::decodeRequestOptionPart(
	S & in, Request & request,
	ConnectionOption & connOption) {
	try {
		request.optional_.set<Options::TXN_TIMEOUT_INTERVAL>(-1);

		decodeOptionPart(in, request.optional_);

		if (strlen(request.optional_.get<Options::DB_NAME>()) == 0) {
			bool useConnDBName = !isNewSQL(request);
			useConnDBName &= !connOption.dbName_.empty();
			request.optional_.set<Options::DB_NAME>(
				!useConnDBName ? GS_PUBLIC : connOption.dbName_.c_str());
		}

		if (isNewSQL(request)) {
			connOption.connected_ = true;
			connOption.isAuthenticated_ = true;
			connOption.isImmediateConsistency_ = true;
			connOption.requestType_ = Message::REQUEST_NEWSQL;
			connOption.userType_ = StatementMessage::USER_ADMIN;
			connOption.isAdminAndPublicDB_ = true;
			connOption.dbId_ = request.optional_.get<Options::DB_VERSION_ID>();
			connOption.priv_ = ALL;
		}

		if (request.optional_.get<Options::TXN_TIMEOUT_INTERVAL>() >= 0) {
			request.fixed_.cxtSrc_.txnTimeoutInterval_ =
				request.optional_.get<Options::TXN_TIMEOUT_INTERVAL>();
		}
		else {
			request.fixed_.cxtSrc_.txnTimeoutInterval_ =
				connOption.txnTimeoutInterval_;
			request.optional_.set<Options::TXN_TIMEOUT_INTERVAL>(
				TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL);
		}

		request.fixed_.cxtSrc_.storeMemoryAgingSwapRate_ =
			request.optional_.get<Options::STORE_MEMORY_AGING_SWAP_RATE>();
		if (!TransactionContext::isStoreMemoryAgingSwapRateSpecified(
			request.fixed_.cxtSrc_.storeMemoryAgingSwapRate_)) {
			request.fixed_.cxtSrc_.storeMemoryAgingSwapRate_ =
				connOption.storeMemoryAgingSwapRate_;
		}

		request.fixed_.cxtSrc_.timeZone_ =
			request.optional_.get<Options::TIME_ZONE_OFFSET>();
		if (request.fixed_.cxtSrc_.timeZone_.isEmpty()) {
			request.fixed_.cxtSrc_.timeZone_ = connOption.timeZone_;
		}
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

template<typename S>
void StatementHandler::decodeOptionPart(
	S & in, OptionSet & optionSet) {
	try {
		optionSet.decode(in);
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

bool StatementHandler::isNewSQL(const Request & request) {
	return request.optional_.get<Options::REPLY_PID>() != UNDEF_PARTITIONID;
}

bool StatementHandler::isSkipReply(const Request & request) {
	return request.optional_.get<Options::FOR_SYNC>() &&
		request.optional_.get<Options::SUB_CONTAINER_ID>() == -1;
}

StatementHandler::CaseSensitivity StatementHandler::getCaseSensitivity(
	const Request & request) {
	return getCaseSensitivity(request.optional_);
}

StatementHandler::CaseSensitivity StatementHandler::getCaseSensitivity(
	const OptionSet & optionSet) {
	return optionSet.get<Options::SQL_CASE_SENSITIVITY>();
}

CreateDropIndexMode
StatementHandler::getCreateDropIndexMode(const OptionSet & optionSet) {
	return optionSet.get<Options::CREATE_DROP_INDEX_MODE>();
}

bool StatementHandler::isDdlTransaction(const OptionSet & optionSet) {
	return optionSet.get<Options::DDL_TRANSACTION_MODE>();
}

void StatementHandler::assignIndexExtension(
	IndexInfo & indexInfo, const OptionSet & optionSet) {
	indexInfo.extensionName_.append(getExtensionName(optionSet));

	const ExtensionColumnTypes* types =
		optionSet.get<Options::EXTENSION_TYPE_LIST>();
	if (types != NULL) {
		indexInfo.extensionOptionSchema_.assign(
			types->columnTypeList_.begin(),
			types->columnTypeList_.end());
	}

	const ExtensionParams* params =
		optionSet.get<Options::EXTENSION_PARAMS>();
	if (params != NULL) {
		indexInfo.extensionOptionFixedPart_.assign(
			params->fixedPart_.begin(),
			params->fixedPart_.end());
		indexInfo.extensionOptionVarPart_.assign(
			params->varPart_.begin(),
			params->varPart_.end());;
	}
}

const char8_t* StatementHandler::getExtensionName(const OptionSet & optionSet) {
	return optionSet.get<Options::EXTENSION_NAME>();
}

/*!
	@brief Decodes index information
*/
template <typename S>
void StatementHandler::decodeIndexInfo(
	S & in, IndexInfo & indexInfo) {
	try {
		uint32_t size;
		in >> size;

		in >> indexInfo.indexName_;

		uint32_t columnIdCount;
		in >> columnIdCount;
		for (uint32_t i = 0; i < columnIdCount; i++) {
			uint32_t columnId;
			in >> columnId;
			indexInfo.columnIds_.push_back(columnId);
		}

		decodeEnumData<S, MapType>(in, indexInfo.mapType);
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes multiple row data
*/
template <typename S>
void StatementHandler::decodeMultipleRowData(
	S & in, uint64_t & numRow,
	RowData & rowData) {
	try {
		in >> numRow;
		decodeBinaryData(in, rowData, true);
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes fetch option
*/
template<typename S>
void StatementHandler::decodeFetchOption(
	S & in, FetchOption & fetchOption) {
	try {
		in >> fetchOption.limit_;
		in >> fetchOption.size_;
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes TQL partial execute option
*/
template<typename S>
void StatementHandler::decodePartialQueryOption(
	S & in, util::StackAllocator & alloc,
	bool& isPartial, PartialQueryOption & partialQueryOption) {
	try {
		decodeBooleanData<S>(in, isPartial);
		int32_t entryNum;
		in >> entryNum;
		if (isPartial) {
			for (int32_t i = 0; i < entryNum; i++) {
				int8_t entryType;
				in >> entryType;

				util::XArray<uint8_t>* entryBody = ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
				decodeBinaryData<S>(in, *entryBody, false);
				partialQueryOption.insert(std::make_pair(entryType, entryBody));
			}
		}
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

template<typename S>
void StatementHandler::decodeGeometryRelatedQuery(
	S & in, GeometryQuery & query) {
	try {
		in >> query.columnId_;
		decodeVarSizeBinaryData<S>(in, query.intersection_);
		decodeEnumData<S, GeometryOperator>(in, query.operator_);
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

template<typename S>
void StatementHandler::decodeGeometryWithExclusionQuery(
	S & in, GeometryQuery & query) {
	try {
		in >> query.columnId_;
		decodeVarSizeBinaryData<S>(in, query.intersection_);
		decodeVarSizeBinaryData<S>(in, query.disjoint_);
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes TimeRelatedCondition
*/
template<typename S>
void StatementHandler::decodeTimeRelatedCondition(
	S & in,
	TimeRelatedCondition & condition) {
	try {
		in >> condition.rowKey_;
		decodeEnumData<S, TimeOperator>(in, condition.operator_);
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes InterpolateCondition
*/
template<typename S>
void StatementHandler::decodeInterpolateCondition(
	S & in,
	InterpolateCondition & condition) {
	try {
		in >> condition.rowKey_;
		in >> condition.columnId_;
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes AggregateQuery
*/
template<typename S>
void StatementHandler::decodeAggregateQuery(
	S & in, AggregateQuery & query) {
	try {
		in >> query.start_;
		in >> query.end_;
		in >> query.columnId_;
		decodeEnumData<S, AggregationType>(in, query.aggregationType_);
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes RangeQuery
*/
template<typename S>
void StatementHandler::decodeRangeQuery(
	S & in, RangeQuery & query) {
	try {
		in >> query.start_;
		in >> query.end_;
		decodeEnumData<S, OutputOrder>(in, query.order_);
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes SamplingQuery
*/
template <typename S>
void StatementHandler::decodeSamplingQuery(
	S & in, SamplingQuery & query) {
	try {
		in >> query.start_;
		in >> query.end_;

		int32_t columnCount;
		in >> columnCount;
		for (int32_t i = 0; i < columnCount; i++) {
			ColumnId columnId;
			in >> columnId;
			query.interpolatedColumnIdList_.push_back(columnId);
		}

		in >> query.interval_;
		decodeEnumData<S, TimeUnit>(in, query.timeUnit_);
		decodeEnumData<S, InterpolationMode>(in, query.mode_);
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes ContainerConditionData
*/
template <typename S>
void StatementHandler::decodeContainerConditionData(
	S & in,
	KeyDataStore::ContainerCondition & containerCondition) {
	try {
		if (in.base().remaining() != 0) {
			int32_t num;
			in >> num;

			for (int32_t i = 0; i < num; i++) {
				int32_t containerAttribute;
				in >> containerAttribute;
				containerCondition.insertAttribute(
					static_cast<ContainerAttribute>(containerAttribute));
			}
		}
		else {
			containerCondition.insertAttribute(
				static_cast<ContainerAttribute>(CONTAINER_ATTR_SINGLE));  
		}
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

template <typename S>
void StatementHandler::decodeResultSetId(
	S & in, ResultSetId & resultSetId) {
	try {
		if (in.base().remaining() != 0) {
			in >> resultSetId;
		}
		else {
			resultSetId = UNDEF_RESULTSETID;
		}
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes boolean data
*/
template <typename S>
void StatementHandler::decodeBooleanData(
	S & in, bool& boolData) {
	try {
		int8_t tmp;
		in >> tmp;
		boolData = tmp ? true : false;
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}


/*!
	@brief Decodes VarSize-typed binary data
*/
template <typename S>
void StatementHandler::decodeVarSizeBinaryData(
	S & in,
	util::XArray<uint8_t> &binaryData) {
	try {
		uint32_t size = ValueProcessor::getVarSize(in);
		binaryData.resize(size);
		in >> std::make_pair(binaryData.data(), size);
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes UUID
*/
template <typename S>
void StatementHandler::decodeUUID(S &in, uint8_t *uuid, size_t uuidSize) {
	try {
		assert(uuid != NULL);
		in.readAll(uuid, uuidSize);
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

template
void StatementHandler::decodeUUID(
		util::ByteStream<util::ArrayInStream> &in, uint8_t *uuid,
		size_t uuidSize);

/*!
	@brief Decodes ReplicationAck
*/
template <typename S>
void StatementHandler::decodeReplicationAck(
	S & in, ReplicationAck & ack) {


	try {
		in >> ack.clusterVer_;
		in >> ack.replStmtType_;
		in >> ack.replStmtId_;
		in >> ack.clientId_.sessionId_;
		decodeUUID<S>(in, ack.clientId_.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
		in >> ack.replId_;
		in >> ack.replMode_;
		in >> ack.taskStatus_;
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Encodes common part of message
*/
EventByteOutStream StatementHandler::encodeCommonPart(
	Event & ev, StatementId stmtId, StatementExecStatus status) {
	try {
		ev.setPartitionIdSpecified(false);

		EventByteOutStream out = ev.getOutStream();

		out << stmtId;
		out << status;

		return out;
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief Encodes index information
*/
void StatementHandler::encodeIndexInfo(
	EventByteOutStream & out, const IndexInfo & indexInfo) {
	try {
		const size_t sizePos = out.base().position();

		uint32_t size = 0;
		out << size;

		out << indexInfo.indexName_;

		out << static_cast<uint32_t>(indexInfo.columnIds_.size());
		for (size_t i = 0; i < indexInfo.columnIds_.size(); i++) {
			out << indexInfo.columnIds_[i];
		}

		encodeEnumData<EventByteOutStream, MapType>(out, indexInfo.mapType);

		const size_t lastPos = out.base().position();
		size = static_cast<uint32_t>(lastPos - sizePos - sizeof(size));
		out.base().position(sizePos);
		out << size;
		out.base().position(lastPos);
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void StatementHandler::encodeIndexInfo(
	util::XArrayByteOutStream & out, const IndexInfo & indexInfo) {
	try {
		const size_t sizePos = out.base().position();

		uint32_t size = 0;
		out << size;

		out << indexInfo.indexName_;

		out << static_cast<uint32_t>(indexInfo.columnIds_.size());
		for (size_t i = 0; i < indexInfo.columnIds_.size(); i++) {
			out << indexInfo.columnIds_[i];
		}
		const uint8_t tmp = static_cast<uint8_t>(indexInfo.mapType);
		out << tmp;
		const size_t lastPos = out.base().position();
		size = static_cast<uint32_t>(lastPos - sizePos - sizeof(size));
		out.base().position(sizePos);
		out << size;
		out.base().position(lastPos);
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}
/*!
	@brief Encodes binary data
*/
template<typename S>
void StatementHandler::encodeBinaryData(
	S & out, const uint8_t * data, size_t size) {
	try {
		out << std::pair<const uint8_t*, size_t>(data, size);
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

template<typename S>
void StatementHandler::encodeVarSizeBinaryData(
	S & out, const uint8_t * data, size_t size) {
	try {
		switch (ValueProcessor::getEncodedVarSize(static_cast<uint32_t>(size))) {
		case 1:
			out << ValueProcessor::encode1ByteVarSize(static_cast<uint8_t>(size));
			break;
		case 4:
			out << ValueProcessor::encode4ByteVarSize(static_cast<uint32_t>(size));
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED, "");
		}
		out << std::pair<const uint8_t*, size_t>(data, size);
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

template<typename S>
void StatementHandler::encodeContainerKey(
	S & out, const FullContainerKey & containerKey) {
	try {
		const void* body;
		size_t size;
		containerKey.toBinary(body, size);
		encodeVarSizeBinaryData(out, static_cast<const uint8_t*>(body), size);
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief Encodes exception information
*/
template <typename ByteOutStream>
void StatementHandler::encodeException(
	ByteOutStream & out, const std::exception & exception, bool detailsHidden,
	const ExceptionParameterList * paramList) {
	const util::Exception e = GS_EXCEPTION_CONVERT(exception, "");

	const uint32_t maxDepth =
		(detailsHidden ? 1 : static_cast<uint32_t>(e.getMaxDepth() + 1));

	out << maxDepth;

	for (uint32_t depth = 0; depth < maxDepth; ++depth) {
		out << e.getErrorCode(depth);

		util::NormalOStringStream oss;

		e.formatMessage(oss, depth);
		out << oss.str().c_str();
		oss.str("");

		e.formatTypeName(oss, depth);
		out << (detailsHidden ? "" : oss.str().c_str());
		oss.str("");

		e.formatFileName(oss, depth);
		out << (detailsHidden ? "" : oss.str().c_str());
		oss.str("");

		e.formatFunctionName(oss, depth);
		out << (detailsHidden ? "" : oss.str().c_str());
		oss.str("");

		out << static_cast<int32_t>(
			detailsHidden ? -1 : e.getLineNumber(depth));
	}

	{
		const char8_t* errorName = e.getNamedErrorCode().getName();
		out << (errorName == NULL ? "" : errorName);
	}

	{
		const int32_t paramCount = static_cast<int32_t>(
				paramList == NULL ? 0 : paramList->size());
		out << paramCount;

		for (int32_t i = 0; i < paramCount; i++) {
			const ExceptionParameterEntry& entry = (*paramList)[i];
			out << entry.first;
			out << entry.second;
		}
	}
}

/*!
	@brief Encodes exception information
*/
template <typename ByteOutStream>
void StatementHandler::encodeException(ByteOutStream & out,
	const std::exception & exception, const NodeDescriptor & nd) {
	bool detailsHidden;
	if (!nd.isEmpty() && nd.getType() == NodeDescriptor::ND_TYPE_CLIENT) {
		detailsHidden = TXN_DETAIL_EXCEPTION_HIDDEN;
	}
	else {
		detailsHidden = false;
	}

	encodeException(out, exception, detailsHidden);
}

template<typename S>
int32_t StatementHandler::encodeDistributedResult(
	S & out, const ResultSet & rs, int64_t * encodedSize) {
	int32_t entryCount = 0;
	const size_t startPos = out.base().position();

	const util::Vector<int64_t>* distributedTarget =
		static_cast<const ResultSet&>(rs).getDistributedTarget();
	if (distributedTarget != NULL) {
		encodeEnumData<S, QueryResponseType>(out, DIST_TARGET);

		const size_t entryHeadPos = out.base().position();
		out << static_cast<int32_t>(0);
		const size_t entryBodyPos = out.base().position();

		bool uncovered;
		bool reduced;
		rs.getDistributedTargetStatus(uncovered, reduced);
		encodeBooleanData<S>(out, uncovered);
		encodeBooleanData<S>(out, reduced);

		out << static_cast<int32_t>(distributedTarget->size());
		for (util::Vector<int64_t>::const_iterator it = distributedTarget->begin();
			it != distributedTarget->end(); ++it) {
			out << *it;
		}

		const size_t entryEndPos = out.base().position();
		out.base().position(entryHeadPos);
		out << static_cast<int32_t>(entryEndPos - entryBodyPos);
		out.base().position(entryEndPos);

		entryCount++;
	}

	const ResultSetOption& quetyOption = rs.getQueryOption();
	if (quetyOption.isDistribute() && quetyOption.existLimitOffset()) {
		encodeEnumData<S, QueryResponseType>(out, DIST_LIMIT);
		int32_t entrySize = sizeof(int64_t) + sizeof(int64_t);
		out << entrySize;
		encodeLongData<S, int64_t>(out, quetyOption.getDistLimit());
		encodeLongData<S, int64_t>(out, quetyOption.getDistOffset());
		entryCount++;
	}

	if (encodedSize != NULL) {
		*encodedSize += static_cast<int64_t>(out.base().position() - startPos);
	}

	return entryCount;
}

/*!
	@brief Replies success message
*/
void StatementHandler::replySuccess(
	EventContext & ec, util::StackAllocator & alloc,
	const NodeDescriptor & ND, EventType stmtType,
	StatementExecStatus status, const Request & request,
	const Response & response, bool ackWait) {

	util::StackAllocator::Scope scope(alloc);

	if (!isNewSQL(request)) {
		if (!ackWait) {
			Event ev(ec, stmtType, request.fixed_.pId_);
			setSuccessReply(
				alloc, ev, request.fixed_.cxtSrc_.stmtId_,
				status, response, request);

			if (ev.getExtraMessageCount() > 0) {
				EventEngine::EventRequestOption option;
				option.timeoutMillis_ = 0;
				ec.getEngine().send(ev, ND, &option);
			}
			else {
				ec.getEngine().send(ev, ND);
			}
		}
	}
	else {
		if (isSkipReply(request)) {
			return;
		}
		if (!ackWait) {
			const EventType replyEventType =
				resolveReplyEventType(stmtType, request.optional_);
			const PartitionId replyPId =
				request.optional_.get<Options::REPLY_PID>();

			Event ev(ec, replyEventType, replyPId);
			setSuccessReply(
				alloc, ev, request.fixed_.cxtSrc_.stmtId_,
				status, response, request);
			ev.setPartitionId(replyPId);

			EventEngine* replyEE;
			replyEE = sqlService_->getEE();
			NodeDescriptor replyNd = replyEE->getServerND(ND.getId());
			if (ev.getExtraMessageCount() > 0) {
				EventEngine::EventRequestOption option;
				option.timeoutMillis_ = 0;
				replyEE->send(ev, replyNd, &option);
			}
			else {
				replyEE->send(ev, replyNd);
			}
		}
	}
}

void StatementHandler::replySuccess(
	EventContext & ec, util::StackAllocator & alloc,
	const NodeDescriptor & ND, EventType stmtType,
	const Request & request,
	SimpleOutputMessage & message, bool ackWait) {
	util::StackAllocator::Scope scope(alloc);
	bool isSQL = isNewSQL(request);

	if (!ackWait && (!isSQL || !isSkipReply(request))) {
		const EventType replyEventType =
			isSQL ? resolveReplyEventType(stmtType, request.optional_) : stmtType;
		const PartitionId replyPId =
			isSQL ? request.optional_.get<Options::REPLY_PID>() : request.fixed_.pId_;
		EventEngine& replyEE =
			isSQL ? *(sqlService_->getEE()) : ec.getEngine();
		NodeDescriptor replyNd =
			isSQL ? replyEE.getServerND(ND.getId()) : ND;

		Event ev(ec, replyEventType, replyPId);
		ev.setPartitionIdSpecified(false);
		EventByteOutStream out = ev.getOutStream();
		message.encode(out);
		message.addExtraMessage(&ev);

		if (isSQL) {
			ev.setPartitionId(replyPId);
		}

		if (ev.getExtraMessageCount() > 0) {
			EventEngine::EventRequestOption option;
			option.timeoutMillis_ = 0;
			replyEE.send(ev, replyNd, &option);
		}
		else {
			replyEE.send(ev, replyNd);
		}
	}
}

/*!
	@brief Replies success message
*/
void StatementHandler::replySuccess(
		EventContext & ec,
		util::StackAllocator & alloc, StatementExecStatus status,
		const ReplicationContext & replContext) {
	UNUSED_VARIABLE(status);

#define TXN_TRACE_REPLY_SUCCESS_ERROR(replContext)             \
	"(nd=" << replContext.getConnectionND()                    \
		   << ", pId=" << replContext.getPartitionId()         \
		   << ", eventType=" << EventTypeUtility::getEventTypeName(replContext.getStatementType()) \
		   << ", stmtId=" << replContext.getStatementId()      \
		   << ", clientId=" << replContext.getClientId()       \
		   << ", containerId=" << replContext.getContainerId() << ")"

	util::StackAllocator::Scope scope(alloc);

	if (replContext.getConnectionND().isEmpty()) {
		return;
	}
	assert(replContext.getMessage() != NULL);

	try {
		Response response(alloc);

		if (!replContext.isNewSQL()) {
			Event ev(
				ec, replContext.getStatementType(),
				replContext.getPartitionId());
			ev.setPartitionIdSpecified(false);

			EventByteOutStream out = ev.getOutStream();
			encodeBinaryData< EventByteOutStream>(out, replContext.getMessage()->data(), replContext.getMessage()->size());
			const std::vector<ReplicationContext::BinaryData*>& extraMessages = replContext.getExtraMessages();
			for (std::vector<ReplicationContext::BinaryData*>::const_iterator itr = extraMessages.begin();
				itr != extraMessages.end(); itr++) {
				ReplicationContext::BinaryData* binaryData = const_cast<ReplicationContext::BinaryData*>(*itr);
				ev.addExtraMessage(binaryData->data(), binaryData->size());
			}

			ec.getEngine().send(ev, replContext.getConnectionND());

			GS_TRACE_INFO(
				REPLICATION, GS_TRACE_TXN_REPLY_CLIENT,
				TXN_TRACE_REPLY_SUCCESS_ERROR(replContext));
		}
		else {
			if (!replContext.getSyncFlag() && replContext.getSubContainerId() == -1) {
				return;
			}
			Event ev(
				ec, replContext.getReplyEventType(),
				replContext.getReplyPId());

			const FixedRequest::Source source(ev.getPartitionId(), replContext.getStatementType());
			Request request(alloc, source);
			setReplyOption(request.optional_, replContext);

			EventByteOutStream out = ev.getOutStream();
			encodeBinaryData< EventByteOutStream>(out, replContext.getMessage()->data(), replContext.getMessage()->size());
			const std::vector<ReplicationContext::BinaryData*>& extraMessages = replContext.getExtraMessages();
			for (std::vector<ReplicationContext::BinaryData*>::const_iterator itr = extraMessages.begin();
				itr != extraMessages.end(); itr++) {
				ReplicationContext::BinaryData* binaryData = const_cast<ReplicationContext::BinaryData*>(*itr);
				ev.addExtraMessage(binaryData->data(), binaryData->size());
			}

			ev.setPartitionId(replContext.getReplyPId());

			EventEngine* replyEE;
			replyEE = sqlService_->getEE();
			NodeDescriptor replyNd =
				replyEE->getServerND(replContext.getReplyNodeId());
			if (ev.getExtraMessageCount() > 0) {
				EventEngine::EventRequestOption option;
				option.timeoutMillis_ = 0;
				replyEE->send(ev, replyNd, &option);
			}
			else {
				replyEE->send(ev, replyNd);
			}
		}
	}
	catch (EncodeDecodeException& e) {
		GS_RETHROW_USER_ERROR(e, TXN_TRACE_REPLY_SUCCESS_ERROR(replContext));
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, TXN_TRACE_REPLY_SUCCESS_ERROR(replContext));
	}

#undef TXN_TRACE_REPLY_SUCCESS_ERROR
}


/*!
	@brief Replies error message
*/
void StatementHandler::replyError(
	EventContext & ec, util::StackAllocator & alloc,
	const NodeDescriptor & ND, EventType stmtType, StatementExecStatus status,
	const Request & request, const std::exception & e) {
	util::StackAllocator::Scope scope(alloc);

	StatementId stmtId = request.fixed_.cxtSrc_.stmtId_;
	if (!isNewSQL(request)) {
		if (stmtType == CONTINUE_ALTER_CONTAINER) {
			stmtType = PUT_CONTAINER;
			stmtId = request.fixed_.startStmtId_;
		}
		else if (stmtType == CONTINUE_CREATE_INDEX) {
			stmtType = CREATE_INDEX;
			stmtId = request.fixed_.startStmtId_;
		}
		Event ev(ec, stmtType, request.fixed_.pId_);
		setErrorReply(ev, stmtId, status, e, ND);

		if (!ND.isEmpty()) {
			ec.getEngine().send(ev, ND);
		}
	}
	else {
		if (isSkipReply(request)) {
			return;
		}

		EventType replyEventType =
			resolveReplyEventType(stmtType, request.optional_);
		const PartitionId replyPId = request.optional_.get<Options::REPLY_PID>();

		if (replyEventType == CONTINUE_ALTER_CONTAINER) {
			replyEventType = PUT_CONTAINER;
			stmtId = request.fixed_.startStmtId_;
		}
		else if (replyEventType == CONTINUE_CREATE_INDEX) {
			replyEventType = CREATE_INDEX;
			stmtId = request.fixed_.startStmtId_;
		}
		Event ev(ec, replyEventType, replyPId);
		setErrorReply(ev, stmtId, status, e, ND);
		ev.setPartitionId(replyPId);

		EventByteOutStream out = ev.getOutStream();
		OptionSet optionSet(alloc);
		setReplyOption(optionSet, request);
		optionSet.encode(out);

		EventEngine* replyEE;
		replyEE = sqlService_->getEE();
		NodeDescriptor replyNd = replyEE->getServerND(ND.getId());
		if (!replyNd.isEmpty()) {
			replyEE->send(ev, replyNd);
		}
	}
}

/*!
	@brief Handles error
*/
void StatementHandler::handleError(
	EventContext & ec, util::StackAllocator & alloc, Event & ev,
	const Request & request, std::exception & e) {
	util::StackAllocator::Scope scope(alloc);
	ErrorMessage errorMessage(e, "", ev, request);

	try {
		try {
			throw;
		}
		catch (StatementAlreadyExecutedException&) {
			UTIL_TRACE_EXCEPTION_INFO(
				TRANSACTION_SERVICE, e, errorMessage.withDescription(
					"Statement already executed"));
			Response response(alloc);
			response.existFlag_ = false;
			replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
		}
		catch (EncodeDecodeException&) {

			UTIL_TRACE_EXCEPTION(
				TRANSACTION_SERVICE, e, errorMessage.withDescription(
					"Failed to encode or decode message"));

			if ( AUDIT_TRACE_CHECK ) {
				if ( AuditCategoryType(ev) != 0 ) {
					AUDIT_TRACE_ERROR_OPERATOR("")
				}
			}
			replyError(
				ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_ERROR, request, e);
		}
		catch (LockConflictException& e2) {
			UTIL_TRACE_EXCEPTION_INFO(
				TRANSACTION_SERVICE, e, errorMessage.withDescription(
					"Lock conflicted"));
			try {
				util::LockGuard<util::Mutex> guard(
					partitionList_->partition(ev.getPartitionId()).mutex());
				TransactionContext& txn = transactionManager_->get(
					alloc, ev.getPartitionId(), request.fixed_.clientId_);

				util::XArray<const util::XArray<uint8_t>*> logRecordList(
					alloc);

				util::XArray<ClientId> closedResourceIds(alloc);
				if (request.fixed_.cxtSrc_.txnMode_ ==
					TransactionManager::NO_AUTO_COMMIT_BEGIN) {
					util::XArray<uint8_t>* log = abortOnError(txn);
					if (log != NULL) {
						logRecordList.push_back(log);
						if (ev.getType() == PUT_CONTAINER || ev.getType() == CREATE_INDEX) {
							closedResourceIds.push_back(request.fixed_.clientId_);
						}
						else
							if (ev.getType() == PUT_MULTIPLE_ROWS) {
								assert(request.fixed_.startStmtId_ != UNDEF_STATEMENTID);
								transactionManager_->update(
									txn, request.fixed_.startStmtId_);
							}
					}
				}

				executeReplication(request, ec, alloc, NodeDescriptor::EMPTY_ND,
					txn, ABORT_TRANSACTION, request.fixed_.cxtSrc_.stmtId_,
					TransactionManager::REPLICATION_ASYNC,
					closedResourceIds.data(), closedResourceIds.size(),
					logRecordList.data(), logRecordList.size(),
					NULL);

				if (request.fixed_.cxtSrc_.getMode_ == TransactionManager::CREATE ||
					ev.getType() == PUT_CONTAINER || ev.getType() == CREATE_INDEX) {
					transactionManager_->remove(
						request.fixed_.pId_, request.fixed_.clientId_);
				}
			}
			catch (ContextNotFoundException&) {
				UTIL_TRACE_EXCEPTION_INFO(
					TRANSACTION_SERVICE, e, errorMessage.withDescription(
						"Context not found"));
			}

			const LockConflictStatus lockConflictStatus(getLockConflictStatus(
				ev, ec.getHandlerStartMonotonicTime(), request));
			try {
				checkLockConflictStatus(lockConflictStatus, e2);
			}
			catch (LockConflictException&) {
				assert(false);
				throw;
			}
			catch (...) {
				handleError(ec, alloc, ev, request, e);
				return;
			}
			retryLockConflictedRequest(ec, ev, lockConflictStatus);
		}
		catch (DenyException&) {
			UTIL_TRACE_EXCEPTION(
				TRANSACTION_SERVICE, e, errorMessage.withDescription(
					"Request denied"));
			if ( AUDIT_TRACE_CHECK ) {
				if ( AuditCategoryType(ev) != 0 ) {
					AUDIT_TRACE_ERROR_OPERATOR("")
				}
			}
			replyError(ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_DENY, request, e);
		}
		catch (ContextNotFoundException&) {
			UTIL_TRACE_EXCEPTION_WARNING(
				TRANSACTION_SERVICE, e, errorMessage.withDescription(
					"Context not found"));
			if ( AUDIT_TRACE_CHECK ) {
				if ( AuditCategoryType(ev) != 0 ) {
					AUDIT_TRACE_ERROR_OPERATOR("")
				}
			}
			replyError(ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_ERROR, request, e);
		}
		catch (std::exception&) {
			if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
				throw;
			}
			else {
				const util::Exception utilException =
					GS_EXCEPTION_CONVERT(e, "");

				Response response(alloc);

				if (ev.getType() == ABORT_TRANSACTION &&
					utilException.getErrorCode(utilException.getMaxDepth()) ==
					GS_ERROR_TM_TRANSACTION_NOT_FOUND) {
					UTIL_TRACE_EXCEPTION_WARNING(
						TRANSACTION_SERVICE, e, errorMessage.withDescription(
							"Transaction is already timed out"));
					replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
						TXN_STATEMENT_SUCCESS, request, response,
						NO_REPLICATION);
				}
				else {
					UTIL_TRACE_EXCEPTION(
						TRANSACTION_SERVICE, e, errorMessage.withDescription(
							"User error"));
					try {
						util::LockGuard<util::Mutex> guard(
							partitionList_->partition(ev.getPartitionId()).mutex());
						
						TransactionContext& txn = transactionManager_->get(
							alloc, ev.getPartitionId(), request.fixed_.clientId_);

						util::XArray<uint8_t>* log = abortOnError(txn);
						if (log != NULL) {
							util::XArray<ClientId> closedResourceIds(alloc);
							if (ev.getType() == PUT_CONTAINER || ev.getType() == CREATE_INDEX) {
								closedResourceIds.push_back(request.fixed_.clientId_);
							}
							util::XArray<const util::XArray<uint8_t>*>
								logRecordList(alloc);
							logRecordList.push_back(log);

							executeReplication(request, ec, alloc,
								NodeDescriptor::EMPTY_ND, txn,
								ABORT_TRANSACTION, request.fixed_.cxtSrc_.stmtId_,
								TransactionManager::REPLICATION_ASYNC,
								closedResourceIds.data(), closedResourceIds.size(),
								logRecordList.data(), logRecordList.size(),
								NULL);
						}
						if (ev.getType() == PUT_CONTAINER || ev.getType() == CREATE_INDEX) {
							transactionManager_->remove(
								request.fixed_.pId_, request.fixed_.clientId_);
						}
					}
					catch (ContextNotFoundException&) {
						UTIL_TRACE_EXCEPTION_INFO(
							TRANSACTION_SERVICE, e, errorMessage.withDescription(
								"Context not found"));
					}
					if ( AUDIT_TRACE_CHECK ) {
						if ( AuditCategoryType(ev) != 0 ) {
							AUDIT_TRACE_ERROR_OPERATOR("")
						}
					}
					replyError(ec, alloc, ev.getSenderND(), ev.getType(),
						TXN_STATEMENT_ERROR, request, e);
				}
			}
		}
	}
	catch (std::exception&) {
		UTIL_TRACE_EXCEPTION(
			TRANSACTION_SERVICE, e, errorMessage.withDescription(
				"System error"));
		if ( AUDIT_TRACE_CHECK ) {
			if ( AuditCategoryType(ev) != 0 ) {
				AUDIT_TRACE_ERROR_OPERATOR("")
			}
		}
		replyError(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_NODE_ERROR, request, e);
		clusterService_->setError(ec, &e);
	}
}

/*!
	@brief Aborts on error
*/
util::XArray<uint8_t>* StatementHandler::abortOnError(TransactionContext & txn) {
	util::XArray<uint8_t>* log = NULL;

	if (txn.isActive()) {
		util::StackAllocator& alloc = txn.getDefaultAllocator();
		KeyDataStoreValue keyStoreValue = KeyDataStoreValue();
		try {
			keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		}
		catch (UserException& e) {
			UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
				"Container not found. (pId="
				<< txn.getPartitionId()
				<< ", clientId=" << txn.getClientId()
				<< ", containerId=" << txn.getContainerId()
				<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
		}

		if (keyStoreValue.containerId_ != UNDEF_CONTAINERID) {

			DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
			const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
			DSInputMes input(alloc, DS_ABORT, false);
			StackAllocAutoPtr<DSOutputMes> ret(alloc, NULL);
			ret.set(static_cast<DSOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
			TransactionManager::ContextSource dummySource; 
			log = appendDataStoreLog(alloc, txn,
				dummySource, txn.getLastStatementId(), keyStoreValue.storeType_, ret.get()->dsLog_);

		}
		else {
			transactionManager_->remove(txn);
		}
	}

	return log;
}

void StatementHandler::checkLockConflictStatus(
	const LockConflictStatus & status, LockConflictException & e) {
	const EventMonotonicTime elapsedMillis =
		status.emNow_ - status.initialConflictMillis_;
	const EventMonotonicTime timeoutMillis =
		static_cast<EventMonotonicTime>(status.txnTimeoutInterval_) * 1000;

	if (elapsedMillis >= timeoutMillis) {
		GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e,
			"Transaction timed out by lock conflict (" <<
			"elapsedMillis=" << elapsedMillis <<
			", timeoutMillis=" << timeoutMillis << ")"));
	}
}

void StatementHandler::retryLockConflictedRequest(
	EventContext & ec, Event & ev, const LockConflictStatus & status) {
	updateLockConflictStatus(ec, ev, status);
	ec.getEngine().addPending(
		ev, EventEngine::RESUME_AT_PARTITION_CHANGE);
}

StatementHandler::LockConflictStatus StatementHandler::getLockConflictStatus(
	const Event & ev, EventMonotonicTime emNow, const Request & request) {
	return getLockConflictStatus(
		ev, emNow, request.fixed_.cxtSrc_, request.optional_);
}

StatementHandler::LockConflictStatus StatementHandler::getLockConflictStatus(
	const Event & ev, EventMonotonicTime emNow,
	const TransactionManager::ContextSource & cxtSrc,
	const OptionSet & optionSet) {
	EventMonotonicTime initialConflictMillis;
	if (ev.getQueueingCount() <= 1) {
		initialConflictMillis = emNow;
	}
	else {
		initialConflictMillis =
			optionSet.get<Options::LOCK_CONFLICT_START_TIME>();
	}

	return LockConflictStatus(
		cxtSrc.txnTimeoutInterval_, initialConflictMillis, emNow);
}

void StatementHandler::updateLockConflictStatus(
	EventContext & ec, Event & ev, const LockConflictStatus & status) {
	if (ev.getQueueingCount() <= 1) {
		assert(ev.getQueueingCount() == 1);
		ev.incrementQueueingCount();
	}

	updateRequestOption<Options::LOCK_CONFLICT_START_TIME>(
		ec.getAllocator(), ev, status.initialConflictMillis_);
}

void StatementHandler::ConnectionOption::checkPrivilegeForOperator() {
	if (dbId_ != GS_PUBLIC_DB_ID && priv_ != ALL) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_DB_ACCESS_INVALID, "");
	}
}

void StatementHandler::ConnectionOption::checkForUpdate(bool forUpdate) {
	if (dbId_ != GS_PUBLIC_DB_ID && priv_ == READ && forUpdate == true) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_DB_ACCESS_INVALID, "");
	}
}

void StatementHandler::ConnectionOption::checkSelect(SQLParsedInfo & parsedInfo) {
	if (priv_ != ALL) {
		for (size_t i = 0; i < parsedInfo.syntaxTreeList_.size(); i++) {
			if (parsedInfo.syntaxTreeList_[i]->calcCommandType() != SyntaxTree::COMMAND_SELECT) {
				GS_THROW_USER_ERROR(GS_ERROR_TXN_DB_ACCESS_INVALID, "");
			}
		}
	}
}

bool StatementHandler::checkPrivilege(
		EventType command,
		UserType userType, RequestType requestType, bool isSystemMode,
		int32_t featureVersion,
		ContainerAttribute resourceSubType,
		ContainerAttribute expectedResourceSubType) {

	UNUSED_VARIABLE(command);

	bool granted = false;

	switch (requestType) {
	case Message::REQUEST_NOSQL:
		switch (resourceSubType) {
		case CONTAINER_ATTR_LARGE:
			granted = (isSystemMode == true);
			break;
		case CONTAINER_ATTR_VIEW:
			granted = (featureVersion >= StatementMessage::FEATURE_V4_2)
				&& (isSystemMode == true);
			break;
		case CONTAINER_ATTR_SINGLE:
			granted = true;
			break;
		case CONTAINER_ATTR_SINGLE_SYSTEM:
			granted = (isSystemMode == true);
			break;
		case CONTAINER_ATTR_SUB:
			granted = (isSystemMode == true);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_PARAMETER_INVALID,
					"Illeagal parameter. ContainerAttribute = " <<
					static_cast<int32_t>(resourceSubType));
		}
		break;

	case Message::REQUEST_NEWSQL:
		switch (resourceSubType) {
		case CONTAINER_ATTR_LARGE:
		case CONTAINER_ATTR_VIEW:
			granted = true;
			break;
		case CONTAINER_ATTR_SINGLE:
			granted = true;
			break;
		case CONTAINER_ATTR_SINGLE_SYSTEM:
			granted = (isSystemMode == true);
			break;
		case CONTAINER_ATTR_SUB:
			granted = (isSystemMode == true);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_PARAMETER_INVALID,
					"Illeagal parameter. ContainerAttribute = " <<
					static_cast<int32_t>(resourceSubType));
		}
		break;

	default:
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_PARAMETER_INVALID,
				"Illeagal parameter. RequestType = " <<
				static_cast<int32_t>(requestType));
	}

	if (expectedResourceSubType == CONTAINER_ATTR_ANY) {
		return granted;
	}
	else if (granted && (expectedResourceSubType != resourceSubType)) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_CONTAINER_ATTRIBUTE_UNMATCH,
				"Container attribute not match." <<
				" (expected=" << static_cast<int32_t>(expectedResourceSubType) <<
				", actual=" << static_cast<int32_t>(resourceSubType) << ")");
	}

	return granted;
}

bool StatementHandler::isMetaContainerVisible(
	const MetaContainer & metaContainer, int32_t visibility) {
	if ((visibility & StatementMessage::CONTAINER_VISIBILITY_META) == 0) {
		return false;
	}

	if ((visibility &
		StatementMessage::CONTAINER_VISIBILITY_INTERNAL_META) != 0) {
		return true;
	}

	const MetaContainerInfo& info = metaContainer.getMetaContainerInfo();
	return !info.internal_;
}

bool StatementHandler::isSupportedContainerAttribute(ContainerAttribute attribute) {
	switch (attribute) {
	case CONTAINER_ATTR_SINGLE:
	case CONTAINER_ATTR_SINGLE_SYSTEM:
	case CONTAINER_ATTR_LARGE:
	case CONTAINER_ATTR_SUB:
	case CONTAINER_ATTR_VIEW:
		return true;
	default:
		return false;
	}
}

void StatementHandler::checkDbAccessible(
	const char8_t* loginDbName, const char8_t* specifiedDbName
	, bool isNewSql
) {
	if (isNewSql) {
		return;
	}
	if (strcmp(loginDbName, specifiedDbName) != 0) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_CON_ACCESS_INVALID,
			"Only can access connect db :" << loginDbName <<
			", can not access db : " << specifiedDbName);
	}
}

const char8_t* StatementHandler::clusterRoleToStr(
	StatementHandler::ClusterRole role) {
	const ClusterRole allStatesSet =
		CROLE_MASTER + CROLE_FOLLOWER + CROLE_SUBMASTER;
	return role > allStatesSet ? clusterRoleStr[allStatesSet]
		: clusterRoleStr[role];
}

const char8_t* StatementHandler::partitionRoleTypeToStr(
	StatementHandler::PartitionRoleType role) {
	const PartitionRoleType allStatesSet =
		PROLE_OWNER + PROLE_BACKUP + PROLE_CATCHUP + PROLE_NONE;
	return role > allStatesSet ? partitionRoleTypeStr[allStatesSet]
		: partitionRoleTypeStr[role];
}

const char8_t* StatementHandler::partitionStatusToStr(
	StatementHandler::PartitionStatus status) {
	const PartitionStatus allStatesSet =
		PSTATE_ON + PSTATE_SYNC + PSTATE_OFF + PSTATE_STOP;
	return status > allStatesSet ? partitionStatusStr[allStatesSet]
		: partitionStatusStr[status];
}

bool StatementHandler::getApplicationNameByOptionsOrND(
	const OptionSet * optionSet, const NodeDescriptor * nd,
	util::String * nameStr, std::ostream * os) {

	const char8_t* name = (optionSet == NULL ?
		"" : optionSet->get<Options::APPLICATION_NAME>());
	bool found = (strlen(name) > 0);

	ConnectionOption* connOption = NULL;
	if (!found && nd != NULL) {
		connOption = &nd->getUserData<ConnectionOption>();
		if (connOption->getApplicationName(NULL)) {
			found = true;
		}
	}

	if (found) {
		if (connOption == NULL) {
			if (os != NULL) {
				*os << name;
			}
			if (nameStr != NULL) {
				*nameStr = name;
			}
		}
		else {
			if (os != NULL) {
				connOption->getApplicationName(*os);
			}
			if (nameStr != NULL) {
				connOption->getApplicationName(*nameStr);
			}
		}
	}

	return found;
}

const KeyConstraint& StatementHandler::getKeyConstraint(
	const OptionSet & optionSet, bool checkLength) const {
	return getKeyConstraint(
		optionSet.get<Options::CONTAINER_ATTRIBUTE>(), checkLength);
}

const KeyConstraint& StatementHandler::getKeyConstraint(
	ContainerAttribute containerAttribute, bool checkLength) const {
	const uint32_t CONTAINER_ATTR_IS_SYSTEM = 0x00000001;
	const uint32_t CONTAINER_ATTR_FOR_PARTITIONING = (CONTAINER_ATTR_LARGE & CONTAINER_ATTR_SUB);
	return keyConstraint_
		[((containerAttribute & CONTAINER_ATTR_IS_SYSTEM) != 0 ? 1 : 0)]
	[((containerAttribute & CONTAINER_ATTR_FOR_PARTITIONING) != 0 ? 1 : 0)]
	[(checkLength ? 1 : 0)];
}

DataStoreV4* StatementHandler::getDataStore(PartitionId pId) {
	DataStoreBase& dsBase =
		partitionList_->partition(pId).dataStore();
	return static_cast<DataStoreV4*>(&dsBase);
}

DataStoreBase* StatementHandler::getDataStore(PartitionId pId, StoreType type) {
	UNUSED_VARIABLE(type);

	DataStoreBase& dsBase = partitionList_->partition(pId).dataStore();
	return &dsBase;
}

DataStoreBase* StatementHandler::getDataStore(
		PartitionId pId, KeyDataStoreValue * keyStoreVal) {
	UNUSED_VARIABLE(keyStoreVal);

	DataStoreBase& dsBase = partitionList_->partition(pId).dataStore();
	return &dsBase;
}

KeyDataStore* StatementHandler::getKeyDataStore(PartitionId pId) {
	DataStoreBase& dsBase =
		partitionList_->partition(pId).keyStore();
	return static_cast<KeyDataStore*>(&dsBase);
}

LogManager<MutexLocker>* StatementHandler::getLogManager(PartitionId pId) {
	LogManager<MutexLocker>& logMgr =
		partitionList_->partition(pId).logManager();
	return &logMgr;
}

std::string StatementHandler::getAuditContainerName(util::StackAllocator &alloc, FullContainerKey* getcontainerKey) {
	if (getcontainerKey == NULL) {
		return "";
	}
	util::String containerName(alloc);
	getcontainerKey->toString(alloc, containerName);
	return containerName.c_str();
}

std::string StatementHandler::getClientAddress(const Event &ev) {
	util::NormalOStringStream oss;
	std::string work;
	if (ev.getSenderND().isEmpty()){
		return "";
	}
	oss << ev.getSenderND();
	work = oss.str();
	return work.substr(work.find("address=")+8,work.length()-work.find("address=")-9);
}

std::string StatementHandler::getClientAddress(const NodeDescriptor& nd) {
	util::NormalOStringStream oss;
	std::string work;
	oss << nd;
	work = oss.str();
	return work.substr(work.find("address=") + 8, work.length() - work.find("address=") - 9);
}

std::string StatementHandler::ConnectionOption::getAuditApplicationName() {
	util::LockGuard<util::Mutex> guard(mutex_);
	return applicationName_;
}

bool StatementHandler::ConnectionOption::getApplicationName(
	util::BasicString<
	char8_t, std::char_traits<char8_t>,
	util::StdAllocator<char8_t, void> > *name) {
	util::LockGuard<util::Mutex> guard(mutex_);
	if (name != NULL) {
		*name = applicationName_.c_str();
	}
	return !applicationName_.empty();
}

void StatementHandler::ConnectionOption::setFirstStep(
		ClientId & clientId, double storeMemoryAgingSwapRate,
		const util::TimeZone & timeZone, bool isLDAPAuthentication,
		int32_t acceptableFeatureVersion) {
	clientId_ = clientId;
	storeMemoryAgingSwapRate_ = storeMemoryAgingSwapRate;
	timeZone_ = timeZone;
	isLDAPAuthentication_ = isLDAPAuthentication;
	acceptableFeatureVersion_ = acceptableFeatureVersion;
}

void StatementHandler::ConnectionOption::setSecondStep(const char8_t* userName, const char8_t* digest, const char8_t* dbName, const char8_t* applicationName,
	bool isImmediateConsistency, int32_t txnTimeoutInterval, RequestType requestType) {
	util::LockGuard<util::Mutex> guard(mutex_);
	userName_ = userName;
	dbName_ = dbName;
	applicationName_ = applicationName;
	isImmediateConsistency_ = isImmediateConsistency;
	txnTimeoutInterval_ = txnTimeoutInterval;
	requestType_ = requestType;

	digest_ = digest;
}

void StatementHandler::ConnectionOption::setBeforeAuth(UserType userType, bool isAdminAndPublicDB) {
	userType_ = userType;
	isAdminAndPublicDB_ = isAdminAndPublicDB;
}

void StatementHandler::ConnectionOption::setAfterAuth(DatabaseId dbId, EventMonotonicTime authenticationTime,
	PrivilegeType priv, const char8_t* roleName) {
	dbId_ = dbId;
	authenticationTime_ = authenticationTime;
	priv_ = priv;
	if (roleName) {
		roleName_ = roleName;
	}
	isAuthenticated_ = true;
}

void StatementHandler::ConnectionOption::getHandlingClientId(ClientId & clientId) {
	util::LockGuard<util::Mutex> guard(mutex_);
	clientId = handlingClientId_;
}

void StatementHandler::ConnectionOption::setHandlingClientId(ClientId & clientId) {
	util::LockGuard<util::Mutex> guard(mutex_);
	handlingClientId_ = clientId;
	statementList_.insert(clientId.sessionId_);
}

void StatementHandler::ConnectionOption::getLoginInfo(
	SQLString & userName, SQLString & dbName, SQLString & applicationName) {
	util::LockGuard<util::Mutex> guard(mutex_);
	userName = userName_.c_str();
	dbName = dbName_.c_str();
	applicationName = applicationName_.c_str();
}

void StatementHandler::ConnectionOption::getSessionIdList(
	ClientId & clientId,
	util::XArray<SessionId> &sessionIdList) {
	util::LockGuard<util::Mutex> guard(mutex_);
	clientId = handlingClientId_;
	for (std::set<SessionId>::iterator idItr = statementList_.begin();
		idItr != statementList_.end(); idItr++) {
		sessionIdList.push_back(*idItr);
	}
}

void StatementHandler::ConnectionOption::setConnectionEnv(
	uint32_t paramId, const char8_t* value) {
	util::LockGuard<util::Mutex> guard(mutex_);
	envMap_[paramId] = value;
	updatedEnvBits_ |= (1 << paramId);
}

void StatementHandler::ConnectionOption::removeSessionId(ClientId & clientId) {
	util::LockGuard<util::Mutex> guard(mutex_);
	statementList_.erase(clientId.sessionId_);
}

bool StatementHandler::ConnectionOption::getConnectionEnv(
	uint32_t paramId, std::string & value, bool& hasData) {
	util::LockGuard<util::Mutex> guard(mutex_);
	if (envMap_.size() == 0) {
		hasData = false;
		return false;
	}
	else {
		hasData = true;
	}
	std::map<uint32_t, std::string>::const_iterator it =
		envMap_.find(paramId);
	if (it != envMap_.end()) {
		value = (*it).second;
		return true;
	}
	else {
		return false;
	}
}


void StatementHandler::ConnectionOption::getApplicationName(
	std::ostream & os) {
	util::LockGuard<util::Mutex> guard(mutex_);
	os << applicationName_;
}

void  StatementHandler::ConnectionOption::getDbApplicationName(util::String& dbName, util::String& applicationName) {
	util::LockGuard<util::Mutex> guard(mutex_);
	dbName = dbName_.c_str();
	applicationName = applicationName_.c_str(); 
}

void StatementHandler::ErrorMessage::format(std::ostream & os) const {
	os << GS_EXCEPTION_MESSAGE(elements_.cause_);
	os << " (" << elements_.description_;
	formatParameters(os);
	os << ")";
}

void StatementHandler::ConnectionOption::initializeCoreInfo() {
	util::LockGuard<util::Mutex> guard(mutex_);

	userName_.clear();
	digest_.clear();
	dbName_.clear();
	applicationName_ = "";

	clientId_ = ClientId();

	handlingClientId_ = ClientId();
	statementList_.clear();
	envMap_.clear();

}

void StatementHandler::ErrorMessage::formatParameters(std::ostream & os) const {
	const NodeDescriptor& nd = elements_.ev_.getSenderND();

	if (getApplicationNameByOptionsOrND(
		elements_.options_, &nd, NULL, NULL)) {
		os << ", application=";
		getApplicationNameByOptionsOrND(
			elements_.options_, &nd, NULL, &os);
	}

	os << ", source=" << nd;
	os << ", eventType=" << EventTypeUtility::getEventTypeName(elements_.stmtType_) << "(" << elements_.stmtType_ << ")";
	os << ", partition=" << elements_.pId_;

	if (elements_.stmtId_ != UNDEF_STATEMENTID) {
		os << ", statementId=" << elements_.stmtId_;
	}

	if (elements_.clientId_ != ClientId()) {
		os << ", transaction=" << elements_.clientId_;
	}

	if (elements_.containerId_ != UNDEF_CONTAINERID) {
		os << ", containerId=" << elements_.containerId_;
	}
	if (!nd.isEmpty()) {
		if (nd.getUserData<ConnectionOption>().isPublicConnection()) {
			os << ", connection=PUBLIC";
		}
	}
}

ConnectHandler::ConnectHandler(ProtocolVersion currentVersion,
	const ProtocolVersion * acceptableProtocolVersions) :
	currentVersion_(currentVersion),
	acceptableProtocolVersions_(acceptableProtocolVersions) {
}

/*!
	@brief Handler Operator
*/
void ConnectHandler::operator()(EventContext & ec, Event & ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();

	Response response(alloc);
	InMessage inMes(ev.getPartitionId(), ev.getType());
	ConnectRequest& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_ANY;
		const PartitionStatus partitionStatus = PSTATE_ANY;
		if (isNewSQL_) {
			checkExecutable(clusterRole);
		}
		else {
			checkExecutable(
				request.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);
		}

		checkClientVersion(request.clientVersion_);

		ConnectionOption& connOption =
			ev.getSenderND().getUserData<ConnectionOption>();
		connOption.clientVersion_ = request.clientVersion_;

		if (connOption.requestType_ == Message::REQUEST_NOSQL) {
			connOption.keepaliveTime_ = ec.getHandlerStartMonotonicTime();
		}

		ec.getEngine().acceptConnectionControlInfo(
			ev.getSenderND(), in);

		OutMessage outMes(request.oldStmtId_, TXN_STATEMENT_SUCCESS,
			connOption.authMode_, currentVersion_, transactionManager_->isLDAPAuthentication());
		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, NO_REPLICATION);
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void ConnectHandler::checkClientVersion(ProtocolVersion clientVersion) {
	for (const ProtocolVersion* acceptableVersions = acceptableProtocolVersions_;;
		acceptableVersions++) {

		if (clientVersion == *acceptableVersions) {
			return;
		}
		else if (*acceptableVersions == PROTOCOL_VERSION_UNDEFINED) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_CLIENT_VERSION_NOT_ACCEPTABLE,
				"(connectClientVersion=" << clientVersion
				<< ", serverVersion="
				<< currentVersion_ << ")");
		}
	}
}

EventByteOutStream ConnectHandler::encodeCommonPart(
	Event & ev, OldStatementId stmtId, StatementExecStatus status) {
	try {
		ev.setPartitionIdSpecified(false);

		EventByteOutStream out = ev.getOutStream();

		out << stmtId;
		out << status;

		return out;
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void ConnectHandler::replySuccess(
	EventContext & ec, util::StackAllocator & alloc,
	const NodeDescriptor & ND, EventType stmtType,
	const ConnectRequest & request, Serializable & message, bool) {
	util::StackAllocator::Scope scope(alloc);

	Event ev(ec, stmtType, request.pId_);
	ev.setPartitionIdSpecified(false);
	EventByteOutStream out = ev.getOutStream();
	message.encode(out);
	ec.getEngine().exportConnectionControlInfo(ND, out);

	if (ev.getExtraMessageCount() > 0) {
		EventEngine::EventRequestOption option;
		option.timeoutMillis_ = 0;
		ec.getEngine().send(ev, ND, &option);
	}
	else {
		ec.getEngine().send(ev, ND);
	}
}

void ConnectHandler::replyError(EventContext & ec, util::StackAllocator & alloc,
	const NodeDescriptor & ND, EventType stmtType, StatementExecStatus status,
	const ConnectRequest & request, const std::exception & e) {
	util::StackAllocator::Scope scope(alloc);

	Event ev(ec, stmtType, request.pId_);
	EventByteOutStream out = encodeCommonPart(ev, request.oldStmtId_, status);
	encodeException(out, e, ND);

	if (!ND.isEmpty()) {
		ec.getEngine().send(ev, ND);
	}
}

void ConnectHandler::handleError(
	EventContext & ec, util::StackAllocator & alloc,
	Event & ev, const ConnectRequest & request, std::exception & e) {
	util::StackAllocator::Scope scope(alloc);

	FixedRequest fixedRequest(getRequestSource(ev));
	fixedRequest.cxtSrc_.stmtId_ = request.oldStmtId_;
	ErrorMessage errorMessage(e, "", ev, fixedRequest);

	try {
		try {
			throw;

		}
		catch (EncodeDecodeException&) {
			UTIL_TRACE_EXCEPTION(
				TRANSACTION_SERVICE, e, errorMessage.withDescription(
					"Failed to encode or decode message"));
			replyError(ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_ERROR, request, e);

		}
		catch (DenyException&) {
			UTIL_TRACE_EXCEPTION(
				TRANSACTION_SERVICE, e, errorMessage.withDescription(
					"Request denied"));
			replyError(ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_DENY, request, e);

		}
		catch (std::exception&) {
			if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
				throw;
			}
			else {
				UTIL_TRACE_EXCEPTION(
					TRANSACTION_SERVICE, e, errorMessage.withDescription(
						"User error"));
				replyError(ec, alloc, ev.getSenderND(), ev.getType(),
					TXN_STATEMENT_ERROR, request, e);
			}
		}
	}
	catch (std::exception&) {
		UTIL_TRACE_EXCEPTION(
			TRANSACTION_SERVICE, e, errorMessage.withDescription(
				"System error"));
		replyError(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_NODE_ERROR, request, e);
		clusterService_->setError(ec, &e);
	}
}

/*!
	@brief Handler Operator
*/
void GetPartitionAddressHandler::operator()(EventContext & ec, Event & ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);
	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {
		EVENT_START(ec, ev, transactionManager_, UNDEF_DBID, false);

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);

		bool usePublic = usePublicConection(request);

		bool& masterResolving = inMes.masterResolving_;

		if ( AUDIT_TRACE_CHECK ) {
			AUDIT_TRACE_INFO_OPERATOR("")
		}
		const ClusterRole clusterRole =
			CROLE_MASTER | (masterResolving ? CROLE_FOLLOWER : 0);
		const PartitionRoleType partitionRole = PROLE_ANY;
		const PartitionStatus partitionStatus = PSTATE_ANY;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), ANY_CONSISTENCY);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		util::XArrayOutStream<> arrayOut(response.binaryData_);
		util::ByteStream<util::XArrayOutStream<> > out(arrayOut);

		out << partitionTable_->getPartitionNum();

		const NodeId ownerNodeId = (masterResolving ?
			UNDEF_NODEID : partitionTable_->getOwner(request.fixed_.pId_));
		if (ownerNodeId != UNDEF_NODEID) {
			NodeAddress& address = partitionTable_->getNodeAddress(
				ownerNodeId, TRANSACTION_SERVICE, usePublic);
			out << static_cast<uint8_t>(1);
			out << std::pair<const uint8_t*, size_t>(
				reinterpret_cast<const uint8_t*>(&address.address_),
				sizeof(AddressType));
			out << static_cast<uint32_t>(address.port_);
		}
		else {
			out << static_cast<uint8_t>(0);
		}

		util::XArray<NodeId> backupNodeIds(alloc);
		if (!masterResolving) {
			partitionTable_->getBackup(request.fixed_.pId_, backupNodeIds);
		}

		const uint8_t numBackup = static_cast<uint8_t>(backupNodeIds.size());
		out << numBackup;
		for (uint8_t i = 0; i < numBackup; i++) {

			NodeAddress& address = partitionTable_->getNodeAddress(
				backupNodeIds[i], TRANSACTION_SERVICE, usePublic);
			out << std::pair<const uint8_t*, size_t>(
				reinterpret_cast<const uint8_t*>(&address.address_),
				sizeof(AddressType));
			out << static_cast<uint32_t>(address.port_);
		}

		if (masterResolving) {
			encodeClusterInfo(
				out, ec.getEngine(), *partitionTable_,
				CONTAINER_HASH_MODE_CRC32, TRANSACTION_SERVICE, usePublic);
		}

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false),
			&(response.binaryData_));
		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, NO_REPLICATION);
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

template <typename S>
void GetPartitionAddressHandler::encodeClusterInfo(
	S & out, EventEngine & ee,
	PartitionTable & partitionTable, ContainerHashMode hashMode, ServiceType serviceType, bool usePublic) {
	const NodeId masterNodeId = partitionTable.getMaster();
	if (masterNodeId == UNDEF_NODEID) {
		TXN_THROW_DENY_ERROR(GS_ERROR_TXN_CLUSTER_ROLE_UNMATCH,
			"(required={MASTER}, actual={SUB_CLUSTER})");
	}
	const NodeDescriptor& masterND = ee.getServerND(masterNodeId);
	if (masterND.isEmpty()) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID, "ND is empty");
	}

	const int8_t masterMatched = masterND.isSelf();
	out << masterMatched;

	out << hashMode;

	NodeAddress& address = partitionTable.getNodeAddress(masterNodeId, serviceType, usePublic);

	out << std::pair<const uint8_t*, size_t>(
		reinterpret_cast<const uint8_t*>(&address.address_),
		sizeof(AddressType));
	out << static_cast<uint32_t>(address.port_);
}

/*!
	@brief Handler Operator
*/
void GetPartitionContainerNamesHandler::operator()(
	EventContext & ec, Event & ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);
	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		TRANSACTION_EVENT_START(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0));

		bool isNewSql = isNewSQL(request);
		DatabaseId dbId = connOption.dbId_;
		if (isNewSql) {
			dbId = request.optional_.get<Options::DB_VERSION_ID>();
		}

		int64_t& start = inMes.start_;
		ResultSize& limit = inMes.limit_;
		KeyDataStore::ContainerCondition& containerCondition = inMes.containerCondition_;  

		if ( AUDIT_TRACE_CHECK ) {
			AUDIT_TRACE_INFO_OPERATOR("")
		}
		checkSizeLimit(limit);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), ANY_CONSISTENCY);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
		checkDbAccessible(
			connOption.dbName_.c_str(),
			request.optional_.get<Options::DB_NAME>()
			, isNewSql
		);
		KeyDataStore::ContainerCondition validContainerCondition(alloc);
		for (util::Set<ContainerAttribute>::const_iterator itr =
			containerCondition.getAttributes().begin();
			itr != containerCondition.getAttributes().end(); itr++) {
			if (!isSupportedContainerAttribute(*itr)) {
				continue;
			}
			if (checkPrivilege(
				ev.getType(),
				connOption.userType_, connOption.requestType_,
				request.optional_.get<Options::SYSTEM_MODE>(),
				request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>(),
				*itr)) {
				validContainerCondition.insertAttribute(*itr);
			}
		}

		KeyDataStore* keyStore = getKeyDataStore(txn.getPartitionId());

		uint64_t containerNum = keyStore->getContainerCount(txn,
			dbId, validContainerCondition);
		util::XArray<FullContainerKey> containerNameList(alloc);

		keyStore->getContainerNameList(txn, start,
			limit, dbId, validContainerCondition,
			containerNameList);
		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false),
			containerNum, containerNameList);
		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, NO_REPLICATION);

	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void GetContainerPropertiesHandler::operator()(EventContext & ec, Event & ev)

{
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		TRANSACTION_EVENT_START(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0));

		const char8_t* dbName = request.optional_.get<Options::DB_NAME>();
		const int32_t visibility =
			request.optional_.get<Options::CONTAINER_VISIBILITY>();

		ContainerNameList& containerNameList = inMes.containerNameList_;
		uint32_t& propFlags = inMes.propFlags_;
		const uint32_t propTypeCount = util::countNumOfBits(propFlags);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), ANY_CONSISTENCY);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		Event reply(ec, ev.getType(), ev.getPartitionId());

		EventByteOutStream out = encodeCommonPart(
			reply, request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS);

		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
		const int32_t acceptableFeatureVersion =
			request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>();

		txn.setAuditInfo(&ev,&ec,NULL);
		int64_t currentTime = ec.getHandlerStartTime().getUnixTime();
		util::String containerNameStr(alloc);
		bool isNewSql = isNewSQL(request);
		for (ContainerNameList::iterator it = containerNameList.begin();
			it != containerNameList.end(); ++it) {
			const FullContainerKey queryContainerKey(
				alloc, getKeyConstraint(CONTAINER_ATTR_ANY, false), (*it)->data(), (*it)->size());
			checkLoggedInDatabase(
				connOption.dbId_, connOption.dbName_.c_str(),
				queryContainerKey.getComponents(alloc).dbId_,
				request.optional_.get<Options::DB_NAME>()
				, isNewSql
			);
			bool isCaseSensitive = getCaseSensitivity(request).isContainerNameCaseSensitive();
			KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(txn,
				queryContainerKey, isCaseSensitive);

			if (keyStoreValue.containerId_ != UNDEF_CONTAINERID &&
				!checkPrivilege(ev.getType(),
					connOption.userType_, connOption.requestType_,
					request.optional_.get<Options::SYSTEM_MODE>(),
					request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>(),
					keyStoreValue.attribute_,
					request.optional_.get<Options::CONTAINER_ATTRIBUTE>())) {
				keyStoreValue = KeyDataStoreValue();
			}

			if (it - containerNameList.begin() == 1) {
				encodeResultListHead(
					out, static_cast<uint32_t>(containerNameList.size()));
			}

			if (keyStoreValue.containerId_ == UNDEF_CONTAINERID) {
				MetaStore metaStore(*getDataStore(txn.getPartitionId()));

				MetaType::NamingType namingType;
				namingType = static_cast<MetaType::NamingType>(
					request.optional_.get<Options::META_NAMING_TYPE>());

				MetaContainer* metaContainer = metaStore.getContainer(
					txn, queryContainerKey, namingType);

				if (metaContainer != NULL && !isMetaContainerVisible(
					*metaContainer, visibility)) {
					metaContainer = NULL;
				}

				if (metaContainer != NULL &&
					metaContainer->getMetaContainerInfo().nodeDistribution_ &&
					acceptableFeatureVersion < Message::FEATURE_V4_2) {
					metaContainer = NULL;
				}

				const bool forMeta = true;
				encodeContainerProps(
					txn, out, metaContainer, propFlags, propTypeCount,
					forMeta, emNow, containerNameStr, dbName
					, currentTime
					, acceptableFeatureVersion
				);

				continue;
			}
			assert(keyStoreValue.containerId_ != UNDEF_CONTAINERID);
			DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
			const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
			DSInputMes input(alloc, DS_GET_CONTAINER_OBJECT, ANY_CONTAINER, true);
			StackAllocAutoPtr<DSContainerOutputMes> ret(alloc, NULL);
			{
				ret.set(static_cast<DSContainerOutputMes*>(
					ds->exec(&txn, &keyStoreValue, &input)));
			}
			BaseContainer* container = ret.get()->container_;
			checkContainerExistence(container);
			{
				const bool forMeta = false;
				encodeContainerProps(
					txn, out, container, propFlags, propTypeCount,
					forMeta, emNow, containerNameStr, dbName
					, currentTime
					, acceptableFeatureVersion
				);
			}
		}

		ec.getEngine().send(reply, ev.getSenderND());
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

template<typename C>
void GetContainerPropertiesHandler::encodeContainerProps(
	TransactionContext & txn, EventByteOutStream & out, C * container,
	uint32_t propFlags, uint32_t propTypeCount, bool forMeta,
	EventMonotonicTime emNow, util::String & containerNameStr,
	const char8_t* dbName
	, int64_t currentTime
	, int32_t acceptableFeatureVersion
) {
	util::StackAllocator& alloc = txn.getDefaultAllocator();

	const bool found = (container != NULL);
	out << static_cast<uint8_t>(found);

	if (!found) {
		return;
	}

	const FullContainerKey realContainerKey = container->getContainerKey(txn);
	containerNameStr.clear();
	realContainerKey.toString(alloc, containerNameStr);

	encodePropsHead(out, propTypeCount);

	if ((propFlags & (1 << CONTAINER_PROPERTY_ID)) != 0) {
		const ContainerId containerId = container->getContainerId();
		const SchemaVersionId schemaVersionId =
			container->getVersionId();

		MetaDistributionType metaDistType = META_DIST_NONE;
		ContainerId metaContainerId = UNDEF_CONTAINERID;
		int8_t metaNamingType = -1;
		if (forMeta) {
			MetaContainer* metaContainer =
				static_cast<MetaContainer*>(container);
			metaDistType = META_DIST_FULL;
			if (metaContainer->getMetaContainerInfo().nodeDistribution_) {
				metaDistType = META_DIST_NODE;
			}
			metaContainerId = metaContainer->getMetaContainerId();
			metaNamingType = metaContainer->getColumnNamingType();
		}

		encodeId(
			out, schemaVersionId, containerId, realContainerKey,
			metaContainerId, metaDistType, metaNamingType);
	}

	if ((propFlags & (1 << CONTAINER_PROPERTY_SCHEMA)) != 0) {
		if (acceptableFeatureVersion < StatementMessage::FEATURE_V4_3) {
			if (container->getRowKeyColumnNum() > 1) {
				GS_THROW_USER_ERROR(
						GS_ERROR_TXN_CLIENT_VERSION_NOT_ACCEPTABLE,
						"Requested client does not support composite row key "
						"(clientVersion=" << acceptableFeatureVersion << ")");
			}
		}

		checkSchemaFeatureVersion(*container, acceptableFeatureVersion);

		const ContainerType containerType = container->getContainerType();

		const bool optionIncluded = true;
		util::XArray<uint8_t> containerInfo(alloc);
		container->getContainerInfo(txn, containerInfo, optionIncluded, false);
		encodeSchema(out, containerType, containerInfo);
	}

	if ((propFlags & (1 << CONTAINER_PROPERTY_INDEX)) != 0) {
		util::Vector<IndexInfo> indexInfoList(alloc);
		container->getIndexInfoList(txn, indexInfoList);

		for (util::Vector<IndexInfo>::iterator itr = indexInfoList.begin();
			itr != indexInfoList.end();) {
			checkIndexInfoVersion(*itr, acceptableFeatureVersion);

			if (itr->columnIds_.size() > 1) {
				itr = indexInfoList.erase(itr);
			}
			else {
				++itr;
			}
		}

		encodeIndex(out, indexInfoList);
	}

	if ((propFlags & (1 << CONTAINER_PROPERTY_EVENT_NOTIFICATION)) !=
		0) {
		util::XArray<char*> urlList(alloc);
		util::XArray<uint32_t> urlLenList(alloc);


		encodeEventNotification(out, urlList, urlLenList);
	}

	if ((propFlags & (1 << CONTAINER_PROPERTY_TRIGGER)) != 0) {
		util::XArray<const uint8_t*> triggerList(alloc);
		encodeTrigger(out, triggerList);
	}

	if ((propFlags & (1 << CONTAINER_PROPERTY_ATTRIBUTES)) != 0) {
		encodeAttributes(out, container->getAttribute());
	}

	if ((propFlags & (1 << CONTAINER_PROPERTY_INDEX_DETAIL)) != 0) {
		util::Vector<IndexInfo> indexInfoList(alloc);
		if (container->getAttribute() == CONTAINER_ATTR_LARGE) {
			util::StackAllocator& alloc = txn.getDefaultAllocator();
			TablePartitioningIndexInfo tablePartitioningIndexInfo(alloc);
			decodeLargeRow(
				NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
				alloc, txn, container->getDataStore(),
				dbName, container, tablePartitioningIndexInfo, emNow);
			tablePartitioningIndexInfo.getIndexInfoList(alloc, indexInfoList);
		}
		else {
			container->getIndexInfoList(txn, indexInfoList);
		}

		checkIndexInfoVersion(indexInfoList, acceptableFeatureVersion);
		encodeIndexDetail(out, indexInfoList);
	}

	if ((propFlags & (1 << CONTAINER_PROPERTY_PARTITIONING_METADATA)) != 0) {
		encodePartitioningMetaData(
			out, txn, emNow, *container, container->getAttribute(),
			dbName, containerNameStr.c_str()
			, currentTime, acceptableFeatureVersion
		);
	}

	if ((propFlags & (1 << CONTAINER_PROPERTY_NULLS_STATISTICS)) != 0) {
		util::XArray<uint8_t> nullsList(alloc);
		container->getNullsStats(nullsList);
		encodeNulls(out, nullsList);
	}
}

void GetContainerPropertiesHandler::getPartitioningTableIndexInfo(
	TransactionContext & txn, EventMonotonicTime emNow, DataStoreV4 & dataStore,
	BaseContainer & largeContainer, const char8_t* containerName,
	const char* dbName, util::Vector<IndexInfo> &indexInfoList) {
	try {
		util::NormalOStringStream queryStr;
		queryStr << "SELECT * WHERE key = '"
			<< NoSQLUtils::LARGE_CONTAINER_KEY_INDEX
			<< "'";
		FetchOption fetchOption;
		fetchOption.limit_ = UINT64_MAX;
		fetchOption.size_ = UINT64_MAX;

		ResultSet* rs = dataStore.getResultSetManager()->create(
			txn, largeContainer.getContainerId(),
			largeContainer.getVersionId(), emNow, NULL);
		const ResultSetGuard rsGuard(txn, dataStore, *rs);
		QueryProcessor::executeTQL(
			txn, largeContainer, fetchOption.limit_, TQLInfo(dbName, NULL, queryStr.str().c_str()), *rs);

		QueryProcessor::fetch(
			txn, largeContainer, 0, fetchOption.size_, rs);
		const uint64_t rowCount = rs->getResultNum();

		InputMessageRowStore rowStore(
			dataStore.getConfig(),
			largeContainer.getColumnInfoList(), largeContainer.getColumnNum(),
			rs->getRowDataFixedPartBuffer()->data(),
			static_cast<uint32_t>(rs->getRowDataFixedPartBuffer()->size()),
			rs->getRowDataVarPartBuffer()->data(),
			static_cast<uint32_t>(rs->getRowDataVarPartBuffer()->size()),
			rowCount, rs->isRowIdInclude());

		NoSQLUtils::decodePartitioningTableIndexInfo(
			txn.getDefaultAllocator(), rowStore, indexInfoList);

	}
	catch (std::exception& e) {
		GS_RETHROW_USER_ERROR(
				e, GS_EXCEPTION_MERGE_MESSAGE(
						e, "Failed to get partitioning table index metadata ("
						"containerName=" << containerName << ")"));
	}
}

void GetContainerPropertiesHandler::encodeResultListHead(
	EventByteOutStream & out, uint32_t totalCount) {
	try {
		out << (totalCount - 1);
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodePropsHead(
	EventByteOutStream & out, uint32_t propTypeCount) {
	try {
		out << propTypeCount;
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodeId(
	EventByteOutStream & out, SchemaVersionId schemaVersionId,
	ContainerId containerId, const FullContainerKey & containerKey,
	ContainerId metaContainerId, MetaDistributionType metaDistType,
	int8_t metaNamingType) {
	try {
		encodeEnumData<EventByteOutStream, ContainerProperty>(out, CONTAINER_PROPERTY_ID);

		const size_t sizePos = out.base().position();
		uint32_t size = 0;
		out << size;

		out << schemaVersionId;
		out << containerId;
		encodeContainerKey(out, containerKey);
		encodeMetaId(out, metaContainerId, metaDistType, metaNamingType);

		const size_t lastPos = out.base().position();
		size = static_cast<uint32_t>(lastPos - sizePos - sizeof(size));
		out.base().position(sizePos);
		out << size;
		out.base().position(lastPos);
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodeMetaId(
	EventByteOutStream & out, ContainerId metaContainerId,
	MetaDistributionType metaDistType, int8_t metaNamingType) {
	if (metaContainerId == UNDEF_CONTAINERID) {
		return;
	}

	try {
		encodeEnumData<EventByteOutStream, MetaDistributionType>(out, metaDistType);

		const size_t sizePos = out.base().position();
		uint32_t size = 0;
		out << size;

		out << metaContainerId;
		out << metaNamingType;

		const size_t lastPos = out.base().position();
		size = static_cast<uint32_t>(lastPos - sizePos - sizeof(size));
		out.base().position(sizePos);
		out << size;
		out.base().position(lastPos);
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodeSchema(EventByteOutStream & out,
	ContainerType containerType,
	const util::XArray<uint8_t> &serializedCollectionInfo) {
	try {
		encodeEnumData<EventByteOutStream, ContainerProperty>(out, CONTAINER_PROPERTY_SCHEMA);

		const uint32_t size = static_cast<uint32_t>(
			sizeof(uint8_t) + serializedCollectionInfo.size());
		out << size;
		out << containerType;
		out << std::pair<const uint8_t*, size_t>(
			serializedCollectionInfo.data(), serializedCollectionInfo.size());
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodeIndex(
	EventByteOutStream & out, const util::Vector<IndexInfo> &indexInfoList) {
	try {
		encodeEnumData<EventByteOutStream, ContainerProperty>(out, CONTAINER_PROPERTY_INDEX);

		const size_t sizePos = out.base().position();
		uint32_t size = 0;
		out << size;

		const uint32_t num = static_cast<uint32_t>(indexInfoList.size());
		out << num;
		for (uint32_t i = 0; i < num; i++) {
			out << indexInfoList[i].columnIds_[0];
			encodeEnumData<EventByteOutStream, MapType>(out, indexInfoList[i].mapType);
		}

		const size_t lastPos = out.base().position();
		size = static_cast<uint32_t>(lastPos - sizePos - sizeof(size));
		out.base().position(sizePos);
		out << size;
		out.base().position(lastPos);
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodeEventNotification(
	EventByteOutStream & out, const util::XArray<char*> &urlList,
	const util::XArray<uint32_t> &urlLenList) {
	try {
		encodeEnumData<EventByteOutStream, ContainerProperty>(
			out, CONTAINER_PROPERTY_EVENT_NOTIFICATION);

		const size_t sizePos = out.base().position();
		uint32_t size = 0;
		out << size;

		const uint32_t num = static_cast<uint32_t>(urlLenList.size());
		out << num;
		for (uint32_t i = 0; i < num; i++) {
			out << urlLenList[i] + 1;
			out << std::pair<const uint8_t*, size_t>(
				reinterpret_cast<const uint8_t*>(urlList[i]),
				static_cast<size_t>(urlLenList[i] + 1));
		}

		const size_t lastPos = out.base().position();
		size = static_cast<uint32_t>(lastPos - sizePos - sizeof(size));
		out.base().position(sizePos);
		out << size;
		out.base().position(lastPos);
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodeTrigger(
	EventByteOutStream & out, const util::XArray<const uint8_t*> &triggerList) {
	try {
		encodeEnumData<EventByteOutStream, ContainerProperty>(out, CONTAINER_PROPERTY_TRIGGER);

		const size_t sizePos = out.base().position();
		uint32_t size = 0;
		out << size;

		const uint32_t num = static_cast<uint32_t>(triggerList.size());
		out << num;

		const size_t lastPos = out.base().position();
		size = static_cast<uint32_t>(lastPos - sizePos - sizeof(size));
		out.base().position(sizePos);
		out << size;
		out.base().position(lastPos);
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodeAttributes(
	EventByteOutStream & out, const ContainerAttribute containerAttribute) {
	try {
		encodeEnumData<EventByteOutStream, ContainerProperty>(out, CONTAINER_PROPERTY_ATTRIBUTES);
		int32_t attribute = static_cast<int32_t>(containerAttribute);

		const uint32_t size = sizeof(ContainerAttribute);
		out << size;
		out << attribute;
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodeIndexDetail(
	EventByteOutStream & out, const util::Vector<IndexInfo> &indexInfoList) {
	try {
		encodeEnumData<EventByteOutStream, ContainerProperty>(out, CONTAINER_PROPERTY_INDEX_DETAIL);

		const size_t sizePos = out.base().position();
		uint32_t size = 0;
		out << size;

		const uint32_t num = static_cast<uint32_t>(indexInfoList.size());
		out << num;

		for (size_t i = 0; i < indexInfoList.size(); i++) {
			encodeIndexInfo(out, indexInfoList[i]);
		}

		const size_t lastPos = out.base().position();
		size = static_cast<uint32_t>(lastPos - sizePos - sizeof(size));
		out.base().position(sizePos);
		out << size;
		out.base().position(lastPos);
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodeNulls(
	EventByteOutStream & out, const util::XArray<uint8_t> &nullsList) {
	try {
		encodeEnumData<EventByteOutStream, ContainerProperty>(out, CONTAINER_PROPERTY_NULLS_STATISTICS);

		const size_t sizePos = out.base().position();
		uint32_t size = 0;
		out << size;

		const uint32_t num = static_cast<uint32_t>(nullsList.size());
		out << num;
		for (size_t i = 0; i < nullsList.size(); i++) {
			out << nullsList[i];
		}

		const size_t lastPos = out.base().position();
		size = static_cast<uint32_t>(lastPos - sizePos - sizeof(size));
		out.base().position(sizePos);
		out << size;
		out.base().position(lastPos);
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::checkIndexInfoVersion(
	const util::Vector<IndexInfo> indexInfoList,
	int32_t acceptableFeatureVersion) {
	for (util::Vector<IndexInfo>::const_iterator itr = indexInfoList.begin();
		itr != indexInfoList.end(); ++itr) {
		checkIndexInfoVersion(*itr, acceptableFeatureVersion);
	}
}

void GetContainerPropertiesHandler::checkIndexInfoVersion(
	const IndexInfo & info, int32_t acceptableFeatureVersion) {
	if (acceptableFeatureVersion < StatementMessage::FEATURE_V4_3 &&
			info.columnIds_.size() > 1) {
		GS_THROW_USER_ERROR(
				GS_ERROR_TXN_CLIENT_VERSION_NOT_ACCEPTABLE,
				"Requested client does not support composite column index "
				"(clientVersion=" << acceptableFeatureVersion << ")");
	}
}

void GetContainerPropertiesHandler::encodePartitioningMetaData(
	EventByteOutStream & out,
	TransactionContext & txn, EventMonotonicTime emNow,
	BaseContainer & largeContainer, ContainerAttribute attribute,
	const char8_t* dbName, const char8_t* containerName
	, int64_t currentTime, int32_t acceptableFeatureVersion
) {
	try {
		util::StackAllocator& alloc = txn.getDefaultAllocator();

		encodeEnumData<EventByteOutStream, ContainerProperty>(out, CONTAINER_PROPERTY_PARTITIONING_METADATA);

		const size_t sizePos = out.base().position();
		uint32_t size = 0;
		out << size;

		int32_t distributedPolicy = 1;

		if (attribute != CONTAINER_ATTR_LARGE) {
			return;
		}

		TablePartitioningInfo<util::StackAllocator> partitioningInfo(alloc);
		decodeLargeRow(NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
			alloc, txn, getDataStore(txn.getPartitionId()), dbName,
			&largeContainer, partitioningInfo, emNow);

		bool isTableExpiration = partitioningInfo.isTableExpiration();
		if (isTableExpiration) {
			distributedPolicy = 2;
		}

		out << distributedPolicy;

		int32_t partitioningType = static_cast<int32_t>(partitioningInfo.partitionType_);
		out << partitioningType;

		int32_t distributedMethod = 0;
		out << distributedMethod;

		if (SyntaxTree::isIncludeHashPartitioningType(partitioningInfo.partitionType_)) {
			out << static_cast<int32_t>(partitioningInfo.partitioningNum_);
		}
		if (SyntaxTree::isRangePartitioningType(partitioningInfo.partitionType_)) {
			const TupleList::TupleColumnType partitionColumnOriginalType =
					partitioningInfo.partitionColumnType_;

			const int8_t typeOrdinal =
					ValueProcessor::getPrimitiveColumnTypeOrdinal(
							convertTupleTypeToNoSQLType(
									partitionColumnOriginalType), false);
			out << typeOrdinal;

			switch (partitionColumnOriginalType)
			{
			case TupleList::TYPE_BYTE:
			{
				int8_t value = static_cast<int8_t>(partitioningInfo.intervalValue_);
				out << value;
			}
			break;
			case TupleList::TYPE_SHORT:
			{
				int16_t value = static_cast<int16_t>(partitioningInfo.intervalValue_);
				out << value;
			}
			break;
			case TupleList::TYPE_INTEGER:
			{
				int32_t value = static_cast<int32_t>(partitioningInfo.intervalValue_);
				out << value;
			}
			break;
			case TupleList::TYPE_LONG:
			case TupleList::TYPE_TIMESTAMP:
			{
				int64_t value = static_cast<int64_t>(partitioningInfo.intervalValue_);
				out << value;
			}
			break;
			case TupleList::TYPE_MICRO_TIMESTAMP:
			{
				MicroTimestamp value = ValueProcessor::getMicroTimestamp(
						static_cast<Timestamp>(partitioningInfo.intervalValue_));
				out.writeAll(&value, sizeof(value));
			}
			break;
			case TupleList::TYPE_NANO_TIMESTAMP:
			{
				NanoTimestamp value = ValueProcessor::getNanoTimestamp(
						static_cast<Timestamp>(partitioningInfo.intervalValue_));
				out.writeAll(&value, sizeof(value));
			}
			break;
			default:
				GS_THROW_USER_ERROR(GS_ERROR_TXN_CONTAINER_PROPERTY_INVALID,
					"Not supported type=" << partitionColumnOriginalType);
			}
			int8_t unitValue = -1;
			if (TupleColumnTypeUtils::isTimestampFamily(
					partitionColumnOriginalType)) {
				unitValue = static_cast<int8_t>(partitioningInfo.intervalUnit_);
				StatementHandler::checkMetaTableFeatureVersion(unitValue, static_cast<uint8_t>(acceptableFeatureVersion));
			}
			out << unitValue;
		}
		out << static_cast<int64_t>(partitioningInfo.partitioningVersionId_);

		out << static_cast<int32_t>(partitioningInfo.condensedPartitionIdList_.size());

		for (size_t pos = 0; pos < partitioningInfo.condensedPartitionIdList_.size(); pos++) {
			out << static_cast<int64_t>(partitioningInfo.condensedPartitionIdList_[pos]);
		}

		if (partitioningInfo.partitioningColumnId_ != UNDEF_COLUMNID) {
			int32_t partitionColumnSize = 0;
			if (partitioningInfo.opt_ == NULL) {
				partitionColumnSize = 1;
				out << partitionColumnSize;
				out << partitioningInfo.partitioningColumnId_;
			}
			else {
				partitionColumnSize = static_cast<int32_t>(partitioningInfo.opt_->partitionColumnIdList_.size());
				out << partitionColumnSize;
				for (int32_t pos = 0; pos < partitionColumnSize; pos++) {
					out << static_cast<int32_t>(partitioningInfo.opt_->partitionColumnIdList_[pos]);
				}
			}
		}

		if (partitioningInfo.subPartitioningColumnId_ !=
				static_cast<ColumnId>(-1)) {
			int32_t partitionColumnSize = 0;
			if (partitioningInfo.opt_ == NULL) {
				partitionColumnSize = 1;
				out << partitionColumnSize;
				out << partitioningInfo.subPartitioningColumnId_;
			}
			else {
				partitionColumnSize = static_cast<int32_t>(partitioningInfo.opt_->subPartitionColumnIdList_.size());
				out << partitionColumnSize;
				for (int32_t pos = 0; pos < partitionColumnSize; pos++) {
					out << static_cast<int32_t>(partitioningInfo.opt_->subPartitionColumnIdList_[pos]);
				}
			}
		}

		util::Vector<int64_t> availableList(alloc);
		util::Vector<int64_t> availableCountList(alloc);
		util::Vector<int64_t> disAvailableList(alloc);
		util::Vector<int64_t> disAvailableCountList(alloc);
		if (partitioningInfo.partitionType_ != SyntaxTree::TABLE_PARTITION_TYPE_HASH) {
			partitioningInfo.getSubIdList(alloc, availableList, availableCountList, disAvailableList, disAvailableCountList
				, currentTime
			);
		}

		out << static_cast<int32_t>(availableList.size());

		for (size_t pos = 0; pos < availableList.size(); pos++) {
			out << static_cast<int64_t>(availableList[pos]);
			out << static_cast<int64_t>(availableCountList[pos]);
		}

		out << static_cast<int32_t>(disAvailableList.size());

		for (size_t pos = 0; pos < disAvailableList.size(); pos++) {
			out << static_cast<int64_t>(disAvailableList[pos]);
			out << static_cast<int64_t>(disAvailableCountList[pos]);
		}


		out << static_cast<int8_t>(partitioningInfo.currentStatus_);

		if (partitioningInfo.currentStatus_ == PARTITION_STATUS_CREATE_START ||
			partitioningInfo.currentStatus_ == PARTITION_STATUS_DROP_START) {
			out << static_cast<int64_t>(partitioningInfo.currentAffinityNumber_);
		}

		if (partitioningInfo.currentStatus_ == INDEX_STATUS_CREATE_START) {
			encodeStringData<EventByteOutStream>(out, partitioningInfo.currentIndexName_);
			out << static_cast<int32_t>(partitioningInfo.currentIndexType_);
			int32_t partitionColumnSize = 1;
			out << partitionColumnSize;
			out << static_cast<int32_t>(partitioningInfo.currentIndexColumnId_);
		}
		else if (partitioningInfo.currentStatus_ == INDEX_STATUS_DROP_START) {
			encodeStringData<EventByteOutStream>(out, partitioningInfo.currentIndexName_);
		}

		if (distributedPolicy >= 2) {
			out << currentTime;
			out << partitioningInfo.timeSeriesProperty_.elapsedTime_;
			out << partitioningInfo.timeSeriesProperty_.timeUnit_;
		}


		const size_t lastPos = out.base().position();
		size = static_cast<uint32_t>(lastPos - sizePos - sizeof(size));
		out.base().position(sizePos);
		out << size;
		out.base().position(lastPos);
	}
	catch (UserException& e) {
		GS_RETHROW_USER_ERROR(
				e, GS_EXCEPTION_MERGE_MESSAGE(
						e, "Failed to get partitioning metadata ("
						"containerName=" << containerName << ")"));
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(
				e, GS_EXCEPTION_MERGE_MESSAGE(
						e, "Failed to get partitioning metadata ("
						"containerName=" << containerName << ")"));
	}
}

/*!
	@brief Handler Operator
*/
void PutContainerHandler::operator()(EventContext & ec, Event & ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		TRANSACTION_EVENT_START(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0));

		util::XArray<uint8_t>& containerNameBinary = inMes.containerNameBinary_;
		util::String containerName(alloc); 
		ContainerType& containerType = inMes.containerType_;
		bool& modifiable = inMes.modifiable_;
		util::XArray<uint8_t>& containerInfo = inMes.containerInfo_;


		if (connOption.requestType_ == Message::REQUEST_NOSQL) {
			connOption.keepaliveTime_ = ec.getHandlerStartMonotonicTime();
		}
		request.fixed_.clientId_ = request.optional_.get<Options::CLIENT_ID>();

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;

		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		GS_TRACE_INFO(
			DATASTORE_BACKGROUND, GS_ERROR_DS_BACKGROUND_TASK_INVALID,
			"[PutContainerHandler PartitionId = " << request.fixed_.pId_ <<
			", stmtId = " << request.fixed_.cxtSrc_.stmtId_);


		request.fixed_.cxtSrc_.getMode_ = TransactionManager::PUT; 
		request.fixed_.cxtSrc_.txnMode_ = TransactionManager::NO_AUTO_COMMIT_BEGIN;

		util::LockGuard<util::Mutex> guard(
				partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn = transactionManager_->putNoExpire(
			alloc, request.fixed_.pId_, request.fixed_.clientId_,
			request.fixed_.cxtSrc_, now, emNow);
		if (txn.getPartitionId() != request.fixed_.pId_) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_TM_TRANSACTION_MODE_INVALID,
				"Invalid context exist request pId=" << request.fixed_.pId_ <<
				", txn pId=" << txn.getPartitionId());
		}
		txn.setAuditInfo(&ev,&ec,NULL);

		if (!checkPrivilege(
			ev.getType(),
			connOption.userType_, connOption.requestType_,
			request.optional_.get<Options::SYSTEM_MODE>(),
			request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>(),
			request.optional_.get<Options::CONTAINER_ATTRIBUTE>())) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_ACCESS_INVALID,
					"Can not create container attribute : " <<
					static_cast<int32_t>(
							request.optional_.get<Options::CONTAINER_ATTRIBUTE>()));
		}
		const FullContainerKey containerKey(
			alloc, getKeyConstraint(request.optional_),
			containerNameBinary.data(), containerNameBinary.size());
		bool isNewSql = isNewSQL(request);
		checkLoggedInDatabase(
			connOption.dbId_, connOption.dbName_.c_str(),
			containerKey.getComponents(alloc).dbId_,
			request.optional_.get<Options::DB_NAME>()
			, isNewSql
		);
		if (containerName.compare(GS_USERS) == 0 ||
			containerName.compare(GS_DATABASES) == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_ACCESS_INVALID,
				"Can not create container name : " <<
				request.optional_.get<Options::DB_NAME>());
		}




		bool keyStoreCaseSensitive = false;
		KeyDataStore* keyStore = getKeyDataStore(txn.getPartitionId());
		KeyDataStoreValue keyStoreValue = keyStore->get(txn, containerKey, keyStoreCaseSensitive);

		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_PUT_CONTAINER, &containerKey, containerType, &containerInfo,
			modifiable, inMes.featureVersion_, inMes.isCaseSensitive_);
		StackAllocAutoPtr<DSPutContainerOutputMes> ret(alloc, NULL);
		{
			ret.set(static_cast<DSPutContainerOutputMes*>(
				ds->exec(&txn, &keyStoreValue, &input)));
		}
		PutStatus putStatus = ret.get()->status_;
		KeyDataStoreValue outputStoreValue = ret.get()->storeValue_;

		DSPutContainerOutputMes* successMes = ret.get();

		request.fixed_.cxtSrc_.containerId_ = outputStoreValue.containerId_;
		txn.setContainerId(outputStoreValue.containerId_);
		txn.setSenderND(ev.getSenderND());

		util::XArray<const util::XArray<uint8_t>*> logRecordList(alloc);
		util::XArray<ClientId> closedResourceIds(alloc);
		if (putStatus != PutStatus::NOT_EXECUTED) {
			util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
				request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
				keyStoreValue.storeType_, ret.get()->dsLog_);
			if (logBinary != NULL) {
				logRecordList.push_back(logBinary);
			}
		}

		bool isAlterExecuted = false;
		if (putStatus == PutStatus::UPDATE) {
			DSInputMes input(alloc, DS_CONTINUE_ALTER_CONTAINER);
			StackAllocAutoPtr<DSPutContainerOutputMes> retAlter(alloc, NULL);
			{
				retAlter.set(static_cast<DSPutContainerOutputMes*>(
					ds->exec(&txn, &outputStoreValue, &input)));
			}
			isAlterExecuted = retAlter.get()->isExecuted_;

			util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
				request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
				keyStoreValue.storeType_, retAlter.get()->dsLog_);
			if (logBinary != NULL) {
				logRecordList.push_back(logBinary);
			}
		}

		int32_t replicationMode = transactionManager_->getReplicationMode();
		ReplicationContext::TaskStatus taskStatus = ReplicationContext::TASK_FINISHED;
		if (putStatus == PutStatus::CREATE || putStatus == PutStatus::CHANGE_PROPERTY ||
			isAlterExecuted) {
			DSInputMes input(alloc, DS_COMMIT);
			StackAllocAutoPtr<DSOutputMes> retCommit(alloc, NULL);
			{
				retCommit.set(static_cast<DSOutputMes*>(
					ds->exec(&txn, &outputStoreValue, &input)));
			}
			util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
				request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
				keyStoreValue.storeType_, retCommit.get()->dsLog_);
			if (logBinary != NULL) {
				logRecordList.push_back(logBinary);
			}

			transactionManager_->remove(request.fixed_.pId_, request.fixed_.clientId_);
			closedResourceIds.push_back(request.fixed_.clientId_);

			request.fixed_.cxtSrc_.getMode_ = TransactionManager::AUTO;
			request.fixed_.cxtSrc_.txnMode_ = TransactionManager::AUTO_COMMIT;
			TransactionContext& updataTxn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);

			OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
				getReplyOption(alloc, request, NULL, false),
				successMes->mes_);

			const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), updataTxn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				replicationMode, taskStatus, closedResourceIds.data(),
				closedResourceIds.size(), logRecordList.data(),
				logRecordList.size(), request.fixed_.cxtSrc_.stmtId_, 0,
				&outMes);

			replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
				request, outMes, ackWait);
		}
		else if (putStatus == PutStatus::NOT_EXECUTED) {
			DSInputMes input(alloc, DS_COMMIT);
			StackAllocAutoPtr<DSOutputMes> retCommit(alloc, NULL);
			{
				retCommit.set(static_cast<DSOutputMes*>(
					ds->exec(&txn, &outputStoreValue, &input)));
			}

			transactionManager_->remove(request.fixed_.pId_, request.fixed_.clientId_);
			closedResourceIds.push_back(request.fixed_.clientId_);

			request.fixed_.cxtSrc_.getMode_ = TransactionManager::AUTO;
			request.fixed_.cxtSrc_.txnMode_ = TransactionManager::AUTO_COMMIT;
			TransactionContext& updataTxn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);


			OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
				getReplyOption(alloc, request, NULL, false),
				successMes->mes_);
			const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), updataTxn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				replicationMode, taskStatus, closedResourceIds.data(),
				closedResourceIds.size(), logRecordList.data(),
				logRecordList.size(), request.fixed_.cxtSrc_.stmtId_, 0,
				&outMes);

			replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
				request, outMes, ackWait);
		}
		else {
			replicationMode = TransactionManager::REPLICATION_SEMISYNC;
			taskStatus = ReplicationContext::TASK_CONTINUE;

			transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);


			OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
				getReplyOption(alloc, request, NULL, false),
				successMes->mes_);
			const bool ackWait = executeReplication(
				request, ec, alloc,
				ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
				replicationMode, taskStatus, closedResourceIds.data(),
				closedResourceIds.size(), logRecordList.data(),
				logRecordList.size(), request.fixed_.cxtSrc_.stmtId_, 0,
				&outMes);

			continueEvent(
				ec, alloc, ev.getSenderND(), ev.getType(),
				request.fixed_.cxtSrc_.stmtId_, request, response, ackWait);
		}
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void PutContainerHandler::setContainerAttributeForCreate(OptionSet & optionSet) {
	if (optionSet.get<Options::CONTAINER_ATTRIBUTE>() >= CONTAINER_ATTR_ANY) {
		optionSet.set<Options::CONTAINER_ATTRIBUTE>(CONTAINER_ATTR_SINGLE);
	}
}

void PutLargeContainerHandler::setContainerAttributeForCreate(OptionSet & optionSet) {
	if (optionSet.get<Options::CONTAINER_ATTRIBUTE>() >= CONTAINER_ATTR_ANY) {
		optionSet.set<Options::CONTAINER_ATTRIBUTE>(CONTAINER_ATTR_SINGLE);
	}
}

/*!
	@brief Handler Operator
*/
void DropContainerHandler::operator()(EventContext & ec, Event & ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		TRANSACTION_EVENT_START(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0));

		util::XArray<uint8_t>& containerNameBinary = inMes.containerNameBinary_;;
		ContainerType& containerType = inMes.containerType_;

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);

		const FullContainerKey containerKey(
			alloc, getKeyConstraint(CONTAINER_ATTR_ANY, false),
			containerNameBinary.data(), containerNameBinary.size());
		txn.setAuditInfo(&ev,&ec,NULL);

		bool isNewSql = isNewSQL(request);
		checkLoggedInDatabase(
			connOption.dbId_, connOption.dbName_.c_str(),
			containerKey.getComponents(alloc).dbId_,
			request.optional_.get<Options::DB_NAME>()
			, isNewSql
		);

		KeyDataStore* keyStore = getKeyDataStore(txn.getPartitionId());
		KeyDataStoreValue keyStoreValue = keyStore->get(txn,
			containerKey, inMes.isCaseSensitive_);
		{
			if (keyStoreValue.containerId_ != UNDEF_CONTAINERID &&
				!checkPrivilege(ev.getType(),
					connOption.userType_, connOption.requestType_,
					request.optional_.get<Options::SYSTEM_MODE>(),
					request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>(),
					keyStoreValue.attribute_)) {
				GS_THROW_USER_ERROR(GS_ERROR_DS_CON_ACCESS_INVALID,
						"Can not drop container attribute : " <<
						static_cast<int32_t>(keyStoreValue.attribute_));
			}
			if (keyStoreValue.containerId_ == UNDEF_CONTAINERID) {
				OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
					getReplyOption(alloc, request, NULL, false), NULL);
				replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
					request, outMes, NO_REPLICATION);
				return;
			}
		}

		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_DROP_CONTAINER, &containerKey, containerType,
			inMes.isCaseSensitive_);

		StackAllocAutoPtr<DSOutputMes> ret(alloc, NULL);
		{
			ret.set(static_cast<DSOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		}

		if (isNewSQL(request) && isSkipReply(request)) {
			util::String keyStr(alloc);
			containerKey.toString(alloc, keyStr);
			FullContainerKeyComponents keyComponents = containerKey.getComponents(alloc);
			GS_TRACE_WARNING(DATA_EXPIRATION_DETAIL, GS_TRACE_DS_EXPIRED_CONTAINER_INFO,
				"Drop expired container, dbId=" << keyComponents.dbId_ << ", pId=" << request.fixed_.pId_
				<< ", name=" << keyStr.c_str() << ", from=" << ev.getSenderND());
		}
		util::XArray<const util::XArray<uint8_t>*> logRecordList(alloc);
		util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
			request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
			keyStoreValue.storeType_, ret.get()->dsLog_);
		if (logBinary != NULL) {
			logRecordList.push_back(logBinary);
		}

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false), ret.get()->mes_);
		const bool ackWait = executeReplication(
			request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size(),
			&outMes);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, ackWait);
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void GetContainerHandler::operator()(EventContext & ec, Event & ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		TRANSACTION_EVENT_START(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0));

		util::XArray<uint8_t>& containerNameBinary = inMes.containerNameBinary_;
		ContainerType& containerType = inMes.containerType_;

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), ANY_CONSISTENCY);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);

		const FullContainerKey containerKey(
			alloc, getKeyConstraint(CONTAINER_ATTR_ANY, false),
			containerNameBinary.data(), containerNameBinary.size());
		bool isNewSql = isNewSQL(request);
		txn.setAuditInfo(&ev,&ec,NULL);
		checkLoggedInDatabase(
			connOption.dbId_, connOption.dbName_.c_str(),
			containerKey.getComponents(alloc).dbId_,
			request.optional_.get<Options::DB_NAME>()
			, isNewSql
		);

		bool isCaseSensitive = false;
		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(txn,
			containerKey, isCaseSensitive);

		if (keyStoreValue.containerId_ != UNDEF_CONTAINERID &&
			!checkPrivilege(ev.getType(),
				connOption.userType_, connOption.requestType_,
				request.optional_.get<Options::SYSTEM_MODE>(),
				request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>(),
				keyStoreValue.attribute_)) {
			keyStoreValue = KeyDataStoreValue();
		}

		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_GET_CONTAINER, &containerKey, containerType);

		StackAllocAutoPtr<DSOutputMes> ret(alloc, NULL);
		{
			ret.set(static_cast<DSOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		}
		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false), ret.get()->mes_);
		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, NO_REPLICATION);
	}
catch (std::exception& e) {
	handleError(ec, alloc, ev, request, e);
}
}

void CreateIndexHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		TRANSACTION_EVENT_START(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0));

		if (connOption.requestType_ == Message::REQUEST_NOSQL) {
			connOption.keepaliveTime_ = ec.getHandlerStartMonotonicTime();
		}
		request.fixed_.clientId_ = request.optional_.get<Options::CLIENT_ID>();

		IndexInfo& indexInfo = inMes.indexInfo_;

		assignIndexExtension(indexInfo, request.optional_);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		EmptyAllowedKey::validate(
			KeyConstraint::getUserKeyConstraint(
				getDataStore(request.fixed_.pId_)->getConfig().getLimitContainerNameSize()),
			indexInfo.indexName_.c_str(),
			static_cast<uint32_t>(indexInfo.indexName_.size()),
			"indexName");

		request.fixed_.cxtSrc_.getMode_ = TransactionManager::PUT; 
		request.fixed_.cxtSrc_.txnMode_ = TransactionManager::NO_AUTO_COMMIT_BEGIN;

		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn = transactionManager_->putNoExpire(
			alloc, request.fixed_.pId_, request.fixed_.clientId_,
			request.fixed_.cxtSrc_, now, emNow);
		if (txn.getPartitionId() != request.fixed_.pId_) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_TM_TRANSACTION_MODE_INVALID,
				"Invalid context exist request pId=" << request.fixed_.pId_ <<
				", txn pId=" << txn.getPartitionId());
		}

		txn.setSenderND(ev.getSenderND());
		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_CREATE_INDEX, &(inMes.indexInfo_), inMes.isCaseSensitive_,
			inMes.mode_, request.fixed_.schemaVersionId_);
		txn.setAuditInfo(&ev,&ec,&indexInfo.indexName_);
		StackAllocAutoPtr<DSOutputMes> ret(alloc, NULL);
		{
			ret.set(static_cast<DSOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		}
		util::XArray<const util::XArray<uint8_t>*> logRecordList(alloc);
		util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
			request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
			keyStoreValue.storeType_, ret.get()->dsLog_);
		if (logBinary != NULL) {
			logRecordList.push_back(logBinary);
		}

		util::XArray<ClientId> closedResourceIds(alloc);
		int32_t replicationMode = transactionManager_->getReplicationMode();
		ReplicationContext::TaskStatus taskStatus = ReplicationContext::TASK_FINISHED;
		if (ret.get()->isExecuted_) {
			DSInputMes inputCommit(alloc, DS_COMMIT);
			DSOutputMes* retCommit = NULL;
			{
				retCommit = static_cast<DSOutputMes*>(
					ds->exec(&txn, &keyStoreValue, &inputCommit));
			}
			util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
				request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
				keyStoreValue.storeType_, retCommit->dsLog_);
			if (logBinary != NULL) {
				logRecordList.push_back(logBinary);
			}

			transactionManager_->remove(request.fixed_.pId_, request.fixed_.clientId_);
			closedResourceIds.push_back(request.fixed_.clientId_);

			request.fixed_.cxtSrc_.getMode_ = TransactionManager::AUTO;
			request.fixed_.cxtSrc_.txnMode_ = TransactionManager::AUTO_COMMIT;
			txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		}
		else {
			replicationMode = TransactionManager::REPLICATION_SEMISYNC;
			taskStatus = ReplicationContext::TASK_CONTINUE;

			transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);
		}
		GS_TRACE_INFO(
			DATASTORE_BACKGROUND, GS_ERROR_DS_BACKGROUND_TASK_INVALID,
			"[CreateIndexHandler PartitionId = " << txn.getPartitionId() <<
			", tId = " << txn.getId() <<
			", stmtId = " << txn.getLastStatementId() <<
			", orgStmtId = " << request.fixed_.cxtSrc_.stmtId_ <<
			", createIndex isFinished = " <<
			static_cast<int32_t>(ret.get()->isExecuted_));

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false), ret.get()->mes_);
		const bool ackWait = executeReplication(
			request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
			replicationMode, taskStatus, closedResourceIds.data(),
			closedResourceIds.size(), logRecordList.data(),
			logRecordList.size(), request.fixed_.cxtSrc_.stmtId_, 0,
			&outMes);

		if (ret.get()->isExecuted_) {
			replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
				request, outMes, ackWait);
		}
		else {
			continueEvent(
				ec, alloc, ev.getSenderND(), ev.getType(),
				request.fixed_.cxtSrc_.stmtId_, request, response, ackWait);
		}
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Continue current event
*/
void StatementHandler::continueEvent(
		EventContext& ec,
		util::StackAllocator& alloc, const NodeDescriptor& ND, EventType stmtType,
		StatementId originalStmtId, const Request& request,
		const Response& response, bool ackWait) {
	UNUSED_VARIABLE(response);


	util::StackAllocator::Scope scope(alloc);

	if (!ackWait) {
		EventType continueStmtType = stmtType;
		switch (stmtType) {
		case PUT_CONTAINER:
		case CONTINUE_ALTER_CONTAINER:
			continueStmtType = CONTINUE_ALTER_CONTAINER;
			break;
		case CREATE_INDEX:
		case CONTINUE_CREATE_INDEX:
			continueStmtType = CONTINUE_CREATE_INDEX;
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN,
				"(stmtType=" << stmtType << ")");
		}

		Event continueEvent(ec, continueStmtType, request.fixed_.pId_);
		continueEvent.setSenderND(ND);
		EventByteOutStream out = continueEvent.getOutStream();

		out << (request.fixed_.cxtSrc_.stmtId_ + 1); 
		out << request.fixed_.cxtSrc_.containerId_;
		out << request.fixed_.clientId_.sessionId_;
		encodeUUID(out, request.fixed_.clientId_.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
		out << originalStmtId;

		if (continueStmtType == CONTINUE_CREATE_INDEX || continueStmtType == CONTINUE_ALTER_CONTAINER) {
			OptionSet optionSet(alloc);
			setReplyOptionForContinue(optionSet, request);
			optionSet.encode(out);
		}

		ec.getEngine().add(continueEvent);
	}
}

/*!
	@brief Replies success message
*/
void StatementHandler::continueEvent(
		EventContext& ec,
		util::StackAllocator& alloc, StatementExecStatus status,
		const ReplicationContext& replContext) {
	UNUSED_VARIABLE(status);

#define TXN_TRACE_REPLY_SUCCESS_ERROR(replContext)             \
	"(nd=" << replContext.getConnectionND()                    \
		   << ", pId=" << replContext.getPartitionId()         \
		   << ", eventType=" << EventTypeUtility::getEventTypeName(replContext.getStatementType()) \
		   << ", stmtId=" << replContext.getStatementId()      \
		   << ", clientId=" << replContext.getClientId()       \
		   << ", containerId=" << replContext.getContainerId() << ")"

	util::StackAllocator::Scope scope(alloc);
	if (replContext.getConnectionND().isEmpty()) {
		return;
	}
	try {
		EventType continueStmtType = replContext.getStatementType();
		switch (replContext.getStatementType()) {
		case PUT_CONTAINER:
		case CONTINUE_ALTER_CONTAINER:
			continueStmtType = CONTINUE_ALTER_CONTAINER;
			break;
		case CREATE_INDEX:
		case CONTINUE_CREATE_INDEX:
			continueStmtType = CONTINUE_CREATE_INDEX;
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN,
				"(stmtType=" << replContext.getStatementType() << ")");
		}

		Event continueEvent(ec, continueStmtType,
			replContext.getPartitionId());
		continueEvent.setSenderND(replContext.getConnectionND());
		EventByteOutStream out = continueEvent.getOutStream();

		out << (replContext.getStatementId() + 1);
		out << replContext.getContainerId();
		out << replContext.getClientId().sessionId_;
		encodeUUID(out, replContext.getClientId().uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
		out << replContext.getOriginalStatementId();

		if (continueStmtType == CONTINUE_CREATE_INDEX || continueStmtType == CONTINUE_ALTER_CONTAINER) {
			OptionSet optionSet(alloc);
			setReplyOptionForContinue(optionSet, replContext);
			optionSet.encode(out);
		}
		ec.getEngine().add(continueEvent);
	}
	catch (EncodeDecodeException& e) {
		GS_RETHROW_USER_ERROR(e, TXN_TRACE_REPLY_SUCCESS_ERROR(replContext));
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, TXN_TRACE_REPLY_SUCCESS_ERROR(replContext));
	}

#undef TXN_TRACE_REPLY_SUCCESS_ERROR
}

void DropIndexHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		TRANSACTION_EVENT_START(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0));

		IndexInfo& indexInfo = inMes.indexInfo_;

		assignIndexExtension(indexInfo, request.optional_);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);

		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_DROP_INDEX, &(inMes.indexInfo_), inMes.isCaseSensitive_,
			inMes.mode_, request.fixed_.schemaVersionId_);
		txn.setAuditInfo(&ev,&ec,&indexInfo.indexName_);
		StackAllocAutoPtr<DSOutputMes> ret(alloc, NULL);
		{
			ret.set(static_cast<DSOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		}
		util::XArray<const util::XArray<uint8_t>*> logRecordList(alloc);
		util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
			request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
			keyStoreValue.storeType_, ret.get()->dsLog_);
		if (logBinary != NULL) {
			logRecordList.push_back(logBinary);
		}

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false), ret.get()->mes_);
		const bool ackWait = executeReplication(
			request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size(),
			&outMes);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, ackWait);
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void CreateDropTriggerHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		TRANSACTION_EVENT_START(ec, ev, transactionManager_, connOption, false);

		GS_THROW_USER_ERROR(GS_ERROR_CM_NOT_SUPPORTED,
			"Trigger not support");
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void FlushLogHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {
		EVENT_START(ec, ev, transactionManager_, UNDEF_DBID, false);

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		if ( AUDIT_TRACE_CHECK ) {
			AUDIT_TRACE_INFO_OPERATOR("")
		}

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		PartitionId pId = request.fixed_.pId_;
		Partition& partition = partitionList_->partition(pId);
		if (partition.isActive()) {
			const bool byCP = false;
			{
				util::LockGuard<util::Mutex> guard(partition.mutex());
				partition.logManager().flushXLog(LOG_WRITE_WAL_BUFFER, byCP);
			}
			partition.logManager().flushXLog(LOG_FLUSH_FILE, byCP);
		}

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false));
		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, NO_REPLICATION);
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void CreateTransactionContextHandler::operator()(EventContext& ec, Event& ev)

{
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		EVENT_START(ec, ev, transactionManager_, connOption.dbId_, (ev.getSenderND().getId() < 0));

		if ( AUDIT_TRACE_CHECK ) {
			AUDIT_TRACE_INFO_OPERATOR("")
		}
		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		request.fixed_.cxtSrc_.getMode_ = TransactionManager::CREATE;
		request.fixed_.cxtSrc_.txnMode_ = TransactionManager::AUTO_COMMIT;
		transactionManager_->put(
			alloc, request.fixed_.pId_, request.fixed_.clientId_,
			request.fixed_.cxtSrc_, now, emNow);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false));
		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, NO_REPLICATION);
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void CloseTransactionContextHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		EVENT_START(ec, ev, transactionManager_, connOption.dbId_, (ev.getSenderND().getId() < 0));

		if ( AUDIT_TRACE_CHECK ) {
			AUDIT_TRACE_INFO_OPERATOR("")
		}

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		util::XArray<ClientId> closedResourceIds(alloc);
		util::XArray<const util::XArray<uint8_t>*> logRecordList(alloc);

		try {
			util::LockGuard<util::Mutex> guard(
				partitionList_->partition(request.fixed_.pId_).mutex());

			TransactionContext& txn = transactionManager_->get(
				alloc, request.fixed_.pId_, request.fixed_.clientId_);
			if (txn.isActive()) {
				KeyDataStoreValue keyStoreValue = KeyDataStoreValue();
				try {
					keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
				}
				catch (UserException& e) {
					UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
						"Container not found (pId="
						<< txn.getPartitionId()
						<< ", clientId=" << txn.getClientId()
						<< ", containerId=" << txn.getContainerId()
						<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
				}

				if (keyStoreValue.containerId_ != UNDEF_CONTAINERID) {
					DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
					const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
					DSInputMes input(alloc, DS_ABORT, false);
					StackAllocAutoPtr<DSOutputMes> ret(alloc, NULL);
					{
						ret.set(static_cast<DSOutputMes*>(
							ds->exec(&txn, &keyStoreValue, &input)));
					}
					util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
						request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
						keyStoreValue.storeType_, ret.get()->dsLog_);
					if (logBinary != NULL) {
						logRecordList.push_back(logBinary);
					}
				}

				ec.setPendingPartitionChanged(txn.getPartitionId());
			}


			transactionManager_->remove(
				request.fixed_.pId_, request.fixed_.clientId_);
			closedResourceIds.push_back(request.fixed_.clientId_);
		}
		catch (ContextNotFoundException&) {
		}

		request.fixed_.cxtSrc_.getMode_ = TransactionManager::AUTO;
		request.fixed_.cxtSrc_.txnMode_ = TransactionManager::AUTO_COMMIT;
		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false));
		const bool ackWait = executeReplication(
			request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(),
			closedResourceIds.data(), closedResourceIds.size(),
			logRecordList.data(), logRecordList.size(),
			&outMes);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, ackWait);
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void CommitAbortTransactionHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		EVENT_START(ec, ev, transactionManager_, connOption.dbId_, (ev.getSenderND().getId() < 0));

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		if (isDdlTransaction(request.optional_)) {
			request.fixed_.cxtSrc_.getMode_ = TransactionManager::PUT;
		}
		else {
			request.fixed_.cxtSrc_.getMode_ = TransactionManager::GET;
		}
		request.fixed_.cxtSrc_.txnMode_ = TransactionManager::NO_AUTO_COMMIT_CONTINUE;

		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		txn.setAuditInfo(&ev,&ec,NULL);
		
		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);

		util::XArray<const util::XArray<uint8_t>*> logRecordList(alloc);
		StackAllocAutoPtr<DSOutputMes> ret(alloc, NULL);
		switch (ev.getType()) {
		case COMMIT_TRANSACTION: {
			DSInputMes input(alloc, DS_COMMIT, false);
			ret.set(static_cast<DSOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		} break;
		case ABORT_TRANSACTION: {
			DSInputMes input(alloc, DS_ABORT, false);
			ret.set(static_cast<DSOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		} break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN, "");
		}
		util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
			request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
			keyStoreValue.storeType_, ret.get()->dsLog_);
		if (logBinary != NULL) {
			logRecordList.push_back(logBinary);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		util::XArray<ClientId> closedResourceIds(alloc);
		if (isDdlTransaction(request.optional_)) {
			transactionManager_->remove(request.fixed_.pId_, request.fixed_.clientId_);
			closedResourceIds.push_back(request.fixed_.clientId_);
			transactionService_->incrementAbortDDLCount(ev.getPartitionId());
		}

		ec.setPendingPartitionChanged(request.fixed_.pId_);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false), ret.get()->mes_);
		const bool ackWait = executeReplication(
			request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(),
			closedResourceIds.data(), closedResourceIds.size(),
			logRecordList.data(), logRecordList.size(),
			&outMes);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, ackWait);
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}


void PutRowHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		TRANSACTION_EVENT_START(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0));

		const uint64_t numRow = 1;
		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);


		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);

		txn.setAuditInfo(&ev,&ec,NULL);
		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_PUT_ROW, &(inMes.rowData_), inMes.putRowOption_,
			request.fixed_.schemaVersionId_);
		StackAllocAutoPtr<DSOutputMes> ret(alloc, NULL);
		{
			ret.set(static_cast<DSOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		}
		util::XArray<const util::XArray<uint8_t>*> logRecordList(alloc);
		util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
			request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
			keyStoreValue.storeType_, ret.get()->dsLog_);
		if (logBinary != NULL) {
			logRecordList.push_back(logBinary);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false), ret.get()->mes_);
		const bool ackWait = executeReplication(
			request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size(), &outMes);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, ackWait);

		transactionService_->incrementWriteOperationCount(ev.getPartitionId());
		transactionService_->addRowWriteCount(ev.getPartitionId(), numRow);
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}


void PutRowSetHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		TRANSACTION_EVENT_START(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0));

		RowData& multiRowData = inMes.rowData_;
		uint64_t numRow = inMes.numRow_;

		assert(numRow > 0);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		const bool STATEMENTID_NO_CHECK = true;
		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_, request.fixed_.clientId_,
			request.fixed_.cxtSrc_, now, emNow, STATEMENTID_NO_CHECK);
		request.fixed_.startStmtId_ = txn.getLastStatementId();
		transactionManager_->checkStatementContinuousInTransaction(txn,
			request.fixed_.cxtSrc_.stmtId_, request.fixed_.cxtSrc_.getMode_,
			request.fixed_.cxtSrc_.txnMode_);

		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);

		DSInputMes input(alloc, DS_GET_CONTAINER_OBJECT, ANY_CONTAINER);
		StackAllocAutoPtr<DSContainerOutputMes> ret(alloc, NULL);
		{
			ret.set(static_cast<DSContainerOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		}
		BaseContainer* container = ret.get()->container_;
		checkContainerExistence(container);
		if ( AUDIT_TRACE_CHECK ) {
			AUDIT_TRACE_INFO_OPERATOR(getAuditContainerName(alloc,
				 getKeyDataStore(txn.getPartitionId())->getKey(alloc, txn.getContainerId())))
		}

		const bool NO_VALIDATE_ROW_IMAGE = false;
		InputMessageRowStore inputMessageRowStore(
			static_cast<DataStoreV4*>(ds)->getConfig(), container->getColumnInfoList(),
			container->getColumnNum(), multiRowData.data(),
			static_cast<uint32_t>(multiRowData.size()), numRow,
			container->getRowFixedDataSize(), NO_VALIDATE_ROW_IMAGE);

		TxnLogManager txnLogMgr(transactionService_, ec, txn.getPartitionId());

		try {
			for (StatementId rowStmtId = request.fixed_.cxtSrc_.stmtId_;
				inputMessageRowStore.next(); rowStmtId++) {
				const util::StackAllocator::Scope scope(alloc);
				RowData rowData(alloc);
				try {
					transactionManager_->checkStatementAlreadyExecuted(
						txn, rowStmtId, request.fixed_.cxtSrc_.isUpdateStmt_);
				}
				catch (StatementAlreadyExecutedException& e) {
					UTIL_TRACE_EXCEPTION_INFO(TRANSACTION_SERVICE, e,
						"Row already put (pId=" << ev.getPartitionId() <<
						", eventType=" << EventTypeUtility::getEventTypeName(ev.getType()) <<
						", stmtId=" << rowStmtId <<
						", clientId=" << request.fixed_.clientId_ <<
						", containerId=" <<
						((request.fixed_.cxtSrc_.containerId_ ==
							UNDEF_CONTAINERID) ?
							0 : request.fixed_.cxtSrc_.containerId_) <<
						", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");

					continue;
				}
				
				rowData.clear();
				inputMessageRowStore.getCurrentRowData(rowData);

				bool isEmptyLog = true;
				DSInputMes input(alloc, DS_PUT_ROW, &(rowData), inMes.putRowOption_,
					request.fixed_.schemaVersionId_, isEmptyLog);
				StackAllocAutoPtr<DSOutputMes> ret(alloc, NULL);
				{
					ret.set(static_cast<DSOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
				}
				util::XArray<uint8_t>* logBinary = appendDataStoreLog(txnLogMgr.getLogAllocator(), txn,
					request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
					keyStoreValue.storeType_, ret.get()->dsLog_);
				txnLogMgr.addLog(logBinary);

				transactionManager_->update(txn, rowStmtId);
			}
		}
		catch (std::exception&) {
			executeReplication(request, ec, alloc, NodeDescriptor::EMPTY_ND,
				txn, ev.getType(), txn.getLastStatementId(),
				TransactionManager::REPLICATION_ASYNC, NULL, 0,
				txnLogMgr.getLogList().data(), txnLogMgr.getLogList().size(),
				NULL);

			throw;
		}

		bool existFlag = false; 
		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false), existFlag);
		const bool ackWait = executeReplication(
			request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			txnLogMgr.getLogList().data(), txnLogMgr.getLogList().size(), &outMes);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, ackWait);

		transactionService_->incrementWriteOperationCount(ev.getPartitionId());
		transactionService_->addRowWriteCount(ev.getPartitionId(), numRow);

	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void RemoveRowHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		TRANSACTION_EVENT_START(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0));

		const uint64_t numRow = 1;

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());


		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);

		txn.setAuditInfo(&ev,&ec,NULL);
		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_REMOVE_ROW, &(inMes.keyData_),
			request.fixed_.schemaVersionId_);
		StackAllocAutoPtr<DSOutputMes> ret(alloc, NULL);
		{
			ret.set(static_cast<DSOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		}
		util::XArray<const util::XArray<uint8_t>*> logRecordList(alloc);
		util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
			request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
			keyStoreValue.storeType_, ret.get()->dsLog_);
		if (logBinary != NULL) {
			logRecordList.push_back(logBinary);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false), ret.get()->mes_);
		const bool ackWait = executeReplication(
			request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size(), &outMes);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, ackWait);

		transactionService_->incrementWriteOperationCount(ev.getPartitionId());
		transactionService_->addRowWriteCount(ev.getPartitionId(), numRow);
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void UpdateRowByIdHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		TRANSACTION_EVENT_START(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0));

		const uint64_t numRow = 1;
		util::XArray<RowId> rowIds(alloc);
		rowIds.assign(numRow, UNDEF_ROWID);
		rowIds[0] = inMes.rowId_;

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());


		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);

		txn.setAuditInfo(&ev,&ec,NULL);
		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_UPDATE_ROW_BY_ID, inMes.rowId_, &(inMes.rowData_),
			request.fixed_.schemaVersionId_);
		StackAllocAutoPtr<DSOutputMes> ret(alloc, NULL);
		{
			ret.set(static_cast<DSOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		}
		util::XArray<const util::XArray<uint8_t>*> logRecordList(alloc);
		util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
			request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
			keyStoreValue.storeType_, ret.get()->dsLog_);
		if (logBinary != NULL) {
			logRecordList.push_back(logBinary);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false), ret.get()->mes_);
		const bool ackWait = executeReplication(
			request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size(), &outMes);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, ackWait);

		transactionService_->incrementWriteOperationCount(ev.getPartitionId());
		transactionService_->addRowWriteCount(ev.getPartitionId(), numRow);
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void RemoveRowByIdHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		TRANSACTION_EVENT_START(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0));

		const uint64_t numRow = 1;
		util::XArray<RowId> rowIds(alloc);
		rowIds.assign(numRow, UNDEF_ROWID);
		rowIds[0] = inMes.rowId_;

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());


		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);

		txn.setAuditInfo(&ev,&ec,NULL);
		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_REMOVE_ROW_BY_ID, inMes.rowId_, request.fixed_.schemaVersionId_);
		StackAllocAutoPtr<DSOutputMes> ret(alloc, NULL);
		{
			ret.set(static_cast<DSOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		}
		util::XArray<const util::XArray<uint8_t>*> logRecordList(alloc);
		util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
			request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
			keyStoreValue.storeType_, ret.get()->dsLog_);
		if (logBinary != NULL) {
			logRecordList.push_back(logBinary);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false), ret.get()->mes_);
		const bool ackWait = executeReplication(
			request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size(), &outMes);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, ackWait);

		transactionService_->incrementWriteOperationCount(ev.getPartitionId());
		transactionService_->addRowWriteCount(ev.getPartitionId(), numRow);
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void RemoveRowSetByIdHandler::operator()(EventContext& ec, Event& ev) {
	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		TRANSACTION_EVENT_START(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0));

		uint64_t numRow = static_cast<uint64_t>(inMes.rowIds_.size());
		util::XArray<RowId>& rowIds = inMes.rowIds_;

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		const bool STATEMENTID_NO_CHECK = true;
		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_, request.fixed_.clientId_,
			request.fixed_.cxtSrc_, now, emNow, STATEMENTID_NO_CHECK);
		request.fixed_.startStmtId_ = txn.getLastStatementId();
		transactionManager_->checkStatementContinuousInTransaction(
			txn,
			request.fixed_.cxtSrc_.stmtId_, request.fixed_.cxtSrc_.getMode_,
			request.fixed_.cxtSrc_.txnMode_);

		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);

		util::XArray<const util::XArray<uint8_t>*> logRecordList(alloc);

		try {
			for (StatementId rowStmtId = request.fixed_.cxtSrc_.stmtId_, rowCounter = 0;
				rowCounter < numRow; rowCounter++, rowStmtId++) {
				try {
					transactionManager_->checkStatementAlreadyExecuted(
						txn, rowStmtId, request.fixed_.cxtSrc_.isUpdateStmt_);
				}
				catch (StatementAlreadyExecutedException& e) {
					UTIL_TRACE_EXCEPTION_INFO(TRANSACTION_SERVICE, e,
						"Row already put (pId=" << ev.getPartitionId() <<
						", eventType=" << EventTypeUtility::getEventTypeName(ev.getType()) <<
						", stmtId=" << rowStmtId <<
						", clientId=" << request.fixed_.clientId_ <<
						", containerId=" <<
						((request.fixed_.cxtSrc_.containerId_ ==
							UNDEF_CONTAINERID) ?
							0 : request.fixed_.cxtSrc_.containerId_) <<
						", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");

					continue;
				}

				DSInputMes input(alloc, DS_REMOVE_ROW_BY_ID, rowIds[rowCounter], request.fixed_.schemaVersionId_);
				StackAllocAutoPtr<DSOutputMes> ret(alloc, NULL);
				{
					ret.set(static_cast<DSOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
				}
				util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
					request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
					keyStoreValue.storeType_, ret.get()->dsLog_);
				if (logBinary != NULL) {
					logRecordList.push_back(logBinary);
				}

				transactionManager_->update(txn, rowStmtId);
			}
		}
		catch (std::exception&) {
			executeReplication(request, ec, alloc, NodeDescriptor::EMPTY_ND,
				txn, ev.getType(), txn.getLastStatementId(),
				TransactionManager::REPLICATION_ASYNC, NULL, 0,
				logRecordList.data(), logRecordList.size(),
				NULL);
			throw;
		}
		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false));
		const bool ackWait = executeReplication(
			request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size(), &outMes);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, ackWait);
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void UpdateRowSetByIdHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		TRANSACTION_EVENT_START(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0));

		uint64_t numRow = static_cast<uint64_t>(inMes.rowIds_.size());
		util::XArray<RowId>& targetRowIds = inMes.rowIds_;
		RowData& multiRowData = inMes.multiRowData_;

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		const bool STATEMENTID_NO_CHECK = true;
		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_, request.fixed_.clientId_,
			request.fixed_.cxtSrc_, now, emNow, STATEMENTID_NO_CHECK);
		request.fixed_.startStmtId_ = txn.getLastStatementId();
		transactionManager_->checkStatementContinuousInTransaction(txn,
			request.fixed_.cxtSrc_.stmtId_, request.fixed_.cxtSrc_.getMode_,
			request.fixed_.cxtSrc_.txnMode_);

		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);

		DSInputMes input(alloc, DS_GET_CONTAINER_OBJECT, ANY_CONTAINER);
		StackAllocAutoPtr<DSContainerOutputMes> ret(alloc, NULL);
		{
			ret.set(static_cast<DSContainerOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		}
		BaseContainer* container = ret.get()->container_;
		checkContainerExistence(container);

		const bool NO_VALIDATE_ROW_IMAGE = false;
		InputMessageRowStore inputMessageRowStore(
			getDataStore(txn.getPartitionId())->getConfig(), container->getColumnInfoList(),
			container->getColumnNum(), multiRowData.data(),
			static_cast<uint32_t>(multiRowData.size()), numRow,
			container->getRowFixedDataSize(), NO_VALIDATE_ROW_IMAGE);
		response.existFlag_ = false;

		util::XArray<RowId> rowIds(alloc);
		RowData rowData(alloc);
		StatementId rowPos = 0;
		util::XArray<const util::XArray<uint8_t>*> logRecordList(alloc);

		try {
			for (StatementId rowStmtId = request.fixed_.cxtSrc_.stmtId_;
				inputMessageRowStore.next(); rowStmtId++, rowPos++) {
				try {
					transactionManager_->checkStatementAlreadyExecuted(
						txn, rowStmtId, request.fixed_.cxtSrc_.isUpdateStmt_);
				}
				catch (StatementAlreadyExecutedException& e) {
					UTIL_TRACE_EXCEPTION_INFO(TRANSACTION_SERVICE, e,
						"Row already put (pId=" << ev.getPartitionId() <<
						", eventType=" << EventTypeUtility::getEventTypeName(ev.getType()) <<
						", stmtId=" << rowStmtId <<
						", clientId=" << request.fixed_.clientId_ <<
						", containerId=" <<
						((request.fixed_.cxtSrc_.containerId_ ==
							UNDEF_CONTAINERID) ?
							0 : request.fixed_.cxtSrc_.containerId_) <<
						", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");

					continue;
				}

				rowIds.clear();
				rowIds.push_back(targetRowIds[rowPos]);
				rowData.clear();
				inputMessageRowStore.getCurrentRowData(rowData);

				bool isEmptyLog = true;
				DSInputMes input(alloc, DS_UPDATE_ROW_BY_ID, rowIds[0],
					&(rowData), request.fixed_.schemaVersionId_, isEmptyLog);
				StackAllocAutoPtr<DSOutputMes> ret(alloc, NULL);
				{
					ret.set(static_cast<DSOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
				}
				util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
					request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
					keyStoreValue.storeType_, ret.get()->dsLog_);
				if (logBinary != NULL) {
					logRecordList.push_back(logBinary);
				}

				transactionManager_->update(txn, rowStmtId);
			}
		}
		catch (std::exception&) {
			executeReplication(request, ec, alloc, NodeDescriptor::EMPTY_ND,
				txn, ev.getType(), txn.getLastStatementId(),
				TransactionManager::REPLICATION_ASYNC, NULL, 0,
				logRecordList.data(), logRecordList.size(),
				NULL);

			throw;
		}

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false));
		const bool ackWait = executeReplication(
			request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size(), &outMes);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, ackWait);

		transactionService_->incrementWriteOperationCount(ev.getPartitionId());
		transactionService_->addRowWriteCount(ev.getPartitionId(), numRow);
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void GetRowHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);

		const bool forUpdate = request.optional_.get<Options::FOR_UPDATE>();
		TRANSACTION_EVENT_START_FOR_UPDATE(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0), forUpdate);


		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
			forUpdate ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkForUpdate(forUpdate);
		checkConsistency(ev.getSenderND(), forUpdate);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);

		txn.setAuditInfo(&ev,&ec,NULL);
		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_GET_ROW, &(inMes.keyData_), forUpdate,
			request.fixed_.schemaVersionId_, emNow);

		StackAllocAutoPtr<DSOutputMes> ret(alloc, NULL);
		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			ret.set(static_cast<DSOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		}

		util::XArray<const util::XArray<uint8_t>*> logRecordList(alloc);
		util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
			request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
			keyStoreValue.storeType_, ret.get()->dsLog_);
		if (logBinary != NULL) {
			logRecordList.push_back(logBinary);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false), ret.get()->mes_);
		const bool ackWait = executeReplication(
			request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size(), &outMes);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, ackWait);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
			ev.getPartitionId(), ret.get()->num_);
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void GetRowSetHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);

		FetchOption& fetchOption = inMes.fetchOption_;
		bool isPartial = inMes.isPartial_;
		RowId position = inMes.rowId_;

		const bool forUpdate = request.optional_.get<Options::FOR_UPDATE>();
		TRANSACTION_EVENT_START(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0));

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
			forUpdate ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkForUpdate(forUpdate);
		checkConsistency(ev.getSenderND(), forUpdate);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);
		checkQueryOption(request.optional_, fetchOption, isPartial, false);

		if (forUpdate) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_NOT_EXECUTABLE,
				"row lock (forUpdate) is not applicable");
		}

		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		txn.setAuditInfo(&ev,&ec,NULL);

		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_GET_ROW_SET,
			position,
			inMes.fetchOption_.limit_, inMes.fetchOption_.size_,
			request.fixed_.schemaVersionId_, emNow);

		StackAllocAutoPtr<DSRowSetOutputMes> ret(alloc, NULL);
		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			ret.set(static_cast<DSRowSetOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		}

		util::XArray<const util::XArray<uint8_t>*> logRecordList(alloc);
		util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
			request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
			keyStoreValue.storeType_, ret.get()->dsLog_);
		if (logBinary != NULL) {
			logRecordList.push_back(logBinary);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false),
			ret.get()->rs_, ev, ret.get()->last_);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, NO_REPLICATION);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
			ev.getPartitionId(), ret.get()->rs_->getFetchNum());
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void QueryTqlHandler::operator()(EventContext& ec, Event& ev)
{
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);

		util::XArray<uint8_t> suspendedData(alloc);
		size_t extraDataSize;
		decodeSuspendedData(
				ev, request.optional_, in, suspendedData, extraDataSize);

		FetchOption& fetchOption = inMes.fetchOption_;
		bool isPartial = inMes.isPartial_;
		PartialQueryOption& partialQueryOption = inMes.partialQueryOption_;
		util::String& query = inMes.query_;

		FullContainerKey* containerKey = NULL;
		const QueryContainerKey* containerKeyOption =
			request.optional_.get<Options::QUERY_CONTAINER_KEY>();
		const QueryContainerKey* distInfo =
			request.optional_.get<Options::DIST_QUERY>();
		if (distInfo != NULL && distInfo->enabled_) {
			containerKeyOption = distInfo;
		}
		if (containerKeyOption != NULL && containerKeyOption->enabled_) {
			const util::XArray<uint8_t>& keyBinary =
				containerKeyOption->containerKeyBinary_;
			containerKey = ALLOC_NEW(alloc) FullContainerKey(
				alloc, getKeyConstraint(request.optional_),
				keyBinary.data(), keyBinary.size());
		}

		const ContainerId metaContainerId =
			request.optional_.get<Options::META_CONTAINER_ID>();
		bool nodeDistribution = false;
		if (metaContainerId != UNDEF_CONTAINERID) {
			const bool forCore = true;
			const MetaType::InfoTable& infoTable =
				MetaType::InfoTable::getInstance();
			const MetaContainerInfo* info =
				infoTable.findInfo(metaContainerId, forCore);
			if (info != NULL) {
				nodeDistribution = info->nodeDistribution_;
			}
		}

		const bool forUpdate = request.optional_.get<Options::FOR_UPDATE>();
		TRANSACTION_EVENT_START_FOR_UPDATE(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0), forUpdate);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = nodeDistribution ?
			PROLE_ANY :
			(forUpdate ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP));
		const PartitionStatus partitionStatus = nodeDistribution ?
			PSTATE_ANY : PSTATE_ON;

		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkForUpdate(forUpdate);
		checkConsistency(ev.getSenderND(), forUpdate);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		checkQueryOption(request.optional_, fetchOption, isPartial, true);


		util::LockGuard<util::Mutex> guard(
				partitionList_->partition(request.fixed_.pId_).mutex());


		if (metaContainerId != UNDEF_CONTAINERID) {
			TransactionContext& txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);

			util::AllocUniquePtr<DataStoreBase::Scope> dsScope(NULL, alloc);
			if (!nodeDistribution) {
				dsScope.reset(ALLOC_NEW(alloc) DataStoreBase::Scope(
					&txn, getDataStore(txn.getPartitionId()), clusterService_));
			}
			MetaStore metaStore(*getDataStore(txn.getPartitionId()));

			MetaType::NamingType columnNamingType;
			columnNamingType = static_cast<MetaType::NamingType>(
				request.optional_.get<Options::META_NAMING_TYPE>());

			MetaContainer* metaContainer = metaStore.getContainer(
				txn, connOption.dbId_, metaContainerId,
				MetaType::NAMING_NEUTRAL, columnNamingType);

			response.rs_ = getDataStore(txn.getPartitionId())->getResultSetManager()->create(
				txn, txn.getContainerId(),
				metaContainer->getVersionId(), emNow, NULL);
			const ResultSetGuard rsGuard(txn, *getDataStore(txn.getPartitionId()), *response.rs_);

			MetaProcessorSource source(
				connOption.dbId_, connOption.dbName_.c_str());
			source.dataStore_ = getDataStore(txn.getPartitionId());
			source.eventContext_ = &ec;
			source.transactionManager_ = transactionManager_;
			source.transactionService_ = transactionService_;
			source.partitionTable_ = partitionTable_;
			source.sqlService_ = sqlService_;
			source.sqlExecutionManager_ = sqlService_->getExecutionManager();

			if (QueryProcessor::executeMetaTQL(
					txn, *metaContainer, source, fetchOption.limit_,
					TQLInfo(
							request.optional_.get<Options::DB_NAME>(),
							containerKey, query.c_str()),
					*response.rs_, suspendedData)) {
				suspendRequest(ec, ev, suspendedData, extraDataSize);
				return;
			}

			replySuccess(
				ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
			return;
		}

		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		ResultSetOption queryOption;
		createResultSetOption(
			request.optional_, NULL, isPartial, partialQueryOption,
			queryOption);
		const char* dbName = request.optional_.get<Options::DB_NAME>();
		txn.setAuditInfo(&ev,&ec,&query);

		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_QUERY_TQL,
			dbName, containerKey, query, fetchOption.limit_, fetchOption.size_, &queryOption,
			forUpdate,
			request.fixed_.schemaVersionId_, emNow);
		StackAllocAutoPtr<DSTQLOutputMes> ret(alloc, NULL);
		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());
			ret.set(static_cast<DSTQLOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		}

		util::XArray<const util::XArray<uint8_t>*> logRecordList(alloc);
		util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
			request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
			keyStoreValue.storeType_, ret.get()->dsLog_);
		if (logBinary != NULL) {
			logRecordList.push_back(logBinary);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false),
			alloc, ret.get()->rs_, ev, isNewSQL(request));
		const bool ackWait = executeReplication(
			request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size(), &outMes);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, ackWait);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
			ev.getPartitionId(), ret.get()->rs_->getFetchNum());
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void QueryTqlHandler::suspendRequest(
		EventContext& ec, const Event &baseEv,
		const util::XArray<uint8_t> &suspendedData, size_t extraDataSize) {
	Event ev(baseEv);
	EventByteOutStream out = ev.getOutStream();

	assert(out.base().position() >= extraDataSize);
	out.base().position(out.base().position() - extraDataSize);

	if (suspendedData.size() > std::numeric_limits<uint32_t>::max()) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED, "");
	}

	const uint32_t size = static_cast<uint32_t>(suspendedData.size());
	try {
		out << size;
		out.writeAll(suspendedData.data(), size);
	}
	catch (...) {
		std::exception e;
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}

	updateRequestOption<Options::HANDLING_SUSPENDED>(
			ec.getAllocator(), ev, true);
	ev.incrementQueueingCount();
	ec.getEngine().add(ev);
}

void QueryTqlHandler::decodeSuspendedData(
		const Event &ev, const OptionSet &optionSet, EventByteInStream &in,
		util::XArray<uint8_t> &suspendedData, size_t &extraDataSize) {
	suspendedData.clear();
	extraDataSize = in.base().remaining();

	const bool suspended = optionSet.get<Options::HANDLING_SUSPENDED>();
	if (ev.getQueueingCount() <= 1 || !suspended) {
		return;
	}

	try {
		uint32_t size;
		in >> size;

		if (size > in.base().remaining()) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
		}

		suspendedData.resize(size);
		in.readAll(suspendedData.data(), suspendedData.size());
	}
	catch (...) {
		std::exception e;
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}


/*!
	@brief Handler Operator
*/
void AppendRowHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		TRANSACTION_EVENT_START(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0));

		const uint64_t numRow = 1;

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		util::LockGuard<util::Mutex> guard(
				partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		txn.setAuditInfo(&ev,&ec,NULL);
		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_APPEND_ROW, &(inMes.rowData_),
			request.fixed_.schemaVersionId_);
		StackAllocAutoPtr<DSOutputMes> ret(alloc, NULL);
		{
			ret.set(static_cast<DSOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		}
		util::XArray<const util::XArray<uint8_t>*> logRecordList(alloc);
		util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
			request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
			keyStoreValue.storeType_, ret.get()->dsLog_);
		if (logBinary != NULL) {
			logRecordList.push_back(logBinary);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false), ret.get()->mes_);
		const bool ackWait = executeReplication(
			request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size(), &outMes);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, ackWait);

		transactionService_->incrementWriteOperationCount(ev.getPartitionId());
		transactionService_->addRowWriteCount(ev.getPartitionId(), numRow);
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void QueryGeometryRelatedHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);

		FetchOption& fetchOption = inMes.fetchOption_;
		bool isPartial = inMes.isPartial_;

		const bool forUpdate = request.optional_.get<Options::FOR_UPDATE>();
		TRANSACTION_EVENT_START_FOR_UPDATE(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0), forUpdate);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
			forUpdate ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkForUpdate(forUpdate);
		checkConsistency(ev.getSenderND(), forUpdate);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);
		checkQueryOption(request.optional_, fetchOption, isPartial, false);

		util::LockGuard<util::Mutex> guard(
				partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);

		txn.setAuditInfo(&ev,&ec,NULL);

		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_QUERY_GEOMETRY_RELATED,
			inMes.query_,
			inMes.fetchOption_.limit_, inMes.fetchOption_.size_,
			forUpdate, request.fixed_.schemaVersionId_, emNow);

		StackAllocAutoPtr<DSTQLOutputMes> ret(alloc, NULL);
		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			ret.set(static_cast<DSTQLOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		}

		util::XArray<const util::XArray<uint8_t>*> logRecordList(alloc);
		util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
			request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
			keyStoreValue.storeType_, ret.get()->dsLog_);
		if (logBinary != NULL) {
			logRecordList.push_back(logBinary);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false),
			alloc, ret.get()->rs_, ev, isNewSQL(request));

		const bool ackWait = executeReplication(
			request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size(), &outMes);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, ackWait);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
			ev.getPartitionId(), ret.get()->rs_->getFetchNum());
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void QueryGeometryWithExclusionHandler::operator()(
	EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);

		FetchOption& fetchOption = inMes.fetchOption_;
		bool isPartial = inMes.isPartial_;

		const bool forUpdate = request.optional_.get<Options::FOR_UPDATE>();
		TRANSACTION_EVENT_START_FOR_UPDATE(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0), forUpdate);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
			forUpdate ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkForUpdate(forUpdate);
		checkConsistency(ev.getSenderND(), forUpdate);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);
		checkQueryOption(request.optional_, fetchOption, isPartial, false);

		util::LockGuard<util::Mutex> guard(
				partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		txn.setAuditInfo(&ev,&ec,NULL);

		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_QUERY_GEOMETRY_WITH_EXCLUSION,
			inMes.query_,
			inMes.fetchOption_.limit_, inMes.fetchOption_.size_,
			forUpdate, request.fixed_.schemaVersionId_, emNow);

		StackAllocAutoPtr<DSTQLOutputMes> ret(alloc, NULL);
		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			ret.set(static_cast<DSTQLOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		}

		util::XArray<const util::XArray<uint8_t>*> logRecordList(alloc);
		util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
			request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
			keyStoreValue.storeType_, ret.get()->dsLog_);
		if (logBinary != NULL) {
			logRecordList.push_back(logBinary);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false),
			alloc, ret.get()->rs_, ev, isNewSQL(request));

		const bool ackWait = executeReplication(
			request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size(), &outMes);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, ackWait);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
			ev.getPartitionId(), ret.get()->rs_->getFetchNum());
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void GetRowTimeRelatedHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		TRANSACTION_EVENT_START(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0));

		const bool forUpdate = request.optional_.get<Options::FOR_UPDATE>();

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
			forUpdate ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkForUpdate(forUpdate);
		checkConsistency(ev.getSenderND(), forUpdate);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		if (forUpdate) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_NOT_EXECUTABLE,
				"row lock (forUpdate) is not applicable");
		}

		util::LockGuard<util::Mutex> guard(
				partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);

		txn.setAuditInfo(&ev,&ec,NULL);
		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_GET_TIME_RELATED, inMes.condition_,
			request.fixed_.schemaVersionId_, emNow);
		StackAllocAutoPtr<DSOutputMes> ret(alloc, NULL);
		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			ret.set(static_cast<DSOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false), ret.get()->mes_);
		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, NO_REPLICATION);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
			ev.getPartitionId(), ret.get()->num_);
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void GetRowInterpolateHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		TRANSACTION_EVENT_START(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0));

		const bool forUpdate = request.optional_.get<Options::FOR_UPDATE>();
		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
			forUpdate ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkForUpdate(forUpdate);
		checkConsistency(ev.getSenderND(), forUpdate);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		if (forUpdate) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_NOT_EXECUTABLE,
				"row lock (forUpdate) is not applicable");
		}

		util::LockGuard<util::Mutex> guard(
				partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);

		txn.setAuditInfo(&ev,&ec,NULL);
		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_GET_INTERPOLATE, inMes.condition_,
			request.fixed_.schemaVersionId_, emNow);
		StackAllocAutoPtr<DSOutputMes> ret(alloc, NULL);
		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			ret.set(static_cast<DSOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false), ret.get()->mes_);
		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, NO_REPLICATION);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
			ev.getPartitionId(), ret.get()->num_);
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void AggregateHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		TRANSACTION_EVENT_START(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0));

		AggregateQuery& query = inMes.query_;

		const bool forUpdate = request.optional_.get<Options::FOR_UPDATE>();
		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
			forUpdate ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkForUpdate(forUpdate);
		checkConsistency(ev.getSenderND(), forUpdate);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		if (forUpdate) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_NOT_EXECUTABLE,
				"row lock (forUpdate) is not applicable");
		}

		util::LockGuard<util::Mutex> guard(
				partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);

		txn.setAuditInfo(&ev,&ec,NULL);
		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_QUERY_AGGREGATE, query,
			request.fixed_.schemaVersionId_, emNow);
		StackAllocAutoPtr<DSOutputMes> ret(alloc, NULL);
		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			ret.set(static_cast<DSOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false), ret.get()->mes_);
		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, NO_REPLICATION);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
			ev.getPartitionId(), ret.get()->num_);
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void QueryTimeRangeHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);

		FetchOption& fetchOption = inMes.fetchOption_;
		bool isPartial = inMes.isPartial_;

		const bool forUpdate = request.optional_.get<Options::FOR_UPDATE>();
		TRANSACTION_EVENT_START_FOR_UPDATE(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0), forUpdate);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
			forUpdate ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkForUpdate(forUpdate);
		checkConsistency(ev.getSenderND(), forUpdate);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);
		checkQueryOption(request.optional_, fetchOption, isPartial, false);

		util::LockGuard<util::Mutex> guard(
				partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);

		txn.setAuditInfo(&ev,&ec,NULL);
		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_QUERY_TIME_RANGE,
			inMes.query_,
			inMes.fetchOption_.limit_, inMes.fetchOption_.size_,
			forUpdate, request.fixed_.schemaVersionId_, emNow);

		StackAllocAutoPtr<DSTQLOutputMes> ret(alloc, NULL);
		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			ret.set(static_cast<DSTQLOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		}

		util::XArray<const util::XArray<uint8_t>*> logRecordList(alloc);
		util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
			request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
			keyStoreValue.storeType_, ret.get()->dsLog_);
		if (logBinary != NULL) {
			logRecordList.push_back(logBinary);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false),
			alloc, ret.get()->rs_, ev, isNewSQL(request));

		const bool ackWait = executeReplication(
			request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size(), &outMes);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, ackWait);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
			ev.getPartitionId(), ret.get()->rs_->getFetchNum());
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void QueryTimeSamplingHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		TRANSACTION_EVENT_START(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0));

		FetchOption& fetchOption = inMes.fetchOption_;
		bool isPartial = inMes.isPartial_;

		const bool forUpdate = request.optional_.get<Options::FOR_UPDATE>();
		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
			forUpdate ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkForUpdate(forUpdate);
		checkConsistency(ev.getSenderND(), forUpdate);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);
		checkQueryOption(request.optional_, fetchOption, isPartial, false);

		if (forUpdate) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_NOT_EXECUTABLE,
				"row lock (forUpdate) is not applicable");
		}

		util::LockGuard<util::Mutex> guard(
				partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);

		txn.setAuditInfo(&ev,&ec,NULL);
		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_QUERY_SAMPLE,
			inMes.query_,
			inMes.fetchOption_.limit_, inMes.fetchOption_.size_,
			forUpdate, request.fixed_.schemaVersionId_, emNow);

		StackAllocAutoPtr<DSTQLOutputMes> ret(alloc, NULL);
		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			ret.set(static_cast<DSTQLOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		}

		util::XArray<const util::XArray<uint8_t>*> logRecordList(alloc);
		util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
			request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
			keyStoreValue.storeType_, ret.get()->dsLog_);
		if (logBinary != NULL) {
			logRecordList.push_back(logBinary);
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false),
			alloc, ret.get()->rs_, ev, isNewSQL(request));

		const bool ackWait = executeReplication(
			request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size(), &outMes);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, ackWait);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
			ev.getPartitionId(), ret.get()->rs_->getFetchNum());
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void FetchResultSetHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		TRANSACTION_EVENT_START(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0));

		ResultSetId rsId = inMes.rsId_;
		ResultSize startPos = inMes.startPos_;
		ResultSize fetchNum = inMes.fetchNum_;

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), ANY_CONSISTENCY);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		util::LockGuard<util::Mutex> guard(
				partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);

		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_QUERY_FETCH_RESULT_SET,
			rsId, startPos, fetchNum,
			request.fixed_.schemaVersionId_, emNow);
		StackAllocAutoPtr<DSTQLOutputMes> ret(alloc, NULL);
		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			ret.set(static_cast<DSTQLOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		}

		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false),
			ret.get()->rs_, ev);
		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, NO_REPLICATION);

		transactionService_->addRowReadCount(
			ev.getPartitionId(), ret.get()->rs_->getFetchNum());
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void CloseResultSetHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		EVENT_START(ec, ev, transactionManager_, connOption.dbId_, (ev.getSenderND().getId() < 0));

		ResultSetId rsId = inMes.rsId_;

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), ANY_CONSISTENCY);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		util::LockGuard<util::Mutex> guard(
				partitionList_->partition(request.fixed_.pId_).mutex());

		util::StackAllocator& alloc = ec.getAllocator();
		const util::DateTime now = ec.getHandlerStartTime();
		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);

		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		DSInputMes input(alloc, DS_QUERY_CLOSE_RESULT_SET, rsId);
		DSOutputMes* ret = NULL;
		{
			ret = static_cast<DSOutputMes*>(
				ds->exec(&txn, &keyStoreValue, &input));
		}
		transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false),
			ret->mes_);
		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, NO_REPLICATION);
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}


/*!
	@brief Handler Operator
*/
void MultiCreateTransactionContextHandler::operator()(
	EventContext& ec, Event& ev)

{
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
	util::TimeZone timeZone;

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		EVENT_START(ec, ev, transactionManager_, connOption.dbId_, (ev.getSenderND().getId() < 0));

		util::XArray<SessionCreationEntry>& entryList = inMes.entryList_;
		const bool withId = inMes.withId_;

		if ( AUDIT_TRACE_CHECK ) {
			AUDIT_TRACE_INFO_OPERATOR("")
		}

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		Event reply(ec, ev.getType(), ev.getPartitionId());

		EventByteOutStream out = encodeCommonPart(
			reply, request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS);
		encodeIntData<EventByteOutStream, uint32_t>(out, static_cast<uint32_t>(entryList.size()));
		bool isNewSql = isNewSQL(request);
		for (size_t i = 0; i < entryList.size(); i++) {
			SessionCreationEntry& entry = entryList[i];

			const ClientId clientId(request.fixed_.clientId_.uuid_, entry.sessionId_);

			if (!withId) {
				const util::StackAllocator::Scope scope(alloc);

				util::LockGuard<util::Mutex> guard(
					partitionList_->partition(request.fixed_.pId_).mutex());
				
				assert(entry.containerName_ != NULL);
				const TransactionManager::ContextSource src(request.fixed_.stmtType_);
				TransactionContext& txn = transactionManager_->put(
					alloc, request.fixed_.pId_, clientId, src, now, emNow);

				const FullContainerKey containerKey(
					alloc, getKeyConstraint(CONTAINER_ATTR_ANY),
					entry.containerName_->data(), entry.containerName_->size());

				checkLoggedInDatabase(
					connOption.dbId_, connOption.dbName_.c_str(),
					containerKey.getComponents(alloc).dbId_,
					request.optional_.get<Options::DB_NAME>()
					, isNewSql
				);
				bool isCaseSensitive = false;
				KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(txn,
					containerKey, isCaseSensitive);

				if (keyStoreValue.containerId_ != UNDEF_CONTAINERID &&
					!checkPrivilege(ev.getType(),
						connOption.userType_, connOption.requestType_,
						request.optional_.get<Options::SYSTEM_MODE>(),
						request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>(),
						keyStoreValue.attribute_)) {
					keyStoreValue = KeyDataStoreValue();
				}
				checkContainerExistence(keyStoreValue);
				entry.containerId_ = keyStoreValue.containerId_;
			}
			const TransactionManager::ContextSource cxtSrc(
				request.fixed_.stmtType_,
				request.fixed_.cxtSrc_.stmtId_, entry.containerId_,
				TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL,
				TransactionManager::CREATE, TransactionManager::AUTO_COMMIT,
				false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE, timeZone);
			TransactionContext& txn = transactionManager_->put(
				alloc, request.fixed_.pId_, clientId, cxtSrc, now, emNow);

			encodeLongData<EventByteOutStream, ContainerId>(out, txn.getContainerId());
		}

		ec.getEngine().send(reply, ev.getSenderND());
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

template <typename S>
bool MultiCreateTransactionContextHandler::decodeMultiTransactionCreationEntry(
	S& in,
	util::XArray<SessionCreationEntry>& entryList) {
	try {
		util::StackAllocator& alloc = *entryList.get_allocator().base();

		bool withId;
		decodeBooleanData<S>(in, withId);

		int32_t entryCount;
		in >> entryCount;
		entryList.reserve(static_cast<size_t>(entryCount));

		for (int32_t i = 0; i < entryCount; i++) {
			SessionCreationEntry entry;

			OptionSet optionSet(alloc);
			decodeOptionPart(in, optionSet);
			entry.containerAttribute_ =
				optionSet.get<Options::CONTAINER_ATTRIBUTE>();

			if (withId) {
				in >> entry.containerId_;
			}
			else {
				entry.containerName_ = ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
				decodeVarSizeBinaryData(in, *entry.containerName_);
			}

			in >> entry.sessionId_;

			entryList.push_back(entry);
		}

		return withId;
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Handler Operator
*/
void MultiCloseTransactionContextHandler::operator()(
	EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		EVENT_START(ec, ev, transactionManager_, connOption.dbId_, (ev.getSenderND().getId() < 0));

		util::XArray<SessionCloseEntry>& entryList = inMes.entryList_;

		if ( AUDIT_TRACE_CHECK ) {
			AUDIT_TRACE_INFO_OPERATOR("")
		}

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		util::XArray<ClientId> closedResourceIds(alloc);
		util::XArray<const util::XArray<uint8_t>*> logRecordList(alloc);

		for (size_t i = 0; i < entryList.size(); i++) {
			const SessionCloseEntry& entry = entryList[i];

			const ClientId clientId(request.fixed_.clientId_.uuid_, entry.sessionId_);

			try {
				util::LockGuard<util::Mutex> guard(
					partitionList_->partition(request.fixed_.pId_).mutex());

				TransactionContext& txn = transactionManager_->get(
					alloc, request.fixed_.pId_, clientId);
				if (txn.isActive()) {
					KeyDataStoreValue keyStoreValue = KeyDataStoreValue();
					try {
						keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc,
							txn.getContainerId());
					}
					catch (UserException& e) {
						UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
							"Container not found. (pId="
							<< txn.getPartitionId()
							<< ", clientId=" << txn.getClientId()
							<< ", containerId=" << txn.getContainerId()
							<< ", reason=" << GS_EXCEPTION_MESSAGE(e)
							<< ")");
					}
					if (keyStoreValue.containerId_ != UNDEF_CONTAINERID) {
						DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
						const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
						DSInputMes input(alloc, DS_ABORT, false);
						StackAllocAutoPtr<DSOutputMes> ret(alloc, NULL);
						{
							ret.set(static_cast<DSOutputMes*>(
								ds->exec(&txn, &keyStoreValue, &input)));
						}
						util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
							request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
							keyStoreValue.storeType_, ret.get()->dsLog_);
						if (logBinary != NULL) {
							logRecordList.push_back(logBinary);
						}
					}

					ec.setPendingPartitionChanged(txn.getPartitionId());
				}

				transactionManager_->remove(request.fixed_.pId_, clientId);
				closedResourceIds.push_back(clientId);
			}
			catch (ContextNotFoundException&) {
			}
		}

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false));

		request.fixed_.cxtSrc_.getMode_ = TransactionManager::AUTO;
		request.fixed_.cxtSrc_.txnMode_ = TransactionManager::AUTO_COMMIT;
		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);
		const bool ackWait = executeReplication(
			request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.fixed_.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(),
			closedResourceIds.data(), closedResourceIds.size(),
			logRecordList.data(), logRecordList.size(), &outMes);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, ackWait);
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}


template <typename S>
void MultiCloseTransactionContextHandler::decodeMultiTransactionCloseEntry(
	S& in,
	util::XArray<SessionCloseEntry>& entryList) {
	try {
		int32_t entryCount;
		in >> entryCount;
		entryList.reserve(static_cast<size_t>(entryCount));

		for (int32_t i = 0; i < entryCount; i++) {
			SessionCloseEntry entry;

			in >> entry.stmtId_;
			in >> entry.containerId_;
			in >> entry.sessionId_;

			entryList.push_back(entry);
		}
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Handles error
*/
void MultiStatementHandler::handleExecuteError(
	util::StackAllocator& alloc,
	const Event& ev, const Request& wholeRequest,
	EventType stmtType, PartitionId pId, const ClientId& clientId,
	const TransactionManager::ContextSource& src, Progress& progress,
	std::exception& e, const char8_t* executionName) {

	util::TimeZone timeZone;
	util::String containerName(alloc);
	ExceptionParameterList paramList(alloc);

	try {
		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(pId).mutex());

		const TransactionManager::ContextSource subSrc(
			stmtType, 0, src.containerId_,
			TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL,
			TransactionManager::AUTO, TransactionManager::AUTO_COMMIT,
			false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE, timeZone);
		TransactionContext& txn = transactionManager_->put(
			alloc, pId, TXN_EMPTY_CLIENTID, subSrc, 0, 0);
		FullContainerKey* containerKey = getKeyDataStore(txn.getPartitionId())->getKey(alloc, txn.getContainerId());
		if (containerKey != NULL) {
			containerKey->toString(alloc, containerName);
			paramList.push_back(ExceptionParameterEntry(
				"container", containerName.c_str()));
		}
	}
	catch (std::exception& e) {
		if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
			GS_RETHROW_SYSTEM_ERROR(e, "");
		}
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e, "");
	}

	try {
		MultiOpErrorMessage errorMessage(
			e, "", ev, wholeRequest,
			stmtType, pId, clientId, src,
			containerName.c_str(), executionName);

		try {
			throw;
		}
		catch (StatementAlreadyExecutedException&) {
			UTIL_TRACE_EXCEPTION_INFO(
				TRANSACTION_SERVICE, e,
				errorMessage.withDescription("Statement already executed"));
			progress.containerResult_.push_back(
				CONTAINER_RESULT_ALREADY_EXECUTED);
		}
		catch (LockConflictException& e2) {
			UTIL_TRACE_EXCEPTION_INFO(
				TRANSACTION_SERVICE, e,
				errorMessage.withDescription("Lock conflicted"));

			if (src.txnMode_ == TransactionManager::NO_AUTO_COMMIT_BEGIN) {
				try {
					util::LockGuard<util::Mutex> guard(
						partitionList_->partition(pId).mutex());
					TransactionContext& txn =
						transactionManager_->get(alloc, pId, clientId);
					util::XArray<uint8_t>* log = abortOnError(txn);
					if (log != NULL) {
						progress.txnLogMgr_.addLog(log);

						if (stmtType == PUT_MULTIPLE_CONTAINER_ROWS) {
							assert(
								progress.mputStartStmtId_ != UNDEF_STATEMENTID);
							transactionManager_->update(
								txn, progress.mputStartStmtId_);
						}
					}
				}
				catch (ContextNotFoundException&) {
					UTIL_TRACE_EXCEPTION_INFO(
						TRANSACTION_SERVICE, e,
						errorMessage.withDescription("Context not found"));
				}
			}

			try {
				checkLockConflictStatus(progress.lockConflictStatus_, e2);
			}
			catch (LockConflictException&) {
				assert(false);
				throw;
			}
			catch (...) {
				handleExecuteError(
					alloc, ev, wholeRequest, stmtType, pId, clientId, src,
					progress, e, executionName);
				return;
			}
			progress.lockConflicted_ = true;
		}
		catch (std::exception&) {
			if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
				GS_RETHROW_SYSTEM_ERROR(
					e, errorMessage.withDescription("System error"));
			}
			else {
				UTIL_TRACE_EXCEPTION(
					TRANSACTION_SERVICE, e,
					errorMessage.withDescription("User error"));

				try {
					util::LockGuard<util::Mutex> guard(
						partitionList_->partition(pId).mutex());
					TransactionContext& txn =
						transactionManager_->get(alloc, pId, clientId);
					util::XArray<uint8_t>* log = abortOnError(txn);
					progress.txnLogMgr_.addLog(log);
				}
				catch (ContextNotFoundException&) {
					UTIL_TRACE_EXCEPTION_INFO(
						TRANSACTION_SERVICE, e,
						errorMessage.withDescription("Context not found"));
				}
				if ( AUDIT_TRACE_CHECK ) {
					if ( AuditCategoryType(ev) != 0 ) {
						AUDIT_TRACE_ERROR_EXECUTE("")
					}
				}

				progress.lastExceptionData_.clear();
				util::XArrayByteOutStream out(
					util::XArrayOutStream<>(progress.lastExceptionData_));
				encodeException(
					out, e, TXN_DETAIL_EXCEPTION_HIDDEN, &paramList);

				progress.containerResult_.push_back(CONTAINER_RESULT_FAIL);
			}
		}
	}
	catch (std::exception&) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Handles error
*/
void MultiStatementHandler::handleWholeError(
	EventContext& ec, util::StackAllocator& alloc,
	const Event& ev, const Request& request, std::exception& e) {
	ErrorMessage errorMessage(e, "", ev, request);
	try {
		throw;
	}
	catch (EncodeDecodeException&) {
		UTIL_TRACE_EXCEPTION(
			TRANSACTION_SERVICE, e, errorMessage.withDescription(
				"Failed to encode or decode message"));
		if ( AUDIT_TRACE_CHECK ) {
			if ( AuditCategoryType(ev) != 0 ) {
				AUDIT_TRACE_ERROR_OPERATOR("")
			}
		}
		replyError(
			ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_ERROR, request, e);
	}
	catch (DenyException&) {
		UTIL_TRACE_EXCEPTION(
			TRANSACTION_SERVICE, e, errorMessage.withDescription(
				"Request denied"));
		if ( AUDIT_TRACE_CHECK ) {
			if ( AuditCategoryType(ev) != 0 ) {
				AUDIT_TRACE_ERROR_OPERATOR("")
			}
		}
		replyError(
			ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_DENY, request, e);
	}
	catch (std::exception&) {
		const bool isCritical = GS_EXCEPTION_CHECK_CRITICAL(e);
		const char8_t* description = (isCritical ? "System error" : "User error");
		const StatementExecStatus returnStatus =
			(isCritical ? TXN_STATEMENT_NODE_ERROR : TXN_STATEMENT_ERROR);

		UTIL_TRACE_EXCEPTION(
			TRANSACTION_SERVICE, e, errorMessage.withDescription(
				description));
		if ( AUDIT_TRACE_CHECK ) {
			if ( AuditCategoryType(ev) != 0 ) {
				AUDIT_TRACE_ERROR_OPERATOR("")
			}
		}
		replyError(
			ec, alloc, ev.getSenderND(), ev.getType(), returnStatus,
			request, e);

		if (isCritical) {
			clusterService_->setError(ec, &e);
		}
	}
}

void MultiStatementHandler::decodeContainerOptionPart(
	EventByteInStream& in, const FixedRequest& fixedRequest,
	OptionSet& optionSet) {
	try {
		optionSet.set<Options::TXN_TIMEOUT_INTERVAL>(-1);
		decodeOptionPart(in, optionSet);

		if (optionSet.get<Options::TXN_TIMEOUT_INTERVAL>() < 0) {
			optionSet.set<Options::TXN_TIMEOUT_INTERVAL>(
				fixedRequest.cxtSrc_.txnTimeoutInterval_);
		}
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

void MultiStatementHandler::MultiOpErrorMessage::format(
	std::ostream& os) const {
	const ErrorMessage::Elements& elements = base_.elements_;

	os << GS_EXCEPTION_MESSAGE(elements.cause_);
	os << " (" << elements.description_;
	os << ", containerName=" << containerName_;
	os << ", executionName=" << executionName_;
	base_.formatParameters(os);
	os << ")";
}

/*!
	@brief Handler Operator
*/
void MultiPutHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);

	try {
		ConnectionOption& connOption =
			ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);
		TRANSACTION_EVENT_START(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0));

		util::XArray<const MessageSchema*> schemaList(alloc);
		util::XArray<const RowSetRequest*> rowSetList(alloc);

		decodeMultiSchema(in, request, schemaList);
		decodeMultiRowSet(in, request, schemaList, rowSetList);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkPrivilegeForOperator();
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);


		CheckedSchemaIdSet checkedSchemaIdSet(alloc);
		checkedSchemaIdSet.reserve(rowSetList.size());

		TxnLogManager txnLogMgr(transactionService_, ec, request.fixed_.pId_);
		Progress progress(alloc, getLockConflictStatus(ev, emNow, request), txnLogMgr);

		const PutRowOption putRowOption =
			request.optional_.get<Options::PUT_ROW_OPTION>();

		bool succeeded = true;
		for (size_t i = 0; i < rowSetList.size(); i++) {
			const RowSetRequest& rowSetRequest = *(rowSetList[i]);
			const MessageSchema& messageSchema =
				*(schemaList[rowSetRequest.schemaIndex_]);

			execute(
				ec, ev, request, rowSetRequest, messageSchema,
				checkedSchemaIdSet, progress, putRowOption);

			if (!progress.lastExceptionData_.empty() ||
				progress.lockConflicted_) {
				succeeded = false;
				break;
			}
		}

		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false));
		bool ackWait = false;
		{
			util::LockGuard<util::Mutex> guard(
				partitionList_->partition(request.fixed_.pId_).mutex());

			TransactionContext& txn = transactionManager_->put(
				alloc, request.fixed_.pId_, request.fixed_.clientId_,
				request.fixed_.cxtSrc_, now, emNow);

			const int32_t replMode = succeeded ?
				transactionManager_->getReplicationMode() :
				TransactionManager::REPLICATION_ASYNC;
			const NodeDescriptor& clientND = succeeded ?
				ev.getSenderND() : NodeDescriptor::EMPTY_ND;

			ackWait = executeReplication(
				request, ec, alloc, clientND, txn,
				request.fixed_.stmtType_, request.fixed_.cxtSrc_.stmtId_, replMode, NULL, 0,
				progress.txnLogMgr_.getLogList().data(), progress.txnLogMgr_.getLogList().size(),
				&outMes);
		}

		if (progress.lockConflicted_) {
			retryLockConflictedRequest(ec, ev, progress.lockConflictStatus_);
		}
		else if (succeeded) {
			replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
				request, outMes, ackWait);
		}
		else {
			Event errorReply(ec, request.fixed_.stmtType_, request.fixed_.pId_);
			EventByteOutStream out = encodeCommonPart(
				errorReply, request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_ERROR);
			out.writeAll(progress.lastExceptionData_.data(),
				progress.lastExceptionData_.size());
			ec.getEngine().send(errorReply, ev.getSenderND());
		}

		if (succeeded) {
			transactionService_->incrementWriteOperationCount(
				ev.getPartitionId());
			transactionService_->addRowWriteCount(
				ev.getPartitionId(), progress.totalRowCount_);
		}
	}
	catch (std::exception& e) {
		handleWholeError(ec, alloc, ev, request, e);
	}
}

void MultiPutHandler::execute(
	EventContext& ec, const Event& ev, const Request& request,
	const RowSetRequest& rowSetRequest, const MessageSchema& schema,
	CheckedSchemaIdSet& idSet, Progress& progress,
	PutRowOption putRowOption) {
	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	const ClientId clientId(request.fixed_.clientId_.uuid_, rowSetRequest.sessionId_);
	const TransactionManager::ContextSource src =
		createContextSource(request, rowSetRequest);
	const util::XArray<uint8_t>& multiRowData = rowSetRequest.rowSetData_;
	const uint64_t numRow = rowSetRequest.rowCount_;
	assert(numRow > 0);
	progress.mputStartStmtId_ = UNDEF_STATEMENTID;

	try {
		util::LockGuard<util::Mutex> guard(
				partitionList_->partition(request.fixed_.pId_).mutex());

		const bool STATEMENTID_NO_CHECK = true;
		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_,
			clientId, src, now, emNow, STATEMENTID_NO_CHECK);
		progress.mputStartStmtId_ = txn.getLastStatementId();

		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());

		checkContainerExistence(keyStoreValue);
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);

		DSInputMes input(alloc, DS_GET_CONTAINER_OBJECT, ANY_CONTAINER);
		StackAllocAutoPtr<DSContainerOutputMes> ret(alloc, NULL);
		{
			ret.set(static_cast<DSContainerOutputMes*>(
				ds->exec(&txn, &keyStoreValue, &input)));
		}
		BaseContainer* container = ret.get()->container_;
		checkContainerExistence(container);

		const size_t capacityBefore = idSet.base().capacity();
		UNUSED_VARIABLE(capacityBefore);

		checkSchema(txn, *container, schema, rowSetRequest.schemaIndex_, idSet);
		assert(idSet.base().capacity() == capacityBefore);
		if ( AUDIT_TRACE_CHECK ) {
			ConnectionOption& connOption =
				ev.getSenderND().getUserData<ConnectionOption>();
			AUDIT_TRACE_INFO_OPERATOR(getAuditContainerName(alloc,
				 getKeyDataStore(txn.getPartitionId())->getKey(alloc, txn.getContainerId())))
		}
		{
			const util::StackAllocator::Scope scope(alloc);

			const bool NO_VALIDATE_ROW_IMAGE = false;
			InputMessageRowStore inputMessageRowStore(
				static_cast<DataStoreV4*>(ds)->getConfig(), container->getColumnInfoList(),
				container->getColumnNum(),
				const_cast<uint8_t*>(multiRowData.data()),
				static_cast<uint32_t>(multiRowData.size()), numRow,
				container->getRowFixedDataSize(), NO_VALIDATE_ROW_IMAGE);

			util::XArray<RowId> rowIds(alloc);
			rowIds.assign(static_cast<size_t>(1), UNDEF_ROWID);
			util::XArray<uint8_t> rowData(alloc);

			for (StatementId rowStmtId = src.stmtId_; inputMessageRowStore.next();
				rowStmtId++) {
				try {
					TransactionManager::ContextSource rowSrc = src;
					rowSrc.stmtId_ = rowStmtId;
					txn = transactionManager_->put(
						alloc, request.fixed_.pId_, clientId, rowSrc, now, emNow);
				}
				catch (StatementAlreadyExecutedException& e) {
					UTIL_TRACE_EXCEPTION_INFO(TRANSACTION_SERVICE, e,
						"Row already put. (pId=" <<
						request.fixed_.pId_ <<
						", eventType=" << EventTypeUtility::getEventTypeName(PUT_MULTIPLE_CONTAINER_ROWS) <<
						", stmtId=" << rowStmtId << ", clientId=" << clientId <<
						", containerId=" <<
						((src.containerId_ == UNDEF_CONTAINERID) ?
							0 : src.containerId_) <<
						", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");

					continue;
				}

				rowData.clear();
				inputMessageRowStore.getCurrentRowData(rowData);

				bool isEmptyLog = true;
				DSInputMes input(alloc, DS_PUT_ROW, &rowData, putRowOption,
					MAX_SCHEMAVERSIONID, isEmptyLog);
				StackAllocAutoPtr<DSOutputMes> ret(alloc, NULL);
				{
					ret.set(static_cast<DSOutputMes*>(
						ds->exec(&txn, &keyStoreValue, &input)));
				}
				util::XArray<uint8_t>* logBinary = appendDataStoreLog(
					progress.txnLogMgr_.getLogAllocator(), txn,
					request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
					keyStoreValue.storeType_, ret.get()->dsLog_);
				progress.txnLogMgr_.addLog(logBinary);
				transactionManager_->update(txn, rowStmtId);
			}
		}

		progress.containerResult_.push_back(CONTAINER_RESULT_SUCCESS);
		progress.totalRowCount_ += numRow;
	}
	catch (std::exception& e) {
		try {
			throw;
		}
		catch (UserException& e2) {
			if (e2.getErrorCode() == GS_ERROR_DS_DS_CONTAINER_EXPIRED) {
				progress.containerResult_.push_back(CONTAINER_RESULT_SUCCESS);
				return;
			}
		}
		catch (...) {
		}
		handleExecuteError(
			alloc, ev, request, PUT_MULTIPLE_CONTAINER_ROWS,
			request.fixed_.pId_, clientId, src, progress, e, "put");
	}
}

void MultiPutHandler::checkSchema(
	TransactionContext& txn,
	BaseContainer& container, const MessageSchema& schema,
	int32_t localSchemaId, CheckedSchemaIdSet& idSet) {
	util::StackAllocator& alloc = txn.getDefaultAllocator();

	ColumnSchemaId schemaId = container.getColumnSchemaId();

	if (schemaId != UNDEF_COLUMN_SCHEMAID &&
		idSet.find(CheckedSchemaId(localSchemaId, schemaId)) != idSet.end()) {
		return;
	}


	const uint32_t columnCount = container.getColumnNum();
	ColumnInfo* infoList = container.getColumnInfoList();

	if (columnCount != schema.getColumnCount()) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID, "");
	}

	util::XArray<char8_t> name1(alloc);
	util::XArray<char8_t> name2(alloc);

	for (uint32_t i = 0; i < columnCount; i++) {
		ColumnInfo& info = infoList[i];

		if (info.getColumnType() != schema.getColumnFullType(i)) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID, "");
		}

		const char8_t* name1Ptr =
			info.getColumnName(txn, *(container.getObjectManager()), container.getMetaAllocateStrategy());
		const char8_t* name2Ptr = schema.getColumnName(i).c_str();

		name1.resize(strlen(name1Ptr));
		name2.resize(strlen(name2Ptr));

		if (name1.size() != name2.size()) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID, "");
		}

		if (memcmp(name1Ptr, name2Ptr, name1.size()) != 0) {
			ValueProcessor::convertUpperCase(
				name1Ptr, name1.size(), name1.data());

			ValueProcessor::convertUpperCase(
				name2Ptr, name2.size(), name2.data());

			if (memcmp(name1.data(), name2.data(), name1.size()) != 0) {
				GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID, "");
			}
		}
	}
	util::Vector<ColumnId> keyColumnIdList(txn.getDefaultAllocator());
	container.getKeyColumnIdList(keyColumnIdList);
	const util::Vector<ColumnId>& schemaKeyColumnIdList = schema.getRowKeyColumnIdList();
	if (keyColumnIdList.size() != schemaKeyColumnIdList.size()) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID, "");
	}
	for (size_t i = 0; i < keyColumnIdList.size(); i++) {
		if (keyColumnIdList[i] != schemaKeyColumnIdList[i]) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID, "");
		}
	}
	if (schemaId != UNDEF_COLUMN_SCHEMAID) {
		idSet.insert(CheckedSchemaId(localSchemaId, schemaId));
	}
}

TransactionManager::ContextSource MultiPutHandler::createContextSource(
	const Request& request, const RowSetRequest& rowSetRequest) {
	return TransactionManager::ContextSource(
		request.fixed_.stmtType_, rowSetRequest.stmtId_,
		rowSetRequest.containerId_,
		rowSetRequest.option_.get<Options::TXN_TIMEOUT_INTERVAL>(),
		rowSetRequest.getMode_,
		rowSetRequest.txnMode_,
		false,
		request.fixed_.cxtSrc_.storeMemoryAgingSwapRate_,
		request.fixed_.cxtSrc_.timeZone_);
}

template <typename S>
void MultiPutHandler::decodeMultiSchema(
	S& in, const Request& request,
	util::XArray<const MessageSchema*>& schemaList) {
	try {
		util::StackAllocator& alloc = *schemaList.get_allocator().base();

		int32_t headCount;
		in >> headCount;
		schemaList.reserve(static_cast<size_t>(headCount));

		for (int32_t i = 0; i < headCount; i++) {
			ContainerType containerType;
			decodeEnumData< S, ContainerType>(in, containerType);

			const char8_t* dummyContainerName = "_";
			MessageSchema* schema = ALLOC_NEW(alloc) MessageSchema(alloc,
				getDataStore(request.fixed_.pId_)->getConfig(), dummyContainerName, in, MessageSchema::DEFAULT_VERSION);

			schemaList.push_back(schema);
		}
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

template <typename S>
void MultiPutHandler::decodeMultiRowSet(
	S& in, const Request& request,
	const util::XArray<const MessageSchema*>& schemaList,
	util::XArray<const RowSetRequest*>& rowSetList) {
	try {
		util::StackAllocator& alloc = *rowSetList.get_allocator().base();

		int32_t bodyCount;
		in >> bodyCount;
		rowSetList.reserve(static_cast<size_t>(bodyCount));

		for (int32_t i = 0; i < bodyCount; i++) {
			RowSetRequest* subRequest = ALLOC_NEW(alloc) RowSetRequest(alloc);

			in >> subRequest->stmtId_;
			in >> subRequest->containerId_;
			in >> subRequest->sessionId_;

			in >> subRequest->getMode_;
			in >> subRequest->txnMode_;

			decodeContainerOptionPart(in, request.fixed_, subRequest->option_);

			in >> subRequest->schemaIndex_;

			if (subRequest->schemaIndex_ < 0 ||
				static_cast<size_t>(subRequest->schemaIndex_) >=
				schemaList.size()) {
				TXN_THROW_DECODE_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
			}

			uint32_t bytesSize;
			in >> bytesSize;
			subRequest->rowSetData_.resize(
				bytesSize - sizeof(subRequest->rowCount_));

			in >> subRequest->rowCount_;
			in.readAll(
				subRequest->rowSetData_.data(), subRequest->rowSetData_.size());

			rowSetList.push_back(subRequest);
		}
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Handler Operator
*/
void MultiGetHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);

	try {
		ConnectionOption& connOption =
			ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);

		util::XArray<SearchEntry> searchList(alloc);

		decodeMultiSearchEntry(
			in, request.fixed_.pId_,
			connOption.dbId_, connOption.dbName_.c_str(),
			request.optional_.get<Options::DB_NAME>(),
			connOption.userType_, connOption.requestType_,
			request.optional_.get<Options::SYSTEM_MODE>(),
			searchList);
		TRANSACTION_EVENT_START(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0));

		const bool forUpdate = request.optional_.get<Options::FOR_UPDATE>();
		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
			forUpdate ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		const int32_t acceptableFeatureVersion =
			request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>();
		checkAuthentication(ev.getSenderND(), emNow);  
		connOption.checkForUpdate(forUpdate);
		checkConsistency(ev.getSenderND(), forUpdate);
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		if (forUpdate) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_NOT_EXECUTABLE,
				"row lock (forUpdate) is not applicable");
		}

		Event reply(ec, ev.getType(), ev.getPartitionId());

		EventByteOutStream out = encodeCommonPart(
			reply, request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS);

		SchemaMap schemaMap(alloc);
		buildSchemaMap(
				request.fixed_.pId_, searchList, schemaMap, out,
				acceptableFeatureVersion);

		const size_t bodyTopPos = out.base().position();
		uint32_t entryCount = 0;
		encodeIntData<EventByteOutStream, uint32_t>(out, entryCount);

		TxnLogManager txnLogMgr(transactionService_, ec, request.fixed_.pId_);
		Progress progress(alloc, getLockConflictStatus(ev, emNow, request), txnLogMgr);

		bool succeeded = true;
		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			for (size_t i = 0; i < searchList.size(); i++) {
				const SearchEntry& entry = searchList[i];

				entryCount += execute(
					ec, ev, request, entry, schemaMap, out, progress);

				if (!progress.lastExceptionData_.empty() ||
					progress.lockConflicted_) {
					succeeded = false;
					break;
				}
			}
		}


		if (progress.lockConflicted_) {
			retryLockConflictedRequest(ec, ev, progress.lockConflictStatus_);
		}
		else if (succeeded) {
			const size_t bodyEndPos = out.base().position();
			out.base().position(bodyTopPos);
			encodeIntData<EventByteOutStream, uint32_t>(out, entryCount);
			out.base().position(bodyEndPos);
			ec.getEngine().send(reply, ev.getSenderND());
		}
		else {
			Event errorReply(ec, request.fixed_.stmtType_, request.fixed_.pId_);
			EventByteOutStream out = encodeCommonPart(
				errorReply, request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_ERROR);
			out.writeAll(progress.lastExceptionData_.data(),
				progress.lastExceptionData_.size());
			ec.getEngine().send(errorReply, ev.getSenderND());
		}

		if (succeeded) {
			transactionService_->incrementReadOperationCount(
				ev.getPartitionId());
			transactionService_->addRowReadCount(
				ev.getPartitionId(), progress.totalRowCount_);
		}
	}
	catch (std::exception& e) {
		handleWholeError(ec, alloc, ev, request, e);
	}
}

uint32_t MultiGetHandler::execute(
	EventContext& ec, const Event& ev, const Request& request,
	const SearchEntry& entry, const SchemaMap& schemaMap,
	EventByteOutStream& replyOut, Progress& progress) {
	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	const ClientId clientId(request.fixed_.clientId_.uuid_, entry.sessionId_);
	const TransactionManager::ContextSource src =
		createContextSource(request, entry);

	const char8_t* getType =
		(entry.predicate_->predicateType_ == PREDICATE_TYPE_RANGE) ?
		"rangeKey" : "distinctKey";

	uint32_t entryCount = 0;

	try {
		const util::StackAllocator::Scope scope(alloc);

		util::LockGuard<util::Mutex> guard(
				partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_, clientId, src, now, emNow);

		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc,
			entry.containerId_);
		checkContainerExistence(keyStoreValue);
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_GET_CONTAINER_OBJECT, ANY_CONTAINER);
		StackAllocAutoPtr<DSContainerOutputMes> ret(alloc, NULL);
		ret.set(static_cast<DSContainerOutputMes*>(
			ds->exec(&txn, &keyStoreValue, &input)));
		BaseContainer* container = ret.get()->container_;

		const RowKeyPredicate& predicate = *entry.predicate_;
		checkContainerRowKey(container, predicate);

		if ( AUDIT_TRACE_CHECK ) {
			ConnectionOption& connOption =
				ev.getSenderND().getUserData<ConnectionOption>();
			AUDIT_TRACE_INFO_OPERATOR(getAuditContainerName(alloc, 
				getKeyDataStore(txn.getPartitionId())->getKey(alloc, txn.getContainerId())))
		}
		if (predicate.predicateType_ == PREDICATE_TYPE_RANGE) {
			RowData* startRowKey = NULL;
			RowData* finishRowKey = NULL;

			if (predicate.keyType_ == 255) {
				const bool NO_VALIDATE_ROW_IMAGE = true;
				InputMessageRowStore inputMessageRowStore(
					getDataStore(txn.getPartitionId())->getConfig(), container->getRowKeyColumnInfoList(txn),
					container->getRowKeyColumnNum(),
					const_cast<uint8_t*>(predicate.compositeKeys_->data()),
					static_cast<uint32_t>(predicate.compositeKeys_->size()),
					predicate.rowCount_, container->getRowKeyFixedDataSize(alloc),
					NO_VALIDATE_ROW_IMAGE);

				if (predicate.rangeFlags_[0]) {
					if (inputMessageRowStore.next()) {
						startRowKey = ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
						inputMessageRowStore.getCurrentRowData(*startRowKey);
					}
					else {
						assert(false);
					}
				}
				if (predicate.rangeFlags_[1]) {
					if (inputMessageRowStore.next()) {
						finishRowKey = ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
						inputMessageRowStore.getCurrentRowData(*finishRowKey);
					}
					else {
						assert(false);
					}
				}
			}
			else {
				assert(predicate.keys_->size() == 2);
				startRowKey = (*predicate.keys_)[0];
				finishRowKey = (*predicate.keys_)[1];
			}
			executeRange(ec, schemaMap, txn, container, startRowKey,
				finishRowKey, replyOut, progress);
			entryCount++;
		}
		else {
			if (predicate.keyType_ == 255) {
				RowData rowKey(alloc);

				const bool NO_VALIDATE_ROW_IMAGE = true;
				InputMessageRowStore inputMessageRowStore(
					getDataStore(txn.getPartitionId())->getConfig(), container->getRowKeyColumnInfoList(txn),
					container->getRowKeyColumnNum(),
					const_cast<uint8_t*>(predicate.compositeKeys_->data()),
					static_cast<uint32_t>(predicate.compositeKeys_->size()),
					predicate.rowCount_, container->getRowKeyFixedDataSize(alloc),
					NO_VALIDATE_ROW_IMAGE);
				while (inputMessageRowStore.next()) {
					rowKey.clear();
					inputMessageRowStore.getCurrentRowData(rowKey);
					executeGet(ec, schemaMap, txn, container, rowKey,
						replyOut, progress);
					entryCount++;
				}
			}
			else {
				const RowKeyDataList& distinctKeys = *predicate.keys_;
				for (RowKeyDataList::const_iterator it = distinctKeys.begin();
					it != distinctKeys.end(); ++it) {
					const RowKeyData& rowKey = **it;
					executeGet(ec, schemaMap, txn, container, rowKey,
						replyOut, progress);
					entryCount++;
				}
			}
		}

	}
	catch (std::exception& e) {
		handleExecuteError(
			alloc, ev, request, GET_MULTIPLE_CONTAINER_ROWS,
			request.fixed_.pId_, clientId, src, progress, e, getType);
	}

	return entryCount;
}

void MultiGetHandler::executeRange(
	EventContext& ec, const SchemaMap& schemaMap,
	TransactionContext& txn, BaseContainer* container, const RowKeyData* startKey,
	const RowKeyData* finishKey, EventByteOutStream& replyOut, Progress& progress) {
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	ResultSet* rs = getDataStore(txn.getPartitionId())->getResultSetManager()->create(txn,
		container->getContainerId(), container->getVersionId(), emNow,
		NULL);
	const ResultSetGuard rsGuard(txn, *getDataStore(txn.getPartitionId()), *rs);

	QueryProcessor::search(txn, *container, MAX_RESULT_SIZE,
		startKey, finishKey, *rs);
	rs->setResultType(RESULT_ROWSET);


	QueryProcessor::fetch(txn, *container, 0, MAX_RESULT_SIZE, rs);

	encodeEntry(
		container->getContainerKey(txn), container->getContainerId(),
		schemaMap, *rs, replyOut);

	progress.totalRowCount_ += rs->getResultNum();
}

void MultiGetHandler::executeGet(
	EventContext& ec, const SchemaMap& schemaMap,
	TransactionContext& txn, BaseContainer* container, const RowKeyData& rowKey,
	EventByteOutStream& replyOut, Progress& progress) {
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
	ResultSet* rs = getDataStore(txn.getPartitionId())->getResultSetManager()->create(txn,
		container->getContainerId(), container->getVersionId(),
		emNow, NULL);
	const ResultSetGuard rsGuard(txn, *getDataStore(txn.getPartitionId()), *rs);

	QueryProcessor::get(txn, *container,
		static_cast<uint32_t>(rowKey.size()), rowKey.data(), *rs);
	rs->setResultType(RESULT_ROWSET);


	QueryProcessor::fetch(
		txn, *container, 0, MAX_RESULT_SIZE, rs);

	encodeEntry(
		container->getContainerKey(txn),
		container->getContainerId(), schemaMap, *rs, replyOut);

	progress.totalRowCount_ += rs->getFetchNum();
}

void MultiGetHandler::buildSchemaMap(
		PartitionId pId,
		const util::XArray<SearchEntry>& searchList, SchemaMap& schemaMap,
		EventByteOutStream& out, int32_t acceptableFeatureVersion) {
	typedef util::XArray<uint8_t> SchemaData;
	typedef std::pair< ColumnSchemaId, ContainerType > LocalSchemaType;
	typedef util::Map<LocalSchemaType, LocalSchemaId> LocalIdMap;
	typedef LocalIdMap::iterator LocalIdMapItr;

	util::TimeZone timeZone;
	util::StackAllocator& alloc = *schemaMap.get_allocator().base();

	SchemaData schemaBuf(alloc);


	const size_t outStartPos = out.base().position();
	int32_t schemaCount = 0;
	out << schemaCount;

	LocalIdMap localIdMap(alloc);
	for (util::XArray<SearchEntry>::const_iterator it = searchList.begin();;
		++it) {
		if (it == searchList.end()) {
			const size_t outEndPos = out.base().position();
			out.base().position(outStartPos);
			out << schemaCount;
			out.base().position(outEndPos);
			return;
		}

		util::LockGuard<util::Mutex> guard(
				partitionList_->partition(pId).mutex());

		const ContainerId containerId = it->containerId_;
		const TransactionManager::ContextSource src(
			GET_MULTIPLE_CONTAINER_ROWS,
			0, containerId, TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL,
			TransactionManager::AUTO, TransactionManager::AUTO_COMMIT,
			false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE, timeZone);
		TransactionContext& txn =
			transactionManager_->put(alloc, pId, TXN_EMPTY_CLIENTID, src, 0, 0);

		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, containerId);
		checkContainerExistence(keyStoreValue);
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_GET_CONTAINER_OBJECT, ANY_CONTAINER);
		StackAllocAutoPtr<DSContainerOutputMes> ret(alloc, NULL);
		{
			ret.set(static_cast<DSContainerOutputMes*>(
				ds->exec(&txn, &keyStoreValue, &input)));
		}
		BaseContainer* container = ret.get()->container_;
		checkContainerExistence(container);

		const ContainerType containerType = container->getContainerType();
		ColumnSchemaId columnSchemaId = container->getColumnSchemaId();


		if (columnSchemaId == UNDEF_COLUMN_SCHEMAID) {
			break;
		}

		LocalSchemaType localSchemaType(columnSchemaId, containerType);
		LocalIdMapItr idIt = localIdMap.find(localSchemaType);
		LocalSchemaId localId;
		if (idIt == localIdMap.end()) {
			localId = schemaCount;

			schemaBuf.clear();
			{
				util::XArrayByteOutStream schemaOut = util::XArrayByteOutStream(
					util::XArrayOutStream<>(schemaBuf));
				schemaOut << static_cast<uint8_t>(containerType);
			}

			checkSchemaFeatureVersion(*container, acceptableFeatureVersion);

			const bool optionIncluded = false;
			container->getContainerInfo(txn, schemaBuf, optionIncluded);

			out.writeAll(schemaBuf.data(), schemaBuf.size());

			schemaCount++;
		}
		else {
			localId = idIt->second;
		}

		localIdMap.insert(std::make_pair(localSchemaType, localId));
		schemaMap.insert(std::make_pair(containerId, localId));
	}


	out.base().position(outStartPos);
	schemaMap.clear();


	typedef util::XArray<SchemaData*> SchemaList;
	typedef util::MultiMap<uint32_t, LocalSchemaId> DigestMap;
	typedef DigestMap::iterator DigestMapItr;

	SchemaList schemaList(alloc);
	DigestMap digestMap(alloc);

	for (util::XArray<SearchEntry>::const_iterator it = searchList.begin();
		it != searchList.end(); ++it) {
		util::LockGuard<util::Mutex> guard(
				partitionList_->partition(pId).mutex());

		const ContainerId containerId = it->containerId_;
		const TransactionManager::ContextSource src(
			GET_MULTIPLE_CONTAINER_ROWS,
			0, containerId, TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL,
			TransactionManager::AUTO, TransactionManager::AUTO_COMMIT,
			false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE, timeZone);
		TransactionContext& txn =
			transactionManager_->put(alloc, pId, TXN_EMPTY_CLIENTID, src, 0, 0);

		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, containerId);
		checkContainerExistence(keyStoreValue);
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_GET_CONTAINER_OBJECT, ANY_CONTAINER);
		StackAllocAutoPtr<DSContainerOutputMes> ret(alloc, NULL);
		{
			ret.set(static_cast<DSContainerOutputMes*>(ds->exec(&txn, &keyStoreValue, &input)));
		}
		BaseContainer* container = ret.get()->container_;
		checkContainerExistence(container);

		const ContainerType containerType = container->getContainerType();

		schemaBuf.clear();
		{
			util::XArrayByteOutStream schemaOut =
				util::XArrayByteOutStream(util::XArrayOutStream<>(schemaBuf));
			schemaOut << static_cast<uint8_t>(containerType);
		}

		const bool optionIncluded = false;
		container->getContainerInfo(txn, schemaBuf, optionIncluded);

		uint32_t digest = 1;
		for (SchemaData::iterator schemaIt = schemaBuf.begin();
			schemaIt != schemaBuf.end(); ++schemaIt) {
			digest = 31 * digest + (*schemaIt);
		}

		LocalSchemaId schemaId = -1;
		std::pair<DigestMapItr, DigestMapItr> range =
			digestMap.equal_range(digest);

		for (DigestMapItr rangeIt = range.first; rangeIt != range.second;
			++rangeIt) {
			SchemaData& targetSchema = *schemaList[rangeIt->second];

			if (targetSchema.size() == schemaBuf.size() &&
				memcmp(targetSchema.data(), schemaBuf.data(),
					schemaBuf.size()) == 0) {
				schemaId = rangeIt->second;
				break;
			}
		}

		if (schemaId < 0) {
			SchemaData* targetSchema = ALLOC_NEW(alloc) SchemaData(alloc);
			targetSchema->resize(schemaBuf.size());
			memcpy(targetSchema->data(), schemaBuf.data(), schemaBuf.size());

			schemaId = static_cast<int32_t>(schemaList.size());
			digestMap.insert(std::make_pair(digest, schemaId));
			schemaList.push_back(targetSchema);
		}

		schemaMap.insert(std::make_pair(containerId, schemaId));
	}

	out << static_cast<int32_t>(schemaList.size());
	for (SchemaList::iterator it = schemaList.begin(); it != schemaList.end();
		++it) {
		SchemaData& targetSchema = **it;
		out.writeAll(targetSchema.data(), targetSchema.size());
	}
}

void MultiGetHandler::checkContainerRowKey(
	BaseContainer* container, const RowKeyPredicate& predicate) {
	checkContainerExistence(container);

	if (!container->definedRowKey()) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_COL_ROWKEY_UNDEFINED, "");
	}

	if (predicate.compositeColumnTypes_ != NULL) {
		bool isMatch = container->checkRowKeySchema(*(predicate.compositeColumnTypes_));
		if (!isMatch) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_COL_ROWKEY_INVALID, "");
		}
	}
	else {
		const ColumnInfo& keyColumnInfo =
			container->getColumnInfo(ColumnInfo::ROW_KEY_COLUMN_ID);
		if (predicate.keyType_ != keyColumnInfo.getColumnType()) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_COL_ROWKEY_INVALID, "");
		}
	}
}

TransactionManager::ContextSource MultiGetHandler::createContextSource(
	const Request& request, const SearchEntry& entry) {
	return TransactionManager::ContextSource(
		request.fixed_.stmtType_, entry.stmtId_,
		entry.containerId_,
		request.fixed_.cxtSrc_.txnTimeoutInterval_,
		entry.getMode_,
		entry.txnMode_,
		false,
		request.fixed_.cxtSrc_.storeMemoryAgingSwapRate_,
		request.fixed_.cxtSrc_.timeZone_);
}

template <typename S>
void MultiGetHandler::decodeMultiSearchEntry(
	S& in, PartitionId pId,
	DatabaseId loginDbId,
	const char8_t* loginDbName, const char8_t* specifiedDbName,
	UserType userType, RequestType requestType, bool isSystemMode,
	util::XArray<SearchEntry>& searchList) {
	try {
		util::StackAllocator& alloc = *searchList.get_allocator().base();
		util::XArray<const RowKeyPredicate*> predicateList(alloc);

		{
			int32_t headCount;
			in >> headCount;
			predicateList.reserve(static_cast<size_t>(headCount));

			for (int32_t i = 0; i < headCount; i++) {
				const RowKeyPredicate* predicate = ALLOC_NEW(alloc)
					RowKeyPredicate(decodePredicate(in, alloc));
				predicateList.push_back(predicate);
			}
		}

		{
			int32_t bodyCount;
			in >> bodyCount;
			searchList.reserve(static_cast<size_t>(bodyCount));
			bool isNewSql = false;
			for (int32_t i = 0; i < bodyCount; i++) {
				OptionSet optionSet(alloc);
				decodeOptionPart(in, optionSet);

				util::XArray<uint8_t>* containerName =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
				decodeVarSizeBinaryData(in, *containerName);

				int32_t predicateIndex;
				in >> predicateIndex;

				if (predicateIndex < 0 ||
					static_cast<size_t>(predicateIndex) >=
					predicateList.size()) {
					TXN_THROW_DECODE_ERROR(GS_ERROR_TXN_DECODE_FAILED,
						"(predicateIndex=" << predicateIndex << ")");
				}

				util::LockGuard<util::Mutex> guard(
						partitionList_->partition(pId).mutex());

				const TransactionManager::ContextSource src(
					GET_MULTIPLE_CONTAINER_ROWS);
				TransactionContext& txn = transactionManager_->put(
					alloc, pId, TXN_EMPTY_CLIENTID, src, 0, 0);
				const FullContainerKey* containerKey =
					ALLOC_NEW(alloc) FullContainerKey(
						alloc, getKeyConstraint(CONTAINER_ATTR_ANY),
						containerName->data(), containerName->size());
				checkLoggedInDatabase(
					loginDbId, loginDbName,
					containerKey->getComponents(alloc).dbId_, specifiedDbName
					, isNewSql
				);

				KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(txn,
					*containerKey, getCaseSensitivity(optionSet).isContainerNameCaseSensitive());
				if (keyStoreValue.containerId_ == UNDEF_CONTAINERID) {
					continue;
				}
				DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
				const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
				DSInputMes input(alloc, DS_GET_CONTAINER_OBJECT, ANY_CONTAINER);
				StackAllocAutoPtr<DSContainerOutputMes> ret(alloc, NULL);
				{
					ret.set(static_cast<DSContainerOutputMes*>(
						ds->exec(&txn, &keyStoreValue, &input)));
				}
				BaseContainer* container = ret.get()->container_;
				if (container != NULL && !checkPrivilege(
					GET_MULTIPLE_CONTAINER_ROWS,
					userType, requestType, isSystemMode,
					optionSet.get<Options::ACCEPTABLE_FEATURE_VERSION>(),
					container->getAttribute(),
					optionSet.get<Options::CONTAINER_ATTRIBUTE>())) {
					GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
						"Forbidden Container Attribute");
				}

				if (container != NULL) {
					const TablePartitioningVersionId expectedVersion =
						optionSet.get<Options::SUB_CONTAINER_VERSION>();
					const TablePartitioningVersionId actualVersion =
						container->getTablePartitioningVersionId();
					if (expectedVersion >= 0 && expectedVersion < actualVersion) {
						GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DML_EXPIRED_SUB_CONTAINER_VERSION,
							"Sub container version expired ("
							"expected=" << expectedVersion <<
							", actual=" << actualVersion << ")");
					}
				}

				const ContainerId containerId = (container != NULL) ?
					container->getContainerId() : UNDEF_CONTAINERID;
				if (containerId == UNDEF_CONTAINERID) {
					continue;
				}

				SearchEntry entry(containerId, containerKey,
					predicateList[static_cast<size_t>(predicateIndex)]);

				searchList.push_back(entry);
			}
		}
	}
	catch (UserException& e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

template <typename S>
MultiGetHandler::RowKeyPredicate MultiGetHandler::decodePredicate(
	S& in, util::StackAllocator& alloc) {
	int8_t keyTypeOrdinal;
	in >> keyTypeOrdinal;
	int8_t predicateType;
	in >> predicateType;

	const bool composite = (keyTypeOrdinal == -1);
	ColumnType keyType = COLUMN_TYPE_ANY;
	if (!composite && !ValueProcessor::findColumnTypeByPrimitiveOrdinal(
			keyTypeOrdinal, false, false, keyType)) {
		TXN_THROW_DECODE_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
	}

	if (predicateType != PREDICATE_TYPE_RANGE && predicateType != PREDICATE_TYPE_DISTINCT) {
		TXN_THROW_DECODE_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
	}
	RowKeyPredicate predicate = {
			keyType, static_cast<RowKeyPredicateType>(predicateType), {0, 0}, 0, NULL, NULL, NULL };

	if (composite) {
		int32_t columnCount;
		in >> columnCount;

		RowKeyColumnTypeList* columnTypes =
			ALLOC_NEW(alloc) util::XArray<ColumnType>(alloc);
		columnTypes->reserve(static_cast<size_t>(columnCount));

		for (int32_t i = 0; i < columnCount; i++) {
			int8_t typeOrdinal;
			in >> typeOrdinal;

			ColumnType columnType;
			if (!ValueProcessor::findColumnTypeByPrimitiveOrdinal(
					typeOrdinal, false, false, columnType)) {
				TXN_THROW_DECODE_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
			}
			columnTypes->push_back(columnType);
		}
		predicate.compositeColumnTypes_ = columnTypes;
		if (predicateType == PREDICATE_TYPE_RANGE) {
			for (size_t i = 0; i < 2; i++) {
				in >> predicate.rangeFlags_[i];
			}
		}
		uint32_t bytesSize;
		in >> bytesSize;

		predicate.compositeKeys_ =
			ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
		predicate.compositeKeys_->resize(
			bytesSize - sizeof(predicate.rowCount_));

		in >> predicate.rowCount_;
		in.readAll(
			predicate.compositeKeys_->data(), predicate.compositeKeys_->size());
	}
	else {
		const MessageRowKeyCoder keyCoder(keyType);
		if (predicateType == PREDICATE_TYPE_RANGE) {
			RowKeyDataList* startFinishKeys =
				ALLOC_NEW(alloc) util::XArray<RowKeyData*>(alloc);

			for (size_t i = 0; i < 2; i++) {
				in >> predicate.rangeFlags_[i];

				if (predicate.rangeFlags_[i]) {
					RowKeyData* key = ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
					keyCoder.decode(in, *key);
					startFinishKeys->push_back(key);
				}
				else {
					startFinishKeys->push_back(NULL);
				}
			}
			predicate.keys_ = startFinishKeys;
		}
		else {
			int32_t keyCount;
			in >> keyCount;

			RowKeyDataList* distinctKeys =
				ALLOC_NEW(alloc) util::XArray<RowKeyData*>(alloc);
			distinctKeys->reserve(static_cast<size_t>(keyCount));

			for (int32_t i = 0; i < keyCount; i++) {
				RowKeyData* key = ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
				keyCoder.decode(in, *key);
				distinctKeys->push_back(key);
			}
			predicate.keys_ = distinctKeys;
		}
	}

	return predicate;
}

void MultiGetHandler::encodeEntry(
	const FullContainerKey& containerKey,
	ContainerId containerId, const SchemaMap& schemaMap, ResultSet& rs,
	EventByteOutStream& out) {
	encodeContainerKey(out, containerKey);

	LocalSchemaId schemaId;
	{
		SchemaMap::const_iterator it = schemaMap.find(containerId);
		if (it == schemaMap.end()) {
			assert(false);
			GS_THROW_SYSTEM_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
		}

		schemaId = it->second;
	}
	out << schemaId;

	out << rs.getResultNum();

	if (rs.getResultNum() > 0) {
		const util::XArray<uint8_t>* rowDataFixedPart =
			rs.getRowDataFixedPartBuffer();
		const util::XArray<uint8_t>* rowDataVarPart = rs.getRowDataVarPartBuffer();

		out.writeAll(rowDataFixedPart->data(), rowDataFixedPart->size());
		out.writeAll(rowDataVarPart->data(), rowDataVarPart->size());
	}
}

/*!
	@brief Handler Operator
*/
void MultiQueryHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);

	try {
		ConnectionOption& connOption =
			ev.getSenderND().getUserData<ConnectionOption>();

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);

		QueryRequestList queryList(alloc);

		decodeMultiQuery(in, request, queryList);
		TRANSACTION_EVENT_START(ec, ev, transactionManager_, connOption, (ev.getSenderND().getId() < 0));

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkTransactionTimeout(
			emNow, ev.getQueuedMonotonicTime(),
			request.fixed_.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());

		int32_t fetchByteSize =
			request.optional_.get<Options::FETCH_BYTES_SIZE>();

		Event reply(ec, ev.getType(), ev.getPartitionId());

		EventByteOutStream out = encodeCommonPart(
			reply, request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS);
		encodeMultiSearchResultHead(
			out, static_cast<uint32_t>(queryList.size()));

		TxnLogManager txnLogMgr(transactionService_, ec, request.fixed_.pId_);
		Progress progress(alloc, getLockConflictStatus(ev, emNow, request), txnLogMgr);

		bool succeeded = true;
		{
			const util::StackAllocator::ConfigScope queryScope(alloc);
			alloc.setTotalSizeLimit(transactionService_->getWorkMemoryByteSizeLimit());

			for (size_t i = 0; i < queryList.size(); i++) {
				const QueryRequest& queryRequest = *(queryList[i]);

				const bool forUpdate =
					queryRequest.optionSet_.get<Options::FOR_UPDATE>();
				if (forUpdate) {
					GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_NOT_EXECUTABLE,
						"row lock (forUpdate) is not applicable");
				}

				checkConsistency(ev.getSenderND(), forUpdate);
				const PartitionRoleType partitionRole = forUpdate ?
					PROLE_OWNER :
					(PROLE_OWNER | PROLE_BACKUP);
				checkExecutable(
					request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);
				bool isTQL = (queryRequest.stmtType_ == QUERY_TQL) ? true : false;
				checkQueryOption(
					queryRequest.optionSet_,
					queryRequest.fetchOption_, queryRequest.isPartial_, isTQL);
				execute(
					ec, ev, request, fetchByteSize, queryRequest, out,
					progress);

				if (!progress.lastExceptionData_.empty() ||
					progress.lockConflicted_) {
					succeeded = false;
					break;
				}
			}
		}

		if (progress.lockConflicted_) {
			retryLockConflictedRequest(ec, ev, progress.lockConflictStatus_);
		}
		else if (succeeded) {
			ec.getEngine().send(reply, ev.getSenderND());
		}
		else {
			Event errorReply(ec, request.fixed_.stmtType_, request.fixed_.pId_);
			EventByteOutStream out = encodeCommonPart(
				errorReply, request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_ERROR);
			out.writeAll(
				progress.lastExceptionData_.data(),
				progress.lastExceptionData_.size());
			ec.getEngine().send(errorReply, ev.getSenderND());
		}

		if (succeeded) {
			transactionService_->incrementReadOperationCount(
				ev.getPartitionId());
			transactionService_->addRowReadCount(
				ev.getPartitionId(), progress.totalRowCount_);
		}
	}
	catch (std::exception& e) {
		handleWholeError(ec, alloc, ev, request, e);
	}
}

void MultiQueryHandler::execute(
	EventContext& ec, const Event& ev, const Request& request,
	int32_t& fetchByteSize, const QueryRequest& queryRequest,
	EventByteOutStream& replyOut, Progress& progress) {
	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	const ClientId clientId(request.fixed_.clientId_.uuid_, queryRequest.sessionId_);
	const TransactionManager::ContextSource src =
		createContextSource(request, queryRequest);

	const char8_t* queryType = getQueryTypeName(queryRequest.stmtType_);

	try {
		const util::StackAllocator::Scope scope(alloc);

		util::LockGuard<util::Mutex> guard(
				partitionList_->partition(request.fixed_.pId_).mutex());

		TransactionContext& txn = transactionManager_->put(
			alloc, request.fixed_.pId_, clientId, src, now, emNow);

		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		checkContainerExistence(keyStoreValue);
		DataStoreV4* ds = dynamic_cast<DataStoreV4*>(getDataStore(txn.getPartitionId(), keyStoreValue.storeType_));
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
		DSInputMes input(alloc, DS_GET_CONTAINER_OBJECT, ANY_CONTAINER);
		StackAllocAutoPtr<DSContainerOutputMes> ret(alloc, NULL);

		ret.set(static_cast<DSContainerOutputMes*>(
			ds->exec(&txn, &keyStoreValue, &input)));
		BaseContainer* container = ret.get()->container_;
		checkContainerSchemaVersion(
			container, queryRequest.schemaVersionId_);

		const FetchOption& fetchOption = queryRequest.fetchOption_;
		ResultSetOption queryOption;
		createResultSetOption(
			queryRequest.optionSet_, &fetchByteSize,
			queryRequest.isPartial_, queryRequest.partialQueryOption_,
			queryOption);
		ResultSet* rs = ds->getResultSetManager()->create(
			txn, txn.getContainerId(), UNDEF_SCHEMAVERSIONID, emNow,
			&queryOption);
		const ResultSetGuard rsGuard(txn, *ds, *rs);
		rs->setSchemaVersionId(container->getVersionId());
		if ( AUDIT_TRACE_CHECK ) {
			ConnectionOption& connOption =
				ev.getSenderND().getUserData<ConnectionOption>();
			if ( queryRequest.stmtType_ == QUERY_TQL ){
				AUDIT_TRACE_INFO_OPERATOR(getAuditContainerName(alloc,
					getKeyDataStore(txn.getPartitionId())->getKey(alloc, txn.getContainerId()))
					 + std::string(":") + queryRequest.query_.tqlQuery_->c_str())
			}
			else{
				AUDIT_TRACE_INFO_OPERATOR(getAuditContainerName(alloc,
					 getKeyDataStore(txn.getPartitionId())->getKey(alloc, txn.getContainerId())))
			}
		}
		switch (queryRequest.stmtType_) {
		case QUERY_TQL: {
			const util::String* query = queryRequest.query_.tqlQuery_;
			FullContainerKey* containerKey = NULL;
			const QueryContainerKey* distInfo =
				queryRequest.optionSet_.get<Options::DIST_QUERY>();
			if (distInfo != NULL && distInfo->enabled_) {
				const TablePartitioningVersionId expectedVersion =
					queryRequest.optionSet_.get<Options::SUB_CONTAINER_VERSION>();
				const TablePartitioningVersionId actualVersion =
					container->getTablePartitioningVersionId();
				if (expectedVersion >= 0 && expectedVersion < actualVersion) {
					GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DML_EXPIRED_SUB_CONTAINER_VERSION,
						"Sub container version expired ("
						"expected=" << expectedVersion <<
						", actual=" << actualVersion << ")");
				}

				const util::XArray<uint8_t>& keyBinary =
					distInfo->containerKeyBinary_;
				containerKey = ALLOC_NEW(alloc) FullContainerKey(
					alloc, getKeyConstraint(queryRequest.optionSet_),
					keyBinary.data(), keyBinary.size());
			}
			QueryProcessor::executeTQL(
				txn, *container, fetchOption.limit_,
				TQLInfo(
					request.optional_.get<Options::DB_NAME>(),
					containerKey, query->c_str()), *rs);


			QueryProcessor::fetch(txn, *container, 0, fetchOption.size_, rs);
		} break;
		case QUERY_COLLECTION_GEOMETRY_RELATED: {
			GeometryQuery* query = queryRequest.query_.geometryQuery_;
			QueryProcessor::searchGeometryRelated(txn, *static_cast<Collection*>(container),
				fetchOption.limit_, query->columnId_,
				static_cast<uint32_t>(query->intersection_.size()),
				query->intersection_.data(), query->operator_, *rs);


			QueryProcessor::fetch(txn, *container, 0, fetchOption.size_, rs);
		} break;
		case QUERY_COLLECTION_GEOMETRY_WITH_EXCLUSION: {
			GeometryQuery* query = queryRequest.query_.geometryQuery_;
			QueryProcessor::searchGeometry(txn, *static_cast<Collection*>(container), fetchOption.limit_,
				query->columnId_,
				static_cast<uint32_t>(query->intersection_.size()),
				query->intersection_.data(),
				static_cast<uint32_t>(query->disjoint_.size()),
				query->disjoint_.data(), *rs);


			QueryProcessor::fetch(txn, *container, 0, fetchOption.size_, rs);
		} break;
		case QUERY_TIME_SERIES_RANGE: {
			RangeQuery* query = queryRequest.query_.rangeQuery_;
			QueryProcessor::search(txn, *static_cast<TimeSeries*>(container), query->order_,
				fetchOption.limit_, query->start_, query->end_, *rs);


			QueryProcessor::fetch(txn, *container, 0, fetchOption.size_, rs);
		} break;
		case QUERY_TIME_SERIES_SAMPLING: {
			SamplingQuery* query = queryRequest.query_.samplingQuery_;
			QueryProcessor::sample(txn, *static_cast<TimeSeries*>(container), fetchOption.limit_,
				query->start_, query->end_, query->toSamplingOption(), *rs);


			QueryProcessor::fetch(txn, *container, 0, fetchOption.size_, rs);
		} break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNSUPPORTED,
				"Unsupported query (statement type=" <<
				queryRequest.stmtType_ << ")");
		}



		encodeSearchResult(alloc, replyOut, *rs);

		transactionManager_->update(txn, queryRequest.stmtId_);

		int32_t rsUseSize = rs->getSerializedSize();
		if (fetchByteSize > rsUseSize) {
			fetchByteSize -= rsUseSize;
		}
		else {
			fetchByteSize = 1;
		}

		progress.totalRowCount_ += rs->getFetchNum();
	}
	catch (std::exception& e) {
		do {
			if (!queryRequest.optionSet_.get<Options::SUB_CONTAINER_EXPIRABLE>()) {
				break;
			}

			try {
				throw;
			}
			catch (UserException& e2) {
				if (e2.getErrorCode() !=
					GS_ERROR_DS_CONTAINER_UNEXPECTEDLY_REMOVED &&
					e2.getErrorCode() !=
					GS_ERROR_DS_DS_CONTAINER_EXPIRED) {
					break;
				}
			}

			try {
				encodeEmptySearchResult(alloc, replyOut);
				progress.containerResult_.push_back(CONTAINER_RESULT_SUCCESS);
			}
			catch (std::exception& e2) {
				handleExecuteError(
					alloc, ev, request, EXECUTE_MULTIPLE_QUERIES,
					request.fixed_.pId_, clientId, src, progress, e2,
					queryType);
			}
			return;
		} while (false);

		handleExecuteError(
			alloc, ev, request, EXECUTE_MULTIPLE_QUERIES,
			request.fixed_.pId_, clientId, src, progress, e, queryType);
	}
}

TransactionManager::ContextSource MultiQueryHandler::createContextSource(
	const Request& request, const QueryRequest& queryRequest) {
	return TransactionManager::ContextSource(
		request.fixed_.stmtType_, queryRequest.stmtId_,
		queryRequest.containerId_,
		queryRequest.optionSet_.get<Options::TXN_TIMEOUT_INTERVAL>(),
		queryRequest.getMode_,
		queryRequest.txnMode_,
		false,
		request.fixed_.cxtSrc_.storeMemoryAgingSwapRate_,
		request.fixed_.cxtSrc_.timeZone_);
}

template<typename S>
void MultiQueryHandler::decodeMultiQuery(
	S& in, const Request& request,
	util::XArray<const QueryRequest*>& queryList) {
	util::StackAllocator& alloc = *queryList.get_allocator().base();

	try {
		int32_t entryCount;
		in >> entryCount;
		queryList.reserve(static_cast<size_t>(entryCount));

		for (int32_t i = 0; i < entryCount; i++) {
			QueryRequest* subRequest = ALLOC_NEW(alloc) QueryRequest(alloc);

			int32_t stmtType;
			in >> stmtType;
			subRequest->stmtType_ = static_cast<EventType>(stmtType);

			in >> subRequest->stmtId_;
			in >> subRequest->containerId_;
			in >> subRequest->sessionId_;
			in >> subRequest->schemaVersionId_;

			in >> subRequest->getMode_;
			in >> subRequest->txnMode_;

			decodeContainerOptionPart(in, request.fixed_, subRequest->optionSet_);

			decodeFetchOption<S>(in, subRequest->fetchOption_);
			decodePartialQueryOption<S>(
				in, alloc, subRequest->isPartial_,
				subRequest->partialQueryOption_);

			switch (subRequest->stmtType_) {
			case QUERY_TQL:
				subRequest->query_.tqlQuery_ =
					ALLOC_NEW(alloc) util::String(alloc);
				in >> *subRequest->query_.tqlQuery_;
				break;
			case QUERY_COLLECTION_GEOMETRY_RELATED: {
				GeometryQuery* query = ALLOC_NEW(alloc) GeometryQuery(alloc);

				decodeGeometryRelatedQuery(in, *query);

				subRequest->query_.geometryQuery_ = query;
			} break;
			case QUERY_COLLECTION_GEOMETRY_WITH_EXCLUSION: {
				GeometryQuery* query = ALLOC_NEW(alloc) GeometryQuery(alloc);

				decodeGeometryWithExclusionQuery(in, *query);

				subRequest->query_.geometryQuery_ = query;
			} break;
			case QUERY_TIME_SERIES_RANGE: {
				RangeQuery* query = ALLOC_NEW(alloc) RangeQuery();

				decodeRangeQuery(in, *query);

				subRequest->query_.rangeQuery_ = query;
			} break;
			case QUERY_TIME_SERIES_SAMPLING: {
				SamplingQuery* query = ALLOC_NEW(alloc) SamplingQuery(alloc);

				decodeSamplingQuery(in, *query);

				subRequest->query_.samplingQuery_ = query;
			} break;
			default:
				TXN_THROW_DECODE_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
			}

			queryList.push_back(subRequest);
		}
	}
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

void MultiQueryHandler::encodeMultiSearchResultHead(
	EventByteOutStream& out, uint32_t queryCount) {
	try {
		out << queryCount;
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void MultiQueryHandler::encodeEmptySearchResult(
		util::StackAllocator& alloc, EventByteOutStream& out) {
	UNUSED_VARIABLE(alloc);
	try {
		int64_t allSize = 0;
		int32_t entryNum = 0;
		allSize += sizeof(int32_t);

		out << allSize;
		out << entryNum;
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void MultiQueryHandler::encodeSearchResult(
	util::StackAllocator& alloc,
	EventByteOutStream& out, const ResultSet& rs) {
	try {
		const size_t allPos = out.base().position();
		int64_t allSize = 0;
		out << allSize;

		switch (rs.getResultType()) {
		case RESULT_ROWSET: {
			PartialQueryOption partialQueryOption(alloc);
			rs.getQueryOption().encodePartialQueryOption(alloc, partialQueryOption);

			const size_t entryNumPos = out.base().position();
			int32_t entryNum = 0;
			out << entryNum;

			allSize += sizeof(int32_t);

			if (!partialQueryOption.empty()) {
				encodeEnumData<EventByteOutStream, QueryResponseType>(out, PARTIAL_EXECUTION_STATE);

				const size_t entrySizePos = out.base().position();
				int32_t entrySize = 0;
				out << entrySize;

				encodeBooleanData<EventByteOutStream>(out, true);
				entrySize += static_cast<int32_t>(sizeof(int8_t));

				int32_t partialEntryNum = static_cast<int32_t>(partialQueryOption.size());
				out << partialEntryNum;
				entrySize += static_cast<int32_t>(sizeof(int32_t));

				PartialQueryOption::const_iterator itr;
				for (itr = partialQueryOption.begin(); itr != partialQueryOption.end(); itr++) {
					encodeIntData<EventByteOutStream, int8_t>(out, itr->first);
					entrySize += static_cast<int32_t>(sizeof(int8_t));

					int32_t partialEntrySize =
							static_cast<int32_t>(itr->second->size());
					out << partialEntrySize;
					entrySize += static_cast<int32_t>(sizeof(int32_t));
					encodeBinaryData< EventByteOutStream>(out, itr->second->data(), itr->second->size());
					entrySize += static_cast<int32_t>(itr->second->size());
				}
				const size_t entryLastPos = out.base().position();
				out.base().position(entrySizePos);
				out << entrySize;
				out.base().position(entryLastPos);
				entryNum++;

				allSize += sizeof(int8_t) + sizeof(int32_t) + entrySize;
			}

			entryNum += encodeDistributedResult<EventByteOutStream>(out, rs, &allSize);

			{
				encodeEnumData<EventByteOutStream, QueryResponseType>(out, ROW_SET);

				const int32_t entrySize = static_cast<int32_t>(
						sizeof(ResultSize) +
						rs.getFixedOffsetSize() +
						rs.getVarOffsetSize());
				out << entrySize;

				out << rs.getFetchNum();
				out << std::make_pair(rs.getFixedStartData(),
					static_cast<size_t>(rs.getFixedOffsetSize()));
				out << std::make_pair(rs.getVarStartData(),
					static_cast<size_t>(rs.getVarOffsetSize()));
				entryNum++;

				allSize += sizeof(int8_t) + sizeof(int32_t) + entrySize;
			}

			const size_t lastPos = out.base().position();
			out.base().position(entryNumPos);
			out << entryNum;
			out.base().position(lastPos);
		} break;
		case RESULT_EXPLAIN: {
			const size_t entryNumPos = out.base().position();
			int32_t entryNum = 0;
			out << entryNum;

			entryNum += encodeDistributedResult<EventByteOutStream>(out, rs, &allSize);

			allSize += sizeof(int32_t);
			{
				encodeEnumData<EventByteOutStream, QueryResponseType>(out, QUERY_ANALYSIS);
				const int32_t entrySize = static_cast<int32_t>(
						sizeof(ResultSize) +
						rs.getFixedOffsetSize() +
						rs.getVarOffsetSize());
				out << entrySize;

				out << rs.getResultNum();
				out << std::make_pair(rs.getFixedStartData(),
					static_cast<size_t>(rs.getFixedOffsetSize()));
				out << std::make_pair(rs.getVarStartData(),
					static_cast<size_t>(rs.getVarOffsetSize()));
				entryNum++;

				allSize += sizeof(int8_t) + sizeof(int32_t) + entrySize;
			}

			const size_t lastPos = out.base().position();
			out.base().position(entryNumPos);
			out << entryNum;
			out.base().position(lastPos);
		} break;
		case RESULT_AGGREGATE: {
			int32_t entryNum = 1;
			out << entryNum;

			allSize += sizeof(int32_t);
			{
				encodeEnumData<EventByteOutStream, QueryResponseType>(out, AGGREGATION);
				const util::XArray<uint8_t>* rowDataFixedPart =
					rs.getRowDataFixedPartBuffer();
				const int32_t entrySize = static_cast<int32_t>(
						sizeof(ResultSize) + rowDataFixedPart->size());
				out << entrySize;

				out << rs.getResultNum();
				encodeBinaryData<EventByteOutStream>(
					out, rowDataFixedPart->data(), rowDataFixedPart->size());

				allSize += sizeof(int8_t) + sizeof(int32_t) + entrySize;
			}
		} break;
		case PARTIAL_RESULT_ROWSET: {
			assert(rs.getId() != UNDEF_RESULTSETID);
			int32_t entryNum = 2;
			out << entryNum;

			allSize += sizeof(int32_t);
			{
				encodeEnumData<EventByteOutStream, QueryResponseType>(out, PARTIAL_FETCH_STATE);
				int32_t entrySize = sizeof(ResultSize) + sizeof(ResultSetId);
				out << entrySize;

				out << rs.getResultNum();
				out << rs.getId();

				allSize += sizeof(int8_t) + sizeof(int32_t) + entrySize;
			}
			{
				encodeEnumData<EventByteOutStream, QueryResponseType>(out, ROW_SET);
				const int32_t entrySize = static_cast<int32_t>(
						sizeof(ResultSize) +
						rs.getFixedOffsetSize() +
						rs.getVarOffsetSize());
				out << entrySize;

				out << rs.getFetchNum();
				out << std::make_pair(rs.getFixedStartData(),
					static_cast<size_t>(rs.getFixedOffsetSize()));
				out << std::make_pair(rs.getVarStartData(),
					static_cast<size_t>(rs.getVarOffsetSize()));

				allSize += sizeof(int8_t) + sizeof(int32_t) + entrySize;
			}
		} break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_RESULT_TYPE_INVALID,
					"(resultType=" << static_cast<int32_t>(rs.getResultType()) << ")");
		}

		const size_t lastPos = out.base().position();
		out.base().position(allPos);
		out << allSize;

		out.base().position(lastPos);
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

const char8_t* MultiQueryHandler::getQueryTypeName(EventType queryStmtType) {
	switch (queryStmtType) {
	case QUERY_TQL:
		return "tql";
	case QUERY_COLLECTION_GEOMETRY_RELATED:
		return "geometryRelated";
	case QUERY_COLLECTION_GEOMETRY_WITH_EXCLUSION:
		return "geometryWithExlusion";
	case QUERY_TIME_SERIES_RANGE:
		return "timeRange";
	case QUERY_TIME_SERIES_SAMPLING:
		return "timeSampling";
	default:
		return "unknown";
	}
}


void CheckTimeoutHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	if (!clusterManager_->checkRecoveryCompleted()) {
		return;
	}

	util::StackAllocator& alloc = ec.getAllocator();
	
	if (!this->clusterManager_->checkRecoveryCompleted()) {
		return;
	}

	try {

		if (isNewSQL_) {
			checkAuthenticationTimeout(ec);
			return;
		}

		const PartitionGroupConfig& pgConfig =
			transactionManager_->getPartitionGroupConfig();
		PartitionId startPId = pgConfig.getGroupBeginPartitionId(static_cast<PartitionGroupId>(ec.getWorkerId()));
		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(startPId).mutex());
		
		{
			const util::StackAllocator::Scope scope(alloc);

			util::XArray<bool> checkPartitionFlagList(alloc);
			if (isTransactionTimeoutCheckEnabled(
				ec.getWorkerId(), checkPartitionFlagList)) {

				checkRequestTimeout(ec, checkPartitionFlagList);
				checkTransactionTimeout(ec, checkPartitionFlagList);
				checkKeepaliveTimeout(ec, checkPartitionFlagList);
			}
		}

		checkReplicationTimeout(ec);
		checkAuthenticationTimeout(ec);
		checkResultSetTimeout(ec);
	}
	catch (std::exception& e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			"resource timeout check failed (nd="
			<< ev.getSenderND() << ", reason=" << GS_EXCEPTION_MESSAGE(e)
			<< ")");

		if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
			clusterService_->setError(ec, &e);
		}
	}
}

void CheckTimeoutHandler::checkReplicationTimeout(EventContext& ec) {
	util::StackAllocator& alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
	const PartitionGroupId pgId = ec.getWorkerId();

	util::StackAllocator::Scope scope(alloc);

	size_t timeoutResourceCount = 0;

	try {
		util::XArray<PartitionId> pIds(alloc);
		util::XArray<ReplicationId> timeoutResourceIds(alloc);

		transactionManager_->getReplicationTimeoutContextId(
			pgId, emNow, pIds, timeoutResourceIds);
		timeoutResourceCount = timeoutResourceIds.size();

		for (size_t i = 0; i < timeoutResourceIds.size(); i++) {
			util::StackAllocator::Scope innerScope(alloc);

			try {
				ReplicationContext& replContext =
					transactionManager_->get(pIds[i], timeoutResourceIds[i]);


				if (replContext.getTaskStatus() == ReplicationContext::TASK_CONTINUE) {
					continueEvent(ec, alloc, TXN_STATEMENT_SUCCESS_BUT_REPL_TIMEOUT, replContext);
					transactionManager_->remove(pIds[i], timeoutResourceIds[i]);
				}
				else {
					replySuccess(ec, alloc, TXN_STATEMENT_SUCCESS_BUT_REPL_TIMEOUT,
						replContext);
					transactionManager_->remove(pIds[i], timeoutResourceIds[i]);
				}
			}
			catch (ContextNotFoundException& e) {
				UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
					"(pId=" << pIds[i]
					<< ", contextId=" << timeoutResourceIds[i]
					<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
			}
		}

		if (timeoutResourceCount > 0) {
			GS_TRACE_WARNING(REPLICATION_TIMEOUT,
				GS_TRACE_TXN_REPLICATION_TIMEOUT,
				"(pgId=" << pgId << ", timeoutResourceCount="
				<< timeoutResourceCount << ")");
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e,
			"(pId=" << pgId << ", timeoutResourceCount=" << timeoutResourceCount
			<< ")");
	}
}

void CheckTimeoutHandler::checkTransactionTimeout(
	EventContext& ec, const util::XArray<bool>& checkPartitionFlagList) {

	util::StackAllocator& alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
	const PartitionGroupId pgId = ec.getWorkerId();

	util::StackAllocator::Scope scope(alloc);

	size_t timeoutResourceCount = 0;

	try {
		util::XArray<PartitionId> pIds(alloc);
		util::XArray<ClientId> timeoutResourceIds(alloc);

		transactionManager_->getTransactionTimeoutContextId(alloc, pgId, emNow,
			checkPartitionFlagList, pIds, timeoutResourceIds);
		timeoutResourceCount = timeoutResourceIds.size();

		for (size_t i = 0; i < timeoutResourceIds.size(); i++) {
			util::StackAllocator::Scope innerScope(alloc);

			try {
				TransactionContext& txn = transactionManager_->get(
					alloc, pIds[i], timeoutResourceIds[i]);

				if (partitionTable_->isOwner(pIds[i])) {
					util::XArray<uint8_t>* log = abortOnError(txn);
					if (log != NULL) {
						util::XArray<const util::XArray<uint8_t>*>
							logRecordList(alloc);
						logRecordList.push_back(log);

						Request dummyRequest(alloc, FixedRequest::Source(
							UNDEF_PARTITIONID, UNDEF_EVENT_TYPE));

						Response response(alloc);
						executeReplication(dummyRequest, ec, alloc,
							NodeDescriptor::EMPTY_ND, txn,
							TXN_COLLECT_TIMEOUT_RESOURCE, UNDEF_STATEMENTID,
							transactionManager_->getReplicationMode(), NULL, 0,
							logRecordList.data(), logRecordList.size(),
							NULL);
					}
				}
				else {
				}
			}
			catch (ContextNotFoundException& e) {
				UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
					"(pId=" << pIds[i]
					<< ", contextId=" << timeoutResourceIds[i]
					<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
			}
		}

		if (timeoutResourceCount > 0) {
			util::NormalOStringStream ss;
			ss << "{";
			for (size_t i = 0;
				i < timeoutResourceIds.size() && i < MAX_OUTPUT_COUNT; i++) {
				ss << timeoutResourceIds[i] << ", ";
			}
			ss << "}";
			GS_TRACE_WARNING(TRANSACTION_TIMEOUT,
				GS_TRACE_TXN_TRANSACTION_TIMEOUT,
				"(pgId=" << pgId
				<< ", timeoutResourceCount=" << timeoutResourceCount
				<< ", clientId=" << ss.str() << ")");
		}

		const PartitionId beginPId =
			transactionManager_->getPartitionGroupConfig()
			.getGroupBeginPartitionId(pgId);
		const PartitionId endPId =
			transactionManager_->getPartitionGroupConfig()
			.getGroupEndPartitionId(pgId);
		for (PartitionId pId = beginPId; pId < endPId; pId++) {
			ec.setPendingPartitionChanged(pId);
		}
	}
	catch (DenyException&) {
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "(pgId=" << pgId
			<< ", timeoutResourceCount="
			<< timeoutResourceCount << ")");
	}
}

void CheckTimeoutHandler::checkRequestTimeout(
	EventContext& ec, const util::XArray<bool>& checkPartitionFlagList) {

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime& now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
	const PartitionGroupId pgId = ec.getWorkerId();
	util::TimeZone timeZone;

	util::StackAllocator::Scope scope(alloc);

	size_t timeoutResourceCount = 0;

	try {
		util::XArray<PartitionId> pIds(alloc);
		util::XArray<ClientId> timeoutResourceIds(alloc);

		transactionManager_->getRequestTimeoutContextId(alloc, pgId, emNow,
			checkPartitionFlagList, pIds, timeoutResourceIds);
		timeoutResourceCount = timeoutResourceIds.size();

		for (size_t i = 0; i < timeoutResourceIds.size(); i++) {
			util::StackAllocator::Scope innerScope(alloc);

			try {
				if (partitionTable_->isOwner(pIds[i])) {
					TransactionContext& txn = transactionManager_->get(
						alloc, pIds[i], timeoutResourceIds[i]);

					util::XArray<ClientId> closedResourceIds(alloc);
					util::XArray<const util::XArray<uint8_t>*> logRecordList(
						alloc);

					util::XArray<uint8_t>* log = abortOnError(txn);
					if (log != NULL) {
						logRecordList.push_back(log);

					}

					const TransactionManager::ContextSource src(
						TXN_COLLECT_TIMEOUT_RESOURCE, txn.getLastStatementId(),
						txn.getContainerId(),
						txn.getTransactionTimeoutInterval(),
						TransactionManager::AUTO,
						TransactionManager::AUTO_COMMIT,
						false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE,
						timeZone);

					transactionManager_->remove(pIds[i], timeoutResourceIds[i]);
					closedResourceIds.push_back(timeoutResourceIds[i]);

					txn = transactionManager_->put(
						alloc, pIds[i], timeoutResourceIds[i], src, now, emNow);
					Request dummyRequest(alloc, FixedRequest::Source(
						UNDEF_PARTITIONID, UNDEF_EVENT_TYPE));

					Response response(alloc);
					executeReplication(dummyRequest, ec, alloc,
						NodeDescriptor::EMPTY_ND, txn,
						TXN_COLLECT_TIMEOUT_RESOURCE, UNDEF_STATEMENTID,
						transactionManager_->getReplicationMode(),
						closedResourceIds.data(), closedResourceIds.size(),
						logRecordList.data(), logRecordList.size(), NULL);

				}
				else {
					transactionManager_->remove(pIds[i], timeoutResourceIds[i]);
				}
			}
			catch (ContextNotFoundException& e) {
				UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
					"(pId=" << pIds[i]
					<< ", contextId=" << timeoutResourceIds[i]
					<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
			}
		}

		if (timeoutResourceCount > 0) {
			util::NormalOStringStream ss;
			ss << "{";
			for (size_t i = 0;
				i < timeoutResourceIds.size() && i < MAX_OUTPUT_COUNT; i++) {
				ss << timeoutResourceIds[i] << ", ";
			}
			ss << "}";
			GS_TRACE_WARNING(SESSION_TIMEOUT, GS_TRACE_TXN_SESSION_TIMEOUT,
				"(pgId=" << pgId
				<< ", timeoutResourceCount=" << timeoutResourceCount
				<< ", clientId=" << ss.str() << ")");
		}

		const PartitionId beginPId =
			transactionManager_->getPartitionGroupConfig()
			.getGroupBeginPartitionId(pgId);
		const PartitionId endPId =
			transactionManager_->getPartitionGroupConfig()
			.getGroupEndPartitionId(pgId);
		for (PartitionId pId = beginPId; pId < endPId; pId++) {
			ec.setPendingPartitionChanged(pId);
		}
	}
	catch (DenyException&) {
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "(pgId=" << pgId
			<< ", timeoutResourceCount="
			<< timeoutResourceCount << ")");
	}
}

void CheckTimeoutHandler::checkKeepaliveTimeout(
	EventContext& ec, const util::XArray<bool>& checkPartitionFlagList) {

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime& now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
	const PartitionGroupId pgId = ec.getWorkerId();
	util::TimeZone timeZone;

	util::StackAllocator::Scope scope(alloc);

	size_t timeoutResourceCount = 0;

	try {
		util::XArray<PartitionId> pIds(alloc);
		util::XArray<ClientId> timeoutResourceIds(alloc);

		transactionManager_->getKeepaliveTimeoutContextId(alloc, pgId, emNow,
			checkPartitionFlagList, pIds, timeoutResourceIds);
		timeoutResourceCount = timeoutResourceIds.size();

		for (size_t i = 0; i < timeoutResourceIds.size(); i++) {
			util::StackAllocator::Scope innerScope(alloc);

			try {
				if (partitionTable_->isOwner(pIds[i])) {
					TransactionContext& txn = transactionManager_->get(
						alloc, pIds[i], timeoutResourceIds[i]);

					util::XArray<ClientId> closedResourceIds(alloc);
					util::XArray<const util::XArray<uint8_t>*> logRecordList(
						alloc);

					util::XArray<uint8_t>* log = abortOnError(txn);
					if (log != NULL) {
						logRecordList.push_back(log);

					}

					const TransactionManager::ContextSource src(
						TXN_COLLECT_TIMEOUT_RESOURCE, txn.getLastStatementId(),
						txn.getContainerId(),
						txn.getTransactionTimeoutInterval(),
						TransactionManager::AUTO,
						TransactionManager::AUTO_COMMIT,
						false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE,
						timeZone);

					transactionManager_->remove(pIds[i], timeoutResourceIds[i]);
					closedResourceIds.push_back(timeoutResourceIds[i]);

					txn = transactionManager_->put(
						alloc, pIds[i], timeoutResourceIds[i], src, now, emNow);
					Request dummyRequest(alloc, FixedRequest::Source(
						UNDEF_PARTITIONID, UNDEF_EVENT_TYPE));

					Response response(alloc);
					executeReplication(dummyRequest, ec, alloc,
						NodeDescriptor::EMPTY_ND, txn,
						TXN_COLLECT_TIMEOUT_RESOURCE, UNDEF_STATEMENTID,
						transactionManager_->getReplicationMode(),
						closedResourceIds.data(), closedResourceIds.size(),
						logRecordList.data(), logRecordList.size(), NULL);
					transactionService_->incrementAbortDDLCount(pIds[i]);
				}
				else {
					transactionManager_->remove(pIds[i], timeoutResourceIds[i]);
				}
			}
			catch (ContextNotFoundException& e) {
				UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
					"(pId=" << pIds[i]
					<< ", contextId=" << timeoutResourceIds[i]
					<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
			}
		}

		if (timeoutResourceCount > 0) {
			util::NormalOStringStream ss;
			ss << "{";
			for (size_t i = 0;
				i < timeoutResourceIds.size() && i < MAX_OUTPUT_COUNT; i++) {
				ss << timeoutResourceIds[i] << ", ";
			}
			ss << "}";
			GS_TRACE_WARNING(TRANSACTION_TIMEOUT,
				GS_TRACE_TXN_KEEPALIVE_TIMEOUT,
				"(pgId=" << pgId
				<< ", timeoutResourceCount=" << timeoutResourceCount
				<< ", clientId=" << ss.str() << ")");
		}

		const PartitionId beginPId =
			transactionManager_->getPartitionGroupConfig()
			.getGroupBeginPartitionId(pgId);
		const PartitionId endPId =
			transactionManager_->getPartitionGroupConfig()
			.getGroupEndPartitionId(pgId);
		for (PartitionId pId = beginPId; pId < endPId; pId++) {
			ec.setPendingPartitionChanged(pId);
		}
	}
	catch (DenyException&) {
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "(pgId=" << pgId
			<< ", timeoutResourceCount="
			<< timeoutResourceCount << ")");
	}
}

void CheckTimeoutHandler::checkNoExpireTransaction(util::StackAllocator& alloc, PartitionId pId) {

	util::TimeZone timeZone;
	util::StackAllocator::Scope scope(alloc);
	size_t noExpireResourceCount = 0;

	try {
		util::XArray<ClientId> noExpireResourceIds(alloc);

		transactionManager_->getNoExpireTransactionContextId(alloc, pId, noExpireResourceIds);
		noExpireResourceCount = noExpireResourceIds.size();

		for (size_t i = 0; i < noExpireResourceIds.size(); i++) {
			util::StackAllocator::Scope innerScope(alloc);

			try {
				TransactionContext& txn = transactionManager_->get(
					alloc, pId, noExpireResourceIds[i]);

				util::XArray<ClientId> closedResourceIds(alloc);
				util::XArray<const util::XArray<uint8_t>*> logRecordList(
					alloc);

				util::XArray<uint8_t>* log = abortOnError(txn);
				if (log != NULL) {
					logRecordList.push_back(log);

				}

				const TransactionManager::ContextSource src(
					TXN_COLLECT_TIMEOUT_RESOURCE, txn.getLastStatementId(),
					txn.getContainerId(),
					txn.getTransactionTimeoutInterval(),
					TransactionManager::AUTO,
					TransactionManager::AUTO_COMMIT,
					false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE,
					timeZone);

				transactionManager_->remove(pId, noExpireResourceIds[i]);
				closedResourceIds.push_back(noExpireResourceIds[i]);

				transactionService_->incrementAbortDDLCount(pId);
			}
			catch (ContextNotFoundException& e) {
				UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
					"(pId=" << pId
					<< ", contextId=" << noExpireResourceIds[i]
					<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
			}
		}

		if (noExpireResourceCount > 0) {
			util::NormalOStringStream ss;
			ss << "{";
			for (size_t i = 0;
				i < noExpireResourceIds.size() && i < MAX_OUTPUT_COUNT; i++) {
				ss << noExpireResourceIds[i] << ", ";
			}
			ss << "}";
			GS_TRACE_WARNING(TRANSACTION_TIMEOUT,
				GS_TRACE_TXN_KEEPALIVE_TIMEOUT,
				"(pId=" << pId
				<< ", no expire noExpireResourceCount=" << noExpireResourceCount
				<< ", clientId=" << ss.str() << ")");
		}

	}
	catch (DenyException&) {
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "(pId=" << pId
			<< ", noExpireResourceCount="
			<< noExpireResourceCount << ")");
	}
}

void TransactionService::checkNoExpireTransaction(util::StackAllocator& alloc, PartitionId pId) {

	checkTimeoutHandler_.checkNoExpireTransaction(alloc, pId);

}

void CheckTimeoutHandler::checkResultSetTimeout(EventContext& ec) {
	util::StackAllocator& alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
	const PartitionGroupId pgId = ec.getWorkerId();

	util::StackAllocator::Scope scope(alloc);

	size_t timeoutResourceCount = 0;

	const PartitionGroupConfig& pgConfig =
		transactionManager_->getPartitionGroupConfig();
	PartitionId startPId = pgConfig.getGroupBeginPartitionId(pgId);
	const PartitionId endPId =
		pgConfig.getGroupEndPartitionId(pgId);

	try {
		transactionService_->getResultSetHolderManager().closeAll(
				alloc, pgId, *partitionList_);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}

	try {
		const util::DateTime now = ec.getHandlerStartTime();
		KeyDataStoreValue dummy;
		DSInputMes input(alloc, DS_CHECK_TIMEOUT_RESULT_SET, emNow);
		for (PartitionId currentPId = startPId; currentPId < endPId; currentPId++) {
			Partition* partition = &partitionList_->partition(currentPId);
			if (!partition->isActive()) {
				continue;
			}
			TransactionManager::ContextSource src;
			DataStoreV4* dataStore = getDataStore(currentPId);
			if (dataStore) {
				TransactionContext& txn =
					transactionManager_->put(alloc, currentPId,
						TXN_EMPTY_CLIENTID, src, now, emNow);
				dataStore->exec(&txn, &dummy, &input);
			}
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e,
			"(pId=" << pgId << ", timeoutResourceCount=" << timeoutResourceCount
			<< ")");
	}
}

bool CheckTimeoutHandler::isTransactionTimeoutCheckEnabled(
	PartitionGroupId pgId, util::XArray<bool>& checkPartitionFlagList) {
	const PartitionId beginPId =
		transactionManager_->getPartitionGroupConfig().getGroupBeginPartitionId(
			pgId);
	const PartitionId endPId =
		transactionManager_->getPartitionGroupConfig().getGroupEndPartitionId(
			pgId);

	bool existTarget = false;
	for (PartitionId pId = beginPId; pId < endPId; pId++) {
		const bool flag =
			transactionService_->isTransactionTimeoutCheckEnabled(pId);
		checkPartitionFlagList.push_back(flag);
		existTarget |= flag;
	}
	return existTarget;
}

DataStorePeriodicallyHandler::DataStorePeriodicallyHandler()
	: StatementHandler(), pIdCursor_(NULL) {
	pIdCursor_ = UTIL_NEW PartitionId[KeyDataStore::MAX_PARTITION_NUM];
	for (PartitionGroupId pgId = 0; pgId < KeyDataStore::MAX_PARTITION_NUM;
		pgId++) {
		pIdCursor_[pgId] = UNDEF_PARTITIONID;
	}

	currentLimit_ = 0;
	currentPos_ = 0;
	expiredCheckCount_ = 3600 * 6;
	expiredCounter_ = 0;
}

DataStorePeriodicallyHandler::~DataStorePeriodicallyHandler() {
	delete[] pIdCursor_;
	for (size_t pos = 0; pos < context_.size(); pos++) {
		delete context_[pos];
	}
}

DataStorePeriodicallyHandler::ExpiredContainerContext::ExpiredContainerContext(
	const PartitionGroupConfig& pgConfig, PartitionGroupId pgId) :
	status_(ExpiredContainerContext::INIT),
	pgId_(pgId),
	startPId_(pgConfig.getGroupBeginPartitionId(pgId)),
	endPId_(pgConfig.getGroupEndPartitionId(pgId)),
	currentPId_(startPId_), currentPos_(0), checkInterval_(1), limitCount_(1),
	interruptionInterval_(1), counter_(0) {
};

bool DataStorePeriodicallyHandler::ExpiredContainerContext::next(int64_t size) {

	if (size == 0 || size < limitCount_) {
		currentPId_++;
		currentPos_ = 0;
	}
	else {
		currentPos_ += size;
	}
	if (currentPId_ == endPId_) {
		status_ = INIT;
		currentPId_ = startPId_;
		currentPos_ = 0;
		return true;
	}
	return false;
}

int64_t DataStorePeriodicallyHandler::getContainerNameList(
		EventContext& ec, PartitionGroupId pgId,
		PartitionId pId, int64_t start, ResultSize limit,
		util::XArray<FullContainerKey>& containerNameList) {
	UNUSED_VARIABLE(pgId);

	Partition& partition = partitionList_->partition(pId);
	if (!partitionTable_->isOwner(pId)) {
		return 0;
	}
	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	util::LockGuard<util::Mutex> guard(partition.mutex());
	if (!partition.isActive()) {
		return 0;
	}
	TransactionManager::ContextSource src;
	TransactionContext& txn = transactionManager_->put(
		alloc, pId, TXN_EMPTY_CLIENTID, src, now, emNow);
	KeyDataStore::ContainerCondition validContainerCondition(alloc);
	validContainerCondition.insertAttribute(CONTAINER_ATTR_LARGE);
	
	KeyDataStore* keyStore = getKeyDataStore(txn.getPartitionId());
	keyStore->getContainerNameList(txn, start,
		limit, UNDEF_DBID, validContainerCondition,
		containerNameList);
	return containerNameList.size();
}

void DataStorePeriodicallyHandler::sendCheckDropContainerList(
	EventContext& ec, PartitionId pId, util::XArray<FullContainerKey>& containerNameList) {

	util::StackAllocator& alloc = ec.getAllocator();
	EventEngine& ee = ec.getEngine();
	for (size_t pos = 0; pos < containerNameList.size(); pos++) {
		util::StackAllocator::Scope scope(alloc);
		util::String keyStr(alloc);
		containerNameList[pos].toString(alloc, keyStr);
		Event requestEv(ec, UPDATE_CONTAINER_STATUS, pId);
		EventByteOutStream out = requestEv.getOutStream();
		StatementMessage::FixedRequest::Source source(pId, UPDATE_CONTAINER_STATUS);
		StatementMessage::FixedRequest request(source);
		request.cxtSrc_.stmtId_ = 0;
		request.encode(out);
		StatementMessage::OptionSet optionalRequest(alloc);
		optionalRequest.set<StatementMessage::Options::REPLY_PID>(pId);
		bool systemMode = true;
		optionalRequest.set<StatementMessage::Options::SYSTEM_MODE>(systemMode);
		bool forSync = true;
		optionalRequest.set<StatementMessage::Options::FOR_SYNC>(forSync);
		int32_t subContainerId = -1;
		optionalRequest.set<StatementMessage::Options::SUB_CONTAINER_ID>(subContainerId);
		optionalRequest.encode(out);
		encodeContainerKey(out, containerNameList[pos]);
		out << TABLE_PARTITIONING_CHECK_EXPIRED;
		int32_t bodySize = 0;
		out << bodySize;
		const NodeDescriptor& nd = ee.getSelfServerND();
		requestEv.setSenderND(nd);
		ee.add(requestEv);
	}
}

bool DataStorePeriodicallyHandler::checkAndDrop(EventContext& ec, ExpiredContainerContext& cxt) {

	util::StackAllocator& alloc = ec.getAllocator();
	if (clusterManager_->getStandbyInfo().isStandby()) {
		return false;
	}
	ExpiredContainerContext::Status status = cxt.getStatus();
	switch (status) {
	case ExpiredContainerContext::INIT:
		return cxt.checkCounter();
	case ExpiredContainerContext::EXECUTE:
	{
		util::StackAllocator::Scope scope(alloc);
		util::XArray<FullContainerKey> containerNameList(alloc);
		int64_t size = getContainerNameList(ec, cxt.pgId_, cxt.currentPId_,
			cxt.currentPos_, cxt.limitCount_, containerNameList);
		sendCheckDropContainerList(ec, cxt.currentPId_, containerNameList);

		if (cxt.next(size)) {
			return false;
		}
		if (cxt.checkInterruption()) {
			return false;
		}
		return true;
	}
	default:
		break;
	}
	return false;
}

void DataStorePeriodicallyHandler::setConcurrency(int64_t concurrency) {
	concurrency_ = static_cast<int32_t>(concurrency);
	expiredCounterList_.assign(concurrency, 0);
	startPosList_.assign(concurrency, 0);

	for (uint32_t pgId = 0; pgId < concurrency; pgId++) {
		ExpiredContainerContext *cxt = UTIL_NEW ExpiredContainerContext(
			transactionManager_->getPartitionGroupConfig(), pgId);
		context_.push_back(cxt);
	}
}

/*!
	@brief Handler Operator
*/
void DataStorePeriodicallyHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	try {
		if (clusterManager_->checkRecoveryCompleted()) {
			WATCHER_START;
			const PartitionGroupId pgId = ec.getWorkerId();
			const PartitionGroupConfig& pgConfig =
				transactionManager_->getPartitionGroupConfig();

			{
				int32_t checkInterval = dsConfig_->getCheckErasableExpiredInterval();
				int32_t interruptionInterval = 1000;
				int32_t limitCount = dsConfig_->getExpiredCheckCount();
				ExpiredContainerContext& context = getContext(pgId);
				context.prepare(checkInterval, limitCount, interruptionInterval);
				while (checkAndDrop(ec, context));
			}

			PartitionId startPId = pgConfig.getGroupBeginPartitionId(pgId);
			PartitionId endPId = pgConfig.getGroupEndPartitionId(pgId);

			if (!transactionService_->onBackgroundTask(pgId)) {
				for (PartitionId currentPId = startPId; currentPId < endPId; currentPId++) {
					if (partitionList_->partition(currentPId).isActive())  {
						util::LockGuard<util::Mutex> guard(
								partitionList_->partition(currentPId).mutex());
						DataStoreV4* ds = getDataStore(currentPId);
						util::StackAllocator& alloc = ec.getAllocator();
						const util::DateTime now = ec.getHandlerStartTime();
						const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
						TransactionManager::ContextSource src;
						TransactionContext& txn =
							transactionManager_->put(alloc, currentPId,
								TXN_EMPTY_CLIENTID, src, now, emNow);

						const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);
						KeyDataStoreValue dummy;
						DSInputMes input(alloc, DS_SEARCH_BACKGROUND_TASK);
						StackAllocAutoPtr<DSBackgroundTaskOutputMes> ret(alloc, static_cast<DSBackgroundTaskOutputMes*>(ds->exec(&txn, &dummy, &input)));
						BGTask nextTask = ret.get()->bgTask_;
						if (nextTask.bgId_ != UNDEF_BACKGROUND_ID) {
							Event continueEvent(ec, BACK_GROUND, currentPId);
							EventByteOutStream out = continueEvent.getOutStream();
							out << nextTask.bgId_;

							ec.getEngine().add(continueEvent);
							transactionService_->setBackgroundTask(pgId, true);
							break;
						}
					}
				}
			}

			{
				if (pIdCursor_[pgId] == UNDEF_PARTITIONID) {
					pIdCursor_[pgId] = pgConfig.getGroupBeginPartitionId(pgId);
				}
				uint64_t periodMaxScanCount = dsConfig_->getBatchScanNum();

				uint64_t count = 0;
				PartitionId startPId = pIdCursor_[pgId];
				while (count < periodMaxScanCount) {
					util::LockGuard<util::Mutex> guard(
						partitionList_->partition(pIdCursor_[pgId]).mutex());
					Partition* partition = &partitionList_->partition(pIdCursor_[pgId]);
					if (partition->isActive()) {
						DataStoreV4* ds = getDataStore(pIdCursor_[pgId]);
						Timestamp timestamp = ec.getHandlerStartTime().getUnixTime();

						util::StackAllocator& alloc = ec.getAllocator();
						const util::DateTime now = ec.getHandlerStartTime();
						const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
						TransactionManager::ContextSource src;
						TransactionContext& txn =
							transactionManager_->put(alloc, pIdCursor_[pgId],
								TXN_EMPTY_CLIENTID, src, now, emNow);
						KeyDataStoreValue dummy;
						DSInputMes input(alloc, DS_SCAN_CHUNK_GROUP, timestamp);
						StackAllocAutoPtr<DSScanChunkGroupOutputMes> ret(alloc, static_cast<DSScanChunkGroupOutputMes*>(ds->exec(&txn, &dummy, &input)));
						uint64_t scanNum = ret.get()->num_;
						count += scanNum;
					}
					else {
						count = periodMaxScanCount; 
					}
					pIdCursor_[pgId]++;
					if (pIdCursor_[pgId] ==
						pgConfig.getGroupEndPartitionId(pgId)) {
						pIdCursor_[pgId] =
							pgConfig.getGroupBeginPartitionId(pgId);
					}
					if (pIdCursor_[pgId] == startPId) {
						break;
					}
				}  
			}
			WATCHER_END_NORMAL(CHUNK_EXPIRE_PERIODICALLY, pgId);
		}
	}
	catch (UserException& e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			"(nd=" << ev.getSenderND() << ", reason=" << GS_EXCEPTION_MESSAGE(e)
			<< ")");

		if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
			clusterService_->setError(ec, &e);
		}
	}
}

KeepLogHandler::KeepLogHandler() : StatementHandler(){
}

KeepLogHandler::~KeepLogHandler() {
}

void KeepLogHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);
	try {
		if (!clusterManager_->checkRecoveryCompleted()) {
			return;
		}
		const PartitionGroupId pgId = ec.getWorkerId();
		const PartitionGroupConfig& pgConfig = transactionManager_->getPartitionGroupConfig();
		PartitionId startPId = pgConfig.getGroupBeginPartitionId(pgId);
		PartitionId endPId = pgConfig.getGroupEndPartitionId(pgId);
		KeepLogManager& keepLogMgr = *resourceSet_->syncMgr_->getKeepLogManager();
		EventType eventType = ev.getType();
	}
	catch (UserException& e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			"(nd=" << ev.getSenderND() << ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
		if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
			clusterService_->setError(ec, &e);
		}
	}
}


AdjustStoreMemoryPeriodicallyHandler::AdjustStoreMemoryPeriodicallyHandler()
	: StatementHandler(), pIdCursor_(NULL) {
	pIdCursor_ = UTIL_NEW PartitionId[KeyDataStore::MAX_PARTITION_NUM];
	for (PartitionGroupId pgId = 0; pgId < KeyDataStore::MAX_PARTITION_NUM;
		pgId++) {
		pIdCursor_[pgId] = UNDEF_PARTITIONID;
	}
}

AdjustStoreMemoryPeriodicallyHandler::~AdjustStoreMemoryPeriodicallyHandler() {
	delete[] pIdCursor_;
}
/*!
	@brief Handler Operator
*/
void AdjustStoreMemoryPeriodicallyHandler::operator()(EventContext & ec, Event & ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	try {
		if (clusterManager_->checkRecoveryCompleted()) {
			WATCHER_START;
			const PartitionGroupId pgId = ec.getWorkerId();
			const PartitionGroupConfig& pgConfig =
				transactionManager_->getPartitionGroupConfig();
			const PartitionId groupBeginPId = pgConfig.getGroupBeginPartitionId(pgId);
			const PartitionId groupEndPId = pgConfig.getGroupEndPartitionId(pgId);

			if (pIdCursor_[pgId] == UNDEF_PARTITIONID) {
				pIdCursor_[pgId] = groupBeginPId;
			}

			if (pgId == 0) {
				partitionList_->interchangeableStoreMemory().calculate();
			}
			ChunkManager& chunkManager =
				partitionList_->partition(groupBeginPId).chunkManager();
			uint64_t newLimit = partitionList_->interchangeableStoreMemory().
				getChunkBufferLimit(pgId);
			uint64_t currentLimit = chunkManager.getCurrentLimit();
			if (newLimit != currentLimit) {
				util::LockGuard<util::Mutex> guard(
					partitionList_->partition(groupBeginPId).mutex());
				chunkManager.adjustStoreMemory(newLimit);  
			}
			pIdCursor_[pgId]++;
			if (pIdCursor_[pgId] == groupEndPId) {
				pIdCursor_[pgId] = groupBeginPId;
			}
			WATCHER_END_NORMAL(ADJUST_STORE_MEMORY_PERIODICALLY, pgId);
		}
	}
	catch (UserException& e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			"(nd=" << ev.getSenderND() << ", reason=" << GS_EXCEPTION_MESSAGE(e)
			<< ")");

		if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
			clusterService_->setError(ec, &e);
		}
	}
}

BackgroundHandler::BackgroundHandler()
	: StatementHandler() {
	lsnCounter_ = UTIL_NEW LogSequentialNumber[KeyDataStore::MAX_PARTITION_NUM];
	for (PartitionGroupId pgId = 0; pgId < KeyDataStore::MAX_PARTITION_NUM;
		pgId++) {
		lsnCounter_[pgId] = 0;
	}
}

BackgroundHandler::~BackgroundHandler() {
	delete[] lsnCounter_;
}

/*!
	@brief Handler Operator
*/
void BackgroundHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
	EVENT_START(ec, ev, transactionManager_, UNDEF_DBID, false);

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);
	BGTask currentTask;


	if (clusterManager_->isSystemError() || clusterManager_->isNormalShutdownCall()) {
		GS_TRACE_INFO(
			DATASTORE_BACKGROUND, GS_ERROR_DS_BACKGROUND_TASK_INVALID,
			"[BackgroundHandler::operator] stop BGService");
		return;
	}

	try {
		EventByteInStream in(ev.getInStream());
		decodeIntData<EventByteInStream, BackgroundId>(in, currentTask.bgId_);
		const PartitionGroupId pgId = ec.getWorkerId();
		if (clusterManager_->checkRecoveryCompleted()) {
			bool isFinished = true;

			const uint64_t startClock =
				util::Stopwatch::currentClock();
			{
				Partition* partition = &partitionList_->partition(request.fixed_.pId_);
				if (partition->isActive()) {

					util::LockGuard<util::Mutex> guard(
						partitionList_->partition(request.fixed_.pId_).mutex());

					DataStoreV4* ds = getDataStore(request.fixed_.pId_);
					TransactionContext& txn = transactionManager_->put(
						alloc, request.fixed_.pId_,
						TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
					const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);

					KeyDataStoreValue dummy;
					DSInputMes input(alloc, DS_EXECUTE_BACKGROUND_TASK, currentTask);
					StackAllocAutoPtr<DSBackgroundTaskOutputMes> ret(alloc, static_cast<DSBackgroundTaskOutputMes*>(ds->exec(&txn, &dummy, &input)));
					BGTask nextTask = ret.get()->bgTask_;
					isFinished = nextTask.bgId_ == UNDEF_BACKGROUND_ID;
					transactionService_->incrementBackgroundOperationCount(request.fixed_.pId_);
				}
			}
			const uint32_t lap = util::Stopwatch::clockToMillis(
				util::Stopwatch::currentClock() - startClock);
			int32_t waitTime = getWaitTime(ec, ev, lap);
			if (isFinished) {
				transactionService_->setBackgroundTask(pgId, false);
				PartitionId currentPId = request.fixed_.pId_;
				const PartitionGroupConfig& pgConfig = transactionManager_->getPartitionGroupConfig();
				for (uint32_t i = 0; i < pgConfig.getGroupPartitionCount(pgId); i++) {
					util::LockGuard<util::Mutex> guard(
						partitionList_->partition(currentPId).mutex());

					TransactionManager::ContextSource src;
					TransactionContext& txn =
						transactionManager_->put(alloc, currentPId,
							TXN_EMPTY_CLIENTID, src, now, emNow);
					Partition* partition = &partitionList_->partition(currentPId);
					if (partition->isActive()) {
						DataStoreV4* currentDs = getDataStore(currentPId);
						KeyDataStoreValue dummy;
						DSInputMes input(alloc, DS_SEARCH_BACKGROUND_TASK);
						StackAllocAutoPtr<DSBackgroundTaskOutputMes> ret(alloc, static_cast<DSBackgroundTaskOutputMes*>(currentDs->exec(&txn, &dummy, &input)));
						BGTask nextTask = ret.get()->bgTask_;

						if (nextTask.bgId_ != UNDEF_BACKGROUND_ID) {
							Event continueEvent(ec, BACK_GROUND, txn.getPartitionId());
							EventByteOutStream out = continueEvent.getOutStream();
							out << nextTask.bgId_;

							ec.getEngine().addTimer(continueEvent, waitTime);
							transactionService_->setBackgroundTask(pgId, true);
							break;
						}
					}
					currentPId++;
					if (currentPId == pgConfig.getGroupEndPartitionId(pgId)) {
						currentPId = pgConfig.getGroupBeginPartitionId(pgId);
					}
				}
			}
			else {
				ec.getEngine().addTimer(ev, waitTime);
			}
		}
	}
	catch (UserException& e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			"(nd=" << ev.getSenderND() << ", reason=" << GS_EXCEPTION_MESSAGE(e)
			<< ")");

		if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
			clusterService_->setError(ec, &e);
		}
	}
}

int32_t BackgroundHandler::getWaitTime(EventContext& ec, Event& ev, int32_t opeTime) {
	int64_t executableCount = 0;
	int64_t afterCount = 0;
	Partition* partition = &partitionList_->partition(ev.getPartitionId());
	if (!partition->isActive()) {
		return 0;
	}
	double rate = dsConfig_->getBackgroundWaitWeight();
	int32_t waitTime = transactionService_->getWaitTime(
		ec, &ev, opeTime, rate, executableCount, afterCount, TXN_BACKGROUND);
	GS_TRACE_INFO(
		DATASTORE_BACKGROUND, GS_ERROR_DS_BACKGROUND_TASK_INVALID,
		"[BackgroundHandler PartitionId = " << ev.getPartitionId()
		<< ", lsnDiff = " << diffLsn(ev.getPartitionId())
		<< ", opeTime = " << opeTime
		<< ", queueSize = " << executableCount + afterCount
		<< ", waitTime = " << waitTime);

	return waitTime;
}

uint64_t BackgroundHandler::diffLsn(PartitionId pId) {
	const PartitionGroupConfig& pgConfig = transactionManager_->getPartitionGroupConfig();
	PartitionGroupId pgId = pgConfig.getPartitionGroupId(pId);

	uint64_t prev = lsnCounter_[pgId];
	uint64_t current = 0;
	for (PartitionId currentPId = pgConfig.getGroupBeginPartitionId(pgId);
		currentPId < pgConfig.getGroupEndPartitionId(pgId); currentPId++) {
		current += getLogManager(currentPId)->getLSN();
	}
	uint64_t diff = current - prev;
	lsnCounter_[pgId] = current;
	return diff;
}

ContinueCreateDDLHandler::ContinueCreateDDLHandler()
	: StatementHandler() {
}

ContinueCreateDDLHandler::~ContinueCreateDDLHandler() {
}

/*!
	@brief Handler Operator
*/
void ContinueCreateDDLHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Response response(alloc);


	if (clusterManager_->isSystemError() || clusterManager_->isNormalShutdownCall()) {
		GS_TRACE_INFO(
			DATASTORE_BACKGROUND, GS_ERROR_DS_BACKGROUND_TASK_INVALID,
			"[ContinueCreateDDLHandler::operator] stop Service");
		return;
	}

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		EVENT_START(ec, ev, transactionManager_, connOption.dbId_, (ev.getSenderND().getId() < 0));

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkExecutable(
			request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		request.fixed_.cxtSrc_.getMode_ = TransactionManager::GET;
		request.fixed_.cxtSrc_.txnMode_ = TransactionManager::NO_AUTO_COMMIT_CONTINUE;
		TransactionContext& txn = transactionManager_->get(
			alloc, request.fixed_.pId_, request.fixed_.clientId_);

		NodeDescriptor originalND = txn.getSenderND();
		EventType replyEventType = UNDEF_EVENT_TYPE;
		bool isFinished = false;
		util::XArray<const util::XArray<uint8_t>*> logRecordList(alloc);
		util::XArray<ClientId> closedResourceIds(alloc);

		KeyDataStoreValue keyStoreValue = getKeyDataStore(txn.getPartitionId())->get(alloc, txn.getContainerId());
		DataStoreBase* ds = getDataStore(txn.getPartitionId(), keyStoreValue.storeType_);
		const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);

		KeyDataStoreValue outputStoreValue = keyStoreValue;
		StackAllocAutoPtr<DSBaseOutputMes> ret(alloc, NULL);
		switch (ev.getType()) {
		case CONTINUE_CREATE_INDEX: {
			DSInputMes input(alloc, DS_CONTINUE_CREATE_INDEX);

			DSOutputMes* retIndex = NULL;
			{
				retIndex = static_cast<DSOutputMes*>(ds->exec(&txn, &keyStoreValue, &input));
			}
			isFinished = retIndex->isExecuted_;
			util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
				request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
				keyStoreValue.storeType_, retIndex->dsLog_);
			if (logBinary != NULL) {
				logRecordList.push_back(logBinary);
			}

			ret.set(retIndex);
			replyEventType = CREATE_INDEX;
		}
								  break;
		case CONTINUE_ALTER_CONTAINER: {
			DSInputMes input(alloc, DS_CONTINUE_ALTER_CONTAINER);
			DSPutContainerOutputMes* retAlter = NULL;
			{
				retAlter = static_cast<DSPutContainerOutputMes*>(
					ds->exec(&txn, &keyStoreValue, &input));
			}
			isFinished = retAlter->isExecuted_;
			util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
				request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
				keyStoreValue.storeType_, retAlter->dsLog_);
			if (logBinary != NULL) {
				logRecordList.push_back(logBinary);
			}

			ret.set(retAlter);
			replyEventType = PUT_CONTAINER;
		} break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN, "");
		}
		transactionService_->incrementNoExpireOperationCount(request.fixed_.pId_);

		int32_t delayTime = 0;
		ReplicationContext::TaskStatus taskStatus = ReplicationContext::TASK_CONTINUE;
		if (isFinished) {
			DSInputMes input(alloc, DS_COMMIT);
			StackAllocAutoPtr<DSOutputMes> retCommit(alloc, NULL);
			{
				retCommit.set(static_cast<DSOutputMes*>(
					ds->exec(&txn, &outputStoreValue, &input)));
			}
			util::XArray<uint8_t>* logBinary = appendDataStoreLog(alloc, txn,
				request.fixed_.cxtSrc_, request.fixed_.cxtSrc_.stmtId_,
				keyStoreValue.storeType_, retCommit.get()->dsLog_);
			if (logBinary != NULL) {
				logRecordList.push_back(logBinary);
			}

			ec.setPendingPartitionChanged(request.fixed_.pId_);

			transactionManager_->remove(request.fixed_.pId_, request.fixed_.clientId_);
			closedResourceIds.push_back(request.fixed_.clientId_);

			request.fixed_.cxtSrc_.getMode_ = TransactionManager::AUTO;
			request.fixed_.cxtSrc_.txnMode_ = TransactionManager::AUTO_COMMIT;
			txn = transactionManager_->put(
				alloc, request.fixed_.pId_,
				request.fixed_.clientId_, request.fixed_.cxtSrc_, now, emNow);

			taskStatus = ReplicationContext::TASK_FINISHED;
		}
		else {
			transactionManager_->update(txn, request.fixed_.cxtSrc_.stmtId_);
		}

		request.fixed_.cxtSrc_.stmtId_ = request.fixed_.startStmtId_;
		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false),
			ret.get()->mes_);
		const bool ackWait = executeReplication(
			request, ec, alloc,
			ev.getSenderND(), txn, replyEventType, request.fixed_.cxtSrc_.stmtId_,
			TransactionManager::REPLICATION_SEMISYNC, taskStatus,
			closedResourceIds.data(), closedResourceIds.size(),
			logRecordList.data(), logRecordList.size(), request.fixed_.startStmtId_, delayTime,
			&outMes);

		if (isFinished) {
			replySuccess(ec, alloc, ev.getSenderND(), replyEventType,
				request, outMes, ackWait);
		}
		else {
			continueEvent(
				ec, alloc, ev.getSenderND(), replyEventType, request.fixed_.startStmtId_,
				request, response, ackWait);
		}

	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}

UpdateDataStoreStatusHandler::UpdateDataStoreStatusHandler()
	: StatementHandler() {
}

UpdateDataStoreStatusHandler::~UpdateDataStoreStatusHandler() {
}

/*!
	@brief Handler Operator
*/
void UpdateDataStoreStatusHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator& alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);
	BGTask currentTask;

	InMessage inMes;
	try {
		EVENT_START(ec, ev, transactionManager_, UNDEF_DBID, false);

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		Timestamp inputTime = inMes.inputTime_;
		bool force = inMes.force_;

		if (force) {
			GS_TRACE_WARNING(DATA_STORE,
				GS_TRACE_DS_DS_CHANGE_STATUS,
				"Force Update Latest Expiration Check Time :" << inputTime);
		}
		else {
			GS_TRACE_INFO(DATA_STORE,
				GS_TRACE_DS_DS_CHANGE_STATUS,
				"Update Latest Expiration Check Time :" << inputTime);
		}

		const PartitionGroupId pgId = ec.getWorkerId();
		if (clusterManager_->checkRecoveryCompleted()) {
			const PartitionGroupConfig& pgConfig = transactionManager_->getPartitionGroupConfig();
			for (PartitionId currentPId = pgConfig.getGroupBeginPartitionId(pgId);
				currentPId < pgConfig.getGroupEndPartitionId(pgId); currentPId++) {
				Partition* partition = &partitionList_->partition(currentPId);
				if (partition->isActive()) {
					util::LockGuard<util::Mutex> guard(
						partitionList_->partition(currentPId).mutex());

					DataStoreV4* ds = getDataStore(currentPId);
					TransactionContext& txn = transactionManager_->put(
						alloc, currentPId,
						TXN_EMPTY_CLIENTID, request.fixed_.cxtSrc_, now, emNow);
					const DataStoreBase::Scope dsScope(&txn, ds, clusterService_);

					ds->setLastExpiredTime(inputTime, force);
				}
			}
		}
	}
	catch (UserException& e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			"(nd=" << ev.getSenderND() << ", reason=" << GS_EXCEPTION_MESSAGE(e)
			<< ")");

		if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
			clusterService_->setError(ec, &e);
		}
	}
}



void TransactionService::requestUpdateDataStoreStatus(
	const Event::Source& eventSource, Timestamp time, bool force) {
	try {
		for (PartitionGroupId pgId = 0;
			pgId < pgConfig_.getPartitionGroupCount(); pgId++) {

			Event requestEvent(eventSource, UPDATE_DATA_STORE_STATUS,
				pgConfig_.getGroupBeginPartitionId(pgId));
			EventByteOutStream out = requestEvent.getOutStream();
			StatementHandler::encodeLongData<EventByteOutStream, Timestamp>(out, time);
			StatementHandler::encodeBooleanData<EventByteOutStream>(out, force);

			getEE()->add(requestEvent);
		}
	}
	catch (std::exception& e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			"(reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Handler Operator
*/
void UnsupportedStatementHandler::operator()(EventContext& ec, Event& ev) {
	util::StackAllocator& alloc = ec.getAllocator();

	if (ev.getSenderND().isEmpty()) return;

	Request request(alloc, getRequestSource(ev));
	InMessage inMes;
	try {
		EVENT_START(ec, ev, transactionManager_, UNDEF_DBID, false);

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);

		GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNSUPPORTED,
			"Unsupported statement type");
	}
	catch (std::exception& e) {
		handleError(ec, alloc, ev, request, e);
	}
}


/*!
	@brief Handler Operator
*/
void UnknownStatementHandler::operator()(EventContext& ec, Event& ev) {
	try {
		EVENT_START(ec, ev, transactionManager_, UNDEF_DBID, false);
		GS_THROW_USER_ERROR(
			GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN, "Unknown statement type");
	}
	catch (UserException& e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			GS_EXCEPTION_MESSAGE(e) << " (nd=" << ev.getSenderND()
			<< ", pId=" << ev.getPartitionId()
			<< ", eventType=" << ev.getType() << ")");
		EventType eventType = ev.getType();
		if (eventType >= V_1_5_SYNC_START && eventType <= V_1_5_SYNC_END) {
			GS_TRACE_WARNING(TRANSACTION_SERVICE,
				GS_TRACE_TXN_CLUSTER_VERSION_UNMATCHED,
				"Cluster version unmatched, eventType:" << eventType
				<< " is v1.5");
		}
	}
	catch (std::exception& e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			GS_EXCEPTION_MESSAGE(e) << " (nd=" << ev.getSenderND()
			<< ", pId=" << ev.getPartitionId()
			<< ", eventType=" << EventTypeUtility::getEventTypeName(ev.getType()) << ")");
		clusterService_->setError(ec, &e);
	}
}

/*!
	@brief Handler Operator
*/
void IgnorableStatementHandler::operator()(EventContext&, Event& ev) {
	GS_TRACE_INFO(TRANSACTION_SERVICE, GS_TRACE_TXN_REQUEST_IGNORED,
		"(nd=" << ev.getSenderND() << ", type=" << ev.getType() << ")");
}

TransactionService::TransactionService(
	ConfigTable& config, const EventEngine::Config& eeConfig,
	const EventEngine::Source& eeSource, const char8_t* name) :
	eeConfig_(eeConfig),
	eeSource_(eeSource),
	ee_(NULL),
	initalized_(false),
	pgConfig_(config),
	serviceConfig_(config),
	enableTxnTimeoutCheck_(
		pgConfig_.getPartitionCount(), util::Atomic<bool>(false)),
	readOperationCount_(pgConfig_.getPartitionCount(), 0),
	writeOperationCount_(pgConfig_.getPartitionCount(), 0),
	rowReadCount_(pgConfig_.getPartitionCount(), 0),
	rowWriteCount_(pgConfig_.getPartitionCount(), 0),
	backgroundOperationCount_(pgConfig_.getPartitionCount(), 0),
	noExpireOperationCount_(pgConfig_.getPartitionCount(), 0),
	abortDDLCount_(pgConfig_.getPartitionCount(), 0),
	totalInternalConnectionCount_(0),
	totalExternalConnectionCount_(0),
	onBackgroundTask_(pgConfig_.getPartitionGroupCount(), false),
	txnLogAlloc_(pgConfig_.getPartitionGroupCount(), NULL),
	connectHandler_(
		StatementHandler::TXN_CLIENT_VERSION,
		ACCEPTABLE_NOSQL_CLIENT_VERSIONS)
{
	try {
		eeConfig_.setServerNDAutoNumbering(false);
		eeConfig_.setClientNDEnabled(true);

		eeConfig_.setConcurrency(config.getUInt32(CONFIG_TABLE_DS_CONCURRENCY));
		eeConfig_.setPartitionCount(
			config.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM));

		ClusterAdditionalServiceConfig addConfig(config);
		const char* targetAddress = addConfig.getServiceAddress(TRANSACTION_SERVICE);
		eeConfig_.setServerAddress(targetAddress,
			config.getUInt16(CONFIG_TABLE_TXN_SERVICE_PORT));

		if (ClusterService::isMulticastMode(config)) {
			eeConfig_.setMulticastAddress(
				config.get<const char8_t*>(
					CONFIG_TABLE_TXN_NOTIFICATION_ADDRESS),
				config.getUInt16(CONFIG_TABLE_TXN_NOTIFICATION_PORT));
			if (strlen(config.get<const char8_t*>(
				CONFIG_TABLE_TXN_NOTIFICATION_INTERFACE_ADDRESS)) != 0) {
				eeConfig_.setMulticastInterfaceAddress(
					config.get<const char8_t*>(
						CONFIG_TABLE_TXN_NOTIFICATION_INTERFACE_ADDRESS),
					config.getUInt16(CONFIG_TABLE_TXN_SERVICE_PORT));
			}
		}

		eeConfig_.connectionCountLimit_ =
			config.get<int32_t>(CONFIG_TABLE_TXN_CONNECTION_LIMIT);

		eeConfig_.keepaliveEnabled_ =
			config.get<bool>(CONFIG_TABLE_TXN_USE_KEEPALIVE);
		eeConfig_.keepaliveIdle_ =
			config.get<int32_t>(CONFIG_TABLE_TXN_KEEPALIVE_IDLE);
		eeConfig_.keepaliveCount_ =
			config.get<int32_t>(CONFIG_TABLE_TXN_KEEPALIVE_COUNT);
		eeConfig_.keepaliveInterval_ =
			config.get<int32_t>(CONFIG_TABLE_TXN_KEEPALIVE_INTERVAL);

		eeConfig_.eventBufferSizeLimit_ = ConfigTable::megaBytesToBytes(
			config.getUInt32(CONFIG_TABLE_TXN_TOTAL_MESSAGE_MEMORY_LIMIT));

		eeConfig_.setAllAllocatorGroup(ALLOCATOR_GROUP_TXN_MESSAGE);
		eeConfig_.workAllocatorGroupId_ = ALLOCATOR_GROUP_TXN_WORK;

		ee_ = UTIL_NEW EventEngine(eeConfig_, eeSource_, name);



	}
	catch (std::exception& e) {
		delete ee_;

		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

TransactionService::~TransactionService() {
	ee_->shutdown();
	ee_->waitForShutdown();

	for (PartitionGroupId pgId = 0;
		pgId < pgConfig_.getPartitionGroupCount(); pgId++) {
		delete txnLogAlloc_[pgId];
	}

	TransactionManager* transactionManager =
		connectHandler_.transactionManager_;
	if (transactionManager != NULL) {
		for (PartitionId pId = 0; pId < pgConfig_.getPartitionCount(); pId++) {
			transactionManager->removePartition(pId);
		}

		transactionManager->removeAllReplicationContext();
		transactionManager->removeAllAuthenticationContext();

		for (PartitionGroupId pgId = 0;
			pgId < pgConfig_.getPartitionGroupCount(); pgId++) {
			assert(transactionManager->getTransactionContextCount(pgId) == 0);
			assert(transactionManager->getReplicationContextCount(pgId) == 0);
			assert(
				transactionManager->getAuthenticationContextCount(pgId) == 0);
		}
	}

	delete ee_;
}

void TransactionService::initialize(const ManagerSet& mgrSet) {
	try {

		ee_->setUserDataType<StatementHandler::ConnectionOption>();

		ee_->setThreadErrorHandler(serviceThreadErrorHandler_);
		serviceThreadErrorHandler_.initialize(mgrSet);

		connectHandler_.initialize(mgrSet);
		ee_->setHandler(CONNECT, connectHandler_);
		ee_->setHandlingMode(CONNECT, EventEngine::HANDLING_IMMEDIATE);

		disconnectHandler_.initialize(mgrSet);
		ee_->setCloseEventHandler(DISCONNECT, disconnectHandler_);
		ee_->setHandlingMode(
			DISCONNECT, EventEngine::HANDLING_PARTITION_SERIALIZED);

		loginHandler_.initialize(mgrSet);
		ee_->setHandler(LOGIN, loginHandler_);
		ee_->setHandlingMode(LOGIN, EventEngine::HANDLING_PARTITION_SERIALIZED);

		logoutHandler_.initialize(mgrSet);
		ee_->setHandler(LOGOUT, logoutHandler_);
		ee_->setHandlingMode(
			LOGOUT, EventEngine::HANDLING_PARTITION_SERIALIZED);

		getPartitionAddressHandler_.initialize(mgrSet);
		ee_->setHandler(GET_PARTITION_ADDRESS, getPartitionAddressHandler_);
		ee_->setHandlingMode(
			GET_PARTITION_ADDRESS, EventEngine::HANDLING_PARTITION_SERIALIZED);

		getPartitionContainerNamesHandler_.initialize(mgrSet);
		ee_->setHandler(
			GET_PARTITION_CONTAINER_NAMES, getPartitionContainerNamesHandler_);
		ee_->setHandlingMode(GET_PARTITION_CONTAINER_NAMES,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		getContainerPropertiesHandler_.initialize(mgrSet);
		ee_->setHandler(
			GET_CONTAINER_PROPERTIES, getContainerPropertiesHandler_);
		ee_->setHandlingMode(GET_CONTAINER_PROPERTIES,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		putContainerHandler_.initialize(mgrSet);
		ee_->setHandler(PUT_CONTAINER, putContainerHandler_);
		ee_->setHandlingMode(
			PUT_CONTAINER, EventEngine::HANDLING_PARTITION_SERIALIZED);

		putLargeContainerHandler_.initialize(mgrSet);
		ee_->setHandler(PUT_LARGE_CONTAINER, putLargeContainerHandler_);
		ee_->setHandlingMode(
			PUT_LARGE_CONTAINER, EventEngine::HANDLING_PARTITION_SERIALIZED);

		updateContainerStatusHandler_.initialize(mgrSet);
		ee_->setHandler(UPDATE_CONTAINER_STATUS, updateContainerStatusHandler_);
		ee_->setHandlingMode(
			UPDATE_CONTAINER_STATUS, EventEngine::HANDLING_PARTITION_SERIALIZED);

		createLargeIndexHandler_.initialize(mgrSet);
		ee_->setHandler(CREATE_LARGE_INDEX, createLargeIndexHandler_);
		ee_->setHandlingMode(
			CREATE_LARGE_INDEX, EventEngine::HANDLING_PARTITION_SERIALIZED);

		dropContainerHandler_.initialize(mgrSet);
		ee_->setHandler(DROP_CONTAINER, dropContainerHandler_);
		ee_->setHandlingMode(
			DROP_CONTAINER, EventEngine::HANDLING_PARTITION_SERIALIZED);

		getContainerHandler_.initialize(mgrSet);
		ee_->setHandler(GET_COLLECTION, getContainerHandler_);
		ee_->setHandlingMode(
			GET_COLLECTION, EventEngine::HANDLING_PARTITION_SERIALIZED);

		createIndexHandler_.initialize(mgrSet);
		ee_->setHandler(CREATE_INDEX, createIndexHandler_);
		ee_->setHandlingMode(
			CREATE_INDEX, EventEngine::HANDLING_PARTITION_SERIALIZED);

		dropIndexHandler_.initialize(mgrSet);
		ee_->setHandler(DELETE_INDEX, dropIndexHandler_);
		ee_->setHandlingMode(
			DELETE_INDEX, EventEngine::HANDLING_PARTITION_SERIALIZED);

		createDropTriggerHandler_.initialize(mgrSet);
		ee_->setHandler(CREATE_TRIGGER, createDropTriggerHandler_);
		ee_->setHandlingMode(
			CREATE_TRIGGER, EventEngine::HANDLING_PARTITION_SERIALIZED);
		ee_->setHandler(DELETE_TRIGGER, createDropTriggerHandler_);
		ee_->setHandlingMode(
			DELETE_TRIGGER, EventEngine::HANDLING_PARTITION_SERIALIZED);

		flushLogHandler_.initialize(mgrSet);
		ee_->setHandler(FLUSH_LOG, flushLogHandler_);
		ee_->setHandlingMode(
			FLUSH_LOG, EventEngine::HANDLING_PARTITION_SERIALIZED);

		createTransactionContextHandler_.initialize(mgrSet);
		ee_->setHandler(
			CREATE_TRANSACTION_CONTEXT, createTransactionContextHandler_);
		ee_->setHandlingMode(CREATE_TRANSACTION_CONTEXT,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		closeTransactionContextHandler_.initialize(mgrSet);
		ee_->setHandler(
			CLOSE_TRANSACTION_CONTEXT, closeTransactionContextHandler_);
		ee_->setHandlingMode(CLOSE_TRANSACTION_CONTEXT,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		commitAbortTransactionHandler_.initialize(mgrSet);
		ee_->setHandler(COMMIT_TRANSACTION, commitAbortTransactionHandler_);
		ee_->setHandlingMode(
			COMMIT_TRANSACTION, EventEngine::HANDLING_PARTITION_SERIALIZED);
		ee_->setHandler(ABORT_TRANSACTION, commitAbortTransactionHandler_);
		ee_->setHandlingMode(
			ABORT_TRANSACTION, EventEngine::HANDLING_PARTITION_SERIALIZED);

		putRowHandler_.initialize(mgrSet);
		ee_->setHandler(PUT_ROW, putRowHandler_);
		ee_->setHandlingMode(
			PUT_ROW, EventEngine::HANDLING_PARTITION_SERIALIZED);

		putRowSetHandler_.initialize(mgrSet);
		ee_->setHandler(PUT_MULTIPLE_ROWS, putRowSetHandler_);
		ee_->setHandlingMode(
			PUT_MULTIPLE_ROWS, EventEngine::HANDLING_PARTITION_SERIALIZED);

		removeRowHandler_.initialize(mgrSet);
		ee_->setHandler(REMOVE_ROW, removeRowHandler_);
		ee_->setHandlingMode(
			REMOVE_ROW, EventEngine::HANDLING_PARTITION_SERIALIZED);

		updateRowByIdHandler_.initialize(mgrSet);
		ee_->setHandler(UPDATE_ROW_BY_ID, updateRowByIdHandler_);
		ee_->setHandlingMode(
			UPDATE_ROW_BY_ID, EventEngine::HANDLING_PARTITION_SERIALIZED);

		removeRowByIdHandler_.initialize(mgrSet);
		ee_->setHandler(REMOVE_ROW_BY_ID, removeRowByIdHandler_);
		ee_->setHandlingMode(
			REMOVE_ROW_BY_ID, EventEngine::HANDLING_PARTITION_SERIALIZED);

		getRowHandler_.initialize(mgrSet);
		ee_->setHandler(GET_ROW, getRowHandler_);
		ee_->setHandlingMode(
			GET_ROW, EventEngine::HANDLING_PARTITION_SERIALIZED);

		getRowSetHandler_.initialize(mgrSet);
		ee_->setHandler(GET_MULTIPLE_ROWS, getRowSetHandler_);
		ee_->setHandlingMode(
			GET_MULTIPLE_ROWS, EventEngine::HANDLING_PARTITION_SERIALIZED);

		queryTqlHandler_.initialize(mgrSet);
		ee_->setHandler(QUERY_TQL, queryTqlHandler_);
		ee_->setHandlingMode(
			QUERY_TQL, EventEngine::HANDLING_PARTITION_SERIALIZED);

		appendRowHandler_.initialize(mgrSet);
		ee_->setHandler(APPEND_TIME_SERIES_ROW, appendRowHandler_);
		ee_->setHandlingMode(
			APPEND_TIME_SERIES_ROW, EventEngine::HANDLING_PARTITION_SERIALIZED);

		queryGeometryRelatedHandler_.initialize(mgrSet);
		ee_->setHandler(
			QUERY_COLLECTION_GEOMETRY_RELATED, queryGeometryRelatedHandler_);
		ee_->setHandlingMode(QUERY_COLLECTION_GEOMETRY_RELATED,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		queryGeometryWithExclusionHandler_.initialize(mgrSet);
		ee_->setHandler(QUERY_COLLECTION_GEOMETRY_WITH_EXCLUSION,
			queryGeometryWithExclusionHandler_);
		ee_->setHandlingMode(QUERY_COLLECTION_GEOMETRY_WITH_EXCLUSION,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		getRowTimeRelatedHandler_.initialize(mgrSet);
		ee_->setHandler(GET_TIME_SERIES_ROW_RELATED, getRowTimeRelatedHandler_);
		ee_->setHandlingMode(GET_TIME_SERIES_ROW_RELATED,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		getRowInterpolateHandler_.initialize(mgrSet);
		ee_->setHandler(INTERPOLATE_TIME_SERIES_ROW, getRowInterpolateHandler_);
		ee_->setHandlingMode(INTERPOLATE_TIME_SERIES_ROW,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		aggregateHandler_.initialize(mgrSet);
		ee_->setHandler(AGGREGATE_TIME_SERIES, aggregateHandler_);
		ee_->setHandlingMode(
			AGGREGATE_TIME_SERIES, EventEngine::HANDLING_PARTITION_SERIALIZED);

		queryTimeRangeHandler_.initialize(mgrSet);
		ee_->setHandler(QUERY_TIME_SERIES_RANGE, queryTimeRangeHandler_);
		ee_->setHandlingMode(QUERY_TIME_SERIES_RANGE,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		queryTimeSamplingHandler_.initialize(mgrSet);
		ee_->setHandler(QUERY_TIME_SERIES_SAMPLING, queryTimeSamplingHandler_);
		ee_->setHandlingMode(QUERY_TIME_SERIES_SAMPLING,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		fetchResultSetHandler_.initialize(mgrSet);
		ee_->setHandler(FETCH_RESULT_SET, fetchResultSetHandler_);
		ee_->setHandlingMode(
			FETCH_RESULT_SET, EventEngine::HANDLING_PARTITION_SERIALIZED);

		closeResultSetHandler_.initialize(mgrSet);
		ee_->setHandler(CLOSE_RESULT_SET, closeResultSetHandler_);
		ee_->setHandlingMode(
			CLOSE_RESULT_SET, EventEngine::HANDLING_PARTITION_SERIALIZED);

		multiCreateTransactionContextHandler_.initialize(mgrSet);
		ee_->setHandler(CREATE_MULTIPLE_TRANSACTION_CONTEXTS,
			multiCreateTransactionContextHandler_);
		ee_->setHandlingMode(CREATE_MULTIPLE_TRANSACTION_CONTEXTS,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		multiCloseTransactionContextHandler_.initialize(mgrSet);
		ee_->setHandler(CLOSE_MULTIPLE_TRANSACTION_CONTEXTS,
			multiCloseTransactionContextHandler_);
		ee_->setHandlingMode(CLOSE_MULTIPLE_TRANSACTION_CONTEXTS,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		multiPutHandler_.initialize(mgrSet);
		ee_->setHandler(PUT_MULTIPLE_CONTAINER_ROWS, multiPutHandler_);
		ee_->setHandlingMode(PUT_MULTIPLE_CONTAINER_ROWS,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		multiGetHandler_.initialize(mgrSet);
		ee_->setHandler(GET_MULTIPLE_CONTAINER_ROWS, multiGetHandler_);
		ee_->setHandlingMode(GET_MULTIPLE_CONTAINER_ROWS,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		multiQueryHandler_.initialize(mgrSet);
		ee_->setHandler(EXECUTE_MULTIPLE_QUERIES, multiQueryHandler_);
		ee_->setHandlingMode(EXECUTE_MULTIPLE_QUERIES,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		replicationLogHandler_.initialize(mgrSet);
		ee_->setHandler(REPLICATION_LOG, replicationLogHandler_);
		ee_->setHandlingMode(
			REPLICATION_LOG, EventEngine::HANDLING_PARTITION_SERIALIZED);

		replicationAckHandler_.initialize(mgrSet);
		ee_->setHandler(REPLICATION_ACK, replicationAckHandler_);
		ee_->setHandlingMode(
			REPLICATION_ACK, EventEngine::HANDLING_PARTITION_SERIALIZED);

		replicationLog2Handler_.initialize(mgrSet);
		ee_->setHandler(REPLICATION_LOG2, replicationLog2Handler_);
		ee_->setHandlingMode(
			REPLICATION_LOG2, EventEngine::HANDLING_PARTITION_SERIALIZED);

		replicationAck2Handler_.initialize(mgrSet);
		ee_->setHandler(REPLICATION_ACK2, replicationAck2Handler_);
		ee_->setHandlingMode(
			REPLICATION_ACK2, EventEngine::HANDLING_PARTITION_SERIALIZED);

		authenticationHandler_.initialize(mgrSet);
		ee_->setHandler(AUTHENTICATION, authenticationHandler_);
		ee_->setHandlingMode(
			AUTHENTICATION, EventEngine::HANDLING_PARTITION_SERIALIZED);

		authenticationAckHandler_.initialize(mgrSet);
		ee_->setHandler(AUTHENTICATION_ACK, authenticationAckHandler_);
		ee_->setHandlingMode(
			AUTHENTICATION_ACK, EventEngine::HANDLING_PARTITION_SERIALIZED);

		putUserHandler_.initialize(mgrSet);
		ee_->setHandler(PUT_USER, putUserHandler_);
		ee_->setHandlingMode(
			PUT_USER, EventEngine::HANDLING_PARTITION_SERIALIZED);

		dropUserHandler_.initialize(mgrSet);
		ee_->setHandler(DROP_USER, dropUserHandler_);
		ee_->setHandlingMode(
			DROP_USER, EventEngine::HANDLING_PARTITION_SERIALIZED);

		getUsersHandler_.initialize(mgrSet);
		ee_->setHandler(GET_USERS, getUsersHandler_);
		ee_->setHandlingMode(
			GET_USERS, EventEngine::HANDLING_PARTITION_SERIALIZED);

		putDatabaseHandler_.initialize(mgrSet);
		ee_->setHandler(PUT_DATABASE, putDatabaseHandler_);
		ee_->setHandlingMode(
			PUT_DATABASE, EventEngine::HANDLING_PARTITION_SERIALIZED);

		dropDatabaseHandler_.initialize(mgrSet);
		ee_->setHandler(DROP_DATABASE, dropDatabaseHandler_);
		ee_->setHandlingMode(
			DROP_DATABASE, EventEngine::HANDLING_PARTITION_SERIALIZED);

		getDatabasesHandler_.initialize(mgrSet);
		ee_->setHandler(GET_DATABASES, getDatabasesHandler_);
		ee_->setHandlingMode(
			GET_DATABASES, EventEngine::HANDLING_PARTITION_SERIALIZED);

		putPrivilegeHandler_.initialize(mgrSet);
		ee_->setHandler(PUT_PRIVILEGE, putPrivilegeHandler_);
		ee_->setHandlingMode(
			PUT_PRIVILEGE, EventEngine::HANDLING_PARTITION_SERIALIZED);

		dropPrivilegeHandler_.initialize(mgrSet);
		ee_->setHandler(DROP_PRIVILEGE, dropPrivilegeHandler_);
		ee_->setHandlingMode(
			DROP_PRIVILEGE, EventEngine::HANDLING_PARTITION_SERIALIZED);

		checkTimeoutHandler_.initialize(mgrSet);
		ee_->setHandler(TXN_COLLECT_TIMEOUT_RESOURCE, checkTimeoutHandler_);
		ee_->setHandlingMode(TXN_COLLECT_TIMEOUT_RESOURCE,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		dataStorePeriodicallyHandler_.initialize(mgrSet);
		ee_->setHandler(
			CHUNK_EXPIRE_PERIODICALLY, dataStorePeriodicallyHandler_);
		ee_->setHandlingMode(CHUNK_EXPIRE_PERIODICALLY,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		dataStorePeriodicallyHandler_.setConcurrency(mgrSet.txnSvc_->pgConfig_.getPartitionGroupCount());

		adjustStoreMemoryPeriodicallyHandler_.initialize(mgrSet);
		ee_->setHandler(ADJUST_STORE_MEMORY_PERIODICALLY,
			adjustStoreMemoryPeriodicallyHandler_);
		ee_->setHandlingMode(ADJUST_STORE_MEMORY_PERIODICALLY,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		keepLogHandler_.initialize(mgrSet);
		ee_->setHandler(TXN_KEEP_LOG_CHECK_PERIODICALLY, keepLogHandler_);
		ee_->setHandler(TXN_KEEP_LOG_UPDATE, keepLogHandler_);
		ee_->setHandler(TXN_KEEP_LOG_RESET, keepLogHandler_);
		ee_->setHandlingMode(TXN_KEEP_LOG_CHECK_PERIODICALLY,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		backgroundHandler_.initialize(mgrSet);
		ee_->setHandler(BACK_GROUND, backgroundHandler_);
		ee_->setHandlingMode(BACK_GROUND,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		createDDLContinueHandler_.initialize(mgrSet);
		ee_->setHandler(CONTINUE_CREATE_INDEX, createDDLContinueHandler_);
		ee_->setHandlingMode(CONTINUE_CREATE_INDEX,
			EventEngine::HANDLING_PARTITION_SERIALIZED);
		ee_->setHandler(CONTINUE_ALTER_CONTAINER, createDDLContinueHandler_);
		ee_->setHandlingMode(CONTINUE_ALTER_CONTAINER,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		updateDataStoreStatusHandler_.initialize(mgrSet);
		ee_->setHandler(UPDATE_DATA_STORE_STATUS, updateDataStoreStatusHandler_);
		ee_->setHandlingMode(UPDATE_DATA_STORE_STATUS,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		removeRowSetByIdHandler_.initialize(mgrSet);
		ee_->setHandler(REMOVE_MULTIPLE_ROWS_BY_ID_SET, removeRowSetByIdHandler_);
		ee_->setHandlingMode(
			REMOVE_MULTIPLE_ROWS_BY_ID_SET, EventEngine::HANDLING_PARTITION_SERIALIZED);
		updateRowSetByIdHandler_.initialize(mgrSet);
		ee_->setHandler(UPDATE_MULTIPLE_ROWS_BY_ID_SET, updateRowSetByIdHandler_);
		ee_->setHandlingMode(
			UPDATE_MULTIPLE_ROWS_BY_ID_SET, EventEngine::HANDLING_PARTITION_SERIALIZED);

		unsupportedStatementHandler_.initialize(mgrSet);
		for (EventType stmtType = V_1_1_STATEMENT_START;
			stmtType <= V_1_1_STATEMENT_END; stmtType++) {
			ee_->setHandler(stmtType, unsupportedStatementHandler_);
			ee_->setHandlingMode(
				stmtType, EventEngine::HANDLING_PARTITION_SERIALIZED);
		}
		const EventType V1_5_X_STATEMENTS[] = { GET_TIME_SERIES, PUT_TIME_SERIES,
			DELETE_COLLECTION, CREATE_EVENT_NOTIFICATION,
			DELETE_EVENT_NOTIFICATION, GET_TIME_SERIES_ROW,
			QUERY_TIME_SERIES_TQL, PUT_TIME_SERIES_ROW, PUT_TIME_SERIES_ROWSET,
			DELETE_TIME_SERIES_ROW, GET_TIME_SERIES_MULTIPLE_ROWS,
			UNDEF_EVENT_TYPE };
		for (size_t i = 0; V1_5_X_STATEMENTS[i] != UNDEF_EVENT_TYPE; i++) {
			ee_->setHandler(V1_5_X_STATEMENTS[i], unsupportedStatementHandler_);
			ee_->setHandlingMode(V1_5_X_STATEMENTS[i],
				EventEngine::HANDLING_PARTITION_SERIALIZED);
		}
		for (EventType stmtType = V_1_X_REPLICATION_EVENT_START;
			stmtType <= V_1_X_REPLICATION_EVENT_END; stmtType++) {
			ee_->setHandler(stmtType, unsupportedStatementHandler_);
			ee_->setHandlingMode(
				stmtType, EventEngine::HANDLING_PARTITION_SERIALIZED);
		}

		unsupportedStatementHandler_.initialize(mgrSet);
		for (EventType stmtType = V_1_1_STATEMENT_START;
			stmtType <= V_1_1_STATEMENT_END; stmtType++) {
			ee_->setHandler(stmtType, unsupportedStatementHandler_);
			ee_->setHandlingMode(
				stmtType, EventEngine::HANDLING_PARTITION_SERIALIZED);
		}
		for (EventType stmtType = V_1_X_REPLICATION_EVENT_START;
			stmtType <= V_1_X_REPLICATION_EVENT_END; stmtType++) {
			ee_->setHandler(stmtType, unsupportedStatementHandler_);
			ee_->setHandlingMode(
				stmtType, EventEngine::HANDLING_PARTITION_SERIALIZED);
		}

		unknownStatementHandler_.initialize(mgrSet);
		ee_->setUnknownEventHandler(unknownStatementHandler_);

		ignorableStatementHandler_.initialize(mgrSet);
		ee_->setHandler(RECV_NOTIFY_MASTER, ignorableStatementHandler_);
		ee_->setHandlingMode(
			RECV_NOTIFY_MASTER, EventEngine::HANDLING_IMMEDIATE);

		shortTermSyncHandler_.initialize(mgrSet);
		ee_->setHandler(TXN_SHORTTERM_SYNC_REQUEST, shortTermSyncHandler_);
		ee_->setHandler(TXN_SHORTTERM_SYNC_START, shortTermSyncHandler_);
		ee_->setHandler(TXN_SHORTTERM_SYNC_START_ACK, shortTermSyncHandler_);
		ee_->setHandler(TXN_SHORTTERM_SYNC_LOG, shortTermSyncHandler_);
		ee_->setHandler(TXN_SHORTTERM_SYNC_LOG_ACK, shortTermSyncHandler_);
		ee_->setHandler(TXN_SHORTTERM_SYNC_END, shortTermSyncHandler_);
		ee_->setHandler(TXN_SHORTTERM_SYNC_END_ACK, shortTermSyncHandler_);

		longTermSyncHandler_.initialize(mgrSet);
		setClusterHandler();

		ee_->setHandler(TXN_SYNC_REDO_LOG, redoLogHandler_);
		ee_->setHandler(TXN_SYNC_REDO_LOG_REPLICATION, redoLogHandler_);
		ee_->setHandler(TXN_SYNC_REDO_REMOVE, redoLogHandler_);
		ee_->setHandler(TXN_SYNC_REDO_CHECK, redoLogHandler_);
		ee_->setHandler(TXN_SYNC_REDO_ERROR, redoLogHandler_);
		redoLogHandler_.initialize(mgrSet);

		syncCheckEndHandler_.initialize(mgrSet);
		ee_->setHandler(TXN_SYNC_TIMEOUT, syncCheckEndHandler_);
		ee_->setHandler(TXN_SYNC_CHECK_END, syncCheckEndHandler_);

		changePartitionTableHandler_.initialize(mgrSet);
		ee_->setHandler(
			TXN_CHANGE_PARTITION_TABLE, changePartitionTableHandler_);
		changePartitionStateHandler_.initialize(mgrSet);
		ee_->setHandler(
			TXN_CHANGE_PARTITION_STATE, changePartitionStateHandler_);
		dropPartitionHandler_.initialize(mgrSet);
		ee_->setHandler(TXN_DROP_PARTITION, dropPartitionHandler_);

		ee_->setHandler(SQL_GET_CONTAINER, sqlGetContainerHandler_);
		sqlGetContainerHandler_.initialize(mgrSet);

		const PartitionGroupConfig& pgConfig =
			mgrSet.txnMgr_->getPartitionGroupConfig();

		for (PartitionGroupId pgId = 0;
			pgId < pgConfig.getPartitionGroupCount(); pgId++) {
			const PartitionId beginPId =
				pgConfig.getGroupBeginPartitionId(pgId);
			Event timeoutCheckEvent(
				eeSource_, TXN_COLLECT_TIMEOUT_RESOURCE, beginPId);
			ee_->addPeriodicTimer(
				timeoutCheckEvent, TXN_TIMEOUT_CHECK_INTERVAL * 1000);
			Event dataStorePeriodicEvent(
				eeSource_, CHUNK_EXPIRE_PERIODICALLY, beginPId);
			ee_->addPeriodicTimer(
				dataStorePeriodicEvent, CHUNK_EXPIRE_CHECK_INTERVAL * 1000);
			Event adjustStoreMemoryPeriodicEvent(
				eeSource_, ADJUST_STORE_MEMORY_PERIODICALLY, beginPId);
			ee_->addPeriodicTimer(adjustStoreMemoryPeriodicEvent,
				ADJUST_STORE_MEMORY_CHECK_INTERVAL * 1000);

			int32_t interval = CommonUtility::changeTimeSecondToMilliSecond(
				mgrSet.config_->get<int32_t>(CONFIG_TABLE_SYNC_KEEP_LOG_CHECK_INTERVAL));
			Event keepLogCheckEvent(eeSource_, TXN_KEEP_LOG_CHECK_PERIODICALLY, beginPId);
			ee_->addPeriodicTimer(keepLogCheckEvent, interval);
		}

		ee_->setHandler(EXECUTE_JOB, executeHandler_);
		executeHandler_.initialize(mgrSet, TRANSACTION_SERVICE);
		ee_->setHandler(SEND_EVENT, sendEventHandler_);
		ee_->setHandlingMode(SEND_EVENT,
			EventEngine::HANDLING_IMMEDIATE);

		sendEventHandler_.initialize(mgrSet, TRANSACTION_SERVICE);

		transactionManager_ = mgrSet.txnMgr_;
		transactionManager_->initialize(mgrSet);

		for (PartitionGroupId pgId = 0;
			pgId < pgConfig_.getPartitionGroupCount(); pgId++) {
			txnLogAlloc_[pgId] = UTIL_NEW util::StackAllocator(
				util::AllocatorInfo(ALLOCATOR_GROUP_TXN_WORK, "txnLog"),
				eeSource_.fixedAllocator_);
		}

		resultSetHolderManager_.initialize(
			mgrSet.txnMgr_->getPartitionGroupConfig());

		statUpdator_.service_ = this;
		statUpdator_.manager_ = mgrSet.txnMgr_;
		mgrSet.stats_->addUpdator(&statUpdator_);

		syncManager_ = mgrSet.syncMgr_;
		checkpointService_ = mgrSet.cpSvc_;

		userCache_ = mgrSet.userCache_;
		olFactory_ = mgrSet.olFactory_;

		initalized_ = true;
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void TransactionService::start() {
	try {
		if (!initalized_) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_SERVICE_NOT_INITIALIZED, "");
		}

		ee_->start();
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void TransactionService::shutdown() {
	ee_->shutdown();
}

void TransactionService::waitForShutdown() {
	ee_->waitForShutdown();
}

EventEngine* TransactionService::getEE() {
	return ee_;
}

void TransactionService::changeTimeoutCheckMode(PartitionId pId,
	PartitionTable::PartitionStatus after, ChangePartitionType changeType,
	bool isToSubMaster, ClusterStats& stats) {
	const bool isShortTermSyncBegin = (changeType == PT_CHANGE_SYNC_START);
	const bool isShortTermSyncEnd = (changeType == PT_CHANGE_SYNC_END);

	assert(!(isShortTermSyncBegin && isShortTermSyncEnd));
	assert(!(isToSubMaster && isShortTermSyncBegin));
	assert(!(isToSubMaster && isShortTermSyncEnd));


	switch (after) {
	case PartitionTable::PT_ON:
		assert(!isShortTermSyncBegin);
		assert(!isToSubMaster);
		enableTransactionTimeoutCheck(pId);
		stats.setTransactionTimeoutCheck(pId, 1);
		break;

	case PartitionTable::PT_OFF:
		if (isToSubMaster) {
			disableTransactionTimeoutCheck(pId);
			stats.setTransactionTimeoutCheck(pId, 0);
		}
		else if (isShortTermSyncBegin) {
			enableTransactionTimeoutCheck(pId);
			stats.setTransactionTimeoutCheck(pId, 1);
		}
		else if (isShortTermSyncEnd) {
			enableTransactionTimeoutCheck(pId);
			stats.setTransactionTimeoutCheck(pId, 1);
		}
		else {
		}
		break;

	case PartitionTable::PT_SYNC:
		assert(isShortTermSyncBegin);
		assert(!isShortTermSyncEnd);
		assert(!isToSubMaster);
		disableTransactionTimeoutCheck(pId);
		stats.setTransactionTimeoutCheck(pId, 0);
		break;

	case PartitionTable::PT_STOP:
		assert(!isShortTermSyncBegin);
		assert(!isShortTermSyncEnd);
		disableTransactionTimeoutCheck(pId);
		stats.setTransactionTimeoutCheck(pId, 0);
		break;

	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INVALID_ARGS,
				"(pId=" << pId << ", state=" << static_cast<int32_t>(after) <<
				", changeType=" << static_cast<int32_t>(changeType) <<
				", isToSubMaster=" << isToSubMaster << ")");
	}
}

void TransactionService::enableTransactionTimeoutCheck(PartitionId pId) {
	const bool before = enableTxnTimeoutCheck_[pId];
	enableTxnTimeoutCheck_[pId] = true;

	if (!before) {
		GS_TRACE_INFO(TRANSACTION_SERVICE, GS_TRACE_TXN_CHECK_TIMEOUT,
			"timeout check enabled. (pId=" << pId << ")");
	}
}

void TransactionService::disableTransactionTimeoutCheck(PartitionId pId) {
	const bool before = enableTxnTimeoutCheck_[pId];
	enableTxnTimeoutCheck_[pId] = false;

	if (before) {
		GS_TRACE_INFO(TRANSACTION_SERVICE, GS_TRACE_TXN_CHECK_TIMEOUT,
			"timeout check disabled. (pId=" << pId << ")");
	}
}

bool TransactionService::isTransactionTimeoutCheckEnabled(
	PartitionId pId) const {
	return enableTxnTimeoutCheck_[pId];
}

size_t TransactionService::getWorkMemoryByteSizeLimit() const {
	return serviceConfig_.getWorkMemoryByteSizeLimit();
}

ResultSetHolderManager& TransactionService::getResultSetHolderManager() {
	return resultSetHolderManager_;
}

TransactionService::Config::Config(ConfigTable& configTable) :
	workMemoryByteSizeLimit_(
		ConfigTable::megaBytesToBytes(
			configTable.get<int32_t>(CONFIG_TABLE_TXN_WORK_MEMORY_LIMIT)))
{
	setUpConfigHandler(configTable);
}

size_t TransactionService::Config::getWorkMemoryByteSizeLimit() const {
	return workMemoryByteSizeLimit_;
}

void TransactionService::Config::setUpConfigHandler(ConfigTable& configTable) {
	configTable.setParamHandler(CONFIG_TABLE_TXN_WORK_MEMORY_LIMIT, *this);
}

void TransactionService::Config::operator()(
	ConfigTable::ParamId id, const ParamValue& value) {
	switch (id) {
	case CONFIG_TABLE_TXN_WORK_MEMORY_LIMIT:
		workMemoryByteSizeLimit_ = ConfigTable::megaBytesToBytes(value.get<int32_t>());
		break;
	}
}

uint64_t TransactionService::getTotalReadOperationCount() const {
	uint64_t totalCount = 0;
	for (PartitionId pId = 0; pId < pgConfig_.getPartitionCount(); pId++) {
		totalCount += readOperationCount_[pId];
	}
	return totalCount;
}

uint64_t TransactionService::getTotalWriteOperationCount() const {
	uint64_t totalCount = 0;
	for (PartitionId pId = 0; pId < pgConfig_.getPartitionCount(); pId++) {
		totalCount += writeOperationCount_[pId];
	}
	return totalCount;
}

uint64_t TransactionService::getTotalRowReadCount() const {
	uint64_t totalCount = 0;
	for (PartitionId pId = 0; pId < pgConfig_.getPartitionCount(); pId++) {
		totalCount += rowReadCount_[pId];
	}
	return totalCount;
}

uint64_t TransactionService::getTotalRowWriteCount() const {
	uint64_t totalCount = 0;
	for (PartitionId pId = 0; pId < pgConfig_.getPartitionCount(); pId++) {
		totalCount += rowWriteCount_[pId];
	}
	return totalCount;
}

uint64_t TransactionService::getTotalBackgroundOperationCount() const {
	uint64_t totalCount = 0;
	for (PartitionId pId = 0; pId < pgConfig_.getPartitionCount(); pId++) {
		totalCount += backgroundOperationCount_[pId];
	}
	return totalCount;
}

uint64_t TransactionService::getTotalNoExpireOperationCount() const {
	uint64_t totalCount = 0;
	for (PartitionId pId = 0; pId < pgConfig_.getPartitionCount(); pId++) {
		totalCount += noExpireOperationCount_[pId];
	}
	return totalCount;
}
uint64_t TransactionService::getTotalAbortDDLCount() const {
	uint64_t totalCount = 0;
	for (PartitionId pId = 0; pId < pgConfig_.getPartitionCount(); pId++) {
		totalCount += abortDDLCount_[pId];
	}
	return totalCount;
}

TransactionService::StatSetUpHandler TransactionService::statSetUpHandler_;

#define STAT_ADD_SUB(id) STAT_TABLE_ADD_PARAM_SUB(stat, parentId, id)

void TransactionService::StatSetUpHandler::operator()(StatTable& stat) {
	StatTable::ParamId parentId;

	parentId = STAT_TABLE_ROOT;
	stat.resolveGroup(parentId, STAT_TABLE_PERF, "performance");

	parentId = STAT_TABLE_PERF;
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_NUM_CONNECTION);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_NUM_SESSION);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_NUM_TXN);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_TOTAL_LOCK_CONFLICT_COUNT);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_TOTAL_READ_OPERATION);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_TOTAL_WRITE_OPERATION);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_TOTAL_ROW_READ);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_TOTAL_ROW_WRITE);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_NUM_NO_EXPIRE_TXN);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_TOTAL_INTERNAL_CONNECTION_COUNT);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_TOTAL_EXTERNAL_CONNECTION_COUNT);


	stat.resolveGroup(parentId, STAT_TABLE_PERF_TXN_DETAIL, "txnDetail");
	parentId = STAT_TABLE_PERF_TXN_DETAIL;
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_TOTAL_BACKGROUND_OPERATION);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_TOTAL_NO_EXPIRE_OPERATION);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_TOTAL_ABORT_DDL);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_TOTAL_REP_TIMEOUT);
}

bool TransactionService::StatUpdator::operator()(StatTable& stat) {
	if (!stat.getDisplayOption(STAT_TABLE_DISPLAY_SELECT_PERF)) {
		return true;
	}

	TransactionService& svc = *service_;
	TransactionManager& mgr = *manager_;

	EventEngine::Stats txnSvcStats;
	svc.getEE()->getStats(txnSvcStats);

	const uint64_t numClientTCPConnection =
		txnSvcStats.get(EventEngine::Stats::ND_CLIENT_CREATE_COUNT) -
		txnSvcStats.get(EventEngine::Stats::ND_CLIENT_REMOVE_COUNT);
	const uint64_t numReplicationTCPConnection =
		txnSvcStats.get(EventEngine::Stats::ND_SERVER_CREATE_COUNT) -
		txnSvcStats.get(EventEngine::Stats::ND_SERVER_REMOVE_COUNT);
	const uint64_t numClientUDPConnection =
		txnSvcStats.get(EventEngine::Stats::ND_MCAST_CREATE_COUNT) -
		txnSvcStats.get(EventEngine::Stats::ND_MCAST_REMOVE_COUNT);
	const uint64_t numConnection = numClientTCPConnection +
		numReplicationTCPConnection +
		numClientUDPConnection;
	stat.set(STAT_TABLE_PERF_TXN_NUM_CONNECTION, numConnection);

	const uint32_t concurrency =
		mgr.getPartitionGroupConfig().getPartitionGroupCount();
	uint64_t numTxn = 0;
	uint64_t numSession = 0;
	uint64_t numNoExpireTxn = 0;
	uint64_t numReplicationTimeout = 0;
	for (PartitionGroupId pgId = 0; pgId < concurrency; pgId++) {
		numTxn += mgr.getActiveTransactionCount(pgId);
		numSession += mgr.getTransactionContextCount(pgId);
		numNoExpireTxn += mgr.getNoExpireTransactionCount(pgId);
	}
	for (PartitionId pId = 0; pId < mgr.getPartitionGroupConfig().getPartitionCount(); pId++) {
		numReplicationTimeout += mgr.getReplicationTimeoutCount(pId);
	}

	stat.set(STAT_TABLE_PERF_TXN_NUM_SESSION, numSession);
	stat.set(STAT_TABLE_PERF_TXN_NUM_TXN, numTxn);
	stat.set(STAT_TABLE_PERF_TXN_TOTAL_LOCK_CONFLICT_COUNT,
		txnSvcStats.get(EventEngine::Stats::EVENT_PENDING_ADD_COUNT));
	stat.set(STAT_TABLE_PERF_TXN_TOTAL_READ_OPERATION,
		svc.getTotalReadOperationCount());
	stat.set(STAT_TABLE_PERF_TXN_TOTAL_WRITE_OPERATION,
		svc.getTotalWriteOperationCount());
	stat.set(STAT_TABLE_PERF_TXN_TOTAL_ROW_READ, svc.getTotalRowReadCount());
	stat.set(STAT_TABLE_PERF_TXN_TOTAL_ROW_WRITE, svc.getTotalRowWriteCount());

	stat.set(STAT_TABLE_PERF_TXN_TOTAL_BACKGROUND_OPERATION, svc.getTotalBackgroundOperationCount());

	stat.set(STAT_TABLE_PERF_TXN_NUM_NO_EXPIRE_TXN, numNoExpireTxn);
	if (stat.getDisplayOption(STAT_TABLE_DISPLAY_WEB_ONLY) &&
		stat.getDisplayOption(STAT_TABLE_DISPLAY_OPTIONAL_TXN)) {
		stat.set(STAT_TABLE_PERF_TXN_TOTAL_NO_EXPIRE_OPERATION, svc.getTotalNoExpireOperationCount());
		stat.set(STAT_TABLE_PERF_TXN_TOTAL_ABORT_DDL, svc.getTotalAbortDDLCount());
		stat.set(STAT_TABLE_PERF_TXN_TOTAL_REP_TIMEOUT, numReplicationTimeout);
	}
	if (stat.getDisplayOption(STAT_TABLE_DISPLAY_WEB_ONLY) &&
		stat.getDisplayOption(STAT_TABLE_DISPLAY_OPTIONAL_TXN)) {
		stat.set(STAT_TABLE_PERF_TXN_TOTAL_INTERNAL_CONNECTION_COUNT, svc.getTotalInternalConnectionCount());

		stat.set(STAT_TABLE_PERF_TXN_TOTAL_EXTERNAL_CONNECTION_COUNT, svc.getTotalExternalConnectionCount());
	}
	return true;
}

int32_t TransactionService::getWaitTime(EventContext& ec, Event* ev, int32_t opeTime,
	double rate, int64_t& executableCount, int64_t& afterCount, BackgroundEventType type) {
	int32_t waitTime = 0;
	const PartitionGroupId pgId = ec.getWorkerId();
	if (type == TXN_BACKGROUND) {
		EventEngine::Tool::getLiveStats(ec.getEngine(), pgId,
			EventEngine::Stats::EVENT_ACTIVE_EXECUTABLE_ONE_SHOT_COUNT,
			executableCount, &ec, ev);
		EventEngine::Tool::getLiveStats(ec.getEngine(), pgId,
			EventEngine::Stats::EVENT_CYCLE_HANDLING_AFTER_ONE_SHOT_COUNT,
			afterCount, &ec, ev);
		if (executableCount + afterCount > 0) {
			double baseTime = (opeTime == 0) ? 0.5 : static_cast<double>(opeTime);
			waitTime = static_cast<uint32_t>(
				ceil(baseTime * rate));
		}
	}
	else {
		EventEngine::Stats stats;
		if (ec.getEngine().getStats(pgId, stats)) {
			int32_t queueSizeLimit;
			int32_t interval = 0;
			switch (type) {
			case SYNC_EXEC: {
				queueSizeLimit = syncManager_->getExtraConfig().getLimitLongtermQueueSize();
				interval = syncManager_->getExtraConfig().getLongtermHighLoadInterval();
				int32_t logWaitInterval = syncManager_->getExtraConfig().getLongtermLogWaitInterval();
				if (logWaitInterval > 0) {
					return logWaitInterval;
				}
				int32_t queueSize = static_cast<int32_t>(
					stats.get(EventEngine::Stats::EVENT_ACTIVE_QUEUE_SIZE_CURRENT));
				if (queueSize >= queueSizeLimit) {
					return interval;
				}
				else {
					return 0;
				}
				break;
			}
			case CP_CHUNKCOPY:
				queueSizeLimit = checkpointService_->getChunkCopyLimitQueueSize();
				interval = checkpointService_->getChunkCopyInterval();
				break;
			default:
				return 0;
			}
			int32_t queueSize = static_cast<int32_t>(
				stats.get(EventEngine::Stats::EVENT_ACTIVE_QUEUE_SIZE_CURRENT));
			if (queueSize > queueSizeLimit) {
				return interval;
			}
		}
	}
	return waitTime;
}


TransactionService::StatUpdator::StatUpdator()
	: service_(NULL), manager_(NULL) {}

void EventMonitor::reset(EventStart& eventStart) {
	PartitionGroupId pgId = eventStart.getEventContext().getWorkerId();
	eventInfoList_[pgId].init();
}

void EventMonitor::set(EventStart& eventStart, DatabaseId dbId, bool clientRequest) {
	PartitionGroupId pgId = eventStart.getEventContext().getWorkerId();
	eventInfoList_[pgId].eventType_ = eventStart.getEvent().getType();
	eventInfoList_[pgId].pId_ = eventStart.getEvent().getPartitionId();
	eventInfoList_[pgId].startTime_
		= eventStart.getEventContext().getHandlerStartTime().getUnixTime();
	eventInfoList_[pgId].dbId_ = dbId;
	eventInfoList_[pgId].clientRequest_ = clientRequest;

}

void EventMonitor::setType(PartitionGroupId pgId, int32_t typeDetail) {
	eventInfoList_[pgId].type_ = typeDetail;
}

std::string EventMonitor::dump() {
	util::NormalOStringStream strstrm;
	for (size_t pos = 0; pos < eventInfoList_.size(); pos++) {
		eventInfoList_[pos].dump(strstrm, pos);
	}
	return strstrm.str().c_str();
}

void EventMonitor::EventInfo::dump(util::NormalOStringStream& strstrm, size_t pos) {
	strstrm << "workerId=" << pos
		<< ", event=" << EventTypeUtility::getEventTypeName(eventType_)
		<< ", pId=" << pId_
		<< ", startTime=" << CommonUtility::getTimeStr(startTime_)
		<< ", type=" << static_cast<int32_t>(type_)
		<< ", isClient=" << static_cast<int32_t>(clientRequest_)
		<< std::endl;
}


template<typename S>
void GetRowSetHandler::OutMessage::encode(S& out) {
	SimpleOutputMessage::encode(out);
	assert(rs_ != NULL);
	out << rowId_;
}

void GetRowSetHandler::OutMessage::addExtraMessage(Event* ev) {
	if (ev != NULL) {
		ev->addExtraMessage(rs_->getFixedStartData(),
			rs_->getFixedOffsetSize());
		ev->addExtraMessage(rs_->getVarStartData(),
			rs_->getVarOffsetSize());
	}
}
void GetRowSetHandler::OutMessage::addExtraMessage(ReplicationContext* replContext) {
	if (replContext != NULL) {
		replContext->addExtraMessage(rs_->getFixedStartData(),
			rs_->getFixedOffsetSize());
		replContext->addExtraMessage(rs_->getVarStartData(),
			rs_->getVarOffsetSize());
	}
}



template<typename S>
void FetchResultSetHandler::OutMessage::encode(S& out) {
	SimpleOutputMessage::encode(out);
	assert(rs_ != NULL);
	encodeBooleanData<S>(out, rs_->isRelease());
	out << rs_->getVarStartPos();
	out << rs_->getFetchNum();
}

void FetchResultSetHandler::OutMessage::addExtraMessage(Event* ev) {
	if (ev != NULL) {
		ev->addExtraMessage(rs_->getFixedStartData(),
			rs_->getFixedOffsetSize());
		ev->addExtraMessage(rs_->getVarStartData(),
			rs_->getVarOffsetSize());
	}
}
void FetchResultSetHandler::OutMessage::addExtraMessage(ReplicationContext* replContext) {
	if (replContext != NULL) {
		replContext->addExtraMessage(rs_->getFixedStartData(),
			rs_->getFixedOffsetSize());
		replContext->addExtraMessage(rs_->getVarStartData(),
			rs_->getVarOffsetSize());
	}
}


template<typename S>
void StatementHandler::TQLOutputMessage::encode(S& out) {
	SimpleOutputMessage::encode(out);
	assert(rs_ != NULL);
	if (isSQL_) {
		encodeEnumData<S, ResultType>(out, rs_->getResultType());
		out << rs_->getResultNum();
		out << rs_->getFixedOffsetSize();
		encodeBinaryData<S>(out, rs_->getFixedStartData(), static_cast<size_t>(rs_->getFixedOffsetSize()));
		out << rs_->getVarOffsetSize();
		encodeBinaryData<S>(out, rs_->getVarStartData(), static_cast<size_t>(rs_->getVarOffsetSize()));
		return;
	}
	else
		switch (rs_->getResultType()) {
		case RESULT_ROWSET: {
			PartialQueryOption partialQueryOption(alloc_);
			rs_->getQueryOption().encodePartialQueryOption(alloc_, partialQueryOption);

			const size_t entryNumPos = out.base().position();
			int32_t entryNum = 0;
			out << entryNum;

			if (!partialQueryOption.empty()) {
				encodeEnumData<S, QueryResponseType>(out, PARTIAL_EXECUTION_STATE);

				const size_t entrySizePos = out.base().position();
				int32_t entrySize = 0;
				out << entrySize;

				encodeBooleanData<S>(out, true);
				entrySize += static_cast<int32_t>(sizeof(int8_t));

				int32_t partialEntryNum = static_cast<int32_t>(partialQueryOption.size());
				out << partialEntryNum;
				entrySize += static_cast<int32_t>(sizeof(int32_t));

				PartialQueryOption::const_iterator itr;
				for (itr = partialQueryOption.begin(); itr != partialQueryOption.end(); itr++) {
					encodeIntData<S, int8_t>(out, itr->first);
					entrySize += static_cast<int32_t>(sizeof(int8_t));

					int32_t partialEntrySize =
							static_cast<int32_t>(itr->second->size());
					out << partialEntrySize;
					entrySize += static_cast<int32_t>(sizeof(int32_t));
					encodeBinaryData<S>(out, itr->second->data(), itr->second->size());
					entrySize += static_cast<int32_t>(itr->second->size());
				}
				const size_t entryLastPos = out.base().position();
				out.base().position(entrySizePos);
				out << entrySize;
				out.base().position(entryLastPos);
				entryNum++;
			}

			entryNum += encodeDistributedResult<S>(out, *rs_, NULL);

			{
				encodeEnumData<S, QueryResponseType>(out, ROW_SET);

				const int32_t entrySize = static_cast<int32_t>(
						sizeof(ResultSize) +
						rs_->getFixedOffsetSize() +
						rs_->getVarOffsetSize());
				out << entrySize;
				out << rs_->getFetchNum();
				entryNum++;
			}

			const size_t lastPos = out.base().position();
			out.base().position(entryNumPos);
			out << entryNum;
			out.base().position(lastPos);
		} break;
		case RESULT_EXPLAIN: {
			const size_t entryNumPos = out.base().position();
			int32_t entryNum = 0;
			out << entryNum;

			entryNum += encodeDistributedResult<S>(out, *rs_, NULL);

			encodeEnumData<S, QueryResponseType>(out, QUERY_ANALYSIS);
			const int32_t entrySize = static_cast<int32_t>(
					sizeof(ResultSize) +
					rs_->getFixedOffsetSize() +
					rs_->getVarOffsetSize());
			out << entrySize;

			out << rs_->getResultNum();
			entryNum++;

			const size_t lastPos = out.base().position();
			out.base().position(entryNumPos);
			out << entryNum;
			out.base().position(lastPos);
		} break;
		case RESULT_AGGREGATE: {
			int32_t entryNum = 1;
			out << entryNum;

			encodeEnumData<S, QueryResponseType>(out, AGGREGATION);
			const util::XArray<uint8_t>* rowDataFixedPart =
				rs_->getRowDataFixedPartBuffer();
			const int32_t entrySize = static_cast<int32_t>(
					sizeof(ResultSize) + rowDataFixedPart->size());
			out << entrySize;

			out << rs_->getResultNum();
			encodeBinaryData<S>(
				out, rowDataFixedPart->data(), rowDataFixedPart->size());
		} break;
		case PARTIAL_RESULT_ROWSET: {
			int32_t entryNum = 2;
			out << entryNum;
			{
				encodeEnumData<S, QueryResponseType>(out, PARTIAL_FETCH_STATE);
				int32_t entrySize = sizeof(ResultSize) + sizeof(ResultSetId);
				out << entrySize;

				out << rs_->getResultNum();
				out << rs_->getId();
			}
			{
				encodeEnumData<S, QueryResponseType>(out, ROW_SET);
				const int32_t entrySize = static_cast<uint32_t>(
						sizeof(ResultSize) +
						rs_->getFixedOffsetSize() +
						rs_->getVarOffsetSize());
				out << entrySize;

				out << rs_->getFetchNum();
			}
		} break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_RESULT_TYPE_INVALID,
					"(resultType=" <<
					static_cast<int32_t>(rs_->getResultType()) << ")");
		}

}
void StatementHandler::TQLOutputMessage::addExtraMessage(Event* ev) {
	if (isSQL_) {
		return;
	}
	if (ev == NULL) {
		return;
	}

	switch (rs_->getResultType()) {
	case RESULT_ROWSET:
	case RESULT_EXPLAIN:
	case PARTIAL_RESULT_ROWSET: {
		ev->addExtraMessage(rs_->getFixedStartData(),
			rs_->getFixedOffsetSize());
		ev->addExtraMessage(rs_->getVarStartData(),
			rs_->getVarOffsetSize());
	} break;
	case RESULT_AGGREGATE: {
	} break;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_TXN_RESULT_TYPE_INVALID,
				"(resultType=" <<
				static_cast<int32_t>(rs_->getResultType()) << ")");
	}
}
void StatementHandler::TQLOutputMessage::addExtraMessage(ReplicationContext* replContext) {
	if (isSQL_) {
		return;
	}
	if (replContext == NULL) {
		return;
	}

	switch (rs_->getResultType()) {
	case RESULT_ROWSET:
	case RESULT_EXPLAIN:
	case PARTIAL_RESULT_ROWSET: {
		replContext->addExtraMessage(rs_->getFixedStartData(),
			rs_->getFixedOffsetSize());
		replContext->addExtraMessage(rs_->getVarStartData(),
			rs_->getVarOffsetSize());
	} break;
	case RESULT_AGGREGATE: {
	} break;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_TXN_RESULT_TYPE_INVALID,
				"(resultType=" <<
				static_cast<int32_t>(rs_->getResultType()) << ")");
	}
}

bool StatementHandler::checkConstraint(Event& ev, ConnectionOption& connOption, bool isUpdate) {
	uint32_t delayTime1, delayTime2;
	bool isClientNd = (!ev.getSenderND().isEmpty() && ev.getSenderND().getId() < 0);
	if (clusterManager_->getStandbyInfo().isStandby()) {
		if (transactionService_->isUpdateEvent(ev)) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_DENY_REQUEST,
				"Deny write operation (Standby mode), eventType=" << EventTypeUtility::getEventTypeName(ev.getType()));
		}
		if (isUpdate) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_DENY_REQUEST,
				"Deny write operation (Standby mode), eventType=" << EventTypeUtility::getEventTypeName(ev.getType()));
		}
	}
	if (isClientNd) {
		if (transactionManager_->getDatabaseManager().getExecutionConstraint(connOption.dbId_, delayTime1, delayTime2, false)) {
			if (delayTime1 > 0 && connOption.retryCount_ == 0) {
				connOption.retryCount_++;
				transactionService_->getEE()->addTimer(ev, delayTime1);
				return true;
			}
		}
	}
	connOption.retryCount_ = 0;
	return false;
}

util::XArray<uint8_t>* StatementHandler::appendDataStoreLog(
	util::StackAllocator& logAlloc,
	TransactionContext& txn,
	const TransactionManager::ContextSource& cxtSrc, StatementId stmtId,
	StoreType storeType, DataStoreLogV4* dsMes) {

	return appendDataStoreLog(logAlloc, txn, txn.getClientId(), cxtSrc, stmtId, storeType, dsMes);
}


util::XArray<uint8_t>* StatementHandler::appendDataStoreLog(
	util::StackAllocator& logAlloc,
	TransactionContext& txn,
	const ClientId& clientId,
	const TransactionManager::ContextSource& cxtSrc, StatementId stmtId,
	StoreType storeType, DataStoreLogV4* dsMes) {
	if (dsMes == NULL) {
		return NULL;
	}

	util::XArray<uint8_t>* binary = ALLOC_NEW(logAlloc) util::XArray<uint8_t>(logAlloc);
	typedef util::ByteStream< util::XArrayOutStream<> > OutStream;
	util::XArrayOutStream<> arrayOut(*binary);
	OutStream out(arrayOut);

	TxnLog dsLog(txn, clientId, cxtSrc, stmtId, storeType, dsMes);
	dsLog.encode<OutStream>(out);

	util::XArray<uint8_t>* logBinary = ALLOC_NEW(logAlloc) util::XArray<uint8_t>(logAlloc);
	const LogType logType = LogType::OBJLog;
	LogManager<MutexLocker>* logMgr = getLogManager(txn.getPartitionId());
	const LogSequentialNumber lsn = logMgr->appendXLog(
			logType, binary->data(), binary->size(), logBinary);
	partitionTable_->setLSN(txn.getPartitionId(), lsn);
	if (dsLog.isTransactionEnd()) {
		const bool byCP = false;
		logMgr->flushXLogByCommit(byCP);
	}
	return logBinary;
}

const char* StatementHandler::AuditEventType(const Event &ev) {
	EventType eventType = ev.getType();
	switch (eventType) {
	case LOGIN: /* LoginHandler */
		return "CONNECT";
	case GET_PARTITION_ADDRESS: /* getPartitionAddressHandler */
		return "GET_PARTITION_ADDRESS";
	case GET_PARTITION_CONTAINER_NAMES: /* getPartitionContainerNamesHandler */
		return "GET_PARTITION_CONTAINER_NAMES";
	case FLUSH_LOG: /* flushLogHandler */
		return "FLUSH";
	case PUT_MULTIPLE_ROWS: /* putRowSetHandler */
		return "PUT_ROW";
	case EXECUTE_MULTIPLE_QUERIES: /* multiQueryHandler */
		return "FETCH_ALL";
	case GET_MULTIPLE_CONTAINER_ROWS: /* multiGetHandler */
		return "MULTI_GET";
	case PUT_MULTIPLE_CONTAINER_ROWS: /* multiPutHandler */
		return "MULTI_PUT";
	case CREATE_TRANSACTION_CONTEXT: /* createTransactionContextHandler */
		return "CREATE_TRANSACTION_CONTEXT";
	case CLOSE_TRANSACTION_CONTEXT: /* CloseTransactionContextHandler */
		return "CLOSE_TRANSACTION_CONTEXT";
	case CREATE_MULTIPLE_TRANSACTION_CONTEXTS: /* multiCreateTransactionContextHandler */
		return "CREATE_MULTIPLE_TRANSACTION_CONTEXTS";
	case CLOSE_MULTIPLE_TRANSACTION_CONTEXTS: /* multiCloseTransactionContextHandler */
		return "CLOSE_MULTIPLE_TRANSACTION_CONTEXTS";
	case DISCONNECT: /* DisconnectHandler */
		return "DISCONNECT";
	case CONNECT: /* ConnectHandler */
	case LOGOUT: /* LogoutHandler */
	case GET_CONTAINER_PROPERTIES: /* getContainerPropertiesHandler */
	case GET_CONTAINER: /* getContainerHandler */
	case PUT_CONTAINER: /* putContainerHandler */
	case DROP_CONTAINER: /* dropContainerHandler */
	case GET_USERS: /* getUsersHandler */
	case PUT_USER: /* putUserHandler */
	case DROP_USER: /* dropUserHandler */
	case GET_DATABASES: /* getDatabasesHandler */
	case PUT_DATABASE: /* putDatabaseHandler */
	case DROP_DATABASE: /* dropDatabaseHandler */
	case PUT_PRIVILEGE: /* putPrivilegeHandler */
	case DROP_PRIVILEGE: /* dropPrivilegeHandler */
	case FETCH_RESULT_SET: /* FetchResultSetHandler */
	case CLOSE_RESULT_SET: /* closeResultSetHandler */
	case CREATE_INDEX: /* createIndexHandler */
	case DELETE_INDEX: /* dropIndexHandler */
	case CREATE_TRIGGER: /* createDropTriggerHandler */
	case DELETE_TRIGGER: /* createDropTriggerHandler */
	case COMMIT_TRANSACTION: /* commitAbortTransactionHandler */
	case ABORT_TRANSACTION: /* commitAbortTransactionHandler */
	case GET_ROW: /* getRowHandler */
	case QUERY_TQL: /* queryTqlHandler */
	case PUT_ROW: /* putRowHandler */
	case UPDATE_DATA_STORE_STATUS: /* updateDataStoreStatusHandler */
	case PUT_LARGE_CONTAINER: /* putLargeContainerHandler */
	case UPDATE_CONTAINER_STATUS: /* updateContainerStatusHandler */
	case UPDATE_ROW_BY_ID: /* updateRowByIdHandler */
	case REMOVE_ROW: /* removeRowHandler */
	case REMOVE_ROW_BY_ID: /* removeRowByIdHandler */
	case QUERY_COLLECTION_GEOMETRY_RELATED: /* queryGeometryRelatedHandler */
	case QUERY_COLLECTION_GEOMETRY_WITH_EXCLUSION: /* queryGeometryWithExclusionHandler */
	case APPEND_TIME_SERIES_ROW: /* appendRowHandler */
	case QUERY_TIME_SERIES_RANGE: /* queryTimeRangeHandler */
	case QUERY_TIME_SERIES_SAMPLING: /* queryTimeSamplingHandler */
	case GET_MULTIPLE_ROWS: /* getRowSetHandler */
	case GET_TIME_SERIES_ROW_RELATED: /* getRowTimeRelatedHandler */
	case INTERPOLATE_TIME_SERIES_ROW: /* getRowInterpolateHandler */
	case AGGREGATE_TIME_SERIES: /* aggregateHandler */
	case REMOVE_MULTIPLE_ROWS_BY_ID_SET: /* RemoveRowSetByIdHandler */
	case UPDATE_MULTIPLE_ROWS_BY_ID_SET: /* UpdateRowSetByIdHandler */
		return "";
	}
	return "";
}

int32_t StatementHandler::AuditCategoryType(const Event &ev) {
	EventType eventType = ev.getType();
	switch (eventType) {
	case LOGIN: /* LoginHandler */
		return 3;
	case GET_PARTITION_ADDRESS: /* getPartitionAddressHandler */
		return 1;
	case GET_PARTITION_CONTAINER_NAMES: /* getPartitionContainerNamesHandler */
		return 1;
	case FLUSH_LOG: /* flushLogHandler */
		return 2;
	case PUT_MULTIPLE_ROWS: /* putRowSetHandler */
		return 2;
	case EXECUTE_MULTIPLE_QUERIES: /* multiQueryHandler */
		return 1;
	case GET_MULTIPLE_CONTAINER_ROWS: /* multiGetHandler */
		return 1;
	case PUT_MULTIPLE_CONTAINER_ROWS: /* multiPutHandler */
		return 2;
	case CREATE_TRANSACTION_CONTEXT: /* createTransactionContextHandler */
		return 1;
	case CLOSE_TRANSACTION_CONTEXT: /* CloseTransactionContextHandler */
		return 2;
	case CREATE_MULTIPLE_TRANSACTION_CONTEXTS: /* multiCreateTransactionContextHandler */
		return 1;
	case CLOSE_MULTIPLE_TRANSACTION_CONTEXTS: /* multiCloseTransactionContextHandler */
		return 2;
	case CONNECT: /* ConnectHandler */
		return 3;
	case DISCONNECT: /* DisconnectHandler */
	case LOGOUT: /* LogoutHandler */
	case GET_CONTAINER_PROPERTIES: /* getContainerPropertiesHandler */
	case GET_CONTAINER: /* getContainerHandler */
	case PUT_CONTAINER: /* putContainerHandler */
	case DROP_CONTAINER: /* dropContainerHandler */
	case GET_USERS: /* getUsersHandler */
	case PUT_USER: /* putUserHandler */
	case DROP_USER: /* dropUserHandler */
	case GET_DATABASES: /* getDatabasesHandler */
	case PUT_DATABASE: /* putDatabaseHandler */
	case DROP_DATABASE: /* dropDatabaseHandler */
	case PUT_PRIVILEGE: /* putPrivilegeHandler */
	case DROP_PRIVILEGE: /* dropPrivilegeHandler */
	case FETCH_RESULT_SET: /* FetchResultSetHandler */
	case CLOSE_RESULT_SET: /* closeResultSetHandler */
	case CREATE_INDEX: /* createIndexHandler */
	case DELETE_INDEX: /* dropIndexHandler */
	case CREATE_TRIGGER: /* createDropTriggerHandler */
	case DELETE_TRIGGER: /* createDropTriggerHandler */
	case COMMIT_TRANSACTION: /* commitAbortTransactionHandler */
	case ABORT_TRANSACTION: /* commitAbortTransactionHandler */
	case GET_ROW: /* getRowHandler */
	case QUERY_TQL: /* queryTqlHandler */
	case PUT_ROW: /* putRowHandler */
	case UPDATE_DATA_STORE_STATUS: /* updateDataStoreStatusHandler */
	case PUT_LARGE_CONTAINER: /* putLargeContainerHandler */
	case UPDATE_CONTAINER_STATUS: /* updateContainerStatusHandler */
	case UPDATE_ROW_BY_ID: /* updateRowByIdHandler */
	case REMOVE_ROW: /* removeRowHandler */
	case REMOVE_ROW_BY_ID: /* removeRowByIdHandler */
	case QUERY_COLLECTION_GEOMETRY_RELATED: /* queryGeometryRelatedHandler */
	case QUERY_COLLECTION_GEOMETRY_WITH_EXCLUSION: /* queryGeometryWithExclusionHandler */
	case APPEND_TIME_SERIES_ROW: /* appendRowHandler */
	case QUERY_TIME_SERIES_RANGE: /* queryTimeRangeHandler */
	case QUERY_TIME_SERIES_SAMPLING: /* queryTimeSamplingHandler */
	case GET_MULTIPLE_ROWS: /* getRowSetHandler */
	case GET_TIME_SERIES_ROW_RELATED: /* getRowTimeRelatedHandler */
	case INTERPOLATE_TIME_SERIES_ROW: /* getRowInterpolateHandler */
	case AGGREGATE_TIME_SERIES: /* aggregateHandler */
	case REMOVE_MULTIPLE_ROWS_BY_ID_SET: /* RemoveRowSetByIdHandler */
	case UPDATE_MULTIPLE_ROWS_BY_ID_SET: /* UpdateRowSetByIdHandler */
		return 0;
	}
	return 0;
}

bool TransactionService::isUpdateEvent(Event& ev) {
	switch (ev.getType()) {
	case PUT_CONTAINER:
	case PUT_LARGE_CONTAINER:
	case UPDATE_CONTAINER_STATUS:
	case CREATE_LARGE_INDEX:
	case DROP_CONTAINER:
	case CREATE_INDEX:
	case DELETE_INDEX:
	case CREATE_TRIGGER:
	case DELETE_TRIGGER:
	case CREATE_TRANSACTION_CONTEXT:
	case CLOSE_TRANSACTION_CONTEXT:
	case COMMIT_TRANSACTION:
	case ABORT_TRANSACTION:
	case PUT_ROW:
	case PUT_MULTIPLE_ROWS:
	case REMOVE_ROW:
	case UPDATE_ROW_BY_ID:
	case REMOVE_ROW_BY_ID:
	case APPEND_TIME_SERIES_ROW:
	case CREATE_MULTIPLE_TRANSACTION_CONTEXTS:
	case CLOSE_MULTIPLE_TRANSACTION_CONTEXTS:
	case PUT_MULTIPLE_CONTAINER_ROWS:
	case PUT_USER:
	case DROP_USER:
	case PUT_DATABASE:
	case DROP_DATABASE:
	case PUT_PRIVILEGE:
	case DROP_PRIVILEGE:
	case CONTINUE_CREATE_INDEX:
	case UPDATE_DATA_STORE_STATUS:
	case REMOVE_MULTIPLE_ROWS_BY_ID_SET:
	case UPDATE_MULTIPLE_ROWS_BY_ID_SET:
		return true;
	case CONNECT:
	case DISCONNECT:
	case LOGIN:
	case LOGOUT:
	case GET_PARTITION_ADDRESS:
	case GET_PARTITION_CONTAINER_NAMES:
	case GET_CONTAINER_PROPERTIES:
	case GET_COLLECTION:
	case INTERPOLATE_TIME_SERIES_ROW:
	case GET_TIME_SERIES_ROW_RELATED:
	case AGGREGATE_TIME_SERIES:
	case FETCH_RESULT_SET:
	case CLOSE_RESULT_SET:
	case GET_MULTIPLE_CONTAINER_ROWS:
	case EXECUTE_MULTIPLE_QUERIES:
	case QUERY_TIME_SERIES_SAMPLING:
	case AUTHENTICATION_ACK:
	case GET_USERS:
	case GET_DATABASES:
	case SQL_GET_CONTAINER:
	case EXECUTE_JOB:
	case SEND_EVENT:
	case AUTHENTICATION:
	case BACK_GROUND:
		return false;
	case GET_ROW: 
	case GET_MULTIPLE_ROWS: 
	case QUERY_TQL: 
	case QUERY_COLLECTION_GEOMETRY_RELATED: 
	case QUERY_COLLECTION_GEOMETRY_WITH_EXCLUSION: 
	case QUERY_TIME_SERIES_RANGE: 
		return false;
	case FLUSH_LOG:
	case REPLICATION_LOG:
	case REPLICATION_ACK:
	case REPLICATION_LOG2:
	case REPLICATION_ACK2:
	case TXN_COLLECT_TIMEOUT_RESOURCE:
	case CHUNK_EXPIRE_PERIODICALLY:
	case ADJUST_STORE_MEMORY_PERIODICALLY:
	case TXN_KEEP_LOG_CHECK_PERIODICALLY:
	case TXN_KEEP_LOG_UPDATE:
	case TXN_KEEP_LOG_RESET:
		return false;
	case TXN_SHORTTERM_SYNC_REQUEST:
	case TXN_SHORTTERM_SYNC_START:
	case TXN_SHORTTERM_SYNC_START_ACK:
	case TXN_SHORTTERM_SYNC_LOG:
	case TXN_SHORTTERM_SYNC_LOG_ACK:
	case TXN_SHORTTERM_SYNC_END:
	case TXN_SHORTTERM_SYNC_END_ACK:
	case TXN_SYNC_TIMEOUT:
	case TXN_SYNC_CHECK_END:
	case TXN_CHANGE_PARTITION_TABLE:
	case TXN_CHANGE_PARTITION_STATE:
	case TXN_DROP_PARTITION:
	case TXN_LONGTERM_SYNC_REQUEST:
	case TXN_LONGTERM_SYNC_START:
	case TXN_LONGTERM_SYNC_START_ACK:
	case TXN_LONGTERM_SYNC_CHUNK:
	case TXN_LONGTERM_SYNC_CHUNK_ACK:
	case TXN_LONGTERM_SYNC_LOG:
	case TXN_LONGTERM_SYNC_LOG_ACK:
	case TXN_LONGTERM_SYNC_PREPARE_ACK:
	case TXN_LONGTERM_SYNC_RECOVERY_ACK:
	case TXN_SYNC_REDO_LOG:
	case TXN_SYNC_REDO_LOG_REPLICATION:
	case TXN_SYNC_REDO_REMOVE:
	case TXN_SYNC_REDO_CHECK:
	case TXN_SYNC_REDO_ERROR:
		return false;
	default:
		return false;
	}
}
