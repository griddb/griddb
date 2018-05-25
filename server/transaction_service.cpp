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

#include "util/container.h"


#define TEST_PRINT(s)
#define TEST_PRINT1(s, d)

#include "base_container.h"
#include "data_store.h"
#include "message_row_store.h"  
#include "message_schema.h"
#include "query_processor.h"
#include "result_set.h"

#include "cluster_service.h"
#include "partition_table.h"
#include "recovery_manager.h"
#include "system_service.h"
#include "trigger_service.h"



#ifndef _WIN32
#include <signal.h>  
#endif




#define TXN_THROW_DENY_ERROR(errorCode, message) \
	GS_THROW_CUSTOM_ERROR(DenyException, errorCode, message)

#define TXN_TRACE_HANDLER_CALLED(ev)                                      \
	UTIL_TRACE_DEBUG(TRANSACTION_SERVICE,                                 \
		"handler called. (type=" << getEventTypeName(ev.getType()) << "[" \
								 << ev.getType() << "]"                   \
								 << ", nd=" << ev.getSenderND()           \
								 << ", pId=" << ev.getPartitionId() << ")")

#define TXN_FAILOVER_STATE_SET(state, eventType, cxtSrc)
#define TXN_FAILOVER_STATE_SET_IF( \
	cond, stateTrue, stateFalse, eventType, cxtSrc)
#define TXN_FAILOVER_STATE_SET_ACK(state, ack)

UTIL_TRACER_DECLARE(IO_MONITOR);
UTIL_TRACER_DECLARE(TRANSACTION_DETAIL);
UTIL_TRACER_DECLARE(DISTRIBUTED_FRAMEWORK);

Sampling StatementHandler::SamplingQuery::toSamplingOption() const {
	Sampling sampling;
	sampling.interval_ = interval_;
	sampling.timeUnit_ = timeUnit_;

	sampling.interpolatedColumnIdList_.resize(interpolatedColumnIdList_.size());
	std::copy(interpolatedColumnIdList_.begin(),
		interpolatedColumnIdList_.end(),
		sampling.interpolatedColumnIdList_.begin());
	sampling.mode_ = mode_;

	return sampling;
}

const StatementHandler::ClusterRole StatementHandler::CROLE_UNKNOWN = 0;
const StatementHandler::ClusterRole StatementHandler::CROLE_SUBMASTER = 1 << 2;
const StatementHandler::ClusterRole StatementHandler::CROLE_MASTER = 1 << 0;
const StatementHandler::ClusterRole StatementHandler::CROLE_FOLLOWER = 1 << 1;
const StatementHandler::ClusterRole StatementHandler::CROLE_ANY = ~0;
const char8_t *const StatementHandler::clusterRoleStr[8] = {"UNKNOWN", "MASTER",
	"FOLLOWER", "MASTER, FOLLOWER", "SUB_CLUSTER", "MASTER, SUB_CLUSTER",
	"FOLLOWER, SUB_CLUSTER", "MASTER, FOLLOWER, SUB_CLUSTER"};

const StatementHandler::PartitionRoleType StatementHandler::PROLE_UNKNOWN = 0;
const StatementHandler::PartitionRoleType StatementHandler::PROLE_OWNER = 1
																		  << 0;
const StatementHandler::PartitionRoleType StatementHandler::PROLE_BACKUP = 1
																		   << 1;
const StatementHandler::PartitionRoleType StatementHandler::PROLE_CATCHUP =
	1 << 2;
const StatementHandler::PartitionRoleType StatementHandler::PROLE_NONE = 1 << 3;
const StatementHandler::PartitionRoleType StatementHandler::PROLE_ANY = ~0;
const char8_t *const StatementHandler::partitionRoleTypeStr[16] = {"UNKNOWN",
	"OWNER", "BACKUP", "OWNER, BACKUP", "CATCHUP", "OWNER, CATCHUP",
	"BACKUP, CATCHUP", "OWNER, BACKUP, CATCHUP", "NONE", "OWNER, NONE",
	"BACKUP, NONE", "OWNER, BACKUP, NONE", "CATCHUP, NONE",
	"OWNER, CATCHUP, NONE", "BACKUP, CATCHUP, NONE",
	"OWNER, BACKUP, CATCHUP, NONE"};

const StatementHandler::PartitionStatus StatementHandler::PSTATE_UNKNOWN = 0;
const StatementHandler::PartitionStatus StatementHandler::PSTATE_ON = 1 << 0;
const StatementHandler::PartitionStatus StatementHandler::PSTATE_SYNC = 1 << 1;
const StatementHandler::PartitionStatus StatementHandler::PSTATE_OFF = 1 << 2;
const StatementHandler::PartitionStatus StatementHandler::PSTATE_STOP = 1 << 3;
const StatementHandler::PartitionStatus StatementHandler::PSTATE_ANY = ~0;
const char8_t *const StatementHandler::partitionStatusStr[16] = {"UNKNOWN",
	"ON", "SYNC", "ON, SYNC", "OFF", "ON, OFF", "SYNC, OFF", "ON, SYNC, OFF",
	"STOP", "ON, STOP", "SYNC, STOP", "ON, SYNC, STOP", "OFF, STOP",
	"ON, OFF, STOP", "SYNC, OFF, STOP", "ON, SYNC, OFF, STOP"};

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
const StatementHandler::ProtocolVersion StatementHandler::TXN_CLIENT_VERSION =
	TXN_V3_0_X_CE_CLIENT_VERSION;

static const StatementHandler::ProtocolVersion
	ACCEPTABLE_NOSQL_CLIENT_VERSIONS[] = {
		StatementHandler::TXN_CLIENT_VERSION,
		StatementHandler::PROTOCOL_VERSION_UNDEFINED /* sentinel */
};

StatementHandler::StatementHandler()
	: clusterService_(NULL),
	  clusterManager_(NULL),
	  chunkManager_(NULL),
	  dataStore_(NULL),
	  logManager_(NULL),
	  partitionTable_(NULL),
	  transactionService_(NULL),
	  transactionManager_(NULL),
	  triggerService_(NULL),
	  systemService_(NULL),
	  recoveryManager_(NULL)
{
}

StatementHandler::~StatementHandler() {}

void StatementHandler::initialize(const ManagerSet &mgrSet) {
	clusterService_ = mgrSet.clsSvc_;
	clusterManager_ = mgrSet.clsMgr_;
	chunkManager_ = mgrSet.chunkMgr_;
	dataStore_ = mgrSet.ds_;
	logManager_ = mgrSet.logMgr_;
	partitionTable_ = mgrSet.pt_;
	transactionService_ = mgrSet.txnSvc_;
	transactionManager_ = mgrSet.txnMgr_;
	triggerService_ = mgrSet.trgSvc_;
	systemService_ = mgrSet.sysSvc_;
	recoveryManager_ = mgrSet.recoveryMgr_;
}

/*!
	@brief Sets the information about success reply to an client
*/
void StatementHandler::setSuccessReply(Event &ev, StatementId stmtId,
	StatementExecStatus status, const Response &response)
{
	try {
		EventByteOutStream out = encodeCommonPart(ev, stmtId, status);
		EventType eventType = ev.getType();

		switch (eventType) {
		case CONNECT:
		case DISCONNECT:
		case LOGIN:
		case LOGOUT:
		case DROP_CONTAINER:
		case CREATE_TRANSACTION_CONTEXT:
		case CLOSE_TRANSACTION_CONTEXT:
		case CREATE_INDEX:
		case DELETE_INDEX:
		case CREATE_TRIGGER:
		case DELETE_TRIGGER:
		case FLUSH_LOG:
		case UPDATE_ROW_BY_ID:
		case REMOVE_ROW_BY_ID:
		case COMMIT_TRANSACTION:
		case ABORT_TRANSACTION:
		case CLOSE_MULTIPLE_TRANSACTION_CONTEXTS:
		case PUT_MULTIPLE_CONTAINER_ROWS:
		case CLOSE_RESULT_SET:
		case GET_PARTITION_ADDRESS:
			encodeBinaryData(
				out, response.binaryData_.data(), response.binaryData_.size());
			break;
		case GET_PARTITION_CONTAINER_NAMES: {
			out << response.containerNum_;
			out << static_cast<uint32_t>(response.containerNameList_.size());
			for (size_t i = 0; i < response.containerNameList_.size(); i++) {
				const util::String *containerName =
					response.containerNameList_[i];
				encodeStringData<util::String>(out, *containerName);
			}
		} break;

		case GET_CONTAINER_PROPERTIES:
			ev.addExtraMessage(
				response.binaryData_.data(), response.binaryData_.size());
			break;

		case GET_CONTAINER:
			encodeBooleanData(out, response.existFlag_);
			if (response.existFlag_) {
				out << response.schemaVersionId_;
				out << response.containerId_;
				out << response.stringData_;
				encodeBinaryData(out, response.binaryData_.data(),
					response.binaryData_.size());
				{
					int32_t containerAttribute =
						static_cast<int32_t>(response.containerAttribute_);
					out << containerAttribute;
				}
			}
			break;

		case PUT_CONTAINER:
			out << response.schemaVersionId_;
			out << response.containerId_;
			out << response.stringData_;
			encodeBinaryData(
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
				encodeBooleanData(out, response.rs_->isRelease());
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
			encodeBooleanData(out, response.existFlag_);
			if (response.existFlag_) {
				encodeBinaryData(out, response.rs_->getFixedStartData(),
					response.rs_->getFixedOffsetSize());
				encodeBinaryData(out, response.rs_->getVarStartData(),
					response.rs_->getVarOffsetSize());
			}
			break;

		case QUERY_TQL:
		case QUERY_TIME_SERIES_RANGE:
		case QUERY_TIME_SERIES_SAMPLING:
			assert(response.rs_ != NULL);
			encodeEnumData<ResultType>(out, response.rs_->getResultType());
			out << response.rs_->getResultNum();
			switch (response.rs_->getResultType()) {
			case RESULT_ROWSET:
			case RESULT_EXPLAIN: {
				ev.addExtraMessage(response.rs_->getFixedStartData(),
					response.rs_->getFixedOffsetSize());
				ev.addExtraMessage(response.rs_->getVarStartData(),
					response.rs_->getVarOffsetSize());
			} break;
			case RESULT_AGGREGATE: {
				const util::XArray<uint8_t> *rowDataFixedPart =
					response.rs_->getRowDataFixedPartBuffer();
				encodeBinaryData(
					out, rowDataFixedPart->data(), rowDataFixedPart->size());
			} break;
			case PARTIAL_RESULT_ROWSET: {
				out << response.rs_->getId();
				out << response.rs_->getFetchNum();
				ev.addExtraMessage(response.rs_->getFixedStartData(),
					response.rs_->getFixedOffsetSize());
				ev.addExtraMessage(response.rs_->getVarStartData(),
					response.rs_->getVarOffsetSize());
			} break;
			default:
				GS_THROW_USER_ERROR(GS_ERROR_TXN_RESULT_TYPE_INVALID,
					"(resultType=" << response.rs_->getResultType() << ")");
			}
			break;

		case PUT_ROW:
		case PUT_MULTIPLE_ROWS:
		case REMOVE_ROW:
		case APPEND_TIME_SERIES_ROW:
			encodeBooleanData(out, response.existFlag_);
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
			encodeBooleanData(out, response.existFlag_);
			if (response.existFlag_) {
				const util::XArray<uint8_t> *rowDataFixedPart =
					response.rs_->getRowDataFixedPartBuffer();
				encodeBinaryData(
					out, rowDataFixedPart->data(), rowDataFixedPart->size());
			}
			break;

		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN,
				"(stmtType=" << ev.getType() << ")");
		}
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief Sets the information about error reply to an client
*/
void StatementHandler::setErrorReply(Event &ev, StatementId stmtId,
	StatementExecStatus status, const std::exception &exception,
	const NodeDescriptor &nd) {
	try {
		EventByteOutStream out = encodeCommonPart(ev, stmtId, status);

		encodeException(out, exception, nd);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief Checks if a user is authenticated
*/
void StatementHandler::checkAuthentication(
	const NodeDescriptor &ND, EventMonotonicTime emNow) {
	TEST_PRINT("checkAuthentication() S\n");

	const ConnectionOption &connOption = ND.getUserData<ConnectionOption>();

	if (!connOption.isAuthenticated_) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_REQUIRED, "");
	}
	TEST_PRINT("checkAuthentication() E\n");
}

/*!
	@brief Checks if immediate consistency is required
*/
void StatementHandler::checkConsistency(
	const NodeDescriptor &ND, bool requireImmediate) {
	const ConnectionOption &connOption = ND.getUserData<ConnectionOption>();
	if (requireImmediate && (connOption.isImmediateConsistency_ != true)) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_CONSISTENCY_TYPE_UNMATCH,
			"immediate consistency required");
	}
}

/*!
	@brief Checks executable status
*/
void StatementHandler::checkExecutable(PartitionId pId,
	ClusterRole requiredClusterRole, PartitionRoleType requiredPartitionRole,
	PartitionStatus requiredPartitionStatus) {
#define TXN_THROW_PT_STATE_UNMATCH_ERROR(                                     \
	errorCode, requiredPartitionStatus, actualPartitionStatus)                \
	TXN_THROW_DENY_ERROR(errorCode,                                           \
		"(required={" << partitionStatusToStr(requiredPartitionStatus) << "}" \
					  << ", actual="                                          \
					  << partitionStatusToStr(actualPartitionStatus) << ")")

	const PartitionTable::PartitionStatus actualPartitionStatus =
		partitionTable_->getPartitionStatus(pId);
	switch (actualPartitionStatus) {
	case PartitionTable::PT_ON:
		if (!(requiredPartitionStatus & PSTATE_ON)) {
			TXN_THROW_PT_STATE_UNMATCH_ERROR(
				GS_ERROR_TXN_PARTITION_STATE_UNMATCH, requiredPartitionStatus,
				PSTATE_ON);
		}
		break;
	case PartitionTable::PT_SYNC:
		if (!(requiredPartitionStatus & PSTATE_SYNC)) {
			TXN_THROW_PT_STATE_UNMATCH_ERROR(
				GS_ERROR_TXN_PARTITION_STATE_UNMATCH, requiredPartitionStatus,
				PSTATE_SYNC);
		}
		break;
	case PartitionTable::PT_OFF:
		if (!(requiredPartitionStatus & PSTATE_OFF)) {
			TXN_THROW_PT_STATE_UNMATCH_ERROR(
				GS_ERROR_TXN_PARTITION_STATE_UNMATCH, requiredPartitionStatus,
				PSTATE_OFF);
		}
		break;
	case PartitionTable::PT_STOP:
		if (!(requiredPartitionStatus & PSTATE_STOP)) {
			TXN_THROW_PT_STATE_UNMATCH_ERROR(
				GS_ERROR_TXN_PARTITION_STATE_UNMATCH, requiredPartitionStatus,
				PSTATE_STOP);
		}
		break;
	default:
		TXN_THROW_PT_STATE_UNMATCH_ERROR(GS_ERROR_TXN_PARTITION_STATE_INVALID,
			requiredPartitionStatus, PSTATE_UNKNOWN);
	}

	const NodeId myself = 0;
	const bool isOwner =
		partitionTable_->isOwner(pId, myself, PartitionTable::PT_CURRENT_OB);
	const bool isBackup =
		partitionTable_->isBackup(pId, myself, PartitionTable::PT_CURRENT_OB);
	const bool isCatchup =
		partitionTable_->isBackup(pId, myself, PartitionTable::PT_CURRENT_GOAL);
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
			"(required={" << partitionRoleTypeToStr(requiredPartitionRole)
						  << "}"
						  << ", actual="
						  << partitionRoleTypeToStr(actualPartitionRole)
						  << ")");
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
			"(required={" << clusterRoleToStr(requiredClusterRole) << "}"
						  << ", actual=" << clusterRoleToStr(actualClusterRole)
						  << ")");
	}

#undef TXN_THROW_PT_STATE_UNMATCH_ERROR
}

/*!
	@brief Checks if transaction timeout
*/
void StatementHandler::checkTransactionTimeout(EventMonotonicTime now,
	EventMonotonicTime queuedTime, int32_t txnTimeoutIntervalSec,
	uint32_t queueingCount) {
	if (now - queuedTime >=
		static_cast<EventMonotonicTime>(txnTimeoutIntervalSec) * 1000) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TIMEOUT,
			"(startTime=" << queuedTime << ", now=" << now
						  << ", interval=" << txnTimeoutIntervalSec
						  << ", retryCount=" << queueingCount << ")");
	}
}

/*!
	@brief Checks if a specified container exists
*/
void StatementHandler::checkContainerExistence(BaseContainer *container) {
	if (container == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_CONTAINER_NOT_FOUND, "");
	}
}

/*!
	@brief Checks the schema version of container
*/
void StatementHandler::checkContainerSchemaVersion(
	BaseContainer *container, SchemaVersionId schemaVersionId) {
	checkContainerExistence(container);

	if (container->getVersionId() != schemaVersionId) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_CONTAINER_SCHEMA_UNMATCH,
			"(containerId=" << container->getContainerId()
							<< ", expectedVer=" << schemaVersionId
							<< ", actualVer=" << container->getVersionId()
							<< ")");
	}
}

/*!
	@brief Checks the version of replication message
*/
void StatementHandler::checkReplicationMessageVersion(
	ProtocolVersion replMsgVersion) {
	if (replMsgVersion < MIN_ACCEPTABLE_REPLICATION_MSG_VERSION ||
		replMsgVersion > MAX_ACCEPTABLE_REPLICATION_MSG_VERSION) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_REPLICATION_MSG_VERSION_NOT_ACCEPTABLE,
			"(receiveVersion=" << replMsgVersion << ", acceptableVersion="
							   << MIN_ACCEPTABLE_REPLICATION_MSG_VERSION << "-"
							   << MAX_ACCEPTABLE_REPLICATION_MSG_VERSION
							   << ")");
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

/*!
	@brief Decodes fixed part of message
*/
void StatementHandler::decodeFixedPart(
	util::ByteStream<util::ArrayInStream> &in, FixedPart &fixedPart) {
	try {
		switch (fixedPart.stmtType_) {
		case CONNECT:
		case DISCONNECT:
		case LOGIN:
		case LOGOUT:
		case GET_PARTITION_ADDRESS:
		case GET_PARTITION_CONTAINER_NAMES:
		case GET_CONTAINER_PROPERTIES:
		case GET_CONTAINER:
		case PUT_CONTAINER:
		case DROP_CONTAINER:
			in >> fixedPart.cxtSrc_.stmtId_;
			break;

		case FLUSH_LOG:
		case CLOSE_RESULT_SET:
			in >> fixedPart.cxtSrc_.stmtId_;
			in >> fixedPart.cxtSrc_.containerId_;
			break;

		case CREATE_INDEX:
		case DELETE_INDEX:
		case CREATE_TRIGGER:
		case DELETE_TRIGGER:
		case FETCH_RESULT_SET:
			in >> fixedPart.cxtSrc_.stmtId_;
			in >> fixedPart.cxtSrc_.containerId_;
			in >> fixedPart.schemaVersionId_;
			break;

		case CREATE_TRANSACTION_CONTEXT:
		case CLOSE_TRANSACTION_CONTEXT:
		case COMMIT_TRANSACTION:
		case ABORT_TRANSACTION:
			in >> fixedPart.cxtSrc_.stmtId_;
			in >> fixedPart.cxtSrc_.containerId_;
			in >> fixedPart.clientId_.sessionId_;
			decodeUUID(
				in, fixedPart.clientId_.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
			break;

		case GET_ROW:
		case QUERY_TQL:
		case PUT_ROW:
		case PUT_MULTIPLE_ROWS:
		case UPDATE_ROW_BY_ID:
		case REMOVE_ROW:
		case REMOVE_ROW_BY_ID:
		case GET_MULTIPLE_ROWS:
		case APPEND_TIME_SERIES_ROW:
		case GET_TIME_SERIES_ROW_RELATED:
		case INTERPOLATE_TIME_SERIES_ROW:
		case AGGREGATE_TIME_SERIES:
		case QUERY_TIME_SERIES_RANGE:
		case QUERY_TIME_SERIES_SAMPLING:
			in >> fixedPart.cxtSrc_.stmtId_;
			in >> fixedPart.cxtSrc_.containerId_;
			in >> fixedPart.clientId_.sessionId_;
			decodeUUID(
				in, fixedPart.clientId_.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
			in >> fixedPart.schemaVersionId_;
			in >> fixedPart.cxtSrc_.getMode_;
			in >> fixedPart.cxtSrc_.txnMode_;
			break;

		case CREATE_MULTIPLE_TRANSACTION_CONTEXTS:
		case CLOSE_MULTIPLE_TRANSACTION_CONTEXTS:
		case EXECUTE_MULTIPLE_QUERIES:
		case GET_MULTIPLE_CONTAINER_ROWS:
		case PUT_MULTIPLE_CONTAINER_ROWS:
			in >> fixedPart.cxtSrc_.stmtId_;
			decodeUUID(
				in, fixedPart.clientId_.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
			break;

		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN,
				"(stmtType=" << fixedPart.stmtType_ << ")");
		}
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes option part of message
*/
void StatementHandler::decodeOptionPart(
	util::ByteStream<util::ArrayInStream> &in, OptionPart &optionPart) {
	try {
		uint32_t totalSize;
		in >> totalSize;

		const size_t endPos = in.base().position() + totalSize;

		while (in.base().position() < endPos) {
			OptionType optionType;

			in >> optionType;

			if (optionType > OPTION_TYPE_MAX) {
				break;
			}

			switch (optionType) {
			case OPTION_TXN_TIMEOUT_INTERVAL:
				in >> optionPart.txnTimeoutInterval_;
				break;
			case OPTION_FOR_UPDATE:
				decodeBooleanData(in, optionPart.forUpdate_);
				break;
			case OPTION_SYSTEM_MODE:
				decodeBooleanData(in, optionPart.systemMode_);
				break;
			case OPTION_DB_NAME:
				optionPart.dbName_.clear();
				in >> optionPart.dbName_;
				break;
			case OPTION_CONTAINER_ATTRIBUTE: {
				uint32_t containerAttribute;
				in >> containerAttribute;
				optionPart.containerAttribute_ =
					static_cast<ContainerAttribute>(containerAttribute);
			} break;
			case OPTION_REQUEST_MODULE_TYPE: {
				char8_t requestType;
				in >> requestType;
				optionPart.requestType_ = static_cast<RequestType>(requestType);
			} break;
			case OPTION_PUT_ROW_OPTION: {
				int8_t putRowOption;
				in >> putRowOption;
				optionPart.putRowOption_ =
					static_cast<PutRowOption>(putRowOption);
			} break;
			default:
				GS_THROW_USER_ERROR(GS_ERROR_TXN_OPTION_TYPE_INVALID,
					"(optionType=" << static_cast<uint32_t>(optionType) << ")");
			}
		}
		in.base().position(endPos);

		if (optionPart.dbName_.size() ==
			0) {  
			optionPart.dbName_ = GS_PUBLIC;
		}
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes option part of message
*/
void StatementHandler::decodeOptionPart(
	util::ByteStream<util::ArrayInStream> &in, ConnectionOption &connOption,
	FixedPart &fixedPart, OptionPart &optionPart) {
	try {
		optionPart.txnTimeoutInterval_ = -1;
		fixedPart.optPart_ = &optionPart;
		optionPart.fixedPart_ = &fixedPart;

		if (!connOption.dbName_.empty()) {
			optionPart.dbName_ = connOption.dbName_.c_str();
		}
		decodeOptionPart(in, optionPart);


		if (optionPart.txnTimeoutInterval_ >= 0) {
			fixedPart.cxtSrc_.txnTimeoutInterval_ =
				optionPart.txnTimeoutInterval_;
		}
		else {
			fixedPart.cxtSrc_.txnTimeoutInterval_ =
				connOption.txnTimeoutInterval_;
			optionPart.txnTimeoutInterval_ =
				TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL;
		}
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes index information
*/
void StatementHandler::decodeIndexInfo(
	util::ByteStream<util::ArrayInStream> &in, IndexInfo &indexInfo) {
	try {
		in >> indexInfo.columnId;
		decodeEnumData<MapType>(in, indexInfo.mapType);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes trigger information
*/
void StatementHandler::decodeTriggerInfo(
	util::ByteStream<util::ArrayInStream> &in, TriggerInfo &triggerInfo) {
	try {
		decodeStringData<util::String>(in, triggerInfo.name_);

		in >> triggerInfo.type_;

		decodeStringData<util::String>(in, triggerInfo.uri_);

		in >> triggerInfo.operation_;

		int32_t columnCount;
		in >> columnCount;

		std::set<ColumnId> columnIdSet;
		for (int32_t i = 0; i < columnCount; i++) {
			ColumnId columnId;
			in >> columnId;
			std::pair<std::set<ColumnId>::iterator, bool> p =
				columnIdSet.insert(columnId);
			if (p.second) {
				triggerInfo.columnIds_.push_back(columnId);
			}
		}

		decodeStringData<util::String>(in, triggerInfo.jmsProviderTypeName_);

		decodeStringData<util::String>(in, triggerInfo.jmsDestinationTypeName_);

		decodeStringData<util::String>(in, triggerInfo.jmsDestinationName_);

		decodeStringData<util::String>(in, triggerInfo.jmsUser_);

		decodeStringData<util::String>(in, triggerInfo.jmsPassword_);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes multiple row data
*/
void StatementHandler::decodeMultipleRowData(
	util::ByteStream<util::ArrayInStream> &in, uint64_t &numRow,
	RowData &rowData) {
	try {
		in >> numRow;
		decodeBinaryData(in, rowData, true);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes fetch option
*/
void StatementHandler::decodeFetchOption(
	util::ByteStream<util::ArrayInStream> &in, FetchOption &fetchOption) {
	try {
		in >> fetchOption.limit_;
		in >> fetchOption.size_;
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}


/*!
	@brief Decodes TimeRelatedCondition
*/
void StatementHandler::decodeTimeRelatedConditon(
	util::ByteStream<util::ArrayInStream> &in,
	TimeRelatedCondition &condition) {
	try {
		in >> condition.rowKey_;
		decodeEnumData<TimeOperator>(in, condition.operator_);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes InterpolateCondition
*/
void StatementHandler::decodeInterpolateConditon(
	util::ByteStream<util::ArrayInStream> &in,
	InterpolateCondition &condition) {
	try {
		in >> condition.rowKey_;
		in >> condition.columnId_;
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes AggregateQuery
*/
void StatementHandler::decodeAggregateQuery(
	util::ByteStream<util::ArrayInStream> &in, AggregateQuery &query) {
	try {
		in >> query.start_;
		in >> query.end_;
		in >> query.columnId_;
		decodeEnumData<AggregationType>(in, query.aggregationType_);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes RangeQuery
*/
void StatementHandler::decodeRangeQuery(
	util::ByteStream<util::ArrayInStream> &in, RangeQuery &query) {
	try {
		in >> query.start_;
		in >> query.end_;
		decodeEnumData<OutputOrder>(in, query.order_);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes SamplingQuery
*/
void StatementHandler::decodeSamplingQuery(
	util::ByteStream<util::ArrayInStream> &in, SamplingQuery &query) {
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
		decodeEnumData<TimeUnit>(in, query.timeUnit_);
		decodeEnumData<InterpolationMode>(in, query.mode_);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes ContainerConditionData
*/
void StatementHandler::decodeContainerConditionData(
	util::ByteStream<util::ArrayInStream> &in,
	DataStore::ContainerCondition &containerCondition) {
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
				static_cast<ContainerAttribute>(CONTAINER_ATTR_BASE));  
		}
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}


/*!
	@brief Decodes boolean data
*/
void StatementHandler::decodeBooleanData(
	util::ByteStream<util::ArrayInStream> &in, bool &boolData) {
	try {
		int8_t tmp;
		in >> tmp;
		boolData = tmp ? true : false;
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes binary data
*/
void StatementHandler::decodeBinaryData(
	util::ByteStream<util::ArrayInStream> &in,
	util::XArray<uint8_t> &binaryData, bool readAll) {
	try {
		uint32_t size;
		if (readAll) {
			size = static_cast<uint32_t>(in.base().remaining());
		}
		else {
			in >> size;
		}
		binaryData.resize(size);
		in >> std::make_pair(binaryData.data(), size);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes VarSize-typed binary data
*/
void StatementHandler::decodeVarSizeBinaryData(
	util::ByteStream<util::ArrayInStream> &in,
	util::XArray<uint8_t> &binaryData) {
	try {
		uint32_t size = MessageRowStore::getVarSize(in);
		binaryData.resize(size);
		in >> std::make_pair(binaryData.data(), size);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes UUID
*/
void StatementHandler::decodeUUID(util::ByteStream<util::ArrayInStream> &in,
	uint8_t *uuid_, size_t uuidSize) {
	try {
		assert(uuid_ != NULL);
		in.readAll(uuid_, uuidSize);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes ReplicationAck
*/
void StatementHandler::decodeReplicationAck(
	util::ByteStream<util::ArrayInStream> &in, ReplicationAck &ack) {


	try {
		in >> ack.clusterMsgVer_;
		in >> ack.replStmtType_;
		in >> ack.replStmtId_;
		in >> ack.clientId_.sessionId_;
		decodeUUID(in, ack.clientId_.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
		in >> ack.replId_;
		in >> ack.replMode_;
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Encodes common part of message
*/
EventByteOutStream StatementHandler::encodeCommonPart(
	Event &ev, StatementId stmtId, StatementExecStatus status) {
	try {
		ev.setPartitionIdSpecified(false);

		EventByteOutStream out = ev.getOutStream();

		out << stmtId;
		out << status;

		return out;
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief Encodes const string data
*/
void StatementHandler::encodeConstStringData(
	EventByteOutStream &out, const char *str) {
	try {
		size_t size = strlen(str);
		out << static_cast<uint32_t>(size);
		out << std::pair<const uint8_t *, size_t>(
			reinterpret_cast<const uint8_t *>(str), size);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief Encodes boolean data
*/
void StatementHandler::encodeBooleanData(
	EventByteOutStream &out, bool boolData) {
	try {
		const uint8_t tmp = boolData ? 1 : 0;
		out << tmp;
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief Encodes binary data
*/
void StatementHandler::encodeBinaryData(
	EventByteOutStream &out, const uint8_t *data, size_t size) {
	try {
		out << std::pair<const uint8_t *, size_t>(data, size);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief Encodes UUID
*/
void StatementHandler::encodeUUID(
	EventByteOutStream &out, const uint8_t *uuid, size_t uuidSize) {
	try {
		assert(uuid != NULL);
		out << std::pair<const uint8_t *, size_t>(uuid, uuidSize);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief Encodes exception infomation
*/
template <typename ByteOutStream>
void StatementHandler::encodeException(
	ByteOutStream &out, const std::exception &exception, bool detailsHidden) {
	const util::Exception e = GS_EXCEPTION_CONVERT(exception, "");

	const uint32_t maxDepth =
		(detailsHidden ? 1 : static_cast<uint32_t>(e.getMaxDepth() + 1));

	out << maxDepth;

	for (uint32_t depth = 0; depth < maxDepth; ++depth) {
		out << e.getErrorCode(depth);

		util::NormalOStringStream oss;

		e.formatMessage(oss, depth);
		out << oss.str();
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
		const char8_t *errorName = e.getNamedErrorCode().getName();
		out << (errorName == NULL ? "" : errorName);
	}
}

/*!
	@brief Encodes exception information
*/
template <typename ByteOutStream>
void StatementHandler::encodeException(ByteOutStream &out,
	const std::exception &exception, const NodeDescriptor &nd) {
	bool detailsHidden;
	if (!nd.isEmpty() && nd.getType() == NodeDescriptor::ND_TYPE_CLIENT) {
		detailsHidden = TXN_DETAIL_EXCEPTION_HIDDEN;
	}
	else {
		detailsHidden = false;
	}

	encodeException(out, exception, detailsHidden);
}

/*!
	@brief Encodes ReplicationAck
*/
void StatementHandler::encodeReplicationAckPart(EventByteOutStream &out,
	uint32_t replMsgVer, int32_t replMode, const ClientId &clientId,
	ReplicationId replId, EventType replStmtType, StatementId replStmtId) {


	try {
		out << replMsgVer;
		out << replStmtType;
		out << replStmtId;
		out << clientId.sessionId_;
		encodeUUID(out, clientId.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
		out << replId;
		out << replMode;
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief Checks if UPDATE statement is requested
*/
bool StatementHandler::isUpdateStatement(EventType stmtType) {
	switch (stmtType) {
	case COMMIT_TRANSACTION:
	case ABORT_TRANSACTION:
	case APPEND_TIME_SERIES_ROW:
	case PUT_MULTIPLE_CONTAINER_ROWS:
	case PUT_ROW:
	case PUT_MULTIPLE_ROWS:
	case UPDATE_ROW_BY_ID:
	case REMOVE_ROW:
	case REMOVE_ROW_BY_ID:
		return true;
		break;
	default:
		return false;
	}
}

/*!
	@brief Replies success message
*/
void StatementHandler::replySuccess(EventContext &ec,
	util::StackAllocator &alloc, const NodeDescriptor &ND, EventType stmtType,
	StatementExecStatus status, const FixedPart &request,
	const Response &response, bool ackWait) {

	util::StackAllocator::Scope scope(alloc);

	{
		if (!ackWait) {
			Event ev(ec, stmtType, request.pId_);
			setSuccessReply(ev, request.cxtSrc_.stmtId_, status, response);

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
}

/*!
	@brief Replies success message
*/
void StatementHandler::replySuccess(EventContext &ec,
	util::StackAllocator &alloc, StatementExecStatus status,
	const ReplicationContext &replContext) {
#define TXN_TRACE_REPLY_SUCCESS_ERROR(replContext)             \
	"(nd=" << replContext.getConnectionND()                    \
		   << ", pId=" << replContext.getPartitionId()         \
		   << ", eventType=" << replContext.getStatementType() \
		   << ", stmtId=" << replContext.getStatementId()      \
		   << ", clientId=" << replContext.getClientId()       \
		   << ", containerId=" << replContext.getContainerId() << ")"

	util::StackAllocator::Scope scope(alloc);
	const util::DateTime &now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	if (replContext.getConnectionND().isEmpty()) {
		return;
	}

	try {
		Response response(alloc);

		switch (replContext.getStatementType()) {
		case PUT_CONTAINER: {
			TransactionManager::ContextSource src;
			src.stmtId_ = replContext.getStatementId();
			src.containerId_ = replContext.getContainerId();
			TransactionContext &txn =
				transactionManager_->put(alloc, replContext.getPartitionId(),
					TXN_EMPTY_CLIENTID, src, now, emNow);

			ContainerAutoPtr containerAutoPtr(txn, dataStore_,
				txn.getPartitionId(), txn.getContainerId(), ANY_CONTAINER);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			checkContainerExistence(container);

			response.schemaVersionId_ = container->getVersionId();
			response.containerId_ = container->getContainerId();
			ExtendedContainerName *extContainerName;
			container->getExtendedContainerName(txn, extContainerName);
			const char8_t *name = extContainerName->getContainerName();

			uint32_t size = static_cast<uint32_t>(strlen(name));
			response.stringData_.append(name, size);
			const bool optionIncluded = true;
			container->getContainerInfo(
				txn, response.binaryData_, optionIncluded);
		} break;

		case DROP_CONTAINER:
		case CLOSE_TRANSACTION_CONTEXT:
		case CREATE_INDEX:
		case DELETE_INDEX:
		case CREATE_TRIGGER:
		case DELETE_TRIGGER:
		case COMMIT_TRANSACTION:
		case ABORT_TRANSACTION:
		case UPDATE_ROW_BY_ID:
		case REMOVE_ROW_BY_ID:
			break;

		case PUT_ROW:
		case REMOVE_ROW:
		case PUT_MULTIPLE_ROWS:
		case APPEND_TIME_SERIES_ROW:
			{ response.existFlag_ = replContext.getExistFlag(); }
			break;

		case CLOSE_MULTIPLE_TRANSACTION_CONTEXTS:
		case PUT_MULTIPLE_CONTAINER_ROWS:
			break;

		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN,
				"(stmtType=" << replContext.getStatementType() << ")");
		}

		{
			Event ev(ec, replContext.getStatementType(),
				replContext.getPartitionId());
			const FixedPart request(0, 0);

			setSuccessReply(ev, replContext.getStatementId(), status, response);

			ec.getEngine().send(ev, replContext.getConnectionND());

			GS_TRACE_INFO(REPLICATION, GS_TRACE_TXN_REPLY_CLIENT,
				TXN_TRACE_REPLY_SUCCESS_ERROR(replContext));
		}
	}
	catch (EncodeDecodeException &e) {
		GS_RETHROW_USER_ERROR(e, TXN_TRACE_REPLY_SUCCESS_ERROR(replContext));
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, TXN_TRACE_REPLY_SUCCESS_ERROR(replContext));
	}

#undef TXN_TRACE_REPLY_SUCCESS_ERROR
}

/*!
	@brief Replies error message
*/
void StatementHandler::replyError(EventContext &ec, util::StackAllocator &alloc,
	const NodeDescriptor &ND, EventType stmtType, StatementExecStatus status,
	const FixedPart &request, const std::exception &e) {
	util::StackAllocator::Scope scope(alloc);

	{
		Event ev(ec, stmtType, request.pId_);
		setErrorReply(ev, request.cxtSrc_.stmtId_, status, e, ND);

		if (!ND.isEmpty()) {
			ec.getEngine().send(ev, ND);
		}
	}
}

/*!
	@brief Executes replication
*/
bool StatementHandler::executeReplication(const FixedPart &request,
	EventContext &ec, util::StackAllocator &alloc,
	const NodeDescriptor &clientND, TransactionContext &txn,
	EventType replStmtType, StatementId replStmtId, int32_t replMode,
	const ClientId *closedResourceIds, size_t closedResourceIdCount,
	const util::XArray<uint8_t> **logRecordList, size_t logRecordCount,
	bool rowExistFlag) {
#define TXN_TRACE_REPLICATION_ERROR(replMode, txn, replStmtType, replStmtId, \
									closedResourceIdCount, logRecordCount)   \
	"(mode=" << replMode << ", pId=" << txn.getPartitionId()                 \
			 << ", eventType=" << replStmtType << ", stmtId=" << replStmtId  \
			 << ", clientId=" << txn.getClientId()                           \
			 << ", containerId=" << txn.getContainerId()                     \
			 << ", closedResourceIdCount=" << closedResourceIdCount          \
			 << ", logRecordCount=" << logRecordCount << ")"

	util::StackAllocator::Scope scope(alloc);
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	ReplicationId replId = 0;

	if (logRecordCount == 0 && closedResourceIdCount == 0) {
		return false;
	}

	try {
		util::XArray<NodeId> backupNodeIdList(alloc);
		partitionTable_->getBackup(txn.getPartitionId(), backupNodeIdList);

		if (backupNodeIdList.empty()) {
			return false;
		}

		if (replMode == TransactionManager::REPLICATION_SEMISYNC) {
			TransactionManager::ContextSource ctxSrc(replStmtType);
			ctxSrc.containerId_ = txn.getContainerId();
			ctxSrc.stmtId_ = replStmtId;


			ReplicationContext &replContext =
				transactionManager_->put(txn.getPartitionId(),
					txn.getClientId(), ctxSrc, clientND, emNow);


			replId = replContext.getReplicationId();
			replContext.setExistFlag(rowExistFlag);
			replContext.incrementAckCounter(
				static_cast<uint32_t>(backupNodeIdList.size()));
		}

		Event replEvent(ec, REPLICATION_LOG, txn.getPartitionId());
		EventByteOutStream out = replEvent.getOutStream();
		encodeReplicationAckPart(out, REPLICATION_MSG_VERSION, replMode,
			txn.getClientId(), replId, replStmtType, replStmtId);
		encodeIntData<uint32_t>(
			out, static_cast<uint32_t>(closedResourceIdCount));
		for (size_t i = 0; i < closedResourceIdCount; i++) {
			encodeLongData<SessionId>(out, closedResourceIds[i].sessionId_);
			encodeUUID(
				out, closedResourceIds[i].uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
		}
		encodeIntData<uint32_t>(out, static_cast<uint32_t>(logRecordCount));
		for (size_t i = 0; i < logRecordCount; i++) {
			encodeBinaryData(
				out, logRecordList[i]->data(), logRecordList[i]->size());
		}

		for (size_t i = 0; i < backupNodeIdList.size(); i++) {
			const NodeDescriptor &backupND =
				transactionService_->getEE()->getServerND(backupNodeIdList[i]);
			ec.getEngine().send(replEvent, backupND);
		}

		GS_TRACE_INFO(REPLICATION, GS_TRACE_TXN_SEND_LOG,
			TXN_TRACE_REPLICATION_ERROR(replMode, txn, replStmtType, replStmtId,
				closedResourceIdCount, closedResourceIdCount)
				<< " (numBackup=" << backupNodeIdList.size()
				<< ", replId=" << replId << ")");

		return (replMode == TransactionManager::REPLICATION_SEMISYNC);
	}
	catch (EncodeDecodeException &e) {
		TXN_RETHROW_ENCODE_ERROR(
			e, TXN_TRACE_REPLICATION_ERROR(replMode, txn, replStmtType,
				   replStmtId, closedResourceIdCount, closedResourceIdCount));
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, TXN_TRACE_REPLICATION_ERROR(replMode, txn, replStmtType,
				   replStmtId, closedResourceIdCount, closedResourceIdCount));
	}

#undef TXN_TRACE_REPLICATION_ERROR
}

/*!
	@brief Replies ReplicationAck
*/
void StatementHandler::replyReplicationAck(EventContext &ec,
	util::StackAllocator &alloc, const NodeDescriptor &ND,
	const ReplicationAck &request) {
#define TXN_TRACE_REPLY_ACK_ERROR(request, ND)                             \
	"(mode=" << request.replMode_ << ", owner=" << ND                      \
			 << ", replId=" << request.replId_ << ", pId=" << request.pId_ \
			 << ", eventType=" << request.replStmtType_                    \
			 << ", stmtId=" << request.replStmtId_                         \
			 << ", clientId=" << request.clientId_ << ")"

	util::StackAllocator::Scope scope(alloc);

	if (request.replMode_ != TransactionManager::REPLICATION_SEMISYNC) {
		return;
	}

	try {
		Event replAckEvent(ec, REPLICATION_ACK, request.pId_);
		EventByteOutStream out = replAckEvent.getOutStream();
		encodeReplicationAckPart(out, request.clusterMsgVer_, request.replMode_,
			request.clientId_, request.replId_, request.replStmtType_,
			request.replStmtId_);

		ec.getEngine().send(replAckEvent, ND);

		GS_TRACE_INFO(REPLICATION, GS_TRACE_TXN_SEND_ACK,
			TXN_TRACE_REPLY_ACK_ERROR(request, ND));
	}
	catch (EncodeDecodeException &e) {
		TXN_RETHROW_ENCODE_ERROR(e, TXN_TRACE_REPLY_ACK_ERROR(request, ND));
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, TXN_TRACE_REPLY_ACK_ERROR(request, ND));
	}

#undef TXN_TRACE_REPLY_ACK_ERROR
}

/*!
	@brief Handles error
*/
void StatementHandler::handleError(EventContext &ec,
	util::StackAllocator &alloc, Event &ev, const FixedPart &request,
	std::exception &) {
#define TXN_TRACE_ON_ERROR_MINIMUM(cause, category, ev, request, extra)      \
	GS_EXCEPTION_MESSAGE(cause)                                              \
		<< " (" << category << ", nd=" << ev.getSenderND()                   \
		<< ", pId=" << ev.getPartitionId() << ", eventType=" << ev.getType() \
		<< extra << ")"

#define TXN_TRACE_ON_ERROR(cause, category, ev, request)                      \
	TXN_TRACE_ON_ERROR_MINIMUM(cause, category, ev, request,                  \
		", stmtId=" << request.cxtSrc_.stmtId_                                \
					<< ", clientId=" << request.clientId_ << ", containerId=" \
					<< ((request.cxtSrc_.containerId_ == UNDEF_CONTAINERID)   \
							   ? 0                                            \
							   : request.cxtSrc_.containerId_))

	util::StackAllocator::Scope scope(alloc);

	try {
		try {
			throw;
		}
		catch (StatementAlreadyExecutedException &e) {
			UTIL_TRACE_EXCEPTION_INFO(TRANSACTION_SERVICE, e,
				TXN_TRACE_ON_ERROR(e, "Statement already executed", ev,
										  request));
			Response response(alloc);
			response.existFlag_ = false;
			replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
		}
		catch (EncodeDecodeException &e) {
			UTIL_TRACE_EXCEPTION(
				TRANSACTION_SERVICE, e,
				TXN_TRACE_ON_ERROR_MINIMUM(
					e, "Failed to encode or decode message", ev, request, ""));
		}
		catch (LockConflictException &e) {
			UTIL_TRACE_EXCEPTION_INFO(TRANSACTION_SERVICE, e,
				TXN_TRACE_ON_ERROR(e, "Lock conflicted", ev, request));
			try {
				TransactionContext &txn = transactionManager_->get(
					alloc, ev.getPartitionId(), request.clientId_);

				util::XArray<const util::XArray<uint8_t> *> logRecordList(
					alloc);

				if (request.cxtSrc_.txnMode_ ==
					TransactionManager::NO_AUTO_COMMIT_BEGIN) {
					util::XArray<uint8_t> *log =
						ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
					if (abortOnError(txn, *log)) {
						logRecordList.push_back(log);

						if (ev.getType() == PUT_MULTIPLE_ROWS) {
							assert(request.startStmtId_ != UNDEF_STATEMENTID);
							transactionManager_->update(
								txn, request.startStmtId_);
						}
					}
				}

				executeReplication(request, ec, alloc, NodeDescriptor::EMPTY_ND,
					txn, ABORT_TRANSACTION, request.cxtSrc_.stmtId_,
					TransactionManager::REPLICATION_ASYNC, NULL, 0,
					logRecordList.data(), logRecordList.size());

				if (request.cxtSrc_.getMode_ == TransactionManager::CREATE) {
					transactionManager_->remove(
						request.pId_, request.clientId_);
				}
			}
			catch (ContextNotFoundException &) {
				UTIL_TRACE_EXCEPTION_INFO(TRANSACTION_SERVICE, e,
					TXN_TRACE_ON_ERROR(e, "Context not found", ev, request));
			}

			ec.getEngine().addPending(
				ev, EventEngine::RESUME_AT_PARTITION_CHANGE);
		}
		catch (DenyException &e) {
			UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
				TXN_TRACE_ON_ERROR(e, "Request denied", ev, request));
			replyError(ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_DENY, request, e);
		}
		catch (ContextNotFoundException &e) {
			UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
				TXN_TRACE_ON_ERROR(e, "Context not found", ev, request));
			replyError(ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_ERROR, request, e);
		}
		catch (std::exception &e) {
			if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
				throw;
			}
			else {
				TXN_FAILOVER_STATE_SET(
					FailoverTestUtil::STATE_10, ev.getType(), request.cxtSrc_);

				const util::Exception utilException =
					GS_EXCEPTION_CONVERT(e, "");

				if (ev.getType() == ABORT_TRANSACTION &&
					utilException.getErrorCode(utilException.getMaxDepth()) ==
						GS_ERROR_TM_TRANSACTION_NOT_FOUND) {
					UTIL_TRACE_EXCEPTION_WARNING(
						TRANSACTION_SERVICE, e,
						TXN_TRACE_ON_ERROR(e,
							"Transaction is already timed out", ev, request));
					Response response(alloc);
					replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
						TXN_STATEMENT_SUCCESS, request, response,
						NO_REPLICATION);
				}
				else {
					UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
						TXN_TRACE_ON_ERROR(e, "User error", ev, request));
					try {
						TransactionContext &txn = transactionManager_->get(
							alloc, ev.getPartitionId(), request.clientId_);

						util::XArray<uint8_t> *log =
							ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
						if (abortOnError(txn, *log)) {
							util::XArray<const util::XArray<uint8_t> *>
								logRecordList(alloc);
							logRecordList.push_back(log);

							TXN_FAILOVER_STATE_SET(FailoverTestUtil::STATE_11,
								ev.getType(), request.cxtSrc_);

							executeReplication(request, ec, alloc,
								NodeDescriptor::EMPTY_ND, txn,
								ABORT_TRANSACTION, request.cxtSrc_.stmtId_,
								TransactionManager::REPLICATION_ASYNC, NULL, 0,
								logRecordList.data(), logRecordList.size());
						}
					}
					catch (ContextNotFoundException &) {
						UTIL_TRACE_EXCEPTION_INFO(TRANSACTION_SERVICE, e,
							TXN_TRACE_ON_ERROR(e, "Context not found", ev,
													  request));
					}

					TXN_FAILOVER_STATE_SET(FailoverTestUtil::STATE_12,
						ev.getType(), request.cxtSrc_);

					replyError(ec, alloc, ev.getSenderND(), ev.getType(),
						TXN_STATEMENT_ERROR, request, e);
				}
			}
		}
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			TXN_TRACE_ON_ERROR(e, "System error", ev, request));
		replyError(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_NODE_ERROR, request, e);
		clusterService_->setError(ec, &e);
	}

#undef TXN_TRACE_ON_ERROR
#undef TXN_TRACE_ON_ERROR_MINIMUM
}

/*!
	@brief Aborts on error
*/
bool StatementHandler::abortOnError(
	TransactionContext &txn, util::XArray<uint8_t> &log) {
	log.clear();

	if (txn.isActive()) {
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		BaseContainer *container = NULL;

		try {
			container = dataStore_->getContainer(
				txn, txn.getPartitionId(), txn.getContainerId(), ANY_CONTAINER);
		}
		catch (UserException &e) {
			UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
				"Container not found. (pId="
					<< txn.getPartitionId()
					<< ", clientId=" << txn.getClientId()
					<< ", containerId=" << txn.getContainerId()
					<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
		}
		StackAllocAutoPtr<BaseContainer> containerAutoPtr(
			txn.getDefaultAllocator(), container);

		if (containerAutoPtr.get() != NULL) {

			const LogSequentialNumber lsn = logManager_->putAbortTransactionLog(
				log, txn.getPartitionId(), txn.getClientId(), txn.getId(),
				txn.getContainerId(), txn.getLastStatementId());
			partitionTable_->setLSN(txn.getPartitionId(), lsn);

			transactionManager_->abort(txn, *containerAutoPtr.get());

		}
		else {
			transactionManager_->remove(txn);
		}
	}

	return !log.empty();
}

/*!
	@brief Gets access mode to container
*/
ContainerAccessMode StatementHandler::getContainerAccessMode(UserType userType,
	bool isSystemMode, RequestType requestType, bool isWriteMode) {
	ContainerAccessMode mode = UNDEF_ACCESS_MODE;
	if (isSystemMode && requestType == StatementHandler::NOSQL) {
		mode = NOSQL_SYSTEM_ACCESS_MODE;
	}
	else if (!isSystemMode && requestType == StatementHandler::NOSQL) {
		mode = NOSQL_NORMAL_ACCESS_MODE;
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_PARAMETER_INVALID,
			"Illeagal parameter. RequestType = " << requestType);
	}
	return mode;
}

/*!
	@brief Checks if accessible
*/
bool StatementHandler::isAccessibleMode(
	ContainerAccessMode containerAccessMode, ContainerAttribute attribute) {
	bool isAccessible = false;
	switch (containerAccessMode) {
	case NOSQL_SYSTEM_ACCESS_MODE:
	case NOSQL_NORMAL_ACCESS_MODE: {
		switch (attribute) {
		case CONTAINER_ATTR_BASE:
			isAccessible = true;
			break;
		default:
			isAccessible = false;
			break;
		}
	} break;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_PARAMETER_INVALID,
			"Illeagal parameter. ContainerAccessMode = "
				<< containerAccessMode);
		break;
	}
	return isAccessible;
}

void StatementHandler::checkDbAccessible(
	const std::string &loginDbName, const util::String &specifiedDbName) const {
	if (specifiedDbName.compare(loginDbName.c_str()) != 0) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_CON_ACCESS_INVALID,
			"Only can access connect db :"
				<< loginDbName << ", can not access db : " << specifiedDbName);
	}
}

const char8_t *StatementHandler::clusterRoleToStr(
	StatementHandler::ClusterRole role) {
	const ClusterRole allStatesSet =
		CROLE_MASTER + CROLE_FOLLOWER + CROLE_SUBMASTER;
	return role > allStatesSet ? clusterRoleStr[allStatesSet]
							   : clusterRoleStr[role];
}

const char8_t *StatementHandler::partitionRoleTypeToStr(
	StatementHandler::PartitionRoleType role) {
	const PartitionRoleType allStatesSet =
		PROLE_OWNER + PROLE_BACKUP + PROLE_CATCHUP + PROLE_NONE;
	return role > allStatesSet ? partitionRoleTypeStr[allStatesSet]
							   : partitionRoleTypeStr[role];
}

const char8_t *StatementHandler::partitionStatusToStr(
	StatementHandler::PartitionStatus status) {
	const PartitionStatus allStatesSet =
		PSTATE_ON + PSTATE_SYNC + PSTATE_OFF + PSTATE_STOP;
	return status > allStatesSet ? partitionStatusStr[allStatesSet]
								 : partitionStatusStr[status];
}

ConnectHandler::ConnectHandler(ProtocolVersion currentVersion,
	const ProtocolVersion *acceptableProtocolVersons)
	: currentVersion_(currentVersion),
	  acceptableProtocolVersons_(acceptableProtocolVersons) {}

/*!
	@brief Handler Operator
*/
void ConnectHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();

	ConnectRequest request(ev.getPartitionId(), ev.getType());
	Response response(alloc);

	try {

		EventByteInStream in(ev.getInStream());

		decodeIntData<OldStatementId>(in, request.oldStmtId_);
		decodeIntData<ProtocolVersion>(in, request.clientVersion_);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_ANY;
		const PartitionStatus partitionStatus = PSTATE_ANY;
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);

		checkClientVersion(request.clientVersion_);

		ConnectionOption &connOption =
			ev.getSenderND().getUserData<ConnectionOption>();
		connOption.clientVersion_ = request.clientVersion_;

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void ConnectHandler::checkClientVersion(ProtocolVersion clientVersion) {
	for (const ProtocolVersion *acceptableVersions = acceptableProtocolVersons_;
		 ; acceptableVersions++) {
		if (clientVersion == *acceptableVersions) {
			return;
		}
		else if (*acceptableVersions == PROTOCOL_VERSION_UNDEFINED) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_CLIENT_VERSION_NOT_ACCEPTABLE,
				"(connectClientVersion=" << clientVersion << ", serverVersion="
										 << currentVersion_ << ")");
		}
	}
}

EventByteOutStream ConnectHandler::encodeCommonPart(
	Event &ev, OldStatementId stmtId, StatementExecStatus status) {
	try {
		ev.setPartitionIdSpecified(false);

		EventByteOutStream out = ev.getOutStream();

		out << stmtId;
		out << status;

		return out;
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void ConnectHandler::replySuccess(EventContext &ec, util::StackAllocator &alloc,
	const NodeDescriptor &ND, EventType stmtType, StatementExecStatus status,
	const ConnectRequest &request, const Response &, bool) {
	util::StackAllocator::Scope scope(alloc);

	Event ev(ec, stmtType, request.pId_);
	encodeCommonPart(ev, request.oldStmtId_, status);


	if (ev.getExtraMessageCount() > 0) {
		EventEngine::EventRequestOption option;
		option.timeoutMillis_ = 0;
		ec.getEngine().send(ev, ND, &option);
	}
	else {
		ec.getEngine().send(ev, ND);
	}
}

void ConnectHandler::replyError(EventContext &ec, util::StackAllocator &alloc,
	const NodeDescriptor &ND, EventType stmtType, StatementExecStatus status,
	const ConnectRequest &request, const std::exception &e) {
	util::StackAllocator::Scope scope(alloc);

	Event ev(ec, stmtType, request.pId_);
	EventByteOutStream out = encodeCommonPart(ev, request.oldStmtId_, status);
	encodeException(out, e, ND);

	if (!ND.isEmpty()) {
		ec.getEngine().send(ev, ND);
	}
}

void ConnectHandler::handleError(EventContext &ec, util::StackAllocator &alloc,
	Event &ev, const ConnectRequest &request, std::exception &) {
#define TXN_TRACE_ON_ERROR_MINIMUM(cause, category, ev, request, extra)      \
	GS_EXCEPTION_MESSAGE(cause)                                              \
		<< " (" category << ", nd=" << ev.getSenderND()                      \
		<< ", pId=" << ev.getPartitionId() << ", eventType=" << ev.getType() \
		<< extra << ")"

#define TXN_TRACE_ON_ERROR(cause, category, ev, request) \
	TXN_TRACE_ON_ERROR_MINIMUM(                          \
		cause, category, ev, request, ", stmtId=" << request.oldStmtId_)

	util::StackAllocator::Scope scope(alloc);

	try {
		try {
			throw;

		}
		catch (EncodeDecodeException &e) {
			UTIL_TRACE_EXCEPTION(
				TRANSACTION_SERVICE, e,
				TXN_TRACE_ON_ERROR_MINIMUM(
					e, "Failed to encode or decode message", ev, request, ""));

		}
		catch (DenyException &e) {
			UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
				TXN_TRACE_ON_ERROR(e, "Request denied", ev, request));
			replyError(ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_DENY, request, e);

		}
		catch (std::exception &e) {
			if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
				throw;
			}
			else {
				UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
					TXN_TRACE_ON_ERROR(e, "User error", ev, request));
				replyError(ec, alloc, ev.getSenderND(), ev.getType(),
					TXN_STATEMENT_ERROR, request, e);
			}
		}
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			TXN_TRACE_ON_ERROR(e, "System error", ev, request));
		replyError(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_NODE_ERROR, request, e);
		clusterService_->setError(ec, &e);
	}

#undef TXN_TRACE_ON_ERROR
#undef TXN_TRACE_ON_ERROR_MINIMUM
}

/*!
	@brief Handler Operator
*/
void DisconnectHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();

	FixedPart request(ev.getPartitionId(), ev.getType());

	try {

		ConnectionOption &connOption =
			ev.getSenderND().getUserData<ConnectionOption>();
		connOption.clear();

	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void LoginHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	TEST_PRINT("<<<LoginHandler>>> START\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		ConnectionOption &connOption =
			ev.getSenderND().getUserData<ConnectionOption>();
		connOption.clear();

		EventByteInStream in(ev.getInStream());
		decodeFixedPart(in, request);
		decodeOptionPart(in, connOption, request, optionPart);

		util::String userName(alloc);
		util::String digest(alloc);
		std::string clusterName;
		int32_t stmtTimeoutInterval;
		bool isImmediateConsistency;

		decodeStringData<util::String>(in, userName);
		decodeStringData<util::String>(in, digest);
		decodeIntData<int32_t>(in,
			stmtTimeoutInterval);  
		decodeBooleanData(in, isImmediateConsistency);
		if (in.base().remaining() > 0) {
			decodeStringData<std::string>(in, clusterName);
		}
		TEST_PRINT("[RequestMesg]\n");
		TEST_PRINT1("userName=%s\n", userName.c_str());
		TEST_PRINT1("digest=%s\n", digest.c_str());
		TEST_PRINT1("stmtTimeoutInterval=%d\n", stmtTimeoutInterval);
		TEST_PRINT1("isImmediateConsistency=%d\n", isImmediateConsistency);
		TEST_PRINT1("clusterName=%s\n", clusterName.c_str());

		TEST_PRINT1("requestType=%d\n", optionPart.requestType_);
		TEST_PRINT1("dbName=%s\n", optionPart.dbName_.c_str());
		TEST_PRINT1("systemMode=%d\n", optionPart.systemMode_);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_ANY;
		const PartitionStatus partitionStatus = PSTATE_ANY;
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);

		if (clusterName.size() > 0) {
			const std::string currentClusterName =
				clusterService_->getClusterName();
			if (clusterName.compare(currentClusterName) != 0) {
				TXN_THROW_DENY_ERROR(GS_ERROR_TXN_CLUSTER_NAME_INVALID,
					"cluster name invalid (input="
						<< clusterName << ", current=" << currentClusterName
						<< ")");
			}
		}

		if (strcmp(optionPart.dbName_.c_str(), GS_PUBLIC) == 0) {
			if ((strcmp(userName.c_str(), GS_ADMIN_USER) == 0) ||
				(strcmp(userName.c_str(), GS_SYSTEM_USER) == 0) ||
				(userName.find('#') != std::string::npos)) {
				TEST_PRINT("LoginHandler::operator() public&admin S\n");

				if (systemService_->getUserTable().isValidUser(
						userName.c_str(), digest.c_str())) {  

					connOption.isImmediateConsistency_ = isImmediateConsistency;
					connOption.txnTimeoutInterval_ =
						(optionPart.txnTimeoutInterval_ >= 0)
							? optionPart.txnTimeoutInterval_
							: TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL;
					connOption.userType_ = ADMIN;
					if (strcmp(optionPart.dbName_.c_str(), GS_SYSTEM) == 0) {
						GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED,
							"database name invalid (" << optionPart.dbName_
													  << ")");
					}

					connOption.userName_ = userName.c_str();
					connOption.dbName_ = optionPart.dbName_.c_str();
					connOption.requestType_ = optionPart.requestType_;
					connOption.isAdminAndPublicDB_ = true;
					connOption.isAuthenticated_ = true;
					connOption.dbId_ = GS_PUBLIC_DB_ID;
					connOption.authenticationTime_ = emNow;

					replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
						TXN_STATEMENT_SUCCESS, request, response,
						NO_REPLICATION);
					TEST_PRINT("LoginHandler::operator() public&admin E\n");
				}
				else {
					GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED,
						"invalid user name or password (user mame = "
							<< userName.c_str() << ")");
				}
			}
			else {
				GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED,
					"user name invalid (" << userName.c_str() << ")");
			}
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED,
				"database name invalid (" << optionPart.dbName_ << ")");
		}

		TEST_PRINT("<<<LoginHandler>>> END\n");
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void LogoutHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);


		ConnectionOption &connOption =
			ev.getSenderND().getUserData<ConnectionOption>();
		connOption.clear();

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void GetPartitionAddressHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		int8_t masterResolving = 0;
		if (in.base().remaining() > 0) {
			decodeIntData<int8_t>(in, masterResolving);
		}

		const ClusterRole clusterRole =
			CROLE_MASTER | (masterResolving ? CROLE_FOLLOWER : 0);
		const PartitionRoleType partitionRole = PROLE_ANY;
		const PartitionStatus partitionStatus = PSTATE_ANY;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), ANY_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);

		util::XArrayOutStream<> arrayOut(response.binaryData_);
		util::ByteStream<util::XArrayOutStream<> > out(arrayOut);

		out << partitionTable_->getPartitionNum();

		const NodeId ownerNodeId =
			(masterResolving ? UNDEF_NODEID
							 : partitionTable_->getOwner(request.pId_));
		if (ownerNodeId != UNDEF_NODEID) {
			out << static_cast<uint8_t>(1);
			const NodeDescriptor &ownerND =
				transactionService_->getEE()->getServerND(ownerNodeId);
			util::SocketAddress::Inet addr;
			uint16_t port;
			ownerND.getAddress().getIP(&addr, &port);
			out << std::pair<const uint8_t *, size_t>(
				reinterpret_cast<const uint8_t *>(&addr), sizeof(addr));
			out << static_cast<uint32_t>(port);
		}
		else {
			out << static_cast<uint8_t>(0);
		}

		util::XArray<NodeId> backupNodeIds(alloc);
		if (!masterResolving) {
			partitionTable_->getBackup(request.pId_, backupNodeIds);
		}

		const uint8_t numBackup = static_cast<uint8_t>(backupNodeIds.size());
		out << numBackup;
		for (uint8_t i = 0; i < numBackup; i++) {
			const NodeDescriptor &backupND =
				transactionService_->getEE()->getServerND(backupNodeIds[i]);
			util::SocketAddress::Inet addr;
			uint16_t port;
			backupND.getAddress().getIP(&addr, &port);
			out << std::pair<const uint8_t *, size_t>(
				reinterpret_cast<const uint8_t *>(&addr), sizeof(addr));
			out << static_cast<uint32_t>(port);
		}

		if (masterResolving) {
			const NodeId masterNodeId = partitionTable_->getMaster();
			const NodeDescriptor &masterND =
				transactionService_->getEE()->getServerND(masterNodeId);

			const int8_t masterMatched = masterND.isSelf();
			out << masterMatched;

			const ContainerHashMode hashMode = CONTAINER_HASH_MODE_CRC32;
			out << hashMode;

			util::SocketAddress::Inet addr;
			uint16_t port;
			masterND.getAddress().getIP(&addr, &port);
			out << std::pair<const uint8_t *, size_t>(
				reinterpret_cast<const uint8_t *>(&addr), sizeof(addr));
			out << static_cast<uint32_t>(port);
		}

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void GetPartitionContainerNamesHandler::operator()(
	EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		int64_t start;
		ResultSize limit;
		decodeLongData<int64_t>(in, start);
		decodeLongData<ResultSize>(in, limit);
		checkSizeLimit(limit);

		DataStore::ContainerCondition containerCondition(alloc);  
		decodeContainerConditionData(in, containerCondition);	 

		ConnectionOption &connOption =
			ev.getSenderND().getUserData<ConnectionOption>();

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), ANY_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);

		TransactionContext &txn = transactionManager_->put(alloc, request.pId_,
			TXN_EMPTY_CLIENTID, request.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);
		checkDbAccessible(connOption.dbName_, optionPart.dbName_);
		ContainerAccessMode containerAccessMode =
			getContainerAccessMode(connOption.userType_, optionPart.systemMode_,
				connOption.requestType_, false);
		DataStore::ContainerCondition validContainerCondition(alloc);
		for (util::Set<ContainerAttribute>::const_iterator itr =
				 containerCondition.getAttributes().begin();
			 itr != containerCondition.getAttributes().end(); itr++) {
			if (isAccessibleMode(containerAccessMode, *itr)) {
				validContainerCondition.insertAttribute(*itr);
			}
		}

		response.containerNum_ = dataStore_->getContainerCount(txn,
			txn.getPartitionId(), connOption.dbId_, validContainerCondition);
		dataStore_->getContainerNameList(txn, txn.getPartitionId(), start,
			limit, connOption.dbId_, validContainerCondition,
			response.containerNameList_);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void GetContainerPropertiesHandler::operator()(EventContext &ec, Event &ev)

{
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		ConnectionOption &connOption =
			ev.getSenderND().getUserData<ConnectionOption>();

		ContainerNameList containerNameList(alloc);
		uint32_t propFlags = 0;
		try {
			for (uint32_t remaining = 1; remaining > 0; remaining--) {
				util::String *name = ALLOC_NEW(alloc) util::String(alloc);
				in >> *name;
				containerNameList.push_back(name);

				if (containerNameList.size() == 1) {
					uint32_t propTypeCount;
					in >> propTypeCount;

					for (uint32_t i = 0; i < propTypeCount; i++) {
						uint8_t propType;
						in >> propType;
						propFlags |= (1 << propType);
					}

					if (in.base().remaining() > 0) {
						in >> remaining;
					}
				}
			}
		}
		catch (std::exception &e) {
			TXN_RETHROW_DECODE_ERROR(e, "");
		}
		const uint32_t propTypeCount = util::countNumOfBits(propFlags);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), ANY_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);

		Event reply(ec, ev.getType(), ev.getPartitionId());

		EventByteOutStream out = encodeCommonPart(
			reply, request.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS);

		TransactionContext &txn = transactionManager_->put(alloc, request.pId_,
			TXN_EMPTY_CLIENTID, request.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		for (ContainerNameList::iterator it = containerNameList.begin();
			 it != containerNameList.end(); ++it) {
			ExtendedContainerName extContainerName(alloc, connOption.dbId_,
				optionPart.dbName_.c_str(), (*it)->c_str());
			ContainerAutoPtr containerAutoPtr(txn, dataStore_,
				txn.getPartitionId(), extContainerName, ANY_CONTAINER);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			checkDbAccessible(connOption.dbName_, optionPart.dbName_);
			ContainerAccessMode containerAccessMode =
				getContainerAccessMode(connOption.userType_,
					optionPart.systemMode_, connOption.requestType_, false);
			if (container != NULL &&
				!isAccessibleMode(
					containerAccessMode, container->getAttribute())) {
				container = NULL;
			}

			if (it - containerNameList.begin() == 1) {
				encodeResultListHead(
					out, static_cast<uint32_t>(containerNameList.size()));
			}

			const bool found = (container != NULL);
			out << static_cast<uint8_t>(found);

			if (!found) {
				continue;
			}

			encodePropsHead(out, propTypeCount);

			if ((propFlags & (1 << CONTAINER_PROPERTY_ID)) != 0) {
				const ContainerId containerId = container->getContainerId();
				const SchemaVersionId schemaVersionId =
					container->getVersionId();
				ExtendedContainerName *respContainerName;
				container->getExtendedContainerName(txn, respContainerName);
				uint32_t respContainerNameSize = static_cast<uint32_t>(
					strlen(respContainerName->getContainerName()));
				encodeId(out, schemaVersionId, containerId,
					reinterpret_cast<const uint8_t *>(
							 respContainerName->getContainerName()),
					respContainerNameSize);
			}

			if ((propFlags & (1 << CONTAINER_PROPERTY_SCHEMA)) != 0) {
				const ContainerType containerType =
					container->getContainerType();

				const bool optionIncluded = true;
				util::XArray<uint8_t> containerInfo(alloc);
				container->getContainerInfo(txn, containerInfo, optionIncluded);

				encodeSchema(out, containerType, containerInfo);
			}

			if ((propFlags & (1 << CONTAINER_PROPERTY_INDEX)) != 0) {
				util::XArray<IndexInfo> indexInfoList(alloc);
				container->getIndexInfoList(txn, indexInfoList);

				encodeIndex(out, indexInfoList);
			}

			if ((propFlags & (1 << CONTAINER_PROPERTY_EVENT_NOTIFICATION)) !=
				0) {
				util::XArray<char *> urlList(alloc);
				util::XArray<uint32_t> urlLenList(alloc);


				encodeEventNotification(out, urlList, urlLenList);
			}

			if ((propFlags & (1 << CONTAINER_PROPERTY_TRIGGER)) != 0) {
				util::XArray<const uint8_t *> triggerList(alloc);
				container->getTriggerList(txn, triggerList);

				encodeTrigger(out, triggerList);
			}

			if ((propFlags & (1 << CONTAINER_PROPERTY_ATTRIBUTES)) != 0) {
				encodeAttributes(out, container->getAttribute());
			}
		}

		ec.getEngine().send(reply, ev.getSenderND());
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void GetContainerPropertiesHandler::encodeResultListHead(
	EventByteOutStream &out, uint32_t totalCount) {
	try {
		out << (totalCount - 1);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodePropsHead(
	EventByteOutStream &out, uint32_t propTypeCount) {
	try {
		out << propTypeCount;
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodeId(EventByteOutStream &out,
	SchemaVersionId schemaVersionId, ContainerId containerId,
	const uint8_t *containerName, uint32_t containerNameSize) {
	try {
		encodeEnumData<ContainerProperty>(out, CONTAINER_PROPERTY_ID);

		const size_t sizePos = out.base().position();
		uint32_t size = 0;
		out << size;

		out << schemaVersionId;
		out << containerId;
		out << containerNameSize;
		out << std::pair<const uint8_t *, size_t>(
			containerName, containerNameSize);

		const size_t lastPos = out.base().position();
		size = static_cast<uint32_t>(lastPos - sizePos - sizeof(size));
		out.base().position(sizePos);
		out << size;
		out.base().position(lastPos);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodeSchema(EventByteOutStream &out,
	ContainerType containerType,
	const util::XArray<uint8_t> &serializedCollectionInfo) {
	try {
		encodeEnumData<ContainerProperty>(out, CONTAINER_PROPERTY_SCHEMA);

		const uint32_t size = static_cast<uint32_t>(
			sizeof(uint8_t) + serializedCollectionInfo.size());
		out << size;
		out << containerType;
		out << std::pair<const uint8_t *, size_t>(
			serializedCollectionInfo.data(), serializedCollectionInfo.size());
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodeIndex(
	EventByteOutStream &out, const util::XArray<IndexInfo> &indexInfoList) {
	try {
		encodeEnumData<ContainerProperty>(out, CONTAINER_PROPERTY_INDEX);

		const size_t sizePos = out.base().position();
		uint32_t size = 0;
		out << size;

		const uint32_t num = static_cast<uint32_t>(indexInfoList.size());
		out << num;
		for (uint32_t i = 0; i < num; i++) {
			out << indexInfoList[i].columnId;
			encodeEnumData<MapType>(out, indexInfoList[i].mapType);
		}

		const size_t lastPos = out.base().position();
		size = static_cast<uint32_t>(lastPos - sizePos - sizeof(size));
		out.base().position(sizePos);
		out << size;
		out.base().position(lastPos);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodeEventNotification(
	EventByteOutStream &out, const util::XArray<char *> &urlList,
	const util::XArray<uint32_t> &urlLenList) {
	try {
		encodeEnumData<ContainerProperty>(
			out, CONTAINER_PROPERTY_EVENT_NOTIFICATION);

		const size_t sizePos = out.base().position();
		uint32_t size = 0;
		out << size;

		const uint32_t num = static_cast<uint32_t>(urlLenList.size());
		out << num;
		for (uint32_t i = 0; i < num; i++) {
			out << urlLenList[i] + 1;
			out << std::pair<const uint8_t *, size_t>(
				reinterpret_cast<const uint8_t *>(urlList[i]),
				static_cast<size_t>(urlLenList[i] + 1));
		}

		const size_t lastPos = out.base().position();
		size = static_cast<uint32_t>(lastPos - sizePos - sizeof(size));
		out.base().position(sizePos);
		out << size;
		out.base().position(lastPos);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodeTrigger(
	EventByteOutStream &out, const util::XArray<const uint8_t *> &triggerList) {
	try {
		util::StackAllocator &alloc = *triggerList.get_allocator().base();

		encodeEnumData<ContainerProperty>(out, CONTAINER_PROPERTY_TRIGGER);

		const size_t sizePos = out.base().position();
		uint32_t size = 0;
		out << size;

		const uint32_t num = static_cast<uint32_t>(triggerList.size());
		out << num;
		for (size_t i = 0; i < triggerList.size(); i++) {
			TriggerInfo info(alloc);
			TriggerInfo::decode(triggerList[i], info);

			out << info.name_;
			out << static_cast<int8_t>(info.type_);
			out << info.uri_;
			out << info.operation_;
			out << static_cast<uint32_t>(info.columnIds_.size());
			for (size_t i = 0; i < info.columnIds_.size(); i++) {
				out << info.columnIds_[i];
			}
			const std::string jmsProviderName(
				TriggerService::jmsProviderTypeToStr(info.jmsProviderType_));
			out << jmsProviderName;
			const std::string jmsDestinationType(
				TriggerService::jmsDestinationTypeToStr(
					info.jmsDestinationType_));
			out << jmsDestinationType;
			out << info.jmsDestinationName_;
			out << info.jmsUser_;
			out << info.jmsPassword_;
		}

		const size_t lastPos = out.base().position();
		size = static_cast<uint32_t>(lastPos - sizePos - sizeof(size));
		out.base().position(sizePos);
		out << size;
		out.base().position(lastPos);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void GetContainerPropertiesHandler::encodeAttributes(
	EventByteOutStream &out, const ContainerAttribute containerAttribute) {
	try {
		encodeEnumData<ContainerProperty>(out, CONTAINER_PROPERTY_ATTRIBUTES);
		int32_t attribute = static_cast<int32_t>(containerAttribute);

		const uint32_t size = sizeof(ContainerAttribute);
		out << size;
		out << attribute;
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief Handler Operator
*/
void PutContainerHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		util::String containerName(alloc);
		ContainerType containerType;
		bool modifiable;
		util::XArray<uint8_t> containerInfo(alloc);

		decodeStringData<util::String>(in, containerName);
		decodeEnumData<ContainerType>(in, containerType);
		decodeBooleanData(in, modifiable);
		decodeBinaryData(in, containerInfo, true);

		ConnectionOption &connOption =
			ev.getSenderND().getUserData<ConnectionOption>();

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);

		TransactionContext &txn = transactionManager_->put(alloc, request.pId_,
			TXN_EMPTY_CLIENTID, request.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);
		checkDbAccessible(connOption.dbName_, optionPart.dbName_);
		ContainerAccessMode containerAccessMode =
			getContainerAccessMode(connOption.userType_, optionPart.systemMode_,
				connOption.requestType_, true);
		if (!isAccessibleMode(
				containerAccessMode, optionPart.containerAttribute_)) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_ACCESS_INVALID,
				"Can not create container attribute : "
					<< optionPart.containerAttribute_);
		}
		if (containerName.compare(GS_USERS) == 0 ||
			containerName.compare(GS_DATABASES) == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_CON_ACCESS_INVALID,
				"Can not create container name : " << optionPart.dbName_);
		}

		{
			ExtendedContainerName extContainerName(alloc, connOption.dbId_,
				optionPart.dbName_.c_str(), containerName.c_str());
			ContainerAutoPtr containerAutoPtr(txn, dataStore_,
				txn.getPartitionId(), extContainerName, containerType);
			BaseContainer *container = containerAutoPtr.getBaseContainer();

			if (container != NULL && container->hasUncommitedTransaction(txn)) {
				GS_TRACE_INFO(TRANSACTION_SERVICE,
					GS_TRACE_TXN_WAIT_FOR_TRANSACTION_END,
					"Container has uncommited transactions"
						<< " (pId=" << request.pId_
						<< ", stmtId=" << request.cxtSrc_.stmtId_
						<< ", eventType=" << request.stmtType_
						<< ", clientId=" << request.clientId_
						<< ", containerId=" << container->getContainerId()
						<< ", pendingCount=" << ev.getQueueingCount() << ")");
				ec.getEngine().addPending(
					ev, EventEngine::RESUME_AT_PARTITION_CHANGE);

				return;
			}
		}

		DataStore::PutStatus putStatus;
		{
			util::XArrayOutStream<> arrayOut(containerInfo);
			util::ByteStream<util::XArrayOutStream<> > out(arrayOut);
			int32_t containerAttribute = optionPart.containerAttribute_;
			out << containerAttribute;
		}
		ExtendedContainerName extContainerName(alloc, connOption.dbId_,
			optionPart.dbName_.c_str(), containerName.c_str());
		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			extContainerName, containerType,
			static_cast<uint32_t>(containerInfo.size()), containerInfo.data(),
			modifiable, putStatus);
		BaseContainer *container = containerAutoPtr.getBaseContainer();

		response.schemaVersionId_ = container->getVersionId();
		response.containerId_ = container->getContainerId();
		const char8_t *createdContainerName =
			extContainerName.getContainerName();
		uint32_t createdContainerNameSize =
			static_cast<uint32_t>(strlen(createdContainerName));

		response.stringData_.append(
			createdContainerName, createdContainerNameSize);

		const bool optionIncluded = true;
		container->getContainerInfo(txn, response.binaryData_, optionIncluded);
		{
			util::XArrayOutStream<> arrayOut(response.binaryData_);
			util::ByteStream<util::XArrayOutStream<> > out(arrayOut);
			int32_t containerAttribute =
				static_cast<int32_t>(container->getAttribute());
			out << containerAttribute;
		}

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		if (putStatus != DataStore::NOT_EXECUTED) {
			util::XArray<uint8_t> *log =
				ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			const LogSequentialNumber lsn = logManager_->putPutContainerLog(
				*log, txn.getPartitionId(), response.containerId_,
				static_cast<uint32_t>(strlen(extContainerName.c_str())),
				extContainerName.c_str(), response.binaryData_,
				container->getContainerType());
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		request.cxtSrc_.containerId_ = container->getContainerId();
		txn = transactionManager_->put(alloc, request.pId_, TXN_EMPTY_CLIENTID,
			request.cxtSrc_, now, emNow);
		const bool ackWait = executeReplication(request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size());

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void DropContainerHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		util::String containerName(alloc);
		ContainerType containerType;

		decodeStringData<util::String>(in, containerName);
		decodeEnumData<ContainerType>(in, containerType);

		ConnectionOption &connOption =
			ev.getSenderND().getUserData<ConnectionOption>();

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);

		TransactionContext &txn = transactionManager_->put(alloc, request.pId_,
			TXN_EMPTY_CLIENTID, request.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ExtendedContainerName extContainerName(alloc, connOption.dbId_,
			optionPart.dbName_.c_str(), containerName.c_str());

		{
			ContainerAutoPtr containerAutoPtr(txn, dataStore_,
				txn.getPartitionId(), extContainerName, containerType);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			checkDbAccessible(connOption.dbName_, optionPart.dbName_);
			ContainerAccessMode containerAccessMode =
				getContainerAccessMode(connOption.userType_,
					optionPart.systemMode_, connOption.requestType_, true);
			if (container != NULL &&
				!isAccessibleMode(
					containerAccessMode, container->getAttribute())) {
				GS_THROW_USER_ERROR(GS_ERROR_DS_CON_ACCESS_INVALID,
					"Can not drop container attribute : "
						<< optionPart.containerAttribute_);
			}
			if (container == NULL) {
				replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
					TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
				return;
			}
			else if (container->hasUncommitedTransaction(txn)) {
				GS_TRACE_INFO(TRANSACTION_SERVICE,
					GS_TRACE_TXN_WAIT_FOR_TRANSACTION_END,
					"Container has uncommited transactions"
						<< " (pId=" << request.pId_
						<< ", stmtId=" << request.cxtSrc_.stmtId_
						<< ", eventType=" << request.stmtType_
						<< ", clientId=" << request.clientId_
						<< ", containerId=" << container->getContainerId()
						<< ", pendingCount=" << ev.getQueueingCount() << ")");
				ec.getEngine().addPending(
					ev, EventEngine::RESUME_AT_PARTITION_CHANGE);
				return;
			}

			response.containerId_ = container->getContainerId();
			const char8_t *dropedContainerName =
				extContainerName.getContainerName();
			uint32_t dropedContainerNameSize =
				static_cast<uint32_t>(strlen(dropedContainerName));

			response.stringData_.append(
				dropedContainerName, dropedContainerNameSize);
		}

		dataStore_->dropContainer(
			txn, txn.getPartitionId(), extContainerName, containerType);

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		util::XArray<uint8_t> *log =
			ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
		const LogSequentialNumber lsn = logManager_->putDropContainerLog(*log,
			txn.getPartitionId(), response.containerId_,
			static_cast<uint32_t>(strlen(extContainerName.c_str())),
			extContainerName.c_str());

		partitionTable_->setLSN(txn.getPartitionId(), lsn);
		logRecordList.push_back(log);

		const bool ackWait = executeReplication(request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size());

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void GetContainerHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		util::String containerName(alloc);
		ContainerType containerType;

		decodeStringData<util::String>(in, containerName);
		decodeEnumData<ContainerType>(in, containerType);

		ConnectionOption &connOption =
			ev.getSenderND().getUserData<ConnectionOption>();

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), ANY_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);

		TransactionContext &txn = transactionManager_->put(alloc, request.pId_,
			TXN_EMPTY_CLIENTID, request.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ExtendedContainerName extContainerName(alloc, connOption.dbId_,
			optionPart.dbName_.c_str(), containerName.c_str());
		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			extContainerName, containerType);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkDbAccessible(connOption.dbName_, optionPart.dbName_);
		ContainerAccessMode containerAccessMode =
			getContainerAccessMode(connOption.userType_, optionPart.systemMode_,
				connOption.requestType_, false);
		if (container != NULL &&
			!isAccessibleMode(containerAccessMode, container->getAttribute())) {
			container = NULL;
		}
		response.existFlag_ = (container != NULL);

		if (response.existFlag_) {
			response.schemaVersionId_ = container->getVersionId();
			response.containerId_ = container->getContainerId();
			const char8_t *createdContainerName =
				extContainerName.getContainerName();
			uint32_t createdContainerNameSize =
				static_cast<uint32_t>(strlen(createdContainerName));
			response.stringData_.append(
				createdContainerName, createdContainerNameSize);

			const bool optionIncluded = true;
			container->getContainerInfo(
				txn, response.binaryData_, optionIncluded);
		}

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void CreateDropIndexHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		IndexInfo indexInfo;

		decodeIndexInfo(in, indexInfo);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);

		TransactionContext &txn = transactionManager_->put(alloc, request.pId_,
			TXN_EMPTY_CLIENTID, request.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerSchemaVersion(container, request.schemaVersionId_);

		if (container->hasUncommitedTransaction(txn)) {
			GS_TRACE_INFO(TRANSACTION_SERVICE,
				GS_TRACE_TXN_WAIT_FOR_TRANSACTION_END,
				"Container has uncommited transactions"
					<< " (pId=" << request.pId_
					<< ", stmtId=" << request.cxtSrc_.stmtId_ << ", eventType="
					<< request.stmtType_ << ", clientId=" << request.clientId_
					<< ", containerId=" << container->getContainerId()
					<< ", pendingCount=" << ev.getQueueingCount() << ")");
			ec.getEngine().addPending(
				ev, EventEngine::RESUME_AT_PARTITION_CHANGE);
			return;
		}

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		util::XArray<uint8_t> *log =
			ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
		switch (ev.getType()) {
		case CREATE_INDEX: {
			container->createIndex(txn, indexInfo);
			const LogSequentialNumber lsn = logManager_->putCreateIndexLog(*log,
				txn.getPartitionId(), txn.getContainerId(), indexInfo.columnId,
				indexInfo.mapType);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
		} break;
		case DELETE_INDEX: {
			container->dropIndex(txn, indexInfo);
			const LogSequentialNumber lsn = logManager_->putDropIndexLog(*log,
				txn.getPartitionId(), txn.getContainerId(), indexInfo.columnId,
				indexInfo.mapType);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);

		} break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN, "");
		}
		logRecordList.push_back(log);

		const bool ackWait = executeReplication(request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size());

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void CreateDropTriggerHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);

		TransactionContext &txn = transactionManager_->put(alloc, request.pId_,
			TXN_EMPTY_CLIENTID, request.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerSchemaVersion(container, request.schemaVersionId_);

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		util::XArray<uint8_t> *log =
			ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
		switch (ev.getType()) {
		case CREATE_TRIGGER: {
			TriggerInfo info(alloc);
			decodeTriggerInfo(in, info);

			TriggerHandler::checkTriggerRegistration(alloc, info);
			container->putTrigger(txn, info);

			util::XArray<uint8_t> binaryTrigger(alloc);
			TriggerInfo::encode(info, binaryTrigger);

			const LogSequentialNumber lsn = logManager_->putCreateTriggerLog(
				*log, txn.getPartitionId(), txn.getContainerId(),
				static_cast<uint32_t>(info.name_.size()), info.name_.c_str(),
				binaryTrigger);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
		} break;
		case DELETE_TRIGGER: {
			util::String triggerName(alloc);
			decodeStringData<util::String>(in, triggerName);

			container->deleteTrigger(txn, triggerName.c_str());

			const LogSequentialNumber lsn = logManager_->putDropTriggerLog(*log,
				txn.getPartitionId(), txn.getContainerId(),
				static_cast<uint32_t>(triggerName.size()), triggerName.c_str());
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
		} break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN, "");
		}
		logRecordList.push_back(log);

		const bool ackWait = executeReplication(request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size());

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void FlushLogHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);

		const PartitionGroupId pgId =
			logManager_->calcPartitionGroupId(request.pId_);
		logManager_->writeBuffer(pgId);
		logManager_->flushFile(pgId);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void WriteLogPeriodicallyHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	try {
		const PartitionGroupId pgId = ec.getWorkerId();

		if (logManager_ && clusterManager_->checkRecoveryCompleted()) {
			logManager_->writeBuffer(pgId);  
		}
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			"Failed to write log periodically (pgId="
				<< ec.getWorkerId() << ", reason=" << GS_EXCEPTION_MESSAGE(e)
				<< ")");
		clusterService_->setError(ec, &e);
	}
}

/*!
	@brief Handler Operator
*/
void CreateTransactionContextHandler::operator()(EventContext &ec, Event &ev)

{
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);

		request.cxtSrc_.getMode_ = TransactionManager::CREATE;
		request.cxtSrc_.txnMode_ = TransactionManager::AUTO_COMMIT;
		transactionManager_->put(alloc, request.pId_, request.clientId_,
			request.cxtSrc_, now, emNow);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void CloseTransactionContextHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);

		util::XArray<ClientId> closedResourceIds(alloc);
		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);

		try {
			TransactionContext &txn = transactionManager_->get(
				alloc, request.pId_, request.clientId_);
			const DataStore::Latch latch(
				txn, txn.getPartitionId(), dataStore_, clusterService_);

			if (txn.isActive()) {
				BaseContainer *container = NULL;

				try {
					container =
						dataStore_->getContainer(txn, txn.getPartitionId(),
							txn.getContainerId(), ANY_CONTAINER);
				}
				catch (UserException &e) {
					UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
						"Container not found (pId="
							<< txn.getPartitionId()
							<< ", clientId=" << txn.getClientId()
							<< ", containerId=" << txn.getContainerId()
							<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
				}
				StackAllocAutoPtr<BaseContainer> containerAutoPtr(
					txn.getDefaultAllocator(), container);

				if (containerAutoPtr.get() != NULL) {
					util::XArray<uint8_t> *log =
						ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
					const LogSequentialNumber lsn =
						logManager_->putAbortTransactionLog(*log,
							txn.getPartitionId(), txn.getClientId(),
							txn.getId(), txn.getContainerId(),
							request.cxtSrc_.stmtId_);
					partitionTable_->setLSN(txn.getPartitionId(), lsn);
					transactionManager_->abort(txn, *containerAutoPtr.get());
					logRecordList.push_back(log);
				}

				ec.setPendingPartitionChanged(txn.getPartitionId());
			}

			transactionManager_->remove(request.pId_, request.clientId_);
			closedResourceIds.push_back(request.clientId_);
		}
		catch (ContextNotFoundException &) {
		}

		request.cxtSrc_.getMode_ = TransactionManager::AUTO;
		request.cxtSrc_.txnMode_ = TransactionManager::AUTO_COMMIT;
		TransactionContext &txn = transactionManager_->put(alloc, request.pId_,
			request.clientId_, request.cxtSrc_, now, emNow);
		const bool ackWait = executeReplication(request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), closedResourceIds.data(),
			closedResourceIds.size(), logRecordList.data(),
			logRecordList.size());

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void CommitAbortTransactionHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);

		request.cxtSrc_.getMode_ = TransactionManager::GET;
		request.cxtSrc_.txnMode_ = TransactionManager::NO_AUTO_COMMIT_CONTINUE;
		TransactionContext &txn = transactionManager_->put(alloc, request.pId_,
			request.clientId_, request.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerExistence(container);

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		util::XArray<uint8_t> *log =
			ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
		switch (ev.getType()) {
		case COMMIT_TRANSACTION: {
			const LogSequentialNumber lsn =
				logManager_->putCommitTransactionLog(*log, txn.getPartitionId(),
					txn.getClientId(), txn.getId(), txn.getContainerId(),
					request.cxtSrc_.stmtId_);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			transactionManager_->commit(txn, *container);
		} break;

		case ABORT_TRANSACTION: {
			const LogSequentialNumber lsn = logManager_->putAbortTransactionLog(
				*log, txn.getPartitionId(), txn.getClientId(), txn.getId(),
				txn.getContainerId(), request.cxtSrc_.stmtId_);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			transactionManager_->abort(txn, *container);
		} break;

		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN, "");
		}
		logRecordList.push_back(log);

		transactionManager_->update(txn, request.cxtSrc_.stmtId_);

		ec.setPendingPartitionChanged(request.pId_);

		const bool ackWait = executeReplication(request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size());

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void PutRowHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		const uint64_t numRow = 1;
		RowData rowData(alloc);

		decodeBinaryData(in, rowData, true);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);


		TransactionContext &txn = transactionManager_->put(alloc, request.pId_,
			request.clientId_, request.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerSchemaVersion(container, request.schemaVersionId_);


		PutRowOption putRowOption = optionPart.putRowOption_;

		util::XArray<RowId> rowIds(alloc);
		rowIds.assign(numRow, UNDEF_ROWID);
		DataStore::PutStatus putStatus;
		container->putRow(txn, static_cast<uint32_t>(rowData.size()),
			rowData.data(), rowIds[0], putStatus, putRowOption);
		const bool executed = (putStatus != DataStore::NOT_EXECUTED);
		response.existFlag_ = (putStatus == DataStore::UPDATE);

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		{
			const bool withBegin = (request.cxtSrc_.txnMode_ ==
									TransactionManager::NO_AUTO_COMMIT_BEGIN);
			const bool isAutoCommit =
				(request.cxtSrc_.txnMode_ == TransactionManager::AUTO_COMMIT);
			assert(!(withBegin && isAutoCommit));
			assert(numRow == rowIds.size());
			assert((executed && rowIds[0] != UNDEF_ROWID) ||
				   (!executed && rowIds[0] == UNDEF_ROWID));
			util::XArray<uint8_t> *log =
				ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			const LogSequentialNumber lsn = logManager_->putPutRowLog(*log,
				txn.getPartitionId(), txn.getClientId(), txn.getId(),
				txn.getContainerId(), request.cxtSrc_.stmtId_,
				(executed ? rowIds.size() : 0), rowIds, (executed ? numRow : 0),
				rowData, txn.getTransationTimeoutInterval(),
				request.cxtSrc_.getMode_, withBegin, isAutoCommit);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		transactionManager_->update(txn, request.cxtSrc_.stmtId_);

		const bool ackWait = executeReplication(request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size(), response.existFlag_);

		if (executed) {
			util::XArray<TriggerService::OperationType> operationTypeList(
				alloc);
			operationTypeList.push_back(TriggerService::PUT_ROW);
			TriggerHandler::checkTrigger(*triggerService_, txn, *container, ec,
				operationTypeList, 1, rowData.data(), rowData.size());
		}

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);

		transactionService_->incrementWriteOperationCount(ev.getPartitionId());
		transactionService_->addRowWriteCount(ev.getPartitionId(), numRow);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void PutRowSetHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		uint64_t numRow;
		RowData multiRowData(alloc);

		decodeMultipleRowData(in, numRow, multiRowData);
		assert(numRow > 0);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);

		const bool STATEMENTID_NO_CHECK = true;
		TransactionContext &txn =
			transactionManager_->put(alloc, request.pId_, request.clientId_,
				request.cxtSrc_, now, emNow, STATEMENTID_NO_CHECK);
		request.startStmtId_ = txn.getLastStatementId();
		transactionManager_->checkStatementContinuousInTransaction(txn,
			request.cxtSrc_.stmtId_, request.cxtSrc_.getMode_,
			request.cxtSrc_.txnMode_);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerSchemaVersion(container, request.schemaVersionId_);

		PutRowOption putRowOption = optionPart.putRowOption_;

		const bool NO_VALIDATE_ROW_IMAGE = false;
		InputMessageRowStore inputMessageRowStore(
			dataStore_->getValueLimitConfig(), container->getColumnInfoList(),
			container->getColumnNum(), multiRowData.data(),
			static_cast<uint32_t>(multiRowData.size()), numRow,
			container->getRowFixedDataSize(), NO_VALIDATE_ROW_IMAGE);
		response.existFlag_ = false;

		util::XArray<RowId> rowIds(alloc);
		rowIds.assign(static_cast<size_t>(1), UNDEF_ROWID);
		util::XArray<TriggerService::OperationType> operationTypeList(alloc);
		operationTypeList.assign(
			static_cast<size_t>(1), TriggerService::PUT_ROW);
		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		RowData rowData(alloc);

		TXN_FAILOVER_STATE_SET(
			FailoverTestUtil::STATE_0, ev.getType(), request.cxtSrc_);

		try {
			for (StatementId rowStmtId = request.cxtSrc_.stmtId_;
				 inputMessageRowStore.next(); rowStmtId++) {
				try {
					transactionManager_->checkStatementAlreadyExecuted(
						txn, rowStmtId, request.cxtSrc_.isUpdateStmt_);
				}
				catch (StatementAlreadyExecutedException &e) {
					UTIL_TRACE_EXCEPTION_INFO(TRANSACTION_SERVICE, e,
						"Row already put (pId="
							<< ev.getPartitionId() << ", eventType="
							<< ev.getType() << ", stmtId=" << rowStmtId
							<< ", clientId=" << request.clientId_
							<< ", containerId="
							<< ((request.cxtSrc_.containerId_ ==
									UNDEF_CONTAINERID)
									   ? 0
									   : request.cxtSrc_.containerId_)
							<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");

					continue;
				}

				rowData.clear();
				inputMessageRowStore.getCurrentRowData(rowData);

				rowIds[0] = UNDEF_ROWID;
				DataStore::PutStatus putStatus;
				container->putRow(txn, static_cast<uint32_t>(rowData.size()),
					rowData.data(), rowIds[0], putStatus, putRowOption);
				const bool executed = (putStatus != DataStore::NOT_EXECUTED);

				TXN_FAILOVER_STATE_SET_IF((inputMessageRowStore.position() > 1),
					FailoverTestUtil::STATE_2, FailoverTestUtil::STATE_1,
					ev.getType(), request.cxtSrc_);

				{
					const bool withBegin =
						(request.cxtSrc_.txnMode_ ==
							TransactionManager::NO_AUTO_COMMIT_BEGIN);
					const bool isAutoCommit = (request.cxtSrc_.txnMode_ ==
											   TransactionManager::AUTO_COMMIT);
					assert(!(withBegin && isAutoCommit));
					assert(rowIds.size() == 1);
					assert((executed && rowIds[0] != UNDEF_ROWID) ||
						   (!executed && rowIds[0] == UNDEF_ROWID));
					util::XArray<uint8_t> *log =
						ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
					const LogSequentialNumber lsn = logManager_->putPutRowLog(
						*log, txn.getPartitionId(), txn.getClientId(),
						txn.getId(), txn.getContainerId(), rowStmtId,
						(executed ? rowIds.size() : 0), rowIds,
						(executed ? 1 : 0), rowData,
						txn.getTransationTimeoutInterval(),
						request.cxtSrc_.getMode_, withBegin, isAutoCommit);
					partitionTable_->setLSN(txn.getPartitionId(), lsn);
					logRecordList.push_back(log);
				}

				TXN_FAILOVER_STATE_SET(
					FailoverTestUtil::STATE_3, ev.getType(), request.cxtSrc_);

				transactionManager_->update(txn, rowStmtId);

				if (executed) {
					TriggerHandler::checkTrigger(*triggerService_, txn,
						*container, ec, operationTypeList, 1, rowData.data(),
						rowData.size());
				}
			}
		}
		catch (std::exception &) {
			TXN_FAILOVER_STATE_SET(
				FailoverTestUtil::STATE_8, ev.getType(), request.cxtSrc_);

			executeReplication(request, ec, alloc, NodeDescriptor::EMPTY_ND,
				txn, ev.getType(), txn.getLastStatementId(),
				TransactionManager::REPLICATION_ASYNC, NULL, 0,
				logRecordList.data(), logRecordList.size(),
				response.existFlag_);

			TXN_FAILOVER_STATE_SET(
				FailoverTestUtil::STATE_9, ev.getType(), request.cxtSrc_);

			throw;
		}

		const bool ackWait = executeReplication(request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size(), response.existFlag_);

		TXN_FAILOVER_STATE_SET(
			FailoverTestUtil::STATE_4, ev.getType(), request.cxtSrc_);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);

		TXN_FAILOVER_STATE_SET(
			FailoverTestUtil::STATE_7, ev.getType(), request.cxtSrc_);

		transactionService_->incrementWriteOperationCount(ev.getPartitionId());
		transactionService_->addRowWriteCount(ev.getPartitionId(), numRow);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void RemoveRowHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		RowKeyData rowKey(alloc);

		decodeBinaryData(in, rowKey, true);

		const uint64_t numRow = 1;

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);


		TransactionContext &txn = transactionManager_->put(alloc, request.pId_,
			request.clientId_, request.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerSchemaVersion(container, request.schemaVersionId_);

		util::XArray<RowId> rowIds(alloc);
		rowIds.assign(numRow, UNDEF_ROWID);
		container->deleteRow(txn, static_cast<uint32_t>(rowKey.size()),
			rowKey.data(), rowIds[0], response.existFlag_);
		const bool executed = response.existFlag_;

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		{
			const bool withBegin = (request.cxtSrc_.txnMode_ ==
									TransactionManager::NO_AUTO_COMMIT_BEGIN);
			const bool isAutoCommit =
				(request.cxtSrc_.txnMode_ == TransactionManager::AUTO_COMMIT);
			assert(!(withBegin && isAutoCommit));
			assert(numRow == rowIds.size());
			assert((executed && rowIds[0] != UNDEF_ROWID) ||
				   (!executed && rowIds[0] == UNDEF_ROWID));
			util::XArray<uint8_t> *log =
				ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			const LogSequentialNumber lsn =
				logManager_->putRemoveRowLog(*log, txn.getPartitionId(),
					txn.getClientId(), txn.getId(), txn.getContainerId(),
					request.cxtSrc_.stmtId_, (executed ? rowIds.size() : 0),
					rowIds, txn.getTransationTimeoutInterval(),
					request.cxtSrc_.getMode_, withBegin, isAutoCommit);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		transactionManager_->update(txn, request.cxtSrc_.stmtId_);

		const bool ackWait = executeReplication(request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size(), response.existFlag_);

		if (executed) {
			util::XArray<TriggerService::OperationType> operationTypeList(
				alloc);
			operationTypeList.assign(numRow, TriggerService::DELETE_ROW);
			TriggerHandler::checkTrigger(*triggerService_, txn, *container, ec,
				operationTypeList, numRow, NULL, 0);
		}

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);

		transactionService_->incrementWriteOperationCount(ev.getPartitionId());
		transactionService_->addRowWriteCount(ev.getPartitionId(), numRow);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void UpdateRowByIdHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		const uint64_t numRow = 1;
		util::XArray<RowId> rowIds(alloc);
		rowIds.assign(numRow, UNDEF_ROWID);
		RowData rowData(alloc);

		decodeLongData<RowId>(in, rowIds[0]);
		decodeBinaryData(in, rowData, true);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);


		TransactionContext &txn = transactionManager_->put(alloc, request.pId_,
			request.clientId_, request.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerSchemaVersion(container, request.schemaVersionId_);


		DataStore::PutStatus putStatus;
		container->updateRow(txn, static_cast<uint32_t>(rowData.size()),
			rowData.data(), rowIds[0], putStatus);
		const bool executed = (putStatus != DataStore::NOT_EXECUTED);

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		{
			const bool withBegin = (request.cxtSrc_.txnMode_ ==
									TransactionManager::NO_AUTO_COMMIT_BEGIN);
			const bool isAutoCommit =
				(request.cxtSrc_.txnMode_ == TransactionManager::AUTO_COMMIT);
			assert(!(withBegin && isAutoCommit));
			assert(numRow == rowIds.size());
			assert(rowIds[0] != UNDEF_ROWID);
			util::XArray<uint8_t> *log =
				ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			const LogSequentialNumber lsn = logManager_->putUpdateRowLog(*log,
				txn.getPartitionId(), txn.getClientId(), txn.getId(),
				txn.getContainerId(), request.cxtSrc_.stmtId_,
				(executed ? rowIds.size() : 0), rowIds, (executed ? numRow : 0),
				rowData, txn.getTransationTimeoutInterval(),
				request.cxtSrc_.getMode_, withBegin, isAutoCommit);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		transactionManager_->update(txn, request.cxtSrc_.stmtId_);

		const bool ackWait = executeReplication(request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size());

		if (executed) {
			util::XArray<TriggerService::OperationType> operationTypeList(
				alloc);
			operationTypeList.assign(numRow, TriggerService::PUT_ROW);
			TriggerHandler::checkTrigger(*triggerService_, txn, *container, ec,
				operationTypeList, numRow, rowData.data(), rowData.size());
		}

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);

		transactionService_->incrementWriteOperationCount(ev.getPartitionId());
		transactionService_->addRowWriteCount(ev.getPartitionId(), numRow);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void RemoveRowByIdHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		const uint64_t numRow = 1;
		util::XArray<RowId> rowIds(alloc);
		rowIds.assign(numRow, UNDEF_ROWID);

		decodeLongData<RowId>(in, rowIds[0]);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);


		TransactionContext &txn = transactionManager_->put(alloc, request.pId_,
			request.clientId_, request.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerSchemaVersion(container, request.schemaVersionId_);

		container->deleteRow(txn, rowIds[0], response.existFlag_);
		const bool executed = response.existFlag_;

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		{
			const bool withBegin = (request.cxtSrc_.txnMode_ ==
									TransactionManager::NO_AUTO_COMMIT_BEGIN);
			const bool isAutoCommit =
				(request.cxtSrc_.txnMode_ == TransactionManager::AUTO_COMMIT);
			assert(!(withBegin && isAutoCommit));
			assert(numRow == rowIds.size());
			assert(rowIds[0] != UNDEF_ROWID);
			util::XArray<uint8_t> *log =
				ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			const LogSequentialNumber lsn =
				logManager_->putRemoveRowLog(*log, txn.getPartitionId(),
					txn.getClientId(), txn.getId(), txn.getContainerId(),
					request.cxtSrc_.stmtId_, (executed ? rowIds.size() : 0),
					rowIds, txn.getTransationTimeoutInterval(),
					request.cxtSrc_.getMode_, withBegin, isAutoCommit);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		transactionManager_->update(txn, request.cxtSrc_.stmtId_);

		const bool ackWait = executeReplication(request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size());

		if (executed) {
			util::XArray<TriggerService::OperationType> operationTypeList(
				alloc);
			operationTypeList.assign(numRow, TriggerService::DELETE_ROW);
			TriggerHandler::checkTrigger(*triggerService_, txn, *container, ec,
				operationTypeList, numRow, NULL, 0);
		}

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);

		transactionService_->incrementWriteOperationCount(ev.getPartitionId());
		transactionService_->addRowWriteCount(ev.getPartitionId(), numRow);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void GetRowHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		RowKeyData rowKey(alloc);

		decodeBinaryData(in, rowKey, true);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
			optionPart.forUpdate_ ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), optionPart.forUpdate_);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);


		TransactionContext &txn = transactionManager_->put(alloc, request.pId_,
			request.clientId_, request.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerSchemaVersion(container, request.schemaVersionId_);

		response.rs_ = dataStore_->createResultSet(
			txn, txn.getContainerId(), container->getVersionId(), emNow);
		const ResultSetGuard rsGuard(*dataStore_, *response.rs_);
		QueryProcessor::get(txn, *container,
			static_cast<uint32_t>(rowKey.size()), rowKey.data(), *response.rs_);
		util::XArray<RowId> lockedRowId(alloc);
		if (optionPart.forUpdate_) {
			container->getLockRowIdList(txn, *response.rs_, lockedRowId);
		}
		QueryProcessor::fetch(
			txn, *container, 0, MAX_RESULT_SIZE, response.rs_);
		response.existFlag_ = (response.rs_->getResultNum() > 0);

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		if (optionPart.forUpdate_) {
			const bool withBegin = (request.cxtSrc_.txnMode_ ==
									TransactionManager::NO_AUTO_COMMIT_BEGIN);
			const bool isAutoCommit =
				(request.cxtSrc_.txnMode_ == TransactionManager::AUTO_COMMIT);
			assert(!(withBegin && isAutoCommit));
			util::XArray<uint8_t> *log =
				ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			const LogSequentialNumber lsn =
				logManager_->putLockRowLog(*log, txn.getPartitionId(),
					txn.getClientId(), txn.getId(), txn.getContainerId(),
					request.cxtSrc_.stmtId_, lockedRowId.size(), lockedRowId,
					txn.getTransationTimeoutInterval(),
					request.cxtSrc_.getMode_, withBegin, isAutoCommit);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		transactionManager_->update(txn, request.cxtSrc_.stmtId_);

		const bool ackWait = executeReplication(request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.cxtSrc_.stmtId_,
			TransactionManager::REPLICATION_ASYNC, NULL, 0,
			logRecordList.data(), logRecordList.size(), response.existFlag_);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
			ev.getPartitionId(), response.rs_->getFetchNum());
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void GetRowSetHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		FetchOption fetchOption;
		decodeFetchOption(in, fetchOption);

		RowId position;
		decodeLongData<RowId>(in, position);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
			optionPart.forUpdate_ ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), optionPart.forUpdate_);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);
		checkFetchOption(fetchOption);

		if (optionPart.forUpdate_) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_NOT_EXECUTABLE,
				"row lock (forUpdate) is not applicable");
		}


		TransactionContext &txn = transactionManager_->put(alloc, request.pId_,
			request.clientId_, request.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerSchemaVersion(container, request.schemaVersionId_);

		response.rs_ = dataStore_->createResultSet(
			txn, txn.getContainerId(), container->getVersionId(), emNow);
		const ResultSetGuard rsGuard(*dataStore_, *response.rs_);
		QueryProcessor::get(txn, *container, position, fetchOption.limit_,
			response.last_, *response.rs_);

		QueryProcessor::fetch(
			txn, *container, 0, fetchOption.size_, response.rs_);

		transactionManager_->update(txn, request.cxtSrc_.stmtId_);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
			ev.getPartitionId(), response.rs_->getFetchNum());
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
#if defined(PARTIAL_EXE_SERVER_UNIT_TEST_MODE) && \
	defined(GD_ENABLE_NEWSQL_SERVER)
void PartialTQLHandler::operator()(EventContext &ec, Event &ev)
#else
void QueryTqlHandler::operator()(EventContext &ec, Event &ev)
#endif
{
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		FetchOption fetchOption;
		util::String query(alloc);

		decodeFetchOption(in, fetchOption);
		decodeStringData<util::String>(in, query);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
			optionPart.forUpdate_ ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;

		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), optionPart.forUpdate_);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);
		checkFetchOption(fetchOption);


		TransactionContext &txn = transactionManager_->put(alloc, request.pId_,
			request.clientId_, request.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerSchemaVersion(container, request.schemaVersionId_);
		response.rs_ = dataStore_->createResultSet(
			txn, txn.getContainerId(), container->getVersionId(), emNow);
		const ResultSetGuard rsGuard(*dataStore_, *response.rs_);
		QueryProcessor::executeTQL(
			txn, *container, fetchOption.limit_, query.c_str(), *response.rs_);

		util::XArray<RowId> lockedRowId(alloc);
		if (optionPart.forUpdate_) {
			container->getLockRowIdList(txn, *response.rs_, lockedRowId);
		}

		QueryProcessor::fetch(
			txn, *container, 0, fetchOption.size_, response.rs_);

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		if (optionPart.forUpdate_) {
			const bool withBegin = (request.cxtSrc_.txnMode_ ==
									TransactionManager::NO_AUTO_COMMIT_BEGIN);
			const bool isAutoCommit =
				(request.cxtSrc_.txnMode_ == TransactionManager::AUTO_COMMIT);
			assert(!(withBegin && isAutoCommit));
			util::XArray<uint8_t> *log =
				ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			const LogSequentialNumber lsn =
				logManager_->putLockRowLog(*log, txn.getPartitionId(),
					txn.getClientId(), txn.getId(), txn.getContainerId(),
					request.cxtSrc_.stmtId_, lockedRowId.size(), lockedRowId,
					txn.getTransationTimeoutInterval(),
					request.cxtSrc_.getMode_, withBegin, isAutoCommit);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		transactionManager_->update(txn, request.cxtSrc_.stmtId_);

		const bool ackWait = executeReplication(request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.cxtSrc_.stmtId_,
			TransactionManager::REPLICATION_ASYNC, NULL, 0,
			logRecordList.data(), logRecordList.size());

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
			ev.getPartitionId(), response.rs_->getFetchNum());
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}



/*!
	@brief Handler Operator
*/
void AppendRowHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		const uint64_t numRow = 1;
		RowData rowData(alloc);

		decodeBinaryData(in, rowData, true);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);


		TransactionContext &txn = transactionManager_->put(alloc, request.pId_,
			request.clientId_, request.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			request.cxtSrc_.containerId_, TIME_SERIES_CONTAINER);
		TimeSeries *container = containerAutoPtr.getTimeSeries();
		checkContainerSchemaVersion(container, request.schemaVersionId_);

		RowId rowKey = UNDEF_ROWID;
		DataStore::PutStatus putStatus;
		container->appendRow(txn, static_cast<uint32_t>(rowData.size()),
			rowData.data(), rowKey, putStatus);
		const bool executed = (putStatus != DataStore::NOT_EXECUTED);
		response.existFlag_ = (putStatus == DataStore::UPDATE);

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		{
			util::XArray<RowId> rowIds(alloc);
			rowIds.push_back(rowKey);
			const bool withBegin = (request.cxtSrc_.txnMode_ ==
									TransactionManager::NO_AUTO_COMMIT_BEGIN);
			const bool isAutoCommit =
				(request.cxtSrc_.txnMode_ == TransactionManager::AUTO_COMMIT);
			assert(!(withBegin && isAutoCommit));
			assert(rowIds.size() == numRow);
			assert((executed && rowKey != UNDEF_ROWID) ||
				   (!executed && rowKey == UNDEF_ROWID));
			util::XArray<uint8_t> *log =
				ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			const LogSequentialNumber lsn = logManager_->putPutRowLog(*log,
				txn.getPartitionId(), txn.getClientId(), txn.getId(),
				txn.getContainerId(), request.cxtSrc_.stmtId_,
				(executed ? rowIds.size() : 0), rowIds, (executed ? numRow : 0),
				rowData, txn.getTransationTimeoutInterval(),
				request.cxtSrc_.getMode_, withBegin, isAutoCommit);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		transactionManager_->update(txn, request.cxtSrc_.stmtId_);

		const bool ackWait = executeReplication(request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), NULL, 0,
			logRecordList.data(), logRecordList.size(), response.existFlag_);

		if (executed) {
			util::XArray<TriggerService::OperationType> operationTypeList(
				alloc);
			operationTypeList.assign(numRow, TriggerService::PUT_ROW);
			TriggerHandler::checkTrigger(*triggerService_, txn, *container, ec,
				operationTypeList, numRow, rowData.data(), rowData.size());
		}

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);

		transactionService_->incrementWriteOperationCount(ev.getPartitionId());
		transactionService_->addRowWriteCount(ev.getPartitionId(), numRow);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}


/*!
	@brief Handler Operator
*/
void GetRowTimeRelatedHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		TimeRelatedCondition condition;

		decodeTimeRelatedConditon(in, condition);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
			optionPart.forUpdate_ ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), optionPart.forUpdate_);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);

		if (optionPart.forUpdate_) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_NOT_EXECUTABLE,
				"row lock (forUpdate) is not applicable");
		}


		TransactionContext &txn = transactionManager_->put(alloc, request.pId_,
			request.clientId_, request.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			request.cxtSrc_.containerId_, TIME_SERIES_CONTAINER);
		TimeSeries *container = containerAutoPtr.getTimeSeries();
		checkContainerSchemaVersion(container, request.schemaVersionId_);

		response.rs_ = dataStore_->createResultSet(
			txn, txn.getContainerId(), container->getVersionId(), emNow);
		const ResultSetGuard rsGuard(*dataStore_, *response.rs_);
		QueryProcessor::get(txn, *container, condition.rowKey_,
			condition.operator_, *response.rs_);

		QueryProcessor::fetch(
			txn, *container, 0, MAX_RESULT_SIZE, response.rs_);
		response.existFlag_ = (response.rs_->getResultNum() > 0);

		transactionManager_->update(txn, request.cxtSrc_.stmtId_);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
			ev.getPartitionId(), response.rs_->getFetchNum());
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void GetRowInterpolateHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		InterpolateCondition condition;

		decodeInterpolateConditon(in, condition);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
			optionPart.forUpdate_ ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), optionPart.forUpdate_);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);

		if (optionPart.forUpdate_) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_NOT_EXECUTABLE,
				"row lock (forUpdate) is not applicable");
		}


		TransactionContext &txn = transactionManager_->put(alloc, request.pId_,
			request.clientId_, request.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			request.cxtSrc_.containerId_, TIME_SERIES_CONTAINER);
		TimeSeries *container = containerAutoPtr.getTimeSeries();
		checkContainerSchemaVersion(container, request.schemaVersionId_);

		response.rs_ = dataStore_->createResultSet(
			txn, txn.getContainerId(), container->getVersionId(), emNow);
		const ResultSetGuard rsGuard(*dataStore_, *response.rs_);
		QueryProcessor::interpolate(txn, *container, condition.rowKey_,
			condition.columnId_, *response.rs_);

		QueryProcessor::fetch(
			txn, *container, 0, MAX_RESULT_SIZE, response.rs_);
		response.existFlag_ = (response.rs_->getResultNum() > 0);

		transactionManager_->update(txn, request.cxtSrc_.stmtId_);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
			ev.getPartitionId(), response.rs_->getFetchNum());
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void AggregateHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		AggregateQuery query;

		decodeAggregateQuery(in, query);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
			optionPart.forUpdate_ ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), optionPart.forUpdate_);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);

		if (optionPart.forUpdate_) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_NOT_EXECUTABLE,
				"row lock (forUpdate) is not applicable");
		}


		TransactionContext &txn = transactionManager_->put(alloc, request.pId_,
			request.clientId_, request.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			request.cxtSrc_.containerId_, TIME_SERIES_CONTAINER);
		TimeSeries *container = containerAutoPtr.getTimeSeries();
		checkContainerSchemaVersion(container, request.schemaVersionId_);

		response.rs_ = dataStore_->createResultSet(
			txn, txn.getContainerId(), container->getVersionId(), emNow);
		const ResultSetGuard rsGuard(*dataStore_, *response.rs_);
		QueryProcessor::aggregate(txn, *container, query.start_, query.end_,
			query.columnId_, query.aggregationType_, *response.rs_);

		QueryProcessor::fetch(
			txn, *container, 0, MAX_RESULT_SIZE, response.rs_);
		response.existFlag_ = (response.rs_->getResultNum() > 0);

		transactionManager_->update(txn, request.cxtSrc_.stmtId_);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
			ev.getPartitionId(), response.rs_->getFetchNum());
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void QueryTimeRangeHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		FetchOption fetchOption;
		RangeQuery query;

		decodeFetchOption(in, fetchOption);
		decodeRangeQuery(in, query);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
			optionPart.forUpdate_ ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), optionPart.forUpdate_);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);
		checkFetchOption(fetchOption);


		TransactionContext &txn = transactionManager_->put(alloc, request.pId_,
			request.clientId_, request.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			request.cxtSrc_.containerId_, TIME_SERIES_CONTAINER);
		TimeSeries *container = containerAutoPtr.getTimeSeries();
		checkContainerSchemaVersion(container, request.schemaVersionId_);

		response.rs_ = dataStore_->createResultSet(
			txn, txn.getContainerId(), container->getVersionId(), emNow);
		const ResultSetGuard rsGuard(*dataStore_, *response.rs_);
		QueryProcessor::search(txn, *container, query.order_,
			fetchOption.limit_, query.start_, query.end_, *response.rs_);

		util::XArray<RowId> lockedRowId(alloc);
		if (optionPart.forUpdate_) {
			container->getLockRowIdList(txn, *response.rs_, lockedRowId);
		}

		QueryProcessor::fetch(
			txn, *container, 0, fetchOption.size_, response.rs_);

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		if (optionPart.forUpdate_) {
			const bool withBegin = (request.cxtSrc_.txnMode_ ==
									TransactionManager::NO_AUTO_COMMIT_BEGIN);
			const bool isAutoCommit =
				(request.cxtSrc_.txnMode_ == TransactionManager::AUTO_COMMIT);
			assert(!(withBegin && isAutoCommit));
			util::XArray<uint8_t> *log =
				ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			const LogSequentialNumber lsn =
				logManager_->putLockRowLog(*log, txn.getPartitionId(),
					txn.getClientId(), txn.getId(), txn.getContainerId(),
					request.cxtSrc_.stmtId_, lockedRowId.size(), lockedRowId,
					txn.getTransationTimeoutInterval(),
					request.cxtSrc_.getMode_, withBegin, isAutoCommit);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		transactionManager_->update(txn, request.cxtSrc_.stmtId_);

		const bool ackWait = executeReplication(request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.cxtSrc_.stmtId_,
			TransactionManager::REPLICATION_ASYNC, NULL, 0,
			logRecordList.data(), logRecordList.size());

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
			ev.getPartitionId(), response.rs_->getFetchNum());
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void QueryTimeSamplingHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		FetchOption fetchOption;
		SamplingQuery query(alloc);

		decodeFetchOption(in, fetchOption);
		decodeSamplingQuery(in, query);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
			optionPart.forUpdate_ ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), optionPart.forUpdate_);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);
		checkFetchOption(fetchOption);


		TransactionContext &txn = transactionManager_->put(alloc, request.pId_,
			request.clientId_, request.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			request.cxtSrc_.containerId_, TIME_SERIES_CONTAINER);
		TimeSeries *container = containerAutoPtr.getTimeSeries();
		checkContainerSchemaVersion(container, request.schemaVersionId_);

		response.rs_ = dataStore_->createResultSet(
			txn, txn.getContainerId(), container->getVersionId(), emNow);
		const ResultSetGuard rsGuard(*dataStore_, *response.rs_);
		QueryProcessor::sample(txn, *container, fetchOption.limit_,
			query.start_, query.end_, query.toSamplingOption(), *response.rs_);

		util::XArray<RowId> lockedRowId(alloc);
		if (optionPart.forUpdate_) {
			container->getLockRowIdList(txn, *response.rs_, lockedRowId);
		}

		QueryProcessor::fetch(
			txn, *container, 0, fetchOption.size_, response.rs_);

		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);
		if (optionPart.forUpdate_) {
			const bool withBegin = (request.cxtSrc_.txnMode_ ==
									TransactionManager::NO_AUTO_COMMIT_BEGIN);
			const bool isAutoCommit =
				(request.cxtSrc_.txnMode_ == TransactionManager::AUTO_COMMIT);
			assert(!(withBegin && isAutoCommit));
			util::XArray<uint8_t> *log =
				ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			const LogSequentialNumber lsn =
				logManager_->putLockRowLog(*log, txn.getPartitionId(),
					txn.getClientId(), txn.getId(), txn.getContainerId(),
					request.cxtSrc_.stmtId_, lockedRowId.size(), lockedRowId,
					txn.getTransationTimeoutInterval(),
					request.cxtSrc_.getMode_, withBegin, isAutoCommit);
			partitionTable_->setLSN(txn.getPartitionId(), lsn);
			logRecordList.push_back(log);
		}

		transactionManager_->update(txn, request.cxtSrc_.stmtId_);

		const bool ackWait = executeReplication(request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.cxtSrc_.stmtId_,
			TransactionManager::REPLICATION_ASYNC, NULL, 0,
			logRecordList.data(), logRecordList.size());

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, ackWait);

		transactionService_->incrementReadOperationCount(ev.getPartitionId());
		transactionService_->addRowReadCount(
			ev.getPartitionId(), response.rs_->getFetchNum());
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void FetchResultSetHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		ResultSetId rsId;
		decodeLongData<ResultSetId>(in, rsId);

		ResultSize startPos, fetchNum;
		decodeLongData<ResultSize>(in, startPos);
		decodeLongData<ResultSize>(in, fetchNum);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), ANY_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);


		TransactionContext &txn = transactionManager_->put(alloc, request.pId_,
			request.clientId_, request.cxtSrc_, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerSchemaVersion(container, request.schemaVersionId_);

		response.rs_ = dataStore_->getResultSet(txn, rsId);
		if (response.rs_ == NULL) {
			GS_THROW_USER_ERROR(
				GS_ERROR_DS_DS_RESULT_ID_INVALID, "(rsId=" << rsId << ")");
		}

		const ResultSetGuard rsGuard(*dataStore_, *response.rs_);

		QueryProcessor::fetch(
			txn, *container, startPos, fetchNum, response.rs_);

		transactionManager_->update(txn, request.cxtSrc_.stmtId_);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);

		transactionService_->addRowReadCount(
			ev.getPartitionId(), response.rs_->getFetchNum());
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void CloseResultSetHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		ResultSetId rsId;
		decodeLongData<ResultSetId>(in, rsId);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), ANY_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);

		dataStore_->closeResultSet(request.pId_, rsId);

		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_SUCCESS, request, response, NO_REPLICATION);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void MultiCreateTransactionContextHandler::operator()(
	EventContext &ec, Event &ev)

{
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		ConnectionOption &connOption =
			ev.getSenderND().getUserData<ConnectionOption>();

		util::XArray<SessionCreationEntry> entryList(alloc);

		const bool withId = decodeMultiTransactionCreationEntry(in, entryList);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);

		Event reply(ec, ev.getType(), ev.getPartitionId());

		EventByteOutStream out = encodeCommonPart(
			reply, request.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS);
		encodeIntData<uint32_t>(out, static_cast<uint32_t>(entryList.size()));

		for (size_t i = 0; i < entryList.size(); i++) {
			SessionCreationEntry &entry = entryList[i];

			const ClientId clientId(request.clientId_.uuid_, entry.sessionId_);

			if (!withId) {
				const util::StackAllocator::Scope scope(alloc);

				assert(entry.containerName_ != NULL);
				const TransactionManager::ContextSource src(request.stmtType_);
				TransactionContext &txn = transactionManager_->put(
					alloc, request.pId_, clientId, src, now, emNow);
				ExtendedContainerName extContainerName(alloc, connOption.dbId_,
					optionPart.dbName_.c_str(), entry.containerName_->c_str());
				ContainerAutoPtr containerAutoPtr(txn, dataStore_,
					txn.getPartitionId(), extContainerName, ANY_CONTAINER);
				BaseContainer *container = containerAutoPtr.getBaseContainer();
				checkDbAccessible(connOption.dbName_, optionPart.dbName_);
				ContainerAccessMode containerAccessMode =
					getContainerAccessMode(connOption.userType_,
						optionPart.systemMode_, connOption.requestType_, false);
				if (container != NULL &&
					!isAccessibleMode(
						containerAccessMode, container->getAttribute())) {
					container = NULL;
				}
				checkContainerExistence(container);
				entry.containerId_ = container->getContainerId();
			}

			const TransactionManager::ContextSource cxtSrc(request.stmtType_,
				request.cxtSrc_.stmtId_, entry.containerId_,
				TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL,
				TransactionManager::CREATE, TransactionManager::AUTO_COMMIT);

			TransactionContext &txn = transactionManager_->put(
				alloc, request.pId_, clientId, cxtSrc, now, emNow);

			encodeLongData<ContainerId>(out, txn.getContainerId());
		}

		ec.getEngine().send(reply, ev.getSenderND());
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

bool MultiCreateTransactionContextHandler::decodeMultiTransactionCreationEntry(
	util::ByteStream<util::ArrayInStream> &in,
	util::XArray<SessionCreationEntry> &entryList) {
	try {
		util::StackAllocator &alloc = *entryList.get_allocator().base();

		bool withId;
		decodeBooleanData(in, withId);

		int32_t entryCount;
		in >> entryCount;
		entryList.reserve(static_cast<size_t>(entryCount));

		for (int32_t i = 0; i < entryCount; i++) {
			SessionCreationEntry entry;

			if (withId) {
				in >> entry.containerId_;
			}
			else {
				entry.containerName_ = ALLOC_NEW(alloc) util::String(alloc);
				in >> *entry.containerName_;
			}

			in >> entry.sessionId_;

			entryList.push_back(entry);
		}

		return withId;
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Handler Operator
*/
void MultiCloseTransactionContextHandler::operator()(
	EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		util::XArray<SessionCloseEntry> entryList(alloc);

		decodeMultiTransactionCloseEntry(in, entryList);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);

		Event reply(ec, ev.getType(), ev.getPartitionId());

		encodeCommonPart(reply, request.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS);

		util::XArray<ClientId> closedResourceIds(alloc);
		util::XArray<const util::XArray<uint8_t> *> logRecordList(alloc);

		for (size_t i = 0; i < entryList.size(); i++) {
			const SessionCloseEntry &entry = entryList[i];

			const ClientId clientId(request.clientId_.uuid_, entry.sessionId_);

			try {
				TransactionContext &txn =
					transactionManager_->get(alloc, request.pId_, clientId);
				const DataStore::Latch latch(
					txn, txn.getPartitionId(), dataStore_, clusterService_);

				if (txn.isActive()) {
					BaseContainer *container = NULL;

					try {
						container =
							dataStore_->getContainer(txn, txn.getPartitionId(),
								txn.getContainerId(), ANY_CONTAINER);
					}
					catch (UserException &e) {
						UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
							"Container not found. (pId="
								<< txn.getPartitionId()
								<< ", clientId=" << txn.getClientId()
								<< ", containerId=" << txn.getContainerId()
								<< ", reason=" << GS_EXCEPTION_MESSAGE(e)
								<< ")");
					}
					StackAllocAutoPtr<BaseContainer> containerAutoPtr(
						txn.getDefaultAllocator(), container);
					if (containerAutoPtr.get() != NULL) {
						util::XArray<uint8_t> *log =
							ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
						const LogSequentialNumber lsn =
							logManager_->putAbortTransactionLog(*log,
								txn.getPartitionId(), txn.getClientId(),
								txn.getId(), txn.getContainerId(),
								entry.stmtId_);
						partitionTable_->setLSN(txn.getPartitionId(), lsn);
						logRecordList.push_back(log);

						transactionManager_->abort(
							txn, *containerAutoPtr.get());
					}

					ec.setPendingPartitionChanged(txn.getPartitionId());
				}

				transactionManager_->remove(request.pId_, clientId);
				closedResourceIds.push_back(clientId);
			}
			catch (ContextNotFoundException &) {
			}
		}

		request.cxtSrc_.getMode_ = TransactionManager::AUTO;
		request.cxtSrc_.txnMode_ = TransactionManager::AUTO_COMMIT;
		TransactionContext &txn = transactionManager_->put(alloc, request.pId_,
			request.clientId_, request.cxtSrc_, now, emNow);
		const bool ackWait = executeReplication(request, ec, alloc,
			ev.getSenderND(), txn, ev.getType(), request.cxtSrc_.stmtId_,
			transactionManager_->getReplicationMode(), closedResourceIds.data(),
			closedResourceIds.size(), logRecordList.data(),
			logRecordList.size());

		if (!ackWait) {
			ec.getEngine().send(reply, ev.getSenderND());
		}
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

void MultiCloseTransactionContextHandler::decodeMultiTransactionCloseEntry(
	util::ByteStream<util::ArrayInStream> &in,
	util::XArray<SessionCloseEntry> &entryList) {
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
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Handles error
*/
void MultiStatementHandler::handleExecuteError(util::StackAllocator &alloc,
	PartitionId pId, const ClientId &clientId,
	const TransactionManager::ContextSource &src, Progress &progress,
	std::exception &, EventType stmtType, const char8_t *executionName) {
#define TXN_TRACE_MULTI_OP_ERROR_MESSAGE(                                     \
	category, pId, clientId, src, stmtType, executionName)                    \
	" (" << category << ", stmtType=" << stmtType                             \
		 << ", executionName=" << executionName << ", pId=" << pId            \
		 << ", clientId=" << clientId << ", containerId=" << src.containerId_ \
		 << ")"

#define TXN_TRACE_MULTI_OP_ERROR(                                    \
	cause, category, pId, clientId, src, stmtType, executionName)    \
	GS_EXCEPTION_MESSAGE(cause) << TXN_TRACE_MULTI_OP_ERROR_MESSAGE( \
		category, pId, clientId, src, stmtType, executionName)

	try {
		try {
			throw;
		}
		catch (StatementAlreadyExecutedException &e) {
			UTIL_TRACE_EXCEPTION_INFO(
				TRANSACTION_SERVICE, e,
				TXN_TRACE_MULTI_OP_ERROR(e, "Statement already executed", pId,
					clientId, src, stmtType, executionName));
			progress.containerResult_.push_back(
				CONTAINER_RESULT_ALREADY_EXECUTED);
		}
		catch (LockConflictException &e) {
			UTIL_TRACE_EXCEPTION_INFO(TRANSACTION_SERVICE, e,
				TXN_TRACE_MULTI_OP_ERROR(e, "Lock conflicted", pId, clientId,
										  src, stmtType, executionName));

			if (src.txnMode_ == TransactionManager::NO_AUTO_COMMIT_BEGIN) {
				try {
					TransactionContext &txn =
						transactionManager_->get(alloc, pId, clientId);
					util::XArray<uint8_t> *log =
						ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
					if (abortOnError(txn, *log)) {
						progress.logRecordList_.push_back(log);

						if (stmtType == PUT_MULTIPLE_CONTAINER_ROWS) {
							assert(
								progress.mputStartStmtId_ != UNDEF_STATEMENTID);
							transactionManager_->update(
								txn, progress.mputStartStmtId_);
						}
					}
				}
				catch (ContextNotFoundException &) {
					UTIL_TRACE_EXCEPTION_INFO(
						TRANSACTION_SERVICE, e,
						TXN_TRACE_MULTI_OP_ERROR(e, "Context not found", pId,
							clientId, src, stmtType, executionName));
				}
			}

			progress.lockConflicted_ = true;
		}
		catch (std::exception &e) {
			if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
				GS_RETHROW_SYSTEM_ERROR(
					e, TXN_TRACE_MULTI_OP_ERROR_MESSAGE("System error", pId,
						   clientId, src, stmtType, executionName));
			}
			else {
				UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
					TXN_TRACE_MULTI_OP_ERROR(e, "User error", pId, clientId,
										 src, stmtType, executionName));

				TXN_FAILOVER_STATE_SET(
					FailoverTestUtil::STATE_10, stmtType, src);

				try {
					TransactionContext &txn =
						transactionManager_->get(alloc, pId, clientId);
					util::XArray<uint8_t> *log =
						ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
					if (abortOnError(txn, *log)) {
						progress.logRecordList_.push_back(log);
					}
				}
				catch (ContextNotFoundException &) {
					UTIL_TRACE_EXCEPTION_INFO(
						TRANSACTION_SERVICE, e,
						TXN_TRACE_MULTI_OP_ERROR(e, "Context not found", pId,
							clientId, src, stmtType, executionName));
				}

				TXN_FAILOVER_STATE_SET(
					FailoverTestUtil::STATE_11, stmtType, src);

				progress.lastExceptionData_.clear();
				util::XArrayByteOutStream out(
					util::XArrayOutStream<>(progress.lastExceptionData_));
				encodeException(out, e, TXN_DETAIL_EXCEPTION_HIDDEN);

				progress.containerResult_.push_back(CONTAINER_RESULT_FAIL);
			}
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}

#undef TXN_TRACE_MULTI_OP_ERROR
}

/*!
	@brief Handles error
*/
void MultiStatementHandler::handleWholeError(EventContext &ec,
	util::StackAllocator &alloc, const Event &ev, const FixedPart &request,
	std::exception &e) {
	try {
		throw;
	}
	catch (EncodeDecodeException &) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			GS_EXCEPTION_MESSAGE(e) << " (Failed to encode or decode message"
									   ", nd="
									<< ev.getSenderND()
									<< ", pId=" << ev.getPartitionId()
									<< ", eventType=" << ev.getType() << ")");
	}
	catch (DenyException &) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			GS_EXCEPTION_MESSAGE(e)
				<< " (Request denied"
				   ", nd="
				<< ev.getSenderND() << ", pId=" << ev.getPartitionId()
				<< ", eventType=" << ev.getType()
				<< ", stmtId=" << request.cxtSrc_.stmtId_ << ")");
		replyError(ec, alloc, ev.getSenderND(), ev.getType(),
			TXN_STATEMENT_DENY, request, e);
	}
	catch (std::exception &) {
		const bool isCritical = GS_EXCEPTION_CHECK_CRITICAL(e);
		const char8_t *category = (isCritical ? "System error" : "User error");
		const StatementExecStatus returnStatus =
			(isCritical ? TXN_STATEMENT_NODE_ERROR : TXN_STATEMENT_ERROR);

		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			GS_EXCEPTION_MESSAGE(e)
				<< " (" << category << ", nd=" << ev.getSenderND() << ", pId="
				<< ev.getPartitionId() << ", eventType=" << ev.getType()
				<< ", stmtId=" << request.cxtSrc_.stmtId_ << ")");
		replyError(ec, alloc, ev.getSenderND(), ev.getType(), returnStatus,
			request, e);

		if (isCritical) {
			clusterService_->setError(ec, &e);
		}
	}
}

void MultiStatementHandler::decodeContainerOptionPart(
	util::ByteStream<util::ArrayInStream> &in, const FixedPart &fixedPart,
	OptionPart &optionPart) {
	try {
		optionPart.txnTimeoutInterval_ = -1;
		decodeOptionPart(in, optionPart);

		if (optionPart.txnTimeoutInterval_ < 0) {
			optionPart.txnTimeoutInterval_ =
				fixedPart.cxtSrc_.txnTimeoutInterval_;
		}
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Handler Operator
*/
void MultiPutHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		util::XArray<const MessageSchema *> schemaList(alloc);
		util::XArray<const RowSetRequest *> rowSetList(alloc);

		decodeMultiSchema(in, schemaList);
		decodeMultiRowSet(in, request, schemaList, rowSetList);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_OWNER;
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), IMMEDIATE_CONSISTENCY);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);

		Event reply(ec, request.stmtType_, request.pId_);
		encodeCommonPart(reply, request.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS);

		CheckedSchemaIdSet checkedSchemaIdSet(alloc);
		checkedSchemaIdSet.reserve(rowSetList.size());
		Progress progress(alloc);

		TXN_FAILOVER_STATE_SET(
			FailoverTestUtil::STATE_0, ev.getType(), request.cxtSrc_);

		PutRowOption putRowOption = optionPart.putRowOption_;

		bool succeeded = true;
		for (size_t i = 0; i < rowSetList.size(); i++) {
			const RowSetRequest &rowSetRequest = *(rowSetList[i]);
			const MessageSchema &messageSchema =
				*(schemaList[rowSetRequest.schemaIndex_]);

			execute(ec, request, rowSetRequest, messageSchema,
				checkedSchemaIdSet, progress, putRowOption);

			if (!progress.lastExceptionData_.empty() ||
				progress.lockConflicted_) {
				succeeded = false;
				break;
			}
		}

		bool ackWait = false;
		{
			TransactionContext &txn = transactionManager_->put(alloc,
				request.pId_, request.clientId_, request.cxtSrc_, now, emNow);

			const int32_t replMode =
				succeeded ? transactionManager_->getReplicationMode()
						  : TransactionManager::REPLICATION_ASYNC;
			const NodeDescriptor &clientND =
				succeeded ? ev.getSenderND() : NodeDescriptor::EMPTY_ND;

			TXN_FAILOVER_STATE_SET_IF(!succeeded, FailoverTestUtil::STATE_8,
				FailoverTestUtil::STATE_3, ev.getType(), request.cxtSrc_);

			ackWait = executeReplication(request, ec, alloc, clientND, txn,
				request.stmtType_, request.cxtSrc_.stmtId_, replMode, NULL, 0,
				progress.logRecordList_.data(), progress.logRecordList_.size());

			TXN_FAILOVER_STATE_SET_IF(!succeeded, FailoverTestUtil::STATE_9,
				FailoverTestUtil::STATE_IGNORE, ev.getType(), request.cxtSrc_);
		}

		if (progress.lockConflicted_) {
			ec.getEngine().addPending(
				ev, EventEngine::RESUME_AT_PARTITION_CHANGE);
		}
		else if (succeeded) {
			TXN_FAILOVER_STATE_SET(
				FailoverTestUtil::STATE_4, ev.getType(), request.cxtSrc_);

			if (!ackWait) {
				ec.getEngine().send(reply, ev.getSenderND());
			}
		}
		else {
			TXN_FAILOVER_STATE_SET(
				FailoverTestUtil::STATE_12, ev.getType(), request.cxtSrc_);

			Event errorReply(ec, request.stmtType_, request.pId_);
			EventByteOutStream out = encodeCommonPart(
				errorReply, request.cxtSrc_.stmtId_, TXN_STATEMENT_ERROR);
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
	catch (std::exception &e) {
		handleWholeError(ec, alloc, ev, request, e);
	}
}

void MultiPutHandler::execute(EventContext &ec, const FixedPart &request,
	const RowSetRequest &rowSetRequest, const MessageSchema &schema,
	CheckedSchemaIdSet &idSet, Progress &progress, PutRowOption putRowOption) {
	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	const ClientId clientId(request.clientId_.uuid_, rowSetRequest.sessionId_);
	const TransactionManager::ContextSource src =
		createContextSource(request, rowSetRequest);
	const util::XArray<uint8_t> &multiRowData = rowSetRequest.rowSetData_;
	const uint64_t numRow = rowSetRequest.rowCount_;
	assert(numRow > 0);
	progress.mputStartStmtId_ = UNDEF_STATEMENTID;

	try {
		const bool STATEMENTID_NO_CHECK = true;
		TransactionContext &txn = transactionManager_->put(alloc, request.pId_,
			clientId, src, now, emNow, STATEMENTID_NO_CHECK);
		progress.mputStartStmtId_ = txn.getLastStatementId();
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			txn.getContainerId(), ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerExistence(container);

		const size_t capacityBefore = idSet.base().capacity();
		UNUSED_VARIABLE(capacityBefore);

		checkSchema(txn, *container, schema, rowSetRequest.schemaIndex_, idSet);
		assert(idSet.base().capacity() == capacityBefore);

		const bool NO_VALIDATE_ROW_IMAGE = false;
		InputMessageRowStore inputMessageRowStore(
			dataStore_->getValueLimitConfig(), container->getColumnInfoList(),
			container->getColumnNum(),
			const_cast<uint8_t *>(multiRowData.data()),
			static_cast<uint32_t>(multiRowData.size()), numRow,
			container->getRowFixedDataSize(), NO_VALIDATE_ROW_IMAGE);

		util::XArray<RowId> rowIds(alloc);
		rowIds.assign(static_cast<size_t>(1), UNDEF_ROWID);
		util::XArray<TriggerService::OperationType> operationTypeList(alloc);
		operationTypeList.assign(
			static_cast<size_t>(1), TriggerService::PUT_ROW);
		util::XArray<uint8_t> rowData(alloc);

		for (StatementId rowStmtId = src.stmtId_; inputMessageRowStore.next();
			 rowStmtId++) {
			try {
				TransactionManager::ContextSource rowSrc = src;
				rowSrc.stmtId_ = rowStmtId;
				txn = transactionManager_->put(
					alloc, request.pId_, clientId, rowSrc, now, emNow);
			}
			catch (StatementAlreadyExecutedException &e) {
				UTIL_TRACE_EXCEPTION_INFO(TRANSACTION_SERVICE, e,
					"Row already put. (pId="
						<< request.pId_
						<< ", eventType=" << PUT_MULTIPLE_CONTAINER_ROWS
						<< ", stmtId=" << rowStmtId << ", clientId=" << clientId
						<< ", containerId="
						<< ((src.containerId_ == UNDEF_CONTAINERID)
								   ? 0
								   : src.containerId_)
						<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");

				continue;
			}

			rowData.clear();
			inputMessageRowStore.getCurrentRowData(rowData);

			rowIds[0] = UNDEF_ROWID;
			DataStore::PutStatus putStatus;
			container->putRow(txn, static_cast<uint32_t>(rowData.size()),
				rowData.data(), rowIds[0], putStatus, putRowOption);
			const bool executed = (putStatus != DataStore::NOT_EXECUTED);

			TXN_FAILOVER_STATE_SET_IF((inputMessageRowStore.position() > 1),
				FailoverTestUtil::STATE_2, FailoverTestUtil::STATE_1,
				PUT_MULTIPLE_CONTAINER_ROWS, src);

			{
				const bool withBegin =
					(src.txnMode_ == TransactionManager::NO_AUTO_COMMIT_BEGIN);
				const bool isAutoCommit =
					(src.txnMode_ == TransactionManager::AUTO_COMMIT);
				assert(!(withBegin && isAutoCommit));
				assert(rowIds.size() == 1);
				assert((executed && rowIds[0] != UNDEF_ROWID) ||
					   (!executed && rowIds[0] == UNDEF_ROWID));
				util::XArray<uint8_t> *log =
					ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
				const LogSequentialNumber lsn = logManager_->putPutRowLog(*log,
					txn.getPartitionId(), txn.getClientId(), txn.getId(),
					txn.getContainerId(), rowStmtId,
					(executed ? rowIds.size() : 0), rowIds, (executed ? 1 : 0),
					rowData, txn.getTransationTimeoutInterval(), src.getMode_,
					withBegin, isAutoCommit);
				partitionTable_->setLSN(txn.getPartitionId(), lsn);
				progress.logRecordList_.push_back(log);
			}

			TXN_FAILOVER_STATE_SET(
				FailoverTestUtil::STATE_3, PUT_MULTIPLE_CONTAINER_ROWS, src);

			transactionManager_->update(txn, rowStmtId);

			if (executed) {
				TriggerHandler::checkTrigger(*triggerService_, txn, *container,
					ec, operationTypeList, 1, rowData.data(), rowData.size());
			}
		}

		progress.containerResult_.push_back(CONTAINER_RESULT_SUCCESS);
		progress.totalRowCount_ += numRow;
	}
	catch (std::exception &e) {
		handleExecuteError(alloc, request.pId_, clientId, src, progress, e,
			PUT_MULTIPLE_CONTAINER_ROWS, "put");
	}
}

void MultiPutHandler::checkSchema(TransactionContext &txn,
	BaseContainer &container, const MessageSchema &schema,
	int32_t localSchemaId, CheckedSchemaIdSet &idSet) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	ColumnSchemaId schemaId = container.getColumnSchemaId();

	if (schemaId != UNDEF_COLUMN_SCHEMAID &&
		idSet.find(CheckedSchemaId(localSchemaId, schemaId)) != idSet.end()) {
		return;
	}


	const uint32_t columnCount = container.getColumnNum();
	ColumnInfo *infoList = container.getColumnInfoList();

	if (columnCount != schema.getColumnCount()) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID, "");
	}

	util::XArray<char8_t> name1(alloc);
	util::XArray<char8_t> name2(alloc);

	for (uint32_t i = 0; i < columnCount; i++) {
		ColumnInfo &info = infoList[i];

		if (info.getColumnType() != schema.getColumnFullType(i)) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID, "");
		}

		if (info.isKey() ^ (i == schema.getRowKeyColumnId())) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID, "");
		}

		const char8_t *name1Ptr =
			info.getColumnName(txn, *(container.getObjectManager()));
		const char8_t *name2Ptr = schema.getColumnName(i).c_str();

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

	if (schemaId != UNDEF_COLUMN_SCHEMAID) {
		idSet.insert(CheckedSchemaId(localSchemaId, schemaId));
	}
}

TransactionManager::ContextSource MultiPutHandler::createContextSource(
	const FixedPart &request, const RowSetRequest &rowSetRequest) {
	return TransactionManager::ContextSource(request.stmtType_,
		rowSetRequest.stmtId_, rowSetRequest.containerId_,
		rowSetRequest.option_.txnTimeoutInterval_, rowSetRequest.getMode_,
		rowSetRequest.txnMode_);
}

void MultiPutHandler::decodeMultiSchema(
	util::ByteStream<util::ArrayInStream> &in,
	util::XArray<const MessageSchema *> &schemaList) {
	try {
		util::StackAllocator &alloc = *schemaList.get_allocator().base();

		int32_t headCount;
		in >> headCount;
		schemaList.reserve(static_cast<size_t>(headCount));

		for (int32_t i = 0; i < headCount; i++) {
			ContainerType containerType;
			decodeEnumData<ContainerType>(in, containerType);

			const char8_t *dummyContainerName = "_";
			MessageSchema *schema = ALLOC_NEW(alloc) MessageSchema(alloc,
				dataStore_->getValueLimitConfig(), dummyContainerName, in);

			schemaList.push_back(schema);
		}
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

void MultiPutHandler::decodeMultiRowSet(
	util::ByteStream<util::ArrayInStream> &in, const FixedPart &request,
	const util::XArray<const MessageSchema *> &schemaList,
	util::XArray<const RowSetRequest *> &rowSetList) {
	try {
		util::StackAllocator &alloc = *rowSetList.get_allocator().base();

		int32_t bodyCount;
		in >> bodyCount;
		rowSetList.reserve(static_cast<size_t>(bodyCount));

		for (int32_t i = 0; i < bodyCount; i++) {
			RowSetRequest *subRequest = ALLOC_NEW(alloc) RowSetRequest(alloc);

			in >> subRequest->stmtId_;
			in >> subRequest->containerId_;
			in >> subRequest->sessionId_;

			in >> subRequest->getMode_;
			in >> subRequest->txnMode_;

			decodeContainerOptionPart(in, request, subRequest->option_);

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
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Handler Operator
*/
void MultiGetHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		ConnectionOption &connOption =
			ev.getSenderND().getUserData<ConnectionOption>();

		util::XArray<SearchEntry> searchList(alloc);

		ContainerAccessMode containerAccessMode =
			getContainerAccessMode(connOption.userType_, optionPart.systemMode_,
				connOption.requestType_, false);
		checkDbAccessible(connOption.dbName_, optionPart.dbName_);

		decodeMultiSearchEntry(in, request.pId_, connOption.dbId_,
			optionPart.dbName_.c_str(), containerAccessMode, searchList);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole =
			optionPart.forUpdate_ ? PROLE_OWNER : (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkConsistency(ev.getSenderND(), optionPart.forUpdate_);
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);

		if (optionPart.forUpdate_) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_NOT_EXECUTABLE,
				"row lock (forUpdate) is not applicable");
		}

		Event reply(ec, ev.getType(), ev.getPartitionId());

		EventByteOutStream out = encodeCommonPart(
			reply, request.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS);

		SchemaMap schemaMap(alloc);
		buildSchemaMap(request.pId_, searchList, schemaMap, out);

		const size_t bodyTopPos = out.base().position();
		uint32_t entryCount = 0;
		encodeIntData<uint32_t>(out, entryCount);

		Progress progress(alloc);

		bool succeeded = true;
		for (size_t i = 0; i < searchList.size(); i++) {
			const SearchEntry &entry = searchList[i];

			entryCount += execute(ec, request, entry, schemaMap, out, progress);

			if (!progress.lastExceptionData_.empty() ||
				progress.lockConflicted_) {
				succeeded = false;
				break;
			}
		}


		if (progress.lockConflicted_) {
			ec.getEngine().addPending(
				ev, EventEngine::RESUME_AT_PARTITION_CHANGE);
		}
		else if (succeeded) {
			const size_t bodyEndPos = out.base().position();
			out.base().position(bodyTopPos);
			encodeIntData<uint32_t>(out, entryCount);
			out.base().position(bodyEndPos);
			ec.getEngine().send(reply, ev.getSenderND());
		}
		else {
			Event errorReply(ec, request.stmtType_, request.pId_);
			EventByteOutStream out = encodeCommonPart(
				errorReply, request.cxtSrc_.stmtId_, TXN_STATEMENT_ERROR);
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
	catch (std::exception &e) {
		handleWholeError(ec, alloc, ev, request, e);
	}
}

uint32_t MultiGetHandler::execute(EventContext &ec, const FixedPart &request,
	const SearchEntry &entry, const SchemaMap &schemaMap,
	EventByteOutStream &replyOut, Progress &progress) {
	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	const ClientId clientId(request.clientId_.uuid_, entry.sessionId_);
	const TransactionManager::ContextSource src =
		createContextSource(request, entry);

	const char8_t *getType =
		(entry.predicate_->distinctKeys_ == NULL) ? "rangeKey" : "distinctKey";

	uint32_t entryCount = 0;

	try {
		const util::StackAllocator::Scope scope(alloc);

		TransactionContext &txn = transactionManager_->put(
			alloc, request.pId_, clientId, src, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		ContainerAutoPtr containerAutoPtr(txn, dataStore_, txn.getPartitionId(),
			entry.containerId_, ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();

		const RowKeyPredicate &predicate = *entry.predicate_;
		checkContainerRowKey(container, predicate);

		if (entry.predicate_->distinctKeys_ == NULL) {
			ResultSet *rs = dataStore_->createResultSet(txn,
				container->getContainerId(), container->getVersionId(), emNow);
			const ResultSetGuard rsGuard(*dataStore_, *rs);

			QueryProcessor::search(txn, *container, MAX_RESULT_SIZE,
				predicate.startKey_, predicate.finishKey_, *rs);
			rs->setResultType(RESULT_ROWSET);

			if (rs->getResultNum() > 0) {

				QueryProcessor::fetch(txn, *container, 0, MAX_RESULT_SIZE, rs);

				encodeEntry(*entry.containerName_, container->getContainerId(),
					schemaMap, *rs, replyOut);

				entryCount++;

				progress.totalRowCount_ += rs->getResultNum();
			}
		}
		else {
			const RowKeyDataList &distinctKeys = *predicate.distinctKeys_;

			for (RowKeyDataList::const_iterator it = distinctKeys.begin();
				 it != distinctKeys.end(); ++it) {
				const util::StackAllocator::Scope scope(alloc);

				const RowKeyData &rowKey = **it;

				ResultSet *rs = dataStore_->createResultSet(txn,
					container->getContainerId(), container->getVersionId(),
					emNow);
				const ResultSetGuard rsGuard(*dataStore_, *rs);

				QueryProcessor::get(txn, *container,
					static_cast<uint32_t>(rowKey.size()), rowKey.data(), *rs);
				rs->setResultType(RESULT_ROWSET);

				if (rs->getResultNum() > 0) {

					QueryProcessor::fetch(
						txn, *container, 0, MAX_RESULT_SIZE, rs);

					encodeEntry(*entry.containerName_,
						container->getContainerId(), schemaMap, *rs, replyOut);

					entryCount++;

					progress.totalRowCount_ += rs->getFetchNum();
				}
			}
		}

	}
	catch (std::exception &e) {
		handleExecuteError(alloc, request.pId_, clientId, src, progress, e,
			GET_MULTIPLE_CONTAINER_ROWS, getType);
	}

	return entryCount;
}

void MultiGetHandler::buildSchemaMap(PartitionId pId,
	const util::XArray<SearchEntry> &searchList, SchemaMap &schemaMap,
	EventByteOutStream &out) {
	typedef util::XArray<uint8_t> SchemaData;
	typedef util::Map<ColumnSchemaId, LocalSchemaId> LocalIdMap;
	typedef LocalIdMap::iterator LocalIdMapItr;

	util::StackAllocator &alloc = *schemaMap.get_allocator().base();

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

		const ContainerId containerId = it->containerId_;
		const TransactionManager::ContextSource src(GET_MULTIPLE_CONTAINER_ROWS,
			0, containerId, TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL,
			TransactionManager::AUTO, TransactionManager::AUTO_COMMIT);
		TransactionContext &txn =
			transactionManager_->put(alloc, pId, TXN_EMPTY_CLIENTID, src, 0, 0);

		ContainerAutoPtr containerAutoPtr(
			txn, dataStore_, pId, containerId, ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
		checkContainerExistence(container);

		const ContainerType containerType = container->getContainerType();
		ColumnSchemaId columnSchemaId = container->getColumnSchemaId();


		if (columnSchemaId == UNDEF_COLUMN_SCHEMAID) {
			break;
		}

		LocalIdMapItr idIt = localIdMap.find(columnSchemaId);
		LocalSchemaId localId;
		if (idIt == localIdMap.end()) {
			localId = schemaCount;

			schemaBuf.clear();
			{
				util::XArrayByteOutStream schemaOut = util::XArrayByteOutStream(
					util::XArrayOutStream<>(schemaBuf));
				schemaOut << static_cast<uint8_t>(containerType);
			}

			const bool optionIncluded = false;
			container->getContainerInfo(txn, schemaBuf, optionIncluded);

			out.writeAll(schemaBuf.data(), schemaBuf.size());

			schemaCount++;
		}
		else {
			localId = idIt->second;
		}

		localIdMap.insert(std::make_pair(columnSchemaId, localId));
		schemaMap.insert(std::make_pair(containerId, localId));
	}


	out.base().position(outStartPos);
	schemaMap.clear();


	typedef util::XArray<SchemaData *> SchemaList;
	typedef util::MultiMap<uint32_t, LocalSchemaId> DigestMap;
	typedef DigestMap::iterator DigestMapItr;

	SchemaList schemaList(alloc);
	DigestMap digestMap(alloc);

	for (util::XArray<SearchEntry>::const_iterator it = searchList.begin();
		 it != searchList.end(); ++it) {
		const ContainerId containerId = it->containerId_;
		const TransactionManager::ContextSource src(GET_MULTIPLE_CONTAINER_ROWS,
			0, containerId, TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL,
			TransactionManager::AUTO, TransactionManager::AUTO_COMMIT);
		TransactionContext &txn =
			transactionManager_->put(alloc, pId, TXN_EMPTY_CLIENTID, src, 0, 0);

		ContainerAutoPtr containerAutoPtr(
			txn, dataStore_, pId, containerId, ANY_CONTAINER);
		BaseContainer *container = containerAutoPtr.getBaseContainer();
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
			SchemaData &targetSchema = *schemaList[rangeIt->second];

			if (targetSchema.size() == schemaBuf.size() &&
				memcmp(targetSchema.data(), schemaBuf.data(),
					schemaBuf.size()) == 0) {
				schemaId = rangeIt->second;
				break;
			}
		}

		if (schemaId < 0) {
			SchemaData *targetSchema = ALLOC_NEW(alloc) SchemaData(alloc);
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
		SchemaData &targetSchema = **it;
		out.writeAll(targetSchema.data(), targetSchema.size());
	}
}

void MultiGetHandler::checkContainerRowKey(
	BaseContainer *container, const RowKeyPredicate &predicate) {
	checkContainerExistence(container);

	if (!container->definedRowKey()) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_COL_ROWKEY_UNDEFINED, "");
	}

	const ColumnInfo &keyColumnInfo =
		container->getColumnInfo(ColumnInfo::ROW_KEY_COLUMN_ID);
	if (predicate.keyType_ != keyColumnInfo.getColumnType()) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_COL_ROWKEY_INVALID, "");
	}
}

TransactionManager::ContextSource MultiGetHandler::createContextSource(
	const FixedPart &request, const SearchEntry &entry) {
	return TransactionManager::ContextSource(request.stmtType_, entry.stmtId_,
		entry.containerId_, request.cxtSrc_.txnTimeoutInterval_, entry.getMode_,
		entry.txnMode_);
}

void MultiGetHandler::decodeMultiSearchEntry(
	util::ByteStream<util::ArrayInStream> &in, PartitionId pId, DatabaseId dbId,
	const char *dbName, ContainerAccessMode containerAccessMode,
	util::XArray<SearchEntry> &searchList) {
	try {
		util::StackAllocator &alloc = *searchList.get_allocator().base();
		util::XArray<const RowKeyPredicate *> predicareList(alloc);

		{
			int32_t headCount;
			in >> headCount;
			predicareList.reserve(static_cast<size_t>(headCount));

			for (int32_t i = 0; i < headCount; i++) {
				const RowKeyPredicate *predicate = ALLOC_NEW(alloc)
					RowKeyPredicate(decodePredicate(in, alloc));
				predicareList.push_back(predicate);
			}
		}

		{
			int32_t bodyCount;
			in >> bodyCount;
			searchList.reserve(static_cast<size_t>(bodyCount));

			for (int32_t i = 0; i < bodyCount; i++) {
				util::String *containerName =
					ALLOC_NEW(alloc) util::String(alloc);
				in >> *containerName;

				int32_t predicareIndex;
				in >> predicareIndex;

				if (predicareIndex < 0 ||
					static_cast<size_t>(predicareIndex) >=
						predicareList.size()) {
					TXN_THROW_DECODE_ERROR(GS_ERROR_TXN_DECODE_FAILED,
						"(predicareIndex=" << predicareIndex << ")");
				}

				const TransactionManager::ContextSource src(
					GET_MULTIPLE_CONTAINER_ROWS);
				TransactionContext &txn = transactionManager_->put(
					alloc, pId, TXN_EMPTY_CLIENTID, src, 0, 0);
				ExtendedContainerName extContainerName(
					alloc, dbId, dbName, containerName->c_str());
				ContainerAutoPtr containerAutoPtr(
					txn, dataStore_, pId, extContainerName, ANY_CONTAINER);
				BaseContainer *container = containerAutoPtr.getBaseContainer();
				if (container != NULL &&
					!isAccessibleMode(
						containerAccessMode, container->getAttribute())) {
					GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
						"Forbidden Container Attribute");
				}

				const ContainerId containerId =
					(container != NULL) ? container->getContainerId()
										: UNDEF_CONTAINERID;
				if (containerId == UNDEF_CONTAINERID) {
					continue;
				}

				SearchEntry entry(containerId, containerName,
					predicareList[static_cast<size_t>(predicareIndex)]);

				searchList.push_back(entry);
			}
		}
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

MultiGetHandler::RowKeyPredicate MultiGetHandler::decodePredicate(
	util::ByteStream<util::ArrayInStream> &in, util::StackAllocator &alloc) {
	int8_t keyType;
	in >> keyType;

	const MessageRowKeyCoder keyCoder(static_cast<ColumnType>(keyType));
	RowKeyPredicate predicate = {
		static_cast<ColumnType>(keyType), NULL, NULL, NULL};

	int8_t predicateType;
	in >> predicateType;

	if (predicateType == PREDICATE_TYPE_RANGE) {
		RowKeyData **keyList[] = {
			&predicate.startKey_, &predicate.finishKey_, NULL};

		for (RowKeyData ***keyPtr = keyList; *keyPtr != NULL; ++keyPtr) {
			int8_t keyIncluded;
			in >> keyIncluded;

			if (keyIncluded) {
				RowKeyData *key = ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
				keyCoder.decode(in, *key);
				**keyPtr = key;
			}
		}
	}
	else if (predicateType == PREDICATE_TYPE_DISTINCT) {
		int32_t keyCount;
		in >> keyCount;

		RowKeyDataList *distinctKeys =
			ALLOC_NEW(alloc) util::XArray<RowKeyData *>(alloc);
		distinctKeys->reserve(static_cast<size_t>(keyCount));

		for (int32_t i = 0; i < keyCount; i++) {
			RowKeyData *key = ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			keyCoder.decode(in, *key);
			distinctKeys->push_back(key);
		}
		predicate.distinctKeys_ = distinctKeys;
	}
	else {
		TXN_THROW_DECODE_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
	}

	return predicate;
}

void MultiGetHandler::encodeEntry(const util::String &containerName,
	ContainerId containerId, const SchemaMap &schemaMap, ResultSet &rs,
	EventByteOutStream &out) {
	out << containerName;

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

	const util::XArray<uint8_t> *rowDataFixedPart =
		rs.getRowDataFixedPartBuffer();
	const util::XArray<uint8_t> *rowDataVarPart = rs.getRowDataVarPartBuffer();

	out.writeAll(rowDataFixedPart->data(), rowDataFixedPart->size());
	out.writeAll(rowDataVarPart->data(), rowDataVarPart->size());
}

/*!
	@brief Handler Operator
*/
void MultiQueryHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	FixedPart request(ev.getPartitionId(), ev.getType());
	OptionPart optionPart(alloc);
	Response response(alloc);

	try {
		EventByteInStream in(ev.getInStream());

		decodeFixedPart(in, request);
		decodeOptionPart(in, ev.getSenderND().getUserData<ConnectionOption>(),
			request, optionPart);

		QueryRequestList queryList(alloc);

		decodeMultiQuery(in, request, queryList);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionStatus partitionStatus = PSTATE_ON;
		checkAuthentication(ev.getSenderND(), emNow);  
		checkTransactionTimeout(emNow, ev.getQueuedMonotonicTime(),
			request.cxtSrc_.txnTimeoutInterval_, ev.getQueueingCount());

		Event reply(ec, ev.getType(), ev.getPartitionId());

		EventByteOutStream out = encodeCommonPart(
			reply, request.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS);
		encodeMultiSearchResultHead(
			out, static_cast<uint32_t>(queryList.size()));

		Progress progress(alloc);

		bool succeeded = true;
		for (size_t i = 0; i < queryList.size(); i++) {
			const QueryRequest &queryRequest = *(queryList[i]);

			if (queryRequest.optionPart_.forUpdate_) {
				GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_NOT_EXECUTABLE,
					"row lock (forUpdate) is not applicable");
			}

			checkConsistency(
				ev.getSenderND(), queryRequest.optionPart_.forUpdate_);
			const PartitionRoleType partitionRole =
				queryRequest.optionPart_.forUpdate_
					? PROLE_OWNER
					: (PROLE_OWNER | PROLE_BACKUP);
			checkExecutable(
				request.pId_, clusterRole, partitionRole, partitionStatus);
			checkFetchOption(queryRequest.fetchOption_);

			execute(ec, request, queryRequest, out, progress);

			if (!progress.lastExceptionData_.empty() ||
				progress.lockConflicted_) {
				succeeded = false;
				break;
			}
		}

		{
			TransactionContext &txn = transactionManager_->put(alloc,
				request.pId_, request.clientId_, request.cxtSrc_, now, emNow);

			const NodeDescriptor &clientND =
				succeeded ? ev.getSenderND() : NodeDescriptor::EMPTY_ND;

			executeReplication(request, ec, alloc, clientND, txn,
				request.stmtType_, request.cxtSrc_.stmtId_,
				TransactionManager::REPLICATION_ASYNC, NULL, 0,
				progress.logRecordList_.data(), progress.logRecordList_.size());
		}

		if (progress.lockConflicted_) {
			ec.getEngine().addPending(
				ev, EventEngine::RESUME_AT_PARTITION_CHANGE);
		}
		else if (succeeded) {
			ec.getEngine().send(reply, ev.getSenderND());
		}
		else {
			Event errorReply(ec, request.stmtType_, request.pId_);
			EventByteOutStream out = encodeCommonPart(
				errorReply, request.cxtSrc_.stmtId_, TXN_STATEMENT_ERROR);
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
	catch (std::exception &e) {
		handleWholeError(ec, alloc, ev, request, e);
	}
}

void MultiQueryHandler::execute(EventContext &ec, const FixedPart &request,
	const QueryRequest &queryRequest, EventByteOutStream &replyOut,
	Progress &progress) {
	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	const ClientId clientId(request.clientId_.uuid_, queryRequest.sessionId_);
	const TransactionManager::ContextSource src =
		createContextSource(request, queryRequest);

	const char8_t *queryType = getQueryTypeName(queryRequest.stmtType_);

	try {
		const util::StackAllocator::Scope scope(alloc);

		TransactionContext &txn = transactionManager_->put(
			alloc, request.pId_, clientId, src, now, emNow);
		const DataStore::Latch latch(
			txn, txn.getPartitionId(), dataStore_, clusterService_);

		const FetchOption &fetchOption = queryRequest.fetchOption_;
		ResultSet *rs = dataStore_->createResultSet(
			txn, txn.getContainerId(), UNDEF_SCHEMAVERSIONID, emNow);
		const ResultSetGuard rsGuard(*dataStore_, *rs);

		switch (queryRequest.stmtType_) {
		case QUERY_TQL: {
			ContainerAutoPtr containerAutoPtr(txn, dataStore_,
				txn.getPartitionId(), txn.getContainerId(), ANY_CONTAINER);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			checkContainerSchemaVersion(
				container, queryRequest.schemaVersionId_);
			rs->setSchemaVersionId(container->getVersionId());
			const util::String *query = queryRequest.query_.tqlQuery_;
			QueryProcessor::executeTQL(
				txn, *container, fetchOption.limit_, query->c_str(), *rs);


			QueryProcessor::fetch(txn, *container, 0, fetchOption.size_, rs);
		} break;
		case QUERY_TIME_SERIES_RANGE: {
			ContainerAutoPtr containerAutoPtr(txn, dataStore_,
				txn.getPartitionId(), txn.getContainerId(),
				TIME_SERIES_CONTAINER);
			TimeSeries *container = containerAutoPtr.getTimeSeries();
			checkContainerSchemaVersion(
				container, queryRequest.schemaVersionId_);
			rs->setSchemaVersionId(container->getVersionId());
			RangeQuery *query = queryRequest.query_.rangeQuery_;
			QueryProcessor::search(txn, *container, query->order_,
				fetchOption.limit_, query->start_, query->end_, *rs);


			QueryProcessor::fetch(txn, *container, 0, fetchOption.size_, rs);
		} break;
		case QUERY_TIME_SERIES_SAMPLING: {
			ContainerAutoPtr containerAutoPtr(txn, dataStore_,
				txn.getPartitionId(), txn.getContainerId(),
				TIME_SERIES_CONTAINER);
			TimeSeries *container = containerAutoPtr.getTimeSeries();
			checkContainerSchemaVersion(
				container, queryRequest.schemaVersionId_);
			rs->setSchemaVersionId(container->getVersionId());
			SamplingQuery *query = queryRequest.query_.samplingQuery_;
			QueryProcessor::sample(txn, *container, fetchOption.limit_,
				query->start_, query->end_, query->toSamplingOption(), *rs);


			QueryProcessor::fetch(txn, *container, 0, fetchOption.size_, rs);
		} break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNSUPPORTED,
				"Unsupported query (statement type=" << queryRequest.stmtType_
													 << ")");
		}


		encodeSearchResult(replyOut, *rs);

		transactionManager_->update(txn, queryRequest.stmtId_);

		progress.containerResult_.push_back(CONTAINER_RESULT_SUCCESS);
		progress.totalRowCount_ += rs->getFetchNum();
	}
	catch (std::exception &e) {
		handleExecuteError(alloc, request.pId_, clientId, src, progress, e,
			EXECUTE_MULTIPLE_QUERIES, queryType);
	}
}

TransactionManager::ContextSource MultiQueryHandler::createContextSource(
	const FixedPart &request, const QueryRequest &queryRequest) {
	return TransactionManager::ContextSource(request.stmtType_,
		queryRequest.stmtId_, queryRequest.containerId_,
		queryRequest.optionPart_.txnTimeoutInterval_, queryRequest.getMode_,
		queryRequest.txnMode_);
}

void MultiQueryHandler::decodeMultiQuery(
	util::ByteStream<util::ArrayInStream> &in, const FixedPart &request,
	util::XArray<const QueryRequest *> &queryList) {
	util::StackAllocator &alloc = *queryList.get_allocator().base();

	try {
		int32_t entryCount;
		in >> entryCount;
		queryList.reserve(static_cast<size_t>(entryCount));

		for (int32_t i = 0; i < entryCount; i++) {
			QueryRequest *subRequest = ALLOC_NEW(alloc) QueryRequest(alloc);

			int32_t stmtType;
			in >> stmtType;
			subRequest->stmtType_ = static_cast<EventType>(stmtType);

			in >> subRequest->stmtId_;
			in >> subRequest->containerId_;
			in >> subRequest->sessionId_;
			in >> subRequest->schemaVersionId_;

			in >> subRequest->getMode_;
			in >> subRequest->txnMode_;

			decodeContainerOptionPart(in, request, subRequest->optionPart_);

			decodeFetchOption(in, subRequest->fetchOption_);

			switch (subRequest->stmtType_) {
			case QUERY_TQL:
				subRequest->query_.tqlQuery_ =
					ALLOC_NEW(alloc) util::String(alloc);
				in >> *subRequest->query_.tqlQuery_;
				break;
			case QUERY_TIME_SERIES_RANGE: {
				RangeQuery *query = ALLOC_NEW(alloc) RangeQuery();

				decodeRangeQuery(in, *query);

				subRequest->query_.rangeQuery_ = query;
			} break;
			case QUERY_TIME_SERIES_SAMPLING: {
				SamplingQuery *query = ALLOC_NEW(alloc) SamplingQuery(alloc);

				decodeSamplingQuery(in, *query);

				subRequest->query_.samplingQuery_ = query;
			} break;
			default:
				TXN_THROW_DECODE_ERROR(GS_ERROR_TXN_DECODE_FAILED, "");
			}

			queryList.push_back(subRequest);
		}
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

void MultiQueryHandler::encodeMultiSearchResultHead(
	EventByteOutStream &out, uint32_t queryCount) {
	try {
		out << queryCount;
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void MultiQueryHandler::encodeSearchResult(
	EventByteOutStream &out, const ResultSet &rs) {
	try {
		if (rs.getResultType() == PARTIAL_RESULT_ROWSET) {
			assert(rs.getId() != UNDEF_RESULTSETID);

			out << static_cast<uint64_t>(
				sizeof(int8_t) + sizeof(ResultSize) + sizeof(ResultSetId) +
				sizeof(ResultSize) + rs.getFixedOffsetSize() +
				rs.getVarOffsetSize());
			out << static_cast<int8_t>(rs.getResultType());
			out << rs.getResultNum();
			out << rs.getId();
			out << rs.getFetchNum();
			out << std::make_pair(rs.getFixedStartData(),
				static_cast<size_t>(rs.getFixedOffsetSize()));
			out << std::make_pair(rs.getVarStartData(),
				static_cast<size_t>(rs.getVarOffsetSize()));
		}
		else {
			out << static_cast<uint64_t>(sizeof(int8_t) + sizeof(ResultSize) +
										 rs.getFixedOffsetSize() +
										 rs.getVarOffsetSize());
			out << static_cast<int8_t>(rs.getResultType());
			out << rs.getResultNum();
			out << std::make_pair(rs.getFixedStartData(),
				static_cast<size_t>(rs.getFixedOffsetSize()));
			out << std::make_pair(rs.getVarStartData(),
				static_cast<size_t>(rs.getVarOffsetSize()));
		}
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

const char8_t *MultiQueryHandler::getQueryTypeName(EventType queryStmtType) {
	switch (queryStmtType) {
	case QUERY_TQL:
		return "tql";
	case QUERY_TIME_SERIES_RANGE:
		return "timeRange";
	case QUERY_TIME_SERIES_SAMPLING:
		return "timeSampling";
	default:
		return "unknown";
	}
}

/*!
	@brief Handler Operator
*/
void ReplicationLogHandler::operator()(EventContext &ec, Event &ev)

{
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	ReplicationAck request(ev.getPartitionId());

	try {
		EventByteInStream in(ev.getInStream());

		decodeReplicationAck(in, request);

		checkReplicationMessageVersion(request.clusterMsgVer_);
		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = (PROLE_OWNER | PROLE_BACKUP);
		const PartitionStatus partitionStatus = (PSTATE_ON | PSTATE_SYNC);
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus);

		uint32_t closedResourceIdCount;
		util::XArray<ClientId> closedResourceIds(alloc);
		decodeIntData<uint32_t>(in, closedResourceIdCount);
		closedResourceIds.assign(
			static_cast<size_t>(closedResourceIdCount), TXN_EMPTY_CLIENTID);
		for (uint32_t i = 0; i < closedResourceIdCount; i++) {
			decodeLongData<SessionId>(in, closedResourceIds[i].sessionId_);
			decodeUUID(
				in, closedResourceIds[i].uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
		}

		uint32_t logRecordCount;
		util::XArray<uint8_t> logRecordList(alloc);
		decodeIntData<uint32_t>(in, logRecordCount);
		decodeBinaryData(in, logRecordList, true);

		GS_TRACE_INFO(REPLICATION, GS_TRACE_TXN_RECEIVE_LOG,
			"(mode=" << request.replMode_ << ", owner=" << ev.getSenderND()
					 << ", replId=" << request.replId_ << ", pId="
					 << request.pId_ << ", eventType=" << request.replStmtType_
					 << ", stmtId=" << request.replStmtId_
					 << ", clientId=" << request.clientId_
					 << ", closedResourceIdCount=" << closedResourceIdCount
					 << ", logRecordCount=" << logRecordCount << ")");

		replyReplicationAck(ec, alloc, ev.getSenderND(), request);

		recoveryManager_->redoLogList(alloc, RecoveryManager::MODE_REPLICATION,
			request.pId_, now, emNow, logRecordList.data(),
			logRecordList.size());

		for (size_t i = 0; i < closedResourceIds.size(); i++) {
			transactionManager_->remove(request.pId_, closedResourceIds[i]);
		}
	}
	catch (DenyException &e) {
		int32_t errorCode = e.getErrorCode();
		bool isTrace = false;
		switch (errorCode) {
		case GS_ERROR_TXN_CLUSTER_ROLE_UNMATCH:
		case GS_ERROR_TXN_PARTITION_ROLE_UNMATCH:
		case GS_ERROR_TXN_PARTITION_STATE_UNMATCH:
			if (clusterManager_->reportError(errorCode)) {
				isTrace = true;
			}
			break;
		default:
			isTrace = true;
			break;
		}
		if (isTrace) {
			UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
				"Replication denied (nd="
					<< ev.getSenderND() << ", pId=" << ev.getPartitionId()
					<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
		}
		else {
			UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
				"Replication denied (nd="
					<< ev.getSenderND() << ", pId=" << ev.getPartitionId()
					<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
		}
	}
	catch (LogRedoException &e) {
		int32_t errorCode = e.getErrorCode();
		bool isTrace = false;
		switch (errorCode) {
		case GS_ERROR_TXN_REPLICATION_LOG_APPLY_FAILED:
			errorCode = e.getErrorCode(1);
			if (errorCode == GS_ERROR_TXN_REPLICATION_LOG_LSN_INVALID) {
				if (clusterManager_->reportError(errorCode)) {
					isTrace = true;
				}
			}
			break;
		default:
			isTrace = true;
			break;
		}
		if (isTrace) {
			UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,

				"Failed to redo log (nd="
					<< ev.getSenderND() << ", pId=" << ev.getPartitionId()
					<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
		}
		else {
			UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
				"Failed to redo log (nd="
					<< ev.getSenderND() << ", pId=" << ev.getPartitionId()
					<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
		}
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			"Failed to accept replication log "
			"(nd="
				<< ev.getSenderND() << ", pId=" << ev.getPartitionId()
				<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Handler Operator
*/
void ReplicationAckHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();

	ReplicationAck ack(ev.getPartitionId());

	try {
		EventByteInStream in(ev.getInStream());

		decodeReplicationAck(in, ack);

		checkReplicationMessageVersion(ack.clusterMsgVer_);
		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_ANY;
		const PartitionStatus partitionStatus = PSTATE_ANY;
		checkExecutable(ack.pId_, clusterRole, partitionRole, partitionStatus);

		GS_TRACE_INFO(REPLICATION, GS_TRACE_TXN_RECEIVE_ACK,
			"(mode=" << ack.replMode_ << ", backup=" << ev.getSenderND()
					 << ", replId=" << ack.replId_ << ", pId=" << ack.pId_
					 << ", eventType=" << ack.replStmtType_
					 << ", stmtId=" << ack.replStmtId_
					 << ", clientId=" << ack.clientId_ << ")");

		TXN_FAILOVER_STATE_SET_ACK(FailoverTestUtil::STATE_5, ack);

		ReplicationContext &replContext =
			transactionManager_->get(ack.pId_, ack.replId_);

		if (replContext.decrementAckCounter()) {
			TXN_FAILOVER_STATE_SET_ACK(FailoverTestUtil::STATE_6, ack);

			replySuccess(ec, alloc, TXN_STATEMENT_SUCCESS, replContext);
			transactionManager_->remove(ack.pId_, ack.replId_);
		}
	}
	catch (ContextNotFoundException &e) {
		UTIL_TRACE_EXCEPTION_INFO(TRANSACTION_SERVICE, e,
			"Replication timed out (nd="
				<< ev.getSenderND() << ", pId=" << ev.getPartitionId()
				<< ", replEventType=" << ack.replStmtType_
				<< ", replStmtId=" << ack.replStmtId_
				<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			"Failed to receive ack (nd="
				<< ev.getSenderND() << ", pId=" << ev.getPartitionId()
				<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void CheckTimeoutHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();

	try {
		{
			const util::StackAllocator::Scope scope(alloc);

			util::XArray<bool> checkPartitionFlagList(alloc);
			if (isTransactionTimeoutCheckEnabled(
					ec.getWorkerId(), checkPartitionFlagList)) {

				checkRequestTimeout(ec, checkPartitionFlagList);
				checkTransactionTimeout(ec, checkPartitionFlagList);
			}
		}

		checkReplicationTimeout(ec);
		checkResultSetTimeout(ec);
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			"resource timeout check failed (nd="
				<< ev.getSenderND() << ", reason=" << GS_EXCEPTION_MESSAGE(e)
				<< ")");

		if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
			clusterService_->setError(ec, &e);
		}
	}
}

void CheckTimeoutHandler::checkReplicationTimeout(EventContext &ec) {
	util::StackAllocator &alloc = ec.getAllocator();
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
				ReplicationContext &replContext =
					transactionManager_->get(pIds[i], timeoutResourceIds[i]);

				replySuccess(ec, alloc, TXN_STATEMENT_SUCCESS_BUT_REPL_TIMEOUT,
					replContext);

				transactionManager_->remove(pIds[i], timeoutResourceIds[i]);
			}
			catch (ContextNotFoundException &e) {
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
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e,
			"(pId=" << pgId << ", timeoutResourceCount=" << timeoutResourceCount
					<< ")");
	}
}

void CheckTimeoutHandler::checkTransactionTimeout(
	EventContext &ec, const util::XArray<bool> &checkPartitionFlagList) {

	util::StackAllocator &alloc = ec.getAllocator();
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
				TransactionContext &txn = transactionManager_->get(
					alloc, pIds[i], timeoutResourceIds[i]);
				const DataStore::Latch latch(
					txn, txn.getPartitionId(), dataStore_, clusterService_);

				if (partitionTable_->isOwner(pIds[i])) {
					util::XArray<uint8_t> *log =
						ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
					if (abortOnError(txn, *log)) {
						util::XArray<const util::XArray<uint8_t> *>
							logRecordList(alloc);
						logRecordList.push_back(log);

						FixedPart dummyRequest(
							UNDEF_PARTITIONID, UNDEF_EVENT_TYPE);

						executeReplication(dummyRequest, ec, alloc,
							NodeDescriptor::EMPTY_ND, txn,
							TXN_COLLECT_TIMEOUT_RESOURCE, UNDEF_STATEMENTID,
							transactionManager_->getReplicationMode(), NULL, 0,
							logRecordList.data(), logRecordList.size());
					}
				}
				else {
					transactionManager_->remove(txn);
				}
			}
			catch (ContextNotFoundException &e) {
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
	catch (DenyException &) {
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "(pgId=" << pgId
											  << ", timeoutResourceCount="
											  << timeoutResourceCount << ")");
	}
}

void CheckTimeoutHandler::checkRequestTimeout(
	EventContext &ec, const util::XArray<bool> &checkPartitionFlagList) {

	util::StackAllocator &alloc = ec.getAllocator();
	const util::DateTime &now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
	const PartitionGroupId pgId = ec.getWorkerId();

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
					TransactionContext &txn = transactionManager_->get(
						alloc, pIds[i], timeoutResourceIds[i]);
					const DataStore::Latch latch(
						txn, txn.getPartitionId(), dataStore_, clusterService_);

					util::XArray<ClientId> closedResourceIds(alloc);
					util::XArray<const util::XArray<uint8_t> *> logRecordList(
						alloc);

					util::XArray<uint8_t> *log =
						ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
					if (abortOnError(txn, *log)) {
						logRecordList.push_back(log);

					}

					const TransactionManager::ContextSource src(
						TXN_COLLECT_TIMEOUT_RESOURCE, txn.getLastStatementId(),
						txn.getContainerId(),
						txn.getTransationTimeoutInterval(),
						TransactionManager::AUTO,
						TransactionManager::AUTO_COMMIT);

					transactionManager_->remove(pIds[i], timeoutResourceIds[i]);
					closedResourceIds.push_back(timeoutResourceIds[i]);

					txn = transactionManager_->put(
						alloc, pIds[i], timeoutResourceIds[i], src, now, emNow);
					FixedPart dummyRequest(UNDEF_PARTITIONID, UNDEF_EVENT_TYPE);

					executeReplication(dummyRequest, ec, alloc,
						NodeDescriptor::EMPTY_ND, txn,
						TXN_COLLECT_TIMEOUT_RESOURCE, UNDEF_STATEMENTID,
						transactionManager_->getReplicationMode(),
						closedResourceIds.data(), closedResourceIds.size(),
						logRecordList.data(), logRecordList.size());
				}
				else {
					transactionManager_->remove(pIds[i], timeoutResourceIds[i]);
				}
			}
			catch (ContextNotFoundException &e) {
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
	catch (DenyException &) {
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "(pgId=" << pgId
											  << ", timeoutResourceCount="
											  << timeoutResourceCount << ")");
	}
}

void CheckTimeoutHandler::checkResultSetTimeout(EventContext &ec) {
	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
	const PartitionGroupId pgId = ec.getWorkerId();

	util::StackAllocator::Scope scope(alloc);

	size_t timeoutResourceCount = 0;

	try {
		dataStore_->checkTimeoutResultSet(pgId, emNow);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e,
			"(pId=" << pgId << ", timeoutResourceCount=" << timeoutResourceCount
					<< ")");
	}
}

bool CheckTimeoutHandler::isTransactionTimeoutCheckEnabled(
	PartitionGroupId pgId, util::XArray<bool> &checkPartitionFlagList) {
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
	pIdCursor_ = UTIL_NEW PartitionId[DataStore::MAX_PARTITION_NUM];
	for (PartitionGroupId pgId = 0; pgId < DataStore::MAX_PARTITION_NUM;
		 pgId++) {
		pIdCursor_[pgId] = UNDEF_PARTITIONID;
	}
}

DataStorePeriodicallyHandler::~DataStorePeriodicallyHandler() {
	delete[] pIdCursor_;
}

/*!
	@brief Handler Operator
*/
void DataStorePeriodicallyHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	try {
		if (clusterManager_->checkRecoveryCompleted()) {
			WATCHER_START;
			const PartitionGroupId pgId = ec.getWorkerId();
			const PartitionGroupConfig &pgConfig =
				transactionManager_->getPartitionGroupConfig();
			if (pIdCursor_[pgId] == UNDEF_PARTITIONID) {
				pIdCursor_[pgId] = pgConfig.getGroupBeginPartitionId(pgId);
			}

			if (dataStore_ &&
				dataStore_->getObjectManager()->existPartition(
					pIdCursor_[pgId])) {
				uint64_t count = 0;
				while (count < PERIODICAL_MAX_SCAN_COUNT) {
					Timestamp timestamp =
						ec.getHandlerStartTime().getUnixTime();
					uint64_t maxScanNum = PERIODICAL_MAX_SCAN_COUNT - count;
					uint64_t scanNum = PERIODICAL_MAX_SCAN_COUNT;
					bool isTail = dataStore_->executeBatchFree(
						pIdCursor_[pgId], timestamp, maxScanNum, scanNum);
					count += scanNum;
					if (isTail) {
						pIdCursor_[pgId]++;
						if (pIdCursor_[pgId] ==
							pgConfig.getGroupEndPartitionId(pgId)) {
							pIdCursor_[pgId] =
								pgConfig.getGroupBeginPartitionId(pgId);
						}
					}
				}  
				dataStore_->getObjectManager()->resetRefCounter(
					pIdCursor_[pgId]);
			}
			else {
				pIdCursor_[pgId]++;
				if (pIdCursor_[pgId] == pgConfig.getGroupEndPartitionId(pgId)) {
					pIdCursor_[pgId] = pgConfig.getGroupBeginPartitionId(pgId);
				}
			}
			WATCHER_END_1(getEventTypeName(CHUNK_EXPIRE_PERIODICALLY));
		}
	}
	catch (UserException &e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			"(nd=" << ev.getSenderND() << ", reason=" << GS_EXCEPTION_MESSAGE(e)
				   << ")");

		if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
			clusterService_->setError(ec, &e);
		}
	}
}

AdjustStoreMemoryPeriodicallyHandler::AdjustStoreMemoryPeriodicallyHandler()
	: StatementHandler(), pIdCursor_(NULL) {
	pIdCursor_ = UTIL_NEW PartitionId[DataStore::MAX_PARTITION_NUM];
	for (PartitionGroupId pgId = 0; pgId < DataStore::MAX_PARTITION_NUM;
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
void AdjustStoreMemoryPeriodicallyHandler::operator()(
	EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	try {
		if (clusterManager_->checkRecoveryCompleted()) {
			WATCHER_START;
			const PartitionGroupId pgId = ec.getWorkerId();
			const PartitionGroupConfig &pgConfig =
				transactionManager_->getPartitionGroupConfig();
			if (pIdCursor_[pgId] == UNDEF_PARTITIONID) {
				pIdCursor_[pgId] = pgConfig.getGroupBeginPartitionId(pgId);
			}

			chunkManager_->redistributeMemoryLimit(
				pIdCursor_[pgId]);  
			if (chunkManager_ &&
				chunkManager_->existPartition(pIdCursor_[pgId])) {
				chunkManager_->reconstructAffinityTable(pIdCursor_[pgId]);
				chunkManager_->adjustStoreMemory(
					pIdCursor_[pgId]);  
			}

			chunkManager_->adjustCheckpointMemoryPool();  

			pIdCursor_[pgId]++;
			if (pIdCursor_[pgId] == pgConfig.getGroupEndPartitionId(pgId)) {
				pIdCursor_[pgId] = pgConfig.getGroupBeginPartitionId(pgId);
			}

			WATCHER_END_1(getEventTypeName(ADJUST_STORE_MEMORY_PERIODICALLY));
		}
	}
	catch (UserException &e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			"(nd=" << ev.getSenderND() << ", reason=" << GS_EXCEPTION_MESSAGE(e)
				   << ")");

		if (GS_EXCEPTION_CHECK_CRITICAL(e)) {
			clusterService_->setError(ec, &e);
		}
	}
}

/*!
	@brief Handler Operator
*/
void UnsupportedStatementHandler::operator()(EventContext &ec, Event &ev) {
	util::StackAllocator &alloc = ec.getAllocator();

	if (ev.getSenderND().isEmpty()) return;

	FixedPart request(ev.getPartitionId(), ev.getType());

	try {
		EventByteInStream in(ev.getInStream());


		decodeLongData<StatementId>(in, request.cxtSrc_.stmtId_);

		GS_THROW_USER_ERROR(GS_ERROR_TXN_STATEMENT_TYPE_UNSUPPORTED,
			"Unsupported statement type");
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Handler Operator
*/
void UnknownStatementHandler::operator()(EventContext &ec, Event &ev) {
	try {
		GS_THROW_USER_ERROR(
			GS_ERROR_TXN_STATEMENT_TYPE_UNKNOWN, "Unknown statement type");
	}
	catch (UserException &e) {
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
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			GS_EXCEPTION_MESSAGE(e) << " (nd=" << ev.getSenderND()
									<< ", pId=" << ev.getPartitionId()
									<< ", eventType=" << ev.getType() << ")");
		clusterService_->setError(ec, &e);
	}
}

/*!
	@brief Handler Operator
*/
void IgnorableStatementHandler::operator()(EventContext &, Event &ev) {
	GS_TRACE_INFO(TRANSACTION_SERVICE, GS_TRACE_TXN_REQUEST_IGNORED,
		"(nd=" << ev.getSenderND() << ", type=" << ev.getType() << ")");
}

TransactionService::TransactionService(const ConfigTable &config,
	const EventEngine::Config &, const EventEngine::Source &eeSource,
	const char *name)
	: eeSource_(eeSource),
	  ee_(NULL),
	  initalized_(false),
	  pgConfig_(config),
	  enableTxnTimeoutCheck_(pgConfig_.getPartitionCount(),
		  static_cast<util::Atomic<bool> >(false)),
	  readOperationCount_(pgConfig_.getPartitionCount(), 0),
	  writeOperationCount_(pgConfig_.getPartitionCount(), 0),
	  rowReadCount_(pgConfig_.getPartitionCount(), 0),
	  rowWriteCount_(pgConfig_.getPartitionCount(), 0),
	  connectHandler_(StatementHandler::TXN_CLIENT_VERSION,
		  ACCEPTABLE_NOSQL_CLIENT_VERSIONS) {
	try {
		eeConfig_.setServerNDAutoNumbering(false);
		eeConfig_.setClientNDEnabled(true);

		eeConfig_.setConcurrency(config.getUInt32(CONFIG_TABLE_DS_CONCURRENCY));
		eeConfig_.setPartitionCount(
			config.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM));

		eeConfig_.setServerAddress(
			config.get<const char8_t *>(CONFIG_TABLE_TXN_SERVICE_ADDRESS),
			config.getUInt16(CONFIG_TABLE_TXN_SERVICE_PORT));

#ifdef GD_ENABLE_UNICAST_NOTIFICATION
		if (ClusterService::isMulticastMode(config)) {
			eeConfig_.setMulticastAddress(
				config.get<const char8_t *>(
					CONFIG_TABLE_TXN_NOTIFICATION_ADDRESS),
				config.getUInt16(CONFIG_TABLE_TXN_NOTIFICATION_PORT));
		}
#else
		eeConfig_.setMulticastAddress(
			config.get<const char8_t *>(CONFIG_TABLE_TXN_NOTIFICATION_ADDRESS),
			config.getUInt16(CONFIG_TABLE_TXN_NOTIFICATION_PORT));
#endif

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
	catch (std::exception &e) {
		delete ee_;

		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

TransactionService::~TransactionService() {
	ee_->shutdown();
	ee_->waitForShutdown();

	TransactionManager *transactionManager =
		connectHandler_.transactionManager_;
	if (transactionManager != NULL) {
		for (PartitionId pId = 0; pId < pgConfig_.getPartitionCount(); pId++) {
			transactionManager->removePartition(pId);
		}

		transactionManager->removeAllReplicationContext();

		for (PartitionGroupId pgId = 0;
			 pgId < pgConfig_.getPartitionGroupCount(); pgId++) {
			assert(transactionManager->getTransactionContextCount(pgId) == 0);
			assert(transactionManager->getReplicationContextCount(pgId) == 0);
		}
	}

	delete ee_;
}

void TransactionService::initialize(const ManagerSet &mgrSet) {
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

		dropContainerHandler_.initialize(mgrSet);
		ee_->setHandler(DROP_CONTAINER, dropContainerHandler_);
		ee_->setHandlingMode(
			DROP_CONTAINER, EventEngine::HANDLING_PARTITION_SERIALIZED);

		getContainerHandler_.initialize(mgrSet);
		ee_->setHandler(GET_COLLECTION, getContainerHandler_);
		ee_->setHandlingMode(
			GET_COLLECTION, EventEngine::HANDLING_PARTITION_SERIALIZED);

		createDropIndexHandler_.initialize(mgrSet);
		ee_->setHandler(CREATE_INDEX, createDropIndexHandler_);
		ee_->setHandlingMode(
			CREATE_INDEX, EventEngine::HANDLING_PARTITION_SERIALIZED);
		ee_->setHandler(DELETE_INDEX, createDropIndexHandler_);
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


		checkTimeoutHandler_.initialize(mgrSet);
		ee_->setHandler(TXN_COLLECT_TIMEOUT_RESOURCE, checkTimeoutHandler_);
		ee_->setHandlingMode(TXN_COLLECT_TIMEOUT_RESOURCE,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		dataStorePeriodicallyHandler_.initialize(mgrSet);
		ee_->setHandler(
			CHUNK_EXPIRE_PERIODICALLY, dataStorePeriodicallyHandler_);
		ee_->setHandlingMode(CHUNK_EXPIRE_PERIODICALLY,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		adjustStoreMemoryPeriodicallyHandler_.initialize(mgrSet);
		ee_->setHandler(ADJUST_STORE_MEMORY_PERIODICALLY,
			adjustStoreMemoryPeriodicallyHandler_);
		ee_->setHandlingMode(ADJUST_STORE_MEMORY_PERIODICALLY,
			EventEngine::HANDLING_PARTITION_SERIALIZED);

		writeLogPeriodicallyHandler_.initialize(mgrSet);
		ee_->setHandler(WRITE_LOG_PERIODICALLY, writeLogPeriodicallyHandler_);
		ee_->setHandlingMode(
			WRITE_LOG_PERIODICALLY, EventEngine::HANDLING_PARTITION_SERIALIZED);

		unsupportedStatementHandler_.initialize(mgrSet);
		for (EventType stmtType = V_1_1_STATEMENT_START;
			 stmtType <= V_1_1_STATEMENT_END; stmtType++) {
			ee_->setHandler(stmtType, unsupportedStatementHandler_);
			ee_->setHandlingMode(
				stmtType, EventEngine::HANDLING_PARTITION_SERIALIZED);
		}
		const EventType V1_5_X_STATEMENTS[] = {GET_TIME_SERIES, PUT_TIME_SERIES,
			DELETE_COLLECTION, CREATE_EVENT_NOTIFICATION,
			DELETE_EVENT_NOTIFICATION, GET_TIME_SERIES_ROW,
			QUERY_TIME_SERIES_TQL, PUT_TIME_SERIES_ROW, PUT_TIME_SERIES_ROWSET,
			DELETE_TIME_SERIES_ROW, GET_TIME_SERIES_MULTIPLE_ROWS,
			UNDEF_EVENT_TYPE};
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
		ee_->setHandler(TXN_LONGTERM_SYNC_REQUEST, longTermSyncHandler_);
		ee_->setHandler(TXN_LONGTERM_SYNC_START, longTermSyncHandler_);
		ee_->setHandler(TXN_LONGTERM_SYNC_START_ACK, longTermSyncHandler_);
		ee_->setHandler(TXN_LONGTERM_SYNC_CHUNK, longTermSyncHandler_);
		ee_->setHandler(TXN_LONGTERM_SYNC_CHUNK_ACK, longTermSyncHandler_);
		ee_->setHandler(TXN_LONGTERM_SYNC_LOG, longTermSyncHandler_);
		ee_->setHandler(TXN_LONGTERM_SYNC_LOG_ACK, longTermSyncHandler_);

		syncTimeoutHandler_.initialize(mgrSet);
		ee_->setHandler(TXN_SYNC_TIMEOUT, syncTimeoutHandler_);
		changePartitionTableHandler_.initialize(mgrSet);
		ee_->setHandler(
			TXN_CHANGE_PARTITION_TABLE, changePartitionTableHandler_);
		changePartitionStateHandler_.initialize(mgrSet);
		ee_->setHandler(
			TXN_CHANGE_PARTITION_STATE, changePartitionStateHandler_);
		dropPartitionHandler_.initialize(mgrSet);
		ee_->setHandler(TXN_DROP_PARTITION, dropPartitionHandler_);

		checkpointOperationHandler_.initialize(mgrSet);
		ee_->setHandler(PARTITION_GROUP_START, checkpointOperationHandler_);
		ee_->setHandler(PARTITION_START, checkpointOperationHandler_);
		ee_->setHandler(COPY_CHUNK, checkpointOperationHandler_);
		ee_->setHandler(PARTITION_END, checkpointOperationHandler_);
		ee_->setHandler(PARTITION_GROUP_END, checkpointOperationHandler_);
		ee_->setHandler(CLEANUP_CP_DATA, checkpointOperationHandler_);


		const PartitionGroupConfig &pgConfig =
			mgrSet.txnMgr_->getPartitionGroupConfig();

		const int32_t writeLogInterval =
			mgrSet.logMgr_->getConfig().logWriteMode_;
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
			if (writeLogInterval > 0 && writeLogInterval < INT32_MAX) {
				Event writeLogPeriodicallyEvent(
					eeSource_, WRITE_LOG_PERIODICALLY, beginPId);
				ee_->addPeriodicTimer(writeLogPeriodicallyEvent,
					changeTimeSecondToMilliSecond(writeLogInterval));
			}
		}


		statUpdator_.service_ = this;
		statUpdator_.manager_ = mgrSet.txnMgr_;
		mgrSet.stats_->addUpdator(&statUpdator_);

		initalized_ = true;
	}
	catch (std::exception &e) {
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
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void TransactionService::shutdown() {
	ee_->shutdown();
}

void TransactionService::waitForShutdown() {
	ee_->waitForShutdown();
}

EventEngine *TransactionService::getEE() {
	return ee_;
}

void TransactionService::changeTimeoutCheckMode(PartitionId pId,
	PartitionTable::PartitionStatus after, ChangePartitionType changeType,
	bool isToSubMaster, ClusterStats &stats) {
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
			"(pId=" << pId << ", state=" << after
					<< ", changeType=" << static_cast<int32_t>(changeType)
					<< ", isToSubMaster=" << isToSubMaster << ")");
	}
}

void TransactionService::enableTransactionTimeoutCheck(PartitionId pId) {
	const bool before = enableTxnTimeoutCheck_[pId];
	enableTxnTimeoutCheck_[pId] = true;

	if (!before) {
		GS_TRACE_WARNING(TRANSACTION_SERVICE, GS_TRACE_TXN_CHECK_TIMEOUT,
			"timeout check enabled. (pId=" << pId << ")");
	}
}

void TransactionService::disableTransactionTimeoutCheck(PartitionId pId) {
	const bool before = enableTxnTimeoutCheck_[pId];
	enableTxnTimeoutCheck_[pId] = false;

	if (before) {
		GS_TRACE_WARNING(TRANSACTION_SERVICE, GS_TRACE_TXN_CHECK_TIMEOUT,
			"timeout check disabled. (pId=" << pId << ")");
	}
}

bool TransactionService::isTransactionTimeoutCheckEnabled(
	PartitionId pId) const {
	return enableTxnTimeoutCheck_[pId];
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

TransactionService::StatSetUpHandler TransactionService::statSetUpHandler_;

#define STAT_ADD_SUB(id) STAT_TABLE_ADD_PARAM_SUB(stat, parentId, id)

void TransactionService::StatSetUpHandler::operator()(StatTable &stat) {
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
}

bool TransactionService::StatUpdator::operator()(StatTable &stat) {
	if (!stat.getDisplayOption(STAT_TABLE_DISPLAY_SELECT_PERF)) {
		return true;
	}

	TransactionService &svc = *service_;
	TransactionManager &mgr = *manager_;

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
	for (PartitionGroupId pgId = 0; pgId < concurrency; pgId++) {
		numTxn += mgr.getActiveTransactionCount(pgId);
		numSession += mgr.getTransactionContextCount(pgId);
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

	return true;
}

TransactionService::StatUpdator::StatUpdator()
	: service_(NULL), manager_(NULL) {}


