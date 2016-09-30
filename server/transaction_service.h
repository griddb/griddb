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
	@brief Definition of TransactionService
*/
#ifndef TRANSACTION_SERVICE_H_
#define TRANSACTION_SERVICE_H_

#include "util/trace.h"
#include "data_type.h"

#include "cluster_event_type.h"
#include "event_engine.h"
#include "transaction_manager.h"
#include "checkpoint_service.h"
#include "data_store.h"
#include "sync_service.h"



#define TXN_PRIVATE private
#define TXN_PROTECTED protected

#define TXN_THROW_DECODE_ERROR(errorCode, message) \
	GS_THROW_CUSTOM_ERROR(EncodeDecodeException, errorCode, message)

#define TXN_THROW_ENCODE_ERROR(errorCode, message) \
	GS_THROW_CUSTOM_ERROR(EncodeDecodeException, errorCode, message)

#define TXN_RETHROW_DECODE_ERROR(cause, message) \
	GS_RETHROW_CUSTOM_ERROR(                     \
		EncodeDecodeException, GS_ERROR_DEFAULT, cause, message)

#define TXN_RETHROW_ENCODE_ERROR(cause, message) \
	GS_RETHROW_CUSTOM_ERROR(                     \
		EncodeDecodeException, GS_ERROR_DEFAULT, cause, message)

const bool TXN_DETAIL_EXCEPTION_HIDDEN = true;

class SystemService;
class ClusterService;
class TriggerService;
class PartitionTable;
class DataStore;
class LogManager;
class ResultSet;
struct ManagerSet;

UTIL_TRACER_DECLARE(TRANSACTION_SERVICE);
UTIL_TRACER_DECLARE(REPLICATION);
UTIL_TRACER_DECLARE(SESSION_TIMEOUT);
UTIL_TRACER_DECLARE(TRANSACTION_TIMEOUT);
UTIL_TRACER_DECLARE(REPLICATION_TIMEOUT);

/*!
	@brief Exception class for denying the statement execution
*/
class DenyException : public util::Exception {
public:
	DenyException(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_DECL) throw()
		: Exception(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_SET) {}
	virtual ~DenyException() throw() {}
};

/*!
	@brief Exception class to notify encoding/decoding failure
*/
class EncodeDecodeException : public util::Exception {
public:
	EncodeDecodeException(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_DECL) throw()
		: Exception(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_SET) {}
	virtual ~EncodeDecodeException() throw() {}
};

/*!
	@brief Handles the statement(event) requested from a client or another node
*/
class StatementHandler : public EventHandler {
	friend struct ScenarioConfig;
	friend class TransactionHandlerTest;

public:
	StatementHandler();

	virtual ~StatementHandler();
	void initialize(const ManagerSet &mgrSet);

	static const size_t USER_NAME_SIZE_MAX = 64;  
	static const size_t DIGEST_SIZE_MAX = 64;  
	static const size_t DATABASE_NAME_SIZE_MAX = 64;  

	static const size_t USER_NUM_MAX = 128;		 
	static const size_t DATABASE_NUM_MAX = 128;  

	/*!
		@brief Client request type
	*/
	enum RequestType {
		NOSQL = 0,
		NEWSQL = 1,
	};
	/*!
		@brief User type
	*/
	enum UserType {
		ADMIN,
		USER,
	};

	typedef int32_t ProtocolVersion;

	static const ProtocolVersion PROTOCOL_VERSION_UNDEFINED;

	static const ProtocolVersion TXN_V1_0_X_CLIENT_VERSION;
	static const ProtocolVersion TXN_V1_1_X_CLIENT_VERSION;
	static const ProtocolVersion TXN_V1_5_X_CLIENT_VERSION;
	static const ProtocolVersion TXN_V2_0_X_CLIENT_VERSION;
	static const ProtocolVersion TXN_V2_1_X_CLIENT_VERSION;
	static const ProtocolVersion TXN_V2_5_X_CLIENT_VERSION;

	static const ProtocolVersion TXN_V2_7_X_CLIENT_VERSION;

	static const ProtocolVersion TXN_V2_8_X_CLIENT_VERSION;
	static const ProtocolVersion TXN_V2_9_X_CLIENT_VERSION;
	static const ProtocolVersion TXN_V3_0_X_CLIENT_VERSION;
	static const ProtocolVersion TXN_V3_0_X_CE_CLIENT_VERSION;

	static const ProtocolVersion TXN_CLIENT_VERSION;

	static const ProtocolVersion TXN_V2_5_X_REPLICATION_MSG_VERSION = 3;

	static const ProtocolVersion REPLICATION_MSG_VERSION =
		TXN_V2_5_X_REPLICATION_MSG_VERSION;

	static const ProtocolVersion MIN_ACCEPTABLE_REPLICATION_MSG_VERSION =
		TXN_V2_5_X_REPLICATION_MSG_VERSION;
	static const ProtocolVersion MAX_ACCEPTABLE_REPLICATION_MSG_VERSION =
		REPLICATION_MSG_VERSION;



	typedef uint8_t StatementExecStatus;  
	static const StatementExecStatus TXN_STATEMENT_SUCCESS;  
	static const StatementExecStatus
		TXN_STATEMENT_ERROR;  
	static const StatementExecStatus
		TXN_STATEMENT_NODE_ERROR;  
	static const StatementExecStatus
		TXN_STATEMENT_DENY;  
	static const StatementExecStatus
		TXN_STATEMENT_SUCCESS_BUT_REPL_TIMEOUT;  
	struct OptionPart;

	/*!
		@brief Represents fixed part of message
	*/
	struct FixedPart {
		FixedPart(PartitionId pId, EventType stmtType)
			: pId_(pId),
			  stmtType_(stmtType),
			  clientId_(TXN_EMPTY_CLIENTID),
			  cxtSrc_(stmtType, isUpdateStatement(stmtType)),
			  schemaVersionId_(UNDEF_SCHEMAVERSIONID),
			  startStmtId_(UNDEF_STATEMENTID),
			  optPart_(NULL) {}

		const PartitionId pId_;
		const EventType stmtType_;
		ClientId clientId_;
		TransactionManager::ContextSource cxtSrc_;
		SchemaVersionId schemaVersionId_;
		StatementId startStmtId_;
		OptionPart *optPart_;
	};

	typedef int16_t OptionType;
	static const OptionType OPTION_TXN_TIMEOUT_INTERVAL =
		1;  
	static const OptionType OPTION_FOR_UPDATE =
		2;  
	static const OptionType OPTION_CONTAINER_LOCK_CONTINUE =
		3;  
	static const OptionType OPTION_SYSTEM_MODE = 4;
	static const OptionType OPTION_DB_NAME = 5;
	static const OptionType OPTION_CONTAINER_ATTRIBUTE = 6;
	static const OptionType OPTION_PUT_ROW_OPTION = 7;
	static const OptionType OPTION_REQUEST_MODULE_TYPE = 8;

	static const OptionType OPTION_REPLY_PID = 9;
	static const OptionType OPTION_REPLY_SERVICE_TYPE = 10;  
	static const OptionType OPTION_REPLY_EVENT_TYPE = 11;
	static const OptionType OPTION_UUID = 12;
	static const OptionType OPTION_QUERYID = 13;
	static const OptionType OPTION_CONTAINERID = 14;
	static const OptionType OPTION_QUERY_VERSIONID = 15;
	static const OptionType OPTION_USER_TYPE = 16;
	static const OptionType OPTION_DB_VERSIONID = 17;

	static const OptionType OPTION_TYPE_NOSQL_MAX =
		10000;  

	static const OptionType OPTION_SQL_STATEMENT_TIMEOUT_INTERVAL =
		10001;  
	static const OptionType OPTION_SQL_MAX_ROWS = 10002;  
	static const OptionType OPTION_SQL_FETCH_SIZE = 10003;  
	static const OptionType OPTION_TYPE_NEWSQL_MAX =
		INT16_MAX;  

	static const OptionType OPTION_TYPE_MAX =
		OPTION_SQL_FETCH_SIZE;  

	/*!
		@brief Represents option part of message
	*/
	struct OptionPart {
		explicit OptionPart(util::StackAllocator &alloc)
			: txnTimeoutInterval_(TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL),
			  forUpdate_(false),
			  systemMode_(false),
			  alloc_(alloc),
			  dbName_(alloc),
			  containerAttribute_(CONTAINER_ATTR_BASE),
			  requestType_(NOSQL),
			  putRowOption_(PUT_INSERT_OR_UPDATE){};

		int32_t txnTimeoutInterval_;
		bool forUpdate_;
		bool systemMode_;
		util::StackAllocator &alloc_;
		util::String dbName_;
		ContainerAttribute containerAttribute_;
		RequestType requestType_;  
		PutRowOption putRowOption_;

		FixedPart *fixedPart_;
	};

	typedef util::XArray<uint8_t> RowKeyData;  
	typedef util::XArray<uint8_t> RowData;	 

	/*!
		@brief Represents fetch setting
	*/
	struct FetchOption {
		FetchOption() : limit_(0), size_(0) {}

		ResultSize limit_;
		ResultSize size_;
	};


	/*!
		@brief Represents time-related condition
	*/
	struct TimeRelatedCondition {
		TimeRelatedCondition()
			: rowKey_(UNDEF_TIMESTAMP), operator_(TIME_PREV) {}

		Timestamp rowKey_;
		TimeOperator operator_;
	};

	/*!
		@brief Represents interpolation condition
	*/
	struct InterpolateCondition {
		InterpolateCondition()
			: rowKey_(UNDEF_TIMESTAMP), columnId_(UNDEF_COLUMNID) {}

		Timestamp rowKey_;
		ColumnId columnId_;
	};

	/*!
		@brief Represents aggregate condition
	*/
	struct AggregateQuery {
		AggregateQuery()
			: start_(UNDEF_TIMESTAMP),
			  end_(UNDEF_TIMESTAMP),
			  columnId_(UNDEF_COLUMNID),
			  aggregationType_(AGG_MIN) {}

		Timestamp start_;
		Timestamp end_;
		ColumnId columnId_;
		AggregationType aggregationType_;
	};

	/*!
		@brief Represents time range condition
	*/
	struct RangeQuery {
		RangeQuery()
			: start_(UNDEF_TIMESTAMP),
			  end_(UNDEF_TIMESTAMP),
			  order_(ORDER_ASCENDING) {}

		Timestamp start_;
		Timestamp end_;
		OutputOrder order_;
	};

	/*!
		@brief Represents sampling condition
	*/
	struct SamplingQuery {
		explicit SamplingQuery(util::StackAllocator &alloc)
			: start_(UNDEF_TIMESTAMP),
			  end_(UNDEF_TIMESTAMP),
			  timeUnit_(TIME_UNIT_YEAR),
			  interpolatedColumnIdList_(alloc),
			  mode_(INTERP_MODE_LINEAR_OR_PREVIOUS) {}

		Sampling toSamplingOption() const;

		Timestamp start_;
		Timestamp end_;
		uint32_t interval_;
		TimeUnit timeUnit_;
		util::XArray<uint32_t> interpolatedColumnIdList_;
		InterpolationMode mode_;
	};




	/*!
		@brief Represents response to a client
	*/
	struct Response {
		explicit Response(util::StackAllocator &alloc)
			: binaryData_(alloc),
			  containerNum_(0),
			  containerNameList_(alloc),
			  existFlag_(false),
			  schemaVersionId_(UNDEF_SCHEMAVERSIONID),
			  containerId_(UNDEF_CONTAINERID),
			  stringData_(alloc),
			  rs_(NULL),
			  last_(0),
			  containerAttribute_(CONTAINER_ATTR_BASE),
			  putRowOption_(0)
		{
		}


		util::XArray<uint8_t> binaryData_;

		uint64_t containerNum_;
		util::XArray<util::String *> containerNameList_;


		bool existFlag_;
		SchemaVersionId schemaVersionId_;
		ContainerId containerId_;
		util::String stringData_;


		ResultSet *rs_;




		RowId last_;



		ContainerAttribute containerAttribute_;

		uint8_t putRowOption_;
	};

	/*!
		@brief Represents the information about ReplicationAck
	*/
	struct ReplicationAck {
		explicit ReplicationAck(PartitionId pId) : pId_(pId) {}

		const PartitionId pId_;
		ProtocolVersion clusterMsgVer_;
		ReplicationId replId_;
		int32_t replMode_;
		int32_t replStmtType_;
		StatementId replStmtId_;
		ClientId clientId_;
	};


	void setSuccessReply(Event &ev, StatementId stmtId,
		StatementExecStatus status, const Response &response);
	static void setErrorReply(Event &ev, StatementId stmtId,
		StatementExecStatus status, const std::exception &exception,
		const NodeDescriptor &nd);

	typedef uint32_t
		ClusterRole;  
	static const ClusterRole CROLE_UNKNOWN;	
	static const ClusterRole CROLE_SUBMASTER;  
	static const ClusterRole CROLE_MASTER;	 
	static const ClusterRole CROLE_FOLLOWER;   
	static const ClusterRole CROLE_ANY;		   
	static const char8_t *const clusterRoleStr[8];

	typedef uint32_t PartitionRoleType;			   
	static const PartitionRoleType PROLE_UNKNOWN;  
	static const PartitionRoleType
		PROLE_NONE;  
	static const PartitionRoleType
		PROLE_OWNER;  
	static const PartitionRoleType
		PROLE_BACKUP;  
	static const PartitionRoleType
		PROLE_CATCHUP;  
	static const PartitionRoleType PROLE_ANY;  
	static const char8_t *const partitionRoleTypeStr[16];

	typedef uint32_t PartitionStatus;  
	static const PartitionStatus PSTATE_UNKNOWN;  
	static const PartitionStatus
		PSTATE_ON;  
	static const PartitionStatus
		PSTATE_SYNC;  
	static const PartitionStatus
		PSTATE_OFF;  
	static const PartitionStatus
		PSTATE_STOP;  
	static const PartitionStatus PSTATE_ANY;  
	static const char8_t *const partitionStatusStr[16];

	static const bool IMMEDIATE_CONSISTENCY =
		true;  
	static const bool ANY_CONSISTENCY = false;  

	static const bool NO_REPLICATION =
		false;  

	ClusterService *clusterService_;
	ClusterManager *clusterManager_;
	ChunkManager *chunkManager_;
	DataStore *dataStore_;
	LogManager *logManager_;
	PartitionTable *partitionTable_;
	TransactionService *transactionService_;
	TransactionManager *transactionManager_;
	TriggerService *triggerService_;
	SystemService *systemService_;
	RecoveryManager *recoveryManager_;

	/*!
		@brief Represents the information about a connection
	*/
	struct ConnectionOption {
		ConnectionOption()
			: clientVersion_(PROTOCOL_VERSION_UNDEFINED),
			  txnTimeoutInterval_(TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL),
			  isAuthenticated_(false),
			  isImmediateConsistency_(false),
			  dbId_(0),
			  isAdminAndPublicDB_(true),
			  userType_(USER),
			  authenticationTime_(0),
			  requestType_(NOSQL)
		{
		}

		void clear() {
			clientVersion_ = PROTOCOL_VERSION_UNDEFINED;
			txnTimeoutInterval_ = TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL;
			isAuthenticated_ = false;
			isImmediateConsistency_ = false;
			dbId_ = 0;
			isAdminAndPublicDB_ = true;
			userType_ = USER;
			authenticationTime_ = 0;
			requestType_ = NOSQL;

			userName_.clear();
			dbName_.clear();
		}

		ProtocolVersion clientVersion_;
		int32_t txnTimeoutInterval_;
		bool isAuthenticated_;
		bool isImmediateConsistency_;

		DatabaseId dbId_;
		bool isAdminAndPublicDB_;
		UserType userType_;  
		EventMonotonicTime authenticationTime_;
		RequestType requestType_;  

		std::string userName_;
		std::string dbName_;
	};


	void checkAuthentication(
		const NodeDescriptor &ND, EventMonotonicTime emNow);
	void checkConsistency(const NodeDescriptor &ND, bool requireImmediate);
	void checkExecutable(PartitionId pId, ClusterRole requiredClusterRole,
		PartitionRoleType requiredPartitionRole,
		PartitionStatus requiredPartitionStatus);
	void checkTransactionTimeout(EventMonotonicTime now,
		EventMonotonicTime queuedTime, int32_t txnTimeoutIntervalSec,
		uint32_t queueingCount);
	void checkContainerExistence(BaseContainer *container);
	void checkContainerSchemaVersion(
		BaseContainer *container, SchemaVersionId schemaVersionId);
	void checkReplicationMessageVersion(ProtocolVersion replMsgVersion);
	void checkFetchOption(FetchOption fetchOption);
	void checkSizeLimit(ResultSize limit);


	void decodeFixedPart(
		util::ByteStream<util::ArrayInStream> &in, FixedPart &fixedPart);
	void decodeOptionPart(
		util::ByteStream<util::ArrayInStream> &in, OptionPart &optionPart);
	void decodeOptionPart(util::ByteStream<util::ArrayInStream> &in,
		ConnectionOption &connOption, FixedPart &fixedPart,
		OptionPart &optionPart);

	void decodeIndexInfo(
		util::ByteStream<util::ArrayInStream> &in, IndexInfo &indexInfo);
	void decodeTriggerInfo(
		util::ByteStream<util::ArrayInStream> &in, TriggerInfo &triggerInfo);
	void decodeMultipleRowData(util::ByteStream<util::ArrayInStream> &in,
		uint64_t &numRow, RowData &rowData);
	void decodeFetchOption(
		util::ByteStream<util::ArrayInStream> &in, FetchOption &fetchOption);

	void decodeTimeRelatedConditon(util::ByteStream<util::ArrayInStream> &in,
		TimeRelatedCondition &condition);
	void decodeInterpolateConditon(util::ByteStream<util::ArrayInStream> &in,
		InterpolateCondition &condition);
	void decodeAggregateQuery(
		util::ByteStream<util::ArrayInStream> &in, AggregateQuery &query);
	void decodeRangeQuery(
		util::ByteStream<util::ArrayInStream> &in, RangeQuery &query);
	void decodeSamplingQuery(
		util::ByteStream<util::ArrayInStream> &in, SamplingQuery &query);
	void decodeContainerConditionData(util::ByteStream<util::ArrayInStream> &in,
		DataStore::ContainerCondition &containerCondition);

	template <typename IntType>
	void decodeIntData(
		util::ByteStream<util::ArrayInStream> &in, IntType &intData);
	template <typename LongType>
	void decodeLongData(
		util::ByteStream<util::ArrayInStream> &in, LongType &longData);
	template <typename StringType>
	void decodeStringData(
		util::ByteStream<util::ArrayInStream> &in, StringType &strData);
	static void decodeBooleanData(
		util::ByteStream<util::ArrayInStream> &in, bool &boolData);
	static void decodeBinaryData(util::ByteStream<util::ArrayInStream> &in,
		util::XArray<uint8_t> &binaryData, bool readAll);
	void decodeVarSizeBinaryData(util::ByteStream<util::ArrayInStream> &in,
		util::XArray<uint8_t> &binaryData);
	template <typename EnumType>
	void decodeEnumData(
		util::ByteStream<util::ArrayInStream> &in, EnumType &enumData);
	void decodeUUID(util::ByteStream<util::ArrayInStream> &in, uint8_t *uuid,
		size_t uuidSize);

	void decodeReplicationAck(
		util::ByteStream<util::ArrayInStream> &in, ReplicationAck &ack);

	static EventByteOutStream encodeCommonPart(
		Event &ev, StatementId stmtId, StatementExecStatus status);

	template <typename IntType>
	static void encodeIntData(EventByteOutStream &out, IntType intData);
	template <typename LongType>
	static void encodeLongData(EventByteOutStream &out, LongType longData);
	template <typename StringType>
	static void encodeStringData(
		EventByteOutStream &out, const StringType &strData);
	static void encodeConstStringData(
		EventByteOutStream &out, const char *strData);

	static void encodeBooleanData(EventByteOutStream &out, bool boolData);
	static void encodeBinaryData(
		EventByteOutStream &out, const uint8_t *data, size_t size);
	template <typename EnumType>
	static void encodeEnumData(EventByteOutStream &out, EnumType enumData);
	static void encodeUUID(
		EventByteOutStream &out, const uint8_t *uuid, size_t uuidSize);

	template <typename ByteOutStream>
	static void encodeException(ByteOutStream &out,
		const std::exception &exception, bool detailsHidden);
	template <typename ByteOutStream>
	static void encodeException(ByteOutStream &out,
		const std::exception &exception, const NodeDescriptor &nd);

	static void encodeReplicationAckPart(EventByteOutStream &out,
		uint32_t replMsgVer, int32_t replMode, const ClientId &clientId,
		ReplicationId replId, EventType replStmtType, StatementId replStmtId);


	static bool isUpdateStatement(EventType stmtType);

	void replySuccess(EventContext &ec, util::StackAllocator &alloc,
		const NodeDescriptor &ND, EventType stmtType,
		StatementExecStatus status, const FixedPart &request,
		const Response &response, bool ackWait);
	void replySuccess(EventContext &ec, util::StackAllocator &alloc,
		StatementExecStatus status, const ReplicationContext &replContext);
	void replyError(EventContext &ec, util::StackAllocator &alloc,
		const NodeDescriptor &ND, EventType stmtType,
		StatementExecStatus status, const FixedPart &request,
		const std::exception &e);

	bool executeReplication(const FixedPart &request, EventContext &ec,
		util::StackAllocator &alloc, const NodeDescriptor &clientND,
		TransactionContext &txn, EventType replStmtType, StatementId replStmtId,
		int32_t replMode, const ClientId *closedResourceIds,
		size_t closedResourceIdCount,
		const util::XArray<uint8_t> **logRecordList, size_t logRecordCount,
		bool rowExistFlag = false);
	void replyReplicationAck(EventContext &ec, util::StackAllocator &alloc,
		const NodeDescriptor &ND, const ReplicationAck &request);

	void handleError(EventContext &ec, util::StackAllocator &alloc, Event &ev,
		const FixedPart &request, std::exception &e);

	bool abortOnError(TransactionContext &txn, util::XArray<uint8_t> &log);

	ContainerAccessMode getContainerAccessMode(UserType userType,
		bool isSystemMode, RequestType requestType, bool isWriteMode);
	bool isAccessibleMode(
		ContainerAccessMode containerAccessMode, ContainerAttribute attribute);

	void checkDbAccessible(const std::string &loginDbName,
		const util::String &specifiedDbName) const;

	static const char8_t *clusterRoleToStr(ClusterRole role);
	static const char8_t *partitionRoleTypeToStr(PartitionRoleType role);
	static const char8_t *partitionStatusToStr(PartitionStatus status);
};

template <typename IntType>
void StatementHandler::decodeIntData(
	util::ByteStream<util::ArrayInStream> &in, IntType &intData) {
	try {
		in >> intData;
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

template <typename LongType>
void StatementHandler::decodeLongData(
	util::ByteStream<util::ArrayInStream> &in, LongType &longData) {
	try {
		in >> longData;
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

template <typename StringType>
void StatementHandler::decodeStringData(
	util::ByteStream<util::ArrayInStream> &in, StringType &strData) {
	try {
		in >> strData;
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

template <typename EnumType>
void StatementHandler::decodeEnumData(
	util::ByteStream<util::ArrayInStream> &in, EnumType &enumData) {
	try {
		int8_t tmp;
		in >> tmp;
		enumData = static_cast<EnumType>(tmp);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

template <typename IntType>
void StatementHandler::encodeIntData(EventByteOutStream &out, IntType intData) {
	try {
		out << intData;
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

template <typename LongType>
void StatementHandler::encodeLongData(
	EventByteOutStream &out, LongType longData) {
	try {
		out << longData;
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

template <typename StringType>
void StatementHandler::encodeStringData(
	EventByteOutStream &out, const StringType &strData) {
	try {
		const uint32_t size = static_cast<uint32_t>(strData.size());
		out << size;
		out << std::pair<const uint8_t *, size_t>(
			reinterpret_cast<const uint8_t *>(strData.c_str()), size);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

template <typename EnumType>
void StatementHandler::encodeEnumData(
	EventByteOutStream &out, EnumType enumData) {
	try {
		const uint8_t tmp = static_cast<uint8_t>(enumData);
		out << tmp;
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief Handles CONNECT statement
*/
class ConnectHandler : public StatementHandler {
public:
	ConnectHandler(ProtocolVersion currentVersion,
		const ProtocolVersion *acceptableProtocolVersons);

	void operator()(EventContext &ec, Event &ev);

private:
	typedef uint32_t OldStatementId;  
	static const OldStatementId UNDEF_OLD_STATEMENTID = UINT32_MAX;

	const ProtocolVersion currentVersion_;
	const ProtocolVersion *acceptableProtocolVersons_;

	struct ConnectRequest {
		ConnectRequest(PartitionId pId, EventType stmtType)
			: pId_(pId),
			  stmtType_(stmtType),
			  oldStmtId_(UNDEF_OLD_STATEMENTID),
			  clientVersion_(PROTOCOL_VERSION_UNDEFINED) {}

		const PartitionId pId_;
		const EventType stmtType_;
		OldStatementId oldStmtId_;
		ProtocolVersion clientVersion_;
	};

	void checkClientVersion(ProtocolVersion clientVersion);

	EventByteOutStream encodeCommonPart(
		Event &ev, OldStatementId stmtId, StatementExecStatus status);

	void replySuccess(EventContext &ec, util::StackAllocator &alloc,
		const NodeDescriptor &ND, EventType stmtType,
		StatementExecStatus status, const ConnectRequest &request,
		const Response &response, bool ackWait);

	void replyError(EventContext &ec, util::StackAllocator &alloc,
		const NodeDescriptor &ND, EventType stmtType,
		StatementExecStatus status, const ConnectRequest &request,
		const std::exception &e);

	void handleError(EventContext &ec, util::StackAllocator &alloc, Event &ev,
		const ConnectRequest &request, std::exception &e);
};

/*!
	@brief Handles DISCONNECT statement
*/
class DisconnectHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles LOGIN statement
*/
class LoginHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles LOGOUT statement
*/
class LogoutHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles GET_PARTITION_ADDRESS statement
*/
class GetPartitionAddressHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles GET_PARTITION_CONTAINER_NAMES statement
*/
class GetPartitionContainerNamesHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles GET_CONTAINER_PROPERTIES statement
*/
class GetContainerPropertiesHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

private:
	typedef util::XArray<util::String *> ContainerNameList;

	enum ContainerProperty {
		CONTAINER_PROPERTY_ID,
		CONTAINER_PROPERTY_SCHEMA,
		CONTAINER_PROPERTY_INDEX,
		CONTAINER_PROPERTY_EVENT_NOTIFICATION,
		CONTAINER_PROPERTY_TRIGGER,
		CONTAINER_PROPERTY_ATTRIBUTES
	};

	void encodeResultListHead(EventByteOutStream &out, uint32_t totalCount);
	void encodePropsHead(EventByteOutStream &out, uint32_t propTypeCount);
	void encodeId(EventByteOutStream &out, SchemaVersionId schemaVersionId,
		ContainerId containerId, const uint8_t *containerName,
		uint32_t containerNameSize);
	void encodeSchema(EventByteOutStream &out, ContainerType containerType,
		const util::XArray<uint8_t> &serializedCollectionInfo);
	void encodeIndex(
		EventByteOutStream &out, const util::XArray<IndexInfo> &indexInfoList);
	void encodeEventNotification(EventByteOutStream &out,
		const util::XArray<char *> &urlList,
		const util::XArray<uint32_t> &urlLenList);
	void encodeTrigger(EventByteOutStream &out,
		const util::XArray<const uint8_t *> &triggerList);
	void encodeAttributes(
		EventByteOutStream &out, const ContainerAttribute containerAttribute);
};

/*!
	@brief Handles PUT_CONTAINER statement
*/
class PutContainerHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles DROP_CONTAINER statement
*/
class DropContainerHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles GET_CONTAINER statement
*/
class GetContainerHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles CREATE_DROP_INDEX statement
*/
class CreateDropIndexHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles DROP_TRIGGER statement
*/
class CreateDropTriggerHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles FLUSH_LOG statement
*/
class FlushLogHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles WRITE_LOG_PERIODICALLY event
*/
class WriteLogPeriodicallyHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles CREATE_TRANSACTIN_CONTEXT statement
*/
class CreateTransactionContextHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles CLOSE_TRANSACTIN_CONTEXT statement
*/
class CloseTransactionContextHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles COMMIT_TRANSACTIN statement
*/
class CommitAbortTransactionHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles PUT_ROW statement
*/
class PutRowHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles PUT_ROW_SET statement
*/
class PutRowSetHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles REMOVE_ROW statement
*/
class RemoveRowHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles UPDATE_ROW_BY_ID statement
*/
class UpdateRowByIdHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles REMOVE_ROW_BY_ID statement
*/
class RemoveRowByIdHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles GET_ROW statement
*/
class GetRowHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles GET_ROW_SET statement
*/
class GetRowSetHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles QUERY_TQL statement
*/
class QueryTqlHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles APPEND_ROW statement
*/
class AppendRowHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles GET_ROW_TIME_RELATED statement
*/
class GetRowTimeRelatedHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles ROW_INTERPOLATE statement
*/
class GetRowInterpolateHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles AGGREGATE statement
*/
class AggregateHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles QUERY_TIME_RANGE statement
*/
class QueryTimeRangeHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles QUERY_TIME_SAMPING statement
*/
class QueryTimeSamplingHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles FETCH_RESULT_SET statement
*/
class FetchResultSetHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles CLOSE_RESULT_SET statement
*/
class CloseResultSetHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles MULTI_CREATE_TRANSACTION_CONTEXT statement
*/
class MultiCreateTransactionContextHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

private:
	struct SessionCreationEntry {
		SessionCreationEntry()
			: containerName_(NULL),
			  containerId_(UNDEF_CONTAINERID),
			  sessionId_(UNDEF_SESSIONID) {}

		util::String *containerName_;
		ContainerId containerId_;
		SessionId sessionId_;
	};

	bool decodeMultiTransactionCreationEntry(
		util::ByteStream<util::ArrayInStream> &in,
		util::XArray<SessionCreationEntry> &entryList);
};
/*!
	@brief Handles MULTI_CLOSE_TRANSACTION_CONTEXT statement
*/
class MultiCloseTransactionContextHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

private:
	struct SessionCloseEntry {
		SessionCloseEntry()
			: stmtId_(UNDEF_STATEMENTID),
			  containerId_(UNDEF_CONTAINERID),
			  sessionId_(UNDEF_SESSIONID) {}

		StatementId stmtId_;
		ContainerId containerId_;
		SessionId sessionId_;
	};

	void decodeMultiTransactionCloseEntry(
		util::ByteStream<util::ArrayInStream> &in,
		util::XArray<SessionCloseEntry> &entryList);
};

/*!
	@brief Handles multi-type statement
*/
class MultiStatementHandler : public StatementHandler {
	TXN_PROTECTED : enum ContainerResult {
		CONTAINER_RESULT_SUCCESS,
		CONTAINER_RESULT_ALREADY_EXECUTED,
		CONTAINER_RESULT_FAIL
	};

	struct Progress {
		util::XArray<ContainerResult> containerResult_;
		util::XArray<uint8_t> lastExceptionData_;
		util::XArray<const util::XArray<uint8_t> *> logRecordList_;
		bool lockConflicted_;
		StatementId mputStartStmtId_;
		uint64_t totalRowCount_;

		explicit Progress(util::StackAllocator &alloc)
			: containerResult_(alloc),
			  lastExceptionData_(alloc),
			  logRecordList_(alloc),
			  lockConflicted_(false),
			  mputStartStmtId_(UNDEF_STATEMENTID),
			  totalRowCount_(0) {}
	};

	void handleExecuteError(util::StackAllocator &alloc, PartitionId pId,
		const ClientId &clientId, const TransactionManager::ContextSource &src,
		Progress &progress, std::exception &e, EventType stmtType,
		const char8_t *executionName);

	void handleWholeError(EventContext &ec, util::StackAllocator &alloc,
		const Event &ev, const FixedPart &request, std::exception &e);

	void decodeContainerOptionPart(util::ByteStream<util::ArrayInStream> &in,
		const FixedPart &fixedPart, OptionPart &optionPart);
};

/*!
	@brief Handles MULTI_PUT statement
*/
class MultiPutHandler : public MultiStatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

private:
	struct RowSetRequest {
		StatementId stmtId_;
		ContainerId containerId_;
		SessionId sessionId_;
		TransactionManager::GetMode getMode_;
		TransactionManager::TransactionMode txnMode_;

		OptionPart option_;

		int32_t schemaIndex_;
		uint64_t rowCount_;
		util::XArray<uint8_t> rowSetData_;

		explicit RowSetRequest(util::StackAllocator &alloc)
			: stmtId_(UNDEF_STATEMENTID),
			  containerId_(UNDEF_CONTAINERID),
			  sessionId_(UNDEF_SESSIONID),
			  getMode_(TransactionManager::AUTO),
			  txnMode_(TransactionManager::AUTO_COMMIT),
			  option_(alloc),
			  schemaIndex_(-1),
			  rowCount_(0),
			  rowSetData_(alloc) {}
	};

	typedef std::pair<int32_t, ColumnSchemaId> CheckedSchemaId;
	typedef util::SortedList<CheckedSchemaId> CheckedSchemaIdSet;

	void execute(EventContext &ec, const FixedPart &request,
		const RowSetRequest &rowSetRequest, const MessageSchema &schema,
		CheckedSchemaIdSet &idSet, Progress &progress,
		PutRowOption putRowOption);

	void checkSchema(TransactionContext &txn, BaseContainer &container,
		const MessageSchema &schema, int32_t localSchemaId,
		CheckedSchemaIdSet &idSet);

	TransactionManager::ContextSource createContextSource(
		const FixedPart &request, const RowSetRequest &rowSetRequest);

	void decodeMultiSchema(util::ByteStream<util::ArrayInStream> &in,
		util::XArray<const MessageSchema *> &schemaList);
	void decodeMultiRowSet(util::ByteStream<util::ArrayInStream> &in,
		const FixedPart &request,
		const util::XArray<const MessageSchema *> &schemaList,
		util::XArray<const RowSetRequest *> &rowSetList);
};
/*!
	@brief Handles MULTI_GET statement
*/
class MultiGetHandler : public MultiStatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

private:
	typedef util::XArray<RowKeyData *> RowKeyDataList;
	typedef int32_t LocalSchemaId;
	typedef util::Map<ContainerId, LocalSchemaId> SchemaMap;

	enum RowKeyPredicateType { PREDICATE_TYPE_RANGE, PREDICATE_TYPE_DISTINCT };

	struct RowKeyPredicate {
		ColumnType keyType_;
		RowKeyData *startKey_;
		RowKeyData *finishKey_;
		RowKeyDataList *distinctKeys_;
	};

	struct SearchEntry {
		SearchEntry(ContainerId containerId, const util::String *containerName,
			const RowKeyPredicate *predicate)
			: stmtId_(0),
			  containerId_(containerId),
			  sessionId_(TXN_EMPTY_CLIENTID.sessionId_),
			  getMode_(TransactionManager::AUTO),
			  txnMode_(TransactionManager::AUTO_COMMIT),
			  containerName_(containerName),
			  predicate_(predicate) {}

		StatementId stmtId_;
		ContainerId containerId_;
		SessionId sessionId_;
		TransactionManager::GetMode getMode_;
		TransactionManager::TransactionMode txnMode_;

		const util::String *containerName_;
		const RowKeyPredicate *predicate_;
	};

	uint32_t execute(EventContext &ec, const FixedPart &request,
		const SearchEntry &entry, const SchemaMap &schemaMap,
		EventByteOutStream &replyOut, Progress &progress);

	void buildSchemaMap(PartitionId pId,
		const util::XArray<SearchEntry> &searchList, SchemaMap &schemaMap,
		EventByteOutStream &out);

	void checkContainerRowKey(
		BaseContainer *container, const RowKeyPredicate &predicate);

	TransactionManager::ContextSource createContextSource(
		const FixedPart &request, const SearchEntry &entry);

	void decodeMultiSearchEntry(util::ByteStream<util::ArrayInStream> &in,
		PartitionId pId, DatabaseId dbId, const char *dbName,
		ContainerAccessMode containerAccessMode,
		util::XArray<SearchEntry> &searchList);
	RowKeyPredicate decodePredicate(
		util::ByteStream<util::ArrayInStream> &in, util::StackAllocator &alloc);

	void encodeEntry(const util::String &containerName, ContainerId containerId,
		const SchemaMap &schemaMap, ResultSet &rs, EventByteOutStream &out);
};
/*!
	@brief Handles MULTI_QUERY statement
*/
class MultiQueryHandler : public MultiStatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

private:
	struct QueryRequest {
		explicit QueryRequest(util::StackAllocator &alloc)
			: stmtType_(UNDEF_EVENT_TYPE),
			  stmtId_(UNDEF_STATEMENTID),
			  containerId_(UNDEF_CONTAINERID),
			  sessionId_(UNDEF_SESSIONID),
			  schemaVersionId_(UNDEF_SCHEMAVERSIONID),
			  getMode_(TransactionManager::AUTO),
			  txnMode_(TransactionManager::AUTO_COMMIT),
			  optionPart_(alloc) {
			query_.ptr_ = NULL;
		}

		EventType stmtType_;

		StatementId stmtId_;
		ContainerId containerId_;
		SessionId sessionId_;
		SchemaVersionId schemaVersionId_;
		TransactionManager::GetMode getMode_;
		TransactionManager::TransactionMode txnMode_;

		OptionPart optionPart_;

		FetchOption fetchOption_;

		union {
			void *ptr_;
			util::String *tqlQuery_;
			RangeQuery *rangeQuery_;
			SamplingQuery *samplingQuery_;
		} query_;
	};

	enum PartialResult { PARTIAL_RESULT_SUCCESS, PARTIAL_RESULT_FAIL };

	typedef util::XArray<const QueryRequest *> QueryRequestList;

	void execute(EventContext &ec, const FixedPart &request,
		const QueryRequest &queryRequest, EventByteOutStream &replyOut,
		Progress &progress);

	TransactionManager::ContextSource createContextSource(
		const FixedPart &request, const QueryRequest &queryRequest);

	void decodeMultiQuery(util::ByteStream<util::ArrayInStream> &in,
		const FixedPart &request,
		util::XArray<const QueryRequest *> &queryList);

	void encodeMultiSearchResultHead(
		EventByteOutStream &out, uint32_t queryCount);
	void encodeSearchResult(EventByteOutStream &out, const ResultSet &rs);

	const char8_t *getQueryTypeName(EventType queryStmtType);
};

/*!
	@brief Handles REPLICATION_LOG statement requested from another node
*/
class ReplicationLogHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};
/*!
	@brief Handles REPLICATION_ACK statement requested from another node
*/
class ReplicationAckHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};


/*!
	@brief Handles CHECK_TIMEOUT event
*/
class CheckTimeoutHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

private:
	static const size_t MAX_OUTPUT_COUNT =
		100;  
	static const uint64_t TIMEOUT_CHECK_STATE_TRACE_COUNT =
		100;  

	std::vector<uint64_t> timeoutCheckCount_;

	void checkReplicationTimeout(EventContext &ec);
	void checkTransactionTimeout(
		EventContext &ec, const util::XArray<bool> &checkPartitionFlagList);
	void checkRequestTimeout(
		EventContext &ec, const util::XArray<bool> &checkPartitionFlagList);
	void checkResultSetTimeout(EventContext &ec);

	bool isTransactionTimeoutCheckEnabled(
		PartitionGroupId pgId, util::XArray<bool> &checkPartitionFlagList);
};

/*!
	@brief Handles unknown statement
*/
class UnknownStatementHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles unsupported statement
*/
class UnsupportedStatementHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles ignorable statement
*/
class IgnorableStatementHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles DATA_STORE_PERIODICALLY event
*/
class DataStorePeriodicallyHandler : public StatementHandler {
public:
	DataStorePeriodicallyHandler();
	~DataStorePeriodicallyHandler();

	void operator()(EventContext &ec, Event &ev);

private:
	static const uint64_t PERIODICAL_MAX_SCAN_COUNT =
		500;  
	PartitionId *pIdCursor_;
};

/*!
	@brief Handles ADJUST_STORE_MEMORY_PERIODICALLY event
*/
class AdjustStoreMemoryPeriodicallyHandler : public StatementHandler {
public:
	AdjustStoreMemoryPeriodicallyHandler();
	~AdjustStoreMemoryPeriodicallyHandler();

	void operator()(EventContext &ec, Event &ev);

private:
	PartitionId *pIdCursor_;
};


/*!
	@brief TransactionService
*/
class TransactionService {
public:
	TransactionService(const ConfigTable &config,
		const EventEngine::Config &eeConfig,
		const EventEngine::Source &eeSource, const char *name);
	~TransactionService();

	void initialize(const ManagerSet &mgrSet);

	void start();
	void shutdown();
	void waitForShutdown();

	EventEngine *getEE();

	void changeTimeoutCheckMode(PartitionId pId,
		PartitionTable::PartitionStatus after, ChangePartitionType changeType,
		bool isToSubMaster, ClusterStats &stats);

	void enableTransactionTimeoutCheck(PartitionId pId);
	void disableTransactionTimeoutCheck(PartitionId pId);
	bool isTransactionTimeoutCheckEnabled(PartitionId pId) const;

	uint64_t getTotalReadOperationCount() const;
	uint64_t getTotalWriteOperationCount() const;

	uint64_t getTotalRowReadCount() const;
	uint64_t getTotalRowWriteCount() const;

	void incrementReadOperationCount(PartitionId pId);
	void incrementWriteOperationCount(PartitionId pId);

	void addRowReadCount(PartitionId pId, uint64_t count);
	void addRowWriteCount(PartitionId pId, uint64_t count);

private:
	static class StatSetUpHandler : public StatTable::SetUpHandler {
		virtual void operator()(StatTable &stat);
	} statSetUpHandler_;

	class StatUpdator : public StatTable::StatUpdator {
		virtual bool operator()(StatTable &stat);

	public:
		StatUpdator();
		TransactionService *service_;
		TransactionManager *manager_;
	} statUpdator_;

	EventEngine::Config eeConfig_;
	const EventEngine::Source eeSource_;
	EventEngine *ee_;

	bool initalized_;

	const PartitionGroupConfig pgConfig_;

	std::vector<util::Atomic<bool> > enableTxnTimeoutCheck_;

	std::vector<uint64_t> readOperationCount_;
	std::vector<uint64_t> writeOperationCount_;
	std::vector<uint64_t> rowReadCount_;
	std::vector<uint64_t> rowWriteCount_;

	static const int32_t TXN_TIMEOUT_CHECK_INTERVAL =
		3;  
	static const int32_t CHUNK_EXPIRE_CHECK_INTERVAL =
		1;  
	static const int32_t ADJUST_STORE_MEMORY_CHECK_INTERVAL =
		60;  

	ServiceThreadErrorHandler serviceThreadErrorHandler_;

	ConnectHandler connectHandler_;
	DisconnectHandler disconnectHandler_;
	LoginHandler loginHandler_;
	LogoutHandler logoutHandler_;
	GetPartitionAddressHandler getPartitionAddressHandler_;
	GetPartitionContainerNamesHandler getPartitionContainerNamesHandler_;
	GetContainerPropertiesHandler getContainerPropertiesHandler_;
	PutContainerHandler putContainerHandler_;
	DropContainerHandler dropContainerHandler_;
	GetContainerHandler getContainerHandler_;
	CreateDropIndexHandler createDropIndexHandler_;
	CreateDropTriggerHandler createDropTriggerHandler_;
	FlushLogHandler flushLogHandler_;
	WriteLogPeriodicallyHandler writeLogPeriodicallyHandler_;
	CreateTransactionContextHandler createTransactionContextHandler_;
	CloseTransactionContextHandler closeTransactionContextHandler_;
	CommitAbortTransactionHandler commitAbortTransactionHandler_;
	PutRowHandler putRowHandler_;
	PutRowSetHandler putRowSetHandler_;
	RemoveRowHandler removeRowHandler_;
	UpdateRowByIdHandler updateRowByIdHandler_;
	RemoveRowByIdHandler removeRowByIdHandler_;
	GetRowHandler getRowHandler_;
	GetRowSetHandler getRowSetHandler_;
	QueryTqlHandler queryTqlHandler_;


	AppendRowHandler appendRowHandler_;
	GetRowTimeRelatedHandler getRowTimeRelatedHandler_;
	GetRowInterpolateHandler getRowInterpolateHandler_;
	AggregateHandler aggregateHandler_;
	QueryTimeRangeHandler queryTimeRangeHandler_;
	QueryTimeSamplingHandler queryTimeSamplingHandler_;
	FetchResultSetHandler fetchResultSetHandler_;
	CloseResultSetHandler closeResultSetHandler_;
	MultiCreateTransactionContextHandler multiCreateTransactionContextHandler_;
	MultiCloseTransactionContextHandler multiCloseTransactionContextHandler_;
	MultiPutHandler multiPutHandler_;
	MultiGetHandler multiGetHandler_;
	MultiQueryHandler multiQueryHandler_;
	ReplicationLogHandler replicationLogHandler_;
	ReplicationAckHandler replicationAckHandler_;
	CheckTimeoutHandler checkTimeoutHandler_;

	UnsupportedStatementHandler unsupportedStatementHandler_;
	UnknownStatementHandler unknownStatementHandler_;
	IgnorableStatementHandler ignorableStatementHandler_;

	DataStorePeriodicallyHandler dataStorePeriodicallyHandler_;
	AdjustStoreMemoryPeriodicallyHandler adjustStoreMemoryPeriodicallyHandler_;

	ShortTermSyncHandler shortTermSyncHandler_;
	LongTermSyncHandler longTermSyncHandler_;
	SyncTimeoutHandler syncTimeoutHandler_;
	DropPartitionHandler dropPartitionHandler_;
	ChangePartitionStateHandler changePartitionStateHandler_;
	ChangePartitionTableHandler changePartitionTableHandler_;

	CheckpointOperationHandler checkpointOperationHandler_;

};

inline void TransactionService::incrementReadOperationCount(PartitionId pId) {
	++readOperationCount_[pId];
}

inline void TransactionService::incrementWriteOperationCount(PartitionId pId) {
	++writeOperationCount_[pId];
}

inline void TransactionService::addRowReadCount(
	PartitionId pId, uint64_t count) {
	rowReadCount_[pId] += count;
}

inline void TransactionService::addRowWriteCount(
	PartitionId pId, uint64_t count) {
	rowWriteCount_[pId] += count;
}


#endif
