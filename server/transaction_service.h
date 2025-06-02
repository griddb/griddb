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
	@brief Definition of TransactionService
*/
#ifndef TRANSACTION_SERVICE_H_
#define TRANSACTION_SERVICE_H_

#include "util/trace.h"
#include "data_type.h"
#define AUDIT_TRACE_ERROR(tracer, cause, userName, privilege, dbName, appName, sourceAddr, \
						 destAddr, target, statementType, statementInfo, statementId, message)
#define AUDIT_TRACE_ERROR_CODED(tracer, code, userName, privilege, dbName, appName, sourceAddr, \
						 destAddr, target, statementType, statementInfo, statementId, message)
#define AUDIT_TRACER_DECLARE(name)
#define AUDIT_TRACE_INFO(tracer, code, userName, privilege, dbName, appName, sourceAddr, \
						 destAddr, target, statementType, statementInfo, statementId, message)

#include "cluster_event_type.h"
#include "event_engine.h"
#include "transaction_manager.h"
#include "checkpoint_service.h"
#include "data_store_v4.h"
#include "sync_service.h"
#include "result_set.h"
#include "transaction_statement_message.h"
#include "message_schema.h"
#include "key_data_store.h"

const int LDAP_STATUS_INIT         = 0;
const int LDAP_STATUS_BIND_ROOT    = 1;
const int LDAP_STATUS_SEARCH_USER  = 2;
const int LDAP_STATUS_BIND_USER    = 3;
const int LDAP_STATUS_SEARCH_GROUP = 4;
const int LDAP_STATUS_FIN          = 5;



#include "sql_job_manager.h"

struct TableSchemaInfo;
class DDLResultHandler;

#define TXN_PRIVATE private
#define TXN_PROTECTED protected

typedef StatementId ExecId;

typedef util::ByteStream< util::XArrayOutStream<> > OutStream;

const bool TXN_DETAIL_EXCEPTION_HIDDEN = true;

class SystemService;
class ClusterService;
class TriggerService;
class PartitionTable;
class DataStoreV4;
template <class L> class LogManager;
class ResultSet;
class ResultSetOption;
class MetaContainer;
struct ManagerSet;

class PutUserHandler;
class DropUserHandler;
class GetUsersHandler;
class PutDatabaseHandler;
class DropDatabaseHandler;
class GetDatabasesHandler;
class PutPrivilegeHandler;
class DropPrivilegeHandler;
struct SQLParsedInfo;
class KeyDataStore;
template<typename Alloc>
struct TablePartitioningInfo;
class DataStoreLog;
class TxnLogManager;

UTIL_TRACER_DECLARE(TRANSACTION_SERVICE);
UTIL_TRACER_DECLARE(REPLICATION);
UTIL_TRACER_DECLARE(SESSION_TIMEOUT);
UTIL_TRACER_DECLARE(TRANSACTION_TIMEOUT);
UTIL_TRACER_DECLARE(REPLICATION_TIMEOUT);
UTIL_TRACER_DECLARE(AUTHENTICATION_TIMEOUT);

/*!
	@brief Exception class for denying the statement execution
*/
class DenyException : public util::Exception {
public:
	explicit DenyException(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_DECL) throw() :
			Exception(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_SET) {}
	virtual ~DenyException() throw() {}
};

enum BackgroundEventType {
	TXN_BACKGROUND,
	SYNC_EXEC,
	CP_CHUNKCOPY
};


/*!
	@brief Handles the statement(event) requested from a client or another node
*/
class StatementHandler : public EventHandler {
	friend struct ScenarioConfig;
	friend class TransactionHandlerTest;

public:
	typedef StatementMessage Message;

	typedef Message::OptionType OptionType;
	typedef Message::Utils MessageUtils;
	typedef Message::OptionSet OptionSet;
	typedef Message::FixedRequest FixedRequest;
	typedef Message::Request Request;
	typedef Message::Options Options;

	typedef Message::RequestType RequestType;
	typedef Message::UserType UserType;
	typedef Message::CaseSensitivity CaseSensitivity;
	typedef Message::QueryContainerKey QueryContainerKey;

	typedef Message::UUIDObject UUIDObject;
	typedef Message::ExtensionParams ExtensionParams;
	typedef Message::ExtensionColumnTypes ExtensionColumnTypes;
	typedef Message::IntervalBaseValue IntervalBaseValue;
	typedef Message::PragmaList PragmaList;
	typedef Message::CompositeIndexInfos CompositeIndexInfos;


	StatementHandler();

	virtual ~StatementHandler();
	void initialize(const ManagerSet &mgrSet, bool isNewSQL = false);

	static const size_t USER_NAME_SIZE_MAX = 64;  
	static const size_t PASSWORD_SIZE_MAX = 64;  
	static const size_t DATABASE_NAME_SIZE_MAX = 64;  

	static const size_t USER_NUM_MAX = 1000;  
	static const size_t DATABASE_NUM_MAX = TXN_DATABASE_NUM_MAX;  

	enum QueryResponseType {
		ROW_SET					= 0,
		AGGREGATION				= 1,
		QUERY_ANALYSIS			= 2,
		PARTIAL_FETCH_STATE		= 3,	
		PARTIAL_EXECUTION_STATE = 4,	
		DIST_TARGET		= 32,	
		DIST_LIMIT		= 33,	
		DIST_AGGREGATION = 34,	
		DIST_ORDER		= 35,	
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
	static const ProtocolVersion TXN_V3_1_X_CLIENT_VERSION;
	static const ProtocolVersion TXN_V3_2_X_CLIENT_VERSION;
	static const ProtocolVersion TXN_V3_5_X_CLIENT_VERSION;
	static const ProtocolVersion TXN_V4_0_0_CLIENT_VERSION;
	static const ProtocolVersion TXN_V4_0_1_CLIENT_VERSION;
	static const ProtocolVersion TXN_CLIENT_VERSION;


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

	typedef util::XArray<uint8_t> RowKeyData;  
	typedef util::XArray<uint8_t> RowData;	 
	typedef util::Map<int8_t, util::XArray<uint8_t> *> PartialQueryOption;	 

	static const uint8_t CONNECTION_ROUTE_MIN = 0;
	static const uint8_t CONNECTION_ROUTE_LOCAL = 0;
	static const uint8_t CONNECTION_ROUTE_PUBLIC = 1;
	static const uint8_t CONNECTION_ROUTE_MAX = 2;
	
	/*!
		@brief Judge public connection route
	*/
	/** **
		@brief 外部接続指定があるかどうか
		@param [in] request 入力情報
		@param [in] check メッセージ不正時にエラーを返すかどうか（上位で指定）
	** **/
	static bool usePublicConection(Request& request, bool check = true) {
		int8_t connectionRoute = request.optional_.get<Options::CONNECTION_ROUTE>();
		if (check && (connectionRoute < CONNECTION_ROUTE_MIN || connectionRoute >= CONNECTION_ROUTE_MAX)) {
			TXN_THROW_DECODE_ERROR(GS_ERROR_TXN_DECODE_FAILED,
				"Invalid connection route=" << static_cast<int32_t>(connectionRoute));
		}
		return (connectionRoute == CONNECTION_ROUTE_PUBLIC);
	}

	/*!
		@brief Represents fetch setting
	*/
	struct FetchOption {
		FetchOption() : limit_(0), size_(0) {}

		ResultSize limit_;
		ResultSize size_;
	};


	/*!
		@brief Represents the information about a user
	*/
	struct UserInfo {
		explicit UserInfo(util::StackAllocator &alloc)
			: userName_(alloc),
			  property_(0),
			  withDigest_(false),
			  digest_(alloc),
			  isGroupMapping_(false),
			  roleName_(alloc) {}

		util::String userName_;  
		int8_t property_;  
		bool withDigest_;	  
		util::String digest_;  
		bool isGroupMapping_;	
		util::String roleName_;	

		std::string dump() {
			util::NormalOStringStream strstrm;
			strstrm << "\tuserName=" << userName_ << std::endl;
			strstrm << "\tproperty=" << static_cast<int>(property_)
					<< std::endl;
			if (withDigest_) {
				strstrm << "\twithDigest=true" << std::endl;
			}
			else {
				strstrm << "\twithDigest=false" << std::endl;
			}
			strstrm << "\tdigest=" << digest_ << std::endl;
			if (isGroupMapping_) {
				strstrm << "\tgroupMapping=true" << std::endl;
			}
			else {
				strstrm << "\tgroupMapping=false" << std::endl;
			}
			strstrm << "\troleName=" << roleName_ << std::endl;
			return strstrm.str();
		}
	};

	/*!
		@brief Represents the privilege information about a user
	*/
	struct PrivilegeInfo {
		explicit PrivilegeInfo(util::StackAllocator &alloc)
			: userName_(alloc), privilege_(alloc) {}

		util::String userName_;   
		util::String privilege_;  

		std::string dump() {
			util::NormalOStringStream strstrm;
			strstrm << "\tuserName=" << userName_ << std::endl;
			strstrm << "\tprivilege=" << privilege_ << std::endl;
			return strstrm.str();
		}
	};

	/*!
		@brief Represents the information about a database
	*/
	struct DatabaseInfo {
		explicit DatabaseInfo(util::StackAllocator &alloc)
			: dbName_(alloc), property_(0), privilegeInfoList_(alloc) {}

		util::String dbName_;  
		int8_t property_;	  
		util::XArray<PrivilegeInfo *> privilegeInfoList_;  

		std::string dump() {
			util::NormalOStringStream strstrm;
			strstrm << "\tdbName=" << dbName_ << std::endl;
			strstrm << "\tproperty=" << static_cast<int>(property_)
					<< std::endl;
			strstrm << "\tprivilegeNum=" << privilegeInfoList_.size()
					<< std::endl;
			for (size_t i = 0; i < privilegeInfoList_.size(); i++) {
				strstrm << privilegeInfoList_[i]->dump();
			}
			return strstrm.str();
		}
	};

	struct ConnectionOption;

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
			  currentStatus_(-1),
			  currentAffinity_(UNDEF_NODE_AFFINITY_NUMBER),
			  indexInfo_(alloc),
			  binaryData2_(alloc),
			  rs_(NULL),
			  last_(0),
			  userInfoList_(alloc),
			  databaseInfoList_(alloc),
			  containerAttribute_(CONTAINER_ATTR_SINGLE),
			  putRowOption_(0),
			  schemaMessage_(NULL)
			  , compositeIndexInfos_(NULL)
			  , connectionOption_(NULL)
		{
		}


		util::XArray<uint8_t> binaryData_;

		uint64_t containerNum_;
		util::XArray<FullContainerKey> containerNameList_;


		bool existFlag_;
		SchemaVersionId schemaVersionId_;
		ContainerId containerId_;

		LargeContainerStatusType currentStatus_;
		NodeAffinityNumber currentAffinity_;
		IndexInfo indexInfo_;
		uint8_t existIndex_;

		util::XArray<uint8_t> binaryData2_;


		ResultSet *rs_;




		RowId last_;




		util::XArray<UserInfo *> userInfoList_;

		util::XArray<DatabaseInfo *> databaseInfoList_;

		ContainerAttribute containerAttribute_;

		uint8_t putRowOption_;
		TableSchemaInfo *schemaMessage_;
		CompositeIndexInfos *compositeIndexInfos_;

		ConnectionOption *connectionOption_;
	};

	/*!
		@brief Represents the information about ReplicationAck
	*/
	struct ReplicationAck {
		explicit ReplicationAck(PartitionId pId) : pId_(pId) {}

		const PartitionId pId_;
		ClusterVersionId clusterVer_;
		ReplicationId replId_;

		int32_t replMode_;
		int32_t replStmtType_;
		StatementId replStmtId_;
		ClientId clientId_;
		int32_t taskStatus_;
	};

	struct CommonMessageHeader {
		CommonMessageHeader(StatementId stmtId, StatementExecStatus status)
			: stmtId_(stmtId), status_(status) {}
		template<typename S>
		void encode(S& out) {
			try {
				out << stmtId_;
				out << status_;
			}
			catch (std::exception& e) {
				TXN_RETHROW_ENCODE_ERROR(e, "");
			}
		}

		StatementId stmtId_; 
		StatementExecStatus status_; 
	};

	struct SimpleOutputMessage : public Serializable {
		SimpleOutputMessage(StatementId stmtId, StatementExecStatus status,	
			OptionSet* optionSet, Serializable *dsMes = NULL)
			: Serializable(NULL), header_(stmtId, status), optionSet_(optionSet), dsMes_(dsMes) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template <typename S>
		void encode(S& out) {
			header_.encode(out);
			if (optionSet_ != NULL) {
				optionSet_->encode<S>(out);
			}
		}
		template <typename S>
		void decode(S& in) {
			UNUSED_VARIABLE(in);
		}

		virtual void addExtraMessage(Event *event) {
			UNUSED_VARIABLE(event);
		}
		virtual void addExtraMessage(ReplicationContext* replicationContext) {
			UNUSED_VARIABLE(replicationContext);
		}

		CommonMessageHeader header_;
		OptionSet *optionSet_; 
		Serializable* dsMes_;
	};

	struct DSMessage : public SimpleOutputMessage {
		DSMessage(StatementId stmtId, StatementExecStatus status,
			OptionSet* optionSet, Serializable* dsMes)
			: SimpleOutputMessage(stmtId, status, optionSet), dsMes_(dsMes) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			SimpleOutputMessage::encode(out);
			if (dsMes_ != NULL) {
				dsMes_->encode(out);
			}
		}
		template<typename S>
		void decode(S& in) {
			UNUSED_VARIABLE(in);
		}
		Serializable* dsMes_;
	};

	struct RowExistMessage : public SimpleOutputMessage {
		RowExistMessage(StatementId stmtId, StatementExecStatus status,
			OptionSet* optionSet, bool exist)
			: SimpleOutputMessage(stmtId, status, optionSet), existFlag_(exist) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			SimpleOutputMessage::encode(out);
			encodeBooleanData<S>(out, existFlag_);
		}
		template<typename S>
		void decode(S& in) {
			UNUSED_VARIABLE(in);
		}
		bool existFlag_;
	};

	struct SimpleQueryOutputMessage : public SimpleOutputMessage {
		SimpleQueryOutputMessage(StatementId stmtId, StatementExecStatus status,
			OptionSet* optionSet, ResultSet* rs, bool existFlag)
			: SimpleOutputMessage(stmtId, status, optionSet),
			rs_(rs), existFlag_(existFlag) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			SimpleOutputMessage::encode(out);
			encodeBooleanData<S>(out, existFlag_);
			if (existFlag_) {
				encodeBinaryData<S>(out, rs_->getFixedStartData(),
					rs_->getFixedOffsetSize());
				encodeBinaryData<S>(out, rs_->getVarStartData(),
					rs_->getVarOffsetSize());
			}
		}
		template<typename S>
		void decode(S& in) {
			UNUSED_VARIABLE(in);
		}

		ResultSet* rs_;
		bool existFlag_;
	};

	struct TQLOutputMessage : public SimpleOutputMessage {
		TQLOutputMessage(StatementId stmtId, StatementExecStatus status,
			OptionSet* optionSet, util::StackAllocator& alloc, ResultSet* rs,
			Event& ev, bool isSQL)
			: SimpleOutputMessage(stmtId, status, optionSet), alloc_(alloc), rs_(rs),
				ev_(ev), isSQL_(isSQL) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out);
		template<typename S>
		void decode(S& in) {
			UNUSED_VARIABLE(in);
		}
		void addExtraMessage(Event* event);
		void addExtraMessage(ReplicationContext* replicationContext);

		util::StackAllocator& alloc_;
		ResultSet* rs_;
		Event& ev_;
		bool isSQL_;
	};

	struct ReplicationOutputMessage : public Serializable {
		ReplicationOutputMessage(const FixedRequest& request,
			OptionSet* optionSet,
			ReplicationId replId,
			const ClientId& clientId,
			EventType replStmtType,
			StatementId replStmtId,
			int32_t replMode,
			ReplicationContext::TaskStatus taskStatus,
			uint16_t logVersion,
			const ClientId* closedResourceIds, size_t closedResourceIdCount,
			const util::XArray<uint8_t>** logRecordList, size_t logRecordCount)
			: Serializable(NULL), request_(request), optionSet_(optionSet),
			replId_(replId),
			clientId_(clientId),
			replStmtType_(replStmtType),
			replStmtId_(replStmtId),
			replMode_(replMode),
			taskStatus_(taskStatus),
			logVersion_(logVersion),
			closedResourceIds_(closedResourceIds), closedResourceIdCount_(closedResourceIdCount), 
			logRecordList_(logRecordList), logRecordCount_(logRecordCount) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template <typename S>
		void encode(S& out);
		template <typename S>
		void decode(S& in) {
			UNUSED_VARIABLE(in);
		}

		void setNodeAddress(NodeAddress& address) {
			address_ = address;
		}

		const FixedRequest& request_;
		OptionSet* optionSet_;
		ReplicationId replId_;
		const ClientId& clientId_;
		EventType replStmtType_;
		StatementId replStmtId_;
		int32_t replMode_;
		ReplicationContext::TaskStatus taskStatus_;
		uint16_t logVersion_;
		const ClientId* closedResourceIds_;
		size_t closedResourceIdCount_;
		const util::XArray<uint8_t>** logRecordList_;
		size_t logRecordCount_;
		NodeAddress address_;
	};


	struct SimpleInputMessage : public Serializable {
		SimpleInputMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: Serializable(NULL), connOption_(connOption), request_(alloc, src) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			StatementHandler::decodeRequestCommonPart(in, request_, connOption_);
		}

		ConnectionOption& connOption_;
		Request request_;
	};

	struct KeyInputMessage : public SimpleInputMessage {
		KeyInputMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), keyData_(alloc) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeBinaryData<S>(in, keyData_, true);
		}

		RowKeyData keyData_;
	};


	struct QueryInputMessage : public SimpleInputMessage {
		QueryInputMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), alloc_(alloc),
			isPartial_(false), partialQueryOption_(alloc) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			StatementHandler::decodeFetchOption(in, fetchOption_);
			StatementHandler::decodePartialQueryOption(in, alloc_, isPartial_, partialQueryOption_);
		}

		util::StackAllocator& alloc_;
		FetchOption fetchOption_;
		bool isPartial_;
		PartialQueryOption partialQueryOption_;
	};


	void setSuccessReply(
			util::StackAllocator &alloc, Event &ev, StatementId stmtId,
			StatementExecStatus status, const Response &response,
			const Request &request);
	static void setErrorReply(
			Event &ev, StatementId stmtId,
			StatementExecStatus status, const std::exception &exception,
			const NodeDescriptor &nd);
	static std::string getAuditContainerName(util::StackAllocator &alloc, FullContainerKey* getcontainerKey);
	static std::string getClientAddress(const Event & ev);
	static std::string getClientAddress(const NodeDescriptor& nd);

	static OptionSet* getReplyOption(util::StackAllocator& alloc,
		const Request& request, CompositeIndexInfos* infos, bool isContinue);
	static OptionSet* getReplyOption(util::StackAllocator& alloc, 
		const ReplicationContext& replContext, CompositeIndexInfos* infos, bool isContinue);
	static void setReplyOption(OptionSet &optionSet, const Request &request);
	static void setReplyOption(
			OptionSet &optionSet, const ReplicationContext &replContext);

	static void setReplyOptionForContinue(
			OptionSet &optionSet, const Request &request);
	static void setReplyOptionForContinue(
			OptionSet &optionSet, const ReplicationContext &replContext);

	static void setSQLResponseInfo(
			ReplicationContext &replContext, const Request &request);

	static EventType resolveReplyEventType(
			EventType stmtType, const OptionSet &optionSet);

	const char* AuditEventType(const Event &ev);

	int32_t AuditCategoryType(const Event &ev);

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

	typedef std::pair<const char8_t*, const char8_t*> ExceptionParameterEntry;
	typedef util::Vector<ExceptionParameterEntry> ExceptionParameterList;

	ClusterService *clusterService_;
	ClusterManager *clusterManager_;
	PartitionList *partitionList_;
	PartitionTable *partitionTable_;
	TransactionService *transactionService_;
	TransactionManager *transactionManager_;
	SystemService *systemService_;
	RecoveryManager *recoveryManager_;
	SQLService *sqlService_;
	bool isNewSQL_;
	DataStoreConfig* dsConfig_;
	const ManagerSet *resourceSet_;

	/*!
		@brief Represents the information about a connection
	*/
	struct ConnectionOption {
	public:
		ConnectionOption()
			: clientVersion_(PROTOCOL_VERSION_UNDEFINED),
			  txnTimeoutInterval_(TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL),
			  isAuthenticated_(false),
			  isImmediateConsistency_(false),
			  handlingPartitionId_(UNDEF_PARTITIONID),
			  handlingClientId_(),
			  connected_(true),
			  dbId_(0),
			  isAdminAndPublicDB_(true),
			  userType_(Message::USER_NORMAL),
			  authenticationTime_(0),
			  requestType_(Message::REQUEST_NOSQL)
			  ,
			  authMode_(0)
			  ,
			  storeMemoryAgingSwapRate_(TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE),
			  timeZone_(util::TimeZone()),
				updatedEnvBits_(0),
				retryCount_(0)
			  ,clientId_()
			  ,keepaliveTime_(0)
			,
			connectionRoute_(StatementHandler::CONNECTION_ROUTE_LOCAL),
			acceptableFeatureVersion_(0)
		{
		}

		void clear() {
			clientVersion_ = PROTOCOL_VERSION_UNDEFINED;
			txnTimeoutInterval_ = TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL;
			isAuthenticated_ = false;
			isImmediateConsistency_ = false;
			handlingPartitionId_ = UNDEF_PARTITIONID;
			connected_ = false;
			updatedEnvBits_ = 0;
			retryCount_ = 0;
			dbId_ = 0;
			isAdminAndPublicDB_ = true;
			userType_ = Message::USER_NORMAL;
			authenticationTime_ = 0;
			requestType_ = Message::REQUEST_NOSQL;

			storeMemoryAgingSwapRate_ = TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE;
			timeZone_ = util::TimeZone();
			keepaliveTime_ = 0;
			currentSessionId_ = 0;
			connectionRoute_ = StatementHandler::CONNECTION_ROUTE_LOCAL;
			acceptableFeatureVersion_ = 0;
			initializeCoreInfo();
		}

		void getHandlingClientId(ClientId &clientId);
		void setHandlingClientId(ClientId &clientId);
		void getLoginInfo(SQLString &userName, SQLString &dbName, SQLString &applicationName);
		void getSessionIdList(ClientId &clientId, util::XArray<SessionId> &sessionIdList);
		void setConnectionEnv(uint32_t paramId, const char8_t *value);
		bool getConnectionEnv(uint32_t paramId, std::string &value, bool &hasData);
		void removeSessionId(ClientId &clientId);

		void initializeCoreInfo();

		void setFirstStep(
				ClientId &clientId, double storeMemoryAgingSwapRate,
				const util::TimeZone &timeZone, bool isLDAPAuthentication, 
				int32_t acceptableFeatureVersion);
		void setSecondStep(const char8_t *userName, const char8_t *digest, const char8_t *dbName, const char8_t *applicationName,
			bool isImmediateConsistency, int32_t txnTimeoutInterval, RequestType requestType);
		void setBeforeAuth(UserType userType, bool isAdminAndPublicDB);
		void setAfterAuth(DatabaseId dbId, EventMonotonicTime authenticationTime, PrivilegeType priv, const char8_t *roleName);

		void checkPrivilegeForOperator();
		void checkForUpdate(bool forUpdate);
		void checkSelect(SQLParsedInfo &parsedInfo);

		std::string getAuditApplicationName();
		template<typename Alloc>
		bool getApplicationName(
				util::BasicString<
						char8_t, std::char_traits<char8_t>, Alloc> &name) {
			util::BasicString<
					char8_t, std::char_traits<char8_t>,
					util::StdAllocator<char8_t, void> > localName(
					*name.get_allocator().base());
			const bool ret = getApplicationName(&localName);
			name = localName.c_str();
			return ret;
		}
		bool getApplicationName(
				util::BasicString<
						char8_t, std::char_traits<char8_t>,
						util::StdAllocator<char8_t, void> > *name);
		void getApplicationName(std::ostream &os);

		void getDbApplicationName(util::String& dbName, util::String &applicationName);

		bool isPublicConnection() {
			return (connectionRoute_ == StatementHandler::CONNECTION_ROUTE_PUBLIC);
		}
		int8_t getConnectionRoute() {
			return connectionRoute_;
		}
		void setPublicConnection() {
			connectionRoute_ = StatementHandler::CONNECTION_ROUTE_PUBLIC;
		}

		ProtocolVersion clientVersion_;
		int32_t txnTimeoutInterval_;
		bool isAuthenticated_;
		bool isImmediateConsistency_;

		PartitionId handlingPartitionId_;
		ClientId handlingClientId_;
		bool connected_;
		DatabaseId dbId_;
		bool isAdminAndPublicDB_;
		UserType userType_;  
		EventMonotonicTime authenticationTime_;
		RequestType requestType_;  

		std::string userName_;
		std::string dbName_;
		PrivilegeType priv_;
		std::string digest_;
		bool isLDAPAuthentication_;
		std::string roleName_;
		const int8_t authMode_;
		double storeMemoryAgingSwapRate_;
		util::TimeZone timeZone_;

		uint32_t updatedEnvBits_;
		int32_t retryCount_;

		ClientId clientId_;
		EventMonotonicTime keepaliveTime_;
		SessionId currentSessionId_;
		int8_t connectionRoute_;
		int32_t acceptableFeatureVersion_;
	private:
		util::Mutex mutex_;
		std::string applicationName_;
		std::set<SessionId> statementList_;
		std::map<uint32_t, std::string> envMap_;

	};

	struct LockConflictStatus {
		LockConflictStatus(
				int32_t txnTimeoutInterval,
				EventMonotonicTime initialConflictMillis,
				EventMonotonicTime emNow) :
				txnTimeoutInterval_(txnTimeoutInterval),
				initialConflictMillis_(initialConflictMillis),
				emNow_(emNow) {
		}

		int32_t txnTimeoutInterval_;
		EventMonotonicTime initialConflictMillis_;
		EventMonotonicTime emNow_;
	};

	struct ErrorMessage {
		ErrorMessage(
				std::exception &cause, const char8_t *description,
				const Event &ev, const Request &request) :
				elements_(
						cause, description, ev,
						request.fixed_, &request.optional_) {
		}

		ErrorMessage(
				std::exception &cause, const char8_t *description,
				const Event &ev, const FixedRequest &request) :
				elements_(cause, description, ev, request, NULL) {
		}

		ErrorMessage withDescription(const char8_t *description) const {
			ErrorMessage org = *this;
			org.elements_.description_ = description;
			return org;
		}

		void format(std::ostream &os) const;
		void formatParameters(std::ostream &os) const;

		struct Elements {
			Elements(
					std::exception &cause, const char8_t *description,
					const Event &ev, const FixedRequest &request,
					const OptionSet *options) :
					cause_(cause),
					description_(description),
					ev_(ev),
					pId_(ev_.getPartitionId()),
					stmtType_(ev_.getType()),
					clientId_(request.clientId_),
					stmtId_(request.cxtSrc_.stmtId_),
					containerId_(request.cxtSrc_.containerId_),
					options_(options) {
			}
			std::exception &cause_;
			const char8_t *description_;
			const Event &ev_;
			PartitionId pId_;
			EventType stmtType_;
			ClientId clientId_;
			StatementId stmtId_;
			ContainerId containerId_;
			const OptionSet *options_;
		} elements_;
	};


	void checkAuthentication(
			const NodeDescriptor &ND, EventMonotonicTime emNow);
	void checkConsistency(const NodeDescriptor &ND, bool requireImmediate);
	void checkExecutable(
			PartitionId pId, ClusterRole requiredClusterRole,
			PartitionRoleType requiredPartitionRole,
			PartitionStatus requiredPartitionStatus, PartitionTable *pt);
	void checkExecutable(ClusterRole requiredClusterRole);
	static void checkExecutable(ClusterRole requiredClusterRole, PartitionTable *pt);

	void checkTransactionTimeout(
			EventMonotonicTime now,
			EventMonotonicTime queuedTime, int32_t txnTimeoutIntervalSec,
			uint32_t queueingCount);
	void checkContainerExistence(BaseContainer *container);
	void checkContainerExistence(KeyDataStoreValue &keyStoreValue);
	void checkContainerSchemaVersion(
			BaseContainer *container, SchemaVersionId schemaVersionId);
	static void checkSchemaFeatureVersion(
			const BaseContainer &container, int32_t acceptableFeatureVersion);
	static void checkSchemaFeatureVersion(
			int32_t schemaFetureLevel, int32_t acceptableFeatureVersion);
	static void checkMetaTableFeatureVersion(uint8_t unit, int32_t acceptableFeatureVersion);

	void checkFetchOption(FetchOption fetchOption);
	void checkSizeLimit(ResultSize limit);
	static void checkLoggedInDatabase(
			DatabaseId loginDbId, const char8_t *loginDbName,
			DatabaseId specifiedDbId, const char8_t *specifiedDbName
			, bool isNewSQL
			);
	void checkQueryOption(
			const OptionSet &optionSet,
			const FetchOption &fetchOption, bool isPartial, bool isTQL);

	void createResultSetOption(
			const OptionSet &optionSet,
			const int32_t *fetchBytesSize, bool partial,
			const PartialQueryOption &partialOption,
			ResultSetOption &queryOption);
	static void checkLogVersion(uint16_t logVersion);


	static FixedRequest::Source getRequestSource(const Event &ev);

	template<typename S>
	static void decodeRequestCommonPart(
			S &in, Request &request,
			ConnectionOption &connOption);
	template<typename S>
	static void decodeRequestOptionPart(
		S &in, Request &request,
			ConnectionOption &connOption);

	template<typename S>
	static void decodeOptionPart(
			S &in, OptionSet &optionSet);

	static bool isNewSQL(const Request &request);
	static bool isSkipReply(const Request &request);

	static CaseSensitivity getCaseSensitivity(const Request &request);
	static CaseSensitivity getCaseSensitivity(const OptionSet &optionSet);

	static CreateDropIndexMode getCreateDropIndexMode(
			const OptionSet &optionSet);
	static bool isDdlTransaction(const OptionSet &optionSet);

	static void assignIndexExtension(
			IndexInfo &indexInfo, const OptionSet &optionSet);
	static const char8_t* getExtensionName(const OptionSet &optionSet);

	template<typename S>
	static void decodeIndexInfo(
		S &in, IndexInfo &indexInfo);

	template<typename S>
	static void decodeMultipleRowData(S &in,
		uint64_t &numRow, RowData &rowData);
	template<typename S>
	static void decodeFetchOption(
		S &in, FetchOption &fetchOption);
	template<typename S>
	static void decodePartialQueryOption(
		S &in, util::StackAllocator &alloc,
		bool &isPartial, PartialQueryOption &partialQueryOption);

	template<typename S>
	static void decodeGeometryRelatedQuery(
		S &in, GeometryQuery &query);
	template<typename S>
	static void decodeGeometryWithExclusionQuery(
		S &in, GeometryQuery &query);
	template<typename S>
	static void decodeTimeRelatedCondition(S &in,
		TimeRelatedCondition &condition);
	template<typename S>
	static void decodeInterpolateCondition(S &in,
		InterpolateCondition &condition);
	template<typename S>
	static void decodeAggregateQuery(
		S &in, AggregateQuery &query);
	template<typename S>
	static void decodeRangeQuery(
		S &in, RangeQuery &query);
	template<typename S>
	static void decodeSamplingQuery(
		S &in, SamplingQuery &query);
	template <typename S>
	static void decodeContainerConditionData(S& in,
		KeyDataStore::ContainerCondition& containerCondition);
	template <typename S>
	static void decodeResultSetId(
		S &in, ResultSetId &resultSetId);

	template <typename S, typename IntType>
	static void decodeIntData(
		S &in, IntType &intData);
	template <typename S, typename LongType>
	static void decodeLongData(
		S&in, LongType &longData);
	template <typename S, typename StringType>
	static void decodeStringData(
		S&in, StringType &strData);
	template <typename S>
	static void decodeBooleanData(
		S&in, bool &boolData);
	template <typename S>
	static void decodeBinaryData(S&in,
		util::XArray<uint8_t> &binaryData, bool readAll);
	template <typename S>
	static void decodeVarSizeBinaryData(S&in,
		util::XArray<uint8_t> &binaryData);
	template <typename S, typename EnumType>
	static void decodeEnumData(
		S&in, EnumType &enumData);
	template <typename S>
	static void decodeUUID(S &in, uint8_t *uuid, size_t uuidSize);

	template<typename S>
	static void decodeReplicationAck(
		S &in, ReplicationAck &ack);
	template<typename S>
	static void decodeStoreMemoryAgingSwapRate(
		S &in,
		double &storeMemoryAgingSwapRate);
	template<typename S>
	static void decodeUserInfo(
		S &in, UserInfo &userInfo);
	template<typename S>
	static void decodeDatabaseInfo(S &in,
		DatabaseInfo &dbInfo, util::StackAllocator &alloc);

	static EventByteOutStream encodeCommonPart(
		Event &ev, StatementId stmtId, StatementExecStatus status);

	static void encodeIndexInfo(
		EventByteOutStream &out, const IndexInfo &indexInfo);
	static void encodeIndexInfo(
		util::XArrayByteOutStream &out, const IndexInfo &indexInfo);

	template <typename S, typename IntType>
	static void encodeIntData(S &out, IntType intData);
	template <typename S, typename LongType>
	static void encodeLongData(S &out, LongType longData);
	template<typename S, typename CharT, typename Traits, typename Alloc>
	static void encodeStringData(
			S &out,
			const std::basic_string<CharT, Traits, Alloc> &strData);
	template <typename S>
	static void encodeStringData(
		S& out, const char *strData);

	template <typename S>
	static void encodeBooleanData(S& out, bool boolData);
	template <typename S>
	static void encodeBinaryData(
		S& out, const uint8_t *data, size_t size);
	template <typename S, typename EnumType>
	static void encodeEnumData(S& out, EnumType enumData);
	template <typename S>
	static void encodeUUID(
		S& out, const uint8_t *uuid, size_t uuidSize);
	template <typename S>
	static void encodeVarSizeBinaryData(
		S& out, const uint8_t *data, size_t size);
	template <typename S>
	static void encodeContainerKey(S& out,
		const FullContainerKey &containerKey);

	template <typename ByteOutStream>
	static void encodeException(ByteOutStream &out,
		const std::exception &exception, bool detailsHidden,
		const ExceptionParameterList *paramList = NULL);
	template <typename ByteOutStream>
	static void encodeException(ByteOutStream &out,
		const std::exception &exception, const NodeDescriptor &nd);

	template <typename S>
	static void encodeReplicationAckPart(S &out,
		ClusterVersionId clusterVer, int32_t replMode, const ClientId &clientId,
		ReplicationId replId, EventType replStmtType, StatementId replStmtId,
		int32_t taskStatus);
	template <typename S>
	static int32_t encodeDistributedResult(
		S& out, const ResultSet &rs, int64_t *encodedSize);
	template <typename S>
	static void encodeStoreMemoryAgingSwapRate(
		S& out, double storeMemoryAgingSwapRate);
	template <typename S>
	static void encodeTimeZone(
		S& out, const util::TimeZone &timeZone);



	void replySuccess(
			EventContext &ec, util::StackAllocator &alloc,
			const NodeDescriptor &ND, EventType stmtType,
			StatementExecStatus status, const Request &request,
			const Response &response, bool ackWait);
	void replySuccess(
		EventContext& ec, util::StackAllocator& alloc,
		const NodeDescriptor& ND, EventType stmtType,
		const Request& request,
		SimpleOutputMessage& message, bool ackWait);
	void continueEvent(
			EventContext &ec, util::StackAllocator &alloc,
			const NodeDescriptor &ND, EventType stmtType, StatementId originalStmtId,
			const Request &request, const Response &response, bool ackWait);
	void continueEvent(
			EventContext &ec, util::StackAllocator &alloc,
			StatementExecStatus status, const ReplicationContext &replContext);
	void replySuccess(
			EventContext &ec, util::StackAllocator &alloc,
			StatementExecStatus status, const ReplicationContext &replContext);

	void replyError(
			EventContext &ec, util::StackAllocator &alloc,
			const NodeDescriptor &ND, EventType stmtType,
			StatementExecStatus status, const Request &request,
			const std::exception &e);

	bool executeReplication(
		const Request& request, EventContext& ec,
		util::StackAllocator& alloc, const NodeDescriptor& clientND,
		TransactionContext& txn, EventType replStmtType, StatementId replStmtId,
		int32_t replMode,
		const ClientId* closedResourceIds, size_t closedResourceIdCount,
		const util::XArray<uint8_t>** logRecordList, size_t logRecordCount,
		SimpleOutputMessage* mes) {
		return executeReplication(request, ec, alloc, clientND, txn, replStmtType,
			replStmtId, replMode, ReplicationContext::TASK_FINISHED,
			closedResourceIds, closedResourceIdCount,
			logRecordList, logRecordCount, replStmtId, 0, mes);
	}
	bool executeReplication(
		const Request& request, EventContext& ec,
		util::StackAllocator& alloc, const NodeDescriptor& clientND,
		TransactionContext& txn, EventType replStmtType, StatementId replStmtId,
		int32_t replMode, ReplicationContext::TaskStatus taskStatus,
		const ClientId* closedResourceIds, size_t closedResourceIdCount,
		const util::XArray<uint8_t>** logRecordList, size_t logRecordCount,
		StatementId originalStmtId, int32_t delayTime,
		SimpleOutputMessage* mes);
	void replyReplicationAck(
			EventContext &ec, util::StackAllocator &alloc,
			const NodeDescriptor &ND, const ReplicationAck &ack);

	void handleError(
			EventContext &ec, util::StackAllocator &alloc, Event &ev,
			const Request &request, std::exception &e);

	util::XArray<uint8_t>* abortOnError(TransactionContext &txn);

	static void checkLockConflictStatus(
			const LockConflictStatus &status, LockConflictException &e);
	static void retryLockConflictedRequest(
			EventContext &ec, Event &ev, const LockConflictStatus &status);
	static LockConflictStatus getLockConflictStatus(
			const Event &ev, EventMonotonicTime emNow, const Request &request);
	static LockConflictStatus getLockConflictStatus(
			const Event &ev, EventMonotonicTime emNow,
			const TransactionManager::ContextSource &cxtSrc,
			const OptionSet &optionSet);
	static void updateLockConflictStatus(
			EventContext &ec, Event &ev, const LockConflictStatus &status);

	template<OptionType T>
	static void updateRequestOption(
			util::StackAllocator &alloc, Event &ev,
			const typename Message::OptionCoder<T>::ValueType &value);

	bool checkPrivilege(
			EventType command,
			UserType userType, RequestType requestType, bool isSystemMode,
			int32_t featureVersion,
			ContainerAttribute resourceSubType,
			ContainerAttribute expectedResourceSubType = CONTAINER_ATTR_ANY);

	bool isMetaContainerVisible(
			const MetaContainer &metaContainer, int32_t visibility);

	static bool isSupportedContainerAttribute(ContainerAttribute attribute);


	static void checkDbAccessible(
			const char8_t *loginDbName, const char8_t *specifiedDbName
			, bool isNewSql
	);

	static const char8_t *clusterRoleToStr(ClusterRole role);
	static const char8_t *partitionRoleTypeToStr(PartitionRoleType role);
	static const char8_t *partitionStatusToStr(PartitionStatus status);

	static bool getApplicationNameByOptionsOrND(
			const OptionSet *optionSet, const NodeDescriptor *nd,
			util::String *nameStr, std::ostream *os);

	util::XArray<uint8_t>* appendDataStoreLog(
		util::StackAllocator& logAlloc,
		TransactionContext& txn,
		const TransactionManager::ContextSource& cxtSrc, StatementId stmtId,
		StoreType storeType, DataStoreLogV4* dsMes);
	util::XArray<uint8_t>* appendDataStoreLog(
		util::StackAllocator& logAlloc,
		TransactionContext& txn,
		const ClientId& clientId,
		const TransactionManager::ContextSource& cxtSrc, StatementId stmtId,
		StoreType storeType, DataStoreLogV4* dsMes);

	bool checkConstraint(Event& ev, ConnectionOption& connOption, bool isUpdated = false);

protected:
	const KeyConstraint& getKeyConstraint(
			const OptionSet &optionSet, bool checkLength = true) const;
	const KeyConstraint& getKeyConstraint(
			ContainerAttribute containerAttribute, bool checkLength = true) const;
	DataStoreV4* getDataStore(PartitionId pId);
	DataStoreBase* getDataStore(PartitionId pId, StoreType type);
	DataStoreBase* getDataStore(PartitionId pId, KeyDataStoreValue *keyStoreVal);
	KeyDataStore* getKeyDataStore(PartitionId pId);
	LogManager<MutexLocker>* getLogManager(PartitionId pId);

	KeyConstraint keyConstraint_[2][2][2];
};

template <typename S, typename IntType>
void StatementHandler::decodeIntData(
	S &in, IntType &intData) {
	try {
		in >> intData;
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

template <typename S, typename LongType>
void StatementHandler::decodeLongData(
	S &in, LongType &longData) {
	try {
		in >> longData;
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

template <typename S, typename StringType>
void StatementHandler::decodeStringData(
	S &in, StringType &strData) {
	try {
		in >> strData;
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

template <typename S, typename EnumType>
void StatementHandler::decodeEnumData(
	S &in, EnumType &enumData) {
	try {
		int8_t tmp;
		in >> tmp;
		enumData = static_cast<EnumType>(tmp);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

template <typename S, typename IntType>
void StatementHandler::encodeIntData(S &out, IntType intData) {
	try {
		out << intData;
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

template <typename S, typename LongType>
void StatementHandler::encodeLongData(
	S& out, LongType longData) {
	try {
		out << longData;
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

template<typename S, typename CharT, typename Traits, typename Alloc>
void StatementHandler::encodeStringData(
		S &out,
		const std::basic_string<CharT, Traits, Alloc> &strData) {
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

/*!
	@brief Encodes const string data
*/
template<typename S>
void StatementHandler::encodeStringData(
	S& out, const char* str) {
	try {
		size_t size = strlen(str);
		out << static_cast<uint32_t>(size);
		out << std::pair<const uint8_t*, size_t>(
			reinterpret_cast<const uint8_t*>(str), size);
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief Encodes boolean data
*/
template<typename S>
void StatementHandler::encodeBooleanData(
	S& out, bool boolData) {
	try {
		const uint8_t tmp = boolData ? 1 : 0;
		out << tmp;
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

template <typename S, typename EnumType>
void StatementHandler::encodeEnumData(
	S& out, EnumType enumData) {
	try {
		const uint8_t tmp = static_cast<uint8_t>(enumData);
		out << tmp;
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief Encodes UUID
*/
template<typename S>
void StatementHandler::encodeUUID(
	S& out, const uint8_t* uuid, size_t uuidSize) {
	try {
		assert(uuid != NULL);
		out << std::pair<const uint8_t*, size_t>(uuid, uuidSize);
	}
	catch (std::exception& e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

inline std::ostream& operator<<(
		std::ostream &os, const StatementHandler::ErrorMessage &errorMessage) {
	errorMessage.format(os);
	return os;
}

/*!
	@brief Handles CONNECT statement
*/
class ConnectHandler : public StatementHandler {
public:
	ConnectHandler(ProtocolVersion currentVersion,
		const ProtocolVersion *acceptableProtocolVersions);

	void operator()(EventContext &ec, Event &ev);

private:
	typedef uint32_t OldStatementId;  
	static const OldStatementId UNDEF_OLD_STATEMENTID = UINT32_MAX;

	const ProtocolVersion currentVersion_;
	const ProtocolVersion *acceptableProtocolVersions_;

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

	struct InMessage : public Serializable {
		InMessage(PartitionId pId, EventType stmtType)
			: Serializable(NULL), request_(pId, stmtType) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			decodeIntData<S, OldStatementId>(in, request_.oldStmtId_);
			decodeIntData<S, ProtocolVersion>(in, request_.clientVersion_);
		}
		ConnectRequest request_;
	};

	struct OutMessage : public Serializable {
		OutMessage(OldStatementId stmtId, StatementExecStatus status, int8_t authMode, ProtocolVersion version, bool isAuthType)
			: Serializable(NULL), stmtId_(stmtId), status_(status), authMode_(authMode), version_(version), isAuthType_(isAuthType) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
	}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			out << stmtId_;
			out << status_;
			encodeIntData<S, int8_t>(out, authMode_);
			encodeIntData<S>(out, version_);
			int8_t authType = (isAuthType_ ? 1 : 0);
			encodeBooleanData<S>(out, authType);
		}
		template<typename S>
		void decode(S& in) {
			UNUSED_VARIABLE(in);
		}

		OldStatementId stmtId_;
		StatementExecStatus status_;
		int8_t authMode_;
		ProtocolVersion version_;
		bool isAuthType_;
	};

	void checkClientVersion(ProtocolVersion clientVersion);

	void replySuccess(EventContext& ec, util::StackAllocator& alloc,
		const NodeDescriptor& ND, EventType stmtType,
		const ConnectRequest& request, Serializable& message, bool);
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
	@brief Handles GET_PARTITION_ADDRESS statement
*/
class GetPartitionAddressHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

	template <typename S>
	static void encodeClusterInfo(
			S &out, EventEngine &ee,
			PartitionTable &partitionTable, ContainerHashMode hashMode, ServiceType serviceType, bool usePublic);

private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), masterResolving_(false) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			if (in.base().remaining() > 0) {
				decodeBooleanData<S>(in, masterResolving_);
			}
		}
		bool masterResolving_;
	};

	struct OutMessage : public SimpleOutputMessage {
		OutMessage(StatementId stmtId, StatementExecStatus status,
			OptionSet* optionSet, util::XArray<uint8_t>* binary)
			: SimpleOutputMessage(stmtId, status, optionSet), binary_(binary) {}
		void encode(EventByteOutStream& out) {
			SimpleOutputMessage::encode(out);
			encodeBinaryData<EventByteOutStream>(
				out, binary_->data(), binary_->size());

		}
		void decode(EventByteInStream& in) {
			UNUSED_VARIABLE(in);
		}
		void encode(OutStream& out) {
			SimpleOutputMessage::encode(out);
		}

		util::XArray<uint8_t> *binary_;
	};
};

/*!
	@brief Handles GET_PARTITION_CONTAINER_NAMES statement
*/
class GetPartitionContainerNamesHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), start_(0),
			limit_(0), containerCondition_(alloc) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeLongData<S, int64_t>(in, start_);
			decodeLongData<S, ResultSize>(in, limit_);
			decodeContainerConditionData<S>(in, containerCondition_);
		}
		int64_t start_;
		ResultSize limit_;
		KeyDataStore::ContainerCondition containerCondition_;
	};

	struct OutMessage : public SimpleOutputMessage {
		OutMessage(StatementId stmtId, StatementExecStatus status,
			OptionSet* optionSet, uint64_t containerNum,
			util::XArray<FullContainerKey>& containerNameList)
			: SimpleOutputMessage(stmtId, status, optionSet), 
			containerNum_(containerNum), containerNameList_(containerNameList) {}
		void encode(EventByteOutStream& out) {
			SimpleOutputMessage::encode(out);
			out << containerNum_;
			out << static_cast<uint32_t>(containerNameList_.size());
			for (size_t i = 0; i < containerNameList_.size(); i++) {
				encodeContainerKey(out, containerNameList_[i]);
			}
		}
		void decode(EventByteInStream& in) {
			UNUSED_VARIABLE(in);
		}
		void encode(OutStream& out) {
			SimpleOutputMessage::encode(out);
		}

		uint64_t containerNum_;
		util::XArray<FullContainerKey>& containerNameList_;
	};
};

/*!
	@brief Handles GET_CONTAINER_PROPERTIES statement
*/
class GetContainerPropertiesHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

	template<typename C>
	void encodeContainerProps(
			TransactionContext &txn, EventByteOutStream &out, C *container,
			uint32_t propFlags, uint32_t propTypeCount, bool forMeta,
			EventMonotonicTime emNow, util::String &containerNameStr,
			const char8_t *dbName
			, int64_t currentTime
			, int32_t acceptableFeatureVersion
			);

	static void getPartitioningTableIndexInfo(
		TransactionContext &txn, EventMonotonicTime emNow, DataStoreV4 &dataStore,
		BaseContainer &largeContainer, const char8_t *containerName,
		const char *dbName, util::Vector<IndexInfo> &indexInfoList);

private:
	typedef util::XArray<util::XArray<uint8_t> *> ContainerNameList;

	enum ContainerProperty {
		CONTAINER_PROPERTY_ID,
		CONTAINER_PROPERTY_SCHEMA,
		CONTAINER_PROPERTY_INDEX,
		CONTAINER_PROPERTY_EVENT_NOTIFICATION,
		CONTAINER_PROPERTY_TRIGGER,
		CONTAINER_PROPERTY_ATTRIBUTES,
		CONTAINER_PROPERTY_INDEX_DETAIL,
		CONTAINER_PROPERTY_NULLS_STATISTICS,
		CONTAINER_PROPERTY_PARTITIONING_METADATA = 16
	};

	enum MetaDistributionType {
		META_DIST_NONE,
		META_DIST_FULL,
		META_DIST_NODE
	};

	enum PartitioningContainerOption {
		PARTITIONING_PROPERTY_AFFINITY_ = 0 
	};

	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), alloc_(alloc), containerNameList_(alloc), propFlags_(0) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			for (uint32_t remaining = 1; remaining > 0; remaining--) {
				util::XArray<uint8_t>* name = ALLOC_NEW(alloc_) util::XArray<uint8_t>(alloc_);
				decodeVarSizeBinaryData<S>(in, *name);
				containerNameList_.push_back(name);

				if (containerNameList_.size() == 1) {
					uint32_t propTypeCount;
					in >> propTypeCount;

					for (uint32_t i = 0; i < propTypeCount; i++) {
						uint8_t propType;
						in >> propType;
						propFlags_ |= (1 << propType);
					}

					if (in.base().remaining() > 0) {
						in >> remaining;
					}
				}
			}
		}
		util::StackAllocator& alloc_;
		ContainerNameList containerNameList_;
		uint32_t propFlags_;
	};


	void encodeResultListHead(EventByteOutStream &out, uint32_t totalCount);
	void encodePropsHead(EventByteOutStream &out, uint32_t propTypeCount);
	void encodeId(
			EventByteOutStream &out, SchemaVersionId schemaVersionId,
			ContainerId containerId, const FullContainerKey &containerKey,
			ContainerId metaContainerId, MetaDistributionType metaDistType,
			int8_t metaNamingType);
	void encodeMetaId(
			EventByteOutStream &out, ContainerId metaContainerId,
			MetaDistributionType metaDistType, int8_t metaNamingType);
	void encodeSchema(EventByteOutStream &out, ContainerType containerType,
			const util::XArray<uint8_t> &serializedCollectionInfo);
	void encodeIndex(
			EventByteOutStream &out, const util::Vector<IndexInfo> &indexInfoList);
	void encodeEventNotification(EventByteOutStream &out,
			const util::XArray<char *> &urlList,
			const util::XArray<uint32_t> &urlLenList);
	void encodeTrigger(EventByteOutStream &out,
			const util::XArray<const uint8_t *> &triggerList);
	void encodeAttributes(
			EventByteOutStream &out, const ContainerAttribute containerAttribute);
	void encodeIndexDetail(
			EventByteOutStream &out, const util::Vector<IndexInfo> &indexInfoList);
	void encodePartitioningMetaData(
			EventByteOutStream &out,
			TransactionContext &txn, EventMonotonicTime emNow,
			BaseContainer &largeContainer,  ContainerAttribute attribute,
			const char *dbName, const char8_t *containerName
			, int64_t currentTime, int32_t acceptableFeatureVersion
			);
	void encodeNulls(
			EventByteOutStream &out,
			const util::XArray<uint8_t> &nullsList);

	static void checkIndexInfoVersion(
			const util::Vector<IndexInfo> indexInfoList,
			int32_t acceptableFeatureVersion);
	static void checkIndexInfoVersion(
			const IndexInfo &info, int32_t acceptableFeatureVersion);
};

/*!
	@brief Handles PUT_CONTAINER statement
*/
class PutContainerHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), containerNameBinary_(alloc),
			containerInfo_(alloc), containerType_(UNDEF_CONTAINER), modifiable_(false),
			featureVersion_(MessageSchema::DEFAULT_VERSION),
			storeType_(V4_COMPATIBLE) {
		}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeVarSizeBinaryData<S>(in, containerNameBinary_);
			decodeEnumData<S, ContainerType>(in, containerType_);
			decodeBooleanData<S>(in, modifiable_);
			decodeBinaryData<S>(in, containerInfo_, true);
			if (request_.optional_.get<Options::CONTAINER_ATTRIBUTE>() >= CONTAINER_ATTR_ANY) {
				request_.optional_.set<Options::CONTAINER_ATTRIBUTE>(CONTAINER_ATTR_SINGLE);
			}
			isCaseSensitive_ = getCaseSensitivity(request_).isContainerNameCaseSensitive();
			featureVersion_ = request_.optional_.get<Options::FEATURE_VERSION>();
			if (featureVersion_ == MessageSchema::DEFAULT_VERSION) {
				util::XArrayOutStream<> arrayOut(containerInfo_);
				util::ByteStream<util::XArrayOutStream<> > out(arrayOut);
				int32_t containerAttribute =
					request_.optional_.get<Options::CONTAINER_ATTRIBUTE>();
				out << containerAttribute;
			}
		}
		util::XArray<uint8_t> containerNameBinary_;
		util::XArray<uint8_t> containerInfo_;
		ContainerType containerType_;
		bool modifiable_;
		bool isCaseSensitive_;
		int32_t featureVersion_;
		StoreType storeType_;
	};

	typedef DSMessage OutMessage;

	void setContainerAttributeForCreate(OptionSet &optionSet);
};

class PutLargeContainerHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), alloc_(alloc), containerNameBinary_(alloc),
			containerInfo_(alloc), containerType_(UNDEF_CONTAINER), modifiable_(false),
			storeType_(V4_COMPATIBLE),
			normalContainerInfo_(alloc), partitioningInfo_(NULL) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in);
		util::StackAllocator& alloc_;

		util::XArray<uint8_t> containerNameBinary_;
		util::XArray<uint8_t> containerInfo_;
		ContainerType containerType_;
		bool modifiable_;
		bool isCaseSensitive_;
		int32_t featureVersion_;
		StoreType storeType_;

		util::XArray<uint8_t> normalContainerInfo_;
		TablePartitioningInfo<util::StackAllocator> *partitioningInfo_;
	};
	typedef DSMessage OutMessage;

	void setContainerAttributeForCreate(OptionSet &optionSet);
};

class UpdateContainerStatusHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
	void decode(EventByteInStream &in, Request &request,
		ConnectionOption &conn,
		util::XArray<uint8_t> &containerNameBinary,
		ContainerCategory &category, NodeAffinityNumber &affinity,
		TablePartitioningVersionId &versionId,
		ContainerId &largeContainerId, LargeContainerStatusType &status, IndexInfo &indexInfo);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), containerNameBinary_(alloc),
			category_(UNDEF_LARGE_CONTAINER_CATEGORY), affinity_(UNDEF_NODE_AFFINITY_NUMBER),
			versionId_(UNDEF_TABLE_PARTITIONING_VERSIONID), largeContainerId_(UNDEF_CONTAINERID),
			status_(PARTITION_STATUS_NONE), indexInfo_(alloc) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeVarSizeBinaryData<S>(in, containerNameBinary_);
			in >> category_;
			int32_t dataSize = 0; 
			in >> dataSize;
			if (category_ == TABLE_PARTITIONING_CHECK_EXPIRED) {
				largeContainerId_ = UNDEF_CONTAINERID;
			}
			else if (category_ == TABLE_PARTITIONING_VERSION) {
				in >> versionId_;
			}
			else {
				in >> largeContainerId_;
				in >> affinity_;
				if (category_ != TABLE_PARTITIONING_RECOVERY) {
					in >> status_;
				}

				if (in.base().remaining() != 0
					&& category_ != TABLE_PARTITIONING_RECOVERY) {

					if (status_ != PARTITION_STATUS_NONE) {
						decodeIndexInfo<S>(in, indexInfo_);
					}
					else {
						if (isNewSQL(request_)) {
							GS_THROW_USER_ERROR(
								GS_ERROR_SQL_TABLE_PARTITION_INVALID_MESSAGE,
								"Invalid message from sql server, affinity=" << affinity_);
						}
						else {
							GS_THROW_USER_ERROR(
								GS_ERROR_SQL_TABLE_PARTITION_INVALID_MESSAGE,
								"Invalid message from java client, affinity=" << affinity_);
						}
					}
				}
			}
		}
		util::XArray<uint8_t> containerNameBinary_;
		ContainerCategory category_;
		NodeAffinityNumber affinity_;
		TablePartitioningVersionId versionId_;
		ContainerId largeContainerId_;
		LargeContainerStatusType status_;
		IndexInfo indexInfo_;
	};

	struct OutMessage : public SimpleOutputMessage {
		OutMessage(StatementId stmtId, StatementExecStatus status,
			OptionSet* optionSet, NodeAffinityNumber affinity, 
			LargeContainerStatusType largeContainerStatus, IndexInfo *indexInfo)
			: SimpleOutputMessage(stmtId, status, optionSet),
			currentAffinity_(affinity), largeContainerStatus_(largeContainerStatus),
			indexInfo_(indexInfo) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			SimpleOutputMessage::encode(out);
			out << largeContainerStatus_;
			out << currentAffinity_;
			if (!indexInfo_->indexName_.empty()) {
				encodeIndexInfo(out, *indexInfo_);
			}
		}
		template<typename S>
		void decode(S& in) {
			UNUSED_VARIABLE(in);
		}

		NodeAffinityNumber currentAffinity_;
		LargeContainerStatusType largeContainerStatus_;
		IndexInfo* indexInfo_;
	};
};

class CreateLargeIndexHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
	bool checkCreateIndex(PartitionId pId, IndexInfo& indexInfo,
		ColumnType targetColumnType, ContainerType targetContainerType);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), alloc_(alloc), indexInfo_(alloc),
			indexColumnNameStr_(alloc), indexColumnNameList_(alloc), isComposite_(false) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeIndexInfo<S>(in, indexInfo_);
			assignIndexExtension(indexInfo_, request_.optional_);
			util::Vector<ColumnId>& indexColumnIdList
				= indexInfo_.columnIds_;

			decodeStringData<S, util::String>(in, indexColumnNameStr_);
			indexColumnNameList_.push_back(indexColumnNameStr_);

			if (indexColumnIdList.size() > 1) {
				if (in.base().remaining() == 0) {

					GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_INTERNAL, "");
				}

				isComposite_ = true;

				for (size_t pos = 1;
					pos < indexColumnIdList.size(); pos++) {

					util::String indexColumnNameStr(alloc_);
					decodeStringData<S, util::String>(in, indexColumnNameStr);

					indexColumnNameList_.push_back(indexColumnNameStr);
				}
			}
		}
		util::StackAllocator& alloc_;
		IndexInfo indexInfo_;
		util::String indexColumnNameStr_;
		util::Vector<util::String> indexColumnNameList_;
		bool isComposite_;
	};
	struct OutMessage : public SimpleOutputMessage {
		OutMessage(StatementId stmtId, StatementExecStatus status,
			OptionSet* optionSet, uint8_t exist)
			: SimpleOutputMessage(stmtId, status, optionSet), exist_(exist) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			SimpleOutputMessage::encode(out);
			out << exist_;
		}
		template<typename S>
		void decode(S& in) {
			UNUSED_VARIABLE(in);
		}
		uint8_t exist_;
	};
};

/*!
	@brief Handles DROP_CONTAINER statement
*/
class DropContainerHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), containerNameBinary_(alloc),
			containerType_(UNDEF_CONTAINER) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeVarSizeBinaryData<S>(in, containerNameBinary_);
			decodeEnumData<S, ContainerType>(in, containerType_);
			isCaseSensitive_ = getCaseSensitivity(request_).isContainerNameCaseSensitive();
		}
		util::XArray<uint8_t> containerNameBinary_;
		ContainerType containerType_;
		bool isCaseSensitive_;
	};

	typedef DSMessage OutMessage;
};
/*!
	@brief Handles GET_CONTAINER statement
*/
class GetContainerHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), containerNameBinary_(alloc),
			containerType_(UNDEF_CONTAINER) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeVarSizeBinaryData<S>(in, containerNameBinary_);
			decodeEnumData<S, ContainerType>(in, containerType_);
		}
		util::XArray<uint8_t> containerNameBinary_;
		ContainerType containerType_;
	};

	typedef DSMessage OutMessage;
};

/*!
	@brief Handles CREATE_DROP_INDEX statement
*/
class CreateDropIndexHandler : public StatementHandler {
public:

protected:
	bool compareIndexName(util::StackAllocator &alloc,
		const util::String &specifiedName, const util::String &existingName,
		bool isCaseSensitive);
};

/*!
	@brief Handles CREATE_DROP_INDEX statement
*/
class CreateIndexHandler : public CreateDropIndexHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), indexInfo_(alloc) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeIndexInfo<S>(in, indexInfo_);
			isCaseSensitive_ = getCaseSensitivity(request_).isIndexNameCaseSensitive();
			mode_ = getCreateDropIndexMode(request_.optional_);
		}
		IndexInfo indexInfo_;
		bool isCaseSensitive_;
		CreateDropIndexMode mode_;
	};

	typedef DSMessage OutMessage;
};
/*!
	@brief Handles CREATE_DROP_INDEX statement
*/
class DropIndexHandler : public CreateDropIndexHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), indexInfo_(alloc) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeIndexInfo<S>(in, indexInfo_);
			decodeIntData<S, uint8_t>(in, indexInfo_.anyNameMatches_);
			decodeIntData<S, uint8_t>(in, indexInfo_.anyTypeMatches_);
			isCaseSensitive_ = getCaseSensitivity(request_).isIndexNameCaseSensitive();
			mode_ = getCreateDropIndexMode(request_.optional_);
		}
		IndexInfo indexInfo_;
		bool isCaseSensitive_;
		CreateDropIndexMode mode_;
	};
	typedef DSMessage OutMessage;
};

/*!
	@brief Handles DROP_TRIGGER statement
*/
class CreateDropTriggerHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	typedef SimpleInputMessage InMessage;
};
/*!
	@brief Handles FLUSH_LOG statement
*/
class FlushLogHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	typedef SimpleInputMessage InMessage;
	typedef SimpleOutputMessage OutMessage;
};
/*!
	@brief Handles CREATE_TRANSACTION_CONTEXT statement
*/
class CreateTransactionContextHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	typedef SimpleInputMessage InMessage;
	typedef SimpleOutputMessage OutMessage;
};
/*!
	@brief Handles CLOSE_TRANSACTION_CONTEXT statement
*/
class CloseTransactionContextHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	typedef SimpleInputMessage InMessage;
	typedef SimpleOutputMessage OutMessage;
};

/*!
	@brief Handles COMMIT_TRANSACTION statement
*/
class CommitAbortTransactionHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	typedef SimpleInputMessage InMessage;
	typedef SimpleOutputMessage OutMessage;
};

/*!
	@brief Handles PUT_ROW statement
*/
class PutRowHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), rowData_(alloc), putRowOption_(PUT_INSERT_OR_UPDATE) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			StatementHandler::decodeBinaryData(in, rowData_, true);
			putRowOption_ =
				request_.optional_.get<Options::PUT_ROW_OPTION>();
		}

		RowData rowData_;
		PutRowOption putRowOption_;
	};
	typedef DSMessage OutMessage;
};
/*!
	@brief Handles PUT_ROW_SET statement
*/
class PutRowSetHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), rowData_(alloc), numRow_(0), putRowOption_(PUT_INSERT_OR_UPDATE) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			in >> numRow_;
			StatementHandler::decodeBinaryData(in, rowData_, true);
			putRowOption_ =
				request_.optional_.get<Options::PUT_ROW_OPTION>();
		}

		RowData rowData_;
		uint64_t numRow_;
		PutRowOption putRowOption_;
	};

	typedef RowExistMessage OutMessage;
};
/*!
	@brief Handles REMOVE_ROW statement
*/
class RemoveRowHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	typedef KeyInputMessage InMessage;
	typedef DSMessage OutMessage;
};
/*!
	@brief Handles UPDATE_ROW_BY_ID statement
*/
class UpdateRowByIdHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), rowId_(UNDEF_ROWID), rowData_(alloc) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeLongData<S, RowId>(in, rowId_);
			StatementHandler::decodeBinaryData(in, rowData_, true);
		}

		RowId rowId_;
		RowData rowData_;
	};

	typedef SimpleOutputMessage OutMessage;
};
/*!
	@brief Handles REMOVE_ROW_BY_ID statement
*/
class RemoveRowByIdHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), rowId_(UNDEF_ROWID) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeLongData<S, RowId>(in, rowId_);
		}

		RowId rowId_;
	};

	typedef SimpleOutputMessage OutMessage;
};

/*!
	@brief Handles GET_ROW statement
*/
class GetRowHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	typedef KeyInputMessage InMessage;
	typedef DSMessage OutMessage;
};
/*!
	@brief Handles GET_ROW_SET statement
*/
class GetRowSetHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public QueryInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: QueryInputMessage(alloc, src, connOption), rowId_(UNDEF_ROWID) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			QueryInputMessage::decode(in);
			decodeLongData<S, RowId>(in, rowId_);
		}
		RowId rowId_;
	};
	struct OutMessage : public SimpleOutputMessage {
		OutMessage(StatementId stmtId, StatementExecStatus status,
			OptionSet* optionSet, ResultSet* rs,
			Event& ev, RowId rowId)
			: SimpleOutputMessage(stmtId, status, optionSet), rs_(rs),
			ev_(ev), rowId_(rowId) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out);
		template<typename S>
		void decode(S& in) {
			UNUSED_VARIABLE(in);
		}
		void addExtraMessage(Event* event);
		void addExtraMessage(ReplicationContext* replicationContext);

		ResultSet* rs_;
		Event& ev_;
		RowId rowId_;
	};
};
/*!
	@brief Handles QUERY_TQL statement
*/
class QueryTqlHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public QueryInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: QueryInputMessage(alloc, src, connOption), query_(alloc) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			QueryInputMessage::decode(in);
			decodeStringData<S, util::String>(in, query_);
		}
		util::String query_;
	};

	static void suspendRequest(
			EventContext& ec, const Event &baseEv,
			const util::XArray<uint8_t> &suspendedData, size_t extraDataSize);
	static void decodeSuspendedData(
			const Event &ev, const OptionSet &optionSet, EventByteInStream &in,
			util::XArray<uint8_t> &suspendedData, size_t &extraDataSize);

	typedef TQLOutputMessage OutMessage;
};

/*!
	@brief Handles APPEND_ROW statement
*/
class AppendRowHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), rowData_(alloc) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			StatementHandler::decodeBinaryData(in, rowData_, true);
		}

		RowData rowData_;
	};
	typedef DSMessage OutMessage;
};

/*!
	@brief Handles QUERY_GEOMETRY_RELATED statement
*/
class QueryGeometryRelatedHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public QueryInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: QueryInputMessage(alloc, src, connOption), query_(alloc) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			QueryInputMessage::decode(in);
			decodeGeometryRelatedQuery<S>(in, query_);
		}
		GeometryQuery query_;
	};
	typedef TQLOutputMessage OutMessage;
};
/*!
	@brief Handles QUERY_GEOMETRY_WITH_EXCLUSION statement
*/
class QueryGeometryWithExclusionHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public QueryInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: QueryInputMessage(alloc, src, connOption), query_(alloc) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			QueryInputMessage::decode(in);
			decodeGeometryWithExclusionQuery<S>(in, query_);
		}
		GeometryQuery query_;
	};

	typedef TQLOutputMessage OutMessage;
};
/*!
	@brief Handles GET_ROW_TIME_RELATED statement
*/
class GetRowTimeRelatedHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeTimeRelatedCondition<S>(in, condition_);
		}

		TimeRelatedCondition condition_;
	};

	typedef DSMessage OutMessage;
};
/*!
	@brief Handles ROW_INTERPOLATE statement
*/
class GetRowInterpolateHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeInterpolateCondition<S>(in, condition_);
		}

		InterpolateCondition condition_;
	};
	typedef DSMessage OutMessage;
};
/*!
	@brief Handles AGGREGATE statement
*/
class AggregateHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeAggregateQuery<S>(in, query_);
		}

		AggregateQuery query_;
	};
	typedef DSMessage OutMessage;
};
/*!
	@brief Handles QUERY_TIME_RANGE statement
*/
class QueryTimeRangeHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public QueryInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: QueryInputMessage(alloc, src, connOption) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			QueryInputMessage::decode(in);
			decodeRangeQuery<S>(in, query_);
		}
		RangeQuery query_;
	};

	typedef TQLOutputMessage OutMessage;
};
/*!
	@brief Handles QUERY_TIME_SAMPLING statement
*/
class QueryTimeSamplingHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public QueryInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: QueryInputMessage(alloc, src, connOption), query_(alloc) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			QueryInputMessage::decode(in);
			decodeSamplingQuery<S>(in, query_);
		}
		SamplingQuery query_;
	};


	typedef TQLOutputMessage OutMessage;
};

/*!
	@brief Handles FETCH_RESULT_SET statement
*/
class FetchResultSetHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), rsId_(0), startPos_(0), fetchNum_(0) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeLongData<S, ResultSetId>(in, rsId_);
			decodeLongData<S, ResultSize>(in, startPos_);
			decodeLongData<S, ResultSize>(in, fetchNum_);
		}

		ResultSetId rsId_;
		ResultSize startPos_;
		ResultSize fetchNum_;
	};
	struct OutMessage : public SimpleOutputMessage {
		OutMessage(StatementId stmtId, StatementExecStatus status,
			OptionSet* optionSet, ResultSet* rs,
			Event& ev)
			: SimpleOutputMessage(stmtId, status, optionSet), rs_(rs),
			ev_(ev) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out);
		template<typename S>
		void decode(S& in) {
			UNUSED_VARIABLE(in);
		}
		void addExtraMessage(Event* event);
		void addExtraMessage(ReplicationContext* replicationContext);

		ResultSet* rs_;
		Event& ev_;
	};

};
/*!
	@brief Handles CLOSE_RESULT_SET statement
*/
class CloseResultSetHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), rsId_(0) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeLongData<S, ResultSetId>(in, rsId_);
		}

		ResultSetId rsId_;
	};
	typedef DSMessage OutMessage;
};

/*!
	@brief Handles MULTI_CREATE_TRANSACTION_CONTEXT statement
*/
class MultiCreateTransactionContextHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

private:
	struct SessionCreationEntry {
		SessionCreationEntry() :
			containerAttribute_(CONTAINER_ATTR_ANY),
			containerName_(NULL),
			containerId_(UNDEF_CONTAINERID),
			sessionId_(UNDEF_SESSIONID) {}

		ContainerAttribute containerAttribute_;
		util::XArray<uint8_t> *containerName_;
		ContainerId containerId_;
		SessionId sessionId_;
	};

	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), entryList_(alloc), withId_(false) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			withId_ = decodeMultiTransactionCreationEntry(in, entryList_);
		}

		util::XArray<SessionCreationEntry> entryList_;
		bool withId_;
	};
	struct OutMessage : public Serializable {
		OutMessage(StatementId stmtId, StatementExecStatus status, util::XArray<ContainerId> *list)
			: Serializable(NULL), header_(stmtId, status), list_(list) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			header_.encode(out);
			encodeIntData<S, uint32_t>(out, static_cast<uint32_t>(list_->size()));
			util::XArray<ContainerId>::iterator itr;
			for (itr = list_->begin(); itr != list_->end(); itr++) {
				encodeLongData<S, ContainerId>(out, *itr);
			}
		}
		template<typename S>
		void decode(S& in) {
			UNUSED_VARIABLE(in);
		}

		CommonMessageHeader header_;
		util::XArray<ContainerId> *list_;
	};

	template<typename S>
	static bool decodeMultiTransactionCreationEntry(
		S &in,
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
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), entryList_(alloc) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeMultiTransactionCloseEntry(in, entryList_);
		}

		util::XArray<SessionCloseEntry> entryList_;
	};
	typedef SimpleOutputMessage OutMessage;

	template<typename S>
	static void decodeMultiTransactionCloseEntry(
		S &in,
		util::XArray<SessionCloseEntry> &entryList);
};

/*!
	@brief Handles multi-type statement
*/
class MultiStatementHandler : public StatementHandler {
public:
	struct MultiOpErrorMessage;

TXN_PROTECTED :
	enum ContainerResult {
		CONTAINER_RESULT_SUCCESS,
		CONTAINER_RESULT_ALREADY_EXECUTED,
		CONTAINER_RESULT_FAIL
	};

	struct Progress {
		util::XArray<ContainerResult> containerResult_;
		util::XArray<uint8_t> lastExceptionData_;
		TxnLogManager &txnLogMgr_;
		LockConflictStatus lockConflictStatus_;
		bool lockConflicted_;
		StatementId mputStartStmtId_;
		uint64_t totalRowCount_;

		Progress(
				util::StackAllocator &alloc,
				const LockConflictStatus &lockConflictStatus,
				TxnLogManager& txnLogMgr) :
				containerResult_(alloc),
				lastExceptionData_(alloc),
				txnLogMgr_(txnLogMgr),
				lockConflictStatus_(lockConflictStatus),
				lockConflicted_(false),
				mputStartStmtId_(UNDEF_STATEMENTID),
				totalRowCount_(0) {
		}
	};

	void handleExecuteError(
			util::StackAllocator &alloc,
			const Event &ev, const Request &wholeRequest,
			EventType stmtType, PartitionId pId, const ClientId &clientId,
			const TransactionManager::ContextSource &src, Progress &progress,
			std::exception &e, const char8_t *executionName);

	void handleWholeError(
			EventContext &ec, util::StackAllocator &alloc,
			const Event &ev, const Request &request, std::exception &e);

	static void decodeContainerOptionPart(
			EventByteInStream &in, const FixedRequest &fixedRequest,
			OptionSet &optionSet);
};

struct MultiStatementHandler::MultiOpErrorMessage {
	MultiOpErrorMessage(
			std::exception &cause, const char8_t *description,
			const Event &ev, const Request &wholeRequest,
			EventType stmtType, PartitionId pId, const ClientId &clientId,
			const TransactionManager::ContextSource &cxtSrc,
			const char8_t *containerName, const char8_t *executionName) :
			base_(cause, description, ev, wholeRequest),
			containerName_(containerName),
			executionName_(executionName) {
		base_.elements_.stmtType_ = stmtType;
		base_.elements_.pId_ = pId;
		base_.elements_.stmtId_ = UNDEF_STATEMENTID;
		base_.elements_.clientId_ = clientId;
		base_.elements_.containerId_ = cxtSrc.containerId_;
	}

	MultiOpErrorMessage withDescription(const char8_t *description) const {
		MultiOpErrorMessage org = *this;
		org.base_.elements_.description_ = description;
		return org;
	}

	void format(std::ostream &os) const;

	ErrorMessage base_;
	const char8_t *containerName_;
	const char8_t *executionName_;
};

inline std::ostream& operator<<(
		std::ostream &os,
		const MultiStatementHandler::MultiOpErrorMessage &errorMessage) {
	errorMessage.format(os);
	return os;
}

/*!
	@brief Handles MULTI_PUT statement
*/
class MultiPutHandler : public MultiStatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);

private:
	typedef SimpleOutputMessage OutMessage;

	struct RowSetRequest {
		StatementId stmtId_;
		ContainerId containerId_;
		SessionId sessionId_;
		TransactionManager::GetMode getMode_;
		TransactionManager::TransactionMode txnMode_;

		OptionSet option_;

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

	void execute(
			EventContext &ec, const Event &ev, const Request &request,
			const RowSetRequest &rowSetRequest, const MessageSchema &schema,
			CheckedSchemaIdSet &idSet, Progress &progress,
			PutRowOption putRowOption);

	void checkSchema(TransactionContext &txn, BaseContainer &container,
		const MessageSchema &schema, int32_t localSchemaId,
		CheckedSchemaIdSet &idSet);

	TransactionManager::ContextSource createContextSource(
			const Request &request, const RowSetRequest &rowSetRequest);

	template<typename S>
	void decodeMultiSchema(
			S &in, const Request& request,
			util::XArray<const MessageSchema *> &schemaList);
	template<typename S>
	void decodeMultiRowSet(
			S &in, const Request &request,
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
	typedef util::XArray<ColumnType> RowKeyColumnTypeList;

	enum RowKeyPredicateType { PREDICATE_TYPE_RANGE, PREDICATE_TYPE_DISTINCT };

	struct RowKeyPredicate {
		ColumnType keyType_;
		RowKeyPredicateType predicateType_;
		int8_t rangeFlags_[2];
		int32_t rowCount_;
		RowKeyDataList *keys_;
		RowKeyData *compositeKeys_;
		RowKeyColumnTypeList *compositeColumnTypes_;
	};

	struct SearchEntry {
		SearchEntry(ContainerId containerId, const FullContainerKey *containerKey,
			const RowKeyPredicate *predicate)
			: stmtId_(0),
			  containerId_(containerId),
			  sessionId_(TXN_EMPTY_CLIENTID.sessionId_),
			  getMode_(TransactionManager::AUTO),
			  txnMode_(TransactionManager::AUTO_COMMIT),
			  containerKey_(containerKey),
			  predicate_(predicate) {}

		StatementId stmtId_;
		ContainerId containerId_;
		SessionId sessionId_;
		TransactionManager::GetMode getMode_;
		TransactionManager::TransactionMode txnMode_;

		const FullContainerKey *containerKey_;
		const RowKeyPredicate *predicate_;
	};

	enum OptionType {


		OPTION_END = 0xFFFFFFFF
	};

	uint32_t execute(
			EventContext &ec, const Event &ev, const Request &request,
			const SearchEntry &entry, const SchemaMap &schemaMap,
			EventByteOutStream &replyOut, Progress &progress);

	void buildSchemaMap(
			PartitionId pId,
			const util::XArray<SearchEntry> &searchList, SchemaMap &schemaMap,
			EventByteOutStream &out, int32_t acceptableFeatureVersion);

	void checkContainerRowKey(
			BaseContainer *container, const RowKeyPredicate &predicate);

	TransactionManager::ContextSource createContextSource(
			const Request &request, const SearchEntry &entry);

	template<typename S>
	void decodeMultiSearchEntry(
			S &in,
			PartitionId pId, DatabaseId loginDbId,
			const char8_t *loginDbName, const char8_t *specifiedDbName,
			UserType userType, RequestType requestType, bool isSystemMode,
			util::XArray<SearchEntry> &searchList);
	template<typename S>
	RowKeyPredicate decodePredicate(
			S &in, util::StackAllocator &alloc);

	void encodeEntry(
			const FullContainerKey &containerKey, ContainerId containerId,
			const SchemaMap &schemaMap, ResultSet &rs, EventByteOutStream &out);

	void executeRange(
			EventContext &ec, const SchemaMap &schemaMap,
			TransactionContext &txn, BaseContainer *container, const RowKeyData *startKey,
			const RowKeyData *finishKey, EventByteOutStream &replyOut, Progress &progress);
	void executeGet(
			EventContext &ec, const SchemaMap &schemaMap,
			TransactionContext &txn, BaseContainer *container, const RowKeyData &rowKey,
			EventByteOutStream &replyOut, Progress &progress);
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
			  optionSet_(alloc),
			  isPartial_(false),
			  partialQueryOption_(alloc) {
			query_.ptr_ = NULL;
		}

		EventType stmtType_;

		StatementId stmtId_;
		ContainerId containerId_;
		SessionId sessionId_;
		SchemaVersionId schemaVersionId_;
		TransactionManager::GetMode getMode_;
		TransactionManager::TransactionMode txnMode_;

		OptionSet optionSet_;

		FetchOption fetchOption_;
		bool isPartial_;
		PartialQueryOption partialQueryOption_;

		union {
			void *ptr_;
			util::String *tqlQuery_;
			GeometryQuery *geometryQuery_;
			RangeQuery *rangeQuery_;
			SamplingQuery *samplingQuery_;
		} query_;
	};

	enum PartialResult { PARTIAL_RESULT_SUCCESS, PARTIAL_RESULT_FAIL };

	typedef util::XArray<const QueryRequest *> QueryRequestList;

	void execute(
			EventContext &ec, const Event &ev, const Request &request,
			int32_t &fetchByteSize, const QueryRequest &queryRequest,
			EventByteOutStream &replyOut, Progress &progress);

	TransactionManager::ContextSource createContextSource(
			const Request &request, const QueryRequest &queryRequest);

	template<typename S>
	void decodeMultiQuery(
			S &in, const Request &request,
			util::XArray<const QueryRequest *> &queryList);

	void encodeMultiSearchResultHead(
			EventByteOutStream &out, uint32_t queryCount);
	void encodeEmptySearchResult(
			util::StackAllocator &alloc, EventByteOutStream &out);
	void encodeSearchResult(util::StackAllocator &alloc,
			EventByteOutStream &out, const ResultSet &rs);

	const char8_t *getQueryTypeName(EventType queryStmtType);
};

/*!
	@brief Handles REPLICATION_LOG statement requested from another node
*/
class ReplicationLogHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
protected:
	virtual void decode(EventByteInStream &in, ReplicationAck &ack, Request &requestOption, 
		ConnectionOption &connOption) {
		UNUSED_VARIABLE(requestOption);
		UNUSED_VARIABLE(connOption);

		decodeReplicationAck(in, ack);
	}

	virtual bool isOptionalFormat() const {
		return false;
	}
public:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), alloc_(alloc), ack_(src.pId_), logVer_(0),
			closedResourceIds_(alloc), logRecordList_(alloc) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeReplicationAck(in, ack_);
			decodeIntData<S, uint16_t>(in, logVer_);

			uint32_t closedResourceIdCount;
			decodeIntData<S, uint32_t>(in, closedResourceIdCount);
			closedResourceIds_.assign(
				static_cast<size_t>(closedResourceIdCount), TXN_EMPTY_CLIENTID);
			for (uint32_t i = 0; i < closedResourceIdCount; i++) {
				decodeLongData<S, SessionId>(in, closedResourceIds_[i].sessionId_);
				decodeUUID(
					in, closedResourceIds_[i].uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
			}

			uint32_t logRecordCount;
			decodeIntData<S, uint32_t>(in, logRecordCount);
			decodeBinaryData<S>(in, logRecordList_, true);
			lastPosition_ = in.base().position();
		}
		util::StackAllocator& alloc_;
		ReplicationAck ack_;
		uint16_t logVer_;
		util::XArray<ClientId> closedResourceIds_;
		util::XArray<uint8_t> logRecordList_;
		size_t lastPosition_;
	};

};
/*!
	@brief Handles REPLICATION_ACK statement requested from another node
*/
class ReplicationAckHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
protected:
	virtual void decode(
			EventByteInStream &in, ReplicationAck &ack, Request &requestOption,
			ConnectionOption &connOption) {
		UNUSED_VARIABLE(requestOption);
		UNUSED_VARIABLE(connOption);

		decodeReplicationAck(in, ack);
	}

private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), alloc_(alloc), ack_(src.pId_) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeReplicationAck(in, ack_);

		}
		util::StackAllocator& alloc_;
		ReplicationAck ack_;
	};

};

/*!
	@brief Handles REPLICATION_LOG2 statement requested from another node
*/
class ReplicationLog2Handler : public ReplicationLogHandler {
public:
protected:
	void decode(EventByteInStream &in, ReplicationAck &ack, Request &requestOption, 
		ConnectionOption &connOption) {
		decodeRequestCommonPart(in, requestOption, connOption);
		decodeReplicationAck(in, ack);
	}
	bool isOptionalFormat() const {
		return true;
	}
};

/*!
	@brief Handles REPLICATION_ACK2 statement requested from another node
*/
class ReplicationAck2Handler : public ReplicationAckHandler {
public:
protected:
	void decode(EventByteInStream &in, ReplicationAck &ack, Request &requestOption,
		ConnectionOption &connOption) {
		decodeRequestCommonPart(in, requestOption, connOption);
		decodeReplicationAck(in, ack);
	}
};

class DbUserHandler : public StatementHandler {
public:
	static void checkPasswordLength(const char8_t *password);

	struct DUColumnInfo {
		const char *name;
		ColumnType type;
	};

	struct DUColumnValue {
		ColumnType type;
		const char8_t *sval;
		int8_t bval;
	};

	static void makeDatabaseInfoList(const DataStoreConfig& dsConfig,
		TransactionContext& txn, util::StackAllocator& alloc,
		BaseContainer& container, ResultSet& rs,
		util::XArray<DatabaseInfo*>& dbInfoList, util::Vector<DatabaseId> *dbIdList = NULL);

protected:
	struct DUGetInOut {
		enum DUGetType {
			RESULT_NUM,
			DBID,
			ROLE,
			REMOVE,
			AUTH, 
			USER,
		};

		explicit DUGetInOut()
			: type(RESULT_NUM),
			  count(-1), s(NULL), dbId(UNDEF_DBID), priv(READ),
			  logRecordList(NULL) {}

		void setForDbId(const char8_t *dbName, UserType userType) {
			type = DBID;
			s = dbName;
			this->userType = userType;
		}

		void setForRole() {
			type = ROLE;
		}
		void setForRemove(util::XArray<const util::XArray<uint8_t> *> *logRecordList) {
			type = REMOVE;
			this->logRecordList = logRecordList;
		}
		void setForAuth(const char8_t *digest) {
			type = AUTH;
			s = digest;
		}
		int8_t type;
		int64_t count;
		const char8_t *s;
		UserType userType;
		DatabaseId dbId;
		PrivilegeType priv;
		util::XArray<const util::XArray<uint8_t> *> *logRecordList;
	};

	struct DUQueryInOut {
		enum DUQueryType {
			RESULT_NUM,
			AGG,
			USER_DETAILS,
			DB_DETAILS,
			USER_INFO,
			DB_INFO,
			REMOVE,
		};

		enum DUQueryPhase {
			AUTH,
			GET,
			NORMAL,
			SYSTEM,
		};

		explicit DUQueryInOut()
			: type(RESULT_NUM), phase(NORMAL),
			  count(-1), s(NULL), 
			  dbInfo(NULL),
			  flag(false),
			  userInfoList(NULL),
			  dbNameSpecified(false), dbInfoList(NULL),
			  logRecordList(NULL) {}

		void setForAgg(const char8_t *userName = NULL) {
			type = AGG;
			if (userName) {
				this->phase = AUTH;
			}
			s = userName;
			
		}
		void setForUserDetails(const char8_t *digest) {
			type = USER_DETAILS;
			phase = SYSTEM;
			s = digest;
		}
		void setForDatabaseDetails(DatabaseInfo *dbInfo) {
			type = DB_DETAILS;
			phase = SYSTEM;
			this->dbInfo = dbInfo;
		}
		void setForUserInfoList(util::XArray<UserInfo*> *userInfoList) {
			type = USER_INFO;
			phase = GET;
			this->userInfoList = userInfoList;
		}
		void setForDbInfoList(bool dbNameSpecified, util::XArray<DatabaseInfo*> *dbInfoList) {
			type = DB_INFO;
			phase = GET;
			this->dbNameSpecified = dbNameSpecified;
			this->dbInfoList = dbInfoList;
		}
		void setForRemove(util::XArray<const util::XArray<uint8_t> *> *logRecordList) {
			type = REMOVE;
			phase = SYSTEM;
			this->logRecordList = logRecordList;
		}

		int8_t type;
		int8_t phase;
		int64_t count;
		const char8_t *s;
		DatabaseInfo *dbInfo;
		bool flag;
		util::XArray<UserInfo*> *userInfoList;
		bool dbNameSpecified;
		util::XArray<DatabaseInfo*> *dbInfoList;
		util::XArray<const util::XArray<uint8_t> *> *logRecordList;
	};

	template<typename S>
	static void decodeUserInfo(
		S &in, UserInfo &userInfo);
	template<typename S>
	static void decodeDatabaseInfo(S &in,
		DatabaseInfo &dbInfo, util::StackAllocator &alloc);
	
	void makeSchema(util::XArray<uint8_t> &containerSchema, const DUColumnInfo *columnInfoList, int n);

	void makeRow(PartitionId pId,
		util::StackAllocator &alloc, const ColumnInfo *columnInfoList,
		uint32_t columnNum, DUColumnValue *valueList, RowData &rowData);

	void makeRowKey(const char *name, RowKeyData &rowKey);
	void makeRowKey(DatabaseInfo &dbInfo, RowKeyData &rowKey);


	void putContainer(util::StackAllocator &alloc, 
		util::XArray<uint8_t> &containerInfo, const char *name,
		const util::DateTime now, const EventMonotonicTime emNow, const Request &request,
		util::XArray<const util::XArray<uint8_t>*> &logRecordList);

	bool checkContainer(
		EventContext &ec, Request &request, const char8_t *containerName);
	void putRow(
		EventContext &ec, Event &ev, const Request &request,
		const char8_t *containerName, DUColumnValue *cvList,
		util::XArray<const util::XArray<uint8_t>*> &logRecordList);
			
	bool runWithRowKey(
		EventContext &ec, const TransactionManager::ContextSource &cxtSrc, 
		const char8_t *containerName, const RowKeyData &rowKey,
		DUGetInOut *option);
	void runWithTQL(
		EventContext &ec, const TransactionManager::ContextSource &cxtSrc, 
		const char8_t *containerName, const char8_t *tql,
		DUQueryInOut *option);

	void checkUserName(const char8_t *userName, bool detailed);
	void checkAdminUser(UserType userType);
	void checkDatabaseName(const char8_t *dbName);
	void checkConnectedDatabaseName(
		ConnectionOption &connOption, const char8_t *dbName);
	void checkConnectedUserName(
		ConnectionOption &connOption, const char8_t *userName);
	void checkPartitionIdForUsers(PartitionId pId);
	void checkDigest(const char8_t *digest, size_t maxStrLength);
	void checkPrivilegeSize(DatabaseInfo &dbInfo, size_t size);
	void checkModifiable(bool modifiable, bool value);


	void initializeMetaContainer(
		EventContext &ec, Event &ev, const Request &request,
		util::XArray<const util::XArray<uint8_t>*> &logRecordList);

	int64_t getCount(
		EventContext &ec, const Request &request, const char8_t *containerName);
	
	void checkUser(
		EventContext &ec, const Request &request, const char8_t *userName,
		bool &existFlag, bool isRole = false);
	void checkDatabase(
		EventContext &ec, const Request &request, DatabaseInfo &dbInfo);
	void checkDatabaseDetails(
		EventContext &ec, const Request &request, DatabaseInfo &dbInfo,
		bool &existFlag);

	void putDatabaseRow(
		EventContext &ec, Event &ev, const Request &request,
		DatabaseInfo &dbInfo, bool isCreate,
		util::XArray<const util::XArray<uint8_t>*> &logRecordList);

	void getUserInfoList(
		EventContext &ec, 
		const Request &request, const char8_t *userName,
		UserType userType, util::XArray<UserInfo*> &userInfoList);
	
private:
	bool checkDigest(TransactionContext &txn, util::StackAllocator &alloc, 
		BaseContainer *container, ResultSet *rs, const char8_t *digest);
	
	void fetchRole(TransactionContext &txn, util::StackAllocator &alloc, 
		BaseContainer *container, ResultSet *rs, PrivilegeType &priv);
	void removeRowWithRowKey(TransactionContext &txn, util::StackAllocator &alloc, 
		const TransactionManager::ContextSource &cxtSrc, 
		KeyDataStoreValue &keyStoreValue, const RowKeyData &rowKey,
		util::XArray<const util::XArray<uint8_t> *> &logRecordList);
	
	bool checkPrivilege(const DataStoreConfig& dsConfig, TransactionContext &txn, util::StackAllocator &alloc,
		BaseContainer *container, ResultSet *rs, DatabaseInfo &dbInfo);
	void makeUserInfoList(
		TransactionContext &txn, util::StackAllocator &alloc,
		BaseContainer &container, ResultSet &rs,
		util::XArray<UserInfo*> &userInfoList);

	void removeRowWithRS(TransactionContext &txn, util::StackAllocator &alloc, 
		const TransactionManager::ContextSource &cxtSrc,
		KeyDataStoreValue &keyStoreValue,
		BaseContainer &container, ResultSet &rs,
		util::XArray<const util::XArray<uint8_t> *> &logRecordList);
};

/*!
	@brief Handles PUT_USER statement
*/
class PutUserHandler : public DbUserHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), userInfo_(alloc), modifiable_(false) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeUserInfo<S>(in, userInfo_);
			decodeBooleanData<S>(in, modifiable_);
		}
		UserInfo userInfo_;
		bool modifiable_;
	};

	typedef SimpleOutputMessage OutMessage;
	void checkUserDetails(
		EventContext &ec, const Request &request, const char8_t *userName,
		const char8_t *digest, bool detailFlag, bool &existFlag);

	void putUserRow(
		EventContext &ec, Event &ev, const Request &request,
		UserInfo &userInfo,
		util::XArray<const util::XArray<uint8_t>*> &logRecordList);
};

/*!
	@brief Handles DROP_USER statement
*/
class DropUserHandler : public DbUserHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), userName_(alloc) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeStringData<S, util::String>(in, userName_);
		}
		util::String userName_;
	};

	typedef SimpleOutputMessage OutMessage;
	void removeUserRowInDB(
		EventContext &ec, Event &ev,
		const Request &request, const char8_t *userName,
		util::XArray<const util::XArray<uint8_t> *> &logRecordList);
	void removeUserRow(
		EventContext &ec, Event &ev, const Request &request,
		const char8_t *userName,
		util::XArray<const util::XArray<uint8_t> *> &logRecordList);
};

/*!
	@brief Handles GET_USERS statement
*/
class GetUsersHandler : public DbUserHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), userName_(alloc), withFilter_(false),
			property_(Message::USER_NORMAL) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeBooleanData<S>(in, withFilter_);
			if (withFilter_) {
				decodeStringData<S, util::String>(in, userName_);
				in >> property_;
			}
		}
		util::String userName_;
		bool withFilter_;
		int8_t property_;  
	};
	struct OutMessage : public SimpleOutputMessage {
		OutMessage(StatementId stmtId, StatementExecStatus status,
			OptionSet* optionSet, util::XArray<UserInfo*>* userInfoList, Request& request)
			: SimpleOutputMessage(stmtId, status, optionSet), userInfoList_(userInfoList), request_(request) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			SimpleOutputMessage::encode(out);
			out << static_cast<uint32_t>(userInfoList_->size());
			for (size_t i = 0; i < userInfoList_->size(); i++) {
				UserInfo* info = (*userInfoList_)[i];
				encodeStringData<S>(out, info->userName_);
				out << info->property_;
				encodeBooleanData<S>(out, info->withDigest_);
				encodeStringData<S>(out, info->digest_);
			}
			if (request_.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>() >= StatementMessage::FEATURE_V4_5) {
				for (size_t i = 0; i < userInfoList_->size(); i++) {
					UserInfo* info = (*userInfoList_)[i];
					encodeBooleanData(out, info->isGroupMapping_);
					encodeStringData(out, info->roleName_);
				}
			}
		}
		template<typename S>
		void decode(S& in) {
			UNUSED_VARIABLE(in);
		}
		util::XArray<UserInfo*> *userInfoList_;
		Request& request_;
	};

	void checkNormalUser(UserType userType, const char8_t *userName);
	
	void makeUserInfoListForAdmin(util::StackAllocator &alloc,
		const char8_t *userName, util::XArray<UserInfo*> &userInfoList);
	void makeUserInfoListForLDAPUser(util::StackAllocator &alloc,
		ConnectionOption &connOption, util::XArray<UserInfo*> &userInfoList);
	void checkUserInfoList(int32_t featureVersion, util::XArray<UserInfo *> &userInfoList);
};

/*!
	@brief Handles PUT_DATABASE statement
*/
class PutDatabaseHandler : public DbUserHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), alloc_(alloc), dbInfo_(alloc), modifiable_(false) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeDatabaseInfo(in, dbInfo_, alloc_);
			decodeBooleanData<S>(in, modifiable_);
		}
		util::StackAllocator& alloc_;
		DatabaseInfo dbInfo_;
		bool modifiable_;  
	};

	typedef SimpleOutputMessage OutMessage;
	void setPrivilegeInfoListForAdmin(util::StackAllocator &alloc,
		const char8_t *userName, util::XArray<PrivilegeInfo *> &privilegeInfoList);
	void checkDatabase(
		EventContext &ec, const Request &request, DatabaseInfo &dbInfo,
		bool &existFlag);
};

/*!
	@brief Handles DROP_DATABASE statement
*/
class DropDatabaseHandler : public DbUserHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), dbName_(alloc) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeStringData<S, util::String>(in, dbName_);
		}
		util::String dbName_;
	};
	typedef SimpleOutputMessage OutMessage;
	void removeDatabaseRow(
		EventContext &ec, Event &ev,
		const Request &request, const char8_t *dbName, bool isAdmin,
		util::XArray<const util::XArray<uint8_t> *> &logRecordList);
};

/*!
	@brief Handles GET_DATABASES statement
*/
class GetDatabasesHandler : public DbUserHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), dbName_(alloc), withFilter_(false),
			property_(0) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeBooleanData<S>(in, withFilter_);
			if (withFilter_) {
				decodeStringData<S, util::String>(in, dbName_);
				if (dbName_.empty()) {
					dbName_ = GS_PUBLIC;
				}
				in >> property_;
			}
		}
		util::String dbName_;
		bool withFilter_;
		int8_t property_;  
	};

	struct OutMessage : public SimpleOutputMessage {
		OutMessage(StatementId stmtId, StatementExecStatus status,
			OptionSet* optionSet, util::XArray<DatabaseInfo*>* databaseInfoList)
			: SimpleOutputMessage(stmtId, status, optionSet), databaseInfoList_(databaseInfoList) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			SimpleOutputMessage::encode(out);
			out << static_cast<uint32_t>(databaseInfoList_->size());
			for (size_t i = 0; i < databaseInfoList_->size(); i++) {
				DatabaseInfo* info = (*databaseInfoList_)[i];
				encodeStringData<S>(out, info->dbName_);
				out << info->property_;
				out << static_cast<uint32_t>(info->privilegeInfoList_.size());
				for (size_t j = 0; j < info->privilegeInfoList_.size(); j++) {
					encodeStringData<S>(out,
						info->privilegeInfoList_[j]->userName_);
					encodeStringData<S>(out,
						info->privilegeInfoList_[j]->privilege_);
				}
			}
		}
		template<typename S>
		void decode(S& in) {
			UNUSED_VARIABLE(in);
		}
		util::XArray<DatabaseInfo*> *databaseInfoList_;
	};

	void makeDatabaseInfoListForPublic(
		util::StackAllocator &alloc,
		util::XArray<UserInfo *> &userInfoList,
		util::XArray<DatabaseInfo *> &dbInfoList);
	void getDatabaseInfoList(
		EventContext &ec, const Request &request,
		const char8_t *dbName, const char8_t *userName,
		util::XArray<DatabaseInfo *> &dbInfoList);
	
	void checkDatabaseInfoList(int32_t featureVersion, util::XArray<DatabaseInfo *> &dbInfoList);
};

/*!
	@brief Handles PUT_PRIVILEGE statement
*/
class PutPrivilegeHandler : public DbUserHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), alloc_(alloc), dbInfo_(alloc) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeDatabaseInfo(in, dbInfo_, alloc_);
		}
		util::StackAllocator& alloc_;
		DatabaseInfo dbInfo_;
	};
	typedef SimpleOutputMessage OutMessage;
};

/*!
	@brief Handles DROP_PRIVILEGE statement
*/
class DropPrivilegeHandler : public DbUserHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), alloc_(alloc), dbInfo_(alloc) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeDatabaseInfo(in, dbInfo_, alloc_);
		}
		util::StackAllocator& alloc_;
		DatabaseInfo dbInfo_;
	};
	typedef SimpleOutputMessage OutMessage;
	void removeDatabaseRow(
		EventContext &ec, Event &ev,
		const Request &request, DatabaseInfo &dbInfo,
		util::XArray<const util::XArray<uint8_t> *> &logRecordList);
};



/*!
	@brief Handles LOGIN statement
*/
class LoginHandler : public DbUserHandler {
public:

	struct LoginContext {
		EventContext &ec_;
		Event &ev_;
		Request &request_;
		const Response &response_;
		const bool systemMode_;
		const char8_t *dbName_;
		const char8_t *userName_;
		const char8_t *digest_;
		DatabaseId dbId_;
		PrivilegeType priv_;
		util::String roleName_;
		bool isLDAPAuthentication_;

		LoginContext(EventContext &ec, Event &ev, Request &request, const Response &response, bool systemMode):
			ec_(ec), ev_(ev), request_(request), response_(response), systemMode_(systemMode),
			roleName_(ec.getAllocator()) {
			ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();
			dbName_ = connOption.dbName_.c_str();
			userName_ = connOption.userName_.c_str();
			digest_ = connOption.digest_.c_str();
			isLDAPAuthentication_ = connOption.isLDAPAuthentication_;

			dbId_ = UNDEF_DBID;
			priv_ = READ;
		}
	};

	/*!
		@brief Represents the information about AuthenticationAck
	*/
	struct AuthenticationAck {
		explicit AuthenticationAck(PartitionId pId) : pId_(pId) {}

		PartitionId pId_;
		ClusterVersionId clusterVer_;
		AuthenticationId authId_;

		PartitionId authPId_;
	};
	
	static const uint32_t MAX_APPLICATION_NAME_LEN = 64;

	void operator()(EventContext &ec, Event &ev);

protected:
	struct AuthenticationAckPart {
		AuthenticationAckPart(ClusterVersionId clusterVer,
			AuthenticationId authId, PartitionId authPId) :
			clusterVer_(clusterVer), authId_(authId), authPId_(authPId) {}
		template<typename S>
		void encode(S& out) {

			out << clusterVer_;
			out << authId_;
			out << authPId_;
		}
		template<typename S>
		void decode(S& in) {
			in >> clusterVer_;
			in >> authId_;
			in >> authPId_;
		}
		ClusterVersionId clusterVer_;
		AuthenticationId authId_;
		PartitionId authPId_;
	};
	bool checkPublicDB(const char8_t *dbName);

	template<typename S>
	static void decodeAuthenticationAck(
		S &in, AuthenticationAck &ack);
	void encodeAuthenticationAckPart(EventByteOutStream &out,
		ClusterVersionId clusterVer, AuthenticationId authId, PartitionId authPId);

	bool executeAuthenticationInternal(
		EventContext &ec,
		util::StackAllocator &alloc, TransactionManager::ContextSource &cxtSrc,
		const char8_t *userName, const char8_t *digest, const char8_t *dbName,
		UserType userType, int checkLevel, DatabaseId &dbId, PrivilegeType &priv);

	void addUserCache(EventContext &ec, 
		const char8_t *dbName, const char8_t *userName, const char8_t *digest,
		DatabaseId dbId, EventMonotonicTime authTime, PrivilegeType priv, bool isLDAPAuthentication, const char8_t *roleName);

	void releaseUserCache(ConnectionOption &connOption);
	void checkClusterName(std::string &clusterName);
	void checkApplicationName(const char8_t *applicationName);
	
	bool checkSystemDB(const char8_t *dbName);
	bool checkAdmin(util::String &userName);
	bool checkLocalAuthNode();
	
	void executeAuthentication(
		EventContext &ec, Event &ev, StatementId authStmtId,
		const char8_t *userName, const char8_t *digest,
		const char8_t *dbName, UserType userType,
		util::XArray<const char8_t *> &roleNameList);

	bool checkUserCache(EventContext &ec, LoginContext &login);

	void authAdmin(util::StackAllocator &alloc, LoginContext &login);
	void authInternalUser(util::StackAllocator &alloc, LoginContext &login);
	

	void authLDAP(LoginContext &lc);
	void authorization(util::StackAllocator &alloc, LoginContext &login,
		util::XArray<const char8_t *> &roleNameList);
	
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), userName_(alloc),
			digest_(alloc), stmtTimeoutInterval_(0), isImmediateConsistency_(false) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			decodeStringData<S, util::String>(in, userName_);
			decodeStringData<S, util::String>(in, digest_);
			decodeIntData<S, int32_t>(in,
				stmtTimeoutInterval_);  
			decodeBooleanData<S>(in, isImmediateConsistency_);
			if (in.base().remaining() > 0) {
				decodeStringData<S, std::string>(in, clusterName_);
			}
		}
		util::String userName_;
		util::String digest_;
		std::string clusterName_;
		int32_t stmtTimeoutInterval_;
		bool isImmediateConsistency_;
	};

	struct OutMessage : public SimpleOutputMessage {
		OutMessage(StatementId stmtId, StatementExecStatus status,
			OptionSet* optionSet, ConnectionOption* connectionOption)
			: SimpleOutputMessage(stmtId, status, optionSet), connectionOption_(connectionOption) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			SimpleOutputMessage::encode(out);
			if (connectionOption_ != NULL) {
				encodeIntData<S, int8_t>(out, connectionOption_->authMode_);
				encodeIntData<S, DatabaseId>(out, connectionOption_->dbId_);
			}
		}
		template<typename S>
		void decode(S& in) {
			UNUSED_VARIABLE(in);
		}
		ConnectionOption* connectionOption_;
	};
};

/*!
	@brief Handles AUTHENTICATION statement requested from another node
*/
class AuthenticationHandler : public LoginHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public Serializable {
		InMessage(util::StackAllocator& alloc, AuthenticationAck& ack)
			: Serializable(&alloc), authAck_(ack), userName_(alloc), digest_(alloc),
			dbName_(alloc), userType_(Message::USER_NORMAL), optionalRequest_(alloc),
			isSQL_(false), pIdForNewSQL_(UNDEF_PARTITIONID), roleNameList_(alloc) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			decodeAuthenticationAck<S>(in, authAck_);
			decodeStringData<S, util::String>(in, userName_);
			decodeStringData<S, util::String>(in, digest_);
			decodeStringData<S, util::String>(in, dbName_);

			char8_t byteData;
			in >> byteData;
			userType_ = static_cast<UserType>(byteData);

			in >> isSQL_;

			in >> pIdForNewSQL_;
			int32_t num = 0;
			if (strlen(digest_.c_str()) == 0) {
				in >> num;
				for (int32_t i = 0; i < num; i++) {
					util::String* roleName = ALLOC_NEW(*alloc_) util::String(*alloc_);
					decodeStringData<S, util::String>(in, *roleName);
					roleNameList_.push_back(roleName->c_str());
				}
			}

			optionalRequest_.decode<S>(in);
		}
		AuthenticationAck& authAck_;
		util::String userName_;
		util::String digest_;
		util::String dbName_;
		UserType userType_;
		StatementMessage::OptionSet optionalRequest_;
		char8_t isSQL_;
		PartitionId pIdForNewSQL_;
		util::XArray<const char8_t*> roleNameList_;

	};


	void replyAuthenticationAck(EventContext &ec, util::StackAllocator &alloc,
		const NodeDescriptor &ND, const AuthenticationAck &request,
		DatabaseId dbId, PrivilegeType priv, const char *roleName, bool isNewSQL = false);
	void replySuccess(
		EventContext& ec, util::StackAllocator& alloc,
		const NodeDescriptor& ND, EventType stmtType,
		const AuthenticationAck& request, Serializable& message,
		bool isSQL);
};
/*!
	@brief Handles AUTHENTICATION_ACK statement requested from another node
*/
class AuthenticationAckHandler : public LoginHandler {
public:
	void operator()(EventContext &ec, Event &ev);

	void authHandleError(
		EventContext &ec, AuthenticationAck &ack, std::exception &e);
private:
	struct InMessage : public Serializable {
		InMessage(util::StackAllocator& alloc, AuthenticationAck&ack)
			: Serializable(&alloc), authAck_(ack), num_(0), dbId_(UNDEF_DBID),
			dbName_(alloc), priv_(ALL) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			decodeAuthenticationAck<S>(in, authAck_);
			in >> num_;
			if (num_ > 0) {
				decodeStringData<S, util::String>(in, dbName_);
				in >> dbId_;
				util::String privilege(*alloc_);
				decodeStringData<S, util::String>(in, privilege);
				if (strcmp(privilege.c_str(), "ALL") == 0) {
					priv_ = ALL;
				}
				else {
					priv_ = READ;
				}
			}
		}
		AuthenticationAck& authAck_;
		int32_t num_;
		DatabaseId dbId_;
		util::String dbName_;
		PrivilegeType priv_ = ALL;
	};

	typedef LoginHandler::OutMessage OutMessage;
	void replySuccess(
			EventContext &ec, util::StackAllocator &alloc,
			StatementExecStatus status, const Request &request,
			const AuthenticationContext &authContext);
};


/*!
	@brief Handles DISCONNECT statement
*/
class DisconnectHandler : public LoginHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles LOGOUT statement
*/
class LogoutHandler : public LoginHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	typedef SimpleInputMessage InMessage;
	typedef SimpleOutputMessage OutMessage;
};

/*!
	@brief Handles CHECK_TIMEOUT event
*/
class CheckTimeoutHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
	void checkNoExpireTransaction(util::StackAllocator &alloc, PartitionId pId);

private:
	static const size_t MAX_OUTPUT_COUNT =
		100;  
	static const uint64_t TIMEOUT_CHECK_STATE_TRACE_COUNT =
		100;  

	std::vector<uint64_t> timeoutCheckCount_;

	void checkReplicationTimeout(EventContext &ec);
	void checkAuthenticationTimeout(EventContext &ec);
	void checkTransactionTimeout(
		EventContext &ec, const util::XArray<bool> &checkPartitionFlagList);
	void checkRequestTimeout(
		EventContext &ec, const util::XArray<bool> &checkPartitionFlagList);
	void checkResultSetTimeout(EventContext &ec);
	void checkKeepaliveTimeout(
		EventContext &ec, const util::XArray<bool> &checkPartitionFlagList);

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
private:
	struct InMessage : public Serializable {
		InMessage() : Serializable(NULL), id_(UNDEF_STATEMENTID) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			decodeLongData<S, StatementId>(in, id_);
		}
		StatementId id_;
	};

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

	int64_t getContainerNameList(
		EventContext& ec, PartitionGroupId pgId,
		PartitionId pId, int64_t start, ResultSize limit, util::XArray<FullContainerKey>& containerNameList);

	void sendCheckDropContainerList(EventContext& ec,
		PartitionId pId, util::XArray<FullContainerKey>& containerNameList);

	struct ExpiredContainerContext {

		enum Status {
			INIT,
			EXECUTE
		};

		ExpiredContainerContext(
			const PartitionGroupConfig& pgConfig, PartitionGroupId pgId);

		Status getStatus() {
			return status_;
		}

		void setStatus(Status status) {
			status_ = status;
		}
		
		bool checkInterruption() {
			return (
					static_cast<int64_t>(watch_.elapsedMillis()) >=
					interruptionInterval_);
		}

		void prepare(int32_t checkInterval, int32_t limitCount, int32_t interruptionInterval) {
			checkInterval_ = checkInterval;
			limitCount_ = limitCount;
			interruptionInterval_ = interruptionInterval;
			watch_.reset();
			watch_.start();
		}

		bool checkCounter() {
			counter_++;
			if (counter_ % checkInterval_ == 0) {
				status_ = EXECUTE;
				return true;
			}
			else {
				return false;
			}
		}

		bool next(int64_t size);

		util::Stopwatch watch_;
		Status status_;
		PartitionGroupId pgId_;
		uint32_t startPId_;
		uint32_t endPId_;
		uint32_t currentPId_;
		int64_t currentPos_;
		int32_t checkInterval_;
		int32_t limitCount_;
		int32_t interruptionInterval_;
		int64_t counter_;
	};

	ExpiredContainerContext& getContext(PartitionGroupId pgId) {
		return *context_[pgId];
	}

	bool checkAndDrop(EventContext& ec, ExpiredContainerContext& cxt);

	void setConcurrency(int64_t concurrency);

	std::vector<ExpiredContainerContext*> context_;

private:
	static const uint64_t PERIODICAL_MAX_SCAN_COUNT =
		500;  

	static const uint64_t PERIODICAL_TABLE_SCAN_MAX_COUNT =
		5000;  

	static const uint64_t PERIODICAL_TABLE_SCAN_WAIT_COUNT =
		30;  

	PartitionId *pIdCursor_;
	int64_t currentLimit_;
	int64_t currentPos_;
	int64_t expiredCheckCount_;
	int64_t expiredCounter_;
	int32_t concurrency_;
	std::vector<int64_t> expiredCounterList_;
	std::vector<int64_t> startPosList_;
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



class KeepLogHandler : public StatementHandler {
public:
	KeepLogHandler();
	~KeepLogHandler();
	void operator()(EventContext& ec, Event& ev);
};


/*!
	@brief Handles BACK_GROUND event
*/
class BackgroundHandler : public StatementHandler {
public:
	BackgroundHandler();
	~BackgroundHandler();

	void operator()(EventContext &ec, Event &ev);
	int32_t getWaitTime(EventContext &ec, Event &ev, int32_t opeTime);
	uint64_t diffLsn(PartitionId pId);
private:
	struct InMessage : public Serializable {
		InMessage()	: Serializable(NULL), bgId_(false) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			decodeIntData<S, BackgroundId>(in, bgId_);
		}
		BackgroundId bgId_;
	};

	LogSequentialNumber *lsnCounter_;
};

/*!
	@brief Handles CONTINUE_CREATE_INDEX/CONTINUE_ALTER_CONTAINER event
*/
class ContinueCreateDDLHandler : public StatementHandler {
public:
	ContinueCreateDDLHandler();
	~ContinueCreateDDLHandler();

	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public Serializable {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: Serializable(&alloc), connOption_(connOption), request_(alloc, src) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			request_.fixed_.decode(in);
			in >> request_.fixed_.startStmtId_;
			if (in.base().remaining() != 0) {
				decodeRequestOptionPart<S>(in, request_, connOption_);
			}
		}

		ConnectionOption& connOption_;
		Request request_;
	};

	typedef DSMessage OutMessage;
};

/*!
	@brief Handles UPDATE_DATA_STORE_STATUS event
*/
class UpdateDataStoreStatusHandler : public StatementHandler {
public:
	UpdateDataStoreStatusHandler();
	~UpdateDataStoreStatusHandler();

	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public Serializable {
		InMessage()	: Serializable(NULL), inputTime_(0), force_(false) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			decodeLongData<S, Timestamp>(in, inputTime_);
			decodeBooleanData<S>(in, force_);
		}

		Timestamp inputTime_;
		bool force_;
	};
};


class SQLGetContainerHandler : public StatementHandler {
public:
	typedef std::pair<ContainerId, BtreeMap::SearchContext*> SearchEntry;
	typedef util::Vector<SearchEntry> SearchList;
	typedef util::Vector<int64_t> EstimationList;

	enum SubCommand {
		SUB_CMD_NONE,
		SUB_CMD_GET_CONTAINER,
		SUB_CMD_ESTIMATE_INDEX_SEARCH_SIZE,
	};

	static const int32_t LATEST_FEATURE_VERSION;

	void operator()(EventContext &ec, Event &ev);

	template<typename S>
	static void encodeSubCommand(S &out, SubCommand cmd) {
		encodeIntData(out, static_cast<int32_t>(cmd));
	}

	template<typename S>
	static SubCommand decodeSubCommand(S &in) {
		int32_t cmdNum;
		decodeIntData(in, cmdNum);
		return checkSubCommand(cmdNum);
	}

	template<typename S>
	static void encodeResult(
			S &out, SQLVariableSizeGlobalAllocator &varAlloc,
			TableSchemaInfo *info, const EstimationList &estimationList);

	template<typename S>
	static void decodeResult(
			S &in, util::StackAllocator &alloc,
			SQLVariableSizeGlobalAllocator &varAlloc,
			util::AllocUniquePtr<TableSchemaInfo> &info,
			EstimationList &estimationList);

	template<typename S>
	static void encodeTableSchemaInfo(
			S &out, SQLVariableSizeGlobalAllocator &varAlloc,
			TableSchemaInfo *info);

	template<typename S>
	static util::AllocUniquePtr<TableSchemaInfo>::ReturnType
	decodeTableSchemaInfo(
			S &in, util::StackAllocator &alloc,
			SQLVariableSizeGlobalAllocator &varAlloc);

	template<typename S>
	static void encodeEstimationList(
			S &out, const EstimationList &estimationList);

	template<typename S>
	static void decodeEstimationList(
			S &in, EstimationList &estimationList);

private:
	struct InMessage : public SimpleInputMessage {
		InMessage(
				util::StackAllocator& alloc, const FixedRequest::Source& src,
				ConnectionOption& connOption) :
				SimpleInputMessage(alloc, src, connOption),
				containerNameBinary_(alloc),
				isContainerLock_(false),
				queryId_(UNDEF_SESSIONID),
				subCmd_(SUB_CMD_NONE),
				searchList_(alloc) {
			alloc_ = &alloc;
		}

		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}

		template<typename S>
		void decode(S& in);

		util::XArray<uint8_t> containerNameBinary_;
		bool isContainerLock_;
		ClientId clientId_;
		SessionId queryId_;
		SubCommand subCmd_;
		SearchList searchList_;
	};

	struct OutMessage : public SimpleOutputMessage {
		OutMessage(
				StatementId stmtId, StatementExecStatus status,
				OptionSet *optionSet, TableSchemaInfo *info,
				SQLVariableSizeGlobalAllocator *globalVarAlloc,
				const EstimationList &estimationList) :
				SimpleOutputMessage(stmtId, status, optionSet),
				info_(info),
				globalVarAlloc_(globalVarAlloc),
				estimationList_(estimationList) {
		}

		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out);

		template<typename S>
		void decode(S& in) {
			UNUSED_VARIABLE(in);
		}

		TableSchemaInfo *info_;
		SQLVariableSizeGlobalAllocator *globalVarAlloc_;

		EstimationList estimationList_;
	};

	void estimateIndexSearchSize(
			EventContext &ec, Event &ev, InMessage &inMes);

	static void checkAcceptableVersion(
			const StatementMessage::Request &request);

	static SubCommand checkSubCommand(int32_t cmdNum);

	template<typename S>
	static void decodeContainerCondition(
			S &in, InMessage &msg, const ConnectionOption &connOption);

	template<typename S>
	static void decodeSearchList(
			TransactionContext &txn, S &in, SearchList &searchList);

	template<typename S>
	static void decodeSearchEntry(
			TransactionContext &txn, S &in, SearchEntry &entry);

	template<typename S>
	static void decodeTermCondition(
			TransactionContext &txn, S &in, BtreeMap::SearchContext &sc,
			const util::Vector<ColumnId> &columnIdList);

	template<typename S>
	static void decodeColumnIdList(S &in, util::Vector<ColumnId> &columnIdList);
};


/*!
	@brief Handles REMOVE_ROW_BY_ID_SET statement
*/
class RemoveRowSetByIdHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), rowIds_(alloc) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			uint64_t numRow;
			in >> numRow;
			rowIds_.resize(static_cast<size_t>(numRow));
			for (uint64_t i = 0; i < numRow; i++) {
				in >> rowIds_[i];
			}
		}

		util::XArray<RowId> rowIds_;
	};

	typedef SimpleOutputMessage OutMessage;
};

/*!
	@brief Handles REMOVE_ROW_BY_ID_SET statement
*/
class UpdateRowSetByIdHandler : public StatementHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	struct InMessage : public SimpleInputMessage {
		InMessage(util::StackAllocator& alloc, const FixedRequest::Source& src, ConnectionOption& connOption)
			: SimpleInputMessage(alloc, src, connOption), rowIds_(alloc), multiRowData_(alloc) {}
		void encode(EventByteOutStream& out) {
			encode<EventByteOutStream>(out);
		}
		void decode(EventByteInStream& in) {
			decode<EventByteInStream>(in);
		}
		void encode(OutStream& out) {
			encode<OutStream>(out);
		}
		template<typename S>
		void encode(S& out) {
			UNUSED_VARIABLE(out);
		}
		template<typename S>
		void decode(S& in) {
			SimpleInputMessage::decode(in);
			uint64_t numRow;
			in >> numRow;
			rowIds_.resize(static_cast<size_t>(numRow));
			for (uint64_t i = 0; i < numRow; i++) {
				in >> rowIds_[i];
			}
			decodeBinaryData<S>(in, multiRowData_, true);
		}

		util::XArray<RowId> rowIds_;
		RowData multiRowData_;
	};
	typedef SimpleOutputMessage OutMessage;
};

/*!
	@brief TransactionService
*/
class TransactionService {
public:
	TransactionService(
			ConfigTable &config, const EventEngine::Config &eeConfig,
			const EventEngine::Source &eeSource, const char8_t *name);
	~TransactionService();

	void initialize(const ManagerSet &mgrSet);

	void start();
	void shutdown();
	void waitForShutdown();

	EventEngine *getEE();

	int32_t getWaitTime(EventContext &ec, Event *ev, int32_t opeTime,
			double rate, int64_t &executableCount, int64_t &afterCount,
			BackgroundEventType type);

	void checkNoExpireTransaction(util::StackAllocator &alloc, PartitionId pId);
	void changeTimeoutCheckMode(PartitionId pId,
		PartitionTable::PartitionStatus after, ChangePartitionType changeType,
		bool isToSubMaster, ClusterStats &stats);

	void enableTransactionTimeoutCheck(PartitionId pId);
	void disableTransactionTimeoutCheck(PartitionId pId);
	bool isTransactionTimeoutCheckEnabled(PartitionId pId) const;

	size_t getWorkMemoryByteSizeLimit() const;

	uint64_t getTotalReadOperationCount() const;
	uint64_t getTotalWriteOperationCount() const;

	uint64_t getTotalRowReadCount() const;
	uint64_t getTotalRowWriteCount() const;

	uint64_t getTotalBackgroundOperationCount() const;
	uint64_t getTotalNoExpireOperationCount() const;
	uint64_t getTotalAbortDDLCount() const;

	void incrementReadOperationCount(PartitionId pId);
	void incrementWriteOperationCount(PartitionId pId);

	void incrementBackgroundOperationCount(PartitionId pId);
	void incrementNoExpireOperationCount(PartitionId pId);
	void incrementAbortDDLCount(PartitionId pId);

	void addRowReadCount(PartitionId pId, uint64_t count);
	void addRowWriteCount(PartitionId pId, uint64_t count);

	bool onBackgroundTask(PartitionGroupId pgId);
	void setBackgroundTask(PartitionGroupId pgId, bool onBackgroundTask);

	int64_t getTotalInternalConnectionCount() {
		return totalInternalConnectionCount_;
	}

	int64_t getTotalExternalConnectionCount() {
		return totalExternalConnectionCount_;
	}

	void setTotalInternalConnectionCount() {
		totalInternalConnectionCount_++;
	}

	void setTotalExternalConnectionCount() {
		totalExternalConnectionCount_++;
	}


	util::StackAllocator* getTxnLogAlloc(PartitionGroupId pgId);

	TransactionManager *getManager() {
		return transactionManager_;
	}

	bool isUpdateEvent(Event &ev);

	ResultSetHolderManager& getResultSetHolderManager();

	void requestUpdateDataStoreStatus(const Event::Source &eventSource, Timestamp time, bool force);

	UserCache *userCache_;
	OpenLDAPFactory* olFactory_;

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
	void setClusterHandler();

	const PartitionGroupConfig pgConfig_;

	class Config : public ConfigTable::ParamHandler {
	public:
		Config(ConfigTable &configTable);

		size_t getWorkMemoryByteSizeLimit() const;

	private:
		void setUpConfigHandler(ConfigTable &configTable);
		virtual void operator()(
			ConfigTable::ParamId id, const ParamValue &value);

		util::Atomic<size_t> workMemoryByteSizeLimit_;
	} serviceConfig_;

	std::vector< util::Atomic<bool> > enableTxnTimeoutCheck_;

	std::vector<uint64_t> readOperationCount_;
	std::vector<uint64_t> writeOperationCount_;
	std::vector<uint64_t> rowReadCount_;
	std::vector<uint64_t> rowWriteCount_;
	std::vector<uint64_t> backgroundOperationCount_;
	std::vector<uint64_t> noExpireOperationCount_;
	std::vector<uint64_t> abortDDLCount_;

	int64_t totalInternalConnectionCount_;
	int64_t totalExternalConnectionCount_;

	std::vector<bool> onBackgroundTask_; 
	std::vector<util::StackAllocator *> txnLogAlloc_; 

	static const int32_t TXN_TIMEOUT_CHECK_INTERVAL =
		3;  
	static const int32_t CHUNK_EXPIRE_CHECK_INTERVAL =
		1;  
	static const int32_t ADJUST_STORE_MEMORY_CHECK_INTERVAL =
		1; 

	ServiceThreadErrorHandler serviceThreadErrorHandler_;

	ConnectHandler connectHandler_;
	DisconnectHandler disconnectHandler_;
	LoginHandler loginHandler_;
	LogoutHandler logoutHandler_;
	GetPartitionAddressHandler getPartitionAddressHandler_;
	GetPartitionContainerNamesHandler getPartitionContainerNamesHandler_;
	GetContainerPropertiesHandler getContainerPropertiesHandler_;
	PutContainerHandler putContainerHandler_;
	PutLargeContainerHandler putLargeContainerHandler_;
	UpdateContainerStatusHandler updateContainerStatusHandler_;
	CreateLargeIndexHandler createLargeIndexHandler_;
	DropContainerHandler dropContainerHandler_;
	GetContainerHandler getContainerHandler_;
	CreateIndexHandler createIndexHandler_;
	DropIndexHandler dropIndexHandler_;
	CreateDropTriggerHandler createDropTriggerHandler_;
	FlushLogHandler flushLogHandler_;
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
	QueryGeometryRelatedHandler queryGeometryRelatedHandler_;
	QueryGeometryWithExclusionHandler queryGeometryWithExclusionHandler_;
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
	ReplicationLog2Handler replicationLog2Handler_;
	ReplicationAck2Handler replicationAck2Handler_;
	AuthenticationHandler authenticationHandler_;
	AuthenticationAckHandler authenticationAckHandler_;
	PutUserHandler putUserHandler_;
	DropUserHandler dropUserHandler_;
	GetUsersHandler getUsersHandler_;
	PutDatabaseHandler putDatabaseHandler_;
	DropDatabaseHandler dropDatabaseHandler_;
	GetDatabasesHandler getDatabasesHandler_;
	PutPrivilegeHandler putPrivilegeHandler_;
	DropPrivilegeHandler dropPrivilegeHandler_;
	CheckTimeoutHandler checkTimeoutHandler_;

	UnsupportedStatementHandler unsupportedStatementHandler_;
	UnknownStatementHandler unknownStatementHandler_;
	IgnorableStatementHandler ignorableStatementHandler_;

	DataStorePeriodicallyHandler dataStorePeriodicallyHandler_;
	AdjustStoreMemoryPeriodicallyHandler adjustStoreMemoryPeriodicallyHandler_;
	KeepLogHandler keepLogHandler_;

	BackgroundHandler backgroundHandler_;
	ContinueCreateDDLHandler createDDLContinueHandler_;

	UpdateDataStoreStatusHandler updateDataStoreStatusHandler_;

	ShortTermSyncHandler shortTermSyncHandler_;
	LongTermSyncHandler longTermSyncHandler_;
	SyncCheckEndHandler syncCheckEndHandler_;
	DropPartitionHandler dropPartitionHandler_;
	ChangePartitionStateHandler changePartitionStateHandler_;
	ChangePartitionTableHandler changePartitionTableHandler_;
	RedoLogHandler redoLogHandler_;

	SQLGetContainerHandler sqlGetContainerHandler_;

	ExecuteJobHandler executeHandler_;
	ControlJobHandler controlHandler_;
	SendEventHandler sendEventHandler_;
	TransactionManager *transactionManager_;

	SyncManager *syncManager_;
	DataStoreV4 *dataStore_;
	CheckpointService *checkpointService_;

	RemoveRowSetByIdHandler removeRowSetByIdHandler_;
	UpdateRowSetByIdHandler updateRowSetByIdHandler_;

public:

	PartitionGroupId getPartitionGroupId(PartitionId pId) {
		return pgConfig_.getPartitionGroupId(pId);
	}

	PartitionGroupId getPartitionGroupCount() {
		return pgConfig_.getPartitionGroupCount();
	}

	ResultSetHolderManager resultSetHolderManager_;
};

class TxnLogManager {
public:
	TxnLogManager(TransactionService *txnSvc, EventContext& ec, PartitionId pId) :
		txnLogAlloc_(*(txnSvc->getTxnLogAlloc(txnSvc->getPartitionGroupId(pId)))),
		eeAlloc_(ec.getAllocator()), logRecordList_(txnLogAlloc_),
		limit_(txnSvc->getWorkMemoryByteSizeLimit()) {
	}
	~TxnLogManager() {
		util::StackAllocator::Tool::forceReset(txnLogAlloc_);
		txnLogAlloc_.setFreeSizeLimit(txnLogAlloc_.base().getElementSize());
		txnLogAlloc_.trim();
	}
	util::XArray<const util::XArray<uint8_t>*>& getLogList() {
		return logRecordList_;
	}

	void addLog(const util::XArray<uint8_t>* logRecord) {
		if (logRecord != NULL) {
			logRecordList_.push_back(logRecord);
		}
	}
	util::StackAllocator& getLogAllocator() const {
		return txnLogAlloc_;
	}
private:
	util::StackAllocator& txnLogAlloc_;
	util::StackAllocator& eeAlloc_;
	util::XArray<const util::XArray<uint8_t>*> logRecordList_;
	size_t limit_;
};


template<StatementMessage::OptionType T>
typename StatementMessage::OptionCoder<T>::ValueType
StatementMessage::OptionSet::get() const {
	typedef OptionCoder<T> Coder;
	typedef typename util::IsSame<
			typename Coder::ValueType, typename Coder::StorableType>::Type Same;
	EntryMap::const_iterator it = entryMap_.find(T);

	if (it == entryMap_.end()) {
		return Coder().getDefault();
	}

	return toValue<Coder>(
			it->second.get<typename Coder::StorableType>(), Same());
}

template<StatementMessage::OptionType T>
void StatementMessage::OptionSet::set(
		const typename OptionCoder<T>::ValueType &value) {
	typedef OptionCoder<T> Coder;
	typedef typename util::IsSame<
			typename Coder::ValueType, typename Coder::StorableType>::Type Same;
	ValueStorage storage;
	storage.set(toStorable<Coder>(value, Same()), getAllocator());
	entryMap_[T] = storage;
}

template<StatementMessage::OptionType T>
void StatementHandler::updateRequestOption(
		util::StackAllocator &alloc, Event &ev,
		const typename Message::OptionCoder<T>::ValueType &value) {
	Request request(alloc, getRequestSource(ev));
	util::XArray<uint8_t> remaining(alloc);

	try {
		EventByteInStream in(ev.getInStream());
		request.decode<EventByteInStream>(in);

		decodeBinaryData<EventByteInStream>(in, remaining, true);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}

	try {
		request.optional_.set<T>(value);
		ev.getMessageBuffer().clear();

		EventByteOutStream out(ev.getOutStream());
		request.encode<EventByteOutStream>(out);

		encodeBinaryData<EventByteOutStream>(out, remaining.data(), remaining.size());
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

/*!
	@brief Decodes binary data
*/
template <typename S>
void StatementHandler::decodeBinaryData(
	S& in,
	util::XArray<uint8_t>& binaryData, bool readAll) {
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
	catch (std::exception& e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}


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

inline void TransactionService::incrementBackgroundOperationCount(PartitionId pId) {
	++backgroundOperationCount_[pId];
}
inline void TransactionService::incrementNoExpireOperationCount(PartitionId pId) {
	++noExpireOperationCount_[pId];
}
inline void TransactionService::incrementAbortDDLCount(PartitionId pId) {
	++abortDDLCount_[pId];
}


inline bool TransactionService::onBackgroundTask(PartitionGroupId pgId) {
	return onBackgroundTask_[pgId];
}

inline void TransactionService::setBackgroundTask(PartitionGroupId pgId, bool onBackgroundTask) {
	onBackgroundTask_[pgId] = onBackgroundTask;
}

inline util::StackAllocator* TransactionService::getTxnLogAlloc(PartitionGroupId pgId) {
	return txnLogAlloc_[pgId];
}


#endif
