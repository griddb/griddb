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
	@brief Implementation of LoginHandler
*/
#include "transaction_service.h"

EventMonotonicTime userCacheUpdateInterval = 60;

#define TEST_PRINT(s)
#define TEST_PRINT1(s, d)

#include "sql_service.h"

UTIL_TRACER_DECLARE(AUTH_OPERATION);
UTIL_TRACER_DECLARE(CONNECTION_DETAIL);

AUDIT_TRACER_DECLARE(AUDIT_CONNECT);

#define TXN_THROW_DENY_ERROR(errorCode, message) \
	GS_THROW_CUSTOM_ERROR(DenyException, errorCode, message)

#define TXN_TRACE_HANDLER_CALLED(ev)               \
	UTIL_TRACE_DEBUG(TRANSACTION_SERVICE,          \
		"handler called. (type=" <<  EventTypeUtility::getEventTypeName(ev.getType()) \
							<< "[" << ev.getType() << "]" \
							<< ", nd=" << ev.getSenderND() \
							<< ", pId=" << ev.getPartitionId() << ")")


template<>
template<typename S>
SQLTableInfo* StatementMessage::CustomOptionCoder<SQLTableInfo*, 0>::decode(
	S& in, util::StackAllocator& alloc) const {
	return SQLExecution::decodeTableInfo(alloc, in);
}

template<>
template<>
void StatementMessage::CustomOptionCoder<
	StatementMessage::CompositeIndexInfos*, 0>::encode(
		EventByteOutStream& out, const ValueType& value) const;
template<>
template<>
void StatementMessage::CustomOptionCoder<
	StatementMessage::CompositeIndexInfos*, 0>::encode(
		OutStream& out, const ValueType& value) const;

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


template<>
template<>
inline void StatementMessage::CustomOptionCoder<
	StatementMessage::CompositeIndexInfos*, 0>::encode(
		EventByteOutStream& out, const ValueType& value) const {
	assert(value != NULL);
	const size_t size = value->indexInfoList_.size();
	out << static_cast<uint64_t>(size);
	for (size_t pos = 0; pos < size; pos++) {
		StatementHandler::encodeIndexInfo(out, value->indexInfoList_[pos]);
	}
}
template<>
template<>
inline void StatementMessage::CustomOptionCoder<
	StatementMessage::CompositeIndexInfos*, 0>::encode(
		OutStream& out, const ValueType& value) const {
	assert(value != NULL);
	const size_t size = value->indexInfoList_.size();
	out << static_cast<uint64_t>(size);
	for (size_t pos = 0; pos < size; pos++) {
		StatementHandler::encodeIndexInfo(out, value->indexInfoList_[pos]);
	}
}



void LoginHandler::checkClusterName(std::string &clusterName) {
	if (clusterName.size() > 0) {
		const std::string currentClusterName =
			clusterService_->getManager()->getClusterName();
		if (clusterName.compare(currentClusterName) != 0) {
			TXN_THROW_DENY_ERROR(GS_ERROR_TXN_CLUSTER_NAME_INVALID,
				"cluster name invalid (input="
					<< clusterName << ", current=" << currentClusterName
					<< ")");
		}
	}
}

void LoginHandler::checkApplicationName(const char8_t *applicationName) {
	if (strlen(applicationName) != 0) {
		try {
			NoEmptyKey::validate(
					KeyConstraint::getUserKeyConstraint(MAX_APPLICATION_NAME_LEN),
					applicationName, static_cast<uint32_t>(strlen(applicationName)),
					"applicationName");
		}
		catch (UserException &e) {
			GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e,
					"Application name invalid (input=" <<
					applicationName << ")"));
		}
	}
}

bool LoginHandler::checkPublicDB(const char8_t *dbName) {
	if (strcmp(dbName, GS_PUBLIC) == 0) {
		return true;
	}
	return false;
}

bool LoginHandler::checkSystemDB(const char8_t *dbName) {
	if (strcmp(dbName, GS_SYSTEM) == 0) {
		return true;
	}
	return false;
}

bool LoginHandler::checkAdmin(util::String &userName) {
	if ((strcmp(userName.c_str(), GS_ADMIN_USER) == 0) ||
		(strcmp(userName.c_str(), GS_SYSTEM_USER) == 0) ||
		(userName.find('#') != std::string::npos)) {
			return true;
	}
	return false;
}

bool LoginHandler::checkLocalAuthNode() {
	NodeAddress nodeAddress =
			clusterManager_->getPartition0NodeAddr();
	TEST_PRINT1(
			"NodeAddress(TxnServ) %s\n", nodeAddress.dump().c_str());
	if (nodeAddress.port_ == 0) {
		TXN_THROW_DENY_ERROR(
				GS_ERROR_TXN_AUTHENTICATION_SERVICE_NOT_READY, "");
	}

	const NodeDescriptor &nd0 =
		transactionService_->getEE()->getServerND(
			util::SocketAddress(nodeAddress.toString(false).c_str(),
				nodeAddress.port_));

	if (nd0.isSelf() && !isNewSQL_) {
		return true;
	} else {
		return false;
	}
}

bool LoginHandler::checkUserCache(EventContext &ec, LoginContext &login) {
	UNUSED_VARIABLE(ec);

	UTIL_TRACE_DEBUG(AUTH_OPERATION, "checkUserCache()");
	TEST_PRINT("checkUserCache() S\n");

	bool status = false;

	UserCache *uc = transactionService_->userCache_;
	if (uc == NULL) {
		return false;
	}
	GlobalVariableSizeAllocator& varAlloc = uc->getAllocator();

	UserString dbUserName(varAlloc);
	dbUserName.append(login.dbName_);
	dbUserName.append(":");
	dbUserName.append(login.userName_);

	UserValue *value = ALLOC_VAR_SIZE_NEW(varAlloc) UserValue(varAlloc);
	value->isLDAPAuthenticate_ = login.isLDAPAuthentication_;
	value->time_ = login.ec_.getHandlerStartMonotonicTime();
	char digest[SHA256_DIGEST_STRING_LENGTH + 1];
	if (login.isLDAPAuthentication_) {
		SHA256_Data(reinterpret_cast<const uint8_t*>(login.digest_),
					strlen(login.digest_), digest);
		digest[SHA256_DIGEST_STRING_LENGTH] = '\0';
		value->digest_ = digest;
	} else {
		value->digest_ = login.digest_;
	}
	
	int ret = uc->checkAndGet(dbUserName, value);
	TEST_PRINT1("ret=%d\n", ret);
	switch (ret) {
	case 0://認証成功
		login.dbId_ = value->dbId_;
		login.priv_ = value->priv_;
		login.roleName_ = value->roleName_.c_str();
		status = true;
		break;
	case 1://認証失敗
		GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED, "");
	default://2 キャッシュに存在しない
		status = false;
		break;
	}
	ALLOC_VAR_SIZE_DELETE(varAlloc, value);
	return status;
}

void LoginHandler::addUserCache(
		EventContext &ec, const char8_t *dbName, const char8_t *userName,
		const char8_t *digest,
		DatabaseId dbId, EventMonotonicTime authTime, PrivilegeType priv,
		bool isLDAPAuthentication, const char8_t *roleName) {
	UNUSED_VARIABLE(ec);

	TEST_PRINT("addUserCache() S\n");

	UserCache *uc = transactionService_->userCache_;
	if (uc == NULL) {
		return;
	}

	TransactionManager* txnMgr = transactionService_->getManager();
	if ((isLDAPAuthentication == false) && (txnMgr->isLDAPAuthentication() == true)) {
		return;
	}

	GlobalVariableSizeAllocator& varAlloc = uc->getAllocator();
	
	UserString dbUserName(varAlloc);
	dbUserName.append(dbName);
	dbUserName.append(":");
	dbUserName.append(userName);

	UserValue *value = ALLOC_VAR_SIZE_NEW(varAlloc) UserValue(varAlloc);
	value->userName_= userName;
	value->digest_ = digest;
	value->dbName_= dbName;
	value->dbId_ = dbId;
	value->priv_ = priv;
	value->time_ = authTime;
	value->isLDAPAuthenticate_ = isLDAPAuthentication;
	char digest0[SHA256_DIGEST_STRING_LENGTH + 1];
	if (isLDAPAuthentication) {
		SHA256_Data(reinterpret_cast<const uint8_t*>(digest),
					strlen(digest), digest0);
		digest0[SHA256_DIGEST_STRING_LENGTH] = '\0';
		value->digest_ = digest0;
		
		value->roleName_ = roleName;
	} else {
		value->digest_ = digest;
	}

	uc->put(dbUserName, value, true);

	TEST_PRINT("addUserCache() E\n");
}
		
void LoginHandler::releaseUserCache(ConnectionOption &connOption) {
	TEST_PRINT("releaseCache() S\n");
	
	if (connOption.isAdminAndPublicDB_==false && (connOption.dbName_.empty()==false)) {
		UserCache *uc = transactionService_->userCache_;
		if (uc) {
			GlobalVariableSizeAllocator& varAlloc = uc->getAllocator();

			UserString dbUserName(varAlloc);
			dbUserName.append(connOption.dbName_.c_str());
			dbUserName.append(":");
			dbUserName.append(connOption.userName_.c_str());

			uc->release(dbUserName);
		}
	}
	TEST_PRINT("releaseCache() E\n");
}

void LoginHandler::authorization(util::StackAllocator &alloc, LoginContext &login,
	util::XArray<const char8_t *> &roleNameList) {
	UTIL_TRACE_DEBUG(AUTH_OPERATION, "authorization()");
	TEST_PRINT("authorization() S\n");
		
	EventContext &ec = login.ec_;
	Event &ev = login.ev_;
	Request &request = login.request_;
	const Response &response = login.response_;
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	const char8_t *dbName = login.dbName_;
	const char8_t *userName = login.userName_;
	const char8_t *digest = login.digest_;

		

	util::String empty(alloc);

	if (checkPublicDB(dbName)) {
		if (checkLocalAuthNode()) {
			TEST_PRINT("PUBLICDB-LOCAL\n");
			DatabaseId dbId = UNDEF_DBID;
			PrivilegeType priv = READ;
			const char *roleName = NULL;

			for (uint32_t i = 0; i < static_cast<uint32_t>(roleNameList.size()); i++) {
				DatabaseId tmpDbId = UNDEF_DBID;
				PrivilegeType tmpPriv = READ;
				if (executeAuthenticationInternal(
						ec, alloc, request.fixed_.cxtSrc_,
						roleNameList[i], empty.c_str(),
						dbName, Message::USER_NORMAL, 5,
						tmpDbId, tmpPriv)) { 

					if (dbId==UNDEF_DBID){
						dbId = tmpDbId;
						priv = tmpPriv;
						roleName = roleNameList[i];
					} else {
						if (tmpPriv==ALL) {
							priv = ALL;
							roleName = roleNameList[i];
						}
					}
				}
			}
			if (dbId == UNDEF_DBID) {
				GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED, "");
			}
			
			ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();
			connOption.setAfterAuth(dbId, emNow, ALL, roleName);
			addUserCache(ec, dbName, userName, digest, dbId, emNow, priv, true, roleName);
			TEST_PRINT("AUTH_SUCCESS");
			TEST_PRINT1(" roleName=%s\n", roleName);
			TEST_PRINT1(" priv=%d\n", priv);
			GS_TRACE_INFO(AUTH_OPERATION, GS_TRACE_TXN_AUTH_STATUS, "Successfully authenticated: userName="
				<< connOption.userName_.c_str() << " dbName=" << connOption.dbName_.c_str()); 
			TEST_PRINT("LoginHandler::operator() authLDAP & checkPublicDB E\n");
			replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response,
				NO_REPLICATION);

		} else {
			TEST_PRINT("PUBLICDB-REMOTE\n");
			
			executeAuthentication(
					ec, ev, request.fixed_.cxtSrc_.stmtId_,
					empty.c_str(), empty.c_str(),
					dbName, Message::USER_NORMAL, roleNameList);
			
		}
	} else {
		if (checkLocalAuthNode()) {
			TEST_PRINT("NORMALDB-LOCAL\n");
			DatabaseId dbId = UNDEF_DBID;
			PrivilegeType priv = READ;
			const char *roleName = NULL;

			for (uint32_t i = 0; i < static_cast<uint32_t>(roleNameList.size()); i++) {
				DatabaseId tmpDbId = UNDEF_DBID;
				PrivilegeType tmpPriv = READ;
				if (executeAuthenticationInternal(
						ec, alloc, request.fixed_.cxtSrc_,
						roleNameList[i], empty.c_str(),
						dbName, Message::USER_NORMAL, 7,
						tmpDbId, tmpPriv)) { 

					if (dbId==UNDEF_DBID){
						dbId = tmpDbId;
						priv = tmpPriv;
						roleName = roleNameList[i];
					} else {
						if (tmpPriv==ALL) {
							priv = ALL;
							roleName = roleNameList[i];
						}
					}
				}
			}
			if (dbId == UNDEF_DBID) {
				GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED, "");
			}
			
			ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();
			connOption.setAfterAuth(dbId, emNow, priv, roleName);
			addUserCache(ec, dbName, userName, digest, dbId, emNow, priv, true, roleName);
			TEST_PRINT("AUTH_SUCCESS");
			TEST_PRINT1(" roleName=%s\n", roleName);
			TEST_PRINT1(" priv=%d\n", priv);
			GS_TRACE_INFO(AUTH_OPERATION, GS_TRACE_TXN_AUTH_STATUS, "Successfully authenticated: userName="
				<< connOption.userName_.c_str() << " dbName=" << connOption.dbName_.c_str()); 
			replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response,
				NO_REPLICATION);

		} else {
			TEST_PRINT("NORMALDB-REMOTE\n");
			
			executeAuthentication(
					ec, ev, request.fixed_.cxtSrc_.stmtId_,
					empty.c_str(), empty.c_str(),
					dbName, Message::USER_NORMAL, roleNameList);
			
		}
	}
	TEST_PRINT("authorization() E\n");
}

void LoginHandler::authAdmin(util::StackAllocator &alloc, LoginContext &login) {
	UTIL_TRACE_DEBUG(AUTH_OPERATION, "authAdmin()");
	TEST_PRINT("authAdmin() S\n");

	EventContext &ec = login.ec_;
	Event &ev = login.ev_;
	Request &request = login.request_;
	const Response &response = login.response_;
	const bool systemMode = login.systemMode_;
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
	const char8_t *dbName = login.dbName_;
	const char8_t *userName = login.userName_;
	const char8_t *digest = login.digest_;

	DatabaseId dbId = UNDEF_DBID;
	PrivilegeType priv = READ;

	ConnectionOption &connOption =
			ev.getSenderND().getUserData<ConnectionOption>();

	if (checkPublicDB(dbName) || checkSystemDB(dbName)) {
		if (systemService_->getUserTable().isValidUser(userName, digest)) {
			UserType userType;
			userType = Message::USER_ADMIN;
			if (checkSystemDB(dbName)) {
				GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED,
						"database name invalid (" << dbName << ")");
			}

			connOption.setBeforeAuth(userType, true);

			if (checkPublicDB(dbName)) {
				dbId = GS_PUBLIC_DB_ID;
			}
			else {
				dbId = GS_SYSTEM_DB_ID;
			}
			connOption.setAfterAuth(dbId, emNow, ALL, NULL);
			GS_TRACE_INFO(AUTH_OPERATION, GS_TRACE_TXN_AUTH_STATUS, "Successfully authenticated: userName="
				<< connOption.userName_.c_str() << " dbName=" << connOption.dbName_.c_str()); 
			replySuccess(
					ec, alloc, ev.getSenderND(), ev.getType(),
					TXN_STATEMENT_SUCCESS, request, response,
					NO_REPLICATION);
			TEST_PRINT("LoginHandler::operator() public&admin  [A1] E\n");
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED,
					"invalid user name or password (user name = " <<
					userName << ")");
		}
	}
	else { 
		bool isLocalAuthNode = checkLocalAuthNode();
		if (systemService_->getUserTable().isValidUser(userName, digest)) {
			TEST_PRINT("LoginHandler::operator() admin S\n");
			if (isLocalAuthNode) {
				TEST_PRINT("Self\n");
				TEST_PRINT("[A4] S\n");

				UserType userType;
				userType = Message::USER_ADMIN;
				connOption.setBeforeAuth(userType, false);
				
				if (executeAuthenticationInternal(
						ec, alloc,
						request.fixed_.cxtSrc_, userName, digest,
						dbName, userType, 2,
						dbId, priv) == false) {  
					GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED, "");
				}
					
				connOption.setAfterAuth(dbId, emNow, priv, NULL);
				GS_TRACE_INFO(AUTH_OPERATION, GS_TRACE_TXN_AUTH_STATUS, "Successfully authenticated: userName="
					<< connOption.userName_.c_str() << " dbName=" << connOption.dbName_.c_str()); 
				replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
					TXN_STATEMENT_SUCCESS, request, response,
					NO_REPLICATION);
				TEST_PRINT("[A4] E\n");
			}
			else {
				TEST_PRINT("Other\n");
				TEST_PRINT("[A5] S\n");

				UserType userType;
				userType = Message::USER_ADMIN;
				connOption.setBeforeAuth(userType, false);

				util::XArray<const char8_t *> dummyList(alloc);
				executeAuthentication(
						ec, ev, request.fixed_.cxtSrc_.stmtId_,
						userName, digest,
						dbName, userType, dummyList);

			}
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED,
				"invalid user name or password (user name = "
					<< userName << ")");
		}
	}
	TEST_PRINT("authAdmin() E\n");
}

void LoginHandler::authInternalUser(util::StackAllocator &alloc, LoginContext &login) {
	UTIL_TRACE_DEBUG(AUTH_OPERATION, "authInternalUser()");
	EventContext &ec = login.ec_;
	Event &ev = login.ev_;
	Request &request = login.request_;
	const Response &response = login.response_;
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
	const char8_t *dbName = login.dbName_;
	const char8_t *userName = login.userName_;
	const char8_t *digest = login.digest_;

	DatabaseId dbId = UNDEF_DBID;
	PrivilegeType priv = READ;
	
	ConnectionOption &connOption =
			ev.getSenderND().getUserData<ConnectionOption>();

	if (checkPublicDB(dbName)) {

		if (checkLocalAuthNode()) {
			TEST_PRINT("Self\n");
			TEST_PRINT("[A2] S\n");
			
			connOption.setBeforeAuth(Message::USER_NORMAL, true);

			if (executeAuthenticationInternal(
					ec, alloc, request.fixed_.cxtSrc_,
					userName, digest,
					dbName, Message::USER_NORMAL, 1,
					dbId, priv) == false) { 
				GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED, "");
			}
				
			connOption.setAfterAuth(dbId, emNow, priv, NULL);
			addUserCache(ec, dbName, userName, digest, dbId, emNow, priv, false, NULL);
			GS_TRACE_INFO(AUTH_OPERATION, GS_TRACE_TXN_AUTH_STATUS, "Successfully authenticated: userName="
				<< connOption.userName_.c_str() << " dbName=" << connOption.dbName_.c_str()); 
			replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response,
				NO_REPLICATION);
			TEST_PRINT("[A2] E\n");
		}
		else {
			TEST_PRINT("Other\n");
			TEST_PRINT("[A3] S\n");

			connOption.setBeforeAuth(Message::USER_NORMAL, false);

			util::XArray<const char8_t *> dummyList(alloc);
			executeAuthentication(
					ec, ev, request.fixed_.cxtSrc_.stmtId_,
					userName, digest,
					dbName, connOption.userType_, dummyList);

		}
		TEST_PRINT("LoginHandler::operator() public&normal E\n");
	}
	else if (checkSystemDB(dbName)) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED,
			"database name invalid (" << dbName << ")");
	}
	else {

		TEST_PRINT("LoginHandler::operator() normal S\n");

		if (checkLocalAuthNode()) {
			TEST_PRINT("Self\n");
			TEST_PRINT("[A6] S\n");
			
			connOption.setBeforeAuth(Message::USER_NORMAL, false);

			if (executeAuthenticationInternal(
					ec, alloc, request.fixed_.cxtSrc_,
					userName, digest,
					dbName, Message::USER_NORMAL, 3,
					dbId, priv) == false) {  
				GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED, "");
			}
				
			connOption.setAfterAuth(dbId, emNow, priv, NULL);
			addUserCache(ec, dbName, userName, digest, dbId, emNow, priv, false, NULL);
			GS_TRACE_INFO(AUTH_OPERATION, GS_TRACE_TXN_AUTH_STATUS, "Successfully authenticated: userName="
				<< connOption.userName_.c_str() << " dbName=" << connOption.dbName_.c_str()); 
			replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
				TXN_STATEMENT_SUCCESS, request, response,
				NO_REPLICATION);
			TEST_PRINT("[A6] E\n");
		}
		else {
			TEST_PRINT("Other\n");
			TEST_PRINT("[A7] S\n");
			
			connOption.setBeforeAuth(Message::USER_NORMAL, false);

			util::XArray<const char8_t *> dummyList(alloc);
			executeAuthentication(
					ec, ev, request.fixed_.cxtSrc_.stmtId_,
					userName, digest,
					dbName, connOption.userType_, dummyList);

		}

		TEST_PRINT("LoginHandler::operator() normal E\n");
	}
}

/*!
	@brief Handler Operator
*/
void LoginHandler::operator()(EventContext& ec, Event& ev) {
	TXN_TRACE_HANDLER_CALLED(ev);
	UTIL_TRACE_DEBUG(AUTH_OPERATION, "LoginHandler::operator()");
	TEST_PRINT("<<<LoginHandler>>> START\n");

	util::StackAllocator& alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	Request request(alloc, getRequestSource(ev));
	Response response(alloc);

	try {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();

		{

		sqlService_->getExecutionManager()->clearConnection(ev, isNewSQL_);

		EventByteInStream in(ev.getInStream());
		decodeRequestCommonPart(in, request, connOption);
		EVENT_START(ec, ev, transactionManager_, connOption.dbId_, (ev.getSenderND().getId() < 0));

		const RequestType requestType =
				request.optional_.get<Options::REQUEST_MODULE_TYPE>();
		const char8_t *dbName = request.optional_.get<Options::DB_NAME>();
		const bool systemMode = request.optional_.get<Options::SYSTEM_MODE>();
		const int32_t txnTimeoutInterval =
				(request.optional_.get<Options::TXN_TIMEOUT_INTERVAL>() >= 0 ?
				request.optional_.get<Options::TXN_TIMEOUT_INTERVAL>() :
				TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL);
		const char8_t *applicationName =
				request.optional_.get<Options::APPLICATION_NAME>();
		const double storeMemoryAgingSwapRate =
				request.optional_.get<Options::STORE_MEMORY_AGING_SWAP_RATE>();
		const util::TimeZone timeZone =
				request.optional_.get<Options::TIME_ZONE_OFFSET>();
		const int8_t authType = request.optional_.get<Options::AUTHENTICATION_TYPE>();
		const int32_t acceptableFeatureVersion =
				request.optional_.get<Options::ACCEPTABLE_FEATURE_VERSION>();

		util::String userName(alloc);
		util::String digest(alloc);
		std::string clusterName;
		int32_t stmtTimeoutInterval;
		bool isImmediateConsistency;

		decodeStringData<util::ByteStream<util::ArrayInStream>, util::String>(in, userName);
		decodeStringData<util::ByteStream<util::ArrayInStream>, util::String>(in, digest);
		decodeIntData<util::ByteStream<util::ArrayInStream>, int32_t>(in,
			stmtTimeoutInterval);  
		decodeBooleanData(in, isImmediateConsistency);
		if (in.base().remaining() > 0) {
			decodeStringData<util::ByteStream<util::ArrayInStream>, std::string>(in, clusterName);
		}
		response.connectionOption_ = &connOption;
		TEST_PRINT("[RequestMesg]\n");
		TEST_PRINT1("userName=%s\n", userName.c_str());
		TEST_PRINT1("digest=%s\n", digest.c_str());
		TEST_PRINT1("stmtTimeoutInterval=%d\n", stmtTimeoutInterval);
		TEST_PRINT1("isImmediateConsistency=%d\n", isImmediateConsistency);
		TEST_PRINT1("clusterName=%s\n", clusterName.c_str());

		TEST_PRINT1("requestType=%d\n", requestType);
		TEST_PRINT1("dbName=%s\n", dbName);
		TEST_PRINT1("systemMode=%d\n", systemMode);

		TEST_PRINT1("applicationName=%s\n", applicationName);
		TEST_PRINT1("authType=%d\n", authType);
		
		UTIL_TRACE_DEBUG(AUTH_OPERATION, "userName=" << userName.c_str() << " dbName=" << dbName);

		bool usePublic = usePublicConection(request, false);
		const char* serviceType = isNewSQL_ ? "SQL" : "Transaction";

		if (usePublic) {
			connOption.setPublicConnection();
			isNewSQL_ ? sqlService_->setTotalExternalConnectionCount() : transactionService_->setTotalExternalConnectionCount();
			GS_TRACE_INFO(CONNECTION_DETAIL, GS_TRACE_TXN_CONNECTION_STATUS, "operation=connect,"
				<< "serviceType=" << serviceType << ",userName=" << userName.c_str()
				<< ",source=" << ev.getSenderND() << ",connection=PUBLIC");
		}
		else {
			isNewSQL_ ? sqlService_->setTotalInternalConnectionCount() : transactionService_->setTotalInternalConnectionCount();
			GS_TRACE_DEBUG(CONNECTION_DETAIL, GS_TRACE_TXN_CONNECTION_STATUS, "operation=connect,"
				<< "serviceType=" << serviceType << ",userName=" << userName.c_str()
				<< ",source=" << ev.getSenderND());
		}

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_ANY;
		const PartitionStatus partitionStatus = PSTATE_ANY;
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		checkClusterName(clusterName);
		checkApplicationName(applicationName);

		TransactionManager* txnMgr = transactionService_->getManager();
			
		bool isLDAPAuthentication = txnMgr->isLDAPAuthentication();
		if ((authType == 1) && (isLDAPAuthentication == false)) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_LDAP_FAILED, "");
		} else if ((authType == 0) && (isLDAPAuthentication == true)) {
			isLDAPAuthentication = false;
		}
		TEST_PRINT1("isLDAPAuthentication=%d\n", isLDAPAuthentication);

		connOption.setFirstStep(
				request.fixed_.clientId_, storeMemoryAgingSwapRate, timeZone,
				isLDAPAuthentication, acceptableFeatureVersion);
		connOption.setSecondStep(userName.c_str(), digest.c_str(), dbName, applicationName,
					isImmediateConsistency, txnTimeoutInterval, requestType);

		LoginContext login(ec, ev, request, response, systemMode);

		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.fixed_.pId_).mutex());

		if (checkAdmin(userName)) {
			authAdmin(alloc, login);

		} else {
			if (checkUserCache(ec, login)) {

				connOption.setBeforeAuth(Message::USER_NORMAL, false);

				connOption.setAfterAuth(login.dbId_, emNow, login.priv_, login.roleName_.c_str());
				GS_TRACE_INFO(AUTH_OPERATION, GS_TRACE_TXN_AUTH_STATUS, "Successfully authenticated: userName="
					<< connOption.userName_.c_str() << " dbName=" << connOption.dbName_.c_str()); 
				TEST_PRINT("LoginHandler::operator() checkUserCache=true E\n");
				replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
					TXN_STATEMENT_SUCCESS, request, response,
					NO_REPLICATION);
			} else {
				{
					authInternalUser(alloc, login);
				}
			}
		}

		}
		TEST_PRINT("<<<LoginHandler>>> END\n");
	}
	catch (DenyException &e) {
		handleError(ec, alloc, ev, request, e);
	}
	catch (std::exception &e) {
		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();
		GS_TRACE_INFO(AUTH_OPERATION, GS_TRACE_TXN_AUTH_STATUS, "Failed authentication: userName="
			<< connOption.userName_.c_str() << " dbName=" << connOption.dbName_.c_str()); 
		handleError(ec, alloc, ev, request, e);
	}
}



bool LoginHandler::executeAuthenticationInternal(
		EventContext &ec, util::StackAllocator &alloc,
		TransactionManager::ContextSource &cxtSrc,
		const char8_t *userName, const char8_t *digest, const char8_t *dbName,
		UserType userType, int checkLevel, DatabaseId &dbId, PrivilegeType &priv) {
	TEST_PRINT("executeAuthenticationInternal() S\n");
	TEST_PRINT1("checkLevel(d)=%d\n",checkLevel);
			
	if (checkPublicDB(dbName)) {
		dbId = GS_PUBLIC_DB_ID;
		priv = ALL;
	}
	else if (checkSystemDB(dbName)) {
		dbId = GS_SYSTEM_DB_ID;
	}
	else {
		dbId = UNDEF_DBID;
	}

	if ((checkLevel & 0x01) && (userType == Message::USER_NORMAL)) {
		TEST_PRINT("executeAuthenticationInterval() (Ph1) S\n");

		const util::StackAllocator::Scope scope(alloc);
		RowKeyData rowKey(alloc);
		makeRowKey(userName, rowKey);
		
		if (checkLevel & 0x04) { 
			TEST_PRINT("Authorization\n");
			DUGetInOut option;
			if (runWithRowKey(ec, cxtSrc, GS_USERS, rowKey, &option) == false) {
				return false;
			}
			if (option.count != 1) {
				TEST_PRINT("count!=1(userName)\n");
				return false;
			}
		}
		else {
			TEST_PRINT("Internal\n");
			DUGetInOut option;
			option.setForAuth(digest);
			if (runWithRowKey(ec, cxtSrc, GS_USERS, rowKey, &option) == false) {
				return false;
			}
		}
		TEST_PRINT("executeAuthenticationInterval() (Ph1) E\n");
	}
	if (checkLevel & 0x02) {
		TEST_PRINT("executeAuthenticationInterval() (Ph2) S\n");

		const util::StackAllocator::Scope scope(alloc);
		
		util::String dbUserName(alloc);
		dbUserName.append(dbName);
		dbUserName.append(":");
		if (userType == Message::USER_NORMAL) {
			dbUserName.append(userName);
		}
		else {
			dbUserName.append(
				"admin");  
		}

		RowKeyData rowKey(alloc);
		makeRowKey(dbUserName.c_str(), rowKey);
		
		{
			DUGetInOut option;
			option.setForDbId(dbName, userType);
			if (runWithRowKey(ec, cxtSrc, GS_DATABASES, rowKey, &option) == false) {
				return false;
			}
			dbId = option.dbId;
			TEST_PRINT1("dbId=%ld\n", dbId);
		}
		{
			DUGetInOut option;
			option.setForRole();
			runWithRowKey(ec, cxtSrc, GS_DATABASES, rowKey, &option);
			priv = option.priv;
			TEST_PRINT1("priv=%d\n", priv);
		}
		TEST_PRINT("executeAuthenticationInterval() (Ph2) E\n");
	}
	if ((checkLevel & 0x02) && (userType == Message::USER_NORMAL)) {
		TEST_PRINT("executeAuthenticationInterval() (Ph3) S\n");

		const util::StackAllocator::Scope scope(alloc);

		util::String dbUserName(alloc);
		dbUserName.append(dbName);
		dbUserName.append(":admin");

		RowKeyData rowKey(alloc);
		makeRowKey(dbUserName.c_str(), rowKey);

		{
			DUGetInOut option;
			option.setForDbId(dbName, Message::USER_ADMIN);
			if (runWithRowKey(ec, cxtSrc, GS_DATABASES, rowKey, &option) == false) {
				return false;
			}
			dbId = option.dbId;
			TEST_PRINT1("dbId=%ld\n", dbId);
		}
		TEST_PRINT("executeAuthenticationInterval() (Ph3) E\n");
	}
	TEST_PRINT("executeAuthenticationInternal() E\n");
	return true;
}
void LoginHandler::executeAuthentication(
		EventContext &ec, Event &ev, StatementId authStmtId,
		const char8_t *userName, const char8_t *digest,
		const char8_t *dbName, UserType userType,
		util::XArray<const char8_t *> &roleNameList) {
	TEST_PRINT("executeAuthentication() S\n");
	TEST_PRINT1("ev.pId=%d\n", ev.getPartitionId());

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
	const NodeDescriptor &clientND = ev.getSenderND();

	AuthenticationId authId = 0;

	AuthenticationContext &authContext =
		transactionManager_->putAuth(ev.getPartitionId(),
			authStmtId, clientND, emNow
			, isNewSQL_
			);
	authId = authContext.getAuthenticationId();
	TEST_PRINT1("authId=%ld\n", authId);

	Event authEvent(ec, AUTHENTICATION, 0 /*pId*/);
	EventByteOutStream out = authEvent.getOutStream();
	encodeAuthenticationAckPart(
		out, GS_CLUSTER_MESSAGE_CURRENT_VERSION, authId, ev.getPartitionId());
	TEST_PRINT1("userName=%s\n", userName);
	TEST_PRINT1("digest=%s\n", digest);
	TEST_PRINT1("dbName=%s\n", dbName);
	TEST_PRINT1("userType=%d\n", userType);
	encodeStringData<EventByteOutStream>(out, userName);
	encodeStringData<EventByteOutStream>(out, digest);
	encodeStringData<EventByteOutStream>(out, dbName);
	char8_t byteData = userType;
	out << byteData;

	uint8_t isSQL = 0;
	PartitionId pId = ev.getPartitionId();
	if (isNewSQL_) {
		isSQL = 1;
	}
	out << isSQL;
	out << pId;
	int32_t num = 0;
	if (strlen(digest) == 0) {
		num = static_cast<int32_t>(roleNameList.size());
		TEST_PRINT1("num=%d\n", num);
		out << num;
		for (uint32_t i = 0; i < static_cast<uint32_t>(roleNameList.size()); i++) {
			encodeStringData<EventByteOutStream>(out, roleNameList[i]);
			TEST_PRINT1("roleName=%s\n", roleNameList[i]);
		}
	}

	StatementMessage::OptionSet optionalRequest(alloc);
	optionalRequest.set<StatementMessage::Options::ACCEPTABLE_FEATURE_VERSION>(StatementMessage::FEATURE_V4_3);
	optionalRequest.encode<EventByteOutStream>(out);

	NodeAddress nodeAddress = clusterManager_->getPartition0NodeAddr();
	TEST_PRINT1("NodeAddress(TS) %s\n", nodeAddress.dump().c_str());
	TEST_PRINT1("address =%s\n", nodeAddress.toString(false).c_str());
	const NodeDescriptor &nd =
		transactionService_->getEE()->getServerND(util::SocketAddress(
			nodeAddress.toString(false).c_str(), nodeAddress.port_));

	EventRequestOption sendOption;
	sendOption.timeoutMillis_ = EE_PRIORITY_HIGH;

	transactionService_->getEE()->send(authEvent, nd, &sendOption);

	TEST_PRINT("executeAuthentication() E\n");
}

void AuthenticationHandler::operator()(EventContext &ec, Event &ev) {
	TEST_PRINT("<<<AuthenticationHandler>>> START\n");

	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();

	AuthenticationAck request(ev.getPartitionId());

	TransactionManager::ContextSource cxtSrc;

	try {
		EventByteInStream in(ev.getInStream());

		util::String userName(alloc);
		util::String digest(alloc);
		util::String dbName(alloc);
		char8_t byteData;
		UserType userType;

		decodeAuthenticationAck<EventByteInStream>(in, request);
		TEST_PRINT1("(authId=%ld\n)", request.authId_);
		TEST_PRINT1("(authPId=%d\n)", request.authPId_);
		decodeStringData<EventByteInStream, util::String>(in, userName);
		decodeStringData<EventByteInStream, util::String>(in, digest);
		decodeStringData<EventByteInStream, util::String>(in, dbName);
		in >> byteData;
		userType = static_cast<UserType>(byteData);

		char8_t isSQL;
		in >> isSQL;

		PartitionId pIdForNewSQL;
		in >> pIdForNewSQL;
		util::XArray<const char8_t *> roleNameList(alloc);
		int32_t num = 0;
		if (strlen(digest.c_str()) == 0) {
			in >> num;
			for (int32_t i = 0; i < num; i++) {
				util::String *roleName = ALLOC_NEW(alloc) util::String(alloc);
				decodeStringData<util::ByteStream<util::ArrayInStream>, util::String>(in, *roleName);
				roleNameList.push_back(roleName->c_str());
			}
		}
		StatementMessage::OptionSet optionalRequest(alloc);
		optionalRequest.decode(in);
		TEST_PRINT1("userName=%s\n", userName.c_str());
		TEST_PRINT1("digest=%s\n", digest.c_str());
		TEST_PRINT1("dbName=%s\n", dbName.c_str());
		TEST_PRINT1("userType=%d\n", userType);
		TEST_PRINT1("num=%d\n", num);
		for (int32_t i = 0; i < num; i++) {
			TEST_PRINT1("roleName[%d]=", i);
			TEST_PRINT1("%s\n", roleNameList[i]);
		}

		clusterService_->checkVersion(request.clusterVer_);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_ANY;
		const PartitionStatus partitionStatus = PSTATE_ANY;
		checkExecutable(
			request.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		checkPartitionIdForUsers(request.pId_);

		DatabaseId dbId = UNDEF_DBID;
		PrivilegeType priv = READ;
		const char *pRoleName = NULL;

		util::LockGuard<util::Mutex> guard(
			partitionList_->partition(request.pId_).mutex());
		{
			TEST_PRINT("LOGIN\n");
			try {
				if (strlen(digest.c_str()) == 0) {
					
					int checkLevel;
					if (checkPublicDB(dbName.c_str())) {
						checkLevel = 5;
					} else {
						checkLevel = 7;
					}

					for (uint32_t i = 0; i < static_cast<uint32_t>(roleNameList.size()); i++) {
						DatabaseId tmpDbId = UNDEF_DBID;
						PrivilegeType tmpPriv = READ;
						if (executeAuthenticationInternal(ec, alloc,
								cxtSrc, roleNameList[i], digest.c_str(),
								dbName.c_str(), userType, checkLevel, tmpDbId, tmpPriv)) {
					
							if (dbId==UNDEF_DBID){
								dbId = tmpDbId;
								priv = tmpPriv;
								pRoleName = roleNameList[i];
							} else {
								if (tmpPriv==ALL) {
									priv = ALL;
									pRoleName = roleNameList[i];
								}
							}
						}
					}
					if (dbId == UNDEF_DBID) {
						GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED, "");
					}
				}
				else {
					if (checkPublicDB(dbName.c_str())) {
						if (executeAuthenticationInternal(ec, alloc, cxtSrc,
							userName.c_str(), digest.c_str(), dbName.c_str(),
							userType, 1, dbId, priv) == false) {  
							GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED, "");
						}
					} else {
						if (executeAuthenticationInternal(ec, alloc,
							cxtSrc, userName.c_str(), digest.c_str(),
							dbName.c_str(), userType, 3, dbId, priv) == false) {  
							GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED, "");
						}
					}
				} 
				const int32_t featureVersion = optionalRequest.get<Options::ACCEPTABLE_FEATURE_VERSION>();
				if ((featureVersion < StatementMessage::FEATURE_V4_3) && (priv == READ)) {
						GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED,
							"Unsupported feature requested "
							"(requestedVersion=" << featureVersion << ")");
				}
			}
			catch (std::exception &) {
				dbId = UNDEF_DBID;
			}

			if (isSQL) {
				NodeId nodeId = ClusterService::resolveSenderND(ev);
				const NodeDescriptor &ND = sqlService_->getEE()->getServerND(nodeId);
				request.pId_ = pIdForNewSQL;
				replyAuthenticationAck(ec, alloc, ND, request, dbId, priv, pRoleName, true);
			}
			else {
				replyAuthenticationAck(ec, alloc, ev.getSenderND(), request, dbId, priv, pRoleName);
			}
		}
		TEST_PRINT("<<<AuthenticationHandler>>> END\n");
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(TRANSACTION_SERVICE, e,
			"Failed to accept authentication "
			"(nd="
				<< ev.getSenderND() << ", pId=" << ev.getPartitionId()
				<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}


/*!
	@brief Decodes AuthenticationAck
*/
template<typename S>
void LoginHandler::decodeAuthenticationAck(
		S &in, AuthenticationAck &ack) {


	try {
		in >> ack.clusterVer_;
		in >> ack.authId_;
		in >> ack.authPId_;
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

/*!
	@brief Encodes AuthenticationAck
*/
void LoginHandler::encodeAuthenticationAckPart(
		EventByteOutStream &out, ClusterVersionId clusterVer,
		AuthenticationId authId, PartitionId authPId) {


	try {
		out << clusterVer;
		out << authId;
		out << authPId;
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void AuthenticationAckHandler::replySuccess(
		EventContext &ec, util::StackAllocator &alloc,
		StatementExecStatus status, const Request &request,
		const AuthenticationContext &authContext)  
{
	UNUSED_VARIABLE(status);

#define TXN_TRACE_REPLY_SUCCESS_ERROR(replContext)     \
	"(nd=" << authContext.getConnectionND()            \
		   << ", pId=" << authContext.getPartitionId() \
		   << ", stmtId=" << authContext.getStatementId() << ")"

	TEST_PRINT("replySuccess() START\n");
	util::StackAllocator::Scope scope(alloc);

	if (authContext.getConnectionND().isEmpty()) {
		return;
	}

	TEST_PRINT1("pId=%d\n", authContext.getPartitionId());
	TEST_PRINT1("stmtId=%ld\n", authContext.getStatementId());
	try {
		ConnectionOption* connOption = &(authContext.getConnectionND().getUserData<ConnectionOption>());
		OutMessage outMes(authContext.getStatementId(), TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false), connOption);

		Event ev(ec, LOGIN, authContext.getPartitionId());
		ev.setPartitionIdSpecified(false);
		EventByteOutStream out = ev.getOutStream();
		outMes.encode(out);

		if (!authContext.isSQLService()) {
			ec.getEngine().send(ev, authContext.getConnectionND());
		}
		else {
			sqlService_->getEE()->send(ev, authContext.getConnectionND());
		}

		TEST_PRINT("replySuccess() END\n");
		GS_TRACE_INFO(REPLICATION, GS_TRACE_TXN_REPLY_CLIENT,
			TXN_TRACE_REPLY_SUCCESS_ERROR(replContext));
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

void AuthenticationHandler::replyAuthenticationAck(
		EventContext &ec, util::StackAllocator &alloc,
		const NodeDescriptor &ND, const AuthenticationAck &request,
		DatabaseId dbId, PrivilegeType priv, const char *roleName, bool isNewSQL) {
#define TXN_TRACE_REPLY_ACK_ERROR(request, ND)        \
	"(owner=" << ND << ", authId=" << request.authId_ \
			  << ", pId=" << request.pId_ << ")"

	TEST_PRINT("replyAuthenticationAck() START\n");
	util::StackAllocator::Scope scope(alloc);

	util::String dbName(alloc);  
	util::String privilege(alloc);
	try {
		TEST_PRINT1("request.pId=%d\n", request.pId_);
		TEST_PRINT1("request.authPId=%d\n", request.authPId_);
		Event authAckEvent(ec, AUTHENTICATION_ACK, request.authPId_);
		EventByteOutStream out = authAckEvent.getOutStream();
		encodeAuthenticationAckPart(out, request.clusterVer_, request.authId_,
			request.authPId_);  
		int32_t num = 0;
		if (dbId == UNDEF_DBID) {
			out << num;
		}
		else {
			num = 1;
			out << num;
			if (roleName) {
				dbName.append(roleName);
			}
			out << dbName;  
			out << dbId;
			if (priv == ALL) {
				privilege.append("ALL");
			} else {
				privilege.append("READ");
			}
			out << privilege;
		}
		TEST_PRINT1("num=%d\n", num);
		TEST_PRINT1("dbId=%ld\n", dbId);
		TEST_PRINT1("priv=%d\n", priv);
		TEST_PRINT1("privilege=%s\n", privilege.c_str());
		TEST_PRINT1("roleName=%s\n", dbName.c_str());
		if (isNewSQL) {
			EventRequestOption sendOption;
			sendOption.timeoutMillis_ = EE_PRIORITY_HIGH;

			sqlService_->getEE()->send(authAckEvent, ND, &sendOption);

		}
		else {
			ec.getEngine().send(authAckEvent, ND);
		}

		TEST_PRINT("replyAuthenticationAck() END\n");
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
	@brief Handles error in authentication
*/
void AuthenticationAckHandler::authHandleError(
		EventContext &ec, AuthenticationAck &ack, std::exception &e) {
	UNUSED_VARIABLE(e);

	try {
		TEST_PRINT("authHandleError() S\n");
		AuthenticationContext &authContext = transactionManager_->getAuth(
			ack.pId_, ack.authId_);

		Event ev(ec, LOGIN, ack.pId_);

		try {
			throw;
		}
		catch (DenyException &e) {
			setErrorReply(ev, authContext.getStatementId(), TXN_STATEMENT_DENY,
				e,
				authContext.getConnectionND());  
		}
		catch (std::exception &e) {
			setErrorReply(ev, authContext.getStatementId(), TXN_STATEMENT_ERROR,
				e,
				authContext.getConnectionND());  
		}

		if (!authContext.getConnectionND().isEmpty()) {
			TEST_PRINT("send\n");
			if (!authContext.isSQLService()) {
				ec.getEngine().send(ev, authContext.getConnectionND());
			}
			else {
				sqlService_->getEE()->send(ev, authContext.getConnectionND());
			}
		}

		transactionManager_->removeAuth(ack.pId_, ack.authId_);
		TEST_PRINT("authHandleError() E\n");
	}
	catch (ContextNotFoundException &) {
		TEST_PRINT("authHandleError() ContextNotFoundException\n");
	}
	catch (std::exception &) {
	}
}

/*!
	@brief Handler Operator
*/
void AuthenticationAckHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);
	TEST_PRINT("<<<AuthenticationAckHandler>>> START\n");

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	AuthenticationAck ack(ev.getPartitionId());
	Request request(alloc, getRequestSource(ev));

	try {
		EventByteInStream in(ev.getInStream());
		InMessage inMes(alloc, ack);
		inMes.decode(in);

		DatabaseId dbId = inMes.dbId_;
		PrivilegeType priv = inMes.priv_;
		util::String& dbName = inMes.dbName_;
		
		TEST_PRINT1("(authId=%ld\n)", ack.authId_);
		TEST_PRINT1("(authPId=%d\n)", ack.authPId_);

		if (inMes.num_ <= 0) {
			AuthenticationContext& authContext = transactionManager_->getAuth(
				ack.pId_, ack.authId_);
			ConnectionOption& connOption =
				authContext.getConnectionND().getUserData<ConnectionOption>();
			GS_TRACE_INFO(AUTH_OPERATION, GS_TRACE_TXN_AUTH_STATUS, "Failed authentication: userName="
				<< connOption.userName_.c_str() << " dbName=" << connOption.dbName_.c_str());
			GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED, "");
		}

		TEST_PRINT1("dbId=%ld\n", dbId);
		TEST_PRINT1("priv=%d\n", priv);
		TEST_PRINT1("roleName=%s\n", dbName.c_str());
		
		clusterService_->checkVersion(ack.clusterVer_);

		const ClusterRole clusterRole = (CROLE_MASTER | CROLE_FOLLOWER);
		const PartitionRoleType partitionRole = PROLE_ANY;
		const PartitionStatus partitionStatus = PSTATE_ANY;
		checkExecutable(
				request.fixed_.pId_, clusterRole, partitionRole, partitionStatus, partitionTable_);

		AuthenticationContext &authContext = transactionManager_->getAuth(
				ack.pId_, ack.authId_);

		ConnectionOption &connOption =
				authContext.getConnectionND().getUserData<ConnectionOption>();
		connOption.setAfterAuth(dbId, emNow, priv, dbName.c_str());
		if (strcmp(dbName.c_str(), "") == 0) {
			addUserCache(ec, connOption.dbName_.c_str(), connOption.userName_.c_str(), connOption.digest_.c_str(), dbId, emNow, priv,
				false, NULL);
		} else {
			addUserCache(ec, connOption.dbName_.c_str(), connOption.userName_.c_str(), connOption.digest_.c_str(), dbId, emNow, priv,
				true, dbName.c_str());
		}
		GS_TRACE_INFO(AUTH_OPERATION, GS_TRACE_TXN_AUTH_STATUS, "Successfully authenticated: userName="
			<< connOption.userName_.c_str() << " dbName=" << connOption.dbName_.c_str()); 
		replySuccess(
				ec, alloc, TXN_STATEMENT_SUCCESS, request,
				authContext);  
		transactionManager_->removeAuth(ack.pId_, ack.authId_);

		TEST_PRINT("<<<AuthenticationAckHandler>>> END\n");
		TEST_PRINT("[A3/5/7] E\n");
	}
	catch (ContextNotFoundException &e) {
		UTIL_TRACE_EXCEPTION_INFO(TRANSACTION_SERVICE, e,
			"Authentication timed out (nd="
				<< ev.getSenderND() << ", pId=" << ev.getPartitionId()
				<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
	catch (std::exception &e) {
		authHandleError(ec, ack, e);
	}
}


	
/*!
	@brief Handler Operator
*/
void DisconnectHandler::operator()(EventContext &ec, Event &ev) {
	TXN_TRACE_HANDLER_CALLED(ev);

	util::StackAllocator &alloc = ec.getAllocator();

	Request request(alloc, getRequestSource(ev));

	try {

		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();
		releaseUserCache(connOption);
		sqlService_->getExecutionManager()->clearConnection(ev, isNewSQL_);

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

	Response response(alloc);

	ConnectionOption& connOption =
		ev.getSenderND().getUserData<ConnectionOption>();

	InMessage inMes(alloc, getRequestSource(ev), connOption);
	Request& request = inMes.request_;
	try {

		EventByteInStream in(ev.getInStream());
		inMes.decode(in);
		EVENT_START(ec, ev, transactionManager_, connOption.dbId_, (ev.getSenderND().getId() < 0));


		releaseUserCache(connOption);

		sqlService_->getExecutionManager()->clearConnection(ev, isNewSQL_);
		OutMessage outMes(request.fixed_.cxtSrc_.stmtId_, TXN_STATEMENT_SUCCESS,
			getReplyOption(alloc, request, NULL, false));
		replySuccess(ec, alloc, ev.getSenderND(), ev.getType(),
			request, outMes, NO_REPLICATION);
	}
	catch (std::exception &e) {
		handleError(ec, alloc, ev, request, e);
	}
}

/*!
	@brief Checks authentication timeout
*/
void CheckTimeoutHandler::checkAuthenticationTimeout(EventContext &ec) {

	util::StackAllocator &alloc = ec.getAllocator();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
	const PartitionGroupId pgId = ec.getWorkerId();

	util::StackAllocator::Scope scope(alloc);

	size_t timeoutResourceCount = 0;

	try {
		util::XArray<PartitionId> pIds(alloc);
		util::XArray<ReplicationId> timeoutResourceIds(alloc);

		transactionManager_->getAuthenticationTimeoutContextId(
			pgId, emNow, pIds, timeoutResourceIds);
		timeoutResourceCount = timeoutResourceIds.size();

		for (size_t i = 0; i < timeoutResourceIds.size(); i++) {
			try {
				AuthenticationContext &authContext =
					transactionManager_->getAuth(
						pIds[i], timeoutResourceIds[i]);

				try {
					TXN_THROW_DENY_ERROR(
						GS_ERROR_TXN_AUTHENTICATION_TIMEOUT, "");
				}
				catch (std::exception &e) {
					Event ev(ec, LOGIN, authContext.getPartitionId());
					setErrorReply(ev, authContext.getStatementId(),
						TXN_STATEMENT_DENY, e,
						authContext.getConnectionND());  

					if (!authContext.getConnectionND().isEmpty()) {
						TEST_PRINT("send\n");
						if (!authContext.isSQLService()) {
							ec.getEngine().send(
								ev, authContext.getConnectionND());
						}
						else {
							sqlService_->getEE()->send(
								ev, authContext.getConnectionND());
						}
					}
				}

				transactionManager_->removeAuth(
					pIds[i], timeoutResourceIds[i]);
			}
			catch (ContextNotFoundException &e) {
				UTIL_TRACE_EXCEPTION_WARNING(TRANSACTION_SERVICE, e,
					"(pId=" << pIds[i]
							<< ", contextId=" << timeoutResourceIds[i]
							<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
			}
		}

		if (timeoutResourceCount > 0) {
			TEST_PRINT1("TIMEOUT count=%ld\n", timeoutResourceCount);
			GS_TRACE_WARNING(AUTHENTICATION_TIMEOUT,
				GS_TRACE_TXN_AUTHENTICATION_TIMEOUT,
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

