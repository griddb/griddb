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
	@brief Implementation of TransactionManagement for authentication
*/
#include "transaction_manager.h"
#include "gs_error.h"
#include "transaction_context.h"

#include "base_container.h"


#define TM_THROW_AUTH_CONTEXT_NOT_FOUND(message) \
	GS_THROW_CUSTOM_ERROR(ContextNotFoundException, GS_ERROR_TM_AUTHENTICATION_NOT_FOUND, message)

AuthenticationContext::AuthenticationContext() :
	id_(TXN_UNDEF_AUTHENTICATIONID),
	pId_(UNDEF_PARTITIONID),
	stmtId_(0),
	clientNd_(NodeDescriptor::EMPTY_ND),
	timeout_(0),
	isSQLService_(false)
{
}
AuthenticationContext::AuthenticationContext(const AuthenticationContext &authContext) :
	id_(authContext.id_),
	pId_(authContext.pId_),
	stmtId_(authContext.stmtId_),
	clientNd_(authContext.clientNd_),
	timeout_(authContext.timeout_),
    isSQLService_(authContext.isSQLService_)
{
}
AuthenticationContext::~AuthenticationContext()
{
}
AuthenticationContext& AuthenticationContext::operator =(const AuthenticationContext &authContext)
{
	if (this == &authContext) {
		return *this;
	}
	id_ = authContext.id_;
	pId_ = authContext.pId_;
	stmtId_ = authContext.stmtId_;
	clientNd_ = authContext.clientNd_;
	timeout_ = authContext.timeout_;
	isSQLService_ = authContext.isSQLService_;
	return *this;
}
AuthenticationId AuthenticationContext::getAuthenticationId() const
{
	return id_;
}
PartitionId AuthenticationContext::getPartitionId() const
{
	return pId_;
}
StatementId AuthenticationContext::getStatementId() const
{
	return stmtId_;
}
const NodeDescriptor& AuthenticationContext::getConnectionND() const
{
	return clientNd_;
}
EventMonotonicTime AuthenticationContext::getExpireTime() const
{
	return timeout_;
}
void AuthenticationContext::clear()
{
	id_ = TXN_UNDEF_REPLICATIONID;
	pId_ = UNDEF_PARTITIONID;
	stmtId_ = TXN_MIN_CLIENT_STATEMENTID,
	clientNd_ = NodeDescriptor::EMPTY_ND;
	timeout_ = 0;
	isSQLService_ = false;
}

int32_t TransactionManager::getUserCacheSize() const
{
	return userCacheSize_;
}

int32_t TransactionManager::getUserCacheUpdateInterval() const
{
	return userCacheUpdateInterval_;
}

bool TransactionManager::isLDAPAuthentication() const
{
	return isLDAPAuthentication_;
}

bool TransactionManager::isRoleMappingByGroup() const
{
	return isRoleMappingByGroup_;
}

bool TransactionManager::isLDAPSimpleMode() const
{
	return isLDAPSimpleMode_;
}

const char* TransactionManager::getLDAPUrl() const
{
	return ldapUrl_.c_str();
}

const char* TransactionManager::getLDAPUserDNPrefix() const
{
	return ldapUserDNPrefix_.c_str();
}

const char* TransactionManager::getLDAPUserDNSuffix() const
{
	return ldapUserDNSuffix_.c_str();
}

const char* TransactionManager::getLDAPBindDN() const
{
	return ldapBindDN_.c_str();
}

const char* TransactionManager::getLDAPBindPassword() const
{
	return ldapBindPassword_.c_str();
}

const char* TransactionManager::getLDAPBaseDN() const
{
	return ldapBaseDN_.c_str();
}

const char* TransactionManager::getLDAPSearchAttribute() const
{
	return ldapSearchAttribute_.c_str();
}

const char* TransactionManager::getLDAPMemberOfAttribute() const
{
	return ldapMemberOfAttribute_.c_str();
}

int32_t TransactionManager::getLDAPWaitTime() const
{
	return ldapWaitTime_;
}

int32_t TransactionManager::getLoginWaitTime() const
{
	return loginWaitTime_;
}

int32_t TransactionManager::getLoginRepetitionNum() const
{
	return loginRepetitionNum_;
}

int32_t TransactionManager::getAuthenticationTimeoutInterval() const
{
	return authenticationTimeoutInterval_;
}

int32_t TransactionManager::getReauthenticationInterval() const
{
	return reauthConfig_.getAtomicReauthenticationInterval();
}

/*!
	@brief Sets reauthenticationInterval
*/
TransactionManager::Config::Config(int32_t reauthenticationInterval) :
		atomicReauthenticationInterval_(reauthenticationInterval)
{}

/*!
	@brief Sets parameter Handler to configTable
*/
void TransactionManager::Config::setUpConfigHandler(ConfigTable &configTable) {
	if (!configTable.isSetParamHandler(CONFIG_TABLE_TXN_REAUTHENTICATION_INTERVAL)) {
		configTable.setParamHandler(CONFIG_TABLE_TXN_REAUTHENTICATION_INTERVAL, *this);
	}
}

/*!
	@brief Updates config
*/
void TransactionManager::Config::operator()(
		ConfigTable::ParamId id, const ParamValue &value) {
	switch (id) {
	case CONFIG_TABLE_TXN_REAUTHENTICATION_INTERVAL:
		setAtomicReauthenticationInterval(value.get<int32_t>());
		break;
	}
}


/*!
	@brief Creates authentication context
*/
AuthenticationContext& TransactionManager::putAuth(PartitionId pId, 
											StatementId stmtId,
											NodeDescriptor ND,
											EventMonotonicTime emNow,
											bool isSQLService)
{
	createAuthContextPartition(pId);
	return authContextPartition_[pId]->putAuth(stmtId, ND,
		authenticationTimeoutInterval_, emNow, isSQLService);
}

/*!
	@brief Gets authentication context
*/
AuthenticationContext &TransactionManager::getAuth(
	PartitionId pId, AuthenticationId authId) {
	createAuthContextPartition(pId);
	return authContextPartition_[pId]->getAuth(authId);
}

/*!
	@brief Removes authentication context
*/
void TransactionManager::removeAuth(PartitionId pId, AuthenticationId authId) {
	createAuthContextPartition(pId);
	authContextPartition_[pId]->removeAuth(authId);
}

/*!
	@brief Gets authentication contextID (partitionID and authenticationID) list on authentication timeout
*/
void TransactionManager::getAuthenticationTimeoutContextId(
	PartitionGroupId pgId, EventMonotonicTime emNow,
	util::XArray<PartitionId> &pIds, util::XArray<AuthenticationId> &authIds) {
	try {
		AuthenticationContextKey *key;
		for (AuthenticationContext *authContext =
				 authContextMap_[pgId]->refresh(emNow, key);
			 authContext != NULL;
			 authContext = authContextMap_[pgId]->refresh(emNow, key)) {
			pIds.push_back(key->pId_);
			authIds.push_back(key->authId_);
			authContextPartition_[authContext->getPartitionId()]
				->authTimeoutCount_++;
		}

		assert(pIds.size() == authIds.size());
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e,
			"Failed to check authentication timeout "
			"(pgId="
				<< pgId << ", emNow=" << emNow
				<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

uint64_t TransactionManager::getAuthenticationContextCount(
	PartitionGroupId pgId) {
	return authContextMap_[pgId]->size();
}

uint64_t TransactionManager::getAuthenticationTimeoutCount(
	PartitionId pId) const {
	return (authContextPartition_[pId] == NULL)
			   ? 0
			   : authContextPartition_[pId]->getAuthenticationTimeoutCount();
}

TransactionManager::AuthenticationContextPartition::
	AuthenticationContextPartition(
		PartitionId pId, AuthenticationContextMap *authContextMap)
	: pId_(pId), maxAuthId_(INITIAL_AUTHENTICATIONID), authContextMap_(authContextMap), authTimeoutCount_(0) {
}

TransactionManager::AuthenticationContextPartition::
	~AuthenticationContextPartition() {
	{
		AuthenticationContextMap::Cursor cursor = authContextMap_->getCursor();
		for (AuthenticationContext *authContext = cursor.next();
			 authContext != NULL; authContext = cursor.next()) {
			if (authContext->getPartitionId() == pId_) {
				const AuthenticationContextKey key(
					pId_, authContext->getAuthenticationId());
				authContextMap_->remove(key);
			}
		}
	}
}

AuthenticationContext &
TransactionManager::AuthenticationContextPartition::putAuth(StatementId stmtId,
	NodeDescriptor ND, int32_t authTimeoutInterval, EventMonotonicTime emNow,
	bool isSQLService)
{
	try {
		const AuthenticationId authId = ++maxAuthId_;
		const EventMonotonicTime authTimeout =
			emNow + static_cast<EventMonotonicTime>(authTimeoutInterval) * 1000;
		const AuthenticationContextKey key(pId_, authId);
		AuthenticationContext &authContext =
			authContextMap_->create(key, authTimeout);
		authContext.clear();
		authContext.pId_ = pId_;
		authContext.id_ = authId;
		authContext.stmtId_ = stmtId;  
		authContext.clientNd_ = ND;
		authContext.timeout_ = authTimeout;
		authContext.isSQLService_ = isSQLService;
		return authContext;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e,
			"Failed to put authentication context "
			"(pId="
				<< pId_ << ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

AuthenticationContext &
TransactionManager::AuthenticationContextPartition::getAuth(
	AuthenticationId authId) {
	try {
		const AuthenticationContextKey key(pId_, authId);
		AuthenticationContext *authContext = authContextMap_->get(key);
		if (authContext != NULL) {
			return *authContext;
		}
		else {
			TM_THROW_AUTH_CONTEXT_NOT_FOUND(
				"(pId=" << pId_ << ", authId=" << authId << ")");
		}
	}
	catch (ContextNotFoundException &) {
		throw;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e,
			"Failed to get authentication context "
			"(pId="
				<< pId_ << ", authId=" << authId
				<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void TransactionManager::AuthenticationContextPartition::removeAuth(
	AuthenticationId authId) {
	try {
		const AuthenticationContextKey key(pId_, authId);
		authContextMap_->remove(key);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e,
			"Failed to remove authentication context "
			"(pId="
				<< pId_ << ", authId=" << authId
				<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

uint64_t TransactionManager::AuthenticationContextPartition::
	getAuthenticationTimeoutCount() const {
	return authTimeoutCount_;
}
