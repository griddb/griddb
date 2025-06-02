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
	@brief Definition of TransactionContext
*/
#ifndef TRANSACTION_CONTEXT_H_
#define TRANSACTION_CONTEXT_H_

#include "util/time.h"
#include "data_type.h"
#include "gs_error.h"

#include <string.h>

namespace util {
class StackAllocator;
}
class TransactionManager;

const int32_t TXN_MIN_TRANSACTION_TIMEOUT_INTERVAL = 0;

#include "event_engine.h"

const int32_t TXN_NO_TRANSACTION_TIMEOUT_INTERVAL = INT32_MAX;


const int32_t TXN_STABLE_TRANSACTION_TIMEOUT_INTERVAL = 300;

const int32_t TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL =
	TXN_STABLE_TRANSACTION_TIMEOUT_INTERVAL;

const int32_t CONNECTION_KEEPALIVE_TIMEOUT_INTERVAL = (TXN_STABLE_TRANSACTION_TIMEOUT_INTERVAL + 3) * 1000;

const StatementId TXN_MIN_CLIENT_STATEMENTID = 1;

const double TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE = -1.0;

const size_t TXN_CLIENT_UUID_BYTE_SIZE = 16;

const size_t TXN_DATABASE_NUM_MAX = 1000;

/*!
	@brief Represents transaction context id (clientID)
*/
struct ClientId {
	uint8_t uuid_[TXN_CLIENT_UUID_BYTE_SIZE];
	SessionId sessionId_;

	ClientId() : sessionId_(0) {
		memset(uuid_, 0, TXN_CLIENT_UUID_BYTE_SIZE);
	}

	void init() {
		memset(uuid_, 0, TXN_CLIENT_UUID_BYTE_SIZE);
		sessionId_ = 0;
	}

	ClientId(const ClientId &id) : sessionId_(id.sessionId_) {
		memcpy(uuid_, id.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
	}

	ClientId(const uint8_t *uuid, SessionId sessionId) : sessionId_(sessionId) {
		memcpy(uuid_, uuid, TXN_CLIENT_UUID_BYTE_SIZE);
	}

	ClientId &operator=(const ClientId &id) {
		if (this == &id) {
			return *this;
		}
		memcpy(uuid_, id.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
		sessionId_ = id.sessionId_;
		return *this;
	}

	bool operator==(const ClientId &id) const {
		if (sessionId_ == id.sessionId_) {
			return (memcmp(uuid_, id.uuid_, TXN_CLIENT_UUID_BYTE_SIZE) == 0);
		}
		else {
			return false;
		}
	}

	bool operator<(const ClientId &id) const {
		if (sessionId_ > id.sessionId_) {
			return true;
		}
		if (sessionId_ == id.sessionId_) {
			if (memcmp(uuid_, id.uuid_, TXN_CLIENT_UUID_BYTE_SIZE) > 0) {
				return true;
			}
		}
		return false;
	}

	bool operator!=(const ClientId &id) const {
		if (sessionId_ == id.sessionId_) {
			return (memcmp(uuid_, id.uuid_, TXN_CLIENT_UUID_BYTE_SIZE) != 0);
		}
		else {
			return true;
		}
	}

	std::string dump(util::StackAllocator &alloc);
};

std::ostream &operator<<(std::ostream &stream, const ClientId &id);

const ClientId TXN_EMPTY_CLIENTID = ClientId();

/*!
	@brief Hash function of clientID
*/
struct ClientIdHash {
	size_t operator()(const ClientId &clientId) const {
		return static_cast<size_t>(clientId.sessionId_);
	}
};

/*!
	@brief Represents contextual information around the current transaction
*/
class TransactionContext {
	friend class TransactionManager;
	friend class TransactionHandlerTest;
	friend class StoreV5Impl;	

public:
	TransactionContext();
	~TransactionContext();

	TransactionContext &operator=(const TransactionContext &txn);

	TransactionManager &getManager();

	void replaceDefaultAllocator(util::StackAllocator *allocator) {
		alloc_ = allocator;
	}
	util::StackAllocator &getDefaultAllocator();

	const ClientId &getClientId() const;

	PartitionId getPartitionId() const;

	ContainerId getContainerId() const;

	TransactionId getId() const;

	StatementId getLastStatementId() const;

	bool isAutoCommit() const;

	bool isActive() const;

	bool isExclusive() const;

	bool isRedo() const;

	void setCancelRequest(bool enabled);

	bool isCancelRequested() const;

	const util::DateTime &getStatementStartTime() const;

	const util::DateTime &getStatementExpireTime() const;

	int64_t getTransactionStartTime() const;

	int64_t getTransactionExpireTime() const;

	int32_t getTransactionTimeoutInterval() const;

	int64_t getExpireTime() const;

	const NodeDescriptor& getSenderND() const;

	void setSenderND(const NodeDescriptor &nd);
	void setContainerId(ContainerId containerId);

	double getStoreMemoryAgingSwapRate() const;
	bool isStoreMemoryAgingSwapRateEnabled() const;
	static bool isStoreMemoryAgingSwapRateSpecified(double storeMemoryAgingSwapRate);
	const util::TimeZone &getTimeZone() const;


	void setContainerNameStr(util::String* containerNameStr) {
		containerNameStr_ = containerNameStr;
	}
	
	util::String* getContainerNameStr() {
		return containerNameStr_;
	}
	
	const char* getContainerName() const {
		return (containerNameStr_ ? containerNameStr_->c_str() : "");
	}
	void setAuditInfo(Event* ev,EventContext* ec,util::String* objectNameStr) {
		ev_ = ev;
		ec_ = ec;
		objectNameStr_ = objectNameStr;
	}
	Event* getEvent() {
		return(ev_);
	}
	EventContext* getEventContext() {
		return(ec_);
	}
	util::String* getobjectNameStr() {
		return(objectNameStr_);
	}

private:
	typedef int32_t State;  
	static const State ACTIVE = 1;  
	static const State INACTIVE = 0;  

	static const TransactionId AUTO_COMMIT_TXNID = 3;

	TransactionManager *manager_;

	util::StackAllocator *alloc_;

	ClientId clientId_;

	PartitionId pId_;
	ContainerId containerId_;
	TransactionId txnId_;
	StatementId lastStmtId_;

	State state_;
	bool isExclusive_;
	bool isRedo_;
	bool cancelRequested_;

	util::DateTime stmtStartTime_;
	util::DateTime stmtExpireTime_;

	int64_t txnStartTime_;
	int64_t txnExpireTime_;
	int64_t contextExpireTime_;

	int32_t txnTimeoutInterval_;

	NodeDescriptor senderND_;

	double storeMemoryAgingSwapRate_;

	util::TimeZone timeZone_;

	TransactionContext(const TransactionContext &txn);
	void set(const ClientId &clientId, PartitionId pId, ContainerId containerId,
		int64_t contextExpireTime, int32_t txnTimeoutInterval, 
		double storeMemoryAgingSwapRate, const util::TimeZone &timeZone);
	void clear();

	util::String* containerNameStr_;

	Event* ev_;

	EventContext* ec_;

	util::String* objectNameStr_;
	
public:
	TransactionContext(util::StackAllocator &alloc);

};

inline TransactionContext &TransactionContext::operator=(
	const TransactionContext &txn) {
	if (this == &txn) {
		return *this;
	}
	manager_ = txn.manager_;

	alloc_ = txn.alloc_;
	clientId_ = txn.clientId_;
	pId_ = txn.pId_;
	containerId_ = txn.containerId_;
	txnId_ = txn.txnId_;
	lastStmtId_ = txn.lastStmtId_;
	state_ = txn.state_;
	isExclusive_ = txn.isExclusive_;
	isRedo_ = txn.isRedo_;
	cancelRequested_ = txn.cancelRequested_;
	stmtStartTime_ = txn.stmtStartTime_;
	stmtExpireTime_ = txn.stmtExpireTime_;
	txnStartTime_ = txn.txnStartTime_;
	txnExpireTime_ = txn.txnExpireTime_;
	contextExpireTime_ = txn.contextExpireTime_;
	txnTimeoutInterval_ = txn.txnTimeoutInterval_;
	storeMemoryAgingSwapRate_ = txn.storeMemoryAgingSwapRate_;
	timeZone_ = txn.timeZone_;
	return *this;
}

/*!
	@brief Gets Transaction manager which manages this context
*/
inline TransactionManager &TransactionContext::getManager() {
	if (manager_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	else {
		return *manager_;
	}
}

/*!
	@brief Gets thread default stack allocator
*/
inline util::StackAllocator &TransactionContext::getDefaultAllocator() {
	return *alloc_;
}

/*!
	@brief Gets clientID
*/
inline const ClientId &TransactionContext::getClientId() const {
	return clientId_;
}

/*!
	@brief Gets PartitionID
*/
inline PartitionId TransactionContext::getPartitionId() const {
	return pId_;
}

/*!
	@brief Gets ContainerID
*/
inline ContainerId TransactionContext::getContainerId() const {
	return containerId_;
}

/*!
	@brief Gets current TransactionID
*/
inline TransactionId TransactionContext::getId() const {
	return txnId_;
}

/*!
	@brief Gets last executed statement's ID in current transaction
*/
inline StatementId TransactionContext::getLastStatementId() const {
	return lastStmtId_;
}

/*!
	@brief Checks if current transaction is auto-commit
*/
inline bool TransactionContext::isAutoCommit() const {
	return (txnId_ == AUTO_COMMIT_TXNID);
}

/*!
	@brief Checks if current transaction is active
*/
inline bool TransactionContext::isActive() const {
	return (state_ == ACTIVE);
}

/*!
	@brief Checks if current transaction is exclusive
*/
inline bool TransactionContext::isExclusive() const {
	return isExclusive_;
}

/*!
	@brief Checks if current transaction is executing in recovery(redo)
*/
inline bool TransactionContext::isRedo() const {
	return isRedo_;
}

inline void TransactionContext::setCancelRequest(bool enabled) {
	cancelRequested_ = enabled;
}

inline bool TransactionContext::isCancelRequested() const {
	return cancelRequested_;
}

inline void TransactionContext::set(const ClientId &clientId, PartitionId pId,
	ContainerId containerId, int64_t contextExpireTime,
	int32_t txnTimeoutInterval, double storeMemoryAgingSwapRate,
	const util::TimeZone &timeZone) {
	clientId_ = clientId;
	pId_ = pId;
	containerId_ = containerId;
	contextExpireTime_ = contextExpireTime;
	txnTimeoutInterval_ = txnTimeoutInterval;
	storeMemoryAgingSwapRate_ = storeMemoryAgingSwapRate;
	timeZone_ = timeZone;
}

/*!
	@brief Gets current statement start time
*/
inline const util::DateTime &TransactionContext::getStatementStartTime() const {
	return stmtStartTime_;
}

/*!
	@brief Gets current statement expire time
*/
inline const util::DateTime &TransactionContext::getStatementExpireTime()
	const {
	return stmtExpireTime_;
}

/*!
	@brief Gets current transaction start time
*/
inline int64_t TransactionContext::getTransactionStartTime() const {
	return txnStartTime_;
}

/*!
	@brief Gets current transaction expire time
*/
inline int64_t TransactionContext::getTransactionExpireTime() const {
	return txnExpireTime_;
}

/*!
	@brief Gets current transaction timeout interval
*/
inline int32_t TransactionContext::getTransactionTimeoutInterval() const {
	return txnTimeoutInterval_;
}

/*!
	@brief Gets this context expire time
*/
inline int64_t TransactionContext::getExpireTime() const {
	return contextExpireTime_;
}

/*!
	@brief Sets this context NodeDescriptor
*/
inline void TransactionContext::setSenderND(const NodeDescriptor &nd) {
	senderND_ = nd;
}

/*!
	@brief Gets this context NodeDescriptor
*/
inline const NodeDescriptor& TransactionContext::getSenderND() const {
	return senderND_;
}

inline void TransactionContext::setContainerId(ContainerId containerId) {
	containerId_ = containerId;
}

inline double TransactionContext::getStoreMemoryAgingSwapRate() const {
	return storeMemoryAgingSwapRate_;
}

inline bool TransactionContext::isStoreMemoryAgingSwapRateEnabled() const {
	return isStoreMemoryAgingSwapRateSpecified(storeMemoryAgingSwapRate_);
}

inline bool TransactionContext::isStoreMemoryAgingSwapRateSpecified(
		double storeMemoryAgingSwapRate) {
	return (storeMemoryAgingSwapRate >= 0);
}

inline const util::TimeZone &TransactionContext::getTimeZone() const {
	return timeZone_;
}

inline void TransactionContext::clear() {
	alloc_ = NULL;
	clientId_ = TXN_EMPTY_CLIENTID;
	containerId_ = UNDEF_CONTAINERID;
	txnId_ = UNDEF_TXNID;
	lastStmtId_ = INITIAL_STATEMENTID;
	state_ = INACTIVE;
	isExclusive_ = false;
	isRedo_ = false;
	cancelRequested_ = false;
	stmtStartTime_ = 0;
	stmtExpireTime_ = 0;
	txnStartTime_ = 0;
	txnExpireTime_ = 0;
	contextExpireTime_ = 0;
	txnTimeoutInterval_ = TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL;
	storeMemoryAgingSwapRate_ = TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE;
	timeZone_ = util::TimeZone();
}

#define TXN_CHECK_NOT_CANCELLED(txn)                                        \
	do {                                                                    \
		if ((txn).isCancelRequested()) {                                    \
			GS_THROW_USER_ERROR(                                            \
				GS_ERROR_TXN_CANCELLED, "Transaction operation cancelled"); \
		}                                                                   \
	} while (false)
#endif
