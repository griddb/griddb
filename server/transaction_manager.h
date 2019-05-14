﻿/*
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
	@brief Definition of TransactionManagement
*/
#ifndef TRANSACTION_MANAGER_H_
#define TRANSACTION_MANAGER_H_

#include "util/trace.h"
#include "config_table.h"
#include "data_type.h"
#include "event_engine.h"
#include "expirable_map.h"
#include "transaction_context.h"


class BaseContainer;

UTIL_TRACER_DECLARE(TRANSACTION_MANAGER);

#define TM_OUTPUT_GETMODE(getMode) static_cast<uint32_t>(getMode)
#define TM_OUTPUT_TXNMODE(txnMode) static_cast<uint32_t>(txnMode)

/*!
	@brief Exception class to notify duplicated statement execution
*/
class StatementAlreadyExecutedException : public util::Exception {
public:
	StatementAlreadyExecutedException(
		UTIL_EXCEPTION_CONSTRUCTOR_ARGS_DECL) throw()
		: Exception(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_SET) {}
	virtual ~StatementAlreadyExecutedException() throw() {}
};

/*!
	@brief Exception "context not found"
*/
class ContextNotFoundException : public UserException {
public:
	ContextNotFoundException(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_DECL) throw()
		: UserException(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_SET) {}
	virtual ~ContextNotFoundException() throw() {}
};

typedef uint64_t AuthenticationId;
const AuthenticationId TXN_UNDEF_AUTHENTICATIONID = 0;

/*!
	@brief Represents contextual information around the current authentication
*/
class AuthenticationContext {
	friend class TransactionManager;

public:
	AuthenticationContext();
	~AuthenticationContext();

	AuthenticationContext &operator=(const AuthenticationContext &authContext);

	AuthenticationId getAuthenticationId() const;

	PartitionId getPartitionId() const;

	StatementId getStatementId() const;

	const NodeDescriptor &getConnectionND() const;

	EventMonotonicTime getExpireTime() const;


private:
	AuthenticationId id_;

	PartitionId pId_;
	StatementId stmtId_;

	NodeDescriptor clientNd_;

	EventMonotonicTime timeout_;


	AuthenticationContext(const AuthenticationContext &authContext);
	void clear();
};


typedef uint64_t ReplicationId;
const ReplicationId TXN_UNDEF_REPLICATIONID = 0;

/*!
	@brief Represents contextual information around the current replication
*/
class ReplicationContext {
	friend class TransactionManager;

public:
	typedef util::XArray<uint8_t, util::StdAllocator<uint8_t, void> > BinaryData;

	ReplicationContext();
	~ReplicationContext();

	ReplicationContext &operator=(const ReplicationContext &replContext);

	ReplicationId getReplicationId() const;

	int32_t getStatementType() const;

	const ClientId &getClientId() const;

	PartitionId getPartitionId() const;

	ContainerId getContainerId() const;

	SchemaVersionId getContainerSchemaVersionId() const;
	void setContainerSchemaVersionId(SchemaVersionId schemaVersionId);

	StatementId getStatementId() const;

	const NodeDescriptor &getConnectionND() const;

	void incrementAckCounter(uint32_t count = 1);
	bool decrementAckCounter();

	EventMonotonicTime getExpireTime() const;

	bool getExistFlag() const;
	void setExistFlag(bool flag);

	/*!
		@brief Task status
	*/
	enum TaskStatus {
		TASK_FINISHED = 0,
		TASK_CONTINUE = 1,
	};

	void setTaskStatus(TaskStatus status) {
		taskStatus_ =  status;
	}
	TaskStatus getTaskStatus() const {
		return taskStatus_;
	}
	void setOriginalStmtId(StatementId stmtId) {
		originalStmtId_ = stmtId;
	}
	StatementId getOriginalStatementId() const {
		return originalStmtId_;
	}

	void setBinaryData(const void *data, size_t size);
	const std::vector<BinaryData*>& getBinaryDatas() const;

private:
	ReplicationId id_;

	int32_t stmtType_;

	ClientId clientId_;
	ClientId ackClientId_;

	PartitionId pId_;
	ContainerId containerId_;
	SchemaVersionId schemaVersionId_;
	StatementId stmtId_;

	NodeDescriptor clientNd_;

	int32_t ackCounter_;
	EventMonotonicTime timeout_;

	TaskStatus taskStatus_;
	StatementId originalStmtId_;

	bool existFlag_;


	bool isSync_;
	int32_t subContainerId_;
	StatementId execId_;

	util::VariableSizeAllocator<> *alloc_;
	std::vector<BinaryData*> binaryDatas_;
	
	ReplicationContext(const ReplicationContext &replContext);
	void clear();
	void clearBinaryData();
};

/*!
	@brief TransactionManager
*/
class TransactionManager {
	friend class TransactionHandlerTest;

public:
	/*!
		@brief Replication mode
	*/
	enum ReplicationMode { REPLICATION_ASYNC, REPLICATION_SEMISYNC };

	TransactionManager(ConfigTable &config);
	~TransactionManager();

	void createPartition(PartitionId pId);

	void removePartition(PartitionId pId);

	bool hasPartition(PartitionId pId) const;

	const PartitionGroupConfig &getPartitionGroupConfig() const;

	int32_t getReplicationMode() const;

	int32_t getReplicationTimeoutInterval() const;
	int32_t getAuthenticationTimeoutInterval() const;

	int32_t getReauthenticationInterval() const;

	typedef uint8_t GetMode;
	static const GetMode AUTO;  
	static const GetMode
		CREATE;  
	static const GetMode
		GET;  
	static const GetMode
		PUT;  

	typedef uint8_t TransactionMode;
	static const TransactionMode AUTO_COMMIT;  
	static const TransactionMode
		NO_AUTO_COMMIT_BEGIN;  
	static const TransactionMode
		NO_AUTO_COMMIT_CONTINUE;  
	static const TransactionMode
		NO_AUTO_COMMIT_BEGIN_OR_CONTINUE;  

	/*!
		@brief ContextSource
	*/
	struct ContextSource {
		int32_t stmtType_;
		bool isUpdateStmt_;
		StatementId stmtId_;
		ContainerId containerId_;
		int32_t txnTimeoutInterval_;
		GetMode getMode_;
		TransactionMode txnMode_;

		ContextSource();
		explicit ContextSource(int32_t stmtType, bool isUpdateStmt = false);
		ContextSource(int32_t stmtType, StatementId stmtId,
			ContainerId containerId, int32_t txnTimeoutInterval,
			GetMode getMode, TransactionMode txnMode,
			bool isUpdateStmt = false);
	};

	TransactionContext &put(util::StackAllocator &alloc, PartitionId pId,
		const ClientId &clientId, const ContextSource &src,
		const util::DateTime &now, EventMonotonicTime emNow,
		bool isRedo = false, TransactionId txnId = UNDEF_TXNID);

	TransactionContext &putNoExpire(util::StackAllocator &alloc, PartitionId pId,
		const ClientId &clientId, const ContextSource &src,
		const util::DateTime &now, EventMonotonicTime emNow,
		bool isRedo = false, TransactionId txnId = UNDEF_TXNID);

	TransactionContext &get(
		util::StackAllocator &alloc, PartitionId pId, const ClientId &clientId);

	void remove(PartitionId pId, const ClientId &clientId);

	void update(TransactionContext &txn, StatementId stmtId);

	void commit(TransactionContext &txn, BaseContainer &container);

	void abort(TransactionContext &txn, BaseContainer &container);

	void remove(TransactionContext &txn);

	void checkStatementAlreadyExecuted(
		const TransactionContext &txn, StatementId stmtId, bool isUpdateStmt);

	void checkStatementContinuousInTransaction(const TransactionContext &txn,
		StatementId stmtId, GetMode getMode, TransactionMode txnMode);

	bool isActiveTransaction(PartitionId pId, TransactionId txnId);

	void getTransactionContextId(
		PartitionId pId, util::XArray<ClientId> &clientIds);

	void getTransactionTimeoutContextId(util::StackAllocator &alloc,
		PartitionGroupId pgId, EventMonotonicTime emNow,
		const util::XArray<bool> &checkPartitionFlagList,
		util::XArray<PartitionId> &pIds, util::XArray<ClientId> &clientIds);

	void getRequestTimeoutContextId(util::StackAllocator &alloc,
		PartitionGroupId pgId, EventMonotonicTime emNow,
		const util::XArray<bool> &checkPartitionFlagList,
		util::XArray<PartitionId> &pIds, util::XArray<ClientId> &clientIds);

	void getKeepaliveTimeoutContextId(util::StackAllocator &alloc,
		PartitionGroupId pgId, EventMonotonicTime emNow,
		const util::XArray<bool> &checkPartitionFlagList,
		util::XArray<PartitionId> &pIds, util::XArray<ClientId> &clientIds);
	void getNoExpireTransactionContextId(util::StackAllocator &alloc,
		PartitionId pId, util::XArray<ClientId> &clientIds);

	void backupTransactionActiveContext(PartitionId pId,
		TransactionId &maxTxnId, util::XArray<ClientId> &clientIds,
		util::XArray<TransactionId> &activeTxnIds,
		util::XArray<ContainerId> &refContainerIds,
		util::XArray<StatementId> &lastStmtIds,
		util::XArray<int32_t> &txnTimeoutIntervalSec);

	void restoreTransactionActiveContext(PartitionId pId,
		TransactionId maxTxnId, uint32_t numContext, const ClientId *clientIds,
		const TransactionId *activeTxnIds, const ContainerId *refContainerIds,
		const StatementId *lastStmtIds, const int32_t *txnTimeoutIntervalSec,
		EventMonotonicTime emNow);

	ReplicationContext &put(PartitionId pId, const ClientId &clientId,
		const ContextSource &src, NodeDescriptor ND, EventMonotonicTime emNow);
	ReplicationContext &get(PartitionId pId, ReplicationId replId);
	void remove(PartitionId pId, ReplicationId replId);

	void removeAllReplicationContext();

	void getReplicationTimeoutContextId(PartitionGroupId pgId,
		EventMonotonicTime emNow, util::XArray<PartitionId> &pIds,
		util::XArray<ReplicationId> &replIds);


	AuthenticationContext &putAuth(PartitionId pId, StatementId stmtId,
		NodeDescriptor ND, EventMonotonicTime emNow, bool isNewSQL = false);

	AuthenticationContext &getAuth(PartitionId pId, AuthenticationId authId);

	void removeAuth(PartitionId pId, AuthenticationId authId);

	void removeAllAuthenticationContext();

	void getAuthenticationTimeoutContextId(PartitionGroupId pgId,
		EventMonotonicTime emNow, util::XArray<PartitionId> &pIds,
		util::XArray<AuthenticationId> &authIds);

	void checkActiveTransaction(PartitionGroupId pgId);

	uint64_t getTransactionContextCount(PartitionGroupId pgId);
	uint64_t getActiveTransactionCount(PartitionGroupId pgId);
	uint64_t getReplicationContextCount(PartitionGroupId pgId);
	uint64_t getAuthenticationContextCount(PartitionGroupId pgId);

	uint64_t getRequestTimeoutCount(PartitionId pId) const;
	uint64_t getTransactionTimeoutCount(PartitionId pId) const;
	uint64_t getReplicationTimeoutCount(PartitionId pId) const;

	uint64_t getAuthenticationTimeoutCount(PartitionId pId) const;

	uint64_t getNoExpireTransactionCount(PartitionGroupId pgId) const;

	size_t getMemoryUsage(
		PartitionGroupId pgId, bool includeFreeMemory = false);

	GetMode getContextGetModeForRecovery(GetMode decodedGetMode) const;
	TransactionMode getTransactionModeForRecovery(
		bool withBegin, bool isAutoCommit) const;

	bool lockPartition(PartitionId pId);
	void unlockPartition(PartitionId pId);

private:
	static const TransactionId INITIAL_TXNID =
		TransactionContext::AUTO_COMMIT_TXNID;
	static const ReplicationId INITIAL_REPLICATIONID = 1;
	static const AuthenticationId INITIAL_AUTHENTICATIONID = 1;

	static const size_t DEFAULT_BLOCK_SIZE_BITS =
		20;  
	static const size_t DEFAULT_FREE_ELEMENT_LIMIT = 0;

	static const size_t HASH_SIZE = 40037;
	static const uint32_t TIMER_MERGIN_SEC = 60;
	static const uint32_t TIMER_INTERVAL_MILLISEC = 1000;

	static const uint32_t NUM_LOCK_MUTEX = 13;

	struct TransactionContextKeyHash {
		size_t operator()(const ClientId &key) {
			uint64_t uuid_upper;
			memcpy(&uuid_upper, key.uuid_, sizeof(uint64_t));
			return static_cast<size_t>(uuid_upper + key.sessionId_);
		}
	};

	struct ActiveTransactionKey {
		TransactionId txnId_;
		PartitionId pId_;

		ActiveTransactionKey() : txnId_(0), pId_(0) {}
		ActiveTransactionKey(PartitionId pId, TransactionId txnId)
			: txnId_(txnId), pId_(pId) {}
		bool operator==(const ActiveTransactionKey &key) const {
			return (txnId_ == key.txnId_ && pId_ == key.pId_);
		}
	};
	struct ActiveTransaction {
		ClientId clientId_;

		ActiveTransaction()
			: clientId_(TXN_EMPTY_CLIENTID)
		{
		}
	};
	struct ActiveTransactionKeyHash {
		size_t operator()(const ActiveTransactionKey &key) {
			return static_cast<size_t>(key.txnId_);
		}
	};

	struct ReplicationContextKey {
		ReplicationId replId_;
		PartitionId pId_;

		ReplicationContextKey() : replId_(0), pId_(0) {}
		ReplicationContextKey(PartitionId pId, ReplicationId replId)
			: replId_(replId), pId_(pId) {}

		bool operator==(const ReplicationContextKey &key) const {
			return (replId_ == key.replId_ && pId_ == key.pId_);
		}
	};
	struct ReplicationContextKeyHash {
		size_t operator()(const ReplicationContextKey &key) {
			return static_cast<size_t>(key.replId_);
		}
	};
	struct AuthenticationContextKey {
		AuthenticationId authId_;
		PartitionId pId_;

		AuthenticationContextKey() : authId_(0), pId_(0) {}
		AuthenticationContextKey(PartitionId pId, AuthenticationId authId)
			: authId_(authId), pId_(pId) {}

		bool operator==(const AuthenticationContextKey &key) const {
			return (authId_ == key.authId_ && pId_ == key.pId_);
		}
	};
	struct AuthenticationContextKeyHash {
		size_t operator()(const AuthenticationContextKey &key) {
			return static_cast<size_t>(key.authId_);
		}
	};

	typedef util::ExpirableMap<ClientId, TransactionContext, EventMonotonicTime,
		TransactionContextKeyHash>
		TransactionContextMap;
	typedef util::ExpirableMap<ActiveTransactionKey, ActiveTransaction,
		EventMonotonicTime, ActiveTransactionKeyHash>
		ActiveTransactionMap;
	typedef util::ExpirableMap<ReplicationContextKey, ReplicationContext,
		EventMonotonicTime, ReplicationContextKeyHash>
		ReplicationContextMap;
	typedef util::ExpirableMap<AuthenticationContextKey, AuthenticationContext,
		EventMonotonicTime, AuthenticationContextKeyHash>
		AuthenticationContextMap;

	/*!
		@brief Provides the functions of managing each partition
	*/
	class Partition {
		friend class TransactionManager;

	public:
		Partition(PartitionId pId, int32_t requestTimeoutInterval,
			TransactionContextMap *txnContextMap,
			ActiveTransactionMap *activeTxnMap);
		~Partition();

		TransactionContext &put(util::StackAllocator &alloc,
			const ClientId &clientId, ContainerId containerId,
			StatementId stmtId, int32_t txnTimeoutInterval,
			const util::DateTime &now, EventMonotonicTime emNow,
			GetMode getMode, TransactionMode txnMode, bool isUpdateStmt,
			bool isRedo, TransactionId txnId, bool isExistTimeoutLimit);
		TransactionContext &get(
			util::StackAllocator &alloc, const ClientId &clientId);
		void remove(const ClientId &clientId);
		void update(TransactionContext &txn, StatementId stmtId);

		TransactionId assignNewTransactionId();
		void begin(TransactionContext &txn, TransactionId txnId,
			EventMonotonicTime emNow);
		void commit(TransactionContext &txn);
		void abort(TransactionContext &txn);

		void checkStatementAlreadyExecuted(const TransactionContext &txn,
			StatementId stmtId, bool isUpdateStmt) const;
		void checkStatementContinuousInTransaction(
			const TransactionContext &txn, StatementId stmtId, GetMode getMode,
			TransactionMode txnMode) const;

		bool isActiveTransaction(TransactionId txnId);

		void getTransactionContextId(util::XArray<ClientId> &clientIds);

		void backupTransactionActiveContext(TransactionId &maxTxnId,
			util::XArray<ClientId> &clientIds,
			util::XArray<TransactionId> &activeTxnIds,
			util::XArray<ContainerId> &refContainerIds,
			util::XArray<StatementId> &lastStmtIds,
			util::XArray<int32_t> &txnTimeoutIntervalSec);
		void restoreTransactionActiveContext(TransactionManager *manager,
			TransactionId maxTxnId, uint32_t numContext,
			const ClientId *clientIds, const TransactionId *activeTxnIds,
			const ContainerId *refContainerIds, const StatementId *lastStmtIds,
			const int32_t *txnTimeoutIntervalSec, EventMonotonicTime emNow);

		uint64_t getRequestTimeoutCount() const;
		uint64_t getTransactionTimeoutCount() const;

	private:
		const PartitionId pId_;

		const int32_t txnTimeoutLimit_;

		TransactionId maxTxnId_;

		TransactionContextMap *txnContextMap_;
		ActiveTransactionMap *activeTxnMap_;

		uint64_t reqTimeoutCount_;
		uint64_t txnTimeoutCount_;

		TransactionContext autoContext_;

		TransactionContext *getAutoContext();
		void endTransaction(TransactionContext &txn);
	};

	class ReplicationContextPartition {
		friend class TransactionManager;

	public:
		ReplicationContextPartition(
			PartitionId pId, ReplicationContextMap *replContextMap,
			util::VariableSizeAllocator<> *replAllocator);
		~ReplicationContextPartition();

		ReplicationContext &put(const ClientId &clientId,
			ContainerId containerId, int32_t stmtType, StatementId stmtId,
			NodeDescriptor ND, int32_t replTimeoutInterval,
			EventMonotonicTime emNow);
		ReplicationContext &get(ReplicationId replId);
		void remove(ReplicationId replId);

		uint64_t getReplicationTimeoutCount() const;

	private:
		const PartitionId pId_;
		ReplicationId maxReplId_;

		ReplicationContextMap *replContextMap_;

		util::VariableSizeAllocator<> *replAllocator_;

		uint64_t replTimeoutCount_;
	};

	class AuthenticationContextPartition {
		friend class TransactionManager;

	public:
		AuthenticationContextPartition(
			PartitionId pId, AuthenticationContextMap *authContextMap);
		~AuthenticationContextPartition();

		AuthenticationContext &putAuth(StatementId stmtId, NodeDescriptor ND,
			int32_t authTimeoutInterval, EventMonotonicTime emNow,
			bool isSQLService = false);
		AuthenticationContext &getAuth(AuthenticationId authId);
		void removeAuth(AuthenticationId authId);

		uint64_t getAuthenticationTimeoutCount() const;

	private:
		const PartitionId pId_;
		AuthenticationId maxAuthId_;

		AuthenticationContextMap *authContextMap_;

		uint64_t authTimeoutCount_;
	};

	/*!
		@brief Represents config
	*/
	struct Config : public ConfigTable::ParamHandler {
		Config(int32_t reauthenticationInterval);

		void setUpConfigHandler(ConfigTable &configTable);
		virtual void operator()(
			ConfigTable::ParamId id, const ParamValue &value);

		int32_t getAtomicReauthenticationInterval() const {
			return atomicReauthenticationInterval_;
		}
		void setAtomicReauthenticationInterval(int32_t val) {
			atomicReauthenticationInterval_ = val;
		}

		util::Atomic<int32_t> atomicReauthenticationInterval_;
	};

	const PartitionGroupConfig pgConfig_;
	const int32_t replicationMode_;
	const int32_t replicationTimeoutInterval_;
	const int32_t authenticationTimeoutInterval_;
	Config reauthConfig_;
	const int32_t txnTimeoutLimit_;

	std::vector<TransactionContextMap::Manager *> txnContextMapManager_;
	std::vector<TransactionContextMap *> txnContextMap_;

	std::vector<ActiveTransactionMap::Manager *> activeTxnMapManager_;
	std::vector<ActiveTransactionMap *> activeTxnMap_;

	std::vector<ReplicationContextMap::Manager *> replContextMapManager_;
	std::vector<ReplicationContextMap *> replContextMap_;

	std::vector<AuthenticationContextMap::Manager *> authContextMapManager_;
	std::vector<AuthenticationContextMap *> authContextMap_;

	std::vector<util::VariableSizeAllocator<> *> replAllocator_;

	std::vector<Partition *> partition_;
	std::vector<ReplicationContextPartition *> replContextPartition_;
	std::vector<AuthenticationContextPartition *> authContextPartition_;

	std::vector<int32_t> ptLock_;
	util::Mutex *ptLockMutex_;

	void finalize();

	void createReplContextPartition(PartitionId pId);
	void createAuthContextPartition(PartitionId pId);

	void begin(TransactionContext &txn, EventMonotonicTime emNow);

	void updateRequestTimeout(
		PartitionGroupId pgId, const TransactionContext &txn);
	void updateTransactionOrRequestTimeout(
		PartitionGroupId pgId, const TransactionContext &txn);


	/*!
		@brief Handles config
	*/
	static class ConfigSetUpHandler : public ConfigTable::SetUpHandler {
		virtual void operator()(ConfigTable &config);
	} configSetUpHandler_;
};
#endif
