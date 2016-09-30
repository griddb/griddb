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

#ifndef NDEBUG
#endif

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


typedef uint64_t ReplicationId;
const ReplicationId TXN_UNDEF_REPLICATIONID = 0;

/*!
	@brief Represents contextual information around the current replication
*/
class ReplicationContext {
	friend class TransactionManager;

public:
	ReplicationContext();
	~ReplicationContext();

	ReplicationContext &operator=(const ReplicationContext &replContext);

	ReplicationId getReplicationId() const;

	int32_t getStatementType() const;

	const ClientId &getClientId() const;

	PartitionId getPartitionId() const;

	ContainerId getContainerId() const;

	StatementId getStatementId() const;

	const NodeDescriptor &getConnectionND() const;

	void incrementAckCounter(uint32_t count = 1);
	bool decrementAckCounter();

	EventMonotonicTime getExpireTime() const;

	bool getExistFlag() const;
	void setExistFlag(bool flag);


private:
	ReplicationId id_;

	int32_t stmtType_;

	ClientId clientId_;

	PartitionId pId_;
	ContainerId containerId_;
	StatementId stmtId_;

	NodeDescriptor clientNd_;

	int32_t ackCounter_;
	EventMonotonicTime timeout_;

	bool existFlag_;

	ReplicationContext(const ReplicationContext &replContext);
	void clear();

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

	TransactionManager(const ConfigTable &config);
	~TransactionManager();

	void createPartition(PartitionId pId);

	void removePartition(PartitionId pId);

	bool hasPartition(PartitionId pId) const;

	const PartitionGroupConfig &getPartitionGroupConfig() const;

	int32_t getReplicationMode() const;

	int32_t getReplicationTimeoutInterval() const;

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
	static const TransactionMode NO_AUTO_COMMIT_BEGIN;  
	static const TransactionMode NO_AUTO_COMMIT_CONTINUE;			
	static const TransactionMode NO_AUTO_COMMIT_BEGIN_OR_CONTINUE;  

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


	void checkActiveTransaction(PartitionGroupId pgId);

	uint64_t getTransactionContextCount(PartitionGroupId pgId);
	uint64_t getActiveTransactionCount(PartitionGroupId pgId);
	uint64_t getReplicationContextCount(PartitionGroupId pgId);

	uint64_t getRequestTimeoutCount(PartitionId pId) const;
	uint64_t getTransactionTimeoutCount(PartitionId pId) const;
	uint64_t getReplicationTimeoutCount(PartitionId pId) const;


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

	typedef util::ExpirableMap<ClientId, TransactionContext, EventMonotonicTime,
		TransactionContextKeyHash>
		TransactionContextMap;
	typedef util::ExpirableMap<ActiveTransactionKey, ActiveTransaction,
		EventMonotonicTime, ActiveTransactionKeyHash>
		ActiveTransactionMap;
	typedef util::ExpirableMap<ReplicationContextKey, ReplicationContext,
		EventMonotonicTime, ReplicationContextKeyHash>
		ReplicationContextMap;

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
			bool isRedo, TransactionId txnId);
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
			PartitionId pId, ReplicationContextMap *replContextMap);
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

		uint64_t replTimeoutCount_;
	};



	const PartitionGroupConfig pgConfig_;
	const int32_t replicationMode_;
	const int32_t replicationTimeoutInterval_;
	const int32_t txnTimeoutLimit_;

	std::vector<TransactionContextMap::Manager *> txnContextMapManager_;
	std::vector<TransactionContextMap *> txnContextMap_;

	std::vector<ActiveTransactionMap::Manager *> activeTxnMapManager_;
	std::vector<ActiveTransactionMap *> activeTxnMap_;

	std::vector<ReplicationContextMap::Manager *> replContextMapManager_;
	std::vector<ReplicationContextMap *> replContextMap_;


	std::vector<Partition *> partition_;
	std::vector<ReplicationContextPartition *> replContextPartition_;

	std::vector<int32_t> ptLock_;
	util::Mutex *ptLockMutex_;

	void finalize();

	void createReplContextPartition(PartitionId pId);

	void begin(TransactionContext &txn, EventMonotonicTime emNow);


	/*!
		@brief Handles config
	*/
	static class ConfigSetUpHandler : public ConfigTable::SetUpHandler {
		virtual void operator()(ConfigTable &config);
	} configSetUpHandler_;
};
#endif
