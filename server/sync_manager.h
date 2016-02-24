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
	@brief Definition of SyncManager
*/

#ifndef SYNC_MANAGER_H_
#define SYNC_MANAGER_H_

#include "util/container.h"
#include "config_table.h"
#include "data_type.h"
#include "partition_table.h"

typedef util::VariableSizeAllocatorTraits<256, 1024 * 1024, 1024 * 1024 * 2>
	SyncVariableSizeAllocatorTraits;
typedef util::VariableSizeAllocator<util::Mutex,
	SyncVariableSizeAllocatorTraits>
	SyncVariableSizeAllocator;



/*!
	@brief Operation type related with Synchronization
*/
enum SyncOperationType {
	OP_SHORTTERM_SYNC_REQUEST,
	OP_SHORTTERM_SYNC_START,
	OP_SHORTTERM_SYNC_START_ACK,
	OP_SHORTTERM_SYNC_LOG,
	OP_SHORTTERM_SYNC_LOG_ACK,
	OP_SHORTTERM_SYNC_END,
	OP_SHORTTERM_SYNC_END_ACK,
	OP_LONGTERM_SYNC_REQUEST,
	OP_LONGTERM_SYNC_START,
	OP_LONGTERM_SYNC_START_ACK,
	OP_LONGTERM_SYNC_CHUNK,
	OP_LONGTERM_SYNC_CHUNK_ACK,
	OP_LONGTERM_SYNC_LOG,
	OP_LONGTERM_SYNC_LOG_ACK,
	OP_SYNC_TIMEOUT,
	OP_DROP_PARTITION,
};

/*!
	@brief Synchronization ID
*/
struct SyncId {
	static const int32_t UNDEF_CONTEXT_ID = -1;
	static const int32_t INITIAL_CONTEXT_VERSION = 0;


	SyncId()
		: contextId_(UNDEF_CONTEXT_ID),
		  contextVersion_(INITIAL_CONTEXT_VERSION) {}

	SyncId(int32_t contextId, uint64_t contextVersion)
		: contextId_(contextId), contextVersion_(contextVersion) {}

	~SyncId() {}

	void reset() {
		contextId_ = UNDEF_CONTEXT_ID;
		contextVersion_ = INITIAL_CONTEXT_VERSION;
	}

	bool operator==(const SyncId &id) const {
		if (contextId_ == id.contextId_) {
			return (contextVersion_ == id.contextVersion_);
		}
		else {
			return false;
		}
	}

	bool isValid() {
		return (contextId_ != UNDEF_CONTEXT_ID);
	}

	std::string dump() {
		util::NormalOStringStream ss;
		ss << "{contextId:" << contextId_ << ", version:" << contextVersion_
		   << "}";
		return ss.str();
	}


	int32_t contextId_;
	uint64_t contextVersion_;
};

/*!
	@brief Represents the statistics of synchronization
*/
struct SyncOptStat {
	SyncOptStat(uint32_t partitionNum) {
		allocateList_.assign(partitionNum, 0);
		referenceCounter_.assign(partitionNum, 0);
		existContextCounter_.assign(partitionNum, 0);
		totalAllocateList_.assign(partitionNum, 0);
		partitionNum_ = static_cast<uint32_t>(allocateList_.size());
	}

	void clear() {
		for (PartitionId pId = 0; pId < partitionNum_; pId++) {
			allocateList_[pId] = 0;
			referenceCounter_[pId] = 0;
			totalAllocateList_[pId] = 0;
			existContextCounter_[pId] = 0;
		}
	}

	void statAllocate(PartitionId pId, uint32_t size) {
		allocateList_[pId] += size;
		referenceCounter_[pId]++;
		totalAllocateList_[pId]++;
		assert(allocateList_[pId] >= 0);
	}

	void statFree(PartitionId pId, uint32_t size) {
		allocateList_[pId] -= size;
		referenceCounter_[pId]--;
		assert(allocateList_[pId] >= 0);
	}

	void setContext(PartitionId pId) {
		existContextCounter_[pId]++;
	}
	void freeContext(PartitionId pId) {
		existContextCounter_[pId]--;
	}

	uint64_t getAllocateSize() {
		uint64_t totalAllocateSize = 0;
		for (PartitionId pId = 0; pId < partitionNum_; pId++) {
			totalAllocateSize += allocateList_[pId];
		}
		return totalAllocateSize;
	}

	uint64_t getTotalAllocateSize() {
		uint64_t totalAllocateSize = 0;
		for (PartitionId pId = 0; pId < partitionNum_; pId++) {
			totalAllocateSize += totalAllocateList_[pId];
		}
		return totalAllocateSize;
	}

	uint64_t getUnfixCount() {
		uint64_t totalUnfixCount = 0;
		for (PartitionId pId = 0; pId < partitionNum_; pId++) {
			totalUnfixCount += referenceCounter_[pId];
		}
		return totalUnfixCount;
	}

	uint64_t getContextCount() {
		uint64_t totalContextCount = 0;
		for (PartitionId pId = 0; pId < partitionNum_; pId++) {
			totalContextCount += existContextCounter_[pId];
		}
		return totalContextCount;
	}

	std::string dump() {
		util::NormalOStringStream ss;
		ss << "allocate info:{";
		for (PartitionId pId = 0; pId < partitionNum_; pId++) {
			ss << "{pId:" << pId << ", allocate:" << allocateList_[pId]
			   << ", ref:" << referenceCounter_[pId] << "}";
		}
		ss << "}";
		return ss.str();
	}

	std::vector<uint64_t> allocateList_;
	std::vector<uint32_t> referenceCounter_;
	std::vector<uint64_t> totalAllocateList_;
	std::vector<uint64_t> existContextCounter_;

	uint32_t partitionNum_;
};

/*!
	@brief Represents contextual information around the current synchronization
*/
class SyncContext {
	friend class SyncManager;
	friend class SyncManagerTest;
	struct SendBackup;

public:

	SyncContext();

	~SyncContext();

	void incrementCounter(NodeId syncTargetNodeId);

	void resetCounter() {
		for (size_t i = 0; i < sendBackups_.size(); i++) {
			sendBackups_[i].isAcked_ = true;
		}
		numSendBackup_ = static_cast<uint32_t>(sendBackups_.size());
	}

	bool decrementCounter(NodeId syncTargetNodeId);

	int32_t getCounter() {
		return numSendBackup_;
	}

	void getSyncTargetNodeIds(util::XArray<NodeId> &backups) {
		for (size_t pos = 0; pos < sendBackups_.size(); pos++) {
			backups.push_back(sendBackups_[pos].nodeId_);
		}
	}

	void getSyncIncompleteNodIds(std::vector<NodeId> &syncIncompleteNodeIds);

	void setSyncTargetLsn(NodeId syncTargetNodeId, LogSequentialNumber lsn);

	void setSyncTargetLsnWithSyncId(
		NodeId syncTargetNodeId, LogSequentialNumber lsn, SyncId backupSyncId);

	LogSequentialNumber getSyncTargetLsn(NodeId syncTargetNodeId) const;

	bool getSyncTargetLsnWithSyncId(NodeId syncTargetNodeId,
		LogSequentialNumber &backupLsn, SyncId &backupSyncId);

	int32_t getId() const {
		return id_;
	}

	void setId(int32_t id) {
		id_ = id;
	}

	uint64_t getVersion() const {
		return version_;
	}

	NodeId getRecvNodeId() {
		return recvNodeId_;
	}

	void setRecvNodeId(NodeId recvNodeId) {
		recvNodeId_ = recvNodeId;
	}

	void updateVersion() {
		version_++;
	}

	PartitionRevision &getPartitionRevision() {
		return ptRev_;
	}

	void setPartitionRevision(PartitionRevision &ptRev) {
		ptRev_ = ptRev;
	}

	PartitionId getPartitionId() const {
		return pId_;
	}

	void setPartitionId(PartitionId pId) {
		pId_ = pId;
	}

	StatementId createStatementId() {
		return ++nextStmtId_;
	}

	StatementId getStatementId() const {
		return nextStmtId_;
	}

	uint32_t getCounter() const {
		return numSendBackup_;
	}

	void setSyncCheckpointInfo(CheckpointId cpId) {
		cpId_ = cpId;
	}

	CheckpointId getSyncCheckpointId() const {
		return cpId_;
	}

	void setSyncCheckpointCompleted() {
		isSyncCpCompleted_ = true;
	}

	bool isSyncCheckpointCompleted() {
		return isSyncCpCompleted_;
	}

	void setPartitionStatus(PartitionStatus status) {
		status_ = status;
	}

	PartitionStatus getPartitionStatus() {
		return status_;
	}

	void setChunkInfo(CheckpointId cpId, int32_t totalChunkSize) {
		cpId_ = cpId;
		totalChunkNum_ = totalChunkSize;
	}

	void getChunkInfo(int32_t &chunkNum, int32_t &chunkSize) {
		chunkNum = chunkNum_;
		chunkSize = chunkBaseSize_;
	}

	int32_t getChunkNum() {
		return chunkNum_;
	}

	void setTotalChunkNum(int32_t totalChunkNum) {
		totalChunkNum_ = totalChunkNum;
	}

	int32_t getTotalChunkNum() {
		return totalChunkNum_;
	}

	bool isRecvCompleted() {
		return (totalChunkNum_ == processedChunkNum_);
	}

	bool isRecvImcompleted() {
		return (totalChunkNum_ > 0 && totalChunkNum_ < processedChunkNum_);
	}

	void incProcessedChunkNum(int32_t chunkNum = 1) {
		processedChunkNum_ += chunkNum;
	}

	int32_t getProcessedChunkNum() {
		return processedChunkNum_;
	}

	void setUsed() {
		used_ = true;
	}

	void setUnuse() {
		used_ = false;
	}

	void clear(SyncVariableSizeAllocator &alloc, SyncOptStat *stat);

	void setNextEmptyChain(SyncContext *next) {
		nextEmptyChain_ = next;
	}

	SyncContext *getNextEmptyChain() const {
		return nextEmptyChain_;
	}

	void getSyncId(SyncId &syncId) {
		syncId.contextId_ = id_;
		syncId.contextVersion_ = version_;
	}

	void copyLogBuffer(SyncVariableSizeAllocator &alloc,
		const uint8_t *logBuffer, int32_t logBufferSize, SyncOptStat *stat);

	void copyChunkBuffer(SyncVariableSizeAllocator &alloc,
		const uint8_t *chunkBuffer, int32_t chunkSize, int32_t chunkNum,
		SyncOptStat *stat);

	void freeBuffer(
		SyncVariableSizeAllocator &alloc, SyncType syncType, SyncOptStat *stat);

	void getLogBuffer(uint8_t *&logBuffer, int32_t &logBufferSize);

	void getChunkBuffer(uint8_t *&chunkBuffer, int32_t chunkNo);

	void setDownNextOwner(bool flag) {
		isDownNextOwner_ = flag;
	}

	bool getDownNextOwner() {
		return isDownNextOwner_;
	}

	std::string dump(bool isDetail = false);


private:

	struct SendBackup {
		SendBackup()
			: nodeId_(UNDEF_NODEID), isAcked_(false), lsn_(UNDEF_LSN) {}

		SendBackup(NodeId nodeId)
			: nodeId_(nodeId), isAcked_(false), lsn_(UNDEF_LSN) {}

		~SendBackup() {}

		NodeId nodeId_;
		bool isAcked_;
		LogSequentialNumber lsn_;
		SyncId backupSyncId_;
	};



	int32_t id_;
	PartitionId pId_;
	uint64_t version_;
	bool used_;
	uint32_t numSendBackup_;
	StatementId nextStmtId_;
	CheckpointId cpId_;
	NodeId recvNodeId_;

	bool isSyncCpCompleted_;
	SyncContext *nextEmptyChain_;
	PartitionRevision ptRev_;
	bool isDownNextOwner_;

	util::NormalXArray<SendBackup> sendBackups_;

	int32_t processedChunkNum_;

	int32_t totalChunkNum_;

	uint8_t *logBuffer_;

	int32_t logBufferSize_;

	uint8_t *chunkBuffer_;

	int32_t chunkBufferSize_;

	int32_t chunkBaseSize_;

	int32_t chunkNum_;

	int32_t chunkNo_;

	PartitionStatus status_;

	int32_t queueSize_;

};

/*!
	@brief SyncManager
*/
class SyncManager {
	class SyncConfig;
	class ExtraConfig;
	class SyncContextTable;

	friend class SyncManagerTest;

public:
	static const uint32_t SYNC_MODE_NORMAL = 0;
	static const uint32_t SYNC_MODE_RETRY_CHUNK = 1;


	SyncOptStat syncOptStat_;

	SyncOptStat *getSyncOptStat() {
		return &syncOptStat_;
	}

	SyncManager(const ConfigTable &configTable, PartitionTable *pt);

	~SyncManager();

	SyncContext *createSyncContext(PartitionId pId, PartitionRevision &ptRev);

	void setCurrentSyncId(
		PartitionId pId, SyncContext *context, PartitionRevision &ptRev) {
		currentSyncPId_ = pId;
		SyncId syncId(context->id_, context->version_);
		currentSyncId_ = syncId;
		currentRev_ = ptRev;
	}

	void resetCurrentSyncId() {
		currentSyncPId_ = UNDEF_PARTITIONID;
		currentSyncId_.reset();
		currentRev_.updateRevision(1);
	}

	void getCurrentSyncId(
		PartitionId &pId, SyncId &syncId, PartitionRevision &ptRev) {
		pId = currentSyncPId_;
		syncId = currentSyncId_;
		ptRev = currentRev_;
	}

	util::FixedSizeAllocator<util::Mutex> &getFixedSizeAllocator() {
		return fixedSizeAlloc_;
	}

	SyncContext *getSyncContext(PartitionId pId, SyncId &syncId);

	void removeSyncContext(PartitionId pId, SyncContext *&context);

	void removePartition(PartitionId pId);

	void checkExecutable(
		SyncOperationType operation, PartitionId pId, PartitionRole &role);

	SyncVariableSizeAllocator &getVariableSizeAllocator() {
		return varSizeAlloc_;
	}
	SyncConfig &getConfig() {
		return syncConfig_;
	};

	ExtraConfig &getExtraConfig() {
		return extraConfig_;
	};

	PartitionTable *getPartitionTable() {
		return pt_;
	}

	std::string dumpAll();

	std::string dump(PartitionId pId);

	int32_t getActiveContextNum();

	Size_t getChunkSize() {
		return chunkSize_;
	}

	int32_t getRecoveryLevel() {
		return recoveryLevel_;
	}

	void setSyncMode(int32_t mode) {
		syncMode_ = mode;
	}

	int32_t getSyncMode() {
		return syncMode_;
	}

	void setRestored(PartitionId pId) {
		undoPartitionList_[pId] = 1;
	}

	uint8_t isRestored(PartitionId pId) {
		return undoPartitionList_[pId];
	}

	uint8_t *getChunkBuffer(PartitionGroupId pgId) {
		return &chunkBufferList_[chunkSize_ * pgId];
	}

private:
	static const uint32_t DEFAULT_CONTEXT_SLOT_NUM = 1;



	class SyncContextTable {
		friend class SyncManager;

	public:
		SyncContextTable(util::StackAllocator &alloc, PartitionId pId,
			uint32_t numInitialSlot, SyncVariableSizeAllocator *varAlloc);

		~SyncContextTable();

		SyncContext *createSyncContext(PartitionRevision &ptRev);

		SyncContext *getSyncContext(int32_t id, uint64_t version) const;

		void removeSyncContext(SyncVariableSizeAllocator &varSizeAlloc,
			SyncContext *&context, SyncOptStat *stat);

	private:
		static const uint32_t SLOT_SIZE = 128;

		const PartitionId pId_;
		int32_t numCounter_;
		SyncContext *freeList_;
		int32_t numUsed_;
		util::StackAllocator *alloc_;
		util::XArray<SyncContext *> slots_;
		SyncVariableSizeAllocator *varSizeAlloc_;
	};

	/*!
		@brief Represents config for SyncManager
	*/
	class SyncConfig {
	public:
		SyncConfig(const ConfigTable &config)
			: syncTimeoutInterval_(changeTimeSecToMill(
				  config.get<int32_t>(CONFIG_TABLE_SYNC_TIMEOUT_INTERVAL))),
			  maxMessageSize_(config.get<int32_t>(
				  CONFIG_TABLE_SYNC_LONG_SYNC_MAX_MESSAGE_SIZE)) {}

		int32_t getSyncTimeoutInterval() const {
			return syncTimeoutInterval_;
		}

		int32_t getMaxMessageSize() const {
			return maxMessageSize_;
		}

		bool setMaxMessageSize(int32_t maxMessageSize) {
			maxMessageSize_ = maxMessageSize;
			return true;
		}

	private:
		int32_t syncTimeoutInterval_;
		int32_t maxMessageSize_;
	};

	/*!
		@brief Represents extra config for SyncManager
	*/
	class ExtraConfig {
	public:
		static const int32_t SYC_APPROXIMATE_GAP_LSN = 100;
		static const int32_t SYC_LOCKCONFLICT_INTERVAL = 30000;
		static const int32_t SYC_APPROXIMATE_WAIT_INTERVAL = 10000;

		static const int32_t SYC_SHORTTERM_LIMIT_QUEUE_SIZE = 10000;
		static const int32_t SYC_SHORTTERM_LOWLOAD_LOG_INTERVAL = 0;
		static const int32_t SYC_SHORTTERM_HIGHLOAD_LOG_INTERVAL = 0;

		static const int32_t SYC_LONGTERM_LIMIT_QUEUE_SIZE = 40;
		static const int32_t SYC_LONGTERM_LOWLOAD_LOG_INTERVAL = 0;
		static const int32_t SYC_LONGTERM_HIGHLOAD_LOG_INTERVAL = 100;
		static const int32_t SYC_LONGTERM_LOWLOAD_CHUNK_INTERVAL = 0;
		static const int32_t SYC_LONGTERM_HIGHLOAD_CHUNK_INTERVAL = 100;

		static const int32_t SYC_LONGTERM_DUMP_CHUNK_INTERVAL = 5000;

		ExtraConfig()
			: lockConflictPendingInterval_(SYC_LOCKCONFLICT_INTERVAL),
			  longtermNearestLsnGap_(SYC_APPROXIMATE_GAP_LSN),
			  longtermNearestInterval_(SYC_APPROXIMATE_WAIT_INTERVAL),
			  shorttermLimitQueueSize_(SYC_SHORTTERM_LIMIT_QUEUE_SIZE),
			  shorttermLowLoadLogInterval_(SYC_SHORTTERM_LOWLOAD_LOG_INTERVAL),
			  shorttermHighLoadLogInterval_(
				  SYC_SHORTTERM_HIGHLOAD_LOG_INTERVAL),
			  longtermLimitQueueSize_(SYC_LONGTERM_LIMIT_QUEUE_SIZE),
			  longtermLowLoadLogInterval_(SYC_LONGTERM_LOWLOAD_LOG_INTERVAL),
			  longtermHighLoadLogInterval_(SYC_LONGTERM_HIGHLOAD_LOG_INTERVAL),
			  longtermLowLoadChunkInterval_(
				  SYC_LONGTERM_LOWLOAD_CHUNK_INTERVAL),
			  longtermHighLoadChunkInterval_(
				  SYC_LONGTERM_HIGHLOAD_CHUNK_INTERVAL),
			  longtermDumpChunkInterval_(SYC_LONGTERM_DUMP_CHUNK_INTERVAL) {}

		int32_t getLongtermDumpChunkInterval() const {
			return longtermDumpChunkInterval_;
		}

		int32_t getLockConflictPendingInterval() const {
			return lockConflictPendingInterval_;
		}

		bool setApproximateLsnGap(int32_t gap) {
			if (gap < 0 || gap > INT32_MAX) {
				return false;
			}
			longtermNearestLsnGap_ = gap;
			return true;
		}

		int32_t getApproximateGapLsn() {
			return longtermNearestLsnGap_;
		}

		bool setApproximateWaitInterval(int32_t interval) {
			if (interval < 0 || interval > INT32_MAX) {
				return false;
			}
			longtermNearestInterval_ = interval;
			return true;
		}

		int32_t getApproximateWaitInterval() {
			return longtermNearestInterval_;
		}

		bool setLimitShorttermQueueSize(int32_t size) {
			if (size < 0 || size > INT32_MAX) {
				return false;
			}
			shorttermLimitQueueSize_ = size;
			return true;
		}

		int32_t getLimitShorttermQueueSize() {
			return shorttermLimitQueueSize_;
		}

		bool setLimitLongtermQueueSize(int32_t size) {
			if (size < 0 || size > INT32_MAX) {
				return false;
			}
			longtermLimitQueueSize_ = size;
			return true;
		}

		int32_t getLimitLongtermQueueSize() {
			return longtermLimitQueueSize_;
		}

		bool setShorttermLowLoadLogWaitInterval(int32_t size) {
			if (size < 0 || size > INT32_MAX) {
				return false;
			}
			shorttermLowLoadLogInterval_ = size;
			return true;
		}

		int32_t getShorttermLowLoadLogWaitInterval() {
			return shorttermLowLoadLogInterval_;
		}

		bool setShorttermHighLoadLogWaitInterval(int32_t size) {
			if (size < 0 || size > INT32_MAX) {
				return false;
			}
			shorttermHighLoadLogInterval_ = size;
			return true;
		}

		int32_t getShorttermHighLoadLogWaitInterval() {
			return shorttermHighLoadLogInterval_;
		}

		bool setLongtermLowLoadLogWaitInterval(int32_t size) {
			if (size < 0 || size > INT32_MAX) {
				return false;
			}
			longtermLowLoadLogInterval_ = size;
			return true;
		}

		int32_t getLongtermLowLoadLogWaitInterval() {
			return longtermLowLoadLogInterval_;
		}

		bool setLongtermHighLoadLogWaitInterval(int32_t size) {
			if (size < 0 || size > INT32_MAX) {
				return false;
			}
			longtermHighLoadLogInterval_ = size;
			return true;
		}

		int32_t getLongtermHighLoadLogWaitInterval() {
			return longtermHighLoadLogInterval_;
		}

		bool setLongtermLowLoadChunkWaitInterval(int32_t size) {
			if (size < 0 || size > INT32_MAX) {
				return false;
			}
			longtermLowLoadChunkInterval_ = size;
			return true;
		}

		int32_t getLongtermLowLoadChunkWaitInterval() {
			return longtermLowLoadChunkInterval_;
		}

		bool setLongtermHighLoadChunkWaitInterval(int32_t size) {
			if (size < 0 || size > INT32_MAX) {
				return false;
			}
			longtermHighLoadChunkInterval_ = size;
			return true;
		}

		int32_t getLongtermHighLoadChunkWaitInterval() {
			return longtermHighLoadChunkInterval_;
		}

	private:
		int32_t lockConflictPendingInterval_;
		int32_t longtermNearestLsnGap_;
		int32_t longtermNearestInterval_;
		int32_t shorttermLimitQueueSize_;
		int32_t shorttermLowLoadLogInterval_;
		int32_t shorttermHighLoadLogInterval_;
		int32_t longtermLimitQueueSize_;
		int32_t longtermLowLoadLogInterval_;
		int32_t longtermHighLoadLogInterval_;
		int32_t longtermLowLoadChunkInterval_;
		int32_t longtermHighLoadChunkInterval_;
		int32_t longtermDumpChunkInterval_;
	};



	util::RWLock &getAllocLock() {
		return allocLock;
	}

	void createPartition(PartitionId pId);



	static class ConfigSetUpHandler : public ConfigTable::SetUpHandler {
		virtual void operator()(ConfigTable &config);
	} configSetUpHandler_;



	util::FixedSizeAllocator<util::Mutex> fixedSizeAlloc_;

	util::StackAllocator alloc_;

	SyncVariableSizeAllocator varSizeAlloc_;

	util::RWLock allocLock;

	SyncContextTable **syncContextTables_;
	PartitionTable *pt_;

	SyncConfig syncConfig_;
	ExtraConfig extraConfig_;

	uint8_t *chunkBufferList_;
	PartitionId currentSyncPId_;
	SyncId currentSyncId_;
	PartitionRevision currentRev_;

	Size_t chunkSize_;
	int32_t recoveryLevel_;
	int32_t syncMode_;

	std::vector<bool> undoPartitionList_;
};

#endif
