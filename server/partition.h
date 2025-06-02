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
	@brief Definition of Partition
*/
#ifndef PARTITION_H_
#define PARTITION_H_

#include "utility_v5.h"
#include "data_type.h"
#include "chunk_manager.h"
#include "chunk.h"
#include "config_table.h"
#include "log_manager.h"
#include "transaction_manager.h" 
#include <vector>
#include <string>
#include <memory>
#include <random>
#include <mutex>

class ConfigTable;
struct ManagerSet;
struct DataStoreConfig;

template <class L> class LogManager;
class TransactionManager;
class ResultSetManager;
class ClusterService;
class TransactionContext;
class KeyDataStoreValue;
class KeyDataStore;
class RecoveryManager;


enum RedoMode {
	REDO_MODE_REPLICATION,	  
	REDO_MODE_SHORT_TERM_SYNC,  
	REDO_MODE_LONG_TERM_SYNC,   
	REDO_MODE_RECOVERY,		   
	REDO_MODE_RECOVERY_CHUNK	,
	REDO_MODE_ARCHIVE,
	REDO_MODE_ARCHIVE_REPLICATION,
};
class DataStoreStats;
class ObjectManagerStats;

/*!
	@brief Throws exception of log redo failure.
*/
class LogRedoException : public util::Exception {
public:
	explicit LogRedoException(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_DECL) throw() :
			Exception(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_SET) {}
	virtual ~LogRedoException() throw() {}
};

/*!
	@brief データストア基底クラス
*/
class DataStoreBase {
protected:
	util::StackAllocator* stAlloc_;
	util::FixedSizeAllocator<util::Mutex>* resultSetPool_;
	TransactionManager* txnMgr_;
	ChunkManager* chunkManager_;
	LogManager<MutexLocker>* logManager_;

public:
	enum class Support {
		TRIGGER,
	};
	struct StatsSet {
		StatsSet(DataStoreStats &dsStats, ObjectManagerStats &objMgrStats) :
				dsStats_(dsStats),
				objMgrStats_(objMgrStats) {
		}
		DataStoreStats &dsStats_;
		ObjectManagerStats &objMgrStats_;
	};
	class Scope {
		TransactionContext* txn_;
		DataStoreBase* ds_;
	public:
		Scope(TransactionContext* txn, DataStoreBase* ds, ClusterService* clusterService)
			: txn_(txn), ds_(ds) {
			ds->preProcess(txn_, clusterService);
		}
		~Scope() {
			ds_->postProcess(txn_);
		}
	};

	DataStoreBase(
			util::StackAllocator* stAlloc,
			util::FixedSizeAllocator<util::Mutex>* resultSetPool,
			ConfigTable* configTable, TransactionManager* txnMgr, ChunkManager* chunkManager,
			LogManager<MutexLocker>* logManager, KeyDataStore* keyStore) {
		UNUSED_VARIABLE(configTable);
		UNUSED_VARIABLE(keyStore);
		stAlloc_ = stAlloc;
		resultSetPool_ = resultSetPool;
		txnMgr_ = txnMgr;
		chunkManager_ = chunkManager;
		logManager_ = logManager;
	}
	virtual ~DataStoreBase() {};
	virtual void initialize(ManagerSet &resourceSet) {
		UNUSED_VARIABLE(resourceSet);
	};
	virtual Serializable* exec(TransactionContext* txn, KeyDataStoreValue* storeValue, Serializable* message) = 0;
	virtual void finalize() = 0;
	virtual void redo(
			util::StackAllocator &alloc, RedoMode mode, const util::DateTime &redoStartTime, 
			Timestamp redoStartEmTime,
			Serializable* message) = 0;
	virtual bool support(Support type) = 0;
	virtual void preProcess(TransactionContext* txn, ClusterService* clsService) = 0;
	virtual void postProcess(TransactionContext* txn) = 0;
	virtual void activate(
		TransactionContext& txn, ClusterService* clusterService) = 0;
	virtual StoreType getStoreType() = 0;

	static const size_t RESULTSET_POOL_BLOCK_SIZE_BITS = 20;
};


enum CheckpointPhase {
	CP_PHASE_NONE,
	CP_PHASE_PRE_WRITE,
	CP_PHASE_PRE_SYNC,
	CP_PHASE_FINISH
};

enum EndCheckpointMode {
	CP_END_FLUSH_DATA,
	CP_END_ADD_CPLOG,
	CP_END_FLUSH_CPLOG
};

class InterchangeableStoreMemory;
class ChunkManager;
class DataStoreBase;
template <class L> class LogManager;
class VirtualFileBase;
class Log;
class ChunkCopyContext;
class MeshedChunkTable;
class TransactionManager;
class DataStoreLogV4;
class PartitionList;
/*!
	@brief パーティション処理クラス
*/
class Partition {
private:
	static const size_t STORE_MEM_POOL_SIZE_BITS = 12; 
	static const uint64_t ADJUST_MEMORY_TIME_INTERVAL_ = 30 * 1000 * 1000;  
public:
	static const int32_t PARTIAL_CHECKPOINT_INTERVAL = 2;
	static const int32_t VARINT_MAX_LEN = 9;

	static bool dumpMode_; 

	typedef util::FixedSizeAllocator<util::Mutex> FixedMemoryPool;
	typedef util::VariableSizeAllocator<util::NoopMutex,
			util::VariableSizeAllocatorTraits<128, 1024, 1024 * 8> >
			PartitionAllocator;

	struct StatsSet {
		StatsSet(
				FileStatTable &dataFileStats,
				LogManagerStats &logMgrStats,
				ChunkManagerStats &chunkMgrStats,
				const DataStoreBase::StatsSet &dsStats);

		FileStatTable &dataFileStats_;
		LogManagerStats &logMgrStats_;
		ChunkManagerStats &chunkMgrStats_;
		DataStoreBase::StatsSet dsStats_;
	};
	ConfigTable& getConfig() {
		return configTable_;
	}
	std::string& getUuid() {
		return uuid_;
	}

private:
	enum Status {
		NotAvailablePartition,
		RestoredPartition,
		ActivePartition
	};

	Status status_;
	void setStatus(Status status) { status_ = status; }
	Status getStatus() { return status_; }
	bool isAvailable() { return (status_ != Status::NotAvailablePartition); }

	ConfigTable& configTable_;
	ChunkManager* chunkManager_;
	FixedMemoryPool chunkMemoryPool_;
	util::StackAllocator chunkStackAlloc_;
	DataStoreBase* dataStore_;
	DataStoreBase* keyStore_;
	UTIL_UNIQUE_PTR<ResultSetManager> rsManager_;

	LogManager<MutexLocker>* defaultLogManager_;
	FixedMemoryPool logMemoryPool_;
	util::StackAllocator logStackAlloc_;
	WALBuffer* cpWALBuffer_;

	VirtualFileBase* dataFile_;

	ManagerSet* managerSet_;
	FixedMemoryPool storeMemoryPool_;
	util::StackAllocator storeStackAlloc_;
	util::FixedSizeAllocator<util::Mutex>& resultSetPool_;

	int32_t pId_;
	const char* const tmpPath_;
	WALBuffer* longtermSyncWalBuffer_;
	LogManager<MutexLocker>* longtermSyncLogManager_;

	int32_t checkpointRange_;

	uint64_t lastCpLsn_;

	util::Mutex &lock_;


	StatsSet statsSet_;
	std::string uuid_;

public:
	Partition(
			ConfigTable& configTable,
			int32_t pId, InterchangeableStoreMemory& ism,
			util::FixedSizeAllocator<util::Mutex>& resultSetPool,
			Chunk::ChunkMemoryPool& chunkMemoryPool,
			ChunkBuffer* chunkBuffer,
			const char* dataPath, const char* logPath,
			int32_t initialBufferSize, int32_t chunkSize,
			const StatsSet &statsSet);

	virtual ~Partition();

	void initialize(ManagerSet &resourceSet);

	void fin();

	void drop();

	void reinit(PartitionList& ptList);

	void recover(
			InterchangeableStoreMemory* ism, LogSequentialNumber targetLsn = 0,
			RecoveryManager *rm = NULL, int32_t workerId = -1);

	void restore(const uint8_t* logListBinary, uint32_t size);

	void activate();

	void catchup(uint8_t* chunk, uint32_t size, int64_t count);

	uint32_t undo(util::StackAllocator& alloc);

	bool isActive();

	void finalizeRecovery(bool commit);

	bool needCheckpoint(bool updateLastLsn);

	void fullCheckpoint(CheckpointPhase phase);

	void startCheckpoint0(bool withFlush);

	void startCheckpoint(const DuplicateLogMode *duplicateMode = NULL);
	void closePrevLogFiles();

	void executeCheckpoint0();

	int32_t executePartialCheckpoint(
			MeshedChunkTable::CsectionChunkCursor& cursor, int32_t maxCount,
			int32_t checkpointRange);


	void endCheckpoint0(CheckpointPhase phase);

	void endCheckpoint(EndCheckpointMode x);
	LogIterator<MutexLocker> endCheckpointPrepareRelaseBlock();
	void endCheckpointReleaseBlock(const Log* log);
	void removeLogFiles(int32_t range);
	int64_t postCheckpoint(
			int32_t mode, UtilBitmap& bitmap, const std::string& path,
			bool enableDuplicateLog, bool skipBaseLine);
	int32_t calcCheckpointRange(int32_t mode);


	void flushData();

	bool redo(RedoMode mode, Log* log);

	bool redoLogList(
		util::StackAllocator* alloc, RedoMode mode,
		const util::DateTime &redoStartTime, 
		Timestamp redoStartEmTime,
		const uint8_t* data, size_t dataLen, size_t& pos,
		util::Stopwatch* watch, int32_t limitInterval);

	void genTempLogManager();

	int64_t collectOffset(int32_t mode, UtilBitmap& bitmap);

	util::Mutex& mutex() { return lock_; } 

	DataStoreBase& dataStore();

	DataStoreBase& keyStore();

	LogManager<MutexLocker>& logManager();
	ChunkManager& chunkManager();

	ConfigTable& config() { return configTable_; }

	LogSequentialNumber getLSN();
	VirtualFileBase* dataFile() { return dataFile_; }


private:
	void initializeLogFormatVersion(uint16_t xlogFormatVersion);

	util::XArray<uint8_t>* appendDataStoreLog(
			TransactionContext& txn,
			const TransactionManager::ContextSource& cxtSrc, StatementId stmtId,
			StoreType storeType, DataStoreLogV4* dsMes);

	util::XArray<uint8_t>* appendDataStoreLog(TransactionContext& txn,
			const ClientId& clientId,
			const TransactionManager::ContextSource& cxtSrc, StatementId stmtId,
			StoreType storeType, DataStoreLogV4* dsMes);

	void removeAllLogFiles();
	uint64_t copyChunks(
			UtilBitmap& bitmap, const char* dataPath, const char* xLogPath,
			bool keepSrcOffset);
	void copyCpLog(const char* path, uint8_t* buffer, size_t bufferSize);
	void copyXLog(const char* path, uint8_t* buffer, size_t bufferSize);
	void writeTxnLog(util::StackAllocator &alloc);
	void redoTxnLog(Log* log);
};

struct PartitionListStats;

/*!
	@brief パーティションオブジェクト群の管理クラス
*/
class PartitionList {
public:
	static const int32_t INITIAL_X_WAL_BUFFER_SIZE;
	static const int32_t INITIAL_CP_WAL_BUFFER_SIZE;
	static const int32_t INITIAL_AFFINITY_MANAGER_SIZE;
	static const std::string CLUSTER_SNAPSHOT_INFO_FILE_NAME;

	PartitionList(::ConfigTable& config, TransactionManager* txnMgr,
				  util::FixedSizeAllocator<util::Mutex>& resultsetPool);

	virtual ~PartitionList();

	Partition& partition(size_t pId);
	InterchangeableStoreMemory& interchangeableStoreMemory();

	void initialize(ManagerSet & resourceSet);

	PartitionGroupConfig& getPgConfig() { return pgConfig_; }

	Chunk::ChunkMemoryPool& getChunkMemoryPool() { return *chunkMemoryPool_; }

	ChunkBuffer& chunkBuffer(size_t pgId);

	ConfigTable& getConfigTable() { return configTable_; }

	const PartitionListStats& getStats() { return *stats_; }


	/*!
		@brief Configuration of DataStore
	*/
	struct Config : public ConfigTable::ParamHandler {
		Config(ConfigTable &configTable, PartitionList* partitionList);

		void setUpConfigHandler(ConfigTable& configTable);
		virtual void operator()(
				ConfigTable::ParamId id, const ParamValue &value);

		uint64_t getAtomicStoreMemoryLimit() {
			return atomicStoreMemoryLimit_;
		}
		bool setAtomicStoreMemoryLimit(uint64_t memoryLimitByte);


		int32_t getAtomicShiftableMemRate() {
			return atomicShiftableMemRate_;
		}
		bool setAtomicShiftableMemRate(int32_t rate);

		int32_t getAtomicEMAHalfLifePeriod() {
			return atomicEMAHalfLifePeriod_;
		}
		bool setAtomicEMAHalfLifePeriod(int32_t period);

		double getAtomicStoreMemoryColdRate() {
			return static_cast<double>(atomicStoreMemoryColdRate_) / 1.0E6;
		}
		bool setAtomicStoreMemoryColdRate(double rate);

		uint64_t getAtomicCpFileFlushSize() {
			return atomicCpFileFlushSize_;
		}
		bool setAtomicCpFileFlushSize(uint64_t size);

		bool getAtomicCpFileAutoClearCache() {
			return atomicCpFileAutoClearCache_;
		}
		bool setAtomicCpFileAutoClearCache(bool flag);

		static uint32_t getChunkSize(uint32_t chunkExpSize);
		static uint32_t getChunkSize(const ConfigTable &configTable);
		static uint32_t getChunkExpSize(const ConfigTable &configTable);

		PartitionList* partitionList_;
		uint32_t partitionCount_;
		uint32_t chunkExpSize_;

		util::Atomic<uint64_t> atomicStoreMemoryLimit_;
		util::Atomic<uint32_t> atomicShiftableMemRate_;
		util::Atomic<uint32_t> atomicEMAHalfLifePeriod_;
		util::Atomic<uint64_t> atomicStoreMemoryColdRate_;
		util::Atomic<uint64_t> atomicCpFileFlushSize_;
		util::Atomic<uint64_t> atomicCpFileFlushInterval_;
		util::Atomic<bool> atomicCpFileAutoClearCache_;
	};

	Config& config() { return config_; }

private:
	util::VariableSizeAllocator<> varAlloc_;
	std::vector< Partition* > ptList_;
	std::vector< ChunkBuffer* > chunkBufferList_;
	InterchangeableStoreMemory* ism_;
	Config config_;

	PartitionGroupConfig pgConfig_;
	ConfigTable& configTable_;
	util::FixedSizeAllocator<util::Mutex>& resultSetPool_;
	Chunk::ChunkMemoryPool* chunkMemoryPool_;
	TransactionManager* transactionManager_;
	uint32_t partitionCount_;
	util::AllocUniquePtr<PartitionListStats> stats_;
	ManagerSet* managerSet_;

public:
	static class ConfigSetUpHandler : public ConfigTable::SetUpHandler {
		virtual void operator()(ConfigTable &config);
	} configSetUpHandler_;
};

class ChunkBufferStats;

/*!
	@brief DBMS全体の統計値
*/
struct PartitionListStats {
public:
	enum Param {


		PL_STAT_STORE_MEMORY,
		PL_STAT_POOL_BUFFER_MEMORY, 


		PL_STAT_STORE_MEMORY_LIMIT,
		PL_STAT_ERASABLE_EXPIRED_TIME,

		PL_STAT_COMPRESSION_MODE, 
		PL_STAT_BACKGROUND_MIN_RATE, 
		PL_STAT_BATCH_SCAN_INTERVAL_NUM, 

		PL_STAT_END
	};

	typedef LocalStatTable<Param, PL_STAT_END> Table;

	struct SubEntrySet;
	struct PartitionEntry;
	struct PartitionGroupEntry;
	struct TotalEntry;
	struct Mapper;

	class Updator;
	class SetUpHandler;

	typedef util::AllocVector<PartitionEntry*> PTList;
	typedef util::AllocVector<PartitionGroupEntry*> PGList;

	PartitionListStats(
			const util::StdAllocator<void, void> &alloc,
			const ConfigTable& configTable, PartitionList::Config &config);

	void initialize(
			StatTable &stat, const DataStoreConfig &dsConfig,
			Chunk::ChunkMemoryPool &chunkMemoryPool,
			InterchangeableStoreMemory &ism);

	Partition::StatsSet toPartitionStatsSet(PartitionId pId);
	PartitionEntry& getPartitionEntry(PartitionId pId);
	PartitionGroupEntry& getPartitionGroupEntry(PartitionGroupId pgId);

	void getPGChunkBufferStats(
			PartitionGroupId pgId, ChunkBufferStats &stats) const;
	void getPGCategoryChunkBufferStats(
			PartitionGroupId pgId, ChunkCategoryId categoryId,
			ChunkBufferStats &stats) const;

private:
	util::StdAllocator<void, void> alloc_;
	const ConfigTable &configTable_;
	PartitionList::Config &config_;

	util::AllocUniquePtr<Mapper> mapper_;
	util::AllocUniquePtr<SubEntrySet> sub_;
	util::AllocUniquePtr<Updator> updator_;
};

class PartitionListStats::Updator : public StatTable::StatUpdator {
public:
	Updator(
			PartitionListStats &base, const DataStoreConfig &dsConfig,
			Chunk::ChunkMemoryPool &chunkMemoryPool);
	virtual bool operator()(StatTable &stat);

private:
	void updatePartitionListStats(
			PartitionListStats::Table &stats,
			const PartitionGroupEntry &pgEntry);

	PartitionListStats &base_;
	const DataStoreConfig &dsConfig_;
	Chunk::ChunkMemoryPool &chunkMemoryPool_;
};

class PartitionListStats::SetUpHandler : public StatTable::SetUpHandler {
	virtual void operator()(StatTable& stat);

	static SetUpHandler instance_;
};

/*!
	@brief データストア取得
*/
inline DataStoreBase& Partition::dataStore() {
	assert(isActive() && dataStore_ != NULL);
	return *dataStore_;
}

/*!
	@brief キーストア取得
*/
inline DataStoreBase& Partition::keyStore() {
	assert(keyStore_ != NULL);
	return *keyStore_;
}

/*!
	@brief ログマネージャ取得
*/
inline LogManager<MutexLocker>& Partition::logManager() {
	assert(defaultLogManager_ != NULL);
	return *defaultLogManager_;
}
/*!
	@brief チャンクマネージャ取得
*/
inline ChunkManager& Partition::chunkManager() {
	assert(chunkManager_ != NULL);
	return *chunkManager_;
}

/*!
	@brief パーティション取得
*/
inline Partition& PartitionList::partition(size_t pId) {
	assert(pId < ptList_.size());
	if (pId >= ptList_.size()) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Illeagal PID: " << pId);
	}
	return *ptList_[pId];
}

inline InterchangeableStoreMemory& PartitionList::interchangeableStoreMemory() {
	return *ism_;
}

inline ChunkBuffer& PartitionList::chunkBuffer(size_t pgId) {
	assert(pgId < chunkBufferList_.size());
	return *chunkBufferList_[pgId];
}
#endif 
