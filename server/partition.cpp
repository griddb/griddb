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
	@brief Implementation of Partition
*/
#include <cassert>
#include "partition.h"
#include "log_manager.h"
#include "config_table.h"
#include "interchangeable.h"
#include "affinity_manager.h"
#include "chunk_buffer.h"
#include "data_store_v4.h"
#include "key_data_store.h"
#include "data_store_log_v4.h"
#include "picojson.h"
#include "cluster_service.h" 
#include "container_message_v4.h"
#include "transaction_manager.h"
#include "result_set.h"
#include "recovery_manager.h" 

UTIL_TRACER_DECLARE(PARTITION);  
UTIL_TRACER_DECLARE(PARTITION_DETAIL);
UTIL_TRACER_DECLARE(SIZE_MONITOR);
UTIL_TRACER_DECLARE(IO_MONITOR);
UTIL_TRACER_DECLARE(RECOVERY_MANAGER);


const int32_t PartitionList::INITIAL_X_WAL_BUFFER_SIZE = 256 * 1024;
const int32_t PartitionList::INITIAL_CP_WAL_BUFFER_SIZE = 4 * 1024;
const int32_t PartitionList::INITIAL_AFFINITY_MANAGER_SIZE = 1024;
const std::string PartitionList::CLUSTER_SNAPSHOT_INFO_FILE_NAME(
		"gs_cluster_snapshot_info.json");

struct PartitionListStats::SubEntrySet {
	explicit SubEntrySet(const util::StdAllocator<void, void> &alloc);
	~SubEntrySet();

	void initializeSub(const Mapper &mapper, const ConfigTable& configTable);

	template<typename E>
	void initializetList(
			util::AllocVector<E*> &list, const Mapper &mapper, uint32_t count);
	template<typename E>
	void clearList(util::AllocVector<E*> &list);

	template<typename E>
	static E& getListElement(util::AllocVector<E*> &list, size_t index);
	template<typename E>
	static const E& getListElementRef(
			const util::AllocVector<E*> &list, size_t index);

	util::StdAllocator<void, void> alloc_;
	PTList ptList_;
	PGList pgList_;
	FileStatTable dataFileStats_;
	InterchangeableStoreMemory *ism_;
};

struct PartitionListStats::PartitionEntry {
	explicit PartitionEntry(const Mapper &mapper);

	void apply(StatTable &stat) const;
	void merge(const PartitionEntry &entry);

	LogManagerStats logMgrStats_;
	ChunkManagerStats chunkMgrStats_;
	ObjectManagerStats objMgrStats_;
	DataStoreStats dsStats_;
};

struct PartitionListStats::PartitionGroupEntry {
	explicit PartitionGroupEntry(const Mapper &mapper);

	void apply(StatTable &stat) const;
	void merge(const PartitionGroupEntry &entry);

	ChunkBufferStats chunkBufStats_;
};

struct PartitionListStats::TotalEntry {
	explicit TotalEntry(const Mapper &mapper);

	void apply(StatTable &stat) const;
	void merge(FileStatTable &dataFileStats, const PartitionEntry &ptEntry);

	FileStatTable::BaseTable fileStats_;
	Table table_;
};

struct PartitionListStats::Mapper {
	Mapper(const util::StdAllocator<void, void> &alloc, uint32_t chunkSize);

	static void setUpDataFileMapper(FileStatTable::BaseMapper &mapper);

	static void setUpMapper(Table::Mapper &mapper);
	static void setUpBasicMapper(Table::Mapper &mapper, bool detail);
	static void setUpCPMapper(Table::Mapper &mapper, bool detail);
	static void setUpDSMapper(Table::Mapper &mapper);
	static void setUpTxnMapper(Table::Mapper &mapper);

	FileStatTable::BaseMapper dataFileMapper_;

	LogManagerStats::Mapper logMgrMapper_;
	ChunkManagerStats::Mapper chunkMgrMapper_;
	ObjectManagerStats::Mapper objMgrMapper_;
	DataStoreStats::Mapper dsMapper_;

	ChunkBufferStats::Mapper chunkBufMapper_;
	Table::Mapper partitionListMapper_;
};

/*!
	@brief コンストラクタ
*/
Partition::Partition(
		ConfigTable& configTable, int32_t pId, InterchangeableStoreMemory& ism,
		util::FixedSizeAllocator<util::Mutex>& resultSetPool,
		Chunk::ChunkMemoryPool& chunkMemoryPool, ChunkBuffer* chunkBuffer,
		const char* dataPath, const char* logPath,
		int32_t initialBufferSize, int32_t chunkSize,
		const StatsSet &statsSet) :
		configTable_(configTable), chunkManager_(NULL),
		chunkMemoryPool_(util::AllocatorInfo(ALLOCATOR_GROUP_STORE, "chunkFixed"),
			1 << STORE_MEM_POOL_SIZE_BITS),
		chunkStackAlloc_(util::AllocatorInfo(ALLOCATOR_GROUP_STORE, "chunkFixed"), &chunkMemoryPool_),
		dataStore_(NULL), keyStore_(NULL),
		defaultLogManager_(NULL),
		logMemoryPool_(util::AllocatorInfo(ALLOCATOR_GROUP_LOG, "logFixed"),
			1 << STORE_MEM_POOL_SIZE_BITS),
		logStackAlloc_(util::AllocatorInfo(ALLOCATOR_GROUP_LOG, "logFixed"), &logMemoryPool_),
		cpWALBuffer_(NULL), dataFile_(NULL), managerSet_(NULL),
		storeMemoryPool_(util::AllocatorInfo(ALLOCATOR_GROUP_STORE, "dataStoreFixed"),
			1 << STORE_MEM_POOL_SIZE_BITS),
		storeStackAlloc_(util::AllocatorInfo(ALLOCATOR_GROUP_STORE, "dataStoreFixed"), &storeMemoryPool_),
		resultSetPool_(resultSetPool), pId_(pId),
		tmpPath_(configTable.get<const char8_t*>(CONFIG_TABLE_DS_SYNC_TEMP_PATH)),
		checkpointRange_(configTable.getUInt32(CONFIG_TABLE_CP_PARTIAL_CHECKPOINT_INTERVAL)),
		lastCpLsn_(-1),
		lock_(chunkBuffer->mutex()),
		statsSet_(statsSet)
{
	UNUSED_VARIABLE(chunkMemoryPool);
	UNUSED_VARIABLE(initialBufferSize);

	try {
		int32_t stripeSize = configTable.getUInt32(CONFIG_TABLE_DS_DB_FILE_SPLIT_STRIPE_SIZE);
		int32_t numFiles = configTable.getUInt32(CONFIG_TABLE_DS_DB_FILE_SPLIT_COUNT);

		setStatus(Status::NotAvailablePartition);

		{
			util::NormalOStringStream oss;
			oss << dataPath << "/" << pId_;
			std::unique_ptr<NoLocker> noLocker(UTIL_NEW NoLocker());
			dataFile_ = UTIL_NEW VirtualFileNIO(
					std::move(noLocker), oss.str().c_str(), pId_,
					stripeSize, numFiles, chunkSize,
					&statsSet_.dataFileStats_);
		}
		dataFile_->open();

		WALBuffer* xWALBuffer = UTIL_NEW WALBuffer(pId, statsSet_.logMgrStats_);
		ism.regist(pId, xWALBuffer);
		cpWALBuffer_ = UTIL_NEW WALBuffer(pId_, statsSet_.logMgrStats_);
		cpWALBuffer_->resize(PartitionList::INITIAL_CP_WAL_BUFFER_SIZE);
		AffinityManager* affinityManager = UTIL_NEW AffinityManager(pId);
		ism.regist(pId, affinityManager);

		{
			util::NormalOStringStream ossCpLog;
			ossCpLog << dataPath << "/" << pId_;
			util::NormalOStringStream ossXLog;
			ossXLog << logPath << "/" << pId;
			std::unique_ptr<MutexLocker> locker(UTIL_NEW MutexLocker());
			defaultLogManager_ = UTIL_NEW LogManager<MutexLocker>(
					configTable, std::move(locker), logStackAlloc_,
					*xWALBuffer, *cpWALBuffer_,
					checkpointRange_, ossCpLog.str().c_str(),
					ossXLog.str().c_str(), pId, statsSet_.logMgrStats_, uuid_);
		}
		chunkBuffer->setDataFile(pId, *dataFile_);
		chunkManager_ = UTIL_NEW ChunkManager(
				configTable, *defaultLogManager_, *chunkBuffer,
				*affinityManager, chunkSize, pId,
				statsSet_.chunkMgrStats_);

		DataStoreV4::createResultSetManager(
				resultSetPool_, configTable, *chunkBuffer, rsManager_);

	} catch (std::exception &e) {  
		GS_RETHROW_SYSTEM_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e, ""));
	}
}

/*!
	@brief デストラクタ
*/
Partition::~Partition() {
	delete dataStore_;
	dataStore_ = NULL;
	delete keyStore_;
	keyStore_ = NULL;
	delete chunkManager_;
	chunkManager_ = NULL;
	delete defaultLogManager_;
	defaultLogManager_ = NULL;
	delete cpWALBuffer_;
	cpWALBuffer_ = NULL;
	delete dataFile_;
	dataFile_ = NULL;
}

/*!
	@brief drop後の再初期化
*/
void Partition::reinit(PartitionList& ptList) {

	try {
		setStatus(Status::NotAvailablePartition);

		delete dataStore_;
		dataStore_ = NULL;
		delete keyStore_;
		keyStore_ = NULL;
		delete chunkManager_;
		chunkManager_ = NULL;
		delete defaultLogManager_;
		defaultLogManager_ = NULL;
		delete cpWALBuffer_;
		cpWALBuffer_ = NULL;
		delete dataFile_;
		dataFile_ = NULL;

		const uint32_t chunkSize =
				PartitionList::Config::getChunkSize(configTable_);
		const int32_t stripeSize = configTable_.getUInt32(
				CONFIG_TABLE_DS_DB_FILE_SPLIT_STRIPE_SIZE);
		const int32_t numFiles = configTable_.getUInt32(
				CONFIG_TABLE_DS_DB_FILE_SPLIT_COUNT);
		const char* const dataPath =
				configTable_.get<const char8_t*>(CONFIG_TABLE_DS_DB_PATH);
		const char* const logPath =
				configTable_.get<const char8_t*>(CONFIG_TABLE_DS_TRANSACTION_LOG_PATH);

		{
			util::NormalOStringStream oss;
			oss << dataPath << "/" << pId_;
			std::unique_ptr<NoLocker> noLocker(UTIL_NEW NoLocker());
			dataFile_ = UTIL_NEW VirtualFileNIO(
					std::move(noLocker), oss.str().c_str(), pId_,
					stripeSize, numFiles, chunkSize,
					&statsSet_.dataFileStats_);
		}
		dataFile_->open();

		InterchangeableStoreMemory& ism = ptList.interchangeableStoreMemory();
		PartitionGroupConfig& pgConfig = ptList.getPgConfig();

		WALBuffer* xWALBuffer = UTIL_NEW WALBuffer(pId_, statsSet_.logMgrStats_);
		ism.regist(pId_, xWALBuffer);
		cpWALBuffer_ = UTIL_NEW WALBuffer(pId_, statsSet_.logMgrStats_);
		cpWALBuffer_->resize(PartitionList::INITIAL_CP_WAL_BUFFER_SIZE);
		AffinityManager* affinityManager = UTIL_NEW AffinityManager(pId_);
		ism.regist(pId_, affinityManager);

		{
			util::NormalOStringStream ossCpLog;
			ossCpLog << dataPath << "/" << pId_;
			util::NormalOStringStream ossXLog;
			ossXLog << logPath << "/" << pId_;

			std::unique_ptr<MutexLocker> locker(UTIL_NEW MutexLocker());
			defaultLogManager_ = UTIL_NEW LogManager<MutexLocker>(
					configTable_, std::move(locker), logStackAlloc_,
					*xWALBuffer, *cpWALBuffer_,
					checkpointRange_, ossCpLog.str().c_str(),
					ossXLog.str().c_str(), pId_, statsSet_.logMgrStats_, uuid_);

			defaultLogManager_->init(0, true, false);
			defaultLogManager_->setLSN(0);

			Log log(LogType::CheckpointEndLog, defaultLogManager_->getLogFormatVersion());
			log.setLsn(0);
			defaultLogManager_->appendCpLog(log);
			defaultLogManager_->appendXLog(LogType::CheckpointStartLog, NULL, 0, NULL);

			defaultLogManager_->flushCpLog(LOG_WRITE_WAL_BUFFER, false);
			defaultLogManager_->flushCpLog(LOG_FLUSH_FILE, false);
			defaultLogManager_->flushXLog(LOG_WRITE_WAL_BUFFER, false);
			defaultLogManager_->flushXLog(LOG_FLUSH_FILE, false);
		}
		PartitionGroupId pgId = pgConfig.getPartitionGroupId(pId_);
		ChunkBuffer* chunkBuffer = ism.getChunkBuffer(pgId);
		assert(chunkBuffer);
		chunkBuffer->setDataFile(pId_, *dataFile_);
		chunkManager_ = UTIL_NEW ChunkManager(
				configTable_, *defaultLogManager_, *chunkBuffer,
				*affinityManager, chunkSize, pId_,
				statsSet_.chunkMgrStats_);
	} catch (std::exception &e) {  
		GS_RETHROW_SYSTEM_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e, ""));
	}
}
#include "sync_service.h"
/*!
	@brief 初期化処理
*/
void Partition::initialize(ManagerSet &resourceSet) {
	managerSet_ = &resourceSet;
	bool uuidInfoFlag = managerSet_->config_->get<bool>(CONFIG_TABLE_DS_AUTO_ARCHIVE_OUTPUT_UUID_INFO);
	if (uuidInfoFlag) {
		uuid_ = managerSet_->syncSvc_->getRedoManager().getUuid();
		defaultLogManager_->setUuid(uuid_);
	}
}

/*!
	@brief 終了処理
*/
void Partition::fin() {
	try {
		if (dataStore_) {
			dataStore_->finalize();
		}
		if (keyStore_) {
			keyStore_->finalize();
		}

		chunkManager_->fin();

		const bool byCP = false;
		defaultLogManager_->fin(byCP);

		if (dataFile_ != nullptr) {
			dataFile_->close();
		}
	} catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e, ""));
	}
}

/*!
	@brief 全リソースの削除（finよりも強い）
*/
void Partition::drop() {
	try {
		MeshedChunkTable::StableChunkCursor cursor =
				chunkManager_->createStableChunkCursor();
		MeshedChunkTable::Group* group = NULL;
		RawChunkInfo chunkInfo;
		ChunkAccessor ca;
		while (cursor.next(chunkInfo)) {
			if (chunkInfo.getGroupId() != -1) {
				if (!group || group->getId() != chunkInfo.getGroupId()) {
					group = chunkManager_->putGroup(chunkInfo.getGroupId());
				}
				chunkManager_->removeChunk(*group, chunkInfo.getChunkId());
			}
		}
		fin();
		if (dataFile_ != NULL) {
			dataFile_->remove();
		}
		removeAllLogFiles();
	} catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e, ""));
	}
}

/*!
	@brief LSN取得
*/
LogSequentialNumber Partition::getLSN() {
	return defaultLogManager_->getLSN(); 
}


/*!
	@brief 通常リカバリ
	@note recoveryTargetLSNが0以外の時、XLogのredo中にrecoveryTargetLSNと一致するLSNを持つログレコードが存在しなかった場合にはシステムエラー例外が送出される
*/
void Partition::recover(
		InterchangeableStoreMemory* ism, LogSequentialNumber recoveryTargetLsn,
		RecoveryManager* recoveryMgr, int32_t workerId) {
	try {
		const uint64_t startTime = util::Stopwatch::currentClock();
		uint64_t adjustTime = startTime;

		const bool byCP = false;
		std::tuple<LogIterator<MutexLocker>, LogIterator<MutexLocker>> its =
				defaultLogManager_->init(byCP);
		if (recoveryMgr) {
			recoveryMgr->setParallelRecoveryReady();
		}
		{
			LogIterator<MutexLocker> it = defaultLogManager_->createChunkDataLogIterator();
			std::unique_ptr<Log> log;
			while (true) {
				util::StackAllocator::Scope scope(defaultLogManager_->getAllocator());
				log = it.next(true);
				if (log == NULL) break;
				if (!log->checkSum()) {
					GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_INVALID_CHUNK_DATA,
						"ChunkData is wrong");
				}
				redo(REDO_MODE_RECOVERY, log.get());
			}
			it.fin();
		}
		if (recoveryMgr && recoveryMgr->isErrorOccured()) {
			GS_TRACE_INFO(RECOVERY_MANAGER, GS_TRACE_RM_RECOVERY_INFO,
					"Recovery process has been interrupted.");
			return;
		}
		const uint64_t ph1Time = util::Stopwatch::currentClock();
		int32_t redoCount = 0;
		{
			LogIterator<MutexLocker>& it = std::get<0>(its);
			std::unique_ptr<Log> log;
			while (true) {
				util::StackAllocator::Scope scope(defaultLogManager_->getAllocator());
				log = it.next(true);
				if (log == NULL) break;
				if (!log->checkSum()) {
					GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_RECOVERY_FAILED,
						"CPLog redo failed");
				}
				redo(REDO_MODE_RECOVERY, log.get());
				++redoCount;
				if (log->getType() == LogType::TXNLog 
						&& managerSet_ && managerSet_->pt_) {
					managerSet_->pt_->setLSN(pId_, defaultLogManager_->getLSN());
				}
			}
			it.fin();
			chunkManager_->reinit();
		}
		if (recoveryMgr && recoveryMgr->isErrorOccured()) {
			GS_TRACE_INFO(RECOVERY_MANAGER, GS_TRACE_RM_RECOVERY_INFO,
					"Recovery process has been interrupted.");
			return;
		}
		chunkManager_->setLogVersion(defaultLogManager_->getLogVersion());
		if (recoveryTargetLsn > 0
				&& recoveryTargetLsn < defaultLogManager_->getLSN()) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_RECOVERY_FAILED,
					"[CLUSTER_SNAPSHOT_RESTORE] Redo failed: targetLSN is too small. " <<
					"Check " << PartitionList::CLUSTER_SNAPSHOT_INFO_FILE_NAME.c_str() <<
					": (pId=" << pId_ << ", targetLSN=" << recoveryTargetLsn <<
					", redoStartLSN=" << defaultLogManager_->getLSN() << ")");
		}
		initializeLogFormatVersion(defaultLogManager_->getLogFormatVersion());
		const uint64_t dsInitTime = util::Stopwatch::currentClock();
		activate();
		if (recoveryMgr && recoveryMgr->isErrorOccured()) {
			GS_TRACE_INFO(RECOVERY_MANAGER, GS_TRACE_RM_RECOVERY_INFO,
					"Recovery process has been interrupted.");
			return;
		}
		const uint64_t ph2Time = util::Stopwatch::currentClock();
		{
			LogIterator<MutexLocker>& it = std::get<1>(its);
			std::unique_ptr<Log> log;
			int64_t currentVersion = -1;
			while (true) {
				if (recoveryTargetLsn > 0
						&& recoveryTargetLsn == defaultLogManager_->getLSN()) {
					GS_TRACE_INFO(RECOVERY_MANAGER, GS_TRACE_RM_RECOVERY_INFO,
							"[CLUSTER_SNAPSHOT_RESTORE] Redo finished. (pId=" << pId_ <<
							",lsn=" << recoveryTargetLsn << ")");
					break;
				}
				util::StackAllocator::Scope scope(defaultLogManager_->getAllocator());
				log = it.next(true);
				if (log == NULL) break;
				if (!log->checkSum()) {
					GS_TRACE_WARNING(
						PARTITION, GS_TRACE_RM_APPLY_LOG_FAILED,
						"XLog checksum is not match. (pId=" << pId_ << ",lsn=" << log->getLsn() << ")");
					break;
				}
				redo(REDO_MODE_RECOVERY, log.get());
				if (log->getLsn() != UNDEF_LSN && managerSet_ && managerSet_->pt_) {
					managerSet_->pt_->setLSN(pId_, defaultLogManager_->getLSN());
				}
				const uint64_t currentTime = util::Stopwatch::currentClock();
				if (ism && currentTime - adjustTime > ADJUST_MEMORY_TIME_INTERVAL_) {
					if (recoveryMgr && recoveryMgr->isErrorOccured()) {
						GS_TRACE_INFO(RECOVERY_MANAGER, GS_TRACE_RM_RECOVERY_INFO,
									  "Recovery process has been interrupted.");
						return;
					}
					adjustTime = currentTime;
					if (recoveryMgr) {
						recoveryMgr->adjustMemory(pId_, workerId);
					}
					else {
						ism->calculate();
						ism->resize();
					}
				}
			}
			{
				std::vector<int32_t>& versionList = it.getLogVersionList();
				for (int32_t logver : versionList) {
					LogIterator<MutexLocker> logIt = defaultLogManager_->createXLogVersionIterator(logver);
					util::StackAllocator::Scope scope(defaultLogManager_->getAllocator());
					while (true) {
						log = logIt.next(true);
						if (log == NULL) break;
						defaultLogManager_->setExistLsnMap(logver, log->getLsn() + 1);
						break;
					}
					logIt.fin();
				}
			}
			it.fin();
		}
		if (recoveryTargetLsn > 0
				&& recoveryTargetLsn != defaultLogManager_->getLSN()) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_RECOVERY_FAILED,
					"[CLUSTER_SNAPSHOT_RESTORE] Redo failed: targetLSN is too large. " <<
					"Check " << PartitionList::CLUSTER_SNAPSHOT_INFO_FILE_NAME.c_str() <<
					": (pId=" << pId_ << ", targetLSN=" << recoveryTargetLsn <<
					", redoFinishedLSN=" << defaultLogManager_->getLSN() << ")");
		}
		if (managerSet_ && managerSet_->pt_) {
			const uint64_t endTime = util::Stopwatch::currentClock();
			GS_TRACE_INFO(RECOVERY_MANAGER, GS_TRACE_RM_RECOVERY_INFO,
					"Partition recovery finished (pId=" << pId_ <<
					", lsn=" << managerSet_->pt_->getLSN(pId_) <<
					", total(ms)=" << util::Stopwatch::clockToMillis(endTime - startTime) <<
					", ph0=" << util::Stopwatch::clockToMillis(ph1Time - startTime) <<
					", ph1=" << util::Stopwatch::clockToMillis(dsInitTime - ph1Time) <<
					", dsInit=" << util::Stopwatch::clockToMillis(ph2Time - dsInitTime) <<
					", ph2=" << util::Stopwatch::clockToMillis(endTime - ph2Time) << ")");
		}
		setStatus(Status::RestoredPartition);
	} catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e, "Recovery failed. partitionId=" << pId_));
	}
}

/*!
	@brief チャンクテーブルを復元する。チェックポイントログを引数
*/
void Partition::restore(const uint8_t* logListBinary, uint32_t size) {
	chunkManager_->reinit();
	util::DateTime redoStartTime;
	Timestamp redoStartEmTime = 0;
	size_t pos = 0;
	redoLogList(
			NULL, REDO_MODE_LONG_TERM_SYNC, redoStartTime, redoStartEmTime,
			logListBinary, size, pos, NULL, 0);
}

/*!
	@brief DataStoreを復元して、アクティベート
*/
void Partition::activate() {
	if (keyStore_) {
		assert(dataStore_);
		return;
	}
	try {
		assert(managerSet_);
		util::StackAllocator::Scope scope(storeStackAlloc_);

		util::TimeZone timeZone;
		const Timestamp stmtStartTimeOnRecovery = 0;
		const EventMonotonicTime stmtStartEmTimeOnRecovery = 0;
		const bool REDO = true;
		const TransactionManager::ContextSource src(UNDEF_EVENT_TYPE,
				UNDEF_STATEMENTID, UNDEF_CONTAINERID,
				TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL,
				TransactionManager::AUTO, TransactionManager::AUTO_COMMIT,
				false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE, timeZone);

		TransactionContext &txn =
				managerSet_->txnMgr_->put(
					storeStackAlloc_, pId_, TXN_EMPTY_CLIENTID, src,
					stmtStartTimeOnRecovery, stmtStartEmTimeOnRecovery, REDO);

		KeyDataStore* keyStore = UTIL_NEW KeyDataStore(
				&storeStackAlloc_, &resultSetPool_, &configTable_,
				managerSet_->txnMgr_,
				chunkManager_, defaultLogManager_, NULL,
				statsSet_.dsStats_);
		keyStore_ = keyStore;
		keyStore_->initialize(*managerSet_);
		keyStore_->activate(txn, managerSet_->clsSvc_);

		dataStore_ = UTIL_NEW DataStoreV4(
				&storeStackAlloc_, &resultSetPool_, &configTable_,
				managerSet_->txnMgr_,
				chunkManager_, defaultLogManager_, keyStore, *rsManager_,
				statsSet_.dsStats_);
		dataStore_->initialize(*managerSet_);
		dataStore_->activate(txn, managerSet_->clsSvc_);
		GS_TRACE_DEBUG(
				PARTITION, GS_TRACE_RM_RECOVERY_INFO,
				"Partition::activate(): pId=" << pId_ << ",lsn=" << defaultLogManager_->getLSN());
	} catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e, ""));
	}
}

/*!
	@brief 長期同期におけるチャンク適用。基本1チャンクずつ
*/
void Partition::catchup(uint8_t *chunk, uint32_t size, int64_t count) {
 	chunkManager_->restoreChunk(chunk, size, count);
}

/*!
	@brief undo処理
*/
uint32_t Partition::undo(util::StackAllocator& alloc) {
	try {
		util::StackAllocator::Scope scope(alloc);

		util::XArray<ClientId> undoTxnContextIds(alloc);

		TransactionManager* transactionManager = managerSet_->txnMgr_;
		transactionManager->getTransactionContextId(pId_, undoTxnContextIds);

		if (!isActive()) {
			assert(undoTxnContextIds.size() == 0);
			return static_cast<uint32_t>(undoTxnContextIds.size());
		}
		for (size_t i = 0; i < undoTxnContextIds.size(); i++) {
			TransactionContext &txn =
					transactionManager->get(alloc, pId_, undoTxnContextIds[i]);
			txn.setAuditInfo(NULL, NULL, NULL);

			if (txn.isActive()) {
				util::StackAllocator::Scope innerScope(alloc);

				KeyDataStoreValue keyStoreValue = KeyDataStoreValue();
				try {
					KeyDataStore* keyDataStore = static_cast<KeyDataStore*>(keyStore_);

					keyStoreValue = keyDataStore->get(alloc, txn.getContainerId());
				}
				catch (UserException& e) {
					UTIL_TRACE_EXCEPTION_WARNING(PARTITION, e,
						"Container not found. (pId="
						<< txn.getPartitionId()
						<< ", clientId=" << txn.getClientId()
						<< ", containerId=" << txn.getContainerId()
						<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
				}

				if (keyStoreValue.containerId_ != UNDEF_CONTAINERID) {
					const DataStoreBase::Scope dsScope(&txn, dataStore_, managerSet_->clsSvc_);
					DSInputMes input(alloc, DS_ABORT);
					StackAllocAutoPtr<DSOutputMes> ret(alloc, NULL);
					{
						ret.set(static_cast<DSOutputMes*>(dataStore_->exec(&txn, &keyStoreValue, &input)));
					}
					TransactionManager::ContextSource dummySource; 
					appendDataStoreLog(
							txn, dummySource, txn.getLastStatementId(),
							keyStoreValue.storeType_, ret.get()->dsLog_);
				}
				else {
					transactionManager->remove(txn);
				}
			}
			transactionManager->remove(pId_, undoTxnContextIds[i]);
		}
		if (undoTxnContextIds.size() > 0) {
			GS_TRACE_INFO(PARTITION, GS_TRACE_RM_COMPLETE_UNDO,
				"Complete undo (pId=" << pId_ << ", lsn=" << defaultLogManager_->getLSN()
									  << ", txnCount="
									  << undoTxnContextIds.size() << ")");
		}
		else {
			GS_TRACE_WARNING(PARTITION_DETAIL, GS_TRACE_RM_COMPLETE_UNDO,
				"Complete undo (pId=" << pId_ << ", lsn=" << defaultLogManager_->getLSN()
									  << ", txnCount="
									  << undoTxnContextIds.size() << ")");
		}
		return static_cast<uint32_t>(undoTxnContextIds.size());
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, "Undo failed. (pId=" << pId_ << ", reason="
													  << GS_EXCEPTION_MESSAGE(e)
													  << ")");
	}
}

/*!
	@brief LogFormatVersion初期設定
	@note 基本はログファイルから得たLogFormatVersion。
		V5.6以降: ZSTD圧縮を使用していてそのLogFormatVersion以下だった場合上書き
*/
void Partition::initializeLogFormatVersion(uint16_t xlogFormatVersion) {
	uint16_t logFormatVersion = xlogFormatVersion;
	const int32_t compressionMode = configTable_.get<int32_t>(
			CONFIG_TABLE_DS_STORE_COMPRESSION_MODE);
	if (compressionMode == ChunkCompressionTypes::BLOCK_COMPRESSION_ZSTD
			&& logFormatVersion < LogManagerConst::getV56zstdFormatVersion()) {
		logFormatVersion = LogManagerConst::getV56zstdFormatVersion();
	}
	defaultLogManager_->setLogFormatVersion(logFormatVersion);
	if (pId_ == 0) {
		GS_TRACE_INFO(
				PARTITION, GS_TRACE_RM_RECOVERY_INFO,
				"initializeLogFormatVersion: version=" << logFormatVersion);
	}
}

/*!
	@brief undo処理のログ出力
*/
util::XArray<uint8_t>* Partition::appendDataStoreLog(
	TransactionContext& txn,
	const TransactionManager::ContextSource& cxtSrc, StatementId stmtId,
	StoreType storeType, DataStoreLogV4* dsMes) {

	return appendDataStoreLog(txn, txn.getClientId(), cxtSrc, stmtId, storeType, dsMes);
}

/*!
	@brief undo処理のログ出力
*/
util::XArray<uint8_t>* Partition::appendDataStoreLog(TransactionContext& txn,
	const ClientId& clientId,
	const TransactionManager::ContextSource& cxtSrc, StatementId stmtId,
	StoreType storeType, DataStoreLogV4* dsMes) {
	if (dsMes == NULL) {
		return NULL;
	}
	util::StackAllocator& alloc = txn.getDefaultAllocator();

	util::XArray<uint8_t>* binary = ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
	TxnLog dsLog(txn, clientId, cxtSrc, stmtId, storeType, dsMes);

	typedef util::ByteStream< util::XArrayOutStream<> > OutStream;
	util::XArrayOutStream<> arrayOut(*binary);
	OutStream out(arrayOut);
	const LogType logType = LogType::OBJLog;

	dsLog.encode<OutStream>(out);

	util::XArray<uint8_t>* logBinary = ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
	const LogSequentialNumber lsn = defaultLogManager_->appendXLog(
			logType, binary->data(), binary->size(), logBinary);

	if (managerSet_ && managerSet_->pt_) {
		managerSet_->pt_->setLSN(txn.getPartitionId(), lsn);
	}
	if (dsLog.isTransactionEnd()) {
		const bool byCP = false;
		defaultLogManager_->flushXLogByCommit(byCP);
	}
	return logBinary;
}

/*!
	@brief アクティブかどうかをチェック
    @note アクティブとは、DataStoreへの要求を受け付け可能な状態
*/
bool Partition::isActive() {
	return (dataStore_ != NULL);
}

void Partition::finalizeRecovery(bool commit) {
	UNUSED_VARIABLE(commit);

	setStatus(Status::ActivePartition);
}

/*!
	@brief チェックポイント実行判定
	@param [in] updateLastLsn 最終CPLSNを更新する場合true
*/
bool Partition::needCheckpoint(bool updateLastLsn) {
	uint64_t currentLsn = logManager().getLSN();
	if (currentLsn != lastCpLsn_) {
		if (updateLastLsn) {
			lastCpLsn_ = currentLsn;
		}
		return true;
	}
	else {
		return false;
	}
}

/*!
	@brief フルチェックポイント処理
	@note リカバリ後のチェックポイント
*/
void Partition::fullCheckpoint(CheckpointPhase phase) {
	if (phase == CP_PHASE_PRE_WRITE) {
		startCheckpoint0(false);
		executeCheckpoint0();
	}
	if (phase == CP_PHASE_PRE_SYNC) {
		closePrevLogFiles();
	}
	endCheckpoint0(phase);
}

/*!
	@brief リカバリ後のチェックポイント
*/
void Partition::startCheckpoint0(bool withFlush) {
	try {
		const bool byCP = true;
		defaultLogManager_->startCheckpoint0(withFlush, byCP);
		util::StackAllocator::Scope scope(storeStackAlloc_);

		writeTxnLog(storeStackAlloc_);
		chunkManager_->startCheckpoint(defaultLogManager_->getLogVersion());
	} catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e, ""));
	}
}

/*!
	@brief 通常のチェックポイント
*/
void Partition::startCheckpoint(const DuplicateLogMode *duplicateMode) {
	try {
		if (duplicateMode) {
			defaultLogManager_->setDuplicateLogMode(*duplicateMode);
		}
		const bool byCP = true;
		defaultLogManager_->startCheckpoint(false, byCP);
		util::StackAllocator::Scope scope(storeStackAlloc_);

		writeTxnLog(storeStackAlloc_);
		chunkManager_->startCheckpoint(defaultLogManager_->getLogVersion());
	} catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e, ""));
	}
}

void Partition::closePrevLogFiles() {
	try {
		const bool byCP = true;
		defaultLogManager_->closePendingFiles(false, byCP);
	} catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e, ""));
	}
}

/*!
	@brief フルチェックポイント処理
*/
void Partition::executeCheckpoint0() {
	MeshedChunkTable::CsectionChunkCursor cursor = chunkManager_->createCsectionChunkCursor();
	RawChunkInfo chunkInfo;
	while (cursor.next(chunkInfo)) {
		if (chunkInfo.getOffset() >= 0) {
			chunkManager_->flushBuffer(chunkInfo.getOffset());
		}
		Log log(LogType::CPLog, defaultLogManager_->getLogFormatVersion());
		log.setChunkId(chunkInfo.getChunkId())
			.setGroupId(chunkInfo.getGroupId())
			.setOffset(chunkInfo.getOffset())
			.setVacancy(chunkInfo.getVacancy())
			.setLsn(getLSN());

		defaultLogManager_->appendCpLog(log);
	}
}

/*!
	@brief N世代パーシャルチェックポイント処理

	@note 以下の3世代に分けて扱う
		N-2世代更新チャンク...スナップショットは本来必要無いが、…
		N-1世代更新チャンク...チャンクテーブル通りにスナップショット。ここがN世代チェックポイントのターゲット
		N世代更新チャンク...チャンクテーブルは古いのでCPログからスナップショット
*/
int32_t Partition::executePartialCheckpoint(
		MeshedChunkTable::CsectionChunkCursor& cursor, int32_t maxcount,
		int32_t checkpointRange) {
	int64_t logVersion = defaultLogManager_->getLogVersion();
	int32_t count = 0;
	RawChunkInfo chunkInfo;
	while (cursor.next(chunkInfo)) {
		bool test1 = (logVersion == (chunkInfo.getLogVersion() + 1));
		bool test2 = (logVersion >= (chunkInfo.getLogVersion() + 2));
		bool test3 = ((chunkInfo.getChunkId() % checkpointRange) ==
					  (logVersion % checkpointRange));
		if (test1 || (test2 && test3)) {
			if (chunkInfo.getOffset() >= 0) {
				chunkManager_->flushBuffer(chunkInfo.getOffset());
			}
			Log log(LogType::CPLog, defaultLogManager_->getLogFormatVersion());
			log.setChunkId(chunkInfo.getChunkId())
				.setGroupId(chunkInfo.getGroupId())
				.setOffset(chunkInfo.getOffset())
				.setVacancy(chunkInfo.getVacancy())
				.setLsn(getLSN());
			defaultLogManager_->appendCpLog(log);
			++count;

		} else {
		}
		if (count >= maxcount) {
			return count;
		}
	}
	return count;
}

int64_t Partition::collectOffset(int32_t mode, UtilBitmap& bitmap) {
	MeshedChunkTable::CsectionChunkCursor cursor = 
			chunkManager().createCsectionChunkCursor();
	RawChunkInfo chunkInfo;
	int64_t counter = 0;
	while (cursor.next(chunkInfo)) {
		if (chunkInfo.getVacancy() < 0) {
			continue;
		}
		int64_t offset = chunkInfo.getOffset();
		if (offset >= 0) {
			switch(mode) {
			case CP_INCREMENTAL_BACKUP_LEVEL_1_CUMULATIVE:
				if (chunkInfo.isCumulativeDirty()) {
					bitmap.set(offset, true);
					++counter;
				}
				break;
			case CP_INCREMENTAL_BACKUP_LEVEL_1_DIFFERENTIAL:
				if (chunkInfo.isDifferentialDirty()) {
					bitmap.set(offset, true);
					++counter;
				}
				break;
			default:
				bitmap.set(offset, true);
				++counter;
				break;
			}
		}		
	}
	if (CP_INCREMENTAL_BACKUP_LEVEL_0 == mode) {
		chunkManager().resetIncrementalDirtyFlags(true); 
	} else if (CP_INCREMENTAL_BACKUP_LEVEL_1_CUMULATIVE == mode
			   || CP_INCREMENTAL_BACKUP_LEVEL_1_DIFFERENTIAL == mode) {
		chunkManager().resetIncrementalDirtyFlags(false); 
	}
	return counter;
}



/*!
	@brief チェックポイント終了処理
		過去世代のCP、Xログファイルを削除
	@note リカバリ後のチェックポイント
*/
void Partition::endCheckpoint0(CheckpointPhase phase) {
	try {
		if (phase == CP_PHASE_PRE_SYNC) {
			flushData();

			Log log(LogType::CheckpointEndLog, defaultLogManager_->getLogFormatVersion());
			log.setLsn(getLSN());
			defaultLogManager_->appendCpLog(log);
		}
		if (phase == CP_PHASE_FINISH) {
			const bool byCP = true;
			const int64_t logVersion = defaultLogManager_->getLogVersion();
			defaultLogManager_->flushCpLog(LOG_WRITE_WAL_BUFFER, byCP);
			defaultLogManager_->flushCpLog(LOG_FLUSH_FILE, byCP);
			defaultLogManager_->removeLogFiles(logVersion, checkpointRange_, 0);
		}
	} catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e, ""));
	}
}

/*!
	@brief チェックポイント終了。データファイルフラッシュ、CP完了ログ出力、CPログフラッシュ
	@note 通常のチェックポイント
*/
void Partition::endCheckpoint(EndCheckpointMode x) {
	try {
		const bool byCP = true;
		switch (x) {
		case CP_END_FLUSH_DATA:
			flushData();
			break;
		case CP_END_ADD_CPLOG:
			{
				Log log(LogType::CheckpointEndLog, defaultLogManager_->getLogFormatVersion());
				log.setLsn(getLSN());
				defaultLogManager_->appendCpLog(log);
			}
			break;
		case CP_END_FLUSH_CPLOG:
			defaultLogManager_->flushCpLog(LOG_WRITE_WAL_BUFFER, byCP);
			defaultLogManager_->flushCpLog(LOG_FLUSH_FILE, byCP);
			break;
		}
	} catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e, ""));
	}
}

/*!
	@brief チェックポイント終了: 不要になった前世代のブロックイメージ解放準備
	@note 通常のチェックポイント
*/
LogIterator<MutexLocker> Partition::endCheckpointPrepareRelaseBlock() {
	try {
		int64_t logVersion = defaultLogManager_->getLogVersion();
		return defaultLogManager_->createLogIterator(logVersion - 1);
	} catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e, ""));
	}
}		
		
/*!
	@brief チェックポイント終了: 不要になった前世代のブロックイメージ解放実行
	@note 通常のチェックポイント
*/
void Partition::endCheckpointReleaseBlock(const Log* log) {
	try {
		chunkManager_->freeSpace(log);
	} catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e, ""));
	}
}

/*!
	@brief チェックポイント終了: 不要になったトランザクションログファイル削除
	@note 通常のチェックポイント
*/
void Partition::removeLogFiles(int32_t range) {
	try {
		int64_t logVersion = defaultLogManager_->getLogVersion();
		defaultLogManager_->removeLogFiles(logVersion, range, 0);
	} catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e, ""));
	}
}

int32_t Partition::calcCheckpointRange(int32_t mode) {
	if (mode == CP_NORMAL || mode == CP_REQUESTED) {
		return checkpointRange_;
	}
	else {
		return 1;
	}
}


/*!
	@brief モードに依存するチェックポイント後処理
*/
int64_t Partition::postCheckpoint(
		int32_t mode, UtilBitmap& bitmap, const std::string& path,
		bool enableDuplicateLog, bool skipBaseLine) {
	int64_t blockCount = 0;
	if (mode == CP_BACKUP || mode == CP_BACKUP_START
			|| mode == CP_INCREMENTAL_BACKUP_LEVEL_0
			|| mode == CP_PREPARE_LONGTERM_SYNC) {
		try {
			util::NormalOStringStream oss;
			oss	<< path.c_str() << "/data/" << pId_;

			util::NormalOStringStream oss2;
			oss2 << path.c_str() << "/txnlog/" << pId_;
			util::FileSystem::createDirectoryTree(oss2.str().c_str());
			const size_t bufferSize = 256 * 1024;
			uint8_t buf[bufferSize];
			if (!skipBaseLine) {
				const bool keepSrcOffset = (mode != CP_PREPARE_LONGTERM_SYNC); 
				blockCount = copyChunks(bitmap, oss.str().c_str(), NULL, keepSrcOffset); 
			}
			util::LockGuard<util::Mutex> guard(mutex());
			if (!skipBaseLine) {
				copyCpLog(oss.str().c_str(), buf, bufferSize);
			}
			if (!enableDuplicateLog) {
				assert(util::FileSystem::isDirectory(oss2.str().c_str()));
				copyXLog(oss2.str().c_str(), buf, bufferSize);
			}
		} catch (std::exception &e) {
			GS_RETHROW_USER_ERROR(e, "postCheckpoint failed." << 
				"(pId=" << pId_ << ", reason=" << 
				GS_EXCEPTION_MESSAGE(e) << ")");
		}
	}
	else if (CP_INCREMENTAL_BACKUP_LEVEL_1_CUMULATIVE == mode ||
			 CP_INCREMENTAL_BACKUP_LEVEL_1_DIFFERENTIAL == mode) {
		try {
			util::NormalOStringStream oss;
			oss	<< path.c_str() << "/data/" << pId_;
			util::FileSystem::createDirectoryTree(oss.str().c_str());
			assert(util::FileSystem::isDirectory(oss.str().c_str()));

			util::NormalOStringStream oss2;
			oss2 << path.c_str() << "/txnlog/" << pId_;
			util::FileSystem::createDirectoryTree(oss2.str().c_str());
			assert(util::FileSystem::isDirectory(oss2.str().c_str()));

			blockCount = copyChunks(bitmap, oss.str().c_str(), oss2.str().c_str(), false); 
			const size_t bufferSize = 256 * 1024;

			uint8_t buf[bufferSize];
			copyCpLog(oss.str().c_str(), buf, bufferSize);

			copyXLog(oss2.str().c_str(), buf, bufferSize);
		} catch (std::exception &e) {
			GS_RETHROW_USER_ERROR(e, "postCheckpoint failed." << 
				"(pId=" << pId_ << ", reason=" << 
				GS_EXCEPTION_MESSAGE(e) << ")");
		}
	}

	return blockCount;
}

/*!
	@brief REDOハンドラ
	@note 以下のようにひっくり返した関係にある
		通常時：データストア,チャンクマネージャ,→→→ログマネージャ
		リカバリ時：ログマネージャ→→→チャンクマネージャ,データストア,
*/
bool Partition::redo(RedoMode mode, Log* log) {
	const Timestamp stmtStartTimeOnRecovery = 0;

	int32_t op = log->getType();
	switch (op) {
	case LogType::CPLog:
	case LogType::CPULog:
		chunkManager_->redo(log);
		return true;
	case LogType::OBJLog:
	{
		const int64_t currentLsn =
				static_cast<int64_t>(defaultLogManager_->getLSN());
		const int64_t diff = static_cast<int64_t>(log->getLsn()) - currentLsn;
		if (diff == 1 || currentLsn == 0) {
			int64_t dataSize = log->getDataSize();
			const uint8_t* data = static_cast<const uint8_t*>(log->getData());
			if (dataSize > 0 && data) {
				util::StackAllocator::Scope scope(storeStackAlloc_);
				util::ArrayByteInStream in = util::ArrayByteInStream(
						util::ArrayInStream(data, dataSize));

				StoreType storeType;
				DataStoreUtil::decodeEnumData<util::ArrayByteInStream, StoreType>(in, storeType);
				assert(storeType == V4_COMPATIBLE);
				SimpleMessage message(&storeStackAlloc_, data, dataSize);
				dataStore_->redo(
						storeStackAlloc_, mode, stmtStartTimeOnRecovery, stmtStartTimeOnRecovery,
						&message);
				defaultLogManager_->setLSN(log->getLsn());
			}
			else {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Empty logData");
			}
		}
		else if (diff <= 0) {
			GS_TRACE_DEBUG(PARTITION,
					GS_TRACE_RM_APPLY_LOG_FAILED,
					"Log already applied (pId=" << pId_ <<
					", lsn=" << currentLsn <<
					", logLsn=" << log->getLsn() << ")");
		}
		else {
			GS_THROW_CUSTOM_ERROR(LogRedoException,
					GS_ERROR_TXN_REPLICATION_LOG_LSN_INVALID,
					"Invalid LSN (pId=" << pId_ <<
					", currentLsn=" << currentLsn <<
					", logLsn=" << log->getLsn() <<
					 ")");
		}
		return true;
	}
	case LogType::TXNLog:
		redoTxnLog(log);
		if (log->getLsn() != UINT64_MAX) {
			defaultLogManager_->setLSN(log->getLsn());
		}
		GS_TRACE_DEBUG(
				PARTITION, GS_TRACE_RM_RECOVERY_INFO,
				"REDO TXNLOG: pId=" << pId_ << ",log=" << log->toString() <<
				",lsn=" << log->getLsn());
		return true;
	case LogType::PutChunkStartLog:
	case LogType::PutChunkDataLog:
	case LogType::PutChunkEndLog:
		chunkManager_->redo(log);
		return true;
	case LogType::NOPLog:
		return true;
	case LogType::CheckpointStartLog:  
	{
		uint16_t logFormatVersion = static_cast<uint16_t>(log->getFormatVersion());
		if (logFormatVersion == 0) {
			logFormatVersion = LogManagerConst::getBaseFormatVersion();
		}
		if (!LogManagerConst::isAcceptableFormatVersion(logFormatVersion)) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_REDO_LOG_VERSION_NOT_ACCEPTABLE,
				"(logFormatVersion=" << static_cast<uint32_t>(logFormatVersion) <<
				", baseFormatVersion=" <<
				static_cast<uint32_t>(LogManagerConst::getBaseFormatVersion()) <<
				")" );
		}
		initializeLogFormatVersion(logFormatVersion);
		return true;
	}
	case LogType::EOFLog:
		return false;
	case LogType::CheckpointEndLog:
		return true; 
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Undefine Operation Id");
	}
}

/*!
	@brief ログリストのredo処理
*/
bool Partition::redoLogList(
		util::StackAllocator* alloc, RedoMode mode,
		const util::DateTime &redoStartTime, Timestamp redoStartEmTime,
		const uint8_t* logListBinary, size_t totalLen, size_t &pos,
		util::Stopwatch* watch, int32_t limitInterval) {
	UNUSED_VARIABLE(alloc);
	bool enaleInterruption = (watch && limitInterval >= 0);

	const uint8_t* logTop = logListBinary;
	while (pos < totalLen) { 
		util::StackAllocator::Scope scope(storeStackAlloc_);

		uint64_t logSize  = *reinterpret_cast<uint64_t*>(const_cast<uint8_t*>(
			logTop + pos));
		pos += sizeof(uint64_t);
		size_t logPos = pos;

		Log log(LogType::NOPLog, defaultLogManager_->getLogFormatVersion());
		util::ArrayByteInStream in = util::ArrayByteInStream(
				util::ArrayInStream(logTop + pos, logSize));
		log.decode<util::ArrayByteInStream>(in, storeStackAlloc_);
		pos += logSize;

		LogType op = log.getType();
		switch (op) {
		case LogType::CPLog:
		case LogType::CPULog:
			chunkManager_->redo(&log);
			break;

		case LogType::OBJLog:
			{
				const int64_t currentLsn =
						static_cast<int64_t>(defaultLogManager_->getLSN());
				const int64_t diff = static_cast<int64_t>(log.getLsn()) - currentLsn;
				if (diff == 1 || currentLsn == 0) {
					int64_t byteListSize = log.getDataSize();
					const uint8_t* byteList = static_cast<const uint8_t*>(log.getData());
					if (byteListSize > 0 && byteList) {
						SimpleMessage message(&storeStackAlloc_, byteList, byteListSize);
						dataStore_->redo(
							storeStackAlloc_, mode, redoStartTime, redoStartEmTime,
							&message);
						defaultLogManager_->copyXLog(
							op, log.getLsn(), logTop + logPos, logSize);
						defaultLogManager_->setLSN(log.getLsn());
						if (managerSet_ && managerSet_->pt_) {
							managerSet_->pt_->setLSN(pId_, log.getLsn());
						}
					}
					else {
						GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Empty logData");
					}
				}
				else if (diff <= 0) {
					if (managerSet_->clsSvc_->getManager()->reportError(
							GS_ERROR_RM_ALREADY_APPLY_LOG)) {
						GS_TRACE_WARNING(PARTITION,
								GS_TRACE_RM_APPLY_LOG_FAILED,
								"Log already applied (pId=" << pId_ <<
								", lsn=" << currentLsn <<
								", logLsn=" << log.getLsn() << ")");
					}
					else {
						GS_TRACE_DEBUG(PARTITION,
								GS_TRACE_RM_APPLY_LOG_FAILED,
								"Log already applied (pId=" << pId_ <<
								", lsn=" << currentLsn <<
								", logLsn=" << log.getLsn() << ")");
					}
					continue;
				}
				else {
					GS_THROW_CUSTOM_ERROR(LogRedoException,
							GS_ERROR_TXN_REPLICATION_LOG_LSN_INVALID,
							"Invalid LSN (pId=" << pId_ <<
							", currentLsn=" << currentLsn <<
							", logLsn=" << log.getLsn() <<
							 ")");
				}
			}
			break;
		case LogType::TXNLog:
			redoTxnLog(&log);
			if (log.getLsn() != UINT64_MAX && managerSet_ && managerSet_->pt_) {
				defaultLogManager_->setLSN(log.getLsn());
				managerSet_->pt_->setLSN(pId_, log.getLsn());
			}
			GS_TRACE_DEBUG(
				PARTITION, GS_TRACE_RM_RECOVERY_INFO,
				"RedoLogList TXNLOG: pId=" << pId_ << ",log=" << log.toString() <<
				",lsn=" << log.getLsn());
			break;
		case LogType::PutChunkStartLog:
		case LogType::PutChunkDataLog:
		case LogType::PutChunkEndLog:
			break;
		case LogType::NOPLog:
			break;
		case LogType::CheckpointStartLog:  
			{
				uint16_t logFormatVersion =
						static_cast<uint16_t>(log.getFormatVersion());
				if (logFormatVersion == 0) {
					logFormatVersion = LogManagerConst::getBaseFormatVersion();
				}
				if (!LogManagerConst::isAcceptableFormatVersion(logFormatVersion)) {
					GS_THROW_USER_ERROR(GS_ERROR_RM_REDO_LOG_VERSION_NOT_ACCEPTABLE,
							"(logFormatVersion=" <<
							static_cast<uint32_t>(logFormatVersion) <<
							", baseFormatVersion=" <<
							static_cast<uint32_t>(LogManagerConst::getBaseFormatVersion()) <<
							")" );
				}
				initializeLogFormatVersion(logFormatVersion);
			}
			break;
		case LogType::CheckpointEndLog:
			break;
		case LogType::EOFLog:
			break;
		default:
			GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Undefine Operation Id");
		}
		if (enaleInterruption && watch->elapsedMillis() >= limitInterval) {
			break;
		}
	} 
	if (enaleInterruption) {
		return (pos >= totalLen);
	}
	else {
		return true;
	}
}

/*!
	@brief データファイルフラッシュ
*/
void Partition::flushData() {
	StatStopwatch &watch = statsSet_.chunkMgrStats_.timeTable_(
			ChunkManagerStats::CHUNK_STAT_CP_FILE_FLUSH_TIME);

	watch.start();
	dataFile_->flush();
	watch.stop();

	statsSet_.chunkMgrStats_.table_(
			ChunkManagerStats::CHUNK_STAT_CP_FILE_FLUSH_COUNT).increment();
}

/*!
	@brief ログファイル全削除
	@note dropPartition
*/
void Partition::removeAllLogFiles() {
	try {
		const bool byCP = false;
		defaultLogManager_->removeAllLogFiles(byCP);
	} catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e, ""));
	}
}

/*!
	@brief テンポラルなログマネージャ生成
*/
void Partition::genTempLogManager() {
	try {
		std::unique_ptr<WALBuffer> localXWALBuffer(
				UTIL_NEW WALBuffer(pId_, statsSet_.logMgrStats_));
		std::unique_ptr<WALBuffer> localCPWALBuffer(
				UTIL_NEW WALBuffer(pId_, statsSet_.logMgrStats_));

		int64_t logVersion = 0;
		std::unique_ptr<MutexLocker> locker(UTIL_NEW MutexLocker());
		LogManager<MutexLocker> logManager(
				configTable_, std::move(locker), logStackAlloc_,
				*localXWALBuffer, *localCPWALBuffer, 1,
				tmpPath_, tmpPath_, pId_, statsSet_.logMgrStats_, uuid_);
		const bool byCP = false;
		logManager.init(logVersion, true, byCP);

		MeshedChunkTable::StableChunkCursor cursor = chunkManager_->createStableChunkCursor();
		RawChunkInfo chunkInfo;
		while (cursor.next(chunkInfo)) {
			Log log(LogType::CPLog, defaultLogManager_->getLogFormatVersion());
			log.setChunkId(chunkInfo.getChunkId())
				.setGroupId(chunkInfo.getGroupId())
				.setOffset(chunkInfo.getOffset())
				.setVacancy(chunkInfo.getVacancy())
				.setLsn(getLSN());
			logManager.appendCpLog(log);
		}
		Log log(LogType::CheckpointEndLog, defaultLogManager_->getLogFormatVersion());
		log.setLsn(defaultLogManager_->getLSN());
		logManager.appendCpLog(log);
		logManager.flushCpLog(LOG_WRITE_WAL_BUFFER, byCP);
		logManager.flushCpLog(LOG_FLUSH_FILE, byCP);
		logManager.fin(byCP);
	} catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e, ""));
	}
}

uint64_t Partition::copyChunks(
		UtilBitmap& bitmap, const char* dataPath, const char* xLogPath,
		bool keepSrcOffset) {
	uint64_t blockCount = 0;
	ChunkCopyContext cxt;
	try {
		int32_t stripeSize = configTable_.getUInt32(CONFIG_TABLE_DS_DB_FILE_SPLIT_STRIPE_SIZE);
		int32_t numFiles = configTable_.getUInt32(CONFIG_TABLE_DS_DB_FILE_SPLIT_COUNT);
		const int32_t chunkSize = chunkManager_->getChunkSize();

		std::unique_ptr<NoLocker> noLocker(UTIL_NEW NoLocker());
		std::unique_ptr<VirtualFileNIO> destFile;
		std::unique_ptr<WALBuffer> localXWALBuffer;
		std::unique_ptr<WALBuffer> localCPWALBuffer;

		cxt.tmpBuffer_ = UTIL_NEW uint8_t[chunkSize];
		Chunk chunk;
		chunk.setData(cxt.tmpBuffer_, chunkSize);
		cxt.chunk_ = &chunk;
		if (!xLogPath) {
			destFile.reset(UTIL_NEW VirtualFileNIO(
					std::move(noLocker), dataPath, pId_, stripeSize, numFiles,
					chunkSize, NULL));
			destFile->open();
			cxt.datafile_ = destFile.get();
			cxt.logManager_ = NULL;
		}
		else {
			cxt.datafile_ = NULL;
			localXWALBuffer.reset(UTIL_NEW WALBuffer(pId_, statsSet_.logMgrStats_));
			localCPWALBuffer.reset(UTIL_NEW WALBuffer(pId_, statsSet_.logMgrStats_));
			int64_t logVersion = defaultLogManager_->getLogVersion();
			std::unique_ptr<MutexLocker> mutexLocker(UTIL_NEW MutexLocker());
			cxt.logManager_ = UTIL_NEW LogManager<MutexLocker>(
					configTable_, std::move(mutexLocker), logStackAlloc_,
					*localXWALBuffer, *localCPWALBuffer, 1,
					dataPath, xLogPath, pId_, statsSet_.logMgrStats_, uuid_);
			cxt.logManager_->setSuffix(LogManagerConst::INCREMENTAL_FILE_SUFFIX);
			const bool byCP = false;
			cxt.logManager_->init(logVersion, true, byCP);

			Log log(LogType::PutChunkStartLog, defaultLogManager_->getLogFormatVersion());
			log.setLsn(defaultLogManager_->getLSN());

			cxt.logManager_->appendCpLog(log);
		}
		for (int64_t i = 0; i < bitmap.length(); i++) {
			if (bitmap.get(i)) {
				chunkManager_->copyChunk(&cxt, i, keepSrcOffset);
				++blockCount;
			}
		}
		if (!xLogPath) {
			destFile->flush();
			destFile->close();
			cxt.datafile_ = NULL;
		}
		else {
			Log log(LogType::PutChunkEndLog, defaultLogManager_->getLogFormatVersion());
			log.setOffset(blockCount)
				.setLsn(defaultLogManager_->getLSN());
			cxt.logManager_->appendCpLog(log);
			const bool byCP = false;
			cxt.logManager_->flushCpLog(LOG_WRITE_WAL_BUFFER, byCP);
			cxt.logManager_->flushCpLog(LOG_FLUSH_FILE, byCP);
			cxt.logManager_->fin(byCP);
		}
		assert(blockCount == cxt.offset_);
		delete [] cxt.tmpBuffer_;
		delete cxt.logManager_;
	} catch (std::exception &e) {
     	delete [] cxt.tmpBuffer_;
		GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e, "backup chunk failed."));
	}
	return blockCount;
}

void Partition::copyCpLog(const char* path, uint8_t* buffer, size_t bufferSize) {
	defaultLogManager_->copyCpLog(path, buffer, bufferSize);
}
void Partition::copyXLog(const char* path, uint8_t* buffer, size_t bufferSize) {
	defaultLogManager_->copyXLog(path, buffer, bufferSize);
}

/*!
	@brief トランザクション情報復元
*/
void Partition::redoTxnLog(Log* log) {
	assert(log);
	int64_t dataSize = log->getDataSize();
	const uint8_t* data = static_cast<const uint8_t*>(log->getData());
	if (dataSize > 0 && data) {
		TransactionId maxTxnId = 0;
		uint32_t numContext;
		const ClientId *clientIds;
		const TransactionId *activeTxnIds;
		const ContainerId *refContainerIds;
		const StatementId *lastStmtIds;
		const int32_t *txnTimeoutIntervalSec;
		EventMonotonicTime emNow = 0;

		const uint8_t* addr = data;
		addr += util::varIntDecode32(addr, numContext);
		uint32_t numContainerRefCount = 0;
		addr += util::varIntDecode32(addr, numContainerRefCount);
		clientIds = reinterpret_cast<const ClientId *>(addr);
		addr += numContext * sizeof(ClientId);
		activeTxnIds = reinterpret_cast<const TransactionId*>(addr);
		addr += numContext * sizeof(TransactionId);
		refContainerIds = reinterpret_cast<const ContainerId*>(addr);
		addr += numContext * sizeof(ContainerId);
		lastStmtIds = reinterpret_cast<const StatementId*>(addr);
		addr += numContext * sizeof(StatementId);
		txnTimeoutIntervalSec = reinterpret_cast<const int32_t*>(addr);
		addr += numContext * sizeof(uint32_t);
		maxTxnId = *reinterpret_cast<const TransactionId*>(addr);
		addr += sizeof(TransactionId);

		if (managerSet_ && managerSet_->txnMgr_) {
			managerSet_->txnMgr_->restoreTransactionActiveContext(
				pId_, maxTxnId, numContext, clientIds, activeTxnIds, refContainerIds,
				lastStmtIds, txnTimeoutIntervalSec, emNow);
		}
	}
	else {
	}
}

void Partition::writeTxnLog(util::StackAllocator &alloc) {
	try {
		util::XArray<uint64_t> dirtyChunkList(alloc);

		util::XArray<ClientId> clientIds(alloc);
		util::XArray<TransactionId> activeTxnIds(alloc);
		util::XArray<ContainerId> refContainerIds(alloc);
		util::XArray<StatementId> lastExecStmtIds(alloc);
		util::XArray<int32_t> txnTimeoutIntervalSec(alloc);
		TransactionId maxAssignedTxnId = 0;

		if (managerSet_ && managerSet_->txnMgr_) {
			managerSet_->txnMgr_->backupTransactionActiveContext(pId_,
					maxAssignedTxnId, clientIds, activeTxnIds,
				refContainerIds, lastExecStmtIds,
				txnTimeoutIntervalSec);
		}
		util::XArray<uint8_t> binaryLogBuf(alloc);
		{
			uint8_t tmp[VARINT_MAX_LEN * 2];
			uint8_t *addr = tmp;
			uint32_t txnNum = static_cast<uint32_t>(activeTxnIds.size());
			addr += util::varIntEncode32(addr, txnNum);
			uint32_t numContainerRefCount = 0;
			addr += util::varIntEncode32(addr, numContainerRefCount);
			binaryLogBuf.push_back(tmp, (addr - tmp));
			binaryLogBuf.push_back(
				reinterpret_cast<const uint8_t*>(clientIds.data()),
				clientIds.size() * sizeof(ClientId));
			binaryLogBuf.push_back(
				reinterpret_cast<const uint8_t*>(activeTxnIds.data()),
				activeTxnIds.size() * sizeof(TransactionId));
			binaryLogBuf.push_back(
				reinterpret_cast<const uint8_t*>(refContainerIds.data()),
				refContainerIds.size() * sizeof(ContainerId));
			binaryLogBuf.push_back(
				reinterpret_cast<const uint8_t*>(lastExecStmtIds.data()),
				lastExecStmtIds.size() * sizeof(StatementId));
			binaryLogBuf.push_back(
				reinterpret_cast<const uint8_t*>(txnTimeoutIntervalSec.data()),
				txnTimeoutIntervalSec.size() * sizeof(uint32_t));
			assert(clientIds.size() == activeTxnIds.size()
				   && activeTxnIds.size() == refContainerIds.size()
				   && refContainerIds.size() == lastExecStmtIds.size()
				   && lastExecStmtIds.size() == txnTimeoutIntervalSec.size()
				   && txnTimeoutIntervalSec.size() == static_cast<size_t>(txnNum));
			binaryLogBuf.push_back(
					reinterpret_cast<const uint8_t*>(&maxAssignedTxnId),
					sizeof(TransactionId));
		}
		const LogType logType = LogType::TXNLog;
		Log log(logType, defaultLogManager_->getLogFormatVersion());
		log.setLsn(defaultLogManager_->getLSN());
		log.setDataSize(binaryLogBuf.size());
		if (binaryLogBuf.size() > 0) {
			log.setData(binaryLogBuf.data());
		}
		defaultLogManager_->appendCpLog(log);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(
			e, "Write checkpoint start log failed. (" <<
			"pId=" << pId_ <<
			", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}


Partition::StatsSet::StatsSet(
		FileStatTable &dataFileStats,
		LogManagerStats &logMgrStats,
		ChunkManagerStats &chunkMgrStats,
		const DataStoreBase::StatsSet &dsStats) :
		dataFileStats_(dataFileStats),
		logMgrStats_(logMgrStats),
		chunkMgrStats_(chunkMgrStats),
		dsStats_(dsStats) {
}

/*!
	@brief コンストラクタ
*/
PartitionList::PartitionList(
		::ConfigTable& configTable, TransactionManager* txnMgr,
		util::FixedSizeAllocator<util::Mutex>& resultSetPool) :
		varAlloc_(util::AllocatorInfo(
				ALLOCATOR_GROUP_STORE, "partitionListVar")),
		config_(configTable, this), pgConfig_(configTable),
		configTable_(configTable), resultSetPool_(resultSetPool),
		chunkMemoryPool_(nullptr), transactionManager_(txnMgr),
		partitionCount_(configTable.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM)),
		stats_(ALLOC_UNIQUE(
				varAlloc_, PartitionListStats,
				varAlloc_, configTable_, config_)), managerSet_(NULL)
{
	try {
		const char* const DATA_FOLDER = configTable.get<const char8_t*>(CONFIG_TABLE_DS_DB_PATH);
		const char* const LOG_FOLDER = configTable.get<const char8_t*>(CONFIG_TABLE_DS_TRANSACTION_LOG_PATH);


		const uint32_t partitionGroupCount = pgConfig_.getPartitionGroupCount();
		const uint32_t chunkExpSize = Config::getChunkExpSize(configTable_);
		const uint32_t dataStoreBlockSize = Config::getChunkSize(chunkExpSize);

		uint64_t storeMemoryLimitMB =
				configTable.getUInt32(CONFIG_TABLE_DS_STORE_MEMORY_LIMIT);
		uint64_t chunkBufferLimit =
				storeMemoryLimitMB * 1024 * 1024 / dataStoreBlockSize;
		uint64_t initialBufferSize = chunkBufferLimit;
		if (initialBufferSize < partitionGroupCount) {
			initialBufferSize = partitionGroupCount;
			chunkBufferLimit = partitionGroupCount;
		}
		uint64_t walBufferLimit = INITIAL_X_WAL_BUFFER_SIZE;
		uint64_t affinityManagerLimit = INITIAL_AFFINITY_MANAGER_SIZE;

		chunkMemoryPool_ = UTIL_NEW Chunk::ChunkMemoryPool(
				util::AllocatorInfo(ALLOCATOR_GROUP_STORE, "txnBlockPool"),
				dataStoreBlockSize);

		ism_ = UTIL_NEW InterchangeableStoreMemory(
				configTable, chunkExpSize, chunkBufferLimit,
				walBufferLimit, affinityManagerLimit);
		ptList_.clear();
		ptList_.reserve(partitionCount_);

		const double hashTableSizeRate = configTable.get<double>(
				CONFIG_TABLE_DS_STORE_BUFFER_TABLE_SIZE_RATE);
		size_t hashTableSize = ChunkBuffer::calcHashTableSize(
				partitionGroupCount, chunkBufferLimit, chunkExpSize,
				hashTableSizeRate);
				
		size_t fileSystemBlockSize;
		util::FileSystemStatus status;
		util::FileSystem::getStatus(DATA_FOLDER, &status);
		fileSystemBlockSize = status.getBlockSize();

		CompressorParam compressorParam;
		compressorParam.mode_ =
				ChunkManager::Config::getCompressionMode(configTable);
		compressorParam.fileSystemBlockSize_ = static_cast<int32_t>(fileSystemBlockSize);
		compressorParam.minSpaceSavings_ = CompressorParam::DEFAULT_MIN_SPACE_SAVINGS;

		for (int32_t pgId = 0;
				static_cast<uint32_t>(pgId) < partitionGroupCount; ++pgId) {
			ChunkBuffer* chunkBuffer = UTIL_NEW ChunkBuffer(
					pgId,
					static_cast<int32_t>(dataStoreBlockSize),
					static_cast<int32_t>(initialBufferSize),
					compressorParam,
					pgConfig_.getGroupBeginPartitionId(pgId),
					pgConfig_.getGroupEndPartitionId(pgId),
					hashTableSize, ChunkManager::CHUNK_CATEGORY_NUM,
					stats_->getPartitionGroupEntry(pgId).chunkBufStats_);
			chunkBuffer->setMemoryPool(chunkMemoryPool_);
			chunkBuffer->setPartitionList(this);
			chunkBuffer->setStoreMemoryLimitBlockCount(chunkBufferLimit);
			chunkBufferList_.push_back(chunkBuffer);
			ism_->regist(pgId, chunkBuffer, initialBufferSize);
		}
		for (size_t pId = 0; pId < partitionCount_; ++pId) {
			const PartitionGroupId pgId = pgConfig_.getPartitionGroupId(
					static_cast<PartitionId>(pId));
			Partition* pt = UTIL_NEW Partition(
					configTable, static_cast<int32_t>(pId), *ism_,
					resultSetPool, *chunkMemoryPool_, chunkBufferList_[pgId],
					DATA_FOLDER, LOG_FOLDER,
					static_cast<int32_t>(initialBufferSize),
					static_cast<int32_t>(dataStoreBlockSize),
					stats_->toPartitionStatsSet(static_cast<PartitionId>(pId)));
			ptList_.push_back(pt);
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e, ""));
	}
}

PartitionList::~PartitionList() {
	for (auto itr : ptList_) {
		delete itr;
	}
	for (size_t pgId = 0; pgId < pgConfig_.getPartitionGroupCount(); ++pgId) {
		ism_->unregistChunkBuffer(static_cast<int32_t>(pgId));
	}
	delete chunkMemoryPool_;
	delete ism_;
}


void PartitionList::initialize(ManagerSet &resourceSet) {
	managerSet_ = &resourceSet;
	for (auto& itr : ptList_) {
		itr->initialize(resourceSet);
	}
	assert(resourceSet.stats_ != NULL);
	assert(resourceSet.dsConfig_ != NULL);
	assert(chunkMemoryPool_ != NULL);
	assert(ism_ != NULL);

	stats_->initialize(
			*resourceSet.stats_, *resourceSet.dsConfig_, *chunkMemoryPool_,
			*ism_);
}

PartitionList::ConfigSetUpHandler PartitionList::configSetUpHandler_;

void PartitionList::ConfigSetUpHandler::operator()(ConfigTable& config) {
	CONFIG_TABLE_RESOLVE_GROUP(config, CONFIG_TABLE_DS, "dataStore");

	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_DS_PERSISTENCY_MODE, INT32).
			setExtendedType(ConfigTable::EXTENDED_TYPE_ENUM).
			addEnum(LogManager<MutexLocker>::PERSISTENCY_NORMAL, "NORMAL").
			addEnum(LogManager<MutexLocker>::PERSISTENCY_KEEP_ALL_LOG, "KEEP_ALL_LOG").
			setDefault(LogManager<MutexLocker>::PERSISTENCY_NORMAL);
	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_DS_RETAINED_FILE_COUNT, INT32).
			setMin(2).
			setDefault(2);
	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_DS_LOG_FILE_CLEAR_CACHE_INTERVAL, INT32).
			setMin(0).
			setDefault(100);
}

PartitionList::Config::Config(ConfigTable& configTable, PartitionList *partitionList) :
		partitionList_(partitionList),
		partitionCount_(configTable.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM)),
		chunkExpSize_(0),
		atomicStoreMemoryLimit_(
				ConfigTable::megaBytesToBytes(
					configTable.getUInt32(CONFIG_TABLE_DS_STORE_MEMORY_LIMIT)) ),
		atomicShiftableMemRate_(20), atomicEMAHalfLifePeriod_(0),
		atomicStoreMemoryColdRate_(0), atomicCpFileFlushSize_(0),
		atomicCpFileFlushInterval_(0), atomicCpFileAutoClearCache_(false)
{
	chunkExpSize_ = util::nextPowerBitsOf2(static_cast<uint32_t>(
			configTable.getUInt32(CONFIG_TABLE_DS_STORE_BLOCK_SIZE)));
	setUpConfigHandler(configTable);
}

void PartitionList::Config::setUpConfigHandler(ConfigTable& configTable) {
	configTable.setParamHandler(CONFIG_TABLE_DS_STORE_MEMORY_LIMIT, *this);
	configTable.setParamHandler(CONFIG_TABLE_DS_STORE_MEMORY_REDISTRIBUTE_SHIFTABLE_MEMORY_RATE, *this);
	configTable.setParamHandler(CONFIG_TABLE_DS_STORE_MEMORY_REDISTRIBUTE_EMA_HALF_LIFE_PERIOD, *this);
	configTable.setParamHandler(CONFIG_TABLE_DS_STORE_MEMORY_COLD_RATE, *this);
	configTable.setParamHandler(CONFIG_TABLE_DS_CHECKPOINT_FILE_FLUSH_SIZE, *this); 
	configTable.setParamHandler(CONFIG_TABLE_DS_CHECKPOINT_FILE_AUTO_CLEAR_CACHE, *this); 
}

void PartitionList::Config::operator()(
		ConfigTable::ParamId id, const ParamValue& value) {
	switch (id) {
	case CONFIG_TABLE_DS_STORE_MEMORY_LIMIT:
		setAtomicStoreMemoryLimit(ConfigTable::megaBytesToBytes(
			static_cast<uint32_t>(value.get<int32_t>())));
		break;
	case CONFIG_TABLE_DS_STORE_MEMORY_REDISTRIBUTE_SHIFTABLE_MEMORY_RATE:
		setAtomicShiftableMemRate(value.get<int32_t>());
		break;
	case CONFIG_TABLE_DS_STORE_MEMORY_REDISTRIBUTE_EMA_HALF_LIFE_PERIOD:
		setAtomicEMAHalfLifePeriod(value.get<int32_t>());
		break;
	case CONFIG_TABLE_DS_STORE_MEMORY_COLD_RATE:
		setAtomicStoreMemoryColdRate(value.get<double>());
		break;
	case CONFIG_TABLE_DS_CHECKPOINT_FILE_FLUSH_SIZE:
		setAtomicCpFileFlushSize(value.get<int64_t>());
		break;
	case CONFIG_TABLE_DS_CHECKPOINT_FILE_AUTO_CLEAR_CACHE:
		setAtomicCpFileAutoClearCache(value.get<bool>());
		break;
	}
}

bool PartitionList::Config::setAtomicStoreMemoryLimit(uint64_t memoryLimitByte) {
	assert((memoryLimitByte >> chunkExpSize_) <= UINT32_MAX);

	int32_t nth = chunkExpSize_ - ChunkManager::MIN_CHUNK_EXP_SIZE_;
	uint64_t limitMegaByte = memoryLimitByte / 1024 / 1024;
	if (limitMegaByte > ChunkManager::LIMIT_CHUNK_NUM_LIST[nth]) {
		GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_GREATER_THAN_UPPER_LIMIT,
				"Too large value (source=gs_node.json, "
				<< "name=dataStore.storeMemoryLimit, "
				<< "value=" << limitMegaByte << "MB, "
				<< "max=" << ChunkManager::LIMIT_CHUNK_NUM_LIST[nth] << "MB)");
	}
	atomicStoreMemoryLimit_ = memoryLimitByte;
	for (uint32_t pId = 0; pId < partitionCount_; ++pId) {
		partitionList_->partition(pId).chunkManager().getConfig()
				.setAtomicStoreMemoryLimit(memoryLimitByte);
	}
	partitionList_->interchangeableStoreMemory().updateBufferSize(memoryLimitByte);
	return true;
}

bool PartitionList::Config::setAtomicShiftableMemRate(int32_t rate) {
	atomicShiftableMemRate_ = rate;
	partitionList_->interchangeableStoreMemory().
			updateShiftableMemRate(atomicShiftableMemRate_);
	return true;
}

bool PartitionList::Config::setAtomicEMAHalfLifePeriod(int32_t period) {
	atomicEMAHalfLifePeriod_ = period;
	partitionList_->interchangeableStoreMemory().
			updateEMAHalfLifePeriod(atomicEMAHalfLifePeriod_);
	return true;
}

bool PartitionList::Config::setAtomicStoreMemoryColdRate(double rate) {
	if (rate > 1.0) {
		rate = 1.0;
	}
	atomicStoreMemoryColdRate_ = static_cast<uint64_t>(rate * 1.0E6);
	for (uint32_t pId = 0; pId < partitionCount_; ++pId) {
		partitionList_->partition(pId).chunkManager().getConfig()
				.setAtomicStoreMemoryColdRate(rate);
	}
	return true;
}

bool PartitionList::Config::setAtomicCpFileFlushSize(uint64_t size) {
	atomicCpFileFlushSize_ = size;
	atomicCpFileFlushInterval_ = size / (UINT64_C(1) << chunkExpSize_);
	GS_TRACE_INFO(
			SIZE_MONITOR, GS_TRACE_CHM_CP_FILE_FLUSH_SIZE,
			"setAtomicCpFileFlushSize: size," << atomicCpFileFlushSize_ <<
			",interval," << atomicCpFileFlushInterval_);
	for (uint32_t pId = 0; pId < partitionCount_; ++pId) {
		partitionList_->partition(pId).chunkManager().getConfig()
				.setAtomicCpFileFlushSize(size);
	}
	return true;
}
bool PartitionList::Config::setAtomicCpFileAutoClearCache(bool flag) {
	atomicCpFileAutoClearCache_ = flag;
	for (uint32_t pId = 0; pId < partitionCount_; ++pId) {
		partitionList_->partition(pId).chunkManager().getConfig()
				.setAtomicCpFileAutoClearCache(flag);
	}
	return true;
}

uint32_t PartitionList::Config::getChunkSize(uint32_t chunkExpSize) {
	const uint32_t size = 1 << chunkExpSize;
	assert(util::nextPowerBitsOf2(size) == chunkExpSize);
	return size;
}

uint32_t PartitionList::Config::getChunkSize(const ConfigTable &configTable) {
	return getChunkSize(getChunkExpSize(configTable));
}

uint32_t PartitionList::Config::getChunkExpSize(const ConfigTable &configTable) {
	return util::nextPowerBitsOf2(
			configTable.getUInt32(CONFIG_TABLE_DS_STORE_BLOCK_SIZE));
}


PartitionListStats::PartitionListStats(
		const util::StdAllocator<void, void> &alloc,
		const ConfigTable& configTable, PartitionList::Config &config) :
		alloc_(alloc),
		configTable_(configTable),
		config_(config),
		mapper_(ALLOC_UNIQUE(
				alloc_, Mapper, alloc_,
				PartitionList::Config::getChunkSize(configTable))),
		sub_(ALLOC_UNIQUE(alloc_, SubEntrySet, alloc_)) {
	sub_->initializeSub(*mapper_, configTable);
}

void PartitionListStats::initialize(
		StatTable &stat, const DataStoreConfig &dsConfig,
		Chunk::ChunkMemoryPool &chunkMemoryPool,
		InterchangeableStoreMemory &ism) {
	sub_->ism_ = &ism;
	updator_ = ALLOC_UNIQUE(alloc_, Updator, *this, dsConfig, chunkMemoryPool);
	stat.addUpdator(updator_.get());
}

Partition::StatsSet PartitionListStats::toPartitionStatsSet(PartitionId pId) {
	PartitionEntry &ptEntry = getPartitionEntry(pId);
	const DataStoreBase::StatsSet dsStats(ptEntry.dsStats_, ptEntry.objMgrStats_);
	return Partition::StatsSet(
			sub_->dataFileStats_, ptEntry.logMgrStats_, ptEntry.chunkMgrStats_,
			dsStats);
}

PartitionListStats::PartitionEntry& PartitionListStats::getPartitionEntry(
		PartitionId pId) {
	return SubEntrySet::getListElement(sub_->ptList_, pId);
}

PartitionListStats::PartitionGroupEntry&
PartitionListStats::getPartitionGroupEntry(PartitionGroupId pgId) {
	return SubEntrySet::getListElement(sub_->pgList_, pgId);
}

void PartitionListStats::getPGChunkBufferStats(
		PartitionGroupId pgId, ChunkBufferStats &stats) const {
	const PartitionGroupEntry &pgEntry =
			SubEntrySet::getListElementRef(sub_->pgList_, pgId);
	stats.table_.assign(pgEntry.chunkBufStats_.table_);

	const uint32_t chunkSize =
			PartitionList::Config::getChunkSize(configTable_);
	{
		const PartitionGroupConfig pgConfig(configTable_);
		const PartitionId beginPId = pgConfig.getGroupBeginPartitionId(pgId);
		const PartitionId endPId = pgConfig.getGroupEndPartitionId(pgId);
		for (PartitionId pId = beginPId; pId != endPId; pId++) {
			const ChunkManagerStats &cmStats = SubEntrySet::getListElementRef(
					sub_->ptList_, pId).chunkMgrStats_;
			const uint64_t chunkCount = cmStats.table_(
					ChunkManagerStats::CHUNK_STAT_USE_CHUNK_COUNT).get();
			stats.table_(ChunkBufferStats::BUF_STAT_USE_STORE_SIZE).add(
					chunkSize * chunkCount);
		}
	}

	{
		const uint64_t bufCount = stats.table_(
				ChunkBufferStats::BUF_STAT_USE_BUFFER_COUNT).get();
		stats.table_(ChunkBufferStats::BUF_STAT_USE_BUFFER_SIZE).set(
				chunkSize * bufCount);
	}

	{
		InterchangeableStoreMemory *ism = sub_->ism_;
		assert(ism != NULL);
		const uint64_t limitCount = ism->getChunkBufferLimit(pgId);
		stats.table_(ChunkBufferStats::BUF_STAT_BUFFER_LIMIT_SIZE).set(
				chunkSize * limitCount);
	}
}

void PartitionListStats::getPGCategoryChunkBufferStats(
		PartitionGroupId pgId, ChunkCategoryId categoryId,
		ChunkBufferStats &stats) const {
	ChunkGroupStats::Table groupStats(NULL);
	{
		const PartitionGroupConfig pgConfig(configTable_);
		const PartitionId beginPId = pgConfig.getGroupBeginPartitionId(pgId);
		const PartitionId endPId = pgConfig.getGroupEndPartitionId(pgId);
		for (PartitionId pId = beginPId; pId != endPId; pId++) {
			const DataStoreStats &dsStats = SubEntrySet::getListElementRef(
					sub_->ptList_, pId).dsStats_;
			ChunkGroupStats::Table subStats(NULL);
			dsStats.getChunkStats(categoryId, subStats);
			groupStats.merge(subStats);
		}
	}

	const uint32_t chunkSize =
			PartitionList::Config::getChunkSize(configTable_);

	typedef std::pair<ChunkGroupStats::Param, ChunkBufferStats::Param> ParamEntry;
	typedef std::pair<uint32_t, ParamEntry> FullParamEntry;

	const FullParamEntry entryList[] = {
		FullParamEntry(chunkSize, ParamEntry(
				ChunkGroupStats::GROUP_STAT_USE_BLOCK_COUNT,
				ChunkBufferStats::BUF_STAT_USE_STORE_SIZE)),
		FullParamEntry(chunkSize, ParamEntry(
				ChunkGroupStats::GROUP_STAT_USE_BUFFER_COUNT,
				ChunkBufferStats::BUF_STAT_USE_BUFFER_SIZE)),
		FullParamEntry(1, ParamEntry(
				ChunkGroupStats::GROUP_STAT_SWAP_READ_COUNT,
				ChunkBufferStats::BUF_STAT_SWAP_READ_COUNT)),
		FullParamEntry(1, ParamEntry(
				ChunkGroupStats::GROUP_STAT_SWAP_WRITE_COUNT,
				ChunkBufferStats::BUF_STAT_SWAP_WRITE_COUNT)),
	};

	const size_t entryCount = sizeof(entryList) / sizeof(*entryList);
	for (size_t i = 0; i < entryCount; i++) {
		const uint32_t unit = entryList[i].first;
		const ParamEntry params = entryList[i].second;
		stats.table_(params.second).set(unit * groupStats(params.first).get());
	}
}


PartitionListStats::SubEntrySet::SubEntrySet(
		const util::StdAllocator<void, void> &alloc) :
		alloc_(alloc),
		ptList_(alloc),
		pgList_(alloc),
		dataFileStats_(alloc),
		ism_(NULL) {
}

PartitionListStats::SubEntrySet::~SubEntrySet() {
	clearList(ptList_);
	clearList(pgList_);
}

void PartitionListStats::SubEntrySet::initializeSub(
		const Mapper &mapper, const ConfigTable& configTable) {
	const PartitionGroupConfig pgConfig(configTable);
	initializetList(ptList_, mapper, pgConfig.getPartitionCount());
	initializetList(pgList_, mapper, pgConfig.getPartitionCount());
}

template<typename E>
void PartitionListStats::SubEntrySet::initializetList(
		util::AllocVector<E*> &list, const Mapper &mapper, uint32_t count) {
	assert(list.empty());
	for (uint32_t i = 0; i < count; i++) {
		util::AllocUniquePtr<E> entry(ALLOC_UNIQUE(alloc_, E, mapper));
		list.push_back(entry.get());
		entry.release();
	}
}

template<typename E>
E& PartitionListStats::SubEntrySet::getListElement(
		util::AllocVector<E*> &list, size_t index) {
	getListElementRef(list, index);
	return *list[index];
}

template<typename E>
const E& PartitionListStats::SubEntrySet::getListElementRef(
		const util::AllocVector<E*> &list, size_t index) {
	if (index >= list.size() || list[index] == NULL) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	return *list[index];
}

template<typename E>
void PartitionListStats::SubEntrySet::clearList(util::AllocVector<E*> &list) {
	while (!list.empty()) {
		util::AllocUniquePtr<E> ptr(list.back(), alloc_);
		list.pop_back();
	}
}


PartitionListStats::PartitionEntry::PartitionEntry(const Mapper &mapper) :
		logMgrStats_(&mapper.logMgrMapper_),
		chunkMgrStats_(&mapper.chunkMgrMapper_),
		objMgrStats_(&mapper.objMgrMapper_),
		dsStats_(&mapper.dsMapper_) {
}

void PartitionListStats::PartitionEntry::apply(StatTable &stat) const {
	logMgrStats_.table_.apply(stat);
	chunkMgrStats_.table_.apply(stat);
	objMgrStats_.table_.apply(stat);
	dsStats_.table_.apply(stat);
}

void PartitionListStats::PartitionEntry::merge(const PartitionEntry &entry) {
	logMgrStats_.merge(entry.logMgrStats_.table_);
	chunkMgrStats_.table_.merge(entry.chunkMgrStats_.table_);
	objMgrStats_.table_.merge(entry.objMgrStats_.table_);
	dsStats_.table_.merge(entry.dsStats_.table_);
}


PartitionListStats::PartitionGroupEntry::PartitionGroupEntry(
		const Mapper &mapper) :
		chunkBufStats_(&mapper.chunkBufMapper_) {
}

void PartitionListStats::PartitionGroupEntry::apply(StatTable &stat) const {
	chunkBufStats_.table_.apply(stat);
}

void PartitionListStats::PartitionGroupEntry::merge(
		const PartitionGroupEntry &entry) {
	chunkBufStats_.table_.merge(entry.chunkBufStats_.table_);
}


PartitionListStats::TotalEntry::TotalEntry(const Mapper &mapper) :
		fileStats_(&mapper.dataFileMapper_),
		table_(&mapper.partitionListMapper_) {
}

void PartitionListStats::TotalEntry::apply(StatTable &stat) const {
	fileStats_.apply(stat);
	table_.apply(stat);
}

void PartitionListStats::TotalEntry::merge(
		FileStatTable &dataFileStats, const PartitionEntry &ptEntry) {
	UNUSED_VARIABLE(ptEntry);

	dataFileStats.get(fileStats_);
}


PartitionListStats::Mapper::Mapper(
		const util::StdAllocator<void, void> &alloc, uint32_t chunkSize) :
		dataFileMapper_(alloc),
		logMgrMapper_(alloc),
		chunkMgrMapper_(alloc),
		objMgrMapper_(alloc),
		dsMapper_(alloc),
		chunkBufMapper_(alloc),
		partitionListMapper_(alloc) {
	setUpDataFileMapper(dataFileMapper_);

	LogManagerStats::setUpMapper(logMgrMapper_);
	ChunkManagerStats::setUpMapper(chunkMgrMapper_, chunkSize);
	ObjectManagerStats::setUpMapper(objMgrMapper_);
	DataStoreStats::setUpMapper(dsMapper_, chunkSize);

	ChunkBufferStats::setUpMapper(chunkBufMapper_);
	setUpMapper(partitionListMapper_);
}

void PartitionListStats::Mapper::setUpDataFileMapper(
		FileStatTable::BaseMapper &mapper) {
	mapper.addFilter(STAT_TABLE_DISPLAY_SELECT_CP);

	mapper.bind(
			FileStatTable::FILE_STAT_SIZE, STAT_TABLE_PERF_DATA_FILE_SIZE);
	mapper.bind(
			FileStatTable::FILE_STAT_ALLOCATED_SIZE,
			STAT_TABLE_PERF_DATA_FILE_ALLOCATE_SIZE);
}

void PartitionListStats::Mapper::setUpMapper(Table::Mapper &mapper) {
	for (size_t i = 0; i < 2; i++) {
		Table::Mapper sub(mapper.getSource());
		setUpBasicMapper(sub, (i > 0));
		mapper.addSub(sub);
	}

	for (size_t i = 0; i < 2; i++) {
		Table::Mapper sub(mapper.getSource());
		setUpCPMapper(sub, (i > 0));
		mapper.addSub(sub);
	}

	{
		Table::Mapper sub(mapper.getSource());
		setUpDSMapper(sub);
		mapper.addSub(sub);
	}

	{
		Table::Mapper sub(mapper.getSource());
		setUpTxnMapper(sub);
		mapper.addSub(sub);
	}
}

void PartitionListStats::Mapper::setUpBasicMapper(
		Table::Mapper &mapper, bool detail) {
	mapper.addFilter(STAT_TABLE_DISPLAY_SELECT_PERF);

	if (detail) {
		mapper.addFilter(STAT_TABLE_DISPLAY_WEB_ONLY);

		mapper.bind(
				PL_STAT_STORE_MEMORY_LIMIT,
				STAT_TABLE_PERF_DS_STORE_MEMORY_LIMIT);
	}
	else {
		mapper.bind(
				PL_STAT_STORE_MEMORY,
				STAT_TABLE_PERF_DS_STORE_MEMORY);
		mapper.bind(
				PL_STAT_ERASABLE_EXPIRED_TIME,
				STAT_TABLE_PERF_DS_EXP_ERASABLE_EXPIRED_TIME)
				.setDateTimeType();
	}
}

void PartitionListStats::Mapper::setUpCPMapper(
		Table::Mapper &mapper, bool detail) {
	mapper.addFilter(STAT_TABLE_DISPLAY_SELECT_PERF);
	mapper.addFilter(STAT_TABLE_DISPLAY_SELECT_CP);

	if (detail) {
		mapper.addFilter(STAT_TABLE_DISPLAY_WEB_ONLY);
	}
	else {
		mapper.bind(
				PL_STAT_COMPRESSION_MODE,
				STAT_TABLE_PERF_STORE_COMPRESSION_MODE)
				.setNameCoder(&ChunkManager::Coders::MODE_CODER);
	}
}

void PartitionListStats::Mapper::setUpDSMapper(
		Table::Mapper &mapper) {
	mapper.addFilter(STAT_TABLE_DISPLAY_SELECT_PERF);
	mapper.addFilter(STAT_TABLE_DISPLAY_WEB_ONLY);
	mapper.addFilter(STAT_TABLE_DISPLAY_OPTIONAL_DS);

	mapper.bind(
			PL_STAT_POOL_BUFFER_MEMORY,
			STAT_TABLE_PERF_DS_DETAIL_POOL_BUFFER_MEMORY);
}

void PartitionListStats::Mapper::setUpTxnMapper(
		Table::Mapper &mapper) {
	mapper.addFilter(STAT_TABLE_DISPLAY_SELECT_PERF);
	mapper.addFilter(STAT_TABLE_DISPLAY_WEB_ONLY);
	mapper.addFilter(STAT_TABLE_DISPLAY_OPTIONAL_TXN);

	mapper.bind(
			PL_STAT_BACKGROUND_MIN_RATE,
			STAT_TABLE_PERF_TXN_BACKGROUND_MIN_RATE)
			.setValueType(ParamValue::PARAM_TYPE_DOUBLE)
			.setConversionUnit(100, true);
	mapper.bind(
			PL_STAT_BATCH_SCAN_INTERVAL_NUM,
			STAT_TABLE_PERF_DS_EXP_BATCH_SCAN_NUM);
}


PartitionListStats::Updator::Updator(
		PartitionListStats &base, const DataStoreConfig &dsConfig,
		Chunk::ChunkMemoryPool &chunkMemoryPool) :
		base_(base),
		dsConfig_(dsConfig),
		chunkMemoryPool_(chunkMemoryPool) {
}

bool PartitionListStats::Updator::operator()(StatTable &stat) {
	Mapper &mapper = *base_.mapper_;

	PartitionEntry ptEntry(mapper);
	{
		PTList &list = base_.sub_->ptList_;
		for (PTList::iterator it = list.begin(); it != list.end(); ++it) {
			ptEntry.merge(**it);
		}
		ptEntry.apply(stat);
	}

	PartitionGroupEntry pgEntry(mapper);
	{
		PGList &list = base_.sub_->pgList_;
		for (PGList::iterator it = list.begin(); it != list.end(); ++it) {
			pgEntry.merge(**it);
		}
		pgEntry.apply(stat);
	}

	{
		TotalEntry totalEntry(mapper);
		updatePartitionListStats(totalEntry.table_, pgEntry);
		totalEntry.merge(base_.sub_->dataFileStats_, ptEntry);
		totalEntry.apply(stat);
	}

	return true;
}

void PartitionListStats::Updator::updatePartitionListStats(
		PartitionListStats::Table &stats, const PartitionGroupEntry &pgEntry) {
	const uint32_t chunkSize =
			PartitionList::Config::getChunkSize(base_.configTable_);
	{
		util::AllocatorStats allocStats;
		chunkMemoryPool_.getStats(allocStats);

		const uint64_t useCount = pgEntry.chunkBufStats_.table_(
				ChunkBufferStats::BUF_STAT_USE_BUFFER_COUNT).get();
		stats(PL_STAT_STORE_MEMORY).set(
				chunkSize * useCount +
				allocStats.values_[util::AllocatorStats::STAT_CACHE_SIZE]);
		stats(PL_STAT_POOL_BUFFER_MEMORY).set(
				allocStats.values_[util::AllocatorStats::STAT_TOTAL_SIZE]);
	}

	{
		const ConfigTable &configTable = base_.configTable_;
		stats(PL_STAT_COMPRESSION_MODE).set(static_cast<uint64_t>(
				ChunkManager::Config::getCompressionMode(configTable)));
	}

	{
		PartitionList::Config &config = base_.config_;
		stats(PL_STAT_STORE_MEMORY_LIMIT).set(
				config.getAtomicStoreMemoryLimit());
	}

	{
		const DataStoreConfig &dsConfig = dsConfig_;
		stats(PL_STAT_ERASABLE_EXPIRED_TIME).set(
				static_cast<uint64_t>(dsConfig.getErasableExpiredTime()));
		stats(PL_STAT_BACKGROUND_MIN_RATE).set(
				static_cast<uint64_t>(dsConfig.getBackgroundMinRatePct()));
		stats(PL_STAT_BATCH_SCAN_INTERVAL_NUM).set(
				static_cast<uint64_t>(dsConfig.getBatchScanNum()));
	}
}


PartitionListStats::SetUpHandler PartitionListStats::SetUpHandler::instance_;

#define STAT_ADD(id) STAT_TABLE_ADD_PARAM(stat, parentId, id)
#define STAT_ADD_SUB(id) STAT_TABLE_ADD_PARAM_SUB(stat, parentId, id)
#define STAT_ADD_SUB_SUB(id) STAT_TABLE_ADD_PARAM_SUB_SUB(stat, parentId, id)

#define CHUNK_STAT_ADD_CATEGORY(id) \
	stat.resolveGroup(parentId, id, STAT_TABLE_EXTRACT_SYMBOL(id, 5));
#define CHUNK_STAT_ADD_PARAM(id) \
	stat.addParam( \
			parentId, DataStoreStats::getStoreParamId(parentId, id), \
			STAT_TABLE_EXTRACT_SYMBOL(id, 5));

void PartitionListStats::SetUpHandler::operator()(StatTable& stat) {
	StatTable::ParamId parentId;

	parentId = STAT_TABLE_ROOT;
	stat.resolveGroup(parentId, STAT_TABLE_PERF, "performance");

	parentId = STAT_TABLE_PERF;
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_STORE_MEMORY_LIMIT);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_STORE_MEMORY);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_STORE_TOTAL_USE);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_ALLOCATE_DATA);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_BATCH_FREE);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_SWAP_READ);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_SWAP_WRITE);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_SWAP_READ_SIZE);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_SWAP_READ_TIME);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_SWAP_WRITE_SIZE);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_SWAP_WRITE_TIME);

	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_DS_DETAIL_POOL_BUFFER_MEMORY);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_CHECKPOINT_FILE_FLUSH_COUNT);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_CHECKPOINT_FILE_FLUSH_TIME);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_LOG_FILE_FLUSH_COUNT);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_LOG_FILE_FLUSH_TIME);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_SWAP_READ_UNCOMPRESS_TIME);
	STAT_ADD_SUB(STAT_TABLE_PERF_DS_SWAP_WRITE_COMPRESS_TIME);

	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_NUM_BACKGROUND);
	STAT_ADD_SUB(STAT_TABLE_PERF_TXN_BACKGROUND_MIN_RATE);

	STAT_ADD(STAT_TABLE_PERF_DATA_FILE_SIZE);
	STAT_ADD(STAT_TABLE_PERF_DATA_FILE_USAGE_RATE);
	STAT_ADD(STAT_TABLE_PERF_STORE_COMPRESSION_MODE);
	STAT_ADD(STAT_TABLE_PERF_DATA_FILE_ALLOCATE_SIZE);
	STAT_ADD(STAT_TABLE_PERF_CHECKPOINT_WRITE); 
	STAT_ADD(STAT_TABLE_PERF_CHECKPOINT_WRITE_SIZE);
	STAT_ADD(STAT_TABLE_PERF_CHECKPOINT_WRITE_TIME);
	STAT_ADD(STAT_TABLE_PERF_CHECKPOINT_WRITE_COMPRESS_TIME); 

	stat.resolveGroup(parentId, STAT_TABLE_PERF_DS_EXP, "expirationDetail");

	parentId = STAT_TABLE_PERF_DS_EXP;
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_DS_EXP_ERASABLE_EXPIRED_TIME);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_DS_EXP_LATEST_EXPIRATION_CHECK_TIME);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_DS_EXP_ESTIMATED_ERASABLE_EXPIRED_TIME);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_DS_EXP_ESTIMATED_BATCH_FREE);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_DS_EXP_LAST_BATCH_FREE);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_DS_EXP_BATCH_SCAN_TOTAL_NUM);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_DS_EXP_BATCH_SCAN_TOTAL_TIME);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_DS_EXP_BATCH_SCAN_NUM);

	parentId = STAT_TABLE_PERF;
	stat.resolveGroup(parentId, STAT_TABLE_PERF_DS_DETAIL, "storeDetail");

	parentId = STAT_TABLE_PERF_DS_DETAIL;
	CHUNK_STAT_ADD_CATEGORY(STAT_TABLE_PERF_DS_DETAIL_META_DATA);
	CHUNK_STAT_ADD_CATEGORY(STAT_TABLE_PERF_DS_DETAIL_MAP_DATA);
	CHUNK_STAT_ADD_CATEGORY(STAT_TABLE_PERF_DS_DETAIL_ROW_DATA);
	CHUNK_STAT_ADD_CATEGORY(STAT_TABLE_PERF_DS_DETAIL_BATCH_FREE_MAP_DATA);
	CHUNK_STAT_ADD_CATEGORY(STAT_TABLE_PERF_DS_DETAIL_BATCH_FREE_ROW_DATA);
	for (int32_t parentId = STAT_TABLE_PERF_DS_DETAIL_CATEGORY_START + 1;
		 parentId < STAT_TABLE_PERF_DS_DETAIL_CATEGORY_END; parentId++) {
		CHUNK_STAT_ADD_PARAM(STAT_TABLE_PERF_DS_DETAIL_STORE_MEMORY);
		CHUNK_STAT_ADD_PARAM(STAT_TABLE_PERF_DS_DETAIL_STORE_USE);
		CHUNK_STAT_ADD_PARAM(STAT_TABLE_PERF_DS_DETAIL_SWAP_READ);
		CHUNK_STAT_ADD_PARAM(STAT_TABLE_PERF_DS_DETAIL_SWAP_WRITE);
	}
	parentId = STAT_TABLE_ROOT;
	stat.resolveGroup(parentId, STAT_TABLE_CP, "checkpoint");

	parentId = STAT_TABLE_CP;
	STAT_ADD(STAT_TABLE_CP_DUPLICATE_LOG);
}

