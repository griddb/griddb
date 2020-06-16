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
	@brief Definition of CheckpointService
*/
#ifndef CHECKPOINT_SERVICE_H_
#define CHECKPOINT_SERVICE_H_

#include "util/container.h"
#include "bit_array.h"
#include "cluster_event_type.h"  
#include "data_type.h"
#include "event_engine.h"
#include "gs_error.h"
#include "recovery_manager.h"  
#include "system_service.h"	
#include "sync_manager.h"

class CheckpointServiceMainHandler;  
class CheckpointServiceGroupHandler;  
class CheckpointOperationHandler;   
class LogFlushPeriodicallyHandler;  
class ConfigTable;
class ResourceSet;

/*!
	@brief Locks a Partition group.
*/
class PartitionGroupLock {
public:
	PartitionGroupLock(TransactionManager &txnManager, PartitionGroupId pgId);
	~PartitionGroupLock();

private:
	TransactionManager &transactionManager_;
	PartitionGroupId pgId_;
};

/*!
	@brief Locks a Partition.
*/
class PartitionLock {
public:
	PartitionLock(TransactionManager &txnManager, PartitionId pId);
	~PartitionLock();

private:
	TransactionManager &transactionManager_;
	PartitionId pId_;
};

static const uint32_t CP_HANDLER_PARTITION_ID = 0;
static const uint32_t CP_SERIALIZED_PARTITION_ID = 0;

static const int32_t secondLimit = INT32_MAX / 1000;

/*!
	@brief Converts time unit from second to millisecond.
*/
static inline int32_t changeTimeSecondToMilliSecond(int32_t second) {
	if (second > secondLimit) {
		return INT32_MAX;
	}
	else {
		return second * 1000;
	}
}

class LogManager;
class RecoveryManager;
class TransactionManager;
class TransactionService;
class CheckpointManager;

/*!
	@brief Operates Checkpoint events.
*/
class CheckpointHandler : public EventHandler {
public:
	CheckpointHandler(){};
	~CheckpointHandler(){};

	void initialize(const ResourceSet &resourceSet);

	TransactionService *transactionService_;
	TransactionManager *transactionManager_;
	CheckpointService *checkpointService_;
	ClusterService *clusterService_;

	LogManager *logManager_;
	DataStore *dataStore_;
	ChunkManager *chunkManager_;
	GlobalFixedSizeAllocator *fixedSizeAlloc_;
	ConfigTable *config_;

private:
};

/*!
	@brief Operates main Checkpoint events.
*/
class CheckpointServiceMainHandler : public CheckpointHandler {
public:
	CheckpointServiceMainHandler(){};
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Operates Checkpoint events for each group of CheckpointService.
*/
class CheckpointServiceGroupHandler : public CheckpointHandler {
public:
	CheckpointServiceGroupHandler(){};
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Operates Checkpoint events for Transaction Service.
*/
class CheckpointOperationHandler : public CheckpointHandler {
public:
	static const int32_t CHUNK_META_DATA_LOG_MAX_NUM = 10000;

	CheckpointOperationHandler(){};
	void operator()(EventContext &ec, Event &ev);
	LogSequentialNumber writeCheckpointStartLog(
			util::StackAllocator &alloc, int32_t mode,
			PartitionGroupId pgId, PartitionId pId, CheckpointId cpId);

	void writeChunkMetaDataLog(
			util::StackAllocator &alloc, int32_t mode,
			PartitionGroupId pgId, PartitionId pId, CheckpointId cpId,
			bool isRestored);

	void compressChunkMetaDataLog(
			util::StackAllocator &alloc,
			PartitionGroupId pgId, PartitionId pId,
			ChunkCategoryId categoryId,
			ChunkId startChunkId, int32_t count,
			util::XArray<uint8_t> &logBuffer,
			util::XArray<uint8_t> &metaDataEmptySize,
			util::XArray<uint8_t> &metaDataFileOffset,
			util::XArray<uint8_t> &metaDataChunkKey, int32_t validCount);

	void checkFailureWhileWriteChunkMetaLog(int32_t mode);

	void setIncrementalBackupFilePos(
			PartitionId pId, bool isCumulative, ChunkBitArray &bitArray);

	void resetIncrementalBackupFilePos(PartitionId pId);
};

/*!
	@brief Operates intermittent log flush.
*/
class FlushLogPeriodicallyHandler : public CheckpointHandler {
public:
	FlushLogPeriodicallyHandler(){};
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Operates Checkpoint events.
*/
class CheckpointFile;   
class LongtermSyncInfo; 
class CheckpointService {
public:
	friend class CheckpointOperationHandler;
	friend class CheckpointServiceMainHandler;
	friend class CheckpointServiceGroupHandler;

	static const uint32_t CP_CHUNK_COPY_NUM = 10;  
	static const uint32_t ALL_GROUP_CHECKPOINT_END_CHECK_INTERVAL =
		1000;  
	static const std::string PID_LSN_INFO_FILE_NAME;
	static const uint32_t PID_LSN_INFO_FILE_VERSION = 0x1;  

	static const uint32_t BACKUP_ARCHIVE_LOG_MODE_FLAG = 1;
	static const uint32_t BACKUP_DUPLICATE_LOG_MODE_FLAG = 1 << 1;
	static const uint32_t BACKUP_SKIP_BASELINE_MODE_FLAG = 1 << 2; 
	static const uint64_t INCREMENTAL_BACKUP_COUNT_LIMIT = 999;

	static const char *const INCREMENTAL_BACKUP_FILE_SUFFIX;
	static const uint32_t MAX_BACKUP_NAME_LEN;
	static const uint32_t MAX_LONG_ARCHIVE_NAME_LEN;

	static const char *const SYNC_TEMP_FILE_SUFFIX; 

	typedef int64_t SyncSequentialNumber; 

	static const SyncSequentialNumber UNDEF_SYNC_SEQ_NUMBER; 
	static const SyncSequentialNumber MAX_SYNC_SEQ_NUMBER; 


	/*!
		@brief Types of events
	*/
	enum EventType {
		CP_REQUEST_CHECKPOINT,
		CP_TIMER_LOG_FLUSH,
		CP_REQUEST_GROUP_CHECKPOINT  
	};

	/*!
		@brief Status of checkpoint for each partitionGroup
	*/
	enum GroupCheckpointStatus { GROUP_CP_COMPLETED = 0, GROUP_CP_RUNNING };

	enum ArchiveLogMode {
		ARCHIVE_LOG_OFF = 0,
		ARCHIVE_LOG_ON,
		ARCHIVE_LOG_RUNNING
	};

	enum DuplicateLogMode {
		DUPLICATE_LOG_OFF = 0,
		DUPLICATE_LOG_ON,
		DUPLICATE_LOG_ERROR = -1
	};

	/*!
		@brief Status of backup
	*/
	struct BackupStatus {
		BackupStatus()
			: name_(""),
			  cumlativeCount_(0),
			  differentialCount_(0),
			  incrementalLevel_(-1),
			  archiveLogMode_(false),
			  skipBaselineMode_(false),  
			  duplicateLogMode_(false){};

		void reset() {
			name_.clear();
			cumlativeCount_ = 0;
			differentialCount_ = 0;
			incrementalLevel_ = 0;
			archiveLogMode_ = false;
			skipBaselineMode_ = false;  
			duplicateLogMode_ = false;
		}

		u8string name_;
		uint64_t cumlativeCount_;
		uint64_t differentialCount_;
		int32_t incrementalLevel_;
		bool archiveLogMode_;
		bool skipBaselineMode_;  
		bool duplicateLogMode_;
	};
	typedef std::map<int64_t, int64_t> CpLongtermSyncOffsetMap;
	struct CpLongtermSyncInfo {
		CpLongtermSyncInfo()
			: ssn_(UNDEF_SYNC_SEQ_NUMBER),
			  targetPId_(UNDEF_PARTITIONID),
			  logManager_(NULL),
			  syncCpFile_(NULL),
			  startLsn_(0),
			  cpId_(UNDEF_CHECKPOINT_ID),
			  readItr_(NULL),
			  newOffsetMap_(NULL),
			  oldOffsetMap_(NULL),
			  readCount_(0),
			  eventSrc_(NULL),
			  errorOccured_(false),
			  chunkSize_(0),
			  totalChunkCount_(0),
			  readyCount_(0),
			  atomicStopFlag_(0),
			  startTime_(0) {};

		SyncSequentialNumber ssn_;
		PartitionId targetPId_;
		LogManager* logManager_;
		CheckpointFile* syncCpFile_;
		u8string dir_; 
		LogSequentialNumber startLsn_;
		CheckpointId cpId_;
		CpLongtermSyncOffsetMap::iterator readItr_;
		CpLongtermSyncOffsetMap *newOffsetMap_;
		CpLongtermSyncOffsetMap *oldOffsetMap_;
		uint64_t readCount_;
		EventEngine::Source *eventSrc_;
		LongtermSyncInfo longtermSyncInfo_;
		bool errorOccured_;
		uint32_t chunkSize_;  
		size_t totalChunkCount_;  
		util::Atomic<uint64_t> readyCount_;  
		util::Atomic<uint32_t> atomicStopFlag_;  
		uint64_t startTime_;
	};

	CheckpointService(
			const ConfigTable &config, EventEngine::Config eeConfig,
			EventEngine::Source source, const char8_t *name,
			ServiceThreadErrorHandler &serviceThreadErrorHandler);

	~CheckpointService();

	void initialize(ResourceSet &resourceSet);


	void start(const Event::Source &eventSource);

	void shutdown();

	void waitForShutdown();

	EventEngine *getEE();

	void executeRecoveryCheckpoint(const Event::Source &eventSource);

	void requestNormalCheckpoint(const Event::Source &eventSource);

	bool requestOnlineBackup(
			const Event::Source &eventSource,
			const std::string &backupName, int32_t mode,
			const SystemService::BackupOption &option,
			picojson::value &result);

	void requestCleanupLogFiles(const Event::Source &eventSource);

	bool requestPrepareLongArchive(
		const Event::Source &eventSource,
		const std::string &archiveName, picojson::value &result);

	bool checkLongtermSyncIsReady(SyncSequentialNumber ssn);

	void requestShutdownCheckpoint(const Event::Source &eventSource);

	void executeOnTransactionService(
			EventContext &ec, GSEventType eventType,
			int32_t mode, PartitionId pId, CheckpointId cpId,
			const std::string &backupPath, bool executeAndWait,
			SyncSequentialNumber ssn);

	CheckpointId getLastCompletedCheckpointId(PartitionGroupId pgId) const;

	CheckpointId getCurrentCheckpointId(PartitionGroupId pgId) const;

	const PartitionGroupConfig &getPGConfig() const {
		return pgConfig_;
	}

	static const char8_t *checkpointModeToString(int32_t mode);

	void changeParam(std::string &paramName, std::string &value);

	int32_t getLastMode() const {
		return lastMode_;
	}

	int64_t getLastDuration(const util::DateTime &now) const {
		const int64_t startTime = startTime_;
		const int64_t endTime = endTime_;
		if (startTime == 0) {
			return 0;
		}
		else if (endTime > 0 && startTime <= endTime) {
			return endTime - startTime;
		}
		else {
			return (now.getUnixTime() - startTime);
		}
	}

	int64_t getLastStartTime() const {
		return startTime_;
	}

	int64_t getLastEndTime() const {
		return endTime_;
	}

	int32_t getPendingPartitionCount() const {
		return pendingPartitionCount_;
	}

	int64_t getTotalNormalCpOperation() const {
		return totalNormalCpOperation_;
	}

	int64_t getTotalRequestedCpOperation() const {
		return totalRequestedCpOperation_;
	}

	int64_t getTotalBackupOperation() const {
		return totalBackupOperation_;
	}

	void setCurrentCpGrpId(PartitionGroupId pgId) {
		currentCpGrpId_ = pgId;
	}

	PartitionGroupId getCurrentCpGroupId() {
		return currentCpGrpId_;
	}

	void setCurrentCpPId(PartitionId pId) {
		currentCpPId_ = pId;
	}

	PartitionGroupId getCurrentCpPId() {
		return currentCpPId_;
	}

	void updateLastArchivedCpIdList();
	CheckpointId getLastArchivedCpId(PartitionGroupId pgId);

	void setArchiveLogMode(int32_t mode) {
		archiveLogMode_ = mode;
	}

	int32_t getArchiveLogMode() const {
		return archiveLogMode_;
	}

	BackupStatus &getLastQueuedBackupStatus() {
		return lastQueuedBackupStatus_;
	}

	BackupStatus &getLastCompletedBackupStatus() {
		return lastCompletedBackupStatus_;
	}


	bool getLongSyncChunk(
			SyncSequentialNumber ssn, uint64_t size, uint8_t* buffer,
			uint64_t &resultSize, uint64_t &totalCount,
			uint64_t &completedCount, uint64_t &readyCount);
	bool getLongSyncLog(SyncSequentialNumber ssn,
			LogSequentialNumber starLsn, LogCursor &cursor);

	bool getLongSyncCheckpointStartLog(SyncSequentialNumber ssn, LogCursor &cursor);

	bool isEntry(SyncSequentialNumber ssn);

	SyncSequentialNumber getCurrentSyncSequentialNumber(PartitionId pId);

	CpLongtermSyncInfo* getCpLongtermSyncInfo(SyncSequentialNumber ssn);

	bool setCpLongtermSyncInfo(SyncSequentialNumber id, const CpLongtermSyncInfo &cpLongtermSyncInfo);

	bool updateCpLongtermSyncInfo(SyncSequentialNumber id, const CpLongtermSyncInfo &cpLongtermSyncInfo);

	bool removeCpLongtermSyncInfo(SyncSequentialNumber id);

	PartitionTable *getPartitionTable() {
		return partitionTable_;
	}

	void setCurrentCheckpointId(PartitionId pId, CheckpointId cpId) {
		currentCheckpointIdList_[pId] = cpId;
	}

	CheckpointId getCurrentCheckpointId(PartitionId pId) {
		return currentCheckpointIdList_[pId];
	}

	void setCheckpointReady(PartitionId pId, bool flag) {
		checkpointReadyList_[pId] = (flag ? 1 : 0);
	}

	bool isCheckpointReady(PartitionId pId) {
		return (checkpointReadyList_[pId] == 1);
	}

	/*!
		@brief Gets transactionEE queue size
	*/
	int32_t getTransactionEEQueueSize(PartitionGroupId pgId);

	void requestStartCheckpointForLongtermSync(
			const Event::Source &eventSource,
			PartitionId pId, LongtermSyncInfo *longtermSynInfo);

	void requestStopCheckpointForLongtermSync(
			const Event::Source &eventSource,
			PartitionId pId, int64_t syncSequentialNumber);

	void setPeriodicCheckpointFlag(bool flag);
	bool getPeriodicCheckpointFlag();

	uint16_t getNewestLogVersion();
	void setNewestLogVersion(uint16_t version);
	
	int32_t getChunkCopyInterval() {
		return chunkCopyIntervalMillis_;
	}

	int32_t getChunkCopyLimitQueueSize() {
		return CP_CHUNK_COPY_WITH_SLEEP_LIMIT_QUEUE_SIZE;
	}

private:
	static const int32_t CP_CHUNK_COPY_WITH_SLEEP_LIMIT_QUEUE_SIZE = 40;

	/*!
		@brief Manages LSNs for each Partition.
	*/
	class PIdLsnInfo {
	public:
		PIdLsnInfo();
		~PIdLsnInfo();

		void setConfigValue(
				CheckpointService *cpService, uint32_t partitionNum,
				uint32_t partitionGroupNum, const std::string &path);

		void startCheckpoint();  

		void setLsn(PartitionId pId, LogSequentialNumber lsn);

		void endCheckpoint(const char8_t *
				backupPath);  

	private:
		void writeFile(const char8_t *backupPath = NULL);

		CheckpointService *checkpointService_;
		util::Mutex mutex_;
		uint32_t partitionNum_;
		uint32_t partitionGroupNum_;

		std::string path_;
		std::vector<LogSequentialNumber> lsnList_;
	};

	static EventEngine::Config &createEEConfig(
			const ConfigTable &config, EventEngine::Config &eeConfig);

	void runCheckpoint(
			EventContext &ec, int32_t mode, uint32_t flag,
			const std::string &backupPath, SyncSequentialNumber ssn);

	void runGroupCheckpoint(
			PartitionGroupId pgId, EventContext &ec,
			int32_t mode, uint32_t flag, const std::string &backupPath,
			SyncSequentialNumber ssn);

	void waitAllGroupCheckpointEnd();

	uint64_t backupLog(
			uint32_t flag, PartitionGroupId pgId, const std::string &backupPath);

	bool makeBackupDirectory(
			int32_t mode, bool makeSubDir,
			const std::string &backupName);

	void setLastCpStartLsn(PartitionId pId, LogSequentialNumber lsn);

	LogSequentialNumber diffLastCpStartLsn(
			PartitionId pId, LogSequentialNumber lsn);

	void setNeedGroupCheckpoint(PartitionGroupId pgId, bool needCp);

	bool makeLongArchiveDirectory(
			int32_t mode, const std::string &longArchivePath);
	std::string getChunkHeaderDumpString(const uint8_t* chunk);

	inline int32_t getCheckpointInterval() const {
		return cpInterval_;
	}

	static class ConfigSetUpHandler : public ConfigTable::SetUpHandler {
		virtual void operator()(ConfigTable &config);
	} configSetUpHandler_;

	static class StatSetUpHandler : public StatTable::SetUpHandler {
		virtual void operator()(StatTable &stat);
	} statSetUpHandler_;

	class StatUpdator : public StatTable::StatUpdator {
		virtual bool operator()(StatTable &stat);

	public:
		CheckpointService *service_;
	} statUpdator_;

	CheckpointServiceMainHandler checkpointServiceMainHandler_;
	CheckpointServiceGroupHandler checkpointServiceGroupHandler_;
	FlushLogPeriodicallyHandler flushLogPeriodicallyHandler_;

	ServiceThreadErrorHandler serviceThreadErrorHandler_;

	EventEngine ee_;

	ClusterService *clusterService_;
	EventEngine *clsEE_;
	TransactionService *transactionService_;
	TransactionManager *transactionManager_;
	EventEngine *txnEE_;
	SystemService *systemService_;
	LogManager *logManager_;
	DataStore *dataStore_;
	ChunkManager *chunkManager_;
	PartitionTable *partitionTable_;
	bool initailized_;

	SyncService *syncService_;

	GlobalFixedSizeAllocator *fixedSizeAlloc_;

	util::Atomic<bool> requestedShutdownCheckpoint_;

	const PartitionGroupConfig pgConfig_;

	const int32_t cpInterval_;	
	const int32_t logWriteMode_;  
	const std::string backupTopPath_;  
	util::Mutex cpLongtermSyncMutex_;
	const std::string syncTempTopPath_;  
	const std::string longArchiveTopPath_;  

	typedef std::map<SyncSequentialNumber, CpLongtermSyncInfo> CpLongtermSyncInfoMap;
	CpLongtermSyncInfoMap cpLongtermSyncInfoMap_;
	std::vector<SyncSequentialNumber> ssnList_; 

	util::Atomic<int32_t>
		chunkCopyIntervalMillis_;  

	volatile bool backupEndPending_;

	std::string lastBackupPath_;
	bool currentDuplicateLogMode_;

	PartitionGroupId currentCpGrpId_;
	PartitionId currentCpPId_;

	RecoveryManager::BackupInfo backupInfo_;

	std::vector<uint8_t> groupCheckpointStatus_;
	bool parallelCheckpoint_;
	util::Atomic<bool> errorOccured_;

	PIdLsnInfo lsnInfo_;
	bool enableLsnInfoFile_;

	std::vector<CheckpointId> lastArchivedCpIdList_;

	std::vector<LogSequentialNumber> lastCpStartLsnList_;
	std::vector<uint8_t> needGroupCheckpoint_;

	util::NormalXArray<uint8_t> longtermSyncReadBuffer_;
	util::NormalXArray<uint8_t> longtermSyncWriteBuffer_;


	util::Atomic<int32_t> lastMode_;
	util::Atomic<int64_t> startTime_;
	util::Atomic<int64_t> endTime_;
	util::Atomic<int32_t> pendingPartitionCount_;
	util::Atomic<int64_t> totalNormalCpOperation_;
	util::Atomic<int64_t> totalRequestedCpOperation_;
	util::Atomic<int64_t> totalBackupOperation_;

	util::Atomic<int32_t> archiveLogMode_;
	util::Atomic<int32_t> duplicateLogMode_;

	BackupStatus lastQueuedBackupStatus_;
	BackupStatus lastCompletedBackupStatus_;
	util::Atomic<bool> enablePeriodicCheckpoint_;
	std::vector<CheckpointId> currentCheckpointIdList_;

	std::vector<uint8_t> checkpointReadyList_;



	util::Atomic<int32_t> newestLogVersion_;
};


inline uint16_t CheckpointService::getNewestLogVersion() {
	return static_cast<uint16_t>(newestLogVersion_);
}

inline void CheckpointService::setNewestLogVersion(uint16_t version) {
	newestLogVersion_ = static_cast<int32_t>(version);
}

#endif  
