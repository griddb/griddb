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
#include "log_manager.h"

class PartitionList;


class CheckpointServiceMainHandler;  
class LogFlushPeriodicallyHandler;  
class ConfigTable;
struct ManagerSet;

/*!
	@brief パーティションロック(シンクサービスとの排他用)
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

struct CheckpointTypes  {
	enum PeriodicFlag {
		PERIODIC_INACTIVE,
		PERIODIC_ACTIVE,

		PERIODIC_FLAG_END
	};

	static const util::NameCoderEntry<PeriodicFlag> PERIODIC_FLAG_LIST[];
	static const util::NameCoder<
			PeriodicFlag, PERIODIC_FLAG_END> PERIODIC_FLAG_CODER;

	static const util::NameCoderEntry<CheckpointMode> CP_MODE_LIST[];
	static const util::NameCoder<
			CheckpointMode, CP_MODE_END> CP_MODE_CODER;
};

/*!
	@brief チェックポイントサービスのイベントメッセージ
*/
class CheckpointMainMessage {
public:
	CheckpointMainMessage();
	CheckpointMainMessage(
			int32_t mode, uint32_t flag, const std::string &path, int64_t ssn);

	static CheckpointMainMessage decode(EventByteInStream &in);
	void encode(EventByteOutStream &out);

	int32_t mode() { return mode_; }
	uint32_t flag() { return flag_; }
	const std::string& backupPath() { return backupPath_; }
	int64_t ssn() { return ssn_; }

	bool isBackupMode();
	int32_t getIncrementalBackupLevel();
private:
	int32_t mode_;
	uint32_t flag_;
	std::string backupPath_;
	int64_t ssn_;
};

/*!
	@brief チェックポイントサービスの統計情報
*/
struct CheckpointStats  {
	enum Param {
		CP_STAT_START_TIME,
		CP_STAT_END_TIME,
		CP_STAT_MODE,
		CP_STAT_PENDING_PARTITION_COUNT,
		CP_STAT_NORMAL_OPERATION,
		CP_STAT_REQUESTED_OPERATION,
		CP_STAT_BACKUP_OPERATION,
		CP_STAT_PERIODIC,

		CP_STAT_END
	};

	typedef LocalStatTable<Param, CP_STAT_END> Table;
	typedef Table::Mapper Mapper;

	class SetUpHandler;
	class Updator;

	explicit CheckpointStats(const Mapper *mapper);

	static void setUpMapper(Mapper &mapper);

	Table table_;
};

class CheckpointStats::SetUpHandler : public StatTable::SetUpHandler {
	virtual void operator()(StatTable &stat);
	static SetUpHandler instance_;
};

class CheckpointStats::Updator : public StatTable::StatUpdator {
public:
	explicit Updator(const util::StdAllocator<void, void> &alloc);
	virtual bool operator()(StatTable &stat);

	Mapper mapper_;
	CheckpointStats stats_;

public:
	CheckpointService* cpSvc_;

};

/*!
	@brief チェックポイント操作の状態保持
	@note
		チェックポイント対象は、基本的にパーティション単位で独立している。
		一方、パーティションごとに直列にチェックポイント操作を進めると、
		特にファイルフラッシュの待ち時間が無視できなくなる(特にXFS上、
		多パーティションの場合)。

		そこで、チェックポイント操作の途中フェーズ駆動とし、対象の
		全パーティションが所定のフェーズに進むまで、次のフェーズの処理は
		中断する。これにより、フラッシュ処理がまとまり、待ち時間が短くなる。

		このクラスは、フェーズ間で共有する必要のある状態を保持する。
		パーティションごとの状態エントリの列より構成される。
*/
class CheckpointOperationState {
public:
	class Entry;

	CheckpointOperationState();
	~CheckpointOperationState();

	void assign(uint32_t partitionCount);
	void clear();

	Entry& getEntry(PartitionId pId);

	CheckpointPhase getPhase();
	void nextPhase(CheckpointPhase phase);

private:
	CheckpointOperationState(const CheckpointOperationState&);
	CheckpointOperationState& operator=(const CheckpointOperationState&);

	CheckpointPhase phase_;
	std::vector<Entry*> entryList_;
};

/*!
	@brief チェックポイント操作の状態エントリ
	@note 各パーティションと対応する
*/
class CheckpointOperationState::Entry {
public:
	Entry();

	util::LocalUniquePtr<PartitionLock>& getLock();
	UtilBitmap& getBitmap();
	int64_t getBlockCount();
	void setBlockCount(int64_t count);

	bool resolveCheckpointReady(bool current);
	bool resolveNeedCheckpoint(bool current);

private:
	typedef std::pair<bool, bool> OptionalBoolValue;

	Entry(const Entry&);
	Entry& operator=(const Entry&);

	static bool resolveInitialValue(OptionalBoolValue &value, bool current);

	util::LocalUniquePtr<PartitionLock> lock_;
	UtilBitmap bitmap_;
	int64_t blockCount_;

	OptionalBoolValue checkpointReady_;
	OptionalBoolValue needCheckpoint_;
};

template <class L> class LogManager;
class RecoveryManager;
class TransactionManager;
class TransactionService;
class CheckpointManager;

/*!
	@brief チェックポイントサービス用ハンドラの基底クラス
*/
class CheckpointHandler : public EventHandler {
public:
	CheckpointHandler()
	: transactionService_(NULL), transactionManager_(NULL),
		checkpointService_(NULL), clusterService_(NULL),
		partitionList_(NULL), fixedSizeAlloc_(NULL), config_(NULL) {};
	~CheckpointHandler(){};

	void initialize(const ManagerSet &resourceSet);

	TransactionService *transactionService_;
	TransactionManager *transactionManager_;
	CheckpointService *checkpointService_;
	ClusterService *clusterService_;

	PartitionList* partitionList_;
	GlobalFixedSizeAllocator *fixedSizeAlloc_;
	ConfigTable *config_;

private:
};


/*!
	@brief チェックポイント処理ハンドラ
*/
class CheckpointServiceMainHandler : public CheckpointHandler {
public:
	CheckpointServiceMainHandler(){};
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief 定周期ログフラッシュハンドラ
*/
class FlushLogPeriodicallyHandler : public CheckpointHandler {
public:
	FlushLogPeriodicallyHandler(){};
	void operator()(EventContext &ec, Event &ev);
};

class VirtualFileBase;
class LongtermSyncInfo; 
struct CheckpointStats;
/*!
	@brief チェックポイントサービス
*/
class CheckpointService {
public:
	friend class CheckpointServiceMainHandler;

	static const std::string PID_LSN_INFO_FILE_NAME;
	static const std::string AUTO_ARCHIVE_COMMAND_INFO_FILE_NAME;

	static const uint32_t BACKUP_ARCHIVE_LOG_MODE_FLAG = 1;
	static const uint32_t BACKUP_DUPLICATE_LOG_MODE_FLAG = 1 << 1;
	static const uint32_t BACKUP_STOP_ON_DUPLICATE_ERROR_FLAG = 1 << 2;
	static const uint32_t BACKUP_SKIP_BASELINE_MODE_FLAG = 1 << 3;
	static const uint64_t INCREMENTAL_BACKUP_COUNT_LIMIT = 999;

	static const char *const INCREMENTAL_BACKUP_FILE_SUFFIX;
	static const uint32_t MAX_BACKUP_NAME_LEN;

	static const char *const SYNC_TEMP_FILE_SUFFIX; 

	typedef int64_t SyncSequentialNumber; 

	static const SyncSequentialNumber UNDEF_SYNC_SEQ_NUMBER; 
	static const SyncSequentialNumber MAX_SYNC_SEQ_NUMBER; 

	static const uint32_t RELEASE_BLOCK_THRESHOLD_MILLIS = 1000; 
	/*!
		@brief CP途中失敗時の後始末
	*/
	class CheckpointDataCleaner;
	/*!
		@brief Types of events
	*/
	enum EventType {
		CP_REQUEST_CHECKPOINT,
		CP_TIMER_LOG_FLUSH,
	};

	/*!
		@brief Status of backup
	*/
	struct BackupStatus {
		BackupStatus()
			: name_(""),
			  cumulativeCount_(0),
			  differentialCount_(0),
			  incrementalLevel_(-1),
			  archiveLogMode_(false),
			  skipBaselineMode_(false),  
			  duplicateLogMode_(false){};

		void reset() {
			name_.clear();
			cumulativeCount_ = 0;
			differentialCount_ = 0;
			incrementalLevel_ = 0;
			archiveLogMode_ = false;
			skipBaselineMode_ = false;  
			duplicateLogMode_ = false;
		}

		u8string name_;
		uint64_t cumulativeCount_;
		uint64_t differentialCount_;
		int32_t incrementalLevel_;
		bool archiveLogMode_;
		bool skipBaselineMode_;  
		bool duplicateLogMode_;
	};
	typedef std::map<int64_t, int64_t> CpLongtermSyncOffsetMap;
	/*!
		@brief 長期同期管理情報
	*/
	struct CpLongtermSyncInfo {
		CpLongtermSyncInfo()
			: ssn_(UNDEF_SYNC_SEQ_NUMBER),
			  targetPId_(UNDEF_PARTITIONID),
			  syncCpFile_(NULL),
			  startLsn_(0),
			  logVersion_(0),
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
		VirtualFileBase* syncCpFile_;
		u8string dir_; 
		LogSequentialNumber startLsn_;
		int64_t logVersion_;
		CpLongtermSyncOffsetMap::iterator readItr_;
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

	/*
		@brief コンストラクタ
	*/
	CheckpointService(
			const ConfigTable &config, EventEngine::Config eeConfig,
			EventEngine::Source source, const char8_t *name,
			ServiceThreadErrorHandler &serviceThreadErrorHandler);

	/*
		@brief デストラクタ
	*/
	~CheckpointService();

	void initialize(ManagerSet &resourceSet);

	void start(const Event::Source &eventSource);

	void shutdown();

	void waitForShutdown();

	EventEngine *getEE();

	void executeRecoveryCheckpoint(const Event::Source &eventSource);

	void requestNormalCheckpoint(const Event::Source &eventSource);

	bool getLongSyncCheckpointStartLog(
			SyncSequentialNumber ssn, LogIterator<MutexLocker> &itr);

	bool isEntry(SyncSequentialNumber ssn);

	bool getLongSyncChunk(
			SyncSequentialNumber ssn, uint64_t size, uint8_t* buffer,
			uint64_t &resultSize, uint64_t &totalCount,
			uint64_t &completedCount, uint64_t &readyCount);

	CpLongtermSyncInfo* getCpLongtermSyncInfo(SyncSequentialNumber id);

	bool setCpLongtermSyncInfo(
		SyncSequentialNumber id, const CpLongtermSyncInfo &cpLongtermSyncInfo);

	bool updateCpLongtermSyncInfo(
		SyncSequentialNumber id, const CpLongtermSyncInfo &cpLongtermSyncInfo);

	void removeCpLongtermSyncInfoAll();

	SyncSequentialNumber getCurrentSyncSequentialNumber(PartitionId pId);

	bool removeCpLongtermSyncInfo(SyncSequentialNumber id);

	bool checkLongtermSyncIsReady(SyncSequentialNumber ssn);

	bool requestOnlineBackup(
			const Event::Source &eventSource,
			const std::string &backupName, int32_t mode,
			const SystemService::BackupOption &option,
			picojson::value &result);

	void requestCleanupLogFiles(const Event::Source &eventSource);

	void requestShutdownCheckpoint(const Event::Source &eventSource);

	CheckpointId getLastCompletedCheckpointId(PartitionGroupId pgId) const;

	CheckpointId getCurrentCheckpointId(PartitionGroupId pgId) const;

	const PartitionGroupConfig &getPGConfig() const {
		return pgConfig_;
	}

	static const char8_t *checkpointModeToString(int32_t mode);

	int64_t getLastDuration(const util::DateTime &now) const;

	/*!
		@brief Gets transactionEE queue size
	*/
	int32_t getTransactionEEQueueSize(PartitionGroupId pgId);

	void requestStartCheckpointForLongtermSync(
		const Event::Source& eventSource,
		PartitionId pId, LongtermSyncInfo* longtermSynInfo);


	void requestStartCheckpointForLongtermSyncPre(
			const Event::Source &eventSource,
			PartitionId pId, LongtermSyncInfo *longtermSynInfo);

	void requestStartCheckpointForLongtermSyncPost(
		const Event::Source& eventSource,
		PartitionId pId, int64_t syncSequentialNumbero);

	void requestSyncCheckpoint(
		EventContext& ec, PartitionId pId, LongtermSyncInfo* longtermSyncInfo);

	void requestStopCheckpointForLongtermSync(
			const Event::Source &eventSource,
			PartitionId pId, int64_t syncSequentialNumber);

	void setPeriodicCheckpointFlag(bool flag);
	bool getPeriodicCheckpointFlag();

	uint16_t getNewestLogFormatVersion();
	void setNewestLogFormatVersion(uint16_t version);

	int32_t getChunkCopyInterval() {
		return chunkCopyIntervalMillis_;
	}

	int32_t getChunkCopyLimitQueueSize() {
		return CP_CHUNK_COPY_WITH_SLEEP_LIMIT_QUEUE_SIZE;
	}

	inline int32_t getCheckpointInterval() const {
		return cpInterval_;
	}

	inline RecoveryManager::BackupInfo& backupInfo() {
		return backupInfo_;
	}

	inline void setErrorOccured(bool flag) {
		errorOccured_ = flag;
	}

	void runCheckpoint(EventContext &ec, CheckpointMainMessage &message);

	void checkFailureBeforeWriteGroupLog(int32_t mode);
	void checkFailureAfterWriteGroupLog(int32_t mode);
	void checkFailureWhileWriteChunkMetaLog(int32_t mode);
	void checkFailureLongtermSyncCheckpoint(int32_t mode, int32_t point);
	
	struct BackupContext;

	void writeAutoArchiveRoleInfo(PartitionId pId, PartitionStatus status);
	void getAutoArchiveCommandParam(picojson::value &result);
	bool isAutoArchvice();
	const char* getArchiveName();

private:
	static const int32_t CP_CHUNK_COPY_WITH_SLEEP_LIMIT_QUEUE_SIZE = 40;

	struct AutoArchiveInfo {
		AutoArchiveInfo(const ConfigTable& config);
		bool enableAutoArchiveSetting_;
		bool enableAutoArchive_;
		const std::string autoArchiveName_;
		const std::string autoArchiveInfoClusterInfoPathName_;
		bool autoArchiveStopOnErrorFlag_;
		bool autoArchiveSkipBaseLineFlag_;
		bool autoArchiveDuplicateLogFlag_;
		bool autoArchiveClusterRoleOutput_;
		std::vector<int64_t> autoArchiveSequenceNoList_;
		bool isStopOnError() {
			return autoArchiveStopOnErrorFlag_;
		}
	};

	struct AutoArchiveCommand {
		AutoArchiveCommand() : stopOnDuplicateError_(false), skipBaseLine_(false), duplicateLog_(false) {}
		AutoArchiveCommand(const std::string& archiveName, bool stopOnDuplicateError, bool skipBaseLine, bool duplicateLog) :
			archiveName_(archiveName), stopOnDuplicateError_(stopOnDuplicateError), skipBaseLine_(skipBaseLine), duplicateLog_(duplicateLog){}
		std::string archiveName_;
		bool stopOnDuplicateError_;
		bool skipBaseLine_;
		bool duplicateLog_;
		UTIL_OBJECT_CODER_MEMBERS(archiveName_, stopOnDuplicateError_, skipBaseLine_, duplicateLog_);
	};

	enum AutoArchiveOutputType {
		OUTPUT_RECOVERY,
		OUTPUT_ROLE,
		OUTPUT_CHECKPOINT,
		OUTPUT_SUMMARY,
		OUTPUT_COMMAND
	};

	struct AutoArchiveOutputOption {
		AutoArchiveOutputOption() : stopOnDuplicateError_(false), skipBaseLine_(false), duplicateLog_(true), roleStatus_(PartitionTable::PT_NONE), throwException_(false) {}
		bool stopOnDuplicateError_;
		bool skipBaseLine_;
		bool duplicateLog_;
		PartitionRoleStatus roleStatus_;
		bool throwException_;
	};

	bool isAutoArchiveDuplidateLog();
	void checkPrevAutoArchiveInfo();
	void resetAutoArchiveInfo(PartitionId pId);
	void setAutoArchiveInfo(PartitionId pId, const CheckpointPhase phase);
	bool checkAutoArchiveCommandInfo(BackupContext& cxt);
	void writeAutoArchiveCommandInfo(CheckpointMainMessage& message);
	void writeAutoArchiveInfoFile(const std::string& pathName, const std::string &fileName, const std::string& data);
	bool writeAutoArchiveInfo(PartitionId pId, AutoArchiveOutputType type, AutoArchiveOutputOption& option);
	void handleAutoArchiveError(std::exception& e, const char* str);

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

		void endCheckpoint(const char8_t *backupPath);

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

	void partitionCheckpoint(
			PartitionId pId, EventContext &ec, CheckpointMainMessage &message,
			CheckpointOperationState &state);

	bool checkCPrunnable(int32_t mode); 

	void cleanSyncTempDir();

	bool prepareBackupDirectory(CheckpointMainMessage &message);

	void traceCheckpointStart(CheckpointMainMessage &message);
	void traceCheckpointEnd(CheckpointMainMessage &message);

	void prepareAllCheckpoint(CheckpointMainMessage &message);
	void completeAllCheckpoint(CheckpointMainMessage &message, bool& backupCommandWorking);

	bool needCollectOffset(int32_t mode);

	void stopLongtermSync(
			PartitionId pId, EventContext &ec, CheckpointMainMessage &message);

	void notifyCheckpointEnd(EventContext &ec, CheckpointMainMessage &message);
	void handleError(EventContext &ec, CheckpointMainMessage &message);

	void checkSystemError();
	public:
	void cancelSyncCheckpoint(EventContext& ec, PartitionId pId, int64_t ssn);

	struct BackupContext {
		const std::string &backupName_;
		int32_t &mode_;
		const SystemService::BackupOption &option_;
		picojson::value &result_;
		int32_t &errorNo_;
		std::string &reason_;
		picojson::object &errorInfo_;
		std::string backupPath_;
		std::string origBackupPath_;

		BackupContext(
				const std::string &backupName, int32_t &mode,
				const SystemService::BackupOption &option,
				picojson::value &result,
				int32_t &errorNo, std::string &reason,
				picojson::object &errorInfo)
		: backupName_(backupName), mode_(mode), option_(option),
		  result_(result), errorNo_(errorNo), reason_(reason),
		  errorInfo_(errorInfo)
		  {}
	};

	bool checkBackupRunnable(BackupContext &cxt);

	bool checkBackupParams(BackupContext &cxt);

	bool checkBackupCondition(BackupContext &cxt);

	bool prepareBackupDir(BackupContext &cxt);

	bool prepareExecuteBackup(BackupContext &cxt);

	uint64_t backupLog(
			uint32_t flag, PartitionId pId, const std::string &backupPath);

	bool makeBackupDirectory(
			int32_t mode, bool makeSubDir,
			const std::string &backupName);

	void setLastMode(int32_t mode);

	LocalStatValue& stats(CheckpointStats::Param param);
	const CheckpointStats::Table& stats() const;
	CheckpointStats::Table& stats();

	static class ConfigSetUpHandler : public ConfigTable::SetUpHandler {
		virtual void operator()(ConfigTable &config);
	} configSetUpHandler_;

	CheckpointServiceMainHandler checkpointServiceMainHandler_;
	FlushLogPeriodicallyHandler flushLogPeriodicallyHandler_;

	ServiceThreadErrorHandler serviceThreadErrorHandler_;

	EventEngine ee_;
	const ConfigTable& configTable_;
	ClusterService *clusterService_;
	EventEngine *clsEE_;
	TransactionService *transactionService_;
	TransactionManager *transactionManager_;
	EventEngine *txnEE_;
	SystemService *systemService_;
	PartitionList* partitionList_;
	PartitionTable *partitionTable_;
	bool initialized_;

	SyncService *syncService_;

	GlobalFixedSizeAllocator *fixedSizeAlloc_;
	util::VariableSizeAllocator<> varAlloc_;

	util::Atomic<bool> requestedShutdownCheckpoint_;

	const PartitionGroupConfig pgConfig_;

	const int32_t cpInterval_;	
	const int32_t logWriteMode_;  
	const std::string backupTopPath_;  
	util::Mutex cpLongtermSyncMutex_;
	const std::string syncTempTopPath_;  

	typedef std::map<SyncSequentialNumber, CpLongtermSyncInfo> CpLongtermSyncInfoMap;
	CpLongtermSyncInfoMap cpLongtermSyncInfoMap_;
	std::vector<SyncSequentialNumber> ssnList_; 

	util::Atomic<int32_t>
		chunkCopyIntervalMillis_;  

	volatile bool backupEndPending_;

	std::string lastBackupPath_;
	bool currentDuplicateLogMode_;

	PartitionId currentCpPId_;

	RecoveryManager::BackupInfo backupInfo_;

	bool parallelCheckpoint_;
	util::Atomic<bool> errorOccured_;

	PIdLsnInfo lsnInfo_;
	bool enableLsnInfoFile_;

	std::vector<LogSequentialNumber> lastCpStartLsnList_;

	util::NormalXArray<uint8_t> longtermSyncReadBuffer_;
	util::NormalXArray<uint8_t> longtermSyncWriteBuffer_;


	CheckpointStats::Updator statUpdator_;

	util::Atomic<int32_t> lastMode_;
	util::Atomic<int64_t> startTime_;
	util::Atomic<int64_t> endTime_;
	util::Atomic<int32_t> pendingPartitionCount_;
	util::Atomic<int64_t> totalNormalCpOperation_;
	util::Atomic<int64_t> totalRequestedCpOperation_;
	util::Atomic<int64_t> totalBackupOperation_;

	util::Atomic<int32_t> archiveLogMode_;

	BackupStatus lastQueuedBackupStatus_;
	BackupStatus lastCompletedBackupStatus_;
	util::Atomic<bool> enablePeriodicCheckpoint_;
	util::Atomic<int32_t> newestLogFormatVersion_;
	AutoArchiveInfo autoArchiveInfo_;

};

/*!
	@brief CP途中失敗時の後始末処理
 */
class CheckpointService::CheckpointDataCleaner {
public:
	CheckpointDataCleaner(
			CheckpointService &service, EventContext &ec,
			const bool &backupCommandWorking, const std::string &backupPath);
	~CheckpointDataCleaner();

private:
	CheckpointService &service_;
	EventContext &ec_;
	const bool &backupCommandWorking_;
	const std::string backupPath_;
	uint32_t workerId_;
};

inline uint16_t CheckpointService::getNewestLogFormatVersion() {
	return static_cast<uint16_t>(newestLogFormatVersion_);
}

inline void CheckpointService::setNewestLogFormatVersion(uint16_t version) {
	newestLogFormatVersion_ = static_cast<int32_t>(version);
}


#endif  
