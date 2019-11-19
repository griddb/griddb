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
	@brief Definition of RecoveryManager
*/
#ifndef RECOVERY_MANAGER_H_
#define RECOVERY_MANAGER_H_
#include "util/allocator.h"
#include "util/container.h"
#include "config_table.h"
#include "data_store_common.h"
#include "event_engine.h"
#include "sha2.h"
#include "container_key.h"

struct LogRecord;
class LogCursor;
class TransactionContext;
class ConfigTable;
struct ManagerSet;
class PartitionTable;
class ClusterService;
class SystemService;
class DataStore;
class LogManager;
class ChunkManager;
class TransactionManager;
class BaseContainer;

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
	@brief Recovers database.
*/
class RecoveryManager {
public:
	static const std::string BACKUP_INFO_FILE_NAME;
	static const std::string BACKUP_INFO_DIGEST_FILE_NAME;
	static const uint32_t BACKUP_INFO_FILE_VERSION = 0x1;  

	static const ChunkCategoryId CHUNK_CATEGORY_NON_READY_PARTITION =
		UNDEF_CHUNK_CATEGORY_ID;  

	RecoveryManager(
			ConfigTable &configTable, bool releaseUnusedFileBlocks);
	~RecoveryManager();

	void initialize(ManagerSet &mgrSet);

	static bool checkBackupInfoFileName(const std::string &name);

	static bool checkBackupInfoDigestFileName(const std::string &name);

	static void checkExistingFiles1(ConfigTable &configTable, bool &createFrag,
		bool &existBackupFile, bool forceRecoveryFromExistingFiles);

	static void checkExistingDbDir(
			const char8_t *const, uint32_t splitCount,
			ConfigTable &param, bool &createFlag, bool &createNew);

	static void scanExistingFiles(
			ConfigTable &param, bool &createFlag,
			bool &existBackupInfoFile, bool forceRecoveryFromExistingFiles,
			std::vector<std::vector<CheckpointId> > &logFileInfoList,
			std::vector< std::set<int32_t> > &cpFileInfoList,
			bool &existBackupInfoFile1, bool &existBackupInfoFile2);

	static void checkExistingFiles2(ConfigTable &configTable,
		LogManager &logMgr, bool &createFrag, bool existBackupFile,
		bool forceRecoveryFromExistingFiles);

	bool readBackupInfoFile();
	bool readBackupInfoFile(const std::string &path);

	void removeBackupInfoFile();




	void recovery(util::StackAllocator &alloc, bool existBackup,
		bool forceRecoveryFromExistingFiles,
		const std::string &recoveryTargetPartition, bool forLongArchive);

	size_t undo(util::StackAllocator &alloc, PartitionId pId);

	/*!
		@brief Loggin modes of replication and synchronization and recovery.
	*/
	enum Mode {
		MODE_REPLICATION,	  
		MODE_SHORT_TERM_SYNC,  
		MODE_LONG_TERM_SYNC,   
		MODE_RECOVERY,		   
		MODE_RECOVERY_CHUNK	
	};

	void redoLogList(util::StackAllocator &alloc, Mode mode, PartitionId pId,
		const util::DateTime &redoStartTime, EventMonotonicTime redoStartEmTime,
		const uint8_t *binaryLogRecords, size_t size);

	LogSequentialNumber restoreTransaction(
		PartitionId pId, const uint8_t *buffer, EventMonotonicTime emNow);

	void dropPartition(TransactionContext &txn, util::StackAllocator &alloc,
		PartitionId pId, bool putLog = true);
	void dumpCheckpointFile(
		util::StackAllocator &alloc, const char8_t *dirPath);

	void dumpLogFile(
		util::StackAllocator &alloc, PartitionGroupId pgId, CheckpointId cpId);

	void dumpFileVersion(util::StackAllocator &alloc, const char8_t *dbPath);

	/*!
		@brief Returns recovery progress ratio.
	*/
	static double getProgressRate() {
		return (progressPercent_ / 100.0);
	}

	/*!
		@brief Sets progress ratio.
	*/
	static void setProgressPercent(uint32_t percent) {
		progressPercent_ = percent;
	}

	/*!
		@brief Manages information for backup.
	*/
	class BackupInfo {
	public:
		enum IncrementalBackupMode {
			INCREMENTAL_BACKUP_NONE = 0,  
			INCREMENTAL_BACKUP_LV0,		  
			INCREMENTAL_BACKUP_LV1_CUMULATIVE,   
			INCREMENTAL_BACKUP_LV1_DIFFERENTIAL  
		};

		BackupInfo();
		~BackupInfo();

		void setConfigValue(uint32_t partitionNum, uint32_t partitionGroupNum);

		void startBackup(const std::string &backupPath);  
		void startIncrementalBackup(const std::string &backupPath,
			const std::string &lastBackupPath, int32_t mode);  

		void
		endBackup();  

		void errorBackup();  

		void startCpFileCopy(PartitionGroupId pgId, CheckpointId cpId);
		void endCpFileCopy(PartitionGroupId pgId, uint64_t fileSize);
		void startIncrementalBlockCopy(PartitionGroupId pgId, int32_t mode);
		void endIncrementalBlockCopy(PartitionGroupId pgId, CheckpointId cpId);
		void startLogFileCopy();
		void endLogFileCopy(PartitionGroupId pgId, uint64_t fileSize);

		void readBackupInfoFile();
		void readBackupInfoFile(const std::string &path);

		void removeBackupInfoFile(LogManager &logManager);
		void removeUnnecessaryLogFiles(std::vector<CheckpointId> &lastCpIdList);

		void setBackupPath(const std::string &backupPath);  
		void setDataStorePath(const std::string &dataStorePath);  
		void setGSVersion(const std::string &gsVersion);  

		CheckpointId getBackupCheckpointId(PartitionGroupId pgId);
		uint64_t getBackupCheckpointFileSize(PartitionGroupId pgId);
		uint64_t getBackupLogFileSize(PartitionGroupId pgId);

		IncrementalBackupMode getIncrementalBackupMode();
		void setIncrementalBackupMode(IncrementalBackupMode mode);
		bool isIncrementalBackup();
		std::vector<CheckpointId> &getIncrementalBackupCpIdList(
			PartitionGroupId pgId);
		bool getCheckCpFileSizeFlag();
		void setCheckCpFileSizeFlag(bool flag);

	private:
		void reset();

		void writeBackupInfoFile();

		uint32_t partitionNum_;
		uint32_t partitionGroupNum_;

		uint32_t currentPartitionGroupId_;

		std::string backupPath_;
		std::string dataStorePath_;
		std::string status_;
		std::string gsVersion_;

		int64_t backupStartTime_;
		int64_t backupEndTime_;
		int64_t cpFileCopyStartTime_;
		int64_t cpFileCopyEndTime_;
		int64_t logFileCopyStartTime_;
		int64_t logFileCopyEndTime_;

		std::vector<CheckpointId> cpIdList_;

		std::vector<uint64_t> cpFileSizeList_;
		std::vector<uint64_t> logFileSizeList_;

		std::vector<double> cpFileCopyTimeList_;
		std::vector<double> logFileCopyTimeList_;

		std::vector<std::vector<CheckpointId> > incrementalBackupCpIdList_;
		IncrementalBackupMode incrementalBackupMode_;
		bool checkCpFileSizeFlag_;

		char digestData_[SHA256_DIGEST_STRING_LENGTH + 1];
	};

private:
	void redoAll(util::StackAllocator &alloc, Mode mode, LogCursor &cursor,
		bool isIncrementalBackup, bool forLongArchive);

	void redoChunkLog(util::StackAllocator &alloc, PartitionGroupId pgId,
			Mode mode, CheckpointId recoveryCpId);

	void recoveryChunkMetaData(
			util::StackAllocator &alloc, const LogRecord &logRecord);

	LogSequentialNumber redoLogRecord(util::StackAllocator &alloc, Mode mode,
		PartitionId pId, const util::DateTime &stmtStartTime,
		EventMonotonicTime stmtStartEmTime, const LogRecord &logRecord,
		LogSequentialNumber &redoStartLsn, bool isIncrementalBackup);

	LogSequentialNumber redoLogRecordForDataArchive(util::StackAllocator &alloc,
		Mode mode, PartitionId pId, const util::DateTime &stmtStartTime,
		EventMonotonicTime stmtStartEmTime, const LogRecord &logRecord,
		LogSequentialNumber &redoStartLsn, bool isIncrementalBackup);

	void redoCheckpointStartLog(util::StackAllocator &alloc,
		Mode mode, PartitionId pId, const util::DateTime &stmtStartTime,
		EventMonotonicTime stmtStartEmTime, const LogRecord &logRecord,
		LogSequentialNumber &redoStartLsn, bool isIncrementalBackup,
		bool forLongArchive);

	void redoChunkMetaDataLog(util::StackAllocator &alloc,
		Mode mode, PartitionId pId, const util::DateTime &stmtStartTime,
		EventMonotonicTime stmtStartEmTime, const LogRecord &logRecord,
		LogSequentialNumber &redoStartLsn, bool isIncrementalBackup,
		bool forLongArchive);

	bool redoChunkLogRecord(util::StackAllocator &alloc, Mode mode,
		PartitionId pId, const LogRecord &logRecord,
		LogSequentialNumber &redoStartLsn);

	void removeTransaction(Mode mode, TransactionContext &txn);

	void parseRecoveryTargetPartition(const std::string &str);

	void reconstructValidBitArray(
			util::StackAllocator &alloc, Mode mode,
			LogCursor &cursor, BitArray &validBitArray);

	void checkContainerExistence(
		BaseContainer *container, ContainerId containerId);
	void checkContainerExistence(
		BaseContainer *container, const util::String &containerName);

	static void logRecordToString(
		util::StackAllocator &alloc, const KeyConstraint &containerKeyConstraint,
		PartitionId pId, const LogRecord &logRecord,
		std::string &dumpString, LogSequentialNumber &lsn);

	/*!
		@brief Handler to set up Stat.
	*/
	static class StatSetUpHandler : public StatTable::SetUpHandler {
		virtual void operator()(StatTable &stat);
	} statSetUpHandler_;

	/*!
		@brief Updates Stat.
	*/
	class StatUpdator : public StatTable::StatUpdator {
		virtual bool operator()(StatTable &stat);

	public:
		RecoveryManager *manager_;
	} statUpdator_;

	static const uint64_t ADJUST_MEMORY_TIME_INTERVAL_ = 30 * 1000 * 1000;

	PartitionGroupConfig pgConfig_;
	const uint8_t CHUNK_EXP_SIZE_;
	const int32_t CHUNK_SIZE_;

	bool recoveryOnlyCheckChunk_;	
	std::string recoveryDownPoint_;  
	int32_t recoveryDownCount_;		 

	BackupInfo backupInfo_;

	static util::Atomic<uint32_t> progressPercent_;  

	std::vector<PartitionId> recoveryPartitionId_;
	bool recoveryTargetPartitionMode_;

	bool releaseUnusedFileBlocks_;

	std::vector<PartitionId> syncChunkPId_;
	std::vector<int64_t> syncChunkNum_;
	std::vector<int64_t> syncChunkCount_;

	ChunkManager *chunkMgr_;
	LogManager *logMgr_;
	PartitionTable *pt_;
	ClusterService *clsSvc_;
	SystemService *sysSvc_;
	TransactionManager *txnMgr_;
	DataStore *ds_;
};

#endif
