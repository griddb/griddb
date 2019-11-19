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
    @brief Definition of LogManager
*/
#ifndef LOG_MANAGER_H_
#define LOG_MANAGER_H_

#include "data_store_common.h"
#include "transaction_context.h"
#include "config_table.h"
#include "bit_array.h"
#include "util/file.h"
#include "util/container.h"

class PartitionTable;

class LogCursor;
struct LogRecord;
struct ChunkTableInfo;

/*!
    @brief Writes and reads logs.
*/
class LogManager {
public:

	struct Config;

	static const int32_t LOGMGR_VARINT_MAX_LEN = 9;

	static const char *const LOG_FILE_BASE_NAME;
	static const char *const LOG_FILE_SEPARATOR;
	static const char *const LOG_FILE_EXTENSION;

	/*!
		@brief Log types of operations
	*/
	enum LogType { 
		LOG_TYPE_UNDEF = 0,

		LOG_TYPE_CHECKPOINT_START,		/*!< start of checkpoint */

		LOG_TYPE_COMMIT,				/*!< commit transaction */
		LOG_TYPE_ABORT,					/*!< abort transaction */

		LOG_TYPE_CREATE_PARTITION,		/*!< obsolete log type, not supported for version 2.0 or lator */
		LOG_TYPE_DROP_PARTITION,		/*!< drop partition */
		LOG_TYPE_PUT_CONTAINER,			/*!< put Container */
		LOG_TYPE_DROP_CONTAINER,		/*!< drop Container */
		LOG_TYPE_CREATE_INDEX,			/*!< create index */
		LOG_TYPE_DROP_INDEX,			/*!< drop index */
		LOG_TYPE_CREATE_TRIGGER,		/*!< set trigger */
		LOG_TYPE_DROP_TRIGGER,			/*!< remove trigger */

		LOG_TYPE_PUT_ROW,				/*!< put Row(s) */
		LOG_TYPE_REMOVE_ROW,			/*!< remove Row(s) */
		LOG_TYPE_LOCK_ROW,				/*!< lock Row(s) */

		LOG_TYPE_CHECKPOINT_END,		/*!< end of checkpoint */
		LOG_TYPE_SHUTDOWN,				/*!< shutdown node */
		LOG_TYPE_CHUNK_META_DATA,		/*!< chunk metadata */

		LOG_TYPE_UPDATE_ROW,			/*!< udpate Row(s) */

		LOG_TYPE_PUT_CHUNK_START,		/*!< start of chunk data */
		LOG_TYPE_PUT_CHUNK_DATA,		/*!< chunk data */
		LOG_TYPE_PUT_CHUNK_END,			/*!< end of chunk data */

		LOG_TYPE_CONTINUE_CREATE_INDEX, /*!< continue to alter Container */
		LOG_TYPE_CONTINUE_ALTER_CONTAINER, /*!< continue to alter Container */

		LOG_TYPE_PARTITION_EXPIRATION_PUT_CONTAINER,	/*!< put Container with partition expiration*/

		LOG_TYPE_WITH_BEGIN = 0x80,		/*!< flags for transaction start */
		LOG_TYPE_IS_AUTO_COMMIT = 0x40,	/*!< flags for autocommit transaction */
		LOG_TYPE_CLEAR_FLAGS = 0x3f,	/*!< empty flag */

		LOG_TYPE_NOOP = 0x3e
	};

	enum ArchiveLogMode {
		ARCHIVE_LOG_DISABLE = 0,
		ARCHIVE_LOG_ENABLE,
		ARCHIVE_LOG_RUNNING,
		ARCHIVE_LOG_ERROR = -1
	};

	enum DuplicateLogMode {
		DUPLICATE_LOG_DISABLE = 0,
		DUPLICATE_LOG_ENABLE,
		DUPLICATE_LOG_ERROR = -1
	};

	/*!
		@brief Persistency mode of logs
	*/
	enum PersistencyMode {
		PERSISTENCY_NORMAL = 1,
		PERSISTENCY_KEEP_ALL_LOG = 2
	};

	uint64_t getFileFlushCount() const;
	uint64_t getFileFlushTime() const;

	explicit LogManager(const Config &config);	

	~LogManager();

	void open(
			bool checkOnly, bool forceRecoveryFromExistingFiles = false,
			bool isIncremental = false,
			PartitionGroupId cpLongSyncPgId = UNDEF_PARTITIONGROUPID);  

	void openPartial(PartitionGroupId pgId, CheckpointId cpId, const char8_t *suffix);

	void create(PartitionGroupId pgId, CheckpointId cpId, const char8_t *suffix);

	void close();

	CheckpointId getFirstCheckpointId(PartitionGroupId pgId) const;

	CheckpointId getLastCheckpointId(PartitionGroupId pgId) const;

	CheckpointId getLastCompletedCheckpointId(PartitionGroupId pgId) const;

	bool findLog(LogCursor &cursor, PartitionId pId, LogSequentialNumber lsn);

	void updateAvailableStartLsn(
			PartitionTable *pt, PartitionGroupId pgId,
			util::XArray<uint8_t> &binaryLogRecords, CheckpointId cpId);

	bool findCheckpointStartLog(
			LogCursor &cursor, PartitionGroupId pgId, CheckpointId cpId);

	bool findCheckpointEndLog(
			LogCursor &cursor, PartitionGroupId pgId, CheckpointId cpId);

	LogSequentialNumber getLSN(PartitionId pId) const;

	void setLSN(PartitionId pId, LogSequentialNumber lsn);

	void setAvailableStartLSN(PartitionId  pId, CheckpointId cpId);






	void putReplicationLog(
			PartitionId pId, LogSequentialNumber lsn,
			uint32_t len, uint8_t *logRecord);

	LogSequentialNumber putCheckpointStartLog(
			util::XArray<uint8_t> &binaryLogBuf,
			PartitionId pId,
			TransactionId maxTxnId,	LogSequentialNumber cpLsn,
			const util::XArray<ClientId> &clientIds,
			const util::XArray<TransactionId> &activeTxnIds,
			const util::XArray<ContainerId> &refContainerIds,
			const util::XArray<StatementId> &lastExecStmtIds,
			const util::XArray<int32_t> &txnTimeoutIntervalSec);

	LogSequentialNumber putCommitTransactionLog(
			util::XArray<uint8_t> &binaryLogBuf, 
			PartitionId pId, const ClientId &clientId,
			TransactionId txnId, ContainerId containerId,
			StatementId stmtId);

	LogSequentialNumber putAbortTransactionLog(
			util::XArray<uint8_t> &binaryLogBuf, 
			PartitionId pId, const ClientId &clientId,
			TransactionId txnId, ContainerId containerId,
			StatementId stmtId);

	LogSequentialNumber putDropPartitionLog(
			util::XArray<uint8_t> &binaryLogBuf,
			PartitionId pId) ;


	LogSequentialNumber putPutContainerLog(
			util::XArray<uint8_t> &binaryLogBuf,
			PartitionId pId, const ClientId &clientId, TransactionId txnId,
			ContainerId containerId, StatementId stmtId,
			uint32_t containerNameLen, const uint8_t *containerName,
			const util::XArray<uint8_t> &containerInfo,
			int32_t containerType,
			uint32_t extensionNameLen, const char *extensionName,
			int32_t txnTimeoutInterval,
			int32_t txnContextCreateMode, bool withBegin,
			bool isAutoCommit, RowId cursorRowId, bool isPartitionExpiration);

	LogSequentialNumber putDropContainerLog(
			util::XArray<uint8_t> &binaryLogBuf,
			PartitionId pId, ContainerId containerId,
			uint32_t containerNameLen, const uint8_t *containerName);

	LogSequentialNumber putCreateIndexLog(
			util::XArray<uint8_t> &binaryLogBuf,
			PartitionId pId, const ClientId &clientId, TransactionId txnId,
			ContainerId containerId, StatementId stmtId,
			const util::Vector<ColumnId> &columnIds,
			int32_t mapType, uint32_t indexNameLen, const char *indexName,
			uint32_t extensionNameLen, const char *extensionName,
			const util::XArray<uint32_t> &paramDataLen,
			const util::XArray<const uint8_t*> &paramData,
			int32_t txnTimeoutInterval, int32_t txnContextCreateMode,
			bool withBegin, bool isAutoCommit, RowId cursorRowId);

	LogSequentialNumber putContinueCreateIndexLog(
			util::XArray<uint8_t> &binaryLogBuf,
			PartitionId pId, const ClientId &clientId, TransactionId txnId,
			ContainerId containerId, StatementId stmtId,
			int32_t txnTimeoutInterval, int32_t txnContextCreateMode,
			bool withBegin, bool isAutoCommit, RowId cursor);

	LogSequentialNumber putContinueAlterContainerLog(
			util::XArray<uint8_t> &binaryLogBuf,
			PartitionId pId, const ClientId &clientId, TransactionId txnId,
			ContainerId containerId, StatementId stmtId,
			int32_t txnTimeoutInterval, int32_t txnContextCreateMode,
			bool withBegin, bool isAutoCommit, RowId cursorRowId);

	LogSequentialNumber putDropIndexLog(
			util::XArray<uint8_t> &binaryLogBuf,
			PartitionId pId, ContainerId containerId,
			const util::Vector<ColumnId> &columnIds, int32_t mapType,
			uint32_t indexNameLen, const char *indexName,
			uint32_t extensionNameLen, const char *extensionName,
			const util::XArray<uint32_t> &paramDataLen,
			const util::XArray<const uint8_t*> &paramData);

	LogSequentialNumber putCreateTriggerLog(
			util::XArray<uint8_t> &binaryLogBuf,
			PartitionId pId, ContainerId containerId,
			uint32_t nameLen, const char *name,
			const util::XArray<uint8_t> &triggerInfo);

	LogSequentialNumber putDropTriggerLog(
			util::XArray<uint8_t> &binaryLogBuf,
			PartitionId pId, ContainerId containerId,
			uint32_t nameLen, const char *name);

	LogSequentialNumber putPutRowLog(
			util::XArray<uint8_t> &binaryLogBuf,
			PartitionId pId, const ClientId &clientId,
			TransactionId txnId, ContainerId containerId,
			StatementId stmtId,
			uint64_t numRowId, const util::XArray<RowId> &rowIds,
			uint64_t numRow, const util::XArray<uint8_t> &rowData,
			int32_t txnTimeoutInterval, int32_t txnContextCreateMode,
			bool withBegin, bool isAutoCommit);

	LogSequentialNumber putUpdateRowLog(
			util::XArray<uint8_t> &binaryLogBuf,
			PartitionId pId, const ClientId &clientId,
			TransactionId txnId, ContainerId containerId,
			StatementId stmtId,
			uint64_t numRowId, const util::XArray<RowId> &rowIds,
			uint64_t numRow, const util::XArray<uint8_t> &rowData,
			int32_t txnTimeoutInterval, int32_t txnContextCreateMode,
			bool withBegin, bool isAutoCommit);

	LogSequentialNumber putRemoveRowLog(
			util::XArray<uint8_t> &binaryLogBuf,
			PartitionId pId, const ClientId &clientId,
			TransactionId txnId, ContainerId containerId,
			StatementId stmtId,
			uint64_t numRowId, const util::XArray<RowId> &rowIds,
			int32_t txnTimeoutInterval, int32_t txnContextCreateMode,
			bool withBegin, bool isAutoCommit);

	LogSequentialNumber putLockRowLog(
			util::XArray<uint8_t> &binaryLogBuf,
			PartitionId pId, const ClientId &clientId,
			TransactionId txnId, ContainerId containerId,
			StatementId stmtId,
			uint64_t numRowId, const util::XArray<RowId> &rowIds,
			int32_t txnTimeoutInterval, int32_t txnContextCreateMode,
			bool withBegin, bool isAutoCommit);

	LogSequentialNumber putCheckpointEndLog(
			util::XArray<uint8_t> &binaryLogBuf,
			PartitionId pId);

	LogSequentialNumber putChunkMetaDataLog(
			util::XArray<uint8_t> &binaryLogBuf,
			PartitionId pId, ChunkCategoryId categoryId,
			ChunkId startChunkId, int32_t chunkNum,
			int32_t validChunkNum, uint32_t expandedSize,
			const util::XArray<uint8_t> *chunkMetaDataList,
			bool sentinel);


	void prepareCheckpoint(PartitionGroupId pgId, CheckpointId cpId);

	void postCheckpoint(PartitionGroupId pgId);

	LogSequentialNumber putChunkStartLog(
			util::XArray<uint8_t> &binaryLogBuf,
			PartitionId pId, uint64_t putChunkId,
			uint64_t chunkNum);

	LogSequentialNumber putChunkDataLog(
			util::XArray<uint8_t> &binaryLogBuf,
			PartitionId pId, uint64_t putChunkId,
			uint32_t chunkSize,
			const uint8_t* chunkImage);


	LogSequentialNumber putChunkEndLog(
			util::XArray<uint8_t> &binaryLogBuf,
			PartitionId pId, uint64_t putChunkId);

	void writeBuffer(PartitionGroupId pgId);

	void flushFile(PartitionGroupId pgId, bool executeChildren = true);

	void addSyncLogManager(LogManager *logMgr, PartitionId filterPId);
	void removeSyncLogManager(LogManager *logMgr, PartitionId filterPId);

	static uint16_t getBaseVersion();
	static uint16_t getLatestVersion();

	static bool isAcceptableVersion(uint16_t version);
	void setLogVersion(PartitionId pId, uint16_t version);

	uint16_t getLogVersion(PartitionId pId);
	uint64_t copyLogFile(PartitionGroupId pgId, const char8_t *dirPath);

	static uint32_t parseLogHeader(const uint8_t *&addr, LogRecord &record);
	static uint32_t parseLogBody(const uint8_t *&addr, LogRecord &record);

	PartitionGroupId calcPartitionGroupId(PartitionId pId) const;

	PartitionId calcGroupTopPartitionId(PartitionGroupId pgId) const;

	uint64_t getLogSize(PartitionGroupId pgId);

	static bool isLsnAssignable(uint8_t logType);

	void setAutoCleanupLogFlag(bool flag);
	bool getAutoCleanupLogFlag();

	void setArchiveLogMode(ArchiveLogMode mode) {
		archiveLogMode_ = mode;
	};
	ArchiveLogMode getArchiveLogMode() {
		return archiveLogMode_;
	};

	static void setDuplicateLogMode(int32_t mode) {
		duplicateLogMode_ = mode;
	};
	static int32_t getDuplicateLogMode() {
		return duplicateLogMode_;
	};

	void setDuplicateLogPath(PartitionGroupId pgId, const char8_t *duplicatePath);
	void getDuplicateLogPath(PartitionGroupId pgId, std::string& duplicatePath);

	void setStopOnDuplicateErrorFlag(bool flag);

	void cleanupLogFiles(PartitionGroupId pgId, CheckpointId baseCpId);

	CheckpointId getLastArchivedCpId(PartitionGroupId pgId);

	bool isLongtermSyncLogAvailable() const;

	void setLongtermSyncLogError(const std::string &message);

	std::string getLongtermSyncLogErrorMessage() const;

	static uint16_t selectNewerVersion(uint16_t arg1, uint16_t arg2);
	static bool checkFileName(
			const std::string &name,
			PartitionGroupId &pgId, CheckpointId &cpId);


	uint16_t getFileVersion(PartitionGroupId pgId, CheckpointId cpId);

	static const char* logTypeToString(LogType type);

	Config& getConfig() { return config_; }

	/*!
		@brief Configuration of LogManaegr
	*/
	struct Config : public ConfigTable::ParamHandler {
		Config(ConfigTable &configTable);

		void setUpConfigHandler();
		virtual void operator()(
				ConfigTable::ParamId id, const ParamValue &value);

		uint32_t getBlockSize() const;
		uint32_t getBlockBufferCount() const;
		uint32_t getIndexBufferSize() const;

		ConfigTable *configTable_;
		PartitionGroupConfig pgConfig_;

		int32_t persistencyMode_;
		int32_t logWriteMode_;
		bool alwaysFlushOnTxnEnd_;

		std::string logDirectory_;
		uint32_t blockSizeBits_;
		uint32_t blockBufferCountBits_;
		uint32_t lockRetryCount_;
		uint32_t lockRetryIntervalMillis_;

		bool indexEnabled_;
		uint32_t indexBufferSizeBits_;

		bool primaryCheckpointAssumed_;
		bool emptyFileAppendable_;

		uint32_t ioWarningThresholdMillis_;

		uint64_t retainedFileCount_;
		uint32_t logFileClearCacheInterval_;
		bool lsnBaseFileRetention_;
	};

private:
	friend class LogCursor;
	friend class LogManagerFunctionTest;

	class ConflictionDetector;
	class ConflictionDetectorScope;

	struct ReferenceCounter;
	struct LogFileInfo;
	struct LogBlocksInfo;

	class LogFileLatch;
	class LogBlocksLatch;
	class LogFileIndex;

	struct PartitionInfo;
	class PartitionGroupManager;

	typedef uint64_t FileOffset;
	typedef std::pair<CheckpointId, uint64_t> BlockPosition;
	typedef util::FixedSizeAllocator<> BlockPool;

	static const uint8_t LOG_RECORD_PADDING_ELEMENT = 0xfc;

	static const char *const LOG_INDEX_FILE_BASE_NAME;
	static const char *const LOG_INDEX_FILE_SEPARATOR;
	static const char *const LOG_INDEX_FILE_EXTENSION;

	static const char *const LOG_DUMP_FILE_BASE_NAME;
	static const char *const LOG_DUMP_FILE_EXTENSION;

	LogManager(const LogManager&);
	LogManager& operator=(const LogManager&);

	template<typename Alloc>
	LogSequentialNumber serializeLogCommonPart(
			util::XArray<uint8_t, Alloc> &logBuf,
			uint8_t type, PartitionId pId, SessionId sessionId,
			TransactionId txnId, ContainerId containerId);
	template<typename Alloc>
	void putBlockTailNoopLog(
			util::XArray<uint8_t, Alloc> &logBuf, PartitionId pId);
	void putLog(
			PartitionId pId, LogSequentialNumber lsn,
			uint8_t *serializedLogRecord, size_t logRecordLen,
			bool needUpdateLength = true);
	void flushByCommit(PartitionId pId);

	LogSequentialNumber putPutOrUpdateRowLog(
			LogType logType,
			util::XArray<uint8_t> &binaryLogBuf,
			PartitionId pId, const ClientId &clientId, TransactionId txnId,
			ContainerId containerId, StatementId stmtId,
			uint64_t numRowId, const util::XArray<RowId> &rowIds,
			uint64_t numRow, const util::XArray<uint8_t> &rowData,
			int32_t txnTimeoutInterval, int32_t txnContextCreateMode,
			bool withBegin, bool isAutoCommit);

	static const char* maskDirectoryPath(const char *dirPath);

	static uint64_t getBlockIndex(const Config &config, uint64_t fileOffset);
	static uint32_t getBlockOffset(const Config &config, uint64_t fileOffset);
	static uint64_t getFileOffset(
			const Config &config, uint64_t blockIndex, uint32_t blockOffset = 0);
	static uint32_t getBlockRemaining(
			const Config &config, uint64_t fileOffset);

	static int32_t getLogVersionScore(uint16_t version);
	static class ConfigSetUpHandler : public ConfigTable::SetUpHandler {
		virtual void operator()(ConfigTable &config);
	} configSetUpHandler_;

	Config config_;

	std::vector<PartitionInfo> partitionInfoList_;
	std::vector<PartitionGroupManager*> pgManagerList_;

	ArchiveLogMode archiveLogMode_; 
	static int32_t duplicateLogMode_; 
	bool stopOnDuplicateErrorFlag_; 
	std::vector<LogManager*> childLogManagerList_; 
	PartitionId filterPId_; 
};



struct LogManager::ReferenceCounter {
public:
	ReferenceCounter();

	size_t getCount() const;

private:
	friend class PartitionGroupManager;
	friend class LogFileLatch;
	friend class LogBlocksLatch;

	size_t increment();
	size_t decrement();

	size_t count_;
};

/*!
	@brief Information of a Log file
*/
struct LogManager::LogFileInfo {
public:
	LogFileInfo();

	void open(
			const Config &config,
			PartitionGroupId pgId, CheckpointId cpId, bool expectExisting,
			bool checkOnly, const char8_t *suffix);
	void close();
	bool isClosed() const;

	static std::string createLogFilePath(
			const char8_t *dirPath, PartitionGroupId pgId, CheckpointId cpId,
			const char8_t *suffix = NULL);

	ReferenceCounter& getReferenceCounter();
	CheckpointId getCheckpointId() const;
	uint64_t getFileSize() const;
	void setAvailableFileSize(uint64_t size);
	bool isAvailableFileSizeUpdated() const;
	uint64_t getTotalBlockCount(bool ignoreIncompleteTail) const;

	uint32_t readBlock(void *buffer, uint64_t startIndex, uint32_t count);
	void writeBlock(const void *buffer, uint64_t startIndex, uint32_t count);

	void flush();
	static void clearCache(
			util::NamedFile &file, const Config &config,
			uint64_t flushCount, bool forceClearCache);
	static uint64_t flush(
			util::NamedFile &file, const Config &config, bool failOnClose,
			uint64_t flushCount, bool forceClearCache);
	util::NamedFile* getFileDirect() { return file_; }

	void setDuplicateLogFile(util::NamedFile* file) { file2_ = file; }

	void setDuplicateLogPath(const char8_t* duplicatePath);

	void setStopOnDuplicateErrorFlag(bool flag) { stopOnDuplicateErrorFlag_ = flag; }

	void createDuplicateLog(PartitionGroupId pgId, const char8_t *duplicatePath);

private:
	void lock();

	ReferenceCounter referenceCounter_;
	CheckpointId cpId_;
	util::NamedFile *file_;
	util::NamedFile *file2_; 
	uint64_t fileSize_;
	uint64_t availableFileSize_;
	const Config *config_;
	std::string duplicateLogPath_; 
	bool stopOnDuplicateErrorFlag_; 
	int32_t duplicateLogMode_; 
};

/*!
	@brief Information of a LogBlock for logging
*/
struct LogManager::LogBlocksInfo {
public:

	static const uint32_t LOGBLOCK_HEADER_SIZE = sizeof(uint32_t) * 16;

	static const uint16_t LOGBLOCK_BASE_VERSION;
	static const uint16_t LOGBLOCK_LATEST_VERSION;
	LogBlocksInfo();

	void initialize(const Config &config, BlockPool &blocksPool,
			uint64_t startIndex, bool multiple);
	void clear(BlockPool &blocksPool);
	bool isEmpty() const;

	ReferenceCounter& getReferenceCounter();

	uint32_t getBlockCount() const;
	uint64_t getStartIndex() const;
	uint64_t getFileEndOffset() const;
	bool prepareNextBodyOffset(uint64_t &totalOffset) const;

	uint32_t getBodyPart(
			uint8_t *buffer, uint64_t index, uint32_t offset, uint32_t size) const;
	uint32_t putBodyPart(
			const uint8_t *buffer, uint64_t index, uint32_t offset, uint32_t size);

	void fillBodyRemaining(uint64_t index, uint32_t offset, uint8_t value);

	uint32_t copyAllFromFile(LogFileInfo &fileInfo);
	void copyToFile(LogFileInfo &fileInfo, uint64_t index);

	void initializeBlockHeader(uint64_t index, uint16_t logVersion);

	uint64_t getLastCheckpointId(uint64_t index) const;
	void setLastCheckpointId(uint64_t index, uint64_t cpId);

	uint64_t getCheckpointFileInfoOffset(uint64_t index) const;
	void setCheckpointFileInfoOffset(uint64_t index, uint64_t offset);

	uint32_t getFirstRecordOffest(uint64_t index) const;
	void setFirstRecordOffest(uint64_t index, uint32_t offset);

	bool isNormalShutdownCompleted(uint64_t index) const;
	void setNormalShutdownCompleted(uint64_t index, bool completed);

	uint16_t getVersion() const;

	static bool isAcceptableVersion(uint16_t version);

	static bool isValidRecordOffset(const Config &config, uint64_t offset);

	const uint8_t* data() const;

private:
	static const uint32_t LOGBLOCK_CHECKSUM_OFFSET = 0;		
	static const uint32_t LOGBLOCK_VERSION_OFFSET =
			LOGBLOCK_CHECKSUM_OFFSET + sizeof(uint32_t);		
	static const uint32_t LOGBLOCK_MAGIC_OFFSET =
			LOGBLOCK_VERSION_OFFSET + sizeof(uint16_t);		
	static const uint32_t LOGBLOCK_LAST_CHECKPOINT_ID_OFFSET =
			LOGBLOCK_MAGIC_OFFSET + sizeof(uint16_t);		
	static const uint32_t LOGBLOCK_CHECKPOINT_FILE_INFO_POS_OFFSET =
			LOGBLOCK_LAST_CHECKPOINT_ID_OFFSET + sizeof(uint64_t);		
	static const uint32_t LOGBLOCK_FIRSTRECORD_OFFSET =
			LOGBLOCK_CHECKPOINT_FILE_INFO_POS_OFFSET + sizeof(uint64_t);		
	static const uint32_t LOGBLOCK_DUMMY_OFFSET =
			LOGBLOCK_FIRSTRECORD_OFFSET + sizeof(uint32_t);		
	static const uint32_t LOGBLOCK_LSN_INFO_OFFSET =
			LOGBLOCK_DUMMY_OFFSET + sizeof(uint32_t);		

	static const uint64_t LOGBLOCK_NORMAL_SHUTDOWN_FLAG =
			static_cast<uint64_t>(1) << 63;

	static const uint16_t LOGBLOCK_ACCEPTABLE_VERSIONS[];
	static const uint16_t LOGBLOCK_MAGIC_NUMBER = 0xAD69;

	void validate(uint64_t index) const;

	void updateChecksum(uint64_t index);
	uint32_t getChecksum(uint64_t index) const;

	template<typename T>
	T getHeaderFileld(uint64_t index, uint32_t offset) const;
	template<typename T>
	void setHeaderFileld(uint64_t index, uint32_t offset, T value);

	uint32_t getBlockHeadOffset(uint64_t index) const;

	ReferenceCounter referenceCounter_;
	uint8_t *blocks_;
	uint64_t startIndex_;
	uint32_t blockCount_;
	const Config *config_;
};

/*!
    @brief Manages latch of file of logs.
*/
class LogManager::LogFileLatch {
public:
	LogFileLatch();
	~LogFileLatch();

	void set(PartitionGroupManager &manager,
			CheckpointId cpId, bool expectExisting,
			const char8_t *suffix = NULL);
	void clear();

	PartitionGroupManager* getManager();
	LogFileInfo* get();
	LogFileInfo* get() const;

	void duplicate(LogFileLatch &dest);

private:
	LogFileLatch(const LogFileLatch&);
	LogFileLatch& operator=(const LogFileLatch&);

	PartitionGroupManager *manager_;
	LogFileInfo *fileInfo_;
};

/*!
    @brief Manages latch of block of logs.
*/
class LogManager::LogBlocksLatch {
public:
	LogBlocksLatch();
	~LogBlocksLatch();

	void setForScan(LogFileLatch &fileLatch, uint64_t startIndex);
	void setForAppend(LogFileLatch &fileLatch);

	void clear();

	PartitionGroupManager* getManager();
	LogFileInfo* getFileInfo();
	LogFileInfo* getFileInfo() const;
	LogBlocksInfo* get();
	LogBlocksInfo* get() const;

	void duplicate(LogBlocksLatch &dest);

private:
	LogBlocksLatch(const LogBlocksLatch&);
	LogBlocksLatch& operator=(const LogBlocksLatch&);

	void set(LogFileLatch &fileLatch, uint64_t startIndex, bool multiple);

	LogFileLatch fileLatch_;
	LogBlocksInfo *blocksInfo_;
};

/*!
    @brief Manages index of logs.
*/
class LogManager::LogFileIndex {
public:
	LogFileIndex();
	~LogFileIndex();

	void initialize(const Config &config,
			PartitionGroupId pgId, CheckpointId cpId, uint64_t logBlockCount,
			const std::vector<PartitionInfo> &partitionInfoList);
	void clear();
	bool isEmpty();

	bool tryFind(PartitionId pId, LogSequentialNumber startLsn,
			uint64_t &blockIndex, int32_t &direction, bool &empty);
	void trySetLastEntry(uint64_t blockIndex,
			PartitionId activePId, LogSequentialNumber activeLsn);

	void prepare(PartitionGroupManager &manager);
	void rebuild(PartitionGroupManager &manager);

	bool find(PartitionId pId, LogSequentialNumber startLsn,
			uint64_t &blockIndex, int32_t &direction, bool &empty);
	void setLastEntry(uint64_t blockIndex,
			PartitionId activePId, LogSequentialNumber activeLsn);

private:
	LogFileIndex(const LogFileIndex&);
	LogFileIndex& operator=(const LogFileIndex&);

	enum HeaderFiledId {
		HEADER_FIELD_VERSION,
		HEADER_FIELD_MAGIC,
		HEADER_FIELD_PARTITION_GROUP_ID,
		HEADER_FIELD_CHECKPOINT_ID,
		HEADER_FIELD_PARTITION_COUNT,
		HEADER_FIELD_MAX
	};

	static const uint64_t VERSION_NUMBER = 0x1;
	static const uint64_t MAGIC_NUMBER = UINT64_C(0xE3B0D89A9F5DE617);

	uint64_t getValue(uint64_t valueId);
	void setValue(uint64_t valueId, uint64_t value);

	void prepareBuffer(uint64_t valueId);
	void updateFile();

	void invalidate();

	static uint64_t getFileOffset(uint64_t valueId);
	uint32_t getBufferValueCount() const;
	uint32_t getPartitionCount() const;
	std::string getFileName() const;

	const Config *config_;
	PartitionGroupId pgId_;
	CheckpointId cpId_;
	const std::vector<PartitionInfo> *partitionInfoList_;

	util::NamedFile file_;
	uint64_t *buffer_;
	uint64_t bufferValueId_;
	uint64_t valueCount_;
	bool dirty_;
	bool invalid_;
};

/*!
	@brief Recovery information of a Partition
*/
struct LogManager::PartitionInfo {

	PartitionInfo() : lsn_(0), availableStartLsn_(0)
	{
	}

	LogSequentialNumber lsn_;

	LogSequentialNumber availableStartLsn_;
	std::map<CheckpointId, LogSequentialNumber> startLsnMap_;


};

/*!
    @brief Manages logs for the Partition group.
*/
class LogManager::PartitionGroupManager {
public:
	PartitionGroupManager();
	~PartitionGroupManager();

	void open(
			PartitionGroupId pgId, const Config &config,
			CheckpointId initialCpId, CheckpointId lastCpId,
			std::vector<PartitionInfo> &partitionInfoList,
			bool emptyFileAppendable, bool checkOnly,
			const char8_t *suffix = NULL, bool isSyncTempLogFile = false);
	void close();
	bool isClosed() const;

	CheckpointId getFirstCheckpointId() const;
	CheckpointId getLastCheckpointId() const;
	CheckpointId getCompletedCheckpointId() const;

	bool findLog(
			LogBlocksLatch &blocksLatch, uint64_t &offset,
			PartitionId pId, LogSequentialNumber lsn);
	bool findLogByIndex(
			LogBlocksLatch &blocksLatch, uint64_t &offset,
			PartitionId pId, LogSequentialNumber lsn,
			const BlockPosition &start, const BlockPosition &end,
			int32_t &direction);
	bool findLogByScan(
			LogBlocksLatch &blocksLatch, uint64_t &offset,
			PartitionId pId, LogSequentialNumber lsn,
			const BlockPosition &start, const BlockPosition &end,
			int32_t &direction);
	bool findCheckpointStartLog(
			LogBlocksLatch &blocksLatch, uint64_t &offset, CheckpointId cpId);
	bool findCheckpointEndLog(
			LogBlocksLatch &blocksLatch, uint64_t &offset, CheckpointId cpId);

	bool getLogHeader(
			LogBlocksLatch &blocksLatch, uint64_t &offset, LogRecord &logRecord,
			util::XArray<uint8_t> *data, bool sameFileOnly,
			const BlockPosition *endPosition = NULL);
	bool getLogBody(
			LogBlocksLatch &blocksLatch, uint64_t &offset,
			util::XArray<uint8_t> &data, uint32_t bodyLen);
	bool skipLogBody(
			LogBlocksLatch &blocksLatch, uint64_t &offset, uint32_t bodyLen);
	void putLog(const uint8_t *data, uint32_t size);

	void writeBuffer();
	void flushFile(bool forceClearCache = false);

	uint64_t copyLogFile(
			PartitionGroupId pgId, const char8_t *dirPath, bool duplicateLogMode);

	void prepareCheckpoint();

	void cleanupLogFiles(CheckpointId baseCpId = UNDEF_CHECKPOINT_ID);

	void setAutoCleanupLogFlag(bool flag);
	bool getAutoCleanupLogFlag();

	void setDuplicateLogPath(const char8_t *duplicateLogPath);

	void getDuplicateLogPath(std::string& duplicatePath);

	void setStopOnDuplicateErrorFlag(bool flag);
	bool getLongtermSyncLogErrorFlag() const;

	void setLongtermSyncLogError(const std::string &message);

	const std::string& getLongtermSyncLogErrorMessage() const;

	uint64_t getAvailableEndOffset(
			LogFileLatch &fileLatch, LogBlocksLatch &blocksLatch);
	uint64_t getAvailableEndBlock(
			LogFileLatch &fileLatch, LogBlocksLatch &blocksLatch);

	LogBlocksInfo* latchBlocks(
			LogFileLatch &fileLatch, uint64_t startIndex, bool multiple);
	void unlatchBlocks(LogFileLatch &fileLatch, LogBlocksInfo *&blocksInfo);

	LogFileInfo* latchFile(
			CheckpointId cpId, bool expectExisting, const char8_t *suffix);
	void unlatchFile(LogFileInfo *&fileInfo);


	void setLogVersion(uint16_t version);
	uint16_t getLogVersion();

	uint64_t getFileFlushCount() {
		return flushCount_;
	}
	uint64_t getFileFlushTime() {
		return flushTime_;
	}

private:
	typedef std::map<CheckpointId, LogFileInfo*> FileInfoMap;
	typedef std::map<BlockPosition, LogBlocksInfo*> BlocksInfoMap;

	PartitionGroupManager(const PartitionGroupManager&);
	PartitionGroupManager& operator=(const PartitionGroupManager&);

	void getAllBlockRange(BlockPosition &start, BlockPosition &end) const;
	BlockPosition splitBlockRange(
			const BlockPosition &start, const BlockPosition &end,
			LogFileLatch &fileLatch, LogBlocksLatch &blocksLatch);
	BlockPosition getMinBlockScanEnd(
			const BlockPosition &start, const BlockPosition &end);
	static bool isRangeEmpty(
			const BlockPosition &start, const BlockPosition &end);

	void updateTailBlock(
			PartitionId activePId, LogSequentialNumber activeLsn);

	bool prepareBlockForScan(
			LogBlocksLatch &blocksLatch, uint64_t &offset,
			bool sameFileOnly, const BlockPosition *endPosition = NULL);
	uint16_t getLogVersion(LogManager::LogType type);
	void prepareBlockForAppend(
			PartitionId activePId, LogSequentialNumber activeLsn, 
			uint16_t logVersion);

	void scanExistingFiles(
			LogFileLatch &fileLatch, LogBlocksLatch &blocksLatch,
			CheckpointId initialCpId, CheckpointId endCpId,
			LogFileIndex &fileIndex, const char8_t *suffix);

	PartitionGroupId pgId_;

	FileInfoMap fileInfoMap_;
	BlocksInfoMap blocksInfoMap_;

	CheckpointId firstCheckpointId_;
	CheckpointId lastCheckpointId_;
	CheckpointId completedCheckpointId_;

	LogFileIndex tailFileIndex_;
	LogBlocksLatch tailBlocksLatch_;

	uint64_t tailOffset_;
	uint64_t checkpointFileInfoOffset_;
	bool dirty_;
	util::Atomic<bool> flushRequired_;
	bool normalShutdownCompleted_;
	bool tailReadOnly_;
	bool checkOnly_;

	bool execAutoCleanup_;

	bool stopOnDuplicateErrorFlag_;
	std::string longtermSyncLogErrorMessage_; 

	bool longtermSyncLogErrorFlag_; 
	uint16_t logVersion_;
	util::Atomic<uint64_t> flushCount_; 
	util::Atomic<uint64_t> flushTime_;
	const Config *config_;
	UTIL_UNIQUE_PTR<BlockPool> blocksPool_;
	std::vector<PartitionInfo> *partitionInfoList_;

	util::Atomic<util::NamedFile*> lastLogFile_;
	std::string duplicateLogPath_;
};

/*!
	@brief Information of LogRecord for recovery
*/
struct LogRecord {
public:
	typedef LogManager::LogType LogType;

	static const uint32_t LOG_HEADER_SIZE = sizeof(uint8_t)
			+ sizeof(LogSequentialNumber) + sizeof(uint32_t)
			+ sizeof(PartitionId);

	LogSequentialNumber lsn_;
	uint32_t type_;
	PartitionId partitionId_;
	TransactionId txnId_;
	ContainerId containerId_;

	uint32_t bodyLen_;

	ClientId clientId_;					

	StatementId stmtId_;				
	uint32_t txnContextCreationMode_;	
	uint32_t txnTimeout_;				
	uint64_t numRowId_;					
	uint64_t numRow_;					
	uint32_t dataLen_;					
	const uint8_t *rowIds_;				
	const uint8_t *rowData_;			

	uint32_t containerType_;			
	uint32_t containerNameLen_;			
	uint32_t containerInfoLen_;			
	const uint8_t *containerName_;		
	const uint8_t *containerInfo_;		

	uint32_t columnIdCount_;
	uint32_t mapType_;
	uint32_t indexNameLen_;
	const char *indexName_;
	const ColumnId *columnIds_;

	LogSequentialNumber cpLsn_;
	uint32_t txnContextNum_;
	uint32_t containerNum_;
	const uint8_t *clientIds_;
	const uint8_t *activeTxnIds_;
	const uint8_t *refContainerIds_;
	const uint8_t *lastExecStmtIds_;
	const uint8_t *txnTimeouts_;

	ChunkId startChunkId_;
	int32_t chunkNum_;
	uint8_t chunkCategoryId_;
	uint8_t emptyInfo_;
	uint32_t expandedLen_;   

	uint32_t extensionNameLen_;
	const char *extensionName_;
	uint32_t paramCount_;
	const uint32_t *paramDataLen_;
	const uint8_t *paramData_;

	bool withBegin_;					
	bool isAutoCommit_;					

	uint16_t fileVersion_;

	LogRecord() {
		reset();
	}
	void reset();

	void dump(std::ostream &os);
	std::string getDumpString();

	static const char *const GS_LOG_TYPE_NAME_CHECKPOINT_START;
	static const char *const GS_LOG_TYPE_NAME_CREATE_SESSION;
	static const char *const GS_LOG_TYPE_NAME_CLOSE_SESSION;
	static const char *const GS_LOG_TYPE_NAME_BEGIN;
	static const char *const GS_LOG_TYPE_NAME_COMMIT;
	static const char *const GS_LOG_TYPE_NAME_ABORT;

	static const char *const GS_LOG_TYPE_NAME_CREATE_PARTITION;
	static const char *const GS_LOG_TYPE_NAME_DROP_PARTITION;
	static const char *const GS_LOG_TYPE_NAME_CREATE_CONTAINER;
	static const char *const GS_LOG_TYPE_NAME_DROP_CONTAINER;
	static const char *const GS_LOG_TYPE_NAME_CREATE_INDEX;
	static const char *const GS_LOG_TYPE_NAME_DROP_INDEX;
	static const char *const GS_LOG_TYPE_NAME_MIGRATE_SCHEMA;
	static const char *const GS_LOG_TYPE_NAME_CREATE_EVENT_NOTIFICATION;
	static const char *const GS_LOG_TYPE_NAME_DELETE_EVENT_NOTIFICATION;

	static const char *const GS_LOG_TYPE_NAME_CREATE_COLLECTION_ROW;
	static const char *const GS_LOG_TYPE_NAME_UPDATE_COLLECTION_ROW;
	static const char *const GS_LOG_TYPE_NAME_DELETE_COLLECTION_ROW;
	static const char *const GS_LOG_TYPE_NAME_PUT_COLLECTION_MULTIPLE_ROWS;
	static const char *const GS_LOG_TYPE_NAME_LOCK_COLLECTION_ROW;

	static const char *const GS_LOG_TYPE_NAME_CREATE_TIME_SERIES_ROW;
	static const char *const GS_LOG_TYPE_NAME_UPDATE_TIME_SERIES_ROW;
	static const char *const GS_LOG_TYPE_NAME_DELETE_TIME_SERIES_ROW;
	static const char *const GS_LOG_TYPE_NAME_PUT_TIME_SERIES_MULTIPLE_ROWS;
	static const char *const GS_LOG_TYPE_NAME_LOCK_TIME_SERIES_ROW;

	static const char *const GS_LOG_TYPE_NAME_CHECKPOINT_FILE_INFO;
	static const char *const GS_LOG_TYPE_NAME_CHUNK_TABLE_INFO;
	static const char *const GS_LOG_TYPE_NAME_CPFILE_LSN_INFO;

	static const char *const GS_LOG_TYPE_NAME_UNDEF;
};

/*!
    @brief Iterates records for reading logs sequential.
*/
 class LogCursor {
public:
	friend class LogManager;

	LogCursor();

	~LogCursor();

	bool isEmpty() const;

	void clear();

	bool nextLog(
			LogRecord &logRecord,
			util::XArray<uint8_t> &binaryLogRecord,
			PartitionId pId = UNDEF_PARTITIONID,
			uint8_t targetLogType = LogManager::LOG_TYPE_UNDEF);

	template<typename Stream>
	void exportPosition(Stream &stream);

	template<typename Stream>
	void importPosition(Stream &stream, LogManager &logManager);

	/*!
		@brief Recovery progress information
	*/
	struct Progress {

		Progress();

		PartitionGroupId pgId_;

		CheckpointId cpId_;

		uint64_t fileSize_;

		uint64_t offset_;

	};

	Progress getProgress() const;

private:
	LogCursor(const LogCursor&);
	LogCursor& operator=(const LogCursor&);

	void exportPosition(
			CheckpointId &cpId, uint64_t &offset,
			PartitionId &pId, LogSequentialNumber &lsn);
	void importPosition(
			LogManager &logManager, CheckpointId cpId, uint64_t offset,
			PartitionId pId, LogSequentialNumber lsn);

	PartitionGroupId pgId_;
	LogManager::LogBlocksLatch blocksLatch_;
	uint64_t offset_;
};



#define LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE_CUSTOM( \
	detector, entering)

#define LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE(pgId, entering)


inline LogSequentialNumber LogManager::getLSN(PartitionId pId) const {
	return partitionInfoList_[pId].lsn_;
}

inline void LogManager::setLSN(PartitionId pId, LogSequentialNumber lsn) {
	LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE(
			config_.pgConfig_.getPartitionGroupId(pId), true);



	partitionInfoList_[pId].lsn_ = lsn;
}

inline void LogManager::setAvailableStartLSN(PartitionId pId, CheckpointId cpId) {
	LOG_MANAGER_CONFLICTION_DETECTOR_SCOPE(
			config_.pgConfig_.getPartitionGroupId(pId), true);
	partitionInfoList_[pId].startLsnMap_[cpId] = getLSN(pId);
}

inline PartitionGroupId LogManager::calcPartitionGroupId(
		PartitionId pId) const {
	return config_.pgConfig_.getPartitionGroupId(pId);
}

inline PartitionId LogManager::calcGroupTopPartitionId(
		PartitionGroupId pgId) const {
	return config_.pgConfig_.getGroupBeginPartitionId(pgId);
}

inline void LogManager::setLogVersion(PartitionId pId, uint16_t version) {
	PartitionGroupManager &pgManager =
			*pgManagerList_[calcPartitionGroupId(pId)];
	pgManager.setLogVersion(version);
}

inline uint16_t LogManager::getLogVersion(PartitionId pId) {
	PartitionGroupManager &pgManager =
			*pgManagerList_[calcPartitionGroupId(pId)];
	return pgManager.getLogVersion();
}

inline uint16_t LogManager::selectNewerVersion(uint16_t arg1, uint16_t arg2) {
	int32_t score1 = getLogVersionScore(arg1);
	int32_t score2 = getLogVersionScore(arg2);
	assert(score1 != 0 && score2 != 0);
	return score1 > score2 ? arg1 : arg2;
}

template<typename Stream>
void LogCursor::exportPosition(Stream &stream) {
	CheckpointId cpId;
	uint64_t offset;
	PartitionId pId;
	LogSequentialNumber lsn;

	exportPosition(cpId, offset, pId, lsn);

	stream << cpId;
	stream << offset;
	stream << pId;
	stream << lsn;
}

template<typename Stream>
void LogCursor::importPosition(Stream &stream, LogManager &logManager) {
	CheckpointId cpId;
	uint64_t offset;
	PartitionId pId;
	LogSequentialNumber lsn;

	stream >> cpId;
	stream >> offset;
	stream >> pId;
	stream >> lsn;

	importPosition(logManager, cpId, offset, pId, lsn);
}

#endif
