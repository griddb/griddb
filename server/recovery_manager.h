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

/*!
	@brief Throws exception of log redo failure.
*/
class LogRedoException : public util::Exception {
public:
	LogRedoException(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_DECL) throw()
		: Exception(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_SET) {}
	virtual ~LogRedoException() throw() {}
};

/*!
	@brief Recovers database.
*/
class RecoveryManager {
public:

	static const ChunkCategoryId CHUNK_CATEGORY_NON_READY_PARTITION =
		UNDEF_CHUNK_CATEGORY_ID;  

	explicit RecoveryManager(ConfigTable &configTable);
	~RecoveryManager();

	void initialize(ManagerSet &mgrSet);


	static void checkExistingFiles(ConfigTable &configTable, bool &createFrag,
		bool forceRecoveryFromExistingFiles);





	void recovery(util::StackAllocator &alloc,
		const std::string &recoveryTargetPartition);

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


private:
	void redoAll(util::StackAllocator &alloc, Mode mode, LogCursor &cursor);


	LogSequentialNumber redoLogRecord(util::StackAllocator &alloc, Mode mode,
		PartitionId pId, const util::DateTime &stmtStartTime,
		EventMonotonicTime stmtStartEmTime, const LogRecord &logRecord,
		LogSequentialNumber &redoStartLsn);

	bool redoChunkLogRecord(util::StackAllocator &alloc, Mode mode,
		PartitionId pId, const LogRecord &logRecord,
		LogSequentialNumber &redoStartLsn);

	void removeTransaction(Mode mode, TransactionContext &txn);

	void parseRecoveryTargetPartition(const std::string &str);

	static void logRecordToString(PartitionId pId, const LogRecord &logRecord,
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

	PartitionGroupConfig pgConfig_;
	const uint8_t CHUNK_EXP_SIZE_;
	const int32_t CHUNK_SIZE_;

	bool recoveryOnlyCheckChunk_;	
	std::string recoveryDownPoint_;  
	int32_t recoveryDownCount_;		 


	static util::Atomic<uint32_t> progressPercent_;  

	std::vector<PartitionId> recoveryPartitionId_;
	bool recoveryTargetPartitionMode_;

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
