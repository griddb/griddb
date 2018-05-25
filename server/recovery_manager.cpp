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
	@brief Implementation of RecoveryManager
*/
#include "recovery_manager.h"
#include "cluster_event_type.h"
#include "gs_error.h"
#include "log_manager.h"
#include "transaction_manager.h"

#include "base_container.h"
#include "checkpoint_service.h"
#include "chunk_manager.h"
#include "collection.h"
#include "data_store.h"
#include "object_manager.h"
#include "partition_table.h"
#include "system_service.h"
#include "time_series.h"
#include "transaction_service.h"

UTIL_TRACER_DECLARE(RECOVERY_MANAGER);
UTIL_TRACER_DECLARE(RECOVERY_MANAGER_DETAIL);

#include "picojson.h"
#include <fstream>

#define RM_THROW_LOG_REDO_ERROR(errorCode, message) \
	GS_THROW_CUSTOM_ERROR(LogRedoException, errorCode, message)
#define RM_RETHROW_LOG_REDO_ERROR(cause, message) \
	GS_RETHROW_CUSTOM_ERROR(LogRedoException, GS_ERROR_DEFAULT, cause, message)

#ifndef NDEBUG
#endif


util::Atomic<uint32_t> RecoveryManager::progressPercent_(0);

namespace {
/*!
	@brief Formats Checkpoint IDs
*/
struct CheckpointIdFormatter {
	CheckpointIdFormatter(CheckpointId checkpointId)
		: checkpointId_(checkpointId) {}

	CheckpointId checkpointId_;
};

std::ostream &operator<<(
	std::ostream &s, const CheckpointIdFormatter &formatter) {
	if (formatter.checkpointId_ == UNDEF_CHECKPOINT_ID) {
		s << "(undefined)";
	}
	else {
		s << formatter.checkpointId_;
	}

	return s;
}
}

/*!
	@brief Constructor of RecoveryManager
*/
RecoveryManager::RecoveryManager(ConfigTable &configTable)
	: pgConfig_(configTable),
	  CHUNK_EXP_SIZE_(util::nextPowerBitsOf2(
		  configTable.getUInt32(CONFIG_TABLE_DS_STORE_BLOCK_SIZE))),
	  CHUNK_SIZE_(1 << CHUNK_EXP_SIZE_),
	  recoveryOnlyCheckChunk_(
		  configTable.get<bool>(CONFIG_TABLE_DEV_RECOVERY_ONLY_CHECK_CHUNK)),
	  recoveryDownPoint_(configTable.get<const char8_t *>(
		  CONFIG_TABLE_DEV_RECOVERY_DOWN_POINT)),
	  recoveryDownCount_(
		  configTable.get<int32_t>(CONFIG_TABLE_DEV_RECOVERY_DOWN_COUNT)),
	  recoveryTargetPartitionMode_(false) {
	statUpdator_.manager_ = this;

}

/*!
	@brief Destructor of RecoveryManager
*/
RecoveryManager::~RecoveryManager() {}

/*!
	@brief Initializer of RecoveryManager
*/
void RecoveryManager::initialize(ManagerSet &mgrSet) {
	chunkMgr_ = mgrSet.chunkMgr_;
	logMgr_ = mgrSet.logMgr_;
	pt_ = mgrSet.pt_;
	clsSvc_ = mgrSet.clsSvc_;
	sysSvc_ = mgrSet.sysSvc_;
	txnMgr_ = mgrSet.txnMgr_;
	ds_ = mgrSet.ds_;

	mgrSet.stats_->addUpdator(&chunkMgr_->getChunkManagerStats());
	mgrSet.stats_->addUpdator(&statUpdator_);

}


/*!
	@brief Checks if files exist.
*/
void RecoveryManager::checkExistingFiles1(
	ConfigTable &param, bool &createFlag, bool forceRecoveryFromExistingFiles) {
	createFlag = false;
	const char8_t *const dbPath =
		param.get<const char8_t *>(CONFIG_TABLE_DS_DB_PATH);
	if (util::FileSystem::exists(dbPath)) {
		if (!util::FileSystem::isDirectory(dbPath)) {
			GS_THROW_SYSTEM_ERROR(
				GS_ERROR_CM_IO_ERROR, "Create data directory failed. path=\""
										  << dbPath << "\" is not directory");
		}
	}
	else {
		try {
			util::FileSystem::createDirectoryTree(dbPath);
		}
		catch (std::exception &e) {
			GS_RETHROW_SYSTEM_ERROR(
				e, "Create data directory failed. (path=\""
					   << dbPath << "\""
					   << ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
		}
		createFlag = true;
		return;
	}
	try {
		const uint32_t partitionGroupNum =
			param.getUInt32(CONFIG_TABLE_DS_CONCURRENCY);

		util::Directory dir(dbPath);

		std::vector<std::vector<CheckpointId> > logFileInfoList(
			partitionGroupNum);
		std::vector<int32_t> cpFileInfoList(partitionGroupNum);

		for (u8string fileName; dir.nextEntry(fileName);) {
			PartitionGroupId pgId = UNDEF_PARTITIONGROUPID;
			CheckpointId cpId = UNDEF_CHECKPOINT_ID;

			if (LogManager::checkFileName(fileName, pgId, cpId)) {
				if (pgId >= partitionGroupNum) {
					GS_THROW_SYSTEM_ERROR(
						GS_ERROR_RM_PARTITION_GROUP_NUM_NOT_MATCH,
						"Invalid PartitionGroupId: "
							<< pgId << ", config partitionGroupNum="
							<< partitionGroupNum << ", fileName=" << fileName);
				}
				logFileInfoList[pgId].push_back(cpId);
			}
			else if (CheckpointFile::checkFileName(fileName, pgId)) {
				if (pgId >= partitionGroupNum) {
					GS_THROW_SYSTEM_ERROR(
						GS_ERROR_RM_PARTITION_GROUP_NUM_NOT_MATCH,
						"Invalid PartitionGroupId: "
							<< pgId << ", config partitionGroupNum="
							<< partitionGroupNum << ", fileName=" << fileName);
				}
				cpFileInfoList[pgId] = 1;
			}
			else {
				continue;
			}
		}
		bool existLogFile = false;
		std::vector<PartitionGroupId> emptyPgIdList;
		for (PartitionGroupId pgId = 0; pgId < partitionGroupNum; ++pgId) {
			if (logFileInfoList[pgId].size() == 0) {
				if (forceRecoveryFromExistingFiles) {
					emptyPgIdList.push_back(pgId);
				}
				else if (existLogFile) {
					GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_LOG_FILE_NOT_FOUND,
						"Log files are not found: pgId=" << pgId);
				}
			}
			else {
				existLogFile = true;
			}
		}
		if (!existLogFile) {
			for (PartitionGroupId pgId = 0; pgId < partitionGroupNum; ++pgId) {
				if (cpFileInfoList[pgId] != 0) {
					GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_CP_FILE_WITH_NO_LOG_FILE,
						"Checkpoint file is exist, but log files are not "
						"exist: pgId="
							<< pgId);
				}
			}
			createFlag = true;
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "File access failed. reason=("
										 << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void RecoveryManager::checkExistingFiles2(ConfigTable &param,
	LogManager &logMgr, bool &createFlag, bool forceRecoveryFromExistingFiles) {
	if (createFlag) {
		return;
	}
	const char8_t *const dbPath =
		param.get<const char8_t *>(CONFIG_TABLE_DS_DB_PATH);
	try {
		const uint32_t partitionGroupNum =
			param.getUInt32(CONFIG_TABLE_DS_CONCURRENCY);

		util::Directory dir(dbPath);

		std::vector<std::vector<CheckpointId> > logFileInfoList(
			partitionGroupNum);
		std::vector<int32_t> cpFileInfoList(partitionGroupNum);

		for (u8string fileName; dir.nextEntry(fileName);) {
			PartitionGroupId pgId = UNDEF_PARTITIONGROUPID;
			CheckpointId cpId = UNDEF_CHECKPOINT_ID;

			if (LogManager::checkFileName(fileName, pgId, cpId)) {
				if (pgId >= partitionGroupNum) {
					GS_THROW_SYSTEM_ERROR(
						GS_ERROR_RM_PARTITION_GROUP_NUM_NOT_MATCH,
						"Invalid PartitionGroupId: "
							<< pgId << ", config partitionGroupNum="
							<< partitionGroupNum << ", fileName=" << fileName);
				}
				logFileInfoList[pgId].push_back(cpId);
			}
			else if (CheckpointFile::checkFileName(fileName, pgId)) {
				if (pgId >= partitionGroupNum) {
					GS_THROW_SYSTEM_ERROR(
						GS_ERROR_RM_PARTITION_GROUP_NUM_NOT_MATCH,
						"Invalid PartitionGroupId: "
							<< pgId << ", config partitionGroupNum="
							<< partitionGroupNum << ", fileName=" << fileName);
				}
				cpFileInfoList[pgId] = 1;
			}
			else {
				continue;
			}
		}
		bool existLogFile = false;
		std::vector<PartitionGroupId> emptyPgIdList;
		for (PartitionGroupId pgId = 0; pgId < partitionGroupNum; ++pgId) {
			if (logFileInfoList[pgId].size() == 0) {
				if (forceRecoveryFromExistingFiles) {
					emptyPgIdList.push_back(pgId);
				}
				else if (existLogFile) {
					GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_LOG_FILE_NOT_FOUND,
						"Log files are not found: pgId=" << pgId);
				}
			}
			else {
				existLogFile = true;
			}
		}
		if (existLogFile) {
			for (PartitionGroupId pgId = 0; pgId < partitionGroupNum; ++pgId) {
				if (cpFileInfoList[pgId] == 0) {
					if (logMgr.getLastCompletedCheckpointId(pgId) !=
						UNDEF_CHECKPOINT_ID) {
						if (forceRecoveryFromExistingFiles) {
							if (logFileInfoList[pgId].size() != 0) {
								GS_THROW_SYSTEM_ERROR(
									GS_ERROR_RM_LOG_FILE_CP_FILE_MISMATCH,
									"Checkpoint file is not exist, and log "
									"files are not complete: pgId="
										<< pgId);
							}
							createFlag = true;
						}
						else {
							GS_THROW_SYSTEM_ERROR(
								GS_ERROR_RM_LOG_FILE_CP_FILE_MISMATCH,
								"Checkpoint file is not exist, and log files "
								"are not complete: pgId="
									<< pgId);
						}
					}
					else {
						createFlag = true;
					}
				}
			}
		}
		else {
			for (PartitionGroupId pgId = 0; pgId < partitionGroupNum; ++pgId) {
				if (cpFileInfoList[pgId] != 0) {
					GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_CP_FILE_WITH_NO_LOG_FILE,
						"Checkpoint file is exist, but log files are not "
						"exist: pgId="
							<< pgId);
				}
			}
			createFlag = true;
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "File access failed. reason=("
										 << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Parses a string of a partition of the recovery target.
*/
void RecoveryManager::parseRecoveryTargetPartition(const std::string &str) {
	const size_t MAX_PARTITION_NUM_STRING_LEN =
		5;  
	const uint32_t partitionNum = pgConfig_.getPartitionCount();
	if (str.length() > 0) {
		recoveryPartitionId_.assign(partitionNum, 0);
		recoveryTargetPartitionMode_ = true;
	}
	else {
		recoveryPartitionId_.assign(partitionNum, 1);
		recoveryTargetPartitionMode_ = false;
		return;
	}
	const std::string validChars = "0123456789,-";
	if (str.find_first_not_of(validChars, 0) != std::string::npos) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_INVALID_PARTITION,
			"Invalid partition range (" << str << ")");
	}
	bool validPIdFound = false;
	std::vector<std::string> pIdsList;
	size_t current = 0;
	size_t found = 0;
	while ((found = str.find_first_of(',', current)) != std::string::npos) {
		std::string token = std::string(str, current, found - current);
		pIdsList.push_back(token);
		current = found + 1;
	}
	if (current < str.length()) {
		found = str.length();
		std::string token = std::string(str, current, found - current);
		pIdsList.push_back(token);
	}
	const std::string numChars = "0123456789";
	std::vector<std::string>::iterator itr = pIdsList.begin();
	for (; itr != pIdsList.end(); ++itr) {
		current = 0;
		std::string numStr = *itr;
		if ((found = numStr.find_first_of('-', current)) != std::string::npos) {
			std::string startStr =
				std::string(numStr, current, found - current);
			std::string endStr =
				std::string(numStr, found + 1, numStr.length() - (found + 1));
			if (startStr.length() > MAX_PARTITION_NUM_STRING_LEN) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_INVALID_PARTITION,
					"Invalid partition No. (" << startStr << ")");
			}
			if (startStr.find_first_not_of(numChars, 0) != std::string::npos) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_INVALID_PARTITION,
					"Invalid partition range (" << numStr << ")");
			}
			if (endStr.length() > MAX_PARTITION_NUM_STRING_LEN) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_INVALID_PARTITION,
					"Invalid partition No. (" << endStr << ")");
			}
			if (endStr.find_first_not_of(numChars, 0) != std::string::npos) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_INVALID_PARTITION,
					"Invalid partition range (" << numStr << ")");
			}
			if ((startStr.length() == 0) || (endStr.length() == 0)) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_INVALID_PARTITION,
					"Invalid partition range (" << numStr << ")");
			}
			uint32_t startPId = atoi(startStr.c_str());
			uint32_t endPId = atoi(endStr.c_str());
			if (startPId >= partitionNum) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_INVALID_PARTITION,
					"Invalid partition No. (" << startStr << ")");
			}
			if (endPId >= partitionNum) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_INVALID_PARTITION,
					"Invalid partition No. (" << endStr << ")");
			}
			if (startPId >= endPId) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_INVALID_PARTITION,
					"Invalid partition range (" << numStr << ")");
			}
			for (uint32_t pId = startPId; pId <= endPId; ++pId) {
				recoveryPartitionId_[pId] = 1;
			}
			validPIdFound = true;
		}
		else {
			if (numStr.length() > MAX_PARTITION_NUM_STRING_LEN) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_INVALID_PARTITION,
					"Invalid partition No. (" << numStr << ")");
			}
			uint32_t targetPId = atoi(numStr.c_str());
			if (targetPId >= partitionNum) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_INVALID_PARTITION,
					"Invalid partition No. (" << numStr << ")");
			}
			recoveryPartitionId_[targetPId] = 1;
			validPIdFound = true;
		}
	}
	if (!validPIdFound) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_INVALID_PARTITION,
			"Invalid partition range (" << str << ")");
	}
}


/*!
	@brief Recovers database at the time of starting
*/
void RecoveryManager::recovery(
	util::StackAllocator &alloc, const std::string &recoveryTargetPartition)
{
	try {
		util::StackAllocator::Scope scope(alloc);

		const PartitionGroupId partitionGroupNum =
			pgConfig_.getPartitionGroupCount();

		util::XArray<bool> chunkTableInitialized(alloc);
		chunkTableInitialized.assign(pgConfig_.getPartitionCount(), false);

		parseRecoveryTargetPartition(recoveryTargetPartition);

		syncChunkPId_.assign(partitionGroupNum, UNDEF_PARTITIONID);
		syncChunkNum_.assign(partitionGroupNum, 0);
		syncChunkCount_.assign(partitionGroupNum, 0);

		progressPercent_ = 0;
		for (PartitionGroupId pgId = 0; pgId < partitionGroupNum; pgId++) {
			util::StackAllocator::Scope groupScope(alloc);

			CheckpointId completedCpId = 0;
			const CheckpointId lastCpId = logMgr_->getLastCheckpointId(pgId);
			CheckpointId recoveryStartCpId = 0;

			completedCpId = logMgr_->getLastCompletedCheckpointId(pgId);
			GS_TRACE_INFO(RECOVERY_MANAGER, GS_TRACE_RM_RECOVERY_INFO,
				"Recovery started (pgId="
					<< pgId << ", completedCpId="
					<< CheckpointIdFormatter(completedCpId)
					<< ", lastCpId=" << CheckpointIdFormatter(lastCpId) << ")");

			recoveryStartCpId = completedCpId;
			if (recoveryStartCpId != UNDEF_CHECKPOINT_ID) {
				LogCursor cursor;
				util::XArray<uint8_t> data(alloc);
				LogRecord record;

				logMgr_->findCheckpointEndLog(cursor, pgId, recoveryStartCpId);
				if (!cursor.nextLog(record, data) ||
					record.type_ != LogManager::LOG_TYPE_CHECKPOINT_END) {
					GS_THROW_USER_ERROR(
						GS_ERROR_RM_CHECKPOINT_END_LOG_NOT_FOUND,
						"(pgId=" << pgId << ", recoveryStartCpId="
								 << CheckpointIdFormatter(recoveryStartCpId)
								 << ")");
				}
				chunkMgr_->setCheckpointBit(
					pgId, record.rowData_, record.numRow_);
			}  

			chunkMgr_->isValidFileHeader(pgId);
			std::cout << "[" << (pgId + 1) << "/"
					  << pgConfig_.getPartitionGroupCount() << "]...";
			{
				LogCursor cursor;
				if (recoveryStartCpId == UNDEF_CHECKPOINT_ID) {
					if (lastCpId > 0) {
						logMgr_->findCheckpointStartLog(cursor, pgId, 1);
					}
				}
				else {
					logMgr_->findCheckpointStartLog(
						cursor, pgId, recoveryStartCpId);
				}

				redoAll(alloc, MODE_RECOVERY, cursor);
				progressPercent_ = static_cast<uint32_t>(
					(pgId + 1.0) / partitionGroupNum * 90);
			}
			std::cout << " done." << std::endl;
		}  
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(
			e, "Recovery failed. reason=(" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Undoes uncommitted transactions of restored partitions.
*/
size_t RecoveryManager::undo(util::StackAllocator &alloc, PartitionId pId) {

	try {
		util::StackAllocator::Scope scope(alloc);

		util::XArray<ClientId> undoTxnContextIds(alloc);

		txnMgr_->getTransactionContextId(pId, undoTxnContextIds);

		if (!ds_->isRestored(pId)) {  
			assert(undoTxnContextIds.size() == 0);
			return undoTxnContextIds.size();
		}
		for (size_t i = 0; i < undoTxnContextIds.size(); i++) {
			TransactionContext &txn =
				txnMgr_->get(alloc, pId, undoTxnContextIds[i]);

			if (txn.isActive()) {
				util::StackAllocator::Scope innerScope(alloc);

				const DataStore::Latch latch(
					txn, txn.getPartitionId(), ds_, clsSvc_);

				ContainerAutoPtr containerAutoPtr(txn, ds_,
					txn.getPartitionId(), txn.getContainerId(), ANY_CONTAINER);
				BaseContainer *container = containerAutoPtr.getBaseContainer();
				if (container == NULL) {
					GS_THROW_USER_ERROR(GS_ERROR_RM_CONTAINER_NOT_FOUND,
						"(containerId=" << txn.getContainerId() << ")");
				}

				util::XArray<uint8_t> log(alloc);
				const LogSequentialNumber lsn = logMgr_->putAbortTransactionLog(
					log, txn.getPartitionId(), txn.getClientId(), txn.getId(),
					txn.getContainerId(), txn.getLastStatementId());
				pt_->setLSN(txn.getPartitionId(), lsn);

				txnMgr_->abort(txn, *container);
			}

			txnMgr_->remove(pId, undoTxnContextIds[i]);
		}

		ds_->setUndoCompleted(pId);

		if (undoTxnContextIds.size() > 0) {
			GS_TRACE_INFO(RECOVERY_MANAGER, GS_TRACE_RM_COMPLETE_UNDO,
				"Complete undo (pId=" << pId << ", lsn=" << logMgr_->getLSN(pId)
									  << ", txnCount="
									  << undoTxnContextIds.size() << ")");
		}
		else {
			GS_TRACE_WARNING(RECOVERY_MANAGER_DETAIL, GS_TRACE_RM_COMPLETE_UNDO,
				"Complete undo (pId=" << pId << ", lsn=" << logMgr_->getLSN(pId)
									  << ", txnCount="
									  << undoTxnContextIds.size() << ")");
		}

		return undoTxnContextIds.size();
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, "Undo failed. (pId=" << pId << ", reason="
													  << GS_EXCEPTION_MESSAGE(e)
													  << ")");
	}
}

/*!
	@brief Redoes logs,
*/
void RecoveryManager::redoLogList(util::StackAllocator &alloc, Mode mode,
	PartitionId pId, const util::DateTime &redoStartTime,
	EventMonotonicTime redoStartEmTime, const uint8_t *binaryLogRecords,
	size_t size) {
	try {
		const uint8_t *addr = binaryLogRecords;

		while (static_cast<size_t>(addr - binaryLogRecords) < size) {
			const uint8_t *binaryLogRecord = addr;
			LogRecord logRecord;

			uint32_t binaryLogRecordSize =
				LogManager::parseLogHeader(addr, logRecord);

			if (mode != MODE_RECOVERY && pId != logRecord.partitionId_) {
				RM_THROW_LOG_REDO_ERROR(
					GS_ERROR_TXN_REPLICATION_LOG_LSN_INVALID,
					"Invalid logRecord, target pId:" << pId << ", log pId:"
													 << logRecord.partitionId_);
			}

			const LogSequentialNumber currentLsn =
				logMgr_->getLSN(logRecord.partitionId_);
			const int64_t diff = logRecord.lsn_ - currentLsn;

			if (diff == 1 || currentLsn == 0) {
				binaryLogRecordSize +=
					LogManager::parseLogBody(addr, logRecord);

				LogSequentialNumber dummy = UNDEF_LSN;
				const LogSequentialNumber nextLsn =
					redoLogRecord(alloc, mode, logRecord.partitionId_,
						redoStartTime, redoStartEmTime, logRecord, dummy);

				logMgr_->putReplicationLog(logRecord.partitionId_,
					logRecord.lsn_, binaryLogRecordSize,
					const_cast<uint8_t *>(binaryLogRecord));

				pt_->setLSN(logRecord.partitionId_, nextLsn);
			}
			else if (diff <= 0) {
				addr += logRecord.bodyLen_;

				if (clsSvc_->getManager()->reportError(
						GS_ERROR_RM_ALREADY_APPLY_LOG)) {
					GS_TRACE_WARNING(RECOVERY_MANAGER,
						GS_TRACE_RM_APPLY_LOG_FAILED,
						"Log already applied (pId="
							<< pId << ", lsn=" << currentLsn
							<< ", logLsn=" << logRecord.lsn_ << ")");
				}
				else {
					GS_TRACE_DEBUG(RECOVERY_MANAGER,
						GS_TRACE_RM_APPLY_LOG_FAILED,
						"Log already applied (pId="
							<< pId << ", lsn=" << currentLsn
							<< ", logLsn=" << logRecord.lsn_ << ")");
				}
			}
			else {
				RM_THROW_LOG_REDO_ERROR(
					GS_ERROR_TXN_REPLICATION_LOG_LSN_INVALID,
					"Invalid LSN (pId=" << logRecord.partitionId_
										<< ", currentLsn=" << currentLsn
										<< ", logLsn=" << logRecord.lsn_
										<< ")");
			}
		}
	}
	catch (LogRedoException &e) {
		RM_RETHROW_LOG_REDO_ERROR(
			e, "(mode=" << mode << ", pId=" << pId
						<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Apply replication log failed. (mode="
									   << mode << ", pId=" << pId << ", reason="
									   << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Recovers information of transactions at the beginning of checkpoint.
*/
LogSequentialNumber RecoveryManager::restoreTransaction(
	PartitionId pId, const uint8_t *buffer, EventMonotonicTime emNow) {
	try {
		const uint8_t *addr = buffer;
		LogRecord logRecord;

		LogManager::parseLogHeader(addr, logRecord);
		LogManager::parseLogBody(addr, logRecord);

		if (logRecord.type_ != LogManager::LOG_TYPE_CHECKPOINT_START ||
			logRecord.partitionId_ != pId) {
			GS_THROW_USER_ERROR(GS_ERROR_RM_REDO_LOG_CONTENT_INVALID,
				"(type=" << logRecord.type_
						 << ", pId=" << logRecord.partitionId_ << ")");
		}

		txnMgr_->restoreTransactionActiveContext(pId, logRecord.txnId_,
			logRecord.txnContextNum_,
			reinterpret_cast<const ClientId *>(logRecord.clientIds_),
			reinterpret_cast<const TransactionId *>(logRecord.activeTxnIds_),
			reinterpret_cast<const ContainerId *>(logRecord.refContainerIds_),
			reinterpret_cast<const StatementId *>(logRecord.lastExecStmtIds_),
			reinterpret_cast<const int32_t *>(logRecord.txnTimeouts_), emNow);

		logMgr_->setLSN(pId, logRecord.lsn_);

		pt_->setLSN(pId, logRecord.lsn_);

		pt_->setStartLSN(pId, logRecord.lsn_);

		return logRecord.lsn_;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, "Restore transaction failed. w(pId="
				   << pId << ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Drops all data in the partition.
*/
void RecoveryManager::dropPartition(TransactionContext &txn,
	util::StackAllocator &alloc, PartitionId pId, bool putLog) {
	try {
		util::StackAllocator::Scope scope(alloc);

		if (putLog) {
			util::XArray<uint8_t> binaryLogList(alloc);
			logMgr_->putDropPartitionLog(binaryLogList, pId);
			logMgr_->setLSN(pId, 0);
		}

		ds_->dropPartition(pId);

		txnMgr_->removePartition(pId);

		pt_->setLSN(pId, 0);

	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, "Drop partition failed. (pId="
				   << pId << ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Outputs all data of Checkpoint files in CSV file.
*/
void RecoveryManager::dumpCheckpointFile(
	util::StackAllocator &alloc, const char8_t *dirPath) {
	std::cout << "--dbdump" << std::endl;

	util::StackAllocator::Scope scope(alloc);

	if (!util::FileSystem::isDirectory(dirPath)) {
		GS_THROW_USER_ERROR(GS_ERROR_RM_CHECKPOINT_END_LOG_NOT_FOUND,
			"Output directory does not found (path=" << dirPath << ")");
	}

	for (PartitionGroupId pgId = 0; pgId < pgConfig_.getPartitionGroupCount();
		 pgId++) {
		std::cout << "[" << (pgId + 1) << "/"
				  << pgConfig_.getPartitionGroupCount() << "]...";

		util::StackAllocator::Scope groupScope(alloc);

		const CheckpointId recoveryStartCpId =
			logMgr_->getLastCompletedCheckpointId(pgId);

		if (recoveryStartCpId == UNDEF_CHECKPOINT_ID) {
			GS_THROW_USER_ERROR(GS_ERROR_RM_CHECKPOINT_END_LOG_NOT_FOUND,
				"recoveryStartCpId not found (pgId=" << pgId << ")");
		}

		LogCursor cursor;
		util::XArray<uint8_t> data(alloc);
		LogRecord record;

		logMgr_->findCheckpointEndLog(cursor, pgId, recoveryStartCpId);
		if (!cursor.nextLog(record, data) ||
			record.type_ != LogManager::LOG_TYPE_CHECKPOINT_END) {
			GS_THROW_USER_ERROR(GS_ERROR_RM_CHECKPOINT_END_LOG_NOT_FOUND,
				"(pgId=" << pgId << ", recoveryStartCpId="
						 << CheckpointIdFormatter(recoveryStartCpId) << ")");
		}

		util::NamedFile dumpFile;
		{
			util::NormalOStringStream oss;
			oss << "gs_cp_dump_" << pgId << ".csv";
			std::string filePath;
			util::FileSystem::createPath(dirPath, oss.str().c_str(), filePath);
			dumpFile.open(filePath.c_str(), util::FileFlag::TYPE_READ_WRITE |
												util::FileFlag::TYPE_CREATE |
												util::FileFlag::TYPE_TRUNCATE);
		}

		const uint64_t blockCount =
			chunkMgr_->getChunkManagerStats().getCheckpointFileSize(pgId) /
			CHUNK_SIZE_;
		if (blockCount > 0) {
			util::NormalOStringStream oss;
			oss << chunkMgr_->dumpChunkCSVField();
			oss << std::endl;

			BitArray validBitArray(record.numRow_);
			validBitArray.putAll(record.rowData_, record.numRow_);

			for (uint64_t blockNo = 0; blockNo < blockCount; ++blockNo) {
				oss << chunkMgr_->dumpChunkCSV(pgId, blockNo);
				oss << std::endl;

				if (blockNo + 1 >= record.numRow_ || blockNo % 100 == 0) {
					const std::string data = oss.str();
					dumpFile.write(data.c_str(), data.size());

					oss.str(std::string());
				}
			}
		}
		else {
			std::cout << " WARNING empty cp file (pgId=" << pgId
					  << ", cpId=" << recoveryStartCpId << ")... ";
		}

		dumpFile.sync();
		dumpFile.close();

		std::cout << " done." << std::endl;
	}
}

/*!
	@brief Redoes from cursor position of Log to the end of Checkpoint file or
   Log.
*/
void RecoveryManager::redoAll(
	util::StackAllocator &alloc, Mode mode, LogCursor &cursor) {

	util::XArray<uint64_t> numRedoLogRecord(alloc);
	numRedoLogRecord.assign(pgConfig_.getPartitionCount(), 0);

	util::XArray<uint64_t> numAfterDropLogs(alloc);
	numAfterDropLogs.assign(pgConfig_.getPartitionCount(), 0);

	util::XArray<LogSequentialNumber> redoStartLsn(alloc);
	redoStartLsn.assign(pgConfig_.getPartitionCount(), 0);

	const uint32_t traceIntervalMillis = 15000;
	util::Stopwatch traceTimer(util::Stopwatch::STATUS_STARTED);

	LogRecord logRecord;
	util::XArray<uint8_t> binaryLogRecord(alloc);

	const PartitionGroupId pgId = cursor.getProgress().pgId_;
	const PartitionId beginPId = pgConfig_.getGroupBeginPartitionId(pgId);
	const PartitionId endPId = pgConfig_.getGroupEndPartitionId(pgId);

	for (uint64_t logCount = 0;; logCount++) {
		binaryLogRecord.clear();
		if (!cursor.nextLog(logRecord, binaryLogRecord)) {
			break;
		}
		const PartitionId pId = logRecord.partitionId_;
		if (!(beginPId <= pId && pId < endPId)) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_PARTITION_NUM_NOT_MATCH,
				"Invalid PartitionId: " << pId << ", config partitionNum="
										<< pgConfig_.getPartitionCount()
										<< ", pgId=" << pgId << ", beginPId="
										<< beginPId << ", endPId=" << endPId);
		}

		if (traceTimer.elapsedMillis() >= traceIntervalMillis) {
			const LogCursor::Progress progress = cursor.getProgress();

			sysSvc_->traceStats(alloc);
			GS_TRACE_INFO(RECOVERY_MANAGER, GS_TRACE_RM_REDO_LOG_STATUS,
				"Log redo working ("
				"pgId="
					<< progress.pgId_ << ", cpId=" << progress.cpId_
					<< ", fileOffset=" << progress.offset_ << "/"
					<< progress.fileSize_ << "("
					<< (static_cast<double>(progress.offset_) * 100 /
						   progress.fileSize_)
					<< "%)"
					<< ", logCount=" << logCount << ")");

			traceTimer.reset();
			traceTimer.start();
		}

		if (recoveryPartitionId_[pId] == 0) {
			continue;
		}
		const LogSequentialNumber lsn = logRecord.lsn_;

		const LogSequentialNumber prevLsn =
			logMgr_->getLSN(logRecord.partitionId_);
		const bool lsnAssignable =
			LogManager::isLsnAssignable(static_cast<uint8_t>(logRecord.type_));

		if (numAfterDropLogs[pId] > 0) {
			numAfterDropLogs[pId]++;
			continue;
		}

		if (numRedoLogRecord[pId] == 0) {
			bool checkCpStart = true;

			if (checkCpStart &&
				(logRecord.type_ != LogManager::LOG_TYPE_CHECKPOINT_START)) {
				GS_TRACE_DEBUG(RECOVERY_MANAGER, GS_TRACE_RM_REDO_LOG_STATUS,
					"Log before cp/put_container skipped (pId="
						<< logRecord.partitionId_ << ", prevLsn=" << prevLsn
						<< ", targetLsn=" << lsn
						<< ", lsnAssignable=" << lsnAssignable
						<< ", logType=" << LogManager::logTypeToString(
											   static_cast<LogManager::LogType>(
												   logRecord.type_))
						<< ")");
				continue;
			}
			redoStartLsn[pId] = lsn;
			GS_TRACE_INFO(RECOVERY_MANAGER, GS_TRACE_RM_REDO_LOG_STATUS,
				"Redo started (pId=" << pId << ", lsn=" << lsn << ")");
		}

		if (!(prevLsn == 0 || (lsnAssignable && lsn == prevLsn + 1) ||
				(!lsnAssignable && lsn == prevLsn))) {
			GS_THROW_USER_ERROR(GS_ERROR_RM_REDO_LOG_LSN_INVALID,
				"Invalid LSN (pId="
					<< logRecord.partitionId_ << ", prevLsn=" << prevLsn
					<< ", targetLsn=" << lsn
					<< ", lsnAssignable=" << lsnAssignable << ", logType="
					<< LogManager::logTypeToString(
						   static_cast<LogManager::LogType>(logRecord.type_))
					<< ", logFileVersion="
					<< static_cast<uint32_t>(logRecord.fileVersion_) << ")");
		}

		if (prevLsn > lsn) {
			GS_TRACE_INFO(RECOVERY_MANAGER, GS_TRACE_RM_REDO_LOG_STATUS,
				"Redo lsn jumped (pId=" << pId << ", lsn=" << lsn
										<< ", prevLsn=" << prevLsn << ")");
		}

		const Timestamp stmtStartTimeOnRecovery = 0;
		const LogSequentialNumber nextLsn = redoLogRecord(alloc, mode,
			logRecord.partitionId_, stmtStartTimeOnRecovery,
			stmtStartTimeOnRecovery, logRecord, redoStartLsn[pId]);

		logMgr_->setLSN(logRecord.partitionId_, nextLsn);
		pt_->setLSN(logRecord.partitionId_, nextLsn);

		if (logRecord.type_ == LogManager::LOG_TYPE_DROP_PARTITION) {
			numAfterDropLogs[pId]++;
		}
		numRedoLogRecord[logRecord.partitionId_]++;

		ChunkManager::Config &chunkManagerConfig = chunkMgr_->getConfig();
		ChunkManager::ChunkManagerStats &chunkManagerStats =
			chunkMgr_->getChunkManagerStats();
		uint64_t storeMem = chunkManagerStats.getStoreMemory();
		if (storeMem > chunkManagerConfig.getAtomicStoreMemoryLimit() &&
			(storeMem - chunkManagerConfig.getAtomicStoreMemoryLimit()) >
				10 * 1024 * 1024) {
			chunkMgr_->redistributeMemoryLimit(0);
			for (PartitionGroupId tmpPgId = 0; tmpPgId < pgId; ++tmpPgId) {
				chunkMgr_->adjustPGStoreMemory(tmpPgId);
			}
		}
	}
	for (size_t i = 0; i < numAfterDropLogs.size(); i++) {
		if (numAfterDropLogs[i] <= 0) {
			continue;
		}

		const PartitionId pId = static_cast<PartitionId>(i);
		GS_TRACE_INFO(RECOVERY_MANAGER, GS_TRACE_RM_REDO_LOG_STATUS,
			"One or more logs after drop log ignored (pId="
				<< pId << ", numLogs=" << numAfterDropLogs[i] << ")");
	}

	for (size_t i = 0; i < numRedoLogRecord.size(); i++) {
		if (numRedoLogRecord[i] <= 0) {
			continue;
		}

		const PartitionId pId = static_cast<PartitionId>(i);
		const LogSequentialNumber lsn = logMgr_->getLSN(pId);
		GS_TRACE_INFO(RECOVERY_MANAGER, GS_TRACE_RM_REDO_LOG_STATUS,
			"Redo finished (pId=" << pId << ", lsn=" << lsn << ")");
	}
}

/*!
	@brief Redoes LogRecord of Chunks; i.e. applies to DataStore.
*/

/*!
	@brief Redoes each parsed LogRecord, i.e. apply to TxnManager and DataStore.
*/
LogSequentialNumber RecoveryManager::redoLogRecord(util::StackAllocator &alloc,
	Mode mode, PartitionId pId, const util::DateTime &stmtStartTime,
	EventMonotonicTime stmtStartEmTime, const LogRecord &logRecord,
	LogSequentialNumber &redoStartLsn) {
	try {
		util::StackAllocator::Scope scope(alloc);
		LogSequentialNumber nextLsn = logRecord.lsn_;

		const bool REDO = true;

		const PartitionGroupId pgId = pgConfig_.getPartitionGroupId(pId);
		switch (logRecord.type_) {
		case LogManager::LOG_TYPE_CHECKPOINT_START:
			if ((mode == MODE_LONG_TERM_SYNC) ||
				(mode == MODE_RECOVERY && logRecord.lsn_ == redoStartLsn)) {
				GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL,
					GS_TRACE_RM_REDO_LOG_STATUS,
					"LOG_TYPE_CHECKPOINT_START(pId="
						<< pId << ", lsn=" << logRecord.lsn_
						<< ", maxTxnId=" << logRecord.txnId_
						<< ", numTxn=" << logRecord.txnContextNum_ << ")");

				const uint32_t partitionCount = pgConfig_.getPartitionCount();
				if (logRecord.containerId_ != partitionCount) {
					GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_PARTITION_NUM_NOT_MATCH,
						"Invalid PartitionNum: log partitionNum="
							<< logRecord.containerId_
							<< ", config partitionNum=" << partitionCount);
				}
				txnMgr_->restoreTransactionActiveContext(
					pId, logRecord.txnId_, logRecord.txnContextNum_,
					reinterpret_cast<const ClientId *>(logRecord.clientIds_),
					reinterpret_cast<const TransactionId *>(
						logRecord.activeTxnIds_),
					reinterpret_cast<const ContainerId *>(
						logRecord.refContainerIds_),
					reinterpret_cast<const StatementId *>(
						logRecord.lastExecStmtIds_),
					reinterpret_cast<const int32_t *>(logRecord.txnTimeouts_),
					stmtStartEmTime);
			}
			else {
				GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL,
					GS_TRACE_RM_REDO_LOG_STATUS,
					"LOG_TYPE_CP_START: skipped.(pId="
						<< pId << ", lsn=" << logRecord.lsn_
						<< ", redoStartLsn=" << redoStartLsn << ")");
			}
			break;

		case LogManager::LOG_TYPE_CHUNK_META_DATA:
			if (mode == MODE_RECOVERY && logRecord.lsn_ == redoStartLsn) {
				GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL,
					GS_TRACE_RM_REDO_LOG_STATUS,
					"LOG_TYPE_CHUNK_META_DATA(pId="
						<< pId << ", lsn=" << logRecord.lsn_
						<< ", redoStartLsn=" << redoStartLsn
						<< ", chunkCategoryId="
						<< (int32_t)logRecord.chunkCategoryId_
						<< ", startChunkId=" << logRecord.startChunkId_
						<< ", chunkNum=" << logRecord.chunkNum_ << ")");
				if (logRecord.containerId_ == UNDEF_CONTAINERID) {
					const util::DateTime now(0);
					const EventMonotonicTime emNow = 0;
					const TransactionManager::ContextSource src(
						UNDEF_EVENT_TYPE);
					TransactionContext &txn = txnMgr_->put(
						alloc, pId, TXN_EMPTY_CLIENTID, src, now, emNow);
					if (logRecord.chunkNum_ > 0) {
						ds_->restartPartition(txn, clsSvc_);
						GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL,
							GS_TRACE_RM_REDO_LOG_STATUS,
							"LOG_TYPE_CHUNK_META_DATA: complete.(pId="
								<< pId << ", lsn=" << logRecord.lsn_
								<< ", redoStartLsn=" << redoStartLsn << ")");
					}
					else {
						GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL,
							GS_TRACE_RM_REDO_LOG_STATUS,
							"LOG_TYPE_CHUNK_META_DATA: non_ready "
							"partition.(pId="
								<< pId << ", lsn=" << logRecord.lsn_
								<< ", redoStartLsn=" << redoStartLsn << ")");
					}
					redoStartLsn =
						UNDEF_LSN;  
					break;
				}
				const uint8_t *addr = logRecord.rowData_;
				const PartitionGroupId currentPgId =
					pgConfig_.getPartitionGroupId(logRecord.partitionId_);
				for (int32_t chunkId = logRecord.startChunkId_;
					 chunkId < logRecord.startChunkId_ + logRecord.chunkNum_;
					 chunkId++) {
					uint8_t unoccupiedSize = *addr;
					++addr;
					uint64_t filePos;
					addr += util::varIntDecode64(addr, filePos);
					uint32_t chunkKey = UNDEF_CHUNK_KEY;
					if (chunkMgr_->isBatchFreeMode(
							logRecord.chunkCategoryId_)) {
						addr += util::varIntDecode32(addr, chunkKey);
					}
					if (unoccupiedSize != 0xff) {
						chunkMgr_->recoveryChunk(logRecord.partitionId_,
							logRecord.chunkCategoryId_, chunkId, chunkKey,
							unoccupiedSize, filePos);

						ChunkManager::ChunkManagerStats &chunkManagerStats =
							chunkMgr_->getChunkManagerStats();
						ChunkManager::Config &chunkManagerConfig =
							chunkMgr_->getConfig();
						uint64_t storeMem = chunkManagerStats.getStoreMemory();
						if (storeMem > chunkManagerConfig
										   .getAtomicStoreMemoryLimit() &&
							(storeMem -
								chunkManagerConfig
									.getAtomicStoreMemoryLimit()) >
								10 * 1024 * 1024) {
							chunkMgr_->redistributeMemoryLimit(0);
							for (PartitionGroupId tmpPgId = 0;
								 tmpPgId < currentPgId; ++tmpPgId) {
								chunkMgr_->adjustPGStoreMemory(tmpPgId);
							}
						}

					}
					GS_TRACE_DEBUG(RECOVERY_MANAGER_DETAIL,
						GS_TRACE_RM_REDO_LOG_STATUS,
						"initMetaChunk (pId,"
							<< logRecord.partitionId_ << ",categoryId,"
							<< (uint32_t)logRecord.chunkCategoryId_
							<< ",chunkId," << chunkId << ",unoccupiedSize,"
							<< (uint32_t)unoccupiedSize << ",filePos,"
							<< filePos << ",chunkKey," << chunkKey << ")");
				}
			}
			else {
				GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL,
					GS_TRACE_RM_REDO_LOG_STATUS,
					"LOG_TYPE_CHUNK_META_DATA: skipped.(pId="
						<< pId << ", lsn=" << logRecord.lsn_
						<< ", redoStartLsn=" << redoStartLsn << ")");
			}
			break;

		case LogManager::LOG_TYPE_CHECKPOINT_END:
		case LogManager::LOG_TYPE_SHUTDOWN:
			break;

		case LogManager::LOG_TYPE_PUT_CHUNK_START: {
			const bool checkpointReady =
				ds_->isRestored(pId) && chunkMgr_->existPartition(pId);
			if (checkpointReady) {
				GS_TRACE_WARNING(RECOVERY_MANAGER, GS_TRACE_RM_REDO_LOG_STATUS,
					"LOG_TYPE_PUT_CHUNK_START: Sync start for active "
					"partition. Drop force. (pId="
						<< pId << ")");

				ds_->dropPartition(pId);
				txnMgr_->removePartition(pId);

				pt_->setLSN(pId, 0);
			}
			if (syncChunkPId_[pgId] != UNDEF_PARTITIONID) {
				GS_TRACE_WARNING(RECOVERY_MANAGER, GS_TRACE_RM_REDO_LOG_STATUS,
					"LOG_TYPE_PUT_CHUNK_START: Incomplete sync detect. (pId="
						<< syncChunkPId_[pgId] << ")");
			}
			syncChunkPId_[pgId] = pId;
			syncChunkNum_[pgId] = logRecord.chunkNum_;
			syncChunkCount_[pgId] = 0;

			GS_TRACE_INFO(RECOVERY_MANAGER, GS_TRACE_RM_REDO_LOG_STATUS,
				"LOG_TYPE_PUT_CHUNK_START: (pId=" << pId << ", lsn="
												  << logRecord.lsn_ << ")");
		} break;
		case LogManager::LOG_TYPE_PUT_CHUNK_DATA: {
			chunkMgr_->recoveryChunk(
				pId, logRecord.rowData_, logRecord.dataLen_);
			++syncChunkCount_[pgId];
		} break;
		case LogManager::LOG_TYPE_PUT_CHUNK_END: {
			if (syncChunkNum_[pgId] != syncChunkCount_[pgId]) {
			}
			const util::DateTime now(0);
			const EventMonotonicTime emNow = 0;
			const TransactionManager::ContextSource src(UNDEF_EVENT_TYPE);
			TransactionContext &txn =
				txnMgr_->put(alloc, pId, TXN_EMPTY_CLIENTID, src, now, emNow);

			ds_->restartPartition(txn, clsSvc_);
			GS_TRACE_INFO(RECOVERY_MANAGER, GS_TRACE_RM_REDO_LOG_STATUS,
				"LOG_TYPE_PUT_CHUNK_END: complete.(pId="
					<< pId << ", lsn=" << logRecord.lsn_ << ")");

			syncChunkPId_[pgId] = UNDEF_PARTITIONID;
			syncChunkNum_[pgId] = 0;
			syncChunkCount_[pgId] = 0;
		} break;

		case LogManager::LOG_TYPE_COMMIT: {
			GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL, GS_TRACE_RM_REDO_LOG_STATUS,
				"LOG_TYPE_COMMIT(pId="
					<< pId << ", lsn=" << logRecord.lsn_
					<< ", clientId=" << logRecord.clientId_ << ", containerId="
					<< logRecord.containerId_ << ", txnId=" << logRecord.txnId_
					<< ", stmtId=" << logRecord.stmtId_ << ")");
			const TransactionManager::ContextSource src(COMMIT_TRANSACTION,
				logRecord.stmtId_, logRecord.containerId_,
				logRecord.txnTimeout_, TransactionManager::PUT,
				TransactionManager::NO_AUTO_COMMIT_BEGIN_OR_CONTINUE);
			TransactionContext &txn =
				txnMgr_->put(alloc, pId, logRecord.clientId_, src,
					stmtStartTime, stmtStartEmTime, REDO, logRecord.txnId_);
			assert(txn.getId() == logRecord.txnId_);
			assert(txn.getContainerId() == logRecord.containerId_);

			const DataStore::Latch latch(
				txn, txn.getPartitionId(), ds_, clsSvc_);

			ContainerAutoPtr containerAutoPtr(txn, ds_, txn.getPartitionId(),
				txn.getContainerId(), ANY_CONTAINER);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			assert(container != NULL);

			txnMgr_->commit(txn, *container);
			txnMgr_->update(txn, logRecord.stmtId_);

			removeTransaction(mode, txn);
		} break;

		case LogManager::LOG_TYPE_ABORT: {
			GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL, GS_TRACE_RM_REDO_LOG_STATUS,
				"LOG_TYPE_ABORT(pId="
					<< pId << ", lsn=" << logRecord.lsn_
					<< ", clientId=" << logRecord.clientId_ << ", containerId="
					<< logRecord.containerId_ << ", txnId=" << logRecord.txnId_
					<< ", stmtId=" << logRecord.stmtId_ << ")");
			const TransactionManager::ContextSource src(ABORT_TRANSACTION,
				logRecord.stmtId_, logRecord.containerId_,
				logRecord.txnTimeout_, TransactionManager::PUT,
				TransactionManager::NO_AUTO_COMMIT_BEGIN_OR_CONTINUE);
			TransactionContext &txn =
				txnMgr_->put(alloc, pId, logRecord.clientId_, src,
					stmtStartTime, stmtStartEmTime, REDO, logRecord.txnId_);
			assert(txn.getId() == logRecord.txnId_);
			assert(txn.getContainerId() == logRecord.containerId_);

			const DataStore::Latch latch(
				txn, txn.getPartitionId(), ds_, clsSvc_);

			ContainerAutoPtr containerAutoPtr(txn, ds_, txn.getPartitionId(),
				txn.getContainerId(), ANY_CONTAINER);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			assert(container != NULL);

			txnMgr_->abort(txn, *container);
			txnMgr_->update(txn, logRecord.stmtId_);

			removeTransaction(mode, txn);
		} break;

		case LogManager::LOG_TYPE_DROP_PARTITION:
			if (mode == MODE_LONG_TERM_SYNC || mode == MODE_RECOVERY) {
				if (recoveryTargetPartitionMode_) {
					recoveryPartitionId_[pId] = 0;
					GS_TRACE_INFO(RECOVERY_MANAGER, GS_TRACE_RM_REDO_LOG_STATUS,
						"--recovery-target-partition is specified. Ignore "
						"after DROP_PARTITION_LOG (pId="
							<< pId << ", lsn=" << logRecord.lsn_ << ")");
					GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL,
						GS_TRACE_RM_REDO_LOG_STATUS,
						"--recovery-target-partition is specified. Ignore "
						"after DROP_PARTITION_LOG (pId="
							<< pId << ", lsn=" << logRecord.lsn_ << ")");
					break;
				}

				GS_TRACE_INFO(RECOVERY_MANAGER, GS_TRACE_RM_REDO_LOG_STATUS,
					"LOG_TYPE_DROP_PARTITION(pId=" << pId << ", lsn="
												   << logRecord.lsn_ << ")");
				GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL,
					GS_TRACE_RM_REDO_LOG_STATUS,
					"LOG_TYPE_DROP_PARTITION(pId=" << pId << ", lsn="
												   << logRecord.lsn_ << ")");

				ds_->dropPartition(pId);
				txnMgr_->removePartition(pId);

				pt_->setLSN(pId, 0);
				nextLsn = 0;
			}
			break;

		case LogManager::LOG_TYPE_PUT_CONTAINER: {
			GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL, GS_TRACE_RM_REDO_LOG_STATUS,
				"LOG_TYPE_CREATE_CONTAINER(pId="
					<< pId << ", lsn=" << logRecord.lsn_
					<< ", containerId=" << logRecord.containerId_
					<< ", stmtId=" << logRecord.stmtId_
					<< ", containerName=" << logRecord.containerName_
					<< ", containerType=" << logRecord.containerType_ << ")");
			const TransactionManager::ContextSource src(PUT_CONTAINER,
				logRecord.stmtId_, logRecord.containerId_,
				TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL,
				TransactionManager::AUTO, TransactionManager::AUTO_COMMIT);
			TransactionContext &txn = txnMgr_->put(alloc, pId,
				TXN_EMPTY_CLIENTID, src, stmtStartTime, stmtStartEmTime, REDO);

			const DataStore::Latch latch(
				txn, txn.getPartitionId(), ds_, clsSvc_);

			ExtendedContainerName extContainerName(txn.getDefaultAllocator(),
				logRecord.containerName_, logRecord.containerNameLen_);
			DataStore::PutStatus dummy;
			const bool isModifiable = true;
			ContainerAutoPtr containerAutoPtr(txn, ds_, txn.getPartitionId(),
				extContainerName,
				static_cast<uint8_t>(logRecord.containerType_),
				logRecord.containerInfoLen_, logRecord.containerInfo_,
				isModifiable, dummy);

			BaseContainer *container = containerAutoPtr.getBaseContainer();
			assert(container != NULL);
			if (container->getContainerId() != logRecord.containerId_) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_DS_CONTAINER_ID_INVALID,
					"log containerId : " << logRecord.containerId_
										 << ", redo containerId : "
										 << container->getContainerId());
			}
		} break;

		case LogManager::LOG_TYPE_DROP_CONTAINER: {
			GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL, GS_TRACE_RM_REDO_LOG_STATUS,
				"LOG_TYPE_DROP_CONTAINER(pId="
					<< pId << ", lsn=" << logRecord.lsn_
					<< ", containerId=" << logRecord.containerId_
					<< ", stmtId=" << logRecord.stmtId_
					<< ", containerName=" << logRecord.containerName_ << ")");
			const TransactionManager::ContextSource src(DROP_CONTAINER,
				logRecord.stmtId_, logRecord.containerId_,
				TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL,
				TransactionManager::AUTO, TransactionManager::AUTO_COMMIT);
			TransactionContext &txn = txnMgr_->put(alloc, pId,
				TXN_EMPTY_CLIENTID, src, stmtStartTime, stmtStartEmTime, REDO);

			const DataStore::Latch latch(
				txn, txn.getPartitionId(), ds_, clsSvc_);

			ExtendedContainerName extContainerName(txn.getDefaultAllocator(),
				logRecord.containerName_, logRecord.containerNameLen_);
			ds_->dropContainer(
				txn, txn.getPartitionId(), extContainerName, ANY_CONTAINER);
		} break;

		case LogManager::LOG_TYPE_CREATE_INDEX: {
			GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL, GS_TRACE_RM_REDO_LOG_STATUS,
				"LOG_TYPE_CREATE_INDEX(pId="
					<< pId << ", lsn=" << logRecord.lsn_
					<< ", containerId=" << logRecord.containerId_ << ", stmtId="
					<< logRecord.stmtId_ << ", columnId=" << logRecord.columnId_
					<< ", mapType=" << logRecord.mapType_ << ")");
			const TransactionManager::ContextSource src(CREATE_INDEX,
				logRecord.stmtId_, logRecord.containerId_,
				TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL,
				TransactionManager::AUTO, TransactionManager::AUTO_COMMIT);
			TransactionContext &txn = txnMgr_->put(alloc, pId,
				TXN_EMPTY_CLIENTID, src, stmtStartTime, stmtStartEmTime, REDO);

			const DataStore::Latch latch(
				txn, txn.getPartitionId(), ds_, clsSvc_);

			ContainerAutoPtr containerAutoPtr(txn, ds_, txn.getPartitionId(),
				txn.getContainerId(), ANY_CONTAINER);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			assert(container != NULL);

			IndexInfo indexInfo(
				logRecord.columnId_, static_cast<MapType>(logRecord.mapType_));
			container->createIndex(txn, indexInfo);
		} break;

		case LogManager::LOG_TYPE_DROP_INDEX: {
			GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL, GS_TRACE_RM_REDO_LOG_STATUS,
				"LOG_TYPE_DROP_INDEX(pId="
					<< pId << ", lsn=" << logRecord.lsn_
					<< ", containerId=" << logRecord.containerId_ << ", stmtId="
					<< logRecord.stmtId_ << ", columnId=" << logRecord.columnId_
					<< ", mapType=" << logRecord.mapType_ << ")");
			const TransactionManager::ContextSource src(DELETE_INDEX,
				logRecord.stmtId_, logRecord.containerId_,
				TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL,
				TransactionManager::AUTO, TransactionManager::AUTO_COMMIT);
			TransactionContext &txn = txnMgr_->put(alloc, pId,
				TXN_EMPTY_CLIENTID, src, stmtStartTime, stmtStartEmTime, REDO);

			const DataStore::Latch latch(
				txn, txn.getPartitionId(), ds_, clsSvc_);

			ContainerAutoPtr containerAutoPtr(txn, ds_, txn.getPartitionId(),
				txn.getContainerId(), ANY_CONTAINER);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			assert(container != NULL);

			IndexInfo indexInfo(
				logRecord.columnId_, static_cast<MapType>(logRecord.mapType_));
			container->dropIndex(txn, indexInfo);
		} break;

		case LogManager::LOG_TYPE_CREATE_TRIGGER: {
			GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL, GS_TRACE_RM_REDO_LOG_STATUS,
				"LOG_TYPE_CREATE_TRIGGER(pId="
					<< pId << ", lsn=" << logRecord.lsn_
					<< ", containerId=" << logRecord.containerId_
					<< ", stmtId=" << logRecord.stmtId_
					<< ", triggerName=" << logRecord.containerName_ << ")");
			const TransactionManager::ContextSource src(CREATE_TRIGGER,
				logRecord.stmtId_, logRecord.containerId_,
				TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL,
				TransactionManager::AUTO, TransactionManager::AUTO_COMMIT);
			TransactionContext &txn = txnMgr_->put(alloc, pId,
				TXN_EMPTY_CLIENTID, src, stmtStartTime, stmtStartEmTime, REDO);

			const DataStore::Latch latch(
				txn, txn.getPartitionId(), ds_, clsSvc_);

			ContainerAutoPtr containerAutoPtr(txn, ds_, txn.getPartitionId(),
				txn.getContainerId(), ANY_CONTAINER);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			assert(container != NULL);

			TriggerInfo info(alloc);
			TriggerInfo::decode(logRecord.containerInfo_, info);
			container->putTrigger(txn, info);
		} break;

		case LogManager::LOG_TYPE_DROP_TRIGGER: {
			GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL, GS_TRACE_RM_REDO_LOG_STATUS,
				"LOG_TYPE_DROP_TRIGGER(pId="
					<< pId << ", lsn=" << logRecord.lsn_
					<< ", containerId=" << logRecord.containerId_
					<< ", stmtId=" << logRecord.stmtId_
					<< ", triggerName=" << logRecord.containerName_ << ")");
			const TransactionManager::ContextSource src(DELETE_TRIGGER,
				logRecord.stmtId_, logRecord.containerId_,
				TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL,
				TransactionManager::AUTO, TransactionManager::AUTO_COMMIT);
			TransactionContext &txn = txnMgr_->put(alloc, pId,
				TXN_EMPTY_CLIENTID, src, stmtStartTime, stmtStartEmTime, REDO);

			const DataStore::Latch latch(
				txn, txn.getPartitionId(), ds_, clsSvc_);

			ContainerAutoPtr containerAutoPtr(txn, ds_, txn.getPartitionId(),
				txn.getContainerId(), ANY_CONTAINER);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			assert(container != NULL);

			const std::string triggerName(
				logRecord.containerName_, logRecord.containerNameLen_);
			container->deleteTrigger(txn, triggerName.c_str());
		} break;

		case LogManager::LOG_TYPE_PUT_ROW: {
			const TransactionManager::ContextSource src(PUT_ROW,
				logRecord.stmtId_, logRecord.containerId_,
				logRecord.txnTimeout_,
				txnMgr_->getContextGetModeForRecovery(
					static_cast<TransactionManager::GetMode>(
						logRecord.txnContextCreationMode_)),
				txnMgr_->getTransactionModeForRecovery(
					logRecord.withBegin_, logRecord.isAutoCommit_));
			GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL, GS_TRACE_RM_REDO_LOG_STATUS,
				"LOG_TYPE_PUT_ROW(pId="
					<< pId << ", lsn=" << logRecord.lsn_
					<< ", clientId=" << logRecord.clientId_ << ", containerId="
					<< logRecord.containerId_ << ", txnId=" << logRecord.txnId_
					<< ", stmtId=" << logRecord.stmtId_
					<< ", getMode=" << TM_OUTPUT_GETMODE(src.getMode_)
					<< ", txnMode=" << TM_OUTPUT_TXNMODE(src.txnMode_)
					<< ", numRow=" << logRecord.numRow_
					<< ", dataLen=" << logRecord.dataLen_
					<< ", withBegin=" << (int32_t)logRecord.withBegin_
					<< ", isAutoCommit=" << (int32_t)logRecord.isAutoCommit_
					<< ", rowId=" << (logRecord.numRow_ == 1
											 ? reinterpret_cast<const RowId *>(
												   logRecord.rowIds_)[0]
											 : -1)
					<< ")");
			TransactionContext &txn =
				txnMgr_->put(alloc, pId, logRecord.clientId_, src,
					stmtStartTime, stmtStartEmTime, REDO, logRecord.txnId_);
			assert(txn.getId() == logRecord.txnId_);
			assert(txn.getContainerId() == logRecord.containerId_);

			const DataStore::Latch latch(
				txn, txn.getPartitionId(), ds_, clsSvc_);

			ContainerAutoPtr containerAutoPtr(txn, ds_, txn.getPartitionId(),
				txn.getContainerId(), ANY_CONTAINER);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			assert(container != NULL);

			if (logRecord.numRow_ > 1) {
				DataStore::PutStatus status;  
				container->putRowList(txn, logRecord.dataLen_,
					logRecord.rowData_, logRecord.numRow_, status);
			}
			else if (logRecord.numRow_ == 1) {
				RowId rowId;				  
				DataStore::PutStatus status;  
				container->putRow(txn, logRecord.dataLen_, logRecord.rowData_,
					rowId, status, PUT_INSERT_OR_UPDATE);
				if (logRecord.numRowId_ == 1) {
					assert(rowId == reinterpret_cast<const RowId *>(
										logRecord.rowIds_)[0]);
				}
			}
			else {
			}

			txnMgr_->update(txn, logRecord.stmtId_);

			removeTransaction(mode, txn);
		} break;

		case LogManager::LOG_TYPE_UPDATE_ROW: {
			const TransactionManager::ContextSource src(UPDATE_ROW_BY_ID,
				logRecord.stmtId_, logRecord.containerId_,
				logRecord.txnTimeout_,
				txnMgr_->getContextGetModeForRecovery(
					static_cast<TransactionManager::GetMode>(
						logRecord.txnContextCreationMode_)),
				txnMgr_->getTransactionModeForRecovery(
					logRecord.withBegin_, logRecord.isAutoCommit_));
			GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL, GS_TRACE_RM_REDO_LOG_STATUS,
				"LOG_TYPE_UPDATE_ROW(pId="
					<< pId << ", lsn=" << logRecord.lsn_
					<< ", clientId=" << logRecord.clientId_ << ", containerId="
					<< logRecord.containerId_ << ", txnId=" << logRecord.txnId_
					<< ", stmtId=" << logRecord.stmtId_
					<< ", getMode=" << TM_OUTPUT_GETMODE(src.getMode_)
					<< ", txnMode=" << TM_OUTPUT_TXNMODE(src.txnMode_)
					<< ", numRow=" << logRecord.numRow_
					<< ", dataLen=" << logRecord.dataLen_ << ")");
			TransactionContext &txn =
				txnMgr_->put(alloc, pId, logRecord.clientId_, src,
					stmtStartTime, stmtStartEmTime, REDO, logRecord.txnId_);
			assert(txn.getId() == logRecord.txnId_);
			assert(txn.getContainerId() == logRecord.containerId_);

			const DataStore::Latch latch(
				txn, txn.getPartitionId(), ds_, clsSvc_);

			ContainerAutoPtr containerAutoPtr(txn, ds_, txn.getPartitionId(),
				txn.getContainerId(), ANY_CONTAINER);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			assert(container != NULL);
			assert(logRecord.numRow_ == logRecord.numRowId_);

			for (uint64_t i = 0; i < logRecord.numRowId_; i++) {
				const RowId rowId =
					reinterpret_cast<const RowId *>(logRecord.rowIds_)[i];
				DataStore::PutStatus status;
				container->updateRow(
					txn, logRecord.dataLen_, logRecord.rowData_, rowId, status);
				assert(status == DataStore::UPDATE);
			}

			txnMgr_->update(txn, logRecord.stmtId_);

			removeTransaction(mode, txn);
		} break;

		case LogManager::LOG_TYPE_REMOVE_ROW: {
			const TransactionManager::ContextSource src(REMOVE_ROW_BY_ID,
				logRecord.stmtId_, logRecord.containerId_,
				logRecord.txnTimeout_,
				txnMgr_->getContextGetModeForRecovery(
					static_cast<TransactionManager::GetMode>(
						logRecord.txnContextCreationMode_)),
				txnMgr_->getTransactionModeForRecovery(
					logRecord.withBegin_, logRecord.isAutoCommit_));
			GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL, GS_TRACE_RM_REDO_LOG_STATUS,
				"LOG_TYPE_REMOVE_ROW(pId="
					<< pId << ", lsn=" << logRecord.lsn_
					<< ", clientId=" << logRecord.clientId_ << ", containerId="
					<< logRecord.containerId_ << ", txnId=" << logRecord.txnId_
					<< ", stmtId=" << logRecord.stmtId_
					<< ", getMode=" << TM_OUTPUT_GETMODE(src.getMode_)
					<< ", txnMode=" << TM_OUTPUT_TXNMODE(src.txnMode_)
					<< ", numRow=" << logRecord.numRowId_ << ")");
			TransactionContext &txn =
				txnMgr_->put(alloc, pId, logRecord.clientId_, src,
					stmtStartTime, stmtStartEmTime, REDO, logRecord.txnId_);
			assert(txn.getId() == logRecord.txnId_);
			assert(txn.getContainerId() == logRecord.containerId_);

			const DataStore::Latch latch(
				txn, txn.getPartitionId(), ds_, clsSvc_);

			ContainerAutoPtr containerAutoPtr(txn, ds_, txn.getPartitionId(),
				txn.getContainerId(), ANY_CONTAINER);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			assert(container != NULL);
			assert(logRecord.numRow_ == logRecord.numRowId_);

			for (uint64_t i = 0; i < logRecord.numRowId_; i++) {
				const RowId rowId =
					reinterpret_cast<const RowId *>(logRecord.rowIds_)[i];
				bool dummy;
				container->redoDeleteRow(txn, rowId, dummy);
			}

			txnMgr_->update(txn, logRecord.stmtId_);

			removeTransaction(mode, txn);
		} break;

		case LogManager::LOG_TYPE_LOCK_ROW: {
			const TransactionManager::ContextSource src(GET_ROW,
				logRecord.stmtId_, logRecord.containerId_,
				logRecord.txnTimeout_,
				txnMgr_->getContextGetModeForRecovery(
					static_cast<TransactionManager::GetMode>(
						logRecord.txnContextCreationMode_)),
				txnMgr_->getTransactionModeForRecovery(
					logRecord.withBegin_, logRecord.isAutoCommit_));
			GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL, GS_TRACE_RM_REDO_LOG_STATUS,
				"LOG_TYPE_LOCK_ROW(pId="
					<< pId << ", lsn=" << logRecord.lsn_
					<< ", clientId=" << logRecord.clientId_ << ", containerId="
					<< logRecord.containerId_ << ", txnId=" << logRecord.txnId_
					<< ", stmtId=" << logRecord.stmtId_
					<< ", getMode=" << TM_OUTPUT_GETMODE(src.getMode_)
					<< ", txnMode=" << TM_OUTPUT_TXNMODE(src.txnMode_)
					<< ", numRow=" << logRecord.numRowId_ << ")");
			TransactionContext &txn =
				txnMgr_->put(alloc, pId, logRecord.clientId_, src,
					stmtStartTime, stmtStartEmTime, REDO, logRecord.txnId_);
			assert(txn.getId() == logRecord.txnId_);
			assert(txn.getContainerId() == logRecord.containerId_);

			const DataStore::Latch latch(
				txn, txn.getPartitionId(), ds_, clsSvc_);

			ContainerAutoPtr containerAutoPtr(txn, ds_, txn.getPartitionId(),
				txn.getContainerId(), ANY_CONTAINER);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			assert(container != NULL);

			util::XArray<RowId> lockedRowIds(alloc);
			lockedRowIds.assign(logRecord.numRowId_, UNDEF_ROWID);
			memcpy(lockedRowIds.data(), logRecord.rowIds_,
				sizeof(RowId) * logRecord.numRowId_);
			container->lockRowList(txn, lockedRowIds);

			txnMgr_->update(txn, logRecord.stmtId_);

			removeTransaction(mode, txn);
		} break;

		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_REPLICATION_LOG_TYPE_INVALID,
				"(pId=" << pId << ", lsn=" << logRecord.lsn_
						<< ", type=" << logRecord.type_ << ")");
		}

		return nextLsn;
	}
	catch (std::exception &e) {
		RM_RETHROW_LOG_REDO_ERROR(
			e, "(pId=" << pId << ", lsn=" << logRecord.lsn_
					   << ", type=" << logRecord.type_
					   << ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Removes transactions for after redoing.
*/
void RecoveryManager::removeTransaction(Mode mode, TransactionContext &txn) {
	switch (mode) {
	case MODE_REPLICATION:
	case MODE_SHORT_TERM_SYNC:
		break;

	case MODE_LONG_TERM_SYNC:
	case MODE_RECOVERY:
		if (!txn.isActive()) {
			txnMgr_->remove(txn.getPartitionId(), txn.getClientId());
		}
		break;

	default:
		GS_THROW_USER_ERROR(
			GS_ERROR_RM_REDO_MODE_INVALID, "(mode=" << mode << ")");
	}
}

/*!
	@brief Outputs all data of Log files to the standard I/O.
*/
void RecoveryManager::dumpLogFile(
	util::StackAllocator &alloc, PartitionGroupId pgId, CheckpointId cpId) {
	try {
		util::StackAllocator::Scope groupScope(alloc);

		std::cerr << "[" << (pgId + 1) << "/"
				  << pgConfig_.getPartitionGroupCount() << "]...";

		const PartitionId beginPId = pgConfig_.getGroupBeginPartitionId(pgId);
		const PartitionId endPId = pgConfig_.getGroupEndPartitionId(pgId);

		LogCursor cursor;
		logMgr_->findCheckpointStartLog(cursor, pgId, cpId);

		util::XArray<uint64_t> numRedoLogRecord(alloc);
		numRedoLogRecord.assign(pgConfig_.getPartitionCount(), 0);

		util::XArray<uint64_t> numAfterDropLogs(alloc);
		numAfterDropLogs.assign(pgConfig_.getPartitionCount(), 0);

		const uint32_t traceIntervalMillis = 15000;
		util::Stopwatch traceTimer(util::Stopwatch::STATUS_STARTED);

		LogRecord logRecord;
		util::XArray<uint8_t> binaryLogRecord(alloc);

		for (uint64_t logCount = 0;; logCount++) {
			binaryLogRecord.clear();
			if (!cursor.nextLog(logRecord, binaryLogRecord)) {
				break;
			}

			if (traceTimer.elapsedMillis() >= traceIntervalMillis) {
				const LogCursor::Progress progress = cursor.getProgress();

				std::cerr << "Log dump working ("
							 "pgId="
						  << progress.pgId_ << ", cpId=" << progress.cpId_
						  << ", fileOffset=" << progress.offset_ << "/"
						  << progress.fileSize_ << "("
						  << (static_cast<double>(progress.offset_) * 100 /
								 progress.fileSize_)
						  << "%)"
						  << ", logCount=" << logCount << ")" << std::endl;

				traceTimer.reset();
				traceTimer.start();
			}

			const LogSequentialNumber lsn = logRecord.lsn_;
			const PartitionId pId = logRecord.partitionId_;

			if (!(beginPId <= pId && pId < endPId)) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_PARTITION_NUM_NOT_MATCH,
					"Invalid PartitionId: "
						<< pId << ", config partitionNum="
						<< pgConfig_.getPartitionCount() << ", pgId=" << pgId
						<< ", beginPId=" << beginPId << ", endPId=" << endPId);
			}
			const LogSequentialNumber prevLsn =
				logMgr_->getLSN(logRecord.partitionId_);
			const bool lsnAssignable = LogManager::isLsnAssignable(
				static_cast<uint8_t>(logRecord.type_));

			if (!(prevLsn == 0 || (lsnAssignable && lsn == prevLsn + 1) ||
					(!lsnAssignable && lsn == prevLsn))) {
				std::cerr << "Invalid LSN (pId=" << logRecord.partitionId_
						  << ", prevLsn=" << prevLsn << ", targetLsn=" << lsn
						  << ", lsnAssignable=" << lsnAssignable << ", logType="
						  << LogManager::logTypeToString(
								 static_cast<LogManager::LogType>(
									 logRecord.type_))
						  << ", logFileVersion="
						  << static_cast<uint32_t>(logRecord.fileVersion_)
						  << ")" << std::endl;
			}

			if (prevLsn > lsn) {
				std::cerr << "LSN jumped (pId=" << pId << ", lsn=" << lsn
						  << ", prevLsn=" << prevLsn << ")" << std::endl;
			}

			std::string dumpString;
			LogSequentialNumber nextLsn = 0;
			logRecordToString(
				logRecord.partitionId_, logRecord, dumpString, nextLsn);
			std::cout << dumpString.c_str() << std::endl;

			logMgr_->setLSN(logRecord.partitionId_, nextLsn);

			if (logRecord.type_ == LogManager::LOG_TYPE_DROP_PARTITION) {
				numAfterDropLogs[pId]++;
			}
			numRedoLogRecord[logRecord.partitionId_]++;
		}

		for (size_t i = 0; i < numAfterDropLogs.size(); i++) {
			if (numAfterDropLogs[i] <= 0) {
				continue;
			}

			const PartitionId pId = static_cast<PartitionId>(i);
			std::cerr << "One or more logs after drop log ignored (pId=" << pId
					  << ", numLogs=" << numAfterDropLogs[i] << ")"
					  << std::endl;
		}

		for (size_t i = 0; i < numRedoLogRecord.size(); i++) {
			if (numRedoLogRecord[i] <= 0) {
				continue;
			}

			const PartitionId pId = static_cast<PartitionId>(i);
			const LogSequentialNumber lsn = logMgr_->getLSN(pId);
			std::cerr << "Dump finished (pId=" << pId << ", lsn=" << lsn
					  << ", numRedoLogRecord=" << numRedoLogRecord[i] << ")"
					  << std::endl;
		}

		std::cout << " done." << std::endl;
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e,
			"Dump log file failed. (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Converts a LogRecord to a string.
*/
void RecoveryManager::logRecordToString(PartitionId pId,
	const LogRecord &logRecord, std::string &dumpString,
	LogSequentialNumber &nextLsn) {
	util::NormalOStringStream ss;

	nextLsn = logRecord.lsn_;
	switch (logRecord.type_) {
	case LogManager::LOG_TYPE_CHECKPOINT_START:
		ss << "LOG_TYPE_CHECKPOINT_START, pId=" << pId
		   << ", lsn=" << logRecord.lsn_ << ", maxTxnId=" << logRecord.txnId_
		   << ", numTxn=" << logRecord.txnContextNum_;
		break;

	case LogManager::LOG_TYPE_CHECKPOINT_END:
		ss << "LOG_TYPE_CHECKPOINT_END, pId=" << pId
		   << ", lsn=" << logRecord.lsn_ << ", dataLen=" << logRecord.dataLen_;
		break;

	case LogManager::LOG_TYPE_SHUTDOWN:
		ss << "LOG_TYPE_SHUTDOWN, pId=" << pId << ", lsn=" << logRecord.lsn_
		   << ", dataLen=" << logRecord.dataLen_;
		break;

	case LogManager::LOG_TYPE_CHUNK_META_DATA: {
		ss << "LOG_TYPE_CHUNK_META_DATA, pId=" << pId
		   << ", lsn=" << logRecord.lsn_
		   << ", chunkCategoryId=" << (int32_t)logRecord.chunkCategoryId_
		   << ", startChunkId=" << logRecord.startChunkId_
		   << ", chunkNum=" << logRecord.chunkNum_
		   << ", dataLen=" << logRecord.dataLen_;
	} break;

	case LogManager::LOG_TYPE_PUT_CHUNK_START: {
		ss << "LOG_TYPE_PUT_CHUNK_START, pId=" << pId
		   << ", lsn=" << logRecord.lsn_
		   << ", putChunkId=" << logRecord.containerId_
		   << ", chunkNum=" << logRecord.chunkNum_;
	} break;

	case LogManager::LOG_TYPE_PUT_CHUNK_DATA: {
		ss << "LOG_TYPE_PUT_CHUNK_DATA, pId=" << pId
		   << ", lsn=" << logRecord.lsn_
		   << ", putChunkId=" << logRecord.containerId_
		   << ", dataLen=" << logRecord.dataLen_;
	} break;

	case LogManager::LOG_TYPE_PUT_CHUNK_END: {
		ss << "LOG_TYPE_PUT_CHUNK_END, pId=" << pId
		   << ", lsn=" << logRecord.lsn_
		   << ", putChunkId=" << logRecord.containerId_;
	} break;

	case LogManager::LOG_TYPE_COMMIT:
		ss << "LOG_TYPE_COMMIT, pId=" << pId << ", lsn=" << logRecord.lsn_
		   << ", containerId=" << logRecord.containerId_
		   << ", clientId=" << logRecord.clientId_
		   << ", txnId=" << logRecord.txnId_
		   << ", stmtId=" << logRecord.stmtId_;
		break;

	case LogManager::LOG_TYPE_ABORT:
		ss << "LOG_TYPE_ABORT, pId=" << pId << ", lsn=" << logRecord.lsn_
		   << ", containerId=" << logRecord.containerId_
		   << ", clientId=" << logRecord.clientId_
		   << ", txnId=" << logRecord.txnId_
		   << ", stmtId=" << logRecord.stmtId_;
		break;

	case LogManager::LOG_TYPE_DROP_PARTITION:
		ss << "LOG_TYPE_DROP_PARTITION, pId=" << pId
		   << ", lsn=" << logRecord.lsn_;
		nextLsn = 0;
		break;

	case LogManager::LOG_TYPE_PUT_CONTAINER:
		ss << "LOG_TYPE_PUT_CONTAINER, pId=" << pId
		   << ", lsn=" << logRecord.lsn_
		   << ", containerId=" << logRecord.containerId_
		   << ", clientId=" << logRecord.clientId_
		   << ", txnId=" << logRecord.txnId_ << ", stmtId=" << logRecord.stmtId_
		   << ", containerName=" << logRecord.containerName_;
		break;

	case LogManager::LOG_TYPE_DROP_CONTAINER:
		ss << "LOG_TYPE_DROP_CONTAINER, pId=" << pId
		   << ", lsn=" << logRecord.lsn_
		   << ", containerId=" << logRecord.containerId_
		   << ", clientId=" << logRecord.clientId_
		   << ", txnId=" << logRecord.txnId_ << ", stmtId=" << logRecord.stmtId_
		   << ", containerName=" << logRecord.containerName_;
		break;

	case LogManager::LOG_TYPE_CREATE_INDEX:
		ss << "LOG_TYPE_CREATE_INDEX, pId=" << pId << ", lsn=" << logRecord.lsn_
		   << ", containerId=" << logRecord.containerId_
		   << ", clientId=" << logRecord.clientId_
		   << ", txnId=" << logRecord.txnId_ << ", stmtId=" << logRecord.stmtId_
		   << ", columnId=" << logRecord.columnId_
		   << ", mapType=" << logRecord.mapType_;
		break;

	case LogManager::LOG_TYPE_DROP_INDEX:
		ss << "LOG_TYPE_DROP_INDEX, pId=" << pId << ", lsn=" << logRecord.lsn_
		   << ", containerId=" << logRecord.containerId_
		   << ", clientId=" << logRecord.clientId_
		   << ", txnId=" << logRecord.txnId_ << ", stmtId=" << logRecord.stmtId_
		   << ", columnId=" << logRecord.columnId_
		   << ", mapType=" << logRecord.mapType_;
		break;

	case LogManager::LOG_TYPE_PUT_ROW:
		ss << "LOG_TYPE_PUT_ROW, pId=" << pId << ", lsn=" << logRecord.lsn_
		   << ", containerId=" << logRecord.containerId_
		   << ", clientId=" << logRecord.clientId_
		   << ", txnId=" << logRecord.txnId_ << ", stmtId=" << logRecord.stmtId_
		   << ", clientId=" << logRecord.clientId_
		   << ", numRowId=" << logRecord.numRowId_
		   << ", numRow=" << logRecord.numRow_
		   << ", dataLen=" << logRecord.dataLen_
		   << ", withBegin=" << (int32_t)logRecord.withBegin_
		   << ", isAutoCommit=" << (int32_t)logRecord.isAutoCommit_;
		break;

	case LogManager::LOG_TYPE_UPDATE_ROW:
		ss << "LOG_TYPE_UPDATE_ROW, pId=" << pId << ", lsn=" << logRecord.lsn_
		   << ", containerId=" << logRecord.containerId_
		   << ", clientId=" << logRecord.clientId_
		   << ", txnId=" << logRecord.txnId_ << ", stmtId=" << logRecord.stmtId_
		   << ", clientId=" << logRecord.clientId_
		   << ", numRowId=" << logRecord.numRowId_
		   << ", numRow=" << logRecord.numRow_
		   << ", dataLen=" << logRecord.dataLen_;
		break;

	case LogManager::LOG_TYPE_REMOVE_ROW:
		ss << "LOG_TYPE_REMOVE_ROW, pId=" << pId << ", lsn=" << logRecord.lsn_
		   << ", containerId=" << logRecord.containerId_
		   << ", clientId=" << logRecord.clientId_
		   << ", txnId=" << logRecord.txnId_ << ", stmtId=" << logRecord.stmtId_
		   << ", clientId=" << logRecord.clientId_
		   << ", numRowId=" << logRecord.numRowId_;
		break;

	case LogManager::LOG_TYPE_LOCK_ROW:
		ss << "LOG_TYPE_LOCK_ROW, pId=" << pId << ", lsn=" << logRecord.lsn_
		   << ", containerId=" << logRecord.containerId_
		   << ", clientId=" << logRecord.clientId_
		   << ", txnId=" << logRecord.txnId_ << ", stmtId=" << logRecord.stmtId_
		   << ", clientId=" << logRecord.clientId_
		   << ", numRowId=" << logRecord.numRowId_;
		break;

	default:
		ss << "LOG_TYPE_UNKNOWN, pId=" << pId << ", lsn=" << logRecord.lsn_
		   << ", type=" << logRecord.type_;
		break;
	}

	dumpString = ss.str();
}

namespace {
/*!
	@brief Checks digit.
*/
struct NoDigitChecker {
	inline bool operator()(char8_t ch) {
		return !('0' <= ch && ch <= '9');
	}
};
}

/*!
	@brief Outputs version of Log files to the standard I/O.
*/
void RecoveryManager::dumpFileVersion(
	util::StackAllocator &alloc, const char8_t *dbPath) {
	LogManager &logManager = *logMgr_;

	try {
		util::StackAllocator::Scope groupScope(alloc);

		if (!util::FileSystem::isDirectory(dbPath)) {
			std::cerr << "Data directory(" << dbPath << ") is not found."
					  << std::endl;
			return;
		}

		std::cout << "log_file_name, version" << std::endl;
		util::Directory dir(dbPath);
		for (u8string fileName; dir.nextEntry(fileName);) {
			if (fileName.find(LogManager::LOG_FILE_BASE_NAME) != 0) {
				continue;
			}

			const u8string::iterator beginIt = fileName.begin();
			u8string::iterator lastIt =
				beginIt + strlen(LogManager::LOG_FILE_BASE_NAME);

			u8string::iterator it =
				std::find_if(lastIt, fileName.end(), NoDigitChecker());
			if (it == fileName.end() || it == lastIt) {
				continue;
			}
			const PartitionGroupId pgId =
				util::LexicalConverter<uint32_t>()(std::string(lastIt, it));
			lastIt = it;

			size_t lastOff = lastIt - beginIt;
			if (fileName.find(LogManager::LOG_FILE_SEPARATOR, lastOff) !=
				lastOff) {
				continue;
			}
			lastIt += strlen(LogManager::LOG_FILE_SEPARATOR);

			it = std::find_if(lastIt, fileName.end(), NoDigitChecker());
			if (it == fileName.end() || it == lastIt) {
				continue;
			}
			const CheckpointId cpId =
				util::LexicalConverter<uint64_t>()(std::string(lastIt, it));
			lastIt = it;

			lastOff = lastIt - beginIt;
			if (fileName.find(LogManager::LOG_FILE_EXTENSION, lastOff) !=
					lastOff ||
				lastIt + strlen(LogManager::LOG_FILE_EXTENSION) !=
					fileName.end()) {
				continue;
			}

			uint16_t version = 0;
			try {
				version = logManager.getFileVersion(pgId, cpId);
			}
			catch (std::exception &e) {
				std::cout << fileName.c_str() << ", 0,"
						  << GS_EXCEPTION_MESSAGE(e) << std::endl;
				continue;
			}
			std::cout << fileName.c_str() << ", " << version << std::endl;
		}
		std::cerr << " done." << std::endl;
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "Dump file version failed. (reason="
									   << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

RecoveryManager::StatSetUpHandler RecoveryManager::statSetUpHandler_;

#define STAT_ADD(id) STAT_TABLE_ADD_PARAM(stat, parentId, id)

void RecoveryManager::StatSetUpHandler::operator()(StatTable &stat) {
	StatTable::ParamId parentId;

	parentId = STAT_TABLE_ROOT;
	stat.resolveGroup(parentId, STAT_TABLE_RM, "recovery");

	parentId = STAT_TABLE_RM;
	STAT_ADD(STAT_TABLE_RM_PROGRESS_RATE);
}

bool RecoveryManager::StatUpdator::operator()(StatTable &stat) {
	if (!stat.getDisplayOption(STAT_TABLE_DISPLAY_SELECT_RM)) {
		return true;
	}

	RecoveryManager &mgr = *manager_;

	if (stat.getDisplayOption(STAT_TABLE_DISPLAY_WEB_ONLY)) {
		stat.set(STAT_TABLE_RM_PROGRESS_RATE, mgr.getProgressRate());
	}

	return true;
}
