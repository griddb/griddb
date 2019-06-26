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
#include "zlib_utils.h"

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
#include "message_schema.h"

UTIL_TRACER_DECLARE(RECOVERY_MANAGER);
UTIL_TRACER_DECLARE(RECOVERY_MANAGER_DETAIL);

#include "picojson.h"
#include <fstream>

#define RM_THROW_LOG_REDO_ERROR(errorCode, message) \
	GS_THROW_CUSTOM_ERROR(LogRedoException, errorCode, message)
#define RM_RETHROW_LOG_REDO_ERROR(cause, message) \
	GS_RETHROW_CUSTOM_ERROR(LogRedoException, GS_ERROR_DEFAULT, cause, message)


const std::string RecoveryManager::BACKUP_INFO_FILE_NAME("gs_backup_info.json");
const std::string RecoveryManager::BACKUP_INFO_DIGEST_FILE_NAME(
	"gs_backup_info_digest.json");

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
RecoveryManager::RecoveryManager(
		ConfigTable &configTable, bool releaseUnusedFileBlocks)
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
	  recoveryTargetPartitionMode_(false),
	  releaseUnusedFileBlocks_(releaseUnusedFileBlocks),
	  chunkMgr_(NULL), logMgr_(NULL), pt_(NULL),
	  clsSvc_(NULL), sysSvc_(NULL), txnMgr_(NULL), ds_(NULL) {
	statUpdator_.manager_ = this;

	backupInfo_.setConfigValue(
		configTable.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM),
		configTable.getUInt32(CONFIG_TABLE_DS_CONCURRENCY));
	backupInfo_.setBackupPath(
		configTable.get<const char8_t *>(CONFIG_TABLE_DS_DB_PATH));
	backupInfo_.setDataStorePath(
		configTable.get<const char8_t *>(CONFIG_TABLE_DS_DB_PATH));
	backupInfo_.setGSVersion(
		configTable.get<const char8_t *>(CONFIG_TABLE_DEV_SIMPLE_VERSION));
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

bool RecoveryManager::checkBackupInfoFileName(const std::string &fileName) {
	std::string::size_type pos = fileName.find(BACKUP_INFO_FILE_NAME);
	if (0 != pos) {
		return false;
	}
	if (BACKUP_INFO_FILE_NAME.length() != fileName.length()) {
		return false;
	}
	return true;
}

bool RecoveryManager::checkBackupInfoDigestFileName(
	const std::string &fileName) {
	std::string::size_type pos = fileName.find(BACKUP_INFO_DIGEST_FILE_NAME);
	if (0 != pos) {
		return false;
	}
	if (BACKUP_INFO_DIGEST_FILE_NAME.length() != fileName.length()) {
		return false;
	}
	return true;
}

void RecoveryManager::removeBackupInfoFile() {
	backupInfo_.removeBackupInfoFile(*logMgr_);
}

/*!
	@brief Checks if files exist.
*/
void RecoveryManager::checkExistingFiles1(ConfigTable &param, bool &createFlag,
	bool &existBackupInfoFile, bool forceRecoveryFromExistingFiles) {
	createFlag = false;
	bool existBackupInfoFile1 = false;
	bool existBackupInfoFile2 = false;
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
			else if (checkBackupInfoFileName(fileName)) {
				existBackupInfoFile1 = true;
			}
			else if (checkBackupInfoDigestFileName(fileName)) {
				existBackupInfoFile2 = true;
			}
			else {
				continue;
			}
		}
		if (existBackupInfoFile1 || existBackupInfoFile2) {
			if (existBackupInfoFile1 && existBackupInfoFile2) {
				existBackupInfoFile = true;
			}
			else {
				if (existBackupInfoFile1) {
					GS_THROW_SYSTEM_ERROR(
						GS_ERROR_RM_READ_BACKUP_INFO_FILE_FAILED,
						"backupInfoFile is exist, but backupInfoDigestFile is "
						"not found.");
				}
				else {
					GS_THROW_SYSTEM_ERROR(
						GS_ERROR_RM_READ_BACKUP_INFO_FILE_FAILED,
						"backupInfoDigestFile is exist, but backupInfoFile is "
						"not found.");
				}
			}
		}
		else {
			existBackupInfoFile = false;
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
			if (existBackupInfoFile) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_BACKUP_WITH_NO_LOG_FILE,
					"Backup info file is exist, but log files are not exist");
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
	LogManager &logMgr, bool &createFlag, bool existBackupInfoFile,
	bool forceRecoveryFromExistingFiles) {
//	bool existBackupInfoFile1 = false;
//	bool existBackupInfoFile2 = false;
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
			else if (checkBackupInfoFileName(fileName)) {
//				existBackupInfoFile1 = true;
			}
			else if (checkBackupInfoDigestFileName(fileName)) {
//				existBackupInfoFile2 = true;
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
			if (existBackupInfoFile) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_BACKUP_WITH_NO_LOG_FILE,
					"Backup info file is exist, but log files are not exist");
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

bool RecoveryManager::readBackupInfoFile() {
	try {
		backupInfo_.readBackupInfoFile();
		return backupInfo_.isIncrementalBackup();
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

bool RecoveryManager::readBackupInfoFile(const std::string &lastBackupPath) {
	try {
		backupInfo_.readBackupInfoFile(lastBackupPath);
		return backupInfo_.isIncrementalBackup();
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Recovers database at the time of starting
*/
void RecoveryManager::recovery(util::StackAllocator &alloc, bool existBackup,
	bool forceRecoveryFromExistingFiles,
	const std::string &recoveryTargetPartition, bool forLongArchive)
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
			bool isIncrementalBackup = backupInfo_.isIncrementalBackup();
			if (existBackup) {
				CheckpointId logCompletedCpId =
					logMgr_->getLastCompletedCheckpointId(pgId);
				if (logCompletedCpId == UNDEF_CHECKPOINT_ID) {
					if (forceRecoveryFromExistingFiles) {
						GS_TRACE_INFO(RECOVERY_MANAGER,
							GS_TRACE_RM_RECOVERY_INFO,
							"Recovery (from backup) skipped (pgId=" << pgId
																	<< ")");
						continue;
					}
					else {
						GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_RECOVERY_FAILED,
							"Backup log file is corrupted. (pgId=" << pgId
																   << ")");
					}
				}
				if (isIncrementalBackup && !forLongArchive) {
					std::vector<CheckpointId> &incrementalBackupCpIdList =
						backupInfo_.getIncrementalBackupCpIdList(pgId);

					if (incrementalBackupCpIdList.size() == 1) {
						isIncrementalBackup = false;
					}
					else {
						std::vector<CheckpointId>::iterator itr =
							incrementalBackupCpIdList.begin();
						for (; itr != incrementalBackupCpIdList.end(); ++itr) {
							if (itr == incrementalBackupCpIdList.begin()) {
								redoChunkLog(alloc, pgId, MODE_RECOVERY, *itr);
							}
							else {
								redoChunkLog(
									alloc, pgId, MODE_RECOVERY_CHUNK, *itr);
							}
						}
					}
					completedCpId = incrementalBackupCpIdList.back();
					GS_TRACE_INFO(RECOVERY_MANAGER, GS_TRACE_RM_RECOVERY_INFO,
						"Recovery (from backup) started (pgId="
							<< pgId << ", completedCpId="
							<< CheckpointIdFormatter(completedCpId)
							<< ", lastCpId=" << CheckpointIdFormatter(lastCpId)
							<< ")");

					recoveryStartCpId = completedCpId;
				}
				else {
					completedCpId = backupInfo_.getBackupCheckpointId(pgId);
					GS_TRACE_INFO(RECOVERY_MANAGER, GS_TRACE_RM_RECOVERY_INFO,
						"Recovery (from backup) started (pgId="
							<< pgId << ", completedCpId="
							<< CheckpointIdFormatter(completedCpId)
							<< ", lastCpId=" << CheckpointIdFormatter(lastCpId)
							<< ")");

					recoveryStartCpId = completedCpId;

					uint64_t checkpointFileSize =
						chunkMgr_->getChunkManagerStats().getCheckpointFileSize(
							pgId);

					uint64_t infoCpFileSize =
						backupInfo_.getBackupCheckpointFileSize(pgId);
					bool checkCpFileSize = backupInfo_.getCheckCpFileSizeFlag();
					if (checkCpFileSize  
							&& checkpointFileSize != infoCpFileSize) {
						GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_RECOVERY_FAILED,
							"Checkpoint file size is wrong. (pgId="
								<< pgId
								<< ", expectedFileSize=" << infoCpFileSize
								<< ", actualFileSize=" << checkpointFileSize
								<< ")");
					}
				}
			}
			else {

				completedCpId = logMgr_->getLastCompletedCheckpointId(pgId);
				GS_TRACE_INFO(RECOVERY_MANAGER, GS_TRACE_RM_RECOVERY_INFO,
					"Recovery started (pgId="
						<< pgId << ", completedCpId="
						<< CheckpointIdFormatter(completedCpId) << ", lastCpId="
						<< CheckpointIdFormatter(lastCpId) << ")");

				recoveryStartCpId = completedCpId;
			}
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
				if (!isIncrementalBackup) {
					chunkMgr_->resetCheckpointBit(pgId);
				}
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

				redoAll(alloc, MODE_RECOVERY, cursor,
						isIncrementalBackup, forLongArchive);
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

				bool isAllowExpiration = true;
				ContainerAutoPtr containerAutoPtr(txn, ds_,
					txn.getPartitionId(), txn.getContainerId(), ANY_CONTAINER,
					isAllowExpiration);
				BaseContainer *container = containerAutoPtr.getBaseContainer();
				checkContainerExistence(container, txn.getContainerId());

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
				const LogSequentialNumber nextLsn = redoLogRecord(alloc, mode,
					logRecord.partitionId_, redoStartTime, redoStartEmTime,
					logRecord, dummy, false);

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

		if (logMgr_->getArchiveLogMode() != LogManager::ARCHIVE_LOG_DISABLE) {
			logMgr_->setArchiveLogMode(LogManager::ARCHIVE_LOG_ERROR);
		}
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

		logMgr_->findCheckpointStartLog(cursor, pgId, recoveryStartCpId);
		BitArray validBitArray(record.numRow_);
		reconstructValidBitArray(alloc, MODE_RECOVERY, cursor, validBitArray);

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
			oss << "#, isValid, " << chunkMgr_->dumpChunkCSVField();
			oss << std::endl;

			for (uint64_t blockNo = 0; blockNo < blockCount; ++blockNo) {
				if (validBitArray.get(blockNo)) {
					oss << blockNo << ", valid, " << chunkMgr_->dumpChunkCSV(pgId, blockNo);
				} else {
					oss << blockNo << ", invalid, ";
				}
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

void RecoveryManager::reconstructValidBitArray(
		util::StackAllocator &alloc, Mode mode,
		LogCursor &cursor, BitArray &validBitArray) {

	util::XArray<uint64_t> numRedoLogRecord(alloc);
	numRedoLogRecord.assign(pgConfig_.getPartitionCount(), 0);

	util::XArray<uint64_t> numAfterDropLogs(alloc);
	numAfterDropLogs.assign(pgConfig_.getPartitionCount(), 0);

	util::XArray<LogSequentialNumber> redoStartLsn(alloc);
	redoStartLsn.assign(pgConfig_.getPartitionCount(), 0);

//	const uint32_t traceIntervalMillis = 15000;
	util::Stopwatch traceTimer(util::Stopwatch::STATUS_STARTED);

	LogRecord logRecord;
	util::XArray<uint8_t> binaryLogRecord(alloc);

	const PartitionGroupId pgId = cursor.getProgress().pgId_;
	const PartitionId beginPId = pgConfig_.getGroupBeginPartitionId(pgId);
	const PartitionId endPId = pgConfig_.getGroupEndPartitionId(pgId);

//	uint64_t startTime = util::Stopwatch::currentClock();
	for (uint64_t logCount = 0;; logCount++) {
		binaryLogRecord.clear();
		if (!cursor.nextLog(logRecord, binaryLogRecord)) {
			break;
		}
		const PartitionId pId = logRecord.partitionId_;
		if (!(beginPId <= pId && pId < endPId)) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_PARTITION_NUM_NOT_MATCH,
					"Invalid PartitionId: " << pId
					<< ", config partitionNum=" << pgConfig_.getPartitionCount()
					<< ", pgId=" << pgId << ", beginPId="
					<< beginPId << ", endPId=" << endPId);
		}
		const LogSequentialNumber lsn = logRecord.lsn_;

//		const LogSequentialNumber prevLsn =
//			logMgr_->getLSN(logRecord.partitionId_);
//		const bool lsnAssignable =
//			LogManager::isLsnAssignable(static_cast<uint8_t>(logRecord.type_));

		if (numAfterDropLogs[pId] > 0) {
			numAfterDropLogs[pId]++;
			continue;
		}
		if (numRedoLogRecord[pId] == 0) {
			bool checkCpStart = true;

			if (checkCpStart &&
					(logRecord.type_ != LogManager::LOG_TYPE_CHECKPOINT_START)) {
				continue;
			}
			redoStartLsn[pId] = lsn;
		}

		const Timestamp stmtStartTime = 0;
		const EventMonotonicTime stmtStartEmTime = 0;
		try {
			util::StackAllocator::Scope scope(alloc);
			LogSequentialNumber nextLsn = logRecord.lsn_;

//			const KeyConstraint containerKeyConstraint =
//					KeyConstraint::getSystemKeyConstraint(
//						ds_->getValueLimitConfig().getLimitContainerNameSize());

			switch (logRecord.type_) {
				case LogManager::LOG_TYPE_CHECKPOINT_START:
					redoCheckpointStartLog(
							alloc, mode, pId, stmtStartTime,
							stmtStartEmTime, logRecord, redoStartLsn[pId],
							false, false);
					break;

				case LogManager::LOG_TYPE_CHUNK_META_DATA:
					redoChunkMetaDataLog(
							alloc, mode, pId, stmtStartTime,
							stmtStartEmTime, logRecord, redoStartLsn[pId],
							false, false);
					break;
				default:
					break;
			}
			logMgr_->setLSN(logRecord.partitionId_, nextLsn);
			pt_->setLSN(logRecord.partitionId_, nextLsn);

			if (logRecord.type_ == LogManager::LOG_TYPE_DROP_PARTITION) {
				numAfterDropLogs[pId]++;
			}
			numRedoLogRecord[logRecord.partitionId_]++;
		}
		catch (std::exception &e) {
			RM_RETHROW_LOG_REDO_ERROR(
					e, "(pId=" << pId << ", lsn=" << logRecord.lsn_
					<< ", type=" << logRecord.type_
					<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
		}
	}
	validBitArray = chunkMgr_->getCheckpointBit(pgId);
}

/*!
	@brief Redoes from cursor position of Log to the end of Checkpoint file or
   Log.
*/
void RecoveryManager::redoAll(util::StackAllocator &alloc, Mode mode,
	LogCursor &cursor, bool isIncrementalBackup, bool forLongArchive) {

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

	uint64_t startTime = util::Stopwatch::currentClock();
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
		const LogSequentialNumber nextLsn =
			forLongArchive ? 
				redoLogRecordForDataArchive(alloc, mode, logRecord.partitionId_,
					stmtStartTimeOnRecovery, stmtStartTimeOnRecovery, logRecord,
					redoStartLsn[pId], isIncrementalBackup) :
			redoLogRecord(alloc, mode, logRecord.partitionId_,
				stmtStartTimeOnRecovery, stmtStartTimeOnRecovery, logRecord,
				redoStartLsn[pId], isIncrementalBackup);

		logMgr_->setLSN(logRecord.partitionId_, nextLsn);
		pt_->setLSN(logRecord.partitionId_, nextLsn);

		if (logRecord.type_ == LogManager::LOG_TYPE_DROP_PARTITION) {
			numAfterDropLogs[pId]++;
		}
		numRedoLogRecord[logRecord.partitionId_]++;
		const uint64_t currentTime = util::Stopwatch::currentClock();
		if (currentTime - startTime > ADJUST_MEMORY_TIME_INTERVAL_) {
			startTime = currentTime;
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
void RecoveryManager::redoChunkLog(util::StackAllocator &alloc,
	PartitionGroupId pgId, Mode mode, CheckpointId cpId) {
	util::XArray<uint64_t> numRedoLogRecord(alloc);
	numRedoLogRecord.assign(pgConfig_.getPartitionCount(), 0);

	util::XArray<LogSequentialNumber> redoStartLsn(alloc);
	redoStartLsn.assign(pgConfig_.getPartitionCount(), 0);

	if (MODE_RECOVERY == mode) {
		LogCursor cursor;
		util::XArray<uint8_t> data(alloc);
		LogRecord record;

		logMgr_->findCheckpointEndLog(cursor, pgId, cpId);
		if (!cursor.nextLog(record, data) ||
			record.type_ != LogManager::LOG_TYPE_CHECKPOINT_END) {
			GS_THROW_USER_ERROR(GS_ERROR_RM_CHECKPOINT_END_LOG_NOT_FOUND,
				"(pgId=" << pgId << ", recoveryStartCpId="
						 << CheckpointIdFormatter(cpId) << ")");
		}
		chunkMgr_->resetCheckpointBit(pgId);
	}
	chunkMgr_->isValidFileHeader(pgId);

	LogManager::Config srcLogManagerConfig = logMgr_->getConfig();
	LogManager srcLogManager(srcLogManagerConfig);
	if (MODE_RECOVERY == mode) {
		srcLogManager.openPartial(pgId, cpId, NULL);
	}
	else {
		assert(MODE_RECOVERY_CHUNK == mode);
		srcLogManager.openPartial(
			pgId, cpId, CheckpointService::INCREMENTAL_BACKUP_FILE_SUFFIX);
	}

	LogCursor srcLogCursor;
	srcLogManager.findCheckpointStartLog(srcLogCursor, pgId, cpId);

	const PartitionId beginPId = pgConfig_.getGroupBeginPartitionId(pgId);
	const PartitionId endPId = pgConfig_.getGroupEndPartitionId(pgId);

	const uint32_t traceIntervalMillis = 15000;
	util::Stopwatch traceTimer(util::Stopwatch::STATUS_STARTED);

	LogRecord logRecord;
	util::XArray<uint8_t> binaryLogRecord(alloc);
	uint64_t startTime = util::Stopwatch::currentClock();
	while (srcLogCursor.nextLog(logRecord, binaryLogRecord)) {
		const PartitionId pId = logRecord.partitionId_;
		if (!(beginPId <= pId && pId < endPId)) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_PARTITION_NUM_NOT_MATCH,
				"Invalid PartitionId: " << pId << ", config partitionNum="
										<< pgConfig_.getPartitionCount()
										<< ", pgId=" << pgId << ", beginPId="
										<< beginPId << ", endPId=" << endPId);
		}
		if (traceTimer.elapsedMillis() >= traceIntervalMillis) {
			const LogCursor::Progress progress = srcLogCursor.getProgress();

			sysSvc_->traceStats(alloc);
			GS_TRACE_INFO(RECOVERY_MANAGER, GS_TRACE_RM_REDO_LOG_STATUS,
				"Log redo working ("
					<< "pgId=" << progress.pgId_ << ", cpId=" << progress.cpId_
					<< ", fileOffset=" << progress.offset_ << "/"
					<< progress.fileSize_ << "("
					<< (static_cast<double>(progress.offset_) * 100 /
						   progress.fileSize_)
					<< "%)"
					<< ", logCount=" << numRedoLogRecord[pId] << ")");

			traceTimer.reset();
			traceTimer.start();
		}
		if (recoveryPartitionId_[pId] == 0) {
			continue;
		}

		const LogSequentialNumber lsn = logRecord.lsn_;
		const bool lsnAssignable =
			LogManager::isLsnAssignable(static_cast<uint8_t>(logRecord.type_));

		if (numRedoLogRecord[pId] == 0) {
			bool checkCpStart = (MODE_RECOVERY == mode);

			if (checkCpStart &&
				(logRecord.type_ != LogManager::LOG_TYPE_CHECKPOINT_START)) {
				GS_TRACE_DEBUG(RECOVERY_MANAGER, GS_TRACE_RM_REDO_LOG_STATUS,
					"Log before cp/put_container skipped (pId="
						<< logRecord.partitionId_ << ", targetLsn=" << lsn
						<< ", lsnAssignable=" << lsnAssignable
						<< ", logType=" << LogManager::logTypeToString(
											   static_cast<LogManager::LogType>(
												   logRecord.type_))
						<< ")");
				continue;
			}
			redoStartLsn[pId] = lsn;
			GS_TRACE_INFO(RECOVERY_MANAGER, GS_TRACE_RM_REDO_LOG_STATUS,
				"Redo chunk started (pId=" << pId << ", lsn=" << lsn << ")");
		}

		bool metaEnd = redoChunkLogRecord(
			alloc, mode, logRecord.partitionId_, logRecord, redoStartLsn[pId]);

		const uint64_t currentTime = util::Stopwatch::currentClock();
		if (currentTime - startTime > ADJUST_MEMORY_TIME_INTERVAL_) {
			startTime = currentTime;
			chunkMgr_->redistributeMemoryLimit(0);
			for (PartitionGroupId tmpPgId = 0; tmpPgId < pgId; ++tmpPgId) {
				chunkMgr_->adjustPGStoreMemory(tmpPgId);
			}
		}

		numRedoLogRecord[logRecord.partitionId_]++;
		binaryLogRecord.clear();

		if (metaEnd && ((logRecord.partitionId_ + 1) == endPId)) {
			break;
		}
	}
}

/*!
	@brief Redoes LogRecord of Chunks; i.e. applies to DataStore.
*/
bool RecoveryManager::redoChunkLogRecord(util::StackAllocator &alloc, Mode mode,
	PartitionId pId, const LogRecord &logRecord,
	LogSequentialNumber &redoStartLsn) {
	try {
		util::StackAllocator::Scope scope(alloc);


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
					uint16_t logVersion = static_cast<uint16_t>(logRecord.txnId_);
					if (logVersion == 0) {
						logVersion = logMgr_->getBaseVersion();
					}
					assert(LogManager::isAcceptableVersion(logVersion));
					logMgr_->setLogVersion(pId, logVersion);
					if (logRecord.chunkNum_ > 0) {
						GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL,
								GS_TRACE_RM_REDO_LOG_STATUS,
								"(Incremental Backup)LOG_TYPE_CHUNK_META_DATA: "
								"complete.(pId="
								<< pId << ", lsn=" << logRecord.lsn_
								<< ", redoStartLsn=" << redoStartLsn << ")");
					}
					else {
						GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL,
								GS_TRACE_RM_REDO_LOG_STATUS,
								"(Incremental Backup)LOG_TYPE_CHUNK_META_DATA: "
								"non_ready partition.(pId="
								<< pId << ", lsn=" << logRecord.lsn_
								<< ", redoStartLsn=" << redoStartLsn << ")");
					}
					redoStartLsn =
						UNDEF_LSN;  
					return true;
					break;
				}
				recoveryChunkMetaData(alloc, logRecord);
			}
			else {
				GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL,
					GS_TRACE_RM_REDO_LOG_STATUS,
					"LOG_TYPE_CHUNK_META_DATA: skipped.(pId="
						<< pId << ", lsn=" << logRecord.lsn_
						<< ", redoStartLsn=" << redoStartLsn << ")");
			}
			break;

		case LogManager::LOG_TYPE_PUT_CHUNK_START:
			if (mode == MODE_RECOVERY_CHUNK) {
				syncChunkPId_[pgId] = pId;
				syncChunkNum_[pgId] = logRecord.numRow_;
				syncChunkCount_[pgId] = 0;

				GS_TRACE_INFO(RECOVERY_MANAGER, GS_TRACE_RM_REDO_LOG_STATUS,
					"LOG_TYPE_PUT_CHUNK_START: (pId="
						<< pId << ", lsn=" << logRecord.lsn_
						<< ", chunkNum=" << logRecord.numRow_ << ")");
			}
			break;
		case LogManager::LOG_TYPE_PUT_CHUNK_DATA:
			if (mode == MODE_RECOVERY_CHUNK) {
				chunkMgr_->recoveryChunk(
					pId, logRecord.rowData_, logRecord.dataLen_, true);
				++syncChunkCount_[pgId];

				GS_TRACE_DEBUG(RECOVERY_MANAGER_DETAIL,
					GS_TRACE_RM_REDO_LOG_STATUS,
					"PUT_CHUNK_DATA(RecoveryChunk) (pId,"
						<< logRecord.partitionId_ << ",chunkCount,"
						<< syncChunkCount_[pgId]);
			}
			break;
		case LogManager::LOG_TYPE_PUT_CHUNK_END:
			if (mode == MODE_RECOVERY_CHUNK) {
				GS_TRACE_DEBUG(RECOVERY_MANAGER_DETAIL,
					GS_TRACE_RM_REDO_LOG_STATUS,
					"PUT_CHUNK_END(RecoveryChunk) (pId,"
						<< logRecord.partitionId_ << ",chunkCount,"
						<< syncChunkCount_[pgId] << ",expectedCount,"
						<< syncChunkNum_[pgId]);
				if (syncChunkNum_[pgId] != syncChunkCount_[pgId]) {
					GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_INVALID_CHUNK_DATA,
						"Chunk Data Num is wrong: log chunkDataNum="
							<< syncChunkNum_[pgId]
							<< ", chunkDataCount=" << syncChunkCount_[pgId]);
				}
			}
			break;

		default:
			break;
		}

		return false;
	}
	catch (std::exception &e) {
		RM_RETHROW_LOG_REDO_ERROR(
			e, "(pId=" << pId << ", lsn=" << logRecord.lsn_
				<< ", type=" << logRecord.type_
				<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void RecoveryManager::recoveryChunkMetaData(
		util::StackAllocator &alloc, const LogRecord &logRecord) {
	const uint8_t *addr = logRecord.rowData_;
	const PartitionGroupId currentPgId =
			pgConfig_.getPartitionGroupId(logRecord.partitionId_);
	uint64_t startTime = util::Stopwatch::currentClock();

	util::XArray<uint8_t> dataBuffer(alloc);
	uint32_t expandedSize = static_cast<uint32_t>(logRecord.expandedLen_);
	if (expandedSize > 0) {
		uint8_t temp = 0;
		dataBuffer.assign(expandedSize, temp);
		try {
			ZlibUtils zlib;
			zlib.uncompressData(
					addr, logRecord.dataLen_,
					dataBuffer.data(), expandedSize);
			assert(expandedSize == logRecord.expandedLen_);
			addr = dataBuffer.data();
		}
		catch(std::exception &e) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_INVALID_CHUNK_DATA, 
					"Expand chunk meta data failed. (reason="
					<< GS_EXCEPTION_MESSAGE(e) << ")");
		}
	}
	util::XArray<uint8_t> emptySizeList(alloc);
	util::XArray<uint64_t> fileOffsetList(alloc);
	util::XArray<ChunkKey> chunkKeyList(alloc);

	emptySizeList.reserve(logRecord.chunkNum_);
	for (int32_t chunkId = logRecord.startChunkId_;
			chunkId < logRecord.startChunkId_ + logRecord.chunkNum_;
			chunkId++) {
		emptySizeList.push_back(*addr);
		++addr;
	}
	fileOffsetList.reserve(logRecord.chunkNum_);
	int64_t prevOffset = 0;
	int32_t prevChunkKey = 0;
	for (int32_t chunkId = logRecord.startChunkId_;
			chunkId < logRecord.startChunkId_ + logRecord.chunkNum_;
			chunkId++) {
		uint64_t zigzagDiff;
		addr += util::varIntDecode64(addr, zigzagDiff);
		int64_t diff = util::zigzagDecode64(zigzagDiff);
		int64_t fileBlockOffset = prevOffset + diff;
		fileOffsetList.push_back(fileBlockOffset);
		prevOffset = fileBlockOffset;
	}
	const bool isBatchFreeMode =
			chunkMgr_->isBatchFreeMode(logRecord.chunkCategoryId_);
	if (isBatchFreeMode) {
		chunkKeyList.reserve(logRecord.chunkNum_);
		for (int32_t chunkId = logRecord.startChunkId_;
				chunkId < logRecord.startChunkId_ + logRecord.chunkNum_;
				chunkId++) {
			uint32_t zigzagDiff;
			addr += util::varIntDecode32(addr, zigzagDiff);
			int32_t diff = util::zigzagDecode32(zigzagDiff);
			int32_t chunkKey = prevChunkKey + diff;
			chunkKeyList.push_back(chunkKey);
			prevChunkKey = chunkKey;
		}
	}
	for (int32_t pos = 0; pos < logRecord.chunkNum_; pos++) {
		const int32_t chunkId = pos + logRecord.startChunkId_;
		ChunkKey chunkKey = 0;
		const uint8_t unoccupiedSize = emptySizeList[pos];
		const uint64_t filePos = fileOffsetList[pos];
		if (unoccupiedSize != 0xff) {
			if (isBatchFreeMode) {
				chunkKey = chunkKeyList[pos];
			}
			chunkMgr_->recoveryChunk(
					logRecord.partitionId_,
					logRecord.chunkCategoryId_, chunkId, chunkKey,
					unoccupiedSize, filePos);
			const uint64_t currentTime = util::Stopwatch::currentClock();
			if (currentTime - startTime > ADJUST_MEMORY_TIME_INTERVAL_) {
				startTime = currentTime;

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

void RecoveryManager::redoCheckpointStartLog(util::StackAllocator &alloc,
	Mode mode, PartitionId pId, const util::DateTime &stmtStartTime,
	EventMonotonicTime stmtStartEmTime, const LogRecord &logRecord,
		LogSequentialNumber &redoStartLsn, bool isIncrementalBackup,
		bool forLongArchive) {
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
		if (!forLongArchive) {
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
			}
			else {
				GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL,
					GS_TRACE_RM_REDO_LOG_STATUS,
					"LOG_TYPE_CP_START: skipped.(pId="
						<< pId << ", lsn=" << logRecord.lsn_
						<< ", redoStartLsn=" << redoStartLsn << ")");
			}
}


void RecoveryManager::redoChunkMetaDataLog(util::StackAllocator &alloc,
		Mode mode, PartitionId pId, const util::DateTime &stmtStartTime,
		EventMonotonicTime stmtStartEmTime, const LogRecord &logRecord,
		LogSequentialNumber &redoStartLsn, bool isIncrementalBackup,
		bool forLongArchive) {
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
			return;
				}
		if (!isIncrementalBackup || forLongArchive) {
					recoveryChunkMetaData(alloc, logRecord);
				}
			}
			else {
				GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL,
					GS_TRACE_RM_REDO_LOG_STATUS,
					"LOG_TYPE_CHUNK_META_DATA: skipped.(pId="
						<< pId << ", lsn=" << logRecord.lsn_
						<< ", redoStartLsn=" << redoStartLsn << ")");
			}
}

LogSequentialNumber RecoveryManager::redoLogRecordForDataArchive(util::StackAllocator &alloc,
	Mode mode, PartitionId pId, const util::DateTime &stmtStartTime,
	EventMonotonicTime stmtStartEmTime, const LogRecord &logRecord,
	LogSequentialNumber &redoStartLsn, bool isIncrementalBackup) {

	try {
		util::StackAllocator::Scope scope(alloc);
		LogSequentialNumber nextLsn = logRecord.lsn_;

		switch (logRecord.type_) {
		case LogManager::LOG_TYPE_CHECKPOINT_START:
			redoCheckpointStartLog(
					alloc, mode, pId, stmtStartTime,
					stmtStartEmTime, logRecord, redoStartLsn,
					isIncrementalBackup, true);
			break;

		case LogManager::LOG_TYPE_CHUNK_META_DATA:
			redoChunkMetaDataLog(
					alloc, mode, pId, stmtStartTime,
					stmtStartEmTime, logRecord, redoStartLsn,
					isIncrementalBackup, true);
			break;
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

LogSequentialNumber RecoveryManager::redoLogRecord(util::StackAllocator &alloc,
	Mode mode, PartitionId pId, const util::DateTime &stmtStartTime,
	EventMonotonicTime stmtStartEmTime, const LogRecord &logRecord,
	LogSequentialNumber &redoStartLsn, bool isIncrementalBackup) {
	try {
		util::StackAllocator::Scope scope(alloc);
		LogSequentialNumber nextLsn = logRecord.lsn_;

		const KeyConstraint containerKeyConstraint =
			KeyConstraint::getSystemKeyConstraint(
				ds_->getValueLimitConfig().getLimitContainerNameSize());

		const bool REDO = true;

		bool isAllowExpiration = true;

		const PartitionGroupId pgId = pgConfig_.getPartitionGroupId(pId);
		switch (logRecord.type_) {
		case LogManager::LOG_TYPE_CHECKPOINT_START:
			redoCheckpointStartLog(
					alloc, mode, pId, stmtStartTime,
					stmtStartEmTime, logRecord, redoStartLsn,
					isIncrementalBackup, false);
			break;

		case LogManager::LOG_TYPE_CHUNK_META_DATA:
			redoChunkMetaDataLog(
					alloc, mode, pId, stmtStartTime,
					stmtStartEmTime, logRecord, redoStartLsn,
					isIncrementalBackup, false);
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
				pId, logRecord.rowData_, logRecord.dataLen_, false);
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
				TransactionManager::NO_AUTO_COMMIT_BEGIN_OR_CONTINUE,
				false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE);
			TransactionContext &txn =
				txnMgr_->put(alloc, pId, logRecord.clientId_, src,
					stmtStartTime, stmtStartEmTime, REDO, logRecord.txnId_);
			assert(txn.getId() == logRecord.txnId_);
			assert(txn.getContainerId() == logRecord.containerId_);

			const DataStore::Latch latch(
				txn, txn.getPartitionId(), ds_, clsSvc_);

			ContainerAutoPtr containerAutoPtr(txn, ds_, txn.getPartitionId(),
				txn.getContainerId(), ANY_CONTAINER, isAllowExpiration);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			checkContainerExistence(container, txn.getContainerId());

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
				TransactionManager::NO_AUTO_COMMIT_BEGIN_OR_CONTINUE,
				false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE);
			TransactionContext &txn =
				txnMgr_->put(alloc, pId, logRecord.clientId_, src,
					stmtStartTime, stmtStartEmTime, REDO, logRecord.txnId_);
			assert(txn.getId() == logRecord.txnId_);
			assert(txn.getContainerId() == logRecord.containerId_);

			const DataStore::Latch latch(
				txn, txn.getPartitionId(), ds_, clsSvc_);

			ContainerAutoPtr containerAutoPtr(txn, ds_, txn.getPartitionId(),
				txn.getContainerId(), ANY_CONTAINER, isAllowExpiration);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			checkContainerExistence(container, txn.getContainerId());

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

		case LogManager::LOG_TYPE_PARTITION_EXPIRATION_PUT_CONTAINER:
			logMgr_->setLogVersion(pId, logMgr_->getLatestVersion());
		case LogManager::LOG_TYPE_PUT_CONTAINER: {
			const TransactionManager::ContextSource src(PUT_CONTAINER,
				logRecord.stmtId_, logRecord.containerId_,
				logRecord.txnTimeout_,
				txnMgr_->getContextGetModeForRecovery(
					static_cast<TransactionManager::GetMode>(
						logRecord.txnContextCreationMode_)),
				txnMgr_->getTransactionModeForRecovery(
					logRecord.withBegin_, logRecord.isAutoCommit_),
				false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE);
			GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL, GS_TRACE_RM_REDO_LOG_STATUS,
				"LOG_TYPE_CREATE_CONTAINER(pId="
					<< pId << ", lsn=" << logRecord.lsn_
					<< ", containerId=" << logRecord.containerId_
					<< ", stmtId=" << logRecord.stmtId_
					<< ", containerName=" << logRecord.containerName_
					<< ", containerType=" << logRecord.containerType_
					<< ", getMode=" << TM_OUTPUT_GETMODE(src.getMode_)
					<< ", txnMode=" << TM_OUTPUT_TXNMODE(src.txnMode_)
					<< ", cursorRowId=" << logRecord.numRow_
					<< ", withBegin=" << (int32_t)logRecord.withBegin_
					<< ", isAutoCommit=" << (int32_t)logRecord.isAutoCommit_
					<< ")");
			TransactionContext &txn =
				txnMgr_->putNoExpire(alloc, pId, logRecord.clientId_, src,
					stmtStartTime, stmtStartEmTime, REDO, logRecord.txnId_);
			assert(txn.getId() == logRecord.txnId_ || logRecord.txnId_ == UNDEF_TXNID);
			assert(txn.getContainerId() == logRecord.containerId_);

			const DataStore::Latch latch(
				txn, txn.getPartitionId(), ds_, clsSvc_);

			const FullContainerKey containerKey(
				alloc, containerKeyConstraint,
				logRecord.containerName_, logRecord.containerNameLen_);
			DataStore::PutStatus dummy;
			const bool isModifiable = true;
			ContainerAutoPtr containerAutoPtr(txn, ds_, txn.getPartitionId(),
				containerKey,
				static_cast<uint8_t>(logRecord.containerType_),
				logRecord.containerInfoLen_, logRecord.containerInfo_,
				isModifiable, MessageSchema::V4_1_VERSION, dummy);

			BaseContainer *container = containerAutoPtr.getBaseContainer();
			util::String containerName(alloc);
			containerKey.toString(alloc, containerName);
			checkContainerExistence(container, containerName);
			if (container->getContainerId() != logRecord.containerId_ || 
				txn.getContainerId() != logRecord.containerId_) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_DS_CONTAINER_ID_INVALID,
					"log containerId : " << logRecord.containerId_
										 << ", redo containerId : "
										 << container->getContainerId());
			}
			if (!txn.isAutoCommit()) {
				txnMgr_->update(txn, logRecord.stmtId_);

				removeTransaction(mode, txn);
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
				TransactionManager::AUTO, TransactionManager::AUTO_COMMIT,
				false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE);
			TransactionContext &txn = txnMgr_->put(alloc, pId,
				TXN_EMPTY_CLIENTID, src, stmtStartTime, stmtStartEmTime, REDO);

			const DataStore::Latch latch(
				txn, txn.getPartitionId(), ds_, clsSvc_);

			const FullContainerKey containerKey(
				alloc, containerKeyConstraint,
				logRecord.containerName_, logRecord.containerNameLen_);
			ds_->dropContainer(
				txn, txn.getPartitionId(), containerKey, ANY_CONTAINER);
		} break;

		case LogManager::LOG_TYPE_CONTINUE_CREATE_INDEX:
		case LogManager::LOG_TYPE_CONTINUE_ALTER_CONTAINER: {
			const TransactionManager::ContextSource src(CONTINUE_CREATE_INDEX,
				logRecord.stmtId_, logRecord.containerId_,
				logRecord.txnTimeout_,
				txnMgr_->getContextGetModeForRecovery(
					static_cast<TransactionManager::GetMode>(
						logRecord.txnContextCreationMode_)),
				txnMgr_->getTransactionModeForRecovery(
					logRecord.withBegin_, logRecord.isAutoCommit_),
				false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE);
			GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL, GS_TRACE_RM_REDO_LOG_STATUS,
				"LOG_TYPE_CREATE_INDEX(pId="
					<< pId << ", lsn=" << logRecord.lsn_
					<< ", clientId=" << logRecord.clientId_ << ", containerId="
					<< logRecord.containerId_ << ", txnId=" << logRecord.txnId_
					<< ", stmtId=" << logRecord.stmtId_
					<< ", getMode=" << TM_OUTPUT_GETMODE(src.getMode_)
					<< ", txnMode=" << TM_OUTPUT_TXNMODE(src.txnMode_)
					<< ", cursorRowId=" << logRecord.numRow_
					<< ", withBegin=" << (int32_t)logRecord.withBegin_
					<< ", isAutoCommit=" << (int32_t)logRecord.isAutoCommit_
					<< ")");
			TransactionContext &txn =
				txnMgr_->put(alloc, pId, logRecord.clientId_, src,
					stmtStartTime, stmtStartEmTime, REDO, logRecord.txnId_);
			assert(txn.getId() == logRecord.txnId_);
			assert(txn.getContainerId() == logRecord.containerId_);

			const DataStore::Latch latch(
				txn, txn.getPartitionId(), ds_, clsSvc_);

			ContainerAutoPtr containerAutoPtr(txn, ds_, txn.getPartitionId(),
				txn.getContainerId(), ANY_CONTAINER, isAllowExpiration);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			checkContainerExistence(container, txn.getContainerId());

			if (container->getContainerId() != logRecord.containerId_ || 
				txn.getContainerId() != logRecord.containerId_) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_DS_CONTAINER_ID_INVALID,
					"log containerId : " << logRecord.containerId_
										 << ", redo containerId : "
										 << container->getContainerId());
			}
			if (logRecord.type_ == LogManager::LOG_TYPE_CONTINUE_CREATE_INDEX) {
				IndexCursor indexCursor = container->getIndexCursor(txn);
				container->continueCreateIndex(txn, indexCursor);
			} else {
				ContainerCursor containerCursor(txn.isAutoCommit());
				containerCursor = container->getContainerCursor(txn);
				container->continueChangeSchema(txn, containerCursor);
			}
			txnMgr_->update(txn, logRecord.stmtId_);

			removeTransaction(mode, txn);
		} break;

		case LogManager::LOG_TYPE_CREATE_INDEX: {
			const TransactionManager::ContextSource src(CREATE_INDEX,
				logRecord.stmtId_, logRecord.containerId_,
				logRecord.txnTimeout_,
				txnMgr_->getContextGetModeForRecovery(
					static_cast<TransactionManager::GetMode>(
						logRecord.txnContextCreationMode_)),
				txnMgr_->getTransactionModeForRecovery(
					logRecord.withBegin_, logRecord.isAutoCommit_),
				false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE);
			GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL, GS_TRACE_RM_REDO_LOG_STATUS,
				"LOG_TYPE_CREATE_INDEX(pId=" << pId
					<< ", lsn=" << logRecord.lsn_
					<< ", containerId=" << logRecord.containerId_
					<< ", txnId=" << logRecord.txnId_
					<< ", stmtId=" << logRecord.stmtId_
					<< ", columnCount=" << logRecord.columnIdCount_
					<< ", firstColumnId="
					<< (logRecord.columnIdCount_ > 0 ? logRecord.columnIds_[0] : UNDEF_COLUMNID)
					<< ", mapType=" << logRecord.mapType_
					<< ", getMode=" << TM_OUTPUT_GETMODE(src.getMode_)
					<< ", txnMode=" << TM_OUTPUT_TXNMODE(src.txnMode_)
					<< ", cursorRowId=" << logRecord.numRow_
					<< ", withBegin=" << (int32_t)logRecord.withBegin_
					<< ", isAutoCommit=" << (int32_t)logRecord.isAutoCommit_
					<< ", indexName=" << logRecord.indexName_ << ")");
			TransactionContext &txn =
				txnMgr_->putNoExpire(alloc, pId, logRecord.clientId_, src,
					stmtStartTime, stmtStartEmTime, REDO, logRecord.txnId_);
			assert(txn.getId() == logRecord.txnId_ || logRecord.txnId_ == UNDEF_TXNID);
			assert(txn.getContainerId() == logRecord.containerId_);

			const DataStore::Latch latch(
				txn, txn.getPartitionId(), ds_, clsSvc_);

			ContainerAutoPtr containerAutoPtr(txn, ds_, txn.getPartitionId(),
				txn.getContainerId(), ANY_CONTAINER, isAllowExpiration);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			checkContainerExistence(container, txn.getContainerId());

			IndexInfo indexInfo(alloc);
			indexInfo.mapType = static_cast<MapType>(logRecord.mapType_);
			indexInfo.columnIds_.resize(logRecord.columnIdCount_, 0);
			if (!indexInfo.columnIds_.empty()) {
				memcpy(&(indexInfo.columnIds_[0]), logRecord.columnIds_,
					sizeof(ColumnId)*logRecord.columnIdCount_);
			}
			indexInfo.indexName_.append(logRecord.indexName_, logRecord.indexNameLen_-1);
			indexInfo.extensionName_.append(logRecord.extensionName_, logRecord.extensionNameLen_-1);
			util::XArray<uint8_t> paramSchema(alloc);
			size_t paramOffset = 0;
			if (logRecord.paramCount_ > 0) {
				indexInfo.extensionOptionSchema_.push_back(
					logRecord.paramData_ + paramOffset, logRecord.paramDataLen_[0]);
				paramOffset += logRecord.paramDataLen_[0];
			}
			util::XArray<uint8_t> paramFixed(alloc);
			if (logRecord.paramCount_ > 1) {
				indexInfo.extensionOptionFixedPart_.push_back(
					logRecord.paramData_ + paramOffset, logRecord.paramDataLen_[1]);
				paramOffset += logRecord.paramDataLen_[1];
			}
			util::XArray<uint8_t> paramVar(alloc);
			if (logRecord.paramCount_ > 2) {
				indexInfo.extensionOptionVarPart_.push_back(
					logRecord.paramData_ + paramOffset, logRecord.paramDataLen_[2]);
				paramOffset += logRecord.paramDataLen_[2];
			}

			if (container->getContainerId() != logRecord.containerId_ || 
				txn.getContainerId() != logRecord.containerId_) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_DS_CONTAINER_ID_INVALID,
					"log containerId : " << logRecord.containerId_
										 << ", redo containerId : "
										 << container->getContainerId());
			}
			IndexCursor indexCursor;
			container->createIndex(txn, indexInfo, indexCursor);

			if (!txn.isAutoCommit()) {
				txnMgr_->update(txn, logRecord.stmtId_);

				removeTransaction(mode, txn);
			}
		} break;

		case LogManager::LOG_TYPE_DROP_INDEX: {
			GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL, GS_TRACE_RM_REDO_LOG_STATUS,
				"LOG_TYPE_DROP_INDEX(pId=" << pId
					<< ", lsn=" << logRecord.lsn_
					<< ", containerId=" << logRecord.containerId_
					<< ", stmtId=" << logRecord.stmtId_
					<< ", columnCount=" << logRecord.columnIdCount_
					<< ", firstColumnId="
					<< (logRecord.columnIdCount_ > 0 ? logRecord.columnIds_[0] : UNDEF_COLUMNID)
					<< ", mapType=" << logRecord.mapType_
					<< ", indexName=" << logRecord.indexName_ << ")");
			const TransactionManager::ContextSource src(DELETE_INDEX,
				logRecord.stmtId_, logRecord.containerId_,
				TXN_DEFAULT_TRANSACTION_TIMEOUT_INTERVAL,
				TransactionManager::AUTO, TransactionManager::AUTO_COMMIT,
				false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE);
			TransactionContext &txn = txnMgr_->put(alloc, pId,
				TXN_EMPTY_CLIENTID, src, stmtStartTime, stmtStartEmTime, REDO);

			const DataStore::Latch latch(
				txn, txn.getPartitionId(), ds_, clsSvc_);

			ContainerAutoPtr containerAutoPtr(txn, ds_, txn.getPartitionId(),
				txn.getContainerId(), ANY_CONTAINER, isAllowExpiration);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			checkContainerExistence(container, txn.getContainerId());

			IndexInfo indexInfo(alloc);
			indexInfo.mapType = static_cast<MapType>(logRecord.mapType_);
			indexInfo.columnIds_.resize(logRecord.columnIdCount_, 0);
			if (!indexInfo.columnIds_.empty()) {
				memcpy(&(indexInfo.columnIds_[0]), logRecord.columnIds_,
					sizeof(ColumnId)*logRecord.columnIdCount_);
			}
			indexInfo.indexName_.append(logRecord.indexName_, logRecord.indexNameLen_-1);
			indexInfo.extensionName_.append(logRecord.extensionName_, logRecord.extensionNameLen_-1);
			size_t paramOffset = 0;
			if (logRecord.paramCount_ > 0) {
				indexInfo.anyNameMatches_ = *static_cast<const uint8_t*>(logRecord.paramData_ + paramOffset);
				paramOffset += logRecord.paramDataLen_[0];
			}
			if (logRecord.paramCount_ > 1) {
				indexInfo.anyTypeMatches_ = *static_cast<const uint8_t*>(logRecord.paramData_ + paramOffset);
				paramOffset += logRecord.paramDataLen_[1];
			}

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
				TransactionManager::AUTO, TransactionManager::AUTO_COMMIT,
				false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE);
			TransactionContext &txn = txnMgr_->put(alloc, pId,
				TXN_EMPTY_CLIENTID, src, stmtStartTime, stmtStartEmTime, REDO);

			const DataStore::Latch latch(
				txn, txn.getPartitionId(), ds_, clsSvc_);

			ContainerAutoPtr containerAutoPtr(txn, ds_, txn.getPartitionId(),
				txn.getContainerId(), ANY_CONTAINER, isAllowExpiration);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			checkContainerExistence(container, txn.getContainerId());

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
				TransactionManager::AUTO, TransactionManager::AUTO_COMMIT,
				false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE);
			TransactionContext &txn = txnMgr_->put(alloc, pId,
				TXN_EMPTY_CLIENTID, src, stmtStartTime, stmtStartEmTime, REDO);

			const DataStore::Latch latch(
				txn, txn.getPartitionId(), ds_, clsSvc_);

			ContainerAutoPtr containerAutoPtr(txn, ds_, txn.getPartitionId(),
				txn.getContainerId(), ANY_CONTAINER, isAllowExpiration);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			checkContainerExistence(container, txn.getContainerId());

			const std::string triggerName(
				reinterpret_cast<const char*>(logRecord.containerName_),
				logRecord.containerNameLen_);
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
					logRecord.withBegin_, logRecord.isAutoCommit_),
				false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE);
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
				txn.getContainerId(), ANY_CONTAINER, isAllowExpiration);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			checkContainerExistence(container, txn.getContainerId());

			if (logRecord.numRow_ > 1) {
				DataStore::PutStatus status;  
				container->putRowList(txn, logRecord.dataLen_,
					logRecord.rowData_, logRecord.numRow_, status);
			}
			else if (logRecord.numRow_ == 1) {
				RowId rowId = reinterpret_cast<const RowId *>(logRecord.rowIds_)[0];
				DataStore::PutStatus status;
				container->redoPutRow(txn, logRecord.dataLen_, logRecord.rowData_,
					rowId, status, PUT_INSERT_OR_UPDATE);
				if (status == DataStore::NOT_EXECUTED) {
					GS_TRACE_INFO(
						RECOVERY_MANAGER, GS_ERROR_DS_BACKGROUND_TASK_INVALID, 
						"(pId=" << logRecord.partitionId_ 
								<< ", lsn=" << logRecord.lsn_
								<< ", logType = " << logRecord.type_ 
								<< ", containerId = " << logRecord.containerId_
								<< ", containerType =" << (int32_t)container->getContainerType()
								<< ", log rowId = " << reinterpret_cast<const RowId *>(logRecord.rowIds_)[0]
								<< ", not executed"
								);
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
					logRecord.withBegin_, logRecord.isAutoCommit_),
				false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE);
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
				txn.getContainerId(), ANY_CONTAINER, isAllowExpiration);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			checkContainerExistence(container, txn.getContainerId());
			assert(logRecord.numRow_ == logRecord.numRowId_);

			for (uint64_t i = 0; i < logRecord.numRowId_; i++) {
				const RowId rowId =
					reinterpret_cast<const RowId *>(logRecord.rowIds_)[i];
				DataStore::PutStatus status;
				container->updateRow(
					txn, logRecord.dataLen_, logRecord.rowData_, rowId, status);
				assert(status == DataStore::UPDATE);
				if (status != DataStore::UPDATE) {
					GS_TRACE_INFO(
						RECOVERY_MANAGER, GS_ERROR_DS_BACKGROUND_TASK_INVALID, 
						"(pId=" << logRecord.partitionId_ 
								<< ", lsn=" << logRecord.lsn_
								<< ", logType = " << logRecord.type_ 
								<< ", containerId = " << logRecord.containerId_
								<< ", containerType =" << (int32_t)container->getContainerType()
								<< ", log rowId = " << reinterpret_cast<const RowId *>(logRecord.rowIds_)[i]
								<< ", not executed"
								);
				}
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
					logRecord.withBegin_, logRecord.isAutoCommit_),
				false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE);
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
				txn.getContainerId(), ANY_CONTAINER, isAllowExpiration);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			checkContainerExistence(container, txn.getContainerId());
			assert(logRecord.numRow_ == logRecord.numRowId_);

			for (uint64_t i = 0; i < logRecord.numRowId_; i++) {
				const RowId rowId =
					reinterpret_cast<const RowId *>(logRecord.rowIds_)[i];
				bool dummy;
				container->redoDeleteRow(txn, rowId, dummy);
				if (!dummy) {
					GS_TRACE_INFO(
						RECOVERY_MANAGER, GS_ERROR_DS_BACKGROUND_TASK_INVALID, 
						"(pId=" << logRecord.partitionId_ 
								<< ", lsn=" << logRecord.lsn_
								<< ", logType = " << logRecord.type_ 
								<< ", containerId = " << logRecord.containerId_
								<< ", containerType =" << (int32_t)container->getContainerType()
								<< ", log rowId = " << reinterpret_cast<const RowId *>(logRecord.rowIds_)[i]
								<< ", not executed"
								);
				}
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
					logRecord.withBegin_, logRecord.isAutoCommit_),
				false, TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE);
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
				txn.getContainerId(), ANY_CONTAINER, isAllowExpiration);
			BaseContainer *container = containerAutoPtr.getBaseContainer();
			checkContainerExistence(container, txn.getContainerId());

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

		const KeyConstraint containerKeyConstraint =
				KeyConstraint::getSystemKeyConstraint(
				ds_->getValueLimitConfig().getLimitContainerNameSize());

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
					alloc, containerKeyConstraint,
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
	@brief Checks if a specified container exists
*/
void RecoveryManager::checkContainerExistence(
	BaseContainer *container, ContainerId containerId) {
	if (container == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_RM_CONTAINER_NOT_FOUND,
			"(containerId=" << containerId << ")");
	}
}

/*!
	@brief Checks if a specified container exists
*/
void RecoveryManager::checkContainerExistence(
	BaseContainer *container, const util::String &containerName) {
	if (container == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_RM_CONTAINER_NOT_FOUND,
			"(containerName=" << containerName << ")");
	}
}

/*!
	@brief Converts a LogRecord to a string.
*/
void RecoveryManager::logRecordToString(
	util::StackAllocator &alloc, const KeyConstraint &containerKeyConstraint,
	PartitionId pId,
	const LogRecord &logRecord, std::string &dumpString,
	LogSequentialNumber &nextLsn) {
	util::NormalOStringStream ss;

	util::StackAllocator::Scope groupScope(alloc);

	nextLsn = logRecord.lsn_;
	switch (logRecord.type_) {
	case LogManager::LOG_TYPE_CHECKPOINT_START:
		{
			ss << "LOG_TYPE_CHECKPOINT_START, pId=" << pId
			   << ", lsn=" << logRecord.lsn_ << ", maxTxnId=" << logRecord.txnId_
			   << ", numTxn=" << logRecord.txnContextNum_;

			const ClientId *clientIds = reinterpret_cast<const ClientId *>(logRecord.clientIds_);
			const TransactionId *txnIds = reinterpret_cast<const TransactionId *>(logRecord.activeTxnIds_);
			const ContainerId* containerIds = reinterpret_cast<const ContainerId *>(logRecord.refContainerIds_);
			const StatementId* stmtIds = reinterpret_cast<const StatementId *>(logRecord.lastExecStmtIds_);
			const int32_t* txnTimeouts = reinterpret_cast<const int32_t *>(logRecord.txnTimeouts_);

			ss << ", (clientId, txnId, containerId, stmtId, txnTimeout)";
			for (size_t pos = 0; pos < logRecord.txnContextNum_; ++pos) {
				ss << ",(" << clientIds[pos] << "," << txnIds[pos] <<
					"," << containerIds[pos] << "," << stmtIds[pos] <<
					"," << txnTimeouts[pos] << ")";
			}
		}
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
		{
			const FullContainerKey containerKey(
				alloc, containerKeyConstraint,
				logRecord.containerName_, logRecord.containerNameLen_);
			util::String name(alloc);
			containerKey.toString(alloc, name);
			ss << "LOG_TYPE_PUT_CONTAINER, pId=" << pId
			   << ", lsn=" << logRecord.lsn_
			   << ", containerId=" << logRecord.containerId_
			   << ", clientId=" << logRecord.clientId_
			   << ", txnId=" << logRecord.txnId_ << ", stmtId=" << logRecord.stmtId_
			   << ", containerName=" << name.c_str();
		}
		break;

	case LogManager::LOG_TYPE_DROP_CONTAINER:
		{
			const FullContainerKey containerKey(
				alloc, containerKeyConstraint,
				logRecord.containerName_, logRecord.containerNameLen_);
			util::String name(alloc);
			containerKey.toString(alloc, name);
			ss << "LOG_TYPE_DROP_CONTAINER, pId=" << pId
			   << ", lsn=" << logRecord.lsn_
			   << ", containerId=" << logRecord.containerId_
			   << ", clientId=" << logRecord.clientId_
			   << ", txnId=" << logRecord.txnId_ << ", stmtId=" << logRecord.stmtId_
			   << ", containerName=" << name.c_str();
		}
		break;

	case LogManager::LOG_TYPE_CREATE_INDEX:
		ss << "LOG_TYPE_CREATE_INDEX, pId=" << pId << ", lsn=" << logRecord.lsn_
		   << ", containerId=" << logRecord.containerId_
		   << ", clientId=" << logRecord.clientId_
		   << ", txnId=" << logRecord.txnId_ << ", stmtId=" << logRecord.stmtId_
		   << ", columnCount=" << logRecord.columnIdCount_
		   << ", firstColumnId="
		   << (logRecord.columnIdCount_ > 0 ? logRecord.columnIds_[0] : UNDEF_COLUMNID)
		   << ", mapType=" << logRecord.mapType_
		   << ", indexName=" << logRecord.indexName_
		   << ", paramCount=" << logRecord.paramCount_;
		break;

	case LogManager::LOG_TYPE_DROP_INDEX:
		ss << "LOG_TYPE_DROP_INDEX, pId=" << pId << ", lsn=" << logRecord.lsn_
		   << ", containerId=" << logRecord.containerId_
		   << ", clientId=" << logRecord.clientId_
		   << ", txnId=" << logRecord.txnId_ << ", stmtId=" << logRecord.stmtId_
		   << ", columnCount=" << logRecord.columnIdCount_
		   << ", firstColumnId="
		   << (logRecord.columnIdCount_ > 0 ? logRecord.columnIds_[0] : UNDEF_COLUMNID)
		   << ", mapType=" << logRecord.mapType_
		   << ", indexName=" << logRecord.indexName_
		   << ", paramCount=" << logRecord.paramCount_;
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

	case LogManager::LOG_TYPE_PARTITION_EXPIRATION_PUT_CONTAINER:
		{
			const FullContainerKey containerKey(
				alloc, containerKeyConstraint,
				logRecord.containerName_, logRecord.containerNameLen_);
			util::String name(alloc);
			containerKey.toString(alloc, name);

			ss << "LOG_TYPE_PARTITION_EXPIRATION_PUT_CONTAINER, pId=" << pId
			   << ", lsn=" << logRecord.lsn_
			   << ", containerId=" << logRecord.containerId_
			   << ", clientId=" << logRecord.clientId_
			   << ", txnId=" << logRecord.txnId_ << ", stmtId=" << logRecord.stmtId_
			   << ", containerName=" << name.c_str();
		}
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


RecoveryManager::BackupInfo::BackupInfo()
	: partitionNum_(0),
	  partitionGroupNum_(0),
	  currentPartitionGroupId_(0),
	  backupStartTime_(0),
	  backupEndTime_(0),
	  cpFileCopyStartTime_(0),
	  cpFileCopyEndTime_(0),
	  logFileCopyStartTime_(0),
	  logFileCopyEndTime_(0),
	  incrementalBackupMode_(INCREMENTAL_BACKUP_NONE),
	  checkCpFileSizeFlag_(true) {}

RecoveryManager::BackupInfo::~BackupInfo() {}

void RecoveryManager::BackupInfo::reset() {
	currentPartitionGroupId_ = 0;

	backupStartTime_ = 0;
	backupEndTime_ = 0;
	cpFileCopyStartTime_ = 0;
	cpFileCopyEndTime_ = 0;
	logFileCopyStartTime_ = 0;
	logFileCopyEndTime_ = 0;

	backupPath_.clear();
	status_.clear();

	cpIdList_.clear();

	cpFileSizeList_.clear();
	logFileSizeList_.clear();
	incrementalBackupMode_ = INCREMENTAL_BACKUP_NONE;
	incrementalBackupCpIdList_.clear();
	checkCpFileSizeFlag_ = true;
}

void RecoveryManager::BackupInfo::writeBackupInfoFile() {
	std::string backupInfoFileName;
	std::string backupInfoDigestFileName;
	util::NamedFile file;
	try {
		picojson::object jsonConfigInfo;
		jsonConfigInfo["partitionNum"] =
			picojson::value(static_cast<double>(partitionNum_));
		jsonConfigInfo["groupNum"] =
			picojson::value(static_cast<double>(partitionGroupNum_));

		picojson::object jsonBackupInfo;
		jsonBackupInfo["systemVersion"] = picojson::value(gsVersion_);
		jsonBackupInfo["backupInfoFileVersion"] =
			picojson::value(static_cast<double>(BACKUP_INFO_FILE_VERSION));

		jsonBackupInfo["backupPath"] = picojson::value(backupPath_);
		jsonBackupInfo["backupStatus"] = picojson::value(status_);
		jsonBackupInfo["backupEndGroupId"] =
			picojson::value(static_cast<double>(currentPartitionGroupId_));

		util::NormalOStringStream oss;
		if (backupStartTime_ > 0) {
			util::DateTime(backupStartTime_).format(oss, true);
			jsonBackupInfo["backupStartTime"] = picojson::value(oss.str());
		}
		if (backupEndTime_ > 0) {
			clearStringStream(oss);
			util::DateTime(backupEndTime_).format(oss, true);
			jsonBackupInfo["backupEndTime"] = picojson::value(oss.str());
		}
		if (cpFileCopyStartTime_ > 0) {
			clearStringStream(oss);
			util::DateTime(cpFileCopyStartTime_).format(oss, true);
			jsonBackupInfo["cpFileCopyStartTime"] = picojson::value(oss.str());
		}
		if (cpFileCopyEndTime_ > 0) {
			clearStringStream(oss);
			util::DateTime(cpFileCopyEndTime_).format(oss, true);
			jsonBackupInfo["cpFileCopyEndTime"] = picojson::value(oss.str());
		}
		if (logFileCopyStartTime_ > 0) {
			clearStringStream(oss);
			util::DateTime(logFileCopyStartTime_).format(oss, true);
			jsonBackupInfo["logFileCopyStartTime"] = picojson::value(oss.str());
		}
		if (logFileCopyEndTime_ > 0) {
			clearStringStream(oss);
			util::DateTime(logFileCopyEndTime_).format(oss, true);
			jsonBackupInfo["logFileCopyEndTime"] = picojson::value(oss.str());
		}
		jsonBackupInfo["checkCpFileSize"] = picojson::value(checkCpFileSizeFlag_);

		picojson::array cpIdList;
		picojson::array cpFileSizeList;
		picojson::array logFileSizeList;
		picojson::array cpFileCopyTimeList;
		picojson::array logFileCopyTimeList;
		picojson::array incrementalBackupCpIdList;
		for (PartitionGroupId pgId = 0; pgId < partitionGroupNum_; ++pgId) {
			cpIdList.push_back(
				picojson::value(static_cast<double>(cpIdList_[pgId])));
			cpFileSizeList.push_back(
				picojson::value(static_cast<double>(cpFileSizeList_[pgId])));
			logFileSizeList.push_back(
				picojson::value(static_cast<double>(logFileSizeList_[pgId])));
			cpFileCopyTimeList.push_back(
				picojson::value(cpFileCopyTimeList_[pgId]));
			logFileCopyTimeList.push_back(
				picojson::value(logFileCopyTimeList_[pgId]));

			picojson::array tmpCpIdList;
			std::vector<CheckpointId> &innerCpIdList =
				incrementalBackupCpIdList_[pgId];
			std::vector<CheckpointId>::iterator itr = innerCpIdList.begin();
			for (; itr != innerCpIdList.end(); ++itr) {
				tmpCpIdList.push_back(
					picojson::value(static_cast<double>(*itr)));
			}
			incrementalBackupCpIdList.push_back(picojson::value(tmpCpIdList));
		}
		jsonBackupInfo["checkpointId"] = picojson::value(cpIdList);
		jsonBackupInfo["cpFileSize"] = picojson::value(cpFileSizeList);
		jsonBackupInfo["logFileSize"] = picojson::value(logFileSizeList);
		jsonBackupInfo["cpFileCopyTimeSec"] =
			picojson::value(cpFileCopyTimeList);
		jsonBackupInfo["logFileCopyTimeSec"] =
			picojson::value(logFileCopyTimeList);
		jsonBackupInfo["incrementalBackupMode"] =
			picojson::value(static_cast<double>(incrementalBackupMode_));
		jsonBackupInfo["incrementalBackupCpId"] =
			picojson::value(incrementalBackupCpIdList);

		picojson::object jsonObject;
		jsonObject["configInfo"] = picojson::value(jsonConfigInfo);
		jsonObject["backupInfo"] = picojson::value(jsonBackupInfo);

		std::string jsonString(picojson::value(jsonObject).serialize());
		SHA256_Data(reinterpret_cast<const uint8_t *>(jsonString.data()),
			jsonString.size(), digestData_);
		digestData_[SHA256_DIGEST_STRING_LENGTH] = '\0';
		GS_TRACE_INFO(RECOVERY_MANAGER, GS_TRACE_RM_BACKUP_INFO_FILE_DIGEST,
			"digest = " << digestData_);

		picojson::object jsonDigest;
		jsonDigest["digestValue"] = picojson::value(std::string(digestData_));
		std::string digestString(picojson::value(jsonDigest).serialize());

		util::FileSystem::createPath(backupPath_.c_str(),
			BACKUP_INFO_DIGEST_FILE_NAME.c_str(), backupInfoDigestFileName);

		util::FileSystem::createPath(backupPath_.c_str(),
			BACKUP_INFO_FILE_NAME.c_str(), backupInfoFileName);

		file.open(backupInfoDigestFileName.c_str(),
			util::FileFlag::TYPE_READ_WRITE | util::FileFlag::TYPE_CREATE |
				util::FileFlag::TYPE_TRUNCATE);
		file.lock();
		file.write(digestString.c_str(), digestString.length());
		file.close();

		file.open(backupInfoFileName.c_str(),
			util::FileFlag::TYPE_READ_WRITE | util::FileFlag::TYPE_CREATE |
				util::FileFlag::TYPE_TRUNCATE);
		file.lock();
		file.write(jsonString.c_str(), jsonString.length());
		file.close();
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e,
			"Write " << backupInfoFileName.c_str()
					 << " failed. (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void RecoveryManager::BackupInfo::removeUnnecessaryLogFiles(
	std::vector<CheckpointId> &lastCpIdList) {
	try {
		const char8_t *dirPath = dataStorePath_.c_str();

		if (!util::FileSystem::isDirectory(dirPath)) {
			return;
		}

		util::Directory dir(dirPath);

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

			if (cpId < lastCpIdList[pgId]) {
				u8string targetFileName;
				try {
					util::FileSystem::createPath(
						dirPath, fileName.c_str(), targetFileName);
					util::FileSystem::remove(targetFileName.c_str(), false);
				}
				catch (std::exception &e) {
					UTIL_TRACE_EXCEPTION_WARNING(RECOVERY_MANAGER, e,
						"remove " << targetFileName.c_str() << " failed.");
				}
			}
		}
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION_WARNING(
			RECOVERY_MANAGER, e, "RemoveNoNeedLogFile failed.");
	}
}

void RecoveryManager::BackupInfo::removeBackupInfoFile(LogManager &logManager) {
	std::string backupInfoFileName;
	std::string backupInfoDigestFileName;
	util::FileSystem::createPath(
		backupPath_.c_str(), BACKUP_INFO_FILE_NAME.c_str(), backupInfoFileName);
	util::FileSystem::createPath(backupPath_.c_str(),
		BACKUP_INFO_DIGEST_FILE_NAME.c_str(), backupInfoDigestFileName);
	if (util::FileSystem::exists(backupInfoFileName.c_str())) {
		try {
			util::FileSystem::remove(backupInfoFileName.c_str(), false);
		}
		catch (std::exception &e) {
			GS_RETHROW_SYSTEM_ERROR(e,
				"Remove " << backupInfoFileName.c_str() << " failed. (reason="
						  << GS_EXCEPTION_MESSAGE(e) << ")");
		}
	}
	else {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_REMOVE_BACKUP_INFO_FILE_FAILED,
			"remove " << backupInfoFileName.c_str() << " failed. (Not found) ");
	}
	if (util::FileSystem::exists(backupInfoDigestFileName.c_str())) {
		try {
			util::FileSystem::remove(backupInfoDigestFileName.c_str(), false);
		}
		catch (std::exception &e) {
			GS_RETHROW_SYSTEM_ERROR(
				e, "Remove " << backupInfoDigestFileName.c_str()
							 << " failed. (reason=" << GS_EXCEPTION_MESSAGE(e)
							 << ")");
		}
	}
	else {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_REMOVE_BACKUP_INFO_FILE_FAILED,
			"remove " << backupInfoDigestFileName.c_str()
					  << " failed. (Not found) ");
	}

	if (isIncrementalBackup()) {
		std::vector<CheckpointId> lastCpIdList;
		lastCpIdList.assign(partitionGroupNum_, 0);
		for (PartitionGroupId pgId = 0; pgId < partitionGroupNum_; ++pgId) {
			std::vector<CheckpointId> &cpIdList =
				getIncrementalBackupCpIdList(pgId);
			if (cpIdList.size() <= 1) {
				continue;
			}
			std::vector<CheckpointId>::iterator itr = cpIdList.begin();
			if (!cpIdList.empty()) {
				lastCpIdList[pgId] = cpIdList.back();
			}
			for (; itr != cpIdList.end(); ++itr) {
				u8string prefix;
				util::FileSystem::createPath(dataStorePath_.c_str(),
					LogManager::LOG_FILE_BASE_NAME, prefix);
				util::NormalOStringStream strstrm(prefix);
				strstrm << LogManager::LOG_FILE_SEPARATOR << (*itr);
				u8string targetFileName;
				if (itr != cpIdList.begin()) {
					strstrm << CheckpointService::INCREMENTAL_BACKUP_FILE_SUFFIX
							<< LogManager::LOG_FILE_EXTENSION;
					targetFileName = strstrm.str();
					if (util::FileSystem::exists(targetFileName.c_str())) {
						try {
							util::FileSystem::remove(
								targetFileName.c_str(), false);
						}
						catch (std::exception &e) {
							UTIL_TRACE_EXCEPTION_WARNING(RECOVERY_MANAGER, e,
								"remove " << targetFileName.c_str()
										  << " failed.");
						}
					}
				}
			}
			logManager.cleanupLogFiles(pgId, lastCpIdList[pgId]);
		}  
		removeUnnecessaryLogFiles(lastCpIdList);
	}
}

void RecoveryManager::BackupInfo::readBackupInfoFile() {
	readBackupInfoFile(backupPath_);
}

void RecoveryManager::BackupInfo::readBackupInfoFile(const std::string &path) {
	std::string backupInfoFileName;
	std::string backupInfoDigestFileName;
	try {
		util::FileSystem::createPath(path.c_str(),
			BACKUP_INFO_DIGEST_FILE_NAME.c_str(), backupInfoDigestFileName);
		std::ifstream digestfs(backupInfoDigestFileName.c_str());
		if (!digestfs) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_READ_BACKUP_INFO_FILE_FAILED,
				"open " << backupInfoDigestFileName.c_str() << " failed.");
		}

		util::FileSystem::createPath(
			path.c_str(), BACKUP_INFO_FILE_NAME.c_str(), backupInfoFileName);
		std::ifstream ifs(backupInfoFileName.c_str());
		if (!ifs) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_READ_BACKUP_INFO_FILE_FAILED,
				"open " << backupInfoFileName.c_str() << " failed.");
		}
		std::string jsonString;
		std::getline(ifs, jsonString);

		picojson::value value;
		std::string error;
		picojson::parse(value, jsonString.begin(), jsonString.end(), &error);
		if (!error.empty()) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_READ_BACKUP_INFO_FILE_FAILED,
				"Backup info file is corrupted: " << error.c_str());
		}
		picojson::value digest;
		error = picojson::parse(digest, digestfs);
		if (!error.empty()) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_READ_BACKUP_INFO_FILE_FAILED,
				"Backup info digest file is corrupted: " << error.c_str());
		}
		if (digest.is<picojson::object>() &&
			digest.get("digestValue").is<std::string>()) {
			SHA256_Data(reinterpret_cast<const uint8_t *>(jsonString.data()),
				jsonString.size(), digestData_);
			digestData_[SHA256_DIGEST_STRING_LENGTH] = '\0';
			GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL,
				GS_TRACE_RM_BACKUP_INFO_FILE_DIGEST,
				"read digest = " << digestData_);
			GS_TRACE_INFO(RECOVERY_MANAGER_DETAIL,
				GS_TRACE_RM_BACKUP_INFO_FILE_DIGEST,
				"file digest = "
					<< digest.get("digestValue").get<std::string>());
			if (digest.get("digestValue").get<std::string>() !=
				std::string(digestData_)) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_READ_BACKUP_INFO_FILE_FAILED,
					"Backup info file is changed illegally.");
			}
		}
		else {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_READ_BACKUP_INFO_FILE_FAILED,
				"Backup info digest file is corrupted. : "
					<< picojson::value(value).serialize().c_str()
					<< picojson::value(digest).serialize().c_str());
		}

		status_ =
			value.get("backupInfo").get("backupStatus").get<std::string>();
		if (status_ != "end") {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_READ_BACKUP_INFO_FILE_FAILED,
				"Backup is not completed.");
		}
		double jsonBackupInfoVersion =
			value.get("backupInfo").get("backupInfoFileVersion").get<double>();
		uint32_t backupInfoVersion =
			static_cast<uint32_t>(jsonBackupInfoVersion);
		if (backupInfoVersion != BACKUP_INFO_FILE_VERSION) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_READ_BACKUP_INFO_FILE_FAILED,
				"BackupInfo file version is unknown.: version="
					<< backupInfoVersion);
		}

		double jsonPartitionNum =
			value.get("configInfo").get("partitionNum").get<double>();
		uint32_t partitionNum = static_cast<uint32_t>(jsonPartitionNum);
		double jsonPartitionGroupNum =
			value.get("configInfo").get("groupNum").get<double>();
		uint32_t partitionGroupNum =
			static_cast<uint32_t>(jsonPartitionGroupNum);

		if (partitionNum != partitionNum_) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_READ_BACKUP_INFO_FILE_FAILED,
				"Parameter partitionNum is not match.: config file="
					<< partitionNum_ << ", backupFile=" << partitionNum);
		}
		if (partitionGroupNum != partitionGroupNum_) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_READ_BACKUP_INFO_FILE_FAILED,
				"Parameter concurrency is not match.: config file="
					<< partitionGroupNum_
					<< ", backupFile=" << partitionGroupNum);
		}
		picojson::array cpIdArray =
			value.get("backupInfo").get("checkpointId").get<picojson::array>();
		picojson::array cpFileSizeArray =
			value.get("backupInfo").get("cpFileSize").get<picojson::array>();
		picojson::array logFileSizeArray =
			value.get("backupInfo").get("logFileSize").get<picojson::array>();

		double jsonIncrementalBackupMode =
			value.get("backupInfo").get("incrementalBackupMode").get<double>();
		incrementalBackupMode_ = static_cast<BackupInfo::IncrementalBackupMode>(
			static_cast<uint32_t>(jsonIncrementalBackupMode));

		picojson::array incrementalBackupCpIdArray =
			value.get("backupInfo")
				.get("incrementalBackupCpId")
				.get<picojson::array>();

		if (partitionGroupNum != cpIdArray.size() ||
			partitionGroupNum != cpFileSizeArray.size() ||
			partitionGroupNum != logFileSizeArray.size() ||
			partitionGroupNum != incrementalBackupCpIdArray.size()) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_READ_BACKUP_INFO_FILE_FAILED,
				"Backup info file is corrupted. partitionGroupNum="
					<< partitionGroupNum
					<< ", checkpointIdListSize=" << cpIdArray.size()
					<< ", cpFileSizeListSize=" << cpFileSizeArray.size()
					<< ", logFileSizeListSize=" << logFileSizeArray.size()
					<< ", incrementalBackupCpIdListSize="
					<< incrementalBackupCpIdArray.size());
		}
		cpIdList_.clear();
		cpIdList_.reserve(partitionGroupNum);
		for (picojson::array::iterator itr = cpIdArray.begin();
			 itr != cpIdArray.end(); ++itr) {
			cpIdList_.push_back(
				static_cast<CheckpointId>((*itr).get<double>()));
		}

		cpFileSizeList_.clear();
		cpFileSizeList_.reserve(partitionGroupNum);
		for (picojson::array::iterator itr = cpFileSizeArray.begin();
			 itr != cpFileSizeArray.end(); ++itr) {
			cpFileSizeList_.push_back(
				static_cast<uint64_t>((*itr).get<double>()));
		}

		logFileSizeList_.clear();
		logFileSizeList_.reserve(partitionGroupNum);
		for (picojson::array::iterator itr = logFileSizeArray.begin();
			 itr != logFileSizeArray.end(); ++itr) {
			logFileSizeList_.push_back(
				static_cast<uint64_t>((*itr).get<double>()));
		}

		incrementalBackupCpIdList_.clear();
		incrementalBackupCpIdList_.reserve(partitionGroupNum);
		for (picojson::array::iterator itr = incrementalBackupCpIdArray.begin();
			 itr != incrementalBackupCpIdArray.end(); ++itr) {
			picojson::array &tmpArray = (*itr).get<picojson::array>();
			picojson::array::iterator itr2 = tmpArray.begin();
			std::vector<CheckpointId> tmpVector;
			tmpVector.reserve(tmpArray.size());
			for (; itr2 != tmpArray.end(); ++itr2) {
				tmpVector.push_back(
					static_cast<CheckpointId>((*itr2).get<double>()));
			}
			incrementalBackupCpIdList_.push_back(tmpVector);
		}
		checkCpFileSizeFlag_ = true;
		if (value.get("backupInfo").contains("checkCpFileSize")) {
			checkCpFileSizeFlag_ = value.get("backupInfo").get("checkCpFileSize").get<bool>();
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e,
			"Read " << backupInfoFileName.c_str()
					<< " failed. (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void RecoveryManager::BackupInfo::setConfigValue(
	uint32_t partitionNum, uint32_t partitionGroupNum) {
	partitionNum_ = partitionNum;
	partitionGroupNum_ = partitionGroupNum;
}

void RecoveryManager::BackupInfo::startBackup(const std::string &backupPath) {
	reset();

	backupPath_ = backupPath;
	status_ = "start";
	incrementalBackupMode_ = INCREMENTAL_BACKUP_NONE;

	backupStartTime_ = util::DateTime::now(false).getUnixTime();

	cpIdList_.assign(partitionGroupNum_, 0);
	cpFileSizeList_.assign(partitionGroupNum_, 0);
	logFileSizeList_.assign(partitionGroupNum_, 0);
	cpFileCopyTimeList_.assign(partitionGroupNum_, 0.0);
	logFileCopyTimeList_.assign(partitionGroupNum_, 0.0);

	std::vector<CheckpointId> tmpCpIdList;
	incrementalBackupCpIdList_.assign(partitionGroupNum_, tmpCpIdList);

	writeBackupInfoFile();
}

void RecoveryManager::BackupInfo::startIncrementalBackup(
	const std::string &backupPath, const std::string &lastBackupPath,
	int32_t mode) {
	reset();

	backupPath_ = backupPath;
	status_ = "start";
	switch (mode) {
	case CP_INCREMENTAL_BACKUP_LEVEL_0:
		incrementalBackupMode_ = INCREMENTAL_BACKUP_LV0;
		break;
	case CP_INCREMENTAL_BACKUP_LEVEL_1_CUMULATIVE:
		incrementalBackupMode_ = INCREMENTAL_BACKUP_LV1_CUMULATIVE;
		break;
	case CP_INCREMENTAL_BACKUP_LEVEL_1_DIFFERENTIAL:
		incrementalBackupMode_ = INCREMENTAL_BACKUP_LV1_DIFFERENTIAL;
		break;
	default:
		assert(false);
	}

	backupStartTime_ = util::DateTime::now(false).getUnixTime();

	cpIdList_.assign(partitionGroupNum_, 0);
	cpFileSizeList_.assign(partitionGroupNum_, 0);
	logFileSizeList_.assign(partitionGroupNum_, 0);
	cpFileCopyTimeList_.assign(partitionGroupNum_, 0.0);
	logFileCopyTimeList_.assign(partitionGroupNum_, 0.0);

	std::vector<CheckpointId> tmpCpIdList;
	incrementalBackupCpIdList_.assign(partitionGroupNum_, tmpCpIdList);

	if (CP_INCREMENTAL_BACKUP_LEVEL_0 == mode) {
		writeBackupInfoFile();
	}
	else {
		readBackupInfoFile(lastBackupPath);
		if (CP_INCREMENTAL_BACKUP_LEVEL_1_CUMULATIVE == mode) {
			for (PartitionGroupId pgId = 0; pgId < partitionGroupNum_; ++pgId) {
				std::vector<CheckpointId> &cpIdList =
					incrementalBackupCpIdList_[pgId];
				assert(cpIdList.size() > 0);
				if (cpIdList.size() > 1) {
					cpIdList.erase(cpIdList.begin() + 1, cpIdList.end());
				}
			}
		}
		else {
		}
		backupPath_ = backupPath;
		status_ = "start";
		backupStartTime_ = util::DateTime::now(false).getUnixTime();
		writeBackupInfoFile();
	}
}

void RecoveryManager::BackupInfo::endBackup() {
	status_ = "end";
	backupEndTime_ = util::DateTime::now(false).getUnixTime();

	writeBackupInfoFile();
}

void RecoveryManager::BackupInfo::errorBackup() {
	status_ = "error";

	writeBackupInfoFile();
}

void RecoveryManager::BackupInfo::startCpFileCopy(
	PartitionGroupId pgId, CheckpointId cpId) {
	currentPartitionGroupId_ = pgId;
	cpIdList_[pgId] = cpId;
	cpFileCopyStartTime_ = util::DateTime::now(false).getUnixTime();
}

void RecoveryManager::BackupInfo::endCpFileCopy(
	PartitionGroupId pgId, uint64_t fileSize) {
	cpFileSizeList_[pgId] = fileSize;
	cpFileCopyEndTime_ = util::DateTime::now(false).getUnixTime();
	cpFileCopyTimeList_[pgId] =
		static_cast<double>(cpFileCopyEndTime_ - cpFileCopyStartTime_) / 1000.0;
}

void RecoveryManager::BackupInfo::startIncrementalBlockCopy(
	PartitionGroupId pgId, int32_t mode) {
	currentPartitionGroupId_ = pgId;
	switch (mode) {
	case CP_INCREMENTAL_BACKUP_LEVEL_0:
		incrementalBackupMode_ = INCREMENTAL_BACKUP_LV0;
		break;
	case CP_INCREMENTAL_BACKUP_LEVEL_1_CUMULATIVE:
		incrementalBackupMode_ = INCREMENTAL_BACKUP_LV1_CUMULATIVE;
		break;
	case CP_INCREMENTAL_BACKUP_LEVEL_1_DIFFERENTIAL:
		incrementalBackupMode_ = INCREMENTAL_BACKUP_LV1_DIFFERENTIAL;
		break;
	default:
		assert(false);
	}
	cpFileCopyStartTime_ = util::DateTime::now(false).getUnixTime();
}

void RecoveryManager::BackupInfo::endIncrementalBlockCopy(
	PartitionGroupId pgId, CheckpointId cpId) {
	cpFileCopyEndTime_ = util::DateTime::now(false).getUnixTime();
	cpFileCopyTimeList_[pgId] =
		static_cast<double>(cpFileCopyEndTime_ - cpFileCopyStartTime_) / 1000.0;
	incrementalBackupCpIdList_[pgId].push_back(cpId);
}

void RecoveryManager::BackupInfo::startLogFileCopy() {
	logFileCopyStartTime_ = util::DateTime::now(false).getUnixTime();
}

void RecoveryManager::BackupInfo::endLogFileCopy(
	PartitionGroupId pgId, uint64_t fileSize) {
	logFileSizeList_[pgId] = fileSize;
	logFileCopyEndTime_ = util::DateTime::now(false).getUnixTime();
	logFileCopyTimeList_[pgId] =
		static_cast<double>(logFileCopyEndTime_ - logFileCopyStartTime_) /
		1000.0;

	writeBackupInfoFile();
}

void RecoveryManager::BackupInfo::setBackupPath(const std::string &backupPath) {
	reset();
	backupPath_ = backupPath;
}

void RecoveryManager::BackupInfo::setDataStorePath(
	const std::string &dataStorePath) {
	dataStorePath_ = dataStorePath;
}

void RecoveryManager::BackupInfo::setGSVersion(const std::string &gsVersion) {
	gsVersion_ = gsVersion;
}

CheckpointId RecoveryManager::BackupInfo::getBackupCheckpointId(
	PartitionGroupId pgId) {
	if (pgId > cpIdList_.size()) {
		return UNDEF_CHECKPOINT_ID;
	}
	return cpIdList_[pgId];
}

uint64_t RecoveryManager::BackupInfo::getBackupCheckpointFileSize(
	PartitionGroupId pgId) {
	if (pgId > cpFileSizeList_.size()) {
		return 0;
	}
	return cpFileSizeList_[pgId];
}

uint64_t RecoveryManager::BackupInfo::getBackupLogFileSize(
	PartitionGroupId pgId) {
	if (pgId > logFileSizeList_.size()) {
		return 0;
	}
	return logFileSizeList_[pgId];
}

bool RecoveryManager::BackupInfo::isIncrementalBackup() {
	return (incrementalBackupMode_ != INCREMENTAL_BACKUP_NONE);
}

RecoveryManager::BackupInfo::IncrementalBackupMode
RecoveryManager::BackupInfo::getIncrementalBackupMode() {
	return incrementalBackupMode_;
}

void RecoveryManager::BackupInfo::setIncrementalBackupMode(
	IncrementalBackupMode mode) {
	incrementalBackupMode_ = mode;
}

std::vector<CheckpointId>
	&RecoveryManager::BackupInfo::getIncrementalBackupCpIdList(
		PartitionGroupId pgId) {
	return incrementalBackupCpIdList_[pgId];
}

bool RecoveryManager::BackupInfo::getCheckCpFileSizeFlag() {
	return checkCpFileSizeFlag_;
}

void RecoveryManager::BackupInfo::setCheckCpFileSizeFlag(bool flag) {
	checkCpFileSizeFlag_ = flag;
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
