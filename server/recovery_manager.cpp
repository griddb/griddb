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
#include "cluster_service.h"
#include "gs_error.h"
#include "partition.h"
#include "interchangeable.h"

#include "checkpoint_service.h"
#include "chunk_manager.h"
#include "partition_table.h"
#include "system_service.h"
#include "transaction_service.h"
#include "cluster_manager.h"

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
	  CHUNK_EXP_SIZE_(static_cast<uint8_t>(util::nextPowerBitsOf2(
		  configTable.getUInt32(CONFIG_TABLE_DS_STORE_BLOCK_SIZE)))),
	  CHUNK_SIZE_(1 << CHUNK_EXP_SIZE_),
	  recoveryOnlyCheckChunk_(
		  configTable.get<bool>(CONFIG_TABLE_DEV_RECOVERY_ONLY_CHECK_CHUNK)),
	  recoveryDownPoint_(configTable.get<const char8_t *>(
		  CONFIG_TABLE_DEV_RECOVERY_DOWN_POINT)),
	  recoveryDownCount_(
		  configTable.get<int32_t>(CONFIG_TABLE_DEV_RECOVERY_DOWN_COUNT)),
	  recoveryTargetPartitionMode_(false),
	  releaseUnusedFileBlocks_(releaseUnusedFileBlocks),
	  pt_(NULL),
	  clsSvc_(NULL), sysSvc_(NULL), txnMgr_(NULL),
	  partitionList_(NULL)
{
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
void RecoveryManager::initialize(ManagerSet &resourceSet) {
	pt_ = resourceSet.pt_;
	clsSvc_ = resourceSet.clsSvc_;
	sysSvc_ = resourceSet.sysSvc_;
	txnMgr_ = resourceSet.txnMgr_;
	partitionList_ = resourceSet.partitionList_;
	resourceSet.stats_->addUpdator(&statUpdator_);

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
	backupInfo_.removeBackupInfoFile(*partitionList_);
}

void RecoveryManager::checkExistingFiles1(ConfigTable &param, bool &createFlag,
		bool &existBackupInfoFile, bool forceRecoveryFromExistingFiles) {

	createFlag = false;
	existBackupInfoFile = false;
	const char8_t *const dbPath =
		param.get<const char8_t *>(CONFIG_TABLE_DS_DB_PATH);
	const char8_t *const txnLogPath =
		param.get<const char8_t *>(CONFIG_TABLE_DS_TRANSACTION_LOG_PATH);
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
		const uint32_t partitionNum = param.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM);

		util::Directory dir(dbPath);

		bool existBackupInfoFile1, existBackupInfoFile2;

		scanExistingFiles(param,
				existBackupInfoFile1, existBackupInfoFile2);

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
		bool existDataFile = false;
		for (PartitionId pId = 0; pId < partitionNum; ++pId) {
			std::vector<int32_t> logFileInfo;
			std::vector<int32_t> cplogFileInfo;
			std::vector<int32_t> dataFileInfo;

			{
				util::NormalOStringStream ossTxnLog;
				ossTxnLog << txnLogPath << "/" << pId;
				UtilFile::getFileNumbers(
					ossTxnLog.str().c_str(), LogManagerConst::XLOG_FILE_SUFFIX,
					LogManagerConst::INCREMENTAL_FILE_SUFFIX, logFileInfo);

				if (logFileInfo.size() == 0) {
					if (forceRecoveryFromExistingFiles) {

					}
					else if (existLogFile) {
						GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_LOG_FILE_NOT_FOUND,
							"Log files are not found: pId=" << pId);
					}
				} else {
					existLogFile = true;
				}
			}
			{
				util::NormalOStringStream ossCpLog;
				ossCpLog << dbPath << "/" << pId;
				UtilFile::getFileNumbers(
					ossCpLog.str().c_str(), LogManagerConst::CPLOG_FILE_SUFFIX,
					LogManagerConst::INCREMENTAL_FILE_SUFFIX, cplogFileInfo);
			}
			{
				util::NormalOStringStream ossData;
				ossData << dbPath << "/" << pId;
				UtilFile::getFileNumbers(
					ossData.str().c_str(), ".dat",
					LogManagerConst::INCREMENTAL_FILE_SUFFIX, dataFileInfo);

				if (dataFileInfo.empty()) {
					if (cplogFileInfo.size() > 0) {
						if (forceRecoveryFromExistingFiles) {
							if (logFileInfo.size() != 0) {
								GS_THROW_SYSTEM_ERROR(
									GS_ERROR_RM_LOG_FILE_CP_FILE_MISMATCH,
									"Data file is not exist, and log "
									"files are not complete: pId="
										<< pId);
							}
							createFlag = true;
						}
						else {
							GS_THROW_SYSTEM_ERROR(
								GS_ERROR_RM_LOG_FILE_CP_FILE_MISMATCH,
								"Data file is not exist, and log files "
								"are not complete: pId="
									<< pId);
						}
					}

				} else {
					existDataFile = true;
				}
			}
		}

		if (!existLogFile) {
			if (existDataFile) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_CP_FILE_WITH_NO_LOG_FILE,
					"Data file is exist, but log files are not exist");
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

void RecoveryManager::scanExistingFiles(
		ConfigTable &param,
		bool &existBackupInfoFile1, bool &existBackupInfoFile2) {

	const uint32_t partitionNum = param.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM);

	const char8_t *const dbPath =
		param.get<const char8_t *>(CONFIG_TABLE_DS_DB_PATH);

	existBackupInfoFile1 = false;
	existBackupInfoFile2 = false;

	util::Directory dir(dbPath);
	for (u8string fileName; dir.nextEntry(fileName);) {
		if (checkBackupInfoFileName(fileName)) {
			existBackupInfoFile1 = true;
		}
		else if (checkBackupInfoDigestFileName(fileName)) {
			existBackupInfoFile2 = true;
		}
		else {
			continue;
		}
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
void RecoveryManager::checkRestoreFile() {
	const uint32_t partitionNum = pgConfig_.getPartitionCount();
	const std::string& dataPath = backupInfo_.getDataStorePath();
	for (uint32_t pId = 0; pId < partitionNum; ++pId) {
		Partition& partition = partitionList_->partition(pId);
		const int64_t cpLogVer = backupInfo_.getBackupCheckpointId(pId);
		util::NormalOStringStream oss1;
		oss1 << dataPath.c_str() << "/" << pId << "/" << pId << "_" << cpLogVer << LogManagerConst::CPLOG_FILE_SUFFIX;
		if (!util::FileSystem::exists(oss1.str().c_str())) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_REMOVE_BACKUP_INFO_FILE_FAILED,
				"File " << oss1.str().c_str() << " not found.");
		}
		if (backupInfo_.isIncrementalBackup()) {
			util::NormalOStringStream oss2;
			oss2 << dataPath.c_str() << "/" << pId;
			util::NormalOStringStream oss3;
			oss3 << LogManagerConst::INCREMENTAL_FILE_SUFFIX << LogManagerConst::CPLOG_FILE_SUFFIX;
			std::vector<int32_t> chunkLogs;
			UtilFile::getFileNumbers(oss2.str().c_str(), oss3.str().c_str(), NULL, chunkLogs);
			std::vector<CheckpointId> cpIdList = backupInfo_.getIncrementalBackupCpIdList(pId);
			std::set<int64_t> infoVerSet;
			for (const auto& itr : cpIdList) {
				infoVerSet.insert(static_cast<int64_t>(itr));
			}
			if (infoVerSet.erase(cpLogVer) != 1) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_READ_BACKUP_INFO_FILE_FAILED,
						"BackupInfoFile is corrupted. ");
			}
			for (const auto& itr : chunkLogs) {
				if (infoVerSet.erase(itr) != 1) {
					GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_INVALID_FILE_FOUND,
						"Invalid file found. (" << pId << "_" << itr << oss3.str().c_str() << ")");
				}
			}
			if (!infoVerSet.empty()) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_INCOMPLETE_CP_FILE,
					"Restore file not found. (" << pId << "_" << *infoVerSet.begin() << oss3.str().c_str() << ")");
			}
		}
	}
}

void RecoveryManager::recovery(util::StackAllocator &alloc, bool existBackup,
	bool forceRecoveryFromExistingFiles,
	const std::string &recoveryTargetPartition, bool forLongArchive)
{
	try {
		util::StackAllocator::Scope scope(alloc);
		const uint64_t currentTime = util::Stopwatch::currentClock();
		const uint32_t partitionNum = pgConfig_.getPartitionCount();
		if (existBackup) {
			checkRestoreFile();
		}
		int64_t releasedBlockCount = 0;
		for (uint32_t pId = 0; pId < partitionNum; ++pId) {
			Partition &partition = partitionList_->partition(pId);
			InterchangeableStoreMemory& ism = partitionList_->interchangeableStoreMemory();
			partition.recover(&ism);
			ism.calculate();
			ism.resize();
			if (releaseUnusedFileBlocks_) {
				releasedBlockCount += partition.chunkManager().releaseUnusedFileBlocks();
			}
			progressPercent_ = static_cast<uint32_t>(
					(pId + 1.0) / partitionNum * 90);
		}
		GS_TRACE_INFO(RECOVERY_MANAGER, GS_TRACE_RM_RECOVERY_INFO,
			"Partition recovery finished (elapsedMillis="
			<< (util::Stopwatch::currentClock() - currentTime) / 1000 << ")");

	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(
			e, "Recovery failed. reason=(" << GS_EXCEPTION_MESSAGE(e) << ")");
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

/*!
	@brief コンストラクタ
*/
RecoveryManager::BackupInfo::BackupInfo()
	: partitionNum_(0),
	  partitionGroupNum_(0),
	  currentPartitionId_(0),
	  backupStartTime_(0),
	  backupEndTime_(0),
	  cpFileCopyStartTime_(0),
	  cpFileCopyEndTime_(0),
	  logFileCopyStartTime_(0),
	  logFileCopyEndTime_(0),
	  incrementalBackupMode_(INCREMENTAL_BACKUP_NONE),
	  checkCpFileSizeFlag_(true), skipBaselineFlag_(false) {}

/*!
	@brief デストラクタ
*/
RecoveryManager::BackupInfo::~BackupInfo() {}

/*!
	@brief 内部バックアップ情報リセット
*/
void RecoveryManager::BackupInfo::reset() {
	currentPartitionId_ = 0;

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
	skipBaselineFlag_ = false;
}

/*!
	@brief バックアップ情報ファイル書き込み
*/
void RecoveryManager::BackupInfo::writeBackupInfoFile() {
	if (skipBaselineFlag_) {
		return;
	}
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
			picojson::value(static_cast<double>(currentPartitionId_));

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
		for (PartitionId pId = 0; pId < partitionNum_; ++pId) {
			cpIdList.push_back(
				picojson::value(static_cast<double>(cpIdList_[pId])));
			cpFileSizeList.push_back(
				picojson::value(static_cast<double>(cpFileSizeList_[pId])));
			logFileSizeList.push_back(
				picojson::value(static_cast<double>(logFileSizeList_[pId])));
			cpFileCopyTimeList.push_back(
				picojson::value(cpFileCopyTimeList_[pId]));
			logFileCopyTimeList.push_back(
				picojson::value(logFileCopyTimeList_[pId]));

			picojson::array tmpCpIdList;
			std::vector<CheckpointId> &innerCpIdList =
				incrementalBackupCpIdList_[pId];
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

/*!
	@brief バックアップ情報ファイル削除
*/
void RecoveryManager::BackupInfo::removeBackupInfoFile(PartitionList &ptList) {
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
}

/*!
	@brief バックアップ情報ファイル読み込み
*/
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

		if (partitionNum != cpIdArray.size() ||
			partitionNum != cpFileSizeArray.size() ||
			partitionNum != logFileSizeArray.size() ||
			partitionNum != incrementalBackupCpIdArray.size()) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_RM_READ_BACKUP_INFO_FILE_FAILED,
				"Backup info file is corrupted. partitionNum="
					<< partitionNum
					<< ", checkpointIdListSize=" << cpIdArray.size()
					<< ", cpFileSizeListSize=" << cpFileSizeArray.size()
					<< ", logFileSizeListSize=" << logFileSizeArray.size()
					<< ", incrementalBackupCpIdListSize="
					<< incrementalBackupCpIdArray.size());
		}
		cpIdList_.clear();
		cpIdList_.reserve(partitionNum);
		for (picojson::array::iterator itr = cpIdArray.begin();
			 itr != cpIdArray.end(); ++itr) {
			cpIdList_.push_back(
				static_cast<CheckpointId>((*itr).get<double>()));
		}

		cpFileSizeList_.clear();
		cpFileSizeList_.reserve(partitionNum);
		for (picojson::array::iterator itr = cpFileSizeArray.begin();
			 itr != cpFileSizeArray.end(); ++itr) {
			cpFileSizeList_.push_back(
				static_cast<uint64_t>((*itr).get<double>()));
		}

		logFileSizeList_.clear();
		logFileSizeList_.reserve(partitionNum);
		for (picojson::array::iterator itr = logFileSizeArray.begin();
			 itr != logFileSizeArray.end(); ++itr) {
			logFileSizeList_.push_back(
				static_cast<uint64_t>((*itr).get<double>()));
		}

		incrementalBackupCpIdList_.clear();
		incrementalBackupCpIdList_.reserve(partitionNum);
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

void RecoveryManager::BackupInfo::startBackup(
		const std::string &backupPath, bool skipBaselineFlag) {
	reset();

	backupPath_ = backupPath;
	status_ = "start";
	incrementalBackupMode_ = INCREMENTAL_BACKUP_NONE;
	skipBaselineFlag_ = skipBaselineFlag;

	backupStartTime_ = util::DateTime::now(false).getUnixTime();

	cpIdList_.assign(partitionNum_, 0);
	cpFileSizeList_.assign(partitionNum_, 0);
	logFileSizeList_.assign(partitionNum_, 0);
	cpFileCopyTimeList_.assign(partitionNum_, 0.0);
	logFileCopyTimeList_.assign(partitionNum_, 0.0);

	std::vector<CheckpointId> tmpCpIdList;
	incrementalBackupCpIdList_.assign(partitionNum_, tmpCpIdList);

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

	cpIdList_.assign(partitionNum_, 0);
	cpFileSizeList_.assign(partitionNum_, 0);
	logFileSizeList_.assign(partitionNum_, 0);
	cpFileCopyTimeList_.assign(partitionNum_, 0.0);
	logFileCopyTimeList_.assign(partitionNum_, 0.0);

	std::vector<CheckpointId> tmpCpIdList;
	incrementalBackupCpIdList_.assign(partitionNum_, tmpCpIdList);

	if (CP_INCREMENTAL_BACKUP_LEVEL_0 == mode) {
		writeBackupInfoFile();
	}
	else {
		readBackupInfoFile(lastBackupPath);
		if (CP_INCREMENTAL_BACKUP_LEVEL_1_CUMULATIVE == mode) {
			for (PartitionId pId = 0; pId < partitionNum_; ++pId) {
				std::vector<CheckpointId> &cpIdList =
						incrementalBackupCpIdList_[pId];
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
	PartitionId pId, CheckpointId cpId) {
	currentPartitionId_ = pId;
	cpIdList_[pId] = cpId;
	cpFileCopyStartTime_ = util::DateTime::now(false).getUnixTime();
}

void RecoveryManager::BackupInfo::endCpFileCopy(
	PartitionId pId, uint64_t fileSize) {
	cpFileSizeList_[pId] = fileSize;
	cpFileCopyEndTime_ = util::DateTime::now(false).getUnixTime();
	cpFileCopyTimeList_[pId] =
		static_cast<double>(cpFileCopyEndTime_ - cpFileCopyStartTime_) / 1000.0;
}

void RecoveryManager::BackupInfo::startIncrementalBlockCopy(
	PartitionId pId, int32_t mode) {
	currentPartitionId_ = pId;
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
	PartitionId pId, CheckpointId cpId) {
	cpFileCopyEndTime_ = util::DateTime::now(false).getUnixTime();
	cpFileCopyTimeList_[pId] =
		static_cast<double>(cpFileCopyEndTime_ - cpFileCopyStartTime_) / 1000.0;
	incrementalBackupCpIdList_[pId].push_back(cpId);
}

void RecoveryManager::BackupInfo::startLogFileCopy() {
	logFileCopyStartTime_ = util::DateTime::now(false).getUnixTime();
}

void RecoveryManager::BackupInfo::endLogFileCopy(
	PartitionId pId, uint64_t fileSize) {
	logFileSizeList_[pId] = fileSize;
	logFileCopyEndTime_ = util::DateTime::now(false).getUnixTime();
	logFileCopyTimeList_[pId] =
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

const std::string& RecoveryManager::BackupInfo::getDataStorePath() {
	return dataStorePath_;
}

void RecoveryManager::BackupInfo::setGSVersion(const std::string &gsVersion) {
	gsVersion_ = gsVersion;
}

CheckpointId RecoveryManager::BackupInfo::getBackupCheckpointId(
	PartitionId pId) {
	if (pId > cpIdList_.size()) {
		return UNDEF_CHECKPOINT_ID;
	}
	return cpIdList_[pId];
}

uint64_t RecoveryManager::BackupInfo::getBackupCheckpointFileSize(
	PartitionId pId) {
	if (pId > cpFileSizeList_.size()) {
		return 0;
	}
	return cpFileSizeList_[pId];
}

uint64_t RecoveryManager::BackupInfo::getBackupLogFileSize(
	PartitionId pId) {
	if (pId > logFileSizeList_.size()) {
		return 0;
	}
	return logFileSizeList_[pId];
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
		PartitionId pId) {
	return incrementalBackupCpIdList_[pId];
}

bool RecoveryManager::BackupInfo::getCheckCpFileSizeFlag() {
	return checkCpFileSizeFlag_;
}

void RecoveryManager::BackupInfo::setCheckCpFileSizeFlag(bool flag) {
	checkCpFileSizeFlag_ = flag;
}
