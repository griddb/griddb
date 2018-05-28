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
	@brief Implementation of CheckpointService
*/
#include "checkpoint_service.h"
#include "util/trace.h"
#include "cluster_event_type.h"  
#include "config_table.h"
#include "data_store.h"
#include "log_manager.h"
#include "transaction_service.h"

#include "picojson.h"
#include <fstream>
#include "zlib_utils.h"

#ifndef _WIN32
#include <signal.h>  
#endif


UTIL_TRACER_DECLARE(CHECKPOINT_SERVICE);
UTIL_TRACER_DECLARE(CHECKPOINT_SERVICE_DETAIL);
UTIL_TRACER_DECLARE(CHECKPOINT_SERVICE_STATUS_DETAIL);
UTIL_TRACER_DECLARE(IO_MONITOR);

const std::string CheckpointService::PID_LSN_INFO_FILE_NAME("gs_lsn_info.json");

const uint32_t CheckpointService::MAX_BACKUP_NAME_LEN = 12;

const char *const CheckpointService::SYNC_TEMP_FILE_SUFFIX =
		"_sync_temp";

const CheckpointService::SyncSequentialNumber
	CheckpointService::UNDEF_SYNC_SEQ_NUMBER = INT64_MAX;

const CheckpointService::SyncSequentialNumber
	CheckpointService::MAX_SYNC_SEQ_NUMBER = UNDEF_SYNC_SEQ_NUMBER - 1;

/*!
	@brief Constructor of CheckpointService
*/
CheckpointService::CheckpointService(
		const ConfigTable &config, EventEngine::Config eeConfig,
		EventEngine::Source source, const char8_t *name,
		ServiceThreadErrorHandler &serviceThreadErrorHandler)
	: serviceThreadErrorHandler_(serviceThreadErrorHandler),
	  ee_(createEEConfig(config, eeConfig), source, name),
	  clusterService_(NULL),
	  clsEE_(NULL),
	  transactionService_(NULL),
	  transactionManager_(NULL),
	  txnEE_(NULL),
	  systemService_(NULL),
	  logManager_(NULL),
	  dataStore_(NULL),
	  chunkManager_(NULL),
	  partitionTable_(NULL),
	  initailized_(false),
	  syncService_(NULL),
	  fixedSizeAlloc_(NULL),
	  requestedShutdownCheckpoint_(false),
	  pgConfig_(config),
	  cpInterval_(config.get<int32_t>(
		  CONFIG_TABLE_CP_CHECKPOINT_INTERVAL)),  
	  logWriteMode_(config.get<int32_t>(
		  CONFIG_TABLE_DS_LOG_WRITE_MODE)),  
	  syncTempTopPath_(config.get<const char8_t *>(CONFIG_TABLE_DS_SYNC_TEMP_PATH)),
	  chunkCopyIntervalMillis_(config.get<int32_t>(
		  CONFIG_TABLE_CP_CHECKPOINT_COPY_INTERVAL)),  

	  currentDuplicateLogMode_(false),
	  currentCpGrpId_(UINT32_MAX),
	  currentCpPId_(UNDEF_PARTITIONID),
	  parallelCheckpoint_(config.get<bool>(
		  CONFIG_TABLE_CP_USE_PARALLEL_MODE)),  
	  errorOccured_(false),
	  enableLsnInfoFile_(true),  
	  lastMode_(CP_UNDEF)
{
	statUpdator_.service_ = this;
	try {

		ee_.setHandler(CP_REQUEST_CHECKPOINT, checkpointServiceMainHandler_);

		ee_.setHandler(
			CP_REQUEST_GROUP_CHECKPOINT, checkpointServiceGroupHandler_);

		ee_.setHandler(CP_TIMER_LOG_FLUSH, flushLogPeriodicallyHandler_);

		ee_.setThreadErrorHandler(serviceThreadErrorHandler_);

		groupCheckpointStatus_.resize(
				config.getUInt32(CONFIG_TABLE_DS_CONCURRENCY));

		lsnInfo_.setConfigValue(
				this, config.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM),
				config.getUInt32(CONFIG_TABLE_DS_CONCURRENCY),
				config.get<const char8_t *>(CONFIG_TABLE_DS_DB_PATH));

		lastArchivedCpIdList_.assign(pgConfig_.getPartitionGroupCount(), 0);

		currentCheckpointIdList_.assign(pgConfig_.getPartitionCount(), 0);

		checkpointReadyList_.assign(pgConfig_.getPartitionCount(), 0);

		ssnList_.assign(pgConfig_.getPartitionCount(), UNDEF_SYNC_SEQ_NUMBER);
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(
				e, "Initialize failed. (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

EventEngine::Config &CheckpointService::createEEConfig(
	const ConfigTable &config, EventEngine::Config &eeConfig) {

	EventEngine::Config tmpConfig;
	eeConfig = tmpConfig;

	if (config.get<bool>(CONFIG_TABLE_CP_USE_PARALLEL_MODE)) {
		eeConfig.setConcurrency(config.getUInt32(CONFIG_TABLE_DS_CONCURRENCY));
	}
	eeConfig.setPartitionCount(config.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM));

	eeConfig.setAllAllocatorGroup(ALLOCATOR_GROUP_CP);

	return eeConfig;
}

/*!
	@brief Destructor of CheckpointService
*/
CheckpointService::~CheckpointService() {}

/*!
	@brief Initializer of CheckpointService
*/
void CheckpointService::initialize(ManagerSet &mgrSet) {
	try {
		clusterService_ = mgrSet.clsSvc_;
		clsEE_ = clusterService_->getEE();
		transactionService_ = mgrSet.txnSvc_;
		txnEE_ = transactionService_->getEE();
		transactionManager_ = mgrSet.txnMgr_;
		systemService_ = mgrSet.sysSvc_;
		partitionTable_ = mgrSet.pt_;
		logManager_ = mgrSet.logMgr_;
		dataStore_ = mgrSet.ds_;
		chunkManager_ = mgrSet.chunkMgr_;
		fixedSizeAlloc_ = mgrSet.fixedSizeAlloc_;

		mgrSet.stats_->addUpdator(&statUpdator_);

		checkpointServiceMainHandler_.initialize(mgrSet);
		checkpointServiceGroupHandler_.initialize(mgrSet);
		flushLogPeriodicallyHandler_.initialize(mgrSet);
		syncService_ = mgrSet.syncSvc_;
		serviceThreadErrorHandler_.initialize(mgrSet);
		initailized_ = true;
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(
				e, "Initialize failed. (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Starts an event.
*/
void CheckpointService::start(const Event::Source &eventSource) {
	try {
		if (!initailized_) {
			GS_THROW_USER_ERROR(GS_ERROR_CP_SERVICE_NOT_INITIALIZED, "");
		}
		ee_.start();

		Event requestEvent(
				eventSource, CP_REQUEST_CHECKPOINT, CP_HANDLER_PARTITION_ID);

		EventByteOutStream out = requestEvent.getOutStream();

		int32_t mode = CP_NORMAL;
		uint32_t flag = 0;
		std::string backupPath;  
		out << mode;
		out << flag;
		out << backupPath;
		out << UNDEF_SYNC_SEQ_NUMBER; 

		ee_.addTimer(requestEvent, changeTimeSecondToMilliSecond(cpInterval_));

		if (logWriteMode_ > 0 && logWriteMode_ < INT32_MAX) {
			Event flushLogEvent(
					eventSource, CP_TIMER_LOG_FLUSH, CP_SERIALIZED_PARTITION_ID);
			ee_.addPeriodicTimer(
					flushLogEvent, changeTimeSecondToMilliSecond(logWriteMode_));
		}
		if (!syncTempTopPath_.empty()) {
			if (util::FileSystem::exists(syncTempTopPath_.c_str()) &&
				util::FileSystem::isDirectory(syncTempTopPath_.c_str())) {
				util::Directory dir(syncTempTopPath_.c_str());
				u8string name;
				while(dir.nextEntry(name)) {
					u8string path;
					util::FileSystem::createPath(syncTempTopPath_.c_str(), name.c_str(), path);
					if (util::FileSystem::isDirectory(path.c_str())) {
						bool isNumber = true;
						for (u8string::const_iterator itr = name.begin();
								itr != name.end(); ++itr) {
							if (!isdigit(static_cast<uint8_t>(*itr))) {
								isNumber = false;
								break;
							}
						}
						if (isNumber) {
							try {
								util::FileSystem::remove(path.c_str(), true);
							}
							catch (std::exception &e) {
								UTIL_TRACE_EXCEPTION(
										CHECKPOINT_SERVICE, e,
										"Failed to remove syncTemp child dir (path=" <<
										path <<
										", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
							}
						}
					}
				}
			}
			else {
				try {
					util::FileSystem::createDirectoryTree(syncTempTopPath_.c_str());
				}
				catch (std::exception &e) {
					GS_RETHROW_SYSTEM_ERROR(
							e, "Create synctemp top directory failed. (path=\"" <<
							syncTempTopPath_ << "\"" <<
							", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
				}
				if (!util::FileSystem::isDirectory(syncTempTopPath_.c_str())) {
					GS_THROW_SYSTEM_ERROR(
							GS_ERROR_CP_SERVICE_START_FAILED,
							"The specified path is not directory. (path=" <<
							syncTempTopPath_ << ")");
				}
			}
		}
	}
	catch (std::exception &e) {
		clusterService_->setError(eventSource, &e);
		GS_RETHROW_SYSTEM_ERROR(
				e, "Start failed. (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Shuts down the event.
*/
void CheckpointService::shutdown() {
	ee_.shutdown();
}

/*!
	@brief Waits for the event shutdown.
*/
void CheckpointService::waitForShutdown() {
	ee_.waitForShutdown();
}

/*!
	@brief Returns the event.
*/
EventEngine *CheckpointService::getEE() {
	return &ee_;
}


/*!
	@brief Starts a checkpoint.
*/
void CheckpointService::requestNormalCheckpoint(
	const Event::Source &eventSource) {
	if (lastMode_ == CP_SHUTDOWN) {
		return;
	}
	try {
		if (requestedShutdownCheckpoint_) {
			GS_THROW_USER_ERROR(
					GS_ERROR_CP_CONTROLLER_ILLEAGAL_STATE,
					"Checkpoint cancelled: already requested shutdown ("
					"lastMode=" <<
					checkpointModeToString(lastMode_) << ")");
		}
		GS_TRACE_INFO(
				CHECKPOINT_SERVICE, GS_TRACE_CP_STATUS, "[NormalCP]requested.");

		Event requestEvent(
				eventSource, CP_REQUEST_CHECKPOINT, CP_HANDLER_PARTITION_ID);

		EventByteOutStream out = requestEvent.getOutStream();
		int32_t mode = CP_REQUESTED;
		uint32_t flag = 0;
		std::string backupPath;  
		out << mode;
		out << flag;
		out << backupPath;
		out << UNDEF_SYNC_SEQ_NUMBER; 

		ee_.add(requestEvent);
	}
	catch (std::exception &e) {
		clusterService_->setError(eventSource, &e);
		GS_RETHROW_SYSTEM_ERROR(
				e, "Request normal checkpoint failed. (reason=" <<
				GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Starts a Checkpoint for the Shutdown.
*/
void CheckpointService::requestShutdownCheckpoint(
	const Event::Source &eventSource) {
	if (lastMode_ == CP_SHUTDOWN) {
		return;
	}
	try {
		GS_TRACE_INFO(
				CHECKPOINT_SERVICE, GS_TRACE_CP_STATUS, "[ShutdownCP]requested.");

		Event requestEvent(
				eventSource, CP_REQUEST_CHECKPOINT, CP_HANDLER_PARTITION_ID);

		EventByteOutStream out = requestEvent.getOutStream();
		int32_t mode = CP_SHUTDOWN;
		uint32_t flag = 0;
		std::string backupPath;  
		out << mode;
		out << flag;
		out << backupPath;
		out << UNDEF_SYNC_SEQ_NUMBER; 

		if (!requestedShutdownCheckpoint_) {
			requestedShutdownCheckpoint_ = true;
			chunkCopyIntervalMillis_ = 0;  
			ee_.add(requestEvent);
		}
	}
	catch (std::exception &e) {
		clusterService_->setError(eventSource, &e);
		GS_RETHROW_SYSTEM_ERROR(
				e, "Request shutdowncheckpoint failed. (reason=" <<
				GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Starts a Checkpoint after the recovery.
*/
void CheckpointService::executeRecoveryCheckpoint(
	const Event::Source &eventSource) {
	if (lastMode_ == CP_SHUTDOWN) {
		return;
	}
	try {
		GS_TRACE_INFO(
				CHECKPOINT_SERVICE, GS_TRACE_CP_STATUS, "[RecoveryCheckpoint]");

		Event requestEvent(
				eventSource, CP_REQUEST_CHECKPOINT, CP_HANDLER_PARTITION_ID);

		EventByteOutStream out = requestEvent.getOutStream();
		int32_t mode = CP_AFTER_RECOVERY;
		uint32_t flag = 0;
		std::string backupPath;  
		out << mode;
		out << flag;
		out << backupPath;
		out << UNDEF_SYNC_SEQ_NUMBER; 

		ee_.add(requestEvent);
	}
	catch (std::exception &e) {
		clusterService_->setError(eventSource, &e);
		GS_RETHROW_SYSTEM_ERROR(
				e, "Recovery checkpoint failed. (reason=" <<
				GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Initializes CheckpointHandler.
*/
void CheckpointHandler::initialize(const ManagerSet &mgrSet) {
	try {
		checkpointService_ = mgrSet.cpSvc_;
		clusterService_ = mgrSet.clsSvc_;
		transactionService_ = mgrSet.txnSvc_;
		transactionManager_ = mgrSet.txnMgr_;
		logManager_ = mgrSet.logMgr_;
		dataStore_ = mgrSet.ds_;
		chunkManager_ = mgrSet.chunkMgr_;
		fixedSizeAlloc_ = mgrSet.fixedSizeAlloc_;
		config_ = mgrSet.config_;
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(
				e, "Initialize failed. (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Initializes CheckpointService.
*/
void CheckpointServiceMainHandler::operator()(EventContext &ec, Event &ev) {
	bool critical = false;
	try {
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);

		EventByteInStream in = ev.getInStream();

		int32_t mode;
		uint32_t flag;
		std::string backupPath;
		CheckpointService::SyncSequentialNumber ssn;

		in >> mode;
		in >> flag;
		in >> backupPath;
		in >> ssn; 

		if (mode == CP_AFTER_RECOVERY || mode == CP_SHUTDOWN) {
			critical = true;
		}

		if (mode == CP_NORMAL) {
			ec.getEngine().addTimer(
					ev, changeTimeSecondToMilliSecond(
					checkpointService_->getCheckpointInterval()));
		}

		checkpointService_->runCheckpoint(ec, mode, flag, backupPath, ssn);
	}
	catch (UserException &e) {
		checkpointService_->errorOccured_ = true;
		if (critical) {
			clusterService_->setError(ec, &e);
			GS_RETHROW_SYSTEM_ERROR(e, "");
		}
		else {
			UTIL_TRACE_EXCEPTION_WARNING(CHECKPOINT_SERVICE, e, "");
		}
	}
	catch (SystemException &) {
		checkpointService_->errorOccured_ = true;
		throw;
	}
	catch (std::exception &e) {
		checkpointService_->errorOccured_ = true;
		clusterService_->setError(ec, &e);
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Initializes CheckpointServiceGroupHandler.
*/
void CheckpointServiceGroupHandler::operator()(EventContext &ec, Event &ev) {
	bool critical = false;
	try {
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);

		EventByteInStream in = ev.getInStream();

		int32_t mode;
		uint32_t flag;
		std::string backupPath;
		CheckpointService::SyncSequentialNumber ssn;

		in >> mode;
		in >> flag;
		in >> backupPath;
		in >> ssn; 

		if (mode == CP_AFTER_RECOVERY || mode == CP_SHUTDOWN) {
			critical = true;
		}

		checkpointService_->runGroupCheckpoint(
				ec.getWorkerId(), ec, mode, flag, backupPath, ssn);
	}
	catch (UserException &e) {
		checkpointService_->errorOccured_ = true;
		if (critical) {
			clusterService_->setError(ec, &e);
			GS_RETHROW_SYSTEM_ERROR(e, "");
		}
		else {
			UTIL_TRACE_EXCEPTION_WARNING(
					CHECKPOINT_SERVICE, e, "workerId=" << ec.getWorkerId());
		}
	}
	catch (SystemException &) {
		checkpointService_->errorOccured_ = true;
		throw;
	}
	catch (std::exception &e) {
		checkpointService_->errorOccured_ = true;
		clusterService_->setError(ec, &e);
		GS_RETHROW_SYSTEM_ERROR(
				e, "(workerId=" << ec.getWorkerId() <<
				", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

const char8_t *CheckpointService::checkpointModeToString(int32_t mode) {
	switch (mode) {
	case CP_NORMAL:
		return "NORMAL_CHECKPOINT";
	case CP_REQUESTED:
		return "REQUESTED_CHECKPOINT";
	case CP_AFTER_RECOVERY:
		return "RECOVERY_CHECKPOINT";
	case CP_SHUTDOWN:
		return "SHUTDOWN_CHECKPOINT";
	case CP_PREPARE_LONGTERM_SYNC:
		return "PREPARE_LONGTERM_SYNC";
	case CP_STOP_LONGTERM_SYNC:
		return "STOP_LONGTERM_SYNC";
	default:
		return "UNKNOWN";
	}
}

void CheckpointService::changeParam(
	std::string &paramName, std::string &value) {
	try {
		int32_t int32Value = atoi(value.c_str());
		if (paramName == "checkpointChunkCopyIntervalMillis") {
			if (int32Value >= 0) {
				chunkCopyIntervalMillis_ = int32Value;
				GS_TRACE_INFO(
						CHECKPOINT_SERVICE, GS_TRACE_CP_PARAMETER_INFO,
						"parameter changed: checkpointChunkCopyIntervalMillis=" <<
						chunkCopyIntervalMillis_ << ", value=" << value);
			}
			else {
				GS_TRACE_INFO(
						CHECKPOINT_SERVICE, GS_TRACE_CP_PARAMETER_INFO,
						"illeagal parameter value: "
						"checkpointChunkCopyIntervalMillis=" <<
						chunkCopyIntervalMillis_ << ", value=" << value);
			}
		}
		else if (paramName == "dumpInternalParam") {
			GS_TRACE_INFO(
					CHECKPOINT_SERVICE, GS_TRACE_CP_PARAMETER_INFO,
					"dump parameters: checkpointInterval=" <<
					cpInterval_ << ", checkpointChunkCopyIntervalMillis=" <<
					chunkCopyIntervalMillis_);
		}
		else {
			GS_TRACE_INFO(
					CHECKPOINT_SERVICE, GS_TRACE_CP_PARAMETER_INFO,
					"[NOTE] Parameter(" << paramName << "," << value <<
					") cannot update");
		}
	}
	catch (UserException &e) {
		UTIL_TRACE_EXCEPTION_WARNING(
				CHECKPOINT_SERVICE, e,
				"User error occured, but continue running: (reason=" <<
				GS_EXCEPTION_MESSAGE(e) << ")");
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION_ERROR(CHECKPOINT_SERVICE, e,
				"Unexpected error occured, but continue running (reason=" <<
				GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Gets transactionEE queue size
*/
int32_t CheckpointService::getTransactionEEQueueSize(PartitionGroupId pgId) {
	EventEngine::Stats stats;
	if (txnEE_->getStats(pgId, stats)) {
		return static_cast<int32_t>(
				stats.get(EventEngine::Stats::EVENT_ACTIVE_QUEUE_SIZE_CURRENT));
	}
	else {
		return 0;
	}
}

CheckpointService::CpLongtermSyncInfo* CheckpointService::getCpLongtermSyncInfo(
		SyncSequentialNumber id) {
	util::LockGuard<util::Mutex> guard(cpLongtermSyncMutex_);
	if (cpLongtermSyncInfoMap_.find(id) != cpLongtermSyncInfoMap_.end()) {
		return &cpLongtermSyncInfoMap_[id];
	}
	else {
		return NULL;
	}
}

bool CheckpointService::setCpLongtermSyncInfo(
		SyncSequentialNumber id, const CpLongtermSyncInfo &cpLongtermSyncInfo) {
	util::LockGuard<util::Mutex> guard(cpLongtermSyncMutex_);
	if (cpLongtermSyncInfoMap_.find(id) == cpLongtermSyncInfoMap_.end()) {
		cpLongtermSyncInfoMap_[id] = cpLongtermSyncInfo;
		return true;
	}
	else {
		return false;
	}
}

bool CheckpointService::updateCpLongtermSyncInfo(
		SyncSequentialNumber id, const CpLongtermSyncInfo &cpLongtermSyncInfo) {
	util::LockGuard<util::Mutex> guard(cpLongtermSyncMutex_);
	if (cpLongtermSyncInfoMap_.find(id) != cpLongtermSyncInfoMap_.end()) {
		cpLongtermSyncInfoMap_[id] = cpLongtermSyncInfo;
		return true;
	}
	else {
		return false;
	}
}

CheckpointService::SyncSequentialNumber CheckpointService::getCurrentSyncSequentialNumber(PartitionId pId) {
	util::LockGuard<util::Mutex> guard(cpLongtermSyncMutex_);
	assert(pId < pgConfig_.getPartitionCount());
	return ssnList_[pId];
}

bool CheckpointService::removeCpLongtermSyncInfo(SyncSequentialNumber id) {
	util::LockGuard<util::Mutex> guard(cpLongtermSyncMutex_);
	if (cpLongtermSyncInfoMap_.find(id) != cpLongtermSyncInfoMap_.end()) {
		cpLongtermSyncInfoMap_.erase(id);
		return true;
	}
	else {
		return false;
	}
}

std::string CheckpointService::getChunkHeaderDumpString(const uint8_t* chunkData) {
	util::NormalOStringStream oss;

	oss << "(PG,P,Cat,Chunk),(" <<
	ChunkManager::ChunkHeader::getPartitionGroupId(chunkData) <<
	"," <<
	ChunkManager::ChunkHeader::getPartitionId(chunkData) <<
	"," <<
	(int32_t)ChunkManager::ChunkHeader::getChunkCategoryId(chunkData) <<
	"," <<
	ChunkManager::ChunkHeader::getChunkId(chunkData) <<
	"),CheckSum,0x" << std::setw(8) << std::setfill('0') << std::hex <<
	ChunkManager::ChunkHeader::getCheckSum(chunkData) << std::dec;
	return oss.str();
}

bool CheckpointService::checkLongtermSyncIsReady(SyncSequentialNumber ssn) {
	if (ssn != UNDEF_SYNC_SEQ_NUMBER) {
		CpLongtermSyncInfo *info = getCpLongtermSyncInfo(ssn);
		if (info && info->logManager_ &&
			!info->logManager_->isLongtermSyncLogAvailable()) {
			return true;
		}
		else {
			return false;
		}
	}
	return false;
}

bool CheckpointService::getLongSyncChunk(
		SyncSequentialNumber ssn, uint32_t size, uint8_t* buffer) {
	if (ssn != UNDEF_SYNC_SEQ_NUMBER) {
		CpLongtermSyncInfo *info = getCpLongtermSyncInfo(ssn);
		if (info && info->newOffsetMap_) {
			if (info->readCount_ == 0) {
				info->readItr_ = info->newOffsetMap_->begin();
			}
			if (info->readItr_ == info->newOffsetMap_->end()) {
				GS_THROW_USER_ERROR(
						GS_ERROR_CP_LONGTERM_SYNC_FAILED,
						"No chunk: ssn=" << ssn << ", pId=" << info->targetPId_);
			}
			if (info && info->logManager_ &&
						!info->logManager_->isLongtermSyncLogAvailable()) {
				info->errorOccured_ = true;
				GS_THROW_USER_ERROR(
						GS_TRACE_CP_LONGTERM_SYNC_LOG_WRITE_FAILED,
						"SyncTemp log write failed (reason=" <<
						info->logManager_->getLongtermSyncLogErrorMessage() << ")");
			}
			const size_t chunkSize =
					1 << chunkManager_->getConfig().getChunkExpSize();
			if (size < chunkSize) {
				GS_THROW_USER_ERROR(
						GS_ERROR_CHM_GET_CHECKPOINT_CHUNK_FAILED,
						"Invalid size (specified size=" << size <<
						", chunkSize=" << chunkSize << ")");
			}

			ChunkManager::FileManager fileManager(chunkManager_->getConfig(), *(info->syncCpFile_));
			uint32_t remain = static_cast<uint32_t>(size / chunkSize);
			uint8_t *addr = buffer;
			while(info->readItr_ != info->newOffsetMap_->end() && remain > 0) {
				info->syncCpFile_->readBlock(addr, 1, info->readItr_->second);
				fileManager.uncompressChunk(addr);
				++info->readItr_;
				++info->readCount_;
				addr += chunkSize;
				--remain;
			}
			return (info->readItr_ == info->newOffsetMap_->end());
		}
		else {
			GS_THROW_USER_ERROR(
					GS_ERROR_CP_LONGTERM_SYNC_FAILED,
					"Invalid syncSequentialNumber: ssn=" << ssn);
		}
	}
	else {
		GS_THROW_USER_ERROR(
				GS_ERROR_CP_LONGTERM_SYNC_FAILED,
				"Invalid syncSequentialNumber: ssn=" << ssn);
	}
}

bool CheckpointService::getLongSyncLog(
		SyncSequentialNumber ssn,
		LogSequentialNumber startLsn, LogCursor &cursor) {

	assert(ssn != UNDEF_SYNC_SEQ_NUMBER);
	CpLongtermSyncInfo *info = getCpLongtermSyncInfo(ssn);
	if (info && info->logManager_) {
		if (info->logManager_->isLongtermSyncLogAvailable()) {
			return info->logManager_->findLog(
					cursor, info->targetPId_, startLsn);
		}
		else {
			info->errorOccured_ = true;
			GS_THROW_USER_ERROR(
					GS_ERROR_CP_LOG_FILE_WRITE_FAILED,
					"SyncTemp log write failed (reason=" <<
					info->logManager_->getLongtermSyncLogErrorMessage() << ")");
			return false;
		}
	}
	else {
		GS_THROW_USER_ERROR(
				GS_ERROR_CP_LONGTERM_SYNC_FAILED,
				"Invalid syncSequentialNumber: ssn=" << ssn);
		return false;
	}
}

bool CheckpointService::getLongSyncCheckpointStartLog(
		SyncSequentialNumber ssn, LogCursor &cursor) {
	assert(ssn != UNDEF_SYNC_SEQ_NUMBER);
	CpLongtermSyncInfo *info = getCpLongtermSyncInfo(ssn);
	if (info && info->logManager_) {
		if (info->logManager_->isLongtermSyncLogAvailable()) {
			const PartitionGroupId pgId =
					getPGConfig().getPartitionGroupId(info->targetPId_);
			return info->logManager_->findCheckpointStartLog(
					cursor, pgId, info->cpId_);
		}
		else {
			info->errorOccured_ = true;
			GS_THROW_USER_ERROR(
					GS_TRACE_CP_LONGTERM_SYNC_LOG_WRITE_FAILED,
					"SyncTemp log write failed (reason=" <<
					info->logManager_->getLongtermSyncLogErrorMessage() << ")");
			return false;
		}
	}
	else {
		GS_THROW_USER_ERROR(
				GS_ERROR_CP_LONGTERM_SYNC_FAILED,
				"Invalid syncSequentialNumber: ssn=" << ssn);
		return false;
	}
}

bool CheckpointService::isEntry(SyncSequentialNumber ssn) {
	assert(ssn != UNDEF_SYNC_SEQ_NUMBER);
	CpLongtermSyncInfo *info = getCpLongtermSyncInfo(ssn);
	if (info) {
		if (info->syncCpFile_ && info->logManager_) {
			return true;
		}
		else {
			return false;
		}
	}
	else {
		GS_THROW_USER_ERROR(
				GS_ERROR_CP_LONGTERM_SYNC_FAILED,
				"Invalid syncSequentialNumber: ssn=" << ssn);
		return false;
	}
}


void CheckpointService::runCheckpoint(
		EventContext &ec, int32_t mode, uint32_t flag,
		const std::string &backupPath, SyncSequentialNumber ssn) {
	if ((lastMode_ == CP_UNDEF && mode != CP_AFTER_RECOVERY) ||
			lastMode_ == CP_SHUTDOWN ||
			(mode != CP_SHUTDOWN && requestedShutdownCheckpoint_)  
		) {
		GS_TRACE_WARNING(
				CHECKPOINT_SERVICE, GS_TRACE_CP_CHECKPOINT_CANCELLED,
				"Checkpoint cancelled by status (mode=" <<
				checkpointModeToString(mode) <<
				", lastMode=" << checkpointModeToString(lastMode_) <<
				", shutdownRequested=" << requestedShutdownCheckpoint_ <<
				")");
		return;
	}

	currentCpGrpId_ = 0;
	errorOccured_ = false;

	struct CheckpointDataCleaner {
		CheckpointDataCleaner(
				CheckpointService &service,
				ChunkManager &chunkManager, EventContext &ec)
			: service_(service),
			  chunkManager_(chunkManager),
			  ec_(ec),
			  workerId_(ec.getWorkerId()) {}
		~CheckpointDataCleaner() {
			if (workerId_ == 0) {
				for (PartitionGroupId pgId = 0;
					 pgId < service_.pgConfig_.getPartitionGroupCount();
					 ++pgId) {
					const PartitionId startPId =
							service_.pgConfig_.getGroupBeginPartitionId(pgId);
					const CheckpointId cpId = 0;  
					try {
						std::string dummy;
						service_.executeOnTransactionService(ec_,
								CLEANUP_CP_DATA, CP_UNDEF, startPId, cpId,
								dummy, false, UNDEF_SYNC_SEQ_NUMBER);  
					}
					catch (...) {
					}
				}
				try {
					service_.endTime_ =
							util::DateTime::now(false).getUnixTime();
					service_.pendingPartitionCount_ = 0;
				}
				catch (...) {
				}
			}
		}

		CheckpointService &service_;
		ChunkManager &chunkManager_;
		EventContext &ec_;
		uint32_t workerId_;
	} cpDataCleaner(
			*this, *chunkManager_, ec);

	startTime_ = 0;
	endTime_ = 0;

	lastMode_ = mode;
	startTime_ = util::DateTime::now(false).getUnixTime();
	pendingPartitionCount_ = pgConfig_.getPartitionCount();

	if (mode != CP_PREPARE_LONGTERM_SYNC && mode != CP_STOP_LONGTERM_SYNC) { 
		GS_TRACE_INFO(
				CHECKPOINT_SERVICE, GS_TRACE_CP_STATUS,
				"[CP_START] mode=" << checkpointModeToString(mode));
	}
	else {
		GS_TRACE_INFO(
				CHECKPOINT_SERVICE, GS_TRACE_CP_STATUS,
				"[CP_LONGTERM_SYNC_START] mode=" << checkpointModeToString(mode) <<
				", SSN=" << ssn);
	}
	if (mode != CP_PREPARE_LONGTERM_SYNC && mode != CP_STOP_LONGTERM_SYNC) { 
		if (parallelCheckpoint_) {
			groupCheckpointStatus_.assign(
					pgConfig_.getPartitionGroupCount(), GROUP_CP_COMPLETED);
			PartitionGroupId pgId = 1;
			try {
				for (; pgId < pgConfig_.getPartitionGroupCount(); ++pgId) {
					PartitionId topPId = pgConfig_.getGroupBeginPartitionId(pgId);
					Event requestEvent(ec, CP_REQUEST_GROUP_CHECKPOINT, topPId);
					EventByteOutStream out = requestEvent.getOutStream();
					out << mode;
					out << flag;
					out << backupPath;
					out << UNDEF_SYNC_SEQ_NUMBER; 

					groupCheckpointStatus_[pgId] = GROUP_CP_RUNNING;
					ee_.add(requestEvent);
				}
				pgId = 0;
				groupCheckpointStatus_[pgId] = GROUP_CP_RUNNING;
				runGroupCheckpoint(pgId, ec, mode, flag, backupPath, ssn);
			}
			catch (...) {
				groupCheckpointStatus_[pgId] = GROUP_CP_COMPLETED;
				waitAllGroupCheckpointEnd();
				throw;
			}
			waitAllGroupCheckpointEnd();
		}
		else {
			for (PartitionGroupId pgId = 0;
				 pgId < pgConfig_.getPartitionGroupCount(); ++pgId) {
				runGroupCheckpoint(pgId, ec, mode, flag, backupPath, ssn);
			}
		}
	}
	else if (mode == CP_PREPARE_LONGTERM_SYNC) {
		assert(ssn != UNDEF_SYNC_SEQ_NUMBER);
		CpLongtermSyncInfo *info = getCpLongtermSyncInfo(ssn);
		if (info != NULL) {
			info->startTime_ = startTime_;
			info->oldOffsetMap_ = UTIL_NEW CpLongtermSyncOffsetMap;
			assert(info->targetPId_ != UNDEF_PARTITIONID);
			const PartitionGroupId pgId =
					getPGConfig().getPartitionGroupId(info->targetPId_);
			runGroupCheckpoint(pgId, ec, mode, flag, backupPath, ssn);
		}
	}
	else {
		assert(mode == CP_STOP_LONGTERM_SYNC);
		assert(ssn != UNDEF_SYNC_SEQ_NUMBER);
		CpLongtermSyncInfo *info = getCpLongtermSyncInfo(ssn);
		if (info != NULL) {
			assert(info->targetPId_ != UNDEF_PARTITIONID);
			const PartitionGroupId pgId =
					getPGConfig().getPartitionGroupId(info->targetPId_);
			runGroupCheckpoint(pgId, ec, mode, flag, backupPath, ssn);
		}
	}

	if (mode == CP_AFTER_RECOVERY) {
		clusterService_->requestCompleteCheckpoint(ec, ec.getAllocator(), true);
		RecoveryManager::setProgressPercent(100);
	}
	if (mode == CP_SHUTDOWN) {
		clusterService_->requestCompleteCheckpoint(
				ec, ec.getAllocator(), false);
	}

	if (mode != CP_PREPARE_LONGTERM_SYNC && mode != CP_STOP_LONGTERM_SYNC) { 
		GS_TRACE_INFO(
				CHECKPOINT_SERVICE, GS_TRACE_CP_STATUS,
				"[CP_END] mode=" << checkpointModeToString(mode) <<
				", commandElapsedMillis=" <<
				getLastDuration(util::DateTime::now(false)));
	}
	else {
		GS_TRACE_INFO(
				CHECKPOINT_SERVICE, GS_TRACE_CP_STATUS,
				"[CP_LONGTERM_SYNC_END] mode=" << checkpointModeToString(mode) <<
				", SSN=" << ssn << ", commandElapsedMillis=" <<
				getLastDuration(util::DateTime::now(false)));
	}

	pendingPartitionCount_ = 0;

	if (errorOccured_) {
		if (mode == CP_PREPARE_LONGTERM_SYNC) { 
			assert(ssn != UNDEF_SYNC_SEQ_NUMBER);
			CpLongtermSyncInfo *info = getCpLongtermSyncInfo(ssn);
			if (info != NULL) { 
				info->errorOccured_ = true;
				syncService_->notifyCheckpointLongSyncReady(
						ec, info->targetPId_, &info->longtermSyncInfo_, true);
			}
		}
		return;
	}
	if (mode == CP_NORMAL) {
		totalNormalCpOperation_++;
	}
	else if (mode == CP_REQUESTED) {
		totalRequestedCpOperation_++;
	}
	else if (mode == CP_PREPARE_LONGTERM_SYNC) { 
		assert(ssn != UNDEF_SYNC_SEQ_NUMBER);
		CpLongtermSyncInfo *info = getCpLongtermSyncInfo(ssn);
		if (info != NULL) { 
			syncService_->notifyCheckpointLongSyncReady(
					ec, info->targetPId_, &info->longtermSyncInfo_, false);
		}
	}
}

void CheckpointService::waitAllGroupCheckpointEnd() {
	for (;;) {
		bool completed = true;
		for (PartitionGroupId pgId = 0;
				pgId < pgConfig_.getPartitionGroupCount(); ++pgId) {
			if (groupCheckpointStatus_[pgId] == GROUP_CP_RUNNING) {
				completed = false;
				break;
			}
		}
		if (completed) {
			break;
		}
		util::Thread::sleep(ALL_GROUP_CHECKPOINT_END_CHECK_INTERVAL);
	}
}

void CheckpointService::runGroupCheckpoint(
		PartitionGroupId pgId, EventContext &ec, int32_t mode, uint32_t flag,
		const std::string &backupPath, SyncSequentialNumber ssn) {
	struct GroupCheckpointDataCleaner {
		explicit GroupCheckpointDataCleaner(
				CheckpointService &service, PartitionGroupId pgId)
				: service_(service), pgId_(pgId) {}

		~GroupCheckpointDataCleaner() {
			service_.groupCheckpointStatus_[pgId_] = GROUP_CP_COMPLETED;
		}
		CheckpointService &service_;
		const PartitionGroupId pgId_;
	} groupCpDataCleaner(*this, pgId);

	util::StackAllocator &alloc = ec.getAllocator();

	GS_TRACE_INFO(
			CHECKPOINT_SERVICE_STATUS_DETAIL, GS_TRACE_CP_STATUS,
			"[CP_GROUP_START_REQUEST] mode=" << checkpointModeToString(mode) <<
			", pgId=" << pgId);


	PartitionGroupLock pgLock(*transactionManager_, pgId);

	util::StackAllocator::Scope scope(alloc);  

	if (mode == CP_STOP_LONGTERM_SYNC) {
		assert(ssn != UNDEF_SYNC_SEQ_NUMBER);
		CpLongtermSyncInfo *info = getCpLongtermSyncInfo(ssn);
		if (info != NULL) {
			CpLongtermSyncInfo tmpInfo = *info;

			const CheckpointId cpId = logManager_->getLastCheckpointId(pgId);
			if (tmpInfo.logManager_ != NULL) {
				tmpInfo.logManager_->flushFile(pgId);
			}
			assert(info->targetPId_ != UNDEF_PARTITIONID);
			executeOnTransactionService(
					ec, CP_TXN_STOP_LONGTERM_SYNC, mode, tmpInfo.targetPId_,
					cpId, tmpInfo.dir_, true, ssn);

			removeCpLongtermSyncInfo(ssn);

			delete tmpInfo.newOffsetMap_;
			delete tmpInfo.oldOffsetMap_;
			delete tmpInfo.syncCpFile_;

			if (!tmpInfo.dir_.empty()) {
				try {
					util::FileSystem::remove(tmpInfo.dir_.c_str(), true);
				} catch(std::exception &e) {
					GS_TRACE_WARNING(CHECKPOINT_SERVICE,
							GS_TRACE_CP_LONGTERM_SYNC_INFO,
							"Remove long sync temporary files failed.  (reason=" <<
							 GS_EXCEPTION_MESSAGE(e) << ")");
				}
			}
		}
		return;
	}

	const PartitionId startPId = pgConfig_.getGroupBeginPartitionId(pgId);
	const PartitionId endPId = pgConfig_.getGroupEndPartitionId(pgId);
	const CheckpointId cpId = logManager_->getLastCheckpointId(pgId) + 1;

	GS_TRACE_INFO(
			CHECKPOINT_SERVICE_STATUS_DETAIL, GS_TRACE_CP_STATUS,
			"[CP_GROUP_START] mode=" << checkpointModeToString(mode) <<
			", pgId=" << pgId << ", cpId=" << cpId);

	uint64_t beforeAllocatedCheckpointBufferCount =
			chunkManager_->getChunkManagerStats()
				.getAllocatedCheckpointBufferCount();

	logManager_->flushFile(pgId);
	executeOnTransactionService(
			ec, PARTITION_GROUP_START, mode, startPId, cpId, backupPath, true,
			ssn);


	if (clusterService_->isError()) {
		GS_THROW_USER_ERROR(
				GS_ERROR_CP_CONTROLLER_ILLEAGAL_STATE,
				"Checkpoint cancelled ("
				"systemErrorOccurred=" <<
				clusterService_->isError() << ")");
	}


	int64_t totalWriteCount = 0;

	for (PartitionId pId = startPId; pId < endPId; pId++) {
		GS_TRACE_DEBUG(
				CHECKPOINT_SERVICE, GS_TRACE_CP_STATUS,
				"[CP_PARTITION_START] mode=" << checkpointModeToString(mode) <<
				", pgId=" << pgId << ", pId=" << pId <<
				", cpId=" << cpId);
		PartitionLock pLock(*transactionManager_, pId);


		struct ChunkStatusCleaner {
			explicit ChunkStatusCleaner(
					ChunkManager &chunkManager, PartitionId targetPId)
					: chunkManager_(chunkManager), targetPId_(targetPId) {}
			~ChunkStatusCleaner() {
				try {
				}
				catch (...) {
				}
			}
			ChunkManager &chunkManager_;
			PartitionId targetPId_;
		} chunkStatusCleaner(*chunkManager_, pId);

		executeOnTransactionService(
				ec, PARTITION_START, mode, pId, cpId, backupPath, true,
				ssn);

		const bool checkpointReady = isCheckpointReady(pId);

		if (clusterService_->isError()) {
			GS_THROW_USER_ERROR(
					GS_ERROR_CP_CONTROLLER_ILLEAGAL_STATE,
					"Checkpoint cancelled ("
					"systemErrorOccurred=" <<
					clusterService_->isError() << ")");
		}

		if (checkpointReady) {
			while (chunkManager_->isCopyLeft(pId)) {
				executeOnTransactionService(
						ec, COPY_CHUNK, mode, pId, cpId, backupPath, true,
						ssn);
				if (clusterService_->isError()) {
					GS_THROW_USER_ERROR(
							GS_ERROR_CP_CONTROLLER_ILLEAGAL_STATE,
							"Checkpoint cancelled ("
							"systemErrorOccurred=" <<
							clusterService_->isError() << ")");
				}

				totalWriteCount += chunkManager_->writeChunk(pId);

				int32_t queueSize = getTransactionEEQueueSize(pgId);
				if (mode != CP_AFTER_RECOVERY && mode != CP_SHUTDOWN &&
						(queueSize > CP_CHUNK_COPY_WITH_SLEEP_LIMIT_QUEUE_SIZE)) {
					util::Thread::sleep(chunkCopyIntervalMillis_);
				}
			}

			executeOnTransactionService(
					ec, PARTITION_END, mode, pId, cpId, backupPath, true,
					ssn);

			if (clusterService_->isError()) {
				GS_THROW_USER_ERROR(
						GS_ERROR_CP_CONTROLLER_ILLEAGAL_STATE,
						"Checkpoint cancelled ("
						"systemErrorOccurred=" <<
						clusterService_->isError() << ")");
			}
		}
		GS_TRACE_DEBUG(
				CHECKPOINT_SERVICE, GS_TRACE_CP_STATUS,
				"[CP_PARTITION_END] mode=" <<
				checkpointModeToString(mode) << ", pgId=" << pgId << ", pId=" <<
				pId << ", cpId=" << cpId <<
				", writeCount=" << totalWriteCount <<
				", isReady=" << (checkpointReady ? "true" : "false"));

		if (pId + 1 < endPId) {
			pendingPartitionCount_--;

		}
		chunkManager_->flush(pgId);
		if (!parallelCheckpoint_) {
			for (uint32_t flushPgId = 0;
					flushPgId < pgConfig_.getPartitionGroupCount(); ++flushPgId) {
				GS_TRACE_INFO(
						CHECKPOINT_SERVICE_DETAIL, GS_TRACE_CP_STATUS,
						"Flush Log while CP: flushPgId = " << flushPgId);
				logManager_->flushFile(
						flushPgId, pgId == flushPgId);  
			}
		}
	}

	chunkManager_->flush(pgId);
	logManager_->flushFile(pgId);
	executeOnTransactionService(
			ec, PARTITION_GROUP_END, mode, startPId, cpId, backupPath, true,
			ssn);

	uint64_t cpMallocCount = chunkManager_->getChunkManagerStats()
			.getAllocatedCheckpointBufferCount() -
				beforeAllocatedCheckpointBufferCount;

	GS_TRACE_INFO(
			CHECKPOINT_SERVICE_STATUS_DETAIL, GS_TRACE_CP_STATUS,
			"[CP_GROUP_END] mode=" << checkpointModeToString(mode) <<
			", pgId=" << pgId << ", cpId=" << cpId <<
			", bufferAllocateCount=" << cpMallocCount <<
			", writeCount=" << totalWriteCount);

	if (clusterService_->isError()) {
		GS_THROW_USER_ERROR(
				GS_ERROR_CP_CONTROLLER_ILLEAGAL_STATE,
				"Checkpoint cancelled ("
				"systemErrorOccurred=" <<
				clusterService_->isError() << ")");
	}

	if (CP_PREPARE_LONGTERM_SYNC == mode) {

		assert(ssn != UNDEF_SYNC_SEQ_NUMBER);
		CpLongtermSyncInfo *info = getCpLongtermSyncInfo(ssn);
		if (info != NULL) {
			info->startTime_ = startTime_;
			assert(info->targetPId_ != UNDEF_PARTITIONID);
			assert(pgId == getPGConfig().getPartitionGroupId(info->targetPId_));
		}
		GS_TRACE_INFO(
				CHECKPOINT_SERVICE, GS_TRACE_CP_STATUS,
				"[CP_PREPARE_LONGTERM_SYNC_CHUNK_COPY_START] mode=" <<
				checkpointModeToString(mode) << ", pgId=" << pgId <<
				", cpId=" << cpId << ", synctempPath=" << info->dir_.c_str());

		assert(info->newOffsetMap_ == NULL);
		uint64_t fileSize = 0;
		try {
			info->newOffsetMap_ = UTIL_NEW CpLongtermSyncOffsetMap;
			info->readItr_ = info->newOffsetMap_->begin();
			info->syncCpFile_ = UTIL_NEW CheckpointFile(
					chunkManager_->getConfig().getChunkExpSize(),
					info->dir_, pgId);
			info->syncCpFile_->open();

			fileSize = chunkManager_->makeSyncTempCpFile(
					pgId, info->targetPId_, cpId,
					info->dir_.c_str(), *info->oldOffsetMap_, *info->newOffsetMap_, *info->syncCpFile_);
		}
		catch (std::exception &e) {
			GS_RETHROW_USER_ERROR(
					e, "Create temporary file for sync failed. (reason=" <<
					GS_EXCEPTION_MESSAGE(e) << ")");
		}
		assert(info->syncCpFile_);

		GS_TRACE_INFO(
				CHECKPOINT_SERVICE, GS_TRACE_CP_STATUS,
				"[CP_PREPARE_LONGTERM_SYNC_CHUNK_COPY_END] mode=" <<
				checkpointModeToString(mode) << ", pgId=" << pgId <<
				", cpId=" << cpId << ", fileSize=" << fileSize);
	}

	pendingPartitionCount_--;

	lsnInfo_.endCheckpoint();
}



/*!
	@brief Starts a Checkpoint on a transaction thread.
*/

void CheckpointService::executeOnTransactionService(
		EventContext &ec,
		GSEventType eventType, int32_t mode, PartitionId pId, CheckpointId cpId,
		const std::string &backupPath, bool executeAndWait,
		SyncSequentialNumber ssn)
{
	try {
		Event requestEvent(ec, eventType, pId);

		EventByteOutStream out = requestEvent.getOutStream();
		out << mode;
		out << cpId;
		out << backupPath;
		out << ssn; 

		if (executeAndWait) {
			txnEE_->executeAndWait(ec, requestEvent);
		}
		else {
			txnEE_->add(requestEvent);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(
				e, "Execute handler on TransactionService failed. (reason=" <<
				GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Executes operations of a Checkpoint for transaction thread.
*/
void CheckpointOperationHandler::operator()(EventContext &ec, Event &ev) {
	try {
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);

		EventByteInStream in = ev.getInStream();

		const PartitionId pId = ev.getPartitionId();
		const PartitionGroupId pgId =
				checkpointService_->getPGConfig().getPartitionGroupId(pId);

		int32_t mode;
		CheckpointId cpId;
		std::string backupPath;
		CheckpointService::SyncSequentialNumber ssn;

		in >> mode;
		in >> cpId;
		in >> backupPath;
		in >> ssn; 

		WATCHER_START;
		switch (ev.getType()) {
		case CLEANUP_CP_DATA:
			chunkManager_->cleanCheckpointData(pgId);
			break;

		case CP_TXN_STOP_LONGTERM_SYNC:
			{
				assert(ssn != CheckpointService::UNDEF_SYNC_SEQ_NUMBER);
				CheckpointService::CpLongtermSyncInfo *info =
						checkpointService_->getCpLongtermSyncInfo(ssn);
				if (info && info->logManager_) {
					logManager_->removeSyncLogManager(info->logManager_, info->targetPId_);
					delete info->logManager_;
					info->logManager_ = NULL;
				}
			}
			break;
		case PARTITION_GROUP_START:

			checkpointService_->setCurrentCpGrpId(pgId);

			chunkManager_->startCheckpoint(pgId, cpId);
			logManager_->prepareCheckpoint(pgId, cpId);

			if (ssn != CheckpointService::UNDEF_SYNC_SEQ_NUMBER) {
				CheckpointService::CpLongtermSyncInfo *info =
						checkpointService_->getCpLongtermSyncInfo(ssn);
				if (info != NULL) {
					LogManager::Config syncLogManagerConfig =
							logManager_->getConfig();
					syncLogManagerConfig.persistencyMode_ =
							LogManager::PERSISTENCY_KEEP_ALL_LOG;
					syncLogManagerConfig.alwaysFlushOnTxnEnd_ = false;
					syncLogManagerConfig.logDirectory_ = info->dir_;
					syncLogManagerConfig.emptyFileAppendable_ = true;
					LogManager *syncLogManager =
							UTIL_NEW LogManager(syncLogManagerConfig);
					info->logManager_ = syncLogManager;
				}
			}
			break;

		case PARTITION_START:
			{
				checkpointService_->setCurrentCpPId(pId);
				bool checkpointReady =
						dataStore_->isRestored(pId) && chunkManager_->existPartition(pId);
				checkpointService_->setCheckpointReady(pId, checkpointReady);
				if (checkpointReady) {
					chunkManager_->startCheckpoint(pId);
				}
//				LogSequentialNumber lsn = 
				writeCheckpointStartLog(
						alloc, mode, pgId, pId, cpId);
				writeChunkMetaDataLog(
						alloc, mode, pgId, pId, cpId, checkpointReady);
			}
			break;

		case COPY_CHUNK:
			if (checkpointService_->isCheckpointReady(pId)) {
				chunkManager_->copyChunk(pId);
			}
			break;

		case PARTITION_END:
			if (checkpointService_->isCheckpointReady(pId)) {
				chunkManager_->endCheckpoint(pId);
				checkpointService_->setCurrentCheckpointId(
						pId, logManager_->getLastCheckpointId(pgId));
			}
			checkpointService_->setCurrentCpPId(UNDEF_PARTITIONID);

			if (cpId == 0) {
				logManager_->setAvailableStartLSN(pId, cpId);
			}

			break;

		case PARTITION_GROUP_END: {
			util::XArray<uint8_t> bitList(alloc);
			util::XArray<uint8_t> binaryLogBuf(alloc);

			chunkManager_->endCheckpoint(pgId, cpId);
			chunkManager_->flush(pgId);

			checkpointService_->setCurrentCpGrpId(UINT32_MAX);


			BitArray &validBitArray = chunkManager_->getCheckpointBit(pgId);

			const PartitionGroupConfig &pgConfig =
					checkpointService_->getPGConfig();
			{
				const PartitionId startPId =
						pgConfig.getGroupBeginPartitionId(pgId);
				const PartitionId endPId =
						pgConfig.getGroupEndPartitionId(pgId);
				for (PartitionId pId = startPId; pId < endPId; pId++) {
					checkpointService_->lsnInfo_.setLsn(
							pId, logManager_->getLSN(pId));  
				}
			}
			logManager_->putCheckpointEndLog(
					binaryLogBuf,
					pgConfig.getGroupBeginPartitionId(pgId),
					validBitArray);

			logManager_->writeBuffer(pgId);
			logManager_->flushFile(pgId);
			logManager_->postCheckpoint(
					pgId);  

			{
				PartitionTable *pt = checkpointService_->getPartitionTable();
				util::XArray<uint8_t> binaryLogRecords(alloc);
				CheckpointId cpId = logManager_->getFirstCheckpointId(pgId);
				logManager_->updateAvailableStartLsn(
						pt, pgId, binaryLogRecords, cpId);

				const PartitionId startPId =
						pgConfig.getGroupBeginPartitionId(pgId);
				const PartitionId endPId =
						pgConfig.getGroupEndPartitionId(pgId);
				clusterService_->requestUpdateStartLsn(
						ec, ec.getAllocator(), startPId, endPId);
			}
			if (ssn != CheckpointService::UNDEF_SYNC_SEQ_NUMBER) {
				CheckpointService::CpLongtermSyncInfo *info =
						checkpointService_->getCpLongtermSyncInfo(ssn);
				if (info != NULL) {
					assert(info->oldOffsetMap_);
					chunkManager_->getCheckpointChunkPos(
							info->targetPId_, *info->oldOffsetMap_);

					info->cpId_ = logManager_->getLastCheckpointId(pgId);

					logManager_->copyLogFile(pgId, info->dir_.c_str());

					info->logManager_->open(false, true, false, pgId);
					logManager_->addSyncLogManager(info->logManager_, info->targetPId_);
				}
			}

			if (mode == CP_SHUTDOWN) {
				logManager_->writeBuffer(
						pgId);  
				logManager_->flushFile(
						pgId);  
			}
		} break;
		}
		WATCHER_END_3(getEventTypeName(ev.getType()), pId, pgId);
	}
	catch (std::exception &e) {
		clusterService_->setError(ec, &e);
		GS_RETHROW_SYSTEM_ERROR(
				e, "Group checkpoint failed. (reason=" <<
				GS_EXCEPTION_MESSAGE(e) << ")");
	}
}


/*!
	@brief Outputs a log of starting a Checkpoint.
*/
LogSequentialNumber CheckpointOperationHandler::writeCheckpointStartLog(
		util::StackAllocator &alloc, int32_t mode, PartitionGroupId pgId,
		PartitionId pId, CheckpointId cpId) {
	try {
		util::XArray<uint64_t> dirtyChunkList(alloc);

		util::XArray<ClientId> activeClientIds(alloc);
		util::XArray<TransactionId> activeTxnIds(alloc);
		util::XArray<ContainerId> activeRefContainerIds(alloc);
		util::XArray<StatementId> activeLastExecStmtIds(alloc);
		util::XArray<int32_t> activeTimeoutIntervalSec(alloc);

		TransactionId maxAssignedTxnId = 0;


		transactionManager_->backupTransactionActiveContext(pId,
				maxAssignedTxnId, activeClientIds, activeTxnIds,
				activeRefContainerIds, activeLastExecStmtIds,
				activeTimeoutIntervalSec);

		util::XArray<uint8_t> logBuffer(alloc);
		return logManager_->putCheckpointStartLog(
				logBuffer, pId, maxAssignedTxnId,
				logManager_->getLSN(pId), activeClientIds, activeTxnIds,
				activeRefContainerIds, activeLastExecStmtIds,
				activeTimeoutIntervalSec);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(
				e, "Write checkpoint start log failed. (pgId=" <<
				pgId << ", pId=" << pId << ", mode=" << mode << ", cpId=" <<
				cpId << ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}
void CheckpointOperationHandler::compressChunkMetaDataLog(
		util::StackAllocator &alloc,
		PartitionGroupId pgId, PartitionId pId,
		ChunkCategoryId categoryId,
		ChunkId startChunkId, int32_t count,
		util::XArray<uint8_t> &logBuffer,
		util::XArray<uint8_t> &metaDataEmptySize,
		util::XArray<uint8_t> &metaDataFileOffset,
		util::XArray<uint8_t> &metaDataChunkKey
) {
	util::XArray<uint8_t> compressBuffer(alloc);
	util::XArray<uint8_t> compressSource(alloc);
	compressSource.reserve(metaDataEmptySize.size()
			+ metaDataFileOffset.size()
			+ metaDataChunkKey.size()
			+ sizeof(uint32_t) * 2);
	compressSource.push_back(
			metaDataEmptySize.data(), metaDataEmptySize.size());
	compressSource.push_back(metaDataFileOffset.data(),
			metaDataFileOffset.size());
	const bool isBatchFreeMode = chunkManager_->isBatchFreeMode(categoryId);
	if (isBatchFreeMode) {
		compressSource.push_back(
				metaDataChunkKey.data(), metaDataChunkKey.size());
	}
	uint32_t origSize = static_cast<uint32_t>(compressSource.size());
	uint32_t compSize = static_cast<uint32_t>(origSize * 1.1 + 14);
	uint8_t temp = 0;
	compressBuffer.assign(compSize, temp);

	try {
		ZlibUtils zlib;
		zlib.compressData(
			compressSource.data(), origSize,
			compressBuffer.data(), compSize);

		if (compSize < origSize) {
			compressBuffer.resize(compSize);
			logManager_->putChunkMetaDataLog(
					logBuffer, pId, categoryId, startChunkId, count,
					origSize, &compressBuffer, false);
		}
		else {
			logManager_->putChunkMetaDataLog(
					logBuffer, pId, categoryId, startChunkId, count,
					0, &compressSource, false);
		}
	} catch(std::exception &e) {
		logManager_->putChunkMetaDataLog(
				logBuffer, pId, categoryId, startChunkId, count,
				0, &compressSource, false);
		GS_TRACE_WARNING(CHECKPOINT_SERVICE,
				GS_TRACE_CM_COMPRESSION_FAILED,
				"Compress chunk meta data log failed. (reason="
				<< GS_EXCEPTION_MESSAGE(e) << ")");
	}
	GS_TRACE_INFO(
			CHECKPOINT_SERVICE_DETAIL, GS_TRACE_CP_STATUS,
					"writeChunkMetaDaLog,pgId," <<
					pgId << ",pId," << pId << ",chunkCategoryId," <<
					(int32_t)categoryId << ",startChunkId," <<
					startChunkId << ",chunkNum," << count);
}

/*!
	@brief Outputs a log of metadata of Chunk.
*/
void CheckpointOperationHandler::writeChunkMetaDataLog(
		util::StackAllocator &alloc, int32_t mode, PartitionGroupId pgId,
		PartitionId pId, CheckpointId cpId, bool isRestored) {
	try {

		if (isRestored) {
			int32_t count = 0;
			util::XArray<uint8_t> logBuffer(alloc);
			util::XArray<uint8_t> metaDataEmptySize(alloc);
			util::XArray<uint8_t> metaDataFileOffset(alloc);
			util::XArray<uint8_t> metaDataChunkKey(alloc);
			ChunkCategoryId categoryId;
			ChunkId chunkId;
			ChunkId startChunkId = 0;
			int64_t scanSize = chunkManager_->getScanSize(pId);
			ChunkKey *metaChunkKey;
			ChunkManager::MetaChunk *metaChunk =
					chunkManager_->begin(pId, categoryId, chunkId, metaChunkKey);
			GS_TRACE_INFO(
					CHECKPOINT_SERVICE_DETAIL, GS_TRACE_CP_STATUS,
					"writeChunkMetaDataLog: pId=" << pId <<
					",chunkId" << chunkId);

			uint64_t prevPos = 0;
			ChunkKey prevChunkKey = 0;
			uint8_t varIntBuf[LogManager::LOGMGR_VARINT_MAX_LEN];

			for (int64_t index = 0; index < scanSize; index++) {
				if (!metaChunk) {
					metaDataEmptySize.push_back(0xff);
					int64_t filePosDiff = 0;
					uint64_t zigzagDiff = util::zigzagEncode64(filePosDiff);

					int32_t encodedSize = util::varIntEncode64(varIntBuf, zigzagDiff);
					metaDataFileOffset.push_back(varIntBuf, encodedSize);
					if (chunkManager_->isBatchFreeMode(categoryId)) {
						int32_t chunkKeyDiff = 0;
						uint32_t zigzagDiff = util::zigzagEncode32(chunkKeyDiff);
						encodedSize = util::varIntEncode32(varIntBuf, zigzagDiff);
						metaDataChunkKey.push_back(varIntBuf, encodedSize);
					}
				}
				else {
					uint8_t unoccupiedSize = metaChunk->getUnoccupiedSize();
					metaDataEmptySize.push_back(unoccupiedSize);

					int64_t filePos = metaChunk->getCheckpointPos();
					assert(filePos != -1);

					int64_t diff = static_cast<int64_t>(filePos)
							- static_cast<int64_t>(prevPos);
					uint64_t zigzagDiff = util::zigzagEncode64(diff);

					int32_t encodedSize = util::varIntEncode64(varIntBuf, zigzagDiff);
					metaDataFileOffset.push_back(varIntBuf, encodedSize);
					prevPos = filePos;
					if (chunkManager_->isBatchFreeMode(categoryId)) {
						assert(metaChunkKey != NULL);
						assert(*metaChunkKey >= 0);
						int32_t diff = static_cast<int32_t>(*metaChunkKey)
								- static_cast<int32_t>(prevChunkKey);
						uint32_t zigzagDiff = util::zigzagEncode32(diff);
						prevChunkKey = *metaChunkKey;
						encodedSize = util::varIntEncode32(varIntBuf, zigzagDiff);
						metaDataChunkKey.push_back(varIntBuf, encodedSize);
					}
					GS_TRACE_DEBUG(
							CHECKPOINT_SERVICE_DETAIL, GS_TRACE_CP_STATUS,
							"chunkMetaData: (chunkId," <<
							chunkId << ",freeInfo," <<
							(int32_t)metaChunk->getUnoccupiedSize() <<
							",pos," << metaChunk->getCheckpointPos() <<
							",chunkKey," << (metaChunkKey ? *metaChunkKey : -1));
				}
				++count;
				if (count == CHUNK_META_DATA_LOG_MAX_NUM) {
					compressChunkMetaDataLog(
							alloc, pgId, pId, categoryId, startChunkId, count,
							logBuffer, metaDataEmptySize,
							metaDataFileOffset, metaDataChunkKey);

					startChunkId += count;
					count = 0;
					prevPos = 0;
					prevChunkKey = 0;
					metaDataEmptySize.clear();
					metaDataFileOffset.clear();
					metaDataChunkKey.clear();
					logBuffer.clear();
				}

				ChunkCategoryId prevCategoryId = categoryId;
				metaChunk = chunkManager_->next(pId, categoryId, chunkId, metaChunkKey);
				if (categoryId != prevCategoryId) {
					if (count > 0) {

						compressChunkMetaDataLog(
								alloc, pgId, pId, prevCategoryId, startChunkId, count,
								logBuffer, metaDataEmptySize,
								metaDataFileOffset, metaDataChunkKey);

						startChunkId = 0;
						count = 0;
						prevPos = 0;
						prevChunkKey = 0;
						metaDataEmptySize.clear();
						metaDataFileOffset.clear();
						metaDataChunkKey.clear();
						logBuffer.clear();
					}
				}
			}  
			logBuffer.clear();
			logManager_->putChunkMetaDataLog(
					logBuffer, pId, UNDEF_CHUNK_CATEGORY_ID, 0, 1, 0, NULL, true);
		}
		else {
			util::XArray<uint8_t> logBuffer(alloc);
			logManager_->putChunkMetaDataLog(
					logBuffer, pId, UNDEF_CHUNK_CATEGORY_ID, 0, 0, 0, NULL, true);
		}  
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(
				e, "Write chunk meta data log failed. (pgId=" <<
				pgId << ", pId=" << pId << ", mode=" << mode << ", cpId=" <<
				cpId << ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Flushes Log files at fixed intervals.
*/
void FlushLogPeriodicallyHandler::operator()(EventContext &ec) {
	try {
		for (uint32_t pgId = 0;
				 pgId < checkpointService_->getPGConfig().getPartitionGroupCount();
				++pgId) {
			GS_TRACE_INFO(
					CHECKPOINT_SERVICE_DETAIL, GS_TRACE_CP_FLUSH_LOG,
					"Flush Log: pgId = " << pgId);
			logManager_->flushFile(
					pgId, false);   
		}
	}
	catch (std::exception &e) {
		clusterService_->setError(ec, &e);
		GS_RETHROW_SYSTEM_ERROR(
				e, "Flush log file failed. (reason=" <<
				GS_EXCEPTION_MESSAGE(e) << ")");
	}
}


/*!
	@brief Locks a Partition group.
*/
PartitionGroupLock::PartitionGroupLock(
		TransactionManager &txnManager, PartitionGroupId pgId)
		: transactionManager_(txnManager), pgId_(pgId) {
}

/*!
	@brief Unlocks a Partition group.
*/
PartitionGroupLock::~PartitionGroupLock() {
}

/*!
	@brief Locks a Partition.
*/
PartitionLock::PartitionLock(
		TransactionManager &txnManager, PartitionId pId)
		: transactionManager_(txnManager), pId_(pId) {
	while (!transactionManager_.lockPartition(pId_)) {
		util::Thread::sleep(100);
	}
}

/*!
	@brief Unlocks a Partition.
*/
PartitionLock::~PartitionLock() {
	transactionManager_.unlockPartition(pId_);
}


CheckpointService::PIdLsnInfo::PIdLsnInfo()
		: checkpointService_(NULL), partitionNum_(0), partitionGroupNum_(0) {}

CheckpointService::PIdLsnInfo::~PIdLsnInfo() {}

void CheckpointService::PIdLsnInfo::setConfigValue(
		CheckpointService *checkpointService, uint32_t partitionNum,
		uint32_t partitionGroupNum, const std::string &path) {
	checkpointService_ = checkpointService;
	partitionNum_ = partitionNum;
	partitionGroupNum_ = partitionGroupNum;
	path_ = path;

	lsnList_.assign(partitionNum_, 0);
}

void CheckpointService::PIdLsnInfo::startCheckpoint() {}

void CheckpointService::PIdLsnInfo::endCheckpoint() {
	writeFile();
}

void CheckpointService::PIdLsnInfo::setLsn(
		PartitionId pId, LogSequentialNumber lsn) {
	lsnList_[pId] = lsn;
}

void CheckpointService::PIdLsnInfo::writeFile() {
	util::LockGuard<util::Mutex> guard(mutex_);
	std::string lsnInfoFileName;
	util::NamedFile file;
	try {
		picojson::object jsonNodeInfo;
		NodeAddress &address =
				checkpointService_->partitionTable_->
				getNodeInfo(0).getNodeAddress();
		jsonNodeInfo["address"] = picojson::value(address.toString(false));
		jsonNodeInfo["port"] =
				picojson::value(static_cast<double>(address.port_));

		picojson::object jsonLsnInfo;

		picojson::array lsnList;
		for (PartitionId pId = 0; pId < partitionNum_; ++pId) {
			lsnList.push_back(
					picojson::value(static_cast<double>(lsnList_[pId])));
		}

		picojson::object jsonObject;
		jsonObject["nodeInfo"] = picojson::value(jsonNodeInfo);
		jsonObject["partitionNum"] =
				picojson::value(static_cast<double>(partitionNum_));
		jsonObject["groupNum"] =
				picojson::value(static_cast<double>(partitionGroupNum_));
		jsonObject["lsnInfo"] = picojson::value(lsnList);

		std::string jsonString(picojson::value(jsonObject).serialize());

		util::FileSystem::createPath(
				path_.c_str(), PID_LSN_INFO_FILE_NAME.c_str(), lsnInfoFileName);
		file.open(
				lsnInfoFileName.c_str(),
						util::FileFlag::TYPE_READ_WRITE |
						util::FileFlag::TYPE_CREATE |
						util::FileFlag::TYPE_TRUNCATE);
		file.lock();
		file.write(jsonString.c_str(), jsonString.length());
		file.close();
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(
				e, "Write lsn info file failed. (fileName=" <<
				lsnInfoFileName.c_str() << ", reason=" <<
				GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

CheckpointService::ConfigSetUpHandler CheckpointService::configSetUpHandler_;

void CheckpointService::ConfigSetUpHandler::operator()(ConfigTable &config) {
	CONFIG_TABLE_RESOLVE_GROUP(config, CONFIG_TABLE_CP, "checkpoint");

	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_CP_CHECKPOINT_INTERVAL, INT32)
			.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
			.setMin(1)
			.setDefault(60);
	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_CP_CHECKPOINT_MEMORY_LIMIT, INT32)
			.setUnit(ConfigTable::VALUE_UNIT_SIZE_MB)
			.setMin(1)
			.setMax("128TB")
			.setDefault(1024);
	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_CP_USE_PARALLEL_MODE, BOOL)
			.setExtendedType(ConfigTable::EXTENDED_TYPE_LAX_BOOL)
			.setDefault(false);

	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_CP_CHECKPOINT_COPY_INTERVAL_MILLIS, INT32)
			.deprecate();
	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_CP_CHECKPOINT_COPY_INTERVAL, INT32)
			.setUnit(ConfigTable::VALUE_UNIT_DURATION_MS)
			.setMin(0)
			.setDefault(100);
}

CheckpointService::StatSetUpHandler CheckpointService::statSetUpHandler_;

#define STAT_ADD(id) STAT_TABLE_ADD_PARAM(stat, parentId, id)

void CheckpointService::StatSetUpHandler::operator()(StatTable &stat) {
	StatTable::ParamId parentId;

	parentId = STAT_TABLE_ROOT;
	stat.resolveGroup(parentId, STAT_TABLE_CP, "checkpoint");

	parentId = STAT_TABLE_CP;
	STAT_ADD(STAT_TABLE_CP_START_TIME);
	STAT_ADD(STAT_TABLE_CP_END_TIME);
	STAT_ADD(STAT_TABLE_CP_MODE);
	STAT_ADD(STAT_TABLE_CP_PENDING_PARTITION);
	STAT_ADD(STAT_TABLE_CP_NORMAL_CHECKPOINT_OPERATION);
	STAT_ADD(STAT_TABLE_CP_REQUESTED_CHECKPOINT_OPERATION);

	parentId = STAT_TABLE_ROOT;
	stat.resolveGroup(parentId, STAT_TABLE_PERF, "performance");

	parentId = STAT_TABLE_PERF;
	STAT_ADD(STAT_TABLE_PERF_CHECKPOINT_FILE_SIZE);
	STAT_ADD(STAT_TABLE_PERF_CHECKPOINT_FILE_USAGE_RATE);
	STAT_ADD(STAT_TABLE_PERF_STORE_COMPRESSION_MODE);
	STAT_ADD(STAT_TABLE_PERF_CHECKPOINT_FILE_ALLOCATE_SIZE);
	STAT_ADD(STAT_TABLE_PERF_CURRENT_CHECKPOINT_WRITE_BUFFER_SIZE);
	STAT_ADD(STAT_TABLE_PERF_CHECKPOINT_WRITE_SIZE);
	STAT_ADD(STAT_TABLE_PERF_CHECKPOINT_WRITE_TIME);
	STAT_ADD(STAT_TABLE_PERF_CHECKPOINT_MEMORY_LIMIT);
	STAT_ADD(STAT_TABLE_PERF_CHECKPOINT_MEMORY);
}

bool CheckpointService::StatUpdator::operator()(StatTable &stat) {
	if (!stat.getDisplayOption(STAT_TABLE_DISPLAY_SELECT_CP)) {
		return true;
	}

	CheckpointService &svc = *service_;

	ChunkManager::ChunkManagerStats &cmStats =
			svc.chunkManager_->getChunkManagerStats();
	ChunkManager::Config &cmConfig = svc.chunkManager_->getConfig();

	if (stat.getDisplayOption(STAT_TABLE_DISPLAY_WEB_ONLY)) {
		stat.set(STAT_TABLE_CP_START_TIME, svc.getLastStartTime());

		stat.set(STAT_TABLE_CP_END_TIME, svc.getLastEndTime());

		stat.set(STAT_TABLE_CP_MODE, checkpointModeToString(svc.getLastMode()));

		stat.set(
			STAT_TABLE_CP_PENDING_PARTITION, svc.getPendingPartitionCount());

		stat.set(STAT_TABLE_CP_NORMAL_CHECKPOINT_OPERATION,
			svc.getTotalNormalCpOperation());

		stat.set(STAT_TABLE_CP_REQUESTED_CHECKPOINT_OPERATION,
			svc.getTotalRequestedCpOperation());
	}



	stat.set(
			STAT_TABLE_PERF_CHECKPOINT_FILE_SIZE, cmStats.getCheckpointFileSize());

	stat.set(STAT_TABLE_PERF_CHECKPOINT_FILE_USAGE_RATE,
			cmStats.getCheckpointFileUsageRate());

	stat.set(STAT_TABLE_PERF_STORE_COMPRESSION_MODE,
			cmStats.getActualCompressionMode());
	stat.set(STAT_TABLE_PERF_CHECKPOINT_FILE_ALLOCATE_SIZE,
			cmStats.getCheckpointFileAllocateSize());

	stat.set(STAT_TABLE_PERF_CURRENT_CHECKPOINT_WRITE_BUFFER_SIZE,
			cmStats.getCheckpointWriteBufferSize());

	stat.set(STAT_TABLE_PERF_CHECKPOINT_WRITE_SIZE,
			cmStats.getCheckpointWriteSize());

	if (stat.getDisplayOption(STAT_TABLE_DISPLAY_WEB_ONLY)) {
		stat.set(STAT_TABLE_PERF_CHECKPOINT_WRITE_TIME,
				cmStats.getCheckpointWriteTime());

		stat.set(STAT_TABLE_PERF_CHECKPOINT_MEMORY_LIMIT,
				cmConfig.getAtomicCheckpointMemoryLimit());

		stat.set(
				STAT_TABLE_PERF_CHECKPOINT_MEMORY, cmStats.getCheckpointMemory());
	}

	return true;
}

void CheckpointService::requestStartCheckpointForLongtermSync(
		const Event::Source &eventSource,
		PartitionId pId, LongtermSyncInfo *longtermSyncInfo) {
	assert(longtermSyncInfo);
	SyncSequentialNumber ssn = longtermSyncInfo->getSequentialNumber();
	if (lastMode_ == CP_SHUTDOWN) {
		GS_THROW_USER_ERROR(GS_ERROR_CP_CONTROLLER_ILLEAGAL_STATE,
				"RequestStartLongtermSync cancelled by already requested shutdown ("
				"lastMode=" <<
				checkpointModeToString(lastMode_) << ")");
	}

	if (requestedShutdownCheckpoint_) {
		GS_THROW_USER_ERROR(GS_ERROR_CP_CONTROLLER_ILLEAGAL_STATE,
				"RequestStartLongtermSync cancelled by already requested shutdown ("
				"lastMode=" <<
				checkpointModeToString(lastMode_) << ")");
	}

	if (pId >= pgConfig_.getPartitionCount()) {
		GS_THROW_USER_ERROR(GS_ERROR_CP_LONGTERM_SYNC_FAILED,
				"RequestStartLongtermSync: invalid pId (pId=" <<
				pId << ", syncSeqNumber=" << ssnList_.at(pId) << ")");
	}

	std::string syncTempPath;
	util::NormalOStringStream oss;
	oss << ssn;
	util::FileSystem::createPath(
			syncTempTopPath_.c_str(), oss.str().c_str(), syncTempPath);
	std::string origSyncTempPath(syncTempPath);

	{
		CpLongtermSyncInfo info;
		info.ssn_ = ssn;
		info.targetPId_ = pId;
		info.dir_ = syncTempPath;
		info.longtermSyncInfo_.copy(*longtermSyncInfo);
		bool success = setCpLongtermSyncInfo(ssn, info);
		if (!success) {
			GS_THROW_USER_ERROR(GS_ERROR_CP_LONGTERM_SYNC_FAILED,
					"Same syncSequentialNumber already used (pId=" <<
					pId << ", syncSequentialNumber=" << ssn << ")");
		}
	}
	CpLongtermSyncInfo *info = getCpLongtermSyncInfo(ssn);
	assert(info != NULL);

	if (ssnList_.at(pId) != UNDEF_SYNC_SEQ_NUMBER) {
		info->errorOccured_ = true;
		GS_THROW_USER_ERROR(GS_ERROR_CP_CONTROLLER_ILLEAGAL_STATE,
				"RequestStartLongtermSync: another long sync is already running. (pId=" <<
				pId << ", anotherSSN=" << ssnList_.at(pId) <<
				", thisSSN=" << ssn << ")");
	}

	if (util::FileSystem::exists(syncTempPath.c_str())) {
		try {
			util::FileSystem::remove(syncTempPath.c_str(), true);
		}
		catch (std::exception &e) {
			info->errorOccured_ = true;
			GS_RETHROW_USER_ERROR(e,
					"Failed to remove syncTemp top dir (path=" <<
					syncTempPath <<
					", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
		}
	}

	try {
		util::FileSystem::createDirectoryTree(syncTempPath.c_str());
	}
	catch (std::exception &) {
		info->errorOccured_ = true;
		GS_THROW_USER_ERROR(GS_ERROR_CP_LONGTERM_SYNC_FAILED,
				"SyncTemp directory can't create (pId=" <<
				pId << ", path=" << syncTempPath << ")");
	}
	try {
		GS_TRACE_INFO(CHECKPOINT_SERVICE, GS_TRACE_CP_STATUS,
				"[PrepareLongtermSync] requested (pId=" <<
				pId << ", path=" << syncTempPath << ")");

		Event requestEvent(
				eventSource, CP_REQUEST_CHECKPOINT, CP_HANDLER_PARTITION_ID);

		EventByteOutStream out = requestEvent.getOutStream();

		int32_t mode = CP_PREPARE_LONGTERM_SYNC;
		uint32_t flag = 0;
		out << mode;
		out << flag;
		out << syncTempPath;
		out << ssn; 

		ee_.add(requestEvent);
	}
	catch (std::exception &e) {
		info->errorOccured_ = true;
		clusterService_->setError(eventSource, &e);
		GS_RETHROW_SYSTEM_ERROR(
				e, "Request prepare long sync failed.  (reason=" <<
				GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

void CheckpointService::requestStopCheckpointForLongtermSync(
		const Event::Source &eventSource,
		PartitionId pId, int64_t ssn) {
	CpLongtermSyncInfo* longSyncInfo = getCpLongtermSyncInfo(ssn);
	if (longSyncInfo != NULL) {
		try {
			GS_TRACE_INFO(
					CHECKPOINT_SERVICE, GS_TRACE_CP_STATUS,
					"[StopCpLongtermSync] requested (SSN=" << ssn << ")");

			Event requestEvent(
					eventSource, CP_REQUEST_CHECKPOINT, CP_HANDLER_PARTITION_ID);

			EventByteOutStream out = requestEvent.getOutStream();

			std::string dummy;
			int32_t mode = CP_STOP_LONGTERM_SYNC;
			uint32_t flag = 0;
			out << mode;
			out << flag;
			out << dummy;
			out << ssn; 

			ee_.add(requestEvent);
		}
		catch (std::exception &e) {
			clusterService_->setError(eventSource, &e);
			GS_RETHROW_SYSTEM_ERROR(
					e, "Request stop long sync failed.  (reason=" <<
					GS_EXCEPTION_MESSAGE(e) << ")");
		}
	}
	}

