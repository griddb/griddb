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
#include "cluster_event_type.h"  
#include "config_table.h"
#include "util/trace.h"
#include "data_store_v4.h"
#include "log_manager.h"
#include "transaction_service.h"
#include "chunk_manager.h"
#include "partition.h" 

#include "picojson.h"
#include <fstream>
#include "zlib_utils.h"



#ifndef _WIN32
#include <signal.h>  
#endif
#include <iostream>

UTIL_TRACER_DECLARE(CHECKPOINT_SERVICE);
UTIL_TRACER_DECLARE(CHECKPOINT_SERVICE_DETAIL);
UTIL_TRACER_DECLARE(CHECKPOINT_SERVICE_STATUS_DETAIL);
UTIL_TRACER_DECLARE(IO_MONITOR);

const std::string CheckpointService::PID_LSN_INFO_FILE_NAME("gs_lsn_info.json");

const char *const CheckpointService::INCREMENTAL_BACKUP_FILE_SUFFIX =
		"_incremental";

const uint32_t CheckpointService::MAX_BACKUP_NAME_LEN = 12;

const char *const CheckpointService::SYNC_TEMP_FILE_SUFFIX =
		"_sync_temp";

const CheckpointService::SyncSequentialNumber
	CheckpointService::UNDEF_SYNC_SEQ_NUMBER = INT64_MAX;

const CheckpointService::SyncSequentialNumber
	CheckpointService::MAX_SYNC_SEQ_NUMBER = UNDEF_SYNC_SEQ_NUMBER - 1;



const util::NameCoderEntry<CheckpointTypes::PeriodicFlag>
		CheckpointTypes::PERIODIC_FLAG_LIST[] = {
	UTIL_NAME_CODER_ENTRY(PERIODIC_INACTIVE),
	UTIL_NAME_CODER_ENTRY(PERIODIC_ACTIVE)
};

const util::NameCoder<
		CheckpointTypes::PeriodicFlag, CheckpointTypes::PERIODIC_FLAG_END>
		CheckpointTypes::PERIODIC_FLAG_CODER(PERIODIC_FLAG_LIST, 1);

const util::NameCoderEntry<CheckpointMode> CheckpointTypes::CP_MODE_LIST[] = {
	UTIL_NAME_CODER_ENTRY(CP_UNKNOWN),
	UTIL_NAME_CODER_ENTRY_CUSTOM("_NORMAL_CHECKPOINT", CP_NORMAL),
	UTIL_NAME_CODER_ENTRY_CUSTOM("_REQUESTED_CHECKPOINT", CP_REQUESTED),
	UTIL_NAME_CODER_ENTRY(CP_BACKUP),
	UTIL_NAME_CODER_ENTRY_CUSTOM("_BACKUP_PHASE1", CP_BACKUP_START),
	UTIL_NAME_CODER_ENTRY_CUSTOM("_BACKUP_PHASE2", CP_BACKUP_END),
	UTIL_NAME_CODER_ENTRY_CUSTOM("_RECOVERY_CHECKPOINT", CP_AFTER_RECOVERY),
	UTIL_NAME_CODER_ENTRY_CUSTOM("_SHUTDOWN_CHECKPOINT", CP_SHUTDOWN),
	UTIL_NAME_CODER_ENTRY(CP_BACKUP_WITH_LOG_ARCHIVE),
	UTIL_NAME_CODER_ENTRY(CP_BACKUP_WITH_LOG_DUPLICATE),
	UTIL_NAME_CODER_ENTRY(CP_ARCHIVE_LOG_START),
	UTIL_NAME_CODER_ENTRY(CP_ARCHIVE_LOG_END),
	UTIL_NAME_CODER_ENTRY(CP_INCREMENTAL_BACKUP_LEVEL_0),
	UTIL_NAME_CODER_ENTRY(CP_INCREMENTAL_BACKUP_LEVEL_1_CUMULATIVE),
	UTIL_NAME_CODER_ENTRY(CP_INCREMENTAL_BACKUP_LEVEL_1_DIFFERENTIAL),
	UTIL_NAME_CODER_ENTRY(CP_PREPARE_LONG_ARCHIVE),
	UTIL_NAME_CODER_ENTRY(CP_PREPARE_LONGTERM_SYNC),
	UTIL_NAME_CODER_ENTRY(CP_STOP_LONGTERM_SYNC),
	UTIL_NAME_CODER_ENTRY(CP_AFTER_LONGTERM_SYNC)
};

const util::NameCoder<CheckpointMode, CP_MODE_END>
		CheckpointTypes::CP_MODE_CODER(CP_MODE_LIST, 1);



CheckpointMainMessage::CheckpointMainMessage()
: mode_(-1), flag_(0), backupPath_(""), ssn_(-1) {
	;
}

CheckpointMainMessage::CheckpointMainMessage(
		int32_t mode, uint32_t flag, const std::string &path, int64_t ssn)
: mode_(mode), flag_(flag), backupPath_(path), ssn_(ssn) {
	;
}

/*!
	@brief デコード
 */
CheckpointMainMessage CheckpointMainMessage::decode(EventByteInStream &in) {
	int32_t mode;
	uint32_t flag;
	std::string backupPath;
	int64_t ssn;

	in >> mode;
	in >> flag;
	in >> backupPath;
	in >> ssn;

	return CheckpointMainMessage(mode, flag, backupPath, ssn);
}

/*!
	@brief エンコード
 */
void CheckpointMainMessage::encode(EventByteOutStream& out) {
	out << mode_;
	out << flag_;
	out << backupPath_;
	out << ssn_;
}


bool CheckpointMainMessage::isBackupMode() {
	switch(mode_) {
	case CP_BACKUP:
	case CP_BACKUP_START:
	case CP_BACKUP_END:
	case CP_INCREMENTAL_BACKUP_LEVEL_0:
	case CP_INCREMENTAL_BACKUP_LEVEL_1_CUMULATIVE:
	case CP_INCREMENTAL_BACKUP_LEVEL_1_DIFFERENTIAL:
		return true;
	default:
		return false;
	}
}
int32_t CheckpointMainMessage::getIncrementalBackupLevel() {
	switch(mode_) {
	case CP_INCREMENTAL_BACKUP_LEVEL_0:
		return 0;
	case CP_INCREMENTAL_BACKUP_LEVEL_1_CUMULATIVE:
	case CP_INCREMENTAL_BACKUP_LEVEL_1_DIFFERENTIAL:
		return 1;
	default:
		return -1;
	}
}

CheckpointStats::CheckpointStats(const Mapper *mapper) :
		table_(mapper) {
}

void CheckpointStats::setUpMapper(Mapper &mapper) {
	mapper.addFilter(STAT_TABLE_DISPLAY_SELECT_CP);
	mapper.addFilter(STAT_TABLE_DISPLAY_WEB_ONLY);

	mapper.bind(CP_STAT_START_TIME, STAT_TABLE_CP_START_TIME);
	mapper.bind(CP_STAT_END_TIME, STAT_TABLE_CP_END_TIME);
	mapper.bind(CP_STAT_MODE, STAT_TABLE_CP_MODE)
			.setNameCoder(&CheckpointTypes::CP_MODE_CODER);
	mapper.bind(CP_STAT_PENDING_PARTITION_COUNT, STAT_TABLE_CP_PENDING_PARTITION);
	mapper.bind(
			CP_STAT_NORMAL_OPERATION,
			STAT_TABLE_CP_NORMAL_CHECKPOINT_OPERATION);
	mapper.bind(
			CP_STAT_REQUESTED_OPERATION,
			STAT_TABLE_CP_REQUESTED_CHECKPOINT_OPERATION);
	mapper.bind(CP_STAT_BACKUP_OPERATION, STAT_TABLE_CP_BACKUP_OPERATION);
	mapper.bind(CP_STAT_PERIODIC, STAT_TABLE_CP_PERIODIC_CHECKPOINT)
			.setNameCoder(&CheckpointTypes::PERIODIC_FLAG_CODER);
}



CheckpointStats::SetUpHandler CheckpointStats::SetUpHandler::instance_;

#define STAT_ADD(id) STAT_TABLE_ADD_PARAM(stat, parentId, id)

void CheckpointStats::SetUpHandler::operator()(StatTable &stat) {
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
	STAT_ADD(STAT_TABLE_CP_PERIODIC_CHECKPOINT); 
	STAT_ADD(STAT_TABLE_CP_BACKUP_OPERATION);
}



CheckpointStats::Updator::Updator(
		const util::StdAllocator<void, void> &alloc) :
		mapper_(alloc),
		stats_(&mapper_) {
	setUpMapper(mapper_);
}

bool CheckpointStats::Updator::operator()(StatTable &stat) {
	stats_.table_.apply(stat);
	return true;
}



CheckpointOperationState::CheckpointOperationState() :
		phase_(CP_PHASE_NONE) {
}

CheckpointOperationState::~CheckpointOperationState() {
	clear();
}

void CheckpointOperationState::assign(uint32_t partitionCount) {
	while (entryList_.size() < partitionCount) {
		std::unique_ptr<Entry> entry(UTIL_NEW Entry());
		entryList_.push_back(entry.get());
		entry.release();
	}
}

void CheckpointOperationState::clear() {
	while (!entryList_.empty()) {
		std::unique_ptr<Entry> entry(entryList_.back());
		entryList_.pop_back();
	}
}

CheckpointOperationState::Entry&
CheckpointOperationState::getEntry(PartitionId pId) {
	assert(pId < entryList_.size());
	Entry *entry = entryList_[pId];

	assert(entry != NULL);
	return *entry;
}

CheckpointPhase CheckpointOperationState::getPhase() {
	return phase_;
}

void CheckpointOperationState::nextPhase(CheckpointPhase phase) {
	assert(static_cast<int32_t>(phase_) + 1 == static_cast<int32_t>(phase));
	phase_ = phase;
}


CheckpointOperationState::Entry::Entry() :
		bitmap_(false),
		blockCount_(0) {
}

util::LocalUniquePtr<PartitionLock>&
CheckpointOperationState::Entry::getLock() {
	return lock_;
}

UtilBitmap& CheckpointOperationState::Entry::getBitmap() {
	return bitmap_;
}

int64_t CheckpointOperationState::Entry::getBlockCount() {
	return blockCount_;
}

void CheckpointOperationState::Entry::setBlockCount(int64_t count) {
	blockCount_ = count;
}

bool CheckpointOperationState::Entry::resolveCheckpointReady(bool current) {
	return resolveInitialValue(checkpointReady_, current);
}

bool CheckpointOperationState::Entry::resolveNeedCheckpoint(bool current) {
	return resolveInitialValue(needCheckpoint_, current);
}

bool CheckpointOperationState::Entry::resolveInitialValue(
		OptionalBoolValue &value, bool current) {
	bool &assigned = value.first;
	bool &initialValue = value.second;

	if (!assigned) {
		assigned = true;
		initialValue = current;
	}

	return initialValue;
}


/*!
	@brief コンストラクタ
*/
CheckpointService::CheckpointService(
		const ConfigTable &config, EventEngine::Config eeConfig,
		EventEngine::Source source, const char8_t *name,
		ServiceThreadErrorHandler &serviceThreadErrorHandler) :
		serviceThreadErrorHandler_(serviceThreadErrorHandler),
		ee_(createEEConfig(config, eeConfig), source, name),
		configTable_(config),
		clusterService_(NULL),
		clsEE_(NULL),
		transactionService_(NULL),
		transactionManager_(NULL),
		txnEE_(NULL),
		systemService_(NULL),
		partitionList_(NULL),
		partitionTable_(NULL),
		initialized_(false),
		syncService_(NULL),
		fixedSizeAlloc_(NULL),
		varAlloc_(util::AllocatorInfo(
				ALLOCATOR_GROUP_CP, "checkpointServiceVar")),
		requestedShutdownCheckpoint_(false),
		pgConfig_(config),
		cpInterval_(config.get<int32_t>(
				CONFIG_TABLE_CP_CHECKPOINT_INTERVAL)),
		logWriteMode_(config.get<int32_t>(
				CONFIG_TABLE_DS_LOG_WRITE_MODE)),
		backupTopPath_(config.get<const char8_t *>(CONFIG_TABLE_DS_BACKUP_PATH)),
		syncTempTopPath_(config.get<const char8_t *>(CONFIG_TABLE_DS_SYNC_TEMP_PATH)),
		chunkCopyIntervalMillis_(config.get<int32_t>(
				CONFIG_TABLE_CP_CHECKPOINT_COPY_INTERVAL)),
		backupEndPending_(false),
		currentDuplicateLogMode_(false),
		currentCpPId_(UNDEF_PARTITIONID),
		parallelCheckpoint_(false), 
		errorOccured_(false),
		enableLsnInfoFile_(true), 
		statUpdator_(varAlloc_),
		lastMode_(CP_UNKNOWN),
		archiveLogMode_(0),
		enablePeriodicCheckpoint_(true), 
		newestLogFormatVersion_(0)  
{
	try {
		backupInfo_.setConfigValue(
				config.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM),
				config.getUInt32(CONFIG_TABLE_DS_CONCURRENCY));
		backupInfo_.setGSVersion(
				config.get<const char8_t *>(CONFIG_TABLE_DEV_SIMPLE_VERSION));

		ee_.setHandler(CP_REQUEST_CHECKPOINT, checkpointServiceMainHandler_);

		ee_.setHandler(CP_TIMER_LOG_FLUSH, flushLogPeriodicallyHandler_);

		ee_.setThreadErrorHandler(serviceThreadErrorHandler_);

		lsnInfo_.setConfigValue(
				this, config.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM),
				config.getUInt32(CONFIG_TABLE_DS_CONCURRENCY),
				config.get<const char8_t *>(CONFIG_TABLE_DS_DB_PATH));

		ssnList_.assign(pgConfig_.getPartitionCount(), UNDEF_SYNC_SEQ_NUMBER);

		lastCpStartLsnList_.assign(pgConfig_.getPartitionCount(), UNDEF_LSN);

		uint8_t temp = 0;

		setLastMode(CP_UNKNOWN);
		setPeriodicCheckpointFlag(true);
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

	eeConfig.setPartitionCount(config.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM));

	eeConfig.setAllAllocatorGroup(ALLOCATOR_GROUP_CP);

	return eeConfig;
}

/*!
	@brief デストラクタ
*/
CheckpointService::~CheckpointService() {}

/*!
	@brief 初期化
	@note ハンドラ内で利用する他サービスのアドレスをセットする
*/
void CheckpointService::initialize(ManagerSet &resourceSet) {
	try {
		clusterService_ = resourceSet.clsSvc_;
		clsEE_ = clusterService_->getEE();
		transactionService_ = resourceSet.txnSvc_;
		txnEE_ = transactionService_->getEE();
		transactionManager_ = resourceSet.txnMgr_;
		systemService_ = resourceSet.sysSvc_;
		partitionTable_ = resourceSet.pt_;
		partitionList_ = resourceSet.partitionList_;
		fixedSizeAlloc_ = resourceSet.fixedSizeAlloc_;

		resourceSet.stats_->addUpdator(&statUpdator_);

		checkpointServiceMainHandler_.initialize(resourceSet);
		flushLogPeriodicallyHandler_.initialize(resourceSet);
		syncService_ = resourceSet.syncSvc_;
		serviceThreadErrorHandler_.initialize(resourceSet);
		initialized_ = true;
		errorOccured_ = false;

		Partition& partition = partitionList_->partition(0);
		newestLogFormatVersion_ = partition.logManager().getLogFormatVersion();
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(
				e, "Initialize failed. (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief サービス開始処理
*/
void CheckpointService::start(const Event::Source &eventSource) {
	try {
		if (!initialized_) {
			GS_THROW_USER_ERROR(GS_ERROR_CP_SERVICE_NOT_INITIALIZED, "");
		}
		ee_.start();

		Event requestEvent(
				eventSource, CP_REQUEST_CHECKPOINT, CP_HANDLER_PARTITION_ID);

		CheckpointMainMessage message(CP_NORMAL, 0, "", UNDEF_SYNC_SEQ_NUMBER);
		EventByteOutStream out = requestEvent.getOutStream();
		message.encode(out);

		ee_.addTimer(requestEvent, CommonUtility::changeTimeSecondToMilliSecond(cpInterval_));

		if (logWriteMode_ > 0 && logWriteMode_ < INT32_MAX) {
			Event flushLogEvent(
					eventSource, CP_TIMER_LOG_FLUSH, CP_SERIALIZED_PARTITION_ID);
			ee_.addPeriodicTimer(
					flushLogEvent, CommonUtility::changeTimeSecondToMilliSecond(logWriteMode_));
		}

		if (!syncTempTopPath_.empty()) {
			cleanSyncTempDir();
		}
	}
	catch (std::exception &e) {
		clusterService_->setError(eventSource, &e);
		GS_RETHROW_SYSTEM_ERROR(
				e, "Start failed. (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief 長期同期一時ディレクトリ初期化
*/
void CheckpointService::cleanSyncTempDir() {
	try {
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
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(
			e, "Initialize synctemp directory failed. (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief サービス終了処理
*/
void CheckpointService::shutdown() {
	ee_.shutdown();
}

/*!
	@brief シャットダウン待ち
*/
void CheckpointService::waitForShutdown() {
	ee_.waitForShutdown();
}

/*!
	@brief EE取得
*/
EventEngine *CheckpointService::getEE() {
	return &ee_;
}


/*!
	@brief チェックポイントサービスハンドラ
	@note CheckpointServiceのスレッドで実行
*/
void CheckpointServiceMainHandler::operator()(EventContext &ec, Event &ev) {
	bool critical = false;
	try {
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);

		EventByteInStream in = ev.getInStream();
		CheckpointMainMessage message = CheckpointMainMessage::decode(in);

		if (message.mode() == CP_AFTER_RECOVERY || message.mode() == CP_SHUTDOWN) {
			critical = true;
		}

		if (message.mode() == CP_NORMAL) {
			ec.getEngine().addTimer(
					ev, CommonUtility::changeTimeSecondToMilliSecond(
					checkpointService_->getCheckpointInterval()));

			if (checkpointService_->getPeriodicCheckpointFlag()) {
				checkpointService_->runCheckpoint(ec, message);
			}
			else {
				GS_TRACE_INFO(
						CHECKPOINT_SERVICE_STATUS_DETAIL, GS_TRACE_CP_STATUS,
						"Periodic checkpoint skipped.");
			}
		}
		else {
			checkpointService_->runCheckpoint(ec, message);
		}
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
	catch (SystemException &e) {
		UTIL_TRACE_EXCEPTION(CHECKPOINT_SERVICE, e, "SystemException");
		checkpointService_->errorOccured_ = true;
		throw;
	}
	catch (std::exception &e) {
		checkpointService_->errorOccured_ = true;
		clusterService_->setError(ec, &e);
		UTIL_TRACE_EXCEPTION(CHECKPOINT_SERVICE, e, "std::exception");
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief オフセット収集を行うかを判定
*/
bool CheckpointService::needCollectOffset(int32_t mode) {
	switch(mode) {
	case CP_BACKUP:
	case CP_BACKUP_START:
	case CP_BACKUP_WITH_LOG_DUPLICATE:
	case CP_INCREMENTAL_BACKUP_LEVEL_0:
	case CP_INCREMENTAL_BACKUP_LEVEL_1_CUMULATIVE:
	case CP_INCREMENTAL_BACKUP_LEVEL_1_DIFFERENTIAL:
	case CP_PREPARE_LONGTERM_SYNC:
		return true;
		break;
	default:
		return false;
	}
}

/*!
	@brief チェックポイント実行可能かを判定
*/
bool CheckpointService::checkCPrunnable(int32_t mode) {
	if ((lastMode_ == CP_UNKNOWN && mode != CP_AFTER_RECOVERY) ||
			lastMode_ == CP_SHUTDOWN ||
			(mode != CP_BACKUP_END && mode != CP_SHUTDOWN && backupEndPending_) ||
			(mode == CP_BACKUP_END && !backupEndPending_) ||
			(mode != CP_SHUTDOWN && requestedShutdownCheckpoint_)  
		) {
		GS_TRACE_WARNING(
				CHECKPOINT_SERVICE, GS_TRACE_CP_CHECKPOINT_CANCELLED,
				"Checkpoint cancelled by status (mode=" <<
				checkpointModeToString(mode) <<
				", lastMode=" << checkpointModeToString(lastMode_) <<
				", shutdownRequested=" << requestedShutdownCheckpoint_ <<
				", backupEndPending=" << backupEndPending_ <<
				")");
		return false;
	}
	return true;
}


/*!
	@brief チェックポイント実行本体
*/
void CheckpointService::runCheckpoint(
		EventContext &ec, CheckpointMainMessage &message) {

	try {
		if (!checkCPrunnable(message.mode())) {
			return;
		}

		errorOccured_ = false;
		backupEndPending_ = false;
		bool backupCommandWorking = prepareBackupDirectory(message);

		CheckpointDataCleaner cpDataCleaner(
			*this, ec, backupCommandWorking, message.backupPath());

		setLastMode(message.mode());

		const int64_t startTime = util::DateTime::now(false).getUnixTime();
		stats(CheckpointStats::CP_STAT_START_TIME).set(
			static_cast<uint64_t>(startTime));
		stats(CheckpointStats::CP_STAT_END_TIME).set(0);

		LocalStatValue& pendingPartitionCount =
			stats(CheckpointStats::CP_STAT_PENDING_PARTITION_COUNT);
		pendingPartitionCount.set(pgConfig_.getPartitionCount());

		traceCheckpointStart(message);

		prepareAllCheckpoint(message);
		checkFailureLongtermSyncCheckpoint(message.mode(), 0);
		if (message.mode() != CP_PREPARE_LONGTERM_SYNC
			&& message.mode() != CP_STOP_LONGTERM_SYNC
			&& message.mode() != CP_AFTER_LONGTERM_SYNC) {
				{
					const uint32_t partitionCount = pgConfig_.getPartitionCount();
					CheckpointOperationState state;
					state.assign(partitionCount);
					{
						state.nextPhase(CP_PHASE_PRE_WRITE);
						for (PartitionId pId = 0; pId < partitionCount; ++pId) {
							partitionCheckpoint(pId, ec, message, state);
						}
					}
					util::FileSystem::syncAll();
					{
						state.nextPhase(CP_PHASE_PRE_SYNC);
						for (PartitionId pId = 0; pId < partitionCount; ++pId) {
							partitionCheckpoint(pId, ec, message, state);
						}
					}
					util::FileSystem::syncAll();
					{
						state.nextPhase(CP_PHASE_FINISH);
						for (PartitionId pId = 0; pId < partitionCount; ++pId) {
							partitionCheckpoint(pId, ec, message, state);
							pendingPartitionCount.decrement();
						}
					}
					state.clear();
				}

				lsnInfo_.endCheckpoint(message.backupPath().c_str());
		}
		else if (message.mode() == CP_PREPARE_LONGTERM_SYNC
			|| message.mode() == CP_AFTER_LONGTERM_SYNC) {
			const int64_t ssn = message.ssn();
			assert(ssn != UNDEF_SYNC_SEQ_NUMBER);
			checkFailureLongtermSyncCheckpoint(message.mode(), 1);

			CpLongtermSyncInfo* info = getCpLongtermSyncInfo(ssn);
			if (info != NULL) {
				info->startTime_ = startTime;
				assert(info->targetPId_ != UNDEF_PARTITIONID);
				PartitionId pId = info->targetPId_;
				pendingPartitionCount.set(1);

				const uint32_t partitionCount = pgConfig_.getPartitionCount();
				CheckpointOperationState state;
				state.assign(partitionCount);
				{
					state.nextPhase(CP_PHASE_PRE_WRITE);
					partitionCheckpoint(pId, ec, message, state);
				}
				util::FileSystem::syncAll();
				{
					state.nextPhase(CP_PHASE_PRE_SYNC);
					partitionCheckpoint(pId, ec, message, state);
				}
				util::FileSystem::syncAll();
				{
					state.nextPhase(CP_PHASE_FINISH);
					partitionCheckpoint(pId, ec, message, state);
					pendingPartitionCount.decrement();
				}
				state.clear();
			}
		}
		else {
			assert(message.mode() == CP_STOP_LONGTERM_SYNC);
			const int64_t ssn = message.ssn();
			assert(ssn != UNDEF_SYNC_SEQ_NUMBER);
			CpLongtermSyncInfo* info = getCpLongtermSyncInfo(ssn);
			if (info != NULL) {
				assert(info->targetPId_ != UNDEF_PARTITIONID);
				pendingPartitionCount.set(1);
				stopLongtermSync(info->targetPId_, ec, message);
				pendingPartitionCount.decrement();
			}
		}
		notifyCheckpointEnd(ec, message);
		traceCheckpointEnd(message);

		pendingPartitionCount.set(0);

		if (errorOccured_) {
			handleError(ec, message);
			return;
		}
		completeAllCheckpoint(message, backupCommandWorking);
	}
	catch (std::exception& e) {
		handleError(ec, message);
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void CheckpointService::checkFailureBeforeWriteGroupLog(int32_t mode) {
}
void CheckpointService::checkFailureAfterWriteGroupLog(int32_t mode) {
}
void CheckpointService::checkFailureWhileWriteChunkMetaLog(int32_t mode) {
}

void CheckpointService::checkFailureLongtermSyncCheckpoint(int32_t mode, int32_t point) {
}
/*!
	@brief 長期同期停止処理
*/
void CheckpointService::stopLongtermSync(
		PartitionId pId, EventContext &ec, CheckpointMainMessage &message) {

	int32_t mode = message.mode();
	PartitionLock pLock(*transactionManager_, pId);

	Partition& partition = partitionList_->partition(pId);

	const int64_t ssn = message.ssn();
	assert(ssn != UNDEF_SYNC_SEQ_NUMBER);

	CpLongtermSyncInfo *info = getCpLongtermSyncInfo(ssn);
	if (info != NULL) {
		CpLongtermSyncInfo tmpInfo = *info;

		assert(info->targetPId_ != UNDEF_PARTITIONID);
		LogManager<MutexLocker>& logManager =
				partitionList_->partition(info->targetPId_).logManager();
		logManager.setKeepXLogVersion(-1); 
		removeCpLongtermSyncInfo(ssn);

		try {
			delete tmpInfo.syncCpFile_;
		}
		catch (std::exception &e) {
			GS_TRACE_WARNING(CHECKPOINT_SERVICE,
							 GS_TRACE_CP_LONGTERM_SYNC_INFO,
							 "Remove long sync temporary file failed.  (reason=" <<
							 GS_EXCEPTION_MESSAGE(e) << ")");
		}
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
}
/*!
	@brief パーティション単位のチェックポイント処理
*/
void CheckpointService::partitionCheckpoint(
		PartitionId pId, EventContext& ec, CheckpointMainMessage &message,
		CheckpointOperationState &state) {
	const int32_t mode = message.mode();

	const CheckpointPhase phase = state.getPhase();
	CheckpointOperationState::Entry &stateEntry = state.getEntry(pId);

	if (phase == CP_PHASE_PRE_WRITE) {
		GS_TRACE_DEBUG(
				CHECKPOINT_SERVICE, GS_TRACE_CP_STATUS,
				"[CP_PARTITION_START] mode=" << checkpointModeToString(mode) <<
				", pId=" << pId <<
				", backupPath=" << message.backupPath());
		{
			uint16_t newestLogFormatVersion = getNewestLogFormatVersion();
			uint16_t logFormatVersion =
					partitionList_->partition(pId).logManager().getLogFormatVersion();
			uint16_t version = LogManagerConst::selectNewerVersion(
					newestLogFormatVersion, logFormatVersion);
			if (newestLogFormatVersion != version) {
				partitionList_->partition(pId).logManager().setLogFormatVersion(version);
				setNewestLogFormatVersion(version);
				GS_TRACE_INFO(
					CHECKPOINT_SERVICE_DETAIL, GS_TRACE_CP_STATUS,
					"setLogFormatVersion: pId=" << pId <<
					", old=" << logFormatVersion << ", new=" << version
					);
			}
		}
	}

	if (phase == CP_PHASE_PRE_WRITE) {
		stateEntry.getLock() = UTIL_MAKE_LOCAL_UNIQUE(
				stateEntry.getLock(), PartitionLock,
				*transactionManager_, pId);
	}
	else {
		assert(stateEntry.getLock().get() != NULL);
	}

	Partition& partition = partitionList_->partition(pId);

	const bool checkpointReady = stateEntry.resolveCheckpointReady(
			(partition.chunkManager().getStatus() == 1));
	const bool needCheckpoint = stateEntry.resolveNeedCheckpoint(
			partition.needCheckpoint(phase == CP_PHASE_PRE_WRITE));
	const bool isNormalCheckpoint =
			(mode == CP_NORMAL || mode == CP_REQUESTED || mode == CP_SHUTDOWN);

	if (mode == CP_AFTER_RECOVERY) {

		partition.fullCheckpoint(phase);

		if (phase == CP_PHASE_FINISH) {
			lsnInfo_.setLsn(pId, partition.logManager().getLSN());
			partitionTable_->setStartLSN(pId, partition.logManager().getStartLSN());
		}
	}
	else if ( checkpointReady && ((mode != CP_NORMAL) || needCheckpoint) ) {
		const bool byCP = true;
		if (phase == CP_PHASE_PRE_WRITE) {
			util::LockGuard<util::Mutex> guard(partition.mutex());
			partition.logManager().flushXLog(LOG_WRITE_WAL_BUFFER, byCP);
		}
		if (phase == CP_PHASE_PRE_SYNC) {
			partition.logManager().flushXLog(LOG_FLUSH_FILE, byCP);
		}
		UtilBitmap &bitmap = stateEntry.getBitmap();
		LogSequentialNumber startLsn = UNDEF_LSN;
		if (phase == CP_PHASE_PRE_WRITE) {
			DuplicateLogMode duplicateLogMode;
			const uint32_t flag = message.flag();
			const bool duplicateFlag = ((flag & BACKUP_DUPLICATE_LOG_MODE_FLAG) != 0);
			const bool skipBaseLineFlag = ((flag & BACKUP_SKIP_BASELINE_MODE_FLAG) != 0);
			if (message.isBackupMode() && duplicateFlag) {
				duplicateLogMode.status_ = DuplicateLogMode::DUPLICATE_LOG_ENABLE;
				duplicateLogMode.stopOnDuplicateErrorFlag_ =
						((flag & BACKUP_STOP_ON_DUPLICATE_ERROR_FLAG) != 0);
				util::NormalOStringStream oss;
				oss << message.backupPath().c_str() << "/txnlog/" << pId;
				util::FileSystem::createDirectoryTree(oss.str().c_str());
				duplicateLogMode.duplicateLogPath_ = oss.str();
			}
			util::LockGuard<util::Mutex> guard(partition.mutex());
			if (message.isBackupMode()) {
				partition.startCheckpoint(&duplicateLogMode);
			}
			else {
				partition.startCheckpoint(NULL);
			}
			if (mode == CP_PREPARE_LONGTERM_SYNC) {
				partition.logManager().setKeepXLogVersion(
						partition.logManager().getStartLogVersion());
			}
			startLsn = partition.logManager().getLSN();
			if (needCollectOffset(mode)) {
				int64_t blockCount = partition.collectOffset(mode, bitmap);
				stateEntry.setBlockCount(blockCount);
			}
		}
		if (phase == CP_PHASE_PRE_WRITE && mode == CP_PREPARE_LONGTERM_SYNC) {
			const int64_t ssn = message.ssn();
			assert(ssn != UNDEF_SYNC_SEQ_NUMBER);
			CpLongtermSyncInfo* info = getCpLongtermSyncInfo(ssn);
			info->logVersion_ = partition.logManager().getLogVersion();
			info->startLsn_ = startLsn;
		}
		if (phase == CP_PHASE_PRE_SYNC) {
			partition.closePrevLogFiles();
		}
		checkSystemError();

		if (phase == CP_PHASE_PRE_WRITE) {
			util::LocalUniquePtr<MeshedChunkTable::CsectionChunkCursor> cursorPtr;
			{
				util::LockGuard<util::Mutex> guard(partition.mutex());
				cursorPtr = UTIL_MAKE_LOCAL_UNIQUE(
						cursorPtr, MeshedChunkTable::CsectionChunkCursor,
						partition.chunkManager().createCsectionChunkCursor());
			}
			MeshedChunkTable::CsectionChunkCursor &cursor = *cursorPtr;

			const PartitionGroupId pgId =
					getPGConfig().getPartitionGroupId(pId);
			while (cursor.hasNext()) {
				{
					util::LockGuard<util::Mutex> guard(partition.mutex());
					partition.executePartialCheckpoint(
							cursor, Partition::PARTIAL_CHECKPOINT_INTERVAL,
							partition.calcCheckpointRange(mode));
				}
				checkSystemError();
				int32_t queueSize = getTransactionEEQueueSize(pgId);
				if (mode != CP_AFTER_RECOVERY && mode != CP_SHUTDOWN) {
					util::Thread::sleep(0); 
				}
				if (mode != CP_AFTER_RECOVERY && mode != CP_SHUTDOWN &&
					(queueSize > CP_CHUNK_COPY_WITH_SLEEP_LIMIT_QUEUE_SIZE)) {
					util::Thread::sleep(chunkCopyIntervalMillis_);
				}
			}
		}

		if (phase == CP_PHASE_PRE_SYNC) {

			partition.endCheckpoint(CP_END_FLUSH_DATA);
		}
		if (phase == CP_PHASE_PRE_SYNC) {
			util::LockGuard<util::Mutex> guard(partition.mutex());
			partition.endCheckpoint(CP_END_ADD_CPLOG);
		}

		if (phase == CP_PHASE_FINISH) {
			partition.endCheckpoint(CP_END_FLUSH_CPLOG);
		}
		if (phase == CP_PHASE_FINISH && mode != CP_SHUTDOWN) {
			LogIterator<MutexLocker> cpit = partition.endCheckpointPrepareRelaseBlock();
			std::unique_ptr<Log> log;
			uint64_t releaseCount = 0;
			while (true) {
				util::Thread::sleep(0); 
				util::LockGuard<util::Mutex> guard(partition.mutex());
				const uint64_t startClock = util::Stopwatch::currentClock();
				while (true) {
					util::StackAllocator::Scope scope(partition.logManager().getAllocator());
					log = cpit.next(false);
					if (log == NULL) break;
					partition.endCheckpointReleaseBlock(log.get());
					++releaseCount;

					const uint32_t lap = util::Stopwatch::clockToMillis(
							util::Stopwatch::currentClock() - startClock);
					if (lap > RELEASE_BLOCK_THRESHOLD_MILLIS) {
						GS_TRACE_DEBUG(IO_MONITOR, GS_TRACE_CM_LONG_EVENT,
										 "relaseBlock: break: lap(millis)=" << lap <<
										 ", releaseCount=" << releaseCount);
						break;
					}
				}
				if (log == NULL) break;
			}
		}
		if (phase == CP_PHASE_FINISH) {
			partition.removeLogFiles(partition.calcCheckpointRange(mode));
			partitionTable_->setStartLSN(pId, partition.logManager().getStartLSN());
			const uint32_t flag = message.flag();
			const bool enableDuplicateLog = ((flag & BACKUP_DUPLICATE_LOG_MODE_FLAG) != 0);
			const bool skipBaseLineFlag = ((flag & BACKUP_SKIP_BASELINE_MODE_FLAG) != 0);
			const int64_t logVer = partition.logManager().getLogVersion();
			if ((CP_BACKUP == mode || CP_BACKUP_START == mode ||
					CP_INCREMENTAL_BACKUP_LEVEL_0 == mode)) {
				if (!skipBaseLineFlag) {
					backupInfo_.setCheckCpFileSizeFlag(false);
				}
				backupInfo_.startCpFileCopy(pId, logVer);
				if (CP_INCREMENTAL_BACKUP_LEVEL_0 == mode) {
					backupInfo_.startIncrementalBlockCopy(pId, mode);
				}
				backupInfo_.startLogFileCopy();
			}
			if (CP_INCREMENTAL_BACKUP_LEVEL_1_CUMULATIVE == mode ||
					CP_INCREMENTAL_BACKUP_LEVEL_1_DIFFERENTIAL == mode) {
				backupInfo_.startIncrementalBlockCopy(pId, mode);
				backupInfo_.startLogFileCopy();
			}
			partition.postCheckpoint(
					mode, bitmap, message.backupPath(),
					enableDuplicateLog, skipBaseLineFlag);

			if ((CP_BACKUP == mode || CP_BACKUP_START == mode ||
					CP_INCREMENTAL_BACKUP_LEVEL_0 == mode)) {
				if (CP_INCREMENTAL_BACKUP_LEVEL_0 == mode) {
					backupInfo_.endIncrementalBlockCopy(pId, logVer);
				}
				backupInfo_.endCpFileCopy(pId, 0);
				backupInfo_.endLogFileCopy(pId, 0);
			}
			if (CP_INCREMENTAL_BACKUP_LEVEL_1_CUMULATIVE == mode ||
					CP_INCREMENTAL_BACKUP_LEVEL_1_DIFFERENTIAL == mode) {
				backupInfo_.endIncrementalBlockCopy(pId, logVer);
				backupInfo_.endLogFileCopy(pId, 0);
			}
			if (mode == CP_PREPARE_LONGTERM_SYNC) {
				const int64_t ssn = message.ssn();
				assert(ssn != UNDEF_SYNC_SEQ_NUMBER);
				CpLongtermSyncInfo *info = getCpLongtermSyncInfo(ssn);
				info->totalChunkCount_= stateEntry.getBlockCount();
				info->readyCount_= stateEntry.getBlockCount();

				int32_t stripeSize = configTable_.getUInt32(CONFIG_TABLE_DS_DB_FILE_SPLIT_STRIPE_SIZE);
				int32_t numFiles = configTable_.getUInt32(CONFIG_TABLE_DS_DB_FILE_SPLIT_COUNT);
				const int32_t chunkSize = partition.chunkManager().getChunkSize();

				util::NormalOStringStream oss;
				oss	<< message.backupPath().c_str() << "/data/" << pId;
				std::unique_ptr<NoLocker> noLocker(UTIL_NEW NoLocker());
				VirtualFileNIO* dataFile = UTIL_NEW VirtualFileNIO(
						std::move(noLocker), oss.str().c_str(), pId,
						stripeSize, numFiles, chunkSize, NULL);
				dataFile->open();
				info->syncCpFile_ = dataFile;

				util::NormalOStringStream ossCpLog;
				ossCpLog << message.backupPath().c_str() << "/data/" << pId;
				util::NormalOStringStream ossXLog;
				ossXLog << message.backupPath().c_str() << "/txnlog/" << pId;

				syncService_->notifyCheckpointLongSyncReady(
					ec, info->targetPId_, &info->longtermSyncInfo_, false);

				GS_TRACE_INFO(
					CHECKPOINT_SERVICE_STATUS_DETAIL, GS_TRACE_CP_STATUS,
					"makeSyncTempCpFile(completed): ssn," << ssn <<
					",pId," << info->targetPId_ << ",totalCount," << info->totalChunkCount_);
			}
		}
	}

	if (phase == CP_PHASE_FINISH) {
		lsnInfo_.setLsn(pId, partition.logManager().getLSN());
		checkSystemError();

		GS_TRACE_INFO(
				CHECKPOINT_SERVICE_STATUS_DETAIL, GS_TRACE_CP_STATUS,
				"[CP_PARTITION_END] mode=" << checkpointModeToString(mode) <<
				", pId=" << pId);

		stateEntry.getLock().reset();
		assert(stateEntry.getLock().get() == NULL);
	}
}

bool CheckpointService::prepareBackupDirectory(CheckpointMainMessage &message) {
	if (message.mode() == CP_BACKUP ||
			message.mode() == CP_BACKUP_START ||
			message.mode() == CP_BACKUP_END ||
			message.mode() == CP_INCREMENTAL_BACKUP_LEVEL_0) {
		makeBackupDirectory(message.mode(), true, message.backupPath());

		return true;
	}
	else {
		return false;
	}
}

void CheckpointService::traceCheckpointStart(CheckpointMainMessage &message) {
	if (message.mode() != CP_PREPARE_LONGTERM_SYNC && message.mode() != CP_STOP_LONGTERM_SYNC) { 
		if (message.mode() != CP_NORMAL) {
		GS_TRACE_INFO(
				CHECKPOINT_SERVICE, GS_TRACE_CP_STATUS,
				"[CP_START] mode=" << checkpointModeToString(message.mode()) <<
				", backupPath=" << message.backupPath().c_str());
		}
		else {
			GS_TRACE_INFO(
					CHECKPOINT_SERVICE_STATUS_DETAIL, GS_TRACE_CP_STATUS,
					"[CP_START] mode=" << checkpointModeToString(message.mode()) <<
					", backupPath=" << message.backupPath().c_str());
		}
	}
	else {
		GS_TRACE_INFO(
				CHECKPOINT_SERVICE, GS_TRACE_CP_STATUS,
				"[CP_LONGTERM_SYNC_START] mode=" << checkpointModeToString(message.mode()) <<
				", SSN=" << message.ssn());
	}
}

void CheckpointService::traceCheckpointEnd(CheckpointMainMessage &message) {
	if (message.mode() != CP_PREPARE_LONGTERM_SYNC && message.mode() != CP_STOP_LONGTERM_SYNC) { 
		GS_TRACE_INFO(
				CHECKPOINT_SERVICE, GS_TRACE_CP_STATUS,
				"[CP_END] mode=" << checkpointModeToString(message.mode()) <<
				", backupPath=" << message.backupPath() <<
				", elapsedMillis=" <<
				getLastDuration(util::DateTime::now(false)));
	}
	else {
		GS_TRACE_INFO(
				CHECKPOINT_SERVICE, GS_TRACE_CP_STATUS,
				"[CP_LONGTERM_SYNC_END] mode=" << checkpointModeToString(message.mode()) <<
				", SSN=" << message.ssn() << ", elapsedMillis=" <<
				getLastDuration(util::DateTime::now(false)));
	}
}

void CheckpointService::prepareAllCheckpoint(CheckpointMainMessage &message) {
	const uint32_t flag = message.flag();
	const bool skipBaseLineFlag = ((flag & BACKUP_SKIP_BASELINE_MODE_FLAG) != 0);
	if (message.mode() == CP_BACKUP || message.mode() == CP_BACKUP_START) {
		backupInfo_.startBackup(message.backupPath(), skipBaseLineFlag);
	}
	else if (message.mode() == CP_INCREMENTAL_BACKUP_LEVEL_0 ||
			message.mode() == CP_INCREMENTAL_BACKUP_LEVEL_1_CUMULATIVE ||
			message.mode() == CP_INCREMENTAL_BACKUP_LEVEL_1_DIFFERENTIAL) {
		backupInfo_.startIncrementalBackup(
				message.backupPath(), lastBackupPath_, message.mode());
	}
}

void CheckpointService::notifyCheckpointEnd(
		EventContext &ec, CheckpointMainMessage &message) {
	if (message.mode() == CP_AFTER_RECOVERY) {
		clusterService_->requestCompleteCheckpoint(ec, ec.getAllocator(), true);
		std::cout << "done." << std::endl;
	}
	if (message.mode() == CP_AFTER_LONGTERM_SYNC) {
		CpLongtermSyncInfo* info = getCpLongtermSyncInfo(message.ssn());
		assert(info);

		syncService_->notifySyncCheckpointEnd(
			ec, info->targetPId_, &info->longtermSyncInfo_, false);
	}
	if (message.mode() == CP_SHUTDOWN) {
		clusterService_->requestCompleteCheckpoint(
				ec, ec.getAllocator(), false);
	}
}

void CheckpointService::handleError(EventContext &ec, CheckpointMainMessage &message) {
	if (message.mode() == CP_BACKUP || message.mode() == CP_BACKUP_START ||
			message.mode() == CP_BACKUP_END ||
			message.mode() == CP_INCREMENTAL_BACKUP_LEVEL_0 ||
			message.mode() == CP_INCREMENTAL_BACKUP_LEVEL_1_CUMULATIVE ||
			message.mode() == CP_INCREMENTAL_BACKUP_LEVEL_1_DIFFERENTIAL) {
		backupInfo_.errorBackup();
	}
	else if (message.mode() == CP_PREPARE_LONGTERM_SYNC || message.mode() == CP_AFTER_LONGTERM_SYNC) { 
		assert(message.ssn() != UNDEF_SYNC_SEQ_NUMBER);
		CpLongtermSyncInfo *info = getCpLongtermSyncInfo(message.ssn());
		if (info != NULL) { 
			info->errorOccured_ = true;
			syncService_->notifySyncCheckpointError(
					ec, info->targetPId_, &info->longtermSyncInfo_);
		}
	}
}

void CheckpointService::completeAllCheckpoint(CheckpointMainMessage &message, bool& backupCommandWorking) {
	backupCommandWorking = false;
	if (message.mode() == CP_BACKUP_START) {
		backupEndPending_ = true;
	}
	if (message.mode() == CP_BACKUP || message.mode() == CP_BACKUP_START ||
			message.mode() == CP_INCREMENTAL_BACKUP_LEVEL_0 ||
			message.mode() == CP_INCREMENTAL_BACKUP_LEVEL_1_CUMULATIVE ||
			message.mode() == CP_INCREMENTAL_BACKUP_LEVEL_1_DIFFERENTIAL) {
		lastBackupPath_ = message.backupPath();
	}
	if (message.mode() == CP_BACKUP || message.mode() == CP_BACKUP_END ||
			message.mode() == CP_INCREMENTAL_BACKUP_LEVEL_0 ||
			message.mode() == CP_INCREMENTAL_BACKUP_LEVEL_1_CUMULATIVE ||
			message.mode() == CP_INCREMENTAL_BACKUP_LEVEL_1_DIFFERENTIAL) {
		backupInfo_.endBackup();
		stats(CheckpointStats::CP_STAT_BACKUP_OPERATION).increment();
	}
	else if (message.mode() == CP_NORMAL) {
		stats(CheckpointStats::CP_STAT_NORMAL_OPERATION).increment();
	}
	else if (message.mode() == CP_REQUESTED) {
		stats(CheckpointStats::CP_STAT_REQUESTED_OPERATION).increment();
	}
}

/*
	@brief 強制チェックポイント要求
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

		CheckpointMainMessage message(CP_REQUESTED, 0, "", UNDEF_SYNC_SEQ_NUMBER);
		EventByteOutStream out = requestEvent.getOutStream();
		message.encode(out);

		ee_.add(requestEvent);
	}
	catch (std::exception &e) {
		clusterService_->setError(eventSource, &e);
		GS_RETHROW_SYSTEM_ERROR(
				e, "Request normal checkpoint failed. (reason=" <<
				GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*
	@brief シャットダウン時チェックポイント要求
	@note 要求後、他のチェックポイント要求は無視される
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

		CheckpointMainMessage message(CP_SHUTDOWN, 0, "", UNDEF_SYNC_SEQ_NUMBER);
		EventByteOutStream out = requestEvent.getOutStream();
		message.encode(out);

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

int64_t CheckpointService::getLastDuration(const util::DateTime &now) const {
	const int64_t startTime = static_cast<int64_t>(stats()(
			CheckpointStats::CP_STAT_START_TIME).get());
	const int64_t endTime = static_cast<int64_t>(stats()(
			CheckpointStats::CP_STAT_END_TIME).get());
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

/*
	@brief 長期同期キャッチアップ側リカバリチェックポイント実行要求
*/
void CheckpointService::requestSyncCheckpoint(
		EventContext& ec, PartitionId pId, LongtermSyncInfo* longtermSyncInfo) {

	if (lastMode_ == CP_SHUTDOWN) {
		return;
	}
	assert(longtermSyncInfo);
	try {
		if (requestedShutdownCheckpoint_) {
			GS_THROW_USER_ERROR(
					GS_ERROR_CP_CONTROLLER_ILLEAGAL_STATE,
					"Checkpoint cancelled: already requested shutdown ("
					"lastMode=" <<
					checkpointModeToString(lastMode_) << ")");
		}
		GS_TRACE_INFO(
			CHECKPOINT_SERVICE, GS_TRACE_CP_STATUS, "[SyncCP] requested. pId="
			<< pId << ", SSN=" << longtermSyncInfo->getSequentialNumber());
		
		{
			CpLongtermSyncInfo info;
			info.ssn_ = longtermSyncInfo->getSequentialNumber();
			info.targetPId_ = pId;
			info.longtermSyncInfo_.copy(*longtermSyncInfo);
			info.chunkSize_ = partitionList_->partition(pId).chunkManager().getChunkSize();
			bool success = setCpLongtermSyncInfo(info.ssn_, info);
			if (!success) {
				GS_THROW_USER_ERROR(GS_ERROR_CP_LONGTERM_SYNC_FAILED,
					"Same syncSequentialNumber already used (pId=" <<
					pId << ", syncSequentialNumber=" << info.ssn_ << ")");
			}
		}

		Event requestEvent(
				ec, CP_REQUEST_CHECKPOINT, CP_HANDLER_PARTITION_ID);

		CheckpointMainMessage message(CP_AFTER_LONGTERM_SYNC, 0, "",
									  longtermSyncInfo->getSequentialNumber());
		EventByteOutStream out = requestEvent.getOutStream();
		message.encode(out);

		ee_.add(requestEvent);
	}
	catch (std::exception &e) {
		clusterService_->setError(ec, &e);
		GS_RETHROW_SYSTEM_ERROR(
				e, "Request sync checkpoint failed. (reason=" <<
				GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*
	@brief 長期同期オーナ側の停止要求
*/
void CheckpointService::cancelSyncCheckpoint(EventContext& ec, PartitionId pId, int64_t ssn) {
	CpLongtermSyncInfo* longSyncInfo = getCpLongtermSyncInfo(ssn);
	if (longSyncInfo != NULL) {
		try {
			longSyncInfo->atomicStopFlag_ = 1; 
			GS_TRACE_INFO(
					CHECKPOINT_SERVICE, GS_TRACE_CP_STATUS,
					"[CancelSyncCheckpoint] requested (SSN=" << ssn << ")");

			Event requestEvent(
					ec, CP_REQUEST_CHECKPOINT, CP_HANDLER_PARTITION_ID);

			CheckpointMainMessage message(CP_STOP_LONGTERM_SYNC, 0, "", ssn);
			EventByteOutStream out = requestEvent.getOutStream();
			message.encode(out);

			ee_.add(requestEvent);
		}
		catch (std::exception &e) {
			clusterService_->setError(ec, &e);
			GS_RETHROW_SYSTEM_ERROR(
					e, "Cancel sync checkpoint failed.  (reason=" <<
					GS_EXCEPTION_MESSAGE(e) << ")");
		}
	}
};

/*
	@brief リカバリ後チェックポイント実行
	@note チェックポイント完了後、クラスタサービスに通知
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

		CheckpointMainMessage message(CP_AFTER_RECOVERY, 0, "", UNDEF_SYNC_SEQ_NUMBER);
		EventByteOutStream out = requestEvent.getOutStream();
		message.encode(out);

		ee_.add(requestEvent);
	}
	catch (std::exception &e) {
		clusterService_->setError(eventSource, &e);
		GS_RETHROW_SYSTEM_ERROR(
				e, "Recovery checkpoint failed. (reason=" <<
				GS_EXCEPTION_MESSAGE(e) << ")");
	}
}


bool CheckpointService::checkBackupRunnable(BackupContext &cxt) {
	if (lastMode_ == CP_SHUTDOWN) {
		cxt.reason_ = "Shutdown is already in progress";
		cxt.errorNo_ = WEBAPI_CP_SHUTDOWN_IS_ALREADY_IN_PROGRESS;
		cxt.errorInfo_["errorStatus"] =
				picojson::value(static_cast<double>(cxt.errorNo_));
		cxt.errorInfo_["reason"] = picojson::value(cxt.reason_);
		cxt.result_ = picojson::value(cxt.errorInfo_);
		GS_TRACE_ERROR(CHECKPOINT_SERVICE,
				GS_TRACE_CP_CONTROLLER_ILLEAGAL_STATE,
				"Checkpoint cancelled by already requested shutdown ("
				"lastMode=" << checkpointModeToString(lastMode_) <<
				", backupName=" << cxt.backupName_ << ")");
		return false;
	}

	if (requestedShutdownCheckpoint_) {
		cxt.reason_ = "Shutdown is already in progress";
		cxt.errorNo_ = WEBAPI_CP_SHUTDOWN_IS_ALREADY_IN_PROGRESS;
		cxt.errorInfo_["errorStatus"] =
				picojson::value(static_cast<double>(cxt.errorNo_));
		cxt.errorInfo_["reason"] = picojson::value(cxt.reason_);
		cxt.result_ = picojson::value(cxt.errorInfo_);
		GS_TRACE_ERROR(
				CHECKPOINT_SERVICE, GS_TRACE_CP_CONTROLLER_ILLEAGAL_STATE,
				"Checkpoint cancelled by already requested shutdown ("
				"lastMode=" << checkpointModeToString(lastMode_) <<
				", backupName=" << cxt.backupName_ << ")");
		return false;
	}
	return true;
}

bool CheckpointService::checkBackupParams(BackupContext &cxt) {
	if (cxt.mode_ == CP_BACKUP || cxt.mode_ == CP_BACKUP_START) {
		if (cxt.backupName_.empty()) {
			cxt.reason_ = "BackupName is empty";
			cxt.errorNo_ = WEBAPI_CP_BACKUPNAME_IS_EMPTY;
			cxt.errorInfo_["errorStatus"] =
					picojson::value(static_cast<double>(cxt.errorNo_));
			cxt.errorInfo_["reason"] = picojson::value(cxt.reason_);
			cxt.result_ = picojson::value(cxt.errorInfo_);
			GS_TRACE_ERROR(
					CHECKPOINT_SERVICE, GS_TRACE_CP_BACKUP_FAILED,
					"BackupName is empty ("
					"mode=" << checkpointModeToString(cxt.mode_) << ")");
			return false;
		}

		try {
			AlphaOrDigitKey::validate(
				cxt.backupName_.c_str(), static_cast<uint32_t>(cxt.backupName_.size()),
				MAX_BACKUP_NAME_LEN, "backupName");
		} catch (UserException &e) {
			UTIL_TRACE_EXCEPTION(CHECKPOINT_SERVICE, e,
					"BackupName is invalid (name=" << cxt.backupName_ <<
					", mode=" << checkpointModeToString(cxt.mode_) <<
					", reason=" << GS_EXCEPTION_MESSAGE(e) <<")");
			return false;
		}
		try {
			util::FileSystem::createDirectoryTree(backupTopPath_.c_str());
			if (!util::FileSystem::isDirectory(backupTopPath_.c_str())) {
				util::NormalOStringStream oss;
				oss << "Failed to create backup top dir (path="
					<< backupTopPath_ << ")";
				cxt.reason_ = oss.str();
				cxt.errorNo_ = WEBAPI_CP_CREATE_BACKUP_PATH_FAILED;
				cxt.errorInfo_["errorStatus"] =
						picojson::value(static_cast<double>(cxt.errorNo_));
				cxt.errorInfo_["reason"] = picojson::value(cxt.reason_);
				cxt.result_ = picojson::value(cxt.errorInfo_);
				GS_TRACE_ERROR(CHECKPOINT_SERVICE, GS_TRACE_CP_BACKUP_FAILED,
						"Failed to create backup top dir (path=" <<
						backupTopPath_ <<
						", mode=" << checkpointModeToString(cxt.mode_) << ")");
				return false;
			}
		}
		catch (std::exception &e) {
			util::NormalOStringStream oss;
			oss << "Failed to create backup top dir (path=" << backupTopPath_
				<< ")";
			cxt.reason_ = oss.str();
			cxt.errorNo_ = WEBAPI_CP_CREATE_BACKUP_PATH_FAILED;
			cxt.errorInfo_["errorStatus"] =
					picojson::value(static_cast<double>(cxt.errorNo_));
			cxt.errorInfo_["reason"] = picojson::value(cxt.reason_);
			cxt.result_ = picojson::value(cxt.errorInfo_);
			UTIL_TRACE_EXCEPTION(CHECKPOINT_SERVICE, e,
					"Failed to create backup top dir (path=" <<
					backupTopPath_ <<
					", mode=" << checkpointModeToString(cxt.mode_) <<
					", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
			return false;
		}

		if (cxt.option_.isIncrementalBackup_) {
			if (cxt.option_.incrementalBackupLevel_ == 0) {
				cxt.backupPath_.append("_lv0");
			}
			else {
				assert(cxt.option_.incrementalBackupLevel_ == 1);
				uint64_t nextCumulativeCount =
						lastQueuedBackupStatus_.cumulativeCount_;
				uint64_t nextDifferentialCount =
						lastQueuedBackupStatus_.differentialCount_;
				if (cxt.option_.isCumulativeBackup_) {
					++nextCumulativeCount;
					nextDifferentialCount = 0;
				}
				else {
					++nextDifferentialCount;
				}
				if (nextCumulativeCount > INCREMENTAL_BACKUP_COUNT_LIMIT ||
						nextDifferentialCount > INCREMENTAL_BACKUP_COUNT_LIMIT) {
					cxt.errorNo_ = WEBAPI_CP_INCREMENTAL_BACKUP_COUNT_EXCEEDS_LIMIT;
					cxt.errorInfo_["errorStatus"] =
							picojson::value(static_cast<double>(cxt.errorNo_));
					cxt.errorInfo_["reason"] = picojson::value(cxt.reason_);
					cxt.result_ = picojson::value(cxt.errorInfo_);
					GS_TRACE_ERROR(
							CHECKPOINT_SERVICE, GS_TRACE_CP_BACKUP_FAILED,
							"Incremental backup count exceeds limit. (name=" <<
							cxt.backupName_ << ", path=" << cxt.backupPath_ <<
							", mode=" << checkpointModeToString(cxt.mode_) <<
							", nextCumulativeCount=" << nextCumulativeCount <<
							", nextDifferentialCount=" <<
							nextDifferentialCount << ")");
					return false;
				}
				util::NormalOStringStream oss;
				oss << "_lv1_" << std::setfill('0') << std::setw(3)
					<< nextCumulativeCount << "_" << std::setw(3)
					<< nextDifferentialCount;
				cxt.backupPath_.append(oss.str());
			}
		}
		if (util::FileSystem::exists(cxt.backupPath_.c_str())) {
			util::NormalOStringStream oss;
			oss << "Backup name is already used (name=" << cxt.backupName_
				<< ", path=" << cxt.backupPath_ << ")";
			cxt.reason_ = oss.str();
			cxt.errorNo_ = WEBAPI_CP_BACKUPNAME_ALREADY_EXIST;
			cxt.errorInfo_["errorStatus"] =
					picojson::value(static_cast<double>(cxt.errorNo_));
			cxt.errorInfo_["reason"] = picojson::value(cxt.reason_);
			cxt.result_ = picojson::value(cxt.errorInfo_);
			GS_TRACE_ERROR(CHECKPOINT_SERVICE, GS_TRACE_CP_BACKUP_FAILED,
					"Backup name is already used (name=" <<
					cxt.backupName_ << ", path=" << cxt.backupPath_ <<
					", mode=" << checkpointModeToString(cxt.mode_) << ")");
			return false;
		}
		if (cxt.option_.isIncrementalBackup_ && cxt.option_.incrementalBackupLevel_ == 1) {
			u8string checkPath(cxt.origBackupPath_);
			if (lastQueuedBackupStatus_.incrementalLevel_ == 0) {
				checkPath.append("_lv0");
			}
			else {
				util::NormalOStringStream oss;
				oss << "_lv1_" << std::setfill('0') << std::setw(3)
					<< lastQueuedBackupStatus_.cumulativeCount_ << "_"
					<< std::setw(3)
					<< lastQueuedBackupStatus_.differentialCount_;
				checkPath.append(oss.str());
			}
			if (!util::FileSystem::exists(checkPath.c_str())) {
				util::NormalOStringStream oss;
				oss << "Last incremental backup is not found (name="
					<< cxt.backupName_ << ", path=" << checkPath << ")";
				cxt.reason_ = oss.str();
				cxt.errorNo_ = WEBAPI_CP_BACKUPNAME_UNMATCH;
				cxt.errorInfo_["errorStatus"] =
						picojson::value(static_cast<double>(cxt.errorNo_));
				cxt.errorInfo_["reason"] = picojson::value(cxt.reason_);
				cxt.result_ = picojson::value(cxt.errorInfo_);
				GS_TRACE_ERROR(CHECKPOINT_SERVICE, GS_TRACE_CP_BACKUP_FAILED,
						"Last incremental backup directory is not found (name=" <<
						cxt.backupName_ << ", path=" << checkPath <<
						", mode=" << checkpointModeToString(cxt.mode_) << ")");
				return false;
			}
		}
	}
	else {
		assert(cxt.mode_ == CP_BACKUP_END);
		if (!util::FileSystem::exists(cxt.backupPath_.c_str())) {
			util::NormalOStringStream oss;
			oss << "Backup name is not found (name=" << cxt.backupName_ << ")";
			cxt.reason_ = oss.str();
			cxt.errorNo_ = WEBAPI_CP_BACKUPNAME_UNMATCH;
			cxt.errorInfo_["errorStatus"] =
					picojson::value(static_cast<double>(cxt.errorNo_));
			cxt.errorInfo_["reason"] = picojson::value(cxt.reason_);
			cxt.result_ = picojson::value(cxt.errorInfo_);
			GS_TRACE_ERROR(CHECKPOINT_SERVICE, GS_TRACE_CP_BACKUP_FAILED,
					"Backup name is not found (name=" <<
					cxt.backupName_ << ", path=" << cxt.backupPath_ <<
					", mode=" << checkpointModeToString(cxt.mode_) << ")");
			return false;
		}
	}
	return true;
}

bool CheckpointService::checkBackupCondition(BackupContext &cxt) {
	if (cxt.option_.isIncrementalBackup_) {
		switch (cxt.option_.incrementalBackupLevel_) {
		case 0:
			break;
		case 1:
			if (lastQueuedBackupStatus_.incrementalLevel_ == -1) {
				util::NormalOStringStream oss;
				oss << "Last backup is not incremental backup (name="
					<< cxt.backupName_ << ")";
				cxt.reason_ = oss.str();
				cxt.errorNo_ =
						WEBAPI_CP_PREVIOUS_BACKUP_MODE_IS_NOT_INCREMENTAL_MODE;
				cxt.errorInfo_["errorStatus"] =
						picojson::value(static_cast<double>(cxt.errorNo_));
				cxt.errorInfo_["reason"] = picojson::value(cxt.reason_);
				cxt.result_ = picojson::value(cxt.errorInfo_);
				GS_TRACE_ERROR(
						CHECKPOINT_SERVICE, GS_TRACE_CP_BACKUP_FAILED,
						"Previous backup is not incremental backup (name=" <<
						cxt.backupName_ << ", path=" << cxt.backupPath_ <<
						", mode=" << checkpointModeToString(cxt.mode_) << ")");
				return false;
			}
			break;
		default:
			{
				util::NormalOStringStream oss;
				oss << "Unknown incremental backup level: "
					<< cxt.option_.incrementalBackupLevel_ << " (name=" << cxt.backupName_ << ")";
				cxt.reason_ = oss.str();
				cxt.errorNo_ = WEBAPI_CP_INCREMENTAL_BACKUP_LEVEL_IS_INVALID;
				cxt.errorInfo_["errorStatus"] =
						picojson::value(static_cast<double>(cxt.errorNo_));
				cxt.errorInfo_["reason"] = picojson::value(cxt.reason_);
				cxt.result_ = picojson::value(cxt.errorInfo_);
				GS_TRACE_ERROR(
						CHECKPOINT_SERVICE, GS_TRACE_CP_BACKUP_FAILED,
						"Unknown incremental backup level: " <<
						cxt.option_.incrementalBackupLevel_ << " (name=" << cxt.backupName_ <<
						")" <<
						", path=" << cxt.backupPath_ <<
						", mode=" << checkpointModeToString(cxt.mode_) << ")");
				return false;
			}
		}
	}
	return true;
}

bool CheckpointService::prepareBackupDir(BackupContext &cxt) {

	bool makeSubdir = !(
		(cxt.option_.incrementalBackupLevel_ == 1) ||
		(cxt.option_.logDuplicate_ && cxt.option_.skipBaseline_) );

	if (!makeBackupDirectory(cxt.mode_, makeSubdir, cxt.backupPath_)) {
		util::NormalOStringStream oss;
		oss << "Backup directory can't create (name=" << cxt.backupName_
			<< ", path=" << cxt.backupPath_ << ")";
		cxt.reason_ = oss.str();
		cxt.errorNo_ = WEBAPI_CP_CREATE_BACKUP_PATH_FAILED;
		cxt.errorInfo_["errorStatus"] =
				picojson::value(static_cast<double>(cxt.errorNo_));
		cxt.errorInfo_["reason"] = picojson::value(cxt.reason_);
		cxt.result_ = picojson::value(cxt.errorInfo_);

		GS_TRACE_ERROR(CHECKPOINT_SERVICE, GS_TRACE_CP_BACKUP_FAILED,
				"Backup directory can't create (name=" <<
				cxt.backupName_ << ", path=" << cxt.backupPath_ <<
				", mode=" << checkpointModeToString(cxt.mode_) << ")");
		return false;
	}
	return true;
}

bool CheckpointService::prepareExecuteBackup(BackupContext &cxt) {

		GS_TRACE_INFO(
				CHECKPOINT_SERVICE, GS_TRACE_CP_STATUS,
				"[OnlineBackup] requested (name=" <<
				cxt.backupName_ << ", path=" << cxt.backupPath_ <<
				", mode=" << checkpointModeToString(cxt.mode_) <<
				", logArchive=" << (cxt.option_.logArchive_ ? "true" : "false") <<
				", logDuplicate=" << (cxt.option_.logDuplicate_ ? "true" : "false") <<
				", isIncrementalBackup:" << (cxt.option_.isIncrementalBackup_ ? 1 : 0) <<
				", incrementalBackupLevel:" << cxt.option_.incrementalBackupLevel_ <<
				", isCumulativeBackup:" << (cxt.option_.isCumulativeBackup_ ? 1 : 0) <<
				")");

		switch (cxt.mode_) {
		case CP_BACKUP:
		case CP_BACKUP_START:
		case CP_BACKUP_END:
			lastQueuedBackupStatus_.name_ = cxt.backupName_;
			if (cxt.option_.isIncrementalBackup_) {
				lastQueuedBackupStatus_.archiveLogMode_ = false;
				lastQueuedBackupStatus_.duplicateLogMode_ = false;
				if (cxt.option_.incrementalBackupLevel_ == 0) {
					cxt.mode_ = CP_INCREMENTAL_BACKUP_LEVEL_0;
					lastQueuedBackupStatus_.incrementalLevel_ = 0;
					lastQueuedBackupStatus_.cumulativeCount_ = 0;
					lastQueuedBackupStatus_.differentialCount_ = 0;
				}
				else {
					assert(cxt.option_.incrementalBackupLevel_ == 1);
					lastQueuedBackupStatus_.incrementalLevel_ = 1;
					if (cxt.option_.isCumulativeBackup_) {
						cxt.mode_ = CP_INCREMENTAL_BACKUP_LEVEL_1_CUMULATIVE;
						++lastQueuedBackupStatus_.cumulativeCount_;
						lastQueuedBackupStatus_.differentialCount_ = 0;
					}
					else {
						cxt.mode_ = CP_INCREMENTAL_BACKUP_LEVEL_1_DIFFERENTIAL;
						++lastQueuedBackupStatus_.differentialCount_;
					}
				}
			}
			else {
				lastQueuedBackupStatus_.archiveLogMode_ = cxt.option_.logArchive_;
				lastQueuedBackupStatus_.duplicateLogMode_ = cxt.option_.logDuplicate_;
				lastQueuedBackupStatus_.incrementalLevel_ = -1;
				lastQueuedBackupStatus_.cumulativeCount_ = 0;
				lastQueuedBackupStatus_.differentialCount_ = 0;
			}
			break;
		default: {
			assert(false);
			cxt.reason_ = "Unknown mode";
			cxt.errorNo_ = WEBAPI_CP_OTHER_REASON;
			cxt.errorInfo_["errorStatus"] =
					picojson::value(static_cast<double>(cxt.errorNo_));
			cxt.errorInfo_["reason"] = picojson::value(cxt.reason_);
			cxt.result_ = picojson::value(cxt.errorInfo_);

			GS_TRACE_ERROR(
					CHECKPOINT_SERVICE, GS_TRACE_CP_BACKUP_FAILED,
					"Unacceptable mode");
			return false;
		}
		} 
		return true;
}

/*!
	@brief オンラインバックアップ実行要求
*/
bool CheckpointService::requestOnlineBackup(
		const Event::Source &eventSource,
		const std::string &backupName, int32_t mode,
		const SystemService::BackupOption &option,
		picojson::value &result) {
	picojson::object errorInfo;
	int32_t errorNo = 0;
	std::string reason;

	BackupContext cxt(
			backupName, mode, option, result,
			errorNo, reason, errorInfo);

	if (!checkBackupRunnable(cxt)) {
		return false;
	}
	util::FileSystem::createPath(
		backupTopPath_.c_str(), backupName.c_str(), cxt.backupPath_);
	cxt.origBackupPath_ = cxt.backupPath_;

	if (!checkBackupParams(cxt)) {
		return false;
	}
	if (!checkBackupCondition(cxt)) {
		return false;
	}
	if (!prepareBackupDir(cxt)) {
		return false;
	}
	try {
		if (!prepareExecuteBackup(cxt)) {
			return false;
		}
		uint32_t flag = 0;
		flag |= (cxt.option_.logArchive_ ? BACKUP_ARCHIVE_LOG_MODE_FLAG : 0);
		flag |= (cxt.option_.logDuplicate_ ? BACKUP_DUPLICATE_LOG_MODE_FLAG : 0);
		flag |= (cxt.option_.stopOnDuplicateError_ ? BACKUP_STOP_ON_DUPLICATE_ERROR_FLAG : 0);

		if (cxt.option_.logDuplicate_) {
			flag |= (cxt.option_.skipBaseline_ ? BACKUP_SKIP_BASELINE_MODE_FLAG : 0);
		}
		Event requestEvent(
				eventSource, CP_REQUEST_CHECKPOINT, CP_HANDLER_PARTITION_ID);

		CheckpointMainMessage message(cxt.mode_, flag, cxt.backupPath_, UNDEF_SYNC_SEQ_NUMBER);
		EventByteOutStream out = requestEvent.getOutStream();
		message.encode(out);

		ee_.add(requestEvent);
		return true;
	}
	catch (std::exception &e) {
		clusterService_->setError(eventSource, &e);
		GS_RETHROW_SYSTEM_ERROR(
				e, "Request online backup failed.  (reason=" <<
				GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief チェックポイントサービスハンドラ初期化
	@note ハンドラ内で利用する他サービスのアドレスをセットする
*/
void CheckpointHandler::initialize(const ManagerSet &resourceSet) {
	try {
		checkpointService_ = resourceSet.cpSvc_;
		clusterService_ = resourceSet.clsSvc_;
		transactionService_ = resourceSet.txnSvc_;
		transactionManager_ = resourceSet.txnMgr_;
		partitionList_ = resourceSet.partitionList_;
		fixedSizeAlloc_ = resourceSet.fixedSizeAlloc_;
		config_ = resourceSet.config_;
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(
				e, "Initialize failed. (reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

const char8_t *CheckpointService::checkpointModeToString(int32_t mode) {
	return CheckpointTypes::CP_MODE_CODER(
			static_cast<CheckpointMode>(mode), "UNKNOWN");
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

void CheckpointService::removeCpLongtermSyncInfoAll() {
	util::LockGuard<util::Mutex> guard(cpLongtermSyncMutex_);
	for (auto it = cpLongtermSyncInfoMap_.begin(); it != cpLongtermSyncInfoMap_.end(); ) {
		delete (*it).second.syncCpFile_;
		it = cpLongtermSyncInfoMap_.erase(it); 
	}
}

void CheckpointService::checkSystemError() {
	if (clusterService_->isError()) {
		GS_THROW_USER_ERROR(
				GS_ERROR_CP_CONTROLLER_ILLEAGAL_STATE,
				"Checkpoint cancelled ("
				"systemErrorOccurred=" <<
				clusterService_->isError() << ")");
	}
}

uint64_t CheckpointService::backupLog(
		uint32_t flag, PartitionId pId, const std::string &backupPath) {
	return 0;
}


/*!
	@brief 定期的なログファイルのフラッシュ処理
	@note CPServiceのスレッドで実行
*/
void FlushLogPeriodicallyHandler::operator()(EventContext &ec, Event &ev) {
	try {
		const uint64_t startClock = util::Stopwatch::currentClock();
		const uint32_t partitionCount =
				checkpointService_->getPGConfig().getPartitionCount();
		const bool byCP = true;


		for (uint32_t pId = 0; pId < partitionCount; ++pId) {
			PartitionLock pLock(*transactionManager_, pId);

			Partition &partition = partitionList_->partition(pId);
			if (partition.isActive()
					&& partition.logManager().checkNeedFlushXLog()) {
				{
					util::LockGuard<util::Mutex> guard(partition.mutex());
					if (partition.isActive())  {
						partition.logManager().flushXLog(LOG_WRITE_WAL_BUFFER, byCP);
					}
				}
				if (partition.isActive())  {
					partition.logManager().flushXLog(LOG_FLUSH_FILE, byCP);
				}
			}
		}
		const uint32_t lap = util::Stopwatch::clockToMillis(
			util::Stopwatch::currentClock() - startClock);
		GS_TRACE_DEBUG(
				CHECKPOINT_SERVICE_DETAIL, GS_TRACE_CP_FLUSH_LOG,
				"FlushLogPeriodicallyHandler: elapsedMillis," << lap);
	}
	catch (std::exception &e) {
		clusterService_->setError(ec, &e);
		GS_RETHROW_SYSTEM_ERROR(
				e, "Flush log file failed. (reason=" <<
				GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

bool CheckpointService::makeBackupDirectory(
		int32_t mode, bool makeSubDir,
		const std::string &backupPath) {
	if (mode == CP_BACKUP || mode == CP_BACKUP_START) {
		try {
			util::FileSystem::createDirectoryTree(backupPath.c_str());
		}
		catch (std::exception &) {
			return false;
		}
	}
	return true;
}

/*!
	@brief パーティションロック取得 (シンクサービスとの排他用)
*/
PartitionLock::PartitionLock(
		TransactionManager &txnManager, PartitionId pId)
		: transactionManager_(txnManager), pId_(pId) {
	while (!transactionManager_.lockPartition(pId_)) {
		util::Thread::sleep(100);
	}
}

/*!
	@brief パーティションロック解除
*/
PartitionLock::~PartitionLock() {
	transactionManager_.unlockPartition(pId_);
}

/*!
	@brief コンストラクタ
*/
CheckpointService::PIdLsnInfo::PIdLsnInfo()
		: checkpointService_(NULL), partitionNum_(0), partitionGroupNum_(0) {}

/*!
	@brief デストラクタ
*/
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

void CheckpointService::PIdLsnInfo::endCheckpoint(const char* backupPath) {
	writeFile(backupPath);
}

void CheckpointService::PIdLsnInfo::setLsn(
		PartitionId pId, LogSequentialNumber lsn) {
	lsnList_[pId] = lsn;
}

/*!
	@brief PID-LSN情報ファイル書き込み
	@note CPスレッドからの呼び出し、並列に呼び出される可能性有り

 gs_lsn_info.json
{
	"nodeInfo":{
		"address":"1aa.bb.cc.dd",
		"port":10010 (CLUSTER_SERVICE, /node/partitionと同じ)
		"partitionNum":xxx,
	"concurrency":yyy
	},
	"lsnInfo":[111111,222222,0,33,0,444.....,99999], 
}
※LSNは、オーナでもバックアップでもないPartitionは0とする
*/
void CheckpointService::PIdLsnInfo::writeFile(const char8_t *backupPath) {
	util::LockGuard<util::Mutex> guard(mutex_);
	std::string lsnInfoFileName;
	util::NamedFile file;
	try {
		picojson::object jsonNodeInfo;
		NodeAddress &address = checkpointService_->partitionTable_->getNodeAddress(0);
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

		if (backupPath && strcmp(backupPath, "") != 0) {
			util::FileSystem::createPath(
					backupPath, PID_LSN_INFO_FILE_NAME.c_str(), lsnInfoFileName);
		}
		else {
			util::FileSystem::createPath(
					path_.c_str(), PID_LSN_INFO_FILE_NAME.c_str(), lsnInfoFileName);
		}
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

bool CheckpointService::getLongSyncLog(
		SyncSequentialNumber ssn,
		LogSequentialNumber startLsn, LogIterator<MutexLocker> &itr) {

	assert(ssn != UNDEF_SYNC_SEQ_NUMBER);
	CpLongtermSyncInfo *info = getCpLongtermSyncInfo(ssn);
	if (info) {
		LogManager<MutexLocker>& logManager =
				partitionList_->partition(info->targetPId_).logManager();
		try {
			itr = logManager.createXLogIterator(startLsn);
			return itr.checkExists(startLsn);
		}
		catch(std::exception &e) {
			info->errorOccured_ = true;
			GS_THROW_USER_ERROR(
					GS_ERROR_CP_LOG_FILE_WRITE_FAILED,
					"getLongSyncLog failed (reason=" << e.what() << ")");
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
		SyncSequentialNumber ssn, LogIterator<MutexLocker> &itr) {

	assert(ssn != UNDEF_SYNC_SEQ_NUMBER);
	CpLongtermSyncInfo *info = getCpLongtermSyncInfo(ssn);
	if (info) {
		try {
			LogManager<MutexLocker>& logManager =
					partitionList_->partition(info->targetPId_).logManager();

			itr = logManager.createLogIterator(info->logVersion_);
			return true;
		}
		catch(std::exception &e) {
			info->errorOccured_ = true;
			GS_THROW_USER_ERROR(
					GS_ERROR_CP_LOG_FILE_WRITE_FAILED,
					"getLongSyncLog failed (reason=" << e.what() << ")");
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
		if (info->syncCpFile_) {
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

bool CheckpointService::getLongSyncChunk(
		SyncSequentialNumber ssn, uint64_t size, uint8_t* buffer,
		uint64_t &readSize, uint64_t &totalCount,
		uint64_t &completedCount, uint64_t &readyCount) {

	readSize = 0;
	totalCount = 0;
	completedCount = 0;
	readyCount = 0;

	if (ssn != UNDEF_SYNC_SEQ_NUMBER) {
		CpLongtermSyncInfo *info = getCpLongtermSyncInfo(ssn);
		if (info && info->atomicStopFlag_ == 1) {
			GS_THROW_USER_ERROR(
					GS_ERROR_CP_LONGTERM_SYNC_FAILED,
					"LongtermSync cancelled: ssn=" << ssn);
		}
		if (info) {
			totalCount = info->totalChunkCount_;
			completedCount = info->readCount_;
			readyCount = info->readyCount_;
			if (info->readCount_ >= info->totalChunkCount_) {
				GS_TRACE_INFO(
						CHECKPOINT_SERVICE_STATUS_DETAIL, GS_TRACE_CP_STATUS,
						"[CP_GET_LONGTERM_SYNC_CHUNK_END]: totalCount," << totalCount <<
						",completedCount," << completedCount <<
						",readyCount," << readyCount);
				return true;
			}
			if (info->readCount_ >= info->readyCount_) {
				GS_TRACE_DEBUG(
						CHECKPOINT_SERVICE_STATUS_DETAIL, GS_TRACE_CP_STATUS,
						"[CP_GET_LONGTERM_SYNC_CHUNK_AT_TAIL]: totalCount," << totalCount <<
						",completedCount," << completedCount <<
						",readyCount," << readyCount);
				return false;
			}
			assert(info->chunkSize_ > 0);
			const uint32_t chunkSize = info->chunkSize_;
			if (size < chunkSize) {
				GS_THROW_USER_ERROR(
						GS_ERROR_CHM_GET_CHECKPOINT_CHUNK_FAILED,
						"Invalid size (specified size=" << size <<
						", chunkSize=" << chunkSize << ")");
			}
			std::unique_ptr<NoLocker> noLocker(UTIL_NEW NoLocker());
			StatStopwatch::NonStat ioWatch;
			uint32_t remain = size / chunkSize;
			size_t fileRemain = info->readyCount_ - info->readCount_;
			uint8_t *addr = buffer;
			for(uint32_t chunkCount = 0; remain > 0 && fileRemain > 0; ++chunkCount) {
				info->syncCpFile_->seekAndRead(
						info->readCount_, addr, chunkSize, ioWatch.get());
				addr += chunkSize;
				readSize += chunkSize;
				++info->readCount_;
				--remain;
				--fileRemain;
			}
			completedCount = info->readCount_;
			readyCount = info->readyCount_;

			GS_TRACE_DEBUG(
					CHECKPOINT_SERVICE_STATUS_DETAIL, GS_TRACE_CP_STATUS,
					"[CP_GET_LONGTERM_SYNC_CHUNK]: totalCount," << totalCount <<
					",completedCount," << completedCount <<
					",readyCount," << readyCount);
			return (info->readCount_ == info->totalChunkCount_);
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

CheckpointService::ConfigSetUpHandler CheckpointService::configSetUpHandler_;

void CheckpointService::ConfigSetUpHandler::operator()(ConfigTable &config) {
	CONFIG_TABLE_RESOLVE_GROUP(config, CONFIG_TABLE_CP, "checkpoint");

	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_CP_CHECKPOINT_INTERVAL, INT32)
			.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
			.setMin(1)
			.setDefault(300);

	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_CP_CHECKPOINT_COPY_INTERVAL_MILLIS, INT32)
			.deprecate();
	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_CP_CHECKPOINT_COPY_INTERVAL, INT32)
			.setUnit(ConfigTable::VALUE_UNIT_DURATION_MS)
			.setMin(0)
			.setDefault(100);
	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_CP_PARTIAL_CHECKPOINT_INTERVAL, INT32)
			.setMin(1)
			.setMax(1024)
			.setDefault(5);
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
		info.chunkSize_ = partitionList_->partition(pId).chunkManager().getChunkSize();
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
		removeCpLongtermSyncInfoAll();
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

		CheckpointMainMessage message(CP_PREPARE_LONGTERM_SYNC, 0, syncTempPath, ssn);
		EventByteOutStream out = requestEvent.getOutStream();
		message.encode(out);

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
			longSyncInfo->atomicStopFlag_ = 1; 
			GS_TRACE_INFO(
					CHECKPOINT_SERVICE, GS_TRACE_CP_STATUS,
					"[StopCpLongtermSync] requested (SSN=" << ssn << ")");

			Event requestEvent(
					eventSource, CP_REQUEST_CHECKPOINT, CP_HANDLER_PARTITION_ID);

			CheckpointMainMessage message(CP_STOP_LONGTERM_SYNC, 0, "", ssn);
			EventByteOutStream out = requestEvent.getOutStream();
			message.encode(out);

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

void CheckpointService::setPeriodicCheckpointFlag(bool flag) {
	enablePeriodicCheckpoint_ = flag;
	stats(CheckpointStats::CP_STAT_PERIODIC).set(flag ?
			CheckpointTypes::PERIODIC_ACTIVE :
			CheckpointTypes::PERIODIC_INACTIVE);
}

bool CheckpointService::getPeriodicCheckpointFlag() {
	return enablePeriodicCheckpoint_;
}

void CheckpointService::setLastMode(int32_t mode) {
	lastMode_ = mode;
	stats(CheckpointStats::CP_STAT_MODE).set(static_cast<uint64_t>(mode));
}

inline LocalStatValue& CheckpointService::stats(CheckpointStats::Param param) {
	return stats()(param);
}

inline const CheckpointStats::Table& CheckpointService::stats() const {
	return statUpdator_.stats_.table_;
}

inline CheckpointStats::Table& CheckpointService::stats() {
	return statUpdator_.stats_.table_;
}


CheckpointService::CheckpointDataCleaner::CheckpointDataCleaner(
		CheckpointService &service, EventContext &ec,
		const bool &backupCommandWorking, const std::string &backupPath) :
		service_(service),
		ec_(ec),
		backupCommandWorking_(backupCommandWorking),
		backupPath_(backupPath),
		workerId_(ec.getWorkerId()) {
}

CheckpointService::CheckpointDataCleaner::~CheckpointDataCleaner() {
	if (workerId_ != 0) {
		return;
	}

	if (backupCommandWorking_) {
		try {
			util::FileSystem::remove(backupPath_.c_str());
		}
		catch (...) {
		}
	}

	CheckpointStats::Table &stats = service_.stats();
	stats(CheckpointStats::CP_STAT_END_TIME).set(
			static_cast<uint64_t>(util::DateTime::now(false).getUnixTime()));
	stats(CheckpointStats::CP_STAT_PENDING_PARTITION_COUNT).set(0);
}

