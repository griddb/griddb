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
	@brief Implementation of SyncManager
*/

#include "sync_manager.h"
#include "util/trace.h"
#include "gs_error.h"
#include <iostream>
#include "checkpoint_service.h"
#include "sync_service.h"
#include "transaction_service.h"
#include "partition_table.h"
#include "cluster_manager.h"

UTIL_TRACER_DECLARE(SYNC_SERVICE);
UTIL_TRACER_DECLARE(SYNC_DETAIL);

#define RM_THROW_LOG_REDO_ERROR(errorCode, message) \
	GS_THROW_CUSTOM_ERROR(LogRedoException, , errorCode, message)
#define RM_RETHROW_LOG_REDO_ERROR(cause, message) \
	GS_RETHROW_CUSTOM_ERROR(LogRedoException, GS_ERROR_DEFAULT, cause, message)

const int32_t SyncManager::DEFAULT_LOG_SYNC_MESSAGE_MAX_SIZE = 2;
const int32_t SyncManager::DEFAULT_CHUNK_SYNC_MESSAGE_MAX_SIZE = 2;

int32_t errorCounter3 = 0;
int32_t errorMax3 = 5;
#define RAISE_EXCEPTION3() \
if (errorCounter3 < errorMax3) { \
std::cout << "exception" << errorCounter3 << std::endl; \
errorCounter3++; \
GS_THROW_USER_ERROR(0, ""); \
} \

/*!
	@brief SyncManager
*/
/*!
	@brief 同期管理
	@param [in] configTable コンフィグ
	@param [in] pt パーティションテーブル
	@return なし
	@note 短期/長期同期共通
*/
SyncManager::SyncManager(const ConfigTable& configTable, PartitionTable* pt) : syncOptStat_(pt->getPartitionNum()),
	fixedSizeAlloc_(util::AllocatorInfo(ALLOCATOR_GROUP_CS, "syncManagerFixed"), 1 << 18),
	globalStackAlloc_(util::AllocatorInfo(ALLOCATOR_GROUP_CS, "syncManagerStack"), &fixedSizeAlloc_),
	varSizeAlloc_(util::AllocatorInfo(ALLOCATOR_GROUP_CS, "syncManagerVar")),
	pt_(pt), syncConfig_(configTable), extraConfig_(configTable), syncSequentialNumber_(0), partitionList_(NULL),
	cpSvc_(NULL), syncSvc_(NULL), txnSvc_(NULL), txnMgr_(NULL), clsSvc_(NULL), clsMgr_(NULL), recoveryMgr_(NULL),
	keepLogMgr_(NULL), syncEntries_(pt->getPartitionNum()){
	try {
		const uint32_t partitionNum = configTable.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM);
		if (partitionNum <= 0 || partitionNum != pt->getPartitionNum()) {
			GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_INFO, "");
		}
		for (PartitionId pId = 0; pId < partitionNum; pId++) {
			syncContextTables_.push_back(ALLOC_NEW(globalStackAlloc_)
				SyncContextTable(globalStackAlloc_, pId, DEFAULT_CONTEXT_SLOT_NUM, &varSizeAlloc_));
		}
		chunkSize_ = configTable.getUInt32(CONFIG_TABLE_DS_STORE_BLOCK_SIZE);
		chunkBufferList_.assign(chunkSize_ * configTable.getUInt32(CONFIG_TABLE_DS_CONCURRENCY), 0);
		ConfigTable* tmpTable= const_cast<ConfigTable*>(&configTable);
		config_.setUpConfigHandler(this, *tmpTable);
		undoPartitionList_.assign(partitionNum, 0);
	}
	catch (std::exception& e) {
		clear();
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief ~SyncManager
*/
/*!
	@brief デストラクタ
	@return なし
*/
SyncManager::~SyncManager() {
	clear();
}

/*!
	@brief clear
*/
/*!
	@brief クリア
	@return なし
*/
void SyncManager::clear() {
	for (size_t i = 0; i < syncContextTables_.size(); i++) {
		ALLOC_DELETE(globalStackAlloc_, syncContextTables_[i]);
	}
}

/*!
	@brief initialize
*/
/*!
	@brief 初期化
	@param [in] mgrSet 管理情報
	@return なし
*/
void SyncManager::initialize(ManagerSet* mgrSet) {
	cpSvc_ = mgrSet->cpSvc_;
	syncSvc_ = mgrSet->syncSvc_;
	txnSvc_ = mgrSet->txnSvc_;
	txnMgr_ = mgrSet->txnMgr_;
	clsMgr_ = mgrSet->clsMgr_;
	recoveryMgr_ = mgrSet->recoveryMgr_;
	partitionList_ = mgrSet->partitionList_;
	switch (syncConfig_.getSyncPolicy()) {
	case 0:
		break;
	case 1:
		pt_->getConfig().setLimitOwnerBackupLsnGap(OWNER_BACKUP_GAP_CLASSIC);
		pt_->setImmediateCheck(false);
		syncConfig_.setParallelSync(false);
		syncConfig_.setLogInterruptionInterval(-1);
		break;
	case 2:
		pt_->getStableGoal().setEnable(true);
		pt_->getStableGoal().setInitialGenerate(true);
		pt_->getConfig().setLimitOwnerBackupLsnGap(OWNER_BACKUP_GAP_LARGE);
		syncConfig_.setParallelSync(true);
		syncConfig_.setKeepLogInterval(60 * 60);
		syncConfig_.setKeepLogLsnDifference(10000000);
		syncConfig_.setParallelSyncNum(-1);
		syncConfig_.setParallelChunkSyncNum(1);
		break;
	case 3:
		pt_->getStableGoal().setEnable(true);
		pt_->getStableGoal().setInitialGenerate(true);
		pt_->getStableGoal().setPolicy(1);
		pt_->getConfig().setLimitOwnerBackupLsnGap(OWNER_BACKUP_GAP_LARGE);
		syncConfig_.setParallelSync(true);
		syncConfig_.setKeepLogInterval(60 * 60);
		syncConfig_.setKeepLogLsnDifference(10000000);
		syncConfig_.setParallelSyncNum(-1);
		syncConfig_.setParallelChunkSyncNum(1);
		break;
	case 4:
		pt_->getStableGoal().setEnable(true);
		pt_->getStableGoal().setInitialGenerate(true);
		pt_->getStableGoal().setPolicy(1);
		pt_->getConfig().setLimitOwnerBackupLsnGap(OWNER_BACKUP_GAP_LARGE);
		syncConfig_.setParallelSync(true);
		syncConfig_.setKeepLogInterval(60 * 60);
		syncConfig_.setKeepLogLsnDifference(10000000);
		syncConfig_.setParallelSyncNum(-1);
		syncConfig_.setParallelChunkSyncNum(1);
		break;
	default:
		break;
	}

	adjustParallelSyncParam(
		syncConfig_.enableParallelSync(), syncConfig_.getParallelSyncNum(), syncConfig_.getParallelChunkSyncNum());
}

/*!
	@brief createSyncContext
*/
/*!
	@brief 同期用コンテキスト生成
	@param [in] ec イベントコンテキスト
	@param [in] pId パーティションID
	@param [in] role ロール
	@param [in] syncMode 同期モード
	@param [in] roleStatus ロール
	@return SyncContext 同期コンテキスト
	@note 短期/長期同期共通、オーナ/キャッチアップ共通
*/
SyncContext* SyncManager::createSyncContext(EventContext& ec, PartitionId pId, PartitionRole& role,
	SyncMode syncMode, PartitionRoleStatus roleStatus) {
	try {
		if (pId >= pt_->getPartitionNum()) {
			GS_THROW_USER_ERROR(GS_ERROR_SYNC_INVALID_CONTEXT, "");
		}
		PartitionRevision& ptRev = role.getRevision();
		util::LockGuard<util::WriteLock> guard(getAllocLock());
		createPartition(pId);
		getSyncOptStat()->setContext(pId);
		SyncContext* context = syncContextTables_[pId]->createSyncContext(ptRev);
		context->setSyncMode(syncMode, roleStatus);
		context->setPartitionTable(pt_);
		bool isOwner = (roleStatus == PartitionTable::PT_OWNER);
		bool longtermSyncCheck = (syncMode == MODE_LONGTERM_SYNC && isOwner);
		checkCurrentContext(ec, pId, ptRev);
		context->setSequentialNumber(syncSequentialNumber_);
		syncSequentialNumber_++;
		if (syncMode == MODE_LONGTERM_SYNC) {
			setCurrentSyncId(pId, context);
		}
		if (roleStatus == PartitionTable::PT_OWNER) {
			int64_t keepLogVersion = partitionList_->partition(pId).logManager().getStartLogVersion();
			if (syncMode == MODE_LONGTERM_SYNC) {
				partitionList_->partition(pId).logManager().setKeepXLogVersion(keepLogVersion);
				context->setKeepLogVersion(keepLogVersion);
			}
		}
		if (longtermSyncCheck) {
			LongtermSyncInfo syncInfo(context->getId(), context->getVersion(), context->getSequentialNumber());
			cpSvc_->requestStartCheckpointForLongtermSyncPre(ec, pId, &syncInfo);
		}
		return context;
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief getSyncContext
*/
/*!
	@brief 同期用コンテキスト取得
	@param [in] pId パーティションID
	@param [in] syncId 同期ID
	@param [in] withLock ロック付き取得
	@return SyncContext 同期コンテキスト
	@note
*/
SyncContext* SyncManager::getSyncContext(PartitionId pId, SyncId& syncId, bool withLock) {
	if (withLock) {
		util::LockGuard<util::ReadLock> guard(getAllocLock());
		return getSyncContextInternal(pId, syncId);
	}
	else {
		return getSyncContextInternal(pId, syncId);
	}
}

/*!
	@brief getSyncContextInternal
*/
/*!
	@brief 同期用コンテキスト取得(内部)
	@param [in] pId パーティションID
	@param [in] syncId 同期ID
	@return SyncContext 同期コンテキスト
	@note
*/
SyncContext* SyncManager::getSyncContextInternal(PartitionId pId, SyncId& syncId) {
	try {
		if (!syncId.isValid()) {
			return NULL;
		}
		if (pId >= pt_->getPartitionNum()) {
			GS_THROW_USER_ERROR(GS_ERROR_SYNC_INVALID_CONTEXT, "");
		}
		if (syncContextTables_[pId] == NULL || !syncId.isValid()) {
			return NULL;
		}
		return syncContextTables_[pId]->getSyncContext(syncId.contextId_, syncId.contextVersion_);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief removeSyncContext
*/
/*!
	@brief 同期用コンテキスト取得(内部)
	@param [in] ec パーティションID
	@param [in] pId 同期ID
	@param [in] context 同期コンテキスト
	@param [in] isFailed エラーフラグ
	@return なし
	@note
*/
void SyncManager::removeSyncContext(EventContext& ec, PartitionId pId, SyncContext*& context, bool isFailed) {
	try {
		if (pId >= pt_->getPartitionNum()) {
			GS_THROW_USER_ERROR(GS_ERROR_SYNC_INVALID_CONTEXT, "");
		}
		if (context == NULL) {
			GS_THROW_USER_ERROR(GS_ERROR_SYNC_INVALID_CONTEXT, "");
		}
		util::LockGuard<util::WriteLock> guard(getAllocLock());
		if (syncContextTables_[pId] == NULL) {
			return;
		}
		if (pId != context->getPartitionId()) {
			return;
		}
		getSyncOptStat()->freeContext(pId);
		bool longtermSyncCheck = (context->getSyncMode() == MODE_LONGTERM_SYNC
			&& context->getPartitionRoleStatus() == PartitionTable::PT_OWNER);
		if (longtermSyncCheck) {
			cpSvc_->requestStopCheckpointForLongtermSync(ec, pId, context->getSequentialNumber());
		}
		if (context->isExecuteCheckpoint() && context->getPartitionRoleStatus() != PartitionTable::PT_OWNER) {
			cpSvc_->cancelSyncCheckpoint(ec, pId, context->getSequentialNumber());
		}
		if (context->getSyncMode() == MODE_LONGTERM_SYNC) {
			resetCurrentSyncId(context);
		}
		if (context->getSyncMode() == MODE_SHORTTERM_SYNC && context->isUseLongSyncLog()) {
			SyncId longSyncId;
			context->getLongSyncId(longSyncId);
			SyncContext* longSyncContext = syncContextTables_[pId]->getSyncContext(
				longSyncId.contextId_, longSyncId.contextVersion_);
			if (longSyncContext) {
				Event syncCheckEndEvent(ec, TXN_SYNC_TIMEOUT, pId);
				EventByteOutStream out = syncCheckEndEvent.getOutStream();
				SyncCheckEndInfo syncCheckEndInfo(ec.getAllocator(), TXN_SYNC_TIMEOUT,
					this, pId, MODE_LONGTERM_SYNC, longSyncContext);
				syncSvc_->encode(syncCheckEndEvent, syncCheckEndInfo, out);
				longSyncContext->setCatchupSync(false);
				txnSvc_->getEE()->addTimer(syncCheckEndEvent, 0);
			}
		}
		context->endCheck();
		if (isFailed) {
			GS_TRACE_SYNC(context, "SYNC_END", ", " << context->dump(2));
		}
		else if (context->checkTotalTime() || context->isDump()) {
			GS_TRACE_SYNC(context, "SYNC_END", ", " << context->dump(2));
		}
		else {
			GS_TRACE_INFO(SYNC_DETAIL, GS_TRACE_SYNC_OPERATION, "SYNC_END, " << context->dump(2));
		}
		syncContextTables_[pId]->removeSyncContext(varSizeAlloc_, context, getSyncOptStat());
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief setShorttermSyncRequest
*/
/*!
	@brief 同期用コンテキスト取得(内部)
	@param [in] ec パーティションID
	@param [in] pId 同期ID
	@param [in] context 同期コンテキスト
	@param [in] isFailed エラーフラグ
	@return なし
	@note
*/
void SyncManager::setShorttermSyncRequest(SyncContext* context, SyncRequestInfo& syncRequestInfo,
	util::Set<NodeId>& syncTargetNodeSet) {

	PartitionRole& nextRole = syncRequestInfo.getPartitionRole();
	PartitionId pId = context->getPartitionId();
	util::StackAllocator& alloc = syncRequestInfo.getAllocator();

	if (!pt_->isOwner(pId, SELF_NODEID) && nextRole.isOwner(SELF_NODEID)) {
		txnSvc_->checkNoExpireTransaction(alloc, pId);
	}
	pt_->setPartitionRole(pId, nextRole);
	context->setPartitionRoleStatus(pt_->getPartitionRoleStatus(pId));
	context->setOwnerLsn(pt_->getLSN(pId));
	SyncId dummySyncId;

	syncRequestInfo.set(TXN_SHORTTERM_SYNC_START, context, pt_->getLSN(pId), dummySyncId);
	nextRole.getShortTermSyncTargetNodeId(syncTargetNodeSet);
}

void SyncManager::setShorttermSyncStart(
	SyncContext* context,
	SyncRequestInfo& syncRequestInfo,
	NodeId senderNodeId) {

	PartitionId pId = context->getPartitionId();
	util::StackAllocator& alloc
		= syncRequestInfo.getAllocator();
	PartitionRole& nextRole
		= syncRequestInfo.getPartitionRole();
	if (!pt_->isOwner(pId, SELF_NODEID)
		&& nextRole.isOwner(SELF_NODEID)) {
		txnSvc_->checkNoExpireTransaction(alloc, pId);
	}
	pt_->setPartitionRole(
		context->getPartitionId(), nextRole);
	context->setPartitionRoleStatus(
		pt_->getPartitionRoleStatus(pId));
	context->setRecvNodeId(senderNodeId);
	context->setOwnerLsn(syncRequestInfo.getLsn());
}

int64_t SyncManager::getTargetSSN(PartitionId pId, SyncContext* context) {
	int64_t ssn = -1;
	if (context->isUseLongSyncLog()) {
		SyncId longSyncId;
		context->getLongSyncId(longSyncId);
		SyncContext* longContext = getSyncContext(pId, longSyncId);
		if (longContext) {
			ssn = longContext->getSequentialNumber();
		}
	}
	else {
		ssn = context->getSequentialNumber();
	}
	return ssn;
}

void SyncManager::checkShorttermGetSyncLog(SyncContext* context, SyncRequestInfo& syncRequestInfo) {
	PartitionId pId = context->getPartitionId();
	util::StackAllocator& alloc = syncRequestInfo.getAllocator();
	context->resetCounter();
	util::XArray<NodeId> backups(alloc);
	context->getSyncTargetNodeIds(backups);
	size_t pos = 0;
	LogSequentialNumber ownerLsn = pt_->getLSN(pId);
	LogSequentialNumber maxLsn = pt_->getMaxLsn(pId);
	for (pos = 0; pos < backups.size(); pos++) {
		LogSequentialNumber backupLsn = 0;
		SyncId backupSyncId;
		NodeId targetNodeId = backups[pos];
		if (!context->getSyncTargetLsnWithSyncId(targetNodeId, backupLsn, backupSyncId)) {
			GS_THROW_SYNC_ERROR(GS_ERROR_SYM_INVALID_PARTITION_REVISION, context,
				"Target short term sync already removed", ", owner=" << pt_->dumpNodeAddress(0)
				<< ", owner LSN=" << ownerLsn << ", backupNode=" << pt_->dumpNodeAddress(targetNodeId)
				<< ", backup LSN=" << backupLsn);
		}
		syncRequestInfo.clearSearchCondition();
		syncRequestInfo.set(SYC_SHORTTERM_SYNC_LOG, context, ownerLsn, backupSyncId);
		if (ownerLsn > backupLsn && pt_->isOwnerOrBackup(pId, targetNodeId)) {
			syncRequestInfo.setSearchLsnRange(backupLsn + 1, ownerLsn);
			SearchLogInfo& info = context->getSearchLogInfo(targetNodeId);
			util::Stopwatch logWatch;
			context->start(logWatch);
			int64_t ssn = getTargetSSN(pId, context);
			try {
				LogManager<MutexLocker>& logManager = partitionList_->partition(pId).logManager();
				syncRequestInfo.getLogs(pId, ssn, &logManager, cpSvc_, context->isUseLongSyncLog(), false, info);
				context->endLog(logWatch);
				context->setProcessedLsn(syncRequestInfo.getStartLsn(), syncRequestInfo.getEndLsn());
				context->incProcessedLogNum(syncRequestInfo.getBinaryLogSize());
			}
			catch (UserException&) {
				context->endLog(logWatch);
				LogSequentialNumber prevStartLSN = pt_->getStartLSN(pId);
				pt_->setStartLSN(pId, pt_->getLSN(pId));
				GS_THROW_SYNC_ERROR(GS_ERROR_SYNC_LOG_GET_FAILED, context,
					"Short term log sync failed, log is not found or invalid",
					", owner=" << pt_->dumpNodeAddress(0) << ", owner LSN=" << ownerLsn << ", backup="
					<< pt_->dumpNodeAddress(targetNodeId) << ", backup LSN=" << backupLsn << ", maxLSN=" << maxLsn
					<< ", log start LSN=" << prevStartLSN);
			}
		}
	}
}

void SyncManager::setShorttermGetSyncLog(SyncContext* context, SyncRequestInfo& syncRequestInfo, NodeId targetNodeId) {
	LogSequentialNumber backupLsn = 0;
	SyncId backupSyncId;
	PartitionId pId = context->getPartitionId();
	LogSequentialNumber ownerLsn = pt_->getLSN(pId);
	LogSequentialNumber maxLsn = pt_->getMaxLsn(pId);
	if (!context->getSyncTargetLsnWithSyncId(targetNodeId, backupLsn, backupSyncId)) {
		GS_THROW_SYNC_ERROR(
			GS_ERROR_SYM_INVALID_PARTITION_REVISION, context,
			"Target short term sync already removed", ", owner="
			<< pt_->dumpNodeAddress(0)
			<< ", owner LSN=" << ownerLsn
			<< ", backupNode=" << pt_->dumpNodeAddress(targetNodeId)
			<< ", backup LSN=" << backupLsn
			<< ", log start LSN=" << pt_->getStartLSN(pId));
	}
	syncRequestInfo.clearSearchCondition();
	syncRequestInfo.set(SYC_SHORTTERM_SYNC_LOG, context, ownerLsn, backupSyncId);
	if (ownerLsn > backupLsn &&
		pt_->isOwnerOrBackup(pId, targetNodeId)) {
		syncRequestInfo.setSearchLsnRange(
			backupLsn + 1, ownerLsn);
		SearchLogInfo& info = context->getSearchLogInfo(targetNodeId);
		util::Stopwatch logWatch;
		context->start(logWatch);
		int64_t ssn = getTargetSSN(pId, context);
		try {
			LogManager<MutexLocker>& logManager = partitionList_->partition(pId).logManager();
			syncRequestInfo.getLogs(pId, ssn, &logManager, cpSvc_, context->isUseLongSyncLog(), false, info);
			context->endLog(logWatch);
			context->setProcessedLsn(syncRequestInfo.getStartLsn(), syncRequestInfo.getEndLsn());
			context->incProcessedLogNum(syncRequestInfo.getBinaryLogSize());
			context->setSyncTargetEndLsn(targetNodeId, syncRequestInfo.getEndLsn());
		}
		catch (UserException&) {
			context->endLog(logWatch);
			LogSequentialNumber prevStartLSN = pt_->getStartLSN(pId);
			pt_->setStartLSN(pId, pt_->getLSN(pId));
			GS_THROW_SYNC_ERROR(
				GS_ERROR_SYNC_LOG_GET_FAILED, context,
				"Short term log sync failed, log is not found or invalid",
				", owner=" << pt_->dumpNodeAddress(0)
				<< ", owner LSN=" << ownerLsn << ", backup="
				<< pt_->dumpNodeAddress(targetNodeId)
				<< ", backup LSN=" << backupLsn
				<< ", maxLSN=" << maxLsn
				<< ", log start LSN=" << prevStartLSN);
		}
	}
	else {
		syncRequestInfo.setSearchLsnRange(ownerLsn, ownerLsn);
		context->setSyncTargetEndLsn(targetNodeId, ownerLsn);
	}
}

void SyncManager::checkRestored(PartitionId pId) {

	Partition& partition = partitionList_->partition(pId);

	if (!partition.isActive()) {
		LogSequentialNumber prev = pt_->getLSN(pId);
		removePartition(pId);
		recoveryPartition(pId);
	}
}

void SyncManager::removePartition(PartitionId pId) {

	SyncPartitionLock partitionLock(txnMgr_, pId);
	{
		Partition& partition = partitionList_->partition(pId);
		partition.drop();
		partition.reinit(*partitionList_); 
	}
	setUndoCompleted(pId);
	pt_->setLSN(pId, 0);
}

void SyncManager::recoveryPartition(PartitionId pId) {
	Partition& partition = partitionList_->partition(pId);
	partition.recover(NULL);
	partition.fullCheckpoint(CP_PHASE_PRE_WRITE);
	partition.fullCheckpoint(CP_PHASE_PRE_SYNC);
	partition.fullCheckpoint(CP_PHASE_FINISH);
}


bool SyncManager::setShorttermSyncLog(SyncContext* context, SyncRequestInfo& syncRequestInfo,
		const util::DateTime& now, const EventMonotonicTime emNow) {
	bool isContinue = false;
	util::StackAllocator& alloc = syncRequestInfo.getAllocator();
	SyncVariableSizeAllocator& varSizeAlloc = getVariableSizeAllocator();
	PartitionId pId = context->getPartitionId();
	uint8_t* logBuffer = NULL;
	int32_t logBufferSize = 0;
	context->getLogBuffer(logBuffer, logBufferSize);
	size_t startLogOffset = context->getLogOffset();
	size_t logOffset = startLogOffset;
	if (logBuffer != NULL && logBufferSize > 0) {
		util::Stopwatch logWatch;
		context->start(logWatch);
		LogSequentialNumber startLsn = pt_->getLSN(pId);
		checkRestored(pId);
		try {
			Partition& partition = partitionList_->partition(pId);
			try {
				uint32_t limitInterval = UINT32_MAX;
				if (!partition.redoLogList(
					&alloc, REDO_MODE_SHORT_TERM_SYNC,
					now, emNow, logBuffer, logBufferSize, logOffset, &logWatch, limitInterval)) {
					context->setLogOffset(logOffset);
					isContinue = true;
				}
				else {
					context->freeBuffer(varSizeAlloc, LOG_SYNC, getSyncOptStat());
					context->setLogOffset(0);
				}
			}
			catch (std::exception& e) {
				context->setLogOffset(logOffset);
				GS_RETHROW_USER_OR_SYSTEM(e, "");
			}
			context->endLog(logWatch);
			context->incProcessedLogNum(logOffset - startLogOffset);
			context->setProcessedLsn(startLsn, pt_->getLSN(pId));
			setUndoCompleted(pId);
		}
		catch (std::exception& e) {
			context->endLog(logWatch);
			context->incProcessedLogNum(logOffset - startLogOffset);
			LogSequentialNumber endLsn = pt_->getLSN(pId);
			context->setProcessedLsn(startLsn, endLsn);
			context->freeBuffer(varSizeAlloc, LOG_SYNC, getSyncOptStat());
			GS_RETHROW_LOG_REDO_ERROR(
				e, context, "Short term log redo failed",
				", received [" << syncRequestInfo.getStartLsn()
				<< ", " << syncRequestInfo.getEndLsn() << "]"
				<< ", applied [" << startLsn << ", " << endLsn << "]");
		}
	}
	return isContinue;
}

void SyncManager::setShorttermSyncLogAck(
	SyncContext* context,
	SyncRequestInfo& syncRequestInfo,
	NodeId targetNodeId) {

	LogSequentialNumber backupLsn = 0;
	SyncId backupSyncId;
	PartitionId pId = context->getPartitionId();
	LogSequentialNumber ownerLsn = pt_->getLSN(pId);

	if (!context->getSyncTargetLsnWithSyncId(
		targetNodeId, backupLsn, backupSyncId)) {

		GS_THROW_SYNC_ERROR(
			GS_ERROR_SYM_INVALID_PARTITION_REVISION, context,
			"Target short term sync already removed", ", owner="
			<< pt_->dumpNodeAddress(0)
			<< ", owner LSN=" << ownerLsn
			<< ", targetNode=" << pt_->dumpNodeAddress(targetNodeId)
			<< ", backup LSN=" << backupLsn
			<< ", log start LSN=" << pt_->getStartLSN(pId));
	}
	syncRequestInfo.clearSearchCondition();
	syncRequestInfo.set(
		TXN_SHORTTERM_SYNC_END,
		context, ownerLsn, backupSyncId);
}

void SyncManager::setShorttermGetSyncNextLog(
	SyncContext* context,
	SyncRequestInfo& syncRequestInfo,
	SyncResponseInfo& syncResponseInfo, NodeId targetNodeId) {

	PartitionId pId = context->getPartitionId();
	LogSequentialNumber ownerLsn
		= pt_->getLSN(pId);
	LogSequentialNumber backupLsn
		= syncResponseInfo.getTargetLsn();
	LogSequentialNumber maxLsn = pt_->getMaxLsn(pId);
	syncRequestInfo.setSearchLsnRange(
		backupLsn + 1, ownerLsn);

	util::Stopwatch logWatch;
	context->start(logWatch);
	int64_t ssn = getTargetSSN(pId, context);

	try {
		LogManager<MutexLocker>& logManager = partitionList_->partition(pId).logManager();
		SearchLogInfo& info = context->getSearchLogInfo(targetNodeId);
		syncRequestInfo.getLogs(pId, ssn,
			&logManager, cpSvc_, context->isUseLongSyncLog(), false, info);

		context->endLog(logWatch);
		context->setProcessedLsn(
			syncRequestInfo.getStartLsn(),
			syncRequestInfo.getEndLsn());
		context->incProcessedLogNum(
			syncRequestInfo.getBinaryLogSize());
		context->setSyncTargetEndLsn(
			targetNodeId, syncRequestInfo.getEndLsn());
	}
	catch (UserException&) {
		context->endLog(logWatch);
		LogSequentialNumber prevStartLSN
			= pt_->getStartLSN(pId);
		pt_->setStartLSN(pId, pt_->getLSN(pId));

		GS_THROW_SYNC_ERROR(
			GS_ERROR_SYNC_LOG_GET_FAILED, context,
			"Short term log sync failed, log is not found or invalid",
			", owner=" << pt_->dumpNodeAddress(0)
			<< ", owner LSN=" << ownerLsn << ", backup="
			<< pt_->dumpNodeAddress(targetNodeId)
			<< ", backup LSN=" << backupLsn
			<< ", maxLSN=" << maxLsn
			<< ", log start LSN=" << prevStartLSN);
	}
	syncRequestInfo.set(
		SYC_SHORTTERM_SYNC_LOG, context,
		ownerLsn, syncResponseInfo.getBackupSyncId());
}


PartitionStatus SyncManager::setShorttermSyncEnd(
	SyncContext* context) {

	PartitionId pId = context->getPartitionId();
	PartitionStatus nextStatus = PartitionTable::PT_ON;
	PartitionRoleStatus nextRoleStatus = PartitionTable::PT_NONE;
	if (pt_->isOwner(pId)) {
		nextRoleStatus = PartitionTable::PT_OWNER;
	}
	else if (pt_->isBackup(pId)) {
		nextRoleStatus = PartitionTable::PT_BACKUP;
	}
	else {
		nextStatus = PartitionTable::PT_OFF;
	}
	pt_->setPartitionRoleStatus(pId, nextRoleStatus);
	return nextStatus;
}


void SyncManager::setShorttermSyncEndAck(SyncContext* context, SyncRequestInfo& syncRequestInfo) {
	PartitionId pId = context->getPartitionId();
	PartitionRole role;
	pt_->getPartitionRole(pId, role);
	syncRequestInfo.setPartitionRole(role);
}

void SyncManager::setLongtermSyncRequest(SyncContext* context, SyncRequestInfo& syncRequestInfo) {
	PartitionId pId = context->getPartitionId();
	PartitionRole& nextRole = syncRequestInfo.getPartitionRole();
	context->startAll();
	pt_->setPartitionRole(pId, nextRole, PartitionTable::PT_CURRENT_OB);
}

void SyncManager::setLongtermSyncPrepareAck(SyncContext* context, SyncRequestInfo& syncRequestInfo,
	SyncResponseInfo& syncResponseInfo, util::XArray<NodeId>& catchups) {
	PartitionId pId = context->getPartitionId();
	pt_->getCatchup(pId, catchups);
	syncRequestInfo.set(TXN_LONGTERM_SYNC_START, context, pt_->getLSN(pId), syncResponseInfo.getBackupSyncId());
	PartitionRole nextRole;
	pt_->getPartitionRole(pId, nextRole);
	syncRequestInfo.setPartitionRole(nextRole);
}

void SyncManager::setLongtermSyncStart(PartitionId pId, SyncRequestInfo& syncRequestInfo) {
	PartitionRole& nextCatchup = syncRequestInfo.getPartitionRole();
	pt_->setPartitionRole(pId, nextCatchup, PartitionTable::PT_CURRENT_OB);
}

bool SyncManager::checkLongGetSyncLog(SyncContext* context, SyncRequestInfo& syncRequestInfo,
	SyncResponseInfo& syncResponseInfo, NodeId targetNodeId) {
	PartitionId pId = context->getPartitionId();
	LogSequentialNumber ownerLsn = pt_->getLSN(pId);
	LogSequentialNumber catchupLsn = syncResponseInfo.getTargetLsn();
	context->setSyncTargetLsnWithSyncId(targetNodeId, catchupLsn, syncResponseInfo.getBackupSyncId());
	LogManager<MutexLocker>& logManager = partitionList_->partition(pId).logManager();
	syncRequestInfo.clearSearchCondition();
	syncRequestInfo.set(SYC_LONGTERM_SYNC_LOG, context, pt_->getLSN(pId), syncResponseInfo.getBackupSyncId());
	bool isSyncLog = false;
	SyncStat& stat = clsMgr_->getSyncStat();
	stat.syncApplyNum_ = 0;
	stat.syncChunkNum_ = 0;
	LogSequentialNumber startLsn = (ownerLsn == catchupLsn) ? ownerLsn : catchupLsn + 1;
	syncRequestInfo.setSearchLsnRange(startLsn, ownerLsn);
	if ((ownerLsn == 0 && catchupLsn == 0) || (ownerLsn == catchupLsn)) {
		isSyncLog = true;
	}
	else {
		util::Stopwatch logWatch;
		context->start(logWatch);
		try {
			SearchLogInfo& info = context->getSearchLogInfo(targetNodeId);
			syncRequestInfo.getLogs(pId, context->getSequentialNumber(), &logManager, cpSvc_, context->isUseLongSyncLog(), false, info);
			isSyncLog = (syncRequestInfo.getBinaryLogSize() > 0);
			if (isSyncLog) {
				context->setProcessedLsn(syncRequestInfo.getStartLsn(), syncRequestInfo.getEndLsn());
				context->incProcessedLogNum(syncRequestInfo.getBinaryLogSize());
			}
		}
		catch (UserException&) {
			isSyncLog = false;
		}
		context->endLog(logWatch);
	}
	return isSyncLog;
}

bool SyncManager::setLongtermSyncChunk(SyncContext* context, NodeId senderNodeId) {
	bool ret = false;
	PartitionId pId = context->getPartitionId();
	SyncVariableSizeAllocator& varSizeAlloc = getVariableSizeAllocator();
	GS_OUTPUT_SYNC(context, "[TXN_LONGTERM_SYNC_CHUNK:setLongtermSyncChunk:START]", "");
	Partition& partition = partitionList_->partition(pId);
	if (context->getProcessedChunkNum() == 0) {
		removePartition(pId);
		GS_OUTPUT_SYNC(context, "[TXN_LONGTERM_SYNC_CHUNK:removePartition:END]", "");
		GS_TRACE_SYNC(context, "[CATCHUP] Long term apply chunks start", "");;
	}
	int32_t chunkNum = 0;
	int32_t chunkSize = 0;
	context->getChunkInfo(chunkNum, chunkSize);
	util::Stopwatch chunkWatch;
	context->start(chunkWatch);
	GS_OUTPUT_SYNC(context, "[TXN_LONGTERM_SYNC_CHUNK:catchup:START]", "CHUNK:" << chunkNum);
	for (int32_t chunkNo = 0; chunkNo < chunkNum; chunkNo++) {
		uint8_t* chunkBuffer = NULL;
		context->getChunkBuffer(chunkBuffer, chunkNo);
		if (chunkBuffer != NULL) {
			try {
				partition.catchup(chunkBuffer, chunkSize, context->getProcessedChunkNum());
			}
			catch (std::exception& e) {
				context->freeBuffer(varSizeAlloc, CHUNK_SYNC, getSyncOptStat());
				context->endChunk(chunkWatch);
				context->endChunkAll();
				GS_RETHROW_SYNC_ERROR(e, context,
					"[CATCHUP] Long term chunk sync failed, recovery chunk is failed", "");
			}
			context->incProcessedChunkNum();
			bool isFirst = (context->getProcessedChunkNum() == 0);
			if (isFirst || (context->getProcessedChunkNum() %
				getExtraConfig().getLongtermDumpChunkInterval()) == 0) {
				GS_TRACE_SYNC(context, "[CATCHUP] Long term apply chunks", ", owner="
					<< pt_->dumpNodeAddress(senderNodeId)
					<< ", processed chunks="
					<< context->getProcessedChunkNum());
			}
			SyncStat& stat = clsMgr_->getSyncStat();
			stat.syncApplyNum_ = static_cast<int32_t>(context->getProcessedChunkNum());
		}
	}
	context->endChunk(chunkWatch);
	context->freeBuffer(varSizeAlloc, CHUNK_SYNC, getSyncOptStat());
	uint8_t* logBuffer = NULL;
	int32_t logBufferSize = 0;
	context->getLogBuffer(logBuffer, logBufferSize);
	if (logBuffer != NULL && logBufferSize > 0) {
		GS_OUTPUT_SYNC(context, "[TXN_LONGTERM_SYNC_CHUNK:restore:START]", "logBufferSizeB=" << logBufferSize);
		{
			GS_TRACE_SYNC(context, "[CATCHUP] Long term restore partition", "");
			try {
				partition.restore(logBuffer, logBufferSize);
			}
			catch (std::exception& e) {
				context->freeBuffer(varSizeAlloc, LOG_SYNC, getSyncOptStat());
				context->endChunkAll();
				GS_RETHROW_SYNC_ERROR(e, context,
					"[CATCHUP] Long term chunk sync failed, restore is failed", "");
			}
		}
		ret = true;
		GS_TRACE_SYNC(context, "[CATCHUP] Long term apply chunks completed", "");
		context->freeBuffer(varSizeAlloc, LOG_SYNC, getSyncOptStat());
		GS_OUTPUT_SYNC(context, "[TXN_LONGTERM_SYNC_CHUNK:restore:END]", "");
	}
	context->endChunkAll();
	GS_OUTPUT_SYNC(context, "[TXN_LONGTERM_SYNC_CHUNK:setLongtermSyncChunk:END]", "");
	return ret;
}

void SyncManager::checkRemoveLogFile(SyncContext* context, SearchLogInfo& info, LogManager<MutexLocker>& logManager) {
}

void SyncManager::setLongtermSyncChunkAck(
	SyncContext* context, SyncRequestInfo& syncRequestInfo,
	SyncResponseInfo& syncResponseInfo, NodeId senderNodeId) {

	PartitionId pId = context->getPartitionId();
	context->endChunkAll();
	syncRequestInfo.set(SYC_LONGTERM_SYNC_LOG, context, pt_->getLSN(pId), syncResponseInfo.getBackupSyncId());
	LogSequentialNumber ownerLsn = pt_->getLSN(pId);
	LogSequentialNumber catchupLsn = syncResponseInfo.getTargetLsn();
	if (ownerLsn > catchupLsn) {
		syncRequestInfo.setSearchLsnRange(catchupLsn + 1, ownerLsn);
		util::Stopwatch logWatch;
		context->start(logWatch);
		LogManager<MutexLocker>& logManager = partitionList_->partition(pId).logManager();
		try {
			SearchLogInfo& info = context->getSearchLogInfo(senderNodeId);
			int64_t beforeVersion = info.logVersion_;
			syncRequestInfo.getLogs(
				pId, context->getSequentialNumber(),
				&logManager, cpSvc_, context->isUseLongSyncLog(), false, info);
			checkRemoveLogFile(context, info, logManager);
			context->endLog(logWatch);
			context->setProcessedLsn(syncRequestInfo.getStartLsn(), syncRequestInfo.getEndLsn());
			context->incProcessedLogNum(syncRequestInfo.getBinaryLogSize());
			GS_TRACE_SYNC(context, "[OWNER] Long term send chunks completed and send logs start",
				", send chunks=" << context->getProcessedChunkNum() << ", start lsn=" << syncRequestInfo.getStartLsn());
		}
		catch (UserException& e) {
			context->endLog(logWatch);
			UTIL_TRACE_EXCEPTION(SYNC_SERVICE, e, "reason=" << GS_EXCEPTION_MESSAGE(e));
			GS_TRACE_SYNC(context, "Long term log sync failed, log is not found or invalid",
				", received [" << syncRequestInfo.getStartLsn() << ", " << syncRequestInfo.getEndLsn() << "]"
				<< ", logStartLsn=" << pt_->getStartLSN(pId));
		}
	}
	else {
		syncRequestInfo.setSearchLsnRange(ownerLsn, ownerLsn);
	}
}

bool SyncManager::setLongtermSyncLog(SyncContext* context, SyncRequestInfo& syncRequestInfo,
		const util::DateTime& now, const EventMonotonicTime emNow) {
	bool isContinue = false;
	util::StackAllocator& alloc = syncRequestInfo.getAllocator();
	SyncVariableSizeAllocator& varSizeAlloc = getVariableSizeAllocator();
	PartitionId pId = context->getPartitionId();
	uint8_t* logBuffer = NULL;
	int32_t logBufferSize = 0;
	context->getLogBuffer(logBuffer, logBufferSize);
	size_t startLogOffset = context->getLogOffset();
	size_t logOffset = startLogOffset;
	if (logBuffer != NULL && logBufferSize > 0) {
		util::Stopwatch logWatch;
		context->start(logWatch);
		LogSequentialNumber startLsn = pt_->getLSN(pId);
		checkRestored(pId);
		try {
			Partition& partition = partitionList_->partition(pId);
			uint32_t limitInterval = getConfig().getLogInterruptionInterval();
			if (!partition.redoLogList(&alloc, REDO_MODE_LONG_TERM_SYNC,
				now, emNow, logBuffer, logBufferSize, logOffset, &logWatch, limitInterval)) {
				context->setLogOffset(logOffset);
				isContinue = true;
			}
			else {
				context->freeBuffer(varSizeAlloc, LOG_SYNC, getSyncOptStat());
				context->setLogOffset(0);
			}
			context->endLog(logWatch);
			context->incProcessedLogNum(logOffset - startLogOffset);
			context->setProcessedLsn(startLsn, pt_->getLSN(pId));
			setUndoCompleted(pId);
		}
		catch (std::exception& e) {
			context->endLog(logWatch);
			context->incProcessedLogNum(logOffset - startLogOffset);
			LogSequentialNumber endLsn = pt_->getLSN(pId);
			context->setProcessedLsn(startLsn, endLsn);
			GS_RETHROW_LOG_REDO_ERROR(
				e, context, "Long term log redo failed",
				", received [" << syncRequestInfo.getStartLsn()
				<< ", " << syncRequestInfo.getEndLsn() << "]"
				<< ", applied [" << startLsn << ", " << endLsn << "]");
		}
	}
	return isContinue;
}

void SyncManager::setLongtermSyncLogAck(
	SyncContext* context, SyncRequestInfo& syncRequestInfo,
	SyncResponseInfo& syncResponseInfo, NodeId senderNodeId) {

	PartitionId pId = context->getPartitionId();
	LogSequentialNumber ownerLsn = pt_->getLSN(pId);
	LogSequentialNumber catchupLsn = syncResponseInfo.getTargetLsn();
	syncRequestInfo.setSearchLsnRange(catchupLsn + 1, ownerLsn);
	util::Stopwatch logWatch;
	context->start(logWatch);
	LogSequentialNumber startLsn = pt_->getLSN(pId);
	try {
		LogManager<MutexLocker>& logManager = partitionList_->partition(pId).logManager();
		SearchLogInfo& info = context->getSearchLogInfo(senderNodeId);
		syncRequestInfo.getLogs(
			pId, context->getSequentialNumber(),
			&logManager, cpSvc_, context->isUseLongSyncLog(), false, info);
		checkRemoveLogFile(context, info, logManager);
		context->endLog(logWatch);
		context->setProcessedLsn(startLsn, pt_->getLSN(pId));
		context->incProcessedLogNum(syncRequestInfo.getBinaryLogSize());
	}
	catch (UserException&) {
		context->endLog(logWatch);
		GS_THROW_SYNC_ERROR(
			GS_ERROR_SYNC_LOG_GET_FAILED, context,
			"Long term log sync failed, log is not found or invalid",
			", owner=" << pt_->dumpNodeAddress(0)
			<< ", owner LSN="
			<< ownerLsn << ", catchup="
			<< pt_->dumpNodeAddress(senderNodeId)
			<< ", catchup LSN=" << catchupLsn);
	}
	syncRequestInfo.set(
		SYC_LONGTERM_SYNC_LOG, context,
		ownerLsn, syncResponseInfo.getBackupSyncId());
}

/*!
	@brief Checks if a specified operation is executable
*/
void SyncManager::checkExecutable(
	EventType operation,
	PartitionId pId,
	PartitionRole& candNextRole) {

	try {
		checkActiveStatus();
		if (pId >= pt_->getPartitionNum()) {
			GS_THROW_USER_ERROR(
				GS_ERROR_SYM_INVALID_PARTITION_INFO, "");
		}
		if (operation == TXN_SYNC_TIMEOUT
			|| operation == TXN_SYNC_CHECK_END) {
			return;
		}

		PartitionRevisionNo candRevision
			= candNextRole.getRevisionNo();
		PartitionRole currentRole;
		PartitionRevisionNo currentRevision
			= currentRole.getRevisionNo();
		if (candRevision < currentRevision) {
			GS_THROW_USER_ERROR(
				GS_ERROR_SYM_INVALID_PARTITION_REVISION,
				"candRevision=" << candRevision
				<< ", currentRevision=" << currentRevision);
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(
				e, "Failed to check sync, pId=" << pId));
	}
}

/*!
	@brief Creates Partition
*/
void SyncManager::createPartition(PartitionId pId) {
	if (pId >= pt_->getPartitionNum()) {
		return;
	}
	if (syncContextTables_[pId] != NULL) {
		return;
	}
	try {
		syncContextTables_[pId] = ALLOC_NEW(globalStackAlloc_)
			SyncContextTable(globalStackAlloc_, pId, DEFAULT_CONTEXT_SLOT_NUM, &varSizeAlloc_);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, GS_EXCEPTION_MERGE_MESSAGE(e, "Failed to sync: pId=" << pId));
	}
}

SyncManager::SyncContextTable::SyncContextTable(
	util::StackAllocator& alloc,
	PartitionId pId,
	uint32_t numInitialSlot,
	SyncVariableSizeAllocator* varSizeAlloc) :
	pId_(pId),
	numCounter_(0),
	freeList_(NULL),
	numUsed_(0),
	globalStackAlloc_(&alloc),
	slots_(alloc),
	varSizeAlloc_(varSizeAlloc) {

	try {
		for (uint32_t i = 0; i < numInitialSlot; i++) {

			SyncContext* slot = ALLOC_NEW(*globalStackAlloc_)
				SyncContext[SLOT_SIZE];
			slots_.push_back(slot);
			for (uint32_t j = 0; j < SLOT_SIZE; j++) {
				slot[j].setPartitionId(pId_);
				slot[j].setId(numCounter_++);
				slot[j].nextEmptyChain_ = freeList_;
				freeList_ = &slot[j];
			}
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

SyncManager::SyncContextTable::~SyncContextTable() {
	for (size_t i = 0; i < slots_.size(); i++) {
		for (uint32_t j = 0; j < SLOT_SIZE; j++) {

			slots_[i][j].clear(*varSizeAlloc_, NULL);
			ALLOC_DELETE(*globalStackAlloc_, &slots_[i][j]);
		}
	}
}

/*!
	@brief Creates SyncContext
*/
SyncContext* SyncManager::SyncContextTable::createSyncContext(PartitionRevision& ptRev) {
	try {
		SyncContext* context = freeList_;
		if (context == NULL) {
			SyncContext* slot = ALLOC_NEW(*globalStackAlloc_)
				SyncContext[SLOT_SIZE];
			slots_.push_back(slot);
			for (uint32_t j = 0; j < SLOT_SIZE; j++) {
				slot[j].setPartitionId(pId_);
				slot[j].setId(numCounter_++);
				slot[j].nextEmptyChain_ = freeList_;
				freeList_ = &slot[j];
			}
			context = freeList_;
		}
		freeList_ = context->getNextEmptyChain();
		context->setNextEmptyChain(NULL);
		context->setPartitionRevision(ptRev);
		context->setUsed();
		numUsed_++;
		return context;
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Removes SyncContext
*/
void SyncManager::SyncContextTable::removeSyncContext(
	SyncVariableSizeAllocator& alloc,
	SyncContext*& context,
	SyncOptStat* stat) {

	try {
		if (context == NULL) {
			return;
		}
		context->clear(alloc, stat);
		context->setNextEmptyChain(freeList_);
		freeList_ = context;
		numUsed_--;
		context = NULL;
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Gets SyncContext
*/
SyncContext* SyncManager::SyncContextTable::getSyncContext(
	int32_t id, uint64_t version) const {

	if (id < numCounter_) {
		const uint32_t slotNo = id / SLOT_SIZE;
		const uint32_t offset = id - SLOT_SIZE * slotNo;
		SyncContext* slot = slots_[slotNo];
		if (slot[offset].version_ == version && slot[offset].used_) {
			return &slot[offset];
		}
	}
	return NULL;
}

SyncContext::SyncContext() :
	id_(0),
	pId_(0),
	version_(0),
	used_(false),
	numSendBackup_(0),
	recvNodeId_(UNDEF_NODEID),
	isCatchupSync_(false),
	isSyncCpCompleted_(false),
	nextEmptyChain_(NULL),
	isDump_(false),
	isExecuteCheckpoint_(false),
	processedChunkNum_(0),
	logBuffer_(NULL),
	logBufferSize_(0),
	chunkBuffer_(NULL),
	chunkBufferSize_(0),
	chunkBaseSize_(0),
	chunkNum_(0),
	mode_(MODE_SHORTTERM_SYNC),
	roleStatus_(PartitionTable::PT_OWNER),
	processedLogNum_(0),
	processedLogSize_(0),
	actualLogTime_(0),
	actualChunkTime_(0),
	chunkLeadTime_(0),
	totalTime_(0),
	startLsn_(0),
	endLsn_(0),
	syncSequentialNumber_(0),
	longSyncType_(NOT_LONG_TERM_SYNC),
	invalidCount_(0),
	prevProcessedChunkNum_(0),
	prevProcessedLogNum_(0),
	logOffset_(0),
	keepLogVersion_(-1),
	eventType_(UNDEF_EVENT_TYPE),
	prevEventType_(UNDEF_EVENT_TYPE),
	stateStartTime_(-1)
{
}

SyncContext::~SyncContext() {
}

/*!
	@brief Increments counter
*/
void SyncContext::incrementCounter(
	NodeId syncTargetNodeId) {

	try {
		for (size_t i = 0; i < sendBackups_.size(); i++) {
			if (sendBackups_[i].nodeId_ == syncTargetNodeId) {
				sendBackups_[i].isAcked_ = false;
				sendBackups_[i].lsn_ = UNDEF_LSN;
				numSendBackup_++;
				return;
			}
		}
		SendBackup sb(syncTargetNodeId);
		sendBackups_.push_back(sb);
		numSendBackup_++;
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Decrements counter
*/
bool SyncContext::decrementCounter(
	NodeId syncTargetNodeId) {

	for (size_t i = 0; i < sendBackups_.size(); i++) {
		if (sendBackups_[i].nodeId_ == syncTargetNodeId) {
			sendBackups_[i].isAcked_ = true;
			numSendBackup_--;
			break;
		}
	}
	return (numSendBackup_ == 0);
}

/*!
	@brief Sets LSN and SyncID of target node id
*/
void SyncContext::setSyncTargetLsnWithSyncId(
	NodeId syncTargetNodeId,
	LogSequentialNumber lsn,
	SyncId backupSyncId) {

	for (size_t i = 0; i < sendBackups_.size(); i++) {
		if (sendBackups_[i].nodeId_ == syncTargetNodeId) {
			if (sendBackups_[i].lsn_ == UNDEF_LSN
				|| (sendBackups_[i].lsn_ != UNDEF_LSN
					&& sendBackups_[i].lsn_ < lsn)) {
				sendBackups_[i].lsn_ = lsn;
				sendBackups_[i].endLsn_ = lsn;
			}
			sendBackups_[i].backupSyncId_ = backupSyncId;
			break;
		}
	}
}

void SyncContext::setSyncTargetLsn(
	NodeId syncTargetNodeId, LogSequentialNumber lsn) {

	for (size_t i = 0; i < sendBackups_.size(); i++) {
		if (sendBackups_[i].nodeId_ == syncTargetNodeId) {
			if (sendBackups_[i].lsn_ == UNDEF_LSN
				|| (sendBackups_[i].lsn_ != UNDEF_LSN
					&& sendBackups_[i].lsn_ < lsn)) {
				sendBackups_[i].lsn_ = lsn;
			}
			break;
		}
	}
}

void SyncContext::SendBackup::dump() {
	std::cout << "nodeId=" << nodeId_ << ", lsn=" << lsn_ << ", endLsn=" << endLsn_ << std::endl;
}

void SyncContext::setSyncTargetEndLsn(NodeId syncTargetNodeId, LogSequentialNumber lsn) {

	for (size_t i = 0; i < sendBackups_.size(); i++) {
		if (sendBackups_[i].nodeId_ == syncTargetNodeId) {
			if (sendBackups_[i].lsn_ == UNDEF_LSN
				|| (sendBackups_[i].lsn_ != UNDEF_LSN
					&& sendBackups_[i].lsn_ < lsn)) {
				sendBackups_[i].endLsn_ = lsn;
			}
			break;
		}
	}
}

/*!
	@brief Gets LSN of target node id
*/
LogSequentialNumber SyncContext::getSyncTargetLsn(
	NodeId syncTargetNodeId) const {

	for (size_t i = 0; i < sendBackups_.size(); i++) {
		if (sendBackups_[i].nodeId_ == syncTargetNodeId) {
			return sendBackups_[i].lsn_;
		}
	}
	return UNDEF_LSN;
}

LogSequentialNumber SyncContext::getSyncTargetEndLsn(
	NodeId syncTargetNodeId) const {

	for (size_t i = 0; i < sendBackups_.size(); i++) {
		if (sendBackups_[i].nodeId_ == syncTargetNodeId) {
			return sendBackups_[i].endLsn_;
		}
	}
	return UNDEF_LSN;
}


/*!
	@brief Gets LSN and SyncID of target node id
*/
bool SyncContext::getSyncTargetLsnWithSyncId(
	NodeId syncTargetNodeId,
	LogSequentialNumber& backupLsn,
	SyncId& backupSyncId) {

	for (size_t i = 0; i < sendBackups_.size(); i++) {
		if (sendBackups_[i].nodeId_ == syncTargetNodeId) {
			backupLsn = sendBackups_[i].lsn_;
			backupSyncId = sendBackups_[i].backupSyncId_;
			return true;
		}
	}
	return false;
}

/*!
	@brief Copies log buffer
*/
void SyncContext::copyLogBuffer(
	SyncVariableSizeAllocator& alloc,
	const uint8_t* logBuffer,
	int32_t logBufferSize,
	SyncOptStat* stat) {

	try {
		if (logBuffer == NULL || logBufferSize == 0) {
			return;
		}
		logBuffer_ = static_cast<uint8_t*>(
			alloc.allocate(logBufferSize));
		if (stat != NULL) {
			stat->statAllocate(pId_, logBufferSize);
		}
		memcpy(logBuffer_, logBuffer, logBufferSize);
		logBufferSize_ = logBufferSize;
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Copies chunk buffer
*/
void SyncContext::copyChunkBuffer(
	SyncVariableSizeAllocator& alloc,
	const uint8_t* chunkBuffer,
	int32_t chunkSize,
	int32_t chunkNum,
	SyncOptStat* stat) {

	try {
		if (chunkBuffer == NULL
			|| chunkSize == 0 || chunkNum == 0) {
			return;
		}
		int32_t allocSize = chunkSize * chunkNum;
		chunkBuffer_ = static_cast<uint8_t*>(
			alloc.allocate(allocSize));
		if (stat != NULL) {
			stat->statAllocate(pId_, allocSize);
		}
		memcpy(chunkBuffer_, chunkBuffer, allocSize);
		chunkBaseSize_ = chunkSize;
		chunkNum_ = chunkNum;
		chunkBufferSize_ = allocSize;
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Deallocate buffer
*/
void SyncContext::freeBuffer(
	SyncVariableSizeAllocator& alloc,
	SyncType syncType, SyncOptStat* stat) {

	try {
		switch (syncType) {
		case LOG_SYNC: {
			if (logBuffer_) {
				alloc.deallocate(logBuffer_);
				if (stat != NULL) {
					stat->statFree(pId_, logBufferSize_);
				}
			}
			logBuffer_ = NULL;
			logBufferSize_ = 0;
			break;
		}
		case CHUNK_SYNC: {
			if (chunkBuffer_) {
				alloc.deallocate(chunkBuffer_);
				if (stat != NULL) {
					stat->statFree(pId_, chunkBufferSize_);
				}
			}
			chunkBuffer_ = NULL;
			chunkBufferSize_ = 0;
			chunkNum_ = 0;
			break;
		}
		default:
			GS_THROW_USER_ERROR(
				GS_ERROR_SYM_INVALID_SYNC_TYPE, "");
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Gets log buffer
*/
void SyncContext::getLogBuffer(
	uint8_t*& logBuffer, int32_t& logBufferSize) {
	logBuffer = logBuffer_;
	logBufferSize = logBufferSize_;
}

/*!
	@brief Gets chunk buffer
*/
void SyncContext::getChunkBuffer(
	uint8_t*& chunkBuffer, int32_t chunkNo) {

	chunkBuffer = NULL;
	if (chunkBuffer_ == NULL
		|| chunkNo >= chunkNum_
		|| chunkBufferSize_ < chunkBaseSize_ * chunkNo) {
		return;
	}
	chunkBuffer = static_cast<uint8_t*>(
		chunkBuffer_ + (chunkBaseSize_ * chunkNo));
}

void SyncContext::clear(SyncVariableSizeAllocator& alloc, SyncOptStat* stat) {
	try {
		sendBackups_.clear();
		numSendBackup_ = 0;
		isSyncCpCompleted_ = false;
		isDump_ = false;
		processedChunkNum_ = 0;
		chunkNum_ = 0;
		if (logBuffer_ != NULL) {
			alloc.deallocate(logBuffer_);
			if (stat != NULL) {
				stat->statFree(pId_, logBufferSize_);
			}
		}
		if (chunkBuffer_ != NULL) {
			alloc.deallocate(chunkBuffer_);
			if (stat != NULL) {
				stat->statFree(pId_, chunkBufferSize_);
			}
		}
		logBuffer_ = NULL;
		chunkBuffer_ = NULL;
		processedChunkNum_ = 0;
		logBufferSize_ = 0;
		chunkBufferSize_ = 0;
		chunkBaseSize_ = 0;
		chunkNum_ = 0;
		recvNodeId_ = UNDEF_NODEID;
		setUnuse();
		updateVersion();
		mode_ = MODE_SHORTTERM_SYNC;
		roleStatus_ = PartitionTable::PT_OWNER;
		processedLogNum_ = 0;
		processedLogSize_ = 0;
		actualLogTime_ = 0;
		actualChunkTime_ = 0;
		chunkLeadTime_ = 0;
		totalTime_ = 0;
		startLsn_ = 0;
		endLsn_ = 0;
		syncSequentialNumber_ = 0;
		longSyncType_ = NOT_LONG_TERM_SYNC;
		invalidCount_ = 0;
		prevProcessedChunkNum_ = 0;
		prevProcessedLogNum_ = 0;
		logOffset_ = 0;
		keepLogVersion_ = -1;
		eventType_ = UNDEF_EVENT_TYPE;
		prevEventType_ = UNDEF_EVENT_TYPE;
		stateStartTime_ = -1;
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Gets number of active contexts
*/
int32_t SyncManager::getActiveContextNum() {

	int32_t activeCount = 0;
	for (PartitionId pId = 0;
		pId < pt_->getPartitionNum(); pId++) {
		if (syncContextTables_[pId] != NULL) {
			activeCount += syncContextTables_[pId]->numUsed_;
		}
	}
	return activeCount;
}

std::string SyncManager::dumpAll() {

	util::NormalOStringStream ss;
	ss << "{";
	for (PartitionId pId = 0;
		pId < pt_->getPartitionNum(); pId++) {
		dump(pId);
	}
	ss << "}";
	return ss.str();
}

std::string SyncManager::dump(PartitionId pId) {

	util::NormalOStringStream ss;
	if (syncContextTables_[pId] != NULL) {
		for (size_t pos = 0;
			pos < syncContextTables_[pId]->slots_.size();
			pos++) {
			ss << syncContextTables_[pId]->slots_[pos]->dump();
		}
	}
	return ss.str();
}

std::string dumpNodeAddressList(
	PartitionTable* pt, NodeIdList& nodeList) {

	util::NormalOStringStream ss;
	int32_t listSize = static_cast<int32_t>(nodeList.size());
	ss << "[";
	for (int32_t pos = 0;
		pos < listSize; pos++) {
		ss << pt->dumpNodeAddress(nodeList[pos]);
		if (pos != listSize - 1) {
			ss << ",";
		}
	}
	ss << "]";
	return ss.str().c_str();
}

std::string dumpNodeAddressListWithLsn(
	PartitionTable* pt, NodeIdList& nodeList) {

	util::NormalOStringStream ss;
	int32_t listSize = static_cast<int32_t>(nodeList.size());
	ss << "[";
	for (int32_t pos = 0; pos < listSize; pos++) {
		ss << pt->dumpNodeAddress(nodeList[pos]);
		if (pos != listSize - 1) {
			ss << ",";
		}
	}
	ss << "]";
	return ss.str().c_str();
}

std::string SyncContext::dump(uint8_t detailMode) {

	util::NormalOStringStream ss;
	ss << "pId=" << pId_
		<< ", lsn=" << pt_->getLSN(pId_)
		<< ", maxLsn=" << pt_->getMaxLsn(pId_)
		<< ", logStartLsn=" << pt_->getStartLSN(pId_)
		<< ", SSN=" << syncSequentialNumber_
		<< ", revision=" << ptRev_.getRevisionNo()
		<< ", mode=" << getSyncModeStr()
		<< ", role=" << PartitionTable::dumpPartitionRoleStatus(roleStatus_);

	if (detailMode == 4) {
		if (roleStatus_ == PartitionTable::PT_OWNER) {
			NodeIdList nodeIdList;
			ss << ", backups=[";
			for (size_t pos = 0; pos < sendBackups_.size(); pos++) {
				ss << "(" << pt_->dumpNodeAddress(
					sendBackups_[pos].nodeId_);
				if (sendBackups_[pos].lsn_ != UNDEF_LSN) {
					ss << ", " << sendBackups_[pos].lsn_;
				}
				ss << ")";
				if (pos != sendBackups_.size() - 1) {
					ss << ",";
				}
			}
			ss << "]";
		}
		else {
			NodeIdList nodeIdList;
			nodeIdList.push_back(recvNodeId_);
			ss << ", owner="
				<< pt_->dumpNodeAddressList(nodeIdList);
		}
	}
	else {
		if (roleStatus_ == PartitionTable::PT_OWNER) {
			NodeIdList nodeIdList;
			ss << ", backups=[";
			for (size_t pos = 0; pos < sendBackups_.size(); pos++) {
				ss << "(" << pt_->dumpNodeAddress(
					sendBackups_[pos].nodeId_) << ")";
				if (pos != sendBackups_.size() - 1) {
					ss << ",";
				}
			}
			ss << "]";
		}
		else {
			NodeIdList nodeIdList;
			nodeIdList.push_back(recvNodeId_);
			ss << ", owner="
				<< pt_->dumpNodeAddressList(nodeIdList);
		}
	}
	switch (detailMode) {
	case 1: {
		ss << "no=" << id_
			<< ":" << version_
			<< ":" << ptRev_.toString()
			<< ", isSyncCpCompleted=" << isSyncCpCompleted_
			<< ", used=" << used_
			<< ", numSendBackup=" << numSendBackup_
			<< ", processedChunkNum=" << processedChunkNum_
			<< ", chunkBufferSizeB=" << chunkBufferSize_
			<< ", chunkBaseSizeB=" << chunkBaseSize_;
	}
		  break;
	case 2: {
		if (processedLogNum_ != 0) {
			ss << ", processedLogCount=" << processedLogNum_;
		}
		if (processedLogSize_ != 0) {
			ss << ", processedLogSizeMB="
				<< (processedLogSize_ / (1024 * 1024));
		}
		if (actualLogTime_ != 0) {
			ss << ", actualLogTimeMills=" << actualLogTime_;
		}
		if (startLsn_ == 0 && endLsn_ == 0) {
		}
		else {
			ss << ", startLSN=" << startLsn_
				<< ", endLSN=" << endLsn_;
		}
		if (mode_ == MODE_LONGTERM_SYNC) {
			if (processedChunkNum_ != 0) {
				ss << ", processedChunkCount=" << processedChunkNum_;
			}
			if (actualChunkTime_ != 0) {
				ss << ", actualChunkTimeMills=" << actualChunkTime_;
			}
			if (chunkLeadTime_ != 0) {
				ss << ", chunkLeadTimeMills=" << chunkLeadTime_;
			}
		}
		ss << ", totalTimeMills=" << totalTime_;
	}
		  break;
	case 3: {

	}
		  break;
	}
	return ss.str();
}

SyncManager::ConfigSetUpHandler SyncManager::configSetUpHandler_;

void SyncManager::ConfigSetUpHandler::operator()(ConfigTable& config) {

	CONFIG_TABLE_RESOLVE_GROUP(
		config, CONFIG_TABLE_SYNC, "sync");

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_TIMEOUT_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
		.setMin(1)
		.setDefault(30);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_LONG_SYNC_TIMEOUT_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
		.setMin(1)
		.setDefault(INT32_MAX);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_LONG_SYNC_MAX_MESSAGE_SIZE, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_SIZE_B)
		.setMin(1024)
		.setMax(static_cast<int32_t>(ConfigTable::megaBytesToBytes(128)))
		.setDefault(static_cast<int32_t>(ConfigTable::megaBytesToBytes(
			DEFAULT_LOG_SYNC_MESSAGE_MAX_SIZE)));

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_LOG_MAX_MESSAGE_SIZE, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_SIZE_MB)
		.setMin(1)
		.setMax(128)
		.setDefault(DEFAULT_LOG_SYNC_MESSAGE_MAX_SIZE);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_CHUNK_MAX_MESSAGE_SIZE, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_SIZE_MB)
		.setMin(1)
		.setMax(128)
		.setDefault(DEFAULT_CHUNK_SYNC_MESSAGE_MAX_SIZE);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_APPROXIMATE_GAP_LSN, INT32)
		.setMin(0)
		.setDefault(100);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_LOCKCONFLICT_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
		.setMin(0)
		.setDefault(30);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_APPROXIMATE_WAIT_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
		.setMin(0)
		.setDefault(10);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_LONGTERM_LIMIT_QUEUE_SIZE, INT32)
		.setMin(0)
		.setDefault(40);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_LONGTERM_HIGHLOAD_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_MS)
		.setMin(0)
		.setDefault(0);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_LONGTERM_LOG_WAIT_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_MS)
		.setMin(0)
		.setDefault(0);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_LONGTERM_DUMP_CHUNK_INTERVAL, INT32)
		.setMin(1)
		.setDefault(10000);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_LONGTERM_CHECK_INTERVAL_COUNT, INT32)
		.setDefault(1000);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_LOG_INTERRUPTION_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
		.setMin(-1)
		.setDefault(5);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_REDO_LOG_REPLICATION_TIMEOUT_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
		.setDefault(30)
		.setMax(60 * 60);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_REDO_LOG_ERROR_KEEP_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
		.setDefault(10 * 60)
		.setMax(60 * 60 * 24);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_REDO_LOG_CHECK_REPLICATION_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
		.setMin(1 * 10)
		.setDefault(60)
		.setMax(60 * 60 * 24);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_REDO_LOG_MAX_MESSAGE_SIZE, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_SIZE_B)
		.setMin(1024)
		.setMax(static_cast<int32_t>(ConfigTable::megaBytesToBytes(128)))
		.setDefault(static_cast<int32_t>(ConfigTable::megaBytesToBytes(
			DEFAULT_LOG_SYNC_MESSAGE_MAX_SIZE)));

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_REDO_LOG_RETRY_TIMEOUT_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
		.setMin(60)
		.setDefault(60 * 10)
		.setMax(60 * 60 * 24);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_REDO_LOG_RETRY_CHECK_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
		.setMin(1)
		.setDefault(10)
		.setMax(60 * 60 * 24);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_ENABLE_KEEP_LOG, BOOL)
		.setExtendedType(ConfigTable::EXTENDED_TYPE_LAX_BOOL)
		.setDefault(false);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_KEEP_LOG_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
		.setMin(0)
		.setDefault(60 * 20);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_KEEP_LOG_LSN_DIFFERENCE, INT32)
		.setDefault(1000000)
		.setMin(0);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_ENABLE_PARALLEL_SYNC, BOOL)
		.setExtendedType(ConfigTable::EXTENDED_TYPE_LAX_BOOL)
		.setDefault(false);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_MAX_PARALLEL_SYNC_NUM, INT32)
		.setMin(-1)
		.setMax(128)
		.setDefault(-1);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_MAX_PARALLEL_CHUNK_SYNC_NUM, INT32)
		.setMin(-1)
		.setMax(128)
		.setDefault(1);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_LONGTERM_CHECK_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
		.setMin(60)
		.setDefault(60 * 60 * 24);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_SYNC_POLICY, INT32)
		.setDefault(0);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_KEEP_LOG_CHECK_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
		.setMin(1)
		.setDefault(30)
		.setMax(60 * 60 * 24);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_LOG_SYNC_LSN_DIFFERENCE, INT32)
		.setMin(0)
		.setDefault(5000000);

	CONFIG_TABLE_ADD_SERVICE_ADDRESS_PARAMS(
		config, SYNC, 10020);
}

void SyncManager::Config::setUpConfigHandler(
	SyncManager* syncMgr, ConfigTable& configTable) {

	syncMgr_ = syncMgr;

	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_LONG_SYNC_TIMEOUT_INTERVAL, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_LONG_SYNC_MAX_MESSAGE_SIZE, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_APPROXIMATE_GAP_LSN, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_LOCKCONFLICT_INTERVAL, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_APPROXIMATE_WAIT_INTERVAL, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_LONGTERM_LIMIT_QUEUE_SIZE, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_LONGTERM_HIGHLOAD_INTERVAL, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_LONGTERM_LOG_WAIT_INTERVAL, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_LOG_MAX_MESSAGE_SIZE, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_CHUNK_MAX_MESSAGE_SIZE, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_LONGTERM_CHECK_INTERVAL_COUNT, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_LOG_INTERRUPTION_INTERVAL, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_REDO_LOG_REPLICATION_TIMEOUT_INTERVAL, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_REDO_LOG_ERROR_KEEP_INTERVAL, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_REDO_LOG_CHECK_REPLICATION_INTERVAL, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_REDO_LOG_MAX_MESSAGE_SIZE, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_REDO_LOG_RETRY_TIMEOUT_INTERVAL, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_REDO_LOG_RETRY_CHECK_INTERVAL, *this);

	configTable.setParamHandler(CONFIG_TABLE_SYNC_ENABLE_KEEP_LOG, *this);
	configTable.setParamHandler(CONFIG_TABLE_SYNC_KEEP_LOG_INTERVAL, *this);
	configTable.setParamHandler(CONFIG_TABLE_SYNC_KEEP_LOG_LSN_DIFFERENCE, *this);
	configTable.setParamHandler(CONFIG_TABLE_SYNC_ENABLE_PARALLEL_SYNC, *this);
	configTable.setParamHandler(CONFIG_TABLE_SYNC_MAX_PARALLEL_SYNC_NUM, *this);
	configTable.setParamHandler(CONFIG_TABLE_SYNC_MAX_PARALLEL_CHUNK_SYNC_NUM, *this);
	configTable.setParamHandler(CONFIG_TABLE_SYNC_LONGTERM_CHECK_INTERVAL, *this);
	configTable.setParamHandler(CONFIG_TABLE_SYNC_SYNC_POLICY, *this);
	configTable.setParamHandler(CONFIG_TABLE_SYNC_KEEP_LOG_CHECK_INTERVAL, *this);

}

void SyncManager::Config::operator() (ConfigTable::ParamId id, const ParamValue& value) {
	switch (id) {
	case CONFIG_TABLE_SYNC_LONG_SYNC_MAX_MESSAGE_SIZE:
		syncMgr_->getConfig().setMaxMessageSize(value.get<int32_t>());
		break;
	case CONFIG_TABLE_SYNC_APPROXIMATE_GAP_LSN:
		syncMgr_->getExtraConfig().setApproximateLsnGap(value.get<int32_t>());
		break;
	case CONFIG_TABLE_SYNC_LOCKCONFLICT_INTERVAL:
		syncMgr_->getExtraConfig().setLockWaitInterval(
			CommonUtility::changeTimeSecondToMilliSecond(value.get<int32_t>()));
		break;
	case CONFIG_TABLE_SYNC_APPROXIMATE_WAIT_INTERVAL:
		syncMgr_->getExtraConfig().setApproximateWaitInterval(
			CommonUtility::changeTimeSecondToMilliSecond(value.get<int32_t>()));
		break;
	case CONFIG_TABLE_SYNC_LONGTERM_LIMIT_QUEUE_SIZE:
		syncMgr_->getExtraConfig().setLimitLongtermQueueSize(value.get<int32_t>());
		break;
	case CONFIG_TABLE_SYNC_LONGTERM_HIGHLOAD_INTERVAL:
		syncMgr_->getExtraConfig().setLongtermHighLoadInterval(value.get<int32_t>());
		break;
	case CONFIG_TABLE_SYNC_LONGTERM_LOG_WAIT_INTERVAL:
		syncMgr_->getExtraConfig().setLongtermLogWaitInterval(value.get<int32_t>());
		break;
	case CONFIG_TABLE_SYNC_LONGTERM_DUMP_CHUNK_INTERVAL:
		syncMgr_->getExtraConfig().setLongtermDumpInterval(value.get<int32_t>());
		break;
	case CONFIG_TABLE_SYNC_LOG_MAX_MESSAGE_SIZE:
		syncMgr_->getConfig().setMaxMessageSize(static_cast<int32_t>(
			ConfigTable::megaBytesToBytes(value.get<int32_t>())));
		break;
	case CONFIG_TABLE_SYNC_CHUNK_MAX_MESSAGE_SIZE:
		syncMgr_->getConfig().setMaxChunkMessageSize(static_cast<int32_t>(
			ConfigTable::megaBytesToBytes(value.get<int32_t>())));
		break;
	case CONFIG_TABLE_SYNC_LONGTERM_CHECK_INTERVAL_COUNT:
		syncMgr_->getConfig().setLongtermCheckInterval(value.get<int32_t>());
		break;
	case CONFIG_TABLE_SYNC_LOG_INTERRUPTION_INTERVAL:
		syncMgr_->getConfig().setLogInterruptionInterval(value.get<int32_t>());
		break;
	case CONFIG_TABLE_SYNC_REDO_LOG_REPLICATION_TIMEOUT_INTERVAL:
		syncMgr_->getConfig().setLogReplicationTimeoutInterval(value.get<int32_t>());
		break;
	case CONFIG_TABLE_SYNC_REDO_LOG_ERROR_KEEP_INTERVAL:
		syncMgr_->getConfig().setLogErrorKeepInterval(value.get<int32_t>());
		break;
	case CONFIG_TABLE_SYNC_REDO_LOG_MAX_MESSAGE_SIZE:
		syncMgr_->getConfig().setMaxRedoMessageSize(static_cast<int32_t>(
			ConfigTable::megaBytesToBytes(value.get<int32_t>())));
		break;
	case CONFIG_TABLE_SYNC_REDO_LOG_RETRY_TIMEOUT_INTERVAL:
		syncMgr_->getConfig().setRetryTimeoutInterval(value.get<int32_t>());
		break;
	case CONFIG_TABLE_SYNC_REDO_LOG_RETRY_CHECK_INTERVAL:
		syncMgr_->getConfig().setRetryCheckInterval(value.get<int32_t>());
		break;
	case CONFIG_TABLE_SYNC_KEEP_LOG_INTERVAL:
	{
		syncMgr_->getConfig().setKeepLogInterval(value.get<int32_t>());
		syncMgr_->getClusterManager()->setConfigurationChange();
		break;
	}
	case CONFIG_TABLE_SYNC_KEEP_LOG_LSN_DIFFERENCE:
	{
		syncMgr_->getConfig().setKeepLogLsnDifference(value.get<int32_t>());
		syncMgr_->getClusterManager()->setConfigurationChange();
		break;
	}
	case CONFIG_TABLE_SYNC_LOG_SYNC_LSN_DIFFERENCE:
	{
		syncMgr_->getPartitionTable()->setLogSyncLsnDifference(value.get<int32_t>());
		break;
	}
	case CONFIG_TABLE_SYNC_ENABLE_PARALLEL_SYNC:
	{
		syncMgr_->adjustParallelSyncParam(
			value.get<bool>(), syncMgr_->getConfig().getParallelSyncNum(),
			syncMgr_->getConfig().getParallelChunkSyncNum());
		break;
	}
	case CONFIG_TABLE_SYNC_MAX_PARALLEL_SYNC_NUM:
	{
		syncMgr_->adjustParallelSyncParam(
			syncMgr_->getConfig().enableParallelSync(),
			value.get<int32_t>(), syncMgr_->getConfig().getParallelChunkSyncNum());
		break;
	}
	case CONFIG_TABLE_SYNC_MAX_PARALLEL_CHUNK_SYNC_NUM:
	{
		syncMgr_->adjustParallelSyncParam(
			syncMgr_->getConfig().enableParallelSync(),
			syncMgr_->getConfig().getParallelSyncNum(), value.get<int32_t>());
		break;
	}
	case CONFIG_TABLE_SYNC_LONGTERM_CHECK_INTERVAL:
		syncMgr_->getConfig().setLongtermCheckInterval(value.get<int32_t>());
		break;
	}
}

void SyncManager::resetCurrentSyncId(SyncContext* context) {
	if (context->getPartitionRevision().getRevisionNo() >= currentSyncEntry_.ptRev_.getRevisionNo()) {
		currentSyncEntry_.reset();
	}
	syncEntries_.reset(context->pId_);
}

void SyncManager::checkCurrentContext(EventContext& ec, PartitionId pId, PartitionRevision& revision) {
	PartitionId checkPId = pId;
	SyncId syncId;
	PartitionRevision ptRev;
	if (!syncConfig_.enableParallelSync()) {
		getCurrentSyncId(checkPId, syncId);
	}
	else {
		syncId = getCurrentSyncId(pId);
		if (!syncId.isValid()) {
			checkPId = UNDEF_PARTITIONID;
		}
		else {
			checkPId = pId;
		}
	}
	if (checkPId != UNDEF_PARTITIONID && syncContextTables_[checkPId] != NULL) {
		SyncContext* targetContext = syncContextTables_[checkPId]->getSyncContext(syncId.contextId_, syncId.contextVersion_);
		if (targetContext != NULL && !targetContext->isCatchupSync()) {
			if (revision.getRevisionNo() < targetContext->getPartitionRevision().getRevisionNo()) {
				GS_THROW_SYNC_ERROR(GS_ERROR_SYM_INVALID_PARTITION_REVISION, targetContext,
					"Target longterm sync is old", "latest revision=" << targetContext->getPartitionRevision().getRevisionNo()
					<< ", current revision=" << revision.getRevisionNo());
			}
			targetContext->setCatchupSync(false);
			Event syncCheckEndEvent(ec, TXN_SYNC_TIMEOUT, checkPId);
			SyncCheckEndInfo syncCheckEndInfo(ec.getAllocator(), TXN_SYNC_TIMEOUT,
				this, pId, MODE_LONGTERM_SYNC, targetContext);
			EventByteOutStream out = syncCheckEndEvent.getOutStream();
			syncSvc_->encode(syncCheckEndEvent, syncCheckEndInfo, out);
			txnSvc_->getEE()->addTimer(syncCheckEndEvent, 0);
		}
	}
}

void SyncContext::startAll() {
	watch_.reset();
	watch_.start();
	if (getSyncMode() == MODE_LONGTERM_SYNC) {
		GS_TRACE_SYNC(this, "SYNC_START", ", " << dump(0));
	}
	else {
		GS_TRACE_INFO(SYNC_DETAIL, GS_TRACE_SYNC_OPERATION, "SYNC_START, " << dump(0));
	}
}

int32_t SyncManager::getTransactionEEQueueSize(
	PartitionId pId) {

	EventEngine::Stats stats;
	if (txnSvc_->getEE()->getStats(
		pt_->getPartitionGroupId(pId), stats)) {
		return static_cast<int32_t>(
			stats.get(
				EventEngine::Stats::EVENT_ACTIVE_QUEUE_SIZE_CURRENT));
	}
	else {
		return 0;
	}
}

bool SyncManager::controlSyncLoad(
	EventContext& ec, Event& ev, PartitionId pId, LogSequentialNumber lsn, int32_t& waitTime) {
	if (ev.getType() == TXN_LONGTERM_SYNC_LOG_ACK) {
		if (pt_->getLSN(pId) - lsn < static_cast<LogSequentialNumber>(getExtraConfig().getApproximateGapLsn())) {
			waitTime = getExtraConfig().getApproximateWaitInterval();
			return false;
		}
	}
	int64_t executableCount = 0;
	int64_t afterCount = 0;
	waitTime = txnSvc_->getWaitTime(ec, &ev, 0, 0, executableCount, afterCount, SYNC_EXEC);
	return true;
}

void SyncManager::checkActiveStatus() {
	const StatementHandler::ClusterRole clusterRole =
		(StatementHandler::CROLE_MASTER
			| StatementHandler::CROLE_FOLLOWER);
	try {
		StatementHandler::checkExecutable(clusterRole, pt_);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
}

bool SyncContext::checkLongTermSync(CheckpointService* cpSvc, int32_t checkInterval) {
	if (!checkLongTermSyncCore(cpSvc, checkInterval)) {
		GS_TRACE_SYNC(this, "Detected long term sync failure;", ", " << dump(2));
		pt_->clearCatchupRole(pId_);
		pt_->setErrorStatus(pId_, PartitionTable::PT_ERROR_LONG_SYNC_FAIL);
		return false;
	}
	return true;
}

bool SyncContext::checkLongTermSyncCore(CheckpointService* cpSvc, int32_t checkInterval) {
	if (checkInterval == 0) {
		return true;
	}
	if (prevEventType_ != eventType_) {
		stateStartTime_ = watch_.elapsedMillis();
		prevEventType_ = eventType_;
		return true;
	}
	if (isExecuteCheckpoint()) {
		if (!cpSvc->getCpLongtermSyncInfo(syncSequentialNumber_)) {
			return false;
		}
	}
	switch (eventType_) {
	case TXN_LONGTERM_SYNC_START:
		return true;
	case TXN_LONGTERM_SYNC_START_ACK:
		if (isExecuteCheckpoint()) {
			stateStartTime_ = watch_.elapsedMillis();
			return true;
		}
		break;
	case TXN_LONGTERM_SYNC_CHUNK_ACK:
	case TXN_LONGTERM_SYNC_CHUNK:
		if (isExecuteCheckpoint()) {
			stateStartTime_ = watch_.elapsedMillis();
			return true;
		}
		if (eventType_ == TXN_LONGTERM_SYNC_CHUNK_ACK && isSyncCheckpointCompleted()) {
			stateStartTime_ = watch_.elapsedMillis();
			return true;
		}
		if (prevProcessedChunkNum_ != processedChunkNum_) {
			prevProcessedChunkNum_ = processedChunkNum_;
			stateStartTime_ = watch_.elapsedMillis();
			return true;
		}
		break;
	case TXN_LONGTERM_SYNC_LOG:
	case TXN_LONGTERM_SYNC_LOG_ACK:
		if (prevProcessedLogNum_ != processedLogNum_) {
			prevProcessedLogNum_ = processedLogNum_;
			stateStartTime_ = watch_.elapsedMillis();
			return true;
		}
		break;
	case TXN_LONGTERM_SYNC_PREPARE_ACK:
	case TXN_LONGTERM_SYNC_RECOVERY_ACK:
		break;
	}

	int64_t current = watch_.elapsedMillis();
	if (stateStartTime_ != -1 && (current - stateStartTime_) > checkInterval) {
		return false;
	}
	return true;
}

SearchLogInfo& SyncContext::getSearchLogInfo(NodeId nodeId) {
	std::map<NodeId, SearchLogInfo>::iterator it = searchLogInfos_.find(nodeId);
	if (it == searchLogInfos_.end()) {
		searchLogInfos_.insert(std::make_pair(nodeId, SearchLogInfo()));
		return searchLogInfos_[nodeId];
	}
	else {
		return (*it).second;
	}
}

void SyncManager::adjustParallelSyncParam(bool enableParallel, int32_t parallelNum, int32_t chunkParallelNum) {
	const PartitionGroupConfig& pgConfig = getTransactionManager()->getPartitionGroupConfig();
	int32_t currentParallelNum = parallelNum;
	int32_t currentChunkParallelNum = chunkParallelNum;
	int32_t concurrency = pgConfig.getPartitionGroupCount();
	syncConfig_.setParallelSync(enableParallel);
	if (enableParallel) {
		if (parallelNum <= 0 || parallelNum > concurrency) {
			currentParallelNum = concurrency;
		}
		syncConfig_.setParallelSyncNum(currentParallelNum);
		if (chunkParallelNum == -1 || currentChunkParallelNum > concurrency) {
			currentChunkParallelNum = concurrency;
		}
		if (currentParallelNum < currentChunkParallelNum) {
			currentChunkParallelNum = currentParallelNum;
		}
		syncConfig_.setParallelChunkSyncNum(currentChunkParallelNum);
		pt_->setParallelSyncNum(currentParallelNum);
		pt_->setParallelChunkSyncNum(currentChunkParallelNum);
	}
	else {
		pt_->setParallelSyncNum(1);
		pt_->setParallelChunkSyncNum(1);
	}
}

SyncManager::SyncConfig::SyncConfig(const ConfigTable& config) :
	syncTimeoutInterval_(
		CommonUtility::changeTimeSecondToMilliSecond(config.get<int32_t>(CONFIG_TABLE_SYNC_TIMEOUT_INTERVAL))),
	maxMessageSize_(
		static_cast<int32_t>(config.get<int32_t>(CONFIG_TABLE_SYNC_LONG_SYNC_MAX_MESSAGE_SIZE))),
	sendChunkSizeLimit_(
		static_cast<int32_t>(ConfigTable::megaBytesToBytes(config.get<int32_t>(CONFIG_TABLE_SYNC_CHUNK_MAX_MESSAGE_SIZE)))),
	blockSize_((config.get<int32_t>(CONFIG_TABLE_DS_STORE_BLOCK_SIZE))),
	parallelSyncNum_(config.get<int32_t>(CONFIG_TABLE_SYNC_MAX_PARALLEL_SYNC_NUM)),
	parallelChunkSyncNum_(config.get<int32_t>(CONFIG_TABLE_SYNC_MAX_PARALLEL_CHUNK_SYNC_NUM)),
	enableParallelSync_(config.get<bool>(CONFIG_TABLE_SYNC_ENABLE_PARALLEL_SYNC)),
	syncPolicy_(config.get<int32_t>(CONFIG_TABLE_SYNC_SYNC_POLICY)) {
	if (config.get<int32_t>(CONFIG_TABLE_SYNC_LONG_SYNC_MAX_MESSAGE_SIZE) ==
		static_cast<int32_t>(ConfigTable::megaBytesToBytes(DEFAULT_LOG_SYNC_MESSAGE_MAX_SIZE))) {
		maxMessageSize_ = 
			(static_cast<int32_t>(ConfigTable::megaBytesToBytes(
				config.get<int32_t>(CONFIG_TABLE_SYNC_LOG_MAX_MESSAGE_SIZE))));
	}
	sendChunkNum_ = (sendChunkSizeLimit_ / blockSize_) + 1;
	setLogInterruptionInterval(CONFIG_TABLE_SYNC_LOG_INTERRUPTION_INTERVAL);
	setLogReplicationTimeoutInterval(
		config.get<int32_t>(CONFIG_TABLE_SYNC_REDO_LOG_REPLICATION_TIMEOUT_INTERVAL));
	setLogErrorKeepInterval(
		config.get<int32_t>(CONFIG_TABLE_SYNC_REDO_LOG_ERROR_KEEP_INTERVAL));
	setReplicationCheckInterval(
		config.get<int32_t>(CONFIG_TABLE_SYNC_REDO_LOG_CHECK_REPLICATION_INTERVAL));
	setRetryTimeoutInterval(
		config.get<int32_t>(CONFIG_TABLE_SYNC_REDO_LOG_RETRY_TIMEOUT_INTERVAL));
	setRetryCheckInterval(
		config.get<int32_t>(CONFIG_TABLE_SYNC_REDO_LOG_RETRY_CHECK_INTERVAL));
	setKeepLogInterval(config.get<int32_t>(CONFIG_TABLE_SYNC_KEEP_LOG_INTERVAL));
	setKeepLogLsnDifference(config.get<int32_t>(CONFIG_TABLE_SYNC_KEEP_LOG_LSN_DIFFERENCE));
	setLongtermCheckInterval(config.get<int32_t>(CONFIG_TABLE_SYNC_LONGTERM_CHECK_INTERVAL));
	if (getLogErrorKeepInterval() <= getReplicationCheckInterval()) {
		setReplicationCheckInterval(
			config.get<int32_t>(CONFIG_TABLE_SYNC_REDO_LOG_ERROR_KEEP_INTERVAL));
	}
	maxRedoMessageSize_ =
		static_cast<int32_t>(config.get<int32_t>(CONFIG_TABLE_SYNC_REDO_LOG_MAX_MESSAGE_SIZE));
}
