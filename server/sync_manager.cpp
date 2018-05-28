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
#include "cluster_manager.h"
#include <iostream>
#include "cluster_service.h"
#include "checkpoint_service.h"
#include "sync_service.h"
#include "transaction_service.h"

UTIL_TRACER_DECLARE(CLUSTER_OPERATION);
UTIL_TRACER_DECLARE(SYNC_SERVICE);
UTIL_TRACER_DECLARE(SYNC_DETAIL);

#define TRACE_REVISION(rev1, rev2) \
		"next=" << rev1.toString() << ", current=" << rev2.toString()

const int32_t SyncManager::DEFAULT_LOG_SYNC_MESSAGE_MAX_SIZE = 8;

SyncManager::SyncManager(const ConfigTable &configTable, PartitionTable *pt)
	: syncOptStat_(pt->getPartitionNum()),
	fixedSizeAlloc_(
			util::AllocatorInfo(ALLOCATOR_GROUP_CS, "syncManagerFixed"), 1 << 18),
	alloc_(
			util::AllocatorInfo(ALLOCATOR_GROUP_CS, "syncManagerStack"), &fixedSizeAlloc_),
	varSizeAlloc_(util::AllocatorInfo(ALLOCATOR_GROUP_CS, "syncManagerVar")),
	syncContextTables_(NULL), pt_(pt), syncConfig_(configTable), extraConfig_(configTable),
	chunkBufferList_(NULL), currentSyncPId_(UNDEF_PARTITIONID),
	syncSequentialNumber_(0)
	, longSyncEntryManager_(alloc_, pt->getPartitionNum()), cpSvc_(NULL)
	, syncSvc_(NULL), txnSvc_(NULL)
{
	try {
		const uint32_t partitionNum = configTable.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM);
		if (partitionNum <= 0 || partitionNum != pt->getPartitionNum()) {
			GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_INFO, "");
		}
		syncContextTables_ = ALLOC_NEW(alloc_) SyncContextTable * [partitionNum];
		for (PartitionId pId = 0; pId < partitionNum; pId++) {
			syncContextTables_[pId] = ALLOC_NEW(alloc_) SyncContextTable(
					alloc_, pId, DEFAULT_CONTEXT_SLOT_NUM, &varSizeAlloc_);
		}
		chunkSize_ = configTable.getUInt32(CONFIG_TABLE_DS_STORE_BLOCK_SIZE);
		recoveryLevel_ = configTable.get<int32_t>(CONFIG_TABLE_DS_RECOVERY_LEVEL);
		syncMode_ = SYNC_MODE_NORMAL;
		undoPartitionList_.assign(partitionNum, 0);
		chunkBufferList_ = ALLOC_NEW(alloc_) uint8_t [
				chunkSize_ * configTable.getUInt32(CONFIG_TABLE_DS_CONCURRENCY)];
		ConfigTable *tmpTable = const_cast<ConfigTable*>(&configTable);
		config_.setUpConfigHandler(this, *tmpTable);
	}
	catch (std::exception &e) {
		ALLOC_DELETE(alloc_, chunkBufferList_);
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

SyncManager::~SyncManager() {
	ALLOC_DELETE(alloc_, chunkBufferList_);
	for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
		if (syncContextTables_[pId]) {
			ALLOC_DELETE(alloc_, syncContextTables_[pId]);
		}
	}
}

void SyncManager::initialize(ManagerSet *mgrSet) {
	cpSvc_ = mgrSet->cpSvc_;
	syncSvc_ = mgrSet->syncSvc_;
	txnSvc_ = mgrSet->txnSvc_;
}

/*!
	@brief Creates SyncContext
*/
SyncContext *SyncManager::createSyncContext(EventContext &ec, PartitionId pId,
		PartitionRevision &ptRev, SyncMode syncMode, PartitionRoleStatus roleStatus) {
	try {
		util::LockGuard<util::WriteLock> guard(getAllocLock());
		if (pId >= pt_->getPartitionNum()) {
			return NULL;
		}
		createPartition(pId);
		getSyncOptStat()->setContext(pId);
		SyncContext *context =  syncContextTables_[pId]->createSyncContext(ptRev);
		context->setSyncMode(syncMode, roleStatus);

//		int64_t syncSequentialNumber = syncSequentialNumber_;
//		bool isReuse = false;
		bool isOwner = (roleStatus == PartitionTable::PT_OWNER);
		bool longtermSyncCheck 
				= (syncMode == MODE_LONGTERM_SYNC && isOwner);
		if (syncMode == MODE_LONGTERM_SYNC) {
			checkCurrentContext(ec, pId, true);
			checkCurrentContext(ec, pId, false);
		}
		context->setSequentialNumber(syncSequentialNumber_);
		syncSequentialNumber_++;
		if (syncMode == MODE_LONGTERM_SYNC) {
			longSyncEntryManager_.setCurrentSyncId(pId, context, ptRev);
		}
		if (longtermSyncCheck) {		
			LongtermSyncInfo syncInfo(context->getId(), context->getVersion(), context->getSequentialNumber());
			cpSvc_->requestStartCheckpointForLongtermSync(ec, pId, &syncInfo);
		}
		return context;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Gets SyncContext
*/
SyncContext *SyncManager::getSyncContext(PartitionId pId, SyncId &syncId) {
	try {
		if (!syncId.isValid()) {
			return NULL;
		}
		util::LockGuard<util::ReadLock> guard(getAllocLock());
		if (pId >= pt_->getPartitionNum()) {
			return NULL;
		}
		if (syncContextTables_[pId] == NULL || !syncId.isValid()) {
			return NULL;
		}
		return syncContextTables_[pId]->getSyncContext(
			syncId.contextId_, syncId.contextVersion_);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Removes SyncContext
*/
void SyncManager::removeSyncContext(EventContext &ec, PartitionId pId, SyncContext *&context) {
	try {
		util::LockGuard<util::WriteLock> guard(getAllocLock());
		if (pId >= pt_->getPartitionNum()) {
			return;
		}
		if (syncContextTables_[pId] == NULL) {
			return;
		}
		getSyncOptStat()->freeContext(pId);
		bool longtermSyncCheck 
				= (context->getSyncMode() == MODE_LONGTERM_SYNC
					&& context->getPartitionRoleStatus() == PartitionTable::PT_OWNER);
		if (longtermSyncCheck) {
			int64_t checkId = longSyncEntryManager_.getEntry(pId).syncSequentialNumber_;
			if (checkId > 0) {
				cpSvc_->requestStopCheckpointForLongtermSync(ec, pId, checkId);
			}
		}
		if (context->getSyncMode() == MODE_LONGTERM_SYNC) {
			longSyncEntryManager_.resetCurrentSyncId(pId,
				context->getPartitionRoleStatus() == PartitionTable::PT_OWNER);
		}
		context->endCheck();
		if (context->checkTotalTime()) {
			GS_TRACE_WARNING(
					SYNC_DETAIL, GS_TRACE_SYNC_OPERATION, "SYNC_END, " << context->dump(2));
		}
		else {
			GS_TRACE_INFO(
					SYNC_DETAIL, GS_TRACE_SYNC_OPERATION, "SYNC_END, " << context->dump(2));
		}
		syncContextTables_[pId]->removeSyncContext(varSizeAlloc_, context, getSyncOptStat());
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Removes Partition
*/
void SyncManager::removePartition(PartitionId pId) {
	if (pId >= pt_->getPartitionNum()) {
		GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_INFO, "");
	}
	pt_->execDrop(pId);
}

/*!
	@brief Checks if a specified operation is executable
*/
void SyncManager::checkExecutable(
	SyncOperationType operation, PartitionId pId, PartitionRole &candNextRole) {
	try {
		if (pId >= pt_->getPartitionNum()) {
			GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_INFO,"");
		}
		if (operation == OP_SYNC_TIMEOUT) {
			return;
		}
		if (pt_->isSubMaster()) {
			GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_CLUSTER_INFO, "");
		}
		if (pt_->getPartitionStatus(pId) == PartitionTable::PT_STOP) {
			GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_STATUS, "");
		}

		PartitionRole currentRole;
		PartitionRole nextRole;
		switch (operation) {
		case OP_SHORTTERM_SYNC_REQUEST: {
			pt_->getPartitionRole(pId, nextRole, PartitionTable::PT_NEXT_OB);
			pt_->getPartitionRole(pId, currentRole, PartitionTable::PT_CURRENT_OB);
			if (!currentRole.isOwner() && !candNextRole.isOwner()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_ROLE, "");
			}
			if (nextRole.getRevision() > candNextRole.getRevision()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_REVISION,
						TRACE_REVISION(nextRole.getRevision(), candNextRole.getRevision()));
			}
			break;
		}
		case OP_SHORTTERM_SYNC_START: {
			pt_->getPartitionRole(pId, nextRole, PartitionTable::PT_NEXT_OB);
			if (nextRole.getRevision() > candNextRole.getRevision()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_REVISION,
						TRACE_REVISION(nextRole.getRevision(), candNextRole.getRevision()));
			}
			break;
		}
		case OP_SHORTTERM_SYNC_START_ACK:
		case OP_SHORTTERM_SYNC_LOG_ACK:
		case OP_SHORTTERM_SYNC_END_ACK: {
			pt_->getPartitionRole(pId, nextRole, PartitionTable::PT_NEXT_OB);
			pt_->getPartitionRole(pId, currentRole, PartitionTable::PT_CURRENT_OB);
			if (!currentRole.isOwner() && !nextRole.isOwner()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_ROLE, "");
			}
			if (nextRole.getRevision() != candNextRole.getRevision()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_REVISION,
						TRACE_REVISION(nextRole.getRevision(), candNextRole.getRevision()));
			}
			if (pt_->getPartitionStatus(pId) == PartitionTable::PT_ON) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_ROLE, "");
			}
			break;
		}
		case OP_SHORTTERM_SYNC_LOG:
		case OP_SHORTTERM_SYNC_END: {
			pt_->getPartitionRole(pId, nextRole, PartitionTable::PT_NEXT_OB);
			if (nextRole.getRevision() != candNextRole.getRevision()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_REVISION,
						TRACE_REVISION(nextRole.getRevision(), candNextRole.getRevision()));
			}
			break;
		}
		case OP_LONGTERM_SYNC_REQUEST: {
			if (!candNextRole.isOwner()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_ROLE, "");
			}
			pt_->getPartitionRole(pId, nextRole, PartitionTable::PT_NEXT_GOAL);
			if (nextRole.getRevision() > candNextRole.getRevision()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_REVISION,
						TRACE_REVISION(nextRole.getRevision(), candNextRole.getRevision()));
			}
			break;
		}
		case OP_LONGTERM_SYNC_START: {
			if (!candNextRole.isBackup()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_ROLE, "");
			}
			pt_->getPartitionRole(pId, nextRole, PartitionTable::PT_NEXT_GOAL);
			if (nextRole.getRevision() > candNextRole.getRevision()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_REVISION,
						TRACE_REVISION(nextRole.getRevision(), candNextRole.getRevision()));
			}
			break;
		}
		case OP_LONGTERM_SYNC_START_ACK:
		case OP_LONGTERM_SYNC_CHUNK_ACK:
		case OP_LONGTERM_SYNC_LOG_ACK: {
			pt_->getPartitionRole(pId, nextRole, PartitionTable::PT_NEXT_GOAL);
			if (!nextRole.isOwner()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_ROLE, "");
			}
			if (nextRole.getRevision() != candNextRole.getRevision()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_REVISION,
						TRACE_REVISION(nextRole.getRevision(), candNextRole.getRevision()));
			}
			break;
		}
		case OP_LONGTERM_SYNC_LOG:
		case OP_LONGTERM_SYNC_CHUNK: {
			pt_->getPartitionRole(pId, nextRole, PartitionTable::PT_NEXT_GOAL);
			if (!nextRole.isBackup()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_ROLE, "");
			}
			if (nextRole.getRevision() != candNextRole.getRevision()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_REVISION,
						TRACE_REVISION(nextRole.getRevision(), candNextRole.getRevision()));
			}
			pt_->getPartitionRole(pId, currentRole);
			if (currentRole.isOwner()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_ROLE, "");
			}
			break;
		}
		case OP_DROP_PARTITION: {
			if (!pt_->checkDrop(pId)) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_INFO, "");
			}
		}
		default: { break; }
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, GS_EXCEPTION_MERGE_MESSAGE(
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
		syncContextTables_[pId] = ALLOC_NEW(alloc_) SyncContextTable(
				alloc_, pId, DEFAULT_CONTEXT_SLOT_NUM, &varSizeAlloc_);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, GS_EXCEPTION_MERGE_MESSAGE(
				e, "Failed to sync: pId=" << pId));
	}
}


SyncManager::SyncContextTable::SyncContextTable(util::StackAllocator &alloc,
		PartitionId pId, uint32_t numInitialSlot, SyncVariableSizeAllocator *varSizeAlloc)
	: pId_(pId), numCounter_(0), freeList_(NULL), numUsed_(0), alloc_(&alloc),
			slots_(alloc), varSizeAlloc_(varSizeAlloc) {
	try {
		for (uint32_t i = 0; i < numInitialSlot; i++) {
			SyncContext *slot = ALLOC_NEW(alloc) SyncContext [SLOT_SIZE];
			slots_.push_back(slot);
			for (uint32_t j = 0; j < SLOT_SIZE; j++) {
				slot[j].setPartitionId(pId_);
				slot[j].setId(numCounter_++);
				slot[j].nextEmptyChain_ = freeList_;
				freeList_ = &slot[j];
			}
		}
	}
	catch (std::exception &e) {
		for (size_t i = 0; i < slots_.size(); i++) {
			for (uint32_t j = 0; j < SLOT_SIZE; j++) {
				ALLOC_DELETE(alloc, &slots_[i][j]);
			}
			ALLOC_DELETE(alloc, slots_[i]);
		}
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

SyncManager::SyncContextTable::~SyncContextTable() {
	for (size_t i = 0; i < slots_.size(); i++) {
		for (uint32_t j = 0; j < SLOT_SIZE; j++) {
			slots_[i][j].clear(*varSizeAlloc_, NULL);
			ALLOC_DELETE(*alloc_, &slots_[i][j]);
		}
		ALLOC_DELETE(*alloc_, slots_[i]);
	}
}

/*!
	@brief Creates SyncContext
*/
SyncContext *SyncManager::SyncContextTable::createSyncContext(
	PartitionRevision &ptRev) {
	try {
		SyncContext *context = freeList_;
		if (context == NULL) {
			SyncContext *slot = ALLOC_NEW(*alloc_) SyncContext [SLOT_SIZE];
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
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Removes SyncContext
*/
void SyncManager::SyncContextTable::removeSyncContext(
		SyncVariableSizeAllocator &alloc, SyncContext *&context, SyncOptStat *stat) {
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
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Gets SyncContext
*/
SyncContext *SyncManager::SyncContextTable::getSyncContext(
		int32_t id, uint64_t version) const {
	if (id < numCounter_) {
		const uint32_t slotNo = id / SLOT_SIZE;
		const uint32_t offset = id - SLOT_SIZE * slotNo;
		SyncContext *slot = slots_[slotNo];
		if (slot[offset].version_ == version && slot[offset].used_) {
			return &slot[offset];
		}
	}
	return NULL;
}


SyncContext::SyncContext() : id_(0), pId_(0), version_(0), used_(false),
		numSendBackup_(0), nextStmtId_(0), cpId_(UNDEF_CHECKPOINT_ID),
		recvNodeId_(UNDEF_NODEID), isSyncCpCompleted_(false),
		nextEmptyChain_(NULL), isDownNextOwner_(false), processedChunkNum_(0),
		totalChunkNum_(0), logBuffer_(NULL), logBufferSize_(0), chunkBuffer_(NULL),
		chunkBufferSize_(0), chunkBaseSize_(0), chunkNum_(0), chunkNo_(0),
		status_(PartitionTable::PT_OFF), queueSize_(0)
		, mode_(MODE_SHORTTERM_SYNC), roleStatus_(PartitionTable::PT_OWNER)
		, processedLogNum_(0), processedLogSize_(0), actualLogTime_(0)
		, actualChunkTime_(0), chunkLeadTime_(0), totalTime_(0)
		, startLsn_(0), endLsn_(0), syncSequentialNumber_(0), globalSequentialNumber_(0)
		, notifyLongSync_(false)
{
}
SyncContext::~SyncContext() {
}

/*!
	@brief Increments counter
*/
void SyncContext::incrementCounter(NodeId syncTargetNodeId) {
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
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Decrements counter
*/
bool SyncContext::decrementCounter(NodeId syncTargetNodeId) {
//	uint32_t before = numSendBackup_;
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
	@brief Gets id list of nodes under synchronization
*/
void SyncContext::getSyncCompleteNodeIds(
		std::vector<NodeId> &syncCompleteNodeIds, bool isComplete) {
	try {
		for (size_t i = 0; i < sendBackups_.size(); i++) {
			if (isComplete) {
				if (sendBackups_[i].isAcked_) {
					syncCompleteNodeIds.push_back(sendBackups_[i].nodeId_);
				}
			}
			else {
				if (!sendBackups_[i].isAcked_) {
					syncCompleteNodeIds.push_back(sendBackups_[i].nodeId_);
				}
			}
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Sets LSN of target node id
*/
void SyncContext::setSyncTargetLsn(
	NodeId syncTargetNodeId, LogSequentialNumber lsn) {
	for (size_t i = 0; i < sendBackups_.size(); i++) {
		if (sendBackups_[i].nodeId_ == syncTargetNodeId) {
			sendBackups_[i].lsn_ = lsn;
			break;
		}
	}
}

/*!
	@brief Sets LSN and SyncID of target node id
*/
void SyncContext::setSyncTargetLsnWithSyncId(
		NodeId syncTargetNodeId, LogSequentialNumber lsn, SyncId backupSyncId) {
	for (size_t i = 0; i < sendBackups_.size(); i++) {
		if (sendBackups_[i].nodeId_ == syncTargetNodeId) {
			sendBackups_[i].lsn_ = lsn;
			sendBackups_[i].backupSyncId_ = backupSyncId;
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

/*!
	@brief Gets LSN and SyncID of target node id
*/
bool SyncContext::getSyncTargetLsnWithSyncId(NodeId syncTargetNodeId,
	LogSequentialNumber &backupLsn, SyncId &backupSyncId) {
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
void SyncContext::copyLogBuffer(SyncVariableSizeAllocator &alloc,
	const uint8_t *logBuffer, int32_t logBufferSize, SyncOptStat *stat) {
	try {
		if (logBuffer == NULL || logBufferSize == 0) {
			return;
		}
		logBuffer_ = static_cast<uint8_t *>(alloc.allocate(logBufferSize));
		if (stat != NULL) {
			stat->statAllocate(pId_, logBufferSize);
		}
		memcpy(logBuffer_, logBuffer, logBufferSize);
		logBufferSize_ = logBufferSize;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Copies chunk buffer
*/
void SyncContext::copyChunkBuffer(SyncVariableSizeAllocator &alloc,
	const uint8_t *chunkBuffer, int32_t chunkSize, int32_t chunkNum,
	SyncOptStat *stat) {
	try {
		if (chunkBuffer == NULL || chunkSize == 0 || chunkNum == 0) {
			return;
		}
		int32_t allocSize = chunkSize * chunkNum;
		chunkBuffer_ = static_cast<uint8_t *>(alloc.allocate(allocSize));
		if (stat != NULL) {
			stat->statAllocate(pId_, allocSize);
		}
		memcpy(chunkBuffer_, chunkBuffer, allocSize);
		chunkBaseSize_ = chunkSize;
		chunkNum_ = chunkNum;
		chunkBufferSize_ = allocSize;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Deallocate buffer
*/
void SyncContext::freeBuffer(
	SyncVariableSizeAllocator &alloc, SyncType syncType, SyncOptStat *stat) {
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
			GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_SYNC_TYPE, "");
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Gets log buffer
*/
void SyncContext::getLogBuffer(uint8_t *&logBuffer, int32_t &logBufferSize) {
	logBuffer = logBuffer_;
	logBufferSize = logBufferSize_;
}

/*!
	@brief Gets chunk buffer
*/
void SyncContext::getChunkBuffer(uint8_t *&chunkBuffer, int32_t chunkNo) {
	chunkBuffer = NULL;
	if (chunkBuffer_ == NULL || chunkNo >= chunkNum_ ||
		chunkBufferSize_ < chunkBaseSize_ * chunkNo) {
		return;
	}
	chunkBuffer = static_cast<uint8_t *>(chunkBuffer_ + (chunkBaseSize_ * chunkNo));
}

void SyncContext::clear(SyncVariableSizeAllocator &alloc, SyncOptStat *stat) {
	try {
		sendBackups_.clear();
		numSendBackup_ = 0;
		nextStmtId_ = 0;
		cpId_ = UNDEF_CHECKPOINT_ID;
		isSyncCpCompleted_ = false;
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
		totalChunkNum_ = 0;
		logBufferSize_ = 0;
		chunkBufferSize_ = 0;
		chunkBaseSize_ = 0;
		chunkNum_ = 0;
		chunkNo_ = 0;
		isDownNextOwner_ = false;
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
		globalSequentialNumber_ = 0;
		notifyLongSync_ = false;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Gets number of active contexts
*/
int32_t SyncManager::getActiveContextNum() {
	int32_t activeCount = 0;
	for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
		if (syncContextTables_[pId] != NULL) {
			activeCount += syncContextTables_[pId]->numUsed_;
		}
	}
	return activeCount;
}

std::string SyncManager::dumpAll() {
	util::NormalOStringStream ss;
	ss << "{";
	for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
		dump(pId);
	}
	ss << "}";
	return ss.str();
}

std::string SyncManager::dump(PartitionId pId) {
	util::NormalOStringStream ss;
	if (syncContextTables_[pId] != NULL) {
		for (size_t pos = 0; pos < syncContextTables_[pId]->slots_.size();
			pos++) {
			ss << syncContextTables_[pId]->slots_[pos]->dump();
		}
	}
	return ss.str();
}

std::string SyncContext::dump(uint8_t detailMode) {
	util::NormalOStringStream ss;
	ss << "SSN=" << syncSequentialNumber_
			<< ", pId=" << pId_ << ", mode=" << getSyncModeStr()
			<< ", role=" << dumpPartitionRoleStatus(roleStatus_);
	switch (detailMode) {
		case 1: {
			ss << "no=" << 	id_ << ":" << version_ << ":" << ptRev_.toString() 
					<< ", cpId=" << cpId_ << ", nextStmtId=" << nextStmtId_
					<< ", isSyncCpCompleted=" << isSyncCpCompleted_ << ", used=" << used_
					<< ", numSendBackup=" << numSendBackup_
					<< ", processedChunkNum=" << processedChunkNum_
					<< ", totalChunkNum=" << totalChunkNum_
					<< ", chunkBufferSize=" << chunkBufferSize_
					<< ", chunkBaseSize=" << chunkBaseSize_ << ", chunkNo=" << chunkNo_
					<< ", status=" << static_cast<int32_t>(status_);
		}
		break;
		case 2: {
			ss << ", processedLogCount=" << processedLogNum_
					<< ", processedLogSize=" << (processedLogSize_ / (1024*1024))
					<< ", actualLogTime=" << actualLogTime_
					<< ", startLSN=" << startLsn_ << ", endLSN=" << endLsn_;
			if (mode_ == MODE_LONGTERM_SYNC) {
				ss << ", processedChunkCount=" << processedChunkNum_
					<< ", actualChunkTime=" << actualChunkTime_
					<< ", chunkLeadTime=" << chunkLeadTime_;
			};
			ss << ", totalTime=" << totalTime_;
		}
		break;
	}
	return ss.str();
}

SyncManager::ConfigSetUpHandler SyncManager::configSetUpHandler_;

void SyncManager::ConfigSetUpHandler::operator()(ConfigTable &config) {
	CONFIG_TABLE_RESOLVE_GROUP(config, CONFIG_TABLE_SYNC, "sync");

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_SYNC_TIMEOUT_INTERVAL, INT32)
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
		.setDefault(8);

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
		config, CONFIG_TABLE_SYNC_SHORTTERM_LIMIT_QUEUE_SIZE, INT32)
		.setMin(0)
		.setDefault(10000);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_SHORTTERM_LOWLOAD_LOG_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_MS)
		.setMin(0)
		.setDefault(0);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_SHORTTERM_HIGHLOAD_LOG_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_MS)
		.setMin(0)
		.setDefault(0);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_LONGTERM_LIMIT_QUEUE_SIZE, INT32)
		.setMin(0)
		.setDefault(40);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_LONGTERM_LOWLOAD_LOG_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_MS)
		.setMin(0)
		.setDefault(0);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_LONGTERM_HIGHLOAD_LOG_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_MS)
		.setMin(0)
		.setDefault(100);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_LONGTERM_LOWLOAD_CHUNK_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_MS)
		.setMin(0)
		.setDefault(0);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_LONGTERM_HIGHLOAD_CHUNK_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_MS)
		.setMin(0)
		.setDefault(100);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_LONGTERM_DUMP_CHUNK_INTERVAL, INT32)
		.setMin(1)
		.setDefault(5000);

	CONFIG_TABLE_ADD_SERVICE_ADDRESS_PARAMS(config, SYNC, 10020);
}

void SyncManager::Config::setUpConfigHandler(
	SyncManager *syncMgr, ConfigTable &configTable) {
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
		CONFIG_TABLE_SYNC_SHORTTERM_LIMIT_QUEUE_SIZE, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_SHORTTERM_LOWLOAD_LOG_INTERVAL, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_SHORTTERM_HIGHLOAD_LOG_INTERVAL, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_LONGTERM_LIMIT_QUEUE_SIZE, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_LONGTERM_LOWLOAD_LOG_INTERVAL, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_LONGTERM_HIGHLOAD_LOG_INTERVAL, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_LONGTERM_LOWLOAD_CHUNK_INTERVAL, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_LONGTERM_HIGHLOAD_CHUNK_INTERVAL, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_LONGTERM_DUMP_CHUNK_INTERVAL, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_LOG_MAX_MESSAGE_SIZE, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_SYNC_CHUNK_MAX_MESSAGE_SIZE, *this);
}

void SyncManager::Config::operator()(
	ConfigTable::ParamId id, const ParamValue &value) {
	switch (id) {
	case CONFIG_TABLE_SYNC_LONG_SYNC_TIMEOUT_INTERVAL:
		syncMgr_->getPartitionTable()->getClusterManager()->getConfig().setLongTermTimeoutInterval(
				changeTimeSecToMill(value.get<int32_t>()));
		break;
	case CONFIG_TABLE_SYNC_LONG_SYNC_MAX_MESSAGE_SIZE:
			syncMgr_->getConfig().setMaxMessageSize(value.get<int32_t>());
		break;
	case CONFIG_TABLE_SYNC_APPROXIMATE_GAP_LSN:
		syncMgr_->getExtraConfig().setApproximateLsnGap(value.get<int32_t>());
		break;
	case CONFIG_TABLE_SYNC_LOCKCONFLICT_INTERVAL:
		syncMgr_->getExtraConfig().setLockWaitInterval(
		changeTimeSecToMill(value.get<int32_t>()));
		break;
	case CONFIG_TABLE_SYNC_APPROXIMATE_WAIT_INTERVAL:
		syncMgr_->getExtraConfig().setApproximateWaitInterval(
				changeTimeSecToMill(value.get<int32_t>()));
		break;
	case CONFIG_TABLE_SYNC_SHORTTERM_LIMIT_QUEUE_SIZE:
		syncMgr_->getExtraConfig().setLimitShorttermQueueSize(
				value.get<int32_t>());
		break;
	case CONFIG_TABLE_SYNC_SHORTTERM_LOWLOAD_LOG_INTERVAL:
		syncMgr_->getExtraConfig().setShorttermLowLoadLogWaitInterval(
				changeTimeSecToMill(value.get<int32_t>()));
		break;
	case CONFIG_TABLE_SYNC_SHORTTERM_HIGHLOAD_LOG_INTERVAL:
		syncMgr_->getExtraConfig().setShorttermHighLoadLogWaitInterval(
				changeTimeSecToMill(value.get<int32_t>()));
		break;
	case CONFIG_TABLE_SYNC_LONGTERM_LIMIT_QUEUE_SIZE:
		syncMgr_->getExtraConfig().setLimitLongtermQueueSize(
				value.get<int32_t>());
		break;
	case CONFIG_TABLE_SYNC_LONGTERM_LOWLOAD_LOG_INTERVAL:
		syncMgr_->getExtraConfig().setLongtermLowLoadLogWaitInterval(
				changeTimeSecToMill(value.get<int32_t>()));
		break;
	case CONFIG_TABLE_SYNC_LONGTERM_HIGHLOAD_LOG_INTERVAL:
		syncMgr_->getExtraConfig().setLongtermHighLoadLogWaitInterval(
				changeTimeSecToMill(value.get<int32_t>()));
		break;
	case CONFIG_TABLE_SYNC_LONGTERM_LOWLOAD_CHUNK_INTERVAL:
		syncMgr_->getExtraConfig().setLongtermLowLoadChunkWaitInterval(
				changeTimeSecToMill(value.get<int32_t>()));
		break;
	case CONFIG_TABLE_SYNC_LONGTERM_HIGHLOAD_CHUNK_INTERVAL:
		syncMgr_->getExtraConfig().setLongtermHighLoadChunkWaitInterval(
				changeTimeSecToMill(value.get<int32_t>()));
		break;
		case CONFIG_TABLE_SYNC_LONGTERM_DUMP_CHUNK_INTERVAL:
			syncMgr_->getExtraConfig().setLongtermDumpInterval(
					value.get<int32_t>());
		case CONFIG_TABLE_SYNC_LOG_MAX_MESSAGE_SIZE:
				syncMgr_->getConfig().setMaxMessageSize(ConfigTable::megaBytesToBytes(
						value.get<int32_t>()));
			break;
		case CONFIG_TABLE_SYNC_CHUNK_MAX_MESSAGE_SIZE:
				syncMgr_->getConfig().setMaxChunkMessageSize(ConfigTable::megaBytesToBytes(
						value.get<int32_t>()));
			break;
	}
}

void SyncManager::checkCurrentContext(EventContext &ec, PartitionId pId, bool isOwner) {
	PartitionId currentPId;
	if (isOwner) {
			currentPId = longSyncEntryManager_.currentPId_;
	}
	else {
			currentPId = longSyncEntryManager_.currentCatchupPId_;
	}

	if (currentPId != UNDEF_PARTITIONID && currentPId != pId) {
		SyncId syncId;
		PartitionRevision ptRev;
		getCurrentSyncId(currentPId, syncId, ptRev, isOwner);
			if (currentPId != UNDEF_PARTITIONID && syncContextTables_[currentPId] != NULL &&syncId.isValid()) {
			SyncContext *targetContext = syncContextTables_[currentPId]->getSyncContext(
					syncId.contextId_, syncId.contextVersion_);
			if (targetContext != NULL) {
				Event syncTimeoutEvent(ec, TXN_SYNC_TIMEOUT, currentPId);
				SyncTimeoutInfo syncTimeoutInfo(
						ec.getAllocator(), TXN_SYNC_TIMEOUT, this, currentPId, MODE_LONGTERM_SYNC, targetContext);
				syncSvc_->encode(syncTimeoutEvent, syncTimeoutInfo);
				txnSvc_->getEE()->addTimer(syncTimeoutEvent, 0);
			}
		}
	}
}

void SyncManager::LongSyncEntryManager::setCurrentSyncId(PartitionId pId,
		SyncContext *context, PartitionRevision &ptRev) {
	if (context->getPartitionRoleStatus() == PartitionTable::PT_OWNER) {
		syncEntryList_[pId].syncId_.contextId_ = context->getId();
		syncEntryList_[pId].syncId_.contextVersion_ = context->getVersion();
		syncEntryList_[pId].ptRev_ = ptRev;
		syncEntryList_[pId].syncSequentialNumber_ = context->getSequentialNumber();
		currentPId_ = pId;
		currentSyncSequentialNumber_ = context->getSequentialNumber();
	}
	else {
		syncCatchupEntryList_[pId].syncId_.contextId_ = context->getId();
		syncCatchupEntryList_[pId].syncId_.contextVersion_ = context->getVersion();
		syncCatchupEntryList_[pId].ptRev_ = ptRev;
		syncCatchupEntryList_[pId].syncSequentialNumber_ = context->getSequentialNumber();
		currentCatchupPId_ = pId;
		currentSyncCatchupSequentialNumber_ = context->getSequentialNumber();
	}
}

void SyncManager::LongSyncEntryManager::resetCurrentSyncId(PartitionId pId, bool isOwner) {
	if (isOwner) {
		if (pId != UNDEF_PARTITIONID) {
		syncEntryList_[pId].syncId_.reset();
		syncEntryList_[pId].syncSequentialNumber_ = -1;
		syncEntryList_[pId].ptRev_.updateRevision(1);
		}
		if (pId != UNDEF_PARTITIONID && pId == currentPId_) {
		currentPId_ = -1;
		currentSyncSequentialNumber_ = -1;
		}
	}
	else {
		if (pId != UNDEF_PARTITIONID) {
			syncCatchupEntryList_[pId].syncId_.reset();
			syncCatchupEntryList_[pId].syncSequentialNumber_ = -1;
			syncCatchupEntryList_[pId].ptRev_.updateRevision(1);
		}
		if (pId != UNDEF_PARTITIONID && pId == currentPId_) {
			currentCatchupPId_ = -1;
			currentSyncCatchupSequentialNumber_ = -1;
		}
	}
}

PartitionId SyncManager::checkCurrentSyncStatus() {
	util::LockGuard<util::WriteLock> guard(getAllocLock());
	PartitionId currentPId = UNDEF_PARTITIONID;
	SyncId syncId;
	PartitionRevision ptRev;
	getCurrentSyncId(currentPId, syncId, ptRev, true);
	if (currentPId != UNDEF_PARTITIONID && syncContextTables_[currentPId] != NULL
			&& syncId.isValid() && pt_->isOwner(currentPId, 0, PartitionTable::PT_CURRENT_GOAL)) {
		SyncContext *targetContext = syncContextTables_[currentPId]->getSyncContext(
				syncId.contextId_, syncId.contextVersion_);
		if (targetContext) {
			return currentSyncStatus_.checkAndUpdate(targetContext);
		}
		else {
		}
	}
	return UNDEF_PARTITIONID;
}
