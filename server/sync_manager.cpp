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
UTIL_TRACER_DECLARE(CLUSTER_OPERATION);
UTIL_TRACER_DECLARE(SYNC_SERVICE);

#define TRACE_REVISION(rev1, rev2) \
	"next:" << rev1.toString() << ", current:" << rev2.toString()


SyncManager::SyncManager(const ConfigTable &configTable, PartitionTable *pt)
	: syncOptStat_(pt->getPartitionNum()),
	  fixedSizeAlloc_(
		  util::AllocatorInfo(ALLOCATOR_GROUP_CS, "syncManagerFixed"), 1 << 18),
	  alloc_(util::AllocatorInfo(ALLOCATOR_GROUP_CS, "syncManagerStack"),
		  &fixedSizeAlloc_),
	  varSizeAlloc_(util::AllocatorInfo(ALLOCATOR_GROUP_CS, "syncManagerVar")),
	  syncContextTables_(NULL),
	  pt_(pt),
	  syncConfig_(configTable),
	  chunkBufferList_(NULL),
	  currentSyncPId_(UNDEF_PARTITIONID) {
	try {
		const uint32_t partitionNum =
			configTable.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM);
		if (partitionNum <= 0 || partitionNum != pt->getPartitionNum()) {
			GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_INFO, "");
		}

		syncContextTables_ =
			ALLOC_NEW(alloc_) SyncContextTable * [partitionNum];
		for (PartitionId pId = 0; pId < partitionNum; pId++) {
			syncContextTables_[pId] = NULL;
		}
		chunkSize_ = configTable.getUInt32(CONFIG_TABLE_DS_STORE_BLOCK_SIZE);
		recoveryLevel_ =
			configTable.get<int32_t>(CONFIG_TABLE_DS_RECOVERY_LEVEL);
		syncMode_ = SYNC_MODE_NORMAL;
		undoPartitionList_.assign(partitionNum, 0);

		chunkBufferList_ = ALLOC_NEW(alloc_)
			uint8_t[chunkSize_ *
					configTable.getUInt32(CONFIG_TABLE_DS_CONCURRENCY)];
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

/*!
	@brief Creates SyncContext
*/
SyncContext *SyncManager::createSyncContext(
	PartitionId pId, PartitionRevision &ptRev) {
	try {
		util::LockGuard<util::WriteLock> guard(getAllocLock());

		if (pId >= pt_->getPartitionNum()) {
			return NULL;
		}

		createPartition(pId);
		getSyncOptStat()->setContext(pId);

		return syncContextTables_[pId]->createSyncContext(ptRev);
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
void SyncManager::removeSyncContext(PartitionId pId, SyncContext *&context) {
	try {
		util::LockGuard<util::WriteLock> guard(getAllocLock());

		if (pId >= pt_->getPartitionNum()) {
			return;
		}

		if (syncContextTables_[pId] == NULL) {
			return;
		}
		getSyncOptStat()->freeContext(pId);


		syncContextTables_[pId]->removeSyncContext(
			varSizeAlloc_, context, getSyncOptStat());
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
			GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_INFO, "");
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
			pt_->getPartitionRole(
				pId, currentRole, PartitionTable::PT_CURRENT_OB);

			if (!currentRole.isOwner() && !candNextRole.isOwner()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_ROLE, "");
			}
			if (nextRole.getRevision() > candNextRole.getRevision()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_REVISION,
					TRACE_REVISION(nextRole.getRevision(),
										candNextRole.getRevision()));
			}
			break;
		}
		case OP_SHORTTERM_SYNC_START: {
			pt_->getPartitionRole(pId, nextRole, PartitionTable::PT_NEXT_OB);
			if (nextRole.getRevision() > candNextRole.getRevision()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_REVISION,
					TRACE_REVISION(nextRole.getRevision(),
										candNextRole.getRevision()));
			}
			break;
		}
		case OP_SHORTTERM_SYNC_START_ACK:
		case OP_SHORTTERM_SYNC_LOG_ACK:
		case OP_SHORTTERM_SYNC_END_ACK: {
			pt_->getPartitionRole(pId, nextRole, PartitionTable::PT_NEXT_OB);
			pt_->getPartitionRole(
				pId, currentRole, PartitionTable::PT_CURRENT_OB);
			if (!currentRole.isOwner() && !nextRole.isOwner()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_ROLE, "");
			}
			if (nextRole.getRevision() != candNextRole.getRevision()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_REVISION,
					TRACE_REVISION(nextRole.getRevision(),
										candNextRole.getRevision()));
			}
			break;
		}
		case OP_SHORTTERM_SYNC_LOG:
		case OP_SHORTTERM_SYNC_END: {
			pt_->getPartitionRole(pId, nextRole, PartitionTable::PT_NEXT_OB);
			if (nextRole.getRevision() != candNextRole.getRevision()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_REVISION,
					TRACE_REVISION(nextRole.getRevision(),
										candNextRole.getRevision()));
			}
			break;
		}
		case OP_LONGTERM_SYNC_REQUEST: {
			if (!candNextRole.isOwner()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_SYM_INVALID_PARTITION_ROLE, "not owner");
			}
			pt_->getPartitionRole(pId, nextRole, PartitionTable::PT_NEXT_GOAL);
			if (nextRole.getRevision() > candNextRole.getRevision()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_REVISION,
					TRACE_REVISION(nextRole.getRevision(),
										candNextRole.getRevision()));
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
					TRACE_REVISION(nextRole.getRevision(),
										candNextRole.getRevision()));
			}
			break;
		}
		case OP_LONGTERM_SYNC_START_ACK:
		case OP_LONGTERM_SYNC_CHUNK_ACK:
		case OP_LONGTERM_SYNC_LOG_ACK: {
			pt_->getPartitionRole(pId, nextRole, PartitionTable::PT_NEXT_GOAL);
			if (!nextRole.isOwner()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_SYM_INVALID_PARTITION_ROLE, "not owner");
			}
			if (nextRole.getRevision() != candNextRole.getRevision()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_REVISION,
					TRACE_REVISION(nextRole.getRevision(),
										candNextRole.getRevision()));
			}
			break;
		}
		case OP_LONGTERM_SYNC_LOG:
		case OP_LONGTERM_SYNC_CHUNK: {
			pt_->getPartitionRole(pId, nextRole, PartitionTable::PT_NEXT_GOAL);
			if (!nextRole.isBackup()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_SYM_INVALID_PARTITION_ROLE, "not catchup");
			}
			if (nextRole.getRevision() != candNextRole.getRevision()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_REVISION,
					TRACE_REVISION(nextRole.getRevision(),
										candNextRole.getRevision()));
			}
			pt_->getPartitionRole(pId, currentRole);
			if (currentRole.isOwner()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_SYM_INVALID_PARTITION_ROLE, "owner");
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
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(
				   e, "Failed to check executable: pId=" << pId));
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
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(
				   e, "Failed to create partition: pId=" << pId));
	}
}


SyncManager::SyncContextTable::SyncContextTable(util::StackAllocator &alloc,
	PartitionId pId, uint32_t numInitialSlot,
	SyncVariableSizeAllocator *varSizeAlloc)
	: pId_(pId),
	  numCounter_(0),
	  freeList_(NULL),
	  numUsed_(0),
	  alloc_(&alloc),
	  slots_(alloc),
	  varSizeAlloc_(varSizeAlloc) {
	try {
		for (uint32_t i = 0; i < numInitialSlot; i++) {
			SyncContext *slot = ALLOC_NEW(alloc) SyncContext[SLOT_SIZE];
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
			SyncContext *slot = ALLOC_NEW(*alloc_) SyncContext[SLOT_SIZE];
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
	SyncVariableSizeAllocator &alloc, SyncContext *&context,
	SyncOptStat *stat) {
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


SyncContext::SyncContext()
	: id_(0),
	  pId_(0),
	  version_(0),
	  used_(false),
	  numSendBackup_(0),
	  nextStmtId_(0),
	  cpId_(UNDEF_CHECKPOINT_ID),
	  recvNodeId_(UNDEF_NODEID),
	  isSyncCpCompleted_(false),
	  nextEmptyChain_(NULL),
	  isDownNextOwner_(false),
	  processedChunkNum_(0),
	  totalChunkNum_(0),
	  logBuffer_(NULL),
	  logBufferSize_(0),
	  chunkBuffer_(NULL),
	  chunkBufferSize_(0),
	  chunkBaseSize_(0),
	  chunkNum_(0),
	  chunkNo_(0),
	  status_(PartitionTable::PT_OFF),
	  queueSize_(0) {}

SyncContext::~SyncContext() {}

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
	uint32_t before = numSendBackup_;
	for (size_t i = 0; i < sendBackups_.size(); i++) {
		if (sendBackups_[i].nodeId_ == syncTargetNodeId) {
			sendBackups_[i].isAcked_ = true;
			numSendBackup_--;
			break;
		}
	}
	TRACE_SYNC_NORMAL(INFO, "target:" << syncTargetNodeId
									  << ", before:" << before
									  << ", after:" << numSendBackup_);

	return (numSendBackup_ == 0);
}

/*!
	@brief Gets id list of nodes under synchronization
*/
void SyncContext::getSyncIncompleteNodIds(
	std::vector<NodeId> &syncIncompleteNodeIds) {
	try {
		syncIncompleteNodeIds.clear();
		for (size_t i = 0; i < sendBackups_.size(); i++) {
			if (!sendBackups_[i].isAcked_) {
				syncIncompleteNodeIds.push_back(sendBackups_[i].nodeId_);
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
	chunkBuffer =
		static_cast<uint8_t *>(chunkBuffer_ + (chunkBaseSize_ * chunkNo));
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

std::string SyncContext::dump(bool isDetail) {
	util::NormalOStringStream ss;
	ss << "{";
	ss << "id:" << id_ << ", version:" << version_ << ", pId:" << pId_
	   << ", ptRev:" << ptRev_.toString();

	if (isDetail) {
		ss << ", cpId:" << cpId_ << ", nextStmtId:" << nextStmtId_
		   << ", isSyncCpCompleted:" << isSyncCpCompleted_ << ", used:" << used_
		   << ", numSendBackup:" << numSendBackup_
		   << ", processedChunkNum:" << processedChunkNum_
		   << ", totalChunkNum:" << totalChunkNum_
		   << ", logBufferAddress:" << logBuffer_
		   << ", chunkBufferSize:" << chunkBufferSize_
		   << ", chunkBaseSize:" << chunkBaseSize_ << ", chunkNo:" << chunkNo_
		   << ", status:" << status_;
	}
	ss << "}";
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
		.setDefault(1200);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_SYNC_LONG_SYNC_MAX_MESSAGE_SIZE, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_SIZE_B)
		.setMin(1)
		.setDefault(1024 * 1024 * 85 / 100);  

	CONFIG_TABLE_ADD_SERVICE_ADDRESS_PARAMS(config, SYNC, 10020);
}
