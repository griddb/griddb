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
	@brief Implementation of SyncService
*/
#include "sync_service.h"
#include "transaction_service.h"
#include "json.h"
#include "uuid_utils.h"

#ifndef _WIN32
#include <signal.h>  
#endif

UTIL_TRACER_DECLARE(CLUSTER_SERVICE);
UTIL_TRACER_DECLARE(SYNC_SERVICE);
UTIL_TRACER_DECLARE(IO_MONITOR);
UTIL_TRACER_DECLARE(SYNC_DETAIL);
UTIL_TRACER_DECLARE(REDO_MANAGER);

#define RM_THROW_LOG_REDO_ERROR(errorCode, message) \
	GS_THROW_CUSTOM_ERROR(LogRedoException, , errorCode, message)
#define RM_RETHROW_LOG_REDO_ERROR(cause, message) \
	GS_RETHROW_CUSTOM_ERROR(LogRedoException, GS_ERROR_DEFAULT, cause, message)

class PartitionRevisionMessage {

public:
	PartitionRevisionMessage(PartitionRevision& revision) : revision_(revision) {}

	void encode(EventByteOutStream& out) {
		out << revision_.addr_;
		out << revision_.port_;
		out << revision_.sequentialNumber_;
	}

	void decode(EventByteInStream& in) {
		in >> revision_.addr_;
		in >> revision_.port_;
		in >> revision_.sequentialNumber_;
	}

private:
	PartitionRevision& revision_;
};

SyncService::SyncService(
	const ConfigTable& config,
	const EventEngine::Config& eeConfig,
	EventEngine::Source source,
	const char8_t* name,
	SyncManager& syncMgr,
	ClusterVersionId versionId,
	ServiceThreadErrorHandler& serviceThreadErrorHandler) :
	varAlloc_(util::AllocatorInfo(ALLOCATOR_GROUP_SYNC, "syncAll")),
	ee_(createEEConfig(config, eeConfig), source, name),
	syncMgr_(&syncMgr),
	redoMgr_(config, varAlloc_, this),
	txnSvc_(NULL),
	clsSvc_(NULL),
	versionId_(versionId),
	serviceThreadErrorHandler_(serviceThreadErrorHandler),
	initialized_(false)
{
	try {
		syncMgr_ = &syncMgr;
		ee_.setHandler(SYC_SHORTTERM_SYNC_LOG, recvSyncMessageHandler_);
		setClusterHandler();
		ee_.setUnknownEventHandler(unknownSyncEventHandler_);
		ee_.setThreadErrorHandler(serviceThreadErrorHandler_);
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

void SyncService::initialize(ManagerSet& mgrSet) {

	clsSvc_ = mgrSet.clsSvc_;
	txnSvc_ = mgrSet.txnSvc_;
	partitionList_ = mgrSet.partitionList_;
	syncMgr_->initialize(&mgrSet);
	redoMgr_.initialize(mgrSet);
	recvSyncMessageHandler_.initialize(mgrSet);
	serviceThreadErrorHandler_.initialize(mgrSet);
	initialized_ = true;
}

SyncService::~SyncService() {

	ee_.shutdown();
	ee_.waitForShutdown();
}

/*!
	@brief Sets EventEngine config
*/
EventEngine::Config SyncService::createEEConfig(
	const ConfigTable& config, const EventEngine::Config& src) {

	EventEngine::Config eeConfig = src;
	eeConfig.setServerAddress(
		config.get<const char8_t*>(
			CONFIG_TABLE_SYNC_SERVICE_ADDRESS),
		config.getUInt16(CONFIG_TABLE_SYNC_SERVICE_PORT));
	eeConfig.setPartitionCount(
		config.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM));
	eeConfig.setConcurrency(
		config.get<int32_t>(CONFIG_TABLE_DS_CONCURRENCY));

	eeConfig.setAllAllocatorGroup(ALLOCATOR_GROUP_SYNC);
	return eeConfig;
}

/*!
	@brief Starts SyncService
*/
void SyncService::start() {

	try {
		if (!initialized_) {
			GS_THROW_USER_ERROR(
				GS_ERROR_CS_SERVICE_NOT_INITIALIZED, "");
		}
		ee_.start();
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Shutdown SyncService
*/
void SyncService::shutdown() {

	ee_.shutdown();
}

/*!
	@brief Waits for shutdown
*/
void SyncService::waitForShutdown() {

	ee_.waitForShutdown();
}

template <class T>
void SyncService::decode(util::StackAllocator& alloc, EventEngine::Event& ev, T& t, EventByteInStream& in) {

	try {
		util::XArray<char8_t> buffer(alloc);
		const char8_t* eventBuffer = reinterpret_cast<const char8_t*>(
			ev.getMessageBuffer().getXArray().data() + ev.getMessageBuffer().getOffset());

		uint8_t decodedVersionId;
		in >> decodedVersionId;
		checkVersion(decodedVersionId);

		uint16_t logVersion;
		in >> logVersion;
		StatementHandler::checkLogVersion(logVersion);
		t.decode(in, eventBuffer);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
}

template
void SyncService::decode(
		util::StackAllocator& alloc, EventEngine::Event& ev,
		SyncRequestInfo& t, EventByteInStream& in);

template
void SyncService::decode(
		util::StackAllocator& alloc, EventEngine::Event& ev,
		SyncResponseInfo& t, EventByteInStream& in);

/*!
	@brief Checks the version of synchronization message
*/
void SyncService::checkVersion(uint8_t versionId) {
	if (versionId != versionId_) {
		GS_THROW_USER_ERROR(
			GS_ERROR_CS_ENCODE_DECODE_VERSION_CHECK,
			"Cluster message version is unmatched,  acceptableClusterVersion="
			<< static_cast<int32_t>(versionId_)
			<< ", connectClusterVersion=" << static_cast<int32_t>(versionId) << " (sync)");
	}
}

uint16_t SyncService::getLogVersion(PartitionId pId) {
	return partitionList_->partition(pId).logManager().getLogFormatVersion();
}


template <class T>
void SyncService::encode(
	Event& ev, T& t, EventByteOutStream& out) {

	try {
		out << versionId_;
		uint16_t logVersion = getLogVersion(ev.getPartitionId());
		out << logVersion;
		t.encode(out);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
}

void SyncHandler::initialize(const ManagerSet& mgrSet) {

	try {
		clsSvc_ = mgrSet.clsSvc_;
		txnSvc_ = mgrSet.txnSvc_;
		txnEE_ = txnSvc_->getEE();
		syncSvc_ = mgrSet.syncSvc_;
		syncEE_ = syncSvc_->getEE();
		pt_ = mgrSet.pt_;
		clsMgr_ = clsSvc_->getManager();
		syncMgr_ = syncSvc_->getManager();
		txnMgr_ = mgrSet.txnMgr_;
		cpSvc_ = mgrSet.cpSvc_;
		recoveryMgr_ = mgrSet.recoveryMgr_;
		partitionList_ = mgrSet.partitionList_;
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Requests DropPartition
*/
void SyncService::requestDrop(
	const Event::Source& eventSource,
	util::StackAllocator& alloc,
	PartitionId pId,
	bool isForce,
	PartitionRevisionNo revision) {

	try {
		DropPartitionInfo dropPartitionInfo(
			alloc, TXN_DROP_PARTITION,
			syncMgr_, pId, isForce, revision);

		Event dropEvent(
			eventSource, TXN_DROP_PARTITION, pId);
		clsSvc_->encode(dropEvent, dropPartitionInfo);
		txnSvc_->getEE()->add(dropEvent);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void SyncService::notifyCheckpointLongSyncReady(EventContext& ec, PartitionId pId, LongtermSyncInfo* syncInfo) {
	clsSvc_->request(ec, TXN_LONGTERM_SYNC_PREPARE_ACK, pId, txnSvc_->getEE(), *syncInfo);
}

void SyncService::notifySyncCheckpointEnd(EventContext& ec, PartitionId pId, LongtermSyncInfo* syncInfo) {
	clsSvc_->request(ec, TXN_LONGTERM_SYNC_RECOVERY_ACK, pId, txnSvc_->getEE(), *syncInfo);
}

void SyncService::notifySyncCheckpointError(
	EventContext& ec,
	PartitionId pId,
	LongtermSyncInfo* syncInfo) {
	
	SyncId syncId(syncInfo->getId(), syncInfo->getVersion());
	SyncContext* context = syncMgr_->getSyncContext(pId, syncId);
	if (context) {
		SyncCheckEndInfo syncCheckEndInfo(
			ec.getAllocator(), TXN_SYNC_TIMEOUT,
			syncMgr_, pId, MODE_LONGTERM_SYNC, context);
		Event syncCheckEndEvent(ec, TXN_SYNC_TIMEOUT, pId);
		EventByteOutStream out = syncCheckEndEvent.getOutStream();
		encode(syncCheckEndEvent, syncCheckEndInfo, out);
		txnSvc_->getEE()->add(syncCheckEndEvent);
	}
}

void SyncHandler::sendEvent(
		EventContext& ec,
		EventEngine& ee,
		NodeId targetNodeId,
		Event& ev,
		int32_t delayTime) {
	UNUSED_VARIABLE(ec);

	if (targetNodeId == SELF_NODEID) {
		if (delayTime == UNDEF_DELAY_TIME) {
			delayTime = EE_PRIORITY_HIGH;
		}
		ee.addTimer(ev, delayTime);
	}
	else {
		EventRequestOption sendOption;
		if (delayTime != UNDEF_DELAY_TIME) {
			sendOption.timeoutMillis_ = delayTime;
		}
		else {
			sendOption.timeoutMillis_ = EE_PRIORITY_HIGH;
		}
		const NodeDescriptor& nd = ee.getServerND(targetNodeId);
		ee.send(ev, nd, &sendOption);
	}
}

/*!
	@brief Handler Operator
*/
void ShortTermSyncHandler::operator()(EventContext& ec, Event& ev) {

	SyncContext* context = NULL;
	SyncContext* longSyncContext = NULL;
	PartitionId pId = ev.getPartitionId();
	EventType eventType = ev.getType();
	NodeId senderNodeId = ClusterService::resolveSenderND(ev);
	SyncId longSyncId;
	try {
		EVENT_START(ec, ev, txnMgr_, UNDEF_DBID, false);

		util::StackAllocator& alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);
		EventByteInStream in = ev.getInStream();
		SyncRequestInfo syncRequestInfo(alloc, eventType, syncMgr_, pId, clsSvc_->getEE(), MODE_SHORTTERM_SYNC);
		SyncResponseInfo syncResponseInfo(alloc, eventType, syncMgr_, pId, MODE_SHORTTERM_SYNC, pt_->getLSN(pId));
		Partition& partition = partitionList_->partition(pId);
		KeepLogManager* keepLogMgr = syncMgr_->getKeepLogManager();
		util::LockGuard<util::Mutex> guard(partition.mutex());

		WATCHER_START;

		switch (eventType) {

		case TXN_SHORTTERM_SYNC_REQUEST: {
			syncSvc_->decode(alloc, ev, syncRequestInfo, in);
			decodeLongSyncRequestInfo(pId, in, longSyncId, longSyncContext);
			syncMgr_->checkExecutable(TXN_SHORTTERM_SYNC_REQUEST, pId, syncRequestInfo.getPartitionRole());
			context = syncMgr_->createSyncContext(
				ec, pId, syncRequestInfo.getPartitionRole(), MODE_SHORTTERM_SYNC, PartitionTable::PT_OWNER);
			if (context == NULL) {
				return;
			}
			context->setLongSyncId(longSyncId);
			context->setEvent(eventType);
			if (context->isUseLongSyncLog()) {
				GS_TRACE_SYNC(context, "[OWNER] Catchup promotion sync start, SYNC_START", "")
			}
			GS_OUTPUT_SYNC(context, "[" << EventTypeUtility::getEventTypeName(eventType) << ":START]", "");
			executeSyncRequest(ec, pId, context, syncRequestInfo, syncResponseInfo);
			checkOwner(pt_->getLSN(pId), context);
			break;
		}
		case TXN_SHORTTERM_SYNC_START: {
			syncSvc_->decode(alloc, ev, syncRequestInfo, in);
			syncMgr_->checkExecutable(TXN_SHORTTERM_SYNC_START, pId, syncRequestInfo.getPartitionRole());
			executeSyncStart(ec, ev, pId, context, syncRequestInfo, syncResponseInfo, senderNodeId);
			break;
		}
		case TXN_SHORTTERM_SYNC_START_ACK: {
			syncSvc_->decode(alloc, ev, syncResponseInfo, in);
			SyncId syncId = syncResponseInfo.getSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			syncMgr_->checkExecutable(
				TXN_SHORTTERM_SYNC_START_ACK, pId,
				syncResponseInfo.getPartitionRole());
			executeSyncStartAck(
				ec, ev, pId, context,
				syncRequestInfo, syncResponseInfo, senderNodeId);
			break;
		}
		case TXN_SHORTTERM_SYNC_LOG: {
			syncSvc_->decode(alloc, ev, syncRequestInfo, in);
			SyncId syncId = syncRequestInfo.getBackupSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			syncMgr_->checkExecutable(TXN_SHORTTERM_SYNC_LOG, pId, syncRequestInfo.getPartitionRole());
			executeSyncLog(ec, ev, pId, context, syncRequestInfo, syncResponseInfo);
			break;
		}
		case TXN_SHORTTERM_SYNC_LOG_ACK: {

			syncSvc_->decode(
				alloc, ev, syncResponseInfo, in);
			SyncId syncId = syncResponseInfo.getSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			syncMgr_->checkExecutable(
				TXN_SHORTTERM_SYNC_LOG_ACK, pId,
				syncResponseInfo.getPartitionRole());
			executeSyncLogAck(
				ec, ev, pId, context,
				syncRequestInfo, syncResponseInfo, senderNodeId);
			break;
		}
		case TXN_SHORTTERM_SYNC_END: {
			syncSvc_->decode(alloc, ev, syncRequestInfo, in);
			SyncId syncId = syncRequestInfo.getBackupSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			syncMgr_->checkExecutable(
				TXN_SHORTTERM_SYNC_END, pId,
				syncRequestInfo.getPartitionRole());
			executeSyncEnd(
				ec, pId, context,
				syncRequestInfo, syncResponseInfo, senderNodeId);
			break;
		}
		case TXN_SHORTTERM_SYNC_END_ACK: {
			syncSvc_->decode(alloc, ev, syncResponseInfo, in);
			SyncId syncId = syncResponseInfo.getSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			syncMgr_->checkExecutable(
				TXN_SHORTTERM_SYNC_END_ACK, pId,
				syncResponseInfo.getPartitionRole());
			executeSyncEndAck(
				ec, pId, context, syncResponseInfo, senderNodeId);
			break;
		}
		default: {
			GS_THROW_USER_ERROR(
				GS_ERROR_SYNC_EVENT_TYPE_INVALID, "");
		}
		}
		WATCHER_END_DETAIL(eventType, pId, ec.getWorkerId());
		if (context) {
			GS_OUTPUT_SYNC(context, "[" << EventTypeUtility::getEventTypeName(eventType) << ":END]", "");
		}
	}
	catch (UserException& e) {
		if (context != NULL) {
			GS_OUTPUT_SYNC(context, "[" << EventTypeUtility::getEventTypeName(eventType) << ":ERROR]", "");

			UTIL_TRACE_EXCEPTION(
				SYNC_DETAIL, e, "Short term sync failed, pId=" << pId
				<< ", lsn=" << pt_->getLSN(pId)
				<< ", revision="
				<< context->getPartitionRevision().getRevisionNo()
				<< ", event=" << EventTypeUtility::getEventTypeName(eventType)
				<< ", reason=" << GS_EXCEPTION_MESSAGE(e));

			addCheckEndEvent(
				ec, pId, ec.getAllocator(),
				MODE_SHORTTERM_SYNC, context, true);
		}
		else {
			TRACE_SYNC_EXCEPTION(e, eventType, pId, ERROR, "Short term sync operation failed");
		}
		if (context == NULL && longSyncId.isValid()) {
			longSyncContext = syncMgr_->getSyncContext(pId, longSyncId);
			if (longSyncContext) {
				syncMgr_->removeSyncContext(ec, pId, longSyncContext, true);
			}
		}
	}
	catch (LockConflictException& e) {
		if (context != NULL) {
			addCheckEndEvent(ec, pId, ec.getAllocator(), MODE_SHORTTERM_SYNC, context, true);
		}
	}
	catch (LogRedoException& e) {
		TRACE_SYNC_EXCEPTION(e, eventType, pId, WARNING,
			"Short term sync failed, log redo exception is occurred");
		if (context != NULL) {
			addCheckEndEvent(ec, pId, ec.getAllocator(), MODE_SHORTTERM_SYNC, context, true);
		}
	}
	catch (std::exception& e) {
		if (context != NULL) {
			GS_OUTPUT_SYNC(context, "[" << EventTypeUtility::getEventTypeName(eventType) << ":START]", "");
			addCheckEndEvent(ec, pId, ec.getAllocator(), MODE_SHORTTERM_SYNC, context, true);
		}
		UTIL_TRACE_EXCEPTION(SYNC_SERVICE, e, "");
		clsSvc_->setError(ec, &e);
	}
}

int32_t errorCounter2 = 0;
int32_t errorMax2 = 5;
#define RAISE_EXCEPTION2() \
if (errorCounter2 < errorMax2) { \
std::cout << "exception" << errorCounter2 << std::endl; \
errorCounter2++; \
GS_THROW_USER_ERROR(0, ""); \
} \


/*!
	@brief Gets log information satisfies a presetting condition
*/
bool SyncRequestInfo::getLogs(
	PartitionId pId,
	int64_t sId,
	LogManager<MutexLocker>* logMgr,
	CheckpointService* cpSvc,
	bool useLongSyncLog,
	bool isCheck, SearchLogInfo& info) {

	binaryLogRecords_.clear();
	bool isSyncLog = false;
	LogSequentialNumber lastLsn = 0;
	condition_.logSize_ = syncMgr_->getConfig().getMaxMessageSize();
	std::unique_ptr<Log> log;
	try {
		LogSequentialNumber current = request_.startLsn_;
		LogIterator<MutexLocker> logIt = logMgr->createXLogIterator(current);
		if (current == info.lsn_ && info.logVersion_ == logIt.getLogVersion()) {
			logIt.setOffset(info.offset_);
		}
		GS_OUTPUT_SYNC2(pt_->getDumpLock(),
			"GetLog start, pId=" << pId << ", startLsn=" << request_.startLsn_ << ", endLsn=" << request_.endLsn_
			<< ", offset=" << logIt.getOffset() << ", version=" << logIt.getLogVersion());
		if (!logIt.checkExists(request_.startLsn_)) {
			if (request_.syncMode_ == MODE_LONGTERM_SYNC || useLongSyncLog) {
				GS_THROW_USER_ERROR(GS_ERROR_SYNC_LOG_GET_FAILED,
					"Long term log find failed, start Lsn=" << request_.startLsn_);
			}
			else {
				GS_THROW_USER_ERROR(GS_ERROR_SYNC_LOG_GET_FAILED,
					"Short term log find failed, start Lsn=" << request_.startLsn_);
			}
		}
		while (binaryLogRecords_.size() <= condition_.logSize_) {
			util::StackAllocator::Scope scope(logMgr->getAllocator());
			log = logIt.next(false);
			if (log == NULL) break;
			log->encode(logMgr->getAllocator(), binaryLogRecords_);
			lastLsn = log->getLsn();
			if (log->getLsn() >= request_.endLsn_) {
				break;
			}
		}
		logIt.fin();
		if (request_.endLsn_ <= lastLsn) {
			isSyncLog = true;
		}
		request_.endLsn_ = lastLsn;
		request_.binaryLogSize_ = static_cast<uint32_t>(binaryLogRecords_.size());
		info.lsn_ = lastLsn + 1;
		info.offset_ = logIt.getOffset();
		info.logVersion_ = logIt.getLogVersion();
		GS_OUTPUT_SYNC2(pt_->getDumpLock(),
			"GetLog end, pId=" << pId << ", startLsn=" << request_.startLsn_ << ", endLsn=" << request_.endLsn_ 
			<< ", size=" << request_.binaryLogSize_ << ", offset=" << logIt.getOffset() << ", version=" << info.logVersion_);
		return isSyncLog;
	}
	catch (std::exception& e) {
		request_.endLsn_ = lastLsn;
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}


bool SyncRequestInfo::getLogs(PartitionId pId, int64_t startLogVersion, int64_t startOffset, LogManager<MutexLocker>* logMgr) {
	binaryLogRecords_.clear();
	LogSequentialNumber lastLsn = 0;
	std::unique_ptr<Log> log;
	try {
		LogSequentialNumber current = request_.startLsn_;
		LogIterator<MutexLocker> logIt = logMgr->createXLogIterator(current);
		if (startLogVersion == logIt.getLogVersion()) {
			logIt.setOffset(startOffset);
		}
		GS_OUTPUT_SYNC2(pt_->getDumpLock(),
			"GetLog start, pId=" << pId << ", startLsn=" << request_.startLsn_ << ", endLsn=" << request_.endLsn_
			<< ", offset=" << logIt.getOffset() << ", version=" << logIt.getLogVersion());
		try {
			if (!logIt.checkExists(request_.startLsn_)) {
				return false;
			}
		}
		catch (std::exception& e) {
			UTIL_TRACE_EXCEPTION_WARNING(
				SYNC_SERVICE, e, "reason=" << GS_EXCEPTION_MESSAGE(e));
			return false;
		}
		while (true) {
			util::StackAllocator::Scope scope(logMgr->getAllocator());
			log = logIt.next(false);
			if (log == NULL) break;
			log->encode(logMgr->getAllocator(), binaryLogRecords_);
			lastLsn = log->getLsn();
			if (log->getLsn() >= request_.endLsn_) {
				break;
			}
		}
		logIt.fin();
		request_.endLsn_ = lastLsn;
		request_.binaryLogSize_ = static_cast<uint32_t>(binaryLogRecords_.size());
		GS_OUTPUT_SYNC2(pt_->getDumpLock(),
			"GetLog end, pId=" << pId << ", startLsn=" << request_.startLsn_ << ", endLsn=" << request_.endLsn_
			<< ", size=" << request_.binaryLogSize_ << ", offset=" << logIt.getOffset() << ", version=" << info.logVersion_);
		return true;
	}
	catch (std::exception& e) {
		request_.endLsn_ = lastLsn;
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}


/*!
	@brief Gets Chunks
*/
bool SyncRequestInfo::getChunks(
		util::StackAllocator& alloc,
		PartitionId pId,
		PartitionTable* pt,
		CheckpointService* cpSvc,
		Partition* partition,
		int64_t sId,
		uint64_t& totalCount) {
	UNUSED_VARIABLE(pt);

	bool lastChunkGet = false;
	try {
		chunks_.clear();
		binaryLogRecords_.clear();
		uint32_t chunkSize = condition_.chunkSize_;
		request_.numChunk_ = 0;
		request_.binaryLogSize_ = 0;
		uint64_t resultSize;

		for (uint32_t pos = 0; pos < condition_.chunkNum_;pos++) {
			uint8_t* chunk = ALLOC_NEW(alloc) uint8_t [chunkSize];
			uint64_t completedCount;
			uint64_t readyCount;
			lastChunkGet = cpSvc->getLongSyncChunk(sId, chunkSize, chunk, resultSize, totalCount, completedCount, readyCount);
			if (totalCount != completedCount && resultSize == 0) {
				break;
			}
			chunks_.push_back(chunk);
			request_.numChunk_++;
			if (lastChunkGet) break;
		}
		GS_OUTPUT_SYNC2(
			pt_->getDumpLock(), "GetChunk, pId=" << pId << ", chunk num = " << request_.numChunk_ << ", lastChunkGet = " << (int)lastChunkGet);
		if (lastChunkGet) {
			LogManager<MutexLocker>& logManager = partition->logManager();
			std::unique_ptr<Log> log;
			LogIterator<MutexLocker> logIt(&logManager, 0);
			util::StackAllocator::Scope scope(logManager.getAllocator());
			GS_OUTPUT_SYNC2(pt_->getDumpLock(), "Get Long cp start log pId=" << pId <<  ", ssn = " << sId);

			if (!cpSvc->getLongSyncCheckpointStartLog(sId, logIt)) {
				GS_THROW_USER_ERROR(GS_ERROR_SYNC_CHUNK_GET_FAILED,
					"Long term sync get checkpoint start log failed, pId=" << pId);
			}
			log = logIt.next(false); 
			if (log == NULL) {
				GS_THROW_USER_ERROR(GS_ERROR_SYNC_CHUNK_GET_FAILED,
					"Long term sync get checkpoint start log failed, pId=" << pId);
			}
			log->encode(logManager.getAllocator(), binaryLogRecords_);
			request_.binaryLogSize_ = static_cast<uint32_t>(binaryLogRecords_.size());
			logIt.fin();
		}
		return lastChunkGet;
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Handler Operator
*/
void SyncCheckEndHandler::operator()(EventContext& ec, Event& ev) {

	SyncContext* context = NULL;
	PartitionId pId = ev.getPartitionId();
	try {
		EVENT_START(ec, ev, txnMgr_, UNDEF_DBID, false);

		util::StackAllocator& alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);
		EventByteInStream in = ev.getInStream();
		SyncCheckEndInfo syncCheckEndInfo(alloc, TXN_SYNC_TIMEOUT, syncMgr_, pId, MODE_SYNC_TIMEOUT, context);

		syncSvc_->decode(alloc, ev, syncCheckEndInfo, in);
		SyncId syncId = syncCheckEndInfo.getSyncId();
		context = syncMgr_->getSyncContext(pId, syncId);
		syncMgr_->checkExecutable(TXN_SYNC_TIMEOUT, pId, syncCheckEndInfo.getPartitionRole());

		if (context != NULL) {
			PartitionRevisionNo nextRevision = pt_->getPartitionRevision(pId, PartitionTable::PT_CURRENT_OB);
			if (syncCheckEndInfo.getRevisionNo() < nextRevision) {
				syncMgr_->removeSyncContext(ec, pId, context, true);
				return;
			}
			if (in.base().remaining() > 0) {
				int32_t longtermSyncType;
				in >> longtermSyncType;
				if (context->checkLongTermSync(cpSvc_, syncMgr_->getConfig().getLongtermCheckInterval())) {
					addLongtermSyncCheckEvent(ec, pId, alloc, context, longtermSyncType);
				}
				return;
			}

			if (context->getSyncMode() == MODE_SHORTTERM_SYNC
				&& pt_->getPartitionStatus(pId) != PartitionTable::PT_ON) {

				if (context->checkTotalTime(clsMgr_->getConfig().getShortTermTimeoutInterval())) {
					GS_TRACE_SYNC(context, "Shortterm sync timeout", "");
				}
				else {
					GS_TRACE_SYNC(
						context, "Short term sync error", ", check previous trace,"
						" key SSN=" << context->getSequentialNumber());
				}
				PartitionRole currentRole;
				pt_->getPartitionRole(pId, currentRole);
				pt_->clear(pId);
				PartitionRole afterRole;
				pt_->getPartitionRole(pId, afterRole);
				clsSvc_->requestChangePartitionStatus(
					ec, alloc, pId,
					PartitionTable::PT_OFF, PT_CHANGE_SYNC_END);
			}
			if (context->getSyncMode() == MODE_LONGTERM_SYNC) {
				PartitionRole catchupRole;
				pt_->getPartitionRole(pId, catchupRole);
				catchupRole.clearCatchup();
				pt_->setPartitionRole(pId, catchupRole);
			}
			if (context->getSyncMode() == MODE_SHORTTERM_SYNC ||
				(context->getSyncMode() == MODE_LONGTERM_SYNC
					&& !context->isCatchupSync())) {

				if (context->getSyncMode() == MODE_LONGTERM_SYNC) {
					GS_TRACE_SYNC_INFO(context, "Remove long term sync context", "");
				}
				syncMgr_->removeSyncContext(ec, pId, context, true);
			}
		}
	}
	catch (UserException& e) {
		UTIL_TRACE_EXCEPTION_WARNING(
			SYNC_SERVICE, e, "reason=" << GS_EXCEPTION_MESSAGE(e));
		if (context) {
			syncMgr_->removeSyncContext(ec, pId, context, true);
		}
	}
	catch (std::exception& e) {
		if (context) {
			syncMgr_->removeSyncContext(ec, pId, context, true);
		}
		UTIL_TRACE_EXCEPTION(SYNC_SERVICE, e, "");
		clsSvc_->setError(ec, &e);
	}
}

/*!
	@brief Handler Operator
*/
void DropPartitionHandler::operator()(EventContext& ec, Event& ev) {

	PartitionId pId = ev.getPartitionId();
	EventType eventType = ev.getType();
	try {
		EVENT_START(ec, ev, txnMgr_, UNDEF_DBID, false);
		clsMgr_->checkNodeStatus();
		util::StackAllocator& alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);
		DropPartitionInfo dropPartitionInfo(
			alloc, TXN_DROP_PARTITION,
			syncMgr_, pId, false);

		clsSvc_->decode(ev, dropPartitionInfo);
		Partition& partition = partitionList_->partition(pId);

		if (!dropPartitionInfo.isForce()) {
			syncMgr_->checkActiveStatus();
			if (pt_->getLSN(pId) == 0
				&& partition.isActive()
				&& !partition.chunkManager().hasBlock()) {
				return;
			}
			if (pt_->getPartitionRoleStatus(pId)
				!= PartitionTable::PT_NONE) {
				return;
			}
			if (pt_->getPartitionRevision(
				pId, PartitionTable::PT_CURRENT_OB)
				!= dropPartitionInfo.getPartitionRevision()) {
				return;
			}
		}
		LogSequentialNumber prevLsn = pt_->getLSN(pId);
		{
			util::LockGuard<util::Mutex> guard(partition.mutex());
			syncMgr_->removePartition(pId);
			syncMgr_->recoveryPartition(pId);
		}
		TRACE_CLUSTER_NORMAL_OPERATION(
			INFO, "Drop partition completed (pId=" << pId
			<< ", lsn=" << prevLsn << ")");
		clsSvc_->requestChangePartitionStatus(
			ec, alloc, pId,
			PartitionTable::PT_OFF, PT_CHANGE_NORMAL);
	}
	catch (UserException& e) {
		UTIL_TRACE_EXCEPTION_DEBUG(
			SYNC_SERVICE, e, "");
	}
	catch (LockConflictException& e) {
		UTIL_TRACE_EXCEPTION(
			SYNC_SERVICE, e,
			"Drop partition pending, pId=" << pId
			<< ", reason=" << GS_EXCEPTION_MESSAGE(e));

		TRACE_SYNC_EXCEPTION(
			e, eventType, pId, WARNING,
			"Drop partition pending, "
			"checkpoint-sync lock conflict exception is occurred, retry after "
			<< syncMgr_->getExtraConfig()
			.getLockConflictPendingInterval() / 1000 << " sec");

		txnEE_->addTimer(
			ev, syncMgr_->getExtraConfig()
			.getLockConflictPendingInterval());

	}
	catch (std::exception& e) {
		UTIL_TRACE_EXCEPTION(
			SYNC_SERVICE, e,
			"Drop partition failed, pId=" << pId
			<< ", reason=" << GS_EXCEPTION_MESSAGE(e));
	}
}

/*!
	@brief Handler Operator
*/
void RecvSyncMessageHandler::operator() (EventContext& ec, Event& ev) {

	SyncContext* context = NULL;
	PartitionId pId = ev.getPartitionId();
	EventType txnEventType;
	EventType syncEventType = ev.getType();
	try {
		clsMgr_->checkNodeStatus();
		util::StackAllocator& alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);
		EventByteInStream in = ev.getInStream();

		WATCHER_START;
		switch (syncEventType) {
			case SYC_SHORTTERM_SYNC_LOG: {
				txnEventType = TXN_SHORTTERM_SYNC_LOG;
				break;
			}
			case SYC_LONGTERM_SYNC_LOG: {
				txnEventType = TXN_LONGTERM_SYNC_LOG;
				break;
			}
			case SYC_LONGTERM_SYNC_CHUNK: {
				txnEventType = TXN_LONGTERM_SYNC_CHUNK;
				break;
			}
			default: {
				GS_THROW_USER_ERROR(GS_ERROR_SYNC_EVENT_TYPE_INVALID, "");
			}
		}
		SyncRequestInfo syncRequestInfo(alloc, syncEventType, syncMgr_, pId, clsSvc_->getEE());
		syncSvc_->decode(alloc, ev, syncRequestInfo, in);
		syncMgr_->checkExecutable(txnEventType, pId, syncRequestInfo.getPartitionRole());
		{
			SyncId syncId = syncRequestInfo.getBackupSyncId();
			util::LockGuard<util::ReadLock> guard(syncMgr_->getAllocLock());
			context = syncMgr_->getSyncContext(pId, syncId, false);
			if (context == NULL) {
				return;
			}
		}
		syncRequestInfo.setEventType(txnEventType);
		sendRequest(ec, *txnEE_, txnEventType, pId, syncRequestInfo, 0);
		WATCHER_END_DETAIL(syncEventType, pId, ec.getWorkerId());
	}
	catch (UserException& e) {
		TRACE_SYNC_EXCEPTION(e, syncEventType, pId, ERROR, "Recv sync message failed, ");
	}
	catch (std::exception& e) {
		UTIL_TRACE_EXCEPTION(SYNC_SERVICE, e, "");
		clsSvc_->setError(ec, &e);
	}
}

/*!
	@brief Adds Sync Timeout event
*/
void SyncHandler::addCheckEndEvent(
	EventContext& ec,
	PartitionId pId,
	util::StackAllocator& alloc,
	SyncMode mode,
	SyncContext* context,
	bool isImmediate) {

	try {
		int32_t timeoutInterval = 0;
		if (isImmediate) {
			timeoutInterval = 0;
		}
		else if (mode == MODE_SHORTTERM_SYNC) {
			timeoutInterval
				= clsMgr_->getConfig().getShortTermTimeoutInterval();
		}
		Event syncCheckEndEvent(
			ec, TXN_SYNC_TIMEOUT, pId);
		SyncCheckEndInfo syncCheckEndInfo(
			alloc, TXN_SYNC_TIMEOUT,
			syncMgr_, pId, mode, context);

		EventByteOutStream out
			= syncCheckEndEvent.getOutStream();
		syncSvc_->encode(
			syncCheckEndEvent, syncCheckEndInfo, out);
		txnEE_->addTimer(
			syncCheckEndEvent, timeoutInterval);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void SyncHandler::addLongtermSyncCheckEvent(
	EventContext& ec,
	PartitionId pId,
	util::StackAllocator& alloc,
	SyncContext* context,
	int32_t longtermSyncType) {

	try {
		int32_t timeoutInterval = clsMgr_->getConfig().getCheckLoadBalanceInterval();
		Event syncCheckEndEvent(ec, TXN_SYNC_TIMEOUT, pId);
		SyncCheckEndInfo syncCheckEndInfo(alloc, TXN_SYNC_TIMEOUT, syncMgr_, pId, MODE_LONGTERM_SYNC, context);

		EventByteOutStream out = syncCheckEndEvent.getOutStream();
		syncSvc_->encode(syncCheckEndEvent, syncCheckEndInfo, out);
		out << longtermSyncType;
		txnEE_->addTimer(syncCheckEndEvent, timeoutInterval);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

SyncPartitionLock::SyncPartitionLock(
	TransactionManager* txnMgr, PartitionId pId) :
	txnMgr_(txnMgr),
	pId_(pId) {
	if (!txnMgr_->lockPartition(pId_)) {
		GS_THROW_CUSTOM_ERROR(LockConflictException,
			GS_ERROR_TXN_PARTITION_LOCK_CONFLICT,
			"Checkpoint-Sync lock conflict is occurred, pId=" << pId_);
	}
}

SyncPartitionLock::~SyncPartitionLock() {

	txnMgr_->unlockPartition(pId_);
}

/*!
	@brief Handler Operator
*/
void UnknownSyncEventHandler::operator()(
	EventContext&, Event& ev) {

	EventType eventType = ev.getType();
	try {
		GS_THROW_USER_ERROR(
			GS_ERROR_SYNC_SERVICE_UNKNOWN_EVENT_TYPE, "");
	}
	catch (UserException& e) {
		UTIL_TRACE_EXCEPTION_WARNING(
			SYNC_SERVICE, e,
			"Unknown sync event, type:"
			<< EventTypeUtility::getEventTypeName(eventType));
	}
}

void SyncRequestInfo::decode(EventByteInStream& in, const char8_t* bodyBuffer) {

	SyncVariableSizeAllocator& varSizeAlloc = syncMgr_->getVariableSizeAllocator();

	switch (eventType_) {
	case TXN_SHORTTERM_SYNC_REQUEST:
	case TXN_LONGTERM_SYNC_REQUEST: {
		SyncManagerInfo::decode(in, bodyBuffer);
		break;
	}
	case TXN_SHORTTERM_SYNC_START:
	case TXN_LONGTERM_SYNC_START: {
		request_.decode(in);
		SyncManagerInfo::decode(in, bodyBuffer);
		break;
	}
	case TXN_SHORTTERM_SYNC_LOG:
	case TXN_SHORTTERM_SYNC_END:
	case TXN_LONGTERM_SYNC_LOG:
	case TXN_LONGTERM_SYNC_CHUNK: {

		request_.decode(in);
		partitionRole_.set(request_.ptRev_);
		break;
	}
	case SYC_SHORTTERM_SYNC_LOG:
	case SYC_LONGTERM_SYNC_LOG: {

		request_.decode(in);
		partitionRole_.set(request_.ptRev_);
		util::LockGuard<util::ReadLock> guard(syncMgr_->getAllocLock());
		SyncContext* context = syncMgr_->getSyncContext(pId_, backupSyncId_, false);
		if (context == NULL) {
			return;
		}
		if (request_.binaryLogSize_ > 0) {
			const size_t currentPosition = in.base().position();
			const uint8_t* logBuffer = reinterpret_cast<const uint8_t*>(
					bodyBuffer + currentPosition);
			if (logBuffer == NULL) {
				GS_THROW_USER_ERROR(GS_ERROR_SYNC_SERVICE_DECODE_MESSAGE_FAILED, "");
			}
			context->copyLogBuffer(varSizeAlloc, logBuffer,
				request_.binaryLogSize_, syncMgr_->getSyncOptStat());
		}
		break;
	}
	case SYC_LONGTERM_SYNC_CHUNK: {

		request_.decode(in);
		partitionRole_.set(request_.ptRev_);
		util::LockGuard<util::ReadLock> guard(syncMgr_->getAllocLock());
		SyncContext* context = syncMgr_->getSyncContext(pId_, backupSyncId_, false);
		if (context == NULL) {
			return;
		}
		if (request_.numChunk_ == 0 || request_.chunkSize_ == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_SYNC_SERVICE_DECODE_MESSAGE_FAILED, "");
		}
		size_t currentPosition = in.base().position();
		const uint8_t* chunkBuffer = reinterpret_cast<const uint8_t*>(
				bodyBuffer + currentPosition);
		if (chunkBuffer == NULL) {
			GS_THROW_USER_ERROR(GS_ERROR_SYNC_SERVICE_DECODE_MESSAGE_FAILED, "");
		}
		context->copyChunkBuffer(varSizeAlloc, chunkBuffer, request_.chunkSize_,
			request_.numChunk_, syncMgr_->getSyncOptStat());
		if (request_.binaryLogSize_ > 0) {
			size_t nextPosition = currentPosition + request_.chunkSize_ * request_.numChunk_;
			const uint8_t* logBuffer = reinterpret_cast<const uint8_t*>(bodyBuffer + nextPosition);
			if (logBuffer == NULL) {
				GS_THROW_USER_ERROR(GS_ERROR_SYNC_SYNCMESSAGE_FAILED, "");
			}
			context->copyLogBuffer(varSizeAlloc, logBuffer, request_.binaryLogSize_, syncMgr_->getSyncOptStat());
		}
		break;
	}
	default: {
		GS_THROW_USER_ERROR(GS_ERROR_SYNC_EVENT_TYPE_INVALID, "");
	}
	}

	switch (eventType_) {
	case TXN_SHORTTERM_SYNC_REQUEST:
	case TXN_LONGTERM_SYNC_REQUEST:
	case TXN_SHORTTERM_SYNC_START:
	case TXN_LONGTERM_SYNC_START: {

		NodeAddress ownerAddress;
		std::vector<NodeAddress> backupsAddress, catchupAddress;
		NodeId owner;
		NodeIdList backups, catchups;
		partitionRole_.get(ownerAddress, backupsAddress, catchupAddress);
		owner = ClusterService::changeNodeId(ownerAddress, ee_);

		for (size_t pos = 0;pos < backupsAddress.size(); pos++) {
			NodeId nodeId = ClusterService::changeNodeId(backupsAddress[pos], ee_);
			if (nodeId != UNDEF_NODEID) {
				backups.push_back(nodeId);
			}
		}
		for (size_t pos = 0;pos < catchupAddress.size(); pos++) {
			NodeId nodeId = ClusterService::changeNodeId(catchupAddress[pos], ee_);
			if (nodeId != UNDEF_NODEID) {
				catchups.push_back(nodeId);
			}
		}
		partitionRole_.set(owner, backups, catchups);
	}
	default:
		break;
	}
}

void SyncResponseInfo::decode(EventByteInStream& in, const char8_t*) {

	switch (eventType_) {
	case TXN_SHORTTERM_SYNC_START_ACK:
	case TXN_SHORTTERM_SYNC_END_ACK:
	case TXN_SHORTTERM_SYNC_LOG_ACK:
	case TXN_LONGTERM_SYNC_START_ACK:
	case TXN_LONGTERM_SYNC_LOG_ACK:
	case TXN_LONGTERM_SYNC_CHUNK_ACK: {
		response_.decode(in);
		partitionRole_.set(response_.ptRev_);
		break;
	}
	default: {
		GS_THROW_USER_ERROR(GS_ERROR_SYNC_EVENT_TYPE_INVALID, "");
	}
	}
}

void SyncManagerInfo::encode(EventByteOutStream& out) {

	try {
		msgpack::sbuffer buffer;
		try {
			msgpack::pack(buffer, partitionRole_);
		}
		catch (std::exception& e) {
			GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e, "Failed to encode message"));
		}
		uint32_t packedSize = static_cast<uint32_t>(buffer.size());
		if (packedSize == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_CS_ENCODE_DECODE_VALIDATION_CHECK, "");
		}
		out << packedSize;
		out.writeAll(buffer.data(), packedSize);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
}

void SyncManagerInfo::decode(EventByteInStream& in, const char8_t* bodyBuffer) {

	uint32_t bodySize;
	in >> bodySize;
	const char8_t* dataBuffer = bodyBuffer + in.base().position();
	const size_t lastSize = in.base().position() + bodySize;
	msgpack::unpacked msg;
	msgpack::unpack(&msg, dataBuffer, static_cast<size_t>(bodySize));
	msgpack::object obj = msg.get();
	obj.convert(&partitionRole_);
	in.base().position(lastSize);
}

void SyncCheckEndInfo::decode(EventByteInStream& in, const char8_t*) {

	PartitionRevisionMessage revision(ptRev_);
	revision.decode(in);
	syncId_.decode(in);
	int32_t tmpSyncMode;
	in >> tmpSyncMode;
	syncMode_ = static_cast<SyncMode>(tmpSyncMode);
}

void SyncCheckEndInfo::encode(EventByteOutStream& out) {

	if (context_ == NULL || !syncId_.isValid()) {
		GS_THROW_USER_ERROR(GS_ERROR_SYNC_SERVICE_ENCODE_MESSAGE_FAILED, "pId=" << pId_);
	}

	PartitionRevisionMessage revision(context_->getPartitionRevision());
	revision.encode(out);
	syncId_.encode(out);
	out << static_cast<int32_t>(syncMode_);
}

void SyncRequestInfo::encode(EventByteOutStream& out) {
	switch (eventType_) {
		case TXN_SHORTTERM_SYNC_REQUEST:
		case TXN_LONGTERM_SYNC_REQUEST: {
			SyncManagerInfo::encode(out);
			break;
		}
		case TXN_SHORTTERM_SYNC_START:
		case TXN_LONGTERM_SYNC_START: {
			request_.encode(out);
			SyncManagerInfo::encode(out);
			break;
		}
		case TXN_SHORTTERM_SYNC_END: {
			request_.encode(out);
			break;
		}
		case TXN_SHORTTERM_SYNC_LOG:
		case TXN_LONGTERM_SYNC_LOG:
		case TXN_LONGTERM_SYNC_CHUNK: {
			request_.encode(out, false);
			break;
		}
		case SYC_SHORTTERM_SYNC_LOG:
		case SYC_LONGTERM_SYNC_LOG: {
			request_.encode(out);
			break;
		}
		case SYC_LONGTERM_SYNC_CHUNK: {
			request_.encode(out);
			break;
		}
		default: {
			GS_THROW_USER_ERROR(
				GS_ERROR_SYNC_EVENT_TYPE_INVALID, "");
		}
	}
}

void SyncResponseInfo::encode(EventByteOutStream& out) {

	switch (eventType_) {
	case TXN_SHORTTERM_SYNC_START_ACK:
	case TXN_SHORTTERM_SYNC_END_ACK:
	case TXN_SHORTTERM_SYNC_LOG_ACK:
	case TXN_LONGTERM_SYNC_START_ACK:
	case TXN_LONGTERM_SYNC_LOG_ACK:
	case TXN_LONGTERM_SYNC_CHUNK_ACK: {
		response_.encode(out);
		break;
	}
	default: {
		GS_THROW_USER_ERROR(
			GS_ERROR_SYNC_EVENT_TYPE_INVALID, "");
	}
	}
}

/*!
	@brief Encodes synchronization request
*/
void SyncRequestInfo::Request::encode(
	EventByteOutStream& out, bool isOwner) {

	out << syncMode_;
	PartitionRevisionMessage revision(ptRev_);
	revision.encode(out);
	requestInfo_->syncId_.encode(out);
	requestInfo_->backupSyncId_.encode(out);
	out << ownerLsn_;
	out << startLsn_;
	out << endLsn_;
	out << chunkSize_;
	out << numChunk_;
	out << binaryLogSize_;

	if (isOwner) {
		if (numChunk_ > 0) {
			util::XArray<uint8_t*>& chunks = requestInfo_->chunks_;
			for (size_t pos = 0; pos < numChunk_; pos++) {
				out.writeAll(chunks[pos], chunkSize_);
			}
		}
		if (binaryLogSize_ > 0) {
			util::XArray<uint8_t>& logRecord = requestInfo_->binaryLogRecords_;
			out.writeAll(logRecord.data(), logRecord.size());
		}
	}
}

/*!
	@brief Decodes synchronization request
*/
void SyncRequestInfo::Request::decode(EventByteInStream& in) {

	in >> syncMode_;
	PartitionRevisionMessage revision(ptRev_);
	revision.decode(in);
	requestInfo_->syncId_.decode(in);
	requestInfo_->backupSyncId_.decode(in);
	in >> ownerLsn_;
	in >> startLsn_;
	in >> endLsn_;
	in >> chunkSize_;
	in >> numChunk_;
	in >> binaryLogSize_;
}

/*!
	@brief Encodes synchronization response
*/
void SyncResponseInfo::Response::encode(EventByteOutStream& out) {

	out << syncMode_;
	PartitionRevisionMessage revision(ptRev_);
	revision.encode(out);
	responseInfo_->syncId_.encode(out);
	responseInfo_->backupSyncId_.encode(out);
	out << targetLsn_;
}

/*!
	@brief Decodes synchronization response
*/
void SyncResponseInfo::Response::decode(EventByteInStream& in) {

	in >> syncMode_;
	PartitionRevisionMessage revision(ptRev_);
	revision.decode(in);
	responseInfo_->syncId_.decode(in);
	responseInfo_->backupSyncId_.decode(in);
	in >> targetLsn_;
}

/*!
	@brief Executes DropPartition
*/
void SyncHandler::removePartition(PartitionId pId) {

	SyncPartitionLock partitionLock(txnMgr_, pId);
	
	Partition& partition = partitionList_->partition(pId);
	partition.drop();
	partition.reinit(*partitionList_);
	
	syncMgr_->setUndoCompleted(pId);
	pt_->setLSN(pId, 0);
}

void ShortTermSyncHandler::undoPartition(
	util::StackAllocator& alloc, SyncContext* context, PartitionId pId) {
	bool isStandby = clsMgr_->getStandbyInfo().isStandby();
	if (!syncMgr_->isUndoCompleted(pId) && !isStandby) {
		LogSequentialNumber prevLsn = pt_->getLSN(pId);
		const size_t undoNum = partitionList_->partition(pId).undo(alloc);
		if (undoNum > 0) {
			TRACE_CLUSTER_NORMAL_OPERATION(INFO,
				"Undo partition (pId=" << pId << ", undoNum=" << undoNum << 
				", before LSN=" << prevLsn << ", after LSN=" << pt_->getLSN(pId) << ")");
		}
		context->setOwnerLsn(pt_->getLSN(pId));
	}
	syncMgr_->setUndoCompleted(pId);
}

void ShortTermSyncHandler::decodeLongSyncRequestInfo(
	PartitionId pId, EventByteInStream& in, SyncId& longSyncId, SyncContext*& longSyncContext) {
	if (in.base().remaining()) {
		in >> longSyncId.contextId_;
		in >> longSyncId.contextVersion_;
		longSyncContext = syncMgr_->getSyncContext(pId, longSyncId);
		if (longSyncContext == NULL) {
			GS_TRACE_SYNC_NORMAL("Long sync context is not found");
		}
	}
}

void SyncHandler::sendResponse(
	EventContext& ec, EventType eventType, PartitionId pId, SyncRequestInfo& syncRequestInfo,
	SyncResponseInfo& syncResponseInfo, SyncContext* context, NodeId senderNodeId) {
	Event ackEvent(ec, eventType, pId);
	syncResponseInfo.set(eventType, syncRequestInfo, pt_->getLSN(pId), context);
	EventByteOutStream out = ackEvent.getOutStream();
	syncSvc_->encode(ackEvent, syncResponseInfo, out);
	sendEvent(ec, *txnEE_, senderNodeId, ackEvent);
}

int64_t ShortTermSyncHandler::getTargetSSN(PartitionId pId, SyncContext* context) {
	int64_t ssn = -1;
	if (context->isUseLongSyncLog()) {
		SyncId longSyncId;
		context->getLongSyncId(longSyncId);
		SyncContext* longContext = syncMgr_->getSyncContext(pId, longSyncId);
		if (longContext) {
			ssn = longContext->getSequentialNumber();
		}
	}
	else {
		ssn = context->getSequentialNumber();
	}
	return ssn;
}

void SyncHandler::sendRequest(
		EventContext& ec, EventEngine& ee, EventType eventType, PartitionId pId,
		SyncRequestInfo& syncRequestInfo, NodeId senderNodeId, int32_t waitTime) {
	Event requestEvent(ec, eventType, pId);
	EventByteOutStream out = requestEvent.getOutStream();
	syncSvc_->encode(requestEvent, syncRequestInfo, out);
	sendEvent(ec, ee, senderNodeId, requestEvent, waitTime);
}

template <typename T>
void SyncHandler::sendSyncRequest(
	EventContext& ec, EventEngine& ee, EventType eventType, PartitionId pId,
	T& syncRequestInfo, NodeId senderNodeId, int32_t waitTime) {
	Event requestEvent(ec, eventType, pId);
	EventByteOutStream out = requestEvent.getOutStream();
	syncSvc_->encode(requestEvent, syncRequestInfo, out);
	sendEvent(ec, ee, senderNodeId, requestEvent, waitTime);
}

template<typename T>
void SyncHandler::sendMultiRequest(
	EventContext& ec,
	EventEngine& ee,
	EventType eventType,
	PartitionId pId,
	SyncRequestInfo& syncRequestInfo,
	T& t,
	SyncContext* context) {

	Event requestEvent(ec, eventType, pId);
	EventByteOutStream out = requestEvent.getOutStream();
	syncSvc_->encode(
		requestEvent, syncRequestInfo, out);
	for (typename T::iterator it = t.begin();
		it != t.end(); it++) {
		sendEvent(ec, ee, (*it), requestEvent);
		context->incrementCounter(*it);
	}
}

template void SyncHandler::sendMultiRequest(
	EventContext& ec, EventEngine& ee, EventType eventType,
	PartitionId pId, SyncRequestInfo& requestInfo,
	util::XArray<NodeId>& t, SyncContext* context);

template void SyncHandler::sendMultiRequest(
	EventContext& ec, EventEngine& ee, EventType eventType,
	PartitionId pId, SyncRequestInfo& requestInfo,
	std::vector<NodeId>& t, SyncContext* context);

template void SyncHandler::sendMultiRequest(
	EventContext& ec, EventEngine& ee, EventType eventType,
	PartitionId pId, SyncRequestInfo& requestInfo,
	util::Set<NodeId>& t, SyncContext* context);

void ShortTermSyncHandler::executeSyncRequest(
	EventContext& ec,
	PartitionId pId,
	SyncContext* context,
	SyncRequestInfo& syncRequestInfo,
	SyncResponseInfo& syncResponseInfo) {

	util::StackAllocator& alloc = ec.getAllocator();
	clsSvc_->requestChangePartitionStatus(
		ec, alloc, pId,
		PartitionTable::PT_SYNC, PT_CHANGE_SYNC_START);

	util::Set<NodeId> syncTargetNodeSet(alloc);
	syncMgr_->setShorttermSyncRequest(
		context, syncRequestInfo, syncTargetNodeSet);
	addCheckEndEvent(
		ec, pId, alloc,
		MODE_SHORTTERM_SYNC, context, false);

	syncMgr_->checkRestored(pId);

	undoPartition(alloc, context, pId);

	if (syncTargetNodeSet.size() > 0) {
		sendMultiRequest(
			ec, *txnEE_, TXN_SHORTTERM_SYNC_START,
			pId, syncRequestInfo, syncTargetNodeSet, context);
	}
	else {
		sendResponse(
			ec, TXN_SHORTTERM_SYNC_END_ACK, pId,
			syncRequestInfo, syncResponseInfo, context, 0);
		context->incrementCounter(0);
		context->setOwnerLsn(pt_->getLSN(pId));
	}
	context->startAll();
}

void ShortTermSyncHandler::executeSyncStart(EventContext& ec, Event& ev, PartitionId pId, SyncContext* context,
	SyncRequestInfo& syncRequestInfo, SyncResponseInfo& syncResponseInfo, NodeId senderNodeId) {
	util::StackAllocator& alloc = ec.getAllocator();
	clsSvc_->requestChangePartitionStatus(ec, alloc, pId, PartitionTable::PT_SYNC, PT_CHANGE_SYNC_START);
	PartitionRole& nextRole = syncRequestInfo.getPartitionRole();
	context = syncMgr_->createSyncContext(ec, pId, nextRole, MODE_SHORTTERM_SYNC, PartitionTable::PT_BACKUP);
	if (context == NULL) {
		return;
	}
	GS_OUTPUT_SYNC(context, "[" << EventTypeUtility::getEventTypeName(ev.getType()) << ":START]", "");
	context->setEvent(ev.getType());
	syncMgr_->setShorttermSyncStart(context, syncRequestInfo, senderNodeId);
	syncMgr_->checkRestored(pId);
	addCheckEndEvent(ec, pId, alloc, MODE_SHORTTERM_SYNC, context, false);
	sendResponse(ec, TXN_SHORTTERM_SYNC_START_ACK, pId, syncRequestInfo, syncResponseInfo, context, senderNodeId);
	context->startAll();
}

void ShortTermSyncHandler::checkBackup(
	NodeId backupNodeId,
	LogSequentialNumber backupLsn,
	SyncContext* context) {

	LogSequentialNumber sendLsn
		= context->getSyncTargetEndLsn(backupNodeId);
	if (sendLsn != backupLsn) {
		GS_THROW_USER_ERROR(
			GS_ERROR_SYNC_CHECK_LSN,
			"Backup Lsn is invalid, send Lsn=" << sendLsn
			<< ", current Lsn=" << backupLsn);
	}
}

void ShortTermSyncHandler::executeSyncStartAck(EventContext& ec, Event& ev, PartitionId pId, SyncContext* context,
	SyncRequestInfo& syncRequestInfo, SyncResponseInfo& syncResponseInfo, NodeId senderNodeId) {
	UNUSED_VARIABLE(ev);
	util::StackAllocator& alloc = ec.getAllocator();
	if (context != NULL) {
		GS_OUTPUT_SYNC(context, "[" << EventTypeUtility::getEventTypeName(ev.getType()) << ":START]", "");
		context->setEvent(ev.getType());
		LogSequentialNumber backupLsn = syncResponseInfo.getTargetLsn();
		LogSequentialNumber ownerLsn = pt_->getLSN(pId);
		checkOwner(ownerLsn, context);
		context->setSyncTargetLsnWithSyncId(senderNodeId, backupLsn, syncResponseInfo.getBackupSyncId());
		LogSequentialNumber maxLsn = pt_->getMaxLsn(pId);
		if (ownerLsn >= backupLsn) {
			if (context->decrementCounter(senderNodeId)) {
				syncMgr_->checkShorttermGetSyncLog(context, syncRequestInfo);
				util::XArray<NodeId> backups(alloc);
				context->getSyncTargetNodeIds(backups);
				int32_t sendCount = 0;
				for (size_t pos = 0; pos < backups.size(); pos++) {
					syncMgr_->setShorttermGetSyncLog(context, syncRequestInfo, backups[pos]);
					sendRequest(ec, *syncEE_, SYC_SHORTTERM_SYNC_LOG, pId, syncRequestInfo, backups[pos]);
					sendCount++;
				}
				if (sendCount == 0) {
					sendResponse(
						ec, TXN_SHORTTERM_SYNC_END_ACK, pId,
						syncRequestInfo, syncResponseInfo, context, 0);
				}
			}
		}
		else {
			clsSvc_->requestChangePartitionStatus(
				ec, alloc, pId, PartitionTable::PT_OFF, PT_CHANGE_SYNC_END);
			GS_THROW_SYNC_ERROR(
				GS_ERROR_SYNC_OWNER_BACKUP_INVALID_LSN_CONSTRAINT,
				context,
				"Backup LSN is larger than owner LSN",
				", owner=" << pt_->dumpNodeAddress(0)
				<< ", owner LSN=" << ownerLsn
				<< ", backup=" << pt_->dumpNodeAddress(senderNodeId)
				<< ", backup LSN=" << backupLsn
				<< ", maxLSN=" << maxLsn);
		}
	}
}

void ShortTermSyncHandler::executeSyncLog(EventContext& ec, Event& ev, PartitionId pId,
	SyncContext* context, SyncRequestInfo& syncRequestInfo, SyncResponseInfo& syncResponseInfo) {
	if (context != NULL) {
		GS_OUTPUT_SYNC(context, "[" << EventTypeUtility::getEventTypeName(ev.getType()) << ":START]", "");
		context->setEvent(ev.getType());
		if (syncMgr_->setShorttermSyncLog(context, syncRequestInfo, ec.getHandlerStartTime(),
			ec.getHandlerStartMonotonicTime())) {
			txnEE_->add(ev);
		}
		else {
			sendResponse(ec, TXN_SHORTTERM_SYNC_LOG_ACK, pId, syncRequestInfo, syncResponseInfo,
				context, context->getRecvNodeId());
		}
		syncMgr_->setUndoCompleted(pId);
	}
}

void ShortTermSyncHandler::checkOwner(
	LogSequentialNumber targetLsn,
	SyncContext* context,
	bool isOwner) {

	if (targetLsn != context->getOwnerLsn()) {
		if (isOwner) {
			GS_THROW_USER_ERROR(
				GS_ERROR_SYNC_CHECK_LSN,
				"Owner Lsn is changed, start="
				<< context->getOwnerLsn()
				<< ", current=" << targetLsn);
		}
		else {
			GS_THROW_USER_ERROR(
				GS_ERROR_SYNC_CHECK_LSN,
				"Backup Lsn is not same, owner Lsn="
				<< context->getOwnerLsn()
				<< ", backupLsn=" << targetLsn);
		}
	}
}

void ShortTermSyncHandler::executeSyncLogAck(
	EventContext& ec, Event& ev, PartitionId pId, SyncContext* context,
	SyncRequestInfo& syncRequestInfo, SyncResponseInfo& syncResponseInfo, NodeId senderNodeId) {

	util::StackAllocator& alloc = ec.getAllocator();
	if (context != NULL) {
		GS_OUTPUT_SYNC(context, "[" << EventTypeUtility::getEventTypeName(ev.getType()) << ":START]", "");
		context->setEvent(ev.getType());
		LogSequentialNumber ownerLsn = pt_->getLSN(pId);
		LogSequentialNumber backupLsn = syncResponseInfo.getTargetLsn();
		checkOwner(ownerLsn, context);
		context->setSyncTargetLsn(senderNodeId, backupLsn);
		if (ownerLsn == backupLsn) {
			if (context->decrementCounter(senderNodeId)) {
				context->resetCounter();
				util::XArray<NodeId> backups(alloc);
				context->getSyncTargetNodeIds(backups);
				for (size_t pos = 0; pos < backups.size(); pos++) {
					syncMgr_->setShorttermSyncLogAck(context, syncRequestInfo, backups[pos]);
					sendRequest(ec, *txnEE_, TXN_SHORTTERM_SYNC_END, pId, syncRequestInfo, backups[pos]);
				}
			}
		}
		else {
			syncMgr_->setShorttermGetSyncNextLog(
				context, syncRequestInfo, syncResponseInfo, senderNodeId);
			sendRequest(ec, *syncEE_, SYC_SHORTTERM_SYNC_LOG, pId, syncRequestInfo, senderNodeId);
			checkOwner(ownerLsn, context);
		}
	}
}

void ShortTermSyncHandler::executeSyncEnd(
	EventContext& ec,
	PartitionId pId,
	SyncContext* context,
	SyncRequestInfo& syncRequestInfo,
	SyncResponseInfo& syncResponseInfo,
	NodeId senderNodeId) {

	util::StackAllocator& alloc = ec.getAllocator();
	if (context != NULL) {
		GS_OUTPUT_SYNC(context, "[" << EventTypeUtility::getEventTypeName(TXN_SHORTTERM_SYNC_END) << ":START]", "");
		context->setEvent(TXN_SHORTTERM_SYNC_END);
		syncMgr_->setUndoCompleted(pId);
		checkOwner(pt_->getLSN(pId), context, false);
		syncMgr_->setShorttermSyncEnd(context);
		clsSvc_->requestChangePartitionStatus(
			ec, alloc, pId,
			PartitionTable::PT_ON, PT_CHANGE_SYNC_END);

		sendResponse(
			ec, TXN_SHORTTERM_SYNC_END_ACK, pId,
			syncRequestInfo, syncResponseInfo,
			context, senderNodeId);
		context->endAll();
		syncMgr_->removeSyncContext(ec, pId, context, false);
	}
}

void ShortTermSyncHandler::executeSyncEndAck(
	EventContext& ec,
	PartitionId pId,
	SyncContext* context,
	SyncResponseInfo& syncResponseInfo,
	NodeId senderNodeId) {

	util::StackAllocator& alloc = ec.getAllocator();
	if (context != NULL) {
		GS_OUTPUT_SYNC(context, "[" << EventTypeUtility::getEventTypeName(TXN_SHORTTERM_SYNC_END_ACK) << ":START]", "");
		context->setEvent(TXN_SHORTTERM_SYNC_END_ACK);
		if (pt_->getLSN(pId) != syncResponseInfo.getTargetLsn()) {
			GS_THROW_SYNC_ERROR(
				GS_ERROR_SYM_INVALID_PARTITION_REVISION, context,
				"Target short term sync already removed",
				", owner=" << pt_->dumpNodeAddress(0)
				<< ", owner LSN=" << pt_->getLSN(pId)
				<< ", backup=" << pt_->dumpNodeAddress(senderNodeId)
				<< ", backup LSN=" << syncResponseInfo.getTargetLsn());
		}

		context->setSyncTargetLsn(
			senderNodeId, syncResponseInfo.getTargetLsn());

		if (context->decrementCounter(senderNodeId)) {
			PartitionStatus nextStatus
				= syncMgr_->setShorttermSyncEnd(context);
			clsSvc_->requestChangePartitionStatus(
				ec, alloc, pId, nextStatus, PT_CHANGE_SYNC_END);
			context->endAll();

			if (context->isUseLongSyncLog()) {
				GS_TRACE_SYNC(context, "[OWNER] Catchup promotion sync completed, SYNC_END", "")
			}
			syncMgr_->removeSyncContext(ec, pId, context, false);
		}
	}
}


RedoManager::RedoManager(
		const ConfigTable& config, 
		GlobalVariableSizeAllocator& varAlloc,
		SyncService* syncSvc) :
		partitionNum_(config.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM)),
		varAlloc_(varAlloc),
		contextInfoList_(partitionNum_, RedoInfo()),
		syncSvc_(NULL),
		txnSvc_(NULL),
		clsMgr_(NULL),
		pt_(NULL) {
	UNUSED_VARIABLE(syncSvc);
	UUIDUtils::generate(uuid_);
	uuidString_.resize(UUID_STRING_SIZE);
	UUIDUtils::unparse(uuid_, const_cast<char*>(uuidString_.data()));
	uuidString_.resize(UUID_STRING_SIZE - 1);
	contextMapList_.assign(partitionNum_, ContextMap(varAlloc));
}

RedoManager::~RedoManager() {
	for (size_t pos = 0; pos < contextMapList_.size(); pos++) {
		ContextMap& cxtMap = contextMapList_[pos];
		for (const auto& elem : cxtMap) {
			ALLOC_VAR_SIZE_DELETE(varAlloc_, elem.second);
		}
	}
}

void RedoManager::initialize(ManagerSet& mgrSet) {
	syncSvc_ = mgrSet.syncSvc_;
	txnSvc_ = mgrSet.txnSvc_;
	clsMgr_ = mgrSet.clsMgr_;
	pt_ = mgrSet.pt_;
}

bool RedoManager::put(
	const Event::Source& eventSource, util::StackAllocator& alloc, PartitionId pId, 
	std::string& logFilePath,  std::string& logFileName,
	LogSequentialNumber startLsn, LogSequentialNumber endLsn, std::string& uuidString,
	RequestId &requestId, bool forceApply, RestContext& restCxt) {

	try {
		if (!checkRange(pId, restCxt)) {
			return false;
		}
		if (!checkStandby(restCxt)) {
			return false;
		}
		if (!checkExecutable(pId, restCxt)) {
			return false;
		}
		ContextMap& cxtMap = contextMapList_[pId];
		{
			util::LockGuard<util::Mutex> guard(contextInfoList_[pId].lock_);
			requestId = reserveId(pId);
			ContextMap::iterator it = cxtMap.find(requestId);
			if (it == cxtMap.end()) {
				cxtMap.insert(std::make_pair(requestId,  ALLOC_VAR_SIZE_NEW(varAlloc_)
					RedoContext(varAlloc_, logFilePath, logFileName, startLsn, endLsn, pId, requestId, uuidString_, forceApply)));
				picojson::object redoInfo;
				redoInfo["requestId"] = picojson::value(static_cast<double>(requestId));
				redoInfo["uuid"] = picojson::value(uuidString_);
				restCxt.result_ = picojson::value(redoInfo);
			}
			else {
				GS_THROW_USER_ERROR(GS_ERROR_SYNC_REDO_INTERNAL, "RequestId (" << requestId << ") is already exist");
			}
		}
		sendRequest(eventSource, alloc, pId, requestId, TXN_SYNC_REDO_LOG);
		return true;
	}
	catch (std::exception& e) {
		UTIL_TRACE_EXCEPTION(REDO_MANAGER, e, "");
		util::NormalOStringStream oss;
		oss << "Internal error ocurred  (pId=" << pId << ", uuid = " << uuidString << ", requestId = " << requestId << ")";
		restCxt.setError(WEBAPI_REDO_INVALID_REQUEST_ID, oss.str().c_str());
		return false;
	}
}

bool RedoManager::getStatus(PartitionId pId, std::string& uuidString, RequestId requestId, RestContext& restCxt) {
	try {
		if (!checkRange(pId, restCxt)) {
			return false;
		}

		ContextMap& cxtMap = contextMapList_[pId];
		picojson::value& result = restCxt.result_;
		util::LockGuard<util::Mutex> guard(contextInfoList_[pId].lock_);
		if (requestId == UNDEF_SYNC_REQUEST_ID) {
			if (!checkUuid(uuidString, restCxt, false)) {
				return false;
			}
			RedoContextList list;
			for (const auto& elem : cxtMap) {
				list.append(elem.second);
			}
			JsonUtils::OutStream out(result);
			util::ObjectCoder().encode(out, list);
			return true;
		}
		else {
			if (!checkUuid(uuidString, restCxt, true)) {
				return false;
			}
			ContextMap::iterator it = cxtMap.find(requestId);
			if (it != cxtMap.end()) {
				JsonUtils::OutStream out(result);
				util::ObjectCoder().encode(out, it->second);
				return true;
			}
			else {
				if (requestId < UNDEF_SYNC_REQUEST_ID || requestId >= contextInfoList_[pId].counter_) {
					util::NormalOStringStream oss;
					oss << "Specified requestId not found (pId=" << pId << ", uuid=" << uuidString << ", requestId=" << requestId << ")";
					restCxt.setError(WEBAPI_REDO_REQUEST_ID_IS_NOT_FOUND, oss.str().c_str());
					return false;
				}
				else {
					return true;
				}
			}
		}
	}
	catch (std::exception& e) {
		UTIL_TRACE_EXCEPTION(REDO_MANAGER, e, "");
		util::NormalOStringStream oss;
		oss << "Internal error ocurred  (pId=" << pId << ", uuid = " << uuidString << ", requestId = " << requestId << ")";
		restCxt.setError(WEBAPI_REDO_INVALID_UUID, oss.str().c_str());
		return false;
	}
}

bool RedoManager::cancel(
	const Event::Source& eventSource, util::StackAllocator& alloc, PartitionId pId, std::string& uuidString, RestContext& restCxt, RequestId requestId) {
	try {
		if (!checkRange(pId, restCxt)) {
			return false;
		}
		if (!checkUuid(uuidString, restCxt, true)) {
			return false;
		}
		sendRequest(eventSource, alloc, pId, requestId, TXN_SYNC_REDO_REMOVE);
		return true;
	}
	catch (std::exception& e) {
		UTIL_TRACE_EXCEPTION(REDO_MANAGER, e, "");
		util::NormalOStringStream oss;
		oss << "Internal error ocurred  (pId=" << pId << ", uuid = " << uuidString << ", requestId = " << requestId << ")";
		restCxt.setError(WEBAPI_REDO_INVALID_REQUEST_ID, oss.str().c_str());
		return false;
	}
}

RedoContext* RedoManager::get(PartitionId pId, RequestId requestId) {
	try {
		ContextMap& cxtMap = contextMapList_[pId];
		util::LockGuard<util::Mutex> guard(contextInfoList_[pId].lock_);
		ContextMap::iterator it = cxtMap.find(requestId);
		if (it != cxtMap.end()) {
			return (*it).second;
		}
		return NULL;
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

bool RedoManager::remove(PartitionId pId,  RequestId requestId) {
	ContextMap& cxtMap = contextMapList_[pId];
	util::LockGuard<util::Mutex> guard(contextInfoList_[pId].lock_);
	if (requestId != UNDEF_SYNC_REQUEST_ID) {
		ContextMap::iterator it = cxtMap.find(requestId);
		if (it != cxtMap.end()) {
			ALLOC_VAR_SIZE_DELETE(varAlloc_, it->second);
			cxtMap.erase(it);
			return true;
		}
		return false;
	}
	else {
		for (const auto& elem : cxtMap) {
			ALLOC_VAR_SIZE_DELETE(varAlloc_, elem.second);
		}
		cxtMap.clear();
		return true;
	}
}

int64_t RedoManager::getContextNum(PartitionId pId) {
	util::LockGuard<util::Mutex> guard(contextInfoList_[pId].lock_);
	return contextMapList_[pId].size();
}

int64_t RedoManager::getTotalContextNum() {
	int64_t totalNum = 0;
	for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
		totalNum += getContextNum(pId);
	}
	return totalNum;
}


RequestId RedoManager::reserveId(PartitionId pId) {
	return contextInfoList_[pId].counter_++;
}

bool RedoManager::checkRange(PartitionId pId, RestContext& cxt) {
	if (pId >= partitionNum_) {
		util::NormalOStringStream oss;
		oss << "PartitionId=" << pId << " is out of range";
		cxt.setError(WEBAPI_REDO_INVALID_PARTITION_ID, oss.str().c_str());
		return false;
	}
	return true;
}

bool RedoManager::checkStandby(RestContext& cxt) {
	if (!clsMgr_->getStandbyInfo().isStandby()) {
		util::NormalOStringStream oss;
		oss << "Redo log operation can execute only standby mode" << std::endl;
		cxt.setError(WEBAPI_REDO_NOT_STANDBY_MODE, oss.str().c_str());
		return false;
	}
	return true;
}

bool RedoManager::checkUuid(std::string& uuidString, RestContext& cxt, bool check) {
	if (check && uuidString.empty()) {
		util::NormalOStringStream oss;
		oss << "Uuid must be specified (current=" << uuidString_ << ")";
		cxt.setError(WEBAPI_REDO_INVALID_UUID, oss.str().c_str());
		return false;
	}
	if (!uuidString.empty() && uuidString != uuidString_) {
		util::NormalOStringStream oss;
		oss << "Uuid is not match (specified=" << uuidString << ", expected=" << uuidString_ << ")";
		cxt.setError(WEBAPI_REDO_INVALID_UUID, oss.str().c_str());
		return false;
	}
	return true;
}

bool RedoManager::checkExecutable(PartitionId pId, RestContext& cxt) {
	try {
		syncSvc_->getManager()->checkActiveStatus();
	}
	catch (std::exception& e) {
		util::NormalOStringStream oss;
		oss << "Invalid cluster status (expected=MASTER or FOLLOWER, actual=" << pt_->dumpCurrentClusterStatus() << ")";
		cxt.setError(WEBAPI_REDO_INVALID_CLUSTER_STATUS, oss.str().c_str());
		return false;
	}
	if (!pt_->isOwner(pId)) {
		util::NormalOStringStream oss;
		oss << "Invalid partition role (expected=OWNER, actual=" << pt_->dumpPartitionRoleStatus(pt_->getPartitionRoleStatus(pId)) << ")";
		cxt.setError(WEBAPI_REDO_NOT_OWNER, oss.str().c_str());
		return false;
	}
	if (pt_->getPartitionStatus(pId) != PartitionTable::PT_ON) {
		util::NormalOStringStream oss;
		oss << "Invalid partition status (expected=ON, actual=OFF or SYNC)";
		cxt.setError(WEBAPI_REDO_NOT_OWNER, oss.str().c_str());
		return false;
	}
	return true;
	
}

void RedoManager::sendRequest(
	const Event::Source& eventSource, util::StackAllocator& alloc, PartitionId pId, RequestId requestId, EventType eventType) {
	try {
		SyncRedoRequestInfo syncRequestInfo(alloc);
		syncRequestInfo.requestId_ = requestId;

		Event requestEvent(eventSource, eventType, pId);
		EventByteOutStream out = requestEvent.getOutStream();
		syncSvc_->encode(requestEvent, syncRequestInfo, out);
		txnSvc_->getEE()->add(requestEvent);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}



RedoContext::RedoContext(GlobalVariableSizeAllocator& varAlloc, std::string& logFilePath, std::string& logFileName, LogSequentialNumber startLsn, LogSequentialNumber endLsn,
	PartitionId pId, RequestId requestId, std::string& uuid, bool forceApply) : varAlloc_(varAlloc),
	logFileDir_(logFilePath), logFileName_(logFileName), readOffset_(0), currentLsn_(UNDEF_LSN), 
	orgStartLsn_(startLsn), orgEndLsn_(endLsn), startLsn_(UNDEF_LSN), endLsn_(UNDEF_LSN), prevReadLsn_(UNDEF_LSN),
	status_(RedoStatus::Status::WAIT), errorCode_(0), partitionId_(pId), uuid_(uuid), requestId_(requestId),
	start_(0), end_(0), checkAckCount_(0), ackCount_(0), sequenceNo_(0), isContinue_(false), isOwner_(false),
	senderNodeId_(0), logMgrStats_(NULL), readfileSize_(-1), skipReadLog_(!forceApply), executionStatusValue_(ExecutionStatus::INIT), 
	backupErrorList_(varAlloc_), backupErrorMap_(varAlloc_), counter_(0), retryCount_(0) {
	start_ = util::DateTime::now(false).getUnixTime();
	startTime_ = CommonUtility::getTimeStr(start_);
	util::FileSystem::createPath(logFilePath.c_str(), logFileName.c_str(), logFilePathName_);
}

void RedoContext::begin() {
	status_ = RedoStatus::RUNNING;
}

void RedoContext::end() {
	end_ = util::DateTime::now(false).getUnixTime();
	endTime_ = CommonUtility::getTimeStr(end_);
}

void RedoContext::setException(std::exception& e, const char* str) {
	try {
		end();
		std::string message;
		if (str) {
			message = str;
		}
		UTIL_TRACE_EXCEPTION(REDO_MANAGER, e, message);
		status_ = RedoStatus::FAIL;
		executionStatus_ = getExecutionStatus();
		const util::Exception checkException = GS_EXCEPTION_CONVERT(e, message);
		errorCode_ = checkException.getErrorCode();
		const char* errorStr = checkException.getNamedErrorCode().getName();
		if (errorStr) {
			errorName_ = checkException.getNamedErrorCode().getName();
		}
	}
	catch (std::exception& e2) {
		UTIL_TRACE_EXCEPTION(REDO_MANAGER, e2, "");
	}
}

bool RedoContext::checkLsn() {
	GS_TRACE_INFO(REDO_MANAGER, GS_TRACE_SYNC_REDO_OPERATION,
		"Redo complete (requestId=" << requestId_ << ", startLsn = " << startLsn_ << ", endLsn = " << endLsn_ << ", elapsedMills=" << watch_.elapsedMillis() << ")");
	return true;
}

const char8_t* RedoManager::RedoStatus::Coder::operator()(Status status, const char8_t* name) const {
	UNUSED_VARIABLE(name);
	return getStatusName(status);
}

const char8_t* RedoManager::RedoStatus::Coder::getStatusName(Status status)  {
	switch (status) {
	case RedoStatus::WAIT: return "WAIT";
	case RedoStatus::RUNNING: return "RUNNING";
	case RedoStatus::FINISH: return "FINISH";
	case RedoStatus::WARNING: return "WARNING";
	case RedoStatus::FAIL: return "FAIL";
	default: return "UNDEF";
	}
}

SyncRedoRequestInfo::SyncRedoRequestInfo(util::StackAllocator& alloc) :
	requestId_(RedoManager::UNDEF_SYNC_REQUEST_ID), sequenceNo_(0), startLsn_(UNDEF_LSN), endLsn_(UNDEF_LSN), errorCode_(0), binaryLogRecordsSize_(0), binaryLogRecords_(alloc) {}

void SyncRedoRequestInfo::encode(EventByteOutStream& out) {
	out << requestId_;
	out << sequenceNo_;
	out << startLsn_;
	out << endLsn_;
	out << binaryLogRecordsSize_;
	out << errorCode_;
	if (errorCode_ != 0) {
		StatementHandler::encodeStringData(out, errorName_);
	}
	if (binaryLogRecordsSize_ > 0) {
		StatementHandler::encodeBinaryData(out, binaryLogRecords_.data(), binaryLogRecords_.size());
	}
}

void SyncRedoRequestInfo::decode(EventByteInStream& in, const char8_t* data) {
	UNUSED_VARIABLE(data);

	in >> requestId_;
	in >> sequenceNo_;
	in >> startLsn_;
	in >> endLsn_;
	in >> binaryLogRecordsSize_;
	in >> errorCode_;
	if (errorCode_ > 0) {
		StatementHandler::decodeStringData(in, errorName_);
	}
	if (binaryLogRecordsSize_ > 0) {
		StatementHandler::decodeBinaryData(in, binaryLogRecords_, true);
	}
}

void SyncRedoRequestInfo::initLog() {
	binaryLogRecordsSize_ = 0;
	binaryLogRecords_.clear();
}

std::string SyncRedoRequestInfo::dump() {
	util::NormalOStringStream oss;
	oss << "Request (requestId=" << requestId_ << ", sequenceNo=" << sequenceNo_ << ")";
	return oss.str();
}

#define TRACE_REDO_EXCEPTION(errorCode, message, cxt)       \
	try {                                                       \
		GS_THROW_USER_ERROR(errorCode, message);                \
	}                                                           \
	catch (std::exception & e) {                                \
		cxt.setException(e); \
	}

#define TRACE_REDO_EXCEPTION_BACKUP(errorCode, message)       \
	try {                                                       \
		GS_THROW_USER_ERROR(errorCode, message);                \
	}                                                           \
	catch (std::exception & e) {                                \
		UTIL_TRACE_EXCEPTION(REDO_MANAGER, e, ""); \
	}


void RedoLogHandler::operator()(EventContext& ec, Event& ev) {

	PartitionId pId = ev.getPartitionId();
	EventType eventType = ev.getType();
	NodeId senderNodeId = ClusterService::resolveSenderND(ev);
	util::StackAllocator& alloc = ec.getAllocator();
	SyncRedoRequestInfo syncRedoRequestInfo(alloc);
	RedoManager& redoMgr = syncSvc_->getRedoManager();

	try {
		EVENT_START(ec, ev, txnMgr_, UNDEF_DBID, false);

		util::StackAllocator::Scope scope(alloc);
		EventByteInStream in = ev.getInStream();
		Partition& partition = partitionList_->partition(pId);
		util::LockGuard<util::Mutex> guard(partition.mutex());
		WATCHER_START;
		try {
			syncSvc_->decode(alloc, ev, syncRedoRequestInfo, in);
		}
		catch (std::exception& e) {
			GS_RETHROW_USER_ERROR(e, "Redo decode failed");
		}
		switch (eventType) {
		case TXN_SYNC_REDO_LOG: {
			RedoContext* cxt = redoMgr.get(pId, syncRedoRequestInfo.requestId_);
			if (cxt == NULL) {
				return;
			}
			cxt->senderNodeId_ = senderNodeId;
			switch (cxt->status_) {
			case RedoManager::RedoStatus::WAIT: {
				if (!prepareExecution(ec, *cxt, syncRedoRequestInfo)) {
					return;
				}
				break;
			}
			case RedoManager::RedoStatus::RUNNING: {
				if (!checkAck(*cxt, syncRedoRequestInfo)) {
					return;
				}
				break;
			}
			default:
				break;
			}
			if (!checkStandby(*cxt)) {
				return;
			}
			if (!checkExecutable(*cxt)) {
				return;
			}
			if (!readLogFile(ec, *cxt, syncRedoRequestInfo)) {
				return;
			}
			if (!redoLog(ec, *cxt, syncRedoRequestInfo)) {
				return;
			}
			if (!executeReplication(ec, *cxt, syncRedoRequestInfo)) {
				return;
			}
			if (!postExecution(ec, *cxt, syncRedoRequestInfo)) {
				return;
			}
			break;
		}
		case TXN_SYNC_REDO_LOG_REPLICATION: {
			std::string uuid;
			RedoContext cxt(redoMgr.getAllocator(), pId, redoMgr.getUuid(), senderNodeId);
			do {
				if (!checkStandby(cxt)) {
					break;
				}
				if (!checkExecutable(cxt)) {
					break;
				}
				if (!executeReplicationAck(ec, cxt, syncRedoRequestInfo)) {
					break;
				}
				if (redoLog(ec, cxt, syncRedoRequestInfo)) {
					break;
				}
			} while (false);
			checkBackupError(ec, cxt, syncRedoRequestInfo);
			break;
		}
		case TXN_SYNC_REDO_CHECK: {
			RedoContext* cxt = redoMgr.get(pId, syncRedoRequestInfo.requestId_);
			if (cxt != NULL) {
				int32_t waitTime = 0;
				switch (cxt->status_) {
				case RedoManager::RedoStatus::RUNNING: {
					if (!checkReplicationTimeout(*cxt)) {
						return;
					}
					waitTime = syncMgr_->getConfig().getLogReplicationTimeoutInterval();
					break;
				}
				case RedoManager::RedoStatus::FINISH: {
					break;
				}
				case RedoManager::RedoStatus::FAIL: {
					if (checkDisplayErrorStatus(ec, *cxt)) {
						return;
					}
					waitTime = syncMgr_->getConfig().getReplicationCheckInterval();
					break;
				}
				default:
					break;
				}
				sendSyncRequest(ec, *txnSvc_->getEE(), TXN_SYNC_REDO_CHECK,
					cxt->partitionId_, syncRedoRequestInfo, 0, waitTime);
			}
			break;
		}
		case TXN_SYNC_REDO_REMOVE: {
			RedoContext* cxt = redoMgr.get(pId, syncRedoRequestInfo.requestId_);
			if (cxt != NULL) {
				redoMgr.remove(pId, cxt->requestId_);
			}
			break;
		}
		case TXN_SYNC_REDO_ERROR: {
			RedoContext* cxt = redoMgr.get(pId, syncRedoRequestInfo.requestId_);
			if (cxt != NULL) {
				checkBackupError(ec, *cxt, syncRedoRequestInfo);
			}
			break;
		}
		}
	}
	catch (std::exception& e) {
		util::NormalOStringStream oss;
		oss << "Redo (pId=" << pId << ", event=" << EventTypeUtility::getEventTypeName(ev.getType()) << ") ";
		if (syncRedoRequestInfo.requestId_ != RedoManager::UNDEF_SYNC_REQUEST_ID) {
			RedoContext* cxt = redoMgr.get(pId, syncRedoRequestInfo.requestId_);
			if (cxt) {
				cxt->dump(oss);
				cxt->setException(e, oss.str().c_str());
			}
		}
		else {
			UTIL_TRACE_EXCEPTION(SYNC_SERVICE, e, oss.str());
		}
	}
}

bool RedoLogHandler::isSemiSyncReplication() {
	return false;
}

bool RedoLogHandler::checkStandby(RedoContext& cxt) {
	cxt.executionStatusValue_ = RedoContext::ExecutionStatus::CHECK_STANDBY;
	if (!clsMgr_->getStandbyInfo().isStandby()) {
		TRACE_REDO_EXCEPTION(GS_ERROR_SYNC_REDO_NOT_STANDBY_MODE, "Not standby mode", cxt);
		return false;
	}
	return true;
}

bool RedoLogHandler::prepareExecution(EventContext& ec, RedoContext& cxt, SyncRedoRequestInfo& syncRedoRequestInfo) {
	cxt.executionStatusValue_ = RedoContext::ExecutionStatus::PREPARE_EXECUTION;
	pt_->getPartitionRole(cxt.partitionId_, cxt.role_);
	cxt.isOwner_ = cxt.role_.isOwner();
	syncRedoRequestInfo.startLsn_ = pt_->getLSN(cxt.partitionId_);
	if (cxt.isOwner_ && cxt.status_ == RedoManager::RedoStatus::WAIT) {
		cxt.begin();
		cxt.ackCount_ = cxt.role_.getBackupSize();
		sendSyncRequest(ec, *txnSvc_->getEE(), TXN_SYNC_REDO_CHECK,
			cxt.partitionId_, syncRedoRequestInfo, 0, syncMgr_->getConfig().getLogReplicationTimeoutInterval());
	}
	return true;
}

bool RedoLogHandler::checkAck(RedoContext& cxt, SyncRedoRequestInfo& syncRedoRequestInfo) {
	if (!cxt.isOwner_) {
		return false;
	}
	if (!isSemiSyncReplication()) {
		return true;
	}
	if (cxt.senderNodeId_ == 0) {
		return true;
	}
	if (cxt.checkAckCount_ > 0) {
		return false;
	}
	if (cxt.sequenceNo_ != syncRedoRequestInfo.sequenceNo_) {
		return false;
	}
	cxt.ackCount_++;
	if (cxt.checkAckCount_ != cxt.ackCount_) {
		return false;
	}
	cxt.sequenceNo_++;
	return true;
}

bool RedoLogHandler::checkExecutable(RedoContext& cxt) {
	cxt.executionStatusValue_ = RedoContext::ExecutionStatus::CHECK_CLUSTER;
	try {
		syncMgr_->checkActiveStatus();
	}
	catch (std::exception& e) {
		TRACE_REDO_EXCEPTION(GS_ERROR_SYNC_REDO_DENY_BY_CLUSTER_ROLE,
			"Invalid cluster (" << pt_->dumpCurrentClusterStatus() << ")", cxt);
		return false;
	}

	PartitionId pId = cxt.partitionId_;
	PartitionRole currentRole;
	pt_->getPartitionRole(pId, currentRole);
	if (cxt.isOwner_) {
		if (!(pt_->isOwner(pId) && (pt_->getPartitionStatus(pId) == PartitionTable::PT_ON))) {
			TRACE_REDO_EXCEPTION(GS_ERROR_SYNC_REDO_DENY_BY_PARTITION_ROLE,
				"Deny owner partition role (role=" << pt_->dumpPartitionRoleStatus(pt_->getPartitionRoleStatus(pId))
				<< ", status=" << pt_->dumpPartitionStatus(pt_->getPartitionStatus(pId)) << ")", cxt);
			return false;
		}
	}
	else {
		if (pt_->isOwner(pId)) {
			TRACE_REDO_EXCEPTION(GS_ERROR_SYNC_REDO_DENY_BY_PARTITION_ROLE,
				"Deny backup partition role (role is owner)", cxt);
			return false;
		}
	}
	return true;
}	

bool RedoLogHandler::readLogFile(EventContext& ec, RedoContext& cxt, SyncRedoRequestInfo& syncRedoRequestInfo) {
	cxt.executionStatusValue_ = RedoContext::ExecutionStatus::READ_LOG;
	if (!cxt.isOwner_) {
		return false;
	}
	cxt.isContinue_ = true;
	Partition& partition = partitionList_->partition(cxt.partitionId_);
	util::XArray<uint8_t>& logList = syncRedoRequestInfo.binaryLogRecords_;
	syncRedoRequestInfo.initLog();
	try {
		if (!util::FileSystem::exists(cxt.logFilePathName_.c_str())) {
			GS_THROW_USER_ERROR(GS_ERROR_SYNC_REDO_INVALID_READ_LOG_FILE_PATH,
				"Specified log file is not found (LogFilePath=" << cxt.logFilePathName_ << ")");
		}
		std::unique_ptr<Log> log;
		WALBuffer xWALBuffer(cxt.partitionId_, cxt.logMgrStats_);
		WALBuffer cpWALBuffer(cxt.partitionId_, cxt.logMgrStats_);

		std::unique_ptr<MutexLocker> locker(UTIL_NEW MutexLocker());
		LogManager<MutexLocker> logManager(partition.getConfig(), std::move(locker), getLogStackAllocator(cxt.partitionId_),
			xWALBuffer, cpWALBuffer,
			cxt.logFilePathName_.c_str(), cxt.partitionId_, cxt.logMgrStats_);

		LogIterator<MutexLocker> logIt(&logManager, 0, cxt.readOffset_, cxt.readfileSize_);
		logIt.add(cxt.logFilePathName_);
		int32_t limitSize = syncMgr_->getConfig().getMaxRedoMessageSize();
		LogSequentialNumber currentLsn = UNDEF_LSN;
		LogSequentialNumber &prevLsn = cxt.prevReadLsn_;
		LogSequentialNumber actualLsn = pt_->getLSN(cxt.partitionId_);
		while (logList.size() <= static_cast<size_t>(limitSize)) {
			util::StackAllocator::Scope scope(logManager.getAllocator());
			log = logIt.next(false);
			if (log == NULL) {
				if (logIt.getFileSize() == -1) {
					GS_THROW_USER_ERROR(GS_ERROR_SYNC_REDO_INVALID_READ_LOG_FILE_PATH,
						"Specified log file is not found (LogFilePath=" << cxt.logFilePathName_ << ")");
				}
				else if (logIt.getFileSize() == 0) {
					GS_THROW_USER_ERROR(GS_ERROR_SYNC_REDO_INVALID_READ_LOG,
						"Specified log file is empty (LogFilePath=" << cxt.logFilePathName_ << ")");
				}
				cxt.isContinue_ = false;
				syncRedoRequestInfo.endLsn_ = currentLsn;
				syncRedoRequestInfo.binaryLogRecordsSize_ = logList.size();
				break;
			}
			if (!log->checkSum()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYNC_REDO_INVALID_READ_LOG,
					"Log check sum error(LogFilePath=" << cxt.logFilePathName_ << ")");
			}
			if (!(log->getType() == OBJLog || log->getType() == TXNLog)) {
				continue;
			}
			currentLsn = log->getLsn();
			if (prevLsn == UNDEF_LSN) {
				if (currentLsn > actualLsn + 1) {
					if (cxt.watch_.elapsedMillis() >= static_cast<uint32_t>(syncMgr_->getConfig().getRetryTimeoutInterval())) {
						GS_THROW_USER_ERROR(
							GS_ERROR_SYNC_REDO_READ_LOG_RETRY_TIMEOUT, "Retry timeout (expect lsn=" << currentLsn << ", actual lsn=" << actualLsn << ")");
					}
					else {
						sendSyncRequest(ec, *txnSvc_->getEE(), TXN_SYNC_REDO_LOG,
							cxt.partitionId_, syncRedoRequestInfo, 0, static_cast<uint32_t>(syncMgr_->getConfig().getRetryCheckInterval()));
						cxt.retryCount_++;
						return false;
					}
				}
				syncRedoRequestInfo.startLsn_ = currentLsn;
			}
			else {
				if (currentLsn != prevLsn + 1) {
					GS_THROW_USER_ERROR(GS_ERROR_SYNC_REDO_INVALID_READ_LOG,
						"Invalid log lsn gap (currentLsn=" << currentLsn << ", prevLsn=" << prevLsn << ")");
				}
			}
			prevLsn = currentLsn;
			if ((currentLsn != actualLsn + 1) && cxt.skipReadLog_) {
				continue;
			}
			actualLsn = currentLsn;
			log->encode(logManager.getAllocator(), logList);
		}
		logIt.fin();
		if (logList.size() == 0) {
			complete(cxt);
			return false;
		}
		cxt.readOffset_ = logIt.getOffset();
		cxt.readfileSize_ = logIt.getFileSize();
		syncRedoRequestInfo.endLsn_ = currentLsn;
		syncRedoRequestInfo.binaryLogRecordsSize_ = logList.size();
		return true;
	}
	catch (std::exception& e) {
		syncRedoRequestInfo.initLog();
		cxt.setException(e);
		return false;
	}
}

bool RedoLogHandler::redoLog(EventContext& ec, RedoContext& cxt, SyncRedoRequestInfo& syncRedoRequestInfo) {
	cxt.executionStatusValue_ = RedoContext::ExecutionStatus::REDO;
	util::Stopwatch logWatch;
	util::StackAllocator& alloc = ec.getAllocator();
	Partition& partition = partitionList_->partition(cxt.partitionId_);
	size_t logOffset = 0;
	LogSequentialNumber startLsn = pt_->getLSN(cxt.partitionId_);
	LogSequentialNumber endLsn = startLsn;
	try {
		syncMgr_->checkRestored(cxt.partitionId_);
		partition.redoLogList(&alloc, REDO_MODE_LONG_TERM_SYNC,
			ec.getHandlerStartTime(), ec.getHandlerStartMonotonicTime(),
			reinterpret_cast<uint8_t*>(syncRedoRequestInfo.binaryLogRecords_.data()),
			syncRedoRequestInfo.binaryLogRecords_.size(),
			logOffset, &logWatch, UINT32_MAX);
		endLsn = pt_->getLSN(cxt.partitionId_);
		if (startLsn != endLsn) {
			if (cxt.startLsn_ == UNDEF_LSN) {
				cxt.startLsn_ = startLsn;
			}
			cxt.endLsn_ = endLsn;
		}
		return true;
	}
	catch (std::exception& e) {
		syncRedoRequestInfo.initLog();
		cxt.setException(e);
		LogSequentialNumber endLsn = pt_->getLSN(cxt.partitionId_);
		if (startLsn != endLsn) {
			if (cxt.startLsn_ == UNDEF_LSN) {
				cxt.startLsn_ = startLsn;
			}
			cxt.endLsn_ = endLsn;
		}
		if (pt_->checkClearRole(cxt.partitionId_)) {
			clsSvc_->requestChangePartitionStatus(ec,
				alloc, cxt.partitionId_, PartitionTable::PT_OFF, PT_CHANGE_NORMAL);
			GS_TRACE_ERROR(REDO_MANAGER, GS_TRACE_SYNC_REDO_OPERATION,
				"Role is changed to none (reason=Failed to redo)");
		}
		return false;
	}
}

bool RedoLogHandler::executeReplication(EventContext& ec, RedoContext& cxt, SyncRedoRequestInfo& syncRedoRequestInfo) {
	cxt.executionStatusValue_ = RedoContext::ExecutionStatus::REPLICATION;
	if (!cxt.isOwner_) {
		return false;
	}
	int32_t backupSize = cxt.role_.getBackupSize();
	if (backupSize > 0) {
		cxt.watch_.start();
		cxt.ackCount_ = cxt.checkAckCount_;
		Event requestEvent(ec, TXN_SYNC_REDO_LOG_REPLICATION, cxt.partitionId_);
		EventByteOutStream out = requestEvent.getOutStream();
		syncSvc_->encode(requestEvent, syncRedoRequestInfo, out);
		NodeIdList& backups = cxt.role_.getBackups();
		for (size_t pos = 0; pos < backups.size(); pos++) {
			sendEvent(ec, *txnEE_, backups[pos], requestEvent);
		}
	}
	return true;
}

bool RedoLogHandler::postExecution(EventContext& ec, RedoContext& cxt, SyncRedoRequestInfo& syncRedoRequestInfo) {
	cxt.executionStatusValue_ = RedoContext::ExecutionStatus::POST_EXECUTION;
	cxt.counter_++;
	if (!cxt.isOwner_) {
		return false;
	}
	if (!cxt.isContinue_) {
		if (cxt.checkLsn()) {
			complete(cxt);
		}
		return true;
	}
	sendSyncRequest(ec, *txnSvc_->getEE(), TXN_SYNC_REDO_LOG,
		cxt.partitionId_, syncRedoRequestInfo, 0, 0);
	return false;
}

bool RedoLogHandler::executeReplicationAck(EventContext& ec, RedoContext& cxt, SyncRedoRequestInfo& syncRedoRequestInfo) {
	if (cxt.isOwner_) {
		return false;
	}
	if (!isSemiSyncReplication()) {
		return true; 
	}
	syncRedoRequestInfo.binaryLogRecordsSize_ = 0;
	sendSyncRequest(ec, *txnSvc_->getEE(), TXN_SYNC_REDO_LOG,
		cxt.partitionId_, syncRedoRequestInfo, cxt.senderNodeId_, 0);
	return true;
}

bool RedoLogHandler::checkReplicationTimeout(RedoContext& cxt) {
	if (!cxt.isOwner_) {
		return false;
	}
	return true;
}

bool RedoLogHandler::checkDisplayErrorStatus(EventContext& ec, RedoContext& cxt) {
	UNUSED_VARIABLE(ec);

	if (!cxt.isOwner_) {
		return false;
	}
	if (cxt.status_ != RedoManager::RedoStatus::FAIL && cxt.status_ != RedoManager::RedoStatus::WARNING) {
		return true;
	}
	int64_t current = util::DateTime::now(false).getUnixTime();
	if (cxt.end_ + syncMgr_->getConfig().getLogErrorKeepInterval() <= current) {
		syncSvc_->getRedoManager().remove(cxt.partitionId_, cxt.requestId_);
		return true;
	}
	return false;
}

void RedoLogHandler::complete(RedoContext& cxt) {
	try {
		if (cxt.backupErrorMap_.size() > 0) {
			try {
				GS_THROW_USER_ERROR(GS_ERROR_SYNC_REDO_BACKUP_REDO_FAILED, "");
			}
			catch (std::exception& e) {
				cxt.setException(e);
			}
		}
		else {
			syncSvc_->getRedoManager().remove(cxt.partitionId_, cxt.requestId_);
		}
	}
	catch (std::exception& e) {
		UTIL_TRACE_EXCEPTION(REDO_MANAGER, e, "");
	}
}

void RedoLogHandler::checkBackupError(EventContext& ec, RedoContext& cxt, SyncRedoRequestInfo& syncRedoRequestInfo) {
	try {
		if (cxt.isOwner_) {
			std::string nodeAddress(pt_->dumpNodeAddress(cxt.senderNodeId_));
			cxt.appendBackupError(cxt.senderNodeId_, nodeAddress,
				syncRedoRequestInfo.errorCode_, syncRedoRequestInfo.errorName_, syncRedoRequestInfo.endLsn_);
		}
		else {
			if (cxt.errorCode_ > 0 && !cxt.errorName_.empty()) {
				syncRedoRequestInfo.endLsn_ = pt_->getLSN(cxt.partitionId_);
				syncRedoRequestInfo.initLog();
				syncRedoRequestInfo.errorCode_ = cxt.errorCode_;
				syncRedoRequestInfo.errorName_ = cxt.errorName_;
				sendSyncRequest(ec, *txnSvc_->getEE(), TXN_SYNC_REDO_ERROR,
					cxt.partitionId_, syncRedoRequestInfo, cxt.senderNodeId_, 0);
			}
		}
	}
	catch (std::exception& e) {
		UTIL_TRACE_EXCEPTION(REDO_MANAGER, e, "");
	}
}

RedoContext::~RedoContext() {
	for (const auto& elem : backupErrorList_) {
		ALLOC_VAR_SIZE_DELETE(varAlloc_, elem);
	}
}

void RedoContext::appendBackupError(NodeId nodeId, std::string& node, int32_t errorCode, std::string& errorName, LogSequentialNumber lsn) {
	try {
		std::map<NodeId, ErrorInfo*>::iterator it = backupErrorMap_.find(nodeId);
		if (it != backupErrorMap_.end()) {
			ErrorInfo* errorInfo = (*it).second;
			if (errorInfo) {
				errorInfo->update(errorCode, errorName, lsn);
			}
		}
		else {
			backupErrorList_.push_back(ALLOC_VAR_SIZE_NEW(varAlloc_) ErrorInfo(varAlloc_, node, errorCode, errorName, lsn));
			backupErrorMap_.insert(std::make_pair(nodeId, backupErrorList_.back()));
		}
	}
	catch (std::exception& e) {
		UTIL_TRACE_EXCEPTION(REDO_MANAGER, e, "");
	}
}
