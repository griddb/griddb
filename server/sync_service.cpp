﻿/*
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
#include "chunk_manager.h"
#include "cluster_common.h"
#include "data_store.h"
#include "event_engine.h"
#include "log_manager.h"
#include "recovery_manager.h"
#include "sync_manager.h"
#include "transaction_service.h"
#ifndef _WIN32
#include <signal.h>  
#endif

UTIL_TRACER_DECLARE(CLUSTER_SERVICE);
UTIL_TRACER_DECLARE(SYNC_SERVICE);
UTIL_TRACER_DECLARE(CLUSTER_OPERATION);
UTIL_TRACER_DECLARE(IO_MONITOR);
UTIL_TRACER_DECLARE(SYNC_DETAIL);
UTIL_TRACER_DECLARE(CLUSTER_DUMP);

#define GS_TRACE_CLUSTER_INFO(s) \
	GS_TRACE_INFO(CLUSTER_OPERATION, GS_TRACE_CS_CLUSTER_STATUS, s); \

#define GS_TRACE_CLUSTER_DUMP(s) \
	GS_TRACE_DEBUG(CLUSTER_OPERATION, GS_TRACE_CS_CLUSTER_STATUS, s); 


#define RM_THROW_LOG_REDO_ERROR(errorCode, message) \
	GS_THROW_CUSTOM_ERROR(LogRedoException, , errorCode, message)
#define RM_RETHROW_LOG_REDO_ERROR(cause, message) \
	GS_RETHROW_CUSTOM_ERROR(LogRedoException, GS_ERROR_DEFAULT, cause, message)

SyncService::SyncService(const ConfigTable &config,
		EventEngine::Config &eeConfig, EventEngine::Source source,
		const char8_t *name, SyncManager &syncMgr, ClusterVersionId versionId,
		ServiceThreadErrorHandler &serviceThreadErrorHandler) :
		ee_(createEEConfig(config, eeConfig), source, name),
		syncMgr_(&syncMgr),
		versionId_(versionId),
		serviceThreadErrorHandler_(serviceThreadErrorHandler),
		initailized_(false) 
		{
	try {
		syncMgr_ = &syncMgr;
		ee_.setHandler(SYC_SHORTTERM_SYNC_LOG, recvSyncMessageHandler_);
		ee_.setHandler(SYC_LONGTERM_SYNC_LOG, recvSyncMessageHandler_);
		ee_.setHandler(SYC_LONGTERM_SYNC_CHUNK, recvSyncMessageHandler_);

		ee_.setUnknownEventHandler(unknownSyncEventHandler_);
		ee_.setThreadErrorHandler(serviceThreadErrorHandler_);
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

void SyncService::initialize(ManagerSet &mgrSet) {
	clsSvc_ = mgrSet.clsSvc_;
	txnSvc_ = mgrSet.txnSvc_;
	logMgr_ = mgrSet.logMgr_;
	syncMgr_->initialize(&mgrSet);
	recvSyncMessageHandler_.initialize(mgrSet);
	serviceThreadErrorHandler_.initialize(mgrSet);
	initailized_ = true;
}

SyncService::~SyncService() {
	ee_.shutdown();
	ee_.waitForShutdown();
}

/*!
	@brief Sets EventEngine config
*/
EventEngine::Config &SyncService::createEEConfig(
		const ConfigTable &config, EventEngine::Config &eeConfig) {
	EventEngine::Config tmpConfig;
	eeConfig = tmpConfig;
	eeConfig.setServerAddress(
			config.get<const char8_t *>(CONFIG_TABLE_SYNC_SERVICE_ADDRESS),
			config.getUInt16(CONFIG_TABLE_SYNC_SERVICE_PORT));
	eeConfig.setPartitionCount(config.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM));
	eeConfig.setAllAllocatorGroup(ALLOCATOR_GROUP_SYNC);
	return eeConfig;
}

/*!
	@brief Starts SyncService
*/
void SyncService::start() {
	try {
		if (!initailized_) {
			GS_THROW_USER_ERROR(GS_ERROR_CS_SERVICE_NOT_INITIALIZED, "");
		}
		ee_.start();
	}
	catch (std::exception &e) {
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
void SyncService::decode(
	util::StackAllocator &alloc, EventEngine::Event &ev, T &t, EventByteInStream &in) {
	try {
//		const size_t currentPosition = in.base().position();
		util::XArray<char8_t> buffer(alloc);
		const char8_t *eventBuffer = reinterpret_cast<const char8_t *>(
				ev.getMessageBuffer().getXArray().data() +
				ev.getMessageBuffer().getOffset());

		uint8_t decodedVersionId;
		in >> decodedVersionId;
		checkVersion(decodedVersionId);

		uint16_t logVersion;
		in >> logVersion;
		StatementHandler::checkLogVersion(logVersion);
		t.decode(in, eventBuffer);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
}

/*!
	@brief Checks the version of synchronization message
*/
void SyncService::checkVersion(uint8_t versionId) {
	if (versionId != versionId_) {
		GS_THROW_USER_ERROR(GS_ERROR_CS_ENCODE_DECODE_VERSION_CHECK,
				"Cluster message version is unmatched,  acceptableClusterVersion="
				<< static_cast<int32_t>(versionId_)
				<< ", connectClusterVersion=" << static_cast<int32_t>(versionId));
	}
}

uint16_t SyncService::getLogVersion(PartitionId pId) {
	return logMgr_->getLogVersion(pId);
}

template <class T>
void SyncService::encode(Event &ev, T &t, EventByteOutStream &out) {
	try {
//		const size_t currentPosition = out.base().position();
		out << versionId_;
		uint16_t logVersion = getLogVersion(ev.getPartitionId());
		out << logVersion;
		t.encode(out);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
}

void SyncHandler::initialize(const ManagerSet &mgrSet) {
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
		logMgr_ = mgrSet.logMgr_;
		chunkMgr_ = mgrSet.chunkMgr_;
		ds_ = mgrSet.ds_;
		cpSvc_ = mgrSet.cpSvc_;
		recoveryMgr_ = mgrSet.recoveryMgr_;
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Requests DropPartition
*/
void SyncService::requestDrop(const Event::Source &eventSource,
		util::StackAllocator &alloc, PartitionId pId,
		bool isForce, PartitionRevisionNo revision) {
	try {
		DropPartitionInfo dropPartitionInfo(
				alloc, TXN_DROP_PARTITION, syncMgr_, pId, isForce, revision);	
		Event dropEvent(eventSource, TXN_DROP_PARTITION, pId);
		clsSvc_->encode(dropEvent, dropPartitionInfo);
		txnSvc_->getEE()->add(dropEvent);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void SyncService::notifyCheckpointLongSyncReady(
		EventContext &ec, PartitionId pId,
		LongtermSyncInfo *syncInfo, bool errorOccured) {
	clsSvc_->request(ec, TXN_LONGTERM_SYNC_PREPARE_ACK,
			pId, txnSvc_->getEE(), *syncInfo);
}

void SyncHandler::sendEvent(
		EventContext &ec, EventEngine &ee, NodeId targetNodeId,
		Event &ev, int32_t delayTime) {


	if (targetNodeId == SELF_NODEID) {
		if (delayTime == UNDEF_DELAY_TIME) {
			ee.add(ev);
		}
		else {
			ee.addTimer(ev, delayTime);
		}
	}
	else {
		EventRequestOption sendOption;
		if (delayTime != UNDEF_DELAY_TIME) {
			sendOption.timeoutMillis_ = delayTime;
		}
		else {
			sendOption.timeoutMillis_ = EE_PRIORITY_HIGH;
		}
		const NodeDescriptor &nd = ee.getServerND(targetNodeId);
		ee.send(ev, nd, &sendOption);
	}
}

/*!
	@brief Handler Operator
*/
void ShortTermSyncHandler::operator()(EventContext &ec, Event &ev) {

	SyncContext *context = NULL;
	SyncContext *longSyncContext = NULL;
	PartitionId pId = ev.getPartitionId();
	EventType eventType = ev.getType();
//	const NodeDescriptor &senderNd = ev.getSenderND();
	NodeId senderNodeId = ClusterService::resolveSenderND(ev);
	SyncId longSyncId;
	try {
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);
//		SyncVariableSizeAllocator &varSizeAlloc = syncMgr_->getVariableSizeAllocator();
		EventByteInStream in = ev.getInStream();
		SyncRequestInfo syncRequestInfo(alloc, eventType, syncMgr_, pId,
				clsSvc_->getEE(), MODE_SHORTTERM_SYNC);
		SyncResponseInfo syncResponseInfo(alloc, eventType, syncMgr_, pId,
				MODE_SHORTTERM_SYNC, pt_->getLSN(pId));

		WATCHER_START;

		switch (eventType) {

		case TXN_SHORTTERM_SYNC_REQUEST: {
			syncSvc_->decode(alloc, ev, syncRequestInfo, in);
			decodeLongSyncRequestInfo(pId, in, longSyncId, longSyncContext);
			syncMgr_->checkExecutable(TXN_SHORTTERM_SYNC_REQUEST, pId,
					syncRequestInfo.getPartitionRole());
			context = syncMgr_->createSyncContext(ec, pId,
					syncRequestInfo.getPartitionRole().getRevision(),
					MODE_SHORTTERM_SYNC, PartitionTable::PT_OWNER);
			context->setLongSyncId(longSyncId);


			executeSyncRequest(ec, ev, pId, context,
					syncRequestInfo, syncResponseInfo);

			checkOwner(pt_->getLSN(pId), context);
			break;
		}
		case TXN_SHORTTERM_SYNC_START: {
			syncSvc_->decode(alloc, ev, syncRequestInfo, in);
			syncMgr_->checkExecutable(TXN_SHORTTERM_SYNC_START, pId,
					syncRequestInfo.getPartitionRole());
			
			executeSyncStart(ec, ev, pId, context,
					syncRequestInfo, syncResponseInfo, senderNodeId);
			break;
		}
		case TXN_SHORTTERM_SYNC_START_ACK: {
			syncSvc_->decode(alloc, ev, syncResponseInfo, in);
			SyncId syncId = syncResponseInfo.getSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			syncMgr_->checkExecutable(TXN_SHORTTERM_SYNC_START_ACK, pId,
					syncResponseInfo.getPartitionRole());


			executeSyncStartAck(ec, ev, pId, context,
					syncRequestInfo, syncResponseInfo, senderNodeId);
			break;
		}
		case TXN_SHORTTERM_SYNC_LOG: {
			syncSvc_->decode(alloc, ev, syncRequestInfo, in);
			SyncId syncId = syncRequestInfo.getBackupSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			syncMgr_->checkExecutable(
					TXN_SHORTTERM_SYNC_LOG, pId, syncRequestInfo.getPartitionRole());

			executeSyncLog(ec, ev, pId, context,
					syncRequestInfo, syncResponseInfo, senderNodeId);
			break;
		}
		case TXN_SHORTTERM_SYNC_LOG_ACK: {
			syncSvc_->decode(alloc, ev, syncResponseInfo, in);
			SyncId syncId = syncResponseInfo.getSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			syncMgr_->checkExecutable(TXN_SHORTTERM_SYNC_LOG_ACK, pId,
					syncResponseInfo.getPartitionRole());

			executeSyncLogAck(ec, ev, pId, context,
					syncRequestInfo, syncResponseInfo, senderNodeId);
			break;
		}
		case TXN_SHORTTERM_SYNC_END: {
			syncSvc_->decode(alloc, ev, syncRequestInfo, in);
			SyncId syncId = syncRequestInfo.getBackupSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			syncMgr_->checkExecutable(
				TXN_SHORTTERM_SYNC_END, pId, syncRequestInfo.getPartitionRole());
			executeSyncEnd(ec, ev, pId, context,
					syncRequestInfo, syncResponseInfo, senderNodeId);
			break;
		}
		case TXN_SHORTTERM_SYNC_END_ACK: {
			syncSvc_->decode(alloc, ev, syncResponseInfo, in);
			SyncId syncId = syncResponseInfo.getSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			syncMgr_->checkExecutable(TXN_SHORTTERM_SYNC_END_ACK, pId,
					syncResponseInfo.getPartitionRole());
			executeSyncEndAck(ec, ev, pId, context,
					syncRequestInfo, syncResponseInfo, senderNodeId);
			break;
		}
		default: {
			GS_THROW_USER_ERROR(GS_ERROR_SYNC_EVENT_TYPE_INVALID, ""); 
		}
		}
		WATCHER_END_SYNC(getEventTypeName(eventType), pId);
	}
	catch (UserException &e) {
//			int32_t errorCode = e.getErrorCode();
		if (context != NULL) {
			UTIL_TRACE_EXCEPTION(SYNC_DETAIL, e, "Short term sync faild, pId=" << pId 
				<< ", revision=" << context->getPartitionRevision().sequentialNumber_ 
				<< ", event=" << getEventTypeName(eventType));
			addCheckEndEvent(ec, pId,ec.getAllocator(), MODE_SHORTTERM_SYNC, context, true);
		}
		else {
			TRACE_SYNC_EXCEPTION(
				e, eventType, pId, ERROR, "Short term sync operation failed");
		}
		if (context == NULL && longSyncId.isValid()) {
			longSyncContext = syncMgr_->getSyncContext(pId, longSyncId);
			if (longSyncContext) {
				syncMgr_->removeSyncContext(ec, pId, longSyncContext, true);
			}
		}
	}
	catch (LockConflictException &e) {
//			int32_t errorCode = e.getErrorCode();
		if (context != NULL) {
			addCheckEndEvent(ec, pId,ec.getAllocator(), MODE_SHORTTERM_SYNC, context, true);
		}
	}
	catch (LogRedoException &e) {
//			int32_t errorCode = e.getErrorCode();
		TRACE_SYNC_EXCEPTION(e, eventType, pId, ERROR,
				"Short term sync failed, log redo exception is occurred.");
		if (context != NULL) {
				addCheckEndEvent(ec, pId,ec.getAllocator(), MODE_SHORTTERM_SYNC, context, true);
		}
	}
	catch (std::exception &e) {
		if (context != NULL) {
			addCheckEndEvent(ec, pId,ec.getAllocator(), MODE_SHORTTERM_SYNC, context, true);
		}
		clsSvc_->setError(ec, &e);
	}
}

/*!
	@brief Handler Operator
*/
void LongTermSyncHandler::operator()(EventContext &ec, Event &ev) {
	
	SyncContext *context = NULL;
	PartitionId pId = ev.getPartitionId();
	EventType eventType = ev.getType();
//	const NodeDescriptor &senderNd = ev.getSenderND();
	NodeId senderNodeId = ClusterService::resolveSenderND(ev);
	

	try {
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);
//		SyncVariableSizeAllocator &varSizeAlloc = syncMgr_->getVariableSizeAllocator();
		EventByteInStream in = ev.getInStream();
		SyncRequestInfo syncRequestInfo(alloc, eventType, syncMgr_, pId,
				clsSvc_->getEE(), MODE_LONGTERM_SYNC);
		SyncResponseInfo syncResponseInfo(alloc, eventType, syncMgr_, pId,
				MODE_LONGTERM_SYNC, pt_->getLSN(pId));

		WATCHER_START;
		switch (eventType) {
		case TXN_LONGTERM_SYNC_REQUEST: {
			syncSvc_->decode(alloc, ev, syncRequestInfo, in);
			syncMgr_->checkExecutable(TXN_LONGTERM_SYNC_REQUEST, pId,
					syncRequestInfo.getPartitionRole());
			context = syncMgr_->createSyncContext(
					ec, pId, syncRequestInfo.getPartitionRole().getRevision(),
					MODE_LONGTERM_SYNC, PartitionTable::PT_OWNER);

			executeSyncRequest(ec, ev, pId, context, syncRequestInfo, syncResponseInfo);
			break;
		}
		case TXN_LONGTERM_SYNC_PREPARE_ACK: {
			LongtermSyncInfo syncInfo;
			clsSvc_->decode(alloc, ev, syncInfo);
			SyncId syncId;
			syncId.contextId_ = syncInfo.getId();
			syncId.contextVersion_ = syncInfo.getVersion();
			context = syncMgr_->getSyncContext(pId, syncId);
			if (context == NULL) {
				return;
			}
			PartitionRole checkRole;
			checkRole.set(context->getPartitionRevision());
			syncMgr_->checkExecutable(TXN_LONGTERM_SYNC_PREPARE_ACK, pId,
				checkRole);
			executeSyncPrepareAck(ec, ev, pId, context, syncRequestInfo,
					syncResponseInfo, syncInfo);
			break;
		}
		case TXN_LONGTERM_SYNC_START: {
			syncSvc_->decode(alloc, ev, syncRequestInfo, in);
			syncMgr_->checkExecutable(TXN_LONGTERM_SYNC_START, pId,
					syncRequestInfo.getPartitionRole());

			executeSyncStart(ec, ev, pId, context, syncRequestInfo,
					syncResponseInfo, senderNodeId);
			break;
		}
		case TXN_LONGTERM_SYNC_START_ACK: {
			syncSvc_->decode(alloc, ev, syncResponseInfo, in);
			SyncStat &stat = clsMgr_->getSyncStat();
			stat.init(true);
			SyncId syncId = syncResponseInfo.getSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			syncMgr_->checkExecutable(TXN_LONGTERM_SYNC_START_ACK, pId,
					syncResponseInfo.getPartitionRole());
			executeSyncStartAck(ec, ev, pId, context, syncRequestInfo,
					syncResponseInfo, senderNodeId);
			break;
		}
		case TXN_LONGTERM_SYNC_CHUNK: {
			syncSvc_->decode(alloc, ev, syncRequestInfo, in);
			SyncId syncId = syncRequestInfo.getBackupSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			syncMgr_->checkExecutable(TXN_LONGTERM_SYNC_CHUNK, pId,
					syncRequestInfo.getPartitionRole());
			executeSyncChunk(ec, ev, pId, context, syncRequestInfo,
					syncResponseInfo, senderNodeId);
			break;
		}
		case TXN_LONGTERM_SYNC_CHUNK_ACK: {
			SyncId syncId;
			if (senderNodeId == SELF_NODEID) {
				LongtermSyncInfo syncInfo;
				clsSvc_->decode(alloc, ev, syncInfo);
				syncId.contextId_ = syncInfo.getId();
				syncId.contextVersion_ = syncInfo.getVersion();
				context = syncMgr_->getSyncContext(pId, syncId);
				if (context == NULL) {
					return;
				}
				util::XArray<NodeId> tmpCatchups(alloc);
				context->getSyncTargetNodeIds(tmpCatchups);
				if (tmpCatchups.size() != 1) {
					return;
				}
				if (!context->isSyncStartCompleted()) {
					return;
				}
				SyncId catchupSyncId;
				context->getCatchupSyncId(catchupSyncId);
				senderNodeId = tmpCatchups[0];
				PartitionRole nextRole;
				pt_->getPartitionRole(pId, nextRole);
				syncMgr_->checkExecutable(TXN_LONGTERM_SYNC_CHUNK_ACK, pId, nextRole);
				if (context->isSyncCheckpointPending()) {
					return;
				}
			}
			else {
				syncSvc_->decode(alloc, ev, syncResponseInfo, in);
				syncId = syncResponseInfo.getSyncId();
				context = syncMgr_->getSyncContext(pId, syncId);
				if (context == NULL) {
					return;
				}
				syncMgr_->checkExecutable(TXN_LONGTERM_SYNC_CHUNK_ACK, pId,
					syncResponseInfo.getPartitionRole());
				context->setSyncCheckpointPending(false);
			}
			executeSyncChunkAck(ec, ev, pId, context, syncRequestInfo,
					syncResponseInfo, senderNodeId);
			break;
		}
		case TXN_LONGTERM_SYNC_LOG: {
			syncSvc_->decode(alloc, ev, syncRequestInfo, in);
			SyncId syncId = syncRequestInfo.getBackupSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			syncMgr_->checkExecutable(
					TXN_LONGTERM_SYNC_LOG, pId, syncRequestInfo.getPartitionRole());
			executeSyncLog(ec, ev, pId, context, syncRequestInfo,
					syncResponseInfo, senderNodeId);
			break;
		}
		case TXN_LONGTERM_SYNC_LOG_ACK: {
			syncSvc_->decode(alloc, ev, syncResponseInfo, in);
			SyncId syncId = syncResponseInfo.getSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			syncMgr_->checkExecutable(TXN_LONGTERM_SYNC_LOG_ACK, pId,
					syncResponseInfo.getPartitionRole());
			executeSyncLogAck(ec, ev, pId, context, syncRequestInfo,
					syncResponseInfo, senderNodeId);
			break;
		}
		default: { GS_THROW_USER_ERROR(GS_ERROR_SYNC_EVENT_TYPE_INVALID, ""); }
		}
		WATCHER_END_SYNC(getEventTypeName(eventType), pId);
	}
	catch (UserException &e) {
		TRACE_SYNC_EXCEPTION(e, eventType, pId, WARNING,
				"Long term sync operation is failed.");
		bool isPending = false;
			int32_t errorCode = e.getErrorCode();
		if (context != NULL) {
			if (errorCode != GS_ERROR_SYM_INVALID_PARTITION_REVISION
				&& errorCode != GS_ERROR_SYM_INVALID_PARTITION_ROLE) {
				UTIL_TRACE_EXCEPTION(SYNC_SERVICE, e, "LONGTERM SYNC FAILED, pId=" << pId 
					<< ", ssn=" <<  context->getSequentialNumber() 
					<< ", revision=" << context->getPartitionRevision().sequentialNumber_ 
					<< ", event=" << getEventTypeName(eventType));
				if (context->isCatchupSync()) {
					isPending = true;
				}
			}
			if (!isPending) {
				addCheckEndEvent(ec, pId,ec.getAllocator(), MODE_LONGTERM_SYNC, context, true);
			}
		}
	}
	catch (LockConflictException &e) {
//			int32_t errorCode = e.getErrorCode();
		TRACE_SYNC_EXCEPTION(e, eventType, pId, ERROR,
				"Long term sync failed, checkpoint-sync lock confict exception is occurred, retry after "
				<< syncMgr_->getExtraConfig().getLockConflictPendingInterval() / 1000 << " sec");
		txnEE_->addTimer(
			ev, syncMgr_->getExtraConfig().getLockConflictPendingInterval());
	}
	catch (LogRedoException &e) {
//			int32_t errorCode = e.getErrorCode();
		TRACE_SYNC_EXCEPTION(e, eventType, pId, ERROR,
				"Long term sync failed, log redo exception is occurred."
				<< syncMgr_->getExtraConfig().getLockConflictPendingInterval());
		if (context != NULL) {
			addCheckEndEvent(ec, pId,ec.getAllocator(), MODE_LONGTERM_SYNC, context, true);
		}
	}
	catch (std::exception &e) {
		if (context != NULL) {
			addCheckEndEvent(ec, pId,ec.getAllocator(), MODE_LONGTERM_SYNC, context, true);
		}
		clsSvc_->setError(ec, &e);
	}
}

/*!
	@brief Gets log information satifies a presetting condition
*/
bool SyncRequestInfo::getLogs(PartitionId pId, int64_t sId,
		LogManager *logMgr, CheckpointService *cpSvc, bool useLongSyncLog, bool isCheck) {

	bool isSyncLog = false;
	LogRecord logRecord;
	LogSequentialNumber lastLsn = 0;
	logRecord.lsn_ = request_.startLsn_;
	int32_t callLogCount = 0;
	condition_.logSize_ = syncMgr_->getConfig().getMaxMessageSize();

	try {
		binaryLogRecords_.clear();
		LogCursor cursor;
		if (request_.syncMode_ == MODE_LONGTERM_SYNC || useLongSyncLog) {
			if (!cpSvc->getLongSyncLog(sId, request_.startLsn_, cursor)) {
				if (!isCheck) {
					GS_TRACE_CLUSTER_INFO("Long term log find failed, pId=" 
						<< pId << ", SSN=" << sId << ", start Lsn=" << request_.startLsn_);
				}
				GS_THROW_USER_ERROR(
					GS_ERROR_SYNC_LOG_GET_FAILED,
					"Long term log find failed, start Lsn=" << request_.startLsn_);
			}
		}
		else {
			if (!logMgr->findLog(cursor, pId, request_.startLsn_)) {
				GS_TRACE_CLUSTER_INFO("Short term log find failed, pId=" 
					<< pId << ", SSN=" << sId << ", start Lsn=" << request_.startLsn_);
				GS_THROW_USER_ERROR(
					GS_ERROR_SYNC_LOG_GET_FAILED,
					"Short term log find failed, start Lsn=" << request_.startLsn_);
			}
		}

		while (cursor.nextLog(logRecord, binaryLogRecords_, pId) &&
			logRecord.lsn_ <= request_.endLsn_ &&
			binaryLogRecords_.size() <= condition_.logSize_) {
			callLogCount++;
			lastLsn = logRecord.lsn_;
			assert(pId == logRecord.partitionId_);
		}
		if (request_.endLsn_ <= lastLsn) {
			isSyncLog = true;
		}
		request_.endLsn_ = lastLsn;
		request_.binaryLogSize_ = static_cast<uint32_t>(binaryLogRecords_.size());
		return isSyncLog;
	}
	catch (std::exception &e) {
		request_.endLsn_ = lastLsn;
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Gets Chunks
*/
bool SyncRequestInfo::getChunks(util::StackAllocator &alloc, PartitionId pId, PartitionTable *pt,
		LogManager *logMgr, ChunkManager *chunkMgr, CheckpointService *cpSvc, int64_t sId, uint64_t &totalCount) {
	bool lastChunkGet = false;
	int32_t callLogCount = 0;
	try {
		chunks_.clear();
		binaryLogRecords_.clear();
		uint32_t chunkSize = condition_.chunkSize_;
		request_.numChunk_ = 0;
		request_.binaryLogSize_ = 0;
		uint64_t resultSize;
//		const PartitionGroupId pgId = pt->getPartitionGroupId(pId);
		for (uint32_t pos = 0;pos < condition_.chunkNum_;pos++) {
			uint8_t *chunk = ALLOC_NEW(alloc) uint8_t [chunkSize];
			uint64_t completedCount;
			uint64_t readyCount;
			lastChunkGet = cpSvc->getLongSyncChunk(
					sId, chunkSize, chunk, resultSize,
					totalCount, completedCount, readyCount);
			if (totalCount !=  completedCount && resultSize == 0) {
				break;
			}
			chunks_.push_back(chunk);
			request_.numChunk_++;
			if (lastChunkGet) break;
		}
		if (lastChunkGet) {
			LogCursor cursor;
			if (!cpSvc->getLongSyncCheckpointStartLog(sId, cursor)) {
				GS_THROW_USER_ERROR(GS_ERROR_SYNC_CHUNK_GET_FAILED, 
					"Long term sync get checkpoint start log failed, pId=" << pId);
			}
			LogRecord logRecord;
			if (!cursor.nextLog(logRecord, binaryLogRecords_, pId,
					LogManager::LOG_TYPE_CHECKPOINT_START)) {
				GS_THROW_USER_ERROR(GS_ERROR_SYNC_CHUNK_GET_FAILED, "");
				callLogCount++;
			}
			request_.binaryLogSize_ =
				static_cast<uint32_t>(binaryLogRecords_.size());
		}
		return lastChunkGet;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Handler Operator
*/
void SyncCheckEndHandler::operator()(EventContext &ec, Event &ev) {
	SyncContext *context = NULL;
	PartitionId pId = ev.getPartitionId();
	EventType eventType = ev.getType();
	try {
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);
		EventByteInStream in = ev.getInStream();
		SyncCheckEndInfo syncCheckEndInfo(
				alloc, TXN_SYNC_TIMEOUT, syncMgr_, pId, MODE_SYNC_TIMEOUT, context);
		syncSvc_->decode(alloc, ev, syncCheckEndInfo, in);
		SyncId syncId = syncCheckEndInfo.getSyncId();
		context = syncMgr_->getSyncContext(pId, syncId);
		syncMgr_->checkExecutable(
			TXN_SYNC_TIMEOUT, pId, syncCheckEndInfo.getPartitionRole());

		if (context != NULL) {
//			NodeId currentOwner = pt_->getOwner(pId);
			PartitionRevisionNo nextRevision = pt_->getPartitionRevision(pId, PartitionTable::PT_CURRENT_OB);
			if (syncCheckEndInfo.getRevisionNo() < nextRevision) {
				syncMgr_->removeSyncContext(ec, pId, context, true);
				return;
			}
			if (context->getSyncMode() == MODE_SHORTTERM_SYNC && 
					pt_->getPartitionStatus(pId) != PartitionTable::PT_ON) {
//				PartitionRole &checkRole = syncCheckEndInfo.getPartitionRole();
				if (context->checkTotalTime(
					clsMgr_->getConfig().getShortTermTimeoutInterval())) {
					GS_TRACE_SYNC(context, "Shortterm sync timeout", "");
				}
				else {
					GS_TRACE_SYNC(context, "Shortterm sync error", "");
				}
//				PartitionRoleStatus roleStatus = pt_->getPartitionRoleStatus(pId);
				PartitionRole currentRole;
				pt_->getPartitionRole(pId, currentRole);
				pt_->clear(pId, PartitionTable::PT_CURRENT_OB);
				PartitionRole afterRole;
				pt_->getPartitionRole(pId, afterRole);
				clsSvc_->requestChangePartitionStatus(ec,
						alloc, pId, PartitionTable::PT_OFF, PT_CHANGE_SYNC_END);
			}
			if (context->getSyncMode() == MODE_LONGTERM_SYNC) {
				if (eventType == TXN_SYNC_CHECK_END) {
				}
				PartitionRole catchupRole;
				pt_->getPartitionRole(pId, catchupRole);
				catchupRole.clearCatchup();
				pt_->setPartitionRole(pId, catchupRole);
			}
			if (context->getSyncMode() == MODE_SHORTTERM_SYNC ||
				(context->getSyncMode() == MODE_LONGTERM_SYNC && !context->isCatchupSync())) {
					if (context->getSyncMode() == MODE_LONGTERM_SYNC) {
						GS_TRACE_SYNC(context, "Remove long term sync context", "");
					}
				syncMgr_->removeSyncContext(ec, pId, context, true);
			}
		}
	}
	catch (UserException &e) {
		UTIL_TRACE_EXCEPTION_WARNING(SYNC_SERVICE, e, "");
		if (context) {
			syncMgr_->removeSyncContext(ec, pId, context, true);
		}
	}
	catch (std::exception &e) {
		if (context) {
		syncMgr_->removeSyncContext(ec, pId, context, true);
		}
		clsSvc_->setError(ec, &e);
	}
}

/*!
	@brief Handler Operator
*/
void DropPartitionHandler::operator()(EventContext &ec, Event &ev) {
	PartitionId pId = ev.getPartitionId();
	EventType eventType = ev.getType();
	try {
		clsMgr_->checkNodeStatus();
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);
		DropPartitionInfo dropPartitionInfo(
				alloc, TXN_DROP_PARTITION, syncMgr_, pId, false);
		clsSvc_->decode(alloc, ev, dropPartitionInfo);
		if (!dropPartitionInfo.isForce()) {
			if (pt_->getLSN(pId) == 0 && !chunkMgr_->existPartition(pId)) {
				return;
			}
			if (pt_->getPartitionRoleStatus(pId) != PartitionTable::PT_NONE) {
				return;
			}
			if (pt_->getPartitionStatus(pId) != PartitionTable::PT_OFF) {
				return;
			}
			if (pt_->getPartitionRevision(pId, 0) != dropPartitionInfo.getPartitionRevision()) {
				return;
			}
			if (pt_->getPartitionRoleRevision(pId, PartitionTable::PT_CURRENT_OB)
					!= dropPartitionInfo.getPartitionRevision()) {
				return;
			}
			if (pt_->isCatchup(pId, 0, PartitionTable::PT_CURRENT_OB)) {
				return;
			}
		}
		const util::DateTime &now = ec.getHandlerStartTime();
		const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
		const TransactionManager::ContextSource src(eventType);
		TransactionContext &txn = txnMgr_->put(alloc, pId, TXN_EMPTY_CLIENTID, src, now, emNow);
		DataStore::Latch latch(txn, pId, ds_, clsSvc_);
		{
			SyncPartitionLock partitionLock(txnMgr_, pId);
			syncMgr_->removePartition(pId);
			LogSequentialNumber prevLsn = pt_->getLSN(pId, 0);
			recoveryMgr_->dropPartition(txn, alloc, pId);
			TRACE_CLUSTER_NORMAL_OPERATION(INFO,
					"Drop partition completed (pId=" << pId << ", LSN=" << prevLsn << ")");
		}
		clsSvc_->requestChangePartitionStatus(ec,
			alloc, pId, PartitionTable::PT_OFF, PT_CHANGE_NORMAL);
	}
	catch (UserException &e) {
		UTIL_TRACE_EXCEPTION_DEBUG(SYNC_SERVICE, e, "");
	}
	catch (LockConflictException &e) {
		UTIL_TRACE_EXCEPTION(SYNC_SERVICE, e, 
			"Drop partition pending, pId=" << pId << ", reason=" << GS_EXCEPTION_MESSAGE(e));
	}
	catch (std::exception &e) {
		clsSvc_->setError(ec, &e);
	}
}

/*!
	@brief Handler Operator
*/
void RecvSyncMessageHandler::operator()(EventContext &ec, Event &ev) {
	SyncContext *context = NULL;
	PartitionId pId = ev.getPartitionId();
	EventType txnEventType;
	EventType syncEventType = ev.getType();
	try {
		clsMgr_->checkNodeStatus();
		util::StackAllocator &alloc = ec.getAllocator();
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
		default: { GS_THROW_USER_ERROR(GS_ERROR_SYNC_EVENT_TYPE_INVALID, ""); }
		}

		SyncRequestInfo syncRequestInfo(
			alloc, syncEventType, syncMgr_, pId, clsSvc_->getEE());
		syncSvc_->decode(alloc, ev, syncRequestInfo, in);
		syncMgr_->checkExecutable(
				txnEventType, pId, syncRequestInfo.getPartitionRole());

		SyncId syncId = syncRequestInfo.getBackupSyncId();
		context = syncMgr_->getSyncContext(pId, syncId);
		if (context == NULL) {
			return;
		}
		syncRequestInfo.setEventType(txnEventType);
		sendRequest(ec, *txnEE_, txnEventType, pId, syncRequestInfo, 0);
		WATCHER_END_SYNC(getEventTypeName(syncEventType), pId);
	}
	catch (UserException &e) {
		TRACE_SYNC_EXCEPTION(
				e, syncEventType, pId, ERROR, "Recv sync is failed.");
	}
	catch (std::exception &e) {
		clsSvc_->setError(ec, &e);
	}
}

/*!
	@brief Adds Sync Timeout event
*/
void SyncHandler::addCheckEndEvent(EventContext &ec, PartitionId pId,
	util::StackAllocator &alloc, SyncMode mode, SyncContext *context,
	bool isImmediate) {
	try {
		int32_t timeoutInterval = INT32_MAX;;
		if (isImmediate) {
			timeoutInterval = 0;
		}
		else if (mode == MODE_SHORTTERM_SYNC) {
			timeoutInterval = clsMgr_->getConfig().getShortTermTimeoutInterval();
		}
		Event syncCheckEndEvent(ec, TXN_SYNC_TIMEOUT, pId);
		SyncCheckEndInfo syncCheckEndInfo(
				alloc, TXN_SYNC_TIMEOUT, syncMgr_, pId, mode, context);
		EventByteOutStream out = syncCheckEndEvent.getOutStream();
		syncSvc_->encode(syncCheckEndEvent, syncCheckEndInfo, out);
		txnEE_->addTimer(syncCheckEndEvent, timeoutInterval);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

SyncPartitionLock::SyncPartitionLock(
	TransactionManager *txnMgr, PartitionId pId)
	: txnMgr_(txnMgr), pId_(pId) {
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
void UnknownSyncEventHandler::operator()(EventContext &, Event &ev) {
	EventType eventType = ev.getType();
	try {
		GS_THROW_USER_ERROR(GS_ERROR_SYNC_SERVICE_UNKNOWN_EVENT_TYPE, "");
	}
	catch (UserException &e) {
		UTIL_TRACE_EXCEPTION_WARNING(SYNC_SERVICE, e,
				"Unknown sync event, type:" << getEventTypeName(eventType));
	}
}

void SyncRequestInfo::decode(EventByteInStream &in, const char8_t *bodyBuffer) {
	SyncVariableSizeAllocator &varSizeAlloc =
		syncMgr_->getVariableSizeAllocator();
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
		SyncContext *context = syncMgr_->getSyncContext(pId_, backupSyncId_);
		if (context == NULL) {
			return;
		}
		if (request_.binaryLogSize_ > 0) {
			const size_t currentPosition = in.base().position();
			const uint8_t *logBuffer
					= reinterpret_cast<const uint8_t *>(bodyBuffer + currentPosition);
			if (logBuffer == NULL) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SYNC_SERVICE_DECODE_MESSAGE_FAILED, "");
			}
			context->copyLogBuffer(varSizeAlloc, logBuffer,
					request_.binaryLogSize_, syncMgr_->getSyncOptStat());
		}
		break;
	}
	case SYC_LONGTERM_SYNC_CHUNK: {
		request_.decode(in);
		partitionRole_.set(request_.ptRev_);
		SyncContext *context = syncMgr_->getSyncContext(pId_, backupSyncId_);
		if (context == NULL) {
			return;
		}
		if (request_.numChunk_ == 0 || request_.chunkSize_ == 0) {
			GS_THROW_USER_ERROR(
					GS_ERROR_SYNC_SERVICE_DECODE_MESSAGE_FAILED, "");
		}
		size_t currentPosition = in.base().position();
		const uint8_t *chunkBuffer =
				reinterpret_cast<const uint8_t *>(bodyBuffer + currentPosition);
		if (chunkBuffer == NULL) {
			GS_THROW_USER_ERROR(
				GS_ERROR_SYNC_SERVICE_DECODE_MESSAGE_FAILED, "");
		}
		context->copyChunkBuffer(varSizeAlloc, chunkBuffer, request_.chunkSize_,
				request_.numChunk_, syncMgr_->getSyncOptStat());
		if (request_.binaryLogSize_ > 0) {
			size_t nextPosition
					= currentPosition + request_.chunkSize_ * request_.numChunk_;
			const uint8_t *logBuffer = reinterpret_cast<const uint8_t *>(bodyBuffer + nextPosition);
			if (logBuffer == NULL) {
				GS_THROW_USER_ERROR(GS_ERROR_SYNC_SYNCMESSAGE_FAILED, "");
			}
			context->copyLogBuffer(varSizeAlloc, logBuffer,
				request_.binaryLogSize_, syncMgr_->getSyncOptStat());
		}
		break;
	}
	default: { GS_THROW_USER_ERROR(GS_ERROR_SYNC_EVENT_TYPE_INVALID, ""); }
	}
	switch (eventType_) {
	case TXN_SHORTTERM_SYNC_REQUEST:
	case TXN_LONGTERM_SYNC_REQUEST:
	case TXN_SHORTTERM_SYNC_START:
	case TXN_LONGTERM_SYNC_START: {
		NodeAddress ownerAddress;
		std::vector<NodeAddress> backupsAddress, catchupAddress;
		NodeId owner;
		std::vector<NodeId> backups, catchups;
		partitionRole_.get(ownerAddress, backupsAddress, catchupAddress);
		owner = ClusterService::changeNodeId(ownerAddress, ee_);
		for (size_t pos = 0; pos < backupsAddress.size(); pos++) {
			NodeId nodeId = ClusterService::changeNodeId(backupsAddress[pos], ee_);
			if (nodeId != UNDEF_NODEID) {
				backups.push_back(nodeId);
			}
		}
		for (size_t pos = 0; pos < catchupAddress.size(); pos++) {
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

void SyncResponseInfo::decode(EventByteInStream &in, const char8_t *) {
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
	default: { GS_THROW_USER_ERROR(GS_ERROR_SYNC_EVENT_TYPE_INVALID, ""); }
	}
}

void SyncManagerInfo::encode(EventByteOutStream &out) {
	try {
		msgpack::sbuffer buffer;
		try {
			msgpack::pack(buffer, partitionRole_);
		}
		catch (std::exception &e) {
			GS_RETHROW_USER_ERROR(
					e, GS_EXCEPTION_MERGE_MESSAGE(e, "Failed to encode message"));
		}
		uint32_t packedSize = static_cast<uint32_t>(buffer.size());
		if (packedSize == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_CS_ENCODE_DECODE_VALIDATION_CHECK, "");
		}
		out << packedSize;
		out.writeAll(buffer.data(), packedSize);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
}

void SyncManagerInfo::decode(EventByteInStream &in, const char8_t *bodyBuffer) {
	uint32_t bodySize;
	in >> bodySize;
	const char8_t *dataBuffer = bodyBuffer + in.base().position();
	uint32_t lastSize = in.base().position() + bodySize;
	msgpack::unpacked msg;
	msgpack::unpack(&msg, dataBuffer, static_cast<size_t>(bodySize));
	msgpack::object obj = msg.get();
	obj.convert(&partitionRole_);
	in.base().position(lastSize);
}

void SyncCheckEndInfo::decode(EventByteInStream &in, const char8_t *) {
	in >> ptRev_.addr_;
	in >> ptRev_.port_;
	in >> ptRev_.sequentialNumber_;
	in >> syncId_.contextId_;
	in >> syncId_.contextVersion_;
	int32_t tmpSyncMode;
	in >> tmpSyncMode;
	syncMode_ = static_cast<SyncMode>(tmpSyncMode);
}

void SyncCheckEndInfo::encode(EventByteOutStream &out) {
	if (context_ == NULL || !syncId_.isValid()) {
		GS_THROW_USER_ERROR(
			GS_ERROR_SYNC_SERVICE_ENCODE_MESSAGE_FAILED, "pId=" << pId_);
	}

	PartitionRevision &ptRev = context_->getPartitionRevision();
	out << ptRev.addr_;
	out << ptRev.port_;
	out << ptRev.sequentialNumber_;
	out << syncId_.contextId_;
	out << syncId_.contextVersion_;
	out << static_cast<int32_t>(syncMode_);
}

void SyncRequestInfo::encode(EventByteOutStream &out) {
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
	default: { GS_THROW_USER_ERROR(GS_ERROR_SYNC_EVENT_TYPE_INVALID, ""); }
	}
}

void SyncResponseInfo::encode(EventByteOutStream &out) {
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
	default: { GS_THROW_USER_ERROR(GS_ERROR_SYNC_EVENT_TYPE_INVALID, ""); }
	}
}

/*!
	@brief Encodes synchronization request
*/
void SyncRequestInfo::Request::encode(
	EventByteOutStream &out, bool isOwner) const {
	out << syncMode_;
	out << ptRev_.addr_;
	out << ptRev_.port_;
	out << ptRev_.sequentialNumber_;
	out << requestInfo_->syncId_.contextId_;
	out << requestInfo_->syncId_.contextVersion_;
	out << requestInfo_->backupSyncId_.contextId_;
	out << requestInfo_->backupSyncId_.contextVersion_;
	out << ownerLsn_;
	out << startLsn_;
	out << endLsn_;
	out << chunkSize_;
	out << numChunk_;
	out << binaryLogSize_;
	if (isOwner) {
		if (numChunk_ > 0) {
			util::XArray<uint8_t *> &chunks = requestInfo_->chunks_;
			for (size_t pos = 0; pos < numChunk_; pos++) {
				out.writeAll(chunks[pos], chunkSize_);
			}
		}
		if (binaryLogSize_ > 0) {
			util::XArray<uint8_t> &logRecord = requestInfo_->binaryLogRecords_;
			out.writeAll(logRecord.data(), logRecord.size());
		}
	}
}

/*!
	@brief Decodes synchronization request
*/
void SyncRequestInfo::Request::decode(EventByteInStream &in) {
	in >> syncMode_;
	in >> ptRev_.addr_;
	in >> ptRev_.port_;
	in >> ptRev_.sequentialNumber_;
	in >> requestInfo_->syncId_.contextId_;
	in >> requestInfo_->syncId_.contextVersion_;
	in >> requestInfo_->backupSyncId_.contextId_;
	in >> requestInfo_->backupSyncId_.contextVersion_;
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
void SyncResponseInfo::Response::encode(EventByteOutStream &out) const {
	out << syncMode_;
	out << ptRev_.addr_;
	out << ptRev_.port_;
	out << ptRev_.sequentialNumber_;
	out << responseInfo_->syncId_.contextId_;
	out << responseInfo_->syncId_.contextVersion_;
	out << responseInfo_->backupSyncId_.contextId_;
	out << responseInfo_->backupSyncId_.contextVersion_;
	out << targetLsn_;
}

/*!
	@brief Decodes synchronization response
*/
void SyncResponseInfo::Response::decode(EventByteInStream &in) {
	in >> syncMode_;
	in >> ptRev_.addr_;
	in >> ptRev_.port_;
	in >> ptRev_.sequentialNumber_;
	in >> responseInfo_->syncId_.contextId_;
	in >> responseInfo_->syncId_.contextVersion_;
	in >> responseInfo_->backupSyncId_.contextId_;
	in >> responseInfo_->backupSyncId_.contextVersion_;
	in >> targetLsn_;
}

/*!
	@brief Executes DropPartition
*/
void SyncHandler::removePartition(util::StackAllocator &alloc,
		PartitionId pId, EventType eventType, const util::DateTime &now,
		const EventMonotonicTime emNow) {
	util::StackAllocator::Scope scope(alloc);
	const TransactionManager::ContextSource src(eventType);
	TransactionContext &txn =
		txnMgr_->put(alloc, pId, TXN_EMPTY_CLIENTID, src, now, emNow);
	DataStore::Latch latch(txn, pId, ds_, clsSvc_);
	{
		SyncPartitionLock partitionLock(txnMgr_, pId);
		syncMgr_->removePartition(pId);
//		LogSequentialNumber prevLsn = pt_->getLSN(pId, 0);
		recoveryMgr_->dropPartition(txn, alloc, pId, true);
	}
	pt_->setLSN(pId, 0);
}

void ShortTermSyncHandler::undoPartition(util::StackAllocator &alloc,
		SyncContext *context, PartitionId pId) {
	if (context->getPartitionRevision().isInitialRevision()) {
		LogSequentialNumber prevLsn = pt_->getLSN(pId);
		const size_t undoNum = recoveryMgr_->undo(alloc, pId);
		if (undoNum > 0) {
			TRACE_CLUSTER_NORMAL_OPERATION(INFO, "Undo partition (pId=" << pId
					<< ", undoNum=" << undoNum << ", before LSN=" << prevLsn << ", after LSN="
					<< pt_->getLSN(pId) << ")");
		}
	}
	ds_->setUndoCompleted(pId);
}

void ShortTermSyncHandler::decodeLongSyncRequestInfo(
		PartitionId pId, EventByteInStream &in, SyncId &longSyncId,
		SyncContext *&longSyncContext) {
	if (in.base().remaining()) {
		in >> longSyncId.contextId_;
		in >> longSyncId.contextVersion_;
		GS_TRACE_SYNC_NORAML("Catchup promotion sync start, pId="
				<< pId << ", syncId=" << longSyncId.dump());
		longSyncContext = syncMgr_->getSyncContext(pId, longSyncId);
		if (longSyncContext) {
		}
		else {
			GS_TRACE_SYNC_NORAML("Long sync context is not found");
		}
	}
}

void SyncHandler::sendResponse(EventContext &ec, 
		EventType eventType, PartitionId pId, SyncRequestInfo &syncRequestInfo,
		SyncResponseInfo &syncResponseInfo, SyncContext *context, NodeId senderNodeId) {
	Event ackEvent(ec, eventType, pId);
	syncResponseInfo.set(eventType, syncRequestInfo, pt_->getLSN(pId), context);
	EventByteOutStream out = ackEvent.getOutStream();
	syncSvc_->encode(ackEvent, syncResponseInfo, out);
	sendEvent(ec, *txnEE_, senderNodeId, ackEvent);
}

void SyncHandler::checkRestored(util::StackAllocator &alloc,
		PartitionId pId, EventType eventType, const util::DateTime &now,
		const EventMonotonicTime emNow) {
	if (!ds_->isRestored(pId)
			&& ds_->getObjectManager()->existPartition(pId)
			&& pt_->getLSN(pId) == 0) {
		removePartition(alloc, pId, eventType, now, emNow);
	}
}

int64_t ShortTermSyncHandler::getTargetSSN(PartitionId pId, SyncContext *context) {
	int64_t ssn = -1;
	if (context->isUseLongSyncLog()) {
		SyncId longSyncId;
		context->getLongSyncId(longSyncId);
		SyncContext *longContext = syncMgr_->getSyncContext(pId, longSyncId);
		if (longContext) {
			ssn = longContext->getSequentialNumber();
		}
	}
	else {
		ssn = context->getSequentialNumber();
	}
	return ssn;
}

void LongTermSyncHandler::requestShortTermSync(
		EventContext &ec, PartitionId pId, SyncContext *context) {
	util::StackAllocator &alloc = ec.getAllocator();

	context->setCatchupSync();
	SyncRequestInfo syncRequestInfo(alloc, TXN_SHORTTERM_SYNC_REQUEST,
			syncMgr_, pId, clsSvc_->getEE(), MODE_SHORTTERM_SYNC);

	PartitionRole nextRole;
	pt_->getPartitionRole(pId, nextRole, PartitionTable::PT_CURRENT_OB);
	nextRole.promoteCatchup();

	syncRequestInfo.setPartitionRole(nextRole);
	GS_TRACE_SYNC(context, 
			"Request promoted partition short term sync", ", role=" << nextRole);

	Event requestEvent(ec, TXN_SHORTTERM_SYNC_REQUEST, pId);
	EventByteOutStream out = requestEvent.getOutStream();
	syncSvc_->encode(requestEvent, syncRequestInfo, out);
	SyncId syncId;
	context->getSyncId(syncId);
	out << syncId.contextId_;
	out << syncId.contextVersion_;
	txnEE_->addTimer(requestEvent, EE_PRIORITY_HIGH);
}

void SyncHandler::sendRequest(EventContext &ec, EventEngine &ee,
		EventType eventType, PartitionId pId, 
		SyncRequestInfo &syncRequestInfo, NodeId senderNodeId, int32_t waitTime) {
	Event requestEvent(ec, eventType, pId);
	EventByteOutStream out = requestEvent.getOutStream();
	syncSvc_->encode(requestEvent, syncRequestInfo, out);
	sendEvent(ec, ee, senderNodeId, requestEvent);
}

template<typename T>
void SyncHandler::sendMultiRequest(EventContext &ec, EventEngine &ee,
		EventType eventType, PartitionId pId, 
		SyncRequestInfo &syncRequestInfo, T &t, SyncContext *context) {
	Event requestEvent(ec, eventType, pId);
	EventByteOutStream out = requestEvent.getOutStream();
	syncSvc_->encode(requestEvent, syncRequestInfo, out);
	for (typename T::iterator it = t.begin(); it != t.end(); it++) {
		sendEvent(ec, ee, (*it), requestEvent);
		context->incrementCounter(*it);
	}
}

void ShortTermSyncHandler::executeSyncRequest(EventContext &ec, Event &ev,
		PartitionId pId, SyncContext *context, SyncRequestInfo &syncRequestInfo,
		SyncResponseInfo &syncResponseInfo) {
	util::StackAllocator &alloc = ec.getAllocator();
	clsSvc_->requestChangePartitionStatus(ec,
			alloc, pId, PartitionTable::PT_SYNC, PT_CHANGE_SYNC_START);
	util::Set<NodeId> syncTargetNodeSet(alloc);
	syncMgr_->setShortermSyncRequest(context, syncRequestInfo, syncTargetNodeSet);
	addCheckEndEvent(ec, pId, alloc, MODE_SHORTTERM_SYNC, context);
	if (syncTargetNodeSet.size() > 0) {
		sendMultiRequest(ec, *txnEE_, TXN_SHORTTERM_SYNC_START,
				pId, syncRequestInfo, syncTargetNodeSet, context);
	}
	else {
		undoPartition(alloc, context, pId);
		sendResponse(ec, TXN_SHORTTERM_SYNC_END_ACK, pId,
				syncRequestInfo, syncResponseInfo, context, 0);
		context->incrementCounter(0);
		context->setOwnerLsn(pt_->getLSN(pId));
	}
	context->startAll();
}

void ShortTermSyncHandler::executeSyncStart(EventContext &ec, Event &ev, PartitionId pId,
		SyncContext *context, SyncRequestInfo &syncRequestInfo,
		SyncResponseInfo &syncResponseInfo, NodeId senderNodeId) {
	util::StackAllocator &alloc = ec.getAllocator();
	clsSvc_->requestChangePartitionStatus(ec,
			alloc, pId, PartitionTable::PT_SYNC, PT_CHANGE_SYNC_START);
	PartitionRole &nextRole = syncRequestInfo.getPartitionRole();
	context = syncMgr_->createSyncContext(ec, pId, nextRole.getRevision(),
			MODE_SHORTTERM_SYNC, PartitionTable::PT_BACKUP);
	syncMgr_->setShortermSyncStart(context, syncRequestInfo, senderNodeId);
	checkRestored(alloc, pId, ev.getType(),
			ec.getHandlerStartTime(), ec.getHandlerStartMonotonicTime());
	addCheckEndEvent(ec, pId, alloc, MODE_SHORTTERM_SYNC, context);

	sendResponse(ec, TXN_SHORTTERM_SYNC_START_ACK, pId,
			syncRequestInfo, syncResponseInfo, context, senderNodeId);
	context->startAll();
}
	
void ShortTermSyncHandler::checkBackup(
		NodeId backupNodeId, LogSequentialNumber backupLsn, SyncContext *context) {
	LogSequentialNumber sendLsn = context->getSyncTargetEndLsn(backupNodeId);
	if (sendLsn != backupLsn) {
		GS_THROW_USER_ERROR(GS_ERROR_SYNC_CHECK_LSN,
				"Backup Lsn is invalid, send Lsn=" << sendLsn
				<< ", current Lsn=" << backupLsn);
	}
}

void ShortTermSyncHandler::executeSyncStartAck(EventContext &ec, Event &ev, PartitionId pId,
		SyncContext *context, SyncRequestInfo &syncRequestInfo,
		SyncResponseInfo &syncResponseInfo, NodeId senderNodeId) {
	util::StackAllocator &alloc = ec.getAllocator();
	if (context != NULL) {
		LogSequentialNumber backupLsn = syncResponseInfo.getTargetLsn();
		LogSequentialNumber ownerLsn = pt_->getLSN(pId);
		checkOwner(ownerLsn, context);
		context->setSyncTargetLsnWithSyncId(senderNodeId, backupLsn,
				syncResponseInfo.getBackupSyncId());
		LogSequentialNumber maxLsn = pt_->getMaxLsn(pId);				
		if (ownerLsn >= backupLsn) {
			if (context->decrementCounter(senderNodeId)) {
				undoPartition(alloc, context, pId);
				syncMgr_->checkShorttermGetSyncLog(context, syncRequestInfo, syncResponseInfo);
				util::XArray<NodeId> backups(alloc);
				context->getSyncTargetNodeIds(backups);
				int32_t sendCount = 0;
				for (size_t pos = 0; pos < backups.size(); pos++) {
					syncMgr_->setShorttermGetSyncLog(context, syncRequestInfo, syncResponseInfo, backups[pos]);
					sendRequest(ec, *syncEE_, SYC_SHORTTERM_SYNC_LOG,
						pId, syncRequestInfo, backups[pos]);
					sendCount++;
				}
				if (sendCount == 0) {
					sendResponse(ec, TXN_SHORTTERM_SYNC_END_ACK, pId,
							syncRequestInfo, syncResponseInfo, context, 0);
				}
			}
		}
		else {
			clsSvc_->requestChangePartitionStatus(ec, alloc, pId,
				 PartitionTable::PT_OFF, PT_CHANGE_SYNC_END);
			GS_THROW_SYNC_ERROR(GS_ERROR_SYNC_OWNER_BACKUP_INVALID_LSN_CONSTRAINT, context, 
				"Backup LSN is larger than owner LSN", ", owner=" << pt_->dumpNodeAddress(0) 
					<< ", owner LSN=" << ownerLsn << ", backup=" 
					<< pt_->dumpNodeAddress(senderNodeId) << ", backup LSN=" << backupLsn 
					<< ", maxLSN=" << maxLsn);
		}
	}
}

void ShortTermSyncHandler::executeSyncLog(EventContext &ec, Event &ev, PartitionId pId,
		SyncContext *context, SyncRequestInfo &syncRequestInfo,
		SyncResponseInfo &syncResponseInfo, NodeId senderNodeId) {

//	util::StackAllocator &alloc = ec.getAllocator();
//		SyncVariableSizeAllocator &varSizeAlloc = syncMgr_->getVariableSizeAllocator();
	ds_->setUndoCompleted(pId);
	if (context != NULL) {
		syncMgr_->setShorttermSyncLog(context, syncRequestInfo, ev.getType(),
				ec.getHandlerStartTime(), ec.getHandlerStartMonotonicTime());
		sendResponse(ec, TXN_SHORTTERM_SYNC_LOG_ACK, pId,
				syncRequestInfo, syncResponseInfo, context, context->getRecvNodeId());
	}
}

void ShortTermSyncHandler::checkOwner(
		LogSequentialNumber targetLsn, SyncContext *context, bool isOwner) {
	if (targetLsn != context->getOwnerLsn()) {
		if (isOwner) {
			GS_THROW_USER_ERROR(GS_ERROR_SYNC_CHECK_LSN,
					"Owner Lsn is changed, start=" << context->getOwnerLsn()
					<< ", current=" << targetLsn);
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_SYNC_CHECK_LSN,
					"Backup Lsn is not same, owner Lsn=" << context->getOwnerLsn()
					<< ", backupLsn=" << targetLsn);
		}
	}
}

void ShortTermSyncHandler::executeSyncLogAck(
		EventContext &ec, Event &ev, PartitionId pId,
		SyncContext *context, SyncRequestInfo &syncRequestInfo,
		SyncResponseInfo &syncResponseInfo, NodeId senderNodeId) {
	util::StackAllocator &alloc = ec.getAllocator();
//	EventType eventType = ev.getType();
	if (context != NULL) {
		LogSequentialNumber ownerLsn = pt_->getLSN(pId);
		LogSequentialNumber backupLsn = syncResponseInfo.getTargetLsn();
//		LogSequentialNumber maxLsn = pt_->getMaxLsn(pId);
		checkOwner(ownerLsn, context);
		context->setSyncTargetLsn(senderNodeId, backupLsn);
		if (ownerLsn == backupLsn) {
			if (context->decrementCounter(senderNodeId)) {
				context->resetCounter();
				util::XArray<NodeId> backups(alloc);
				context->getSyncTargetNodeIds(backups);
				for (size_t pos = 0; pos < backups.size(); pos++) {
					syncMgr_->setShorttermSyncLogAck(context, syncRequestInfo, backups[pos]);
					sendRequest(ec, *txnEE_,
							TXN_SHORTTERM_SYNC_END, pId, syncRequestInfo, backups[pos]);
				}
			}
		}
		else {
			syncMgr_->setShorttermGetSyncNextLog(
					context, syncRequestInfo, syncResponseInfo, senderNodeId);
			sendRequest(ec, *syncEE_, SYC_SHORTTERM_SYNC_LOG,
					pId, syncRequestInfo, senderNodeId);
			checkOwner(ownerLsn, context);
		}
	}
}

void ShortTermSyncHandler::executeSyncEnd(
		EventContext &ec, Event &ev, PartitionId pId,
		SyncContext *context, SyncRequestInfo &syncRequestInfo,
		SyncResponseInfo &syncResponseInfo, NodeId senderNodeId) {
	util::StackAllocator &alloc = ec.getAllocator();
	if (context != NULL) {
//		PartitionStatus nextStatus = 
		syncMgr_->setShorttermSyncEnd(context);
		clsSvc_->requestChangePartitionStatus(ec,
			alloc, pId, PartitionTable::PT_ON, PT_CHANGE_SYNC_END);
		sendResponse(ec, TXN_SHORTTERM_SYNC_END_ACK, pId,
				syncRequestInfo, syncResponseInfo, context, senderNodeId);
		context->endAll();
		syncMgr_->removeSyncContext(ec, pId, context, false);
	}
}

void ShortTermSyncHandler::executeSyncEndAck(
		EventContext &ec, Event &ev, PartitionId pId,
		SyncContext *context, SyncRequestInfo &syncRequestInfo,
		SyncResponseInfo &syncResponseInfo, NodeId senderNodeId) {
	util::StackAllocator &alloc = ec.getAllocator();
	if (context != NULL) {
		if (pt_->getLSN(pId) != syncResponseInfo.getTargetLsn()) {
			GS_THROW_SYNC_ERROR(GS_ERROR_SYM_INVALID_PARTITION_REVISION, context, 
				"Target short term sync already removed", ", owner=" << pt_->dumpNodeAddress(0) 
					<< ", owner LSN=" << pt_->getLSN(pId) << ", backup=" << pt_->dumpNodeAddress(senderNodeId)
					<< ", backup LSN=" << syncResponseInfo.getTargetLsn());
		}
		context->setSyncTargetLsn(senderNodeId, syncResponseInfo.getTargetLsn());
		if (context->decrementCounter(senderNodeId)) {
			PartitionStatus nextStatus = syncMgr_->setShorttermSyncEnd(context);
			clsSvc_->requestChangePartitionStatus(ec,
				alloc, pId, nextStatus, PT_CHANGE_SYNC_END);
			context->endAll();
			syncMgr_->removeSyncContext(ec, pId, context, false);
		}
	}
}

void LongTermSyncHandler::executeSyncRequest(
		EventContext &ec, Event &ev, PartitionId pId,
		SyncContext *context, SyncRequestInfo &syncRequestInfo,
		SyncResponseInfo &syncResponseInfo) {
	syncMgr_->setLongtermSyncRequest(context, syncRequestInfo);
}

void LongTermSyncHandler::executeSyncPrepareAck(
		EventContext &ec, Event &ev, PartitionId pId,
		SyncContext *context, SyncRequestInfo &syncRequestInfo,
		SyncResponseInfo &syncResponseInfo, LongtermSyncInfo &syncInfo) {
	util::StackAllocator &alloc = ec.getAllocator();
	if (!context->isSendReady()) {
		context->setSendReady();
	}
	else {
		Event syncChunkAckEvent(ec, TXN_LONGTERM_SYNC_CHUNK_ACK, pId);
		clsSvc_->encode(syncChunkAckEvent, syncInfo);
		sendEvent(ec, *txnEE_, 0, syncChunkAckEvent);
		return;
	}
	util::XArray<NodeId> catchups(alloc);
	syncMgr_->setLongtermSyncPrepareAck(
			context, syncRequestInfo, syncResponseInfo, catchups);
	if (catchups.size() > 0) {
		sendMultiRequest(ec, *txnEE_, TXN_LONGTERM_SYNC_START,
				pId, syncRequestInfo, catchups, context);
	}
	else {
	}
}

void LongTermSyncHandler::executeSyncStart(EventContext &ec, Event &ev, PartitionId pId,
		SyncContext *context, SyncRequestInfo &syncRequestInfo,
		SyncResponseInfo &syncResponseInfo, NodeId senderNodeId) {
	SyncStat &stat = clsMgr_->getSyncStat();
	stat.init(true);

	syncMgr_->setLongtermSyncStart(pId, syncRequestInfo);
	context = syncMgr_->createSyncContext(ec, pId, syncRequestInfo.getPartitionRole().getRevision(),
			MODE_LONGTERM_SYNC, PartitionTable::PT_CATCHUP);
	context->startAll();
	checkRestored(ec.getAllocator(), pId, ev.getType(),
			ec.getHandlerStartTime(), ec.getHandlerStartMonotonicTime());
	sendResponse(ec, TXN_LONGTERM_SYNC_START_ACK, pId,
			syncRequestInfo, syncResponseInfo, context, senderNodeId);
}

void LongTermSyncHandler::executeSyncStartAck(EventContext &ec, Event &ev, PartitionId pId,
		SyncContext *context, SyncRequestInfo &syncRequestInfo,
		SyncResponseInfo &syncResponseInfo, NodeId senderNodeId) {

	SyncStat &stat = clsMgr_->getSyncStat();
	stat.init(true);
//	EventType eventType = ev.getType();
//	util::StackAllocator &alloc = ec.getAllocator();
	if (context != NULL) {
		if (syncMgr_->checkLongGetSyncLog(
				context, syncRequestInfo, syncResponseInfo, senderNodeId)) {
			sendRequest(ec, *syncEE_, SYC_LONGTERM_SYNC_LOG,
					pId, syncRequestInfo, senderNodeId);
		}
		else {
			syncMgr_->setLongGetSyncChunk(context, syncRequestInfo, syncResponseInfo,
				ev.getType(), ec.getHandlerStartTime(), ec.getHandlerStartMonotonicTime(), senderNodeId);
//			syncRequestInfo.request_.numChunk_;
			sendRequest(ec, *syncEE_, SYC_LONGTERM_SYNC_CHUNK, pId, syncRequestInfo, senderNodeId);
		}
	}
}	

void LongTermSyncHandler::executeSyncChunk(EventContext &ec, Event &ev, PartitionId pId,
		SyncContext *context, SyncRequestInfo &syncRequestInfo,
		SyncResponseInfo &syncResponseInfo, NodeId senderNodeId) {
//	util::StackAllocator &alloc = ec.getAllocator();
//	SyncVariableSizeAllocator &varSizeAlloc = syncMgr_->getVariableSizeAllocator();
	EventType eventType = ev.getType();
	const util::DateTime &now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();

	if (context != NULL) {
		syncMgr_->setLongtermSyncChunk(context, syncRequestInfo, syncResponseInfo,
			eventType, now, emNow, senderNodeId);
		sendResponse(ec, TXN_LONGTERM_SYNC_CHUNK_ACK, pId,
				syncRequestInfo, syncResponseInfo,
				context, pt_->getOwner(pId, PartitionTable::PT_CURRENT_OB));
	}
}

void LongTermSyncHandler::executeSyncChunkAck(EventContext &ec, Event &ev, PartitionId pId,
		SyncContext *context, SyncRequestInfo &syncRequestInfo,
		SyncResponseInfo &syncResponseInfo, NodeId senderNodeId) {
	util::StackAllocator &alloc = ec.getAllocator();
//	SyncVariableSizeAllocator &varSizeAlloc = syncMgr_->getVariableSizeAllocator();
	EventType eventType = ev.getType();
	if (context != NULL) {
		util::XArray<NodeId> catchups(alloc);
//		LogSequentialNumber catchupLsn = syncResponseInfo.getTargetLsn();
		int32_t waitTime = 0;
		int32_t ret = syncMgr_->setLongtermSyncCheckChunk(ec, ev,
			context, syncResponseInfo, catchups, eventType, waitTime);
		if (ret == -1) {
			return;
		}
		else if (ret == 0) {
			requestShortTermSync(ec, pId, context);
		}
		if (context->isSyncCheckpointCompleted()) {
			syncMgr_->setLongtermSyncChunkAck(context, syncRequestInfo, syncResponseInfo);
			sendRequest(ec, *syncEE_, SYC_LONGTERM_SYNC_LOG,
					pId, syncRequestInfo, senderNodeId, waitTime);
		}
		else {
			const util::DateTime &now = ec.getHandlerStartTime();
			const EventMonotonicTime emNow =
				ec.getHandlerStartMonotonicTime();
			syncMgr_->setLongtermSyncGetChunkAck(
				context, syncRequestInfo, syncResponseInfo, eventType, now, emNow, senderNodeId);
			if (syncRequestInfo.getChunkList().size() == 0) {
				return;
			}
			sendRequest(ec, *syncEE_, SYC_LONGTERM_SYNC_CHUNK,
					pId, syncRequestInfo, senderNodeId, waitTime);
		}
	}
}

void LongTermSyncHandler::executeSyncLog(EventContext &ec, Event &ev, PartitionId pId,
		SyncContext *context, SyncRequestInfo &syncRequestInfo,
		SyncResponseInfo &syncResponseInfo, NodeId senderNodeId) {
//			util::StackAllocator &alloc = ec.getAllocator();
//			SyncVariableSizeAllocator &varSizeAlloc = syncMgr_->getVariableSizeAllocator();

		if (context != NULL) {
			const util::DateTime &now = ec.getHandlerStartTime();
			const EventMonotonicTime emNow =
				ec.getHandlerStartMonotonicTime();
			EventType eventType = ev.getType();
			syncMgr_->setLongtermSyncLog(context, syncRequestInfo,
					syncResponseInfo, eventType, now, emNow);
			sendResponse(ec, TXN_LONGTERM_SYNC_LOG_ACK, pId,
					syncRequestInfo, syncResponseInfo, context,
					pt_->getOwner(pId, PartitionTable::PT_CURRENT_OB));
		}
}

void LongTermSyncHandler::executeSyncLogAck(EventContext &ec, Event &ev, PartitionId pId,
		SyncContext *context, SyncRequestInfo &syncRequestInfo,
		SyncResponseInfo &syncResponseInfo, NodeId senderNodeId) {
//	util::StackAllocator &alloc = ec.getAllocator();
//	SyncVariableSizeAllocator &varSizeAlloc = syncMgr_->getVariableSizeAllocator();
//	EventType eventType = ev.getType();
	if (context != NULL) {
		LogSequentialNumber ownerLsn = pt_->getLSN(pId);
		LogSequentialNumber catchupLsn = syncResponseInfo.getTargetLsn();
		int32_t waitTime = 0;
		if (!syncMgr_->controlSyncLoad(ec, ev, pId, catchupLsn, waitTime)) {
			requestShortTermSync(ec, pId, context);
			return;
		}
//		const NodeDescriptor &nd = syncEE_->getServerND(senderNodeId);
		context->setSyncTargetLsn(senderNodeId, catchupLsn);
		if (ownerLsn == catchupLsn) {
			syncRequestInfo.setSearchLsnRange(ownerLsn, ownerLsn);
		}
		else {
			syncMgr_->setLongtermSyncLogAck(
					context, syncRequestInfo, syncResponseInfo, senderNodeId);
			sendRequest(ec, *syncEE_, SYC_LONGTERM_SYNC_LOG,
					pId, syncRequestInfo, senderNodeId, waitTime);
		}
	}
}

