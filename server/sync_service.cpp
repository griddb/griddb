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
#include "chunk_manager.h"
#include "cluster_common.h"
#include "data_store.h"
#include "event_engine.h"
#include "log_manager.h"
#include "log_manager.h"
#include "object_manager.h"
#include "recovery_manager.h"
#include "sync_manager.h"
#include "transaction_manager.h"
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

/*!
	@brief Decodes synchronization message
*/
template <class T>
void SyncService::decode(
	util::StackAllocator &alloc, EventEngine::Event &ev, T &t) {
	try {
		EventByteInStream in = ev.getInStream();
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
		clsSvc_->getStats().set(ClusterStats::SYNC_SEND,
			static_cast<int32_t>(in.base().position()));
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
				<< static_cast<int32_t>(versionId_) << ", connectClusterVersion=" << static_cast<int32_t>(versionId));
	}
}

uint16_t SyncService::getLogVersion(PartitionId pId) {
	return logMgr_->getLogVersion(pId);
}

/*!
	@brief Encodes synchronization message
*/
template <class T>
void SyncService::encode(Event &ev, T &t) {
	try {
		EventByteOutStream out = ev.getOutStream();
//		const size_t currentPosition = out.base().position();
		out << versionId_;
		uint16_t logVersion = getLogVersion(ev.getPartitionId());
		out << logVersion;
		t.encode(out);
		clsSvc_->getStats().set(ClusterStats::SYNC_RECEIVE,
				static_cast<int32_t>(out.base().position()));
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
	util::StackAllocator &alloc, PartitionId pId, bool isForce, PartitionRevisionNo revision) {
	try {
		DropPartitionInfo dropPartitionInfo(
				alloc, TXN_DROP_PARTITION, syncMgr_, pId, isForce, revision);	
		Event dropEvent(eventSource, TXN_DROP_PARTITION, pId);
		clsSvc_->encode(dropEvent, dropPartitionInfo);
		txnSvc_->getEE()->add(dropEvent);
	}
	catch (UserException &e) {
		UTIL_TRACE_EXCEPTION_WARNING(SYNC_SERVICE, e, "");
	}
	catch (std::exception &e) {
		clsSvc_->setError(eventSource, &e);
	}
}

void SyncService::notifyCheckpointLongSyncReady(
		EventContext &ec, PartitionId pId, LongtermSyncInfo *syncInfo, bool errorOccured) {
	clsSvc_->request(ec, TXN_LONGTERM_SYNC_PREPARE_ACK,
			pId, txnSvc_->getEE(), *syncInfo);
}

void SyncHandler::sendEvent(EventContext &ec, EventEngine &ee, NodeId targetNodeId,
		Event &ev, int32_t delayTime) {

	if (targetNodeId == -1) {
		GS_THROW_USER_ERROR(GS_ERROR_SYNC_INVALID_SENDER_ND, "");
	}


	if (targetNodeId == 0) {
		if (delayTime == -1) {
			ee.add(ev);
		}
		else {
			ee.addTimer(ev, delayTime);
		}
	}
	else {
		EventRequestOption sendOption;
		if (delayTime != -1) {
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
	PartitionId pId = ev.getPartitionId();
	EventType eventType = ev.getType();
	const NodeDescriptor &senderNd = ev.getSenderND();
	NodeId senderNodeId = ClusterService::resolveSenderND(ev);
	try {
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);
		SyncVariableSizeAllocator &varSizeAlloc = syncMgr_->getVariableSizeAllocator();


		SyncRequestInfo syncRequestInfo(alloc, eventType, syncMgr_, pId,
				clsSvc_->getEE(), MODE_SHORTTERM_SYNC);
		SyncResponseInfo syncResponseInfo(alloc, eventType, syncMgr_, pId,
				MODE_SHORTTERM_SYNC, logMgr_->getLSN(pId));

		WATCHER_START;

		switch (eventType) {

		case TXN_SHORTTERM_SYNC_REQUEST: {

			syncSvc_->decode(alloc, ev, syncRequestInfo);
			syncMgr_->checkExecutable(OP_SHORTTERM_SYNC_REQUEST, pId,
					syncRequestInfo.getPartitionRole());
			PartitionRole &nextRole = syncRequestInfo.getPartitionRole();
			context = syncMgr_->createSyncContext(ec, pId, nextRole.getRevision(),
					MODE_SHORTTERM_SYNC, PartitionTable::PT_OWNER);
			
			
				addTimeoutEvent(ec, pId, alloc, MODE_SHORTTERM_SYNC, context);
			context->setPartitionStatus(pt_->getPartitionStatus(pId));
			clsSvc_->requestChangePartitionStatus(ec,
				alloc, pId, PartitionTable::PT_SYNC, PT_CHANGE_SYNC_START);
			pt_->setPartitionRole(pId, nextRole, PartitionTable::PT_NEXT_OB);
			SyncId dummySyncId;
			syncRequestInfo.set(TXN_SHORTTERM_SYNC_START, context,
					logMgr_->getLSN(pId), dummySyncId);

			util::Set<NodeId> syncTargetNodeSet(alloc);
			nextRole.getShortTermSyncTargetNodeId(syncTargetNodeSet);
			if (syncTargetNodeSet.size() > 0) {
				Event syncStartEvent(ec, TXN_SHORTTERM_SYNC_START, pId);
				syncSvc_->encode(syncStartEvent, syncRequestInfo);
				for (util::Set<NodeId>::iterator it = syncTargetNodeSet.begin();
						it != syncTargetNodeSet.end(); it++) {
					NodeId targetNodeId = *it;
					sendEvent(ec, *txnEE_, targetNodeId, syncStartEvent);
					context->incrementCounter(targetNodeId);
				}
			}
			else {
				undoPartition(alloc, context, pId);
				LogSequentialNumber ownerLsn = logMgr_->getLSN(pId);
				LogSequentialNumber maxLsn = pt_->getMaxLsn(pId);
				if (ownerLsn < maxLsn) {
					GS_THROW_USER_ERROR(GS_ERROR_SYNC_OWNER_MAX_LSN_CONSTRAINT,
							"Cluster max LSN is larger than owner LSN (pId=" << pId << ", owner=" 
							<< pt_->dumpNodeAddress(0) << ", owner LSN=" << ownerLsn << ", maxLSN=" << maxLsn << ")");
				}
				Event syncEndEvent(ec, TXN_SHORTTERM_SYNC_END_ACK, pId);
				syncResponseInfo.set(TXN_SHORTTERM_SYNC_END_ACK,
						syncRequestInfo, logMgr_->getLSN(pId), context);
				syncSvc_->encode(syncEndEvent, syncResponseInfo);
				sendEvent(ec, *txnEE_, 0, syncEndEvent);
				context->incrementCounter(0);
			}
			context->startAll();
			break;
		}
		case TXN_SHORTTERM_SYNC_START: {
			syncSvc_->decode(alloc, ev, syncRequestInfo);
			syncMgr_->checkExecutable(OP_SHORTTERM_SYNC_START, pId,
					syncRequestInfo.getPartitionRole());
			
			PartitionRole &nextRole = syncRequestInfo.getPartitionRole();
			clsSvc_->requestChangePartitionStatus(ec,
				alloc, pId, PartitionTable::PT_OFF, PT_CHANGE_SYNC_START);
			pt_->setPartitionRole(pId, nextRole, PartitionTable::PT_NEXT_OB);
			context = syncMgr_->createSyncContext(ec, pId, nextRole.getRevision(),
					MODE_SHORTTERM_SYNC, PartitionTable::PT_BACKUP);
			context->setRecvNodeId(senderNodeId);
			addTimeoutEvent(ec, pId, alloc, MODE_SHORTTERM_SYNC, context);
			if (syncRequestInfo.getLsn() == pt_->getLSN(pId)) {
				pt_->setOtherStatus(pId, PT_OTHER_SHORT_SYNC_COMPLETE);
			}
			Event syncStartAckEvent(ec, TXN_SHORTTERM_SYNC_START_ACK, pId);
			syncResponseInfo.set(TXN_SHORTTERM_SYNC_START_ACK, syncRequestInfo,
					logMgr_->getLSN(pId), context);
			syncSvc_->encode(syncStartAckEvent, syncResponseInfo);
			sendEvent(ec, *txnEE_, senderNodeId, syncStartAckEvent);
			context->startAll();
			break;
		}
		case TXN_SHORTTERM_SYNC_START_ACK: {
			syncSvc_->decode(alloc, ev, syncResponseInfo);
			SyncId syncId = syncResponseInfo.getSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			syncMgr_->checkExecutable(OP_SHORTTERM_SYNC_START_ACK, pId,
					syncResponseInfo.getPartitionRole());


			if (context != NULL) {
				LogSequentialNumber backupLsn = syncResponseInfo.getTargetLsn();
				LogSequentialNumber ownerLsn = logMgr_->getLSN(pId);
				context->setSyncTargetLsnWithSyncId(senderNodeId, backupLsn,
						syncResponseInfo.getBackupSyncId());
				LogSequentialNumber maxLsn = pt_->getMaxLsn(pId);
				
				if (ownerLsn >= backupLsn && (ownerLsn >= maxLsn)) {
					if (context->decrementCounter(senderNodeId)) {
						context->resetCounter();
						undoPartition(alloc, context, pId);
						util::XArray<NodeId> backups(alloc);
						context->getSyncTargetNodeIds(backups);
						size_t pos = 0;
						int32_t sendCount = 0;
						for (pos = 0; pos < backups.size(); pos++) {
							LogSequentialNumber backupLsn = 0;
							SyncId backupSyncId;
							NodeId targetNodeId = backups[pos];
							if (!context->getSyncTargetLsnWithSyncId(
									targetNodeId, backupLsn, backupSyncId)) {
								DUMP_CLUSTER("Backup Lsn invalid, targetNode=" << pt_->dumpNodeAddress(targetNodeId) << ", lsn=" << backupLsn);
								GS_THROW_USER_ERROR(GS_ERROR_SYNC_INVALID_CONTEXT,
									"Invalid sync context, targetNode=" << pt_->dumpNodeAddress(targetNodeId)
									<< ", lsn=" << backupLsn);
							}
							syncRequestInfo.clearSearchCondition();
							syncRequestInfo.set(SYC_SHORTTERM_SYNC_LOG, context, ownerLsn, backupSyncId);
							if (ownerLsn > backupLsn &&
									pt_->isOwnerOrBackup(pId, targetNodeId, PartitionTable::PT_NEXT_OB)) {
								syncRequestInfo.setSearchLsnRange(backupLsn + 1, ownerLsn);
								util::Stopwatch logWatch;
								context->start(logWatch);
								try {
									syncRequestInfo.getLogs(pId, context->getSequentialNumber(), logMgr_, cpSvc_);
									context->endLog(logWatch);
									context->setProcessedLsn(syncRequestInfo.getStartLsn(), syncRequestInfo.getEndLsn());
									context->incProcessedLogNum(syncRequestInfo.getBinaryLogSize());
								}
								catch (UserException &) {
									context->endLog(logWatch);
									pt_->setStartLSN(pId, pt_->getLSN(pId));
									DUMP_CLUSTER("Short term log sync failed, log is not found or invalid, pId="
										<< pId << ", backupLsn=" << backupLsn << ", backupAddress=" << pt_->dumpNodeAddress(targetNodeId) 
										<< ", getStartLsn=" << syncRequestInfo.getStartLsn() << ", actualGetLastLsn=" << syncRequestInfo.getEndLsn());

									GS_THROW_USER_ERROR(GS_ERROR_SYNC_LOG_GET_FAILED,
										"Short term log sync failed, log is not found or invalid, pId="
										<< pId << ", backupLsn=" << backupLsn << ", backupAddress=" << pt_->dumpNodeAddress(targetNodeId) 
										<< ", getStartLsn=" << syncRequestInfo.getStartLsn() << ", actualGetLastLsn=" << syncRequestInfo.getEndLsn());
								}
							}
						}

						for (pos = 0; pos < backups.size(); pos++) {
							LogSequentialNumber backupLsn = 0;
							SyncId backupSyncId;
							NodeId targetNodeId = backups[pos];
							if (!context->getSyncTargetLsnWithSyncId(
									targetNodeId, backupLsn, backupSyncId)) {
								DUMP_CLUSTER( "Invalid sync context, targetNode=" << pt_->dumpNodeAddress(targetNodeId)
											<< ", lsn=" << backupLsn);
								GS_THROW_USER_ERROR(GS_ERROR_SYNC_INVALID_CONTEXT,
									"Invalid sync context, targetNode=" << pt_->dumpNodeAddress(targetNodeId)
									<< ", lsn=" << backupLsn);
							}
							syncRequestInfo.clearSearchCondition();
							syncRequestInfo.set(SYC_SHORTTERM_SYNC_LOG, context,
									ownerLsn, backupSyncId);

							if (ownerLsn > backupLsn && pt_->isOwnerOrBackup(
									pId, targetNodeId, PartitionTable::PT_NEXT_OB)) {
								syncRequestInfo.setSearchLsnRange(backupLsn + 1, ownerLsn);
								util::Stopwatch logWatch;
								context->start(logWatch);
								try {
									syncRequestInfo.getLogs(pId, context->getSequentialNumber(), logMgr_, cpSvc_);
									context->endLog(logWatch);
									context->setProcessedLsn(syncRequestInfo.getStartLsn(), syncRequestInfo.getEndLsn());
									context->incProcessedLogNum(syncRequestInfo.getBinaryLogSize());
								}
								catch (UserException &) {
									context->endLog(logWatch);
									pt_->setStartLSN(pId, pt_->getLSN(pId));
									clsSvc_->requestGossip(ec, alloc, pId, targetNodeId, GOSSIP_INVALID_NODE);
									DUMP_CLUSTER("Short term log sync failed, log is not found or invalid, pId="
										<< pId << ", backupLsn=" << backupLsn << ", backupAddress=" << pt_->dumpNodeAddress(targetNodeId) 
										<< ", getStartLsn=" << syncRequestInfo.getStartLsn() << ", actualGetLastLsn=" << syncRequestInfo.getEndLsn());

									GS_THROW_USER_ERROR(GS_ERROR_SYNC_LOG_GET_FAILED,
										"Short term log sync failed, log is not found or invalid, pId="
										<< pId << ", backupLsn=" << backupLsn << ", backupAddress=" << pt_->dumpNodeAddress(targetNodeId) 
										<< ", getStartLsn=" << syncRequestInfo.getStartLsn() << ", actualGetLastLsn=" << syncRequestInfo.getEndLsn());
								}
							}
							else {
								syncRequestInfo.setSearchLsnRange(ownerLsn, ownerLsn);
							}
							Event syncLogEvent(ec, SYC_SHORTTERM_SYNC_LOG, pId);
							syncSvc_->encode(syncLogEvent, syncRequestInfo);
							sendEvent(ec, *syncEE_, backups[pos], syncLogEvent);
								sendCount++;
							}
						if (sendCount == 0) {
							Event syncEndEvent(ec, TXN_SHORTTERM_SYNC_END_ACK, pId);
							syncResponseInfo.set(TXN_SHORTTERM_SYNC_END_ACK,
									syncRequestInfo, logMgr_->getLSN(pId), context);
							syncSvc_->encode(syncEndEvent, syncResponseInfo);
							sendEvent(ec, *txnEE_, 0, syncEndEvent);
						}
					}
				}
				else {
					clsSvc_->requestGossip(ec, alloc, pId, 0, GOSSIP_INVALID_NODE);
					clsSvc_->requestChangePartitionStatus(ec, alloc, pId,
						 context->getPartitionStatus(), PT_CHANGE_SYNC_END);

					if (ownerLsn < backupLsn) {
						DUMP_CLUSTER( "Backup LSN is larger than owner LSN (pId=" << pId << ", owner=" 
								<< pt_->dumpNodeAddress(0) << ", owner LSN=" << ownerLsn 
								<< ", backup=" << pt_->dumpNodeAddress(senderNodeId)
								<< ", backup LSN=" << backupLsn << ")");
						GS_THROW_USER_ERROR(GS_ERROR_SYNC_OWNER_BACKUP_INVALID_LSN_CONSTRAINT,
								"Backup LSN is larger than owner LSN (pId=" << pId << ", owner=" 
								<< pt_->dumpNodeAddress(0) << ", owner LSN=" << ownerLsn 
								<< ", backup=" << pt_->dumpNodeAddress(senderNodeId)
								<< ", backup LSN=" << backupLsn << ")");
					}
					else if (ownerLsn < maxLsn) {
						DUMP_CLUSTER( "Cluster max LSN is larger than owner LSN (pId=" << pId << ", owner=" 
								<< pt_->dumpNodeAddress(0) << ", owner LSN=" << ownerLsn << ", max LSN=" << maxLsn
								<< ", backup=" << pt_->dumpNodeAddress(senderNodeId)
								<< ", backup LSN=" << backupLsn << ")");
						GS_THROW_USER_ERROR(GS_ERROR_SYNC_OWNER_MAX_LSN_CONSTRAINT,
								"Cluster max LSN is larger than owner LSN (pId=" << pId << ", owner=" 
								<< pt_->dumpNodeAddress(0) << ", owner LSN=" << ownerLsn << ", maxLSN=" << maxLsn
								<< ", backup=" << pt_->dumpNodeAddress(senderNodeId)
								<< ", backup LSN=" << backupLsn << ")");
					}
					else {
						GS_THROW_USER_ERROR(GS_ERROR_SYNC_OWNER_BACKUP_INVALID_LSN_CONSTRAINT, "");
					}
				}
			}
			break;
		}
		case TXN_SHORTTERM_SYNC_LOG: {
			syncSvc_->decode(alloc, ev, syncRequestInfo);
			SyncId syncId = syncRequestInfo.getBackupSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			syncMgr_->checkExecutable(
					OP_SHORTTERM_SYNC_LOG, pId, syncRequestInfo.getPartitionRole());
			ds_->setUndoCompleted(pId);

			if (context != NULL) {
				senderNodeId = context->getRecvNodeId();
				uint8_t *logBuffer = NULL;
				int32_t logBufferSize = 0;
				context->getLogBuffer(logBuffer, logBufferSize);
				if (logBuffer != NULL && logBufferSize > 0) {
					util::Stopwatch logWatch;
					context->start(logWatch);
					LogSequentialNumber startLsn = pt_->getLSN(pId);
					try {
						recoveryMgr_->redoLogList(alloc, RecoveryManager::MODE_SHORT_TERM_SYNC, pId,
							ec.getHandlerStartTime(), ec.getHandlerStartMonotonicTime(), logBuffer, logBufferSize);
					}
					catch (std::exception &e) {
						context->endLog(logWatch);
						RM_RETHROW_LOG_REDO_ERROR(e, "");
					}
					context->endLog(logWatch);
					context->setProcessedLsn(startLsn, pt_->getLSN(pId));
					context->incProcessedLogNum(logBufferSize);
					context->freeBuffer(varSizeAlloc, LOG_SYNC, syncMgr_->getSyncOptStat());
				}

				if (syncRequestInfo.getLsn() == pt_->getLSN(pId)) {
					pt_->setOtherStatus(pId, PT_OTHER_SHORT_SYNC_COMPLETE);
				}

				Event syncLogAckEvent(ec, TXN_SHORTTERM_SYNC_LOG_ACK, pId);
				syncResponseInfo.set(TXN_SHORTTERM_SYNC_LOG_ACK,
						syncRequestInfo, logMgr_->getLSN(pId), context);
				syncSvc_->encode(syncLogAckEvent, syncResponseInfo);
				sendEvent(ec, *txnEE_, senderNodeId, syncLogAckEvent);
			}
			break;
		}
		case TXN_SHORTTERM_SYNC_LOG_ACK: {
			syncSvc_->decode(alloc, ev, syncResponseInfo);
			SyncId syncId = syncResponseInfo.getSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			syncMgr_->checkExecutable(OP_SHORTTERM_SYNC_LOG_ACK, pId,
					syncResponseInfo.getPartitionRole());
			if (context != NULL) {
				SyncControlContext control(eventType, pId, syncResponseInfo.getTargetLsn());
				controlSyncLoad(control);

				LogSequentialNumber ownerLsn = logMgr_->getLSN(pId);
				LogSequentialNumber backupLsn = syncResponseInfo.getTargetLsn();
				context->setSyncTargetLsn(senderNodeId, backupLsn);
				if (ownerLsn == backupLsn) {
					if (context->decrementCounter(senderNodeId)) {
						context->resetCounter();
						util::XArray<NodeId> backups(alloc);
						context->getSyncTargetNodeIds(backups);
						for (size_t pos = 0; pos < backups.size(); pos++) {
							LogSequentialNumber backupLsn = 0;
							SyncId backupSyncId;
							NodeId targetNodeId = backups[pos];
							if (!context->getSyncTargetLsnWithSyncId(
									targetNodeId, backupLsn, backupSyncId)) {
								DUMP_CLUSTER(	"Invalid sync context, targetNode=" << pt_->dumpNodeAddress(targetNodeId)
										<< ", lsn=" << backupLsn);
								GS_THROW_USER_ERROR(GS_ERROR_SYNC_INVALID_CONTEXT,
									"Invalid sync context, targetNode=" << pt_->dumpNodeAddress(targetNodeId)
									<< ", lsn=" << backupLsn);
							}
							syncRequestInfo.clearSearchCondition();
							syncRequestInfo.set(
									TXN_SHORTTERM_SYNC_END, context, ownerLsn, backupSyncId);
							Event syncEndEvent(ec, TXN_SHORTTERM_SYNC_END, pId);
							syncSvc_->encode(syncEndEvent, syncRequestInfo);
							sendEvent(ec, *txnEE_, backups[pos], syncEndEvent);
						}
					}
				}
				else {
					syncRequestInfo.setSearchLsnRange(backupLsn + 1, ownerLsn);
					util::Stopwatch logWatch;
					context->start(logWatch);
					try {
						syncRequestInfo.getLogs(pId, context->getSequentialNumber(), logMgr_, cpSvc_);
						context->endLog(logWatch);
						context->setProcessedLsn(syncRequestInfo.getStartLsn(), syncRequestInfo.getEndLsn());
						context->incProcessedLogNum(syncRequestInfo.getBinaryLogSize());
					}
					catch (UserException &) {
						context->endLog(logWatch);
						pt_->setStartLSN(pId, pt_->getLSN(pId));
						DUMP_CLUSTER("Short term log sync failed, log is not found or invalid, pId="
							<< pId << ", lsn=" << backupLsn << ", nd=" <<  pt_->dumpNodeAddress(senderNodeId) 
										<< ", startLsn=" << syncRequestInfo.getStartLsn() << ", endLsn=" << syncRequestInfo.getEndLsn() 
										<< ", logStartLsn=" << pt_->getStartLSN(pId));


						GS_THROW_USER_ERROR(GS_ERROR_SYNC_LOG_GET_FAILED,
							"Short term log sync failed, log is not found or invalid, pId="
							<< pId << ", lsn=" << backupLsn << ", nd=" <<  pt_->dumpNodeAddress(senderNodeId) 
										<< ", startLsn=" << syncRequestInfo.getStartLsn() << ", endLsn=" << syncRequestInfo.getEndLsn() 
										<< ", logStartLsn=" << pt_->getStartLSN(pId) << ", senderNode=" << senderNd);
					}
					Event syncLogEvent(ec, SYC_SHORTTERM_SYNC_LOG, pId);
					syncRequestInfo.set(SYC_SHORTTERM_SYNC_LOG, context,
							ownerLsn, syncResponseInfo.getBackupSyncId());
					syncSvc_->encode(syncLogEvent, syncRequestInfo);
					sendEvent(ec, *syncEE_, senderNodeId, syncLogEvent, control.getOption()->timeoutMillis_);
				}
			}
			break;
		}
		case TXN_SHORTTERM_SYNC_END: {
			syncSvc_->decode(alloc, ev, syncRequestInfo);
			SyncId syncId = syncRequestInfo.getBackupSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			syncMgr_->checkExecutable(
				OP_SHORTTERM_SYNC_END, pId, syncRequestInfo.getPartitionRole());
			if (context != NULL) {
				if (senderNd.isEmpty()) {
					GS_THROW_USER_ERROR(GS_ERROR_SYNC_INVALID_SENDER_ND, "");
				}
				pt_->updatePartitionRole(pId);
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
				clsSvc_->requestChangePartitionStatus(ec,
					alloc, pId, nextStatus, PT_CHANGE_SYNC_END);

				Event syncEndAckEvent(ec, TXN_SHORTTERM_SYNC_END_ACK, pId);
				syncResponseInfo.set(TXN_SHORTTERM_SYNC_END_ACK,
						syncRequestInfo, logMgr_->getLSN(pId), context);
				syncSvc_->encode(syncEndAckEvent, syncResponseInfo);
				sendEvent(ec, *txnEE_, senderNodeId, syncEndAckEvent);
				context->endAll();
				syncMgr_->removeSyncContext(ec, pId, context, false);
			}
			break;
		}
		case TXN_SHORTTERM_SYNC_END_ACK: {
			syncSvc_->decode(alloc, ev, syncResponseInfo);
			SyncId syncId = syncResponseInfo.getSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			syncMgr_->checkExecutable(OP_SHORTTERM_SYNC_END_ACK, pId,
					syncResponseInfo.getPartitionRole());
			if (context != NULL) {
				if (pt_->getLSN(pId) != syncResponseInfo.getTargetLsn()) {
					DUMP_CLUSTER("Invalid owner backup lsn, owner=" << pt_->getLSN(pId)
						<< ", backup=" << syncResponseInfo.getTargetLsn());
					GS_THROW_USER_ERROR(
							GS_ERROR_SYNC_OWNER_BACKUP_INVALID_LSN_CONSTRAINT,
							"Invalid owner backup lsn, owner=" << pt_->getLSN(pId)
							<< ", backup=" << syncResponseInfo.getTargetLsn());
				}
				context->setSyncTargetLsn(senderNodeId, syncResponseInfo.getTargetLsn());
				if (context->decrementCounter(senderNodeId)) {
					pt_->updatePartitionRole(pId);
					PartitionStatus nextStatus = PartitionTable::PT_ON;
					PartitionRoleStatus nextRoleStatus = PartitionTable::PT_NONE;
					if (pt_->isOwner(pId)) {
						nextRoleStatus = PartitionTable::PT_OWNER;
					}
					else if (pt_->isBackup(pId)) {
						nextRoleStatus = PartitionTable::PT_BACKUP;
					}
					else if (pt_->isCatchup(pId)) {
						nextRoleStatus = PartitionTable::PT_CATCHUP;
						nextStatus = PartitionTable::PT_OFF;
					}
					else {
						nextStatus = PartitionTable::PT_OFF;
					}
					pt_->setPartitionRoleStatus(pId, nextRoleStatus);
					clsSvc_->requestChangePartitionStatus(ec,
						alloc, pId, nextStatus, PT_CHANGE_SYNC_END);
					context->endAll();
					if (pt_->hasCatchupRole(pId)) {
						GS_TRACE_WARNING(SYNC_DETAIL, GS_TRACE_SYNC_OPERATION,
						"Pre short term sync completed, long term sync start, pId="
							<< pId << ", SSN=" << context->getSequentialNumber() 
							<< ", revision=" << context->getPartitionRevision().sequentialNumber_
							<< ", lsn=" << pt_->getLSN(pId));
					}
					syncMgr_->removeSyncContext(ec, pId, context, false);

					if (pt_->hasCatchupRole(pId)) {
						NodeId nextOwner = pt_->getOwner(pId);
						SyncRequestInfo syncRequestInfoForLong(alloc,
								TXN_LONGTERM_SYNC_REQUEST, syncMgr_, pId, txnEE_,
								MODE_LONGTERM_SYNC);
						PartitionRole role;
						pt_->getPartitionRole(pId, role, PartitionTable::PT_NEXT_OB);
						syncRequestInfoForLong.setPartitionRole(role);
						Event requestEvent(ec, TXN_LONGTERM_SYNC_REQUEST, pId);
						syncSvc_->encode(requestEvent, syncRequestInfoForLong);
						sendEvent(ec, *txnEE_, nextOwner, requestEvent);
					}
				}
			}
			break;
		}
		default: { GS_THROW_USER_ERROR(GS_ERROR_SYNC_EVENT_TYPE_INVALID, ""); }
		}
		WATCHER_END_SYNC(getEventTypeName(eventType), pId);
	}
	catch (UserException &e) {
		if (context != NULL) {
			int32_t errorCode = e.getErrorCode();
			if (errorCode != GS_ERROR_SYM_INVALID_PARTITION_REVISION) {
				UTIL_TRACE_EXCEPTION(SYNC_DETAIL, e, "Short term sync faild, pId=" << pId 
					<< ", revision=" << context->getPartitionRevision().sequentialNumber_ 
					<< ", event=" << getEventTypeName(eventType));
				pt_->setErrorStatus(pId, PT_ERROR_SHORT_SYNC_FAIL);

				DUMP_CLUSTER("SHORTTERM SYNC FAILED, pId=" << pId 
					<< ", ssn=" << context->getSequentialNumber() 
					<< ", revision=" << context->getPartitionRevision().sequentialNumber_ 
					<< ", event=" << getEventTypeName(eventType) << ", error=" << e.getNamedErrorCode().getName());
				addTimeoutEvent(ec, pId,ec.getAllocator(), MODE_SHORTTERM_SYNC, context, true);
				}
				TRACE_SYNC_EXCEPTION(
					e, eventType, pId, ERROR, "Short term sync operation failed, SSN=" << context->getSequentialNumber());
		}
		else {
		TRACE_SYNC_EXCEPTION(
			e, eventType, pId, ERROR, "Short term sync operation failed");
		}
	}
	catch (LockConflictException &e) {
		if (context != NULL) {
			addTimeoutEvent(ec, pId,ec.getAllocator(), MODE_SHORTTERM_SYNC, context, true);
		}
	}
	catch (LogRedoException &e) {
		TRACE_SYNC_EXCEPTION(e, eventType, pId, ERROR,
				"Short term sync failed, log redo exception is occurred.");
		if (context != NULL) {
				addTimeoutEvent(ec, pId,ec.getAllocator(), MODE_SHORTTERM_SYNC, context, true);
		}
		{
			util::StackAllocator &alloc = ec.getAllocator();
			util::StackAllocator::Scope scope(alloc);
			clsSvc_->requestGossip(ec, alloc, pId, 0, GOSSIP_INVALID_NODE);
		}
	}
	catch (std::exception &e) {
		if (context != NULL) {
			addTimeoutEvent(ec, pId,ec.getAllocator(), MODE_SHORTTERM_SYNC, context, true);
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
		SyncVariableSizeAllocator &varSizeAlloc = syncMgr_->getVariableSizeAllocator();
		SyncRequestInfo syncRequestInfo(alloc, eventType, syncMgr_, pId,
				clsSvc_->getEE(), MODE_LONGTERM_SYNC);
		SyncResponseInfo syncResponseInfo(alloc, eventType, syncMgr_, pId,
				MODE_LONGTERM_SYNC, logMgr_->getLSN(pId));

		WATCHER_START;
		switch (eventType) {
		case TXN_LONGTERM_SYNC_REQUEST: {
			syncSvc_->decode(alloc, ev, syncRequestInfo);
			syncMgr_->checkExecutable(OP_LONGTERM_SYNC_REQUEST, pId,
					syncRequestInfo.getPartitionRole());
			PartitionRole &nextRole = syncRequestInfo.getPartitionRole();
			context = syncMgr_->createSyncContext(ec, pId, nextRole.getRevision(),
					MODE_LONGTERM_SYNC, PartitionTable::PT_OWNER);
			context->startAll();
			DUMP_CLUSTER( "LONGTERM SYNC START, pId=" << pId << ", revision=" << context->getPartitionRevision().sequentialNumber_);
			pt_->setPartitionRoleWithCheck(pId, nextRole, PartitionTable::PT_CURRENT_OB);
		}
		break;
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
			if (!context->isSendReady()) {
				context->setSendReady();
			}
			else {
					Event syncChunkAckEvent(ec, TXN_LONGTERM_SYNC_CHUNK_ACK, pId);
					clsSvc_->encode(syncChunkAckEvent, syncInfo);
					sendEvent(ec, *txnEE_, senderNodeId, syncChunkAckEvent);
					return;
				}
			PartitionRole checkRole;
			checkRole.set(context->getPartitionRevision());
			syncMgr_->checkExecutable(OP_LONGTERM_SYNC_PREPARE_ACK, pId,
				checkRole);
			util::XArray<NodeId> catchups(alloc);
			pt_->getCatchup(pId, catchups, PartitionTable::PT_CURRENT_OB);
			syncRequestInfo.set(TXN_LONGTERM_SYNC_START, context,
					logMgr_->getLSN(pId), syncResponseInfo.getBackupSyncId());
			PartitionRole nextRole;
			pt_->getPartitionRole(pId, nextRole);
			syncRequestInfo.setPartitionRole(nextRole);
			if (catchups.size() > 0) {
				Event syncStartEvent(ec, TXN_LONGTERM_SYNC_START, pId);
				syncSvc_->encode(syncStartEvent, syncRequestInfo);
				for (size_t pos = 0; pos < catchups.size(); pos++) {
					sendEvent(ec, *txnEE_, catchups[pos], syncStartEvent);
					context->incrementCounter(catchups[pos]);
				}
			}
			else {
				GS_THROW_USER_ERROR(GS_ERROR_SYNC_INVALID_CATCHUP_INFO, "");
			}
			break;
		}
		case TXN_LONGTERM_SYNC_START: {
			syncSvc_->decode(alloc, ev, syncRequestInfo);
			SyncStat &stat = clsMgr_->getSyncStat();
			stat.init(false);
			syncMgr_->checkExecutable(OP_LONGTERM_SYNC_START, pId,
					syncRequestInfo.getPartitionRole());
			PartitionRole &nextCatchup = syncRequestInfo.getPartitionRole();
			pt_->setPartitionRoleWithCheck(pId, nextCatchup);

			context = syncMgr_->createSyncContext(ec, pId, nextCatchup.getRevision(),
					MODE_LONGTERM_SYNC, PartitionTable::PT_CATCHUP);
			context->startAll();
			Event syncStartAckEvent(ec, TXN_LONGTERM_SYNC_START_ACK, pId);
			syncResponseInfo.set(TXN_LONGTERM_SYNC_START_ACK, syncRequestInfo,
					logMgr_->getLSN(pId), context);
			syncSvc_->encode(syncStartAckEvent, syncResponseInfo);
			sendEvent(ec, *txnEE_, senderNodeId, syncStartAckEvent);
			break;
		}
		case TXN_LONGTERM_SYNC_START_ACK: {
			syncSvc_->decode(alloc, ev, syncResponseInfo);
			SyncStat &stat = clsMgr_->getSyncStat();
			stat.init(true);
			SyncId syncId = syncResponseInfo.getSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			syncMgr_->checkExecutable(OP_LONGTERM_SYNC_START_ACK, pId,
					syncResponseInfo.getPartitionRole());
			if (context != NULL) {
				context->setSyncStartCompleted(true);
				LogSequentialNumber ownerLsn = logMgr_->getLSN(pId);
				LogSequentialNumber catchupLsn = syncResponseInfo.getTargetLsn();
				context->setSyncTargetLsnWithSyncId(senderNodeId, catchupLsn, syncResponseInfo.getBackupSyncId());

				syncRequestInfo.clearSearchCondition();
				syncRequestInfo.set(SYC_LONGTERM_SYNC_LOG, context,
						logMgr_->getLSN(pId), syncResponseInfo.getBackupSyncId());
				syncRequestInfo.setSearchLsnRange(catchupLsn + 1, catchupLsn + 1);
				bool isSyncLog = false;
				if ((ownerLsn == 0 && catchupLsn == 0) || ownerLsn == catchupLsn) {
					isSyncLog = true;
				}
				else {
					try {
						isSyncLog = syncRequestInfo.getLogs(pId, context->getSequentialNumber(), logMgr_, cpSvc_);
					}
					catch (UserException &) {
					}
				}
				if (isSyncLog) {

					GS_TRACE_WARNING(SYNC_DETAIL, GS_TRACE_SYNC_OPERATION,
					"Log sync start, pId=" << pId << ", SSN=" << context->getSequentialNumber() 
						<< ", revision=" << context->getPartitionRevision().sequentialNumber_
						<< ", lsn=" << pt_->getLSN(pId));

					context->setSyncCheckpointCompleted();
					SyncStat &stat = clsMgr_->getSyncStat();
					stat.syncApplyNum_ = 0;
					stat.syncChunkNum_ = 0;
					LogSequentialNumber startLsn
							= (ownerLsn == catchupLsn) ? ownerLsn : catchupLsn + 1;
					syncRequestInfo.setSearchLsnRange(startLsn, ownerLsn);
					if ((ownerLsn == 0 && catchupLsn == 0) || (ownerLsn == catchupLsn)) {
					}
					else {
						util::Stopwatch logWatch;
						context->start(logWatch);
						try {
						syncRequestInfo.getLogs(pId, context->getSequentialNumber(),  logMgr_, cpSvc_);
						context->endLog(logWatch);
						context->setProcessedLsn(syncRequestInfo.getStartLsn(), syncRequestInfo.getEndLsn());
						context->incProcessedLogNum(syncRequestInfo.getBinaryLogSize());
						}
						catch (UserException &) {
							context->endLog(logWatch);
							clsSvc_->requestGossip(
									ec, alloc, pId, senderNodeId, GOSSIP_INVALID_CATHCUP_PARTITION);
							DUMP_CLUSTER("Long term log sync failed, log is not found or invalid, pId="
								<< pId << ", lsn=" << catchupLsn << ", nd=" << pt_->dumpNodeAddress(senderNodeId) 
											<< ", startLsn=" << syncRequestInfo.getStartLsn() << ", endLsn=" << syncRequestInfo.getEndLsn() 
											<< ", logStartLsn=" << pt_->getStartLSN(pId));

							GS_THROW_USER_ERROR(GS_ERROR_SYNC_LOG_GET_FAILED,
								"Long term log sync failed, log is not found or invalid, pId="
									<< pId << ", catchupLsn=" << catchupLsn << ", backupAddress=" << pt_->dumpNodeAddress(senderNodeId) 
								<< ", getStartLsn=" << syncRequestInfo.getStartLsn() << ", actualGetLastLsn=" << syncRequestInfo.getEndLsn());
						}
					}
					Event syncLogEvent(ec, SYC_LONGTERM_SYNC_LOG, pId);
					syncSvc_->encode(syncLogEvent, syncRequestInfo);
					sendEvent(ec, *syncEE_, senderNodeId, syncLogEvent);
					break;
				}
				else {
					GS_TRACE_WARNING(SYNC_DETAIL, GS_TRACE_SYNC_OPERATION,
					"Long term chunk sync start, pId=" << pId << ", SSN=" << context->getSequentialNumber() 
						<< ", revision=" << context->getPartitionRevision().sequentialNumber_
						<< ", lsn=" << pt_->getLSN(pId));
					uint64_t totalCount = 0;
					syncRequestInfo.set(SYC_LONGTERM_SYNC_CHUNK);
					SyncStat &stat = clsMgr_->getSyncStat();
					int32_t sendChunkNum = syncMgr_->getConfig().getSendChunkNum();
					int32_t sendChunkSize = syncMgr_->getChunkSize();
					syncRequestInfo.setSearchChunkCondition(
							sendChunkNum, sendChunkSize);
					const util::DateTime &now = ec.getHandlerStartTime();
					const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
					const TransactionManager::ContextSource src(eventType);
					TransactionContext &txn = txnMgr_->put(
							alloc, pId, TXN_EMPTY_CLIENTID, src, now, emNow);
					DataStore::Latch latch(txn, pId, ds_, clsSvc_);
					util::Stopwatch chunkWatch;
					context->start(chunkWatch);
					try {
						if (syncRequestInfo.getChunks(alloc, pId, pt_, logMgr_, chunkMgr_, cpSvc_,
								context->getSequentialNumber(), totalCount)) {
							context->setSyncCheckpointCompleted();
						GS_TRACE_WARNING(SYNC_DETAIL, GS_TRACE_SYNC_OPERATION,
						"Long term chunk sync completed and log sync start, pId=" << pId << ", SSN=" << context->getSequentialNumber() 
							<< ", revision=" << context->getPartitionRevision().sequentialNumber_
							<< ", send chunks=" << context->getProcessedChunkNum());
						}
					}
					catch (std::exception &e) {
						context->endChunk(chunkWatch);
						clsSvc_->requestGossip(
								ec, alloc, pId, senderNodeId, GOSSIP_INVALID_CATHCUP_PARTITION);
						GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e,
								"Long term chunk sync failed, get chunk"));
					}
					context->endChunk(chunkWatch);
					Event syncChunkEvent(ec, SYC_LONGTERM_SYNC_CHUNK, pId);
					syncSvc_->encode(syncChunkEvent, syncRequestInfo);
					context->setSyncCheckpointPending(true);
					sendEvent(ec, *syncEE_, senderNodeId, syncChunkEvent);
					int32_t prevCount = context->getProcessedChunkNum() %
							syncMgr_->getExtraConfig().getLongtermDumpChunkInterval();
					bool isFirst = (context->getProcessedChunkNum() == 0);

					context->incProcessedChunkNum(syncRequestInfo.getChunkNum());
					pt_->setPartitionChunkNum(pId, context->getProcessedChunkNum());
					if ((prevCount + sendChunkNum) >= syncMgr_->getExtraConfig()
								.getLongtermDumpChunkInterval() || isFirst || context->isSyncCheckpointCompleted()) {
						if (context->isSyncCheckpointCompleted()) {
							GS_TRACE_WARNING(SYNC_DETAIL, GS_TRACE_SYNC_OPERATION,
							"Long term chunk sync completed, (pId=" << pId
								<< ", catchup=" << pt_->dumpNodeAddress(senderNodeId)
											<< ", send chunks=" << context->getProcessedChunkNum() << ", total chunks=" << totalCount);
						}
						else {
							GS_TRACE_WARNING(SYNC_DETAIL, GS_TRACE_SYNC_OPERATION,
							"Long term chunk sync (pId=" << pId
								<< ", catchup=" << pt_->dumpNodeAddress(senderNodeId)
											<< ", send chunks=" << context->getProcessedChunkNum() << ", total chunks=" << totalCount);
						}
						stat.syncChunkNum_ = context->getProcessedChunkNum();
					}
				}
			}
			break;
		}
		case TXN_LONGTERM_SYNC_CHUNK: {
			syncSvc_->decode(alloc, ev, syncRequestInfo);
			SyncId syncId = syncRequestInfo.getBackupSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			syncMgr_->checkExecutable(OP_LONGTERM_SYNC_CHUNK, pId,
					syncRequestInfo.getPartitionRole());
			if (context != NULL) {
				if (context->getProcessedChunkNum() == 0) {
					removePartition(ec, ev);
					util::XArray<uint8_t> binaryLogList(alloc);
					logMgr_->putDropPartitionLog(binaryLogList, pId);
				}
				int32_t chunkNum = 0;
				int32_t chunkSize = 0;
				context->getChunkInfo(chunkNum, chunkSize);
//				PartitionRevisionNo revisionNo = syncRequestInfo.getPartitionRole().getRevisionNo();
				util::Stopwatch chunkWatch;
				context->start(chunkWatch);
				for (int32_t chunkNo = 0; chunkNo < chunkNum; chunkNo++) {
					uint8_t *chunkBuffer = NULL;
					context->getChunkBuffer(chunkBuffer, chunkNo);
					if (chunkBuffer != NULL) {
						try {
							chunkMgr_->recoveryChunk(pId, chunkBuffer, chunkSize, false);
						}
						catch (std::exception &e) {
							context->endChunk(chunkWatch);
							clsSvc_->requestGossip(
									ec, alloc, pId, senderNodeId, GOSSIP_INVALID_CATHCUP_PARTITION);
							GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e,
									"Long term chunk sync failed, reason=recovery chunk is failed"));
						}
						context->incProcessedChunkNum();
						bool isFirst = (context->getProcessedChunkNum() == 0);
						pt_->setPartitionChunkNum(pId, context->getProcessedChunkNum());
						if (isFirst || (context->getProcessedChunkNum() %
								syncMgr_->getExtraConfig().getLongtermDumpChunkInterval()) == 0) {
							GS_TRACE_WARNING(SYNC_DETAIL, GS_TRACE_SYNC_OPERATION,
									"Long term chunk sync (pId=" << pId << ", owner="
									<< pt_->dumpNodeAddress(senderNodeId) << ", processed chunks="
									<< context->getProcessedChunkNum());
						}
						pt_->setPartitionChunkNum(pId, context->getProcessedChunkNum(), 0);	
						SyncStat &stat = clsMgr_->getSyncStat();
						stat.syncApplyNum_ = context->getProcessedChunkNum();
					}
				}
				context->endChunk(chunkWatch);
				context->freeBuffer(varSizeAlloc, CHUNK_SYNC, syncMgr_->getSyncOptStat());
				uint8_t *logBuffer = NULL;
				int32_t logBufferSize = 0;
				context->getLogBuffer(logBuffer, logBufferSize);
				if (logBuffer != NULL && logBufferSize > 0) {
//					LogSequentialNumber startLsn = 
					recoveryMgr_->restoreTransaction(pId, logBuffer, ec.getHandlerStartMonotonicTime());

				GS_TRACE_WARNING(SYNC_DETAIL, GS_TRACE_SYNC_OPERATION,
					"Long term chunk sync completed, pId=" << pId << ", SSN=" << context->getSequentialNumber() 
						<< ", revision=" << context->getPartitionRevision().sequentialNumber_
						<< ", lsn=" << pt_->getLSN(pId));
					context->freeBuffer(varSizeAlloc, LOG_SYNC, syncMgr_->getSyncOptStat());
					const util::DateTime &now = ec.getHandlerStartTime();
					const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
					const TransactionManager::ContextSource src(eventType);
					TransactionContext &txn = txnMgr_->put(
							alloc, pId, TXN_EMPTY_CLIENTID, src, now, emNow);
					ds_->restartPartition(txn, clsSvc_);
				}
				context->endChunkAll();
				Event syncChunkAckEvent(ec, TXN_LONGTERM_SYNC_CHUNK_ACK, pId);
				syncResponseInfo.set(TXN_LONGTERM_SYNC_CHUNK_ACK,
						syncRequestInfo, logMgr_->getLSN(pId), context);
				syncSvc_->encode(syncChunkAckEvent, syncResponseInfo);
				NodeId ownerNodeId = pt_->getOwner(pId, PartitionTable::PT_CURRENT_OB);
				sendEvent(ec, *txnEE_, ownerNodeId, syncChunkAckEvent);
			}
			break;
		}
		case TXN_LONGTERM_SYNC_CHUNK_ACK: {
			SyncId syncId;
//			LogSequentialNumber catchupLsn;
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
				assert(tmpCatchups.size() == 1);
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

				syncMgr_->checkExecutable(OP_LONGTERM_SYNC_CHUNK_ACK, pId, nextRole);
				if (context->isSyncCheckpointPending()) {
					return;
				}
			}
			else {
				syncSvc_->decode(alloc, ev, syncResponseInfo);
				syncId = syncResponseInfo.getSyncId();
				context = syncMgr_->getSyncContext(pId, syncId);
				if (context == NULL) {
					return;
				}
				syncMgr_->checkExecutable(OP_LONGTERM_SYNC_CHUNK_ACK, pId,
					syncResponseInfo.getPartitionRole());
				context->setSyncCheckpointPending(false);
			}
			if (context != NULL) {
				util::XArray<NodeId> catchups(alloc);
				context->getSyncTargetNodeIds(catchups);
				if (catchups.size() != 1) {
					return ;
				}
				senderNodeId = catchups[0];

				LogSequentialNumber ownerLsn = logMgr_->getLSN(pId);
				LogSequentialNumber catchupLsn =
					syncResponseInfo.getTargetLsn();
				SyncControlContext control(eventType, pId, catchupLsn);
				if (!controlSyncLoad(control)) {
					sendEvent(ec, *txnEE_, 0, ev, control.getWaitTime());
					return;
				}
				context->setSyncTargetLsn(senderNodeId, catchupLsn);
				if (context->isSyncCheckpointCompleted()) {
					context->endChunkAll();

					syncRequestInfo.set(SYC_LONGTERM_SYNC_LOG, context, logMgr_->getLSN(pId),
							syncResponseInfo.getBackupSyncId());
					if (ownerLsn > catchupLsn) {
						syncRequestInfo.setSearchLsnRange(catchupLsn + 1, ownerLsn);
						util::Stopwatch logWatch;
						context->start(logWatch);
						try {
							syncRequestInfo.getLogs(pId, context->getSequentialNumber(), logMgr_, cpSvc_);
							context->endLog(logWatch);
							context->setProcessedLsn(syncRequestInfo.getStartLsn(), syncRequestInfo.getEndLsn());
							context->incProcessedLogNum(syncRequestInfo.getBinaryLogSize());

							GS_TRACE_WARNING(SYNC_DETAIL, GS_TRACE_SYNC_OPERATION,
							"Long term chunk sync completed and log sync start, pId=" << pId << ", SSN=" << context->getSequentialNumber() 
								<< ", revision=" << context->getPartitionRevision().sequentialNumber_
								<< ", send chunks=" << context->getProcessedChunkNum()
								<< ", start lsn=" << syncRequestInfo.getStartLsn());
						}
						catch (UserException &e) {
						context->endLog(logWatch);
							UTIL_TRACE_EXCEPTION(SYNC_SERVICE, e, "");
							DUMP_CLUSTER("Long term log sync failed, log is not found or invalid, pId="
								<< pId << ", lsn=" << catchupLsn << ", nd=" << pt_->dumpNodeAddress(senderNodeId) 
										<< ", startLsn=" << syncRequestInfo.getStartLsn() << ", endLsn=" << syncRequestInfo.getEndLsn() 
										<< ", logStartLsn=" << pt_->getStartLSN(pId));
							clsSvc_->requestGossip(
									ec, alloc, pId, senderNodeId, GOSSIP_INVALID_CATHCUP_PARTITION);
							GS_THROW_USER_ERROR(GS_ERROR_SYNC_LOG_GET_FAILED,
								"Long term log sync failed, log is not found or invalid, pId="
								<< pId << ", lsn=" << catchupLsn << ", nd=" << pt_->dumpNodeAddress(senderNodeId) 
										<< ", startLsn=" << syncRequestInfo.getStartLsn() << ", endLsn=" << syncRequestInfo.getEndLsn() 
										<< ", logStartLsn=" << pt_->getStartLSN(pId));
						}
					}
					else {
						syncRequestInfo.setSearchLsnRange(ownerLsn, ownerLsn);
					}
					Event syncLogEvent(ec, SYC_LONGTERM_SYNC_LOG, pId);
					syncSvc_->encode(syncLogEvent, syncRequestInfo);
					sendEvent(ec, *syncEE_, senderNodeId, syncLogEvent, control.getOption()->timeoutMillis_);
				}
				else {
					SyncId catchupSyncId;
					context->getCatchupSyncId(catchupSyncId);
					syncRequestInfo.set(SYC_LONGTERM_SYNC_CHUNK, context, logMgr_->getLSN(pId),
							catchupSyncId);
					int32_t sendChunkNum = syncMgr_->getConfig().getSendChunkNum();
					int32_t sendChunkSize = syncMgr_->getChunkSize();
					syncRequestInfo.setSearchChunkCondition(sendChunkNum,
							sendChunkSize);
					uint64_t totalCount = 0;
					const util::DateTime &now = ec.getHandlerStartTime();
					const EventMonotonicTime emNow =
						ec.getHandlerStartMonotonicTime();
					const TransactionManager::ContextSource src(eventType);
					TransactionContext &txn = txnMgr_->put(
							alloc, pId, TXN_EMPTY_CLIENTID, src, now, emNow);
					DataStore::Latch latch(txn, pId, ds_, clsSvc_);
					util::Stopwatch chunkWatch;
					context->start(chunkWatch);
//					bool isChunkEnd = false;
					try {
						if (syncRequestInfo.getChunks(alloc, pId, pt_, logMgr_, chunkMgr_, cpSvc_,
								context->getSequentialNumber(), totalCount)) {
							context->setSyncCheckpointCompleted();
						}
						else {
							if (syncRequestInfo.getChunkNum() == 0) {
								context->setSyncCheckpointPending(false);
								context->endLog(chunkWatch);
								return;
							}
						}
						context->endLog(chunkWatch);
					}
					catch (std::exception &e) {
						context->endLog(chunkWatch);
						GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e,
								"Long term chunk sync failed, get chunk is failed"));
					}

					if (syncRequestInfo.getChunkList().size() == 0) {
						return;
					}
					Event syncChunkEvent(ec, SYC_LONGTERM_SYNC_CHUNK, pId);
					syncSvc_->encode(syncChunkEvent, syncRequestInfo);
					context->setSyncCheckpointPending(true);
					sendEvent(ec, *syncEE_, senderNodeId, syncChunkEvent, control.getOption()->timeoutMillis_);
					int32_t prevCount = context->getProcessedChunkNum() %
							syncMgr_->getExtraConfig().getLongtermDumpChunkInterval();
					bool isFirst = (context->getProcessedChunkNum() == 0);

					context->incProcessedChunkNum(syncRequestInfo.getChunkNum());
					pt_->setPartitionChunkNum(pId, context->getProcessedChunkNum());
					if ((prevCount + sendChunkNum) >= syncMgr_->getExtraConfig()
								.getLongtermDumpChunkInterval() || isFirst || context->isSyncCheckpointCompleted()) {
						if (context->isSyncCheckpointCompleted()) {
							GS_TRACE_WARNING(SYNC_DETAIL, GS_TRACE_SYNC_OPERATION,
									"Long term chunk sync completed, (pId=" << pId << ", catchup="
									<< pt_->dumpNodeAddress(senderNodeId) << ", send chunks="
									<< context->getProcessedChunkNum() << ")");
						}
						else {
							GS_TRACE_WARNING(SYNC_DETAIL, GS_TRACE_SYNC_OPERATION,
									"Long term chunk sync (pId=" << pId << ", catchup="
									<< pt_->dumpNodeAddress(senderNodeId) << ", send chunks="
									<< context->getProcessedChunkNum() << ")");
						}
					}
					SyncStat &stat = clsMgr_->getSyncStat();
					stat.syncChunkNum_ = context->getProcessedChunkNum();
					break;
				}
			}
			break;
		}
		case TXN_LONGTERM_SYNC_LOG: {
			syncSvc_->decode(alloc, ev, syncRequestInfo);
			SyncId syncId = syncRequestInfo.getBackupSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			syncMgr_->checkExecutable(
					OP_LONGTERM_SYNC_LOG, pId, syncRequestInfo.getPartitionRole());
			if (context != NULL) {
				if (context->getProcessedChunkNum() == 0 && logMgr_->getLSN(pId) == 0) {
					removePartition(ec, ev);
				}
				uint8_t *logBuffer = NULL;
				int32_t logBufferSize = 0;
				context->getLogBuffer(logBuffer, logBufferSize);
				if (logBuffer != NULL && logBufferSize > 0) {
					util::Stopwatch logWatch;
					context->start(logWatch);
					LogSequentialNumber startLsn = pt_->getLSN(pId);
					try {
						recoveryMgr_->redoLogList(alloc, RecoveryManager::MODE_LONG_TERM_SYNC, pId,
							ec.getHandlerStartTime(), ec.getHandlerStartMonotonicTime(), logBuffer, logBufferSize);
						context->endLog(logWatch);
						context->setProcessedLsn(startLsn, pt_->getLSN(pId));
						context->incProcessedLogNum(logBufferSize);
					}
					catch (std::exception &e) {
						context->endLog(logWatch);
						RM_RETHROW_LOG_REDO_ERROR(e, "");
					}
					context->freeBuffer(
						varSizeAlloc, LOG_SYNC, syncMgr_->getSyncOptStat());
				}

				Event syncLogAckEvent(ec, TXN_LONGTERM_SYNC_LOG_ACK, pId);
				syncResponseInfo.set(TXN_LONGTERM_SYNC_LOG_ACK, syncRequestInfo,
					logMgr_->getLSN(pId), context);
				syncSvc_->encode(syncLogAckEvent, syncResponseInfo);
				NodeId ownerNodeId = pt_->getOwner(pId, PartitionTable::PT_CURRENT_OB);
				sendEvent(ec, *txnEE_, ownerNodeId, syncLogAckEvent);
			}
			break;
		}
		case TXN_LONGTERM_SYNC_LOG_ACK: {
			syncSvc_->decode(alloc, ev, syncResponseInfo);
			SyncId syncId = syncResponseInfo.getSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			syncMgr_->checkExecutable(OP_LONGTERM_SYNC_LOG_ACK, pId,
					syncResponseInfo.getPartitionRole());
			if (context != NULL) {
				LogSequentialNumber ownerLsn = logMgr_->getLSN(pId);
				LogSequentialNumber catchupLsn = syncResponseInfo.getTargetLsn();
				SyncControlContext control(eventType, pId, catchupLsn);
				if (!controlSyncLoad(control)) {
					sendEvent(ec, *txnEE_, 0, ev, control.getWaitTime());
					return;
				}
				const NodeDescriptor &nd = syncEE_->getServerND(senderNodeId);
				if (nd.isEmpty()) {
					GS_THROW_USER_ERROR(GS_ERROR_SYNC_INVALID_SENDER_ND, "");
				}
				context->setSyncTargetLsn(senderNodeId, catchupLsn);
				if (ownerLsn == catchupLsn) {
					syncRequestInfo.setSearchLsnRange(ownerLsn, ownerLsn);
				}
				else {
					syncRequestInfo.setSearchLsnRange(catchupLsn + 1, ownerLsn);
					util::Stopwatch logWatch;
					context->start(logWatch);
					LogSequentialNumber startLsn = pt_->getLSN(pId);
					try {
						syncRequestInfo.getLogs(pId, context->getSequentialNumber(), logMgr_, cpSvc_);
						context->endLog(logWatch);
						context->setProcessedLsn(startLsn, pt_->getLSN(pId));
						context->incProcessedLogNum(syncRequestInfo.getBinaryLogSize());
					}
					catch (UserException &) {
						context->endLog(logWatch);
						clsSvc_->requestGossip(ec, alloc, pId, senderNodeId, GOSSIP_INVALID_CATHCUP_PARTITION);

						DUMP_CLUSTER("Long term log sync failed, log is not found or invalid, pId="
							<< pId << ", lsn=" << catchupLsn << ", nd=" << pt_->dumpNodeAddress(senderNodeId) 
										<< ", startLsn=" << syncRequestInfo.getStartLsn() << ", endLsn=" << syncRequestInfo.getEndLsn() 
										<< ", logStartLsn=" << pt_->getStartLSN(pId));

						GS_THROW_USER_ERROR(GS_ERROR_SYNC_LOG_GET_FAILED, "Long term log sync failed, log is not found or invalid, pId="
							<< pId << ", lsn=" << catchupLsn << ", nd=" << pt_->dumpNodeAddress(senderNodeId) 
										<< ", startLsn=" << syncRequestInfo.getStartLsn() << ", endLsn=" << syncRequestInfo.getEndLsn() 
										<< ", logStartLsn=" << pt_->getStartLSN(pId));
					}
					Event syncLogEvent(ec, SYC_LONGTERM_SYNC_LOG, pId);
					syncRequestInfo.set(SYC_LONGTERM_SYNC_LOG, context,
							ownerLsn, syncResponseInfo.getBackupSyncId());
					syncSvc_->encode(syncLogEvent, syncRequestInfo);
					sendEvent(ec, *syncEE_, senderNodeId, syncLogEvent, control.getWaitTime());
				}
			}
			break;
		}
		default: { GS_THROW_USER_ERROR(GS_ERROR_SYNC_EVENT_TYPE_INVALID, ""); }
		}
		WATCHER_END_SYNC(getEventTypeName(eventType), pId);
	}
	catch (UserException &e) {
		TRACE_SYNC_EXCEPTION(e, eventType, pId, WARNING,
				"Long term sync operation is failed.");
		if (context != NULL) {
			int32_t errorCode = e.getErrorCode();
			if (errorCode != GS_ERROR_SYM_INVALID_PARTITION_REVISION
				&& errorCode != GS_ERROR_SYM_INVALID_PARTITION_ROLE) {
				UTIL_TRACE_EXCEPTION(SYNC_SERVICE, e, "LONGTERM SYNC FAILED, pId=" << pId 
					<< ", ssn=" <<  context->getSequentialNumber() 
					<< ", revision=" << context->getPartitionRevision().sequentialNumber_ 
					<< ", event=" << getEventTypeName(eventType));
				pt_->setErrorStatus(pId, PT_ERROR_LONG_SYNC_FAIL);
				DUMP_CLUSTER( "LONGTERM SYNC FAILED, pId=" << pId 
					<< ", revision=" << context->getPartitionRevision().sequentialNumber_ 
					<< ", event=" << getEventTypeName(eventType)  << ", error=" << e.getNamedErrorCode().getName());
			}
			syncMgr_->removeSyncContext(ec, pId, context, true);
		}
	}
	catch (LockConflictException &e) {
		TRACE_SYNC_EXCEPTION(e, eventType, pId, ERROR,
				"Long term sync failed, lock confict exception is occurred, retry:"
				<< syncMgr_->getExtraConfig().getLockConflictPendingInterval());
		txnEE_->addTimer(
			ev, syncMgr_->getExtraConfig().getLockConflictPendingInterval());
	}
	catch (LogRedoException &e) {
		TRACE_SYNC_EXCEPTION(e, eventType, pId, ERROR,
				"Long term sync failed, log redo exception is occurred."
				<< syncMgr_->getExtraConfig().getLockConflictPendingInterval());
		if (context != NULL) {
			syncMgr_->removeSyncContext(ec, pId, context, true);
		}
		{
			util::StackAllocator &alloc = ec.getAllocator();
			util::StackAllocator::Scope scope(alloc);
			clsSvc_->requestGossip(ec, alloc, pId, 0, GOSSIP_INVALID_CATHCUP_PARTITION);
		}
	}
	catch (std::exception &e) {
		if (context != NULL) {
			syncMgr_->removeSyncContext(ec, pId, context, true);
		}
		clsSvc_->setError(ec, &e);
	}
}

/*!
	@brief Gets log information satifies a presetting condition
*/
bool SyncRequestInfo::getLogs(PartitionId pId, int64_t sId,  LogManager *logMgr, CheckpointService *cpSvc) {

	bool isSyncLog = false;
	LogRecord logRecord;
	LogSequentialNumber lastLsn = 0;
	logRecord.lsn_ = request_.startLsn_;
	int32_t callLogCount = 0;
	condition_.logSize_ = syncMgr_->getConfig().getMaxMessageSize();

	try {
		binaryLogRecords_.clear();
		LogCursor cursor;
		if (request_.syncMode_ == MODE_LONGTERM_SYNC) {
			if (!cpSvc->getLongSyncLog(sId, request_.startLsn_, cursor)) {
				GS_THROW_USER_ERROR(
					GS_ERROR_SYNC_LOG_GET_FAILED,
					"Long term log find failed, pId=" << pId << ", lsn=" << request_.startLsn_);
			}
		}
		else {
			if (!logMgr->findLog(cursor, pId, request_.startLsn_)) {
				GS_THROW_USER_ERROR(
					GS_ERROR_SYNC_LOG_GET_FAILED,
					"Short term log find failed, pId=" << pId << ", lsn=" << request_.startLsn_);
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
void SyncTimeoutHandler::operator()(EventContext &ec, Event &ev) {
	SyncContext *context = NULL;
	PartitionId pId = ev.getPartitionId();
//	EventType eventType = ev.getType();
	try {
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);
		SyncTimeoutInfo syncTimeoutInfo(
				alloc, TXN_SYNC_TIMEOUT, syncMgr_, pId, MODE_SYNC_TIMEOUT, context);
		syncSvc_->decode(alloc, ev, syncTimeoutInfo);
		SyncId syncId = syncTimeoutInfo.getSyncId();
		context = syncMgr_->getSyncContext(pId, syncId);
		syncMgr_->checkExecutable(
			OP_SYNC_TIMEOUT, pId, syncTimeoutInfo.getPartitionRole());
		if (context != NULL) {
//			NodeId currentOwner = pt_->getOwner(pId);
//			NodeId nextOwner = pt_->getOwner(pId, PartitionTable::PT_NEXT_OB);
			PartitionRevisionNo nextRevision = pt_->getPartitionRevision(pId, PartitionTable::PT_NEXT_OB);
			if (context != NULL && context->getSyncMode() == MODE_SHORTTERM_SYNC && 
					pt_->getPartitionStatus(pId) == PartitionTable::PT_SYNC) {
//				PartitionRole &checkRole = syncTimeoutInfo.getPartitionRole();
				if (syncTimeoutInfo.getRevisionNo() < nextRevision) {
					GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_REVISION, "revision check");
				}
				DUMP_CLUSTER( "SHORTTERM SYNC TIMEOUT, pId=" 
					<< pId << ", revision=" << context->getPartitionRevision().sequentialNumber_);

				if (context->checkTotalTime(clsMgr_->getConfig().getShortTermTimeoutInterval())) {
					GS_TRACE_WARNING(SYNC_DETAIL, GS_TRACE_SYNC_OPERATION,
							"Shortterm sync timeout, reset owner partition status, pId=" << pId << ", SSN=" << context->getSequentialNumber() 
						<< ", revision=" << context->getPartitionRevision().sequentialNumber_);
				}
				else {
					GS_TRACE_WARNING(SYNC_DETAIL, GS_TRACE_SYNC_OPERATION,
							"Shortterm sync error, reset owner partition status, pId=" << pId << ", SSN=" << context->getSequentialNumber() 
						<< ", revision=" << context->getPartitionRevision().sequentialNumber_);
				}

				clsSvc_->requestChangePartitionStatus(ec,
						alloc, pId, PartitionTable::PT_OFF, PT_CHANGE_SYNC_END);
			}
			syncMgr_->removeSyncContext(ec, pId, context, true);
		}
	}
	catch (UserException &e) {
		UTIL_TRACE_EXCEPTION_INFO(SYNC_SERVICE, e, "");
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
			if (pt_->getPartitionRoleRevision(pId, PartitionTable::PT_NEXT_OB)
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
		if (!pt_->isMaster()) {
			pt_->clear(pId, PartitionTable::PT_NEXT_OB);
			pt_->clear(pId, PartitionTable::PT_CURRENT_OB);
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
		SyncOperationType operationType;

		WATCHER_START;

		switch (syncEventType) {
		case SYC_SHORTTERM_SYNC_LOG: {
			operationType = OP_SHORTTERM_SYNC_LOG;
			txnEventType = TXN_SHORTTERM_SYNC_LOG;
			break;
		}
		case SYC_LONGTERM_SYNC_LOG: {
			operationType = OP_LONGTERM_SYNC_LOG;
			txnEventType = TXN_LONGTERM_SYNC_LOG;
			break;
		}
		case SYC_LONGTERM_SYNC_CHUNK: {
			operationType = OP_LONGTERM_SYNC_CHUNK;
			txnEventType = TXN_LONGTERM_SYNC_CHUNK;
			break;
		}
		default: { GS_THROW_USER_ERROR(GS_ERROR_SYNC_EVENT_TYPE_INVALID, ""); }
		}

		SyncRequestInfo syncRequestInfo(
			alloc, syncEventType, syncMgr_, pId, clsSvc_->getEE());
		syncSvc_->decode(alloc, ev, syncRequestInfo);
		syncMgr_->checkExecutable(
				operationType, pId, syncRequestInfo.getPartitionRole());

		SyncId syncId = syncRequestInfo.getBackupSyncId();
		context = syncMgr_->getSyncContext(pId, syncId);
		if (context == NULL) {
			return;
		}
		syncRequestInfo.setEventType(txnEventType);
		Event requestEvent(ec, txnEventType, pId);
		syncSvc_->encode(requestEvent, syncRequestInfo);
		SyncControlContext control(txnEventType, pId, syncRequestInfo.getLsn());
		controlSyncLoad(control);
		sendEvent(ec, *txnEE_, 0, requestEvent, control.getWaitTime());
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
	@brief Gets size of the current queue
*/
int32_t SyncHandler::getTransactionEEQueueSize(PartitionId pId) {
	EventEngine::Stats stats;
	if (txnEE_->getStats(pt_->getPartitionGroupId(pId), stats)) {
		return static_cast<int32_t>(
			stats.get(EventEngine::Stats::EVENT_ACTIVE_QUEUE_SIZE_CURRENT));
	}
	else {
		return 0;
	}
}

/*!
	@brief Adds Sync Timeout event
*/
void SyncHandler::addTimeoutEvent(EventContext &ec, PartitionId pId,
	util::StackAllocator &alloc, SyncMode mode, SyncContext *context,
	bool isImmediate) {
	try {
		int32_t timeoutInterval;
		if (isImmediate) {
			timeoutInterval = 0;
		}
		else if (mode == MODE_SHORTTERM_SYNC) {
			timeoutInterval = clsMgr_->getConfig().getShortTermTimeoutInterval();
		}
		else if (mode == MODE_LONGTERM_SYNC) {
			timeoutInterval = clsMgr_->getConfig().getLongTermTimeoutInterval();
			if (timeoutInterval == INT32_MAX) {
				return;
			}
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_SYNC_TIMER_SET_FAILED, "");
		}
		Event syncTimeoutEvent(ec, TXN_SYNC_TIMEOUT, pId);
		SyncTimeoutInfo syncTimeoutInfo(
				alloc, TXN_SYNC_TIMEOUT, syncMgr_, pId, mode, context);
		syncSvc_->encode(syncTimeoutEvent, syncTimeoutInfo);
		txnEE_->addTimer(syncTimeoutEvent, timeoutInterval);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Control synchronization load
*/
bool SyncHandler::controlSyncLoad(SyncControlContext &control) {
	int32_t queueSize = getTransactionEEQueueSize(control.pId_);
	EventType eventType = control.eventType_;
	if (eventType == TXN_LONGTERM_SYNC_LOG_ACK) {
		if (pt_->getLSN(control.pId_) - control.lsn_ <
			static_cast<LogSequentialNumber>(
				syncMgr_->getExtraConfig().getApproximateGapLsn())) {
			control.waitTime_ = syncMgr_->getExtraConfig().getApproximateWaitInterval();
			return false;
		}
	}
	bool isHighLoad = false;
	int32_t queueSizeLimit = 0;
	switch (eventType) {
	case TXN_SHORTTERM_SYNC_LOG_ACK:
	case TXN_SHORTTERM_SYNC_LOG:
		queueSizeLimit =
			syncMgr_->getExtraConfig().getLimitShorttermQueueSize();
		if (queueSize > queueSizeLimit) {
			isHighLoad = true;
		}
		break;
	case TXN_LONGTERM_SYNC_CHUNK_ACK:
	case TXN_LONGTERM_SYNC_LOG_ACK:
	case TXN_LONGTERM_SYNC_CHUNK:
	case TXN_LONGTERM_SYNC_LOG:
		queueSizeLimit = syncMgr_->getExtraConfig().getLimitLongtermQueueSize();
		if (queueSize > queueSizeLimit) {
			isHighLoad = true;
		}
		break;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_SYNC_SERVICE_UNKNOWN_EVENT_TYPE, "");
		break;
	}
	switch (eventType) {
	case TXN_LONGTERM_SYNC_CHUNK_ACK: {
		if (isHighLoad) {
			control.waitTime_ 
					= syncMgr_->getExtraConfig().getLongtermHighLoadChunkWaitInterval();
		}
		else {
			control.waitTime_ 
					= syncMgr_->getExtraConfig().getLongtermLowLoadChunkWaitInterval();
		}
		break;
	}
	case TXN_LONGTERM_SYNC_LOG_ACK: {
		if (isHighLoad) {
			control.waitTime_
					= syncMgr_->getExtraConfig().getLongtermHighLoadLogWaitInterval();
		}
		else {
			control.waitTime_
					= syncMgr_->getExtraConfig().getLongtermLowLoadLogWaitInterval();
		}
		break;
	}
	case TXN_SHORTTERM_SYNC_LOG_ACK:
	case TXN_SHORTTERM_SYNC_LOG:
	case TXN_LONGTERM_SYNC_LOG:
	case TXN_LONGTERM_SYNC_CHUNK:
	default:
		control.waitTime_ = 0;
		break;
	}
	control.option_.timeoutMillis_ = control.waitTime_;
	return true;
}

SyncHandler::SyncPartitionLock::SyncPartitionLock(
	TransactionManager *txnMgr, PartitionId pId)
	: txnMgr_(txnMgr), pId_(pId) {
	if (!txnMgr_->lockPartition(pId_)) {
		GS_THROW_CUSTOM_ERROR(LockConflictException,
				GS_ERROR_TXN_PARTITION_LOCK_CONFLICT,
				"Lock conflict is occurred, pId=" << pId_);
	}
}

SyncHandler::SyncPartitionLock::~SyncPartitionLock() {
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
			GS_THROW_USER_ERROR(GS_ERROR_SYNC_CONTEXT_ALREADY_REMOVED,"");
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
			GS_THROW_USER_ERROR(GS_ERROR_SYNC_CONTEXT_ALREADY_REMOVED, 
					"Backup syncId not found, syncid=" << backupSyncId_.dump());
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
		partitionRole_.restoreType();
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
	msgpack::unpacked msg;
	msgpack::unpack(&msg, dataBuffer, static_cast<size_t>(bodySize));
	msgpack::object obj = msg.get();
	obj.convert(&partitionRole_);
}

void SyncTimeoutInfo::decode(EventByteInStream &in, const char8_t *) {
	in >> ptRev_.addr_;
	in >> ptRev_.port_;
	in >> ptRev_.sequentialNumber_;
	in >> syncId_.contextId_;
	in >> syncId_.contextVersion_;
	int32_t tmpSyncMode;
	in >> tmpSyncMode;
	syncMode_ = static_cast<SyncMode>(tmpSyncMode);
}

void SyncTimeoutInfo::encode(EventByteOutStream &out) {
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
	out << globalSequentialNumber_;
	out << requestInfo_->syncId_.contextId_;
	out << requestInfo_->syncId_.contextVersion_;
	out << requestInfo_->backupSyncId_.contextId_;
	out << requestInfo_->backupSyncId_.contextVersion_;
	out << stmtId_;
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
	in >> globalSequentialNumber_;
	in >> requestInfo_->syncId_.contextId_;
	in >> requestInfo_->syncId_.contextVersion_;
	in >> requestInfo_->backupSyncId_.contextId_;
	in >> requestInfo_->backupSyncId_.contextVersion_;
	in >> stmtId_;
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
	out << stmtId_;
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
	in >> stmtId_;
	in >> targetLsn_;
}

bool SyncTimeoutInfo::check(PartitionId pId, PartitionTable *pt) {
	try {
		PartitionRole nextRole;
		pt->getPartitionRole(pId, nextRole, PartitionTable::PT_NEXT_OB);
		return (nextRole.isOwner() && syncMode_ == MODE_SHORTTERM_SYNC &&
				pt_->getPartitionStatus(pId) == PartitionTable::PT_SYNC);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Executes DropPartition
*/
void SyncHandler::removePartition(EventContext &ec, EventEngine::Event &ev) {
	PartitionId pId = ev.getPartitionId();
	EventType eventType = ev.getType();
	util::StackAllocator &alloc = ec.getAllocator();
	util::StackAllocator::Scope scope(alloc);
	const util::DateTime &now = ec.getHandlerStartTime();
	const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
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

std::string SyncManagerInfo::dump() {
	util::NormalOStringStream ss;
	ss << "syncManagerInfo:{";
	switch (eventType_) {
	case TXN_SHORTTERM_SYNC_REQUEST:
	case TXN_LONGTERM_SYNC_REQUEST:
	case TXN_SHORTTERM_SYNC_START:
	case TXN_LONGTERM_SYNC_START: {
		ss << "pId=" << pId_ << ", eventType:" << getEventTypeName(eventType_)
		<< ", syncId:" << syncId_.dump()
		<< ", backupSyncId:" << backupSyncId_.dump()
		<< ", role:" << partitionRole_.dump();
		break;
	}
	default: {
		ss << "pId=" << pId_ << ", eventType:" << getEventTypeName(eventType_)
		<< ", syncId:" << syncId_.dump()
		<< ", backupSyncId:" << backupSyncId_.dump()
		<< ", role:{revision:" << partitionRole_.getRevision().toString()
		<< "}";
		break;
	}
	}
	ss << "}";
	return ss.str();
}

std::string SyncRequestInfo::dump(int32_t mode) {
	util::NormalOStringStream ss;
	ss << "syncRequestInfo:{";
	if (mode == 1) {
		ss << getPartitionRole();
	}
	else {
		ss << request_.dump() << ", " << SyncManagerInfo::dump();
	}
	ss << "}";
	return ss.str();
}

std::string SyncRequestInfo::Request::dump() {
	util::NormalOStringStream ss;
	ss << "request:{";
	ss << "ptRev:" << ptRev_.toString() << ", stmtId:" << stmtId_
	<< ", ownerLsn:" << ownerLsn_ << ", startLsn:" << startLsn_
	<< ", endLsn:" << endLsn_ << ", logSize:" << binaryLogSize_
	<< ", chunkSize:" << chunkSize_ << ", chunkNum:" << numChunk_;
	ss << "}";
	return ss.str();
}

std::string SyncResponseInfo::dump() {
	util::NormalOStringStream ss;
	ss << "syncResponseInfo:{";
	ss << response_.dump() << ", " << SyncManagerInfo::dump();
	ss << "}";
	return ss.str();
}

std::string SyncResponseInfo::Response::dump() {
	util::NormalOStringStream ss;
	ss << "response:{";
	ss << "ptRev:" << ptRev_.toString() << ", stmtId:" << stmtId_
	<< ", targetLsn:" << targetLsn_;
	ss << "}";
	return ss.str();
}

std::string SyncTimeoutInfo::dump() {
	util::NormalOStringStream ss;
	ss << "SyncTimeoutInfo:{";
	ss << "pId=" << pId_ << ", ptRev:" << ptRev_.toString()
	<< ", syncId:" << syncId_.dump() << ", mode:" << syncMode_;
	ss << "}";
	return ss.str();
}

std::string DropPartitionInfo::dump() {
	util::NormalOStringStream ss;
	ss << "DropPartitionInfo:{";
	ss << "pId=" << pId_ << ", isForce:" << forceFlag_;
	ss << "}";
	return ss.str();
}

void ShortTermSyncHandler::undoPartition(util::StackAllocator &alloc,
		SyncContext *context, PartitionId pId) {
	if (context->getPartitionRevision().isInitialRevision()) {
		LogSequentialNumber prevLsn = logMgr_->getLSN(pId);
		const size_t undoNum = recoveryMgr_->undo(alloc, pId);
		if (undoNum > 0) {
			TRACE_CLUSTER_NORMAL_OPERATION(INFO, "Undo partition (pId=" << pId
					<< ", undoNum=" << undoNum << ", before LSN=" << prevLsn << ", after LSN="
					<< logMgr_->getLSN(pId) << ")");
		}
	}
	ds_->setUndoCompleted(pId);
}


