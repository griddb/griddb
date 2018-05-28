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

UTIL_TRACER_DECLARE(CLUSTER_SERVICE);
UTIL_TRACER_DECLARE(SYNC_SERVICE);
UTIL_TRACER_DECLARE(CLUSTER_OPERATION);
UTIL_TRACER_DECLARE(IO_MONITOR);
UTIL_TRACER_DECLARE(SYNC_DETAIL);

#define RM_THROW_LOG_REDO_ERROR(errorCode, message) \
	GS_THROW_CUSTOM_ERROR(LogRedoException, , errorCode, message)
#define RM_RETHROW_LOG_REDO_ERROR(cause, message) \
	GS_RETHROW_CUSTOM_ERROR(LogRedoException, GS_ERROR_DEFAULT, cause, message)

SyncService::SyncService(const ConfigTable &config,
		EventEngine::Config &eeConfig, EventEngine::Source source,
		const char8_t *name, SyncManager &syncMgr, ClusterVersionId versionId,
		ServiceThreadErrorHandler &serviceThreadErrorHandler) :
		ee_(createEEConfig(config, eeConfig), source, name),
		syncMgr_(&syncMgr), versionId_(versionId),
		serviceThreadErrorHandler_(serviceThreadErrorHandler),
		initailized_(false) {
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
				"Cluster message version is unmatched,  current: "
				<< versionId_ << ", target:" << versionId);
	}
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
		out << LogManager::getVersion();
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
	util::StackAllocator &alloc, PartitionId pId, bool isForce) {
	try {
		DropPartitionInfo dropPartitionInfo(
				alloc, TXN_DROP_PARTITION, syncMgr_, pId, isForce);
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
		EventContext &ec, PartitionId pId, LongtermSyncInfo *syncInfo,
		bool errorOccured) {
	clsSvc_->request(ec, TXN_LONGTERM_SYNC_PREPARE_ACK, pId, txnSvc_->getEE(), *syncInfo);
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
		clsMgr_->checkNodeStatus();
		clsMgr_->checkActiveStatus();
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);
		SyncVariableSizeAllocator &varSizeAlloc = syncMgr_->getVariableSizeAllocator();

		SyncRequestInfo syncRequestInfo(alloc, eventType, syncMgr_, pId,
				clsSvc_->getEE(), MODE_SHORTTERM_SYNC);
		SyncResponseInfo syncResponseInfo(alloc, eventType, syncMgr_, pId,
				MODE_SHORTTERM_SYNC, logMgr_->getLSN(pId));

		EventRequestOption sendOption;
		sendOption.timeoutMillis_ = EE_PRIORITY_HIGH;

		WATCHER_START;

		switch (eventType) {
		case TXN_SHORTTERM_SYNC_REQUEST: {
			syncSvc_->decode(alloc, ev, syncRequestInfo);
			syncMgr_->checkExecutable(OP_SHORTTERM_SYNC_REQUEST, pId,
					syncRequestInfo.getPartitionRole());

			PartitionRole &nextRole = syncRequestInfo.getPartitionRole();
			context = syncMgr_->createSyncContext(ec, pId, nextRole.getRevision(),
					MODE_SHORTTERM_SYNC, PartitionTable::PT_OWNER);
			if (context == NULL) {
				GS_THROW_USER_ERROR(GS_ERROR_SYNC_CREATE_CONTEXT_FAILED, "");
			}
			context->startAll();
			if (context->getSyncMode() == MODE_LONGTERM_SYNC) {
				GS_TRACE_WARNING(
						SYNC_DETAIL, GS_TRACE_SYNC_OPERATION, "SYNC_START, " << context->dump(0));
			}
			else {
				GS_TRACE_INFO(
						SYNC_DETAIL, GS_TRACE_SYNC_OPERATION, "SYNC_START, " << context->dump(0));
			}
			context->setDownNextOwner(syncRequestInfo.getIsDownNextOwner());
			
			try {
				addTimeoutEvent(ec, pId, alloc, MODE_SHORTTERM_SYNC, context);
			}
			catch (std::exception &e) {
				if (context != NULL) {
					syncMgr_->removeSyncContext(ec, pId, context);
				}
				GS_RETHROW_USER_ERROR(e, "");
			}

			PartitionRole currentRole;
			pt_->getPartitionRole(pId, currentRole);
			context->setPartitionStatus(pt_->getPartitionStatus(pId));
			clsSvc_->requestChangePartitionStatus(ec,
				alloc, pId, PartitionTable::PT_SYNC, PT_CHANGE_SYNC_START);
			pt_->setPartitionRole(pId, nextRole, PartitionTable::PT_NEXT_OB);
			SyncId dummySyncId;
			syncRequestInfo.set(TXN_SHORTTERM_SYNC_START, context,
					logMgr_->getLSN(pId), dummySyncId);

			std::vector<NodeId> downNodeList;
			for (NodeId nodeId = 1; nodeId < pt_->getNodeNum(); nodeId++) {
				if (pt_->getHeartbeatTimeout(nodeId) == UNDEF_TTL) {
					downNodeList.push_back(nodeId);
				}
			}
			if (downNodeList.size() > 0) {
				currentRole.remove(downNodeList);
				pt_->setPartitionRole(pId, currentRole);
			}

			util::Set<NodeId> syncTargetNodeSet(alloc);
			currentRole.getSyncTargetNodeId(syncTargetNodeSet);
			nextRole.getSyncTargetNodeId(syncTargetNodeSet);
			if (syncTargetNodeSet.size() > 0) {
				Event syncStartEvent(ec, TXN_SHORTTERM_SYNC_START, pId);
				syncSvc_->encode(syncStartEvent, syncRequestInfo);
				for (util::Set<NodeId>::iterator it = syncTargetNodeSet.begin();
						it != syncTargetNodeSet.end(); it++) {
					NodeId targetNodeId = *it;
					const NodeDescriptor &nd = txnEE_->getServerND(targetNodeId);
					if (!nd.isEmpty()) {
						txnEE_->send(syncStartEvent, nd, &sendOption);
					}
					else {
						GS_THROW_USER_ERROR(GS_ERROR_SYNC_INVALID_SENDER_ND, "");
					}
					context->incrementCounter(targetNodeId);
				}
			}
			else {
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
				syncMgr_->setRestored(pId);

				Event syncEndEvent(ec, TXN_SHORTTERM_SYNC_END_ACK, pId);
				syncResponseInfo.set(TXN_SHORTTERM_SYNC_END_ACK,
						syncRequestInfo, logMgr_->getLSN(pId), context);
				syncSvc_->encode(syncEndEvent, syncResponseInfo);
				txnEE_->addTimer(syncEndEvent, EE_PRIORITY_HIGH);
				context->incrementCounter(0);
			}
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
			if (context == NULL) {
				GS_THROW_USER_ERROR(GS_ERROR_SYNC_CREATE_CONTEXT_FAILED, "");
			}
			context->startAll();
			if (context->getSyncMode() == MODE_LONGTERM_SYNC) {
				GS_TRACE_WARNING(
						SYNC_DETAIL, GS_TRACE_SYNC_OPERATION, "SYNC_START, " << context->dump(0));
			}
			else {
				GS_TRACE_INFO(
						SYNC_DETAIL, GS_TRACE_SYNC_OPERATION, "SYNC_START, " << context->dump(0));
			}
			context->setRecvNodeId(senderNodeId);
			addTimeoutEvent(ec, pId, alloc, MODE_SHORTTERM_SYNC, context);

			Event syncStartAckEvent(ec, TXN_SHORTTERM_SYNC_START_ACK, pId);
			syncResponseInfo.set(TXN_SHORTTERM_SYNC_START_ACK, syncRequestInfo,
					logMgr_->getLSN(pId), context);
			syncSvc_->encode(syncStartAckEvent, syncResponseInfo);
			if (!senderNd.isEmpty()) {
				txnEE_->send(syncStartAckEvent, senderNd, &sendOption);
			}
			else {
				GS_THROW_USER_ERROR(GS_ERROR_SYNC_INVALID_SENDER_ND, "");
			}
			break;
		}
		case TXN_SHORTTERM_SYNC_START_ACK: {
			syncSvc_->decode(alloc, ev, syncResponseInfo);
			syncMgr_->checkExecutable(OP_SHORTTERM_SYNC_START_ACK, pId,
					syncResponseInfo.getPartitionRole());

			SyncId syncId = syncResponseInfo.getSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			if (context != NULL) {
				LogSequentialNumber backupLsn = syncResponseInfo.getTargetLsn();
				LogSequentialNumber ownerLsn = logMgr_->getLSN(pId);
				context->setSyncTargetLsnWithSyncId(senderNodeId, backupLsn,
						syncResponseInfo.getBackupSyncId());
				LogSequentialNumber maxLsn = pt_->getMaxLsn(pId);
				
				if (ownerLsn >= backupLsn && (ownerLsn >= maxLsn)) {
					if (context->decrementCounter(senderNodeId)) {
						context->resetCounter();
						if (context->getPartitionRevision().isInitialRevision()) {
							LogSequentialNumber prevLsn = logMgr_->getLSN(pId);
							const size_t undoNum = recoveryMgr_->undo(alloc, pId);
							if (undoNum > 0) {
								TRACE_CLUSTER_NORMAL_OPERATION(INFO, "Undo partition (pId=" << pId
										<< ", undoNum=" << undoNum << ", before LSN=" << prevLsn << ", after LSN="
										<< logMgr_->getLSN(pId) << ")");
							}
						}
						ownerLsn = logMgr_->getLSN(pId);
						ds_->setUndoCompleted(pId);
						syncMgr_->setRestored(pId);

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
								addTimeoutEvent(ec, pId, alloc, MODE_SHORTTERM_SYNC, context, true);
								return;
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
									UTIL_TRACE_WARNING(SYNC_SERVICE, 
											"Short term log sync failed, log is not found or invalid, pId="
											<< pId << ", lsn=" << backupLsn << ", nd=" << senderNd);
									addTimeoutEvent(ec, pId, alloc, MODE_SHORTTERM_SYNC, context, true);
									return;
								}
							}
						}

						for (pos = 0; pos < backups.size(); pos++) {
							LogSequentialNumber backupLsn = 0;
							SyncId backupSyncId;
							NodeId targetNodeId = backups[pos];
							if (!context->getSyncTargetLsnWithSyncId(
									targetNodeId, backupLsn, backupSyncId)) {
								continue;
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
									std::vector<NodeId> removeNodeIdList;
									removeNodeIdList.push_back(targetNodeId);
									UTIL_TRACE_WARNING(SYNC_SERVICE, 
											"Short term log sync failed, log is not found or invalid, pId=" 
											<< pId << ", lsn=" << backupLsn << ", nd=" << senderNd);
									pt_->updatePartitionRole(pId, removeNodeIdList, PartitionTable::PT_NEXT_OB);
									clsSvc_->requestGossip(ec, alloc, pId, targetNodeId, GOSSIP_INVALID_NODE);
									continue;
								}
							}
							else {
								syncRequestInfo.setSearchLsnRange(ownerLsn, ownerLsn);
							}
							Event syncLogEvent(ec, SYC_SHORTTERM_SYNC_LOG, pId);
							syncSvc_->encode(syncLogEvent, syncRequestInfo);
							const NodeDescriptor &nd = syncEE_->getServerND(backups[pos]);
							if (!nd.isEmpty()) {
								syncEE_->send(syncLogEvent, nd, &sendOption);
								sendCount++;
							}
							else {
								GS_THROW_USER_ERROR(GS_ERROR_SYNC_INVALID_SENDER_ND, "");
							}
						}
						if (sendCount == 0) {
							Event syncEndEvent(ec, TXN_SHORTTERM_SYNC_END_ACK, pId);
							syncResponseInfo.set(TXN_SHORTTERM_SYNC_END_ACK,
									syncRequestInfo, logMgr_->getLSN(pId), context);
							syncSvc_->encode(syncEndEvent, syncResponseInfo);
							txnEE_->addTimer(syncEndEvent, EE_PRIORITY_HIGH);
						}
					}
				}
				else {
					TRACE_CLUSTER_NORMAL_OPERATION(INFO, 
							"Backup LSN is larger than owner LSN (pId=" << pId << ", owner=" 
							<< pt_->dumpNodeAddress(0) << ", owner LSN=" << ownerLsn 
							<< ", backup=" << pt_->dumpNodeAddress(senderNodeId)
							<< ", backup LSN=" << backupLsn << ")");
					clsSvc_->requestChangePartitionStatus(ec, alloc, pId,
						 context->getPartitionStatus(), PT_CHANGE_SYNC_END);
					clsSvc_->requestGossip(ec, alloc, pId, 0, GOSSIP_INVALID_NODE);
					syncMgr_->removeSyncContext(ec, pId, context);
				}
			}
			break;
		}
		case TXN_SHORTTERM_SYNC_LOG: {
			syncSvc_->decode(alloc, ev, syncRequestInfo);
			syncMgr_->checkExecutable(
					OP_SHORTTERM_SYNC_LOG, pId, syncRequestInfo.getPartitionRole());

			ds_->setUndoCompleted(pId);
			syncMgr_->setRestored(pId);

			SyncId syncId = syncRequestInfo.getBackupSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
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

				Event syncLogAckEvent(ec, TXN_SHORTTERM_SYNC_LOG_ACK, pId);
				syncResponseInfo.set(TXN_SHORTTERM_SYNC_LOG_ACK,
						syncRequestInfo, logMgr_->getLSN(pId), context);
				syncSvc_->encode(syncLogAckEvent, syncResponseInfo);
				const NodeDescriptor &nd = txnEE_->getServerND(senderNodeId);
				if (!nd.isEmpty()) {
					txnEE_->send(syncLogAckEvent, nd, &sendOption);
				}
				else {
					GS_THROW_USER_ERROR(GS_ERROR_SYNC_INVALID_SENDER_ND, "");
				}
			}
			break;
		}
		case TXN_SHORTTERM_SYNC_LOG_ACK: {
			syncSvc_->decode(alloc, ev, syncResponseInfo);
			syncMgr_->checkExecutable(OP_SHORTTERM_SYNC_LOG_ACK, pId,
					syncResponseInfo.getPartitionRole());

			SyncId syncId = syncResponseInfo.getSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
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
								continue;
							}
							syncRequestInfo.clearSearchCondition();
							syncRequestInfo.set(
									TXN_SHORTTERM_SYNC_END, context, ownerLsn, backupSyncId);
							Event syncEndEvent(ec, TXN_SHORTTERM_SYNC_END, pId);
							syncSvc_->encode(syncEndEvent, syncRequestInfo);
							const NodeDescriptor &nd = txnEE_->getServerND(backups[pos]);
							if (!nd.isEmpty()) {
								txnEE_->send(syncEndEvent, nd, &sendOption);
							}
							else {
								GS_THROW_USER_ERROR(GS_ERROR_SYNC_INVALID_SENDER_ND, "");
							}
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
						UTIL_TRACE_WARNING(SYNC_SERVICE,
							"Short term log sync failed, log is not found or invalid, pId="
							<< pId << ", lsn=" << backupLsn << ", nd=" << senderNd);
						addTimeoutEvent(ec, pId, alloc, MODE_SHORTTERM_SYNC, context, true);
						return;
					}
					Event syncLogEvent(ec, SYC_SHORTTERM_SYNC_LOG, pId);
					syncRequestInfo.set(SYC_SHORTTERM_SYNC_LOG, context,
							ownerLsn, syncResponseInfo.getBackupSyncId());
					syncSvc_->encode(syncLogEvent, syncRequestInfo);
					const NodeDescriptor &nd = syncEE_->getServerND(senderNodeId);
					if (nd.isEmpty()) {
						GS_THROW_USER_ERROR(GS_ERROR_SYNC_INVALID_SENDER_ND, "");
					}
					syncEE_->send(syncLogEvent, nd, control.getOption());
				}
			}
			break;
		}
		case TXN_SHORTTERM_SYNC_END: {
			syncSvc_->decode(alloc, ev, syncRequestInfo);
			syncMgr_->checkExecutable(
				OP_SHORTTERM_SYNC_END, pId, syncRequestInfo.getPartitionRole());

			SyncId syncId = syncRequestInfo.getBackupSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
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
				txnEE_->send(syncEndAckEvent, senderNd, &sendOption);
				context->endAll();
				syncMgr_->removeSyncContext(ec, pId, context);
			}
			break;
		}
		case TXN_SHORTTERM_SYNC_END_ACK: {
			syncSvc_->decode(alloc, ev, syncResponseInfo);
			syncMgr_->checkExecutable(OP_SHORTTERM_SYNC_END_ACK, pId,
					syncResponseInfo.getPartitionRole());

			SyncId syncId = syncResponseInfo.getSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			if (context != NULL) {
				if (pt_->getLSN(pId) != syncResponseInfo.getTargetLsn()) {
					GS_THROW_USER_ERROR(GS_ERROR_SYNC_OWNER_BACKUP_INVALID_RELATION, "");
				}
				context->setSyncTargetLsn(senderNodeId, syncResponseInfo.getTargetLsn());
				if (context->decrementCounter(senderNodeId)) {
					pt_->updatePartitionRole(pId);
					PartitionStatus nextStatus = PartitionTable::PT_ON;
					PartitionRoleStatus nextRoleStatus =
						PartitionTable::PT_NONE;
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
					pt_->setPartitionRoleStatus(pId, nextRoleStatus);
					context->endAll();
					syncMgr_->removeSyncContext(ec, pId, context);

					if (!pt_->isMaster()) {
						PartitionRole currentRole;
						pt_->getPartitionRole(pId, currentRole);
						ClusterManager::ChangePartitionTableInfo
								changePartitionTableInfo(alloc, 0, currentRole);
						Event requestEvent(ec, TXN_CHANGE_PARTITION_TABLE, pId);
						clsSvc_->encode(requestEvent, changePartitionTableInfo);
						NodeId master = pt_->getMaster();
						if (master != UNDEF_NODEID) {
							const NodeDescriptor &nd = txnEE_->getServerND(master);
							txnEE_->send(requestEvent, nd);
						}
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
		TRACE_SYNC_EXCEPTION(
			e, eventType, pId, WARNING, "Short term sync operation failed.");
		switch (eventType) {
			case TXN_SHORTTERM_SYNC_START:
			case TXN_SHORTTERM_SYNC_LOG:
			case TXN_SHORTTERM_SYNC_END: {
				if (context != NULL) {
					syncMgr_->removeSyncContext(ec, pId, context);
				}
				break;
			}
		}
	}
	catch (LockConflictException &e) {
		TRACE_SYNC_EXCEPTION(e, eventType, pId, WARNING,
				"Short term sync failed, lockConflict is occurred.");
	}
	catch (LogRedoException &e) {
		TRACE_SYNC_EXCEPTION(e, eventType, pId, WARNING,
				"Short term sync failed, log redo exception is occurred.");
		if (context != NULL) {
			syncMgr_->removeSyncContext(ec, pId, context);
		}
		{
			util::StackAllocator &alloc = ec.getAllocator();
			util::StackAllocator::Scope scope(alloc);
			clsSvc_->requestGossip(ec, alloc, pId, 0, GOSSIP_INVALID_PARTITION);
		}
	}
	catch (std::exception &e) {
		if (context != NULL) {
			syncMgr_->removeSyncContext(ec, pId, context);
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
	const NodeDescriptor &senderNd = ev.getSenderND();
	NodeId senderNodeId = ClusterService::resolveSenderND(ev);
	try {
		clsMgr_->checkNodeStatus();
		clsMgr_->checkActiveStatus();
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
			if (context == NULL) {
				GS_THROW_USER_ERROR(GS_ERROR_SYNC_CREATE_CONTEXT_FAILED, "");
			}
			context->startAll();
			if (context->getSyncMode() == MODE_LONGTERM_SYNC) {
				GS_TRACE_WARNING(
						SYNC_DETAIL, GS_TRACE_SYNC_OPERATION, "SYNC_START, " << context->dump(0));
			}
			else {
				GS_TRACE_INFO(
						SYNC_DETAIL, GS_TRACE_SYNC_OPERATION, "SYNC_START, " << context->dump(0));
			}
			addTimeoutEvent(ec, pId, alloc, MODE_LONGTERM_SYNC, context);
			pt_->setPartitionRole(pId, nextRole, PartitionTable::PT_NEXT_GOAL);
			pt_->setPartitionRole(pId, nextRole, PartitionTable::PT_CURRENT_GOAL);
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
				GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
			}
			PartitionRole &nextRole = syncRequestInfo.getPartitionRole();
			pt_->getPartitionRole(pId, nextRole, PartitionTable::PT_NEXT_GOAL);
			syncRequestInfo.set(TXN_LONGTERM_SYNC_START, context,
					logMgr_->getLSN(pId), syncResponseInfo.getBackupSyncId());
			std::vector<NodeId> &catchups = nextRole.getBackups();
			if (catchups.size() > 0) {
				Event syncStartEvent(ec, TXN_LONGTERM_SYNC_START, pId);
				syncSvc_->encode(syncStartEvent, syncRequestInfo);
				for (size_t pos = 0; pos < catchups.size(); pos++) {
					const NodeDescriptor &nd = txnEE_->getServerND(catchups[pos]);
					if (!nd.isEmpty()) {
						txnEE_->send(syncStartEvent, nd);
					}
					else {
						GS_THROW_USER_ERROR(GS_ERROR_SYNC_INVALID_SENDER_ND, "");
					}
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
			pt_->setPartitionRole(pId, nextCatchup, PartitionTable::PT_NEXT_GOAL);

			pt_->setPartitionRole(pId, nextCatchup, PartitionTable::PT_CURRENT_GOAL);
			context = syncMgr_->createSyncContext(ec, pId, nextCatchup.getRevision(),
					MODE_LONGTERM_SYNC, PartitionTable::PT_CATCHUP);
			if (context == NULL) {
				GS_THROW_USER_ERROR(GS_ERROR_SYNC_CREATE_CONTEXT_FAILED, "");
			}
			context->startAll();

			if (context->getSyncMode() == MODE_LONGTERM_SYNC) {
				GS_TRACE_WARNING(
						SYNC_DETAIL, GS_TRACE_SYNC_OPERATION, "SYNC_START, " << context->dump(0));
			}
			else {
				GS_TRACE_INFO(
						SYNC_DETAIL, GS_TRACE_SYNC_OPERATION, "SYNC_START, " << context->dump(0));
			}
			addTimeoutEvent(ec, pId, alloc, MODE_LONGTERM_SYNC, context);

			Event syncStartAckEvent(ec, TXN_LONGTERM_SYNC_START_ACK, pId);
			syncResponseInfo.set(TXN_LONGTERM_SYNC_START_ACK, syncRequestInfo,
					logMgr_->getLSN(pId), context);
			syncSvc_->encode(syncStartAckEvent, syncResponseInfo);
			if (!senderNd.isEmpty()) {
				txnEE_->send(syncStartAckEvent, senderNd);
			}
			else {
				GS_THROW_USER_ERROR(GS_ERROR_SYNC_INVALID_SENDER_ND, "");
			}
			break;
		}
		case TXN_LONGTERM_SYNC_START_ACK: {
			syncSvc_->decode(alloc, ev, syncResponseInfo);
			SyncStat &stat = clsMgr_->getSyncStat();
			stat.init(true);
			syncMgr_->checkExecutable(OP_LONGTERM_SYNC_START_ACK, pId,
					syncResponseInfo.getPartitionRole());
			SyncId syncId = syncResponseInfo.getSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			const NodeDescriptor &nd = syncEE_->getServerND(senderNodeId);
			if (nd.isEmpty()) {
				GS_THROW_USER_ERROR(GS_ERROR_SYNC_INVALID_SENDER_ND, "");
			}
			if (context != NULL) {
				LogSequentialNumber ownerLsn = logMgr_->getLSN(pId);
				LogSequentialNumber catchupLsn = syncResponseInfo.getTargetLsn();
				context->setSyncTargetLsn(senderNodeId, catchupLsn);
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
							clsSvc_->requestGossip(ec, alloc, pId, senderNodeId, GOSSIP_INVALID_CATHCUP_PARTITION);
							GS_THROW_USER_ERROR(GS_ERROR_SYNC_LOG_NOT_FOUND, "");
						}
					}
					Event syncLogEvent(ec, SYC_LONGTERM_SYNC_LOG, pId);
					syncSvc_->encode(syncLogEvent, syncRequestInfo);
					syncEE_->send(syncLogEvent, nd);
					break;
				}
				else {
//					const PartitionGroupId pgId = pt_->getPartitionGroupId(pId);
					const CheckpointId cpId = cpSvc_->getCurrentCheckpointId(pId);
					syncRequestInfo.set(SYC_LONGTERM_SYNC_CHUNK);

					int32_t chunkNum;
					try {
						chunkNum = static_cast<int32_t>(chunkMgr_->startSync(cpId, pId));
					}
					catch (std::exception &e) {
						clsSvc_->requestGossip(ec, alloc, pId, senderNodeId, GOSSIP_INVALID_CATHCUP_PARTITION);
						GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e,
								"Long term chunk sync failed, start sync"));
					}
					SyncStat &stat = clsMgr_->getSyncStat();
					stat.syncChunkNum_ = chunkNum;
					if (chunkNum == 0) {
						clsSvc_->requestGossip(ec, alloc, pId, senderNodeId, GOSSIP_INVALID_CATHCUP_PARTITION);
						GS_THROW_USER_ERROR(GS_ERROR_SYNC_NO_CHUNK_GET_FAILED, "");
					}
					context->setChunkInfo(cpId, chunkNum);

					int32_t sendChunkNum = syncMgr_->getConfig().getSendChunkNum();
					int32_t sendChunkSize = syncMgr_->getChunkSize();
					syncRequestInfo.setSearchChunkCondition(
							sendChunkNum, sendChunkSize, chunkNum);
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
								context->getSequentialNumber())) {
							context->setSyncCheckpointCompleted();
						}
					}
					catch (std::exception &e) {
						UTIL_TRACE_EXCEPTION(SYNC_SERVICE, e, "");
						context->endChunk(chunkWatch);
						clsSvc_->requestGossip(ec, alloc, pId, senderNodeId, GOSSIP_INVALID_CATHCUP_PARTITION);
						GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e,
								"Long term chunk sync failed, get chunk"));
					}
					context->endChunk(chunkWatch);
					Event syncChunkEvent(ec, SYC_LONGTERM_SYNC_CHUNK, pId);
					syncSvc_->encode(syncChunkEvent, syncRequestInfo);
					syncEE_->send(syncChunkEvent, nd);
					int32_t prevCount = context->getProcessedChunkNum() %
							syncMgr_->getExtraConfig().getLongtermDumpChunkInterval();
					context->incProcessedChunkNum(sendChunkNum);
					if ((prevCount + sendChunkNum) >= syncMgr_->getExtraConfig()
								.getLongtermDumpChunkInterval()) {
			GS_TRACE_INFO(SYNC_DETAIL, GS_TRACE_SYNC_OPERATION,
					"Long term chunk sync (pId=" << pId
						<< ", catchup=" << pt_->dumpNodeAddress(senderNodeId)
						<< ", send chunks(" << context->getProcessedChunkNum() << "/"
						<< context->getTotalChunkNum() << "))");
					}
				}
			}
			break;
		}
		case TXN_LONGTERM_SYNC_CHUNK: {
			syncSvc_->decode(alloc, ev, syncRequestInfo);
			syncMgr_->checkExecutable(OP_LONGTERM_SYNC_CHUNK, pId,
					syncRequestInfo.getPartitionRole());
			SyncId syncId = syncRequestInfo.getBackupSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			if (context != NULL) {
				if (context->getProcessedChunkNum() == 0) {
					removePartition(ec, ev);
					util::XArray<uint8_t> binaryLogList(alloc);
					logMgr_->putDropPartitionLog(binaryLogList, pId);
				}
				int32_t chunkNum = 0;
				int32_t chunkSize = 0;
				context->getChunkInfo(chunkNum, chunkSize);
				int32_t revisionNo = syncRequestInfo.getPartitionRole().getRevisionNo();
				util::Stopwatch chunkWatch;
				context->start(chunkWatch);
				for (int32_t chunkNo = 0; chunkNo < chunkNum; chunkNo++) {
					uint8_t *chunkBuffer = NULL;
					context->getChunkBuffer(chunkBuffer, chunkNo);
					if (chunkBuffer != NULL) {
						if (syncMgr_->getRecoveryLevel() >= 1) {
							if (context->getProcessedChunkNum() == 0) {
								util::XArray<uint8_t> binaryLogBuf(alloc);
								logMgr_->putChunkStartLog(
										binaryLogBuf, pId, revisionNo, context->getTotalChunkNum());
							}
						}

						try {
							chunkMgr_->recoveryChunk(pId, chunkBuffer, chunkSize);
						}
						catch (std::exception &e) {
							context->endChunk(chunkWatch);
							clsSvc_->requestGossip(ec, alloc, pId, senderNodeId, GOSSIP_INVALID_CATHCUP_PARTITION);
							GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e,
									"Long term chunk sync failed, recovery chunk is failed"));
						}
						if (syncMgr_->getRecoveryLevel() >= 1) {
							util::XArray<uint8_t> binaryLogBuf(alloc);
							logMgr_->putChunkDataLog(
									binaryLogBuf, pId, revisionNo, chunkSize, chunkBuffer);
						}
						context->incProcessedChunkNum();
						if ((context->getProcessedChunkNum() %
								syncMgr_->getExtraConfig().getLongtermDumpChunkInterval()) == 0) {
							GS_TRACE_INFO(SYNC_DETAIL, GS_TRACE_SYNC_OPERATION,
									"Long term chunk sync (pId=" << pId << ", owner="
									<< pt_->dumpNodeAddress(senderNodeId) << ", processed chunks("
									<< context->getProcessedChunkNum() << "/"
									<< context->getTotalChunkNum() << "))");
						}
						SyncStat &stat = clsMgr_->getSyncStat();
						stat.syncApplyNum_++;
					}
				}
				context->endChunk(chunkWatch);
				context->freeBuffer(varSizeAlloc, CHUNK_SYNC, syncMgr_->getSyncOptStat());
				uint8_t *logBuffer = NULL;
				int32_t logBufferSize = 0;
				context->getLogBuffer(logBuffer, logBufferSize);
				if (logBuffer != NULL && logBufferSize > 0) {
					if (!context->isRecvCompleted()) {
						GS_THROW_USER_ERROR(GS_ERROR_SYNC_INVALID_APPLY_CHUNK,
							"invalid recv chunk, pId="
								<< pId << ", processed chunks("
								<< context->getProcessedChunkNum() << "/"
								<< context->getTotalChunkNum() << ")");
					}
//					LogSequentialNumber startLsn
//							= 
					recoveryMgr_->restoreTransaction(pId, logBuffer, ec.getHandlerStartMonotonicTime());
					context->freeBuffer(varSizeAlloc, LOG_SYNC, syncMgr_->getSyncOptStat());
					clsSvc_->requestUpdateStartLsn(ec, ec.getAllocator(), pId, pId + 1);
					const util::DateTime &now = ec.getHandlerStartTime();
					const EventMonotonicTime emNow = ec.getHandlerStartMonotonicTime();
					const TransactionManager::ContextSource src(eventType);
					TransactionContext &txn = txnMgr_->put(
							alloc, pId, TXN_EMPTY_CLIENTID, src, now, emNow);
					ds_->restartPartition(txn, clsSvc_);
					if (syncMgr_->getRecoveryLevel() >= 1) {
						util::XArray<uint8_t> binaryLogBuf(alloc);
						logMgr_->putChunkEndLog(binaryLogBuf, pId, revisionNo);
					}
				}
				context->endChunkAll();
				Event syncChunkAckEvent(ec, TXN_LONGTERM_SYNC_CHUNK_ACK, pId);
				syncResponseInfo.set(TXN_LONGTERM_SYNC_CHUNK_ACK,
						syncRequestInfo, logMgr_->getLSN(pId), context);
				syncSvc_->encode(syncChunkAckEvent, syncResponseInfo);
				NodeId ownerNodeId = pt_->getOwner(pId, PartitionTable::PT_NEXT_GOAL);
				const NodeDescriptor &nd = txnEE_->getServerND(ownerNodeId);
				if (!nd.isEmpty()) {
					txnEE_->send(syncChunkAckEvent, nd);
				}
				else {
					GS_THROW_USER_ERROR(GS_ERROR_SYNC_INVALID_SENDER_ND, "");
				}
			}
			break;
		}
		case TXN_LONGTERM_SYNC_CHUNK_ACK: {
			syncSvc_->decode(alloc, ev, syncResponseInfo);
			syncMgr_->checkExecutable(OP_LONGTERM_SYNC_CHUNK_ACK, pId,
				syncResponseInfo.getPartitionRole());
			SyncId syncId = syncResponseInfo.getSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			if (context != NULL) {
				LogSequentialNumber ownerLsn = logMgr_->getLSN(pId);
				LogSequentialNumber catchupLsn =
					syncResponseInfo.getTargetLsn();
				SyncControlContext control(eventType, pId, catchupLsn);
				if (!controlSyncLoad(control)) {
					txnEE_->addTimer(ev, control.getWaitTime());
					return;
				}
				const NodeDescriptor &nd = syncEE_->getServerND(senderNodeId);
				if (nd.isEmpty()) {
					GS_THROW_USER_ERROR(GS_ERROR_SYNC_INVALID_SENDER_ND, "");
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
						}
						catch (UserException &e) {
						context->endLog(logWatch);
							UTIL_TRACE_EXCEPTION(SYNC_SERVICE, e, "");
							clsSvc_->requestGossip(ec, alloc, pId, senderNodeId, GOSSIP_INVALID_CATHCUP_PARTITION);
							GS_THROW_USER_ERROR(GS_ERROR_SYNC_LOG_NOT_FOUND, "");
						}
					}
					else {
						syncRequestInfo.setSearchLsnRange(ownerLsn, ownerLsn);
					}
					Event syncLogEvent(ec, SYC_LONGTERM_SYNC_LOG, pId);
					syncSvc_->encode(syncLogEvent, syncRequestInfo);
					syncEE_->send(syncLogEvent, nd, control.getOption());
				}
				else {
					const PartitionGroupId pgId = pt_->getPartitionGroupId(pId);
					const CheckpointId cpId = cpSvc_->getCurrentCheckpointId(pId);
					if (context->getSyncCheckpointId() != cpId) {
						clsSvc_->requestGossip(ec, alloc, pId, senderNodeId, GOSSIP_INVALID_CATHCUP_PARTITION);
						GS_THROW_USER_ERROR(GS_ERROR_SYNC_CHUNK_GET_FAILED,
								"Long term chunk sync failed, new checkpoint is running, pId="
								<< pId << ", cpId=" << cpId << ", pgId=" << pgId << ", send chunks("
								<< context->getProcessedChunkNum() << "/"
								<< context->getTotalChunkNum() << ")");
					}
					syncRequestInfo.set(SYC_LONGTERM_SYNC_CHUNK, context, logMgr_->getLSN(pId),
							syncResponseInfo.getBackupSyncId());
					int32_t sendChunkNum = syncMgr_->getConfig().getSendChunkNum();
					int32_t sendChunkSize = syncMgr_->getChunkSize();
					syncRequestInfo.setSearchChunkCondition(sendChunkNum,
							sendChunkSize, context->getTotalChunkNum());
					const util::DateTime &now = ec.getHandlerStartTime();
					const EventMonotonicTime emNow =
						ec.getHandlerStartMonotonicTime();
					const TransactionManager::ContextSource src(eventType);
					TransactionContext &txn = txnMgr_->put(
							alloc, pId, TXN_EMPTY_CLIENTID, src, now, emNow);
					DataStore::Latch latch(txn, pId, ds_, clsSvc_);
					util::Stopwatch chunkWatch;
					context->start(chunkWatch);
					try {
						if (syncRequestInfo.getChunks(alloc, pId, pt_, logMgr_, chunkMgr_, cpSvc_,
								context->getSequentialNumber())) {
							context->setSyncCheckpointCompleted();
						}
						context->endLog(chunkWatch);
					}
					catch (std::exception &e) {
						UTIL_TRACE_EXCEPTION(SYNC_SERVICE, e, "");

						context->endLog(chunkWatch);
						GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e,
								"Long term chunk sync failed, get chunk is failed"));
					}
					Event syncChunkEvent(ec, SYC_LONGTERM_SYNC_CHUNK, pId);
					syncSvc_->encode(syncChunkEvent, syncRequestInfo);
					syncEE_->send(syncChunkEvent, nd, control.getOption());
					context->incProcessedChunkNum(sendChunkNum);
					if ((context->getProcessedChunkNum() %
							syncMgr_->getExtraConfig().getLongtermDumpChunkInterval()) == 0) {
						GS_TRACE_INFO(SYNC_DETAIL, GS_TRACE_SYNC_OPERATION,
								"Long term chunk sync (pId=" << pId << ", catchup="
								<< pt_->dumpNodeAddress(senderNodeId) << ", send chunks("
								<< context->getProcessedChunkNum() << "/"
								<< context->getTotalChunkNum() << "))");
					}
					break;
				}
			}
			break;
		}
		case TXN_LONGTERM_SYNC_LOG: {
			syncSvc_->decode(alloc, ev, syncRequestInfo);
			syncMgr_->checkExecutable(
					OP_LONGTERM_SYNC_LOG, pId, syncRequestInfo.getPartitionRole());
			SyncId syncId = syncRequestInfo.getBackupSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
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

					if (!context->getNotifyLongSync() && (syncRequestInfo.getLsn() -  pt_->getLSN(pId)) <= 10000) 
					{
						NodeId master = pt_->getMaster();
						if (master != UNDEF_NODEID && master > 0) {
							ClusterManager::HeartbeatResInfo heartbeatResInfo(alloc, master);
							Event requestEvent(ec, CS_HEARTBEAT_RES, CS_HANDLER_PARTITION_ID);
							heartbeatResInfo.add(pId, pt_->getPartitionStatus(pId),
								pt_->getLSN(pId), pt_->getPartitionRoleStatus(pId));
							clsSvc_->encode(requestEvent, heartbeatResInfo);
							const NodeDescriptor &nd = clsSvc_->getEE()->getServerND(master);
							if (!nd.isEmpty()) {
								clsSvc_->getEE()->send(requestEvent, nd);
								context->setNotifyLongSync();
							}
						}
					}

				Event syncLogAckEvent(ec, TXN_LONGTERM_SYNC_LOG_ACK, pId);
				syncResponseInfo.set(TXN_LONGTERM_SYNC_LOG_ACK, syncRequestInfo,
					logMgr_->getLSN(pId), context);
				syncSvc_->encode(syncLogAckEvent, syncResponseInfo);
				NodeId ownerNodeId = pt_->getOwner(pId, PartitionTable::PT_NEXT_GOAL);
				const NodeDescriptor &nd = txnEE_->getServerND(ownerNodeId);
				if (!nd.isEmpty()) {
					txnEE_->send(syncLogAckEvent, nd);
				}
				else {
					GS_THROW_USER_ERROR(GS_ERROR_SYNC_INVALID_SENDER_ND, "");
				}
			}
			break;
		}
		case TXN_LONGTERM_SYNC_LOG_ACK: {
			syncSvc_->decode(alloc, ev, syncResponseInfo);
			syncMgr_->checkExecutable(OP_LONGTERM_SYNC_LOG_ACK, pId,
					syncResponseInfo.getPartitionRole());
			SyncId syncId = syncResponseInfo.getSyncId();
			context = syncMgr_->getSyncContext(pId, syncId);
			if (context != NULL) {
				LogSequentialNumber ownerLsn = logMgr_->getLSN(pId);
				LogSequentialNumber catchupLsn = syncResponseInfo.getTargetLsn();
				SyncControlContext control(eventType, pId, catchupLsn);
				if (!controlSyncLoad(control)) {
					txnEE_->addTimer(ev, control.getWaitTime());
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
						TRACE_CLUSTER_NORMAL_OPERATION(
								INFO, "Long term log sync failed (Log is not found, pId=" << pId
								<< ", LSN=" << catchupLsn << ", node=" << senderNd << ")");
						GS_THROW_USER_ERROR(GS_ERROR_SYNC_LOG_NOT_FOUND, "");
					}
					Event syncLogEvent(ec, SYC_LONGTERM_SYNC_LOG, pId);
					syncRequestInfo.set(SYC_LONGTERM_SYNC_LOG, context,
							ownerLsn, syncResponseInfo.getBackupSyncId());
					syncSvc_->encode(syncLogEvent, syncRequestInfo);
					syncEE_->send(syncLogEvent, nd, control.getOption());
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
			syncMgr_->removeSyncContext(ec, pId, context);
			pt_->clear(pId, PartitionTable::PT_NEXT_GOAL);
			pt_->clear(pId, PartitionTable::PT_CURRENT_GOAL);
		}
	}
	catch (LockConflictException &e) {
		TRACE_SYNC_EXCEPTION(e, eventType, pId, WARNING,
				"Long term sync failed, lock confict exception is occurred, retry:"
				<< syncMgr_->getExtraConfig().getLockConflictPendingInterval());
		txnEE_->addTimer(
			ev, syncMgr_->getExtraConfig().getLockConflictPendingInterval());
	}
	catch (LogRedoException &e) {
		TRACE_SYNC_EXCEPTION(e, eventType, pId, WARNING,
				"Long term sync failed, log redo exception is occurred."
				<< syncMgr_->getExtraConfig().getLockConflictPendingInterval());
		if (context != NULL) {
			syncMgr_->removeSyncContext(ec, pId, context);
			pt_->clear(pId, PartitionTable::PT_NEXT_GOAL);
			pt_->clear(pId, PartitionTable::PT_CURRENT_GOAL);
		}
		{
			util::StackAllocator &alloc = ec.getAllocator();
			util::StackAllocator::Scope scope(alloc);
			clsSvc_->requestGossip(ec, alloc, pId, 0, GOSSIP_INVALID_CATHCUP_PARTITION);
		}
	}
	catch (std::exception &e) {
		if (context != NULL) {
			syncMgr_->removeSyncContext(ec, pId, context);
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
					GS_ERROR_SYNC_LOG_NOT_FOUND, "Log serch failed, pId=" << pId);
			}
		}
		else {
			if (!logMgr->findLog(cursor, pId, request_.startLsn_)) {
				GS_THROW_USER_ERROR(
					GS_ERROR_SYNC_LOG_NOT_FOUND, "Log serch failed, pId=" << pId);
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
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Gets Chunks
*/
bool SyncRequestInfo::getChunks(util::StackAllocator &alloc, PartitionId pId, PartitionTable *pt,
		LogManager *logMgr, ChunkManager *chunkMgr, CheckpointService *cpSvc, int64_t sId) {
	bool lastChunkGet = false;
	int32_t callLogCount = 0;
	try {
		chunks_.clear();
		binaryLogRecords_.clear();
		uint32_t chunkSize = condition_.chunkSize_;
		request_.numChunk_ = 0;
		request_.binaryLogSize_ = 0;
//		const PartitionGroupId pgId = pt->getPartitionGroupId(pId);
		for (uint32_t pos = 0;pos < condition_.chunkNum_;pos++) {
			uint8_t *chunk = ALLOC_NEW(alloc) uint8_t [chunkSize];
			lastChunkGet = cpSvc->getLongSyncChunk(sId, chunkSize, chunk);
			chunks_.push_back(chunk);
			request_.numChunk_++;
			if (lastChunkGet) break;
		}
		if (lastChunkGet) {
			LogCursor cursor;
			if (!cpSvc->getLongSyncCheckpointStartLog(sId, cursor)) {
				GS_THROW_USER_ERROR(GS_ERROR_SYNC_CHUNK_GET_FAILED, "");
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
		clsMgr_->checkNodeStatus();
		syncMgr_->checkExecutable(
			OP_SYNC_TIMEOUT, pId, syncTimeoutInfo.getPartitionRole());
		if (context != NULL) {
			PartitionRole currentRole, nextRole;
			pt_->getPartitionRole(
				pId, currentRole, PartitionTable::PT_CURRENT_OB);
			pt_->getPartitionRole(pId, nextRole, PartitionTable::PT_NEXT_OB);
			NodeId currentOwner = currentRole.getOwner();
			NodeId nextOwner = nextRole.getOwner();
			if (pt_->getPartitionStatus(pId) == PartitionTable::PT_SYNC) {
				bool isOwner = false;
				if (currentOwner == 0) {
					currentRole.clearBackup();
					pt_->setPartitionRole(pId, currentRole, PartitionTable::PT_CURRENT_OB);
					clsSvc_->requestChangePartitionStatus(ec,
						alloc, pId, PartitionTable::PT_ON, PT_CHANGE_SYNC_END);
					isOwner = true;
					pt_->setPartitionRoleStatus(pId, PartitionTable::PT_OWNER);
				}
				else if (currentOwner == UNDEF_NODEID && nextOwner == 0) {
					nextRole.clearBackup();
					pt_->setPartitionRole(pId, nextRole, PartitionTable::PT_CURRENT_OB);
					clsSvc_->requestChangePartitionStatus(ec,
						alloc, pId, PartitionTable::PT_ON, PT_CHANGE_SYNC_END);
					isOwner = true;
					pt_->setPartitionRoleStatus(pId, PartitionTable::PT_OWNER);
				}
				else {
					clsSvc_->requestChangePartitionStatus(ec,
						alloc, pId, PartitionTable::PT_OFF, PT_CHANGE_SYNC_END);
					pt_->setPartitionRoleStatus(pId, PartitionTable::PT_NONE);
				}
				if (isOwner && !pt_->isMaster()) {
					PartitionRole currentRole;
					pt_->getPartitionRole(pId, currentRole);
					ClusterManager::ChangePartitionTableInfo
							changePartitionTableInfo(alloc, 0, currentRole);
					Event requestEvent(ec, TXN_CHANGE_PARTITION_TABLE, pId);
					clsSvc_->encode(requestEvent, changePartitionTableInfo);
					NodeId master = pt_->getMaster();
					if (master != UNDEF_NODEID) {
						const NodeDescriptor &nd = txnEE_->getServerND(master);
						txnEE_->send(requestEvent, nd);
					}
				}
			}
			syncMgr_->removeSyncContext(ec, pId, context);
		}
	}
	catch (UserException &e) {
		UTIL_TRACE_EXCEPTION_INFO(SYNC_SERVICE, e, "");
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);
		clsSvc_->requestChangePartitionStatus(ec,
			alloc, pId, PartitionTable::PT_OFF, PT_CHANGE_SYNC_END);
		if (context) {
		syncMgr_->removeSyncContext(ec, pId, context);
	}
	}
	catch (std::exception &e) {
		if (context) {
		syncMgr_->removeSyncContext(ec, pId, context);
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
			syncMgr_->checkExecutable(
				OP_DROP_PARTITION, pId, dropPartitionInfo.getPartitionRole());
			if (pt_->getLSN(pId) == 0 && !chunkMgr_->existPartition(pId)) {
				GS_THROW_USER_ERROR(GS_ERROR_SYM_INVALID_PARTITION_INFO, "");
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
			TRACE_CLUSTER_NORMAL_OPERATION(
					INFO,"Drop partition completed (pId=" << pId << ", LSN=" << prevLsn << ")");
		}
		if (!pt_->isMaster()) {
			pt_->clear(pId, PartitionTable::PT_NEXT_OB);
			pt_->clear(pId, PartitionTable::PT_NEXT_GOAL);
			pt_->clear(pId, PartitionTable::PT_CURRENT_OB);
			pt_->clear(pId, PartitionTable::PT_CURRENT_GOAL);
		}
		clsSvc_->requestChangePartitionStatus(ec,
			alloc, pId, PartitionTable::PT_OFF, PT_CHANGE_NORMAL);
	}
	catch (UserException &e) {
		UTIL_TRACE_EXCEPTION_DEBUG(SYNC_SERVICE, e, "");
	}
	catch (LockConflictException &e) {
		UTIL_TRACE_EXCEPTION(SYNC_SERVICE, e, 
			"Pending drop partition, reason=" << GS_EXCEPTION_MESSAGE(e));
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
			GS_THROW_USER_ERROR(GS_ERROR_SYNC_CONTEXT_ALREADY_REMOVED, "");
		}
		syncRequestInfo.setEventType(txnEventType);
		Event requestEvent(ec, txnEventType, pId);
		syncSvc_->encode(requestEvent, syncRequestInfo);
		SyncControlContext control(txnEventType, pId, syncRequestInfo.getLsn());
		controlSyncLoad(control);
		if (control.getWaitTime() > 0) {
			txnEE_->addTimer(requestEvent, control.getWaitTime());
		}
		else {
			txnEE_->addTimer(requestEvent, 0);
		}
		WATCHER_END_SYNC(getEventTypeName(syncEventType), pId);
	}
	catch (UserException &e) {
		TRACE_SYNC_EXCEPTION(
				e, syncEventType, pId, WARNING, "Recv sync is failed.");
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
			GS_THROW_USER_ERROR(GS_ERROR_SYNC_CONTEXT_ALREADY_REMOVED, "");
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
		context->setTotalChunkNum(request_.totalChunkNum_);
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
		std::vector<NodeAddress> backupsAddress;
		NodeId owner;
		std::vector<NodeId> backups;
		partitionRole_.get(ownerAddress, backupsAddress);
		owner = ClusterService::changeNodeId(ownerAddress, ee_);
		for (size_t pos = 0; pos < backupsAddress.size(); pos++) {
			NodeId nodeId = ClusterService::changeNodeId(backupsAddress[pos], ee_);
			if (nodeId != UNDEF_NODEID) {
				backups.push_back(nodeId);
			}
		}
		partitionRole_.set(owner, backups);
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
			GS_ERROR_SYNC_SERVICE_ENCODE_MESSAGE_FAILED, "pId:" << pId_);
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
	out << totalChunkNum_;
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
	in >> totalChunkNum_;
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
		ss << "pId:" << pId_ << ", eventType:" << getEventTypeName(eventType_)
		<< ", syncId:" << syncId_.dump()
		<< ", backupSyncId:" << backupSyncId_.dump()
		<< ", role:" << partitionRole_.dump();
		break;
	}
	default: {
		ss << "pId:" << pId_ << ", eventType:" << getEventTypeName(eventType_)
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
	ss << "pId:" << pId_ << ", ptRev:" << ptRev_.toString()
	<< ", syncId:" << syncId_.dump() << ", mode:" << syncMode_;
	ss << "}";
	return ss.str();
}

std::string DropPartitionInfo::dump() {
	util::NormalOStringStream ss;
	ss << "DropPartitionInfo:{";
	ss << "pId:" << pId_ << ", isForce:" << forceFlag_;
	ss << "}";
	return ss.str();
}
