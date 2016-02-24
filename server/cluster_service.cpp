/*
	Copyright (c) 2012 TOSHIBA CORPORATION.

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
	@brief Implementation of ClusterService
*/
#include "util/allocator.h"
#include "util/container.h"
#include "util/trace.h"
#include "cluster_common.h"
#include "cluster_event_type.h"
#include "gs_error.h"

#include "checkpoint_service.h"
#include "cluster_service.h"
#include "event_engine.h"
#include "sync_service.h"
#include "system_service.h"
#include "transaction_service.h"
#include "trigger_service.h"


#ifndef _WIN32
#define SYSTEM_CAPTURE_SIGNAL
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>
#endif

UTIL_TRACER_DECLARE(CLUSTER_SERVICE);
UTIL_TRACER_DECLARE(SYNC_SERVICE);
UTIL_TRACER_DECLARE(CLUSTER_OPERATION);


/*!
	@brief Gets node id of sender ND
*/
NodeId ClusterService::resolveSenderND(Event &ev) {
	const NodeDescriptor &senderNd = ev.getSenderND();
	if (!senderNd.isEmpty()) {
		return static_cast<NodeId>(ev.getSenderND().getId());
	}
	else {
		return 0;
	}
}

ClusterService::ClusterService(const ConfigTable &config,
	EventEngine::Config &eeconfig, EventEngine::Source source,
	const char8_t *name, ClusterManager &clsMgr, ClusterVersionId versionId,
	ServiceThreadErrorHandler &serviceThreadErrorHandler)
	: serviceThreadErrorHandler_(serviceThreadErrorHandler),
	  ee_(createEEConfig(config, eeconfig), source, name),
	  versionId_(versionId),
	  fixedSizeAlloc_(source.fixedAllocator_),
	  clsMgr_(&clsMgr),
	  txnSvc_(NULL),
	  syncSvc_(NULL),
	  sysSvc_(NULL),
	  cpSvc_(NULL),
	  clusterStats_(config.get<int32_t>(CONFIG_TABLE_DS_PARTITION_NUM)),
	  pt_(NULL),
	  initailized_(false),
	  isSystemServiceError_(false) {
	try {
		clsMgr_ = &clsMgr;


		ee_.setHandler(CS_HEARTBEAT, heartbeatHandler_);
		ee_.setHandlingMode(CS_HEARTBEAT, EventEngine::HANDLING_IMMEDIATE);
		ee_.setHandler(CS_HEARTBEAT_RES, heartbeatHandler_);
		ee_.setHandlingMode(CS_HEARTBEAT_RES, EventEngine::HANDLING_IMMEDIATE);

		ee_.setHandler(CS_NOTIFY_CLUSTER, notifyClusterHandler_);
		ee_.setHandlingMode(CS_NOTIFY_CLUSTER, EventEngine::HANDLING_IMMEDIATE);
		ee_.setHandler(CS_NOTIFY_CLUSTER_RES, notifyClusterHandler_);
		ee_.setHandlingMode(
			CS_NOTIFY_CLUSTER_RES, EventEngine::HANDLING_IMMEDIATE);

		ee_.setHandler(CS_UPDATE_PARTITION, updatePartitionHandler_);

		ee_.setHandler(CS_GOSSIP, gossipHandler_);

		ee_.setHandler(CS_JOIN_CLUSTER, systemCommandHandler_);
		ee_.setHandler(CS_LEAVE_CLUSTER, systemCommandHandler_);
		ee_.setHandler(CS_SHUTDOWN_NODE_NORMAL, systemCommandHandler_);
		ee_.setHandler(CS_SHUTDOWN_NODE_FORCE, systemCommandHandler_);
		ee_.setHandler(CS_SHUTDOWN_CLUSTER, systemCommandHandler_);
		ee_.setHandler(
			CS_COMPLETE_CHECKPOINT_FOR_SHUTDOWN, systemCommandHandler_);
		ee_.setHandler(
			CS_COMPLETE_CHECKPOINT_FOR_RECOVERY, systemCommandHandler_);

		ee_.setHandler(CS_ORDER_DROP_PARTITION, orderDropPartitionHandler_);


		ee_.setHandler(CS_TIMER_CHECK_CLUSTER, timerCheckClusterHandler_);
		Event checkClusterEvent(
			source, CS_TIMER_CHECK_CLUSTER, CS_HANDLER_PARTITION_ID);
		ee_.addPeriodicTimer(
			checkClusterEvent, clsMgr_->getConfig().getHeartbeatInterval());

		ee_.setHandler(CS_TIMER_NOTIFY_CLUSTER, timerNotifyClusterHandler_);
		Event notifyClusterEvent(
			source, CS_TIMER_NOTIFY_CLUSTER, CS_HANDLER_PARTITION_ID);
		ee_.addPeriodicTimer(notifyClusterEvent,
			clsMgr_->getConfig().getNotifyClusterInterval());

		ee_.setHandler(CS_TIMER_NOTIFY_CLIENT, timerNotifyClientHandler_);
		Event notifyClientEvent(
			source, CS_TIMER_NOTIFY_CLIENT, CS_HANDLER_PARTITION_ID);
		ee_.addPeriodicTimer(
			notifyClientEvent, clsMgr_->getConfig().getNotifyClusterInterval());

		ee_.setHandler(
			CS_TIMER_CHECK_LOAD_BALANCE, timerCheckLoadBalanceHandler_);
		Event checkLoadbalanceEvent(
			source, CS_TIMER_CHECK_LOAD_BALANCE, CS_HANDLER_PARTITION_ID);
		ee_.addPeriodicTimer(checkLoadbalanceEvent,
			clsMgr_->getConfig().getCheckLoadBalanceInterval());

		ee_.setUnknownEventHandler(unknownEventHandler_);
		ee_.setThreadErrorHandler(serviceThreadErrorHandler_);

	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Sets EventEngine config
*/
EventEngine::Config &ClusterService::createEEConfig(
	const ConfigTable &config, EventEngine::Config &eeConfig) {
	EventEngine::Config tmpConfig;
	eeConfig = tmpConfig;

	eeConfig.setServerNDAutoNumbering(true);

	eeConfig.setPartitionCount(config.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM));

	eeConfig.setServerAddress(
		config.get<const char8_t *>(CONFIG_TABLE_CS_SERVICE_ADDRESS),
		config.getUInt16(CONFIG_TABLE_CS_SERVICE_PORT));

	eeConfig.setMulticastAddress(
		config.get<const char8_t *>(CONFIG_TABLE_CS_NOTIFICATION_ADDRESS),
		config.getUInt16(CONFIG_TABLE_CS_NOTIFICATION_PORT));

	eeConfig.setAllAllocatorGroup(ALLOCATOR_GROUP_CS);

	return eeConfig;
}

ClusterService::~ClusterService() {
	ee_.shutdown();
	ee_.waitForShutdown();
}

void ClusterService::initialize(ManagerSet &mgrSet) {
	try {
		txnSvc_ = mgrSet.txnSvc_;
		syncSvc_ = mgrSet.syncSvc_;
		cpSvc_ = mgrSet.cpSvc_;
		sysSvc_ = mgrSet.sysSvc_;

		pt_ = mgrSet.pt_;
		ConfigTable *config = mgrSet.config_;
		varSizeAlloc_ = mgrSet.varSizeAlloc_;

		mgrSet.stats_->addUpdator(&clsMgr_->getStatUpdator());

		heartbeatHandler_.initialize(mgrSet);
		notifyClusterHandler_.initialize(mgrSet);
		updatePartitionHandler_.initialize(mgrSet);
		gossipHandler_.initialize(mgrSet);
		systemCommandHandler_.initialize(mgrSet);
		timerCheckClusterHandler_.initialize(mgrSet);
		timerNotifyClusterHandler_.initialize(mgrSet);
		timerNotifyClientHandler_.initialize(mgrSet);
		timerCheckLoadBalanceHandler_.initialize(mgrSet);
		orderDropPartitionHandler_.initialize(mgrSet);
		unknownEventHandler_.initialize(mgrSet);
		serviceThreadErrorHandler_.initialize(mgrSet);




		NodeAddress clusterAddress, txnAddress, syncAddress;
		ClusterService::getAddress(clusterAddress, 0, getEE(CLUSTER_SERVICE));
		ClusterService::getAddress(txnAddress, 0, getEE(TRANSACTION_SERVICE));
		ClusterService::getAddress(syncAddress, 0, getEE(SYNC_SERVICE));

		EventEngine::Config tmp;
		tmp.setServerAddress(
			config->get<const char8_t *>(CONFIG_TABLE_SYS_SERVICE_ADDRESS),
			config->getUInt16(CONFIG_TABLE_SYS_SERVICE_PORT));

		util::SocketAddress::Inet sysAddr;
		uint16_t sysPort;
		tmp.serverAddress_.getIP(&sysAddr, &sysPort);

		AddressType sysAddrType;
		memcpy(&sysAddrType, &sysAddr, sizeof(sysAddrType));

		NodeAddress systemAddress(sysAddrType, sysPort);

		pt_->setNodeInfo(0, CLUSTER_SERVICE, clusterAddress);
		pt_->setNodeInfo(0, TRANSACTION_SERVICE, txnAddress);
		pt_->setNodeInfo(0, SYNC_SERVICE, syncAddress);
		pt_->setNodeInfo(0, SYSTEM_SERVICE, systemAddress);

		pt_->getPartitionRevision().set(
			clusterAddress.address_, clusterAddress.port_);

		initailized_ = true;

		UTIL_TRACE_WARNING(
			CLUSTER_SERVICE, "Self node info:{" << pt_->dumpNodes() << "}");
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Starts ClusterService
*/
void ClusterService::start() {
	try {
		if (!initailized_) {
			GS_THROW_USER_ERROR(GS_ERROR_CS_SERVICE_NOT_INITIALIZED, "");
		}
		ee_.start();
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Shutdown ClusterService
*/
void ClusterService::shutdown() {
	ee_.shutdown();
}

/*!
	@brief Waits for shutdown ClusterService
*/
void ClusterService::waitForShutdown() {
	ee_.waitForShutdown();
}

/*!
	@brief Encodes massage for cluster service
*/
template <class T>
void ClusterService::encode(Event &ev, T &t) {
	try {
		EventByteOutStream out = ev.getOutStream();

		out << versionId_;
		msgpack::sbuffer buffer;

		try {
			msgpack::pack(buffer, t);
		}
		catch (std::exception &e) {
			GS_RETHROW_USER_ERROR(
				e, GS_EXCEPTION_MERGE_MESSAGE(e, "Failed to encode message"));
		}

		uint32_t packedSize = static_cast<uint32_t>(buffer.size());
		if (packedSize == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_CS_ENCODE_DECODE_VALIDATION_CHECK,
				"Invalid message size(0)");
		}


		out << packedSize;
		out.writeAll(buffer.data(), packedSize);

		TRACE_CLUSTER_HANDLER(ev.getType(), INFO, "size:" << packedSize);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
}
template void ClusterService::encode(Event &ev, DropPartitionInfo &t);

/*!
	@brief Checks the version of massage for cluster service
*/
void ClusterService::checkVersion(uint8_t versionId) {
	if (versionId != versionId_) {
		GS_THROW_USER_ERROR(GS_ERROR_CS_ENCODE_DECODE_VERSION_CHECK,
			"Cluster message version is unmatched, acceptableClustertVersion:"
				<< static_cast<int32_t>(versionId_)
				<< ", connectClusterVersion:"
				<< static_cast<int32_t>(versionId));
	}
}

/*!
	@brief Decodes massage for cluster service
*/
template <class T>
void ClusterService::decode(Event &ev, T &t) {
	try {
		EventByteInStream in = ev.getInStream();
		const uint8_t *eventBuffer = ev.getMessageBuffer().getXArray().data() +
									 ev.getMessageBuffer().getOffset();

		if (eventBuffer == NULL) {
			GS_THROW_USER_ERROR(GS_ERROR_CS_SERVICE_DECODE_MESSAGE_FAILED,
				"Event initial buffer is null");
		}
		uint8_t decodedVersionId;
		in >> decodedVersionId;
		checkVersion(decodedVersionId);

		uint32_t packedSize;
		in >> packedSize;

		TRACE_CLUSTER_HANDLER(ev.getType(), INFO, "size:" << packedSize);

		const char8_t *bodyBuffer = reinterpret_cast<const char8_t *>(
			eventBuffer + in.base().position());
		if (bodyBuffer == NULL) {
			GS_THROW_USER_ERROR(GS_ERROR_CS_SERVICE_DECODE_MESSAGE_FAILED,
				"Event body buffer is null");
		}

		uint32_t remainSize = static_cast<int32_t>(in.base().remaining());
		if (remainSize != packedSize) {
			GS_THROW_USER_ERROR(GS_ERROR_CS_SERVICE_DECODE_MESSAGE_FAILED,
				"Remained:" << remainSize << ", packed:" << packedSize);
		}

		try {
			msgpack::unpacked msg;
			msgpack::unpack(&msg, bodyBuffer, remainSize);
			msgpack::object obj = msg.get();
			obj.convert(&t);
		}
		catch (std::exception &e) {
			GS_RETHROW_USER_ERROR(
				e, GS_EXCEPTION_MERGE_MESSAGE(e, "Failed to decode message"));
		}

		t.check();
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
}

template void ClusterService::decode(Event &ev, DropPartitionInfo &t);

/*!
	@brief Handles system error
*/
void ClusterService::setError(
	const Event::Source &eventSource, std::exception *e) {
	try {
		if (e != NULL) {
			UTIL_TRACE_EXCEPTION_INFO(CLUSTER_OPERATION, *e,
				GS_EXCEPTION_MERGE_MESSAGE(*e, "[ERROR] SetError requested"));
		}

		if (!clsMgr_->setSystemError()) {
			UTIL_TRACE_WARNING(
				CLUSTER_SERVICE, "Already reported system error.");
			return;
		}

		cpSvc_->shutdown();
		txnSvc_->shutdown();
		syncSvc_->shutdown();

		{
			util::StackAllocator alloc(
				util::AllocatorInfo(ALLOCATOR_GROUP_MAIN, "setError"),
				fixedSizeAlloc_);
			requestGossip(eventSource, alloc);
		}

		for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
			pt_->setPartitionStatus(pId, PartitionTable::PT_STOP);
		}

		clsMgr_->updateClusterStatus(TO_SUBMASTER, true);
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(CLUSTER_OPERATION, e,
			"[CRITICAL] Unrecoveble error is occured, abort process.");
		abort();
	}
}

/*!
	@brief Requests Gossip
*/
void ClusterService::requestGossip(const Event::Source &eventSource,
	util::StackAllocator &alloc, PartitionId pId, NodeId nodeId,
	GossipType gossipType) {
	try {
		if (gossipType != GOSSIP_NORMAL && gossipType != GOSSIP_GOAL_SELF) {
			return;
		}

		ClusterManager::GossipInfo gossipInfo(alloc);
		gossipInfo.setTarget(pId, nodeId, gossipType);

		clsMgr_->get(gossipInfo);

		if (gossipInfo.isValid()) {
			Event gossipEvent(eventSource, CS_GOSSIP, CS_HANDLER_PARTITION_ID);
			encode(gossipEvent, gossipInfo);

			NodeId master = pt_->getMaster();
			if (master != UNDEF_NODEID) {
				const NodeDescriptor &nd = ee_.getServerND(master);
				if (!nd.isEmpty()) {
					if (master == 0) {
						ee_.add(gossipEvent);
						TRACE_CLUSTER_EE_ADD(CS_GOSSIP, WARNING, "");
					}
					else {
						ee_.send(gossipEvent, nd);
						TRACE_CLUSTER_EE_SEND(CS_GOSSIP, nd, WARNING, "");
					}
				}
				else {
					GS_THROW_USER_ERROR(GS_ERROR_CS_INVALID_SENDER_ND, "");
				}
			}
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Requests ChangePartitionStatus
*/
void ClusterService::requestChangePartitionStatus(EventContext &ec,
	util::StackAllocator &alloc, PartitionId pId, PartitionStatus status) {
	try {
		Event requestEvent(ec, TXN_CHANGE_PARTITION_STATE, pId);
		ClusterManager::ChangePartitionStatusInfo changePartitionStatusInfo(
			alloc, 0, pId, status, true, PT_CHANGE_NORMAL);
		encode(requestEvent, changePartitionStatusInfo);
		TRACE_CLUSTER_HANDLER(
			TXN_CHANGE_PARTITION_STATE, INFO, changePartitionStatusInfo.dump());
		txnSvc_->getEE()->addTimer(requestEvent, EE_PRIORITY_HIGH);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Executes ChangePartitionStatus directly in the same thread
*/
void ClusterService::requestChangePartitionStatus(util::StackAllocator &alloc,
	PartitionId pId, PartitionStatus status,
	ChangePartitionType changePartitionType) {
	try {
		ClusterManager::ChangePartitionStatusInfo changePartitionStatusInfo(
			alloc, 0, pId, status, false, changePartitionType);
		TRACE_CLUSTER_HANDLER(
			TXN_CHANGE_PARTITION_STATE, INFO, changePartitionStatusInfo.dump());
		txnSvc_->changeTimeoutCheckMode(pId, status, changePartitionType,
			changePartitionStatusInfo.isToSubMaster(), getStats());
		clsMgr_->set(changePartitionStatusInfo);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}



/*!
	@brief Requests UpdateStartLSN
*/
void ClusterService::requestUpdateStartLsn(const Event::Source &eventSource,
	util::StackAllocator &alloc, PartitionId startPId, PartitionId endPId) {
	EventType eventType = CS_HEARTBEAT_RES;

	try {
		util::StackAllocator::Scope scope(alloc);

		if (startPId > endPId) {
			return;
		}
		NodeId master = pt_->getMaster();
		if (master != UNDEF_NODEID && master > 0) {
			ClusterManager::HeartbeatResInfo heartbeatResInfo(alloc, master, 1);
			for (PartitionId pId = startPId; pId < endPId; pId++) {
				heartbeatResInfo.add(pId, pt_->getStartLSN(pId));
			}
			Event requestEvent(eventSource, eventType, CS_HANDLER_PARTITION_ID);
			encode(requestEvent, heartbeatResInfo);
			TRACE_CLUSTER_HANDLER(
				CS_HEARTBEAT_RES, WARNING, heartbeatResInfo.dump());
			const NodeDescriptor &nd = ee_.getServerND(master);
			if (!nd.isEmpty()) {
				ee_.send(requestEvent, nd);
				TRACE_CLUSTER_EE_SEND(CS_HEARTBEAT_RES, nd, INFO, "");
			}
		}
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION_WARNING(CLUSTER_SERVICE, e, "");
	}
}

/*!
	@brief Requests checkpoint completed
*/
void ClusterService::requestCompleteCheckpoint(const Event::Source &eventSource,
	util::StackAllocator &alloc, bool isRecovery) {
	try {
		util::StackAllocator::Scope scope(alloc);

		ClusterManager::CompleteCheckpointInfo completeCpInfo(alloc, 0);

		EventType eventType;
		if (isRecovery) {
			eventType = CS_COMPLETE_CHECKPOINT_FOR_RECOVERY;
		}
		else {
			eventType = CS_COMPLETE_CHECKPOINT_FOR_SHUTDOWN;
		}
		request(eventSource, eventType, CS_HANDLER_PARTITION_ID, &ee_,
			completeCpInfo);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(
				   e, "Failed to request checkpoint completion: isRecovery="
						  << isRecovery));
	}
}

/*!
	@brief Encodes massage for client
*/
void ClusterService::encodeNotifyClient(Event &ev) {
	try {
		EventByteOutStream out = ev.getOutStream();
		const uint8_t hashType = 0;
		util::SocketAddress::Inet address;
		uint16_t port;
		const NodeDescriptor &nd =
			getEE(TRANSACTION_SERVICE)->getSelfServerND();
		if (nd.isEmpty()) {
			GS_THROW_USER_ERROR(GS_ERROR_CS_INVALID_SENDER_ND, "");
		}
		nd.getAddress().getIP(&address, &port);
		out.writeAll(&address, sizeof(address));
		out << static_cast<uint32_t>(port);
		out << pt_->getPartitionNum();
		out << hashType;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Template function for request event
*/
template <class T>
void ClusterService::request(const Event::Source &eventSource,
	EventType eventType, PartitionId pId, EventEngine *targetEE, T &t) {
	try {
		Event requestEvent(eventSource, eventType, pId);
		encode(requestEvent, t);

		TRACE_CLUSTER_HANDLER(eventType, INFO, t.dump());

		targetEE->add(requestEvent);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

template void ClusterService::request(const Event::Source &eventSource,
	EventType eventType, PartitionId pId, EventEngine *targetEE,
	ClusterManager::JoinClusterInfo &t);

template void ClusterService::request(const Event::Source &eventSource,
	EventType eventType, PartitionId pId, EventEngine *targetEE,
	ClusterManager::LeaveClusterInfo &t);


template void ClusterService::request(const Event::Source &eventSource,
	EventType eventType, PartitionId pId, EventEngine *targetEE,
	ClusterManager::ShutdownNodeInfo &t);

template void ClusterService::request(const Event::Source &eventSource,
	EventType eventType, PartitionId pId, EventEngine *targetEE,
	ClusterManager::ShutdownClusterInfo &t);


/*!
	@brief Handler Operator
*/
void HeartbeatHandler::operator()(EventContext &ec, Event &ev) {
	EventType eventType = ev.getType();
	TRACE_CLUSTER_HANDLER_DETAIL(ev, eventType, DEBUG, "");

	try {
		clsMgr_->checkNodeStatus();

		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);

		NodeId senderNodeId = ClusterService::resolveSenderND(ev);
		checkAutoNdNumbering(ev.getSenderND());

		switch (eventType) {
		case CS_HEARTBEAT: {
			try {
				clsMgr_->checkClusterStatus(OP_HEARTBEAT);
			}
			catch (UserException &e) {
				TRACE_CLUSTER_EXCEPTION(e, eventType, DEBUG, "");
				return;
			}

			ClusterManager::HeartbeatInfo heartbeatInfo(
				alloc, senderNodeId, pt_);
			clsSvc_->decode(ev, heartbeatInfo);

			TRACE_CLUSTER_HANDLER(eventType, INFO, heartbeatInfo.dump());

			updateNodeList(eventType, heartbeatInfo.getNodeAddressList());

			clsMgr_->set(heartbeatInfo);
			bool isInitialCluster = heartbeatInfo.isIntialCluster();

			if (heartbeatInfo.isStatusChange()) {
				for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
					clsSvc_->requestChangePartitionStatus(
						ec, alloc, pId, PartitionTable::PT_OFF);
				}
				clsMgr_->updateClusterStatus(TO_SUBMASTER);
			}

			ClusterManager::HeartbeatResInfo heartbeatResInfo(
				alloc, senderNodeId);
			clsMgr_->get(heartbeatResInfo, isInitialCluster);

			Event requestEvent(ec, CS_HEARTBEAT_RES, CS_HANDLER_PARTITION_ID);
			clsSvc_->encode(requestEvent, heartbeatResInfo);

			TRACE_CLUSTER_HANDLER(
				CS_HEARTBEAT_RES, INFO, heartbeatResInfo.dump());

			const NodeDescriptor &nd = ev.getSenderND();
			if (!nd.isEmpty()) {
				clsEE_->send(requestEvent, nd);
				TRACE_CLUSTER_EE_SEND(CS_HEARTBEAT_RES, nd, INFO, "");
			}
			else {
				GS_THROW_USER_ERROR(GS_ERROR_CS_INVALID_SENDER_ND, "");
			}

			TRACE_CLUSTER_HANDLER(eventType, DEBUG, "");

			break;
		}

		case CS_HEARTBEAT_RES: {
			clsMgr_->checkClusterStatus(OP_HEARTBEAT_RES);

			ClusterManager::HeartbeatResInfo heartbeatResInfo(
				alloc, senderNodeId);
			clsSvc_->decode(ev, heartbeatResInfo);

			TRACE_CLUSTER_HANDLER_DETAIL(
				ev, eventType, INFO, heartbeatResInfo.dump());

			clsMgr_->set(heartbeatResInfo);

			TRACE_CLUSTER_HANDLER(eventType, DEBUG, "");

			break;
		}
		}
	}
	catch (UserException &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, WARNING, "");
	}
	catch (std::exception &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, ERROR, "");
		clsSvc_->setError(ec, &e);
	}
}

/*!
	@brief Handler Operator
*/
void NotifyClusterHandler::operator()(EventContext &ec, Event &ev) {
	EventType eventType = ev.getType();
	TRACE_CLUSTER_HANDLER_DETAIL(ev, eventType, DEBUG, "");

	try {
		clsMgr_->checkNodeStatus();

		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);
		NodeId senderNodeId = ClusterService::resolveSenderND(ev);
		checkAutoNdNumbering(ev.getSenderND());

		switch (eventType) {
		case CS_NOTIFY_CLUSTER: {
			if (senderNodeId == 0) {
				return;
			}

			try {
				clsMgr_->checkClusterStatus(OP_NOTIFY_CLUSTER);
			}
			catch (UserException &e) {
				TRACE_CLUSTER_EXCEPTION(e, eventType, DEBUG, "");
				return;
			}

			ClusterManager::NotifyClusterInfo recvNotifyClusterInfo(
				alloc, senderNodeId, pt_);
			clsSvc_->decode(ev, recvNotifyClusterInfo);

			TRACE_CLUSTER_HANDLER(
				eventType, INFO, recvNotifyClusterInfo.dump());

			if (!clsMgr_->set(recvNotifyClusterInfo)) {
				return;
			}

			std::vector<AddressInfo> &addressInfoList =
				recvNotifyClusterInfo.getNodeAddressInfoList();

			updateNodeList(eventType, addressInfoList);

			if (recvNotifyClusterInfo.isFollow()) {
				ClusterManager::NotifyClusterResInfo notifyClusterResInfo(
					alloc, senderNodeId, pt_);
				clsMgr_->get(notifyClusterResInfo);

				Event requestEvent(
					ec, CS_NOTIFY_CLUSTER_RES, CS_HANDLER_PARTITION_ID);
				clsSvc_->encode(requestEvent, notifyClusterResInfo);

				TRACE_CLUSTER_HANDLER(
					CS_NOTIFY_CLUSTER_RES, INFO, notifyClusterResInfo.dump());

				const NodeDescriptor &nd = ev.getSenderND();
				if (!nd.isEmpty()) {
					clsEE_->send(requestEvent, nd);
					TRACE_CLUSTER_EE_SEND(CS_NOTIFY_CLUSTER_RES, nd, INFO, "");
				}
				else {
					GS_THROW_USER_ERROR(GS_ERROR_CS_INVALID_SENDER_ND, "");
				}
			}

			TRACE_CLUSTER_HANDLER(eventType, DEBUG, "");

			break;
		}

		case CS_NOTIFY_CLUSTER_RES: {
			clsMgr_->checkClusterStatus(OP_NOTIFY_CLUSTER_RES);

			ClusterManager::NotifyClusterResInfo notifyClusterResInfo(
				alloc, senderNodeId, pt_);
			clsSvc_->decode(ev, notifyClusterResInfo);

			TRACE_CLUSTER_HANDLER(eventType, INFO, notifyClusterResInfo.dump());

			std::vector<AddressInfo> &addressInfoList =
				notifyClusterResInfo.getNodeAddressInfoList();
			updateNodeList(eventType, addressInfoList);

			clsMgr_->set(notifyClusterResInfo);

			TRACE_CLUSTER_HANDLER(eventType, DEBUG, "");

			break;
		}
		}
	}
	catch (UserException &e) {
		int32_t errorCode = e.getErrorCode(1);
		if (errorCode == 0) {
			errorCode = e.getErrorCode();
		}
		if (errorCode == GS_ERROR_CS_ENCODE_DECODE_VERSION_CHECK) {
			if (clsMgr_->reportError(errorCode)) {
				TRACE_CLUSTER_EXCEPTION(e, eventType, ERROR,
					"Connected cluster node is invalid, address:"
						<< ev.getSenderND());
			}
			else {
				TRACE_CLUSTER_EXCEPTION(e, eventType, WARNING,
					"Connected cluster node is invalid, address:"
						<< ev.getSenderND());
			}
		}
		else {
			TRACE_CLUSTER_EXCEPTION(e, eventType, WARNING,
				"Connected cluster node is invalid, address:"
					<< ev.getSenderND());
		}
	}
	catch (std::exception &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, ERROR, "");
		clsSvc_->setError(ec, &e);
	}
}

/*!
	@brief Handler Operator
*/
void SystemCommandHandler::operator()(EventContext &ec, Event &ev) {
	EventType eventType = ev.getType();
	TRACE_CLUSTER_HANDLER_DETAIL(ev, eventType, DEBUG, "");

	try {
		if (eventType != CS_SHUTDOWN_NODE_FORCE) {
			clsMgr_->checkNodeStatus();
		}

		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);

		NodeId senderNodeId = ClusterService::resolveSenderND(ev);

		switch (eventType) {
		case CS_JOIN_CLUSTER: {
			clsMgr_->checkClusterStatus(OP_JOIN_CLUSTER);

			clsMgr_->checkCommandStatus(OP_JOIN_CLUSTER);

			ClusterManager::JoinClusterInfo joinClusterInfo(
				alloc, senderNodeId);
			clsSvc_->decode(ev, joinClusterInfo);

			TRACE_CLUSTER_HANDLER(eventType, WARNING, joinClusterInfo.dump());

			clsMgr_->set(joinClusterInfo);

			for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
				clsSvc_->requestChangePartitionStatus(
					ec, alloc, pId, PartitionTable::PT_OFF);
			}

			clsMgr_->updateNodeStatus(OP_JOIN_CLUSTER);

			TRACE_CLUSTER_HANDLER(eventType, DEBUG, "");

			break;
		}

		case CS_LEAVE_CLUSTER: {
			clsMgr_->checkClusterStatus(OP_LEAVE_CLUSTER);

			clsMgr_->checkCommandStatus(OP_LEAVE_CLUSTER);

			ClusterManager::LeaveClusterInfo leaveClusterInfo(
				alloc, senderNodeId);
			clsSvc_->decode(ev, leaveClusterInfo);

			TRACE_CLUSTER_HANDLER(eventType, WARNING, leaveClusterInfo.dump());

			clsMgr_->set(leaveClusterInfo);

			clsSvc_->requestGossip(ec, alloc);

			for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
				clsSvc_->requestChangePartitionStatus(
					ec, alloc, pId, PartitionTable::PT_STOP);
			}

			clsMgr_->updateClusterStatus(TO_SUBMASTER, true);

			clsMgr_->updateNodeStatus(OP_LEAVE_CLUSTER);

			if (clsMgr_->isShutdownPending()) {
				TRACE_CLUSTER_OPERATION(eventType, INFO,
					"[NOTE] Normal shutdown is started by pending "
					"command(shutdown)");

				clsMgr_->checkClusterStatus(OP_SHUTDOWN_NODE_NORMAL);

				clsMgr_->checkCommandStatus(OP_SHUTDOWN_NODE_NORMAL);

				cpSvc_->requestShutdownCheckpoint(ec);

				clsMgr_->updateNodeStatus(OP_SHUTDOWN_NODE_NORMAL);
			}

			TRACE_CLUSTER_HANDLER(eventType, DEBUG, "");

			break;
		}
		case CS_SHUTDOWN_NODE_FORCE: {
			clsMgr_->checkClusterStatus(OP_SHUTDOWN_NODE_FORCE);

			clsMgr_->checkCommandStatus(OP_SHUTDOWN_NODE_FORCE);

			ClusterManager::ShutdownNodeInfo shutdownNodeInfo(
				alloc, senderNodeId, true);
			clsSvc_->decode(ev, shutdownNodeInfo);

			TRACE_CLUSTER_HANDLER(eventType, WARNING, shutdownNodeInfo.dump());

			sysSvc_->shutdown();
			clsSvc_->shutdown();
			cpSvc_->shutdown();
			txnSvc_->shutdown();
			syncSvc_->shutdown();

#if defined(SYSTEM_CAPTURE_SIGNAL)
			pid_t pid = getpid();
			kill(pid, SIGTERM);
#endif  

			TRACE_CLUSTER_OPERATION(eventType, INFO,
				"[INFO] Force shutdown node operation is completed.");

			break;
		}

		case CS_SHUTDOWN_NODE_NORMAL: {
			clsMgr_->checkClusterStatus(OP_SHUTDOWN_NODE_NORMAL);

			clsMgr_->checkCommandStatus(OP_SHUTDOWN_NODE_NORMAL);

			ClusterManager::ShutdownNodeInfo shutdownNodeInfo(
				alloc, senderNodeId, false);
			clsSvc_->decode(ev, shutdownNodeInfo);

			TRACE_CLUSTER_HANDLER(eventType, WARNING, shutdownNodeInfo.dump());

			cpSvc_->requestShutdownCheckpoint(ec);

			clsMgr_->updateNodeStatus(OP_SHUTDOWN_NODE_NORMAL);

			TRACE_CLUSTER_OPERATION(eventType, INFO,
				"[INFO] Normal shutdown node operation is completed.");

			break;
		}

		case CS_SHUTDOWN_CLUSTER: {
			clsMgr_->checkClusterStatus(OP_SHUTDOWN_CLUSTER);

			clsMgr_->checkCommandStatus(OP_SHUTDOWN_CLUSTER);

			TRACE_CLUSTER_HANDLER(eventType, WARNING, "");

			Event leaveEvent(ec, CS_LEAVE_CLUSTER, CS_HANDLER_PARTITION_ID);
			ClusterManager::LeaveClusterInfo leaveClusterInfo(
				alloc, senderNodeId);
			clsSvc_->encode(leaveEvent, leaveClusterInfo);

			TRACE_CLUSTER_HANDLER(
				CS_LEAVE_CLUSTER, WARNING, leaveClusterInfo.dump());

			util::XArray<NodeId> liveNodeIdList(alloc);
			pt_->getLiveNodeIdList(liveNodeIdList);

			TRACE_CLUSTER_OPERATION(eventType, INFO,
				"[INFO] Shutdown cluster info, nodeNum:"
					<< liveNodeIdList.size() << ", address list: "
					<< pt_->dumpNodeAddressList(liveNodeIdList));

			for (size_t pos = 0; pos < liveNodeIdList.size(); pos++) {
				const NodeDescriptor &nd =
					clsEE_->getServerND(liveNodeIdList[pos]);
				if (!nd.isEmpty()) {
					if (nd.getId() == 0) {
						clsEE_->add(leaveEvent);
					}
					else {
						clsEE_->send(leaveEvent, nd);
						TRACE_CLUSTER_EE_SEND(
							CS_LEAVE_CLUSTER, nd, WARNING, "");
					}
				}
				else {
					TRACE_CLUSTER_HANDLER(eventType, INFO,
						"Target nd(nodId:" << liveNodeIdList[pos]
										   << ") is invalid.");
				}
			}

			TRACE_CLUSTER_OPERATION(eventType, INFO,
				"[INFO] Shutdown cluster operation is completed.");

			break;
		}

		case CS_COMPLETE_CHECKPOINT_FOR_SHUTDOWN: {
			TRACE_CLUSTER_OPERATION(eventType, INFO,
				"[INFO] Shutdown checkpoint is completed, shutdown all "
				"services.");

			clsMgr_->checkClusterStatus(OP_COMPLETE_CHECKPOINT_FOR_SHUTDOWN);

			clsMgr_->checkCommandStatus(OP_COMPLETE_CHECKPOINT_FOR_SHUTDOWN);

			TRACE_CLUSTER_HANDLER(eventType, WARNING, "");

			sysSvc_->shutdown();
			clsSvc_->shutdown();
			cpSvc_->shutdown();
			txnSvc_->shutdown();
			syncSvc_->shutdown();

#if defined(SYSTEM_CAPTURE_SIGNAL)
			pid_t pid = getpid();
			kill(pid, SIGTERM);
#endif  

			clsMgr_->updateNodeStatus(OP_COMPLETE_CHECKPOINT_FOR_SHUTDOWN);

			TRACE_CLUSTER_OPERATION(eventType, INFO,
				"[INFO] Normal shutdown node operation is completed.");

			break;
		}

		case CS_COMPLETE_CHECKPOINT_FOR_RECOVERY: {
			TRACE_CLUSTER_OPERATION(eventType, INFO,
				"[INFO] Recovery checkpoint is completed, start all services.");
			{
				util::LockGuard<util::Mutex> lock(clsMgr_->getClusterLock());

				if (!clsMgr_->isSignalBeforeRecovery()) {
					clsMgr_->checkClusterStatus(
						OP_COMPLETE_CHECKPOINT_FOR_RECOVERY);

					clsMgr_->checkCommandStatus(
						OP_COMPLETE_CHECKPOINT_FOR_RECOVERY);

					TRACE_CLUSTER_HANDLER(eventType, WARNING, "");

					clsMgr_->updateNodeStatus(
						OP_COMPLETE_CHECKPOINT_FOR_RECOVERY);

					TRACE_CLUSTER_HANDLER(eventType, DEBUG, "");
				}
				else {
					TRACE_CLUSTER_HANDLER(eventType, WARNING, "");

					sysSvc_->shutdown();
					clsSvc_->shutdown();
					cpSvc_->shutdown();
					txnSvc_->shutdown();
					syncSvc_->shutdown();

#if defined(SYSTEM_CAPTURE_SIGNAL)
					pid_t pid = getpid();
					kill(pid, SIGTERM);
#endif  
					TRACE_CLUSTER_OPERATION(eventType, INFO,
						"[INFO] Normal shutdown node operation is completed.");
				}
			}

			break;
		}
		default: {
			GS_THROW_USER_ERROR(GS_ERROR_CS_INVALID_CLUSTER_OPERATION_TYPE, "");
			break;
		}
		}
	}
	catch (UserException &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, WARNING, "");
	}
	catch (std::exception &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, ERROR, "");
		clsSvc_->setError(ec, &e);
	}
}

/*!
	@brief Handler Operator
*/
void TimerCheckClusterHandler::operator()(EventContext &ec, Event &ev) {
	EventType eventType = ev.getType();
	TRACE_CLUSTER_HANDLER_DETAIL(ev, eventType, DEBUG, "");

	try {
		clsMgr_->checkNodeStatus();

		try {
			clsMgr_->checkClusterStatus(OP_TIMER_CHECK_CLUSTER);
		}
		catch (UserException &) {
			return;
		}

		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);

		UTIL_TRACE_DEBUG(CLUSTER_SERVICE, clsMgr_->dump());
		UTIL_TRACE_DEBUG(CLUSTER_SERVICE,
			pt_->dumpPartitions(alloc, PartitionTable::PT_CURRENT_OB));
		UTIL_TRACE_DEBUG(CLUSTER_SERVICE,
			pt_->dumpPartitions(alloc, PartitionTable::PT_NEXT_OB));
		UTIL_TRACE_DEBUG(CLUSTER_SERVICE,
			pt_->dumpPartitions(alloc, PartitionTable::PT_NEXT_GOAL));
		UTIL_TRACE_DEBUG(CLUSTER_SERVICE, pt_->dumpDatas(true));

		bool isInitialCluster = clsMgr_->isInitialCluster();
		bool isPendingSync = clsMgr_->isPendingSync();
		ClusterManager::HeartbeatCheckInfo heartbeatCheckInfo(alloc);
		clsMgr_->get(heartbeatCheckInfo);

		TRACE_CLUSTER_HANDLER(eventType, INFO, heartbeatCheckInfo.dump());

		ClusterStatusTransition nextTransition =
			heartbeatCheckInfo.getNextTransition();
		if (nextTransition == TO_SUBMASTER) {
			for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
				clsSvc_->requestChangePartitionStatus(
					ec, alloc, pId, PartitionTable::PT_OFF);
			}
		}

		clsMgr_->updateClusterStatus(nextTransition);

		if (!pt_->isFollower()) {
			bool isAddNewNode = heartbeatCheckInfo.isAddNewNode();
			if (isPendingSync) isAddNewNode = true;

			ClusterManager::HeartbeatInfo heartbeatInfo(
				alloc, 0, pt_, isAddNewNode);

			clsMgr_->set(heartbeatInfo);

			clsMgr_->get(heartbeatInfo);

			if (nextTransition == TO_MASTER) {
				heartbeatInfo.setInitialCluster();
			}

			std::vector<NodeId> &activeNodeList =
				heartbeatCheckInfo.getActiveNodeList();

			int32_t activeNodeListSize =
				static_cast<int32_t>(activeNodeList.size());

			if (activeNodeListSize > 1) {
				Event heartbeatEvent(ec, CS_HEARTBEAT, CS_HANDLER_PARTITION_ID);
				clsSvc_->encode(heartbeatEvent, heartbeatInfo);

				TRACE_CLUSTER_HANDLER(CS_HEARTBEAT, INFO, heartbeatInfo.dump());

				NodeId nodeId;
				int32_t duplicateNum =
					clsMgr_->getConfig().getDuplicateMaxLsnNodeNum();
				int32_t validCount = 0;

				int64_t currentTime =
					util::DateTime::now(TRIM_MILLISECONDS).getUnixTime();
				for (nodeId = 1; nodeId < activeNodeListSize;
					 nodeId++, validCount++) {
					TRACE_CLUSTER_NORMAL(INFO,
						"node:"
							<< pt_->dumpNodeAddress(activeNodeList[nodeId])
							<< ", time:"
							<< getTimeStr(
								   clsMgr_->nextHeartbeatTime(currentTime))
							<< ", ack"
							<< pt_->getAckHeartbeat(activeNodeList[nodeId]));

					if (pt_->getAckHeartbeat(activeNodeList[nodeId])) {
						pt_->setHeartbeatTimeout(activeNodeList[nodeId],
							clsMgr_->nextHeartbeatTime(currentTime));

						TRACE_CLUSTER_NORMAL(INFO,
							"node:"
								<< pt_->dumpNodeAddress(activeNodeList[nodeId])
								<< ", time:"
								<< getTimeStr(
									   clsMgr_->nextHeartbeatTime(currentTime))
								<< ", ack"
								<< pt_->getAckHeartbeat(
									   activeNodeList[nodeId]));
					}
					pt_->setAckHeartbeat(activeNodeList[nodeId], false);

					const NodeDescriptor &nd =
						clsEE_->getServerND(activeNodeList[nodeId]);
					if (!nd.isEmpty()) {
						if (validCount == duplicateNum) {
							heartbeatEvent.getMessageBuffer().clear();
							clsSvc_->encode(heartbeatEvent, heartbeatInfo);
							TRACE_CLUSTER_HANDLER(CS_HEARTBEAT, WARNING,
								"Clear duplicate maxlsn list after nodeId:"
									<< nodeId
									<< ", duplicateNum:" << duplicateNum << ","
									<< heartbeatInfo.dump());
						}
						if (!clsEE_->send(heartbeatEvent, nd)) {
							pt_->setAckHeartbeat(activeNodeList[nodeId], false);
							pt_->setHeartbeatTimeout(
								activeNodeList[nodeId], UNDEF_TTL);
						}
						TRACE_CLUSTER_EE_SEND(CS_HEARTBEAT, nd, INFO, "");
					}
					else {
						TRACE_CLUSTER_HANDLER(eventType, INFO,
							"Target nd(nodeId:" << nodeId << ") is invalid.");
						continue;
					}
				}
			}

			if (pt_->isMaster()) {
				if ((isInitialCluster && activeNodeListSize > 1) &&
					(nextTransition == TO_MASTER)) {
					clsMgr_->setPendingSync();
					TRACE_CLUSTER_HANDLER(eventType, INFO,
						"Pending 1 heartbeat interval for collecting stats "
						"from follower nodes");
					TRACE_CLUSTER_HANDLER(eventType, DEBUG, "");
					return;
				}

				ClusterManager::UpdatePartitionInfo updatePartitionInfo(alloc,
					0, pt_, isAddNewNode,
					((nextTransition != KEEP) || clsMgr_->isForceSync()));
				clsMgr_->set(updatePartitionInfo);

				if (updatePartitionInfo.isSync()) {
					Event updatePartitionEvent(
						ec, CS_UPDATE_PARTITION, CS_HANDLER_PARTITION_ID);
					clsSvc_->encode(updatePartitionEvent, updatePartitionInfo);

					TRACE_CLUSTER_HANDLER(
						CS_UPDATE_PARTITION, INFO, updatePartitionInfo.dump());

					util::Set<NodeId> &filterNodeSet =
						updatePartitionInfo.getFilterNodeSet();
					for (util::Set<NodeId>::iterator it = filterNodeSet.begin();
						 it != filterNodeSet.end(); it++) {
						NodeId nodeId = (*it);
						if (nodeId == 0) {
							clsEE_->add(updatePartitionEvent);
						}
						else {
							const NodeDescriptor &nd =
								clsEE_->getServerND(nodeId);
							if (!nd.isEmpty()) {
								clsEE_->send(updatePartitionEvent, nd);
								TRACE_CLUSTER_EE_SEND(
									CS_UPDATE_PARTITION, nd, INFO, "");
							}
							else {
								TRACE_CLUSTER_HANDLER(CS_UPDATE_PARTITION,
									WARNING,
									"Target nd(" << nodeId << ") is invalid.");
							}
						}
					}
				}
			}
		}
		TRACE_CLUSTER_HANDLER(eventType, DEBUG, "");
	}
	catch (UserException &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, WARNING, "");
	}
	catch (std::exception &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, ERROR, "");
		clsSvc_->setError(ec, &e);
	}
}

/*!
	@brief Handler Operator
*/
void TimerNotifyClusterHandler::operator()(EventContext &ec, Event &ev) {
	EventType eventType = ev.getType();
	TRACE_CLUSTER_HANDLER_DETAIL(ev, eventType, DEBUG, "");

	try {
		clsMgr_->checkNodeStatus();

		try {
			clsMgr_->checkClusterStatus(OP_TIMER_NOTIFY_CLUSTER);
		}
		catch (UserException &) {
			return;
		}

		TRACE_CLUSTER_HANDLER(eventType, INFO, "");

		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);

		ClusterManager::NotifyClusterInfo notifyClusterInfo(alloc, 0, pt_);
		clsMgr_->get(notifyClusterInfo);

		Event notifyClusterEvent(
			ec, CS_NOTIFY_CLUSTER, CS_HANDLER_PARTITION_ID);
		clsSvc_->encode(notifyClusterEvent, notifyClusterInfo);

		TRACE_CLUSTER_HANDLER(
			CS_NOTIFY_CLUSTER, INFO, notifyClusterInfo.dump());

		const NodeDescriptor &nd = clsEE_->getMulticastND();
		if (!nd.isEmpty()) {
			TRACE_CLUSTER_EE_SEND(CS_NOTIFY_CLUSTER, nd, DEBUG, "");
			clsEE_->send(notifyClusterEvent, nd);
		}
		else {
			TRACE_CLUSTER_HANDLER(CS_NOTIFY_CLUSTER, INFO,
				"Cluster muliticast address is invalid.");
		}
		TRACE_CLUSTER_HANDLER(eventType, DEBUG, "");
	}
	catch (UserException &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, WARNING, "");
	}
	catch (std::exception &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, ERROR, "");
		clsSvc_->setError(ec, &e);
	}
}

/*!
	@brief Handler Operator
*/
void TimerNotifyClientHandler::operator()(EventContext &ec, Event &ev) {
	EventType eventType = ev.getType();
	TRACE_CLUSTER_HANDLER_DETAIL(ev, eventType, DEBUG, "");

	try {
		clsMgr_->checkNodeStatus();

		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);

		try {
			clsMgr_->checkClusterStatus(OP_TIMER_NOTIFY_CLIENT);
		}
		catch (UserException &) {
			return;
		}

		TRACE_CLUSTER_HANDLER(eventType, INFO, "");

		Event notifyClientEvent(
			ec, RECV_NOTIFY_MASTER, CS_HANDLER_PARTITION_ID);
		clsSvc_->encodeNotifyClient(notifyClientEvent);

		const NodeDescriptor &nd = txnEE_->getMulticastND();
		if (!nd.isEmpty()) {
			notifyClientEvent.setPartitionIdSpecified(false);
			TRACE_CLUSTER_EE_SEND(RECV_NOTIFY_MASTER, nd, DEBUG, "");
			txnEE_->send(notifyClientEvent, nd);
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_CS_INVALID_SENDER_ND, "");
		}
		TRACE_CLUSTER_HANDLER(eventType, DEBUG, "");
	}
	catch (UserException &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, WARNING, "");
	}
	catch (std::exception &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, ERROR, "");
		clsSvc_->setError(ec, &e);
	}
}

/*!
	@brief Handler Operator
*/
void TimerCheckLoadBalanceHandler::operator()(EventContext &ec, Event &ev) {
	EventType eventType = ev.getType();
	TRACE_CLUSTER_HANDLER_DETAIL(ev, eventType, DEBUG, "");

	try {
		clsMgr_->checkCheckpointDelayLimitTime(
			util::DateTime::now(TRIM_MILLISECONDS).getUnixTime());

		clsMgr_->checkNodeStatus();

		try {
			clsMgr_->checkClusterStatus(OP_TIMER_CHECK_LOAD_BALANCE);
		}
		catch (UserException &) {
			return;
		}

		TRACE_CLUSTER_HANDLER(eventType, INFO, "");

		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);

		bool isCatchupError = pt_->isCatchupError();

		if (pt_->isMaster()) {
			pt_->resetBlockQueue();

			CheckedPartitionStat balanceStatus = pt_->checkPartitionStat();

			PartitionId targetCatchupPId = pt_->getCatchupPId();

			TRACE_CLUSTER_HANDLER(eventType, WARNING,
				"info={[INFO] Partition status:"
					<< pt_->dumpPartitionStat(balanceStatus)
					<< ", catchup:" << targetCatchupPId << "}");

			if (targetCatchupPId != UNDEF_PARTITIONID) {
				int64_t limitTime =
					pt_->getPartitionInfo(targetCatchupPId)->longTermTimeout_;
				int64_t checkTime =
					util::DateTime::now(TRIM_MILLISECONDS).getUnixTime();
				bool timeoutCheck =
					(limitTime != UNDEF_TTL && checkTime > limitTime);

				TRACE_CLUSTER_HANDLER(eventType, WARNING,
					"checkTime:" << checkTime
								 << ", timeoutCheck:" << timeoutCheck);

				if (timeoutCheck || limitTime == UNDEF_TTL) {
					pt_->setCatchupPId(UNDEF_PARTITIONID, UNDEF_NODEID);
					pt_->getPartitionInfo(targetCatchupPId)->longTermTimeout_ =
						UNDEF_TTL;
				}
			}

			if (balanceStatus != PartitionTable::PT_NORMAL) {
				TRACE_CLUSTER_HANDLER(eventType, WARNING,
					"[NOTE] Cluster partition status:"
						<< pt_->dumpPartitionStat(balanceStatus));
			}
			if ((balanceStatus != PartitionTable::PT_NORMAL &&
					pt_->getCatchupPId() == UNDEF_PARTITIONID) ||
				isCatchupError) {
				if (clsMgr_->checkLoadBalance()) {
					TRACE_CLUSTER_OPERATION(eventType, INFO,
						"[NOTE] Resync by cluster partition status:"
							<< pt_->dumpPartitionStat(balanceStatus));

					TRACE_CLUSTER_HANDLER(eventType, WARNING,
						"cachupPId:" << pt_->getCatchupPId()
									 << ", isCatchup:" << isCatchupError);

					clsMgr_->setForceSync();
				}
				else {
					TRACE_CLUSTER_HANDLER(eventType, WARNING,
						"Detect cluster partition status irregular, but skip "
						"load balancing, load balancer is not active.");
				}
			}

			NodeId ownerNodeId;
			ClusterManager::OrderDropPartitionInfo orderDropPartitionInfo(
				alloc);

			int32_t backupNum = pt_->getReplicationNum();
			int32_t liveNodeNum = pt_->getLiveNum();
			if (backupNum > liveNodeNum) {
				backupNum = liveNodeNum;
			}

			for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
				bool needSync =
					(balanceStatus == PartitionTable::PT_NORMAL &&
						pt_->getPartitionInfo(pId)->shortTermTimeout_ ==
							UNDEF_TTL);
				bool isReplicaLoss = false;

				if (!pt_->isActive(pId)) {
					TRACE_CLUSTER_HANDLER(eventType, INFO,
						"pId:" << pId
							   << " is not service, set replica loss info");

					orderDropPartitionInfo.setReplicaLoss(pId);
					continue;
				}

				ownerNodeId = pt_->getOwner(pId);
				if (ownerNodeId == UNDEF_NODEID) {
					ownerNodeId =
						pt_->getOwner(pId, PartitionTable::PT_NEXT_OB);
				}

				if (ownerNodeId == UNDEF_NODEID) {
					continue;
				}

				PartitionStatus pstatus;
				pstatus = pt_->getPartitionStatus(pId, ownerNodeId);

				TRACE_CLUSTER_HANDLER(eventType, INFO,
					"Check owner :{pId:" << pId << ", owner:" << ownerNodeId
										 << ", status:"
										 << dumpPartitionStatus(pstatus)
										 << ", needSync:" << needSync);

				if (pstatus != PartitionTable::PT_ON) {
					isReplicaLoss = true;
					if (needSync) {
						TRACE_CLUSTER_OPERATION(eventType, INFO,
							"[NOTE] Resync by cluster partition check, pId:"
								<< pId << ", owner:"
								<< pt_->dumpNodeAddress(ownerNodeId)
								<< ", status:" << dumpPartitionStatus(pstatus));
						clsMgr_->setForceSync();
					}
				}
				util::XArray<NodeId> backups(alloc);
				pt_->getBackup(pId, backups, PartitionTable::PT_CURRENT_OB);
				int32_t backupCount = 0;
				for (int32_t pos = 0;
					 pos < static_cast<int32_t>(backups.size()); pos++) {
					pstatus = pt_->getPartitionStatus(pId, backups[pos]);

					TRACE_CLUSTER_HANDLER(eventType, INFO,
						"Check backups, pId:"
							<< pId << ", backup:" << backups[pos]
							<< ", status:" << dumpPartitionStatus(pstatus)
							<< ", needSync:" << needSync);

					if (pstatus != PartitionTable::PT_ON) {
						isReplicaLoss = true;
						if (needSync) {
							TRACE_CLUSTER_OPERATION(eventType, INFO,
								"[NOTE] Resync by cluster partition check, pId="
									<< pId << ", backup:"
									<< pt_->dumpNodeAddress(backups[pos])
									<< ", status:"
									<< dumpPartitionStatus(pstatus));
							clsMgr_->setForceSync();
							break;
						}
					}
					else {
						backupCount++;
					}
				}
				if (isReplicaLoss || backupCount < backupNum) {
					orderDropPartitionInfo.setReplicaLoss(pId);

					TRACE_CLUSTER_HANDLER(eventType, WARNING,
						"Replica loss, pId:" << pId
											 << ", backupCount:" << backupCount
											 << ", backupNum:" << backupNum);
				}
				else {
				}
			}

			util::XArray<NodeId> activeNodeList(alloc);
			pt_->getLiveNodeIdList(activeNodeList);

			TRACE_CLUSTER_HANDLER(CS_ORDER_DROP_PARTITION, WARNING,
				orderDropPartitionInfo.dump());

			for (size_t pos = 0; pos < activeNodeList.size(); pos++) {
				Event orderDropPartitionEvent(
					ec, CS_ORDER_DROP_PARTITION, CS_HANDLER_PARTITION_ID);
				clsSvc_->encode(
					orderDropPartitionEvent, orderDropPartitionInfo);

				NodeId nodeId = activeNodeList[pos];
				if (nodeId == 0) {
					clsEE_->add(orderDropPartitionEvent);
				}
				else {
					const NodeDescriptor &nd = clsEE_->getServerND(nodeId);
					if (!nd.isEmpty()) {
						TRACE_CLUSTER_EE_SEND(
							CS_ORDER_DROP_PARTITION, nd, INFO, "");
						clsEE_->send(orderDropPartitionEvent, nd);
					}
					else {
						TRACE_CLUSTER_HANDLER(CS_UPDATE_PARTITION, WARNING,
							"Target nd(" << nodeId << ") is invalid.");
					}
				}
			}
		}
		TRACE_CLUSTER_HANDLER(eventType, DEBUG, "");
	}
	catch (UserException &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, WARNING, "");
	}
	catch (std::exception &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, ERROR, "");
		clsSvc_->setError(ec, &e);
	}
}

/*!
	@brief Handler Operator
*/
void OrderDropPartitionHandler::operator()(EventContext &ec, Event &ev) {
	EventType eventType = ev.getType();
	TRACE_CLUSTER_HANDLER_DETAIL(ev, eventType, DEBUG, "");

	try {
		clsMgr_->checkNodeStatus();

		clsMgr_->checkClusterStatus(OP_ORDER_DROP_PARTITION);

		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);

		ClusterManager::OrderDropPartitionInfo orderDropPartitionInfo(alloc);
		clsSvc_->decode(ev, orderDropPartitionInfo);

		TRACE_CLUSTER_HANDLER(
			eventType, WARNING, orderDropPartitionInfo.dump());

		util::Set<PartitionId> replicaLossPartitionSet(alloc);
		std::vector<PartitionId> &replicaLossPartitionList =
			orderDropPartitionInfo.getPartitionList();
		for (size_t pos = 0; pos < replicaLossPartitionList.size(); pos++) {
			replicaLossPartitionSet.insert(replicaLossPartitionList[pos]);
		}
		for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
			bool isReplicaLoss = false, isInvalidChunk = false;
			util::Set<PartitionId>::iterator setItr =
				replicaLossPartitionSet.find(pId);
			isReplicaLoss = (setItr != replicaLossPartitionSet.end());
			if (isReplicaLoss) {
				TRACE_CLUSTER_HANDLER(
					eventType, WARNING, "Replica loss partition, pId:" << pId);
			}
			isInvalidChunk =
				(pt_->getLSN(pId) == 0 && chunkMgr_->existPartition(pId));
			if (isInvalidChunk) {
				TRACE_CLUSTER_HANDLER(
					eventType, WARNING, "Invalid partition, pId:" << pId);
			}
			if (pt_->checkDrop(pId) && (!isReplicaLoss || isInvalidChunk)) {
				syncSvc_->requestDrop(ec, alloc, pId, false);
			}
		}
		TRACE_CLUSTER_HANDLER(eventType, DEBUG, "");
	}
	catch (UserException &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, WARNING, "");
	}
	catch (std::exception &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, ERROR, "");
		clsSvc_->setError(ec, &e);
	}
}

template <class T>
std::string dumpPartitionList(
	PartitionTable *pt, T &partitionList, PartitionRevision &revision) {
	util::NormalOStringStream ss;
	int32_t listSize = static_cast<int32_t>(partitionList.size());
	ss << "{revision:" << revision.toString();
	ss << ", count:" << listSize << ", partitions:[";
	for (int32_t pos = 0; pos < listSize; pos++) {
		ss << partitionList[pos];
		ss << "(" << pt->getLSN(partitionList[pos]) << ")";
		if (pos != listSize - 1) {
			ss << ",";
		}
	}
	ss << "]}";
	return ss.str().c_str();
}

/*!
	@brief Handler Operator
*/
void UpdatePartitionHandler::operator()(EventContext &ec, Event &ev) {
	EventType eventType = ev.getType();
	TRACE_CLUSTER_HANDLER_DETAIL(ev, eventType, INFO, "");

	try {
		clsMgr_->checkNodeStatus();

		clsMgr_->checkClusterStatus(OP_UPDATE_PARTITION);

		EventType eventType = ev.getType();

		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);
		NodeId senderNodeId = ClusterService::resolveSenderND(ev);

		ClusterManager::UpdatePartitionInfo updatePartitionInfo(
			alloc, senderNodeId, pt_);
		clsSvc_->decode(ev, updatePartitionInfo);

		if (!updatePartitionInfo.isPending()) {
			TRACE_CLUSTER_HANDLER(
				eventType, WARNING, updatePartitionInfo.dump());
		}

		if (pt_->isFollower()) {
			std::vector<LogSequentialNumber> &maxLsnList =
				updatePartitionInfo.getMaxLsnList();
			for (PartitionId pId = 0; pId < maxLsnList.size(); pId++) {
				pt_->setRepairedMaxLsn(pId, maxLsnList[pId]);
			}
		}

		SubPartitionTable &subPartitionTable =
			updatePartitionInfo.getSubPartitionTable();
		int32_t subPartitionSize = subPartitionTable.size();

		updateNodeList(eventType, subPartitionTable.getNodeAddressList());

		if (pt_->isFollower()) {
			std::vector<AddressInfo> &addressInfoList =
				subPartitionTable.getNodeAddressList();
			NodeId masterNodeId = pt_->getMaster();

			TRACE_CLUSTER_HANDLER(
				eventType, INFO, "Update follower down node list:{size:"
									 << addressInfoList.size()
									 << ", master:" << masterNodeId << "}");

			for (size_t pos = 0; pos < addressInfoList.size(); pos++) {
				NodeId nodeId = ClusterService::getNodeId(
					addressInfoList[pos].clusterAddress_, clsEE_);
				if (!addressInfoList[pos].isActive_) {
					TRACE_CLUSTER_HANDLER(eventType, WARNING,
						"nodeId:" << addressInfoList[pos].dump());
				}
				else {
					TRACE_CLUSTER_HANDLER(eventType, INFO,
						"nodeId:" << addressInfoList[pos].dump());
				}
				if (nodeId == 0 || nodeId == masterNodeId) {
				}
				else {
					if (addressInfoList[pos].isActive_) {
						pt_->setHeartbeatTimeout(nodeId, 1);
					}
					else {
						pt_->setHeartbeatTimeout(nodeId, UNDEF_TTL);
						TRACE_CLUSTER_HANDLER(eventType, WARNING,
							"downNode:{" << pt_->dumpNodeAddress(nodeId)
										 << "}");
					}
				}
			}
		}

		clsMgr_->checkCheckpointDelayLimitTime();

		if (updatePartitionInfo.isPending()) {
			updatePartitionInfo.setPending(false);
			Event requestEvent(
				ec, CS_UPDATE_PARTITION, CS_HANDLER_PARTITION_ID);
			clsSvc_->encode(requestEvent, updatePartitionInfo);

			TRACE_CLUSTER_HANDLER(
				CS_UPDATE_PARTITION, INFO, updatePartitionInfo.dump());

			clsEE_->addTimer(requestEvent,
				clsMgr_->getExtraConfig().getClusterReconstructWaitTime());

			TRACE_CLUSTER_HANDLER(CS_UPDATE_PARTITION, INFO,
				"Pending sync operation, revision:"
					<< pt_->getPartitionRevision().toString() << ", waitTime:"
					<< clsMgr_->getExtraConfig()
						   .getClusterReconstructWaitTime());

			TRACE_CLUSTER_HANDLER(eventType, DEBUG, "");

			return;
		}

		pt_->updatePartitionRevision(
			subPartitionTable.revision_.sequentialNumber_);

		util::XArray<PartitionId> nextOwnerList(alloc);
		util::XArray<PartitionId> nextBackupList(alloc);
		util::XArray<PartitionId> nextGoalOwnerList(alloc);
		util::XArray<PartitionId> nextGoalCatchupList(alloc);

		for (int32_t pos = 0; pos < subPartitionSize; pos++) {
			SubPartition &subPartition = subPartitionTable.getSubPartition(pos);
			TRACE_CLUSTER_HANDLER(
				eventType, INFO, "Recv:" << subPartition.dump());

			NodeAddress ownerAddress;
			std::vector<NodeAddress> backupAddressList;
			subPartition.role_.get(ownerAddress, backupAddressList);

			NodeId owner = ClusterService::getNodeId(ownerAddress, clsEE_);
			std::vector<NodeId> backups;
			for (int32_t backupPos = 0;
				 backupPos < static_cast<int32_t>(backupAddressList.size());
				 backupPos++) {
				NodeId backup = ClusterService::getNodeId(
					backupAddressList[backupPos], clsEE_);
				backups.push_back(backup);
			}
			subPartition.role_.set(owner, backups);
			subPartition.role_.restoreType();

			NodeId currentOwner =
				ClusterService::getNodeId(subPartition.currentOwner_, clsEE_);

			pt_->set(subPartition);
			TRACE_CLUSTER_HANDLER(eventType, WARNING,
				"CurrentOwner:" << subPartition.currentOwner_.toString()
								<< ", Changed:" << subPartition.dump());

			PartitionId pId = subPartition.pId_;
			PartitionRole &role = subPartition.role_;

			if (pId == UNDEF_PARTITIONID || pId >= pt_->getPartitionNum()) {
				TRACE_SYNC_HANDLER(
					eventType, pId, WARNING, "Invalid decode pId");
				continue;
			}

			if (role.getTableType() == PartitionTable::PT_NEXT_OB ||
				role.getTableType() == PartitionTable::PT_NEXT_GOAL) {
			}
			else {
				TRACE_SYNC_HANDLER(eventType, pId, WARNING,
					"Invalid decode, type=" << dumpPartitionTableType(
						role.getTableType()));
			}
			PartitionTable::TableType type = role.getTableType();

			PartitionRole nextRole, currentRole;
			pt_->getPartitionRole(pId, currentRole);
			bool isOwnerOrBackup = false;
			bool isCurrentOwner = (currentOwner == 0);
			bool isCurrentBackup = currentRole.isBackup();
			bool isNextOwner = role.isOwner();
			bool isNextBackup = role.isBackup();

			bool isCurrentOwnerDown = subPartition.isDownNextOwner_;

			if (type == PartitionTable::PT_NEXT_OB) {
				if (role.getRevision().sequentialNumber_ != 1 &&
					role.getRevision().sequentialNumber_ ==
						currentRole.getRevision().sequentialNumber_) {
					continue;
				}
				bool isNextDownOwner = (isCurrentOwnerDown && isNextOwner);

				TRACE_CLUSTER_HANDLER(eventType, WARNING,
					"current:" << currentRole << ", next:" << role
							   << ", isNextDownOwner:" << isNextDownOwner
							   << ", isCurrentOwner:" << isCurrentOwner);

				if (isCurrentOwner || isNextDownOwner) {
					SyncRequestInfo syncRequestInfo(alloc,
						TXN_SHORTTERM_SYNC_REQUEST, syncMgr_, pId, clsEE_,
						MODE_SHORTTERM_SYNC);

					syncRequestInfo.setPartitionRole(role);
					syncRequestInfo.setIsDownNextOwner();

					Event requestEvent(ec, TXN_SHORTTERM_SYNC_REQUEST, pId);
					syncSvc_->encode(requestEvent, syncRequestInfo);

					TRACE_CLUSTER_HANDLER(TXN_SHORTTERM_SYNC_REQUEST, INFO,
						syncRequestInfo.dump());

					TRACE_CLUSTER_HANDLER(TXN_SHORTTERM_SYNC_REQUEST, WARNING,
						"requestSyncInfo:{pId:"
							<< pId
							<< ", mode:shortTerm, role:current or next owner}");

					txnEE_->addTimer(requestEvent, EE_PRIORITY_HIGH);

					TRACE_SYNC_EE_ADD(
						TXN_SHORTTERM_SYNC_REQUEST, pId, DEBUG, "");

					nextOwnerList.push_back(pId);
					isOwnerOrBackup = true;
				}
				else if (isNextBackup) {
					TRACE_CLUSTER_HANDLER(TXN_SHORTTERM_SYNC_REQUEST, WARNING,
						"requestSyncInfo:{pId:"
							<< pId << ", mode:shortTerm, role:next backup}");

					nextBackupList.push_back(pId);
					isOwnerOrBackup = true;
				}
				else if (!pt_->isMaster() && !isCurrentOwner &&
						 !isCurrentBackup && !isNextOwner && !isNextBackup) {
					ClusterManager::ChangePartitionTableInfo
						changePartitionTableInfo(alloc, 0, role);
					Event requestEvent(ec, TXN_CHANGE_PARTITION_TABLE, pId);
					clsSvc_->encode(requestEvent, changePartitionTableInfo);

					TRACE_CLUSTER_HANDLER(TXN_CHANGE_PARTITION_TABLE, INFO,
						changePartitionTableInfo.dump());

					TRACE_CLUSTER_HANDLER(TXN_SHORTTERM_SYNC_REQUEST, WARNING,
						"requestSyncInfo:{pId:"
							<< pId << ", mode:shortTerm, role:none}");

					txnEE_->addTimer(requestEvent, EE_PRIORITY_HIGH);
					TRACE_SYNC_EE_ADD(
						TXN_CHANGE_PARTITION_TABLE, pId, DEBUG, "");
				}
			}
			else if (type == PartitionTable::PT_NEXT_GOAL) {
				if (isNextOwner) {
					SyncRequestInfo syncRequestInfo(alloc,
						TXN_LONGTERM_SYNC_REQUEST, syncMgr_, pId, clsEE_,
						MODE_LONGTERM_SYNC);
					syncRequestInfo.setPartitionRole(role);
					Event requestEvent(ec, TXN_LONGTERM_SYNC_REQUEST, pId);
					syncSvc_->encode(requestEvent, syncRequestInfo);

					TRACE_CLUSTER_HANDLER(TXN_LONGTERM_SYNC_REQUEST, WARNING,
						syncRequestInfo.dump());
					TRACE_CLUSTER_HANDLER(TXN_LONGTERM_SYNC_REQUEST, WARNING,
						"requestSyncInfo:{pId:"
							<< pId << ", mode:longTerm, role:next goal owner}");

					txnEE_->add(requestEvent);

					TRACE_SYNC_EE_ADD(
						TXN_SHORTTERM_SYNC_REQUEST, pId, WARNING, "");

					nextGoalOwnerList.push_back(pId);
					isOwnerOrBackup = true;
				}
				else if (isNextBackup) {
					TRACE_CLUSTER_HANDLER(TXN_SHORTTERM_SYNC_REQUEST, WARNING,
						"requestSyncInfo:{pId:"
							<< pId << ", mode:longTerm, role:catchup}");

					nextGoalCatchupList.push_back(pId);
					isOwnerOrBackup = true;
				}
				else {
					ClusterManager::ChangePartitionTableInfo
						changePartitionTableInfo(alloc, 0, role);
					Event requestEvent(ec, TXN_CHANGE_PARTITION_TABLE, pId);
					clsSvc_->encode(requestEvent, changePartitionTableInfo);

					TRACE_CLUSTER_HANDLER(TXN_CHANGE_PARTITION_TABLE, INFO,
						changePartitionTableInfo.dump());

					TRACE_CLUSTER_HANDLER(TXN_SHORTTERM_SYNC_REQUEST, WARNING,
						"requestSyncInfo:{pId:"
							<< pId << ", mode:longTerm, role:none}");

					txnEE_->addTimer(requestEvent, EE_PRIORITY_HIGH);
					TRACE_SYNC_EE_ADD(
						TXN_CHANGE_PARTITION_TABLE, pId, WARNING, "");
				}
			}
		}

		PartitionRevision &currentRevision = pt_->getPartitionRevision();
		bool isDump = false;
		if (nextOwnerList.size() > 0) {
			TRACE_CLUSTER_OPERATION(eventType, INFO,
				"[INFO] New next owners:{revision:"
					<< currentRevision << ", "
					<< dumpPartitionList(pt_, nextOwnerList, currentRevision));
			isDump = true;
		}
		if (nextBackupList.size() > 0) {
			TRACE_CLUSTER_OPERATION(eventType, INFO,
				"[INFO] Next backups:{revision:"
					<< currentRevision << ","
					<< dumpPartitionList(pt_, nextBackupList, currentRevision));
			isDump = true;
		}
		if (nextGoalOwnerList.size() > 0) {
			TRACE_CLUSTER_OPERATION(eventType, INFO,
				"[INFO] Next goal owners:{revision:" << currentRevision << ","
													 << dumpPartitionList(pt_,
															nextGoalOwnerList,
															currentRevision));
		}
		if (nextGoalCatchupList.size() > 0) {
			TRACE_CLUSTER_OPERATION(eventType, INFO,
				"[INFO] Next goal catchups:{revision:"
					<< currentRevision << ","
					<< dumpPartitionList(
						   pt_, nextGoalCatchupList, currentRevision));
		}

		if (isDump) {
			nextOwnerList.clear();
			nextBackupList.clear();

			for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
				if (pt_->isOwner(pId)) {
					nextOwnerList.push_back(pId);
				}
				else if (pt_->isBackup(pId)) {
					nextBackupList.push_back(pId);
				}
			}

			if (nextOwnerList.size() > 0) {
				TRACE_CLUSTER_HANDLER(eventType, WARNING,
					"[INFO] Current owners:" << dumpPartitionList(
						pt_, nextOwnerList, currentRevision));
			}
			if (nextBackupList.size() > 0) {
				TRACE_CLUSTER_HANDLER(eventType, WARNING,
					"[INFO] Current backups:" << dumpPartitionList(
						pt_, nextBackupList, currentRevision));
			}
		}
		TRACE_CLUSTER_HANDLER(eventType, DEBUG, "");
	}
	catch (UserException &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, WARNING, "");
	}
	catch (std::exception &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, ERROR, "");
		clsSvc_->setError(ec, &e);
	}
}

/*!
	@brief Handler Operator
*/
void GossipHandler::operator()(EventContext &ec, Event &ev) {
	EventType eventType = ev.getType();
	TRACE_CLUSTER_HANDLER_DETAIL(ev, eventType, DEBUG, "");

	try {
		clsMgr_->checkNodeStatus();

		clsMgr_->checkClusterStatus(OP_GOSSIP);

		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);

		ClusterManager::GossipInfo gossipInfo(alloc);
		clsSvc_->decode(ev, gossipInfo);

		TRACE_CLUSTER_HANDLER(eventType, WARNING, gossipInfo.dump());

		NodeId clusterNodeId = checkAddress(eventType, gossipInfo.getAddress(),
			UNDEF_NODEID, CLUSTER_SERVICE, clsEE_);
		gossipInfo.setNodeId(clusterNodeId);

		clsMgr_->set(gossipInfo);

		TRACE_CLUSTER_HANDLER(eventType, DEBUG, "");
	}
	catch (UserException &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, WARNING, "");
	}
	catch (std::exception &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, ERROR, "");
		clsSvc_->setError(ec, &e);
	}
}

void ClusterHandler::initialize(const ManagerSet &mgrSet) {
	try {
		clsSvc_ = mgrSet.clsSvc_;
		clsEE_ = clsSvc_->getEE();
		txnSvc_ = mgrSet.txnSvc_;
		txnEE_ = txnSvc_->getEE();
		syncSvc_ = mgrSet.syncSvc_;
		syncEE_ = syncSvc_->getEE();
		cpSvc_ = mgrSet.cpSvc_;
		cpEE_ = cpSvc_->getEE();
		sysSvc_ = mgrSet.sysSvc_;
		sysEE_ = sysSvc_->getEE();
		pt_ = mgrSet.pt_;
		clsMgr_ = clsSvc_->getManager();
		syncMgr_ = syncSvc_->getManager();
		chunkMgr_ = mgrSet.chunkMgr_;
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Gets node address
*/
void ClusterService::getAddress(
	NodeAddress &nodeAddress, NodeId nodeId, EventEngine *ee) {
	util::SocketAddress::Inet address;
	uint16_t port;
	const NodeDescriptor &nd = ee->getServerND(nodeId);
	if (nd.isEmpty()) {
		GS_THROW_USER_ERROR(GS_ERROR_CS_INVALID_ND, "");
	}
	nd.getAddress().getIP(&address, &port);
	nodeAddress.address_ = *reinterpret_cast<AddressType *>(&address);
	nodeAddress.port_ = port;
}

/*!
	@brief Gets node id
*/
NodeId ClusterService::getNodeId(NodeAddress &nodeAddress, EventEngine *ee) {
	const NodeDescriptor &nd = ee->getServerND(
		util::SocketAddress(*(util::SocketAddress::Inet *)&nodeAddress.address_,
			nodeAddress.port_));
	if (!nd.isEmpty()) {
		return static_cast<NodeId>(nd.getId());
	}
	else {
		return UNDEF_NODEID;
	}
}

/*!
	@brief Sets node address and gets node id
*/
NodeId ClusterHandler::checkAddress(EventType eventType, NodeAddress &address,
	NodeId clusterNodeId, ServiceType serviceType, EventEngine *ee) {
	try {
		if (clusterNodeId != UNDEF_NODEID &&
			pt_->getNodeInfo(clusterNodeId).check(serviceType)) {
			TRACE_CLUSTER_HANDLER(eventType, DEBUG,
				"Already setted, service={type:" << serviceType << ", address:{"
												 << address.toString() << "}}");
			return clusterNodeId;
		}

		if (serviceType == SYSTEM_SERVICE) {
			if (!pt_->setNodeInfo(clusterNodeId, serviceType, address)) {
				TRACE_CLUSTER_HANDLER(
					eventType, DEBUG, "System service setted, service={type:"
										  << serviceType << ", address:{"
										  << address.toString() << "}}");
			}
			return clusterNodeId;
		}

		util::SocketAddress socketAddress(
			*(util::SocketAddress::Inet *)&address.address_, address.port_);
		NodeId nodeId;

		if (serviceType != CLUSTER_SERVICE) {
			ee->setServerNodeId(socketAddress, clusterNodeId, false);
		}

		const NodeDescriptor &nd = ee->resolveServerND(socketAddress);
		if (!nd.isEmpty()) {
			nodeId = static_cast<NodeId>(nd.getId());
			if (pt_->setNodeInfo(nodeId, serviceType, address)) {
				TRACE_CLUSTER_HANDLER(
					eventType, INFO, "Set new address, service:{type:"
										 << serviceType << ", address:{"
										 << address.toString() << "}}");
			}
			return nodeId;
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_CS_GET_INVALID_NODE_ADDRESS, "");
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Updates address of node list
*/
void ClusterHandler::updateNodeList(
	EventType eventType, std::vector<AddressInfo> &addressInfoList) {
	try {
		for (size_t nodePos = 0; nodePos < addressInfoList.size(); nodePos++) {
			NodeId clusterNodeId = UNDEF_NODEID, tmpNodeId;
			EventEngine *targetEE;

			for (int32_t serviceType = 0; serviceType < SERVICE_MAX;
				 serviceType++) {
				ServiceType currentServiceType =
					static_cast<ServiceType>(serviceType);
				if (clusterNodeId == 0) {
					break;
				}
				NodeAddress &targetAddress =
					addressInfoList[nodePos].getNodeAddress(currentServiceType);

				if (!targetAddress.isValid()) {
					continue;
				}
				targetEE = clsSvc_->getEE(currentServiceType);

				tmpNodeId = checkAddress(eventType, targetAddress,
					clusterNodeId, currentServiceType, targetEE);

				if (clusterNodeId != UNDEF_NODEID &&
					(clusterNodeId != tmpNodeId)) {
					const NodeDescriptor &nd = targetEE->getSelfServerND();
					targetEE->resetConnection(nd);
					GS_THROW_USER_ERROR(
						GS_ERROR_CS_INVALID_SERVICE_ADDRESS_RELATION,
						"cluster nodeId:" << clusterNodeId
										  << ", current nodeId:" << tmpNodeId
										  << ", serviceType:" << serviceType);
				}
				else {
					clusterNodeId = tmpNodeId;
				}
			}
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Checks partition table and sets node information of a specified ND
*/
void ClusterHandler::checkAutoNdNumbering(const NodeDescriptor &nd) {
	try {
		if (!nd.isEmpty()) {
			NodeId nodeId = static_cast<NodeId>(nd.getId());
			if (nodeId >= pt_->getNodeNum()) {
				NodeAddress address;
				address.set(nd.getAddress());
				pt_->setNodeInfo(nodeId, CLUSTER_SERVICE, address);
			}
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Handler Operator
*/
void ChangePartitionStateHandler::operator()(EventContext &ec, Event &ev) {
	EventType eventType = ev.getType();
	PartitionId pId = ev.getPartitionId();

	TRACE_SYNC_HANDLER_DETAIL(ev, eventType, pId, DEBUG, "");

	try {
		clsMgr_->checkNodeStatus();

		clsMgr_->checkClusterStatus(OP_CHANGE_PARTITION_STATUS);

		TRACE_SYNC_HANDLER(eventType, pId, DEBUG, "");

		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);

		ClusterManager::ChangePartitionStatusInfo changePartitionStatusInfo(
			alloc);
		clsSvc_->decode(ev, changePartitionStatusInfo);

		TRACE_SYNC_HANDLER(
			eventType, pId, INFO, changePartitionStatusInfo.dump());

		PartitionStatus changeStatus;
		changePartitionStatusInfo.getPartitionStatus(pId, changeStatus);

		txnSvc_->changeTimeoutCheckMode(pId, changeStatus,
			changePartitionStatusInfo.getPartitionChangeType(),
			changePartitionStatusInfo.isToSubMaster(), clsSvc_->getStats());

		clsMgr_->set(changePartitionStatusInfo);

		TRACE_CLUSTER_HANDLER(eventType, DEBUG, "");
	}
	catch (UserException &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, WARNING, "");
	}
	catch (std::exception &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, ERROR, "");
		clsSvc_->setError(ec, &e);
	}
}

/*!
	@brief Handler Operator
*/
void ChangePartitionTableHandler::operator()(EventContext &ec, Event &ev) {
	EventType eventType = ev.getType();
	PartitionId pId = ev.getPartitionId();

	TRACE_SYNC_HANDLER_DETAIL(ev, eventType, pId, DEBUG, "");

	try {
		clsMgr_->checkNodeStatus();

		clsMgr_->checkClusterStatus(OP_CHANGE_PARTITION_TABLE);

		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);

		ClusterManager::ChangePartitionTableInfo changePartitionTableInfo(
			alloc, 0);
		clsSvc_->decode(ev, changePartitionTableInfo);

		TRACE_SYNC_HANDLER(
			eventType, pId, WARNING, changePartitionTableInfo.dump());

		PartitionRole &nextRole = changePartitionTableInfo.getPartitionRole();

		NodeAddress ownerAddress;
		std::vector<NodeAddress> backupsAddress;
		NodeId owner;
		std::vector<NodeId> backups;
		nextRole.get(ownerAddress, backupsAddress);
		owner = ClusterService::getNodeId(ownerAddress, clsSvc_->getEE());
		for (size_t pos = 0; pos < backupsAddress.size(); pos++) {
			NodeId nodeId = ClusterService::getNodeId(
				backupsAddress[pos], clsSvc_->getEE());
			if (nodeId != UNDEF_NODEID) {
				backups.push_back(nodeId);
			}
		}
		nextRole.set(owner, backups);

		PartitionId currentPId = UNDEF_PARTITIONID;
		SyncId currentSyncId;
		PartitionRevision currentRevision;
		bool isPrevCatchup = false;
		bool isPrevCatchupOwner = false;
		if (nextRole.getTableType() == PartitionTable::PT_NEXT_GOAL) {
			syncMgr_->getCurrentSyncId(
				currentPId, currentSyncId, currentRevision);
			syncMgr_->getSyncContext(currentPId, currentSyncId);

			if (currentPId != UNDEF_PARTITIONID) {
				isPrevCatchup = pt_->isBackup(
					currentPId, 0, PartitionTable::PT_CURRENT_GOAL);
				isPrevCatchupOwner = pt_->isOwner(
					currentPId, 0, PartitionTable::PT_CURRENT_GOAL);
			}
		}
		UTIL_TRACE_WARNING(CLUSTER_SERVICE,
			"currentPId:" << currentPId
						  << ", currentSyncId:" << currentSyncId.dump()
						  << ", isPrevCatchup:" << isPrevCatchup
						  << ", isPrevCatchupOwner:" << isPrevCatchupOwner);

		clsMgr_->set(changePartitionTableInfo);
		PartitionTable::TableType tableType =
			changePartitionTableInfo.getPartitionRole().getTableType();

		if (tableType == PartitionTable::PT_NEXT_OB) {
			clsSvc_->requestChangePartitionStatus(
				alloc, pId, PartitionTable::PT_OFF, PT_CHANGE_SYNC_END);
		}

		if (nextRole.getTableType() == PartitionTable::PT_NEXT_GOAL) {
			if (nextRole.getOwner() == UNDEF_NODEID) {
				if (nextRole.getRevision() == currentRevision) {
					SyncContext *context =
						syncMgr_->getSyncContext(pId, currentSyncId);
					if (context != NULL) {
						UTIL_TRACE_WARNING(CLUSTER_SERVICE,
							"Clear catchup info(catchup), pId:"
								<< currentPId << ", ptRev:"
								<< context->getPartitionRevision());
						syncMgr_->removeSyncContext(currentPId, context);
					}
				}
			}
		}
		TRACE_CLUSTER_HANDLER(eventType, DEBUG, "");
	}
	catch (UserException &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, WARNING, "");
	}
	catch (std::exception &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, ERROR, "");
		clsSvc_->setError(ec, &e);
	}
}

/*!
	@brief Handler Operator
*/
void UnknownClusterEventHandler::operator()(EventContext &, Event &ev) {
	EventType eventType = ev.getType();
	try {
		if (eventType >= 0 && eventType <= V_1_1_STATEMENT_END) {
			GS_THROW_USER_ERROR(GS_ERROR_CS_CLUSTER_VERSION_UNMATCHED,
				"Cluster version is unmatched, event type:"
					<< getEventTypeName(eventType) << " is before v1.5");
		}
		else if (eventType >= V_1_5_CLUSTER_START &&
				 eventType <= V_1_5_CLUSTER_END) {
			GS_THROW_USER_ERROR(GS_ERROR_CS_CLUSTER_VERSION_UNMATCHED,
				"Cluster version is unmatched, event type:"
					<< getEventTypeName(eventType) << " is v1.5");
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_CS_SERVICE_UNKNOWN_EVENT_TYPE,
				"Unknown cluster event type:" << eventType);
		}
	}
	catch (std::exception &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, WARNING, "");
	}
}

/*!
	@brief Gets EventEngine object about a specified service type
*/
EventEngine *ClusterService::getEE(ServiceType type) {
	switch (type) {
	case CLUSTER_SERVICE:
		return &ee_;
	case TRANSACTION_SERVICE:
		return txnSvc_->getEE();
	case SYNC_SERVICE:
		return syncSvc_->getEE();
	case SYSTEM_SERVICE:
		return sysSvc_->getEE();
	default:
		GS_THROW_USER_ERROR(GS_ERROR_CS_CLUSTER_INVALID_SERVICE_TYPE, "");
	}
}

/*!
	@brief Shutdown all services
*/
void ClusterService::shutdownAllService(bool isInSytemService) {
	try {
		if (isInSytemService) {
			isSystemServiceError_ = true;
		}

		sysSvc_->shutdown();
		cpSvc_->shutdown();
		txnSvc_->shutdown();
		syncSvc_->shutdown();

		shutdown();

#if defined(SYSTEM_CAPTURE_SIGNAL)
		pid_t pid = getpid();
		kill(pid, SIGTERM);
#endif  

		TRACE_CLUSTER_NORMAL_OPERATION(
			INFO, "[INFO] Shutdown node operation is completed.");
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(CLUSTER_OPERATION, e,
			"[CRITICAL] Unrecoveble error is occured, abort process.");
		abort();
	}
}
