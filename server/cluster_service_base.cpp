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
	@brief Implementation of ClusterService
*/
#include "cluster_service.h"
#include "cluster_manager.h"
#include "picojson.h"

#include "sql_service.h"

#ifndef _WIN32
#define SYSTEM_CAPTURE_SIGNAL
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>
#endif

UTIL_TRACER_DECLARE(CLUSTER_SERVICE);
UTIL_TRACER_DECLARE(CLUSTER_OPERATION);
UTIL_TRACER_DECLARE(CLUSTER_INFO_TRACE);

#define TEST_PRINT(s)
#define TEST_PRINT1(s, d)

const char *ClusterService::NotificationManager::
		CONFIG_ADDRESS = "address";

const char *ClusterService::NotificationManager::
		CONFIG_PORT = "port";

const char *ClusterService::NotificationManager::
		CONFIG_URL = "url";

const char *ClusterService::NotificationManager::
		CONFIG_UPDATE_INTERVAL = "updateInterval";

const char8_t *ServiceConfig::
		TYPE_NAME_CLUSTER = "cluster";

const char8_t *ServiceConfig::
		TYPE_NAME_TRANSACTION = "transaction";

const char8_t *ServiceConfig::
		TYPE_NAME_SYNC = "sync";

const char8_t *ServiceConfig::
		TYPE_NAME_SYSTEM = "system";

const char8_t *ServiceConfig::
		TYPE_NAME_SQL = "sql";

const char8_t *ServiceConfig::
		TYPE_NAME_TRANSACTION_LOCAL = "transactionLocal";

const char8_t *ServiceConfig::
		TYPE_NAME_SQL_LOCAL = "sqlLocal";

const char8_t *const ServiceConfig::
		SERVICE_TYPE_NAMES [] = {
				TYPE_NAME_CLUSTER,
				TYPE_NAME_TRANSACTION,
				TYPE_NAME_SYNC,
				TYPE_NAME_SYSTEM
				,
				TYPE_NAME_SQL
};

const char8_t *const ServiceConfig::
		SERVICE_TYPE_NAMES_WITH_PUBLIC [] = {
				TYPE_NAME_CLUSTER,
				TYPE_NAME_TRANSACTION,
				TYPE_NAME_SYNC,
				TYPE_NAME_SYSTEM
				, 
				TYPE_NAME_SQL
				,
				TYPE_NAME_TRANSACTION_LOCAL
				, 
				TYPE_NAME_SQL_LOCAL
};

ServiceType ServiceConfig::SERVICE_TYPE_LIST [] = {
		CLUSTER_SERVICE,
		TRANSACTION_SERVICE, 
		SYNC_SERVICE,
		SYSTEM_SERVICE
		,
		SQL_SERVICE
};

int32_t ServiceConfig::SERVICE_TYPE_LIST_MAP [] = {
		0, 1, 2, 3, 4
};

ServiceType ServiceConfig::
		SERVICE_TYPE_LIST_WITH_PUBLIC [] = {
				CLUSTER_SERVICE,
				TRANSACTION_SERVICE, 
				SYNC_SERVICE,
				SYSTEM_SERVICE
				,
				SQL_SERVICE
				, 
				TRANSACTION_SERVICE
				, 
				SQL_SERVICE
};

int32_t ServiceConfig::
		SERVICE_TYPE_LIST_MAP_WITH_PUBLIC [] = {
				0, 5, 2, 3, 6, 1, 4
};

const char8_t *ServiceConfig::dumpServiceType(
		ServiceType serviceType) {

	return ServiceConfig::SERVICE_TYPE_NAMES[
			static_cast<size_t>(serviceType)];
}

/*!
	@brief Gets node id of sender ND
*/
NodeId ClusterService::resolveSenderND(Event &ev) {

	const NodeDescriptor &senderNd = ev.getSenderND();
	if (!senderNd.isEmpty()) {
		return static_cast<NodeId>(ev.getSenderND().getId());
	}
	else {
		return SELF_NODEID;
	}
}

ClusterService::ClusterService(
		const ConfigTable &config,
		EventEngine::Config &eeconfig,
		EventEngine::Source source,
		const char8_t *name,
		ClusterManager &clsMgr,
		ClusterVersionId versionId,
		util::VariableSizeAllocator<> &alloc) :
		ee_(createEEConfig(
				config, eeconfig), source, name),
		versionId_(versionId),
		fixedSizeAlloc_(source.fixedAllocator_),
		varSizeAlloc_(NULL),
		clsMgr_(&clsMgr),
		clusterStats_(config.get<int32_t>(
				CONFIG_TABLE_DS_PARTITION_NUM)),
		pt_(clsMgr.getPartitionTable()),
		initailized_(false),
		isSystemServiceError_(false),
		notificationManager_(clsMgr_, alloc) {

	try {

		clsMgr_ = &clsMgr;

		ee_.setHandler(
				CS_UPDATE_PARTITION, updatePartitionHandler_);

		ee_.setHandler(
				CS_JOIN_CLUSTER, systemCommandHandler_);
		ee_.setHandler(
				CS_LEAVE_CLUSTER, systemCommandHandler_);
		ee_.setHandler(
				CS_INCREASE_CLUSTER, clusterSystemCommandHandler_);
		ee_.setHandler(
				CS_DECREASE_CLUSTER, clusterSystemCommandHandler_);

		ee_.setHandler(
				CS_SHUTDOWN_NODE_NORMAL, systemCommandHandler_);
		ee_.setHandler(
				CS_SHUTDOWN_NODE_FORCE, systemCommandHandler_);
		ee_.setHandler(
				CS_SHUTDOWN_CLUSTER, systemCommandHandler_);
		ee_.setHandler(
				CS_COMPLETE_CHECKPOINT_FOR_SHUTDOWN,
						systemCommandHandler_);
		ee_.setHandler(
				CS_COMPLETE_CHECKPOINT_FOR_RECOVERY,
						systemCommandHandler_);

		ee_.setHandler(
				CS_TIMER_CHECK_CLUSTER, timerCheckClusterHandler_);
		Event checkClusterEvent(
				source, CS_TIMER_CHECK_CLUSTER, CS_HANDLER_PARTITION_ID);
		ee_.addPeriodicTimer(
				checkClusterEvent, clsMgr_->getConfig().
						getHeartbeatInterval());

		ee_.setHandler(
				CS_TIMER_NOTIFY_CLIENT, timerNotifyClientHandler_);
		Event notifyClientEvent(
				source, CS_TIMER_NOTIFY_CLIENT, CS_HANDLER_PARTITION_ID);
		ee_.addPeriodicTimer(
				notifyClientEvent, clsMgr_->getConfig().
						getNotifyClusterInterval());

		ee_.setHandler(
				SQL_TIMER_NOTIFY_CLIENT, timerSQLNotifyClientHandler_);
		Event requestNotifyClientEvent(
				source, SQL_TIMER_NOTIFY_CLIENT, CS_HANDLER_PARTITION_ID);
		ee_.addPeriodicTimer(
				requestNotifyClientEvent, changeTimeSecToMill(
						config.get<int32_t>(
								CONFIG_TABLE_SQL_NOTIFICATION_INTERVAL)));
		ee_.setHandler(CS_TIMER_REQUEST_SQL_CHECK_TIMEOUT,
				timerRequestSQLCheckTimeoutHandler_);

		Event requestCheckTimeoutEvent(
				source, CS_TIMER_REQUEST_SQL_CHECK_TIMEOUT,
						CS_HANDLER_PARTITION_ID);
		ee_.addPeriodicTimer(
				requestCheckTimeoutEvent,
						JobManager::DEFAULT_RESOURCE_CHECK_TIME);

		ee_.setHandler(
				CS_NEWSQL_PARTITION_REFRESH,
						newSQLPartitionRefreshHandler_);
		ee_.setHandler(
				CS_NEWSQL_PARTITION_REFRESH_ACK,
						newSQLPartitionRefreshHandler_);
		setClusterHandler(source);

		ee_.setUnknownEventHandler(unknownEventHandler_);
		ee_.setThreadErrorHandler(serviceThreadErrorHandler_);

		util::StackAllocator currentAlloc(
				util::AllocatorInfo(
						ALLOCATOR_GROUP_CS, "clusterService"),
						fixedSizeAlloc_);
		util::StackAllocator::Scope scope(currentAlloc);
		ClusterAdditionalServiceConfig addConfig(config, pt_);

		notificationManager_.initialize(config);
		if (notificationManager_.getMode()
				== NOTIFICATION_RESOLVER) {
			ee_.setHandler(
					CS_UPDATE_PROVIDER, timerNotifyClusterHandler_);
			ee_.setHandler(
					CS_CHECK_PROVIDER, timerNotifyClusterHandler_);

			Event updateResolverEvent(
					source,
					CS_UPDATE_PROVIDER,
					CS_HANDLER_PARTITION_ID);
			ee_.addPeriodicTimer(updateResolverEvent,
					notificationManager_.updateResolverInterval());
		}

		NodeAddressSet &addressInfo
				= notificationManager_.getFixedAddressInfo();
		util::XArray<uint8_t> digestBinary(currentAlloc);
		clsMgr_->setDigest(
				config, digestBinary,
				addressInfo, notificationManager_.getMode());
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Sets EventEngine config
*/
EventEngine::Config &ClusterService::createEEConfig(
		const ConfigTable &config,
		EventEngine::Config &eeConfig) {

	EventEngine::Config tmpConfig;
	eeConfig = tmpConfig;

	eeConfig.setServerNDAutoNumbering(true);

	eeConfig.setPartitionCount(
			config.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM));

	eeConfig.setServerAddress(
			config.get<const char8_t *>(
				CONFIG_TABLE_CS_SERVICE_ADDRESS),
			config.getUInt16(CONFIG_TABLE_CS_SERVICE_PORT));

	if (isMulticastMode(config)) {
		eeConfig.setMulticastAddress(
				config.get<const char8_t *>(
						CONFIG_TABLE_CS_NOTIFICATION_ADDRESS),
				config.getUInt16(
						CONFIG_TABLE_CS_NOTIFICATION_PORT));

		if (strlen(config.get<const char8_t *>(
				CONFIG_TABLE_CS_NOTIFICATION_INTERFACE_ADDRESS)) != 0) {

			eeConfig.setMulticastIntefaceAddress(
					config.get<const char8_t *>(
							CONFIG_TABLE_CS_NOTIFICATION_INTERFACE_ADDRESS),
					config.getUInt16(CONFIG_TABLE_CS_SERVICE_PORT));
		}
	}

	eeConfig.setAllAllocatorGroup(ALLOCATOR_GROUP_CS);
	return eeConfig;
}

ClusterService::~ClusterService() {
	ee_.shutdown();
	ee_.waitForShutdown();
}

void ClusterService::initialize(ResourceSet &resourceSet) {

	try {
		resourceSet_ = &resourceSet;
		clsMgr_->initialize(this, &ee_ );

		pt_ = resourceSet.pt_;
		ConfigTable *config = resourceSet.config_;
		varSizeAlloc_ = resourceSet.varSizeAlloc_;

		resourceSet.stats_->addUpdator(
				&clsMgr_->getStatUpdator());

		heartbeatHandler_.initialize(resourceSet);
		notifyClusterHandler_.initialize(resourceSet);
		updatePartitionHandler_.initialize(resourceSet);
		systemCommandHandler_.initialize(resourceSet);
		timerCheckClusterHandler_.initialize(resourceSet);
		timerNotifyClusterHandler_.initialize(resourceSet);
		timerNotifyClientHandler_.initialize(resourceSet);

		timerSQLNotifyClientHandler_.initialize(resourceSet);
		timerRequestSQLCheckTimeoutHandler_.initialize(resourceSet);
		newSQLPartitionRefreshHandler_.initialize(resourceSet);

		ClusterAdditionalServiceConfig addConfig(*config);
		unknownEventHandler_.initialize(resourceSet);
		serviceThreadErrorHandler_.initialize(resourceSet);

		NodeAddress clusterAddress, txnAddress, syncAddress;
		NodeAddress sqlAddress;
		ClusterService::changeAddress(
				clusterAddress, SELF_NODEID, getEE(CLUSTER_SERVICE));
		ClusterService::changeAddress(
				txnAddress, SELF_NODEID, getEE(TRANSACTION_SERVICE));
		ClusterService::changeAddress(
				syncAddress, SELF_NODEID, getEE(SYNC_SERVICE));
		ClusterService::changeAddress(
				sqlAddress, SELF_NODEID, getEE(SQL_SERVICE));

		EventEngine::Config tmp;
		tmp.setServerAddress(
				config->get<const char8_t *>(
						CONFIG_TABLE_SYS_SERVICE_ADDRESS),
				config->getUInt16(CONFIG_TABLE_SYS_SERVICE_PORT));

		util::SocketAddress::Inet sysAddr;
		uint16_t sysPort;
		tmp.serverAddress_.getIP(&sysAddr, &sysPort);

		AddressType sysAddrType;
		memcpy(&sysAddrType, &sysAddr, sizeof(sysAddrType));
		NodeAddress systemAddress(sysAddrType, sysPort);

		pt_->setNodeInfo(SELF_NODEID,
				CLUSTER_SERVICE, clusterAddress);

		pt_->setNodeInfo(SELF_NODEID,
				TRANSACTION_SERVICE, txnAddress);
		
		pt_->setNodeInfo(SELF_NODEID,
				SYNC_SERVICE, syncAddress);
		
		pt_->setNodeInfo(SELF_NODEID,
				SYSTEM_SERVICE, systemAddress);

		pt_->setNodeInfo(SELF_NODEID,
				SQL_SERVICE, sqlAddress);

		if (notificationManager_.getMode() == NOTIFICATION_FIXEDLIST) {
			NodeAddressSet &addressInfo
					= notificationManager_.getFixedAddressInfo();
			if (!addConfig.isSetServiceAddress()) {
					GS_TRACE_WARNING(
							CLUSTER_OPERATION, GS_TRACE_CS_OPERATION,
							"Recommended specify fixed list service address");
			}
			bool selfIncluded = true;
			bool unmatchAddress = false;
			const NodeAddress *failAddress = NULL;
			const NodeAddress *correctAddress = NULL;
			ServiceType failType;
			for (NodeAddressSet::iterator it = addressInfo.begin();
					it != addressInfo.end(); it++) {
				if ((*it).clusterAddress_ == clusterAddress) {
					if ((*it).transactionAddress_ == txnAddress) {
					}
					else {
						unmatchAddress = true;
						failAddress = &(*it).transactionAddress_;
						correctAddress = &txnAddress;
						failType = TRANSACTION_SERVICE;
						break;
					}
					if ((*it).syncAddress_ == syncAddress) {
					}
					else {
						unmatchAddress = true;
						failAddress = &(*it).syncAddress_;
						correctAddress = &syncAddress;
						failType = SYNC_SERVICE;
						break;
					}
					if ((*it).systemAddress_ == systemAddress) {
					}
					else {
						unmatchAddress = true;
						failAddress = &(*it).systemAddress_;
						correctAddress = &systemAddress;
						failType = SYSTEM_SERVICE;
						break;
					}
					if ((*it).sqlAddress_ == sqlAddress) {
					}
					else {
						unmatchAddress = true;
						failAddress = &(*it).sqlAddress_;
						correctAddress = &sqlAddress;
						failType = SQL_SERVICE;
						break;
					}
					selfIncluded = true;
				}
			}
			if (!selfIncluded) {
				GS_THROW_USER_ERROR(GS_ERROR_CS_CONFIG_ERROR,
					"Failed to check notification list in config. "
					"Self cluster address is not included in this list "
						"(address = " << clusterAddress.toString() << ")");
			}
			else if (unmatchAddress) {
				GS_THROW_USER_ERROR(GS_ERROR_CS_CONFIG_ERROR,
					"Failed to check notification list in config. "
					"Self service address (" 
					<< ServiceConfig::dumpServiceType(failType) << ") is not included in this list "
						"(expected=" << (*correctAddress).toString() 
						<< ", actual=" << (*failAddress).toString() << ")");
			}
			updateNodeList(addressInfo,
					notificationManager_.getPublicFixedAddressInfo());

			if (pt_->getNodeNum()
					!= notificationManager_.getFixedNodeNum()) {
				GS_THROW_USER_ERROR(GS_ERROR_CS_CONFIG_ERROR,
					"Failed to check notification member. "
					"Duplicate member is included in this list");
			}
		}
		pt_->getPartitionRevision().set(
				clusterAddress.address_, clusterAddress.port_);
		initailized_ = true;
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Starts ClusterService
*/
void ClusterService::start(const Event::Source &eventSource) {
	try {
		if (!initailized_) {
			GS_THROW_USER_ERROR(
					GS_ERROR_CS_SERVICE_NOT_INITIALIZED, "");
		}
		ee_.start();

		const ClusterNotificationMode mode
				= notificationManager_.getMode();
		if (mode == NOTIFICATION_RESOLVER) {
			Event checkResolverEvent(
					eventSource, CS_CHECK_PROVIDER,
							CS_HANDLER_PARTITION_ID);
			ee_.addTimer(checkResolverEvent,
				notificationManager_.checkResolverInterval());
		}
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
	@brief Encodes message for cluster service
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
					e, GS_EXCEPTION_MERGE_MESSAGE(
							e, "Failed to encode message"));
		}
		uint32_t packedSize = static_cast<uint32_t>(buffer.size());
		if (packedSize == 0) {
				GS_THROW_USER_ERROR(
						GS_ERROR_CS_ENCODE_DECODE_VALIDATION_CHECK,
						"Invalid message size(0)");
		}
		out << packedSize;
		out.writeAll(buffer.data(), packedSize);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
}

template <class T>
void ClusterService::encode(T &t, EventByteOutStream &out) {
	try {
		out << versionId_;
		msgpack::sbuffer buffer;
		try {
			msgpack::pack(buffer, t);
		}
		catch (std::exception &e) {
			GS_RETHROW_USER_ERROR(
				e, GS_EXCEPTION_MERGE_MESSAGE(
						e, "Failed to encode message"));
		}
		uint32_t packedSize = static_cast<uint32_t>(buffer.size());
		if (packedSize == 0) {
			GS_THROW_USER_ERROR(
					GS_ERROR_CS_ENCODE_DECODE_VALIDATION_CHECK,
				"Invalid message size(0)");
		}
		out << packedSize;
		out.writeAll(buffer.data(), packedSize);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
}


/*!
	@brief Checks the version of message for cluster service
*/
void ClusterService::checkVersion(uint8_t versionId) {
	if (versionId != versionId_) {
		GS_THROW_USER_ERROR(
				GS_ERROR_CS_ENCODE_DECODE_VERSION_CHECK,
			"Cluster message version is unmatched, acceptableClusterVersion:"
				<< static_cast<int32_t>(versionId_)
				<< ", connectClusterVersion:"
				<< static_cast<int32_t>(versionId));
	}
}

/*!
	@brief Decodes message for cluster service
*/
template <class T>
void ClusterService::decode(Event &ev, T &t) {

	try {
		EventByteInStream in = ev.getInStream();
		const uint8_t *eventBuffer
				= ev.getMessageBuffer().getXArray().data()
						+ ev.getMessageBuffer().getOffset();
		if (eventBuffer == NULL) {
			GS_THROW_USER_ERROR(
					GS_ERROR_CS_SERVICE_DECODE_MESSAGE_FAILED,
					"Event initial buffer is null");
		}
		uint8_t decodedVersionId;
		in >> decodedVersionId;
		checkVersion(decodedVersionId);
		uint32_t packedSize;
		in >> packedSize;
		const char8_t *bodyBuffer = reinterpret_cast<
				const char8_t *>(eventBuffer + in.base().position());
		if (bodyBuffer == NULL) {
			GS_THROW_USER_ERROR(
					GS_ERROR_CS_SERVICE_DECODE_MESSAGE_FAILED,
					"Event body buffer is null");
		}
		uint32_t remainSize = static_cast<int32_t>(in.base().remaining());
		if (remainSize != packedSize) {
			GS_THROW_USER_ERROR(
					GS_ERROR_CS_SERVICE_DECODE_MESSAGE_FAILED,
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
					e, GS_EXCEPTION_MERGE_MESSAGE(
							e, "Failed to decode message"));
		}
		t.check();
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
}

template <class T>
void ClusterService::decode(
		Event &ev, T &t, EventByteInStream &in) {

	try {
		const uint8_t *eventBuffer
				= ev.getMessageBuffer().getXArray().data()
						+ ev.getMessageBuffer().getOffset();
		if (eventBuffer == NULL) {
			GS_THROW_USER_ERROR(
					GS_ERROR_CS_SERVICE_DECODE_MESSAGE_FAILED,
				"Event initial buffer is null");
		}

		uint8_t decodedVersionId;
		in >> decodedVersionId;
		checkVersion(decodedVersionId);
		uint32_t packedSize;
		in >> packedSize;
		const char8_t *bodyBuffer = reinterpret_cast<const char8_t *>(
				eventBuffer + in.base().position());
		if (bodyBuffer == NULL) {
			GS_THROW_USER_ERROR(
					GS_ERROR_CS_SERVICE_DECODE_MESSAGE_FAILED,
					"Event body buffer is null");
		}
		uint32_t lastSize = static_cast<uint32_t>(
				in.base().position()) + packedSize;
		try {
			msgpack::unpacked msg;
			msgpack::unpack(&msg, bodyBuffer, packedSize);
			msgpack::object obj = msg.get();
			obj.convert(&t);
		}
		catch (std::exception &e) {
			GS_RETHROW_USER_ERROR(
				e, GS_EXCEPTION_MERGE_MESSAGE(
						e, "Failed to decode message"));
		}
		in.base().position(lastSize);
		t.check();
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
}


/*!
	@brief Handles system error
*/
void ClusterService::setSystemError(std::exception *e) {

	try {
		if (e != NULL) {
			UTIL_TRACE_EXCEPTION_ERROR(CLUSTER_OPERATION, *e,
				GS_EXCEPTION_MERGE_MESSAGE(
						*e, "[ERROR] SetError requested"));
		}
		if (!clsMgr_->setSystemError()) {
			UTIL_TRACE_WARNING(CLUSTER_SERVICE,
					"Already reported system error.");
			return;
		}

		bool isAutoShutdown
				= clsMgr_->getConfig().isAutoShutdown();

		if (isAutoShutdown) {
			GS_TRACE_INFO(CLUSTER_OPERATION,
					GS_TRACE_CS_CLUSTER_STATUS,
					"Execute auto shutdown by detecting"
					" abnormal node status, trace current stats");
			try {
				resourceSet_->getSystemService()->traceStats();
			}
			catch (std::exception &e) {
				UTIL_TRACE_EXCEPTION(CLUSTER_SERVICE, e, "");
			}
		}

		executeShutdown(isAutoShutdown);
	
		for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
			pt_->setPartitionStatus(pId, PartitionTable::PT_STOP);
		}
		clsMgr_->updateClusterStatus(TO_SUBMASTER, true);
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(CLUSTER_OPERATION, e,
			"[CRITICAL] Unrecoverable error is occurred, abort process, reason="
			<< GS_EXCEPTION_MESSAGE(e));
		abort();
	}
}

/*!
	@brief Requests ChangePartitionStatus
*/
void ClusterService::requestChangePartitionStatus(
		EventContext &ec,
		util::StackAllocator &alloc,
		PartitionId pId,
		PartitionStatus status) {

	try {
		Event requestEvent(ec, TXN_CHANGE_PARTITION_STATE, pId);
		ChangePartitionStatusInfo
				changePartitionStatusInfo(alloc, 0, pId, status,
						true, PT_CHANGE_NORMAL);

		encode(requestEvent, changePartitionStatusInfo);
		resourceSet_->getTransactionService()->getEE()->addTimer(
					requestEvent, EE_PRIORITY_HIGH);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Executes ChangePartitionStatus directly in the same thread
*/
void ClusterService::requestChangePartitionStatus(
		EventContext &ec,
		util::StackAllocator &alloc,
		PartitionId pId,
		PartitionStatus status,
		ChangePartitionType changePartitionType) {

	UNUSED_VARIABLE(ec);

	try {
		ChangePartitionStatusInfo
				changePartitionStatusInfo(
						alloc, 0, pId, status, false, changePartitionType);

		resourceSet_->getTransactionService()->
				changeTimeoutCheckMode(
						pId, status, changePartitionType,
						changePartitionStatusInfo.isToSubMaster(),
						getStats());

		clsMgr_->setChangePartitionStatusInfo(
				changePartitionStatusInfo);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

template void ClusterService::request(
		const Event::Source &eventSource, EventType eventType,
		PartitionId pId, EventEngine *targetEE, LongtermSyncInfo &t);

/*!
	@brief Requests checkpoint completed
*/
void ClusterService::requestCompleteCheckpoint(
		const Event::Source &eventSource,
		util::StackAllocator &alloc,
		bool isRecovery) {

	try {
		util::StackAllocator::Scope scope(alloc);
		CompleteCheckpointInfo
				completeCpInfo(alloc, 0);

		EventType eventType;
		if (isRecovery) {
			eventType = CS_COMPLETE_CHECKPOINT_FOR_RECOVERY;
		}
		else {
			eventType = CS_COMPLETE_CHECKPOINT_FOR_SHUTDOWN;
		}
		request(eventSource, eventType, CS_HANDLER_PARTITION_ID,
				&ee_, completeCpInfo);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(
				e, "Failed to request checkpoint completion: isRecovery="
						<< isRecovery));
	}
}

bool ClusterService::isError() {
	return clsMgr_->isError();
}

void TimerNotifyClientHandler::encode(Event &ev) {
	try {
		EventByteOutStream out = ev.getOutStream();
		const ContainerHashMode hashType
				= CONTAINER_HASH_MODE_CRC32;
		util::SocketAddress::Inet address;
		uint16_t port;
		const NodeDescriptor &nd
				= clsSvc_->getEE(TRANSACTION_SERVICE)->getSelfServerND();
		if (nd.isEmpty()) {
			GS_THROW_USER_ERROR(
					GS_ERROR_EE_PARAMETER_INVALID, "ND is empty");
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
	@brief Encodes message for client
*/
void ClusterService::encodeNotifyClient(Event &ev) {
	try {
		EventByteOutStream out = ev.getOutStream();
		const ContainerHashMode hashType
				= CONTAINER_HASH_MODE_CRC32;
		util::SocketAddress::Inet address;
		uint16_t port;
		const NodeDescriptor &nd
				= getEE(TRANSACTION_SERVICE)->getSelfServerND();
		if (nd.isEmpty()) {
			GS_THROW_USER_ERROR(
					GS_ERROR_EE_PARAMETER_INVALID, "ND is empty");
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
		targetEE->add(requestEvent);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

template void ClusterService::request(const Event::Source &eventSource,
	EventType eventType, PartitionId pId, EventEngine *targetEE,
	JoinClusterInfo &t);

template void ClusterService::request(const Event::Source &eventSource,
	EventType eventType, PartitionId pId, EventEngine *targetEE,
	LeaveClusterInfo &t);

template void ClusterService::request(const Event::Source &eventSource,
	EventType eventType, PartitionId pId, EventEngine *targetEE,
	IncreaseClusterInfo &t);

template void ClusterService::request(const Event::Source &eventSource,
	EventType eventType, PartitionId pId, EventEngine *targetEE,
	DecreaseClusterInfo &t);

template void ClusterService::request(const Event::Source &eventSource,
	EventType eventType, PartitionId pId, EventEngine *targetEE,
	ShutdownNodeInfo &t);

template void ClusterService::request(const Event::Source &eventSource,
	EventType eventType, PartitionId pId, EventEngine *targetEE,
	ShutdownClusterInfo &t);

/*!
	@brief Handler Operator
*/
void SystemCommandHandler::operator()(
		EventContext &ec, Event &ev) {

	EventType eventType = ev.getType();

	try {
		if (eventType != CS_SHUTDOWN_NODE_FORCE) {
			clsMgr_->checkNodeStatus();
		}
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);
		NodeId senderNodeId = ClusterService::resolveSenderND(ev);

		switch (eventType) {

			case CS_JOIN_CLUSTER: {
				JoinClusterInfo
						joinClusterInfo(alloc, senderNodeId, false);
				clsSvc_->decode(ev, joinClusterInfo);
				doJoinCluster(joinClusterInfo, ec);
				break;
			}
			
			case CS_LEAVE_CLUSTER: {
				LeaveClusterInfo
						leaveClusterInfo(alloc, senderNodeId);
				clsSvc_->decode(ev, leaveClusterInfo);
				doLeaveCluster(leaveClusterInfo, ec);
				break;
			}
			case CS_SHUTDOWN_NODE_FORCE: {
				ShutdownNodeInfo
						shutdownNodeInfo(alloc, senderNodeId, true);
				clsSvc_->decode(ev, shutdownNodeInfo);
				doShutdownNodeForce();
				break;
			}
			case CS_SHUTDOWN_NODE_NORMAL: {
				ShutdownNodeInfo
						shutdownNodeInfo(alloc, senderNodeId, false);
				clsSvc_->decode(ev, shutdownNodeInfo);
				doShutdownNormal(
						shutdownNodeInfo, ec);
				break;
			}
			case CS_SHUTDOWN_CLUSTER: {
				doShutdownCluster(ec, senderNodeId);
				break;
			}
			case CS_COMPLETE_CHECKPOINT_FOR_SHUTDOWN: {
				doCompleteCheckpointForShutdown();
				break;
			}
			case CS_COMPLETE_CHECKPOINT_FOR_RECOVERY: {
				doCompleteCheckpointForRecovery();
				break;
			}
			default: {
				GS_THROW_USER_ERROR(
						GS_ERROR_CS_INVALID_CLUSTER_OPERATION_TYPE, "");
				break;
			}
		}
	}
	catch (UserException &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, WARNING, "");
	}
	catch (std::exception &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, ERROR, "");
		clsSvc_->setSystemError(&e);
	}
}

void TimerCheckClusterHandler::sendHeartbeat(
		EventContext &ec,
		HeartbeatInfo &heartbeatInfo,
		HeartbeatCheckInfo &heartbeatCheckInfo) {

	NodeIdList &activeNodeList
			= heartbeatCheckInfo.getActiveNodeList();
	int32_t activeNodeListSize
			= static_cast<int32_t>(activeNodeList.size());

	if (!clsMgr_->isInitialCluster()) {
		pt_->incPartitionRevision();
	}
	if (activeNodeListSize > 1) {
		Event heartbeatEvent(
				ec, CS_HEARTBEAT, CS_HANDLER_PARTITION_ID);
		EventByteOutStream out = heartbeatEvent.getOutStream();
		clsSvc_->encode(heartbeatInfo, out);
		if (pt_->hasPublicAddress()
				&& heartbeatInfo.getNodeAddressList().size() > 0) {
			PublicAddressInfoMessage publicAddressInfo;
			pt_->getPublicNodeAddress(
					publicAddressInfo.getPublicNodeAddressList());
			clsSvc_->encode(publicAddressInfo, out);
		}
		EventEngine *clsEE = clsSvc_->getEE();
		for (NodeId nodeId = 1; nodeId < activeNodeListSize;nodeId++) {
			pt_->setAckHeartbeat(activeNodeList[nodeId], false);
			const NodeDescriptor &nd
					= clsEE->getServerND(activeNodeList[nodeId]);
			if (!nd.isEmpty()) {
				clsEE->send(heartbeatEvent, nd);
			}
		}
	}
}

void TimerCheckClusterHandler::sendUpdatePartitionInfo(
		EventContext &ec,
		UpdatePartitionInfo &updatePartitionInfo,
		NodeIdList &activeNodeList) {

	if (updatePartitionInfo.isNeedUpdatePartition()) {
		Event updatePartitionEvent(
				ec, CS_UPDATE_PARTITION, CS_HANDLER_PARTITION_ID);
		EventByteOutStream out = updatePartitionEvent.getOutStream();
		clsSvc_->encode(updatePartitionInfo, out);

		if (updatePartitionInfo.dropPartitionNodeInfo_.getSize() > 0
				|| pt_->hasPublicAddress()) {
			clsSvc_->encode(
					updatePartitionInfo.dropPartitionNodeInfo_, out);
		}
		if (pt_->hasPublicAddress()) {
			PublicAddressInfoMessage publicAddressInfo;
			pt_->getPublicNodeAddress(
					publicAddressInfo.getPublicNodeAddressList());
			clsSvc_->encode(publicAddressInfo, out);
		}
		EventEngine *clsEE = clsSvc_->getEE();
		for (NodeIdList::iterator it = activeNodeList.begin();
			it != activeNodeList.end(); it++) {
			NodeId nodeId = (*it);
			if (nodeId == 0) {
				clsEE->add(updatePartitionEvent);
			}
			else {
				const NodeDescriptor &nd = clsSvc_->getEE()->getServerND(nodeId);
				if (!nd.isEmpty()) {
					clsEE->send(updatePartitionEvent, nd);
				}
			}
		}
	}
}

void TimerCheckClusterHandler::doAfterHeartbeatCheckInfo(
		EventContext &ec,
		HeartbeatCheckInfo &heartbeatCheckInfo) {

	util::StackAllocator &alloc = heartbeatCheckInfo.getAllocator();
	ClusterStatusTransition nextTransition
			= heartbeatCheckInfo.getNextTransition();
	if (nextTransition == TO_SUBMASTER) {
		for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
			clsSvc_->requestChangePartitionStatus(
					ec, alloc, pId, PartitionTable::PT_OFF);
		}
		resourceSet_->getSQLService()->requestCancel(ec);
	}
	clsMgr_->updateClusterStatus(nextTransition);
}

void TimerCheckClusterHandler::doAfterUpdatePartitionInfo(
		EventContext &ec,
		HeartbeatInfo &heartbeatInfo,
		HeartbeatCheckInfo &heartbeatCheckInfo) {

	util::StackAllocator &alloc = ec.getAllocator();
	sendHeartbeat(ec, heartbeatInfo, heartbeatCheckInfo);

	if (pt_->isMaster()) {
		bool clusterChange = clsMgr_->isUpdatePartition();
		ClusterStatusTransition nextTransition
				= heartbeatCheckInfo.getNextTransition();
		UpdatePartitionInfo updatePartitionInfo(
				alloc, 0, pt_, heartbeatCheckInfo.isAddNewNode(),
				(nextTransition != KEEP) || clusterChange);
		if (clusterChange) {
			updatePartitionInfo.setAddOrDownNode();
		}
		if (nextTransition != KEEP || heartbeatInfo.isAddNewNode()) {
			updatePartitionInfo.setToMaster();
		}
		clsMgr_->getUpdatePartitionInfo(updatePartitionInfo);
		sendUpdatePartitionInfo(ec, updatePartitionInfo,
				heartbeatCheckInfo.getActiveNodeList());
	}
}

/*!
	@brief Handler Operator
*/
void TimerCheckClusterHandler::operator()(
		EventContext &ec, Event &ev) {

	EventType eventType = ev.getType();
	try {
		clsMgr_->checkNodeStatus();
		try {
			clsMgr_->checkClusterStatus(CS_TIMER_CHECK_CLUSTER);
		}
		catch (UserException &) {
			return;
		}
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);


		HeartbeatCheckInfo heartbeatCheckInfo(alloc);
		clsMgr_->getHeartbeatCheckInfo(heartbeatCheckInfo);
		doAfterHeartbeatCheckInfo(ec, heartbeatCheckInfo);
		if (!pt_->isFollower()) {
			HeartbeatInfo heartbeatInfo(
					alloc, 0, pt_, heartbeatCheckInfo.isAddNewNode());
			clsMgr_->setHeartbeatInfo(heartbeatInfo);
			clsMgr_->getHeartbeatInfo(heartbeatInfo);
			doAfterUpdatePartitionInfo(
					ec, heartbeatInfo, heartbeatCheckInfo);
		}
	}
	catch (UserException &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, WARNING, "");
	}
	catch (std::exception &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, ERROR, "");
		clsSvc_->setSystemError(&e);
	}
}


void UpdatePartitionHandler::checkAndRequestDropPartition(
		EventContext &ec,
		UpdatePartitionInfo &updatePartitionInfo)  {

	util::StackAllocator &alloc = ec.getAllocator();
	DropPartitionNodeInfo &dropPartitionNodeInfo
			= updatePartitionInfo.dropPartitionNodeInfo_;
	dropPartitionNodeInfo.init();
	
	for (size_t pos = 0;
			pos < dropPartitionNodeInfo.getSize(); pos++) {
		DropNodeSet *dropInfo = dropPartitionNodeInfo.get(pos);
		NodeAddress &selfAddress
				= pt_->getNodeAddress(SELF_NODEID);
		if (dropInfo->getNodeAddress() == selfAddress) {
			for (size_t i = 0; i < dropInfo->pIdList_.size(); i++) {
				resourceSet_->getSyncService()->requestDrop(
						ec, alloc, dropInfo->pIdList_[i], false,
						dropInfo->revisionList_[i]);
			}
			break;
		}
	}
}

void UpdatePartitionHandler::updateNodeInfo(
		EventContext &ec,
		UpdatePartitionInfo &updatePartitionInfo) {

	bool needCancel = false;
	SubPartitionTable &subPartitionTable
			= updatePartitionInfo.getSubPartitionTable();
	clsSvc_->updateNodeList(
			subPartitionTable.getNodeAddressList(),
			subPartitionTable.getPublicNodeAddressList());

	for (size_t pos = 0; pos < subPartitionTable.size(); pos++) {
		resolveAddress(subPartitionTable.getSubPartition(pos));
	}

	if (pt_->isFollower()) {
		LsnList &maxLsnList = updatePartitionInfo.getMaxLsnList();
		for (PartitionId pId = 0; pId < maxLsnList.size(); pId++) {
			pt_->setRepairedMaxLsn(pId, maxLsnList[pId]);
		}
		AddressInfoList &addressInfoList
				= subPartitionTable.getNodeAddressList();
		NodeId masterNodeId = pt_->getMaster();
		for (size_t pos = 0; pos < addressInfoList.size(); pos++) {
			NodeId nodeId = ClusterService::changeNodeId(
				addressInfoList[pos].clusterAddress_, clsSvc_->getEE());
			if (nodeId == 0 || nodeId == masterNodeId) {
			}
			else {
				if (addressInfoList[pos].isActive_) {
					if (pt_->getHeartbeatTimeout(nodeId) == UNDEF_TTL) {
						needCancel = true;
					}
					pt_->setHeartbeatTimeout(nodeId, NOT_UNDEF_TTL);
				}
				else {
					if (pt_->getHeartbeatTimeout(nodeId) != UNDEF_TTL) {
						needCancel = true;
					}
					pt_->setHeartbeatTimeout(nodeId, UNDEF_TTL);
				}
			}
		}
	}
	else if (pt_->isMaster()) {
		needCancel = clsMgr_->isAddOrDownNode();
	}
	if (needCancel) {
		resourceSet_->getSQLService()->requestCancel(ec);
	}
}
	
void ClusterHandler::resolveAddress(PartitionRole &role) {

	NodeAddress nextOwnerAddress;
	std::vector<NodeAddress> backupAddressList;
	std::vector<NodeAddress> catchupAddressList;
	role.get(
			nextOwnerAddress, backupAddressList, catchupAddressList);
	EventEngine *clsEE = clsSvc_->getEE();
	NodeId nextOwnerId = ClusterService::changeNodeId(
			nextOwnerAddress, clsEE);
	NodeIdList nextBackups, nextCatchups;
	
	for (int32_t backupPos = 0;
		backupPos < static_cast<int32_t>(backupAddressList.size());
		backupPos++) {
		NodeId nextBackupId = ClusterService::changeNodeId(
			backupAddressList[backupPos], clsEE);
		if (nextBackupId == UNDEF_NODEID) {
			GS_THROW_USER_ERROR(
					GS_ERROR_PT_INVALID_NODE_ADDRESS,
					"Specify invalid node address="
					<< backupAddressList[backupPos].dump());
		}
		nextBackups.push_back(nextBackupId);
	}

	for (int32_t cachupPos = 0;
			cachupPos < static_cast<int32_t>(catchupAddressList.size());
			cachupPos++) {
		NodeId nextCatchupId = ClusterService::changeNodeId(
				catchupAddressList[cachupPos], clsEE);
		if (nextCatchupId == UNDEF_NODEID) {
			GS_THROW_USER_ERROR(
					GS_ERROR_PT_INVALID_NODE_ADDRESS, 
					"Specify invalid node address"
					<< catchupAddressList[cachupPos].dump());
		}
		nextCatchups.push_back(nextCatchupId);
	}
	role.set(nextOwnerId, nextBackups, nextCatchups);
}

bool UpdatePartitionHandler::resolveAddress(
		SubPartition &subPartition) {

	ClusterHandler::resolveAddress(subPartition.role_);
	NodeId currentOwner = ClusterService::changeNodeId(
			subPartition.currentOwner_, clsSvc_->getEE());
	subPartition.currentOwnerId_ = currentOwner;
	return (currentOwner == 0);
}

void UpdatePartitionHandler::requestSync(
		EventContext &ec,
		PartitionId pId,
		PartitionRole &role,
		bool isShorttermSync) {

	EventType eventType = TXN_SHORTTERM_SYNC_REQUEST;
	SyncMode syncMode = MODE_SHORTTERM_SYNC;

	if (!isShorttermSync) {
		eventType = TXN_LONGTERM_SYNC_REQUEST;
		syncMode = MODE_LONGTERM_SYNC;
	}
	
	util::StackAllocator &alloc = ec.getAllocator();

	SyncRequestInfo syncRequestInfo(
			alloc, eventType,
			resourceSet_->getSyncManager(),
			pId, clsSvc_->getEE(), syncMode);

	syncRequestInfo.setPartitionRole(role);
	Event requestEvent(ec, eventType, pId);
	EventByteOutStream out = requestEvent.getOutStream();

	resourceSet_->getSyncService()->encode(
			requestEvent, syncRequestInfo, out);
	resourceSet_->getTransactionService()->getEE()
			->addTimer(requestEvent, EE_PRIORITY_HIGH);
}


void UpdatePartitionHandler::decode(
		EventContext &ec, Event &ev,
		UpdatePartitionInfo &updatePartitionInfo) {

	EventByteInStream in = ev.getInStream();
	clsSvc_->decode(ev, updatePartitionInfo, in);
	if (in.base().remaining()) {
		clsSvc_->decode(
				ev, updatePartitionInfo.dropPartitionNodeInfo_, in);
		checkAndRequestDropPartition(ec, updatePartitionInfo);
		
		if (in.base().remaining()) {
			PublicAddressInfoMessage publicAddressInfo;
			clsSvc_->decode(ev, publicAddressInfo, in);
			updatePartitionInfo.getSubPartitionTable()
					.setPublicNodeAddressList(
							publicAddressInfo.getPublicNodeAddressList());
		}
	}
}

/*!
	@brief Handler Operator
*/
void UpdatePartitionHandler::operator()(
		EventContext &ec, Event &ev) {

	EventType eventType = ev.getType();
	
	SyncManager *syncMgr
			= resourceSet_->getSyncManager();
	TransactionService *txnSvc
			= resourceSet_->getTransactionService();

	try {
		clsMgr_->checkNodeStatus();
		clsMgr_->checkClusterStatus(CS_UPDATE_PARTITION);
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);
		NodeId senderNodeId = ClusterService::resolveSenderND(ev);
		UpdatePartitionInfo
				updatePartitionInfo(alloc, senderNodeId, pt_);

		decode(ec, ev, updatePartitionInfo);

		bool traceFlag 
				= (updatePartitionInfo.dropPartitionNodeInfo_.getSize() > 0);
		updateNodeInfo(ec, updatePartitionInfo);
		SubPartitionTable &subPartitionTable
				= updatePartitionInfo.getSubPartitionTable();
		clsMgr_->setUpdatePartitionInfo(updatePartitionInfo);
		util::Vector<PartitionId> &shorttermSyncPIdList
				= updatePartitionInfo.getShorttermSyncPIdList();
		util::Vector<PartitionId> &longtermSyncPIdList
				= updatePartitionInfo.getLongtermSyncPIdList();
		util::Vector<PartitionId> &changePartitionPIdList
				= updatePartitionInfo.getChangePartitonPIdList();

		EventEngine *clsEE = clsSvc_->getEE();

		for (size_t pos = 0;
				pos < shorttermSyncPIdList.size(); pos++) {
		
			PartitionRole &role
					= subPartitionTable.getSubPartition(
							shorttermSyncPIdList[pos]).role_;
			PartitionId pId = role.getPartitionId();
			
			SyncRequestInfo syncRequestInfo(alloc,
					TXN_SHORTTERM_SYNC_REQUEST,
					syncMgr, pId, clsEE, MODE_SHORTTERM_SYNC);

			requestSync(ec, pId, role, true);
			traceFlag = true;
		}

		for (size_t pos = 0;
				pos < longtermSyncPIdList.size(); pos++) {

			PartitionRole &role = subPartitionTable.getSubPartition(
					longtermSyncPIdList[pos]).role_;
			PartitionId pId = role.getPartitionId();
			
			SyncRequestInfo syncRequestInfo(alloc,
				TXN_LONGTERM_SYNC_REQUEST, syncMgr, pId, clsEE,
				MODE_LONGTERM_SYNC);
			requestSync(ec, pId, role, false);
			traceFlag = true;
		}

		for (size_t pos = 0;
				pos < changePartitionPIdList.size(); pos++) {

			PartitionRole &role = subPartitionTable.getSubPartition(
					changePartitionPIdList[pos]).role_;
			PartitionId pId = role.getPartitionId();

			ChangePartitionTableInfo
					changePartitionTableInfo(alloc, 0, role);
			clsSvc_->request(ec, TXN_CHANGE_PARTITION_TABLE,
					pId, txnSvc->getEE(), changePartitionTableInfo);
			traceFlag = true;
		}

		if (traceFlag) {
			TRACE_CLUSTER_NORMAL_OPERATION(INFO,
					"Recieve next partition, revision=" 
					<< subPartitionTable.getRevision().sequentialNumber_);
		}
	}
	catch (UserException &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, WARNING, "");
	}
	catch (std::exception &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, ERROR, "");
		clsSvc_->setSystemError(&e);
	}
}

void ClusterHandler::initialize(const ResourceSet &resourceSet) {
	try {
		resourceSet_ = &resourceSet;
		clsSvc_ = resourceSet.clsSvc_;
		pt_ = resourceSet.pt_;
		clsMgr_ = clsSvc_->getManager();
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}


/*!
	@brief Gets node address
*/
void ClusterService::changeAddress(
		NodeAddress &nodeAddress, NodeId nodeId,
		EventEngine *ee) {

	util::SocketAddress::Inet address;
	uint16_t port;
	const NodeDescriptor &nd = ee->getServerND(nodeId);
	if (nd.isEmpty()) {
		GS_THROW_USER_ERROR(
				GS_ERROR_EE_PARAMETER_INVALID, "ND is empty");
	}
	nd.getAddress().getIP(&address, &port);
	nodeAddress.address_
			= *reinterpret_cast<AddressType *>(&address);
	nodeAddress.port_ = port;
}

/*!
	@brief Gets node id
*/
NodeId ClusterService::changeNodeId(
		NodeAddress &nodeAddress, EventEngine *ee) {

	const NodeDescriptor &nd = ee->getServerND(
		util::SocketAddress(
				*(util::SocketAddress::Inet *)&nodeAddress.address_,
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
NodeId ClusterHandler::checkAddress(
		PartitionTable *pt,
		NodeAddress &address,
		NodeId clusterNodeId,
		ServiceType serviceType,
		EventEngine *ee) {

	try {
		if (clusterNodeId != UNDEF_NODEID &&
			pt->checkSetService(clusterNodeId, serviceType)) {
			return clusterNodeId;
		}
		if (serviceType == SYSTEM_SERVICE) {
			pt->setNodeInfo(clusterNodeId, serviceType, address);
			return clusterNodeId;
		}
		util::SocketAddress socketAddress(
				*(util::SocketAddress::Inet *)&address.address_,
						address.port_);

		NodeId nodeId;
		if (serviceType != CLUSTER_SERVICE) {
			ee->setServerNodeId(socketAddress, clusterNodeId, false);
		}
		const NodeDescriptor &nd
				= ee->resolveServerND(socketAddress);
		if (!nd.isEmpty()) {
			nodeId = static_cast<NodeId>(nd.getId());
			pt->setNodeInfo(nodeId, serviceType, address);
			return nodeId;
		}
		else {
			GS_THROW_USER_ERROR(
					GS_ERROR_CS_GET_INVALID_NODE_ADDRESS, "");
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Updates address of node list
*/
template <class T1, class T2>
void ClusterService::updateNodeList(
		T1 &addressInfoList, T2 &publicAddressInfoList) {

	try {
		bool usePublic = (publicAddressInfoList.size() > 0);
		int32_t counter = 0;
		for (typename T1::iterator it = addressInfoList.begin();
			it != addressInfoList.end(); it++, counter++) {
			NodeId clusterNodeId = UNDEF_NODEID, tmpNodeId;
			EventEngine *targetEE;
			for (int32_t serviceType = 0; serviceType < SERVICE_MAX;serviceType++) {
				ServiceType currentServiceType
						= static_cast<ServiceType>(serviceType);
				if (clusterNodeId == 0) {
					break;
				}
				AddressInfo *tmpInfo = const_cast<AddressInfo *>(&(*it));
				NodeAddress &targetAddress
						= tmpInfo->getNodeAddress(currentServiceType);
				if (!targetAddress.isValid()) {
					continue;
				}
				targetEE = getEE(currentServiceType);
				tmpNodeId = ClusterHandler::checkAddress(
						pt_, targetAddress, clusterNodeId,
						currentServiceType, targetEE);
				if (clusterNodeId != UNDEF_NODEID &&
					(clusterNodeId != tmpNodeId)) {
					const NodeDescriptor &nd = targetEE->getSelfServerND();
					targetEE->resetConnection(nd);
					GS_THROW_USER_ERROR(
						GS_ERROR_CS_INVALID_SERVICE_ADDRESS_CONSTRAINT,
						"(clusterNodeId="
							<< clusterNodeId << ", currentNodeId=" << tmpNodeId
							<< ", serviceType=" << serviceType << ")");
				}
				else {
					clusterNodeId = tmpNodeId;
				}
				if (serviceType == 0 && usePublic) {
					pt_->setPublicAddressInfo(
							clusterNodeId, publicAddressInfoList[counter]);
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
void ClusterHandler::checkAutoNdNumbering(
		const NodeDescriptor &nd) {

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
void ChangePartitionStateHandler::operator()(
		EventContext &ec, Event &ev) {

	EventType eventType = ev.getType();
	PartitionId pId = ev.getPartitionId();
	try {
		clsMgr_->checkNodeStatus();
		clsMgr_->checkClusterStatus(TXN_CHANGE_PARTITION_STATE);
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);
		ChangePartitionStatusInfo
				changePartitionStatusInfo(alloc);
		clsSvc_->decode(ev, changePartitionStatusInfo);
		PartitionStatus changeStatus;
		changePartitionStatusInfo.getPartitionStatus(pId, changeStatus);
		resourceSet_->getTransactionService()->changeTimeoutCheckMode(
				pId, changeStatus,
			changePartitionStatusInfo.getPartitionChangeType(),
			changePartitionStatusInfo.isToSubMaster(), clsSvc_->getStats());
		clsMgr_->setChangePartitionStatusInfo(changePartitionStatusInfo);
	}
	catch (UserException &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, WARNING, "");
	}
	catch (std::exception &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, ERROR, "");
		clsSvc_->setSystemError(&e);
	}
}

/*!
	@brief Handler Operator
*/
void ChangePartitionTableHandler::operator()(
		EventContext &ec, Event &ev) {

	EventType eventType = ev.getType();
	PartitionId pId = ev.getPartitionId();
	try {
		clsMgr_->checkNodeStatus();
		clsMgr_->checkClusterStatus(TXN_CHANGE_PARTITION_TABLE);
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);

		ChangePartitionTableInfo
				changePartitionTableInfo(alloc, 0);
		clsSvc_->decode(ev, changePartitionTableInfo);
		PartitionRole &nextRole
				= changePartitionTableInfo.getPartitionRole();
		ClusterHandler::resolveAddress(nextRole);
		if (clsMgr_->setChangePartitionTableInfo(
				changePartitionTableInfo)) {
			resourceSet_->getTransactionService()
					->checkNoExpireTransaction(alloc, pId);
		}
	}
	catch (UserException &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, WARNING, "");
	}
	catch (std::exception &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, ERROR, "");
		clsSvc_->setSystemError(&e);
	}
}

/*!
	@brief Handler Operator
*/
void UnknownClusterEventHandler::operator()(
		EventContext &, Event &ev) {

	EventType eventType = ev.getType();
	try {
		if (eventType >= 0 && eventType <= V_1_1_STATEMENT_END) {
			GS_THROW_USER_ERROR(
					GS_ERROR_CS_CLUSTER_VERSION_UNMATCHED,
					"Cluster version is unmatched, event type:"
					<< getEventTypeName(eventType) << " is before v1.5");
		}
		else if (eventType >= V_1_5_CLUSTER_START &&
				eventType <= V_1_5_CLUSTER_END) {
			GS_THROW_USER_ERROR(
					GS_ERROR_CS_CLUSTER_VERSION_UNMATCHED,
					"Cluster version is unmatched, event type:"
					<< getEventTypeName(eventType) << " is v1.5");
		}
		else {
			GS_THROW_USER_ERROR(
					GS_ERROR_CS_SERVICE_UNKNOWN_EVENT_TYPE,
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
		return resourceSet_->getTransactionService()->getEE();
	case SYNC_SERVICE:
		return resourceSet_->getSyncService()->getEE();
	case SYSTEM_SERVICE:
		return resourceSet_->getSystemService()->getEE();
	case SQL_SERVICE:
		return resourceSet_->getSQLService()->getEE();
	default:
		GS_THROW_USER_ERROR(
				GS_ERROR_CS_CLUSTER_INVALID_SERVICE_TYPE, "");
	}
}

void ClusterService::executeShutdown(bool isForce) {

	resourceSet_->getCheckpointService()->shutdown();
	resourceSet_->getTransactionService()->shutdown();
	resourceSet_->getSyncService()->shutdown();

	resourceSet_->getSQLService()->shutdown();
	
	if (isForce) {
		resourceSet_->getSystemService()->shutdown();
		resourceSet_->getClusterService()->shutdown();

#if defined(SYSTEM_CAPTURE_SIGNAL)
		pid_t pid = getpid();
		kill(pid, SIGTERM);
#endif  
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

		executeShutdown(true);

#if defined(SYSTEM_CAPTURE_SIGNAL)
		pid_t pid = getpid();
		kill(pid, SIGTERM);
#endif  

		TRACE_CLUSTER_NORMAL_OPERATION(
				INFO, "Complete shutdown node operation");
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(CLUSTER_OPERATION, e,
				"Unrecoverable error is occurred, abort process, reason="
			<< GS_EXCEPTION_MESSAGE(e));
		abort();
	}
}

/*!
	@brief Constructor of NotificationManager
*/
ClusterService::NotificationManager::NotificationManager(
		ClusterManager *clsMgr, 
		util::VariableSizeAllocator<> &valloc) :
		clsMgr_(clsMgr),
		pt_(clsMgr->getPartitionTable()),
		localVarAlloc_(valloc),
		mode_(NOTIFICATION_MULTICAST),
		fixedNodeNum_(0),
		resolverUpdateInterval_(0),
		resolverCheckInterval_(DEFAULT_CHECK_INTERVAL),
		resolverCheckLongInterval_(LONG_CHECK_INTERVAL),
		value_(UTIL_NEW picojson::value) {
}

/*!
	@brief Destructor of NotificationManager
*/
ClusterService::NotificationManager::~NotificationManager() {
	for (size_t pos = 0; pos < resolverList_.size(); pos++) {
		delete resolverList_[pos];
	}
	if (value_) {
		delete value_;
	}
}


#define CLUSTER_SET_RESOLVER_ADDRESS(resolver, info, index, type) \
	info.type##Address_.set(                                      \
		resolver->getAddress(index, resolver->getType(#type)))


/*!
	@brief Initializer
*/
void ClusterService::NotificationManager::initialize(
		const ConfigTable &config) {

	try {
		const int32_t notificationInterval =
			config.get<int32_t>(CONFIG_TABLE_CS_NOTIFICATION_INTERVAL);
		const picojson::value &memberValue =
			config.get<picojson::value>(
					CONFIG_TABLE_CS_NOTIFICATION_MEMBER);
		const char8_t *providerURL = config.get<const char8_t *>(
			CONFIG_TABLE_CS_NOTIFICATION_PROVIDER_URL);
		if (!memberValue.is<picojson::null>()) {
			mode_ = NOTIFICATION_FIXEDLIST;
		}
		else if (strlen(providerURL) != 0) {
			mode_ = NOTIFICATION_RESOLVER;
		}
		else {
			mode_ = NOTIFICATION_MULTICAST;
		}
		if (mode_ != NOTIFICATION_MULTICAST) {
			ServiceAddressResolver::Config resolverConfig;
			if (mode_ == NOTIFICATION_RESOLVER) {
				resolverConfig.providerURL_ = providerURL;
			}
			resolverList_.reserve(resolverList_.size() + 1);
			ServiceAddressResolver *resolver =
				UTIL_NEW ServiceAddressResolver(
						localVarAlloc_, resolverConfig);
			resolverList_.push_back(resolver);

			ClusterAdditionalServiceConfig addConfig(config);
			bool usePublicAddress = addConfig.hasPublicAddress();
			for (int32_t i = 0;i < addConfig.serviceTypeSize_;i++) {
				resolver->initializeType(addConfig.serviceTypeList_[i],
						addConfig.serviceTypeNameList_[i]);
			}

			if (mode_ == NOTIFICATION_FIXEDLIST) {
				resolver->importFrom(memberValue);
				resolver->normalize();
				const size_t count = resolver->getEntryCount();
				fixedNodeNum_ = static_cast<int32_t>(count);
				for (size_t index = 0; index < count; index++) {
					AddressInfo info;
					AddressInfo publicInfo;
					memset(&info, 0, sizeof(AddressInfo));

					CLUSTER_SET_RESOLVER_ADDRESS(
							resolver, info, index, cluster);

					if (usePublicAddress) {
						util::SocketAddress localTxnAddress
								= resolver->getAddress(index, resolver->getType(
								ServiceConfig::TYPE_NAME_TRANSACTION_LOCAL));
						util::SocketAddress txnAddress
								= resolver->getAddress(index, resolver->getType(
									ServiceConfig::TYPE_NAME_TRANSACTION));

						if (localTxnAddress.getPort() != txnAddress.getPort()) {
							GS_THROW_USER_ERROR(GS_ERROR_CS_CONFIG_ERROR,
									"Unmatch transaction port, nodeConfig="
									<< localTxnAddress.getPort() 
									<< ", fixedList=" << txnAddress.getPort());
						}
						info.transactionAddress_.set(localTxnAddress);
						publicInfo.transactionAddress_.set(txnAddress);

						util::SocketAddress localSqlAddress
								= resolver->getAddress(index, resolver->getType(
								ServiceConfig::TYPE_NAME_SQL_LOCAL));
						util::SocketAddress sqlAddress
								= resolver->getAddress(index, resolver->getType(
								ServiceConfig::TYPE_NAME_SQL));
						if (localSqlAddress.getPort() != sqlAddress.getPort()) {
							GS_THROW_USER_ERROR(
									GS_ERROR_CS_CONFIG_ERROR,
									"Unmatch sql port, nodeConfig="
									<< localSqlAddress.getPort()
									<< ", fixedList=" << sqlAddress.getPort());
						}
						info.sqlAddress_.set(localSqlAddress);
						publicInfo.sqlAddress_.set(sqlAddress);
					}
					else {
						if (addConfig.isSetLocalAddress()) {
							GS_THROW_USER_ERROR(
									GS_ERROR_CS_CONFIG_ERROR,
									"LocalServiceAddres is , but not defined node configuration");
						}
						CLUSTER_SET_RESOLVER_ADDRESS(
								resolver, info, index, transaction);
						CLUSTER_SET_RESOLVER_ADDRESS(
								resolver, info, index, sql);
					}
					CLUSTER_SET_RESOLVER_ADDRESS(
							resolver, info, index, sync);
					CLUSTER_SET_RESOLVER_ADDRESS(
							resolver, info, index, system);

					fixedAddressInfoSet_.insert(info);
					if (usePublicAddress) {
						publicFixedAddressInfoSet_.push_back(publicInfo);
					}
				}
			}
			else {
				const int32_t updateInterval = config.get<int32_t>(
						CONFIG_TABLE_CS_NOTIFICATION_PROVIDER_UPDATE_INTERVAL);
				resolverUpdateInterval_ = changeTimeSecToMill(
						updateInterval > 0 ? updateInterval : notificationInterval);
				if (resolverUpdateInterval_ < resolverCheckLongInterval_) {
					resolverCheckLongInterval_ = DEFAULT_CHECK_INTERVAL;
				}
			}
		}
		clsMgr_->setNotificationMode(mode_);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e,
			"Failed to initialize cluster notification "
			"(reason = "
				<< GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Sets digest value of a set of fixed address info.
*/
void ClusterService::NotificationManager::setDigest(
	util::XArray<uint8_t> &digestBinary) {

	for (NodeAddressSetItr it = fixedAddressInfoSet_.begin();
		it != fixedAddressInfoSet_.end(); it++) {
		digestBinary.push_back(
			reinterpret_cast<const uint8_t *>(&(*it)), sizeof(AddressInfo));
	}
}

bool ClusterService::isMulticastMode(const ConfigTable &config) {
	const picojson::value &memberValue =
			config.get<picojson::value>(
					CONFIG_TABLE_CS_NOTIFICATION_MEMBER);
	const char8_t *providerURL =
			config.get<const char8_t *>(
					CONFIG_TABLE_CS_NOTIFICATION_PROVIDER_URL);
	if (!memberValue.is<picojson::null>()) {
		return false;
	}
	else if (strlen(providerURL) != 0) {
		return false;
	}
	else {
		return true;
	}
}

/*!
	@brief Gets a set of fixed address info.
*/
NodeAddressSet &
		ClusterService::NotificationManager::getFixedAddressInfo() {
	return fixedAddressInfoSet_;
}

AddressInfoList &
		ClusterService::NotificationManager::getPublicFixedAddressInfo() {
	return publicFixedAddressInfoSet_;
}


/*!
	@brief Gets NotificationManager.
*/
ClusterService::NotificationManager &
		ClusterService::getNotificationManager() {
	return notificationManager_;
}

/*!
	@brief Requests the resolver the next update.
*/
bool ClusterService::NotificationManager::next() {
	try {
		for (size_t pos = 0; pos < resolverList_.size(); pos++) {
			size_t readSize = 0;
			const bool completed
					= resolverList_[pos]->checkUpdated(&readSize);
			const bool available = resolverList_[pos]->isAvailable();
			if (!completed && available) {
				clsMgr_->reportError(
						GS_ERROR_CS_PROVIDER_TIMEOUT);
				GS_TRACE_INFO(
						CLUSTER_INFO_TRACE, GS_TRACE_CS_CLUSTER_STATUS,
						"Previous provider update request timeout");
			}
			if (!available) {
				GS_TRACE_INFO(
						CLUSTER_INFO_TRACE, GS_TRACE_CS_CLUSTER_STATUS,
						"Try to get initial provider update interval");
				return false;
			}
			if (completed && resolverList_[pos]->isChanged()) {
				util::LockGuard<util::Mutex> guard(lock_);
				resolverList_[pos]->exportTo(*value_);
			}
			const bool immediate = resolverList_[pos]->update();
			if (immediate) {
				if (resolverList_[pos]->isChanged()) {
					util::LockGuard<util::Mutex> guard(lock_);
					resolverList_[pos]->exportTo(*value_);
				}
				return true;
			}
			else {
				return false;
			}
		}
		return false;
	}
	catch (std::exception &e) {
		for (size_t pos = 0; pos < resolverList_.size(); pos++) {
			try {
				resolverList_[pos]->update();
			}
			catch (std::exception &e2) {
				UTIL_TRACE_EXCEPTION(CLUSTER_SERVICE, e2, "");
			}
		}
		UTIL_TRACE_EXCEPTION(CLUSTER_SERVICE, e, "");
		return false;
	}
}

int32_t ClusterService::NotificationManager::check() {
	try {
		for (size_t pos = 0; pos < resolverList_.size(); pos++) {
			size_t readSize = 0;
			if (resolverList_[pos]->checkUpdated(&readSize)) {
				if (resolverList_[pos]->isChanged()) {
					util::LockGuard<util::Mutex> guard(lock_);
					resolverList_[pos]->exportTo(*value_);
				}
				return 0;
			}
			if (readSize > 0) {
				return resolverCheckLongInterval_;
			}
			else {
				return resolverCheckInterval_;
			}
		}
		return resolverCheckInterval_;
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(CLUSTER_SERVICE, e, "");
		return resolverCheckLongInterval_;
	}
}

/*!
	@brief Gets the current list in JSON.
*/
void ClusterService::NotificationManager::getNotificationMember(
	picojson::value &target) {
	try {
		util::LockGuard<util::Mutex> guard(lock_);
		target = *value_;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e,
			"Failed to Get notification member "
			"(reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}


void TimerRequestSQLCheckTimeoutHandler::operator()(
		EventContext &ec, Event &ev) {

	UNUSED_VARIABLE(ev);

	SQLService *sqlSvc = resourceSet_->getSQLService();
	try {
		Event checkResourceEvent(ec,
				CHECK_TIMEOUT_JOB, CS_HANDLER_PARTITION_ID);

		EventEngine *sqlEE = sqlSvc->getEE();
		const NodeDescriptor &nd = sqlEE->getServerND(0);
		EventByteOutStream out = checkResourceEvent.getOutStream();

		sqlEE->send(checkResourceEvent, nd);
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(CLUSTER_SERVICE, e, "");
	}
}

void NewSQLPartitionRefreshHandler::operator()(
		EventContext &ec, Event &ev) {

	try {
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);
		EventType eventType = ev.getType();
		const NodeDescriptor &nd = ev.getSenderND();
		EventEngine *clsEE = clsSvc_->getEE();
		switch (eventType) {
			case CS_NEWSQL_PARTITION_REFRESH: {
				SubPartitionTable subTable;
				subTable.set(pt_);
				if (nd.isEmpty()) {
					pt_->updateNewSQLPartiton(subTable);
				}
				else {
					Event requestEvent(
							ec, CS_NEWSQL_PARTITION_REFRESH_ACK,
							CS_HANDLER_PARTITION_ID);
					clsSvc_->encode(requestEvent, subTable);
					clsEE->send(requestEvent, nd);
				}
				break;
			}
			case CS_NEWSQL_PARTITION_REFRESH_ACK: {
				SubPartitionTable subTable;
				clsSvc_->decode(ev, subTable);
				subTable.init(pt_, clsEE);
				pt_->updateNewSQLPartiton(subTable);
			}
		}
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(CLUSTER_SERVICE, e, "");
	}
}

void ClusterService::requestRefreshPartition(
		EventContext &ec,
		util::XArray<PartitionId> *pIdList) {

	try {
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);
		RefreshPartitionInfo refreshPartitionInfo(alloc);
		if (pIdList) {
			refreshPartitionInfo.set(*pIdList);
		}
		else {
			refreshPartitionInfo.set(pt_);
		}
		Event requetEvent(
				ec, CS_NEWSQL_PARTITION_REFRESH, CS_HANDLER_PARTITION_ID);
		encode(requetEvent, refreshPartitionInfo);
		NodeId master = pt_->getMaster();
		if(master != UNDEF_NODEID) {
			const NodeDescriptor &nd = ee_.getServerND(master);
			if(!nd.isEmpty()) {
				if(master == 0) {
					ee_.add(requetEvent);
				}
				else {
					ee_.send(requetEvent, nd);
				}
			}
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void SubPartitionTable::init(
		PartitionTable *pt, EventEngine *ee) {

	UNUSED_VARIABLE(pt);

	for (size_t pos = 0;
			pos < subPartitionList_.size(); pos++) {
		
		PartitionRole &role = subPartitionList_[pos].role_;
		NodeId owner= ClusterService::changeNodeId(
						role.getOwnerAddress(), ee);

		role.setOwner(owner);
		std::vector<NodeAddress> &backupsAddress
				= role.getBackupAddress();
		std::vector<NodeAddress> &catchupAddress
				= role.getCatchupAddress();
		NodeIdList &backups = role.getBackups();
		NodeIdList &catchups = role.getCatchups();

		backups.assign(backupsAddress.size(), UNDEF_NODEID);
		catchups.assign(catchupAddress.size(), UNDEF_NODEID);
		
		for (size_t subPos = 0;
				subPos < backupsAddress.size(); subPos++) {

			NodeId nodeId = ClusterService::changeNodeId(
					backupsAddress[pos], ee);
			if (nodeId != UNDEF_NODEID) {
				backups[subPos] = nodeId;
			}
		}

		for (size_t subPos = 0;
				subPos < catchupAddress.size(); subPos++) {

			NodeId nodeId = ClusterService::changeNodeId(
					catchupAddress[pos], ee);
			if (nodeId != UNDEF_NODEID) {
				catchups[subPos] = nodeId;
			}
		}
	}
}


void SystemCommandHandler::doJoinCluster(
		JoinClusterInfo &joinClusterInfo,
		EventContext &ec) {

	clsMgr_->checkClusterStatus(CS_JOIN_CLUSTER);
	clsMgr_->checkCommandStatus(CS_JOIN_CLUSTER);

	TRACE_CLUSTER_NORMAL_OPERATION(
			INFO, "Called Join cluster");

	util::StackAllocator &alloc = ec.getAllocator();
	PartitionTable *pt = clsMgr_->getPartitionTable();
	clsMgr_->setJoinClusterInfo(joinClusterInfo);
	for (PartitionId pId = 0; pId < pt->getPartitionNum(); pId++) {
		clsSvc_->requestChangePartitionStatus(
				ec, alloc, pId, PartitionTable::PT_OFF);
	}
	clsMgr_->updateNodeStatus(CS_JOIN_CLUSTER);
}

void SystemCommandHandler::doLeaveCluster(
		LeaveClusterInfo &leaveClusterInfo,
		EventContext &ec) {

	clsMgr_->checkClusterStatus(CS_LEAVE_CLUSTER);
	clsMgr_->checkCommandStatus(CS_LEAVE_CLUSTER);

	TRACE_CLUSTER_NORMAL_OPERATION(INFO, "Called Leave cluster");

	util::StackAllocator &alloc = ec.getAllocator();
	clsMgr_->setLeaveClusterInfo(leaveClusterInfo);
	PartitionTable *pt = clsMgr_->getPartitionTable();
	for (PartitionId pId = 0; pId < pt->getPartitionNum(); pId++) {
		clsSvc_->requestChangePartitionStatus(
				ec, alloc, pId, PartitionTable::PT_STOP);
	}
	clsMgr_->updateClusterStatus(TO_SUBMASTER, true);
	resourceSet_->getSQLService()->requestCancel(ec);
	clsMgr_->updateNodeStatus(CS_LEAVE_CLUSTER);

	if (clsMgr_->isShutdownPending()) {
		TRACE_CLUSTER_NORMAL_OPERATION(INFO,
				"Normal shutdown operation is started by pending operation");
		clsMgr_->checkClusterStatus(
				CS_SHUTDOWN_NODE_NORMAL);
		clsMgr_->checkCommandStatus(
				CS_SHUTDOWN_NODE_NORMAL);

		resourceSet_->getCheckpointService()
				->requestShutdownCheckpoint(ec);
		clsMgr_->updateNodeStatus(
				CS_SHUTDOWN_NODE_NORMAL);
	}
}


void SystemCommandHandler::doShutdownNodeForce() {

	clsMgr_->checkClusterStatus(CS_SHUTDOWN_NODE_FORCE);
	clsMgr_->checkCommandStatus(CS_SHUTDOWN_NODE_FORCE);

	clsSvc_->executeShutdown(true);
	
	TRACE_CLUSTER_NORMAL_OPERATION(
			INFO, "Force shutdown node operation is completed");
}

void SystemCommandHandler::doShutdownNormal(
		ShutdownNodeInfo &shutdownNodeInfo,
		EventContext &ec) {

	UNUSED_VARIABLE(shutdownNodeInfo);

	clsMgr_->checkClusterStatus(CS_SHUTDOWN_NODE_NORMAL);
	clsMgr_->checkCommandStatus(CS_SHUTDOWN_NODE_NORMAL);
	resourceSet_->getCheckpointService()->requestShutdownCheckpoint(ec);
	clsMgr_->updateNodeStatus(CS_SHUTDOWN_NODE_NORMAL);
	TRACE_CLUSTER_NORMAL_OPERATION(INFO,
			"Normal shutdown node operation is completed");
}

void SystemCommandHandler::doShutdownCluster(
		EventContext &ec,
		NodeId senderNodeId) {
	
	clsMgr_->checkClusterStatus(CS_SHUTDOWN_CLUSTER);
	clsMgr_->checkCommandStatus(CS_SHUTDOWN_CLUSTER);

	util::StackAllocator &alloc = ec.getAllocator();
	Event leaveEvent(
			ec, CS_LEAVE_CLUSTER, CS_HANDLER_PARTITION_ID);
	LeaveClusterInfo
			leaveClusterInfo(alloc, senderNodeId);
	clsSvc_->encode(leaveEvent, leaveClusterInfo);

	PartitionTable *pt = clsMgr_->getPartitionTable();
	util::XArray<NodeId> liveNodeIdList(alloc);
	pt->getLiveNodeIdList(liveNodeIdList);
	TRACE_CLUSTER_NORMAL_OPERATION(
			INFO,
			"Shutdown cluster (nodeNum=" 
			<< liveNodeIdList.size() << ", addressList="
			<< pt_->dumpNodeAddressList(liveNodeIdList) << ")");
	
	for (size_t pos = 0; pos < liveNodeIdList.size(); pos++) {
		EventEngine *ee = clsSvc_->getEE();
		const NodeDescriptor &nd
				= ee->getServerND(liveNodeIdList[pos]);
		if (!nd.isEmpty()) {
			if (nd.getId() == 0) {
				ee->add(leaveEvent);
			}
			else {
				ee->send(leaveEvent, nd);
			}
		}
	}
	TRACE_CLUSTER_NORMAL_OPERATION(
			INFO, "Shutdown cluster operation is completed");
}

void SystemCommandHandler::doCompleteCheckpointForShutdown() {

	clsMgr_->checkClusterStatus(
			CS_COMPLETE_CHECKPOINT_FOR_SHUTDOWN);
	clsMgr_->checkCommandStatus(
			CS_COMPLETE_CHECKPOINT_FOR_SHUTDOWN);

	TRACE_CLUSTER_NORMAL_OPERATION(INFO,
			"Shutdown checkpoint is completed, shutdown all services");

	clsSvc_->executeShutdown(true);

	clsMgr_->updateNodeStatus(
			CS_COMPLETE_CHECKPOINT_FOR_SHUTDOWN);
	TRACE_CLUSTER_NORMAL_OPERATION(INFO,
			"Normal shutdown node operation is completed.");
}

void SystemCommandHandler::doCompleteCheckpointForRecovery() {
	
		TRACE_CLUSTER_NORMAL_OPERATION(
				INFO, "Recovery checkpoint is completed, start all services");
		{
			util::LockGuard<util::Mutex> lock(
					clsMgr_->getClusterLock());

			if (!clsMgr_->isSignalBeforeRecovery()) {
				clsMgr_->checkClusterStatus(
						CS_COMPLETE_CHECKPOINT_FOR_RECOVERY);
				clsMgr_->checkCommandStatus(
						CS_COMPLETE_CHECKPOINT_FOR_RECOVERY);
				clsMgr_->updateNodeStatus(
						CS_COMPLETE_CHECKPOINT_FOR_RECOVERY);
		}
		else {
			clsSvc_->executeShutdown(true);
			TRACE_CLUSTER_NORMAL_OPERATION(
					INFO, "Normal shutdown node operation is completed");
		}
	}
}

ClusterAdditionalServiceConfig::
		ClusterAdditionalServiceConfig(
				const ConfigTable &config,
				PartitionTable *pt) : hasPublic_(false) {

	txnServiceAddress_ = config.get<const char8_t *>(
			CONFIG_TABLE_TXN_SERVICE_ADDRESS);
	txnLocalServiceAddress_
			= config.get<const char8_t *>(
					CONFIG_TABLE_TXN_LOCAL_SERVICE_ADDRESS);
	txnAddressSize_ = strlen(txnServiceAddress_) ;
	txnInternalAddressSize_ =  strlen(txnLocalServiceAddress_);
	txnPort_ = config.getUInt16(
			CONFIG_TABLE_TXN_SERVICE_PORT);

	sqlServiceAddress_ = config.get<const char8_t *>(
			CONFIG_TABLE_SQL_SERVICE_ADDRESS);
	sqlLocalServiceAddress_ = config.get<const char8_t *>(
			CONFIG_TABLE_SQL_LOCAL_SERVICE_ADDRESS);
	sqlAddressSize_ = strlen(sqlServiceAddress_) ;
	sqlInternalAddressSize_ = 
			strlen(sqlLocalServiceAddress_);
	sqlPort_ = config.getUInt16(
			CONFIG_TABLE_SQL_SERVICE_PORT);

	serviceTypeList_ = ServiceConfig::SERVICE_TYPE_LIST_MAP;
	serviceTypeSize_
			= sizeof(ServiceConfig::SERVICE_TYPE_LIST_MAP)/sizeof(int32_t);
	serviceTypeNameList_ = ServiceConfig::SERVICE_TYPE_NAMES;

	if (txnAddressSize_ == 0 && txnInternalAddressSize_ > 0) {
		GS_THROW_USER_ERROR(
				GS_ERROR_CS_CONFIG_ERROR, "");
	}
	else {
		if (txnInternalAddressSize_ > 0) {
			hasPublic_ = true;
			serviceTypeList_
					= ServiceConfig::SERVICE_TYPE_LIST_MAP_WITH_PUBLIC;
			serviceTypeSize_ 
				= sizeof(ServiceConfig::SERVICE_TYPE_LIST_MAP_WITH_PUBLIC)
						/sizeof(int32_t);
			serviceTypeNameList_
					= ServiceConfig::SERVICE_TYPE_NAMES_WITH_PUBLIC;

			if (pt != NULL) {

				pt->setPublicAddress();
				AddressInfo publicAddressInfo;
				NodeAddress txnAddress(txnServiceAddress_, txnPort_);
				publicAddressInfo.setNodeAddress(
						TRANSACTION_SERVICE, txnAddress);

				NodeAddress sqlAddress(sqlServiceAddress_, sqlPort_);
				publicAddressInfo.setNodeAddress(
						SQL_SERVICE, sqlAddress);
				pt->setPublicAddressInfo(
						SELF_NODEID, publicAddressInfo);
			}
		}
	}
	if (sqlAddressSize_ == 0 && sqlInternalAddressSize_ > 0) {
		GS_THROW_USER_ERROR(
				GS_ERROR_CS_CONFIG_ERROR, "");
	}
	if ((txnInternalAddressSize_ > 0
			&& sqlInternalAddressSize_ == 0)
		|| (sqlInternalAddressSize_ > 0
			&& txnInternalAddressSize_== 0)) {
		GS_THROW_USER_ERROR(GS_ERROR_CS_CONFIG_ERROR, "");
	}
};

const char *ClusterAdditionalServiceConfig::getServiceAddress(
		ServiceType serviceType) {

	const char *localAddress = NULL;
	const char *publicAddress = NULL;
	const char *targetAddress = NULL;
	switch (serviceType) {
		case TRANSACTION_SERVICE:
			targetAddress = txnServiceAddress_;
			localAddress = txnLocalServiceAddress_;
			publicAddress = txnServiceAddress_;
			break;
		case SQL_SERVICE:
			targetAddress = sqlServiceAddress_;
			localAddress = sqlLocalServiceAddress_;
			publicAddress = sqlServiceAddress_;
			break;
		default:
			GS_THROW_USER_ERROR(
					GS_ERROR_CS_SERVICE_CLUSTER_INTERNAL_FAILED,"");
	}
	if (strlen(publicAddress) > 0) {
		if (strlen(localAddress) > 0) {
			targetAddress = localAddress;
		}
	}
	else {
		if (strlen(localAddress) > 0) {
			GS_THROW_USER_ERROR(GS_ERROR_CS_CONFIG_ERROR, "");
		}
	}
	return targetAddress;
}

const char *ClusterAdditionalServiceConfig::getAddress(
		ServiceType serviceType, int32_t type) {

	UNUSED_VARIABLE(type);

	switch (serviceType) {
		case TRANSACTION_SERVICE:
			return txnServiceAddress_;
		case SQL_SERVICE:
			return sqlServiceAddress_;
		default:
			GS_THROW_USER_ERROR(
					GS_ERROR_CS_SERVICE_CLUSTER_INTERNAL_FAILED, "");
	}
}

uint16_t ClusterAdditionalServiceConfig::getPort(
		ServiceType serviceType) {

	switch (serviceType) {
		case TRANSACTION_SERVICE:
			return txnPort_;
		case SQL_SERVICE:
			return sqlPort_;
		default:
			GS_THROW_USER_ERROR(
					GS_ERROR_CS_SERVICE_CLUSTER_INTERNAL_FAILED, "");
	}
}

/*!
	@brief Handler Operator
*/
void TimerNotifyClientHandler::operator()(
		EventContext &ec, Event &ev) {

	EventType eventType = ev.getType();
	TransactionService *txnSvc
			= resourceSet_->getTransactionService();

	try {
		if (clsSvc_->getNotificationManager().getMode() != NOTIFICATION_MULTICAST) {
			return;
		}

		clsMgr_->checkNodeStatus();
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);
		try {
			clsMgr_->checkClusterStatus(CS_TIMER_NOTIFY_CLIENT);
		}
		catch (UserException &) {
			return;
		}

		EventEngine *txnEE = txnSvc->getEE();
		Event notifyClientEvent(
				ec, RECV_NOTIFY_MASTER, CS_HANDLER_PARTITION_ID);
		clsSvc_->encodeNotifyClient(notifyClientEvent);
		const NodeDescriptor &nd = txnEE->getMulticastND();
		if (!nd.isEmpty()) {
			notifyClientEvent.setPartitionIdSpecified(false);
			txnEE->send(notifyClientEvent, nd);
		}
		else {
			GS_THROW_USER_ERROR(
					GS_ERROR_EE_PARAMETER_INVALID, "ND is empty");
		}
	}
	catch (UserException &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, WARNING, "");
	}
	catch (std::exception &e) {
		TRACE_CLUSTER_EXCEPTION(e, eventType, ERROR, "");
		clsSvc_->setSystemError(&e);
	}
}

uint32_t ClusterStats::getNotTransactionTimeoutCheckCount(
		PartitionTable *pt) {

	uint32_t count = 0;
	for (PartitionId pId = 0;
			pId < partitionNum_; pId++) {
		if (!checkResourceList_[pId]
			&& pt->isOwnerOrBackup(pId)) {
			count++;
		}
	}
	return count;
}

template void ClusterService::encode(
		Event &ev, HeartbeatInfo &t);

template void ClusterService::encode(
		Event &ev, HeartbeatResInfo &t);

template void ClusterService::encode(
		Event &ev, NotifyClusterInfo &t);

template void ClusterService::encode(
		NotifyClusterInfo &t,
		EventByteOutStream &out);

template void ClusterService::encode(
		Event &ev, NotifyClusterResInfo &t);

template void ClusterService::encode(
		NotifyClusterResInfo &t,
		EventByteOutStream &out);

template void ClusterService::encode(
		Event &ev, DropPartitionInfo &t);

template void ClusterService::decode(
		Event &ev, HeartbeatInfo &t);

template void ClusterService::decode(
		Event &ev, HeartbeatResInfo &t);

template void ClusterService::decode(
		Event &ev, DropPartitionInfo &t);

template void ClusterService::decode(
		Event &ev, LongtermSyncInfo &t);

template void ClusterService::decode(
		Event &ev, IncreaseClusterInfo &t);

template void ClusterService::decode(
		Event &ev, DecreaseClusterInfo &t);

template void ClusterService::decode(
		Event &ev, HeartbeatInfo &t,
		EventByteInStream &in);

template void ClusterService::decode(
		Event &ev, HeartbeatResInfo &t,
		EventByteInStream &in);

template void ClusterService::decode(
		Event &ev, NotifyClusterInfo &t,
		EventByteInStream &in);

template void ClusterService::decode(
		Event &ev, NotifyClusterResInfo &t,
		EventByteInStream &in);