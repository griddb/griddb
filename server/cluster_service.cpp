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

UTIL_TRACER_DECLARE(CLUSTER_INFO_TRACE);
UTIL_TRACER_DECLARE(CLUSTER_DUMP);

#include "json.h"
#include "picojson.h"


#ifndef _WIN32
#define SYSTEM_CAPTURE_SIGNAL
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>
#endif

UTIL_TRACER_DECLARE(CLUSTER_SERVICE);
UTIL_TRACER_DECLARE(SYNC_SERVICE);
UTIL_TRACER_DECLARE(CLUSTER_OPERATION);

#define TEST_PRINT(s)
#define TEST_PRINT1(s, d)

const char *ClusterService::NotificationManager::CONFIG_ADDRESS = "address";
const char *ClusterService::NotificationManager::CONFIG_PORT = "port";
const char *ClusterService::NotificationManager::CONFIG_URL = "url";
const char *ClusterService::NotificationManager::CONFIG_UPDATE_INTERVAL =
	"updateInterval";


#define CLUSTER_SET_ADDRESS(info, pos, type, resolver, target, SERVICE_TYPE) \
	do {                                                                     \
		const picojson::value &value =                                       \
			JsonUtils::as<picojson::value>(target, SERVICE_TYPE);            \
		info.set(JsonUtils::as<std::string>(value, CONFIG_ADDRESS).c_str(),  \
			JsonUtils::asInt<uint16_t>(value, CONFIG_PORT));                 \
		resolver.setAddress(pos, type,                                       \
			resolver.makeSocketAddress(                                      \
				JsonUtils::as<std::string>(value, CONFIG_ADDRESS).c_str(),   \
				JsonUtils::asInt<uint16_t>(value, CONFIG_PORT)));            \
	} while (false)

#define CLUSTER_MAKE_JSON_ADDRESS(entryObj, pos, key, service)                 \
	do {                                                                       \
		picojson::object currentObj;                                           \
		NodeAddress &address = pt_->getNodeInfo(pos).getNodeAddress(service);  \
		currentObj[CONFIG_ADDRESS] = picojson::value(address.toString(false)); \
		currentObj[CONFIG_PORT] =                                              \
			picojson::value(static_cast<double>(address.port_));               \
		entryObj[key] = picojson::value(currentObj);                           \
	} while (false)

#define CLUSTER_MAKE_JSON_ADDRESS_FROM_SOCKET(                                \
	entryObj, resolver, pos, key, service)                                    \
	do {                                                                      \
		picojson::object currentObj;                                          \
		const util::SocketAddress &addr = resolver.getAddress(pos, service);  \
		u8string addrString;                                                  \
		uint16_t port;                                                        \
		addr.getIP(&addrString, &port);                                       \
		currentObj[CONFIG_ADDRESS] = picojson::value(addrString);             \
		currentObj[CONFIG_PORT] = picojson::value(static_cast<double>(port)); \
		entryObj[key] = picojson::value(currentObj);                          \
	} while (false)

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

ClusterService::ClusterService(const ConfigTable &config,
	EventEngine::Config &eeconfig, EventEngine::Source source,
	const char8_t *name, ClusterManager &clsMgr, ClusterVersionId versionId,
		ServiceThreadErrorHandler &serviceThreadErrorHandler,
		util::VariableSizeAllocator<> &alloc) :
	serviceThreadErrorHandler_(serviceThreadErrorHandler),
	ee_(createEEConfig(config, eeconfig), source, name),
	versionId_(versionId),
	fixedSizeAlloc_(source.fixedAllocator_),
	varSizeAlloc_(NULL),
	clsMgr_(&clsMgr),
	txnSvc_(NULL),
	syncSvc_(NULL),
	sysSvc_(NULL),
	cpSvc_(NULL),
	clusterStats_(config.get<int32_t>(CONFIG_TABLE_DS_PARTITION_NUM)),
	pt_(NULL),
	initailized_(false),
	isSystemServiceError_(false),
	notificationManager_(clsMgr_, alloc)
{
	try {
		clsMgr_ = &clsMgr;

		ee_.setHandler(CS_HEARTBEAT, heartbeatHandler_);
		ee_.setHandlingMode(CS_HEARTBEAT, EventEngine::HANDLING_IMMEDIATE);
		ee_.setHandler(CS_HEARTBEAT_RES, heartbeatHandler_);
		ee_.setHandlingMode(CS_HEARTBEAT_RES, EventEngine::HANDLING_IMMEDIATE);

		ee_.setHandler(CS_NOTIFY_CLUSTER, notifyClusterHandler_);
		ee_.setHandlingMode(CS_NOTIFY_CLUSTER, EventEngine::HANDLING_IMMEDIATE);
		ee_.setHandler(CS_NOTIFY_CLUSTER_RES, notifyClusterHandler_);
		ee_.setHandlingMode(CS_NOTIFY_CLUSTER_RES, EventEngine::HANDLING_IMMEDIATE);

		ee_.setHandler(CS_UPDATE_PARTITION, updatePartitionHandler_);

		ee_.setHandler(CS_JOIN_CLUSTER, systemCommandHandler_);
		ee_.setHandler(CS_LEAVE_CLUSTER, systemCommandHandler_);
		ee_.setHandler(CS_INCREASE_CLUSTER, systemCommandHandler_);
		ee_.setHandler(CS_DECREASE_CLUSTER, systemCommandHandler_);
		ee_.setHandler(CS_SHUTDOWN_NODE_NORMAL, systemCommandHandler_);
		ee_.setHandler(CS_SHUTDOWN_NODE_FORCE, systemCommandHandler_);
		ee_.setHandler(CS_SHUTDOWN_CLUSTER, systemCommandHandler_);
		ee_.setHandler(
			CS_COMPLETE_CHECKPOINT_FOR_SHUTDOWN, systemCommandHandler_);
		ee_.setHandler(
			CS_COMPLETE_CHECKPOINT_FOR_RECOVERY, systemCommandHandler_);

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



		ee_.setUnknownEventHandler(unknownEventHandler_);
		ee_.setThreadErrorHandler(serviceThreadErrorHandler_);
		util::StackAllocator alloc(
			util::AllocatorInfo(ALLOCATOR_GROUP_CS, "clusterService"),
			fixedSizeAlloc_);
		util::StackAllocator::Scope scope(alloc);

		notificationManager_.initialize(config);
		if (notificationManager_.getMode() == NOTIFICATION_RESOLVER) {
			ee_.setHandler(CS_UPDATE_PROVIDER, timerNotifyClusterHandler_);
			ee_.setHandler(CS_CHECK_PROVIDER, timerNotifyClusterHandler_);

			Event updateResolverEvent(
				source, CS_UPDATE_PROVIDER, CS_HANDLER_PARTITION_ID);
			assert(notificationManager_.updateResolverInterval() > 0);
			ee_.addPeriodicTimer(updateResolverEvent,
				notificationManager_.updateResolverInterval());
		}

		NodeAddressSet &addressInfo = notificationManager_.getFixedAddressInfo();
		util::XArray<uint8_t> digestBinary(alloc);
		clsMgr_->setDigest(
			config, digestBinary, addressInfo, notificationManager_.getMode());
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
	if (isMulticastMode(config)) {
		eeConfig.setMulticastAddress(
			config.get<const char8_t *>(CONFIG_TABLE_CS_NOTIFICATION_ADDRESS),
			config.getUInt16(CONFIG_TABLE_CS_NOTIFICATION_PORT));
	}
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
		clsMgr_->initialize(this, &ee_ );

		pt_ = mgrSet.pt_;
		ConfigTable *config = mgrSet.config_;
		varSizeAlloc_ = mgrSet.varSizeAlloc_;

		mgrSet.stats_->addUpdator(&clsMgr_->getStatUpdator());

		heartbeatHandler_.initialize(mgrSet);
		notifyClusterHandler_.initialize(mgrSet);
		updatePartitionHandler_.initialize(mgrSet);
		systemCommandHandler_.initialize(mgrSet);
		timerCheckClusterHandler_.initialize(mgrSet);
		timerNotifyClusterHandler_.initialize(mgrSet);
		timerNotifyClientHandler_.initialize(mgrSet);


		unknownEventHandler_.initialize(mgrSet);
		serviceThreadErrorHandler_.initialize(mgrSet);

		NodeAddress clusterAddress, txnAddress, syncAddress;
		ClusterService::changeAddress(clusterAddress, SELF_NODEID, getEE(CLUSTER_SERVICE));
		ClusterService::changeAddress(txnAddress, SELF_NODEID, getEE(TRANSACTION_SERVICE));
		ClusterService::changeAddress(syncAddress, SELF_NODEID, getEE(SYNC_SERVICE));

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

		pt_->setNodeInfo(SELF_NODEID, CLUSTER_SERVICE, clusterAddress);
		pt_->setNodeInfo(SELF_NODEID, TRANSACTION_SERVICE, txnAddress);
		pt_->setNodeInfo(SELF_NODEID, SYNC_SERVICE, syncAddress);
		pt_->setNodeInfo(SELF_NODEID, SYSTEM_SERVICE, systemAddress);
		if (notificationManager_.getMode() == NOTIFICATION_FIXEDLIST) {
			NodeAddressSet &addressInfo = notificationManager_.getFixedAddressInfo();
			bool selfIncluded = false;
			for (NodeAddressSet::iterator it = addressInfo.begin(); it != addressInfo.end(); it++) {
				const NodeAddress &address = (*it).clusterAddress_;
				if (address == clusterAddress) {
					selfIncluded = true;
					break;
				}
			}
			if (!selfIncluded) {
				GS_THROW_USER_ERROR(GS_ERROR_CS_CONFIG_ERROR,
					"Failed to check notification list in config. "
					"Self cluster address is not included in this list "
						"(address = " << clusterAddress.toString() << ")");
			}
			updateNodeList(addressInfo);
			if (pt_->getNodeNum() != notificationManager_.getFixedNodeNum()) {
				GS_THROW_USER_ERROR(GS_ERROR_CS_CONFIG_ERROR,
					"Failed to check notification member. "
					"Duplicate member is included in this list");
			}
		}
		pt_->getPartitionRevision().set(clusterAddress.address_, clusterAddress.port_);
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
			GS_THROW_USER_ERROR(GS_ERROR_CS_SERVICE_NOT_INITIALIZED, "");
		}
		ee_.start();

		const ClusterNotificationMode mode = notificationManager_.getMode();
		if (mode == NOTIFICATION_RESOLVER) {
			Event checkResolverEvent(
				eventSource, CS_CHECK_PROVIDER, CS_HANDLER_PARTITION_ID);
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
					e, GS_EXCEPTION_MERGE_MESSAGE(e, "Failed to encode message"));
		}
		uint32_t packedSize = static_cast<uint32_t>(buffer.size());
		if (packedSize == 0) {
				GS_THROW_USER_ERROR(GS_ERROR_CS_ENCODE_DECODE_VALIDATION_CHECK,
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
				e, GS_EXCEPTION_MERGE_MESSAGE(e, "Failed to encode message"));
		}
		uint32_t packedSize = static_cast<uint32_t>(buffer.size());
		if (packedSize == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_CS_ENCODE_DECODE_VALIDATION_CHECK,
				"Invalid message size(0)");
		}
		out << packedSize;
		out.writeAll(buffer.data(), packedSize);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
}

template void ClusterService::encode(Event &ev, DropPartitionInfo &t);

/*!
	@brief Checks the version of message for cluster service
*/
void ClusterService::checkVersion(uint8_t versionId) {
	if (versionId != versionId_) {
		GS_THROW_USER_ERROR(GS_ERROR_CS_ENCODE_DECODE_VERSION_CHECK,
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
void ClusterService::decode(util::StackAllocator &alloc, Event &ev, T &t) {
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

template <class T>
void ClusterService::decode(
		util::StackAllocator &alloc, Event &ev, T &t, EventByteInStream &in) {
	try {
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
		const char8_t *bodyBuffer = reinterpret_cast<const char8_t *>(
				eventBuffer + in.base().position());
		if (bodyBuffer == NULL) {
			GS_THROW_USER_ERROR(GS_ERROR_CS_SERVICE_DECODE_MESSAGE_FAILED,
				"Event body buffer is null");
		}
//		uint32_t remainSize = static_cast<int32_t>(in.base().remaining());
		uint32_t lastSize = in.base().position() + packedSize;
		try {
			msgpack::unpacked msg;
			msgpack::unpack(&msg, bodyBuffer, packedSize);
			msgpack::object obj = msg.get();
			obj.convert(&t);
		}
		catch (std::exception &e) {
			GS_RETHROW_USER_ERROR(
				e, GS_EXCEPTION_MERGE_MESSAGE(e, "Failed to decode message"));
		}
		in.base().position(lastSize);
		t.check();
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
}

template void ClusterService::decode(util::StackAllocator &alloc, Event &ev, DropPartitionInfo &t);
template void ClusterService::decode(util::StackAllocator &alloc, Event &ev, LongtermSyncInfo &t);

/*!
	@brief Handles system error
*/
void ClusterService::setError(
	const Event::Source &eventSource, std::exception *e) {
	try {
		if (e != NULL) {
			UTIL_TRACE_EXCEPTION_ERROR(CLUSTER_OPERATION, *e,
				GS_EXCEPTION_MERGE_MESSAGE(*e, "[ERROR] SetError requested"));
		}
		if (!clsMgr_->setSystemError()) {
			UTIL_TRACE_WARNING(CLUSTER_SERVICE, "Already reported system error.");
			return;
		}
		cpSvc_->shutdown();
		txnSvc_->shutdown();
		syncSvc_->shutdown();
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
void ClusterService::requestChangePartitionStatus(EventContext &ec,
	util::StackAllocator &alloc, PartitionId pId, PartitionStatus status) {
	try {
		Event requestEvent(ec, TXN_CHANGE_PARTITION_STATE, pId);
		ClusterManager::ChangePartitionStatusInfo changePartitionStatusInfo(
				alloc, 0, pId, status, true, PT_CHANGE_NORMAL);
		encode(requestEvent, changePartitionStatusInfo);
		if (txnSvc_) {
			txnSvc_->getEE()->addTimer(requestEvent, EE_PRIORITY_HIGH);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Executes ChangePartitionStatus directly in the same thread
*/
void ClusterService::requestChangePartitionStatus(EventContext &ec,
		util::StackAllocator &alloc, PartitionId pId, PartitionStatus status,
	ChangePartitionType changePartitionType) {
	try {
		ClusterManager::ChangePartitionStatusInfo changePartitionStatusInfo(
			alloc, 0, pId, status, false, changePartitionType);
		txnSvc_->changeTimeoutCheckMode(pId, status, changePartitionType,
			changePartitionStatusInfo.isToSubMaster(), getStats());
		clsMgr_->setChangePartitionStatusInfo(changePartitionStatusInfo);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

template void ClusterService::request(const Event::Source &eventSource,
	EventType eventType, PartitionId pId, EventEngine *targetEE,
	LongtermSyncInfo &t);

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

void TimerNotifyClientHandler::encode(Event &ev) {
	try {
		EventByteOutStream out = ev.getOutStream();
		const ContainerHashMode hashType = CONTAINER_HASH_MODE_CRC32;
		util::SocketAddress::Inet address;
		uint16_t port;
		const NodeDescriptor &nd = clsSvc_->getEE(TRANSACTION_SERVICE)->getSelfServerND();
		if (nd.isEmpty()) {
			GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID, "ND is empty");
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
		const ContainerHashMode hashType = CONTAINER_HASH_MODE_CRC32;
		util::SocketAddress::Inet address;
		uint16_t port;
		const NodeDescriptor &nd = getEE(TRANSACTION_SERVICE)->getSelfServerND();
		if (nd.isEmpty()) {
			GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID, "ND is empty");
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
	ClusterManager::JoinClusterInfo &t);

template void ClusterService::request(const Event::Source &eventSource,
	EventType eventType, PartitionId pId, EventEngine *targetEE,
	ClusterManager::LeaveClusterInfo &t);

template void ClusterService::request(const Event::Source &eventSource,
	EventType eventType, PartitionId pId, EventEngine *targetEE,
	ClusterManager::IncreaseClusterInfo &t);

template void ClusterService::request(const Event::Source &eventSource,
	EventType eventType, PartitionId pId, EventEngine *targetEE,
	ClusterManager::DecreaseClusterInfo &t);

template void ClusterService::request(const Event::Source &eventSource,
	EventType eventType, PartitionId pId, EventEngine *targetEE,
	ClusterManager::ShutdownNodeInfo &t);

template void ClusterService::request(const Event::Source &eventSource,
	EventType eventType, PartitionId pId, EventEngine *targetEE,
	ClusterManager::ShutdownClusterInfo &t);

void HeartbeatHandler::doAfterHeartbeatInfo(
		EventContext &ec, ClusterManager::HeartbeatInfo &heartbeatInfo) {
	if (heartbeatInfo.isStatusChange()) {
		util::StackAllocator &alloc = ec.getAllocator();
		for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
			clsSvc_->requestChangePartitionStatus(
				ec, alloc, pId, PartitionTable::PT_OFF);
		}
		clsMgr_->updateClusterStatus(TO_SUBMASTER);
	}
}

/*!
	@brief Handler Operator
*/
void HeartbeatHandler::operator()(EventContext &ec, Event &ev) {
	EventType eventType = ev.getType();
	try {
		clsMgr_->checkNodeStatus();
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);
		NodeId senderNodeId = ClusterService::resolveSenderND(ev);
		checkAutoNdNumbering(ev.getSenderND());

		switch (eventType) {
		case CS_HEARTBEAT: {
			try {
				clsMgr_->checkClusterStatus(CS_HEARTBEAT);
			}
			catch (UserException &e) {
				return;
			}

			ClusterManager::HeartbeatInfo heartbeatInfo(alloc, senderNodeId, pt_);
			clsSvc_->decode(alloc, ev, heartbeatInfo);

			clsSvc_->updateNodeList(heartbeatInfo.getNodeAddressList());
			if (!clsMgr_->setHeartbeatInfo(heartbeatInfo)) {
				return;
			}
			doAfterHeartbeatInfo(ec, heartbeatInfo);

			ClusterManager::HeartbeatResInfo heartbeatResInfo(alloc, senderNodeId,
				heartbeatInfo.getPartitionRevisionNo());
			clsMgr_->getHeartbeatResInfo(heartbeatResInfo);

			Event requestEvent(ec, CS_HEARTBEAT_RES, CS_HANDLER_PARTITION_ID);
			clsSvc_->encode(requestEvent, heartbeatResInfo);

			const NodeDescriptor &nd = ev.getSenderND();
			if (!nd.isEmpty()) {
				clsEE_->send(requestEvent, nd);
			}
			break;
		}
		case CS_HEARTBEAT_RES: {
			clsMgr_->checkClusterStatus(CS_HEARTBEAT_RES);
			ClusterManager::HeartbeatResInfo heartbeatResInfo(alloc, senderNodeId, 0);
			clsSvc_->decode(alloc, ev, heartbeatResInfo);
			clsMgr_->setHeartbeatResInfo(heartbeatResInfo);
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

template<class T>
void NotifyClusterHandler::doAfterNotifyCluster(T &t) {
	clsSvc_->updateNodeList(t.getNodeAddressInfoList());
}

void NotifyClusterHandler::handleError(Event &ev, UserException &e) {
	EventType eventType = ev.getType();
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

/*!
	@brief Handler Operator
*/
void NotifyClusterHandler::operator()(EventContext &ec, Event &ev) {
	EventType eventType = ev.getType();
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
				clsMgr_->checkClusterStatus(CS_NOTIFY_CLUSTER);
			}
			catch (UserException &e) {
				return;
			}

			ClusterManager::NotifyClusterInfo recvNotifyClusterInfo(alloc, senderNodeId, pt_);
			clsSvc_->decode(alloc, ev, recvNotifyClusterInfo);
			if (!clsMgr_->setNotifyClusterInfo(recvNotifyClusterInfo)) {
				return;
			}
			doAfterNotifyCluster(recvNotifyClusterInfo);

			if (recvNotifyClusterInfo.isFollow()) {
				ClusterManager::NotifyClusterResInfo notifyClusterResInfo(alloc, senderNodeId, pt_);
				clsMgr_->getNotifyClusterResInfo(notifyClusterResInfo);

				Event requestEvent(ec, CS_NOTIFY_CLUSTER_RES, CS_HANDLER_PARTITION_ID);
				clsSvc_->encode(requestEvent, notifyClusterResInfo);

				const NodeDescriptor &nd = ev.getSenderND();
				if (!nd.isEmpty()) {
					clsEE_->send(requestEvent, nd);
				}
			}
			break;
		}
		case CS_NOTIFY_CLUSTER_RES: {
			clsMgr_->checkClusterStatus(CS_NOTIFY_CLUSTER_RES);
			ClusterManager::NotifyClusterResInfo notifyClusterResInfo(alloc, senderNodeId, pt_);
			clsSvc_->decode(alloc, ev, notifyClusterResInfo);

			doAfterNotifyCluster(notifyClusterResInfo);

			clsMgr_->setNotifyClusterResInfo(notifyClusterResInfo);
			break;
		}
		}
	}
	catch (UserException &e) {
		handleError(ev, e);
	}
	catch (std::exception &e) {
		clsSvc_->setError(ec, &e);
	}
}

/*!
	@brief Handler Operator
*/
void SystemCommandHandler::operator()(EventContext &ec, Event &ev) {
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
			ClusterManager::JoinClusterInfo joinClusterInfo(alloc, senderNodeId, false);
			clsSvc_->decode(alloc, ev, joinClusterInfo);
			doJoinCluster(clsMgr_, joinClusterInfo, ec);
			break;
		}
		case CS_LEAVE_CLUSTER: {
			ClusterManager::LeaveClusterInfo leaveClusterInfo(alloc, senderNodeId);
			clsSvc_->decode(alloc, ev, leaveClusterInfo);
			doLeaveCluster(clsMgr_, cpSvc_, leaveClusterInfo, ec);
			break;
		}
		case CS_INCREASE_CLUSTER: {
			ClusterManager::IncreaseClusterInfo increaseClusterInfo(alloc, senderNodeId);
			clsSvc_->decode(alloc, ev, increaseClusterInfo);
			doIncreaseCluster(clsMgr_, increaseClusterInfo);
			break;
		}
		case CS_DECREASE_CLUSTER: {
			ClusterManager::DecreaseClusterInfo decreaseClusterInfo(alloc, senderNodeId, pt_);
			clsSvc_->decode(alloc, ev, decreaseClusterInfo);
			doDecreaseCluster(clsMgr_, decreaseClusterInfo, ec);
			break;
		}
		case CS_SHUTDOWN_NODE_FORCE: {
			ClusterManager::ShutdownNodeInfo shutdownNodeInfo(alloc, senderNodeId, true);
			clsSvc_->decode(alloc, ev, shutdownNodeInfo);
			doShutdownNodeForce(clsMgr_, cpSvc_, txnSvc_,
					syncSvc_, sysSvc_
					);
			break;
		}
		case CS_SHUTDOWN_NODE_NORMAL: {
			ClusterManager::ShutdownNodeInfo shutdownNodeInfo(alloc, senderNodeId, false);
			clsSvc_->decode(alloc, ev, shutdownNodeInfo);
			doShutdownNormal(clsMgr_, cpSvc_, shutdownNodeInfo, ec);
			break;
		}
		case CS_SHUTDOWN_CLUSTER: {
			doShutdownCluster(clsMgr_, ec, senderNodeId);
			break;
		}
		case CS_COMPLETE_CHECKPOINT_FOR_SHUTDOWN: {
			doCompleteCheckpointForShutdown(clsMgr_, cpSvc_, txnSvc_,
					syncSvc_, sysSvc_
					);
			break;
		}
		case CS_COMPLETE_CHECKPOINT_FOR_RECOVERY: {
			doCompleteCheckpointForRecovery(clsMgr_, cpSvc_, txnSvc_,
					syncSvc_, sysSvc_
					);
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

void TimerCheckClusterHandler::sendHeartbeat(EventContext &ec,
		ClusterManager::HeartbeatInfo &heartbeatInfo,
		ClusterManager::HeartbeatCheckInfo &heartbeatCheckInfo) {
	std::vector<NodeId> &activeNodeList = heartbeatCheckInfo.getActiveNodeList();
	int32_t activeNodeListSize = static_cast<int32_t>(activeNodeList.size());
	pt_->incPartitionRevision();
	if (activeNodeListSize > 1) {
		Event heartbeatEvent(ec, CS_HEARTBEAT, CS_HANDLER_PARTITION_ID);
		clsSvc_->encode(heartbeatEvent, heartbeatInfo);
//		int64_t currentTime = clsMgr_->getMonotonicTime();
		for (NodeId nodeId = 1; nodeId < activeNodeListSize;nodeId++) {
			pt_->setAckHeartbeat(activeNodeList[nodeId], false);
			const NodeDescriptor &nd = clsEE_->getServerND(activeNodeList[nodeId]);
			if (!nd.isEmpty()) {
				clsEE_->send(heartbeatEvent, nd);
			}
		}
	}
}

void TimerCheckClusterHandler::sendUpdatePartitionInfo(EventContext &ec,
		ClusterManager::UpdatePartitionInfo &updatePartitionInfo, std::vector<NodeId> &activeNodeList) {
	if (updatePartitionInfo.isNeedUpdatePartition()) {
		Event updatePartitionEvent(ec, CS_UPDATE_PARTITION, CS_HANDLER_PARTITION_ID);
		EventByteOutStream out = updatePartitionEvent.getOutStream();
		clsSvc_->encode(updatePartitionInfo, out);
		if (updatePartitionInfo.dropPartitionNodeInfo_.getSize() > 0) {
			clsSvc_->encode(updatePartitionInfo.dropPartitionNodeInfo_, out);
		}
		for (std::vector<NodeId>::iterator it = activeNodeList.begin();
			it != activeNodeList.end(); it++) {
			NodeId nodeId = (*it);
			if (nodeId == 0) {
				clsEE_->add(updatePartitionEvent);
			}
			else {
				const NodeDescriptor &nd = clsEE_->getServerND(nodeId);
				if (!nd.isEmpty()) {
					clsEE_->send(updatePartitionEvent, nd);
				}
			}
		}
	}
}

void TimerCheckClusterHandler::doAfterHeartbeatCheckInfo(EventContext &ec,
		ClusterManager::HeartbeatCheckInfo &heartbeatCheckInfo) {
	util::StackAllocator &alloc = heartbeatCheckInfo.getAllocator();
	ClusterStatusTransition nextTransition = heartbeatCheckInfo.getNextTransition();
	if (nextTransition == TO_SUBMASTER) {
		for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
			clsSvc_->requestChangePartitionStatus(ec, alloc, pId, PartitionTable::PT_OFF);
		}
	}
	clsMgr_->updateClusterStatus(nextTransition);
}

void TimerCheckClusterHandler::doAfterUpdatePartitionInfo(EventContext &ec,
		ClusterManager::HeartbeatInfo &heartbeatInfo,
		ClusterManager::HeartbeatCheckInfo &heartbeatCheckInfo) {
	util::StackAllocator &alloc = ec.getAllocator();
	sendHeartbeat(ec, heartbeatInfo, heartbeatCheckInfo);
	if (pt_->isMaster()) {
		bool clusterChange = clsMgr_->isUpdatePartition();
		ClusterStatusTransition nextTransition = heartbeatCheckInfo.getNextTransition();
		ClusterManager::UpdatePartitionInfo updatePartitionInfo(alloc, 0, pt_,
				heartbeatCheckInfo.isAddNewNode(),
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
void TimerCheckClusterHandler::operator()(EventContext &ec, Event &ev) {
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


		ClusterManager::HeartbeatCheckInfo heartbeatCheckInfo(alloc);
		clsMgr_->getHeartbeatCheckInfo(heartbeatCheckInfo);
		doAfterHeartbeatCheckInfo(ec, heartbeatCheckInfo);
		if (!pt_->isFollower()) {
			ClusterManager::HeartbeatInfo heartbeatInfo(
					alloc, 0, pt_, heartbeatCheckInfo.isAddNewNode());
			clsMgr_->setHeartbeatInfo(heartbeatInfo);
			clsMgr_->getHeartbeatInfo(heartbeatInfo);
			doAfterUpdatePartitionInfo(ec, heartbeatInfo, heartbeatCheckInfo);
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
void TimerNotifyClusterHandler::operator()(EventContext &ec, Event &ev) {
	EventType eventType = ev.getType();
	try {
		clsMgr_->checkNodeStatus();
		ClusterService::NotificationManager &manager = clsSvc_->getNotificationManager();
		ClusterNotificationMode mode = manager.getMode();
		switch (eventType) {
		case CS_UPDATE_PROVIDER: {
			if (!manager.next()) {
				Event checkResolverEvent(ec, CS_CHECK_PROVIDER, CS_HANDLER_PARTITION_ID);
				clsEE_->addTimer(checkResolverEvent, manager.checkResolverInterval());
			}
			return;
		}
		case CS_CHECK_PROVIDER: {
			int32_t interval = manager.check();
			if (interval > 0) {
				Event checkResolverEvent(ec, CS_CHECK_PROVIDER, CS_HANDLER_PARTITION_ID);
				clsEE_->addTimer(checkResolverEvent, interval);
			}
			return;
		}
		default:
			break;
		}
		try {
			clsMgr_->checkClusterStatus(CS_TIMER_NOTIFY_CLUSTER);
		}
		catch (UserException &) {
			return;
		}
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);

		switch (eventType) {
		case CS_TIMER_NOTIFY_CLUSTER: {
			switch (mode) {
			case NOTIFICATION_MULTICAST: {
				ClusterManager::NotifyClusterInfo notifyClusterInfo(alloc, 0, pt_);
				clsMgr_->getNotifyClusterInfo(notifyClusterInfo);
				Event notifyClusterEvent(ec, CS_NOTIFY_CLUSTER, CS_HANDLER_PARTITION_ID);
				clsSvc_->encode(notifyClusterEvent, notifyClusterInfo);
				const NodeDescriptor &nd = clsEE_->getMulticastND();
				clsEE_->send(notifyClusterEvent, nd);
				break;
			}
			case NOTIFICATION_FIXEDLIST: {
				ClusterManager::NotifyClusterInfo notifyClusterInfo(alloc, 0, pt_);
				Event notifyClusterEvent(ec, CS_NOTIFY_CLUSTER, CS_HANDLER_PARTITION_ID);
				bool isFirst = true;
				for (int32_t nodeId = 1; nodeId < manager.getFixedNodeNum();nodeId++) {
					const NodeDescriptor &nd = clsEE_->getServerND(nodeId);
					if (isFirst) {
						clsMgr_->getNotifyClusterInfo(notifyClusterInfo);
						clsSvc_->encode(notifyClusterEvent, notifyClusterInfo);
						isFirst = false;
					}
					clsEE_->send(notifyClusterEvent, nd);
				}
				break;
			}
			case NOTIFICATION_RESOLVER: {
				ServiceAddressResolver *resolver = manager.getResolver();
				size_t entryCount = resolver->getEntryCount();
				if (entryCount == 0 && resolver->isAvailable()) {
					GS_TRACE_WARNING(CLUSTER_SERVICE, GS_TRACE_CS_CLUSTER_STATUS,
						"Notification provider is available, but notification "
						"member is null");
				}
				NodeId nodeId;
				bool isFirst = true;
				bool isAvailableSend = true;
				ClusterManager::NotifyClusterInfo notifyClusterInfo(alloc, 0, pt_);
				Event notifyClusterEvent(ec, CS_NOTIFY_CLUSTER, CS_HANDLER_PARTITION_ID);
				for (size_t pos = 0; pos < entryCount; pos++) {
					try {
						nodeId = UNDEF_NODEID;
						const util::SocketAddress &clusterSocketAddress =
							resolver->getAddress(
								pos, static_cast<uint32_t>(CLUSTER_SERVICE));
						const NodeDescriptor &nd = clsEE_->getServerND(clusterSocketAddress);
						if (nd.isEmpty()) {
							for (int32_t i = 0; i < SERVICE_MAX; i++) {
								ServiceType serviceType = static_cast<ServiceType>(i);
								EventEngine *ee = clsSvc_->getEE(serviceType);
								const util::SocketAddress &currentSocketAddress =
										resolver->getAddress(pos, i);
								if (serviceType == SYSTEM_SERVICE) {
									NodeAddress ptAddress;
									ptAddress.set(currentSocketAddress);
									if (pt_->setNodeInfo(nodeId, serviceType, ptAddress)) {
										GS_TRACE_INFO(CLUSTER_INFO_TRACE, GS_TRACE_CS_CLUSTER_STATUS,
											"[RESOLVER] Set entry "
												<< SERVICE_TYPE_NAMES[i]
												<< ", address = "
												<< currentSocketAddress
												<< ", nodeId = " << nodeId);
									}
									continue;
								}
								if (nodeId != UNDEF_NODEID &&
									pt_->checkSetService(nodeId, serviceType)) {
									GS_TRACE_INFO(CLUSTER_INFO_TRACE, GS_TRACE_CS_CLUSTER_STATUS,
										"[RESOLVER] Duplicate entry "
											<< SERVICE_TYPE_NAMES[i]
											<< ", address = "
											<< currentSocketAddress
											<< ", nodeId = " << nodeId);
									continue;
								}
								if (serviceType != CLUSTER_SERVICE) {
									ee->setServerNodeId(currentSocketAddress, nodeId, false);
								}
								const NodeDescriptor &nd =
									ee->resolveServerND(currentSocketAddress);
								if (!nd.isEmpty()) {
									if (serviceType == CLUSTER_SERVICE) {
										nodeId = static_cast<NodeId>(nd.getId());
									}
									else {
										NodeId tmpNodeId = static_cast<NodeId>(nd.getId());
										if (tmpNodeId != nodeId) {
											GS_THROW_USER_ERROR(
												GS_ERROR_CS_ENTRY_ADDRESS_FAILED,
												"[RESOLVER] Set entry failed, "
												"unmatch cluster nodeId, "
												"cluster = "
													<< nodeId << ", "
													<< SERVICE_TYPE_NAMES[i]
													<< " = " << tmpNodeId);
										}
									}
									NodeAddress ptAddress;
									ptAddress.set(currentSocketAddress);
									if (pt_->setNodeInfo(nodeId, serviceType, ptAddress)) {
										GS_TRACE_INFO(CLUSTER_INFO_TRACE, GS_TRACE_CS_CLUSTER_STATUS,
											"[RESOLVER] Set entry "
												<< SERVICE_TYPE_NAMES[i]
												<< ", address = "
												<< currentSocketAddress
												<< ", nodeId = " << nodeId);
									}
								}
								else {
									GS_THROW_USER_ERROR(
										GS_ERROR_CS_GET_INVALID_NODE_ADDRESS,
										"Failed to get address "
										"(address = "
											<< currentSocketAddress
											<< ", nodeId = " << nodeId << ")");
								}
							}
						}
						else {
							nodeId = static_cast<NodeId>(nd.getId());
						}
						{
							const NodeDescriptor &nd = clsEE_->getServerND(nodeId);
							if (isFirst && isAvailableSend) {
								try {
									clsMgr_->getNotifyClusterInfo(notifyClusterInfo);
								}
								catch (std::exception &e3) {
									isAvailableSend = false;
									UTIL_TRACE_EXCEPTION(CLUSTER_SERVICE, e3, "");
									break;
								}
								clsSvc_->encode(notifyClusterEvent, notifyClusterInfo);
								isFirst = false;
							}
							if (!isAvailableSend) {
								continue;
							}
							clsEE_->send(notifyClusterEvent, nd);
						}
					}
					catch (std::exception &e2) {
						UTIL_TRACE_EXCEPTION(CLUSTER_SERVICE, e2, "");
					}
				}
				break;
			}
			default: {
				GS_THROW_USER_ERROR(GS_ERROR_CS_ERROR_INTERNAL,
					"Invalid notification mode (mode= " << static_cast<uint32_t>(mode) << ")");
			}
			}
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
void TimerNotifyClientHandler::operator()(EventContext &ec, Event &ev) {
	EventType eventType = ev.getType();
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

		Event notifyClientEvent(ec, RECV_NOTIFY_MASTER, CS_HANDLER_PARTITION_ID);
		clsSvc_->encodeNotifyClient(notifyClientEvent);
		const NodeDescriptor &nd = txnEE_->getMulticastND();
		if (!nd.isEmpty()) {
			notifyClientEvent.setPartitionIdSpecified(false);
			txnEE_->send(notifyClientEvent, nd);
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID, "ND is empty");
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

void UpdatePartitionHandler::checkAndRequestDropPartition(EventContext &ec,
		ClusterManager::UpdatePartitionInfo &updatePartitionInfo)  {
	util::StackAllocator &alloc = ec.getAllocator();
	DropPartitionNodeInfo &dropPartitionNodeInfo = updatePartitionInfo.dropPartitionNodeInfo_;
	dropPartitionNodeInfo.init();
	for (size_t pos = 0; pos < dropPartitionNodeInfo.getSize(); pos++) {
		DropNodeSet *dropInfo = dropPartitionNodeInfo.get(pos);
		NodeAddress &selfAddress = pt_->getNodeAddress(SELF_NODEID);
		if (dropInfo->getNodeAddress() == selfAddress) {
			for (size_t i = 0; i < dropInfo->pIdList_.size(); i++) {
				syncSvc_->requestDrop(ec, alloc, dropInfo->pIdList_[i], false,
						dropInfo->revisionList_[i]);
			}
			break;
		}
	}
}

void UpdatePartitionHandler::updateNodeInfo(EventContext &ec,
		ClusterManager::UpdatePartitionInfo &updatePartitionInfo) {
	bool needCancel = false;
	SubPartitionTable &subPartitionTable = updatePartitionInfo.getSubPartitionTable();
	clsSvc_->updateNodeList(subPartitionTable.getNodeAddressList());

	for (int32_t pos = 0; pos < subPartitionTable.size(); pos++) {
		resolveAddress(subPartitionTable.getSubPartition(pos));
	}

	if (pt_->isFollower()) {
		std::vector<LogSequentialNumber> &maxLsnList =
			updatePartitionInfo.getMaxLsnList();
		for (PartitionId pId = 0; pId < maxLsnList.size(); pId++) {
			pt_->setRepairedMaxLsn(pId, maxLsnList[pId]);
		}
		std::vector<AddressInfo> &addressInfoList = subPartitionTable.getNodeAddressList();
		NodeId masterNodeId = pt_->getMaster();
		for (size_t pos = 0; pos < addressInfoList.size(); pos++) {
			NodeId nodeId = ClusterService::changeNodeId(
				addressInfoList[pos].clusterAddress_, clsEE_);
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
}
	
void ClusterHandler::resolveAddress(PartitionRole &role) {
	NodeAddress nextOwnerAddress;
	std::vector<NodeAddress> backupAddressList;
	std::vector<NodeAddress> catchupAddressList;
	role.get(nextOwnerAddress, backupAddressList, catchupAddressList);
	NodeId nextOwnerId = ClusterService::changeNodeId(nextOwnerAddress, clsEE_);
	std::vector<NodeId> nextBackups, nextCatchups;
	for (int32_t backupPos = 0;
		backupPos < static_cast<int32_t>(backupAddressList.size());
		backupPos++) {
		NodeId nextBackupId = ClusterService::changeNodeId(
			backupAddressList[backupPos], clsEE_);
		if (nextBackupId == UNDEF_NODEID) {
			GS_THROW_USER_ERROR(GS_ERROR_PT_INVALID_NODE_ADDRESS,
					"Specify invalid node address=" << backupAddressList[backupPos].dump());
		}
		nextBackups.push_back(nextBackupId);
	}
	for (int32_t cachupPos = 0;
			cachupPos < static_cast<int32_t>(catchupAddressList.size());
			cachupPos++) {
		NodeId nextCatchupId = ClusterService::changeNodeId(
				catchupAddressList[cachupPos], clsEE_);
		if (nextCatchupId == UNDEF_NODEID) {
			GS_THROW_USER_ERROR(GS_ERROR_PT_INVALID_NODE_ADDRESS, 
					"Specify invalid node address" << catchupAddressList[cachupPos].dump());
		}
		nextCatchups.push_back(nextCatchupId);
	}
	role.set(nextOwnerId, nextBackups, nextCatchups);
}

bool UpdatePartitionHandler::resolveAddress(SubPartition &subPartition) {
	ClusterHandler::resolveAddress(subPartition.role_);
	NodeId currentOwner = ClusterService::changeNodeId(
		subPartition.currentOwner_, clsEE_);
	subPartition.currentOwnerId_ = currentOwner;
	return (currentOwner == 0);
}

void UpdatePartitionHandler::requestSync(EventContext &ec,
		PartitionId pId, PartitionRole &role, bool isShorttermSync) {
	EventType eventType = TXN_SHORTTERM_SYNC_REQUEST;
	SyncMode syncMode = MODE_SHORTTERM_SYNC;
	if (!isShorttermSync) {
		eventType = TXN_LONGTERM_SYNC_REQUEST;
		syncMode = MODE_LONGTERM_SYNC;
	}
	util::StackAllocator &alloc = ec.getAllocator();
	SyncRequestInfo syncRequestInfo(alloc,
		eventType, syncMgr_, pId, clsEE_,
		syncMode);
	syncRequestInfo.setPartitionRole(role);
	Event requestEvent(ec, eventType, pId);
	EventByteOutStream out = requestEvent.getOutStream();
	syncSvc_->encode(requestEvent, syncRequestInfo, out);
	txnEE_->addTimer(requestEvent, EE_PRIORITY_HIGH);
}

/*!
	@brief Handler Operator
*/
void UpdatePartitionHandler::operator()(EventContext &ec, Event &ev) {
	EventType eventType = ev.getType();
	try {
		clsMgr_->checkNodeStatus();
		clsMgr_->checkClusterStatus(CS_UPDATE_PARTITION);
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);
		NodeId senderNodeId = ClusterService::resolveSenderND(ev);
		ClusterManager::UpdatePartitionInfo updatePartitionInfo(alloc, senderNodeId, pt_);
		EventByteInStream in = ev.getInStream();
		clsSvc_->decode(alloc, ev, updatePartitionInfo, in);
		if (in.base().remaining()) {
			clsSvc_->decode(alloc, ev, updatePartitionInfo.dropPartitionNodeInfo_, in);
			DropPartitionNodeInfo &dropPartitionNodeInfo = updatePartitionInfo.dropPartitionNodeInfo_;
			dropPartitionNodeInfo.init();
			for (size_t pos = 0; pos < dropPartitionNodeInfo.getSize(); pos++) {
				DropNodeSet *dropInfo = dropPartitionNodeInfo.get(pos);
				NodeAddress &selfAddress = pt_->getNodeAddress(SELF_NODEID);
				if (dropInfo->getNodeAddress() == selfAddress) {
					for (size_t i = 0; i < dropInfo->pIdList_.size(); i++) {
						syncSvc_->requestDrop(ec, alloc, dropInfo->pIdList_[i], false,
								dropInfo->revisionList_[i]);
					}
					break;
				}
			}
		}
		updateNodeInfo(ec, updatePartitionInfo);
		checkAndRequestDropPartition(ec, updatePartitionInfo);
		SubPartitionTable &subPartitionTable = updatePartitionInfo.getSubPartitionTable();
		clsMgr_->setUpdatePartitionInfo(updatePartitionInfo);
		util::Vector<PartitionId> &shorttermSyncPIdList = updatePartitionInfo.getShorttermSyncPIdList();
		util::Vector<PartitionId> &longtermSyncPIdList = updatePartitionInfo.getLongtermSyncPIdList();
		util::Vector<PartitionId> &changePartitionPIdList = updatePartitionInfo.getChangePartitonPIdList();
		for (PartitionId pos = 0; pos < shorttermSyncPIdList.size(); pos++) {
			PartitionRole &role = subPartitionTable.getSubPartition(shorttermSyncPIdList[pos]).role_;
			PartitionId pId = role.getPartitionId();
			SyncRequestInfo syncRequestInfo(alloc,
				TXN_SHORTTERM_SYNC_REQUEST, syncMgr_, pId, clsEE_,
				MODE_SHORTTERM_SYNC);
			requestSync(ec, pId, role, true);
		}
		for (PartitionId pos = 0; pos < longtermSyncPIdList.size(); pos++) {
			PartitionRole &role = subPartitionTable.getSubPartition(longtermSyncPIdList[pos]).role_;
			PartitionId pId = role.getPartitionId();
			SyncRequestInfo syncRequestInfo(alloc,
				TXN_LONGTERM_SYNC_REQUEST, syncMgr_, pId, clsEE_,
				MODE_LONGTERM_SYNC);
			requestSync(ec, pId, role, false);
		}
		for (PartitionId pos = 0; pos < changePartitionPIdList.size(); pos++) {
			PartitionRole &role = subPartitionTable.getSubPartition(changePartitionPIdList[pos]).role_;
			PartitionId pId = role.getPartitionId();
			ClusterManager::ChangePartitionTableInfo
				changePartitionTableInfo(alloc, 0, role);
			clsSvc_->request(ec, TXN_CHANGE_PARTITION_TABLE,
					pId, txnEE_, changePartitionTableInfo);
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
void ClusterService::changeAddress(NodeAddress &nodeAddress, NodeId nodeId,
		EventEngine *ee) {
	util::SocketAddress::Inet address;
	uint16_t port;
	const NodeDescriptor &nd = ee->getServerND(nodeId);
	if (nd.isEmpty()) {
		GS_THROW_USER_ERROR(GS_ERROR_EE_PARAMETER_INVALID, "ND is empty");
	}
	nd.getAddress().getIP(&address, &port);
	nodeAddress.address_ = *reinterpret_cast<AddressType *>(&address);
	nodeAddress.port_ = port;
}

/*!
	@brief Gets node id
*/
NodeId ClusterService::changeNodeId(NodeAddress &nodeAddress, EventEngine *ee) {
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
NodeId ClusterHandler::checkAddress(PartitionTable *pt,
		NodeAddress &address, NodeId clusterNodeId, ServiceType serviceType, EventEngine *ee) {
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
			*(util::SocketAddress::Inet *)&address.address_, address.port_);
		NodeId nodeId;
		if (serviceType != CLUSTER_SERVICE) {
			ee->setServerNodeId(socketAddress, clusterNodeId, false);
		}
		const NodeDescriptor &nd = ee->resolveServerND(socketAddress);
		if (!nd.isEmpty()) {
			nodeId = static_cast<NodeId>(nd.getId());
			pt->setNodeInfo(nodeId, serviceType, address);
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
template <class T>
void ClusterService::updateNodeList(T &addressInfoList) {
	try {
		for (typename T::iterator it = addressInfoList.begin();
			it != addressInfoList.end(); it++) {
			NodeId clusterNodeId = UNDEF_NODEID, tmpNodeId;
			EventEngine *targetEE;
			for (int32_t serviceType = 0; serviceType < SERVICE_MAX;serviceType++) {
				ServiceType currentServiceType = static_cast<ServiceType>(serviceType);
				if (clusterNodeId == 0) {
					break;
				}
				AddressInfo *tmpInfo = const_cast<AddressInfo *>(&(*it));
				NodeAddress &targetAddress = tmpInfo->getNodeAddress(currentServiceType);
				if (!targetAddress.isValid()) {
					continue;
				}
				targetEE = getEE(currentServiceType);
				tmpNodeId = ClusterHandler::checkAddress(pt_,
					targetAddress, clusterNodeId, currentServiceType, targetEE);
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
	try {
		clsMgr_->checkNodeStatus();
		clsMgr_->checkClusterStatus(TXN_CHANGE_PARTITION_STATE);
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);
		ClusterManager::ChangePartitionStatusInfo changePartitionStatusInfo(alloc);
		clsSvc_->decode(alloc, ev, changePartitionStatusInfo);
		PartitionStatus changeStatus;
		changePartitionStatusInfo.getPartitionStatus(pId, changeStatus);
		txnSvc_->changeTimeoutCheckMode(pId, changeStatus,
			changePartitionStatusInfo.getPartitionChangeType(),
			changePartitionStatusInfo.isToSubMaster(), clsSvc_->getStats());
		clsMgr_->setChangePartitionStatusInfo(changePartitionStatusInfo);
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
	try {
		clsMgr_->checkNodeStatus();
		clsMgr_->checkClusterStatus(TXN_CHANGE_PARTITION_TABLE);
		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);

		ClusterManager::ChangePartitionTableInfo changePartitionTableInfo(alloc, 0);
		clsSvc_->decode(alloc, ev, changePartitionTableInfo);
		PartitionRole &nextRole = changePartitionTableInfo.getPartitionRole();
		ClusterHandler::resolveAddress(nextRole);
		if (clsMgr_->setChangePartitionTableInfo(changePartitionTableInfo)) {
			txnSvc_->checkNoExpireTransaction(alloc, pId);
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

		TRACE_CLUSTER_NORMAL_OPERATION(INFO, "Complete shutdown node operation");
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
	ClusterManager *clsMgr,  util::VariableSizeAllocator<> &valloc)
	: clsMgr_(clsMgr),
	pt_(clsMgr->getPartitionTable()),
	valloc_(valloc),
	mode_(NOTIFICATION_MULTICAST),
	fixedNodeNum_(0),
	resolverUpdateInterval_(0),
	resolverCheckInterval_(DEFAULT_CHECK_INTERVAL),
	resolverCheckLongInterval_(LONG_CHECK_INTERVAL),
	value_(UTIL_NEW picojson::value)
{
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

/*!
	@brief Initializer
*/
void ClusterService::NotificationManager::initialize(const ConfigTable &config) {
	try {
		const int32_t notificationInterval =
			config.get<int32_t>(CONFIG_TABLE_CS_NOTIFICATION_INTERVAL);
		const picojson::value &memberValue =
			config.get<picojson::value>(CONFIG_TABLE_CS_NOTIFICATION_MEMBER);
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
				UTIL_NEW ServiceAddressResolver(valloc_, resolverConfig);
			resolverList_.push_back(resolver);
			for (int32_t i = 0; i < SERVICE_MAX; i++) {
				resolver->initializeType(
					static_cast<ServiceType>(i), SERVICE_TYPE_NAMES[i]);
			}
			if (mode_ == NOTIFICATION_FIXEDLIST) {
				resolver->importFrom(memberValue);
				resolver->normalize();

#define CLUSTER_SET_RESOLVER_ADDRESS(resolver, info, index, type) \
	info.type##Address_.set(                                      \
		resolver->getAddress(index, resolver->getType(#type)))

				const size_t count = resolver->getEntryCount();
				for (size_t index = 0; index < count; index++) {
					AddressInfo info;
					memset(&info, 0, sizeof(AddressInfo));

					CLUSTER_SET_RESOLVER_ADDRESS(
						resolver, info, index, cluster);
					CLUSTER_SET_RESOLVER_ADDRESS(
						resolver, info, index, transaction);
					CLUSTER_SET_RESOLVER_ADDRESS(resolver, info, index, sync);
					CLUSTER_SET_RESOLVER_ADDRESS(resolver, info, index, system);
					fixedAddressInfoSet_.insert(info);
				}
				fixedNodeNum_ = static_cast<int32_t>(count);
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
		config.get<picojson::value>(CONFIG_TABLE_CS_NOTIFICATION_MEMBER);
	const char8_t *providerURL =
		config.get<const char8_t *>(CONFIG_TABLE_CS_NOTIFICATION_PROVIDER_URL);
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
NodeAddressSet &ClusterService::NotificationManager::getFixedAddressInfo() {
	return fixedAddressInfoSet_;
}

/*!
	@brief Gets NotificationManager.
*/
ClusterService::NotificationManager &ClusterService::getNotificationManager() {
	return notificationManager_;
}

/*!
	@brief Requests the resolver the next update.
*/
bool ClusterService::NotificationManager::next() {
	try {
		for (size_t pos = 0; pos < resolverList_.size(); pos++) {
			size_t readSize = 0;
			const bool completed = resolverList_[pos]->checkUpdated(&readSize);
			const bool available = resolverList_[pos]->isAvailable();
			if (!completed && available) {
				clsMgr_->reportError(GS_ERROR_CS_PROVIDER_TIMEOUT);
				GS_TRACE_INFO(CLUSTER_INFO_TRACE, GS_TRACE_CS_CLUSTER_STATUS,
					"Previous provider update request timeout");
			}
			if (!available) {
				GS_TRACE_INFO(CLUSTER_INFO_TRACE, GS_TRACE_CS_CLUSTER_STATUS,
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


void SystemCommandHandler::doJoinCluster(ClusterManager *clsMgr,
		ClusterManager::JoinClusterInfo &joinClusterInfo, EventContext &ec) {

	clsMgr->checkClusterStatus(CS_JOIN_CLUSTER);
	clsMgr->checkCommandStatus(CS_JOIN_CLUSTER);

	TRACE_CLUSTER_NORMAL_OPERATION(INFO, "Called Join cluster");

	util::StackAllocator &alloc = ec.getAllocator();
	PartitionTable *pt = clsMgr->getPartitionTable();
	clsMgr->setJoinClusterInfo(joinClusterInfo);
	for (PartitionId pId = 0; pId < pt->getPartitionNum(); pId++) {
		clsSvc_->requestChangePartitionStatus(ec, alloc, pId, PartitionTable::PT_OFF);
	}
	clsMgr->updateNodeStatus(CS_JOIN_CLUSTER);
}

void SystemCommandHandler::doLeaveCluster(ClusterManager *clsMgr,
		CheckpointService *cpSvc, ClusterManager::LeaveClusterInfo &leaveClusterInfo,
		EventContext &ec) {

	clsMgr->checkClusterStatus(CS_LEAVE_CLUSTER);
	clsMgr->checkCommandStatus(CS_LEAVE_CLUSTER);

	TRACE_CLUSTER_NORMAL_OPERATION(INFO, "Called Leave cluster");

	util::StackAllocator &alloc = ec.getAllocator();
	clsMgr->setLeaveClusterInfo(leaveClusterInfo);
	PartitionTable *pt = clsMgr->getPartitionTable();
	for (PartitionId pId = 0; pId < pt->getPartitionNum(); pId++) {
		clsSvc_->requestChangePartitionStatus(ec, alloc, pId, PartitionTable::PT_STOP);
	}
	clsMgr->updateClusterStatus(TO_SUBMASTER, true);
	clsMgr->updateNodeStatus(CS_LEAVE_CLUSTER);
	if (clsMgr->isShutdownPending()) {
		TRACE_CLUSTER_NORMAL_OPERATION(INFO,
				"Normal shutdown operation is started by pending operation");
		clsMgr->checkClusterStatus(CS_SHUTDOWN_NODE_NORMAL);
		clsMgr->checkCommandStatus(CS_SHUTDOWN_NODE_NORMAL);
		if (cpSvc) cpSvc->requestShutdownCheckpoint(ec);
		clsMgr->updateNodeStatus(CS_SHUTDOWN_NODE_NORMAL);
	}
}

void SystemCommandHandler::doIncreaseCluster(ClusterManager *clsMgr,
		ClusterManager::IncreaseClusterInfo &increaseClusterInfo) {
	clsMgr->checkClusterStatus(CS_INCREASE_CLUSTER);
	clsMgr->checkCommandStatus(CS_INCREASE_CLUSTER);
	clsMgr->setIncreaseClusterInfo(increaseClusterInfo);
}

void SystemCommandHandler::doDecreaseCluster(ClusterManager *clsMgr,
		ClusterManager::DecreaseClusterInfo &decreaseClusterInfo, EventContext &ec) {
	clsMgr->checkClusterStatus(CS_DECREASE_CLUSTER);
	clsMgr->checkCommandStatus(CS_DECREASE_CLUSTER);
	util::StackAllocator &alloc = ec.getAllocator();
	clsMgr->setDecreaseClusterInfo(decreaseClusterInfo);
	std::vector<NodeId> &leaveNodeList = decreaseClusterInfo.getLeaveNodeList();
	PartitionTable *pt = clsMgr->getPartitionTable();
	if (leaveNodeList.size() > 0 && decreaseClusterInfo.isAutoLeave()) {
		TRACE_CLUSTER_NORMAL_OPERATION(INFO,
				"Auto leaving node is started by decrease cluster operation (leaveNodes="
				<< pt->dumpNodeAddressList(leaveNodeList) << ")");
		Event leaveEvent(ec, CS_LEAVE_CLUSTER, CS_HANDLER_PARTITION_ID);
		ClusterManager::LeaveClusterInfo leaveClusterInfo(alloc, 0, false, true);
		clsSvc_->encode(leaveEvent, leaveClusterInfo);
		EventEngine *ee = clsSvc_->getEE();
		for (size_t pos = 0; pos < leaveNodeList.size(); pos++) {
			const NodeDescriptor &nd = ee->getServerND(leaveNodeList[pos]);
			if (!nd.isEmpty()) {
				ee->send(leaveEvent, nd);
			}
		}
	}
}

void SystemCommandHandler::doShutdownNodeForce(ClusterManager *clsMgr,
	CheckpointService *cpSvc, TransactionService *txnSvc, SyncService *syncSvc,
	SystemService *sysSvc
	) {

	clsMgr->checkClusterStatus(CS_SHUTDOWN_NODE_FORCE);
	clsMgr->checkCommandStatus(CS_SHUTDOWN_NODE_FORCE);

	if (sysSvc) sysSvc->shutdown();
	clsSvc_->shutdown();
	if (cpSvc) cpSvc->shutdown();
	if (txnSvc) txnSvc->shutdown();
	if (syncSvc) syncSvc->shutdown();
#if defined(SYSTEM_CAPTURE_SIGNAL)
	pid_t pid = getpid();
	kill(pid, SIGINT);
#endif  
	
	TRACE_CLUSTER_NORMAL_OPERATION(INFO, "Force shutdown node operation is completed");
}

void SystemCommandHandler::doShutdownNormal(ClusterManager *clsMgr, CheckpointService *cpSvc,
		ClusterManager::ShutdownNodeInfo &shutdownNodeInfo, EventContext &ec) {

	clsMgr->checkClusterStatus(CS_SHUTDOWN_NODE_NORMAL);
	clsMgr->checkCommandStatus(CS_SHUTDOWN_NODE_NORMAL);
	if (cpSvc) cpSvc->requestShutdownCheckpoint(ec);
	clsMgr->updateNodeStatus(CS_SHUTDOWN_NODE_NORMAL);
	TRACE_CLUSTER_NORMAL_OPERATION(INFO, "Normal shutdown node operation is completed");
}

void SystemCommandHandler::doShutdownCluster(ClusterManager *clsMgr,
		EventContext &ec, NodeId senderNodeId) {
	
	clsMgr->checkClusterStatus(CS_SHUTDOWN_CLUSTER);
	clsMgr->checkCommandStatus(CS_SHUTDOWN_CLUSTER);

	util::StackAllocator &alloc = ec.getAllocator();
	Event leaveEvent(ec, CS_LEAVE_CLUSTER, CS_HANDLER_PARTITION_ID);
	ClusterManager::LeaveClusterInfo leaveClusterInfo(alloc, senderNodeId);
	clsSvc_->encode(leaveEvent, leaveClusterInfo);

	PartitionTable *pt = clsMgr->getPartitionTable();
	util::XArray<NodeId> liveNodeIdList(alloc);
	pt->getLiveNodeIdList(liveNodeIdList);
	TRACE_CLUSTER_NORMAL_OPERATION(INFO,
			"Shutdown cluster (nodeNum=" << liveNodeIdList.size() << ", addressList="
			<< pt_->dumpNodeAddressList(liveNodeIdList) << ")");
	for (size_t pos = 0; pos < liveNodeIdList.size(); pos++) {
		EventEngine *ee = clsSvc_->getEE();
		const NodeDescriptor &nd = ee->getServerND(liveNodeIdList[pos]);
		if (!nd.isEmpty()) {
			if (nd.getId() == 0) {
				ee->add(leaveEvent);
			}
			else {
				ee->send(leaveEvent, nd);
			}
		}
	}
	TRACE_CLUSTER_NORMAL_OPERATION(INFO, "Shutdown cluster operation is completed");
}

void SystemCommandHandler::doCompleteCheckpointForShutdown(ClusterManager *clsMgr,
	CheckpointService *cpSvc, TransactionService *txnSvc, SyncService *syncSvc,
	SystemService *sysSvc
	) {

	clsMgr->checkClusterStatus(CS_COMPLETE_CHECKPOINT_FOR_SHUTDOWN);
	clsMgr->checkCommandStatus(CS_COMPLETE_CHECKPOINT_FOR_SHUTDOWN);

	TRACE_CLUSTER_NORMAL_OPERATION(INFO,
			"Shutdown checkpoint is completed, shutdown all services");
	if (sysSvc) sysSvc->shutdown();
	clsSvc_->shutdown();
	if (cpSvc) cpSvc->shutdown();
	if (txnSvc) txnSvc->shutdown();
	if (syncSvc) syncSvc->shutdown();

#if defined(SYSTEM_CAPTURE_SIGNAL)
	pid_t pid = getpid();
	kill(pid, SIGTERM);
#endif  

	clsMgr->updateNodeStatus(CS_COMPLETE_CHECKPOINT_FOR_SHUTDOWN);
	TRACE_CLUSTER_NORMAL_OPERATION(INFO,
			"Normal shutdown node operation is completed.");
}

void SystemCommandHandler::doCompleteCheckpointForRecovery(ClusterManager *clsMgr,
	CheckpointService *cpSvc, TransactionService *txnSvc, SyncService *syncSvc,
	SystemService *sysSvc
	) {
	TRACE_CLUSTER_NORMAL_OPERATION(INFO, "Recovery checkpoint is completed, start all services");
	{
		util::LockGuard<util::Mutex> lock(clsMgr->getClusterLock());
		if (!clsMgr->isSignalBeforeRecovery()) {
			clsMgr->checkClusterStatus(CS_COMPLETE_CHECKPOINT_FOR_RECOVERY);
			clsMgr->checkCommandStatus(CS_COMPLETE_CHECKPOINT_FOR_RECOVERY);
			clsMgr->updateNodeStatus(CS_COMPLETE_CHECKPOINT_FOR_RECOVERY);
		}
		else {
			if (sysSvc) sysSvc->shutdown();
			clsSvc_->shutdown();
			if (cpSvc) cpSvc->shutdown();
			if (txnSvc) txnSvc->shutdown();
			if (syncSvc) syncSvc->shutdown();

#if defined(SYSTEM_CAPTURE_SIGNAL)
			pid_t pid = getpid();
			kill(pid, SIGTERM);
#endif  
			TRACE_CLUSTER_NORMAL_OPERATION(INFO, "Normal shutdown node operation is completed");
		}
	}
}
