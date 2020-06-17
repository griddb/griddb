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
	@brief Definition of ClusterService
*/

#ifndef CLUSTER_SERVICE_H_
#define CLUSTER_SERVICE_H_

#include "util/container.h"
#include "cluster_common.h"
#include "cluster_event_type.h"
#include "partition_table.h"
#include "event_engine.h"
#include "system_service.h"
#include "service_address.h"
#include "resource_set.h"

class HeartbeatInfo;
class NotifyClusterInfo;
class UpdatePartitionInfo;
class JoinClusterInfo;
class LeaveClusterInfo;
class IncreaseClusterInfo;
class DecreaseClusterInfo;
class ShutdownNodeInfo;
class HeartbeatCheckInfo;

class ServiceConfig {

public:
	
	static const char8_t *TYPE_NAME_CLUSTER;
	static const char8_t *TYPE_NAME_TRANSACTION;
	static const char8_t *TYPE_NAME_SYNC;
	static const char8_t *TYPE_NAME_SYSTEM;
	static const char8_t *TYPE_NAME_SQL;
	static const char8_t *TYPE_NAME_TRANSACTION_LOCAL;
	static const char8_t *TYPE_NAME_SQL_LOCAL;

	static const char8_t *const SERVICE_TYPE_NAMES[];
	static const char8_t *const SERVICE_TYPE_NAMES_WITH_PUBLIC[];

	static ServiceType SERVICE_TYPE_LIST[];
	static int32_t SERVICE_TYPE_LIST_MAP[];
	static ServiceType SERVICE_TYPE_LIST_WITH_PUBLIC[];
	static int32_t SERVICE_TYPE_LIST_MAP_WITH_PUBLIC[];

	static const char8_t *dumpServiceType(
			ServiceType serviceType);
};

struct ClusterAdditionalServiceConfig {

	ClusterAdditionalServiceConfig(
			const ConfigTable &config,
			PartitionTable *pt = NULL);

	bool hasPublicAddress() {
		return hasPublic_;
	}

	const char *getAddress(
			ServiceType serviceType, int32_t type = 0);

	const char *getServiceAddress(
			ServiceType serviceType);
	
	uint16_t getPort(ServiceType serviceTy);

	bool isSetServiceAddress() {
		return (
			txnAddressSize_ > 0 
			&& sqlAddressSize_ > 0
			);
	}

	bool isSetLocalAddress() {
		return (
			txnInternalAddressSize_ > 0 
			&& sqlInternalAddressSize_ > 0
			);
	}

	const char *txnServiceAddress_;
	const char *txnLocalServiceAddress_;
	size_t txnAddressSize_;
	size_t txnInternalAddressSize_;
	uint16_t txnPort_;

	const char *sqlServiceAddress_;
	const char *sqlLocalServiceAddress_;
	size_t sqlAddressSize_;
	size_t sqlInternalAddressSize_;
	uint16_t sqlPort_;

	bool hasPublic_;
	int32_t *serviceTypeList_;
	int32_t serviceTypeSize_;
	const char8_t *const *serviceTypeNameList_;
};

/*!
	@brief Represents cluster statistics
*/
struct ClusterStats {

	ClusterStats(int32_t partitionNum) :
			partitionNum_(partitionNum),
			totalErrorCount_(0) {

		valueList_.assign(MAX_GET_TYPE, 0);
		eventErrorCount_.assign(EVENT_TYPE_MAX, 0);
		partitionErrorCount_.assign(partitionNum, 0);
		checkResourceList_.assign(partitionNum, 0);
	}

	/*!
		@brief cluster statistics type for setting
	*/
	enum SetType {
			CLUSTER_RECEIVE,
			CLUSTER_SEND,
			SYNC_RECEIVE,
			SYNC_SEND
	};

	static const int32_t TYPE_UNIT = 4;

	/*!
		@brief cluster statistics type for getting
	*/
	enum GetType {
		CLUSTER_RECEIVE_BYTE,
		CLUSTER_SEND_BYTE,
		SYNC_RECEIVE_BYTE,
		SYNC_SEND_BYTE,
		CLUSTER_RECEIVE_COUNT,
		CLUSTER_SEND_COUNT,
		SYNC_RECEIVE_COUNT,
		SYNC_SEND_COUNT,
		MAX_GET_TYPE
	};

	std::vector<int64_t> valueList_;
	std::vector<int32_t> eventErrorCount_;
	std::vector<int32_t> partitionErrorCount_;
	std::vector<uint8_t> checkResourceList_;

	uint32_t partitionNum_;
	uint32_t totalErrorCount_;

	void setTransactionTimeoutCheck(
			PartitionId pId, uint8_t flag) {
		checkResourceList_[pId] = flag;
	}

	uint32_t getNotTransactionTimeoutCheckCount(
			PartitionTable *pt);

	int32_t getErrorCount() {
		return totalErrorCount_;
	}

	void set(SetType type, int32_t size) {
		valueList_[type] += size;
		valueList_[type + TYPE_UNIT]++;
	}

	int64_t get(GetType type) {
		return valueList_[type];
	}

	int64_t getClusterByteAll() {
		return (
			valueList_[CLUSTER_RECEIVE_BYTE]
					+ valueList_[CLUSTER_SEND_BYTE]);
	}

	int64_t getSyncByteAll() {
		return (valueList_[SYNC_RECEIVE_BYTE]
				+ valueList_[SYNC_SEND_BYTE]);
	}

	int64_t getByteAll() {
		return (getClusterByteAll() + getSyncByteAll());
	}

	int64_t getClusterCountAll() {
		return (valueList_[SYNC_RECEIVE_BYTE]
				+ valueList_[SYNC_SEND_BYTE]);
	}

	int64_t getSyncCountAll() {
		return (valueList_[SYNC_RECEIVE_COUNT]
				+ valueList_[SYNC_SEND_COUNT]);
	}

	int64_t getCountAll() {
		return (getClusterCountAll()
				+ getSyncCountAll());
	}
};

static const uint32_t CS_HANDLER_PARTITION_ID = 0;

/*!
	@brief Handles generic event about cluster
*/
class ClusterHandler : public EventHandler {
public:

	ClusterHandler() :
			pt_(NULL),
			clsMgr_(NULL),
			clsSvc_(NULL) {}

	~ClusterHandler() {}

	void initialize(const ResourceSet &resourceSet);

	static NodeId checkAddress(PartitionTable *pt,
			NodeAddress &address,
			NodeId clusterNodeId,
			ServiceType serviceType,
			EventEngine *ee);

protected:

	void checkAutoNdNumbering(const NodeDescriptor &nd);

	void resolveAddress(PartitionRole &role);

	PartitionTable *pt_;
	ClusterManager *clsMgr_;
	ClusterService *clsSvc_;
	const ResourceSet *resourceSet_;
};

/*!
	@brief Handles heartbeat event
*/

class HeartbeatHandler : public ClusterHandler {

public:
	
	HeartbeatHandler() {}
	
	void operator()(EventContext &ec, Event &ev);

private:

	void doAfterHeartbeatInfo(EventContext &ec,
			HeartbeatInfo &heartbeatInfo);

	void decode(util::StackAllocator &alloc, Event &ev,
			HeartbeatInfo &heartbeatInfo,
			PublicAddressInfoMessage &publicAddressInfo);
};

class SQLTimerNotifyClientHandler : public ClusterHandler {

public:
	
	void operator()(EventContext &ec, Event &ev);

private:
	
	void encode(EventByteOutStream &out);
};

/*!
	@brief Handles NotifyCluster event
*/
class NotifyClusterHandler : public ClusterHandler {

public:
	
	NotifyClusterHandler() {}
	
	void operator()(EventContext &ec, Event &ev);

private:

	template<class T>
	void doAfterNotifyCluster(T &t);

	void decode(util::StackAllocator &alloc, Event &ev,
			NotifyClusterInfo &notifyClusterInfo);

	void handleError(Event &ev, UserException &e);
};

/*!
	@brief Handles UpdatePartition event
*/
class UpdatePartitionHandler : public ClusterHandler {

public:

	UpdatePartitionHandler() {}

	void operator()(EventContext &ec, Event &ev);

private:

	void checkAndRequestDropPartition(EventContext &ec,
			UpdatePartitionInfo &updatePartitionInfo);

	void updateNodeInfo(EventContext &ec,
			UpdatePartitionInfo &updatePartitionInfo);

	bool resolveAddress(SubPartition &subPartition);

	void requestSync(EventContext &ec,
			PartitionId pId, PartitionRole &role, bool isShorttermSync);

	void decode(EventContext &ec, Event &ev,
			UpdatePartitionInfo &updateParttiionInfo);
};

class NewSQLPartitionRefreshHandler : public ClusterHandler {

public:

	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles WebAPI command about Cluster
*/
class SystemCommandHandler : public ClusterHandler {

public:

	SystemCommandHandler() {}

	void operator()(EventContext &ec, Event &ev);

private:

	void doJoinCluster(
		JoinClusterInfo &joinClusterInfo,
		EventContext &ec);

	void doLeaveCluster(
			LeaveClusterInfo &leaveClusterInfo,
			EventContext &ec);

	void doIncreaseCluster(
			IncreaseClusterInfo &increaseClusterInfo);

	void doDecreaseCluster(
			DecreaseClusterInfo &decreaseClusterInfo,
			EventContext &ec);

	void doShutdownNodeForce();

	void doShutdownNormal(
			ShutdownNodeInfo &shutdownNodeInfo,
			EventContext &ec);

	void doShutdownCluster(
			EventContext &ec,
			NodeId senderNodeId);

	void doCompleteCheckpointForShutdown();

	void doCompleteCheckpointForRecovery();
};

class ClusterSystemCommandHandler : public ClusterHandler {

public:

	ClusterSystemCommandHandler() {}

	void operator()(EventContext &ec, Event &ev);

private:

	void doIncreaseCluster(
			IncreaseClusterInfo &increaseClusterInfo);

	void doDecreaseCluster(
			DecreaseClusterInfo &increaseClusterInfo,
			EventContext &ec);
};

/*!
	@brief Handles periodic CheckCluster event
*/
class TimerCheckClusterHandler : public ClusterHandler {

public:

	TimerCheckClusterHandler() {}

	void operator()(EventContext &ec, Event &ev);

private:

	void doAfterHeartbeatCheckInfo(EventContext &ec,
			HeartbeatCheckInfo &heartbeatCheckInfo);

	void doAfterUpdatePartitionInfo(EventContext &ec,
			HeartbeatInfo &heartbeatInfo,
			HeartbeatCheckInfo &heartbeatCheckInfo);

	void sendHeartbeat(EventContext &ec,
			HeartbeatInfo &heartbeatInfo,
			HeartbeatCheckInfo &heartbeatCheckInfo);

	void sendUpdatePartitionInfo(EventContext &ec,
			UpdatePartitionInfo &updatePartitionInfo,
			NodeIdList &activeNodeList);
};

/*!
	@brief Handles periodic NotifyCluster event
*/
class TimerNotifyClusterHandler : public ClusterHandler {

public:

	TimerNotifyClusterHandler() {}

	void operator()(EventContext &ec, Event &ev);

private:

	bool doProviderEvent(EventContext &ec, Event &ev);

	void doMulticastNotifyEvent(EventContext &ec, Event &ev);

	void doFixedListNotifyEvent(EventContext &ec, Event &ev);

	void doProviderNotifyEvent(EventContext &ec, Event &ev);

	bool checkResolvePublic(int32_t servicePos) {
		return (servicePos > SERVICE_MAX);
	}

	void resolveLocalAddress(
			const util::SocketAddress &socket,
			ServiceType serviceType,
			NodeId &nodeId);

	void resolvePublicAddress(
			const util::SocketAddress &socket,
			ServiceType serviceType,
			AddressInfo &publicAddressInfo);
};

/*!
	@brief Handles periodic NotifyClient event
*/
class TimerNotifyClientHandler : public ClusterHandler {

public:

	TimerNotifyClientHandler() {}

	void operator()(EventContext &ec, Event &ev);

private:

	void encode(Event &ev);
};

class TimerSQLNotifyClientHandler : public ClusterHandler {
public:
	TimerSQLNotifyClientHandler() {}
	void operator()(EventContext &ec, Event &ev);
};

class TimerRequestSQLCheckTimeoutHandler : public ClusterHandler {

public:

	TimerRequestSQLCheckTimeoutHandler() {}

	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles unknown cluster event
*/
class UnknownClusterEventHandler : public ClusterHandler {

public:

	UnknownClusterEventHandler() {}

	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles ChangePartitionState event
*/
class ChangePartitionStateHandler : public ClusterHandler {

public:

	ChangePartitionStateHandler() {}

	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles ChangePartitionTable event
*/
class ChangePartitionTableHandler : public ClusterHandler {

public:

	ChangePartitionTableHandler() {}

	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief ClusterService
*/
class ClusterService {

	class NotificationManager;
	friend class TimerNotifyClusterHandler;

public:

	ClusterService(
			const ConfigTable &config,
			EventEngine::Config &eeConfig,
			EventEngine::Source source,
			const char8_t *name,
			ClusterManager &clsMgr,
			ClusterVersionId versionId,
			util::VariableSizeAllocator<> &notifyAlloc
	);

	~ClusterService();

	void initialize(ResourceSet &resourceSet);

	void start(const Event::Source &eventSource);

	void shutdown();

	void waitForShutdown();

	void setSystemError(std::exception *e);

	void shutdownAllService(bool isInSytemService = false);

	bool isSystemServiceError() {
		return isSystemServiceError_;
	}

	bool isError();

	EventEngine *getEE(
			ServiceType type = CLUSTER_SERVICE);

	template <class T>
	void encode(Event &ev, T &t);

	template <class T>
	void encode(T &t, EventByteOutStream &out);

	template <class T>
	void decode(Event &ev, T &t);

	template <class T>
	void decode(Event &ev, T &t, EventByteInStream &in);

	template <class T>
	void request(
			const Event::Source &eventSource, EventType eventType,
			PartitionId pId, EventEngine *targetEE, T &t);

	void encodeNotifyClient(Event &ev);

	void requestRefreshPartition(
			EventContext &ec,
			util::XArray<PartitionId> *pIdList = NULL);

	void requestChangePartitionStatus(
			EventContext &ec,
			util::StackAllocator &alloc,
			PartitionId pId, PartitionStatus status);

	void requestChangePartitionStatus(
			EventContext &ec, util::StackAllocator &alloc,
			PartitionId pId, PartitionStatus status,
			ChangePartitionType changePartitionType);

	void requestCompleteCheckpoint(
			const Event::Source &eventSource,
			util::StackAllocator &alloc, bool isRecovery);

	ClusterManager *getManager() {
		return clsMgr_;
	}

	static NodeId resolveSenderND(Event &ev);

	static void changeAddress(
			NodeAddress &nodeAddress, NodeId nodeId, EventEngine *ee);

	static NodeId changeNodeId(
			NodeAddress &nodeAddress, EventEngine *ee);

	ClusterStats &getStats() {
		return clusterStats_;
	}
	PartitionTable *getPartitionTable() {
		return pt_;
	}

	template <class T1, class T2>
	void updateNodeList(
			T1 &AddressInfoList, T2 &publicAddressInfoList);
	NotificationManager &getNotificationManager();

	static bool isMulticastMode(
			const ConfigTable &config);

	void checkVersion(ClusterVersionId decodedVersion);

	void executeShutdown(bool isForce = false);

private:

	EventEngine::Config &createEEConfig(
			const ConfigTable &config,
			EventEngine::Config &eeConfig);

	void setClusterHandler(EventEngine::Source &source);

	HeartbeatHandler heartbeatHandler_;
	NotifyClusterHandler notifyClusterHandler_;
	UpdatePartitionHandler updatePartitionHandler_;
	SystemCommandHandler systemCommandHandler_;
	ClusterSystemCommandHandler clusterSystemCommandHandler_;

	TimerCheckClusterHandler timerCheckClusterHandler_;
	TimerNotifyClusterHandler timerNotifyClusterHandler_;
	TimerNotifyClientHandler timerNotifyClientHandler_;

	SQLTimerNotifyClientHandler
			timerSQLNotifyClientHandler_;

	TimerRequestSQLCheckTimeoutHandler
			timerRequestSQLCheckTimeoutHandler_;

	NewSQLPartitionRefreshHandler
			newSQLPartitionRefreshHandler_;

	UnknownClusterEventHandler unknownEventHandler_;
	ServiceThreadErrorHandler serviceThreadErrorHandler_;

	EventEngine ee_;
	ClusterVersionId versionId_;
	GlobalFixedSizeAllocator *fixedSizeAlloc_;
	GlobalVariableSizeAllocator *varSizeAlloc_;

	ClusterManager *clsMgr_;
	ClusterStats clusterStats_;
	PartitionTable *pt_;
	bool initailized_;
	bool isSystemServiceError_;
	ResourceSet *resourceSet_;

	/*!
		@brief NotificationManager
	*/
	class NotificationManager {
	public:
		static const int32_t DEFAULT_CHECK_INTERVAL = 1 * 1000;
		static const int32_t LONG_CHECK_INTERVAL = 3 * 1000;

		static const char *CONFIG_ADDRESS;
		static const char *CONFIG_PORT;
		static const char *CONFIG_URL;
		static const char *CONFIG_UPDATE_INTERVAL;

		/*!
			@brief Constructor of NotificationManager
		*/
		NotificationManager(ClusterManager *clsMgr,
				util::VariableSizeAllocator<> &valloc);

		/*!
			@brief Destructor of NotificationManager
		*/
		~NotificationManager();

		/*!
			@brief Initializer
		*/
		void initialize(const ConfigTable &config);

		/*!
			@brief Sets resolver updating interval
		*/
		int32_t updateResolverInterval() {
			return resolverUpdateInterval_;
		}

		int32_t checkResolverInterval() {
			return resolverCheckInterval_;
		}

		int32_t checkResolverLongInterval() {
			return resolverCheckLongInterval_;
		}

		/*!
			@brief Gets the cluster notification mode.
		*/
		ClusterNotificationMode getMode() {
			return mode_;
		}

		/*!
			@brief Sets digest value of a set of fixed address info.
		*/
		void setDigest(util::XArray<uint8_t> &digestBinary);

		/*!
			@brief Gets a set of fixed address info.
		*/
		NodeAddressSet &getFixedAddressInfo();
		
		AddressInfoList &getPublicFixedAddressInfo();

		/*!
			@brief Gets the number of fixed adderss info.
		*/
		int32_t getFixedNodeNum() {
			return fixedNodeNum_;
		}

		/*!
			@brief Requests the resolver the next update.
		*/
		bool next();

		int32_t check();

		/*!
			@brief Requests the resolver to start to update.
		*/
		ServiceAddressResolver *getResolver(int32_t pos = 0) {
			return resolverList_[pos];
		}

		void getNotificationMember(picojson::value &target);

	private:

		ClusterManager *clsMgr_;

		PartitionTable *pt_;

		util::VariableSizeAllocator<> &localVarAlloc_;

		ClusterNotificationMode mode_;

		std::vector<ServiceAddressResolver *> resolverList_;

		NodeAddressSet fixedAddressInfoSet_;
		AddressInfoList publicFixedAddressInfoSet_;

		int32_t fixedNodeNum_;

		int32_t resolverUpdateInterval_;

		int32_t resolverCheckInterval_;
		int32_t resolverCheckLongInterval_;
		util::Mutex lock_;
		picojson::value *value_;
	};

	NotificationManager notificationManager_;
};

#endif
