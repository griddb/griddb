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
	@brief Definition of ClusterService
*/

#ifndef CLUSTER_SERVICE_H_
#define CLUSTER_SERVICE_H_

#include "util/container.h"
#include "cluster_common.h"
#include "cluster_event_type.h"
#include "cluster_manager.h"
#include "event_engine.h"
#include "gs_error.h"
#include "system_service.h"


class ClusterService;
class ClusterManager;
class TransactionService;
class ClusterHandler;
class TriggerService;
class CheckpointService;
class SystemService;
class SyncService;
class SyncManager;
class DataStore;
class LogManager;
class TransactionManager;
class RecoveryManager;
class ObjectManager;

/*!
	@brief Represents the pointer of all manager objects
*/
struct ManagerSet {
	ManagerSet(ClusterService *clsSvc, SyncService *syncSvc,
		TransactionService *txnSvc, CheckpointService *cpSvc,
		SystemService *sysSvc, TriggerService *trgSvc,
		PartitionTable *pt, DataStore *ds, LogManager *logMgr,
		ClusterManager *clsMgr, SyncManager *syncMgr,
		TransactionManager *txnMgr, ChunkManager *chunkMgr,
		RecoveryManager *recoveryMgr, ObjectManager *objectMgr,
		GlobalFixedSizeAllocator *fixedSizeAlloc,
		GlobalVariableSizeAllocator *varSizeAlloc, ConfigTable *config,
		StatTable *stats)
		: clsSvc_(clsSvc),
		  syncSvc_(syncSvc),
		  txnSvc_(txnSvc),
		  cpSvc_(cpSvc),
		  sysSvc_(sysSvc),
		  trgSvc_(trgSvc),
		  pt_(pt),
		  ds_(ds),
		  logMgr_(logMgr),
		  clsMgr_(clsMgr),
		  syncMgr_(syncMgr),
		  txnMgr_(txnMgr),
		  chunkMgr_(chunkMgr),
		  recoveryMgr_(recoveryMgr),
		  objectMgr_(objectMgr),
		  fixedSizeAlloc_(fixedSizeAlloc),
		  varSizeAlloc_(varSizeAlloc),
		  config_(config),
		  stats_(stats) {
	}


	ClusterService *clsSvc_;
	SyncService *syncSvc_;
	TransactionService *txnSvc_;
	CheckpointService *cpSvc_;
	SystemService *sysSvc_;
	TriggerService *trgSvc_;
	PartitionTable *pt_;
	DataStore *ds_;
	LogManager *logMgr_;
	ClusterManager *clsMgr_;
	SyncManager *syncMgr_;
	TransactionManager *txnMgr_;
	ChunkManager *chunkMgr_;
	RecoveryManager *recoveryMgr_;
	ObjectManager *objectMgr_;
	GlobalFixedSizeAllocator *fixedSizeAlloc_;
	GlobalVariableSizeAllocator *varSizeAlloc_;

	ConfigTable *config_;
	StatTable *stats_;
};

/*!
	@brief Represents cluster statistics
*/
struct ClusterStats {
	ClusterStats(int32_t partitionNum)
		: partitionNum_(partitionNum), totalErrorCount_(0) {
		valueList_.assign(MAX_GET_TYPE, 0);
		eventErrorCount_.assign(EVENT_TYPE_MAX, 0);
		partitionErrorCount_.assign(partitionNum, 0);
		checkResourceList_.assign(partitionNum, 0);
	}

	/*!
		@brief cluster statistics type for setting
	*/
	enum SetType { CLUSTER_RECIEVE, CLUSTER_SEND, SYNC_RECIEVE, SYNC_SEND };

	static const int32_t TYPE_UNIT = 4;

	/*!
		@brief cluster statistics type for getting
	*/
	enum GetType {
		CLUSTER_RECIEVE_BYTE,
		CLUSTER_SEND_BYTE,
		SYNC_RECIEVE_BYTE,
		SYNC_SEND_BYTE,
		CLUSTER_RECIEVE_COUNT,
		CLUSTER_SEND_COUNT,
		SYNC_RECIEVE_COUNT,
		SYNC_SEND_COUNT,
		MAX_GET_TYPE
	};

	std::vector<int64_t> valueList_;
	std::vector<int32_t> eventErrorCount_;
	std::vector<int32_t> partitionErrorCount_;
	std::vector<uint8_t> checkResourceList_;

	uint32_t partitionNum_;
	uint32_t totalErrorCount_;

	void setTransactionTimeoutCheck(PartitionId pId, uint8_t flag) {
		checkResourceList_[pId] = flag;
	}

	uint32_t getNotTransactionTimeoutCheckCount(PartitionTable *pt) {
		uint32_t count = 0;
		for (PartitionId pId = 0; pId < partitionNum_; pId++) {
			if (!checkResourceList_[pId] && pt->isOwnerOrBackup(pId)) {
				count++;
			}
		}
		return count;
	}

	void setErrorStats(
		EventType eventType, PartitionId pId = UNDEF_PARTITIONID) {
		if (eventType >= 0 && eventType < EVENT_TYPE_MAX) {
			eventErrorCount_[eventType]++;
			totalErrorCount_++;
		}
		if (pId != UNDEF_PARTITIONID && pId < partitionNum_) {
			partitionErrorCount_[pId]++;
		}
	}

	int32_t getPartitionErrorCount(PartitionId pId) {
		return partitionErrorCount_[pId];
	}

	int32_t getEventErrorCount(EventType type) {
		return eventErrorCount_[type];
	}

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
			valueList_[CLUSTER_RECIEVE_BYTE] + valueList_[CLUSTER_SEND_BYTE]);
	}

	int64_t getSyncByteAll() {
		return (valueList_[SYNC_RECIEVE_BYTE] + valueList_[SYNC_SEND_BYTE]);
	}

	int64_t getByteAll() {
		return (getClusterByteAll() + getSyncByteAll());
	}

	int64_t getClusterCountAll() {
		return (valueList_[SYNC_RECIEVE_BYTE] + valueList_[SYNC_SEND_BYTE]);
	}

	int64_t getSyncCountAll() {
		return (valueList_[SYNC_RECIEVE_COUNT] + valueList_[SYNC_SEND_COUNT]);
	}

	int64_t getCountAll() {
		return (getClusterCountAll() + getSyncCountAll());
	}
};

static const uint32_t CS_HANDLER_PARTITION_ID = 0;

/*!
	@brief Handles generic event about cluster
*/
class ClusterHandler : public EventHandler {
public:

	ClusterHandler()
		: pt_(NULL),
		  clsMgr_(NULL),
		  clsEE_(NULL),
		  clsSvc_(NULL),
		  txnSvc_(NULL),
		  txnEE_(NULL),
		  cpSvc_(NULL),
		  cpEE_(NULL),
		  sysSvc_(NULL),
		  syncEE_(NULL),
		  syncMgr_(NULL),
		  chunkMgr_(NULL) {}

	~ClusterHandler() {}

	void initialize(const ManagerSet &mgrSet);

protected:

	void updateNodeList(
		EventType type, std::vector<AddressInfo> &AddressInfoList);

	NodeId checkAddress(EventType type, NodeAddress &address,
		NodeId clusterNodeId, ServiceType serviceType, EventEngine *ee);

	void checkAutoNdNumbering(const NodeDescriptor &nd);


	PartitionTable *pt_;
	ClusterManager *clsMgr_;
	EventEngine *clsEE_;
	ClusterService *clsSvc_;
	TransactionService *txnSvc_;
	EventEngine *txnEE_;
	CheckpointService *cpSvc_;
	EventEngine *cpEE_;
	SystemService *sysSvc_;
	EventEngine *sysEE_;
	SyncService *syncSvc_;
	EventEngine *syncEE_;
	SyncManager *syncMgr_;
	ChunkManager *chunkMgr_;
};

/*!
	@brief Handles heartbeat event
*/
class HeartbeatHandler : public ClusterHandler {
public:
	HeartbeatHandler() {}

	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles NotifyCluster event
*/
class NotifyClusterHandler : public ClusterHandler {
public:
	NotifyClusterHandler() {}

	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles UpdatePartition event
*/
class UpdatePartitionHandler : public ClusterHandler {
public:
	UpdatePartitionHandler() {}

	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles Gossip event
*/
class GossipHandler : public ClusterHandler {
public:
	GossipHandler() {}

	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles WebAPI command about Cluster
*/
class SystemCommandHandler : public ClusterHandler {
public:
	SystemCommandHandler() {}

	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles periodic CheckCluster event
*/
class TimerCheckClusterHandler : public ClusterHandler {
public:
	TimerCheckClusterHandler() {}

	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles periodic NotifyCluster event
*/
class TimerNotifyClusterHandler : public ClusterHandler {
public:
	TimerNotifyClusterHandler() {}

	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles periodic NotifyClient event
*/
class TimerNotifyClientHandler : public ClusterHandler {
public:
	TimerNotifyClientHandler() {}

	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles periodic CheckLoadBalance event
*/
class TimerCheckLoadBalanceHandler : public ClusterHandler {
public:
	TimerCheckLoadBalanceHandler() {}

	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handles DropPartition event from master node to follower nodes
*/
class OrderDropPartitionHandler : public ClusterHandler {
public:
	OrderDropPartitionHandler() {}

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
public:

	ClusterService(const ConfigTable &config, EventEngine::Config &eeConfig,
		EventEngine::Source source, const char8_t *name, ClusterManager &clsMgr,
		ClusterVersionId versionId,
		ServiceThreadErrorHandler &serviceThreadErrorHandler);

	~ClusterService();

	void initialize(ManagerSet &mgrSet);

	void start();

	void shutdown();

	void waitForShutdown();

	void setError(const Event::Source &eventSource, std::exception *e);

	void shutdownAllService(bool isInSytemService = false);

	bool isSystemServiceError() {
		return isSystemServiceError_;
	}

	bool isError() {
		return clsMgr_->isError();
	}

	EventEngine *getEE(ServiceType type = CLUSTER_SERVICE);

	template <class T>
	void encode(Event &ev, T &t);

	template <class T>
	void decode(Event &ev, T &t);

	template <class T>
	void request(const Event::Source &eventSource, EventType eventType,
		PartitionId pId, EventEngine *targetEE, T &t);

	void encodeNotifyClient(Event &ev);

	void requestGossip(const Event::Source &eventSource,
		util::StackAllocator &alloc, PartitionId pId = UNDEF_PARTITIONID,
		NodeId nodeId = 0, GossipType gossipType = GOSSIP_NORMAL);



	void requestChangePartitionStatus(EventContext &ec,
		util::StackAllocator &alloc, PartitionId pId, PartitionStatus status);

	void requestChangePartitionStatus(util::StackAllocator &alloc,
		PartitionId pId, PartitionStatus status,
		ChangePartitionType changePartitionType);

	void requestCompleteCheckpoint(const Event::Source &eventSource,
		util::StackAllocator &alloc, bool isRecovery);

	void requestUpdateStartLsn(const Event::Source &eventSource,
		util::StackAllocator &alloc, PartitionId startPId, PartitionId endPId);

	ClusterManager *getManager() {
		return clsMgr_;
	}

	std::string getClusterName() {
		return clsMgr_->getClusterName();
	}

	static NodeId resolveSenderND(Event &ev);

	static void getAddress(
		NodeAddress &nodeAddress, NodeId nodeId, EventEngine *ee);

	static NodeId getNodeId(NodeAddress &nodeAddress, EventEngine *ee);

	ClusterStats &getStats() {
		return clusterStats_;
	}


private:

	EventEngine::Config &createEEConfig(
		const ConfigTable &config, EventEngine::Config &eeConfig);

	void checkVersion(ClusterVersionId decodedVersion);



	HeartbeatHandler heartbeatHandler_;
	NotifyClusterHandler notifyClusterHandler_;
	UpdatePartitionHandler updatePartitionHandler_;
	GossipHandler gossipHandler_;
	SystemCommandHandler systemCommandHandler_;
	TimerCheckClusterHandler timerCheckClusterHandler_;
	TimerNotifyClusterHandler timerNotifyClusterHandler_;
	TimerNotifyClientHandler timerNotifyClientHandler_;
	TimerCheckLoadBalanceHandler timerCheckLoadBalanceHandler_;
	OrderDropPartitionHandler orderDropPartitionHandler_;

	UnknownClusterEventHandler unknownEventHandler_;
	ServiceThreadErrorHandler serviceThreadErrorHandler_;


	EventEngine ee_;
	ClusterVersionId versionId_;
	GlobalFixedSizeAllocator *fixedSizeAlloc_;
	GlobalVariableSizeAllocator *varSizeAlloc_;

	ClusterManager *clsMgr_;
	TransactionService *txnSvc_;
	SyncService *syncSvc_;
	SystemService *sysSvc_;
	CheckpointService *cpSvc_;
	ClusterStats clusterStats_;
	PartitionTable *pt_;
	bool initailized_;
	bool isSystemServiceError_;
};

#endif
