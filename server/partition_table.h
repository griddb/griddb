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
	@brief Definition of PartitionTable
*/

#ifndef PARTITION_TABLE_H_
#define PARTITION_TABLE_H_

#include "util/type.h"
#include "util/container.h"
#include "util/net.h"
#include "cluster_common.h"
#include "data_type.h"
#include "config_table.h"
class ClusterManager;

#include "msgpack_utils.h"
class ConfigTable;

typedef int32_t NodeId;

static const int32_t UNDEF_NODEID = -1;
static const int64_t UNDEF_TTL = 0;
static const int64_t NOT_UNDEF_TTL = 1;
static const NodeId SELF_NODEID = 0;

static const int32_t PT_ERROR_NORMAL = 0;
static const int32_t PT_OTHER_NORMAL = 0;
static const int32_t PT_ERROR_LONG_SYNC_FAIL = -2;
static const int32_t PT_ERROR_SHORT_SYNC_FAIL = -1;

static const int32_t PT_BACKUP_GAP_FACTOR = 2;
static const int32_t PT_OWNER_LOAD_LIMIT = 2;
static const int32_t PT_DATA_LOAD_LIMIT = 2;

class PartitionTable;
class SubPartitionTable;
class SubPartition;
struct AddressInfo;
typedef std::vector<AddressInfo> AddressInfoList;
typedef std::set<AddressInfo> NodeAddressSet;
typedef NodeAddressSet::iterator NodeAddressSetItr;


/*!
	@brief Result of Partition statistics check
*/
enum CheckedPartitionSummaryStatus {
	PT_NORMAL,
	PT_NOT_BALANCE,
	PT_REPLICA_LOSS,
	PT_OWNER_LOSS,
	PT_ABNORMAL,
	PT_INITIAL
};

/*!
	@brief Entry type of PartitionTable
*/
enum PartitionTableType {
	PT_CURRENT_OB,
	PT_CURRENT_GOAL,
	PT_TABLE_MAX = 2
};

/*!
	@brief Partition role
*/
enum PartitionRoleStatus {
	PT_OWNER,
	PT_BACKUP,
	PT_CATCHUP,
	PT_NONE,
	PT_ROLE_MAX = 5
};

namespace picojson {
	class value;
}

/*!
	@brief Encodes node address
*/
struct NodeAddress {

	NodeAddress() : address_(0), port_(0) {}

	NodeAddress(AddressType address, uint16_t port) :
			address_(address), port_(port) {}
	
	NodeAddress(const char8_t *addr, uint16_t port);
	
	void set(const char8_t *addr, uint16_t port);
	
	void set(const util::SocketAddress &addr);

	void clear() {
		address_ = 0;
		port_ = 0;
	}

	bool isValid() {
		return (address_ != 0);
	}

	bool operator==(const NodeAddress &nodeAddress) const {
		return (
			memcmp(
				&address_, &(nodeAddress.address_),
						sizeof(address_)) == 0 &&
			port_ == nodeAddress.port_);
	}

	bool operator < (const NodeAddress& nodeAddress) const {
		if (port_ < nodeAddress.port_) {
			return true;
		}
		else if (port_ > nodeAddress.port_) {
			return false;
		}
		return (memcmp(
				&address_, &(nodeAddress.address_),
						sizeof(address_)) < 0);
	}

	NodeAddress &operator=(const NodeAddress &nodeAddress) {
		address_ = nodeAddress.address_;
		port_ = nodeAddress.port_;
		return *this;
	}

	std::string dump(bool flag = true) {
		return toString(flag);
	}

	std::string toString(bool isPort = true) const;

	MSGPACK_DEFINE(address_, port_);

	AddressType address_;
	uint16_t port_;
};

/*!
	@brief Represents the addresses of all services
*/
struct AddressInfo {

	AddressInfo() : isActive_(false) {}

	
	AddressInfo(
			NodeAddress &clusterAddress,
			NodeAddress &txnAddress,
			NodeAddress &syncAddress,
			NodeAddress &systemAddress,
			NodeAddress &sqlAddress) : 
					clusterAddress_(clusterAddress),
					transactionAddress_(txnAddress),
					syncAddress_(syncAddress),
					systemAddress_(systemAddress),
					sqlAddress_(sqlAddress),
					isActive_(false) {}

	NodeAddress &getNodeAddress(ServiceType type);

	void setNodeAddress(
			ServiceType type, NodeAddress &address);

	bool operator<(const AddressInfo &right) const {
	
		if (clusterAddress_.address_
				< right.clusterAddress_.address_) {
			return true;
		}
		if (clusterAddress_.address_
					> right.clusterAddress_.address_) {
			return false;
		}
		
		return (clusterAddress_.port_
				< right.clusterAddress_.port_);
	}

	std::string dump();

	bool isValid() {
		return (transactionAddress_.isValid()
			&& sqlAddress_.isValid());
	}

	MSGPACK_DEFINE(
			clusterAddress_, transactionAddress_, syncAddress_,
			systemAddress_, sqlAddress_, dummy2_, isActive_);

	NodeAddress clusterAddress_;
	NodeAddress transactionAddress_;
	NodeAddress syncAddress_;
	NodeAddress systemAddress_;
	NodeAddress sqlAddress_;
	NodeAddress dummy2_;
	bool isActive_;
};

struct PublicAddressInfoMessage {
public:

	PublicAddressInfoMessage() {}

	PublicAddressInfoMessage(AddressInfoList &addressInfoList) :
			addressInfoList_(addressInfoList) {
	}

	bool check() {
		return true;
	}

	AddressInfoList &getPublicNodeAddressList() {
		return addressInfoList_;
	}

	MSGPACK_DEFINE(addressInfoList_);

private:
	
	AddressInfoList addressInfoList_;				
};

struct LoadSummary {
	
	LoadSummary(util::StackAllocator &alloc, int32_t nodeLimit);

	void init(int32_t nodeNum);

	void add(NodeId nodeId, bool isOwner = true);

	void setAlive(NodeId nodeId, uint8_t isAlive) {
		liveNodeList_[nodeId] = isAlive;
	}

	void update();

	bool isBalance(bool isOwner, int32_t checkCount);

	std::string dump();

	uint32_t minOwnerLoad_;
	uint32_t maxOwnerLoad_;
	uint32_t minDataLoad_;
	uint32_t maxDataLoad_;

	util::XArray<uint32_t> ownerLoadList_;
	util::XArray<uint32_t> dataLoadList_;
	util::XArray<uint8_t> liveNodeList_;
	
	uint32_t ownerLimit_;
	uint32_t dataLimit_;
	
	CheckedPartitionSummaryStatus status_;
	int32_t nodeNum_;
	int32_t ownerLoadCount_;
	int32_t dataLoadCount_;
};

struct DropNodeSet {

	DropNodeSet() :
			address_(0), port_(0), pos_(0), nodeId_(0) {}

	void setAddress(NodeAddress &address) {

		address_ = address.address_;
		port_ = address.port_;
	}

	void append(PartitionId pId, PartitionRevisionNo revision) {
		pIdList_.push_back(pId);
		revisionList_.push_back(revision);
	}

	void init() {
		pos_ = 0;
		NodeAddress tmp(address_, port_);
		nodeAddress_ = tmp;
		nodeId_ = 0;
	}

	NodeAddress &getNodeAddress() {
		return nodeAddress_;
	}
	
	std::string dump();

	MSGPACK_DEFINE(
			address_, port_, pIdList_, revisionList_);

	NodeAddress nodeAddress_;
	AddressType address_;
	uint16_t port_;
	int32_t pos_;
	NodeId nodeId_;
	PartitionIdList pIdList_;
	std::vector<PartitionRevisionNo> revisionList_;
};

class  DropPartitionNodeInfo {
public:

	DropPartitionNodeInfo(util::StackAllocator &alloc) :
			alloc_(alloc), tmpDropNodeMap_(alloc) {}

	void init();

	void set(PartitionId pId, NodeAddress &address,
			PartitionRevisionNo revision, NodeId nodeId);

	size_t getSize() {
		return dropNodeMap_.size();
	}

	DropNodeSet *get(size_t pos) {
		if (pos >= dropNodeMap_.size()) {
			return NULL;
		}
		return &dropNodeMap_[pos];
	}

	bool check() {
		return true;
	}
	std::string dump();

	MSGPACK_DEFINE(dropNodeMap_);

private:

	util::StackAllocator &alloc_;
	util::Map<NodeAddress, DropNodeSet> tmpDropNodeMap_;
	std::vector<DropNodeSet> dropNodeMap_;
};

/*!
	@brief Represents the revision of each address
*/
struct PartitionRevision {

	static const PartitionRevisionNo INITIAL_CLUSTER_REVISION = 1;

	PartitionRevision() :
			addr_(0), port_(0),
			sequentialNumber_(INITIAL_CLUSTER_REVISION) {}
	
	PartitionRevision(
			AddressType addr, uint16_t port,
			int32_t sequentialNumber) :
					addr_(addr), port_(port),
					sequentialNumber_(sequentialNumber) {

		addr_ = addr;
		port_ = port;
		sequentialNumber_ = sequentialNumber;
	}

	void set(AddressType addr, uint16_t port) {
		addr_ = addr;
		port_ = port;
	}

	~PartitionRevision() {}

	bool operator==(
			const PartitionRevision &rev) const {
	
		return (
				memcmp(&addr_, &(rev.addr_), sizeof(addr_)) == 0
				&& port_ == rev.port_
				&& sequentialNumber_ == rev.sequentialNumber_);
	}

	bool operator!=(const PartitionRevision &rev) const {

		int32_t addrDiff = memcmp(
				&addr_, &(rev.addr_), sizeof(addr_));
		int32_t portDiff = port_ - rev.port_;
		
		if (addrDiff == 0 && portDiff == 0) {
			return sequentialNumber_ != rev.sequentialNumber_;
		}
		else {
			return true;
		}
	}

	bool operator>(const PartitionRevision &rev) const {
		return sequentialNumber_ > rev.sequentialNumber_;
	}

	void updateRevision(
			PartitionRevisionNo sequentialNumber) {
		sequentialNumber_ = sequentialNumber;
	}

	bool isInitialRevision() const {
		return (sequentialNumber_
				== INITIAL_CLUSTER_REVISION);
	}

	std::string toString(bool isDetail = false) const;

	MSGPACK_DEFINE(addr_, port_, sequentialNumber_);

	AddressType addr_;
	uint16_t port_;
	PartitionRevisionNo sequentialNumber_;
};

/*!
	@brief Represents role of each partition
*/
class PartitionRole {

public:

	PartitionRole() {
		init(UNDEF_PARTITIONID);
	}

	PartitionRole(
			PartitionId pId,
			PartitionRevision revision, PartitionTableType type) : 
					pId_(pId),
					revision_(revision),
					type_(type),
					ownerNodeId_(UNDEF_NODEID),
					isSelfBackup_(false),
					isSelfCatchup_(false) {}

	void init(PartitionId pId, PartitionTableType type = PT_CURRENT_OB) {
		pId_ = pId;
		ownerNodeId_ = UNDEF_NODEID;
		type_ = type;
		isSelfBackup_ = false;
		isSelfCatchup_ = false;
	}

	PartitionRevision &getRevision() {
		return revision_;
	}

	void setRevision(PartitionRevision &revision) {
		revision_ = revision;
	}

	PartitionRevisionNo getRevisionNo() {
		return revision_.sequentialNumber_;
	}

	void getBackups(util::XArray<NodeId> &backups);

	void getCatchups(util::XArray<NodeId> &catchups);

	void clearBackup();

	void clearCatchup();

	NodeIdList &getBackups() {
		return backups_;
	}

	NodeIdList &getCatchups() {
		return catchups_;
	}

	int32_t getBackupSize() {
		return static_cast<int32_t>(backups_.size());
	}

	void clear();

	bool isOwnerOrBackup(NodeId nodeId = 0) {
		return (isOwner(nodeId) || isBackup(nodeId));
	}

	NodeId getOwner() {
		return ownerNodeId_;
	}

	void set(
			NodeId owner, NodeIdList &backups, NodeIdList &catchups);

	void setOwner(NodeId owner) {
		ownerNodeId_ = owner;
	}

	void set(PartitionTableType type) {
		type_ = type;
	}

	void set(PartitionRevision &revision) {
		revision_ = revision;
	}

	bool isOwner(NodeId targetNodeId = 0) {
		return (ownerNodeId_ == targetNodeId);
	}

	bool isBackup(NodeId targetNodeId = 0);

	bool isCatchup(NodeId targetNodeId = 0);

	void promoteCatchup();

	PartitionId getPartitionId() {
		return pId_;
	}

	PartitionTableType getPartitionTableType() {
		return type_;
	}

	bool operator==(PartitionRole &role) const;

	bool check() {
		return true;
	}

	void encodeAddress(PartitionTable *pt, PartitionRole &role);

	void get(
		NodeAddress &nodeAddress,
		std::vector<NodeAddress> &backupAddress) {
		nodeAddress = ownerAddress_;
		backupAddress = backupAddressList_;
	}

	void get(
		NodeAddress &nodeAddress,
		std::vector<NodeAddress> &backupAddress,
				std::vector<NodeAddress> &catchupAddress) {
		nodeAddress = ownerAddress_;
		backupAddress = backupAddressList_;
		catchupAddress = catchupAddressList_;
	}

	void getShortTermSyncTargetNodeId(
			util::Set<NodeId> &targetNodeSet);

	PartitionRoleStatus getPartitionRoleStatus();

	NodeAddress &getOwnerAddress() {
		return ownerAddress_;
	}

	std::vector<NodeAddress>  &getBackupAddress() {
		return backupAddressList_;
	}

	std::vector<NodeAddress>  &getCatchupAddress() {
		return catchupAddressList_;
	}

	bool hasCatchupRole() {
		return (catchups_.size() > 0);
	}

	std::string dump();
	std::string dumpIds();
	std::string dump(PartitionTable *pt,
			ServiceType type = CLUSTER_SERVICE);

	void checkBackup();

	void checkCatchup();

	MSGPACK_DEFINE(
			pId_, ownerAddress_,
			backupAddressList_, catchupAddressList_, revision_);

private:

	PartitionId pId_;
	PartitionRevision revision_;
	NodeAddress ownerAddress_;
	std::vector<NodeAddress> backupAddressList_;
	PartitionTableType type_;
	NodeId ownerNodeId_;
	NodeIdList backups_;
	bool isSelfBackup_;
	std::vector<NodeAddress> catchupAddressList_;
	NodeIdList catchups_;
	bool isSelfCatchup_;
};

/*!
	@brief Represents the table of partitions
*/
class PartitionTable {

	struct NodeInfo;
	struct PartitionInfo;
	class PartitionConfig;

public:
	static const int32_t MAX_NODE_NUM = 1000;

	static const int32_t PARTITION_RULE_UNDEF = -1;
	static const int32_t PARTITION_RULE_INITIAL = 1;
	static const int32_t PARTITION_RULE_ADD = 2;
	static const int32_t PARTITION_RULE_REMOVE = 3;
	static const int32_t PARTITION_RULE_SWAP = 4;

	/*!
		@brief Target queue type of change
	*/
	enum ChangePartitionTableType {
		PT_CHANGE_NEXT_TABLE,
		PT_CHANGE_GOAL_TABLE,
		PT_CHANGE_NEXT_GOAL_TABLE
	};

	/*!
		@brief Cluster status
	*/
	enum ClusterStatus { MASTER, SUB_MASTER, FOLLOWER };

	/*!
		@brief Partition status
	*/
	enum PartitionStatus {
		PT_STOP,
		PT_OFF,
		PT_SYNC,
		PT_ON,
		PT_STATUS_MAX = 4 
	};

	/*!
		@brief Encodes context of Partition
	*/
	struct PartitionContext {

		PartitionContext(
				util::StackAllocator &alloc,
				PartitionTable *pt,
				bool &needUpdatePartition, 
				SubPartitionTable &subPartitionTable,
				DropPartitionNodeInfo &dropNodeInfo);

		void setRuleLimitTime(int64_t limitTime) {
			currentRuleLimitTime_ = limitTime;
		}

		void setRepairPartition(bool isRepair) {
			isRepairPartition_ = isRepair;
		}

		void setConfigurationChange() {
			isConfigurationChange_ = true;
		}

		bool isConfigurationChange() {
			return isConfigurationChange_;
		}

		util::StackAllocator &getAllocator() {
			return *alloc_;
		}

		util::StackAllocator *alloc_;
		bool &needUpdatePartition_;
		int64_t currentRuleLimitTime_;
		PartitionTable *pt_;
		bool isRepairPartition_;
		SubPartitionTable &subPartitionTable_;
		DropPartitionNodeInfo &dropNodeInfo_;
		bool isGoalPartition_;
		bool isLoadBalance_;
		bool isAutoGoal_;

		int64_t currentTime_;
		bool isConfigurationChange_;
		int32_t backupNum_;
	};

	PartitionTable(const ConfigTable &configTable);

	~PartitionTable();

	struct GoalPartition {

		GoalPartition() : partitionNum_(1) {}

		void init(PartitionTable *pt);

		void set(PartitionTable *pt,
				util::Vector<PartitionRole> &goalList);

		void setPartitionRole(PartitionRole &role);

		PartitionRole &getPartitionRole(PartitionId pId);

		size_t size();

		void update(PartitionRole &role);

		void copy(util::Vector<PartitionRole> &roleList);

		std::vector<PartitionRole> goalPartitions_;
		uint32_t partitionNum_;
		util::Mutex lock_;
	};

	void copyGoal(util::Vector<PartitionRole> &roleList) {
		goal_.copy(roleList);
	}

	void setGoal(
			PartitionTable *pt, util::Vector<PartitionRole> &goalList) {
		goal_.set(pt, goalList);
	}

	void updateGoal(PartitionRole &role) {
		goal_.update(role);
	}

	NodeId getOwner(
			PartitionId pId, PartitionTableType type = PT_CURRENT_OB);

	bool isOwner(
			PartitionId pId, NodeId targetNodeDescriptor = 0,
			PartitionTableType type = PT_CURRENT_OB);

	bool isOwnerOrBackup(
			PartitionId pId, NodeId targetNodeDescriptor = 0,
			PartitionTableType type = PT_CURRENT_OB);

	void getBackup(
			PartitionId pId, util::XArray<NodeId> &backups,
			PartitionTableType type = PT_CURRENT_OB);

	void getCatchup(
			PartitionId pId, util::XArray<NodeId> &catchups,
			PartitionTableType type = PT_CURRENT_OB);

	NodeId getCatchup(PartitionId pId);

	bool isBackup(
			PartitionId pId, NodeId targetNodeId = 0,
			PartitionTableType type = PT_CURRENT_OB);

	bool isCatchup(
			PartitionId pId, NodeId targetNodeId = 0,
			PartitionTableType type = PT_CURRENT_OB);

	bool hasCatchupRole(
			PartitionId pId, PartitionTableType type = PT_CURRENT_OB);

	size_t getBackupSize(
			util::StackAllocator &alloc, PartitionId pId,
			PartitionTableType type = PT_CURRENT_OB);

	bool hasPublicAddress() {
		return hasPublicAddress_;
	}

	void setPublicAddress() {
		hasPublicAddress_ = true;
	}

	void getPublicNodeAddress(
			AddressInfoList &addressInfoList);

	void resetDownNode(NodeId targetNodeId);

	bool checkConfigurationChange(
			util::StackAllocator &alloc,
			PartitionContext &context, bool checkFlag);

	LogSequentialNumber getLSN(
			PartitionId pId, NodeId targetNodeId = 0);

	LogSequentialNumber getStartLSN(
			PartitionId pId, NodeId targetNodeId = 0);

	void resizeCurrenPartition(int32_t resizeNum);

	LogSequentialNumber getMaxLsn(PartitionId pId);

	void setMaxLsn(
			PartitionId pId, LogSequentialNumber lsn);

	void setRepairedMaxLsn(
			PartitionId pId, LogSequentialNumber lsn);

	bool setNodeInfo(
			NodeId nodeId, ServiceType type, NodeAddress &address);

	void setMaxLsnList(LsnList &maxLsnList) {
		maxLsnList = maxLsnList_;
	}

	void changeClusterStatus(
			ClusterStatus status,
			NodeId masterNodeId,
			int64_t nextHeartbeatTimeout);

	PartitionRevisionNo getCurrentRevisionNo() {
		return currentRevisionNo_;
	}

	void setGoalPartition(GoalPartition &goal);

	void planNext(PartitionContext &context);
	
	bool checkPeriodicDrop(
			util::StackAllocator &alloc, DropPartitionNodeInfo &dropInfo);
	
	void planGoal(PartitionContext &context);
	
	bool check();

	std::string dumpPartitionRule(int32_t ruleNo);

	int32_t getRuleNo() {
		return currentRuleNo_;
	}

	std::string dumpCurrentRule() {
		return dumpPartitionRule(currentRuleNo_);
	}

	int64_t getRuleLimitTime() {
		return nextApplyRuleLimitTime_;
	}

	bool isAvailable(PartitionId pId);

	void setLSN(
			PartitionId pId,
			LogSequentialNumber lsn,
			NodeId targetNodeId = 0);

	void setStartLSN(
			PartitionId pId,
			LogSequentialNumber lsn,
			NodeId targetNodeId = 0);

	void setLsnWithCheck(
			PartitionId pId,
			LogSequentialNumber lsn,
			NodeId targetNodeId = 0);

	PartitionStatus getPartitionStatus(
			PartitionId pId, NodeId targetNode = 0);

	PartitionRoleStatus getPartitionRoleStatus(
			PartitionId pId, NodeId targetNode = 0);

	void setPartitionStatus(
			PartitionId pId, PartitionStatus status, NodeId targetNode = 0);

	void setPartitionRoleStatus(
			PartitionId pId,
			PartitionRoleStatus status,
			NodeId targetNode = 0);

	void setPartitionRevision(
			PartitionId pId,
			PartitionRevisionNo partitionRevisuion,
			NodeId targetNode = 0);

	PartitionRevisionNo getPartitionRevision(
			PartitionId pId, NodeId targetNode);

	void getPartitionRole(
			PartitionId pId,
			PartitionRole &role,
			PartitionTableType type = PT_CURRENT_OB);

	void setPartitionRole(
			PartitionId pId,
			PartitionRole &role,
			PartitionTableType type = PT_CURRENT_OB);

	void setErrorStatus(
			PartitionId pId,
			int32_t status,
			NodeId targetNode = 0);

	int32_t  getErrorStatus(
			PartitionId pId, NodeId targetNode);

	void handleErrorStatus(
			PartitionId pId, NodeId targetNode, int32_t status);

	bool check(PartitionContext &context);

	void setBlockQueue(PartitionId pId,
		ChangePartitionTableType tableType = PT_CHANGE_NEXT_TABLE,
		NodeId targetNode = UNDEF_NODEID);

	void resetBlockQueue();

	CheckedPartitionSummaryStatus checkPartitionSummaryStatus(
			util::StackAllocator &alloc,
			bool isBalanceCheck = false);

	CheckedPartitionSummaryStatus getPartitionSummaryStatus() {
		return loadInfo_.status_;
	}

	void setPartitionSummaryStatus(
			CheckedPartitionSummaryStatus status) {
		loadInfo_.status_ = status;
	}

	bool checkNextOwner(
			PartitionId pId,
			NodeId checkedNode,
			LogSequentialNumber checkedLsn,
			LogSequentialNumber checkedMaxLsn,
			NodeId currentOwner = UNDEF_NODEID);

	bool checkNextBackup(
			LogSequentialNumber checkedLsn,
			LogSequentialNumber currentOwnerStartLsn,
			LogSequentialNumber currentOwnerLsn);

	template <class T>
	bool getLiveNodeIdList(
			T &liveNodeIdList, int64_t checkTime = UNDEF_TTL,
		bool isCheck = false) {

		bool isDownNode = false;
		
		for (NodeId nodeId = 0;
				nodeId < nodeNum_; nodeId++) {
			int64_t tmpTime = nodes_[nodeId].heartbeatTimeout_;
			if (nodes_[nodeId].isAlive(checkTime, isCheck)) {
				liveNodeIdList.push_back(nodeId);
			}
			else {
				if (tmpTime > 0) {
					isDownNode = true;
					downNodeList_.insert(nodeId);
				}
			}
		}
		return isDownNode;
	}

	bool checkLiveNode(
			NodeId nodeId, int64_t limitTime = UNDEF_TTL) {
		return nodes_[nodeId].isAlive(limitTime);
	}

	void setHeartbeatTimeout(NodeId nodeId,
			int64_t heartbeatTimeout = UNDEF_TTL) {
		nodes_[nodeId].heartbeatTimeout_ = heartbeatTimeout;
	}

	void setAckHeartbeat(NodeId nodeId, bool isAcked = true) {
		nodes_[nodeId].isAcked_ = isAcked;
	}

	bool getAckHeartbeat(NodeId nodeId) {
		return nodes_[nodeId].isAcked_;
	}

	int64_t getHeartbeatTimeout(NodeId nodeId) {
		return nodes_[nodeId].heartbeatTimeout_;
	}

	void clear(PartitionId pId, PartitionTableType type);

	void clearCurrentStatus();

	void clearRole(PartitionId pId);
	
	void set(SubPartition &subPartition);
	
	bool checkClearRole(PartitionId pId);

	void getNodeAddressInfo(
			AddressInfoList &addressInfoList,
			AddressInfoList &publicAddressInfoList);

	void clearCatchupRole(PartitionId pId);

	int32_t getNodeNum() {
		return nodeNum_;
	}

	uint32_t getPartitionNum() {
		return config_.partitionNum_;
	}

	uint32_t getReplicationNum() {
		return config_.replicationNum_;
	}

	PartitionGroupId getPartitionGroupId(PartitionId pId);

	void updatePartitionRevision(
			PartitionRevisionNo partitionSequentialNumber);

	NodeAddress &getNodeAddress(
			NodeId nodeId, ServiceType type = CLUSTER_SERVICE) {
		return getNodeInfo(nodeId).getNodeAddress(type);
	}

	NodeAddress &getPublicNodeAddress(
			NodeId nodeId, ServiceType type);

	bool checkSetService(
			NodeId nodeId, ServiceType type = CLUSTER_SERVICE) {
		return getNodeInfo(nodeId).check(type);
	}

	PartitionRevision &getPartitionRevision() {
		return revision_;
	}

	void setPublicAddressInfo(
			NodeId nodeId, AddressInfo &publicAddressInfo);

	PartitionRevision &incPartitionRevision();

	PartitionRevisionNo getPartitionRevisionNo() {
		return revision_.sequentialNumber_;
	}

	NodeId getMaster() {
		return masterNodeId_;
	}

	void setMaster(NodeId nodeId) {
		masterNodeId_ = nodeId;
	}

	bool isMaster() {
		return (masterNodeId_ == 0);
	}

	bool isSubMaster() {
		return (masterNodeId_ == UNDEF_NODEID);
	}

	bool isFollower() {
		return (masterNodeId_ > 0);
	}

	PartitionConfig &getConfig() {
		return config_;
	}

	void getDownNodes(
			util::Set<NodeId> &downNodes, bool isClear = true);

	PartitionId getCatchupPId() {
		return prevCatchupPId_;
	}

	NodeInfo &getNodeInfo(NodeId nodeId);

	void getAddNodes(util::Set<NodeId> &nodeIds);

	std::string dumpNodeAddress(
			NodeId nodeId, ServiceType type = CLUSTER_SERVICE);

	template <class T>
	std::string dumpNodeAddressList(
			T &nodeList, ServiceType type = CLUSTER_SERVICE) {

		util::NormalOStringStream ss;
		int32_t listSize = static_cast<int32_t>(nodeList.size());
		ss << "[";
		for (int32_t pos = 0; pos < listSize; pos++) {
			ss << dumpNodeAddress(nodeList[pos], type);
			if (pos != listSize - 1) {
				ss << ",";
			}
		}
		ss << "]";
		return ss.str().c_str();
	}

	std::string dumpNodeAddressInfoList(
			AddressInfoList &nodeList) {

		util::NormalOStringStream ss;
		int32_t listSize = static_cast<int32_t>(nodeList.size());
		ss << "[";
		for (int32_t pos = 0; pos < listSize; pos++) {
			ss << nodeList[pos].dump();
			if (pos != listSize - 1) {
				ss << ",";
			}
		}
		ss << "]";
		return ss.str().c_str();
	}

	std::string dumpNodes();

	std::string dumpPartitions(
			util::StackAllocator &alloc, PartitionTableType type,
			bool isSummary = false);
	
	std::string dumpPartitionsList(
			util::StackAllocator &alloc, PartitionTableType type);
	
	std::string dumpPartitionsNew(
			util::StackAllocator &alloc, PartitionTableType type,
			SubPartitionTable *subTable = NULL);

	static const std::string dumpPartitionRoleStatus(
			PartitionRoleStatus status);

	void genPartitionColumnStr(
			util::NormalOStringStream &ss,
			std::string &roleName,
			PartitionId pId,
			NodeId nodeId,
			int32_t nodeIdDigit,
			PartitionTableType type);

	std::string dumpPartitionStatusForRest(
			PartitionStatus status);

	std::string dumpPartitionStatusEx(
			PartitionStatus status);

	std::string dumpCurrentClusterStatus();

	std::string dumpDatas(
			bool isDetail = false, bool lsnOnly = false);

	void getServiceAddress(
		NodeId nodeId, picojson::value &result,
		ServiceType addressType);


	void updateNewSQLPartiton(
			std::vector<PartitionRole> &newsqlPartitionList);

	void updateNewSQLPartiton(
			SubPartitionTable &newsqlPartition);

	NodeId getNewSQLOwner(PartitionId pId);

	PartitionRevisionNo getNewSQLPartitionRevision(
			PartitionId pId);

	std::string dumpNewSQLPartition();

	util::Mutex newsqlLock_;
	std::vector<PartitionRole> newsqlPartitionList_;

template <typename T1, typename T2>
void getActiveNodeList(T1 &activeNodeList, 
		T2 &downNodeSet, int64_t currentTime);

private:
	
	struct PartitionInfo {

		PartitionInfo() : 
				pgId_(0),
				partitionRevision_(
						PartitionRevision::INITIAL_CLUSTER_REVISION),
			available_(true) {}

		void clear() {
			partitionRevision_
					= PartitionRevision::INITIAL_CLUSTER_REVISION;
		}

		PartitionGroupId pgId_;
		PartitionRevisionNo partitionRevision_;
		bool available_;
	};

	struct NodeInfo {
		
		struct NodeBaseInfo {
			NodeBaseInfo() : isSet_(false) {}
		
			bool set(NodeAddress &address) {
				if (!isSet_ && address.isValid()) {
					address_ = address;
					isSet_ = true;
					return true;
				}
				return false;
			}

			bool check() {
				return isSet_;
			}

			NodeAddress address_;
			bool isSet_;
		};

		NodeInfo() :
				nodeBaseInfo_(NULL),
				heartbeatTimeout_(UNDEF_TTL),
				isAcked_(false),
				globalStackAlloc_(NULL) {}

		~NodeInfo();

		void init(util::StackAllocator &alloc) {
			globalStackAlloc_ = &alloc;
			nodeBaseInfo_ = ALLOC_NEW(alloc)
					NodeBaseInfo [SERVICE_MAX];
		}

		NodeAddress &getNodeAddress(
				ServiceType type = CLUSTER_SERVICE) {
			return nodeBaseInfo_[type].address_;
		}

		bool check(ServiceType type) {
			return nodeBaseInfo_[type].check();
		}

		bool isAlive(
				int64_t limitTime = UNDEF_TTL, bool isCheck = true);

		std::string &dumpNodeAddress(ServiceType serviceType);
		
		std::string dump();

		NodeBaseInfo *nodeBaseInfo_;
		int64_t heartbeatTimeout_;
		bool isAcked_;
		util::StackAllocator *globalStackAlloc_;
		AddressInfo publicAddressInfo_;
	};

	class BlockQueue {

	public:
		
		BlockQueue(
			int32_t partitionNum,
			int32_t queueSizeLimit = INT32_MAX) :
					queueList_(NULL),
					partitionNum_(partitionNum),
					queueSize_(queueSizeLimit) {}

		~BlockQueue();

		void init(util::StackAllocator &alloc);

		bool find(PartitionId pId, NodeId nodeId);
		
		void push(PartitionId pId, NodeId nodeId);
		
		void clear(PartitionId pId);
		
		void clear();
		
		void erase(PartitionId pId, NodeId nodeId);
		
		int32_t size(PartitionId pId);

	private:

		void checkRange(PartitionId pId);

		std::set<NodeId> *queueList_;
		uint32_t partitionNum_;
		int32_t queueSize_;
		util::StackAllocator *globalStackAlloc_;
	};

	struct CurrentPartitionInfo {
		CurrentPartitionInfo() :
			lsn_(0),
			startLsn_(0),
			status_(PT_OFF),
			roleStatus_(PT_NONE), 
			partitionRevision_(1),
			chunkCount_(0),
			errorStatus_(PT_OTHER_NORMAL),
			otherStatus_(PT_OTHER_NORMAL) {}

		void clear() {
			startLsn_ = 0;
			status_ = PT_OFF;
			roleStatus_ = PT_NONE;
			partitionRevision_
					= PartitionRevision::INITIAL_CLUSTER_REVISION;
			chunkCount_ = 0;
			errorStatus_ = PT_OTHER_NORMAL;
		}

		LogSequentialNumber lsn_;
		LogSequentialNumber startLsn_;
		PartitionStatus status_;
		PartitionRoleStatus roleStatus_;
		PartitionRevisionNo partitionRevision_;
		int64_t chunkCount_;
		int32_t errorStatus_;
		int32_t otherStatus_;
	};

private:

	class CurrentPartitions {
	public:

		void init();

		void resize(int32_t nodeNum, bool withCheck);

		LogSequentialNumber getLsn(NodeId nodeId);

		void setLsn(
				NodeId nodeId,
				LogSequentialNumber lsn);

		LogSequentialNumber getStartLsn(NodeId nodeId);

		void setStartLsn(
				NodeId nodeId, LogSequentialNumber startLsn);

		void setErrorStatus(
				NodeId nodeId, int32_t status);

		int32_t getErrorStatus(NodeId nodeId);

		PartitionStatus getPartitionStatus(NodeId nodeId);

		void setPartitionStatus(
				NodeId nodeId, PartitionStatus status);

		PartitionRoleStatus getPartitionRoleStatus(NodeId nodeId);

		void setPartitionRoleStatus(
				NodeId nodeId, PartitionRoleStatus roleStatus);

		PartitionRevisionNo getPartitionRevision(NodeId nodeId);

		void setPartitionRevision(
				NodeId nodeId, PartitionRevisionNo partitionRevision);

		void resetPartitionInfo(NodeId nodeId);

	private:

		void checkSize(NodeId nodeId);

		std::vector<CurrentPartitionInfo> currentPartitionInfos_;
	};

	class Partition {

	public:

		Partition() : roles_(NULL), globalStackAlloc_(NULL) {}

		~Partition();

		void init(util::StackAllocator &alloc, PartitionId pId,
				PartitionGroupId pgId);

		void clear(PartitionTableType type = PT_TABLE_MAX);

		void check(NodeId nodeId);

		PartitionRole *roles_;
		PartitionInfo partitionInfo_;

	private:
		
		util::StackAllocator *globalStackAlloc_;
	};

	struct ReplicaInfo {

		ReplicaInfo(PartitionId pId, uint32_t count) :
				pId_(pId), count_(count) {}

		PartitionId pId_;
		uint32_t count_;
	};

	struct DataNodeInfo {
		DataNodeInfo(NodeId nodeId, uint32_t count, uint32_t) :
				nodeId_(nodeId), count_(count) {}

		NodeId nodeId_;
		uint32_t count_;
	};

	class PartitionConfig {

	public:

		PartitionConfig(
				const ConfigTable &config, int32_t maxNodeNum);

		int32_t getLimitClusterNodeNum() {
			return limitClusterNodeNum_;
		}

		bool setLimitOwnerBackupLsnGap(int32_t gap);

		int32_t getLimitOwnerBackupLsnGap() {
			return limitOwnerBackupLsnGap_;
		}

		bool setLimitOwnerBackupLsnDetectErrorGap(int32_t gap);

		int32_t getLimitOwnerBackupLsnDetectErrorGap() {
			return limitOwnerBackupLsnDetectErrorGap_;
		}

		bool setLimitOwnerCatchupLsnGap(int32_t gap);

		int32_t getLimitOwnerCatchupLsnGap() {
			return limitOwnerCatchupLsnGap_;
		}

		bool setOwnerLoadLimit(int32_t limitCount);

		int32_t getOwnerLoadLimit() {
			return ownerLoadLimit_;
		}

		bool setDataLoadLimit(int32_t limitCount);

		int32_t getDataLoadLimit() {
			return dataLoadLimit_;
		}

		uint32_t partitionNum_;
		int32_t replicationNum_;
		int32_t limitOwnerBackupLsnGap_;
		int32_t limitOwnerBackupLsnDetectErrorGap_;
		int32_t limitOwnerCatchupLsnGap_;
		int32_t limitClusterNodeNum_;
		int32_t limitMaxLsnGap_;
		uint32_t ownerLoadLimit_;
		uint32_t dataLoadLimit_;
		int32_t maxNodeNum_;
	};

	static bool cmpReplica(
			const ReplicaInfo &a, const ReplicaInfo &b) {
		return (a.count_ < b.count_);
	};

	bool isBalance(
			uint32_t minLoad, uint32_t maxLoad, bool isOwnerCheck);

	bool checkLimitOwnerBackupLsnGap(
			LogSequentialNumber ownerLSN,
		LogSequentialNumber backupLSN, int32_t gapFactor = 1);

	void setAvailable(PartitionId pId, bool available);

	bool checkTargetPartition(
			PartitionId pId, int32_t backupNum, bool &catchupWait);

	void clearPartitionRole(PartitionId pId);

	bool applyInitialRule(PartitionContext &context);
	bool applyAddRule(PartitionContext &context);
	bool applyRemoveRule(PartitionContext &context);
	bool applySwapRule(PartitionContext &context);

	void checkRepairPartition(PartitionContext &context);

	struct ScoreNode {
		ScoreNode(
				double score, NodeId nodeId, int32_t count, int32_t ownerCount) : 
				score_(score), nodeId_(nodeId),
				count_(count), ownerCount_(ownerCount) {}

		double score_;
		NodeId nodeId_;
		int32_t count_;
		int32_t ownerCount_;
	};

	struct ScoreInfo {
		ScoreInfo(double score, PartitionId pId) :
			score_(score), pId_(pId), assigned_(false),
			nodeId_(0), randVal_(0) {}

		double score_;
		PartitionId pId_;
		bool assigned_;
		NodeId nodeId_;
		int32_t randVal_;
	};

	static bool cmpScoreNode(
		const ScoreInfo &a, const ScoreInfo &b) {
		return (a.score_ > b.score_);
	};

	static bool cmpScore(
	const ScoreNode &a, const ScoreNode &b) {
		if (a.score_ > b.score_) {
			return true;
		}
		else if (a.score_ < b.score_) {
			return false;
		}
		else {
			if (a.count_ < b.count_) {
				return true;
			}
			else if (a.count_ > b.count_) {
				return false;
			}
			else {
				return (a.ownerCount_ < b.ownerCount_);
			}
		}
	};

	void assignGoalTable(
			util::StackAllocator &alloc,
			util::XArray<NodeId> &nodeIdList,
			util::Vector<PartitionRole> &roleList);

	void modifyGoalTable(
			util::StackAllocator &alloc,
			util::XArray<NodeId> &nodeIdList,
			util::Vector<PartitionRole> &roleList);

	void checkRange(PartitionId pId);
	
	void checkNode(NodeId nodeId);

	util::FixedSizeAllocator<util::Mutex> fixedSizeAlloc_;
	util::StackAllocator globalStackAlloc_;

	int32_t nodeNum_;
	NodeId masterNodeId_;
	PartitionRevision revision_;
	std::vector<LogSequentialNumber> maxLsnList_;

	util::Mutex revisionLock_;
	util::Mutex nodeInfoLock_;
	util::RWLock lock_;
	util::RWLock *partitionLockList_;

	std::set<NodeId> downNodeList_;
	std::set<NodeId> addNodeList_;

	GoalPartition goal_;
	int32_t currentRuleNo_;
	int64_t nextApplyRuleLimitTime_;

	PartitionConfig config_;
	NodeId prevMaxNodeId_;

	BlockQueue nextBlockQueue_;
	BlockQueue goalBlockQueue_;
	PartitionId prevCatchupPId_;
	NodeId prevCatchupNodeId_;

	Partition *partitions_;
	CurrentPartitions *currentPartitions_;

	NodeInfo *nodes_;

	LoadSummary loadInfo_;

	int32_t partitionDigit_;
	int32_t lsnDigit_;

	bool isRepaired_;
	PartitionRevisionNo currentRevisionNo_;
	PartitionRevisionNo prevDropRevisionNo_;
	bool isGoalPartition_;
	bool hasPublicAddress_;
};

typedef PartitionTable::PartitionStatus PartitionStatus;
typedef PartitionTable::PartitionContext PartitionContext;

/*!
	@brief Represents data cut out from Partition for communicatition between
   nodes
*/
class SubPartition {
public:

	SubPartition() {}
	
	SubPartition(PartitionId pId,
			PartitionRole &role, NodeAddress &currentOwner) :
					pId_(pId), role_(role),
					currentOwner_(currentOwner),
					currentOwnerId_(UNDEF_NODEID) {};

	std::string dump(PartitionTable *pt = NULL);

	PartitionId pId_;
	PartitionRole role_;
	NodeAddress currentOwner_;
	NodeId currentOwnerId_;

	MSGPACK_DEFINE(pId_, role_, currentOwner_);
};

std::ostream &operator<<(std::ostream &stream, SubPartition &subPartition);
std::ostream &operator<<(std::ostream &stream, PartitionRole &PartitionRole);
std::ostream &operator<<(std::ostream &stream, NodeAddress &nodeAddres);
std::ostream &operator<<(std::ostream &stream, PartitionRevision &revision);

/*!
	@brief SubPartition Table
*/
class EventEngine;
class SubPartitionTable {
public:

	SubPartitionTable() : partitionNum_(1) {}

	size_t size() {
		return subPartitionList_.size();
	}

	SubPartition &getSubPartition(size_t pos) {
		return subPartitionList_[pos];
	}

	void init(PartitionTable *pt, EventEngine *ee);

	void prepareMap(
			util::Map<PartitionId, PartitionRole*> &roleMap);

	void set(
			PartitionTable *pt,
			PartitionId pId,
			PartitionRole &role,
			NodeAddress &ownerAddress);

	void set(
			PartitionTable *pt,
			PartitionId pId,
			PartitionRole &role,
			NodeId ownerNodeId = UNDEF_NODEID);

	void setNodeAddress(AddressInfo &addressInfo) {
		nodeAddressList_.push_back(addressInfo);
	}

	AddressInfoList &getNodeAddressList() {
		return nodeAddressList_;
	}

	void setPublicNodeAddressList(
			AddressInfoList &publicNodeAddressList) {
		publicNodeAddressList_ = publicNodeAddressList;
	}

	AddressInfoList &getPublicNodeAddressList() {
		return publicNodeAddressList_;
	}

	void set(PartitionTable *pt);

	bool check() {
		return true;
	}

	PartitionRevision &getRevision() {
		return revision_;
	}

	void clear() {
		subPartitionList_.clear();
	}

	void setRevision(PartitionRevision &revison) {
		revision_ = revison;
	}

	MSGPACK_DEFINE(
			subPartitionList_, nodeAddressList_, revision_);

	std::string dump(bool isTrace = true);

	std::vector<SubPartition> subPartitionList_;
	AddressInfoList nodeAddressList_;
	PartitionRevision revision_;
	uint32_t partitionNum_;
	AddressInfoList publicNodeAddressList_;
};

const std::string dumpPartitionStatus(
		PartitionStatus status);

const std::string dumpPartitionRoleStatusEx(
		PartitionRoleStatus status);

#endif
