/*
	Copyright (c) 2014 TOSHIBA CORPORATION.

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

#include "event_engine.h"
class ClusterManager;

#include "msgpack.hpp"
class ConfigTable;

#define TEST_CLUSTER_MANAGER

#define TEST_PARTITION_TABLE

typedef int32_t NodeId;
const int32_t UNDEF_NODEID = -1;
const int64_t UNDEF_TTL = 0;

class SubPartitionTable;
class SubPartition;

namespace picojson {
class value;
}


static std::string getTimeStr(int64_t timeval) {
	util::NormalOStringStream oss;
	oss.clear();
	util::DateTime dtime(timeval);
	dtime.format(oss, true);
	return oss.str();
}


/*!
	@brief Encodes node address
*/
struct NodeAddress {

	NodeAddress() : address_(0), port_(0) {}

	NodeAddress(AddressType address, uint16_t port)
		: address_(address), port_(port) {}

	NodeAddress(const char8_t *addr, uint16_t port);

	void set(const char8_t *addr, uint16_t port);

	void set(const util::SocketAddress &addr);

	void clear() {
		address_ = 0;
		port_ = 0;
	}

	bool isValid() {
		return (address_ != 0 && port_ >= 0);
	}

	bool operator==(const NodeAddress &nodeAddress) const {
		return (
			memcmp(&address_, &(nodeAddress.address_), sizeof(address_)) == 0 &&
			port_ == nodeAddress.port_);
	}

	NodeAddress &operator=(const NodeAddress &nodeAddress) {
		address_ = nodeAddress.address_;
		port_ = nodeAddress.port_;
		return *this;
	}

	std::string dump() {
		return toString(true);
	}
	std::string toString(bool isPort = true) {
		util::NormalOStringStream ss;
		util::SocketAddress::Inet *tmp = (util::SocketAddress::Inet *)&address_;
		ss << (int32_t)tmp->value_[0] << "." << (int32_t)tmp->value_[1] << "."
		   << (int32_t)tmp->value_[2] << "." << (int32_t)tmp->value_[3];
		if (isPort) {
			ss << ":" << port_;
		}
		return ss.str();
	}

	MSGPACK_DEFINE(address_, port_);


	AddressType address_;
	uint16_t port_;
};

/*!
	@brief Represents the addresses of all services
*/
struct AddressInfo {

	AddressInfo() : isActive_(false) {}

	AddressInfo(NodeAddress &clusterAddress, NodeAddress &txnAddress,
		NodeAddress &syncAddress, NodeAddress &systemAddress)
		: clusterAddress_(clusterAddress),
#ifdef GD_ENABLE_UNICAST_NOTIFICATION
		  transactionAddress_(txnAddress),
#else
		  txnAddress_(txnAddress),
#endif
		  syncAddress_(syncAddress),
		  systemAddress_(systemAddress),
		  isActive_(false) {
	}

	NodeAddress &getNodeAddress(ServiceType type);

	void setNodeAddress(ServiceType type, NodeAddress &address);

#ifdef GD_ENABLE_UNICAST_NOTIFICATION
	bool operator<(const AddressInfo &right) const {
		if (clusterAddress_.address_ < right.clusterAddress_.address_) {
			return true;
		}
		if (clusterAddress_.address_ > right.clusterAddress_.address_) {
			return false;
		}
		return (clusterAddress_.port_ < right.clusterAddress_.port_);
	}
#endif

	std::string dump() {
		util::NormalOStringStream ss;
		ss << "NodeAddress:{"
		   << "cluster:{" << clusterAddress_.dump() << "}, "
		   << "sync:{" << syncAddress_.dump() << "}, "
#ifdef GD_ENABLE_UNICAST_NOTIFICATION
		   << "transaction:{" << transactionAddress_.dump() << "}, "
#else
		   << "transaction:{" << txnAddress_.dump() << "}, "
#endif
		   << "system:{" << systemAddress_.dump() << "}, "
		   << "isActive:{" << isActive_ << "}"
		   << "}";
		return ss.str();
	}


	NodeAddress clusterAddress_;
#ifdef GD_ENABLE_UNICAST_NOTIFICATION
	NodeAddress transactionAddress_;
#else
	NodeAddress txnAddress_;
#endif
	NodeAddress syncAddress_;
	NodeAddress systemAddress_;
	NodeAddress sqlServiceAddress_;
	NodeAddress dummy1_;
	NodeAddress dummy2_;
	bool isActive_;

#ifdef GD_ENABLE_UNICAST_NOTIFICATION
	MSGPACK_DEFINE(clusterAddress_, transactionAddress_, syncAddress_,
		systemAddress_, sqlServiceAddress_, dummy1_, dummy2_, isActive_);
#else
	MSGPACK_DEFINE(clusterAddress_, txnAddress_, syncAddress_, systemAddress_,
		sqlServiceAddress_, dummy1_, dummy2_, isActive_);

#endif
};

/*!
	@brief Represents the table of partitions
*/
class PartitionTable {
	TEST_PARTITION_TABLE
	TEST_CLUSTER_MANAGER

	struct NodeInfo;
	struct PartitionInfo;
	class PartitionConfig;

public:
	static const int32_t MAX_NODE_NUM = 1000;


	/*!
		@brief Target queue type of change
	*/
	enum ChangeTableType {
		PT_CHANGE_NEXT_TABLE,
		PT_CHANGE_GOAL_TABLE,
		PT_CHANGE_NEXT_GOAL_TABLE
	};

	/*!
		@brief Cluster status
	*/
	enum ClusterStatus { MASTER, SUB_MASTER, FOLLOWER };

	/*!
		@brief Entry type of PartitionTable
	*/
	enum TableType {
		PT_NEXT_OB,
		PT_NEXT_GOAL,
		PT_CURRENT_OB,
		PT_CURRENT_GOAL,
		PT_TMP_OB,
		PT_TMP_GOAL,
		PT_TABLE_MAX = 6
	};

	/*!
		@brief Partition status
	*/
	enum PartitionStatus { PT_STOP, PT_OFF, PT_SYNC, PT_ON, PT_STATUS_MAX = 4 };

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

	/*!
		@brief Result of Partition statistics check
	*/
	enum CheckedPartitionStat {
		PT_NORMAL,
		PT_REPLICA_LOSS,
		PT_NOT_BALANCE,
		PT_PARTIAL_STOP,
		PT_ABNORMAL
	};

	std::string dumpPartitionStat(CheckedPartitionStat stat) {
		std::string returnStr;
		switch (stat) {
		case PT_NORMAL:
			return "PT_NORMAL";
		case PT_REPLICA_LOSS:
			return "PT_REPLICA_LOSS";
		case PT_NOT_BALANCE:
			return "PT_NOT_BALANCE";
		case PT_PARTIAL_STOP:
			return "PT_PARTIAL_STOP";
		default:
			return "UNDEFINED";
		}
	}



	/*!
		@brief Represents the revision of each address
	*/
	struct PartitionRevision {

		PartitionRevision()
			: addr_(0), port_(0), sequentialNumber_(INITIAL_CLUSTER_REVISION) {}

		PartitionRevision(
			AddressType addr, uint16_t port, int32_t sequentialNumber)
			: addr_(addr), port_(port), sequentialNumber_(sequentialNumber) {
			addr_ = addr;
			port_ = port;
			sequentialNumber_ = sequentialNumber;
		}

		void set(AddressType addr, uint16_t port) {
			addr_ = addr;
			port_ = port;
		}

		~PartitionRevision() {}

		bool operator==(const PartitionRevision &rev) const {
			return (memcmp(&addr_, &(rev.addr_), sizeof(addr_)) == 0 &&
					port_ == rev.port_ &&
					sequentialNumber_ == rev.sequentialNumber_);
		}

		bool operator!=(const PartitionRevision &rev) const {
			int32_t addrDiff = memcmp(&addr_, &(rev.addr_), sizeof(addr_));
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

		void updateRevision(int32_t sequentialNumber) {
			sequentialNumber_ = sequentialNumber;
		}

		static const int32_t INITIAL_CLUSTER_REVISION = 1;
		bool isInitialRevision() const {
			return (sequentialNumber_ == INITIAL_CLUSTER_REVISION);
		}

		std::string toString(bool isDetail = false) const {
			util::NormalOStringStream ss;
			if (isDetail) {
				ss << addr_ << ":" << port_ << ":" << sequentialNumber_;
			}
			else {
				ss << sequentialNumber_;
			}
			return ss.str();
		}

		MSGPACK_DEFINE(addr_, port_, sequentialNumber_);


		AddressType addr_;
		uint16_t port_;
		int32_t sequentialNumber_;
	};

	/*!
		@brief Represents role of each partition
	*/
	class PartitionRole {
	public:

		PartitionRole() {
			init(UNDEF_PARTITIONID);
		}

		PartitionRole(PartitionId pId, TableType type)
			: pId_(pId),
			  type_(type),
			  messageType_(type),
			  ownerNodeId_(UNDEF_NODEID),
			  isSelfBackup_(false) {}

		PartitionRole(PartitionId pId, PartitionRevision revision, NodeId owner,
			std::vector<NodeId> &backups, TableType type)
			: pId_(pId),
			  revision_(revision),
			  type_(type),
			  messageType_(static_cast<int8_t>(type)),
			  ownerNodeId_(owner),
			  backups_(backups),
			  isSelfBackup_(false) {}

		void init(PartitionId pId, TableType type = PT_NEXT_OB) {
			pId_ = pId;
			ownerNodeId_ = UNDEF_NODEID;
			type_ = type;
			messageType_ = static_cast<int8_t>(type_);
			isSelfBackup_ = false;
		}

		PartitionRevision &getRevision() {
			return revision_;
		}

		int32_t getRevisionNo() {
			return revision_.sequentialNumber_;
		}

		void getBackups(util::XArray<NodeId> &backups) {
			size_t size = backups_.size() * sizeof(NodeId);
			if (size == 0) return;
			backups.resize(size / sizeof(NodeId));
			memcpy(backups.data(), reinterpret_cast<uint8_t *>(&backups_[0]),
				size);
		}

		std::vector<NodeId> &getBackups() {
			return backups_;
		}

		int32_t getBackupSize() {
			return static_cast<int32_t>(backups_.size());
		}

		void clear() {
			ownerNodeId_ = UNDEF_NODEID;
			backups_.clear();
			isSelfBackup_ = false;
		}

		void clearAddress() {
			backupAddressList_.clear();
			isSelfBackup_ = false;
		}

		bool isOwnerOrBackup() {
			return (isOwner() || isBackup());
		}

		NodeId getOwner() {
			return ownerNodeId_;
		}

		void checkBackup() {
			if (backups_.size() > 0) {
				std::sort(backups_.begin(), backups_.end());
				if (backups_[0] == 0) {
					isSelfBackup_ = true;
				}
				else {
					isSelfBackup_ = false;
				}
			}
			else {
				isSelfBackup_ = false;
			}
		}

		void set(PartitionRevision &revision, NodeId owner,
			std::vector<NodeId> &backups) {
			revision_ = revision;
			ownerNodeId_ = owner;
			backups_ = backups;
			checkBackup();
		}

		void set(PartitionRevision &revision, NodeId owner) {
			revision_ = revision;
			ownerNodeId_ = owner;
			checkBackup();
		}

		void set(NodeId owner, std::vector<NodeId> &backups) {
			ownerNodeId_ = owner;
			backups_ = backups;
			checkBackup();
		}

		void setOwner(NodeId owner) {
			ownerNodeId_ = owner;
		}

		void set(TableType type) {
			type_ = type;
			messageType_ = static_cast<uint8_t>(type);
		}

		void restoreType() {
			type_ = static_cast<TableType>(messageType_);
		}

		void set(PartitionRevision &revision) {
			revision_ = revision;
		}

		void remove(std::vector<NodeId> &removeNodeList) {
			std::vector<NodeId> result;
			for (size_t pos = 0; pos < removeNodeList.size(); pos++) {
				if (removeNodeList[pos] == ownerNodeId_) {
					ownerNodeId_ = UNDEF_NODEID;
					backups_.clear();
					isSelfBackup_ = false;
				}
			}

			std::set_difference(backups_.begin(), backups_.end(),
				removeNodeList.begin(), removeNodeList.end(),
				std::inserter(result, result.end()));
			backups_ = result;
			if (backups_.size() > 0) {
				std::sort(backups_.begin(), backups_.end());
				if (backups_[0] == 0) {
					isSelfBackup_ = true;
				}
				else {
					isSelfBackup_ = false;
				}
			}
			else {
				isSelfBackup_ = false;
			}
		}

		void clearBackup() {
			backups_.clear();
			backupAddressList_.clear();
			isSelfBackup_ = false;
		}

		bool isOwner(NodeId targetNodeId = 0) {
			return (ownerNodeId_ == targetNodeId);
		}

		bool isBackup(NodeId targetNodeId = 0) {
			if (backups_.size() == 0) return false;
			if (targetNodeId == 0) {
				return isSelfBackup_;
			}
			std::vector<NodeId>::iterator it;
			it = std::find(backups_.begin(), backups_.end(), targetNodeId);
			if (it != backups_.end()) {
				return true;
			}
			else {
				return false;
			}
		}

		PartitionId getPartitionId() {
			return pId_;
		}

		TableType getTableType() {
			return type_;
		}

		void getTargetNodeSet(util::Set<NodeId> &targetNodeSet) {
			if (ownerNodeId_ != UNDEF_NODEID) {
				targetNodeSet.insert(ownerNodeId_);
			}
			for (std::vector<NodeId>::iterator it = backups_.begin();
				 it != backups_.end(); it++) {
				targetNodeSet.insert(*it);
			}
		}

		bool operator==(PartitionRole &role) const;

		bool check() {
			return true;
		}

		void encodeAddress(PartitionTable *pt, PartitionRole &role);

		void get(
			NodeAddress &nodeAddress, std::vector<NodeAddress> &backupAddress) {
			nodeAddress = ownerAddress_;
			backupAddress = backupAddressList_;
		}

		void getSyncTargetNodeId(util::Set<NodeId> &targetNodeSet) {
			if (ownerNodeId_ != UNDEF_NODEID && ownerNodeId_ > 0) {
				targetNodeSet.insert(ownerNodeId_);
			}
			for (size_t pos = 0; pos < backups_.size(); pos++) {
				if (backups_[pos] != UNDEF_NODEID && backups_[pos] > 0) {
					targetNodeSet.insert(backups_[pos]);
				}
			}
		}

		PartitionRoleStatus getPartitionRoleStatus() {
			if (ownerNodeId_ == 0) {
				return PT_OWNER;
			}
			else if (isBackup()) {
				return PT_BACKUP;
			}
			else {
				return PT_NONE;
			}
		}
		NodeAddress &getOwnerAddress() {
			return ownerAddress_;
		}

		std::string dump();
		std::string dumpIds();

		std::string dump(PartitionTable *pt, ServiceType type);

		MSGPACK_DEFINE(
			pId_, ownerAddress_, backupAddressList_, revision_, messageType_);

	private:

		PartitionId pId_;
		PartitionRevision revision_;
		NodeAddress ownerAddress_;
		std::vector<NodeAddress> backupAddressList_;
		TableType type_;
		int8_t messageType_;
		NodeId ownerNodeId_;
		std::vector<NodeId> backups_;
		bool isSelfBackup_;
	};

	/*!
		@brief Encodes context of Partition
	*/
	struct PartitionContext {

		PartitionContext(util::StackAllocator &alloc, PartitionTable *pt,
			bool isSync = false)
			: alloc_(&alloc),
			  changedShortTermPIdList_(alloc),
			  changedLongTermPIdList_(alloc),
			  shortTermPIdList_(alloc),
			  changedNodeSet_(alloc),
			  isSync_(isSync),
			  isDownNode_(false),
			  shortTermTimeout_(UNDEF_TTL),
			  longTermTimeout_(UNDEF_TTL),
			  pt_(pt),
			  isRepairPartition_(false) {}

		void setChangePId(SyncMode mode, PartitionId pId) {
			if (mode == MODE_SHORTTERM_SYNC) {
				changedShortTermPIdList_.push_back(pId);
			}
			else {
				changedLongTermPIdList_.push_back(pId);
			}
		}

		util::XArray<PartitionId> &getShortTermPIdList() {
			return shortTermPIdList_;
		}

		void setSyncLimitTime(
			int64_t shortTermTimeout, int64_t longTermTimeout) {
			shortTermTimeout_ = shortTermTimeout;
			longTermTimeout_ = longTermTimeout;
		}

		void setRepairPartition(bool isRepair) {
			isRepairPartition_ = isRepair;
		}

		std::string dump() {
			util::NormalOStringStream ss;
			ss << "PartitionContext:{"
			   << "changedShortTermPIdList:" << changedShortTermPIdList_
			   << ", changedLongTermPIdList:" << changedLongTermPIdList_
			   << ", shortTermPIdList:" << shortTermPIdList_
			   << ", changedNodeSet:"
			   << pt_->dumpNodeAddressSet(changedNodeSet_)
			   << ", isSync:" << isSync_ << ", isDownNode:" << isDownNode_
			   << ", shortTermTimeout:" << getTimeStr(shortTermTimeout_)
			   << ", longTermTimeout:" << getTimeStr(longTermTimeout_) << "}";
			return ss.str();
		}


		util::StackAllocator *alloc_;
		util::XArray<PartitionId> changedShortTermPIdList_;
		util::XArray<PartitionId> changedLongTermPIdList_;
		util::XArray<PartitionId> shortTermPIdList_;
		util::Set<NodeId> changedNodeSet_;
		bool isSync_;
		bool isDownNode_;
		int64_t shortTermTimeout_;
		int64_t longTermTimeout_;

		PartitionTable *pt_;
		bool isRepairPartition_;
	};



	PartitionTable(const ConfigTable &configTable);

	~PartitionTable();

	void initialize(ClusterManager *clsMgr) {
		clsMgr_ = clsMgr;
	}

	EventMonotonicTime getMonotonicTime();

	NodeId getOwner(PartitionId pId, TableType type = PT_CURRENT_OB);

	bool isOwner(PartitionId pId, NodeId targetNodeDescriptor = 0,
		TableType type = PT_CURRENT_OB);
	bool isOwnerOrBackup(PartitionId pId, NodeId targetNodeDescriptor = 0,
		TableType type = PT_CURRENT_OB);

	void getBackup(PartitionId pId, util::XArray<NodeId> &backups,
		TableType type = PT_CURRENT_OB);

	void getBackup(PartitionId pId, std::vector<NodeId> &backups,
		TableType type = PT_CURRENT_OB);  

	bool isBackup(PartitionId pId, NodeId targetNodeId = 0,
		TableType type = PT_CURRENT_OB);

	LogSequentialNumber getLSN(PartitionId pId, NodeId targetNodeId = 0) {
		return partitions_[pId].getLsn(targetNodeId);
	}

	LogSequentialNumber getStartLSN(PartitionId pId, NodeId targetNodeId = 0) {
		return partitions_[pId].getStartLsn(targetNodeId);
	}

	LogSequentialNumber getMaxLsn(PartitionId pId) {
		return maxLsnList_[pId];
	}

	void setMaxLsn(PartitionId pId, LogSequentialNumber lsn) {
		if (lsn > maxLsnList_[pId]) {
			maxLsnList_[pId] = lsn;
		}
	}

	void setRepairedMaxLsn(PartitionId pId, LogSequentialNumber lsn) {
		maxLsnList_[pId] = lsn;
	}

	void setRepairLsn(PartitionId pId, LogSequentialNumber lsn) {
		repairLsnList_[pId] = lsn;
	}

	bool setGapCount(PartitionId pId) {
		gapCountList_[pId]++;
		if (gapCountList_[pId] > PartitionConfig::PT_LSN_NOTCHANGE_COUNT_MAX) {
			return true;
		}
		else {
			return false;
		}
	}

	void resetGapCount(PartitionId pId) {
		gapCountList_[pId] = 0;
	}

	int32_t getGapCount(PartitionId pId) {
		return gapCountList_[pId];
	}

	bool checkMaxLsn(PartitionId pId, LogSequentialNumber lsn) {
		if (repairLsnList_[pId] > maxLsnList_[pId]) {
			return (lsn <= maxLsnList_[pId]);
		}
		else {
			return (lsn <= repairLsnList_[pId]);
		}
	}
	void setActiveOwner(PartitionId pId, NodeId owner) {
		currentActiveOwnerList_[pId] = owner;
	}

	NodeId getActiveOwner(PartitionId pId) {
		return currentActiveOwnerList_[pId];
	}

	bool setNodeInfo(NodeId nodeId, ServiceType type, NodeAddress &address);

	std::vector<LogSequentialNumber> &getMaxLsnList() {
		return maxLsnList_;
	}

	void setLSN(
		PartitionId pId, LogSequentialNumber lsn, NodeId targetNodeId = 0) {
		partitions_[pId].setLsn(targetNodeId, lsn);
	}

	void setStartLSN(
		PartitionId pId, LogSequentialNumber lsn, NodeId targetNodeId = 0) {
		partitions_[pId].setStartLsn(targetNodeId, lsn);
	}

	void setLsnWithCheck(
		PartitionId pId, LogSequentialNumber lsn, NodeId targetNodeId = 0) {
		partitions_[pId].setLsnWithCheck(targetNodeId, lsn);
	}

	int32_t getLsnChangeCount(PartitionId pId, NodeId nodeId) {
		return partitions_[pId].getLsnChangeCount(nodeId);
	}

	PartitionStatus getPartitionStatus(PartitionId pId, NodeId targetNode = 0) {
		return partitions_[pId].getPartitionStatus(targetNode);
	}

	PartitionRoleStatus getPartitionRoleStatus(
		PartitionId pId, NodeId targetNode = 0) {
		return static_cast<PartitionRoleStatus>(
			partitions_[pId].getPartitionRoleStatus(targetNode));
	}

	void setPartitionStatus(
		PartitionId pId, PartitionStatus status, NodeId targetNode = 0) {
		return partitions_[pId].setPartitionStatus(targetNode, status);
	};

	void setPartitionRoleStatus(
		PartitionId pId, PartitionRoleStatus status, NodeId targetNode = 0) {
		return partitions_[pId].setPartitionRoleStatus(targetNode, status);
	};

	void checkAndSetPartitionStatus(PartitionId pId, NodeId targetNode,
		PartitionStatus status, PartitionRoleStatus roleStatus);

	void updatePartitionRole(PartitionId pId, TableType type = PT_CURRENT_OB);

	void updatePartitionRole(PartitionId pId,
		std::vector<NodeId> &removeNodeIdList, TableType type = PT_CURRENT_OB) {
		util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
		partitions_[pId].roles_[type].remove(removeNodeIdList);
	}

	void getPartitionRole(
		PartitionId pId, PartitionRole &role, TableType type = PT_CURRENT_OB) {
		util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
		role = partitions_[pId].roles_[type];
	}

	void setPartitionRole(
		PartitionId pId, PartitionRole &role, TableType type = PT_CURRENT_OB) {
		util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
		partitions_[pId].roles_[type] = role;
		partitions_[pId].roles_[type].set(type);
		if (type == PT_CURRENT_OB) {
			PartitionRoleStatus nextStatus =
				partitions_[pId].roles_[type].getPartitionRoleStatus();
			partitions_[pId].setPartitionRoleStatus(0, nextStatus);
		}
	}

	bool isRepairedPartition() {
		bool retVal = isRepaired_;
		isRepaired_ = false;
		return retVal;
	}

	void setRepairedPartition(bool flag = true) {
		isRepaired_ = flag;
	}

	void checkRelation(PartitionContext &context, PartitionId pId,
		int32_t longtermTimeout, int32_t slackTime, int32_t heartbeatSlack);

	void setGossip(PartitionId pId = UNDEF_PARTITIONID);

	void resetBlockQueue() {
		nextBlockQueue_.clear();
		goalBlockQueue_.clear();
	};

	void setBlockQueue(PartitionId pId,
		ChangeTableType tableType = PT_CHANGE_NEXT_TABLE,
		NodeId targetNode = UNDEF_NODEID);

	void filter(PartitionContext &context, util::Set<NodeId> &changedNodeSet,
		SubPartitionTable &subTable, bool isShortSync = true);

	CheckedPartitionStat checkPartitionStat(bool firstCheckSkip = true);

	CheckedPartitionStat getPartitionStat() {
		return loadInfo_.status_;
	}

	bool checkDrop(PartitionId pId);

	void execDrop(PartitionId pId) {
		partitions_[pId].partitionInfo_.isDrop_ = true;
	}

	bool isDrop(PartitionId pId) {
		bool retVal = partitions_[pId].partitionInfo_.isDrop_;
		partitions_[pId].partitionInfo_.isDrop_ = false;
		return retVal;
	}

	int64_t getLongTermLimitTime(PartitionId pId) {
		return partitions_[pId].partitionInfo_.longTermTimeout_;
	}

	template <class T>
	bool getLiveNodeIdList(T &liveNodeIdList, int64_t checkTime = UNDEF_TTL,
		bool isCheck = false) {
		bool isDownNode = false;
		for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
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

	int32_t getLiveNum(int64_t checkTime = UNDEF_TTL) {
		int32_t count = 0;
		for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
			if (nodes_[nodeId].isAlive(checkTime)) {
				count++;
			}
		}
		return count;
	}

	bool checkLiveNode(NodeId nodeId, int64_t limitTime = UNDEF_TTL) {
		return nodes_[nodeId].isAlive(limitTime);
	}

	void setHeartbeatTimeout(NodeId nodeId, int64_t heartbeatTimeout) {
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

	bool isActive(PartitionId pId) {
		return partitions_[pId].partitionInfo_.isActive_;
	}

	void setActive(PartitionId pId, bool isActive) {
		partitions_[pId].partitionInfo_.isActive_ = isActive;
	}

	void set(SubPartition &subPartition);

	void clear(PartitionId pId, TableType type) {
		util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
		partitions_[pId].clear(type);
	}

	void clearAll(bool isLeave = false);

	void clearRole(PartitionId pId);

	int32_t getNodeNum() {
		return nodeNum_;
	}

	uint32_t getPartitionNum() {
		return config_.partitionNum_;
	}

	uint32_t getReplicationNum() {
		return config_.replicationNum_;
	}

	PartitionGroupId getPartitionGroupId(PartitionId pId) {
		return partitions_[pId].partitionInfo_.pgId_;
	}

	void createNextPartition(PartitionContext &context);

	void createCatchupPartition(PartitionContext &context, bool isSkip = false);

	void resizeMasterInfo(uint32_t newSize) {
		for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
			partitions_[pId].resizeMaster(newSize);
		}
	}

	void updatePartitionRevision(int32_t partitionSequentialNumber);

	void getSyncTargetPIdList(util::XArray<PartitionId> &pIdList) {
		util::LockGuard<util::WriteLock> lock(lock_);
		for (std::set<PartitionId>::iterator it = gossips_.begin();
			 it != gossips_.end(); it++) {
			pIdList.push_back(*it);
		}

		gossips_.clear();
	}

	void clearGossip() {
		gossips_.clear();
	}

	void setCatchupError() {
		failCatchup_ = true;
	}

	bool isCatchupError() {
		bool retVal = failCatchup_;
		failCatchup_ = false;
		return retVal;
	}

	NodeInfo &getNodeInfo(NodeId nodeId) {
		return nodes_[nodeId];
	}

	PartitionInfo *getPartitionInfo(PartitionId pId) {
		return &partitions_[pId].partitionInfo_;
	}

	PartitionRevision &getPartitionRevision() {
		return revision_;
	}

	PartitionRevision &incPartitionRevision() {
		util::LockGuard<util::Mutex> lock(revisionLock_);
		if (revision_.sequentialNumber_ == INT32_MAX) {
			revision_.sequentialNumber_ = 2;
		}
		else {
			revision_.sequentialNumber_++;
		}
		return revision_;
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

	void setDownNode(NodeId nodeId) {
		util::LockGuard<util::Mutex> lock(nodeInfoLock_);
		downNodeList_.insert(nodeId);
	}

	void getDownNodes(std::set<NodeId> &downNodes, bool isClear = true) {
		util::LockGuard<util::Mutex> lock(nodeInfoLock_);
		downNodes = downNodeList_;
		if (isClear) {
			downNodeList_.clear();
		}
	}

	PartitionId getCatchupPId() {
		return prevCatchupPId_;
	}

	void setCatchupPId(PartitionId prevPId, NodeId prevNodeId) {
		prevCatchupPId_ = prevPId;
		prevCatchupNodeId_ = prevNodeId;
	}

	void getAddNodes(std::set<NodeId> &nodeIds) {
		util::LockGuard<util::Mutex> lock(nodeInfoLock_);
		NodeId nodeId = getNodeNum();
		if (nodeId > prevMaxNodeId_) {
			for (int32_t pos = prevMaxNodeId_; pos < nodeId; pos++) {
				if (nodes_[pos].nodeBaseInfo_[CLUSTER_SERVICE + 1].check()) {
					nodeIds.insert(pos);
				}
			}
			prevMaxNodeId_ = nodeId;
		}
	}

	bool isSetted(NodeId nodeId) {
		if (nodeId >= nodeNum_) {
			return false;
		}
		return nodes_[nodeId].nodeBaseInfo_[CLUSTER_SERVICE + 1].check();
	}

	std::string dumpNodeAddress(
		NodeId nodeId, ServiceType type = CLUSTER_SERVICE) {
		if (nodeId >= nodeNum_) {
			return std::string("");
		}
		return nodes_[nodeId].nodeBaseInfo_[type].address_.toString();
	}

	void setFailoverTime();
	void resetFailoverTime() {
		prevFailoverTime_ = 0;
	}
	bool checkFailoverTime();

	std::string dumpNodeSet(std::set<NodeId> &nodeSet) {
		util::NormalOStringStream ss;
		int32_t listSize = static_cast<int32_t>(nodeSet.size());
		int32_t pos = 0;
		ss << "[";
		for (std::set<NodeId>::iterator it = nodeSet.begin();
			 it != nodeSet.end(); it++, pos++) {
			ss << dumpNodeAddress(*it);
			if (pos != listSize - 1) {
				ss << ",";
			}
		}
		ss << "]";
		return ss.str().c_str();
	}

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

	std::string dumpNodeAddressInfoList(std::vector<AddressInfo> &nodeList) {
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

	std::string dumpNodeAddressSet(
		util::Set<NodeId> &nodeSet, ServiceType type = CLUSTER_SERVICE) {
		util::NormalOStringStream ss;
		ss << "[";
		int32_t setSize = static_cast<int32_t>(nodeSet.size());
		int32_t pos = 0;
		for (util::Set<NodeId>::iterator it = nodeSet.begin();
			 it != nodeSet.end(); it++, pos++) {
			ss << dumpNodeAddress(*it, type);
			if (pos != setSize - 1) {
				ss << ",";
			}
		}
		ss << "]";
		return ss.str().c_str();
	}

	std::string dumpNodes();

	std::string dumpPartitions(util::StackAllocator &alloc, TableType type);

	std::string dumpPartitionStatusForRest(PartitionStatus status) {
		switch (status) {
		case PT_STOP:
			return "STOP";
		case PT_OFF:
			return "OFF";
		case PT_SYNC:
			return "SYNC";
		case PT_ON:
			return "ON";
		default:
			break;
		}
		return NULL;
	}

	std::string dumpPartitionStatusEx(PartitionStatus status) {
		switch (status) {
		case PT_STOP:
			return "STP";
		case PT_OFF:
			return "OFF";
		case PT_SYNC:
			return "SYC";
		case PT_ON:
			return "ON ";
		default:
			break;
		}
		return NULL;
	}

	std::string dumpCurrentClusterStatus() {
		if (masterNodeId_ == 0) {
			return "MASTER";
		}
		else if (masterNodeId_ == -1) {
			return "SUB_MASTER";
		}
		else {
			return "FOLLOWER";
		}
	}

	std::string dumpDatas(bool isDetail = false);

	void getServiceAddress(
		NodeId nodeId, picojson::value &result, ServiceType addressType);


private:

	struct PartitionInfo {

		PartitionInfo()
			: lsn_(0),
			  startLsn_(0),
			  status_(PT_STOP),
			  isActive_(false),
			  shortTermTimeout_(UNDEF_TTL),
			  longTermTimeout_(UNDEF_TTL),
			  pgId_(0),
			  isDrop_(false),
			  notChangeCount_(0) {}

		void clear() {
			shortTermTimeout_ = UNDEF_TTL;
			longTermTimeout_ = UNDEF_TTL;
			isActive_ = false;
			isDrop_ = false;
			notChangeCount_ = 0;
		}


		LogSequentialNumber lsn_;
		LogSequentialNumber startLsn_;

		PartitionStatus status_;
		bool isActive_;
		int64_t shortTermTimeout_;
		int64_t longTermTimeout_;
		PartitionGroupId pgId_;
		bool isDrop_;
		int8_t notChangeCount_;
		uint8_t roleStatus_;
	};

	struct NodeInfo {

		struct NodeBaseInfo {

			NodeBaseInfo() : isSetted_(false) {}

			bool set(NodeAddress &address) {
				if (!isSetted_ && address.isValid()) {
					address_ = address;
					isSetted_ = true;
					return true;
				}
				return false;
			}

			bool check() {
				return isSetted_;
			}


			NodeAddress address_;
			bool isSetted_;
		};


		NodeInfo()
			: nodeBaseInfo_(NULL),
			  heartbeatTimeout_(UNDEF_TTL),
			  isAcked_(false),
			  alloc_(NULL) {}

		~NodeInfo() {
			if (nodeBaseInfo_) {
				for (int32_t pos = 0; pos < SERVICE_MAX; pos++) {
					ALLOC_DELETE(*alloc_, &nodeBaseInfo_[pos]);
				}
			}
		}

		void init(util::StackAllocator &alloc) {
			alloc_ = &alloc;
			nodeBaseInfo_ = ALLOC_NEW(alloc) NodeBaseInfo[SERVICE_MAX];
		}

		NodeAddress &getNodeAddress(ServiceType type = CLUSTER_SERVICE) {
			return nodeBaseInfo_[type].address_;
		}


		bool check(ServiceType type) {
			return nodeBaseInfo_[type].check();
		}

		bool isAlive(int64_t limitTime = UNDEF_TTL, bool isCheck = true) {
			if (heartbeatTimeout_ > limitTime) {
				return true;
			}
			else {
				if (isCheck) {
					if (heartbeatTimeout_ != UNDEF_TTL) {
						heartbeatTimeout_ = UNDEF_TTL;
					}
				}
				return false;
			}
		}

		std::string &dumpNodeAddress(ServiceType serviceType);

		std::string dump() {
			util::NormalOStringStream ss;
			ss << "[";
			for (int32_t pos = 0; pos < SERVICE_MAX; pos++) {
				ss << nodeBaseInfo_[pos].address_.toString();
				if (pos != SERVICE_MAX - 1) {
					ss << ",";
				}
			}
			ss << "]";
			return ss.str().c_str();
		}


		NodeBaseInfo *nodeBaseInfo_;
		int64_t heartbeatTimeout_;
		bool isAcked_;
		util::StackAllocator *alloc_;
	};

	class BlockQueue {
	public:

		BlockQueue(int32_t partitionNum, int32_t queueSizeLimit = INT32_MAX)
			: queueList_(NULL),
			  partitionNum_(partitionNum),
			  queueSize_(queueSizeLimit) {}

		~BlockQueue() {
			if (queueList_) {
				for (uint32_t pos = 0; pos < partitionNum_; pos++) {
					ALLOC_DELETE(*alloc_, &queueList_[pos]);
				}
			}
		};

		void init(util::StackAllocator &alloc) {
			alloc_ = &alloc;
			queueList_ = ALLOC_NEW(alloc) std::set<NodeId>[partitionNum_];
		}

		bool find(PartitionId pId, NodeId nodeId);

		void push(PartitionId pId, NodeId nodeId);

		void clear(PartitionId pId);

		void clear() {
			for (PartitionId pId = 0; pId < partitionNum_; pId++) {
				clear(pId);
			}
		}

		void erase(PartitionId pId, NodeId nodeId);

		int32_t size(PartitionId pId) {
			return static_cast<int32_t>(queueList_[pId].size());
		}

	private:

		std::set<NodeId> *queueList_;
		uint32_t partitionNum_;
		int32_t queueSize_;
		util::StackAllocator *alloc_;
	};

	class Partition {
		struct MasterPartitionInfo;

	public:

		Partition() : roles_(NULL), masterInfoSize_(0), alloc_(NULL) {}

		~Partition() {
			if (roles_) {
				for (int32_t pos = 0; pos < PT_TABLE_MAX; pos++) {
					ALLOC_DELETE(*alloc_, &roles_[pos]);
				}
			}
		}

		void init(util::StackAllocator &alloc, PartitionId pId,
			PartitionGroupId pgId, int32_t nodeLimit) {
			masterInfoLimitSize_ = nodeLimit;
			alloc_ = &alloc;
			roles_ = ALLOC_NEW(alloc) PartitionRole[PT_TABLE_MAX];
			for (int32_t pos = 0; pos < PT_TABLE_MAX; pos++) {
				roles_[pos].init(pId, static_cast<TableType>(pos));
			}
			partitionInfo_.pgId_ = pgId;
		}

		void resizeMaster(int32_t newSize) {
			if (newSize == 0) {
				masterPartitionInfos_.clear();
				masterInfoSize_ = 0;
			}
			if (newSize > masterInfoSize_) {
				masterPartitionInfos_.resize(newSize);
				masterInfoSize_ = newSize;
			}
		}

		void clear(TableType type = PT_TABLE_MAX) {
			if (type == PT_TABLE_MAX) {
				for (size_t i = 0; i < PT_TABLE_MAX; i++) {
					roles_[i].clear();
				}
			}
			else {
				roles_[type].clear();
			}
		};

		void checkAndResize(NodeId nodeId);

		void check(NodeId nodeId);

		LogSequentialNumber getLsn(NodeId nodeId) {
			if (nodeId == 0) {
				return partitionInfo_.lsn_;
			}
			else {
				checkAndResize(nodeId);
				return masterPartitionInfos_[nodeId].lsn_;
			}
		}

		int32_t getLsnChangeCount(NodeId nodeId) {
			if (nodeId > 0) {
				checkAndResize(nodeId);
			}
			return masterPartitionInfos_[nodeId].notChangeCount_;
		}

		LogSequentialNumber getStartLsn(NodeId nodeId) {
			if (nodeId == 0) {
				return partitionInfo_.startLsn_;
			}
			else {
				checkAndResize(nodeId);
				return masterPartitionInfos_[nodeId].startLsn_;
			}
		}

		void setLsn(NodeId nodeId, LogSequentialNumber lsn) {
			if (nodeId == 0) {
				partitionInfo_.lsn_ = lsn;
			}
			else {
				checkAndResize(nodeId);
				masterPartitionInfos_[nodeId].lsn_ = lsn;
			}
		}

		void setStartLsn(NodeId nodeId, LogSequentialNumber lsn) {
			if (nodeId == 0) {
				partitionInfo_.startLsn_ = lsn;
			}
			else {
				checkAndResize(nodeId);
				masterPartitionInfos_[nodeId].startLsn_ = lsn;
			}
		}

		void setLsnWithCheck(NodeId nodeId, LogSequentialNumber lsn) {
			if (nodeId > 0) {
				checkAndResize(nodeId);
			}
			else {
				partitionInfo_.lsn_ = lsn;
			}
			if (lsn == masterPartitionInfos_[nodeId].lsn_) {
				if (masterPartitionInfos_[nodeId].notChangeCount_ <
					PartitionConfig::PT_LSN_NOTCHANGE_COUNT_MAX) {
					masterPartitionInfos_[nodeId].notChangeCount_++;
				}
			}
			else {
				masterPartitionInfos_[nodeId].notChangeCount_ = 0;
			}
			masterPartitionInfos_[nodeId].lsn_ = lsn;
		}

		PartitionStatus getPartitionStatus(NodeId nodeId) {
			if (nodeId == 0) {
				return partitionInfo_.status_;
			}
			else {
				checkAndResize(nodeId);
				return masterPartitionInfos_[nodeId].status_;
			}
		}

		void setPartitionStatus(NodeId nodeId, PartitionStatus status) {
			if (nodeId == 0) {
				partitionInfo_.status_ = status;
			}
			else {
				checkAndResize(nodeId);
				masterPartitionInfos_[nodeId].status_ = status;
			}
		}

		PartitionRoleStatus getPartitionRoleStatus(NodeId nodeId) {
			if (nodeId == 0) {
				return static_cast<PartitionRoleStatus>(
					partitionInfo_.roleStatus_);
			}
			else {
				checkAndResize(nodeId);
				return static_cast<PartitionRoleStatus>(
					masterPartitionInfos_[nodeId].roleStatus_);
			}
		}

		void setPartitionRoleStatus(
			NodeId nodeId, PartitionRoleStatus roleStatus) {
			if (nodeId == 0) {
				partitionInfo_.roleStatus_ = roleStatus;
			}
			else {
				checkAndResize(nodeId);
				masterPartitionInfos_[nodeId].roleStatus_ = roleStatus;
			}
		}


		PartitionRole *roles_;

		PartitionInfo partitionInfo_;

	private:

		struct MasterPartitionInfo {
			MasterPartitionInfo()
				: lsn_(0),
				  startLsn_(0),
				  status_(PT_OFF),
				  notChangeCount_(0),
				  roleStatus_(static_cast<uint8_t>(PT_NONE)) {}
			LogSequentialNumber lsn_;
			LogSequentialNumber startLsn_;
			PartitionStatus status_;
			int8_t notChangeCount_;
			uint8_t roleStatus_;
		};



		std::vector<MasterPartitionInfo> masterPartitionInfos_;

		int32_t masterInfoSize_;
		int32_t masterInfoLimitSize_;
		util::StackAllocator *alloc_;
	};

	struct ReplicaInfo {

		ReplicaInfo(PartitionId pId, uint32_t count)
			: pId_(pId), count_(count) {}


		PartitionId pId_;
		uint32_t count_;
	};

	struct CatchupNodeInfo {

		CatchupNodeInfo(NodeId nodeId, uint32_t count, uint32_t)
			: nodeId_(nodeId), count_(count) {}


		NodeId nodeId_;
		uint32_t count_;
	};

	struct CatchupOwnerNodeInfo {

		CatchupOwnerNodeInfo(NodeId nodeId, uint32_t count1, uint32_t count2)
			: nodeId_(nodeId), count1_(count1), count2_(count2) {}


		NodeId nodeId_;
		uint32_t count1_;
		uint32_t count2_;
	};

	struct LoadSummary {

		LoadSummary(int32_t nodeLimit)
			: ownerLimit_(PartitionConfig::PT_OWNER_LOAD_LIMIT),
			  dataLimit_(PartitionConfig::PT_DATA_LOAD_LIMIT),
			  status_(PT_NORMAL),
			  ownerLoadCount_(0),
			  dataLoadCount_(0) {
			ownerLoadList_.assign(static_cast<size_t>(nodeLimit), 0);
			dataLoadList_.assign(static_cast<size_t>(nodeLimit), 0);
			liveNodeList_.assign(static_cast<size_t>(nodeLimit), 0);
		}

		void init(int32_t nodeNum) {
			std::fill(
				ownerLoadList_.begin(), ownerLoadList_.begin() + nodeNum, 0);
			std::fill(
				dataLoadList_.begin(), dataLoadList_.begin() + nodeNum, 0);
			minOwnerLoad_ = UINT32_MAX;
			maxOwnerLoad_ = 0;
			minDataLoad_ = UINT32_MAX;
			maxDataLoad_ = 0;
			nodeNum_ = nodeNum;
			ownerLoadCount_ = 0;
			dataLoadCount_ = 0;
		}

		void add(NodeId nodeId, bool isOwner = true) {
			if (isOwner) {
				ownerLoadList_[nodeId]++;
			}
			else {
				dataLoadList_[nodeId]++;
			}
		}

		void setAlive(NodeId nodeId, uint8_t isAlive) {
			liveNodeList_[nodeId] = isAlive;
		}

		void update() {
			for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
				if (liveNodeList_[nodeId]) {
					uint32_t currentOwnerLoad = ownerLoadList_[nodeId];
					uint32_t currentDataLoad = dataLoadList_[nodeId];
					if (currentOwnerLoad < minOwnerLoad_)
						minOwnerLoad_ = ownerLoadList_[nodeId];
					if (currentOwnerLoad > maxOwnerLoad_)
						maxOwnerLoad_ = ownerLoadList_[nodeId];
					if (currentDataLoad < minDataLoad_)
						minDataLoad_ = dataLoadList_[nodeId];
					if (currentDataLoad > maxDataLoad_)
						maxDataLoad_ = dataLoadList_[nodeId];
					ownerLoadCount_ += currentOwnerLoad;
					dataLoadCount_ += currentDataLoad;
				}
			}
		}

		bool isBalance(bool isOwner, int32_t checkCount) {
			if (isOwner) {
				return (checkCount <= ownerLoadCount_ &&
						maxOwnerLoad_ - minOwnerLoad_ <= ownerLimit_);
			}
			else {
				return (checkCount <= dataLoadCount_ &&
						maxDataLoad_ - minDataLoad_ <= dataLimit_);
			}
		}

		std::string dump() {
			util::NormalOStringStream ss;
			int32_t i = 0;
			ss << "{ownerLoad:[";
			for (i = 0; i < nodeNum_; i++) {
				ss << ownerLoadList_[i];
				if (i != nodeNum_ - 1) {
					ss << ",";
				}
			}
			ss << "], dataLoad:[";
			for (i = 0; i < nodeNum_; i++) {
				ss << dataLoadList_[i];
				if (i != nodeNum_ - 1) {
					ss << ",";
				}
			}
			ss << "], ownerLoadCount:" << ownerLoadCount_
			   << ", dataLoadCount:" << dataLoadCount_ << "}";
			return ss.str();
		}


		uint32_t minOwnerLoad_;
		uint32_t maxOwnerLoad_;
		uint32_t minDataLoad_;
		uint32_t maxDataLoad_;
		std::vector<uint32_t> ownerLoadList_;
		std::vector<uint32_t> dataLoadList_;
		std::vector<uint8_t> liveNodeList_;
		uint32_t ownerLimit_;
		uint32_t dataLimit_;
		CheckedPartitionStat status_;
		int32_t nodeNum_;
		int32_t ownerLoadCount_;
		int32_t dataLoadCount_;
	};

	class PartitionConfig {
	public:

		static const int32_t PT_BACKUP_GAP_FACTOR = 2;
		static const int32_t PT_OWNER_LOAD_LIMIT = 2;
		static const int32_t PT_DATA_LOAD_LIMIT = 2;
		static const int32_t PT_LSN_NOTCHANGE_COUNT_MAX = 12;
		static const int64_t PT_FAILOVER_LIMIT_TIME = 30 * 1000;


		PartitionConfig(const ConfigTable &config, int32_t maxNodeNum);

		int32_t getLimitClusterNodeNum() {
			return limitClusterNodeNum_;
		}

		bool setLimitOwnerBackupLsnGap(int32_t gap) {
			if (gap < 0 || gap > INT32_MAX) {
				return false;
			}
			limitOwnerBackupLsnGap_ = gap;
			return true;
		}

		int32_t getLimitOwnerBackupLsnGap() {
			return limitOwnerBackupLsnGap_;
		}

		bool setLimitOwnerBackupLsnDetectErrorGap(int32_t gap) {
			if (gap < 0 || gap > INT32_MAX) {
				return false;
			}
			limitOwnerBackupLsnDetectErrorGap_ = gap;
			return true;
		}

		int32_t getLimitOwnerBackupLsnDetectErrorGap() {
			return limitOwnerBackupLsnDetectErrorGap_;
		}

		bool setLimitOwnerBackupLsnDropGap(int32_t gap) {
			if (gap < 0 || gap > INT32_MAX) {
				return false;
			}
			limitOwnerBackupLsnDropGap_ = gap;
			return true;
		}

		int32_t getLimitOwnerBackupLsnDropGap() {
			return limitOwnerBackupLsnDropGap_;
		}

		bool setLimitOwnerCatchupLsnGap(int32_t gap) {
			if (gap < 0 || gap > INT32_MAX) {
				return false;
			}
			limitOwnerCatchupLsnGap_ = gap;
			return true;
		}

		int32_t getLimitOwnerCatchupLsnGap() {
			return limitOwnerCatchupLsnGap_;
		}

		bool setOwnerLoadLimit(int32_t limitCount) {
			if (limitCount < 0 || limitCount > INT32_MAX) {
				return false;
			}
			ownerLoadLimit_ = limitCount;
			return true;
		}

		int32_t getOwnerLoadLimit() {
			return ownerLoadLimit_;
		}

		bool setDataLoadLimit(int32_t limitCount) {
			if (limitCount < 0 || limitCount > INT32_MAX) {
				return false;
			}
			dataLoadLimit_ = limitCount;
			return true;
		}

		int32_t getDataLoadLimit() {
			return dataLoadLimit_;
		}

		bool isSucceedCurrentOwner() {
			return isSucceedCurrentOwner_;
		}

		void setSucceedCurrentOwner(bool flag) {
			isSucceedCurrentOwner_ = flag;
		}


		uint32_t partitionNum_;
		int32_t replicationNum_;
		int32_t limitOwnerBackupLsnGap_;
		int32_t limitOwnerBackupLsnDetectErrorGap_;
		int32_t limitOwnerBackupLsnDropGap_;
		int32_t limitOwnerCatchupLsnGap_;
		int32_t limitClusterNodeNum_;
		int32_t limitMaxLsnGap_;
		uint32_t ownerLoadLimit_;
		uint32_t dataLoadLimit_;
		int32_t maxNodeNum_;
		bool isSucceedCurrentOwner_;
	};



	static bool cmpReplica(const ReplicaInfo &a, const ReplicaInfo &b) {
		return (a.count_ < b.count_);
	};

	static bool cmpCatchupOwnerNode(
		const CatchupOwnerNodeInfo &a, const CatchupOwnerNodeInfo &b) {
		if (a.count1_ < b.count1_) {
			return true;
		}
		else if (a.count1_ > b.count1_) {
			return false;
		}
		else {
			if (a.count2_ < b.count2_) {
				return true;
			}
			else if (a.count2_ > b.count2_) {
				return false;
			}
			else {
				if (a.nodeId_ < b.nodeId_) {
					return true;
				}
				else {
					return false;
				}
			}
		}
	};

	static bool cmpCatchupNode(
		const CatchupNodeInfo &a, const CatchupNodeInfo &b) {
		if (a.count_ < b.count_) {
			return true;
		}
		else if (a.count_ > b.count_) {
			return false;
		}
		else {
			if (a.nodeId_ < b.nodeId_) {
				return true;
			}
			else {
				return false;
			}
		}
	};

	template <class T>
	static void sortOrderList(util::XArray<T> &orderList,
		util::XArray<NodeId> &nodeList, util::XArray<uint32_t> &ownerLoadList,
		util::XArray<uint32_t> &dataLoadList, uint32_t &maxOwnerLoad,
		uint32_t &minOwnerLoad, uint32_t &maxDataLoad, uint32_t &minDataLoad,
		bool (*func)(const T &, const T &), bool isCatchup = false) {
		NodeId nodeId;
		minOwnerLoad = UINT32_MAX;
		maxOwnerLoad = 0;
		minDataLoad = UINT32_MAX;
		maxDataLoad = 0;
		uint32_t currentOwnerLoad, currentDataLoad, targetLoad;
		for (util::XArray<NodeId>::iterator nodeItr = nodeList.begin();
			 nodeItr != nodeList.end(); nodeItr++) {
			nodeId = (*nodeItr);

			currentOwnerLoad = ownerLoadList[nodeId];
			currentDataLoad = dataLoadList[nodeId];

			if (isCatchup) {
				targetLoad = currentDataLoad;
			}
			else {
				targetLoad = currentOwnerLoad;
			}
			T nodeInfo(nodeId, targetLoad, currentDataLoad);
			orderList.push_back(nodeInfo);
			if (currentOwnerLoad < minOwnerLoad)
				minOwnerLoad = ownerLoadList[nodeId];
			if (currentOwnerLoad > maxOwnerLoad)
				maxOwnerLoad = ownerLoadList[nodeId];
			if (currentDataLoad < minDataLoad)
				minDataLoad = dataLoadList[nodeId];
			if (currentDataLoad > maxDataLoad)
				maxDataLoad = dataLoadList[nodeId];
		}
		std::sort(orderList.begin(), orderList.end(), func);
	}



	bool isBalance(uint32_t minLoad, uint32_t maxLoad, bool isOwnerCheck) {
		if (isOwnerCheck) {
			return (maxLoad - minLoad <= config_.ownerLoadLimit_);
		}
		else {
			return (maxLoad - minLoad <= config_.dataLoadLimit_);
		}
	}

	void adjustMaxLSN(LogSequentialNumber &maxLSN, int32_t maxLsnGap);

	bool checkLsnGap(LogSequentialNumber ownerLSN,
		LogSequentialNumber backupLSN, uint64_t gapValue) {
		if (ownerLSN >= backupLSN) {
			return (ownerLSN - backupLSN > gapValue);
		}
		else {
			return (backupLSN - ownerLSN > gapValue);
		}
	}

	bool checkLimitOwnerBackupLsnGap(LogSequentialNumber ownerLSN,
		LogSequentialNumber backupLSN, int32_t gapFactor = 1) {
		uint64_t gapValue =
			static_cast<uint64_t>(config_.limitOwnerBackupLsnGap_ * gapFactor);
		if (ownerLSN >= backupLSN) {
			return (ownerLSN - backupLSN > gapValue);
		}
		else {
			return (backupLSN - ownerLSN > gapValue);
		}
	}

	bool checkLimitOwnerBackupLsnDetectErrorGap(
		LogSequentialNumber ownerLSN, LogSequentialNumber backupLSN) {
		uint64_t gapValue =
			static_cast<uint64_t>(config_.limitOwnerBackupLsnDetectErrorGap_);
		if (ownerLSN >= backupLSN) {
			return (ownerLSN - backupLSN > gapValue);
		}
		else {
			return (backupLSN - ownerLSN > gapValue);
		}
	}

	bool checkPromoteOwnerCatchupLsnGap(
		LogSequentialNumber ownerLSN, LogSequentialNumber catchupLSN) {
		uint64_t gapValue =
			static_cast<uint64_t>(config_.limitOwnerCatchupLsnGap_);

		if (ownerLSN == 0 && catchupLSN == 0) {
			return true;
		}
		else if (catchupLSN > ownerLSN) {
			return true;
		}
		else if (ownerLSN - catchupLSN <= gapValue) {
			return true;
		}
		else {
			return false;
		}
	}

	bool checkNextCandOwner(PartitionId pId, NodeId nodeId, bool isOwnerActive,
		PartitionRole *currentRole) {
		LogSequentialNumber targetLsn = getLSN(pId, nodeId);
		LogSequentialNumber &maxLsn = maxLsnList_[pId];

		if (isOwnerActive) {
			if (((getPartitionStatus(pId, nodeId) == PT_ON) &&
					currentRole->isOwner(nodeId)) ||
				currentRole->isBackup(nodeId) ||
				(prevCatchupPId_ == pId && prevCatchupNodeId_ == nodeId &&
					checkPromoteOwnerCatchupLsnGap(maxLsn, targetLsn))) {
				return true;
			}
		}
		else {
			if (maxLsn <= targetLsn) {
				return true;
			}
		}
	}



	util::FixedSizeAllocator<util::Mutex> fixedSizeAlloc_;
	util::StackAllocator alloc_;

	int32_t nodeNum_;
	NodeId masterNodeId_;
	PartitionRevision revision_;
	std::set<PartitionId> gossips_;
	std::vector<LogSequentialNumber> maxLsnList_;
	std::vector<LogSequentialNumber> repairLsnList_;
	std::vector<NodeId> currentActiveOwnerList_;

	std::vector<int32_t> gapCountList_;

	util::Mutex revisionLock_;
	util::Mutex nodeInfoLock_;
	util::RWLock lock_;
	util::RWLock *partitionLockList_;

	std::set<NodeId> downNodeList_;
	std::set<NodeId> addNodeList_;

	PartitionConfig config_;
	NodeId prevMaxNodeId_;

	BlockQueue nextBlockQueue_;
	BlockQueue goalBlockQueue_;
	PartitionId promoteCandPId_;
	PartitionId prevCatchupPId_;
	PartitionId currentCatchupPId_;
	NodeId prevCatchupNodeId_;

	Partition *partitions_;

	NodeInfo *nodes_;

	LoadSummary loadInfo_;

	int32_t partitionDigit_;
	int32_t lsnDigit_;

	bool failCatchup_;
	bool isRepaired_;
	int64_t prevFailoverTime_;

	ClusterManager *clsMgr_;

};

typedef PartitionTable::CheckedPartitionStat CheckedPartitionStat;
typedef PartitionTable::PartitionStatus PartitionStatus;
typedef PartitionTable::PartitionRevision PartitionRevision;
typedef PartitionTable::PartitionRole PartitionRole;
typedef PartitionTable::PartitionContext PartitionContext;
typedef PartitionTable::PartitionRoleStatus PartitionRoleStatus;
typedef PartitionTable::TableType TableType;

/*!
	@brief Represents data cut out from Partition for communicatition between
   nodes
*/
class SubPartition {
public:

	SubPartition() {}

	SubPartition(PartitionId pId, bool isActive, bool isDownNextOwner,
		PartitionRole &role, NodeAddress &currentOwner)
		: pId_(pId),
		  isActive_(isActive),
		  isDownNextOwner_(isDownNextOwner),
		  role_(role),
		  currentOwner_(currentOwner){};

	SubPartition(PartitionId pId, bool isActive, bool isDownNextOwner,
		PartitionRole &role)
		: pId_(pId),
		  isActive_(isActive),
		  isDownNextOwner_(isDownNextOwner),
		  role_(role){};

	MSGPACK_DEFINE(pId_, isActive_, isDownNextOwner_, role_, currentOwner_);

	std::string dump() {
		util::NormalOStringStream ss;
		ss << "SubPartition:{"
		   << "pId:" << pId_ << ", isActive:" << isActive_
		   << ", isDownNextOwner:" << isDownNextOwner_
		   << ", role:" << role_.dump()
		   << ", current owner:" << currentOwner_.toString() << "}";
		return ss.str();
	}


	PartitionId pId_;
	bool isActive_;
	bool isDownNextOwner_;
	PartitionRole role_;
	NodeAddress currentOwner_;
};

std::ostream &operator<<(std::ostream &stream, SubPartition &subPartition);
std::ostream &operator<<(std::ostream &stream, PartitionRole &PartitionRole);
std::ostream &operator<<(std::ostream &stream, NodeAddress &nodeAddres);
std::ostream &operator<<(std::ostream &stream, PartitionRevision &revision);

/*!
	@brief SubPartition Table
*/
class SubPartitionTable {
public:

	SubPartitionTable() {}

	SubPartitionTable(PartitionRevision &revision) : revision_(revision) {}

	int32_t size() {
		return static_cast<int32_t>(subPartitionList_.size());
	}

	SubPartition &getSubPartition(int32_t pos) {
		return subPartitionList_[pos];
	}

	void set(PartitionTable *pt, PartitionId pId, bool isActive,
		bool isDownNextOwner, PartitionRole &role, NodeAddress &ownerAddress) {
		SubPartition subPartition(
			pId, isActive, isDownNextOwner, role, ownerAddress);
		PartitionRole &tmpRole = subPartition.role_;
		tmpRole.encodeAddress(pt, role);
		subPartitionList_.push_back(subPartition);
	}

	void set(PartitionTable *pt, PartitionId pId, bool isActive,
		bool isDownNextOwner, PartitionRole &role) {
		SubPartition subPartition(pId, isActive, isDownNextOwner, role);
		PartitionRole &tmpRole = subPartition.role_;
		tmpRole.encodeAddress(pt, role);
		subPartitionList_.push_back(subPartition);
	}

	void setNodeAddress(AddressInfo &addressInfo) {
		nodeAddressList_.push_back(addressInfo);
	}

	std::vector<AddressInfo> &getNodeAddressList() {
		return nodeAddressList_;
	}

	PartitionRevision &getRevision() {
		return revision_;
	}

	MSGPACK_DEFINE(subPartitionList_, nodeAddressList_, revision_);

	std::string dump() {
		util::NormalOStringStream ss;
		ss << "subPartitionTable:{"
		   << "ptRev:" << revision_.toString();
		ss << ", [";
		for (size_t pos = 0; pos < subPartitionList_.size(); pos++) {
			ss << subPartitionList_[pos].dump();
			if (pos != subPartitionList_.size() - 1) {
				ss << ",";
			}
		}
		ss << "]}";
		return ss.str();
	}


	std::vector<SubPartition> subPartitionList_;
	std::vector<AddressInfo> nodeAddressList_;

	PartitionRevision revision_;
};

static inline const std::string dumpPartitionStatus(PartitionStatus status) {
	switch (status) {
	case PartitionTable::PT_STOP:
		return "STOP";
	case PartitionTable::PT_OFF:
		return "OFF";
	case PartitionTable::PT_SYNC:
		return "SYNC";
	case PartitionTable::PT_ON:
		return "ON";
	default:
		return "";
	}
};

static inline const std::string dumpPartitionTableType(
	PartitionTable::TableType type) {
	switch (type) {
	case PartitionTable::PT_NEXT_OB:
		return "NEXT_OB";
	case PartitionTable::PT_NEXT_GOAL:
		return "NEXT_GOAL";
	case PartitionTable::PT_CURRENT_OB:
		return "CURRENT_OB";
	case PartitionTable::PT_CURRENT_GOAL:
		return "CURRENT_GOAL";
	case PartitionTable::PT_TMP_OB:
		return "TMP_OB";
	case PartitionTable::PT_TMP_GOAL:
		return "TMP_GOAL";
	default:
		return "";
	}
}

static inline const std::string dumpPartitionTableType(uint8_t type) {
	TableType tableType = static_cast<TableType>(type);
	switch (tableType) {
	case PartitionTable::PT_NEXT_OB:
		return "NEXT_OB";
	case PartitionTable::PT_NEXT_GOAL:
		return "NEXT_GOAL";
	case PartitionTable::PT_CURRENT_OB:
		return "CURRENT_OB";
	case PartitionTable::PT_CURRENT_GOAL:
		return "CURRENT_GOAL";
	case PartitionTable::PT_TMP_OB:
		return "TMP_OB";
	case PartitionTable::PT_TMP_GOAL:
		return "TMP_GOAL";
	default:
		return "";
	}
}

static inline const std::string dumpPartitionRoleStatus(
	PartitionTable::PartitionRoleStatus status) {
	switch (status) {
	case PartitionTable::PT_OWNER:
		return "OWNER";
	case PartitionTable::PT_BACKUP:
		return "BACKUP";
	case PartitionTable::PT_CATCHUP:
		return "CATCHUP";
	case PartitionTable::PT_NONE:
		return "NONE";
	default:
		return "";
	}
}

#endif
