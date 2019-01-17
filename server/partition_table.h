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
#include "event_engine.h"
class ClusterManager;

#include "msgpack_utils.h"
class ConfigTable;


typedef int32_t NodeId;

const int32_t UNDEF_NODEID = -1;
const int64_t UNDEF_TTL = 0;
const int64_t NOT_UNDEF_TTL = 1;
const NodeId SELF_NODEID = 0;

const int32_t PARTITION_STATUS_SUCCESS = 0;
const int32_t OWNER_ROLE_UNMATCH = 1;
const int32_t OWNER_STATUS_UNMATCH = 2;
const int32_t OWNER_REVISION_UNMATCH = 3;
const int32_t OWNER_RECIEVE_ERROR = 4;
const int32_t BACKUP_ROLE_UNMATCH = 5;
const int32_t BACKUP_STATUS_UNMATCH = 6;
const int32_t BACKUP_REIVION_UNMATCH = 7;
const int32_t BACKUP_RECIEVE_ERROR = 8;

const int32_t CATCHUP_ROLE_UNMATCH = 9;
const int32_t CATCHUP_STATUS_UNMATCH = 10;
const int32_t CATCHUP_REIVION_UNMATCH = 11;
const int32_t CATCHUP_RECIEVE_ERROR = 12;
const int32_t CATCHUP_PROMOTION = 13;

class SubPartitionTable;
class SubPartition;

namespace picojson {
class value;
}
/*
static std::string dumpPartitionReason(int32_t reason) {
	switch (reason) {
	case PARTITION_STATUS_SUCCESS:
		return "PARTITION_STATUS_SUCCESS";
	case OWNER_ROLE_UNMATCH:
		return "OWNER_ROLE_UNMATCH";
	case OWNER_STATUS_UNMATCH:
		return "OWNER_STATUS_UNMATCH";
	case OWNER_REVISION_UNMATCH:
		return "OWNER_REVISION_UNMATCH";
	case OWNER_RECIEVE_ERROR:
		return "OWNER_RECIEVE_ERROR";
	case BACKUP_ROLE_UNMATCH:
		return "BACKUP_ROLE_UNMATCH";
	case BACKUP_STATUS_UNMATCH:
		return "BACKUP_STATUS_UNMATCH";
	case BACKUP_REIVION_UNMATCH:
		return "BACKUP_REIVION_UNMATCH";
	case BACKUP_RECIEVE_ERROR:
		return "BACKUP_RECIEVE_ERROR";
	case CATCHUP_ROLE_UNMATCH:
		return "CATCHUP_ROLE_UNMATCH";
	case CATCHUP_STATUS_UNMATCH:
		return "CATCHUP_STATUS_UNMATCH";
	case CATCHUP_REIVION_UNMATCH:
		return "CATCHUP_REIVION_UNMATCH";
	case CATCHUP_RECIEVE_ERROR:
		return "CATCHUP_RECIEVE_ERROR";
	case CATCHUP_PROMOTION:
		return "CATCHUP_PROMOTION";
	default:
		return "";
	}
}
*/
class	ClusterExecStatus {
public:
	static const int32_t STATUS_INITAL = 0;
	static const int32_t STATUS_RECONSTRUCT = 1;
	static const int32_t STATUS_NORMAL = 2;
	static const int32_t STATUS_NODE_DOWN = 3;
	static const int32_t STATUS_SHORT_SYNC = 4;
	static const int32_t STATUS_LONG_SYNC = 5;
	static const int32_t STATUS_MAX = 6;

	static const int32_t DETECT_FAIL_SINGLE = 0;
	static const int32_t DETECT_FAIL_MULTI = 1;
	static const int32_t DETECT_NODE_RECOVERY = 2;
	static const int32_t DETECT_INCREASE_NODE = 3;
	static const int32_t DETECT_CATCHUP_NODE = 4;
	static const int32_t DETECT_INVALID_SELECT_OWNER = 5;
	static const int32_t DETECT_SHORT_SYNC_TIMEOUT = 6;
	static const int32_t DETECT_INVALID_LOG = 7;
	static const int32_t DETECT_INVALID_CHUNK = 8;
	static const int32_t DETECT_DEATCITVATE_OWNER = 9;
	static const int32_t DETECT_NOT_BALANCE = 10;
	static const int32_t DETECT_MAX = 11;

	static const int32_t NEXT_NORMAL = 0;
	static const int32_t NEXT_RESCUE_MODE = 1;
	static const int32_t NEXT_OWNER_CHANGE = 2;
	static const int32_t NEXT_OWNER_CATCHUP = 3;
	static const int32_t NEXT_MAX = 4;

	static const int32_t CURRENT_STATUS_MAX = 3;
	int32_t status_[CURRENT_STATUS_MAX];
	std::vector< std::vector< std::vector<int64_t> > > coverageList_;
	
	ClusterExecStatus() :
	coverageList_(STATUS_MAX, std::vector<std::vector<int64_t> >(DETECT_MAX, std::vector<int64_t>(NEXT_MAX)))
	{
	}

	void init() {
		for (int32_t pos = 0; pos < CURRENT_STATUS_MAX; pos++) {
			status_[pos] = -1;
		}
	}

	void exec();
	std::string dumpClusterStatus(int32_t status);
	std::string dumpDetection(int32_t status);
	std::string dumpNext(int32_t status);

	void setClusterStatus(int32_t status);
	void setDetection(int32_t status);
	void setNext(int32_t status);
	static void trace();
};


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
		return (address_ != 0);
	}

	bool operator==(const NodeAddress &nodeAddress) const {
		return (
			memcmp(&address_, &(nodeAddress.address_), sizeof(address_)) == 0 &&
			port_ == nodeAddress.port_);
	}

	bool operator < (const NodeAddress& nodeAddress) const {
		if (port_ < nodeAddress.port_) {
			return true;
		}
		else {
			return (memcmp(&address_, &(nodeAddress.address_), sizeof(address_)) < 0);
		}
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
		transactionAddress_(txnAddress),
		syncAddress_(syncAddress),
		systemAddress_(systemAddress),
		isActive_(false) {}

	NodeAddress &getNodeAddress(ServiceType type);
	void setNodeAddress(ServiceType type, NodeAddress &address);

	bool operator<(const AddressInfo &right) const {
		if (clusterAddress_.address_ < right.clusterAddress_.address_) {
			return true;
		}
		if (clusterAddress_.address_ > right.clusterAddress_.address_) {
			return false;
		}
		return (clusterAddress_.port_ < right.clusterAddress_.port_);
	}

	std::string dump() {
		util::NormalOStringStream ss;
		ss << "NodeAddress:{"
		<< "cluster={" << clusterAddress_.dump() << "}, "
		<< "sync={" << syncAddress_.dump() << "}, "
		<< "transaction={" << transactionAddress_.dump() << "}, "
		<< "system={" << systemAddress_.dump() << "}, "
		<< "isActive={" << isActive_ << "}"
		<< "}";
		return ss.str();
	}

	NodeAddress clusterAddress_;
	NodeAddress transactionAddress_;
	NodeAddress syncAddress_;
	NodeAddress systemAddress_;
	NodeAddress sqlServiceAddress_;
	NodeAddress dummy1_;
	NodeAddress dummy2_;
	bool isActive_;
	MSGPACK_DEFINE(clusterAddress_, transactionAddress_, syncAddress_, systemAddress_,
		sqlServiceAddress_, dummy1_, dummy2_, isActive_);
};

	static const int32_t PT_BACKUP_GAP_FACTOR = 2;
	static const int32_t PT_OWNER_LOAD_LIMIT = 2;
	static const int32_t PT_DATA_LOAD_LIMIT = 2;
	static const int32_t PT_LSN_NOTCHANGE_COUNT_MAX = 12;
	/*!
		@brief Result of Partition statistics check
	*/
	enum CheckedPartitionStat {
		PT_NORMAL,
		PT_NOT_BALANCE,
		PT_REPLICA_LOSS,
		PT_PARTIAL_STOP,
		PT_ABNORMAL
	};

//static int32_t PT_ERROR_SHORT_OWNER_BACKUP_INVALID = -3;
static const int32_t PT_ERROR_LONG_SYNC_FAIL = -2;
static const int32_t PT_ERROR_SHORT_SYNC_FAIL = -1;
static const int32_t PT_ERROR_NORMAL = 0;
static const int32_t PT_OTHER_NORMAL = 0;
static const int32_t PT_OTHER_SHORT_SYNC_COMPLETE = 1;

	struct LoadSummary {
		LoadSummary(util::StackAllocator &alloc, int32_t nodeLimit)
			: ownerLimit_(PT_OWNER_LOAD_LIMIT),
			dataLimit_(PT_DATA_LOAD_LIMIT),
			status_(PT_NORMAL),
			ownerLoadCount_(0),
			dataLoadCount_(0),
			ownerLoadList_(alloc),
			dataLoadList_(alloc),
			liveNodeList_(alloc),
			nodeNum_(0)
		{
			ownerLoadList_.assign(static_cast<size_t>(nodeLimit), 0);
			dataLoadList_.assign(static_cast<size_t>(nodeLimit), 0);
			liveNodeList_.assign(static_cast<size_t>(nodeLimit), 0);
		}

		void setStatus(CheckedPartitionStat stat) {
			if (status_ == PT_ABNORMAL) return;
			int32_t  check1 = static_cast<int32_t>(stat);
			int32_t  check2 = static_cast<int32_t>(status_);
			if (check1 > check2) {
				status_ = stat;
			}
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

		void addOwner(NodeId nodeId) {
			ownerLoadList_[nodeId]++;
			dataLoadList_[nodeId]++;
		}
		void addBackup(NodeId nodeId) {
			dataLoadList_[nodeId]++;
		}

		void deleteOwner(NodeId nodeId) {
			if (ownerLoadList_[nodeId] > 0) {
				ownerLoadList_[nodeId]--;
			}
			if (dataLoadList_[nodeId] > 0) {
				dataLoadList_[nodeId]--;
			}
		}

		void deleteBackup(NodeId nodeId) {
			if (dataLoadList_[nodeId] > 0) {
				dataLoadList_[nodeId]--;
			}
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

		uint32_t ownerLimit_;
		uint32_t dataLimit_;
		CheckedPartitionStat status_;
		int32_t ownerLoadCount_;
		int32_t dataLoadCount_;
		util::XArray<uint32_t> ownerLoadList_;
		util::XArray<uint32_t> dataLoadList_;
		util::XArray<uint8_t> liveNodeList_;
		int32_t nodeNum_;

};

	struct DropNodeSet {
		DropNodeSet() : pos_(0) {}
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
		}
		NodeAddress &getNodeAddress() {
			return nodeAddress_;
		}

		MSGPACK_DEFINE(address_, port_, pIdList_, revisionList_);

		NodeAddress nodeAddress_;

		AddressType address_;
		uint16_t port_;
		int32_t pos_;
		std::vector<PartitionId> pIdList_;
		std::vector<PartitionRevisionNo> revisionList_;
	};

	class  DropPartitionNodeInfo {
	public:
		DropPartitionNodeInfo(util::StackAllocator &alloc) : alloc_(alloc), tmpDropNodeMap_(alloc) {}
		void set(PartitionId pId, NodeAddress &address, PartitionRevisionNo revision) {
			util::Map<NodeAddress, DropNodeSet>::iterator it = tmpDropNodeMap_.find(address);
			if (it != tmpDropNodeMap_.end()) {
				(*it).second.append(pId, revision);
				if (static_cast<size_t>((*it).second.pos_) >= dropNodeMap_.size()) return;
				dropNodeMap_[(*it).second.pos_].append(pId, revision);
			}
			else {
				DropNodeSet target;
				target.setAddress(address);
				target.append(pId, revision);
				target.pos_ = dropNodeMap_.size();
				dropNodeMap_.push_back(target);
				tmpDropNodeMap_.insert(std::make_pair(address, target));
			}
		}

		void init() {
			for (size_t pos = 0; pos < dropNodeMap_.size(); pos++) {
				dropNodeMap_[pos].init();
			}
		}

		size_t getSize() {
			return dropNodeMap_.size();
		}

		DropNodeSet *get(size_t pos) {
			return &dropNodeMap_[pos];
		}

		bool check() {
			return true;
		}

		MSGPACK_DEFINE(dropNodeMap_);

	private:
		util::StackAllocator &alloc_;
		util::Map<NodeAddress, DropNodeSet> tmpDropNodeMap_;
		std::vector<DropNodeSet> dropNodeMap_;
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
	static const int32_t PARTITION_ASSIGN_NORMAL = 0;
	static const int32_t PARTITION_ASSIGN_SHUFFLE = 1;

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
		PT_CURRENT_OB,
		PT_TABLE_MAX = 2
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
		static const PartitionRevisionNo INITIAL_CLUSTER_REVISION = 1;

		PartitionRevision()
			: addr_(0), port_(0), sequentialNumber_(INITIAL_CLUSTER_REVISION) {}
		PartitionRevision(AddressType addr, uint16_t port, int32_t sequentialNumber)
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

		void updateRevision(PartitionRevisionNo sequentialNumber) {
			sequentialNumber_ = sequentialNumber;
		}

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

		PartitionRole(PartitionId pId, PartitionRevision revision, TableType type) : 
			pId_(pId),
			revision_(revision),
			type_(type),
			ownerNodeId_(UNDEF_NODEID),
			messageType_(static_cast<int8_t>(type)),
			isSelfBackup_(false),
			isSelfCatchup_(false)
			{}

		void init(PartitionId pId, TableType type = PT_NEXT_OB) {
			pId_ = pId;
			ownerNodeId_ = UNDEF_NODEID;
			type_ = type;
			messageType_ = static_cast<int8_t>(type_);
			isSelfBackup_ = false;
			isSelfCatchup_ = false;
		}

		PartitionRevision &getRevision() {
			return revision_;
		}

		PartitionRevisionNo getRevisionNo() {
			return revision_.sequentialNumber_;
		}

		void getBackups(util::XArray<NodeId> &backups) {
			size_t size = backups_.size() * sizeof(NodeId);
			if (size == 0) return;
			backups.resize(size / sizeof(NodeId));
			memcpy(backups.data(), reinterpret_cast<uint8_t *>(&backups_[0]), size);
		}

		void getCatchups(util::XArray<NodeId> &catchups) {
			size_t size = catchups_.size() * sizeof(NodeId);
			if (size == 0) return;
			catchups.resize(size / sizeof(NodeId));
			memcpy(catchups.data(), reinterpret_cast<uint8_t *>(&catchups_[0]), size);
		}

		std::vector<NodeId> &getBackups() {
			return backups_;
		}

		std::vector<NodeId> &getCatchups() {
			return catchups_;
		}

		int32_t getBackupSize() {
			return static_cast<int32_t>(backups_.size());
		}

		int32_t getCatchupSize() {
			return static_cast<int32_t>(catchups_.size());
		}

		void clearBackup() {
			backups_.clear();
			backupAddressList_.clear();
			isSelfBackup_ = false;
		}

		void clearCatchup() {
			catchups_.clear();
			catchupAddressList_.clear();
			isSelfCatchup_ = false;
		}

		void clear() {
			ownerNodeId_ = UNDEF_NODEID;
			backups_.clear();
			catchups_.clear();
			backupAddressList_.clear();
			catchupAddressList_.clear();
			isSelfBackup_ = false;
			isSelfCatchup_ = false;
		}

		void clearAddress() {
			backupAddressList_.clear();
			isSelfBackup_ = false;
			catchupAddressList_.clear();
			isSelfCatchup_ = false;
		}

		bool isOwnerOrBackup() {
			return (isOwner() || isBackup());
		}

		NodeId getOwner() {
			return ownerNodeId_;
		}

		void set(NodeId owner, std::vector<NodeId> &backups, std::vector<NodeId> &catchups) {
			ownerNodeId_ = owner;
			backups_ = backups;
			catchups_ = catchups;
			checkBackup();
			checkCatchup();
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

		bool isCatchup(NodeId targetNodeId = 0) {
			if (catchups_.size() == 0) return false;
			if (targetNodeId == 0) {
				return isSelfCatchup_;
			}
			std::vector<NodeId>::iterator it;
			it = std::find(catchups_.begin(), catchups_.end(), targetNodeId);
			if (it != catchups_.end()) {
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

		void get(
			NodeAddress &nodeAddress, std::vector<NodeAddress> &backupAddress,
					std::vector<NodeAddress> &catchupAddress) {
			nodeAddress = ownerAddress_;
			backupAddress = backupAddressList_;
			catchupAddress = catchupAddressList_;
		}

		void getShortTermSyncTargetNodeId(util::Set<NodeId> &targetNodeSet) {
			if (ownerNodeId_ != UNDEF_NODEID && ownerNodeId_ > 0) {
				targetNodeSet.insert(ownerNodeId_);
			}
			for (size_t pos = 0; pos < backups_.size(); pos++) {
				if (backups_[pos] != UNDEF_NODEID && backups_[pos] > 0) {
					targetNodeSet.insert(backups_[pos]);
				}
			}
		}

		void getLongTermSyncTargetNodeId(util::Set<NodeId> &targetNodeSet) {
			if (ownerNodeId_ != UNDEF_NODEID && ownerNodeId_ > 0) {
				targetNodeSet.insert(ownerNodeId_);
			}
			for (size_t pos = 0; pos < catchups_.size(); pos++) {
				if (catchups_[pos] != UNDEF_NODEID && catchups_[pos] > 0) {
					targetNodeSet.insert(catchups_[pos]);
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
			else if (isCatchup()) {
				return PT_CATCHUP;
			}
			else {
				return PT_NONE;
			}
		}

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
		std::string dump(PartitionTable *pt, ServiceType type = CLUSTER_SERVICE);

		MSGPACK_DEFINE(
				pId_, ownerAddress_, backupAddressList_, catchupAddressList_, revision_, messageType_);


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

		void checkCatchup() {
			if (catchups_.size() > 0) {
				std::sort(catchups_.begin(), catchups_.end());
				if (catchups_[0] == 0) {
					isSelfCatchup_ = true;
				}
				else {
					isSelfCatchup_ = false;
				}
			}
			else {
				isSelfCatchup_ = false;
			}
		}
	private:

		PartitionId pId_;
		PartitionRevision revision_;
		NodeAddress ownerAddress_;
		std::vector<NodeAddress> backupAddressList_;
		TableType type_;
		NodeId ownerNodeId_;
		int8_t messageType_;
		std::vector<NodeId> backups_;
		bool isSelfBackup_;
		std::vector<NodeAddress> catchupAddressList_;
		std::vector<NodeId> catchups_;
		bool isSelfCatchup_;
	};

	/*!
		@brief Encodes context of Partition
	*/
	struct PartitionContext {

		PartitionContext(util::StackAllocator &alloc, PartitionTable *pt,
				bool needUpdatePartition, SubPartitionTable &subPartitionTable) : 
			alloc_(&alloc),
			changedShortTermPIdList_(alloc),
			changedLongTermPIdList_(alloc),
			changedPIdSet_(alloc),
			shortTermPIdList_(alloc),
			needUpdatePartition_(needUpdatePartition),
			shortTermTimeout_(UNDEF_TTL),
			pt_(pt),
			isRepairPartition_(false),
			isShufflePartition_(false),
			subPartitionTable_(subPartitionTable),
			tmpSubPartitionList_(alloc),
			enableDrop_(false),
			balanceCheck_(false),
			irregularPartitionList_(alloc),
			promoteFlag_(false),
			currentTime_(0),
			withRescue_(false),
			errorPId_(0),
			errorReason_(0)
		{
		}

		void setChangePId(SyncMode mode, PartitionId pId, PartitionRole &role);

		util::XArray<PartitionId> &getShortTermPIdList() {
			return shortTermPIdList_;
		}

		void setShorTermSyncLimitTime(int64_t shortTermTimeout) {
			shortTermTimeout_ = shortTermTimeout;
		}

		void setRepairPartition(bool isRepair) {
			isRepairPartition_ = isRepair;
		}

		void setShufflePartition(bool isShuffle);

		util::StackAllocator *alloc_;
		util::XArray<PartitionId> changedShortTermPIdList_;
		util::XArray<PartitionId> changedLongTermPIdList_;
		util::Set<PartitionId> changedPIdSet_;
		util::XArray<PartitionId> shortTermPIdList_;
		bool needUpdatePartition_;
		int64_t shortTermTimeout_;
		PartitionTable *pt_;
		bool isRepairPartition_;
		bool isShufflePartition_;
		SubPartitionTable &subPartitionTable_;
		util::Vector<SubPartition> tmpSubPartitionList_;
		bool enableDrop_;
		bool balanceCheck_;
		util::Vector<PartitionId> irregularPartitionList_;
		bool promoteFlag_;
		int64_t currentTime_;
		bool withRescue_;
		PartitionId errorPId_;
		int32_t errorReason_;
	};

	PartitionTable(const ConfigTable &configTable);
	~PartitionTable();

	void initialize() {
	}

	NodeId getOwner(PartitionId pId, TableType type = PT_CURRENT_OB);

	bool isOwner(PartitionId pId, NodeId targetNodeDescriptor = 0,
		TableType type = PT_CURRENT_OB);

	bool isOwnerOrBackup(PartitionId pId, NodeId targetNodeDescriptor = 0,
		TableType type = PT_CURRENT_OB);

	void getBackup(PartitionId pId, util::XArray<NodeId> &backups,
		TableType type = PT_CURRENT_OB);

	void getBackup(PartitionId pId, std::vector<NodeId> &backups,
			TableType type = PT_CURRENT_OB); 

	void getCatchup(PartitionId pId, util::XArray<NodeId> &catchups,
			TableType type = PT_CURRENT_OB);

	void getCatchup(PartitionId pId, std::vector<NodeId> &catchups,
			TableType type = PT_CURRENT_OB); 

	bool isBackup(PartitionId pId, NodeId targetNodeId = 0,
		TableType type = PT_CURRENT_OB);

	bool isCatchup(PartitionId pId, NodeId targetNodeId = 0,
			TableType type = PT_CURRENT_OB);

	bool hasCatchupRole(PartitionId pId, TableType type = PT_CURRENT_OB);
	void clearCatchupRole(PartitionId pId, TableType type = PT_CURRENT_OB);

	size_t getBackupSize(PartitionId pId, TableType type = PT_CURRENT_OB) {
		std::vector<NodeId> backups;
		getBackup(pId, backups, type);
		return backups.size();
	}

	bool isChangeToOwner(PartitionId pId, ChangePartitionType changePartitionType) {
		return (!isOwner(pId, PartitionTable::PT_CURRENT_OB)
				&& isOwner(pId, 0, PartitionTable::PT_NEXT_OB)
				&& changePartitionType != PT_CHANGE_SYNC_END);
	}

	void resetDownNode(NodeId targetNodeId);

	LogSequentialNumber getLSN(PartitionId pId, NodeId targetNodeId = 0) {
		util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
		return currentPartitions_[pId].getLsn(targetNodeId);
	}

	LogSequentialNumber getStartLSN(PartitionId pId, NodeId targetNodeId = 0) {
		util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
		return currentPartitions_[pId].getStartLsn(targetNodeId);
	}

	void resizeCurrenPartition(int32_t resizeNum);

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

	bool setNodeInfo(NodeId nodeId, ServiceType type, NodeAddress &address);

	void setMaxLsnList(std::vector<LogSequentialNumber> &maxLsnList) {
		maxLsnList = maxLsnList_;
	}

	void changeClusterStatus(ClusterStatus status,
			NodeId masterNodeId, int64_t currentTime, int64_t nextHeartbeatTimeout);

	PartitionRevisionNo getCurrentRevisionNo() {
		return currentRevisionNo_;
	}


	bool isAvailable(PartitionId pId) {
		return partitions_[pId].partitionInfo_.available_;
	}

	void setAvailable(PartitionId pId, bool available) {
		partitions_[pId].partitionInfo_.available_ = available;
	}

	void setLSN(
		PartitionId pId, LogSequentialNumber lsn, NodeId targetNodeId = 0) {
		util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
		currentPartitions_[pId].setLsn(targetNodeId, lsn);
	}

	void setStartLSN(
		PartitionId pId, LogSequentialNumber lsn, NodeId targetNodeId = 0) {
		util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
		currentPartitions_[pId].setStartLsn(targetNodeId, lsn);
	}

	void setLsnWithCheck(
		PartitionId pId, LogSequentialNumber lsn, NodeId targetNodeId = 0) {
		util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
		currentPartitions_[pId].setLsn(targetNodeId, lsn, true);
	}

	int32_t getLsnChangeCount(PartitionId pId, NodeId targetNode = 0) {
		util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
		return currentPartitions_[pId].getLsnChangeCount(targetNode);
	}

	PartitionStatus getPartitionStatus(PartitionId pId, NodeId targetNode = 0) {
		util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
		return currentPartitions_[pId].getPartitionStatus(targetNode);
	}

	PartitionRoleStatus getPartitionRoleStatus(
		PartitionId pId, NodeId targetNode = 0) {
		util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
		return currentPartitions_[pId].getPartitionRoleStatus(targetNode);
	}

	void setPartitionStatus(
		PartitionId pId, PartitionStatus status, NodeId targetNode = 0) {
		util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
		return currentPartitions_[pId].setPartitionStatus(targetNode, status);
	};

	void setPartitionRoleStatus(
		PartitionId pId, PartitionRoleStatus status, NodeId targetNode = 0) {
		util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
		currentPartitions_[pId].setPartitionRoleStatus(targetNode, status);
	};

	void setPartitionRevision(
			PartitionId pId, PartitionRevisionNo partitionRevisuion, NodeId targetNode = 0) {
		util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
		currentPartitions_[pId].setPartitionRevision(targetNode, partitionRevisuion);
	};

	PartitionRevisionNo getPartitionRevision(PartitionId pId, NodeId targetNode) {
		util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
		return currentPartitions_[pId].getPartitionRevision(targetNode);
	}

	PartitionRevisionNo getPartitionRoleRevision(PartitionId pId, TableType type) {
		util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
		return partitions_[pId].roles_[type].getRevisionNo();
	}

	void setOtherStatus(
			PartitionId pId, int32_t status, NodeId targetNode = 0) {
		util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
		currentPartitions_[pId].setOtherStatus(targetNode, status);
	};

	int32_t  getOtherStatus(PartitionId pId, NodeId targetNode) {
		util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
		return currentPartitions_[pId].getOtherStatus(targetNode);
	}

	void setErrorStatus(
			PartitionId pId, int32_t status, NodeId targetNode = 0) {
		util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
		currentPartitions_[pId].setErrorStatus(targetNode, status);
	};

	int32_t  getErrorStatus(PartitionId pId, NodeId targetNode) {
		util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
		return currentPartitions_[pId].getErrorStatus(targetNode);
	}

	void setPartitionChunkNum(
			PartitionId pId, int64_t chunkNum, NodeId targetNode = 0) {
		util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
		currentPartitions_[pId].setPartitionChunkCount(targetNode, chunkNum);
	};

	int64_t getPartitionChunkNum(PartitionId pId, NodeId targetNode) {
		util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
		return currentPartitions_[pId].getPartitionChunkCount(targetNode);
	}

	bool checkActivePartition(PartitionId pId, PartitionRoleStatus role, NodeId targetNode);

	void updatePartitionRole(PartitionId pId, TableType type = PT_CURRENT_OB);

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
			currentPartitions_[pId].setPartitionRoleStatus(SELF_NODEID, nextStatus);
			currentPartitions_[pId].setPartitionRevision(SELF_NODEID, role.getRevisionNo());
		}
	}
	
void setPartitionRoleWithCheck(
		PartitionId pId, PartitionRole &role, TableType type = PT_CURRENT_OB) {
		util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
		if (role.getRevisionNo() <= partitions_[pId].roles_[type].getRevisionNo()) {
			return;
		}
		partitions_[pId].roles_[type] = role;
		partitions_[pId].roles_[type].set(type);
		if (type == PT_CURRENT_OB) {
			PartitionRoleStatus nextStatus =
				partitions_[pId].roles_[type].getPartitionRoleStatus();
			currentPartitions_[pId].setPartitionRoleStatus(SELF_NODEID, nextStatus);
			currentPartitions_[pId].setPartitionRevision(SELF_NODEID, role.getRevisionNo());
		}
	}



	bool isRepairedPartition() {
		bool retVal = isRepaired_;
		isRepaired_ = false;
		return retVal;
	}

	void check(PartitionContext &context, bool balanceCheck, bool dropCheck, DropPartitionNodeInfo &dropNodeInfo);

	void setChangePartition(PartitionId pId = UNDEF_PARTITIONID);

	void resetBlockQueue() {
		util::LockGuard<util::WriteLock> lock(lock_);
		nextBlockQueue_.clear();
		goalBlockQueue_.clear();
	};

	void resetBlockQueue(ChangeTableType type) {
		util::LockGuard<util::WriteLock> lock(lock_);
		if (type == PT_CHANGE_NEXT_TABLE) {
		nextBlockQueue_.clear();
		}
		else {
		goalBlockQueue_.clear();
		}
	};

	void setBlockQueue(PartitionId pId,
		ChangeTableType tableType = PT_CHANGE_NEXT_TABLE,
		NodeId targetNode = UNDEF_NODEID);

	void setSubPartiton(PartitionContext &context,
			SubPartitionTable &subTable);

	bool findBlockQueue(PartitionId pId,
			ChangeTableType tableType = PT_CHANGE_NEXT_TABLE,
			NodeId targetNode = UNDEF_NODEID);

	CheckedPartitionStat checkPartitionStat(bool firstCheckSkip = true);

	CheckedPartitionStat getPartitionStat() {
		return loadInfo_.status_;
	}

	bool checkNextOwner(PartitionId pId, NodeId checkedNode, LogSequentialNumber checkedLsn,
			LogSequentialNumber checkedMaxLsn, NodeId currentOwner = UNDEF_NODEID);

	bool checkNextBackup(PartitionId pId, NodeId checkedNode, LogSequentialNumber checkedLsn,
			LogSequentialNumber currentOwnerStartLsn, LogSequentialNumber currentOwnerLsn);


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

	void setHeartbeatTimeout(NodeId nodeId, int64_t heartbeatTimeout = UNDEF_TTL) {
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

	void clear(PartitionId pId, TableType type) {
		util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
		partitions_[pId].clear(type);
	}

	void clearCurrentStatus();
	void clearRole(PartitionId pId);
	void set(SubPartition &subPartition);

	void getNodeAddressInfo(std::vector<AddressInfo> &addressInfoList) {
		int32_t nodeNum = getNodeNum();
		for (int32_t nodeId = 0; nodeId < nodeNum; nodeId++) {
			AddressInfo addressInfo;
			addressInfo.setNodeAddress(
				CLUSTER_SERVICE, getNodeInfo(nodeId).getNodeAddress(CLUSTER_SERVICE));
			addressInfo.setNodeAddress(
				SYNC_SERVICE, getNodeInfo(nodeId).getNodeAddress(SYNC_SERVICE));
			addressInfo.setNodeAddress(
				TRANSACTION_SERVICE, getNodeInfo(nodeId).getNodeAddress(TRANSACTION_SERVICE));
			if (getHeartbeatTimeout(nodeId) != UNDEF_TTL) {
				addressInfo.isActive_ = true;
			}
			else {
				addressInfo.isActive_ = false;
			}
			addressInfoList.push_back(addressInfo);
		}
	}

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

	void updatePartitionRevision(PartitionRevisionNo partitionSequentialNumber);

	void getChangeTargetPIdList(util::XArray<PartitionId> &pIdList) {
		util::LockGuard<util::WriteLock> lock(lock_);
		for (std::set<PartitionId>::iterator it = targetChangePartitonList_.begin();
			it != targetChangePartitonList_.end(); it++) {
			pIdList.push_back(*it);
		}
		targetChangePartitonList_.clear();
	}

	bool checkShorTermSyncTimeout(PartitionId pId, int64_t checkTime);

	int64_t getShorTermSyncTimeout(PartitionId pId) {
		return partitions_[pId].partitionInfo_.shortTermTimeout_;
		}

	void setShorTermSyncTimeout(PartitionId pId, int64_t timeout) {
		partitions_[pId].partitionInfo_.shortTermTimeout_ = timeout;
	}

	void clearChangePartition() {
		util::LockGuard<util::WriteLock> lock(lock_);
		targetChangePartitonList_.clear();
	}
	void setCatchupError(PartitionId pId) {
		if (currentCatchupPId_ == pId) {
		failCatchup_ = true;
	}
	}

	bool isCatchupError() {
		bool retVal = failCatchup_;
		failCatchup_ = false;
		return retVal;
	}

	NodeAddress &getNodeAddress(NodeId nodeId, ServiceType type = CLUSTER_SERVICE) {
		return getNodeInfo(nodeId).getNodeAddress(type);
	}

	bool checkSetService(NodeId nodeId, ServiceType type = CLUSTER_SERVICE) {
		return getNodeInfo(nodeId).check(type);
	}

	PartitionRevision &getPartitionRevision() {
		return revision_;
	}

	PartitionRevision &incPartitionRevision() {
		util::LockGuard<util::Mutex> lock(revisionLock_);
		if (revision_.sequentialNumber_ == INT64_MAX) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_CS_MAX_PARTITION_REIVISION,
					"Cluster partition sequence number is limit");
		}
		else {
			revision_.sequentialNumber_++;
		}
		return revision_;
	}

	PartitionRevisionNo getPartitionRevisionNo() {
		return revision_.sequentialNumber_;
	}

	bool checkPartitionRevision(PartitionRevisionNo revision, int32_t diff) {
		if (revision == (getPartitionRevisionNo() + diff)) {
			return true;
		}
		else {
			return false;
		}
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

	void getDownNodes(util::Set<NodeId> &downNodes, bool isClear = true) {
		util::LockGuard<util::Mutex> lock(nodeInfoLock_);
		for (std::set<NodeId>::iterator it = downNodeList_.begin(); it != downNodeList_.end(); it++) {
			downNodes.insert((*it));
		}
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

	void getAddNodes(util::Set<NodeId> &nodeIds) {
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

	bool isSet(NodeId nodeId) {
		if (nodeId >= nodeNum_) {
			return false;
		}
		return nodes_[nodeId].nodeBaseInfo_[CLUSTER_SERVICE + 1].check();
	}

	std::string dumpNodeAddress(
		NodeId nodeId, ServiceType type = CLUSTER_SERVICE) {
		if (nodeId >= nodeNum_ || nodeId < 0) {
			return std::string("");
		}
		return nodes_[nodeId].nodeBaseInfo_[type].address_.toString();
	}

	std::string dumpNodeSet(util::Set<NodeId> &nodeSet) {
		util::NormalOStringStream ss;
		int32_t listSize = static_cast<int32_t>(nodeSet.size());
		int32_t pos = 0;
		ss << "[";
		for (util::Set<NodeId>::iterator it = nodeSet.begin();
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
	std::string dumpPartitionsList(util::StackAllocator &alloc, TableType type);
	std::string dumpPartitionsNew(util::StackAllocator &alloc, TableType type);

void genPartitionColumnStr(
		util::NormalOStringStream &ss, std::string &roleName, PartitionId pId,
				NodeId nodeId, int32_t partitoinDigit, int32_t nodeIdDigit, TableType type);

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
		return std::string();
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
		return std::string();
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


template <typename T1, typename T2>
void getActiveNodeList(T1 &activeNodeList,  T2 &downNodeSet, int64_t currentTime);

private:
	struct PartitionInfo {
		PartitionInfo() : 
			shortTermTimeout_(UNDEF_TTL),
			pgId_(0),
			partitionRevision_(PartitionRevision::INITIAL_CLUSTER_REVISION),
			failed_(false),
			available_(true)
		{}
		void clear() {
			shortTermTimeout_ = UNDEF_TTL;
			partitionRevision_ = PartitionRevision::INITIAL_CLUSTER_REVISION;
			failed_ = false;
			available_ = true;
		}
		int64_t shortTermTimeout_;
		PartitionGroupId pgId_;
		PartitionRevisionNo partitionRevision_;
		bool failed_;
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

	public:

	struct CurrentPartitionInfo {
		CurrentPartitionInfo() : lsn_(0), startLsn_(0), status_(PT_OFF), roleStatus_(PT_NONE), 
			partitionRevision_(1), chunkCount_(0),
			notUpdateCount_(0), notLongtermUdatedCount_(0),
			otherStatus_(PT_OTHER_NORMAL), errorStatus_(PT_OTHER_NORMAL) {}
		LogSequentialNumber lsn_;
		LogSequentialNumber startLsn_;
		PartitionStatus status_;
		PartitionRoleStatus roleStatus_;
		PartitionRevisionNo partitionRevision_;
		int64_t chunkCount_;
		int32_t notUpdateCount_;
		int32_t notLongtermUdatedCount_;

		int32_t otherStatus_;
		int32_t errorStatus_;

		void clear() {
			startLsn_ = 0;
			status_ = PT_OFF;
			roleStatus_ = PT_NONE;
			partitionRevision_ = PartitionRevision::INITIAL_CLUSTER_REVISION;
			notUpdateCount_ = 0;
			chunkCount_ = 0;
			notLongtermUdatedCount_ = 0;
			otherStatus_ = PT_OTHER_NORMAL;
			errorStatus_ = PT_OTHER_NORMAL;
		}
	};

	void setPartitionInfos(PartitionId pId, CurrentPartitionInfo &info);

private:

	class CurrentPartitions {
	public:

		void init() {
			CurrentPartitionInfo first;
			currentPartitionInfos_.push_back(first);
		}

		void resize(int32_t nodeNum, bool withCheck) {
			if (withCheck) {
				if (currentPartitionInfos_.size() > static_cast<size_t>(nodeNum)) {
					return;
				}
			}
			currentPartitionInfos_.resize(nodeNum);
			}

		LogSequentialNumber getLsn(NodeId nodeId) {
			checkSize(nodeId);
			return currentPartitionInfos_[nodeId].lsn_;
			}

		void setLsn(NodeId nodeId, LogSequentialNumber lsn, bool updateCheck = false) {
			checkSize(nodeId);
			if (updateCheck) {
				if (currentPartitionInfos_[nodeId].lsn_ == lsn) {
					if (currentPartitionInfos_[nodeId].notUpdateCount_ != INT32_MAX) {
						currentPartitionInfos_[nodeId].notUpdateCount_++;
			}
		}
				else {
					currentPartitionInfos_[nodeId].notUpdateCount_ = 0;
				}
			}
			currentPartitionInfos_[nodeId].lsn_ = lsn;
		}

		LogSequentialNumber getStartLsn(NodeId nodeId) {
			checkSize(nodeId);
			return currentPartitionInfos_[nodeId].startLsn_;
			}

		void setStartLsn(NodeId nodeId, LogSequentialNumber startLsn) {
			checkSize(nodeId);
			currentPartitionInfos_[nodeId].startLsn_ = startLsn;
			}

		PartitionStatus getPartitionStatus(NodeId nodeId) {
			checkSize(nodeId);
			return currentPartitionInfos_[nodeId].status_;
		}

		void setPartitionStatus(NodeId nodeId, PartitionStatus status) {
			checkSize(nodeId);
			currentPartitionInfos_[nodeId].status_ = status;
			}

		PartitionRoleStatus getPartitionRoleStatus(NodeId nodeId) {
			checkSize(nodeId);
			return currentPartitionInfos_[nodeId].roleStatus_;
			}

		void setPartitionRoleStatus(NodeId nodeId, PartitionRoleStatus roleStatus) {
			checkSize(nodeId);
			currentPartitionInfos_[nodeId].roleStatus_ = roleStatus;
		}

		PartitionRevisionNo getPartitionRevision(NodeId nodeId) {
			checkSize(nodeId);
			return currentPartitionInfos_[nodeId].partitionRevision_;
			}

		void setPartitionRevision(NodeId nodeId, PartitionRevisionNo partitionRevision) {
			checkSize(nodeId);
			currentPartitionInfos_[nodeId].partitionRevision_ = partitionRevision;
			}

		int64_t getPartitionChunkCount(NodeId nodeId) {
			checkSize(nodeId);
			return currentPartitionInfos_[nodeId].chunkCount_;
		}

		void setPartitionChunkCount(NodeId nodeId, int64_t chunkCount) {
			checkSize(nodeId);
			currentPartitionInfos_[nodeId].chunkCount_ = chunkCount;
			}

		int32_t getOtherStatus(NodeId nodeId) {
			checkSize(nodeId);
			return currentPartitionInfos_[nodeId].otherStatus_;
			}

		void setErrorStatus(NodeId nodeId, int32_t status) {
			checkSize(nodeId);
			currentPartitionInfos_[nodeId].errorStatus_ = status;
				}

		int32_t getErrorStatus(NodeId nodeId) {
			checkSize(nodeId);
			int32_t ret = currentPartitionInfos_[nodeId].errorStatus_;
			currentPartitionInfos_[nodeId].errorStatus_ = 0;
			return ret;
			}

		void setOtherStatus(NodeId nodeId, int32_t status) {
			checkSize(nodeId);
			currentPartitionInfos_[nodeId].otherStatus_ = status;
			}

		int32_t getLsnChangeCount(NodeId nodeId) {
			checkSize(nodeId);
			return currentPartitionInfos_[nodeId].notUpdateCount_;
		}

		void resetPartitionInfo(NodeId nodeId) {
			checkSize(nodeId);
			currentPartitionInfos_[nodeId].roleStatus_ = PT_NONE;
			currentPartitionInfos_[nodeId].status_ = PT_OFF;
			}

	private:
		void checkSize(NodeId nodeId) {
			if (static_cast<size_t>(nodeId) >= currentPartitionInfos_.size()) {
				GS_THROW_USER_ERROR(GS_ERROR_PT_INTERAL,
						"Specifed nodeId is over limit, nodeId=" << nodeId
						<< ", current=" << currentPartitionInfos_.size());
			}
		}

		std::vector<CurrentPartitionInfo> currentPartitionInfos_;
	};

	class Partition {
	public:
		Partition() : roles_(NULL), alloc_(NULL) {}
		~Partition() {
			if (roles_) {
				for (int32_t pos = 0; pos < PT_TABLE_MAX; pos++) {
					ALLOC_DELETE(*alloc_, &roles_[pos]);
			}
			}
		}

		void init(util::StackAllocator &alloc, PartitionId pId,
				PartitionGroupId pgId, int32_t nodeLimit) {
			alloc_ = &alloc;
			roles_ = ALLOC_NEW(alloc) PartitionRole[PT_TABLE_MAX];
			for (int32_t pos = 0; pos < PT_TABLE_MAX; pos++) {
				roles_[pos].init(pId, static_cast<TableType>(pos));
			}
			partitionInfo_.pgId_ = pgId;
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
		}

		void check(NodeId nodeId);

		PartitionRole *roles_;
		PartitionInfo partitionInfo_;
	private:
		util::StackAllocator *alloc_;
	};

	struct ReplicaInfo {
		ReplicaInfo(PartitionId pId, uint32_t count)
			: pId_(pId), count_(count) {}
		PartitionId pId_;
		uint32_t count_;
	};

	struct DataNodeInfo {
		DataNodeInfo(NodeId nodeId, uint32_t count, uint32_t)
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

	class PartitionConfig {
	public:
		PartitionConfig(const ConfigTable &config, int32_t maxNodeNum);
		int32_t getLimitClusterNodeNum() {
			return limitClusterNodeNum_;
		}

		bool setLimitOwnerBackupLsnGap(int32_t gap) {
			if (gap < 0 || gap > INT32_MAX) {
				return false;
			}
			limitOwnerBackupLsnGap_ = gap;
			limitOwnerBackupLsnDetectErrorGap_ = limitOwnerBackupLsnGap_ * PT_BACKUP_GAP_FACTOR;
			limitOwnerBackupLsnDropGap_ = limitOwnerBackupLsnGap_ * PT_BACKUP_GAP_FACTOR;
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
		int32_t assignMode_;
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
		const DataNodeInfo &a, const DataNodeInfo &b) {
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

	template <class T>
	static void sortOrderList(util::Vector<T> &orderList,
		util::Vector<NodeId> &nodeList, util::Vector<uint32_t> &ownerLoadList,
		util::Vector<uint32_t> &dataLoadList, uint32_t &maxOwnerLoad,
		uint32_t &minOwnerLoad, uint32_t &maxDataLoad, uint32_t &minDataLoad,
		bool (*func)(const T &, const T &), bool isCatchup = false) {
		NodeId nodeId;
		minOwnerLoad = UINT32_MAX;
		maxOwnerLoad = 0;
		minDataLoad = UINT32_MAX;
		maxDataLoad = 0;
		uint32_t currentOwnerLoad, currentDataLoad, targetLoad;
		for (util::Vector<NodeId>::iterator nodeItr = nodeList.begin();
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

	bool checkOwnerBackupLsnGap(PartitionId pId, NodeId owner, NodeId backup) {
		bool isOwnerChanged = (getLsnChangeCount(pId, owner) == 0);
		bool isBackupNotChanged =
			(getLsnChangeCount(pId, backup) >= PT_LSN_NOTCHANGE_COUNT_MAX);
		return (checkLimitOwnerBackupLsnDetectErrorGap(getLSN(pId, owner), getLSN(pId, backup)) &&
				isOwnerChanged && isBackupNotChanged);
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

	NodeInfo &getNodeInfo(NodeId nodeId) {
		if (nodeId >= config_.maxNodeNum_ || nodeId == UNDEF_NODEID) {
			GS_THROW_USER_ERROR(GS_ERROR_CS_SERVICE_CLUSTER_INTERNAL_FAILED, "");
		}
		return nodes_[nodeId];
	}

	NodeId repairNotAvailablePartition(util::StackAllocator &alloc,
			PartitionId pId, LoadSummary &loadInfo, util::XArray<NodeId> &liveNodeList);

	void resetPrevCatchup(PartitionContext &context, PartitionId targetPId = UNDEF_PARTITIONID);

	util::FixedSizeAllocator<util::Mutex> fixedSizeAlloc_;
	util::StackAllocator alloc_;

	int32_t nodeNum_;
	NodeId masterNodeId_;
	PartitionRevision revision_;
	std::set<PartitionId> targetChangePartitonList_;
	std::vector<LogSequentialNumber> maxLsnList_;

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
	PartitionId prevCatchupPId_;
	PartitionId currentCatchupPId_;
	NodeId prevCatchupNodeId_;

	Partition *partitions_;
	CurrentPartitions *currentPartitions_;

	NodeInfo *nodes_;

	LoadSummary loadInfo_;

	int32_t partitionDigit_;
	int32_t lsnDigit_;

	bool failCatchup_;
	bool isRepaired_;
	PartitionRevisionNo currentRevisionNo_;
};

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
	SubPartition(PartitionId pId,
		PartitionRole &role, NodeAddress &currentOwner)
		: pId_(pId),
		role_(role),
		currentOwner_(currentOwner){};

	SubPartition(PartitionId pId,
		PartitionRole &role)
		: pId_(pId),
		role_(role){};

	void setCurrentOwner(PartitionTable *pt, NodeId currentOwner, NodeId nextOwner) {
		if (nextOwner == UNDEF_NODEID) {
			currentOwner_.clear();
		}
		else if (currentOwner == UNDEF_NODEID) {
			currentOwner_ = pt->getNodeAddress(nextOwner);
		}
		else {
				currentOwner_ = pt->getNodeAddress(currentOwner);
		}
	}

	MSGPACK_DEFINE(pId_, role_, currentOwner_);

	std::string dump(PartitionTable *pt = NULL) {
		util::NormalOStringStream ss;
		ss << "SubPartition:{"
		<< "pId=" << pId_;
		if (pt == NULL) {
			ss << ", role:" << role_.dump();
		}
		else {
			ss << ", role:" << role_.dump(pt);
		}
		ss << ", current owner:" << currentOwner_.toString() << "}";
		return ss.str();
	}


	PartitionId pId_;
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
	bool check() {
		return true;
	}

	int32_t size() {
		return static_cast<int32_t>(subPartitionList_.size());
	}
	SubPartition &getSubPartition(int32_t pos) {
		return subPartitionList_[pos];
	}
	void set(PartitionTable *pt, PartitionId pId,
		PartitionRole &role, NodeAddress &ownerAddress) {
		SubPartition subPartition(pId, role, ownerAddress);
		PartitionRole &tmpRole = subPartition.role_;
		tmpRole.encodeAddress(pt, role);
		subPartitionList_.push_back(subPartition);
	}

	void set(PartitionTable *pt, PartitionId pId, PartitionRole &role,
			NodeId ownerNodeId = UNDEF_NODEID);

	void setNodeAddress(AddressInfo &addressInfo) {
		nodeAddressList_.push_back(addressInfo);
	}

	std::vector<AddressInfo> &getNodeAddressList() {
		return nodeAddressList_;
	}

	void validate(PartitionTable *pt);


	PartitionRevision &getRevision() {
		return revision_;
	}

	MSGPACK_DEFINE(subPartitionList_, nodeAddressList_, revision_);

	std::string dump() {
		util::NormalOStringStream ss;
		ss << "subPartitionTable:{"
		<< "ptRev=" << revision_.toString();
		ss << ", [";
		ss << std::endl;
		for (size_t pos = 0; pos < subPartitionList_.size(); pos++) {
			ss << subPartitionList_[pos].dump();
			ss <<std::endl;
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
	case PartitionTable::PT_CURRENT_OB:
		return "CURRENT_OB";
	default:
		return "";
	}
}

static inline const std::string dumpPartitionTableType(uint8_t type) {
	TableType tableType = static_cast<TableType>(type);
	switch (tableType) {
	case PartitionTable::PT_NEXT_OB:
		return "NEXT_OB";
	case PartitionTable::PT_CURRENT_OB:
		return "CURRENT_OB";
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

static inline const std::string dumpPartitionRoleStatusEx(
	PartitionTable::PartitionRoleStatus status) {
	switch (status) {
	case PartitionTable::PT_OWNER:
		return "O";
	case PartitionTable::PT_BACKUP:
		return "B";
	case PartitionTable::PT_CATCHUP:
		return "C";
	case PartitionTable::PT_NONE:
		return "N";
	default:
		return "";
	}
}

#endif
