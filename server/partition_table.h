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
#include <unordered_set>
#include "msgpack_utils.h"
class ConfigTable;
class PartitionTable;
class EventEngine;
class KeepLogManager;
struct ManagerSet;

typedef int32_t NodeId;
typedef std::string RackZoneIdString;
typedef int32_t RackZoneId;
const int32_t UNDEF_SSL_PORT = -1;
const int32_t UNDEF_RACKZONEID = -1;

const int32_t UNDEF_NODEID = -1;
const int64_t UNDEF_TTL = 0;
const int64_t NOT_UNDEF_TTL = 1;
const NodeId SELF_NODEID = 0;

const int32_t PARTITION_ASSIGNMENT_RULE_DEFAULT = 0;
const int32_t PARTITION_ASSIGNMENT_RULE_ROUNDROBIN = 1;

const int32_t PARTITION_ASSIGNMENT_RULE_STABLE_GOAL = 2;
const int32_t PARTITION_ASSIGNMENT_RULE_PARTIAL_STABLE_GOAL = 3;
const int32_t PARTITION_ASSIGNMENT_RULE_COMMAND_SET_GOAL = 4;

static const int32_t OWNER_BACKUP_GAP_CLASSIC = 1000;
static const int32_t OWNER_BACKUP_GAP_DEFAULT = 1000;
static const int32_t OWNER_BACKUP_GAP_LARGE = 3000;

struct AddressInfo;
typedef std::vector<AddressInfo> AddressInfoList;

namespace picojson {
	class value;
}

/*!
	@brief Encodes node address
*/
struct NodeAddress {

	NodeAddress() : address_(0), port_(0) {}
	NodeAddress(AddressType address, uint16_t port)
		: address_(address), port_(port) {}
	NodeAddress(const char8_t* addr, uint16_t port);
	void set(const char8_t* addr, uint16_t port);
	void set(const util::SocketAddress& addr);

	void clear() {
		address_ = 0;
		port_ = 0;
	}

	bool isValid() {
		return (address_ != 0);
	}

	bool operator==(const NodeAddress& nodeAddress) const {
		return (
			memcmp(&address_, &(nodeAddress.address_),
				sizeof(address_)) == 0 && port_ == nodeAddress.port_);
	}

	bool operator < (const NodeAddress& nodeAddress) const {
		if (port_ < nodeAddress.port_) {
			return true;
		}
		else if (port_ > nodeAddress.port_) {
			return false;
		}
		return (memcmp(&address_, &(nodeAddress.address_), sizeof(address_)) < 0);
	}

	NodeAddress& operator=(const NodeAddress& nodeAddress) {
		address_ = nodeAddress.address_;
		port_ = nodeAddress.port_;
		return *this;
	}

	std::string dump(bool flag = true) const {
		return toString(flag);
	}

	std::string toString(bool isPort = true) const {
		util::NormalOStringStream ss;
		const util::SocketAddress::Inet *tmp =
				reinterpret_cast<const util::SocketAddress::Inet*>(&address_);
		ss << static_cast<int32_t>(tmp->value_[0]) << "." <<
				static_cast<int32_t>(tmp->value_[1]) << "." <<
				static_cast<int32_t>(tmp->value_[2]) << "." <<
				static_cast<int32_t>(tmp->value_[3]);
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
	AddressInfo(NodeAddress& clusterAddress, NodeAddress& txnAddress,
		NodeAddress& syncAddress, NodeAddress& systemAddress, NodeAddress& sqlAddress) :
		clusterAddress_(clusterAddress),
		transactionAddress_(txnAddress),
		syncAddress_(syncAddress),
		systemAddress_(systemAddress),
		sqlAddress_(sqlAddress),
		isActive_(false) {}

	NodeAddress& getNodeAddress(ServiceType type);
	void setNodeAddress(ServiceType type, NodeAddress& address);

	bool operator<(const AddressInfo& right) const {
		if (clusterAddress_.address_ < right.clusterAddress_.address_) {
			return true;
		}
		if (clusterAddress_.address_ > right.clusterAddress_.address_) {
			return false;
		}
		return (clusterAddress_.port_ < right.clusterAddress_.port_);
	}

	bool isValid() {
		return (transactionAddress_.isValid() && sqlAddress_.isValid());
	}

	NodeAddress clusterAddress_;
	NodeAddress transactionAddress_;
	NodeAddress syncAddress_;
	NodeAddress systemAddress_;
	NodeAddress sqlAddress_;

	NodeAddress dummy2_;
	bool isActive_;
	MSGPACK_DEFINE(
		clusterAddress_, transactionAddress_, syncAddress_,
		systemAddress_, sqlAddress_, dummy2_, isActive_);
};

struct PublicAddressInfoMessage {
public:

	PublicAddressInfoMessage() {}
	PublicAddressInfoMessage(AddressInfoList& addressInfoList) :
		addressInfoList_(addressInfoList) {
	}

	bool check() {
		return (addressInfoList_.size() > 0);
	}

	AddressInfoList& getPublicNodeAddressList() {
		return addressInfoList_;
	}
	MSGPACK_DEFINE(addressInfoList_);

private:
	AddressInfoList addressInfoList_;
};

/** **
	@brief ラックゾーン通信情報
	@note サブマスタが相手をフォローする際に、自分が保持している情報を送信
** **/
class RackZoneInfoList {
public:

	/** **
		@brief ラックゾーン情報
		@note ノードアドレスとラックゾーンID文字列のペア
	** **/
	class RackZoneInfo {
		friend class RackZoneInfoList;
		friend class PartitionTable;

	public:
		RackZoneInfo() {}
		RackZoneInfo(NodeAddress& address, std::string& rackZoneId) :
			address_(address), rackZoneId_(rackZoneId) {}

		bool check() {
			return true;
		}
		MSGPACK_DEFINE(address_, rackZoneId_);

	private:
		
		NodeAddress address_;
		std::string rackZoneId_;
	};

	RackZoneInfoList() {}

	void add(NodeAddress& address, std::string& rackZoneId) {
		rackZoneInfoList_.emplace_back(RackZoneInfo(address, rackZoneId));
	}

	bool check() {
		return (rackZoneInfoList_.size() > 0);
	}

	std::vector<RackZoneInfo>& get() {
		return rackZoneInfoList_;
	}

	MSGPACK_DEFINE(rackZoneInfoList_);

private:
	std::vector<RackZoneInfo> rackZoneInfoList_;
};

/** **
	@brief パーティションドロップ情報
	@note ノードごとにドロップ可能パーティションをまとめたもの
** **/
class DropNodeSet {
public:
	friend class DropPartitionNodeInfo;
	DropNodeSet() : address_(0), port_(0), pos_(0), nodeId_(0) {}

	void setAddress(NodeAddress& address) {
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

	NodeAddress& getNodeAddress() {
		return nodeAddress_;
	}

	std::string dump();

	MSGPACK_DEFINE(address_, port_, pIdList_, revisionList_);

	NodeAddress nodeAddress_;
	AddressType address_;
	uint16_t port_;
	int32_t pos_;
	NodeId nodeId_;
	PartitionIdList pIdList_;
	std::vector<PartitionRevisionNo> revisionList_;
};

/** **
	@brief パーティションドロップ通信情報
	@note マスタが定期的にチェックして発行
** **/
class  DropPartitionNodeInfo {

public:

	DropPartitionNodeInfo(util::StackAllocator& alloc) :
		alloc_(alloc), tmpDropNodeMap_(alloc) {}

	void set(PartitionId pId, NodeAddress& address,
		PartitionRevisionNo revision, NodeId nodeId) {
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
			target.pos_ = static_cast<int32_t>(dropNodeMap_.size());
			target.nodeId_ = nodeId;
			dropNodeMap_.emplace_back(target);
			tmpDropNodeMap_.emplace(address, target);
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

	DropNodeSet* get(size_t pos) {
		if (pos >= dropNodeMap_.size()) {
			return NULL;
		}
		return &dropNodeMap_[pos];
	}

	bool check() {
		return (dropNodeMap_.size() > 0);
	}
	std::string dump();

	MSGPACK_DEFINE(dropNodeMap_);

private:

	util::StackAllocator& alloc_;
	util::Map<NodeAddress, DropNodeSet> tmpDropNodeMap_;
	std::vector<DropNodeSet> dropNodeMap_;
};

struct SSLPair {
public:
	SSLPair() : sslPortNo_(UNDEF_SSL_PORT) {}
	SSLPair(NodeAddress& address, int32_t sslPortNo) : address_(address), sslPortNo_(sslPortNo) {}
	MSGPACK_DEFINE(address_, sslPortNo_);
	NodeAddress address_;
	int32_t sslPortNo_;
};

struct SSLAddressList {

public:
	SSLAddressList() {}

	bool check() {
		return (sslAddressList_.size() > 0);
	}
	
	void add(NodeAddress& address, int32_t sslPortNo) {
		sslAddressList_.push_back(SSLPair(address, sslPortNo));
	}

	std::vector<SSLPair> &getList() {
		return sslAddressList_;
	}

	void dump() {
		for (size_t pos = 0; pos < sslAddressList_.size(); pos++) {
			std::cout << sslAddressList_[pos].address_.toString() << "," << sslAddressList_[pos].sslPortNo_ << std::endl;
		}
	}

	MSGPACK_DEFINE(sslAddressList_);

private:
	std::vector<SSLPair> sslAddressList_;
};

/** **
	@brief パーティションテーブル
	@note パーティション操作およびノード情報を含むクラスタ情報の一部を管理
** **/
class PartitionTable {

	struct NodeInfo;
	struct PartitionInfo;
	class PartitionConfig;
	friend struct ScenarioCondition;

public:
	class SubPartition;

	enum CheckedPartitionSummaryStatus {
		PT_NORMAL,
		PT_NOT_BALANCE,
		PT_REPLICA_LOSS,
		PT_OWNER_LOSS,
		PT_ABNORMAL,
		PT_INITIAL
	};
	static const int32_t PT_ERROR_NORMAL = 0;
	static const int32_t PT_OTHER_NORMAL = 0;
	static const int32_t PT_ERROR_LONG_SYNC_FAIL = -2;
	static const int32_t PT_ERROR_SHORT_SYNC_FAIL = -1;

	static const int32_t MAX_NODE_NUM = 1000;

	static const int32_t PARTITION_RULE_UNDEF = -1;
	static const int32_t PARTITION_RULE_INITIAL = 1;
	static const int32_t PARTITION_RULE_ADD = 2;
	static const int32_t PARTITION_RULE_REMOVE = 3;
	static const int32_t PARTITION_RULE_SWAP = 4;

	static const int32_t RACKZONE_ID_STRING_MAX = 64;

	template<class X, class Y>
	static void arrayShuffle(X& ary, util::Random& random) {
		int32_t size = static_cast<int32_t>(ary.size());
		for (int32_t i = 0;i < size; i++) {
			int32_t j = random.nextInt32(size);
			Y t = ary[i];
			ary[i] = ary[j];
			ary[j] = t;
		}
	}

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
		PT_CURRENT_OB,
		PT_CURRENT_GOAL,
		PT_STABLE_GOAL,
		PT_TABLE_MAX = 3
	};
	static const int32_t PT_ROLE_TYPE_NUM = 1;

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
		@brief Represents the revision of each address
	*/
	/** **
		@brief パーティションリビジョン
		@note ハートビートごとに内部カウンタをインクリメント
	** **/
	class PartitionRevision {

	public:
		friend class PartitionRevisionMessage;

		static const PartitionRevisionNo INITIAL_CLUSTER_REVISION = 1;

		PartitionRevision() : addr_(0), port_(0), sequentialNumber_(INITIAL_CLUSTER_REVISION) {}
		PartitionRevision(AddressType addr, uint16_t port, int32_t sequentialNumber) :
			addr_(addr), port_(port), sequentialNumber_(sequentialNumber) {}

		void set(AddressType addr, uint16_t port) {
			addr_ = addr;
			port_ = port;
		}

		~PartitionRevision() {}

		bool operator==(const PartitionRevision& rev) const {
			return (memcmp(&addr_, &(rev.addr_), sizeof(addr_)) == 0 &&
				port_ == rev.port_ && sequentialNumber_ == rev.sequentialNumber_);
		}

		bool operator!=(const PartitionRevision& rev) const {
			int32_t addrDiff = memcmp(&addr_, &(rev.addr_), sizeof(addr_));
			int32_t portDiff = port_ - rev.port_;
			if (addrDiff == 0 && portDiff == 0) {
				return sequentialNumber_ != rev.sequentialNumber_;
			}
			else {
				return true;
			}
		}

		bool operator>(const PartitionRevision& rev) const {
			return sequentialNumber_ > rev.sequentialNumber_;
		}

		PartitionRevisionNo getRevisionNo() {
			return sequentialNumber_;
		}

		void increment() {
			sequentialNumber_++;
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

	private:
		AddressType addr_;
		uint16_t port_;
		PartitionRevisionNo sequentialNumber_;
	};

	/** **
		@brief パーティションロール
		@note ロールおよび対応するアドレスを管理するが、アドレスはノード間通信の際にのみセットする
	** **/
	class PartitionRole {
	public:

		PartitionRole() {
			init(UNDEF_PARTITIONID);
		}

		PartitionRole(PartitionId pId, PartitionRevision revision, TableType type) :
			pId_(pId), revision_(revision), type_(type), ownerNodeId_(UNDEF_NODEID),
			isSelfBackup_(false), isSelfCatchup_(false) {}

		void init(PartitionId pId) {
			pId_ = pId;
			ownerNodeId_ = UNDEF_NODEID;
			type_ = PT_CURRENT_OB;
			isSelfBackup_ = false;
			isSelfCatchup_ = false;
		}

		PartitionRevision& getRevision() {
			return revision_;
		}

		void setRevision(PartitionRevision& revision) {
			revision_ = revision;
		}

		PartitionRevisionNo getRevisionNo() {
			return revision_.getRevisionNo();
		}

		void swapRole(NodeId backupNodeId) {
			if (backupNodeId == ownerNodeId_) return;
			backups_.erase(std::remove(backups_.begin(), backups_.end(), backupNodeId));
			backups_.push_back(ownerNodeId_);
			ownerNodeId_ = backupNodeId;
			checkBackup();
		}

		void getBackups(util::XArray<NodeId>& backups) {
			getRoles(true, backups);
		}

		void getCatchups(util::XArray<NodeId>& catchups) {
			getRoles(false, catchups);
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

		NodeIdList& getBackups() {
			return backups_;
		}

		NodeIdList& getCatchups() {
			return catchups_;
		}

		int32_t getBackupSize() {
			return static_cast<int32_t>(backups_.size());
		}

		int32_t getRoledSize() {
			return ((ownerNodeId_ == UNDEF_NODEID) ? 0 : 1) + static_cast<int32_t>(backups_.size());
		}

		void clear() {
			ownerNodeId_ = UNDEF_NODEID;
			clearBackup();
			clearCatchup();
		}

		bool isOwnerOrBackup(NodeId nodeId = 0) {
			return (isOwner(nodeId) || isBackup(nodeId));
		}

		NodeId getOwner() {
			return ownerNodeId_;
		}

		void set(NodeId owner, NodeIdList& backups, NodeIdList& catchups) {
			ownerNodeId_ = owner;
			backups_ = backups;
			catchups_ = catchups;
			checkBackup();
			checkCatchup();
		}

		void setOwner(NodeId owner) {
			ownerNodeId_ = owner;
		}

		void set(PartitionRevision& revision) {
			revision_ = revision;
		}

		bool isOwner(NodeId targetNodeId = 0) {
			return (ownerNodeId_ == targetNodeId);
		}

		bool isBackup(NodeId targetNodeId = 0) {
			return isRole(true, targetNodeId);
		}

		bool isCatchup(NodeId targetNodeId = 0) {
			return isRole(false, targetNodeId);
		}

		void checkBackup() {
			checkRole(true);
		}

		void checkCatchup() {
			checkRole(false);
		}

		void promoteCatchup() {
			if (catchups_.size() == 0) return;
			isSelfCatchup_ = false;
			backupAddressList_.push_back(catchupAddressList_[0]);
			backups_.push_back(catchups_[0]);
			catchups_.clear();
			catchupAddressList_.clear();
			std::sort(backups_.begin(), backups_.end());
			if (backups_[0] == 0) {
				isSelfBackup_ = true;
			}
			else {
				isSelfBackup_ = false;
			}
		}

		void getRoleSet(std::set<NodeId>& roleSet) {
			if (ownerNodeId_ != UNDEF_NODEID) {
				roleSet.insert(ownerNodeId_);
				for (size_t pos = 0; pos < backups_.size(); pos++) {
					roleSet.insert(backups_[pos]);
				}
			}
		}

		void getRoleSet(PartitionTable* pt, std::set<NodeId>& roleSet) {
			if (ownerNodeId_ != UNDEF_NODEID) {
				if (pt->getHeartbeatTimeout(ownerNodeId_) != UNDEF_TTL) {
					roleSet.insert(ownerNodeId_);
				}
				for (size_t pos = 0; pos < backups_.size(); pos++) {
					if (pt->getHeartbeatTimeout(backups_[pos]) != UNDEF_TTL) {
						roleSet.insert(backups_[pos]);
					}
				}
			}
		}

		PartitionId getPartitionId() {
			return pId_;
		}

		bool operator==(PartitionRole& role) const;

		bool check() {
			return true;
		}

		void encodeAddress(PartitionTable* pt, PartitionRole& role);

		void get(NodeAddress& nodeAddress, std::vector<NodeAddress>& backupAddress) {
			nodeAddress = ownerAddress_;
			backupAddress = backupAddressList_;
		}

		void get(NodeAddress& nodeAddress, std::vector<NodeAddress>& backupAddress,
			std::vector<NodeAddress>& catchupAddress) {
			nodeAddress = ownerAddress_;
			backupAddress = backupAddressList_;
			catchupAddress = catchupAddressList_;
		}

		void getShortTermSyncTargetNodeId(util::Set<NodeId>& targetNodeSet) {
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
			else if (isCatchup()) {
				return PT_CATCHUP;
			}
			else {
				return PT_NONE;
			}
		}

		NodeAddress& getOwnerAddress() {
			return ownerAddress_;
		}

		std::vector<NodeAddress>& getBackupAddress() {
			return backupAddressList_;
		}

		std::vector<NodeAddress>& getCatchupAddress() {
			return catchupAddressList_;
		}

		bool hasCatchupRole() {
			return (catchups_.size() > 0);
		}

		std::string dump();
		std::string dumpIds();
		std::string dump(PartitionTable* pt, ServiceType type = CLUSTER_SERVICE);

		MSGPACK_DEFINE(pId_, ownerAddress_,
			backupAddressList_, catchupAddressList_, revision_);

	private:

		void getRoles(bool isBackup, util::XArray<NodeId>& backups) {
			NodeIdList& roles = isBackup ? backups_ : catchups_;
			size_t size = roles.size();
			if (size == 0) return;
			backups.resize(size);
			memcpy(backups.data(), reinterpret_cast<uint8_t*>(&roles[0]), size * sizeof(NodeId));
		}

		bool isRole(bool isBackup, NodeId targetNodeId = 0) {
			NodeIdList& roles = isBackup ? backups_ : catchups_;
			bool& isSelf = isBackup ? isSelfBackup_ : isSelfCatchup_;

			if (roles.size() == 0) return false;
			if (targetNodeId == 0) {
				return isSelf;
			}
			if (roles.size() == 1) {
				return (roles[0] == targetNodeId);
			}
			NodeIdList::iterator it;
			it = std::find(roles.begin(), roles.end(), targetNodeId);
			if (it != roles.end()) {
				return true;
			}
			else {
				return false;
			}
		}

		void checkRole(bool isBackup) {
			NodeIdList& roles = isBackup ? backups_ : catchups_;
			bool& isSelf = isBackup ? isSelfBackup_ : isSelfCatchup_;
			if (roles.size() == 1) {
				if (roles[0] == 0) {
					isSelf = true;
				}
			}
			else if (roles.size() >= 2) {
				assert(isBackup);
				std::sort(roles.begin(), roles.end());
				if (roles[0] == 0) {
					isSelf = true;
				}
				else {
					isSelf = false;
				}
			}
			else {
				isSelf = false;
			}
		}

		void clearAddressList(PartitionTable* pt, PartitionRole& role, bool isBackup);
		PartitionId pId_;
		PartitionRevision revision_;
		NodeAddress ownerAddress_;
		std::vector<NodeAddress> backupAddressList_;
		TableType type_;
		NodeId ownerNodeId_;
		NodeIdList backups_;
		bool isSelfBackup_;
		std::vector<NodeAddress> catchupAddressList_;
		NodeIdList catchups_;
		bool isSelfCatchup_;
	};

	/** **
		@brief サブパーティション
		@note パーティション単位でロールを切り出したもの
	** **/
	class SubPartition {
	public:
		SubPartition() : pId_(UNDEF_PARTITIONID), currentOwnerId_(UNDEF_NODEID) {}
		SubPartition(PartitionId pId, PartitionRole& role, NodeAddress& currentOwner) :
			pId_(pId), role_(role), currentOwner_(currentOwner), currentOwnerId_(UNDEF_NODEID) {};

		MSGPACK_DEFINE(pId_, role_, currentOwner_);

		std::string dump(PartitionTable* pt = NULL) {
			util::NormalOStringStream ss;
			ss << "Partition:{" << "pId=" << pId_;
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
		NodeId currentOwnerId_;
	};

	/** **
		@brief サブパーティションテーブル
		@note ノード間通信、パーティション部分情報切り出しのため
	** **/
	class SubPartitionTable {
	public:
		SubPartitionTable() : partitionNum_(1) {}

		int32_t size() {
			return static_cast<int32_t>(subPartitionList_.size());
		}

		SubPartition& getSubPartition(int32_t pos) {
			return subPartitionList_[pos];
		}

		void init(PartitionTable* pt, EventEngine* ee);

		void prepareMap(util::Map<PartitionId, PartitionRole*>& roleMap) {
			for (size_t pos = 0; pos < subPartitionList_.size(); pos++) {
				roleMap[subPartitionList_[pos].pId_] = &subPartitionList_[pos].role_;
			}
		}

		void set(PartitionTable* pt, PartitionId pId, PartitionRole& role, NodeAddress& ownerAddress) {
			SubPartition subPartition(pId, role, ownerAddress);
			PartitionRole& tmpRole = subPartition.role_;
			tmpRole.encodeAddress(pt, role);
			subPartitionList_.push_back(subPartition);
		}

		void set(PartitionTable* pt, PartitionId pId, PartitionRole& role,
			NodeId ownerNodeId = UNDEF_NODEID);

		void setNodeAddress(AddressInfo& addressInfo) {
			nodeAddressList_.push_back(addressInfo);
		}

		AddressInfoList& getNodeAddressList() {
			return nodeAddressList_;
		}

		void setPublicNodeAddressList(AddressInfoList& publicNodeAddressList) {
			publicNodeAddressList_ = publicNodeAddressList;
		}

		AddressInfoList& getPublicNodeAddressList() {
			return publicNodeAddressList_;
		}

		void set(PartitionTable* pt, TableType  tableType =  PartitionTable::PT_CURRENT_OB) {
			for (PartitionId pId = 0; pId < pt->getPartitionNum(); pId++) {
				PartitionRole tmpRole;
				pt->getPartitionRole(pId, tmpRole, tableType);
				set(pt, pId, tmpRole);
			}
		}

		bool check() {
			return true;
		}

		PartitionRevision& getRevision() {
			return revision_;
		}

		void clear() {
			subPartitionList_.clear();
		}

		void setRevision(PartitionRevision& revision) {
			revision_ = revision;
		}

		MSGPACK_DEFINE(subPartitionList_, nodeAddressList_, revision_);

		std::string dump(bool isTrace = true) {
			util::NormalOStringStream ss;
			ss << "NextPartitionTable:{";
			ss << "revision=" << revision_.getRevisionNo();
			ss << ", [";
			if (!isTrace) {
				ss << std::endl;
			}
			for (size_t pos = 0; pos < subPartitionList_.size(); pos++) {
				ss << subPartitionList_[pos].dump();
				if (!isTrace) {
					ss << std::endl;
				}
				if (pos != subPartitionList_.size() - 1) {
					ss << ",";
				}
			}
			ss << "]}";
			return ss.str();
		}
	private:
		std::vector<SubPartition> subPartitionList_;
		AddressInfoList nodeAddressList_;
		PartitionRevision revision_;
		uint32_t partitionNum_;
		AddressInfoList publicNodeAddressList_;
	};

	struct NodesStat {
		NodesStat(util::StackAllocator& alloc) : ownerCountList_(alloc), dataCountList_(alloc),
			ownerRackZoneCountList_(alloc), dataRackZoneCountList_(alloc),
			nodeRackZoneCountList_(alloc), rackZoneIdList_(alloc) {}

		static const int32_t AVERAGE = 0;
		static const int32_t VARIANCE = 1;
		static const int32_t STANDARD_DEVIATION  = 2;
		static const int32_t TYPE_MAX = 3;

		static const int32_t OWNER_COUNT = 0;
		static const int32_t DATA_COUNT = 1;
		static const int32_t OWNER_RACKZONE_COUNT = 2;
		static const int32_t DATA_RACKZONE_COUNT = 3;
		static const int32_t RACKZONE_NODE_COUNT = 4;
		static const int32_t RACKZONE_ID_LIST = 5;
		static const int32_t TARGET_MAX = 6;

		void dumpHeader(util::NormalOStringStream &oss, int32_t type, int32_t target);

		void dumpTypeName(util::NormalOStringStream& oss, int32_t type);
		void dumpTargetName(util::NormalOStringStream& oss, int32_t target);
		util::Vector<int32_t>& getTargetList(int32_t target);
		double calc(int32_t statType, int32_t target);
		void filter(util::Vector<int32_t> &tmpList, int32_t type);
		bool check(util::StackAllocator& alloc, double ratio);
		std::string dump();

		util::Vector<int32_t> ownerCountList_;
		util::Vector<int32_t> dataCountList_;
		util::Vector<int32_t> ownerRackZoneCountList_;
		util::Vector<int32_t> dataRackZoneCountList_;
		util::Vector<int32_t> nodeRackZoneCountList_;
		util::Vector<RackZoneId> rackZoneIdList_;
	};

	/** **
		@brief パーティションコンテキスト
		@note パーティション操作に必要な情報群
	** **/
	struct PartitionContext {

		PartitionContext(util::StackAllocator& eventStackAlloc, PartitionTable* pt,
			bool& needUpdatePartition, SubPartitionTable& subPartitionTable,
			DropPartitionNodeInfo& dropNodeInfo) :
			eventStackAlloc_(&eventStackAlloc),
			needUpdatePartition_(needUpdatePartition),
			currentRuleLimitTime_(UNDEF_TTL),
			pt_(pt),
			isRepairPartition_(false),
			subPartitionTable_(subPartitionTable),
			dropNodeInfo_(dropNodeInfo),
			isGoalPartition_(false),
			isLoadBalance_(true),
			isAutoGoal_(true),
			currentTime_(0),
			isConfigurationChange_(false),
			backupNum_(0),
			 stablePartition_(false)
		{}

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

		void setStablePartition() {
			stablePartition_ = true;
		}

		bool getStablePartition() {
			return stablePartition_;
		}

		util::StackAllocator& getAllocator() {
			return *eventStackAlloc_;
		}

		util::StackAllocator* eventStackAlloc_;
		bool& needUpdatePartition_;
		int64_t currentRuleLimitTime_;
		PartitionTable* pt_;
		bool isRepairPartition_;
		SubPartitionTable& subPartitionTable_;
		DropPartitionNodeInfo& dropNodeInfo_;
		bool isGoalPartition_;
		bool isLoadBalance_;
		bool isAutoGoal_;

		int64_t currentTime_;
		bool isConfigurationChange_;
		int32_t backupNum_;
		bool stablePartition_;
	};

	/** **
		@brief ゴールパーティション管理クラス
		@note ロール集合の管理。排他必要
	** **/
	class GoalPartition {
		friend class PartitionTable;
	public:

		GoalPartition(const char* goalFileDir, const char* goalFileName);

		void init(PartitionTable* pt) {
			util::LockGuard<util::Mutex> guard(lock_);
			partitionNum_ = pt->getPartitionNum();
			for (PartitionId pId = 0; pId < pt->getPartitionNum(); pId++) {
				PartitionRole role(pId, pt->getPartitionRevision(), PT_CURRENT_OB);
				role.encodeAddress(pt, role);
				role.checkBackup();
				role.checkCatchup();
				goalPartitions_.emplace_back(role);
			}
		}

		void set(PartitionTable* pt, util::Vector<PartitionRole>& goalList) {

			if (goalList.size() != partitionNum_) {
				GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
			}
			util::LockGuard<util::Mutex> guard(lock_);
			goalPartitions_.clear();
			for (size_t pos = 0; pos < goalList.size(); pos++) {
				if (goalList[pos].getPartitionId() != pos) {
					GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
				}
				goalList[pos].encodeAddress(pt, goalList[pos]);
				goalList[pos].checkBackup();
				goalList[pos].checkCatchup();
				goalPartitions_.emplace_back(goalList[pos]);
			}
		}

		void setPartitionRole(PartitionRole& role) {
			util::LockGuard<util::Mutex> guard(lock_);
			goalPartitions_.emplace_back(role);
		}

		PartitionRole& getPartitionRole(PartitionId pId) {
			if (pId >= partitionNum_) {
				GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
			}
			util::LockGuard<util::Mutex> guard(lock_);
			return goalPartitions_[pId];
		}

		size_t size() {
			util::LockGuard<util::Mutex> guard(lock_);
			return goalPartitions_.size();
		}

		void update(PartitionRole& role) {
			util::LockGuard<util::Mutex> guard(lock_);
			updateMain(role);
		}

		void clear() {
			util::LockGuard<util::Mutex> guard(lock_);
			for (PartitionId pId = 0; pId < goalPartitions_.size(); pId++) {
				goalPartitions_[pId].clear();
			}
			goalData_.clear();
			isValidateFormat_ = false;
			hashValue_ = -1;
		}

		void updateMain(PartitionRole& role) {
			PartitionId rolePId = role.getPartitionId();
			if (goalPartitions_.size() <= rolePId) {
				return;
			}
			role.checkBackup();
			role.checkCatchup();
			goalPartitions_[rolePId] = role;
		}

		void copy(util::Vector<PartitionRole>& roleList) {
			util::LockGuard<util::Mutex> guard(lock_);
			for (size_t pos = 0; pos < goalPartitions_.size(); pos++) {
				roleList.emplace_back(goalPartitions_[pos]);
			}
		}

		void updateAll(util::Vector<PartitionRole>& roleList) {
			util::LockGuard<util::Mutex> guard(lock_);
			for (size_t pos = 0; pos < roleList.size(); pos++) {
				updateMain(roleList[pos]);
			}
		}

		std::string& getGoalData() {
			return goalData_;
		}

		int64_t getGoalHashValue() {
			return hashValue_;
		}

		void setGoalData(std::string& goalData) {
			goalData_ = goalData;
			isValidateFormat_ = true;
		}

		std::string& getGoalFilaName() {
			return goalFileName_;
		}

		bool isEmpty() {
			return goalData_.empty();
		}

		bool isValidateFormat() {
			return isValidateFormat_;
		}

		bool isEnableInitialGenerate() {
			return enableInitialGenerate_;
		}

		void setInitialGenerate(bool enable) {
			enableInitialGenerate_ = enable;
		}

		virtual bool load();
		virtual bool vaildateFormat() { return true; };

	protected:

		std::vector<PartitionRole> goalPartitions_;
		uint32_t partitionNum_;
		util::Mutex lock_;
		std::string goalFileName_;
		std::string goalData_;
		bool isValidateFormat_;
		bool enableInitialGenerate_;
		int64_t hashValue_;
	};

	class StableGoalPartition : public GoalPartition {
		friend class PartitionTable;
	public:
		static const size_t MAX_OPEN_TRIAL_MAX = 10;
		static const size_t MAX_CALC_HASH_SIZE = 1024*1024;
		StableGoalPartition(const ConfigTable& config, const char* configDir);
		bool isEnable() {
			return enable_; 
		}

		bool isSucceedOnError();

		void setAssigned() {
			isAssigned_ = true;
		}
		
		bool isAssigned() {
			bool ret = isAssigned_;
			isAssigned_ = false;
			return ret;
		}

		bool validateFormat(picojson::value& value, bool calcCheckSum = false);
		bool writeFile();
		bool load();
		void writeStableGoal(std::string& goal);
		void setCheckSum();

		uint32_t calcCheckSum();

		std::string getPolicyName() {
			switch (policy_) {
			case 0: return ("SET_ASSIGNMENT_RULE");
			case 1: return ("PARTIAL_STABLE_GOAL");
			default: return ("UNDEF");
			}
		};

		int32_t getPolicy() {
			return policy_;
		}

		void setPolicy(int32_t policy) {
			policy_ = policy;
		}

		void setEnable(bool enable) {
			enable_ = enable;
		}


		void initialCheck();

	private:

		bool enable_;
		int32_t policy_;
		const char* configDir_;
		bool isAssigned_;
	};


	PartitionTable(const ConfigTable& configTable, const char* confPath);

	~PartitionTable();

	void initialize(ManagerSet* mgrSet);

	bool enableParallelSync() {
		return (parallelSyncNum_ > 1);
	}

	bool enableParallelChunkSync() {
		return (parallelChunkSyncNum_ > 0);
	}

	void copyGoal(util::Vector<PartitionRole>& roleList, TableType target = PT_CURRENT_GOAL) {
		switch (target) {
		case PT_CURRENT_GOAL:
			goal_.copy(roleList);
			break;
		case PT_STABLE_GOAL:
			stableGoal_.copy(roleList);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
		}
	}

	void setGoal(PartitionTable* pt, util::Vector<PartitionRole>& goalList, TableType target = PT_CURRENT_GOAL) {
		switch (target) {
		case PT_CURRENT_GOAL:
			goal_.set(pt, goalList);
			break;
		case PT_STABLE_GOAL:
			stableGoal_.set(pt, goalList);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
		}
	}

	void updateGoal(PartitionRole& role, TableType target = PT_CURRENT_GOAL) {
		switch (target) {
		case PT_CURRENT_GOAL:
			goal_.update(role);
			break;
		case PT_STABLE_GOAL:
			stableGoal_.update(role);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
		}
	}

	void updateStableGoal(util::StackAllocator& alloc) {
		util::Vector<PartitionRole> goalList(alloc);
		copyGoal(goalList, PT_CURRENT_GOAL);
		stableGoal_.updateAll(goalList);
	}

	void writeStableGoal(util::StackAllocator& alloc);
	
	void getGoalPartitions(util::StackAllocator& alloc, const  ServiceTypeInfo& addressType,
		picojson::value& result, TableType target);
	
	bool setGoalPartitions(util::StackAllocator& alloc, const picojson::value* paramValue, TableType tableType = PT_CURRENT_GOAL,  bool checkNode = true);
	StableGoalPartition& getStableGoal() {
		return stableGoal_;
	}

	NodeId getOwner(PartitionId pId);

	bool isOwner(PartitionId pId, NodeId targetNodeDescriptor = 0);

	bool isOwnerOrBackup(PartitionId pId, NodeId targetNodeDescriptor = 0);

	void getBackup(PartitionId pId, util::XArray<NodeId>& backups);

	void getCatchup(PartitionId pId, util::XArray<NodeId>& catchups);

	NodeId getCatchup(PartitionId pId);

	bool isBackup(PartitionId pId, NodeId targetNodeId = 0);

	bool isCatchup(PartitionId pId, NodeId targetNodeId = 0);

	void setPrevCatchup(PartitionId pId, NodeId nodeId) {
		prevCatchupPId_ = pId;
		prevCatchupNodeId_ = nodeId;
		PartitionGroupId pgId = pgConfig_.getPartitionGroupId(pId);
		if (parallelCatchupList_.size() > pgId) {
			parallelCatchupList_[pgId].reset();
		}
	}

	void resetCatchup() {
		if (prevCatchupPId_ != UNDEF_PARTITIONID) {
			PartitionGroupId pgId = pgConfig_.getPartitionGroupId(prevCatchupPId_);
			if (parallelCatchupList_.size() > pgId) {
				parallelCatchupList_[pgId].reset();
			}
		}
		prevCatchupPId_ = UNDEF_PARTITIONID;
		prevCatchupNodeId_ = UNDEF_NODEID;
	}

	void resetCatchup(PartitionId pId) {
		if (pId < 0 || pId >= getPartitionNum()) {
			return;
		}
		PartitionGroupId pgId = pgConfig_.getPartitionGroupId(pId);
		if (parallelCatchupList_.size() > pgId) {
			parallelCatchupList_[pgId].reset();
		}
	}

	size_t getBackupSize(util::StackAllocator& alloc, PartitionId pId) {
		util::XArray<NodeId> backups(alloc);
		getBackup(pId, backups);
		return backups.size();
	}

	bool hasPublicAddress() {
		return hasPublicAddress_;
	}

	void setPublicAddress() {
		hasPublicAddress_ = true;
	}

	int32_t getSSLPortNo(NodeId nodeId) {
		return getNodeInfo(nodeId).sslPort_;
	}

	void setSSLPortNo(NodeId nodeId, int32_t sslPort) {
		getNodeInfo(nodeId).sslPort_ = sslPort;
	}

	bool isUseSSL() {
		return (getSSLPortNo(0) != UNDEF_SSL_PORT);
	}

	void getPublicNodeAddress(AddressInfoList& addressInfoList) {
		util::LockGuard<util::Mutex> lock(nodeInfoLock_);
		for (NodeId nodeId = 0; nodeId < getNodeNum(); nodeId++) {
			addressInfoList.emplace_back(nodes_[nodeId].publicAddressInfo_);
		}
	}
	void setImmediateCheck(bool flag) {
		immediateCheck_ = flag;
	}

	void getRackZoneInfoList(RackZoneInfoList& rackZoneInfoList);

	void setRackZoneInfoList(util::StackAllocator& alloc, RackZoneInfoList& rackZoneInfoList);

	void setRackZoneId(const char* rackZoneIdName, NodeId nodeId = 0);
	void setStandbyMode(bool standby, NodeId nodeId);
	bool getStandbyMode(NodeId nodeId);

	void setStartTime(int64_t startTime, NodeId nodeId) {
		if (nodeId >= MAX_NODE_NUM) {
			return;
		}
		nodes_[nodeId].startTime_ = startTime;
	}

	int64_t getStartTime(NodeId nodeId) {
		if (nodeId >= MAX_NODE_NUM) {
			return -1;
		}
		return nodes_[nodeId].startTime_;
	}

	bool needPartitionCheck() {
		bool check = needCheck_;
		needCheck_ = false;
		return check;
	}

	void setPartitionCheck() {
		needCheck_ = true;
	}

	void getRackZoneIdList(util::Vector<int32_t>& rackZoneIdList);

	template<typename T>
	void getRackZoneIdName(T& rackZoneIdName, NodeId nodeId);

	bool isEnableRackZoneAwareness() {
		return enableRackZoneAwareness_;
	}

	bool isOrgEnableRackZoneAwareness() {
		return orgEnableRackZoneAwareness_;
	}

	void resetDownNode(NodeId targetNodeId);

	bool checkConfigurationChange(util::StackAllocator& alloc,
		PartitionContext& context, bool checkFlag);

	LogSequentialNumber getLSN(PartitionId pId, NodeId targetNodeId = 0) {
		if (pId >= config_.partitionNum_) {
			GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
		}
		util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
		return currentPartitions_[pId].getLsn(targetNodeId);
	}

	LogSequentialNumber getStartLSN(PartitionId pId, NodeId targetNodeId = 0) {
		if (pId >= config_.partitionNum_) {
			GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
		}
		util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
		return currentPartitions_[pId].getStartLsn(targetNodeId);
	}

	void resizeCurrenPartition(int32_t resizeNum);

	LogSequentialNumber getMaxLsn(PartitionId pId) {
		if (pId >= config_.partitionNum_) {
			GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
		}
		return maxLsnList_[pId];
	}

	void setMaxLsn(PartitionId pId, LogSequentialNumber lsn) {
		if (pId >= config_.partitionNum_) {
			GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
		}
		if (lsn > maxLsnList_[pId]) {
			maxLsnList_[pId] = lsn;
		}
	}

	void setRepairedMaxLsn(PartitionId pId, LogSequentialNumber lsn) {
		if (pId >= config_.partitionNum_) {
			GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
		}
		maxLsnList_[pId] = lsn;
	}

	bool setNodeInfo(NodeId nodeId, ServiceType type, NodeAddress& address);

	void setMaxLsnList(LsnList& maxLsnList) {
		maxLsnList = maxLsnList_;
	}

	void changeClusterStatus(ClusterStatus status, NodeId masterNodeId,
		int64_t currentTime, int64_t nextHeartbeatTimeout);

	PartitionRevisionNo getCurrentRevisionNo() {
		return currentRevisionNo_;
	}

	void planNext(PartitionContext& context);
	bool checkPeriodicDrop(util::StackAllocator& alloc, DropPartitionNodeInfo& dropInfo);
	void planGoal(PartitionContext& context);
	void assignDefaultGoal(util::StackAllocator& alloc);
	void assignRoundRobinGoal(util::StackAllocator& alloc);
	bool assignSucceedStableGoal(util::StackAllocator& alloc);

	std::string dumpPartitionRule(int32_t ruleNo) {
		switch (ruleNo) {
		case PartitionTable::PARTITION_RULE_INITIAL: return "Initial";
		case PartitionTable::PARTITION_RULE_ADD: return "Add";
		case PartitionTable::PARTITION_RULE_REMOVE: return "Remove";
		case PartitionTable::PARTITION_RULE_SWAP: return "Swap";
		default: return "None";
		}
	};

	int32_t getRuleNo() {
		return currentRuleNo_;
	}

	int64_t getRuleLimitTime() {
		return nextApplyRuleLimitTime_;
	}

	bool isAvailable(PartitionId pId) {
		if (pId >= config_.partitionNum_) {
			GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
		}
		return partitions_[pId].partitionInfo_.available_;
	}

	void setLSN(PartitionId pId, LogSequentialNumber lsn, NodeId targetNodeId = 0) {
		if (pId >= config_.partitionNum_) {
			GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
		}
		util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
		currentPartitions_[pId].setLsn(targetNodeId, lsn);
	}

	void setStartLSN(PartitionId pId, LogSequentialNumber lsn, NodeId targetNodeId = 0) {
		if (pId >= config_.partitionNum_) {
			GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
		}
		util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
		currentPartitions_[pId].setStartLsn(targetNodeId, lsn);
	}

	void setLsnWithCheck(PartitionId pId, LogSequentialNumber lsn, NodeId targetNodeId = 0) {
		if (pId >= config_.partitionNum_) {
			GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
		}
		util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
		currentPartitions_[pId].setLsn(targetNodeId, lsn, true);
	}

	PartitionStatus getPartitionStatus(PartitionId pId, NodeId targetNode = 0) {
		if (pId >= config_.partitionNum_) {
			GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
		}
		util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
		return currentPartitions_[pId].getPartitionStatus(targetNode);
	}

	PartitionRoleStatus getPartitionRoleStatus(PartitionId pId, NodeId targetNode = 0) {
		if (pId >= config_.partitionNum_) {
			GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
		}
		util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
		return currentPartitions_[pId].getPartitionRoleStatus(targetNode);
	}

	void getPartitionRoleStatus(PartitionId pId, PartitionRoleStatus& prevStatus, PartitionRoleStatus& curretStatus, NodeId targetNode = 0) {
		if (pId >= config_.partitionNum_) {
			GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
		}
		util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
		prevStatus  = currentPartitions_[pId].getPartitionPrevRoleStatus(targetNode);
		curretStatus = currentPartitions_[pId].getPartitionRoleStatus(targetNode);
	}

	void setPartitionStatus(PartitionId pId, PartitionStatus status, NodeId targetNode = 0) {
		if (pId >= config_.partitionNum_) {
			GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
		}
		util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
		return currentPartitions_[pId].setPartitionStatus(targetNode, status);
	}

	void setPartitionRoleStatus(PartitionId pId, PartitionRoleStatus status, NodeId targetNode = 0) {
		if (pId >= config_.partitionNum_) {
			GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
		}
		util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
		currentPartitions_[pId].setPartitionRoleStatus(targetNode, status);
	};

	void setPartitionRevision(PartitionId pId, PartitionRevisionNo partitionRevision, NodeId targetNode = 0) {
		if (pId >= config_.partitionNum_) {
			GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
		}
		util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
		currentPartitions_[pId].setPartitionRevision(targetNode, partitionRevision);
	};

	PartitionRevisionNo getPartitionRevision(PartitionId pId, NodeId targetNode) {
		if (pId >= config_.partitionNum_) {
			GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
		}
		util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
		return currentPartitions_[pId].getPartitionRevision(targetNode);
	}

	void getPartitionRole(PartitionId pId, PartitionRole& role, TableType type = PT_CURRENT_OB) {
		if (pId >= config_.partitionNum_) {
			GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
		}
		switch (type) {
		case PT_CURRENT_OB: {
			util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
			role = partitions_[pId].roles_[type];
			break;
		}
		case PT_CURRENT_GOAL: {
			role = goal_.getPartitionRole(pId);
			break;
		}
		case PT_STABLE_GOAL: {
			role = stableGoal_.getPartitionRole(pId);
			break;
		}
		default:
			GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");		
		}
	}

	void setPartitionRole(PartitionId pId, PartitionRole& role, TableType type = PT_CURRENT_OB) {
		if (pId >= config_.partitionNum_) {
			GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
		}
		switch (type) {
		case PT_CURRENT_OB: {
			util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
			partitions_[pId].roles_[type] = role;
			PartitionRoleStatus nextStatus = partitions_[pId].roles_[type].getPartitionRoleStatus();
			currentPartitions_[pId].setPartitionRoleStatus(SELF_NODEID, nextStatus);
			currentPartitions_[pId].setPartitionRevision(SELF_NODEID, role.getRevisionNo());
			break;
		}
		case PT_CURRENT_GOAL: {
			goal_.update(role);
			break;
		}
		case PT_STABLE_GOAL: {
			goal_.update(role);
			break;
		}
		default:
			GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");		
		}
	}

	void setErrorStatus(PartitionId pId, int32_t status,NodeId targetNode = 0) {
		util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
		currentPartitions_[pId].setErrorStatus(targetNode, status);
	};

	int32_t  getErrorStatus(PartitionId pId, NodeId targetNode) {
		util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
		return currentPartitions_[pId].getErrorStatus(targetNode);
	}

	void handleErrorStatus(PartitionId pId, NodeId targetNode, int32_t status);

	bool check(PartitionContext& context);

	void setBlockQueue(PartitionId pId, ChangeTableType tableType = PT_CHANGE_NEXT_TABLE,
		NodeId targetNode = UNDEF_NODEID);

	void resetBlockQueue() {
		util::LockGuard<util::WriteLock> lock(lock_);
		nextBlockQueue_.clear();
		goalBlockQueue_.clear();
	};

	CheckedPartitionSummaryStatus checkPartitionSummaryStatus(
		util::StackAllocator& alloc, bool isBalanceCheck, bool firstSkip);

	CheckedPartitionSummaryStatus getPartitionSummaryStatus() {
		return loadInfo_.status_;
	}

	void setPartitionSummaryStatus(CheckedPartitionSummaryStatus status) {
		loadInfo_.status_ = status;
	}

	bool checkNextOwner(PartitionId pId, NodeId checkedNode, LogSequentialNumber checkedLsn,
		LogSequentialNumber checkedMaxLsn, NodeId currentOwner = UNDEF_NODEID);

	bool checkNextBackup(PartitionId pId, NodeId checkedNode, LogSequentialNumber checkedLsn,
		LogSequentialNumber currentOwnerStartLsn, LogSequentialNumber currentOwnerLsn);

	template <class T>
	bool getLiveNodeIdList(T& liveNodeIdList, int64_t checkTime = UNDEF_TTL, bool isCheck = false) {
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

	template <class T>
	bool getAllNodeIdList(T& nodeIdList) {
		for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
			nodeIdList.push_back(nodeId);
		}
		return true;
	}

	template <class T>
	void getOrderingLiveNodeIdList(util::StackAllocator& alloc, T& liveNodeIdList) {
		util::Set<std::pair<std::string, NodeId>> nodeOrderingSet(alloc);
		for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
			if (nodes_[nodeId].heartbeatTimeout_ != UNDEF_TTL) {
				nodeOrderingSet.insert(std::make_pair(nodes_[nodeId].getNodeAddress().toString(), nodeId));
			}
		}
		for (const auto& elem: nodeOrderingSet) {
			liveNodeIdList.push_back(elem.second);
		}
	}
	
	template <class T>
	void getLiveNodeAddressSet(T& liveNodeAddressSet) {
		bool forSSL = isUseSSL();
		for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
			if (nodeId < nodeNum_ && nodes_[nodeId].heartbeatTimeout_ != UNDEF_TTL) {
				if (!forSSL) {
					liveNodeAddressSet.insert(getNodeAddress(nodeId, SYSTEM_SERVICE));
				}
				else {
					NodeAddress address = getNodeAddress(nodeId, SYSTEM_SERVICE);
					address.port_ = static_cast<uint16_t>(getSSLPortNo(nodeId));
					liveNodeAddressSet.insert(address);
				}
			}
		}
	}

	void setReserveNum(int32_t reserveNum) {
		reserveNum_ = reserveNum;
	}

	int32_t getReserveNum() {
		return reserveNum_;
	}

	bool checkLiveNode(NodeId nodeId, int64_t limitTime = UNDEF_TTL) {
		return nodes_[nodeId].isAlive(limitTime);
	}

	void setHeartbeatTimeout(NodeId nodeId, int64_t heartbeatTimeout = UNDEF_TTL) {
		nodes_[nodeId].heartbeatTimeout_ = heartbeatTimeout;
		if (heartbeatTimeout != UNDEF_TTL) {
			nodes_[nodeId].isAcked_ = true;
		}
		else {
			nodes_[nodeId].isAcked_ = false;
		}
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

	void clear(PartitionId pId) {
		util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
		partitions_[pId].clear();
	}

	void clearCurrentStatus();
	void clearRole(PartitionId pId);
	bool checkClearRole(PartitionId pId);

	void getNodeAddressInfo(AddressInfoList& addressInfoList,
		AddressInfoList& publicAddressInfoList);

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

	PartitionGroupId getPartitionGroupId(PartitionId pId) {
		if (pId >= config_.partitionNum_) {
			GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
		}
		return partitions_[pId].partitionInfo_.pgId_;
	}

	void updatePartitionRevision(PartitionRevisionNo partitionSequentialNumber);

	NodeAddress& getNodeAddress(NodeId nodeId, ServiceType type = CLUSTER_SERVICE) {
		return getNodeInfo(nodeId).getNodeAddress(type);
	}

	NodeAddress& getPublicNodeAddress(NodeId nodeId, ServiceType type) {
		if (!getNodeInfo(nodeId).publicAddressInfo_.isValid()) {
			return getNodeAddress(nodeId, type);
		}
		else {
			return getNodeInfo(nodeId).publicAddressInfo_.getNodeAddress(type);
		}
	}

	void progress(double &goalProgress, double &replicaProgress);
	void getProgress(double& goalProgress, double& replicaProgress) {
		goalProgress = goalProgress_;
		replicaProgress = replicaProgress_;
	}

	void setAssignementRule(int32_t rule) {
		goalAssignmentRule_ = rule;
	}

	void setEnableStableGoal(bool flag) {
		stableGoal_.enable_ = flag;
	}

	void setPolicy(int32_t policy) {
		stableGoal_.policy_ = policy;
	}

	void setInitialGenerate(bool flag) {
		stableGoal_.enableInitialGenerate_ = flag;
	}

	util::Mutex& getDumpLock() {
		return dumpLock_;
	}

	void setParallelSyncNum(int32_t syncNum) {
		parallelSyncNum_ = syncNum;
	}

	void setParallelChunkSyncNum(int32_t syncNum) {
		parallelChunkSyncNum_ = syncNum;
	}

	int32_t getParallelSyncNum() {
		return parallelSyncNum_;
	}

	int32_t getParallelChunkSyncNum() {
		return parallelChunkSyncNum_;
	}

	void setLogSyncLsnDifference(LogSequentialNumber lsn) {
		logSyncLsnDifference_ = lsn;
	}

	LogSequentialNumber getLogSyncLsnDifference() {
		return logSyncLsnDifference_;
	}

	/*!
		@brief Get node address
	*/
	/** **
		@brief 指定サービスのIPアドレスを取得
		@param [in] nodeId ノードID
		@param [in] type サービス種別
		@param [in] usePublicAddress 外部IPアドレス取得要求
		@note クライアント指定によるが、単一ネットワーク構成でusePublic=trueの場合、結果的に内部IPアドレスを返す
	** **/
	NodeAddress& getNodeAddress(NodeId nodeId, ServiceType type, bool usePublicAddress) {

		if (!usePublicAddress) {
			return getNodeAddress(nodeId, type);
		}
		else {
			return getPublicNodeAddress(nodeId, type);
		}
	}

	bool checkSetService(NodeId nodeId, ServiceType type = CLUSTER_SERVICE) {
		return getNodeInfo(nodeId).check(type);
	}

	PartitionRevision& getPartitionRevision() {
		return revision_;
	}

	void setPublicAddressInfo(NodeId nodeId, AddressInfo& publicAddressInfo);

	PartitionRevision& incPartitionRevision() {
		util::LockGuard<util::Mutex> lock(revisionLock_);
		if (revision_.getRevisionNo() == INT64_MAX) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_CS_MAX_PARTITION_REVISION,
				"Cluster partition sequence number is limit");
		}
		else {
			revision_.increment();
		}
		return revision_;
	}

	PartitionRevisionNo getPartitionRevisionNo() {
		return revision_.getRevisionNo();
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

	PartitionConfig& getConfig() {
		return config_;
	}


	PartitionId getCatchupPId() {
		return prevCatchupPId_;
	}

	std::string dumpNodeAddress(NodeId nodeId, ServiceType type = CLUSTER_SERVICE) {
		if (nodeId >= nodeNum_ || nodeId < 0) {
			return std::string("");
		}
		return nodes_[nodeId].nodeBaseInfo_[type].address_.toString();
	}

	template <class T>
	std::string dumpNodeAddressList(T& nodeList, ServiceType type = CLUSTER_SERVICE) {
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

	std::string dumpNodes();
	std::string dumpPartitions(util::StackAllocator& alloc, TableType type, KeepLogManager* keepLogManager=NULL);
	std::string dumpPartitionsList(util::StackAllocator& alloc, TableType type);
	std::string dumpPartitionsNew(util::StackAllocator& alloc, TableType type,
		SubPartitionTable* subTable = NULL);

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

	static std::string dumpClusterStatus(NodeId nodeId) {
		if (nodeId == SELF_NODEID) {
			return "MASTER";
		}
		else if (nodeId == UNDEF_NODEID) {
			return "SUB_MASTER";
		}
		else {
			return "FOLLOWER";
		}
	}

	std::string dumpCurrentClusterStatus() {
		return dumpClusterStatus(masterNodeId_);
	}

	static inline const std::string dumpPartitionStatus(
		PartitionStatus status) {

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
	std::string dumpGoalAssignmentRule();
	std::string dumpGoalAssignmentRule(int32_t rule);

	std::string dumpDatas(bool isDetail = false, bool lsnOnly = false);

	void set(SubPartition& subPartition);

	void getServiceAddress(NodeId nodeId, picojson::value& result,
		ServiceType addressType, bool forSsl);

	void getServiceAddress(NodeId nodeId, picojson::object& result,
		const ServiceTypeInfo& addressType);
	void updateNewSQLPartition(SubPartitionTable& newsqlPartition, bool force);
	NodeId getNewSQLOwner(PartitionId pId);
	PartitionRevisionNo getNewSQLPartitionRevision(PartitionId pId);

	template <typename T1, typename T2>
	void getActiveNodeList(T1& activeNodeList, T2& downNodeSet, int64_t currentTime);

	void setGoalPartition() {
		isGoalPartition_ = true;
	}

	void stat(util::StackAllocator& alloc, TableType type, NodesStat& stat);

private:


	struct PartitionInfo {

		PartitionInfo() : pgId_(0), partitionRevision_(PartitionRevision::INITIAL_CLUSTER_REVISION),
			available_(true) {}

		void clear() {
			partitionRevision_ = PartitionRevision::INITIAL_CLUSTER_REVISION;
		}

		PartitionGroupId pgId_;
		PartitionRevisionNo partitionRevision_;
		bool available_;
	};

	struct NodeInfo {

		struct NodeBaseInfo {

			NodeBaseInfo() : isSet_(false) {}

			bool set(NodeAddress& address) {
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

		NodeInfo() : nodeBaseInfo_(NULL),
			heartbeatTimeout_(UNDEF_TTL),
			isAcked_(false),
			globalStackAlloc_(NULL),
			sslPort_(UNDEF_SSL_PORT),
			rackZoneIdName_(""),
			rackZoneId_(UNDEF_RACKZONEID),
			standbyMode_(false),
			startTime_(INT64_MAX)
		{}

		~NodeInfo() {
			if (nodeBaseInfo_) {
				for (int32_t pos = 0; pos < SERVICE_MAX; pos++) {
					ALLOC_DELETE(*globalStackAlloc_, &nodeBaseInfo_[pos]);
				}
			}
		}

		void init(util::StackAllocator& alloc) {
			globalStackAlloc_ = &alloc;
			nodeBaseInfo_ = ALLOC_NEW(alloc) NodeBaseInfo[SERVICE_MAX];
		}

		NodeAddress& getNodeAddress(ServiceType type = CLUSTER_SERVICE) {
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

		std::string& dumpNodeAddress(ServiceType serviceType);
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

		NodeBaseInfo* nodeBaseInfo_;
		int64_t heartbeatTimeout_;
		bool isAcked_;
		util::StackAllocator* globalStackAlloc_;
		AddressInfo publicAddressInfo_;
		int32_t sslPort_;
		std::string rackZoneIdName_;
		RackZoneId rackZoneId_;
		bool standbyMode_;
		int64_t startTime_;
	};

	class BlockQueue {
	public:
		BlockQueue(int32_t partitionNum, int32_t queueSizeLimit = INT32_MAX)
			: queueList_(NULL), partitionNum_(partitionNum),
			queueSize_(queueSizeLimit), globalStackAlloc_(NULL) {}

		~BlockQueue() {
			if (queueList_) {
				for (uint32_t pos = 0; pos < partitionNum_; pos++) {
					ALLOC_DELETE(*globalStackAlloc_, &queueList_[pos]);
				}
			}
		};

		void init(util::StackAllocator& alloc) {
			globalStackAlloc_ = &alloc;
			queueList_ = ALLOC_NEW(*globalStackAlloc_) std::set<NodeId>[partitionNum_];
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
		std::set<NodeId>* queueList_;
		uint32_t partitionNum_;
		int32_t queueSize_;
		util::StackAllocator* globalStackAlloc_;
	};

	struct CurrentPartitionInfo {
		CurrentPartitionInfo() :
			lsn_(0),
			startLsn_(0),
			status_(PT_OFF),
			roleStatus_(PT_NONE),
			partitionRevision_(1),
			chunkCount_(0),
			errorStatus_(PT_OTHER_NORMAL), prevRoleStatus_(PT_NONE), pending_(false) {}

		LogSequentialNumber lsn_;
		LogSequentialNumber startLsn_;
		PartitionStatus status_;
		PartitionRoleStatus roleStatus_;
		PartitionRevisionNo partitionRevision_;
		int64_t chunkCount_;
		int32_t errorStatus_;
		PartitionRoleStatus prevRoleStatus_;
		bool pending_;

		void clear() {
			startLsn_ = 0;
			status_ = PT_OFF;
			roleStatus_ = PT_NONE;
			partitionRevision_ = PartitionRevision::INITIAL_CLUSTER_REVISION;
			chunkCount_ = 0;
			errorStatus_ = PT_OTHER_NORMAL;
			prevRoleStatus_ = PT_NONE;
		}
	};

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

		void setLsn(
				NodeId nodeId, LogSequentialNumber lsn,
				bool updateCheck = false) {
			UNUSED_VARIABLE(updateCheck);

			checkSize(nodeId);
			currentPartitionInfos_[nodeId].lsn_ = lsn;
		}

		LogSequentialNumber getStartLsn(NodeId nodeId) {
			checkSize(nodeId);
			return currentPartitionInfos_[nodeId].startLsn_;
		}

		void setStartLsn(NodeId nodeId, LogSequentialNumber startLsn) {
			checkSize(nodeId);
			if (currentPartitionInfos_[nodeId].startLsn_ < startLsn) {
				currentPartitionInfos_[nodeId].startLsn_ = startLsn;
			}
		}

		void setErrorStatus(NodeId nodeId, int32_t status) {
			checkSize(nodeId);
			int32_t prev = currentPartitionInfos_[nodeId].errorStatus_;
			if (prev != PT_ERROR_NORMAL && status == PT_ERROR_NORMAL && !currentPartitionInfos_[nodeId].pending_) {
				currentPartitionInfos_[nodeId].pending_ = true;
				return;
			}
			currentPartitionInfos_[nodeId].errorStatus_ = status;
			currentPartitionInfos_[nodeId].pending_ = false;
		}

		int32_t getErrorStatus(NodeId nodeId) {
			checkSize(nodeId);
			int32_t ret = currentPartitionInfos_[nodeId].errorStatus_;
			currentPartitionInfos_[nodeId].errorStatus_ = PT_ERROR_NORMAL;
			return ret;
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

		PartitionRoleStatus getPartitionPrevRoleStatus(NodeId nodeId) {
			checkSize(nodeId);
			return currentPartitionInfos_[nodeId].prevRoleStatus_;
		}
		
		void setPartitionRoleStatus(
			NodeId nodeId, PartitionRoleStatus roleStatus) {
			checkSize(nodeId);
			PartitionRoleStatus prevStatus = currentPartitionInfos_[nodeId].roleStatus_;
			if (prevStatus != roleStatus) {
				currentPartitionInfos_[nodeId].prevRoleStatus_ = prevStatus;
			}
			currentPartitionInfos_[nodeId].roleStatus_ = roleStatus;
		}

		PartitionRevisionNo getPartitionRevision(NodeId nodeId) {
			checkSize(nodeId);
			return currentPartitionInfos_[nodeId].partitionRevision_;
		}

		void setPartitionRevision(
			NodeId nodeId, PartitionRevisionNo partitionRevision) {
			checkSize(nodeId);
			currentPartitionInfos_[nodeId].partitionRevision_ = partitionRevision;
		}

		void resetPartitionInfo(NodeId nodeId) {
			checkSize(nodeId);
			currentPartitionInfos_[nodeId].roleStatus_ = PT_NONE;
			currentPartitionInfos_[nodeId].status_ = PT_OFF;
		}

	private:
		void checkSize(NodeId nodeId) {
			if (static_cast<size_t>(nodeId) >= currentPartitionInfos_.size()) {
				GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL,
					"Specified nodeId is invalid, nodeId=" << nodeId
					<< ", max node size=" << currentPartitionInfos_.size());
			}
		}

		std::vector<CurrentPartitionInfo> currentPartitionInfos_;
	};

	class Partition {
	public:
		Partition() : roles_(NULL), globalStackAlloc_(NULL) {}
		~Partition() {
			if (roles_) {
				for (int32_t pos = 0; pos < PT_ROLE_TYPE_NUM; pos++) {
					ALLOC_DELETE(*globalStackAlloc_, &roles_[pos]);
				}
			}
		}

		void init(
				util::StackAllocator& alloc, PartitionId pId,
				PartitionGroupId pgId, int32_t nodeLimit) {
			UNUSED_VARIABLE(nodeLimit);

			globalStackAlloc_ = &alloc;
			roles_ = ALLOC_NEW(alloc) PartitionRole[PT_ROLE_TYPE_NUM];
			for (int32_t pos = 0; pos < PT_ROLE_TYPE_NUM; pos++) {
				roles_[pos].init(pId);
			}
			partitionInfo_.pgId_ = pgId;
		}

		void clear() {
			roles_[PT_CURRENT_OB].clear();
		}

		void check(NodeId nodeId);

		PartitionRole* roles_;
		PartitionInfo partitionInfo_;
	private:
		util::StackAllocator* globalStackAlloc_;
	};

	struct ReplicaInfo {
		ReplicaInfo(PartitionId pId, uint32_t count, uint32_t load) : pId_(pId), count_(count), load_(load){}
		PartitionId pId_;
		uint32_t count_;
		uint32_t load_;
	};

	class PartitionConfig {
	public:
		PartitionConfig(
			const ConfigTable& config, int32_t maxNodeNum);
		int32_t getLimitClusterNodeNum() {
			return limitClusterNodeNum_;
		}

		bool setLimitOwnerBackupLsnGap(int32_t gap) {
			if (gap < 0 || gap > INT32_MAX) {
				return false;
			}
			limitOwnerBackupLsnGap_ = gap;
			limitOwnerBackupLsnDetectErrorGap_
				= limitOwnerBackupLsnGap_ * PT_BACKUP_GAP_FACTOR;
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
		int32_t limitOwnerCatchupLsnGap_;
		int32_t limitClusterNodeNum_;
		int32_t limitMaxLsnGap_;
		uint32_t ownerLoadLimit_;
		uint32_t dataLoadLimit_;
		int32_t maxNodeNum_;
	};

	/** **
		@brief ゴール対象ノード情報
		@note パーティション割り付けを行うノード情報。優先リスト数に応じた優先度を保持
	** **/
	struct GoalNodeInfo {

		GoalNodeInfo(NodeId nodePos, int32_t priority) : nodePos_(nodePos), priority_(priority) {}
		bool operator<(const GoalNodeInfo& another) const {
			return (priority_ > another.priority_);
		}

		NodeId nodePos_;
		int32_t priority_;
	};

	/** **
		@brief ゴールプランナー
		@note LSNリストとクラスタ情報からゴールパーティション決定のためのヒューリスティック実行
	** **/
	class GoalPlanner {
	public:
		static const int32_t PRIORIY_1ST = 0;
		static const int32_t PRIORIY_2ND = 1;
		static const int32_t PRIORIY_NONE = 2;

		static const int32_t MODE_RACKZONE = 0;
		static const int32_t MODE_NORMAL = 1;
		static const int32_t MODE_MAX = 2;

		GoalPlanner(PartitionTable* pt, util::StackAllocator& alloc,
			util::XArray<NodeId>& nodeIdList, util::Vector<PartitionRole>& roleList);

		bool assginePriorityPartition(NodeId targetNodePos, int32_t priorityNo, int32_t& priority);

		int32_t getReplicaNum() {
			return backupNum_ + 1;
		}

		int32_t calcPriority(NodeId targetNodePos) {
			return static_cast<int32_t>(
					priorityPartitionLists_[targetNodePos].size() +
					priorityPartitionBackupLists_[targetNodePos].size());
		}

		void setOrigianlRackZoneAwareness(bool flag) {
			orgRackZoneAwareness_ = flag;
		}

		int32_t setRackZoneMode(int32_t mode);

		void validate(util::StackAllocator& alloc);

	private:

		void setupMaxLsn();
		void setupPriorityPartition();
		bool assignPartition(PartitionId pId, int32_t nodePos, bool backupOnly, int prioriyNo);

		util::StackAllocator& alloc_;
		PartitionTable* pt_;
		util::XArray<NodeId>& nodeIdList_;
		util::Vector<PartitionRole>& roleList_;
		PartitionId partitionNum_;
		int32_t nodeNum_;
		int32_t backupNum_;
		util::Vector<util::Vector<PartitionId>> priorityPartitionLists_;
		util::Vector<util::Vector<PartitionId>> priorityPartitionBackupLists_;
		util::Vector<PartitionId> priorityCountList_;
		util::Vector<LogSequentialNumber> maxLsnList_;
		util::Vector<RackZoneId> rackZoneIdList_;
		bool currentRackZoneAwareness_;
		bool orgRackZoneAwareness_;
	};

	/** **
		@brief ゴール情報修正プランナー
		@note オーナの偏りを解消する
	** **/
	class ModifyPlanner {
	public:
		
		ModifyPlanner(util::StackAllocator& alloc, uint32_t nodeNum, uint32_t partitionNum,
			util::XArray<NodeId>& nodeIdList, util::Vector<PartitionRole>& roleList);

		util::Vector<PartitionId>& getResolvePartitionIdList() {
			return pIdList_;
		}

		void update();

		bool check(int32_t limit) {
			if (maxOwnerCount_ - minOwnerCount_ <= limit) {
				return true;
			}
			return false;
		}

		NodeId getSwappableNodeId(util::StackAllocator& alloc, PartitionRole& role);

	private:
		void init() {
			maxOwnerCount_ = -1;
			minOwnerCount_ = INT32_MAX;
			maxOwnerNodeId_ = UNDEF_NODEID;
			minOwnerNodeId_ = UNDEF_NODEID;
		}

		util::Vector<int32_t> ownerCountList_;
		util::Vector<PartitionId> pIdList_;
		util::XArray<NodeId>& nodeIdList_;
		util::Random random_;
		int32_t maxOwnerCount_;
		int32_t minOwnerCount_;
		NodeId maxOwnerNodeId_;
		NodeId minOwnerNodeId_;
	};


	struct LoadSummary {
		LoadSummary(util::StackAllocator& alloc, int32_t nodeLimit) :
			ownerLoadList_(alloc),
			dataLoadList_(alloc),
			liveNodeList_(alloc),
			ownerLimit_(PT_OWNER_LOAD_LIMIT),
			dataLimit_(PT_DATA_LOAD_LIMIT),
			status_(PT_INITIAL),
			nodeNum_(0),
			maxDataLoad_(0),
			minDataLoad_(0),
			maxOwnerLoad_(0),
			minOwnerLoad_(0),
			ownerLoadCount_(0),
			dataLoadCount_(0) {
			ownerLoadList_.assign(static_cast<size_t>(nodeLimit), 0);
			dataLoadList_.assign(static_cast<size_t>(nodeLimit), 0);
			liveNodeList_.assign(static_cast<size_t>(nodeLimit), 0);
		}

		void init(int32_t nodeNum) {
			std::fill(ownerLoadList_.begin(), ownerLoadList_.begin() + nodeNum, 0);
			std::fill(dataLoadList_.begin(), dataLoadList_.begin() + nodeNum, 0);
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

		util::Vector<uint32_t> ownerLoadList_;
		util::Vector<uint32_t> dataLoadList_;
		util::Vector<uint8_t> liveNodeList_;
		uint32_t ownerLimit_;
		uint32_t dataLimit_;
		CheckedPartitionSummaryStatus status_;
		int32_t nodeNum_;
		uint32_t maxDataLoad_;
		uint32_t minDataLoad_;
		uint32_t maxOwnerLoad_;
		uint32_t minOwnerLoad_;
		int32_t ownerLoadCount_;
		int32_t dataLoadCount_;
	};


	static const int32_t PT_BACKUP_GAP_FACTOR = 2;
	static const int32_t PT_OWNER_LOAD_LIMIT = 2;
	static const int32_t PT_DATA_LOAD_LIMIT = 2;

	void genPartitionColumnStr(util::NormalOStringStream& ss, std::string& roleName,
		PartitionId pId, NodeId nodeId, int32_t partitionDigit, int32_t nodeIdDigit, TableType type);

	void getAddNodes(util::Set<NodeId>& nodeIds) {
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

	void getDownNodes(util::Set<NodeId>& downNodes, bool isClear = true) {
		util::LockGuard<util::Mutex> lock(nodeInfoLock_);
		for (const auto& nodeId : downNodeList_) {
			downNodes.insert(nodeId);
		}
		if (isClear) {
			downNodeList_.clear();
		}
	}

	/** **
		@brief ラックゾーンID取得
		@note 指定ノードのラックゾーンID取得
	** **/
	RackZoneId getRackZoneId(NodeId nodeId = 0);

	/** **
		@brief ラックゾーンID取得
		@note 指定ノードIDのラックゾーンID取得
	** **/
	bool isUseRackZoneAwareness(util::StackAllocator& alloc);

	bool checkLimitOwnerBackupLsnGap(
		LogSequentialNumber ownerLSN,
		LogSequentialNumber backupLSN, int32_t gapFactor = 1) {
		uint64_t gapValue =
			static_cast<uint64_t>(config_.limitOwnerBackupLsnGap_) * static_cast<uint64_t>(gapFactor);
		if (ownerLSN >= backupLSN) {
			return (ownerLSN - backupLSN > gapValue);
		}
		else {
			return (backupLSN - ownerLSN > gapValue);
		}
	}

	NodeInfo& getNodeInfo(NodeId nodeId) {
		if (nodeId >= config_.maxNodeNum_ || nodeId == UNDEF_NODEID) {
			GS_THROW_USER_ERROR(GS_ERROR_CS_SERVICE_CLUSTER_INTERNAL_FAILED, "");
		}
		return nodes_[nodeId];
	}

	void setAvailable(PartitionId pId, bool available) {
		partitions_[pId].partitionInfo_.available_ = available;
	}

	bool checkTargetPartition(PartitionId pId, int32_t backupNum, bool& catchupWait, PartitionContext& context);

	void clearPartitionRole(PartitionId pId);

	bool applyInitialRule(PartitionContext& context);
	bool applyAddRule(PartitionContext& context);
	bool applyRemoveRule(PartitionContext& context);
	bool applySwapRule(PartitionContext& context);

	struct ApplyContext {
		ApplyContext(PartitionContext& cxt, util::Vector<ReplicaInfo> &resolvePartitionList);
		util::StackAllocator& alloc_;
		SubPartitionTable &subPartitionTable_;
		util::Vector<ReplicaInfo>& resolvePartitionList_;
		util::Vector<PartitionGroupId> catchupPGIdList_;
		int32_t syncMaxNum_;
		int32_t chunkSyncMaxNum_;
		int32_t currentSyncCount_;
		int32_t currentChunkSyncCount_;
	};
	void setParallelCatchup(ApplyContext& cxt);

	void getReplicaLossPIdList(util::StackAllocator& alloc, util::Vector<ReplicaInfo>& resolvePIdList, bool checkLoad);
	void getCurrentCatchupPIdList(util::Vector<PartitionId>& catchupPIdList);
	void checkRepairPartition(PartitionContext& context);

	static bool cmpReplica(const ReplicaInfo& a, const ReplicaInfo& b) {
		if (a.count_ < b.count_) {
			return true;
		}
		if (a.count_ > b.count_) {
			return false;
		}
		return (a.load_ < b.load_);
	};

	void assignGoalTable(util::StackAllocator& alloc, util::XArray<NodeId>& nodeIdList,
		util::Vector<PartitionRole>& roleList);

	void modifyGoalTable(util::StackAllocator& alloc, util::XArray<NodeId>& nodeIdList,
		util::Vector<PartitionRole>& roleList);

	void preparePlanGoal();

	void addCandBackup(PartitionId pId, NodeIdList& nodeList, PartitionContext& context, NodeId owner, util::XArray<NodeId> &activeNodeList);

	bool checkTransitionLimitTime(PartitionContext& cxt) {
		return (!immediateCheck_ || (immediateCheck_  && cxt.currentTime_ >= nextApplyRuleLimitTime_));
	}

	util::FixedSizeAllocator<util::Mutex> fixedSizeAlloc_;
	util::StackAllocator globalStackAlloc_;

	int32_t nodeNum_;
	NodeId masterNodeId_;
	PartitionRevision revision_;
	std::vector<LogSequentialNumber> maxLsnList_;

	util::Mutex revisionLock_;
	util::Mutex nodeInfoLock_;
	util::RWLock lock_;
	util::RWLock* partitionLockList_;

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
	struct ParallelCatchup {
		PartitionId pId_;
		NodeId nodeId_;
		ParallelCatchup() : pId_(UNDEF_PARTITIONID), nodeId_(UNDEF_NODEID) {}
		ParallelCatchup(PartitionId pId, NodeId nodeId) : pId_(pId), nodeId_(nodeId) {}
		void reset() {
			pId_ = UNDEF_PARTITIONID;
			nodeId_ = UNDEF_NODEID;
		}
		bool operator<(const ParallelCatchup& right) const {
			if (pId_ < right.pId_) {
				return true;
			}
			if (pId_ > right.pId_) {
				return false;
			}
			return (nodeId_ < right.nodeId_);
		}
	};
	std::vector< ParallelCatchup> parallelCatchupList_;

	Partition* partitions_;
	CurrentPartitions* currentPartitions_;

	NodeInfo* nodes_;

	util::Mutex newsqlLock_;
	std::vector<PartitionRole> newsqlPartitionList_;
	std::unordered_map <std::string, int32_t> rackZoneMap_;
	bool enableRackZoneAwareness_;
	bool orgEnableRackZoneAwareness_;

	LoadSummary loadInfo_;

	double goalProgress_;
	double replicaProgress_;

	int32_t partitionDigit_;
	int32_t lsnDigit_;

	bool isRepaired_;
	PartitionRevisionNo currentRevisionNo_;
	PartitionRevisionNo prevDropRevisionNo_;
	bool isGoalPartition_;
	bool hasPublicAddress_;
	std::set<std::pair<std::string, NodeId>> nodeOrderingSet_;
	int32_t goalAssignmentRule_;
	StableGoalPartition stableGoal_;
	int32_t reserveNum_;
	int32_t currentGoalAssignmentRule_;
	bool needCheck_;
	const PartitionGroupConfig pgConfig_;
	int32_t parallelSyncNum_;
	int32_t parallelChunkSyncNum_;
	bool immediateCheck_;
	ManagerSet* mgrSet_;
	util::Mutex dumpLock_;
	int32_t prevCatchupWaitCount_;
	int32_t logSyncLsnDifference_;
	bool applyNodeBalance_;
};

typedef PartitionTable::PartitionStatus PartitionStatus;
typedef PartitionTable::PartitionRevision PartitionRevision;
typedef PartitionTable::PartitionRole PartitionRole;
typedef PartitionTable::PartitionContext PartitionContext;
typedef PartitionTable::PartitionRoleStatus PartitionRoleStatus;
typedef PartitionTable::TableType TableType;

std::ostream& operator<<(std::ostream& stream, PartitionTable::SubPartition& subPartition);
std::ostream& operator<<(std::ostream& stream, PartitionRole& PartitionRole);
std::ostream& operator<<(std::ostream& stream, NodeAddress& nodeAddres);
std::ostream& operator<<(std::ostream& stream, PartitionRevision& revision);

#endif
