﻿/*
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
	@brief Implementation of PartitionTable
*/
#include "partition_table.h"
#include "config_table.h"
#include "picojson.h"
#include "json.h"

UTIL_TRACER_DECLARE(CLUSTER_SERVICE);
UTIL_TRACER_DECLARE(CLUSTER_OPERATION);
UTIL_TRACER_DECLARE(CLUSTER_DUMP);

#include <iomanip>

#define GS_TRACE_CLUSTER_INFO(s) \
	GS_TRACE_INFO(CLUSTER_OPERATION, GS_TRACE_CS_CLUSTER_STATUS, s); 

#define GS_TRACE_CLUSTER_DUMP(s) \
	GS_TRACE_DEBUG(CLUSTER_OPERATION, GS_TRACE_CS_CLUSTER_STATUS, s); 

NodeAddress::NodeAddress(const char8_t *addr, uint16_t port) {
	util::SocketAddress address(addr, port);
	address.getIP((util::SocketAddress::Inet *)&address_, &port_);
}

void NodeAddress::set(const char8_t *addr, uint16_t port) {
	util::SocketAddress address(addr, port);
	address.getIP((util::SocketAddress::Inet *)&address_, &port_);
}

void NodeAddress::set(const util::SocketAddress &socketAddress) {
	socketAddress.getIP((util::SocketAddress::Inet *)&address_, &port_);
}

/*!
	@brief Gets NodeAddress that correspond to a specified type
*/
NodeAddress &AddressInfo::getNodeAddress(ServiceType type) {
	switch (type) {
	case CLUSTER_SERVICE:
		return clusterAddress_;
	case TRANSACTION_SERVICE:
		return transactionAddress_;
	case SYNC_SERVICE:
		return syncAddress_;
	case SYSTEM_SERVICE:
		return systemAddress_;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_PT_INVALID_NODE_ADDRESS, "");
	}
}

/*!
	@brief Sets NodeAddress that correspond to a specified type
*/
void AddressInfo::setNodeAddress(ServiceType type, NodeAddress &address) {
	switch (type) {
	case CLUSTER_SERVICE:
		clusterAddress_ = address;
		break;
	case TRANSACTION_SERVICE:
		transactionAddress_ = address;
		break;
	case SYNC_SERVICE:
		syncAddress_ = address;
		break;
	case SYSTEM_SERVICE:
		systemAddress_ = address;
		break;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_PT_INVALID_NODE_ADDRESS, "");
	}
}

PartitionTable::PartitionTable(const ConfigTable &configTable)
	: fixedSizeAlloc_(
			util::AllocatorInfo(ALLOCATOR_GROUP_CS, "partitionTableFixed"), 1 << 18),
	alloc_(util::AllocatorInfo(ALLOCATOR_GROUP_CS, "partitionTableStack"),
		&fixedSizeAlloc_),
	nodeNum_(0),
	masterNodeId_(UNDEF_NODEID),
	partitionLockList_(NULL),
	config_(configTable, MAX_NODE_NUM),
	prevMaxNodeId_(0),
	nextBlockQueue_(configTable.get<int32_t>(CONFIG_TABLE_DS_PARTITION_NUM)),
	goalBlockQueue_(configTable.get<int32_t>(CONFIG_TABLE_DS_PARTITION_NUM)),
	prevCatchupPId_(UNDEF_PARTITIONID),
	partitions_(NULL),
	nodes_(NULL),
	loadInfo_(alloc_, MAX_NODE_NUM),
	currentRevisionNo_(1),
	prevDropRevisionNo_(0)
{
	try {
		uint32_t partitionNum = config_.partitionNum_;
		partitionLockList_ = ALLOC_NEW(alloc_) util::RWLock [partitionNum];
		partitions_ = ALLOC_NEW(alloc_) Partition [partitionNum];
		currentPartitions_ = ALLOC_NEW(alloc_) CurrentPartitions [partitionNum];
		NodeId maxNodeNum = config_.limitClusterNodeNum_;
		PartitionGroupConfig pgConfig(configTable);
		for (PartitionId pId = 0; pId < partitionNum; pId++) {
			partitions_[pId].init(
				alloc_, pId, pgConfig.getPartitionGroupId(pId), maxNodeNum);
			currentPartitions_[pId].init();
		}
		nodes_ = ALLOC_NEW(alloc_) NodeInfo [maxNodeNum];
		for (NodeId nodeId = 0; nodeId < maxNodeNum; nodeId++) {
			nodes_[nodeId].init(alloc_);
		}
		maxLsnList_.assign(partitionNum, 0);
		nextBlockQueue_.init(alloc_);
		goalBlockQueue_.init(alloc_);

		int32_t digit = 0;
		for (; partitionNum != 0; digit++) {
			partitionNum /= 10;
		}
		partitionDigit_ = digit;
		lsnDigit_ = 10;
		goal_.init(this);

		PartitionRole initRole;
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

PartitionTable::PartitionConfig::PartitionConfig(
		const ConfigTable &configTable, int32_t maxNodeNum)
	: partitionNum_(configTable.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM)),
	replicationNum_(
		configTable.get<int32_t>(CONFIG_TABLE_CS_REPLICATION_NUM) - 1),
	limitOwnerBackupLsnGap_(
		configTable.get<int32_t>(CONFIG_TABLE_CS_OWNER_BACKUP_LSN_GAP)),
	limitOwnerCatchupLsnGap_(
		configTable.get<int32_t>(CONFIG_TABLE_CS_OWNER_CATCHUP_LSN_GAP)),
	limitClusterNodeNum_(maxNodeNum),
	limitMaxLsnGap_(0),
	ownerLoadLimit_(PT_OWNER_LOAD_LIMIT),
	dataLoadLimit_(PT_DATA_LOAD_LIMIT),
	maxNodeNum_(maxNodeNum)
{
	limitOwnerBackupLsnDetectErrorGap_ =
		limitOwnerBackupLsnGap_ * PT_BACKUP_GAP_FACTOR;
}

PartitionTable::~PartitionTable() {

	for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
		if (partitionLockList_) {
			ALLOC_DELETE(alloc_, &partitionLockList_[pId]);
		}
		if (partitions_) {
			ALLOC_DELETE(alloc_, &partitions_[pId]);
		}
		if (currentPartitions_) {
			ALLOC_DELETE(alloc_, &currentPartitions_[pId]);
		}
	}
	if (nodes_) {
		for (NodeId nodeId = 0; nodeId < config_.getLimitClusterNodeNum();nodeId++) {
			if (&nodes_[nodeId]) {
				ALLOC_DELETE(alloc_, &nodes_[nodeId]);
			}
		}
	}
}

NodeId PartitionTable::getOwner(PartitionId pId, TableType type) {
	util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
	return partitions_[pId].roles_[type].getOwner();
}

bool PartitionTable::isOwner(
		PartitionId pId, NodeId targetNodeId, TableType type) {
	util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
	return (partitions_[pId].roles_[type].isOwner(targetNodeId));
}

bool PartitionTable::isOwnerOrBackup(
		PartitionId pId, NodeId targetNodeId, TableType type) {
	util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
	return (partitions_[pId].roles_[type].isOwner(targetNodeId) ||
			partitions_[pId].roles_[type].isBackup(targetNodeId));
}

void PartitionTable::getBackup(
		PartitionId pId, util::XArray<NodeId> &backups, TableType type) {
	util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
	partitions_[pId].roles_[type].getBackups(backups);
}

void PartitionTable::getBackup(
		PartitionId pId, std::vector<NodeId> &backups, TableType type) {
	util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
	backups = partitions_[pId].roles_[type].getBackups();
}

void PartitionTable::getCatchup(
		PartitionId pId, util::XArray<NodeId> &catchups, TableType type) {
	util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
	partitions_[pId].roles_[type].getCatchups(catchups);
}

void PartitionTable::getCatchup(
		PartitionId pId, std::vector<NodeId> &catchups, TableType type) {
	util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
	catchups = partitions_[pId].roles_[type].getCatchups();
}

NodeId PartitionTable::getCatchup(PartitionId pId) {
	util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
	std::vector<NodeId> &catchups
			= partitions_[pId].roles_[PT_CURRENT_OB].getCatchups();
	if (catchups.size() == 1) {
		return catchups[0];
	}
	else {
		return UNDEF_NODEID;
	}
}


bool PartitionTable::isBackup(
		PartitionId pId, NodeId targetNodeId, TableType type) {
	util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
	return (partitions_[pId].roles_[type].isBackup(targetNodeId));
}

bool PartitionTable::isCatchup(
		PartitionId pId, NodeId targetNodeId, TableType type) {
	util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
	return (partitions_[pId].roles_[type].isCatchup(targetNodeId));
}

bool PartitionTable::hasCatchupRole(
		PartitionId pId, TableType type) {
	util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
	return (partitions_[pId].roles_[type].hasCatchupRole());
}

bool PartitionTable::setNodeInfo(
	NodeId nodeId, ServiceType type, NodeAddress &address) {
	if (nodeId >= config_.limitClusterNodeNum_) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_SET_INVALID_NODE_ID, "");
	}
	if (nodeId >= nodeNum_) {
		nodeNum_ = nodeId + 1;
		resizeCurrenPartition(0);
	}
	return nodes_[nodeId].nodeBaseInfo_[type].set(address);
}

/*!
	@brief Checks cluster status and update load information
*/
CheckedPartitionStat PartitionTable::checkPartitionStat(bool firstCheckSkip) {
	try {
		int32_t nodeNum = getNodeNum();
		int32_t backupNum = getReplicationNum();
		int32_t liveNum = 0;
		for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
			if (checkLiveNode(nodeId) || (nodeId == 0 && firstCheckSkip)) {
				liveNum++;
				loadInfo_.setAlive(nodeId, 1);
			}
			else {
				loadInfo_.setAlive(nodeId, 0);
			}
		}
		if (backupNum > liveNum - 1) {
			backupNum = liveNum - 1;
		}
		loadInfo_.init(nodeNum);
		NodeId ownerNodeId;
		bool isPartial = false;
		bool isReplicaLoss = false;
		for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
			ownerNodeId = getOwner(pId);
			if (ownerNodeId == UNDEF_NODEID) {
				isPartial = true;
				continue;
			}
			loadInfo_.add(ownerNodeId);
			loadInfo_.add(ownerNodeId, false);
			std::vector<NodeId> backups;
			getBackup(pId, backups);
			liveNum = 0;
			for (std::vector<NodeId>::iterator it = backups.begin();
				it != backups.end(); it++) {
				if (checkLiveNode(*it) || ((*it == 0) && firstCheckSkip)) {
					loadInfo_.add(*it, false);
					liveNum++;
				}
			}
			if (liveNum < backupNum) {
				isReplicaLoss = true;
			}
		}
		loadInfo_.update();
		CheckedPartitionStat retStatus;
		if (isPartial) {
			retStatus = PT_PARTIAL_STOP;
		}
		else if (isReplicaLoss) {
			retStatus = PT_REPLICA_LOSS;
		}
		else if (loadInfo_.isBalance(true, getPartitionNum()) &&
			loadInfo_.isBalance(
				false, (backupNum + 1) * getPartitionNum())) {
			retStatus = PT_NORMAL;
		}
		else {
			retStatus = PT_NOT_BALANCE;
		}
		loadInfo_.status_ = retStatus;
		return retStatus;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void PartitionTable::set(SubPartition &subPartition) {
	PartitionId pId = subPartition.pId_;
	if (pId == UNDEF_PARTITIONID) {
		return;
	}
	PartitionRole &role = subPartition.role_;
	TableType type = role.getTableType();
	util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
	partitions_[pId].roles_[type] = role;
}

void PartitionTable::clearCurrentStatus() {
	util::LockGuard<util::WriteLock> lock(lock_);
	for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
		setHeartbeatTimeout(nodeId, UNDEF_TTL);
	}
}

void PartitionTable::clearRole(PartitionId pId) {
	util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
	partitions_[pId].clear();
	partitions_[pId].partitionInfo_.clear();
}

bool PartitionTable::checkClearRole(PartitionId pId) {
	if (isBackup(pId) 
		&& getPartitionStatus(pId) == PT_ON) {
		clear(pId, PartitionTable::PT_CURRENT_OB);
		setPartitionRoleStatus(pId, PT_NONE);
		setPartitionStatus(pId, PT_OFF);
		return true;
	}
	else {
		return false;
	}
}


bool PartitionTable::checkConfigurationChange(
		util::StackAllocator &alloc, PartitionContext &context, bool checkFlag) {
	util::Set<NodeId> addNodeSet(alloc);
	util::Set<NodeId> downNodeSet(alloc);
	getAddNodes(addNodeSet);
	getDownNodes(downNodeSet);
	bool isAddOrDownNode = (addNodeSet.size() > 0 || downNodeSet.size() > 0 || checkFlag);
	if (isAddOrDownNode) {
		GS_TRACE_CLUSTER_INFO("Detect cluster configuration change, create new goal partition, "
			"current_time=" << getTimeStr(context.currentTime_) 
			<< ", revision=" << getPartitionRevisionNo());
		context.setConfigurationChange();
		planGoal(context);
	}
	return isAddOrDownNode;
}

bool PartitionTable::check(PartitionContext &context) {
//	bool retFlag = false;
	try {
		util::StackAllocator &alloc = *context.alloc_;
		checkRepairPartition(context);

		if (context.currentTime_ < nextApplyRuleLimitTime_) {
			GS_TRACE_CLUSTER_DUMP("Not check interval, "
				<< ", limitTime=" << getTimeStr(nextApplyRuleLimitTime_)
				<< ", currentTime=" << getTimeStr(context.currentTime_));
			return false;
		}

		GS_TRACE_CLUSTER_DUMP(dumpPartitions(alloc, PT_CURRENT_OB));
		GS_TRACE_CLUSTER_DUMP(dumpDatas(true));
		
		nextApplyRuleLimitTime_ = context.currentRuleLimitTime_;
		bool isAllSame = true;		
		bool catchupWait = false;
		bool isIrregular = false;
		for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
			PartitionRole currentRole;
			PartitionRole goalRole;
			getPartitionRole(pId, currentRole, PT_CURRENT_OB);
			getPartitionRole(pId, goalRole, PT_CURRENT_GOAL);
			if (currentRole == goalRole) {
			}
			else {
				isAllSame = false;
			}
			if (checkTargetPartition(pId, catchupWait)) {
				isIrregular = true;
			}
		}
		if (catchupWait && !isIrregular) {
			return false;
		}
		if (isAllSame) {
			GS_TRACE_CLUSTER_DUMP("Current partition is same to goal partition");
			CheckedPartitionStat balanceStatus = checkPartitionStat();
			if (balanceStatus != PT_NORMAL) {
				GS_TRACE_CLUSTER_INFO("Goal is not balance, create new goal partition");
				if (context.isLoadBalance_) {
					planGoal(context);
				}
				return true;
			}
			else {
				context.isGoalPartition_ = true;
				if (checkDrop(alloc, context.dropNodeInfo_)) {
					context.needUpdatePartition_ = true;
					context.subPartitionTable_.setRevision(getPartitionRevision());
					return true;
				}
			}
			return false;
		}
		return true;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void PartitionTable::updatePartitionRevision(
	PartitionRevisionNo partitionSequentialNumber) {
	util::LockGuard<util::Mutex> lock(revisionLock_);
	if (partitionSequentialNumber > revision_.sequentialNumber_ &&
		partitionSequentialNumber !=
			PartitionRevision::INITIAL_CLUSTER_REVISION) {
		revision_.sequentialNumber_ = partitionSequentialNumber;
	}
}

bool PartitionTable::checkDrop(util::StackAllocator &alloc, DropPartitionNodeInfo &dropInfo) {
	util::XArray<NodeId> activeNodeList(alloc);
	getLiveNodeIdList(activeNodeList);
	int32_t replicationNum = getReplicationNum();
	int32_t liveNodeListSize = static_cast<int32_t>(activeNodeList.size() - 1);
	bool enableDrop = false;
//	int32_t currentRevisionNo = revision_.sequentialNumber_;
	if (prevDropRevisionNo_ == currentRevisionNo_) {
		return false;
	}
	if (replicationNum > liveNodeListSize) {
		replicationNum = liveNodeListSize;
	}
	if (replicationNum == 0) return false;
	for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
		int32_t backupSize = getBackupSize(alloc, pId);
		if (backupSize != replicationNum) {
			continue;
		}
		for (size_t pos = 0; pos < activeNodeList.size(); pos++) {
			NodeId nodeId = activeNodeList[pos];
			if (getPartitionRoleStatus(pId, nodeId) != PartitionTable::PT_NONE) {
				continue;
			}
			NodeAddress &address = getNodeAddress(nodeId);
			PartitionRevisionNo revision = getPartitionRevision(pId, nodeId);
			dropInfo.set(pId, address, revision, nodeId);
			enableDrop = true;
		}
	}
	if (enableDrop) {
		prevDropRevisionNo_ = currentRevisionNo_;
	}
	return enableDrop;
}

bool PartitionTable::PartitionRole::operator==(PartitionRole &role) const {
	NodeId tmpOwnerId = role.getOwner();
	if (ownerNodeId_ == tmpOwnerId) {
		bool retFlag = true;
		std::vector<NodeId> tmpBackups = role.getBackups();
		if (backups_ == tmpBackups) {
		}
		else {
			retFlag = false;
		}
		std::vector<NodeId> tmpCatchups = role.getCatchups();
		if (catchups_ == tmpCatchups) {
		}
		else {
			retFlag = false;
		}
		return retFlag;
	}
	return false;
}

void PartitionTable::PartitionRole::encodeAddress(
	PartitionTable *pt, PartitionRole &role) {
	pId_ = role.getPartitionId();
	revision_ = role.getRevision();
	type_ = role.getTableType();

	NodeId ownerNodeId = role.getOwner();
	if (ownerNodeId != UNDEF_NODEID) {
		ownerAddress_ = pt->getNodeInfo(ownerNodeId).getNodeAddress();
	}
	std::vector<NodeId> &backups = role.getBackups();
	int32_t backupSize = static_cast<int32_t>(backups.size());
	backupAddressList_.clear();
	backupAddressList_.reserve(backupSize);
	for (int32_t pos = 0; pos < backupSize; pos++) {
		NodeId backupNodeId = backups[pos];
		backupAddressList_.push_back(
			pt->getNodeInfo(backupNodeId).getNodeAddress());
	}
	std::vector<NodeId> &catchups = role.getCatchups();
	int32_t catchupSize = static_cast<int32_t>(catchups.size());
	catchupAddressList_.clear();
	catchupAddressList_.reserve(catchupSize);
	for (int32_t pos = 0; pos < catchupSize; pos++) {
		NodeId catchupNodeId = catchups[pos];
		catchupAddressList_.push_back(
			pt->getNodeInfo(catchupNodeId).getNodeAddress());
	}
}

void PartitionTable::BlockQueue::push(PartitionId pId, NodeId nodeId) {
	if (static_cast<int32_t>(queueList_[pId].size()) > queueSize_) {
		queueList_[pId].clear();
	}
	queueList_[pId].insert(nodeId);
}

bool PartitionTable::BlockQueue::find(PartitionId pId, NodeId nodeId) {
	std::set<NodeId>::iterator setItr = queueList_[pId].find(nodeId);
	return (setItr != queueList_[pId].end());
}

void PartitionTable::BlockQueue::clear(PartitionId pId) {
	queueList_[pId].clear();
}

void PartitionTable::BlockQueue::erase(PartitionId pId, NodeId nodeId) {
	std::set<NodeId>::iterator setItr = queueList_[pId].find(nodeId);
	if (setItr != queueList_[pId].end()) {
		queueList_[pId].erase(setItr);
	}
}

template<class T> void arrayShuffle(util::XArray<T> &ary, uint32_t size, util::Random &random) {
	for(uint32_t i = 0;i < size; i++) {
		uint32_t j = (uint32_t)(random.nextInt32()) % size;
		T t = ary[i];
		ary[i] = ary[j];
		ary[j] = t;
	}
}

std::ostream &operator<<(std::ostream &ss, PartitionRole &partitionRole) {
	ss << partitionRole.dumpIds();
	return ss;
}

std::ostream &operator<<(std::ostream &ss, NodeAddress &nodeAddres) {
	ss << nodeAddres.toString();
	return ss;
}

std::ostream &operator<<(std::ostream &ss, PartitionRevision &revision) {
	ss << revision.toString();
	return ss;
}

std::string PartitionTable::dumpNodes() {
	util::NormalOStringStream ss;
	std::string terminateCode = "\n ";
	std::string tabCode = "\t";

	NodeInfo *nodeInfo;

	for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
		nodeInfo = &nodes_[nodeId];
		ss << tabCode << "#" << nodeId << " : ";
		ss << nodes_[nodeId].dump();

		if (masterNodeId_ == 0 || masterNodeId_ == UNDEF_NODEID) {
			if (nodeId == 0) {
				ss << "(Master)";
			}
			else if (nodeInfo->heartbeatTimeout_ == UNDEF_TTL) {
				ss << "(Down)";
			}
		}
		else {
			if (masterNodeId_ == nodeId) {
				ss << "(Master)";
			}
		}
		ss << " : ";
		if (nodeInfo->heartbeatTimeout_ == UNDEF_TTL) {
			ss << nodeInfo->heartbeatTimeout_;
		}
		else {
			ss << getTimeStr(nodeInfo->heartbeatTimeout_);
		}
		if (nodeId != nodeNum_ - 1) {
			ss << terminateCode;
		}
	}
	return ss.str();
}

void PartitionTable::genPartitionColumnStr(
		util::NormalOStringStream &ss, std::string &roleName, PartitionId pId,
		NodeId nodeId, int32_t lsnDigit, int32_t nodeIdDigit, TableType type) {
		ss << roleName << "(";
		ss << "#" << std::setw(nodeIdDigit) << nodeId;
		ss << ", ";
		ss << std::setw(lsnDigit_) << getLSN(pId, nodeId);
		if (type == PT_CURRENT_OB) {
			ss << ", ";
			ss << dumpPartitionStatusEx(getPartitionStatus(pId, nodeId));
		}
		ss << ")";
}

std::string PartitionTable::dumpPartitionsNew(
	util::StackAllocator &alloc, TableType type, SubPartitionTable *subTable) {
	util::NormalOStringStream ss;
	util::XArray<uint32_t> countList(alloc);
	util::XArray<uint32_t> ownerCountList(alloc);
	countList.assign(static_cast<size_t>(nodeNum_), 0);
	ownerCountList.assign(static_cast<size_t>(nodeNum_), 0);
	std::string terminateCode = "\n ";
	uint32_t partitionNum = config_.partitionNum_;
//	int32_t replicationNum = config_.replicationNum_;
//	size_t pos = 0;

	PartitionId pId;
	LogSequentialNumber maxLSN = 0;
	for (pId = 0; pId < partitionNum; pId++) {
		if (maxLsnList_[pId] > maxLSN) {
			maxLSN = maxLsnList_[pId];
		}
	}
	int32_t digit = 0;
	for (; maxLSN != 0; digit++) {
		maxLSN /= 10;
	}
	lsnDigit_ = digit;
	int32_t revisionDigit = 0;
	PartitionRevisionNo revison = getPartitionRevisionNo();
	PartitionRevisionNo tmpRevision = revison;
	for (; tmpRevision != 0; revisionDigit++) {
		tmpRevision /= 10;
	}

	int32_t nodeIdDigit = 0;
	PartitionRevisionNo maxNodeId = getNodeNum();
	for (; maxNodeId != 0; nodeIdDigit++) {
		maxNodeId /= 10;
	}

	std::string partitionType;
	std::string ownerStr("O");
	std::string backupStr("B");
	std::string catchupStr("C");
	std::string noneStr("-");

	switch (type) {
	case PT_CURRENT_OB:
		partitionType = "CURRENT_TABLE";
		break;
	case PT_CURRENT_GOAL:
		partitionType = "GOAL_TABLE";
		break;
	default:
		break;
	}
	ss << "[" << partitionType << "]" << " "
			<< getTimeStr(util::DateTime::now(TRIM_MILLISECONDS).getUnixTime()) << std::endl;
	for (NodeId nodeId = 0; nodeId < getNodeNum(); nodeId++) {
		ss << "#" << std::setw(nodeIdDigit) << nodeId << ", " << dumpNodeAddress(nodeId);
		if (getHeartbeatTimeout(nodeId) == UNDEF_TTL) {
			ss << "(Down)";
		}
			ss << std::endl;
	}
	util::Vector<PartitionId> partitionIdList(alloc);
	util::Map<PartitionId, PartitionRole*> roleMap(alloc);
	if (subTable) {
		for (size_t pos = 0; pos < static_cast<size_t>(subTable->size()); pos++) {		
			SubPartition &sub = subTable->getSubPartition(pos);
			partitionIdList.push_back(sub.pId_);
		}
		if (partitionIdList.size() > 0) {
			std::sort(partitionIdList.begin(), partitionIdList.end());
		}
		subTable->prepareMap(roleMap);
	}
	else {
	for (pId = 0; pId < partitionNum; pId++) {
			partitionIdList.push_back(pId);
		}
	}
	for (size_t pos = 0; pos < partitionIdList.size(); pos++) {
		pId = partitionIdList[pos];
		PartitionRole role;
		if (!subTable) {
		getPartitionRole(pId, role, type);
		}
		else {
			util::Map<PartitionId, PartitionRole*>::iterator it = roleMap.find(pId);
			if (it != roleMap.end()) {
				role = *(*it).second;
			}
			else {
				continue;
			}
		}
//		int32_t psum = 0;
//		int32_t currentPos = 0;
		ss << "[P" << std::setfill('0') << std::setw(partitionDigit_) << pId;
		ss << "(" << std::setw(revisionDigit) << role.getRevisionNo() << ")]";
		NodeId owner = role.getOwner();
		ss << " ";
		if (owner == UNDEF_NODEID) {
		}
		else {
			genPartitionColumnStr(ss, ownerStr, pId, owner, digit, nodeIdDigit, type);
		}
		std::vector<NodeId> &backups = role.getBackups();
		if (backups.size() > 0) {
			if (owner != UNDEF_NODEID) {
				ss << ", ";
			}
			for (size_t pos = 0; pos < backups.size(); pos++) {
				genPartitionColumnStr(ss, backupStr, pId, backups[pos], digit, nodeIdDigit, type);
				if (pos != backups.size() - 1) {
					ss << ", ";
				}
			}
		}
		std::vector<NodeId> &catchups = role.getCatchups();
		if (catchups.size() > 0) {
			ss << ", ";
			for (size_t pos = 0; pos < catchups.size(); pos++) {
				genPartitionColumnStr(ss, catchupStr, pId, catchups[pos], digit, nodeIdDigit, type);
				if (pos != catchups.size() - 1) {
					ss << ", ";
				}
		}
		}
		ss << std::endl;
	}
	return ss.str();
}



std::string PartitionTable::dumpPartitions(
	util::StackAllocator &alloc, TableType type) {
	util::NormalOStringStream ss;
	util::XArray<uint32_t> countList(alloc);
	util::XArray<uint32_t> ownerCountList(alloc);
	countList.assign(static_cast<size_t>(nodeNum_), 0);
	ownerCountList.assign(static_cast<size_t>(nodeNum_), 0);
	std::string terminateCode = "\n ";
	uint32_t partitionNum = config_.partitionNum_;
	int32_t replicationNum = config_.replicationNum_;
	size_t pos = 0;

	ss << terminateCode << "[PARTITION ROLE]" << terminateCode;
	ss << "# partitionNum:" << partitionNum
	<< ", replicaNum:" << (replicationNum + 1) << terminateCode;
	std::string partitionType;
	std::string ownerStr("O");
	std::string backupStr("B");
	std::string catchupStr("C");
	std::string noneStr("-");

	switch (type) {
	case PT_CURRENT_OB:
		partitionType = "CURRENT_TABLE";
		break;
	case PT_CURRENT_GOAL:
		partitionType = "CURRENT_GOAL";
		break;
	default:
		break;
	}
	ss << "PartitionType : " << partitionType << terminateCode;

	PartitionId pId;
	LogSequentialNumber maxLSN = 0;
	for (pId = 0; pId < partitionNum; pId++) {
		if (maxLsnList_[pId] > maxLSN) {
			maxLSN = maxLsnList_[pId];
		}
	}
	int32_t digit = 0;
	for (; maxLSN != 0; digit++) {
		maxLSN /= 10;
	}
	lsnDigit_ = digit;

	for (pId = 0; pId < partitionNum; pId++) {	
		PartitionRole role;
		getPartitionRole(pId, role, type);
		int32_t psum = 0;
		int32_t currentPos = 0;
		ss << "[P" << std::setfill('0') << std::setw(partitionDigit_) << pId
		<< "]";
		ss << "[" << dumpPartitionStatusEx(getPartitionStatus(pId)) << "]";

		std::vector<NodeId> currentBackupList;
		NodeId curretOwnerNodeId = role.getOwner();
		PartitionRevision &revision = role.getRevision();
		std::vector<NodeId> &backups = role.getBackups();
		std::vector<NodeId> &catchups = role.getCatchups();
		int32_t backupNum = static_cast<int32_t>(backups.size());
		int32_t catchupNum = static_cast<int32_t>(catchups.size());

		for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
			if (curretOwnerNodeId == nodeId) {
				ss << ownerStr;
				psum++;
				countList[nodeId]++;
				ownerCountList[nodeId]++;
			}
			else if (currentPos < backupNum && backups[currentPos] == nodeId) {
				ss << backupStr;
				psum++;
				pos++;
				countList[nodeId]++;
				currentPos++;
			}
			else if (catchupNum > 0 && catchups[0] == nodeId) {
				ss << catchupStr;
			}
			else {
				ss << noneStr;
			}
			if (nodeId != nodeNum_ - 1) ss << ",";
		}
		ss << "[" << std::setw(lsnDigit_) << maxLsnList_[pId] << "]";
		ss << "<" << revision.toString() << ">";
		ss << "(" << psum << ")" << terminateCode;
	}
	for (pos = 0; pos < countList.size(); pos++) {
		ss << countList[pos];
		if (pos != countList.size() - 1) ss << ",";
	}
	ss << terminateCode;
	for (pos = 0; pos < ownerCountList.size(); pos++) {
		ss << ownerCountList[pos];
		if (pos != ownerCountList.size() - 1) ss << ",";
	}
	return ss.str();
}

std::string PartitionTable::dumpPartitionsList(
	util::StackAllocator &alloc, TableType type) {
	util::NormalOStringStream ss;
	util::XArray<uint32_t> countList(alloc);
	util::XArray<uint32_t> ownerCountList(alloc);
	countList.assign(static_cast<size_t>(nodeNum_), 0);
	ownerCountList.assign(static_cast<size_t>(nodeNum_), 0);
	uint32_t partitionNum = config_.partitionNum_;
//	int32_t replicationNum = config_.replicationNum_;
//	size_t pos = 0;

	PartitionId pId;
	LogSequentialNumber maxLSN = 0;
	for (pId = 0; pId < partitionNum; pId++) {
		if (maxLsnList_[pId] > maxLSN) {
			maxLSN = maxLsnList_[pId];
		}
	}
	int32_t digit = 0;
	for (; maxLSN != 0; digit++) {
		maxLSN /= 10;
	}

	ss << "[";
	for (pId = 0; pId < partitionNum; pId++) {
		PartitionRole *role = &partitions_[pId].roles_[type];
//		int32_t psum = 0;
//		int32_t currentPos = 0;
		std::vector<NodeId> currentBackupList;
		NodeId curretOwnerNodeId = role->getOwner();
//		PartitionRevision &revision = role->getRevision();
		std::vector<NodeId> &backups = role->getBackups();
//		int32_t backupNum = static_cast<int32_t>(backups.size());
		ss << "{pId=" << pId << ", owner="<< dumpNodeAddress(curretOwnerNodeId) 
				<< ", backup=" << dumpNodeAddressList(backups) << "}";
		if (pId != partitionNum - 1) {
			ss << ",";
		}
		else {
			ss << "]";
		}
	}
	return ss.str();
}

std::string PartitionTable::dumpDatas(bool isDetail) {
	PartitionId pId;
	LogSequentialNumber maxLSN = 0;
	uint32_t partitionNum = config_.partitionNum_;

	for (pId = 0; pId < partitionNum; pId++) {
		if (maxLsnList_[pId] > maxLSN) {
			maxLSN = maxLsnList_[pId];
		}
	}
	int32_t digit = 0;
	for (; maxLSN != 0; digit++) {
		maxLSN /= 10;
	}
	lsnDigit_ = digit;

	util::NormalOStringStream ss;
	std::string terminateCode = "\n ";

	ss << terminateCode << "[LSN LIST]" << terminateCode;
	for (pId = 0; pId < partitionNum; pId++) {
		ss << "[P" << std::setfill('0') << std::setw(partitionDigit_) << pId
		<< "]";
		for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
			if (isMaster()) {
				ss << std::setw(lsnDigit_) << getLSN(pId, nodeId);
			}
			else {
				if (nodeId == 0) {
					ss << std::setw(lsnDigit_) << getLSN(pId, nodeId);
				}
				else {
					ss << std::setw(lsnDigit_) << 0;
				}
			}
			if (nodeId != nodeNum_ - 1) ss << ",";
		}
		ss << terminateCode;
	}

	ss << terminateCode << "[DETAIL LIST]" << terminateCode;
	int32_t revisionDigit = 0;
	{
			PartitionRevisionNo maxRevision = -1;
			NodeId nodeId;
			for (pId = 0; pId < partitionNum; pId++) {
				if (isMaster()) {
				for (nodeId = 0; nodeId < nodeNum_; nodeId++) {
					PartitionRevisionNo revision = getPartitionRevision(pId, nodeId);
					if (revision > maxRevision) {
						maxRevision = revision;
					}
				}
				}
			}
			for (; maxRevision != 0; revisionDigit++) {
				maxRevision /= 10;
			}
	}

	ss << terminateCode << "[STATUS LIST]" << terminateCode;
	for (pId = 0; pId < partitionNum; pId++) {
		ss << "[P" << std::setfill('0') << std::setw(partitionDigit_) << pId
		<< "]";
		if (isMaster()) {
			for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
				PartitionStatus pstat = getPartitionStatus(pId, nodeId);
				ss << dumpPartitionStatusEx(pstat);
				if (nodeId != nodeNum_ - 1) ss << ",";
			}
		}
		else {
			PartitionStatus pstat = getPartitionStatus(pId, 0);
			ss << dumpPartitionStatusEx(pstat);
		}
		ss << terminateCode;
	}

	ss << terminateCode << "[ROLE LIST]" << terminateCode;
	for (pId = 0; pId < partitionNum; pId++) {
		ss << "[P" << std::setfill('0') << std::setw(partitionDigit_) << pId
		<< "]";
		if (isMaster()) {
			for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
				PartitionRoleStatus prolestat = getPartitionRoleStatus(pId, nodeId);
				ss << dumpPartitionRoleStatusEx(prolestat);
				if (nodeId != nodeNum_ - 1) ss << ",";
			}
		}
		else {
			PartitionRoleStatus prolestat = getPartitionRoleStatus(pId, 0);
			ss << dumpPartitionRoleStatusEx(prolestat);
		}
		ss << terminateCode;
	}
	ss << terminateCode << "[REVISION LIST]" << terminateCode;
	for (pId = 0; pId < partitionNum; pId++) {
		ss << "[P" << std::setfill('0') << std::setw(partitionDigit_) << pId
		<< "]";
		if (isMaster()) {
			for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
				PartitionRevisionNo revision = getPartitionRevision(pId, nodeId);
				ss << revision;
				if (nodeId != nodeNum_ - 1) ss << ",";
			}
		}
		else {
			PartitionRevisionNo revision = getPartitionRevision(pId, 0);
			ss << revision;
		}
		ss << terminateCode;
	}
	isDetail=true;
	if (isDetail) {
		ss << terminateCode << "[START LSN LIST]" << terminateCode;
		for (pId = 0; pId < partitionNum; pId++) {
			ss << "[P" << std::setfill('0') << std::setw(partitionDigit_) << pId
			<< "]";
			for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
				if (isMaster()) {
					ss << std::setw(lsnDigit_) << getStartLSN(pId, nodeId);
				}
				else {
					if (nodeId == 0) {
						ss << std::setw(lsnDigit_) << getStartLSN(pId, nodeId);
					}
					else {
						ss << std::setw(lsnDigit_) << 0;
					}
				}
				if (nodeId != nodeNum_ - 1) ss << ",";
			}
			ss << terminateCode;
		}
	}
	return ss.str();
}

void PartitionTable::getServiceAddress(
	NodeId nodeId, picojson::value &result, ServiceType addressType) {
	result = picojson::value(picojson::object());
	picojson::object &resultObj = result.get<picojson::object>();

	if (nodeId != UNDEF_NODEID) {
		NodeAddress address = getNodeInfo(nodeId).getNodeAddress(addressType);
		resultObj["address"] = picojson::value(address.toString(false));
		resultObj["port"] = picojson::value(static_cast<double>(address.port_));
	}
	else {
		resultObj["address"] = picojson::value("undef");
		resultObj["port"] = picojson::value("undef");
	}
}

std::string PartitionRole::dumpIds() {
	util::NormalOStringStream ss1;
	util::NormalOStringStream ss2;
	util::NormalOStringStream ss3;
	ss2 << "{";
	for (size_t pos = 0; pos < backups_.size(); pos++) {
		ss2 << backups_[pos];
		if (pos != backups_.size() - 1) {
			ss2 << ",";
		}
	}
	ss2 << "}";
	ss3 << "{";
	for (size_t pos = 0; pos < catchups_.size(); pos++) {
		ss3 << catchups_[pos];
		if (pos != catchups_.size() - 1) {
			ss3 << ",";
		}
	}
	ss3 << "}";
	ss1 << " {pId=" << pId_ << ", owner=" << ownerNodeId_
		<< ", backups=" << ss2.str() << ", catchups=" << ss3.str() << ", revision=" << revision_.toString()
		<< "}";

	return ss1.str();
}

std::string PartitionRole::dump() {
	util::NormalOStringStream ss1;
	util::NormalOStringStream ss2;
	util::NormalOStringStream ss3;

	ss2 << "[";
	for (size_t pos = 0; pos < backupAddressList_.size(); pos++) {
		ss2 << backupAddressList_[pos].toString();
		if (pos < backups_.size()) {
			ss2 << "(";
			ss2 << backups_[pos];
			ss2 << ")";
		}
		if (pos != backupAddressList_.size() - 1) {
			ss2 << ",";
		}
	}
	ss2 << "]";

	ss3 << "[";
	for (size_t pos = 0; pos < catchupAddressList_.size(); pos++) {
		ss3 << catchupAddressList_[pos].toString();
		if (pos < catchups_.size()) {
			ss3 << "(";
			ss3 << catchups_[pos];
			ss3 << ")";
		}
		if (pos != catchupAddressList_.size() - 1) {
			ss3 << ",";
		}
	}
	ss3 << "]";

	ss1 << " {pId=" << pId_ << ", ownerAddress:" << ownerAddress_.toString()
		<< "(" << ownerNodeId_ << ")";
		if (backups_.size() > 0) {
			ss1 << ", backupAddressList:" << ss2.str();
		}
		if (catchups_.size() > 0) {
			ss1 << ", catchupAddressList:" << ss3.str();
		}
		ss1 << ", ptRev:" << revision_.toString() << "}";

	return ss1.str();
}


std::string PartitionRole::dump(PartitionTable *pt, ServiceType serviceType) {
	util::NormalOStringStream ss1;
	util::NormalOStringStream ss2;
	util::NormalOStringStream ss3;

	ss2 << "[";
	for (size_t pos = 0; pos < backups_.size(); pos++) {
		NodeAddress &address =
			pt->getNodeAddress(backups_[pos], serviceType);
		ss2 << address.toString();
		if (pos != backups_.size() - 1) {
			ss2 << ",";
		}
	}
	ss2 << "]";

	ss3 << "[";
	for (size_t pos = 0; pos < catchups_.size(); pos++) {
		NodeAddress &address =
			pt->getNodeAddress(catchups_[pos], serviceType);
		ss3 << address.toString();
		if (pos != catchups_.size() - 1) {
			ss3 << ",";
	}
	}
	ss3 << "]";

	ss1 << " {pId=" << pId_ << ", ownerAddress=";
	if (ownerNodeId_ == UNDEF_NODEID) {ss1 << "UNDEF";}
	else pt->getNodeAddress(ownerNodeId_, serviceType);
	ss1 << "(" << ownerNodeId_ << ")";
	if (backups_.size() > 0) {
		ss1 << ", backupAddressList=" << ss2.str();
	}
	if (catchups_.size() > 0) {
		ss1 << ", catchupAddressList=" << ss3.str();
	}
	ss1 << ", revision=" << revision_.toString() << "}";

	return ss1.str();
}


template <typename T1, typename T2>
void PartitionTable::getActiveNodeList(T1 &activeNodeList,  T2 &downNodeSet,
		int64_t currentTime) {
	if (!isFollower()) {
		int32_t nodeNum = getNodeNum();
		activeNodeList.reserve(nodeNum);
		if (getLiveNodeIdList(
				activeNodeList, currentTime, true) && isMaster()) {
			getDownNodes(downNodeSet, false);
		}
	}
}

template void PartitionTable::getActiveNodeList(std::vector<NodeId> &activeNodeList,
		util::Set<NodeId> &downNodeList, int64_t currentTime);

void PartitionTable::changeClusterStatus(ClusterStatus status,
		NodeId masterNodeId, int64_t baseTime, int64_t nextHeartbeatTimeout) {
	switch (status) { 
		case SUB_MASTER:
			clearCurrentStatus();
			setMaster(UNDEF_NODEID);
			resetBlockQueue();
			resizeCurrenPartition(0);
		break;
		case MASTER:
			setMaster(SELF_NODEID);
		break;
		case FOLLOWER: {
			setHeartbeatTimeout(SELF_NODEID, nextHeartbeatTimeout);
			setHeartbeatTimeout(masterNodeId, nextHeartbeatTimeout);
			int32_t nodeNum = getNodeNum();
			if (nodeNum > 1) {
				for (NodeId target = 1; target < nodeNum; target++) {
					if (target != masterNodeId) {
						setHeartbeatTimeout(target, UNDEF_TTL);
					}
				}
			}
			setMaster(masterNodeId);
		}
		break;
		default:
		break;
	}
}

void PartitionTable::resetDownNode(NodeId targetNodeId) {
	for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
		PartitionRole currentRole;
		getPartitionRole(pId, currentRole);
		NodeId currentOwner = currentRole.getOwner();
		bool isDown = false;
		if (currentOwner != UNDEF_NODEID) {
			if (nodes_[currentOwner].heartbeatTimeout_ == UNDEF_TTL) {
				currentOwner = UNDEF_NODEID;
				isDown = true;
			}
		}
		std::vector<NodeId> currentBackups = currentRole.getBackups();
		std::vector<NodeId> currentCatchups = currentRole.getCatchups();
		std::vector<NodeId> tmpBackups, tmpCatchups;
		for (size_t pos = 0; pos < currentBackups.size(); pos++) {
			if (nodes_[currentBackups[pos]].heartbeatTimeout_ == UNDEF_TTL) {
				isDown = true;
			}
			else {
				tmpBackups.push_back(currentBackups[pos]);
			}
		}
		for (size_t pos = 0; pos < currentCatchups.size(); pos++) {
			if (nodes_[currentCatchups[pos]].heartbeatTimeout_ == UNDEF_TTL) {
				isDown = true;
			}
			else {
				tmpCatchups.push_back(currentCatchups[pos]);
			}
		}
		if (isDown) {
			currentRole.set(currentOwner, tmpBackups, tmpCatchups);
			setPartitionRole(pId, currentRole);
		}
		setPartitionStatus(pId, PT_OFF, targetNodeId);
		setPartitionRoleStatus(pId, PT_NONE, targetNodeId);
	}
}

void PartitionTable::resizeCurrenPartition(int32_t resizeNum) {
	bool withCheck = false;
	if (isFollower()) {
		resizeNum = 1;
	}
	else {
		if (resizeNum == 0) {
			resizeNum = nodeNum_;
		}
		withCheck = true;
	}
	for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
		util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
		currentPartitions_[pId].resize(resizeNum, withCheck);
	}
}

bool PartitionTable::checkNextOwner(PartitionId pId, NodeId checkedNode,
		LogSequentialNumber checkedLsn, LogSequentialNumber checkedMaxLsn,
		NodeId currentOwner) {
	if (currentOwner == UNDEF_NODEID) {
		bool check1 = (checkedMaxLsn <= checkedLsn);
		bool check2 = false;
		if (!isAvailable(pId)) {
			check2 = true;
		}
		else {
			check2 = true;
		}
		return (check1 && check2);
	}
	else {
		bool check1 = (isOwnerOrBackup(pId, checkedNode));
		bool check2 = !checkLimitOwnerBackupLsnGap(checkedMaxLsn, checkedLsn);
		if (check1 && check2) {
			if (currentOwner != checkedNode) {
				LogSequentialNumber startLsn = getStartLSN(pId, currentOwner);
				if (checkedLsn >= startLsn) {
					return true;
				}
			}
			else {
				return true;
			}
		}
		return false;
	}
}

bool PartitionTable::checkNextBackup(
		PartitionId pId, NodeId checkedNode, LogSequentialNumber checkedLsn,
		LogSequentialNumber currentOwnerStartLsn, LogSequentialNumber currentOwnerLsn) {

	bool backupCond1 = !checkLimitOwnerBackupLsnGap(currentOwnerLsn, checkedLsn);
	bool backupCond2 = (checkedLsn >= currentOwnerStartLsn);
	return (backupCond1 && backupCond2);
}

void SubPartitionTable::set(PartitionTable *pt, PartitionId pId,
	PartitionRole &role, NodeId ownerNodeId) {
	NodeAddress ownerAddress;
	role.checkBackup();
	role.checkCatchup();
	if (ownerNodeId != UNDEF_NODEID) {
		ownerAddress = pt->getNodeAddress(ownerNodeId);
	}
	SubPartition subPartition(pId, role, ownerAddress);
	PartitionRole &tmpRole = subPartition.role_;
	tmpRole.encodeAddress(pt, role);
	subPartitionList_.push_back(subPartition);
}

void PartitionTable::planGoal(PartitionContext &context) {

	util::StackAllocator &alloc = *context.alloc_;
	util::XArray<NodeId> nodeIdList(alloc);
	getLiveNodeIdList(nodeIdList);
	nextApplyRuleLimitTime_ = UNDEF_TTL;
	currentRuleNo_ = PARTITION_RULE_UNDEF;
	for (int32_t i = 0; i < getNodeNum(); i++) {
		if (getHeartbeatTimeout(i) == UNDEF_TTL) {
			resetDownNode(i);
		}
	}

	for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
		clearCatchupRole(pId);
	}

	util::Vector<util::Vector<ScoreInfo> > scoreTable(alloc);
	util::Vector<PartitionRole> roleList(alloc);

	try {
		prepareGoalTable(alloc, nodeIdList, scoreTable, roleList);
		assignGoalTable(alloc, nodeIdList, scoreTable, roleList);
		modifyGoalTable(alloc, nodeIdList, roleList);

		setGoal(this, roleList);
		GS_TRACE_CLUSTER_INFO(dumpPartitionsNew(alloc, PT_CURRENT_GOAL));
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e,"");
	}
}

void PartitionTable::prepareGoalTable(
		util::StackAllocator &alloc,
		util::XArray<NodeId> &nodeIdList,
		util::Vector<util::Vector<ScoreInfo> > &scoreTable,
		util::Vector<PartitionRole> &roleList) {
	size_t pos;
	PartitionId pId;
	int64_t baseValue = util::DateTime::now(false).getUnixTime();
	for (pos = 0; pos < nodeIdList.size(); pos++) {
		util::Vector<ScoreInfo> score(alloc);
		scoreTable.push_back(score);
		util::Random random(static_cast<int64_t>(baseValue + pos));
		for (pId = 0; pId < getPartitionNum(); pId++) {
			ScoreInfo info(0, pId);
			info.randVal_ = random.nextInt32();
			scoreTable[pos].push_back(info);
		}
	}
	for (pId = 0; pId < getPartitionNum(); pId++) {
		PartitionRole role(pId, revision_, PT_CURRENT_OB);
		roleList.push_back(role);
		LogSequentialNumber maxLsn = 0;
		double maxLsnCount = 0;
		for (pos = 0; pos < nodeIdList.size(); pos++) {
			LogSequentialNumber currentLsn = getLSN(pId, nodeIdList[pos]);
			if (currentLsn > maxLsn) {
				maxLsn = currentLsn;
			}
		}
		for (pos = 0; pos < nodeIdList.size(); pos++) {
			LogSequentialNumber currentLsn = getLSN(pId, nodeIdList[pos]);
			if (currentLsn == maxLsn || isOwnerOrBackup(pId, nodeIdList[pos])) {
				maxLsnCount++;
			}
		}
		double currentScore = 0;
		for (pos = 0; pos < nodeIdList.size(); pos++) {
			currentScore = (1 / maxLsnCount);
			LogSequentialNumber currentLsn = getLSN(pId, nodeIdList[pos]);
			if (currentLsn == maxLsn || isOwnerOrBackup(pId, nodeIdList[pos])) {
				scoreTable[pos][pId].score_ = currentScore;
			}
			else {
				scoreTable[pos][pId].assigned_ = false;
			}
		}
	}
}

void PartitionTable::assignGoalTable(
		util::StackAllocator &alloc,
		util::XArray<NodeId> &nodeIdList,
		util::Vector<util::Vector<ScoreInfo> > &scoreTable,
		util::Vector<PartitionRole> &roleList) {
	int32_t assignedCount = 0;
	int32_t nodeNum = nodeIdList.size();
	int32_t backupNum = getReplicationNum();
	if (backupNum > nodeNum - 1) {
		backupNum = nodeNum - 1;
	}
	int32_t targetCount = (1 + backupNum) * getPartitionNum(); 
//	int32_t ownerTargetCount = getPartitionNum();
//	NodeId targetNodePos = 0;
//	int32_t phase = 0;
//	int32_t ownerCheckLimit = getPartitionNum() / nodeNum + 1;
//	int32_t ownerCheckCounter = 0;
//	bool ownerCheckMode =  true;
	util::Vector<int32_t> countList(nodeNum, 0, alloc);
	util::Vector<int32_t> ownerCountList(nodeNum, 0, alloc);
	bool allOwnerAssinged = false;
	int32_t loop = 0;
	int32_t slackCount = 10;
	int32_t loopLimit = (targetCount/nodeNum) + 1 + slackCount;
	util::Vector<util::Vector<ScoreInfo> > scorePartitionList(alloc);
	scorePartitionList = scoreTable;
	for (size_t pos = 0; pos < nodeIdList.size(); pos++) {
		std::sort(scorePartitionList[pos].begin(), 
				scorePartitionList[pos].end(), cmpScoreNode);
	}
	while (true) {
		util::Vector<ScoreNode> scoreNodeList(alloc);
		util::Vector<double> scoreSumList(getPartitionNum(), 0, alloc);
		for (size_t pos = 0; pos < static_cast<size_t>(nodeNum); pos++) {
			ScoreNode node(0, pos, countList[pos], ownerCountList[pos]);
			scoreNodeList.push_back(node);
			for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
				scoreNodeList[pos].score_ += scoreTable[pos][pId].score_;
				scoreSumList[pId] += scoreTable[pos][pId].score_;
			}
		}
		std::sort(scoreNodeList.begin(), scoreNodeList.end(), cmpScore);
		size_t assignedNodePos = -1;
		if (!allOwnerAssinged) {
		for (size_t pos = 0; pos < scoreNodeList.size(); pos++) {
			size_t nodePos = scoreNodeList[pos].nodeId_;
			util::Vector<ScoreInfo>::iterator it = scorePartitionList[nodePos].begin();
			int32_t assingPos = -1;
			for (size_t i = 0; i < scorePartitionList[nodePos].size(); i++) {
				PartitionId targetPId = scorePartitionList[nodePos][i].pId_; 
				PartitionRole &role = roleList[targetPId];
//				LogSequentialNumber maxLsn = getMaxLsn(targetPId);
				if (scoreTable[nodePos][targetPId].assigned_) {
					continue;
				}
//				std::vector<NodeId> &backups = role.getBackups();
				if (role.getOwner() == UNDEF_NODEID) {
					role.setOwner(nodeIdList[nodePos]);
					countList[nodePos]++;
					ownerCountList[nodePos]++;
					assignedCount++;
					assingPos = i;
					assignedNodePos = pos;
					scoreTable[nodePos][targetPId].assigned_ = true;
					scorePartitionList[nodePos].erase(it + i);
					break;
				}
			}
				if (static_cast<uint32_t>(assignedCount) == getPartitionNum()) {
					allOwnerAssinged = true;
					break;
				}
			}
		}
		for (size_t pos = assignedNodePos + 1; pos < scoreNodeList.size(); pos++) {
			size_t nodePos = scoreNodeList[pos].nodeId_;
			util::Vector<ScoreInfo>::iterator it = scorePartitionList[nodePos].begin();
			int32_t assingPos = -1;
			bool assigned = false;
			for (size_t i = 0; i < scorePartitionList[nodePos].size(); i++) {
				PartitionId targetPId = scorePartitionList[nodePos][i].pId_; 
				PartitionRole &role = roleList[targetPId];
				if (scoreTable[nodePos][targetPId].assigned_) {
					continue;
				}
				std::vector<NodeId> &backups = role.getBackups();
				if (role.getBackupSize() >= backupNum) {
					continue;
				}
				else {
					assigned = true;
					backups.push_back(nodeIdList[nodePos]);
					countList[nodePos]++;
				}
				assingPos = i;
				assignedCount++;
				scoreTable[nodePos][targetPId].assigned_ = true;
				if (role.getBackupSize() == backupNum) {
					scorePartitionList[nodePos].erase(it + assingPos);
				}
				break;
			}
			if (assignedCount >= targetCount) {
				break;
			}
			if (!assigned) {
				modifyBackup(alloc, nodeIdList, nodePos, scoreTable, countList, roleList);
			}
		}
		if (assignedCount >= targetCount) {
			break;
		}
		loop++;
		if (loop > loopLimit) {
			break;
		}
	}
}

void PartitionTable::modifyBackup(
	util::StackAllocator &alloc,
	util::XArray<NodeId> &nodeIdList,
	int32_t nodePos,
	util::Vector<util::Vector<ScoreInfo> > &scoreTable,
	util::Vector<NodeId> &countList,
	util::Vector<PartitionRole> &roleList) {
	int32_t maxCount = -1;
	int32_t checkNodeId = -1;
	int32_t nodeNum = nodeIdList.size();
	for (int32_t p = 0; p < nodeNum; p++) {
		if (countList[p] > maxCount) {
			checkNodeId = p;
			maxCount = countList[p];
		}
	}
	if (checkNodeId == -1) return;
	util::XArray<PartitionId> pIdList(alloc);
	for (int32_t p = 0; static_cast<uint32_t>(p) < getPartitionNum(); p++) {
		pIdList.push_back(p);
	}
	util::Random random(static_cast<int64_t>(util::DateTime::now(false).getUnixTime()));
	arrayShuffle(pIdList,
			static_cast<uint32_t>(pIdList.size()), random);
	for (int32_t q = 0; static_cast<uint32_t>(q) < getPartitionNum(); q++) {
		PartitionId currentPId = pIdList[q];
		PartitionRole &currentRole = roleList[currentPId];
		currentRole.checkBackup();
		if (currentRole.isOwnerOrBackup(nodeIdList[nodePos])) {
			continue;
		}
		if (!currentRole.isBackup(nodeIdList[checkNodeId])) {
			continue;
		}
		PartitionRole swapRole = currentRole;
		std::vector<NodeId> &backups = currentRole.getBackups();
		std::vector<NodeId> &tmpBackups = swapRole.getBackups();
		tmpBackups.clear();	
		tmpBackups.push_back(nodeIdList[nodePos]);
		for (int32_t k = 0; static_cast<size_t>(k) < backups.size(); k++) {
			if (backups[k] != nodeIdList[checkNodeId]) {
				tmpBackups.push_back(backups[k]);								
			}
		}
		roleList[currentPId] = swapRole;
		scoreTable[nodePos][currentPId].assigned_ = true;
		countList[nodePos]++;
		countList[checkNodeId]--;
		break;
	}
}

void PartitionTable::modifyGoalTable(
	util::StackAllocator &alloc,
	util::XArray<NodeId> &nodeIdList,
	util::Vector<PartitionRole> &roleList) {
	int32_t nodeNum = nodeIdList.size();
	LoadSummary loadSummary(alloc, getNodeNum());
	loadSummary.init(nodeNum);
	util::XArray<uint32_t> &ownerLoadList = loadSummary.ownerLoadList_;
	util::XArray<uint32_t> &dataLoadList = loadSummary.dataLoadList_;
	uint32_t minOwnerLoad, maxOwnerLoad, minDataLoad, maxDataLoad;
	for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
		NodeId owner = roleList[pId].getOwner();
		if (owner != UNDEF_NODEID) {
			loadSummary.addOwner(owner);
		}
		std::vector<NodeId> &backups = roleList[pId].getBackups();
		for (size_t pos = 0; pos < backups.size(); pos++) {
			loadSummary.addBackup(backups[pos]);
		}
	}
	util::XArray<CatchupOwnerNodeInfo> ownerOrderList(alloc);
	sortOrderList(ownerOrderList, nodeIdList, ownerLoadList,
		dataLoadList, maxOwnerLoad, minOwnerLoad, maxDataLoad, minDataLoad,
		cmpCatchupOwnerNode);

	bool modify = false;
	bool check1 = false;
	int32_t modifyLoopCount = 1;
	if (maxOwnerLoad - minOwnerLoad > 2) {
		for (int32_t k = 0; k < modifyLoopCount;k++) {
			check1 = true;
			util::XArray<PartitionId> pIdList(alloc);
			for (int32_t p = 0; static_cast<uint32_t>(p) < getPartitionNum(); p++) {
				pIdList.push_back(p);
			}
			util::Random random(static_cast<int64_t>(util::DateTime::now(false).getUnixTime()));
			arrayShuffle(pIdList,
					static_cast<uint32_t>(pIdList.size()), random);

			for (size_t pos = 0; pos < pIdList.size(); pos++) {
				PartitionId pId = pIdList[pos];
				NodeId owner = roleList[pId].getOwner();
				if (owner == UNDEF_NODEID) continue;
//				int32_t ownerCount = ownerLoadList[owner];
				std::vector<NodeId> &backups = roleList[pId].getBackups();
				std::vector<NodeId> &catchups = roleList[pId].getCatchups();
				if (catchups.size() > 0) {
					continue;
				}
				int32_t ownerMaxPos = 0;
				int32_t ownerMinPos = 0;
				int32_t ownerMaxCount = ownerLoadList[0];
				int32_t ownerMinCount = ownerLoadList[0];
				for (size_t j = 0; j < ownerLoadList.size(); j++) {
					if (!checkLiveNode(j)) continue;
					if (ownerLoadList[j] > static_cast<uint32_t>(ownerMaxCount)) {
						ownerMaxCount = ownerLoadList[j];
						ownerMaxPos = j;
					}
					if (ownerLoadList[j] < static_cast<uint32_t>(ownerMinCount)) {
						ownerMinCount = ownerLoadList[j];
						ownerMinPos = j;
					}
				}
				if (owner != ownerMaxPos) {
					continue;
				}
				bool find = true;
				for (size_t pos = 0; pos < backups.size(); pos++) {
					if (backups[pos] == ownerMinPos) {
					}
					else {
						find = false;
				}
					}
				if (!find) {
					continue;
				}
				ownerLoadList[ownerMaxPos]--;
				ownerLoadList[ownerMinPos]++;
				NodeId swapOwner = ownerMinPos;
					std::vector<NodeId> swapBackups;
				swapBackups.push_back(ownerMaxPos);
					for (size_t i = 0; i < backups.size(); i++) {
						if (backups[i] != swapOwner) {
							swapBackups.push_back(backups[i]);
						}
					}
					roleList[pId].clear();
					roleList[pId].set(swapOwner, swapBackups, catchups);
				if (ownerLoadList[ownerMaxPos] - ownerLoadList[ownerMinPos] <= 2) {
						modify = true;
						break;
					}
				}
			}
		}
}

bool PartitionTable::applyInitialRule(PartitionContext &context) {
	SubPartitionTable &subPartitionTable = context.subPartitionTable_;
	subPartitionTable.clear();
//	bool isConfigurationChange = context.isConfigurationChange();;
	util::NormalOStringStream strstrm;
	for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
		LogSequentialNumber maxLsn = getMaxLsn(pId);
		if (getOwner(pId) == UNDEF_NODEID) {
			PartitionRole nextRole(pId, revision_, PT_CURRENT_OB);
			PartitionRole goalRole;
			getPartitionRole(pId, goalRole, PT_CURRENT_GOAL);
			NodeId goalOwner = goalRole.getOwner();
			LogSequentialNumber goalLsn = getLSN(pId, goalOwner);
//			NodeId targetNodeId = -1;
			if (goalLsn >= maxLsn && context.isLoadBalance_) {
				std::vector<NodeId> &nextBackups = nextRole.getBackups();
				std::vector<NodeId> &goalBackups = goalRole.getBackups();
				nextRole.setOwner(goalOwner);
				LogSequentialNumber ownerLsn = getLSN(pId, goalOwner);
				LogSequentialNumber ownerStartLsn = getStartLSN(pId, goalOwner);
				for (size_t pos = 0; pos < goalBackups.size(); pos++) {
					NodeId candBackup = goalBackups[pos];
					if (nextBlockQueue_.find(pId, candBackup)) {
						continue;
					}
					LogSequentialNumber backupLSN = getLSN(pId, candBackup);
					if (checkNextBackup(pId, candBackup, backupLSN, ownerStartLsn, ownerLsn)) {
						nextBackups.push_back(candBackup);
					}
				}
				subPartitionTable.set(this, pId, nextRole, goalOwner);
			}
			else {
				NodeId targetNodeId = -1;
				LogSequentialNumber tmpMaxLSN = 0;
				NodeId backupId = UNDEF_NODEID;
				util::Random random(static_cast<int64_t>(util::DateTime::now(false).getUnixTime()));
				util::XArray<NodeId> activeNodeList(*context.alloc_);
				getLiveNodeIdList(activeNodeList);	
				arrayShuffle(activeNodeList,
						static_cast<uint32_t>(activeNodeList.size()), random);

				for (size_t pos = 0; pos < activeNodeList.size(); pos++) {
					NodeId nodeId = activeNodeList[pos];
					LogSequentialNumber lsn = getLSN(pId, nodeId);
					if (lsn >= maxLsn) {
						targetNodeId = nodeId;
						if (isBackup(pId, nodeId)) {
							backupId = nodeId;
						}
					}
					if (lsn >= tmpMaxLSN) {
						tmpMaxLSN = lsn;
					}
				}
				if (targetNodeId == UNDEF_NODEID) {
					util::NormalOStringStream strstrm;
					try {
						strstrm << "Target partition owner is not assgined, pId="
								<< pId << ", expected maxLSN=" << maxLsn
								<< ", actual maxLSN=" << tmpMaxLSN;
						GS_THROW_USER_ERROR(GS_ERROR_CS_PARTITION_OWNER_NOT_ASSGIN,
								strstrm.str().c_str());
					}
					catch (std::exception &e) {
						UTIL_TRACE_EXCEPTION_WARNING(CLUSTER_OPERATION, e, "");
					}
					clearRole(pId);
					setPartitionRoleStatus(pId, PT_NONE);
					setPartitionStatus(pId, PT_OFF);
					continue;
				}
				if (!context.isLoadBalance_) {
					if (backupId != UNDEF_NODEID) {
						targetNodeId = backupId;
					}
				}
				nextRole.setOwner(targetNodeId);
				std::vector<NodeId> &nextBackups = nextRole.getBackups();
				std::vector<NodeId> &goalBackups = goalRole.getBackups();
				std::vector<NodeId> checkRole = goalBackups;
				if (context.isLoadBalance_) {
					if (targetNodeId != goalOwner) {
						checkRole.push_back(goalOwner);
					}
					LogSequentialNumber ownerLsn = getLSN(pId, targetNodeId);
					LogSequentialNumber ownerStartLsn = getStartLSN(pId, targetNodeId);
					for (size_t pos = 0; pos < checkRole.size(); pos++) {
						NodeId candBackup = checkRole[pos];
						if (candBackup == targetNodeId) continue;
						if (nextBlockQueue_.find(pId, candBackup)) {
							continue;
						}
						LogSequentialNumber backupLSN = getLSN(pId, candBackup);
						if (checkNextBackup(pId, candBackup, backupLSN, ownerStartLsn, ownerLsn)) {
							nextBackups.push_back(candBackup);
						}
					}
				}
				subPartitionTable.set(this, pId, nextRole, targetNodeId);
			}
		}
		else if (context.isConfigurationChange()) {
			PartitionRole goalRole;
			getPartitionRole(pId, goalRole, PT_CURRENT_GOAL);
			PartitionRole currentRole;
			getPartitionRole(pId, currentRole);
			std::vector<NodeId> &goalBackups = goalRole.getBackups();
			std::vector<NodeId> &nextBackups = currentRole.getBackups();
			currentRole.setRevision(revision_);
			currentRole.clearCatchup();
			int32_t backupNum = currentRole.getBackupSize();
			if (backupNum < context.backupNum_ &&  context.isLoadBalance_) {
				std::vector<NodeId> checkRole = goalBackups;
				NodeId ownerNodeId = currentRole.getOwner();
				LogSequentialNumber ownerLsn = getLSN(pId, ownerNodeId);
				LogSequentialNumber ownerStartLsn = getStartLSN(pId, ownerNodeId);
				if (goalRole.getOwner() != ownerNodeId) {
					checkRole.push_back(goalRole.getOwner());
				}
				for (size_t pos = 0; pos < checkRole.size(); pos++) {
					NodeId candBackup = checkRole[pos];
					if (!currentRole.isOwnerOrBackup(candBackup)) {
						LogSequentialNumber backupLSN = getLSN(pId, candBackup);
						if (checkNextBackup(pId, candBackup, backupLSN, ownerStartLsn, ownerLsn)) {
							nextBackups.push_back(candBackup);
						}
					}
					if (nextBackups.size() >= static_cast<size_t>(context.backupNum_)) {
						break;
					}
				}
			}
			subPartitionTable.set(this, pId, currentRole, getOwner(pId));
		}
	}
	if (subPartitionTable.size() > 0) {
		prevCatchupPId_ = UNDEF_PARTITIONID;
		return true;
	}
	else {
		return false;
	}
}

bool PartitionTable::applyAddRule(PartitionContext &context) {
	SubPartitionTable &subPartitionTable = context.subPartitionTable_;	
	subPartitionTable.clear();
	util::StackAllocator &alloc = *context.alloc_;
	uint32_t partitionNum = getPartitionNum();
	int32_t targetBackupNum;
	int32_t replicationNum =  getReplicationNum();
	util::XArray<ReplicaInfo> resolvePartitonList(alloc);
	util::XArray<NodeId> liveNodeList(alloc);
	getLiveNodeIdList(liveNodeList);
	int32_t backupNum = 0;
	size_t currentNodeNum = liveNodeList.size();
	if (static_cast<size_t>(replicationNum) > currentNodeNum - 1) {
		targetBackupNum = currentNodeNum - 1;
	}
	else {
		targetBackupNum = replicationNum;
	}
	PartitionId pId;
	for (pId = 0; pId < partitionNum; pId++) {
		backupNum = getBackupSize(alloc, pId);
		ReplicaInfo repInfo(pId, backupNum);
		resolvePartitonList.push_back(repInfo);
	}
	std::sort(resolvePartitonList.begin(), resolvePartitonList.end(), cmpReplica);
	util::Vector<PartitionId> catchupPIdList(alloc);
	if (resolvePartitonList.size() > 1) {
		ReplicaInfo first(resolvePartitonList[0].pId_, resolvePartitonList[0].count_);
		if (resolvePartitonList[0].pId_ == prevCatchupPId_) {
			resolvePartitonList.erase(resolvePartitonList.begin());
			resolvePartitonList.push_back(first);
		}
	}
	for (size_t pos = 0; pos < resolvePartitonList.size(); pos++) {
		PartitionId targetPId = resolvePartitonList[pos].pId_;
		NodeId currentOwner = getOwner(targetPId);
		if (currentOwner == UNDEF_NODEID) {
			continue;
		}
		PartitionRole currentRole;
		getPartitionRole(targetPId, currentRole, PT_CURRENT_OB);
		if (currentRole.hasCatchupRole()) {
			catchupPIdList.push_back(targetPId);
		}
		PartitionRole nextRole(targetPId, revision_, PT_CURRENT_OB);
		PartitionRole goalRole;
		getPartitionRole(targetPId, goalRole, PT_CURRENT_GOAL);
		NodeId goalOwner = goalRole.getOwner();
		std::vector<NodeId> &currentBackups = currentRole.getBackups();
//		std::vector<NodeId> &nextBackups = nextRole.getBackups();
		std::vector<NodeId> &goalBackups = goalRole.getBackups();
		std::vector<NodeId> catchups;
		if (!currentRole.isOwnerOrBackup(goalOwner)) {
			if (goalBlockQueue_.find(targetPId, goalOwner)) {
				continue;
			}
			catchups.push_back(goalOwner);
			nextRole.set(currentOwner, currentBackups, catchups);
			subPartitionTable.set(this, targetPId, nextRole, currentOwner);
			prevCatchupPId_ = targetPId;
		}
		if (subPartitionTable.size() == 1) {
			break;
		}
		for (size_t pos = 0; pos < goalBackups.size(); pos++) {
			if (nextBlockQueue_.find(targetPId, goalBackups[pos])) {
				continue;
			}
			if (!currentRole.isOwnerOrBackup(goalBackups[pos])) {
				if (goalBlockQueue_.find(targetPId, goalBackups[pos])) {
					continue;
				}
				catchups.push_back(goalBackups[pos]);
				nextRole.set(currentOwner, currentBackups, catchups);
				subPartitionTable.set(this, targetPId, nextRole, currentOwner);
				prevCatchupPId_ = targetPId;
				break;
			}
		}
		if (subPartitionTable.size() == 1) {
			break;
		}
	}
	if (subPartitionTable.size() > 0) {
		PartitionId catchupPId = subPartitionTable.getSubPartition(0).pId_;
		for (size_t pos = 0; pos < catchupPIdList.size(); pos++) {
			PartitionId checkPId = catchupPIdList[pos];
			if (catchupPId != catchupPIdList[pos]) {
				PartitionRole nextRole;
				getPartitionRole(checkPId, nextRole, PT_CURRENT_OB);
				nextRole.clearCatchup();
				setPartitionRole(checkPId, nextRole, PT_CURRENT_OB);
				NodeId currentOwner = getOwner(checkPId);
				subPartitionTable.set(this, checkPId, nextRole, currentOwner);
			}
		}
		return true;
	}
	else {
		return false;
	}
}

bool PartitionTable::applyRemoveRule(PartitionContext &context) {
	SubPartitionTable &subPartitionTable = context.subPartitionTable_;	
	subPartitionTable.clear();
//	util::StackAllocator &alloc = *context.alloc_;
	for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
		NodeId currentOwner = getOwner(pId);
		if (currentOwner == UNDEF_NODEID) {
			continue;
		}
		PartitionRole currentRole, goalRole;
		getPartitionRole(pId, currentRole, PT_CURRENT_OB);
		getPartitionRole(pId, goalRole, PT_CURRENT_GOAL);
		PartitionRole nextRole(pId, revision_, PT_CURRENT_OB);
		NodeId goalOwner = goalRole.getOwner();
		std::vector<NodeId> &currentBackups = currentRole.getBackups();
		std::vector<NodeId> nextBackups;
		std::vector<NodeId> nextCatchups;
//		bool assigned = false;
		if (!currentRole.isOwnerOrBackup(goalOwner)) {
			continue;
		}
		bool diff = false;
		for (size_t pos = 0; pos < currentBackups.size(); pos++) {
			if (nextBlockQueue_.find(pId, currentBackups[pos])) {
				continue;
			}
			if (goalRole.isOwnerOrBackup(currentBackups[pos])) {
				nextBackups.push_back(currentBackups[pos]);
			}
			else {
				diff = true;
			}
		}
		if (diff && (nextBackups.size() >= static_cast<size_t>(context.backupNum_))) {
			nextRole.set(currentOwner, nextBackups, nextCatchups);
			subPartitionTable.set(this, pId, nextRole, currentOwner);
		}
	}

	if (subPartitionTable.size() > 0) {
		prevCatchupPId_ = UNDEF_PARTITIONID;
		return true;
	}
	else {
		return false;
	}
}

bool PartitionTable::applySwapRule(PartitionContext &context) {
	SubPartitionTable &subPartitionTable = context.subPartitionTable_;	
	subPartitionTable.clear();
//	util::StackAllocator &alloc = *context.alloc_;
	for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
		NodeId currentOwner = getOwner(pId);
		if (currentOwner == UNDEF_NODEID) {
			continue;
		}
		PartitionRole currentRole, goalRole;
		getPartitionRole(pId, currentRole, PT_CURRENT_OB);
		getPartitionRole(pId, goalRole, PT_CURRENT_GOAL);
		PartitionRole nextRole(pId, revision_, PT_CURRENT_OB);
		NodeId goalOwner = goalRole.getOwner();
		if (currentOwner == goalOwner) {
			continue;
		}
		std::vector<NodeId> &currentBackups = currentRole.getBackups();
		std::vector<NodeId> &goalBackups = goalRole.getBackups();

		std::vector<NodeId> nextBackups;
		std::vector<NodeId> nextCatchups;
		if (!currentRole.isOwnerOrBackup(goalOwner)) {
			continue;
		}
		bool flag = true;
		for (size_t pos = 0; pos < goalBackups.size(); pos++) {
			if (nextBlockQueue_.find(pId, goalBackups[pos])) {
				continue;
			}
			if (!currentRole.isOwnerOrBackup(goalBackups[pos])) {
				flag = false;
				break;
			}
		}
		if (!flag) {
			continue;
		}
		nextBackups.push_back(currentOwner);
		for (size_t pos = 0; pos < currentBackups.size(); pos++) {
			if (nextBlockQueue_.find(pId, currentBackups[pos])) {
				continue;
			}
			if (!goalRole.isOwnerOrBackup(currentBackups[pos])) {
				continue;
			}
			if (currentBackups[pos] != goalOwner) {
				nextBackups.push_back(currentBackups[pos]);
			}
		}
		nextRole.set(goalOwner, nextBackups, nextCatchups);
		subPartitionTable.set(this, pId, nextRole, currentOwner);
	}

	if (subPartitionTable.size() > 0) {
		prevCatchupPId_ = UNDEF_PARTITIONID;
		return true;
	}
	else {
		return false;
	}
}

void PartitionTable::planNext(PartitionContext &context) {
	util::StackAllocator &alloc = *context.alloc_;
	util::Vector<SubPartition> nextPartitionList(alloc);
	util::Vector<SubPartition> currentPartitionList(alloc);
	currentRuleNo_ = PARTITION_RULE_UNDEF;
	util::Vector<NodeId> activeNodeList(alloc);
	getLiveNodeIdList(activeNodeList);
	int32_t backupNum = getReplicationNum();
	int32_t liveNum = activeNodeList.size();
	nextApplyRuleLimitTime_ = context.currentRuleLimitTime_;
	if (backupNum > liveNum - 1) {
		context.backupNum_ = liveNum - 1;
	}
	else {
		context.backupNum_ = backupNum;
	}
//	int32_t applyRuleNo = -1;
	while (true) {
		if (applyInitialRule(context)) {
			currentRuleNo_ = PARTITION_RULE_INITIAL;
			nextApplyRuleLimitTime_ = context.currentRuleLimitTime_;
			break;
		}
		else if (!context.isLoadBalance_) {
			break;
		}
		else if (applySwapRule(context)) {
			currentRuleNo_ = PARTITION_RULE_SWAP;
			nextApplyRuleLimitTime_ = context.currentRuleLimitTime_;
			break;
		}
		else if (applyRemoveRule(context)) {
			currentRuleNo_ = PARTITION_RULE_REMOVE;
			nextApplyRuleLimitTime_ = context.currentRuleLimitTime_;
			break;
		}
		else if (applyAddRule(context)) {
			currentRuleNo_ = PARTITION_RULE_ADD;
			nextApplyRuleLimitTime_ = context.currentRuleLimitTime_;
			break;
		}
		else {
			currentRuleNo_ = PARTITION_RULE_UNDEF;
		}
		break;
	}
	PartitionRevisionNo revision = getPartitionRevisionNo();
	if (currentRuleNo_ != PARTITION_RULE_UNDEF) {
		GS_TRACE_CLUSTER_INFO(
			std::endl << "Apply Rule:" << dumpPartitionRule(currentRuleNo_) << ", revision=" << revision);
		SubPartitionTable &subPartitionTable = context.subPartitionTable_;	
		if (subPartitionTable.size() > 0) {
			currentRevisionNo_ = revision;
			context.needUpdatePartition_ = true;
			GS_TRACE_CLUSTER_INFO("Next partitions  : " << std::endl << dumpPartitionsNew(alloc, PT_CURRENT_OB, &subPartitionTable));
		}
	}	
	context.subPartitionTable_.revision_ = getPartitionRevision();
	std::vector<AddressInfo> &nodeList = context.subPartitionTable_.getNodeAddressList();
	getNodeAddressInfo(nodeList);
}

std::string DropNodeSet::dump() {
	util::NormalOStringStream oss;
	NodeAddress address(address_, port_);
	oss << "{address=" << address.dump();
	oss << ", dropInfo={";
	for (size_t pos = 0; pos < pIdList_.size(); pos++) {
		oss << "(pId=" << pIdList_[pos];
		oss << ", revision=" << revisionList_[pos] << ")";
		if (pos != pIdList_.size() - 1) {
			oss << ",";
		}
		else {
			oss << "}";
		}
	}
	oss << "}";
	return oss.str().c_str();
}

std::string DropPartitionNodeInfo::dump() {
	util::NormalOStringStream oss;
	for (size_t pos = 0; pos < dropNodeMap_.size(); pos++) {
		oss << dropNodeMap_[pos].dump();
	}
	return oss.str().c_str();
}
	
void PartitionTable::clearPartitionRole(PartitionId pId) {
	PartitionRole tmpRole(pId, revision_, PT_CURRENT_OB);
	setPartitionRole(pId, tmpRole);
}
	
void PartitionTable::clearCatchupRole(PartitionId pId) {
	PartitionRole currentRole;
	getPartitionRole(pId, currentRole, PT_CURRENT_OB);
	currentRole.clearCatchup();
	setPartitionRole(pId, currentRole, PT_CURRENT_OB);
}


bool PartitionTable::checkTargetPartition(PartitionId pId, bool &catchupWait) {
	PartitionRole currentRole;
	getPartitionRole(pId, currentRole);
	NodeId currentOwnerId = currentRole.getOwner();
	std::vector<NodeId> &currentBackups = currentRole.getBackups();
	std::vector<NodeId> &currentCatchups = currentRole.getCatchups();
	bool detectError = false;
//	PartitionRoleStatus roleStatus;
	if (currentOwnerId != UNDEF_NODEID 
			&& getPartitionRoleStatus(pId, currentOwnerId) != PT_OWNER) {
		detectError = true;
		GS_TRACE_CLUSTER_INFO("Unmatch owner role, ownerAddress=" 
			<< dumpNodeAddress(currentOwnerId)
			<< ", expected=OWNER, actual=" 
			<< dumpPartitionRoleStatus(getPartitionRoleStatus(pId, currentOwnerId)));
	}
	for (size_t pos = 0; pos < currentBackups.size(); pos++) {
		PartitionRoleStatus roleStatus = getPartitionRoleStatus(pId, currentBackups[pos]);
		if (roleStatus != PT_BACKUP) {
			if (roleStatus == PT_CATCHUP) {
				catchupWait = true;
			}
			else {
				detectError = true;
				GS_TRACE_CLUSTER_INFO("Unmatch backup role, backupAddress=" 
					<< dumpNodeAddress(currentBackups[pos])
					<< ", expected=BACKUP, actual=" 
					<< dumpPartitionRoleStatus(roleStatus));
			}
		}
	}
	for (size_t pos = 0; pos < currentCatchups.size(); pos++) {
		PartitionRoleStatus roleStatus = getPartitionRoleStatus(pId, currentCatchups[pos]);
		if (roleStatus != PT_CATCHUP) {
			if (roleStatus == PT_BACKUP) {
				PartitionRole currentRole;
				getPartitionRole(pId, currentRole);
				currentRole.promoteCatchup();
				setPartitionRole(pId, currentRole);
			}
			else {
				setBlockQueue(pId, PT_CHANGE_GOAL_TABLE, currentCatchups[pos]);
				detectError = true;
				GS_TRACE_CLUSTER_INFO("Unmatch catchup role, catchupAddress=" 
					<< dumpNodeAddress(currentCatchups[pos])
					<< ", expected=CATCHUP, actual=" 
					<< dumpPartitionRoleStatus(roleStatus));
			}
		}
		else {
			catchupWait = true;
		}
	}
	if (detectError) {
		clearPartitionRole(pId);
	}
	return detectError;
}

void PartitionTable::checkRepairPartition(PartitionContext &context) {
	util::StackAllocator &alloc = *context.alloc_;
	if (context.isRepairPartition_) {
		util::Vector<NodeId> liveNodeList(alloc);
		getLiveNodeIdList(liveNodeList);
		for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
			LogSequentialNumber maxLSN = 0;
			for (size_t pos = 0; pos < liveNodeList.size(); pos++) {
				LogSequentialNumber lsn = getLSN(pId, liveNodeList[pos]);
				if (lsn > maxLSN) {
					maxLSN = lsn;
				}
			}
			setMaxLsn(pId, maxLSN);
		}
	}
}
void PartitionTable::setBlockQueue(
	PartitionId pId, ChangeTableType tableType, NodeId nodeId) {
	if (pId >= getPartitionNum() || nodeId >= getNodeNum()) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_SET_INVALID_NODE_ID, "");
	}
	if (tableType == PT_CHANGE_NEXT_TABLE) {
		nextBlockQueue_.push(pId, nodeId);
	}
	else if (tableType == PT_CHANGE_GOAL_TABLE) {
		goalBlockQueue_.push(pId, nodeId);
	}
}

int32_t PartitionTable::checkRoleStatus(util::StackAllocator &alloc) {
	int32_t invalidCount = 0;
	for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
		PartitionRoleStatus roleStatus = getPartitionRoleStatus(pId);
		PartitionStatus status = getPartitionStatus(pId);
		if ((roleStatus == PT_OWNER || roleStatus == PT_BACKUP) && status == PT_OFF) {
			invalidCount++;
			continue;
		}
		if (roleStatus == PT_NONE && (status == PT_ON || status == PT_SYNC)) {
			invalidCount++;
			continue;
		}
	}
	return invalidCount;
}