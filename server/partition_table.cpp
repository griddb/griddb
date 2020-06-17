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
	@brief Implementation of PartitionTable
*/
#include "partition_table.h"
#include "config_table.h"
#include "picojson.h"
#include "json.h"
#include <iomanip>
#include <queue>

UTIL_TRACER_DECLARE(CLUSTER_SERVICE);
UTIL_TRACER_DECLARE(CLUSTER_OPERATION);
UTIL_TRACER_DECLARE(CLUSTER_DUMP);

#define GS_TRACE_CLUSTER_INFO(s) \
	GS_TRACE_INFO(CLUSTER_OPERATION, GS_TRACE_CS_CLUSTER_STATUS, s); 

#define GS_TRACE_CLUSTER_DUMP(s) \
	GS_TRACE_DEBUG(CLUSTER_OPERATION, GS_TRACE_CS_CLUSTER_STATUS, s); 

NodeAddress::NodeAddress(
		const char8_t *addr, uint16_t port) {

	util::SocketAddress address(addr, port);
	address.getIP(reinterpret_cast<util::SocketAddress::Inet *>
			(&address_), &port_);
}

void NodeAddress::set(
		const char8_t *addr, uint16_t port) {

	util::SocketAddress address(addr, port);
	address.getIP(reinterpret_cast<util::SocketAddress::Inet *>
			(&address_), &port_);
}

void NodeAddress::set(
		const util::SocketAddress &socketAddress) {

	socketAddress.getIP(
			reinterpret_cast<util::SocketAddress::Inet *>
					(&address_), &port_);
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
	case SQL_SERVICE:
		return sqlAddress_;
	default:
		GS_THROW_USER_ERROR(
				GS_ERROR_PT_INVALID_NODE_ADDRESS, "");
	}
}

std::string NodeAddress::toString(bool isPort) const {

	util::NormalOStringStream ss;
	util::SocketAddress::Inet *tmp
			= (util::SocketAddress::Inet *)&address_;

	ss << static_cast<int32_t>(tmp->value_[0]) << "."
			<< static_cast<int32_t>(tmp->value_[1]) << "."
			<< static_cast<int32_t>(tmp->value_[2]) << "." 
			<< static_cast<int32_t>(tmp->value_[3]);

	if (isPort) {
		ss << ":" << port_;
	}
	return ss.str();
}

/*!
	@brief Sets NodeAddress that correspond to a specified type
*/
void AddressInfo::setNodeAddress(
		ServiceType type, NodeAddress &address) {

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
	case SQL_SERVICE:
		sqlAddress_ = address;
		break;
	default:
		GS_THROW_USER_ERROR(
				GS_ERROR_PT_INVALID_NODE_ADDRESS, "");
	}
}

PartitionTable::PartitionTable(const ConfigTable &configTable) :
		fixedSizeAlloc_(
			util::AllocatorInfo(
				ALLOCATOR_GROUP_CS, "partitionTableFixed"), 1 << 18),
		globalStackAlloc_(
				util::AllocatorInfo(ALLOCATOR_GROUP_CS, "partitionTableStack"),
				&fixedSizeAlloc_),
		nodeNum_(0),
		masterNodeId_(UNDEF_NODEID),
		partitionLockList_(NULL),
		currentRuleNo_(PARTITION_RULE_UNDEF),
		config_(configTable, MAX_NODE_NUM),
		prevMaxNodeId_(0),
		nextBlockQueue_(
			configTable.get<int32_t>(CONFIG_TABLE_DS_PARTITION_NUM)),
		goalBlockQueue_(
				configTable.get<int32_t>(CONFIG_TABLE_DS_PARTITION_NUM)),
		prevCatchupPId_(UNDEF_PARTITIONID),
		prevCatchupNodeId_(UNDEF_NODEID),
		partitions_(NULL),
		nodes_(NULL),
		loadInfo_(globalStackAlloc_, MAX_NODE_NUM),
		currentRevisionNo_(1),
		prevDropRevisionNo_(0),
		isGoalPartition_(false),
		hasPublicAddress_(false) {

	try {
		
		uint32_t partitionNum = config_.partitionNum_;
		partitionLockList_
				= ALLOC_NEW(globalStackAlloc_) util::RWLock [partitionNum];
		partitions_
				= ALLOC_NEW(globalStackAlloc_) Partition [partitionNum];
		currentPartitions_
				= ALLOC_NEW(globalStackAlloc_) CurrentPartitions [partitionNum];

		NodeId maxNodeNum = config_.limitClusterNodeNum_;
		PartitionGroupConfig pgConfig(configTable);
		
		for (PartitionId pId = 0; pId < partitionNum; pId++) {
			
			partitions_[pId].init(globalStackAlloc_, pId,
					pgConfig.getPartitionGroupId(pId));
			currentPartitions_[pId].init();
		}
		
		nodes_ = ALLOC_NEW(globalStackAlloc_) NodeInfo [maxNodeNum];
		for (NodeId nodeId = 0; nodeId < maxNodeNum; nodeId++) {
			nodes_[nodeId].init(globalStackAlloc_);
		}

		maxLsnList_.assign(partitionNum, 0);
		nextBlockQueue_.init(globalStackAlloc_);
		goalBlockQueue_.init(globalStackAlloc_);

		int32_t digit = 0;
		for (; partitionNum != 0; digit++) {
			partitionNum /= 10;
		}

		partitionDigit_ = digit;
		lsnDigit_ = 10;
		goal_.init(this);

		PartitionRole initRole;
		newsqlPartitionList_.resize(getPartitionNum(), initRole);
		for (PartitionId pId = 0; pId < partitionNum; pId++) {
			newsqlPartitionList_[pId].init(pId);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

PartitionTable::PartitionConfig::PartitionConfig(
		const ConfigTable &configTable, int32_t maxNodeNum) :
		partitionNum_(configTable.getUInt32(
				CONFIG_TABLE_DS_PARTITION_NUM)),
		replicationNum_(
				configTable.get<int32_t>(
						CONFIG_TABLE_CS_REPLICATION_NUM) - 1),
		limitOwnerBackupLsnGap_(
			configTable.get<int32_t>(
					CONFIG_TABLE_CS_OWNER_BACKUP_LSN_GAP)),
		limitOwnerCatchupLsnGap_(
			configTable.get<int32_t>(
					CONFIG_TABLE_CS_OWNER_CATCHUP_LSN_GAP)),
		limitClusterNodeNum_(maxNodeNum),
		limitMaxLsnGap_(0),
		ownerLoadLimit_(PT_OWNER_LOAD_LIMIT),
		dataLoadLimit_(PT_DATA_LOAD_LIMIT),
		maxNodeNum_(maxNodeNum) {
	limitOwnerBackupLsnDetectErrorGap_ =
			limitOwnerBackupLsnGap_ * PT_BACKUP_GAP_FACTOR;
}

bool PartitionTable::PartitionConfig::setLimitOwnerBackupLsnGap(
		int32_t gap) {

	if (gap < 0 || gap > INT32_MAX) {
		return false;
	}
	limitOwnerBackupLsnGap_ = gap;
	limitOwnerBackupLsnDetectErrorGap_
			= limitOwnerBackupLsnGap_ * PT_BACKUP_GAP_FACTOR;
	return true;
}

bool PartitionTable::PartitionConfig::setLimitOwnerBackupLsnDetectErrorGap(
		int32_t gap) {

	if (gap < 0 || gap > INT32_MAX) {
		return false;
	}
	limitOwnerBackupLsnDetectErrorGap_ = gap;
	return true;
}

bool PartitionTable::PartitionConfig::setLimitOwnerCatchupLsnGap(
		int32_t gap) {

	if (gap < 0 || gap > INT32_MAX) {
		return false;
	}
	limitOwnerCatchupLsnGap_ = gap;
	return true;
}

bool PartitionTable::PartitionConfig::setOwnerLoadLimit(
		int32_t limitCount) {

	if (limitCount < 0 || limitCount > INT32_MAX) {
		return false;
	}
	ownerLoadLimit_ = limitCount;
	return true;
}

bool PartitionTable::PartitionConfig::setDataLoadLimit(
		int32_t limitCount) {

	if (limitCount < 0 || limitCount > INT32_MAX) {
		return false;
	}
	dataLoadLimit_ = limitCount;
	return true;
}


PartitionTable::~PartitionTable() {

	for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
		if (partitionLockList_) {
			ALLOC_DELETE(
					globalStackAlloc_, &partitionLockList_[pId]);
		}
		if (partitions_) {
			ALLOC_DELETE(
					globalStackAlloc_, &partitions_[pId]);
		}
		if (currentPartitions_) {
			ALLOC_DELETE(
					globalStackAlloc_, &currentPartitions_[pId]);
		}
	}

	if (nodes_) {
		for (NodeId nodeId = 0;
				nodeId < config_.getLimitClusterNodeNum();nodeId++) {
			if (&nodes_[nodeId]) {
				ALLOC_DELETE(globalStackAlloc_, &nodes_[nodeId]);
			}
		}
	}
}

void PartitionTable::checkRange(PartitionId pId) {
	
	if (pId >= config_.partitionNum_) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}
}

void PartitionTable::checkNode(NodeId nodeId) {

	if (nodeId >= config_.maxNodeNum_
			|| nodeId == UNDEF_NODEID) {
		GS_THROW_USER_ERROR(
				GS_ERROR_CS_SERVICE_CLUSTER_INTERNAL_FAILED, "");
	}
}

NodeId PartitionTable::getOwner(
		PartitionId pId,
		PartitionTableType type) {

	checkRange(pId);

	util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
	return partitions_[pId].roles_[type].getOwner();
}

bool PartitionTable::isOwner(
		PartitionId pId,
		NodeId targetNodeId,
		PartitionTableType type) {

	checkRange(pId);

	util::LockGuard<util::ReadLock>
			currentLock(partitionLockList_[pId]);
	return (partitions_[pId].roles_[type].isOwner(targetNodeId));
}

bool PartitionTable::isOwnerOrBackup(
		PartitionId pId, NodeId targetNodeId, PartitionTableType type) {

	checkRange(pId);

	util::LockGuard<util::ReadLock>
			currentLock(partitionLockList_[pId]);

	return (partitions_[pId].roles_[type].isOwner(targetNodeId) ||
			partitions_[pId].roles_[type].isBackup(targetNodeId));
}

void PartitionTable::getBackup(
		PartitionId pId,
		util::XArray<NodeId> &backups,
		PartitionTableType type) {

	checkRange(pId);

	util::LockGuard<util::ReadLock>
			currentLock(partitionLockList_[pId]);
	partitions_[pId].roles_[type].getBackups(backups);
}

void PartitionTable::getCatchup(
		PartitionId pId,
		util::XArray<NodeId> &catchups, PartitionTableType type) {

	checkRange(pId);

	util::LockGuard<util::ReadLock>
			currentLock(partitionLockList_[pId]);
	partitions_[pId].roles_[type].getCatchups(catchups);
}

NodeId PartitionTable::getCatchup(PartitionId pId) {

	checkRange(pId);

	util::LockGuard<util::ReadLock>
			currentLock(partitionLockList_[pId]);
	NodeIdList &catchups
			= partitions_[pId].roles_[PT_CURRENT_OB].getCatchups();
	if (catchups.size() == 1) {
		return catchups[0];
	}
	else {
		return UNDEF_NODEID;
	}
}

bool PartitionTable::isBackup(
		PartitionId pId, NodeId targetNodeId, PartitionTableType type) {

	checkRange(pId);

	util::LockGuard<util::ReadLock>
			currentLock(partitionLockList_[pId]);
	return (partitions_[pId].roles_[type].isBackup(targetNodeId));
}

bool PartitionTable::isCatchup(
		PartitionId pId,
		NodeId targetNodeId,
		PartitionTableType type) {

	checkRange(pId);

	util::LockGuard<util::ReadLock>
			currentLock(partitionLockList_[pId]);
	return (partitions_[pId].roles_[type].isCatchup(targetNodeId));
}

bool PartitionTable::hasCatchupRole(
		PartitionId pId, PartitionTableType type) {

	checkRange(pId);

	util::LockGuard<util::ReadLock>
			currentLock(partitionLockList_[pId]);
	return (partitions_[pId].roles_[type].hasCatchupRole());
}

bool PartitionTable::setNodeInfo(
		NodeId nodeId, ServiceType type, NodeAddress &address) {

	if (nodeId >= config_.limitClusterNodeNum_) {
		GS_THROW_USER_ERROR(
				GS_ERROR_PT_SET_INVALID_NODE_ID, "");
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
CheckedPartitionSummaryStatus PartitionTable::checkPartitionSummaryStatus(
		util::StackAllocator &alloc,
		bool isBalanceCheck) {

	try {
		if (isSubMaster()) {
			return PT_INITIAL;
		}
		else if (isFollower()) {
			return PT_NORMAL;
		}

		bool firstCheckSkip = true;
		int32_t nodeNum = getNodeNum();
		int32_t backupNum = getReplicationNum();
		int32_t liveNum = 0;

		for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
			if (checkLiveNode(nodeId)
					|| (nodeId == 0 && firstCheckSkip)) {
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
			util::XArray<NodeId> backups(alloc);
			getBackup(pId, backups);
			liveNum = 0;

			for (util::XArray<NodeId>::iterator it = backups.begin();
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
		CheckedPartitionSummaryStatus retStatus;

		if (isPartial) {
			retStatus = PT_OWNER_LOSS;
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
			if (!isBalanceCheck && isGoalPartition_) {
				retStatus = PT_NORMAL;
			}
			else {
				retStatus = PT_NOT_BALANCE;
			}
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

	checkRange(pId);

	PartitionRole &role = subPartition.role_;
	PartitionTableType type = role.getPartitionTableType();

	util::LockGuard<util::WriteLock>
			currentLock(partitionLockList_[pId]);
	partitions_[pId].roles_[type] = role;
}

void PartitionTable::clearCurrentStatus() {

	util::LockGuard<util::WriteLock> lock(lock_);
	for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
		setHeartbeatTimeout(nodeId, UNDEF_TTL);
	}
}

void PartitionTable::clearRole(PartitionId pId) {

	checkRange(pId);

	util::LockGuard<util::WriteLock>
			currentLock(partitionLockList_[pId]);
	partitions_[pId].clear();
	partitions_[pId].partitionInfo_.clear();
}

bool PartitionTable::checkClearRole(PartitionId pId) {

	checkRange(pId);

	if (isBackup(pId) 
			&& getPartitionStatus(pId) == PT_ON) {
		clear(pId, PT_CURRENT_OB);
		setPartitionRoleStatus(pId, PT_NONE);
		setPartitionStatus(pId, PT_OFF);
		return true;
	}
	else {
		return false;
	}
}

bool PartitionTable::checkConfigurationChange(
		util::StackAllocator &alloc,
		PartitionContext &context,
		bool checkFlag) {

	util::Set<NodeId> addNodeSet(alloc);
	util::Set<NodeId> downNodeSet(alloc);
	getAddNodes(addNodeSet);
	getDownNodes(downNodeSet);
	bool isAddOrDownNode
			= (addNodeSet.size() > 0 || downNodeSet.size() > 0 || checkFlag);
	if (isAddOrDownNode) {
		GS_TRACE_CLUSTER_INFO(
				"Detect cluster configuration change, create new goal partition, "
			"current_time=" << getTimeStr(context.currentTime_) 
			<< ", revision=" << getPartitionRevisionNo());
		context.setConfigurationChange();
		if (context.isAutoGoal_) {
			planGoal(context);
		}
	}
	checkRepairPartition(context);
	return isAddOrDownNode;
}

bool PartitionTable::check(PartitionContext &context) {

	try {
		util::StackAllocator &alloc = context.getAllocator();
		if (context.currentTime_ < nextApplyRuleLimitTime_) {
			GS_TRACE_CLUSTER_DUMP("Not check interval, "
				<< ", limitTime=" << getTimeStr(nextApplyRuleLimitTime_)
				<< ", currentTime=" << getTimeStr(context.currentTime_));
			if (getPartitionSummaryStatus() == PT_INITIAL) {
				checkPartitionSummaryStatus(alloc);
			}
			return false;
		}
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
			if (checkTargetPartition(
					pId, context.backupNum_, catchupWait)) {
				isIrregular = true;
			}
		}
		if (catchupWait && !isIrregular) {
				return false;
		}
		if (isAllSame && !isIrregular) {
			isGoalPartition_ = true;
			loadInfo_.status_ = PT_NORMAL;

			GS_TRACE_CLUSTER_DUMP(
					"Current partition is same to goal partition");
			context.isGoalPartition_ = true;
			return false;
		}
		else {
			isGoalPartition_ = false;
			if (loadInfo_.status_ != PT_REPLICA_LOSS) {
				loadInfo_.status_ = PT_NOT_BALANCE;
			}
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
	if (partitionSequentialNumber
			> revision_.sequentialNumber_ &&
		partitionSequentialNumber !=
			PartitionRevision::INITIAL_CLUSTER_REVISION) {
		revision_.sequentialNumber_ = partitionSequentialNumber;
	}
}

bool PartitionTable::checkPeriodicDrop(
		util::StackAllocator &alloc,
		DropPartitionNodeInfo &dropInfo) {

	util::XArray<NodeId> activeNodeList(alloc);
	getLiveNodeIdList(activeNodeList);
	int32_t liveNum = static_cast<int32_t>(
			activeNodeList.size());
	int32_t backupNum = getReplicationNum();
	if (backupNum > liveNum - 1) {
		backupNum = liveNum - 1;
	}
	
	bool enableDrop = false;
	
	for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
		PartitionRole currentRole;
		PartitionRole goalRole;
		getPartitionRole(pId, currentRole, PT_CURRENT_OB);
		getPartitionRole(pId, goalRole, PT_CURRENT_GOAL);
		if (currentRole.getOwner() == UNDEF_NODEID) {
			continue;
		}
		if (goalRole.getOwner() == UNDEF_NODEID) {
				continue;
		}
		if (getPartitionStatus(pId, currentRole.getOwner()) != PT_ON) {
			continue;
		}
		if (currentRole.getBackupSize() < backupNum
				|| goalRole.getBackupSize() < backupNum) {
			continue;
		}
		std::vector<NodeId> &backups = currentRole.getBackups();
		bool allActiveFlag = true;
		for (size_t pos = 0; pos < backups.size(); pos++) {
			if (getPartitionStatus(pId, backups[pos]) != PT_ON) {
				allActiveFlag = false;
				break;
			}
		}
		if (!allActiveFlag) {
			continue;
		}
		if (currentRole == goalRole) {
			for (size_t pos = 0; pos < activeNodeList.size(); pos++) {
				NodeId nodeId = activeNodeList[pos];
				if (getPartitionRoleStatus(pId, nodeId)
						!= PT_NONE) {
					continue;
				}
				if (getPartitionStatus(pId, nodeId) == PT_ON
						|| getPartitionStatus(pId, nodeId) == PT_SYNC) {
					continue;
				}
				NodeAddress &address = getNodeAddress(nodeId);
				PartitionRevisionNo revision
						= getPartitionRevision(pId, nodeId);
				dropInfo.set(pId, address, revision, nodeId);
				enableDrop = true;
			}
		}
	}
	return enableDrop;
}

bool PartitionRole::operator==(PartitionRole &role) const {

	NodeId tmpOwnerId = role.getOwner();
	if (ownerNodeId_ == tmpOwnerId) {
		bool retFlag = true;
		
		NodeIdList tmpBackups = role.getBackups();

		if (backups_ == tmpBackups) {
		}
		else {
			retFlag = false;
		}
		NodeIdList tmpCatchups = role.getCatchups();

		if (catchups_ == tmpCatchups) {
		}
		else {
			retFlag = false;
		}
		return retFlag;
	}
	return false;
}

void PartitionRole::encodeAddress(
		PartitionTable *pt, PartitionRole &role) {

	pId_ = role.getPartitionId();
	revision_ = role.getRevision();
	type_ = role.getPartitionTableType();

	NodeId ownerNodeId = role.getOwner();
	if (ownerNodeId != UNDEF_NODEID) {
		ownerAddress_
				= pt->getNodeInfo(ownerNodeId).getNodeAddress();
	}
	NodeIdList &backups = role.getBackups();
	int32_t backupSize = static_cast<int32_t>(backups.size());
	backupAddressList_.clear();
	backupAddressList_.reserve(backupSize);
	for (int32_t pos = 0; pos < backupSize; pos++) {
		NodeId backupNodeId = backups[pos];
		backupAddressList_.push_back(
				pt->getNodeInfo(backupNodeId).getNodeAddress());
	}
	
	NodeIdList &catchups = role.getCatchups();
	int32_t catchupSize = static_cast<int32_t>(catchups.size());
	catchupAddressList_.clear();
	catchupAddressList_.reserve(catchupSize);
	for (int32_t pos = 0; pos < catchupSize; pos++) {
		NodeId catchupNodeId = catchups[pos];
		catchupAddressList_.push_back(
				pt->getNodeInfo(catchupNodeId).getNodeAddress());
	}
}

void PartitionRole::getBackups(util::XArray<NodeId> &backups) {

	size_t size = backups_.size() * sizeof(NodeId);
	if (size == 0) return;
	backups.resize(size / sizeof(NodeId));
	memcpy(backups.data(),
			reinterpret_cast<uint8_t *>(&backups_[0]), size);
}

void PartitionRole::getCatchups(util::XArray<NodeId> &catchups) {
	
	size_t size = catchups_.size() * sizeof(NodeId);
	if (size == 0) return;
	catchups.resize(size / sizeof(NodeId));
	memcpy(catchups.data(),
			reinterpret_cast<uint8_t *>(&catchups_[0]), size);
}

void PartitionRole::clearBackup() {
	backups_.clear();
	backupAddressList_.clear();
	isSelfBackup_ = false;
}

void PartitionRole::clearCatchup() {
	catchups_.clear();
	catchupAddressList_.clear();
	isSelfCatchup_ = false;
}

void PartitionRole::clear() {
	ownerNodeId_ = UNDEF_NODEID;
	backups_.clear();
	catchups_.clear();
	backupAddressList_.clear();
	catchupAddressList_.clear();
	isSelfBackup_ = false;
	isSelfCatchup_ = false;
}

void PartitionRole::set(
		NodeId owner, NodeIdList &backups, NodeIdList &catchups) {

	ownerNodeId_ = owner;
	backups_ = backups;
	catchups_ = catchups;
	checkBackup();
	checkCatchup();
}

bool PartitionRole::isBackup(NodeId targetNodeId) {

	if (backups_.size() == 0) return false;
	if (targetNodeId == 0) {
		return isSelfBackup_;
	}
	NodeIdList::iterator it;
	it = std::find(backups_.begin(), backups_.end(), targetNodeId);
	if (it != backups_.end()) {
		return true;
	}
	else {
		return false;
	}
}

bool PartitionRole::isCatchup(NodeId targetNodeId) {
	if (catchups_.size() == 0) return false;
	if (targetNodeId == 0) {
		return isSelfCatchup_;
	}
	NodeIdList::iterator it;
	it = std::find(catchups_.begin(), catchups_.end(), targetNodeId);
	if (it != catchups_.end()) {
		return true;
	}
	else {
		return false;
	}
}

void PartitionRole::promoteCatchup() {

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

void PartitionRole::getShortTermSyncTargetNodeId(
		util::Set<NodeId> &targetNodeSet) {
	if (ownerNodeId_ != UNDEF_NODEID
			&& ownerNodeId_ > 0) {
		targetNodeSet.insert(ownerNodeId_);
	}
	for (size_t pos = 0; pos < backups_.size(); pos++) {
		if (backups_[pos] != UNDEF_NODEID
				&& backups_[pos] > 0) {
			targetNodeSet.insert(backups_[pos]);
		}
	}
}

PartitionRoleStatus PartitionRole::getPartitionRoleStatus() {

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

void PartitionRole::checkBackup() {

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

void PartitionRole::checkCatchup() {
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

PartitionTable::PartitionContext::PartitionContext(
		util::StackAllocator &alloc,
		PartitionTable *pt,
		bool &needUpdatePartition, 
		SubPartitionTable &subPartitionTable,
		DropPartitionNodeInfo &dropNodeInfo) : 
	alloc_(&alloc),
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
	backupNum_(0) {
}

size_t PartitionTable::getBackupSize(
		util::StackAllocator &alloc, PartitionId pId,
		PartitionTableType type) {

	util::XArray<NodeId> backups(alloc);
	getBackup(pId, backups, type);
	return backups.size();
}

void PartitionTable::getPublicNodeAddress(
		AddressInfoList &addressInfoList) {

	for (NodeId nodeId = 0;
			nodeId < getNodeNum(); nodeId++) {
		addressInfoList.push_back(
				nodes_[nodeId].publicAddressInfo_);
	}
}

LogSequentialNumber PartitionTable::getLSN(
		PartitionId pId, NodeId targetNodeId) {

	checkRange(pId);

	util::LockGuard<util::ReadLock>
			currentLock(partitionLockList_[pId]);
	return currentPartitions_[pId].getLsn(targetNodeId);
}

LogSequentialNumber PartitionTable::getStartLSN(
		PartitionId pId, NodeId targetNodeId) {

	checkRange(pId);

	util::LockGuard<util::ReadLock>
			currentLock(partitionLockList_[pId]);
	return currentPartitions_[pId].getStartLsn(targetNodeId);
}

void PartitionTable::setMaxLsn(
		PartitionId pId, LogSequentialNumber lsn) {

	if (pId >= config_.partitionNum_) {
		GS_THROW_USER_ERROR(
				GS_ERROR_PT_INTERNAL, "");
	}
	if (lsn > maxLsnList_[pId]) {
		maxLsnList_[pId] = lsn;
	}
}

LogSequentialNumber PartitionTable::getMaxLsn(PartitionId pId) {

	checkRange(pId);

	return maxLsnList_[pId];
}

void PartitionTable::setRepairedMaxLsn(
		PartitionId pId, LogSequentialNumber lsn) {

	checkRange(pId);

	maxLsnList_[pId] = lsn;
}

std::string PartitionTable::dumpPartitionRule(int32_t ruleNo) {

	switch (ruleNo) {
		case PartitionTable::PARTITION_RULE_INITIAL: return "Initial";
		case PartitionTable::PARTITION_RULE_ADD: return "Add";
		case PartitionTable::PARTITION_RULE_REMOVE: return "Remove";
		case PartitionTable::PARTITION_RULE_SWAP: return "Swap";
		default: return "None";
	}
}

bool PartitionTable::isAvailable(PartitionId pId) {

	checkRange(pId);

	return partitions_[pId].partitionInfo_.available_;
}

void PartitionTable::setLSN(
	PartitionId pId,
	LogSequentialNumber lsn,
	NodeId targetNodeId) {

	checkRange(pId);

	util::LockGuard<util::WriteLock>
			currentLock(partitionLockList_[pId]);
	currentPartitions_[pId].setLsn(targetNodeId, lsn);
}

void PartitionTable::setStartLSN(
	PartitionId pId,
	LogSequentialNumber lsn,
	NodeId targetNodeId) {

	checkRange(pId);

	util::LockGuard<util::WriteLock>
			currentLock(partitionLockList_[pId]);
	currentPartitions_[pId].setStartLsn(
			targetNodeId, lsn);
}

void PartitionTable::setLsnWithCheck(
	PartitionId pId,
	LogSequentialNumber lsn,
	NodeId targetNodeId) {

	checkRange(pId);

	util::LockGuard<util::WriteLock>
			currentLock(partitionLockList_[pId]);
	currentPartitions_[pId].setLsn(targetNodeId, lsn);
}

PartitionStatus PartitionTable::getPartitionStatus(
		PartitionId pId, NodeId targetNode) {

	checkRange(pId);

	util::LockGuard<util::ReadLock>
			currentLock(partitionLockList_[pId]);
	return currentPartitions_[pId].getPartitionStatus(targetNode);
}

PartitionRoleStatus PartitionTable::getPartitionRoleStatus(
	PartitionId pId, NodeId targetNode) {

	checkRange(pId);

	util::LockGuard<util::ReadLock>
			currentLock(partitionLockList_[pId]);
	return currentPartitions_[pId].getPartitionRoleStatus(targetNode);
}


void PartitionTable::setPartitionStatus(
	PartitionId pId, PartitionStatus status, NodeId targetNode) {

	checkRange(pId);

	util::LockGuard<util::WriteLock>
			currentLock(partitionLockList_[pId]);
	return currentPartitions_[pId].setPartitionStatus(
			targetNode, status);
}

void PartitionTable::setPartitionRoleStatus(
	PartitionId pId,
	PartitionRoleStatus status,
	NodeId targetNode) {

	checkRange(pId);

	util::LockGuard<util::WriteLock>
			currentLock(partitionLockList_[pId]);
	currentPartitions_[pId].setPartitionRoleStatus(
			targetNode, status);
}

void PartitionTable::setPartitionRevision(
		PartitionId pId,
		PartitionRevisionNo partitionRevisuion,
		NodeId targetNode) {

	checkRange(pId);

	util::LockGuard<util::WriteLock>
			currentLock(partitionLockList_[pId]);
	currentPartitions_[pId].setPartitionRevision(
			targetNode, partitionRevisuion);
}

PartitionRevisionNo PartitionTable::getPartitionRevision(
		PartitionId pId, NodeId targetNode) {

	checkRange(pId);

	util::LockGuard<util::ReadLock>
			currentLock(partitionLockList_[pId]);
	return currentPartitions_[pId].getPartitionRevision(targetNode);
}

void PartitionTable::getPartitionRole(
		PartitionId pId,
		PartitionRole &role,
		PartitionTableType type) {

	checkRange(pId);

	switch (type) {
		case PT_CURRENT_OB: {
			util::LockGuard<util::ReadLock>
					currentLock(partitionLockList_[pId]);
			role = partitions_[pId].roles_[type];
		}
		break;
		case PT_CURRENT_GOAL: {
			role = goal_.getPartitionRole(pId);
		}
		break;
		default:
		break;
	}
}

void PartitionTable::setPartitionRole(
		PartitionId pId,
		PartitionRole &role,
		PartitionTableType type) {

	checkRange(pId);

	switch (type) {
		case PT_CURRENT_OB: {
			util::LockGuard<util::WriteLock>
					currentLock(partitionLockList_[pId]);

			partitions_[pId].roles_[type] = role;
			partitions_[pId].roles_[type].set(type);
			PartitionRoleStatus nextStatus =
				partitions_[pId].roles_[type].getPartitionRoleStatus();
			currentPartitions_[pId].setPartitionRoleStatus(
					SELF_NODEID, nextStatus);
			currentPartitions_[pId].setPartitionRevision(
					SELF_NODEID, role.getRevisionNo());
		}
		break;
		case PT_CURRENT_GOAL: {
			goal_.update(role);
		}
		break;
		default:
		break;
	}
}

void PartitionTable::setErrorStatus(
		PartitionId pId,
		int32_t status,
		NodeId targetNode) {

	checkRange(pId);

	util::LockGuard<util::WriteLock>
			currentLock(partitionLockList_[pId]);
	currentPartitions_[pId].setErrorStatus(
			targetNode, status);
}

int32_t PartitionTable::getErrorStatus(
		PartitionId pId, NodeId targetNode) {

	checkRange(pId);

	util::LockGuard<util::ReadLock>
			currentLock(partitionLockList_[pId]);
	return currentPartitions_[pId].getErrorStatus(targetNode);
}

void PartitionTable::resetBlockQueue() {

	util::LockGuard<util::WriteLock> lock(lock_);
	nextBlockQueue_.clear();
	goalBlockQueue_.clear();
}

void PartitionTable::clear(
		PartitionId pId, PartitionTableType type) {

	checkRange(pId);

	util::LockGuard<util::WriteLock>
			currentLock(partitionLockList_[pId]);
	partitions_[pId].clear(type);
}

PartitionGroupId PartitionTable::getPartitionGroupId(PartitionId pId) {

	checkRange(pId);

	return partitions_[pId].partitionInfo_.pgId_;
}

NodeAddress &PartitionTable::getPublicNodeAddress(
		NodeId nodeId, ServiceType type) {

	if (!getNodeInfo(nodeId).publicAddressInfo_.isValid()) {
		return getNodeAddress(nodeId, type);
	}
	else {
		return getNodeInfo(nodeId)
				.publicAddressInfo_.getNodeAddress(type);
	}
}

PartitionRevision &PartitionTable::incPartitionRevision() {

	util::LockGuard<util::Mutex> lock(revisionLock_);
	if (revision_.sequentialNumber_ == INT64_MAX) {
		GS_THROW_SYSTEM_ERROR(
				GS_ERROR_CS_MAX_PARTITION_REVISION,
				"Cluster partition sequence number is limit");
	}
	else {
		revision_.sequentialNumber_++;
	}
	return revision_;
}

void PartitionTable::getDownNodes(
		util::Set<NodeId> &downNodes, bool isClear) {

	util::LockGuard<util::Mutex> lock(nodeInfoLock_);
	for (std::set<NodeId>::iterator it = downNodeList_.begin();
			it != downNodeList_.end(); it++) {
		downNodes.insert((*it));
	}
	if (isClear) {
		downNodeList_.clear();
	}
}

PartitionTable::NodeInfo &PartitionTable::getNodeInfo(NodeId nodeId) {

	checkNode(nodeId);

	return nodes_[nodeId];
}

void PartitionTable::getAddNodes(util::Set<NodeId> &nodeIds) {

	util::LockGuard<util::Mutex> lock(nodeInfoLock_);
	NodeId nodeId = getNodeNum();
	if (nodeId > prevMaxNodeId_) {
		for (int32_t pos = prevMaxNodeId_;
				pos < nodeId; pos++) {
			if (nodes_[pos].nodeBaseInfo_[CLUSTER_SERVICE + 1].check()) {
				nodeIds.insert(pos);
			}
		}
		prevMaxNodeId_ = nodeId;
	}
}

std::string PartitionTable::dumpNodeAddress(
	NodeId nodeId, ServiceType type) {

	if (nodeId >= nodeNum_ || nodeId < 0) {
		return std::string("");
	}
	return nodes_[nodeId].nodeBaseInfo_[type].address_.toString();
}

std::string PartitionTable::dumpPartitionStatusForRest(
		PartitionStatus status) {

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

std::string PartitionTable::dumpPartitionStatusEx(
		PartitionStatus status) {

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

std::string PartitionTable::dumpCurrentClusterStatus() {

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

PartitionTable::NodeInfo::~NodeInfo() {

	if (nodeBaseInfo_) {
		for (int32_t pos = 0;
				pos < SERVICE_MAX; pos++) {
			ALLOC_DELETE(
					*globalStackAlloc_, &nodeBaseInfo_[pos]);
		}
	}
}

bool PartitionTable::NodeInfo::isAlive(
		int64_t limitTime, bool isCheck) {

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

bool PartitionTable::checkLimitOwnerBackupLsnGap(
		LogSequentialNumber ownerLSN,
	LogSequentialNumber backupLSN, int32_t gapFactor) {
	uint64_t gapValue =
		static_cast<uint64_t>(
				config_.limitOwnerBackupLsnGap_ * gapFactor);
	if (ownerLSN >= backupLSN) {
		return (ownerLSN - backupLSN > gapValue);
	}
	else {
		return (backupLSN - ownerLSN > gapValue);
	}
}

void PartitionTable::setAvailable(PartitionId pId, bool available) {

	checkRange(pId);
	partitions_[pId].partitionInfo_.available_ = available;
}

std::string PartitionTable::NodeInfo::dump() {

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

void PartitionTable::BlockQueue::checkRange(PartitionId pId) {
	
	if (pId >= partitionNum_) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}
}

PartitionTable::BlockQueue::~BlockQueue() {

	if (queueList_) {
		for (uint32_t pos = 0;
				pos < partitionNum_; pos++) {
			ALLOC_DELETE(*globalStackAlloc_,
					&queueList_[pos]);
		}
	}
}

void PartitionTable::BlockQueue::init(util::StackAllocator &alloc) {

	globalStackAlloc_ = &alloc;
	queueList_ = ALLOC_NEW(*globalStackAlloc_)
			std::set<NodeId>[partitionNum_];
}

void PartitionTable::BlockQueue::clear() {

	for (PartitionId pId = 0; pId < partitionNum_; pId++) {
		clear(pId);
	}
}

int32_t PartitionTable::BlockQueue::size(PartitionId pId) {

	checkRange(pId);

	return static_cast<int32_t>(queueList_[pId].size());
}

void PartitionTable::BlockQueue::push(
		PartitionId pId, NodeId nodeId) {

	checkRange(pId);

	if (static_cast<int32_t>(
			queueList_[pId].size()) > queueSize_) {
		queueList_[pId].clear();
	}
	queueList_[pId].insert(nodeId);
}

bool PartitionTable::BlockQueue::find(
		PartitionId pId, NodeId nodeId) {

	checkRange(pId);

	std::set<NodeId>::iterator setItr
			= queueList_[pId].find(nodeId);
	return (setItr != queueList_[pId].end());
}

void PartitionTable::BlockQueue::clear(PartitionId pId) {

	checkRange(pId);

	queueList_[pId].clear();
}

void PartitionTable::BlockQueue::erase(
		PartitionId pId, NodeId nodeId) {
	
	checkRange(pId);

	std::set<NodeId>::iterator setItr
			= queueList_[pId].find(nodeId);
	if (setItr != queueList_[pId].end()) {
		queueList_[pId].erase(setItr);
	}
}

void PartitionTable::CurrentPartitions::init() {

	CurrentPartitionInfo first;
	currentPartitionInfos_.push_back(first);
}

void PartitionTable::CurrentPartitions::resize(
		int32_t nodeNum, bool withCheck) {

	if (withCheck) {
		if (currentPartitionInfos_.size()
				> static_cast<size_t>(nodeNum)) {
			return;
		}
	}
	currentPartitionInfos_.resize(nodeNum);
}

LogSequentialNumber PartitionTable::CurrentPartitions::getLsn(
		NodeId nodeId) {

	checkSize(nodeId);
	return currentPartitionInfos_[nodeId].lsn_;
}

void PartitionTable::CurrentPartitions::setLsn(
		NodeId nodeId,
		LogSequentialNumber lsn) {

	checkSize(nodeId);
	currentPartitionInfos_[nodeId].lsn_ = lsn;
}

LogSequentialNumber PartitionTable::CurrentPartitions::getStartLsn(
		NodeId nodeId) {

	checkSize(nodeId);
	return currentPartitionInfos_[nodeId].startLsn_;
}

void PartitionTable::CurrentPartitions::setStartLsn(
		NodeId nodeId, LogSequentialNumber startLsn) {

	checkSize(nodeId);

	if (currentPartitionInfos_[nodeId].startLsn_ < startLsn) {
		currentPartitionInfos_[nodeId].startLsn_ = startLsn;
	}
}

void PartitionTable::CurrentPartitions::setErrorStatus(
		NodeId nodeId, int32_t status) {

	checkSize(nodeId);
	currentPartitionInfos_[nodeId].errorStatus_ = status;
}

int32_t PartitionTable::CurrentPartitions::getErrorStatus(
		NodeId nodeId) {

	checkSize(nodeId);
	int32_t ret
			= currentPartitionInfos_[nodeId].errorStatus_;
	currentPartitionInfos_[nodeId].errorStatus_
			= PT_ERROR_NORMAL;
	return ret;
}

PartitionStatus PartitionTable::CurrentPartitions::getPartitionStatus(
		NodeId nodeId) {

	checkSize(nodeId);
	return currentPartitionInfos_[nodeId].status_;
}

void PartitionTable::CurrentPartitions::setPartitionStatus(
		NodeId nodeId, PartitionStatus status) {

	checkSize(nodeId);
	currentPartitionInfos_[nodeId].status_ = status;
}

PartitionRoleStatus PartitionTable::CurrentPartitions::getPartitionRoleStatus(
		NodeId nodeId) {

	checkSize(nodeId);
	return currentPartitionInfos_[nodeId].roleStatus_;
}

void PartitionTable::CurrentPartitions::setPartitionRoleStatus(
		NodeId nodeId, PartitionRoleStatus roleStatus) {

	checkSize(nodeId);
	currentPartitionInfos_[nodeId].roleStatus_ = roleStatus;
}

PartitionRevisionNo PartitionTable::CurrentPartitions::getPartitionRevision(
		NodeId nodeId) {

	checkSize(nodeId);
	return currentPartitionInfos_[nodeId].partitionRevision_;
}

void PartitionTable::CurrentPartitions::setPartitionRevision(
		NodeId nodeId, PartitionRevisionNo partitionRevision) {

	checkSize(nodeId);
	currentPartitionInfos_[nodeId].partitionRevision_ = partitionRevision;
}

void PartitionTable::CurrentPartitions::resetPartitionInfo(
		NodeId nodeId) {

	checkSize(nodeId);
	currentPartitionInfos_[nodeId].roleStatus_ = PT_NONE;
	currentPartitionInfos_[nodeId].status_ = PT_OFF;
}

void PartitionTable::CurrentPartitions::checkSize(NodeId nodeId) {

	if (static_cast<size_t>(nodeId)
			>= currentPartitionInfos_.size()) {

		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL,
				"Specifed nodeId is invalid, nodeId=" << nodeId
				<< ", max node size=" << currentPartitionInfos_.size());
	}
}

PartitionTable::Partition::~Partition() {

	if (roles_) {
		for (int32_t pos = 0; pos < PT_TABLE_MAX; pos++) {
			ALLOC_DELETE(*globalStackAlloc_, &roles_[pos]);
		}
	}
}

void PartitionTable::Partition::init(
		util::StackAllocator &alloc,
		PartitionId pId,
		PartitionGroupId pgId) {

	globalStackAlloc_ = &alloc;
	roles_ = ALLOC_NEW(alloc) PartitionRole[PT_TABLE_MAX];
	
	for (int32_t pos = 0; pos < PT_TABLE_MAX; pos++) {
		roles_[pos].init(pId, static_cast<PartitionTableType>(pos));
	}
	partitionInfo_.pgId_ = pgId;
}

void PartitionTable::Partition::clear(
		PartitionTableType type) {

	if (type == PT_TABLE_MAX) {
		for (size_t i = 0; i < PT_TABLE_MAX; i++) {
			roles_[i].clear();
		}
	}
	else {
		roles_[type].clear();
	}
}


template<class T> void arrayShuffle(
		util::XArray<T> &ary,
		uint32_t size,
		util::Random &random) {

	for(uint32_t i = 0;i < size; i++) {
		uint32_t j = (uint32_t)(random.nextInt32()) % size;
		T t = ary[i];
		ary[i] = ary[j];
		ary[j] = t;
	}
}

std::ostream &operator<<(
		std::ostream &ss, PartitionRole &partitionRole) {
	ss << partitionRole.dumpIds();
	return ss;
}

std::ostream &operator<<(
		std::ostream &ss, NodeAddress &nodeAddres) {
	ss << nodeAddres.toString();
	return ss;
}

std::ostream &operator<<(
		std::ostream &ss, PartitionRevision &revision) {
	ss << revision.toString();
	return ss;
}

std::string PartitionTable::dumpNodes() {

	util::NormalOStringStream ss;
	std::string terminateCode = "\n ";
	std::string tabCode = "\t";

	NodeInfo *nodeInfo;
	bool usePublic = hasPublicAddress();
	AddressInfoList addressInfoList;
	if (usePublic) {
		getPublicNodeAddress(addressInfoList);
	}
	for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
		nodeInfo = &nodes_[nodeId];
		ss << tabCode << "#" << nodeId << " : ";
		ss << nodes_[nodeId].dump();

		if (masterNodeId_ == 0
				|| masterNodeId_ == UNDEF_NODEID) {
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
		if (usePublic) {
			ss << "(";
			ss << addressInfoList[nodeId].getNodeAddress(
					TRANSACTION_SERVICE).dump().c_str();
			ss << ",";
			ss << addressInfoList[nodeId].getNodeAddress(
					SQL_SERVICE).dump().c_str();
			ss << ")";
		}
		if (nodeId != nodeNum_ - 1) {
			ss << terminateCode;
		}
	}
	return ss.str();
}

void PartitionTable::genPartitionColumnStr(
		util::NormalOStringStream &ss,
		std::string &roleName,
		PartitionId pId,
		NodeId nodeId,
		int32_t nodeIdDigit,
		PartitionTableType type) {

	ss << roleName << "(";
	ss << "#" << std::setw(nodeIdDigit) << nodeId;
	ss << ", ";
	ss << std::setw(lsnDigit_) << getLSN(pId, nodeId);
	if (type == PT_CURRENT_OB) {
		ss << ", ";
		ss << dumpPartitionStatusEx(
				getPartitionStatus(pId, nodeId));
	}
	ss << ")";
}

std::string PartitionTable::dumpPartitionsNew(
		util::StackAllocator &alloc,
		PartitionTableType type,
		SubPartitionTable *subTable) {

	util::StackAllocator::Scope scope(alloc);
	util::NormalOStringStream ss;
	util::XArray<uint32_t> countList(alloc);
	util::XArray<uint32_t> ownerCountList(alloc);
	countList.assign(
			static_cast<size_t>(nodeNum_), 0);
	ownerCountList.assign(
			static_cast<size_t>(nodeNum_), 0);
	std::string terminateCode = "\n ";
	uint32_t partitionNum = config_.partitionNum_;

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
			<< getTimeStr(util::DateTime::now(
					TRIM_MILLISECONDS).getUnixTime()) 
			<< std::endl;

	for (NodeId nodeId = 0;
			nodeId < getNodeNum(); nodeId++) {
		ss << "#" << std::setw(nodeIdDigit)
				<< nodeId << ", " << dumpNodeAddress(nodeId);
		if (getHeartbeatTimeout(nodeId) == UNDEF_TTL) {
			ss << "(Down)";
		}
			ss << std::endl;
	}
	util::Vector<PartitionId> partitionIdList(alloc);
	util::Map<PartitionId, PartitionRole*> roleMap(alloc);
	if (subTable) {
		for (size_t pos = 0;
				pos < subTable->size(); pos++) {

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
		for (size_t pos = 0;
				pos < partitionIdList.size(); pos++) {
			pId = partitionIdList[pos];
			PartitionRole role;
			if (!subTable) {
				getPartitionRole(pId, role, type);
			}
			else {
				util::Map<PartitionId, PartitionRole*>::iterator
						it = roleMap.find(pId);
				if (it != roleMap.end()) {
					role = *(*it).second;
				}
				else {
					continue;
				}
			}
			ss << "[P" << std::setfill('0') 
					<< std::setw(partitionDigit_) << pId;
			ss << "(" << std::setw(revisionDigit)
					<< role.getRevisionNo() << ")]";
			NodeId owner = role.getOwner();
			ss << " ";
			if (owner == UNDEF_NODEID) {
			}
			else {
				genPartitionColumnStr(
						ss, ownerStr, pId, owner, nodeIdDigit, type);
			}
			NodeIdList &backups = role.getBackups();
			if (backups.size() > 0) {
				if (owner != UNDEF_NODEID) {
					ss << ", ";
				}
				for (size_t pos = 0; pos < backups.size(); pos++) {
					genPartitionColumnStr(
							ss, backupStr, pId, backups[pos], nodeIdDigit, type);
					if (pos != backups.size() - 1) {
						ss << ", ";
					}
				}
			}
			NodeIdList &catchups = role.getCatchups();
			if (catchups.size() > 0) {
				ss << ", ";
				for (size_t pos = 0;
						pos < catchups.size(); pos++) {
					genPartitionColumnStr(
							ss, catchupStr, pId, catchups[pos], nodeIdDigit, type);
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
		util::StackAllocator &alloc,
		PartitionTableType type,
		bool isSummary) {

	util::StackAllocator::Scope scope(alloc);
	util::NormalOStringStream ss;
	util::XArray<uint32_t> countList(alloc);
	util::XArray<uint32_t> ownerCountList(alloc);
	countList.assign(
			static_cast<size_t>(nodeNum_), 0);
	ownerCountList.assign(
			static_cast<size_t>(nodeNum_), 0);

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
		ss << "[P" << std::setfill('0')
				<< std::setw(partitionDigit_) << pId
				<< "]";
		ss << "[" 
				<< dumpPartitionStatusEx(getPartitionStatus(pId)) << "]";

		NodeIdList currentBackupList;
		NodeId curretOwnerNodeId = role.getOwner();
		PartitionRevision &revision = role.getRevision();
		NodeIdList &backups = role.getBackups();
		NodeIdList &catchups = role.getCatchups();
		int32_t backupNum = static_cast<int32_t>(backups.size());
		int32_t catchupNum = static_cast<int32_t>(catchups.size());

		for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
			if (curretOwnerNodeId == nodeId) {
				ss << ownerStr;
				psum++;
				countList[nodeId]++;
				ownerCountList[nodeId]++;
			}
			else if (currentPos < backupNum
					&& backups[currentPos] == nodeId) {
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
		ss << "[" << std::setw(lsnDigit_)
				<< maxLsnList_[pId] << "]";
		ss << "<" << revision.toString() << ">";
		ss << "(" << psum << ")" << terminateCode;
	}
	if (isSummary) {
		ss.str("");
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
	ss << terminateCode;
	return ss.str();
}

std::string PartitionTable::dumpPartitionsList(
		util::StackAllocator &alloc,
		PartitionTableType type) {

	util::StackAllocator::Scope scope(alloc);
	util::NormalOStringStream ss;
	util::XArray<uint32_t> countList(alloc);
	util::XArray<uint32_t> ownerCountList(alloc);
	countList.assign(
			static_cast<size_t>(nodeNum_), 0);
	ownerCountList.assign(
			static_cast<size_t>(nodeNum_), 0);
	uint32_t partitionNum = config_.partitionNum_;

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
		NodeIdList currentBackupList;
		NodeId curretOwnerNodeId = role->getOwner();
		NodeIdList &backups = role->getBackups();
		ss << "{pId=" << pId 
				<< ", owner="<< dumpNodeAddress(curretOwnerNodeId) 
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

std::string PartitionTable::dumpDatas(
		bool isDetail, bool lsnOnly) {

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
		ss << "[P" << std::setfill('0')
				<< std::setw(partitionDigit_) << pId
				<< "]";
		for (NodeId nodeId = 0;
				nodeId < nodeNum_; nodeId++) {
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

	if (!lsnOnly) {

	ss << terminateCode << "[DETAIL LIST]" << terminateCode;
	int32_t revisionDigit = 0;
	{
			PartitionRevisionNo maxRevision = -1;
			NodeId nodeId;
			for (pId = 0; pId < partitionNum; pId++) {
				if (isMaster()) {
				for (nodeId = 0;
						nodeId < nodeNum_; nodeId++) {
					PartitionRevisionNo revision
							= getPartitionRevision(pId, nodeId);
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
		ss << "[P" << std::setfill('0')
				<< std::setw(partitionDigit_) << pId
				<< "]";

		if (isMaster()) {
			for (NodeId nodeId = 0;
					nodeId < nodeNum_; nodeId++) {
				PartitionStatus pstat
						= getPartitionStatus(pId, nodeId);
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
		ss << "[P" << std::setfill('0')
				<< std::setw(partitionDigit_) << pId
				<< "]";

		if (isMaster()) {
			for (NodeId nodeId = 0;
					nodeId < nodeNum_; nodeId++) {
				PartitionRoleStatus prolestat
						= getPartitionRoleStatus(pId, nodeId);
				ss << dumpPartitionRoleStatusEx(prolestat);
				if (nodeId != nodeNum_ - 1) ss << ",";
			}
		}
		else {
			PartitionRoleStatus prolestat
					= getPartitionRoleStatus(pId, 0);
			ss << dumpPartitionRoleStatusEx(prolestat);
		}
		ss << terminateCode;
	}
	ss << terminateCode << "[REVISION LIST]" << terminateCode;
	for (pId = 0; pId < partitionNum; pId++) {
		ss << "[P" << std::setfill('0')
				<< std::setw(partitionDigit_) << pId
				<< "]";
		if (isMaster()) {
			for (NodeId nodeId = 0;
					nodeId < nodeNum_; nodeId++) {
				PartitionRevisionNo revision
						= getPartitionRevision(pId, nodeId);
				ss << revision;
				if (nodeId != nodeNum_ - 1) ss << ",";
			}
		}
		else {
			PartitionRevisionNo revision
					= getPartitionRevision(pId, 0);
			ss << revision;
		}
		ss << terminateCode;
	}
	isDetail=true;
	if (isDetail) {
		ss << terminateCode << "[START LSN LIST]" << terminateCode;
		for (pId = 0; pId < partitionNum; pId++) {
			ss << "[P" << std::setfill('0')
					<< std::setw(partitionDigit_) << pId
					<< "]";

			for (NodeId nodeId = 0;
					nodeId < nodeNum_; nodeId++) {
				if (isMaster()) {
					ss << std::setw(lsnDigit_)
							<< getStartLSN(pId, nodeId);
				}
				else {
					if (nodeId == 0) {
						ss << std::setw(lsnDigit_)
								<< getStartLSN(pId, nodeId);
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
	}
	return ss.str();
}

void PartitionTable::getServiceAddress(
		NodeId nodeId,
		picojson::value &result,
		ServiceType addressType) {

	result = picojson::value(picojson::object());
	picojson::object &resultObj
			= result.get<picojson::object>();

	if (nodeId != UNDEF_NODEID) {
		NodeAddress address;
		if (hasPublicAddress_ && 
				(addressType == TRANSACTION_SERVICE
				|| addressType == SQL_SERVICE
				)) {
			address = getNodeInfo(nodeId).publicAddressInfo_
					.getNodeAddress(addressType);
		}
		else {
			 address = getNodeInfo(nodeId)
						.getNodeAddress(addressType);
		}

		resultObj["address"]
				= picojson::value(address.toString(false));
		resultObj["port"]
			= picojson::value(static_cast<double>(address.port_));
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
		<< ", backups=" << ss2.str()
		<< ", catchups=" << ss3.str() << ", revision="
		<< revision_.toString()
		<< "}";

	return ss1.str();
}

std::string PartitionRole::dump() {

	util::NormalOStringStream ss1;
	util::NormalOStringStream ss2;
	util::NormalOStringStream ss3;

	ss2 << "[";
	for (size_t pos = 0;
			pos < backupAddressList_.size(); pos++) {
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
	for (size_t pos = 0;
			pos < catchupAddressList_.size(); pos++) {
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

	ss1 << " {pId=" << pId_ 
			<< ", ownerAddress:" << ownerAddress_.toString()
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


std::string PartitionRole::dump(
		PartitionTable *pt,
		ServiceType serviceType) {

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
	if (ownerNodeId_ == UNDEF_NODEID) {
			ss1 << "UNDEF";
	}
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

void PartitionTable::updateNewSQLPartiton(
		std::vector<PartitionRole> &newsqlPartitionList) {

	util::LockGuard<util::Mutex> lock(newsqlLock_);
	if (newsqlPartitionList.size() != getPartitionNum()) {
		GS_THROW_USER_ERROR(
				GS_ERROR_PT_INTERNAL, "");
	}
	for (PartitionId pId = 0;
			pId < newsqlPartitionList.size(); pId++) {
		if ((newsqlPartitionList[pId].getRevisionNo()
				== PartitionRevision::INITIAL_CLUSTER_REVISION
				|| newsqlPartitionList[pId].getRevisionNo()
						> newsqlPartitionList[pId].getRevisionNo())) {
			newsqlPartitionList_[pId] = newsqlPartitionList[pId];
		}
	}
}

void PartitionTable::updateNewSQLPartiton(
		SubPartitionTable &newsqlPartition) {

	util::LockGuard<util::Mutex> lock(newsqlLock_);
	for (size_t pos = 0; pos < newsqlPartition.size(); pos++) {

		SubPartition sub = newsqlPartition.getSubPartition(pos);
		
		if (sub.pId_ < getPartitionNum()) {
			if ((sub.role_.getRevisionNo()
					==  PartitionRevision::INITIAL_CLUSTER_REVISION
					|| sub.role_.getRevisionNo()
							> newsqlPartitionList_[sub.pId_].getRevisionNo())) {
				newsqlPartitionList_[sub.pId_] = sub.role_;
			}
		}
	}
}

NodeId PartitionTable::getNewSQLOwner(PartitionId pId) {

	util::LockGuard<util::Mutex> lock(newsqlLock_);
	if (pId >= getPartitionNum()) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}
	return newsqlPartitionList_[pId].getOwner();
}

PartitionRevisionNo PartitionTable::getNewSQLPartitionRevision(
		PartitionId pId) {

	util::LockGuard<util::Mutex> lock(newsqlLock_);
	if (pId >= getPartitionNum()) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}
	return newsqlPartitionList_[pId].getRevisionNo();
}

std::string PartitionTable::dumpNewSQLPartition() {

	util::LockGuard<util::Mutex> lock(newsqlLock_);
	util::NormalOStringStream ss;
	for (PartitionId pId = 0;
			pId < newsqlPartitionList_.size(); pId++) {
		NodeId nodeId = newsqlPartitionList_[pId].getOwner();
		PartitionRevisionNo rev
				= newsqlPartitionList_[pId].getRevisionNo();
		ss << "pId=" << pId << ", revision=" << rev
				<< ", owner=" << dumpNodeAddress(nodeId) << std::endl;
	}
	ss << std::endl;
	return ss.str().c_str();
}

template <typename T1, typename T2>
void PartitionTable::getActiveNodeList(
		T1 &activeNodeList,  T2 &downNodeSet,
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

template void PartitionTable::getActiveNodeList(
		NodeIdList &activeNodeList,
		util::Set<NodeId> &downNodeList,
		int64_t currentTime);

void PartitionTable::changeClusterStatus(
		ClusterStatus status,
		NodeId masterNodeId,
		int64_t nextHeartbeatTimeout) {

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
			setHeartbeatTimeout(
					SELF_NODEID, nextHeartbeatTimeout);
			setHeartbeatTimeout(
					masterNodeId, nextHeartbeatTimeout);
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

void PartitionTable::resetDownNode(
		NodeId targetNodeId) {

	for (PartitionId pId = 0;
			pId < getPartitionNum(); pId++) {
		PartitionRole currentRole;
		getPartitionRole(pId, currentRole);
		NodeId currentOwner = currentRole.getOwner();
		bool isDown = false;
		if (currentOwner != UNDEF_NODEID) {
			if (nodes_[currentOwner].heartbeatTimeout_
					== UNDEF_TTL) {
				currentOwner = UNDEF_NODEID;
				isDown = true;
			}
		}
		NodeIdList &currentBackups
				= currentRole.getBackups();
		NodeIdList &currentCatchups
				= currentRole.getCatchups();
		NodeIdList tmpBackups, tmpCatchups;
		for (size_t pos = 0;
				pos < currentBackups.size(); pos++) {
			if (nodes_[currentBackups[pos]].heartbeatTimeout_
					== UNDEF_TTL) {
				isDown = true;
			}
			else {
				tmpBackups.push_back(currentBackups[pos]);
			}
		}
		for (size_t pos = 0;
				pos < currentCatchups.size(); pos++) {
			if (nodes_[currentCatchups[pos]].heartbeatTimeout_
					== UNDEF_TTL) {
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

void PartitionTable::resizeCurrenPartition(
		int32_t resizeNum) {

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
		util::LockGuard<util::WriteLock>
				currentLock(partitionLockList_[pId]);
		currentPartitions_[pId].resize(resizeNum, withCheck);
	}
}

bool PartitionTable::checkNextOwner(
		PartitionId pId,
		NodeId checkedNode,
		LogSequentialNumber checkedLsn,
		LogSequentialNumber checkedMaxLsn,
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
		bool check2 = !checkLimitOwnerBackupLsnGap(
				checkedMaxLsn, checkedLsn);
		if (check1 && check2) {
			if (currentOwner != checkedNode) {
				LogSequentialNumber startLsn
						= getStartLSN(pId, currentOwner);
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
		LogSequentialNumber checkedLsn,
		LogSequentialNumber currentOwnerStartLsn,
		LogSequentialNumber currentOwnerLsn) {

	bool backupCond1 =
			!checkLimitOwnerBackupLsnGap(
					currentOwnerLsn, checkedLsn);
	bool backupCond2 =
			(checkedLsn >= currentOwnerStartLsn);
	return (backupCond1 && backupCond2);
}

void SubPartitionTable::prepareMap(
		util::Map<PartitionId, PartitionRole*> &roleMap) {

	for (size_t pos = 0;
			pos < subPartitionList_.size(); pos++) {

		roleMap[subPartitionList_[pos].pId_]
			= &subPartitionList_[pos].role_;
	}
}

void SubPartitionTable::set(
		PartitionTable *pt,
		PartitionId pId,
		PartitionRole &role,
		NodeId ownerNodeId) {

	if (pId >= pt->getPartitionNum()) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}
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

void SubPartitionTable::set(
		PartitionTable *pt,
		PartitionId pId,
		PartitionRole &role,
		NodeAddress &ownerAddress) {

	SubPartition subPartition(pId, role, ownerAddress);
	PartitionRole &tmpRole = subPartition.role_;
	tmpRole.encodeAddress(pt, role);
	subPartitionList_.push_back(subPartition);
}

void SubPartitionTable::set(PartitionTable *pt) {

	for (PartitionId pId = 0;
			pId < pt->getPartitionNum(); pId++) {
		
		PartitionRole tmpRole;
		pt->getPartitionRole(pId, tmpRole);
		set(pt, pId, tmpRole);
	}
}

const std::string dumpPartitionStatus(
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
}

const std::string PartitionTable::dumpPartitionRoleStatus(
		PartitionRoleStatus status) {

	switch (status) {
	case PT_OWNER:
		return "OWNER";
	case PT_BACKUP:
		return "BACKUP";
	case PT_CATCHUP:
		return "CATCHUP";
	case PT_NONE:
		return "NONE";
	default:
		return "";
	}
}

const std::string dumpPartitionRoleStatusEx(
		PartitionRoleStatus status) {

	switch (status) {
	case PT_OWNER:
		return "O";
	case PT_BACKUP:
		return "B";
	case PT_CATCHUP:
		return "C";
	case PT_NONE:
		return "N";
	default:
		return "";
	}
}

std::string SubPartitionTable::dump(bool isTrace) {

	util::NormalOStringStream ss;
	ss << "NextPartitionTable:{";
	ss << "revision=" << revision_.sequentialNumber_;
	ss << ", [";
	if (!isTrace) {
		ss << std::endl;
	}

	for (size_t pos = 0; pos < subPartitionList_.size(); pos++) {
		ss << subPartitionList_[pos].dump();
		if (!isTrace) {
			ss <<std::endl;
		}
		if (pos != subPartitionList_.size() - 1) {
			ss << ",";
		}
	}
	ss << "]}";
	return ss.str();
}

void PartitionTable::planGoal(PartitionContext &context) {

	util::StackAllocator &alloc = context.getAllocator();
	util::XArray<NodeId> nodeIdList(alloc);
	getLiveNodeIdList(nodeIdList);
	nextApplyRuleLimitTime_ = UNDEF_TTL;
	currentRuleNo_ = PARTITION_RULE_UNDEF;
	isGoalPartition_ = false;
	for (int32_t i = 0; i < getNodeNum(); i++) {
		if (getHeartbeatTimeout(i) == UNDEF_TTL) {
			resetDownNode(i);
		}
	}

	for (PartitionId pId = 0;
			pId < getPartitionNum(); pId++) {
		clearCatchupRole(pId);
	}

	util::Vector<util::Vector<ScoreInfo> > scoreTable(alloc);
	util::Vector<PartitionRole> roleList(alloc);

	try {
		assignGoalTable(alloc, nodeIdList, roleList);
		modifyGoalTable(alloc, nodeIdList, roleList);
		setGoal(this, roleList);
		GS_TRACE_CLUSTER_INFO(
				dumpPartitionsNew(alloc, PT_CURRENT_GOAL));
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e,"");
	}
}

bool assignPartition(
		PartitionId pId,
		NodeId nodeId,
		int32_t nodePos,
		PartitionRole &role,
		util::Vector<int32_t> &dataCountList,
		util::Vector<int32_t> &partitionCountList, 
		int32_t replicaNum) {

	int32_t backupNum = replicaNum - 1;
	if (replicaNum == 1
			&& role.getOwner() != UNDEF_NODEID) {
		return false;
	}
	if (role.getOwner() != UNDEF_NODEID
			&& role.getBackupSize() >= backupNum) {
		return false;
	}
	if (role.isOwnerOrBackup(nodeId)) {
		return false;
	}
	bool assigned = true;
	if (role.getOwner() == UNDEF_NODEID) {
		assert(partitionCountList[pId] == 0);
		role.setOwner(nodeId);
		dataCountList[nodePos]++;
		partitionCountList[pId]++;
	}
	else if (role.getBackupSize() < backupNum) {
		NodeIdList &backups = role.getBackups();
		backups.push_back(nodeId);
		dataCountList[nodePos]++;
		partitionCountList[pId]++;
	}
	else {
		assigned = false;
	}
	role.checkBackup();
	return assigned;
}

bool checkAssingedNum(
		util::Vector<int32_t> &partitionCountList,
		int32_t limit) {

	for (size_t pos = 0;
			pos < partitionCountList.size(); pos++) {
		if (partitionCountList[pos] < limit) {
			return false;
		}
	}
	return true;
}

struct CheckNodeInfo {

	CheckNodeInfo(
			NodeId nodeId,
			util::Vector<PartitionId> *priortyList,
			util::Vector<PartitionId> *priortyBackupList) :
					nodeId_(nodeId),
					priortyList_(priortyList),
					priortyBackupList_(priortyBackupList) {}

	bool operator<(const CheckNodeInfo& another) const {
			return (priortyList_->size() + priortyBackupList_->size())
				> (another.priortyList_->size()
						+ another.priortyBackupList_->size());
	}

	NodeId nodeId_;
	util::Vector<PartitionId> *priortyList_;
	util::Vector<PartitionId> *priortyBackupList_;
};

void PartitionTable::assignGoalTable(
		util::StackAllocator &alloc,
		util::XArray<NodeId> &nodeIdList,
		util::Vector<PartitionRole> &roleList) {

	int32_t backupNum = getReplicationNum();
	int32_t partitionNum = getPartitionNum();
	int32_t nodeNum = static_cast<int32_t>(nodeIdList.size());
	if (backupNum > nodeNum - 1) {
		backupNum = nodeNum - 1;
	}
	util::Vector<int32_t> dataCountList(
			nodeNum, 0, alloc);
	util::Vector<int32_t> partitionCountList(
			partitionNum, 0, alloc);
	util::Vector<util::Vector<PartitionId> >
			priorityPartitionLists(alloc);
	util::Vector<bool> priorityPartitionAssignedList(
			partitionNum, false, alloc);
	util::Vector<util::Vector<PartitionId> >
			priorityPartitionBackupLists(alloc);

	util::Deque<CheckNodeInfo> nodeQueue(alloc);
	util::Vector<PartitionId> adjustPartitionList(alloc);
	for (int32_t pos = 0; pos < nodeNum; pos++) {
		util::Vector<PartitionId>
				tmpPriorityPartitionList(alloc);
		priorityPartitionLists.push_back(
				tmpPriorityPartitionList);
		priorityPartitionBackupLists.push_back(
				tmpPriorityPartitionList);
	}

	util::Vector<LogSequentialNumber>
			maxLsnList(getPartitionNum(), 0, alloc);
	for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
		PartitionRole role(pId, revision_, PT_CURRENT_OB);
		roleList.push_back(role);
		LogSequentialNumber maxLsn = 0;
		for (size_t pos = 0;
				pos < nodeIdList.size(); pos++) {
			LogSequentialNumber currentLsn = getLSN(pId, nodeIdList[pos]);
			if (currentLsn > maxLsn) {
				maxLsn = currentLsn;
			}
		}
		maxLsnList[pId] = maxLsn;
	}
	util::Random random(
			static_cast<int64_t>(util::DateTime::now(false).getUnixTime()));

	for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
		for (size_t pos = 0; pos < nodeIdList.size(); pos++) {
			LogSequentialNumber lsn = getLSN(pId, nodeIdList[pos]);
			if (maxLsnList[pId] != 0) {
				if (lsn == maxLsnList[pId]) {
					priorityPartitionLists[pos].push_back(pId);
					priorityPartitionAssignedList[pId] = true;
				}
				else if (lsn != 0
						&& !checkLimitOwnerBackupLsnGap(maxLsnList[pId], lsn)) {
					priorityPartitionBackupLists[pos].push_back(pId);
				}
			}
		}
	}
	for (int32_t pos = 0; pos < nodeNum; pos++) {
		CheckNodeInfo node(pos, &priorityPartitionLists[pos],
				&priorityPartitionBackupLists[pos]);
		nodeQueue.push_back(node);
	}

	int64_t nodeNumCounter = 0;
	int32_t totalAssignCount = 0;
	int32_t dataTotalLimit = (1 + backupNum) * getPartitionNum(); 

	int32_t checkReplicaNum = backupNum + 1;
	while (totalAssignCount < dataTotalLimit) {
		bool isAssigned = false;
		if (nodeNumCounter % nodeNum == 0) {
			std::sort(nodeQueue.begin(), nodeQueue.end());
		}
		nodeNumCounter++;
		NodeId targetNodePos = nodeQueue.front().nodeId_;
		nodeQueue.pop_front();
		NodeId targetNodeId = nodeIdList[targetNodePos];
		util::Vector<PartitionId> &priorityPartitionList
				= priorityPartitionLists[targetNodePos];
		util::Vector<PartitionId> &priorityPartitionBackupList
				= priorityPartitionBackupLists[targetNodePos];
		for (util::Vector<PartitionId>::iterator
				it = priorityPartitionList.begin();
				it != priorityPartitionList.end(); it++) {

			PartitionId targetPartitionId = (*it);
			PartitionRole &role = roleList[targetPartitionId];

			if (assignPartition(targetPartitionId, targetNodeId, 
					targetNodePos, role, dataCountList, 
					partitionCountList, checkReplicaNum)) {
				totalAssignCount++;
				nodeQueue.push_back(
						CheckNodeInfo(targetNodePos,
								&priorityPartitionList, &priorityPartitionBackupList));
				priorityPartitionList.erase(it);
				isAssigned = true;
				break;
			}
		}
		if (isAssigned) {
			continue;
		}
		adjustPartitionList.clear();
		for (PartitionId pId = 0;
				pId < static_cast<uint32_t>(partitionNum); pId++) {
			if (partitionCountList[pId] < checkReplicaNum) {
				adjustPartitionList.push_back(pId);
			}
		}
		for (util::Vector<PartitionId>::iterator
				it = priorityPartitionBackupList.begin();
				it != priorityPartitionBackupList.end(); it++) {

			PartitionId targetPartitionId = (*it);
			PartitionRole &role = roleList[targetPartitionId];
			if (role.getOwner() != UNDEF_NODEID
				&& assignPartition(targetPartitionId, targetNodeId, 
					targetNodePos, role, dataCountList, 
					partitionCountList, checkReplicaNum)) {
					totalAssignCount++;
				nodeQueue.push_back(
				CheckNodeInfo(
					targetNodePos, &priorityPartitionLists[targetNodePos],
							&priorityPartitionBackupLists[targetNodePos]));
				priorityPartitionBackupList.erase(it);
					isAssigned = true;
				break;
			}
		}
		if (!isAssigned) {
			for (util::Vector<PartitionId>::iterator
					it = adjustPartitionList.begin();
					it != adjustPartitionList.end(); it++) {

				PartitionId targetPartitionId = (*it);
				PartitionRole &role = roleList[targetPartitionId];
				if (assignPartition(
						targetPartitionId, targetNodeId, 
						targetNodePos, role, dataCountList, 
						partitionCountList, checkReplicaNum)) {

					totalAssignCount++;
					nodeQueue.push_back(
						CheckNodeInfo(
							targetNodePos, &priorityPartitionLists[targetNodePos],
									&priorityPartitionBackupLists[targetNodePos]));
					isAssigned = true;
					break;
				}
			}
		}
		if (!isAssigned) {
			nodeQueue.push_back(
				CheckNodeInfo(
						targetNodePos, &priorityPartitionLists[targetNodePos],
						&priorityPartitionBackupLists[targetNodePos]));
		}
	}
}

void setNodePosition(
		util::Vector<int32_t> &nodeMap,
		NodeId nodeId,
		int32_t pos) {

	if (nodeMap.size() <= static_cast<size_t>(nodeId)) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}
	nodeMap[nodeId] = pos;
}

bool PartitionTable::applyInitialRule(
		PartitionContext &context) {

	SubPartitionTable &subPartitionTable
			= context.subPartitionTable_;
	subPartitionTable.clear();
	util::NormalOStringStream strstrm;
	util::Random random(
			static_cast<int64_t>(util::DateTime::now(false)
					.getUnixTime()));
	util::XArray<NodeId> activeNodeList(context.getAllocator());
	getLiveNodeIdList(activeNodeList);	

	for (PartitionId pId = 0;
			pId < getPartitionNum(); pId++) {

		LogSequentialNumber maxLsn = getMaxLsn(pId);
		if (getOwner(pId) == UNDEF_NODEID) {
			PartitionRole nextRole(pId, revision_, PT_CURRENT_OB);
			PartitionRole goalRole;
			getPartitionRole(pId, goalRole, PT_CURRENT_GOAL);
			NodeId goalOwner = goalRole.getOwner();
			if (goalOwner == UNDEF_NODEID) {
				continue;
			}
			LogSequentialNumber goalLsn = getLSN(pId, goalOwner);
			
			if (goalLsn >= maxLsn && context.isLoadBalance_) {
				NodeIdList &nextBackups = nextRole.getBackups();
				NodeIdList &goalBackups = goalRole.getBackups();
				nextRole.setOwner(goalOwner);
				LogSequentialNumber ownerLsn
						= getLSN(pId, goalOwner);
				LogSequentialNumber ownerStartLsn
						= getStartLSN(pId, goalOwner);
				
				for (size_t pos = 0;
						pos < goalBackups.size(); pos++) {
					NodeId candBackup = goalBackups[pos];
					if (nextBlockQueue_.find(pId, candBackup)) {
						continue;
					}
					LogSequentialNumber backupLSN
							= getLSN(pId, candBackup);
					if (checkNextBackup(
							backupLSN, ownerStartLsn, ownerLsn)) {
						nextBackups.push_back(candBackup);
					}
				}
				setAvailable(pId, true);
				subPartitionTable.set(this, pId, nextRole, goalOwner);
			}
			else {
				NodeId targetNodeId = -1;
				LogSequentialNumber tmpMaxLSN = 0;
				NodeId backupId = UNDEF_NODEID;
				arrayShuffle(activeNodeList,
						static_cast<uint32_t>(activeNodeList.size()), random);

				for (size_t pos = 0;
						pos < activeNodeList.size(); pos++) {
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
					if (isAvailable(pId)) {
						util::NormalOStringStream strstrm;
						try {
							strstrm << "Target partition owner is not assgined, pId="
									<< pId << ", expected maxLSN=" << maxLsn
									<< ", actual maxLSN=" << tmpMaxLSN;
							GS_THROW_USER_ERROR(
									GS_ERROR_CS_PARTITION_OWNER_NOT_ASSGIN,
									strstrm.str().c_str());
						}
						catch (std::exception &e) {
							UTIL_TRACE_EXCEPTION_WARNING(CLUSTER_OPERATION, e, "");
						}
					}
					loadInfo_.status_ = PT_OWNER_LOSS;
					setAvailable(pId, false);
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
				NodeIdList &nextBackups = nextRole.getBackups();
				NodeIdList &goalBackups = goalRole.getBackups();
				NodeIdList checkRole = goalBackups;

				if (context.isLoadBalance_) {
					if (targetNodeId != goalOwner) {
						checkRole.push_back(goalOwner);
					}
					LogSequentialNumber ownerLsn
							= getLSN(pId, targetNodeId);
					LogSequentialNumber ownerStartLsn
							= getStartLSN(pId, targetNodeId);

					for (size_t pos = 0;
							pos < checkRole.size(); pos++) {
						NodeId candBackup = checkRole[pos];
						if (candBackup == targetNodeId) continue;
						if (nextBlockQueue_.find(pId, candBackup)) {
							continue;
						}
						LogSequentialNumber backupLSN
								= getLSN(pId, candBackup);
						if (checkNextBackup(backupLSN,
								ownerStartLsn, ownerLsn)) {
							nextBackups.push_back(candBackup);
						}
					}
				}
				setAvailable(pId, true);
				subPartitionTable.set(
						this, pId, nextRole, targetNodeId);
			}
		}
		else if (context.isConfigurationChange()) {

			PartitionRole goalRole;
			getPartitionRole(pId, goalRole, PT_CURRENT_GOAL);
			PartitionRole currentRole;
			getPartitionRole(pId, currentRole);
			NodeIdList &goalBackups = goalRole.getBackups();
			NodeIdList &nextBackups = currentRole.getBackups();
			currentRole.setRevision(revision_);
			currentRole.clearCatchup();

			int32_t backupNum = currentRole.getBackupSize();
			if (backupNum < context.backupNum_
					&&  context.isLoadBalance_) {

				NodeIdList checkRole = goalBackups;
				NodeId ownerNodeId = currentRole.getOwner();
				if (ownerNodeId != UNDEF_NODEID) {
					LogSequentialNumber ownerLsn
							= getLSN(pId, ownerNodeId);
					LogSequentialNumber ownerStartLsn
							= getStartLSN(pId, ownerNodeId);
					if (goalRole.getOwner() != ownerNodeId) {
						checkRole.push_back(goalRole.getOwner());
					}
					for (size_t pos = 0; pos < checkRole.size(); pos++) {
						NodeId candBackup = checkRole[pos];
						if (nextBlockQueue_.find(pId, candBackup)) {
							continue;
						}
						if (!currentRole.isOwnerOrBackup(candBackup)) {
							LogSequentialNumber backupLSN
									= getLSN(pId, candBackup);
							if (checkNextBackup(backupLSN,
									ownerStartLsn, ownerLsn)) {
								nextBackups.push_back(candBackup);
							}
						}
						if (nextBackups.size() >= static_cast<size_t>(
								context.backupNum_)) {
							break;
						}
					}
				}
				setAvailable(pId, true);
				subPartitionTable.set(
						this, pId, currentRole, getOwner(pId));
			}
		}
	}
	if (subPartitionTable.size() > 0) {
		prevCatchupPId_ = UNDEF_PARTITIONID;
		prevCatchupNodeId_ = UNDEF_NODEID;
		return true;
	}
	else {
		return false;
	}
}

bool PartitionTable::applyAddRule(
		PartitionContext &context) {

	SubPartitionTable &subPartitionTable
			= context.subPartitionTable_;	
	subPartitionTable.clear();

	util::StackAllocator &alloc = context.getAllocator();
	util::StackAllocator::Scope scope(alloc);

	size_t targetBackupNum;
	size_t replicationNum = static_cast<size_t>(getReplicationNum());
	
	util::XArray<ReplicaInfo> resolvePartitonList(alloc);
	util::XArray<NodeId> liveNodeList(alloc);
	getLiveNodeIdList(liveNodeList);

	size_t backupNum = 0;
	size_t currentNodeNum = static_cast<int32_t>(liveNodeList.size());
	
	if (replicationNum > currentNodeNum - 1) {
		targetBackupNum = currentNodeNum - 1;
	}
	else {
		targetBackupNum = replicationNum;
	}
	PartitionId pId;

	util::XArray<PartitionId> pIdList(alloc);
	for (int32_t p = 0; static_cast<uint32_t>(p)
			< getPartitionNum(); p++) {
		pIdList.push_back(p);
	}
	util::Random random(static_cast<int64_t>(
			util::DateTime::now(false).getUnixTime()));
	arrayShuffle(pIdList,
			static_cast<uint32_t>(pIdList.size()), random);

	for (size_t pos = 0; pos < pIdList.size(); pos++) {
		pId = pIdList[pos];
		backupNum = getBackupSize(alloc, pId);
		ReplicaInfo repInfo(pId, static_cast<uint32_t>(backupNum));
		resolvePartitonList.push_back(repInfo);
	}
	std::sort(resolvePartitonList.begin(),
			resolvePartitonList.end(), cmpReplica);
	util::Vector<PartitionId> catchupPIdList(alloc);

	for (size_t pos = 0;
			pos < resolvePartitonList.size(); pos++) {

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
	}

	for (size_t pos = 0;
			pos < resolvePartitonList.size(); pos++) {

		PartitionId targetPId = resolvePartitonList[pos].pId_;
		NodeId currentOwner = getOwner(targetPId);
		if (currentOwner == UNDEF_NODEID) {
			continue;
		}
		PartitionRole currentRole;

		getPartitionRole(
				targetPId, currentRole, PT_CURRENT_OB);
		PartitionRole nextRole(
				targetPId, revision_, PT_CURRENT_OB);
		PartitionRole goalRole;
		getPartitionRole(
				targetPId, goalRole, PT_CURRENT_GOAL);
		NodeId goalOwner = goalRole.getOwner();
		if (goalOwner == UNDEF_NODEID) {
			continue;
		}

		NodeIdList &currentBackups
				= currentRole.getBackups();
		NodeIdList &goalBackups
				= goalRole.getBackups();
		NodeIdList catchups;
		if (!currentRole.isOwnerOrBackup(goalOwner)) {
			if (goalBlockQueue_.find(targetPId, goalOwner)) {
				continue;
			}
			catchups.push_back(goalOwner);
			nextRole.set(
					currentOwner, currentBackups, catchups);
			subPartitionTable.set(
					this, targetPId, nextRole, currentOwner);
			prevCatchupPId_ = targetPId;
			prevCatchupNodeId_ = goalOwner;
		}
		if (subPartitionTable.size() == 1) {
			break;
		}
		util::StackAllocator::Scope scope(alloc);
		util::XArray<NodeId> backupOrderList(alloc);
		for (size_t pos = 0; pos < goalBackups.size(); pos++) {
			if (nextBlockQueue_.find(
					targetPId, goalBackups[pos])) {
				continue;
			}
			if (currentRole.isOwnerOrBackup(
					goalBackups[pos])) {
				continue;
			}
			if (goalBlockQueue_.find(
					targetPId, goalBackups[pos])) {
				continue;
			}
			backupOrderList.push_back(
					goalBackups[pos]);
		}
		if (backupOrderList.size() > 0) {
			arrayShuffle(backupOrderList,
					static_cast<uint32_t>(backupOrderList.size()),
					random);

			catchups.push_back(backupOrderList[0]);
			nextRole.set(
					currentOwner, currentBackups, catchups);
			subPartitionTable.set(
					this, targetPId, nextRole, currentOwner);
			prevCatchupPId_ = targetPId;
			prevCatchupNodeId_ = backupOrderList[0];
		}
		if (subPartitionTable.size() == 1) {
			break;
		}
	}
	if (subPartitionTable.size() > 0) {
		PartitionId catchupPId
				= subPartitionTable.getSubPartition(0).pId_;

		for (size_t pos = 0;
				pos < catchupPIdList.size(); pos++) {

			PartitionId checkPId = catchupPIdList[pos];
			if (catchupPId != catchupPIdList[pos]) {
				PartitionRole nextRole;
				getPartitionRole(
						checkPId, nextRole, PT_CURRENT_OB);
				nextRole.clearCatchup();
				setPartitionRole(
						checkPId, nextRole, PT_CURRENT_OB);
				NodeId currentOwner = getOwner(checkPId);
				subPartitionTable.set(
						this, checkPId, nextRole, currentOwner);
			}
		}
		return true;
	}
	else {
		return false;
	}
}

bool PartitionTable::applyRemoveRule(
		PartitionContext &context) {

	SubPartitionTable &subPartitionTable
			= context.subPartitionTable_;	
	subPartitionTable.clear();

	for (PartitionId pId = 0;
			pId < getPartitionNum(); pId++) {
		NodeId currentOwner = getOwner(pId);
		if (currentOwner == UNDEF_NODEID) {
			continue;
		}
		PartitionRole currentRole, goalRole;
		getPartitionRole(
				pId, currentRole, PT_CURRENT_OB);
		getPartitionRole(
				pId, goalRole, PT_CURRENT_GOAL);
		PartitionRole nextRole(
				pId, revision_, PT_CURRENT_OB);
		NodeId goalOwner = goalRole.getOwner();
		if (goalOwner == UNDEF_NODEID) {
			continue;
		}
		NodeIdList &currentBackups
				= currentRole.getBackups();
		NodeIdList nextBackups;
		NodeIdList nextCatchups;

		if (!currentRole.isOwnerOrBackup(goalOwner)) {
			continue;
		}
		bool diff = false;
		for (size_t pos = 0;
				pos < currentBackups.size(); pos++) {

			if (nextBlockQueue_.find(
					pId, currentBackups[pos])) {
				continue;
			}
			if (goalRole.isOwnerOrBackup(currentBackups[pos])) {
				nextBackups.push_back(currentBackups[pos]);
			}
			else {
				diff = true;
			}
		}
		if (diff) {

			if (nextBackups.size() >=
					static_cast<size_t>(context.backupNum_)) {
				nextRole.set(
						currentOwner, nextBackups, nextCatchups);
				subPartitionTable.set(
						this, pId, nextRole, currentOwner);
			}
			else {
				if (currentBackups.size()
						> static_cast<size_t>(context.backupNum_)) {
					for (size_t pos = 0;
							pos < currentBackups.size(); pos++) {
						if (!goalRole.isOwnerOrBackup(
								currentBackups[pos])) {
							nextBackups.push_back(currentBackups[pos]);
							if (currentBackups.size()
									>= static_cast<size_t>(context.backupNum_)) {
								nextRole.set(
										currentOwner, nextBackups, nextCatchups);
								subPartitionTable.set(
										this, pId, nextRole, currentOwner);
								break;
							}
						}
					}
				}
			}
		}
	}

	if (subPartitionTable.size() > 0) {
		prevCatchupPId_ = UNDEF_PARTITIONID;
		prevCatchupNodeId_ = UNDEF_NODEID;
		return true;
	}
	else {
		return false;
	}
}

bool PartitionTable::applySwapRule(
		PartitionContext &context) {

	SubPartitionTable &subPartitionTable
			= context.subPartitionTable_;	
	subPartitionTable.clear();
	
	for (PartitionId pId = 0;
			pId < getPartitionNum(); pId++) {
		NodeId currentOwner = getOwner(pId);
		if (currentOwner == UNDEF_NODEID) {
			continue;
		}
		PartitionRole currentRole, goalRole;
		getPartitionRole(
				pId, currentRole, PT_CURRENT_OB);
		getPartitionRole(
				pId, goalRole, PT_CURRENT_GOAL);
		PartitionRole nextRole(
				pId, revision_, PT_CURRENT_OB);

		NodeId goalOwner = goalRole.getOwner();
		if (currentOwner == goalOwner) {
			continue;
		}
		NodeIdList &currentBackups
				= currentRole.getBackups();
		NodeIdList &goalBackups
				= goalRole.getBackups();

		NodeIdList nextBackups;
		NodeIdList nextCatchups;
		if (!currentRole.isOwnerOrBackup(goalOwner)) {
			continue;
		}
		bool flag = true;
		for (size_t pos = 0;
				pos < goalBackups.size(); pos++) {
			if (nextBlockQueue_.find(
					pId, goalBackups[pos])) {
				continue;
			}
			if (!currentRole.isOwnerOrBackup(
					goalBackups[pos])) {
				flag = false;
				break;
			}
		}
		if (!flag) {
			continue;
		}
		nextBackups.push_back(currentOwner);
		for (size_t pos = 0;
				pos < currentBackups.size(); pos++) {
			if (nextBlockQueue_.find(
					pId, currentBackups[pos])) {
				continue;
			}
			if (!goalRole.isOwnerOrBackup(
					currentBackups[pos])) {
				continue;
			}
			if (currentBackups[pos] != goalOwner) {
				nextBackups.push_back(
						currentBackups[pos]);
			}
		}
		nextRole.set(
				goalOwner, nextBackups, nextCatchups);
		subPartitionTable.set(
				this, pId, nextRole, currentOwner);
	}

	if (subPartitionTable.size() > 0) {
		prevCatchupPId_ = UNDEF_PARTITIONID;
		prevCatchupNodeId_ = UNDEF_NODEID;
		return true;
	}
	else {
		return false;
	}
}

void PartitionTable::planNext(
		PartitionContext &context) {

	util::StackAllocator &alloc = context.getAllocator();
	util::StackAllocator::Scope scope(alloc);
	util::Vector<SubPartition> nextPartitionList(alloc);
	util::Vector<SubPartition> currentPartitionList(alloc);
	currentRuleNo_ = PARTITION_RULE_UNDEF;
	util::Vector<NodeId> activeNodeList(alloc);
	getLiveNodeIdList(activeNodeList);
	int32_t backupNum = getReplicationNum();
	int32_t liveNum = static_cast<int32_t>(activeNodeList.size());
	nextApplyRuleLimitTime_
			= context.currentRuleLimitTime_;
	
	if (backupNum > liveNum - 1) {
		context.backupNum_ = liveNum - 1;
	}
	else {
		context.backupNum_ = backupNum;
	}
	while (true) {
		if (applyInitialRule(context)) {
			currentRuleNo_ = PARTITION_RULE_INITIAL;
			nextApplyRuleLimitTime_
					= context.currentRuleLimitTime_;
			break;
		}
		else if (applyRemoveRule(context)) {
			currentRuleNo_ = PARTITION_RULE_REMOVE;
			nextApplyRuleLimitTime_
					= context.currentRuleLimitTime_;
			break;
		}
		else if (applySwapRule(context)) {
			currentRuleNo_ = PARTITION_RULE_SWAP;
			nextApplyRuleLimitTime_
					= context.currentRuleLimitTime_;
			break;
		}
		else if (!context.isLoadBalance_) {
			break;
		}
		else if (applyAddRule(context)) {
			currentRuleNo_ = PARTITION_RULE_ADD;
			nextApplyRuleLimitTime_
					= context.currentRuleLimitTime_;
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
			std::endl << "Apply Rule:" 
					<< dumpPartitionRule(currentRuleNo_)
					<< ", revision=" << revision);

		SubPartitionTable &subPartitionTable
				= context.subPartitionTable_;	
		if (subPartitionTable.size() > 0) {
			currentRevisionNo_ = revision;
			context.needUpdatePartition_ = true;
			GS_TRACE_CLUSTER_INFO(
					"Next partitions  : " 
					<< std::endl 
					<< dumpPartitionsNew(
							alloc, PT_CURRENT_OB, &subPartitionTable));
		}
	}	
	context.subPartitionTable_.revision_
			= getPartitionRevision();
	AddressInfoList &nodeList
			= context.subPartitionTable_.getNodeAddressList();
	AddressInfoList &publicNodeList
			= context.subPartitionTable_.getPublicNodeAddressList();
	getNodeAddressInfo(nodeList, publicNodeList);
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


bool PartitionTable::checkTargetPartition(
		PartitionId pId,
		int32_t backupNum,
		bool &catchupWait) {

	PartitionRole currentRole;
	getPartitionRole(pId, currentRole);
	NodeId currentOwnerId
			= currentRole.getOwner();
	NodeIdList &currentBackups
			= currentRole.getBackups();
	NodeIdList &currentCatchups
			= currentRole.getCatchups();
	bool detectError = false;

	if (currentOwnerId == UNDEF_NODEID) {
		loadInfo_.status_ = PT_OWNER_LOSS;
	}
	if (loadInfo_.status_ != PT_OWNER_LOSS) {
		if (currentBackups.size()
				< static_cast<size_t>(backupNum)) {
			loadInfo_.status_ = PT_REPLICA_LOSS;
		}
	}
	if (currentOwnerId != UNDEF_NODEID 
			&& getPartitionRoleStatus(pId, currentOwnerId)
					!= PT_OWNER) {

		detectError = true;
		GS_TRACE_CLUSTER_INFO(
				"Unmatch owner role, pId="
				<< pId << ", ownerAddress=" 
				<< dumpNodeAddress(currentOwnerId)
				<< ", expected=OWNER, actual=" 
				<< dumpPartitionRoleStatus(
						getPartitionRoleStatus(pId, currentOwnerId)));
	}
	for (size_t pos = 0;
			pos < currentBackups.size(); pos++) {

		PartitionRoleStatus roleStatus
				= getPartitionRoleStatus(pId, currentBackups[pos]);
		if (roleStatus != PT_BACKUP) {
			if (roleStatus == PT_CATCHUP) {
				catchupWait = true;
			}
			else {
				detectError = true;
				GS_TRACE_CLUSTER_INFO(
						"Unmatch backup role, pId=" << pId
					<< ", backupAddress=" 
					<< dumpNodeAddress(currentBackups[pos])
					<< ", expected=BACKUP, actual=" 
					<< dumpPartitionRoleStatus(roleStatus));
			}
		}
	}
	if (getRuleNo() ==PARTITION_RULE_ADD
			&& prevCatchupPId_ == pId) {

		if (currentCatchups.size() == 0) {
			PartitionRoleStatus roleStatus
					= getPartitionRoleStatus(pId, prevCatchupNodeId_);
			if (roleStatus != PT_BACKUP) {
				detectError = true;
			}
		}
		else {
			if (currentCatchups[0] != prevCatchupNodeId_) {
				detectError = true;
			}
		}
	}

	if (getRuleNo() !=PARTITION_RULE_ADD
			&& currentCatchups.size() > 0) {
		clearCatchupRole(pId);
		return true;
	}
	if (currentOwnerId != UNDEF_NODEID 
			&& getErrorStatus(pId, currentOwnerId)
				== PT_ERROR_LONG_SYNC_FAIL) {
		clearCatchupRole(pId);
		return true;
	}

	for (size_t pos = 0;
			pos < currentCatchups.size(); pos++) {

		if (getErrorStatus(pId, currentCatchups[pos])
				== PT_ERROR_LONG_SYNC_FAIL) {
			clearCatchupRole(pId);
			return true;
		}
	}
	for (size_t pos = 0;
			pos < currentCatchups.size(); pos++) {

		PartitionRoleStatus roleStatus
				= getPartitionRoleStatus(pId, currentCatchups[pos]);
		if (roleStatus != PT_CATCHUP) {
			if (roleStatus == PT_BACKUP) {
				PartitionRole currentRole;
				getPartitionRole(pId, currentRole);
				currentRole.promoteCatchup();
				setPartitionRole(pId, currentRole);
			}
			else {
				setBlockQueue(
						pId, PT_CHANGE_GOAL_TABLE, currentCatchups[pos]);
				detectError = true;
				GS_TRACE_CLUSTER_INFO(
						"Unmatch catchup role, pId=" << pId 
						<< ", catchupAddress=" 
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

void PartitionTable::checkRepairPartition(
		PartitionContext &context) {

	util::StackAllocator &alloc = context.getAllocator();
	if (context.isRepairPartition_) {

		util::StackAllocator::Scope scope(alloc);
		util::Vector<NodeId> liveNodeList(alloc);
		getLiveNodeIdList(liveNodeList);
		for (PartitionId pId = 0;
				pId < getPartitionNum(); pId++) {

			LogSequentialNumber prevMaxLsn
					= getMaxLsn(pId);
			LogSequentialNumber maxLSN = 0;
			for (size_t pos = 0;
					pos < liveNodeList.size(); pos++) {

				LogSequentialNumber lsn = getLSN(pId, liveNodeList[pos]);
				if (lsn > maxLSN) {
					maxLSN = lsn;
				}
			}
			setRepairedMaxLsn(pId, maxLSN);
			if (maxLSN != prevMaxLsn) {
				GS_TRACE_CLUSTER_INFO(
					"Max Lsn updated, pId=" << pId << ", prevMaxLsn="
					<< prevMaxLsn << ", currentMaxLsn=" << maxLSN);
			}
		}
	}
}

void PartitionTable::setBlockQueue(
		PartitionId pId,
		ChangePartitionTableType tableType,
		NodeId nodeId) {

	if (pId >= getPartitionNum() || nodeId >= getNodeNum()) {
		GS_THROW_USER_ERROR(
				GS_ERROR_PT_SET_INVALID_NODE_ID, "");
	}
	if (tableType == PT_CHANGE_NEXT_TABLE) {
		nextBlockQueue_.push(pId, nodeId);
	}
	else if (tableType == PT_CHANGE_GOAL_TABLE) {
		goalBlockQueue_.push(pId, nodeId);
	}
}

void updateOwnerLoad(
		util::Vector<int32_t> &ownerCountList,
		int32_t &maxOwnerCount,
		int32_t &minOwnerCount,
		int32_t &maxOwnerPos,
		int32_t &minOwnerPos) {

	maxOwnerCount = -1;
	minOwnerCount = INT32_MAX;
	maxOwnerPos = -1;
	minOwnerPos = 0;
	for (size_t pos = 0;
			pos < ownerCountList.size(); pos++) {
		if (ownerCountList[pos] > maxOwnerCount) {
			maxOwnerCount = ownerCountList[pos];
			maxOwnerPos = static_cast<int32_t>(pos);
		}
		if (ownerCountList[pos] < minOwnerCount) {
			minOwnerCount = ownerCountList[pos];
			minOwnerPos = static_cast<int32_t>(pos);
		}
	}
}

int32_t getNodePosition(
		util::Vector<int32_t> &nodeMap, NodeId nodeId) {

	if (nodeMap.size() <= static_cast<size_t>(nodeId)) {
		return -1;
	}
	return nodeMap[nodeId];
}

void PartitionTable::modifyGoalTable(
		util::StackAllocator &alloc,
		util::XArray<NodeId> &nodeIdList,
		util::Vector<PartitionRole> &roleList) {

	int32_t nodeNum = static_cast<int32_t>(nodeIdList.size());
	int32_t maxNodeNum = getNodeNum();
	int32_t replicationNum = getReplicationNum() + 1;

	if (nodeNum == 0) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}
	util::Vector<int32_t> ownerCountList(
			nodeNum, 0, alloc);
	util::Vector<int32_t> nodeMap(
			maxNodeNum, -1, alloc);
	for (size_t pos = 0;
			pos < static_cast<size_t>(nodeNum); pos++) {
		setNodePosition(nodeMap, nodeIdList[pos], static_cast<int32_t>(pos));
	}

	for (PartitionId pId = 0;
			pId < getPartitionNum(); pId++) {

		NodeId owner = roleList[pId].getOwner();
		if (owner != UNDEF_NODEID) {
			int32_t position = getNodePosition(nodeMap, owner);
			if (position == -1) continue;
			ownerCountList[position]++;
		}
	}
	int32_t maxOwnerCount = 0;
	int32_t minOwnerCount = INT32_MAX;
	int32_t maxOwnerPos = -1;
	int32_t minOwnerPos = 0;

	util::XArray<PartitionId> pIdList(alloc);
	for (PartitionId p = 0; p < getPartitionNum(); p++) {
		pIdList.push_back(p);
	}
	util::Random random(static_cast<int64_t>(
			util::DateTime::now(false).getUnixTime()));
	arrayShuffle(pIdList,
			static_cast<uint32_t>(pIdList.size()), random);
	PartitionId targetPId;
	for (int32_t k = 0; k < replicationNum; k++) {

		for (size_t i = 0; i < pIdList.size(); i++) {

			targetPId = pIdList[i];
			updateOwnerLoad(ownerCountList, maxOwnerCount,
					minOwnerCount, maxOwnerPos, minOwnerPos);
			if (maxOwnerCount - minOwnerCount<= 2) {
				return;
			}
			PartitionRole &role = roleList[targetPId];
			NodeId owner = role.getOwner();
			if (owner == UNDEF_NODEID) continue;
			int32_t ownerPosition = getNodePosition(nodeMap, owner);
			if (ownerPosition == -1) {
				continue;
			}

			if (ownerCountList[ownerPosition] == maxOwnerCount) {
				NodeIdList &backups = role.getBackups();
				if (backups.size() == 0) {
					continue;
				}
				int32_t minCount = INT32_MAX;
				size_t minPos = 0;
				for (size_t pos = 0;
						pos < backups.size(); pos++) {

					int32_t backupPosition = getNodePosition(nodeMap, backups[pos]);
					if (backupPosition == -1) {
						continue;
					}
					if (ownerCountList[backupPosition] < minCount) {
						minCount = ownerCountList[backupPosition];
						minPos = pos;
					}
				}

				if (maxOwnerCount == minCount) {
					continue;
				}
				PartitionRole swapRole = role;
				if (minPos >= backups.size()) {
					continue;
				}
				NodeId swapOwner = backups[minPos];
				NodeIdList &swapBackups = swapRole.getBackups();
				swapRole.setOwner(swapOwner);
				swapBackups.clear();
				swapBackups.push_back(owner);

				for (size_t k = 0; k < backups.size(); k++) {
					if (backups[k] == swapOwner) continue;
					swapBackups.push_back(backups[k]);
				}

				roleList[targetPId] = swapRole;
				int32_t swapPosition = getNodePosition(
						nodeMap, swapOwner);
				if (swapPosition == -1) {
					continue;
				}
				ownerCountList[swapPosition]++;
				ownerCountList[ownerPosition]--;
			}
		}
	}
}
	
void PartitionTable::handleErrorStatus(
		PartitionId pId,
		NodeId targetNode,
		int32_t status) {

	if (status == PT_ERROR_LONG_SYNC_FAIL) {
		PartitionRole role;
		getPartitionRole(pId, role);
		NodeId owner = role.getOwner();
		if (owner == targetNode && role.hasCatchupRole()) {
			role.clearCatchup();
			setPartitionRole(pId, role);
		}
	}
}

void PartitionTable::setPublicAddressInfo(
		NodeId nodeId,
		AddressInfo &publicAddressInfo) {
	
	if (publicAddressInfo.isValid()) {
		if (!nodes_[nodeId].publicAddressInfo_.isValid()) {
			nodes_[nodeId].publicAddressInfo_.setNodeAddress(
					TRANSACTION_SERVICE,
					publicAddressInfo.transactionAddress_);
			nodes_[nodeId].publicAddressInfo_.setNodeAddress(
					SQL_SERVICE,
					publicAddressInfo.sqlAddress_);
		}
	}
}

void PartitionTable::getNodeAddressInfo(
		AddressInfoList &addressInfoList,
		AddressInfoList &publicAddressInfoList) {

	int32_t nodeNum = getNodeNum();
	for (int32_t nodeId = 0;
			nodeId < nodeNum; nodeId++) {

		AddressInfo addressInfo;
		addressInfo.setNodeAddress(
				CLUSTER_SERVICE, getNodeInfo(nodeId)
						.getNodeAddress(CLUSTER_SERVICE));
		addressInfo.setNodeAddress(
				SYNC_SERVICE, getNodeInfo(nodeId)
						.getNodeAddress(SYNC_SERVICE));
		addressInfo.setNodeAddress(
				TRANSACTION_SERVICE, getNodeInfo(nodeId)
						.getNodeAddress(TRANSACTION_SERVICE));
		addressInfo.setNodeAddress(
				SYSTEM_SERVICE, getNodeInfo(nodeId)
						.getNodeAddress(SYSTEM_SERVICE));
		addressInfo.setNodeAddress(
				SQL_SERVICE, getNodeInfo(nodeId)
						.getNodeAddress(SQL_SERVICE));
		if (getHeartbeatTimeout(nodeId) != UNDEF_TTL) {
			addressInfo.isActive_ = true;
		}
		else {
			addressInfo.isActive_ = false;
		}
		addressInfoList.push_back(addressInfo);

		if (getNodeInfo(nodeId).publicAddressInfo_.isValid()) {
			AddressInfo publicAddressInfo;
			publicAddressInfo.setNodeAddress(
				TRANSACTION_SERVICE,
				getNodeInfo(nodeId).publicAddressInfo_
						.getNodeAddress(TRANSACTION_SERVICE));
			publicAddressInfo.setNodeAddress(
				SQL_SERVICE,
				getNodeInfo(nodeId).publicAddressInfo_
						.getNodeAddress(SQL_SERVICE));
			publicAddressInfoList.push_back(
					publicAddressInfo);
		}
	}
}

bool PartitionTable::isBalance(
		uint32_t minLoad, uint32_t maxLoad, bool isOwnerCheck) {

	if (isOwnerCheck) {
		return (maxLoad - minLoad <= config_.ownerLoadLimit_);
	}
	else {
		return (maxLoad - minLoad <= config_.dataLoadLimit_);
	}
}


std::string AddressInfo::dump() {

	util::NormalOStringStream ss;
	ss << "NodeAddress:{"
			<< "cluster={" << clusterAddress_.dump() << "}, "
			<< "sync={" << syncAddress_.dump() << "}, "
			<< "transaction={" << transactionAddress_.dump() << "}, "
			<< "system={" << systemAddress_.dump() << "}, "
			<< "sql={" << sqlAddress_.dump() << "},"
			<< "isActive={" << isActive_ << "}"
			<< "}";

	return ss.str();
}

LoadSummary::LoadSummary(util::StackAllocator &alloc, int32_t nodeLimit) :
		ownerLoadList_(alloc),
		dataLoadList_(alloc),
		liveNodeList_(alloc),
		ownerLimit_(PT_OWNER_LOAD_LIMIT),
		dataLimit_(PT_DATA_LOAD_LIMIT),
		status_(PT_INITIAL),
		nodeNum_(0),
		ownerLoadCount_(0),
		dataLoadCount_(0) {

	ownerLoadList_.assign(static_cast<size_t>(nodeLimit), 0);
	dataLoadList_.assign(static_cast<size_t>(nodeLimit), 0);
	liveNodeList_.assign(static_cast<size_t>(nodeLimit), 0);
}

void LoadSummary::init(int32_t nodeNum) {

	std::fill(ownerLoadList_.begin(),
			ownerLoadList_.begin() + nodeNum, 0);

	std::fill(dataLoadList_.begin(),
			dataLoadList_.begin() + nodeNum, 0);

	minOwnerLoad_ = UINT32_MAX;
	maxOwnerLoad_ = 0;
	minDataLoad_ = UINT32_MAX;
	maxDataLoad_ = 0;
	
	nodeNum_ = nodeNum;
	ownerLoadCount_ = 0;
	dataLoadCount_ = 0;
}

void LoadSummary::add(NodeId nodeId, bool isOwner) {

	if (isOwner) {
		ownerLoadList_[nodeId]++;
	}
	else {
		dataLoadList_[nodeId]++;
	}
}

void LoadSummary::update() {

	for (NodeId nodeId = 0;
			nodeId < nodeNum_; nodeId++) {

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

bool LoadSummary::isBalance(bool isOwner, int32_t checkCount) {

	if (isOwner) {
		return (checkCount <= ownerLoadCount_ &&
				maxOwnerLoad_ - minOwnerLoad_ <= ownerLimit_);
	}
	else {
		return (checkCount <= dataLoadCount_ &&
				maxDataLoad_ - minDataLoad_ <= dataLimit_);
	}
}

std::string LoadSummary::dump() {

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

std::string DropNodeSet::dump() {

	util::NormalOStringStream oss;
	NodeAddress address(address_, port_);
	oss << "{address=" << address.dump();
	oss << ", dropInfo={";

	for (size_t pos = 0;
			pos < pIdList_.size(); pos++) {
		
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

void DropPartitionNodeInfo::init() {

	for (size_t pos = 0;pos < dropNodeMap_.size(); pos++) {
		dropNodeMap_[pos].init();
	}
}

void DropPartitionNodeInfo::set(
		PartitionId pId,
		NodeAddress &address,
		PartitionRevisionNo revision,
		NodeId nodeId) {

	util::Map<NodeAddress, DropNodeSet>::iterator
			it = tmpDropNodeMap_.find(address);

	if (it != tmpDropNodeMap_.end()) {
	
		(*it).second.append(pId, revision);
		if (static_cast<size_t>((*it).second.pos_)
				>= dropNodeMap_.size()) return;

		dropNodeMap_[(*it).second.pos_].append(
				pId, revision);
	}
	else {

		DropNodeSet target;
		target.setAddress(address);
		target.append(pId, revision);
		target.pos_
				= static_cast<int32_t>(dropNodeMap_.size());
		target.nodeId_ = nodeId;

		dropNodeMap_.push_back(target);
		tmpDropNodeMap_.insert(
				std::make_pair(address, target));
	}
}

std::string DropPartitionNodeInfo::dump() {
	util::NormalOStringStream oss;
	for (size_t pos = 0;
			pos < dropNodeMap_.size(); pos++) {
		oss << dropNodeMap_[pos].dump();
	}
	return oss.str().c_str();
}

std::string SubPartition::dump(PartitionTable *pt) {

	util::NormalOStringStream ss;
	ss << "Partition:{"
	<< "pId=" << pId_;
	if (pt == NULL) {
		ss << ", role:" << role_.dump();
	}
	else {
		ss << ", role:" << role_.dump(pt);
	}
	ss << ", current owner:"
		<< currentOwner_.toString() << "}";
	return ss.str();
}

std::string PartitionRevision::toString(bool isDetail) const {

	util::NormalOStringStream ss;
	if (isDetail) {
		ss << addr_ << ":" << port_ << ":" << sequentialNumber_;
	}
	else {
		ss << sequentialNumber_;
	}
	return ss.str();
}

void PartitionTable::GoalPartition::init(PartitionTable *pt) {

	util::LockGuard<util::Mutex> guard(lock_);
	partitionNum_ = pt->getPartitionNum();
	
	for (PartitionId pId = 0;
			pId < pt->getPartitionNum(); pId++) {
	
		PartitionRole role(
				pId, pt->getPartitionRevision(), PT_CURRENT_OB);
		role.encodeAddress(pt, role);
		role.checkBackup();
		role.checkCatchup();
		goalPartitions_.push_back(role);
	}
}

void PartitionTable::GoalPartition::set(PartitionTable *pt,
		util::Vector<PartitionRole> &goalList) {

	if (goalList.size() != partitionNum_) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}

	util::LockGuard<util::Mutex> guard(lock_);
	goalPartitions_.clear();
	for (size_t pos = 0; pos < goalList.size(); pos++) {
		if (goalList[pos].getPartitionId() != pos) {
			GS_THROW_USER_ERROR(
					GS_ERROR_PT_INTERNAL, "");
		}
		goalList[pos].encodeAddress(pt, goalList[pos]);
		goalList[pos].checkBackup();
		goalList[pos].checkCatchup();
		goalPartitions_.push_back(goalList[pos]);
	}
}

void PartitionTable::GoalPartition::setPartitionRole(PartitionRole &role) {
	util::LockGuard<util::Mutex> guard(lock_);
	goalPartitions_.push_back(role);
}

PartitionRole &PartitionTable::GoalPartition::getPartitionRole(PartitionId pId) {
	if (pId >= partitionNum_) {
		GS_THROW_USER_ERROR(
				GS_ERROR_PT_INTERNAL, "");
	}
	util::LockGuard<util::Mutex> guard(lock_);
	return goalPartitions_[pId];
}

size_t PartitionTable::GoalPartition::size() {
	util::LockGuard<util::Mutex> guard(lock_);
	return goalPartitions_.size();
}

void PartitionTable::GoalPartition::update(PartitionRole &role) {
	util::LockGuard<util::Mutex> guard(lock_);
	PartitionId rolePId = role.getPartitionId();
	if (goalPartitions_.size() <= rolePId) {
		return;
	}
	role.checkBackup();
	role.checkCatchup();
	goalPartitions_[rolePId] = role;
};

void PartitionTable::GoalPartition::copy(util::Vector<PartitionRole> &roleList) {
	util::LockGuard<util::Mutex> guard(lock_);
	for (size_t pos = 0;
			pos < goalPartitions_.size(); pos++) {
		roleList.push_back(goalPartitions_[pos]);
	}
}