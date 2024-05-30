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
#include "container_key.h"
#include <numeric>

#include "sql_processor.h"

UTIL_TRACER_DECLARE(CLUSTER_SERVICE);

NodeAddress::NodeAddress(const char8_t* addr, uint16_t port) {

	util::SocketAddress address(addr, port);
	address.getIP((util::SocketAddress::Inet*)&address_, &port_);
}

void NodeAddress::set(const char8_t* addr, uint16_t port) {

	util::SocketAddress address(addr, port);
	address.getIP((util::SocketAddress::Inet*)&address_, &port_);
}

void NodeAddress::set(const util::SocketAddress& socketAddress) {

	socketAddress.getIP((util::SocketAddress::Inet*)&address_, &port_);
}

/*!
	@brief Gets NodeAddress that correspond to a specified type
*/
NodeAddress& AddressInfo::getNodeAddress(ServiceType type) {
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
		GS_THROW_USER_ERROR(GS_ERROR_PT_INVALID_NODE_ADDRESS, "");
	}
}

/*!
	@brief Sets NodeAddress that correspond to a specified type
*/
void AddressInfo::setNodeAddress(ServiceType type, NodeAddress& address) {

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
		GS_THROW_USER_ERROR(GS_ERROR_PT_INVALID_NODE_ADDRESS, "");
	}
}

PartitionTable::PartitionTable(const ConfigTable& configTable, const char* configDir) :
	fixedSizeAlloc_(util::AllocatorInfo(ALLOCATOR_GROUP_CS, "partitionTableFixed"), 1 << 18),
	globalStackAlloc_(util::AllocatorInfo(ALLOCATOR_GROUP_CS, "partitionTableStack"), &fixedSizeAlloc_),
	nodeNum_(0),
	masterNodeId_(UNDEF_NODEID),
	partitionLockList_(NULL),
	goal_(NULL, NULL),
	currentRuleNo_(PARTITION_RULE_UNDEF),
	config_(configTable, MAX_NODE_NUM),
	prevMaxNodeId_(0),
	nextBlockQueue_(configTable.get<int32_t>(CONFIG_TABLE_DS_PARTITION_NUM)),
	goalBlockQueue_(configTable.get<int32_t>(CONFIG_TABLE_DS_PARTITION_NUM)),
	prevCatchupPId_(UNDEF_PARTITIONID),
	prevCatchupNodeId_(UNDEF_NODEID),
	partitions_(NULL),
	nodes_(NULL),
	enableRackZoneAwareness_(configTable.get<bool>(CONFIG_TABLE_CS_RACK_ZONE_AWARENESS)),
	orgEnableRackZoneAwareness_(enableRackZoneAwareness_),
	loadInfo_(globalStackAlloc_, MAX_NODE_NUM),
	goalProgress_(0),
	replicaProgress_(0),
	currentRevisionNo_(1),
	prevDropRevisionNo_(0),
	isGoalPartition_(false),
	hasPublicAddress_(false),
	goalAssignmentRule_(configTable.get<int32_t>(CONFIG_TABLE_CS_GOAL_ASSIGNMENT_RULE)),
	stableGoal_(configTable, configDir),
	reserveNum_(1),
	currentGoalAssignmentRule_(-1)
{

	try {
		if (goalAssignmentRule_ != PARTITION_ASSIGNMENT_RULE_DEFAULT
			&& enableRackZoneAwareness_) {
			GS_THROW_USER_ERROR(GS_ERROR_CS_CONFIG_ERROR, 
				"Rackzone awareness and partition assignement rule (NOT 'DEfAULT') can not be defined at the same time");
		}

		uint32_t partitionNum = config_.partitionNum_;
		partitionLockList_ = ALLOC_NEW(globalStackAlloc_) util::RWLock[partitionNum];
		partitions_ = ALLOC_NEW(globalStackAlloc_) Partition[partitionNum];
		currentPartitions_ = ALLOC_NEW(globalStackAlloc_) CurrentPartitions[partitionNum];

		NodeId maxNodeNum = config_.limitClusterNodeNum_;
		PartitionGroupConfig pgConfig(configTable);
		for (PartitionId pId = 0; pId < partitionNum; pId++) {
			partitions_[pId].init(globalStackAlloc_, pId,
				pgConfig.getPartitionGroupId(pId), maxNodeNum);
			currentPartitions_[pId].init();
		}
		nodes_ = ALLOC_NEW(globalStackAlloc_) NodeInfo[maxNodeNum];
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
		stableGoal_.init(this);

		PartitionRole initRole;
		newsqlPartitionList_.resize(getPartitionNum(), initRole);
		for (PartitionId pId = 0; pId < partitionNum; pId++) {
			newsqlPartitionList_[pId].init(pId);
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

PartitionTable::PartitionConfig::PartitionConfig(
	const ConfigTable& configTable, int32_t maxNodeNum) :
	partitionNum_(configTable.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM)),
	replicationNum_(configTable.get<int32_t>(CONFIG_TABLE_CS_REPLICATION_NUM) - 1),
	limitOwnerBackupLsnGap_(configTable.get<int32_t>(CONFIG_TABLE_CS_OWNER_BACKUP_LSN_GAP)),
	limitOwnerCatchupLsnGap_(configTable.get<int32_t>(CONFIG_TABLE_CS_OWNER_CATCHUP_LSN_GAP)),
	limitClusterNodeNum_(maxNodeNum),
	limitMaxLsnGap_(0),
	ownerLoadLimit_(PT_OWNER_LOAD_LIMIT),
	dataLoadLimit_(PT_DATA_LOAD_LIMIT),
	maxNodeNum_(maxNodeNum)
{
	limitOwnerBackupLsnDetectErrorGap_ = limitOwnerBackupLsnGap_ * PT_BACKUP_GAP_FACTOR;
}

PartitionTable::~PartitionTable() {

	for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
		if (partitionLockList_) {
			ALLOC_DELETE(globalStackAlloc_, &partitionLockList_[pId]);
		}
		if (partitions_) {
			ALLOC_DELETE(globalStackAlloc_, &partitions_[pId]);
		}
		if (currentPartitions_) {
			ALLOC_DELETE(globalStackAlloc_, &currentPartitions_[pId]);
		}
	}
	if (nodes_) {
		for (NodeId nodeId = 0; nodeId < config_.getLimitClusterNodeNum();nodeId++) {
			if (&nodes_[nodeId]) {
				ALLOC_DELETE(globalStackAlloc_, &nodes_[nodeId]);
			}
		}
	}
}

NodeId PartitionTable::getOwner(PartitionId pId) {

	if (pId >= config_.partitionNum_) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}
	util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
	return partitions_[pId].roles_[PT_CURRENT_OB].getOwner();
}

bool PartitionTable::isOwner(PartitionId pId, NodeId targetNodeId) {

	if (pId >= config_.partitionNum_) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}
	util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
	return (partitions_[pId].roles_[PT_CURRENT_OB].isOwner(targetNodeId));
}

bool PartitionTable::isOwnerOrBackup(PartitionId pId, NodeId targetNodeId) {

	if (pId >= config_.partitionNum_) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}
	util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
	return (partitions_[pId].roles_[PT_CURRENT_OB].isOwner(targetNodeId) ||
		partitions_[pId].roles_[PT_CURRENT_OB].isBackup(targetNodeId));
}

void PartitionTable::getBackup(PartitionId pId, util::XArray<NodeId>& backups) {

	if (pId >= config_.partitionNum_) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}
	util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
	partitions_[pId].roles_[PT_CURRENT_OB].getBackups(backups);
}

void PartitionTable::getCatchup(PartitionId pId, util::XArray<NodeId>& catchups) {

	if (pId >= config_.partitionNum_) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}
	util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
	partitions_[pId].roles_[PT_CURRENT_OB].getCatchups(catchups);
}

NodeId PartitionTable::getCatchup(PartitionId pId) {

	if (pId >= config_.partitionNum_) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}
	util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
	NodeIdList& catchups = partitions_[pId].roles_[PT_CURRENT_OB].getCatchups();
	if (catchups.size() == 1) {
		return catchups[0];
	}
	else {
		return UNDEF_NODEID;
	}
}

bool PartitionTable::isBackup(PartitionId pId, NodeId targetNodeId) {

	if (pId >= config_.partitionNum_) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}
	util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
	return (partitions_[pId].roles_[PT_CURRENT_OB].isBackup(targetNodeId));
}

bool PartitionTable::isCatchup(PartitionId pId, NodeId targetNodeId) {

	if (pId >= config_.partitionNum_) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}
	util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
	return (partitions_[pId].roles_[PT_CURRENT_OB].isCatchup(targetNodeId));
}

bool PartitionTable::setNodeInfo(NodeId nodeId, ServiceType type, NodeAddress& address) {

	if (nodeId >= config_.limitClusterNodeNum_) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_SET_INVALID_NODE_ID, "");
	}
	if (nodeId >= nodeNum_) {
		nodeNum_ = nodeId + 1;
		resizeCurrenPartition(0);
		if (type == CLUSTER_SERVICE) {
			util::LockGuard<util::Mutex> lock(nodeInfoLock_);
			nodeOrderingSet_.insert(std::make_pair(address.toString(), nodeId));
		}
	}
	return nodes_[nodeId].nodeBaseInfo_[type].set(address);
}

/*!
	@brief Checks cluster status and update load information
*/
PartitionTable::CheckedPartitionSummaryStatus PartitionTable::checkPartitionSummaryStatus(
	util::StackAllocator& alloc, bool isBalanceCheck, bool firstCheckSkip) {

	try {
		if (isSubMaster()) {
			return PT_INITIAL;
		}
		else if (isFollower()) {
			return PT_NORMAL;
		}
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
			util::XArray<NodeId> backups(alloc);
			getBackup(pId, backups);
			liveNum = 0;
			for (util::XArray<NodeId>::iterator it = backups.begin(); it != backups.end(); it++) {
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
			loadInfo_.isBalance(false, (backupNum + 1) * getPartitionNum())) {
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
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void PartitionTable::clearCurrentStatus() {

	util::LockGuard<util::WriteLock> lock(lock_);
	for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
		setHeartbeatTimeout(nodeId, UNDEF_TTL);
	}
}

void PartitionTable::clearRole(PartitionId pId) {

	if (pId >= config_.partitionNum_) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}
	util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
	partitions_[pId].clear();
	partitions_[pId].partitionInfo_.clear();
}

bool PartitionTable::checkClearRole(PartitionId pId) {

	if (pId >= config_.partitionNum_) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}
	if (isBackup(pId) && getPartitionStatus(pId) == PT_ON) {
		clear(pId);
		setPartitionRoleStatus(pId, PT_NONE);
		setPartitionStatus(pId, PT_OFF);
		return true;
	}
	else {
		return false;
	}
}

bool PartitionTable::checkConfigurationChange(util::StackAllocator& alloc,
	PartitionContext& context, bool checkFlag) {

	util::Set<NodeId> addNodeSet(alloc);
	util::Set<NodeId> downNodeSet(alloc);
	getAddNodes(addNodeSet);
	getDownNodes(downNodeSet);
	bool isAddOrDownNode = (addNodeSet.size() > 0 || downNodeSet.size() > 0 || checkFlag);
	if (isAddOrDownNode) {
		GS_TRACE_CLUSTER_INFO(
			"Detect cluster configuration change, create new goal partition, "
			"current_time=" << CommonUtility::getTimeStr(context.currentTime_)
			<< ", revision=" << getPartitionRevisionNo());
		context.setConfigurationChange();
		if (context.isAutoGoal_) {
			planGoal(context);
		}
	}
	checkRepairPartition(context);
	return isAddOrDownNode;
}

bool PartitionTable::check(PartitionContext& context) {

	try {
		util::StackAllocator& alloc = context.getAllocator();
		if (context.currentTime_ < nextApplyRuleLimitTime_) {
			if (getPartitionSummaryStatus() == PT_INITIAL) {
				checkPartitionSummaryStatus(alloc, false, true);
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
			if (checkTargetPartition(pId, context.backupNum_, catchupWait)) {
				isIrregular = true;
			}
		}
		if (catchupWait && !isIrregular) {
			return false;
		}
		if (isAllSame && !isIrregular) {
			isGoalPartition_ = true;
			loadInfo_.status_ = PT_NORMAL;

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
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void PartitionTable::updatePartitionRevision(PartitionRevisionNo partitionSequentialNumber) {

	util::LockGuard<util::Mutex> lock(revisionLock_);
	if (partitionSequentialNumber > revision_.getRevisionNo() &&
		partitionSequentialNumber != PartitionRevision::INITIAL_CLUSTER_REVISION) {
		revision_.updateRevision(partitionSequentialNumber);
	}
}

bool PartitionTable::checkPeriodicDrop(util::StackAllocator& alloc, DropPartitionNodeInfo& dropInfo) {

	util::XArray<NodeId> activeNodeList(alloc);
	getLiveNodeIdList(activeNodeList);
	int32_t liveNum = static_cast<int32_t>(activeNodeList.size());
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
		std::vector<NodeId>& backups = currentRole.getBackups();
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
				if (getPartitionRoleStatus(pId, nodeId) != PartitionTable::PT_NONE) {
					continue;
				}
				if (getPartitionStatus(pId, nodeId) == PT_ON
					|| getPartitionStatus(pId, nodeId) == PT_SYNC) {
					continue;
				}
				NodeAddress& address = getNodeAddress(nodeId);
				dropInfo.set(pId, address, getPartitionRevision(pId, nodeId), nodeId);
				enableDrop = true;
			}
		}
	}
	return enableDrop;
}


bool PartitionTable::PartitionRole::operator==(PartitionRole& role) const {

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

void PartitionTable::PartitionRole::clearAddressList(PartitionTable* pt, PartitionRole& role, bool isBackup) {

	NodeIdList& backupOrCatchups = (isBackup) ? role.getBackups() : role.getCatchups();
	std::vector<NodeAddress>& addressList = (isBackup) ? backupAddressList_ : catchupAddressList_;
	addressList.clear();
	size_t size = backupOrCatchups.size();
	addressList.reserve(size);
	for (size_t pos = 0; pos < size; pos++) {
		addressList.push_back(pt->getNodeInfo(backupOrCatchups[pos]).getNodeAddress());
	}
}

void PartitionTable::PartitionRole::encodeAddress(PartitionTable* pt, PartitionRole& role) {

	pId_ = role.getPartitionId();
	revision_ = role.getRevision();
	type_ = PT_CURRENT_OB;

	NodeId ownerNodeId = role.getOwner();
	if (ownerNodeId != UNDEF_NODEID) {
		ownerAddress_ = pt->getNodeInfo(ownerNodeId).getNodeAddress();
	}
	clearAddressList(pt, role, true);
	clearAddressList(pt, role, false);
}

void PartitionTable::BlockQueue::push(PartitionId pId, NodeId nodeId) {

	if (pId >= partitionNum_) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}
	if (static_cast<int32_t>(queueList_[pId].size()) > queueSize_) {
		queueList_[pId].clear();
	}
	queueList_[pId].insert(nodeId);
}

bool PartitionTable::BlockQueue::find(PartitionId pId, NodeId nodeId) {

	if (pId >= partitionNum_) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}
	std::set<NodeId>::iterator setItr = queueList_[pId].find(nodeId);
	return (setItr != queueList_[pId].end());
}

void PartitionTable::BlockQueue::clear(PartitionId pId) {

	if (pId >= partitionNum_) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}
	queueList_[pId].clear();
}

void PartitionTable::BlockQueue::erase(PartitionId pId, NodeId nodeId) {

	if (pId >= partitionNum_) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}
	std::set<NodeId>::iterator setItr = queueList_[pId].find(nodeId);
	if (setItr != queueList_[pId].end()) {
		queueList_[pId].erase(setItr);
	}
}

void PartitionTable::getServiceAddress(NodeId nodeId, picojson::value& result,
	ServiceType addressType, bool forSsl) {

	result = picojson::value(picojson::object());
	picojson::object& resultObj = result.get<picojson::object>();

	if (nodeId != UNDEF_NODEID) {
		NodeAddress address;
		if (hasPublicAddress_ &&
			(addressType == TRANSACTION_SERVICE || addressType == SQL_SERVICE)) {
			address = getNodeInfo(nodeId).publicAddressInfo_.getNodeAddress(addressType);
		}
		else {
			address = getNodeInfo(nodeId).getNodeAddress(addressType);
		}
		int32_t port;
		if (forSsl) {
			assert(addressType == SYSTEM_SERVICE);
			port = getNodeInfo(nodeId).sslPort_;
		}
		else {
			port = static_cast<int32_t>(address.port_);
		}

		resultObj["address"] = picojson::value(address.toString(false));
		resultObj["port"] = picojson::value(static_cast<double>(port));
	}
	else {
		resultObj["address"] = picojson::value("undef");
		resultObj["port"] = picojson::value("undef");
	}
}

void PartitionTable::getServiceAddress(NodeId nodeId, picojson::object& result,
	const ServiceTypeInfo& addressType) {
	try {
		picojson::value resultValue;
		getServiceAddress(
			nodeId, resultValue, static_cast<ServiceType>(addressType.first),
			addressType.second);
		const picojson::object& resultObj = resultValue.get<picojson::object>();
		result.insert(resultObj.begin(), resultObj.end());
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}


void PartitionTable::updateNewSQLPartition(SubPartitionTable& newsqlPartition, bool force) {

	util::LockGuard<util::Mutex> lock(newsqlLock_);
	for (int32_t pos = 0; pos < newsqlPartition.size(); pos++) {
		SubPartition sub = newsqlPartition.getSubPartition(pos);
		if (sub.pId_ < getPartitionNum()) {
			if ((sub.role_.getRevisionNo() == PartitionRevision::INITIAL_CLUSTER_REVISION
				|| sub.role_.getRevisionNo() > newsqlPartitionList_[sub.pId_].getRevisionNo()) || force) {
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

PartitionRevisionNo PartitionTable::getNewSQLPartitionRevision(PartitionId pId) {

	util::LockGuard<util::Mutex> lock(newsqlLock_);
	if (pId >= getPartitionNum()) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}
	return newsqlPartitionList_[pId].getRevisionNo();
}

template <typename T1, typename T2>
void PartitionTable::getActiveNodeList(T1& activeNodeList, T2& downNodeSet, int64_t currentTime) {

	if (!isFollower()) {
		int32_t nodeNum = getNodeNum();
		activeNodeList.reserve(nodeNum);
		if (getLiveNodeIdList(
			activeNodeList, currentTime, true) && isMaster()) {
			getDownNodes(downNodeSet, false);
		}
	}
}

void PartitionTable::changeClusterStatus(
		ClusterStatus status, NodeId masterNodeId,
		int64_t baseTime, int64_t nextHeartbeatTimeout) {
	UNUSED_VARIABLE(baseTime);

	switch (status) {
	case SUB_MASTER:
		clearCurrentStatus();
		setMaster(UNDEF_NODEID);
		resetBlockQueue();
		resizeCurrenPartition(0);
		nextApplyRuleLimitTime_ = UNDEF_TTL;
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

void PartitionTable::resetDownNode(NodeId targetNodeId) {

	for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
		PartitionRole currentRole;
		getPartitionRole(pId, currentRole);
		NodeId currentOwner = currentRole.getOwner();
		bool isDown = false;
		if (currentOwner != UNDEF_NODEID &&
			nodes_[currentOwner].heartbeatTimeout_ == UNDEF_TTL) {
			currentOwner = UNDEF_NODEID;
			isDown = true;
		}
		NodeIdList& currentBackups = currentRole.getBackups();
		NodeIdList& currentCatchups = currentRole.getCatchups();
		NodeIdList tmpBackups, tmpCatchups;
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
		setPartitionRoleStatus(pId, PT_NONE, targetNodeId);
		setPartitionStatus(pId, PT_OFF, targetNodeId);
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
	UNUSED_VARIABLE(pId);
	UNUSED_VARIABLE(checkedNode);

	bool backupCond1 = !checkLimitOwnerBackupLsnGap(currentOwnerLsn, checkedLsn);
	bool backupCond2 = (checkedLsn >= currentOwnerStartLsn);

	return (backupCond1 && backupCond2);
}

void PartitionTable::SubPartitionTable::set(PartitionTable* pt, PartitionId pId,
	PartitionRole& role, NodeId ownerNodeId) {

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
	PartitionRole& tmpRole = subPartition.role_;
	tmpRole.encodeAddress(pt, role);
	subPartitionList_.push_back(subPartition);
}

/** **
	@brief ラックゾーン情報取得
	@note ラックゾーン情報である、アドレスとラックゾーン名のペア情報を取得する
	@param [out] rackZoneInfoList ラックゾーン情報リスト
** **/
void PartitionTable::getRackZoneInfoList(RackZoneInfoList& rackZoneInfoList) {
	util::LockGuard<util::Mutex> lock(nodeInfoLock_);
	for (int32_t nodeId = 0; nodeId < nodeNum_; nodeId++) {
		if (!nodes_[nodeId].rackZoneIdName_.empty()) {
			rackZoneInfoList.add(nodes_[nodeId].getNodeAddress(), nodes_[nodeId].rackZoneIdName_);
		}
	}
}

/** **
	@brief ラックゾーン情報セット
	@note ラックゾーン情報である、アドレスとラックゾーン名のペア情報をセットする
	@param [in] alloc アロケータ
	@param [in] rackZoneInfoList ラックゾーン情報リスト
** **/
void PartitionTable::setRackZoneInfoList(util::StackAllocator& alloc, RackZoneInfoList& rackZoneInfoList) {
	util::Map<NodeAddress, NodeId> nodeIdMap(alloc);
	for (NodeId nodeId = 0; nodeId < getNodeNum(); nodeId++) {
		nodeIdMap.emplace(getNodeAddress(nodeId), nodeId);
	}
	for (const auto& elem : rackZoneInfoList.get()) {
		decltype(nodeIdMap)::iterator it = nodeIdMap.find(elem.address_);
		if (it != nodeIdMap.end()) {
			setRackZoneId(elem.rackZoneId_.c_str(), (*it).second);
		}
	}
}

/** **
	@brief ラックゾーンIDセット
	@note 指定ノードのラックゾーンIDをセットする
	@param [in] rackZoneIdName ラックゾーンID名
	@param [in] nodeId ノードID
** **/
void PartitionTable::setRackZoneId(const char* rackZoneIdName, NodeId nodeId) {
	if (rackZoneIdName == NULL || (rackZoneIdName != NULL && strlen(rackZoneIdName) == 0)) {
		enableRackZoneAwareness_ = false;
		return;
	}
	if (nodeId >= nodeNum_) {
		return;
	}
	util::LockGuard<util::Mutex> lock(nodeInfoLock_);
	NodeInfo& nodeInfo = nodes_[nodeId];
	nodeInfo.rackZoneIdName_ = rackZoneIdName;
	decltype(rackZoneMap_)::iterator it = rackZoneMap_.find(nodeInfo.rackZoneIdName_);
	if (it != rackZoneMap_.end()) {
		nodeInfo.rackZoneId_ = (*it).second;
	}
	else {
		nodeInfo.rackZoneId_ = static_cast<int32_t>(rackZoneMap_.size());
		rackZoneMap_.emplace(nodeInfo.rackZoneIdName_, nodeInfo.rackZoneId_);
	}
}

/** **
	@brief ラックゾーンIDリスト取得
	@note ラックゾーンIDリストを取得する
	@param [out] rackZoneIdList ラックゾーンIDリスト
** **/
void PartitionTable::getRackZoneIdList(util::Vector<RackZoneId>& rackZoneIdList) {
	util::LockGuard<util::Mutex> lock(nodeInfoLock_);
	for (int32_t nodeId = 0; nodeId < nodeNum_; nodeId++) {
		rackZoneIdList.push_back(nodes_[nodeId].rackZoneId_);
	}
}

/** **
	@brief ラックゾーンIDリスト取得
	@note ラックゾーンIDリストを取得する
	@param [out] rackZoneIdList ラックゾーンID
	@param [out] nodeId ノードID
** **/
template<typename T>
void PartitionTable::getRackZoneIdName(T& rackZoneIdName, NodeId nodeId) {
	if (nodeId >= nodeNum_) {
		return;
	}
	util::LockGuard<util::Mutex> lock(nodeInfoLock_);
	rackZoneIdName = nodes_[nodeId].rackZoneIdName_.c_str();
}
template void PartitionTable::getRackZoneIdName(std::string& rackZoneIdName, NodeId nodeId);
template void PartitionTable::getRackZoneIdName(util::String& rackZoneIdName, NodeId nodeId);

	/** **
	@brief ラックゾーンID（数値化済み）取得
	@note 指定ノードのラックゾーンID（数値）取得
	@param [in] nodeId ノードID
	@return int32_t ラックゾーンID
** **/
int32_t PartitionTable::getRackZoneId(NodeId nodeId) {
	if (nodeId >= nodeNum_) {
		return UNDEF_RACKZONEID;
	}
	return nodes_[nodeId].rackZoneId_;
}

/** **
	@brief ラックゾーンID取得
	@note 指定ノードIDのラックゾーンID取得
	@param [in] alloc アロケータ
	@return bool ラックゾーンアウェアネス有効無効 true/false
** **/
bool PartitionTable::isUseRackZoneAwareness(util::StackAllocator& alloc) {
	if (!enableRackZoneAwareness_) return false;
	util::LockGuard<util::Mutex> lock(nodeInfoLock_);
	int32_t prevRackZoneId = UNDEF_RACKZONEID;
	bool allSame = true;
	util::StackAllocator::Scope scope(alloc);
	util::Vector<RackZoneId> emptyRackZoneIdList(alloc);
	for (int32_t nodeId = 0; nodeId < nodeNum_; nodeId++) {
		int32_t currentRackZoneId = nodes_[nodeId].rackZoneId_;
		if (nodes_[nodeId].heartbeatTimeout_ != UNDEF_TTL) {
			if (currentRackZoneId == UNDEF_RACKZONEID) {
				emptyRackZoneIdList.push_back(nodeId);
			}
			if (prevRackZoneId != UNDEF_RACKZONEID && prevRackZoneId != currentRackZoneId) {
				allSame = false;
			}
			prevRackZoneId = currentRackZoneId;
		}
	}
	bool retFlag = ((emptyRackZoneIdList.empty() && !allSame) ? true : false);
	if (!retFlag) {
		GS_TRACE_WARNING(CLUSTER_OPERATION, GS_TRACE_CS_NOT_APPLICABLE_RACKZONE_AWARENESS,
			"RackZone awareness is not applicable (revision=" << revision_
			<< ", nodes=" << dumpNodeAddressList(emptyRackZoneIdList) << ")");
	}
	return retFlag;
}

/** **
	@brief ゴール生成
	@note パーティション割り当て＋オーナ修正フェーズから構成
	@param [in] context コンテキスト
** **/
void PartitionTable::planGoal(PartitionContext& context) {
	util::StackAllocator& alloc = context.getAllocator();
	nextApplyRuleLimitTime_ = UNDEF_TTL;
	currentRuleNo_ = PARTITION_RULE_UNDEF;
	isGoalPartition_ = false;
	util::Stopwatch  watch;
	watch.reset();
	watch.start();
	int32_t traceLimitTime = 5 * 1000;
	try {
		preparePlanGoal();
		util::Set<NodeAddress> liveNodeAddressSet(alloc);
		getLiveNodeAddressSet(liveNodeAddressSet);
		bool stableCluster =
				(reserveNum_ == static_cast<ptrdiff_t>(liveNodeAddressSet.size()));
		bool assignedGoal = false;

		picojson::value value;
		if (stableGoal_.isEnable()) {
			if (stableGoal_.load() && stableGoal_.validateFormat(value, true)) {
				if (stableCluster) {
					assignedGoal = setGoalPartitions(alloc, &value, PT_STABLE_GOAL, true);
					if (assignedGoal) {
						currentGoalAssignmentRule_ = PARTITION_ASSIGNMENT_RULE_STABLE_GOAL;
						GS_TRACE_CLUSTER_INFO("Plan goal by stable goal (stable cluster)");
					}
				}
				else if (stableGoal_.isSucceedOnError()) {
					if (setGoalPartitions(alloc, &value, PT_STABLE_GOAL, false)) {
						assignedGoal = assignSucceedStableGoal(alloc);
						if (assignedGoal) {
							currentGoalAssignmentRule_ = PARTITION_ASSIGNMENT_RULE_PARTIAL_STABLE_GOAL;
							GS_TRACE_CLUSTER_INFO("Plan goal by partial stable goal  (not stable cluster)");
						}
					}
				}
			}
		}
		if (!assignedGoal) {
			if (stableGoal_.isEnable()) {
				GS_TRACE_CLUSTER_INFO("Not applicable stable goal");
			}
			util::Vector<PartitionRole> roleList(alloc);
			switch (goalAssignmentRule_) {
			case PARTITION_ASSIGNMENT_RULE_DEFAULT: {
				assignDefaultGoal(alloc);
				currentGoalAssignmentRule_ = PARTITION_ASSIGNMENT_RULE_DEFAULT;
				break;
			}
			case PARTITION_ASSIGNMENT_RULE_ROUNDROBIN: {
				assignRoundRobinGoal(alloc);
				currentGoalAssignmentRule_ = PARTITION_ASSIGNMENT_RULE_ROUNDROBIN;
				break;
			}
			default:
				GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "Specified partition assignment rule is not supported");
			}
			GS_TRACE_CLUSTER_INFO("Plan goal (rule=" << dumpGoalAssignmentRule(currentGoalAssignmentRule_) << ")");
			if (stableGoal_.isEnableInitialGenerate() && stableCluster) {
				updateStableGoal(alloc);
				writeStableGoal(alloc);
			}
		}
		GS_TRACE_CLUSTER_INFO(dumpPartitionsNew(alloc, PT_CURRENT_GOAL));
		int32_t lap = watch.elapsedMillis();
		if (lap > traceLimitTime) {
			GS_TRACE_INFO(CLUSTER_OPERATION, GS_TRACE_CS_CLUSTER_STATUS, "Plan goal time=" << lap);
		}
	}
	catch (std::exception& e) {
		int32_t lap = watch.elapsedMillis();
		if (lap > traceLimitTime) {
			GS_TRACE_INFO(CLUSTER_OPERATION, GS_TRACE_CS_CLUSTER_STATUS, "Plan goal time=" << lap);
		}
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/** **
	@brief ゴールプランナーコンストラクタ
	@note パーティションテーブルからゴール計算に必要なクラスタおよびパーティション情報をセット
	@param [in] pt パーティションテーブル
	@param [in] alloc アロケータ
	@param [in] nodeIdList ノードIDリスト
	@param [in] roleList ゴールとなるロールリスト
** **/
PartitionTable::GoalPlanner::GoalPlanner(
		PartitionTable* pt, util::StackAllocator& alloc,
		util::XArray<NodeId>& nodeIdList, util::Vector<PartitionRole>& roleList) :
		alloc_(alloc), pt_(pt), nodeIdList_(nodeIdList), roleList_(roleList),
		partitionNum_(pt->getPartitionNum()),
		nodeNum_(static_cast<int32_t>(nodeIdList.size())),
		backupNum_(pt->getReplicationNum()),
		priorityPartitionLists_(nodeNum_, util::Vector<PartitionId>(alloc), alloc),
		priorityPartitionBackupLists_(nodeNum_, util::Vector<PartitionId>(alloc), alloc),
		priorityCountList_(partitionNum_, 0, alloc),
		maxLsnList_(partitionNum_, 0, alloc), rackZoneIdList_(alloc),
		currentRackZoneAwareness_(false), orgRackZoneAwareness_(true) {

	if (backupNum_ > nodeNum_ - 1) {
		backupNum_ = nodeNum_ - 1;
	}
	pt->getRackZoneIdList(rackZoneIdList_);
	setupMaxLsn();
	setupPriorityPartition();
}

/** **
	@brief 最大LSN情報セット
	@note 各パーティションの最大LSNと、ゴールロールの初期化
	@param [in] なし
** **/
void PartitionTable::GoalPlanner::setupMaxLsn() {
	for (PartitionId pId = 0; pId < partitionNum_; pId++) {
		roleList_.emplace_back(PartitionRole(pId, pt_->revision_, PT_CURRENT_OB));
		LogSequentialNumber maxLsn = 0;
		for (size_t pos = 0; pos < nodeIdList_.size(); pos++) {
			LogSequentialNumber currentLsn = pt_->getLSN(pId, nodeIdList_[pos]);
			if (currentLsn > maxLsn) {
				maxLsn = currentLsn;
			}
		}
		maxLsnList_[pId] = maxLsn;
	}
}

/** **
	@brief パーティションプライオリティ情報のセット
	@note 各パーティションのLSNに従って、第一、第二優先度リストを生成する
	@param [in] なし
** **/
void PartitionTable::GoalPlanner::setupPriorityPartition() {
	for (PartitionId pId = 0; pId < partitionNum_; pId++) {
		for (size_t pos = 0; pos < nodeIdList_.size(); pos++) {
			LogSequentialNumber lsn = pt_->getLSN(pId, nodeIdList_[pos]);
			if (maxLsnList_[pId] != 0) {
				if (lsn == maxLsnList_[pId]) {
					priorityPartitionLists_[pos].push_back(pId);
					priorityCountList_[pId]++;
				}
				else if (lsn != 0 && !pt_->checkLimitOwnerBackupLsnGap(maxLsnList_[pId], lsn)) {
					priorityPartitionBackupLists_[pos].push_back(pId);
					priorityCountList_[pId]++;
				}
			}
		}
	}
}

/** **
	@brief 指定パーティションの割付
	@note 現ゴールロールの状態に従ってオーナ、バックアップの割付判定を行い、ロールにセットする
	@param [in] pId パーティションID
	@param [in] nodePos ノードポジション
	@param [in] backupOnly バックアップ候補のみ。第二優先度の場合はこのフラグがtrue
	@return bool ロールセットしたかどうか true/false
** **/
bool PartitionTable::GoalPlanner::assignPartition(
		PartitionId pId, int32_t nodePos, bool backupOnly, int32_t priorityNo) {
	UNUSED_VARIABLE(priorityNo);

	bool assigned = true;
	NodeId nodeId = nodeIdList_[nodePos];
	PartitionRole& role = roleList_[pId];
	if (role.getOwner() == UNDEF_NODEID) {
		if (backupOnly) {
			return false;
		}
		role.setOwner(nodeId);
	}
	else if (role.getBackupSize() < backupNum_ && !role.isBackup(nodeId) && role.getOwner() != nodeId) {
		if (currentRackZoneAwareness_) {
			if (rackZoneIdList_[role.getOwner()] == rackZoneIdList_[nodeId]) {
				return false;
			}
		}
		role.getBackups().push_back(nodeId);
		role.checkBackup();
	}
	else {
		assigned = false;
	}
	return assigned;
}

/** **
	@brief パーティションの割付
	@note 優先パーティションリストの順に、割付可能なパーティションの割付判定を行う
	@param [in] targetNodePos ノードポジション
	@param [in] priorityNo 優先パーティションリスト番号
	@param [in] priority 割付判定終了時のの該当ノードのプライオリティ
	@return bool ロールセットしたかどうか true/false
** **/
bool PartitionTable::GoalPlanner::assginePriorityPartition(
	NodeId targetNodePos, int32_t priorityNo, int32_t& priority) {

	util::StackAllocator::Scope scope(alloc_);
	util::Vector<PartitionId> adjustPartitionList(alloc_);

	util::Vector<PartitionId>* targetPartitionList = NULL;
	bool backupOnly = false;
	bool erasable = true;
	switch (priorityNo) {
	case PRIORIY_1ST:
		targetPartitionList = &priorityPartitionLists_[targetNodePos];
		break;
	case PRIORIY_2ND:
		targetPartitionList = &priorityPartitionBackupLists_[targetNodePos];
		backupOnly = true;
		break;
	case PRIORIY_NONE:
		for (PartitionId pId = 0; pId < static_cast<uint32_t>(partitionNum_); pId++) {
			if (roleList_[pId].getRoledSize() < getReplicaNum()) {
				adjustPartitionList.push_back(pId);
			}
		}
		targetPartitionList = &adjustPartitionList;
		erasable = false;
		break;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}
	util::Vector<PartitionId>& priorityPartitionList = *targetPartitionList;
	bool assigned = false;
	for (util::Vector<PartitionId>::iterator it = priorityPartitionList.begin();
		it != priorityPartitionList.end(); it++) {
		PartitionId pId = (*it);
		if (assignPartition(pId, targetNodePos, backupOnly, priorityNo)) {
			if (erasable) {
				priorityPartitionList.erase(it);
				priorityCountList_[pId]--;
			}
			assigned = true;
			break;
		}
	}
	priority = calcPriority(targetNodePos);
	return assigned;
}

/** **
	@brief ラックゾーンモードの設定
	@note ラックゾーンモードを設定して、探索上限回数を返す
	@param [in] mode ラックゾーンモード指定
	@param [in] priorityNo 優先パーティションリスト番号
	@param [in] priority 割付判定終了時のの該当ノードのプライオリティ
	@return int32_t 探索上限回数
** **/
int32_t PartitionTable::GoalPlanner::setRackZoneMode(int32_t mode) {
	if (mode == GoalPlanner::MODE_RACKZONE) {
		currentRackZoneAwareness_ = true;
		return (nodeNum_ * partitionNum_);
	}
	else {
		currentRackZoneAwareness_ = false;
		return ((getReplicaNum() + 1) * partitionNum_);
	}
}

/** **
	@brief ゴールのバリデーションチェック
	@note ゴール割付終了後に、レプリカロスとなるロールが存在しないかチェック
	@param [in] alloc アロケータ
** **/
void PartitionTable::GoalPlanner::validate(util::StackAllocator& alloc) {

	util::NormalOStringStream oss;
	util::StackAllocator::Scope scope(alloc_);
	util::Vector<PartitionId> lossPartitionList(alloc), irregularPartitionList(alloc);
	bool irregular = false;

	for (PartitionId pId = 0; pId < static_cast<uint32_t>(partitionNum_); pId++) {
		PartitionRole &role = roleList_[pId];
		if (role.getRoledSize() < getReplicaNum()) {
			lossPartitionList.push_back(pId);
			irregular = true;
		}
		if (orgRackZoneAwareness_ && backupNum_ > 0) {
			int32_t ownerRackZoneId = rackZoneIdList_[role.getOwner()];
			NodeIdList& backups = role.getBackups();
			bool correct = false;
			for (size_t pos = 0; pos < backups.size(); pos++) {
				if (rackZoneIdList_[backups[pos]] != ownerRackZoneId) {
					correct = true;
					break;
				}
			}
			if (!correct) {
				irregularPartitionList.push_back(pId);
				irregular = true;
			}
		}
	}
	if (irregular) {
		oss << "Invalid goal partition";
		if (!lossPartitionList.empty()) {
			oss << " replicaLoss : ";
			CommonUtility::dumpValueList(oss, lossPartitionList);
		}
		if (!irregularPartitionList.empty()) {
			oss << " rackZone constraint : ";
			CommonUtility::dumpValueList(oss, irregularPartitionList);
		}
		GS_THROW_USER_ERROR(GS_ERROR_PT_INVALID_GOAL_TABLE, oss.str().c_str());
	}
}

/** **
	@brief ゴールテーブルセット
	@note ゴール割り当てアルゴリズムに従ってロールをセット。オーナ偏り次フェーズで修正
	@param [in] alloc アロケータ
	@param [in] alloc ノードIDリスト
	@param [in] alloc ゴールリスト
** **/
void PartitionTable::assignGoalTable(util::StackAllocator& alloc,
	util::XArray<NodeId>& nodeIdList, util::Vector<PartitionRole>& roleList) {

	GoalPlanner planner(this, alloc, nodeIdList, roleList);
	util::Deque<GoalNodeInfo> nodeQueue(alloc), nextNodeQueue(alloc);
	int32_t nodeNum = static_cast<int32_t>(nodeIdList.size());
	int32_t totalAssignCount = 0;
	int32_t dataTotalLimit = planner.getReplicaNum() * getPartitionNum();
	int32_t mode = GoalPlanner::MODE_RACKZONE;
	if (!isUseRackZoneAwareness(alloc)) {
		mode = GoalPlanner::MODE_NORMAL;
		planner.setOrigianlRackZoneAwareness(false);
	}
	int32_t searchCounter = 0, searchLimit = 0, priority = 0;
	int32_t resolver[] = { GoalPlanner::PRIORIY_1ST, GoalPlanner::PRIORIY_2ND, GoalPlanner::PRIORIY_NONE };
	int32_t resolverSize = sizeof(resolver) / sizeof(int32_t);

	for (; mode < GoalPlanner::MODE_MAX; mode++) {
		nodeQueue.clear();
		for (int32_t nodePos = 0; nodePos < nodeNum; nodePos++) {
			nodeQueue.emplace_back(GoalNodeInfo(nodePos, planner.calcPriority(nodePos)));
		}
		searchCounter = 0;
		searchLimit = planner.setRackZoneMode(mode);
		while (!nodeQueue.empty() && searchCounter <= searchLimit && totalAssignCount < dataTotalLimit) {
			std::sort(nodeQueue.begin(), nodeQueue.end());
			while (!nodeQueue.empty()) {
				searchCounter++;
				NodeId targetNodePos = nodeQueue.front().nodePos_;
				nodeQueue.pop_front();
				for (int32_t pos = 0; pos < resolverSize; pos++) {
					if (planner.assginePriorityPartition(targetNodePos, resolver[pos], priority)) {
						totalAssignCount++;
						if (totalAssignCount == dataTotalLimit) {
							nodeQueue.clear();
						}
						else {
							nextNodeQueue.emplace_back(GoalNodeInfo(targetNodePos, priority));
						}
						break;
					}
				}
			}
			nodeQueue.swap(nextNodeQueue);
		}
	}
	try {
		planner.validate(alloc);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void PartitionTable::assignDefaultGoal(util::StackAllocator& alloc) {
	util::Vector<PartitionRole> roleList(alloc);
	util::XArray<NodeId> nodeIdList(alloc);
	getLiveNodeIdList(nodeIdList);

	assignGoalTable(alloc, nodeIdList, roleList);
	modifyGoalTable(alloc, nodeIdList, roleList);
	setGoal(this, roleList);
}

void PartitionTable::assignRoundRobinGoal(util::StackAllocator& alloc) {
	util::Vector<PartitionRole> roleList(alloc);
	util::XArray<NodeId> nodeIdList(alloc);
	getOrderingLiveNodeIdList(nodeIdList);

	uint32_t nodeNum = static_cast<uint32_t>(nodeIdList.size());
	uint32_t backupNum = getReplicationNum();
	if (backupNum > nodeNum - 1) {
		backupNum = nodeNum - 1;
	}
	uint32_t ownerPos = 0;
	for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
		roleList.emplace_back(PartitionRole(pId, revision_, PT_CURRENT_OB));
		if (nodeNum == 0) {
			continue;
		}
		PartitionRole& role = roleList[pId];
		role.setOwner(nodeIdList[ownerPos % nodeNum]);
		ownerPos++;
		for (uint32_t pos = 0; pos < backupNum; pos++) {
			role.getBackups().push_back(nodeIdList[(ownerPos + pos) % nodeNum]);
		}
		role.checkBackup();
		role.encodeAddress(this, role);
	}
	setGoal(this, roleList);
}

bool PartitionTable::assignSucceedStableGoal(util::StackAllocator& alloc) {
	util::Vector<PartitionRole> roleList(alloc);
	util::XArray<NodeId> nodeIdList(alloc);
	uint32_t nodeNum = static_cast<uint32_t>(nodeIdList.size());
	uint32_t backupNum = getReplicationNum();
	if (backupNum > nodeNum - 1) {
		backupNum = nodeNum - 1;
	}
	stableGoal_.copy(roleList);
	for (size_t pos1 = 0; pos1 < roleList.size(); pos1++) {
		PartitionRole& role = roleList[pos1];
		NodeId owner = role.getOwner();
		if (owner == UNDEF_NODEID || (owner != UNDEF_NODEID &&  getHeartbeatTimeout(owner) == UNDEF_TTL)) {
			NodeId newOwner = UNDEF_NODEID;
			NodeIdList& backups = role.getBackups();
			for (NodeIdList::iterator it = backups.begin(); it != backups.end(); it++) {
				if (*it != UNDEF_NODEID && getHeartbeatTimeout(*it) != UNDEF_TTL) {
					newOwner = *it;
					backups.erase(it);
					break;
				}
			}
			role.setOwner(newOwner);
		}
		if (role.getOwner() == UNDEF_NODEID) {
			return false;
		}
		NodeIdList& backups = role.getBackups();
		NodeIdList::iterator it = backups.begin();
		while (it != backups.end()) {
			if (*it  == UNDEF_NODEID || (*it != UNDEF_NODEID && getHeartbeatTimeout(*it) == UNDEF_TTL)) {
				it = backups.erase(it);
			}
			else {
				++it;
			}
		}
	}
	setGoal(this, roleList);
	GS_TRACE_CLUSTER_INFO(dumpPartitionsNew(alloc, PT_CURRENT_GOAL));
	return true;
}

/** **
	@brief ゴール修正プランナーコンストラクタ
	@note 各ノードのオーナ保持個数の管理と、偏り解決のためのヒューリスティック実行
	@param [in] alloc アロケータ
	@param [in] nodeNum ノード数
	@param [in] partitionNum パーティション数
	@param [in] nodeIdList ノードIDリスト
	@param [in] roleList ロールリスト（一次割り当て済み）
** **/
PartitionTable::ModifyPlanner::ModifyPlanner(util::StackAllocator& alloc, uint32_t nodeNum,
	uint32_t partitionNum, util::XArray<NodeId>& nodeIdList, util::Vector<PartitionRole>& roleList) :
	ownerCountList_(nodeNum, 0, alloc), pIdList_(partitionNum, alloc), nodeIdList_(nodeIdList),
	random_(static_cast<int64_t>(util::DateTime::now(false).getUnixTime())) {
	init();
	std::iota(pIdList_.begin(), pIdList_.end(), 0);
	arrayShuffle<util::Vector<PartitionId>, PartitionId>(pIdList_, random_);
	for (PartitionId pId = 0; pId < partitionNum; pId++) {
		NodeId owner = roleList[pId].getOwner();
		if (owner != UNDEF_NODEID) {
			ownerCountList_[owner]++;
		}
	}
}

/** **
	@brief オーナ保持個数の更新
	@note スワップ実行の度に更新
	@param [in] なし
** **/
void PartitionTable::ModifyPlanner::update() {
	init();
	for (size_t pos = 0; pos < nodeIdList_.size(); pos++) {
		NodeId target = nodeIdList_[pos];
		if (ownerCountList_[target] > maxOwnerCount_) {
			maxOwnerCount_ = ownerCountList_[target];
			maxOwnerNodeId_ = target;
		}
		if (ownerCountList_[target] < minOwnerCount_) {
			minOwnerCount_ = ownerCountList_[target];
			minOwnerNodeId_ = target;
		}
	}
}

/** **
	@brief スワップ候補のノードIDの取得
	@note 最大個数を持つオーナと、最小個数を持つバックアップのロールスワップ
	@param [in] alloc アロケータ
	@param [in] role ロール
	@return NodeId スワップ対象のノードID
** **/
NodeId PartitionTable::ModifyPlanner::getSwappableNodeId(util::StackAllocator& alloc, PartitionRole& role) {

	NodeId owner = role.getOwner();
	if (owner == UNDEF_NODEID) return UNDEF_NODEID;
	if (ownerCountList_[owner] != maxOwnerCount_) return UNDEF_NODEID;
	NodeIdList& backups = role.getBackups();
	if (backups.size() == 0) {
		return UNDEF_NODEID;
	}
	int32_t minCount = INT32_MAX;
	NodeId swap = UNDEF_NODEID;

	for (size_t pos = 0; pos < backups.size(); pos++) {
		NodeId backup = backups[pos];
		if (ownerCountList_[backup] < minCount) {
			minCount = ownerCountList_[backup];
		}
	}

	util::StackAllocator::Scope scope(alloc);
	util::Vector<NodeId> candList(alloc);
	for (size_t pos = 0; pos < backups.size(); pos++) {
		NodeId backup = backups[pos];
		if (ownerCountList_[backup] == minCount) {
			candList.push_back(backup);
		}
	}
	if (candList.size() == 0) return UNDEF_NODEID;

	if (maxOwnerCount_ == minCount) {
		return UNDEF_NODEID;
	}
	swap = candList[random_.nextInt32(static_cast<int32_t>(candList.size()))];
	ownerCountList_[swap]++;
	ownerCountList_[owner]--;

	return swap;
}

/** **
	@brief ゴール修正
	@note 一次割付ゴールに対してオーナ偏りを修正
	@param [in] alloc アロケータ
	@param [in] nodeIdList ノードIDリスト
	@param [in] roleList ロールリスト
** **/
void PartitionTable::modifyGoalTable(util::StackAllocator& alloc,
	util::XArray<NodeId>& nodeIdList, util::Vector<PartitionRole>& roleList) {

	int32_t replicationNum = getReplicationNum() + 1;
	ModifyPlanner planner(alloc, getNodeNum(), getPartitionNum(), nodeIdList, roleList);
	bool isUpdate = true;
	util::Vector<PartitionId> &pIdList  = planner.getResolvePartitionIdList();
	for (int32_t k = 0; k < replicationNum; k++) {
		bool nextCandidate = false;
		for (size_t pos = 0; pos < pIdList.size(); pos++) {
			if (isUpdate) {
				planner.update();
			}
			if (planner.check(1)) {
				return;
			}
			PartitionRole& role = roleList[pIdList[pos]];
			NodeId swap = planner.getSwappableNodeId(alloc, role);
			if (swap != UNDEF_NODEID) {
				role.swapRole(swap);
				nextCandidate = true;
				isUpdate = true;
			}
			else {
				isUpdate = false;
			}
		}
		if (!nextCandidate) {
			break;
		}
	}
}

bool PartitionTable::applyInitialRule(
	PartitionContext& context) {

	SubPartitionTable& subPartitionTable
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
				NodeIdList& nextBackups = nextRole.getBackups();
				NodeIdList& goalBackups = goalRole.getBackups();
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
					if (checkNextBackup(pId, candBackup,
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
				arrayShuffle<util::XArray<NodeId>, NodeId>(activeNodeList, random);

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
							strstrm << "Target partition owner is not assigned, pId="
								<< pId << ", expected maxLSN=" << maxLsn
								<< ", actual maxLSN=" << tmpMaxLSN;
							GS_THROW_USER_ERROR(
								GS_ERROR_CS_PARTITION_OWNER_NOT_ASSIGN,
								strstrm.str().c_str());
						}
						catch (std::exception& e) {
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
				NodeIdList& nextBackups = nextRole.getBackups();
				NodeIdList& goalBackups = goalRole.getBackups();
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
						if (checkNextBackup(pId, candBackup, backupLSN,
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
			NodeIdList& goalBackups = goalRole.getBackups();
			NodeIdList& nextBackups = currentRole.getBackups();
			currentRole.setRevision(revision_);
			currentRole.clearCatchup();

			int32_t backupNum = currentRole.getBackupSize();
			if (backupNum < context.backupNum_
				&& context.isLoadBalance_) {


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
							if (checkNextBackup(pId, candBackup, backupLSN,
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

void PartitionTable::getReplicaLossPIdList(
	util::StackAllocator& alloc, util::Vector<ReplicaInfo>& resolvePartitionIdList) {

	util::Vector<PartitionId> pIdList(getPartitionNum(), alloc);
	std::iota(pIdList.begin(), pIdList.end(), 0);
	util::Random random(static_cast<int64_t>(util::DateTime::now(false).getUnixTime()));
	arrayShuffle<util::Vector<PartitionId>, PartitionId>(pIdList, random);
	for (size_t pos = 0; pos < pIdList.size(); pos++) {
		resolvePartitionIdList.emplace_back(ReplicaInfo(
				pIdList[pos],
				static_cast<uint32_t>(getBackupSize(alloc, pIdList[pos]))));
	}
	std::sort(resolvePartitionIdList.begin(), resolvePartitionIdList.end(), cmpReplica);
}

void PartitionTable::getCurrentCatchupPIdList(util::Vector<PartitionId>& catchupPIdList) {
	for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
		NodeId currentOwner = getOwner(pId);
		if (currentOwner == UNDEF_NODEID) {
			continue;
		}
		PartitionRole currentRole;
		getPartitionRole(pId, currentRole, PT_CURRENT_OB);
		if (currentRole.hasCatchupRole()) {
			catchupPIdList.push_back(pId);
		}
	}
}

bool PartitionTable::applyAddRule(PartitionContext& context) {

	util::StackAllocator& alloc = context.getAllocator();
	util::StackAllocator::Scope scope(alloc);

	SubPartitionTable& subPartitionTable = context.subPartitionTable_;
	subPartitionTable.clear();
	util::XArray<NodeId> liveNodeList(alloc);
	getLiveNodeIdList(liveNodeList);
	util::Random random(static_cast<int64_t>(util::DateTime::now(false).getUnixTime()));

	util::Vector<ReplicaInfo> resolvePartitionList(alloc);
	getReplicaLossPIdList(alloc, resolvePartitionList);
	util::Vector<PartitionId> catchupPIdList(alloc);
	getCurrentCatchupPIdList(catchupPIdList);

	for (size_t pos = 0; pos < resolvePartitionList.size(); pos++) {
		PartitionId targetPId = resolvePartitionList[pos].pId_;
		NodeId currentOwner = getOwner(targetPId);
		if (currentOwner == UNDEF_NODEID) {
			continue;
		}
		PartitionRole currentRole;
		getPartitionRole(targetPId, currentRole, PT_CURRENT_OB);
		PartitionRole nextRole(targetPId, revision_, PT_CURRENT_OB);
		PartitionRole goalRole;
		getPartitionRole(targetPId, goalRole, PT_CURRENT_GOAL);
		NodeId goalOwner = goalRole.getOwner();
		if (goalOwner == UNDEF_NODEID) {
			continue;
		}
		NodeIdList& currentBackups = currentRole.getBackups();
		NodeIdList& goalBackups = goalRole.getBackups();
		NodeIdList catchups;
		if (!currentRole.isOwnerOrBackup(goalOwner)) {
			if (goalBlockQueue_.find(targetPId, goalOwner)) {
				continue;
			}
			catchups.push_back(goalOwner);
			nextRole.set(currentOwner, currentBackups, catchups);
			subPartitionTable.set(this, targetPId, nextRole, currentOwner);
			prevCatchupPId_ = targetPId;
			prevCatchupNodeId_ = goalOwner;
		}
		if (subPartitionTable.size() == 1) {
			break;
		}
		util::StackAllocator::Scope scope(alloc);
		util::XArray<NodeId> backupOrderList(alloc);
		for (size_t pos = 0; pos < goalBackups.size(); pos++) {
			if (nextBlockQueue_.find(targetPId, goalBackups[pos])) {
				continue;
			}
			if (currentRole.isOwnerOrBackup(goalBackups[pos])) {
				continue;
			}
			if (goalBlockQueue_.find(targetPId, goalBackups[pos])) {
				continue;
			}
			backupOrderList.push_back(goalBackups[pos]);
		}
		if (backupOrderList.size() > 0) {
			arrayShuffle<util::XArray<NodeId>, NodeId>(backupOrderList, random);
			catchups.push_back(backupOrderList[0]);
			nextRole.set(currentOwner, currentBackups, catchups);
			subPartitionTable.set(this, targetPId, nextRole, currentOwner);
			prevCatchupPId_ = targetPId;
			prevCatchupNodeId_ = backupOrderList[0];
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

bool PartitionTable::applyRemoveRule(
	PartitionContext& context) {

	SubPartitionTable& subPartitionTable
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
		NodeIdList& currentBackups
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
	PartitionContext& context) {

	SubPartitionTable& subPartitionTable
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
		NodeIdList& currentBackups
			= currentRole.getBackups();
		NodeIdList& goalBackups
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

void PartitionTable::planNext(PartitionContext& context) {

	util::StackAllocator& alloc = context.getAllocator();
	util::StackAllocator::Scope scope(alloc);
	util::Vector<SubPartition> nextPartitionList(alloc);
	util::Vector<SubPartition> currentPartitionList(alloc);
	currentRuleNo_ = PARTITION_RULE_UNDEF;
	util::Vector<NodeId> activeNodeList(alloc);
	getLiveNodeIdList(activeNodeList);
	int32_t backupNum = getReplicationNum();
	int32_t liveNum = static_cast<int32_t>(activeNodeList.size());
	nextApplyRuleLimitTime_ = context.currentRuleLimitTime_;
	SubPartitionTable& subPartitionTable = context.subPartitionTable_;

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
		GS_TRACE_CLUSTER_INFO(std::endl << "Apply Rule:"
			<< dumpPartitionRule(currentRuleNo_) << ", revision=" << revision);
		if (subPartitionTable.size() > 0) {
			currentRevisionNo_ = revision;
			context.needUpdatePartition_ = true;
			GS_TRACE_CLUSTER_INFO("Next partitions  : "<< std::endl
				<< dumpPartitionsNew(alloc, PT_CURRENT_OB, &subPartitionTable));
		}
	}
	subPartitionTable.setRevision(getPartitionRevision());
	getNodeAddressInfo(subPartitionTable.getNodeAddressList(),
		subPartitionTable.getPublicNodeAddressList());
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

std::string DropPartitionNodeInfo::dump() {
	util::NormalOStringStream oss;
	for (size_t pos = 0; pos < dropNodeMap_.size(); pos++) {
		oss << dropNodeMap_[pos].dump();
	}
	return oss.str().c_str();
}

void PartitionTable::clearPartitionRole(PartitionId pId) {
	PartitionRole prevRole;
	getPartitionRole(pId, prevRole);
	PartitionRole tmpRole(pId, prevRole.getRevision(), PT_CURRENT_OB);
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
	bool& catchupWait) {

	PartitionRole currentRole;
	getPartitionRole(pId, currentRole);
	NodeId currentOwnerId
		= currentRole.getOwner();
	NodeIdList& currentBackups
		= currentRole.getBackups();
	NodeIdList& currentCatchups
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
	if (getRuleNo() == PARTITION_RULE_ADD
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

	if (getRuleNo() != PARTITION_RULE_ADD
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
	PartitionContext& context) {

	util::StackAllocator& alloc = context.getAllocator();
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
	ChangeTableType tableType,
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
	AddressInfo& publicAddressInfo) {

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

void PartitionTable::getNodeAddressInfo(AddressInfoList& addressInfoList,
	AddressInfoList& publicAddressInfoList) {

	int32_t nodeNum = getNodeNum();
	for (int32_t nodeId = 0; nodeId < nodeNum; nodeId++) {
		AddressInfo addressInfo;
		addressInfo.setNodeAddress(CLUSTER_SERVICE,
			getNodeInfo(nodeId).getNodeAddress(CLUSTER_SERVICE));
		addressInfo.setNodeAddress(SYNC_SERVICE, 
			getNodeInfo(nodeId).getNodeAddress(SYNC_SERVICE));
		addressInfo.setNodeAddress(TRANSACTION_SERVICE,
			getNodeInfo(nodeId).getNodeAddress(TRANSACTION_SERVICE));
		addressInfo.setNodeAddress(SYSTEM_SERVICE,
			getNodeInfo(nodeId).getNodeAddress(SYSTEM_SERVICE));
		addressInfo.setNodeAddress(SQL_SERVICE,
			getNodeInfo(nodeId).getNodeAddress(SQL_SERVICE));
		if (getHeartbeatTimeout(nodeId) != UNDEF_TTL) {
			addressInfo.isActive_ = true;
		}
		else {
			addressInfo.isActive_ = false;
		}
		addressInfoList.push_back(addressInfo);

		if (getNodeInfo(nodeId).publicAddressInfo_.isValid()) {
			AddressInfo publicAddressInfo;
			publicAddressInfo.setNodeAddress(TRANSACTION_SERVICE,
				getNodeInfo(nodeId).publicAddressInfo_.getNodeAddress(TRANSACTION_SERVICE));
			publicAddressInfo.setNodeAddress(SQL_SERVICE,
				getNodeInfo(nodeId).publicAddressInfo_.getNodeAddress(SQL_SERVICE));
			publicAddressInfoList.emplace_back(publicAddressInfo);
		}
	}
}

void PartitionTable::set(SubPartition& subPartition) {
	PartitionId pId = subPartition.pId_;
	if (pId == UNDEF_PARTITIONID) {
		return;
	}
	if (pId >= config_.partitionNum_) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}
	PartitionRole& role = subPartition.role_;
	util::LockGuard<util::WriteLock>
	currentLock(partitionLockList_[pId]);
	partitions_[pId].roles_[PT_CURRENT_OB] = role;
}

std::ostream& operator<<(std::ostream& ss, PartitionRole& partitionRole) {
	ss << partitionRole.dumpIds();
	return ss;
}

std::ostream& operator<<(std::ostream& ss, NodeAddress& nodeAddres) {
	ss << nodeAddres.toString();
	return ss;
}

std::ostream& operator<<(std::ostream& ss, PartitionRevision& revision) {
	ss << revision.toString();
	return ss;
}

std::string PartitionTable::dumpNodes() {

	util::NormalOStringStream ss;
	std::string terminateCode = "\n ";
	std::string tabCode = "\t";

	NodeInfo* nodeInfo;
	bool usePublic = hasPublicAddress();
	AddressInfoList addressInfoList;
	if (usePublic) {
		getPublicNodeAddress(addressInfoList);
	}
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
			ss << "(";
			ss << nodeInfo->heartbeatTimeout_;
			ss << ")";
		}
		else {
			ss << CommonUtility::getTimeStr(nodeInfo->heartbeatTimeout_);
		}
		if (enableRackZoneAwareness_) {
			ss << " (";
			{
				util::LockGuard<util::Mutex> lock(nodeInfoLock_);
				ss << nodeInfo->rackZoneIdName_.c_str();
			}
			ss << ")";
		}
		if (usePublic) {
			ss << " (";
			ss << addressInfoList[nodeId].getNodeAddress(TRANSACTION_SERVICE).dump().c_str();
			ss << ",";
			ss << addressInfoList[nodeId].getNodeAddress(SQL_SERVICE).dump().c_str();
			ss << ")";
		}
		if (nodeId != nodeNum_ - 1) {
			ss << terminateCode;
		}
	}
	return ss.str();
}

void PartitionTable::genPartitionColumnStr(
		util::NormalOStringStream& ss, std::string& roleName, PartitionId pId, NodeId nodeId,
		int32_t lsnDigit, int32_t nodeIdDigit, TableType type) {
	UNUSED_VARIABLE(lsnDigit);

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

std::string PartitionTable::dumpPartitionsNew(util::StackAllocator& alloc, TableType type,
	SubPartitionTable* subTable) {

	util::StackAllocator::Scope scope(alloc);
	util::NormalOStringStream ss;
	util::XArray<uint32_t> countList(alloc);
	util::XArray<uint32_t> ownerCountList(alloc);
	countList.assign(static_cast<size_t>(nodeNum_), 0);
	ownerCountList.assign(static_cast<size_t>(nodeNum_), 0);
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
	PartitionRevisionNo revision = getPartitionRevisionNo();
	PartitionRevisionNo tmpRevision = revision;
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
	case PT_STABLE_GOAL:
		partitionType = "STABLE_GOAL_TABLE";
		break;
	default:
		break;
	}
	ss << "[" << partitionType << "] "
		<< CommonUtility::getTimeStr(util::DateTime::now(TRIM_MILLISECONDS).getUnixTime())
		<< std::endl;
	if (type == PT_CURRENT_GOAL) {
		ss << dumpGoalAssignmentRule(currentGoalAssignmentRule_) << std::endl;
	}

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
			SubPartition& sub =
					subTable->getSubPartition(static_cast<int32_t>(pos));
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
		ss << "[P" << std::setfill('0') << std::setw(partitionDigit_) << pId;
		ss << "(" << std::setw(revisionDigit) << role.getRevisionNo() << ")]";
		NodeId owner = role.getOwner();
		ss << " ";
		if (owner == UNDEF_NODEID) {
		}
		else {
			genPartitionColumnStr(ss, ownerStr, pId, owner, digit, nodeIdDigit, type);
		}
		NodeIdList& backups = role.getBackups();
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
		NodeIdList& catchups = role.getCatchups();
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


std::string PartitionTable::dumpPartitions(util::StackAllocator& alloc, TableType type, bool isSummary) {

	util::StackAllocator::Scope scope(alloc);
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
	case PT_STABLE_GOAL:
		partitionType = "STABLE_GOAL_TABLE";
		break;
	default:
		break;
	}
	if (type == PT_CURRENT_GOAL) {
		ss << dumpGoalAssignmentRule(currentGoalAssignmentRule_) << std::endl;
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
		NodeId currentOwnerNodeId = role.getOwner();
		PartitionRevision& revision = role.getRevision();
		NodeIdList& backups = role.getBackups();
		NodeIdList& catchups = role.getCatchups();
		int32_t backupNum = static_cast<int32_t>(backups.size());
		int32_t catchupNum = static_cast<int32_t>(catchups.size());

		for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
			if (currentOwnerNodeId == nodeId) {
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
		ss << "[" << std::setw(lsnDigit_) << maxLsnList_[pId] << "]";
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

std::string PartitionTable::dumpPartitionsList(util::StackAllocator& alloc, TableType type) {

	util::StackAllocator::Scope scope(alloc);
	util::NormalOStringStream ss;
	util::XArray<uint32_t> countList(alloc);
	util::XArray<uint32_t> ownerCountList(alloc);
	countList.assign(static_cast<size_t>(nodeNum_), 0);
	ownerCountList.assign(static_cast<size_t>(nodeNum_), 0);
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
		PartitionRole* role = &partitions_[pId].roles_[type];
		NodeIdList currentBackupList;
		NodeId currentOwnerNodeId = role->getOwner();
		NodeIdList& backups = role->getBackups();
		ss << "{pId=" << pId
			<< ", owner=" << dumpNodeAddress(currentOwnerNodeId)
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

std::string PartitionTable::dumpDatas(bool isDetail, bool lsnOnly) {

	PartitionId pId;
	uint32_t partitionNum = config_.partitionNum_;
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

	util::NormalOStringStream ss;
	std::string terminateCode = "\n ";

	ss << terminateCode << "[LSN LIST]" << terminateCode;
	for (pId = 0; pId < partitionNum; pId++) {
		ss << "[P" << std::setfill('0') << std::setw(partitionDigit_) << pId << "]";
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

	if (!lsnOnly) {
		ss << terminateCode << "[DETAIL LIST]" << terminateCode;
		int32_t revisionDigit = 0;
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

		ss << terminateCode << "[STATUS LIST]" << terminateCode;
		for (pId = 0; pId < partitionNum; pId++) {
			ss << "[P" << std::setfill('0') << std::setw(partitionDigit_) << pId << "]";
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
			ss << "[P" << std::setfill('0') << std::setw(partitionDigit_) << pId << "]";
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
			ss << "[P" << std::setfill('0') << std::setw(partitionDigit_) << pId << "]";
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
		isDetail = true;
		if (isDetail) {
			ss << terminateCode << "[START LSN LIST]" << terminateCode;
			for (pId = 0; pId < partitionNum; pId++) {
				ss << "[P" << std::setfill('0') << std::setw(partitionDigit_) << pId << "]";
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
	}
	return ss.str();
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
	ss1 << " {pId=" << pId_ << ", owner=" << ownerNodeId_ << ", backups=" << ss2.str()
		<< ", catchups=" << ss3.str() << ", revision="
		<< revision_.toString() << "}";

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
	PartitionTable* pt,
	ServiceType serviceType) {

	util::NormalOStringStream ss1;
	util::NormalOStringStream ss2;
	util::NormalOStringStream ss3;

	ss2 << "[";
	for (size_t pos = 0; pos < backups_.size(); pos++) {
		NodeAddress& address =
			pt->getNodeAddress(backups_[pos], serviceType);
		ss2 << address.toString();
		if (pos != backups_.size() - 1) {
			ss2 << ",";
		}
	}
	ss2 << "]";

	ss3 << "[";
	for (size_t pos = 0; pos < catchups_.size(); pos++) {
		NodeAddress& address =
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

template void PartitionTable::getActiveNodeList(NodeIdList& activeNodeList,
	util::Set<NodeId>& downNodeList, int64_t currentTime);

util::Vector<int32_t>& PartitionTable::NodesStat::getTargetList(int32_t target) {
	switch (target) {
	case OWNER_COUNT:
		return ownerCountList_;
	case DATA_COUNT:
		return dataCountList_;
	case OWNER_RACKZONE_COUNT:
		return ownerRackZoneCountList_;
	case DATA_RACKZONE_COUNT:
		return dataRackZoneCountList_;
	case RACKZONE_NODE_COUNT:
		return nodeRackZoneCountList_;
	case RACKZONE_ID_LIST:
		return rackZoneIdList_;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_PT_INTERNAL, "");
	}
}

double PartitionTable::NodesStat::calc(int32_t statType, int32_t target) {

	util::Vector<int32_t>& targeCountList = getTargetList(target);
	switch (statType) {
	case AVERAGE:
		return CommonUtility::average(targeCountList);
	case VARIANCE:
		return CommonUtility::variance(targeCountList);
	case STANDARD_DEVIATION:
		return CommonUtility::standardDeviation(targeCountList);
	default:
		return -1;
	}
}

void PartitionTable::NodesStat::dumpTypeName(util::NormalOStringStream& oss, int32_t type) {
	switch (type) {
	case AVERAGE:
		oss << "AVERAGE";
		break;
	case VARIANCE:
		oss << "VARIANCE";
		break;
	case STANDARD_DEVIATION:
		oss << "STANDARD_DEVIATION";
		break;
	default:
		break;
	}
}

void PartitionTable::NodesStat::dumpTargetName(util::NormalOStringStream& oss, int32_t target) {
	switch (target) {
	case OWNER_COUNT:
		oss << "OWNER_COUNT";
		break;
	case DATA_COUNT:
		oss << "DATA_COUNT";
		break;
	case OWNER_RACKZONE_COUNT:
		oss << "OWNER_RACKZONE_COUNT";
		break;
	case DATA_RACKZONE_COUNT:
		oss << "DATA_RACKZONE_COUNT";
		break;
	case RACKZONE_NODE_COUNT:
		oss << "RACKZONE_NODE_COUNT";
		break;
	case RACKZONE_ID_LIST:
		oss << "RACKZONE_ID_LIST";
		break;
	default:
		break;
	}
}

void PartitionTable::NodesStat::dumpHeader(util::NormalOStringStream &oss, int32_t type, int32_t target) {

	oss << "(";
	dumpTypeName(oss, type);
	oss << ",";
	dumpTargetName(oss, target);
	oss << ")";
}

std::string PartitionTable::NodesStat::dump() {

	util::NormalOStringStream oss;
	for (int32_t target = 0; target < TARGET_MAX; target++) {
		dumpTargetName(oss, target);
		oss << " ";
		CommonUtility::dumpValueList(oss, getTargetList(target));
		oss << std::endl;
	}

	for (int32_t type = 0; type < TYPE_MAX; type++) {
		for (int32_t target = 0; target < RACKZONE_NODE_COUNT; target++) {
			dumpHeader(oss, type, target);
			oss << " " << calc(type, target) << std::endl;
		}
	}
	return oss.str().c_str();
}

bool PartitionTable::NodesStat::check(
		util::StackAllocator& alloc, double ratio) {
	UNUSED_VARIABLE(alloc);

	if (nodeRackZoneCountList_.size() == 0) return true;
	double ownerSd = calc(STANDARD_DEVIATION, OWNER_COUNT);
	double dataSd = calc(STANDARD_DEVIATION, DATA_COUNT);
	return (ownerSd <= ratio && dataSd <= ratio);
}

void PartitionTable::NodesStat::filter(util::Vector<int32_t>& tmpList, int32_t type) {
	util::Vector<int32_t> &targetList = getTargetList(type);
	for (size_t pos = 0; pos < tmpList.size(); pos++) {
		if (tmpList[pos] != -1) {
			targetList.push_back(tmpList[pos]);
		}
	}
}

void PartitionTable::stat(util::StackAllocator& alloc, TableType type, NodesStat& stat) {
	util::Vector<int32_t> tmpOwnerList(nodeNum_, -1, alloc);
	util::Vector<int32_t> tmpDataList(nodeNum_, -1, alloc);
	util::Vector<int32_t> tmpRackZoneOwnerList(nodeNum_, -1, alloc);
	util::Vector<int32_t> tmpRackZoneDataList(nodeNum_, -1, alloc);
	util::Vector<int32_t> tmpNodeRackZoneList(nodeNum_, -1, alloc);
	util::Vector<RackZoneId> tmpRackZoneIdList(nodeNum_, -1, alloc);

	for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
		if (getHeartbeatTimeout(nodeId) != UNDEF_TTL) {
			tmpOwnerList[nodeId] = 0;
			tmpDataList[nodeId] = 0;
			if (enableRackZoneAwareness_) {
				RackZoneId rackZoneId = getRackZoneId(nodeId);
				if (rackZoneId != UNDEF_RACKZONEID) {
					tmpRackZoneOwnerList[rackZoneId] = 0;
					tmpRackZoneDataList[rackZoneId] = 0;
					if (tmpNodeRackZoneList[rackZoneId] == UNDEF_RACKZONEID) {
						tmpNodeRackZoneList[rackZoneId] = 0;
					}
					tmpNodeRackZoneList[rackZoneId]++;
					tmpRackZoneIdList[nodeId] = rackZoneId;
				}
			}
		}
	}

	for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
		PartitionRole role;
		getPartitionRole(pId, role, type);
		NodeId owner = role.getOwner();
		if (owner != UNDEF_NODEID) {
			tmpOwnerList[owner]++;
			tmpDataList[owner]++;
			if (enableRackZoneAwareness_) {
				RackZoneId rackZoneId = getRackZoneId(owner);
				if (rackZoneId != UNDEF_RACKZONEID) {
					tmpRackZoneOwnerList[rackZoneId]++;
					tmpRackZoneDataList[rackZoneId]++;
				}
			}
		}
		std::vector<NodeId>& backups = role.getBackups();
		for (size_t pos = 0; pos < backups.size(); pos++) {
			tmpDataList[backups[pos]]++;
			if (enableRackZoneAwareness_) {
				RackZoneId rackZoneId = getRackZoneId(backups[pos]);
				if (rackZoneId != UNDEF_RACKZONEID) {
					tmpRackZoneDataList[rackZoneId]++;
				}
			}
		}
	}
	stat.filter(tmpOwnerList, NodesStat::OWNER_COUNT);
	stat.filter(tmpDataList, NodesStat::DATA_COUNT);
	if (enableRackZoneAwareness_) {
		stat.filter(tmpRackZoneOwnerList, NodesStat::OWNER_RACKZONE_COUNT);
		stat.filter(tmpRackZoneDataList, NodesStat::DATA_RACKZONE_COUNT);
		stat.filter(tmpNodeRackZoneList, NodesStat::RACKZONE_NODE_COUNT);
		stat.filter(tmpRackZoneIdList, NodesStat::RACKZONE_ID_LIST);
	}
}

#include <algorithm>
void PartitionTable::progress(double& goalProgress, double& replicaProgress) {
	try {
		int32_t goalCount = 0;
		int32_t currentCount = 0;
		int32_t currentReplicaCount = 0;
		NodeIdList activeNodeList;
		getLiveNodeIdList(activeNodeList);
		int32_t liveNum = static_cast<int32_t>(activeNodeList.size());
		int32_t backupNum = getReplicationNum();
		if (backupNum > liveNum - 1) {
			backupNum = liveNum - 1;
		}
		int32_t allReplicaCount = getPartitionNum() * (backupNum + 1);
		for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
			PartitionRole goal;
			PartitionRole current;
			getPartitionRole(pId, goal, PT_CURRENT_GOAL);
			getPartitionRole(pId, current, PT_CURRENT_OB);
			currentReplicaCount += current.getRoledSize();
			if (goal.getOwner() != UNDEF_NODEID) {
				goalCount += goal.getRoledSize();
				NodeIdList& currentBackupList = current.getBackups();
				if (goal.getOwner() == current.getOwner()) {
					currentCount++;
				}
				if (currentBackupList.size() > 0) {
					std::set<NodeId> currentSet;
					for (size_t pos = 0; pos < currentBackupList.size(); pos++) {
						currentSet.insert(currentBackupList[pos]);
					}
					NodeIdList& goalBackupList = goal.getBackups();
					for (size_t pos = 0; pos < goalBackupList.size(); pos++) {
						if (currentSet.count(goalBackupList[pos])) {
							currentCount++;
						}
					}
				}
			}
		}
		replicaProgress = (allReplicaCount > 0) ? ((double)currentReplicaCount / (double)allReplicaCount) : 0;
		goalProgress = (goalCount > 0) ? ((double)currentCount / (double)goalCount) : 0;
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void PartitionTable::getGoalPartitions(
	util::StackAllocator& alloc, const  ServiceTypeInfo& addressType,
	picojson::value& result, TableType target) {
	try {
		util::Vector<PartitionRole> goalList(alloc);
		copyGoal(goalList, target);
		picojson::array& partitionList = picojsonUtil::setJsonArray(result);
		for (uint32_t pId = 0; pId < goalList.size(); pId++) {
			picojson::object partition;
			util::NormalOStringStream oss;
			partition["pId"] = picojson::value(CommonUtility::makeString(oss, pId));

			PartitionRole& currentRole = goalList[pId];
			NodeId ownerNodeId = currentRole.getOwner();
			picojson::object master;

			getServiceAddress(ownerNodeId, master, addressType);
			if (ownerNodeId != UNDEF_NODEID) {
				partition["owner"] = picojson::value(master);
			}
			else {
				partition["owner"] = picojson::value();
			}
			picojson::array& nodeList = picojsonUtil::setJsonArray(partition["backup"]);
			NodeIdList& backups = currentRole.getBackups();
			for (size_t pos = 0; pos < backups.size(); pos++) {
				picojson::object backup;
				NodeId backupNodeId = backups[pos];
				getServiceAddress(backupNodeId, backup, addressType);
				nodeList.push_back(picojson::value(backup));
			}
			partitionList.push_back(picojson::value(partition));
		}
	}
	catch (std::exception& e) {
		return GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

bool PartitionTable::setGoalPartitions(util::StackAllocator& alloc,
	const picojson::value* paramValue, TableType tableType, bool checkNode) {
	bool forSSL = isUseSSL();
	bool stableCheck = (tableType == PT_STABLE_GOAL);

	try {
		util::Set<NodeAddress> constructionNodeSet(alloc);
		util::Map<NodeAddress, NodeId> addressMap(alloc);
		util::Map<NodeAddress, NodeId>::iterator itMap;
		util::Vector<NodeId> liveNodeIdList(alloc);
		util::Vector<PartitionRole> roleList(alloc);
		roleList.reserve(getPartitionNum());
		if (checkNode) {
			getLiveNodeIdList(liveNodeIdList);
		}
		else {
			getAllNodeIdList(liveNodeIdList);
		}
		for (size_t pos = 0; pos < liveNodeIdList.size(); pos++) {
			NodeId nodeId = liveNodeIdList[pos];
			NodeAddress address = getNodeAddress(nodeId, SYSTEM_SERVICE);
			if (!address.isValid()) {
				GS_THROW_USER_ERROR(GS_ERROR_PT_INVALID_GOAL_TABLE, "");
			}
			if (forSSL) {
				address.port_ = static_cast<uint16_t>(getSSLPortNo(nodeId));
			}
			addressMap.insert(std::make_pair(address, nodeId));
		}
		int32_t partitionCount = 0;
		if (paramValue != NULL) {
			util::Set<PartitionId> pIdSet(alloc);
			std::string jsonString(picojson::value(*paramValue).serialize());
			const picojson::array& entryList = JsonUtils::as<picojson::array>(*paramValue);
			for (picojson::array::const_iterator entryIt = entryList.begin();
				entryIt != entryList.end(); ++entryIt) {
				const picojson::value& entry = *entryIt;
				std::string pname = JsonUtils::as<std::string>(entry, "pId");
				const picojson::value* owner = JsonUtils::find<picojson::value>(entry, "owner");
				const picojson::array* backupList = JsonUtils::find<picojson::array>(entry, "backup");
				int64_t intVal;
				PartitionId pId = 0;

				if (pname.size() > 0 && SQLProcessor::ValueUtils::toLong(pname.data(), pname.size(), intVal)) {
					pId = static_cast<int32_t>(intVal);
				}
				else {
					GS_THROW_USER_ERROR(GS_ERROR_SC_GOAL_INVALID_FORMAT,
						"Invalid format, pId=" << pname);
				}
				if (pId >= getPartitionNum()) {
					GS_THROW_USER_ERROR(
						GS_ERROR_SC_GOAL_INVALID_FORMAT, "Invalid format range, pId=" << pId);
				}
				partitionCount++;
				if (owner == NULL) {
					GS_THROW_USER_ERROR(
						GS_ERROR_SC_GOAL_NOT_OWNER, "Invalid format, not owner, pId=" << pId);
				}
				util::Set<PartitionId>::iterator it = pIdSet.find(pId);
				if (it != pIdSet.end()) {
					GS_THROW_USER_ERROR(GS_ERROR_SC_GOAL_DUPLICATE_PARTITION,
						"Duplicated partition pId=" << pId);
				}
				pIdSet.insert(pId);
				PartitionRole role;
				role.init(pId);
				util::Set<NodeId> assignedList(alloc);
				for (size_t i = 0;; i++) {
					const picojson::value* addressValue;
					if (i == 0) {
						addressValue = owner;
					}
					else {
						if (backupList == NULL || i > backupList->size()) {
							break;
						}
						addressValue =
							JsonUtils::find<picojson::value>((*backupList)[i - 1]);
					}
					if (addressValue == NULL ||
						!addressValue->is<picojson::object>()) {
						continue;
					}
					const char* systemAddress = JsonUtils::as<std::string>(*addressValue, "address").c_str();
					uint16_t systemPort = static_cast<uint16_t>(
						JsonUtils::asInt<uint16_t>(*addressValue, "port"));;
					try {
						const util::SocketAddress address(systemAddress, systemPort);
					}
					catch (std::exception& e) {
						GS_THROW_USER_ERROR(GS_ERROR_SC_GOAL_RESOLVE_NODE_FAILED,
							"Target node is not resolve, reason=" << GS_EXCEPTION_MESSAGE(e));
					}
					NodeAddress tmpAddress(systemAddress, systemPort);
					itMap = addressMap.find(tmpAddress);
					if (itMap == addressMap.end()) {
						GS_THROW_USER_ERROR(GS_ERROR_SC_GOAL_RESOLVE_NODE_FAILED,
							"Invalid node address=" << tmpAddress.dump() << ", pId=" << pId);
					}
					constructionNodeSet.insert(tmpAddress);
					NodeId nodeId = (*itMap).second;
					if (nodeId == UNDEF_NODEID && checkNode) {
						GS_THROW_USER_ERROR(GS_ERROR_SC_GOAL_RESOLVE_NODE_FAILED,
							"Invalid node address=" << tmpAddress.dump() << ", pId=" << pId);
					}
					util::Set<NodeId>::iterator it = assignedList.find(nodeId);
					if (it != assignedList.end()) {
						GS_THROW_USER_ERROR(GS_ERROR_SC_GOAL_INVALID_FORMAT,
							"Duplicated node address=" << tmpAddress.dump() << ", pId=" << pId);
					}
					assignedList.insert(nodeId);
					NodeAddress& nodeAddress = getNodeAddress(nodeId, CLUSTER_SERVICE);
					if (i == 0) {
						if (nodeId == UNDEF_NODEID) {
							GS_THROW_USER_ERROR(GS_ERROR_SC_GOAL_NOT_OWNER,
								"Not setted owner address, pId=" << pId);
						}
						role.setOwner(nodeId);
						role.getOwnerAddress() = nodeAddress;
					}
					else {
						role.getBackups().push_back(nodeId);
					}
				}
				role.encodeAddress(this, role);
				if (!stableCheck) {
					updateGoal(role, PT_CURRENT_GOAL);
				}
				else {
					roleList.push_back(role);
				}
			}
			if (isMaster() && !stableCheck) {
				GS_TRACE_CLUSTER_INFO(dumpPartitionsNew(alloc, PartitionTable::PT_CURRENT_GOAL));
			}
		}
		if (stableCheck) {
			if (partitionCount == static_cast<int64_t>(getPartitionNum())) {
				if (!checkNode) {
					stableGoal_.updateAll(roleList);
					return true;
				}
				util::Set<NodeAddress> liveNodeAddressSet(alloc);
				getLiveNodeAddressSet(liveNodeAddressSet);
				if (liveNodeAddressSet == constructionNodeSet) {
					stableGoal_.updateAll(roleList);
					goal_.updateAll(roleList);
					if (stableGoal_.isEnableInitialGenerate()) {
						stableGoal_.setAssigned();
					}
					return true;
				}
				else {
					util::NormalOStringStream oss1, oss2;
					CommonUtility::dumpValueByFunction(oss1, liveNodeAddressSet);
					CommonUtility::dumpValueByFunction(oss2, constructionNodeSet);
					GS_THROW_USER_ERROR(GS_ERROR_SC_GOAL_INVALID_FORMAT,
						"Cluster construction is not match (expected=" <<  oss1.str()  << ", actual=" << oss2.str() << ")");
				}
			}
			else {
				if (checkNode) {
					GS_THROW_USER_ERROR(GS_ERROR_SC_GOAL_INVALID_FORMAT,
						"Not all partition assignment (expected=" << getPartitionNum() << ", actual=" << partitionCount << ")");
				}
			}
		}
		return false;
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(CLUSTER_OPERATION, e, "");
		if (stableCheck) {
			stableGoal_.clear();
			return false;
		}
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void PartitionTable::writeStableGoal(util::StackAllocator& alloc) {
	ServiceTypeInfo addressType(SYSTEM_SERVICE, isUseSSL());
	picojson::value result;
	getGoalPartitions(alloc, addressType, result, PT_CURRENT_GOAL);
	std::string input(result.serialize());
	stableGoal_.setGoalData(input);
	stableGoal_.setCheckSum();
	stableGoal_.writeFile();
	stableGoal_.setAssigned();
}

bool PartitionTable::StableGoalPartition::writeFile() {
	if (!enable_ || !enableInitialGenerate_) {
		return false;
	}
	try {
		if (!util::FileSystem::exists(goalFileName_.c_str())) {
			UtilFile::writeFile(goalFileName_, goalData_.data(), goalData_.size(), false, 10, 100);
			GS_TRACE_INFO(CLUSTER_OPERATION, GS_TRACE_CS_TRACE_STABLE_GOAL,
				"Generate stable goal file (fileName='" << goalFileName_ << "')");
			return true;
		}
		else {
			GS_TRACE_WARNING(CLUSTER_OPERATION, GS_TRACE_CS_TRACE_STABLE_GOAL,
				"Skip generating stable goal (fileName='" << goalFileName_ << "', reason=already exists file)");
			return false;
		}
	}
	catch (std::exception& e) {
		UTIL_TRACE_EXCEPTION_WARNING(CLUSTER_OPERATION, e, "Failed to generate stable goal (fileName='" << goalFileName_ << "')");
		return false;
	}
}

bool PartitionTable::GoalPartition::load() {
	if (goalFileName_.empty()) {
		return false;
	}
	try {
		if (!util::FileSystem::exists(goalFileName_.c_str())) {
			if (!isEnableInitialGenerate()) {
				GS_TRACE_CLUSTER_INFO("Failed to load stable goal (fileName='" << goalFileName_ << "'  is already exists )");
			}
			return false;
		}
		std::allocator<char> alloc;
		util::XArray< uint8_t, util::StdAllocator<uint8_t, void> > buf(alloc);
		ParamTable::fileToBuffer(goalFileName_.c_str(), buf);
		std::string allStr(buf.begin(), buf.end());
		CommonUtility::replaceAll(allStr, "\r\n", "\n");
		CommonUtility::replaceAll(allStr, "\r", "\n");
		goalData_ = allStr;
		GS_TRACE_CLUSTER_INFO("Load stable goal (fileName='" << goalFileName_ << "')");
		return true;
	}
	catch (std::exception& e) {
		UTIL_TRACE_EXCEPTION_WARNING(CLUSTER_OPERATION, e, "Failed to load stable goal (fileName='" << goalFileName_ <<"')");
		return false;
	}
}

bool PartitionTable::StableGoalPartition::load() {
	if (enable_) {
		return GoalPartition::load();
	}
	return false;
}

void PartitionTable::StableGoalPartition::writeStableGoal(std::string& goal) {
	try {
		if (isEnable() && isEnableInitialGenerate()) {
			setGoalData(goal);
			picojson::value value;
			if (validateFormat(value)) {
				writeFile();
			}
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}


std::string PartitionTable::dumpGoalAssignmentRule() {
	return dumpGoalAssignmentRule(goalAssignmentRule_);
}

std::string PartitionTable::dumpGoalAssignmentRule(int32_t rule) {
	switch (rule) {
		case PARTITION_ASSIGNMENT_RULE_DEFAULT: return "DEFAULT";
		case PARTITION_ASSIGNMENT_RULE_ROUNDROBIN: return "ROUNDROBIN";
		case PARTITION_ASSIGNMENT_RULE_STABLE_GOAL: return "STABLE_GOAL";
		case PARTITION_ASSIGNMENT_RULE_PARTIAL_STABLE_GOAL: return "PARTIAL_STABLE_GOAL";
		case PARTITION_ASSIGNMENT_RULE_COMMAND_SET_GOAL: return "COMMAND_SET_GOAL";
		default: return "UNDEF_RULE";
	}
}

PartitionTable::GoalPartition:: GoalPartition(const char* goalFileDir, const char* goalFileName) : 
	partitionNum_(1), isValidateFormat_(false), enableInitialGenerate_(false), hashValue_(-1) {
	if (goalFileDir != NULL && goalFileName != NULL) {
		util::FileSystem::createPath(goalFileDir, goalFileName, goalFileName_);
	}
}

PartitionTable::StableGoalPartition::StableGoalPartition(const ConfigTable& config, const char* configDir) :
	GoalPartition(configDir, config.get<const char8_t*>(CONFIG_TABLE_CS_STABLE_GOAL_FILE_NAME)),
	enable_(config.get<bool>(CONFIG_TABLE_CS_ENABLE_STABLE_GOAL)),
	policy_(config.get<int32_t>(CONFIG_TABLE_CS_STABLE_GOAL_POLICY)),
	configDir_(configDir), isAssigned_(false) {
	enableInitialGenerate_ = (config.get<bool>(CONFIG_TABLE_CS_GENERATE_INITIAL_STABLE_GOAL));
}

bool PartitionTable::StableGoalPartition::validateFormat(picojson::value& value, bool useCheckSum) {
	try {
		const std::string err = JsonUtils::parseAll(value, goalData_.c_str(), goalData_.c_str() + goalData_.size());
		if (!err.empty()) {
			try {
				GS_THROW_USER_ERROR(GS_ERROR_PT_INVALID_STABLE_GOAL_TABLE,
					"Failed to parse stable goal (fileName='" << goalFileName_ << "' , reason=(" << err.c_str() << "))");
			}
			catch (std::exception& e) {
				UTIL_TRACE_EXCEPTION_WARNING(CLUSTER_OPERATION, e, "");
			}
			hashValue_ = -1;
			isValidateFormat_ = false;
			return false;
		}
		else {
			isValidateFormat_ = true;
			if (useCheckSum) {
				int64_t  prevHashValue = hashValue_;
				hashValue_ = calcCheckSum();
				if (prevHashValue != -1 && prevHashValue != hashValue_) {
					GS_TRACE_WARNING(CLUSTER_OPERATION, GS_TRACE_CS_TRACE_STABLE_GOAL,
						"Stable goal file may be changed from initial (fileName='"
						<< goalFileName_ << ", hashValue (prev = " << prevHashValue << ", current = " << hashValue_ << ")");
				}
			}
			return true;
		}
	}
	catch (std::exception& e) {
		UTIL_TRACE_EXCEPTION_WARNING(CLUSTER_OPERATION, e, "");
		return false;
	}
}

uint32_t PartitionTable::StableGoalPartition::calcCheckSum() {
	size_t size = std::min(goalData_.size(), static_cast<size_t>(MAX_CALC_HASH_SIZE));
	return util::CRC32::calculate(goalData_.data(), size);
}

void PartitionTable::StableGoalPartition::setCheckSum() {
	hashValue_ = calcCheckSum();
}

bool PartitionTable::StableGoalPartition::isSucceedOnError() {
	return (enable_ && policy_ == 1 && isValidateFormat());
}

void PartitionTable::StableGoalPartition::initialCheck() {
	try {
		if (!enable_) {
			return;
		}
		picojson::value value;
		if (load() && validateFormat(value)) {
			hashValue_ = calcCheckSum();
			goalData_.clear();
			isValidateFormat_ = false;
		}
		else {
			if (!enableInitialGenerate_) {
				GS_THROW_USER_ERROR(GS_ERROR_CS_CONFIG_ERROR,
					"Invalid stable goal config parameter (enableStableGoal=true, generateInitialStableGoal=false)");
			}
			return;
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}


void PartitionTable::preparePlanGoal() {
	for (int32_t i = 0; i < getNodeNum(); i++) {
		if (getHeartbeatTimeout(i) == UNDEF_TTL) {
			resetDownNode(i);
		}
	}
	for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
		clearCatchupRole(pId);
	}
}

void PartitionTable::setStandbyMode(bool standbyMode, NodeId nodeId) {
	if (nodeId >= nodeNum_) {
		return;
	}
	nodes_[nodeId].standbyMode_ = standbyMode;
}

bool PartitionTable::getStandbyMode(NodeId nodeId) {
	if (nodeId >= nodeNum_) {
		return false;
	}
	return nodes_[nodeId].standbyMode_;
}

