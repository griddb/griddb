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
	@brief Implementation of PartitionTable
*/

#include "partition_table.h"
#include "util/net.h"
#include "util/trace.h"
#include "cluster_manager.h"
#include "config_table.h"
#include "picojson.h"


UTIL_TRACER_DECLARE(CLUSTER_SERVICE);
UTIL_TRACER_DECLARE(CLUSTER_OPERATION);

#include "gs_error.h"
#include <iomanip>
class ConfigTable;

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
#ifdef GD_ENABLE_UNICAST_NOTIFICATION
		return transactionAddress_;
#else
		return txnAddress_;
#endif
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
#ifdef GD_ENABLE_UNICAST_NOTIFICATION
		transactionAddress_ = address;
#else
		txnAddress_ = address;
#endif
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
		  util::AllocatorInfo(ALLOCATOR_GROUP_CS, "partitionTableFixed"),
		  1 << 18),
	  alloc_(util::AllocatorInfo(ALLOCATOR_GROUP_CS, "partitionTableStack"),
		  &fixedSizeAlloc_),
	  nodeNum_(0),
	  masterNodeId_(UNDEF_NODEID),
	  partitionLockList_(NULL),
	  config_(configTable, MAX_NODE_NUM),
	  prevMaxNodeId_(0),
	  nextBlockQueue_(configTable.get<int32_t>(CONFIG_TABLE_DS_PARTITION_NUM)),
	  goalBlockQueue_(configTable.get<int32_t>(CONFIG_TABLE_DS_PARTITION_NUM)),
	  promoteCandPId_(UNDEF_PARTITIONID),
	  prevCatchupPId_(UNDEF_PARTITIONID),
	  currentCatchupPId_(UNDEF_PARTITIONID),
	  prevCatchupNodeId_(UNDEF_NODEID),
	  partitions_(NULL),
	  nodes_(NULL),
	  loadInfo_(MAX_NODE_NUM),
	  failCatchup_(false),
	  isRepaired_(false),
	  prevFailoverTime_(0) {
	try {
		Partition dummyPartition;
		uint32_t partitionNum = config_.partitionNum_;
		partitionLockList_ = ALLOC_NEW(alloc_) util::RWLock[partitionNum];
		partitions_ = ALLOC_NEW(alloc_) Partition[partitionNum];

		NodeId maxNodeNum = config_.limitClusterNodeNum_;
		PartitionGroupConfig pgConfig(configTable);
		for (PartitionId pId = 0; pId < partitionNum; pId++) {
			partitions_[pId].init(
				alloc_, pId, pgConfig.getPartitionGroupId(pId), maxNodeNum);
		}

		nodes_ = ALLOC_NEW(alloc_) NodeInfo[maxNodeNum];
		for (NodeId nodeId = 0; nodeId < maxNodeNum; nodeId++) {
			nodes_[nodeId].init(alloc_);
		}
		maxLsnList_.assign(partitionNum, 0);
		repairLsnList_.assign(partitionNum, UNDEF_LSN);
		currentActiveOwnerList_.assign(partitionNum, UNDEF_NODEID);

		gapCountList_.assign(partitionNum, 0);

		nextBlockQueue_.init(alloc_);
		goalBlockQueue_.init(alloc_);

		int32_t digit = 0;
		for (; partitionNum != 0; digit++) {
			partitionNum /= 10;
		}
		partitionDigit_ = digit;
		lsnDigit_ = 10;

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
	  maxNodeNum_(maxNodeNum),
	  isSucceedCurrentOwner_(false) {
	limitOwnerBackupLsnDetectErrorGap_ =
		limitOwnerBackupLsnGap_ * PT_BACKUP_GAP_FACTOR;

	limitOwnerBackupLsnDropGap_ =
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
	}
	for (NodeId nodeId = 0; nodeId < config_.getLimitClusterNodeNum();
		 nodeId++) {
		if (nodes_) {
			ALLOC_DELETE(alloc_, &nodes_[nodeId]);
		}
	}
}

NodeId PartitionTable::getOwner(PartitionId pId, TableType type) {
	return partitions_[pId].roles_[type].getOwner();
}


bool PartitionTable::isOwner(
	PartitionId pId, NodeId targetNodeId, TableType type) {
	return (partitions_[pId].roles_[type].isOwner(targetNodeId));
}

bool PartitionTable::isOwnerOrBackup(
	PartitionId pId, NodeId targetNodeId, TableType type) {
	if (targetNodeId == 0) {
		return (partitions_[pId].roles_[type].isOwner(targetNodeId) ||
				partitions_[pId].roles_[type].isBackup(targetNodeId));
	}
	else {
		util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
		return (partitions_[pId].roles_[type].isOwner(targetNodeId) ||
				partitions_[pId].roles_[type].isBackup(targetNodeId));
	}
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

bool PartitionTable::isBackup(
	PartitionId pId, NodeId targetNodeId, TableType type) {
	if (targetNodeId == 0) {
		return (partitions_[pId].roles_[type].isBackup(targetNodeId));
	}
	else {
		util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
		return (partitions_[pId].roles_[type].isBackup(targetNodeId));
	}
}

bool PartitionTable::setNodeInfo(
	NodeId nodeId, ServiceType type, NodeAddress &address) {
	if (nodeId >= config_.limitClusterNodeNum_) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_SET_INVALID_NODE_ID, "");
	}

	if (nodeId >= nodeNum_) {
		nodeNum_ = nodeId + 1;
	}
	return nodes_[nodeId].nodeBaseInfo_[type].set(address);
}

void PartitionTable::setGossip(PartitionId pId) {
	util::LockGuard<util::WriteLock> lock(lock_);
	gossips_.insert(pId);
}

void PartitionTable::setBlockQueue(
	PartitionId pId, ChangeTableType tableType, NodeId nodeId) {
	if (pId >= getPartitionNum() || nodeId >= getNodeNum()) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_SET_INVALID_NODE_ID, "");
	}
	util::LockGuard<util::WriteLock> lock(lock_);

	if (tableType == PT_CHANGE_NEXT_TABLE) {
		nextBlockQueue_.push(pId, nodeId);
	}
	else if (tableType == PT_CHANGE_GOAL_TABLE) {
		goalBlockQueue_.push(pId, nodeId);
	}
	else {
	}
}

/*!
	@brief Changes status to a specified type
*/
void PartitionTable::updatePartitionRole(PartitionId pId, TableType type) {
	TableType sourceType;
	switch (type) {
	case PT_CURRENT_OB: {
		sourceType = PT_NEXT_OB;
		break;
	}
	case PT_CURRENT_GOAL: {
		sourceType = PT_NEXT_GOAL;
		break;
	}
	case PT_NEXT_OB: {
		sourceType = PT_TMP_OB;
		break;
	}
	case PT_NEXT_GOAL: {
		sourceType = PT_TMP_GOAL;
		break;
	}
	default: {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INVALID_PARTITION_ROLE_TYPE, "");
	}
	}

	UTIL_TRACE_INFO(CLUSTER_SERVICE,
		"next:" << partitions_[pId].roles_[sourceType].dumpIds());
	UTIL_TRACE_INFO(
		CLUSTER_SERVICE, "current:" << partitions_[pId].roles_[type].dumpIds());

	util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);

	partitions_[pId].roles_[type] = partitions_[pId].roles_[sourceType];
	partitions_[pId].roles_[type].set(type);

	if (type == PT_CURRENT_OB) {
		PartitionRoleStatus nextStatus =
			partitions_[pId].roles_[type].getPartitionRoleStatus();
		partitions_[pId].setPartitionRoleStatus(0, nextStatus);
	}
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
			PartitionRole nextRole;
			getPartitionRole(pId, nextRole);
			ownerNodeId = nextRole.getOwner();
			if (ownerNodeId == UNDEF_NODEID || !isActive(pId)) {
				isPartial = true;
				continue;
			}

			loadInfo_.add(ownerNodeId);
			loadInfo_.add(ownerNodeId, false);

			std::vector<NodeId> backups = nextRole.getBackups();
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

		TRACE_CLUSTER_NORMAL(WARNING,
			loadInfo_.dump()
				<< ", ownerCheckCount:" << getPartitionNum()
				<< ", dataCheckCount:" << (backupNum + 1) * getPartitionNum());

		loadInfo_.status_ = retStatus;
		return retStatus;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Checks the condition for DropPartition
*/
bool PartitionTable::checkDrop(PartitionId pId) {
	int32_t backupMargin = PartitionConfig::PT_BACKUP_GAP_FACTOR;
	return (
		getLSN(pId) != 0 && !isOwner(pId, 0, PT_CURRENT_OB) &&
		!isOwner(pId, 0, PT_NEXT_OB) && !isBackup(pId, 0, PT_CURRENT_OB) &&
		!isBackup(pId, 0, PT_NEXT_OB) && !isBackup(pId, 0, PT_CURRENT_GOAL) &&
		checkLimitOwnerBackupLsnGap(getMaxLsn(pId), getLSN(pId), backupMargin));
}

void PartitionTable::set(SubPartition &subPartition) {
	PartitionId pId = subPartition.pId_;
	if (pId == UNDEF_PARTITIONID) {
		return;
	}
	PartitionRole &role = subPartition.role_;
	TableType type = role.getTableType();
	partitions_[pId].roles_[type] = role;
}

void PartitionTable::clearAll(bool) {
	util::LockGuard<util::WriteLock> lock(lock_);
	gossips_.clear();
	for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
		setHeartbeatTimeout(nodeId, UNDEF_TTL);
	}
	for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
		repairLsnList_[pId] = UNDEF_LSN;
		currentActiveOwnerList_[pId] = UNDEF_NODEID;
		gapCountList_[pId] = 0;
	}
}

void PartitionTable::clearRole(PartitionId pId) {
	util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
	partitions_[pId].clear();
	partitions_[pId].partitionInfo_.clear();
	partitions_[pId].setPartitionRoleStatus(0, PartitionTable::PT_NONE);
}

/*!
	@brief Checks cluster status
*/
void PartitionTable::checkRelation(PartitionContext &context, PartitionId pId,
	int32_t longtermTimeout, int32_t slackTime, int32_t heartbeatSlack) {
	try {
		bool &syncFlag = context.isSync_;
		bool promoteFlag = false;

		PartitionInfo *partitionInfo = getPartitionInfo(pId);

		if (!isActive(pId)) {
			bool isFound = false;
			int32_t maxLsnGap = config_.limitMaxLsnGap_;
			LogSequentialNumber maxLsn = getMaxLsn(pId);
			adjustMaxLSN(maxLsn, maxLsnGap);
			NodeId searchedNodeId = UNDEF_NODEID;

			for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
				if (nodes_[nodeId].heartbeatTimeout_ != UNDEF_TTL) {
					if (getLSN(pId, nodeId) >= maxLsn) {
						isFound = true;
						searchedNodeId = nodeId;
						break;
					}
				}
			}

			if (isFound) {
				syncFlag = true;
				setGossip(pId);
				context.isSync_ = true;
				if (!revision_.isInitialRevision()) {
					TRACE_CLUSTER_NORMAL_OPERATION(INFO,
						"[NOTE] Detect active nodes, pId:"
							<< pId
							<< ", node:" << dumpNodeAddress(searchedNodeId)
							<< ",  maxLsn:" << maxLsn
							<< ", lsn:" << getLSN(pId, searchedNodeId));
				}
			}
			return;
		}

		NodeId ownerNodeId = getOwner(pId);

		if (ownerNodeId == UNDEF_NODEID) {
			ownerNodeId = getOwner(pId, PT_NEXT_OB);
		}
		if (ownerNodeId == UNDEF_NODEID) {
			return;
		}

		LogSequentialNumber ownerLsn = getLSN(pId, ownerNodeId);
		LogSequentialNumber backupLsn;
		NodeId backupNodeId;
		util::XArray<NodeId> backups(*context.alloc_);
		getBackup(pId, backups, PT_CURRENT_OB);

		int64_t checkTime = clsMgr_->getMonotonicTime();
		int64_t limitTime = partitionInfo->shortTermTimeout_;

		bool timeoutCheck = (limitTime != UNDEF_TTL && checkTime > limitTime);
		bool checkFailover = checkFailoverTime();

		if (timeoutCheck) {
			TRACE_CLUSTER_NORMAL(
				INFO, "Short term sync timeout check, limitTime:"
						  << getTimeStr(limitTime)
						  << ", checkTime:" << getTimeStr(checkTime));
		}

		if (timeoutCheck && checkFailover) {
			if (getPartitionStatus(pId, ownerNodeId) != PT_ON) {
				TRACE_CLUSTER_NORMAL_OPERATION(INFO,
					"[NOTE] Detect cluster irregular, current owner is not "
					"active, pId:"
						<< pId << ", owner:" << dumpNodeAddress(ownerNodeId)
						<< "(" << ownerNodeId << "), status:"
						<< getPartitionStatus(pId, ownerNodeId));
				syncFlag = true;
			}
		}

		int32_t pos;
		if (checkFailover) {
			for (pos = 0; pos < static_cast<int32_t>(backups.size()); pos++) {
				backupNodeId = backups[pos];
				backupLsn = getLSN(pId, backupNodeId);

				if (timeoutCheck || limitTime == UNDEF_TTL) {
					if (getPartitionStatus(pId, backupNodeId) != PT_ON) {
						TRACE_CLUSTER_NORMAL_OPERATION(INFO,
							"[NOTE] Detect cluster irregular, current backup "
							"is not active, pId:"
								<< pId
								<< ", backup:" << dumpNodeAddress(backupNodeId)
								<< "(" << backupNodeId << "), status:"
								<< getPartitionStatus(pId, backupNodeId));

						setGossip(pId);
						nextBlockQueue_.push(pId, backupNodeId);
						syncFlag = true;
					}
				}

				bool isOwnerChanged =
					(getLsnChangeCount(pId, ownerNodeId) == 0);
				bool isBackupNotChanged =
					(getLsnChangeCount(pId, backupNodeId) ==
						PartitionConfig::PT_LSN_NOTCHANGE_COUNT_MAX);

				TRACE_CLUSTER_NORMAL(INFO,
					"CheckInfo,pId:"
						<< pId << ", owner:" << ownerNodeId << ", ownerLsn:"
						<< ownerLsn << ", backup:" << backupNodeId
						<< ", backupLsn:" << backupLsn << ", gap:"
						<< config_.limitOwnerBackupLsnDetectErrorGap_
						<< ", gapCheck:"
						<< checkLimitOwnerBackupLsnDetectErrorGap(
							   ownerLsn, backupLsn)
						<< ", isOwnerChanged:" << isOwnerChanged
						<< ", ownerChangeCount:"
						<< getLsnChangeCount(pId, ownerNodeId)
						<< ", isBackupNotChanged:" << isBackupNotChanged
						<< ", backupNotChangeCount:"
						<< getLsnChangeCount(pId, backupNodeId));

				if (checkLimitOwnerBackupLsnDetectErrorGap(
						ownerLsn, backupLsn) &&
					isBackupNotChanged) {
					if (setGapCount(pId)) {
						TRACE_CLUSTER_NORMAL_OPERATION(INFO,
							"[NOTE] Detect cluster irregular, current "
							"owner-backup lsn gap is invalid, pId:"
								<< pId
								<< ", owner:" << dumpNodeAddress(ownerNodeId)
								<< "(" << ownerNodeId << "), backup:"
								<< dumpNodeAddress(backupNodeId) << "("
								<< backupNodeId << "), ownerLsn:" << ownerLsn
								<< ", backupLsn:" << backupLsn);
						setGossip(pId);
						syncFlag = true;
						resetGapCount(pId);
					}
					else {
						GS_TRACE_INFO(CLUSTER_OPERATION, 0,
							"pId=" << pId << ", count=" << getGapCount(pId));
					}
				}
				else {
					resetGapCount(pId);
				}
			}
		}

		if (timeoutCheck) {
			if (checkFailover) {
				NodeId currentOwner = getOwner(pId);
				NodeId nextOwner = getOwner(pId, PT_NEXT_OB);
				if (currentOwner != UNDEF_NODEID && currentOwner != nextOwner) {
					if (getPartitionStatus(pId, nextOwner) != PT_ON) {
						TRACE_CLUSTER_NORMAL_OPERATION(INFO,
							"[NOTE] Detect cluster irregular, next backup is "
							"not active, pId:"
								<< pId
								<< ", backup:" << dumpNodeAddress(nextOwner)
								<< "(" << nextOwner << "), status:"
								<< getPartitionStatus(pId, nextOwner));
						nextBlockQueue_.push(pId, nextOwner);
						setGossip(pId);
					}
				}

				backups.clear();
				getBackup(pId, backups, PT_NEXT_OB);
				for (pos = 0; pos < static_cast<int32_t>(backups.size());
					 pos++) {
					backupNodeId = backups[pos];
					if (getPartitionStatus(pId, backupNodeId) != PT_ON) {
						TRACE_CLUSTER_NORMAL_OPERATION(INFO,
							"[NOTE] Detect cluster irregular, next backup is "
							"not active, pId:"
								<< pId
								<< ", backup:" << dumpNodeAddress(backupNodeId)
								<< "(" << backupNodeId << "), status:"
								<< getPartitionStatus(pId, backupNodeId));
						nextBlockQueue_.push(pId, backupNodeId);
						setGossip(pId);
					}
				}
			}
			partitionInfo->shortTermTimeout_ = UNDEF_TTL;
		}

		limitTime = partitionInfo->longTermTimeout_;
		timeoutCheck = (limitTime != UNDEF_TTL && checkTime > limitTime);

		backups.clear();
		getBackup(pId, backups, PT_CURRENT_GOAL);
		PartitionRole crole;
		getPartitionRole(pId, crole, PT_CURRENT_GOAL);

		for (pos = 0; pos < static_cast<int32_t>(backups.size()); pos++) {
			backupNodeId = backups[pos];
			backupLsn = getLSN(pId, backupNodeId);

			if (timeoutCheck) {
				goalBlockQueue_.push(pId, backupNodeId);
				TRACE_CLUSTER_NORMAL_OPERATION(INFO,
					"[NOTE] Detect cluster irregular, long term sync timeout "
					"is occurred, pId:"
						<< pId << ", owner:" << dumpNodeAddress(ownerNodeId)
						<< "(" << ownerNodeId << "), catchup:"
						<< dumpNodeAddress(backupNodeId) << "(" << backupNodeId
						<< "), limitTime:" << getTimeStr(limitTime)
						<< ", checkTime:" << getTimeStr(checkTime));
				setGossip(pId);
			}
			else {
				if (checkPromoteOwnerCatchupLsnGap(ownerLsn, backupLsn) &&
					((ownerLsn > 0 && backupLsn != 0) ||
						(ownerLsn == 0 && backupLsn == 0))) {
					if (checkTime -
							(limitTime - longtermTimeout - heartbeatSlack) <
						slackTime) {
						TRACE_CLUSTER_NORMAL(INFO,
							"Not reached promotion limit time, pId:"
								<< pId << ", owner:" << ownerNodeId
								<< ", ownerLsn:" << ownerLsn << ", catchup:"
								<< backupNodeId << ", catchupLsn:" << backupLsn
								<< ", currentInterval:"
								<< (checkTime - (limitTime - longtermTimeout -
													heartbeatSlack)));
						continue;
					}
					TRACE_CLUSTER_NORMAL_OPERATION(INFO,
						"[INFO] Detect catchup node promotion, pId:"
							<< pId << ", owner:" << dumpNodeAddress(ownerNodeId)
							<< "(" << ownerNodeId << "), ownerLsn:" << ownerLsn
							<< ", catchup:" << dumpNodeAddress(backupNodeId)
							<< "(" << backupNodeId
							<< "), catchupLsn:" << backupLsn);

					nextBlockQueue_.erase(pId, backupNodeId);
					setGossip(pId);
					promoteFlag = true;
				}
			}
		}

		if (timeoutCheck) {
			partitionInfo->longTermTimeout_ = UNDEF_TTL;
		}

		if (syncFlag == true || promoteFlag == true) {
			context.isSync_ = true;
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void PartitionTable::filter(PartitionContext &context,
	util::Set<NodeId> &changedNodeSet, SubPartitionTable &subPartitionTable,
	bool isShortSync) {
	try {
		PartitionId pId;
		util::XArray<PartitionId>::iterator it;

		changedNodeSet.insert(0);

		if (isShortSync) {
			int64_t checkTime = clsMgr_->getMonotonicTime();
			int32_t backupNum = getReplicationNum();
			int32_t liveNum = getLiveNum();

			if (backupNum > liveNum - 1) {
				backupNum = liveNum - 1;
			}
			for (pId = 0; pId < getPartitionNum(); pId++) {
				PartitionRole &currentRole =
					partitions_[pId].roles_[PT_NEXT_OB];
				PartitionRole &nextRole = partitions_[pId].roles_[PT_TMP_OB];
				PartitionRole &targetRole =
					partitions_[pId].roles_[PT_CURRENT_OB];

				bool checkFlag = true;

				int64_t limitTime = getPartitionInfo(pId)->shortTermTimeout_;
				bool timeoutCheck =
					(limitTime == UNDEF_TTL || checkTime > limitTime);

				if (currentRole == nextRole && currentRole == targetRole &&
					timeoutCheck) {
					NodeId owner;
					std::vector<NodeId> backups;
					owner = nextRole.getOwner();
					if (owner == UNDEF_NODEID) continue;
					if (getPartitionStatus(pId, owner) == PT_ON) {
						backups = nextRole.getBackups();
						int32_t activeCount = 0;
						for (size_t pos = 0; pos < backups.size(); pos++) {
							if (getPartitionStatus(pId, backups[pos]) ==
								PT_ON) {
								activeCount++;
							}
							else {
								break;
							}
						}
						if (activeCount ==
								static_cast<int32_t>(backups.size()) &&
							activeCount >= backupNum) {
							checkFlag = false;
						}
					}
				}
				if (checkFlag) {
					nextRole.set(PT_NEXT_OB);
					nextRole.set(revision_);

					PartitionRole &currentRole =
						partitions_[pId].roles_[PT_CURRENT_OB];
					PartitionRole &nextRole =
						partitions_[pId].roles_[PT_TMP_OB];

					NodeId currentOwner;
					currentOwner = currentRole.getOwner();

					bool isDownNextOwner =
						((currentOwner == UNDEF_NODEID) ||
							(currentOwner != UNDEF_NODEID &&
								getHeartbeatTimeout(currentOwner) ==
									UNDEF_TTL) ||
							(currentOwner != UNDEF_NODEID &&
								getPartitionStatus(pId, currentOwner) !=
									PT_ON));

					subPartitionTable.set(this, pId,
						getPartitionInfo(pId)->isActive_, isDownNextOwner,
						nextRole, currentRole.getOwnerAddress());
					nextRole.getTargetNodeSet(changedNodeSet);
					currentRole.getTargetNodeSet(changedNodeSet);

					getPartitionInfo(pId)->shortTermTimeout_ =
						context.shortTermTimeout_;
				}
			}
		}
		else {
			for (pId = 0; pId < getPartitionNum(); pId++) {
				PartitionRole &currentRole =
					partitions_[pId].roles_[PT_NEXT_GOAL];
				PartitionRole &nextRole = partitions_[pId].roles_[PT_TMP_GOAL];
				if (currentRole == nextRole && pId != currentCatchupPId_) {
				}
				else {
					nextRole.set(PT_NEXT_GOAL);
					subPartitionTable.set(this, pId,
						getPartitionInfo(pId)->isActive_, true, nextRole);
					nextRole.getTargetNodeSet(changedNodeSet);
					currentRole.getTargetNodeSet(changedNodeSet);

					getPartitionInfo(pId)->longTermTimeout_ =
						context.longTermTimeout_;
				}
			}
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void PartitionTable::checkAndSetPartitionStatus(PartitionId pId,
	NodeId targetNode, PartitionStatus status, PartitionRoleStatus roleStatus) {
	NodeId currentActiveOwner = currentActiveOwnerList_[pId];
	NodeId currentOwner = partitions_[pId].roles_[PT_CURRENT_OB].getOwner();

	if (status == PT_ON && roleStatus == PT_OWNER) {
		if (targetNode == currentOwner) {
			if (targetNode != currentActiveOwner) {
				UTIL_TRACE_WARNING(CLUSTER_SERVICE,
					"Change, pId:" << pId << ", targetNode:" << targetNode
								   << ", currentOwner:" << currentOwner
								   << ", currentActiveOwner:"
								   << currentActiveOwner);
				currentActiveOwnerList_[pId] = targetNode;
			}
		}
		else {
			UTIL_TRACE_WARNING(CLUSTER_SERVICE,
				"Detect unmatch, pId:" << pId << ", targetNode:" << targetNode
									   << ", currentOwner:" << currentOwner
									   << ", currentActiveOwner:"
									   << currentActiveOwner);
			partitions_[pId].roles_[PT_CURRENT_OB].setOwner(targetNode);
			setGossip(pId);
		}
	}
	setPartitionStatus(pId, status, targetNode);
	setPartitionRoleStatus(pId, roleStatus, targetNode);
}

void PartitionTable::updatePartitionRevision(
	int32_t partitionSequentialNumber) {
	util::LockGuard<util::Mutex> lock(revisionLock_);
	if (partitionSequentialNumber > revision_.sequentialNumber_ &&
		partitionSequentialNumber !=
			PartitionRevision::INITIAL_CLUSTER_REVISION) {
		revision_.sequentialNumber_ = partitionSequentialNumber;
	}
}

bool PartitionTable::PartitionRole::operator==(PartitionRole &role) const {
	NodeId tmpOwnerId = role.getOwner();
	if (ownerNodeId_ == tmpOwnerId) {
		std::vector<NodeId> tmpBackups = role.getBackups();
		if (backups_ == tmpBackups) {
			return true;
		}
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


void PartitionTable::createNextPartition(PartitionContext &context) {
	try {
		PartitionId targetPId;
		size_t pos = 0;
		NodeId nodeId;
		TableType targetType = PT_TMP_OB;

		bool isSucceedCurrentOwner = config_.isSucceedCurrentOwner();

		int32_t nodeNum = getNodeNum();
		uint32_t partitionNum = getPartitionNum();
		int32_t replicationNum = getReplicationNum();
		util::StackAllocator &alloc = *context.alloc_;

		util::XArray<NodeId> tmpOwnerList(alloc);
		tmpOwnerList.assign(static_cast<size_t>(partitionNum), UNDEF_NODEID);
		util::XArray<uint32_t> ownerLoadList(alloc);
		util::XArray<uint32_t> dataLoadList(alloc);
		util::XArray<bool> isActivePartitionList(alloc);

		ownerLoadList.assign(static_cast<size_t>(nodeNum), 0);
		dataLoadList.assign(static_cast<size_t>(nodeNum), 0);
		isActivePartitionList.assign(static_cast<size_t>(partitionNum), true);

		util::XArray<PartitionId> &targetPIdList = context.shortTermPIdList_;
		bool isRepairPartition = context.isRepairPartition_;

		util::XArray<NodeId>::iterator nodeItr;

		util::XArray<NodeId> liveNodeList(alloc);
		getLiveNodeIdList(liveNodeList);
		int32_t liveNodeListSize = static_cast<int32_t>(liveNodeList.size());
		size_t targetPIdSize = targetPIdList.size();

		if (liveNodeListSize == 0) return;
		if (replicationNum > liveNodeListSize - 1) {
			liveNodeListSize = replicationNum;
		}

		PartitionRole *nextRole, *targetRole, *currentRole;
		uint32_t maxOwnerLoad, minOwnerLoad, maxDataLoad, minDataLoad;

		TRACE_CLUSTER_NORMAL(INFO, "IsRepaired:" << isRepairPartition);

		bool isResync = (targetPIdSize == partitionNum);

		{
			for (PartitionId pId = 0; pId < partitionNum; pId++) {
				PartitionRole currentRole;
				getPartitionRole(pId, currentRole);
				NodeId currentOwner = currentRole.getOwner();
				if (currentOwner != UNDEF_NODEID) {
					if (nodes_[currentOwner].heartbeatTimeout_ == UNDEF_TTL) {
						TRACE_CLUSTER_NORMAL(
							WARNING, "Modify down owner, before:{"
										 << currentRole << "}");
						currentRole.clear();
						currentOwner = UNDEF_NODEID;
						setPartitionRole(pId, currentRole);
						TRACE_CLUSTER_NORMAL(WARNING,
							"Modify down owner, after:{" << currentRole << "}");
					}
				}
				if (currentOwner != UNDEF_NODEID) {
					std::vector<NodeId> currentBackups =
						currentRole.getBackups();
					std::vector<NodeId> tmpBackups;
					bool isDown = false;
					for (size_t pos = 0; pos < currentBackups.size(); pos++) {
						if (nodes_[currentBackups[pos]].heartbeatTimeout_ ==
							UNDEF_TTL) {
							isDown = true;
						}
						else {
							tmpBackups.push_back(currentBackups[pos]);
						}
					}
					if (isDown) {
						TRACE_CLUSTER_NORMAL(
							WARNING, "Modify down backups, before:{"
										 << currentRole << "}");
						currentRole.set(currentOwner, tmpBackups);
						setPartitionRole(pId, currentRole);
						TRACE_CLUSTER_NORMAL(
							WARNING, "Modify down backups, after:{"
										 << currentRole << "}");
					}
				}
			}
		}

		pos = 0;

		for (PartitionId pId = 0; pId < partitionNum; pId++) {
			if (isResync && pos < targetPIdSize && targetPIdList[pos] == pId) {
				TRACE_CLUSTER_NORMAL(
					WARNING, "skip pId:" << targetPIdList[pos]);
				continue;
			}
			for (nodeItr = liveNodeList.begin(); nodeItr != liveNodeList.end();
				 nodeItr++) {
				nextRole = &partitions_[pId].roles_[PT_NEXT_OB];
				nodeId = (*nodeItr);
				if (nextRole->isOwner(nodeId)) {
					ownerLoadList[nodeId]++;
					dataLoadList[nodeId]++;
				}
				else if (nextRole->isBackup(nodeId)) {
					dataLoadList[nodeId]++;
				}
			}
		}

		for (nodeId = 0; nodeId < nodeNum; nodeId++) {
			TRACE_CLUSTER_NORMAL(WARNING,
				"Initial load:{nodeId:"
					<< nodeId << ", ownerLoad:" << ownerLoadList[nodeId]
					<< ", dataLoad:" << dataLoadList[nodeId] << "}");
		}

		LogSequentialNumber maxLsn, targetLsn, ownerLsn, startLsn;
		int32_t candOwnerCount, backupCount;
		NodeId candOwner = UNDEF_NODEID, candBackup;
		std::vector<NodeId> emptyNodeList;

		util::XArray<CatchupOwnerNodeInfo>::iterator ownerNodeItr;
		util::XArray<CatchupNodeInfo>::iterator backupNodeItr;
		NodeId currentOwner;

		for (pos = 0; pos < targetPIdSize; pos++) {
			targetPId = targetPIdList[pos];
			nextRole = &partitions_[targetPId].roles_[PT_NEXT_OB];
			currentRole = &partitions_[targetPId].roles_[PT_CURRENT_OB];

			if (isResync) {
				TRACE_CLUSTER_NORMAL(WARNING, "resync");
				NodeId owner = nextRole->getOwner();
				if (owner != UNDEF_NODEID) {
					if (ownerLoadList[owner] > 0) {
						ownerLoadList[owner]--;
					}
					if (dataLoadList[owner] > 0) {
						dataLoadList[owner]--;
					}
					std::vector<NodeId> &backups = nextRole->getBackups();
					for (size_t pos = 0; pos < backups.size(); pos++) {
						NodeId backup = backups[pos];
						if (backup != UNDEF_NODEID) {
							if (dataLoadList[backup] > 0) {
								dataLoadList[backup]--;
							}
						}
					}
				}
			}

			currentOwner = currentRole->getOwner();
			bool isOwnerActive = false;
			uint32_t checkCount = 0;

			if (currentOwner != UNDEF_NODEID) {
				if (nodes_[currentOwner].heartbeatTimeout_ != UNDEF_TTL &&
					getPartitionStatus(targetPId, currentOwner) == PT_ON &&
					getPartitionRoleStatus(targetPId, currentOwner) ==
						PT_OWNER) {
					isOwnerActive = true;
					checkCount++;
					TRACE_CLUSTER_NORMAL(WARNING,
						"Current owner is active, pId:"
							<< targetPId
							<< ", owner:" << dumpNodeAddress(currentOwner)
							<< "(" << currentOwner << ")");
				}
			}
			isActivePartitionList[targetPId] = isOwnerActive;
			maxLsn = getMaxLsn(targetPId);


			candOwnerCount = 0;

			if (!isActive(targetPId) && isRepairPartition) {
				LogSequentialNumber prevMaxLsn = getMaxLsn(targetPId);
				LogSequentialNumber nextMaxLsn = 0;
				for (nodeItr = liveNodeList.begin();
					 nodeItr != liveNodeList.end(); nodeItr++) {
					nodeId = (*nodeItr);
					targetLsn = getLSN(targetPId, nodeId);
					if (targetLsn > nextMaxLsn) {
						nextMaxLsn = targetLsn;
					}
				}

				util::XArray<CatchupOwnerNodeInfo> ownerOrderList(alloc);
				sortOrderList(ownerOrderList, liveNodeList, ownerLoadList,
					dataLoadList, maxOwnerLoad, minOwnerLoad, maxDataLoad,
					minDataLoad, cmpCatchupOwnerNode);

				for (ownerNodeItr = ownerOrderList.begin();
					 ownerNodeItr != ownerOrderList.end(); ownerNodeItr++) {
					nodeId = (*ownerNodeItr).nodeId_;
					targetLsn = getLSN(targetPId, nodeId);

					TRACE_CLUSTER_NORMAL(
						WARNING, "Reparing check, pId:"
									 << targetPId << ", nodeId:" << nodeId
									 << ", targetLsn:" << targetLsn
									 << ", nextMaxLsn:" << nextMaxLsn);

					if (targetLsn == nextMaxLsn) {
						candOwner = nodeId;
						candOwnerCount++;
						maxLsnList_[targetPId] = nextMaxLsn;
						repairLsnList_[targetPId] = nextMaxLsn;

						TRACE_CLUSTER_NORMAL_OPERATION(INFO,
							"[INFO] Repair partition, pId:"
								<< targetPId << ", owner:"
								<< dumpNodeAddress(candOwner) << "("
								<< candOwner << "), prevMaxLsn:" << prevMaxLsn
								<< ", repairedMaxLsn:" << nextMaxLsn);

						isRepaired_ = true;
						nextBlockQueue_.clear(targetPId);

						break;
					}
				}
			}
			else {
				currentOwner = currentRole->getOwner();
				candOwner = currentRole->getOwner();
				if (isSucceedCurrentOwner && context.isDownNode_ &&
					candOwner != UNDEF_NODEID) {
					TRACE_CLUSTER_NORMAL(
						INFO, "Succeeds current owner:" << candOwner);
					candOwnerCount++;
				}
				else {
					bool isFind;
					for (nodeItr = liveNodeList.begin();
						 nodeItr != liveNodeList.end(); nodeItr++) {
						nodeId = (*nodeItr);
						targetLsn = getLSN(targetPId, nodeId);
						TRACE_CLUSTER_NORMAL(
							WARNING, "(Phase1) Check owner, pId:"
										 << targetPId << ", nodeId:" << nodeId
										 << ", lsn:" << targetLsn);
						isFind = false;
						if (!isOwnerActive) {
							if (maxLsn <= targetLsn) {
								isFind = true;
							}
						}
						else {
							if (((getPartitionStatus(targetPId, nodeId) ==
									 PT_ON) &&
									(currentRole->isOwner(nodeId) ||
										currentRole->isBackup(nodeId))) ||
								(!checkLimitOwnerBackupLsnGap(
									maxLsn, targetLsn))) {
								if (currentOwner != UNDEF_NODEID &&
									currentOwner != nodeId) {
									startLsn =
										getStartLSN(targetPId, currentOwner);
									if (targetLsn >= startLsn) {
										isFind = true;
									}
									else {
										isFind = false;
									}
								}
								else {
									isFind = true;
								}
							}
						}
						if (isFind) {
							candOwner = nodeId;
							candOwnerCount++;
						}
					}
				}
			}
			if (candOwnerCount == 1) {
				if (isSucceedCurrentOwner && candOwner == currentOwner) {
				}
				else if (nextBlockQueue_.find(targetPId, candOwner)) {
					TRACE_CLUSTER_NORMAL(
						WARNING, "Target node is blocked, pId:"
									 << targetPId << ", nodeId:" << candOwner);
					continue;
				}
				tmpOwnerList[targetPId] = candOwner;
				ownerLoadList[candOwner]++;
				dataLoadList[candOwner]++;
				setActive(targetPId, true);
				if (replicationNum == 0) {
					targetRole = &partitions_[targetPId].roles_[targetType];
					targetRole->set(revision_, candOwner, emptyNodeList);
					context.setChangePId(MODE_SHORTTERM_SYNC, targetPId);
					continue;
				}
			}
			else {
			}
		}
		for (pos = 0; pos < targetPIdSize; pos++) {
			targetPId = targetPIdList[pos];
			maxLsn = getMaxLsn(targetPId);
			nextRole = &partitions_[targetPId].roles_[PT_NEXT_OB];
			currentRole = &partitions_[targetPId].roles_[PT_CURRENT_OB];
			bool isOwnerActive = isActivePartitionList[targetPId];
			bool isFind;

			currentOwner = currentRole->getOwner();

			if (tmpOwnerList[targetPId] != UNDEF_NODEID) continue;

			util::XArray<CatchupOwnerNodeInfo> ownerOrderList(alloc);
			sortOrderList(ownerOrderList, liveNodeList, ownerLoadList,
				dataLoadList, maxOwnerLoad, minOwnerLoad, maxDataLoad,
				minDataLoad, cmpCatchupOwnerNode);

			for (nodeId = 0; nodeId < nodeNum; nodeId++) {
				TRACE_CLUSTER_NORMAL(WARNING,
					"Initial load1:{nodeId:"
						<< nodeId << ", ownerLoad:" << ownerLoadList[nodeId]
						<< ", dataLoad:" << dataLoadList[nodeId] << "}");
			}

			for (ownerNodeItr = ownerOrderList.begin();
				 ownerNodeItr != ownerOrderList.end(); ownerNodeItr++) {
				isFind = false;
				candOwner = (*ownerNodeItr).nodeId_;
				targetLsn = getLSN(targetPId, candOwner);

				TRACE_CLUSTER_NORMAL(WARNING,
					"(Phase2) Check owner, pId:"
						<< targetPId << ", nodeId:" << candOwner
						<< ", lsn:" << targetLsn << ", maxLsn:" << maxLsn
						<< ", down:" << isOwnerActive);

				if (!isOwnerActive) {
					if (maxLsn <= targetLsn) {
						isFind = true;
					}
				}
				else {
					if (((getPartitionStatus(targetPId, candOwner) == PT_ON) &&
							(currentRole->isOwner(candOwner) ||
								currentRole->isBackup(candOwner))) ||
						(!checkLimitOwnerBackupLsnGap(maxLsn, targetLsn))) {
						if (currentOwner != UNDEF_NODEID &&
							currentOwner != nodeId) {
							startLsn = getStartLSN(targetPId, currentOwner);
							if (targetLsn >= startLsn) {
								isFind = true;
							}
							else {
								isFind = false;
							}
						}
						else {
							isFind = true;
						}
					}
				}
				if (isFind) {
					if (isSucceedCurrentOwner && candOwner == currentOwner) {
					}
					else if (nextBlockQueue_.find(targetPId, candOwner)) {
						TRACE_CLUSTER_NORMAL(WARNING,
							"Target node is blocked, pId:"
								<< targetPId << ", nodeId:" << candOwner);
						continue;
					}

					tmpOwnerList[targetPId] = candOwner;
					ownerLoadList[candOwner]++;
					dataLoadList[candOwner]++;
					setActive(targetPId, true);
					if (replicationNum == 0) {
						targetRole = &partitions_[targetPId].roles_[targetType];
						targetRole->set(revision_, candOwner, emptyNodeList);
						context.setChangePId(MODE_SHORTTERM_SYNC, targetPId);
					}
					break;
				}
				else {
				}
			}

			if (tmpOwnerList[targetPId] == UNDEF_NODEID) {
				targetRole = &partitions_[targetPId].roles_[targetType];
				setActive(targetPId, false);

				targetRole->clear();
				targetRole->set(revision_);
				context.setChangePId(MODE_SHORTTERM_SYNC, targetPId);

				LogSequentialNumber tmpMaxLsn = 0;

				maxLsn = getMaxLsn(targetPId);
				util::XArray<NodeId> notLiveMaxNodeIdList(alloc);

				for (nodeId = 0; nodeId < nodeNum; nodeId++) {
					targetLsn = getLSN(targetPId, nodeId);
					if (maxLsn == targetLsn) {
						notLiveMaxNodeIdList.push_back(nodeId);
					}
					if (checkLiveNode(nodeId) || (nodeId == 0)) {
						if (tmpMaxLsn < targetLsn) {
							tmpMaxLsn = targetLsn;
						}
					}
				}

				TRACE_CLUSTER_NORMAL_OPERATION(
					INFO, "[NOTE] Target node is not service, pId:"
							  << targetPId << " cluster maxLsn:" << maxLsn
							  << ", current maxLsn:" << tmpMaxLsn << ")");

				if (notLiveMaxNodeIdList.size() > 0) {
					TRACE_CLUSTER_NORMAL_OPERATION(
						INFO, "[INFO] Down nodes information, pId:"
								  << targetPId << ", cluster maxLsn:" << maxLsn
								  << ", nodes:"
								  << dumpNodeAddressList(notLiveMaxNodeIdList));
				}
			}
			else {
				setActive(targetPId, true);
			}
		}

		if (replicationNum > 0) {
			for (pos = 0; pos < targetPIdSize; pos++) {
				targetPId = targetPIdList[pos];
				nextRole = &partitions_[targetPId].roles_[targetType];
				currentRole = &partitions_[targetPId].roles_[PT_CURRENT_OB];

				candOwner = tmpOwnerList[targetPId];

				if (candOwner == UNDEF_NODEID)
					continue;  

				util::XArray<CatchupNodeInfo> backupOrderList(alloc);
				std::vector<NodeId> &backups = nextRole->getBackups();

				backups.clear();

				NodeId candCurrentOwner = currentRole->getOwner();

				if (candCurrentOwner != UNDEF_NODEID) {
					ownerLsn = getLSN(targetPId, candOwner);
					startLsn = getStartLSN(targetPId, candCurrentOwner);
				}
				else {
					ownerLsn = getLSN(targetPId, candOwner);
					startLsn = getStartLSN(targetPId, candOwner);
				}

				for (nodeItr = liveNodeList.begin();
					 nodeItr != liveNodeList.end(); nodeItr++) {
					candBackup = (*nodeItr);
					if (candOwner == candBackup) continue;

					targetLsn = getLSN(targetPId, candBackup);

					TRACE_CLUSTER_NORMAL(WARNING,
						"Check backup, pId:"
							<< targetPId << ", owner:" << candOwner
							<< ", owner lsn:" << ownerLsn << " , backup:"
							<< candBackup << ", backup lsn:" << targetLsn
							<< ", current owner:" << candCurrentOwner
							<< ", start current lsn:" << startLsn
							<< ", isActive:" << isActivePartitionList[targetPId]
							<< ", prev catchup pId:" << prevCatchupPId_
							<< ", prev catchup:" << prevCatchupNodeId_);


					bool check1 = (
						(!checkLimitOwnerBackupLsnGap(ownerLsn, targetLsn))
						&&
						((isActivePartitionList[targetPId] &&
							 targetLsn >= startLsn)
							|| (!isActivePartitionList[targetPId] &&
								   targetLsn >= startLsn)));

					bool check2 = (
						((prevCatchupPId_ == targetPId) &&
							(prevCatchupNodeId_ == candBackup))
						&& ((ownerLsn == 0 && targetLsn == 0) ||
							   (ownerLsn > 0 && targetLsn > 0))
						&&
						!checkLimitOwnerBackupLsnGap(ownerLsn, targetLsn,
							PartitionConfig::PT_BACKUP_GAP_FACTOR));

					if (check1 || check2) {
						if (nextBlockQueue_.find(targetPId, candBackup)) {
							TRACE_CLUSTER_NORMAL(WARNING,
								"Target node is blocked, pId:"
									<< targetPId << ", nodeId:" << candBackup);
							continue;
						}
						TRACE_CLUSTER_NORMAL(
							INFO, "Cand backup, pId:"
									  << targetPId << ", backup:" << candBackup
									  << ", load:" << dataLoadList[candBackup]);

						CatchupNodeInfo catchupInfo(candBackup,
							dataLoadList[candBackup], dataLoadList[candBackup]);
						backupOrderList.push_back(catchupInfo);
					}
				}

				if (backupOrderList.size() != 0) {
					std::sort(backupOrderList.begin(), backupOrderList.end(),
						cmpCatchupNode);
					backupCount = 0;
					for (backupNodeItr = backupOrderList.begin();
						 backupNodeItr != backupOrderList.end();
						 backupNodeItr++) {
						candBackup = (*backupNodeItr).nodeId_;

						TRACE_CLUSTER_NORMAL(
							INFO, "Next backup informations, no:"
									  << backupCount << ", pId:" << targetPId
									  << ", backup:" << candBackup
									  << ", load:" << dataLoadList[candBackup]);

						backups.push_back(candBackup);
						dataLoadList[candBackup]++;
						backupCount++;
						if (backupCount >= replicationNum) {
							break;
						}
					}
				}
				targetRole = &partitions_[targetPId].roles_[targetType];
				targetRole->set(revision_, candOwner);
				context.setChangePId(MODE_SHORTTERM_SYNC, targetPId);
			}
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void PartitionTable::createCatchupPartition(
	PartitionContext &context, bool isSkip) {
	try {
		if (isSkip) {
			if (prevCatchupPId_ != UNDEF_PARTITIONID) {
				context.setChangePId(MODE_LONGTERM_SYNC, prevCatchupPId_);
			}
			prevCatchupPId_ = UNDEF_PARTITIONID;
			prevCatchupNodeId_ = UNDEF_NODEID;

			TRACE_CLUSTER_NORMAL(
				WARNING, "Update catchup information, pId:"
							 << prevCatchupPId_
							 << ", goalNodeId:" << prevCatchupNodeId_);
			return;
		}
		PartitionId pId, targetPId;
		int32_t partitionPos = 0, nodePos = 0;
		NodeId nodeId;

		int32_t nodeNum = getNodeNum();
		uint32_t partitionNum = getPartitionNum();
		int32_t targetBackupNum = getReplicationNum();
		util::StackAllocator &alloc = *context.alloc_;

		util::XArray<uint32_t> ownerLoadList(alloc);
		util::XArray<uint32_t> dataLoadList(alloc);
		util::XArray<NodeId> liveNodeList(alloc);
		util::XArray<NodeId>::iterator nodeItr;
		getLiveNodeIdList(liveNodeList);

		ownerLoadList.assign(static_cast<size_t>(nodeNum), 0);
		dataLoadList.assign(static_cast<size_t>(nodeNum), 0);

		uint32_t minOwnerLoad, maxOwnerLoad, minDataLoad, maxDataLoad;

		int32_t backupNum;
		NodeId owner, target;
		PartitionRole *nextRole, *nextCatchupRole;
		bool isReplicaLoss = false;
		TableType targetNextType = PT_TMP_OB;
		TableType targetType = PT_TMP_GOAL;

		TRACE_CLUSTER_NORMAL(
			INFO, "Create catchup partition is started, prev catchup pId:"
					  << prevCatchupPId_);

		TRACE_CLUSTER_NORMAL(
			WARNING, dumpPartitions(alloc, PartitionTable::PT_NEXT_OB));

		TRACE_CLUSTER_NORMAL(
			WARNING, dumpPartitions(alloc, PartitionTable::PT_TMP_OB));

		util::XArray<ReplicaInfo> replicaLossList(alloc);
		for (pId = 0; pId < partitionNum; pId++) {
			nextRole = &partitions_[pId].roles_[targetNextType];
			nextCatchupRole = &partitions_[pId].roles_[PT_TMP_GOAL];
			nextCatchupRole->clear();

			if (!isActive(pId)) continue;
			backupNum = nextRole->getBackupSize();
			if (backupNum < targetBackupNum) {
				ReplicaInfo repInfo(pId, backupNum);
				replicaLossList.push_back(repInfo);
			}
		}
		if (replicaLossList.size() > 0) {
			isReplicaLoss = true;
		}

		for (pId = 0; pId < partitionNum; pId++) {
			for (nodeItr = liveNodeList.begin(); nodeItr != liveNodeList.end();
				 nodeItr++) {
				nextRole = &partitions_[pId].roles_[targetNextType];
				nodeId = (*nodeItr);
				if (nextRole->isOwner(nodeId)) {
					ownerLoadList[nodeId]++;
					dataLoadList[nodeId]++;
				}
				else if (nextRole->isBackup(nodeId)) {
					dataLoadList[nodeId]++;
				}
			}
		}

		util::XArray<CatchupNodeInfo> catchupOrderList(alloc);
		sortOrderList(catchupOrderList, liveNodeList, ownerLoadList,
			dataLoadList, maxOwnerLoad, minOwnerLoad, maxDataLoad, minDataLoad,
			cmpCatchupNode, true);

		TRACE_CLUSTER_NORMAL(WARNING,
			"Pre-calc catchup info, replicaLoss:"
				<< isReplicaLoss << ", minOwnerLoad:" << minOwnerLoad
				<< ", maxOwnerLoad:" << maxOwnerLoad << ", minDataLoad:"
				<< minDataLoad << ", maxDataLoad:" << maxDataLoad);

		if (!isReplicaLoss && isBalance(minDataLoad, maxDataLoad, false)) {
			TRACE_CLUSTER_NORMAL(WARNING, "Checking replica loss is ok.");

			if (!isBalance(minOwnerLoad, maxOwnerLoad, true)) {
				TRACE_CLUSTER_NORMAL(
					WARNING, "Checking owner balancing is not ok.");

				for (pId = 0; pId < partitionNum; pId++) {
					bool hasMin = false;
					bool hasMax = false;
					target = UNDEF_NODEID;
					nextRole = &partitions_[pId].roles_[targetNextType];
					owner = nextRole->getOwner();
					if (owner == UNDEF_NODEID) continue;

					for (nodeItr = liveNodeList.begin();
						 nodeItr != liveNodeList.end(); nodeItr++) {
						nodeId = *nodeItr;

						if (!goalBlockQueue_.find(pId, nodeId)) {
							bool isOwner = nextRole->isOwner(nodeId);
							bool isBackup = nextRole->isBackup(nodeId);

							if (ownerLoadList[nodeId] == maxOwnerLoad &&
								(isOwner || isBackup))
								hasMax = true;

							if (ownerLoadList[nodeId] == minOwnerLoad &&
								!isOwner && !isBackup) {
								hasMin = true;
								target = nodeId;
							}

							if (hasMax && hasMin) {
								nextCatchupRole =
									&partitions_[pId].roles_[PT_NEXT_GOAL];
								std::vector<NodeId> &catchupList =
									nextRole->getBackups();
								catchupList.clear();
								catchupList.push_back(target);
								nextRole->set(revision_, owner);

								if (prevCatchupPId_ != UNDEF_PARTITIONID &&
									prevCatchupPId_ != pId) {
									context.setChangePId(
										MODE_LONGTERM_SYNC, prevCatchupPId_);
									goalBlockQueue_.push(
										prevCatchupPId_, prevCatchupNodeId_);
									TRACE_CLUSTER_NORMAL(WARNING,
										"Update Changed catchup information, "
										"pId:"
											<< prevCatchupPId_
											<< ", catchupNodeId:"
											<< prevCatchupNodeId_);
								}
								prevCatchupPId_ = pId;
								prevCatchupNodeId_ = nodeId;
								TRACE_CLUSTER_NORMAL(WARNING,
									"Update New catchup information, pId:"
										<< prevCatchupPId_ << ", catchupNodeId:"
										<< prevCatchupNodeId_);

								context.setChangePId(MODE_LONGTERM_SYNC, pId);
								return;
							}
						}
					}
				}
			}
			else {
				TRACE_CLUSTER_NORMAL(WARNING, "All balancing checks is ok.");
				if (prevCatchupPId_ != UNDEF_PARTITIONID) {
					context.setChangePId(MODE_LONGTERM_SYNC, prevCatchupPId_);
					goalBlockQueue_.push(prevCatchupPId_, prevCatchupNodeId_);
					TRACE_CLUSTER_NORMAL(
						WARNING, "Update catchup information, pId:"
									 << prevCatchupPId_
									 << ", goalNodeId:" << prevCatchupNodeId_);
				}
				prevCatchupPId_ = UNDEF_PARTITIONID;
				prevCatchupNodeId_ = UNDEF_NODEID;
				return;
			}
		}


		PartitionId firstPId;
		util::XArray<PartitionId> searchPIdList(alloc);

		if (isReplicaLoss) {
			std::sort(
				replicaLossList.begin(), replicaLossList.end(), cmpReplica);
			for (partitionPos = 0;
				 partitionPos < static_cast<int32_t>(replicaLossList.size());
				 partitionPos++) {
				if (!isActive(replicaLossList[partitionPos].pId_)) continue;
				searchPIdList.push_back(replicaLossList[partitionPos].pId_);
			}

			if (searchPIdList.size() > 0) {
				firstPId = *searchPIdList.begin();
				if (firstPId == prevCatchupPId_) {
					searchPIdList.erase(searchPIdList.begin());
					searchPIdList.push_back(firstPId);
				}
			}
		}
		else {
			if (prevCatchupPId_ == partitionNum - 1) {
				firstPId = 0;
			}
			else {
				firstPId = prevCatchupPId_ + 1;
			}
			for (PartitionId pId = firstPId; pId < partitionNum; pId++) {
				if (!isActive(pId)) continue;
				searchPIdList.push_back(pId);
			}
			for (PartitionId pId = 0; pId < firstPId; pId++) {
				if (!isActive(pId)) continue;
				searchPIdList.push_back(pId);
			}
		}

		for (partitionPos = 0;
			 partitionPos < static_cast<int32_t>(searchPIdList.size());
			 partitionPos++) {
			targetPId = searchPIdList[partitionPos];
			nextRole = &partitions_[targetPId].roles_[targetNextType];

			for (nodePos = 0;
				 nodePos < static_cast<int32_t>(catchupOrderList.size());
				 nodePos++) {
				target = catchupOrderList[nodePos].nodeId_;
				if (target == UNDEF_NODEID) continue;  

				TRACE_CLUSTER_NORMAL(WARNING, "Check catchup informations, pId:"
												  << targetPId
												  << ", target:" << target);

				owner = nextRole->getOwner();
				if (owner >= 0 && owner != target &&
					!nextRole->isBackup(target)) {
					if (!goalBlockQueue_.find(targetPId, target)) {
						if (dataLoadList[target] == minDataLoad ||
							isReplicaLoss) {
							nextCatchupRole =
								&partitions_[targetPId].roles_[targetType];

							std::vector<NodeId> &catchupList =
								nextCatchupRole->getBackups();
							catchupList.clear();
							catchupList.push_back(target);

							nextCatchupRole->set(revision_, owner);

							if (prevCatchupPId_ != UNDEF_PARTITIONID &&
								prevCatchupPId_ != targetPId) {
								PartitionRole *tmpRole =
									&partitions_[prevCatchupPId_]
										 .roles_[targetType];
								tmpRole->set(revision_, UNDEF_NODEID);
								context.setChangePId(
									MODE_LONGTERM_SYNC, prevCatchupPId_);
								goalBlockQueue_.push(
									prevCatchupPId_, prevCatchupNodeId_);
								TRACE_CLUSTER_NORMAL(WARNING,
									"Update catchup information, pId:"
										<< prevCatchupPId_ << ", goalNodeId:"
										<< prevCatchupNodeId_);
							}
							TRACE_CLUSTER_NORMAL(WARNING,
								"Target node is next catchup partition, pId:"
									<< targetPId << ", owner:" << owner
									<< ", target:" << target);

							prevCatchupPId_ = targetPId;
							prevCatchupNodeId_ = target;
							context.setChangePId(MODE_LONGTERM_SYNC, targetPId);
							return;
						}
						else {
							TRACE_CLUSTER_NORMAL(WARNING,
								"Target node cannot move, pId:"
									<< targetPId << ", nodeId:" << target);
						}
					}
					else {
						TRACE_CLUSTER_NORMAL(
							WARNING, "Target node is blocked, pId:"
										 << targetPId << ", nodeId:" << target);
					}
				}
				else {
					TRACE_CLUSTER_NORMAL(
						DEBUG, "Target node is not candidated, pId:"
								   << targetPId << ", nodeId:" << target);
				}
			}
		}
		if (context.changedLongTermPIdList_.size() == 0) {
			if (prevCatchupPId_ != UNDEF_PARTITIONID) {
				context.setChangePId(MODE_LONGTERM_SYNC, prevCatchupPId_);
				goalBlockQueue_.push(prevCatchupPId_, prevCatchupNodeId_);
				TRACE_CLUSTER_NORMAL(
					WARNING, "Update catchup information, pId:"
								 << prevCatchupPId_
								 << ", goalNodeId:" << prevCatchupNodeId_);
			}
			prevCatchupPId_ = UNDEF_PARTITIONID;
			prevCatchupNodeId_ = UNDEF_NODEID;
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void PartitionTable::adjustMaxLSN(
	LogSequentialNumber &maxLSN, int32_t maxLsnGap) {
	uint64_t targetMaxLsnGap = static_cast<uint64_t>(maxLsnGap);
	if (maxLsnGap != 0) {
		if (maxLSN < targetMaxLsnGap) {
			maxLSN = 1;  
		}
		else {
			maxLSN = maxLSN - targetMaxLsnGap;
		}
	}
}

void PartitionTable::Partition::checkAndResize(NodeId nodeId) {
	if (nodeId >= masterInfoSize_) {
		if (nodeId >= masterInfoLimitSize_) {
			GS_THROW_USER_ERROR(
				GS_ERROR_TXN_PARTITION_STATE_INVALID, "max nodeId");
			return;
		}
		if ((nodeId * 2) < masterInfoLimitSize_) {
			masterInfoSize_ = nodeId * 2;
		}
		else {
			masterInfoSize_ = masterInfoLimitSize_;
		}
		masterPartitionInfos_.resize(masterInfoSize_);
	}
}

void PartitionTable::Partition::check(NodeId nodeId) {
	if (nodeId >= masterInfoSize_) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_PARTITION_STATE_INVALID, "max nodeId");
		return;
	}
}

EventMonotonicTime PartitionTable::getMonotonicTime() {
	return clsMgr_->getMonotonicTime();
}

void PartitionTable::setFailoverTime() {
	prevFailoverTime_ = clsMgr_->getMonotonicTime();
}

bool PartitionTable::checkFailoverTime() {
	int64_t currentTime = clsMgr_->getMonotonicTime();
	return ((currentTime - prevFailoverTime_) >
			PartitionConfig::PT_FAILOVER_LIMIT_TIME);
}

std::ostream &operator<<(std::ostream &ss, SubPartition &subPartition) {
	ss << subPartition.dump();
	return ss;
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
	std::string backupStr;
	std::string ownerStr("O");
	std::string noneStr("-");

	switch (type) {
	case PT_NEXT_OB:
		partitionType = "NEXT_TABLE";
		backupStr = "B";
		break;
	case PT_CURRENT_OB:
		partitionType = "CURRENT_TABLE";
		backupStr = "B";
		break;
	case PT_NEXT_GOAL:
		partitionType = "NEXT_GOAL_TABLE";
		backupStr = "G";
		break;
	case PT_CURRENT_GOAL:
		partitionType = "CURERNT_GOAL_TABLE";
		backupStr = "G";
		break;
	case PT_TMP_OB:
		partitionType = "TMP_NEXT_TABLE";
		backupStr = "B";
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
		PartitionRole *role = &partitions_[pId].roles_[type];
		int32_t psum = 0;
		int32_t currentPos = 0;
		ss << "[P" << std::setfill('0') << std::setw(partitionDigit_) << pId
		   << "]";
		ss << "[" << dumpPartitionStatusEx(getPartitionStatus(pId)) << "]";

		std::vector<NodeId> currentBackupList;
		NodeId curretOwnerNodeId = role->getOwner();
		PartitionRevision &revision = role->getRevision();
		std::vector<NodeId> &backups = role->getBackups();
		int32_t backupNum = static_cast<int32_t>(backups.size());

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
		if (type == PT_NEXT_GOAL && countList[pos] > 1) {
		}
		if (pos != countList.size() - 1) ss << ",";
	}
	ss << terminateCode;
	for (pos = 0; pos < ownerCountList.size(); pos++) {
		ss << ownerCountList[pos];
		if (pos != ownerCountList.size() - 1) ss << ",";
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

std::string PartitionRole::dump() {
	util::NormalOStringStream ss1;
	util::NormalOStringStream ss2;

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

	ss1 << "{pId:" << pId_ << ", ownerAddress:" << ownerAddress_.toString()
		<< "(" << ownerNodeId_ << ")"
		<< ", backupAddressList:" << ss2.str()
		<< ", ptRev:" << revision_.toString()
		<< ", type:" << dumpPartitionTableType(type_) << "}";

	return ss1.str();
}

std::string PartitionRole::dumpIds() {
	util::NormalOStringStream ss1;
	util::NormalOStringStream ss2;

	ss2 << "{";
	for (size_t pos = 0; pos < backups_.size(); pos++) {
		ss2 << backups_[pos];
		if (pos != backups_.size() - 1) {
			ss2 << ",";
		}
	}
	ss2 << "}";

	ss1 << "{pId:" << pId_ << ", owner:" << ownerNodeId_
		<< ", backups:" << ss2.str() << ", ptRev:" << revision_.toString()
		<< ", type:" << dumpPartitionTableType(type_) << "}";

	return ss1.str();
}

std::string PartitionRole::dump(PartitionTable *pt, ServiceType serviceType) {
	util::NormalOStringStream ss1;
	util::NormalOStringStream ss2;

	ss2 << "[";
	for (size_t pos = 0; pos < backups_.size(); pos++) {
		NodeAddress &address =
			pt->getNodeInfo(backups_[pos]).getNodeAddress(serviceType);
		ss2 << address.toString();
		if (pos != backups_.size() - 1) {
			ss2 << ",";
		}
	}
	ss2 << "]";

	NodeAddress ownerNodeAddress;
	if (ownerNodeId_ == UNDEF_NODEID) {
		ss1 << "{pId:" << pId_ << ", ownerAddress:UNDEF"
			<< ", backupAddressList:" << ss2.str()
			<< ", revision:" << revision_.toString()
			<< ", type:" << dumpPartitionTableType(type_) << "}";
	}
	else {
		NodeAddress &ownerNodeAddress =
			pt->getNodeInfo(ownerNodeId_).getNodeAddress(serviceType);
		ss1 << "{pId:" << pId_
			<< ", ownerAddress:" << ownerNodeAddress.toString()
			<< ", backupAddressList:" << ss2.str()
			<< ", ptRev:" << revision_.toString()
			<< ", type:" << dumpPartitionTableType(type_) << "}";
	}
	return ss1.str();
}