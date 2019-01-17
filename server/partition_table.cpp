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
#include "util/trace.h"
#include "config_table.h"
#include "picojson.h"


UTIL_TRACER_DECLARE(CLUSTER_SERVICE);
UTIL_TRACER_DECLARE(CLUSTER_OPERATION);
UTIL_TRACER_DECLARE(CLUSTER_DUMP);


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
	currentCatchupPId_(UNDEF_PARTITIONID),
	prevCatchupNodeId_(UNDEF_NODEID),
	partitions_(NULL),
	nodes_(NULL),
	loadInfo_(alloc_, MAX_NODE_NUM),
	failCatchup_(false),
	isRepaired_(false),
	currentRevisionNo_(0)
{
	try {
		Partition dummyPartition;
		uint32_t partitionNum = config_.partitionNum_;
		partitionLockList_ = ALLOC_NEW(alloc_) util::RWLock[partitionNum];
		partitions_ = ALLOC_NEW(alloc_) Partition[partitionNum];
		currentPartitions_ = ALLOC_NEW(alloc_) CurrentPartitions [partitionNum];
		NodeId maxNodeNum = config_.limitClusterNodeNum_;
		PartitionGroupConfig pgConfig(configTable);
		for (PartitionId pId = 0; pId < partitionNum; pId++) {
			partitions_[pId].init(
				alloc_, pId, pgConfig.getPartitionGroupId(pId), maxNodeNum);
			currentPartitions_[pId].init();
		}
		nodes_ = ALLOC_NEW(alloc_) NodeInfo[maxNodeNum];
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
		assignMode_(configTable.get<int32_t>(CONFIG_TABLE_CS_PARTITION_ASSIGN_MODE))
{
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
		if (currentPartitions_) {
			ALLOC_DELETE(alloc_, &currentPartitions_[pId]);
	}
	}
	for (NodeId nodeId = 0; nodeId < config_.getLimitClusterNodeNum();nodeId++) {
		if (nodes_) {
			ALLOC_DELETE(alloc_, &nodes_[nodeId]);
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

void PartitionTable::clearCatchupRole(
	PartitionId pId, TableType type) {
	util::LockGuard<util::ReadLock> currentLock(partitionLockList_[pId]);
	return (partitions_[pId].roles_[type].clearCatchup());
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

void PartitionTable::setChangePartition(PartitionId pId) {
	util::LockGuard<util::WriteLock> lock(lock_);
	targetChangePartitonList_.insert(pId);
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
}

bool PartitionTable::findBlockQueue(
	PartitionId pId, ChangeTableType tableType, NodeId nodeId) {
	if (pId >= getPartitionNum() || nodeId >= getNodeNum()) {
		GS_THROW_USER_ERROR(GS_ERROR_PT_SET_INVALID_NODE_ID, "");
	}
	util::LockGuard<util::WriteLock> lock(lock_);
	if (tableType == PT_CHANGE_NEXT_TABLE) {
		return nextBlockQueue_.find(pId, nodeId);
	}
	else if (tableType == PT_CHANGE_GOAL_TABLE) {
		return goalBlockQueue_.find(pId, nodeId);
	}
	else {
		return false;
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
	default: {
		GS_THROW_USER_ERROR(GS_ERROR_PT_INVALID_PARTITION_ROLE_TYPE, "");
	}
	}
	util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
	partitions_[pId].roles_[type] = partitions_[pId].roles_[sourceType];
	partitions_[pId].roles_[type].set(type);
	if (currentPartitions_[pId].getPartitionRoleStatus(0) == PT_NONE) {
		currentPartitions_[pId].setOtherStatus(SELF_NODEID, PT_OTHER_NORMAL);
	}

	if (type == PT_CURRENT_OB) {
		PartitionRoleStatus nextStatus =
			partitions_[pId].roles_[type].getPartitionRoleStatus();
		currentPartitions_[pId].setPartitionRoleStatus(SELF_NODEID, nextStatus);
		PartitionRevisionNo nextRevision = partitions_[pId].roles_[type].getRevisionNo();
		currentPartitions_[pId].setPartitionRevision(SELF_NODEID, nextRevision);
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
	targetChangePartitonList_.clear();
	for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
		setHeartbeatTimeout(nodeId, UNDEF_TTL);
	}
}

void PartitionTable::clearRole(PartitionId pId) {
	util::LockGuard<util::WriteLock> currentLock(partitionLockList_[pId]);
	partitions_[pId].clear();
	partitions_[pId].partitionInfo_.clear();
}

static const int32_t PT_CHECK_NORMAL = 0;
static const int32_t PT_CHECK_ACTIVATE_PARTITON = 1;
static const int32_t PT_CHECK_STATUS_UNMATCH = 2;
static const int32_t PT_CHECK_ROLE_UNMATCH = 3;
static const int32_t PT_CHECK_REVISION_UNMATCH = 4;
static const int32_t PT_CHECK_ERROR = 5;
static const int32_t PT_CHECK_CATCHUP_PROMOTION = 6;
static const int32_t PT_CHECK_BALANCE = 7;

std::string dumpPartitionCheckStatus(int32_t reason) {
	switch (reason) {
	case PT_CHECK_NORMAL:
		return "CHANGE CLUSTER";
	case PT_CHECK_ACTIVATE_PARTITON:
		return "ACTIVATE PARTITION";
	case PT_CHECK_STATUS_UNMATCH:
		return "STATUS UNMATCH";
	case PT_CHECK_ROLE_UNMATCH:
		return "ROLE UNMATCH";
	case PT_CHECK_REVISION_UNMATCH:
		return "REVISION UNMATCH";
	case PT_CHECK_CATCHUP_PROMOTION:
		return "CATCHUP PROMOTION";
	case PT_CHECK_ERROR:
		return "DETECT_ERROR";
	case PT_CHECK_BALANCE:
		return "RESOLVE BALANCE";
	default:
		return "";
	}
}
/*!
	@brief Checks cluster status
*/
void PartitionTable::check(PartitionContext &context, bool balanceCheck,
		bool dropCheck, DropPartitionNodeInfo &dropPartitionNodeInfo) {
	try {
		int32_t detectedReason = PT_CHECK_NORMAL;
		bool dumpPartition = false;
		bool isError = false;
		CheckedPartitionStat balanceStatus = PT_NORMAL;
		if (balanceCheck) {
			resetBlockQueue();
			balanceStatus = checkPartitionStat();
		}

		int32_t normalCounter = 0;
		bool hasCatchup = false;
		bool isPartial = false;
		for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
			bool &updatePartitionFlag = context.needUpdatePartition_;
			bool &withRescue = context.withRescue_;
			NodeId ownerNodeId = getOwner(pId, PT_NEXT_OB);
			if (ownerNodeId == UNDEF_NODEID) {
				isPartial = true;
				continue;
			}
			LogSequentialNumber ownerLsn;
			NodeId backupNodeId = 0, catchupNodeId;
			util::XArray<NodeId> backups(*context.alloc_);
			getBackup(pId, backups, PT_NEXT_OB);
			util::XArray<NodeId> catchups(*context.alloc_);
			getCatchup(pId, catchups, PT_NEXT_OB);

			bool isNotShortTermSync = checkShorTermSyncTimeout(pId, context.currentTime_);

			bool partitionCheck = true;
			LogSequentialNumber catchupLsn;
//			bool replicaLossCheck = false;

			PartitionRole nextRole;
			getPartitionRole(pId, nextRole, PT_NEXT_OB);
			PartitionRevisionNo nextRevisionNo = nextRole.getRevisionNo();
			bool isSameOwner = (getOwner(pId) == getOwner(pId, PT_NEXT_OB));
			int32_t backupNum = getReplicationNum();
			int32_t liveNodeNum = getLiveNum();
			if (backupNum > liveNodeNum - 1) {
				backupNum = liveNodeNum - 1;
			}
			if (getPartitionRoleStatus(pId, ownerNodeId) != PT_OWNER) {
				context.errorReason_ = OWNER_ROLE_UNMATCH;
				partitionCheck = false;
				withRescue = true;
				context.errorPId_ = pId;
				if (isNotShortTermSync)
				GS_TRACE_DEBUG(CLUSTER_OPERATION, GS_TRACE_CS_NORMAL_OPERATION,
					"OWNER_ROLE_UNMATCH" << ", pId="
							<< pId << ", owner=" << dumpNodeAddress(ownerNodeId) << ", role="
							<< getPartitionRoleStatus(pId, ownerNodeId));

				if (!isSameOwner) {
					detectedReason = PT_CHECK_STATUS_UNMATCH;
				}
			}
			if (getPartitionStatus(pId, ownerNodeId) != PT_ON) {
				context.errorReason_ = OWNER_STATUS_UNMATCH;
				partitionCheck = false;
				withRescue = true;
				context.errorPId_ = pId;
				if (isNotShortTermSync)
				GS_TRACE_DEBUG(CLUSTER_OPERATION, GS_TRACE_CS_NORMAL_OPERATION,
					"OWNER_STATUS_UNMATCH" << ", pId=" << pId
					<< ", owner=" << dumpNodeAddress(ownerNodeId) << ", status="
								<< getPartitionStatus(pId, ownerNodeId));

				if (!isSameOwner) {
					detectedReason = PT_CHECK_ROLE_UNMATCH;
			}
		}
			if (getPartitionRevision(pId, ownerNodeId) != nextRevisionNo) {
				context.errorReason_ = OWNER_REVISION_UNMATCH;
				partitionCheck = false;
				withRescue = true;
				context.errorPId_ = pId;
				if (isNotShortTermSync)
				GS_TRACE_INFO(CLUSTER_OPERATION, GS_TRACE_CS_NORMAL_OPERATION,
					"OWNER_REVISION_UNMATCH" << ", pId=" << pId
					<< ", owner=" << dumpNodeAddress(ownerNodeId) << ", expectedRevision="
							<< nextRevisionNo << ", actualRevision=" << getPartitionRevision(pId, ownerNodeId));
				if (!isSameOwner) {
					detectedReason = PT_CHECK_REVISION_UNMATCH;
				}
			}
			if (getErrorStatus(pId, ownerNodeId) < 0) {
				context.errorReason_ = OWNER_RECIEVE_ERROR;
				partitionCheck = false;
				withRescue = true;
				context.errorPId_ = pId;
				isNotShortTermSync = true;
				detectedReason = PT_CHECK_ERROR;
				isError = true;
				GS_TRACE_INFO(CLUSTER_OPERATION, GS_TRACE_CS_NORMAL_OPERATION,
					"OWNER_RECIEVE_ERROR" << ", pId=" << pId
					<< ", owner=" << dumpNodeAddress(ownerNodeId) 
							<< ", error=" << getErrorStatus(pId, ownerNodeId));


			}

			for (size_t i = 0; i < static_cast<size_t>(backups.size()); i++) {
//				bool currentCheck = false;
				backupNodeId = backups[i];
				if (getPartitionRoleStatus(pId, backupNodeId) != PT_BACKUP) {
					context.errorReason_ = BACKUP_ROLE_UNMATCH;
					partitionCheck = false;
					withRescue = true;
					context.errorPId_ = pId;
					detectedReason = PT_CHECK_ROLE_UNMATCH;
					if (isNotShortTermSync)
					GS_TRACE_DEBUG(CLUSTER_OPERATION, GS_TRACE_CS_NORMAL_OPERATION,
						"BACKUP_ROLE_UNMATCH" << ", pId=" << pId <<
						", backup=" << dumpNodeAddress(backupNodeId) 
								<< ", role=" << dumpPartitionRoleStatus(getPartitionRoleStatus(pId, backupNodeId)) << " NOT BACKUP");

				}
					if (getPartitionStatus(pId, backupNodeId) != PT_ON) {
						context.errorReason_ = BACKUP_STATUS_UNMATCH;

					partitionCheck = false;
					withRescue = true;
					context.errorPId_ = pId;
					detectedReason = PT_CHECK_STATUS_UNMATCH;
					if (isNotShortTermSync)
					GS_TRACE_DEBUG(CLUSTER_OPERATION, GS_TRACE_CS_NORMAL_OPERATION,
						"BACKUP_STATUS_UNMATCH" << ", pId=" << pId <<
						", backup=" << dumpNodeAddress(backupNodeId) 
								<< ", status=" << dumpPartitionStatus(getPartitionStatus(pId, backupNodeId)) << " NOT ON");

				}
				if (getPartitionRevision(pId, backupNodeId) != nextRevisionNo) {
					context.errorReason_ = BACKUP_REIVION_UNMATCH;
					partitionCheck = false;
					withRescue = true;
					context.errorPId_ = pId;
					detectedReason = PT_CHECK_REVISION_UNMATCH;
					if (isNotShortTermSync)
					GS_TRACE_INFO(CLUSTER_OPERATION, GS_TRACE_CS_NORMAL_OPERATION,
						"BACKUP_REIVION_UNMATCH" << ", pId=" << pId <<
						", backup=" << dumpNodeAddress(backupNodeId) 
								<< ", expectedRevision=" << nextRevisionNo 
								<< ", actualRevision=" << getPartitionRevision(pId, backupNodeId));

					}
				if (getErrorStatus(pId, backupNodeId) < 0) {
					context.errorReason_ = BACKUP_RECIEVE_ERROR;
					partitionCheck = false;
					withRescue = true;
					context.errorPId_ = pId;
					isNotShortTermSync = true;
					detectedReason = PT_CHECK_ERROR;
					isError = true;
					GS_TRACE_INFO(CLUSTER_OPERATION, GS_TRACE_CS_NORMAL_OPERATION,
						"BACKUP_RECIEVE_ERROR" << ", pId=" << pId <<
						 ", backup=" << dumpNodeAddress(backupNodeId) 
								<< ", error=" << getErrorStatus(pId, backupNodeId));

				}
			}

			ownerLsn = getLSN(pId, ownerNodeId);
			bool checkedRevision = true;
			for (size_t i = 0; i < static_cast<size_t>(catchups.size()); i++) {
				hasCatchup = true;
				catchupNodeId = catchups[i];
				catchupLsn = getLSN(pId, catchupNodeId);
				if (getErrorStatus(pId, catchupNodeId) < 0) {
					context.errorReason_ = CATCHUP_RECIEVE_ERROR;
					partitionCheck = false;
					isNotShortTermSync = true;
					detectedReason = PT_CHECK_ERROR;
					withRescue = true;
					context.errorPId_ = pId;
					GS_TRACE_INFO(CLUSTER_OPERATION, GS_TRACE_CS_NORMAL_OPERATION,
						"CATCHUP_RECIEVE_ERROR" << ", pId=" << pId <<
						 ", backup=" << dumpNodeAddress(backupNodeId) 
								<< ", error=" << getErrorStatus(pId, backupNodeId));

				}
				LogSequentialNumber startLsn = getStartLSN(pId, ownerNodeId);
				if (checkedRevision && 
					checkPromoteOwnerCatchupLsnGap(ownerLsn, catchupLsn) &&
					((ownerLsn > 0 && catchupLsn != 0) ||
						(ownerLsn == 0 && catchupLsn == 0))
					&& startLsn <= catchupLsn
						) {
					context.errorReason_ = CATCHUP_PROMOTION;
					context.errorPId_ = pId;

					TRACE_CLUSTER_NORMAL_OPERATION(INFO,
						"Detect catchup partition promotion (pId="
							<< pId << ", owner=" << dumpNodeAddress(ownerNodeId)
							<< ", ownerLSN=" << ownerLsn
							<< ", catchup=" << dumpNodeAddress(catchupNodeId)
							<< ", catchupLSN=" << catchupLsn << ")");
					detectedReason = PT_CHECK_CATCHUP_PROMOTION;
					if (partitionCheck) {
						isNotShortTermSync = true;
						partitionCheck = false;
					}
					nextBlockQueue_.erase(pId, catchupNodeId);
					context.promoteFlag_ = true;
			}
		}
			PartitionRevisionNo currentRevision = getPartitionRoleRevision(pId, PT_CURRENT_OB);
			PartitionRevisionNo nextRevision = getPartitionRoleRevision(pId, PT_NEXT_OB);
			if (partitionCheck) {
				if (currentRevision < nextRevision) {
					updatePartitionRole(pId);
					dumpPartition = true;
				}
				partitions_[pId].partitionInfo_.shortTermTimeout_ = UNDEF_TTL;
				isNotShortTermSync = true;
				if (isNotShortTermSync) {
					if (liveNodeNum == 1 || (backups.size() == static_cast<size_t>(backupNum))) {
						normalCounter++;
					}
				}
			}
			else {
				if (isNotShortTermSync) {
					updatePartitionFlag = true;
					setChangePartition(pId);
					context.irregularPartitionList_.push_back(pId);
				}
			}
		}
		if (dumpPartition) {
			DUMP_CLUSTER( "PARTITION STATUS UPDATED");
			DUMP_CLUSTER(dumpPartitionsNew(*context.alloc_, PartitionTable::PT_CURRENT_OB));
		}
		if (static_cast<size_t>(normalCounter) == getPartitionNum() && !isPartial) {
			loadInfo_.status_ = PT_NORMAL;
		}

		if (context.needUpdatePartition_) {
			if (isError)  { 
				detectedReason = PT_CHECK_ERROR;
			}
			DUMP_CLUSTER( "Create next partition, revision=" 
					<< getPartitionRevisionNo() << ", reason=" << dumpPartitionCheckStatus(detectedReason));
		}
		else {
			if (balanceCheck) {
				if (hasCatchup) {
				}
				else {
					if (balanceStatus != PT_NORMAL) {
						context.needUpdatePartition_ = true;
						context.balanceCheck_ = true;
						DUMP_CLUSTER("Create next partition, revision=" << getPartitionRevisionNo() 
								<< ", reason=RESOLVE BALANCE");
					}
				}
			}
			if (dropCheck && !context.needUpdatePartition_) {
				util::XArray<NodeId> activeNodeList(*context.alloc_);
				getLiveNodeIdList(activeNodeList);
				int32_t replicationNum = getReplicationNum();
				int32_t liveNodeListSize = static_cast<int32_t>(activeNodeList.size() - 1);
				if (replicationNum > liveNodeListSize) {
					replicationNum = liveNodeListSize;
				}
				if (replicationNum == 0) return;
				for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
					int32_t backupSize = getBackupSize(pId);
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
						dropPartitionNodeInfo.set(pId, address, revision);
						context.enableDrop_ = true;
					}
				}
			}
		}


	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
			}
		}

void SubPartitionTable::validate(PartitionTable *pt) {
	int32_t totalCatchupCount = 0;
	for (size_t pos = 0; pos < subPartitionList_.size(); pos++) {
		PartitionId pId = subPartitionList_[pos].pId_;
		PartitionRole &nextRole = subPartitionList_[pos].role_;
		NodeAddress &address = subPartitionList_[pos].currentOwner_;
		if (!address.isValid()) {
			std::cout << "CURRENT OWNER IS NOT DEFINE, PID=" << pId << ", ABORT" << std::endl;
			abort();
		}
		if (pt->getReplicationNum() <= static_cast<uint32_t>(nextRole.getBackupSize())) {
			std::cout << "BACKUP NUM IS OVER REPLICATION NUM, PID=" << pId
					<< ", BACKUPSIZE=" << nextRole.getBackupSize() << ", REPLICATIONUM=" << pt->getReplicationNum() << ", ABORT" << std::endl;
			abort();
		}
		int32_t catchupCount = nextRole.getCatchupSize();
		if (catchupCount > 1) {
			std::cout << "CATCHUP NUM IS MUST BE ONE, PID=" << pId << ", ABORT" << std::endl;
			abort();
		}
		if (totalCatchupCount == 1 && catchupCount > 0) {
			std::cout << "CATCHUP NUM IS MUST BE ONE IN CLUSTER, BUT DUPLICATE, PID=" << pId << ", ABORT" << std::endl;
			abort();
		}
		totalCatchupCount += catchupCount;
	}
}

void PartitionTable::setSubPartiton(PartitionContext &context,
	SubPartitionTable &subPartitionTable) {
	try {
		util::XArray<PartitionId>::iterator it;
		util::Vector<bool> checkPIdList_(*context.alloc_);
		checkPIdList_.assign(getPartitionNum(), false);
		for (util::Set<PartitionId>::iterator it = context.changedPIdSet_.begin();
				it != context.changedPIdSet_.end(); it++) {
			PartitionId pId = (*it);
			setShorTermSyncTimeout(pId,  context.shortTermTimeout_);
			SubPartition &subPartition = context.tmpSubPartitionList_[pId];
			NodeId currentOwner = getOwner(pId);
			if (currentOwner == UNDEF_NODEID) {
				currentOwner = subPartition.role_.getOwner();
			}
			context.subPartitionTable_.set(this, subPartition.pId_,
				subPartition.role_, currentOwner);
			checkPIdList_[subPartition.pId_] = true;
//			PartitionRole &nextRole = subPartition.role_;
			setPartitionRole(subPartition.pId_,  subPartition.role_, PT_NEXT_OB);
		}

		for (int32_t pId = 0;static_cast<uint32_t>(pId) < getPartitionNum(); pId++) {
			if (checkPIdList_[pId]) continue;

			PartitionRole currentRole;
			getPartitionRole(pId, currentRole, PT_CURRENT_OB);

			PartitionRole nextRole;
			getPartitionRole(pId, nextRole, PT_NEXT_OB);

			if (currentRole == nextRole && !currentRole.hasCatchupRole()) {
				NodeId ownerId = getOwner(pId);
				if (ownerId != UNDEF_NODEID && getPartitionStatus(pId, ownerId) == PT_ON) {
					util::XArray<NodeId> backups(*context.alloc_);
					getBackup(pId, backups);
					bool isAllActive = true;
					for (size_t pos = 0; pos < backups.size(); pos++) {
						if (getPartitionStatus(pId, backups[pos]) != PT_ON) {
							isAllActive = false;
							break;
						}
					}
					if (isAllActive) {
						continue;
					}
				}
			}

			setShorTermSyncTimeout(pId,  context.shortTermTimeout_);
			NodeId currentOwner = getOwner(pId);
			if (currentOwner == UNDEF_NODEID) {
				currentOwner = nextRole.getOwner();
			}
			nextRole.set(revision_);
			nextRole.clearCatchup();
			context.subPartitionTable_.set(this, pId, nextRole, currentOwner);
			setPartitionRole(pId,  nextRole, PT_NEXT_OB);
		}
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

bool PartitionTable::PartitionRole::operator==(PartitionRole &role) const {
	NodeId tmpOwnerId = role.getOwner();
	if (ownerNodeId_ == tmpOwnerId) {
		std::vector<NodeId> tmpBackups = role.getBackups();
		if (backups_ == tmpBackups) {
			return true;
		}
		std::vector<NodeId> tmpCatchups = role.getCatchups();
		if (catchups_ == tmpCatchups) {
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

void PartitionTable::createNextPartition(PartitionContext &context) {
	try {
		PartitionId targetPId;
		NodeId nodeId;
		util::StackAllocator &alloc = *context.alloc_;
		util::XArray<PartitionId> &targetPIdList = context.shortTermPIdList_;
		bool isRepairPartition = context.isRepairPartition_;
//		bool isShufflePartition = context.isShufflePartition_;
		size_t targetPIdSize = targetPIdList.size();
		uint32_t partitionNum = getPartitionNum();

		size_t pos = 0;
		int32_t nodeNum = getNodeNum();
		int32_t replicationNum = getReplicationNum();
		util::Vector<NodeId> tmpOwnerList(alloc);

		util::XArray<bool> isActivePartitionList(alloc);
		util::XArray<bool> isSetPartitionList(alloc);
		util::XArray<NodeId>::iterator nodeItr;
		util::XArray<NodeId> liveNodeList(alloc);

		NodeId currentOwner;
//		NodeId syncOwner;
		NodeId candOwner;

		util::Vector<SubPartition> &tmpSubPartitionList = context.tmpSubPartitionList_;
		if (targetPIdSize != partitionNum) {
			for (PartitionId pId = 0; pId < partitionNum; pId++) {
				PartitionRole nextRole;
				getPartitionRole(pId, nextRole, PT_NEXT_OB);
				nextRole.set(revision_);
				currentOwner = getOwner(pId);
				if (currentOwner == UNDEF_NODEID) {
					currentOwner = nextRole.getOwner();
				}
				SubPartition subPartition(pId, nextRole);
				subPartition.setCurrentOwner(this, currentOwner, currentOwner);
				tmpSubPartitionList.push_back(subPartition);
				}
			}
			else {
			for (PartitionId pId = 0; pId < partitionNum; pId++) {
				PartitionRole tmpRole(pId, revision_, PT_NEXT_OB);
				SubPartition subPartition(pId, tmpRole);
				tmpSubPartitionList.push_back(subPartition);
			}
		}

		LoadSummary loadSummary(alloc, nodeNum);
		loadSummary.init(nodeNum);
		util::XArray<uint32_t> &ownerLoadList = loadSummary.ownerLoadList_;
		util::XArray<uint32_t> &dataLoadList = loadSummary.dataLoadList_;

		tmpOwnerList.assign(static_cast<size_t>(partitionNum), UNDEF_NODEID);
		isActivePartitionList.assign(static_cast<size_t>(partitionNum), false);
		isSetPartitionList.assign(static_cast<size_t>(partitionNum), false);

		getLiveNodeIdList(liveNodeList);
		int32_t liveNodeListSize = static_cast<int32_t>(liveNodeList.size());
		if (liveNodeListSize == 0) return;
		if (replicationNum > liveNodeListSize - 1) {
			liveNodeListSize = replicationNum;
		}
		pos = 0;
			for (PartitionId pId = 0; pId < partitionNum; pId++) {
			for (nodeItr = liveNodeList.begin(); nodeItr != liveNodeList.end();nodeItr++) {
				nodeId = (*nodeItr);
				if (isOwner(pId, nodeId, PT_NEXT_OB)) {
					loadSummary.addOwner(nodeId);
				}
				else if (isBackup(pId, nodeId, PT_NEXT_OB)) {
					loadSummary.addBackup(nodeId);
			}
		}
		}
		LogSequentialNumber maxLsn, targetLsn, ownerLsn, startLsn;
		int32_t candOwnerCount, backupCount;

		for (pos = 0; pos < targetPIdSize; pos++) {
			targetPId = targetPIdList[pos];
			NodeId owner = getOwner(targetPId);
				if (owner != UNDEF_NODEID) {
				loadSummary.deleteOwner(owner);
				std::vector<NodeId> backups;
				getBackup(targetPId, backups);
				for (size_t pos = 0; pos < backups.size(); pos++) {
					NodeId backup = backups[pos];
					if (backup != UNDEF_NODEID) {
					loadSummary.deleteBackup(backup);
						}
					}
				}
				currentOwner = getOwner(targetPId);
				if (currentOwner != UNDEF_NODEID) {
					if (nodes_[currentOwner].heartbeatTimeout_ != UNDEF_TTL &&
						getPartitionRoleStatus(targetPId, currentOwner) == PT_OWNER) {
						isActivePartitionList[targetPId] = true;
					}
				}
			}
		util::Vector<PartitionId> &irregularPartitionList = context.irregularPartitionList_;
		util::Vector<PartitionId> rescuePartitionList(*context.alloc_);

		for (util::Vector<PartitionId>::iterator it = irregularPartitionList.begin();
				it != irregularPartitionList.end(); it++) {
			targetPId = (*it);
			if (context.withRescue_) {
				DUMP_CLUSTER("RESCUE MODE, PID=" << targetPId);
				NodeId currentOwner = getOwner(targetPId);
				if (currentOwner == UNDEF_NODEID) {
					currentOwner = getOwner(targetPId, PT_NEXT_OB);
					if (currentOwner == UNDEF_NODEID) {
						continue;
					}
				}
				rescuePartitionList.push_back(targetPId);
				tmpOwnerList[targetPId] = currentOwner;
				setAvailable(targetPId, true);
				loadSummary.addOwner(currentOwner);
				PartitionRole &targetRole = tmpSubPartitionList[targetPId].role_;
				targetRole.clear();
				targetRole.setOwner(currentOwner);
				tmpSubPartitionList[targetPId].setCurrentOwner(this, currentOwner, currentOwner);
				if (replicationNum == 0) {
					context.setChangePId(MODE_SHORTTERM_SYNC, targetPId, targetRole);
					isSetPartitionList[targetPId] = true;
				}
				else {
					util::XArray<NodeId> backups(*context.alloc_);
					std::vector<NodeId> &candBackups = targetRole.getBackups();
					getBackup(targetPId, backups, PT_NEXT_OB);
					int32_t backupCount = 0;
					NodeId nextOwner = getOwner(targetPId, PT_NEXT_OB);
					if (currentOwner != nextOwner) {
						backups.push_back(nextOwner);
					}
					for (size_t currentPos = 0; currentPos < backups.size(); currentPos++) {
						bool syncSuccess = ((getOtherStatus(targetPId, backups[currentPos])
									== PT_OTHER_SHORT_SYNC_COMPLETE) == 1);
						if (currentOwner != backups[currentPos] 
								&& syncSuccess && backupCount < replicationNum) {
							candBackups.push_back(backups[currentPos]);
							loadSummary.addBackup(backups[currentPos]);
							backupCount++;
							GS_TRACE_DEBUG(CLUSTER_OPERATION, GS_TRACE_CS_NORMAL_OPERATION,
								"SyncSuccess pId=" << targetPId << ", backup=" << backups[currentPos]);
						}
						else {
							if (!syncSuccess) {
							GS_TRACE_DEBUG(CLUSTER_OPERATION, GS_TRACE_CS_NORMAL_OPERATION,
								"NOT SYNC pId=" << targetPId << ", backup=" << backups[currentPos]);
								setBlockQueue(targetPId, PT_CHANGE_NEXT_TABLE, backups[currentPos]);
								setBlockQueue(targetPId, PT_CHANGE_GOAL_TABLE, backups[currentPos]);
							}
						}
					}
					context.setChangePId(MODE_SHORTTERM_SYNC, targetPId, targetRole);
					isSetPartitionList[targetPId] = true;
				}
			}
		}
		for (int32_t phase = 0; phase < 2; phase++) {
			for (pos = 0; pos < targetPIdSize; pos++) {
				util::Vector<NodeId> checkNodeIdList(alloc);
				targetPId = targetPIdList[pos];
				candOwner = UNDEF_NODEID;
				candOwnerCount = 0;
				maxLsn = getMaxLsn(targetPId);
				if (phase == 0) {
					for (int32_t i = 0; static_cast<uint32_t>(i) < liveNodeList.size(); i++) {
						checkNodeIdList.push_back(liveNodeList[i]);
					}
					if (!isAvailable(targetPId) && isRepairPartition) {
							GS_TRACE_INFO(CLUSTER_OPERATION, GS_TRACE_CS_NORMAL_OPERATION,
								"WITH REPAIR PID=" << targetPId);

//						LogSequentialNumber prevMaxLsn = getMaxLsn(targetPId);
						LogSequentialNumber nextMaxLsn = 0;
						for (nodeItr = liveNodeList.begin();
								nodeItr != liveNodeList.end(); nodeItr++) {
							nodeId = (*nodeItr);
							targetLsn = getLSN(targetPId, nodeId);
							if (targetLsn > nextMaxLsn) {
								nextMaxLsn = targetLsn;
							}
						}
						candOwner = repairNotAvailablePartition(alloc, targetPId, loadSummary, liveNodeList);
						candOwnerCount++;
					}
					else {
						if (tmpOwnerList[targetPId] != UNDEF_NODEID) {
							continue;
						}
				}
			}
			else {
					if (tmpOwnerList[targetPId] != UNDEF_NODEID) {
						continue;
			}
			checkNodeIdList.clear();
			util::XArray<CatchupOwnerNodeInfo> ownerOrderList(alloc);
					uint32_t maxOwnerLoad, minOwnerLoad, maxDataLoad, minDataLoad;
			sortOrderList(ownerOrderList, liveNodeList, ownerLoadList,
				dataLoadList, maxOwnerLoad, minOwnerLoad, maxDataLoad,
				minDataLoad, cmpCatchupOwnerNode);
					util::Vector<bool> checkedList(alloc);
					util::Vector<NodeId> tmpNodeIdList(alloc);
					checkedList.assign(getNodeNum(), false);
					for (size_t pos = 0; pos < ownerOrderList.size(); pos++) {
						NodeId tmpNodeId = ownerOrderList[pos].nodeId_;
						tmpNodeIdList.push_back(tmpNodeId);
						if (isOwnerOrBackup(targetPId, tmpNodeId)) {
							checkNodeIdList.push_back(tmpNodeId);
							checkedList[tmpNodeId] = true;
						}
					}
					for (size_t pos = 0; pos < ownerOrderList.size(); pos++) {
						NodeId tmpNodeId = ownerOrderList[pos].nodeId_;
						if (checkedList[tmpNodeId]) continue;
							checkNodeIdList.push_back(tmpNodeId);
					}
				}
				currentOwner = getOwner(targetPId);
				for (util::Vector<NodeId>::iterator it = checkNodeIdList.begin();
						it != checkNodeIdList.end(); it++) {
					nodeId = (*it);
					targetLsn = getLSN(targetPId, nodeId);
					if (checkNextOwner(targetPId, nodeId, targetLsn, maxLsn, currentOwner)) {
						candOwner = nodeId;
						candOwnerCount++;
						if (phase == 0) {
							continue;
						}
						else {
							break;
						}
					}
					}
				if (phase == 0 && candOwnerCount > 1) {
					continue;
				}
				if (candOwnerCount == 1) {
					setAvailable(targetPId, true);
					PartitionRole &targetRole = tmpSubPartitionList[targetPId].role_;
					tmpOwnerList[targetPId] = candOwner;
					targetRole.setOwner(candOwner);
					loadSummary.addOwner(candOwner);
					if (replicationNum == 0) {
						tmpSubPartitionList[targetPId].setCurrentOwner(this, currentOwner, candOwner);
						context.setChangePId(MODE_SHORTTERM_SYNC, targetPId, targetRole);
						isSetPartitionList[targetPId] = true;
						continue;
				}
			}
			if (tmpOwnerList[targetPId] == UNDEF_NODEID) {
					setAvailable(targetPId, false);
						PartitionRole &targetRole = tmpSubPartitionList[targetPId].role_;
					context.setChangePId(MODE_SHORTTERM_SYNC, targetPId, targetRole);
				LogSequentialNumber tmpMaxLsn = 0;
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
				try {
					util::NormalOStringStream strstrm;
					strstrm << "Target partition owner is not assgined, pId="
							<< targetPId << ", expected maxLSN=" << maxLsn
							<< ", actual maxLSN=" << tmpMaxLsn;
					if (notLiveMaxNodeIdList.size() > 0) {
							strstrm << ", down nodeList="
								<< dumpNodeAddressList(notLiveMaxNodeIdList) << ")";
					}
					GS_THROW_USER_ERROR(GS_ERROR_CS_PARTITION_OWNER_NOT_ASSGIN, strstrm.str().c_str());
				}
				catch (std::exception &e) {
					UTIL_TRACE_EXCEPTION_WARNING(CLUSTER_OPERATION, e, "");
				}
			}
			}
		}
		NodeId candBackup;
		NodeId candCurrentOwner;

		if (replicationNum > 0) {

			for (pos = 0; pos < targetPIdSize; pos++) {
				targetPId = targetPIdList[pos];
				if (isSetPartitionList[targetPId]) continue;
				backupCount = 0;
				candOwner = tmpOwnerList[targetPId];
				if (candOwner == UNDEF_NODEID) {
					continue;
				}
				currentOwner = getOwner(targetPId);
				candCurrentOwner = currentOwner;
				if (currentOwner == UNDEF_NODEID) {
					candCurrentOwner = candOwner;
				}
				ownerLsn = getLSN(targetPId, candCurrentOwner);
				startLsn = getStartLSN(targetPId, candCurrentOwner);
				util::XArray<DataNodeInfo> backupOrderList(alloc);

				PartitionRole &nextRole = tmpSubPartitionList[targetPId].role_;
				NodeId catchupNodeId = UNDEF_NODEID;
				std::vector<NodeId> &catchups = nextRole.getCatchups();
				if (catchups.size() > 0) {
					catchupNodeId = catchups[0];
				}
				nextRole.clear();
				std::vector<NodeId> &backups = nextRole.getBackups();
				GS_TRACE_DEBUG(CLUSTER_OPERATION, GS_TRACE_CS_NORMAL_OPERATION,
					"CHECK BACKUP PID=" << targetPId << ", startLsn=" << startLsn << ", ownerLsn=" << ownerLsn);

				for (nodeItr = liveNodeList.begin();nodeItr != liveNodeList.end(); nodeItr++) {
					candBackup = (*nodeItr);
					if (candOwner == candBackup) {
						continue;
					}
					targetLsn = getLSN(targetPId, candBackup);

					if (checkNextBackup(targetPId, candBackup, targetLsn, startLsn, ownerLsn)) {
						if (nextBlockQueue_.find(targetPId, candBackup)) {
							continue;
						}
						DataNodeInfo dataNodeInfo(candBackup,
							dataLoadList[candBackup], dataLoadList[candBackup]);
						backupOrderList.push_back(dataNodeInfo);
					}
				}
				util::XArray<DataNodeInfo>::iterator backupNodeItr;
				bool catchupPromotion = false;
				backupCount = 0;
				if (catchupNodeId != UNDEF_NODEID) {
					if (checkLiveNode(catchupNodeId)) {
						targetLsn = getLSN(targetPId, catchupNodeId);
						if (checkNextBackup(targetPId, catchupNodeId, targetLsn, startLsn, ownerLsn)) {
							candBackup = catchupNodeId;
							backups.push_back(candBackup);
							loadSummary.addBackup(candBackup);
							backupCount++;
							catchupPromotion = true;							
						}
					}
				}
				if (backupOrderList.size() != 0 && backupCount < replicationNum) {
					std::sort(backupOrderList.begin(), backupOrderList.end(), cmpCatchupNode);
					for (backupNodeItr = backupOrderList.begin();
						backupNodeItr != backupOrderList.end(); backupNodeItr++) {

						candBackup = (*backupNodeItr).nodeId_;
						if (candBackup == catchupNodeId) continue;
						backups.push_back(candBackup);

						loadSummary.addBackup(candBackup);
						backupCount++;
						if (backupCount >= replicationNum) {
							break;
						}
					}
				}
				nextRole.setOwner(candOwner);
				nextRole.clearCatchup();
				tmpSubPartitionList[targetPId].setCurrentOwner(this, candCurrentOwner, candOwner);
				context.setChangePId(MODE_SHORTTERM_SYNC, targetPId, nextRole);
				isSetPartitionList[targetPId] = true;
			}
		}
		if (rescuePartitionList.size() > 0) {
			util::NormalOStringStream ss;
			int32_t listSize = static_cast<int32_t>(rescuePartitionList.size());
			ss << "[";
			for (int32_t pos = 0; pos < listSize; pos++) {
				ss << "(pId=" << rescuePartitionList[pos];
				ss << ", lsn=" << getLSN(rescuePartitionList[pos]) << ")";
				if (pos != listSize - 1) {
					ss << ",";
				}
			}
			ss << "]";
			GS_TRACE_WARNING(CLUSTER_OPERATION, GS_TRACE_CS_NORMAL_OPERATION,
					ss.str().c_str());
		}
		currentRevisionNo_ = getPartitionRevisionNo();
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void PartitionTable::createCatchupPartition(PartitionContext &context, bool isSkip) {
	try {

		PartitionId pId, targetPId;
		int32_t partitionPos = 0, nodePos = 0;
		NodeId nodeId;

		int32_t nodeNum = getNodeNum();
		uint32_t partitionNum = getPartitionNum();
		int32_t targetBackupNum;
		int32_t replicationNum =  getReplicationNum();
		util::StackAllocator &alloc = *context.alloc_;
		util::Vector<SubPartition> &tmpSubPartitionList = context.tmpSubPartitionList_;
		bool isPartial = false;

		if (replicationNum == 0) {
			return;
		}
		if (isSkip) {
			resetPrevCatchup(context);
			return;
		}
		if (context.withRescue_) {
			return;
		}

		util::XArray<NodeId> liveNodeList(alloc);
		util::XArray<NodeId>::iterator nodeItr;
		getLiveNodeIdList(liveNodeList);
		size_t currentNodeNum = liveNodeList.size();
		if (currentNodeNum == 1) {
			return;
		}
		if (static_cast<size_t>(replicationNum) > currentNodeNum - 1) {
			targetBackupNum = currentNodeNum - 1;
		}
		else {
			targetBackupNum = replicationNum;
		}
		LoadSummary loadSummary(alloc, nodeNum);
		loadSummary.init(nodeNum);
		util::XArray<uint32_t> &ownerLoadList = loadSummary.ownerLoadList_;
		util::XArray<uint32_t> &dataLoadList = loadSummary.dataLoadList_;
		uint32_t minOwnerLoad, maxOwnerLoad, minDataLoad, maxDataLoad;
		int32_t backupNum;
		NodeId owner, target;
		bool isReplicaLoss = false;

		util::XArray<ReplicaInfo> replicaLossList(alloc);
		util::XArray<PartitionId> tmpSearchPIdList(alloc);
		for (pId = 0; pId < partitionNum; pId++) {
			tmpSearchPIdList.push_back(pId);
		}
		util::Random random(static_cast<int64_t>(util::DateTime::now(false).getUnixTime()));
		arrayShuffle(tmpSearchPIdList,
				static_cast<uint32_t>(tmpSearchPIdList.size()), random);

		for (size_t tmpPos = 0; tmpPos < tmpSearchPIdList.size(); tmpPos++) {
			pId = tmpSearchPIdList[tmpPos]; 
			PartitionRole &nextRole = context.tmpSubPartitionList_[pId].role_;
			if (nextRole.getOwner() == UNDEF_NODEID) {
				isPartial = true;
				continue;
			}
			backupNum = nextRole.getBackupSize();
			if (backupNum < targetBackupNum) {
				ReplicaInfo repInfo(pId, backupNum);
				replicaLossList.push_back(repInfo);
			}
		}
		if (replicaLossList.size() > 0) {
			isReplicaLoss = true;
		}

		for (pId = 0; pId < partitionNum; pId++) {
			for (nodeItr = liveNodeList.begin(); nodeItr != liveNodeList.end();nodeItr++) {
				PartitionRole &nextRole = context.tmpSubPartitionList_[pId].role_;
				nodeId = (*nodeItr);
				if (nextRole.isOwner(nodeId)) {
					loadSummary.addOwner(nodeId);
				}
				else if (nextRole.isBackup(nodeId)) {
					loadSummary.addBackup(nodeId);
				}
			}
		}
		util::XArray<DataNodeInfo> catchupOrderList(alloc);
		sortOrderList(catchupOrderList, liveNodeList, ownerLoadList,
			dataLoadList, maxOwnerLoad, minOwnerLoad, maxDataLoad, minDataLoad,
			cmpCatchupNode, true);

		bool isDataBalance = isBalance(minDataLoad, maxDataLoad, false);
		bool isOwnerBalance = isBalance(minOwnerLoad, maxOwnerLoad, true);

		DUMP_CLUSTER("CATCHUP CHECK");
		if (!isReplicaLoss && isDataBalance && isOwnerBalance) {
			if (isPartial) {
				DUMP_CLUSTER("OWNER LOSS");
			}
			else {
				DUMP_CLUSTER("ALL BALANCE OK");
			}
			return;
		}
		if (isReplicaLoss) {
			DUMP_CLUSTER("REPLICA LOSS");
		}
		if (!isDataBalance) {
			DUMP_CLUSTER("NOT DATA BALANCE");
		}
		if (!isOwnerBalance) {
			DUMP_CLUSTER("NOT OWNER BALANCE");
		}
		if (isPartial) {
			DUMP_CLUSTER("OWNER LOSS");
		}
		PartitionId firstPId;
		util::XArray<PartitionId> searchPIdList(alloc);
		if (isReplicaLoss) {
			std::sort(replicaLossList.begin(), replicaLossList.end(), cmpReplica);
			for (partitionPos = 0;
					partitionPos < static_cast<int32_t>(replicaLossList.size());partitionPos++) {
				PartitionRole &role = context.tmpSubPartitionList_[replicaLossList[partitionPos].pId_].role_;
				if (role.getOwner() != UNDEF_NODEID) {
				searchPIdList.push_back(replicaLossList[partitionPos].pId_);
			}
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
				if (getOwner(pId) == UNDEF_NODEID) {
					continue;
				}
				searchPIdList.push_back(pId);
			}
			for (PartitionId pId = 0; pId < firstPId; pId++) {
				if (getOwner(pId) == UNDEF_NODEID) {
					continue;
				}
				searchPIdList.push_back(pId);
			}
			util::Random random(static_cast<int64_t>(util::DateTime::now(false).getUnixTime()));
			arrayShuffle(searchPIdList,
					static_cast<uint32_t>(searchPIdList.size()), random);
		}
		bool isFound = false;
		for (partitionPos = 0;partitionPos < static_cast<int32_t>(searchPIdList.size());partitionPos++) {
			targetPId = searchPIdList[partitionPos];
			PartitionRole &nextRole = context.tmpSubPartitionList_[targetPId].role_;

			for (nodePos = 0;nodePos < static_cast<int32_t>(catchupOrderList.size());nodePos++) {
				target = catchupOrderList[nodePos].nodeId_;
				if (target == UNDEF_NODEID) continue; 
				owner = nextRole.getOwner();
				if (owner >= 0 && owner != target && !nextRole.isBackup(target)) {
					if (!goalBlockQueue_.find(targetPId, target)) {
						if (dataLoadList[target] == minDataLoad || isReplicaLoss) {
							PartitionRole &nextCatchupRole = tmpSubPartitionList[targetPId].role_;
							std::vector<NodeId> &catchupList = nextCatchupRole.getCatchups();
							catchupList.clear();
							catchupList.push_back(target);
							nextCatchupRole.setOwner(owner);
							context.setChangePId(MODE_LONGTERM_SYNC, targetPId, nextCatchupRole);
							resetPrevCatchup(context, targetPId);
							prevCatchupPId_ = targetPId;
							prevCatchupNodeId_ = target;
							isFound = true;
							return;
						}
						else {
						}
					}
					else {
					}
				}
				else {
				}
			}
		}
		if (context.changedLongTermPIdList_.size() == 0) {
			resetPrevCatchup(context);
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
	util::StackAllocator &alloc, TableType type) {
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
	case PT_NEXT_OB:
		partitionType = "NEXT_TABLE";
		break;
	case PT_CURRENT_OB:
		partitionType = "CURRENT_TABLE";
		break;
	default:
		break;
	}
	ss << "[" << partitionType << "]" << " " << getTimeStr(util::DateTime::now(TRIM_MILLISECONDS).getUnixTime()) << std::endl;
	for (NodeId nodeId = 0; nodeId < getNodeNum(); nodeId++) {
		ss << "#" << std::setw(nodeIdDigit) << nodeId << ", " << dumpNodeAddress(nodeId);
		if (getHeartbeatTimeout(nodeId) == UNDEF_TTL) {
			ss << "(Down)";
		}
			ss << std::endl;
	}

	for (pId = 0; pId < partitionNum; pId++) {
		PartitionRole role;
		getPartitionRole(pId, role, type);
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
	case PT_NEXT_OB:
		partitionType = "NEXT_TABLE";
		break;
	case PT_CURRENT_OB:
		partitionType = "CURRENT_TABLE";
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
		std::vector<NodeId> &catchups = role->getCatchups();
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
		ss << terminateCode;
	}

	ss << terminateCode << "[OTHER LIST]" << terminateCode;
	for (pId = 0; pId < partitionNum; pId++) {
		ss << "[P" << std::setfill('0') << std::setw(partitionDigit_) << pId
		<< "]";
		if (isMaster()) {
			for (NodeId nodeId = 0; nodeId < nodeNum_; nodeId++) {
				PartitionRevisionNo revision = getOtherStatus(pId, nodeId);
				ss << revision;
				if (nodeId != nodeNum_ - 1) ss << ",";
			}
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
		ss1 << ", ptRev:" << revision_.toString()
		<< ", type:" << dumpPartitionTableType(type_) << "}";

	return ss1.str();
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
	ss2 << "}";
	ss1 << " {pId=" << pId_ << ", owner:" << ownerNodeId_
		<< ", backups:" << ss2.str() << ", catchups:" << ss3.str() << ", ptRev:" << revision_.toString()
		<< ", type:" << dumpPartitionTableType(type_) << "}";

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

	ss1 << " {pId=" << pId_ << ", ownerAddress:";
	if (ownerNodeId_ == UNDEF_NODEID) {ss1 << "UNDEF";}
	else pt->getNodeAddress(ownerNodeId_, serviceType);
	ss1 << "(" << ownerNodeId_ << ")";
	if (backups_.size() > 0) {
		ss1 << ", backupAddressList:" << ss2.str();
	}
	if (catchups_.size() > 0) {
		ss1 << ", catchupAddressList:" << ss3.str();
	}
	ss1 << ", ptRev:" << revision_.toString()
	<< ", type:" << dumpPartitionTableType(type_) << "}";

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
		setOtherStatus(pId, PT_OTHER_NORMAL, targetNodeId);
		setOtherStatus(pId, PT_ERROR_NORMAL, targetNodeId);
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

void PartitionContext::setChangePId(SyncMode mode, PartitionId pId,
		PartitionRole &role) {
	role.checkBackup();
	role.checkCatchup();
	if (mode == MODE_SHORTTERM_SYNC) {
		changedShortTermPIdList_.push_back(pId);
	}
	else {
		changedLongTermPIdList_.push_back(pId);
	}
	changedPIdSet_.insert(pId);
}

void PartitionContext::setShufflePartition(bool isShuffle) {
	if (!isShuffle) return;
	shortTermPIdList_.clear();
	for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
		shortTermPIdList_.push_back(pId);
	}
	util::Random random(static_cast<int64_t>(util::DateTime::now(false).getUnixTime()));
	arrayShuffle(shortTermPIdList_,
			static_cast<uint32_t>(shortTermPIdList_.size()), random);
	isShufflePartition_ = isShuffle;
}


NodeId PartitionTable::repairNotAvailablePartition(util::StackAllocator &alloc,
		PartitionId targetPId, LoadSummary &loadInfo,
		util::XArray<NodeId> &liveNodeList) {
	LogSequentialNumber prevMaxLsn = getMaxLsn(targetPId);
	LogSequentialNumber nextMaxLsn = 0;
	LogSequentialNumber targetLsn;
	NodeId nodeId;

	for (util::XArray<NodeId>::iterator nodeItr = liveNodeList.begin();
			nodeItr != liveNodeList.end(); nodeItr++) {
		nodeId = (*nodeItr);
		targetLsn = getLSN(targetPId, nodeId);
		if (targetLsn > nextMaxLsn) {
			nextMaxLsn = targetLsn;
		}
	}

	util::XArray<CatchupOwnerNodeInfo> ownerOrderList(alloc);
	uint32_t maxOwnerLoad, minOwnerLoad, maxDataLoad, minDataLoad;
	sortOrderList(ownerOrderList, liveNodeList, loadInfo.ownerLoadList_,
			loadInfo.dataLoadList_, maxOwnerLoad, minOwnerLoad, maxDataLoad,
			minDataLoad, cmpCatchupOwnerNode);

	for (util::XArray<CatchupOwnerNodeInfo>::iterator ownerNodeItr = ownerOrderList.begin();
			ownerNodeItr != ownerOrderList.end(); ownerNodeItr++) {
		nodeId = (*ownerNodeItr).nodeId_;
		targetLsn = getLSN(targetPId, nodeId);
		if (targetLsn == nextMaxLsn) {
			maxLsnList_[targetPId] = nextMaxLsn;
			TRACE_CLUSTER_NORMAL_OPERATION(INFO,
				"Repair partition (pId=" << targetPId << ", owner="
					<< dumpNodeAddress(nodeId) << ", prevMaxLSN=" << prevMaxLsn
					<< ", repairedMaxLSN=" << nextMaxLsn);
			isRepaired_ = true;
			nextBlockQueue_.clear(targetPId);
			return nodeId;
		}
	}
	return UNDEF_NODEID;
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
		bool check1 = true;
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


bool PartitionTable::checkNextBackup(PartitionId pId, NodeId checkedNode, LogSequentialNumber checkedLsn,
		LogSequentialNumber currentOwnerStartLsn, LogSequentialNumber currentOwnerLsn) {

	bool backupCond1 = !checkLimitOwnerBackupLsnGap(currentOwnerLsn, checkedLsn);
	bool backupCond2 = (checkedLsn >= currentOwnerStartLsn);


	bool backupCond = (backupCond1 && backupCond2);

	bool catchupCond1 = ((prevCatchupPId_ == pId) && (prevCatchupNodeId_ == checkedNode));
	bool catchupCond2 = ((currentOwnerLsn == 0 && checkedLsn == 0)
			|| (currentOwnerLsn > 0 && checkedLsn > 0));
	bool catchupCond3 = !checkLimitOwnerBackupLsnGap(currentOwnerLsn, checkedLsn,
			PT_BACKUP_GAP_FACTOR);
	bool catchupCond4 = (checkedLsn >= currentOwnerStartLsn);
	bool catchupCond = (catchupCond1 && catchupCond2 && catchupCond3 && catchupCond4);
	return (backupCond || catchupCond);
}

void PartitionTable::resetPrevCatchup(PartitionContext &context, PartitionId targetPId) {
	if (prevCatchupPId_ != UNDEF_PARTITIONID && prevCatchupPId_ != targetPId) {
		if (context.tmpSubPartitionList_.size() > prevCatchupPId_) {
			PartitionRole &targetRole = context.tmpSubPartitionList_[prevCatchupPId_].role_;
			targetRole.clearCatchup();
			context.setChangePId(MODE_LONGTERM_SYNC, prevCatchupPId_, targetRole);
		}
		setBlockQueue(prevCatchupPId_, PT_CHANGE_GOAL_TABLE, prevCatchupNodeId_);
	}
	prevCatchupPId_ = UNDEF_PARTITIONID;
	prevCatchupNodeId_ = UNDEF_NODEID;
}

void SubPartitionTable::set(PartitionTable *pt, PartitionId pId, PartitionRole &role, NodeId ownerNodeId) {
	NodeAddress ownerAddress;
	if (ownerNodeId != UNDEF_NODEID) {
		ownerAddress = pt->getNodeAddress(ownerNodeId);
	}
	SubPartition subPartition(pId, role, ownerAddress);
	PartitionRole &tmpRole = subPartition.role_;
	tmpRole.encodeAddress(pt, role);
	subPartitionList_.push_back(subPartition);
}

bool PartitionTable::checkActivePartition(PartitionId pId, PartitionRoleStatus role, NodeId targetNode) {
	if (getPartitionStatus(pId, targetNode) != PT_ON) {
		return false;
	}
	if (role != PT_NONE) {
		if (getPartitionRoleStatus(pId, targetNode) != role) {
			return false;
		}
	}
	PartitionRole currentRole;
	getPartitionRole(pId, currentRole);
	if (currentRole.getRevisionNo() != getPartitionRevision(pId, targetNode)) {
		return false;
	}
	return true;
}

bool PartitionTable::checkShorTermSyncTimeout(PartitionId pId, int64_t checkTime) {
	int64_t limitTime = partitions_[pId].partitionInfo_.shortTermTimeout_;
	if (limitTime == UNDEF_TTL) return true;
	if (limitTime != UNDEF_TTL && checkTime > limitTime) {
		partitions_[pId].partitionInfo_.shortTermTimeout_ = UNDEF_TTL;
		return true;
	}
	else {
		return false;
	}
}

void ClusterExecStatus::exec() {
	for (int32_t pos = 0; pos < CURRENT_STATUS_MAX; pos++) {
		if (status_[pos] == -1) {
			status_[pos] = 0;
		}
	}
	coverageList_[status_[0]][status_[1]][status_[2]]++;
	DUMP_CLUSTER(status_[0] << "," << status_[1] << "," << status_[2]);
}

std::string ClusterExecStatus::dumpClusterStatus(int32_t status) {
	switch (status) {
	case STATUS_INITAL:
		return "STATUS_INITAL";
	case STATUS_RECONSTRUCT:
		return "STATUS_RECONSTRUCT";
	case STATUS_NORMAL:
		return "STATUS_NORMAL";
	case STATUS_NODE_DOWN:
		return "STATUS_NODE_DOWN";
	case STATUS_SHORT_SYNC:
		return "STATUS_SHORT_SYNC";
	case STATUS_LONG_SYNC:
		return "STATUS_LONG_SYNC";
	default:
		return "";
	}
}

std::string ClusterExecStatus::dumpNext(int32_t status) {
	switch (status) {
	case NEXT_NORMAL:
		return "NEXT_NORMAL";
	case NEXT_RESCUE_MODE:
		return "NEXT_RESCUE_MODE";
	case NEXT_OWNER_CHANGE:
		return "NEXT_OWNER_CHANGE";
	case NEXT_OWNER_CATCHUP:
		return "NEXT_OWNER_CATCHUP";
	default:
		return "";
	}
}

std::string ClusterExecStatus::dumpDetection(int32_t status) {
	switch (status) {
	case DETECT_FAIL_SINGLE:
		return "DETECT_FAIL_SINGLE";
	case DETECT_FAIL_MULTI:
		return "DETECT_FAIL_MULTI";
	case DETECT_NODE_RECOVERY:
		return "DETECT_NODE_RECOVERY";
	case DETECT_INCREASE_NODE:
		return "DETECT_INCREASE_NODE";
	case DETECT_CATCHUP_NODE:
		return "DETECT_CATCHUP_NODE";
	case DETECT_INVALID_SELECT_OWNER:
		return "DETECT_INVALID_SELECT_OWNER";
	case DETECT_SHORT_SYNC_TIMEOUT:
		return "DETECT_SHORT_SYNC_TIMEOUT";
	case DETECT_INVALID_LOG:
		return "DETECT_INVALID_LOG";
	case DETECT_INVALID_CHUNK:
		return "DETECT_INVALID_CHUNK";
	case DETECT_DEATCITVATE_OWNER:
		return "DETECT_DEATCITVATE_OWNER";
	case DETECT_NOT_BALANCE:
		return "DETECT_NOT_BALANCE";
	default:
		return "";
	}
}


void ClusterExecStatus::setClusterStatus(int32_t status) {
	if (status_[0] == -1) {
		status_[0] = status;
	}
}
void ClusterExecStatus::setDetection(int32_t status) {
	if (status_[1] == -1) {
		status_[1] = status;
	}
}
void ClusterExecStatus::setNext(int32_t status) {
	if (status_[2] == -1) {
		status_[2] = status;
	}
}
