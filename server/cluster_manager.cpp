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
	@brief Implementation of ClusterManager
*/

#include "cluster_manager.h"
#include "util/trace.h"
#include "gs_error.h"
#include "picojson.h"
#include "sha2.h"
#include <iostream>

#include "cluster_service.h"

UTIL_TRACER_DECLARE(CLUSTER_SERVICE);
UTIL_TRACER_DECLARE(CLUSTER_OPERATION);


ClusterManager::ClusterManager(const ConfigTable &configTable,
	PartitionTable *partitionTable, ClusterVersionId versionId)
	: clusterConfig_(configTable),
	  clusterInfo_(partitionTable),
	  pt_(partitionTable),
	  versionId_(versionId),
	  isSignalBeforeRecovery_(false),
	  currentChunkSyncPId_(UNDEF_PARTITIONID),
	  currentChunkSyncCPId_(UNDEF_CHECKPOINT_ID),
	  ee_(NULL),
	  clsSvc_(NULL)
{
	statUpdator_.manager_ = this;
	try {
#ifdef GD_ENABLE_UNICAST_NOTIFICATION
#else
		setDigest(configTable);
#endif

		clusterInfo_.startupTime_ =
			util::DateTime::now(TRIM_MILLISECONDS).getUnixTime();

		std::string settedClusterName =
			configTable.get<const char8_t *>(CONFIG_TABLE_CS_CLUSTER_NAME);
		if (settedClusterName.length()) {
			JoinClusterInfo joinInfo(true);
			joinInfo.set(settedClusterName, 0);
			set(joinInfo);
			clusterConfig_.setClusterName(settedClusterName);
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_INVALID,
				"Parameter must be specified "
				"(source=gs_cluster.json:gs_node.json, "
				"name=cluster.clusterName)");
		}
		delayCheckpointLimitTime_.assign(
			configTable.getUInt32(CONFIG_TABLE_DS_CONCURRENCY), 0);
		concurrency_ = configTable.get<int32_t>(CONFIG_TABLE_DS_CONCURRENCY);

#ifdef CLUSTER_MOD_WEB_API
		ConfigTable *tmpTable = (ConfigTable *)&configTable;
		config_.setUpConfigHandler(this, *tmpTable);
#endif
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

ClusterManager::~ClusterManager() {}

/*!
	@brief Checks if node status is executable
*/
void ClusterManager::checkNodeStatus() {
	if (!statusInfo_.checkNodeStatus()) {
		GS_THROW_USER_ERROR(GS_ERROR_CLM_NODE_STATUS_CHECK_FAILED, "");
	}
}

/*!
	@brief Checks if status is active
*/
void ClusterManager::checkActiveStatus() {
	if (!isActive()) {
		GS_THROW_USER_ERROR(GS_ERROR_CLM_NODE_STATUS_CHECK_FAILED, "");
	}
}

/*!
	@brief Checks if a specified operation is executable
*/
void ClusterManager::checkCommandStatus(ClusterOperationType operation) {
	if (!statusInfo_.checkCommandStatus(operation)) {
		GS_THROW_USER_ERROR(GS_ERROR_CLM_CLUSTER_OPERATION_CHECK_FAILED, "");
	}
}

/*!
	@brief Updates node status
*/
void ClusterManager::updateNodeStatus(ClusterOperationType operation) {
	statusInfo_.updateNodeStatus(operation);
}

/*!
	@brief Checks node status
*/
bool ClusterManager::NodeStatusInfo::checkNodeStatus() {
	return (isSystemError_ == false && isShutdown_ == false);
}

/*!
	@brief Checks if status match target
*/
bool ClusterManager::NodeStatusInfo::checkCommandStatus(
	ClusterOperationType operation) {
	NodeStatus targetCurrentStatus, targetNextStatus;
	switch (operation) {
	case OP_JOIN_CLUSTER: {
		targetCurrentStatus = SYS_STATUS_INACTIVE;
		targetNextStatus = SYS_STATUS_INACTIVE;
		break;
	}
	case OP_LEAVE_CLUSTER:
	case OP_SHUTDOWN_CLUSTER:
	{
		targetCurrentStatus = SYS_STATUS_ACTIVE;
		targetNextStatus = SYS_STATUS_ACTIVE;
		break;
	}
	case OP_SHUTDOWN_NODE_NORMAL: {
		targetCurrentStatus = SYS_STATUS_INACTIVE;
		targetNextStatus = nextStatus_;
		if (currentStatus_ == SYS_STATUS_ACTIVE) {
			TRACE_CLUSTER_EXCEPTION_FORCE(GS_ERROR_CLM_PENDING_SHUTDOWN,
				"[NOTE] Request shutdown, but current status is ACTIVE(ideal "
				"INACTIVE), do pending.");
			isShutdownPending_ = true;
		}
		break;
	}
	case OP_COMPLETE_CHECKPOINT_FOR_SHUTDOWN: {
		targetCurrentStatus = SYS_STATUS_INACTIVE;
		targetNextStatus = SYS_STATUS_SHUTDOWN_NORMAL;
		break;
	}
	case OP_COMPLETE_CHECKPOINT_FOR_RECOVERY: {
		targetCurrentStatus = SYS_STATUS_BEFORE_RECOVERY;
		targetNextStatus = SYS_STATUS_BEFORE_RECOVERY;
		break;
	}
	case OP_SHUTDOWN_NODE_FORCE: {
		targetCurrentStatus = currentStatus_;
		targetNextStatus = nextStatus_;
		isShutdown_ = true;
		break;
	}
	default: {
		GS_THROW_USER_ERROR(GS_ERROR_CLM_INVALID_OPERAION_TYPE,
			"Invalid operation:" << operation);
	}
	}
	if (currentStatus_ == targetCurrentStatus &&
		nextStatus_ == targetNextStatus) {
		return true;
	}
	else {
		TRACE_CLUSTER_NORMAL_OPERATION(INFO,
			"Cancelled by unacceptable condition (status:" << getSystemStatus()
														   << ")");
		return false;
	}
}

/*!
	@brief Gets node status
*/
const char8_t *ClusterManager::NodeStatusInfo::getSystemStatus() const {
	if (isShutdown_) {
		return "NORMAL_SHUTDOWN";
	}

	if (isSystemError_) {
		return "ABNORMAL";
	}

	switch (currentStatus_) {
	case SYS_STATUS_BEFORE_RECOVERY: {
		return "INACTIVE";
	}
	case SYS_STATUS_INACTIVE: {
		switch (nextStatus_) {
		case SYS_STATUS_INACTIVE:
			return "INACTIVE";
		case SYS_STATUS_ACTIVE:
			return "ACTIVATING";
		case SYS_STATUS_SHUTDOWN_NORMAL:
			return "NORMAL_SHUTDOWN";
		default:
			break;
		}
		break;
	}
	case SYS_STATUS_ACTIVE: {
		switch (nextStatus_) {
		case SYS_STATUS_INACTIVE:
			return "DEACTIVATING";
		case SYS_STATUS_ACTIVE:
			return "ACTIVE";
		case SYS_STATUS_SHUTDOWN_NORMAL:
			break;
		default:
			break;
		}
		break;
	}
	case SYS_STATUS_SHUTDOWN_NORMAL: {
		return "NORMAL_SHUTDOWN";
	}
	default:
		break;
	}

	return "UNKNOWN";
}

/*!
	@brief Updates node status
*/
void ClusterManager::NodeStatusInfo::updateNodeStatus(
	ClusterOperationType operation) {
	switch (operation) {
	case OP_JOIN_CLUSTER: {
		currentStatus_ = SYS_STATUS_ACTIVE;
		nextStatus_ = SYS_STATUS_ACTIVE;
		break;
	}
	case OP_LEAVE_CLUSTER: {
		currentStatus_ = SYS_STATUS_INACTIVE;
		nextStatus_ = SYS_STATUS_INACTIVE;
		break;
	}
	case OP_SHUTDOWN_NODE_NORMAL: {
		nextStatus_ = SYS_STATUS_SHUTDOWN_NORMAL;
		break;
	}
	case OP_COMPLETE_CHECKPOINT_FOR_SHUTDOWN: {
		currentStatus_ = SYS_STATUS_SHUTDOWN_NORMAL;
		isShutdown_ = true;
		break;
	}
	case OP_COMPLETE_CHECKPOINT_FOR_RECOVERY: {
		currentStatus_ = SYS_STATUS_INACTIVE;
		nextStatus_ = SYS_STATUS_INACTIVE;
		break;
	}
	default: {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_PARTITION_STATE_INVALID,
			"Operation:" << operation << " is unacceptable");
	}
	}
	TRACE_CLUSTER_NORMAL(
		INFO, "Status updated (current:" << getSystemStatus() << ")");
};

/*!
	@brief Checks cluster status
*/
void ClusterManager::checkClusterStatus(ClusterOperationType operation) {
	switch (operation) {
	case OP_COMPLETE_CHECKPOINT_FOR_RECOVERY:
	case OP_COMPLETE_CHECKPOINT_FOR_SHUTDOWN:
	case OP_SHUTDOWN_NODE_FORCE:
	case OP_SHUTDOWN_NODE_NORMAL:
	case OP_JOIN_CLUSTER:
	case OP_CHANGE_PARTITION_STATUS: {
		break;
	}
	default: {
		if (!isJoinCluster()) {
			GS_THROW_USER_ERROR(
				GS_ERROR_CLM_CLUSTER_STATUS_NOT_JOIN_CLUSTER, "");
		}
	}
	}

	switch (operation) {
	case OP_TIMER_CHECK_CLUSTER:
	case OP_LEAVE_CLUSTER:
	case OP_SHUTDOWN_NODE_NORMAL:
	case OP_SHUTDOWN_NODE_FORCE:
	case OP_COMPLETE_CHECKPOINT_FOR_SHUTDOWN:
	case OP_CHANGE_PARTITION_STATUS:
	case OP_CHANGE_PARTITION_TABLE: {
		break;
	}
	case OP_UPDATE_PARTITION:
	case OP_TIMER_CHECK_LOAD_BALANCE:
	case OP_ORDER_DROP_PARTITION: {
		if (pt_->isSubMaster()) {
			GS_THROW_USER_ERROR(GS_ERROR_CLM_CLUSTER_STATUS_IS_SUBMASTER, "");
		}
		break;
	}
	case OP_TIMER_NOTIFY_CLUSTER:
	case OP_HEARTBEAT_RES:
	case OP_NOTIFY_CLUSTER:
	case OP_NOTIFY_CLUSTER_RES:
	{
		if (pt_->isFollower()) {
			GS_THROW_USER_ERROR(GS_ERROR_CLM_CLUSTER_STATUS_IS_FOLLOWER, "");
		}
		break;
	}
	case OP_SHUTDOWN_CLUSTER: {
		if (!pt_->isMaster()) {
			GS_THROW_USER_ERROR(GS_ERROR_CLM_CLUSTER_STATUS_NOT_MASTER, "");
		}
		break;
	}
	case OP_TIMER_NOTIFY_CLIENT:
	case OP_GOSSIP:
	{
		if (!pt_->isMaster()) {
			GS_THROW_USER_ERROR(GS_ERROR_CLM_CLUSTER_STATUS_NOT_MASTER, "");
		}
		break;
	}
	case OP_HEARTBEAT: {
		if (pt_->isMaster()) {
			GS_THROW_USER_ERROR(GS_ERROR_CLM_CLUSTER_STATUS_NOT_FOLLOWER, "");
		}
		if (!isHeartbeatRes()) {
			GS_THROW_USER_ERROR(GS_ERROR_CLM_UNPREPARED_HEARTBEAT, "");
		}
		break;
	}
	case OP_JOIN_CLUSTER: {
		if (isJoinCluster()) {
			GS_THROW_USER_ERROR(
				GS_ERROR_CLM_CLUSTER_STATUS_ALREADY_JOIN_CLUSTER, "");
		}
		break;
	}
	case OP_COMPLETE_CHECKPOINT_FOR_RECOVERY: {
		if (!pt_->isSubMaster()) {
			GS_THROW_USER_ERROR(GS_ERROR_CLM_CLUSTER_STATUS_NOT_SUBMASTER, "");
		}
		break;
	}
	default: {
		GS_THROW_USER_ERROR(GS_ERROR_CLM_INVALID_OPERAION_TYPE,
			"Invalid operation:" << operation);
	}
	}
}

/*!
	@brief Updates cluster status
*/
void ClusterManager::updateClusterStatus(
	ClusterStatusTransition status, bool isLeave) {
	try {
		const std::string &prevStatus = pt_->dumpCurrentClusterStatus();
		switch (status) {
		case TO_SUBMASTER: {
			TRACE_CLUSTER_EXCEPTION_FORCE(GS_ERROR_CLM_STATUS_TO_SUBMASTER,
				"[NOTE] Demote to submaster, current:" << prevStatus
													   << ", next:SUB_MASTER.");

			setNotifyPendingCount();
			pt_->clearAll(isLeave);
			pt_->setMaster(UNDEF_NODEID);
			clearActiveNodeList();
			if (isLeave) {
				setJoinCluster(false);
			}
			setHeartbeatRes(false);
			pt_->resizeMasterInfo(0);
			pt_->resetBlockQueue();
			checkCheckpointDelayLimitTime();
			break;
		}
		case TO_MASTER: {
			TRACE_CLUSTER_EXCEPTION_FORCE(GS_ERROR_CLM_STATUS_TO_SUBMASTER,
				"[NOTE] Promote to master, current:" << prevStatus
													 << ", next:MASTER.");

			pt_->setMaster(0);
			setInitialCluster(false);
			pt_->resizeMasterInfo(pt_->getNodeNum());
			break;
		}
		case KEEP: {
			break;
		}
		default: {
			GS_THROW_USER_ERROR(
				GS_ERROR_CLM_INVALID_CLUSTER_TRANSITION_TYPE, "");
		}
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Gets HeartbeatInfo
*/
void ClusterManager::get(HeartbeatInfo &heartbeatInfo) {
	try {
		heartbeatInfo.set(clusterInfo_);

		if (pt_->isMaster()) {
			heartbeatInfo.setMaxLsnList(pt_->getMaxLsnList());
		}

		if (heartbeatInfo.isAddNewNode()) {
			int32_t nodeNum = pt_->getNodeNum();
			for (NodeId nodeId = 0; nodeId < nodeNum; nodeId++) {
				AddressInfo addressInfo(
					pt_->getNodeInfo(nodeId).getNodeAddress(CLUSTER_SERVICE),
					pt_->getNodeInfo(nodeId).getNodeAddress(
						TRANSACTION_SERVICE),
					pt_->getNodeInfo(nodeId).getNodeAddress(SYNC_SERVICE),
					pt_->getNodeInfo(nodeId).getNodeAddress(SYSTEM_SERVICE)
						);
				heartbeatInfo.addNodeAddress(addressInfo);
			}
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Sets HeartbeatInfo
*/
void ClusterManager::set(HeartbeatInfo &heartbeatInfo) {
	try {
		bool isParentMaster = heartbeatInfo.isParentMaster();
		NodeId senderNodeId = heartbeatInfo.getSenderNodeId();

		int64_t baseTime = getMonotonicTime();
		pt_->setHeartbeatTimeout(0, nextHeartbeatTime(baseTime));

		if (pt_->isMaster()) {
			for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
				pt_->setLsnWithCheck(pId, pt_->getLSN(pId));
				pt_->setMaxLsn(pId, pt_->getLSN(pId));
			}
		}
		else if (pt_->isFollower()) {
			pt_->setHeartbeatTimeout(senderNodeId, nextHeartbeatTime(baseTime));

			NodeId masterNodeId = pt_->getMaster();
			if (senderNodeId != masterNodeId && senderNodeId != 0) {
				TRACE_CLUSTER_NORMAL(INFO, "Change master node, prev:"
											   << masterNodeId
											   << ", current:" << senderNodeId);
				pt_->setMaster(senderNodeId);
			}

			std::vector<LogSequentialNumber> &maxLsnList =
				heartbeatInfo.getMaxLsnList();
			for (PartitionId pId = 0; pId < maxLsnList.size(); pId++) {
				pt_->setMaxLsn(pId, maxLsnList[pId]);
			}

			pt_->updatePartitionRevision(
				heartbeatInfo.getPartitionRevisionSequentialNumber());

			NodeAddress &secondMasterNode = heartbeatInfo.getSecondMaster();
			if (secondMasterNode.isValid()) {
				setSecondMaster(secondMasterNode);
			}
			else {
			}

			if (heartbeatInfo.getReserveNum() > 0) {
				setReserveNum(heartbeatInfo.getReserveNum());
			}
			if (isParentMaster) {
				setInitialCluster(false);
			}

			if (checkParentMaster() && !isParentMaster) {
				TRACE_CLUSTER_EXCEPTION_FORCE(
					GS_ERROR_CLM_RECEIVE_SUBMASTER_HEARTBEAT,
					"[NOTE] Receive heartbeat message from subMaster.");
				heartbeatInfo.setStatusChange();
			}
			setParentMaster(isParentMaster);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Gets HeartbeatResInfo
*/
void ClusterManager::get(
	HeartbeatResInfo &heartbeatResInfo, bool isIntialCluster) {
	try {
		PartitionId pId;
		for (pId = 0; pId < pt_->getPartitionNum(); pId++) {
			if (pt_->getLSN(pId) > 0 ||
				pt_->isOwnerOrBackup(pId, 0, PartitionTable::PT_CURRENT_OB) ||
				pt_->isOwnerOrBackup(pId, 0, PartitionTable::PT_NEXT_OB) ||
				pt_->isBackup(pId, 0, PartitionTable::PT_NEXT_GOAL) ||
				pt_->isDrop(pId)) {
				heartbeatResInfo.add(pId, pt_->getPartitionStatus(pId),
					pt_->getLSN(pId), pt_->getPartitionRoleStatus(pId));
			}
		}

		if (isIntialCluster) {
			for (pId = 0; pId < pt_->getPartitionNum(); pId++) {
				heartbeatResInfo.add(pId, pt_->getStartLSN(pId));
			}
		}
		SyncStat &stat = getSyncStat();
		heartbeatResInfo.set(stat);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Sets HeartbeatResInfo
*/
void ClusterManager::set(HeartbeatResInfo &heartbeatResInfo) {
	try {
		NodeId senderNodeId = heartbeatResInfo.getSenderNodeId();
		std::vector<HeartbeatResValue> &heartbeatResValueList =
			heartbeatResInfo.getHeartbeatResValueList();

		std::vector<HeartbeatResStartValue> &heartbeatResStartValueList =
			heartbeatResInfo.getHeartbeatResStartValueList();

		PartitionId pId;
		size_t pos = 0;

		if (heartbeatResInfo.getType() == 1) {
			for (pos = 0; pos < heartbeatResStartValueList.size(); pos++) {
				HeartbeatResStartValue &resValue =
					heartbeatResStartValueList[pos];
				pId = resValue.pId_;
				pt_->setStartLSN(pId, resValue.lsn_, senderNodeId);
			}
			return;
		}

		PartitionStatus status;
		PartitionRoleStatus roleStatus;
		int64_t baseTime = getMonotonicTime();
		pt_->setHeartbeatTimeout(senderNodeId, nextHeartbeatTime(baseTime));
		pt_->setAckHeartbeat(senderNodeId, true);

		for (pos = 0; pos < heartbeatResValueList.size(); pos++) {
			HeartbeatResValue &resValue = heartbeatResValueList[pos];

			if (!resValue.validate(pt_->getPartitionNum())) {
				TRACE_CLUSTER_NORMAL(WARNING, resValue.dump());
				continue;
			}

			pId = resValue.pId_;
			status = static_cast<PartitionStatus>(resValue.status_);
			roleStatus = static_cast<PartitionRoleStatus>(resValue.roleStatus_);
			TRACE_CLUSTER_NORMAL(DEBUG, resValue.dump());

			pt_->setLsnWithCheck(pId, resValue.lsn_, senderNodeId);
			pt_->setMaxLsn(pId, resValue.lsn_);

			pt_->setPartitionStatus(pId, status, senderNodeId);

			pt_->setPartitionRoleStatus(pId, roleStatus, senderNodeId);

		}

		for (pos = 0; pos < heartbeatResStartValueList.size(); pos++) {
			HeartbeatResStartValue &resValue = heartbeatResStartValueList[pos];
			pId = resValue.pId_;
			pt_->setStartLSN(pId, resValue.lsn_, senderNodeId);
		}

		heartbeatResInfo.setSyncInfo(getSyncStat());
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Adds value to HeartbeatResInfo
*/
void ClusterManager::HeartbeatResInfo::add(PartitionId pId,
	PartitionStatus status, LogSequentialNumber lsn,
	PartitionRoleStatus roleStatus) {
	HeartbeatResValue resValue(pId, status, lsn, roleStatus);
	heartbeatResValues_.push_back(resValue);
}

/*!
	@brief Adds startLSN to HeartbeatResInfo
*/
void ClusterManager::HeartbeatResInfo::add(
	PartitionId pId, LogSequentialNumber lsn) {
	HeartbeatResStartValue resValue(pId, lsn);
	heartbeatResStartValues_.push_back(resValue);
}

/*!
	@brief Gets HeartbeatCheckInfo
*/
void ClusterManager::get(HeartbeatCheckInfo &heartbeatCheckInfo) {
	try {
		int64_t currentTime = getMonotonicTime();
		ClusterStatusTransition nextTransition = KEEP;
		bool isAddNewNode = false;

		if (!pt_->isFollower()) {
			pt_->setHeartbeatTimeout(0, nextHeartbeatTime(currentTime));

			std::vector<NodeId> &activeNodeList =
				heartbeatCheckInfo.getActiveNodeList();
			int32_t nodeNum = pt_->getNodeNum();
			activeNodeList.reserve(nodeNum);

			if (pt_->getLiveNodeIdList(activeNodeList, currentTime, true) &&
				pt_->isMaster()) {
				std::set<NodeId> downNodeSet;
				pt_->getDownNodes(downNodeSet, false);

				for (std::set<NodeId>::iterator it = downNodeSet.begin();
					 it != downNodeSet.end(); it++) {
					NodeId follower = *it;
					TRACE_CLUSTER_EXCEPTION_FORCE(
						GS_ERROR_CLM_DETECT_HEARTBEAT_TO_FOLLOWER,
						"[NOTE] Follower heartbeat timeout, follower:"
							<< pt_->dumpNodeAddress(follower));
				}
			}

			int32_t currentActiveNodeNum =
				static_cast<int32_t>(activeNodeList.size());

			int32_t checkedNodeNum;
			bool isCheckedOk = false;

			if (isInitialCluster()) {
				checkedNodeNum = getIntialClusterNum();
			}
			else {
				checkedNodeNum = getQuorum();
			}
			if (checkedNodeNum > 0 && checkedNodeNum <= currentActiveNodeNum) {
				isCheckedOk = true;
			}
			int32_t prevActiveNodeNum = getActiveNum();

			TRACE_CLUSTER_NORMAL(DEBUG,
				"CurrentActiveNum:" << currentActiveNodeNum
									<< ", prevActiveNum:" << prevActiveNodeNum
									<< ", checkedNum:" << checkedNodeNum);

			if (pt_->isMaster() && !isCheckedOk) {
				nextTransition = TO_SUBMASTER;
				TRACE_CLUSTER_NORMAL_OPERATION(
					INFO, "[NOTE] Detect cluster status change, active:"
							  << currentActiveNodeNum
							  << ", quorum:" << checkedNodeNum);
			}
			else if (pt_->isSubMaster() && isCheckedOk) {
				nextTransition = TO_MASTER;
				TRACE_CLUSTER_NORMAL_OPERATION(
					INFO, "[NOTE] Detect cluster status change, active:"
							  << currentActiveNodeNum
							  << ", checkedNum:" << checkedNodeNum);
			}
			else {
				nextTransition = KEEP;
			}

			if (currentActiveNodeNum != 1 &&
				currentActiveNodeNum > prevActiveNodeNum) {
				TRACE_CLUSTER_NORMAL(
					WARNING, "Detect new active node, "
								 << "currentActiveNum:" << currentActiveNodeNum
								 << ", prevActiveNum:" << prevActiveNodeNum);
				isAddNewNode = true;
			}

			setActiveNodeList(activeNodeList);
		}
		else {
			if (pt_->getHeartbeatTimeout(0) < currentTime) {
				TRACE_CLUSTER_EXCEPTION_FORCE(
					GS_ERROR_CLM_DETECT_HEARTBEAT_TO_MASTER,
					"[NOTE] Master heartbeat timeout, master:"
						<< pt_->dumpNodeAddress(pt_->getMaster())
						<< ", current:" << getTimeStr(currentTime) << ", last:"
						<< getTimeStr(pt_->getHeartbeatTimeout(0)));
				nextTransition = TO_SUBMASTER;
			}
		}
		heartbeatCheckInfo.set(nextTransition, isAddNewNode);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Sets UpdatePartitionInfo
*/
void ClusterManager::set(UpdatePartitionInfo &updatePartitionInfo) {
	try {
		util::StackAllocator &alloc = updatePartitionInfo.getAllocator();

		std::set<NodeId> addNodeSet, downNodeSet;
		pt_->getAddNodes(addNodeSet);
		pt_->getDownNodes(downNodeSet);
		bool isDownNode = false;
		if (downNodeSet.size() > 0) {
			TRACE_CLUSTER_NORMAL_OPERATION(
				INFO, "[INFO] Detect down nodes, count:"
						  << downNodeSet.size()
						  << ", nodes:" << pt_->dumpNodeSet(downNodeSet));
			isDownNode = true;
		}
		if (addNodeSet.size() > 0) {
			TRACE_CLUSTER_NORMAL(INFO, "[INFO] Detect new nodes, count:"
										   << addNodeSet.size() << ", nodes:"
										   << pt_->dumpNodeSet(addNodeSet));
		}

		for (std::set<NodeId>::iterator it = downNodeSet.begin();
			 it != downNodeSet.end(); it++) {
			NodeId downNodeId = *it;
			for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
				pt_->setPartitionStatus(
					pId, PartitionTable::PT_OFF, downNodeId);
				pt_->setPartitionRoleStatus(
					pId, PartitionTable::PT_NONE, downNodeId);
			}
		}

		PartitionTable::PartitionContext context(
			alloc, pt_, updatePartitionInfo.isSync());
		context.changedNodeSet_ = updatePartitionInfo.getFilterNodeSet();
		context.setRepairPartition(isRepairPartition());
		context.isDownNode_ = isDownNode;

		PartitionId pId;

		for (pId = 0; pId < pt_->getPartitionNum(); pId++) {
			pt_->checkRelation(context, pId,
				clusterConfig_.getLongTermTimeoutInterval(),
				clusterConfig_.getOwnerCatchupPromoteCheckInterval(),
				clusterConfig_.getHeartbeatInterval() * 2);
		}

		bool isSync = context.isSync_;
		util::XArray<PartitionId> &pIdList = context.getShortTermPIdList();

		if (addNodeSet.size() != 0 || downNodeSet.size() != 0 ||
			updatePartitionInfo.isSync()) {
			for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
				pIdList.push_back(pId);
			}
			isSync = true;
			pt_->clearGossip();
		}
		else {
			pt_->getSyncTargetPIdList(pIdList);
			if (pIdList.size() == 0) {
				isSync = false;
			}
			else {
				isSync = true;
			}
		}

		TRACE_CLUSTER_NORMAL(INFO, "Before filter context: " << context.dump());


		if (isSync) {
			updatePartitionInfo.setSync(true);
			SubPartitionTable &subPartitionTable =
				updatePartitionInfo.getSubPartitionTable();

			pt_->createNextPartition(context);

			if (pt_->isRepairedPartition()) {
				updatePartitionInfo.setMaxLsnList(pt_->getMaxLsnList());
			}

			int64_t baseTime = getMonotonicTime();
			int32_t slackTime = clusterConfig_.getHeartbeatInterval() * 2;
			int64_t shortTermSyncLimitTime =
				baseTime + clusterConfig_.getShortTermTimeoutInterval() +
				slackTime * 2;
			int64_t longTermSyncLimitTime =
				baseTime + clusterConfig_.getLongTermTimeoutInterval() +
				slackTime;
			context.setSyncLimitTime(
				shortTermSyncLimitTime, longTermSyncLimitTime);

			pt_->filter(context, updatePartitionInfo.getFilterNodeSet(),
				subPartitionTable, true);
			TRACE_CLUSTER_NORMAL(
				INFO, "After filter1 context: " << context.dump());


			if (checkLoadBalance()) {
				pt_->createCatchupPartition(context);
			}
			else {
				TRACE_CLUSTER_NORMAL_OPERATION(INFO,
					"[NOTE] Skip create goal partitions, load balancer is not "
					"active");
				pt_->createCatchupPartition(context, true);
			}

			pt_->filter(context, updatePartitionInfo.getFilterNodeSet(),
				subPartitionTable, false);
			TRACE_CLUSTER_NORMAL(
				INFO, "After filter2 context: " << context.dump());

			for (int32_t pos = 0;
				 pos <
				 static_cast<int32_t>(context.changedLongTermPIdList_.size());
				 pos++) {
				pt_->updatePartitionRole(context.changedLongTermPIdList_[pos],
					PartitionTable::PT_NEXT_GOAL);
			}

			TRACE_CLUSTER_NORMAL(WARNING, pt_->dumpNodes());
			TRACE_CLUSTER_NORMAL(WARNING,
				pt_->dumpPartitions(alloc, PartitionTable::PT_CURRENT_OB));
			TRACE_CLUSTER_NORMAL(
				WARNING, pt_->dumpPartitions(alloc, PartitionTable::PT_TMP_OB));
			TRACE_CLUSTER_NORMAL(WARNING, pt_->dumpDatas(true));
			TRACE_CLUSTER_NORMAL(INFO,
				pt_->dumpPartitions(alloc, PartitionTable::PT_CURRENT_GOAL));
			TRACE_CLUSTER_NORMAL(WARNING,
				pt_->dumpPartitions(alloc, PartitionTable::PT_NEXT_GOAL));

			pt_->incPartitionRevision();

			subPartitionTable.revision_ = pt_->getPartitionRevision();

			util::Set<NodeId> &filterNodeSet =
				updatePartitionInfo.getFilterNodeSet();
			if (filterNodeSet.size() == 1 && *filterNodeSet.begin() == 0) {
				updatePartitionInfo.setPending(false);
			}
			else {
				for (NodeId nodeId = 0; nodeId < pt_->getNodeNum(); nodeId++) {
					AddressInfo addressInfo;

					if (pt_->getHeartbeatTimeout(nodeId) != UNDEF_TTL) {
						addressInfo.isActive_ = true;
					}
					else {
						addressInfo.isActive_ = false;
					}

					addressInfo.setNodeAddress(CLUSTER_SERVICE,
						pt_->getNodeInfo(nodeId).getNodeAddress(
							CLUSTER_SERVICE));
					addressInfo.setNodeAddress(TRANSACTION_SERVICE,
						pt_->getNodeInfo(nodeId).getNodeAddress(
							TRANSACTION_SERVICE));
					addressInfo.setNodeAddress(SYNC_SERVICE,
						pt_->getNodeInfo(nodeId).getNodeAddress(SYNC_SERVICE));
					addressInfo.setNodeAddress(
						SYSTEM_SERVICE, pt_->getNodeInfo(nodeId).getNodeAddress(
											SYSTEM_SERVICE));
					subPartitionTable.setNodeAddress(addressInfo);
					TRACE_CLUSTER_NORMAL(
						INFO, "Send node info:" << addressInfo.dump());
				}
			}
			pt_->setFailoverTime();
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Gets NotifyClusterInfo
*/
void ClusterManager::get(NotifyClusterInfo &notifyClusterInfo) {
	try {
		int32_t notifyPendingCount = updateNotifyPendingCount();
		if (notifyPendingCount > 0) {
			GS_THROW_USER_ERROR(GS_ERROR_CLM_CLUSTER_IS_PENDING,
				"Cluster notification message will send after "
					<< notifyPendingCount *
						   clusterConfig_.getNotifyClusterInterval()
					<< " seconds");
		}


		notifyClusterInfo.set(clusterInfo_, pt_);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Sets NotifyClusterInfo
*/
bool ClusterManager::set(NotifyClusterInfo &notifyClusterInfo) {
	try {
		NodeId senderNodeId = notifyClusterInfo.getSenderNodeId();
		bool isFollowerMaster = notifyClusterInfo.isMaster();

		if (getClusterName() != notifyClusterInfo.getClusterName()) {
			GS_THROW_USER_ERROR(GS_ERROR_CLM_UNMATCH_CLUSTER_NAME,
				"Cluster name is not match, self:"
					<< getClusterName()
					<< ", recv:" << notifyClusterInfo.getClusterName());
		}

		if ((!isNewNode() &&
				getReserveNum() != notifyClusterInfo.getReserveNum())) {
			GS_THROW_USER_ERROR(GS_ERROR_CLM_UNMATCH_RESERVE_NUM,
				"Cluster minNodeNum is not match, self:"
					<< getReserveNum()
					<< ", recv:" << notifyClusterInfo.getReserveNum());
		}

		if (getDigest() != notifyClusterInfo.getDigest()) {
			GS_THROW_USER_ERROR(GS_ERROR_CLM_UNMATCH_DIGEST,
				"Cluster digest is not match, self:"
					<< getDigest()
					<< ", recv:" << notifyClusterInfo.getDigest());
		}

		if (pt_->isMaster()) {
			if (isFollowerMaster) {
				detectMutliMaster();
				TRACE_CLUSTER_EXCEPTION_FORCE(GS_ERROR_CLM_DETECT_DOUBLE_MASTER,
					"[NOTE] Detect multi cluster master node.");
				GS_THROW_USER_ERROR(GS_ERROR_CLM_DETECT_DOUBLE_MASTER, "");
			}
			return false;
		}

		if (isFollowerMaster && notifyClusterInfo.isStable()) {
			GS_THROW_USER_ERROR(GS_ERROR_CLM_CLUSTER_FOLLOWER_IS_STABLE,
				"Self node cannot join cluster : master node is stable");
		}

		if (isNewNode() && !isFollowerMaster) {
			GS_THROW_USER_ERROR(GS_ERROR_CLM_NEW_NODE_NEED_TO_FOLLOW_MASTER,
				"Self node cannot join cluster :"
				"self is newNode and received message from subMaster");
		}

		bool notifyFlag = false;
		int64_t startupTime = getStartupTime();
		int64_t recvStartupTime = notifyClusterInfo.getStartupTime();

		if (isFollowerMaster) {
			notifyFlag = true;
		}
		else if (startupTime > recvStartupTime) {
			notifyFlag = true;
		}
		else if (startupTime == recvStartupTime) {
			UTIL_TRACE_WARNING(CLUSTER_SERVICE,
				"Cluster Startup time=" << getTimeStr(startupTime)
										<< " is same, compare node address.");
			if (pt_->dumpNodeAddress(0) > pt_->dumpNodeAddress(senderNodeId)) {
				notifyFlag = true;
			}
		}
		if (notifyFlag) {
			TRACE_CLUSTER_EXCEPTION_FORCE(GS_ERROR_CLM_STATUS_TO_SUBMASTER,
				"[NOTE] Change to cluster status, current:SUB_MASTER, "
				"Next:FOLLOWER");

			int64_t baseTime = getMonotonicTime();
			pt_->setHeartbeatTimeout(0, nextHeartbeatTime(baseTime));
			pt_->setHeartbeatTimeout(senderNodeId, nextHeartbeatTime(baseTime));
			setHeartbeatRes();

			setIntialClusterNum(notifyClusterInfo.getReserveNum());

			int32_t nodeNum = pt_->getNodeNum();
			if (nodeNum > 1) {
				for (NodeId target = 1; target < nodeNum; target++) {
					if (target != senderNodeId) {
						pt_->setHeartbeatTimeout(target, UNDEF_TTL);
					}
				}
			}
			pt_->setMaster(senderNodeId);
		}
		notifyClusterInfo.setFollow(notifyFlag);

		return true;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
};

void ClusterManager::NotifyClusterInfo::set(
	ClusterInfo &clusterInfo, PartitionTable *pt) {
	try {
		isMaster_ = pt->isMaster();
		isStable_ = clusterInfo.isStable();
		reserveNum_ = clusterInfo.reserveNum_;
		startupTime_ = clusterInfo.startupTime_;
		clusterName_ = clusterInfo.clusterName_;
		digest_ = clusterInfo.digest_;
		AddressInfo addressInfo(
			pt->getNodeInfo(0).getNodeAddress(CLUSTER_SERVICE),
			pt->getNodeInfo(0).getNodeAddress(TRANSACTION_SERVICE),
			pt->getNodeInfo(0).getNodeAddress(SYNC_SERVICE),
			pt->getNodeInfo(0).getNodeAddress(SYSTEM_SERVICE)
				);

		nodeList_.push_back(addressInfo);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Gets NotifyClusterResInfo
*/
void ClusterManager::get(NotifyClusterResInfo &notifyClusterResInfo) {
	try {
		notifyClusterResInfo.set(clusterInfo_, pt_);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}
void ClusterManager::NotifyClusterResInfo::set(
	ClusterInfo &clusterInfo, PartitionTable *pt) {
	try {
		reserveNum_ = clusterInfo.reserveNum_;
		startupTime_ = clusterInfo.startupTime_;
		partitionSequentialNumber_ =
			pt->getPartitionRevision().sequentialNumber_;

		for (PartitionId pId = 0; pId < pt->getPartitionNum(); pId++) {
			lsnList_.push_back(pt->getLSN(pId));
			maxLsnList_.push_back(pt->getMaxLsn(pId));
		}

		for (NodeId nodeId = 0; nodeId < pt->getNodeNum(); nodeId++) {
			AddressInfo addressInfo(
				pt->getNodeInfo(nodeId).getNodeAddress(CLUSTER_SERVICE),
				pt->getNodeInfo(nodeId).getNodeAddress(TRANSACTION_SERVICE),
				pt->getNodeInfo(nodeId).getNodeAddress(SYNC_SERVICE),
				pt->getNodeInfo(nodeId).getNodeAddress(SYSTEM_SERVICE)
					);
			nodeList_.push_back(addressInfo);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Sets NotifyClusterResInfo
*/
void ClusterManager::set(NotifyClusterResInfo &notifyClusterResInfo) {
	try {
		NodeId senderNodeId = notifyClusterResInfo.getSenderNodeId();
		int32_t reserveNum = notifyClusterResInfo.getReserveNum();
		std::vector<LogSequentialNumber> &lsnList =
			notifyClusterResInfo.getLsnList();

		util::StackAllocator &alloc = notifyClusterResInfo.getAllocator();
		util::XArray<NodeId> liveNodeIdList(alloc);

		if (checkActiveNodeMax()) {
			pt_->setHeartbeatTimeout(senderNodeId, UNDEF_TTL);
			GS_THROW_USER_ERROR(GS_ERROR_CLM_ALREADY_STABLE,
				"Cluster live node num is already reached to reserved num="
					<< getReserveNum());
		}
		PartitionId pId;

		if (pt_->isMaster()) {
			util::XArray<PartitionId> invalidPIdList(alloc);
			for (pId = 0; pId < static_cast<uint32_t>(lsnList.size()); pId++) {
				if (!pt_->checkMaxLsn(pId, lsnList[pId])) {
					invalidPIdList.push_back(pId);
				}
			}

			if (invalidPIdList.size() >= 1) {
				TRACE_CLUSTER_NORMAL(WARNING,
					"Candidate follower may have already invalid "
					"data:{follower:"
						<< pt_->dumpNodeAddress(senderNodeId) << invalidPIdList
						<< "}");
			}
		}

		int64_t baseTime = getMonotonicTime();
		pt_->setHeartbeatTimeout(0, nextHeartbeatTime(baseTime));
		pt_->setHeartbeatTimeout(senderNodeId, nextHeartbeatTime(baseTime));

		pt_->updatePartitionRevision(
			notifyClusterResInfo.getPartitionSequentialNumber());

		if (getReserveNum() < reserveNum) {
			TRACE_CLUSTER_NORMAL(WARNING,
				"Modify cluster reserver num : " << getReserveNum() << " to "
												 << reserveNum);
			setReserveNum(reserveNum);
		}

		for (pId = 0; pId < static_cast<uint32_t>(lsnList.size()); pId++) {
			pt_->setLSN(pId, lsnList[pId], senderNodeId);
			pt_->setMaxLsn(pId, lsnList[pId]);
		}

		updateSecondMaster(senderNodeId, notifyClusterResInfo.getStartupTime());
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Sets JoinClusterInfo
*/
void ClusterManager::set(JoinClusterInfo &joinClusterInfo) {
	try {
		std::string &clusterName = joinClusterInfo.getClusterName();
		int32_t minNodeNum = joinClusterInfo.getMinNodeNum();

		if (clusterName.size() == 0) {
			joinClusterInfo.setErrorType(WEBAPI_CS_CLUSTERNAME_SIZE_ZERO);
			GS_THROW_USER_ERROR(GS_ERROR_CLM_JOIN_CLUSTER_INVALID_CLUSTER_NAME,
				"Invalid cluster parameter : cluster name size= 0");
		}

		if (static_cast<int32_t>(clusterName.size()) >
			extraConfig_.getMaxClusterNameSize()) {
			joinClusterInfo.setErrorType(WEBAPI_CS_CLUSTERNAME_SIZE_LIMIT);
			GS_THROW_USER_ERROR(GS_ERROR_CLM_JOIN_CLUSTER_INVALID_CLUSTER_NAME,
				"Invalid cluster parameter : cluster name size:"
					<< clusterName.length()
					<< ", limit:" << extraConfig_.getMaxClusterNameSize());
		}

		if (!validateClusterName(clusterName)) {
			joinClusterInfo.setErrorType(WEBAPI_CS_CLUSTERNAME_INVALID);
			GS_THROW_USER_ERROR(GS_ERROR_CLM_JOIN_CLUSTER_INVALID_CLUSTER_NAME,
				"Invalid cluster parameter : invalid cluster name:"
					<< clusterName);
		}

		const std::string &settedClusterName =
			clusterConfig_.getSettedClusterName();
		if (settedClusterName.length() != 0 &&
			settedClusterName != clusterName) {
			joinClusterInfo.setErrorType(WEBAPI_CS_CLUSTERNAME_UNMATCH);
			GS_THROW_USER_ERROR(GS_ERROR_CLM_JOIN_CLUSTER_UNMATCH_CLUSTER_NAME,
				"Cluster name is not match to set cluster name : current:"
					<< clusterName << ", set:" << settedClusterName);
		}

		if (!joinClusterInfo.isPreCheck()) {
			pt_->resizeMasterInfo(minNodeNum * 2);
			setIntialClusterNum(minNodeNum);
			setClusterName(clusterName);
			setReserveNum(minNodeNum);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Sets LeaveClusterInfo
*/
void ClusterManager::set(LeaveClusterInfo &leaveClusterInfo) {
	try {
		if (!leaveClusterInfo.isForceLeave()) {
			for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
				PartitionRole role;
				pt_->getPartitionRole(pId, role);
				if (role.isOwner() && role.getBackupSize() == 0) {
					leaveClusterInfo.setErrorType(
						WEBAPI_CS_LEAVE_NOT_SAFETY_NODE);
					GS_THROW_USER_ERROR(GS_ERROR_SC_LEAVE_NOT_SAFETY_NODE,
						"Self is not safety node, checked pId:" << pId);
				}
			}
		}

		if (!leaveClusterInfo.isPreCheck()) {
			clusterInfo_.initLeave();
		}
		return;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Gets the list of nodes can leave cluster
*/
void ClusterManager::getSafetyLeaveNodeList(util::StackAllocator &alloc,
	std::vector<NodeId> &candList, int32_t removeNodeNum) {
	try {
		int32_t nodeNum = pt_->getNodeNum();
		util::XArray<bool> candFlagList(alloc);
		candFlagList.assign(nodeNum, true);

		int32_t backupCount = 0;
		NodeId owner;
		int32_t target;
		for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
			if (!pt_->isActive(pId)) {
				continue;
			}

			owner = pt_->getOwner(pId);
			if (owner == UNDEF_NODEID) continue;

			util::XArray<NodeId> backups(alloc);
			pt_->getBackup(pId, backups);

			if (backups.size() == 0) continue;

			backupCount = 0;
			for (target = 0; target < static_cast<int32_t>(backups.size());
				 target++) {
				if (pt_->getPartitionStatus(pId, backups[target]) ==
						PartitionTable::PT_ON &&
					pt_->checkLiveNode(backups[target])) {
					backupCount++;
				}
			}
			if (backupCount == 0) {
				candFlagList[owner] = false;
			}
			UTIL_TRACE_INFO(CLUSTER_SERVICE,
				"pId:" << pId << ", target:" << target << ", backupCount:"
					   << backupCount << ", flag:" << candFlagList[owner]);
		}

		int32_t quorum = getQuorum();

		util::XArray<NodeId> activeNodeList(alloc);
		pt_->getLiveNodeIdList(activeNodeList);

		int32_t activeNum = static_cast<int32_t>(activeNodeList.size());
		UTIL_TRACE_INFO(
			CLUSTER_SERVICE, "quorum:" << quorum << ", active:" << activeNum);

		for (target = 1; target < activeNum; target++) {
			UTIL_TRACE_INFO(CLUSTER_SERVICE,
				"check active:" << activeNum << ", target:" << target
								<< ", flag:" << candFlagList[target]
								<< ", live:" << pt_->checkLiveNode(target));
			if (candFlagList[target] && pt_->checkLiveNode(target)) {
				if (activeNum - 1 < quorum) {
					break;
				}
				candList.push_back(target);
				if (static_cast<int32_t>(candList.size()) == removeNodeNum) {
					break;
				}
				activeNum--;
			}
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Gets GossipInfo
*/
void ClusterManager::get(GossipInfo &gossipInfo) {
	try {
		PartitionId pId;
		NodeId nodeId;
		GossipType gossipType;

		gossipInfo.get(pId, nodeId, gossipType);
		NodeAddress address = pt_->getNodeInfo(nodeId).getNodeAddress();
		if (address.isValid()) {
			gossipInfo.setAddress(address);
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_CLM_INVALID_NODE_ID, "");
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Sets GossipInfo
*/
void ClusterManager::set(GossipInfo &gossipInfo) {
	try {
		PartitionId pId = UNDEF_PARTITIONID;
		NodeId nodeId = UNDEF_NODEID;
		GossipType gossipType;
		gossipInfo.get(pId, nodeId, gossipType);

		if (pId == UNDEF_PARTITIONID && nodeId >= 0 &&
			nodeId < pt_->getNodeNum()) {
			TRACE_CLUSTER_NORMAL_OPERATION(INFO,
				"[INFO] Receive gossip, target down node:"
					<< pt_->dumpNodeAddress(nodeId) << ", nodeId:" << nodeId);

			pt_->setHeartbeatTimeout(nodeId, UNDEF_TTL);
			pt_->setDownNode(nodeId);
			return;
		}

		if (pId >= pt_->getPartitionNum() || nodeId >= pt_->getNodeNum()) {
			GS_THROW_USER_ERROR(GS_ERROR_CLM_INVALID_PARTITION_NUM,
				"Receive gossip is invalid, pId:"
					<< pId << ", check pId:" << pt_->getPartitionNum()
					<< ", nodeId:" << nodeId
					<< ", check nodeId:" << pt_->getNodeNum());
		}
		if (gossipType == GOSSIP_PENDING) {
			TRACE_CLUSTER_NORMAL_OPERATION(INFO,
				"[INFO] Receive gossip, owner backup invalid relation, pId:"
					<< pId);

			pt_->setGossip(pId);
		}
		else {
			if (!pt_->checkLiveNode(nodeId)) {
				TRACE_CLUSTER_NORMAL_OPERATION(
					INFO, "[INFO] Receive gossip, target irregular node:"
							  << pt_->dumpNodeAddress(nodeId) << ", pId:" << pId
							  << ", but this node is not alive");
			}
			else if (pId != UNDEF_PARTITIONID) {
				pt_->setGossip(pId);
				switch (gossipType) {
				case GOSSIP_NORMAL:

					TRACE_CLUSTER_NORMAL_OPERATION(
						INFO, "[INFO] Receive gossip, target irregular node:"
								  << pt_->dumpNodeAddress(nodeId)
								  << ", nodeId:" << nodeId << ", pId:" << pId);

					pt_->setBlockQueue(
						pId, PartitionTable::PT_CHANGE_NEXT_TABLE, nodeId);
					break;

				case GOSSIP_GOAL:

					TRACE_CLUSTER_NORMAL_OPERATION(INFO,
						"[INFO] Receive gossip, in longTerm sync, target "
						"irregular node:"
							<< pt_->dumpNodeAddress(nodeId)
							<< ", nodeId:" << nodeId << ", pId:" << pId);

					pt_->setBlockQueue(
						pId, PartitionTable::PT_CHANGE_GOAL_TABLE, nodeId);
					pt_->setCatchupError();

					break;
				case GOSSIP_GOAL_SELF:

					TRACE_CLUSTER_NORMAL_OPERATION(
						INFO, "[INFO] Receive gossip, target irregular node:"
								  << pt_->dumpNodeAddress(nodeId)
								  << ", nodeId:" << nodeId << ", pId:" << pId);

					pt_->setCatchupError();
					break;

				default:
					TRACE_CLUSTER_NORMAL(
						INFO, "Invalid gossip type:" << gossipType);
					break;
				}
			}
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Sets ChangePartitionStatusInfo
*/
void ClusterManager::set(ChangePartitionStatusInfo &changePartitionStatusInfo) {
	try {
		PartitionId pId;
		PartitionStatus status;
		changePartitionStatusInfo.getPartitionStatus(pId, status);
		switch (status) {
		case PartitionTable::PT_ON:
		case PartitionTable::PT_OFF:
		case PartitionTable::PT_SYNC:
		case PartitionTable::PT_STOP:
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_PARTITION_STATE_INVALID, "");
		}
		pt_->setPartitionStatus(pId, status);
		if (changePartitionStatusInfo.isToSubMaster()) {
			TRACE_CLUSTER_NORMAL(INFO, "Clear role, pId:" << pId);
			pt_->clearRole(pId);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Sets ChangePartitionTableInfo
*/
void ClusterManager::set(ChangePartitionTableInfo &changePartitionTableInfo) {
	try {
		PartitionRole &candRole = changePartitionTableInfo.getPartitionRole();
		PartitionId pId = candRole.getPartitionId();
		PartitionTable::TableType tableType = candRole.getTableType();
		PartitionTable::TableType nextTableType;

		bool isMasterChange = false;
		if (tableType == PartitionTable::PT_NEXT_OB) {
			nextTableType = PartitionTable::PT_CURRENT_OB;
		}
		else if (tableType == PartitionTable::PT_NEXT_GOAL) {
			nextTableType = PartitionTable::PT_CURRENT_GOAL;
		}
		else if (tableType == PartitionTable::PT_CURRENT_OB &&
				 pt_->isMaster()) {
			nextTableType = PartitionTable::PT_CURRENT_OB;
			isMasterChange = true;
			TRACE_CLUSTER_NORMAL(
				WARNING, "Update master partition, role:" << candRole);
		}
		else {
			GS_THROW_USER_ERROR(
				GS_ERROR_CLM_CHANGE_PARTITION_TABLE_SET_CHECK_FAILED,
				"type:" << tableType);
		}

		PartitionRole nextRole;
		pt_->getPartitionRole(pId, nextRole, tableType);

		if (nextRole.getRevision() > candRole.getRevision()) {
			if (tableType == PartitionTable::PT_CURRENT_OB && pt_->isMaster()) {
			}
			else {
				GS_THROW_USER_ERROR(
					GS_ERROR_CLM_CHANGE_PARTITION_TABLE_SET_CHECK_FAILED,
					"Next revision:" << nextRole.getRevision().toString()
									 << ", cand revision:"
									 << candRole.getRevision().toString());
			}
		}
		pt_->setPartitionRole(pId, candRole, tableType);

		TRACE_CLUSTER_NORMAL(INFO, candRole.dump());
		TRACE_CLUSTER_NORMAL(WARNING, candRole.dumpIds());

		if (isMasterChange) {
			NodeId owner = candRole.getOwner();
			if (owner != UNDEF_NODEID) {
				pt_->setPartitionStatus(pId, PartitionTable::PT_ON, owner);
			}
		}

		pt_->setPartitionRole(pId, candRole, nextTableType);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}


/*!
	@brief Sets notifyPendingCount
*/
void ClusterManager::setNotifyPendingCount() {
	if (pt_->isFollower() && !clusterInfo_.isSecondMaster_) {
		clusterInfo_.notifyPendingCount_ = 1;
	}
	else if (clusterInfo_.isSecondMaster_ || pt_->isMaster()) {
		clusterInfo_.notifyPendingCount_ = 0;
	}
	else {
		clusterInfo_.notifyPendingCount_ = 1;
	}
}

/*!
	@brief Updates second master
*/
void ClusterManager::updateSecondMaster(NodeId target, int64_t startupTime) {
	if (clusterInfo_.secondMasterNodeId_ == UNDEF_NODEID ||
		startupTime < clusterInfo_.secondMasterStartupTime_) {
		clusterInfo_.secondMasterNodeId_ = target;
		clusterInfo_.secondMasterStartupTime_ = startupTime;
		clusterInfo_.secondMasterNodeAddress_ =
			pt_->getNodeInfo(clusterInfo_.secondMasterNodeId_)
				.getNodeAddress(CLUSTER_SERVICE);
	}
}

/*!
	@brief Sets second master
*/
void ClusterManager::setSecondMaster(NodeAddress &secondMasterNode) {
	bool prevStatus = clusterInfo_.isSecondMaster_;
	if (pt_->getNodeInfo(0).getNodeAddress(CLUSTER_SERVICE) ==
		secondMasterNode) {
		clusterInfo_.isSecondMaster_ = true;
	}
	else {
		clusterInfo_.isSecondMaster_ = false;
	}
	if (prevStatus != clusterInfo_.isSecondMaster_) {
		UTIL_TRACE_WARNING(CLUSTER_SERVICE,
			"Change second master to " << clusterInfo_.isSecondMaster_);
	}
}

/*!
	@brief Validates cluster name
*/
bool ClusterManager::validateClusterName(std::string &clusterName) {
	size_t len = clusterName.length();
	if (!isalpha(clusterName[0]) && clusterName[0] != '_') {
		return false;
	}
	for (size_t i = 1; i < len; i++) {
		if (!isalnum(clusterName[i]) && clusterName[i] != '_') {
			return false;
		}
	}
	return true;
}

const char8_t *ClusterManager::NodeStatusInfo::dumpNodeStatus() const {
	if (isShutdown_) {
		return "SHUTDOWN";
	}

	if (isSystemError_) {
		return "ABNORMAL";
	}

	switch (currentStatus_) {
	case SYS_STATUS_INACTIVE:
		switch (nextStatus_) {
		case SYS_STATUS_INACTIVE:
			return "INACTIVE";
		case SYS_STATUS_ACTIVE:
			return "ACTIVATING";
		case SYS_STATUS_SHUTDOWN_NORMAL:
			return "NORMAL_SHUTDOWN";
		default:
			break;
		}
		break;

	case SYS_STATUS_ACTIVE:
		switch (nextStatus_) {
		case SYS_STATUS_INACTIVE:
			return "DEACTIVATING";
		case SYS_STATUS_ACTIVE:
			return "ACTIVE";
		case SYS_STATUS_SHUTDOWN_NORMAL:
			break;
		default:
			break;
		}
		break;

	case SYS_STATUS_SHUTDOWN_NORMAL:
		return "NORMAL_SHUTDOWN";

	default:
		break;
	}

	return "UNKNOWN";
}

void ClusterManager::detectMutliMaster() {
	statusInfo_.clusterErrorStatus_.detectMutliMaster_ = true;
}

/*!
	@brief Sets the digest of cluster information
*/
#ifdef GD_ENABLE_UNICAST_NOTIFICATION
void ClusterManager::setDigest(const ConfigTable &configTable,
	util::XArray<uint8_t> &digestBinary, NodeAddressSet &addressInfoList,
	ClusterNotificationMode mode)
#else
void ClusterManager::setDigest(const ConfigTable &configTable)
#endif
{
	try {
		ClusterDigest digest;
		memset(static_cast<void *>(&digest), 0, sizeof(ClusterDigest));

		digest.partitionNum_ =
			configTable.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM);
		digest.replicationNum_ =
			configTable.get<int32_t>(CONFIG_TABLE_CS_REPLICATION_NUM);

		util::SocketAddress socketAddress;

#ifdef GD_ENABLE_UNICAST_NOTIFICATION
		if (mode == NOTIFICATION_MULTICAST)
#endif
		{
			socketAddress.assign(configTable.get<const char8_t *>(
									 CONFIG_TABLE_CS_NOTIFICATION_ADDRESS),
				configTable.getUInt16(CONFIG_TABLE_CS_NOTIFICATION_PORT));
			socketAddress.getIP(reinterpret_cast<util::SocketAddress::Inet *>(
									&digest.notificationClusterAddress_),
				&digest.notificationClusterPort_);

			socketAddress.assign(configTable.get<const char8_t *>(
									 CONFIG_TABLE_TXN_NOTIFICATION_ADDRESS),
				configTable.getUInt16(CONFIG_TABLE_TXN_NOTIFICATION_PORT));
			socketAddress.getIP(reinterpret_cast<util::SocketAddress::Inet *>(
									&digest.notificationClientAddress_),
				&digest.notificationClientPort_);

		}

		digest.notificationClusterInterval_ =
			configTable.get<int32_t>(CONFIG_TABLE_CS_NOTIFICATION_INTERVAL);
		digest.heartbeatInterval_ =
			configTable.get<int32_t>(CONFIG_TABLE_CS_HEARTBEAT_INTERVAL);

		digest.replicationMode_ =
			configTable.get<int32_t>(CONFIG_TABLE_TXN_REPLICATION_MODE);
		digest.chunkSize_ =
			configTable.get<int32_t>(CONFIG_TABLE_DS_STORE_BLOCK_SIZE);

		char digestData[SHA256_DIGEST_STRING_LENGTH + 1];

#ifdef GD_ENABLE_UNICAST_NOTIFICATION

		digest.notificationMode_ = mode;

		if (addressInfoList.size() > 0) {
			assert(mode == NOTIFICATION_FIXEDLIST);

			digestBinary.push_back(reinterpret_cast<const uint8_t *>(&digest),
				sizeof(ClusterDigest));

			AddressInfo info;
			int32_t p = 0;
			for (NodeAddressSetItr it = addressInfoList.begin();
				 it != addressInfoList.end(); it++, p++) {
				memset((char *)&info, 0, sizeof(AddressInfo));
				info = (*it);
				digestBinary.push_back(
					(const uint8_t *)&(info), sizeof(AddressInfo));
			}

			SHA256_Data(reinterpret_cast<const uint8_t *>(digestBinary.data()),
				digestBinary.size(), digestData);
			clusterInfo_.digest_ = digestData;
		}
		else {
			SHA256_Data(reinterpret_cast<const uint8_t *>(&digest),
				sizeof(ClusterDigest), digestData);
			clusterInfo_.digest_ = digestData;
		}
#else
		SHA256_Data(reinterpret_cast<const uint8_t *>(&digest),
			sizeof(ClusterDigest), digestData);
		clusterInfo_.digest_ = digestData;
#endif  
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Sets the limit time for delay checkpoint
*/
void ClusterManager::setCheckpointDelayLimitTime(
	PartitionId pId, PartitionGroupId pgId) {
	if (clusterConfig_.getCheckpointDelayLimitInterval() == 0) return;

	int64_t limitTime =
		getMonotonicTime() + clusterConfig_.getCheckpointDelayLimitInterval();

	for (PartitionGroupId pos = 0;
		 pos < static_cast<uint32_t>(delayCheckpointLimitTime_.size()); pos++) {
		TRACE_CLUSTER_NORMAL(
			INFO, "pId:" << pId << ", pgId:" << pos
						 << ", limitTime:" << getTimeStr(limitTime)
						 << ", current:" << getPendingLimitTime(pos));

		if (pgId == pos) {
			delayCheckpointLimitTime_[pos] = limitTime;
			TRACE_CLUSTER_NORMAL(WARNING,
				"Set checkpoint pending by longTerm sync, delay time, pId:"
					<< pId << ", pgId:" << pos
					<< ", limit:" << getTimeStr(limitTime)
					<< ", current:" << getPendingLimitTime(pos));
		}
		else {
			if (delayCheckpointLimitTime_[pos] > 0) {
				TRACE_CLUSTER_NORMAL(WARNING,
					"Reset checkpoint pending, pgId:"
						<< pos << ", current:" << getPendingLimitTime(pos));
			}
			delayCheckpointLimitTime_[pos] = 0;
		}
	}
}

/*!
	@brief Checks and Resets the limit time for delay checkpoint
*/
void ClusterManager::checkCheckpointDelayLimitTime(int64_t checkTime) {
	for (PartitionGroupId pos = 0;
		 pos < static_cast<uint32_t>(delayCheckpointLimitTime_.size()); pos++) {
		TRACE_CLUSTER_NORMAL(
			INFO, "pgId:" << pos << ", check:" << getTimeStr(checkTime)
						  << ", limit:" << getPendingLimitTime(pos));
		if (delayCheckpointLimitTime_[pos] < checkTime) {
			if (delayCheckpointLimitTime_[pos] > 0) {
				TRACE_CLUSTER_NORMAL(WARNING,
					"Reset checkpoint pending, pgId:"
						<< pos << ", current:" << getPendingLimitTime(pos));
			}
			delayCheckpointLimitTime_[pos] = 0;
		}
		TRACE_CLUSTER_NORMAL(
			INFO, "checkpoint pending, pgId:" << pos << ", current:"
											  << getPendingLimitTime(pos));
	}
}

std::string ClusterManager::dump() {
	util::NormalOStringStream ss;
	std::string terminateCode = "\n ";
	std::string tabCode = "\t";

	ss << terminateCode << "[CLUSTER STATUS]" << terminateCode;
	ss << "# cluster name : " << getClusterName() << terminateCode;
	int32_t reserveNum, quorum, activeNum;
	reserveNum = getReserveNum();
	quorum = getQuorum();
	activeNum = getActiveNum();

	ss << "# "
	   << "secondMasterND:" << clusterInfo_.secondMasterNodeId_
	   << ", revisionNo:" << pt_->getPartitionRevision().sequentialNumber_
	   << terminateCode;
	ss << "# "
	   << "active : " << activeNum << ", reserve : " << reserveNum
	   << ", quorum : " << quorum << terminateCode;
	if (isInitialCluster()) {
		ss << "# process : Initializing" << terminateCode;
	}
	else {
		if (activeNum != reserveNum) {
			ss << "# process : not stable" << terminateCode;
		}
		else {
			ss << "# process : stable" << terminateCode;
		}
	}
	ss << "# cluster status : ";
	NodeId masterNo = pt_->getMaster();
	if (masterNo == 0) {
		ss << "master" << terminateCode;
	}
	else if (masterNo == UNDEF_NODEID) {
		ss << "submaster" << terminateCode;
	}
	else {
		if (clusterInfo_.isParentMaster_) {
			ss << "follower (-> master #" << masterNo << " : ";
		}
		else {
			ss << "follower (-> submaster #" << masterNo << " : ";
		}
		ss << pt_->getNodeInfo(masterNo).getNodeAddress().toString() << ")"
		   << terminateCode;
	}
	ss << "# currentTime : "
	   << getTimeStr(util::DateTime::now(TRIM_MILLISECONDS).getUnixTime())
	   << terminateCode;
	ss << "# address list : " << terminateCode;

	ss << pt_->dumpNodes();

	return ss.str();
}

ClusterManager::ConfigSetUpHandler ClusterManager::configSetUpHandler_;

/*!
	@brief Handler Operator
*/
void ClusterManager::ConfigSetUpHandler::operator()(ConfigTable &config) {
	CONFIG_TABLE_RESOLVE_GROUP(config, CONFIG_TABLE_CS, "cluster");

	CONFIG_TABLE_ADD_SERVICE_ADDRESS_PARAMS(config, CS, 10010);

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_CS_REPLICATION_NUM, INT32)
		.setMin(1)
		.setMax(PartitionTable::MAX_NODE_NUM - 1)
		.setDefault(2);

#ifdef GD_ENABLE_UNICAST_NOTIFICATION
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_CS_NOTIFICATION_ADDRESS, STRING)
		.inherit(CONFIG_TABLE_ROOT_NOTIFICATION_ADDRESS)
		.addExclusive(CONFIG_TABLE_CS_NOTIFICATION_ADDRESS)
		.addExclusive(CONFIG_TABLE_CS_NOTIFICATION_PROVIDER)
		.addExclusive(CONFIG_TABLE_CS_NOTIFICATION_MEMBER);
#else
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_CS_NOTIFICATION_ADDRESS, STRING)
		.inherit(CONFIG_TABLE_ROOT_NOTIFICATION_ADDRESS);
#endif

	CONFIG_TABLE_ADD_PORT_PARAM(
		config, CONFIG_TABLE_CS_NOTIFICATION_PORT, 20000);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_CS_NOTIFICATION_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
		.setMin(1)
		.setDefault(5);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_CS_HEARTBEAT_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
		.setMin(1)
		.setDefault(5);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_CS_LOADBALANCE_CHECK_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
		.setMin(1)
		.setDefault(180);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_CS_CLUSTER_NAME, STRING);

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_CS_OWNER_BACKUP_LSN_GAP, INT32)
		.setMin(0)
		.setDefault(50000);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_CS_OWNER_CATCHUP_LSN_GAP, INT32)
		.setMin(0)
		.setDefault(20000);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_CS_MAX_LSN_GAP, INT32)
		.setMin(0)
		.setDefault(0);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_CS_MAX_LSN_REPLICATION_NUM, INT32)
		.setMin(1)
		.setDefault(1000);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_CS_CATCHUP_NUM, INT32)
		.setMin(0)
		.setDefault(1);

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_CS_CONNECTION_LIMIT, INT32)
		.setMin(3)
		.setMax(65536)
		.setDefault(5000);

#ifdef CLUSTER_MOD_WEB_API
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_CS_CHECKPOINT_DELAY_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
		.setMin(0)
		.setDefault(1200);
#endif

#ifdef GD_ENABLE_UNICAST_NOTIFICATION

	picojson::value defaultValue;

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_CS_NOTIFICATION_MEMBER, JSON)
		.setDefault(defaultValue);

	config.resolveGroup(CONFIG_TABLE_CS, CONFIG_TABLE_CS_NOTIFICATION_PROVIDER,
		"notificationProvider");

	config
		.addParam(CONFIG_TABLE_CS_NOTIFICATION_PROVIDER,
			CONFIG_TABLE_CS_NOTIFICATION_PROVIDER_URL, "url")
		.setType(ParamValue::PARAM_TYPE_STRING)
		.setDefault("");
	config
		.addParam(CONFIG_TABLE_CS_NOTIFICATION_PROVIDER,
			CONFIG_TABLE_CS_NOTIFICATION_PROVIDER_UPDATE_INTERVAL,
			"updateInterval")
		.setType(ParamValue::PARAM_TYPE_INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
		.setMin(1)
		.setDefault(0);

#endif  
}

ClusterManager::StatSetUpHandler ClusterManager::statSetUpHandler_;

#define STAT_ADD(id) STAT_TABLE_ADD_PARAM(stat, parentId, id)

/*!
	@brief Handler Operator
*/
void ClusterManager::StatSetUpHandler::operator()(StatTable &stat) {
	StatTable::ParamId parentId;

	parentId = STAT_TABLE_ROOT;
	stat.resolveGroup(parentId, STAT_TABLE_CS, "cluster");

	parentId = STAT_TABLE_CS;
	STAT_ADD(STAT_TABLE_CS_STARTUP_TIME);
	STAT_ADD(STAT_TABLE_CS_CLUSTER_STATUS);
	STAT_ADD(STAT_TABLE_CS_NODE_STATUS);
	STAT_ADD(STAT_TABLE_CS_CLUSTER_NAME);
	STAT_ADD(STAT_TABLE_CS_DESIGNATED_COUNT);
	STAT_ADD(STAT_TABLE_CS_SYNC_COUNT);
	STAT_ADD(STAT_TABLE_CS_PARTITION_STATUS);
	STAT_ADD(STAT_TABLE_CS_LOAD_BALANCER);
	STAT_ADD(STAT_TABLE_CS_INITIAL_CLUSTER);
	STAT_ADD(STAT_TABLE_CS_ACTIVE_COUNT);
	STAT_ADD(STAT_TABLE_CS_NODE_LIST);
	STAT_ADD(STAT_TABLE_CS_MASTER);

#ifdef GD_ENABLE_UNICAST_NOTIFICATION
	STAT_ADD(STAT_TABLE_CS_NOTIFICATION_MODE);
	STAT_ADD(STAT_TABLE_CS_NOTIFICATION_MEMBER);
#endif

	stat.resolveGroup(parentId, STAT_TABLE_CS_ERROR, "errorInfo");

	parentId = STAT_TABLE_CS_ERROR;
	STAT_ADD(STAT_TABLE_CS_ERROR_REPLICATION);
	STAT_ADD(STAT_TABLE_CS_ERROR_CLUSTER_VERSION);
	STAT_ADD(STAT_TABLE_CS_ERROR_ALREADY_APPLIED_LOG);
	STAT_ADD(STAT_TABLE_CS_ERROR_INVALID_LOG);
}

/*!
	@brief Handler Operator
*/
bool ClusterManager::StatUpdator::operator()(StatTable &stat) {
	if (!stat.getDisplayOption(STAT_TABLE_DISPLAY_SELECT_CS)) {
		return true;
	}

	ClusterManager &mgr = *manager_;
	PartitionTable &pt = *mgr.pt_;
	const NodeId masterNodeId = pt.getMaster();

#ifdef GD_ENABLE_UNICAST_NOTIFICATION
	stat.set(STAT_TABLE_CS_NOTIFICATION_MODE, mgr.getNotificationMode());

	ClusterService *clsSvc = mgr.getService();
	ClusterService::NotificationManager &notifyMgr =
		clsSvc->getNotificationManager();

	if (notifyMgr.getMode() != NOTIFICATION_MULTICAST &&
		stat.getDisplayOption(
			STAT_TABLE_DISPLAY_OPTIONAL_NOTIFICATION_MEMBER)) {
		picojson::value value;
		notifyMgr.getNotificationMember(value);
		if (!value.is<picojson::null>()) {
			stat.set(STAT_TABLE_CS_NOTIFICATION_MEMBER, value);
		}
	}
#endif

	stat.set(STAT_TABLE_CS_NODE_STATUS, mgr.statusInfo_.getSystemStatus());
	stat.set(STAT_TABLE_CS_DESIGNATED_COUNT, mgr.getReserveNum());
	stat.set(
		STAT_TABLE_CS_SYNC_COUNT, pt.getPartitionRevision().sequentialNumber_);

	if (!pt.isFollower()) {
		stat.set(STAT_TABLE_CS_ACTIVE_COUNT, mgr.getActiveNum());
	}

	if (stat.getDisplayOption(STAT_TABLE_DISPLAY_WEB_OR_DETAIL_TRACE)) {
		const char8_t *clusterStatus;
		if (masterNodeId == 0) {
			clusterStatus = "MASTER";
		}
		else if (masterNodeId == UNDEF_NODEID) {
			clusterStatus = "SUB_CLUSTER";
		}
		else {
			if (mgr.checkParentMaster()) {
				clusterStatus = "FOLLOWER";
			}
			else {
				clusterStatus = "SUB_FOLLOWER";
			}
		}

		stat.set(STAT_TABLE_CS_CLUSTER_STATUS, clusterStatus);

		const char8_t *partitionStatus;
		switch (pt.getPartitionStat()) {
		case PartitionTable::PT_NORMAL:
			partitionStatus = "NORMAL";
			break;
		case PartitionTable::PT_REPLICA_LOSS:
			partitionStatus = "REPLICA_LOSS";
			break;
		case PartitionTable::PT_NOT_BALANCE:
			partitionStatus = "NOT_BALANCE";
			break;
		case PartitionTable::PT_PARTIAL_STOP:
		case PartitionTable::PT_ABNORMAL:
			partitionStatus = "OWNER_LOSS";
			break;
		default:
			partitionStatus = "UNKNOWN";
			break;
		}

		stat.set(STAT_TABLE_CS_PARTITION_STATUS, partitionStatus);
	}

	if (stat.getDisplayOption(STAT_TABLE_DISPLAY_WEB_ONLY)) {
		ServiceType addressType;
		if (stat.getDisplayOption(STAT_TABLE_DISPLAY_ADDRESS_CLUSTER)) {
			addressType = CLUSTER_SERVICE;
		}
		else if (stat.getDisplayOption(
					 STAT_TABLE_DISPLAY_ADDRESS_TRANSACTION)) {
			addressType = TRANSACTION_SERVICE;
		}
		else if (stat.getDisplayOption(STAT_TABLE_DISPLAY_ADDRESS_SYNC)) {
			addressType = SYNC_SERVICE;
		}
		else {
			addressType = SYSTEM_SERVICE;
		}

		stat.set(STAT_TABLE_CS_STARTUP_TIME, getTimeStr(mgr.getStartupTime()));

		stat.set(STAT_TABLE_CS_CLUSTER_NAME, mgr.getClusterName());

		const char8_t *loadBalancer;
		if (mgr.checkLoadBalance()) {
			loadBalancer = "ACTIVE";
		}
		else {
			loadBalancer = "INACTIVE";
		}
		stat.set(STAT_TABLE_CS_LOAD_BALANCER, loadBalancer);

		if (mgr.isInitialCluster()) {
			stat.set(STAT_TABLE_CS_INITIAL_CLUSTER, 1);
		}

		picojson::array nodeList;

		const int32_t nodeListSize = pt.getNodeNum();
		size_t activeNum = 0;
		for (NodeId nodeId = 0; nodeId < nodeListSize; nodeId++) {
			NodeAddress &address = pt.getNodeInfo(nodeId).getNodeAddress();
			if (address.isValid()) {
				if ((pt.isMaster() && nodeId != 0 &&
						(pt.getHeartbeatTimeout(nodeId) == UNDEF_TTL)) ||
					(pt.isFollower() && nodeId != 0 &&
						nodeId != masterNodeId)) {
					continue;
				}
				picojson::value follower;
				pt.getServiceAddress(nodeId, follower, addressType);
				nodeList.push_back(follower);
				activeNum++;
			}
			else {
				continue;
			}
			if (nodeId == masterNodeId) {
				picojson::value master;
				pt.getServiceAddress(nodeId, master, addressType);
				stat.set(STAT_TABLE_CS_MASTER, master);
			}
		}
		if (activeNum != 0) {
			nodeList.resize(activeNum);
		}
		stat.set(STAT_TABLE_CS_NODE_LIST, picojson::value(nodeList));
	}

	if (stat.getDisplayOption(STAT_TABLE_DISPLAY_WEB_ONLY) &&
		stat.getDisplayOption(STAT_TABLE_DISPLAY_OPTIONAL_CS)) {
		const ErrorCode codeList[] = {GS_ERROR_TXN_CLUSTER_ROLE_UNMATCH,
			GS_ERROR_CS_ENCODE_DECODE_VERSION_CHECK,
			GS_ERROR_RM_ALREADY_APPLY_LOG,
			GS_ERROR_TXN_REPLICATION_LOG_LSN_INVALID};
		const StatTableParamId paramList[] = {STAT_TABLE_CS_ERROR_REPLICATION,
			STAT_TABLE_CS_ERROR_CLUSTER_VERSION,
			STAT_TABLE_CS_ERROR_ALREADY_APPLIED_LOG,
			STAT_TABLE_CS_ERROR_INVALID_LOG};
		for (size_t i = 0; i < sizeof(codeList) / sizeof(*codeList); i++) {
			const uint64_t value = mgr.getErrorCount(codeList[i]);
			if (value > 0) {
				stat.set(paramList[i], value);
			}
		}
	}

	return true;
}

void ClusterManager::initialize(ClusterService *clsSvc) {
	clsSvc_ = clsSvc;
	ee_ = clsSvc->getEE();
	pt_->initialize(this);
	int64_t baseTime = ee_->getMonotonicTime();
	pt_->setHeartbeatTimeout(0, nextHeartbeatTime(baseTime));
}

#ifdef GD_ENABLE_UNICAST_NOTIFICATION

ClusterService *ClusterManager::getService() {
	return clsSvc_;
}

#endif  

#ifdef CLUSTER_MOD_WEB_API
void ClusterManager::Config::setUpConfigHandler(
	ClusterManager *clsMgr, ConfigTable &configTable) {
	clsMgr_ = clsMgr;
	configTable.setParamHandler(
		CONFIG_TABLE_CS_CHECKPOINT_DELAY_INTERVAL, *this);
}

void ClusterManager::Config::operator()(
	ConfigTable::ParamId id, const ParamValue &value) {
	switch (id) {
	case CONFIG_TABLE_CS_CHECKPOINT_DELAY_INTERVAL:
		clsMgr_->getConfig().setCheckpointDelayLimitInterval(
			changeTimeSecToMill(value.get<int32_t>()));
		break;
	}
}
#endif
