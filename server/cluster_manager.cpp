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
#include "picojson.h"
#include "sha2.h"

#include "cluster_service.h"
#include "container_key.h"

#include "uuid_utils.h"


UTIL_TRACER_DECLARE(CLUSTER_SERVICE);
UTIL_TRACER_DECLARE(CLUSTER_OPERATION);
UTIL_TRACER_DECLARE(CLUSTER_DUMP);


#define TEST_PRINT(s)
#define TEST_PRINT1(s, d)


ClusterManager::ClusterManager(const ConfigTable &configTable,
		PartitionTable *partitionTable, ClusterVersionId versionId) :
	clusterConfig_(configTable), clusterInfo_(partitionTable),
		pt_(partitionTable), versionId_(versionId), isSignalBeforeRecovery_(false),
		ee_(NULL), clsSvc_(NULL)
{
	statUpdator_.manager_ = this;
	try {
		clusterInfo_.startupTime_ = util::DateTime::now(TRIM_MILLISECONDS).getUnixTime();
		std::string currentClusterName =
			configTable.get<const char8_t *>(CONFIG_TABLE_CS_CLUSTER_NAME);
		if (currentClusterName.length() > 0) {
			JoinClusterInfo joinInfo(true);
			joinInfo.set(currentClusterName, 0);
			set(joinInfo);
			clusterConfig_.setClusterName(currentClusterName);
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_INVALID,
					"Parameter must be specified (source=gs_cluster.json:gs_node.json, "
				"name=cluster.clusterName)");
		}
		concurrency_ = configTable.get<int32_t>(CONFIG_TABLE_DS_CONCURRENCY);
		ConfigTable *tmpTable = const_cast<ConfigTable*>(&configTable);
		config_.setUpConfigHandler(this, *tmpTable);
		memset(uuid_, 0, 16);
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
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
	case OP_INCREASE_CLUSTER:
	case OP_DECREASE_CLUSTER:
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
						"Pending SHUTDOWN operation, reason=current status is ACTIVE(ideal INACTIVE)");
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
					"Invalid operation=" << static_cast<int32_t>(operation));
	}
	}
	if (currentStatus_ == targetCurrentStatus && nextStatus_ == targetNextStatus) {
		return true;
	}
	else {
		TRACE_CLUSTER_NORMAL_OPERATION(INFO,
				"Cancelled by unacceptable condition (status=" << getSystemStatus() << ")");
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
		isNormalShutdownCall_ = true;
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
				"Operation=" << static_cast<int32_t>(operation) << " is unacceptable");
	}
	}
	TRACE_CLUSTER_NORMAL(
			INFO, "Status updated (current=" << getSystemStatus() << ")");
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
	case OP_INCREASE_CLUSTER:
	case OP_DECREASE_CLUSTER:
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
		if (!pt_->isFollower()) {
			GS_THROW_USER_ERROR(GS_ERROR_CLM_CLUSTER_STATUS_NOT_FOLLOWER, "");
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
				"Invalid operation=" << static_cast<int32_t>(operation));
	}
	}
}

/*!
	@brief Updates cluster status
*/
void ClusterManager::updateClusterStatus(
	ClusterStatusTransition status, bool isLeave) {
	try {
//		const std::string &prevStatus = pt_->dumpCurrentClusterStatus();
		switch (status) {
		case TO_SUBMASTER: {
			TRACE_CLUSTER_EXCEPTION_FORCE(GS_ERROR_CLM_STATUS_TO_SUBMASTER,
						"Cluster status change to SUB_MASTER");
			setNotifyPendingCount();
			clearActiveNodeList();
			if (isLeave) {
				setJoinCluster(false);
			}
				pt_->changeClusterStatus(PartitionTable::SUB_MASTER, 0, 0, 0);
			break;
		}
		case TO_MASTER: {
			TRACE_CLUSTER_EXCEPTION_FORCE(GS_ERROR_CLM_STATUS_TO_MASTER,
						"Cluster status change to MASTER");
			setInitialCluster(false);
				pt_->changeClusterStatus(PartitionTable::MASTER, 0, 0, 0);
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
			heartbeatInfo.setMaxLsnList();
		}
		std::vector<AddressInfo> &addressList = heartbeatInfo.getNodeAddressList();
		pt_->getNodeAddressInfo(addressList);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Sets HeartbeatInfo
*/
bool ClusterManager::set(HeartbeatInfo &heartbeatInfo) {
	try {
		bool isParentMaster = heartbeatInfo.isParentMaster();
		NodeId senderNodeId = heartbeatInfo.getSenderNodeId();
		int64_t baseTime = getMonotonicTime();
		pt_->setHeartbeatTimeout(SELF_NODEID, nextHeartbeatTime(baseTime));
		if (!pt_->isFollower()) {
			for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
				LogSequentialNumber lsn = pt_->getLSN(pId);
				pt_->setLsnWithCheck(pId, lsn);
				pt_->setMaxLsn(pId, lsn);
			}
			pt_->setAckHeartbeat(0, true);
			if (pt_->isMaster()) {
				NodeAddress nodeAddr = heartbeatInfo.getPartition0NodeAddr();
				TEST_PRINT("ClusterManager::set() MASTER\n");
				NodeAddress addressOnMaster = getPartition0NodeAddrOnMaster();
				if (!(nodeAddr == addressOnMaster)) {
					setPartition0NodeAddr(nodeAddr);
					setPartition0NodeAddrOnMaster(nodeAddr);
					TEST_PRINT1("NodeAddress %s set\n", nodeAddr.dump().c_str());
				}
			}
			else {
				return false;
			}
		}
		else {
			NodeAddress nodeAddr = heartbeatInfo.getPartition0NodeAddr();
			TEST_PRINT("ClusterManager::set() FOLLOWER\n");
			NodeAddress addressOnMaster = getPartition0NodeAddrOnMaster();
			if (!(nodeAddr == addressOnMaster)) {
				setPartition0NodeAddr(nodeAddr);
				setPartition0NodeAddrOnMaster(nodeAddr);
				TEST_PRINT1("NodeAddress %s set\n", nodeAddr.dump().c_str());
			}
			NodeId masterNodeId = pt_->getMaster();
			if (senderNodeId != masterNodeId) {
				GS_TRACE_WARNING(CLUSTER_SERVICE, GS_TRACE_CS_OPERATION,
					"Master address check failed, expected=" << pt_->dumpNodeAddress(masterNodeId)
					<< ", actual=" << pt_->dumpNodeAddress(senderNodeId));
				return false;
			}
			pt_->setHeartbeatTimeout(senderNodeId, nextHeartbeatTime(baseTime));
			std::vector<LogSequentialNumber> &maxLsnList = heartbeatInfo.getMaxLsnList();
			for (PartitionId pId = 0; pId < maxLsnList.size(); pId++) {
				pt_->setMaxLsn(pId, maxLsnList[pId]);
			}
			pt_->updatePartitionRevision(heartbeatInfo.getPartitionRevisionNo());
			NodeAddress &secondMasterNode = heartbeatInfo.getSecondMaster();
			if (secondMasterNode.isValid()) {
				setSecondMaster(secondMasterNode);
			}
			if (heartbeatInfo.getReserveNum() > 0) {
				setReserveNum(heartbeatInfo.getReserveNum());
			}
			if (isParentMaster) {
				setInitialCluster(false);
			}
			setParentMaster(isParentMaster);
		}
		return true;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Gets HeartbeatResInfo
*/
void ClusterManager::get(
	HeartbeatResInfo &heartbeatResInfo) {
	try {
		PartitionId pId;
		for (pId = 0; pId < pt_->getPartitionNum(); pId++) {
			heartbeatResInfo.add(pId,
					pt_->getPartitionStatus(pId, SELF_NODEID),
					pt_->getLSN(pId, SELF_NODEID),
					pt_->getStartLSN(pId, SELF_NODEID),
					pt_->getPartitionRoleStatus(pId, SELF_NODEID),
					pt_->getPartitionRevision(pId, SELF_NODEID),
					pt_->getPartitionChunkNum(pId, SELF_NODEID),
					pt_->getOtherStatus(pId, SELF_NODEID),
					pt_->getErrorStatus(pId, SELF_NODEID)
					);
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
		assert(senderNodeId > 0);
		std::vector<HeartbeatResValue> &heartbeatResValueList =
			heartbeatResInfo.getHeartbeatResValueList();
		PartitionId pId;
		size_t pos = 0;

		PartitionStatus status;
		PartitionRoleStatus roleStatus;
		PartitionRevisionNo partitionRevision;
		int64_t chunkNum;
		int32_t otherStatus;
		int32_t errorStatus;
		int64_t baseTime = getMonotonicTime();
		pt_->setHeartbeatTimeout(senderNodeId, nextHeartbeatTime(baseTime));
		pt_->setAckHeartbeat(senderNodeId, true);
		for (pos = 0; pos < heartbeatResValueList.size(); pos++) {
			HeartbeatResValue &resValue = heartbeatResValueList[pos];
			if (!resValue.validate(pt_->getPartitionNum())) {
				continue;
			}
			pId = resValue.pId_;
			status = static_cast<PartitionStatus>(resValue.status_);
			roleStatus = static_cast<PartitionRoleStatus>(resValue.roleStatus_);
			partitionRevision = resValue.partitionRevision_;
			chunkNum = resValue.chunkCount_;
			otherStatus = resValue.otherStatus_;
			errorStatus = resValue.errorStatus_;

			pt_->setLsnWithCheck(pId, resValue.lsn_, senderNodeId);
			pt_->setMaxLsn(pId, resValue.lsn_);
			pt_->setStartLSN(pId, resValue.startLsn_, senderNodeId);
			pt_->setPartitionStatus(pId, status, senderNodeId);
			pt_->setPartitionRoleStatus(pId, roleStatus, senderNodeId);
			pt_->setPartitionRevision(pId, partitionRevision, senderNodeId);
			pt_->setPartitionChunkNum(pId, chunkNum, senderNodeId);
			pt_->setOtherStatus(pId, otherStatus, senderNodeId);
			pt_->setErrorStatus(pId, errorStatus, senderNodeId);
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
		PartitionStatus status,
		LogSequentialNumber lsn,
		LogSequentialNumber startLsn,
		PartitionRoleStatus roleStatus,
		PartitionRevisionNo partitionRevision,
		int64_t chunkCount,
		int32_t otherStatus,  int32_t errorStatus) {
	HeartbeatResValue resValue(pId, status, lsn, startLsn,
			roleStatus, partitionRevision, chunkCount, otherStatus, errorStatus);
	heartbeatResValues_.push_back(resValue);
}

/*!
	@brief Gets HeartbeatCheckInfo
*/
void ClusterManager::get(HeartbeatCheckInfo &heartbeatCheckInfo) {
	try {
		int64_t currentTime = getMonotonicTime();
		ClusterStatusTransition nextTransition = KEEP;
		bool isAddNewNode = false;
		std::vector<NodeId> &activeNodeList = heartbeatCheckInfo.getActiveNodeList();
		util::StackAllocator &alloc = heartbeatCheckInfo.getAllocator();
		if (!pt_->isFollower()) {
			pt_->setHeartbeatTimeout(SELF_NODEID, nextHeartbeatTime(currentTime));
			util::Set<NodeId> downNodeSet(alloc);
			pt_->getActiveNodeList(activeNodeList, downNodeSet, currentTime);
			if (downNodeSet.size() > 0) {
				setAddOrDownNode();
			}
			if (pt_->isMaster()) {
				for (util::Set<NodeId>::iterator it = downNodeSet.begin();
					it != downNodeSet.end(); it++) {
					TRACE_CLUSTER_EXCEPTION_FORCE_ERROR(
						GS_ERROR_CLM_DETECT_HEARTBEAT_TO_FOLLOWER,
							"Follower heartbeat timeout, follower=" << pt_->dumpNodeAddress(*it));
						pt_->resetDownNode((*it));
				}
			}
			int32_t currentActiveNodeNum = static_cast<int32_t>(activeNodeList.size());
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
				if (checkedNodeNum != 1) {
					for (size_t pos = 0; pos < activeNodeList.size(); pos++) {
						if (!pt_->getAckHeartbeat(activeNodeList[pos])) {
							UTIL_TRACE_INFO(CLUSTER_SERVICE,
									"Pending cluster active, not ack heartbeat="
									<< pt_->dumpNodeAddress(activeNodeList[pos]));
							if (pt_->isSubMaster()) {
								isCheckedOk = false;
							}
							break;
						}
					}
				}
			}
			int32_t prevActiveNodeNum = getActiveNum();
			if (pt_->isMaster() && !isCheckedOk) {
				nextTransition = TO_SUBMASTER;
				TRACE_CLUSTER_NORMAL_OPERATION(INFO,
						"Detect cluster status change (active nodeCount="
							<< currentActiveNodeNum
						<< ", quorum=" << checkedNodeNum << ")");
			}
			else if (pt_->isSubMaster() && isCheckedOk) {
				nextTransition = TO_MASTER;
				TRACE_CLUSTER_NORMAL_OPERATION(INFO,
						"Detect cluster status change (active nodeCount="
							<< currentActiveNodeNum
						<< ", checked nodeNum=" << checkedNodeNum << ")");
			}
			else {
				nextTransition = KEEP;
			}
			if (currentActiveNodeNum != 1 && currentActiveNodeNum > prevActiveNodeNum) {
				TRACE_CLUSTER_NORMAL(WARNING,
						"Detect new active node, "<< "currentActiveNum:" << currentActiveNodeNum
								<< ", prevActiveNum:" << prevActiveNodeNum);
				setAddOrDownNode();
				isAddNewNode = true;

			}

			setActiveNodeList(activeNodeList);
		}
		else {
			if (pt_->getHeartbeatTimeout(0) < currentTime) {
				TRACE_CLUSTER_EXCEPTION_FORCE_ERROR(
					GS_ERROR_CLM_DETECT_HEARTBEAT_TO_MASTER,
						"Master heartbeat timeout, master="
						<< pt_->dumpNodeAddress(pt_->getMaster())
						<< ", current_time=" << getTimeStr(currentTime) 
						<< ", last_time=" << getTimeStr(pt_->getHeartbeatTimeout(0)));
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
void ClusterManager::get(UpdatePartitionInfo &updatePartitionInfo) {
	try {
		util::StackAllocator &alloc = updatePartitionInfo.getAllocator();
		util::Set<NodeId> addNodeSet(alloc);
		util::Set<NodeId> downNodeSet(alloc);
		pt_->getAddNodes(addNodeSet);
		pt_->getDownNodes(downNodeSet);
		bool isAddOrDownNode = (addNodeSet.size() > 0 || downNodeSet.size() > 0);



		SubPartitionTable &subPartitionTable = updatePartitionInfo.getSubPartitionTable();
		PartitionTable::PartitionContext context(
				alloc, pt_, updatePartitionInfo.isNeedUpdatePartition(),
				subPartitionTable);
		context.setRepairPartition(isRepairPartition());
		context.setShufflePartition(isShufflePartition());
		context.currentTime_ = getMonotonicTime();
		bool &execBalanceCheck = context.balanceCheck_;
		bool &execDrop = context.enableDrop_;
		int32_t periodicCount 
				= getConfig().getCheckLoadBalanceInterval()  / getConfig().getHeartbeatInterval();
		bool balanceCheck = ((pt_->getPartitionRevisionNo() % periodicCount) == 0);
		bool dropCheck = ((pt_->getPartitionRevisionNo() % (periodicCount*10)) == 0);
		if (!checkLoadBalance()) {
			balanceCheck = false;
			dropCheck = false;
		}

		pt_->check(context, balanceCheck, dropCheck,
				updatePartitionInfo.getDropPartitionNodeInfo());

		bool &needUpdatePartition = context.needUpdatePartition_;
		util::XArray<PartitionId> &pIdList = context.getShortTermPIdList();
		if (isAddOrDownNode ||
			updatePartitionInfo.isNeedUpdatePartition()) {
			execDrop = false;
			for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
				pIdList.push_back(pId);
			}
			needUpdatePartition = true;
			pt_->clearChangePartition();
		}
		else {
			pt_->getChangeTargetPIdList(pIdList);
			if (pIdList.size() == 0) {
				if (execDrop || execBalanceCheck) {
					needUpdatePartition = true;
					if (execBalanceCheck) {
						for (PartitionId pId = 0; pId < pt_->getPartitionNum(); pId++) {
							pIdList.push_back(pId);
						}
					}
				}
				else {
						needUpdatePartition = false;
				}
			}
			else {
				execDrop = false;
				needUpdatePartition = true;
			}
		}

		if (needUpdatePartition) {
			PartitionRevisionNo revisionNo = pt_->getPartitionRevision().sequentialNumber_;
			if (isAddOrDownNode || context.errorReason_ == PARTITION_STATUS_SUCCESS) {
				GS_TRACE_INFO(CLUSTER_OPERATION, GS_TRACE_CS_NORMAL_OPERATION, 
					"Create cluster next partition revision=" << revisionNo 
					<< ", reason=new cluster construction");
			}
			else if (execDrop) {
				GS_TRACE_INFO(CLUSTER_OPERATION, GS_TRACE_CS_NORMAL_OPERATION, 
					"Create cluster next partition revision=" << revisionNo 
					<< ", reason=check drop partition");
			}
			else if (context.balanceCheck_) {
				GS_TRACE_INFO(CLUSTER_OPERATION, GS_TRACE_CS_NORMAL_OPERATION, 
					"Create cluster next partition revision=" << revisionNo 
					<< ", reason=resolve node balance");
			}
			else {
				if (context.errorReason_ == CATCHUP_PROMOTION) {
				GS_TRACE_INFO(CLUSTER_OPERATION, GS_TRACE_CS_NORMAL_OPERATION, 
					"Create cluster next partition revision=" << revisionNo 
					<< ", reason=catchup promotion, pId=" << context.errorPId_);
				}
				else {
//					GS_TRACE_INFO(CLUSTER_OPERATION, GS_TRACE_CS_NORMAL_OPERATION, 
//						"Create cluster next partition revision=" << revisionNo 
//						<< ", reason=detect partition irregular, pId="
//						<< context.errorPId_ << ", reason=" << dumpPartitionReason(context.errorReason_));
					GS_TRACE_INFO(CLUSTER_OPERATION, GS_TRACE_CS_NORMAL_OPERATION, 
						"Create cluster next partition revision=" << revisionNo 
						<< ", reason=detect partition irregular, pId="
						<< context.errorPId_);
				}
			}
			updatePartitionInfo.setNeedUpdatePartition(true);
			for(NodeId nodeId = 0; nodeId < pt_->getNodeNum(); nodeId++) {
				if (pt_->getHeartbeatTimeout(nodeId) == UNDEF_TTL) {
					pt_->resetDownNode(nodeId);
				}
			}
			if (execDrop) {
				context.needUpdatePartition_ = true;
				updatePartitionInfo.getSubPartitionTable().getRevision().sequentialNumber_ = revisionNo;
				return;
			}
			if (addNodeSet.size() != 0 || downNodeSet.size() != 0 || updatePartitionInfo.isAddNewNode()) {
				context.irregularPartitionList_.clear();
			}

			UUIDUtils::generate(uuid_);
			if (!checkLoadBalance()) {
				bool isDown = ((addNodeSet.size() > 0 || downNodeSet.size() > 0) || updatePartitionInfo.isToMaster());
				if (isDown) {
					pt_->createNextPartition(context);
				}
				else {
					TRACE_CLUSTER_NORMAL_OPERATION(INFO,
							"Skip short term sync (Load balancer is not enable)");
				}
			}
			else {
				pt_->createNextPartition(context);
			}

			if (pt_->isRepairedPartition()) {
				updatePartitionInfo.setMaxLsnList();
			}
			int64_t baseTime = getMonotonicTime();
			int64_t shortTermSyncLimitTime =
					baseTime + clusterConfig_.getShortTermTimeoutInterval() + clusterConfig_.getHeartbeatInterval();
			context.setShorTermSyncLimitTime(shortTermSyncLimitTime);

			if (checkLoadBalance()) {
				pt_->createCatchupPartition(context);
			}
			else {
				TRACE_CLUSTER_NORMAL_OPERATION(INFO,
						"Skip long term sync (Load balancer is not enable)");
				pt_->createCatchupPartition(context, true);
			}

			pt_->setSubPartiton(context, subPartitionTable);

			subPartitionTable.revision_ = pt_->getPartitionRevision();
			std::vector<AddressInfo> &nodeList = subPartitionTable.getNodeAddressList();
			pt_->getNodeAddressInfo(nodeList);

			DUMP_CLUSTER(pt_->dumpPartitionsNew(alloc, PartitionTable::PT_CURRENT_OB));
			DUMP_CLUSTER(pt_->dumpPartitionsNew(alloc, PartitionTable::PT_NEXT_OB));


			TRACE_CLUSTER_NORMAL(WARNING, pt_->dumpNodes());
			TRACE_CLUSTER_NORMAL(WARNING,
				pt_->dumpPartitions(alloc, PartitionTable::PT_CURRENT_OB));
			TRACE_CLUSTER_NORMAL(WARNING, pt_->dumpDatas(true));

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
					"Cluster notification message will send after " << notifyPendingCount *
						clusterConfig_.getNotifyClusterInterval() << " seconds");
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
					"Cluster name is not match, self=" << getClusterName()
					<< ", recv=" << notifyClusterInfo.getClusterName());
		}
		if (getDigest() != notifyClusterInfo.getDigest()) {
			GS_THROW_USER_ERROR(GS_ERROR_CLM_UNMATCH_DIGEST,
					"Cluster digest is not match, self=" << getDigest()
					<< ", recv=" << notifyClusterInfo.getDigest());
		}
		if (pt_->isMaster()) {
			if (isFollowerMaster) {
				detectMutliMaster();
				TRACE_CLUSTER_EXCEPTION_FORCE_ERROR(
						GS_ERROR_CLM_DETECT_DOUBLE_MASTER,
						"Detect multiple clusters of same environment, detected node="
						<< pt_->dumpNodeAddress(senderNodeId));
				GS_THROW_USER_ERROR(GS_ERROR_CLM_DETECT_DOUBLE_MASTER, "");
			}
			return false;
		}
		if ((!isNewNode() &&
				getReserveNum() != notifyClusterInfo.getReserveNum())) {
			GS_THROW_USER_ERROR(GS_ERROR_CLM_UNMATCH_RESERVE_NUM,
					"Cluster minNodeNum is not match, self=" << getReserveNum()
					<< ", recv=" << notifyClusterInfo.getReserveNum());
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
			TRACE_CLUSTER_EXCEPTION_FORCE(GS_ERROR_CLM_STATUS_TO_FOLLOWER,
					"Cluster status change to FOLLOWER");
			setIntialClusterNum(notifyClusterInfo.getReserveNum());
			int64_t baseTime = getMonotonicTime();
			pt_->changeClusterStatus(PartitionTable::FOLLOWER, senderNodeId, baseTime, nextHeartbeatTime(baseTime));
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
				pt->getNodeAddress(SELF_NODEID, CLUSTER_SERVICE),
				pt->getNodeAddress(SELF_NODEID, TRANSACTION_SERVICE),
				pt->getNodeAddress(SELF_NODEID, SYNC_SERVICE),
				pt->getNodeAddress(SELF_NODEID, SYSTEM_SERVICE)
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
					pt->getNodeAddress(nodeId, CLUSTER_SERVICE),
					pt->getNodeAddress(nodeId, TRANSACTION_SERVICE),
					pt->getNodeAddress(nodeId, SYNC_SERVICE),
					pt->getNodeAddress(nodeId, SYSTEM_SERVICE)
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
		int64_t baseTime = getMonotonicTime();
		pt_->setHeartbeatTimeout(0, nextHeartbeatTime(baseTime));
		pt_->setHeartbeatTimeout(senderNodeId, nextHeartbeatTime(baseTime));

		pt_->updatePartitionRevision(
				notifyClusterResInfo.getPartitionRevisionNo());

		if (getReserveNum() < reserveNum) {
			TRACE_CLUSTER_NORMAL(WARNING,
				"Modify cluster reserver num=" << getReserveNum() << " to "
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
					"Invalid cluster parameter, cluster name size=0");
		}
		if (static_cast<int32_t>(clusterName.size()) >
			extraConfig_.getMaxClusterNameSize()) {
			joinClusterInfo.setErrorType(WEBAPI_CS_CLUSTERNAME_SIZE_LIMIT);
			GS_THROW_USER_ERROR(GS_ERROR_CLM_JOIN_CLUSTER_INVALID_CLUSTER_NAME,
					"Invalid cluster parameter, cluster name size=" << clusterName.length()
					<< ", limit=" << extraConfig_.getMaxClusterNameSize());
		}
		try {
			NoEmptyKey::validate(
				KeyConstraint::getUserKeyConstraint(extraConfig_.getMaxClusterNameSize()),
				clusterName.c_str(), static_cast<uint32_t>(clusterName.size()),
				"clusterName");
		}
		catch (std::exception &e) {
			joinClusterInfo.setErrorType(WEBAPI_CS_CLUSTERNAME_INVALID);
			GS_THROW_USER_ERROR(GS_ERROR_CLM_JOIN_CLUSTER_INVALID_CLUSTER_NAME,
					"Invalid cluster parameter : invalid cluster name=" << clusterName);
		}
		const std::string &currentClusterName = clusterConfig_.getSetClusterName();
		if (currentClusterName.length() != 0 && currentClusterName != clusterName) {
			joinClusterInfo.setErrorType(WEBAPI_CS_CLUSTERNAME_UNMATCH);
			GS_THROW_USER_ERROR(GS_ERROR_CLM_JOIN_CLUSTER_UNMATCH_CLUSTER_NAME,
					"Cluster name is not match to set cluster name, current="
					<< clusterName << ", setted cluster name=" << currentClusterName);
		}
		if (!joinClusterInfo.isPreCheck()) {
			pt_->resizeCurrenPartition(minNodeNum);
			setIntialClusterNum(minNodeNum);
			setClusterName(currentClusterName);
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
				bool isOwner = pt_->isOwner(pId);
				int32_t backupSize = pt_->getBackupSize(pId);
				if (isOwner && backupSize == 0) {
					leaveClusterInfo.setErrorType(WEBAPI_CS_LEAVE_NOT_SAFETY_NODE);
					GS_THROW_USER_ERROR(GS_ERROR_SC_LEAVE_NOT_SAFETY_NODE,
							"Self node is not safe, checked pId=" << pId);
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
	@brief Sets IncreaseClusterInfo
*/
void ClusterManager::set(IncreaseClusterInfo &increaseClusterInfo) {
	try {
		if (!checkSteadyStatus()) {
			increaseClusterInfo.setErrorType(WEBAPI_CS_CLUSTER_IS_STABLE);
			GS_THROW_USER_ERROR(GS_ERROR_CLM_NOT_STABLE,
				"Cannot increase cluster operation ( Cluster is not stable, reserveNum="
					<< getReserveNum() << ", activeNodeCount=" << getActiveNum() << ")");
		}

		if (!increaseClusterInfo.isPreCheck()) {
			setReserveNum(
				getReserveNum() + increaseClusterInfo.getAddNodeNum());
			TRACE_CLUSTER_NORMAL_OPERATION(
					INFO, "Increase cluster (stable) operation completed (designatedCount=" << getReserveNum() << ")");
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Sets DecreaseClusterInfo
*/
void ClusterManager::set(DecreaseClusterInfo &decreaseClusterInfo) {
	try {
		if (!checkSteadyStatus()) {
			decreaseClusterInfo.setErrorType(WEBAPI_CS_CLUSTER_IS_STABLE);
			GS_THROW_USER_ERROR(GS_ERROR_CLM_NOT_STABLE,
				"Cannot decrease cluster (not stable), reserveNum:"
					<< getReserveNum() << ", activeNodeNum:" << getActiveNum());
		}

		std::vector<NodeId> &leaveNodeList =
			decreaseClusterInfo.getLeaveNodeList();
		if (leaveNodeList.size() == 0) {
			getSafetyLeaveNodeList(
				decreaseClusterInfo.getAllocator(), leaveNodeList);
		}

		if (!decreaseClusterInfo.isPreCheck()) {
			int32_t leaveNodeNum = static_cast<int32_t>(leaveNodeList.size());
			if (getReserveNum() - leaveNodeNum > 0) {
				setReserveNum(getReserveNum() - leaveNodeNum);
				TRACE_CLUSTER_NORMAL_OPERATION(INFO,
						"Decrease cluster (stable) operation completed (designatedCount="
						<< getReserveNum() << ", leaveNodeList="
						<< pt_->dumpNodeAddressList(leaveNodeList) << ")");
			}
		}
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
			owner = pt_->getOwner(pId);
			if (owner == UNDEF_NODEID) {
				continue;
			}
			util::XArray<NodeId> backups(alloc);
			pt_->getBackup(pId, backups);
			if (backups.size() == 0) continue;
			backupCount = 0;
			for (target = 0; target < static_cast<int32_t>(backups.size());target++) {
				if (pt_->checkLiveNode(backups[target])) {
					backupCount++;
				}
			}
			if (backupCount == 0) {
				candFlagList[owner] = false;
			}
		}

		int32_t quorum = getQuorum();

		util::XArray<NodeId> activeNodeList(alloc);
		pt_->getLiveNodeIdList(activeNodeList);

		int32_t activeNum = static_cast<int32_t>(activeNodeList.size());
		for (target = 1; target < activeNum; target++) {
			if (candFlagList[target] && pt_->checkLiveNode(activeNodeList[target])) {
				if (activeNum - 1 < quorum) {
					break;
				}
				candList.push_back(activeNodeList[target]);
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
		NodeAddress &address = pt_->getNodeAddress(nodeId);
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
					"Receive gossip message(node) from node=" << pt_->dumpNodeAddress(nodeId));
			pt_->setHeartbeatTimeout(nodeId, UNDEF_TTL);
			pt_->setDownNode(nodeId);
			return;
		}
		if (pId >= pt_->getPartitionNum() || nodeId >= pt_->getNodeNum()) {
			GS_THROW_USER_ERROR(GS_ERROR_CLM_INVALID_PARTITION_NUM,
					"Invalid gossip message from node=" << pt_->dumpNodeAddress(nodeId));
		}
			if (!pt_->checkLiveNode(nodeId)) {
					TRACE_CLUSTER_NORMAL_OPERATION(INFO,
							"Receive gossip message from node=" << pt_->dumpNodeAddress(nodeId)
							<< ", pId=" << pId << ", but this node is not alive");
			}
			else if (pId != UNDEF_PARTITIONID) {
			pt_->setChangePartition(pId);
				switch (gossipType) {
				case GOSSIP_INVALID_NODE:
					TRACE_CLUSTER_NORMAL_OPERATION(INFO,
							"Receive gossip message(partition) from node=" << pt_->dumpNodeAddress(nodeId)
							<< ", pId=" << pId);
						pt_->setBlockQueue(pId, PartitionTable::PT_CHANGE_NEXT_TABLE, nodeId);
					break;
				case GOSSIP_INVALID_CATHCUP_PARTITION:
					TRACE_CLUSTER_NORMAL_OPERATION(INFO,
							"Receive gossip message(catchup partition) from node=" << pt_->dumpNodeAddress(nodeId)
							<< ", pId=" << pId);
					pt_->setCatchupError(pId);
					break;
				default:
					TRACE_CLUSTER_NORMAL(INFO, "Invalid gossip type=" << static_cast<int32_t>(gossipType));
					break;
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
		int32_t otherStatus = PT_OTHER_NORMAL;
		switch (status) {
		case PartitionTable::PT_ON:
			otherStatus = PT_OTHER_SHORT_SYNC_COMPLETE;
			break;
		case PartitionTable::PT_OFF:
		case PartitionTable::PT_SYNC:
		case PartitionTable::PT_STOP:
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_TXN_PARTITION_STATE_INVALID, "");
		}
		pt_->setPartitionStatus(pId, status);
		pt_->setOtherStatus(pId, otherStatus);
		if (changePartitionStatusInfo.isToSubMaster()) {
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

		if (tableType == PartitionTable::PT_NEXT_OB) {
			nextTableType = PartitionTable::PT_CURRENT_OB;
		}
		else if (tableType == PartitionTable::PT_CURRENT_OB) {
			nextTableType = PartitionTable::PT_CURRENT_OB;
		}
		else {
			GS_THROW_USER_ERROR(0, "");
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
		pt_->setPartitionRole(pId, candRole, nextTableType);
		if (!pt_->isOwnerOrBackup(pId, SELF_NODEID)) {
			pt_->setPartitionStatus(pId, PartitionTable::PT_OFF);
		}
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
				pt_->getNodeAddress(clusterInfo_.secondMasterNodeId_);
	}
}

/*!
	@brief Sets second master
*/

void ClusterManager::setSecondMaster(NodeAddress &secondMasterNode) {
	bool prevStatus = clusterInfo_.isSecondMaster_;
	if (pt_->getNodeAddress(SELF_NODEID) == secondMasterNode) {
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
void ClusterManager::setDigest(const ConfigTable &configTable,
	util::XArray<uint8_t> &digestBinary, NodeAddressSet &addressInfoList,
		ClusterNotificationMode mode) {
	try {
		ClusterDigest digest;
		memset(static_cast<void *>(&digest), 0, sizeof(ClusterDigest));

		digest.partitionNum_ =
			configTable.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM);
		digest.replicationNum_ =
			configTable.get<int32_t>(CONFIG_TABLE_CS_REPLICATION_NUM);

		util::SocketAddress socketAddress;
		if (mode == NOTIFICATION_MULTICAST) {
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
				digestBinary.push_back((const uint8_t *)&(info), sizeof(AddressInfo));
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
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
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
		ss << pt_->getNodeAddress(masterNo).toString() << ")"
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

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_CS_NOTIFICATION_ADDRESS, STRING)
		.inherit(CONFIG_TABLE_ROOT_NOTIFICATION_ADDRESS)
		.addExclusive(CONFIG_TABLE_CS_NOTIFICATION_ADDRESS)
		.addExclusive(CONFIG_TABLE_CS_NOTIFICATION_PROVIDER)
		.addExclusive(CONFIG_TABLE_CS_NOTIFICATION_MEMBER);

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

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_CS_CHECKPOINT_DELAY_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
		.setMin(0)
		.setDefault(1200);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_CS_CATCHUP_PROMOTION_CHECK_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
		.setMin(0)
		.setDefault(0);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_CS_PARTITION_ASSIGN_MODE, INT32)
		.setDefault(0);

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
	STAT_ADD(STAT_TABLE_CS_NOTIFICATION_MODE);
	STAT_ADD(STAT_TABLE_CS_NOTIFICATION_MEMBER);

	STAT_ADD(STAT_TABLE_CS_CLUSTER_REVISION_ID);

	stat.resolveGroup(parentId, STAT_TABLE_CS_ERROR, "errorInfo");

	parentId = STAT_TABLE_CS_ERROR;
	STAT_ADD(STAT_TABLE_CS_ERROR_REPLICATION);
	STAT_ADD(STAT_TABLE_CS_ERROR_CLUSTER_VERSION);
	STAT_ADD(STAT_TABLE_CS_ERROR_ALREADY_APPLIED_LOG);
	STAT_ADD(STAT_TABLE_CS_ERROR_INVALID_LOG);

	parentId = STAT_TABLE_ROOT;
	stat.resolveGroup(parentId, STAT_TABLE_PERF, "performance");
	parentId = STAT_TABLE_PERF;
	STAT_ADD(STAT_TABLE_PERF_OWNER_COUNT);
	STAT_ADD(STAT_TABLE_PERF_BACKUP_COUNT);
	STAT_ADD(STAT_TABLE_PERF_TOTAL_OWNER_LSN);
	STAT_ADD(STAT_TABLE_PERF_TOTAL_BACKUP_LSN);
	STAT_ADD(STAT_TABLE_PERF_TOTAL_OTHER_LSN);
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

	stat.set(STAT_TABLE_CS_NOTIFICATION_MODE, mgr.getNotificationMode());

	ClusterService *clsSvc = mgr.getService();
	ClusterService::NotificationManager &notifyMgr = clsSvc->getNotificationManager();

	if (notifyMgr.getMode() != NOTIFICATION_MULTICAST &&
		stat.getDisplayOption(STAT_TABLE_DISPLAY_OPTIONAL_NOTIFICATION_MEMBER)) {
		picojson::value value;
		notifyMgr.getNotificationMember(value);
		if (!value.is<picojson::null>()) {
			stat.set(STAT_TABLE_CS_NOTIFICATION_MEMBER, value);
		}
	}
	stat.set(STAT_TABLE_CS_NODE_STATUS, mgr.statusInfo_.getSystemStatus());
	stat.set(STAT_TABLE_CS_DESIGNATED_COUNT, mgr.getReserveNum());
	PartitionRevisionNo revisionNo = pt.getCurrentRevisionNo();
	if (!pt.isMaster()) {
		revisionNo = 0;
	}
	stat.set(STAT_TABLE_CS_SYNC_COUNT, revisionNo);
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
		case PT_NORMAL:
			partitionStatus = "NORMAL";
			break;
		case PT_REPLICA_LOSS:
			partitionStatus = "REPLICA_LOSS";
			break;
		case PT_NOT_BALANCE:
			partitionStatus = "NOT_BALANCE";
			break;
		case PT_PARTIAL_STOP:
		case PT_ABNORMAL:
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
		else if (stat.getDisplayOption(STAT_TABLE_DISPLAY_ADDRESS_TRANSACTION)) {
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

		char tmpBuffer[37];
		UUIDUtils::unparse(mgr.uuid_, (char *)tmpBuffer);
		stat.set(STAT_TABLE_CS_CLUSTER_REVISION_ID, &tmpBuffer[0]);

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
			NodeAddress &address = pt.getNodeAddress(nodeId);
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

	int32_t ownerCount  = 0;
	int32_t backupCount = 0;
	uint64_t  totalOwnerLsn = 0;
	uint64_t totalBackupLsn = 0;
	uint64_t totalOtherLsn = 0;
	for (PartitionId pId = 0; pId < pt.getPartitionNum(); pId++) {
		if (pt.isOwner(pId)) {
			ownerCount++;
			totalOwnerLsn += pt.getLSN(pId);
		}
		else if (pt.isBackup(pId)) {
			backupCount++;
			totalBackupLsn += pt.getLSN(pId);
		}
		else {
			totalOtherLsn += pt.getLSN(pId);
		}
	}
	stat.set(STAT_TABLE_PERF_OWNER_COUNT, ownerCount);
	stat.set(STAT_TABLE_PERF_BACKUP_COUNT, backupCount);
	stat.set(STAT_TABLE_PERF_TOTAL_OWNER_LSN, totalOwnerLsn);
	stat.set(STAT_TABLE_PERF_TOTAL_BACKUP_LSN, totalBackupLsn);
	stat.set(STAT_TABLE_PERF_TOTAL_OTHER_LSN, totalOtherLsn);

	return true;
}

void ClusterManager::initialize(ClusterService *clsSvc) {
	clsSvc_ = clsSvc;
	ee_ = clsSvc->getEE();
	pt_->initialize();
	int64_t baseTime = ee_->getMonotonicTime();
	pt_->setHeartbeatTimeout(SELF_NODEID, nextHeartbeatTime(baseTime));
}

ClusterService *ClusterManager::getService() {
	return clsSvc_;
}

void ClusterManager::Config::setUpConfigHandler(
	ClusterManager *clsMgr, ConfigTable &configTable) {
	clsMgr_ = clsMgr;
	configTable.setParamHandler(
		CONFIG_TABLE_CS_CHECKPOINT_DELAY_INTERVAL, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_CS_CATCHUP_PROMOTION_CHECK_INTERVAL, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_CS_OWNER_BACKUP_LSN_GAP, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_CS_OWNER_CATCHUP_LSN_GAP, *this);
}

void ClusterManager::Config::operator()(
	ConfigTable::ParamId id, const ParamValue &value) {
	switch (id) {
	case CONFIG_TABLE_CS_CHECKPOINT_DELAY_INTERVAL:
		break;
	case CONFIG_TABLE_CS_CATCHUP_PROMOTION_CHECK_INTERVAL:
		clsMgr_->getConfig().setOwnerCatchupPromoteCheckInterval(
			changeTimeSecToMill(value.get<int32_t>()));
		break;
	case CONFIG_TABLE_CS_OWNER_BACKUP_LSN_GAP:
		clsMgr_->getPartitionTable()->getConfig().setLimitOwnerBackupLsnGap(value.get<int32_t>());
		break;
	case CONFIG_TABLE_CS_OWNER_CATCHUP_LSN_GAP:
		clsMgr_->getPartitionTable()->getConfig().setLimitOwnerCatchupLsnGap(value.get<int32_t>());
		break;
	}
}