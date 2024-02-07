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
#include "data_store_common.h"
#include "sql_common.h"
#include "picojson.h"
#include "sha2.h"
#include "cluster_service.h"
#include "container_key.h"
#include "uuid_utils.h"

UTIL_TRACER_DECLARE(CLUSTER_SERVICE);
UTIL_TRACER_DECLARE(CLUSTER_DETAIL);

#define TEST_PRINT(s)
#define TEST_PRINT1(s, d)

ClusterManager::ClusterManager(
	const ConfigTable& configTable,
	PartitionTable* partitionTable,
	ClusterVersionId versionId) :
	clusterConfig_(configTable),
	clusterInfo_(partitionTable),
	pt_(partitionTable),
	versionId_(versionId),
	isSignalBeforeRecovery_(false),
	autoShutdown_(false),
	ee_(NULL)
	,
	clsSvc_(NULL),
	expectedCheckTime_(0)
{
	statUpdator_.manager_ = this;
	try {
		clusterInfo_.startupTime_
			= util::DateTime::now(TRIM_MILLISECONDS).getUnixTime();
		std::string currentClusterName =
			configTable.get<const char8_t*>(
				CONFIG_TABLE_CS_CLUSTER_NAME);

		if (currentClusterName.length() > 0) {
			JoinClusterInfo joinInfo(true);
			joinInfo.set(currentClusterName, 0);
			setJoinClusterInfo(joinInfo);
			clusterConfig_.setClusterName(currentClusterName);
		}
		else {
			GS_THROW_USER_ERROR(
				GS_ERROR_CT_PARAMETER_INVALID,
				"Parameter must be specified (source=gs_cluster.json:gs_node.json, "
				"name=cluster.clusterName)");
		}
		concurrency_
			= configTable.get<int32_t>(CONFIG_TABLE_DS_CONCURRENCY);
		ConfigTable* tmpTable = const_cast<ConfigTable*>(&configTable);
		config_.setUpConfigHandler(this, *tmpTable);
		memset(uuid_, 0, 16);
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

ClusterManager::~ClusterManager() {}

/*!
	@brief Checks if node status is executable
*/
void ClusterManager::checkNodeStatus() {
	if (!statusInfo_.checkNodeStatus()) {
		GS_THROW_USER_ERROR(
			GS_ERROR_CLM_NODE_STATUS_CHECK_FAILED,
			"Target node is shutdowning or abnormal");
	}
}

/*!
	@brief Checks if status is active
*/
void ClusterManager::checkActiveStatus() {
	if (!isActive()) {
		GS_THROW_USER_ERROR(
			GS_ERROR_CLM_NODE_STATUS_CHECK_FAILED, "");
	}
}

/*!
	@brief Checks if a specified operation is executable
*/
void ClusterManager::checkCommandStatus(EventType operation) {
	if (!statusInfo_.checkCommandStatus(operation)) {
		GS_THROW_USER_ERROR(
			GS_ERROR_CLM_CLUSTER_OPERATION_CHECK_FAILED, "");
	}
}

/*!
	@brief Updates node status
*/
void ClusterManager::updateNodeStatus(EventType operation) {
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
	EventType operation) {

	NodeStatus targetCurrentStatus, targetNextStatus;
	switch (operation) {
	case CS_JOIN_CLUSTER: {
		targetCurrentStatus = SYS_STATUS_INACTIVE;
		targetNextStatus = SYS_STATUS_INACTIVE;
		break;
	}
	case CS_LEAVE_CLUSTER:
	case CS_SHUTDOWN_CLUSTER:
	case CS_INCREASE_CLUSTER:
	case CS_DECREASE_CLUSTER:
	{
		targetCurrentStatus = SYS_STATUS_ACTIVE;
		targetNextStatus = SYS_STATUS_ACTIVE;
		break;
	}
	case CS_SHUTDOWN_NODE_NORMAL: {
		targetCurrentStatus = SYS_STATUS_INACTIVE;
		targetNextStatus = nextStatus_;
		if (currentStatus_ == SYS_STATUS_ACTIVE) {
			TRACE_CLUSTER_EXCEPTION_FORCE(
				GS_ERROR_CLM_PENDING_SHUTDOWN,
				"Pending SHUTDOWN operation"
				", reason=current status is ACTIVE(ideal INACTIVE)");
			isShutdownPending_ = true;
		}
		break;
	}
	case CS_COMPLETE_CHECKPOINT_FOR_SHUTDOWN: {
		targetCurrentStatus = SYS_STATUS_INACTIVE;
		targetNextStatus = SYS_STATUS_SHUTDOWN_NORMAL;
		break;
	}
	case CS_COMPLETE_CHECKPOINT_FOR_RECOVERY: {
		targetCurrentStatus = SYS_STATUS_BEFORE_RECOVERY;
		targetNextStatus = SYS_STATUS_BEFORE_RECOVERY;
		break;
	}
	case CS_SHUTDOWN_NODE_FORCE: {
		targetCurrentStatus = currentStatus_;
		targetNextStatus = nextStatus_;
		isShutdown_ = true;
		break;
	}
	default: {
		GS_THROW_USER_ERROR(
			GS_ERROR_CLM_INVALID_OPERATION_TYPE,
			"Invalid operation=" << static_cast<int32_t>(operation));
	}
	}
	if (currentStatus_ == targetCurrentStatus
		&& nextStatus_ == targetNextStatus) {
		return true;
	}
	else {
		TRACE_CLUSTER_NORMAL_OPERATION(INFO,
			"Cancelled by unacceptable condition (status="
			<< getSystemStatus() << ")");
		return false;
	}
}

/*!
	@brief Gets node status
*/
const char8_t* ClusterManager::NodeStatusInfo::getSystemStatus() const {
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
	EventType operation) {

	switch (operation) {
	case CS_JOIN_CLUSTER: {
		currentStatus_ = SYS_STATUS_ACTIVE;
		nextStatus_ = SYS_STATUS_ACTIVE;
		break;
	}
	case CS_LEAVE_CLUSTER: {
		currentStatus_ = SYS_STATUS_INACTIVE;
		nextStatus_ = SYS_STATUS_INACTIVE;
		break;
	}
	case CS_SHUTDOWN_NODE_NORMAL: {
		nextStatus_ = SYS_STATUS_SHUTDOWN_NORMAL;
		isNormalShutdownCall_ = true;
		break;
	}
	case CS_COMPLETE_CHECKPOINT_FOR_SHUTDOWN: {
		currentStatus_ = SYS_STATUS_SHUTDOWN_NORMAL;
		isShutdown_ = true;
		break;
	}
	case CS_COMPLETE_CHECKPOINT_FOR_RECOVERY: {
		currentStatus_ = SYS_STATUS_INACTIVE;
		nextStatus_ = SYS_STATUS_INACTIVE;
		break;
	}
	default: {
		GS_THROW_USER_ERROR(
			GS_ERROR_TXN_PARTITION_STATE_INVALID,
			"Operation=" << static_cast<int32_t>(operation)
			<< " is unacceptable");
	}
	}
};

/*!
	@brief Checks cluster status
*/
void ClusterManager::checkClusterStatus(EventType operation) {

	switch (operation) {
	case CS_COMPLETE_CHECKPOINT_FOR_RECOVERY:
	case CS_COMPLETE_CHECKPOINT_FOR_SHUTDOWN:
	case CS_SHUTDOWN_NODE_FORCE:
	case CS_SHUTDOWN_NODE_NORMAL:
	case CS_JOIN_CLUSTER:
	case TXN_CHANGE_PARTITION_STATE: {
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
	case CS_TIMER_CHECK_CLUSTER:
	case CS_LEAVE_CLUSTER:
	case CS_SHUTDOWN_NODE_NORMAL:
	case CS_SHUTDOWN_NODE_FORCE:
	case CS_COMPLETE_CHECKPOINT_FOR_SHUTDOWN:
	case TXN_CHANGE_PARTITION_STATE:
	case TXN_CHANGE_PARTITION_TABLE: {
		break;
	}
	case CS_UPDATE_PARTITION:
	case CS_TIMER_CHECK_LOAD_BALANCE:
	case CS_ORDER_DROP_PARTITION: {
		if (pt_->isSubMaster()) {
			GS_THROW_USER_ERROR(
				GS_ERROR_CLM_CLUSTER_STATUS_IS_SUBMASTER, "");
		}
		break;
	}
	case CS_TIMER_NOTIFY_CLUSTER:
	case CS_HEARTBEAT_RES:
	case CS_NOTIFY_CLUSTER:
	case CS_NOTIFY_CLUSTER_RES:
	{
		if (pt_->isFollower()) {
			GS_THROW_USER_ERROR(
				GS_ERROR_CLM_CLUSTER_STATUS_IS_FOLLOWER, "");
		}
		break;
	}
	case CS_INCREASE_CLUSTER:
	case CS_DECREASE_CLUSTER:
	case CS_SHUTDOWN_CLUSTER: {
		if (!pt_->isMaster()) {
			GS_THROW_USER_ERROR(
				GS_ERROR_CLM_CLUSTER_STATUS_NOT_MASTER, "");
		}
		break;
	}
	case CS_TIMER_NOTIFY_CLIENT:
	{
		if (!pt_->isMaster()) {
			GS_THROW_USER_ERROR(
				GS_ERROR_CLM_CLUSTER_STATUS_NOT_MASTER, "");
		}
		break;
	}
	case CS_HEARTBEAT: {
		if (!pt_->isFollower()) {
			GS_THROW_USER_ERROR(
				GS_ERROR_CLM_CLUSTER_STATUS_NOT_FOLLOWER, "");
		}
		break;
	}
	case CS_JOIN_CLUSTER: {
		if (isJoinCluster()) {
			GS_THROW_USER_ERROR(
				GS_ERROR_CLM_CLUSTER_STATUS_ALREADY_JOIN_CLUSTER, "");
		}
		break;
	}
	case CS_COMPLETE_CHECKPOINT_FOR_RECOVERY: {
		if (!pt_->isSubMaster()) {
			GS_THROW_USER_ERROR(
				GS_ERROR_CLM_CLUSTER_STATUS_NOT_SUBMASTER, "");
		}
		break;
	}
	default: {
		GS_THROW_USER_ERROR(
			GS_ERROR_CLM_INVALID_OPERATION_TYPE,
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
		const std::string& prevStatus
			= pt_->dumpCurrentClusterStatus();
		switch (status) {
		case TO_SUBMASTER: {
			TRACE_CLUSTER_EXCEPTION_FORCE(
				GS_ERROR_CLM_STATUS_TO_SUBMASTER,
				"Cluster status change to SUB_MASTER");
			setNotifyPendingCount();
			clearActiveNodeList();
			if (isLeave) {
				setJoinCluster(false);
			}
			pt_->changeClusterStatus(
				PartitionTable::SUB_MASTER, 0, 0, 0);
			pt_->setPartitionSummaryStatus(PartitionTable::PT_INITIAL);
			break;
		}
		case TO_MASTER: {
			TRACE_CLUSTER_EXCEPTION_FORCE(
				GS_ERROR_CLM_STATUS_TO_MASTER,
				"Cluster status change to MASTER");
			setInitialCluster(false);
			pt_->changeClusterStatus(
				PartitionTable::MASTER, 0, 0, 0);
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
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Gets HeartbeatInfo
*/
void ClusterManager::getHeartbeatInfo(
	HeartbeatInfo& heartbeatInfo) {

	try {
		heartbeatInfo.set(clusterInfo_);
		if (pt_->isMaster()) {
			heartbeatInfo.setMaxLsnList();
		}
		pt_->getNodeAddressInfo(heartbeatInfo.getNodeAddressList(),
			heartbeatInfo.getPublicNodeAddressList());
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Sets HeartbeatInfo
*/
bool ClusterManager::setHeartbeatInfo(
	HeartbeatInfo& heartbeatInfo) {

	try {
		bool isParentMaster = heartbeatInfo.isParentMaster();
		NodeId senderNodeId = heartbeatInfo.getSenderNodeId();
		int64_t baseTime = getMonotonicTime();
		pt_->setHeartbeatTimeout(
			SELF_NODEID, nextHeartbeatTime(baseTime));

		if (!pt_->isFollower()) {
			NodeId nodeId = pt_->getOwner(0);
			if (nodeId >= 0) {
				NodeAddress& address
					= pt_->getNodeAddress(nodeId, TRANSACTION_SERVICE);
				heartbeatInfo.setPartition0NodeAddr(address);
				TEST_PRINT1("NodeAddress %s(1)\n", address.dump().c_str());
			}
			else {
				NodeAddress address;  
				heartbeatInfo.setPartition0NodeAddr(address);
				TEST_PRINT1("NodeAddress %s(2)\n", address.dump().c_str());
			}
			for (PartitionId pId = 0;
				pId < pt_->getPartitionNum(); pId++) {
				LogSequentialNumber lsn = pt_->getLSN(pId);
				pt_->setLsnWithCheck(pId, lsn);
				pt_->setMaxLsn(pId, lsn);
			}
			pt_->setAckHeartbeat(0, true);
			if (pt_->isMaster()) {
				NodeAddress nodeAddr
					= heartbeatInfo.getPartition0NodeAddr();
				TEST_PRINT("ClusterManager::set() MASTER\n");
				NodeAddress addressOnMaster
					= getPartition0NodeAddrOnMaster();
				if (!(nodeAddr == addressOnMaster)) {
					setPartition0NodeAddr(nodeAddr);
					setPartition0NodeAddrOnMaster(nodeAddr);
					TEST_PRINT1(
						"NodeAddress %s set\n", nodeAddr.dump().c_str());
				}
			}
			else {
				return false;
			}
		}
		else {
			NodeAddress nodeAddr
				= heartbeatInfo.getPartition0NodeAddr();
			TEST_PRINT("ClusterManager::set() FOLLOWER\n");
			NodeAddress addressOnMaster
				= getPartition0NodeAddrOnMaster();
			if (!(nodeAddr == addressOnMaster)) {
				setPartition0NodeAddr(nodeAddr);
				setPartition0NodeAddrOnMaster(nodeAddr);
				TEST_PRINT1(
					"NodeAddress %s set\n", nodeAddr.dump().c_str());
			}
			NodeId masterNodeId = pt_->getMaster();
			if (senderNodeId != masterNodeId) {
				GS_TRACE_WARNING(
					CLUSTER_SERVICE, GS_TRACE_CS_OPERATION,
					"Master address check failed, expected="
					<< pt_->dumpNodeAddress(masterNodeId)
					<< ", actual=" << pt_->dumpNodeAddress(senderNodeId));
				return false;
			}
			pt_->setHeartbeatTimeout(
				senderNodeId, nextHeartbeatTime(baseTime));
			LsnList& maxLsnList = heartbeatInfo.getMaxLsnList();
			for (PartitionId pId = 0; pId < maxLsnList.size(); pId++) {
				pt_->setMaxLsn(pId, maxLsnList[pId]);
			}
			pt_->updatePartitionRevision(
				heartbeatInfo.getPartitionRevisionNo());
			NodeAddress& secondMasterNode
				= heartbeatInfo.getSecondMaster();
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
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Gets HeartbeatResInfo
*/
void ClusterManager::getHeartbeatResInfo(
	HeartbeatResInfo& heartbeatResInfo) {

	try {
		PartitionId pId;
		uint16_t sslPort = pt_->getSSLPortNo(SELF_NODEID);
		for (pId = 0; pId < pt_->getPartitionNum(); pId++) {
			int32_t errorStatus = pt_->getErrorStatus(pId, SELF_NODEID);
			heartbeatResInfo.add(pId,
				pt_->getPartitionStatus(pId, SELF_NODEID),
				pt_->getLSN(pId, SELF_NODEID),
				pt_->getStartLSN(pId, SELF_NODEID),
				pt_->getPartitionRoleStatus(pId, SELF_NODEID),
				pt_->getPartitionRevision(pId, SELF_NODEID),
				0,
				sslPort,
				errorStatus
			);
		}
		SyncStat& stat = getSyncStat();
		heartbeatResInfo.set(stat);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Sets HeartbeatResInfo
*/
void ClusterManager::setHeartbeatResInfo(
	HeartbeatResInfo& heartbeatResInfo) {

	try {
		NodeId senderNodeId = heartbeatResInfo.getSenderNodeId();
		assert(senderNodeId > 0);
		std::vector<HeartbeatResValue>& heartbeatResValueList =
			heartbeatResInfo.getHeartbeatResValueList();
		PartitionId pId;
		size_t pos = 0;

		PartitionStatus status;
		PartitionRoleStatus roleStatus;
		PartitionRevisionNo partitionRevision;
		int64_t chunkNum;
		int32_t sslPort;
		int32_t errorStatus;
		int64_t baseTime = getMonotonicTime();
		pt_->setHeartbeatTimeout(
			senderNodeId, nextHeartbeatTime(baseTime));
		pt_->setAckHeartbeat(senderNodeId, true);
		for (pos = 0; pos < heartbeatResValueList.size(); pos++) {
			HeartbeatResValue& resValue = heartbeatResValueList[pos];
			if (!resValue.validate(pt_->getPartitionNum())) {
				continue;
			}
			pId = resValue.pId_;
			status = static_cast<PartitionStatus>(resValue.status_);
			roleStatus = static_cast<PartitionRoleStatus>(
				resValue.roleStatus_);
			partitionRevision = resValue.partitionRevision_;
			chunkNum = resValue.chunkCount_;
			sslPort = resValue.sslPort_;
			errorStatus = resValue.errorStatus_;
			pt_->handleErrorStatus(
				pId, senderNodeId, errorStatus);
			pt_->setLsnWithCheck(pId, resValue.lsn_, senderNodeId);
			pt_->setMaxLsn(pId, resValue.lsn_);
			pt_->setStartLSN(pId, resValue.startLsn_, senderNodeId);
			pt_->setPartitionStatus(pId, status, senderNodeId);
			pt_->setPartitionRoleStatus(pId, roleStatus, senderNodeId);
			pt_->setPartitionRevision(pId, partitionRevision, senderNodeId);
			pt_->setErrorStatus(pId, errorStatus, senderNodeId);
			if (pos == 0) {
				pt_->setSSLPortNo(senderNodeId, sslPort);
			}
		}
		heartbeatResInfo.setSyncInfo(getSyncStat());
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Adds value to HeartbeatResInfo
*/
void ClusterManager::HeartbeatResInfo::add(
	PartitionId pId,
	PartitionStatus status,
	LogSequentialNumber lsn,
	LogSequentialNumber startLsn,
	PartitionRoleStatus roleStatus,
	PartitionRevisionNo partitionRevision,
	int64_t chunkCount,
	int32_t sslPort,
	int32_t errorStatus) {

	HeartbeatResValue resValue(pId, status, lsn, startLsn,
		roleStatus, partitionRevision,
		chunkCount,
		sslPort,
		errorStatus);
	heartbeatResValues_.push_back(resValue);
}

/*!
	@brief Gets HeartbeatCheckInfo
*/
void ClusterManager::getHeartbeatCheckInfo(
	HeartbeatCheckInfo& heartbeatCheckInfo) {

	try {
		int64_t currentTime = getMonotonicTime();
		if (expectedCheckTime_ != 0) {
			int64_t diff = currentTime - expectedCheckTime_;
//			if (diff >= ClusterService::CLUSTER_TIMER_EVENT_DIFF_TRACE_LIMIT_INTERVAL) {
//				GS_TRACE_WARNING(
//					CLUSTER_DUMP, GS_TRACE_CM_LONG_EVENT, "Heartbeat timer is delayed (" << diff << ")");
//			}
		}
		expectedCheckTime_ = currentTime + getConfig().getHeartbeatInterval();
		ClusterStatusTransition nextTransition = KEEP;
		bool isAddNewNode = false;
		NodeIdList& activeNodeList = heartbeatCheckInfo.getActiveNodeList();
		util::StackAllocator& alloc = heartbeatCheckInfo.getAllocator();
		int32_t periodicCount
			= getConfig().getCheckLoadBalanceInterval()
			/ getConfig().getHeartbeatInterval();
		if ((pt_->getPartitionRevisionNo()
			% getConfig().getBlockClearInterval()) == 0) {
			pt_->resetBlockQueue();
		}
//		GS_TRACE_INFO(
//			CLUSTER_DUMP, GS_TRACE_CS_CLUSTER_STATUS,
//			"CURRENT " << CommonUtility::getTimeStr(currentTime));
		if (!pt_->isFollower()) {
			for (NodeId nodeId = 0; nodeId < pt_->getNodeNum(); nodeId++) {
				int64_t timeout = pt_->getHeartbeatTimeout(nodeId);
//				GS_TRACE_INFO(
//					CLUSTER_DUMP, GS_TRACE_CS_CLUSTER_STATUS,
//					pt_->dumpNodeAddress(nodeId) << " " << CommonUtility::getTimeStr(timeout).c_str());
			}
			pt_->setHeartbeatTimeout(
				SELF_NODEID, nextHeartbeatTime(currentTime));

			util::Set<NodeId> downNodeSet(alloc);
			pt_->getActiveNodeList(
				activeNodeList, downNodeSet, currentTime);
			if (downNodeSet.size() > 0) {
				setAddOrDownNode();
			}
			if (pt_->isMaster()) {
				if ((pt_->getPartitionRevisionNo()
					% periodicCount) == 0) {
					pt_->checkPartitionSummaryStatus(alloc, false, true);
				}
				for (util::Set<NodeId>::iterator it = downNodeSet.begin();
					it != downNodeSet.end(); it++) {
					util::String rackZoneIdName(alloc);
					pt_->getRackZoneIdName(rackZoneIdName, (*it));
					util::NormalOStringStream oss;
					oss << "Follower heartbeat timeout, follower=" << pt_->dumpNodeAddress(*it);
					if (!oss.str().empty()) {
						oss << ", rackZoneId='" << rackZoneIdName.c_str() << "'";
					}
					TRACE_CLUSTER_EXCEPTION_FORCE(GS_ERROR_CLM_DETECT_HEARTBEAT_TO_FOLLOWER, oss.str().c_str());
					pt_->resetDownNode((*it));
				}
			}
			int32_t currentActiveNodeNum
				= static_cast<int32_t>(activeNodeList.size());
			int32_t checkedNodeNum;
			bool isCheckedOk = false;
			if (isInitialCluster()) {
				checkedNodeNum = getInitialClusterNum();
			}
			else {
				checkedNodeNum = getQuorum();
			}
			if (checkedNodeNum > 0
				&& checkedNodeNum <= currentActiveNodeNum) {
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
			if (currentActiveNodeNum != 1
				&& currentActiveNodeNum > prevActiveNodeNum) {
				GS_TRACE_WARNING(CLUSTER_SERVICE,
					GS_TRACE_CS_TRACE_STATS,
					"Detect new active node, currentActiveNum:"
					<< currentActiveNodeNum
					<< ", prevActiveNum:" << prevActiveNodeNum);
				setAddOrDownNode();
				isAddNewNode = true;
			}
			setActiveNodeList(activeNodeList);
		}
		else {
			int64_t timeout = pt_->getHeartbeatTimeout(0);
//			GS_TRACE_INFO(
//				CLUSTER_DUMP, GS_TRACE_CS_CLUSTER_STATUS,
//				pt_->dumpNodeAddress(0) << " " << CommonUtility::getTimeStr(timeout).c_str());

			if (pt_->getHeartbeatTimeout(0) < currentTime) {
				TRACE_CLUSTER_EXCEPTION_FORCE(
					GS_ERROR_CLM_DETECT_HEARTBEAT_TO_MASTER,
					"Master heartbeat timeout, master="
					<< pt_->dumpNodeAddress(pt_->getMaster())
					<< ", current_time=" << CommonUtility::getTimeStr(currentTime)
					<< ", last_time=" << CommonUtility::getTimeStr(pt_->getHeartbeatTimeout(0)));

				nextTransition = TO_SUBMASTER;
			}
		}
		heartbeatCheckInfo.set(nextTransition, isAddNewNode);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Sets UpdatePartitionInfo
*/
void ClusterManager::setupPartitionContext(
	PartitionTable::PartitionContext& context) {

	context.setRepairPartition(isRepairPartition());
	int64_t baseTime = getMonotonicTime();
	int64_t limitTime
		= baseTime + clusterConfig_.getRuleLimitInterval();
	context.setRuleLimitTime(limitTime);
	context.currentTime_ = baseTime;
	context.subPartitionTable_.setRevision(pt_->getPartitionRevision());
	context.isLoadBalance_ = checkLoadBalance();
	context.isAutoGoal_ = checkAutoGoal();
}

void ClusterManager::getUpdatePartitionInfo(
	UpdatePartitionInfo& updatePartitionInfo) {

	try {
		util::StackAllocator& alloc = updatePartitionInfo.getAllocator();
		PartitionTable::PartitionContext context(
			alloc, pt_, updatePartitionInfo.isNeedUpdatePartition(),
			updatePartitionInfo.getSubPartitionTable(),
			updatePartitionInfo.getDropPartitionNodeInfo());
		setupPartitionContext(context);
		bool isConfigurationChange = pt_->checkConfigurationChange(
			alloc, context, updatePartitionInfo.isAddNewNode());

		int32_t periodicCount
			= getConfig().getCheckDropInterval()
			/ getConfig().getHeartbeatInterval();
		if (periodicCount == 0) periodicCount = 1;
		if ((pt_->getPartitionRevisionNo() % periodicCount) == 0) {
			if (pt_->checkPeriodicDrop(
				alloc, context.dropNodeInfo_)) {
				context.needUpdatePartition_ = true;
				context.subPartitionTable_.setRevision(
					pt_->getPartitionRevision());
			}
		}

		if (!isConfigurationChange && !pt_->check(context)) {
			return;
		}

		pt_->planNext(context);

		UUIDUtils::generate(uuid_);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void ClusterManager::setUpdatePartitionInfo(
	UpdatePartitionInfo& updatePartitionInfo) {

	try {
		util::StackAllocator& alloc = updatePartitionInfo.getAllocator();
		PartitionTable::SubPartitionTable& subPartitionTable
			= updatePartitionInfo.getSubPartitionTable();
		int32_t subPartitionSize = subPartitionTable.size();

		util::XArray<PartitionId> currentOwnerList(alloc);
		util::XArray<PartitionId> nextOwnerList(alloc);
		util::XArray<PartitionId> nextBackupList(alloc);
		util::XArray<PartitionId> nextGoalOwnerList(alloc);
		util::XArray<PartitionId> nextGoalCatchupList(alloc);
		util::Vector<PartitionId>& shorttermSyncPIdList
			= updatePartitionInfo.getShorttermSyncPIdList();
		util::Vector<PartitionId>& longtermSyncPIdList
			= updatePartitionInfo.getLongtermSyncPIdList();
		util::Vector<PartitionId>& changePartitionPIdList
			= updatePartitionInfo.getChangePartitionPIdList();

		for (int32_t pos = 0; pos < subPartitionSize; pos++) {
			PartitionTable::SubPartition& subPartition
				= subPartitionTable.getSubPartition(pos);
			bool isCurrentOwner = (subPartition.currentOwnerId_ == 0);
			PartitionId pId = subPartition.pId_;
			PartitionRole& role = subPartition.role_;
			bool isNextOwner = role.isOwner();
			bool isNextBackup = role.isBackup();
			bool isNextCatchup = role.isCatchup();
			bool isLongtermSync = role.hasCatchupRole();
			if (!isLongtermSync) {
				if (isCurrentOwner) {
					currentOwnerList.push_back(pId);
					shorttermSyncPIdList.push_back(pos);
				}
				if (isNextOwner) {
					nextOwnerList.push_back(pId);
				}
				else if (isNextBackup) {
					nextBackupList.push_back(pId);
				}
			}
			else {
				if (isCurrentOwner) {
					nextGoalOwnerList.push_back(pId);
					longtermSyncPIdList.push_back(pos);
				}
				else if (isNextBackup) {
					changePartitionPIdList.push_back(pos);
				}
				else if (isNextCatchup) {
					nextGoalCatchupList.push_back(pId);
				}
			}
			if (!isCurrentOwner && !isNextOwner
				&& !isNextBackup) {
				changePartitionPIdList.push_back(pos);
			}
		}
		pt_->updateNewSQLPartition(subPartitionTable);

		PartitionRevisionNo revisionNo = subPartitionTable.getRevision().getRevisionNo();
		PartitionRevision& currentRevision = subPartitionTable.getRevision();
		if (currentOwnerList.size() > 0) {
			TRACE_CLUSTER_NORMAL_OPERATION(INFO,
				"Short term sync current owners (partitions="
				<< dumpPartitionList(pt_, currentOwnerList) << ")");
		}
		if (nextOwnerList.size() > 0) {
			TRACE_CLUSTER_NORMAL_OPERATION(INFO,
				"Short term sync next owners (revision="
				<< currentRevision << ", partitions="
				<< dumpPartitionList(pt_, nextOwnerList) << ")");
		}
		if (nextBackupList.size() > 0) {
			TRACE_CLUSTER_NORMAL_OPERATION(INFO,
				"Short term sync next backups (revision="
				<< currentRevision << ", partitions="
				<< dumpPartitionList(pt_, nextBackupList) << ")");
		}
		if (nextGoalOwnerList.size() > 0) {
			TRACE_CLUSTER_NORMAL_OPERATION(INFO,
				"Long term sync next owners (revision="
				<< currentRevision << ", partitions="
				<< dumpPartitionList(pt_, nextGoalOwnerList) << ")");
		}
		if (nextGoalCatchupList.size() > 0) {
			TRACE_CLUSTER_NORMAL_OPERATION(INFO,
				"Long term sync next catchups  (revision="
				<< currentRevision << ", partitions="
				<< currentRevision << "," << dumpPartitionList(
					pt_, nextGoalCatchupList));
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Gets NotifyClusterInfo
*/
void ClusterManager::getNotifyClusterInfo(
	NotifyClusterInfo& notifyClusterInfo) {

	try {
		int32_t notifyPendingCount = updateNotifyPendingCount();
		if (notifyPendingCount > 0) {
			GS_THROW_USER_ERROR(
				GS_ERROR_CLM_CLUSTER_IS_PENDING,
				"Cluster notification message will send after "
				<< notifyPendingCount *
				clusterConfig_.getNotifyClusterInterval() / 1000 << " seconds");
		}
		notifyClusterInfo.set(clusterInfo_, pt_);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Sets NotifyClusterInfo
*/
bool ClusterManager::setNotifyClusterInfo(
	NotifyClusterInfo& notifyClusterInfo) {

	try {
		NodeId senderNodeId = notifyClusterInfo.getSenderNodeId();
		bool isFollowerMaster = notifyClusterInfo.isMaster();
		if (getClusterName()
			!= notifyClusterInfo.getClusterName()) {
			GS_TRACE_DEBUG(CLUSTER_DETAIL,
				GS_TRACE_CS_TRACE_STATS,
				"Cluster name is not match, self=" << getClusterName()
				<< ", recv=" << notifyClusterInfo.getClusterName()
			);
			GS_THROW_USER_ERROR(
				GS_ERROR_CLM_UNMATCH_CLUSTER_NAME,
				"Cluster name is not match, self=" << getClusterName()
				<< ", recv=" << notifyClusterInfo.getClusterName());
		}
		if (getDigest() != notifyClusterInfo.getDigest()) {
			GS_TRACE_DEBUG(CLUSTER_DETAIL,
				GS_TRACE_CS_TRACE_STATS,
				"Cluster digest is not match, self=" << getDigest()
				<< ", recv=" << notifyClusterInfo.getDigest()
			);

			GS_THROW_USER_ERROR(
				GS_ERROR_CLM_UNMATCH_DIGEST,
				"Cluster digest is not match, self=" << getDigest()
				<< ", recv=" << notifyClusterInfo.getDigest());
		}
		if (pt_->isMaster()) {
			if (isFollowerMaster) {
				detectMultiMaster();
				TRACE_CLUSTER_EXCEPTION_FORCE(
					GS_ERROR_CLM_DETECT_DOUBLE_MASTER,
					"Detect multiple clusters of same environment, detected node="
					<< pt_->dumpNodeAddress(senderNodeId));
				GS_THROW_USER_ERROR(GS_ERROR_CLM_DETECT_DOUBLE_MASTER, "");
			}
			return false;
		}
		if ((!isNewNode() &&
			getReserveNum() != notifyClusterInfo.getReserveNum())) {
			GS_TRACE_DEBUG(CLUSTER_DETAIL,
				GS_TRACE_CS_TRACE_STATS,
				"Cluster minNodeNum is not match, self="
				<< getReserveNum()
				<< ", recv=" << notifyClusterInfo.getReserveNum()
			);
			GS_THROW_USER_ERROR(
				GS_ERROR_CLM_UNMATCH_RESERVE_NUM,
				"Cluster minNodeNum is not match, self=" << getReserveNum()
				<< ", recv=" << notifyClusterInfo.getReserveNum());
		}
		if (isFollowerMaster && notifyClusterInfo.isStable()) {
			GS_TRACE_DEBUG(CLUSTER_DETAIL,
				GS_TRACE_CS_TRACE_STATS,
				"Self node cannot join cluster : master node is stable"
			);
			GS_THROW_USER_ERROR(
				GS_ERROR_CLM_CLUSTER_FOLLOWER_IS_STABLE,
				"Self node cannot join cluster : master node is stable");
		}
		if (isNewNode() && !isFollowerMaster) {
			GS_TRACE_DEBUG(CLUSTER_DETAIL,
				GS_TRACE_CS_TRACE_STATS,
				"self is newNode and received message from subMaster"
			);
			GS_THROW_USER_ERROR(
				GS_ERROR_CLM_NEW_NODE_NEED_TO_FOLLOW_MASTER,
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
				"Cluster Startup time=" << CommonUtility::getTimeStr(startupTime)
				<< " is same, compare node address.");
			if (pt_->dumpNodeAddress(0)
					> pt_->dumpNodeAddress(senderNodeId)) {
				notifyFlag = true;
			}
		}
		if (notifyFlag) {
			TRACE_CLUSTER_EXCEPTION_FORCE(
				GS_ERROR_CLM_STATUS_TO_FOLLOWER,
				"Cluster status change to FOLLOWER");
/*
			GS_TRACE_CLUSTER_INFO(
				"Cluster status change to FOLLOWER, MASTER="
				<< pt_->dumpNodeAddress(senderNodeId));
*/
			setInitialClusterNum(notifyClusterInfo.getReserveNum());
			int64_t baseTime = getMonotonicTime();
			pt_->changeClusterStatus(PartitionTable::FOLLOWER,
				senderNodeId, baseTime, nextHeartbeatTime(baseTime));
			pt_->setPartitionSummaryStatus(PartitionTable::PT_NORMAL);
		}
		notifyClusterInfo.setFollow(notifyFlag);

		return true;
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
};

void ClusterManager::NotifyClusterInfo::set(
	ClusterInfo& clusterInfo,
	PartitionTable* pt) {

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
			pt->getNodeAddress(SELF_NODEID, SYSTEM_SERVICE),
			pt->getNodeAddress(SELF_NODEID, SQL_SERVICE)
		);
		nodeList_.push_back(addressInfo);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Gets NotifyClusterResInfo
*/
void ClusterManager::getNotifyClusterResInfo(
	NotifyClusterResInfo& notifyClusterResInfo) {

	try {
		notifyClusterResInfo.set(clusterInfo_, pt_);
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void ClusterManager::NotifyClusterResInfo::set(
	ClusterInfo& clusterInfo,
	PartitionTable* pt) {

	try {
		reserveNum_ = clusterInfo.reserveNum_;
		startupTime_ = clusterInfo.startupTime_;
		partitionSequentialNumber_ = pt->getPartitionRevision().getRevisionNo();
		for (PartitionId pId = 0;
			pId < pt->getPartitionNum(); pId++) {
			lsnList_.push_back(pt->getLSN(pId));
			maxLsnList_.push_back(pt->getMaxLsn(pId));
		}
		NodeId nodeNum = pt->getNodeNum();
		for (NodeId nodeId = 0; nodeId < nodeNum; nodeId++) {
			AddressInfo addressInfo(
				pt->getNodeAddress(nodeId, CLUSTER_SERVICE),
				pt->getNodeAddress(nodeId, TRANSACTION_SERVICE),
				pt->getNodeAddress(nodeId, SYNC_SERVICE),
				pt->getNodeAddress(nodeId, SYSTEM_SERVICE),
				pt->getNodeAddress(nodeId, SQL_SERVICE)
			);
			nodeList_.push_back(addressInfo);
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Sets NotifyClusterResInfo
*/
void ClusterManager::setNotifyClusterResInfo(
	NotifyClusterResInfo& notifyClusterResInfo) {

	try {
		NodeId senderNodeId = notifyClusterResInfo.getSenderNodeId();
		int32_t reserveNum = notifyClusterResInfo.getReserveNum();
		LsnList& lsnList = notifyClusterResInfo.getLsnList();

		util::StackAllocator& alloc = notifyClusterResInfo.getAllocator();
		util::XArray<NodeId> liveNodeIdList(alloc);
		if (checkActiveNodeMax()) {
			pt_->setHeartbeatTimeout(senderNodeId, UNDEF_TTL);
			GS_TRACE_CLUSTER_INFO(
				"Cluster live node num is already reached to reserved num="
				<< getReserveNum() << ", sender node can not join, node="
				<< pt_->dumpNodeAddress(senderNodeId));
			GS_THROW_USER_ERROR(GS_ERROR_CLM_ALREADY_STABLE,
				"Cluster live node num is already reached to reserved num="
				<< getReserveNum());
		}
		PartitionId pId;
		int64_t baseTime = getMonotonicTime();
		pt_->setHeartbeatTimeout(
			0, nextHeartbeatTime(baseTime));
		pt_->setHeartbeatTimeout(
			senderNodeId, nextHeartbeatTime(baseTime));

		pt_->updatePartitionRevision(
			notifyClusterResInfo.getPartitionRevisionNo());

		if (getReserveNum() < reserveNum) {
			GS_TRACE_WARNING(CLUSTER_SERVICE,
				GS_TRACE_CS_CLUSTER_STATUS,
				"Modify cluster reserved num="
				<< getReserveNum() << " to "
				<< reserveNum);
			setReserveNum(reserveNum);
		}
		for (pId = 0;
			pId < static_cast<uint32_t>(lsnList.size()); pId++) {
			pt_->setLSN(pId, lsnList[pId], senderNodeId);
			pt_->setMaxLsn(pId, lsnList[pId]);
		}
		updateSecondMaster(
			senderNodeId, notifyClusterResInfo.getStartupTime());
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Sets JoinClusterInfo
*/
void ClusterManager::setJoinClusterInfo(
	JoinClusterInfo& joinClusterInfo) {

	try {
		std::string& clusterName = joinClusterInfo.getClusterName();
		int32_t minNodeNum = joinClusterInfo.getMinNodeNum();

		if (clusterName.size() == 0) {
			joinClusterInfo.setErrorType(
				WEBAPI_CS_CLUSTERNAME_SIZE_ZERO);
			GS_THROW_USER_ERROR(
				GS_ERROR_CLM_JOIN_CLUSTER_INVALID_CLUSTER_NAME,
				"Invalid cluster parameter, cluster name size=0");
		}
		if (static_cast<int32_t>(clusterName.size()) >
			extraConfig_.getMaxClusterNameSize()) {
			joinClusterInfo.setErrorType(
				WEBAPI_CS_CLUSTERNAME_SIZE_LIMIT);
			GS_THROW_USER_ERROR(
				GS_ERROR_CLM_JOIN_CLUSTER_INVALID_CLUSTER_NAME,
				"Invalid cluster parameter, cluster name size="
				<< clusterName.length()
				<< ", limit=" << extraConfig_.getMaxClusterNameSize());
		}
		try {
			NoEmptyKey::validate(
				KeyConstraint::getUserKeyConstraint(
					extraConfig_.getMaxClusterNameSize()),
				clusterName.c_str(),
				static_cast<uint32_t>(clusterName.size()),
				"clusterName");
		}
		catch (std::exception& e) {
			joinClusterInfo.setErrorType(
				WEBAPI_CS_CLUSTERNAME_INVALID);
			GS_THROW_USER_ERROR(
				GS_ERROR_CLM_JOIN_CLUSTER_INVALID_CLUSTER_NAME,
				"Invalid cluster parameter : invalid cluster name="
				<< clusterName);
		}
		const std::string& currentClusterName
			= clusterConfig_.getSetClusterName();
		if (currentClusterName.length() != 0
			&& currentClusterName != clusterName) {
			joinClusterInfo.setErrorType(
				WEBAPI_CS_CLUSTERNAME_UNMATCH);
			GS_THROW_USER_ERROR(
				GS_ERROR_CLM_JOIN_CLUSTER_UNMATCH_CLUSTER_NAME,
				"Cluster name is not match to set cluster name, current="
				<< clusterName << ", setted cluster name="
				<< currentClusterName);
		}
		if (!joinClusterInfo.isPreCheck()) {
			pt_->resizeCurrenPartition(minNodeNum);
			setInitialClusterNum(minNodeNum);
			setClusterName(currentClusterName);
			setReserveNum(minNodeNum);
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Sets LeaveClusterInfo
*/
void ClusterManager::setLeaveClusterInfo(
	LeaveClusterInfo& leaveClusterInfo) {

	try {
		if (!leaveClusterInfo.isForceLeave()) {
			util::StackAllocator& alloc = leaveClusterInfo.getAllocator();
			for (PartitionId pId = 0;
				pId < pt_->getPartitionNum(); pId++) {
				bool isOwner = pt_->isOwner(pId);
				size_t backupSize = pt_->getBackupSize(alloc, pId);
				if (isOwner && backupSize == 0) {
					leaveClusterInfo.setErrorType(
						WEBAPI_CS_LEAVE_NOT_SAFETY_NODE);
					GS_THROW_USER_ERROR(
						GS_ERROR_SC_LEAVE_NOT_SAFETY_NODE,
						"Self node is not safe, checked pId=" << pId);
				}
			}
		}
		if (!leaveClusterInfo.isPreCheck()) {
			clusterInfo_.initLeave();
		}
		return;
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}
/*!
	@brief Sets IncreaseClusterInfo
*/
void ClusterManager::setIncreaseClusterInfo(
	IncreaseClusterInfo& increaseClusterInfo) {

	try {
		if (!checkSteadyStatus()) {
			increaseClusterInfo.setErrorType(
				WEBAPI_CS_CLUSTER_IS_STABLE);
			GS_THROW_USER_ERROR(
				GS_ERROR_CLM_NOT_STABLE,
				"Cannot increase cluster operation "
				"( Cluster is not stable, reserveNum="
				<< getReserveNum() << ", activeNodeCount="
				<< getActiveNum() << ")");
		}

		if (!increaseClusterInfo.isPreCheck()) {
			setReserveNum(
				getReserveNum() + increaseClusterInfo.getAddNodeNum());
			TRACE_CLUSTER_NORMAL_OPERATION(
				INFO, "Increase cluster (stable) operation completed"
				" (designatedCount=" << getReserveNum() << ")");
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Sets DecreaseClusterInfo
*/
void ClusterManager::setDecreaseClusterInfo(
	DecreaseClusterInfo& decreaseClusterInfo) {

	try {
		if (!checkSteadyStatus()) {
			decreaseClusterInfo.setErrorType(
				WEBAPI_CS_CLUSTER_IS_STABLE);
			GS_THROW_USER_ERROR(GS_ERROR_CLM_NOT_STABLE,
				"Cannot decrease cluster (not stable), reserveNum:"
				<< getReserveNum() << ", activeNodeNum:"
				<< getActiveNum());
		}

		NodeIdList& leaveNodeList
			= decreaseClusterInfo.getLeaveNodeList();
		if (leaveNodeList.size() == 0) {
			getSafetyLeaveNodeList(
				decreaseClusterInfo.getAllocator(), leaveNodeList);
		}

		if (!decreaseClusterInfo.isPreCheck()) {
			int32_t leaveNodeNum
				= static_cast<int32_t>(leaveNodeList.size());
			if (getReserveNum() - leaveNodeNum > 0) {
				setReserveNum(getReserveNum() - leaveNodeNum);
				TRACE_CLUSTER_NORMAL_OPERATION(INFO,
					"Decrease cluster (stable) operation completed (designatedCount="
					<< getReserveNum() << ", leaveNodeList="
					<< pt_->dumpNodeAddressList(leaveNodeList) << ")");
			}
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

std::pair<ServiceType, bool> ClusterManager::statOptionToAddressType(
	const StatTable& stat) {
	ServiceType type;
	if (stat.getDisplayOption(
		STAT_TABLE_DISPLAY_ADDRESS_CLUSTER)) {
		type = CLUSTER_SERVICE;
	}
	else if (stat.getDisplayOption(
		STAT_TABLE_DISPLAY_ADDRESS_TRANSACTION)) {
		type = TRANSACTION_SERVICE;
	}
	else if (stat.getDisplayOption(
		STAT_TABLE_DISPLAY_ADDRESS_SYNC)) {
		type = SYNC_SERVICE;
	}
	else if (stat.getDisplayOption(
		STAT_TABLE_DISPLAY_ADDRESS_SQL)) {
		type = SYNC_SERVICE;
	}
	else {
		type = SYSTEM_SERVICE;
	}

	const bool secure =
		stat.getDisplayOption(STAT_TABLE_DISPLAY_ADDRESS_SECURE);
	return std::make_pair(type, secure);
}

/*!
	@brief Gets the list of nodes can leave cluster
*/
void ClusterManager::getSafetyLeaveNodeList(
	util::StackAllocator& alloc,
	NodeIdList& candList,
	int32_t removeNodeNum) {

	try {
		int32_t nodeNum = pt_->getNodeNum();
		util::XArray<bool> candFlagList(alloc);
		candFlagList.assign(nodeNum, true);

		int32_t backupCount = 0;
		NodeId owner;
		int32_t target;
		for (PartitionId pId = 0;
			pId < pt_->getPartitionNum(); pId++) {
			owner = pt_->getOwner(pId);
			if (owner == UNDEF_NODEID || owner >= nodeNum) {
				continue;
			}
			util::XArray<NodeId> backups(alloc);
			pt_->getBackup(pId, backups);
			if (backups.size() == 0) continue;
			backupCount = 0;
			for (target = 0;
				target < static_cast<int32_t>(backups.size());target++) {
				if (backups[target] >= nodeNum) {
					continue;
				}
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
			if (activeNodeList[target] >= nodeNum) {
				continue;
			}
			if (candFlagList[activeNodeList[target]]
				&& pt_->checkLiveNode(activeNodeList[target])) {
				if (activeNum - 1 < quorum) {
					break;
				}
				candList.push_back(activeNodeList[target]);
				if (static_cast<int32_t>(candList.size())
					== removeNodeNum) {
					break;
				}
				activeNum--;
			}
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Sets ChangePartitionStatusInfo
*/
void ClusterManager::setChangePartitionStatusInfo(
	ChangePartitionStatusInfo& changePartitionStatusInfo) {

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
			GS_THROW_USER_ERROR(
				GS_ERROR_TXN_PARTITION_STATE_INVALID, "");
		}
		pt_->setPartitionStatus(pId, status);
		if (changePartitionStatusInfo.isToSubMaster()) {
			pt_->clearRole(pId);
			pt_->setPartitionRoleStatus(pId, PartitionTable::PT_NONE);
		}
		if (status == PartitionTable::PT_OFF) {
			PartitionRoleStatus roleStatus
				= pt_->getPartitionRoleStatus(pId);
			if (roleStatus != PartitionTable::PT_NONE) {
				pt_->clearRole(pId);
				pt_->setPartitionRoleStatus(pId, PartitionTable::PT_NONE);
			}
		}
		if (status == PartitionTable::PT_STOP) {
			pt_->setPartitionRoleStatus(pId, PartitionTable::PT_NONE);
			pt_->setPartitionRevision(pId, 1);
		}
	}
	catch (std::exception& e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

/*!
	@brief Sets ChangePartitionTableInfo
*/
bool ClusterManager::setChangePartitionTableInfo(
	ChangePartitionTableInfo& changePartitionTableInfo) {

	try {
		bool currentBackupChange = false;
		PartitionRole& candRole
			= changePartitionTableInfo.getPartitionRole();
		PartitionId pId = candRole.getPartitionId();
		if (pt_->isBackup(pId, SELF_NODEID)
			&& candRole.isOwner(SELF_NODEID)) {
			currentBackupChange = true;
		}
		pt_->setPartitionRole(pId, candRole);
		if (!pt_->isOwnerOrBackup(pId, SELF_NODEID)) {
			pt_->setPartitionStatus(pId, PartitionTable::PT_OFF);
		}
		return currentBackupChange;
	}
	catch (std::exception& e) {
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
void ClusterManager::updateSecondMaster(
	NodeId target,
	int64_t startupTime) {

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

void ClusterManager::setSecondMaster(
	NodeAddress& secondMasterNode) {
	bool prevStatus = clusterInfo_.isSecondMaster_;
	if (pt_->getNodeAddress(SELF_NODEID)
		== secondMasterNode) {
		clusterInfo_.isSecondMaster_ = true;
	}
	else {
		clusterInfo_.isSecondMaster_ = false;
	}
	if (prevStatus != clusterInfo_.isSecondMaster_) {
		UTIL_TRACE_WARNING(CLUSTER_SERVICE,
			"Change second master to "
			<< clusterInfo_.isSecondMaster_);
	}
}

const char8_t* ClusterManager::
NodeStatusInfo::dumpNodeStatus() const {

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

void ClusterManager::detectMultiMaster() {
	statusInfo_.clusterErrorStatus_.detectMultiMaster_ = true;
}

/*!
	@brief Sets the digest of cluster information
*/
void ClusterManager::setDigest(
	const ConfigTable& configTable,
	util::XArray<uint8_t>& digestBinary,
	NodeAddressSet& addressInfoList,
	ClusterNotificationMode mode) {

	try {
		ClusterDigest digest;
		memset(static_cast<void*>(&digest), 0, sizeof(ClusterDigest));

		digest.partitionNum_ =
			configTable.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM);
		digest.replicationNum_ =
			configTable.get<int32_t>(
				CONFIG_TABLE_CS_REPLICATION_NUM);

		util::SocketAddress socketAddress;
		if (mode == NOTIFICATION_MULTICAST) {
			socketAddress.assign(
				configTable.get<const char8_t*>(
					CONFIG_TABLE_CS_NOTIFICATION_ADDRESS),
				configTable.getUInt16(
					CONFIG_TABLE_CS_NOTIFICATION_PORT));

			socketAddress.getIP(
				reinterpret_cast<util::SocketAddress::Inet*>(
					&digest.notificationClusterAddress_),
				&digest.notificationClusterPort_);

			socketAddress.assign(
				configTable.get<const char8_t*>(
					CONFIG_TABLE_TXN_NOTIFICATION_ADDRESS),
				configTable.getUInt16(
					CONFIG_TABLE_TXN_NOTIFICATION_PORT));

			socketAddress.getIP(
				reinterpret_cast<util::SocketAddress::Inet*>(
					&digest.notificationClientAddress_),
				&digest.notificationClientPort_);

			socketAddress.assign(
				configTable.get<const char8_t*>(
					CONFIG_TABLE_SQL_NOTIFICATION_ADDRESS),
				configTable.getUInt16(
					CONFIG_TABLE_SQL_NOTIFICATION_PORT));

			socketAddress.getIP(
				reinterpret_cast<util::SocketAddress::Inet*>(
					&digest.notificationSqlClientAddress_),
				&digest.notificationSqlClientPort_);
		}

		digest.notificationClusterInterval_ =
			configTable.get<int32_t>(
				CONFIG_TABLE_CS_NOTIFICATION_INTERVAL);
		digest.heartbeatInterval_ =
			configTable.get<int32_t>(
				CONFIG_TABLE_CS_HEARTBEAT_INTERVAL);
		digest.replicationMode_ =
			configTable.get<int32_t>(
				CONFIG_TABLE_TXN_REPLICATION_MODE);
		digest.chunkSize_ =
			configTable.get<int32_t>(
				CONFIG_TABLE_DS_STORE_BLOCK_SIZE);

		char digestData[SHA256_DIGEST_STRING_LENGTH + 1];

		digest.notificationMode_ = mode;
		digestBinary.push_back(
			reinterpret_cast<const uint8_t*>(&digest),
			sizeof(ClusterDigest));

		if (addressInfoList.size() > 0) {
			AddressInfo info;
			for (NodeAddressSetItr it = addressInfoList.begin();
				it != addressInfoList.end(); it++) {
				memset((char*)&info, 0, sizeof(AddressInfo));
				info = (*it);
				digestBinary.push_back(
					(const uint8_t*)&(info), sizeof(AddressInfo));
			}
		}
		if (pt_->hasPublicAddress()) {
			uint8_t publicVal = 1;
			digestBinary.push_back(
				(const uint8_t*)&(publicVal), sizeof(uint8_t));
		}
		SHA256_Data(
			reinterpret_cast<const uint8_t*>(digestBinary.data()),
			digestBinary.size(),
			digestData);

		clusterInfo_.digest_ = digestData;
	}
	catch (std::exception& e) {
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
		<< ", revisionNo:" << pt_->getPartitionRevision().getRevisionNo()
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
		<< CommonUtility::getTimeStr(
			util::DateTime::now(TRIM_MILLISECONDS).getUnixTime())
		<< terminateCode;
	ss << "# address list : " << terminateCode;

	ss << pt_->dumpNodes();

	return ss.str();
}

ClusterManager::ConfigSetUpHandler ClusterManager::configSetUpHandler_;

/*!
	@brief Handler Operator
*/
void ClusterManager::ConfigSetUpHandler::operator()(
	ConfigTable& config) {

	CONFIG_TABLE_RESOLVE_GROUP(config, CONFIG_TABLE_CS, "cluster");

	CONFIG_TABLE_ADD_SERVICE_ADDRESS_PARAMS(config, CS, 10010);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_CS_REPLICATION_NUM, INT32)
		.setMin(1)
		.setMax(PartitionTable::MAX_NODE_NUM - 1)
		.setDefault(2);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_CS_NOTIFICATION_ADDRESS, STRING)
		.inherit(CONFIG_TABLE_ROOT_NOTIFICATION_ADDRESS)
		.addExclusive(CONFIG_TABLE_CS_NOTIFICATION_ADDRESS)
		.addExclusive(CONFIG_TABLE_CS_NOTIFICATION_PROVIDER)
		.addExclusive(CONFIG_TABLE_CS_NOTIFICATION_MEMBER);

	CONFIG_TABLE_ADD_PORT_PARAM(
		config, CONFIG_TABLE_CS_NOTIFICATION_PORT, 20000);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_CS_NOTIFICATION_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
		.setMin(1)
		.setDefault(5);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_CS_HEARTBEAT_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
		.setMin(1)
		.setDefault(5);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_CS_LOADBALANCE_CHECK_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
		.setMin(1)
		.setDefault(180);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_CS_CLUSTER_NAME, STRING);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_CS_OWNER_BACKUP_LSN_GAP, INT32)
		.setMin(0)
		.setDefault(1000);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_CS_OWNER_CATCHUP_LSN_GAP, INT32)
		.setMin(0)
		.setDefault(20000);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_CS_MAX_LSN_GAP, INT32)
		.setMin(0)
		.setDefault(0);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_CS_MAX_LSN_REPLICATION_NUM, INT32)
		.setMin(1)
		.setDefault(1000);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_CS_CHECK_RULE_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
		.setMin(10)
		.setDefault(40);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_CS_DROP_CHECK_INTERVAL, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_DURATION_S)
		.setMin(30)
		.setDefault(300);

	picojson::value defaultValue;

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_CS_NOTIFICATION_MEMBER, JSON)
		.setDefault(defaultValue);

	config.resolveGroup(
		CONFIG_TABLE_CS, CONFIG_TABLE_CS_NOTIFICATION_PROVIDER,
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

	CONFIG_TABLE_ADD_PARAM(config,
		CONFIG_TABLE_CS_NOTIFICATION_INTERFACE_ADDRESS, STRING)
		.setDefault("");

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_CS_ABNORMAL_AUTO_SHUTDOWN, BOOL)
		.setExtendedType(ConfigTable::EXTENDED_TYPE_LAX_BOOL)
		.setDefault(true);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_CS_RACK_ZONE_AWARENESS, BOOL)
		.setExtendedType(ConfigTable::EXTENDED_TYPE_LAX_BOOL)
		.setDefault(false);

	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_CS_RACK_ZONE_ID, STRING)
		.setDefault("");
}

ClusterManager::StatSetUpHandler ClusterManager::statSetUpHandler_;

#define STAT_ADD(id) STAT_TABLE_ADD_PARAM(stat, parentId, id)

/*!
	@brief Handler Operator
*/
void ClusterManager::StatSetUpHandler::operator()(StatTable& stat) {
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

	STAT_ADD(STAT_TABLE_CS_CURRENT_RULE);
	STAT_ADD(STAT_TABLE_CS_APPLY_RULE_LIMIT_TIME);
	STAT_ADD(STAT_TABLE_CS_CLUSTER_REVISION_NO);
	STAT_ADD(STAT_TABLE_CS_AUTO_GOAL);
	STAT_ADD(STAT_TABLE_CS_RACKZONE_ID);
	STAT_ADD(STAT_TABLE_CS_PARTITION_REPLICATION_PROGRESS);
	STAT_ADD(STAT_TABLE_CS_PARTITION_GOAL_PROGRESS);

	stat.resolveGroup(parentId, STAT_TABLE_CS_ERROR, "errorInfo");

	parentId = STAT_TABLE_CS_ERROR;
	STAT_ADD(STAT_TABLE_CS_ERROR_REPLICATION);
	STAT_ADD(STAT_TABLE_CS_ERROR_CLUSTER_VERSION);
	STAT_ADD(STAT_TABLE_CS_ERROR_ALREADY_APPLIED_LOG);
	STAT_ADD(STAT_TABLE_CS_ERROR_INVALID_LOG);
	STAT_ADD(STAT_TABLE_CS_INVALID_PARTITION_STATUS);

	parentId = STAT_TABLE_ROOT;
	stat.resolveGroup(parentId, STAT_TABLE_PERF, "performance");
	parentId = STAT_TABLE_PERF;
	STAT_ADD(STAT_TABLE_PERF_OWNER_COUNT);
	STAT_ADD(STAT_TABLE_PERF_BACKUP_COUNT);
	STAT_ADD(STAT_TABLE_PERF_TOTAL_OWNER_LSN);
	STAT_ADD(STAT_TABLE_PERF_TOTAL_BACKUP_LSN);
	STAT_ADD(STAT_TABLE_PERF_TOTAL_OTHER_LSN);
	STAT_ADD(STAT_TABLE_PERF_TOTAL_CLUSTER_OTHER_LSN);
}

/*!
	@brief Handler Operator
*/
bool ClusterManager::StatUpdator::operator()(StatTable& stat) {

	if (!stat.getDisplayOption(STAT_TABLE_DISPLAY_SELECT_CS)) {
		return true;
	}

	ClusterManager& mgr = *manager_;
	PartitionTable& pt = *mgr.pt_;
	const NodeId masterNodeId = pt.getMaster();

	stat.set(STAT_TABLE_CS_NOTIFICATION_MODE, mgr.getNotificationMode());

	ClusterService* clsSvc = mgr.getService();
	ClusterService::NotificationManager& notifyMgr
		= clsSvc->getNotificationManager();
	if (notifyMgr.getMode() != NOTIFICATION_MULTICAST &&
		stat.getDisplayOption(
			STAT_TABLE_DISPLAY_OPTIONAL_NOTIFICATION_MEMBER)) {
		picojson::value value;
		notifyMgr.getNotificationMember(value);
		if (!value.is<picojson::null>()) {
			stat.set(STAT_TABLE_CS_NOTIFICATION_MEMBER, value);
		}
	}
	stat.set(STAT_TABLE_CS_NODE_STATUS,
		mgr.statusInfo_.getSystemStatus());
	stat.set(STAT_TABLE_CS_DESIGNATED_COUNT,
		mgr.getReserveNum());
	PartitionRevisionNo revisionNo = pt.getCurrentRevisionNo();
	if (!pt.isMaster()) {
		revisionNo = 0;
	}
	stat.set(STAT_TABLE_CS_SYNC_COUNT, revisionNo);
	if (!pt.isFollower()) {
		stat.set(STAT_TABLE_CS_ACTIVE_COUNT,
			mgr.getActiveNum());
	}

	if (stat.getDisplayOption(
		STAT_TABLE_DISPLAY_WEB_OR_DETAIL_TRACE)) {
		const char8_t* clusterStatus;
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

		const char8_t* partitionStatus;
		switch (pt.getPartitionSummaryStatus()) {
		case PartitionTable::PT_NORMAL:
			partitionStatus = "NORMAL";
			break;
		case PartitionTable::PT_REPLICA_LOSS:
			partitionStatus = "REPLICA_LOSS";
			break;
		case PartitionTable::PT_NOT_BALANCE:
			partitionStatus = "NOT_BALANCE";
			break;
		case PartitionTable::PT_OWNER_LOSS:
		case PartitionTable::PT_ABNORMAL:
			partitionStatus = "OWNER_LOSS";
			break;
		case PartitionTable::PT_INITIAL:
			partitionStatus = "INITIAL";
			break;
		default:
			partitionStatus = "UNKNOWN";
			break;
		}
		stat.set(STAT_TABLE_CS_PARTITION_STATUS, partitionStatus);
	}

	bool isDetail = false;
	if (stat.getDisplayOption(STAT_TABLE_DISPLAY_WEB_ONLY)) {
		const std::pair<ServiceType, bool>& addressType =
			statOptionToAddressType(stat);

		stat.set(STAT_TABLE_CS_STARTUP_TIME,
			CommonUtility::getTimeStr(mgr.getStartupTime()));

		stat.set(STAT_TABLE_CS_CLUSTER_NAME,
			mgr.getClusterName());

		if (pt.isOrgEnableRackZoneAwareness()) {
			std::string rackZoneIdName;
			pt.getRackZoneIdName(rackZoneIdName, 0);
			if (!rackZoneIdName.empty()) {
				stat.set(STAT_TABLE_CS_RACKZONE_ID, rackZoneIdName.c_str());
			}
		}

		char tmpBuffer[37];
		UUIDUtils::unparse(mgr.uuid_, (char*)tmpBuffer);
		stat.set(
			STAT_TABLE_CS_CLUSTER_REVISION_ID, &tmpBuffer[0]);

		if (pt.isMaster()) {
			stat.set(STAT_TABLE_CS_CURRENT_RULE,
				pt.dumpPartitionRule(pt.getRuleNo()));
			if (pt.getRuleLimitTime() > 0) {
				stat.set(STAT_TABLE_CS_APPLY_RULE_LIMIT_TIME,
					CommonUtility::getTimeStr(pt.getRuleLimitTime()));
			}
		}
		stat.set(STAT_TABLE_CS_CLUSTER_REVISION_NO,
			pt.getPartitionRevisionNo());

		const char8_t* loadBalancer;
		if (mgr.checkLoadBalance()) {
			loadBalancer = "ACTIVE";
		}
		else {
			loadBalancer = "INACTIVE";
		}
		stat.set(STAT_TABLE_CS_LOAD_BALANCER, loadBalancer);

		const char8_t* autoGoal;
		if (mgr.checkAutoGoal()) {
			autoGoal = "ACTIVE";
		}
		else {
			autoGoal = "INACTIVE";
		}
		stat.set(STAT_TABLE_CS_AUTO_GOAL, autoGoal);

		if (mgr.isInitialCluster()) {
			stat.set(STAT_TABLE_CS_INITIAL_CLUSTER, 1);
		}
		picojson::array nodeList;
		const int32_t nodeListSize = pt.getNodeNum();
		size_t activeNum = 0;
		for (NodeId nodeId = 0; nodeId < nodeListSize; nodeId++) {
			NodeAddress& address = pt.getNodeAddress(nodeId);
			if (address.isValid()) {
				if ((pt.isMaster() && nodeId != 0 &&
					(pt.getHeartbeatTimeout(nodeId) == UNDEF_TTL)) ||
					(pt.isFollower() && nodeId != 0 &&
						nodeId != masterNodeId)) {
					continue;
				}
				picojson::value follower;
				pt.getServiceAddress(
					nodeId, follower, addressType.first, addressType.second);
				nodeList.push_back(follower);
				activeNum++;
			}
			else {
				continue;
			}
			if (nodeId == masterNodeId) {
				picojson::value master;
				pt.getServiceAddress(
					nodeId, master, addressType.first, addressType.second);
				stat.set(STAT_TABLE_CS_MASTER, master);
			}
		}
		if (activeNum != 0) {
			nodeList.resize(activeNum);
		}
		stat.set(STAT_TABLE_CS_NODE_LIST,
			picojson::value(nodeList));
	}

	if (stat.getDisplayOption(STAT_TABLE_DISPLAY_WEB_ONLY) &&
		stat.getDisplayOption(STAT_TABLE_DISPLAY_OPTIONAL_CS)) {
		isDetail = true;
		const ErrorCode codeList[] = {
				GS_ERROR_TXN_CLUSTER_ROLE_UNMATCH,
				GS_ERROR_CS_ENCODE_DECODE_VERSION_CHECK,
				GS_ERROR_RM_ALREADY_APPLY_LOG,
				GS_ERROR_TXN_REPLICATION_LOG_LSN_INVALID,
				GS_ERROR_PT_CHECK_PARTITION_STATUS_FAILED };
		const StatTableParamId paramList[] = {
				STAT_TABLE_CS_ERROR_REPLICATION,
				STAT_TABLE_CS_ERROR_CLUSTER_VERSION,
				STAT_TABLE_CS_ERROR_ALREADY_APPLIED_LOG,
				STAT_TABLE_CS_ERROR_INVALID_LOG,
				STAT_TABLE_CS_INVALID_PARTITION_STATUS };

		for (size_t i = 0; i < sizeof(codeList) / sizeof(*codeList); i++) {
			const uint64_t value = mgr.getErrorCount(codeList[i]);
			if (value > 0) {
				stat.set(paramList[i], value);
			}
		}
	}
	int32_t ownerCount = 0;
	int32_t backupCount = 0;
	uint64_t totalOwnerLsn = 0;
	uint64_t totalBackupLsn = 0;
	uint64_t totalOtherLsn = 0;
	uint64_t clusterTotalOtherLsn = 0;
	bool isMaster = pt.isMaster();
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
			clusterTotalOtherLsn += pt.getLSN(pId);
		}
		if (isMaster && isDetail) {
			for (NodeId nodeId = 1;nodeId < pt.getNodeNum();nodeId++) {
				if (!pt.checkLiveNode(nodeId)) {
					continue;
				}
				if (!pt.isOwnerOrBackup(
					pId, nodeId, PartitionTable::PT_CURRENT_OB)) {
					clusterTotalOtherLsn += pt.getLSN(pId, nodeId);
				}
			}
		}
	}
	stat.set(STAT_TABLE_PERF_OWNER_COUNT, ownerCount);
	stat.set(STAT_TABLE_PERF_BACKUP_COUNT, backupCount);
	stat.set(STAT_TABLE_PERF_TOTAL_OWNER_LSN, totalOwnerLsn);
	stat.set(STAT_TABLE_PERF_TOTAL_BACKUP_LSN, totalBackupLsn);
	stat.set(STAT_TABLE_PERF_TOTAL_OTHER_LSN, totalOtherLsn);
	if (isMaster && isDetail) {
		stat.set(STAT_TABLE_PERF_TOTAL_CLUSTER_OTHER_LSN,
			clusterTotalOtherLsn);
	}

	if (isMaster) {
		if (isDetail) {
			double goalProgress = 0;
			double replicaProgress = 0;
			pt.progress(goalProgress, replicaProgress);
			stat.set(STAT_TABLE_CS_PARTITION_REPLICATION_PROGRESS, replicaProgress);
			stat.set(STAT_TABLE_CS_PARTITION_GOAL_PROGRESS, goalProgress);
		}
	}

	return true;
}

void ClusterManager::initialize(
	ClusterService* clsSvc, EventEngine* ee) {

	clsSvc_ = clsSvc;
	ee_ = ee;
	int64_t baseTime = ee_->getMonotonicTime();
	pt_->setHeartbeatTimeout(
		SELF_NODEID, nextHeartbeatTime(baseTime));
}

ClusterService* ClusterManager::getService() {
	return clsSvc_;
}

void ClusterManager::Config::setUpConfigHandler(
	ClusterManager* clsMgr, ConfigTable& configTable) {

	clsMgr_ = clsMgr;
	configTable.setParamHandler(
		CONFIG_TABLE_CS_OWNER_BACKUP_LSN_GAP, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_CS_OWNER_CATCHUP_LSN_GAP, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_CS_CHECK_RULE_INTERVAL, *this);
	configTable.setParamHandler(
		CONFIG_TABLE_CS_DROP_CHECK_INTERVAL, *this);
}

void ClusterManager::Config::operator()(
	ConfigTable::ParamId id, const ParamValue& value) {

	switch (id) {
	case CONFIG_TABLE_CS_CHECKPOINT_DELAY_INTERVAL:
		break;
	case CONFIG_TABLE_CS_OWNER_BACKUP_LSN_GAP:
		clsMgr_->getPartitionTable()->getConfig()
			.setLimitOwnerBackupLsnGap(value.get<int32_t>());
		break;
	case CONFIG_TABLE_CS_OWNER_CATCHUP_LSN_GAP:
		clsMgr_->getPartitionTable()->getConfig()
			.setLimitOwnerCatchupLsnGap(value.get<int32_t>());
		break;
	case CONFIG_TABLE_CS_CHECK_RULE_INTERVAL:
		clsMgr_->getConfig().setRuleLimitInterval(
			value.get<int32_t>());
		break;
	case CONFIG_TABLE_CS_DROP_CHECK_INTERVAL:
		clsMgr_->getConfig().setCheckDropInterval(
			value.get<int32_t>());
		break;
	}
}

EventMonotonicTime ClusterManager::getMonotonicTime() {
	return ee_->getMonotonicTime() + clusterInfo_.startupTime_ +
		EVENT_MONOTONIC_ADJUST_TIME;
}
