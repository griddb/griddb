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
	@brief Definition of ClusterManager
*/

#ifndef CLUSTER_MANAGER_H_
#define CLUSTER_MANAGER_H_

#include "util/container.h"
#include "cluster_common.h"
#include "cluster_event_type.h"
#include "partition_table.h"
#include "uuid_utils.h"
#include "util/net.h"

class EventEngine;
class PartitionTable;
class ClusterService;
class ClusterManager;
class NotifyClusterInfo;
class NotifyClusterResInfo;
class ChangePartitionStatusInfo;
class ChangePartitionTableInfo;
class HeartbeatInfo;
class HeartbeatCheckInfo;
class HeartbeatResInfo;
class UpdatePartitionInfo;
class JoinClusterInfo;
class LeaveClusterInfo;
class IncreaseClusterInfo;
class DecreaseClusterInfo;

/*!
	@brief cluster status after transition
*/
enum ClusterStatusTransition { TO_MASTER, TO_SUBMASTER, KEEP };

/*!
	@brief ClusterManager
*/
class ClusterManager {

	class ClusterConfig;
	class ExtraConfig;

	friend class SystemService;
	friend class ClusterService;
	friend class HeartbeatHandler;
	friend class HeartbeatResInfo;
	friend class NotifyClusterInfo;

public:

	struct ClusterInfo;
	static const int32_t EVENT_MONOTONIC_ADJUST_TIME = 1000;

	/*!
		@brief Node status
	*/
	enum NodeStatus {
		SYS_STATUS_BEFORE_RECOVERY,
		SYS_STATUS_INACTIVE,
		SYS_STATUS_ACTIVE,
		SYS_STATUS_SHUTDOWN_NORMAL,
		SYS_STATUS_MAX = 4
	};

	/*!
		@brief Error type about cluster
	*/
	enum ClusterErrorType {
		STATUS_NORMAL,
		ALREADY_JOIN_CLUSTER,
		NOT_JOIN_CLUSTER,
		IS_SUBMASTER,
		IS_FOLLOWER,
		NOT_MASTER,
		NOT_FOLLOWER
	};

	ClusterManager(
			const ConfigTable &configTable,
			PartitionTable *partitionTable,
			ClusterVersionId versionId = 0);
	
	~ClusterManager();

	ClusterConfig &getConfig() {
		return clusterConfig_;
	}

	ExtraConfig &getExtraConfig() {
		return extraConfig_;
	}

	StatTable::StatUpdator &getStatUpdator() {
		return statUpdator_;
	}

	void checkNodeStatus();

	bool isError() {
		return !statusInfo_.checkNodeStatus();
	}

	void checkActiveStatus();

	bool isActive() {
		return (statusInfo_.currentStatus_ == SYS_STATUS_ACTIVE &&
				statusInfo_.nextStatus_ == SYS_STATUS_ACTIVE);
	}

	void checkCommandStatus(EventType operation);

	
	NodeAddress getPartition0NodeAddr() {
		util::LockGuard<util::Mutex> lock(clusterLock_);
		return clusterInfo_.partition0NodeAddr_;
	}

	NodeAddress getPartition0NodeAddrOnMaster() {
		return partition0NodeAddrOnMaster_;
	}
	
	void setPartition0NodeAddrOnMaster(NodeAddress nodeAddress) {
		partition0NodeAddrOnMaster_ = nodeAddress;
	}

	void initialize(
			ClusterService *clsSvc, EventEngine *ee);

	typedef int64_t EventMonotonicTime;
	EventMonotonicTime getMonotonicTime();

	std::string getClusterName();

	void checkClusterStatus(EventType operation);

	void updateNodeStatus(EventType eventType);

	bool isShutdownPending() {
		return statusInfo_.isShutdownPending_;
	}

	bool setSystemError(bool reset = true) {
		return statusInfo_.setSystemError(reset);
	}

	bool isSystemError() {
		return statusInfo_.isSystemError_;
	}

	bool isNormalShutdownCall() {
		return statusInfo_.isNormalShutdownCall_;
	}

	void updateClusterStatus(
		ClusterStatusTransition status, bool isLeave = false);

	bool checkRecoveryCompleted() {
		return (statusInfo_.currentStatus_
				!= SYS_STATUS_BEFORE_RECOVERY);
	}

	bool isInitialCluster() {
		return clusterInfo_.isInitialCluster_;
	}

	bool checkLoadBalance() {
		return clusterInfo_.isLoadBalance_;
	}

	bool checkAutoGoal() {
		return clusterInfo_.isAutoGoal_;
	}

	void setLoadBalance(bool flag) {
		clusterInfo_.isLoadBalance_ = flag;
	}

	void setAutoGoal(bool flag) {
		clusterInfo_.isAutoGoal_ = flag;
	}

	bool isUpdatePartition() {
		bool retVal = clusterInfo_.isUpdatePartition_;
		clusterInfo_.isUpdatePartition_ = false;
		return retVal;
	}

	void setUpdatePartition() {
		clusterInfo_.isUpdatePartition_ = true;
	}

	bool isRepairPartition() {
		bool retVal = clusterInfo_.isRepairPartition_;
		clusterInfo_.isRepairPartition_ = false;
		return retVal;
	}

	void setRepairPartition() {
		clusterInfo_.isRepairPartition_ = true;
	}

	bool isAddOrDownNode() {
		bool retVal = clusterInfo_.isAddOrDownNode_;
		clusterInfo_.isAddOrDownNode_ = false;
		return retVal;
	}

	void setAddOrDownNode() {
		clusterInfo_.isAddOrDownNode_ = true;
	}

	void setSignalBeforeRecovery();

	util::Mutex &getClusterLock() {
		return clusterLock_;
	}

	bool isSignalBeforeRecovery() {
		return isSignalBeforeRecovery_;
	}

	void setNotificationMode(ClusterNotificationMode mode) {
		clusterInfo_.mode_ = mode;
	}

	const char *getNotificationMode();

	ClusterService *getService();

	/*!
		@brief Represents Synchronization Statistics
	*/
	struct SyncStat {
		
		SyncStat() : syncChunkNum_(0), syncApplyNum_(0) {}
		
		void init() {
			syncChunkNum_ = 0;
			syncApplyNum_ = 0;
		}

		int64_t syncChunkNum_;
		int64_t syncApplyNum_;
	};

	SyncStat &getSyncStat() {
		return syncStat_;
	}

	/*!
		@brief Handlers error of ClusterManager
	*/
	class ErrorManager {

	public:
		
		static const int32_t MAX_ERROR_COUNT = 7;
		static const int32_t DEFAULT_DUMP_COUNT = 1000;

		ErrorManager();

		void reset();

		void setDumpCount(ErrorCode errorCode, uint32_t count);

		bool reportError(ErrorCode errorCode, int32_t counter = 1);

		uint64_t getErrorCount(ErrorCode errorCode);

	private:

		uint32_t getListNo(ErrorCode errorCode);
		
		std::vector<uint64_t> errorCountList_;
		std::vector<uint64_t> dumpCountList_;
	};

	bool reportError(int32_t errorCode, int32_t count = 1);

	uint64_t getErrorCount(int32_t errorCode);


	std::string dump();

	int32_t getConcurrency() {
		return concurrency_;
	}

	void getHeartbeatInfo(
			HeartbeatInfo &heartbeatInfo);

	bool setHeartbeatInfo(
			HeartbeatInfo &heartbeatInfo);

	void getHeartbeatResInfo(
			HeartbeatResInfo &heartbeatResInfo);

	void setHeartbeatResInfo(
			HeartbeatResInfo &heartbeatResInfo);

	void getHeartbeatCheckInfo(
			HeartbeatCheckInfo &heartbeatCheckInfo);

	void getUpdatePartitionInfo(
			UpdatePartitionInfo &updatePartitionInfo);

	void setUpdatePartitionInfo(
			UpdatePartitionInfo &updatePartitionInfo);

	void getNotifyClusterInfo(
			NotifyClusterInfo &notifyClusterInfo);

	bool setNotifyClusterInfo(
			NotifyClusterInfo &notifyClusterInfo);

	void getNotifyClusterResInfo(
			NotifyClusterResInfo &notifyClusterResInfo);

	void setNotifyClusterResInfo(
			NotifyClusterResInfo &notifyClusterResInfo);

	void setJoinClusterInfo(
			JoinClusterInfo &joinClusterInfo);

	void setLeaveClusterInfo(
			LeaveClusterInfo &leaveClusterInfo);

	void setIncreaseClusterInfo(
			IncreaseClusterInfo &increaseClusterInfo);

	void setDecreaseClusterInfo(
			DecreaseClusterInfo &decreaseClusterInfo);

	void setChangePartitionStatusInfo(
			ChangePartitionStatusInfo &changePartitionStatusInfo);

	bool setChangePartitionTableInfo(
			ChangePartitionTableInfo &changePartitionStatusInfo);

	int64_t nextHeartbeatTime(int64_t baseTime);

	bool isSecondMaster() {
		return clusterInfo_.isSecondMaster_;
	}

	void setDigest(
			const ConfigTable &configTable,
			util::XArray<uint8_t> &digestBinary,
			NodeAddressSet &addressInfoList,
			ClusterNotificationMode mode);

	PartitionTable *getPartitionTable() {
		return pt_;
	}

	int32_t getReserveNum() {
		return clusterInfo_.reserveNum_;
	}

private:

	void getSafetyLeaveNodeList(
			util::StackAllocator &alloc,
			NodeIdList &candList,
			int32_t removeNodeNum = 1);

	bool isJoinCluster() {
		return clusterInfo_.isJoinCluster_;
	}

	void setJoinCluster(bool flag) {
		clusterInfo_.isJoinCluster_ = flag;
	}

	void setInitialCluster(bool isInitialCluster) {
		clusterInfo_.isInitialCluster_ = isInitialCluster;
	}

	int32_t getIntialClusterNum() {
		return clusterInfo_.initialClusterConstructNum_;
	}

	void setIntialClusterNum(int32_t initalClusterNum) {
		clusterInfo_.initialClusterConstructNum_
				= initalClusterNum;
	}

	void setReserveNum(int32_t reserveNodeNum);

	int32_t getQuorum() {
		return clusterInfo_.quorum_;
	}

	void setClusterName(const std::string &clusterName);

	void setPartition0NodeAddr(NodeAddress &nodeAddr);

	bool isNewNode() {
		return (clusterInfo_.initialClusterConstructNum_ == 0);
	}

	bool checkActiveNodeMax() {
		return (clusterInfo_.reserveNum_
				<= clusterInfo_.activeNodeNum_);
	}

	bool checkSteadyStatus() {
		return clusterInfo_.isStable();
	}

	int32_t getActiveNum() {
		return clusterInfo_.activeNodeNum_;
	}

	void setActiveNodeList(NodeIdList &activeNodeList) {
		clusterInfo_.activeNodeNum_
				= static_cast<int32_t>(activeNodeList.size());
	}

	void clearActiveNodeList() {
		clusterInfo_.activeNodeNum_ = 0;
	}

	std::string &getDigest() {
		return clusterInfo_.digest_;
	}

	int64_t getStartupTime() {
		return clusterInfo_.startupTime_;
	}

	bool checkParentMaster() {
		return clusterInfo_.isParentMaster_;
	}

	void setParentMaster(bool isParentMaster) {
		clusterInfo_.isParentMaster_ = isParentMaster;
	}

	void setNotifyPendingCount();

	int32_t updateNotifyPendingCount();

	void detectMutliMaster();

	void setSecondMaster(
			NodeAddress &secondMasterNode);

	void updateSecondMaster(
			NodeId target, int64_t startupTime);

	void setShutdownPending() {
		statusInfo_.isShutdownPending_ = true;
	}

	/*!
		@brief Represents ClusterManager config
	*/
	class ClusterConfig {
	public:
		static const int32_t DEFAULT_CLEAR_BLOCK_INTERVAL = 60;

		ClusterConfig(const ConfigTable &config);

		int32_t getHeartbeatInterval() const {
			return heartbeatInterval_;
		}

		int32_t getNotifyClusterInterval() const {
			return notifyClusterInterval_;
		}

		int32_t getNotifyClientInterval() const {
			return notifyClientInterval_;
		}

		int32_t getCheckLoadBalanceInterval() const {
			return checkLoadBalanceInterval_;
		}

		int32_t getCheckDropInterval() {
			return checkDropInterval_;
		}

		int32_t setCheckDropInterval(int32_t interval)  {
			return checkDropInterval_ = interval;
		}

		const std::string getSetClusterName() const {
			return setClusterName_;
		}

		int32_t getShortTermTimeoutInterval() const {
			return shortTermTimeoutInterval_;
		}

		void setClusterName(std::string &clusterName) {
			setClusterName_ = clusterName;
		}

		bool setShortTermTimeoutInterval(int32_t interval);

		int32_t getBlockClearInterval() {
			return blockClearInterval_;
		}

		void setRuleLimitInterval(int32_t interval);

		int32_t getRuleLimitInterval() {
			return ruleLimitInterval_;
		}

		bool isAutoShutdown() {
			return abnormalAutoShutdown_;
		}

	private:

		int32_t heartbeatInterval_;
		int32_t notifyClusterInterval_;
		int32_t notifyClientInterval_;
		int32_t checkLoadBalanceInterval_;
		int32_t shortTermTimeoutInterval_;
		int32_t blockClearInterval_;
		int32_t ruleLimitInterval_;
		int32_t checkDropInterval_;
		std::string setClusterName_;
		bool abnormalAutoShutdown_;
	};

	/*!
		@brief Represents extra config of ClusterManager
	*/

	class ExtraConfig {

		static const int32_t CS_RECONSTRUCT_WAIT_TIME = 3;
		static const int32_t NEXT_HEARTBEAT_MARGIN = 2;
		static const uint32_t CLUSTER_NAME_STRING_MAX = 64;

	public:

		ExtraConfig();

		int32_t getClusterReconstructWaitTime() const {
			return clusterReconstructWaitTime_;
		}

		int32_t getNextHeartbeatMargin() const {
			return nextHeartbeatMargin_;
		}

		int32_t getMaxClusterNameSize() const {
			return maxClusterNameSize_;
		}

	private:

		int32_t clusterReconstructWaitTime_;
		int32_t nextHeartbeatMargin_;
		int32_t maxClusterNameSize_;
	};

	/*!
		@brief Represents the information of gs_cluster.json file
	*/
	struct ClusterDigest {

		PartitionId partitionNum_;
		int32_t replicationNum_;
		int32_t notificationClusterAddress_;
		uint16_t notificationClusterPort_;
		int32_t notificationClusterInterval_;
		int32_t heartbeatInterval_;
		int32_t notificationClientAddress_;
		uint16_t notificationClientPort_;
		int32_t replicationMode_;
		Size_t chunkSize_;
		int32_t notificationSqlClientAddress_;
		uint16_t notificationSqlClientPort_;
		ClusterNotificationMode notificationMode_;
	};

	struct Config : public ConfigTable::ParamHandler {

		Config() : clsMgr_(NULL) {};
		
		void setUpConfigHandler(
			ClusterManager *clsMgr, ConfigTable &configTable);
		
		virtual void operator()(
			ConfigTable::ParamId id, const ParamValue &value);
		
		ClusterManager *clsMgr_;
	};

	/*!
		@brief Represents cluster information
	*/
	public:

	struct ClusterInfo {

		ClusterInfo(PartitionTable *pt);

		bool isStable() {
			return (reserveNum_ == activeNodeNum_);
		}

		void initLeave();


		std::string clusterName_;
		std::string digest_;
		int64_t startupTime_;
		int32_t reserveNum_;
		bool isSecondMaster_;
		bool isJoinCluster_;
		bool isInitialCluster_;
		int32_t initialClusterConstructNum_;
		bool isParentMaster_;
		NodeId secondMasterNodeId_;
		int64_t secondMasterStartupTime_;
		NodeAddress secondMasterNodeAddress_;
		int32_t notifyPendingCount_;
		int32_t activeNodeNum_;
		int32_t quorum_;
		NodeId prevMaxNodeId_;
		bool isLoadBalance_;
		bool isAutoGoal_;
		bool isUpdatePartition_;
		bool isRepairPartition_;
		PartitionTable *pt_;
		std::set<NodeId> downNodeList_;
		NodeAddress partition0NodeAddr_;  
		bool isAddOrDownNode_;
		ClusterNotificationMode mode_;
	};

	ClusterInfo &getClusterInfo() {
		return clusterInfo_;
	}
	
	void dumpPartition(
			util::StackAllocator &alloc,
			PartitionTableType tableType);

private:

	void setupPartitionContext(
			PartitionTable::PartitionContext &context);

	/*!
		@brief Represents node status
	*/
	struct NodeStatusInfo {

		NodeStatusInfo() :
				isSystemError_(false),
				currentStatus_(SYS_STATUS_BEFORE_RECOVERY),
				nextStatus_(SYS_STATUS_BEFORE_RECOVERY),
				isShutdown_(false),
				isShutdownPending_(false),
				isNormalShutdownCall_(false) {}

		bool checkNodeStatus();

		bool checkCommandStatus(EventType operation);

		void updateNodeStatus(EventType operation);

		const char8_t *getSystemStatus() const;

		bool setSystemError(bool reset = true) {
			if (isSystemError_) {
				return false;
			}
			isSystemError_ = reset;
			return true;
		}

		/*!
			@brief Represents cluster error status
		*/
		struct ClusterErrorStatus {

			ClusterErrorStatus() : detectMutliMaster_(false){};
			bool detectMutliMaster_;
		};

		volatile bool isSystemError_;
		ClusterErrorStatus clusterErrorStatus_;
		const char8_t *dumpNodeStatus() const;
		NodeStatus currentStatus_;
		NodeStatus nextStatus_;
		bool isShutdown_;
		bool isShutdownPending_;
		bool isNormalShutdownCall_;
	};

	static class ConfigSetUpHandler : public ConfigTable::SetUpHandler {

		virtual void operator()(ConfigTable &config);
	} configSetUpHandler_;

	static class StatSetUpHandler : public StatTable::SetUpHandler {

		virtual void operator()(StatTable &stat);
	} statSetUpHandler_;

	class StatUpdator : public StatTable::StatUpdator {

		virtual bool operator()(StatTable &stat);

	public:
		
		ClusterManager *manager_;
	} statUpdator_;

	ClusterConfig clusterConfig_;
	ExtraConfig extraConfig_;
	ClusterInfo clusterInfo_;
	util::Mutex clusterLock_;
	PartitionTable *pt_;
	NodeStatusInfo statusInfo_;
	ClusterVersionId versionId_;
	bool isSignalBeforeRecovery_;

	int32_t concurrency_;
	SyncStat syncStat_;
	ErrorManager errorMgr_;

	int64_t clusterSequenceNumber_;

	EventEngine *ee_;

	NodeAddress
		partition0NodeAddrOnMaster_;  
	ClusterService *clsSvc_;
	Config config_;
	uint8_t uuid_[UUID_BYTE_SIZE];

};

template <class T>
std::string dumpPartitionList(
		PartitionTable *pt, T &partitionList) {
	util::NormalOStringStream ss;
	int32_t listSize = static_cast<int32_t>(partitionList.size());
	ss << "[";
	for (int32_t pos = 0; pos < listSize; pos++) {
		ss << "(pId=" << partitionList[pos];
		ss << ", lsn=" << pt->getLSN(partitionList[pos]) << ")";
		if (pos != listSize - 1) {
			ss << ",";
		}
	}
	ss << "]";
	return ss.str().c_str();
}

/*!
	@brief Base Class of ClusterManager
*/
class ClusterManagerInfo {

public:
	
	ClusterManagerInfo(
			util::StackAllocator &alloc, NodeId nodeId) :
					alloc_(&alloc),
					senderNodeId_(nodeId),
					validCheckValue_(0),
					errorType_(WEBAPI_NORMAL) {}

	ClusterManagerInfo() {}

	bool isValid() {
		return (validCheckValue_ == VALID_VALUE);
	}

	virtual bool check() = 0;

	util::StackAllocator &getAllocator() {
		return *alloc_;
	}

	void encode();

	NodeId getSenderNodeId() {
		return senderNodeId_;
	}

	void setErrorType(WebAPIErrorType errorType) {
		errorType_ = errorType;
	}

	WebAPIErrorType getErrorType() {
		return errorType_;
	}

	MSGPACK_DEFINE(validCheckValue_);

protected:
	
	util::StackAllocator *alloc_;
	NodeId senderNodeId_;

private:
	
	uint8_t validCheckValue_;
	static const uint8_t VALID_VALUE = 0;
	WebAPIErrorType errorType_;
};

class RefreshPartitionInfo : public ClusterManagerInfo {
public:

	RefreshPartitionInfo(util::StackAllocator &alloc) :
			ClusterManagerInfo(alloc, 0) {}

	void set(util::XArray<PartitionId> &pIdList) {

		for(size_t pos = 0;pos < pIdList.size(); pos++) {
			syncPIdList_.push_back(pIdList[pos]);
		}
	}
	void set(PartitionTable *pt) {

		for(PartitionId pId = 0;
				pId < pt->getPartitionNum(); pId++) {
			syncPIdList_.push_back(pId);
		}
	}

	bool check() {return true;}

	PartitionIdList &getSyncPIdList() {
		return syncPIdList_;
	}

	MSGPACK_DEFINE(syncPIdList_);

private:

	PartitionIdList syncPIdList_;
};

/*!
	@brief Represents response information of NotifyCluster event
*/
class NotifyClusterResInfo : public ClusterManagerInfo {
public:

	NotifyClusterResInfo(
		util::StackAllocator &alloc,
		NodeId nodeId, PartitionTable *pt) :
				ClusterManagerInfo(alloc, nodeId),
				reserveNum_(0),
				partitionSequentialNumber_(0),
				pt_(pt) {}

	bool check() {
		return true;
	}

	void validate(uint64_t partitionNum);

	void set(ClusterManager::ClusterInfo &clusterInfo,
			PartitionTable *pt);

	int32_t getReserveNum() {
		return reserveNum_;
	}

	int64_t getStartupTime() {
		return startupTime_;
	}

	LsnList &getLsnList() {
		return lsnList_;
	}

	LsnList &getMaxLsnList() {
		return maxLsnList_;
	}

	AddressInfoList &getNodeAddressList() {
		return nodeList_;
	}

	PartitionRevisionNo getPartitionRevisionNo() {
		return partitionSequentialNumber_;
	}

	AddressInfoList &getNodeAddressInfoList() {
		return nodeList_;
	}

	void setPublicNodeAddressInfoList(
			AddressInfoList &addressList) {
		publicNodeList_ = addressList;
	}

	AddressInfoList &getPublicNodeAddressInfoList() {
		return publicNodeList_;
	}

	MSGPACK_DEFINE(reserveNum_, startupTime_,
			partitionSequentialNumber_,
			lsnList_, maxLsnList_, nodeList_);

private:

	int32_t reserveNum_;
	int64_t startupTime_;
	PartitionRevisionNo partitionSequentialNumber_;
	LsnList lsnList_;
	LsnList maxLsnList_;
	AddressInfoList nodeList_;
	AddressInfoList publicNodeList_;
	PartitionTable *pt_;
};

/*!
	@brief Represents the information of JoinCluster command
*/
class JoinClusterInfo : public ClusterManagerInfo {
public:

	JoinClusterInfo(
		util::StackAllocator &alloc,
		NodeId nodeId,
		bool precheck) :
				ClusterManagerInfo(alloc, nodeId),
				minNodeNum_(0),
				isPreCheck_(precheck) {}

	JoinClusterInfo(bool isPreCheck) :
			minNodeNum_(0), isPreCheck_(isPreCheck) {}

	bool check() {
		return true;
	}

	void set(const std::string &clusterName,
			int32_t minNodeNum) {
		clusterName_ = clusterName;
		minNodeNum_ = minNodeNum;
	}

	std::string &getClusterName() {
		return clusterName_;
	}

	int32_t getMinNodeNum() {
		return minNodeNum_;
	}

	bool isPreCheck() {
		return isPreCheck_;
	}

	MSGPACK_DEFINE(clusterName_, minNodeNum_);

private:

	std::string clusterName_;
	int32_t minNodeNum_;
	bool isPreCheck_;
};

/*!
	@brief Represents the information of LeaveCluster command
*/
class LeaveClusterInfo : public ClusterManagerInfo {
public:
	LeaveClusterInfo(
			util::StackAllocator &alloc,
			NodeId nodeId = 0,
			bool preCheck = false,
			bool isForce = true) :
					ClusterManagerInfo(alloc, nodeId),
					isPreCheck_(preCheck),
					isForce_(isForce) {}

	bool check() {
		return true;
	}

	bool isForceLeave() {
		return isForce_;
	}

	bool isPreCheck() {
		return isPreCheck_;
	}

private:

	bool isPreCheck_;
	bool isForce_;
};

/*!
	@brief Represents the information of IncreaseCluster command
*/

class IncreaseClusterInfo : public ClusterManagerInfo {
public:

	IncreaseClusterInfo(
			util::StackAllocator &alloc,
			NodeId nodeId,
			bool precheck = false,
			int32_t addNodeNum = 1) :
					ClusterManagerInfo(alloc, nodeId),
					addNodeNum_(addNodeNum),
					isPreCheck_(precheck),
					targetNodeId_(UNDEF_NODEID) {}

	bool check() {
		return true;
	}

	int32_t getAddNodeNum() {
		return addNodeNum_;
	}

	NodeAddress &getTargetAddress() {
		return targetAddress_;
	}

	bool isPreCheck() {
		return isPreCheck_;
	}

	MSGPACK_DEFINE(addNodeNum_, targetAddress_);

private:
	int32_t addNodeNum_;
	NodeAddress targetAddress_;

	bool isPreCheck_;
	NodeId targetNodeId_;
};

/*!
	@brief Represents theinformation of DecreaseCluster command
*/
class DecreaseClusterInfo : public ClusterManagerInfo {

public:

	DecreaseClusterInfo(
			util::StackAllocator &alloc,
			NodeId nodeId,
			PartitionTable *pt,
			bool precheck = false,
			bool isAutoLeave = true,
			int32_t leaveNodeNum = 1) :
					ClusterManagerInfo(alloc, nodeId),
					isAutoLeave_(isAutoLeave),
					leaveNodeNum_(leaveNodeNum),
					isPreCheck_(precheck),
					pt_(pt) {}

	bool check() {
		return true;
	}

	NodeIdList &getLeaveNodeList() {
		return leaveNodeList_;
	}

	bool isAutoLeave() {
		return isAutoLeave_;
	}

	int32_t getLeaveNodeNum() {
		return leaveNodeNum_;
	}

	bool isPreCheck() {
		return isPreCheck_;
	}

	void setPreCheck(bool preCheck) {
		isPreCheck_ = preCheck;
	}

	MSGPACK_DEFINE(
			isAutoLeave_, leaveNodeNum_, leaveNodeList_);

private:

	bool isAutoLeave_;
	int32_t leaveNodeNum_;
	NodeIdList leaveNodeList_;

	bool isPreCheck_;
	PartitionTable *pt_;
};

/*!
	@brief Represents the information of ShutdownNode command
*/
class ShutdownNodeInfo : public ClusterManagerInfo {

public:

	ShutdownNodeInfo(
			util::StackAllocator &alloc,
			NodeId nodeId,
			bool force) :
					ClusterManagerInfo(alloc, nodeId),
						isForce_(force) {}

	bool check() {
		return true;
	}

	MSGPACK_DEFINE(isForce_);

private:

	bool isForce_;
};

/*!
	@brief Represents the information of Shutdown command
*/
class ShutdownClusterInfo : public ClusterManagerInfo {

public:

	ShutdownClusterInfo(
			util::StackAllocator &alloc, NodeId nodeId) :
					ClusterManagerInfo(alloc, nodeId) {}

	bool check() {
		return true;
	}
};

/*!
	@brief Represents completed Checkpoint information of ShutdownNode
command
*/
class CompleteCheckpointInfo : public ClusterManagerInfo {

public:

	CompleteCheckpointInfo(
			util::StackAllocator &alloc, NodeId nodeId) :
					ClusterManagerInfo(alloc, nodeId) {}

	bool check() {
		return true;
	}
};

/*!
	@brief Represents the information for check heartbeat
*/
class HeartbeatCheckInfo : public ClusterManagerInfo {

public:

	HeartbeatCheckInfo(
			util::StackAllocator &alloc, NodeId nodeId = 0) :
					ClusterManagerInfo(alloc, nodeId),
					nextTransition_(KEEP),
					isAddNewNode_(false) {}

	bool check() {
		return true;
	}

	ClusterStatusTransition getNextTransition() {
		return nextTransition_;
	}

	NodeIdList &getActiveNodeList() {
		return activeNodeList_;
	}

	bool isAddNewNode() {
		return isAddNewNode_;
	}

	void set(
			ClusterStatusTransition nextTransition,
			bool isAddNewNode) {

		nextTransition_ = nextTransition;
		isAddNewNode_ = isAddNewNode;
	}

private:

	ClusterStatusTransition nextTransition_;
	NodeIdList activeNodeList_;
	bool isAddNewNode_;
};

/*!
	@brief Represents the information of UpdatePartition event
*/

class UpdatePartitionInfo : public ClusterManagerInfo {

public:

	UpdatePartitionInfo(
			util::StackAllocator &alloc,
			NodeId nodeId,
			PartitionTable *pt,
			bool isAddNewNode = false,
			bool needUpdatePartition = false) :
					ClusterManagerInfo(alloc, nodeId),
					dropPartitionNodeInfo_(alloc),
					shorttermSyncPosList_(alloc),
					changePartitionPosList_(alloc),
					longtermSyncPosList_(alloc),
					isAddNewNode_(isAddNewNode),
					needUpdatePartition_(needUpdatePartition),
					pt_(pt),
					isToMaster_(false) {

		if (isAddNewNode) {
			needUpdatePartition_ = true;
		}
	}

	bool check() {
		return true;
	}

	void setToMaster() {
		isToMaster_ = true;
	}

	bool isToMaster() {
		return isToMaster_;
	}

	void setNeedUpdatePartition(
			bool needUpdatePartition) {

		needUpdatePartition_ = needUpdatePartition;
	}

	bool &isNeedUpdatePartition() {
		return needUpdatePartition_;
	}

	SubPartitionTable &getSubPartitionTable() {
		return subPartitionTable_;
	}

	void setMaxLsnList() {
		pt_->setMaxLsnList(maxLsnList_);
	}
	LsnList &getMaxLsnList() {
		return maxLsnList_;
	}

	DropPartitionNodeInfo &getDropPartitionNodeInfo() {
		return dropPartitionNodeInfo_;
	}

	
	bool isAddNewNode() {
		return isAddNewNode_;
	}
	void setAddOrDownNode() {
		isAddNewNode_ = true;
	}

	util::Vector<PartitionId> &getShorttermSyncPIdList() {
		return shorttermSyncPosList_;
	}

	util::Vector<PartitionId> &getLongtermSyncPIdList() {
		return longtermSyncPosList_;
	}

	util::Vector<PartitionId> &getChangePartitonPIdList() {
		return changePartitionPosList_;
	}

	MSGPACK_DEFINE(
			subPartitionTable_, nodeList_, maxLsnList_);

	DropPartitionNodeInfo dropPartitionNodeInfo_;

private:

	SubPartitionTable subPartitionTable_;
	AddressInfoList nodeList_;
	LsnList maxLsnList_;
	util::Vector<PartitionId> shorttermSyncPosList_;
	util::Vector<PartitionId> changePartitionPosList_;
	util::Vector<PartitionId> longtermSyncPosList_;

	bool isAddNewNode_;
	bool needUpdatePartition_;
	PartitionTable *pt_;
	bool isToMaster_;
};

/*!
	@brief Represents the information of ChangePartitionStatus event
*/
class ChangePartitionStatusInfo : public ClusterManagerInfo {

public:

	ChangePartitionStatusInfo(
			util::StackAllocator &alloc,
			NodeId nodeId,
			PartitionId pId,
			PartitionStatus status,
			bool isToSubMaster,
			ChangePartitionType changePartitionType) :
					ClusterManagerInfo(alloc, nodeId),
					pId_(pId),
					status_(static_cast<uint8_t>(status)),
					isToSubMaster_(isToSubMaster),
					changePartitionType_(
							static_cast<uint8_t>(changePartitionType)) {}

	ChangePartitionStatusInfo(
			util::StackAllocator &alloc) :
					ClusterManagerInfo(alloc, 0),
					pId_(UNDEF_PARTITIONID) {}

	bool check() {
		return true;
	}

	void getPartitionStatus(
			PartitionId &pId, PartitionStatus &status) {

		pId = pId_;
		status = static_cast<PartitionStatus>(status_);
	}

	bool isToSubMaster() {
		return isToSubMaster_;
	}

	ChangePartitionType getPartitionChangeType() {
		return static_cast<ChangePartitionType>(
				changePartitionType_);
	}

	MSGPACK_DEFINE(
			pId_, status_, isToSubMaster_, changePartitionType_);

private:

	PartitionId pId_;
	uint8_t status_;
	bool isToSubMaster_;
	uint8_t changePartitionType_;
};

/*!
	@brief Represents the information of ChangePartitionTable event
*/
class ChangePartitionTableInfo : public ClusterManagerInfo {

public:

	ChangePartitionTableInfo(
			util::StackAllocator &alloc,
			NodeId nodeId, PartitionRole &nextRole) :
					ClusterManagerInfo(alloc, nodeId),
					role_(nextRole) {}

	ChangePartitionTableInfo(
			util::StackAllocator &alloc,
			NodeId nodeId) :
					ClusterManagerInfo(alloc, nodeId) {}

	bool check() {
		return true;
	}

	PartitionRole &getPartitionRole() {
		return role_;
	}

	MSGPACK_DEFINE(role_);

private:
	PartitionRole role_;
};

/*!
	@brief Represents the information of Heartbeat event
*/
class HeartbeatInfo : public ClusterManagerInfo {

public:

	HeartbeatInfo(
			util::StackAllocator &alloc,
			NodeId nodeId,
			PartitionTable *pt,
			bool isAddNewNode = false) :
					ClusterManagerInfo(alloc, nodeId),
					isAddNewNode_(isAddNewNode),
					isStatusChange_(false),
					pt_(pt) {}

	bool check() {
		return true;
	}

	void validate(uint64_t partitionNum);

	bool isParentMaster() {
		return isMaster_;
	}

	void setAddOrNewNode() {
		isAddNewNode_ = true;
	}

	LsnList &getMaxLsnList() {
		return maxLsnList_;
	}

	AddressInfoList &getNodeAddressList() {
		return nodeList_;
	}

	AddressInfoList &getPublicNodeAddressList() {
		return publicNodeList_;
	}

	NodeAddress &getSecondMaster() {
		return secondMasterNode_;
	}

	int32_t getReserveNum() {
		return reserveNum_;
	}

	void setStatusChange() {
		isStatusChange_ = true;
	}

	bool isStatusChange() {
		return isStatusChange_;
	}

	bool isAddNewNode() {
		return isAddNewNode_;
	}

	void addNodeAddress(AddressInfo &addressInfo) {
		nodeList_.push_back(addressInfo);
	}

	void set(ClusterManager::ClusterInfo &clusterInfo);

	void setMaxLsnList() {
		pt_->setMaxLsnList(maxLsnList_);
	}

	void setNodeList(AddressInfo &AddressInfo) {
		nodeList_.push_back(AddressInfo);
	}

	PartitionRevisionNo getPartitionRevisionNo() {
		return partitionSequentialNumber_;
	}


	void setPartition0NodeAddr(NodeAddress &nodeAddr) {
		partition0NodeAddr_ = nodeAddr;
	}

	NodeAddress getPartition0NodeAddr() {
		return partition0NodeAddr_;
	}

	MSGPACK_DEFINE(isMaster_, reserveNum_, secondMasterNode_,
		partitionSequentialNumber_, maxLsnList_, nodeList_,
		isInitialCluster_, partition0NodeAddr_);

private:

	bool isMaster_;
	int32_t reserveNum_;
	NodeAddress secondMasterNode_;
	PartitionRevisionNo partitionSequentialNumber_;
	LsnList maxLsnList_;
	AddressInfoList nodeList_;
	bool isInitialCluster_;
	NodeAddress partition0NodeAddr_;  
	bool isAddNewNode_;
	bool isStatusChange_;
	PartitionTable *pt_;
	AddressInfoList publicNodeList_;
};

/*!
	@brief Represents response information of Heartbeat event
*/
class HeartbeatResInfo : public ClusterManagerInfo {
public:
	/*!
		@brief Represents heartbeat response information per partition
	*/
	struct HeartbeatResValue {

		HeartbeatResValue() :
				pId_(UNDEF_PARTITIONID) {}

		HeartbeatResValue(
				PartitionId pId,
				PartitionStatus status,
				LogSequentialNumber lsn, 
				LogSequentialNumber startLsn,
				PartitionRoleStatus roleStatus,
				PartitionRevisionNo partitionRevision,
				int64_t chunkCount,
				int32_t otherStatus,
				int32_t errorStatus) : 
						pId_(pId),
						status_(static_cast<uint8_t>(status)),
						lsn_(lsn),
						startLsn_(startLsn),
						roleStatus_(static_cast<uint8_t>(roleStatus)),
						partitionRevision_(partitionRevision),
						chunkCount_(chunkCount),
						otherStatus_(otherStatus),
						errorStatus_(errorStatus) {}

		bool validate(uint32_t partitionNum);

		MSGPACK_DEFINE(
				pId_, status_, lsn_, startLsn_, roleStatus_,
				partitionRevision_, chunkCount_,
				otherStatus_, errorStatus_);

		PartitionId pId_;
		uint8_t status_;
		LogSequentialNumber lsn_;
		LogSequentialNumber startLsn_;
		uint8_t roleStatus_;
		PartitionRevisionNo partitionRevision_;
		int64_t chunkCount_;
		int32_t otherStatus_;
		int32_t errorStatus_;
	};

	HeartbeatResInfo(
		util::StackAllocator &alloc,
		NodeId nodeId,
		PartitionRevisionNo partitionSequentialNumber) :
				ClusterManagerInfo(alloc, nodeId),
				syncChunkNum_(-1),
				syncApplyNum_(-1),
				partitionSequentialNumber_(partitionSequentialNumber) {}

	bool check() {
		return true;
	}

	void add(
			PartitionId pId,
			PartitionTable::PartitionStatus status,
			LogSequentialNumber lsn,
			LogSequentialNumber startLsn,
			PartitionRoleStatus roleStatus,
			PartitionRevisionNo partitionRevision,
			int64_t chunkCount,
			int32_t otherStatus,
			int32_t errorStatus);

	void add(
			PartitionId pId,
			LogSequentialNumber lsn,
			LogSequentialNumber startLsn);

	void set(ClusterManager::SyncStat &syncStat) {
		syncChunkNum_ = syncStat.syncChunkNum_;
		syncApplyNum_ = syncStat.syncApplyNum_;
	}

	std::vector<HeartbeatResValue> &getHeartbeatResValueList() {
		return heartbeatResValues_;
	}

	void setSyncInfo(ClusterManager::SyncStat &stat);

	PartitionRevisionNo getPartitionRevisionNo() {
		return partitionSequentialNumber_;
	}

	MSGPACK_DEFINE(heartbeatResValues_,
			syncChunkNum_, syncApplyNum_,
			partitionSequentialNumber_);

private:

	std::vector<HeartbeatResValue> heartbeatResValues_;
	int64_t syncChunkNum_;
	int64_t syncApplyNum_;
	PartitionRevisionNo partitionSequentialNumber_;
};

/*!
	@brief Represents the information of NotifyCluster event
*/
class NotifyClusterInfo : public ClusterManagerInfo {

public:
	
	NotifyClusterInfo(
		util::StackAllocator &alloc,
		NodeId nodeId, PartitionTable *pt) :
				ClusterManagerInfo(alloc, nodeId),
					pt_(pt) {}

	bool check() {
		return true;
	}

	bool isMaster() {
		return isMaster_;
	}

	bool isStable() {
		return isStable_;
	}

	int32_t getReserveNum() {
		return reserveNum_;
	}

	std::string &getClusterName() {
		return clusterName_;
	}

	std::string &getDigest() {
		return digest_;
	}

	int64_t getStartupTime() {
		return startupTime_;
	}

	bool isFollow() {
		return isFollow_;
	}

	void setFollow(bool isFollow) {
		isFollow_ = isFollow;
	}

	void set(
			ClusterManager::ClusterInfo &clusterInfo,
			PartitionTable *pt);

	AddressInfoList &getNodeAddressInfoList() {
		return nodeList_;
	}

	AddressInfoList &getPublicNodeAddressInfoList() {
		return publicNodeList_;
	}
	void setPublicNodeAddressInfoList(
			AddressInfoList &publicNodeList) {
			publicNodeList_ = publicNodeList;
	}

	MSGPACK_DEFINE(isMaster_, isStable_,
		reserveNum_, startupTime_,
		nodeList_, clusterName_, digest_);

private:

	bool isMaster_;
	bool isStable_;
	int32_t reserveNum_;
	int64_t startupTime_;
	AddressInfoList nodeList_;
	std::string clusterName_;
	std::string digest_;
	AddressInfoList publicNodeList_;

	bool isFollow_;
	PartitionTable *pt_;
};

typedef HeartbeatResInfo::HeartbeatResValue HeartbeatResValue;
typedef ClusterManager::SyncStat SyncStat;

#endif
