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
#include "config_table.h"
#include "data_type.h"
#include "gs_error.h"
#include "partition_table.h"

#include "event_engine.h"

#define TEST_CLUSTER_MANAGER

#define TEST_TRANSACTION_HANDLER


#ifdef GD_ENABLE_UNICAST_NOTIFICATION

typedef std::set<AddressInfo> NodeAddressSet;
typedef NodeAddressSet::iterator NodeAddressSetItr;

/*!
	@brief cluster notification mode
*/
enum ClusterNotificationMode {
	NOTIFICATION_MULTICAST,
	NOTIFICATION_FIXEDLIST,
	NOTIFICATION_RESOLVER
};

#endif

class ClusterService;

/*!
	@brief cluster status after transition
*/
enum ClusterStatusTransition { TO_MASTER, TO_SUBMASTER, KEEP };

static const std::string getClusterStatusTransition(
	ClusterStatusTransition type) {
	switch (type) {
	case TO_MASTER:
		return "TO_MASTER";
	case TO_SUBMASTER:
		return "TO_SUBMASTER";
	case KEEP:
		return "KEEP";
	default:
		return "";
	}
}

/*!
	@brief Gossip type
*/
enum GossipType {
	GOSSIP_NORMAL = 0,
	GOSSIP_PENDING = 1,
	GOSSIP_LEAVE = 2,
	GOSSIP_GOAL = 3,
	GOSSIP_GOAL_SELF = 4,
};

/*!
	@brief Operation type related with cluster management
*/
enum ClusterOperationType {
	OP_HEARTBEAT,
	OP_HEARTBEAT_RES,
	OP_NOTIFY_CLUSTER,
	OP_NOTIFY_CLUSTER_RES,
	OP_JOIN_CLUSTER,
	OP_LEAVE_CLUSTER,
	OP_SHUTDOWN_NODE_NORMAL,
	OP_SHUTDOWN_NODE_FORCE,
	OP_SHUTDOWN_CLUSTER,
	OP_COMPLETE_CHECKPOINT_FOR_SHUTDOWN,
	OP_COMPLETE_CHECKPOINT_FOR_RECOVERY,
	OP_TIMER_CHECK_CLUSTER,
	OP_TIMER_NOTIFY_CLUSTER,
	OP_TIMER_NOTIFY_CLIENT,
	OP_TIMER_CHECK_LOAD_BALANCE,
	OP_UPDATE_PARTITION,
	OP_GOSSIP,
	OP_CHANGE_PARTITION_STATUS,
	OP_CHANGE_PARTITION_TABLE,
	OP_ORDER_DROP_PARTITION,
};

/*!
	@brief ClusterManager
*/
class ClusterManager {
	class ClusterConfig;
	class ExtraConfig;
	struct ClusterInfo;

	friend class SystemService;
	friend class ClusterManagerTest;
	friend class TransactionHandlerTest;

	friend class ClusterService;


public:
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



	ClusterManager(const ConfigTable &configTable,
		PartitionTable *partitionTable, ClusterVersionId versionId = 0);

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
	void checkCommandStatus(ClusterOperationType operation);

	std::string getClusterName() {
		util::LockGuard<util::Mutex> lock(clusterLock_);
		return clusterInfo_.clusterName_;
	}


	void initialize(ClusterService *clsSvc);

	EventMonotonicTime getMonotonicTime() {
		return ee_->getMonotonicTime() + clusterInfo_.startupTime_ +
			   EVENT_MONOTONIC_ADJUST_TIME;
	}

	void getSafetyLeaveNodeList(util::StackAllocator &alloc,
		std::vector<NodeId> &candList, int32_t removeNodeNum = 1);

	void checkClusterStatus(ClusterOperationType operation);

	void updateNodeStatus(ClusterOperationType eventType);

	bool isShutdownPending() {
		return statusInfo_.isShutdownPending_;
	}

	bool setSystemError(bool reset = true) {
		return statusInfo_.setSystemError(reset);
	}

	void updateClusterStatus(
		ClusterStatusTransition status, bool isLeave = false);

	void updateActiveNodes(int64_t checkTime = UNDEF_TTL) {
		clusterInfo_.activeNodeNum_ = pt_->getLiveNum(checkTime);
	}

	bool checkRecoveryCompleted() {
		return (statusInfo_.currentStatus_ != SYS_STATUS_BEFORE_RECOVERY);
	}

	bool isInitialCluster() {
		return clusterInfo_.isInitialCluster_;
	}

	bool isPendingSync() {
		bool retVal = clusterInfo_.isPendingSync_;
		clusterInfo_.isPendingSync_ = false;
		return retVal;
	}

	void setPendingSync() {
		clusterInfo_.isPendingSync_ = true;
	}

	bool checkLoadBalance() {
		return clusterInfo_.isLoadBalance_;
	}

	void setLoadBalance(bool flag) {
		clusterInfo_.isLoadBalance_ = flag;
	}

	bool isForceSync() {
		bool retVal = clusterInfo_.isForceSync_;
		clusterInfo_.isForceSync_ = false;
		return retVal;
	}

	void setForceSync() {
		clusterInfo_.isForceSync_ = true;
	}

	bool isRepairPartition() {
		bool retVal = clusterInfo_.isRepairPartition_;
		clusterInfo_.isRepairPartition_ = false;
		return retVal;
	}

	void setRepairPartition() {
		clusterInfo_.isRepairPartition_ = true;
	}

	bool isHeartbeatRes() {
		return clusterInfo_.isHeartbeatRes_;
	}

	void setHeartbeatRes(bool isHeartbeatRes = true) {
		clusterInfo_.isHeartbeatRes_ = isHeartbeatRes;
	}

	void setSignalBeforeRecovery() {
		util::LockGuard<util::Mutex> lock(clusterLock_);
		if (statusInfo_.currentStatus_ == SYS_STATUS_BEFORE_RECOVERY) {
			isSignalBeforeRecovery_ = true;
		}
	}

	util::Mutex &getClusterLock() {
		return clusterLock_;
	}

	bool isSignalBeforeRecovery() {
		return isSignalBeforeRecovery_;
	}

#ifdef GD_ENABLE_UNICAST_NOTIFICATION
	void setNotificationMode(ClusterNotificationMode mode) {
		clusterInfo_.mode_ = mode;
	}

	const char *getNotificationMode() {
		switch (clusterInfo_.mode_) {
		case NOTIFICATION_MULTICAST:
			return "MULTICAST";
		case NOTIFICATION_FIXEDLIST:
			return "FIXED_LIST";
		case NOTIFICATION_RESOLVER:
			return "PROVIDER";
		default:
			return "UNDEF_MODE";
		}
	}

	ClusterService *getService();

#endif

	/*!
		@brief Represents Synchronization Statistics
	*/
	struct SyncStat {
		SyncStat() : syncChunkNum_(0), syncApplyNum_(0) {}
		void init(bool isOwner) {
			if (isOwner) {
				syncChunkNum_ = 0;
				syncApplyNum_ = -1;
			}
			else {
				syncChunkNum_ = -1;
				syncApplyNum_ = 0;
			}
		}
		int32_t syncChunkNum_;
		int32_t syncApplyNum_;
	};

	SyncStat &getSyncStat() {
		return syncStat_;
	}
	/*!
		@brief Handlers error of ClusterManager
	*/
	class ErrorManager {
	public:
#ifdef GD_ENABLE_UNICAST_NOTIFICATION
		static const int32_t MAX_ERROR_COUNT = 6;
#else
		static const int32_t MAX_ERROR_COUNT = 5;
#endif
		static const int32_t DEFAULT_DUMP_COUNT = 1000;

		ErrorManager() {
			errorCountList_.assign(MAX_ERROR_COUNT, 0);
			dumpCountList_.assign(MAX_ERROR_COUNT, 0);
			dumpCountList_[0] = 100;
			dumpCountList_[1] = 10000;
			dumpCountList_[2] = 10000;
			dumpCountList_[3] = 10000;
#ifdef GD_ENABLE_UNICAST_NOTIFICATION
			dumpCountList_[4] = 10;
			dumpCountList_[5] = DEFAULT_DUMP_COUNT;
#else
			dumpCountList_[4] = DEFAULT_DUMP_COUNT;
#endif
		}

		void reset() {
			for (int32_t pos = 0; pos < MAX_ERROR_COUNT; pos++) {
				errorCountList_[pos] = 0;
			}
		}

		void setDumpCount(ErrorCode errorCode, uint32_t count) {
			if (count == 0) count = 1;  
			uint32_t listNo = getListNo(errorCode);
			dumpCountList_[listNo] = count;
		}

		bool reportError(ErrorCode errorCode) {
			uint32_t listNo = getListNo(errorCode);
			bool retFlag;
			if (errorCountList_[listNo] % dumpCountList_[listNo] == 0) {
				retFlag = true;
			}
			else {
				retFlag = false;
			}
			errorCountList_[listNo]++;
			return retFlag;
		}

		uint64_t getErrorCount(ErrorCode errorCode) {
			uint32_t listNo = getListNo(errorCode);
			return errorCountList_[listNo];
		}

	private:
		uint32_t getListNo(ErrorCode errorCode) {
			switch (errorCode) {
			case GS_ERROR_CS_ENCODE_DECODE_VERSION_CHECK:
				return 0;
			case GS_ERROR_RM_ALREADY_APPLY_LOG:
				return 1;
			case GS_ERROR_TXN_REPLICATION_LOG_LSN_INVALID:
				return 2;
			case GS_ERROR_TXN_CLUSTER_ROLE_UNMATCH:
			case GS_ERROR_TXN_PARTITION_ROLE_UNMATCH:
			case GS_ERROR_TXN_PARTITION_STATE_UNMATCH:
				return 3;

#ifdef GD_ENABLE_UNICAST_NOTIFICATION
			case GS_ERROR_CS_PROVIDER_TIMEOUT:
				return 4;
			default:
				return MAX_ERROR_COUNT - 1;
#else
			default:
				return 4;
#endif
			}
		}
		std::vector<uint64_t> errorCountList_;
		std::vector<uint64_t> dumpCountList_;
	};

	bool reportError(int32_t errorCode) {
		return errorMgr_.reportError(static_cast<ErrorCode>(errorCode));
	}

	uint64_t getErrorCount(int32_t errorCode) {
		return errorMgr_.getErrorCount(static_cast<ErrorCode>(errorCode));
	}

	void setCheckpointDelayLimitTime(PartitionId pId, PartitionGroupId pgId);

	void checkCheckpointDelayLimitTime(int64_t checkTime = INT64_MAX);

	bool isSyncRunning(PartitionGroupId pgId) {
		return (delayCheckpointLimitTime_[pgId] > 0 &&
				getMonotonicTime() < delayCheckpointLimitTime_[pgId]);
	}


	void setChunkSync(PartitionId pId, CheckpointId cpId) {
		currentChunkSyncPId_ = pId;
		currentChunkSyncCPId_ = cpId;
	}

	void resetChunkSync() {
		currentChunkSyncPId_ = UNDEF_PARTITIONID;
		currentChunkSyncCPId_ = UNDEF_CHECKPOINT_ID;
	}

	void getCurrentChunkSync(PartitionId &pId, CheckpointId &cpId) {
		pId = currentChunkSyncPId_;
		cpId = currentChunkSyncCPId_;
	}

	std::string getPendingLimitTime(PartitionGroupId pgId) {
		if (delayCheckpointLimitTime_[pgId] > 0) {
			return getTimeStr(delayCheckpointLimitTime_[pgId]);
		}
		else {
			return std::string("");
		}
	}

	bool getCurrentPendingCheckpointInfo(uint32_t &pgId, std::string &retTime) {
		pgId = UINT32_MAX;
		retTime = "";
		for (PartitionGroupId pos = 0;
			 pos < static_cast<uint32_t>(delayCheckpointLimitTime_.size());
			 pos++) {
			if (delayCheckpointLimitTime_[pos] > 0) {
				pgId = pos;
				retTime = getTimeStr(delayCheckpointLimitTime_[pos]);
				return true;
			}
		}
		return false;
	}

	void checkCheckPointPending(PartitionId pId, CheckpointId cpId,
		PartitionGroupId currentPgId, PartitionGroupId currentCpPgId);

	std::string dump();

	int32_t getConcurrency() {
		return concurrency_;
	}



	/*!
		@brief Base Class of ClusterManager
	*/
	class ClusterManagerInfo {
	public:

		ClusterManagerInfo(util::StackAllocator &alloc, NodeId nodeId)
			: alloc_(&alloc),
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

	/*!
		@brief Represents the information of Heartbeat event
	*/
	class HeartbeatInfo : public ClusterManagerInfo {
		TEST_CLUSTER_MANAGER

	public:

		HeartbeatInfo(util::StackAllocator &alloc, NodeId nodeId,
			PartitionTable *pt, bool isAddNewNode = false)
			: ClusterManagerInfo(alloc, nodeId),
			  isInitialCluster_(false),
			  activeNodeList_(alloc),
			  isAddNewNode_(isAddNewNode),
			  isStatusChange_(false),
			  pt_(pt) {}

		bool check() {
			return true;
		}

		void validate(uint64_t partitionNum) {
			if (maxLsnList_.size() < partitionNum) {
				GS_THROW_USER_ERROR(GS_ERROR_CLM_INVALID_PARTITION_NUM,
					"Invalid partition num, maxLsn="
						<< maxLsnList_.size() << ", self=" << partitionNum);
			}
		}

		std::string dump() {
			util::NormalOStringStream ss;
			ss << "HeartbeatInfo:{"
			   << "isMaster:" << isMaster_ << ", reserveNum:" << reserveNum_
			   << ", secondMasterNode:" << secondMasterNode_
			   << ", partitionSequentialNumber:" << partitionSequentialNumber_
			   << ", maxLsnListSize:" << maxLsnList_.size()
			   << ", nodeList:" << pt_->dumpNodeAddressInfoList(nodeList_)
			   << ", activeNodeListSize:" << activeNodeList_.size()
			   << ", isAddNewNode:" << isAddNewNode_
			   << ", isStatusChange:" << isStatusChange_
			   << ", isIntialCluster:" << isInitialCluster_ << "}";
			return ss.str();
		}

		void setInitialCluster() {
			isInitialCluster_ = true;
		}

		bool isIntialCluster() {
			return isInitialCluster_;
		}

		bool isParentMaster() {
			return isMaster_;
		}

		std::vector<LogSequentialNumber> &getMaxLsnList() {
			return maxLsnList_;
		}

		util::XArray<NodeId> &getActiveNodeList() {
			return activeNodeList_;
		}

		std::vector<AddressInfo> &getNodeAddressList() {
			return nodeList_;
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

		void set(ClusterInfo &clusterInfo) {
			isMaster_ = clusterInfo.pt_->isMaster();
			secondMasterNode_ = clusterInfo.secondMasterNodeAddress_;
			reserveNum_ = clusterInfo.reserveNum_;
			partitionSequentialNumber_ =
				clusterInfo.pt_->getPartitionRevision().sequentialNumber_;
		}

		void setMaxLsnList(std::vector<LogSequentialNumber> &maxLsnList) {
			maxLsnList_ = maxLsnList;
		}

		void setNodeList(AddressInfo &AddressInfo) {
			nodeList_.push_back(AddressInfo);
		}

		int32_t getPartitionRevisionSequentialNumber() {
			return partitionSequentialNumber_;
		}

		MSGPACK_DEFINE(isMaster_, reserveNum_, secondMasterNode_,
			partitionSequentialNumber_, maxLsnList_, nodeList_,
			isInitialCluster_);

	private:

		bool isMaster_;
		int32_t reserveNum_;
		NodeAddress secondMasterNode_;
		int32_t partitionSequentialNumber_;
		std::vector<LogSequentialNumber> maxLsnList_;
		std::vector<AddressInfo> nodeList_;
		bool isInitialCluster_;

		util::XArray<NodeId> activeNodeList_;
		bool isAddNewNode_;
		bool isStatusChange_;
		PartitionTable *pt_;
	};

	/*!
		@brief Represents response information of Heartbeat event
	*/
	class HeartbeatResInfo : public ClusterManagerInfo {
		TEST_CLUSTER_MANAGER

	public:

		/*!
			@brief Represents heartbeat response information per partition
		*/
		struct HeartbeatResValue {

			HeartbeatResValue() : pId_(UNDEF_PARTITIONID) {}

			HeartbeatResValue(PartitionId pId, PartitionStatus status,
				LogSequentialNumber lsn, PartitionRoleStatus roleStatus)
				: pId_(pId),
				  status_(static_cast<uint8_t>(status)),
				  lsn_(lsn),
				  roleStatus_(static_cast<uint8_t>(roleStatus)) {}

			bool validate(uint32_t partitionNum) {
				if (pId_ >= partitionNum ||
					(status_ >= PartitionTable::PT_STATUS_MAX) ||
					(roleStatus_ >= PartitionTable::PT_ROLE_MAX)) {
					return false;
				}
				else {
					return true;
				}
			}

			std::string dump() {
				util::NormalOStringStream ss;
				ss << "HeartbeatResValue:{"
				   << "pId:" << pId_ << ", status:"
				   << dumpPartitionStatus(static_cast<PartitionStatus>(status_))
				   << ", lsn:" << lsn_ << ", roleStatus:"
				   << dumpPartitionRoleStatus(
						  static_cast<PartitionRoleStatus>(roleStatus_))
				   << "}";
				return ss.str();
			};

			MSGPACK_DEFINE(pId_, status_, lsn_, roleStatus_);


			PartitionId pId_;
			uint8_t status_;
			LogSequentialNumber lsn_;
			uint8_t roleStatus_;
		};

		/*!
			@brief Represents start LSN in heartbeat response information per
		   partition
		*/
		struct HeartbeatResStartValue {

			HeartbeatResStartValue() : pId_(UNDEF_PARTITIONID) {}

			HeartbeatResStartValue(PartitionId pId, LogSequentialNumber lsn)
				: pId_(pId), lsn_(lsn) {}

			bool validate(uint32_t partitionNum) {
				if (pId_ >= partitionNum) {
					return false;
				}
				else {
					return true;
				}
			}

			std::string dump() {
				util::NormalOStringStream ss;
				ss << "HeartbeatResStartValue:{"
				   << "pId:" << pId_ << ", lsn:" << lsn_ << "}";
				return ss.str();
			};

			MSGPACK_DEFINE(pId_, lsn_);


			PartitionId pId_;
			LogSequentialNumber lsn_;
		};


		HeartbeatResInfo(
			util::StackAllocator &alloc, NodeId nodeId, uint8_t type = 0)
			: ClusterManagerInfo(alloc, nodeId),
			  syncChunkNum_(-1),
			  syncApplyNum_(-1),
			  type_(type) {}

		bool check() {
			return true;
		}

		void add(PartitionId pId, PartitionTable::PartitionStatus status,
			LogSequentialNumber lsn, PartitionRoleStatus roleStatus);

		void add(PartitionId pId, LogSequentialNumber lsn);

		void set(SyncStat &syncStat) {
			syncChunkNum_ = syncStat.syncChunkNum_;
			syncApplyNum_ = syncStat.syncApplyNum_;
		}

		std::vector<HeartbeatResValue> &getHeartbeatResValueList() {
			return heartbeatResValues_;
		}

		std::vector<HeartbeatResStartValue> &getHeartbeatResStartValueList() {
			return heartbeatResStartValues_;
		}

		void setSyncInfo(SyncStat &stat) {
			if (syncChunkNum_ != -1) {
				stat.syncChunkNum_ = syncChunkNum_;
			}
			if (syncApplyNum_ != -1) {
				stat.syncApplyNum_ = syncApplyNum_;
			}
		}

		uint8_t getType() {
			return type_;
		}

		MSGPACK_DEFINE(heartbeatResValues_, heartbeatResStartValues_,
			syncChunkNum_, syncApplyNum_, type_);

		std::string dump() {
			util::NormalOStringStream ss;
			ss << "HeartbeatResInfo:{" << dumpList(heartbeatResValues_)
			   << dumpList(heartbeatResStartValues_) << "}";
			return ss.str();
		}

	private:

		std::vector<HeartbeatResValue> heartbeatResValues_;
		std::vector<HeartbeatResStartValue> heartbeatResStartValues_;
		int32_t syncChunkNum_;
		int32_t syncApplyNum_;
		uint8_t type_;
	};

	/*!
		@brief Represents the information of NotifyCluster event
	*/
	class NotifyClusterInfo : public ClusterManagerInfo {
		TEST_CLUSTER_MANAGER

	public:

		NotifyClusterInfo(
			util::StackAllocator &alloc, NodeId nodeId, PartitionTable *pt)
			: ClusterManagerInfo(alloc, nodeId), pt_(pt) {}

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

		void set(ClusterInfo &clusterInfo, PartitionTable *pt);

		std::vector<AddressInfo> &getNodeAddressInfoList() {
			return nodeList_;
		}

		MSGPACK_DEFINE(isMaster_, isStable_, reserveNum_, startupTime_,
			nodeList_, clusterName_, digest_);

		std::string dump() {
			util::NormalOStringStream ss;
			ss << "NotifyClusterInfo:"
			   << "{"
			   << "isMaster:" << isMaster_ << ", isStable:" << isStable_
			   << ", reserveNum:" << reserveNum_
			   << ", startupTime:" << getTimeStr(startupTime_)
			   << ", nodeListSize:" << pt_->dumpNodeAddressInfoList(nodeList_)
			   << ", clusterName:" << clusterName_ << ", digest:" << digest_
			   << ", isFollow:" << isFollow_ << "}";
			return ss.str();
		}

	private:

		bool isMaster_;
		bool isStable_;
		int32_t reserveNum_;
		int64_t startupTime_;
		std::vector<AddressInfo> nodeList_;
		std::string clusterName_;
		std::string digest_;

		bool isFollow_;
		PartitionTable *pt_;
	};

	/*!
		@brief Represents response information of NotifyCluster event
	*/
	class NotifyClusterResInfo : public ClusterManagerInfo {
		TEST_CLUSTER_MANAGER

	public:

		NotifyClusterResInfo(
			util::StackAllocator &alloc, NodeId nodeId, PartitionTable *pt)
			: ClusterManagerInfo(alloc, nodeId),
			  reserveNum_(0),
			  partitionSequentialNumber_(0),
			  pt_(pt) {}

		bool check() {
			return true;
		}

		void validate(uint64_t partitionNum) {
			if (lsnList_.size() != partitionNum ||
				maxLsnList_.size() != partitionNum) {
				GS_THROW_USER_ERROR(GS_ERROR_CLM_INVALID_PARTITION_NUM,
					"Invalid partition num, recvLsn="
						<< lsnList_.size() << ", maxLsn=" << maxLsnList_.size()
						<< ", self=" << partitionNum);
			}
		}

		void set(ClusterInfo &clusterInfo, PartitionTable *pt);

		int32_t getReserveNum() {
			return reserveNum_;
		}

		int64_t getStartupTime() {
			return startupTime_;
		}

		std::vector<LogSequentialNumber> &getLsnList() {
			return lsnList_;
		}

		std::vector<LogSequentialNumber> &getMaxLsnList() {
			return maxLsnList_;
		}

		std::vector<AddressInfo> &getNodeAddressList() {
			return nodeList_;
		}

		int32_t getPartitionSequentialNumber() {
			return partitionSequentialNumber_;
		}

		std::vector<AddressInfo> &getNodeAddressInfoList() {
			return nodeList_;
		}

		MSGPACK_DEFINE(reserveNum_, startupTime_, partitionSequentialNumber_,
			lsnList_, maxLsnList_, nodeList_);

		std::string dump() {
			util::NormalOStringStream ss;
			ss << "NotifyClusterResInfo:{"
			   << "reserveNum:" << reserveNum_
			   << ", startupTime:" << getTimeStr(startupTime_)
			   << ", partitionSequentialNumber:" << partitionSequentialNumber_
			   << ", lsnListSize:" << lsnList_.size()
			   << ", maxLsnListSize:" << maxLsnList_.size()
			   << ", nodeList:" << pt_->dumpNodeAddressInfoList(nodeList_)
			   << "}";
			return ss.str();
		}

	private:

		int32_t reserveNum_;
		int64_t startupTime_;
		int32_t partitionSequentialNumber_;
		std::vector<LogSequentialNumber> lsnList_;
		std::vector<LogSequentialNumber> maxLsnList_;
		std::vector<AddressInfo> nodeList_;

		PartitionTable *pt_;
	};

	/*!
		@brief Represents the information of JoinCluster command
	*/
	class JoinClusterInfo : public ClusterManagerInfo {
	public:

		JoinClusterInfo(
			util::StackAllocator &alloc, NodeId nodeId, bool precheck = false)
			: ClusterManagerInfo(alloc, nodeId),
			  minNodeNum_(0),
			  isPreCheck_(precheck) {}

		JoinClusterInfo(bool isPreCheck)
			: minNodeNum_(0), isPreCheck_(isPreCheck) {}

		bool check() {
			return true;
		}

		void set(const std::string &clusterName, int32_t minNodeNum) {
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

		std::string dump() {
			util::NormalOStringStream ss;
			ss << "JoinClusterInfo:{"
			   << "clusterName:" << clusterName_
			   << ", minNodeNum:" << minNodeNum_
			   << ", isPreCheck:" << isPreCheck_ << "}";
			return ss.str();
		}

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

		LeaveClusterInfo(util::StackAllocator &alloc, NodeId nodeId = 0,
			bool preCheck = false, bool isForce = true)
			: ClusterManagerInfo(alloc, nodeId),
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

		std::string dump() {
			util::NormalOStringStream ss;
			ss << "LeaveClusterInfo:{"
			   << ", isPreCheck:" << isPreCheck_ << ", isForce:" << isForce_
			   << "}";
			return ss.str();
		}

	private:

		bool isPreCheck_;
		bool isForce_;
	};


	/*!
		@brief Represents the information of ShutdownNode command
	*/
	class ShutdownNodeInfo : public ClusterManagerInfo {
	public:

		ShutdownNodeInfo(util::StackAllocator &alloc, NodeId nodeId, bool force)
			: ClusterManagerInfo(alloc, nodeId) {
			isForce_ = force;
		}

		bool check() {
			return true;
		}

		MSGPACK_DEFINE(isForce_);

		std::string dump() {
			util::NormalOStringStream ss;
			ss << "ShutdownNodeInfo:{"
			   << ", isForce:" << isForce_ << "}";
			return ss.str();
		}

	private:

		bool isForce_;
	};

	/*!
		@brief Represents the information of Shutdown command
	*/
	class ShutdownClusterInfo : public ClusterManagerInfo {
	public:

		ShutdownClusterInfo(util::StackAllocator &alloc, NodeId nodeId)
			: ClusterManagerInfo(alloc, nodeId) {}

		bool check() {
			return true;
		}

		std::string dump() {
			util::NormalOStringStream ss;
			ss << "ShutdownClusterInfo:{"
			   << "}";
			return ss.str();
		}
	};

	/*!
		@brief Represents completed Checkpoint information of ShutdownNode
	   command
	*/
	class CompleteCheckpointInfo : public ClusterManagerInfo {
	public:

		CompleteCheckpointInfo(util::StackAllocator &alloc, NodeId nodeId)
			: ClusterManagerInfo(alloc, nodeId) {}

		bool check() {
			return true;
		}

		std::string dump() {
			util::NormalOStringStream ss;
			ss << "CompleteCheckpointInfo:{"
			   << "}";
			return ss.str();
		}
	};

	/*!
		@brief Represents the information for check heartbeat
	*/
	class HeartbeatCheckInfo : public ClusterManagerInfo {
		TEST_CLUSTER_MANAGER;

	public:

		HeartbeatCheckInfo(util::StackAllocator &alloc, NodeId nodeId = 0)
			: ClusterManagerInfo(alloc, nodeId),
			  nextTransition_(KEEP),
			  isAddNewNode_(false) {}

		bool check() {
			return true;
		}

		ClusterStatusTransition getNextTransition() {
			return nextTransition_;
		}

		std::vector<NodeId> &getActiveNodeList() {
			return activeNodeList_;
		}

		bool isAddNewNode() {
			return isAddNewNode_;
		}

		void set(ClusterStatusTransition nextTransition, bool isAddNewNode) {
			nextTransition_ = nextTransition;
			isAddNewNode_ = isAddNewNode;
		}

		std::string dump() {
			util::NormalOStringStream ss;
			ss << "HeartbeatCheckInfo:{"
			   << "nextTransition:"
			   << getClusterStatusTransition(nextTransition_)
			   << ", activeNodeListSize:" << activeNodeList_.size()
			   << ", isAddNewNode:" << isAddNewNode_ << "}";
			return ss.str();
		}

	private:

		ClusterStatusTransition nextTransition_;
		std::vector<NodeId> activeNodeList_;
		bool isAddNewNode_;
	};

	/*!
		@brief Represents the information of UpdatePartition event
	*/
	class UpdatePartitionInfo : public ClusterManagerInfo {
	public:

		UpdatePartitionInfo(util::StackAllocator &alloc, NodeId nodeId,
			PartitionTable *pt, bool isAddNewNode = false, bool isSync = false)
			: ClusterManagerInfo(alloc, nodeId),
			  isPending_(false),
			  isAddNewNode_(isAddNewNode),
			  filterNodeSet_(alloc),
			  isSync_(isSync),
			  pt_(pt) {
			if (isAddNewNode) {
				isPending_ = true;
				isSync_ = true;
			}
		}

		bool check() {
			return true;
		}

		bool isPending() {
			return isPending_;
		}

		void setPending(bool pendingFlag) {
			isPending_ = pendingFlag;
		}

		void setSync(bool isSync) {
			isSync_ = isSync;
		}

		bool isSync() {
			return isSync_;
		}

		SubPartitionTable &getSubPartitionTable() {
			return subPartitionTable_;
		}

		util::Set<NodeId> &getFilterNodeSet() {
			return filterNodeSet_;
		}

		void setMaxLsnList(std::vector<LogSequentialNumber> &maxLsnList) {
			maxLsnList_ = maxLsnList;
		}

		std::vector<LogSequentialNumber> &getMaxLsnList() {
			return maxLsnList_;
		}

		MSGPACK_DEFINE(subPartitionTable_, nodeList_, isPending_, maxLsnList_);

		std::string dump() {
			util::NormalOStringStream ss;
			ss << "UpdatePartitionInfo:{" << subPartitionTable_.dump()
			   << ", nodeList:" << pt_->dumpNodeAddressInfoList(nodeList_)
			   << ", isPending:" << isPending_
			   << ", isAddNewNode:" << isAddNewNode_
			   << ", filterNodeSet:" << pt_->dumpNodeAddressSet(filterNodeSet_)
			   << ", isSync:" << isSync_
			   << ", maxLsnListSize:" << maxLsnList_.size() << "}";
			return ss.str();
		}

	private:

		SubPartitionTable subPartitionTable_;
		std::vector<AddressInfo> nodeList_;
		bool isPending_;
		std::vector<LogSequentialNumber> maxLsnList_;

		bool isAddNewNode_;
		util::Set<NodeId> filterNodeSet_;
		bool isSync_;
		PartitionTable *pt_;
	};

	/*!
		@brief Represents the information of Gossip event
	*/
	class GossipInfo : public ClusterManagerInfo {
	public:

		GossipInfo(util::StackAllocator &alloc, NodeId nodeId = 0)
			: ClusterManagerInfo(alloc, nodeId),
			  pId_(UNDEF_PARTITIONID),
			  gossipMessageType_(0),
			  gossipType_(GOSSIP_NORMAL),
			  nodeId_(UNDEF_NODEID),
			  isValid_(false) {}

		bool check() {
			gossipType_ = static_cast<GossipType>(gossipMessageType_);
			if (!targetNode_.isValid()) {
				return false;
			}
			return true;
		}

		void setAddress(NodeAddress &address) {
			targetNode_ = address;
			isValid_ = true;
		}

		NodeAddress &getAddress() {
			return targetNode_;
		}

		void setTarget(PartitionId pId = UNDEF_PARTITIONID, NodeId nodeId = 0,
			GossipType gossipType = GOSSIP_NORMAL) {
			pId_ = pId;
			nodeId_ = nodeId;
			gossipType_ = gossipType;
			gossipMessageType_ = static_cast<int8_t>(gossipType);
		}

		void setNodeId(NodeId nodeId) {
			nodeId_ = nodeId;
		}

		bool isValid() {
			return isValid_;
		}

		void get(PartitionId &pId, NodeId &nodeId, GossipType &gossipType) {
			pId = pId_;
			nodeId = nodeId_;
			gossipType = gossipType_;
		}

		MSGPACK_DEFINE(pId_, targetNode_, gossipMessageType_);

		std::string dump() {
			util::NormalOStringStream ss;
			ss << "GossipInfo:{"
			   << "pId:" << pId_ << ", targetNode:" << targetNode_
			   << ", gossipMessageType:" << gossipMessageType_
			   << ", gossipType:" << gossipType_ << ", nodeId:" << nodeId_
			   << ", isValid:" << isValid_ << "}";
			return ss.str();
		}

	private:

		PartitionId pId_;
		NodeAddress targetNode_;
		int8_t gossipMessageType_;

		GossipType gossipType_;
		NodeId nodeId_;
		bool isValid_;
	};

	/*!
		@brief Represents the information of ChangePartitionStatus event
	*/
	class ChangePartitionStatusInfo : public ClusterManagerInfo {
	public:

		ChangePartitionStatusInfo(util::StackAllocator &alloc, NodeId nodeId,
			PartitionId pId, PartitionStatus status, bool isToSubMaster,
			ChangePartitionType changePartitionType)
			: ClusterManagerInfo(alloc, nodeId),
			  pId_(pId),
			  status_(static_cast<uint8_t>(status)),
			  isToSubMaster_(isToSubMaster),
			  changePartitionType_(changePartitionType) {}

		ChangePartitionStatusInfo(util::StackAllocator &alloc)
			: ClusterManagerInfo(alloc, 0), pId_(UNDEF_PARTITIONID) {}

		bool check() {
			return true;
		}

		void getPartitionStatus(PartitionId &pId, PartitionStatus &status) {
			pId = pId_;
			status = static_cast<PartitionStatus>(status_);
		}

		bool isToSubMaster() {
			return isToSubMaster_;
		}

		ChangePartitionType getPartitionChangeType() {
			return static_cast<ChangePartitionType>(changePartitionType_);
		}

		MSGPACK_DEFINE(pId_, status_, isToSubMaster_, changePartitionType_);

		std::string dump() {
			util::NormalOStringStream ss;

			ss << "ChangePartitionStatusInfo:{"
			   << "pId:" << pId_ << ", status:"
			   << dumpPartitionStatus(static_cast<PartitionStatus>(status_))
			   << ", isToSubMaster:" << isToSubMaster_
			   << ", changePartitionType:"
			   << dumpChangePartitionType(
					  static_cast<ChangePartitionType>(changePartitionType_))
			   << "}";
			return ss.str();
		}

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
			util::StackAllocator &alloc, NodeId nodeId, PartitionRole &nextRole)
			: ClusterManagerInfo(alloc, nodeId), role_(nextRole) {}

		ChangePartitionTableInfo(util::StackAllocator &alloc, NodeId nodeId)
			: ClusterManagerInfo(alloc, nodeId) {}
		bool check() {
			role_.restoreType();
			return true;
		}

		PartitionRole &getPartitionRole() {
			return role_;
		}

		MSGPACK_DEFINE(role_);

		std::string dump() {
			util::NormalOStringStream ss;

			ss << "ChangePartitionTableInfo:{"
			   << "role:" << role_ << "}";
			return ss.str();
		}

	private:

		PartitionRole role_;
	};

	/*!
		@brief Represents the information of OrderDropPartition event
	*/
	class OrderDropPartitionInfo : public ClusterManagerInfo {
	public:

		OrderDropPartitionInfo(util::StackAllocator &alloc)
			: ClusterManagerInfo(alloc, 0) {}

		bool check() {
			return true;
		}

		std::vector<PartitionId> &getPartitionList() {
			return replicaLossPartitionList_;
		}

		bool isReplicaLoss() {
			return (replicaLossPartitionList_.size() > 0);
		}

		void setReplicaLoss(PartitionId pId) {
			replicaLossPartitionList_.push_back(pId);
		}

		MSGPACK_DEFINE(replicaLossPartitionList_);

		std::string dump() {
			util::NormalOStringStream ss;

			ss << "OrderDropPartitionInfo:{"
			   << "replicaLossPartitionList:" << replicaLossPartitionList_
			   << "}";
			return ss.str();
		}

	private:

		std::vector<PartitionId> replicaLossPartitionList_;
	};




	void get(HeartbeatInfo &heartbeatInfo);

	void set(HeartbeatInfo &heartbeatInfo);

	void get(HeartbeatResInfo &heartbeatResInfo, bool isIntialCluster = false);

	void set(HeartbeatResInfo &heartbeatResInfo);

	void get(HeartbeatCheckInfo &heartbeatCheckInfo);

	void set(UpdatePartitionInfo &updatePartitionInfo);

	void get(NotifyClusterInfo &notifyClusterInfo);

	bool set(NotifyClusterInfo &notifyClusterInfo);

	void get(NotifyClusterResInfo &notifyClusterResInfo);

	void set(NotifyClusterResInfo &notifyClusterResInfo);

	void set(JoinClusterInfo &joinClusterInfo);

	void set(LeaveClusterInfo &leaveClusterInfo);

	void get(GossipInfo &gossipInfo);

	void set(GossipInfo &gossipInfo);

	void set(ChangePartitionStatusInfo &changePartitionStatusInfo);

	void set(ChangePartitionTableInfo &changePartitionStatusInfo);


	int64_t nextHeartbeatTime(int64_t baseTime) {
		return (baseTime + clusterConfig_.getHeartbeatInterval() * 2 +
				extraConfig_.getNextHeartbeatMargin());
	}

#ifdef GD_ENABLE_UNICAST_NOTIFICATION
	void setDigest(const ConfigTable &configTable,
		util::XArray<uint8_t> &digestBinary, NodeAddressSet &addressInfoList,
		ClusterNotificationMode mode);

	PartitionTable *getPartitionTable() {
		return pt_;
	}

#endif


private:


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
		clusterInfo_.initialClusterConstructNum_ = initalClusterNum;
	}

	void setReserveNum(int32_t reserveNodeNum) {
		clusterInfo_.reserveNum_ = reserveNodeNum;
		if (reserveNodeNum == 0) {
			clusterInfo_.quorum_ = 0;
		}
		else {
			clusterInfo_.quorum_ = (reserveNodeNum / 2) + 1;
		}
	}

	int32_t getQuorum() {
		return clusterInfo_.quorum_;
	}

	int32_t getReserveNum() {
		return clusterInfo_.reserveNum_;
	}
	void setClusterName(const std::string &clusterName) {
		util::LockGuard<util::Mutex> lock(clusterLock_);
		clusterInfo_.clusterName_ = clusterName;
		clusterInfo_.isJoinCluster_ = true;
	}


	bool validateClusterName(std::string &clusterName);

	bool isNewNode() {
		return (clusterInfo_.initialClusterConstructNum_ == 0);
	}

	bool checkActiveNodeMax() {
		return (clusterInfo_.reserveNum_ <= clusterInfo_.activeNodeNum_);
	}

	bool checkSteadyStatus() {
		return clusterInfo_.isStable();
	}

	int32_t getActiveNum() {
		return clusterInfo_.activeNodeNum_;
	}

	void setActiveNodeList(std::vector<NodeId> &activeNodeList) {
		clusterInfo_.prevActiveNodeList_ = activeNodeList;
		clusterInfo_.activeNodeNum_ =
			static_cast<int32_t>(activeNodeList.size());
	}

	void clearActiveNodeList() {
		clusterInfo_.prevActiveNodeList_.clear();
	}

	std::string &getDigest() {
		return clusterInfo_.digest_;
	}

#ifdef GD_ENABLE_UNICAST_NOTIFICATION
#else
	void setDigest(const ConfigTable &configTable);
#endif

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

	int32_t updateNotifyPendingCount() {
		if (clusterInfo_.notifyPendingCount_ > 0) {
			clusterInfo_.notifyPendingCount_--;
			return clusterInfo_.notifyPendingCount_ + 1;
		}
		else {
			return 0;
		}
	}

	void detectMutliMaster();

	void setSecondMaster(NodeAddress &secondMasterNode);

	void updateSecondMaster(NodeId target, int64_t startupTime);



	void setShutdownPending() {
		statusInfo_.isShutdownPending_ = true;
	}



	/*!
		@brief Represents ClusterManager config
	*/
	class ClusterConfig {
	public:
		static const uint32_t OWNER_CATCHUP_LSN_CHECK_SLACK_INTERVAL = 60;
		static const uint32_t CHECKPOINT_DELAY_INTERVAL =
			1200;  

		ClusterConfig(const ConfigTable &config) {
			heartbeatInterval_ = changeTimeSecToMill(
				config.get<int32_t>(CONFIG_TABLE_CS_HEARTBEAT_INTERVAL));

			notifyClusterInterval_ = changeTimeSecToMill(
				config.get<int32_t>(CONFIG_TABLE_CS_NOTIFICATION_INTERVAL));

			notifyClientInterval_ = changeTimeSecToMill(
				config.get<int32_t>(CONFIG_TABLE_TXN_NOTIFICATION_INTERVAL));

			checkLoadBalanceInterval_ = changeTimeSecToMill(config.get<int32_t>(
				CONFIG_TABLE_CS_LOADBALANCE_CHECK_INTERVAL));

			shortTermTimeoutInterval_ = changeTimeSecToMill(
				config.get<int32_t>(CONFIG_TABLE_SYNC_TIMEOUT_INTERVAL));

			longTermTimeoutInterval_ = changeTimeSecToMill(config.get<int32_t>(
				CONFIG_TABLE_SYNC_LONG_SYNC_TIMEOUT_INTERVAL));

			duplicateMaxLsnNodeNum_ =
				config.get<int32_t>(CONFIG_TABLE_CS_MAX_LSN_REPLICATION_NUM);

			ownerCatchupPromoteCheckInterval_ =
				changeTimeSecToMill(OWNER_CATCHUP_LSN_CHECK_SLACK_INTERVAL);

#ifdef CLUSTER_MOD_WEB_API
			checkpointDelayLimitInterval_ = changeTimeSecToMill(
				config.get<int32_t>(CONFIG_TABLE_CS_CHECKPOINT_DELAY_INTERVAL));
#else
			checkpointDelayLimitInterval_ =
				changeTimeSecToMill(CHECKPOINT_DELAY_INTERVAL);
#endif
		}

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

		const std::string getSettedClusterName() const {
			return settedClusterName_;
		}

		int32_t getShortTermTimeoutInterval() const {
			return shortTermTimeoutInterval_;
		}

		int32_t getLongTermTimeoutInterval() const {
			return longTermTimeoutInterval_;
		}

		int32_t getDuplicateMaxLsnNodeNum() const {
			return duplicateMaxLsnNodeNum_;
		}

		void setClusterName(std::string &clusterName) {
			settedClusterName_ = clusterName;
		}

		void setDuplicateMaxLsnNodeNum(int32_t nodeNum) {
			duplicateMaxLsnNodeNum_ = nodeNum;
		}

		bool setShortTermTimeoutInterval(int32_t interval) {
			if (interval <= 0 || interval > INT32_MAX) {
				return false;
			}
			shortTermTimeoutInterval_ = interval;
			return true;
		}

		bool setLongTermTimeoutInterval(int32_t interval) {
			if (interval <= 0 || interval > INT32_MAX) {
				return false;
			}
			longTermTimeoutInterval_ = interval;
			return true;
		}

		int32_t getOwnerCatchupPromoteCheckInterval() const {
			return ownerCatchupPromoteCheckInterval_;
		}

		bool setOwnerCatchupPromoteCheckInterval(int32_t interval) {
			if (interval < 0 || interval > INT32_MAX) {
				return false;
			}
			ownerCatchupPromoteCheckInterval_ = interval;
			return true;
		}

		int32_t getCheckpointDelayLimitInterval() const {
			return checkpointDelayLimitInterval_;
		}

		bool setCheckpointDelayLimitInterval(int32_t interval) {
			if (interval < 0 || interval > INT32_MAX) {
				return false;
			}
			checkpointDelayLimitInterval_ = interval;
			return true;
		}

	private:

		int32_t heartbeatInterval_;
		int32_t notifyClusterInterval_;
		int32_t notifyClientInterval_;
		int32_t checkLoadBalanceInterval_;
		int32_t shortTermTimeoutInterval_;
		int32_t longTermTimeoutInterval_;
		int32_t duplicateMaxLsnNodeNum_;
		std::string settedClusterName_;
		int32_t ownerCatchupPromoteCheckInterval_;
		int32_t checkpointDelayLimitInterval_;
	};

	/*!
		@brief Represents extra config of ClusterManager
	*/
	class ExtraConfig {

		static const int32_t CS_RECONSTRUCT_WAIT_TIME = 3;
		static const int32_t NEXT_HEARTBEAT_MARGIN = 2;
		static const uint32_t CLUSTER_NAME_STRING_MAX = 64;

	public:

		ExtraConfig() {
			clusterReconstructWaitTime_ =
				changeTimeSecToMill(CS_RECONSTRUCT_WAIT_TIME);
			maxClusterNameSize_ = CLUSTER_NAME_STRING_MAX;
			nextHeartbeatMargin_ = changeTimeSecToMill(NEXT_HEARTBEAT_MARGIN);
		}

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
#ifdef GD_ENABLE_UNICAST_NOTIFICATION
		ClusterNotificationMode notificationMode_;
#endif
	};

#ifdef CLUSTER_MOD_WEB_API
	struct Config : public ConfigTable::ParamHandler {
		Config() : clsMgr_(NULL){};
		void setUpConfigHandler(
			ClusterManager *clsMgr, ConfigTable &configTable);
		virtual void operator()(
			ConfigTable::ParamId id, const ParamValue &value);
		ClusterManager *clsMgr_;
	};
#endif

	/*!
		@brief Represents cluster information
	*/
	struct ClusterInfo {

		ClusterInfo(PartitionTable *pt)
			: startupTime_(0),
			  reserveNum_(0),
			  isSecondMaster_(false),
			  isJoinCluster_(false),
			  isInitialCluster_(true),
			  initialClusterConstructNum_(0),
			  isParentMaster_(false),
			  secondMasterNodeId_(UNDEF_NODEID),
			  secondMasterStartupTime_(INT64_MAX),
			  notifyPendingCount_(0),
			  activeNodeNum_(0),
			  quorum_(0),
			  prevMaxNodeId_(1),
			  isPendingSync_(false),
			  isLoadBalance_(true),
			  isForceSync_(false),
			  isRepairPartition_(false),
			  pt_(pt),
			  isHeartbeatRes_(false)
#ifdef GD_ENABLE_UNICAST_NOTIFICATION
			  ,
			  mode_(NOTIFICATION_MULTICAST)
#endif
		{
		}

		bool isStable() {
			return (reserveNum_ == activeNodeNum_);
		}

		void initLeave() {
			clusterName_ = "";
			reserveNum_ = 0;
			isSecondMaster_ = false;
			initialClusterConstructNum_ = 0;
			isParentMaster_ = false;
			secondMasterNodeId_ = UNDEF_NODEID;
			secondMasterStartupTime_ = 0;
			secondMasterNodeAddress_.clear();
			notifyPendingCount_ = 0;
			activeNodeNum_ = 0;
			quorum_ = 0;
			prevMaxNodeId_ = 0;
			isPendingSync_ = false;
			isLoadBalance_ = true;
			isForceSync_ = false;
			isRepairPartition_ = false;
			prevActiveNodeList_.clear();
			downNodeList_.clear();
			isHeartbeatRes_ = false;
			isInitialCluster_ = true;
		}


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
		bool isPendingSync_;
		bool isLoadBalance_;
		bool isForceSync_;
		bool isRepairPartition_;
		PartitionTable *pt_;
		std::vector<NodeId> prevActiveNodeList_;
		std::set<NodeId> downNodeList_;
		bool isHeartbeatRes_;

#ifdef GD_ENABLE_UNICAST_NOTIFICATION
		ClusterNotificationMode mode_;
#endif

		std::string dump() {
			util::NormalOStringStream ss;
			ss << "ClusterInfo:{"
			   << "clusterName:" << clusterName_ << ", digest:" << digest_
			   << ", startupTime:" << startupTime_
			   << ", reserveNum:" << reserveNum_
			   << ", isSecondMaster:" << isSecondMaster_
			   << ", isJoinCluster:" << isJoinCluster_
			   << ", isInitialCluster:" << isInitialCluster_
			   << ", initialClusterConstructNum:" << initialClusterConstructNum_
			   << ", isParentMaster:" << isParentMaster_
			   << ", secondMasterNodeId:" << secondMasterNodeId_
			   << ", secondMasterNodeAddress:" << secondMasterNodeAddress_
			   << ", notifyPendingCount:" << notifyPendingCount_
			   << ", activeNodeNum:" << activeNodeNum_ << ", quorum:" << quorum_
			   << ", prevMaxNodeId:" << prevMaxNodeId_
			   << ", isPendingSync:" << isPendingSync_
			   << ", isLoadBalance:" << isLoadBalance_
			   << ", isForceSync:" << isForceSync_
			   << ", isRepairPartition:" << isRepairPartition_
			   << ", prevActiveNodeList:" << prevActiveNodeList_
			   << ", downNodeList:" << downNodeList_
			   << ", isHeartbeatRes:" << isHeartbeatRes_
			   << "}";
			return ss.str();
		}
	};

	/*!
		@brief Represents node status
	*/
	struct NodeStatusInfo {

		NodeStatusInfo()
			: isSystemError_(false),
			  currentStatus_(SYS_STATUS_BEFORE_RECOVERY),
			  nextStatus_(SYS_STATUS_BEFORE_RECOVERY),
			  isShutdown_(false),
			  isShutdownPending_(false) {}

		bool checkNodeStatus();

		bool checkCommandStatus(ClusterOperationType operation);

		void updateNodeStatus(ClusterOperationType operation);

		const char8_t *getSystemStatus() const;

		std::string dump() {
			util::NormalOStringStream ss;
			ss << "NodeStatusInfo:{"
			   << "isSystemError:" << isSystemError_
			   << ", status:" << dumpNodeStatus()
			   << ", currentStatus:" << currentStatus_
			   << ", nextStatus:" << nextStatus_
			   << ", isShutdown:" << isShutdown_
			   << ", isShutdownPending:" << isShutdownPending_ << "}";
			return ss.str();
		};

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
	};


	static class ConfigSetUpHandler : public ConfigTable::SetUpHandler {
		virtual void operator()(ConfigTable &config);
	} configSetUpHandler_;

	static class StatSetUpHandler : public StatTable::SetUpHandler {
		virtual void operator()(StatTable &stat);
	} statSetUpHandler_;

	class StatUpdator : public StatTable::StatUpdator {
		friend class ClusterServiceTest;
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

	std::vector<int64_t> delayCheckpointLimitTime_;
	int32_t concurrency_;

	PartitionId currentChunkSyncPId_;
	CheckpointId currentChunkSyncCPId_;

	SyncStat syncStat_;
	ErrorManager errorMgr_;

	EventEngine *ee_;

	ClusterService *clsSvc_;

#ifdef CLUSTER_MOD_WEB_API
	Config config_;
#endif
};

typedef ClusterManager::HeartbeatResInfo::HeartbeatResValue HeartbeatResValue;
typedef ClusterManager::HeartbeatResInfo::HeartbeatResStartValue
	HeartbeatResStartValue;

typedef ClusterManager::SyncStat SyncStat;

#endif
