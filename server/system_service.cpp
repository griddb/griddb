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
	@brief Implementation of SystemService
*/

#include "util/system.h"
#include "util/file.h"
#include "config_table.h"

#include "cluster_event_type.h"
#include "cluster_service.h"
#include "system_service.h"
#include "transaction_service.h"

#include "chunk_manager.h"  
#include "data_store.h"
#include "ebb_request_parser.h"
#include "json.h"
#include "log_manager.h"
#include "object_manager.h"
#include "picojson.h"
#include "sync_manager.h"

using util::ValueFormatter;


#ifndef _WIN32
#include <signal.h>  
#endif



typedef ObjectManager OCManager;

UTIL_TRACER_DECLARE(SYSTEM_SERVICE);
UTIL_TRACER_DECLARE(SYSTEM_SERVICE_DETAIL);
UTIL_TRACER_DECLARE(CLUSTER_OPERATION);




static void getServiceAddress(PartitionTable *pt, NodeId nodeId,
	picojson::object &result, ServiceType addressType) {
	try {
		picojson::value resultValue;
		pt->getServiceAddress(nodeId, resultValue, addressType);
		const picojson::object &resultObj = resultValue.get<picojson::object>();
		result.insert(resultObj.begin(), resultObj.end());
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION_WARNING(SYSTEM_SERVICE, e, "");
	}
}

static picojson::object &setJsonObject(picojson::value &value) {
	value = picojson::value(picojson::object_type, false);
	return value.get<picojson::object>();
}

static picojson::array &setJsonArray(picojson::value &value) {
	value = picojson::value(picojson::array_type, false);
	return value.get<picojson::array>();
}



SystemService::SystemService(ConfigTable &config, EventEngine::Config eeConfig,
	EventEngine::Source source, const char8_t *name,
	const char8_t *diffFilePath,
	ServiceThreadErrorHandler &serviceThreadErrorHandler)
	: ee_(createEEConfig(config, eeConfig), source, name),
	  gsVersion_(config.get<const char8_t *>(CONFIG_TABLE_DEV_SIMPLE_VERSION)),
	  serviceThreadErrorHandler_(serviceThreadErrorHandler),
	  clsSvc_(NULL),
	  clsMgr_(NULL),
	  syncSvc_(NULL),
	  pt_(NULL),
	  cpSvc_(NULL),
	  chunkMgr_(NULL),
	  txnSvc_(NULL),
	  txnMgr_(NULL),
	  recoveryMgr_(NULL),
	  fixedSizeAlloc_(NULL),
	  varSizeAlloc_(NULL),
	  multicastAddress_(
		  config.get<const char8_t *>(CONFIG_TABLE_TXN_NOTIFICATION_ADDRESS)),
	  multicastPort_(config.getUInt16(CONFIG_TABLE_TXN_NOTIFICATION_PORT)),
	  outputDiffFileName_(diffFilePath),
	  sysConfig_(config),
	  initailized_(false),
	  config_(config),
	  baseStats_(NULL)
{
	statUpdator_.service_ = this;
	try {
		ee_.setHandler(SYS_EVENT_OUTPUT_STATS, outputStatsHandler_);
		Event::Source eventSource(source);
		Event outputStatsEvent(eventSource, SYS_EVENT_OUTPUT_STATS, 0);
		ee_.addPeriodicTimer(
			outputStatsEvent, OUTPUT_STATS_INTERVAL_SEC * 1000);

		ee_.setThreadErrorHandler(serviceThreadErrorHandler_);

		const char8_t *catrgories[] = {"HASH_MAP", "BASE_CONTAINER",
			"CHECKPOINT_SERVICE_DETAIL", "SYSTEM_SERVICE_DETAIL", "REPLICATION",
			"SESSION_DETAIL", "TRANSACTION_DETAIL", "TIMEOUT_DETAIL",
			"RECOVERY_MANAGER_DETAIL", "CHUNK_MANAGER_DETAIL",
			"CHUNK_MANAGER_IO_DETAIL", NULL};

		for (size_t i = 0; catrgories[i] != NULL; i++) {
			unchangableTraceCategories_.insert(catrgories[i]);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

SystemService::~SystemService() {
	ee_.shutdown();
	ee_.waitForShutdown();
}

/*!
	@brief Starts SystemService
*/
void SystemService::start() {
	try {
		if (!initailized_) {
			GS_THROW_USER_ERROR(GS_ERROR_SC_SERVICE_NOT_INITIALIZED, "");
		}
		ee_.start();

		webapiServerThread_.start();

		GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_SERVICE_STARTED,
			"System service started");
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "Failed to start system service"));
	}
}

/*!
	@brief Shutdown SystemService
*/
void SystemService::shutdown() {
	ee_.shutdown();

	webapiServerThread_.shutdown();
}

/*!
	@brief Waits for shutdown SystemService
*/
void SystemService::waitForShutdown() {
	ee_.waitForShutdown();

	webapiServerThread_.waitForShutdown();
}

/*!
	@brief Sets EventEngine config
*/
EventEngine::Config &SystemService::createEEConfig(
	const ConfigTable &config, EventEngine::Config &eeConfig) {
	EventEngine::Config tmpConfig;
	eeConfig = tmpConfig;






	eeConfig.setPartitionCount(config.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM));

	eeConfig.setAllAllocatorGroup(ALLOCATOR_GROUP_SYS);

	return eeConfig;
}

void SystemService::initialize(ManagerSet &mgrSet) {
	clsSvc_ = mgrSet.clsSvc_;
	clsMgr_ = clsSvc_->getManager();
	pt_ = mgrSet.pt_;
	syncSvc_ = mgrSet.syncSvc_;
	cpSvc_ = mgrSet.cpSvc_;
	chunkMgr_ = mgrSet.chunkMgr_;
	dataStore_ = mgrSet.ds_;
	txnSvc_ = mgrSet.txnSvc_;
	txnMgr_ = mgrSet.txnMgr_;
	recoveryMgr_ = mgrSet.recoveryMgr_;
	fixedSizeAlloc_ = mgrSet.fixedSizeAlloc_;
	varSizeAlloc_ = mgrSet.varSizeAlloc_;
	logMgr_ = mgrSet.logMgr_;
	syncMgr_ = mgrSet.syncMgr_;

	baseStats_ = mgrSet.stats_;

	mgrSet.stats_->addUpdator(&statUpdator_);

	webapiServerThread_.initialize(mgrSet);
	outputStatsHandler_.initialize(mgrSet);
	serviceThreadErrorHandler_.initialize(mgrSet);


	initailized_ = true;
}

/*!
	@brief Handles joinCluster command
*/
bool SystemService::joinCluster(const Event::Source &eventSource,
	util::StackAllocator &alloc, const std::string &clusterName,
	uint32_t minNodeNum, picojson::value &result) {
	try {
		GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
			"Join cluster called (clusterName="
				<< clusterName << ", clusterNodeNum=" << minNodeNum << ")");

		ClusterManager::JoinClusterInfo joinClusterInfo(alloc, 0, true);
		joinClusterInfo.set(clusterName, minNodeNum);

		try {
			clsMgr_->set(joinClusterInfo);
		}
		catch (UserException &e) {
			picojson::object errorInfo;
			int32_t errorNo = 0;
			std::string reason;
			switch (joinClusterInfo.getErrorType()) {
			case WEBAPI_CS_CLUSTERNAME_SIZE_LIMIT: {
				util::NormalOStringStream oss;
				oss << "cluster name size:" << clusterName.size() << ", limit:"
					<< clsMgr_->getExtraConfig().getMaxClusterNameSize() << ".";
				reason = oss.str();
				errorNo = WEBAPI_CS_CLUSTERNAME_SIZE_LIMIT;
				break;
			}
			case WEBAPI_CS_CLUSTERNAME_UNMATCH: {
				errorNo = WEBAPI_CS_CLUSTERNAME_UNMATCH;
				reason = "cluster name unmatch, name=" + clusterName +
						 ", target=" +
						 clsMgr_->getConfig().getSettedClusterName() + ".";
				break;
			}
			case WEBAPI_CS_CLUSTERNAME_INVALID: {
				reason =
					"cluster name=" + clusterName + " includes invalid char.";
				errorNo = WEBAPI_CS_CLUSTERNAME_INVALID;
				break;
			}
			default: {
				reason = "other reason, see event log";
				errorNo = WEBAPI_CS_OTHER_REASON;
				break;
			}
			}
			errorInfo["errorStatus"] =
				picojson::value(static_cast<double>(errorNo));
			errorInfo["reason"] = picojson::value(reason);

			result = picojson::value(errorInfo);

			UTIL_TRACE_EXCEPTION_INFO(SYSTEM_SERVICE, e,
				"Join cluster failed (reason="
					<< reason << ", detail=" << GS_EXCEPTION_MESSAGE(e) << ")");

			return false;
		}

		clsSvc_->request(eventSource, CS_JOIN_CLUSTER, CS_HANDLER_PARTITION_ID,
			clsSvc_->getEE(), joinClusterInfo);

		return true;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "Join cluster failed"));
	}
}

/*!
	@brief wrapper function for joinCluster
*/
bool SystemService::joinCluster(const Event::Source &eventSource,
	util::StackAllocator &alloc, const std::string &clusterName,
	uint32_t minNodeNum) {
	picojson::value result;
	return joinCluster(eventSource, alloc, clusterName, minNodeNum, result);
}

/*!
	@brief Handles leaveCluster command
*/
bool SystemService::leaveCluster(const Event::Source &eventSource,
	util::StackAllocator &alloc, bool isForce) {
	try {
		GS_TRACE_INFO(
			SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED, "Leave cluster called");

		ClusterManager::LeaveClusterInfo leaveClusterInfo(
			alloc, 0, true, isForce);

		try {
			clsMgr_->set(leaveClusterInfo);
		}
		catch (UserException &e) {
			UTIL_TRACE_EXCEPTION_INFO(SYSTEM_SERVICE, e,
				GS_EXCEPTION_MERGE_MESSAGE(e, "Join cluster failed"));

			return false;
		}

		clsSvc_->request(eventSource, CS_LEAVE_CLUSTER, CS_HANDLER_PARTITION_ID,
			clsSvc_->getEE(), leaveClusterInfo);
		return true;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "Join cluster failed"));
	}
}

/*!
	@brief Handles shutdownNode command
*/
void SystemService::shutdownNode(const Event::Source &eventSource,
	util::StackAllocator &alloc, bool isForce) {
	try {
		if (isForce) {
			GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
				"Force shutdown node called");
		}
		else {
			GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
				"Normal shutdown node called");
		}

		ClusterManager::ShutdownNodeInfo shutdownNodeInfo(alloc, 0, isForce);

		EventType eventType;
		if (!isForce) {
			eventType = CS_SHUTDOWN_NODE_NORMAL;
		}
		else {
			eventType = CS_SHUTDOWN_NODE_FORCE;
		}
		clsSvc_->request(eventSource, eventType, CS_HANDLER_PARTITION_ID,
			clsSvc_->getEE(), shutdownNodeInfo);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "Shutdown node failed"));
	}
}

/*!
	@brief Handles shutdownCluster command
*/
void SystemService::shutdownCluster(
	const Event::Source &eventSource, util::StackAllocator &alloc) {
	try {
		GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
			"Shutdown cluster called");

		ClusterManager::ShutdownClusterInfo shutdownClusterInfo(alloc, 0);
		clsSvc_->request(eventSource, CS_SHUTDOWN_CLUSTER,
			CS_HANDLER_PARTITION_ID, clsSvc_->getEE(), shutdownClusterInfo);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "Shutdown node failed"));
	}
}

/*!
	@brief Handles checkpoint command
*/
void SystemService::checkpointNode(const Event::Source &eventSource) {
	GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
		"Requested checkpoint node called");

	try {
		cpSvc_->requestNormalCheckpoint(eventSource);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "Requested checkpoint failed"));
	}
}

/*!
	@brief Handles getHosts command
*/
void SystemService::getHosts(picojson::value &result, int32_t addressTypeNum) {
	try {
		GS_TRACE_DEBUG(
			SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED, "Get hosts called");

		const ServiceType addressType =
			static_cast<ServiceType>(addressTypeNum);

		picojson::object hosts;
		NodeId selfNodeId = 0;
		{
			picojson::object self;
			self["status"] = picojson::value(
				std::string(clsMgr_->statusInfo_.getSystemStatus()));
			getServiceAddress(pt_, selfNodeId, self, addressType);
			hosts["self"] = picojson::value(self);
		}

		NodeId masterNodeId = pt_->getMaster();
		if (masterNodeId != UNDEF_NODEID) {
			picojson::object master;
			getServiceAddress(pt_, masterNodeId, master, addressType);
			hosts["master"] = picojson::value(master);
		}

		NodeId followerCount = pt_->getNodeNum();
		picojson::array followerarray;

		for (NodeId followerNodeId = 0; followerNodeId < followerCount;
			 followerNodeId++) {
			if (followerNodeId == masterNodeId ||
				followerNodeId == selfNodeId ||
				pt_->getHeartbeatTimeout(followerNodeId) == UNDEF_TTL ||
				masterNodeId > 0) {
				continue;
			}
			picojson::object follower;
			getServiceAddress(pt_, followerNodeId, follower, addressType);
			followerarray.push_back(picojson::value(follower));
		}
		hosts["follower"] = picojson::value(followerarray);

#ifdef GD_ENABLE_UNICAST_NOTIFICATION
		if (clsSvc_->getNotificationManager().getMode() ==
			NOTIFICATION_MULTICAST)
#endif
		{
			picojson::object multicast;
			multicast["address"] = picojson::value(multicastAddress_);
			multicast["port"] =
				picojson::value(static_cast<double>(multicastPort_));
			hosts["multicast"] = picojson::value(multicast);
		}

		result = picojson::value(hosts);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "Get hosts failed"));
	}
}

/*!
	@brief Handles getPartitions command
*/
void SystemService::getPartitions(picojson::value &result, int32_t partitionNo,
	int32_t addressTypeNum, bool lossOnly, bool force, bool isSelf,
	bool lsnDump, bool notDumpRole, uint32_t partitionGroupNo) {
	try {
		GS_TRACE_DEBUG(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
			"Get partitions called");

		const ServiceType addressType =
			static_cast<ServiceType>(addressTypeNum);

		picojson::array &partitionList = setJsonArray(result);
		util::NormalOStringStream oss;
		LogSequentialNumber lsn;

		bool isAllSearch = (partitionNo == -1);
		bool isCheckPartitionGroup = (partitionGroupNo != UINT32_MAX);

		for (uint32_t pId = 0; pId < pt_->getPartitionNum(); pId++) {
			if (!isAllSearch && pId != static_cast<PartitionId>(partitionNo)) {
				continue;
			}

			if (isCheckPartitionGroup) {
				const PartitionGroupConfig &pgConfig =
					txnMgr_->getPartitionGroupConfig();
				if (pgConfig.getPartitionGroupId(pId) != partitionGroupNo) {
					continue;
				}
			}

			if (lossOnly) {
				if (pt_->isFollower()) {
					continue;
				}
				if (pt_->getOwner(pId) != UNDEF_NODEID && pt_->isMaster() &&
					!force) {
					continue;
				}
			}
			picojson::object partition;

			bool isOwner = false;
			bool isBackup = false;
			bool isCatchup = false;
			if (!notDumpRole) {
				{
					NodeId ownerNodeId = pt_->getOwner(pId);

					if (ownerNodeId == 0) isOwner = true;

					picojson::object master;
					getServiceAddress(pt_, ownerNodeId, master, addressType);

					if ((ownerNodeId != UNDEF_NODEID) || (pt_->isBackup(pId))) {
						if (pt_->isMaster() ||
							(pt_->isFollower() || ownerNodeId == 0)) {
							lsn = pt_->getLSN(pId, ownerNodeId);
						}
						else {
							lsn = 0;
						}
						master["lsn"] =
							picojson::value(static_cast<double>(lsn));
						partition["owner"] = picojson::value(master);
					}
					else {
						partition["owner"] = picojson::value();
					}
				}
				{
					picojson::array &nodeList =
						setJsonArray(partition["backup"]);
					std::vector<NodeId> backupList;
					pt_->getBackup(
						pId, backupList, PartitionTable::PT_CURRENT_OB);
					if (!backupList.empty()) {
						for (size_t pos = 0; pos < backupList.size(); pos++) {
							picojson::object backup;
							NodeId backupNodeId = backupList[pos];

							if (backupNodeId == 0) isBackup = true;

							if (backupNodeId == UNDEF_NODEID) {
								continue;
							}
							getServiceAddress(
								pt_, backupNodeId, backup, addressType);
							if (backupNodeId != UNDEF_NODEID) {
								if (pt_->isMaster() ||
									(pt_->isFollower() && backupNodeId == 0)) {
									lsn = pt_->getLSN(pId, backupNodeId);
								}
								else {
									lsn = 0;
								}
								backup["lsn"] =
									picojson::value(static_cast<double>(lsn));
								nodeList.push_back(picojson::value(backup));
							}
						}
					}
				}
				{
					picojson::array &nodeList =
						setJsonArray(partition["catchup"]);
					std::vector<NodeId> catchupList;
					pt_->getBackup(
						pId, catchupList, PartitionTable::PT_CURRENT_GOAL);
					if (!catchupList.empty()) {
						for (size_t pos = 0; pos < catchupList.size(); pos++) {
							picojson::object catchup;
							NodeId catchupNodeId = catchupList[pos];
							if (catchupNodeId == 0) isCatchup = true;
							if (catchupNodeId == UNDEF_NODEID) {
								continue;
							}

							getServiceAddress(
								pt_, catchupNodeId, catchup, addressType);
							if ((pt_->isMaster() &&
									catchupNodeId != UNDEF_NODEID) ||
								(pt_->isFollower() && catchupNodeId == 0)) {
								lsn = pt_->getLSN(pId, catchupNodeId);
								catchup["lsn"] =
									picojson::value(static_cast<double>(lsn));
								nodeList.push_back(picojson::value(catchup));
							}
						}
					}
				}

				{
					if ((pt_->isMaster() && (lossOnly || force))) {
						picojson::array &nodeList =
							setJsonArray(partition["all"]);
						for (NodeId nodeId = 0; nodeId < pt_->getNodeNum();
							 nodeId++) {
							picojson::object noneNode;
							getServiceAddress(
								pt_, nodeId, noneNode, addressType);
							lsn = pt_->getLSN(pId, nodeId);
							noneNode["lsn"] =
								picojson::value(static_cast<double>(lsn));
							if (pt_->getHeartbeatTimeout(nodeId) != UNDEF_TTL) {
								noneNode["status"] = picojson::value("ACTIVE");
							}
							else {
								noneNode["status"] =
									picojson::value("INACTIVE");
							}
							nodeList.push_back(picojson::value(noneNode));
						}
					}
				}
			}
			else {
				std::string retStr;
				if (pt_->isOwner(pId)) {
					retStr = "OWNER";
				}
				else if (pt_->isBackup(pId)) {
					retStr = "BACKUP";
				}
				else if (pt_->isBackup(
							 pId, 0, PartitionTable::PT_CURRENT_GOAL)) {
					retStr = "CATCHUP";
				}
				else {
					retStr = "NONE";
				}
				partition["role"] = picojson::value(retStr);
			}
			std::string tmp =
				pt_->dumpPartitionStatusForRest(pt_->getPartitionStatus(pId));
			partition["status"] = picojson::value(tmp);
			partition["pId"] = picojson::value(makeString(oss, pId));
			partition["maxLsn"] =
				picojson::value(static_cast<double>(pt_->getMaxLsn(pId)));
			if (lsnDump) {
				partition["lsn"] =
					picojson::value(static_cast<double>(pt_->getLSN(pId)));
			}
			if (isSelf && !isOwner && !isBackup && !isCatchup) {
				continue;
			}
			if (isCheckPartitionGroup) {
				partition["pgId"] =
					picojson::value(makeString(oss, partitionGroupNo));
			}

			partitionList.push_back(picojson::value(partition));
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "Get hosts failed"));
	}
}

/*!
	@brief Handles getStats command
*/
void SystemService::getStats(picojson::value &result, StatTable &stats) {
	try {
		GS_TRACE_INFO(SYSTEM_SERVICE_DETAIL, GS_TRACE_SC_WEB_API_CALLED,
			"Get stat called");

		stats.updateAll();
		stats.toJSON(result, STAT_TABLE_ROOT, StatTable::EXPORT_MODE_DEFAULT);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "Get stat failed"));
	}
}

/*!
	@brief Gets the statistics about synchronization
*/
void SystemService::getSyncStats(picojson::value &result) {
	picojson::object &syncStats = setJsonObject(result);

	syncStats["unfixCount"] = picojson::value(
		static_cast<double>(syncMgr_->getSyncOptStat()->getUnfixCount()));
	syncStats["contextCount"] = picojson::value(
		static_cast<double>(syncMgr_->getSyncOptStat()->getContextCount()));

	PartitionId currentCatchupPId = pt_->getCatchupPId();
	if (currentCatchupPId != UNDEF_PARTITIONID) {
		util::StackAllocator alloc(
			util::AllocatorInfo(ALLOCATOR_GROUP_SYS, "syncStats"),
			fixedSizeAlloc_);

		util::XArray<NodeId> catchups(alloc);
		NodeId owner = pt_->getOwner(currentCatchupPId);
		pt_->getBackup(
			currentCatchupPId, catchups, PartitionTable::PT_CURRENT_GOAL);
		if (owner != UNDEF_NODEID && catchups.size() == 1) {
			int64_t limitTime = pt_->getLongTermLimitTime(currentCatchupPId);
			if (limitTime != UNDEF_TTL) {
				int64_t startTime =
					limitTime -
					clsMgr_->getConfig().getLongTermTimeoutInterval();
				syncStats["pId"] =
					picojson::value(static_cast<double>(currentCatchupPId));
				syncStats["beginTime"] = picojson::value(getTimeStr(startTime));
				syncStats["limitTime"] = picojson::value(getTimeStr(limitTime));

				NodeId catchup = *catchups.begin();
				picojson::object ownerInfo, catchupInfo;
				SyncStat &stat = clsMgr_->getSyncStat();

				ownerInfo["lsn"] = picojson::value(
					static_cast<double>(pt_->getLSN(currentCatchupPId, owner)));
				ownerInfo["chunkNum"] =
					picojson::value(static_cast<double>(stat.syncChunkNum_));
				getServiceAddress(pt_, owner, ownerInfo, SYSTEM_SERVICE);
				syncStats["owner"] = picojson::value(ownerInfo);

				catchupInfo["lsn"] = picojson::value(static_cast<double>(
					pt_->getLSN(currentCatchupPId, catchup)));
				catchupInfo["chunkNum"] =
					picojson::value(static_cast<double>(stat.syncApplyNum_));
				getServiceAddress(pt_, catchup, catchupInfo, SYSTEM_SERVICE);
				syncStats["catchup"] = picojson::value(catchupInfo);
			}
		}
		if (cpSvc_->getCurrentCpPId() != UNDEF_PARTITIONID) {
			picojson::object cpInfo;
			cpInfo["pId"] =
				picojson::value(static_cast<double>(cpSvc_->getCurrentCpPId()));
			cpInfo["pgId"] = picojson::value(
				static_cast<double>(cpSvc_->getCurrentCpGroupId()));
			uint32_t pgId;
			std::string retStr;
			if (clsMgr_->getCurrentPendingCheckpointInfo(pgId, retStr)) {
				cpInfo["pendingPgId"] =
					picojson::value(static_cast<double>(pgId));
				cpInfo["pendingLimitTime"] = picojson::value(retStr);
			}
			syncStats["cpInfo"] = picojson::value(cpInfo);
		}
	}
}

/*!
	@brief Gets the statistics about memory usage for each Partition group
*/

void SystemService::getPGStoreMemoryLimitStats(picojson::value &result) {
	picojson::array &limitStats = setJsonArray(result);

	PartitionGroupId partitionGroupNum =
		chunkMgr_->getConfig().getPartitionGroupNum();
	for (PartitionGroupId pgId = 0; pgId < partitionGroupNum; pgId++) {
		picojson::object pgInfo;

		ChunkManager::ChunkManagerStats &stats =
			chunkMgr_->getChunkManagerStats();

		pgInfo["pgLimit"] = picojson::value(
			static_cast<double>(stats.getPGStoreMemoryLimit(pgId)));
		pgInfo["pgMemory"] =
			picojson::value(static_cast<double>(stats.getPGStoreMemory(pgId)));
		pgInfo["pgSwapRead"] =
			picojson::value(static_cast<double>(stats.getPGSwapRead(pgId)));
		limitStats.push_back(picojson::value(pgInfo));
	}
}

/*!
	@brief Gets the statistics about memory usage
*/
void SystemService::getMemoryStats(picojson::value &result,
	const char8_t *namePrefix, const char8_t *selectedType, int64_t minSize) {
	util::AllocatorManager &allocMgr =
		util::AllocatorManager::getDefaultInstance();

	typedef std::vector<util::AllocatorGroupId> IdList;
	IdList idList;
	idList.push_back(ALLOCATOR_GROUP_ROOT);
	allocMgr.listSubGroup(
		ALLOCATOR_GROUP_ROOT, std::back_inserter(idList), true);

	typedef std::vector<util::AllocatorStats> StatsList;
	StatsList statsList;
	allocMgr.getAllocatorStats(
		&idList[0], idList.size(), std::back_inserter(statsList));

	picojson::object &resultObj = setJsonObject(result);
	picojson::array &allocList = setJsonArray(resultObj["allocators"]);
	for (StatsList::iterator it = statsList.begin(); it != statsList.end();
		 ++it) {
		const util::AllocatorStats &stats = *it;

		allocList.push_back(picojson::value());
		picojson::object &entry = setJsonObject(allocList.back());
		std::string name;
		{
			util::NormalOStringStream oss;
			oss << stats.info_;
			name = oss.str();
			if (name.find(namePrefix) != 0) {
				allocList.pop_back();
				continue;
			}
		}

		picojson::object &statsObj = setJsonObject(entry[name]);
		const char8_t *const typeList[] = {
			"totalSize", "peakTotalSize", "cacheSize", "cacheMissCount",
			"cacheAdjustCount", "cacheAdjustCount", "hugeAllocationCount",
			"allocationCount", "deallocationCount", NULL, "cacheLimit",
			"stableLimit",
		};

		bool found = false;
		for (int32_t i = 0; i < util::AllocatorStats::STAT_TYPE_END; i++) {
			if (typeList[i] == NULL ||
				(strlen(selectedType) > 0 &&
					strcmp(typeList[i], selectedType) != 0)) {
				continue;
			}

			if (stats.values_[i] < minSize) {
				continue;
			}
			found = true;
			statsObj[typeList[i]] =
				picojson::value(static_cast<double>(stats.values_[i]));
		}

		if (!found) {
			allocList.pop_back();
		}
	}
}

/*!
	@brief Gets/Sets config of SystemService
*/
bool SystemService::getOrSetConfig(const std::vector<std::string> namePath,
	picojson::value &result, const picojson::value *paramValue, bool noUnit) {
	const bool updating = (paramValue != NULL);
	GS_TRACE_INFO(SYSTEM_SERVICE_DETAIL, GS_TRACE_SC_WEB_API_CALLED,
		(updating ? "Set" : "Get") << " config called");

	try {
		ConfigTable::ParamId id = CONFIG_TABLE_ROOT;
		for (std::vector<std::string>::const_iterator it = namePath.begin();
			 it != namePath.end(); ++it) {
			const ConfigTable::ParamId groupId = id;
			if (!config_.findParam(groupId, id, it->c_str())) {
				GS_TRACE_WARNING(SYSTEM_SERVICE, GS_TRACE_SC_BAD_REQUEST,
					"Unknown config name (basePath="
						<< config_.pathFormatter(groupId) << ", name=" << *it
						<< ")");
				return false;
			}
		}

		if (updating) {
			assert(paramValue != NULL);

			util::NormalOStringStream oss;
			oss << "Web API (updated=" << util::DateTime::now(false) << ")";
			try {
				config_.set(id, *paramValue, oss.str().c_str());
			}
			catch (UserException &e) {
				UTIL_TRACE_EXCEPTION_WARNING(SYSTEM_SERVICE, e, "");
				return false;
			}

			picojson::value accepted;
			config_.toJSON(accepted, id, ConfigTable::EXPORT_MODE_SHOW_HIDDEN);

			GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_CONFIG_UPDATED,
				"Configuration updated (name=" << config_.pathFormatter(id)
											   << ", acceptedValue=" << accepted
											   << ", inputValue=" << *paramValue
											   << ")");

			config_.toFile(outputDiffFileName_.c_str(), CONFIG_TABLE_ROOT,
				ConfigTable::EXPORT_MODE_DIFF_ONLY);
		}
		else {
			config_.toJSON(result, id,
				ConfigTable::EXPORT_MODE_DEFAULT |
					(noUnit ? ConfigTable::EXPORT_MODE_NO_UNIT : 0));
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(
				   e, (updating ? "Set" : "Get") << " config failed"));
	}

	return true;
}

bool SystemService::getEventStats(picojson::value &result, bool reset) {
	return false;
}

/*!
	@brief Handles getLogs command
*/
void SystemService::getLogs(picojson::value &result, std::string &searchStr,
	std::string &searchStr2, std::string &ignoreStr, uint32_t length) {
	try {
		picojson::array logs;
		std::vector<std::string> history;
		std::deque<std::string> tmpResult;
		util::TraceManager::getInstance().getHistory(history);
		uint32_t counter = 0;
		if (length == UINT32_MAX) {
			length = 30;
		}

		for (std::vector<std::string>::iterator it = history.begin();
			 it != history.end(); ++it) {
			if (!searchStr.empty()) {
				std::string::size_type index = (*it).find(searchStr);
				if (index == std::string::npos) {
					continue;
				}
			}

			if (!searchStr2.empty()) {
				std::string::size_type index = (*it).find(searchStr2);
				if (index == std::string::npos) {
					continue;
				}
			}

			if (!ignoreStr.empty()) {
				std::string::size_type index = (*it).find(ignoreStr);
				if (index != std::string::npos) {
					continue;
				}
			}

			counter++;
			tmpResult.push_back((*it));
			if (counter > length) {
				tmpResult.pop_front();
			}
		}

		for (std::deque<std::string>::iterator it2 = tmpResult.begin();
			 it2 != tmpResult.end(); it2++) {
			logs.push_back(picojson::value(*it2));
		}
		result = picojson::value(logs);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "Get logs failed"));
	}
}

/*!
	@brief Handles setEventLogLevel command
*/
bool SystemService::setEventLogLevel(
	const std::string &category, const std::string &level, bool force) {
	try {
		if (!force &&
			unchangableTraceCategories_.find(category) !=
				unchangableTraceCategories_.end()) {
			GS_TRACE_WARNING(SYSTEM_SERVICE, GS_TRACE_SC_BAD_REQUEST,
				"Unknown event log category (value=" << category << ")");
			return false;
		}

		int32_t outputLevel = 0;
		if (!util::TraceManager::stringToOutputLevel(level, outputLevel)) {
			GS_TRACE_WARNING(SYSTEM_SERVICE, GS_TRACE_SC_BAD_REQUEST,
				"Unknown event log level (value=" << level << ")");
			return false;
		}
		util::Tracer *tracer =
			util::TraceManager::getInstance().getTracer(category.c_str());
		if (!tracer) {
			GS_TRACE_WARNING(SYSTEM_SERVICE, GS_TRACE_SC_BAD_REQUEST,
				"Unknown event log category (value=" << category << ")");
			return false;
		}
		assert(outputLevel != 0);
		tracer->setMinOutputLevel(outputLevel);
		GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_EVENT_LOG_UPDATED,
			"Event log level updated (category=" << category
												 << ", level=" << level << ")");
		return true;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(
				   e, "Failed to set event log level (category="
						  << category << ", level=" << level << ")"));
	}
}

/*!
	@brief Handles getEventLogLevel command
*/
void SystemService::getEventLogLevel(picojson::value &result) {
	try {
		std::vector<util::Tracer *> tracerList;
		util::TraceManager::getInstance().getAllTracers(tracerList);
		picojson::object levelMap;
		for (size_t pos = 0; pos < tracerList.size(); ++pos) {
			if (unchangableTraceCategories_.find(tracerList[pos]->getName()) ==
				unchangableTraceCategories_.end()) {
				levelMap[tracerList[pos]->getName()] = picojson::value(
					std::string(util::TraceManager::outputLevelToString(
						tracerList[pos]->getMinOutputLevel())));
			}
		}
		picojson::object output;
		output["levels"] = picojson::value(levelMap);
		result = picojson::value(output);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "Failed to get event log level"));
	}
}

void SystemService::testEventLogLevel() {
}

/*!
	@brief Writes statistics periodically to Event Log file
*/
void SystemService::traceStats(const util::StdAllocator<void, void> &alloc) {
	try {
		StatTable stats(alloc);
		stats.initialize(*baseStats_);

		const TraceMode traceMode = sysConfig_.getTraceMode();

		if (traceMode == TRACE_MODE_SIMPLE_DETAIL ||
			traceMode == TRACE_MODE_FULL) {
			stats.setDisplayOption(
				STAT_TABLE_DISPLAY_WEB_OR_DETAIL_TRACE, true);
		}

		if (traceMode == TRACE_MODE_FULL) {
			stats.setDisplayOption(STAT_TABLE_DISPLAY_WEB_ONLY, true);
		}

		stats.setDisplayOption(STAT_TABLE_DISPLAY_SELECT_CS, true);
		stats.setDisplayOption(STAT_TABLE_DISPLAY_SELECT_CP, true);
		stats.setDisplayOption(STAT_TABLE_DISPLAY_SELECT_RM, true);
		stats.setDisplayOption(STAT_TABLE_DISPLAY_SELECT_PERF, true);

		stats.updateAll();

		util::NormalOStringStream oss;
		bool first = true;

		ParamTable::ParamId nextId;
		for (ParamTable::ParamId id = STAT_TABLE_ROOT;
			 stats.getNextParam(id, nextId, true);) {
			id = nextId;

			if (!first) {
				oss << ", ";
			}

			if (STAT_TABLE_PERF_DS_DETAIL_ALL_START < id &&
				id < STAT_TABLE_PERF_DS_DETAIL_ALL_END) {
				ParamTable::ParamId parentId;
				stats.getParentParam(id, parentId);
				oss << stats.getName(parentId) << ".";
			}

			oss << stats.getName(id);
			oss << "=";
			oss << *stats.getAnnotatedValue(id, true, false).first;

			first = false;
		}

		GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_TRACE_STATS, oss.str());
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(SYSTEM_SERVICE, e,
			GS_EXCEPTION_MERGE_MESSAGE(e, "Trace stats failed"));
	}
}

/*!
	@brief Gets address type
*/
int32_t SystemService::resolveAddressType(
	const WebAPIRequest::ParameterMap &parameterMap, ServiceType defaultType) {
	std::map<std::string, std::string>::const_iterator it =
		parameterMap.find("addressType");

	if (it == parameterMap.end()) {
		return defaultType;
	}
	const std::string &typeStr = it->second;

	if (typeStr == "cluster") {
		return CLUSTER_SERVICE;
	}
	else if (typeStr == "transaction") {
		return TRANSACTION_SERVICE;
	}
	else if (typeStr == "sync") {
		return SYNC_SERVICE;
	}
	else if (typeStr == "system") {
		return SYSTEM_SERVICE;
	}
	else {
		return -1;
	}
}

/*!
	@brief Sets display options
*/
bool SystemService::acceptStatsOption(
	StatTable &stats, const WebAPIRequest::ParameterMap &parameterMap) {
	stats.setDisplayOption(STAT_TABLE_DISPLAY_WEB_OR_DETAIL_TRACE, true);
	stats.setDisplayOption(STAT_TABLE_DISPLAY_WEB_ONLY, true);

	switch (resolveAddressType(parameterMap, DEFAULT_ADDRESS_TYPE)) {
	case CLUSTER_SERVICE:
		stats.setDisplayOption(STAT_TABLE_DISPLAY_ADDRESS_CLUSTER, true);
		break;
	case TRANSACTION_SERVICE:
		stats.setDisplayOption(STAT_TABLE_DISPLAY_ADDRESS_TRANSACTION, true);
		break;
	case SYNC_SERVICE:
		stats.setDisplayOption(STAT_TABLE_DISPLAY_ADDRESS_SYNC, true);
		break;
	case SYSTEM_SERVICE:
		break;
	default:
		return false;
	}

#ifdef GD_ENABLE_UNICAST_NOTIFICATION
	const size_t maxNameListSize = 9;
#else
	const size_t maxNameListSize = 8;
#endif

	const char8_t *const allNameList[][maxNameListSize] = {
		{"all", "clusterInfo", "recoveryInfo", "cpInfo", "performanceInfo",
			NULL},
		{"detail", "clusterDetail", "storeDetail", "transactionDetail",
			"memoryDetail", "syncDetail", "pgLimitDetail",
#ifdef GD_ENABLE_UNICAST_NOTIFICATION
			"notificationMember",
#endif
			NULL}};

	const int32_t allOptionList[][maxNameListSize] = {
		{-1, STAT_TABLE_DISPLAY_SELECT_CS, STAT_TABLE_DISPLAY_SELECT_CP,
			STAT_TABLE_DISPLAY_SELECT_RM, STAT_TABLE_DISPLAY_SELECT_PERF, -1},
		{-1, STAT_TABLE_DISPLAY_OPTIONAL_CS, STAT_TABLE_DISPLAY_OPTIONAL_DS,
			STAT_TABLE_DISPLAY_OPTIONAL_TXN, STAT_TABLE_DISPLAY_OPTIONAL_MEM,
			STAT_TABLE_DISPLAY_OPTIONAL_SYNC,
			STAT_TABLE_DISPLAY_OPTIONAL_PGLIMIT,
#ifdef GD_ENABLE_UNICAST_NOTIFICATION
			STAT_TABLE_DISPLAY_OPTIONAL_NOTIFICATION_MEMBER,
#endif
			-1}};

	for (size_t i = 0; i < sizeof(allNameList) / sizeof(*allNameList); i++) {
		const char8_t *const *nameList = allNameList[i];
		const int32_t *optionList = allOptionList[i];

		bool found = false;
		for (const char8_t *const *nameIt = nameList; *nameIt != NULL;
			 ++nameIt) {
			const int32_t option = optionList[nameIt - nameList];

			WebAPIRequest::ParameterMap::const_iterator it =
				parameterMap.find(*nameIt);
			if (it == parameterMap.end()) {
				continue;
			}

			bool enabled;
			if (it->second == "true" || it->second == "on") {
				enabled = true;
			}
			else if (it->second == "false" || it->second == "off") {
				enabled = false;
			}
			else {
				return false;
			}

			found = true;
			if (option < 0) {
				for (const int32_t *optionIt = optionList;
					 *(++optionIt) >= 0;) {
					stats.setDisplayOption(*optionIt, enabled);
				}
			}
			else {
				stats.setDisplayOption(option, enabled);
			}
		}

		if (!found && i == 0) {
			for (const int32_t *optionIt = optionList; *(++optionIt) >= 0;) {
				stats.setDisplayOption(*optionIt, true);
			}
		}
	}

	return true;
}

/*!
	@brief Handler Operator
*/
void OutputStatsHandler::operator()(EventContext &ec, Event &) {
	sysSvc_->traceStats(ec.getVariableSizeAllocator());
}


SystemService::WebAPIServerThread::WebAPIServerThread() : runnable_(true) {}

void SystemService::WebAPIServerThread::initialize(ManagerSet &mgrSet) {
	sysSvc_ = mgrSet.sysSvc_;
	clsSvc_ = mgrSet.clsSvc_;
	mgrSet_ = &mgrSet;
}

/*!
	@brief Starts thread for Web API
*/
void SystemService::WebAPIServerThread::run() {
	try {
		ListenerSocketHandler socketHandler(*mgrSet_);
		util::Socket &socket = socketHandler.getFile();

		socket.open(
			util::SocketAddress::FAMILY_INET, util::Socket::TYPE_STREAM);
		socket.setBlockingMode(false);
		socket.setReuseAddress(true);

		util::SocketAddress addr;
		EventEngine::Config tmp;
		tmp.setServerAddress(mgrSet_->config_->get<const char8_t *>(
								 CONFIG_TABLE_SYS_SERVICE_ADDRESS),
			mgrSet_->config_->getUInt16(CONFIG_TABLE_SYS_SERVICE_PORT));

		addr.assign(
			NULL, tmp.serverAddress_.getPort(), tmp.serverAddress_.getFamily());

		socket.bind(addr);

		socket.listen();
		util::IOPoll poll;
		poll.add(&socketHandler, util::IOPollEvent::TYPE_READ);

		while (runnable_) {
			poll.dispatch(ACCEPT_POLLING_MILLISEC);
		}

		socket.close();
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(SYSTEM_SERVICE, e, "");
		clsSvc_->shutdownAllService(true);
	}
}

/*!
	@brief Sets mode to shutdown thread for Web API
*/
void SystemService::WebAPIServerThread::shutdown() {
	runnable_ = false;
}

/*!
	@brief Waits for shutdown
*/
void SystemService::WebAPIServerThread::waitForShutdown() {
	join();
}

SystemService::ListenerSocketHandler::ListenerSocketHandler(ManagerSet &mgrSet)
	: clsSvc_(mgrSet.clsSvc_),
	  syncSvc_(mgrSet.syncSvc_),
	  sysSvc_(mgrSet.sysSvc_),
	  pt_(mgrSet.pt_),
	  syncMgr_(mgrSet.syncSvc_->getManager()),
	  clsMgr_(mgrSet.clsSvc_->getManager()),
	  chunkMgr_(mgrSet.chunkMgr_),
	  cpSvc_(mgrSet.cpSvc_),
	  fixedSizeAlloc_(mgrSet.fixedSizeAlloc_),
	  varSizeAlloc_(util::AllocatorInfo(ALLOCATOR_GROUP_SYS, "webListenerVar")),
	  alloc_(util::AllocatorInfo(ALLOCATOR_GROUP_SYS, "webListenerStack"),
		  fixedSizeAlloc_) {
	alloc_.setFreeSizeLimit(alloc_.base().getElementSize());
}

/*!
	@brief Polling
*/
void SystemService::ListenerSocketHandler::handlePollEvent(
	util::IOPollBase *, util::IOPollEvent event) {
	if (event & ~util::IOPollEvent::TYPE_READ) {
		return;
	}

	try {
		util::Socket socket;
		listenerSocket_.accept(&socket);

		socket.setBlockingMode(true);
		socket.setReceiveTimeout(ACCEPT_POLLING_MILLISEC * 4);
		socket.setSendTimeout(ACCEPT_POLLING_MILLISEC * 2);

		util::NormalXArray<char8_t> buffer;
		WebAPIRequest request;
		do {
			if (!readOnce(socket, buffer)) {
				return;
			}
		} while (!request.set(buffer));
		WebAPIResponse response(request);

		try {
			util::StackAllocator::Scope scope(alloc_);
			dispatch(request, response);
		}
		catch (std::exception &e) {
			UTIL_TRACE_EXCEPTION(SYSTEM_SERVICE, e, "");

			response.setInternalServerError();
			response.hasJson_ = false;
		}
		alloc_.trim();

		response.update();
		socket.send(response.header_.c_str(), response.header_.size());
		socket.send(response.body_.c_str(), response.body_.size());
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(SYSTEM_SERVICE, e, "");
	}
}

util::Socket &SystemService::ListenerSocketHandler::getFile() {
	return listenerSocket_;
}

/*!
	@brief Handles Web API command
*/
void SystemService::ListenerSocketHandler::dispatch(
	WebAPIRequest &request, WebAPIResponse &response) {
	if (!request.jsonError_.empty()) {
		GS_TRACE_WARNING(SYSTEM_SERVICE, GS_TRACE_SC_BAD_REQUEST,
			"Failed to parse json request (reason=" << request.jsonError_
													<< ")");
		response.setBadRequestError();
		return;
	}

	Event::Source eventSource(varSizeAlloc_);
	if (!sysSvc_->getUserTable().isValidUser(
			request.user_.c_str(), request.password_.c_str(), true)) {
		response.setAuthError();
		return;
	}

	if (!clsMgr_->checkRecoveryCompleted()) {
		if (request.pathElements_.size() >= 2 &&
			request.pathElements_[0] == "node" &&
			request.pathElements_[1] == "stat") {
		}
		else {
			picojson::object errorInfo;
			std::string reason("command not executed, under recovering");
			picojson::value result;
			int32_t errorNo = WEBAPI_NOT_RECOVERY_CHECKPOINT;
			errorInfo["errorStatus"] =
				picojson::value(static_cast<double>(errorNo));
			errorInfo["reason"] = picojson::value(reason);
			result = picojson::value(errorInfo);
			response.setJson(result);
			response.setBadRequestError();
			return;
		}
	}

	if (request.pathElements_.size() >= 2 &&
		request.pathElements_[0] == "node") {
		if (request.pathElements_.size() == 2 &&
			request.pathElements_[1] == "join") {
			if (request.method_ != EBB_POST) {
				response.setMethodError();
				return;
			}

			if (request.parameterMap_.find("clusterName") ==
				request.parameterMap_.end()) {
				response.setBadRequestError();
				return;
			}

			const std::string clusterName =
				request.parameterMap_["clusterName"];
			if (clusterName.empty()) {
				response.setBadRequestError();
				return;
			}

			int32_t minNodeNum = 0;  
			if (request.parameterMap_.find("minNodeNum") !=
				request.parameterMap_.end()) {
				minNodeNum = atoi(request.parameterMap_["minNodeNum"].c_str());
				if (minNodeNum < 0) {
					response.setBadRequestError();
					return;
				}
			}
			{
				picojson::value result;
				if (!sysSvc_->joinCluster(eventSource, alloc_, clusterName,
						static_cast<uint32_t>(minNodeNum), result)) {
					response.setJson(result);
					response.setBadRequestError();
				}
			}
		}
		else if (request.pathElements_.size() == 2 &&
				 request.pathElements_[1] == "leave") {
			if (request.method_ != EBB_POST) {
				response.setMethodError();
				return;
			}
			bool isForce = true;
			if (request.parameterMap_.find("force") !=
				request.parameterMap_.end()) {
				const std::string isForceCheckStr =
					request.parameterMap_["force"];
				if (isForceCheckStr == "false") {
					isForce = false;
				}
				else if (isForceCheckStr != "true") {
					response.setBadRequestError();
					return;
				}
			}
			if (!sysSvc_->leaveCluster(eventSource, alloc_, isForce)) {
				response.setBadRequestError();
			}
		}
		else if (request.pathElements_.size() == 2 &&
				 request.pathElements_[1] == "shutdown") {
			if (request.method_ != EBB_POST) {
				response.setMethodError();
				return;
			}

			bool force = false;
			if (request.parameterMap_.find("force") !=
				request.parameterMap_.end()) {
				const std::string forceStr = request.parameterMap_["force"];
				if (forceStr == "true") {
					force = true;
				}
				else if (forceStr != "false") {
					response.setBadRequestError();
					return;
				}
			}
			sysSvc_->shutdownNode(eventSource, alloc_, force);
		}
		else if (request.pathElements_.size() == 2 &&
				 request.pathElements_[1] == "host") {
			if (request.method_ != EBB_GET) {
				response.setMethodError();
				return;
			}

			const int32_t addressTypeNum =
				resolveAddressType(request.parameterMap_, DEFAULT_ADDRESS_TYPE);
			if (addressTypeNum < 0) {
				response.setBadRequestError();
				return;
			}

			picojson::value result;
			sysSvc_->getHosts(result, addressTypeNum);
			response.setJson(result);
		}
		else if (request.pathElements_.size() >= 2 &&
				 request.pathElements_[1] == "stat") {
			if (request.method_ != EBB_GET) {
				response.setMethodError();
				return;
			}

			StatTable stats(varSizeAlloc_);
			stats.initialize(*sysSvc_->baseStats_);

			if (!acceptStatsOption(stats, request.parameterMap_)) {
				response.setBadRequestError();
				return;
			}

			picojson::value result;
			sysSvc_->getStats(result, stats);

			if (request.pathElements_.size() > 2) {
				const std::vector<std::string> &elems = request.pathElements_;
				for (uint32_t p = 2; p < elems.size(); p++) {
					picojson::value subResult = result.get(elems[p]);
					result = subResult;
					if (!result.is<picojson::object>()) {
						return;
					}
				}
				picojson::object namedResult;
				namedResult[elems[elems.size() - 1]] = result;
				result = picojson::value(namedResult);
			}

			if (request.parameterMap_.find("callback") !=
				request.parameterMap_.end()) {
				response.setJson(request.parameterMap_["callback"], result);
			}
			else {
				response.setJson(result);
			}
		}
		else if (request.pathElements_.size() >= 2 &&
				 request.pathElements_[1] == "partition") {
			if (request.pathElements_.size() == 2) {
				if (request.method_ != EBB_GET) {
					response.setMethodError();
					return;
				}

				const int32_t addressTypeNum = resolveAddressType(
					request.parameterMap_, DEFAULT_ADDRESS_TYPE);
				if (addressTypeNum < 0) {
					response.setBadRequestError();
					return;
				}

				int32_t partitionNo = -1;
				if (request.parameterMap_.find("partitionNo") !=
					request.parameterMap_.end()) {
					partitionNo =
						atoi(request.parameterMap_["partitionNo"].c_str());
					if (partitionNo >=
						static_cast<int32_t>(pt_->getPartitionNum())) {
						response.setBadRequestError();
						return;
					}
				}

				bool isLossOnly = false;
				if (request.parameterMap_.find("loss") !=
					request.parameterMap_.end()) {
					const std::string retStr = request.parameterMap_["loss"];
					if (retStr == "true") {
						isLossOnly = true;
					}
				}

				bool isForce = false;
				if (request.parameterMap_.find("force") !=
					request.parameterMap_.end()) {
					const std::string retStr = request.parameterMap_["force"];
					if (retStr == "true") {
						isForce = true;
					}
				}

				bool isSelf = false;
				if (request.parameterMap_.find("isSelf") !=
					request.parameterMap_.end()) {
					const std::string retStr = request.parameterMap_["isSelf"];
					if (retStr == "true") {
						isSelf = true;
					}
				}

				bool lsnDump = false;
				if (request.parameterMap_.find("lsnDump") !=
					request.parameterMap_.end()) {
					const std::string retStr = request.parameterMap_["lsnDump"];
					if (retStr == "true") {
						lsnDump = true;
					}
				}

				bool notRoleDump = false;
				if (request.parameterMap_.find("notRoleDump") !=
					request.parameterMap_.end()) {
					const std::string retStr =
						request.parameterMap_["notRoleDump"];
					if (retStr == "true") {
						notRoleDump = true;
					}
				}

				uint32_t partitionGroupNo = UINT32_MAX;
				if (request.parameterMap_.find("partitionGroupNo") !=
					request.parameterMap_.end()) {
					partitionGroupNo = static_cast<uint32_t>(atoi(
						request.parameterMap_["partitionGroupNo"].c_str()));
				}

				picojson::value result;
				sysSvc_->getPartitions(result, partitionNo, addressTypeNum,
					isLossOnly, isForce, isSelf, lsnDump, notRoleDump,
					partitionGroupNo);

				if (request.parameterMap_.find("callback") !=
					request.parameterMap_.end()) {
					response.setJson(request.parameterMap_["callback"], result);
				}
				else {
					response.setJson(result);
				}
			}
			else {
				response.setBadRequestError();
				return;
			}
		}
		else if (request.pathElements_.size() == 2 &&
				 request.pathElements_[1] == "log") {
			if (request.method_ != EBB_GET) {
				response.setMethodError();
				return;
			}

			std::string serachStr, searchStr2, ignoreStr;
			if (request.parameterMap_.find("searchStr") !=
				request.parameterMap_.end()) {
				serachStr = request.parameterMap_["searchStr"];
			}
			if (request.parameterMap_.find("searchStr2") !=
				request.parameterMap_.end()) {
				searchStr2 = request.parameterMap_["searchStr2"];
			}
			if (request.parameterMap_.find("ignoreStr") !=
				request.parameterMap_.end()) {
				ignoreStr = request.parameterMap_["ignoreStr"];
			}
			uint32_t length = UINT32_MAX;
			if (request.parameterMap_.find("length") !=
				request.parameterMap_.end()) {
				length = atoi(request.parameterMap_["length"].c_str());
			}
			picojson::value result;
			sysSvc_->getLogs(result, serachStr, searchStr2, ignoreStr, length);

			if (request.parameterMap_.find("callback") !=
				request.parameterMap_.end()) {
				response.setJson(request.parameterMap_["callback"], result);
			}
			else {
				response.setJson(result);
			}
		}
		else if (request.pathElements_.size() >= 2 &&
				 request.pathElements_[1] == "trace") {
			if (request.pathElements_.size() == 2) {
				if (request.method_ == EBB_GET) {
					picojson::value result;
					sysSvc_->getEventLogLevel(result);
					response.setJson(result);
				}
				else if (request.method_ == EBB_POST) {
					std::string category;
					if (request.parameterMap_.find("category") !=
						request.parameterMap_.end()) {
						category = request.parameterMap_["category"];
					}
					std::string level;
					if (request.parameterMap_.find("level") !=
						request.parameterMap_.end()) {
						level = request.parameterMap_["level"];
					}
					bool force = false;
					if (request.parameterMap_.find("force") !=
						request.parameterMap_.end()) {
						const std::string forceStr =
							request.parameterMap_["force"];
						if (forceStr == "true") {
							force = true;
						}
					}
					GS_TRACE_INFO(SYSTEM_SERVICE_DETAIL,
						GS_TRACE_SC_WEB_API_CALLED,
						"Set event log level is called (category="
							<< category.c_str() << ", level=" << level.c_str()
							<< ", force=" << ValueFormatter()(force) << ")");
					if (!sysSvc_->setEventLogLevel(category, level, force)) {
						response.setBadRequestError();
						return;
					}
				}
				else {
					response.setMethodError();
					return;
				}
			}
			else {
				response.setMethodError();
				return;
			}
		}
		else if (request.pathElements_.size() >= 2 &&
				 request.pathElements_[1] == "config") {
			const std::vector<std::string> namePath(
				request.pathElements_.begin() + 2, request.pathElements_.end());

			bool noUnit = false;
			picojson::value paramValueStorage;
			const picojson::value *paramValue = NULL;
			if (request.method_ == EBB_POST) {
				WebAPIRequest::ParameterMap::const_iterator it =
					request.parameterMap_.find("paramValue");
				if (it != request.parameterMap_.end()) {
					if (request.jsonValue_.get() != NULL) {
						response.setBadRequestError();
						return;
					}

					const std::string valueStr = it->second;
					const std::string err =
						JsonUtils::parseAll(paramValueStorage, valueStr.c_str(),
							valueStr.c_str() + valueStr.size());
					if (!err.empty()) {
						response.setBadRequestError();
						return;
					}
					paramValue = &paramValueStorage;
				}
				else if (request.jsonValue_.get() == NULL) {
					response.setBadRequestError();
					return;
				}
				else {
					paramValue = request.jsonValue_.get();
				}
			}
			else if (request.method_ == EBB_GET) {
				WebAPIRequest::ParameterMap::const_iterator it =
					request.parameterMap_.find("noUnit");
				if (it != request.parameterMap_.end() && it->second == "true") {
					noUnit = true;
				}
			}
			else {
				response.setMethodError();
				return;
			}

			picojson::value result;
			if (!sysSvc_->getOrSetConfig(
					namePath, result, paramValue, noUnit)) {
				response.setBadRequestError();
			}

			if (!result.is<picojson::null>()) {
				response.setJson(result);
			}
		}
		else if (request.pathElements_.size() == 2 &&
				 request.pathElements_[1] == "checkpoint") {
			if (request.method_ != EBB_POST) {
				response.setMethodError();
				return;
			}

			sysSvc_->checkpointNode(eventSource);
		}
		else if (request.pathElements_.size() >= 2 &&
				 request.pathElements_[1] == "memory") {
			if (request.method_ != EBB_GET) {
				response.setMethodError();
				return;
			}

			const char8_t *prefix = "";
			{
				WebAPIRequest::ParameterMap::iterator it =
					request.parameterMap_.find("prefix");
				if (it != request.parameterMap_.end()) {
					prefix = it->second.c_str();
				}
			}

			const char8_t *type = "totalSize";
			{
				WebAPIRequest::ParameterMap::iterator it =
					request.parameterMap_.find("type");
				if (it != request.parameterMap_.end()) {
					type = it->second.c_str();
				}
			}

			int64_t minSize = 1;
			{
				WebAPIRequest::ParameterMap::iterator it =
					request.parameterMap_.find("min");
				if (it != request.parameterMap_.end()) {
					minSize =
						util::LexicalConverter<int64_t>()(it->second.c_str());
				}
			}

			picojson::value result;
			sysSvc_->getMemoryStats(result, prefix, type, minSize);

			if (request.parameterMap_.find("callback") !=
				request.parameterMap_.end()) {
				response.setJson(request.parameterMap_["callback"], result);
			}
			else {
				response.setJson(result);
			}
		}
		else if (request.pathElements_.size() == 2 &&
				 request.pathElements_[1] == "event") {
			if (request.method_ != EBB_GET) {
				response.setMethodError();
				return;
			}

			bool reset = false;
			if (request.parameterMap_.find("reset") !=
				request.parameterMap_.end()) {
				const std::string resetStr = request.parameterMap_["reset"];
				if (resetStr == "true") {
					reset = true;
					if (request.method_ != EBB_POST) {
						response.setBadRequestError();
						return;
					}
				}
				else if (resetStr != "false") {
					response.setBadRequestError();
					return;
				}
			}

			picojson::value result;
			if (sysSvc_->getEventStats(result, reset)) {
				if (request.parameterMap_.find("callback") !=
					request.parameterMap_.end()) {
					response.setJson(request.parameterMap_["callback"], result);
				}
				else {
					response.setJson(result);
				}
			}
			else {
				response.setBadRequestError();
				return;
			}
		}
		else if (request.pathElements_.size() == 2 &&
				 request.pathElements_[1] == "dump") {
			if (request.method_ != EBB_GET) {
				response.setMethodError();
				return;
			}
			std::cout << clsMgr_->dump();
			{
				std::cout << pt_->dumpPartitions(
					alloc_, PartitionTable::PT_CURRENT_OB);
				std::cout << pt_->dumpPartitions(
					alloc_, PartitionTable::PT_NEXT_OB);
				std::cout << pt_->dumpPartitions(
					alloc_, PartitionTable::PT_CURRENT_GOAL);
			}
			std::cout << pt_->dumpDatas(true);

			if (request.parameterMap_.find("trace") !=
				request.parameterMap_.end()) {
				const std::string resetStr = request.parameterMap_["trace"];
				if (resetStr == "true") {
					GS_TRACE_INFO(CLUSTER_OPERATION, GS_TRACE_CS_CLUSTER_STATUS,
						clsMgr_->dump());
					{
						GS_TRACE_INFO(CLUSTER_OPERATION,
							GS_TRACE_CS_CLUSTER_STATUS,
							pt_->dumpPartitions(
								alloc_, PartitionTable::PT_CURRENT_OB));
						GS_TRACE_INFO(CLUSTER_OPERATION,
							GS_TRACE_CS_CLUSTER_STATUS,
							pt_->dumpPartitions(
								alloc_, PartitionTable::PT_NEXT_OB));
						GS_TRACE_INFO(CLUSTER_OPERATION,
							GS_TRACE_CS_CLUSTER_STATUS,
							pt_->dumpPartitions(
								alloc_, PartitionTable::PT_CURRENT_GOAL));
					}
					GS_TRACE_INFO(CLUSTER_OPERATION, GS_TRACE_CS_CLUSTER_STATUS,
						pt_->dumpDatas(true));
				}
			}
		}
		else if (request.pathElements_.size() == 2 &&
				 request.pathElements_[1] == "profs") {
			if (request.method_ != EBB_GET) {
				response.setMethodError();
				return;
			}
		}
		else {
			response.setBadRequestError();
			return;
		}
	}
	else if (request.pathElements_.size() >= 2 &&
			 request.pathElements_[0] == "cluster") {
		picojson::value result;
		picojson::object &tmp = setJsonObject(result);

		NodeId masterNodeId = pt_->getMaster();

		if (masterNodeId != 0) {
			picojson::object master;
			std::string commands = "[WEB API] ";
			for (size_t pos = 0; pos < request.pathElements_.size(); pos++) {
				commands += "/";
				commands += request.pathElements_[pos];
			}
			if (masterNodeId > 0) {
				getServiceAddress(pt_, masterNodeId, master, SYSTEM_SERVICE);
				tmp["master"] = picojson::value(master);
				GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_BAD_STATUS,
					commands << " is master only command, "
							 << result.serialize());
			}
			else {
				tmp["master"] = picojson::value("undef");
				GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_BAD_STATUS,
					commands << " is master only command, but current status "
								"is submaster.");
			}
			response.setJson(result);
			response.setBadRequestError();
			return;
		}
		if (request.pathElements_.size() == 2 &&
			request.pathElements_[1] == "stop") {
			if (request.method_ != EBB_POST) {
				response.setMethodError();
				return;
			}
			sysSvc_->shutdownCluster(eventSource, alloc_);
		}
		else {
			response.setBadRequestError();
			return;
		}
	}
	else {
		response.setBadRequestError();
		return;
	}
}

/*!
	@brief Reads socket
*/
bool SystemService::ListenerSocketHandler::readOnce(
	util::Socket &socket, util::NormalXArray<char8_t> &buffer) {
	const size_t unitSize = 1024;

	if (buffer.size() + unitSize > MAX_REQUEST_SIZE) {
		GS_THROW_USER_ERROR(GS_ERROR_SC_TOO_LARGE_REQUEST,
			"Too large web API request (limit=" << MAX_REQUEST_SIZE << ")");
	}
	buffer.resize(buffer.size() + unitSize);
	const int64_t readSize =
		socket.receive(&(buffer.end() - unitSize)[0], unitSize);
	if (readSize <= 0) {
		return false;
	}
	buffer.resize(buffer.size() - unitSize + static_cast<size_t>(readSize));
	return true;
}

SystemService::WebAPIRequest::WebAPIRequest() : method_(0), minorVersion_(0) {}

/*!
	@brief Sets input to WebAPIRequest object
*/
bool SystemService::WebAPIRequest::set(util::NormalXArray<char8_t> &input) {
	method_ = 0;
	pathElements_.clear();
	parameterMap_.clear();
	headerValues_.clear();
	lastHeaderField_.clear();
	user_.clear();
	password_.clear();
	minorVersion_ = 0;
	jsonValue_.reset();
	jsonError_.clear();

	input.push_back('\0');
	bool completed = false;

	do {
		const char8_t *const separator = "\r\n\r\n";
		const char8_t *headerEnd = strstr(input.data(), separator);
		if (headerEnd == NULL) {
			break;
		}
		const char8_t *const body = headerEnd + strlen(separator);

		ebb_request_parser parser;
		ebb_request request;

		ebb_request_parser_init(&parser);
		ebb_request_init(&request);

		parser.data = &request;
		request.data = this;

		request.on_path = acceptPath;
		request.on_query_string = acceptQueryString;
		request.on_header_field = acceptHeaderField;
		request.on_header_value = acceptHeaderValue;
		parser.new_request = getParserRequest;

		const size_t inputSize = strlen(input.data());
		ebb_request_parser_execute(&parser, input.data(), inputSize);
		if (parser.current_request != NULL) {
			break;
		}

		method_ = request.method;
		minorVersion_ = request.version_minor;

		std::map<std::string, std::string>::iterator it =
			headerValues_.find("Content-Type");
		if (it != headerValues_.end()) {
			const char8_t *bodyEnd = input.data() + inputSize;

			if (it->second.find("application/x-www-form-urlencoded") == 0 ||
				it->second.find("application/www-form-urlencoded") == 0) {
				acceptQueryString(body, bodyEnd);
			}
			else if (it->second.find("application/json") == 0) {
				jsonValue_.reset(UTIL_NEW picojson::value());
				jsonError_ = JsonUtils::parseAll(*jsonValue_, body, bodyEnd);

				if (!jsonError_.empty()) {
					jsonValue_.reset();
				}
			}
		}
		completed = true;
	} while (false);

	input.pop_back();
	return completed;
}

/*!
	@brief Accepts a query string
*/
void SystemService::WebAPIRequest::acceptQueryString(
	const char8_t *begin, const char8_t *end) {
	for (const char8_t *entry = begin; entry != end;) {
		const char8_t *const entryEnd = std::find(entry, end, '&');
		if (entryEnd == entry) {
			entry++;
			continue;
		}
		const char8_t *const nameEnd = std::find(entry, entryEnd, '=');

		util::NormalIStringStream nameIn(std::string(entry, nameEnd));
		util::NormalIStringStream valueIn(std::string(
			(nameEnd == entryEnd ? entryEnd : nameEnd + 1), entryEnd));

		util::NormalOStringStream nameOut;
		util::NormalOStringStream valueOut;
		util::URLConverter::decode(nameOut, nameIn);
		util::URLConverter::decode(valueOut, valueIn);

		parameterMap_.insert(std::make_pair(nameOut.str(), valueOut.str()));
		entry = entryEnd;
	}
}

/*!
	@brief Accepts a path string
*/
void SystemService::WebAPIRequest::acceptPath(
	ebb_request *request, const char8_t *at, size_t length) {
	WebAPIRequest &base = *static_cast<WebAPIRequest *>(request->data);

	const char8_t *const end = at + length;
	for (const char8_t *entry = at; entry != end;) {
		const char8_t *const elementEnd = std::find(entry, end, '/');
		if (elementEnd == entry) {
			entry++;
			continue;
		}

		util::NormalIStringStream elementIn(std::string(entry, elementEnd));

		util::NormalOStringStream elementOut;
		util::URLConverter::decode(elementOut, elementIn);

		base.pathElements_.push_back(elementOut.str());
		entry = elementEnd;
	}
}

/*!
	@brief Accepts a query string
*/
void SystemService::WebAPIRequest::acceptQueryString(
	ebb_request *request, const char8_t *at, size_t length) {
	WebAPIRequest &base = *static_cast<WebAPIRequest *>(request->data);
	base.acceptQueryString(at, at + length);
}

/*!
	@brief Accepts a header field
*/
void SystemService::WebAPIRequest::acceptHeaderField(
	ebb_request *request, const char8_t *at, size_t length, int) {
	WebAPIRequest &base = *static_cast<WebAPIRequest *>(request->data);
	base.lastHeaderField_.assign(at, length);
}

/*!
	@brief Accepts a header value
*/
void SystemService::WebAPIRequest::acceptHeaderValue(
	ebb_request *request, const char8_t *at, size_t length, int) {
	WebAPIRequest &base = *static_cast<WebAPIRequest *>(request->data);

	const std::string value(at, length);
	base.headerValues_.insert(std::make_pair(base.lastHeaderField_, value));

	if (base.lastHeaderField_ == "Authorization") {
		const char8_t *const head = "Basic ";
		if (value.find(head) == 0) {
			util::NormalIStringStream iss(
				std::string(value.begin() + strlen(head), value.end()));
			util::NormalOStringStream oss;
			util::Base64Converter::decode(oss, iss);

			const std::string pair = oss.str();
			const size_t pos = pair.find(':');
			if (pos != std::string::npos) {
				base.user_.assign(pair.begin(), pair.begin() + pos);
				base.password_.assign(pair.begin() + pos + 1, pair.end());
			}
		}
	}
}

ebb_request *SystemService::WebAPIRequest::getParserRequest(void *data) {
	return static_cast<ebb_request *>(data);
}

SystemService::WebAPIResponse::WebAPIResponse(WebAPIRequest &request)
	: request_(request), statusCode_(200), hasJson_(false) {}

void SystemService::WebAPIResponse::setMethodError() {
	statusCode_ = 405;
}

void SystemService::WebAPIResponse::setAuthError() {
	statusCode_ = 401;
}

void SystemService::WebAPIResponse::setBadRequestError() {
	statusCode_ = 400;
}

void SystemService::WebAPIResponse::setInternalServerError() {
	statusCode_ = 500;
}

void SystemService::WebAPIResponse::setJson(const picojson::value &value) {
	hasJson_ = true;
	body_ = value.serialize();
}

void SystemService::WebAPIResponse::setJson(
	const std::string &callback, const picojson::value &value) {
	hasJson_ = true;
	body_ = callback + "(" + value.serialize() + ")";
}

/*!
	@brief Sets response header
*/
void SystemService::WebAPIResponse::update() {
	util::NormalOStringStream oss;
	oss << "HTTP/1." << request_.minorVersion_ << " " << statusCode_ << "\r\n";
	oss << "WWW-Authenticate: Basic realm=\"Admin only\"\r\n";
	oss << "Connection: close\r\n";


	oss << "Content-Length: " << body_.size() << "\r\n";
	if (hasJson_) {
		oss << "Content-Type: application/json\r\n";
	}
	oss << "\r\n";
	header_ = oss.str();
}

SystemService::SystemConfig::SystemConfig(ConfigTable &configTable)
	: sysStatsInterval_(changeTimeSecToMill(OUTPUT_STATS_INTERVAL_SEC)),
	  traceMode_(static_cast<TraceMode>(
		  configTable.get<int32_t>(CONFIG_TABLE_SYS_TRACE_MODE))) {
	setUpConfigHandler(configTable);
}

void SystemService::SystemConfig::setUpConfigHandler(ConfigTable &configTable) {
	configTable.setParamHandler(CONFIG_TABLE_SYS_TRACE_MODE, *this);
}

void SystemService::SystemConfig::operator()(
	ConfigTable::ParamId id, const ParamValue &value) {
	switch (id) {
	case CONFIG_TABLE_SYS_TRACE_MODE:
		traceMode_ = static_cast<TraceMode>(value.get<int32_t>());
		break;
	}
}

SystemService::ConfigSetUpHandler SystemService::configSetUpHandler_;

void SystemService::ConfigSetUpHandler::operator()(ConfigTable &config) {
	CONFIG_TABLE_RESOLVE_GROUP(config, CONFIG_TABLE_SYS, "system");

	CONFIG_TABLE_ADD_SERVICE_ADDRESS_PARAMS(config, SYS, 10040);

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_SYS_EVENT_LOG_PATH, STRING)
		.setDefault("log");

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_SYS_TRACE_MODE, INT32)
		.setExtendedType(ConfigTable::EXTENDED_TYPE_ENUM)
		.addEnum(TRACE_MODE_SIMPLE, "SIMPLE")
		.addEnum(TRACE_MODE_SIMPLE_DETAIL, "SIMPLE_DETAIL")
		.addEnum(TRACE_MODE_FULL, "FULL")
		.deprecate()
		.setDefault(TRACE_MODE_SIMPLE_DETAIL);
}

SystemService::StatSetUpHandler SystemService::statSetUpHandler_;

#define STAT_ADD(id) STAT_TABLE_ADD_PARAM(stat, parentId, id)
#define STAT_ADD_SUB(id) STAT_TABLE_ADD_PARAM_SUB(stat, parentId, id)
#define STAT_ADD_SUB_SUB(id) STAT_TABLE_ADD_PARAM_SUB_SUB(stat, parentId, id)

void SystemService::StatSetUpHandler::operator()(StatTable &stat) {
	StatTable::ParamId parentId;

	parentId = STAT_TABLE_ROOT;
	STAT_ADD(STAT_TABLE_ROOT_CURRENT_TIME);
	STAT_ADD(STAT_TABLE_ROOT_VERSION);
	STAT_ADD(STAT_TABLE_ROOT_SYNC);
	STAT_ADD(STAT_TABLE_ROOT_PG_STORE_MEMORY_LIMIT);

	stat.resolveGroup(parentId, STAT_TABLE_PERF, "performance");

	parentId = STAT_TABLE_PERF;
	STAT_ADD(STAT_TABLE_PERF_CURRENT_TIME);
	STAT_ADD(STAT_TABLE_PERF_PROCESS_MEMORY);
	STAT_ADD(STAT_TABLE_PERF_PEAK_PROCESS_MEMORY);
	STAT_ADD_SUB_SUB(
		STAT_TABLE_PERF_TXN_DETAIL_DISABLE_TIMEOUT_CHECK_PARTITION_COUNT);
	stat.resolveGroup(parentId, STAT_TABLE_PERF_TXN_EE, "eventEngine");
	stat.resolveGroup(parentId, STAT_TABLE_PERF_MEM, "memoryDetail");

	parentId = STAT_TABLE_PERF_TXN_EE;
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_TXN_EE_CLUSTER);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_TXN_EE_SYNC);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_TXN_EE_TRANSACTION);

	parentId = STAT_TABLE_PERF_MEM;
	STAT_ADD_SUB(STAT_TABLE_PERF_MEM_ALL_TOTAL);
	STAT_ADD_SUB(STAT_TABLE_PERF_MEM_ALL_CACHED);
	STAT_ADD_SUB(STAT_TABLE_PERF_MEM_PROCESS_MEMORY_GAP);
	stat.resolveGroup(parentId, STAT_TABLE_PERF_MEM_DS, "store");
	stat.resolveGroup(parentId, STAT_TABLE_PERF_MEM_WORK, "work");

	parentId = STAT_TABLE_PERF_MEM_DS;
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_DS_STORE_TOTAL);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_DS_STORE_CACHED);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_DS_LOG_TOTAL);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_DS_LOG_CACHED);

	parentId = STAT_TABLE_PERF_MEM_WORK;
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_CHECKPOINT_TOTAL);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_CHECKPOINT_CACHED);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_CLUSTER_TOTAL);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_CLUSTER_CACHED);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_MAIN_TOTAL);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_MAIN_CACHED);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_SYNC_TOTAL);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_SYNC_CACHED);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_SYSTEM_TOTAL);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_SYSTEM_CACHED);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_TRANSACTION_MESSAGE_TOTAL);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_TRANSACTION_MESSAGE_CACHED);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_TRANSACTION_RESULT_TOTAL);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_TRANSACTION_RESULT_CACHED);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_TRANSACTION_WORK_TOTAL);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_TRANSACTION_WORK_CACHED);
}

bool SystemService::StatUpdator::operator()(StatTable &stat) {
	SystemService &svc = *service_;

	const bool perfSelected =
		stat.getDisplayOption(STAT_TABLE_DISPLAY_SELECT_PERF);

	if (stat.getDisplayOption(STAT_TABLE_DISPLAY_WEB_ONLY)) {
		const util::DateTime now = util::DateTime::now(false);

		{
			util::NormalOStringStream oss;
			now.format(oss, true);
			stat.set(STAT_TABLE_ROOT_CURRENT_TIME, oss.str());
		}

		stat.set(STAT_TABLE_ROOT_VERSION, svc.gsVersion_);

		if (perfSelected) {
			stat.set(STAT_TABLE_PERF_CURRENT_TIME, now.getUnixTime());
		}
	}

	if (perfSelected &&
		stat.getDisplayOption(STAT_TABLE_DISPLAY_OPTIONAL_TXN)) {
		stat.set(
			STAT_TABLE_PERF_TXN_DETAIL_DISABLE_TIMEOUT_CHECK_PARTITION_COUNT,
			svc.clsSvc_->getStats().getNotTransactionTimeoutCheckCount(
				svc.pt_));

		picojson::array eeArray;
		EventEngine *eeList[] = {
			svc.clsSvc_->getEE(), svc.syncSvc_->getEE(), svc.txnSvc_->getEE()
		};
		const StatTableParamId paramList[] = {STAT_TABLE_PERF_TXN_EE_CLUSTER,
			STAT_TABLE_PERF_TXN_EE_SYNC, STAT_TABLE_PERF_TXN_EE_TRANSACTION
		};
		for (size_t i = 0; i < sizeof(eeList) / sizeof(*eeList); i++) {
			const uint32_t concurrency =
				static_cast<uint32_t>(svc.clsMgr_->getConcurrency());
			picojson::value eeInfo;
			if (paramList[i] == STAT_TABLE_PERF_TXN_EE) {
				eeInfo = picojson::value(picojson::array());
			}
			EventEngine::Stats eeStats;
			eeList[i]->getStats(eeStats);
			for (PartitionGroupId pgId = 0; pgId < concurrency; pgId++) {
				const int64_t activeQueueSize = eeStats.get(
					EventEngine::Stats::EVENT_ACTIVE_QUEUE_SIZE_CURRENT);
				picojson::object eeInfoObj;
				eeInfoObj["activeQueueSize"] =
					picojson::value(static_cast<double>(activeQueueSize));

				const int64_t activeQueueSizeMax = eeStats.get(
					EventEngine::Stats::EVENT_ACTIVE_QUEUE_SIZE_MAX);
				eeInfoObj["activeQueueSizeMax"] =
					picojson::value(static_cast<double>(activeQueueSizeMax));

				int64_t tmp;
				tmp = eeStats.get(
					EventEngine::Stats::EVENT_ACTIVE_QUEUE_SIZE_MAX);
				eeInfoObj["activeQueueSizeMax"] =
					picojson::value(static_cast<double>(activeQueueSizeMax));

				tmp = eeStats.get(EventEngine::Stats::EVENT_ACTIVE_ADD_COUNT);
				eeInfoObj["acvieAddCount"] =
					picojson::value(static_cast<double>(tmp));

				tmp = eeStats.get(EventEngine::Stats::EVENT_PENDING_ADD_COUNT);
				eeInfoObj["pendingAddCount"] =
					picojson::value(static_cast<double>(tmp));

				tmp = eeStats.get(EventEngine::Stats::EVENT_PENDING_ADD_COUNT);
				eeInfoObj["pendingAddCount"] =
					picojson::value(static_cast<double>(tmp));

				tmp = eeStats.get(
					EventEngine::Stats::EVENT_ACTIVE_QUEUE_SIZE_CURRENT);
				eeInfoObj["activeQueueSizeCurrent"] =
					picojson::value(static_cast<double>(tmp));

				tmp = eeStats.get(
					EventEngine::Stats::EVENT_ACTIVE_BUFFER_SIZE_CURRENT);
				eeInfoObj["activeBufferSizeCurrent"] =
					picojson::value(static_cast<double>(tmp));

				tmp = eeStats.get(
					EventEngine::Stats::EVENT_ACTIVE_BUFFER_SIZE_MAX);
				eeInfoObj["activeBufferSizeMax"] =
					picojson::value(static_cast<double>(tmp));

				tmp = eeStats.get(
					EventEngine::Stats::EVENT_PENDING_QUEUE_SIZE_CURRENT);
				eeInfoObj["pendingQueueSizeCurrent"] =
					picojson::value(static_cast<double>(tmp));

				tmp = eeStats.get(
					EventEngine::Stats::EVENT_PENDING_QUEUE_SIZE_MAX);
				eeInfoObj["pendingQueueSizeMax"] =
					picojson::value(static_cast<double>(tmp));

				if (eeInfo.is<picojson::array>()) {
					eeInfo.get<picojson::array>().push_back(
						picojson::value(eeInfoObj));
				}
				else {
					eeInfo = picojson::value(eeInfoObj);
				}
			}
			stat.set(paramList[i], eeInfo);
		}
	}

	do {
		if (!perfSelected) {
			break;
		}

		size_t processMemory = 0;
		size_t peakProcessMemory = 0;
		try {
			util::MemoryStatus memStatus = util::MemoryStatus::getStatus();
			processMemory = memStatus.getLastUsage();
			peakProcessMemory = memStatus.getPeakUsage();
		}
		catch (std::exception &e) {
			UTIL_TRACE_EXCEPTION_INFO(SYSTEM_SERVICE, e,
				"Failed to get memory status on updating stats (reason="
					<< GS_EXCEPTION_MESSAGE(e) << ")");
		}

		stat.set(STAT_TABLE_PERF_PROCESS_MEMORY, processMemory);

		stat.set(STAT_TABLE_PERF_PEAK_PROCESS_MEMORY, peakProcessMemory);

		if (!stat.getDisplayOption(STAT_TABLE_DISPLAY_WEB_ONLY) ||
			!stat.getDisplayOption(STAT_TABLE_DISPLAY_OPTIONAL_MEM)) {
			break;
		}

		static const int32_t statIdList[] = {
			STAT_TABLE_PERF_MEM_ALL_TOTAL, STAT_TABLE_PERF_MEM_DS_STORE_TOTAL,
			STAT_TABLE_PERF_MEM_DS_LOG_TOTAL,
			STAT_TABLE_PERF_MEM_WORK_CHECKPOINT_TOTAL,
			STAT_TABLE_PERF_MEM_WORK_CLUSTER_TOTAL,
			STAT_TABLE_PERF_MEM_WORK_MAIN_TOTAL,
			STAT_TABLE_PERF_MEM_WORK_SYNC_TOTAL,
			STAT_TABLE_PERF_MEM_WORK_SYSTEM_TOTAL,
			STAT_TABLE_PERF_MEM_WORK_TRANSACTION_MESSAGE_TOTAL,
			STAT_TABLE_PERF_MEM_WORK_TRANSACTION_RESULT_TOTAL,
			STAT_TABLE_PERF_MEM_WORK_TRANSACTION_WORK_TOTAL,
		};
		static const size_t listSize = sizeof(statIdList) / sizeof(*statIdList);
		static const util::AllocatorGroupId allocIdList[listSize] = {
			ALLOCATOR_GROUP_ROOT, ALLOCATOR_GROUP_STORE, ALLOCATOR_GROUP_LOG,
			ALLOCATOR_GROUP_CP, ALLOCATOR_GROUP_CS, ALLOCATOR_GROUP_MAIN,
			ALLOCATOR_GROUP_SYNC, ALLOCATOR_GROUP_SYS,
			ALLOCATOR_GROUP_TXN_MESSAGE, ALLOCATOR_GROUP_TXN_RESULT,
			ALLOCATOR_GROUP_TXN_WORK,
		};

		util::AllocatorManager &allocMgr =
			util::AllocatorManager::getDefaultInstance();

		typedef std::vector<util::AllocatorGroupId> IdList;
		IdList idList;
		idList.push_back(ALLOCATOR_GROUP_ROOT);
		allocMgr.listSubGroup(
			ALLOCATOR_GROUP_ROOT, std::back_inserter(idList), true);

		std::vector<util::AllocatorStats> statsList;
		statsList.resize(idList.size());

		allocMgr.getGroupStats(&idList[0], idList.size(), &statsList[0]);
		for (IdList::iterator it = idList.begin(); it != idList.end(); ++it) {
			const size_t index = static_cast<size_t>(
				std::find(allocIdList, allocIdList + listSize, *it) -
				allocIdList);
			if (index == listSize) {
				continue;
			}

			const int32_t paramId = statIdList[index];
			const util::AllocatorStats &allocStats =
				statsList[it - idList.begin()];

			stat.set(paramId,
				allocStats.values_[util::AllocatorStats::STAT_TOTAL_SIZE]);
			stat.set(paramId + 1,
				allocStats.values_[util::AllocatorStats::STAT_CACHE_SIZE]);

			if (paramId == STAT_TABLE_PERF_MEM_ALL_TOTAL) {
				stat.set(STAT_TABLE_PERF_MEM_PROCESS_MEMORY_GAP,
					static_cast<int64_t>(processMemory) -
						allocStats
							.values_[util::AllocatorStats::STAT_TOTAL_SIZE]);
			}
		}
	} while (false);

	if (stat.getDisplayOption(STAT_TABLE_DISPLAY_OPTIONAL_SYNC)) {
		picojson::value result;
		svc.getSyncStats(result);
		stat.set(STAT_TABLE_ROOT_SYNC, result);
	}

	if (stat.getDisplayOption(STAT_TABLE_DISPLAY_OPTIONAL_PGLIMIT)) {
		picojson::value result;
		svc.getPGStoreMemoryLimitStats(result);
		stat.set(STAT_TABLE_ROOT_PG_STORE_MEMORY_LIMIT, result);
	}


	return true;
}

void ServiceThreadErrorHandler::operator()(
	EventContext &ec, std::exception &e) {
	clsSvc_->setError(ec, &e);
}

void ServiceThreadErrorHandler::initialize(const ManagerSet &mgrSet) {
	clsSvc_ = mgrSet.clsSvc_;
}

void OutputStatsHandler::initialize(ManagerSet &mgrSet) {
	sysSvc_ = mgrSet.sysSvc_;
}

