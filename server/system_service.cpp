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
#include "chunk_buffer.h"  
#include "data_store_v4.h"
#include "interchangeable.h"
#include "ebb_request_parser.h"
#include "json.h"
#include "log_manager.h"
#include "picojson.h"
#include "sync_manager.h"
#include "database_manager.h"
using util::ValueFormatter;


#include "sql_job_manager.h"
#include "sql_execution.h"
#include "sql_service.h"
#include "sql_processor.h"
#include "sql_compiler.h"

#ifndef _WIN32
#include <signal.h>  
#endif


std::string getPathElements(std::vector<std::string>& pathElements, int32_t pos) {
	if (pathElements.size() >= pos) {
		return "";
	}
	return pathElements[pos];
}

#define AUDIT_TRACE_USER_TYPE() 

#define AUDIT_TRACE_INFO_INTERNAL_COMMAND(tracer) { \
}

#define AUDIT_TRACE_INFO_COMMAND() { \
}

#define AUDIT_TRACE_ERROR_CODE_INTERNAL_COMMAND(tracer) { \
}

#define AUDIT_TRACE_ERROR_CODED_COMMAND() { \
}

#define AUDIT_TRACE_ERROR_INTERNAL_COMMAND(tracer) { \
}

#define AUDIT_TRACE_ERROR_COMMAND() { \
}

typedef ObjectManagerV4 OCManager;

UTIL_TRACER_DECLARE(SYSTEM_SERVICE);
UTIL_TRACER_DECLARE(SYSTEM_SERVICE_DETAIL);
UTIL_TRACER_DECLARE(DISTRIBUTED_FRAMEWORK);

UTIL_TRACER_DECLARE(CLUSTER_DETAIL);


AUDIT_TRACER_DECLARE(AUDIT_SYSTEM);
AUDIT_TRACER_DECLARE(AUDIT_STAT);


static void getServiceAddress(
		PartitionTable *pt, NodeId nodeId, picojson::object &result,
		const ServiceTypeInfo &addressType) {
	try {
		pt->getServiceAddress(nodeId, result, addressType);
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION_WARNING(SYSTEM_SERVICE, e, "");
	}
}





SystemService::SystemService(
		ConfigTable &config, const EventEngine::Config &eeConfig,
		EventEngine::Source source, const char8_t *name,
		const char8_t *diffFilePath,
		ServiceThreadErrorHandler &serviceThreadErrorHandler) :
		ee_(createEEConfig(config, eeConfig), source, name),
		gsVersion_(config.get<const char8_t*>(CONFIG_TABLE_DEV_SIMPLE_VERSION)),
		serviceThreadErrorHandler_(serviceThreadErrorHandler),
		clsSvc_(NULL),
		clsMgr_(NULL),
		syncSvc_(NULL),
		pt_(NULL),
		cpSvc_(NULL),
		partitionList_(NULL),
		txnSvc_(NULL),
		txnMgr_(NULL),
		recoveryMgr_(NULL),
		sqlSvc_(NULL),
		fixedSizeAlloc_(NULL),
		varSizeAlloc_(NULL),
		multicastAddress_(
		config.get<const char8_t *>(CONFIG_TABLE_TXN_NOTIFICATION_ADDRESS)),
		multicastPort_(config.getUInt16(CONFIG_TABLE_TXN_NOTIFICATION_PORT)),
		outputDiffFileName_(diffFilePath),
		webapiServerThread_(eeConfig.plainIOMode_.serverAcceptable_, false),
		secureWebapiServerThread_(eeConfig.secureIOMode_.serverAcceptable_, true),
		sysConfig_(config),
		initialized_(false),
		config_(config),
		baseStats_(NULL),
		socketFactory_(source.socketFactory_),
		secureSocketFactories_(source.secureSocketFactories_)
{
	statUpdator_.service_ = this;
	try {
		ee_.setHandler(SYS_EVENT_OUTPUT_STATS, outputStatsHandler_);
		Event::Source eventSource(source);
		Event outputStatsEvent(eventSource, SYS_EVENT_OUTPUT_STATS, 0);
		ee_.addPeriodicTimer(
			outputStatsEvent, OUTPUT_STATS_INTERVAL_SEC * 1000);

		ee_.setThreadErrorHandler(serviceThreadErrorHandler_);

		const char8_t *categories[] = {"HASH_MAP", "BASE_CONTAINER",
			"CHECKPOINT_SERVICE_DETAIL", "SYSTEM_SERVICE_DETAIL", "REPLICATION",
			"SESSION_DETAIL", "TRANSACTION_DETAIL", "TIMEOUT_DETAIL",
			"RECOVERY_MANAGER_DETAIL", "CHUNK_MANAGER_DETAIL",
			"CHUNK_MANAGER_IO_DETAIL", NULL};

		for (size_t i = 0; categories[i] != NULL; i++) {
			unchangeableTraceCategories_.insert(categories[i]);
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
		if (!initialized_) {
			GS_THROW_USER_ERROR(GS_ERROR_SC_SERVICE_NOT_INITIALIZED, "");
		}
		ee_.start();

		webapiServerThread_.tryStart();
		secureWebapiServerThread_.tryStart();

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
	secureWebapiServerThread_.shutdown();
}

/*!
	@brief Waits for shutdown SystemService
*/
void SystemService::waitForShutdown() {
	ee_.waitForShutdown();

	webapiServerThread_.waitForShutdown();
	secureWebapiServerThread_.waitForShutdown();
}

/*!
	@brief Sets EventEngine config
*/
EventEngine::Config SystemService::createEEConfig(
		const ConfigTable &config, const EventEngine::Config &src) {
	EventEngine::Config eeConfig = src;






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
	partitionList_ = mgrSet.partitionList_;
	txnSvc_ = mgrSet.txnSvc_;
	txnMgr_ = mgrSet.txnMgr_;
	recoveryMgr_ = mgrSet.recoveryMgr_;
	fixedSizeAlloc_ = mgrSet.fixedSizeAlloc_;
	varSizeAlloc_ = mgrSet.varSizeAlloc_;
	syncMgr_ = mgrSet.syncMgr_;
	sqlSvc_ = mgrSet.sqlSvc_;

	baseStats_ = mgrSet.stats_;

	mgrSet.stats_->addUpdator(&statUpdator_);

	webapiServerThread_.initialize(mgrSet);
	secureWebapiServerThread_.initialize(mgrSet);

	outputStatsHandler_.initialize(mgrSet);
	serviceThreadErrorHandler_.initialize(mgrSet);

	if (secureWebapiServerThread_.isEnabled()) {
		const uint16_t sslPort = mgrSet.config_->getUInt16(
				CONFIG_TABLE_SYS_SERVICE_SSL_PORT);
		pt_->setSSLPortNo(SELF_NODEID, static_cast<int32_t>(sslPort));
	}


	initialized_ = true;
}

/*!
	@brief Handles clearUserCache command
*/
bool SystemService::clearUserCache(const std::string &name,
	bool isDatabase, picojson::value &result) {
	try {
		GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
			"Clear user cache called (Name="
				<< name << ", isDatabase=" << isDatabase << ")");

			UserCache *uc = txnSvc_->userCache_;
			if (uc == NULL) {
				return true;
			}
			GlobalVariableSizeAllocator& varAlloc = uc->getAllocator();

		try {
			UserString nameUS(varAlloc);
			nameUS.append(name.c_str());
			
			txnSvc_->userCache_->clear(nameUS, isDatabase);
		}
		catch (UserException &e) {
			picojson::object errorInfo;
			int32_t errorNo = 0;
			std::string reason;

			reason = "other reason, see event log";
			errorNo = WEBAPI_CS_OTHER_REASON;

			errorInfo["errorStatus"] =
				picojson::value(static_cast<double>(errorNo));
			errorInfo["reason"] = picojson::value(reason);

			result = picojson::value(errorInfo);

			UTIL_TRACE_EXCEPTION_INFO(SYSTEM_SERVICE, e,
				"Join cluster failed (reason="
					<< reason << ", detail=" << GS_EXCEPTION_MESSAGE(e) << ")");

			return false;
		}

		return true;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "Clear user cache failed"));
	}
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
			clsMgr_->setJoinClusterInfo(joinClusterInfo);
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
						 clsMgr_->getConfig().getSetClusterName() + ".";
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
			clsMgr_->setLeaveClusterInfo(leaveClusterInfo);
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
	@brief Handles increaseCluster command
*/
void SystemService::increaseCluster(
	const Event::Source &eventSource, util::StackAllocator &alloc) {
	try {
		GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
			"Increase cluster called");

		ClusterManager::IncreaseClusterInfo increaseClusterInfo(alloc, 0);
		clsSvc_->request(eventSource, CS_INCREASE_CLUSTER,
			CS_HANDLER_PARTITION_ID, clsSvc_->getEE(), increaseClusterInfo);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "Increase cluster failed"));
	}
}

/*!
	@brief Handles decreaseCluster command
*/
bool SystemService::decreaseCluster(const Event::Source &eventSource,
	util::StackAllocator &alloc, picojson::value &result, bool isAutoLeave,
	int32_t leaveNum) {
	try {
		GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
			"Decrease cluster called ("
			"autoLeave="
				<< ValueFormatter()(isAutoLeave) << ")");

		ClusterManager::DecreaseClusterInfo decreaseClusterInfo(
			alloc, 0, pt_, true, isAutoLeave, leaveNum);
		try {
			clsMgr_->setDecreaseClusterInfo(decreaseClusterInfo);
		}
		catch (UserException &e) {
			UTIL_TRACE_EXCEPTION_INFO(SYSTEM_SERVICE, e,
				GS_EXCEPTION_MERGE_MESSAGE(e, "Decrease cluster failed"));
			return false;
		}

		NodeIdList &leaveNodeList =
			decreaseClusterInfo.getLeaveNodeList();
		bool isFound = false;
		picojson::object decreaseInfo;

		for (int32_t pos = 0; pos < static_cast<int32_t>(leaveNodeList.size());
			 pos++) {
			const NodeDescriptor &nd =
				clsSvc_->getEE()->getServerND(leaveNodeList[pos]);
			if (!nd.isEmpty()) {
				util::NormalOStringStream oss;
				oss << nd.getAddress();
				decreaseInfo["leaveAddress"] = picojson::value(oss.str());
				isFound = true;
			}
		}

		if (leaveNodeList.size() == 0 || !isFound) {
			decreaseInfo["leaveAddress"] = picojson::value(std::string("none"));
		}
		result = picojson::value(decreaseInfo);

		decreaseClusterInfo.setPreCheck(false);
		clsSvc_->request(eventSource, CS_DECREASE_CLUSTER,
			CS_HANDLER_PARTITION_ID, clsSvc_->getEE(), decreaseClusterInfo);

		return true;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "Decrease cluster failed"));
	}
}

/*!
	@brief Handles backupNode command
*/
bool SystemService::backupNode(const Event::Source &eventSource,
	const char8_t *backupName, int32_t mode, 
	BackupOption &option,
	picojson::value &result) {
	GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
		"Backup node called (backupName="
			<< backupName
			<< ", mode=" << CheckpointService::checkpointModeToString(mode)
			<< ", logArchive=" << ValueFormatter()(option.logArchive_)
			<< ", logDuplicate=" << ValueFormatter()(option.logDuplicate_)
			<< ", stopOnDuplicateError=" << ValueFormatter()(option.stopOnDuplicateError_)
			<< ", isIncrementalBackup:" << (option.isIncrementalBackup_ ? 1 : 0)
			<< ", incrementalBackupLevel:" << option.incrementalBackupLevel_
			<< ", isCumulativeBackup:" << (option.isCumulativeBackup_ ? 1 : 0) << ")");

	picojson::object errorInfo;
	int32_t errorNo = 0;
	std::string reason;
	try {
		clsMgr_->checkNodeStatus();
	}
	catch (UserException &e) {
		UTIL_TRACE_EXCEPTION_WARNING(SYSTEM_SERVICE, e,
			"Rejected by unacceptable condition (status="
				<< clsMgr_->statusInfo_.getSystemStatus()
				<< ", detail=" << GS_EXCEPTION_MESSAGE(e) << ")");

		util::NormalOStringStream oss;
		oss << "unacceptable condition (status="
			<< clsMgr_->statusInfo_.getSystemStatus() << ")";
		reason = oss.str();
		errorNo = WEBAPI_CP_OTHER_REASON;
		errorInfo["errorStatus"] =
			picojson::value(static_cast<double>(errorNo));
		errorInfo["reason"] = picojson::value(reason);
		result = picojson::value(errorInfo);
		return false;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "Backup node failed"));
	}

	try {
		if (cpSvc_->requestOnlineBackup(eventSource, backupName, mode,
				option, result)) {
			return true;
		}
		else {
			return false;
		}
	}
	catch (UserException &e) {
		UTIL_TRACE_EXCEPTION(SYSTEM_SERVICE, e,
			"Backup node failed (name=" << backupName << ", detail="
										<< GS_EXCEPTION_MESSAGE(e) << ")");

		reason = "backup failed";
		errorNo = WEBAPI_CP_OTHER_REASON;
		errorInfo["errorStatus"] =
			picojson::value(static_cast<double>(errorNo));
		errorInfo["reason"] = picojson::value(reason);
		result = picojson::value(errorInfo);
		return false;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "Backup node failed"));
	}
}

/*!
	@brief Handles archiveLog command
*/
bool SystemService::archiveLog(
		const Event::Source &eventSource,
		const char8_t *backupName, int32_t mode, picojson::value &result) {
	GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
		"Archive log called (backupName="
			<< backupName << ", mode="
			<< CheckpointService::checkpointModeToString(mode) << ")");
	UNUSED_VARIABLE(eventSource);
	UNUSED_VARIABLE(result);
	assert(false);
	return false; 
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
	@brief Handles 
*/
void SystemService::setPeriodicCheckpointFlag(bool flag) {
	GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
		"Set periodic checkpoint flag called");

	try {
		cpSvc_->setPeriodicCheckpointFlag(flag);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "Set periodic checkpoint flag failed"));
	}
}

/*!
	@brief Handles getHosts command
*/
void SystemService::getHosts(
		picojson::value &result, const ServiceTypeInfo &addressType) {
	try {
		GS_TRACE_DEBUG(
			SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED, "Get hosts called");

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

		if (clsSvc_->getNotificationManager().getMode() ==
			NOTIFICATION_MULTICAST)
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
void SystemService::getPartitions(
		util::StackAllocator &alloc,
		picojson::value &result, int32_t partitionNo,
		const ServiceTypeInfo &addressType, bool lossOnly, bool force, bool isSelf,
		bool lsnDump, bool notDumpRole, uint32_t partitionGroupNo,
		bool sqlOwnerDump, bool replication) {
	try {
		GS_TRACE_DEBUG(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
			"Get partitions called");

		picojson::array &partitionList = picojsonUtil::setJsonArray(result);
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
						if ((pt_->isMaster() && ownerNodeId != UNDEF_NODEID) ||
							(pt_->isFollower() && ownerNodeId == 0)) {
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
				if (sqlOwnerDump)
				{
					const NodeId nodeId = pt_->getNewSQLOwner(pId);
					picojson::value& addressValue = partition["sqlOwner"];
					if (nodeId != UNDEF_NODEID) {
						addressValue = picojson::value(picojson::object());
						getServiceAddress(
							pt_, nodeId,
							JsonUtils::as<picojson::object>(addressValue),
							addressType);
					}
				}
				{
					picojson::array& nodeList =
						picojsonUtil::setJsonArray(partition["backup"]);
					util::XArray<NodeId> backupList(alloc);
					pt_->getBackup(pId, backupList);
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
					picojson::array& nodeList =
						picojsonUtil::setJsonArray(partition["catchup"]);
					util::XArray<NodeId> catchupList(alloc);
					pt_->getCatchup(pId, catchupList);
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
						picojson::array& nodeList =
							picojsonUtil::setJsonArray(partition["all"]);
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
				else if (pt_->isCatchup(pId)) {
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
			partition["pId"] = picojson::value(CommonUtility::makeString(oss, pId));
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
					picojson::value(CommonUtility::makeString(oss, partitionGroupNo));
			}

			partitionList.push_back(picojson::value(partition));
		}
	}
	catch (UserException &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "Get partition failed, target node"));
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "Get partition failed"));
	}
}

void SystemService::getGoalPartitions(
		util::StackAllocator &alloc, const ServiceTypeInfo &addressType,
		picojson::value &result) {
	try {
		GS_TRACE_DEBUG(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
			"Get goal partitions called");
		pt_->getGoalPartitions(alloc, addressType, result, PartitionTable::PT_CURRENT_GOAL);
	}
	catch (std::exception &e) {
	}
}

void SystemService::getDatabaseContraint(
		util::StackAllocator& alloc, const ServiceTypeInfo& addressType,
		DatabaseId dbId, picojson::value& result) {
	UNUSED_VARIABLE(alloc);
	UNUSED_VARIABLE(addressType);
	try {
		GS_TRACE_DEBUG(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
			"Get Database constraint called");
		txnSvc_->getManager()->getDatabaseManager().getExecutionConstraintList(dbId, result);
	}
	catch (std::exception& e) {
		UTIL_TRACE_EXCEPTION(SYSTEM_SERVICE, e, "");
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

void SystemService::traceStats() {
	try {
		StatTable stats(*varSizeAlloc_);
		stats.initialize(*baseStats_);
		WebAPIRequest::ParameterMap parameterMap;
		parameterMap["detail"] = "true";
		acceptStatsOption(stats, parameterMap, NULL);
		stats.setDisplayOption(STAT_TABLE_DISPLAY_WEB_ONLY, true);
		stats.setDisplayOption(STAT_TABLE_DISPLAY_SELECT_CS, true);
		stats.setDisplayOption(STAT_TABLE_DISPLAY_SELECT_CP, true);
		stats.setDisplayOption(STAT_TABLE_DISPLAY_SELECT_RM, true);
		stats.setDisplayOption(STAT_TABLE_DISPLAY_SELECT_PERF, true);
		picojson::value result;
		getStats(result, stats);
		std::string data = result.serialize();
		GS_TRACE_INFO(CLUSTER_OPERATION,
				GS_TRACE_CS_CLUSTER_STATUS, result.serialize());
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "Get stat failed"));
	}
}

/*!
	@brief Gets the statistics about synchronization
*/
void SystemService::getSyncStats(
		picojson::value &result, const ServiceTypeInfo &addressType) {
	picojson::object &syncStats = picojsonUtil::setJsonObject(result);

	syncStats["contextCount"] = picojson::value(
		static_cast<double>(syncMgr_->getContextCount()));

	PartitionId currentCatchupPId = pt_->getCatchupPId();
	if (currentCatchupPId != UNDEF_PARTITIONID) {
		util::StackAllocator alloc(
			util::AllocatorInfo(ALLOCATOR_GROUP_SYS, "syncStats"),
			fixedSizeAlloc_);

		util::XArray<NodeId> catchups(alloc);
		NodeId owner = pt_->getOwner(currentCatchupPId);
		pt_->getCatchup(currentCatchupPId, catchups);
		if (owner != UNDEF_NODEID && catchups.size() == 1) {
				syncStats["pId"] =
					picojson::value(static_cast<double>(currentCatchupPId));
				NodeId catchup = *catchups.begin();
				picojson::object ownerInfo, catchupInfo;
				SyncStat &stat = clsMgr_->getSyncStat();

				ownerInfo["lsn"] = picojson::value(
					static_cast<double>(pt_->getLSN(currentCatchupPId, owner)));
				ownerInfo["chunkNum"] =
					picojson::value(static_cast<double>(stat.syncChunkNum_));
				getServiceAddress(pt_, owner, ownerInfo, addressType);
				syncStats["owner"] = picojson::value(ownerInfo);
				catchupInfo["lsn"] = picojson::value(static_cast<double>(
					pt_->getLSN(currentCatchupPId, catchup)));
				catchupInfo["chunkNum"] =
					picojson::value(static_cast<double>(stat.syncApplyNum_));
				getServiceAddress(pt_, catchup, catchupInfo, addressType);
				syncStats["catchup"] = picojson::value(catchupInfo);
			}
	}
}

/*!
	@brief Gets the statistics about memory usage for each Partition group
*/

void SystemService::getPGStoreMemoryLimitStats(picojson::value &result) {
	picojson::array &limitStats = picojsonUtil::setJsonArray(result);

	typedef std::pair<const char8_t*, ChunkBufferStats::Param> NamedParam;
	const NamedParam pgParamList[] = {
		NamedParam("pgStoreUse", ChunkBufferStats::BUF_STAT_USE_STORE_SIZE),
		NamedParam("pgLimit", ChunkBufferStats::BUF_STAT_BUFFER_LIMIT_SIZE),
		NamedParam("pgMemory", ChunkBufferStats::BUF_STAT_USE_BUFFER_SIZE),
		NamedParam("pgSwapRead", ChunkBufferStats::BUF_STAT_SWAP_READ_COUNT),
		NamedParam(
				"pgNormalSwapRead",
				ChunkBufferStats::BUF_STAT_SWAP_NORMAL_READ_COUNT),
		NamedParam(
				"pgColdBufferingSwapRead",
				ChunkBufferStats::BUF_STAT_SWAP_COLD_READ_COUNT)
	};

	const NamedParam pgCategoryParamList[] = {
		NamedParam("pgCategoryStoreUse", ChunkBufferStats::BUF_STAT_USE_STORE_SIZE),
		NamedParam("pgCategoryStoreMemory", ChunkBufferStats::BUF_STAT_USE_BUFFER_SIZE),
		NamedParam("pgCategorySwapRead", ChunkBufferStats::BUF_STAT_SWAP_READ_COUNT),
		NamedParam("pgCategorySwapWrite", ChunkBufferStats::BUF_STAT_SWAP_WRITE_COUNT),
	};

	const PartitionListStats &plStats = partitionList_->getStats();
	const PartitionGroupConfig &pgConfig =
			txnMgr_->getPartitionGroupConfig();
	const uint32_t pgCount = pgConfig.getPartitionGroupCount();
	const size_t categoryCount = DS_CHUNK_CATEGORY_SIZE;
	for (PartitionGroupId pgId = 0; pgId < pgCount; pgId++) {
		limitStats.push_back(picojson::value(picojson::object()));
		picojson::object &ptInfo = limitStats.back().get<picojson::object>();

		{
			ChunkBufferStats bufStats(NULL);
			plStats.getPGChunkBufferStats(pgId, bufStats);

			const size_t count = sizeof(pgParamList) / sizeof(*pgParamList);
			for (size_t i = 0; i < count; i++) {
				const NamedParam &param = pgParamList[i];
				ptInfo[param.first] = picojson::value(static_cast<double>(
						bufStats.table_(param.second).get()));
			}
		}

		for (size_t categoryId = 0;
				categoryId < categoryCount; categoryId++) {
			ChunkBufferStats stats(NULL);
			plStats.getPGCategoryChunkBufferStats(
					pgId, static_cast<ChunkCategoryId>(categoryId), stats);

			const size_t count =
					sizeof(pgCategoryParamList) / sizeof(*pgCategoryParamList);
			for (size_t i = 0; i < count; i++) {
				const NamedParam &param = pgCategoryParamList[i];
				picojson::value &list = ptInfo[param.first];
				if (categoryId == 0) {
					list = picojson::value(picojson::array());
				}
				const picojson::value value(static_cast<double>(
						stats.table_(param.second).get()));
				list.get<picojson::array>().push_back(value);
			}
		}
	}
}

/*!
	@brief Gets the statistics about memory usage
*/
void SystemService::getMemoryStats(
		picojson::value &result, const char8_t *namePrefix,
		const char8_t *selectedType, int64_t minSize) {
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

	picojson::object &resultObj = picojsonUtil::setJsonObject(result);
	picojson::array &allocList = picojsonUtil::setJsonArray(resultObj["allocators"]);
	for (StatsList::iterator it = statsList.begin(); it != statsList.end();
		 ++it) {
		const util::AllocatorStats &stats = *it;

		allocList.push_back(picojson::value());
		picojson::object &entry = picojsonUtil::setJsonObject(allocList.back());
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

		picojson::object &statsObj = picojsonUtil::setJsonObject(entry[name]);
		const char8_t *const typeList[] = {
			"totalSize", "peakTotalSize", "cacheSize", "cacheMissCount",
			"cacheAdjustCount", "hugeAllocationCount",
			"allocationCount", "deallocationCount", NULL, "cacheLimit",
			"stableLimit",
		};
		UTIL_STATIC_ASSERT(
				sizeof(typeList) / sizeof(*typeList) ==
				util::AllocatorStats::STAT_TYPE_END);

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
	@brief Gets the statistics about synchronization
*/
void SystemService::getSqlTempStoreStats(picojson::value &result) {
	picojson::object &resultObj = picojsonUtil::setJsonObject(result);
	picojson::array &ltsStats = picojsonUtil::setJsonArray(resultObj["sqlTempStoreGroupStats"]);

	LocalTempStore &store =
			clsSvc_->getSQLService()->getExecutionManager()->getJobManager()->getStore();

	util::Mutex &statsMutex = store.getGroupStatsMapMutex();
	{
		util::LockGuard<util::Mutex> guard(statsMutex);
		LocalTempStore::GroupStatsMap groupStatsMap = store.getGroupStatsMap();
		LocalTempStore::GroupStatsMap::const_iterator itr = groupStatsMap.begin();
		for (; itr != groupStatsMap.end(); ++itr) {
			picojson::object groupInfo;
			LocalTempStore::GroupStats stats = (itr->second);
			groupInfo["groupId"] = picojson::value(
					static_cast<double>(itr->first));
			groupInfo["inputBlockCount"] = picojson::value(
					static_cast<double>(itr->second.appendBlockCount_));
			groupInfo["outputBlockCount"] = picojson::value(
					static_cast<double>(itr->second.readBlockCount_));
			groupInfo["swapInBlockCount"] = picojson::value(
					static_cast<double>(itr->second.swapInCount_));
			groupInfo["swapOutBlockCount"] = picojson::value(
					static_cast<double>(itr->second.swapOutCount_));
			groupInfo["activeBlockCount"] = picojson::value(
					static_cast<double>(itr->second.activeBlockCount_));
			groupInfo["maxActiveBlockCount"] = picojson::value(
					static_cast<double>(itr->second.maxActiveBlockCount_));
			groupInfo["blockUsedSize"] = picojson::value(
					static_cast<double>(itr->second.blockUsedSize_));
			groupInfo["latchBlockCount"] = picojson::value(
					static_cast<double>(itr->second.latchBlockCount_));
			groupInfo["maxLatchBlockCount"] = picojson::value(
					static_cast<double>(itr->second.maxLatchBlockCount_));
			ltsStats.push_back(picojson::value(groupInfo));
		}
	}
}


/*!
	@brief Gets/Sets config of SystemService
*/
bool SystemService::getOrSetConfig(
		util::StackAllocator &alloc, const std::vector<std::string> namePath,
		picojson::value &result, const picojson::value *paramValue, bool noUnit) {
	const bool updating = (paramValue != NULL);
	GS_TRACE_INFO(SYSTEM_SERVICE_DETAIL, GS_TRACE_SC_WEB_API_CALLED,
			(updating ? "Set" : "Get") << " config called");

	try {
		ConfigTable::ParamId id = CONFIG_TABLE_ROOT;
		ConfigTable::SubPath subPath(alloc);
		for (std::vector<std::string>::const_iterator it = namePath.begin();
			 it != namePath.end(); ++it) {
			const ConfigTable::ParamId groupId = id;
			if (!config_.findParam(groupId, id, it->c_str(), &subPath)) {
				GS_TRACE_WARNING(SYSTEM_SERVICE, GS_TRACE_SC_BAD_REQUEST,
						"Unknown config name (basePath=" <<
						config_.pathFormatter(groupId, &subPath) <<
						", name=" << *it << ")");
				return false;
			}
		}

		if (updating) {
			assert(paramValue != NULL);

			util::NormalOStringStream oss;
			oss << "Web API (updated=" << util::DateTime::now(false) << ")";
			try {
				config_.set(
						id, *paramValue, oss.str().c_str(), NULL, &subPath);
			}
			catch (UserException &e) {
				UTIL_TRACE_EXCEPTION_WARNING(SYSTEM_SERVICE, e, "");
				return false;
			}

			picojson::value accepted;
			config_.toJSON(accepted, id, ConfigTable::EXPORT_MODE_SHOW_HIDDEN);

			GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_CONFIG_UPDATED,
					"Configuration updated ("
					"name=" << config_.pathFormatter(id, &subPath) <<
					", acceptedValue=" <<
					ConfigTable::getSubValue(accepted, &subPath) <<
					", inputValue=" << *paramValue << ")");

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
	static_cast<void>(result);
	static_cast<void>(reset);
	return false;
}

/*!
	@brief Handles getLogs command
*/
void SystemService::getLogs(
		picojson::value &result, std::string &searchStr,
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
			unchangeableTraceCategories_.find(category) !=
				unchangeableTraceCategories_.end()) {
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
			if (unchangeableTraceCategories_.find(tracerList[pos]->getName()) ==
				unchangeableTraceCategories_.end()) {
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


bool SystemService::getSQLProcessorProfile(
		util::StackAllocator &alloc, EventEngine::VariableSizeAllocator &varSizeAlloc,
		picojson::value &result, const int64_t *id) {
	typedef SQLProcessor::ProfilerManager Manager;
	if (!Manager::isEnabled()) {
		return false;
	}

	Manager &manager = Manager::getInstance();
	if (id == NULL) {
		util::Vector<Manager::ProfilerId> idList(alloc);
		manager.listIds(idList);

		JsonUtils::OutStream out(result);
		util::ObjectCoder().encode(out, idList);
	}
	else {
		SQLProcessor::Profiler profiler(varSizeAlloc);
		if (!manager.getProfiler(*id, profiler)) {
			return false;
		}
		result = profiler.getResult();
	}

	return true;
}

bool SystemService::getSQLProcessorGlobalProfile(picojson::value &result) {
	typedef SQLProcessor::ProfilerManager Manager;
	if (!Manager::isEnabled()) {
		return false;
	}

	Manager &manager = Manager::getInstance();
	manager.getGlobalProfile(result);
	return true;
}

bool SystemService::setSQLProcessorConfig(
		const char8_t *key, const char8_t *value, bool forProfiler) {
	if (forProfiler) {
		typedef SQLProcessor::ProfilerManager Manager;
		if (!Manager::isEnabled()) {
			return false;
		}

		bool boolValue;
		if (strlen(value) == 0) {
			boolValue = false;
		}
		else if (!util::LexicalConverter<bool>()(value, boolValue)) {
			return false;
		}

		Manager &manager = Manager::getInstance();
		if (strcmp(key, "sub") == 0) {
			manager.setActivated(Manager::PROFILE_CORE, boolValue);
		}
		else if (strcmp(key, "memory") == 0) {
			manager.setActivated(Manager::PROFILE_MEMORY, boolValue);
		}
		else if (strcmp(key, "index") == 0) {
			manager.setActivated(Manager::PROFILE_INDEX, boolValue);
		}
		else if (strcmp(key, "interruption") == 0) {
			manager.setActivated(Manager::PROFILE_INTERRUPTION, boolValue);
		}
		else if (strcmp(key, "error") == 0) {
			manager.setActivated(Manager::PROFILE_ERROR, boolValue);
		}
		else {
			return false;
		}
	}
	else {
		typedef SQLProcessorConfig::Manager Manager;
		if (!Manager::isEnabled()) {
			return false;
		}

		int64_t intValue;
		if (strlen(value) == 0) {
			intValue = -1;
		}
		else if (!util::LexicalConverter<int64_t>()(value, intValue)) {
			return false;
		}

		picojson::value configValue((picojson::object()));
		configValue.get<picojson::object>()[key] =
				picojson::value(static_cast<double>(intValue));
		JsonUtils::InStream in(configValue);

		Manager &manager = Manager::getInstance();
		SQLProcessorConfig config;

		manager.mergeTo(config);
		util::ObjectCoder::withoutOptional().decode(in, config);

		manager.apply(config, true);
	}

	return true;
}

bool SystemService::getSQLProcessorConfig(
		bool forProfiler, picojson::value &result) {
	if (forProfiler) {
		typedef SQLProcessor::ProfilerManager Manager;
		if (!Manager::isEnabled()) {
			return false;
		}

		result = picojson::value(picojson::object());
		picojson::object &resultObj = result.get<picojson::object>();

		Manager &manager = Manager::getInstance();
		resultObj["sub"] = picojson::value(
				manager.isActivated(Manager::PROFILE_CORE));
		resultObj["memory"] = picojson::value(
				manager.isActivated(Manager::PROFILE_MEMORY));
		resultObj["index"] = picojson::value(
				manager.isActivated(Manager::PROFILE_INDEX));
		resultObj["interruption"] = picojson::value(
				manager.isActivated(Manager::PROFILE_INTERRUPTION));
		resultObj["error"] = picojson::value(
				manager.isActivated(Manager::PROFILE_ERROR));
	}
	else {
		typedef SQLProcessorConfig::Manager Manager;
		if (!Manager::isEnabled()) {
			return false;
		}

		SQLProcessorConfig config;

		Manager &manager = Manager::getInstance();
		manager.mergeTo(config);

		JsonUtils::OutStream out(result);
		util::ObjectCoder::withDefaults().encode(out, config);
	}

	return true;
}

bool SystemService::getSQLProcessorPartialStatus(picojson::value &result) {
	typedef SQLProcessorConfig::Manager Manager;
	if (!Manager::isEnabled()) {
		return false;
	}

	Manager &manager = Manager::getInstance();

	SQLProcessorConfig::PartialStatus status;
	if (!manager.getPartialStatus(status)) {
		result = picojson::value(picojson::object());
		return true;
	}

	JsonUtils::OutStream out(result);
	util::ObjectCoder().encode(out, status);

	return true;
}

bool SystemService::setSQLProcessorSimulation(
		util::StackAllocator &alloc, const picojson::value &request) {
	typedef SQLProcessorConfig::Manager Manager;
	if (!Manager::isEnabled()) {
		return false;
	}

	Manager &manager = Manager::getInstance();
	if (request.is<picojson::object>() &&
			request.get<picojson::object>().empty()) {
		manager.clearSimulation();
		return true;
	}

	util::Vector<Manager::SimulationEntry> simulationList(alloc);
	{
		const picojson::array &src =
				JsonUtils::as<picojson::array>(request, "simulationList");
		for (picojson::array::const_iterator it = src.begin();
				it != src.end(); ++it) {
			Manager::SimulationEntry srcEntry;
			srcEntry.set(*it);
			simulationList.push_back(srcEntry);
		}
	}

	SQLType::Id type;
	{
		JsonUtils::InStream in(
				JsonUtils::as<picojson::value>(request, "type"));
		util::ObjectCoder().decode(
				in, util::EnumCoder()(type, SQLType::Coder()).coder());
	}

	int64_t inputCount;
	{
		JsonUtils::InStream in(
				JsonUtils::as<picojson::value>(request, "inputCount"));
		util::ObjectCoder().decode(in, inputCount);
	}

	manager.setSimulation(simulationList, type, inputCount);
	return true;
}

bool SystemService::getSQLProcessorSimulation(
		util::StackAllocator &alloc, picojson::value &result) {
	typedef SQLProcessorConfig::Manager Manager;
	if (!Manager::isEnabled()) {
		return false;
	}

	Manager &manager = Manager::getInstance();

	typedef util::Vector<Manager::SimulationEntry> SimulationList;
	SimulationList simulationList(alloc);
	SQLType::Id type;
	int64_t inputCount;

	result = picojson::value(picojson::object());
	picojson::object &resultObj = result.get<picojson::object>();

	if (!manager.getSimulation(simulationList, type, inputCount)) {
		return true;
	}

	{
		picojson::value &destValue = resultObj["simulationList"];

		destValue = picojson::value(picojson::array());
		picojson::array &dest = destValue.get<picojson::array>();

		for (SimulationList::const_iterator it = simulationList.begin();
				it != simulationList.end(); ++it) {
			dest.push_back(picojson::value());
			it->get(dest.back());
		}
	}

	{
		const SQLType::Id &inType = type;
		JsonUtils::OutStream out(resultObj["type"]);
		util::ObjectCoder().encode(
				out, util::EnumCoder()(inType, SQLType::Coder()).coder());
	}

	{
		JsonUtils::OutStream out(resultObj["inputCount"]);
		util::ObjectCoder().encode(out, inputCount);
	}

	return true;
}

bool SystemService::getSQLCompilerProfile(
		util::StackAllocator &alloc, int32_t index,
		const util::Set<util::String> &filteringSet, picojson::value &result) {
	typedef SQLCompiler::ProfilerManager Manager;
	if (!Manager::isEnabled()) {
		return false;
	}

	if (index < 0) {
		return false;
	}

	Manager &manager = Manager::getInstance();

	SQLCompiler::Profiler *profiler =
			manager.getProfiler(alloc, static_cast<uint32_t>(index));
	if (profiler == NULL) {
		return false;
	}

	if (!filteringSet.empty()) {
		size_t foundCount = 2;
		if (filteringSet.find(
				util::String("plan", alloc)) == filteringSet.end()) {
			profiler->plan_ = SQLPreparedPlan(alloc);
			profiler->tableInfoList_.clear();
			foundCount--;
		}
		if (filteringSet.find(
				util::String("optimize", alloc)) == filteringSet.end()) {
			profiler->optimizationResult_.clear();
			foundCount--;
		}
		if (filteringSet.size() != foundCount) {
			return false;
		}
	}

	JsonUtils::OutStream out(result);
	TupleValue::coder(util::ObjectCoder(), NULL).encode(out, *profiler);

	return true;
}

bool SystemService::setSQLCompilerConfig(
		const char8_t *category, const char8_t *key, const char8_t *value) {
	typedef SQLCompiler::ProfilerManager Manager;
	if (!Manager::isEnabled()) {
		return false;
	}

	Manager &manager = Manager::getInstance();

	if (strcmp(category, "profile") == 0) {
		SQLCompiler::Profiler::Target target = manager.getTarget();

		bool boolValue;
		if (strlen(value) == 0) {
			boolValue = false;
		}
		else if (!util::LexicalConverter<bool>()(value, boolValue)) {
			return false;
		}

		picojson::value jsonValue((picojson::object()));
		jsonValue.get<picojson::object>()[key] = picojson::value(boolValue);
		JsonUtils::InStream in(jsonValue);
		util::ObjectCoder().decode(in, target);

		manager.setTarget(target);
		return true;
	}
	else if (strcmp(category, "optimize") == 0) {
		if (!manager.isEnabled()) {
			return false;
		}

		bool forDefault = false;
		bool boolValue = true;
		if (strlen(value) == 0) {
			forDefault = true;
		}
		else if (!util::LexicalConverter<bool>()(value, boolValue)) {
			return false;
		}

		SQLCompiler::OptimizationUnitType type;
		if (!SQLCompiler::OptimizationUnit::TYPE_CODER(key, type)) {
			return false;
		}

		typedef SQLCompiler::OptimizationFlags Flags;
		Flags flags = (forDefault ? 0 : manager.getOptimizationFlags());
		if (boolValue) {
			flags |= static_cast<Flags>(1 << type);
		}
		else {
			flags &= ~static_cast<Flags>(1 << type);
		}

		manager.setOptimizationFlags(flags, forDefault);
		return true;
	}
	else {
		return false;
	}
}

bool SystemService::getSQLCompilerConfig(
		const char8_t *category, picojson::value &result) {
	typedef SQLCompiler::ProfilerManager Manager;
	if (!Manager::isEnabled()) {
		return false;
	}

	Manager &manager = Manager::getInstance();

	if (strcmp(category, "profile") == 0) {
		JsonUtils::OutStream out(result);
		util::ObjectCoder::withDefaults().encode(out, manager.getTarget());
	}
	else if (strcmp(category, "optimize") == 0) {
		result = picojson::value(picojson::object());
		picojson::object &resultObj = result.get<picojson::object>();

		typedef SQLCompiler::OptimizationFlags Flags;
		const Flags flags = manager.getOptimizationFlags();

		for (int32_t i = 0; i < SQLCompiler::OPT_END; i++) {
			const char8_t *name = SQLCompiler::OptimizationUnit::TYPE_CODER(
					static_cast<SQLCompiler::OptimizationUnitType>(i));
			const bool value = ((flags & (static_cast<Flags>(1) << i)) != 0);

			resultObj[name] = picojson::value(value);
		}
	}
	else {
		return false;
	}

	return true;
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
SystemService::ServiceTypeInfo SystemService::resolveAddressType(
		const WebAPIRequest::ParameterMap &parameterMap,
		const ServiceTypeInfo &defaultType) {
	ServiceType type;
	do {
		std::map<std::string, std::string>::const_iterator it =
				parameterMap.find("addressType");

		if (it == parameterMap.end()) {
			type = static_cast<ServiceType>(defaultType.first);
			break;
		}

		const std::string &typeStr = it->second;
		if (typeStr == "cluster") {
			type = CLUSTER_SERVICE;
		}
		else if (typeStr == "transaction") {
			type = TRANSACTION_SERVICE;
		}
		else if (typeStr == "sync") {
			type = SYNC_SERVICE;
		}
		else if (typeStr == "system") {
			type = SYSTEM_SERVICE;
		}
		else if (typeStr == "sql") {
			type = SQL_SERVICE;
		}
		else {
			return ServiceTypeInfo(-1, false);
		}
	}
	while (false);

	bool secure;
	do {
		std::map<std::string, std::string>::const_iterator it =
				parameterMap.find("addressSsl");
		if (it == parameterMap.end()) {
			secure = defaultType.second;
			break;
		}

		const std::string &typeStr = it->second;
		if (typeStr == "true") {
			secure = true;
		}
		else if (typeStr == "false") {
			secure = false;
		}
		else {
			return ServiceTypeInfo(-1, false);
		}
	}
	while (false);

	if (type != SYSTEM_SERVICE) {
		secure = false;
	}

	return ServiceTypeInfo(type, secure);
}

/*!
	@brief Sets display options
*/
bool SystemService::acceptStatsOption(
		StatTable &stats, const WebAPIRequest::ParameterMap &parameterMap,
		const ServiceTypeInfo *defaultType) {
	if (defaultType == NULL) {
		const ServiceTypeInfo localType(
				ServiceTypeInfo::first_type(DEFAULT_ADDRESS_TYPE), false);
		return acceptStatsOption(stats, parameterMap, &localType);
	}

	stats.setDisplayOption(STAT_TABLE_DISPLAY_WEB_OR_DETAIL_TRACE, true);
	stats.setDisplayOption(STAT_TABLE_DISPLAY_WEB_ONLY, true);

	const ServiceTypeInfo &addressType =
			resolveAddressType(parameterMap, *defaultType);
	switch (addressType.first) {
	case CLUSTER_SERVICE:
		stats.setDisplayOption(STAT_TABLE_DISPLAY_ADDRESS_CLUSTER, true);
		break;
	case TRANSACTION_SERVICE:
		stats.setDisplayOption(STAT_TABLE_DISPLAY_ADDRESS_TRANSACTION, true);
		break;
	case SYNC_SERVICE:
		stats.setDisplayOption(STAT_TABLE_DISPLAY_ADDRESS_SYNC, true);
		break;
	case SQL_SERVICE:
		stats.setDisplayOption(STAT_TABLE_DISPLAY_ADDRESS_SQL, true);
		break;
	case SYSTEM_SERVICE:
		break;
	default:
		return false;
	}

	if (addressType.second) {
		stats.setDisplayOption(STAT_TABLE_DISPLAY_ADDRESS_SECURE, true);
	}

	const size_t maxNameListSize = 9;

	const char8_t *const allNameList[][maxNameListSize] = {
		{"all", "clusterInfo", "recoveryInfo", "cpInfo", "performanceInfo",
			NULL},
		{"detail", "clusterDetail", "storeDetail", "transactionDetail",
			"memoryDetail", "syncDetail", "pgLimitDetail", "notificationMember", NULL}};

	const int32_t allOptionList[][maxNameListSize] = {
		{-1, STAT_TABLE_DISPLAY_SELECT_CS, STAT_TABLE_DISPLAY_SELECT_CP,
			STAT_TABLE_DISPLAY_SELECT_RM, STAT_TABLE_DISPLAY_SELECT_PERF, -1},
		{-1, STAT_TABLE_DISPLAY_OPTIONAL_CS, STAT_TABLE_DISPLAY_OPTIONAL_DS,
			STAT_TABLE_DISPLAY_OPTIONAL_TXN, STAT_TABLE_DISPLAY_OPTIONAL_MEM,
			STAT_TABLE_DISPLAY_OPTIONAL_SYNC,
			STAT_TABLE_DISPLAY_OPTIONAL_PGLIMIT,
			STAT_TABLE_DISPLAY_OPTIONAL_NOTIFICATION_MEMBER,
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

SystemService::ServiceTypeInfo SystemService::statOptionToAddressType(
		const StatTable &stat) {
	const std::pair<ServiceType, bool> &base =
			ClusterManager::statOptionToAddressType(stat);
	return ServiceTypeInfo(base.first, base.second);
}

/*!
	@brief Handler Operator
*/
void OutputStatsHandler::operator()(EventContext &ec, Event &) {
	sysSvc_->traceStats(ec.getVariableSizeAllocator());
	if (pt_->isMaster()) {
		GS_TRACE_INFO(CLUSTER_DETAIL,
				GS_TRACE_CS_TRACE_STATS,
				pt_->dumpPartitionsList(ec.getAllocator(), PartitionTable::PT_CURRENT_OB));
		GS_TRACE_INFO(CLUSTER_DETAIL, GS_TRACE_CS_CLUSTER_STATUS,
				clsMgr_->dump());
		{
			util::StackAllocator &alloc = ec.getAllocator();
			GS_TRACE_INFO(CLUSTER_DETAIL,
				GS_TRACE_CS_CLUSTER_STATUS,
				pt_->dumpPartitions(
					alloc, PartitionTable::PT_CURRENT_OB));
		}
		GS_TRACE_INFO(CLUSTER_DETAIL, GS_TRACE_CS_CLUSTER_STATUS,
				pt_->dumpDatas(true));
	}
}


SystemService::WebAPIServerThread::WebAPIServerThread(
		bool enabled, bool secure) :
		runnable_(enabled),
		enabled_(enabled),
		secure_(secure) {
}

bool SystemService::WebAPIServerThread::isEnabled() const {
	return enabled_;
}

void SystemService::WebAPIServerThread::initialize(ManagerSet &mgrSet) {
	sysSvc_ = mgrSet.sysSvc_;
	clsSvc_ = mgrSet.clsSvc_;
	mgrSet_ = &mgrSet;
}

void SystemService::WebAPIServerThread::tryStart() {
	if (!enabled_) {
		return;
	}
	start();
}

/*!
	@brief Starts thread for Web API
*/
void SystemService::WebAPIServerThread::run() {
	try {
		ListenerSocketHandler socketHandler(*mgrSet_, secure_);
		util::Socket &socket = socketHandler.getFile();

		socket.open(
			util::SocketAddress::FAMILY_INET, util::Socket::TYPE_STREAM);
		socket.setBlockingMode(false);
		socket.setReuseAddress(true);

		util::SocketAddress addr;
		EventEngine::Config tmp;
		tmp.setServerAddress(
				mgrSet_->config_->get<const char8_t *>(
						CONFIG_TABLE_SYS_SERVICE_ADDRESS),
				mgrSet_->config_->getUInt16(secure_ ?
						CONFIG_TABLE_SYS_SERVICE_SSL_PORT :
						CONFIG_TABLE_SYS_SERVICE_PORT));

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

void SystemService::WebAPIServerThread::start(
		util::ThreadRunner *runner, const util::ThreadAttribute *attr) {
	util::Thread::start(runner, attr);
}

SystemService::ListenerSocketHandler::ListenerSocketHandler(
		ManagerSet &mgrSet, bool secure) :
		clsSvc_(mgrSet.clsSvc_),
		syncSvc_(mgrSet.syncSvc_),
		sysSvc_(mgrSet.sysSvc_),
		txnSvc_(mgrSet.txnSvc_),
		pt_(mgrSet.pt_),
		syncMgr_(mgrSet.syncSvc_->getManager()),
		clsMgr_(mgrSet.clsSvc_->getManager()),
	  	partitionList_(mgrSet.partitionList_),
		cpSvc_(mgrSet.cpSvc_),
		fixedSizeAlloc_(mgrSet.fixedSizeAlloc_),
		varSizeAlloc_(util::AllocatorInfo(ALLOCATOR_GROUP_SYS, "webListenerVar")),
		alloc_(util::AllocatorInfo(ALLOCATOR_GROUP_SYS, "webListenerStack"),
				fixedSizeAlloc_),
		socketFactory_(resolveSocketFactory(sysSvc_, secure)),
		secure_(secure) {
	alloc_.setFreeSizeLimit(alloc_.base().getElementSize());
	uriToStatement_ = {
		{"stat", "GS_STAT"}, {"shutdown", "GS_STOP_NODE"}, {"join", "GS_JOIN_CLUSTER"}, {"leave", "GS_LEAVE_CLUSTER"},
		{"stop", "GS_STOP_CLUSTER"}, {"host", "GS_CONFIG"},	{"increase", "GS_INCREASE_CLUSTER"}, {"failover", "GS_FAILOVER_CLUSTER"},
		{"partition", "GS_PARTITION"}, {"loadBalance", "GS_LOAD_BALANCE"}, {"checkpoint", "GS_CHECK_POINT"}, {"periodicCheckpoint", "GS_CHECK_POINT"},
		{"log", "GS_LOGS"}, {"trace", "GS_LOG_CONF"}, {"backup", "GS_BACKUP"}, {"config", "GS_PARAM_CONF"},	
		{"clearUserCache", "GS_AUTH_CACHE"}, {"userCache", "GS_AUTH_CACHE"}
	};
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
		AbstractSocket socket;
		socketFactory_.create(socket);
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
			AUDIT_TRACE_INFO_COMMAND();

			dispatch(request, response);

			if (response.statusCode_ >= 400) {
				AUDIT_TRACE_ERROR_CODED_COMMAND();
			};

		}
		catch (std::exception &e) {
			UTIL_TRACE_EXCEPTION(SYSTEM_SERVICE, e, "");
			AUDIT_TRACE_ERROR_COMMAND();

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

std::string SystemService::ListenerSocketHandler::getURI(WebAPIRequest &request) {
	util::NormalOStringStream oss;
	std::vector<std::string> parameters;
	for (size_t pos = 0; pos < request.pathElements_.size(); pos++) {
		oss << "/" << request.pathElements_[pos];
	}

	for (auto it = request.parameterMap_.begin(); it != request.parameterMap_.end(); it++) {
        parameters.push_back(it->first + "=" + it->second);
    }
	std::string separate = " ";
	for (const auto &parameter: parameters) {
        oss << separate << parameter;
		separate = ",";
    }
	return oss.str();
}

std::string SystemService::ListenerSocketHandler::getSenderName(AbstractSocket *socket) {
	util::NormalOStringStream oss;
	util::SocketAddress sa;
	u8string host;
	uint16_t port;
	socket->getPeerName(sa);
	sa.getIP(&host, &port);
	oss << host << ":" << port;
	return  oss.str();
}

std::string SystemService::ListenerSocketHandler::getNodeName(AbstractSocket *socket) {
	util::NormalOStringStream oss;
	util::SocketAddress sa;
	u8string host;
	uint16_t port;
	socket->getSocketName(sa);
	sa.getIP(&host, &port);
	oss << host << ":" << port;
	return  oss.str();
}

std::string SystemService::ListenerSocketHandler::getErrorInfo(WebAPIResponse &response) {
	util::NormalOStringStream oss;
	oss.clear();
	if (response.hasJson_) {
		oss << response.body_.c_str();
	}
	else {
		switch (response.statusCode_) {
			case 400:
				oss << "Bad request.";
				break;
			case 405:
				oss << "Method error.";
				break;
			case 500:
				oss << "Internal service error.";
				break;
		}
	}
	return oss.str();
}

std::string SystemService::ListenerSocketHandler::getStatementName(std::string uriOperator) {
	std::string statementName;
	statementName.clear();

	auto iter = uriToStatement_.find(uriOperator); 
	if (iter != end(uriToStatement_)) {
		statementName = iter->second;
	}
	return statementName;
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
				 request.pathElements_[1] == "clearUserCache") {
			if (request.method_ != EBB_POST) {
				response.setMethodError();
				return;
			}

			const ServiceTypeInfo &addressType =
					getDefaultAddressType(request.parameterMap_);
			if (addressType.first < 0) {
				response.setBadRequestError();
				return;
			}

			if (request.parameterMap_.find("name") ==
				request.parameterMap_.end()) {
				response.setBadRequestError();
				return;
			}

			const std::string name =
				request.parameterMap_["name"];
			if (name.empty()) {
				response.setBadRequestError();
				return;
			}
			bool isDatabase = false;

			if (request.parameterMap_.find("isDatabase") !=
				request.parameterMap_.end()) {
				if (request.parameterMap_["isDatabase"] == "true") {
					isDatabase = true;
				}
				else if (request.parameterMap_["isDatabase"] == "false") {
					isDatabase = false;
				}
				else {
					picojson::object errorInfo;
					int32_t errorNo = 0;
					std::string reason;
					
					errorNo = WEBAPI_CP_OTHER_REASON;
					reason = "unknown parameter value: " + request.parameterMap_["isDatabase"];
					errorInfo["errorStatus"] =
						picojson::value(static_cast<double>(errorNo));
					errorInfo["reason"] = picojson::value(reason);
					response.setJson(picojson::value(errorInfo));
					response.setBadRequestError();
					return;
				}
			}

			picojson::value result;
			if (!sysSvc_->clearUserCache(name, isDatabase, result)) {
				response.setJson(result);
				response.setBadRequestError();
			}
		}
		else if (request.pathElements_.size() == 2 &&
				 request.pathElements_[1] == "userCache") {
			if (request.method_ != EBB_GET) {
				response.setMethodError();
				return;
			}

			const ServiceTypeInfo &addressType =
					getDefaultAddressType(request.parameterMap_);
			if (addressType.first < 0) {
				response.setBadRequestError();
				return;
			}

			picojson::value result;

			UserCache *uc = txnSvc_->userCache_;
			if (uc == NULL) {
				response.setJson(result);
				return;
			}

			if (request.parameterMap_.find("name") !=
				request.parameterMap_.end()) {
				bool isDatabase = false;

				GlobalVariableSizeAllocator& varAlloc = uc->getAllocator();

				const std::string name =
					request.parameterMap_["name"];
				if (request.parameterMap_.find("isDatabase") !=
					request.parameterMap_.end()) {
					if (request.parameterMap_["isDatabase"] == "true") {
						isDatabase = true;
					}
					else if (request.parameterMap_["isDatabase"] == "false") {
						isDatabase = false;
					}
					else {
						picojson::object errorInfo;
						int32_t errorNo = 0;
						std::string reason;
						
						errorNo = WEBAPI_CP_OTHER_REASON;
						reason = "unknown parameter value: " + request.parameterMap_["isDatabase"];
						errorInfo["errorStatus"] =
							picojson::value(static_cast<double>(errorNo));
						errorInfo["reason"] = picojson::value(reason);
						response.setJson(picojson::value(errorInfo));
						response.setBadRequestError();
						return;
					}
				}

				UserString nameUS(varAlloc);
				nameUS.append(name.c_str());
				uc->scan(&nameUS, isDatabase, result);

			} else {
				uc->scan(NULL, false, result);
			}

			response.setJson(result);
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

			const ServiceTypeInfo &addressType =
					getDefaultAddressType(request.parameterMap_);
			if (addressType.first < 0) {
				response.setBadRequestError();
				return;
			}

			picojson::value result;
			sysSvc_->getHosts(result, addressType);
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

			const ServiceTypeInfo &addressType =
					getDefaultAddressType(request.parameterMap_);
			if (addressType.first < 0) {
				response.setBadRequestError();
				return;
			}

			if (!acceptStatsOption(
					stats, request.parameterMap_, &addressType)) {
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

				const ServiceTypeInfo &addressType =
						getDefaultAddressType(request.parameterMap_);
				if (addressType.first < 0) {
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

				bool replicationDump = false;
				if (request.parameterMap_.find("replication") !=
					request.parameterMap_.end()) {
					const std::string retStr = request.parameterMap_["replication"];
					if (retStr == "true" && pt_->isMaster()) {
						replicationDump = true;
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

				bool sqlOwnerDump = false;
				if (request.parameterMap_.find("sqlOwnerDump") !=
					request.parameterMap_.end()) {
					const std::string retStr =
						request.parameterMap_["sqlOwnerDump"];
					if (retStr == "true") {
						sqlOwnerDump = true;
					}
				}

				uint32_t partitionGroupNo = UINT32_MAX;
				if (request.parameterMap_.find("partitionGroupNo") !=
					request.parameterMap_.end()) {
					partitionGroupNo = static_cast<uint32_t>(atoi(
						request.parameterMap_["partitionGroupNo"].c_str()));
				}

				picojson::value result;
				sysSvc_->getPartitions(alloc_, result, partitionNo, addressType,
					isLossOnly, isForce, isSelf, lsnDump, notRoleDump,
					partitionGroupNo, sqlOwnerDump, replicationDump);

				if (request.parameterMap_.find("callback") !=
					request.parameterMap_.end()) {
					response.setJson(request.parameterMap_["callback"], result);
				}
				else {
					response.setJson(result);
				}
			}
			else if (request.pathElements_.size() == 3 &&
					 request.pathElements_[2] == "drop") {
				int32_t partitionNo = -1;
				if (request.parameterMap_.find("partitionNo") !=
					request.parameterMap_.end()) {
					partitionNo =
						atoi(request.parameterMap_["partitionNo"].c_str());
				}
				if (partitionNo >=
						static_cast<int32_t>(pt_->getPartitionNum()) &&
					partitionNo != -1) {
					response.setBadRequestError();
					return;
				}
				bool isForce = false;
				if (request.parameterMap_.find("force") !=
					request.parameterMap_.end()) {
					if (atoi(request.parameterMap_["force"].c_str()) == 1) {
						isForce = true;
					}
				}
				{
					GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
						"Drop partition is called (partitionId="
							<< partitionNo
							<< ", force=" << ValueFormatter()(isForce) << ")");

					if (partitionNo == -1) {
						for (PartitionId pId = 0; pId < pt_->getPartitionNum();
							 pId++) {
							syncSvc_->requestDrop(
								eventSource, alloc_, pId, isForce);
						}
					}
					else {
						syncSvc_->requestDrop(
							eventSource, alloc_, partitionNo, isForce);
					}
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

			std::string searchStr, searchStr2, ignoreStr;
			if (request.parameterMap_.find("searchStr") !=
				request.parameterMap_.end()) {
				searchStr = request.parameterMap_["searchStr"];
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
			sysSvc_->getLogs(result, searchStr, searchStr2, ignoreStr, length);

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
					alloc_, namePath, result, paramValue, noUnit)) {
				response.setBadRequestError();
			}

			if (!result.is<picojson::null>()) {
				response.setJson(result);
			}
		}
		else if (request.pathElements_.size() == 2 &&
				 request.pathElements_[1] == "backup") {
			picojson::object errorInfo;
			int32_t errorNo = 0;
			std::string reason;
			BackupOption option;

			if (request.method_ != EBB_POST) {
				errorNo = WEBAPI_CP_OTHER_REASON;
				reason = "Request method is not POST";
				errorInfo["errorStatus"] =
					picojson::value(static_cast<double>(errorNo));
				errorInfo["reason"] = picojson::value(reason);
				response.setJson(picojson::value(errorInfo));
				response.setMethodError();
				return;
			}

			std::string backupName;
			if (request.parameterMap_.find("backupName") !=
				request.parameterMap_.end()) {
				backupName = request.parameterMap_["backupName"];
			}

			int32_t mode;
			if (request.parameterMap_.find("mode") !=
				request.parameterMap_.end()) {
				if (request.parameterMap_["mode"] == "start") {
					mode = CP_BACKUP_START;
				}
				else if (request.parameterMap_["mode"] == "end") {
					mode = CP_BACKUP_END;
				}
				else if (request.parameterMap_["mode"] == "") {
					mode = CP_BACKUP;
				}
				else {
					errorNo = WEBAPI_CP_BACKUP_MODE_IS_INVALID;
					reason =
						"Unknown backup mode: " + request.parameterMap_["mode"];
					errorInfo["errorStatus"] =
						picojson::value(static_cast<double>(errorNo));
					errorInfo["reason"] = picojson::value(reason);
					response.setJson(picojson::value(errorInfo));
					response.setBadRequestError();
					return;
				}
			}
			else {
				mode = CP_BACKUP;
			}
			option.logArchive_ = false;
			option.logDuplicate_ = false;
			option.stopOnDuplicateError_ = true;
			option.incrementalBackupLevel_ = -1;	 
			option.isIncrementalBackup_ = false;  
			option.isCumulativeBackup_ = false;   
			option.skipBaseline_ = false; 
			if (request.parameterMap_.find("archiveLog") !=
				request.parameterMap_.end()) {
				if (request.parameterMap_["archiveLog"] == "1") {
					option.logArchive_ = true;
				}
				else if (request.parameterMap_["archiveLog"] == "0") {
					option.logArchive_ = false;
				}
				else {
					errorNo = WEBAPI_CP_OTHER_REASON;
					reason = "Invalid archiveLog value: " +
							 request.parameterMap_["archiveLog"];
					errorInfo["errorStatus"] =
							picojson::value(static_cast<double>(errorNo));
					errorInfo["reason"] = picojson::value(reason);
					response.setJson(picojson::value(errorInfo));
					response.setBadRequestError();
					return;
				}
			}
			if (request.parameterMap_.find("duplicateLog") !=
				request.parameterMap_.end()) {
				if (request.parameterMap_["duplicateLog"] == "1") {
					if (option.logArchive_) {
						errorNo = WEBAPI_CP_BACKUP_MODE_IS_INVALID;
						reason =
							"archiveLog=1 and duplicateLog=1 cannot be "
							"specified simultaneously";
						errorInfo["errorStatus"] =
							picojson::value(static_cast<double>(errorNo));
						errorInfo["reason"] = picojson::value(reason);
						response.setJson(picojson::value(errorInfo));
						response.setBadRequestError();
						return;
					}
					option.logDuplicate_ = true;
				}
				else if (request.parameterMap_["duplicateLog"] == "0") {
					option.logDuplicate_ = false;
				}
				else {
					errorNo = WEBAPI_CP_OTHER_REASON;
					reason = "Invalid duplicateLog value: " +
							 request.parameterMap_["duplicateLog"];
					errorInfo["errorStatus"] =
						picojson::value(static_cast<double>(errorNo));
					errorInfo["reason"] = picojson::value(reason);
					response.setJson(picojson::value(errorInfo));
					response.setBadRequestError();
					return;
				}
			}
			if (request.parameterMap_.find("stopOnDuplicateError") !=
				request.parameterMap_.end()) {
				if (option.logDuplicate_ &&
					request.parameterMap_["stopOnDuplicateError"] == "1") {
					option.stopOnDuplicateError_ = true;
				}
				else {
					option.stopOnDuplicateError_ = false;
				}
			}
			if (request.parameterMap_.find("skipBaseline") !=
				request.parameterMap_.end()) {
				if (option.logDuplicate_ &&
					request.parameterMap_["skipBaseline"] == "1") {
					option.skipBaseline_ = true;
				}
				else {
					option.skipBaseline_ = false;
				}
			}

			if (request.parameterMap_.find("incremental") !=
				request.parameterMap_.end()) {
				if (request.parameterMap_["incremental"] == "1") {
					option.isIncrementalBackup_ = true;
				}
				else if (request.parameterMap_["incremental"] == "0") {
					option.isIncrementalBackup_ = false;
				}
				else {
					errorNo = WEBAPI_CP_OTHER_REASON;
					reason = "Invalid incremental value: " +
							 request.parameterMap_["incremental"];
					errorInfo["errorStatus"] =
						picojson::value(static_cast<double>(errorNo));
					errorInfo["reason"] = picojson::value(reason);
					response.setJson(picojson::value(errorInfo));
					response.setBadRequestError();
					return;
				}
			}
			if (option.isIncrementalBackup_ &&
				request.parameterMap_.find("level") !=
					request.parameterMap_.end()) {
				if (request.parameterMap_["level"] == "1") {
					option.incrementalBackupLevel_ = 1;
				}
				else if (request.parameterMap_["level"] == "0") {
					option.incrementalBackupLevel_ = 0;
				}
				else {
					errorNo = WEBAPI_CP_OTHER_REASON;
					reason = "Invalid level value: " +
							 request.parameterMap_["level"];
					errorInfo["errorStatus"] =
						picojson::value(static_cast<double>(errorNo));
					errorInfo["reason"] = picojson::value(reason);
					response.setJson(picojson::value(errorInfo));
					response.setBadRequestError();
					return;
				}
			}
			if (option.isIncrementalBackup_ && option.incrementalBackupLevel_ < 0) {
				errorNo = WEBAPI_CP_OTHER_REASON;
				reason = "incremental=1, but level is not set";
				errorInfo["errorStatus"] =
					picojson::value(static_cast<double>(errorNo));
				errorInfo["reason"] = picojson::value(reason);
				response.setJson(picojson::value(errorInfo));
				response.setBadRequestError();
				return;
			}
			if (option.isIncrementalBackup_ &&
				request.parameterMap_.find("cumulative") !=
					request.parameterMap_.end()) {
				if (request.parameterMap_["cumulative"] == "1") {
					option.isCumulativeBackup_ = true;
				}
				else if (request.parameterMap_["cumulative"] == "0") {
					option.isCumulativeBackup_ = false;
				}
				else {
					errorNo = WEBAPI_CP_OTHER_REASON;
					reason = "Invalid cumulative value: " +
							 request.parameterMap_["cumulative"];
					errorInfo["errorStatus"] =
						picojson::value(static_cast<double>(errorNo));
					errorInfo["reason"] = picojson::value(reason);
					response.setJson(picojson::value(errorInfo));
					response.setBadRequestError();
					return;
				}
			}

			if (option.isIncrementalBackup_) {
				if (option.logArchive_ | option.logDuplicate_) {
					errorNo = WEBAPI_CP_BACKUP_MODE_IS_INVALID;
					reason =
						"incremental=1 and (archiveLog=1 or duplicateLog=1) "
						"cannot be specified simultaneously";
					errorInfo["errorStatus"] =
						picojson::value(static_cast<double>(errorNo));
					errorInfo["reason"] = picojson::value(reason);
					response.setJson(picojson::value(errorInfo));
					response.setBadRequestError();
					return;
				}
			}

			if (((mode == CP_BACKUP || mode == CP_BACKUP_START) &&
					backupName.empty()) ||
				(mode == CP_BACKUP_END && !backupName.empty())) {
				errorNo = WEBAPI_CP_BACKUPNAME_IS_EMPTY;
				reason = "backupName is not specified";
				errorInfo["errorStatus"] =
					picojson::value(static_cast<double>(errorNo));
				errorInfo["reason"] = picojson::value(reason);
				response.setJson(picojson::value(errorInfo));
				response.setBadRequestError();
				return;
			}
			picojson::value result;
			if (!sysSvc_->backupNode(eventSource, backupName.c_str(), mode,
					option,
					result)) {
				response.setJson(result);
				response.setBadRequestError();
				return;
			}
		}
		else if (request.pathElements_.size() == 2 &&
				 request.pathElements_[1] == "archiveLog") {
			picojson::object errorInfo;
			int32_t errorNo = 0;
			std::string reason;

			if (request.method_ != EBB_POST) {
				errorNo = WEBAPI_CP_OTHER_REASON;
				reason = "Request method is not POST";
				errorInfo["errorStatus"] =
					picojson::value(static_cast<double>(errorNo));
				errorInfo["reason"] = picojson::value(reason);
				response.setJson(picojson::value(errorInfo));
				response.setMethodError();
				return;
			}

			std::string backupName;
			if (request.parameterMap_.find("backupName") !=
				request.parameterMap_.end()) {
				backupName = request.parameterMap_["backupName"];
			}
			else {
				errorNo = WEBAPI_CP_BACKUPNAME_IS_EMPTY;
				reason = "backupName is not specified";
				errorInfo["errorStatus"] =
					picojson::value(static_cast<double>(errorNo));
				errorInfo["reason"] = picojson::value(reason);
				response.setJson(picojson::value(errorInfo));
				response.setBadRequestError();
				return;
			}

			int32_t mode;
			if (request.parameterMap_.find("mode") !=
				request.parameterMap_.end()) {
				if (request.parameterMap_["mode"] == "start") {
					mode = CP_ARCHIVE_LOG_START;
				}
				else if (request.parameterMap_["mode"] == "end") {
					mode = CP_ARCHIVE_LOG_END;
				}
				else {
					errorNo = WEBAPI_CP_ARCHIVELOG_MODE_IS_INVALID;
					reason = "unknown mode: " + request.parameterMap_["mode"];
					errorInfo["errorStatus"] =
						picojson::value(static_cast<double>(errorNo));
					errorInfo["reason"] = picojson::value(reason);
					response.setJson(picojson::value(errorInfo));
					response.setBadRequestError();
					return;
				}
			}
			else {
				errorNo = WEBAPI_CP_ARCHIVELOG_MODE_IS_INVALID;
				reason = "mode is not specified";
				errorInfo["errorStatus"] =
					picojson::value(static_cast<double>(errorNo));
				errorInfo["reason"] = picojson::value(reason);
				response.setJson(picojson::value(errorInfo));
				response.setBadRequestError();
				return;
			}

			picojson::value result;
			if (!sysSvc_->archiveLog(
					eventSource, backupName.c_str(), mode, result)) {
				response.setJson(result);
				response.setBadRequestError();
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
		else if (request.pathElements_.size() == 2 &&
				 request.pathElements_[1] == "periodicCheckpoint") {
			picojson::object errorInfo;
			int32_t errorNo = 0;
			std::string reason;
			bool flag = true;
			if (request.method_ != EBB_POST) {
				response.setMethodError();
				return;
			}
			if (request.parameterMap_.find("enable") !=
				request.parameterMap_.end()) {
				if (request.parameterMap_["enable"] == "true") {
					flag = true;
				}
				else if (request.parameterMap_["enable"] == "false") {
					flag = false;
				}
				else {
					errorNo = WEBAPI_CP_OTHER_REASON;
					reason = "unknown parameter value: " + request.parameterMap_["enable"];
					errorInfo["errorStatus"] =
						picojson::value(static_cast<double>(errorNo));
					errorInfo["reason"] = picojson::value(reason);
					response.setJson(picojson::value(errorInfo));
					response.setBadRequestError();
					return;
				}
			}
			sysSvc_->setPeriodicCheckpointFlag(flag);
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
		else if (request.pathElements_.size() >= 2 &&
				 request.pathElements_[1] == "sqlTempStore") {

			if (request.method_ != EBB_GET) {
				response.setMethodError();
				return;
			}

			bool withReset = false;
			if (request.parameterMap_.find("withReset") !=
					request.parameterMap_.end()) {
				if (request.parameterMap_["withReset"] == "1"
						|| request.parameterMap_["withReset"] == "true") {
					withReset = true;
				}
			}
			picojson::value result;
			sysSvc_->getSqlTempStoreStats(result);

			if (withReset) {
				LocalTempStore &store =
						clsSvc_->getSQLService()->getExecutionManager()->getJobManager()->getStore();
				store.clearAllGroupStats();
			}

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
		request.pathElements_[1] == "expirationCheck") {
		const std::vector<std::string> namePath(
			request.pathElements_.begin() + 2, request.pathElements_.end());

		Timestamp time = 0;
		if (request.parameterMap_.find("time") !=
			request.parameterMap_.end()) {
			try {
				const util::DateTime inputTime(request.parameterMap_["time"].c_str(), false);
				time = inputTime.getUnixTime();
			}
			catch (std::exception& e) {
				response.setBadRequestError();
				return;
			}
		}
		else {
			response.setBadRequestError();
			return;
		}
		bool force = false;

		txnSvc_->requestUpdateDataStoreStatus(eventSource, time, force);
		}
		else if (request.pathElements_.size() == 2 && request.pathElements_[1] == "redo") {

			picojson::value result;
			RestContext restCxt(result);
			int64_t pId = -1;
			int64_t requestId = -1;
			std::string uuidString;

			const char* checkParam = "partitionId";
			if (request.parameterMap_.find(checkParam) !=
				request.parameterMap_.end()) {
				if (!response.checkParamValue(checkParam, pId, true)) {
					return;
				}
			}
			else {
				util::NormalOStringStream oss;
				oss << "PartitionId must be specified";
				response.setError(WEBAPI_CP_OTHER_REASON, oss.str().c_str(), 400);
				return;
			}

			checkParam = "uuid";
			if (request.parameterMap_.find(checkParam) !=
				request.parameterMap_.end()) {
				uuidString = request.parameterMap_[checkParam];
			}

			checkParam = "requestId";
			if (request.parameterMap_.find(checkParam) !=
				request.parameterMap_.end()) {
				if (!response.checkParamValue(checkParam, requestId, false)) {
					return;
				}
			}

			if (request.method_ == EBB_GET) {
				if (!syncSvc_->getRedoManager().getStatus(
						static_cast<PartitionId>(pId), uuidString, requestId,
						restCxt)) {
					response.setJson(result);
					response.setBadRequestError();
					return;
				}
				response.setJson(result);
			}
			else if (request.method_ == EBB_POST) {

				std::string mode;
				const char* checkParam = "mode";
				if (request.parameterMap_.find(checkParam) !=
					request.parameterMap_.end()) {
					mode = request.parameterMap_[checkParam];
				}
				else {
					util::NormalOStringStream oss;
					oss << "Mode must be specified";
					response.setError(WEBAPI_CP_OTHER_REASON, oss.str().c_str(), 400);
					return;
				}

				if (mode == "redo") {
					std::string logFilePath;
					std::string logFileName;
					int64_t startLsn = -1;
					int64_t endLsn = -1;
					bool forceApply = false;

					checkParam = "logFilePath";
					if (request.parameterMap_.find(checkParam) !=
						request.parameterMap_.end()) {
						logFilePath = request.parameterMap_[checkParam];
					}
					else {
						util::NormalOStringStream oss;
						oss << "logFilePath must be specified";
						response.setError(WEBAPI_CP_OTHER_REASON, oss.str().c_str(), 400);
						return;
					}

					checkParam = "logFileName";
					if (request.parameterMap_.find(checkParam) !=
						request.parameterMap_.end()) {
						logFileName = request.parameterMap_[checkParam];
					}
					else {
						util::NormalOStringStream oss;
						oss << "logFileName must be specified";
						response.setError(WEBAPI_CP_OTHER_REASON, oss.str().c_str(), 400);
						return;
					}

					checkParam = "startLsn";
					if (request.parameterMap_.find(checkParam) !=
						request.parameterMap_.end()) {
						if (!response.checkParamValue(checkParam, startLsn, false)) {
							return;
						}
					}

					checkParam = "endLsn";
					if (request.parameterMap_.find(checkParam) !=
						request.parameterMap_.end()) {
						if (!response.checkParamValue(checkParam, endLsn, false)) {
							return;
						}
					}

					checkParam = "force";
					if (request.parameterMap_.find(checkParam) !=
						request.parameterMap_.end()) {
						if (request.parameterMap_[checkParam] == "true") {
							forceApply = true;
						}
						else if (request.parameterMap_[checkParam] != "false") {
							util::NormalOStringStream oss;
							oss << "Invalid force value";
							response.setError(WEBAPI_CP_OTHER_REASON, oss.str().c_str(), 400);
							response.setBadRequestError();
							return;
						}
					}
					
					if (!syncSvc_->getRedoManager().put(
							eventSource, alloc_, static_cast<PartitionId>(pId),
							logFilePath, logFileName, startLsn, endLsn,
							uuidString, requestId, forceApply, restCxt)) {
						response.setJson(result);
						response.setBadRequestError();
						return;
					}
					response.setJson(result);
				}
				else if (mode == "cancel") {
					if (!syncSvc_->getRedoManager().cancel(
							eventSource, alloc_, static_cast<PartitionId>(pId),
							uuidString, restCxt, requestId)) {
						response.setJson(result);
						response.setBadRequestError();
						return;
					}
				}
				else {
					response.setBadRequestError();
				}
			}
			else {
				response.setMethodError();
				return;
			}
		}
		else if (request.pathElements_.size() == 2 && request.pathElements_[1] == "autoArchive") {
			if (request.method_ == EBB_GET) {
				picojson::value result;
				cpSvc_->getAutoArchiveCommandParam(result);
				response.setJson(result);
			}
			else if (request.method_ == EBB_POST) {
				BackupOption option;
				option.logArchive_ = true;
				option.logDuplicate_ = true;
				option.stopOnDuplicateError_ = true;
				option.incrementalBackupLevel_ = -1;	 
				option.isIncrementalBackup_ = false;  
				option.isCumulativeBackup_ = false;   
				option.skipBaseline_ = false; 

				std::string archiveName;
				if (request.parameterMap_.find("archiveName") !=
					request.parameterMap_.end()) {
					archiveName = request.parameterMap_["archiveName"];
				}

				if (request.parameterMap_.find("duplicateLog") !=
					request.parameterMap_.end()) {
					if (request.parameterMap_["duplicateLog"] == "1") {
						option.logDuplicate_ = true;
					}
					else {
						option.logDuplicate_ = false;
					}
				}

				if (request.parameterMap_.find("stopOnDuplicateError") !=
					request.parameterMap_.end()) {
					if (option.logDuplicate_  && request.parameterMap_["stopOnDuplicateError"] == "1") {
						option.stopOnDuplicateError_ = true;
					}
					else {
						option.stopOnDuplicateError_ = false;
					}
				}

				if (request.parameterMap_.find("skipBaseline") !=
					request.parameterMap_.end()) {
					if (option.logDuplicate_ && request.parameterMap_["skipBaseline"] == "1") {
						option.skipBaseline_ = true;
					}
					else {
						option.skipBaseline_ = false;
					}
				}
				GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
					"Auto archive node called (archiveName="
					<< archiveName
					<< ", stopOnDuplicateError=" << ValueFormatter()(option.stopOnDuplicateError_)
					<< ", skipBaseline=" << ValueFormatter()(option.skipBaseline_)
					<< ", logDuplicate=" << ValueFormatter()(option.logDuplicate_)
					<< ")");

				picojson::value result;
				try {
					clsMgr_->checkNodeStatus();
				}
				catch (UserException& e) {
					util::NormalOStringStream oss;
					oss << "unacceptable condition (status="
						<< clsMgr_->statusInfo_.getSystemStatus() << ")";
					response.setError(WEBAPI_CP_OTHER_REASON, oss.str().c_str(), 400);
				}
				try {
					if (!cpSvc_->requestOnlineBackup(eventSource, archiveName, CP_BACKUP, option, result)) {
						response.setJson(result);
						response.setBadRequestError();
						return;
					}
				}
				catch (UserException& e) {
					UTIL_TRACE_EXCEPTION(SYSTEM_SERVICE, e,
						"Auto archive node failed (name=" << archiveName << ", detail="
						<< GS_EXCEPTION_MESSAGE(e) << ")");
					response.setError(WEBAPI_CP_OTHER_REASON, "archive failed", 400);
				}
			}
			else {
				response.setMethodError();
				return;
			}
		}
		else if (request.pathElements_.size() == 2 && request.pathElements_[1] == "standby") {
		if (request.method_ == EBB_GET) {
			picojson::value result;
			clsMgr_->getStandbyInfo().getMode(result);
			response.setJson(result);
			return;
		}
		else if (request.method_ == EBB_POST) {
			bool isStandby = false;
			const char* checkParam = "mode";
			if (request.parameterMap_.find(checkParam) !=
				request.parameterMap_.end()) {
				if (request.parameterMap_[checkParam] == "true") {
					isStandby = true;
				}
				else if (request.parameterMap_[checkParam] != "false") {
					util::NormalOStringStream oss;
					oss << "Invalid mode value";
					response.setError(WEBAPI_CP_OTHER_REASON, oss.str().c_str(), 400);
					response.setBadRequestError();
					return;
				}
			}
			else {
				util::NormalOStringStream oss;
				oss << "Failed to change standby mode (reason = not specifed mode)";
				response.setError(WEBAPI_CONFIG_SET_ERROR, oss.str().c_str(), 400);
			}
			picojson::value result;
			RestContext restCxt(result);
			if (!clsSvc_->changeStandbyStatus(eventSource, alloc_, isStandby, restCxt)) {
				response.setJson(result);
				response.setBadRequestError();
				return;
			}
			return;
		}
		else {
			response.setMethodError();
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
					}
					GS_TRACE_INFO(CLUSTER_OPERATION, GS_TRACE_CS_CLUSTER_STATUS,
						pt_->dumpDatas(true));
				}
			}
		}
		else if (request.pathElements_.size() == 2 &&
				request.pathElements_[1] == "sql") {
			SQLExecutionManager *executionManager
					= clsSvc_->getSQLService()->getExecutionManager();
			picojson::value jsonOutValue;
			SQLProfiler profs(alloc_);
			executionManager->getProfiler(alloc_, profs);
			JsonUtils::OutStream out(jsonOutValue);
			util::ObjectCoder().encode(out, profs);

			if (request.parameterMap_.find("callback") !=
				request.parameterMap_.end()) {
				response.setJson(request.parameterMap_["callback"], jsonOutValue);
			}
			else {
				response.setJson(jsonOutValue);
			}
		}
		else if (request.pathElements_.size() == 3 &&
				request.pathElements_[1] == "sql" && request.pathElements_[2] == "job") {
			JobManager *jobManager = clsSvc_->getSQLService()->getExecutionManager()->getJobManager();
			StatJobIdList infoList(alloc_);
			picojson::value jsonOutValue;
			jobManager->getJobIdList(alloc_, infoList);
			JsonUtils::OutStream out(jsonOutValue);
			util::ObjectCoder().encode(out, infoList);
			if (request.parameterMap_.find("callback") !=
				request.parameterMap_.end()) {
				response.setJson(request.parameterMap_["callback"], jsonOutValue);
			}
			else {
				response.setJson(jsonOutValue);
			}
		}
		else if (request.pathElements_.size() == 4 &&
				request.pathElements_[1] == "sql"
				&& request.pathElements_[2] == "cache"
				&& request.pathElements_[3] == "refresh") {

			if (request.method_ != EBB_POST) {
				response.setMethodError();
				return;
			}
			
			SQLExecutionManager *executionManager
							= clsSvc_->getSQLService()->getExecutionManager();
			executionManager->refreshSchemaCache();

		}
		else if (request.pathElements_.size() == 4 &&
				request.pathElements_[1] == "sql"
				&& request.pathElements_[2] == "job"
				&& request.pathElements_[3] == "reset") {

			if (request.method_ != EBB_POST) {
				response.setMethodError();
				return;
			}

			JobManager *jobManager = clsSvc_->getSQLService()
					->getExecutionManager()->getJobManager();
			jobManager->resetSendCounter(alloc_);
		}
		else if (request.pathElements_.size() == 3 &&
				request.pathElements_[1] == "sql" && request.pathElements_[2] == "cache") {
			if (request.method_ != EBB_GET) {
				response.setMethodError();
				return;
			}
			std::string dbName;
			if (request.parameterMap_.find("dbName") !=
					request.parameterMap_.end()) {
				dbName = request.parameterMap_["dbName"];
				SQLExecutionManager *executionManager
							= clsSvc_->getSQLService()->getExecutionManager();
				util::NormalOStringStream oss;
				executionManager->dumpTableCache(oss, dbName.c_str());
				std::cout << oss.str().c_str() << std::endl;

				if (request.parameterMap_.find("cacheSize") !=
						request.parameterMap_.end()) {
					int32_t cacheSize = atoi(request.parameterMap_["cacheSize"].c_str());
					executionManager->resizeTableCache(dbName.c_str(), cacheSize);
				}
			}
		}
		else if (request.pathElements_.size() == 4 &&
				 request.pathElements_[1] == "sql" && request.pathElements_[2] == "job" 
						&& request.pathElements_[3] == "profs") {
			if (request.method_ != EBB_GET) {
				response.setMethodError();
				return;
			}
			int32_t mode = 0;
			if (request.parameterMap_.find("mode") !=
				request.parameterMap_.end()) {
				if (request.parameterMap_["mode"] == "1") {
					mode = 1;
				}
			}

			util::String jobIdString(alloc_);
			if (request.parameterMap_.find("jobId") !=
				request.parameterMap_.end()) {
				jobIdString = request.parameterMap_["jobId"].c_str();
			}
			JobId jobId;
			JobId *currentJobId = NULL;

			if (!jobIdString.empty()) {
				try {
					jobId.parse(alloc_, jobIdString);
					currentJobId = &jobId;
				}
				catch (std::exception &e) {
					currentJobId = NULL;
				}
			}

			JobManager *jobManager = clsSvc_->getSQLService()->getExecutionManager()->getJobManager();
			StatJobProfiler profs(alloc_);
			profs.currentTime_ = CommonUtility::getTimeStr(util::DateTime::now(false).getUnixTime()).c_str();
			jobManager->getProfiler(alloc_, profs, mode, currentJobId);
			picojson::value jsonOutValue;
			JsonUtils::OutStream out(jsonOutValue);
			util::ObjectCoder().encode(out, profs);

			if (request.parameterMap_.find("callback") !=
				request.parameterMap_.end()) {
				response.setJson(request.parameterMap_["callback"], jsonOutValue);
			}
			else {
				response.setJson(jsonOutValue);
			}
		}
		else if (request.pathElements_.size() == 4 &&
				 request.pathElements_[1] == "sql" && request.pathElements_[2] == "job" 
						&& request.pathElements_[3] == "cancel") {
			if (request.method_ != EBB_POST) {
				response.setMethodError();
				return;
			}
			util::String jobIdString(alloc_);
			bool isError = false;
			if (request.parameterMap_.find("jobId") !=
				request.parameterMap_.end()) {
				jobIdString = request.parameterMap_["jobId"].c_str();
			}

			util::String startTimeString(alloc_);
			if (request.parameterMap_.find("startTime") !=
				request.parameterMap_.end()) {
				startTimeString = request.parameterMap_["startTime"].c_str();
			}
			CancelOption option(startTimeString);
			if (request.parameterMap_.find("forced") !=
					request.parameterMap_.end()) {

				const std::string retStr = request.parameterMap_["forced"];
				if (retStr == "true") {
					option.forced_ = true;
				}
			}

			if (request.parameterMap_.find("varTotalSize") !=
					request.parameterMap_.end()) {
				int64_t varTotalSize = atoi(request.parameterMap_["varTotalSize"].c_str());

				if (varTotalSize > 0) {
					option.allocateMemory_ = varTotalSize;
				}
			}

			if (request.parameterMap_.find("duration") !=
				request.parameterMap_.end()) {
				int64_t duration = atoi(request.parameterMap_["duration"].c_str());

				if (duration > 0) {
					int64_t currentTime = util::DateTime::now(false).getUnixTime();
					if (currentTime >= duration * 1000) {
						option.limitStartTime_ = currentTime - duration * 1000;
					}
				}
			}
			JobManager *jobManager = clsSvc_->getSQLService()->getExecutionManager()->getJobManager();
			Event::Source eventSource(varSizeAlloc_);
			util::Vector<JobId> jobIdList(alloc_);
			if (!jobIdString.empty()) {
				JobId jobId;
				try {
					jobId.parse(alloc_, jobIdString);
					jobIdList.push_back(jobId);
				}
				catch (std::exception &e) {
					GS_TRACE_ERROR(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED, "Invalid jobId format=" << jobIdString);
					isError = true;
				}
			}
			if (!isError) {
				if (jobIdList.size() == 1) {
					GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
						"Cancel job called (jobId=" << jobIdList[0] << ")");
				}
				else {
					util::NormalOStringStream oss;
					oss <<  "Cancel job list called(jobIdList=";
					CommonUtility::dumpValueList(oss, jobIdList);
					oss << ")";
					GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
						oss.str().c_str());
				}
				jobManager->cancelAll(eventSource, alloc_, jobIdList, option);
			}
		}
		else if (request.pathElements_.size() == 3 &&
				 request.pathElements_[1] == "sql" && request.pathElements_[2] == "cancel") {
			if (request.method_ != EBB_POST) {
				response.setMethodError();
				return;
			}
			util::String clientIdString(alloc_);
			bool isError = false;
			if (request.parameterMap_.find("clientId") !=
				request.parameterMap_.end()) {
				clientIdString = request.parameterMap_["clientId"].c_str();
			}

			util::String startTimeString(alloc_);
			if (request.parameterMap_.find("startTime") !=
				request.parameterMap_.end()) {
				startTimeString = request.parameterMap_["startTime"].c_str();
			}
			CancelOption option(startTimeString);
			if (request.parameterMap_.find("duration") !=
				request.parameterMap_.end()) {
				int64_t duration = atoi(request.parameterMap_["duration"].c_str());

				if (duration > 0) {
					int64_t currentTime = util::DateTime::now(false).getUnixTime();
					if (currentTime >= duration * 1000) {
						option.limitStartTime_ = currentTime - duration * 1000;
					}
				}
			}

			SQLExecutionManager *executionManager = clsSvc_->getSQLService()->getExecutionManager();
			Event::Source eventSource(varSizeAlloc_);
			util::Vector<ClientId> clientIdList(alloc_);
			if (!clientIdString.empty()) {
				JobId jobId;
				try {
					clientIdString.append(":0:0");
					jobId.parse(alloc_, clientIdString, false);
					clientIdList.push_back(jobId.clientId_);
				}
				catch (std::exception &e) {
					isError = true;
				}
			}
			if (!isError) {
				if (clientIdList.size() == 1) {
					GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
						"Cancel sql called (jobId=" << clientIdList[0] << ")");
				}
				else {
					util::NormalOStringStream oss;
					oss << "Cancel sql list called(jobIdList=";
					CommonUtility::dumpValueList(oss, clientIdList);
					oss << ")";
					GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
						oss.str().c_str());
				}
				executionManager->cancelAll(eventSource, alloc_, clientIdList, option);
			}
		}
		else if (request.pathElements_.size() == 3 &&
			request.pathElements_[1] == "request" &&
			request.pathElements_[2] == "constraint") {
			DatabaseManager &dbManager = txnSvc_->getManager()->getDatabaseManager();
			DatabaseId dbId = UNDEF_DBID;
			int32_t mode = DatabaseManager::ExecutionConstraint::MODE_UNDEF;
			if (request.parameterMap_.find("dbId") !=
				request.parameterMap_.end()) {
				if (!response.checkParamValue("dbId", dbId, false)) {
					return;
				}
				if (dbId < 0 || dbId > MAX_DBID) {
					dbId = UNDEF_DBID;
				}
			}
			if (request.method_ == EBB_GET) {
				picojson::value result;
				dbManager.getExecutionConstraintList(dbId, result);
				std::string jsonString(picojson::value(result).serialize());
				if (request.parameterMap_.find("callback") !=
					request.parameterMap_.end()) {
					response.setJson(request.parameterMap_["callback"], result);
				}
				else {
					response.setJson(result);
				}
			}
			else {
				if (request.parameterMap_.find("mode") !=
					request.parameterMap_.end()) {
					const std::string retStr = request.parameterMap_["mode"];
					mode = DatabaseManager::ExecutionConstraint::getMode(retStr);
					if (mode == DatabaseManager::ExecutionConstraint::MODE_UNDEF) {
						util::NormalOStringStream oss;
						oss << "Invalid mode name=" << retStr;
						response.setError(WEBAPI_DB_INVALID_PARAMETER, oss.str().c_str(), 400);
						return;
					}
					if (mode == DatabaseManager::ExecutionConstraint::MODE_DENY) {
						if (request.parameterMap_.find("denyCommand") !=
							request.parameterMap_.end()) {
							const std::string retStr = request.parameterMap_["denyCommand"];
							mode = DatabaseManager::ExecutionConstraint::getDenyMode(retStr);
							if (mode == DatabaseManager::ExecutionConstraint::MODE_UNDEF) {
								util::NormalOStringStream oss;
								oss << "Invalid command name=" << retStr;
								response.setError(WEBAPI_DB_INVALID_PARAMETER, oss.str().c_str(), 400);
								return;
							}
						}
						else {
							mode = DatabaseManager::ExecutionConstraint::getDefaultDenyMode();
						}
					}
				}
				int64_t delayStartInterval = 0;
				int64_t delayExecutionInterval = 0;
				bool intervalSet1 = false;
				bool intervalSet2 = false;
				if (dbId >= 0 && request.parameterMap_.find("requestDelayTime") !=
					request.parameterMap_.end()) {
					if (!response.checkParamValue("requestDelayTime", delayStartInterval, true)) {
						return;
					}
					intervalSet1 = true;
				}
				if (dbId >= 0 && request.parameterMap_.find("eventDelayTime") !=
					request.parameterMap_.end()) {
					if (!response.checkParamValue("eventDelayTime", delayExecutionInterval, true)) {
						return;
					}
					intervalSet2 = true;
				}

				if (mode != DatabaseManager::ExecutionConstraint::MODE_UNDEF) {
					if (mode == DatabaseManager::ExecutionConstraint::MODE_DELAY
						&& !intervalSet1 && !intervalSet2) {
						util::NormalOStringStream oss;
						oss << "Invalid paramter, mode is DEALY but delay time is not defined";
						response.setError(WEBAPI_DB_INVALID_PARAMETER, oss.str().c_str(), 400);
						return;
					}
					else {
						dbManager.putExecutionConsraint(dbId, mode,
							static_cast<uint32_t>(delayStartInterval), static_cast<uint32_t>(delayExecutionInterval));
					}
				}
			}
		}

		else if (request.pathElements_.size() > 3 &&
				request.pathElements_[1] == "sql" &&
				request.pathElements_[2] == "processor") {
			if (request.pathElements_.size() > 4 &&
					request.pathElements_[3] == "profile") {
				if (request.method_ != EBB_GET) {
					response.setMethodError();
					return;
				}

				picojson::value result;
				int64_t id;
				do {
					if (request.pathElements_[4] == "list") {
						if (sysSvc_->getSQLProcessorProfile(
								alloc_, varSizeAlloc_, result, NULL)) {
							break;
						}
					}
					if (request.pathElements_[4] == "global") {
						if (sysSvc_->getSQLProcessorGlobalProfile(result)) {
							break;
						}
					}
					else if (util::LexicalConverter<int64_t>()(
							request.pathElements_[4].c_str(), id)) {
						if (sysSvc_->getSQLProcessorProfile(
								alloc_, varSizeAlloc_, result, &id)) {
							break;
						}
					}
					response.setBadRequestError();
					return;
				}
				while (false);

				response.setJson(result);
			}
			else if (request.pathElements_.size() > 4 &&
					request.pathElements_[3] == "config") {

				bool forProfiler;
				if (request.pathElements_[4] == "global") {
					forProfiler = false;
				}
				else if (request.pathElements_[4] == "profile") {
					forProfiler = true;
				}
				else {
					response.setBadRequestError();
					return;
				}

				typedef WebAPIRequest::ParameterMap::const_iterator ParamIt;
				const WebAPIRequest::ParameterMap &map =
						request.parameterMap_;

				if (request.method_ == EBB_POST) {
					for (ParamIt it = map.begin(); it != map.end(); ++it) {
						if (!sysSvc_->setSQLProcessorConfig(
								it->first.c_str(), it->second.c_str(), forProfiler)) {
							response.setBadRequestError();
							return;
						}
					}
				}
				else if (request.method_ == EBB_GET) {
					picojson::value result;
					if (!sysSvc_->getSQLProcessorConfig(forProfiler, result)) {
						response.setBadRequestError();
						return;
					}
					response.setJson(result);
				}
				else {
					response.setMethodError();
					return;
				}
			}
			else if (request.pathElements_.size() >= 4 &&
					request.pathElements_[3] == "partial") {
				typedef WebAPIRequest::ParameterMap::const_iterator ParamIt;
				const WebAPIRequest::ParameterMap &map =
						request.parameterMap_;

				if (request.method_ == EBB_POST) {
					SQLProcessorConfig::PartialOption option;
					{
						const ParamIt it = map.find("followingCount");
						if (it == map.end() || !util::LexicalConverter<int32_t>()(
								it->second, option.followingCount_)) {
							response.setBadRequestError();
							return;
						}
					}

					{
						const ParamIt it = map.find("submissionCode");
						if (it == map.end() || !util::LexicalConverter<int64_t>()(
								it->second, option.submissionCode_)) {
							response.setBadRequestError();
							return;
						}
					}

					{
						const ParamIt it = map.find("activated");
						if (it == map.end() || !util::LexicalConverter<bool>()(
								it->second, option.activated_)) {
							response.setBadRequestError();
							return;
						}
					}

					{
						const ParamIt it = map.find("trialCount");
						if (it != map.end() && !util::LexicalConverter<int32_t>()(
								it->second, option.trialCount_)) {
							response.setBadRequestError();
							return;
						}
					}

					typedef SQLProcessorConfig::Manager Manager;
					if (!Manager::isEnabled()) {
						response.setBadRequestError();
						return;
					}

					if (!Manager::getInstance().monitorPartial(option)) {
						response.setBadRequestError();
						return;
					}
				}
				else if (request.method_ == EBB_GET) {
					picojson::value result;
					if (!sysSvc_->getSQLProcessorPartialStatus(result)) {
						response.setBadRequestError();
						return;
					}
					response.setJson(result);
				}
				else {
					response.setMethodError();
					return;
				}
			}
			else if (request.pathElements_.size() >= 4 &&
					request.pathElements_[3] == "simulation") {
				typedef WebAPIRequest::ParameterMap::const_iterator ParamIt;
				const WebAPIRequest::ParameterMap &map =
						request.parameterMap_;

				if (request.method_ == EBB_POST) {
					picojson::value valueStorage;
					const picojson::value *value;
					if (request.jsonValue_.get() != NULL) {
						value = request.jsonValue_.get();
					}
					else {
						ParamIt it = map.find("value");
						if (it == map.end()) {
							response.setBadRequestError();
							return;
						}

						const std::string &valueStr = it->second;
						const std::string &err = JsonUtils::parseAll(
								valueStorage,
								valueStr.c_str(),
								valueStr.c_str() + valueStr.size());
						if (!err.empty()) {
							response.setBadRequestError();
							return;
						}

						value = &valueStorage;
					}

					if (!sysSvc_->setSQLProcessorSimulation(alloc_, *value)) {
						response.setBadRequestError();
						return;
					}
				}
				else if (request.method_ == EBB_GET) {
					picojson::value result;
					if (!sysSvc_->getSQLProcessorSimulation(alloc_, result)) {
						response.setBadRequestError();
						return;
					}
					response.setJson(result);
				}
				else {
					response.setMethodError();
					return;
				}
			}
			else {
				response.setBadRequestError();
				return;
			}
		}
		else if (request.pathElements_.size() > 3 &&
				request.pathElements_[1] == "sql" &&
				request.pathElements_[2] == "compiler") {
			if (request.pathElements_.size() > 4 &&
					request.pathElements_[3] == "profile") {
				if (request.method_ != EBB_GET) {
					response.setMethodError();
					return;
				}

				int32_t index;
				if (!util::LexicalConverter<int32_t>()(
						request.pathElements_[4], index)) {
					response.setBadRequestError();
					return;
				}

				typedef WebAPIRequest::ParameterMap::const_iterator ParamIt;
				const WebAPIRequest::ParameterMap &map =
						request.parameterMap_;
				util::Set<util::String> filteringSet(alloc_);
				for (ParamIt it = map.begin(); it != map.end(); ++it) {
					filteringSet.insert(util::String(it->first.c_str(), alloc_));
				}

				picojson::value result;
				if (!sysSvc_->getSQLCompilerProfile(
						alloc_, index, filteringSet, result)) {
					response.setBadRequestError();
					return;
				}
				response.setJson(result);
			}
			else if (request.pathElements_.size() > 4 &&
					request.pathElements_[3] == "config") {
				typedef WebAPIRequest::ParameterMap::const_iterator ParamIt;
				const WebAPIRequest::ParameterMap &map =
						request.parameterMap_;

				const char8_t *category = request.pathElements_[4].c_str();

				if (request.method_ == EBB_POST) {
					for (ParamIt it = map.begin(); it != map.end(); ++it) {
						if (!sysSvc_->setSQLCompilerConfig(
								category, it->first.c_str(), it->second.c_str())) {
							response.setBadRequestError();
							return;
						}
					}
				}
				else if (request.method_ == EBB_GET) {
					picojson::value result;
					if (!sysSvc_->getSQLCompilerConfig(category, result)) {
						response.setBadRequestError();
						return;
					}
					response.setJson(result);
				}
				else {
					response.setMethodError();
					return;
				}
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
	else if (request.pathElements_.size() >= 2 &&
			 request.pathElements_[0] == "cluster") {
		picojson::value result;
		picojson::object &tmp = picojsonUtil::setJsonObject(result);

		NodeId masterNodeId = pt_->getMaster();

		bool isLoadBalance = (request.pathElements_.size() == 2 &&
							  request.pathElements_[1] == "loadBalance");
		if (masterNodeId != 0 && !isLoadBalance) {
			const ServiceTypeInfo &addressType =
					getDefaultAddressType(request.parameterMap_);
			if (addressType.first < 0) {
				response.setBadRequestError();
				return;
			}

			picojson::object master;
			std::string commands = "[WEB API] ";
			for (size_t pos = 0; pos < request.pathElements_.size(); pos++) {
				commands += "/";
				commands += request.pathElements_[pos];
			}
			if (masterNodeId > 0) {
				getServiceAddress(pt_, masterNodeId, master, addressType);
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
		else if (request.pathElements_.size() == 2 &&
				 request.pathElements_[1] == "increase") {
			if (request.method_ != EBB_POST) {
				response.setMethodError();
				return;
			}
			sysSvc_->increaseCluster(eventSource, alloc_);
		}
		else if (request.pathElements_.size() == 2 &&
				 request.pathElements_[1] == "decrease") {
			if (request.method_ != EBB_POST) {
				response.setMethodError();
				return;
			}

			bool autoLeave = true;
			if (request.parameterMap_.find("autoLeave") !=
				request.parameterMap_.end()) {
				const std::string autoLeaveStr =
					request.parameterMap_["autoLeave"];
				if (autoLeaveStr == "false") {
					autoLeave = false;
				}
				else if (autoLeaveStr != "true") {
					response.setBadRequestError();
					return;
				}
			}

			picojson::value result;
			sysSvc_->decreaseCluster(eventSource, alloc_, result, autoLeave, 1);
			response.setJson(result);
		}
		else if (request.pathElements_.size() == 2 &&
				 request.pathElements_[1] == "failover") {
			if (request.method_ != EBB_POST) {
				response.setMethodError();
				return;
			}

			bool isRepair = false;

			clsMgr_->setUpdatePartition();

			if (request.parameterMap_.find("repair") !=
				request.parameterMap_.end()) {
				const std::string retStr = request.parameterMap_["repair"];
				if (retStr == "true") {
					clsMgr_->setRepairPartition();
					isRepair = true;
					GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
						"Failover with repair called.");
				}
			}

			if (!isRepair) {
				GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
					"Failover called.");
			}
		}
		else if (request.pathElements_.size() == 2 &&
				 request.pathElements_[1] == "loadBalance") {
			if (request.method_ == EBB_GET) {
				const ServiceTypeInfo &addressType =
						getDefaultAddressType(request.parameterMap_);
				if (addressType.first < 0) {
					response.setBadRequestError();
					return;
				}
				picojson::value result;
				sysSvc_->getGoalPartitions(alloc_, addressType, result);
				std::string jsonString(picojson::value(result).serialize());
				if (request.parameterMap_.find("callback") !=
					request.parameterMap_.end()) {
					response.setJson(request.parameterMap_["callback"], result);
				}
				else {
					response.setJson(result);
				}
			}
			else {
				const ServiceTypeInfo& addressType =
					getDefaultAddressType(request.parameterMap_);
				if (addressType.first < 0) {
					response.setBadRequestError();
					return;
				}
				bool currentMode = clsMgr_->checkLoadBalance();
				bool nextMode = true;
				bool isSetted = false;
				if (request.parameterMap_.find("enable") !=
					request.parameterMap_.end()) {
					const std::string retStr = request.parameterMap_["enable"];
					if (retStr == "true") {
						nextMode = true;
						isSetted = true;
					}
					else if (retStr == "false") {
						nextMode = false;
						isSetted = true;
					}
					else {
						response.setMethodError();
						return;
					}
				}

				if (request.parameterMap_.find("autoGoal") !=
					request.parameterMap_.end()) {
					const std::string retStr = request.parameterMap_["autoGoal"];
					if (retStr == "true") {
						clsMgr_->setAutoGoal(true);
					}
					else if (retStr == "false") {
						clsMgr_->setAutoGoal(false);
					}
					else {
						response.setMethodError();
						return;
					}
				}

				if (request.parameterMap_.find("assignmentRule") !=
					request.parameterMap_.end()) {
					const std::string retStr = request.parameterMap_["assignmentRule"];
					if (retStr == "ROUNDROBIN") {
						pt_->assignRoundRobinGoal(alloc_);
						return;
					}
					else if (retStr == "DEFAULT") {
					}
					else {
						response.setMethodError();
						return;
					}
					return;
				}

				if (isSetted) {
					if (nextMode == true) {
						clsMgr_->setUpdatePartition();
					}
					if (currentMode != nextMode) {
						clsMgr_->setLoadBalance(nextMode);
						if (nextMode == true) {
							GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
								"Load balancer configuration is called, "
								"current:INACTIVE, next:ACTIVE");
						}
						else {
							GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
								"Load balancer configuration is called, "
								"current:ACTIVE, next:INACTIVE");
						}
					}
					else {
						if (nextMode == true) {
							GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
								"Load balancer configuration is called, keep ACTIVE");
						}
						else {
							GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_WEB_API_CALLED,
								"Load balancer configuration is called, keep INACTIVE");
						}
					}
				}
				const picojson::value *paramValue = request.jsonValue_.get();
				if (!sysSvc_->setGoalPartitions(alloc_, paramValue, result)) {
					response.setJson(result);
					response.setBadRequestError();
				}
			}
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

bool SystemService::setGoalPartitions(util::StackAllocator &alloc, 
		const picojson::value *paramValue, picojson::value &result) {
	std::string reason;
	try {
		pt_->setGoalPartitions(alloc, paramValue);
		return true;
	}
	catch (UserException &e) {
		int32_t exceptionNo = e.getErrorCode();
		if (e.hasMessage()) {
			util::NormalOStringStream ss;
			e.formatMessage(ss);
			reason = ss.str().c_str();
		}
		int32_t errorNo = 0;
		util::NormalOStringStream ss;
		switch (exceptionNo) {
			case GS_ERROR_SC_GOAL_DUPLICATE_PARTITION :
				errorNo = WEBAPI_CS_GOAL_DUPLICATE_PARTITION;
				break;
			case GS_ERROR_SC_GOAL_NOT_OWNER :
				errorNo = WEBAPI_CS_GOAL_NOT_OWNER;
				break;
			case GS_ERROR_SC_GOAL_RESOLVE_NODE_FAILED :
				errorNo = WEBAPI_CS_GOAL_RESOLVE_NODE_FAILED;
				break;
			case GS_ERROR_SC_GOAL_NOT_CLUSTERED_NODE :
				errorNo = WEBAPI_CS_GOAL_NOT_CLUSTERED_NODE;
				break;
			case GS_ERROR_SC_GOAL_INVALID_FORMAT :
				errorNo = WEBAPI_CS_GOAL_INVALID_FORMAT;
				break;
			default:
				errorNo = WEBAPI_CS_GOAL_INVALID_FORMAT;
				break;
		}
		picojson::object errorInfo;
		errorInfo["errorStatus"] =
			picojson::value(static_cast<double>(errorNo));
		errorInfo["reason"] = picojson::value(reason);
		result = picojson::value(errorInfo);
		UTIL_TRACE_EXCEPTION_INFO(SYSTEM_SERVICE, e,
			"Set goal failed (reason="
				<< reason << ", detail=" << GS_EXCEPTION_MESSAGE(e) << ")");
		return false;
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION_INFO(SYSTEM_SERVICE, e,
			"Set goal failed (reason="
				<< reason << ", detail=" << GS_EXCEPTION_MESSAGE(e) << ")");
		return false;
	}
}

SystemService::ServiceTypeInfo
SystemService::ListenerSocketHandler::getDefaultAddressType(
		const std::map<std::string, std::string> &parameterMap) const {
	const ServiceTypeInfo info(
			ServiceTypeInfo::first_type(DEFAULT_ADDRESS_TYPE), secure_);
	return resolveAddressType(parameterMap, info);
}

/*!
	@brief Reads socket
*/
bool SystemService::ListenerSocketHandler::readOnce(
		AbstractSocket &socket, util::NormalXArray<char8_t> &buffer) {
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

SocketFactory& SystemService::ListenerSocketHandler::resolveSocketFactory(
		SystemService *sysSvc, bool secure) {
	SocketFactory *factory = secure ?
			sysSvc->secureSocketFactories_.second :
			sysSvc->socketFactory_;
	if (factory == NULL) {
		GS_THROW_SYSTEM_ERROR(
				GS_ERROR_SC_SERVICE_CONSTRUCT_FAILED,
				"Socket factory not available (secure=" << secure << ")");
	}
	return *factory;
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
	picojson::object errorInfo;
	int32_t errorNo = WEBAPI_CS_OTHER_REASON;
	std::string reason = "Authentication error.";
	errorInfo["errorStatus"] = picojson::value(static_cast<double>(errorNo));
	errorInfo["reason"] = picojson::value(reason);
	setJson(picojson::value(errorInfo));
	statusCode_ = 401;
}

void SystemService::WebAPIResponse::setError(int32_t errorNo, const char* reason, uint32_t status) {
	picojson::object errorInfo;
	errorInfo["errorStatus"] = picojson::value(static_cast<double>(errorNo));
	errorInfo["reason"] = picojson::value(reason);
	setJson(picojson::value(errorInfo));
	statusCode_ = status;
}

bool SystemService::WebAPIResponse::checkParamValue(const char* paramName, int64_t& value, bool isInteger) {
	assert(paramName);
	if (paramName && SQLProcessor::ValueUtils::toLong(request_.parameterMap_[paramName].data(),
		request_.parameterMap_[paramName].size(), value)) {
		if (isInteger && (value >= UINT32_MAX || value < 0)) {
		}
		else {
			return true;
		}
	}
	util::NormalOStringStream oss;
	oss << "Invalid parameter, name=" << paramName << ", value=" << request_.parameterMap_[paramName];
	setError(WEBAPI_DB_INVALID_PARAMETER, oss.str().c_str(), 400);
	return false;
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
	: sysStatsInterval_(CommonUtility::changeTimeSecondToMilliSecond(OUTPUT_STATS_INTERVAL_SEC)),
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

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_SYS_SERVICE_SSL_PORT, INT32)
		.setMin(0)
		.setMax(65535)
		.setDefault(10045);

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_SYS_EVENT_LOG_PATH, STRING)
		.setDefault("log");
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_SYS_SECURITY_PATH, STRING)
		.setDefault("security");

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_SYS_TRACE_MODE, INT32)
		.setExtendedType(ConfigTable::EXTENDED_TYPE_ENUM)
		.addEnum(TRACE_MODE_SIMPLE, "SIMPLE")
		.addEnum(TRACE_MODE_SIMPLE_DETAIL, "SIMPLE_DETAIL")
		.addEnum(TRACE_MODE_FULL, "FULL")
		.deprecate()
		.setDefault(TRACE_MODE_SIMPLE_DETAIL);

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_SYS_SERVER_SSL_MODE, INT32)
		.setExtendedType(ConfigTable::EXTENDED_TYPE_ENUM)
		.addEnum(SSL_MODE_DISABLED, "DISABLED")
		.addEnum(SSL_MODE_PREFERRED, "PREFERRED")
		.addEnum(SSL_MODE_REQUIRED, "REQUIRED")
		.setDefault(SSL_MODE_DEFAULT);

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_SYS_CLUSTER_SSL_MODE, INT32)
		.setExtendedType(ConfigTable::EXTENDED_TYPE_ENUM)
		.addEnum(SSL_MODE_DISABLED, "DISABLED")
		.addEnum(SSL_MODE_REQUIRED, "REQUIRED")
		.addEnum(SSL_MODE_VERIFY, "VERIFY") 
		.setDefault(SSL_MODE_DEFAULT);

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_SYS_SSL_PROTOCOL_MAX_VERSION, INT32)
		.setExtendedType(ConfigTable::EXTENDED_TYPE_ENUM)
		.addEnum(SslConfig::SSL_VER_NONE, "NONE")
		.addEnum(SslConfig::SSL_VER_TLS1_0, "TLSv1.0")
		.addEnum(SslConfig::SSL_VER_TLS1_1, "TLSv1.1")
		.addEnum(SslConfig::SSL_VER_TLS1_2, "TLSv1.2")
		.addEnum(SslConfig::SSL_VER_TLS1_3, "TLSv1.3")
		.setDefault(SslConfig::SSL_VER_TLS1_2);
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
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_TXN_EE_SQL);

	parentId = STAT_TABLE_PERF_MEM;
	STAT_ADD_SUB(STAT_TABLE_PERF_MEM_ALL_TOTAL);
	STAT_ADD_SUB(STAT_TABLE_PERF_MEM_ALL_CACHED);
	STAT_ADD_SUB(STAT_TABLE_PERF_MEM_ALL_LOCAL_CACHED);
	STAT_ADD_SUB(STAT_TABLE_PERF_MEM_ALL_ELEMENT_COUNT);
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
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_TRANSACTION_STATE_TOTAL);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_TRANSACTION_STATE_CACHED);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_SQL_MESSAGE_TOTAL);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_SQL_MESSAGE_CACHED);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_SQL_WORK_TOTAL);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_SQL_WORK_CACHED);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_SQL_STORE_TOTAL);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_SQL_STORE_CACHED);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_SQL_JOB_TOTAL);
	STAT_ADD_SUB_SUB(STAT_TABLE_PERF_MEM_WORK_SQL_JOB_CACHED);
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
			svc.clsSvc_->getEE(),
			svc.syncSvc_->getEE(),
			svc.txnSvc_->getEE()
			, svc.sqlSvc_->getEE()
		};
		const StatTableParamId paramList[] = {
			STAT_TABLE_PERF_TXN_EE_CLUSTER,
			STAT_TABLE_PERF_TXN_EE_SYNC,
			STAT_TABLE_PERF_TXN_EE_TRANSACTION
			, STAT_TABLE_PERF_TXN_EE_SQL
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

			const int64_t activeQueueSize = eeStats.get(
				EventEngine::Stats::EVENT_ACTIVE_QUEUE_SIZE_CURRENT);
			picojson::object eeInfoObj;
			eeInfoObj["activeQueueSize"] =
				picojson::value(static_cast<double>(activeQueueSize));

			int64_t tmp;
				tmp = eeStats.get(
				EventEngine::Stats::EVENT_ACTIVE_QUEUE_SIZE_MAX);
			eeInfoObj["activeQueueSizeMax"] =
				picojson::value(static_cast<double>(tmp));

				tmp = eeStats.get(
				EventEngine::Stats::EVENT_ACTIVE_ADD_COUNT);
			eeInfoObj["activeAddCount"] =
				picojson::value(static_cast<double>(tmp));

				tmp = eeStats.get(
				EventEngine::Stats::EVENT_PENDING_ADD_COUNT);
			eeInfoObj["pendingAddCount"] =
				picojson::value(static_cast<double>(tmp));

				tmp = eeStats.get(
				EventEngine::Stats::EVENT_PENDING_ADD_COUNT);
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

				tmp = eeStats.get(
				EventEngine::Stats::IO_SEND_BUFFER_SIZE_MAX);

			eeInfoObj["ioSendBufferSizeMax"] =
				picojson::value(static_cast<double>(tmp));

			int64_t sendBufferSizeCurrent = 0;
			for (PartitionGroupId pgId = 0; pgId < concurrency; pgId++) {
				EventEngine::Stats eeGroupStats;
				eeList[i]->getStats(pgId, eeGroupStats);
				sendBufferSizeCurrent += eeGroupStats.get(
						EventEngine::Stats::IO_SEND_BUFFER_SIZE_CURRENT);
			}
			eeInfoObj["ioSendBufferSizeCurrent"] =
				picojson::value(static_cast<double>(sendBufferSizeCurrent));
			if (eeInfo.is<picojson::array>()) {
				eeInfo.get<picojson::array>().push_back(picojson::value(eeInfoObj));
			}
			else {
				eeInfo = picojson::value(eeInfoObj);
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

		svc.sqlSvc_->getExecutionManager()->updateStat(stat);
		if (!stat.getDisplayOption(STAT_TABLE_DISPLAY_WEB_ONLY) ||
			!stat.getDisplayOption(STAT_TABLE_DISPLAY_OPTIONAL_MEM)) {
			break;
		}

		static const int32_t statIdList[] = {
			STAT_TABLE_PERF_MEM_ALL_TOTAL,
			STAT_TABLE_PERF_MEM_DS_STORE_TOTAL,
			STAT_TABLE_PERF_MEM_DS_LOG_TOTAL,
			STAT_TABLE_PERF_MEM_WORK_CHECKPOINT_TOTAL,
			STAT_TABLE_PERF_MEM_WORK_CLUSTER_TOTAL,
			STAT_TABLE_PERF_MEM_WORK_MAIN_TOTAL,
			STAT_TABLE_PERF_MEM_WORK_SYNC_TOTAL,
			STAT_TABLE_PERF_MEM_WORK_SYSTEM_TOTAL,
			STAT_TABLE_PERF_MEM_WORK_TRANSACTION_MESSAGE_TOTAL,
			STAT_TABLE_PERF_MEM_WORK_TRANSACTION_RESULT_TOTAL,
			STAT_TABLE_PERF_MEM_WORK_TRANSACTION_WORK_TOTAL,
			STAT_TABLE_PERF_MEM_WORK_TRANSACTION_STATE_TOTAL,
			STAT_TABLE_PERF_MEM_WORK_SQL_MESSAGE_TOTAL,
			STAT_TABLE_PERF_MEM_WORK_SQL_WORK_TOTAL,
			STAT_TABLE_PERF_MEM_WORK_SQL_STORE_TOTAL,
			STAT_TABLE_PERF_MEM_WORK_SQL_JOB_TOTAL
		};
		static const size_t listSize = sizeof(statIdList) / sizeof(*statIdList);
		static const util::AllocatorGroupId allocIdList[listSize] = {
			ALLOCATOR_GROUP_ROOT,
			ALLOCATOR_GROUP_STORE,
			ALLOCATOR_GROUP_LOG,
			ALLOCATOR_GROUP_CP,
			ALLOCATOR_GROUP_CS,
			ALLOCATOR_GROUP_MAIN,
			ALLOCATOR_GROUP_SYNC,
			ALLOCATOR_GROUP_SYS,
			ALLOCATOR_GROUP_TXN_MESSAGE,
			ALLOCATOR_GROUP_TXN_RESULT,
			ALLOCATOR_GROUP_TXN_WORK,
			ALLOCATOR_GROUP_TXN_STATE,
			ALLOCATOR_GROUP_SQL_MESSAGE,
			ALLOCATOR_GROUP_SQL_WORK,
			ALLOCATOR_GROUP_SQL_LTS,
			ALLOCATOR_GROUP_SQL_JOB
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

		util::AllocatorStats directStats;
		allocMgr.getGroupStats(
				&idList[0], idList.size(), &statsList[0], &directStats);
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
				stat.set(STAT_TABLE_PERF_MEM_PROCESS_MEMORY_GAP, std::max<int64_t>(
						static_cast<int64_t>(processMemory) -
						util::AllocatorManager::estimateHeapUsage(
								allocStats, directStats), 0));
				stat.set(STAT_TABLE_PERF_MEM_ALL_LOCAL_CACHED, std::max<int64_t>(
						allocStats.values_[util::AllocatorStats::STAT_CACHE_SIZE] -
						directStats.values_[util::AllocatorStats::STAT_CACHE_SIZE], 0));
				stat.set(STAT_TABLE_PERF_MEM_ALL_ELEMENT_COUNT,
						directStats.values_[util::AllocatorStats::STAT_CACHE_MISS_COUNT]);
			}
		}
	} while (false);

	if (stat.getDisplayOption(STAT_TABLE_DISPLAY_OPTIONAL_SYNC)) {
		picojson::value result;
		svc.getSyncStats(result, statOptionToAddressType(stat));
		stat.set(STAT_TABLE_ROOT_SYNC, result);
	}

	if (stat.getDisplayOption(STAT_TABLE_DISPLAY_OPTIONAL_PGLIMIT)) {
		picojson::value result;
		svc.getPGStoreMemoryLimitStats(result);
		stat.set(STAT_TABLE_ROOT_PG_STORE_MEMORY_LIMIT, result);
	}

	if (stat.getDisplayOption(STAT_TABLE_DISPLAY_OPTIONAL_MEM)) {
		service_->sqlSvc_->getExecutionManager()->updateStat(stat);
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
	pt_ = mgrSet.pt_;
	clsMgr_ = mgrSet.clsMgr_;
}

void RestContext::setError(int32_t errorNo, const char* reason) {
	reason_ = reason;
	picojson::object errorInfo;
	errorInfo["errorStatus"] = picojson::value(static_cast<double>(errorNo));
	errorInfo["reason"] = picojson::value(reason);
	result_ = picojson::value(errorInfo);
}

