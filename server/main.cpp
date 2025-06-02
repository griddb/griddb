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
	@brief Implementation of main function
*/

#include "checkpoint_service.h"
#include "cluster_manager.h"
#include "cluster_service.h"
#include "config_table.h"
#include "event_engine.h"
#include "partition_table.h"
#include "recovery_manager.h"
#include "sync_manager.h"
#include "sync_service.h"
#include "system_service.h"
#include "transaction_manager.h"
#include "transaction_service.h"
#include "partition.h"
#include "sql_service.h"
#include "sql_execution.h"
#include "sql_command_manager.h"
#include "database_manager.h"
#include "picojson.h"


#ifndef _WIN32
#define MAIN_CAPTURE_SIGNAL
#endif

#ifdef MAIN_CAPTURE_SIGNAL
#include <execinfo.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#endif  

#include <fstream>



const char8_t *const GS_PRODUCT_NAME = "GridDB";
const int32_t GS_MAJOR_VERSION = 5;
const int32_t GS_MINOR_VERSION = 8;
const int32_t GS_REVISION = 0;
const int32_t GS_BUILD_NO = 40624;

const char8_t *const GS_EDITION_NAME = "Community Edition";
const char8_t *const GS_EDITION_NAME_SHORT = "CE";

const char8_t *const SYS_CLUSTER_FILE_NAME = "gs_cluster.json";
const char8_t *const SYS_NODE_FILE_NAME = "gs_node.json";
const char8_t *const SYS_DEVELOPER_FILE_NAME = "gs_developer.json";
const char8_t *const GS_CLUSTER_PARAMETER_DIFF_FILE_NAME = "gs_diff.json";

const char8_t *const GS_TRACE_SECRET_HEX_KEY = "7B790AB2C82F01B3"; 



static void autoJoinCluster(const Event::Source &eventSource,
	util::StackAllocator &alloc, SystemService &sysSvc, PartitionTable &pt,
	ClusterService &clsSvc);

static class MainConfigSetUpHandler : public ConfigTable::SetUpHandler {
	virtual void operator()(ConfigTable &config);

	static ConfigTable::Constraint &declareTraceConfigConstraints(
		ConfigTable::Constraint &constraint,
		util::TraceOption::Level defaultLevel);

} g_mainConfigSetUpHandler;

static void setUpTrace(const ConfigTable *param, bool checkOnly, bool longArchive = false);
static void setUpAllocator();

/*!
	@brief Handler for failure of memory allocation at the StackAllocator
	@note Throws UserException with the error code of stack memory limit over
*/
static class StackMemoryLimitOverErrorHandler
	: public util::AllocationErrorHandler {
	void operator()(util::Exception &e);
} g_stackMemoryLimitOverErrorHandler;

std::string preparePidFile(const char8_t *const configDir);
void createPidFile(const std::string &fileName, util::PIdFile &pidFile);
void cleanupPidFile(const std::string &fileName, util::PIdFile &pidFile);

static void cleanupOnNormalShutdown(const Event::Source &eventSource,
	util::StackAllocator &alloc, SystemService &sysSvc, ClusterManager &clsMgr);

static void forceShutdown(ClusterService &clsSvc,
	const std::string &pidFileName, util::PIdFile &pidFile, bool checkOnly);


#ifdef MAIN_CAPTURE_SIGNAL
void signal_handler(int sig, siginfo_t *siginfo, void *param);
#endif


UTIL_TRACER_DECLARE(MAIN);
UTIL_TRACER_DECLARE(SYSTEM_SERVICE);
AUDIT_TRACER_DECLARE(AUDIT_SYSTEM);

/*!
	@brief Handles trace request for main function
*/
class MainTraceHandler : private util::TraceHandler {
public:
	explicit MainTraceHandler(const ConfigTable &config) : config_(config) {
		util::TraceManager::getInstance().setTraceHandler(this);
	}

	virtual ~MainTraceHandler() {
		try {
			util::TraceManager::getInstance().setTraceHandler(NULL);
		}
		catch (...) {
		}
	}

	virtual void startStream() {
		GS_TRACE_INFO(SYSTEM_SERVICE, GS_TRACE_SC_EVENT_LOG_STARTED,
			config_.get<const char8_t *>(CONFIG_TABLE_DEV_FULL_VERSION));
	}

	const ConfigTable &config_;
};

/*!
	@brief main function
*/
int main(int argc, char **argv) {
	bool checkOnly = false;
	bool systemErrorOccurred = false;
	bool signalOccured = false;

	std::string pidFileName;
	util::PIdFile pidFile;

	try {
		setUpTrace(NULL, false);
		setUpAllocator();

		util::NormalOStringStream version;
		version << GS_MAJOR_VERSION << "." << GS_MINOR_VERSION << "."
				<< GS_REVISION;
		util::NormalOStringStream simpleVersion;
		simpleVersion << version.str()
					<< "-" << GS_BUILD_NO
					<< " " << GS_EDITION_NAME_SHORT;
		util::NormalOStringStream fullVersion;
		fullVersion << GS_PRODUCT_NAME
					<< " version " << version.str()
					<< " build " << GS_BUILD_NO
					<< " " << GS_EDITION_NAME;


		GlobalVariableSizeAllocator tableAlloc(
			(util::AllocatorInfo(ALLOCATOR_GROUP_MAIN, "tableAlloc")));
		ConfigTable config(tableAlloc);

		config.set(CONFIG_TABLE_DEV_SIMPLE_VERSION, simpleVersion.str().c_str(),
			"main", &ConfigTable::silentHandler());
		config.set(CONFIG_TABLE_DEV_FULL_VERSION, fullVersion.str().c_str(),
			"main", &ConfigTable::silentHandler());
		u8string passwordFile;
		bool needHelp = false;
		bool dbDump = false;
		const char8_t *configDir = NULL;
		const char8_t *dbDumpDir = NULL;
		bool logDump = false;
		int32_t logDumpPgId = -1;
		int32_t logDumpCpId = -1;
		bool forceRecoveryFromExistingFiles = false;  
		std::string recoveryTargetPartition;		  
		bool fileVersionDump = false;				  
		bool releaseUnusedFileBlocks = false;		  
		bool longArchive = false;					  
		u8string longArchiveLogDir;					  

		if (argc >= 3) {
			if (strcmp(argv[1], "--conf") == 0) {
				configDir = argv[2];

				const char8_t *const configFileList[] = {
					SYS_CLUSTER_FILE_NAME, SYS_NODE_FILE_NAME};

				config.acceptFile(configDir, configFileList,
					sizeof(configFileList) / sizeof(*configFileList),
					CONFIG_TABLE_ROOT);

				util::FileSystem::createPath(
					configDir, "password", passwordFile);

				pidFileName = preparePidFile(configDir);

				if (argc == 4) {
					if (strcmp(argv[3],
							"--force-recovery-from-existing-files") == 0) {
						forceRecoveryFromExistingFiles = true;
					}
					else if (strcmp(argv[3], "--fileversiondump") == 0) {
						checkOnly = true;
						fileVersionDump = true;
					}
					else if (strcmp(argv[3], "--release-unused-file-blocks") == 0) {
						releaseUnusedFileBlocks = true;
					}
					else {
						needHelp = true;
					}
				}
				if (argc == 5) {
					if (strcmp(argv[3], "--recovery-target-partition") == 0) {
						recoveryTargetPartition = argv[4];
					}
					else if (strcmp(argv[3], "--dbdump") == 0) {
						checkOnly = true;
						dbDump = true;
						dbDumpDir = argv[4];
					}
					else {
						needHelp = true;
					}
				}
				if (argc == 6) {
					if (strcmp(argv[3],
							"--force-recovery-from-existing-files") == 0) {
						forceRecoveryFromExistingFiles = true;
						if (strcmp(argv[4], "--recovery-target-partition") ==
							0) {
							recoveryTargetPartition = argv[5];
						}
						else {
							needHelp = true;
						}
					}
					else if (strcmp(argv[3], "--logdump") == 0) {
						checkOnly = true;
						logDump = true;
						logDumpPgId = atoi((const char *)argv[4]);
						logDumpCpId = atoi((const char *)argv[5]);
					}
					else {
						needHelp = true;
					}
				}
			}
			else {
				needHelp = true;
			}
		}
		else if (argc == 2) {
			if (strcmp(argv[1], "--version") == 0) {
				std::cerr << fullVersion.str() << std::endl;
				return EXIT_SUCCESS;
			}
			else {
				needHelp = true;
			}
		}
		else {
			needHelp = true;
		}


		if (needHelp) {
			std::cerr << "Usage: --conf (Config dir)" << std::endl;
			std::cerr << "Usage: --conf (Config dir) "
						 "[--force-recovery-from-existing-files] "
						 "[--recovery-target-partition a,b-c,d]"
					  << std::endl;
			std::cerr << "Usage: --conf (Config dir) --release-unused-file-blocks"
					  << std::endl;
			std::cerr << "Usage: --conf (Config dir) --dbdump (Dump dir)"
					  << std::endl;
			std::cerr << "Usage: --conf (Config dir) --logdump (GroupId) (Dump "
						 "start CpId)"
					  << std::endl;
			std::cerr << "Usage: --conf (Config dir) --fileversiondump"
					  << std::endl;
			std::cerr << "Usage: --version" << std::endl;
			return EXIT_FAILURE;
		}

		MainTraceHandler traceHandler(config);
		setUpTrace(&config, checkOnly, longArchive);
		config.setTraceEnabled(true);

		AUDIT_TRACE_INFO(AUDIT_SYSTEM, GS_TRACE_SC_WEB_API_CALLED, 
				"", "", "", "", "", "", "SYSTEM", "GS_START_NODE", "", "", "");

#ifdef MAIN_CAPTURE_SIGNAL
		const int syncSignals[] = {
			SIGSEGV
		};
		const int signalNum = sizeof(syncSignals) / sizeof(*syncSignals);

		struct sigaction sa;

		memset(&sa, 0x00, sizeof(struct sigaction));
		sigemptyset(&(sa.sa_mask));

		sa.sa_flags = SA_RESTART | SA_SIGINFO | SA_RESETHAND | SA_ONSTACK;
		sa.sa_sigaction = signal_handler;

		for (int i = 0; i < signalNum; i++) {
			if (0 != sigaction(syncSignals[i], &sa, NULL)) {
				UTIL_THROW_PLATFORM_ERROR(UTIL_EXCEPTION_CREATE_MESSAGE_CHARS(
						"signal=" << syncSignals[i]));
			}
		}

		sigset_t ss1;
		sigemptyset(&ss1);

		if (0 != sigaddset(&ss1, SIGINT)) {
			UTIL_THROW_PLATFORM_ERROR("");
		}
		if (0 != sigaddset(&ss1, SIGTERM)) {
			UTIL_THROW_PLATFORM_ERROR("");
		}

		sigset_t ss2;
		sigemptyset(&ss2);

		for (int i= 0; i < signalNum; i++) {
			if (0 != sigaddset(&ss2, syncSignals[i])) {
				UTIL_THROW_PLATFORM_ERROR(UTIL_EXCEPTION_CREATE_MESSAGE_CHARS(
						"signal=" << syncSignals[i]));
			}
		}
#endif  

		if (!checkOnly) {
			createPidFile(pidFileName, pidFile);
		}

#ifdef MAIN_CAPTURE_SIGNAL
		if (!checkOnly) {


			if (0 != pthread_sigmask(SIG_BLOCK, &ss1, NULL)) {
				const int errsv = errno;
				cleanupPidFile(pidFileName, pidFile);
				UTIL_THROW_PLATFORM_ERROR_WITH_CODE(TYPE_NORMAL, errsv, "");
			}

			if (0 != pthread_sigmask(SIG_UNBLOCK, &ss2, NULL)) {
				const int errsv = errno;
				cleanupPidFile(pidFileName, pidFile);
				UTIL_THROW_PLATFORM_ERROR_WITH_CODE(TYPE_NORMAL, errsv, "");
			}
		}
#endif  

		bool createMode = false;
		bool existBackupInfoFile = false;

		RecoveryManager::checkExistingFiles1(config, createMode,
			existBackupInfoFile, forceRecoveryFromExistingFiles);

		EventEngine::VariableSizeAllocator eeVarSizeAlloc(
			(util::AllocatorInfo(ALLOCATOR_GROUP_MAIN, "eeVar")));
		GlobalVariableSizeAllocator varSizeAlloc(
			(util::AllocatorInfo(ALLOCATOR_GROUP_MAIN, "globalVar")));

		const size_t blockSizeBits = 20;
		const size_t maxCount = ConfigTable::megaBytesToBytes(config.getUInt32(
									CONFIG_TABLE_TXN_TOTAL_MEMORY_LIMIT)) /
								(1 << blockSizeBits);
		GlobalFixedSizeAllocator fixedSizeAlloc(
			util::AllocatorInfo(ALLOCATOR_GROUP_MAIN, "globalFixed"),
			1 << blockSizeBits);
		fixedSizeAlloc.setFreeElementLimit(maxCount);

		util::StackAllocator::setDefaultErrorHandler(
			&g_stackMemoryLimitOverErrorHandler);

		ServiceThreadErrorHandler errorHandler;


		util::SingleThreadExecutor exeSvc(
				util::AllocatorInfo(
						ALLOCATOR_GROUP_MAIN, "globalExecutorService"),
				varSizeAlloc);

		PartitionTable pt(config, configDir);
		ClusterVersionId clsVersionId = GS_CLUSTER_MESSAGE_CURRENT_VERSION;
		ClusterManager clsMgr(config, &pt, clsVersionId, configDir);

		SyncManager syncMgr(config, &pt);

		TransactionManager txnMgr(config, false);

		util::FixedSizeAllocator<util::Mutex> resultSetPool(
			util::AllocatorInfo(ALLOCATOR_GROUP_TXN_RESULT, "resultSetPool"),
			1 << DataStoreBase::RESULTSET_POOL_BLOCK_SIZE_BITS);
		resultSetPool.setTotalElementLimit(
			ConfigTable::megaBytesToBytes(
				config.getUInt32(CONFIG_TABLE_DS_RESULT_SET_MEMORY_LIMIT)) /
			(1 << DataStoreBase::RESULTSET_POOL_BLOCK_SIZE_BITS));

		const uint32_t rsCacheSize =
			config.getUInt32(CONFIG_TABLE_DS_RESULT_SET_CACHE_MEMORY);
		if (rsCacheSize > 0) {
			resultSetPool.setLimit(
				util::AllocatorStats::STAT_STABLE_LIMIT,
				ConfigTable::megaBytesToBytes(rsCacheSize));
		}
		else {
			resultSetPool.setFreeElementLimit(0);
		}

		PartitionList partitionList(config, &txnMgr, resultSetPool);
		RecoveryManager recoveryMgr(config, releaseUnusedFileBlocks);
		if (existBackupInfoFile) {
			recoveryMgr.readBackupInfoFile();
		}
		DataStoreConfig dsConfig(config);

		EventEngine::Config eeConfig;
		EventEngine::Source source(eeVarSizeAlloc, fixedSizeAlloc);

		int32_t cacheSize = config.get<int32_t>(CONFIG_TABLE_SEC_USER_CACHE_SIZE);
		int32_t cacheUpdateInterval = config.get<int32_t>(CONFIG_TABLE_SEC_USER_CACHE_UPDATE_INTERVAL);
		UserCache userCache(cacheSize, varSizeAlloc, cacheUpdateInterval);
		UserCache* pUserCache = NULL;
		if (cacheSize > 0) {
			pUserCache = &userCache;
		}
		
		util::VariableSizeAllocator<> notifyAlloc(
			(util::AllocatorInfo(ALLOCATOR_GROUP_MAIN, "notifyAlloc")));
		ClusterService clsSvc(config, eeConfig, source, "CLUSTER_SERVICE",
			clsMgr, clsVersionId, errorHandler, notifyAlloc);


		SyncService syncSvc(config, eeConfig, source, "SYNC_SERVICE", syncMgr,
			clsVersionId, errorHandler);

		u8string diffFile;
		util::FileSystem::createPath(
			configDir, GS_CLUSTER_PARAMETER_DIFF_FILE_NAME, diffFile);
		SystemService sysSvc(config, eeConfig, source, "SYSTEM_SERVICE",
			diffFile.c_str(), errorHandler);

		sysSvc.getUserTable().load(passwordFile.c_str());

		TransactionService txnSvc(
			config, eeConfig, source, "TRANSACTION_SERVICE");

		CheckpointService cpSvc(
			config, eeConfig, source, "CHECKPOINT_SERVICE", errorHandler);

		SQLVariableSizeGlobalAllocator valloc(
				util::AllocatorInfo(ALLOCATOR_GROUP_SQL_JOB, "JobGlobalAllocator"));
		LocalTempStore::LTSVariableSizeAllocator ltsVarAlloc(
				util::AllocatorInfo(ALLOCATOR_GROUP_SQL_LTS, "ltsVar"));

		LocalTempStore::Config storeConfig(config);
		LocalTempStore store(storeConfig, ltsVarAlloc, !longArchive);
		LTSEventBufferManager ltsEventBufferManager(store);
		source.bufferManager_ = &ltsEventBufferManager;

		SQLService newSqlSvc(config, eeConfig, source, "SQL_SERVICE", &pt);

		StatTable stats(tableAlloc);
		stats.initialize();

		u8string address;
		uint16_t port = 0;
		newSqlSvc.getEE()->getSelfServerND().getAddress().getIP(&address, &port);

		SQLExecutionManager executionManager(config, valloc);
		JobManager jobManager(valloc, store, config);

		DatabaseManager dbMgr(config, varSizeAlloc, txnMgr.getPartitionGroupConfig());

		ManagerSet mgrSet(&clsSvc, &syncSvc, &txnSvc, &cpSvc, &sysSvc,
			&pt, &partitionList, &clsMgr, &syncMgr, &txnMgr,
			&recoveryMgr, &fixedSizeAlloc, &varSizeAlloc, &exeSvc,
			&config, &stats
			, &newSqlSvc, &executionManager, &jobManager
			, pUserCache 
			, NULL
			, &dsConfig
			,&dbMgr
		);


		recoveryMgr.initialize(mgrSet);
		pt.initialize(&mgrSet);

		clsSvc.initialize(mgrSet);
		syncSvc.initialize(mgrSet);
		txnSvc.initialize(mgrSet);
		sysSvc.initialize(mgrSet);
		cpSvc.initialize(mgrSet);
		partitionList.initialize(mgrSet);
		newSqlSvc.initialize(mgrSet);
		executionManager.initialize(mgrSet);
		jobManager.initialize(mgrSet);


		UNUSED_VARIABLE(dbDump);
		UNUSED_VARIABLE(dbDumpDir);
		UNUSED_VARIABLE(logDump);
		UNUSED_VARIABLE(logDumpPgId);
		UNUSED_VARIABLE(logDumpCpId);
		UNUSED_VARIABLE(fileVersionDump);


		try {
			std::cout << "Running..." << std::endl;

			if (config.get<bool>(CONFIG_TABLE_DEV_RECOVERY)) {
				std::cout << "Recovering..." << std::endl;
				util::StackAllocator alloc(
					util::AllocatorInfo(ALLOCATOR_GROUP_MAIN, "recoveryStack"),
					&fixedSizeAlloc);
				recoveryMgr.recovery(alloc, existBackupInfoFile,
					forceRecoveryFromExistingFiles, recoveryTargetPartition,
					longArchive);
			}
			else {
				std::cout << "Skip Open DB" << std::endl;
			}
			cpSvc.executeRecoveryCheckpoint(source);

			if (existBackupInfoFile) {
				recoveryMgr.removeBackupInfoFile();
			}

			if (!longArchive) { 
				sysSvc.start();
				clsSvc.start(source);
				syncSvc.start();
				txnSvc.start();
				cpSvc.start(source);
				newSqlSvc.start();
			}

			if (config.get<bool>(CONFIG_TABLE_DEV_AUTO_JOIN_CLUSTER)) {
				util::StackAllocator alloc(
					util::AllocatorInfo(ALLOCATOR_GROUP_MAIN, "autoJoinStack"),
					&fixedSizeAlloc);
				autoJoinCluster(source, alloc, sysSvc, pt, clsSvc);
			}

#ifdef MAIN_CAPTURE_SIGNAL
			int signo;
			while (1) {
				if (sigwait(&ss1, &signo) == 0) {
					if (SIGINT == signo) {  
						forceShutdown(clsSvc, pidFileName, pidFile, checkOnly);
						signalOccured = true;
						break;
					}
					if (SIGTERM == signo) {  
						if (clsMgr.isSystemError()) {
							forceShutdown(clsSvc, pidFileName, pidFile, checkOnly);
							signalOccured = true;
							const char8_t *msg = "Requested normal shutdown node, but node status is ABNORMAL, so that executes force shutdown node.";
							std::cerr << msg << std::endl;
							GS_TRACE_WARNING(MAIN, GS_TRACE_SC_FORCE_SHUTDOWN, msg);
						}
						else {
							util::StackAllocator alloc(
								util::AllocatorInfo(
									ALLOCATOR_GROUP_MAIN, "shutdownStack"),
								&fixedSizeAlloc);
							cleanupOnNormalShutdown(source, alloc, sysSvc, clsMgr);
						}
						break;
					}
				}
			}
#endif  

			clsSvc.waitForShutdown();
			syncSvc.waitForShutdown();
			txnSvc.waitForShutdown();
			cpSvc.waitForShutdown();
			sysSvc.waitForShutdown();
			newSqlSvc.shutdown();
			newSqlSvc.waitForShutdown();

			systemErrorOccurred = clsSvc.isSystemServiceError();

			if (signalOccured) {
				std::cerr << "Execution terminated." << std::endl;
			}
			else if (systemErrorOccurred) {
				std::cerr << "Execution failed(system service is failed)."
							 " See message logs"
						  << std::endl;
			}
			if (!systemErrorOccurred) {
				systemErrorOccurred = clsMgr.isAutoShutdown();
			}
		}
		catch (std::exception &e) {
			systemErrorOccurred = true;
			UTIL_TRACE_EXCEPTION(MAIN, e, "");

			try {
				if (!longArchive) {
					clsSvc.shutdownAllService();
					newSqlSvc.shutdown();
					newSqlSvc.waitForShutdown();
					clsSvc.waitForShutdown();
					syncSvc.waitForShutdown();
					txnSvc.waitForShutdown();
					cpSvc.waitForShutdown();
					sysSvc.waitForShutdown();
				}
			}
			catch (...) {
				UTIL_TRACE_EXCEPTION(
					MAIN, e, GS_EXCEPTION_MERGE_MESSAGE(
								 e, "Failed to clean up for shutdown"));
				abort();
			}

			std::cerr << "Execution failed. See message logs" << std::endl;
		}
	}
	catch (std::exception &e) {
		systemErrorOccurred = true;
		UTIL_TRACE_EXCEPTION(MAIN, e, "");

		std::cerr << "Execution failed. See message logs" << std::endl;
	}

	if (!checkOnly) {
		cleanupPidFile(pidFileName, pidFile);
	}

	return (systemErrorOccurred ? EXIT_FAILURE : EXIT_SUCCESS);
}



void autoJoinCluster(const Event::Source &eventSource,
	util::StackAllocator &alloc, SystemService &sysSvc, PartitionTable &pt,
	ClusterService &clsSvc) {
	std::cout << "autoJoinCluster...";

	clsSvc.requestCompleteCheckpoint(eventSource, alloc, true);

	sysSvc.joinCluster(eventSource, alloc, "autoJoinCluster", 1);
	ClusterManager &clsMgr = *clsSvc.getManager();

	util::Thread::sleep(1000);
	PartitionId pId;

	while (1) {
		bool completeFlag = true;

		try {
			clsMgr.checkCommandStatus(CS_LEAVE_CLUSTER);
		}
		catch (std::exception &) {
			completeFlag = false;
			std::cout << "retry join cluster" << std::endl;
		}

		if (completeFlag) {
			PartitionRevision &rev = pt.incPartitionRevision();
			NodeIdList backups;
			for (pId = 0; pId < pt.getPartitionNum(); pId++) {
				pt.setPartitionStatus(pId, PartitionTable::PT_ON);
				PartitionRole role(
					pId, rev, PartitionTable::PT_CURRENT_OB);
				pt.setPartitionRole(pId, role);
			}
			break;
		}
		if (clsMgr.isError()) {
			GS_THROW_SYSTEM_ERROR(
				GS_ERROR_SC_EXEC_FATAL, "System error detected");
		}
		util::Thread::sleep(1000);
		sysSvc.joinCluster(eventSource, alloc, "autoJoinCluster", 1);
	}
	std::cout << " complete." << std::endl;
}

std::string preparePidFile(const char8_t *const configDir) {
	std::string fileName;

	fileName += configDir;
	fileName += "/gridstore.pid";

	return fileName;
}

void createPidFile(const std::string &fileName, util::PIdFile &pidFile) {
	util::FileStatus prevStatus;
	bool prevFound = false;
	try {
		if (util::FileSystem::exists(fileName.c_str())) {
			util::FileSystem::getFileStatus(fileName.c_str(), &prevStatus);
			prevFound = true;
		}
	}
	catch (...) {
	}

	try {
		pidFile.open(fileName.c_str());
	}
	catch (...) {
		std::cerr << "GridDB gridstore.pid is already locked." << std::endl;
		exit(1);
	}

	if (prevFound) {
		GS_TRACE_ERROR(MAIN, GS_TRACE_SC_UNEXPECTED_SHUTDOWN_DETECTED,
			"Unexpected shutdown detected. "
			"Previous gridstore.pid file is found ("
			"path="
				<< fileName << ", modificationTime="
				<< prevStatus.getModificationTime() << ")");
	}
}

void cleanupPidFile(const std::string &fileName, util::PIdFile &pidFile) {
	UNUSED_VARIABLE(fileName);

	try {
		pidFile.close();
	}
	catch (...) {
		std::cerr << "Warning: Failed to clean up PID file." << std::endl;
		try {
			throw;
		}
		catch (util::Exception &e) {
			e.format(std::cerr);
		}
		catch (...) {
		}
	}
}

static void cleanupOnNormalShutdown(const Event::Source &eventSource,
	util::StackAllocator &alloc, SystemService &sysSvc,
	ClusterManager &clsMgr) {
	std::cout << "SIGNAL: shutdown start" << std::endl;
	clsMgr.setSignalBeforeRecovery();

	const bool normalShutdown = false;
	sysSvc.leaveCluster(eventSource, alloc);
	sysSvc.shutdownNode(eventSource, alloc, normalShutdown);
}

static void forceShutdown(ClusterService &clsSvc,
		const std::string &pidFileName, util::PIdFile &pidFile, bool checkOnly) {
	if (!checkOnly) {
		cleanupPidFile(pidFileName, pidFile);
	}
	clsSvc.shutdownAllService();
}


#ifdef MAIN_CAPTURE_SIGNAL
void signal_handler(int sig, siginfo_t *siginfo, void *param) {
	UNUSED_VARIABLE(sig);
	UNUSED_VARIABLE(siginfo);
	UNUSED_VARIABLE(param);

	int nptrs;
	const int FRAME_DEPTH = 50;
	void *buffer[FRAME_DEPTH];

	nptrs = backtrace(buffer, FRAME_DEPTH);

	backtrace_symbols_fd(buffer, nptrs, STDOUT_FILENO);

	abort();
}
#endif

#define MAIN_TRACE_DECLARE(configTable, name, defaultLevel)                    \
	declareTraceConfigConstraints(                                             \
		CONFIG_TABLE_ADD_PARAM(configTable, CONFIG_TABLE_TRACE_##name, INT32), \
		util::TraceOption::LEVEL_##defaultLevel)

void MainConfigSetUpHandler::operator()(ConfigTable &config) {
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_ROOT_NOTIFICATION_ADDRESS, STRING)
		.setDefault("239.0.0.1");
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_ROOT_SERVICE_ADDRESS, STRING)
		.setDefault("");

	CONFIG_TABLE_RESOLVE_GROUP(config, CONFIG_TABLE_TRACE, "trace");
	MAIN_TRACE_DECLARE(config, DEFAULT, ERROR);
	MAIN_TRACE_DECLARE(config, MAIN, WARNING);
	MAIN_TRACE_DECLARE(config, BASE_CONTAINER, ERROR);
	MAIN_TRACE_DECLARE(config, DATA_STORE, ERROR);
	MAIN_TRACE_DECLARE(config, KEY_DATA_STORE, ERROR);
	MAIN_TRACE_DECLARE(config, COLLECTION, ERROR);
	MAIN_TRACE_DECLARE(config, TIME_SERIES, ERROR);
	MAIN_TRACE_DECLARE(config, CHUNK_MANAGER, ERROR);
	MAIN_TRACE_DECLARE(config, CHUNK_MANAGER_DETAIL, ERROR);
	MAIN_TRACE_DECLARE(config, CHUNK_MANAGER_IO_DETAIL, ERROR);
	MAIN_TRACE_DECLARE(config, OBJECT_MANAGER, ERROR);
	MAIN_TRACE_DECLARE(config, CHECKPOINT_FILE, ERROR);
	MAIN_TRACE_DECLARE(config, CHECKPOINT_SERVICE, INFO);
	MAIN_TRACE_DECLARE(config, CHECKPOINT_SERVICE_DETAIL, ERROR);
	MAIN_TRACE_DECLARE(config, CHECKPOINT_SERVICE_STATUS_DETAIL, ERROR);
	MAIN_TRACE_DECLARE(config, LOG_MANAGER, WARNING);
	MAIN_TRACE_DECLARE(config, IO_MONITOR, WARNING);
	MAIN_TRACE_DECLARE(config, CLUSTER_OPERATION, INFO);
	MAIN_TRACE_DECLARE(config, CLUSTER_SERVICE, ERROR);
	MAIN_TRACE_DECLARE(config, SYNC_SERVICE, ERROR);
	MAIN_TRACE_DECLARE(config, SYSTEM_SERVICE, INFO);
	MAIN_TRACE_DECLARE(config, SYSTEM_SERVICE_DETAIL, ERROR);
	MAIN_TRACE_DECLARE(config, TRANSACTION_MANAGER, ERROR);
	MAIN_TRACE_DECLARE(config, SESSION_DETAIL, ERROR);
	MAIN_TRACE_DECLARE(config, TRANSACTION_DETAIL, ERROR);
	MAIN_TRACE_DECLARE(config, TIMEOUT_DETAIL, ERROR);
	MAIN_TRACE_DECLARE(config, TRANSACTION_SERVICE, ERROR);
	MAIN_TRACE_DECLARE(config, REPLICATION, WARNING);
	MAIN_TRACE_DECLARE(config, TRANSACTION_TIMEOUT, WARNING);
	MAIN_TRACE_DECLARE(config, SESSION_TIMEOUT, WARNING);
	MAIN_TRACE_DECLARE(config, REPLICATION_TIMEOUT, WARNING);
	MAIN_TRACE_DECLARE(config, RECOVERY_MANAGER, INFO);
	MAIN_TRACE_DECLARE(config, RECOVERY_MANAGER_DETAIL, ERROR);
	MAIN_TRACE_DECLARE(config, EVENT_ENGINE, WARNING);
	MAIN_TRACE_DECLARE(config, TRIGGER_SERVICE, ERROR);
	MAIN_TRACE_DECLARE(config, SQL_SERVICE, ERROR);
	MAIN_TRACE_DECLARE(config, SQL_TEMP_STORE, ERROR);
	MAIN_TRACE_DECLARE(config, TUPLE_LIST, ERROR);
	MAIN_TRACE_DECLARE(config, DISTRIBUTED_FRAMEWORK, ERROR);
	MAIN_TRACE_DECLARE(config, DISTRIBUTED_FRAMEWORK_DETAIL, ERROR);
	MAIN_TRACE_DECLARE(config, SQL_HINT, ERROR);
	MAIN_TRACE_DECLARE(config, SQL_INTERNAL, ERROR);
	MAIN_TRACE_DECLARE(config, MESSAGE_LOG_TEST, ERROR);
	MAIN_TRACE_DECLARE(config, CLUSTER_INFO_TRACE, ERROR);
	MAIN_TRACE_DECLARE(config, SYSTEM, ERROR);
	MAIN_TRACE_DECLARE(config, BTREE_MAP, ERROR);
	MAIN_TRACE_DECLARE(config, AUTHENTICATION_TIMEOUT, WARNING);
	MAIN_TRACE_DECLARE(config, DATASTORE_BACKGROUND, ERROR);
	MAIN_TRACE_DECLARE(config, SYNC_DETAIL, WARNING);
	MAIN_TRACE_DECLARE(config, CLUSTER_DETAIL, WARNING);
	MAIN_TRACE_DECLARE(config, CLUSTER_DUMP, WARNING);
	MAIN_TRACE_DECLARE(config, SQL_DETAIL, WARNING);
	MAIN_TRACE_DECLARE(config, ZLIB_UTILS, ERROR);
	MAIN_TRACE_DECLARE(config, SIZE_MONITOR, WARNING);
	MAIN_TRACE_DECLARE(config, LONG_ARCHIVE, WARNING);
	MAIN_TRACE_DECLARE(config, AUTH_OPERATION, ERROR);
	MAIN_TRACE_DECLARE(config, PARTITION, INFO);
	MAIN_TRACE_DECLARE(config, PARTITION_DETAIL, ERROR);
	MAIN_TRACE_DECLARE(config, DATA_EXPIRATION_DETAIL, INFO);
	MAIN_TRACE_DECLARE(config, CONNECTION_DETAIL, WARNING);
	MAIN_TRACE_DECLARE(config, AUDIT_SYSTEM, CRITICAL);
	MAIN_TRACE_DECLARE(config, AUDIT_STAT, CRITICAL);
	MAIN_TRACE_DECLARE(config, AUDIT_SQL_READ, CRITICAL);
	MAIN_TRACE_DECLARE(config, AUDIT_SQL_WRITE, CRITICAL);
	MAIN_TRACE_DECLARE(config, AUDIT_NOSQL_READ, CRITICAL);
	MAIN_TRACE_DECLARE(config, AUDIT_NOSQL_WRITE, CRITICAL);
	MAIN_TRACE_DECLARE(config, AUDIT_CONNECT, INFO);
	MAIN_TRACE_DECLARE(config, AUDIT_DDL, CRITICAL);
	MAIN_TRACE_DECLARE(config, AUDIT_DCL, CRITICAL);
	MAIN_TRACE_DECLARE(config, REDO_MANAGER, WARNING);
	MAIN_TRACE_DECLARE(config, SITE_REPLICATION, WARNING);

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_TRACE_OUTPUT_TYPE, INT32)
		.setExtendedType(ConfigTable::EXTENDED_TYPE_ENUM)
		.addEnum(util::TraceOption::OUTPUT_NONE, "OUTPUT_NONE")
		.addEnum(
			util::TraceOption::OUTPUT_ROTATION_FILES, "OUTPUT_ROTATION_FILES")
		.addEnum(util::TraceOption::OUTPUT_STDOUT, "OUTPUT_STDOUT")
		.addEnum(util::TraceOption::OUTPUT_STDERR, "OUTPUT_STDERR")
		.addEnum(util::TraceOption::OUTPUT_STDOUT_AND_STDERR,
			"OUTPUT_STDOUT_AND_STDERR")
		.setDefault(util::TraceOption::OUTPUT_ROTATION_FILES);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_TRACE_FILE_COUNT, INT32)
		.setMin(1)
		.setDefault(30);

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_TRACE_AUDIT_LOGS, BOOL)
		.setExtendedType(ConfigTable::EXTENDED_TYPE_LAX_BOOL)
		.setDefault(false);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_TRACE_AUDIT_LOGS_PATH, STRING)
		.setDefault("audit");
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_TRACE_AUDIT_FILE_LIMIT, INT32)
		.setUnit(ConfigTable::VALUE_UNIT_SIZE_MB)
		.setMin(1)
		.setDefault(10);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_TRACE_AUDIT_MESSAGE_LIMIT, INT32)
		.setMin(0)
		.setDefault(1024);
	
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_TRACE_AUDIT_FILE_COUNT, INT32)
		.setMin(0)
		.setDefault(0);

	CONFIG_TABLE_RESOLVE_GROUP(config, CONFIG_TABLE_DEV, "developer");
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DEV_AUTO_JOIN_CLUSTER, BOOL)
		.setExtendedType(ConfigTable::EXTENDED_TYPE_LAX_BOOL)
		.hide(true)
		.setDefault(false);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DEV_RECOVERY, BOOL)
		.setExtendedType(ConfigTable::EXTENDED_TYPE_LAX_BOOL)
		.hide(true)
		.setDefault(true);
	CONFIG_TABLE_ADD_PARAM(
		config, CONFIG_TABLE_DEV_RECOVERY_ONLY_CHECK_CHUNK, BOOL)
		.setExtendedType(ConfigTable::EXTENDED_TYPE_LAX_BOOL)
		.hide(true)
		.setDefault(false);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DEV_RECOVERY_DOWN_POINT, STRING)
		.hide(true)
		.setDefault("");
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DEV_RECOVERY_DOWN_COUNT, INT32)
		.hide(true)
		.setDefault(0);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DEV_CLUSTER_NODE_DUMP, BOOL)
		.setExtendedType(ConfigTable::EXTENDED_TYPE_LAX_BOOL)
		.hide(true)
		.setDefault(false);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DEV_TRACE_SECRET, BOOL)
		.hide(true)
		.setDefault(true);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DEV_SIMPLE_VERSION, STRING)
		.hide(true);
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_DEV_FULL_VERSION, STRING)
		.hide(true);
}

ConfigTable::Constraint &MainConfigSetUpHandler::declareTraceConfigConstraints(
	ConfigTable::Constraint &constraint,
	util::TraceOption::Level defaultLevel) {
	constraint.setExtendedType(ConfigTable::EXTENDED_TYPE_ENUM);
	constraint.setDefault(defaultLevel);

	constraint.addEnum(util::TraceOption::LEVEL_DEBUG, "LEVEL_DEBUG");
	constraint.addEnum(util::TraceOption::LEVEL_INFO, "LEVEL_INFO");
	constraint.addEnum(util::TraceOption::LEVEL_WARNING, "LEVEL_WARNING");
	constraint.addEnum(util::TraceOption::LEVEL_ERROR, "LEVEL_ERROR");
	constraint.addEnum(util::TraceOption::LEVEL_CRITICAL, "LEVEL_CRITICAL");

	return constraint;
}


void setMinOutputLevel(
		const ConfigTable *config, util::TraceManager &manager,
		ConfigTable::ParamId id) {
	util::Tracer &tracer = manager.resolveTracer(
		ParamTable::getParamSymbol(config->getName(id), false, 3).c_str());
	tracer.setMinOutputLevel(config->get<int32_t>(id));
}

void setUpTrace(const ConfigTable *config, bool checkOnly, bool longArchive) {
	util::TraceManager &manager = util::TraceManager::getInstance();

	static GSTraceFormatter formatter(GS_TRACE_SECRET_HEX_KEY);
	if (config == NULL) {
		formatter.setSecret(true);
		manager.setFormatter(&formatter);
		return;
	}

	formatter.setSecret(config->get<bool>(CONFIG_TABLE_DEV_TRACE_SECRET));
	manager.setFormatter(&formatter);

	const char8_t *eventLogPath =
		config->get<const char8_t *>(CONFIG_TABLE_SYS_EVENT_LOG_PATH);
	if (strlen(eventLogPath) > 0) {
		manager.setRotationFilesDirectory(eventLogPath);
	}

	if (longArchive) {
		manager.setRotationFileName("gs_archive_svr");
	} else {
		manager.setRotationFileName("gridstore");
	}

	for (ConfigTable::ParamId id = CONFIG_TABLE_TRACE_TRACER_ID_START;
		 ++id < CONFIG_TABLE_TRACE_TRACER_ID_END;) {
		setMinOutputLevel(config, manager, id);
	}
	for (ConfigTable::ParamId id = CONFIG_TABLE_TRACE_SQL_TRACER_ID_START;
		 ++id < CONFIG_TABLE_TRACE_SQL_TRACER_ID_END;) {
		setMinOutputLevel(config, manager, id);
	}
	manager.setMaxRotationFileCount(
		config->get<int32_t>(CONFIG_TABLE_TRACE_FILE_COUNT));

	manager.setRotationMode(util::TraceOption::ROTATION_DAILY);

	if (!checkOnly || longArchive) {
		manager.setOutputType(static_cast<util::TraceOption::OutputType>(
			config->get<int32_t>(CONFIG_TABLE_TRACE_OUTPUT_TYPE)));
	}
}

void setUpAllocator() {
	util::AllocatorManager &manager =
		util::AllocatorManager::getDefaultInstance();
	const int32_t parentId = ALLOCATOR_GROUP_ROOT;

	manager.addGroup(parentId, ALLOCATOR_GROUP_STORE, "store");
	manager.addGroup(parentId, ALLOCATOR_GROUP_LOG, "log");
	manager.addGroup(parentId, ALLOCATOR_GROUP_CP, "checkpoint");
	manager.addGroup(parentId, ALLOCATOR_GROUP_CS, "cluster");
	manager.addGroup(parentId, ALLOCATOR_GROUP_MAIN, "main");
	manager.addGroup(parentId, ALLOCATOR_GROUP_SYNC, "sync");
	manager.addGroup(parentId, ALLOCATOR_GROUP_SYS, "system");

	manager.addGroup(
		parentId, ALLOCATOR_GROUP_TXN_MESSAGE, "transactionMessage");
	manager.addGroup(parentId, ALLOCATOR_GROUP_TXN_RESULT, "transactionResult");
	manager.addGroup(parentId, ALLOCATOR_GROUP_TXN_WORK, "transactionWork");
	manager.addGroup(parentId, ALLOCATOR_GROUP_TXN_STATE, "transactionState");

	manager.addGroup(parentId, ALLOCATOR_GROUP_SQL_MESSAGE, "sqlMessage");
	manager.addGroup(parentId, ALLOCATOR_GROUP_SQL_WORK, "sqlWork");
	manager.addGroup(parentId, ALLOCATOR_GROUP_SQL_LTS, "sqlTempStore");
	manager.addGroup(parentId, ALLOCATOR_GROUP_SQL_JOB, "sqlJob");
}

void StackMemoryLimitOverErrorHandler::operator()(util::Exception &e) {
	GS_RETHROW_USER_OR_SYSTEM(e, "");
}

void NoSQLCommonUtils::getDatabaseVersion(int32_t &major, int32_t &minor) {
	major = GS_MAJOR_VERSION;
	minor = GS_MINOR_VERSION;
}
