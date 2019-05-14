﻿/*
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
	@brief Definition of SystemService
*/

#ifndef SYSTEM_SERVICE_H_
#define SYSTEM_SERVICE_H_

#include "util/net.h"
#include "cluster_common.h"
#include "config_table.h"
#include "data_type.h"
#include "event_engine.h"
#include "gs_error.h"

class EventEngine;
class PartitionTable;

class CheckpointService;
class ChunkManager;
class ClusterService;
class ClusterManager;
class SystemService;
class TransactionService;
class TransactionManager;
class SyncManager;
class SyncService;
class RecoveryManager;
class DataStore;
class LogManager;
struct ManagerSet;

struct ebb_request_parser;
struct ebb_request;

typedef util::FixedSizeAllocator<util::Mutex> GlobalFixedSizeAllocator;
typedef util::VariableSizeAllocator<util::Mutex> GlobalVariableSizeAllocator;

namespace picojson {
class value;
}

/*!
	@brief Handles thread error for service
*/
class ServiceThreadErrorHandler : public EventEngine::ThreadErrorHandler {
public:
	ServiceThreadErrorHandler() : clsSvc_(NULL) {}

	void operator()(EventContext &ec, std::exception &);

	void initialize(const ManagerSet &mgrSet);

private:
	ClusterService *clsSvc_;
};

/*!
	@brief Handles statistics output
*/
class OutputStatsHandler : public EventHandler {
public:
	OutputStatsHandler() : sysSvc_(NULL), pt_(NULL), clsMgr_(NULL) {};

	void operator()(EventContext &ec, Event &ev);

	void initialize(ManagerSet &mgrSet);

private:
	SystemService *sysSvc_;
	PartitionTable *pt_;
	ClusterManager *clsMgr_;
};

/*!
	@brief SystemService
*/
class SystemService {
	class SystemConfig;

public:
	/*!
		@brief Trace mode
	*/
	enum TraceMode {
		TRACE_MODE_SIMPLE,
		TRACE_MODE_SIMPLE_DETAIL,
		TRACE_MODE_FULL
	};

	SystemService(ConfigTable &config, EventEngine::Config eeConfig,
		EventEngine::Source source, const char8_t *name,
		const char8_t *diffFilePath,
		ServiceThreadErrorHandler &serviceThreadErrorHandler);

	~SystemService();

	void start();

	void shutdown();

	void waitForShutdown();

	void initialize(ManagerSet &mgrSet);

	SystemConfig &getConfig() {
		return sysConfig_;
	};


	bool joinCluster(const Event::Source &eventSource,
		util::StackAllocator &alloc, const std::string &clusterName,
		uint32_t minNodeNum, picojson::value &result);

	bool joinCluster(const Event::Source &eventSource,
		util::StackAllocator &alloc, const std::string &clusterName,
		uint32_t minNodeNum);

	bool leaveCluster(const Event::Source &eventSource,
		util::StackAllocator &alloc, bool isForce = true);

	void shutdownNode(const Event::Source &eventSource,
		util::StackAllocator &alloc, bool force = true);

	void shutdownCluster(
		const Event::Source &eventSource, util::StackAllocator &alloc);
	void increaseCluster(
		const Event::Source &eventSource, util::StackAllocator &alloc);

	bool decreaseCluster(const Event::Source &eventSource,
		util::StackAllocator &alloc, picojson::value &result,
		bool isAutoLeave = false, int32_t leaveNum = 1);
	struct BackupOption {
		bool logArchive_;
		bool logDuplicate_;
		bool stopOnDuplicateError_;
		bool isIncrementalBackup_;
		int32_t incrementalBackupLevel_;
		bool isCumulativeBackup_;
		bool skipBaseline_;
	};


	bool backupNode(
			const Event::Source &eventSource, const char8_t *backupName,
			int32_t mode, 
			BackupOption &option,
			picojson::value &result);

	bool archiveLog(
			const Event::Source &eventSource, const char8_t *backupName,
			int32_t mode, picojson::value &result);

	bool prepareLongArchive(
			const Event::Source &eventSource, const char8_t *longArchiveName,
			picojson::value &result);

	void checkpointNode(const Event::Source &eventSource);

	void setPeriodicCheckpointFlag(bool flag);  

	void getHosts(picojson::value &result, int32_t addressTypeNum);

	void getStats(picojson::value &result, StatTable &stats);

	void getSyncStats(picojson::value &result);

	void getPGStoreMemoryLimitStats(picojson::value &result);

	void getMemoryStats(
			picojson::value &result, const char8_t *namePrefix,
			const char8_t *selectedType, int64_t minSize);


	bool getOrSetConfig(
			util::StackAllocator &alloc,
			const std::vector<std::string> namePath, picojson::value &result,
			const picojson::value *paramValue, bool noUnit);

	void getPartitions(
			picojson::value &result, int32_t partitionNo,
			int32_t addressTypeNum, bool lossOnly = false,
			bool force = false, bool isSelf = false, bool lsnDump = false,
			bool notDumpRole = false, uint32_t partitionGroupNo = UINT32_MAX, bool sqlOwnerDump=false);

	void getGoalPartitions(util::StackAllocator &alloc, picojson::value &result);

	void getLogs(
			picojson::value &result, std::string &searchStr,
			std::string &searchStr2, std::string &ignoreStr, uint32_t length);

	bool setEventLogLevel(
			const std::string &categoryName, const std::string &level,
			bool force);

	void getEventLogLevel(picojson::value &result);

	bool getEventStats(picojson::value &result, bool reset);
	void testEventLogLevel();


	bool getSQLProcessorProfile(
			util::StackAllocator &alloc,
			EventEngine::VariableSizeAllocator &varSizeAlloc,
			picojson::value &result, const int64_t *id);

	bool getSQLProcessorGlobalProfile(picojson::value &result);

	bool setSQLProcessorConfig(
			const char8_t *key, const char8_t *value, bool forProfiler);
	bool getSQLProcessorConfig(bool forProfiler, picojson::value &result);

	bool getSQLProcessorPartialStatus(picojson::value &result);

	bool setSQLProcessorSimulation(
			util::StackAllocator &alloc, const picojson::value &request);
	bool getSQLProcessorSimulation(
			util::StackAllocator &alloc, picojson::value &result);

	bool getSQLCompilerProfile(
			util::StackAllocator &alloc, int32_t index,
			const util::Set<util::String> &filteringSet,
			picojson::value &result);

	bool setSQLCompilerConfig(
			const char8_t *category, const char8_t *key, const char8_t *value);
	bool getSQLCompilerConfig(
			const char8_t *category, picojson::value &result);


	EventEngine *getEE() {
		return &ee_;
	};

	UserTable &getUserTable() {
		return userTable_;
	}

	void traceStats(const util::StdAllocator<void, void> &alloc);

	GlobalVariableSizeAllocator *getVariableSizeAllocator() {
		return varSizeAlloc_;
	}

	struct Tester;
	struct FailureSimulator;

private:
	static const uint32_t WORKER_THREAD_NUM = 1;
	static const uint32_t ACCEPT_POLLING_MILLISEC = 500;
	static const uint32_t MAX_REQUEST_SIZE = 10 * 1024;

	static const int32_t OUTPUT_STATS_INTERVAL_SEC =
		60 * 5;  

	static const ServiceType DEFAULT_ADDRESS_TYPE = SYSTEM_SERVICE;

	/*!
		@brief Thread for Web API server
	*/
	class WebAPIServerThread : public util::Thread {
	public:
		WebAPIServerThread();

		void initialize(ManagerSet &mgrSet);
		virtual void run();
		void shutdown();
		void waitForShutdown();

	private:
		volatile bool runnable_;
		SystemService *sysSvc_;
		ClusterService *clsSvc_;
		ManagerSet *mgrSet_;
	};

	struct WebAPIRequest;
	struct WebAPIResponse;

	/*!
		@brief Handles ListenerSocket
	*/
	class ListenerSocketHandler : public util::IOPollHandler {
	public:
		explicit ListenerSocketHandler(ManagerSet &mgrSet);

		virtual void handlePollEvent(
			util::IOPollBase *io, util::IOPollEvent event);
		virtual util::Socket &getFile();

	private:
		void dispatch(WebAPIRequest &request, WebAPIResponse &response);

		static bool readOnce(
			util::Socket &socket, util::NormalXArray<char8_t> &buffer);
		util::Socket listenerSocket_;

		ClusterService *clsSvc_;
		SyncService *syncSvc_;
		SystemService *sysSvc_;
		TransactionService *txnSvc_;
		PartitionTable *pt_;
		SyncManager *syncMgr_;
		ClusterManager *clsMgr_;
		ChunkManager *chunkMgr_;
		CheckpointService *cpSvc_;
		DataStore *dataStore_;
		GlobalFixedSizeAllocator *fixedSizeAlloc_;
		EventEngine::VariableSizeAllocator varSizeAlloc_;
		util::StackAllocator alloc_;
	};

	/*!
		@brief Represents request for Web API
	*/
	struct WebAPIRequest {
		typedef std::map<std::string, std::string> ParameterMap;
		typedef std::map<std::string, std::string> HeaderValues;

		WebAPIRequest();

		bool set(util::NormalXArray<char8_t> &input);
		void acceptQueryString(const char8_t *begin, const char8_t *end);
		static void acceptPath(
			ebb_request *request, const char8_t *at, size_t length);
		static void acceptQueryString(
			ebb_request *request, const char8_t *at, size_t length);
		static void acceptHeaderField(
			ebb_request *request, const char8_t *at, size_t length, int index);
		static void acceptHeaderValue(
			ebb_request *request, const char8_t *at, size_t length, int index);
		static ebb_request *getParserRequest(void *data);

		int32_t method_;
		std::vector<std::string> pathElements_;
		ParameterMap parameterMap_;
		HeaderValues headerValues_;
		std::string lastHeaderField_;
		std::string user_;
		std::string password_;
		int32_t minorVersion_;
		UTIL_UNIQUE_PTR<picojson::value> jsonValue_;
		std::string jsonError_;
	};

	/*!
		@brief Represents response for Web API
	*/
	struct WebAPIResponse {
		WebAPIResponse(WebAPIRequest &request);

		void setMethodError();
		void setAuthError();
		void setBadRequestError();
		void setInternalServerError();

		void setJson(const picojson::value &value);
		void setJson(const std::string &callback, const picojson::value &value);
		void update();

		WebAPIRequest &request_;
		uint32_t statusCode_;
		bool hasJson_;
		std::string header_;
		std::string body_;
	};

	/*!
		@brief Represents config for SystemService
	*/
	class SystemConfig : public ConfigTable::ParamHandler {
	public:
		explicit SystemConfig(ConfigTable &configTable);

		int32_t getOutputStatsInterval() const {
			return sysStatsInterval_;
		}

		TraceMode getTraceMode() const {
			return traceMode_;
		}

	private:
		void setUpConfigHandler(ConfigTable &configTable);

		virtual void operator()(
			ConfigTable::ParamId id, const ParamValue &value);

		int32_t sysStatsInterval_;
		TraceMode traceMode_;
	};


	EventEngine::Config &createEEConfig(
		const ConfigTable &config, EventEngine::Config &eeConfig);

	static int32_t resolveAddressType(
		const WebAPIRequest::ParameterMap &parameterMap,
		ServiceType defaultType);

	static bool acceptStatsOption(
		StatTable &stats, const WebAPIRequest::ParameterMap &parameterMap);


	static class ConfigSetUpHandler : public ConfigTable::SetUpHandler {
		virtual void operator()(ConfigTable &config);
	} configSetUpHandler_;

	static class StatSetUpHandler : public StatTable::SetUpHandler {
		virtual void operator()(StatTable &stat);
	} statSetUpHandler_;

	class StatUpdator : public StatTable::StatUpdator {
		virtual bool operator()(StatTable &stat);

	public:
		SystemService *service_;
	} statUpdator_;

	EventEngine ee_;

	std::string gsVersion_;
	UserTable userTable_;

	OutputStatsHandler outputStatsHandler_;
	ServiceThreadErrorHandler serviceThreadErrorHandler_;

	ClusterService *clsSvc_;
	ClusterManager *clsMgr_;
	SyncService *syncSvc_;
	PartitionTable *pt_;
	CheckpointService *cpSvc_;
	ChunkManager *chunkMgr_;
	DataStore *dataStore_;
	TransactionService *txnSvc_;
	TransactionManager *txnMgr_;
	RecoveryManager *recoveryMgr_;
	LogManager *logMgr_;
	SyncManager *syncMgr_;

	GlobalFixedSizeAllocator *fixedSizeAlloc_;
	GlobalVariableSizeAllocator *varSizeAlloc_;

	const std::string multicastAddress_;
	const int32_t multicastPort_;
	const std::string outputDiffFileName_;

	WebAPIServerThread webapiServerThread_;

	SystemConfig sysConfig_;

	bool initailized_;

	std::set<std::string> unchangableTraceCategories_;

	ConfigTable &config_;
	StatTable *baseStats_;

	std::set<std::string> moduleList_;


};

#endif
