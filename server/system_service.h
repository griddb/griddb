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
	@brief Definition of SystemService
*/

#ifndef SYSTEM_SERVICE_H_
#define SYSTEM_SERVICE_H_

#include "socket_wrapper.h"
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
class DataStoreV4;
class NoLocker;
class MutexLocker;
template <class L> class LogManager;
struct ManagerSet;
class PartitionList;
class SQLService;

struct ebb_request_parser;
struct ebb_request;

typedef util::FixedSizeAllocator<util::Mutex> GlobalFixedSizeAllocator;
typedef util::VariableSizeAllocator<util::Mutex> GlobalVariableSizeAllocator;

namespace picojson {
class value;
}

struct RestContext {
	RestContext(picojson::value& result) : result_(result), errorNo_(WEBAPI_NORMAL) {}
	picojson::value& result_;
	WebAPIErrorType errorNo_;
	std::string reason_;
	void setError(int32_t errorNo, const char* reason);
};

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

	typedef std::pair<int32_t, bool> ServiceTypeInfo;

	SystemService(
			ConfigTable &config, const EventEngine::Config &eeConfig,
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


	bool clearUserCache(const std::string &name,
		bool isDatabase, picojson::value &result);
	
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

	void getHosts(picojson::value &result, const ServiceTypeInfo &addressType);

	void getStats(picojson::value &result, StatTable &stats);
	void traceStats();

	void getSyncStats(
			picojson::value &result, const ServiceTypeInfo &addressType);

	void getPGStoreMemoryLimitStats(picojson::value &result);

	void getMemoryStats(
			picojson::value &result, const char8_t *namePrefix,
			const char8_t *selectedType, int64_t minSize);

	void getSqlTempStoreStats(picojson::value &result);


	bool getOrSetConfig(
			util::StackAllocator &alloc,
			const std::vector<std::string> namePath, picojson::value &result,
			const picojson::value *paramValue, bool noUnit);

	void getPartitions(
		util::StackAllocator& alloc,
		picojson::value& result, int32_t partitionNo,
		const ServiceTypeInfo& addressType, bool lossOnly,
		bool force, bool isSelf, bool lsnDump,
		bool notDumpRole, uint32_t partitionGroupNo, bool sqlOwnerDump, bool replication);


	bool setGoalPartitions(util::StackAllocator &alloc,
		const picojson::value *request, picojson::value &result);

	void getGoalPartitions(
			util::StackAllocator &alloc, const ServiceTypeInfo &addressType,
			picojson::value &result);

	void getDatabaseContraint(
		util::StackAllocator& alloc, const ServiceTypeInfo& addressType,
		DatabaseId dbId, picojson::value& result);

	void getLogs(
			picojson::value &result, std::string &searchStr,
			std::string &searchStr2, std::string &ignoreStr, uint32_t length);

	void getAuditLogs(
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

	enum SslMode {
		SSL_MODE_DEFAULT,
		SSL_MODE_DISABLED,
		SSL_MODE_PREFERRED,
		SSL_MODE_REQUIRED,
		SSL_MODE_VERIFY 
	};

	struct Tester;


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
		WebAPIServerThread(bool enabled, bool secure);

		bool isEnabled() const;
		void initialize(ManagerSet &mgrSet);

		void tryStart();
		virtual void run();

		void shutdown();
		void waitForShutdown();

	private:
		virtual void start(
				util::ThreadRunner *runner = NULL,
				const util::ThreadAttribute *attr = NULL);

		volatile bool runnable_;
		const bool enabled_;
		const bool secure_;

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
		ListenerSocketHandler(ManagerSet &mgrSet, bool secure);

		virtual void handlePollEvent(
				util::IOPollBase *io, util::IOPollEvent event);
		virtual util::Socket &getFile();

	private:
		void dispatch(WebAPIRequest &request, WebAPIResponse &response);

		ServiceTypeInfo getDefaultAddressType(
				const std::map<std::string, std::string> &parameterMap) const;

		static bool readOnce(
				AbstractSocket &socket, util::NormalXArray<char8_t> &buffer);
		static SocketFactory& resolveSocketFactory(
				SystemService *sysSvc, bool secure);
		static std::string getURI(WebAPIRequest &request);		
		static std::string getSenderName(AbstractSocket *socket);		
		static std::string getNodeName(AbstractSocket *socket);
		std::string getStatementName(std::string uriOperator);
		static std::string getErrorInfo(WebAPIResponse &response);

		util::Socket listenerSocket_;

		ClusterService *clsSvc_;
		SyncService *syncSvc_;
		SystemService *sysSvc_;
		TransactionService *txnSvc_;
		PartitionTable *pt_;
		SyncManager *syncMgr_;
		ClusterManager *clsMgr_;
		PartitionList* partitionList_;
		CheckpointService *cpSvc_;
		GlobalFixedSizeAllocator *fixedSizeAlloc_;
		EventEngine::VariableSizeAllocator varSizeAlloc_;
		util::StackAllocator alloc_;
		SocketFactory &socketFactory_;
		const bool secure_;
		std::unordered_map<std::string, std::string> uriToStatement_;
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
		void setError(int32_t errorNo, const char* reason, uint32_t status);
		bool checkParamValue(const char* paramName, int64_t& value, bool isInteger);

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


	EventEngine::Config createEEConfig(
			const ConfigTable &config, const EventEngine::Config &src);

	static ServiceTypeInfo resolveAddressType(
			const WebAPIRequest::ParameterMap &parameterMap,
			const ServiceTypeInfo &defaultType);

	static bool acceptStatsOption(
			StatTable &stats, const WebAPIRequest::ParameterMap &parameterMap,
			const ServiceTypeInfo *defaultType);

	static ServiceTypeInfo statOptionToAddressType(const StatTable &stat);


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
	PartitionList* partitionList_;
	TransactionService *txnSvc_;
	TransactionManager *txnMgr_;
	RecoveryManager *recoveryMgr_;
	SyncManager *syncMgr_;
	SQLService *sqlSvc_;

	GlobalFixedSizeAllocator *fixedSizeAlloc_;
	GlobalVariableSizeAllocator *varSizeAlloc_;

	const std::string multicastAddress_;
	const int32_t multicastPort_;
	const std::string outputDiffFileName_;

	WebAPIServerThread webapiServerThread_;
	WebAPIServerThread secureWebapiServerThread_;

	SystemConfig sysConfig_;

	bool initialized_;

	std::set<std::string> unchangeableTraceCategories_;

	ConfigTable &config_;
	StatTable *baseStats_;

	std::set<std::string> moduleList_;

	SocketFactory *socketFactory_;
	std::pair<SocketFactory*, SocketFactory*> secureSocketFactories_;


};

#endif
