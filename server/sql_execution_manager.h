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
#ifndef SQL_EXECUTION_MANAGER_H_
#define SQL_EXECUTION_MANAGER_H_

#include "util/container.h"
#include "transaction_context.h"
#include "sql_common.h"
#include "sql_request_info.h"
#include "sql_resource_manager.h"

class SQLExecution;
class ResourceSet;
struct SQLProfiler;
struct CancelOption;
class SQLService;
class JobManager;
class NoSQLDB;
struct SQLProcessorConfig;
class SQLAllocatorManager;

class SQLExecutionManager {

	typedef util::AllocMap<ClientId, SQLExecution*> SQLExecutionMap;
	typedef SQLExecutionMap::iterator SQLExecutionMapItr;

	static const int32_t DEFAULT_TABLE_CACHE_SIZE;

	typedef ResourceManager<
			ClientId,
			SQLExecution,
			SQLExecutionRequestInfo,
			SQLVariableSizeGlobalAllocator> SQLExecutionResourceManager;

public:

	SQLExecutionManager(ConfigTable &config,
			SQLAllocatorManager &allocatorManager);

	~SQLExecutionManager();

	void initialize(const ResourceSet &resourceSet);

	void remove(
			EventContext &ec,
			ClientId &clientId,
			bool withCheck = false);

	void remove(SQLExecution *execution);

	void closeConnection(
			EventContext &ec,
			const NodeDescriptor &nd);

	void setRequestedConnectionEnv(
			const RequestInfo &request,
			const NodeDescriptor &nd);
	
	void setConnectionEnv(
			SQLExecution *execution,
			uint32_t paramId,
			const char8_t *value);
	
	void setConnectionEnv(
			const NodeDescriptor &nd,
			uint32_t paramId,
			const char8_t *value);
	
	bool getConnectionEnv(
			SQLExecution *execution,
			uint32_t paramId,
			std::string &value,
			bool &hasData);

	bool getConnectionEnv(
			const NodeDescriptor &nd,
			uint32_t paramId,
			std::string &value,
			bool &hasData);
	
	void completeConnectionEnvUpdates(
			const NodeDescriptor &nd);
	
	uint32_t getConnectionEnvUpdates(
			const NodeDescriptor &nd);

	void dump();

	void dumpTableCache(
			util::NormalOStringStream &oss,
			const char *dbName);

	void resizeTableCache(
			const char *dbName,
			int32_t cacheSize);

	void getProfiler(
			util::StackAllocator &alloc,
			SQLProfiler &profs);

	void getCurrentClientIdList(
			util::Vector<ClientId> &clientIdList);

	bool cancel(EventContext &ec,
			ClientId &clientId);

	void cancelAll(
			const Event::Source &eventSource,
			util::Vector<ClientId> &clientIdList,
			CancelOption &cancelOption);

	void clearConnection(
			Event &ev, bool isNewSQL);
	
	void checkTimeout(EventContext &ec);

	SQLVariableSizeGlobalAllocator &getVarAllocator();
	
	int64_t getFetchSizeLimit() {
		return config_.fetchSizeLimit_;
	}

	int64_t getNoSQLSizeLimit() {
		return config_.nosqlSizeLimit_;
	}

	NoSQLDB *getDB();

	void updateStat(StatTable &stat);

	void updateStatNormal(StatTable &stat);

	void incOperation(
			bool isSQL, int32_t workerId, int64_t count);
	
	uint64_t getTotalReadOperation();

	uint64_t getTotalWriteOperation();
	
	uint64_t incNodeCounter() {
		return ++cxt_.nodeCounter_;
	}

	util::StackAllocator *getStackAllocator();

	void releaseStackAllocator(
			util::StackAllocator *alloc);
	
	const SQLProcessorConfig* getProcessorConfig() const;

	SQLExecutionResourceManager *getResourceManager() {
		return resourceManager_;
	}

	const ResourceSet *getResourceSet() {
		return resourceSet_;
	}

	struct Config : public ConfigTable::ParamHandler {

		Config(ConfigTable &configTable);

		~Config();

		void setUpConfigHandler(ConfigTable &configTable);

		virtual void operator()(
				ConfigTable::ParamId id,
				const ParamValue &value);

		int32_t traceLimitTime_;
		int32_t traceLimitQuerySize_;
		int64_t fetchSizeLimit_;
		int64_t nosqlSizeLimit_;
		bool multiIndexScan_;
		bool partitioningRowKeyConstraint_;
	};

	static class ConfigSetUpHandler :
			public ConfigTable::SetUpHandler {

		virtual void operator()(ConfigTable &config);
	} 
	configSetUpHandler_;

	struct Stats {
		Stats(int32_t concurrency);

		std::vector<uint64_t> totalReadOperationList_;
		std::vector<uint64_t> totalWriteOperationList_;
	};

	static class StatSetUpHandler : public StatTable::SetUpHandler {
		virtual void operator()(StatTable &stat);
	}
	statSetUpHandler_;

	bool isMultiIndexScan() const {
		return config_.multiIndexScan_;
	}

	bool isPartitioningRowKeyConstraint() const {
		return config_.partitioningRowKeyConstraint_;
	}

	int32_t getTraceLimitTime() const {
		return config_.traceLimitTime_;
	};

	int32_t getTraceLimitQuerySize() const {
		return config_.traceLimitQuerySize_;
	};

private:

	struct Context {
		Context() : nodeCounter_(0) {}

		util::Atomic<uint64_t> nodeCounter_;
	};
	
	util::Mutex lock_;
	SQLAllocatorManager *allocatorManager_;
	SQLVariableSizeGlobalAllocator &globalVarAlloc_;
	SQLExecutionResourceManager *resourceManager_;
	const ResourceSet *resourceSet_;
	NoSQLDB *db_;
	SQLProcessorConfig *processorConfig_;
	Stats stats_;
	Config config_;
	Context cxt_;
};

class SQLConnectionEnvironment  {
public:
	
	SQLConnectionEnvironment (
			SQLExecutionManager *executionMgr,
			SQLExecution *execution);
	
	void setEnv(uint32_t paramId, std::string &key);
	
	bool getEnv(uint32_t paramId, std::string &key);

	SQLExecutionManager *getExecutionManager() {
		return executionMgr_;
	}

private:
	
	SQLExecutionManager *executionMgr_;
	SQLExecution *execution_;
	bool hasData_;
};

typedef ResourceLatch<
		ClientId,
		SQLExecution,
		SQLExecutionRequestInfo,
		ResourceManager<
				ClientId,
				SQLExecution,
				SQLExecutionRequestInfo,
				SQLVariableSizeGlobalAllocator> > ExecutionLatch;

#endif
