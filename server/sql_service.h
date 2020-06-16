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
	@brief Definition of sql service
*/

#ifndef SQL_SERVICE_H_
#define SQL_SERVICE_H_

#include "sql_common.h"
#include "sql_service_handler.h"

class SQLExecutionManager;
struct SQLTableInfo;
class ResourceSet;
class SQLAllocatorManager;

class SQLService {

	struct Context;
	struct Config;

public:

	static const int32_t SQL_V1_1_X_CLIENT_VERSION;
	static const int32_t SQL_V1_5_X_CLIENT_VERSION;
	static const int32_t SQL_V2_9_X_CLIENT_VERSION;
	static const int32_t SQL_V3_0_X_CLIENT_VERSION;
	static const int32_t SQL_V3_1_X_CLIENT_VERSION;
	static const int32_t SQL_V3_2_X_CLIENT_VERSION;
	static const int32_t SQL_V3_5_X_CLIENT_VERSION;
	static const int32_t SQL_V4_0_0_CLIENT_VERSION;
	static const int32_t SQL_V4_0_1_CLIENT_VERSION;
	static const int32_t SQL_CLIENT_VERSION;

	static const int32_t SQL_V4_0_MSG_VERSION;
	static const int32_t SQL_V4_1_MSG_VERSION;
	static const int32_t SQL_V4_1_0_MSG_VERSION;
	static const int32_t SQL_MSG_VERSION;

	SQLService(
			SQLAllocatorManager &allocatorManager,
			ConfigTable &config,
			EventEngine::Source &eeSource,
			const char *name);

	~SQLService();

	static void checkVersion(int32_t versionId);

	static class ConfigSetUpHandler : public ConfigTable::SetUpHandler {

		virtual void operator()(ConfigTable &config);
	}
	configSetUpHandler_;

	void initialize(ResourceSet &resourceSet);

	void start();

	void shutdown();

	void waitForShutdown();

	EventEngine *getEE() {
		return ee_;
	}

	uint32_t getTargetPosition(
			Event &ev, util::XArray<NodeId> &liveNodeList);

	Context &getContext() {
		return cxt_;
	}


	int32_t getConcurrency() {
		assert(config_.pgConfig_);
		return config_.pgConfig_->getPartitionGroupCount();
	}
	
	int32_t getPartitionGroupId(PartitionId pId) {
		assert(config_.pgConfig_);
		return config_.pgConfig_->getPartitionGroupId(pId);
	}

	SQLVariableSizeGlobalAllocator *getAllocator() {
		return globalVarAlloc_;
	}

	TransactionManager *getTransactionManager() {
		return &txnMgr_;
	}

	void requestCancel(EventContext &ec);
	void sendCancel(EventContext &ec, NodeId nodeId);

	template <class T> void encode(Event &ev, T &t);
	template <class T> void decode(Event &ev, T &t);

	uint64_t incClientSequenceNo() {
		return cxt_.incClientSequenceNo();
	}

	uint64_t getClientSequenceNo() {
		return cxt_.getClientSequenceNo();
	}

	PartitionGroupConfig &getConfig() {
		return *config_.pgConfig_;
	}

	void checkNodeStatus();

	void checkActiveStatus();

	SQLAllocatorManager *getAllocatorManager();

	const ResourceSet *getResourceSet() {
		return resourceSet_;
	}

private:

	void setupEE(const ConfigTable &config,
			EventEngine::Source &eeSource,
			const char *name);

	template<class T>
	void setHandler(GSEventType eventType,
			EventEngine::EventHandlingMode mode
					= EventEngine::HANDLING_PARTITION_SERIALIZED);

	template<class T>
	void setHandlerList(std::vector<GSEventType> &eventTypeList,
			EventEngine::EventHandlingMode mode
					= EventEngine::HANDLING_PARTITION_SERIALIZED);

	void setConnectHandler();

	template<class T>
	void entryHandler(T *handler,
			GSEventType eventType,
			EventEngine::EventHandlingMode mode);

	void setCommonHandler();

	SQLVariableSizeGlobalAllocator *globalVarAlloc_;
	
	const ResourceSet *resourceSet_;

	EventEngine *ee_;
	SQLAllocatorManager *allocatorManager_;

	std::vector<ResourceSetReceiver*> handlerList_;
	std::vector<StatementHandler*> connectHandlerList_;
	ServiceThreadErrorHandler serviceThreadErrorHandler_;

	TransactionManager txnMgr_;

	/*!
		@brief Configuration of SQL service
	*/
	struct Config : public ConfigTable::ParamHandler {

		Config(ConfigTable &configTable);

		~Config();

		void setUpConfigHandler(
				ConfigTable &configTable, SQLService *sqlService);
		
		virtual void operator()(
				ConfigTable::ParamId id, const ParamValue &value);

		PartitionGroupConfig *pgConfig_;
		
		SQLService *sqlService_;
	};
	Config config_;

	/*!
		@brief Context of SQL service
	*/
	struct Context {
		Context() : clientSequenceNo_(0) {}

		uint64_t incClientSequenceNo() {
			return ++clientSequenceNo_;
		}

		uint64_t getClientSequenceNo() {
			return clientSequenceNo_;
		}

		util::Atomic <uint64_t> clientSequenceNo_;
	};

	Context cxt_;

	static class StatSetUpHandler : public StatTable::SetUpHandler {
		virtual void operator()(StatTable &stat);
	} statSetUpHandler_;

	class StatUpdator : public StatTable::StatUpdator {
		virtual bool operator()(StatTable &stat);

	public:
		StatUpdator();
		SQLService *service_;
	} statUpdator_;

};

#endif
