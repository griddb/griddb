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

#include "sql_service.h"
#include "sql_execution.h"
#include "sql_execution_manager.h"
#include "base_container.h"
#include "message_schema.h"
#include "sql_utils.h"
#include "sql_allocator_manager.h"
#include "query_processor.h"
#include "sql_table_schema.h"
#include "cluster_manager.h"

UTIL_TRACER_DECLARE(SQL_SERVICE);

SQLService::ConfigSetUpHandler
		SQLService::configSetUpHandler_;

const int32_t SQLService::SQL_V1_1_X_CLIENT_VERSION = -1;
const int32_t SQLService::SQL_V1_5_X_CLIENT_VERSION = -2;
const int32_t SQLService::SQL_V2_9_X_CLIENT_VERSION = -3;
const int32_t SQLService::SQL_V3_0_X_CLIENT_VERSION = -4;
const int32_t SQLService::SQL_V3_1_X_CLIENT_VERSION = -5;
const int32_t SQLService::SQL_V3_2_X_CLIENT_VERSION = -6;
const int32_t SQLService::SQL_V3_5_X_CLIENT_VERSION = -7;
const int32_t SQLService::SQL_V4_0_0_CLIENT_VERSION = -8;
const int32_t SQLService::SQL_V4_0_1_CLIENT_VERSION = -9;
const int32_t SQLService::SQL_CLIENT_VERSION = SQL_V4_0_1_CLIENT_VERSION;

const int32_t SQLService::SQL_V4_0_MSG_VERSION = 1;
const int32_t SQLService::SQL_V4_1_MSG_VERSION = 2;
const int32_t SQLService::SQL_V4_1_0_MSG_VERSION = 3;
const int32_t SQLService::SQL_MSG_VERSION = SQL_V4_1_0_MSG_VERSION;

static const int32_t ACCEPTABLE_NEWSQL_CLIENT_VERSIONS[] = {
		SQLService::SQL_CLIENT_VERSION,
		SQLService::SQL_V4_0_0_CLIENT_VERSION,
		0 /* sentinel */
};


SQLService::SQLService(
		SQLAllocatorManager &allocatorManager,
		ConfigTable &config,
		EventEngine::Source &eeSource,
		const char *name) :
				globalVarAlloc_(
						allocatorManager.getGlobalAllocator()),
				resourceSet_(NULL),
				ee_(NULL),
				allocatorManager_(&allocatorManager),
				txnMgr_(config, true),
				config_(config) {

	try {

		setupEE(config, eeSource, name);
	
		setConnectHandler();

		setHandler<DisconnectHandler>(
				DISCONNECT,
				EventEngine::HANDLING_IMMEDIATE);
		connectHandlerList_.push_back(
				static_cast<StatementHandler*>(
						handlerList_.back()));

		setHandler<LoginHandler>(LOGIN);
		connectHandlerList_.push_back(
				static_cast<StatementHandler*>(
						handlerList_.back()));

		setHandler<SQLGetConnectionAddressHandler>(
				GET_PARTITION_ADDRESS);

		setHandler<SQLRequestHandler>(
				SQL_EXECUTE_QUERY);

		setHandler<NoSQLSyncReceiveHandler>(
				SQL_RECV_SYNC_REQUEST,
				EventEngine::HANDLING_IMMEDIATE);

		setHandler<ExecuteJobHandler>(
				EXECUTE_JOB);

		setHandler<ControlJobHandler>(
				CONTROL_JOB,
				EventEngine::HANDLING_IMMEDIATE);

		setHandler<AuthenticationAckHandler>(
				AUTHENTICATION_ACK);
		connectHandlerList_.push_back(
				static_cast<StatementHandler*>(
						handlerList_.back()));

		setHandler<CheckTimeoutHandler>(
				TXN_COLLECT_TIMEOUT_RESOURCE);

		std::vector<GSEventType> typeList;
		typeList.push_back(CHECK_TIMEOUT_JOB);
		typeList.push_back(REQUEST_CANCEL);
		setHandlerList<SQLResourceCheckHandler>(
				typeList,
				EventEngine::HANDLING_IMMEDIATE);

		setHandler<DispatchJobHandler>(
				DISPATCH_JOB,
				EventEngine::HANDLING_IMMEDIATE);

		setHandler<UpdateTableCacheHandler>(
				UPDATE_TABLE_CACHE,
				EventEngine::HANDLING_IMMEDIATE);

		setHandler<SQLCancelHandler>(
				SQL_CANCEL_QUERY,
				EventEngine::HANDLING_IMMEDIATE);

		setHandler<SQLRequestNoSQLClientHandler>(
				SQL_REQUEST_NOSQL_CLIENT);

		setHandler<IgnorableStatementHandler>(
				SQL_NOTIFY_CLIENT,
				EventEngine::HANDLING_IMMEDIATE);

		setCommonHandler();

		config_.setUpConfigHandler(config, this);
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(
				e, "SQL service initialize failed");
	}
}

SQLService::~SQLService() {

	ee_->shutdown();
	ee_->waitForShutdown();

	delete ee_;
}

void SQLService::initialize(ResourceSet &resourceSet) {

	try {
		resourceSet_ = &resourceSet;

		for (size_t pos = 0; pos < handlerList_.size(); pos++) {
			handlerList_[pos]->initialize(resourceSet);
		}

		for (size_t pos = 0;
				pos < connectHandlerList_.size(); pos++) {
			connectHandlerList_[pos]->setNewSQL();
		}

		serviceThreadErrorHandler_.initialize(resourceSet);

		statUpdator_.service_ = this;
		resourceSet.stats_->addUpdator(&statUpdator_);
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "SQL service initialize failed");
	}
}

void SQLService::start() {
	
	try {
		ee_->start();
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, "SQL service start failed");
	}
}

void SQLService::shutdown() {
	ee_->shutdown();
}

void SQLService::waitForShutdown() {
	ee_->waitForShutdown();
}

template<class T>
void SQLService::setHandler(
		GSEventType eventType, EventEngine::EventHandlingMode mode) {

	T *handler = NULL;
	try {
		handler = UTIL_NEW T;
		entryHandler(handler, eventType, mode);
	}
	catch (std::exception &e) {
		if (handler) {
			delete handler;
		}
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

template<class T>
void SQLService::setHandlerList(
		std::vector<GSEventType> &eventTypeList,
		EventEngine::EventHandlingMode mode) {

	T *handler = NULL;
	try {
		handler = UTIL_NEW T;
		for (size_t pos = 0; pos < eventTypeList.size(); pos++) {
			ee_->setHandler(eventTypeList[pos], *handler);
			ee_->setHandlingMode(eventTypeList[pos], mode);
		}
		handlerList_.push_back(handler);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void SQLService::setConnectHandler() {

	ConnectHandler *handler = NULL;
	try {
		handler = UTIL_NEW ConnectHandler(
				SQL_CLIENT_VERSION, ACCEPTABLE_NEWSQL_CLIENT_VERSIONS);
		entryHandler(
				handler, CONNECT, EventEngine::HANDLING_IMMEDIATE);
		connectHandlerList_.push_back(handler);
	}
	catch (std::exception &e) {
		if (handler) {
			delete handler;
		}
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void SQLService::setCommonHandler() {	

	try {
		{
			UTIL_UNIQUE_PTR<SQLSocketDisconnectHandler> handler(
					UTIL_NEW SQLSocketDisconnectHandler);
			ee_->setDisconnectHandler(*handler);
			handlerList_.push_back(handler.get());
			handler.release();
		}

		{
			UTIL_UNIQUE_PTR<UnknownStatementHandler> handler(
					UTIL_NEW UnknownStatementHandler);
			ee_->setUnknownEventHandler(*handler);
			handlerList_.push_back(handler.get());
			handler.release();
		}

		{
			ee_->setThreadErrorHandler(serviceThreadErrorHandler_);
		}

		ee_->setUserDataType<StatementHandler::ConnectionOption>();
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

template<class T>
void SQLService::entryHandler(T *handler,
		GSEventType eventType, EventEngine::EventHandlingMode mode) {

	ee_->setHandler(eventType, *handler);
	ee_->setHandlingMode(eventType, mode);
	handlerList_.push_back(handler);
}
void SQLService::requestCancel(EventContext &ec) {

	util::StackAllocator &alloc = ec.getAllocator();
	util::Vector<ClientId> clientIdList(alloc);
	resourceSet_->getSQLExecutionManager()
			->getCurrentClientIdList(clientIdList);
	
	for (size_t pos = 0; pos < clientIdList.size(); pos++) {

		Event request(ec, SQL_CANCEL_QUERY, IMMEDIATE_PARTITION_ID);
		EventByteOutStream out = request.getOutStream();
		out << MAX_STATEMENTID;
		
		StatementHandler::encodeUUID(out, clientIdList[pos].uuid_,
				TXN_CLIENT_UUID_BYTE_SIZE);
		out << clientIdList[pos].sessionId_;
		
		uint8_t flag = 1;
		out << flag;
		ee_->add(request);
	}
	util::Vector<JobId> jobIdList(alloc);

	JobManager *jobManager = resourceSet_->getJobManager();

	CancelOption option;
	jobManager->cancelAll(ec, jobIdList, option);
}

void SQLService::sendCancel(EventContext &ec, NodeId nodeId) {

	try {

		Event request(ec, REQUEST_CANCEL, IMMEDIATE_PARTITION_ID);
		const NodeDescriptor &nd = ee_->getServerND(nodeId);
		
		if (nodeId == 0) {
			request.setSenderND(nd);
		}
		ee_->send(request, nd);
	}
	catch (std::exception &) {
	}
}

void SQLService::ConfigSetUpHandler::operator()(ConfigTable &config) {

	CONFIG_TABLE_RESOLVE_GROUP(config, CONFIG_TABLE_SQL, "sql");

	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_SQL_NOTIFICATION_ADDRESS, STRING).
			inherit(CONFIG_TABLE_ROOT_NOTIFICATION_ADDRESS);

	CONFIG_TABLE_ADD_PORT_PARAM(
			config, CONFIG_TABLE_SQL_NOTIFICATION_PORT, 41999);
	
	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_SQL_NOTIFICATION_INTERVAL, INT32).
			setUnit(ConfigTable::VALUE_UNIT_DURATION_S).
			setMin(1).
			setDefault(5);
	
	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_SQL_SERVICE_ADDRESS, STRING).
			inherit(CONFIG_TABLE_ROOT_SERVICE_ADDRESS);
	
	CONFIG_TABLE_ADD_PORT_PARAM(
			config, CONFIG_TABLE_SQL_SERVICE_PORT, 20001);
	
	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_SQL_CONCURRENCY, INT32).
			setMin(1).
			setMax(128).
			setDefault(4);
	
	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_SQL_CONNECTION_LIMIT, INT32).
			setMin(3).
			setMax(65536).
			setDefault(5000);
	
	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_SQL_TOTAL_MEMORY_LIMIT, INT32).
			setUnit(ConfigTable::VALUE_UNIT_SIZE_MB).
			setMin(1).
			setDefault(1024);
	
	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_SQL_USE_KEEPALIVE, BOOL).
			setExtendedType(ConfigTable::EXTENDED_TYPE_LAX_BOOL).
			setDefault(true);
	
	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_SQL_KEEPALIVE_IDLE, INT32).
			setUnit(ConfigTable::VALUE_UNIT_DURATION_S).
			setMin(0).
			setDefault(600);
	
	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_SQL_KEEPALIVE_INTERVAL, INT32).
			setUnit(ConfigTable::VALUE_UNIT_DURATION_S).
			setMin(0).
			setDefault(60);
	
	CONFIG_TABLE_ADD_PARAM(
			config, CONFIG_TABLE_SQL_KEEPALIVE_COUNT, INT32).
			setMin(0).
			setDefault(5);
	
	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_SQL_LOCAL_SERVICE_ADDRESS, STRING)
		.setDefault("");

	CONFIG_TABLE_ADD_PARAM(config,
			CONFIG_TABLE_SQL_NOTIFICATION_INTERFACE_ADDRESS, STRING)
		.setDefault("");
}

template <class T> void SQLService::encode(Event &ev, T &t) {

	try {
		try {
			EventByteOutStream out = ev.getOutStream();
			util::ObjectCoder::withAllocator(globalVarAlloc_).encode(out, t);
		}
		catch (std::exception &e) {
			GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(
					e, "Failed to encode message"));
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
}

template void SQLService::encode(Event &ev, TableSchemaInfo &t);

template <class T> void SQLService::decode(Event &ev, T &t) {

	try {
		try {
			EventByteInStream in = ev.getInStream();
			util::ObjectCoder::withAllocator(*globalVarAlloc_).decode(in, t);
		}
		catch (std::exception &e) {
			GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(
					e, "Failed to encode message"));
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
}

template void SQLService::decode(Event &ev, TableSchemaInfo &t);

SQLService::StatSetUpHandler SQLService::statSetUpHandler_;

#define STAT_ADD(id) STAT_TABLE_ADD_PARAM(stat, parentId, id)

void SQLService::StatSetUpHandler::operator()(StatTable &stat) {
	StatTable::ParamId parentId;

	parentId = STAT_TABLE_ROOT;
	stat.resolveGroup(parentId, STAT_TABLE_PERF, "performance");

	parentId = STAT_TABLE_PERF;
	STAT_ADD(STAT_TABLE_PERF_SQL_NUM_CONNECTION);
}

bool SQLService::StatUpdator::operator()(StatTable &stat) {

	if (!stat.getDisplayOption(STAT_TABLE_DISPLAY_SELECT_PERF)) {
		return true;
	}

	SQLService &svc = *service_;

	EventEngine::Stats sqlSvcStats;
	svc.getEE()->getStats(sqlSvcStats);

	const uint64_t numClientTCPConnection =
			sqlSvcStats.get(EventEngine::Stats::ND_CLIENT_CREATE_COUNT) -
			sqlSvcStats.get(EventEngine::Stats::ND_CLIENT_REMOVE_COUNT);

	const uint64_t numReplicationTCPConnection =
			sqlSvcStats.get(EventEngine::Stats::ND_SERVER_CREATE_COUNT) -
			sqlSvcStats.get(EventEngine::Stats::ND_SERVER_REMOVE_COUNT);

	const uint64_t numClientUDPConnection =
			sqlSvcStats.get(EventEngine::Stats::ND_MCAST_CREATE_COUNT) -
			sqlSvcStats.get(EventEngine::Stats::ND_MCAST_REMOVE_COUNT);
	
	const uint64_t numConnection = numClientTCPConnection +
			numReplicationTCPConnection + numClientUDPConnection;

	stat.set(STAT_TABLE_PERF_SQL_NUM_CONNECTION, numConnection);

	return true;
}

SQLService::StatUpdator::StatUpdator() : service_(NULL) {}

void SQLService::checkVersion(int32_t versionId) {
	if (versionId > SQL_MSG_VERSION) {
		GS_THROW_CUSTOM_ERROR(DenyException,
				GS_ERROR_SQL_MSG_VERSION_NOT_ACCEPTABLE,
				"(receiveVersion=" << versionId
				<< ", acceptableVersion=" << SQL_MSG_VERSION);
	}
}

void SQLService::Config::setUpConfigHandler(
		ConfigTable &configTable, SQLService *sqlService) {
	
	sqlService_ = sqlService;
	configTable.setParamHandler(
			CONFIG_TABLE_SQL_CONCURRENCY, *this);
}

SQLService::Config::Config(ConfigTable &configTable) :
		pgConfig_(NULL),
		sqlService_(NULL) {
	
	pgConfig_ = UTIL_NEW PartitionGroupConfig(
			DataStore::MAX_PARTITION_NUM,
					configTable.get<int32_t>(CONFIG_TABLE_SQL_CONCURRENCY));
}

SQLService::Config::~Config() {
	if (pgConfig_) {
		delete pgConfig_;
		pgConfig_ = NULL;
	}
}

void SQLService::Config::operator() (
		ConfigTable::ParamId id, const ParamValue &value) {

	switch (id) {

	case CONFIG_TABLE_SQL_CONCURRENCY: {

		return;

		if (pgConfig_) {
			delete pgConfig_;
			pgConfig_ = NULL;
		}
		pgConfig_ = UTIL_NEW PartitionGroupConfig(
				DataStore::MAX_PARTITION_NUM, value.get<int32_t>());
		int32_t currentConcurrency = pgConfig_->getPartitionGroupCount();

		sqlService_->getAllocatorManager()->resizeConcurrency(
				SQL_SERVICE, currentConcurrency);
	}
	break;
	default:
		break;
	}
}

void SQLService::checkActiveStatus() {

	const StatementHandler::ClusterRole clusterRole = 
		(StatementHandler::CROLE_MASTER | StatementHandler::CROLE_FOLLOWER);
	StatementHandler::checkExecutable(
			clusterRole, resourceSet_->getPartitionTable());
	
	checkNodeStatus();
}

void BindParam::dump(util::StackAllocator &alloc) {

	util::NormalOStringStream os;
	switch (value_.getType()) {
	case TupleList::TYPE_LONG:
		os << value_.get<int64_t>();
		break;
	case TupleList::TYPE_DOUBLE:
		os << value_.get<double>();
		break;
	case TupleList::TYPE_STRING:
		os << "'";
		os.write(
				static_cast<const char8_t*>(value_.varData()),
				static_cast<std::streamsize>(value_.varSize()));
		os << "'";
		break;
	case TupleList::TYPE_INTEGER:
		{
			int32_t i;
			memcpy(&i, value_.fixedData(), sizeof(int32_t));
			os << i;
			break;
		}
	case TupleList::TYPE_SHORT:
		{
			int16_t i;
			memcpy(&i, value_.fixedData(), sizeof(int16_t));
			os << i;
			break;
		}
	case TupleList::TYPE_BYTE:
		{
			int8_t i;
			memcpy(&i, value_.fixedData(), sizeof(int8_t));
			os << static_cast<int32_t>(i);
			break;
		}
	case TupleList::TYPE_BOOL:
		{
			int8_t i;
			memcpy(&i, value_.fixedData(), sizeof(int8_t));
			os << "(" << static_cast<int32_t>(i) << ")";
			break;
		}
	case TupleList::TYPE_FLOAT:
		{
			float f;
			memcpy(&f, value_.fixedData(), sizeof(float));
			os << f;
			break;
		}
	case TupleList::TYPE_TIMESTAMP:
		{
			os << value_.get<int64_t>();
			break;
		}
	case TupleList::TYPE_BLOB:
		{
			const char temp = 0;
			const size_t MAX_DUMP_BLOB_SIZE = 10;
			util::NormalIStringStream iss;
			TupleValue::LobReader reader(value_);
			const uint64_t count = reader.getPartCount();
			os << "[" << count << "]0x";
			const void *data;
			size_t size;

			if (reader.next(data, size)) {
				const size_t dumpSize = std::min(size, MAX_DUMP_BLOB_SIZE);
				util::XArray<char> buffer(alloc);
				buffer.assign(dumpSize * 2 + 1, temp);
				const char* in = static_cast<const char*>(data);
				size_t strSize = util::HexConverter::encode(
					buffer.data(), in, dumpSize, false);
				util::String hexStr(buffer.data(), strSize, alloc);
				os << hexStr.c_str();
				if (dumpSize < size || count > 1) {
					os << " ...";
				}
			}
			break;
		}
	case TupleList::TYPE_NULL:
		os << "NULL";
		break;
	default:
		os << "(other type)";
		break;
	}
	std::cout << os.str().c_str() << std::endl;
}

SQLAllocatorManager *SQLService::getAllocatorManager() {
	return allocatorManager_;
}

void SQLService::setupEE(
		const ConfigTable &config,
		EventEngine::Source &eeSource,
		const char *name) {

	EventEngine::Config eeConfig;

	eeConfig.setServerNDAutoNumbering(false);
	
	eeConfig.setClientNDEnabled(true);
	
	eeConfig.setConcurrency(
			config.get<int32_t>(CONFIG_TABLE_SQL_CONCURRENCY));
	
	eeConfig.setPartitionCount(DataStore::MAX_PARTITION_NUM);

	ClusterAdditionalServiceConfig addConfig(config);
	const char *targetAddress
			= addConfig.getServiceAddress(SQL_SERVICE);
	eeConfig.setServerAddress(targetAddress,
			config.getUInt16(CONFIG_TABLE_SQL_SERVICE_PORT));

	if (ClusterService::isMulticastMode(config)) {

		eeConfig.setMulticastAddress(
				config.get<const char8_t*>(
						CONFIG_TABLE_SQL_NOTIFICATION_ADDRESS),
				config.getUInt16(CONFIG_TABLE_SQL_NOTIFICATION_PORT));

		if (strlen(config.get<const char8_t *>(
				CONFIG_TABLE_SQL_NOTIFICATION_INTERFACE_ADDRESS)) != 0) {

			eeConfig.setMulticastIntefaceAddress(
					config.get<const char8_t *>(
							CONFIG_TABLE_SQL_NOTIFICATION_INTERFACE_ADDRESS),
					config.getUInt16(CONFIG_TABLE_SQL_SERVICE_PORT));
		}
	}

	eeConfig.connectionCountLimit_ =
			config.get<int32_t>(CONFIG_TABLE_SQL_CONNECTION_LIMIT);

	eeConfig.keepaliveEnabled_ =
			config.get<bool>(CONFIG_TABLE_SQL_USE_KEEPALIVE);
	
	eeConfig.keepaliveIdle_ =
			config.get<int32_t>(CONFIG_TABLE_SQL_KEEPALIVE_IDLE);
	
	eeConfig.keepaliveCount_ =
			config.get<int32_t>(CONFIG_TABLE_SQL_KEEPALIVE_COUNT);
	
	eeConfig.keepaliveInterval_ =
			config.get<int32_t>(CONFIG_TABLE_SQL_KEEPALIVE_INTERVAL);
	
	eeConfig.eventBufferSizeLimit_
			= ConfigTable::megaBytesToBytes(
			static_cast<size_t>(
				config.getUInt32(CONFIG_TABLE_SQL_TOTAL_MEMORY_LIMIT)));
	
	eeConfig.setAllAllocatorGroup(ALLOCATOR_GROUP_SQL_MESSAGE);
	
	eeConfig.workAllocatorGroupId_ = ALLOCATOR_GROUP_SQL_WORK;

	ee_ = UTIL_NEW EventEngine(eeConfig, eeSource, name);
}

uint32_t SQLService::getTargetPosition(
		Event &ev, util::XArray<NodeId> &liveNodeList) {

	StatementHandler::ConnectionOption &connOption =
			ev.getSenderND().getUserData<StatementHandler::ConnectionOption>();

	uint32_t targetNodePos = 0;
	if (!connOption.connected_) {
		targetNodePos = static_cast<uint32_t>(
				cxt_.getClientSequenceNo()
						% static_cast<int32_t>(liveNodeList.size()));
		connOption.connected_ = true;
	}
	else {
		targetNodePos = static_cast<uint32_t>(
			cxt_.incClientSequenceNo()
					% static_cast<int32_t>(liveNodeList.size()));
	}

	return targetNodePos;
}


void SQLService::checkNodeStatus() {

	try {
		resourceSet_->getClusterManager()->checkNodeStatus();
	}
	catch (std::exception &e) {
		GS_THROW_USER_ERROR(GS_ERROR_TXN_CLUSTER_ROLE_UNMATCH, 
				GS_EXCEPTION_MERGE_MESSAGE(e, ""));
	}
}