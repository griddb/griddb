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
	@brief Implementation of TriggerService
*/
#include "trigger_service.h"

#include "base_container.h"
#include "schema.h"
#include "transaction_service.h"

#include "util/type.h"

#define TXN_RETHROW_DECODE_ERROR(cause, message) \
	GS_RETHROW_CUSTOM_ERROR(                     \
		EncodeDecodeException, GS_ERROR_DEFAULT, cause, message)

#define TXN_RETHROW_ENCODE_ERROR(cause, message) \
	GS_RETHROW_CUSTOM_ERROR(                     \
		EncodeDecodeException, GS_ERROR_DEFAULT, cause, message)

#ifdef TRIGGER_CONNECT_ACTIVEMQ
#undef ostringstream ostringstream_is_disabled_because_of_locale_dependency
#undef istringstream istringstream_is_disabled_because_of_locale_dependency
#include "activemq/core/ActiveMQConnectionFactory.h"
#include "activemq/library/ActiveMQCPP.h"
#include "cms/CMSException.h"
#include "cms/Connection.h"
#include "cms/ConnectionFactory.h"
#include "cms/DeliveryMode.h"
#include "cms/Destination.h"
#include "cms/MessageProducer.h"
#include "cms/Session.h"
#include "cms/TextMessage.h"
#define ostringstream ostringstream_is_disabled_because_of_locale_dependency
#define istringstream istringstream_is_disabled_because_of_locale_dependency
#undef min
#undef max
#include <memory>
#include <sstream>
#else
namespace cms {
class TextMessage {
public:
	void setStringProperty(const std::string &, const std::string &) {}
	void setBooleanProperty(const std::string &, bool) {}
	void setByteProperty(const std::string &, int8_t) {}
	void setShortProperty(const std::string &, int16_t) {}
	void setIntProperty(const std::string &, int32_t) {}
	void setLongProperty(const std::string &, int64_t) {}
	void setFloatProperty(const std::string &, float) {}
	void setDoubleProperty(const std::string &, double) {}
};

class DeliveryMode {
public:
	static const int32_t PERSISTENT = 0;
};

class Destination {
public:
	Destination(const std::string &type, const std::string &name)
		: type_(type), name_(name) {}
	~Destination() {}

	const std::string &getType() const {
		return type_;
	}
	const std::string &getName() const {
		return name_;
	}

private:
	const std::string type_;
	const std::string name_;
};

class MessageProducer {
public:
	void setDeliveryMode(int32_t i) {}
	void send(TextMessage *m) {}
};

class Session {
public:
	static const int32_t AUTO_ACKNOWLEDGE = 0;

	Session(int32_t acknowledgeMode) : acknowledgeMode_(acknowledgeMode) {}
	~Session() {}

	Destination *createQueue(const std::string &destinationName) {
		return UTIL_NEW Destination("queue", destinationName);
	}
	Destination *createTopic(const std::string &destinationName) {
		return UTIL_NEW Destination("topic", destinationName);
	}
	MessageProducer *createProducer(Destination *) {
		return UTIL_NEW MessageProducer();
	}
	TextMessage *createTextMessage() {
		return UTIL_NEW TextMessage();
	}
	void close() {}

private:
	const int32_t acknowledgeMode_;
};

class ConnectionFactory;

class Connection {
	friend class ConnectionFactory;

public:
	~Connection(){};

	void start() {}
	void close() {}
	Session *createSession(int32_t i) {
		return UTIL_NEW Session(i);
	}

private:
	ConnectionFactory *factory_;
	std::string user_;
	std::string password_;

	Connection() : factory_(NULL) {}
};

class ConnectionFactory {
public:
	static cms::ConnectionFactory *createCMSConnectionFactory(
		const std::string &brokerURI) {
		ConnectionFactory *factory = UTIL_NEW ConnectionFactory();
		factory->brokerURI_ = brokerURI;
		return factory;
	}
	Connection *createConnection() {
		Connection *conn = UTIL_NEW Connection();
		conn->factory_ = this;
		return conn;
	}
	Connection *createConnection(
		const std::string &user, const std::string &passwd) {
		Connection *conn = createConnection();
		conn->user_ = user;
		conn->password_ = passwd;
		return conn;
	}

private:
	std::string brokerURI_;
};

class CMSException : public std::exception {};
}

namespace activemq {
namespace library {
namespace ActiveMQCPP {
void initializeLibrary() {}
void shutdownLibrary() {}
}
}
}
#endif

const TriggerService::OperationType TriggerService::NO_OPERATION = 0;
const TriggerService::OperationType TriggerService::PUT_ROW = 1 << 0;
const TriggerService::OperationType TriggerService::DELETE_ROW = 1 << 1;

TriggerService::TriggerService(const ConfigTable &param)
	: timeoutInterval_(param.get<int32_t>(CONFIG_TABLE_TRIG_TIMEOUT_INTERVAL)),
	  restTriggerHandler_(NULL),
	  jmsTriggerHandler_(NULL) {
	try {
		restTriggerHandler_ = UTIL_NEW RestTriggerHandler(*this);
		jmsTriggerHandler_ = UTIL_NEW JmsTriggerHandler(*this);

		connectionPool_.push_back(
			UTIL_NEW ConnectionPool(DEFAULT_CONNECTION_POOL_SIZE));

		activemq::library::ActiveMQCPP::initializeLibrary();
	}
	catch (std::exception &e) {
		delete restTriggerHandler_;
		delete jmsTriggerHandler_;

		for (size_t i = 0; i < connectionPool_.size(); i++) {
			delete connectionPool_[i];
		}

		UTIL_RETHROW(GS_ERROR_TRIG_SERVICE_START_FAILED, e, "");
	}
}

TriggerService::~TriggerService() {
	try {
		delete restTriggerHandler_;
		restTriggerHandler_ = NULL;
		delete jmsTriggerHandler_;
		jmsTriggerHandler_ = NULL;

		for (size_t i = 0; i < connectionPool_.size(); i++) {
			delete connectionPool_[i];
		}

		activemq::library::ActiveMQCPP::shutdownLibrary();
	}
	catch (std::exception &e) {
		delete restTriggerHandler_;
		delete jmsTriggerHandler_;

		UTIL_TRACE_EXCEPTION_WARNING(TRIGGER_SERVICE, e, "");
	}
}

/*!
	@brief Get connection
*/
TriggerService::PooledConnection *TriggerService::getConnection(
	const std::string &brokerURI, const std::string &user,
	const std::string &password) {
	const uint32_t workerId = 0;
	return connectionPool_[workerId]->get(brokerURI, user, password);
}

/*!
	@brief Release connection
*/
void TriggerService::releaseConnection(const std::string &brokerURI,
	const std::string &user, const std::string &password,
	PooledConnection *&connection) {
	const uint32_t workerId = 0;
	connectionPool_[workerId]->release(brokerURI, user, password, connection);
}

/*!
	@brief Invalidate connection
*/
void TriggerService::invalidateConnection(const std::string &brokerURI,
	const std::string &user, const std::string &password,
	PooledConnection *&connection) {
	const uint32_t workerId = 0;
	connectionPool_[workerId]->invalidate(
		brokerURI, user, password, connection);
}

/*!
	@brief Gets trigger notification timeout interval
*/
int32_t TriggerService::getTimeoutInterval() const {
	return timeoutInterval_;
}

/*!
	@brief Translate JmsProviderType into string
*/
const char *TriggerService::jmsProviderTypeToStr(JmsProviderType type) {
	switch (type) {
	case JMS_PROVIDER_ACTIVEMQ:
		return "activemq";
	default:
		return "";
	}
}

/*!
	@brief Translate JmsDestinationType into string
*/
const char *TriggerService::jmsDestinationTypeToStr(JmsDestinationType type) {
	switch (type) {
	case JMS_DESTINATON_QUEUE:
		return "queue";
	case JMS_DESTINATON_TOPIC:
		return "topic";
	default:
		return "";
	}
}

TriggerService::ConnectionPool::ConnectionPool(uint32_t maxPoolSize)
	: maxPoolSize_(maxPoolSize), numInstance_(0) {}

TriggerService::ConnectionPool::~ConnectionPool() {
	try {
		close();
	}
	catch (...) {
	}
}

/*!
	@brief Get connection by pool
*/
TriggerService::PooledConnection *TriggerService::ConnectionPool::get(
	const std::string &brokerURI, const std::string &user,
	const std::string &password) {
	try {
		UTIL_UNIQUE_PTR<PooledConnection> pConnection;

		const std::string key = brokerURI + user + password;

		std::map<std::string, PooledConnection *>::iterator it;
		it = pooledConnectionMap_.find(key);
		if (it != pooledConnectionMap_.end()) {
			pConnection.reset(it->second);

			GS_TRACE_DEBUG(TRIGGER_SERVICE, GS_ERROR_TRIG_CONNECTION_POOL,
				"HIT pooled connecction (key=" << key << ")");
		}
		else {
			UTIL_UNIQUE_PTR<cms::ConnectionFactory> factory(
				cms::ConnectionFactory::createCMSConnectionFactory(brokerURI));
			UTIL_UNIQUE_PTR<cms::Connection> connection;

			if (user.size() == 0 && password.size() == 0) {
				connection.reset(factory->createConnection());
			}
			else {
				connection.reset(factory->createConnection(user, password));
			}

			pConnection.reset(
				UTIL_NEW PooledConnection(factory.get(), connection.get()));
			numInstance_++;
			factory.release();
			connection.release();

			pConnection.get()->connection_->start();

			if (pooledConnectionMap_.size() < maxPoolSize_) {
				pooledConnectionMap_.insert(
					std::make_pair(key, pConnection.get()));
				GS_TRACE_DEBUG(TRIGGER_SERVICE, GS_ERROR_TRIG_CONNECTION_POOL,
					"CREATE and POOL connecction (key=" << key << ")");
			}
			else {
				GS_TRACE_DEBUG(TRIGGER_SERVICE, GS_ERROR_TRIG_CONNECTION_POOL,
					"CREATE connecction (key=" << key << ")");
			}
		}

		GS_TRACE_DEBUG(TRIGGER_SERVICE, GS_ERROR_TRIG_CONNECTION_POOL,
			"numPooledConnection = " << pooledConnectionMap_.size()
									 << ", max = " << maxPoolSize_
									 << ", numInstance=" << numInstance_);

		return pConnection.release();
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e,
			"JMS connection error "
			"(brokerURI="
				<< brokerURI << ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Returns connection to pool
*/
void TriggerService::ConnectionPool::release(const std::string &brokerURI,
	const std::string &user, const std::string &password,
	PooledConnection *&connection) {
	if (connection == NULL) {
		return;
	}

	try {
		const std::string key = brokerURI + user + password;
		std::map<std::string, PooledConnection *>::iterator it;
		it = pooledConnectionMap_.find(key);
		if (it != pooledConnectionMap_.end()) {

			if (connection == it->second) {

				if (pooledConnectionMap_.size() >= maxPoolSize_) {
					GS_TRACE_DEBUG(TRIGGER_SERVICE,
						GS_ERROR_TRIG_CONNECTION_POOL,
						"DELETE connecction 1 (key=" << key << ")");
					delete it->second;
					numInstance_--;
					pooledConnectionMap_.erase(it);
				}
				else {
					GS_TRACE_DEBUG(TRIGGER_SERVICE,
						GS_ERROR_TRIG_CONNECTION_POOL,
						"RELEASE connecction (key=" << key << ")");
				}
			}
			else {
				GS_TRACE_DEBUG(TRIGGER_SERVICE, GS_ERROR_TRIG_CONNECTION_POOL,
					"DELETE connecction 2 (key=" << key << ")");
				delete connection;
				numInstance_--;
			}
		}
		else if (pooledConnectionMap_.size() < maxPoolSize_) {
			GS_TRACE_DEBUG(TRIGGER_SERVICE, GS_ERROR_TRIG_CONNECTION_POOL,
				"POOL connection (key=" << key << ")");
			pooledConnectionMap_.insert(std::make_pair(key, connection));
		}
		else {
			GS_TRACE_DEBUG(TRIGGER_SERVICE, GS_ERROR_TRIG_CONNECTION_POOL,
				"DELETE connecction 3 (key=" << key << ")");
			delete connection;
			numInstance_--;
		}

		connection = NULL;

		GS_TRACE_DEBUG(TRIGGER_SERVICE, GS_ERROR_TRIG_CONNECTION_POOL,
			"numPooledConnection = " << pooledConnectionMap_.size()
									 << ", max = " << maxPoolSize_
									 << ", numInstance=" << numInstance_);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e,
			"JMS connection error "
			"(brokerURI="
				<< brokerURI << ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Invalidate connection
*/
void TriggerService::ConnectionPool::invalidate(const std::string &brokerURI,
	const std::string &user, const std::string &password,
	PooledConnection *&connection) {
	if (connection == NULL) {
		return;
	}

	try {
		const std::string key = brokerURI + user + password;
		std::map<std::string, PooledConnection *>::iterator it;
		it = pooledConnectionMap_.find(key);
		if (it != pooledConnectionMap_.end()) {

			if (connection == it->second) {
				GS_TRACE_DEBUG(TRIGGER_SERVICE, GS_ERROR_TRIG_CONNECTION_POOL,
					"DELETE connecction 4 (key=" << key << ")");
				delete it->second;
				numInstance_--;
				pooledConnectionMap_.erase(it);
			}
			else {
				GS_TRACE_DEBUG(TRIGGER_SERVICE, GS_ERROR_TRIG_CONNECTION_POOL,
					"DELETE connecction 5 (key=" << key << ")");
				delete connection;
				numInstance_--;
			}
		}
		else {
			GS_TRACE_DEBUG(TRIGGER_SERVICE, GS_ERROR_TRIG_CONNECTION_POOL,
				"DELETE connecction 6 (key=" << key << ")");
			delete connection;
			numInstance_--;
		}

		connection = NULL;

		GS_TRACE_DEBUG(TRIGGER_SERVICE, GS_ERROR_TRIG_CONNECTION_POOL,
			"numPooledConnection = " << pooledConnectionMap_.size()
									 << ", max = " << maxPoolSize_
									 << ", numInstance=" << numInstance_);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e,
			"JMS connection error "
			"(brokerURI="
				<< brokerURI << ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Close connection pool
*/
void TriggerService::ConnectionPool::close() {
	try {
		std::map<std::string, PooledConnection *>::iterator it;
		for (it = pooledConnectionMap_.begin();
			 it != pooledConnectionMap_.end(); it++) {
			delete it->second;
			numInstance_--;
		}

		pooledConnectionMap_.clear();

		GS_TRACE_DEBUG(TRIGGER_SERVICE, GS_ERROR_TRIG_CONNECTION_POOL,
			"numPooledConnection = " << pooledConnectionMap_.size()
									 << ", max = " << maxPoolSize_
									 << ", numInstance=" << numInstance_);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "JMS connection error"));
	}
}

TriggerService::PooledConnection::PooledConnection(
	cms::ConnectionFactory *factory, cms::Connection *connection)
	: factory_(factory), connection_(connection) {}

TriggerService::PooledConnection::~PooledConnection() {
	if (connection_ != NULL) {
		try {
			connection_->close();
		}
		catch (...) {
		}
		delete connection_;
	}
	if (factory_ != NULL) {
		delete factory_;
	}
}

cms::Connection *TriggerService::PooledConnection::getConnection() {
	return connection_;
}

TriggerService::ConfigSetUpHandler TriggerService::configSetUpHandler_;

void TriggerService::ConfigSetUpHandler::operator()(ConfigTable &config) {
	CONFIG_TABLE_RESOLVE_GROUP(config, CONFIG_TABLE_TRIG, "trigger");

	CONFIG_TABLE_ADD_PARAM(config, CONFIG_TABLE_TRIG_TIMEOUT_INTERVAL, INT32)
		.setMin(1)
		.setMax(60)
		.setDefault(5);
}

TriggerHandler::TriggerHandler(TriggerService &triggerService)
	: triggerService_(triggerService) {}

TriggerHandler::~TriggerHandler() {}

/*!
	@brief Verify notification target URL
*/
void TriggerHandler::checkEventNotificationUrl(
	util::StackAllocator &alloc, const util::String &url) {
	util::String host(alloc);
	util::String port(alloc);
	util::String path(alloc);
	parseUrl(url, host, port, path);
}

/*!
	@brief Verify trigger registration
*/
void TriggerHandler::checkTriggerRegistration(
	util::StackAllocator &alloc, TriggerInfo &info) {
	NoEmptyKey::validate(
		KeyConstraint::getUserKeyConstraint(LIMIT_COLUMN_NAME_SIZE),
		info.name_.c_str(), static_cast<uint32_t>(info.name_.size()),
		"triggerName");

	if (info.operation_ == TriggerService::NO_OPERATION) {
		GS_THROW_USER_ERROR(GS_ERROR_TRIG_TARGET_OPERATION_INVALID,
			"(targetOperation=" << info.operation_ << ")");
	}

	if (info.uri_.size() > TriggerService::LIMIT_TRIGGER_URI_SIZE) {
		GS_THROW_USER_ERROR(
			GS_ERROR_CM_LIMITS_EXCEEDED, "(uri=" << info.uri_ << ")");
	}

	switch (info.type_) {
	case TriggerService::TRIGGER_REST:
		checkEventNotificationUrl(alloc, info.uri_);
		break;

	case TriggerService::TRIGGER_JMS:
		if (info.jmsProviderTypeName_.compare("activemq") == 0) {
			info.jmsProviderType_ = TriggerService::JMS_PROVIDER_ACTIVEMQ;
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_TRIG_JMS_PROVIDER_INVALID,
				"(providerName=" << info.jmsProviderTypeName_ << ")");
		}
		if (info.jmsDestinationTypeName_.compare("queue") == 0) {
			info.jmsDestinationType_ = TriggerService::JMS_DESTINATON_QUEUE;
		}
		else if (info.jmsDestinationTypeName_.compare("topic") == 0) {
			info.jmsDestinationType_ = TriggerService::JMS_DESTINATON_TOPIC;
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_TRIG_JMS_DESTINATION_INVALID,
				"(destinationType=" << info.jmsDestinationTypeName_ << ")");
		}
		if (info.jmsDestinationName_.size() == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_TRIG_JMS_DESTINATION_NAME_INVALID,
				"(destinationName=" << info.jmsDestinationName_ << ")");
		}
		break;

	default:
		GS_THROW_USER_ERROR(
			GS_ERROR_TRIG_TYPE_INVALID, "(triggerType=" << info.type_ << ")");
	}
}

/*!
	@brief Checks which trigger fires
*/
void TriggerHandler::checkTrigger(TriggerService &triggerService,
	TransactionContext &txn, BaseContainer &container, EventContext &ec,
	const util::XArray<TriggerService::OperationType> &operationTypeList,
	uint64_t numRow, const void *putRowData, size_t putRowDataSize) {
	assert(numRow == operationTypeList.size());

	util::StackAllocator &alloc = txn.getDefaultAllocator();
	util::StackAllocator::Scope scope(alloc);

	const PartitionId pId = txn.getPartitionId();
	const ContainerId containerId = container.getContainerId();

	try {
		util::XArray<const uint8_t *> triggerList(alloc);
		container.getTriggerList(txn, triggerList);

		if (triggerList.size() == 0) {
			return;
		}

		const FullContainerKey containerKey = container.getContainerKey(txn);
		util::String containerName(alloc);
		containerKey.toString(alloc, containerName);

		ColumnInfo *schema = container.getColumnInfoList();
		const uint32_t numColumn = container.getColumnNum();

		UTIL_UNIQUE_PTR<InputMessageRowStore> rowStore;
		if (putRowData != NULL && putRowDataSize > 0) {
			rowStore.reset(UTIL_NEW InputMessageRowStore(
				container.getDataStore()->getValueLimitConfig(), schema,
				numColumn, const_cast<void *>(putRowData),
				static_cast<uint32_t>(putRowDataSize), numRow,
				container.getRowFixedSize()));
		}
		else {
			rowStore.reset(UTIL_NEW InputMessageRowStore(
				container.getDataStore()->getValueLimitConfig(), schema,
				numColumn, NULL, 0, 0, container.getRowFixedSize()));
		}

		for (size_t i = 0; i < triggerList.size(); i++) {
			TriggerInfo trigger(alloc);
			TriggerInfo::decode(triggerList[i], trigger);

			rowStore->reset();

			for (size_t j = 0; j < operationTypeList.size(); j++) {
				const TriggerService::OperationType operation =
					operationTypeList[j];
				rowStore->next();

				if (trigger.operation_ & operation) {
					util::StackAllocator::Scope scope(alloc);



					util::String operationName(alloc);
					convertOperationTypeToString(operation, operationName);
					util::XArray<char *> columnNameList(alloc);
					util::XArray<uint32_t> columnNameLengthList(alloc);
					util::XArray<ColumnType> columnTypeList(alloc);
					util::XArray<const uint8_t *> columnDataList(alloc);
					util::XArray<uint32_t> columnDataLengthList(alloc);

					const uint32_t columnIdCount =
						(operation & TriggerService::PUT_ROW)
							? static_cast<uint32_t>(trigger.columnIds_.size())
							: 0;

					for (uint32_t k = 0; k < columnIdCount; k++) {
						assert(rowStore.get() != NULL);

						const ColumnId columnId = trigger.columnIds_[k];

						const ColumnType columnType =
							schema[columnId].getColumnType();
						columnTypeList.push_back(columnType);

						char *s =
							const_cast<char *>(schema[columnId].getColumnName(
								txn, *(container.getObjectManager())));
						uint32_t l = static_cast<uint32_t>(strlen(s));
						columnNameList.push_back(s);
						columnNameLengthList.push_back(l);

						const uint8_t *data;
						uint32_t size;

						if (isSupportedColumnType(columnType)) {
							rowStore->getField(columnId, data, size);
						}
						else {
							data = NULL;
							size = 0;
						}

						columnDataList.push_back(data);
						columnDataLengthList.push_back(size);
					}

					Event event(ec, static_cast<EventType>(trigger.type_), pId);

					switch (trigger.type_) {
					case TriggerService::TRIGGER_REST:
						RestTriggerHandler::encode(event, trigger,
							containerName, operationName, columnIdCount,
							columnTypeList, columnNameList,
							columnNameLengthList, columnDataList,
							columnDataLengthList);
						(*triggerService.restTriggerHandler_)(ec, event);
						break;

					case TriggerService::TRIGGER_JMS:
						switch (trigger.jmsProviderType_) {
						case TriggerService::JMS_PROVIDER_ACTIVEMQ:
							JmsTriggerHandler::encode(event, trigger,
								containerName, operationName, columnIdCount,
								columnTypeList, columnNameList,
								columnNameLengthList, columnDataList,
								columnDataLengthList);
							(*triggerService.jmsTriggerHandler_)(ec, event);
							break;

						default:
							GS_THROW_USER_ERROR(
								GS_ERROR_TRIG_JMS_PROVIDER_INVALID,
								"(providerType=" << trigger.jmsProviderType_
												 << ")");
						}
						break;

					default:
						GS_THROW_USER_ERROR(GS_ERROR_TRIG_TYPE_INVALID,
							"(triggerType=" << trigger.type_ << ")");
					}
				}

			}  
		}	  
	}
	catch (...) {
		try {
			std::exception e;
			GS_RETHROW_USER_OR_SYSTEM(
				e, "Trigger check failed (pId="
					   << pId << ", containerId=" << containerId
					   << ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
		}
		catch (UserException &e) {
			UTIL_TRACE_EXCEPTION(TRIGGER_SERVICE, e, "");
		}
		catch (...) {
			throw;
		}
	}
}

/*!
	@brief Parse notification target URL
*/
void TriggerHandler::parseUrl(const util::String &url, util::String &host,
	util::String &port, util::String &path) {
	try {
		size_t index = 0;
		index = url.find("http://", index);
		if (index == std::string::npos || index != 0) {
			GS_THROW_USER_ERROR(
				GS_ERROR_TXN_EVENT_NOTIFY_URL_METHOD_INVALID, "");
		}
		size_t hostStart = index + strlen("http://");
		index = hostStart;

		size_t hostEnd = 0;
		size_t portStart = 0;
		size_t portEnd = 0;
		size_t pathStart = 0;
		{
			size_t pos = url.find(":", index);
			if (pos != std::string::npos) {
				hostEnd = pos;
				portStart = pos + 1;
				index = portStart;

				pos = url.find("/", index);
				if (pos == std::string::npos) {
					pos = url.size();
				}
				portEnd = pos;
				pathStart = pos;
				index = pathStart;
			}
			else {
				pos = url.find("/", index);
				if (pos == std::string::npos) {
					pos = url.size();
				}
				hostEnd = pos;
				pathStart = pos;
				index = pathStart;
			}
		}
		size_t pathEnd = url.size();

		host.append(url, hostStart, hostEnd - hostStart);
		port.append(url, portStart, portEnd - portStart);
		path.append(url, pathStart, pathEnd - pathStart);

		if (host.length() == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_TXN_EVENT_NOTIFY_URL_HOST_INVALID, "");
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(
			e, "Failed to parse trigger url (url="
				   << url << ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Checks whether data type is includable in notification message
*/
bool TriggerHandler::isSupportedColumnType(ColumnType type) {
	switch (type) {
	case COLUMN_TYPE_STRING:
	case COLUMN_TYPE_BOOL:
	case COLUMN_TYPE_BYTE:
	case COLUMN_TYPE_SHORT:
	case COLUMN_TYPE_INT:
	case COLUMN_TYPE_LONG:
	case COLUMN_TYPE_FLOAT:
	case COLUMN_TYPE_DOUBLE:
	case COLUMN_TYPE_TIMESTAMP:
		return true;
		break;

	case COLUMN_TYPE_BLOB:
	case COLUMN_TYPE_OID:
	case COLUMN_TYPE_STRING_ARRAY:
	case COLUMN_TYPE_BOOL_ARRAY:
	case COLUMN_TYPE_BYTE_ARRAY:
	case COLUMN_TYPE_SHORT_ARRAY:
	case COLUMN_TYPE_INT_ARRAY:
	case COLUMN_TYPE_LONG_ARRAY:
	case COLUMN_TYPE_FLOAT_ARRAY:
	case COLUMN_TYPE_DOUBLE_ARRAY:
	case COLUMN_TYPE_TIMESTAMP_ARRAY:
	case COLUMN_TYPE_WITH_BEGIN:
	default:
		return false;
		break;
	}
}

/*!
	@brief Checks whether data type requires quotation in notification message
*/
bool TriggerHandler::isQuoteColumnType(ColumnType type) {
	switch (type) {
	case COLUMN_TYPE_STRING:
	case COLUMN_TYPE_BOOL:
		return true;
		break;
	default:
		return !isSupportedColumnType(type);
		break;
	}
}

/*!
	@brief Converts row operation type into string
*/
void TriggerHandler::convertOperationTypeToString(
	TriggerService::OperationType type, util::String &str) {
	str.clear();

	switch (type) {
	case TriggerService::PUT_ROW:
		str.append("put");
		break;
	case TriggerService::DELETE_ROW:
		str.append("delete");
		break;
	default:
		str.append("unknown");
		break;
	}
}

/*!
	@brief Converts column data into string
*/
void TriggerHandler::convertColumnDataToString(
	ColumnType type, const uint8_t *data, size_t size, util::String &str) {
	str.clear();

	if (data == NULL) {
		const char *nullStr = "null";
		str.append(nullStr, strlen(nullStr));
	} else {

	switch (type) {
	case COLUMN_TYPE_STRING:
		str.append(reinterpret_cast<const char *>(data), size);
		break;
	case COLUMN_TYPE_BOOL: {
		const bool tmp = *reinterpret_cast<const bool *>(data);
		if (tmp) {
			str.append("true");
		}
		else {
			str.append("false");
		}
	} break;
	case COLUMN_TYPE_BYTE: {
		const int8_t tmp = *reinterpret_cast<const int8_t *>(data);
		util::NormalOStringStream ss;
		ss << static_cast<int32_t>(tmp);
		str.append(ss.str().c_str());
	} break;
	case COLUMN_TYPE_SHORT: {
		const int16_t tmp = *reinterpret_cast<const int16_t *>(data);
		util::NormalOStringStream ss;
		ss << tmp;
		str.append(ss.str().c_str());
	} break;
	case COLUMN_TYPE_INT: {
		const int32_t tmp = *reinterpret_cast<const int32_t *>(data);
		util::NormalOStringStream ss;
		ss << tmp;
		str.append(ss.str().c_str());
	} break;
	case COLUMN_TYPE_LONG: {
		const int64_t tmp = *reinterpret_cast<const int64_t *>(data);
		util::NormalOStringStream ss;
		ss << tmp;
		str.append(ss.str().c_str());
	} break;
	case COLUMN_TYPE_FLOAT: {
		const float tmp = *reinterpret_cast<const float *>(data);
		util::NormalOStringStream ss;
		ss << tmp;
		str.append(ss.str().c_str());
	} break;
	case COLUMN_TYPE_DOUBLE: {
		const double tmp = *reinterpret_cast<const double *>(data);
		util::NormalOStringStream ss;
		ss << tmp;
		str.append(ss.str().c_str());
	} break;
	case COLUMN_TYPE_TIMESTAMP: {
		const int64_t tmp = *reinterpret_cast<const int64_t *>(data);
		util::NormalOStringStream ss;
		ss << tmp;
		str.append(ss.str().c_str());
	} break;
	case COLUMN_TYPE_BLOB:
	case COLUMN_TYPE_OID:
	case COLUMN_TYPE_STRING_ARRAY:
	case COLUMN_TYPE_BOOL_ARRAY:
	case COLUMN_TYPE_BYTE_ARRAY:
	case COLUMN_TYPE_SHORT_ARRAY:
	case COLUMN_TYPE_INT_ARRAY:
	case COLUMN_TYPE_LONG_ARRAY:
	case COLUMN_TYPE_FLOAT_ARRAY:
	case COLUMN_TYPE_DOUBLE_ARRAY:
	case COLUMN_TYPE_TIMESTAMP_ARRAY:
	case COLUMN_TYPE_WITH_BEGIN:
	default:
		break;
	}
	}
}

void TriggerHandler::encodeColumnData(EventByteOutStream &out,
	uint32_t numColumn, const util::XArray<ColumnType> &columnTypeList,
	const util::XArray<char *> &columnNameList,
	const util::XArray<uint32_t> &columnNameLengthList,
	const util::XArray<const uint8_t *> &columnDataList,
	const util::XArray<uint32_t> &columnDataSizeList) {
	out << numColumn;

	for (uint32_t i = 0; i < numColumn; i++) {
		{
			const uint8_t tmp = static_cast<uint8_t>(columnTypeList[i]);
			out << tmp;
		}

		const std::string columnName(
			columnNameList[i], columnNameLengthList[i]);
		out << columnName;

		out << columnDataSizeList[i];
		out << std::pair<const uint8_t *, size_t>(
			columnDataList[i], columnDataSizeList[i]);
	}
}

void TriggerHandler::decodeColumnData(util::StackAllocator &alloc,
	util::ByteStream<util::ArrayInStream> &in, uint32_t &numColumn,
	util::XArray<ColumnType> &columnTypeList,
	util::XArray<util::String *> &columnNameList,
	util::XArray<util::XArray<uint8_t> *> &columnDataList) {
	in >> numColumn;

	for (uint32_t i = 0; i < numColumn; i++) {
		ColumnType columnType;
		{
			uint8_t tmp;
			in >> tmp;
			columnType = static_cast<ColumnType>(tmp);
		}
		columnTypeList.push_back(columnType);

		util::String *columnName = ALLOC_NEW(alloc) util::String(alloc);
		in >> (*columnName);
		columnNameList.push_back(columnName);

		uint32_t columnDataSize;
		in >> columnDataSize;
		util::XArray<uint8_t> *columnData =
			ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
		columnData->resize(columnDataSize);
		in >> std::pair<uint8_t *, size_t>(columnData->data(), columnDataSize);
		columnDataList.push_back(columnData);
	}
}

RestTriggerHandler::RestTriggerHandler(TriggerService &triggerService)
	: TriggerHandler(triggerService) {}

RestTriggerHandler::~RestTriggerHandler() {}

/*!
	@brief Handles rest trigger notification event
*/
void RestTriggerHandler::operator()(EventContext &ec, Event &ev) {
	util::StackAllocator &alloc = ec.getAllocator();
	util::StackAllocator::Scope scope(alloc);

	const PartitionId pId = ev.getPartitionId();

	util::String triggerName(alloc), url(alloc), containerName(alloc),
		operationName(alloc);

	try {
		uint32_t numColumn;
		util::XArray<ColumnType> columnTypeList(alloc);
		util::XArray<util::String *> columnNameList(alloc);
		util::XArray<util::XArray<uint8_t> *> columnDataList(alloc);

		decode(alloc, ev, triggerName, url, containerName, operationName,
			numColumn, columnTypeList, columnNameList, columnDataList);

		util::String host(alloc);
		util::String port(alloc);
		util::String path(alloc);
		parseUrl(url, host, port, path);

		if (port.size() == 0) {
			port.append("80");
		}

		util::String content(alloc);
		createRestContent(alloc, containerName, operationName, numColumn,
			columnTypeList, columnNameList, columnDataList, content);

		util::NormalOStringStream oss;
		oss << "POST"
			<< " " << path << " HTTP/1.1\r\n"
			<< "Host: " << host << ":" << port << "\r\n"
			<< "Connection: close\r\n"
			<< "Content-Length: " << content.size() << "\r\n"
			<< "Content-Type: application/json\r\n"
			<< "\r\n"
			<< content;
		const util::String request(oss.str().c_str(), alloc);

		util::SocketAddress addr(host.c_str(), port.c_str());
		util::Socket socket;

		socket.open(addr.getFamily(), util::Socket::TYPE_STREAM);
		socket.setBlockingMode(false);

		const Timestamp start =
			util::DateTime::now(TRIM_MILLISECONDS).getUnixTime();
		const Timestamp timeout =
			start +
			static_cast<Timestamp>(triggerService_.getTimeoutInterval()) * 1000;

		if (!socket.connect(addr)) {
			util::IOPoll poll;
			poll.add(&socket, util::IOPollEvent::TYPE_WRITE);
			if (!poll.dispatch(triggerService_.getTimeoutInterval() * 1000)) {
				const Timestamp now =
					util::DateTime::now(TRIM_MILLISECONDS).getUnixTime();
				GS_THROW_USER_ERROR(GS_ERROR_TRIG_TIMEOUT,
					"timeout occurred while connecting to server"
						<< " (startTime=" << start << ", now=" << now
						<< ", interval=" << triggerService_.getTimeoutInterval()
						<< ")");
			}
		}

		for (size_t send = 0; send < request.size();) {
			ssize_t len =
				socket.send(request.c_str() + send, request.size() - send);

			if (len == 0) {
				GS_THROW_USER_ERROR(GS_ERROR_TRIG_REST_SEND_FAILED,
					"disconnected while sending request to server");  
			}
			else if (len > 0) {
				send += len;
			}
		}

		GS_TRACE_DEBUG(TRIGGER_SERVICE, GS_ERROR_TRIG_SEND,
			"REST trigger send request succeeded"
				<< " (triggerName=" << triggerName
				<< ", containerName=" << containerName << ", url=" << url
				<< ", content=" << content << ")");

		socket.shutdown(false, true);

		util::XArray<uint8_t> buf(alloc);
		size_t responseBufSize = TriggerService::DEFAULT_RESPONSE_BUFFER_SIZE;
		buf.resize(responseBufSize);
		size_t recv = 0;
		while (recv < responseBufSize) {
			ssize_t len =
				socket.receive(buf.data() + recv, responseBufSize - recv);
			if (len == 0) {
				break;
			}
			else if (len > 0) {
				recv += len;
			}

			if (responseBufSize - recv == 0) {
				buf.resize(responseBufSize +
						   TriggerService::DEFAULT_RESPONSE_BUFFER_SIZE);
				responseBufSize += TriggerService::DEFAULT_RESPONSE_BUFFER_SIZE;
			}

			const Timestamp now =
				util::DateTime::now(TRIM_MILLISECONDS).getUnixTime();
			if (now > timeout) {
				GS_THROW_USER_ERROR(GS_ERROR_TRIG_TIMEOUT,
					"timeout occurred while receiving response"
						<< " (startTime=" << start << ", now=" << now
						<< ", interval=" << triggerService_.getTimeoutInterval()
						<< ")");
			}
		}

		buf[recv] = '\0';
		const util::String response(
			reinterpret_cast<const char *>(buf.data()), responseBufSize, alloc);

		GS_TRACE_DEBUG(TRIGGER_SERVICE, GS_ERROR_TRIG_CONNECTION_POOL,
			"REST trigger recv response succeeded"
				<< " (triggerName=" << triggerName
				<< ", containerName=" << containerName << ", url=" << url
				<< ", response=" << response << ")");

		if (!isRequestSucceeded(response)) {
			GS_THROW_USER_ERROR(GS_ERROR_TRIG_REST_CHECK_RESPONSE_FAILED,
				"REST trigger recv response failed");
		}

		socket.shutdown(true, true);
		socket.close();
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(TRIGGER_SERVICE, e,
			"REST trigger failed"
				<< " (pId=" << pId << ", triggerName=" << triggerName
				<< ", containerName=" << containerName << ", url=" << url
				<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

/*!
	@brief Encodes rest trigger notification event
*/
void RestTriggerHandler::encode(Event &event, const TriggerInfo &info,
	const util::String &containerName, const util::String &operationName,
	uint32_t numColumn, const util::XArray<ColumnType> &columnTypeList,
	const util::XArray<char *> &columnNameList,
	const util::XArray<uint32_t> &columnNameLengthList,
	const util::XArray<const uint8_t *> &columnDataList,
	const util::XArray<uint32_t> &columnDataSizeList) {
	try {
		EventByteOutStream out = event.getOutStream();

		out << info.name_;
		out << info.uri_;
		out << containerName;
		out << operationName;

		encodeColumnData(out, numColumn, columnTypeList, columnNameList,
			columnNameLengthList, columnDataList, columnDataSizeList);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void RestTriggerHandler::decode(util::StackAllocator &alloc, Event &event,
	util::String &triggerName, util::String &uri, util::String &containerName,
	util::String &operationName, uint32_t &numColumn,
	util::XArray<ColumnType> &columnTypeList,
	util::XArray<util::String *> &columnNameList,
	util::XArray<util::XArray<uint8_t> *> &columnDataList) {
	try {
		EventByteInStream in = event.getInStream();

		in >> triggerName;
		in >> uri;
		in >> containerName;
		in >> operationName;

		decodeColumnData(alloc, in, numColumn, columnTypeList, columnNameList,
			columnDataList);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

void RestTriggerHandler::createRestContent(util::StackAllocator &alloc,
	const util::String &containerName, const util::String &operationName,
	uint32_t numColumn, const util::XArray<ColumnType> &columnTypeList,
	const util::XArray<util::String *> &columnNameList,
	const util::XArray<util::XArray<uint8_t> *> &columnDataList,
	util::String &content) {
	content.append("{ ");
	content.append("\"container\":");
	content.append("\"");
	content.append(containerName);
	content.append("\", ");

	content.append("\"event\":");
	content.append("\"");
	content.append(operationName);
	content.append("\"");

	if (numColumn > 0) {
		content.append(", \"row\":{ ");

		for (uint32_t i = 0; i < numColumn; i++) {
			const ColumnType columnType = columnTypeList[i];
			const util::String *columnName = columnNameList[i];
			const util::XArray<uint8_t> *columnData = columnDataList[i];

			util::String columnDataStr(alloc);
			convertColumnDataToString(columnType, columnData->data(),
				columnData->size(), columnDataStr);

			content.append("\"");
			content.append(columnName->c_str(), columnName->size());
			content.append("\":");
			if (columnData != NULL && 
				isQuoteColumnType(columnType)) content.append("\"");
			content.append(columnDataStr);
			if (columnData != NULL && 
				isQuoteColumnType(columnType)) content.append("\"");
			content.append(",");
		}

		util::String::iterator end = content.end();
		end--;
		content.erase(end);

		content.append(" }");
	}

	content.append(" }");
}

bool RestTriggerHandler::isRequestSucceeded(const util::String &response) {
	int32_t statusCode = -1;

	const char *successStatusCodeStr[7] = {
		" 200 ", " 201 ", " 202 ", " 203 ", " 204 ", " 205 ", " 206 "};
	const int32_t successStatusCode[7] = {200, 201, 202, 203, 204, 205, 206};

	for (size_t i = 0; i < 7; i++) {
		if (response.find(successStatusCodeStr[i], 0) != std::string::npos) {
			statusCode = successStatusCode[i];
			break;
		}
	}

	return (statusCode >= 200 && statusCode <= 206);
}

JmsTriggerHandler::JmsTriggerHandler(TriggerService &triggerService)
	: TriggerHandler(triggerService) {}

JmsTriggerHandler::~JmsTriggerHandler() {}

/*!
	@brief Handles Java Message Service (JMS) trigger notification event
*/
void JmsTriggerHandler::operator()(EventContext &ec, Event &ev) {
	util::StackAllocator &alloc = ec.getAllocator();
	util::StackAllocator::Scope scope(alloc);

	const PartitionId pId = ev.getPartitionId();

	util::String triggerName(alloc);

	std::string uri, containerName, operationName, destinationName, user,
		password;

	TriggerService::JmsProviderType providerType = -1;
	TriggerService::JmsDestinationType destinationType = -1;

	TriggerService::PooledConnection *pConnection = NULL;

	try {
		uint32_t numColumn;
		util::XArray<ColumnType> columnTypeList(alloc);
		util::XArray<util::String *> columnNameList(alloc);
		util::XArray<util::XArray<uint8_t> *> columnDataList(alloc);

		decode(alloc, ev, triggerName, uri, providerType, destinationType,
			destinationName, user, password, containerName, operationName,
			numColumn, columnTypeList, columnNameList, columnDataList);

		pConnection = triggerService_.getConnection(uri, user, password);
		cms::Connection *connection = pConnection->getConnection();

		{
			UTIL_UNIQUE_PTR<cms::Session> session(
				connection->createSession(cms::Session::AUTO_ACKNOWLEDGE));

			{

				UTIL_UNIQUE_PTR<cms::Destination> destination;
				switch (destinationType) {
				case TriggerService::JMS_DESTINATON_QUEUE:
					destination.reset(session->createQueue(destinationName));
					break;

				case TriggerService::JMS_DESTINATON_TOPIC:
					destination.reset(session->createTopic(destinationName));
					break;

				default:
					GS_THROW_USER_ERROR(GS_ERROR_TRIG_JMS_DESTINATION_INVALID,
						"(destinationType=" << destinationType << ")");
				}

				UTIL_UNIQUE_PTR<cms::MessageProducer> producer(
					session->createProducer(destination.get()));
				producer->setDeliveryMode(cms::DeliveryMode::PERSISTENT);

				UTIL_UNIQUE_PTR<cms::TextMessage> message(
					session->createTextMessage());
				createJmsMessage(containerName, operationName, numColumn,
					columnTypeList, columnNameList, columnDataList,
					message.get());

				producer->send(message.get());

				GS_TRACE_DEBUG(TRIGGER_SERVICE, GS_ERROR_TRIG_SEND,
					"send JMS trigger. (triggerName="
						<< triggerName << ", containerName=" << containerName
						<< ")");
			}

			session->close();
		}

		triggerService_.releaseConnection(uri, user, password, pConnection);
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(TRIGGER_SERVICE, e,
			"JMS trigger failed"
				<< " (pId=" << pId << ", triggerName=" << triggerName
				<< ", containerName=" << containerName << ", uri=" << uri
				<< ", providerType=" << providerType << ", destinationType="
				<< destinationType << ", destinationName=" << destinationName
				<< ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
		triggerService_.invalidateConnection(uri, user, password, pConnection);
	}
}

/*!
	@brief Encodes Java Message Service (JMS) trigger notification event
*/
void JmsTriggerHandler::encode(Event &event, const TriggerInfo &info,
	const util::String &containerName, const util::String &operationName,
	uint32_t numColumn, const util::XArray<ColumnType> &columnTypeList,
	const util::XArray<char *> &columnNameList,
	const util::XArray<uint32_t> &columnNameLengthList,
	const util::XArray<const uint8_t *> &columnDataList,
	const util::XArray<uint32_t> &columnDataSizeList) {
	try {
		EventByteOutStream out = event.getOutStream();

		out << info.name_;
		out << info.uri_;
		out << containerName;
		out << operationName;

		out << info.jmsProviderType_;
		out << info.jmsDestinationType_;
		out << info.jmsDestinationName_;

		out << info.jmsUser_;
		out << info.jmsPassword_;

		encodeColumnData(out, numColumn, columnTypeList, columnNameList,
			columnNameLengthList, columnDataList, columnDataSizeList);
	}
	catch (std::exception &e) {
		TXN_RETHROW_ENCODE_ERROR(e, "");
	}
}

void JmsTriggerHandler::decode(util::StackAllocator &alloc, Event &event,
	util::String &triggerName, std::string &uri,
	TriggerService::JmsProviderType &providerType,
	TriggerService::JmsDestinationType &destinationType,
	std::string &destinationName, std::string &user, std::string &password,
	std::string &containerName, std::string &operationName, uint32_t &numColumn,
	util::XArray<ColumnType> &columnTypeList,
	util::XArray<util::String *> &columnNameList,
	util::XArray<util::XArray<uint8_t> *> &columnDataList) {
	try {
		EventByteInStream in = event.getInStream();

		in >> triggerName;
		in >> uri;
		in >> containerName;
		in >> operationName;

		in >> providerType;
		in >> destinationType;
		in >> destinationName;

		in >> user;
		in >> password;

		decodeColumnData(alloc, in, numColumn, columnTypeList, columnNameList,
			columnDataList);
	}
	catch (std::exception &e) {
		TXN_RETHROW_DECODE_ERROR(e, "");
	}
}

void JmsTriggerHandler::createJmsMessage(const std::string &containerName,
	const std::string &operationName, uint32_t numColumn,
	const util::XArray<ColumnType> &columnTypeList,
	const util::XArray<util::String *> &columnNameList,
	const util::XArray<util::XArray<uint8_t> *> &columnDataList,
	cms::TextMessage *message) {
	message->setStringProperty("@container", containerName);
	message->setStringProperty("@event", operationName);

	for (uint32_t i = 0; i < numColumn; i++) {
		const ColumnType columnType = columnTypeList[i];
		const std::string columnName(
			columnNameList[i]->c_str(), columnNameList[i]->size());
		const uint8_t *data = columnDataList[i]->data();
		const uint32_t size = static_cast<uint32_t>(columnDataList[i]->size());
		if (data == NULL) {
			continue;
		}
		switch (columnType) {
		case COLUMN_TYPE_STRING: {
			const std::string tmp(reinterpret_cast<const char *>(data), size);
			message->setStringProperty(columnName, tmp);
		} break;
		case COLUMN_TYPE_BOOL: {
			const bool tmp = *reinterpret_cast<const bool *>(data);
			message->setBooleanProperty(columnName, tmp);
		} break;
		case COLUMN_TYPE_BYTE: {
			const int8_t tmp = *reinterpret_cast<const int8_t *>(data);
			message->setByteProperty(columnName, tmp);
		} break;
		case COLUMN_TYPE_SHORT: {
			const int16_t tmp = *reinterpret_cast<const int16_t *>(data);
			message->setShortProperty(columnName, tmp);
		} break;
		case COLUMN_TYPE_INT: {
			const int32_t tmp = *reinterpret_cast<const int32_t *>(data);
			message->setIntProperty(columnName, tmp);
		} break;
		case COLUMN_TYPE_LONG:
		case COLUMN_TYPE_TIMESTAMP: {
			const int64_t tmp = *reinterpret_cast<const int64_t *>(data);
			message->setLongProperty(columnName, tmp);
		} break;
		case COLUMN_TYPE_FLOAT: {
			const float tmp = *reinterpret_cast<const float *>(data);
			message->setFloatProperty(columnName, tmp);
		} break;
		case COLUMN_TYPE_DOUBLE: {
			const double tmp = *reinterpret_cast<const double *>(data);
			message->setDoubleProperty(columnName, tmp);
		} break;
		case COLUMN_TYPE_BLOB:
		case COLUMN_TYPE_OID:
		case COLUMN_TYPE_STRING_ARRAY:
		case COLUMN_TYPE_BOOL_ARRAY:
		case COLUMN_TYPE_BYTE_ARRAY:
		case COLUMN_TYPE_SHORT_ARRAY:
		case COLUMN_TYPE_INT_ARRAY:
		case COLUMN_TYPE_LONG_ARRAY:
		case COLUMN_TYPE_FLOAT_ARRAY:
		case COLUMN_TYPE_DOUBLE_ARRAY:
		case COLUMN_TYPE_TIMESTAMP_ARRAY:
		case COLUMN_TYPE_WITH_BEGIN:
		default:
			message->setStringProperty(columnName, "");
			break;
		}
	}
}
