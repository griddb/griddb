/*
	Copyright (c) 2013 TOSHIBA CORPORATION.

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
	@brief Definition of TriggerService
*/
#ifndef TRIGGER_SERVICE_H_
#define TRIGGER_SERVICE_H_

#include "config_table.h"
#include "data_store_common.h"
#include "event_engine.h"
#include "gs_error.h"

#include "util/trace.h"

#include <map>
#include <queue>

UTIL_TRACER_DECLARE(TRIGGER_SERVICE);

class ConfigTable;
class MessageRowStore;
class TriggerInfo;
class BaseContainer;
class DataStore;
class TransactionContext;
class ClusterService;
namespace cms {
class ConnectionFactory;
class Connection;
class TextMessage;
}
class RestTriggerHandler;
class JmsTriggerHandler;

/*!
	@brief TriggerService
*/
class TriggerService {
public:
	TriggerService(const ConfigTable &param);

	~TriggerService();

	int32_t getTimeoutInterval() const;

	typedef int8_t TriggerType;
	static const TriggerType TRIGGER_UNDEF = -1;
	static const TriggerType TRIGGER_REST = 0;
	static const TriggerType TRIGGER_JMS = 1;

	typedef int32_t OperationType;
	static const OperationType NO_OPERATION;
	static const OperationType PUT_ROW;
	static const OperationType DELETE_ROW;

	typedef int32_t JmsProviderType;
	static const JmsProviderType JMS_PROVIDER_UNDEF = -1;
	static const JmsProviderType JMS_PROVIDER_ACTIVEMQ = 0;

	typedef int32_t JmsDestinationType;
	static const JmsDestinationType JMS_DESTINATON_UNDEF = -1;
	static const JmsDestinationType JMS_DESTINATON_QUEUE = 0;
	static const JmsDestinationType JMS_DESTINATON_TOPIC = 1;

	static const char *jmsProviderTypeToStr(JmsProviderType type);
	static const char *jmsDestinationTypeToStr(JmsDestinationType type);

	static const size_t LIMIT_TRIGGER_URI_SIZE = 2048 * 2;

	static const uint32_t DEFAULT_RESPONSE_BUFFER_SIZE = 1024;

	class PooledConnection;

	/*!
		@brief Connection pool
	*/
	class ConnectionPool {
	public:
		ConnectionPool(uint32_t maxPoolSize);
		~ConnectionPool();

		PooledConnection *get(const std::string &brokerURI,
			const std::string &user, const std::string &password);

		void release(const std::string &brokerURI, const std::string &user,
			const std::string &password, PooledConnection *&connection);

		void invalidate(const std::string &brokerURI, const std::string &user,
			const std::string &password, PooledConnection *&connection);

		void close();

	private:
		const uint32_t maxPoolSize_;
		uint32_t numInstance_;

		std::map<std::string, PooledConnection *> pooledConnectionMap_;
	};

	/*!
		@brief PooledConnection
	*/
	class PooledConnection {
		friend class ConnectionPool;

	public:
		cms::Connection *getConnection();

		~PooledConnection();

	private:
		PooledConnection(
			cms::ConnectionFactory *factory, cms::Connection *connection);

		cms::ConnectionFactory *factory_;
		cms::Connection *connection_;
	};

	PooledConnection *getConnection(const std::string &brokerURI,
		const std::string &user, const std::string &password);

	void releaseConnection(const std::string &brokerURI,
		const std::string &user, const std::string &password,
		PooledConnection *&connection);

	void invalidateConnection(const std::string &brokerURI,
		const std::string &user, const std::string &password,
		PooledConnection *&connection);

private:
	friend class TriggerHandler;

	static const uint32_t DEFAULT_CONNECTION_POOL_SIZE = 5;

	static class ConfigSetUpHandler : public ConfigTable::SetUpHandler {
		virtual void operator()(ConfigTable &config);
	} configSetUpHandler_;

	const int32_t timeoutInterval_;

	std::vector<ConnectionPool *> connectionPool_;

	RestTriggerHandler *restTriggerHandler_;
	JmsTriggerHandler *jmsTriggerHandler_;
};

/*!
	@brief TriggerHandler
*/
class TriggerHandler : virtual public EventHandler {
public:
	TriggerHandler(TriggerService &triggerService);
	virtual ~TriggerHandler();

	static void checkEventNotificationUrl(
		util::StackAllocator &alloc, const util::String &url);

	static void checkTriggerRegistration(
		util::StackAllocator &alloc, TriggerInfo &info);
	static void checkTrigger(TriggerService &triggerService,
		TransactionContext &txn, BaseContainer &container, EventContext &ec,
		const util::XArray<TriggerService::OperationType> &operationTypeList,
		uint64_t numRow, const void *putRowData, size_t putRowDataSize);
	static void parseUrl(const util::String &url, util::String &host,
		util::String &port, util::String &path);

	static bool isSupportedColumnType(ColumnType type);

	static bool isQuoteColumnType(ColumnType type);

	static void convertOperationTypeToString(
		TriggerService::OperationType type, util::String &str);

	static void convertColumnDataToString(
		ColumnType type, const uint8_t *data, size_t size, util::String &str);

protected:
	TriggerService &triggerService_;
	static void encodeColumnData(EventByteOutStream &out, uint32_t numColumn,
		const util::XArray<ColumnType> &columnTypeList,
		const util::XArray<char *> &columnNameList,
		const util::XArray<uint32_t> &columnNameLengthList,
		const util::XArray<const uint8_t *> &columnDataList,
		const util::XArray<uint32_t> &columnDataSizeList);
	static void decodeColumnData(util::StackAllocator &alloc,
		util::ByteStream<util::ArrayInStream> &in, uint32_t &numColumn,
		util::XArray<ColumnType> &columnTypeList,
		util::XArray<util::String *> &columnNameList,
		util::XArray<util::XArray<uint8_t> *> &columnDataList);
};

/*!
	@brief Trigger(REST)Handler
*/
class RestTriggerHandler : public TriggerHandler {
public:
	RestTriggerHandler(TriggerService &triggerService);
	virtual ~RestTriggerHandler();

	void operator()(EventContext &ec, Event &ev);

	static void encode(Event &event, const TriggerInfo &info,
		const util::String &containerName, const util::String &operationName,
		uint32_t numColumn, const util::XArray<ColumnType> &columnTypeList,
		const util::XArray<char *> &columnNameList,
		const util::XArray<uint32_t> &columnNameLengthList,
		const util::XArray<const uint8_t *> &columnDataList,
		const util::XArray<uint32_t> &columnDataSizeList);

private:
	static void decode(util::StackAllocator &alloc, Event &event,
		util::String &triggerName, util::String &uri,
		util::String &containerName, util::String &operationName,
		uint32_t &numColumn, util::XArray<ColumnType> &columnTypeList,
		util::XArray<util::String *> &columnNameList,
		util::XArray<util::XArray<uint8_t> *> &columnDataList);

	static void createRestContent(util::StackAllocator &alloc,
		const util::String &containerName, const util::String &operationName,
		uint32_t numColumn, const util::XArray<ColumnType> &columnTypeList,
		const util::XArray<util::String *> &columnNameList,
		const util::XArray<util::XArray<uint8_t> *> &columnDataList,
		util::String &content);

	static bool isRequestSucceeded(const util::String &response);
};

/*!
	@brief Trigger(JMS)Handler
*/
class JmsTriggerHandler : public TriggerHandler {
public:
	JmsTriggerHandler(TriggerService &triggerService);
	virtual ~JmsTriggerHandler();

	void operator()(EventContext &ec, Event &ev);

	static void encode(Event &event, const TriggerInfo &info,
		const util::String &containerName, const util::String &operationName,
		uint32_t numColumn, const util::XArray<ColumnType> &columnTypeList,
		const util::XArray<char *> &columnNameList,
		const util::XArray<uint32_t> &columnNameLengthList,
		const util::XArray<const uint8_t *> &columnDataList,
		const util::XArray<uint32_t> &columnDataSizeList);

private:
	static void decode(util::StackAllocator &alloc, Event &event,
		util::String &triggerName, std::string &uri,
		TriggerService::JmsProviderType &providerType,
		TriggerService::JmsDestinationType &destinationType,
		std::string &destinationName, std::string &user, std::string &password,
		std::string &containerName, std::string &operationName,
		uint32_t &numColumn, util::XArray<ColumnType> &columnTypeList,
		util::XArray<util::String *> &columnNameList,
		util::XArray<util::XArray<uint8_t> *> &columnDataList);

	static void createJmsMessage(const std::string &containerName,
		const std::string &operationName, uint32_t numColumn,
		const util::XArray<ColumnType> &columnTypeList,
		const util::XArray<util::String *> &columnNameList,
		const util::XArray<util::XArray<uint8_t> *> &columnDataList,
		cms::TextMessage *message);
};
#endif
