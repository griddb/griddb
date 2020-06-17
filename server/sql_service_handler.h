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
#ifndef SQL_SERVICE_HANDLER_H_
#define SQL_SERVICE_HANDLER_H_

#include "sql_common.h"
#include "transaction_service.h"

class ResourceSet;
class ServiceThreadErrorHandler;
class SQLExecutionManager;
class SQLExecution;
struct RequestInfo;

/*!
	@brief Handler of base sql service
*/
class SQLServiceHandler : public StatementHandler {

public:

	SQLServiceHandler() {}

	void replyError(
			EventType eventType,
			PartitionId pId,
			EventContext &ec,
			const NodeDescriptor &nd,
			StatementId stmtId,
			const std::exception &e);

};

/*!
	@brief Handler of sql request
*/
class SQLRequestHandler : public SQLServiceHandler {

	static const char8_t *const querySeparator;

	template <typename T1, typename T2>
	void setValue(
			util::StackAllocator &alloc, T2 value,
					int32_t &size, char *&data) {

		size = sizeof(T1);
		data = reinterpret_cast<char *>(ALLOC_NEW(alloc) T1);
		*reinterpret_cast<T1*>(data) = static_cast<T1>(value);
	}

public:

	void operator()(EventContext &ec, Event &ev);

	static void encodeEnvList(
			EventByteOutStream &out,
			util::StackAllocator &alloc,
			SQLExecutionManager &executionManager,
			const NodeDescriptor &nd);

	static void decodeBindInfo(
			util::ByteStream<util::ArrayInStream> &in,
			RequestInfo &request);

private:

	typedef util::ByteStream< util::ArrayInStream > InStream;

	void decode(
			util::ByteStream<util::ArrayInStream> &in,
			RequestInfo &requestInfo);

	static void decodeBindColumnInfo(
			RequestInfo &requestInfo,
			InStream &fixedPartIn,
			InStream &varPartIn);

	void setDefaultOptionValue(OptionSet &optionSet);
};

/*!
	@brief Handler of request for sql connection node address
*/
class SQLGetConnectionAddressHandler : public SQLServiceHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handler of statement cancel
*/
class SQLCancelHandler : public SQLServiceHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
	bool isClientCancel(Event &ev);
};

/*!
	@brief Handler of disconnect
*/
class SQLSocketDisconnectHandler : public SQLServiceHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

/*!
	@brief Handler of request from advanced-java client
*/
class SQLRequestNoSQLClientHandler : public SQLServiceHandler {
public:
	void operator()(EventContext &ec, Event &ev);

private:

	void executeRepair(
			EventContext &ec,
			SQLExecution *execution,
			util::String &containerName,
			ContainerId largeContainerId,
			SchemaVersionId versionId,
			NodeAffinityNumber affinity);
};


class NoSQLSyncReceiveHandler : public SQLServiceHandler {
public:
	void operator()(EventContext &ec, Event &ev);
};

class UpdateTableCacheHandler : public SQLServiceHandler {
public:
	void operator()(EventContext &ec, Event &ev);
private:
};


#endif
