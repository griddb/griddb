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
#ifndef SQL_REQUEST_INFO_H_
#define SQL_REQUEST_INFO_H_

#include "data_store_common.h"
#include "sql_tuple.h"
#include "transaction_statement_message.h"

class ResourceSet;

typedef uint8_t SQLGetMode;

static const SQLGetMode SQL_CREATE = 1 << 0;

static const SQLGetMode SQL_GET = 1 << 1;

static const SQLGetMode SQL_PUT = (SQL_CREATE |SQL_GET);

static const int32_t SQL_DEFAULT_QUERY_TIMEOUT_INTERVAL = INT32_MAX;
static const int64_t SQL_MAX_ROWS = INT64_MAX;
static const int64_t SQL_DEFAULT_FETCH_SIZE = 65535;

enum SQLRequestType {
	REQUEST_TYPE_EXECUTE,
	REQUEST_TYPE_PREPARE,
	REQUEST_TYPE_QUERY,
	REQUEST_TYPE_UPDATE,
	REQUEST_TYPE_PRAGMA,
	REQUEST_TYPE_FETCH,
	REQUEST_TYPE_CLOSE,
	REQUEST_TYPE_CANCEL,
	UNDEF_REQUEST_TYPE
};

struct BindParam {
	BindParam(util::StackAllocator &alloc, ColumnType type) :
			alloc_(alloc), type_(type) {}

	BindParam(util::StackAllocator &alloc,
			ColumnType type, TupleValue &value) :
					alloc_(alloc), type_(type), value_(value) {}

	~BindParam() {}

	void dump(util::StackAllocator &alloc);

	util::StackAllocator &alloc_;

	ColumnType type_;

	TupleValue value_;
};

struct RequestInfo {
public:
	RequestInfo(
			util::StackAllocator &alloc,
			PartitionId pId, PartitionGroupId pgId) :
					alloc_(alloc),
					pId_(pId),
					pgId_(pgId),
					stmtId_(UNDEF_STATEMENTID),
					requestType_(UNDEF_REQUEST_TYPE),
					closeQueryId_(UNDEF_SESSIONID),
					isAutoCommit_(true),
					transactionStarted_(false),
					retrying_(false),
					queryTimeout_(SQL_DEFAULT_QUERY_TIMEOUT_INTERVAL),
					maxRows_(SQL_MAX_ROWS),
					inTableExists_(false),
					inTableSchema_(alloc),
					inTableData_(alloc),
					sqlCount_(0),
					sqlString_(alloc),
					option_(alloc),
					paramCount_(0),
					sessionMode_(SQL_CREATE),
					eventType_(UNDEF_EVENT_TYPE),
					preparedParamList_(alloc),
					serializedBindInfo_(alloc),
					serialized_(true) {
	}

	RequestInfo(
			util::StackAllocator &alloc,
			bool serialized) :
					alloc_(alloc),
					pId_(0),
					pgId_(0),
					stmtId_(UNDEF_STATEMENTID),
					requestType_(UNDEF_REQUEST_TYPE),
					closeQueryId_(UNDEF_SESSIONID),
					isAutoCommit_(true),
					transactionStarted_(false),
					retrying_(false),
					queryTimeout_(SQL_DEFAULT_QUERY_TIMEOUT_INTERVAL),
					maxRows_(SQL_MAX_ROWS),
					inTableExists_(false),
					inTableSchema_(alloc),
					inTableData_(alloc),
					sqlCount_(0),
					sqlString_(alloc),
					option_(alloc),
					paramCount_(0),
					sessionMode_(SQL_CREATE),
					eventType_(UNDEF_EVENT_TYPE),
					preparedParamList_(alloc),
					serializedBindInfo_(alloc),
					serialized_(serialized) {
	}

	~RequestInfo() {}

	bool isBind() {
		return (inTableExists_ == true);
	}

	util::StackAllocator &alloc_;

	PartitionId pId_;

	PartitionGroupId pgId_;

	ClientId clientId_;

	StatementId stmtId_;

	SQLRequestType requestType_;

	SessionId closeQueryId_;

	bool isAutoCommit_;

	bool transactionStarted_;

	bool retrying_;

	int32_t queryTimeout_;

	int64_t maxRows_;

	bool inTableExists_;

	util::XArray<uint8_t> inTableSchema_;

	util::XArray<uint8_t> inTableData_;

	int32_t sqlCount_;

	util::String sqlString_;

	StatementMessage::OptionSet option_;

	int32_t paramCount_;

	SQLGetMode sessionMode_;

	EventType eventType_;

	util::Vector<BindParam*> preparedParamList_;

	util::XArray<uint8_t> serializedBindInfo_;
	bool serialized_;
};

struct SQLExecutionRequestInfo {

	SQLExecutionRequestInfo(
			RequestInfo &requestInfo,
			const NodeDescriptor *clientNd,
			const ResourceSet &resourceSet) :
					requestInfo_(requestInfo),
					clientNd_(clientNd),
					resourceSet_(resourceSet) {}

	RequestInfo &requestInfo_;
	const NodeDescriptor *clientNd_;
	const ResourceSet &resourceSet_;
};

#endif
