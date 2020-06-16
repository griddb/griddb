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
#ifndef NOSQL_REQUEST_H_
#define NOSQL_REQUEST_H_

#include "sql_common.h"
#include "transaction_statement_message.h"

class NoSQLContainer;
class SQLService;
class TransactionService;
class ResourceSet;

typedef uint8_t StatementExecStatus;

struct NoSQLSyncContext {
	
	class NoSQLRequest {

	public:

		NoSQLRequest(
				SQLVariableSizeGlobalAllocator &varAlloc,
				SQLService *sqlSvc,
				TransactionService *txnSvc,
				ClientId *clientId);

		~NoSQLRequest();

		NoSQLRequestId start(Event &request);

		void get(
				Event &request,
				util::StackAllocator &alloc,
				NoSQLContainer *container,
				int32_t waitInterval,
				int32_t timeoutInterval,
				util::XArray<uint8_t> &response);

		void wait(int32_t waitInterval);
		
		void put(
				NoSQLRequestId requestId,
				StatementExecStatus status,
				EventByteInStream &in,
				util::Exception *exception,
				StatementMessage::Request &request);

		void cancel();
		
		void getResponse(util::XArray<uint8_t> &buffer);
		
		bool isRunning();

		NoSQLRequestId getRequestId() {

			util::LockGuard<util::Condition> guard(condition_);
			return requestId_;
		}

	private:
		
		NoSQLRequest(const NoSQLRequest&);
		NoSQLRequest& operator=(const NoSQLRequest&);

		enum State {
			STATE_NONE,
			STATE_SUCCEEDED,
			STATE_FAILED
		};

		SQLVariableSizeGlobalAllocator &varAlloc_;
		util::Condition condition_;
		State state_;
		util::Exception exception_;
		NoSQLRequestId requestId_;
		EventType eventType_;
		SQLService *sqlSvc_;
		TransactionService *txnSvc_;
		uint8_t *syncBinaryData_;
		size_t syncBinarySize_;
		NoSQLContainer *container_;
		ClientId *clientId_;
		void clear();
};

	NoSQLSyncContext(
			const ResourceSet *resourceSet,
			ClientId &clientId,
			SQLVariableSizeGlobalAllocator &globalVarAlloc,
			StatementMessage::UserType userType,
			const char *dbName,
			DatabaseId dbId,
			int32_t statementTimeoutInterval,
			int32_t txnTimeoutInterval);

	~NoSQLSyncContext() {}

	SQLVariableSizeGlobalAllocator &getGlobalAllocator() {
		return globalVarAlloc_;
	}

	const ResourceSet *getResourceSet() {
		return resourceSet_;
	}

	void get(Event &request,
			util::StackAllocator &alloc,
			NoSQLContainer *container,
			int32_t waitInterval,
			int32_t timeoutInterval,
			util::XArray<uint8_t> &response);

	void wait(int32_t waitInterval);
	
	void put(
			int64_t syncId,
			StatementExecStatus status,
			EventByteInStream &in,
			util::Exception &exception,
			StatementMessage::Request &request);

	void cancel();

	bool isRunning() ;
	
	int64_t getRequestId() {
		return nosqlRequest_.getRequestId();
	}

	PartitionId getReplyPId() {
		return replyPId_;
	}

	int32_t getStatementTimeoutInterval() {
		return timeoutInterval_;
	}

	int32_t getTxnTimeoutInterval() {
		return txnTimeoutInterval_;
	}

	ClientId &getClinetId() {
		return clientId_;
	}

	StatementMessage::UserType getUserType() {
		return userType_;
	}

	DatabaseId getDbId() {
		return dbId_;
	}

	const char *getDbName() {
		return dbName_;
	}

private:

	util::Mutex lock_;
	const ResourceSet *resourceSet_;
	SQLVariableSizeGlobalAllocator &globalVarAlloc_;
	ClientId clientId_;
	StatementMessage::UserType userType_;
	PartitionId replyPId_;
	const char *dbName_;
	DatabaseId dbId_;
	int32_t timeoutInterval_;
	int32_t txnTimeoutInterval_;
	NoSQLRequest nosqlRequest_;
};

#endif
