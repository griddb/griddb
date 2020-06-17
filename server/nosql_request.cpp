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
#include "nosql_request.h"
#include "resource_set.h"
#include "nosql_container.h"
#include "transaction_service.h"
#include "sql_service.h"

UTIL_TRACER_DECLARE(SQL_SERVICE);

void NoSQLSyncContext::put(
	int64_t syncId,
	StatementExecStatus status,
	EventByteInStream &in,
	util::Exception &exception,
	StatementMessage::Request &request) {

		nosqlRequest_.put(
				syncId, status, in, &exception, request);
}

void NoSQLSyncContext::cancel() {
	nosqlRequest_.cancel();
}

bool NoSQLSyncContext::isRunning() {
	return nosqlRequest_.isRunning();
}

void NoSQLSyncContext::get(
		Event &request,
		util::StackAllocator &alloc,
		NoSQLContainer *container,
		int32_t waitInterval,
		int32_t timeoutInterval,
		util::XArray<uint8_t> &response) {

	nosqlRequest_.get(
			request,
			alloc,
			container,
			waitInterval,
			timeoutInterval,
			response);
}

void NoSQLSyncContext::wait(int32_t waitInterval) {

	nosqlRequest_.wait(waitInterval);
}

NoSQLSyncContext::NoSQLRequest::NoSQLRequest(
		SQLVariableSizeGlobalAllocator &varAlloc,
		SQLService *sqlSvc,
		TransactionService *txnSvc,
		ClientId *clientId) :
				varAlloc_(varAlloc),
				state_(STATE_NONE),
				requestId_(0),
				eventType_(UNDEF_EVENT_TYPE),
				sqlSvc_(sqlSvc),
				txnSvc_(txnSvc),
				syncBinaryData_(NULL),
				syncBinarySize_(0),
				container_(NULL),
				clientId_(clientId) {
}

NoSQLSyncContext::NoSQLRequest::~NoSQLRequest() {
	clear();
};

NoSQLSyncContext::NoSQLSyncContext(
			const ResourceSet *resourceSet,
			ClientId &clientId,
			SQLVariableSizeGlobalAllocator &globalVarAlloc,
			StatementMessage::UserType userType,
			const char *dbName,
			DatabaseId dbId,
			int32_t statementTimeoutInterval,
			int32_t txnTimeoutInterval) :
					resourceSet_(resourceSet),
					globalVarAlloc_(globalVarAlloc), 
					clientId_(clientId),
					userType_(userType),
					replyPId_(replyPId_),
					dbName_(dbName),
					dbId_(dbId),
					timeoutInterval_(statementTimeoutInterval),
					txnTimeoutInterval_(txnTimeoutInterval),
					nosqlRequest_(
							globalVarAlloc,
							resourceSet->getSQLService(),
							resourceSet->getTransactionService(),
							&clientId_) {
}

void NoSQLSyncContext::NoSQLRequest::clear() {

	if (syncBinaryData_) {
		varAlloc_.deallocate(syncBinaryData_);
		syncBinaryData_ = NULL;
		syncBinarySize_ = 0;
	}
	eventType_ = UNDEF_EVENT_TYPE;
}

void NoSQLSyncContext::NoSQLRequest::wait(
		int32_t waitInterval) {

	util::LockGuard<util::Condition> guard(condition_);
	requestId_++;
	condition_.wait(waitInterval);
}

void NoSQLSyncContext::NoSQLRequest::get(
		Event &request,
		util::StackAllocator &alloc,
		NoSQLContainer *container,
		int32_t waitInterval,
		int32_t timeoutInterval,
		util::XArray<uint8_t> &response) {

	util::LockGuard<util::Condition> guard(condition_);
	container_ = container;

	try {
		state_ = STATE_NONE;
		clear();

		requestId_++;
		eventType_ = request.getType();

		container->encodeRequestId(alloc, request, requestId_);

		container->sendMessage(*txnSvc_->getEE(), request);

		util::Stopwatch watch;
		watch.start();

		for (;;) {
			condition_.wait(waitInterval);
			if (state_ != STATE_NONE) {
				break;
			}
			int32_t currentTime = watch.elapsedMillis();
			if (currentTime >= timeoutInterval) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_CANCELLED,
						"Elapsed time expired limt interval=" << timeoutInterval);
			}

			sqlSvc_->checkActiveStatus();
			container->checkCondition(
					watch.elapsedMillis(), timeoutInterval);
			
			GS_TRACE_WARNING(
					SQL_SERVICE, GS_TRACE_SQL_LONG_EVENT_WAITING,
					"Long event waiting (eventType=" 
					<< getEventTypeName(eventType_) << 
					", pId=" << request.getPartitionId() <<
					", elapsedMillis=" << watch.elapsedMillis() << ")");
		}
		if (state_ != STATE_SUCCEEDED) {
			try {
				throw exception_;
			}
			catch (std::exception &e) {
				GS_RETHROW_USER_ERROR(e, "");
			}
		}
		else {
			getResponse(response);
		}
	}
	catch (...) {
		container_ = NULL;
		state_ = STATE_NONE;
		exception_ = util::Exception();
		clear();
		throw;
	}
	container_ = NULL;
	clear();
	state_ = STATE_NONE;
}

void NoSQLSyncContext::NoSQLRequest::cancel() {
	
	util::LockGuard<util::Condition> guard(condition_);
	
	try {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_CANCELLED, 
				"Cancel SQL, clientId=" 
				<< *clientId_ << ", location=NoSQL Request" );
	}
	catch (util::Exception &e) {
		exception_ = e;
		state_ = STATE_FAILED;
	}
	container_ = NULL;
	condition_.signal();
}

void NoSQLSyncContext::NoSQLRequest::put(
		NoSQLRequestId requestId,
		StatementExecStatus status,
		EventByteInStream &in,
		util::Exception *exception,
		StatementMessage::Request &request) {

	util::LockGuard<util::Condition> guard(condition_);

	if (requestId_ != requestId) {
		return;
	}

	bool success =
			(status == StatementHandler::TXN_STATEMENT_SUCCESS);

	if (success) {

		if (container_) {
			container_->setRequestOption(request);
		}
		
		syncBinarySize_ = in.base().remaining();
		if (syncBinarySize_ != 0) {
			syncBinaryData_ = static_cast<uint8_t *>(
					varAlloc_.allocate(syncBinarySize_));
			in.base().read(syncBinaryData_, syncBinarySize_);
		}
		state_ = STATE_SUCCEEDED;
	}
	else {
		try {
			throw *exception;
		}
		catch (std::exception &e) {
			exception_ = GS_EXCEPTION_CONVERT(e, "");
			state_ = STATE_FAILED;
		}
	}
	condition_.signal();
}

void NoSQLSyncContext::NoSQLRequest::getResponse(
		util::XArray<uint8_t> &buffer) {

	if (syncBinarySize_  > 0) {
		buffer.resize(syncBinarySize_);
		util::ArrayByteInStream in =
			util::ArrayByteInStream(
				util::ArrayInStream(syncBinaryData_, syncBinarySize_));
		in >> std::make_pair(buffer.data(), syncBinarySize_);
		clear();
	}
}

bool NoSQLSyncContext::NoSQLRequest::isRunning() {

	util::LockGuard<util::Condition> guard(condition_);
	return (requestId_ != UNDEF_NOSQL_REQUESTID
			&& state_ == STATE_NONE
			&& eventType_ != UNDEF_EVENT_TYPE);
}