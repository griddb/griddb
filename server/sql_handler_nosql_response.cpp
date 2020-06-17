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
#include "sql_service_handler.h"
#include "sql_service.h"
#include "sql_execution_manager.h"
#include "sql_execution.h"
#include "nosql_request.h"
#include "sql_job.h"

UTIL_TRACER_DECLARE(SQL_SERVICE);

void NoSQLSyncReceiveHandler::operator ()(
		EventContext &ec, Event &ev) {

	util::StackAllocator &alloc = ec.getAllocator();
	util::StackAllocator::Scope scope(alloc);

	SQLExecutionManager *executionManager
			= resourceSet_->getSQLExecutionManager();
	JobManager *jobManager = resourceSet_->getJobManager();
	
	try {
		Request request(alloc, getRequestSource(ev));

		EventByteInStream in(ev.getInStream());
		StatementId stmtId;
		StatementExecStatus status;

		in >> stmtId;
		in >> status;

		util::Exception dest;
		if (status != TXN_STATEMENT_SUCCESS) {
		
			SQLErrorUtils::decodeException(alloc, in, dest);
		}

		ConnectionOption &connOption =
				ev.getSenderND().getUserData<ConnectionOption>();
		decodeRequestOptionPart(in, request, connOption);

		ClientId targetClientId;
		request.optional_.get<Options::UUID>().get(
				targetClientId.uuid_);
		targetClientId.sessionId_
				= request.optional_.get<Options::QUERY_ID>();

		if (!request.optional_.get<Options::FOR_SYNC>()) {
			
			JobId jobId;
			jobId.clientId_ = targetClientId;
			jobId.execId_
					= request.optional_.get<Options::JOB_EXEC_ID>();
			jobId.versionId_
					= request.optional_.get<Options::JOB_VERSION>();
			
			JobLatch jobLatch(
					jobId, jobManager->getResourceManager(), NULL);

			Job *job = jobLatch.get();
			if (job) {

				job->recieveNoSQLResponse(
						ec,
						dest,
						status,
						request.optional_.get<Options::SUB_CONTAINER_ID>());
			}
			else {
				return;
			}
		}
		else {
			ExecutionLatch executionLatch(
					targetClientId,
					executionManager->getResourceManager(),
					NULL);

			SQLExecution *execution = executionLatch.get();
			
			if (execution) {

				if (status == TXN_STATEMENT_SUCCESS
						&& execution->isCancelled()) {

					status = TXN_STATEMENT_ERROR;

					try {

						GS_THROW_USER_ERROR(
								GS_ERROR_SQL_CANCELLED,
							"Cancel SQL, clientId=" << targetClientId
							<< ", location=NoSQL Recieve");
					}
					catch (std::exception &e) {
						dest = GS_EXCEPTION_CONVERT(e, "");
					}
				}

				NoSQLSyncContext &syncContext
						= execution->getContext().getSyncContext();
				
				const int64_t requestId =
						request.optional_.get<Options::NOSQL_SYNC_ID>();
				
				syncContext.put(
						requestId, status, in, dest, request);
			}
		}
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(SQL_SERVICE, e,
				"NoSQL sync receive failed, reason="
				<< GS_EXCEPTION_MESSAGE(e));
	}
}