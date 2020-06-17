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
#include "resource_set.h"
#include "sql_job.h"
#include "sql_job_manager.h"
#include "transaction_service.h"
#include "sql_execution_manager.h"
#include "sql_service.h"

UTIL_TRACER_DECLARE(SQL_SERVICE);

void SQLJobHandler::initialize(
		const ResourceSet &resourceSet) {

	resourceSet_ = &resourceSet;
}

void ExecuteJobHandler::executeRequest(
		EventContext &ec, Event &ev,
		EventByteInStream &in,
		JobId &jobId,
		ControlType controlType) {

	JobManager *jobManager
			= resourceSet_->getJobManager();

	TaskId taskId = 0;
	InputId inputId = 0;
	PartitionId recvPartitionId = UNDEF_PARTITIONID;

	try {
		bool isEmpty;
		bool isComplete;
		int64_t blockNo = -1;
		
		decodeTaskInfo(
				in, taskId, inputId, isEmpty, isComplete, blockNo);

		EventMonotonicTime emTime 
				= ec.getEngine().getMonotonicTime();
		
		JobLatch latch(
				jobId, jobManager->getResourceManager(), NULL);
		Job *job = latch.get();
		
		if (job == NULL) {
			GS_TRACE_DEBUG(SQL_SERVICE,
					GS_TRACE_SQL_EXECUTION_INFO,
					"ExecuteRequest:jobId=" << jobId);
			return;
		}

		recvPartitionId = job->getPartitionId();
		if (isEmpty || isComplete) {

			TupleList::Block block;
			job->request(
					&ec,
					&ev,
					controlType,
					taskId,
					inputId,
					isEmpty,
					isComplete,
					&block,
					emTime);
		}
		else {
			Task *task = NULL;
			TupleList::Block block;
			bool isExistBlock = false;
			
			bool isExists = job->getTaskBlock(
					taskId,
					inputId,
					task,
					block,
					isExistBlock);

			if (isExists) {
				job->request(
						&ec,
						&ev,
						controlType,
						taskId,
						inputId,
						isEmpty,
						isComplete, 
						&block,
						emTime);
			}
			else {
				if (!isExistBlock) {
					job->checkNextEvent(
							ec, controlType, taskId, inputId);
				}
				else {
				}
			}
		}
	}
	catch (std::exception &e) {
		handleError(ec, e, jobId);
	}
}

void ExecuteJobHandler::executeError(
		EventContext &ec,
		EventByteInStream &in,JobId &jobId) {

	JobManager *jobManager
			= resourceSet_->getJobManager();
	util::StackAllocator &alloc = ec.getAllocator();
	try {
	
		JobLatch latch(
				jobId, jobManager->getResourceManager(), NULL);
		Job *job = latch.get();
		if (job == NULL) {
			GS_TRACE_DEBUG(SQL_SERVICE,
					GS_TRACE_SQL_EXECUTION_INFO,
					"ExecuteError:jobId=" << jobId);
			return;
		}

		StatementHandler::StatementExecStatus status;
		in >> status;
		util::Exception exception;
		SQLErrorUtils::decodeException(alloc, in, exception);
		
		try {
			throw exception;
		}
		catch (std::exception &e) {
			jobManager->executeStatementError(ec, &e, jobId);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void ExecuteJobHandler::executeCancel(
		EventContext &ec,
		JobId &jobId) {

	JobManager *jobManager
			= resourceSet_->getJobManager();

	try {
		JobLatch latch(
				jobId, jobManager->getResourceManager(), NULL);
		Job *job = latch.get();
		
		if (job == NULL) {
			GS_TRACE_DEBUG(SQL_SERVICE,
					GS_TRACE_SQL_EXECUTION_INFO,
					"executeCancel:jobId=" << jobId);
			return;
		}

		job->setCancel();

		if (job->isCoordinator()) {
			try {
				job->checkCancel("job", false);
			}
			catch (std::exception &e) {
				jobManager->executeStatementError(ec, &e, jobId);
			}
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void ExecuteJobHandler::executeRecvAck(
		EventContext &ec,
		Event &ev,
		EventByteInStream &in,
		JobId &jobId) {

	JobManager *jobManager
			= resourceSet_->getJobManager();

	NodeId nodeId = 0;
	
	try {
		if (!ev.getSenderND().isEmpty()) {
			nodeId = static_cast<NodeId>(
					ev.getSenderND().getId());
		}
		
		JobLatch latch(
			jobId, jobManager->getResourceManager(), NULL);
		Job *job = latch.get();
		
		if (job == NULL) {
			GS_TRACE_DEBUG(SQL_SERVICE,
					GS_TRACE_SQL_EXECUTION_INFO,
					"ExecuteRecvAck:jobId=" << jobId);
			return;
		}

		job->recvExecuteSuccess(ec, in, (nodeId == 0));
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void ExecuteJobHandler::operator()(
		EventContext &ec, Event &ev) {

	JobId jobId;
	ControlType controlType = FW_CONTROL_UNDEF;
	int32_t sqlVersionId;
	PartitionId recvPartitionId = ev.getPartitionId();

	EventStart eventStart(
			ec,
			ev,
			resourceSet_->getTransactionService()
					->getManager()->getEventMonitor(),
			(getServiceType() == TRANSACTION_SERVICE));

	try {

		EventByteInStream in(ev.getInStream());
		decodeRequestInfo(in, jobId, controlType, sqlVersionId);

		switch (controlType) {
		case FW_CONTROL_DEPLOY:
			
			executeDeploy(
					ec,
					ev,
					in,
					jobId,
					recvPartitionId);

			break;
		case FW_CONTROL_PIPE:
		case FW_CONTROL_FINISH:
		case FW_CONTROL_PIPE_FINISH:
		case FW_CONTROL_NEXT: {
			executeRequest(ec, ev, in, jobId, controlType);
		}
		break;
		case FW_CONTROL_ERROR: {
			executeError(ec, in, jobId);
		}
		break;
		case FW_CONTROL_CANCEL: {
			executeCancel(ec, jobId);
		}
		break;
		case FW_CONTROL_EXECUTE_SUCCESS: {
			executeRecvAck(ec, ev, in, jobId);
		}
		break;
		default:
			GS_THROW_USER_ERROR(
					GS_ERROR_JOB_INVALID_CONTROL_TYPE, "");
		}
	}
	catch (std::exception &e) {
		try {
			handleError(ec, e, jobId);
		}
		catch (std::exception &e2) {
			UTIL_TRACE_EXCEPTION(SQL_SERVICE, e2, "");
		}
	}
}

void ControlJobHandler::operator()(
		EventContext &ec, Event &ev) {

	JobId jobId;
	ControlType controlType = FW_CONTROL_UNDEF;
	JobManager *jobManager
			= resourceSet_->getJobManager();
	
	try {

		NodeId nodeId = 0;
		if (!ev.getSenderND().isEmpty()) {
			nodeId = static_cast<NodeId>(
					ev.getSenderND().getId());
		}

		EventByteInStream in(ev.getInStream());
		int32_t sqlVersionId;
		decodeRequestInfo(in, jobId, controlType, sqlVersionId);
	
		JobLatch latch(
				jobId, jobManager->getResourceManager(), NULL);
		Job *job = latch.get();

		if (job == NULL) {
			GS_TRACE_DEBUG(SQL_SERVICE,
					GS_TRACE_SQL_EXECUTION_INFO,
					"ControlJob:jobId=" << jobId);
			return;
		}

		switch (controlType) {
		
		case FW_CONTROL_DEPLOY_SUCCESS:
			job->recvDeploySuccess(ec);
		break;

		case FW_CONTROL_PIPE0: {
			EventMonotonicTime emTime = -1;
			job->request(
					&ec,
					NULL, 
					controlType,
					-1,
					0,
					false,
					false,
					static_cast<TupleList::Block *>(NULL),
					emTime);
		}
		break;
		case FW_CONTROL_CANCEL:
			job->setCancel();
			jobManager->cancel(ec, jobId, true);
		break;
		case FW_CONTROL_HEARTBEAT:
			break;
		default:
			GS_THROW_USER_ERROR(
					GS_ERROR_JOB_INVALID_CONTROL_TYPE, "");
		}
	}
	catch (std::exception &e) {
		try {
			handleError(ec, e, jobId);
		}
		catch (std::exception &e2) {
			UTIL_TRACE_EXCEPTION(SQL_SERVICE, e2, "");
		}
	}
}

void SQLResourceCheckHandler::operator() (
		EventContext &ec, Event &ev) {

	SQLService *sqlService
			= resourceSet_->getSQLService();
	JobManager *jobManager
			= resourceSet_->getJobManager();
	SQLExecutionManager *executionManager
			= resourceSet_->getSQLExecutionManager();

	try {

		util::StackAllocator &alloc = ec.getAllocator();
		util::StackAllocator::Scope scope(alloc);
		EventType eventType = ev.getType();
		
		switch (eventType) {
			
case REQUEST_CANCEL: {
				sqlService->requestCancel(ec);
			}
			break;

			case CHECK_TIMEOUT_JOB: {

				executionManager->checkTimeout(ec);				
				jobManager->checkTimeout(ec);
			}
			break;
		}
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(SQL_SERVICE, e, "");
	}
}

void DispatchJobHandler::operator()(
		EventContext &ec, Event &ev) {

	JobManager *jobManager
			= resourceSet_->getJobManager();

	JobId jobId;
	TaskId taskId = 0;
	InputId inputId = 0;
	ControlType controlType = FW_CONTROL_UNDEF;

	try {
		util::StackAllocator &alloc = ec.getAllocator();
		EventByteInStream in(ev.getInStream());
		
		bool isLocal = false;
		if (ev.getSenderND().isEmpty()) {
			isLocal = true;
		}
		
		int32_t sqlVersionId;
		decodeRequestInfo(in, jobId, controlType, sqlVersionId);
		
		bool isEmpty = false;
		bool isComplete = false;
		
		int64_t blockNo = 0;
		decodeTaskInfo(
				in, taskId, inputId, isEmpty, isComplete, blockNo);

		JobLatch latch(
			jobId, jobManager->getResourceManager(), NULL);
		Job *job = latch.get();

		if (job == NULL) {
			GS_TRACE_DEBUG(SQL_SERVICE,
					GS_TRACE_SQL_EXECUTION_INFO,
					"DispatchJob:jobId=" << jobId);
			return;
		}

		if (controlType == FW_CONTROL_ERROR
				|| controlType == FW_CONTROL_CANCEL) {
			job->setCancel();
		}
		job->checkCancel("dispatch", false);

		int32_t loadBalance;
		in >> loadBalance;

		LocalTempStore &store = jobManager->getStore();
		TupleList::Block block(
				store, store.getDefaultBlockSize(),
				LocalTempStore::UNDEF_GROUP_ID);
		
		if (!isEmpty && !isComplete) {
			block.decode(alloc, in);
		}
		job->recieveTaskBlock(
				ec,
				taskId,
				inputId,
				&block,
				blockNo,
				controlType,
				isEmpty,
				false);
	}
	catch (std::exception &e) {
		try {
			handleError(ec, e, jobId);
		}
		catch (std::exception &e2) {
			UTIL_TRACE_EXCEPTION(SQL_SERVICE, e2, "");
		}
	}
}

void SQLJobHandler::handleError(
		EventContext &ec,
		std::exception &e,
		JobId &jobId) {

	JobManager *jobManager
			= resourceSet_->getJobManager();

	if (!jobId.isValid()) {
		UTIL_TRACE_EXCEPTION_WARNING(SQL_SERVICE, e, "");
		return;
	}

	JobLatch latch(
			jobId, jobManager->getResourceManager(), NULL);
	
	Job *job = latch.get();
	if (job != NULL) {
		try {
			job->handleError(ec, &e);
		}
		catch (std::exception &e2) {
			UTIL_TRACE_EXCEPTION(SQL_SERVICE, e2, "");

			if (GS_EXCEPTION_CHECK_CRITICAL(e2)) {

				resourceSet_->getClusterService()->
						setSystemError(&e2);
			}
		}
	}
}

void SQLJobHandler::decodeRequestInfo(
		EventByteInStream &in,
		JobId &jobId,
		ControlType &controlType,
		int32_t &sqlVersionId) {		

	in >> sqlVersionId;
	in.readAll(
			jobId.clientId_.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
	in >> jobId.clientId_.sessionId_;
	in >> jobId.execId_;
	in >> jobId.versionId_;
	int8_t tmp;
	in >> tmp;
	controlType = static_cast<ControlType>(tmp);
	SQLService::checkVersion(sqlVersionId);
}

void SQLJobHandler::decodeTaskInfo(
		EventByteInStream &in,
		TaskId &taskId,
		InputId &inputId,
		bool &isEmpty,
		bool &isComplete,
		int64_t &blockNo) {

	in >> taskId;
	in >> inputId;
	StatementHandler::decodeBooleanData(in, isEmpty);
	StatementHandler::decodeBooleanData(in, isComplete);
	
	if (blockNo != -1) {
		in >> blockNo;
	}
}

void SQLJobHandler::encodeRequestInfo(
		EventByteOutStream &out,
		JobId &jobId,
		ControlType controlType) {

	out << static_cast<int32_t>(
			SQLService::SQL_MSG_VERSION);
	out << std::pair<const uint8_t*, size_t>(
			jobId.clientId_.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
	out << jobId.clientId_.sessionId_;

	out << jobId.execId_;
	out << jobId.versionId_;

	const uint8_t tmp = static_cast<uint8_t>(controlType);
	out << tmp;
}

void SQLJobHandler::encodeTaskInfo(
		EventByteOutStream &out,
		TaskId taskId,
		TaskId inputId,
		bool isEmpty, bool isComplete) {

	out << taskId;
	out << inputId;
	
	StatementHandler::encodeBooleanData(out, isEmpty);
	StatementHandler::encodeBooleanData(out, isComplete);
}

void SQLJobHandler::encodeErrorInfo(EventByteOutStream &out,
		std::exception &e, StatementExecStatus status) {
	const util::Exception checkException = GS_EXCEPTION_CONVERT(e, "");
	out << status;
	StatementHandler::encodeException(out, e, 0);
}

