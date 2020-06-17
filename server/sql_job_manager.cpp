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
#include "sql_job_manager.h"
#include "sql_execution.h"
#include "sql_compiler.h"
#include "data_store.h"
#include "sql_processor.h"
#include "sql_service.h"
#include "uuid_utils.h"

#include "sql_processor_ddl.h"
#include "sql_processor_result.h"
#include "sql_resource_manager.h"
#include "sql_execution_manager.h"
#include "resource_set.h"
#include "sql_allocator_manager.h"
#include "sql_job.h"
#include "sql_utils.h"



UTIL_TRACER_DECLARE(SQL_SERVICE);
UTIL_TRACER_DECLARE(DISTRIBUTED_FRAMEWORK);
UTIL_TRACER_DECLARE(DISTRIBUTED_FRAMEWORK_DETAIL);

JobManager::JobManager(
		SQLVariableSizeGlobalAllocator &globalVarAlloc,
		LocalTempStore &store,
		const ConfigTable &config) :
				globalVarAlloc_(globalVarAlloc),
				resourceSet_(NULL),
				currentCnt_(0), 
				resourceManager_(NULL),
				txnConcurrency_(
						config.get<int32_t>(CONFIG_TABLE_DS_CONCURRENCY)),
				sqlConcurrency_(
						config.get<int32_t>(CONFIG_TABLE_SQL_CONCURRENCY)),
				txnPgConfig_(
						config.get<int32_t>(CONFIG_TABLE_DS_PARTITION_NUM),
						txnConcurrency_),
				sqlPgConfig_(
						DataStore::MAX_PARTITION_NUM, sqlConcurrency_),
				hostPort_(0) {

	try {

		sqlConcurrency_
				= config.get<int32_t>(CONFIG_TABLE_SQL_CONCURRENCY);
		sqlStartPIdList_.assign(sqlConcurrency_, 0);

		for (int32_t pgId = 0; pgId < sqlConcurrency_; pgId++) {
			sqlStartPIdList_[pgId]
					= sqlPgConfig_.getGroupBeginPartitionId(pgId);
		}
		resourceManager_ = UTIL_NEW
				SQLJobResourceManager(globalVarAlloc_);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

JobManager::~JobManager() {
	delete resourceManager_;
}

void JobManager::initialize(
		const ResourceSet &resourceSet) {

	resourceSet_ = &resourceSet;
		
	if (resourceSet.clsSvc_
			&& resourceSet.clsSvc_->getEE()) {
		eeList_.push_back(resourceSet.clsSvc_->getEE());
	}

	if (resourceSet.txnSvc_
			&& resourceSet.txnSvc_->getEE()) {
		eeList_.push_back(resourceSet.txnSvc_->getEE());
	}

	if (resourceSet.syncSvc_
			&& resourceSet.syncSvc_->getEE()) {
		eeList_.push_back(resourceSet.syncSvc_->getEE());
	}

	if (resourceSet.sysSvc_
			&& resourceSet.sysSvc_->getEE()) {
		eeList_.push_back(resourceSet.sysSvc_->getEE());
	}

	if (resourceSet.sqlSvc_
			&& resourceSet.sqlSvc_->getEE()) {
		eeList_.push_back(resourceSet.sqlSvc_->getEE());
	}

	getHostInfo();
}

void JobManager::remove(JobId &jobId) {
	
	try {
		resourceManager_->remove(jobId);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void JobManager::beginJob(
		EventContext &ec,
		JobId &jobId,
		JobInfo *jobInfo,
		int64_t waitInterval) {

	try {

		SQLJobRequestInfo info(jobId, jobInfo, *resourceSet_);
		JobLatch latch(jobId, resourceManager_, &info);
		Job *job = latch.get();

		TRACE_JOB_CONTROL_START(jobId, getHostName(), "DEPLOY");

		job->deploy(ec, jobInfo, 0, waitInterval);

		TRACE_JOB_CONTROL_END(jobId, getHostName(), "DEPLOY");
	}
	catch (std::exception &e) {

		resourceSet_->getJobManager()->rethrowJobException(
				ec.getAllocator(), e, jobId,
				FW_CONTROL_DEPLOY);
	}
}

void JobManager::cancel(
		EventContext &ec, JobId &jobId,
		bool clientCancel) {

	try {
		
		JobLatch latch(jobId, resourceManager_, NULL);
		Job *job = latch.get();
		
		if (job == NULL) {
			GS_TRACE_INFO(SQL_SERVICE,
					GS_TRACE_SQL_EXECUTION_INFO,
					"CANCEL:jobId=" << jobId);
			return;
		}
		job->cancel(ec, clientCancel);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void JobManager::cancelAll(
		const Event::Source &eventSource,
		util::Vector<JobId> &jobIdList,
		CancelOption &cancelOption) {

	try {

		if (jobIdList.empty()) {
			getCurrentJobList(jobIdList);
		}

		for (size_t pos = 0;
				pos < jobIdList.size(); pos++) {
			cancel(
					eventSource, jobIdList[pos], cancelOption);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void JobManager::checkTimeout(EventContext &ec) {

	try {
		
		util::StackAllocator &alloc = ec.getAllocator();
		util::Vector<JobId> jobIdList(alloc);
		getCurrentJobList(jobIdList);
		
		for (util::Vector<JobId>::iterator
				it = jobIdList.begin();
				it != jobIdList.end(); it++) {

			checkAlive(ec, (*it));
		}
	}
	catch (std::exception &e) {
		UTIL_TRACE_EXCEPTION(SQL_SERVICE, e, "");
	}
}

CancelOption::CancelOption(util::String &startTimeStr) :
		startTime_(0) {

	if (startTimeStr.size() > 0) {
		startTime_ = SQLUtils::convertToTime(startTimeStr);
	}
}

void JobManager::getProfiler(
		util::StackAllocator &alloc,
		StatJobProfiler &jobProfiler,
		int32_t mode) {

	util::LockGuard<util::Mutex> guard(
			resourceManager_->getLock());
	int64_t jobCount = 0;

	for (JobMapItr it = resourceManager_->begin();
			it != resourceManager_->end();it++) {
	
		Job *job = (*it).second;
		if (job) {

			StatJobProfilerInfo jobProf(alloc, job->getJobId());
			jobProf.id_ = jobCount++;
			jobProf.startTime_
					= getTimeStr(job->getStartTime()).c_str();
			job->getProfiler(alloc, jobProf, mode);
			
			jobProfiler.jobProfs_.push_back(jobProf);
		}
	}
}

void JobManager::getJobIdList(
		util::StackAllocator &alloc,
		StatJobIdList &infoList) {

	util::LockGuard<util::Mutex> guard(
			resourceManager_->getLock());

	for (JobMapItr it = resourceManager_->begin();
			it != resourceManager_->end();it++) {

		Job *job = (*it).second;
		if (job) {

			StatJobIdListInfo info(alloc);
			info.startTime_
					= getTimeStr(job->getStartTime()).c_str();
			
			util::String uuidStr(alloc);
			job->getJobId().toString(alloc, uuidStr);
			info.jobId_ = uuidStr;
			
			infoList.infoList_.push_back(info);
		}
	}
}

void JobManager::getCurrentJobList(
		util::Vector<JobId> &jobIdList) {

	resourceManager_->getKeyList(jobIdList);
}

void JobManager::checkAlive(
		EventContext &ec,
		JobId &jobId) {

	Job *job = NULL;
	
	JobLatch latch(jobId, resourceManager_, NULL);
	job = latch.get();
	
	if (job == NULL) {
		GS_TRACE_DEBUG(SQL_SERVICE,
				GS_TRACE_SQL_EXECUTION_INFO, "jobId=" << jobId);
		return;
	}
	
	try {
		job->checkCancel("timer check", true, true);
	}
	catch (std::exception &e) {
		if (job) {
			job->setCancel();
			try {
				job->handleError(ec, &e);
			}
			catch (std::exception &) {
			}
			if (!job->isCoordinator()) {
				remove(jobId);
			}
		}
	}
}

void JobManager::cancel(
		const Event::Source &source,
		JobId &jobId,
		CancelOption &option) {

	try {
		
		JobLatch latch(jobId, resourceManager_, NULL);
		Job *job = latch.get();
		
		if (job == NULL) {
			GS_TRACE_DEBUG(SQL_SERVICE,
					GS_TRACE_SQL_EXECUTION_INFO,
					"CANCEL:jobId=" << jobId);
			return;
		}
		
		if (option.startTime_ > 0) {
			if (job->getStartTime() >= option.startTime_) {
				return;
			}
		}
		job->cancel(source);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void ExecuteJobHandler::executeDeploy(
		EventContext &ec,
		Event &ev,
		EventByteInStream &in,
		JobId &jobId,
		PartitionId &recvPartitionId) {

	JobManager *jobManager
			= resourceSet_->getJobManager();
	util::StackAllocator &alloc = ec.getAllocator();
	NodeId nodeId = 0;

	if (!ev.getSenderND().isEmpty()) {
		nodeId = static_cast<NodeId>(
				ev.getSenderND().getId());
	}
	if (nodeId == 0) {
		GS_THROW_USER_ERROR(
				GS_ERROR_JOB_MANAGER_INVALID_PROTOCOL,
				"Control deploy must be partitipant node");
	}
	try {
		JobInfo *jobInfo = ALLOC_NEW(alloc) JobInfo(alloc);
		util::ObjectCoder::withAllocator(alloc).decode(in, jobInfo);	
		
		jobInfo->setup(
				jobInfo->queryTimeout_,
				resourceSet_->getSQLService()->getEE()->getMonotonicTime(),
				util::DateTime::now(false).getUnixTime(),
				jobInfo->isSetJobTime_, false);

		jobInfo->inStream_ = &in;
		SQLJobRequestInfo info(jobId, jobInfo, *resourceSet_);

		JobLatch latch(
				jobId, jobManager->getResourceManager(), &info);

		Job *job = latch.get();
		if (job) {
			job->deploy(ec, jobInfo, nodeId, 0);
			recvPartitionId = job->getPartitionId();
		}
		else {
			GS_TRACE_DEBUG(SQL_SERVICE,
					GS_TRACE_SQL_EXECUTION_INFO,
					"ExecuteDeploy:jobId=" << jobId);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}


void JobManager::removeExecution(
		EventContext *ec, JobId &jobId,
		SQLExecution *execution, bool withCheck) {

	uint8_t versionId
			= execution->getContext().getCurrentJobVersionId();
	if (jobId.versionId_ == versionId) {
		resourceSet_->getSQLExecutionManager()->remove(
				*ec, jobId.clientId_, withCheck);
	}
}

void JobManager::getHostInfo() {

	PartitionTable *pt
			= resourceSet_->getPartitionTable();
	
	hostName_ = pt->getNodeAddress(
			SELF_NODEID, SQL_SERVICE).
					toString(true).c_str();
	hostAddress_ =  pt->getNodeAddress(
			SELF_NODEID, SQL_SERVICE).
					toString(false).c_str();
	hostPort_ =  pt->getNodeAddress(
			SELF_NODEID, SQL_SERVICE).port_;
};

bool JobManager::checkExecutableTask(
		EventContext &ec,
		Event &ev,
		int64_t startTime,
		int64_t limitInterval,
		bool pending) {

	if (!pending) {
		return false;
	}

	int64_t eventCount;
	EventEngine::Tool::getLiveStats(
			ec.getEngine(), ec.getWorkerId(),
			EventEngine::Stats::EVENT_ACTIVE_EXECUTABLE_COUNT,
			eventCount, &ec, &ev);

	if (eventCount > 0) {
		return true;
	}
	
	EventEngine::Tool::getLiveStats(
			ec.getEngine(), ec.getWorkerId(),
			EventEngine::Stats::EVENT_CYCLE_HANDLING_AFTER_COUNT,
			eventCount, &ec, &ev);
	
	if (eventCount > 0) {
		return true;
	}

	resourceSet_->getSQLService()->checkActiveStatus();
	
	const EventMonotonicTime currentTime =
			ec.getEngine().getMonotonicTime();
	if ((currentTime - startTime) > limitInterval) {
		return true;
	}
	
	return false;
}

void JobManager::sendEvent(Event &ev,
		ServiceType type, NodeId nodeId, int64_t waitInterval) {

	EventEngine *ee = getEE(type);
	const NodeDescriptor &nd = ee->getServerND(nodeId);
	
	if (waitInterval) {
		EventRequestOption sendOption;
		sendOption.timeoutMillis_
				= static_cast<int32_t>(waitInterval);
		
		ee->send(ev, nd, &sendOption);
	}
	else {
		ee->send(ev, nd);
	}
}

void JobManager::addEvent(Event &ev, ServiceType type) {

	EventEngine *ee = getEE(type);
	ee->add(ev);
}

void JobManager::executeStatementError(
		EventContext &ec,
		std::exception *e,
		JobId &jobId)  {

	util::StackAllocator &alloc = ec.getAllocator();
	ExecutionLatch latch(jobId.clientId_,
			resourceSet_->getSQLExecutionManager()
					->getResourceManager(), NULL);

	SQLExecution *execution = latch.get();
	if (execution) {

		RequestInfo request(alloc, false);
		execution->execute(
				ec,
				request,
				true,
				e,
				jobId.versionId_,
				&jobId);

		cancel(ec, jobId, false);
		remove(jobId);
		removeExecution(&ec, jobId, execution, true);
	}
	else {
		remove(jobId);
	}
}

void JobManager::executeStatementSuccess(
		EventContext &ec,
		Job *job,
		bool isExplainAnalyze,
		bool withResponse) {

	JobId &jobId = job->getJobId();

	ExecutionLatch latch(
			jobId.clientId_,
			resourceSet_->getSQLExecutionManager()
					->getResourceManager(),
			NULL);

	SQLExecution *execution = latch.get();
	if (execution) {

		if (isExplainAnalyze && withResponse) {
			
			SQLExecution::SQLReplyContext replyCxt;
			replyCxt.setExplainAnalyze(
					&ec, job, jobId.versionId_,&jobId);
			
			execution->replyClient(replyCxt);
		}

		cancel(ec, jobId, false);
		remove(jobId);
		removeExecution(
				&ec, jobId, execution, true);
	}
}

int32_t JobManager::getAssignPId() {

	int32_t current = ++currentCnt_;
	if (current >= sqlConcurrency_) {
		currentCnt_ = 0;
		return sqlStartPIdList_[0];
	}
	else {
		return sqlStartPIdList_[current];
	}
}

PartitionGroupId JobManager::getSQLPgId(
		PartitionId pId) {
	return sqlPgConfig_.getPartitionGroupId(pId);
}

EventType JobManager::getEventType(ControlType type) {
	switch (type) {
		case FW_CONTROL_PIPE:
		case FW_CONTROL_PIPE_FINISH:
		case FW_CONTROL_FINISH:
		case FW_CONTROL_NEXT:
		case FW_CONTROL_ERROR:
		case FW_CONTROL_CANCEL:
		case FW_CONTROL_CLOSE:
		case FW_CONTROL_EXECUTE_SUCCESS:
		case FW_CONTROL_DEPLOY:
			return EXECUTE_JOB;
		case FW_CONTROL_DEPLOY_SUCCESS:
		case FW_CONTROL_PIPE0:
		case FW_CONTROL_HEARTBEAT:
			return CONTROL_JOB;
		default:
			GS_THROW_USER_ERROR(
					GS_ERROR_JOB_MANAGER_INTERNAL, "");
	}
}

void JobManager::rethrowJobException(
		util::StackAllocator &alloc,
		std::exception &e,
		JobId &jobId,
		ControlType controlType,
		TaskId taskId,
		const char *taskName) {

	util::NormalOStringStream oss;
	oss << "Execute SQL failed (reason=";
	oss << GS_EXCEPTION_MESSAGE(e);
	oss << " (jobId=" << jobId
			<< ", operation=" << Job::getControlName(controlType);
	if (taskId != UNDEF_TASKID) {
		if (taskName == NULL) {
			taskName = "";
		}
		oss << ", taskld=" << taskId
				<< ", processor=" <<  taskName;
	}
	oss << ")";

	GS_RETHROW_USER_OR_SYSTEM(e, oss.str().c_str());
}