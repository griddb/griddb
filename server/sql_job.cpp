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
#include "sql_job.h"
#include "sql_processor.h"
#include "resource_set.h"
#include "sql_service.h"
#include "sql_allocator_manager.h"
#include "sql_execution.h"
#include "sql_execution_manager.h"
#include "sql_processor_ddl.h"
#include "sql_processor_result.h"
#include "cluster_manager.h"

UTIL_TRACER_DECLARE(SQL_SERVICE);
UTIL_TRACER_DECLARE(DISTRIBUTED_FRAMEWORK);
UTIL_TRACER_DECLARE(DISTRIBUTED_FRAMEWORK_DETAIL);

Job::Job(SQLJobRequestInfo &info) :
		resourceSet_(&info.resourceSet_),
		jobManager_(resourceSet_->getJobManager()),
		globalVarAlloc_(
				*info.resourceSet_.getSQLService()
						->getAllocatorManager()->getGlobalAllocator()),
		jobStackAlloc_(*assignStackAllocator()),
		jobId_(info.jobId_),
		scope_(NULL),
		noCondition_(globalVarAlloc_),
		nodeList_(globalVarAlloc_), 
		taskList_(globalVarAlloc_),
		coordinator_(info.jobInfo_->coordinator_),
		isSQL_(info.jobInfo_->isSQL_),
		coordinatorNodeId_(0),
		cancelled_(false),
		status_(FW_JOB_INIT),
		queryTimeout_(info.jobInfo_->queryTimeout_),
		expiredTime_(info.jobInfo_->expiredTime_),
		startTime_(info.jobInfo_->startTime_),
		startMonotonicTime_(info.jobInfo_->startMonotonicTime_),
		isSetJobTime_(info.jobInfo_->isSetJobTime_),
		selfTaskNum_(0),
		storeMemoryAgingSwapRate_(info.jobInfo_->storeMemoryAgingSwapRate_),
		timezone_(info.jobInfo_->timezone_),
		deployCompletedCount_(0),
		executeCompletedCount_(0),
		taskCompletedCount_(0),
		resultTaskId_(info.jobInfo_->resultTaskId_),
		connectedPId_(info.jobInfo_->pId_),
		syncContext_(info.jobInfo_->syncContext_),
		baseTaskId_(UNDEF_TASKID),
		limitedAssignNoList_(globalVarAlloc_),
		limitAssignNum_(info.jobInfo_->limitAssignNum_),
		currentAssignNo_(0),
		profilerInfos_(jobStackAlloc_),
		isExplainAnalyze_(info.jobInfo_->isExplainAnalyze_),
		dispatchNodeIdList_(globalVarAlloc_),
		dispatchServiceList_(globalVarAlloc_)
{

	setLimitedAssignNoList();
	GsNodeInfo *nodeInfo = NULL;
	NodeId nodeId;

	for (size_t pos = 0;
			pos < info.jobInfo_->gsNodeInfoList_.size(); pos++) {

		nodeInfo = info.jobInfo_->gsNodeInfoList_[pos];
		if (coordinator_) {
			nodeId = nodeInfo->nodeId_;
		}
		else {
			nodeId = resolveNodeId(nodeInfo);
		}
		nodeList_.push_back(nodeId);
	}

	scope_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
			util::StackAllocator::Scope(jobStackAlloc_);
}

Job::~Job() {

	for (size_t pos = 0; pos < taskList_.size(); pos++) {
		Task *task = taskList_[pos];
		if (task != NULL) {
			ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, task);
			task = NULL;
		}
	}

	for (size_t pos = 0;
			pos < noCondition_.size(); pos++) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, noCondition_[pos]);
	}
	noCondition_.clear();


	util::Vector<TaskProfilerInfo*>
			&profilerList  = profilerInfos_.profilerList_;

	for (size_t pos = 0;
			pos < profilerList.size(); pos++) {
		ALLOC_DELETE(jobStackAlloc_, profilerList[pos]);
	}

	ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, scope_);
	releaseStackAllocator(&jobStackAlloc_);
}

Task *Job::createTask(
		EventContext &ec,
		TaskId taskId,
		JobInfo *jobInfo,
		TaskInfo *taskInfo) {

	Task *task = NULL;

	try {

		task = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
				Task (this, taskInfo);

		if (!isDQLProcessor(taskInfo->sqlType_)) {
			task->cxt_->setEventContext(&ec);
		}

		if (task->isDml_) {
			if (jobInfo->containerType_ == TIME_SERIES_CONTAINER) {
				task->cxt_->setDMLTimeSeries();
			}
		}

		if (taskInfo->sqlType_ == SQLType::EXEC_DDL) {
			baseTaskId_ = task->taskId_;
		}

		task->createProcessor(
				ec.getAllocator(), jobInfo, taskId, taskInfo);
		
		if (task->isImmediated()) {
			noCondition_.push_back(
					ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
							Dispatch(
									task->taskId_,
									task->nodeId_,
									0,
									task->getServiceType(),
									task->loadBalance_));
		}

		selfTaskNum_++;
		return task;
	}
	catch (std::exception &e) {
		ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, task);
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

Task *Job::getTask(TaskId taskId) {

	util::LockGuard<util::Mutex> guard(mutex_);
	if (taskId < 0
			|| taskId >= static_cast<TaskId>(taskList_.size())) {
		return NULL;
	}
	return taskList_[taskId];
}

bool Job::checkStart() {

	int32_t currentCount = ++deployCompletedCount_;
	if (currentCount == static_cast<int32_t>(nodeList_.size())) {	
		return true;
	}
	else {
		return false;
	}
}

void Job::encodeProcessor(
		util::StackAllocator &alloc,
		JobInfo *jobInfo) {

	util::XArrayOutStream<> &out = jobInfo->outStream_;
	util::ByteStream< util::XArrayOutStream<> > currentOut(out);
	SQLProcessor::Factory factory;

	SQLVarSizeAllocator varAlloc(
			util::AllocatorInfo(ALLOCATOR_GROUP_SQL_WORK,
			"TaskVarAllocator"));

	if (jobInfo->taskInfoList_.size() != taskList_.size()) {
		GS_THROW_USER_ERROR(
				GS_ERROR_JOB_INTERNAL,
				"Job internal error, invalid jobInfo, expect="
				<< jobInfo->taskInfoList_.size() 
				<< ", actual=" << taskList_.size());
	}

	for (size_t pos = 0;
			pos < jobInfo->taskInfoList_.size(); pos++) {

		assert(jobInfo->taskInfoList_[pos]);
		if (!isDQLProcessor(
				jobInfo->taskInfoList_[pos]->sqlType_)) {
			continue;
		}

		const size_t startPos = currentOut.base().position();
		currentOut << static_cast<uint32_t>(0);
		Task *task = taskList_[pos];
		
		if (task != NULL) {
			assert(task->processor_);
			SQLProcessor::makeCoder(util::ObjectCoder(),
					*task->cxt_, NULL, NULL).encode(
							currentOut, task->processor_);
		}
		else {

			TaskOption &option = jobInfo->option_;
			ResourceSet *resourceSet = NULL;
			SQLContext cxt(
					resourceSet,
					&globalVarAlloc_,
					NULL,
					&alloc,
					&varAlloc,
					*resourceSet_->getTempolaryStore(),
					resourceSet_->getSQLExecutionManager()->getProcessorConfig());

			option.planNodeId_ = static_cast<uint32_t>(pos);
			
			SQLProcessor *processor = factory.create(
					cxt,
					option,
					jobInfo->taskInfoList_[pos]->inputTypeList_);

			SQLProcessor::makeCoder(
					util::ObjectCoder(), cxt, NULL, NULL).encode(
							currentOut, processor);

			factory.destroy(processor);
		}
		const size_t endPos = currentOut.base().position();
		uint32_t bodySize = static_cast<uint32_t>(
				endPos - startPos - sizeof(uint32_t));
		currentOut.base().position(startPos);
		currentOut << static_cast<uint32_t>(bodySize);
		currentOut.base().position(endPos);
	}

	encodeOptional(currentOut, jobInfo);

	if (!jobInfo->timezone_.isEmpty()) {
		currentOut << jobInfo->timezone_.getOffsetMillis();
	}
}

#define THROW_CANCEL(sourceErrorCode, sourceErrorMessage, location) \
try { \
		GS_THROW_USER_ERROR(sourceErrorCode, sourceErrorMessage); \
} \
catch (std::exception &e) { \
	GS_RETHROW_USER_ERROR_CODED( \
			GS_ERROR_JOB_CANCELLED, e, \
			GS_EXCEPTION_MERGE_MESSAGE(e, "Cancel job") \
			<< ", location=" << location <<", jobId=" << jobId_);  \
}

#define THROW_CANCEL_BY_EXCEPTION(sourceException, location) \
GS_RETHROW_USER_ERROR_CODED( \
		GS_ERROR_JOB_CANCELLED, sourceException, \
		GS_EXCEPTION_MERGE_MESSAGE(sourceException, "Cancel job") \
		<< ", location=" << location << ", jobId=" << jobId_);  

void Job::checkCancel(
		const char *str, 
		bool partitionCheck,
		bool nodeCheck) {
	
	if (cancelled_) {
		THROW_CANCEL(
				GS_ERROR_JOB_REQUEST_CANCEL, "Called cancel", str);
	}

	SQLService *sqlService = resourceSet_->getSQLService();
	if (expiredTime_ != INT64_MAX) {
		int64_t current = sqlService->getEE()->getMonotonicTime();
		if (current >= expiredTime_) {
			THROW_CANCEL(GS_ERROR_JOB_TIMEOUT,
					"Expired query timeout=" << queryTimeout_ 
					<< "ms, startTime=" << getTimeStr(startTime_), str);
		}
	}

	try {
		resourceSet_->getSQLService()->checkActiveStatus();
	} 
	catch (std::exception &e) {
		THROW_CANCEL_BY_EXCEPTION(e, str);
	}

	if (nodeCheck) {
		PartitionTable *pt = resourceSet_->getPartitionTable();

		for (size_t pos = 0; pos < nodeList_.size(); pos++) {
			if (pt->getHeartbeatTimeout(nodeList_[pos]) == UNDEF_TTL) {
				THROW_CANCEL(
						GS_ERROR_JOB_NODE_FAILURE,
						"Node failure occured, node="
						<< pt->dumpNodeAddress(nodeList_[pos]), str);
			}
		}
	}

	if (partitionCheck) {
		try {
			checkAsyncPartitionStatus();
		}
		catch (std::exception &e) {
			THROW_CANCEL_BY_EXCEPTION(e, str);
		}
	}
	
	if (status_ == FW_JOB_INIT || status_ == FW_JOB_DEPLOY) {
		int64_t current = sqlService->getEE()->getMonotonicTime();
		if (current - startMonotonicTime_
				> JobManager::DEFAULT_RESOURCE_CHECK_TIME) {
			THROW_CANCEL(GS_ERROR_JOB_TIMEOUT,
					"Called cancel", str);
		}
	}
}

void Job::cancel(
		EventContext &ec, bool clientCancel) {

	setCancel();
	
	if (isCoordinator()) {

		for (size_t pos = 0; pos < nodeList_.size(); pos++) {
			NodeId targetNodeId = nodeList_[pos];
			if (pos == 0 && !clientCancel) {
				jobManager_->remove(jobId_);
			}
			else {
				sendJobEvent(
						ec,
						targetNodeId,
						FW_CONTROL_CANCEL,
						NULL,
						NULL,
						-1,
						0,
						connectedPId_,
						SQL_SERVICE,
						false,
						false,
						static_cast<TupleList::Block *>(NULL));
			}
		}
	}
	else {
		try {
			if (clientCancel) {
				checkCancel("client", false);
			}
			else {
				checkCancel("internal", false);
			}
		}
		catch (std::exception &e) {
			handleError(ec, &e);
			jobManager_->remove(jobId_);
		}
	}
}
void Job::handleError(
		EventContext &ec, std::exception *e) {

	try {
		const util::Exception
				checkException = GS_EXCEPTION_CONVERT(*e, "");
		int32_t errorCode = checkException.getErrorCode();

		if (errorCode == GS_ERROR_JOB_CANCELLED) {
			UTIL_TRACE_EXCEPTION_INFO(SQL_SERVICE, *e, "");
		}
		else {
			UTIL_TRACE_EXCEPTION_WARNING(SQL_SERVICE, *e, "");
		}

		int32_t sendNode = 0;
		if (!isCoordinator()) {
			sendNode = coordinatorNodeId_;
		}
		
		sendJobEvent(
				ec,
				sendNode,
				FW_CONTROL_ERROR,
				NULL,
				e,
				-1,
				0,
				connectedPId_,
				SQL_SERVICE,
				false,
				false,
				static_cast<TupleList::Block *>(NULL));
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void Job::fetch(EventContext &ec, SQLExecution *execution) {

	checkCancel("fetch", false);
		
	Task *resultTask = taskList_[resultTaskId_];
	if (resultTask == NULL) {
		return;
	}
	
	SQLFetchContext fetchContext;
	getFetchContext(fetchContext);
	
	if (execution->fetch(
			ec,
			fetchContext,
			execution->getContext().getExecId(), 
			jobId_.versionId_)) {

		jobManager_->remove(jobId_);

		if (!execution->getContext().isPreparedStatement()) {
			jobManager_->removeExecution(
					&ec, jobId_, execution, true);
		}
	}
	else if (isCancel()) {
		try {
			GS_THROW_USER_ERROR(GS_ERROR_JOB_CANCELLED, "");
		}
		catch (std::exception &e) {
			jobManager_->executeStatementError(
					ec, &e, jobId_);
		}
	}
}

bool Job::processNextEvent(
		Task *task,
		TupleList::Block *block,
		int64_t blockNo,
		size_t inputId,
		ControlType controlType,
		bool isEmpty,
		bool isLocal) {

	bool isCompleted = (controlType == FW_CONTROL_FINISH);
	bool isExistsBlock = false;
	bool beforeAvailable = false;
	task->blockReaderList_[inputId]->isExists(
			isExistsBlock,beforeAvailable);

	if (block && (!isEmpty && !isCompleted)) {
		task->tempTupleList_[inputId]->append(*block);
		if (!isLocal) {
			if ((task->recvBlockCounterList_[inputId] + 1)  != blockNo) {
				GS_THROW_USER_ERROR(
						GS_ERROR_JOB_INVALID_RECV_BLOCK_NO,
						"Received block number is not a continuous, expected="
						<< (task->recvBlockCounterList_[inputId] + 1)
						<< ", current=" << blockNo);
			}
			task->recvBlockCounterList_[inputId] ++;
		}
		bool afterExistsBlock=false;
		bool afterAvailable=false;
		task->blockReaderList_[inputId]->isExists(
				afterExistsBlock, afterAvailable);
		if (!isExistsBlock || (!beforeAvailable && afterAvailable)) {
		}
		else {
			return false;
		}
	}
	else {
		if (controlType == FW_CONTROL_FINISH) {
			if (!isExistsBlock) {
				task->inputStatusList_[inputId] 
						= TASK_INPUT_STATUS_FINISHED;
			}
			else {
				task->inputStatusList_[inputId]
						= TASK_INPUT_STATUS_PREPARE_FINISH;
				return false;
			}
		}
		else {
		}
	}
	return true;
}

void Job::recieveTaskBlock(
		EventContext &ec,
		TaskId taskId,
		InputId inputId,
		TupleList::Block *block,
		int64_t blockNo,
		ControlType controlType,
		bool isEmpty,
		bool isLocal) {

	util::LockGuard<util::Mutex> guard(mutex_);
	Task *task = taskList_[taskId];
	if (task == NULL) {
		return;
	}

	if (!processNextEvent(
			task,
			block,
			blockNo,
			inputId,
			controlType,
			isEmpty,
			isLocal)) {
		return;
	}

	PartitionId loadBalance = task->loadBalance_;
	ServiceType serviceType = task->getServiceType();

	TRACE_ADD_EVENT(
			"recieve", jobId_, taskId, inputId,
			controlType, loadBalance);

	Event request(ec, EXECUTE_JOB, loadBalance);
	EventByteOutStream out = request.getOutStream();
	
	SQLJobHandler::encodeRequestInfo(
			out, jobId_, controlType);
	
	bool isCompleted = (controlType == FW_CONTROL_FINISH);
	SQLJobHandler::encodeTaskInfo(
			out, taskId, inputId, isEmpty, isCompleted);
	
	jobManager_->addEvent(request, serviceType);
}

bool Job::getTaskBlock(
		TaskId taskId,
		InputId inputId,
		Task *&task,
		TupleList::Block &block,
		bool &isExistBlock) {

	task = NULL;
	util::LockGuard<util::Mutex> guard(mutex_);
	if (taskList_[taskId] == NULL) {
		return false;
	}

	task = taskList_[taskId];
	isExistBlock = task->blockReaderList_[inputId]->isExists();
	return task->getBlock(inputId, block);
}

void Job::getProfiler(
		util::StackAllocator &alloc,
		StatJobProfilerInfo &jobProf,
		int32_t mode) {

	util::LockGuard<util::Mutex> guard(mutex_);

	for (size_t pos = 0; pos < taskList_.size(); pos++) {
	
		Task *task = taskList_[pos];
		if (taskList_[pos] != NULL) {
			
			if (mode == 1 ||
					(mode != 1 && task->status_ != TASK_EXEC_IDLE)) {

				StatTaskProfilerInfo taskProf(alloc);
				taskProf.id_ = task->taskId_;
				taskProf.inputId_ = task->inputNo_;
				taskProf.status_
						= getTaskExecStatusName(task->status_);
				taskProf.name_ = getProcessorName(task->sqlType_);
				taskProf.startTime_ = getTimeStr(task->startTime_).c_str();
				jobProf.taskProfs_.push_back(taskProf);
			}
		}
	}
}

void Job::removeTask(TaskId taskId) {
	util::LockGuard<util::Mutex> guard(mutex_);


	ALLOC_VAR_SIZE_DELETE(
			globalVarAlloc_, taskList_[taskId]);
	taskList_[taskId] = NULL;
}

void Job::deploy(
		EventContext &ec,
		JobInfo *jobInfo,
		NodeId senderNodeId,
		int64_t waitInterval) {

	status_ = Job::FW_JOB_DEPLOY;
	util::LockGuard<util::Mutex> guard(mutex_);
	
	NodeId targetNodeId;
	Task *task = NULL;
	
	if (senderNodeId != SELF_NODEID) {
		coordinatorNodeId_ = senderNodeId;
		taskCompletedCount_++;
	}

	for (size_t pos = 0;
			pos < jobInfo->taskInfoList_.size(); pos++) {

		TaskInfo *taskInfo = jobInfo->taskInfoList_[pos];
		if (taskInfo->nodePos_ == UNDEF_NODEID
				|| taskInfo->nodePos_
						>= static_cast<int32_t>(nodeList_.size())) {

			GS_THROW_USER_ERROR(
					GS_ERROR_JOB_RESOLVE_NODE_FAILED,
					"Resolve node failed, jobId = "
					<< jobId_ << ", taskPos = " << pos);
		}

		taskInfo->nodeId_ = nodeList_[taskInfo->nodePos_];
	}

	for (size_t pos = 0;
			pos < jobInfo->taskInfoList_.size(); pos++) {

		TaskInfo *taskInfo = jobInfo->taskInfoList_[pos];
		targetNodeId = nodeList_[taskInfo->nodePos_];

		if ((pos & JobManager::UNIT_CHECK_COUNT) == 0) {
			checkCancel("deploy", false);
		}

		if (coordinator_ && isExplainAnalyze_) {
			dispatchNodeIdList_.push_back(targetNodeId);
			dispatchServiceList_.push_back(taskInfo->type_);
		}

		task = NULL;
		if (targetNodeId == SELF_NODEID
				|| taskInfo->sqlType_ == SQLType::EXEC_RESULT) {
			try {
				task = createTask(
						ec, static_cast<TaskId>(pos), jobInfo, taskInfo);
			}
			catch (std::exception &e) {
				GS_RETHROW_USER_OR_SYSTEM(e, "");
			}

			taskList_.push_back(task);
		}
		else {
			taskList_.push_back(static_cast<Task*>(NULL));
			if (coordinator_ && isExplainAnalyze_) {
				appendProfiler();
			}

			if (!coordinator_) {
				if (isDQLProcessor(
						jobInfo->taskInfoList_[pos]->sqlType_)) {

					uint32_t size = 0;
					(*jobInfo->inStream_) >> size;
					const size_t startPos
							= (*jobInfo->inStream_).base().position();
					(*jobInfo->inStream_).base().position(
							startPos + size);
				}
			}
		}
	}

	if (selfTaskNum_ > 0) {
		size_t outputId = 0;
		size_t taskSize = taskList_.size();
	
		for (size_t pos = 0;
				pos < jobInfo->taskInfoList_.size(); pos++) {

			TaskInfo *taskInfo = jobInfo->taskInfoList_[pos];
			
			for (InputId inputId = 0; inputId <
					static_cast<InputId>(
							taskInfo->inputList_.size()); inputId++) {

				TaskId inputTaskId = taskInfo->inputList_[inputId];
				if (taskSize <= static_cast<size_t>(inputTaskId)) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_INTERNAL, "");
				}

				Task *inputTask = taskList_[inputTaskId];
				if (inputTask == NULL) {
					continue;
				}

				if (inputTask->outputDispatchList_.size() <= outputId) {
					GS_THROW_USER_ERROR(GS_ERROR_SQL_INTERNAL, "");
				}

				inputTask->outputDispatchList_[outputId].push_back(
						ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
						Dispatch(
								taskInfo->taskId_,
								taskInfo->nodeId_,
								inputId,
								static_cast<ServiceType>(taskInfo->type_),
								taskInfo->loadBalance_));
			}
		}
	}

	if (isCoordinator()) {

		recvDeploySuccess(ec, waitInterval);
		sendJobInfo(ec, jobInfo);
	}
	else {
	
		decodeOptional(*jobInfo->inStream_);
		sendJobEvent(
				ec,
				coordinatorNodeId_,
				FW_CONTROL_DEPLOY_SUCCESS,
				NULL,
				NULL,
				-1,
				0,
				0,
				SQL_SERVICE,
				false,
				false,
				(TupleList::Block*)NULL);

		if (selfTaskNum_ == 1) {
			sendJobEvent(
					ec,
					coordinatorNodeId_,
					FW_CONTROL_EXECUTE_SUCCESS,
					NULL,
					NULL,
					connectedPId_,
					0,
					0,
					SQL_SERVICE,
					false,
					false,
					static_cast<TupleList::Block *>(NULL));

			jobManager_->remove(jobId_);
		}
	}
}

void Job::checkAsyncPartitionStatus() {

	if (isDDL()) {

		util::LockGuard<util::Mutex> guard(mutex_);
		if (taskList_[baseTaskId_]) {
			checkDDLPartitionStatus();
		}
	}
}

void Job::request(
		EventContext *ec,
		Event *ev,
		ControlType controlType,
		TaskId taskId,
		TaskId inputId,
		bool isEmpty,
		bool isComplete,
		TupleList::Block *block,
		EventMonotonicTime emTime,
		int64_t waitTime) {

	try {
	
		checkCancel("request", false);

		switch (controlType) {
			
			case FW_CONTROL_PIPE0: {
				executeJobStart(ec);
			}
			break;

			case FW_CONTROL_PIPE:
			case FW_CONTROL_PIPE_FINISH:
			case FW_CONTROL_FINISH:
			case FW_CONTROL_NEXT: {
	
				executeTask(
						ec,
						ev,
						controlType,
						taskId,
						inputId,
						isEmpty,
						isComplete,
						block,
						emTime);
			}
			break;
			default:
			break;
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void Job::executeJobStart(EventContext *ec) {

	try {
		
		for (size_t pos = 0;
				pos < noCondition_.size(); pos++) {

			sendJobEvent(
					*ec,
					0,
					FW_CONTROL_PIPE,
					NULL,
					NULL,
					noCondition_[pos]->taskId_,
					0,
					noCondition_[pos]->loadBalance_,
					static_cast<ServiceType>(
							noCondition_[pos]->type_),
					true,
					false,
					static_cast<TupleList::Block*>(NULL));
		}

		status_ = Job::FW_JOB_EXECUTE;
	}
	catch (std::exception &e) {
		resourceSet_->getJobManager()->rethrowJobException(
				ec->getAllocator(), e, jobId_, FW_CONTROL_DEPLOY);
	}
}
bool Job::pipeOrFinish(
		Task *task,
		TaskId inputId,
		ControlType controlType,
		TupleList::Block *block) {

	try {
		SQLContext &cxt = *task->cxt_;
		bool remaining;

		if (controlType == FW_CONTROL_PIPE) {
			TRACE_TASK_START(task, inputId, controlType);
			task->startProcessor(TASK_EXEC_PIPE);

			remaining = task->processor_->pipe(
					cxt, inputId, *block);
		}
		else if (controlType == FW_CONTROL_FINISH) {
			TRACE_TASK_START(task, inputId, controlType);
			task->startProcessor(TASK_EXEC_FINISH);
			
			remaining = task->processor_->finish(
					cxt, inputId);
		}
		else {
			return true;
		}

		task->endProcessor();
		TRACE_TASK_END(task, inputId, controlType);
		
		return remaining;
	}
	catch (std::exception &e) {
		task->endProcessor();
		TRACE_TASK_END(task, inputId, controlType);
		
		removeTask(task->taskId_);
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

bool Job::executeContinuous(
		EventContext &ec,
		Event &ev,
		Task *task,
		TaskId inputId,
		ControlType controlType,
		bool checkRemaining,
		bool isEmpty,
		bool isComplete,
		EventMonotonicTime emTime) {

	bool remaining = checkRemaining;
	SQLContext &cxt = *task->cxt_;

	TupleList::Block block;
	if (!task->isCompleted()
			&& (remaining ||
					(task->isImmediated()
							&& task->sqlType_ != SQLType::EXEC_DDL))) {

		bool pending = (controlType != FW_CONTROL_NEXT);

		do {
			pending = jobManager_->checkExecutableTask(
					ec,
					ev,
					emTime,
					JobManager::DEFAULT_TASK_INTERRUPTION_INTERVAL,
					pending);

			if (pending) {
				ControlType nextControlType;
				bool nextEmpty = (remaining || isEmpty);
				if (remaining && !task->isImmediated()) {
					nextControlType = FW_CONTROL_NEXT;
					nextEmpty = true;
				}
				else {
					nextControlType = FW_CONTROL_PIPE;
				}
			
				TRACE_NEXT_EVENT(task, inputId, nextControlType);
				sendJobEvent(
						ec,
						0,
						nextControlType,
						NULL,
						NULL,
						task->taskId_,
						inputId,
						task->loadBalance_,
						task->getServiceType(),
						nextEmpty,
						isComplete,
						static_cast<TupleList::Block*>(NULL));
				
				return false;
			}
			pending = true;
			
			try {
				TaskExecutionStatus status =  (remaining ?
						TASK_EXEC_NEXT :
						TASK_EXEC_PIPE);
				
				TRACE_TASK_START(task, inputId, controlType);
				task->startProcessor(status);
				
				const bool nextRemaining = (remaining ?
						task->processor_->next(cxt) :
						task->processor_->pipe(cxt, inputId, block));
						
				task->endProcessor();
				TRACE_TASK_END(task, inputId, controlType);
				
				if (remaining) {
					if (!nextRemaining) {
						break;
					}
					remaining = nextRemaining;
				}
			}
			catch (std::exception &e) {
				task->endProcessor();
				TRACE_TASK_END(task, inputId, controlType);
				
				removeTask(task->taskId_);
				GS_RETHROW_USER_OR_SYSTEM(e, "");
			}
		} while (!task->isCompleted());
	}
	return true;
}

void Job::doComplete(
		EventContext &ec,
		Task *task) {

	SQLContext &cxt = *task->cxt_;
	if (task->isCompleted() || cxt.isFetchComplete()) {
		if (task->sqlType_ == SQLType::EXEC_RESULT) {
			task->profilerInfo_->complete();
			setProfilerInfo(task);
		}
	}

	int32_t execTaskCount = ++taskCompletedCount_;
	if (execTaskCount == selfTaskNum_ || cxt.isFetchComplete()) {
	
		if (isCoordinator()) {

			int32_t execJobCount = ++executeCompletedCount_;
			if (isExplainAnalyze_) {

				if (execJobCount == static_cast<int32_t>(nodeList_.size())) {

					if (isImmediateReply(
							task->getServiceType(), ec.getWorkerId())) {
							jobManager_->executeStatementSuccess(
								ec, this, true, true);
					}
					else {
						sendJobEvent(
								ec,
								0,
								FW_CONTROL_EXECUTE_SUCCESS,
								NULL,
								NULL,
								connectedPId_,
								0,
								0,
								SQL_SERVICE,
								false,
								false,
								static_cast<TupleList::Block *>(NULL));
					}
				}
			}
			else {
				jobManager_->executeStatementSuccess(
						ec, this, false, false);
			}
		}
		else {
			sendJobEvent(
					ec,
					coordinatorNodeId_,
					FW_CONTROL_EXECUTE_SUCCESS,
					NULL,
					NULL,
					connectedPId_,
					0,
					0,
					SQL_SERVICE,
					false,
					false,
					static_cast<TupleList::Block*>(NULL));
			
			jobManager_->remove(jobId_);
		}
		status_ = Job::FW_JOB_COMPLETE;
	}
	else {
		removeTask(task->taskId_);
	}
}

void Job::setupTaskInfo(
		Task *task,
		EventContext &ec, 
		Event &ev, TaskId inputId) {

	task->inputNo_ = inputId;
	task->startTime_ = util::DateTime::now(false).getUnixTime();
	task->cxt_->setEventContext(&ec);
	task->cxt_->setEvent(&ev);
	task->cxt_->setStoreMemoryAgingSwapRate(
			storeMemoryAgingSwapRate_);
	task->cxt_->setTimeZone(timezone_);
}

void Job::executeTask(
		EventContext *ec,
		Event *ev,
		ControlType controlType,
		TaskId taskId,
		TaskId inputId,
		bool isEmpty,
		bool isComplete,
		TupleList::Block *block,
		EventMonotonicTime emTime) {

	const char *taskName = NULL;
	try {

		Task *task = getTask(taskId);
		if (task == NULL) {
			return;
		}

		setupTaskInfo(task, *ec, *ev, inputId);
		SQLContext &cxt = *task->cxt_;
		taskName = getProcessorName(task->sqlType_);
		if (task->completed_)  {
			return;
		}
		assert(task->processor_);
		
		bool remaining = pipeOrFinish(
				task, inputId, controlType, block);
		
		if (isComplete) {
			if (!task->completed_ && task->isDml_) {
				int32_t loadBalance = task->loadBalance_;
				
				sendJobEvent(
						*ec,
						0,
						FW_CONTROL_FINISH,
						NULL,
						NULL,
						task->taskId_,
						0,
						loadBalance,
						task->getServiceType(),
						isEmpty,
						true,
						static_cast<TupleList::Block*>(NULL));
				return;
			}
		}
		if (!executeContinuous(
				*ec,
				*ev,
				task,
				inputId,
				controlType,
				remaining,
				isEmpty,
				isComplete, emTime)) {
			return;
		}

		if (controlType == FW_CONTROL_NEXT) {
			if (task->isCompleted()) {
				controlType = FW_CONTROL_FINISH;
			}
			else {
				controlType = FW_CONTROL_PIPE;
			}
		}

		if (task->isCompleted() || cxt.isFetchComplete()) {
			doComplete(*ec, task);
		}
		else {
			checkNextEvent(
					*ec, controlType, taskId, inputId);
		}
	}
	catch (std::exception &e) {

		resourceSet_->getJobManager()->rethrowJobException(
				ec->getAllocator(), e, jobId_,
				controlType, taskId, taskName);
	}
}

void Job::setProfilerInfo(Task *task) {

	if (isExplainAnalyze()) {
		task->profilerInfo_->complete();

		util::LockGuard<util::Mutex> guard(mutex_);
		task->profilerInfo_->setCustomProfile(
				task->processor_);
	}
}

void Job::decodeOptional(EventByteInStream &in) {

	if (in.base().remaining() > 0) {
		in >> storeMemoryAgingSwapRate_;
	}

	if (in.base().remaining() > 0) {
		int64_t offset;
		in >> offset;
		timezone_.setOffsetMillis(offset);
	}
}

bool Job::isImmediateReply(
		ServiceType serviceType, uint32_t workerId) {

	return (
		serviceType == SQL_SERVICE &&
				resourceSet_->getSQLService()->getPartitionGroupId(
						connectedPId_) == static_cast<int32_t>(workerId));
}

NodeId Job::resolveNodeId(
		JobInfo::GsNodeInfo *nodeInfo) {

	util::SocketAddress socketAddress(
			nodeInfo->address_.c_str(),
			nodeInfo->port_);

	const NodeDescriptor &nd
			= jobManager_->getDefaultEE()
					->getServerND(socketAddress);
	
	if (nd.isEmpty()) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_COMPILE_INVALID_NODE_ASSIGN,
				"Resolve node failed, jobId="
				<< jobId_ << ", address=" << socketAddress);
	}
	return static_cast<NodeId>(nd.getId());
}

void Job::setLimitedAssignNoList() {

	if (limitAssignNum_
			> resourceSet_->getSQLService()->getConcurrency()) {
		limitAssignNum_
				= resourceSet_->getSQLService()->getConcurrency();
	}

	for (int32_t pos = 0; pos < limitAssignNum_; pos++) {
		limitedAssignNoList_.push_back(
				jobManager_->getAssignPId());
	}
}

util::StackAllocator *Job::assignStackAllocator() {
	return resourceSet_->getSQLExecutionManager()
			->getStackAllocator();
}

void Job::releaseStackAllocator(
		util::StackAllocator *alloc) {
	
	if (alloc) {
		 resourceSet_->getSQLExecutionManager()->
					releaseStackAllocator(alloc);
	}
}

void Job::releaseLocalVarAllocator(
		SQLVarSizeAllocator *varAlloc) {
	
	if (varAlloc) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, varAlloc);
	}
}

SQLVarSizeAllocator *Job::assignLocalVarAllocator() {

	return ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
			SQLVarSizeAllocator(
					util::AllocatorInfo(ALLOCATOR_GROUP_SQL_WORK,
					"TaskVarAllocator"));
}

void Job::encodeOptional(
		util::ByteStream<util::XArrayOutStream<> > &out,
		JobInfo *jobInfo) {
	out << jobInfo->storeMemoryAgingSwapRate_;
}

void Job::checkDDLPartitionStatus() {

	DDLProcessor *ddlProcessor =
			reinterpret_cast<DDLProcessor*>(
					taskList_[baseTaskId_]->processor_);
	
	if (ddlProcessor) {
		ddlProcessor->checkPartitionStatus();
	}
}

void Job::ackDDLStatus(int32_t pos) {

	DDLProcessor *ddlProcessor =
		reinterpret_cast<DDLProcessor*>(
				taskList_[baseTaskId_]->processor_);

	if (ddlProcessor) {
		ddlProcessor->setAckStatus(NULL, pos, ACK_STATUS_OFF);
	}
}

void Job::getFetchContext(SQLFetchContext &cxt) {

	Task *resultTask = taskList_[resultTaskId_];
	ResultProcessor *resultProcessor =
			static_cast<ResultProcessor*>(resultTask->processor_);
	
	cxt.reader_ = resultProcessor->getReader();
	
	cxt.columnList_ = resultProcessor->getColumnInfoList();
	cxt.columnSize_ = resultProcessor->getColumnSize();
	cxt.isCompleted_ = resultTask->isResultCompleted();
}

AssignNo Job::getAssignPId() {

	if (limitAssignNum_ == 0) {
		return jobManager_->getAssignPId();
	}
	else {
		return limitedAssignNoList_[
				currentAssignNo_++ % limitAssignNum_];
	}
}

void Job::checkNextEvent(
		EventContext &ec,
		ControlType controlType,
		TaskId taskId,
		TaskId inputId) {

	util::LockGuard<util::Mutex> guard(mutex_);
	if (taskList_[taskId] == NULL) {
		return;
	}

	Task *task = taskList_[taskId];
	if (task->blockReaderList_.size()
			<= static_cast<size_t>(inputId)) {
		return;
	}

	ControlType nextControlType = controlType;
	bool isEmpty = false;
	bool isComplete = false;

	bool isExistsBlock = task->blockReaderList_[inputId]->isExists();
	bool isPreparedFinished =
			(task->inputStatusList_[inputId]
					== TASK_INPUT_STATUS_PREPARE_FINISH);
	
	if (controlType == FW_CONTROL_NEXT) {
		return;
	}
	
	if (!isExistsBlock) {
		if (isPreparedFinished) {
			nextControlType = FW_CONTROL_FINISH;
			isComplete = true;
			task->inputStatusList_[inputId]
					= TASK_INPUT_STATUS_FINISHED;
		}
		else {
			return;
		}
	}
	else {
	}

	PartitionId loadBalance = task->loadBalance_;
	ServiceType serviceType = task->getServiceType();
	
	TRACE_ADD_EVENT(
			"next", jobId_, taskId,
			inputId, controlType, loadBalance);

	Event request(
			ec, EXECUTE_JOB, loadBalance);
	EventByteOutStream out = request.getOutStream();
	
	SQLJobHandler::encodeRequestInfo(
			out, jobId_, nextControlType);
	SQLJobHandler::encodeTaskInfo(
			out, taskId, inputId, isEmpty, isComplete);
	
	jobManager_->addEvent(request, serviceType);
}

void Job::sendJobInfo(EventContext &ec,
		JobInfo *jobInfo) {

	util::StackAllocator &alloc = ec.getAllocator();

	if (jobInfo->gsNodeInfoList_.size() > 1) {
		encodeProcessor(alloc, jobInfo);
	
		for (size_t pos = 0;
				pos < jobInfo->gsNodeInfoList_.size(); pos++) {
			
			GsNodeInfo *nodeInfo = jobInfo->gsNodeInfoList_[pos];
			if (pos == 0) {
				continue;
			}
			else {
				NodeId nodeId = resolveNodeId(nodeInfo);
				util::Random random(jobInfo->startTime_);
				sendJobEvent(
						ec,
						nodeId,
						FW_CONTROL_DEPLOY,
						jobInfo,
						NULL,
						-1,
						0,
						random.nextInt32(DataStore::MAX_PARTITION_NUM),
						SQL_SERVICE,
						false,
						false,
						static_cast<TupleList::Block*>(NULL));
			}
		}
	}
}

void Job::recvDeploySuccess(
		EventContext &ec, int64_t waitInterval) {

	try {
		EventMonotonicTime emTime = -1;
		
		if (checkStart()) {
			for (size_t pos = 0; pos < nodeList_.size(); pos++) {

				if (nodeList_[pos] == SELF_NODEID) {
					request(
							&ec,
							NULL,
							FW_CONTROL_PIPE0,
							-1,
							0,
							false,
							false,
							static_cast<TupleList::Block *>(NULL),
							emTime,
							waitInterval);
				}
				else {
					sendJobEvent(
							ec,
							nodeList_[pos],
							FW_CONTROL_PIPE0,
							NULL,
							NULL,
							-1,
							0,
							0,
							SQL_SERVICE,
							false,
							false,
							static_cast<TupleList::Block *>(NULL));
				}
			}
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void Job::recvExecuteSuccess(
		EventContext &ec, EventByteInStream &in, bool isPending) {

	try {
		if (!isCoordinator()) {
			return;
		}

		util::StackAllocator &alloc = ec.getAllocator();
		JobProfilerInfo profs(alloc);
		
		if (isExplainAnalyze_) {
			util::ObjectCoder::withAllocator(alloc).decode(in, profs);
		}
		
		int32_t execJobCount = 0;
		if (!isPending) {
			setProfiler(profs, true);
			execJobCount = ++executeCompletedCount_;
		}
		else {
			execJobCount = executeCompletedCount_;
		}
		
		if (execJobCount == static_cast<int32_t>(
				nodeList_.size()) || isPending) {

			jobManager_->executeStatementSuccess(
					ec, this, isExplainAnalyze_, true);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}
		
void Job::fowardRequest(
		EventContext &ec,
		ControlType controlType,
		Task *task,
		int32_t outputId,
		bool isEmpty,
		bool isComplete,
		TupleList::Block *block) {

	try {

		for (size_t pos = 0;
				pos < task->outputDispatchList_[outputId].size(); pos++) {

			Dispatch *dispatch
					= task->outputDispatchList_[outputId][pos];

			sendBlock(
					ec,
					dispatch->nodeId_,
					controlType,
					dispatch->taskId_, 
					dispatch->inputId_,
					dispatch->loadBalance_,
					isEmpty,
					isComplete,
					block,
					task->sendBlockCounter_);
		}
		task->sendBlockCounter_++;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void Job::sendBlock(
		EventContext &ec,
		NodeId nodeId,
		ControlType controlType,
		TaskId taskId,
		InputId inputId,
		AssignNo loadBalance,
		bool isEmpty,
		bool isComplete,
		TupleList::Block *block,
		int64_t blockNo) {

	JobId &jobId = jobId_;

	try {
		if (nodeId == SELF_NODEID) {
			recieveTaskBlock(
					ec,
					taskId,
					inputId,
					block,
					0,
					controlType,
					isEmpty,
					true);
		}
		else {
			Event request(
					ec, DISPATCH_JOB, connectedPId_);
			EventByteOutStream out = request.getOutStream();
			ControlType curentControlType = controlType;

			if (controlType == FW_CONTROL_CANCEL) {
				curentControlType = FW_CONTROL_ERROR;
			}

			SQLJobHandler::encodeRequestInfo(
					out, jobId, curentControlType);
			SQLJobHandler::encodeTaskInfo(
					out, taskId, inputId, isEmpty, isComplete);

			out << blockNo;
			out << loadBalance;
			if (controlType == FW_CONTROL_PIPE) {
				block->encode(out);
			}
			jobManager_->sendEvent(
					request, SQL_SERVICE, nodeId);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void Job::recieveNoSQLResponse(	
		EventContext &ec,
		util::Exception &dest,
		StatementExecStatus status,
		int32_t pos) {

	util::LockGuard<util::Mutex> guard(mutex_);
	if (baseTaskId_ == UNDEF_TASKID
			|| taskList_.size()
					<= static_cast<size_t>(baseTaskId_)) {
		GS_THROW_USER_ERROR(GS_ERROR_JOB_INTERNAL, "");
	}

	if (taskList_[baseTaskId_]) {
		ackDDLStatus(pos);
	}

	if (status != StatementHandler::TXN_STATEMENT_SUCCESS) {
		bool isContinue = false;
		try {
			throw dest;
		}
		catch (std::exception &e) {
			const util::Exception checkException
				= GS_EXCEPTION_CONVERT(e, "");
			int32_t errorCode = checkException.getErrorCode();

			if (errorCode == GS_ERROR_DS_CONTAINER_UNEXPECTEDLY_REMOVED
				|| errorCode == GS_ERROR_TXN_CONTAINER_NOT_FOUND
				|| errorCode == GS_ERROR_DS_DS_CONTAINER_EXPIRED) {
				isContinue = true;
		}
		if (!isContinue) {
			handleError(ec, &e);
		}
	}
	if (!isContinue)
		return;
	}

	Event execEv(ec, EXECUTE_JOB, connectedPId_);
	EventByteOutStream out = execEv.getOutStream();
	ControlType controlType = FW_CONTROL_PIPE;
	
	SQLJobHandler::encodeRequestInfo(
			out, jobId_, controlType);
	
	int32_t inputId = 0;
	bool isEmpty = true;
	bool isComplete = false;
	SQLJobHandler::encodeTaskInfo(
			out, baseTaskId_, inputId, isEmpty, isComplete);

	jobManager_->addEvent(execEv, SQL_SERVICE);
}

void Job::setupContextTask(SQLContext *cxt) {

	cxt->setClientId(&jobId_.clientId_);
	cxt->setExecId(jobId_.execId_);
	cxt->setVersionId(jobId_.versionId_);
	cxt->setStoreMemoryAgingSwapRate(
			storeMemoryAgingSwapRate_);
	cxt->setSyncContext(syncContext_);
	cxt->setTimeZone(timezone_);
	
	if (isSetJobTime_) {
		cxt->setJobStartTime(startTime_);
	}
}

TaskInterruptionHandler::TaskInterruptionHandler() :
		cxt_(NULL),
		intervalMillis_(
				JobManager::DEFAULT_TASK_INTERRUPTION_INTERVAL) {
}

TaskInterruptionHandler::~TaskInterruptionHandler() {
}

bool TaskInterruptionHandler::operator()(
		InterruptionChecker &checker, int32_t flags) {

	UNUSED_VARIABLE(checker);
	UNUSED_VARIABLE(flags);

	EventContext *ec = cxt_->getEventContext();
	Task *task = cxt_->getTask();
	Job *job = task->getJob();
	
	assert(ec != NULL && task != NULL && job != NULL);

	job->checkCancel("processor", false);
	return ec->getEngine().getMonotonicTime() -
			ec->getHandlerStartMonotonicTime() > intervalMillis_;
}

Task::Task(Job *job, TaskInfo *taskInfo) :
		job_(job),
		jobStackAlloc_(job->getAllocator()),
		processorStackAlloc_(NULL),
		processorVarAlloc_(NULL),
		globalVarAlloc_(job->getGlobalAllocator()), 
		outputDispatchList_(
				taskInfo->outputList_.size(),
				DispatchList(globalVarAlloc_),
				globalVarAlloc_),
		completed_(false),
		resultComplete_(false),
		immediated_(false),
		counter_(0),
		status_(TASK_EXEC_IDLE),
		sendBlockCounter_(0),
		tempTupleList_(globalVarAlloc_),
		blockReaderList_(globalVarAlloc_),
		recvBlockCounterList_(globalVarAlloc_),
		inputStatusList_(globalVarAlloc_),
		inputNo_(0),
		startTime_(0),
		taskId_(taskInfo->taskId_),
		nodeId_(taskInfo->nodeId_),
		cxt_(NULL),
		type_(taskInfo->type_),
		sqlType_(taskInfo->sqlType_),
		loadBalance_(taskInfo->loadBalance_),
		isDml_(taskInfo->isDml_),
		processor_(NULL),
		group_(*job->getResourceSet()->getTempolaryStore()),
		profilerInfo_(NULL),
		scope_(NULL) {

	if (taskInfo->inputList_.size() == 0) {
		immediated_ = true;
	}
	inputStatusList_.assign(taskInfo->inputList_.size(), 0);

	for (size_t pos = 0;
			pos < taskInfo->inputTypeList_.size(); pos++) {

		TupleList::Info info;
		info.columnCount_ = taskInfo->inputTypeList_[pos].size();
		info.columnTypeList_ = &taskInfo->inputTypeList_[pos][0];
		
		TupleList *tupleList
				= ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
						TupleList (group_, info);
		tempTupleList_.push_back(tupleList);
		
		TupleList::BlockReader *reader
				= ALLOC_VAR_SIZE_NEW(globalVarAlloc_) 
						TupleList::BlockReader(*tupleList);
		reader->setOnce();
		blockReaderList_.push_back(reader);
	}

	recvBlockCounterList_.assign(
			taskInfo->inputList_.size(), -1);

	if (loadBalance_ == -1) {
		loadBalance_ = job_->getAssignPId();
	}
	taskInfo->loadBalance_ = loadBalance_;
	
	processorStackAlloc_ = job_->assignStackAllocator();
	processorVarAlloc_ = job_->assignLocalVarAllocator();

	scope_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_)
			util::StackAllocator::Scope(*processorStackAlloc_);
	
	cxt_ = ALLOC_VAR_SIZE_NEW(globalVarAlloc_) SQLContext(
			job->getResourceSet(),
			&globalVarAlloc_,
			NULL,
			processorStackAlloc_,
			processorVarAlloc_,
			*job->getResourceSet()->getTempolaryStore(),
			job->getResourceSet()->getSQLExecutionManager()->getProcessorConfig());

	job->setupContextTask(cxt_);
	cxt_->setTask(this);
	setupProfiler(taskInfo);


	interruptionHandler_.cxt_ = cxt_;
	cxt_->setInterruptionHandler(&interruptionHandler_);
}

Task::~Task() {

	SQLProcessor::Factory factory;
	factory.destroy(processor_);

	for (size_t i = 0; i < blockReaderList_.size(); i++) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, blockReaderList_[i]);
	}

	for (size_t i = 0; i < tempTupleList_.size(); i++) {
		ALLOC_VAR_SIZE_DELETE(
				globalVarAlloc_, tempTupleList_[i]);
	}

	tempTupleList_.clear();
	blockReaderList_.clear();

	for (size_t i = 0; i < outputDispatchList_.size(); i++) {
		for (size_t j = 0; j < outputDispatchList_[i].size(); j++) {
			ALLOC_VAR_SIZE_DELETE(
					globalVarAlloc_, outputDispatchList_[i][j]);
		}
	}

	outputDispatchList_.clear();
	ALLOC_VAR_SIZE_DELETE(globalVarAlloc_ , cxt_);
	ALLOC_VAR_SIZE_DELETE(globalVarAlloc_, scope_);
	
	job_->releaseStackAllocator(processorStackAlloc_);
	job_->releaseLocalVarAllocator(processorVarAlloc_);

}

void Task::createProcessor(
		util::StackAllocator &alloc,
		JobInfo *jobInfo,
		TaskId taskId,
		TaskInfo *taskInfo) {

	SQLProcessor::Factory factory;
	if (jobInfo->option_.plan_) {
		
		jobInfo->option_.planNodeId_
				= static_cast<uint32_t>(taskId);

		processor_ = factory.create(
				*cxt_,
				jobInfo->option_,
				taskInfo->inputTypeList_);
	}
	else {
		if (Job::isDQLProcessor(sqlType_)) {
			
			uint32_t size = 0;
			*jobInfo->inStream_ >> size;
			
			SQLProcessor::makeCoder(
					util::ObjectCoder().withAllocator(jobStackAlloc_),
					*cxt_,
					&factory,
					&taskInfo->inputTypeList_).decode(
							*jobInfo->inStream_, processor_);
		}
	}

	if (job_->isExplainAnalyze() && processor_ != NULL) {
	
		SQLProcessor::Profiler profiler(alloc);
		profiler.setForAnalysis(true);

		processor_->setProfiler(profiler);
	}	
}

void Task::setupProfiler(TaskInfo *taskInfo) {

	profilerInfo_ = job_->appendProfiler();
	const ResourceSet *resourceSet = job_->getResourceSet();
	SQLService *sqlService
			= resourceSet->getSQLService();
	TransactionService *txnService
			= resourceSet->getTransactionService();
	
	if (job_->isExplainAnalyze()) {

		int32_t workerId;
		if (type_ == static_cast<uint8_t>(SQL_SERVICE)) {
			workerId = sqlService->getPartitionGroupId(loadBalance_);
		}
		else {
			workerId = txnService->getPartitionGroupId(loadBalance_)
					+ sqlService->getConcurrency();
		}

		profilerInfo_->init(
				jobStackAlloc_,
				taskId_,
				static_cast<int32_t>(
						taskInfo->inputList_.size()),
						workerId,
						true);
	}
}

void Task::appendBlock(
		InputId inputId, TupleList::Block &block) {

	if (tempTupleList_.size() <= static_cast<size_t>(inputId)) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT, "");
	}
	tempTupleList_[inputId]->append(block);
}

bool Task::getBlock(
		InputId inputId, TupleList::Block &block) {

	if (blockReaderList_.size() <= static_cast<size_t>(inputId)) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_PROC_INTERNAL_INVALID_INPUT, "");
	}

	bool retFlag = blockReaderList_[inputId]->next(block);

	if (retFlag) {
		profilerInfo_->incInputCount(
				inputId, TupleList::tupleCount(block));
	}

	return retFlag;
}

std::string Task::dump() {

	util::NormalOStringStream strstrm;
	strstrm << Job::getProcessorName(sqlType_) << ","
			<< taskId_ << "," << counter_
			<<  "," << Job::getTaskExecStatusName(status_);
	return strstrm.str().c_str();
}

void JobInfo::setup(
		int64_t expiredTime,
		int64_t elapsedTime,
		int64_t currentClockTime,
		bool isSetJobTime,
		bool isCoordinator) {

	startTime_ = currentClockTime;
	startMonotonicTime_ = elapsedTime;
	isSetJobTime_ = isSetJobTime;
	queryTimeout_ = expiredTime;
	coordinator_ = isCoordinator;
	
	if (expiredTime ==  INT64_MAX) {
		expiredTime_ = expiredTime;
	}
	else {
		if (elapsedTime >= (INT64_MAX - expiredTime
				- JobManager::JOB_TIMEOUT_SLACK_TIME)) {
			expiredTime_ = INT64_MAX;
		}
		else {
			expiredTime_ = elapsedTime + expiredTime
					+ JobManager::JOB_TIMEOUT_SLACK_TIME;
		}
	}
}

void Job::sendJobEvent(
		EventContext &ec,
		NodeId nodeId,
		ControlType controlType,
		JobInfo *jobInfo,
		std::exception *e,
		TaskId taskId,
		InputId inputId,
		AssignNo loadBalance,
		ServiceType serviceType,
		bool isEmpty,
		bool isComplete,
		TupleList::Block *block) {

	JobId &jobId = jobId_;

	try {
		PartitionId pId = loadBalance;
		EventType eventType
				= jobManager_->getEventType(controlType);

		if (controlType == FW_CONTROL_CANCEL && nodeId != 0) {
			eventType = CONTROL_JOB;
		}

		Event request(ec, eventType, pId);
		EventByteOutStream out = request.getOutStream();
		
		SQLJobHandler::encodeRequestInfo(out, jobId, controlType);
		switch (controlType) {
		case FW_CONTROL_DEPLOY: {
			util::ObjectCoder().encode(out, jobInfo);
			out << std::pair<const uint8_t *, size_t>(
					jobInfo->outBuffer_.data(),
					jobInfo->outBuffer_.size());
		}
		break;
		case FW_CONTROL_PIPE:
		case FW_CONTROL_FINISH:
		case FW_CONTROL_PIPE_FINISH:
		case FW_CONTROL_NEXT: {
			SQLJobHandler::encodeTaskInfo(
					out,
					taskId,
					inputId,
					isEmpty,
					isComplete);
			if (block != NULL) {
				if (isEmpty || isComplete) {
				}
				else {
					block->encode(out);
				}
			}
		}
		break;
		case FW_CONTROL_ERROR: {
			if (e == NULL) {
				GS_THROW_USER_ERROR(
						GS_ERROR_JOB_INVALID_CONTROL_INFO, "");
			}

			StatementHandler::StatementExecStatus status
					= StatementHandler::TXN_STATEMENT_ERROR;
			
			const util::Exception checkException
					= GS_EXCEPTION_CONVERT(*e, "");
			int32_t errorCode = checkException.getErrorCode();
			
			if (errorCode == GS_ERROR_TXN_PARTITION_ROLE_UNMATCH
				|| errorCode == GS_ERROR_TXN_PARTITION_STATE_UNMATCH
				|| errorCode == GS_ERROR_TXN_CLUSTER_ROLE_UNMATCH) {
				status = StatementHandler::TXN_STATEMENT_DENY;
			}

			bool isActive = resourceSet_->getClusterService()
					->getManager()->isActive();
			
			if (errorCode == GS_ERROR_JOB_CANCELLED && !isActive) {

				if (!isCoordinator()) {

					Event cancelRequest(
							ec, CONTROL_JOB, IMMEDIATE_PARTITION_ID);
					EventByteOutStream out = cancelRequest.getOutStream();
					ControlType controlType = FW_CONTROL_CANCEL;

					SQLJobHandler::encodeRequestInfo(out, jobId, controlType);
					jobManager_->sendEvent(
							cancelRequest, SQL_SERVICE, coordinatorNodeId_);

					jobManager_->remove(jobId);

					return;
				}
			}
			if (errorCode == GS_ERROR_JOB_CANCELLED && !isActive) {
				try {
					GS_THROW_USER_ERROR(
							GS_ERROR_TXN_CLUSTER_ROLE_UNMATCH,
							GS_EXCEPTION_MESSAGE(*e));
				}
				catch (std::exception &e2) {
					out << status;
					StatementHandler::encodeException(out, e2, 0);
				}
			}
			else {
				out << status;
				StatementHandler::encodeException(out, *e, 0);
			}
		}
		break;
		case FW_CONTROL_CANCEL:
				setCancel();
		break;
		case FW_CONTROL_EXECUTE_SUCCESS: {
			encodeProfiler(out);
		}
		break;
		case FW_CONTROL_DEPLOY_SUCCESS:
		case FW_CONTROL_PIPE0:
		case FW_CONTROL_HEARTBEAT:
		case FW_CONTROL_CLOSE: {
		}
		break;
		default:
			GS_THROW_USER_ERROR(
					GS_ERROR_JOB_INVALID_CONTROL_TYPE, "");
		}

		jobManager_->sendEvent(
				request, serviceType, nodeId);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void Job::getProfiler(
		util::StackAllocator &alloc,
		TaskProfiler &profile,
		size_t planId) {

	util::LockGuard<util::Mutex> guard(mutex_);

	profile.copy(
			alloc,
			profilerInfos_.profilerList_[planId]->profiler_);

	profile.address_
			= ALLOC_NEW(alloc) util::String(
					resourceSet_->getPartitionTable()->dumpNodeAddress(
							dispatchNodeIdList_[planId],
							static_cast<ServiceType>(
									dispatchServiceList_[planId])).c_str(),
					alloc);
}

void Job::setProfiler(
		JobProfilerInfo &profs, bool isRemote) {

	util::LockGuard<util::Mutex> guard(mutex_);
	for (size_t pos = 0;
			pos < profs.profilerList_.size(); pos++) {
		
		TaskId taskId = profs.profilerList_[pos]->taskId_;
		if (isRemote && resultTaskId_ == taskId) {
		}
		else {
			profilerInfos_.profilerList_[taskId]->profiler_.copy(
					jobStackAlloc_,
					profs.profilerList_[pos]->profiler_);
		}
	}
}

void Job::encodeProfiler(EventByteOutStream &out) {

	util::ObjectCoder().encode(out, profilerInfos_);
}

TaskProfilerInfo *Job::appendProfiler() {

	TaskProfilerInfo *profilerInfo
			= ALLOC_NEW(jobStackAlloc_) TaskProfilerInfo;

	profilerInfos_.profilerList_.push_back(profilerInfo);
	return profilerInfo;
}

void Job::cancel(const Event::Source &source) {
	
	setCancel();
	JobId &jobId = jobId_;
	
	if (isCoordinator()) {
		for (size_t pos = 0; pos < nodeList_.size(); pos++) {
	
			NodeId targetNodeId = nodeList_[pos];
			ControlType controlType = FW_CONTROL_CANCEL;
			EventType eventType = EXECUTE_JOB;

			Event request(source, eventType, connectedPId_);
			EventByteOutStream out = request.getOutStream();
			SQLJobHandler::encodeRequestInfo(out, jobId, controlType);

			jobManager_->sendEvent(
					request, SQL_SERVICE, targetNodeId);
		}
	}
}

const char *Job::getControlName(ControlType type) {

	switch (type) {
		case FW_CONTROL_DEPLOY: return "DEPLOY";
		case FW_CONTROL_DEPLOY_SUCCESS: return "DEPLOY_SUCCESS";
		case FW_CONTROL_PIPE0: return "PIPE0";
		case FW_CONTROL_PIPE: return "PIPE";
		case FW_CONTROL_FINISH: return "FINISH";
		case FW_CONTROL_NEXT: return "NEXT";
		case FW_CONTROL_EXECUTE_SUCCESS: return "EXECUTE_SUCCESS";
		case FW_CONTROL_CANCEL: return "CANCEL";
		case FW_CONTROL_CLOSE: return "CLOSE";
		default: return "UNDEF";
	}
}

const char *Job::getTaskExecStatusName(
		TaskExecutionStatus type) {

	switch (type) {
		case TASK_EXEC_PIPE: return "PIPE";
		case TASK_EXEC_FINISH: return "FINISH";
		case TASK_EXEC_NEXT: return "NEXT";
		case TASK_EXEC_IDLE: return "IDLE";
		default: return "UNDEF";
	}
}

const char *Job::getProcessorName(SQLType::Id type) {
	return SQLType::Coder()(type, "UNDEF");
}
