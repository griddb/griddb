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
#include "sql_job_common.h"
#include "sql_job_manager.h"
#include "partition_table.h"
#include "sql_compiler.h"
#include "uuid_utils.h"
#include "sql_processor.h"

bool JobId::operator==(const JobId &id) const {

	if (execId_ == id.execId_ 
			&& clientId_.sessionId_ == id.clientId_.sessionId_ 
			&& versionId_ == id.versionId_
			&& !memcmp(clientId_.uuid_,
					id.clientId_.uuid_, 
							TXN_CLIENT_UUID_BYTE_SIZE)) {
		return true;
	}
	else {
		return false;
	}
}

bool JobId::operator < (const JobId& id) const {

	if (execId_ < id.execId_) {
		return true;
	}
	else if (execId_ > id.execId_) {
		return false;
	}
	else {
		if (versionId_ < id.versionId_) {
			return true;
		}
		else if (versionId_ > id.versionId_) {
			return false;
		}
		else {
			if (clientId_.sessionId_ < id.clientId_.sessionId_) {
				return true;
			}
			else if (clientId_.sessionId_ > id.clientId_.sessionId_) {
				return false;
			}
			else {
				int32_t ret = memcmp(clientId_.uuid_,
						id.clientId_.uuid_, TXN_CLIENT_UUID_BYTE_SIZE);
				if (ret < 0) {
					return true;
				}
				else {
					return false;
				}
			}
		}
	}
}

void JobId::parse(
		util::StackAllocator &alloc,
		util::String &jobIdStr) {

	const char *separator=":";
	size_t pos1 = jobIdStr.find(separator);
	if (pos1 != 36) {
		GS_THROW_USER_ERROR(
				GS_ERROR_JOB_INVALID_JOBID_FORMAT,
				"Invalid format : " << jobIdStr
				<< ", expected uuid length=36, actual=" << pos1);
	}

	util::String uuidString(
			jobIdStr.c_str(), 36, alloc);

	if (UUIDUtils::parse(
			uuidString.c_str(), clientId_.uuid_) != 0) {

		GS_THROW_USER_ERROR(
				GS_ERROR_JOB_INVALID_JOBID_FORMAT,
				"Invalid format : "
				<< jobIdStr << ", invalid uuid format");
	}

	size_t pos2 = jobIdStr.find(separator, pos1 + 1);
	if (std::string::npos == pos2) {
		GS_THROW_USER_ERROR(
				GS_ERROR_JOB_INVALID_JOBID_FORMAT,
				"Invalid format : "
				<< jobIdStr << ", invalid jobId format");
	}

	util::String sessionIdString(
			jobIdStr, pos1 + 1,
			pos2 - pos1 - 1, alloc);

	clientId_.sessionId_
			= atoi(sessionIdString.c_str());
	
	size_t pos3 = jobIdStr.find(
			separator, pos2 + 1);
	if (std::string::npos == pos3) {
		GS_THROW_USER_ERROR(
				GS_ERROR_JOB_INVALID_JOBID_FORMAT,
				"Invalid format : " 
				<< jobIdStr << ", invalid jobId format");
	}

	util::String execIdString(
			jobIdStr, pos2 + 1,
			pos3 - pos2 - 1, alloc);
	execId_ = atoi(execIdString.c_str());

	size_t pos4 = jobIdStr.find(separator, pos3 + 1);
	if (std::string::npos != pos4) {
		GS_THROW_USER_ERROR(
				GS_ERROR_JOB_INVALID_JOBID_FORMAT,
				"Invalid format : "
				<< jobIdStr << ", invalid jobId format");
	}

	util::String versionIdString(
			jobIdStr, pos3 + 1,
			jobIdStr.size() - pos3 - 1, alloc);

	versionId_ = static_cast<uint8_t>(atoi(versionIdString.c_str()));
}

void JobId::toString(
		util::StackAllocator &alloc,
		util::String &str,
		bool isClientIdOnly) {

	util::NormalOStringStream strstrm;
	char tmpBuffer[UUID_STRING_SIZE];

	UUIDUtils::unparse(clientId_.uuid_, tmpBuffer);
	util::String tmpUUIDStr(tmpBuffer, 36, alloc);

	strstrm << tmpUUIDStr.c_str();
	strstrm << ":";
	strstrm << clientId_.sessionId_;

	if (!isClientIdOnly) {
		strstrm << ":";
		strstrm << execId_;
		strstrm << ":";
		strstrm << static_cast<int32_t>(versionId_);
	}
	str = strstrm.str().c_str();
}

std::string JobId::dump(
		util::StackAllocator &alloc,
		bool isClientIdOnly) {

	util::String jobIdStr(alloc);
	toString(alloc, jobIdStr, isClientIdOnly);

	return jobIdStr.c_str();
}

std::ostream& operator<<(std::ostream &stream,
		const JobId &id) {

	stream << id.clientId_;
	stream << ":";
	stream << id.execId_;
	stream << ":";
	stream << static_cast<int32_t>(id.versionId_);
	return stream;
}

JobInfo::JobInfo(util::StackAllocator &alloc) :
		alloc_(alloc),
		gsNodeInfoList_(alloc),
		coordinator_(true),
		queryTimeout_(INT64_MAX),
		expiredTime_(INT64_MAX),
		startTime_(0),
		startMonotonicTime_(INT64_MAX),
		isSetJobTime_(false),
		taskInfoList_(alloc),
		resultTaskId_(0), pId_(0),
		option_(alloc),
		outBuffer_(alloc),
		outStream_(outBuffer_),
		job_(NULL),
		inStream_(NULL),
		syncContext_(NULL),
		limitAssignNum_(0),
		isExplainAnalyze_(false),
		isRetry_(false),
		containerType_(COLLECTION_CONTAINER),
		isSQL_(true),
		storeMemoryAgingSwapRate_(
				TXN_UNSET_STORE_MEMORY_AGING_SWAP_RATE),
		timezone_(util::TimeZone()) {
}		
		
JobInfo::~JobInfo() {

	for (size_t pos = 0; pos < gsNodeInfoList_.size(); pos++) {
		ALLOC_DELETE(
				alloc_, gsNodeInfoList_[pos]);
	}

	for (size_t pos = 0; pos < taskInfoList_.size(); pos++) {
		ALLOC_DELETE(
				alloc_, taskInfoList_[pos]);
	}
}

TaskInfo *JobInfo::createTaskInfo(
		SQLPreparedPlan *pPlan,
		size_t pos) {

	TaskInfo *taskInfo = ALLOC_NEW(alloc_) TaskInfo(alloc_);

	taskInfo->setPlanInfo(pPlan, pos);
	taskInfo->taskId_ = static_cast<int32_t>(pos);
	taskInfoList_.push_back(taskInfo);
	
	return taskInfo;
}

GsNodeInfo *JobInfo::createAssignInfo(
		PartitionTable *pt,
		NodeId nodeId,
		bool isLocal,
		JobManager *jobManager) {
	
	GsNodeInfo *assignedInfo = ALLOC_NEW(alloc_) GsNodeInfo (alloc_);
	
	if (!isLocal) {
		if (nodeId == 0) {
			assignedInfo->address_
					= jobManager->getHostAddress().c_str();
		}
		else {
			assignedInfo->address_
					= pt->getNodeAddress(nodeId, SQL_SERVICE).
							toString(false).c_str();
		}
		assignedInfo->port_
				= pt->getNodeAddress(nodeId, SQL_SERVICE).port_;
	}

	assignedInfo->nodeId_ = nodeId;
	
	gsNodeInfoList_.push_back(assignedInfo);
	return assignedInfo;
}

TaskInfo::TaskInfo(util::StackAllocator &alloc) :
		alloc_(alloc),
		taskId_(0),
		loadBalance_(0),
		type_(static_cast<uint8_t>(SQL_SERVICE)),
		nodePos_(0),
		inputList_(alloc),
		outputList_(alloc),
		outColumnTypeList_(alloc),
		inputTypeList_(alloc),
		nodeId_(0), 
		sqlType_(SQLType::START_EXEC),
		isDml_(false) {
}
		
void TaskInfo::setSQLType(SQLType::Id sqlType) {
	
	switch (sqlType) {
		case SQLType::EXEC_INSERT:
		case SQLType::EXEC_UPDATE:
		case SQLType::EXEC_DELETE:
			isDml_ = true;
			break;
		default:
			break;
	}
	sqlType_ = sqlType;
}

void TaskInfo::setPlanInfo(
		SQLPreparedPlan *pPlan,
		size_t pos) {

	if (pos >= pPlan->nodeList_.size()) {
		GS_THROW_USER_ERROR(GS_ERROR_SQL_INTERNAL, "");
	}

	setSQLType(pPlan->nodeList_[pos].type_);
	inputList_ = pPlan->nodeList_[pos].inputList_;
}

void TaskProfilerInfo::init(
		util::StackAllocator &alloc,
		int32_t taskId,
		int32_t size,
		int32_t workerId,
		bool enableTimer) {

	taskId_ = taskId;
	profiler_.worker_ = workerId;
	profiler_.rows_ = ALLOC_NEW(alloc)
			util::Vector<int64_t>(size, 0, alloc);
	
	profiler_.customData_ = ALLOC_NEW(alloc) 
			util::XArray<uint8_t>(alloc);
	
	enableTimer_ = enableTimer;
	if (enableTimer) {
		watch_.reset();
	}
}

void TaskProfilerInfo::incInputCount(
		int32_t inputId,
		size_t size) {

	if (enableTimer_) {
		(*profiler_.rows_)[inputId] += size;
	}
}

void TaskProfilerInfo::start() {
	
	if (enableTimer_) {
		watch_.reset();
		watch_.start();
		if (profiler_.executionCount_ == 0) {
			startTime_ = watch_.currentClock();
		}
	}
}

void TaskProfilerInfo::end() {
	
	profiler_.executionCount_++;
	if (enableTimer_ && !completed_) {
		uint64_t actualTime = watch_.elapsedNanos();
		profiler_.actualTime_ += actualTime;
		watch_.reset();
		watch_.start();
	}
}

void TaskProfilerInfo::complete() {
	
	if (enableTimer_) {
		if (endTime_ == 0) {
			endTime_ = watch_.currentClock();
		}
	
		profiler_.leadTime_
				= endTime_ - startTime_;
		profiler_.leadTime_
				= profiler_.leadTime_  / 1000;
		profiler_.actualTime_ 
				= (profiler_.actualTime_ / 1000 / 1000);
		
		completed_ = true;
	}
}

void TaskProfilerInfo::setCustomProfile(
		SQLProcessor *processor) {

	if (processor != NULL) {
		const SQLProcessor::Profiler::StreamData *src =
				processor->getProfiler().getStreamData();

		if (src != NULL) {
			util::XArray<uint8_t> *dest = profiler_.customData_;
			dest->resize(src->size());
			memcpy(dest->data(), src->data(), src->size());
		}
	}
}

StatJobProfilerInfo::StatJobProfilerInfo(
		util::StackAllocator &alloc,
		JobId &targetJobId) : 
				id_(0),
				startTime_(alloc),
				jobId_(alloc),
				taskProfs_(alloc) {
	targetJobId.toString(alloc, jobId_);
}

