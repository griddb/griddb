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
#ifndef SQL_JOB_UTILS_H_
#define SQL_JOB_UTILS_H_

#define DUMP_SEND_CONTROL(s) \
	GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK_DETAIL, 0, s);

#define TRACE_TASK_START(task, inputId, controlType) \
	GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK_DETAIL, \
			GS_TRACE_SQL_INTERNAL_DEBUG, \
			" " << task->job_->jobId_ << " " << \
			" " << task->job_->jobManager_->getHostName() << " " << \
			" TASK " << task->taskId_ << " START IN " << inputId << " " \
			<< JobManager::getProcessorName(task->sqlType_) \
			<< " " << JobManager::getControlName(controlType));

#define TRACE_TASK_END(task, inputId, controlType) \
	GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK_DETAIL, \
			GS_TRACE_SQL_INTERNAL_DEBUG, \
			" " << task->job_->jobId_ << " " << \
			" " << task->job_->jobManager_->getHostName() << " " << \
			" TASK " << task->taskId_ << " END IN " << inputId << " " \
			<< JobManager::getProcessorName(task->sqlType_) \
			<< " " << JobManager::getControlName(controlType));

#define TRACE_ADD_EVENT(str, jobId, taskId, inputId, controlType, pId) \
	GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK_DETAIL, \
			GS_TRACE_SQL_INTERNAL_DEBUG, \
			" " << str << \
			" " << jobId << " " << \
			" " << jobManager_->getHostName() << " " << \
			" TASK " << taskId << " EVENT IN " << inputId << " "  << \
			" " << JobManager::getControlName(controlType));

#define TRACE_ADD_EVENT2(str, jobId, taskId, inputId, controlType, count) \
	GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK_DETAIL, \
			GS_TRACE_SQL_INTERNAL_DEBUG, \
			" " << str << \
			" " << jobId << " " << \
			" " << jobManager_->getHostName() << " " << \
			" TASK " << taskId << " EVENT IN " << inputId << " "  << \
			" " << JobManager::getControlName(controlType) << " " << count);


#define TRACE_NEXT_EVENT(task, inputId, controlType) \
	GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK_DETAIL, \
			GS_TRACE_SQL_INTERNAL_DEBUG, \
			"CONT" << " " << task->job_->jobId_ << " " << \
			" " << task->job_->jobManager_->getHostName() << " " << \
			" TASK " << task->taskId_ << " EVENT IN " << inputId << " " \
			<< JobManager::getProcessorName(task->sqlType_) \
			<< " " << JobManager::getControlName(controlType));

#define TRACE_TASK_COMPLETE(task, outputId) \
	GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK_DETAIL, \
			GS_TRACE_SQL_INTERNAL_DEBUG, \
			" " << task->job_->getJobId() << " " << \
			" " << task->job_->jobManager_->getHostName() << " " << \
			" TASK " << task->taskId_ << " COMPLETE OUT " << outputId << " " \
			<< JobManager::getProcessorName(task->sqlType_));


#define TRACE_JOB_CONTROL_START(jobId, hostName, controlName) \
	GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK_DETAIL, \
			GS_TRACE_SQL_INTERNAL_DEBUG, \
			" " << jobId << " " << \
			" " << hostName << " " << \
			" TASK " << 0 << " START IN " << 0 << " " \
			<< controlName \
			<< " " << "PIPE");

#define TRACE_CONTINUOUS_BLOCK(jobId, taskId, inputId, controlType) \
	GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK_DETAIL, \
			GS_TRACE_SQL_INTERNAL_DEBUG, \
			" " << jobId << " " << \
			" " << jobManager_->getHostName() << " " << \
			" TASK " << taskId << " EMPTY BLOCK IN " << inputId << " " \
			<< JobManager::getProcessorName(task->sqlType_) \
			<< " " << JobManager::getControlName(controlType));

#define TRACE_JOB_CONTROL_END(jobId, hostName, controlName) \
	GS_TRACE_INFO(DISTRIBUTED_FRAMEWORK_DETAIL, \
			GS_TRACE_SQL_INTERNAL_DEBUG, \
			" " << jobId << " " << \
			" " << hostName << " " << \
			" TASK " << 0 << " END IN " << 0 << " " \
			<< controlName \
			<< " " << "PIPE");

#endif
