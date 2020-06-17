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
#include "sql_task_context.h"
#include "sql_job.h"
#include "resource_set.h"
#include "sql_service.h"

TaskContext::TaskContext(const ResourceSet *resourceSet) :
		resourceSet_(resourceSet),
		alloc_(NULL),
		globalVarAlloc_(NULL),
		task_(NULL),
		eventContext_(NULL),
		counter_(0),
		syncContext_(NULL) {
}

int64_t TaskContext::getMonotonicTime() {

	resourceSet_->getSQLService()->checkActiveStatus();
	return resourceSet_->getSQLService()
			->getEE()->getMonotonicTime();
}

NoSQLSyncContext* TaskContext::getSyncContext() const {
	return syncContext_;
}

void TaskContext::setSyncContext(
		NoSQLSyncContext *syncContext) {
	syncContext_ = syncContext;
}

util::StackAllocator& TaskContext::getAllocator() {
	return *alloc_;
}

void TaskContext::setAllocator(
		util::StackAllocator *alloc) {
	alloc_ = alloc;
}

EventContext* TaskContext::getEventContext() const {
	return eventContext_;
}

void TaskContext::setEventContext(
		EventContext *eventContext) {
	eventContext_ = eventContext;
}

Task* TaskContext::getTask() const {
	return task_;
}

void TaskContext::setTask(Task *task) {
	task_ = task;
}

const ResourceSet *TaskContext::getResourceSet() {
	return resourceSet_;
}

bool TaskContext::isTransactionService() {
	return (task_->getServiceType() == TRANSACTION_SERVICE);
}

void TaskContext::setGlobalAllocator(
		SQLVariableSizeGlobalAllocator *globalVarAlloc) {
	globalVarAlloc_ = globalVarAlloc;
}

SQLVariableSizeGlobalAllocator *TaskContext::getGlobalAllocator() {
	return globalVarAlloc_;
}

void TaskContext::checkCancelRequest() {
	task_->getJob()->checkCancel("transfer", false);
}

void TaskContext::transfer(TupleList::Block &block) {

	assert(eventContext_);
	checkCancelRequest();
	task_->getJob()->fowardRequest(*eventContext_,
			FW_CONTROL_PIPE,
			task_, 0, false, false,
			static_cast<TupleList::Block *>(&block));
}

void TaskContext::transfer(TupleList::Block &block,
		bool completed, int32_t outputId) {

	assert(eventContext_);
	checkCancelRequest();
	task_->getJob()->fowardRequest(*eventContext_,
			FW_CONTROL_PIPE,
			task_, outputId,
			false, completed,
			static_cast<TupleList::Block *>(&block));

	task_->setCompleted(completed);
}

void TaskContext::finish() {

	checkCancelRequest();
	finish(0);
}

void TaskContext::finish(int32_t outputId) {

	task_->getJob()->fowardRequest(
			*eventContext_,
			FW_CONTROL_FINISH,
			task_,
			outputId,
			true,
			true,
			static_cast<TupleList::Block *>(NULL));

	task_->setCompleted(true);;
	task_->getJob()->setProfilerInfo(task_);
}

TaskOption::TaskOption(util::StackAllocator &alloc) :
		plan_(NULL),
		planNodeId_(0),
		byteInStream_(NULL),
		inStream_(NULL),
		baseStream_(NULL),
		jsonValue_(NULL) {

	UNUSED_VARIABLE(alloc);
}