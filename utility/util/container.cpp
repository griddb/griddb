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
#include "util/container.h"

namespace util {


namespace detail {


void ConcurrentQueueUtils::errorQueueClosed(ErrorHandler customHandler) {
	try {
		UTIL_THROW_UTIL_ERROR(
				CODE_INVALID_STATUS, "Concurrent queue already closed");
	}
	catch (...) {
		std::exception e;
		if (customHandler != NULL) {
			customHandler(e);
		}
		throw;
	}
}


FuturePool::FuturePool(
		const AllocatorInfo &info, const StdAllocator<void, void> &alloc) :
		alloc_(alloc),
		condPool_(info) {
}

StdAllocator<void, void>& FuturePool::getAllocator() throw() {
	return alloc_;
}

ObjectPool<Condition, Mutex>& FuturePool::getConditionPool() throw() {
	return condPool_;
}


TaskBase::~TaskBase() {
}

TaskBase::TaskBase() {
}


void FutureBaseUtils::errorNotAssigned() {
	UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_OPERATION, "Feature not assigned");
}

void FutureBaseUtils::errorRetrieved() {
	UTIL_THROW_UTIL_ERROR(
			CODE_ILLEGAL_OPERATION, "Feature result already retrieved");
}

void FutureBaseUtils::errorCancelled() {
	UTIL_THROW_UTIL_ERROR(CODE_INVALID_STATUS, "Requested task cancelled");
}

void FutureBaseUtils::errorServiceShutdown(std::exception &e) {
	UTIL_RETHROW_UTIL_ERROR(
			CODE_INVALID_STATUS, e,
			"Requested task inacceptable because of service shutdown");
}

void FutureBaseUtils::raiseExceptionResult(const ExceptionInfo &ex) {
	if (ex.second) {
		PlatformException subEx;
		static_cast<Exception&>(subEx) = ex.first;
		throw subEx;
	}
	try {
		throw Exception(ex.first);
	}
	catch (...) {
		std::exception e;
		throw UTIL_EXCEPTION_CREATE_DETAIL(
				UtilityException, Exception::NamedErrorCode(), &e, "");
	}
}

void FutureBaseUtils::assignExceptionResult(
		std::exception&, LocalUniquePtr<ExceptionInfo> &dest) throw() {
	try {
		throw;
	}
	catch (PlatformException &e) {
		dest = UTIL_MAKE_LOCAL_UNIQUE(dest, ExceptionInfo, ExceptionInfo(e, true));
	}
	catch (Exception &e) {
		dest = UTIL_MAKE_LOCAL_UNIQUE(dest, ExceptionInfo, ExceptionInfo(e, false));
	}
	catch (...) {
		dest = UTIL_MAKE_LOCAL_UNIQUE(
				dest, ExceptionInfo,
				ExceptionInfo(Exception(UTIL_EXCEPTION_CREATE_DETAIL(
						UtilityException,
						UTIL_EXCEPTION_UTIL_NAMED_CODE(CODE_INVALID_STATUS),
						NULL, "")), false));
	}
}

} 


ExecutorService::~ExecutorService() {
}

ExecutorService::ExecutorService() {
}


SingleThreadExecutor::SingleThreadExecutor(
		const AllocatorInfo &info, const StdAllocator<void, void> &alloc) :
		futurePool_(info, alloc),
		queue_(alloc),
		runner_(queue_) {
	queue_.setErrorHandler(&detail::FutureBaseUtils::errorServiceShutdown);
}

SingleThreadExecutor::~SingleThreadExecutor() {
	shutdown();
	waitForShutdown();
}

void SingleThreadExecutor::start() {
	thread_.start(&runner_);
}

void SingleThreadExecutor::shutdown() {
	queue_.close();
}

void SingleThreadExecutor::waitForShutdown() {
	thread_.join();
}

void SingleThreadExecutor::submitTask(AllocUniquePtr<detail::TaskBase> &task) {
	queue_.push(task);
}

detail::FuturePool& SingleThreadExecutor::getFuturePool() {
	return futurePool_;
}


SingleThreadExecutor::Runner::Runner(TaskQueue &queueRef) :
		queueRef_(queueRef) {
}

SingleThreadExecutor::Runner::~Runner() {
}

void SingleThreadExecutor::Runner::run() {
	try {
		while (!queueRef_.isClosed()) {
			try {
				queueRef_.wait();

				AllocUniquePtr<detail::TaskBase> task;
				if (queueRef_.poll(task)) {
					(*task)();
				}
			}
			catch (...) {
			}
		}
	}
	catch (...) {
	}
}



InsertionResetter::~InsertionResetter() {
	reset();
}

void InsertionResetter::release() throw() {
	entry_ = Entry();
}

void InsertionResetter::reset() throw() {
	if (entry_.func_ != NULL) {
		entry_.func_(entry_.container_, entry_.pos_);
		release();
	}
}

InsertionResetter::Entry::Entry() :
		func_(NULL), container_(NULL), pos_(0) {
}

InsertionResetter::Entry::Entry(
		ResetterFunc func, void *container, size_t pos) :
		func_(func), container_(container), pos_(pos) {
}

} 
