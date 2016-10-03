/*
    Copyright (c) 2012 TOSHIBA CORPORATION.
    
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
/*
    Copyright (c) 2008, Yubin Lim(purewell@gmail.com).
    All rights reserved.

    Redistribution and use in source and binary forms, with or without 
    modification, are permitted provided that the following conditions 
    are met:

    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the 
      documentation and/or other materials provided with the distribution.
    * Neither the name of the Purewell nor the names of its contributors
      may be used to endorse or promote products derived from this software
      without specific prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT 
    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR 
    A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT 
    OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, 
    SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT 
    LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, 
    DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY 
    THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT 
    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
    OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
#include "util/thread.h"
#include "util/code.h"
#include "util/time.h"
#include "util/os.h"

#ifdef _WIN32
#include <process.h>
#else
#include <sys/syscall.h>
#endif

#include <assert.h>

#define UTIL_WIN_COND_CHANGE

namespace util {

#if UTIL_MINOR_MODULE_ENABLED


struct Barrier::Data {
#ifdef UTIL_HAVE_POSIX_BARRIER
	pthread_barrier_t barrier_;
#endif
};

struct BarrierAttribute::Data {
#ifdef UTIL_HAVE_POSIX_BARRIER
	pthread_barrierattr_t attr_;
#endif
};

Barrier::Barrier(size_t count, const BarrierAttribute *attr) : data_(new Data) {
#ifdef UTIL_HAVE_POSIX_BARRIER
	if (0 != pthread_barrier_init(&data_->barrier_,
		(attr ? &attr->data_->attr_ : NULL), count))
	{
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

Barrier::~Barrier() {
#ifdef UTIL_HAVE_POSIX_BARRIER
	pthread_barrier_destroy(&data_->barrier_);
#endif
}

void Barrier::wait(void) {
#ifdef UTIL_HAVE_POSIX_BARRIER
	const int result = pthread_barrier_wait(&data_->barrier_);
	if (0 != result && PTHREAD_BARRIER_SERIAL_THREAD != result) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

BarrierAttribute::BarrierAttribute() : data_(new Data) {
#ifdef UTIL_HAVE_POSIX_BARRIER
	if (0 != pthread_barrierattr_init(&data_->attr_)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

BarrierAttribute::~BarrierAttribute() {
#ifdef UTIL_HAVE_POSIX_BARRIER
	pthread_barrierattr_destroy(&data_->attr_);
#endif
}

#endif 


struct Condition::Data {
#ifdef UTIL_HAVE_POSIX_CONDITION
	pthread_cond_t cond_;
	pthread_mutex_t mutex_;
	clockid_t clockId_;
#else
#ifndef UTIL_WIN_COND_CHANGE
	bool lastSingnalWasBroadcast_;
	HANDLE mutex_;
	uint32_t waitersCount_;
	CRITICAL_SECTION waitersCountLock_;
	HANDLE waitBarrier_;
	HANDLE waitBarrierClearSignal_;

	Data() {
		lastSingnalWasBroadcast_ = false;
		mutex_ = CreateEvent(NULL, FALSE, TRUE, NULL);
		waitersCount_ = 0;
		InitializeCriticalSection(&waitersCountLock_);
		waitBarrier_ = CreateSemaphore(NULL, 0, LONG_MAX, NULL);
		waitBarrierClearSignal_ = CreateEvent(NULL, FALSE, FALSE, NULL);

		if (mutex_ == NULL ||
				waitBarrier_ == NULL ||
				waitBarrierClearSignal_ == NULL) {
			if (mutex_ == NULL) {
				CloseHandle(mutex_);
			}
			DeleteCriticalSection(&waitersCountLock_);
			if (waitBarrier_ == NULL) {
				CloseHandle(waitBarrier_);
			}
			if (waitBarrierClearSignal_ == NULL) {
				CloseHandle(waitBarrierClearSignal_);
			}
			UTIL_THROW_UTIL_ERROR_CODED(CODE_INVALID_STATUS);
		}
	}

	~Data() {
		CloseHandle(mutex_);
		DeleteCriticalSection(&waitersCountLock_);
		CloseHandle(waitBarrier_);
		CloseHandle(waitBarrierClearSignal_);
	}
#else
	HANDLE cond_;
	Mutex mutex_;

	Data() {
#ifdef UTIL_COND_MANUAL_RESET
		cond_ = CreateEvent(NULL, true, false, NULL);
#else
		cond_ = CreateEvent(NULL, false, false, NULL);
#endif
		if (cond_ == NULL) UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	~Data() {
		CloseHandle(cond_);
	}
#endif
#endif
};

struct ConditionAttribute::Data {
#ifdef UTIL_HAVE_POSIX_CONDITION
	pthread_condattr_t attr_;
#endif
};

Condition::Condition(const ConditionAttribute *attr) : data_(new Data) {
#ifdef UTIL_HAVE_POSIX_CONDITION
	ConditionAttribute defaultAttr;

	pthread_condattr_t &selectedAttr =
			(attr == NULL ? &defaultAttr : attr)->data_->attr_;

	if (0 != pthread_condattr_getclock(&selectedAttr, &data_->clockId_)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	if (0 != pthread_cond_init(&data_->cond_, &selectedAttr)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	if (0 != pthread_mutex_init(&data_->mutex_, NULL)) {
		pthread_cond_destroy(&data_->cond_);
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

Condition::~Condition() {
#ifdef UTIL_HAVE_POSIX_CONDITION
	pthread_cond_destroy(&data_->cond_);
	pthread_mutex_destroy(&data_->mutex_);
#endif
}

void Condition::signal(void) {
#ifdef UTIL_HAVE_POSIX_CONDITION
	pthread_cond_signal(&data_->cond_);
#else
#ifndef UTIL_WIN_COND_CHANGE
	EnterCriticalSection(&data_->waitersCountLock_);
	bool have_waiters = data_->waitersCount_ > 0;
	LeaveCriticalSection(&data_->waitersCountLock_);

	if (have_waiters) {
		ReleaseSemaphore(data_->waitBarrier_, 1, NULL);
	}
#else
	SetEvent(data_->cond_);
#endif
#endif
}

void Condition::broadcast(void) {
#ifdef UTIL_HAVE_POSIX_CONDITION
	pthread_cond_broadcast(&data_->cond_);
#else
#ifndef UTIL_WIN_COND_CHANGE
	EnterCriticalSection(&data_->waitersCountLock_);

	if (data_->waitersCount_ > 0) {
		data_->lastSingnalWasBroadcast_ = true;
		ReleaseSemaphore(data_->waitBarrier_, data_->waitersCount_, 0);
		LeaveCriticalSection(&data_->waitersCountLock_);
		WaitForSingleObject(data_->waitBarrierClearSignal_, INFINITE);
		data_->lastSingnalWasBroadcast_ = false;
	} else {
		LeaveCriticalSection(&data_->waitersCountLock_);
	}
#else
	SetEvent(data_->cond_);
#endif
#endif
}

void Condition::lock(void) {
#ifdef UTIL_HAVE_POSIX_CONDITION
	if (0 != pthread_mutex_lock(&data_->mutex_)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#else
#ifndef UTIL_WIN_COND_CHANGE
	const DWORD result = WaitForSingleObjectEx(data_->mutex_, INFINITE, TRUE);
	if (result != WAIT_OBJECT_0 && result != WAIT_ABANDONED) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#else
	data_->mutex_.lock();
#endif
#endif
}

void Condition::unlock(void) {
#ifdef UTIL_HAVE_POSIX_CONDITION
	if (0 != pthread_mutex_unlock(&data_->mutex_)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#else
#ifndef UTIL_WIN_COND_CHANGE
	if (0 == SetEvent(data_->mutex_)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#else
	data_->mutex_.unlock();
#endif
#endif
}

void Condition::wait() {
#ifdef UTIL_HAVE_POSIX_CONDITION
	if (0 != pthread_cond_wait(&data_->cond_, &data_->mutex_)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#else
#ifndef UTIL_WIN_COND_CHANGE
	if (!wait(INFINITE)) {
		UTIL_THROW_UTIL_ERROR_CODED(CODE_INVALID_STATUS);
	}
#else
	data_->mutex_.unlock();
	WaitForSingleObject(data_->cond_, INFINITE);
#ifdef UTIL_COND_MANUAL_RESET
	ResetEvent(data_->cond_);
#endif
	data_->mutex_.lock();
#endif
#endif
}

bool Condition::wait(struct timespec &ts) {
	(void) &ts;
	UTIL_THROW_NOIMPL_UTIL();
}

bool Condition::wait(uint32_t timeoutMillis) {
#if defined(UTIL_HAVE_POSIX_CONDITION)
	timespec ts = FileLib::calculateTimeoutSpec(data_->clockId_, timeoutMillis);
	const int result = pthread_cond_timedwait(
			&data_->cond_, &data_->mutex_, &ts);

	if (0 != result) {
		if (result == ETIMEDOUT) {
			return false;
		}
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
	return true;
#else
#ifndef UTIL_WIN_COND_CHANGE
	EnterCriticalSection(&data_->waitersCountLock_);
	data_->waitersCount_++;
	LeaveCriticalSection(&data_->waitersCountLock_);

	DWORD result = SignalObjectAndWait(data_->mutex_, data_->waitBarrier_,
			static_cast<DWORD>(timeoutMillis), FALSE);

	if (result == WAIT_OBJECT_0) {
		EnterCriticalSection(&data_->waitersCountLock_);
		data_->waitersCount_--;
		const bool lastWaiter = (data_->lastSingnalWasBroadcast_
				&& data_->waitersCount_ == 0);
		LeaveCriticalSection(&data_->waitersCountLock_);

		if (lastWaiter) {
			result = SignalObjectAndWait(data_->waitBarrierClearSignal_,
					data_->mutex_, INFINITE, FALSE);
			return (result == WAIT_OBJECT_0);
		} else {
			result = WaitForSingleObjectEx(data_->mutex_, INFINITE, TRUE);
			if (result != WAIT_OBJECT_0 && result != WAIT_ABANDONED) {
				UTIL_THROW_PLATFORM_ERROR(NULL);
			}
			return true;
		}
	} else {
		EnterCriticalSection(&data_->waitersCountLock_);
		data_->waitersCount_--;
		LeaveCriticalSection(&data_->waitersCountLock_);

		return false;
	}
#else
	DWORD tv = static_cast<DWORD>(timeoutMillis);
	data_->mutex_.unlock();
	DWORD res = WaitForSingleObject(data_->cond_, tv);
#ifdef UTIL_COND_MANUAL_RESET
	ResetEvent(data_->cond_);
#endif
	data_->mutex_.lock();
	if (res == WAIT_TIMEOUT) return false;
	return true;
#endif
#endif
}

ConditionAttribute::ConditionAttribute() : data_(new Data) {
#ifdef UTIL_HAVE_POSIX_CONDITION
	if (0 != pthread_condattr_init(&data_->attr_)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	if (0 != pthread_condattr_setclock(&data_->attr_, CLOCK_MONOTONIC)) {
		pthread_condattr_destroy(&data_->attr_);
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

ConditionAttribute::~ConditionAttribute() {
#ifdef UTIL_HAVE_POSIX_CONDITION
	pthread_condattr_destroy(&data_->attr_);
#endif
}


#ifndef _WIN32
UTIL_FLAG_TYPE UTIL_MUTEX_DEFAULT = PTHREAD_MUTEX_DEFAULT;
UTIL_FLAG_TYPE UTIL_MUTEX_FAST = PTHREAD_MUTEX_NORMAL;
UTIL_FLAG_TYPE UTIL_MUTEX_RECURSIVE = PTHREAD_MUTEX_RECURSIVE;
UTIL_FLAG_TYPE UTIL_MUTEX_ERRORCHECK = PTHREAD_MUTEX_ERRORCHECK;
#else
UTIL_FLAG_TYPE UTIL_MUTEX_DEFAULT = 0;
UTIL_FLAG_TYPE UTIL_MUTEX_FAST = 0;
UTIL_FLAG_TYPE UTIL_MUTEX_RECURSIVE = 0;
UTIL_FLAG_TYPE UTIL_MUTEX_ERRORCHECK = 0;
#endif

struct Mutex::Data {
#ifdef UTIL_HAVE_POSIX_MUTEX
	Data(const pthread_mutexattr_t *attr);
#else
	Data();
#endif

	~Data();

#ifdef UTIL_HAVE_POSIX_MUTEX
	pthread_mutex_t mutex_;
#else
	CRITICAL_SECTION cs_;
#endif
};

struct MutexAttribute::Data {
#ifdef UTIL_HAVE_POSIX_MUTEX
	pthread_mutexattr_t attr_;
#endif
};

#ifdef UTIL_HAVE_POSIX_MUTEX
Mutex::Data::Data(const pthread_mutexattr_t *attr) {
	if (0 != pthread_mutex_init(&mutex_, attr)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
}
#else
Mutex::Data::Data() {
	InitializeCriticalSection(&cs_);
}
#endif

Mutex::Data::~Data() {
#ifdef UTIL_HAVE_POSIX_MUTEX
	pthread_mutex_destroy(&mutex_);
#else
	DeleteCriticalSection(&cs_);
#endif
}

Mutex::Mutex(int type) {
#ifdef UTIL_HAVE_POSIX_MUTEX
	MutexAttribute attr;
	attr.setType(type);
	data_.reset(new Data(&attr.data_->attr_));
#else
	data_.reset(new Data());
#endif 
}

Mutex::Mutex(const MutexAttribute *attr) {
#ifdef UTIL_HAVE_POSIX_MUTEX
	const pthread_mutexattr_t *baseAttr = NULL;
	if (attr != NULL) {
		baseAttr = &attr->data_->attr_;
	}
	data_.reset(new Data(baseAttr));
#else
	data_.reset(new Data());
#endif 
}

Mutex::~Mutex() {
}

void Mutex::lock(void) {
#ifdef UTIL_HAVE_POSIX_MUTEX
	pthread_mutex_lock(&data_->mutex_);
#else
	EnterCriticalSection(&data_->cs_);
#endif
}

void Mutex::unlock(void) {
#ifdef UTIL_HAVE_POSIX_MUTEX
	pthread_mutex_unlock(&data_->mutex_);
#else
	LeaveCriticalSection(&data_->cs_);
#endif
}

bool Mutex::tryLock(void) {
#ifdef UTIL_HAVE_POSIX_MUTEX
	return (0 == pthread_mutex_trylock(&data_->mutex_));
#else
	return (TryEnterCriticalSection(&data_->cs_) ? true : false);
#endif
}

bool Mutex::tryLock(uint32_t msec) {
	(void) msec;
	UTIL_THROW_NOIMPL_UTIL();
}

MutexAttribute::MutexAttribute() : data_(new Data()) {
#ifdef UTIL_HAVE_POSIX_MUTEX
	if (0 != pthread_mutexattr_init(&data_->attr_)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

MutexAttribute::~MutexAttribute() {
#ifdef UTIL_HAVE_POSIX_MUTEX
	pthread_mutexattr_destroy(&data_->attr_);
#endif
}

void MutexAttribute::setType(int type) {
#ifdef UTIL_HAVE_POSIX_MUTEX
	if (0 != pthread_mutexattr_settype(&data_->attr_, type)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

void MutexAttribute::getType(int &type) const {
#ifdef UTIL_HAVE_POSIX_MUTEX
	if (0 != pthread_mutexattr_gettype(&data_->attr_, &type)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

void MutexAttribute::setShared(bool shared) {
#ifdef UTIL_HAVE_POSIX_MUTEX
	if (0 != pthread_mutexattr_setpshared(&data_->attr_,
			shared ? PTHREAD_PROCESS_SHARED : PTHREAD_PROCESS_PRIVATE)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

void MutexAttribute::getShared(bool &shared) const {
#ifdef UTIL_HAVE_POSIX_MUTEX
	int type;
	if (0 != pthread_mutexattr_getpshared(&data_->attr_, &type)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
	shared = (type == PTHREAD_PROCESS_SHARED);
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}


struct RWLock::Data {
#ifdef UTIL_HAVE_POSIX_MUTEX
	Data(const pthread_rwlockattr_t *attr);
#else
	Data();
	void unlock();
	void addWriter();
	void removeWriter();
#endif

	~Data();
	void close();

#ifdef UTIL_HAVE_POSIX_MUTEX
	pthread_rwlock_t rwlock_;
#else
	HANDLE mutex_;
	HANDLE readEvent_;
	HANDLE writeEvent_;
	size_t readerCount_;
	size_t waitingWriterCount_;
	size_t writerCount_;
#endif
};

struct RWLockAttribute::Data {
#ifdef UTIL_HAVE_POSIX_MUTEX
	pthread_rwlockattr_t attr_;
#endif
};

#ifdef UTIL_HAVE_POSIX_MUTEX
RWLock::Data::Data(const pthread_rwlockattr_t *attr) {
	if (0 != pthread_rwlock_init(&rwlock_, attr)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
}
#else
RWLock::Data::Data() {
	readerCount_ = 0;
	waitingWriterCount_ = 0;
	writerCount_ = 0;
	mutex_ = NULL;
	readEvent_ = NULL;
	writeEvent_ = NULL;
	try {
		mutex_ = CreateMutexW(NULL, FALSE, NULL);
		if (mutex_ == NULL) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
		readEvent_ = CreateEventW(NULL, TRUE, TRUE, NULL);
		if (readEvent_ == NULL) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
		writeEvent_ = CreateEventW(NULL, TRUE, TRUE, NULL);
		if (writeEvent_ == NULL) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}
	}
	catch (...) {
		close();
		throw;
	}
}
#endif

#ifndef UTIL_HAVE_POSIX_MUTEX
void RWLock::Data::unlock() {
	switch (WaitForSingleObject(mutex_, INFINITE)) {
	case WAIT_OBJECT_0:
		if (readerCount_ == 0) {
			ReleaseMutex(mutex_);
			UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_OPERATION,
					"Not locked");
		}

		writerCount_ = 0;
		if (waitingWriterCount_ == 0) {
			SetEvent(readEvent_);
		}
		if ((--readerCount_) == 0) {
			SetEvent(writeEvent_);
		}
		ReleaseMutex(mutex_);
		break;
	case WAIT_FAILED:
		UTIL_THROW_PLATFORM_ERROR(NULL);
	default:
		UTIL_THROW_UTIL_ERROR_CODED(CODE_INVALID_STATUS);
	}
}
#endif

#ifndef UTIL_HAVE_POSIX_MUTEX
void RWLock::Data::addWriter() {
	switch (WaitForSingleObject(mutex_, INFINITE)) {
	case WAIT_OBJECT_0:
		if (++waitingWriterCount_ == 1) {
			ResetEvent(readEvent_);
		}
		ReleaseMutex(mutex_);
		break;
	case WAIT_FAILED:
		UTIL_THROW_PLATFORM_ERROR(NULL);
	default:
		UTIL_THROW_UTIL_ERROR_CODED(CODE_INVALID_STATUS);
	}
}
#endif

#ifndef UTIL_HAVE_POSIX_MUTEX
void RWLock::Data::removeWriter() {
	switch (WaitForSingleObject(mutex_, INFINITE)) {
	case WAIT_OBJECT_0:
		if (--waitingWriterCount_ == 0 && writerCount_ == 0) {
			SetEvent(readEvent_);
		}
		ReleaseMutex(mutex_);
		break;
	case WAIT_FAILED:
		UTIL_THROW_PLATFORM_ERROR(NULL);
	default:
		UTIL_THROW_UTIL_ERROR_CODED(CODE_INVALID_STATUS);
	}
}
#endif

RWLock::Data::~Data() {
	close();
}

void RWLock::Data::close() {
#ifdef UTIL_HAVE_POSIX_MUTEX
	pthread_rwlock_destroy(&rwlock_);
#else
	if (mutex_ != NULL) {
		CloseHandle(mutex_);
	}
	if (readEvent_ != NULL) {
		CloseHandle(readEvent_);
	}
	if (writeEvent_ != NULL) {
		CloseHandle(writeEvent_);
	}
#endif
}

RWLock::RWLock(const RWLockAttribute *attr) :
		readLock_(newData(attr)),
		writeLock_(readLock_.data_) {
}

RWLock::RWLock() :
		readLock_(newData(NULL)),
		writeLock_(readLock_.data_) {
}

RWLock::~RWLock() {
	delete readLock_.data_;
}

RWLock::Data* RWLock::newData(const RWLockAttribute *attr) {
#ifdef UTIL_HAVE_POSIX_MUTEX
	const pthread_rwlockattr_t *baseAttr = NULL;
	if (attr != NULL) {
		baseAttr = &attr->data_->attr_;
	}

	return new Data(baseAttr);
#else
	return new Data();
#endif 
}

void RWLock::ReadLock::lock(void) {
#ifdef UTIL_HAVE_POSIX_MUTEX
	pthread_rwlock_rdlock(&data_->rwlock_);
#else
	HANDLE handles[] = { data_->mutex_, data_->readEvent_ };
	switch (WaitForMultipleObjects(2, handles, TRUE, INFINITE)) {
	case WAIT_OBJECT_0:
	case (WAIT_OBJECT_0 + 1):
		data_->readerCount_++;
		ResetEvent(data_->writeEvent_);
		ReleaseMutex(data_->mutex_);
		break;
	case WAIT_FAILED:
		UTIL_THROW_PLATFORM_ERROR(NULL);
	default:
		UTIL_THROW_UTIL_ERROR_CODED(CODE_INVALID_STATUS);
	}
#endif
}

bool RWLock::ReadLock::tryLock(void) {
#ifdef UTIL_HAVE_POSIX_MUTEX
	return (0 == pthread_rwlock_tryrdlock(&data_->rwlock_));
#else
	return tryLock(1);
#endif
}

bool RWLock::ReadLock::tryLock(uint32_t msec) {
#ifdef UTIL_HAVE_POSIX_MUTEX
	timespec ts = FileLib::calculateTimeoutSpec(CLOCK_REALTIME, msec);
	return (0 == pthread_rwlock_timedrdlock(&data_->rwlock_, &ts));
#else
	HANDLE handles[] = { data_->mutex_, data_->readEvent_ };
	switch (WaitForMultipleObjects(2, handles, TRUE, msec)) {
	case WAIT_OBJECT_0:
	case (WAIT_OBJECT_0 + 1):
		data_->readerCount_++;
		assert (data_->writerCount_ == 0);
		assert (data_->waitingWriterCount_ == 0);
		ResetEvent(data_->writeEvent_);
		ReleaseMutex(data_->mutex_);
		return true;
	case WAIT_TIMEOUT:
		return false;
	case WAIT_FAILED:
		UTIL_THROW_PLATFORM_ERROR(NULL);
	default:
		UTIL_THROW_UTIL_ERROR_CODED(CODE_INVALID_STATUS);
	}
#endif
}

void RWLock::ReadLock::unlock(void) {
#ifdef UTIL_HAVE_POSIX_MUTEX
	pthread_rwlock_unlock(&data_->rwlock_);
#else
	data_->unlock();
#endif
}

void RWLock::WriteLock::lock(void) {
#ifdef UTIL_HAVE_POSIX_MUTEX
	pthread_rwlock_wrlock(&data_->rwlock_);
#else
	if (!tryLock(INFINITE)) {
		UTIL_THROW_UTIL_ERROR_CODED(CODE_INVALID_STATUS);
	}
#endif
}

bool RWLock::WriteLock::tryLock(void) {
#ifdef UTIL_HAVE_POSIX_MUTEX
	return (0 == pthread_rwlock_trywrlock(&data_->rwlock_));
#else
	return tryLock(1);
#endif
}

bool RWLock::WriteLock::tryLock(uint32_t msec) {
#ifdef UTIL_HAVE_POSIX_MUTEX
	timespec ts = FileLib::calculateTimeoutSpec(CLOCK_REALTIME, msec);
	return (0 == pthread_rwlock_timedwrlock(&data_->rwlock_, &ts));
#else
	data_->addWriter();
	HANDLE handles[] = { data_->mutex_, data_->writeEvent_ };
	switch (WaitForMultipleObjects(2, handles, TRUE, msec)) {
	case WAIT_OBJECT_0:
	case (WAIT_OBJECT_0 + 1):
		data_->waitingWriterCount_--;
		data_->readerCount_++;
		data_->writerCount_++;
		ResetEvent(data_->readEvent_);
		ResetEvent(data_->writeEvent_);
		ReleaseMutex(data_->mutex_);
		return true;
	case WAIT_TIMEOUT:
		data_->removeWriter();
		return false;
	case WAIT_FAILED:
		data_->removeWriter();
		UTIL_THROW_PLATFORM_ERROR(NULL);
	default:
		data_->removeWriter();
		UTIL_THROW_UTIL_ERROR_CODED(CODE_INVALID_STATUS);
	}
#endif
}

void RWLock::WriteLock::unlock(void) {
#ifdef UTIL_HAVE_POSIX_MUTEX
	pthread_rwlock_unlock(&data_->rwlock_);
#else
	data_->unlock();
#endif
}

RWLockAttribute::RWLockAttribute() : data_(new Data()) {
#ifdef UTIL_HAVE_POSIX_MUTEX
	if (0 != pthread_rwlockattr_init(&data_->attr_)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

RWLockAttribute::~RWLockAttribute() {
#ifdef UTIL_HAVE_POSIX_MUTEX
	pthread_rwlockattr_destroy(&data_->attr_);
#endif
}

void RWLockAttribute::setShared(bool shared) {
#ifdef UTIL_HAVE_POSIX_MUTEX
	if (0 != pthread_rwlockattr_setpshared(&data_->attr_,
			shared ? PTHREAD_PROCESS_SHARED : PTHREAD_PROCESS_PRIVATE)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}

void RWLockAttribute::getShared(bool &shared) const {
#ifdef UTIL_HAVE_POSIX_MUTEX
	int type;
	if (0 != pthread_rwlockattr_getpshared(&data_->attr_, &type)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
	shared = (type == PTHREAD_PROCESS_SHARED);
#else
	UTIL_THROW_NOIMPL_UTIL();
#endif
}


struct Semaphore::Data {
#ifdef _WIN32
	HANDLE handle_;
#else
	enum Type {
		TYPE_NAMED,
		TYPE_UNNAMED
	};

	struct Named;
	struct Unnamed;

	Type type_;

	sem_t* getKey();
	const char* getName();
#endif 
};

#ifndef _WIN32
struct Semaphore::Data::Named : public Semaphore::Data {
	sem_t *key_;
	std::string name_;
	Named() : key_(NULL) {
		type_ = TYPE_NAMED;
	}
};

struct Semaphore::Data::Unnamed : public Semaphore::Data {
	sem_t key_;
	Unnamed() {
		type_ = TYPE_UNNAMED;
	}
};

sem_t* Semaphore::Data::getKey() {
	if (type_ == TYPE_NAMED) {
		return static_cast<Named*>(this)->key_;
	}
	else {
		return &static_cast<Unnamed*>(this)->key_;
	}
}

const char* Semaphore::Data::getName() {
	if (type_ == TYPE_NAMED) {
		return static_cast<Named*>(this)->name_.c_str();
	}
	return NULL;
}
#endif 

Semaphore::Semaphore(uint32_t value) {
#ifdef _WIN32
	data_ = new Data();
	try {
		const HANDLE handle = CreateSemaphoreW(NULL, 0, LONG_MAX, NULL);
		if (handle == NULL) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}

		data_->handle_ = handle;
	}
	catch (...) {
		delete data_;
		throw;
	}
#else
	Data::Unnamed *data = new Data::Unnamed();
	try {
		if (-1 == sem_init(&(data->key_), 0, value)) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}

		data_ = data;
	}
	catch (...) {
		delete data;
		throw;
	}
#endif
}

Semaphore::Semaphore(
		const char8_t *name, FileFlag flags, FilePermission perm, uint32_t value) {
#ifdef _WIN32
	data_ = new Data();
	try {
		std::wstring wideNameStr;
		CodeConverter(Code::UTF8, Code::WCHAR_T)(name, wideNameStr);
		const HANDLE handle =
				CreateSemaphoreW(NULL, 0, LONG_MAX, wideNameStr.c_str());
		if (handle == NULL) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}

		data_->handle_ = handle;
	}
	catch (...) {
		delete data_;
		throw;
	}
#else
	Data::Named *data = new Data::Named();
	try {
		CodeConverter(Code::UTF8, Code::CHAR)(name, data->name_);

		if (flags & O_CREAT) {
			data->key_ = sem_open(data->getName(), flags);
		}
		else {
			data->key_ = sem_open(
					data->getName(), flags, static_cast<int>(perm), value);
		}

		if (SEM_FAILED == data->key_) {
			UTIL_THROW_PLATFORM_ERROR(NULL);
		}

		data_ = data;
	}
	catch (...) {
		delete data;
		throw;
	}
#endif
}

Semaphore::~Semaphore() {
#ifdef _WIN32
	CloseHandle(data_->handle_);
	delete data_;
#else
	if (data_->type_ == Data::TYPE_NAMED) {
		Data::Named *namedData = static_cast<Data::Named*>(data_);
		sem_close(namedData->key_);
		delete namedData;
	}
	else {
		Data::Unnamed *unnamedData = static_cast<Data::Unnamed*>(data_);
		sem_destroy(&unnamedData->key_);
		delete unnamedData;
	}
#endif
}

void Semaphore::lock(void) {
#ifdef _WIN32
	WaitForSingleObjectEx(data_->handle_, INFINITE, TRUE);
#else
	sem_wait(data_->getKey());
#endif
}

bool Semaphore::tryLock(void) {
#ifdef _WIN32
	const DWORD result =
		WaitForSingleObjectEx(data_->handle_, 0, TRUE);
	return (result == WAIT_OBJECT_0 || result == WAIT_ABANDONED);
#else
	return (-1 != sem_trywait(data_->getKey()));
#endif
}

bool Semaphore::tryLock(uint32_t msec) {
#ifdef _WIN32
	const DWORD result = WaitForSingleObjectEx(
			data_->handle_, static_cast<DWORD>(msec), TRUE);
	return (result == WAIT_OBJECT_0 || result == WAIT_ABANDONED);
#else
	timespec ts = FileLib::calculateTimeoutSpec(CLOCK_REALTIME, msec);
	return (-1 != sem_timedwait(data_->getKey(), &ts));
#endif
}

void Semaphore::unlock(void) {
#ifdef _WIN32
	ReleaseSemaphore(data_->handle_, 1, NULL);
#else
	sem_post(data_->getKey());
#endif
}

bool Semaphore::unlink(const char8_t *name) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	std::string nameStr;
	CodeConverter(Code::UTF8, Code::CHAR)(name, nameStr);
	return (0 == sem_unlink(nameStr.c_str()));
#endif
}

bool Semaphore::unlink(void) const {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	return (0 == sem_unlink(data_->getName()));
#endif
}

void Semaphore::getValue(size_t &value) const {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	int intValue;
	if (0 != sem_getvalue(const_cast<Data*>(data_)->getKey(), &intValue)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	value = static_cast<size_t>(intValue);
#endif
}


struct SpinLock::Data {
	Data();
	~Data();

#ifdef UTIL_HAVE_POSIX_SPIN
	pthread_spinlock_t mutex_;
#else
	static const DWORD SPIN_COUNT = 2000;
	CRITICAL_SECTION cs_;
#endif
};

SpinLock::Data::Data() {
#ifdef UTIL_HAVE_POSIX_SPIN
	if (0 != pthread_spin_init(&mutex_, PTHREAD_PROCESS_PRIVATE)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#else
	if (!InitializeCriticalSectionAndSpinCount(&cs_, SPIN_COUNT)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

SpinLock::Data::~Data() {
#ifdef UTIL_HAVE_POSIX_SPIN
	pthread_spin_destroy(&mutex_);
#else
	DeleteCriticalSection(&cs_);
#endif
}

SpinLock::SpinLock() : data_(new Data()) {
}

SpinLock::~SpinLock() {
}

void SpinLock::lock(void) {
#ifdef UTIL_HAVE_POSIX_SPIN
	pthread_spin_lock(&data_->mutex_);
#else
	EnterCriticalSection(&data_->cs_);
#endif
}

void SpinLock::unlock(void) {
#ifdef UTIL_HAVE_POSIX_SPIN
	pthread_spin_unlock(&data_->mutex_);
#else
	LeaveCriticalSection(&data_->cs_);
#endif
}

bool SpinLock::tryLock(void) {
#ifdef UTIL_HAVE_POSIX_SPIN
	return (0 == pthread_spin_trylock(&data_->mutex_));
#else
	return (TryEnterCriticalSection(&data_->cs_) ? true : false);
#endif
}

bool SpinLock::tryLock(uint32_t msec) {
	(void) msec;
	UTIL_THROW_NOIMPL_UTIL();
}


struct Thread::Data {
#ifdef _WIN32
	HANDLE handle_;
#else
	pthread_t threadId_;
#endif

	Data() :
#ifdef _WIN32
			handle_(NULL)
#else
			threadId_(0)
#endif
	{
	}
};

struct ThreadAttribute::Data {
#ifdef _WIN32
#else
	pthread_attr_t attr_;
#endif
};

ThreadRunner::~ThreadRunner() {
}

Thread::Thread() : data_(new Data()) {
}

Thread::~Thread() {
	close();
}

void Thread::start(ThreadRunner *runner, const ThreadAttribute *attr) {
	if (!isClosed()) {
		UTIL_THROW_UTIL_ERROR_CODED(CODE_ILLEGAL_OPERATION);
	}

	if (runner == NULL) {
		runner = this;
	}

#ifdef _WIN32
	const uintptr_t result =
			_beginthreadex(NULL, 0, threadProc, runner, 0, NULL);
	if (result == 0) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
	data_->handle_ = reinterpret_cast<HANDLE>(result);
#else
	pthread_attr_t *pthreadAttr = (attr ? &attr->data_->attr_ : NULL);
	if (0 != pthread_create(
			&data_->threadId_, pthreadAttr, threadProc, runner)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

void Thread::close() {
	if (!isClosed()) {
#ifdef _WIN32
		CloseHandle(data_->handle_);
		data_->handle_ = NULL;
#else
		if (data_->threadId_ != 0) {
			pthread_detach(data_->threadId_);
			data_->threadId_ = 0;
		}
#endif
	}
}

void Thread::join() {
	if (isClosed()) {
		return;
	}
#ifdef _WIN32
	if (WaitForSingleObject(data_->handle_, INFINITE) == WAIT_FAILED) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#else
	pthread_join(data_->threadId_, NULL);
	data_->threadId_ = 0;
#endif
}

void Thread::sleep(uint32_t millisecTime) {
#ifdef _WIN32
	Sleep(millisecTime);
#else
	timespec spec;
	spec.tv_sec = millisecTime / 1000;
	spec.tv_nsec = (millisecTime % 1000) * 1000 * 1000;
	nanosleep(&spec, NULL);
#endif
}

void Thread::yield() {
	Thread::sleep(0);
}

uint64_t Thread::getSelfId() {
#ifdef _WIN32
	return GetCurrentThreadId();
#else
	return syscall(SYS_gettid);
#endif
}

void Thread::run() {
}

bool Thread::isClosed() {
#ifdef _WIN32
	return (data_->handle_ == NULL);
#else
	return (data_->threadId_ == 0);
#endif
}

#ifdef _WIN32
unsigned int Thread::threadProc(void *param) {
	ThreadRunner *runner = static_cast<ThreadRunner*>(param);
	runner->run();
	return 0;
}
#else
void* Thread::threadProc(void *param) {
	ThreadRunner *runner = static_cast<ThreadRunner*>(param);
	runner->run();
	return NULL;
}
#endif

ThreadAttribute::ThreadAttribute() : data_(new Data()) {
#ifdef _WIN32
#else
	if (0 != pthread_attr_init(&data_->attr_)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

ThreadAttribute::~ThreadAttribute() {
#ifdef _WIN32
#else
	pthread_attr_destroy(&data_->attr_);
#endif
}

void ThreadAttribute::setScope(int type) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	if (0 != pthread_attr_setscope(&data_->attr_, type)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

void ThreadAttribute::getScope(int &type) const {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	if (0 != pthread_attr_getscope(&data_->attr_, &type)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

void ThreadAttribute::setDetach(bool detached) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	const int type =
			(detached ? PTHREAD_CREATE_DETACHED : PTHREAD_CREATE_JOINABLE);
	if (0 != pthread_attr_setdetachstate(&data_->attr_, type)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

void ThreadAttribute::getDetach(bool &detached) const {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	int type;

	if (0 != pthread_attr_getdetachstate(&data_->attr_, &type)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}

	detached = (type == PTHREAD_CREATE_DETACHED);
#endif
}

void ThreadAttribute::setGuardSize(size_t size) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	if (0 != pthread_attr_setguardsize(&data_->attr_, size)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

void ThreadAttribute::getGuardSize(size_t &size) const {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	if (0 != pthread_attr_getguardsize(&data_->attr_, &size)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

void ThreadAttribute::setStack(void *stack, size_t size) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	if (0 != pthread_attr_setstack(&data_->attr_, stack, size)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

void ThreadAttribute::getStack(void *&stack, size_t &size) const {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	if (0 != pthread_attr_getstack(&data_->attr_, &stack, &size)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

void ThreadAttribute::setStackSize(size_t size) {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	if (0 != pthread_attr_setstacksize(&data_->attr_, size)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

void ThreadAttribute::getStackSize(size_t &size) const {
#ifdef _WIN32
	UTIL_THROW_NOIMPL_UTIL();
#else
	if (0 != pthread_attr_getstacksize(&data_->attr_, &size)) {
		UTIL_THROW_PLATFORM_ERROR(NULL);
	}
#endif
}

ConflictionDetector::ConflictionDetector() {
}

ConflictionDetector::~ConflictionDetector() {
}

void ConflictionDetector::enter() {
	if (++counter_ != 1) {
		assert(false);
		UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_OPERATION,
				"Internal error by critical section confliction");
	}
}

void ConflictionDetector::leave() {
	if (--counter_ != 0) {
		assert(false);
		UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_OPERATION,
				"Internal error by wrong confliction detector usage");
	}
}


ConflictionDetectorScope::ConflictionDetectorScope(
		ConflictionDetector &detector, bool entering) :
		detector_(detector), entering_(entering) {
	if (entering_) {
		detector_.enter();
	}
	else {
		detector_.leave();
	}
}

ConflictionDetectorScope::~ConflictionDetectorScope() try {
	if (entering_) {
		detector_.leave();
	}
	else {
		detector_.enter();
	}
}
catch (...) {
}

} 
