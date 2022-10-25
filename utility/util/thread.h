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
/*!
	@file
    @brief Definition of Utility of threads
*/
#ifndef UTIL_THREAD_H_
#define UTIL_THREAD_H_

#include "util/type.h"
#include "util/file.h"
#include <memory>

/*!
    @brief Declaration of thread local.
*/
#ifdef WIN32
#define UTIL_THREAD_LOCAL __declspec(thread)
#else
#define UTIL_THREAD_LOCAL __thread
#endif

namespace util {

namespace detail { template<typename T> struct AtomicImplResolver; }

/*!
    @brief Value for atomic operation.
*/
template<typename T>
class Atomic : public detail::AtomicImplResolver<T>::Result {
public:
	typedef Atomic<T> ThisType;
	typedef T ValueType;
	typedef typename detail::AtomicImplResolver<T>::Result BaseType;

	Atomic() {}
	~Atomic() {}
	explicit Atomic(ValueType value) : BaseType(value) {}

	ThisType& operator=(ValueType value) {
		BaseType::operator=(value);
		return *this;
	}
};

template<typename L>
class LockGuard {
public:
	explicit LockGuard(L &lockObject);
	~LockGuard();

private:
	LockGuard(const LockGuard&);
	LockGuard& operator=(const LockGuard&);

	L &lockObject_;
};

template<typename L>
inline LockGuard<L>::LockGuard(L &lockObject) : lockObject_(lockObject) {
	lockObject_.lock();
}

template<typename L>
inline LockGuard<L>::~LockGuard() {
	lockObject_.unlock();
}

/*!
    @brief Locks in the scope if object exists.
*/
template<typename L>
class DynamicLockGuard {
public:
	explicit DynamicLockGuard(L *lockObject);
	~DynamicLockGuard();

private:
	DynamicLockGuard(const DynamicLockGuard&);
	DynamicLockGuard& operator=(const DynamicLockGuard&);

	L *lockObject_;
};

template<typename L>
DynamicLockGuard<L>::DynamicLockGuard(L *lockObject) :
		lockObject_(lockObject) {
	if (lockObject_ != NULL) {
		lockObject_->lock();
	}
}

template<typename L>
DynamicLockGuard<L>::~DynamicLockGuard() {
	if (lockObject_ != NULL) {
		lockObject_->unlock();
	}
}

#if UTIL_MINOR_MODULE_ENABLED

class BarrierAttribute;

class Barrier {
public:
	explicit Barrier(size_t count, const BarrierAttribute *attr = NULL);

	virtual ~Barrier();

public:
	void wait(void);

private:
	Barrier(const Barrier&);
	Barrier& operator=(const Barrier&);

	struct Data;
	UTIL_UNIQUE_PTR<Data> data_;
};

class BarrierAttribute {
public:
	BarrierAttribute();

	virtual ~BarrierAttribute();

private:
	BarrierAttribute(const BarrierAttribute&);
	BarrierAttribute& operator=(const BarrierAttribute&);

	friend class Barrier;
	struct Data;
	UTIL_UNIQUE_PTR<Data> data_;
};

#endif 

class Condition;
class ConditionAttribute;
class Mutex;

/*!
    @brief Condition of values, setting and getting atomically.
*/
class Condition {
public:
	explicit Condition(const ConditionAttribute *attr = NULL);

	virtual ~Condition();

public:
	void signal(void);

	void broadcast(void);

	void lock(void);

	void unlock(void);

	void wait(void);

	bool wait(uint32_t timeoutMillis);

	bool wait(struct timespec &ts);

private:
	Condition(const Condition&);
	Condition& operator=(const Condition&);

	struct Data;
	UTIL_UNIQUE_PTR<Data> data_;
};

class ConditionAttribute {
public:
	ConditionAttribute();

	virtual ~ConditionAttribute();

private:
	ConditionAttribute(const ConditionAttribute&);
	ConditionAttribute& operator=(const ConditionAttribute&);

	friend class Condition;
	struct Data;
	UTIL_UNIQUE_PTR<Data> data_;
};

class MutexAttribute;
namespace detail {
class DirectMutex;
}

extern UTIL_FLAG_TYPE UTIL_MUTEX_DEFAULT; 
extern UTIL_FLAG_TYPE UTIL_MUTEX_FAST; 
extern UTIL_FLAG_TYPE UTIL_MUTEX_RECURSIVE; 
extern UTIL_FLAG_TYPE UTIL_MUTEX_ERRORCHECK; 

/*!
    @brief Mutex for prevention of concurrent execution.
*/
class Mutex {
public:
	explicit Mutex(int type = UTIL_MUTEX_FAST);

	explicit Mutex(const MutexAttribute *attr);

	virtual ~Mutex();

	void lock(void);

	bool tryLock(void);

	bool tryLock(uint32_t msec);

	void unlock(void);

private:
	friend class detail::DirectMutex;

	Mutex(const Mutex&);
	Mutex& operator=(const Mutex&);

	Mutex(const FalseType&);

	struct Data;
	UTIL_UNIQUE_PTR<Data> data_;
};

/*!
    @brief Attributes of Mutex.
*/
class MutexAttribute {
public:
	MutexAttribute();

	virtual ~MutexAttribute();

public:
	void setType(int type = UTIL_MUTEX_DEFAULT);

	void getType(int &type) const;

	void setShared(bool shared);

	void getShared(bool &shared) const;

private:
	MutexAttribute(const MutexAttribute&);
	MutexAttribute& operator=(const MutexAttribute&);

	friend class Mutex;
	struct Data;
	UTIL_UNIQUE_PTR<Data> data_;
};

class RWLockAttribute;
class RWLock;

/*!
    @brief Lock permits a single write operation, or multi read operations.
*/
class RWLock {
private:
	struct Data;
public:
	class ReadLock;
	class WriteLock;

	explicit RWLock(const RWLockAttribute *attr);

	explicit RWLock();

	virtual ~RWLock();

	ReadLock& getReadLock();

	WriteLock& getWriteLock();

	inline operator ReadLock&() { return getReadLock(); }
	inline operator WriteLock&() { return getWriteLock(); }

public:
	class ReadLock {
	public:
		void lock(void);

		bool tryLock(void);

		bool tryLock(uint32_t msec);

		void unlock(void);

	private:
		friend class RWLock;

		ReadLock(Data *data) : data_(data) {}
		ReadLock(const ReadLock&);
		ReadLock& operator=(const ReadLock&);

		Data *data_;
	};

public:
	class WriteLock {
	public:
		void lock(void);

		bool tryLock(void);

		bool tryLock(uint32_t msec);

		void unlock(void);

	private:
		friend class RWLock;

		WriteLock(Data *data) : data_(data) {}
		WriteLock(const WriteLock&);
		WriteLock& operator=(const WriteLock&);

		Data *data_;
	};

private:
	RWLock(const RWLock&);
	RWLock& operator=(const RWLock&);

	Data* newData(const RWLockAttribute *attr);

	ReadLock readLock_;
	WriteLock writeLock_;
};

/*!
    @brief Attributes of RWLock.
*/
class RWLockAttribute {
public:
	RWLockAttribute(void);

	virtual ~RWLockAttribute();

	void setShared(bool pshared);

	void getShared(bool &pshared) const;

private:
	RWLockAttribute(const RWLockAttribute&);
	RWLockAttribute& operator=(const RWLockAttribute&);

	friend class RWLock;
	struct Data;
	UTIL_UNIQUE_PTR<Data> data_;
};

inline RWLock::ReadLock& RWLock::getReadLock() {
	return readLock_;
}

inline RWLock::WriteLock& RWLock::getWriteLock() {
	return writeLock_;
}

typedef RWLock::ReadLock ReadLock;
typedef RWLock::WriteLock WriteLock;

/*!
    @brief Semaphore.
*/
class Semaphore {
public:
	explicit Semaphore(uint32_t value = 0);

	Semaphore(const char8_t *name, FileFlag flags, FilePermission perm,
			uint32_t value = 0);

	virtual ~Semaphore();

	static bool unlink(const char8_t *name);

public:
	bool unlink(void) const;
	void lock(void);
	bool tryLock(void);
	bool tryLock(uint32_t msec);
	void unlock(void);

	void getValue(size_t &value) const;

private:
	Semaphore(const Semaphore&);
	Semaphore& operator=(const Semaphore&);

	struct Data;
	Data *data_;
};

/*!
    @brief Lock permits only a single thread operation.
*/
class SpinLock {
public:
	explicit SpinLock();

	virtual ~SpinLock();

	void lock(void);

	bool tryLock(void);

	bool tryLock(uint32_t msec);

	void unlock(void);

private:
	SpinLock(const SpinLock&);
	SpinLock& operator=(const SpinLock&);

	struct Data;
	UTIL_UNIQUE_PTR<Data> data_;
};

class ThreadAttribute;

/*!
    @brief Interface to execute a thread.
*/
class ThreadRunner {
public:
	virtual ~ThreadRunner();
	virtual void run() = 0;
};

/*!
    @brief Interface to run multiple threads.
*/
class Thread : public ThreadRunner {
public:
	Thread();

	virtual ~Thread();

	virtual void start(
			ThreadRunner *runner = NULL, const ThreadAttribute *attr = NULL);

	virtual void close();

public:

	void join();



	static void sleep(uint32_t millisecTime);

	static void yield();

	static uint64_t getSelfId();

protected:
	virtual void run();


private:
	Thread(const Thread&);
	Thread& operator=(const Thread&);

	bool isClosed();

#ifdef _WIN32
	static unsigned int __stdcall threadProc(void*);
#else
	static void* threadProc(void*);
#endif

	struct Data;
	UTIL_UNIQUE_PTR<Data> data_;
};

/*!
    @brief Attribute of a thread.
*/
class ThreadAttribute {
public:
	friend class Thread;

	ThreadAttribute();


	virtual ~ThreadAttribute();


public:
	void setScope(int type);

	void getScope(int &type) const;

	void setDetach(bool detached);

	void getDetach(bool &detached) const;

	void setGuardSize(size_t size);

	void getGuardSize(size_t &size) const;

	void setStack(void *stack, size_t size);

	void getStack(void *&stack, size_t &size) const;

	void setStackSize(size_t size);

	void getStackSize(size_t &size) const;

private:
	ThreadAttribute(const ThreadAttribute&);
	ThreadAttribute& operator=(const ThreadAttribute&);

	struct Data;
	UTIL_UNIQUE_PTR<Data> data_;
};



namespace detail {

#if defined(__GNUC__)
#if !defined(UTIL_ATOMIC_USE_GCC_BUILTINS) || UTIL_ATOMIC_USE_GCC_BUILTINS
#define UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS
#elif defined(__i386__) || defined(__x86_64__)
#define UTIL_ATOMIC_DETAIL_TYPE_GCC_X86
#else
#error 0
#endif
#elif defined(_WIN32)
#define UTIL_ATOMIC_DETAIL_TYPE_WIN32
#else
#error 0
#endif

/*!
    @brief Utility to control an atomic operation.
*/
struct AtomicUtil {

	struct BadType;

	template<typename T, size_t size = sizeof(T)>
	struct StorageTypeResolver {};

	static void fenceBefore();
	static void fenceAfter();
	static void fenceBeforeStore();
	static void fenceAfterStore();
	static void fenceAfterLoad();

	template<typename S, size_t size = sizeof(S)>
	struct CompareExchanger {};
};

template<typename T, typename A,
typename S = typename AtomicUtil::StorageTypeResolver<T>::Result>
class AtomicBase {
public:
	typedef AtomicBase<T, A, S> ThisType;
	typedef T ValueType;
	typedef A AccessorType;
	typedef S StorageType;

	AtomicBase() : value_(StorageType()) {}
	~AtomicBase() {}
	AtomicBase(ValueType value) :
		value_(AccessorType::toStorageType(value)) {
	}

	bool compareExchange(ValueType &expected, ValueType desired) volatile {
		return compareExchangeImpl<SameStorageType>(
				expected, desired, SameStorageType());
	}

	ValueType exchange(ValueType value) volatile {
		ValueType last = const_cast<const volatile StorageType&>(value_);
		do {
		}
		while (!compareExchange(last, value));
		return last;
	}

	void store(ValueType value) volatile {
		storeImpl<LargeStorageSize>(value, LargeStorageSize());
	}

	ValueType load() const volatile {
		return loadImpl<LargeStorageSize>(LargeStorageSize());
	}

	ThisType& operator=(ValueType value) {
		store(value);
		return *this;
	}

	operator ValueType() const volatile {
		return load();
	}

protected:
	inline static StorageType toStorageType(ValueType value) {
		return static_cast<StorageType>(value);
	}

	inline static ValueType toValueType(StorageType value) {
		return static_cast<ValueType>(value);
	}

private:
	typedef typename IsSame<T, S>::Type SameStorageType;
	typedef typename
			BoolType<(sizeof(S) > 4)>::Result LargeStorageSize;

	template<typename>
	bool compareExchangeImpl(
			ValueType &expected, ValueType desired, TrueType) volatile {
		AtomicUtil::fenceBefore();
		const bool succeeded = AtomicUtil::CompareExchanger<ValueType>()(
				expected, desired, &value_);
		AtomicUtil::fenceAfter();
		return succeeded;
	}

	template<typename>
	bool compareExchangeImpl(
			ValueType &expected, ValueType desired, FalseType) volatile {
		AtomicUtil::fenceBefore();
		StorageType storageExpected = AccessorType::toStorageType(expected);
		StorageType storageDesired = AccessorType::toStorageType(desired);
		const bool succeeded = AtomicUtil::CompareExchanger<StorageType>()(
				storageExpected, storageDesired, &value_);
		AtomicUtil::fenceAfter();
		if (!succeeded) {
			expected = AccessorType::toValueType(storageExpected);
		}
		return succeeded;
	}

	template<typename>
	void storeImpl(ValueType value, TrueType) volatile {
		ValueType last = AccessorType::toValueType(value_);
		do {
		}
		while (!compareExchange(last, value));
	}

	template<typename>
	void storeImpl(ValueType value, FalseType) volatile {
		AtomicUtil::fenceBeforeStore();
		const_cast<volatile StorageType&>(value_) =
				AccessorType::toStorageType(value);
		AtomicUtil::fenceAfterStore();
	}

	template<typename>
	ValueType loadImpl(TrueType) const volatile {
		ValueType value = AccessorType::toValueType(
				const_cast<const volatile StorageType&>(value_));
		do {
		}
		while (!const_cast<AtomicBase*>(this)->compareExchange(value, value));
		return value;
	}

	template<typename>
	ValueType loadImpl(FalseType) const volatile {
		const ValueType value = AccessorType::toValueType(
				const_cast<const volatile StorageType&>(value_));
		AtomicUtil::fenceAfterLoad();
		return value;
	}

protected:
	StorageType value_;
};

#define UTIL_ATOMIC_DETAIL_FETCH_AND_OP_BY_CAS(opSymbol, operand) \
	ValueType last = AccessorType::toValueType( \
			const_cast<const volatile StorageType&>(this->value_)); \
	ValueType next; \
	do { \
		next = last opSymbol operand; \
	} \
	while (!this->compareExchange(last, next))

#define UTIL_ATOMIC_DETAIL_FETCH_AND_OP_BY_CAS_RET(opSymbol, operand) \
	UTIL_ATOMIC_DETAIL_FETCH_AND_OP_BY_CAS(opSymbol, operand); \
	return last

#define UTIL_ATOMIC_DETAIL_OP_AND_FETCH_BY_CAS(opSymbol, operand) \
	ValueType last = AccessorType::toValueType( \
			const_cast<const volatile StorageType&>(this->value_)); \
	ValueType next; \
	do { \
		next = last opSymbol operand; \
	} \
	while (!this->compareExchange(last, next))

#define UTIL_ATOMIC_DETAIL_OP_AND_FETCH_BY_CAS_RET(opSymbol, operand) \
	UTIL_ATOMIC_DETAIL_OP_AND_FETCH_BY_CAS(opSymbol, operand); \
	return next

template<typename T>
class AtomicIntegerBase : public AtomicBase< T, AtomicIntegerBase<T> > {


public:
	typedef AtomicIntegerBase<T> ThisType;
	typedef AtomicBase< T, AtomicIntegerBase<T> > BaseType;
	typedef typename BaseType::ValueType ValueType;
	typedef typename BaseType::AccessorType AccessorType;
	typedef typename BaseType::StorageType StorageType;

	AtomicIntegerBase() {}
	~AtomicIntegerBase() {}
	AtomicIntegerBase(ValueType value) : BaseType(value) {}

	ThisType& operator=(ValueType value) {
		BaseType::operator=(value);
		return *this;
	}

	ValueType fetchAdd(ValueType value) volatile {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS)
		return __sync_fetch_and_add(&this->value_, value);
#else
		UTIL_ATOMIC_DETAIL_FETCH_AND_OP_BY_CAS_RET(+, value);
#endif
	}

	ValueType fetchSub(ValueType value) volatile {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS)
		return __sync_fetch_and_sub(&this->value_, value);
#else
		UTIL_ATOMIC_DETAIL_FETCH_AND_OP_BY_CAS_RET(-, value);
#endif
	}

	ValueType fetchAnd(ValueType value) volatile {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS)
		return __sync_fetch_and_and(&this->value_, value);
#else
		UTIL_ATOMIC_DETAIL_FETCH_AND_OP_BY_CAS_RET(&, value);
#endif
	}

	ValueType fetchOr(ValueType value) volatile {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS)
		return __sync_fetch_and_or(&this->value_, value);
#else
		UTIL_ATOMIC_DETAIL_FETCH_AND_OP_BY_CAS_RET(|, value);
#endif
	}

	ValueType fetchXor(ValueType value) volatile {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS)
		return __sync_fetch_and_xor(&this->value_, value);
#else
		UTIL_ATOMIC_DETAIL_FETCH_AND_OP_BY_CAS_RET(^, value);
#endif
	}

	ThisType operator++(int) volatile {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS)
		return __sync_fetch_and_add(&this->value_, 1);
#else
		UTIL_ATOMIC_DETAIL_FETCH_AND_OP_BY_CAS_RET(+, 1);
#endif
	}

	ThisType operator--(int) volatile {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS)
		return __sync_fetch_and_sub(&this->value_, 1);
#else
		UTIL_ATOMIC_DETAIL_FETCH_AND_OP_BY_CAS_RET(-, 1);
#endif
	}

	ThisType operator++() volatile {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS)
		return __sync_add_and_fetch(&this->value_, 1);
#else
		UTIL_ATOMIC_DETAIL_OP_AND_FETCH_BY_CAS_RET(+, 1);
#endif
	}

	ThisType operator--() volatile {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS)
		return __sync_sub_and_fetch(&this->value_, 1);
#else
		UTIL_ATOMIC_DETAIL_OP_AND_FETCH_BY_CAS_RET(-, 1);
#endif
	}

	ThisType operator+=(ValueType value) volatile {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS)
		return __sync_add_and_fetch(&this->value_, value);
#else
		UTIL_ATOMIC_DETAIL_OP_AND_FETCH_BY_CAS_RET(+, value);
#endif
	}

	ThisType operator-=(ValueType value) volatile {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS)
		return __sync_sub_and_fetch(&this->value_, value);
#else
		UTIL_ATOMIC_DETAIL_OP_AND_FETCH_BY_CAS_RET(-, value);
#endif
	}

	ThisType operator&=(ValueType value) volatile {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS)
		return __sync_and_and_fetch(&this->value_, value);
#else
		UTIL_ATOMIC_DETAIL_OP_AND_FETCH_BY_CAS_RET(&, value);
#endif
	}

	ThisType operator|=(ValueType value) volatile {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS)
		return __sync_or_and_fetch(&this->value_, value);
#else
		UTIL_ATOMIC_DETAIL_OP_AND_FETCH_BY_CAS_RET(|, value);
#endif
	}

	ThisType operator^=(ValueType value) volatile {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS)
		return __sync_xor_and_fetch(&this->value_, value);
#else
		UTIL_ATOMIC_DETAIL_OP_AND_FETCH_BY_CAS_RET(^, value);
#endif
	}
};

template<typename T>
class AtomicPointerBase : public AtomicBase< T, AtomicPointerBase<T> > {
public:
	typedef AtomicPointerBase<T> ThisType;
	typedef AtomicBase< T, AtomicPointerBase<T> > BaseType;
	typedef typename BaseType::ValueType ValueType;
	typedef typename BaseType::StorageType StorageType;

	AtomicPointerBase() {}
	~AtomicPointerBase() {}
	AtomicPointerBase(ValueType value) : BaseType(value) {}

	ThisType& operator=(ValueType value) {
		BaseType::operator=(value);
		return *this;
	}

	ValueType fetchAdd(ptrdiff_t value) volatile {
		UTIL_ATOMIC_DETAIL_FETCH_AND_OP_BY_CAS_RET(+, value);
	}

	ValueType fetchSub(ptrdiff_t value) volatile {
		UTIL_ATOMIC_DETAIL_FETCH_AND_OP_BY_CAS_RET(-, value);
	}

	ThisType operator++(int) volatile {
		UTIL_ATOMIC_DETAIL_FETCH_AND_OP_BY_CAS_RET(+, 1);
	}

	ThisType operator--(int) volatile {
		UTIL_ATOMIC_DETAIL_FETCH_AND_OP_BY_CAS_RET(-, 1);
	}

	ThisType operator++() volatile {
		UTIL_ATOMIC_DETAIL_OP_AND_FETCH_BY_CAS_RET(+, 1);
	}

	ThisType operator--() volatile {
		UTIL_ATOMIC_DETAIL_OP_AND_FETCH_BY_CAS_RET(-, 1);
	}

	ThisType operator+=(ptrdiff_t value) volatile {
		UTIL_ATOMIC_DETAIL_OP_AND_FETCH_BY_CAS_RET(+, value);
	}

	ThisType operator-=(ptrdiff_t value) volatile {
		UTIL_ATOMIC_DETAIL_OP_AND_FETCH_BY_CAS_RET(-, value);
	}

private:
	friend class AtomicBase< T, AtomicPointerBase<T> >;
	typedef typename BaseType::AccessorType AccessorType;

	inline static StorageType toStorageType(ValueType value) {
		return static_cast<StorageType>(reinterpret_cast<uintptr_t>(value));
	}

	inline static ValueType toValueType(StorageType value) {
		return reinterpret_cast<ValueType>(static_cast<uintptr_t>(value));
	}
};

template<typename T = bool>
class AtomicBoolBase : public AtomicBase< T, AtomicBoolBase<T> > {
public:
	typedef AtomicBoolBase<T> ThisType;
	typedef AtomicBase< T, AtomicBoolBase<T> > BaseType;
	typedef typename BaseType::ValueType ValueType;
	typedef typename BaseType::StorageType StorageType;

	AtomicBoolBase() {}
	AtomicBoolBase(ValueType value) : BaseType(value) {}

	ThisType& operator=(ValueType value) {
		BaseType::operator=(value);
		return *this;
	}

	ValueType fetchAnd(ValueType value) volatile {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS)
		return __sync_fetch_and_and(&this->value_, value);
#else
		UTIL_ATOMIC_DETAIL_FETCH_AND_OP_BY_CAS_RET(&, value);
#endif
	}

	ValueType fetchOr(ValueType value) volatile {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS)
		return __sync_fetch_and_or(&this->value_, value);
#else
		UTIL_ATOMIC_DETAIL_FETCH_AND_OP_BY_CAS_RET(|, value);
#endif
	}

	ValueType fetchXor(ValueType value) volatile {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS)
		return __sync_fetch_and_xor(&this->value_, value);
#else
		UTIL_ATOMIC_DETAIL_FETCH_AND_OP_BY_CAS_RET(^, value);
#endif
	}

	ThisType operator&=(ValueType value) volatile {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS)
		return __sync_and_and_fetch(&this->value_, value);
#else
		UTIL_ATOMIC_DETAIL_OP_AND_FETCH_BY_CAS_RET(&, value);
#endif
	}

	ThisType operator|=(ValueType value) volatile {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS)
		return __sync_or_and_fetch(&this->value_, value);
#else
		UTIL_ATOMIC_DETAIL_OP_AND_FETCH_BY_CAS_RET(|, value);
#endif
	}

	ThisType operator^=(ValueType value) volatile {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS)
		return __sync_xor_and_fetch(&this->value_, value);
#else
		UTIL_ATOMIC_DETAIL_OP_AND_FETCH_BY_CAS_RET(^, value);
#endif
	}

protected:
	~AtomicBoolBase() {}

private:
	friend class AtomicBase< T, AtomicBoolBase<T> >;
	typedef typename BaseType::AccessorType AccessorType;

	inline static ValueType toValueType(StorageType value) {
		return !!value;
	}
};

template<typename T>
struct AtomicUtil::StorageTypeResolver<T, 1> { typedef uint32_t Result; };
template<typename T>
struct AtomicUtil::StorageTypeResolver<T, 2> { typedef uint32_t Result; };
template<typename T>
struct AtomicUtil::StorageTypeResolver<T, 4> { typedef uint32_t Result; };
template<typename T>
struct AtomicUtil::StorageTypeResolver<T, 8> { typedef uint64_t Result; };

#ifdef _WIN32
extern "C" {
#ifdef _WIN64
__int64 _InterlockedExchange64(volatile __int64* Target, __int64 Value);
__int64 _InterlockedCompareExchange64(volatile __int64* Destination,
		__int64 Exchange, __int64 Comperand);

__int64 _InterlockedIncrement64(volatile __int64* lpAddend);
__int64 _InterlockedDecrement64(volatile __int64* lpAddend);

long _InterlockedExchange(
		volatile long* Target, long Value);
long _InterlockedCompareExchange(
		volatile long* Destination, long Exchange, long Comperand);

long _InterlockedIncrement(volatile long* lpAddend);
long _InterlockedDecrement(volatile long* lpAddend);

#ifdef _MSC_VER
#pragma intrinsic(_InterlockedIncrement)
#pragma intrinsic(_InterlockedDecrement)
#pragma intrinsic(_InterlockedExchange)
#pragma intrinsic(_InterlockedCompareExchange)
#endif

#define InterlockedIncrement _InterlockedIncrement
#define InterlockedDecrement _InterlockedDecrement
#define InterlockedExchange _InterlockedExchange
#define InterlockedCompareExchange _InterlockedCompareExchange
#else
__declspec(dllimport) long __stdcall InterlockedExchange(
		volatile long* Target, long Value);
__declspec(dllimport) long __stdcall InterlockedCompareExchange(
		volatile long* Destination, long Exchange, long Comperand);

__declspec(dllimport) long __stdcall InterlockedIncrement(
		volatile long* lpAddend);
__declspec(dllimport) long __stdcall InterlockedDecrement(
		volatile long* lpAddend);
#endif 
}
#endif 

inline void AtomicUtil::fenceBefore() {
}

inline void AtomicUtil::fenceAfter() {
}

inline void AtomicUtil::fenceBeforeStore() {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS)
	__sync_synchronize();
#elif defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_X86)
	__asm__ __volatile__ ("" ::: "memory");
#elif defined(UTIL_ATOMIC_DETAIL_TYPE_WIN32)
#endif
}

inline void AtomicUtil::fenceAfterStore() {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS)
	__sync_synchronize();
#elif defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_X86)
	__asm__ __volatile__ ("" ::: "memory");
#elif defined(UTIL_ATOMIC_DETAIL_TYPE_WIN32)
	long tmp;
	InterlockedExchange(&tmp, 0);
#endif
}

inline void AtomicUtil::fenceAfterLoad() {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS)
	__sync_synchronize();
#elif defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_X86)
	__asm__ __volatile__ ("" ::: "memory");
#elif defined(UTIL_ATOMIC_DETAIL_TYPE_WIN32)
	long tmp;
	InterlockedExchange(&tmp, 0);
#endif
}

template<typename S>
struct AtomicUtil::CompareExchanger<S, 4> {
	inline bool operator()(S &expected, S desired, volatile S *target) {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS)
		const S previous = __sync_val_compare_and_swap(target, expected, desired);
		const bool succeeded = (previous == expected);
		expected = previous;
		return succeeded;
#elif defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_X86)
		S previous = expected;
		__asm__ (
			"lock ; cmpxchgl %2, %1"
			: "+a" (previous), "+m" (target)
			: "q" (desired)
		);
		const bool succeeded = (previous == expected);
		expected = previous;
		return succeeded;
#elif defined(UTIL_ATOMIC_DETAIL_TYPE_WIN32)
		S previous = expected;
		expected = (S) InterlockedCompareExchange(
				(volatile long*)target, (long) desired, (long) expected);
		return (previous == expected);
#else
#error 0
#endif
	}
};

template<typename S>
struct AtomicUtil::CompareExchanger<S, 8> {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_WIN32) && !defined(_WIN64)
	__forceinline static int64_t compareExchange64Direct(
			volatile int64_t *target, int64_t expected, int64_t desired) {
		int64_t previous;
		__asm {
			mov esi, target
			mov eax, dword ptr expected
			mov edx, dword ptr expected + 4
			mov ebx, dword ptr desired
			mov ecx, dword ptr desired + 4
			lock cmpxchg8b qword ptr [esi]
			mov dword ptr previous, eax
			mov dword ptr previous + 4, edx
		}
		return previous;
	}
#endif

	inline bool operator()(S &expected, S desired, volatile S *target) {
#if defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_BUILTINS)
		const S previous =
				__sync_val_compare_and_swap(target, expected, desired);
		const bool succeeded = (previous == expected);
		expected = previous;
		return succeeded;
#elif defined(UTIL_ATOMIC_DETAIL_TYPE_GCC_X86)
		S previous = expected;
		__asm__ (
			"lock ; cmpxchgq %2, %1"
			: "+a" (previous), "+m" (target)
			: "q" (desired)
		);
		const bool succeeded = (previous == expected);
		expected = previous;
		return succeeded;
#elif defined(UTIL_ATOMIC_DETAIL_TYPE_WIN32)
#ifdef _WIN64
		S previous = expected;
		expected = (S) _InterlockedCompareExchange64(
				(volatile int64_t*) target, (int64_t) desired, (int64_t) expected);
		return (previous == expected);
#else
		S previous = expected;
		expected = (S) compareExchange64Direct(
				(volatile int64_t*) target, (int64_t) expected, (int64_t) desired);
		return (previous == expected);
#endif 
#else
#error 0
#endif
	}
};

template<typename T>
struct AtomicImplResolver { typedef AtomicUtil::BadType Result; };

template<> struct AtomicImplResolver<char> {
	typedef AtomicIntegerBase<char> Result;
};
template<> struct AtomicImplResolver<unsigned char> {
	typedef AtomicIntegerBase<unsigned char> Result;
};
template<> struct AtomicImplResolver<signed char> {
	typedef AtomicIntegerBase<signed char> Result;
};
template<> struct AtomicImplResolver<short> {
	typedef AtomicIntegerBase<short> Result;
};
template<> struct AtomicImplResolver<unsigned short> {
	typedef AtomicIntegerBase<unsigned short> Result;
};
template<> struct AtomicImplResolver<int> {
	typedef AtomicIntegerBase<int> Result;
};
template<> struct AtomicImplResolver<unsigned int> {
	typedef AtomicIntegerBase<unsigned int> Result;
};
template<> struct AtomicImplResolver<long> {
	typedef AtomicIntegerBase<long> Result;
};
template<> struct AtomicImplResolver<unsigned long> {
	typedef AtomicIntegerBase<unsigned long> Result;
};
template<> struct AtomicImplResolver<long long> {
	typedef AtomicIntegerBase<long long> Result;
};
template<> struct AtomicImplResolver<unsigned long long> {
	typedef AtomicIntegerBase<unsigned long long> Result;
};
template<> struct AtomicImplResolver<void*> {
	typedef AtomicPointerBase<void*> Result;
};
template<typename E> struct AtomicImplResolver<E*> {
	typedef AtomicPointerBase<E*> Result;
};
template<> struct AtomicImplResolver<bool> {
	typedef AtomicBoolBase<> Result;
};

typedef Atomic<uint32_t> AtomicUInt32;

} 


/*!
    @brief Detects conflictions.
*/
template<bool Throwable>
class ConflictionDetectorBase {
public:
	ConflictionDetectorBase();
	~ConflictionDetectorBase();

	void enter();
	void leave();

private:
	friend class ConflictionDetectorScope;

	ConflictionDetectorBase(const ConflictionDetectorBase&);
	ConflictionDetectorBase& operator=(const ConflictionDetectorBase&);

	void errorEntering();
	void errorLeaving();

	Atomic<int32_t> counter_;
};

typedef ConflictionDetectorBase<true> ConflictionDetector;
typedef ConflictionDetectorBase<false> NoThrowConflictionDetector;

/*!
    @brief Scope of ConflictionDetector for seting in the clitical section.
*/
class ConflictionDetectorScope {
public:
	ConflictionDetectorScope(ConflictionDetector &detector, bool entering);
	~ConflictionDetectorScope();

private:
	ConflictionDetectorScope(const ConflictionDetectorScope&);
	ConflictionDetectorScope& operator=(const ConflictionDetectorScope&);

	ConflictionDetector &detector_;
	bool entering_;
};


template<bool Throwable>
inline ConflictionDetectorBase<Throwable>::ConflictionDetectorBase() {
}

template<bool Throwable>
inline ConflictionDetectorBase<Throwable>::~ConflictionDetectorBase() {
}

template<bool Throwable>
inline void ConflictionDetectorBase<Throwable>::enter() {
	if (++counter_ != 1) {
		errorEntering();
	}
}

template<bool Throwable>
inline void ConflictionDetectorBase<Throwable>::leave() {
	if (--counter_ != 0) {
		errorLeaving();
	}
}

} 

#endif
