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
/*!
	@file
    @brief Definition of Utility of allocators
*/
#ifndef UTIL_MEMORY_H_
#define UTIL_MEMORY_H_

#if defined(max) || defined(min)
#error "windows.h (or winsock2.h etc) may be included without NOMINMAX"
#endif

#include "util/type.h"
#include "util/thread.h"
#include <limits>
#include <cstdlib>
#include <cassert>
#include <algorithm> 


#ifndef UTIL_ALLOCATOR_EMPTY_ALLOCATOR_CONSTRUCTOR_ALLOWED
#ifdef __GNUC__
#if __GNUC__ >= 8 || \
	defined(_GLIBCXX_FULLY_DYNAMIC_STRING) && _GLIBCXX_FULLY_DYNAMIC_STRING
#define UTIL_ALLOCATOR_EMPTY_ALLOCATOR_CONSTRUCTOR_ALLOWED 0
#else
#define UTIL_ALLOCATOR_EMPTY_ALLOCATOR_CONSTRUCTOR_ALLOWED 1
#endif
#else
#define UTIL_ALLOCATOR_EMPTY_ALLOCATOR_CONSTRUCTOR_ALLOWED 0
#endif
#endif

#ifndef UTIL_ALLOCATOR_BASIC_STRING_ALTER_MODIFIERS
#ifdef __GNUC__
#define UTIL_ALLOCATOR_BASIC_STRING_ALTER_MODIFIERS 0
#else
#define UTIL_ALLOCATOR_BASIC_STRING_ALTER_MODIFIERS 1
#endif
#endif

#ifndef UTIL_ALLOCATOR_FORCE_ALLOCATOR_INFO
#define UTIL_ALLOCATOR_FORCE_ALLOCATOR_INFO 1
#endif

#ifndef UTIL_ALLOCATOR_BASIC_STRING_SUBSTRING_ENABLED
#define UTIL_ALLOCATOR_BASIC_STRING_SUBSTRING_ENABLED 0
#endif

#ifndef UTIL_ALLOCATOR_FIXED_ALLOCATOR_RESERVE_UNIT_SMALL
#define UTIL_ALLOCATOR_FIXED_ALLOCATOR_RESERVE_UNIT_SMALL 1
#endif

#ifndef UTIL_ALLOCATOR_VAR_ALLOCATOR_POOLING
#define UTIL_ALLOCATOR_VAR_ALLOCATOR_POOLING 1
#endif

#ifndef UTIL_ALLOCATOR_PRIOR_REQUESTER_STATS
#define UTIL_ALLOCATOR_PRIOR_REQUESTER_STATS 1
#endif

#ifndef UTIL_ALLOCATOR_NO_CACHE
#define UTIL_ALLOCATOR_NO_CACHE 0
#endif

#ifndef UTIL_ALLOCATOR_NO_CACHE_FIXED_ALLOCATOR
#define UTIL_ALLOCATOR_NO_CACHE_FIXED_ALLOCATOR UTIL_ALLOCATOR_NO_CACHE
#endif

#ifndef UTIL_ALLOCATOR_NO_CACHE_STACK_ALLOCATOR
#define UTIL_ALLOCATOR_NO_CACHE_STACK_ALLOCATOR UTIL_ALLOCATOR_NO_CACHE
#endif

#ifndef UTIL_ALLOCATOR_SUBSTITUTE_FIXED_ALLOCATOR
#define UTIL_ALLOCATOR_SUBSTITUTE_FIXED_ALLOCATOR 0
#endif

#ifndef UTIL_ALLOCATOR_SUBSTITUTE_VAR_ALLOCATOR
#define UTIL_ALLOCATOR_SUBSTITUTE_VAR_ALLOCATOR 0
#endif

#ifndef UTIL_ALLOCATOR_SUBSTITUTE_STACK_ALLOCATOR
#define UTIL_ALLOCATOR_SUBSTITUTE_STACK_ALLOCATOR 0
#endif

#ifndef UTIL_ALLOCATOR_DEBUG_REPORTER_ENABLED
#ifdef NDEBUG
#define UTIL_ALLOCATOR_DEBUG_REPORTER_ENABLED 0
#else
#define UTIL_ALLOCATOR_DEBUG_REPORTER_ENABLED 1
#endif
#endif

#ifndef UTIL_ALLOCATOR_REPORTER_ENABLED
#define UTIL_ALLOCATOR_REPORTER_ENABLED 0
#endif

#ifndef UTIL_ALLOCATOR_REPORTER_ENABLED2
#define UTIL_ALLOCATOR_REPORTER_ENABLED2 0
#endif
#if UTIL_ALLOCATOR_REPORTER_ENABLED2
#include <map>
#include <iostream>
#endif

#ifndef UTIL_ALLOCATOR_REPORTER_ENABLED3
#define UTIL_ALLOCATOR_REPORTER_ENABLED3 0
#if UTIL_ALLOCATOR_REPORTER_ENABLED3
#include <iostream> 
extern int64_t g_limitCount;
extern int64_t g_limitSize;
extern int64_t g_limitCountCount;
extern int64_t g_limitSizeCount;
#endif
#endif

#ifndef UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
#define UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED 0
#endif

#ifndef UTIL_ALLOCATOR_DIFF_REPORTER_ENABLED
#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
#define UTIL_ALLOCATOR_DIFF_REPORTER_ENABLED 1
#else
#define UTIL_ALLOCATOR_DIFF_REPORTER_ENABLED 0
#endif
#endif

#ifndef UTIL_ALLOCATOR_CHECK_CONFLICTION
#define UTIL_ALLOCATOR_CHECK_CONFLICTION 0
#endif

#ifndef UTIL_ALLOCATOR_CHECK_ALLOCATOR_CONFLICTION
#define UTIL_ALLOCATOR_CHECK_ALLOCATOR_CONFLICTION \
	UTIL_ALLOCATOR_CHECK_CONFLICTION
#endif

#ifndef UTIL_ALLOCATOR_CHECK_STAT_CONFLICTION
#define UTIL_ALLOCATOR_CHECK_STAT_CONFLICTION \
	UTIL_ALLOCATOR_CHECK_CONFLICTION
#endif


#if UTIL_ALLOCATOR_CHECK_STAT_CONFLICTION || \
	UTIL_ALLOCATOR_CHECK_ALLOCATOR_CONFLICTION
#define UTIL_ALLOCATOR_DETAIL_CONFLICTION_DETECTOR_ENABLED 1
#else
#define UTIL_ALLOCATOR_DETAIL_CONFLICTION_DETECTOR_ENABLED 0
#endif

#if UTIL_ALLOCATOR_CHECK_ALLOCATOR_CONFLICTION
#define UTIL_ALLOCATOR_DETAIL_LOCK_GUARD_STACK_ALLOCATOR(alloc) \
	LockGuard<MutexType> guard((alloc)->mutex_)
#else
#define UTIL_ALLOCATOR_DETAIL_LOCK_GUARD_STACK_ALLOCATOR(alloc)
#endif

#if UTIL_ALLOCATOR_CHECK_STAT_CONFLICTION
#define UTIL_ALLOCATOR_DETAIL_STAT_GUARD(guard, mutex) \
	LockGuard<Mutex> &guard(*static_cast<LockGuard<Mutex>*>(NULL)); \
	static_cast<void>(mutex); \
	static_cast<void>(guard)
#define UTIL_ALLOCATOR_DETAIL_STAT_GUARD_STACK_ALLOCATOR(alloc) \
	UTIL_ALLOCATOR_DETAIL_STAT_GUARD(guard, (alloc)->mutex_)
#else
#define UTIL_ALLOCATOR_DETAIL_STAT_GUARD(guard, mutex) \
	LockGuard<Mutex> guard(mutex)
#define UTIL_ALLOCATOR_DETAIL_STAT_GUARD_STACK_ALLOCATOR(alloc)
#endif 

#ifndef UTIL_ALLOC_CXX_MOVE_ENABLED
#define UTIL_ALLOC_CXX_MOVE_ENABLED 0
#endif

namespace util {

/*!
	@brief Substitutes mutex object for Allocators.
*/
class NoopMutex {
public:
	inline NoopMutex() {}
	inline ~NoopMutex() {}

	inline bool tryLock(void) { lock(); return true; }
	inline bool tryLock(uint32_t) { lock(); return true; }

#if UTIL_ALLOCATOR_DETAIL_CONFLICTION_DETECTOR_ENABLED

public:
	inline void lock(void) { detector_.enter(); }
	inline void unlock(void) { detector_.leave(); }
private:
	NoThrowConflictionDetector detector_;

#else

public:
	inline void lock(void) {}
	inline void unlock(void) {}

#endif 
};

/*!
	@brief ErrorHandler of allocation errors for customization.
*/
class AllocationErrorHandler {
public:
	virtual ~AllocationErrorHandler();
	virtual void operator()(Exception &e) = 0;
};

#if UTIL_ALLOCATOR_DEBUG_REPORTER_ENABLED
struct AllocatorStats;
namespace detail {
class AllocatorDebugReporter {
public:
	static void reportUnexpectedUsage(
			const AllocatorStats &stats, size_t total, size_t free);
};
}
#endif 

#if UTIL_ALLOCATOR_REPORTER_ENABLED
namespace detail {
class AllocatorReporter {
public:
	AllocatorReporter();
	~AllocatorReporter();

	void reportMissHit(size_t elementSize, size_t totalSize, size_t freeSize);

private:
	static util::Atomic<uint64_t> reporterCount_;

	const uint64_t id_;
	uint64_t repeat_;
	std::string reporterLocation_;
};
}
#define UTIL_DETAIL_ALLOCATOR_DECLARE_REPORTER \
		util::detail::AllocatorReporter allocatorReporter_
#define UTIL_DETAIL_ALLOCATOR_REPORT_MISS_HIT( \
		elementSize, totalSize, freeSize) \
		allocatorReporter_.reportMissHit(elementSize, totalSize, freeSize)
#else
#define UTIL_DETAIL_ALLOCATOR_DECLARE_REPORTER
#define UTIL_DETAIL_ALLOCATOR_REPORT_MISS_HIT( \
		elementSize, totalSize, freeSize)
#endif	

typedef int32_t AllocatorGroupId;
class AllocatorManager;
class AllocatorLimitter;

/*!
	@brief Manages information of allocator.
*/
class AllocatorInfo {
public:
	class ManagerInside {
		friend class AllocatorManager;
	};

	AllocatorInfo(AllocatorGroupId groupId, const char8_t *nameLiteral,
			AllocatorManager *manager = NULL);

	AllocatorInfo();

	AllocatorGroupId getGroupId() const;
	const char8_t* getName() const;
	AllocatorManager& resolveManager() const;

	size_t getUnitSize() const;
	void setUnitSize(size_t size);

	void format(
			std::ostream &stream, bool partial = false, bool nameOnly = false,
			bool withUnit = true, const ManagerInside *inside = NULL) const;

	static void formatUnitSize(std::ostream &stream, int64_t size, bool exact);

private:
	AllocatorGroupId groupId_;
	const char8_t *nameLiteral_;
	AllocatorManager *manager_;
	size_t unitSize_;
};

std::ostream& operator<<(std::ostream &stream, const AllocatorInfo &info);

struct AllocatorStats {
	enum Type {
		STAT_TOTAL_SIZE,
		STAT_PEAK_TOTAL_SIZE,
		STAT_CACHE_SIZE,
		STAT_CACHE_MISS_COUNT,
		STAT_CACHE_ADJUST_COUNT,
		STAT_HUGE_ALLOCATION_COUNT,
		STAT_ALLOCATION_COUNT,
		STAT_DEALLOCATION_COUNT,
		STAT_GROUP_TOTAL_LIMIT,
		STAT_CACHE_LIMIT,
		STAT_STABLE_LIMIT,
		STAT_TYPE_END
	};

	explicit AllocatorStats(const AllocatorInfo &info = AllocatorInfo());
	void merge(const AllocatorStats &stats);

	static int64_t asStatValue(size_t value);

	AllocatorInfo info_;
	int64_t values_[STAT_TYPE_END];
};

class AllocatorDiffReporter {
public:
	class Scope;
	class ActivationSaver;

	AllocatorDiffReporter();
	~AllocatorDiffReporter();

	void setGroup(AllocatorGroupId groupId);
	void setNamePrefix(const char8_t *prefix);

	void start(const AllocatorInfo &startInfo);
	void finish(std::ostream *out = NULL);

private:
	struct Body;
	struct ActivationState;

	Body& prepare();
	void clear();

	static std::ostream& resolveOutput(std::ostream *out);

	Body *body_;
};

class AllocatorDiffReporter::Scope {
public:
	Scope(AllocatorDiffReporter &base, const AllocatorInfo &startInfo);
	~Scope();

private:
	AllocatorDiffReporter &base_;
};

class AllocatorDiffReporter::ActivationSaver {
public:
	typedef AllocatorInfo::ManagerInside ManagerInside;

	ActivationSaver();
	~ActivationSaver();

	void onAllocated(void *ptr);
	void onDeallocated(void *ptr);

	void saveSnapshot(size_t id);
	void removeSnapshot(size_t id);
	void compareSnapshot(
			size_t id, const AllocatorInfo &info, std::ostream *out,
			const ManagerInside *inside = NULL);

private:
	ActivationState& prepare();
	void clear();

	ActivationState *state_;
};

class AllocatorCleanUpHandler {
public:
	void bind(AllocatorCleanUpHandler *&another, NoopMutex*);
	void bind(AllocatorCleanUpHandler *&another, Mutex *mutex);

	void unbind() throw();

	static void cleanUpAll(AllocatorCleanUpHandler *&handler) throw();

protected:
	AllocatorCleanUpHandler();
	virtual ~AllocatorCleanUpHandler() = 0;

	virtual void operator()() throw() = 0;

private:
	AllocatorCleanUpHandler(const AllocatorCleanUpHandler&);
	AllocatorCleanUpHandler& operator=(const AllocatorCleanUpHandler&);

	AllocatorCleanUpHandler **prev_;
	AllocatorCleanUpHandler *next_;

	Mutex *mutex_;
};

namespace detail {
class AllocationRequester {
public:
	enum {
		REQUESTER_ENABLED = true
	};
	explicit AllocationRequester(AllocatorLimitter *limitter);

	bool acquire(size_t size, AllocatorLimitter *base) const;
	bool release(size_t size, AllocatorLimitter *base) const;

private:
	AllocatorLimitter *limitter_;
};

class EmptyAllocationRequester {
public:
	enum {
		REQUESTER_ENABLED = false
	};
	bool acquire(size_t, AllocatorLimitter*) const { return false; }
	bool release(size_t, AllocatorLimitter*) const { return false; }
};
} 

/*!
	@brief Allocates fixed size memory.
*/
template<typename Mutex = NoopMutex>
class FixedSizeAllocator {
public:
	struct ElementHead;

#if !UTIL_ALLOCATOR_FORCE_ALLOCATOR_INFO
	explicit FixedSizeAllocator(size_t elementSize);
#endif

	FixedSizeAllocator(const AllocatorInfo &info, size_t elementSize);

	~FixedSizeAllocator();


	void* allocate();
	template<typename R> void* allocate(const R &requester);

	void deallocate(void *element);
	template<typename R> void deallocate(void *element, const R &requester);


	void setTotalElementLimit(size_t limit);
	void setFreeElementLimit(size_t limit);

	void setErrorHandler(AllocationErrorHandler *errorHandler);
	void addCleanUpHandler(AllocatorCleanUpHandler &cleanUpHandler);

	Mutex& getLock();


	size_t getTotalElementLimit();
	size_t getFreeElementLimit();

	size_t getElementSize();
	size_t getTotalElementCount();
	size_t getFreeElementCount();


	void getStats(AllocatorStats &stats);
	void setLimit(AllocatorStats::Type type, size_t value);
	AllocatorLimitter* setLimit(
			AllocatorStats::Type type, AllocatorLimitter *limitter,
			bool force = false);

#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
	class ManagerTool {
		friend class AllocatorManager;
		typedef Mutex MutexType;
		static AllocatorDiffReporter::ActivationSaver& getActivationSaver(
				FixedSizeAllocator &alloc) {
			return alloc.activationSaver_;
		}
		static MutexType& getLock(FixedSizeAllocator &alloc) {
			return alloc.mutex_;
		}
	};
#endif

private:
	typedef detail::FreeLink FreeLink;

	FixedSizeAllocator(const FixedSizeAllocator&);
	FixedSizeAllocator& operator=(const FixedSizeAllocator&);

	void reserve();
	void clear(size_t preservedCount);

	template<typename R> void acquireForRequester(const R &requester);
	template<typename R> void releaseForRequester(const R &requester);

	const size_t elementSize_;
	size_t totalElementLimit_;
	size_t freeElementLimit_;
	size_t stableElementLimit_;
	AllocationErrorHandler *errorHandler_;

	FreeLink *freeLink_;

	size_t totalElementCount_;
	size_t freeElementCount_;

	Mutex mutex_;
	UTIL_DETAIL_ALLOCATOR_DECLARE_REPORTER;

#if UTIL_ALLOCATOR_REPORTER_ENABLED2
	std::map<void*, std::string> stackTraceMap_;
#endif

#if UTIL_ALLOCATOR_REPORTER_ENABLED3
	int64_t limitSizeOverCount_;
	int64_t limitCallOverCount_;
	int64_t callCount_;
#endif

#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
	AllocatorDiffReporter::ActivationSaver activationSaver_;
#endif

	AllocatorCleanUpHandler *cleanUpHandler_;

	AllocatorStats stats_;
	AllocatorLimitter *limitter_;
};

class FixedSizeCachedAllocator {
public:
	typedef FixedSizeAllocator<> BaseAllocator;
	typedef FixedSizeAllocator<Mutex> LockedBaseAllocator;

	FixedSizeCachedAllocator(
			const AllocatorInfo &localInfo, BaseAllocator *base,
			Mutex *sharedMutex, bool locked, size_t elementSize);
	~FixedSizeCachedAllocator();

	template<typename L> void* allocate(L *mutex);
	template<typename L> void deallocate(L *mutex, void *element);

	void setErrorHandler(AllocationErrorHandler *errorHandler);

	BaseAllocator* base(const NoopMutex*);
	LockedBaseAllocator* base(const Mutex*);

	size_t getElementSize();
	size_t getTotalElementCount();
	size_t getFreeElementCount();

	template<typename L>
	void setLimit(L *mutex, AllocatorStats::Type type, size_t value);

	static void applyFreeElementLimit(BaseAllocator &baseAlloc, size_t scale);


	void getStats(AllocatorStats &stats);
	void setLimit(AllocatorStats::Type type, size_t value);
	AllocatorLimitter* setLimit(
			AllocatorStats::Type type, AllocatorLimitter *limitter,
			bool force = false);

private:
	typedef detail::FreeLink FreeLink;

	bool findLocal(const NoopMutex*);
	bool findLocal(const Mutex*);

	void* allocateLocal(const NoopMutex*);
	void* allocateLocal(const Mutex*);

	void deallocateLocal(const NoopMutex*, void *element);
	void deallocateLocal(const Mutex*, void *element);

	void setLimitLocal(
			const NoopMutex*, AllocatorStats::Type type, size_t value);
	void setLimitLocal(const Mutex*, AllocatorStats::Type type, size_t value);

	void reserve();
	void shrink();
	void clear(size_t preservedCount);

	size_t getStableElementLimit();
	static size_t resolveFreeElementLimit(BaseAllocator *base);

	template<typename L>
	static FixedSizeAllocator<L>* tryPrepareLocal(
			const AllocatorInfo &info, BaseAllocator *base, bool locked,
			size_t elementSize);

	AllocatorStats stats_;

	FreeLink *freeLink_;
	size_t freeElementCount_;

	size_t freeElementLimit_;

	UTIL_UNIQUE_PTR<BaseAllocator> localAlloc_;
	UTIL_UNIQUE_PTR<LockedBaseAllocator> localLockedAlloc_;

	BaseAllocator *base_;
	Mutex *sharedMutex_;
	bool shared_;
};

class VariableSizeAllocatorPool;

template<
		size_t SmallSize = 128,
		size_t MiddleSize = 1024 * 4,
		size_t LargeSize = 1024 * 1024>
struct VariableSizeAllocatorTraits {
public:
	explicit VariableSizeAllocatorTraits(
			VariableSizeAllocatorPool *pool = NULL) :
			pool_(pool) {
	}

	static const size_t FIXED_ALLOCATOR_COUNT = 3;
	static size_t getFixedSize(size_t index);
	static size_t selectFixedAllocator(size_t size);

	VariableSizeAllocatorPool* getPool() const { return pool_; }

private:
	VariableSizeAllocatorPool *pool_;
};

/*!
	@brief Allocates variable size memory.
*/
template<
		typename Mutex = NoopMutex,
		typename Traits = VariableSizeAllocatorTraits<> >
class VariableSizeAllocator {
public:
	typedef Mutex MutexType;
	typedef Traits TraitsType;
	typedef FixedSizeAllocator<Mutex> BaseAllocator;

	static const size_t FIXED_ALLOCATOR_COUNT =
			Traits::FIXED_ALLOCATOR_COUNT;

#if !UTIL_ALLOCATOR_FORCE_ALLOCATOR_INFO
	explicit VariableSizeAllocator(const Traits &traits = Traits());
#endif

	explicit VariableSizeAllocator(
			const AllocatorInfo &info, const Traits &traits = Traits());

	~VariableSizeAllocator();


	void* allocate(size_t size);
	void deallocate(void *element);

	template<typename T> void deleteObject(T *object);


	void setErrorHandler(AllocationErrorHandler *errorHandler);


	BaseAllocator* base(size_t index);
	static size_t getElementHeadSize();

	size_t getTotalElementSize();
	size_t getFreeElementSize();

	size_t getHugeElementCount();
	size_t getHugeElementSize();

	size_t getElementCapacity(const void *element);


	void getStats(AllocatorStats &stats);
	void setLimit(AllocatorStats::Type type, size_t value);
	AllocatorLimitter* setLimit(
			AllocatorStats::Type type, AllocatorLimitter *limitter,
			bool force = false);

	void setBaseLimit(size_t index, AllocatorStats::Type type, size_t value);

#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
	class ManagerTool {
		friend class AllocatorManager;
		typedef Mutex MutexType;
		static AllocatorDiffReporter::ActivationSaver& getActivationSaver(
				VariableSizeAllocator &alloc) {
			return alloc.activationSaver_;
		}
		static MutexType& getLock(VariableSizeAllocator &alloc) {
			return alloc.mutex_;
		}
	};
#endif

private:
#if UTIL_ALLOCATOR_VAR_ALLOCATOR_POOLING
	typedef UTIL_UNIQUE_PTR<FixedSizeCachedAllocator> BaseAllocatorPtr;
#else
	typedef BaseAllocator *BaseAllocatorPtr;
#endif

	VariableSizeAllocator(const VariableSizeAllocator&);
	VariableSizeAllocator& operator=(const VariableSizeAllocator&);

	void initialize(const Traits &traits);
	void clear();

	BaseAllocatorPtr baseList_[FIXED_ALLOCATOR_COUNT];

	AllocationErrorHandler *errorHandler_;

	Mutex mutex_;
	size_t hugeElementCount_;
	size_t hugeElementSize_;
	Traits traits_;

	UTIL_DETAIL_ALLOCATOR_DECLARE_REPORTER;

#if UTIL_ALLOCATOR_REPORTER_ENABLED2
	std::map<void*, std::string> stackTraceMap_;
#endif

#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
	AllocatorDiffReporter::ActivationSaver activationSaver_;
#endif

	AllocatorStats stats_;
	AllocatorLimitter *limitter_;
};

class VariableSizeAllocatorPool {
public:
	explicit VariableSizeAllocatorPool(const AllocatorInfo &info);
	~VariableSizeAllocatorPool();

	void setFreeElementLimitScale(size_t scale);

	static void initializeSubAllocators(
			const AllocatorInfo &localInfo, VariableSizeAllocatorPool *pool,
			UTIL_UNIQUE_PTR<FixedSizeCachedAllocator> *allocList,
			const size_t *elementSizeList, size_t count, bool locked);

private:
	typedef FixedSizeCachedAllocator::BaseAllocator BaseAllocator;

	struct Entry {
		Entry(const AllocatorInfo &info, size_t elementSize);

		Entry *next_;
		BaseAllocator alloc_;
		Mutex allocMutex_;
	};

	VariableSizeAllocatorPool(const VariableSizeAllocatorPool&);
	VariableSizeAllocatorPool& operator=(const VariableSizeAllocatorPool&);

	static void initializeSubAllocatorsNoPool(
			const AllocatorInfo &localInfo,
			UTIL_UNIQUE_PTR<FixedSizeCachedAllocator> *allocList,
			const size_t *elementSizeList, size_t count, bool locked);
	void initializeSubAllocatorsWithPool(
			const AllocatorInfo &localInfo,
			UTIL_UNIQUE_PTR<FixedSizeCachedAllocator> *allocList,
			const size_t *elementSizeList, size_t count);

	Entry* prepareEntries(const size_t *elementSizeList, size_t count);

	AllocatorInfo info_;
	Mutex mutex_;
	Entry *topEntry_;
	size_t freeElementLimitScale_;
};

namespace detail {
struct StdAllocatorHolder;
typedef VariableSizeAllocator<> DefaultVariableSizeAllocator;
typedef VariableSizeAllocator<Mutex> LockedVariableSizeAllocator;
struct VariableSizeAllocatorUtils {
#if UTIL_HAS_TEMPLATE_PLACEMENT_NEW
	template<typename Mutex, typename Traits>
	struct CheckResultOf {
		typedef VariableSizeAllocator<Mutex, Traits>& Type;
	};
	template<typename Mutex, typename Traits>
	static VariableSizeAllocator<Mutex, Traits>& checkType(
			VariableSizeAllocator<Mutex, Traits> &allocator) { return allocator; }
#else
	template<typename Mutex, typename Traits>
	struct CheckResultOf {
		typedef StdAllocatorHolder Type;
	};
	template<typename Mutex>
	struct CheckResultOf< Mutex, VariableSizeAllocatorTraits<> > {
		typedef VariableSizeAllocator<
				Mutex, VariableSizeAllocatorTraits<> >& Type;
	};
	static DefaultVariableSizeAllocator& checkType(
			DefaultVariableSizeAllocator &allocator) { return allocator; }
	static LockedVariableSizeAllocator& checkType(
			LockedVariableSizeAllocator &allocator) { return allocator; }
	template<typename Mutex, typename Traits>
	static StdAllocatorHolder checkType(
			VariableSizeAllocator<Mutex, Traits> &allocator);
#endif 

	template<typename Mutex>
	static FixedSizeAllocator<Mutex>* errorBaseAllocator() {
		typedef FixedSizeAllocator<Mutex> Alloc;
		return static_cast<Alloc*>(errorBaseAllocatorDetail());
	}

	static void* errorBaseAllocatorDetail();
};
}
}	

#define ALLOC_VAR_SIZE_NEW(allocator) \
		new (util::detail::VariableSizeAllocatorUtils::checkType(allocator))

#define ALLOC_VAR_SIZE_DELETE(allocator, object) \
		util::detail::VariableSizeAllocatorUtils::checkType( \
				allocator).deleteObject(object)

#if UTIL_HAS_TEMPLATE_PLACEMENT_NEW
template<typename Mutex, typename Traits>
inline void* operator new(
		size_t size, util::VariableSizeAllocator<Mutex, Traits> &allocator) {
	return allocator.allocate(size);
}

template<typename Mutex, typename Traits>
inline void operator delete(void *p,
		util::VariableSizeAllocator<Mutex, Traits> &allocator) throw() {
	try {
		allocator.deallocate(p);
	}
	catch (...) {
	}
}
#else
inline void* operator new(
		size_t size, util::detail::DefaultVariableSizeAllocator &allocator) {
	return allocator.allocate(size);
}
inline void operator delete(
		void *p,
		util::detail::DefaultVariableSizeAllocator &allocator) throw() {
	try {
		allocator.deallocate(p);
	}
	catch (...) {
	}
}

inline void* operator new(
		size_t size, util::detail::LockedVariableSizeAllocator &allocator) {
	return allocator.allocate(size);
}

inline void operator delete(
		void *p,
		util::detail::LockedVariableSizeAllocator &allocator) throw() {
	try {
		allocator.deallocate(p);
	}
	catch (...) {
	}
}
#endif 

namespace util {

template<typename T, typename BaseAllocator>
class StdAllocator;

/*!
	@brief Allocates memory, which can be freed at once according to the scop.
*/
class StackAllocator {
#if UTIL_FAILURE_SIMULATION_ENABLED
	friend class AllocationFailureSimulator;
#endif
public:
	class Scope;
	class ConfigScope;
	struct Option;

	typedef FixedSizeAllocator<Mutex> BaseAllocator;

#if !UTIL_ALLOCATOR_FORCE_ALLOCATOR_INFO
	explicit StackAllocator(BaseAllocator &base);
#endif

	StackAllocator(
			const AllocatorInfo &info, BaseAllocator *base,
			const Option *option = NULL);

	~StackAllocator();

	void* allocate(size_t size);

	void deallocate(void *ptr);

	template<typename T> void deleteObject(T *object);

	void setErrorHandler(AllocationErrorHandler *errorHandler);

	static void setDefaultErrorHandler(AllocationErrorHandler *errorHandler);

	void trim();


	void setTotalSizeLimit(size_t limit);
	void setFreeSizeLimit(size_t limit);


	size_t getTotalSizeLimit();
	size_t getFreeSizeLimit();

	size_t getTotalSize();
	size_t getFreeSize();

	size_t getHugeCount();
	size_t getHugeSize();


	void getStats(AllocatorStats &stats);
	void setLimit(AllocatorStats::Type type, size_t value);
	AllocatorLimitter* setLimit(
			AllocatorStats::Type type, AllocatorLimitter *limitter,
			bool force = false);

	BaseAllocator& base();

	struct Option {
		Option();

		StdAllocator<void, void> *smallAlloc_;
		size_t smallBlockSize_;
		size_t smallBlockLimit_;
	};

	struct Tool {
		static void forceReset(StackAllocator &alloc);

		static size_t getRestSize(StackAllocator &alloc);
		static size_t getRestSizeAligned(StackAllocator &alloc);
	};

#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
	class ManagerTool {
		friend class AllocatorManager;
		typedef NoopMutex MutexType;
		static AllocatorDiffReporter::ActivationSaver& getActivationSaver(
				StackAllocator &alloc) {
			return alloc.activationSaver_;
		}
		static MutexType& getLock(StackAllocator&) {
			static MutexType mutex;
			return mutex;
		}
	};
#endif

private:
	struct BlockHead {
		BlockHead *prev_;
		size_t blockSize_;

		uint8_t* body();
		uint8_t* end();
		size_t bodySize();
	};

	StackAllocator(const StackAllocator&);
	StackAllocator& operator=(const StackAllocator&);

	void* allocateOverBlock(size_t size);
	void push(BlockHead *&lastBlock, size_t &lastRestSize);
	void pop(BlockHead *lastBlock, size_t lastRestSize);

#if UTIL_ALLOCATOR_PRIOR_REQUESTER_STATS
	detail::AllocationRequester toRequester();
#else
	detail::EmptyAllocationRequester toRequester();
#endif

	size_t getTotalSizeForStats();

	static void handleAllocationError(util::Exception &e);
	static Option resolveOption(BaseAllocator *base, const Option *option);

	static AllocationErrorHandler *defaultErrorHandler_;

	size_t restSize_;
	uint8_t *end_;

	BaseAllocator &base_;

	BlockHead *topBlock_;
	BlockHead *freeBlock_;

	size_t totalSizeLimit_;
	size_t freeSizeLimit_;

	size_t totalSize_;
	size_t freeSize_;

	size_t hugeCount_;
	size_t hugeSize_;

	AllocationErrorHandler *errorHandler_;
	UTIL_DETAIL_ALLOCATOR_DECLARE_REPORTER;

	Option option_;
	size_t smallCount_;

	AllocatorStats stats_;
	AllocatorLimitter *limitter_;

#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
	AllocatorDiffReporter::ActivationSaver activationSaver_;
#endif

#if UTIL_ALLOCATOR_DETAIL_CONFLICTION_DETECTOR_ENABLED
	typedef NoopMutex MutexType;
	MutexType mutex_;
#endif
};

class StackAllocator::Scope {
public:
	explicit Scope(StackAllocator &allocator);

	~Scope();

private:
	Scope(const Scope&);
	Scope& operator=(const Scope&);

	StackAllocator &allocator_;

	BlockHead *lastBlock_;
	size_t lastRestSize_;
};

class StackAllocator::ConfigScope {
public:
	explicit ConfigScope(StackAllocator &allocator);
	~ConfigScope();

	void reset();

private:
	ConfigScope(const ConfigScope&);
	ConfigScope& operator=(const ConfigScope&);

	StackAllocator *allocator_;

	size_t totalSizeLimit_;
	size_t freeSizeLimit_;
};
}	

inline void* operator new(size_t size, util::StackAllocator &allocator) {
	return allocator.allocate(size);
}

inline void* operator new[](size_t size, util::StackAllocator &allocator) {
	return allocator.allocate(size);
}

inline void operator delete(void *p, util::StackAllocator &allocator) throw() {
	try {
		allocator.deallocate(p);
	}
	catch (...) {
	}
}

inline void operator delete[](void *p, util::StackAllocator &allocator) throw() {
	try {
		allocator.deallocate(p);
	}
	catch (...) {
	}
}

namespace util {

/*!
	@brief Allocates for STL containers or strings. (std::allocator compatible)
*/
template<typename T, typename BaseAllocator>
class StdAllocator {
public:
	typedef size_t size_type;
	typedef ptrdiff_t difference_type;
	typedef T value_type;
	typedef value_type *pointer;
	typedef value_type &reference;
	typedef const value_type *const_pointer;
	typedef const value_type &const_reference;

#if UTIL_ALLOCATOR_EMPTY_ALLOCATOR_CONSTRUCTOR_ALLOWED
	inline StdAllocator() throw() : base_(NULL) {
	}
#endif

	inline StdAllocator(BaseAllocator &base) throw() : base_(&base) {
	}

	inline explicit StdAllocator(BaseAllocator *base) throw() : base_(base) {
	}

	template<typename U>
	inline StdAllocator(const StdAllocator<U, BaseAllocator> &other) throw() :
			base_(other.base()) {
	}

	template<typename U>
	inline StdAllocator& operator=(
			const StdAllocator<U, BaseAllocator> &other) {
		base_ = other.base_;
		return *this;
	}

	inline size_type max_size() const throw() {
		return std::numeric_limits<size_t>::max() / sizeof(T);
	}

	inline pointer allocate(
			size_type size, std::allocator<void>::const_pointer = NULL) {
		assert(base_ != NULL);
		return static_cast<pointer>(base_->allocate(size * sizeof(T)));
	}

	inline void deallocate(pointer ptr, size_type) {
		assert(base_ != NULL);
		base_->deallocate(ptr);
	}

#if UTIL_CXX11_SUPPORTED
	template<typename U, typename ...Args>
	inline void construct(U *ptr, Args &&...args) {
		::new (static_cast<void*>(ptr)) U(std::forward<Args>(args)...);
	}
#else
	inline void construct(pointer ptr, const T &value) {
		new(static_cast<void*>(ptr)) T(value);
	}
#endif

#if UTIL_CXX11_SUPPORTED
	template<typename U>
	inline void destroy(U *ptr) {
		ptr->~U();
	}
#else
	inline void destroy(pointer ptr) {
		ptr->~T();
	}
#endif

	inline pointer address(reference value) const {
		return &value;
	}

	inline const_pointer address(const_reference value) const {
		return &value;
	}

	template<typename U>
	struct rebind { typedef StdAllocator<U, BaseAllocator> other; };

	inline BaseAllocator* base() const throw() { return base_; }

	struct EmptyConstructor {
		StdAllocator operator()() const throw() {
			return StdAllocator(static_cast<BaseAllocator*>(NULL));
		}
		bool operator()(const StdAllocator &alloc) const throw() {
			return alloc.base() == NULL;
		}
	};

private:
	BaseAllocator *base_;
};

/*!
	@brief StdAllocator specified in void type.
*/
template<typename BaseAllocator>
class StdAllocator<void, BaseAllocator> {
public:
	typedef void value_type;
	typedef void *pointer;
	typedef const void *const_pointer;

#if UTIL_ALLOCATOR_EMPTY_ALLOCATOR_CONSTRUCTOR_ALLOWED
	inline StdAllocator() throw() : base_(NULL) {
	}
#endif

	inline StdAllocator(BaseAllocator &base) throw() : base_(&base) {
	}

	inline explicit StdAllocator(BaseAllocator *base) throw() : base_(base) {
	}

	template<typename U>
	inline StdAllocator(const StdAllocator<U, BaseAllocator> &other) throw() :
			base_(other.base()) {
	}

	template<typename U>
	inline StdAllocator& operator=(
			const StdAllocator<U, BaseAllocator> &other) {
		base_ = other.base_;
		return *this;
	}

	template<typename U>
	struct rebind { typedef StdAllocator<U, BaseAllocator> other; };

	inline BaseAllocator* base() const throw() { return base_; }

	struct EmptyConstructor {
		StdAllocator operator()() const throw() {
			return StdAllocator(static_cast<BaseAllocator*>(NULL));
		}
		bool operator()(const StdAllocator &alloc) const throw() {
			return alloc.base() == NULL;
		}
	};

private:
	BaseAllocator *base_;
};

/*!
	@brief StdAllocator specified in void type.
*/
template<typename T>
class StdAllocator<T, void> {
public:
	typedef size_t size_type;
	typedef ptrdiff_t difference_type;
	typedef T value_type;
	typedef value_type *pointer;
	typedef value_type &reference;
	typedef const value_type *const_pointer;
	typedef const value_type &const_reference;

	typedef void* (WrapperFunc)(void*, void*, size_t);
	typedef std::pair<void*, WrapperFunc*> WrapperResult;

#if UTIL_ALLOCATOR_EMPTY_ALLOCATOR_CONSTRUCTOR_ALLOWED
	inline StdAllocator() throw() : base_(NULL), wrapper_(NULL) {
	}
#endif

	template<typename BaseAllocator>
	inline StdAllocator(const BaseAllocator &base) throw() :
			base_(wrap(&base).first), wrapper_(wrap(&base).second) {
	}

	template<typename BaseAllocator>
	inline StdAllocator(BaseAllocator &base) throw() :
			base_(wrap(&base).first), wrapper_(wrap(&base).second) {
	}

	template<typename BaseAllocator>
	inline explicit StdAllocator(BaseAllocator *base) throw() :
			base_(wrap(base).first), wrapper_(wrap(base).second) {
	}

	template<typename U, typename BaseAllocator>
	inline StdAllocator<T, void>& operator=(
			const StdAllocator<U, BaseAllocator> &other) {
		base_ = wrap(&other).first;
		wrapper_ = wrap(&other).second;
		return *this;
	}

	inline size_type max_size() const throw() {
		return std::numeric_limits<size_t>::max() / sizeof(T);
	}

	inline pointer allocate(
			size_type size, std::allocator<void>::const_pointer = NULL) {
#if UTIL_ALLOCATOR_EMPTY_ALLOCATOR_CONSTRUCTOR_ALLOWED
		assert(base_ != NULL);
#endif
		return static_cast<pointer>(
				(*wrapper_)(base_, NULL, size * sizeof(T)));
	}

	inline void deallocate(pointer ptr, size_type size) {
#if UTIL_ALLOCATOR_EMPTY_ALLOCATOR_CONSTRUCTOR_ALLOWED
		assert(base_ != NULL);
#endif
		if (ptr != NULL) {
			(*wrapper_)(base_, ptr, size);
		}
	}

#if UTIL_CXX11_SUPPORTED
	template<typename U, typename ...Args>
	inline void construct(U *ptr, Args &&...args) {
		::new (static_cast<void*>(ptr)) U(std::forward<Args>(args)...);
	}
#else
	inline void construct(pointer ptr, const T &value) {
		new(static_cast<void*>(ptr)) T(value);
	}
#endif

#if UTIL_CXX11_SUPPORTED
	template<typename U>
	inline void destroy(U *ptr) {
		ptr->~U();
	}
#else
	inline void destroy(pointer ptr) {
		ptr->~T();
	}
#endif

	inline pointer address(reference value) const {
		return &value;
	}

	inline const_pointer address(const_reference value) const {
		return &value;
	}

	template<typename U>
	struct rebind { typedef StdAllocator<U, void> other; };

	inline void* base() const throw() { return base_; }

	inline WrapperFunc* wrapper() const throw() { return wrapper_; }

	template<typename BaseAllocator>
	inline static WrapperResult wrap(BaseAllocator *base) throw() {
		return wrapSame(base, base);
	}

	struct EmptyConstructor {
		StdAllocator operator()() const throw() {
			return StdAllocator(util::FalseType());
		}
		bool operator()(const StdAllocator &alloc) const throw() {
			return alloc.base() == NULL;
		}
	};

private:

	template<typename BaseAllocator>
	struct CustomWrapper {
		static void* execute(void *alloc, void *ptr, size_t size) {
			if (ptr == NULL) {
				return static_cast<BaseAllocator*>(alloc)->allocate(size);
			}
			else {
				static_cast<BaseAllocator*>(alloc)->deallocate(ptr);
				return NULL;
			}
		}
	};

	template<typename BaseAllocator, typename ValueType>
	struct StdWrapper {
		static void* execute(void *alloc, void *ptr, size_t size) {
			UTIL_STATIC_ASSERT(sizeof(ValueType) == 1);
			if (ptr == NULL) {
				return static_cast<BaseAllocator*>(alloc)->allocate(size);
			}
			else {
				static_cast<BaseAllocator*>(alloc)->deallocate(
						static_cast<ValueType*>(ptr), size);
				return NULL;
			}
		}
	};

	struct Placeholder {};

	explicit StdAllocator(const FalseType&) throw() :
			base_(NULL), wrapper_(NULL) {
	}

	template<typename U, typename BaseAllocator>
	inline static WrapperResult wrapSame(
			const StdAllocator<U, void> *base, BaseAllocator*) throw() {
		assert(base != NULL);
		return WrapperResult(base->base(), base->wrapper());
	}

	template<typename BaseAllocator>
	inline static WrapperResult wrapSame(
			const void*, BaseAllocator *base) throw() {
		return wrapOther<BaseAllocator>(
				ensureNonConstAllocator(base), static_cast<Placeholder*>(NULL));
	}

	template<typename BaseAllocator>
	inline static WrapperResult wrapOther(BaseAllocator *base, void*) throw() {
		return WrapperResult(base, &CustomWrapper<BaseAllocator>::execute);
	}

	template<typename BaseAllocator>
	inline static WrapperResult wrapOther(
			BaseAllocator *base,
			typename BaseAllocator::
					template rebind<Placeholder>::other::value_type*) throw() {
		return WrapperResult(base, &StdWrapper<
				BaseAllocator, typename BaseAllocator::value_type>::execute);
	}

	template<typename BaseAllocator>
	inline static WrapperResult wrapOther(
			BaseAllocator *base,
			typename BaseAllocator::StdAllocatorResolver::
					template Rebind<Placeholder>::Other::ValueType*) throw() {
		typedef typename BaseAllocator::StdAllocatorResolver Resolver;
		return WrapperResult(base, Resolver()(base).wrapper_);
	}

	template<typename Alloc>
	inline static Alloc* ensureNonConstAllocator(Alloc *alloc) {
		return alloc;
	}

	template<typename Alloc>
	inline static Alloc* ensureNonConstAllocator(const Alloc*) {
		UTIL_STATIC_ASSERT(sizeof(Alloc) < 0);
		return NULL;
	}

	void *base_;
	WrapperFunc *wrapper_;
};

/*!
	@brief StdAllocator, enable to specify a base allocator to run.
*/
template<>
class StdAllocator<void, void> {
private:
	typedef StdAllocator<char, void> OtherAllocator;

	typedef OtherAllocator::WrapperFunc WrapperFunc;

public:
	typedef void value_type;
	typedef void *pointer;
	typedef const void *const_pointer;

#if UTIL_ALLOCATOR_EMPTY_ALLOCATOR_CONSTRUCTOR_ALLOWED
	inline StdAllocator() throw() : base_(NULL), wrapper_(NULL) {
	}
#endif

	template<typename BaseAllocator>
	inline StdAllocator(const BaseAllocator &base) throw() :
			base_(OtherAllocator::wrap(&base).first),
			wrapper_(OtherAllocator::wrap(&base).second) {
	}

	template<typename BaseAllocator>
	inline StdAllocator(BaseAllocator &base) throw() :
			base_(OtherAllocator::wrap(&base).first),
			wrapper_(OtherAllocator::wrap(&base).second) {
	}

	template<typename BaseAllocator>
	inline explicit StdAllocator(BaseAllocator *base) throw() :
			base_(OtherAllocator::wrap(base).first),
			wrapper_(OtherAllocator::wrap(base).second) {
	}

	template<typename U, typename BaseAllocator>
	inline StdAllocator& operator=(
			const StdAllocator<U, BaseAllocator> &other) {
		base_ = OtherAllocator::wrap(&other).first;
		wrapper_ = OtherAllocator::wrap(&other).second;
		return *this;
	}

	template<typename U>
	struct rebind { typedef StdAllocator<U, void> other; };

	inline void* base() const throw() { return base_; }

	inline WrapperFunc* wrapper() const throw() { return wrapper_; }

	template<typename U>
	void deleteObject(U *object) {
		if (object != NULL) {
			util::StdAllocator<U, void> typed(*this);
			typed.destroy(object);
			typed.deallocate(object, 1);
		}
	}

	struct EmptyConstructor {
		StdAllocator operator()() const throw() {
			return StdAllocator(util::FalseType());
		}
		bool operator()(const StdAllocator &alloc) const throw() {
			return alloc.base() == NULL;
		}
	};

private:
	explicit StdAllocator(const FalseType&) throw() :
			base_(NULL), wrapper_(NULL) {
	}

	void *base_;
	WrapperFunc *wrapper_;
};

#if !UTIL_HAS_TEMPLATE_PLACEMENT_NEW
namespace detail {
struct StdAllocatorHolder {
public:
	explicit StdAllocatorHolder(const StdAllocator<void, void> &alloc) :
			alloc_(alloc) {
	}

	StdAllocator<void, void> operator()() const {
		return alloc_;
	}

private:
	StdAllocator<void, void> alloc_;
};
template<typename Mutex, typename Traits>
inline StdAllocatorHolder VariableSizeAllocatorUtils::checkType(
		VariableSizeAllocator<Mutex, Traits> &allocator) {
	return StdAllocatorHolder(StdAllocator<void, void>());
}
} 
#endif 

template<typename T, typename U, typename BaseAllocator>
inline bool operator==(
		const StdAllocator<T, BaseAllocator> &op1,
		const StdAllocator<U, BaseAllocator> &op2) throw() {

	return op1.base() == op2.base();
}

template<typename T, typename U, typename BaseAllocator>
inline bool operator!=(
		const StdAllocator<T, BaseAllocator> &op1,
		const StdAllocator<U, BaseAllocator> &op2) throw() {

	return op1.base() != op2.base();
}
} 

inline void* operator new(
		size_t size, util::StdAllocator<void, void> &allocator) {
	util::StdAllocator<uint8_t, void> bytesAlloc(allocator);
	return bytesAlloc.allocate(size);
}

inline void operator delete(
		void *p, util::StdAllocator<void, void> &allocator) throw() {
	util::StdAllocator<uint8_t, void> bytesAlloc(allocator);
	bytesAlloc.deallocate(static_cast<uint8_t*>(p), 0);
}

#if !UTIL_HAS_TEMPLATE_PLACEMENT_NEW
inline void* operator new(
		size_t size, const util::detail::StdAllocatorHolder &holder) {
	util::StdAllocator<uint8_t, void> bytesAlloc(holder());
	return bytesAlloc.allocate(size);
}

inline void operator delete(
		void *p, const util::detail::StdAllocatorHolder &holder) throw() {
	util::StdAllocator<uint8_t, void> bytesAlloc(holder());
	bytesAlloc.deallocate(static_cast<uint8_t*>(p), 0);
}
#endif 

namespace util {

namespace detail {
struct AllocNewChecker {
	static StackAllocator& check(StackAllocator &allocator) throw() {
		return allocator;
	}

	static StdAllocator<void, void>& check(
			StdAllocator<void, void> &allocator) throw() {
		return allocator;
	}

	template<typename Mutex, typename Traits>
	static typename VariableSizeAllocatorUtils::
	template CheckResultOf<Mutex, Traits>::Type check(
			VariableSizeAllocator<Mutex, Traits> &allocator) throw() {
		return VariableSizeAllocatorUtils::checkType(allocator);
	}
};
} 
} 

#define ALLOC_NEW(allocator) \
		new (util::detail::AllocNewChecker::check(allocator))

#define ALLOC_DELETE(allocator, object) \
		util::detail::AllocNewChecker::check(allocator).deleteObject(object)

namespace util {

namespace detail {
#if !UTIL_ALLOC_CXX_MOVE_ENABLED
template<typename T>
class Movable {
public:
	Movable(T &value) throw();
	~Movable();

	Movable(const Movable &another) throw();
	Movable& operator=(const Movable &another) throw();

	void release(T &value) const throw();

private:
	struct Chain {
		Chain();

		bool isTop() const throw();
		Chain& top() throw();

		void add(Chain &chain) throw();
		void remove() throw();

		void clearValue() throw();

		Chain *next_;
		Chain *prev_;
		T value_;
	};
	Chain localChain_;
	Chain &chain_;
};
#endif 

template<typename T>
struct MovableTraits {
#if UTIL_ALLOC_CXX_MOVE_ENABLED
	typedef T ReturnType;
	typedef T &&RefType;
#else
	typedef Movable<T> ReturnType;
	typedef const Movable<T> &RefType;
#endif 
};

template<typename T>
class InitializableValue {
public:
	explicit InitializableValue(const T &value = T()) throw() :
			value_(value) {
	}

	T& get() throw() { return value_; }
	const T& get() const throw() { return value_; }

private:
	T value_;
};

template<size_t BaseSize>
class ApproxAlignedStorage {
public:
	ApproxAlignedStorage() throw() {
		UTIL_STATIC_ASSERT((sizeof(void*) <= sizeof(uint64_t)));
	}

	void* address() throw() { return data_; }
	const void* address() const throw() { return data_; }

private:
	typedef
			typename Conditional<(BaseSize <= 1), uint8_t,
			typename Conditional<(BaseSize <= 2), uint16_t,
			typename Conditional<(BaseSize <= 4), uint32_t,
					uint64_t>::Type>::Type>::Type UnitType;

	enum {
		UNIT_SIZE = sizeof(UnitType),
		UNIT_COUNT = (BaseSize <= UNIT_SIZE ?
				1 : (BaseSize + (UNIT_SIZE - 1)) / UNIT_SIZE)
	};

	UnitType data_[UNIT_COUNT];
};

template<typename T>
struct EmptyObjectTraits {
public:
	static T create() throw() {
		return create<T>(static_cast<const TrueType*>(NULL));
	}

	static bool isEmpty(const T &obj) throw() {
		return isEmpty<T>(&obj);
	}

private:
	template<typename U>
	static U create(
			const typename BoolType<!IsSame<
					typename U::EmptyConstructor,
					void>::VALUE>::Result*) throw() {
		return typename U::EmptyConstructor()();
	}

	template<typename U>
	static U create(const void*) throw() {
		return U();
	}

	template<typename U>
	static bool isEmpty(
			const typename Conditional<
					(sizeof(typename U::EmptyConstructor) > 0),
					U, U>::Type *obj) throw() {
		return typename U::EmptyConstructor()(*obj);
	}

	template<typename U>
	static bool isEmpty(const void*) throw() {
		return false;
	}
};
} 

template< typename T, typename Alloc = StdAllocator<T, void> >
class AllocDefaultDelete {
public:
	explicit AllocDefaultDelete(const Alloc &alloc) throw();

	template<typename U, typename OtherAlloc>
	AllocDefaultDelete(
			const AllocDefaultDelete<U, OtherAlloc> &other) throw();

	template<typename U, typename OtherAlloc>
	void copyTo(AllocDefaultDelete<U, OtherAlloc> &other) const throw();

	void operator()(T *ptr);

	struct EmptyConstructor {
		AllocDefaultDelete operator()() const throw();
		bool operator()(const AllocDefaultDelete &obj) const throw();
	};

private:
	Alloc alloc_;
};

namespace detail {
template<typename D>
class InitializableDeleter {
public:
	InitializableDeleter() throw() : deleter_(EmptyObjectTraits<D>::create()) {
	}

	template<typename B>
	explicit InitializableDeleter(B &deleterBase, void* = NULL) throw() :
			deleter_(deleterBase) {
	}

	D& base() throw() { return deleter_; }
	const D& base() const throw() { return deleter_; }

	template<typename> bool detectNonDeletable() const throw() {
		return false;
	}

	template<typename T> void operator()(T *ptr) {
		deleter_(ptr);
	}

private:
	D deleter_;
};

template<typename D>
class AnyDeleter {
public:
	AnyDeleter() throw() {
	}

	template<typename E, typename T>
	AnyDeleter(const E &typedDeleter, T*) throw() :
			deleter_(typedDeleter),
			typedDeleter_(&deleteAt<E, T>) {
	}

	D& base() throw() { return deleter_; }
	const D& base() const throw() { return deleter_; }

	template<typename T> bool detectNonDeletable() const throw() {
		if (typedDeleter_.get() == NULL) {
			return true;
		}
		const TypeIdFunc type1 = typedDeleter_.get()(NULL, NULL);
		const TypeIdFunc type2 = &typeId<T>;
		return (type1 != type2);
	}

	void operator()(void *ptr) {
		if (ptr != NULL) {
			typedDeleter_.get()(&deleter_.base(), ptr);
		}
	}

private:
	typedef void (*TypeIdFunc)();
	typedef TypeIdFunc (*TypedDeleterFunc)(D*, void*);

	template<typename E, typename T>
	static TypeIdFunc deleteAt(D *deleter, void *ptr) {
		if (ptr == NULL) {
			return &typeId<T>;
		}
		E typed(*deleter);
		typed(static_cast<T*>(ptr));
		return NULL;
	}

	template<typename T> static void typeId() {}

	InitializableDeleter<D> deleter_;
	InitializableValue<TypedDeleterFunc> typedDeleter_;
};
} 


template< typename T, typename D = AllocDefaultDelete<T> >
class AllocUniquePtr {
private:
	enum {
		FOR_ANY = IsSame<T, void>::VALUE
	};
	struct Inaccessible {};
	typedef typename Conditional<
			FOR_ANY, Inaccessible, T>::Type DerefValueType;
	typedef typename Conditional<
			FOR_ANY, Inaccessible, DerefValueType&>::Type DerefValueRefType;

public:
	typedef T element_type;
	typedef D deleter_type;
	typedef element_type *pointer;

	typedef typename detail::MovableTraits<
			AllocUniquePtr>::ReturnType ReturnType;
	typedef typename detail::MovableTraits<
			AllocUniquePtr>::RefType MovableRefType;

	AllocUniquePtr() throw() {}
	~AllocUniquePtr();

	template<typename B>
	AllocUniquePtr(pointer ptr, B &deleterBase) throw();
	template<typename B>
	static ReturnType of(pointer ptr, B &deleterBase) throw();

	AllocUniquePtr(MovableRefType another) throw();
	AllocUniquePtr& operator=(MovableRefType another) throw();

#if UTIL_ALLOC_CXX_MOVE_ENABLED
	template<typename U, typename E>
	AllocUniquePtr(AllocUniquePtr<U, E> &&other) throw();
	template<typename U, typename E>
	AllocUniquePtr& operator=(AllocUniquePtr<U, E> &&other) throw();
#else
	template<typename U, typename E>
	AllocUniquePtr(
			const detail::Movable< AllocUniquePtr<U, E> > &other) throw();
	template<typename U, typename E>
	AllocUniquePtr& operator=(
			const detail::Movable< AllocUniquePtr<U, E> > &other) throw();
#endif 

	deleter_type get_deleter() const throw() { return deleter_.base(); }

	pointer get() const throw() { return ptr_.get(); }
	DerefValueRefType operator*() const { return deref(get()); }
	pointer operator->() const throw() { return &deref(get()); }
	bool isEmpty() const throw() { return get() == pointer(); }

	template<typename U> U* getAs() const throw();
	template<typename U> U& resolveAs() const;

	pointer release() throw();
	void reset() throw();
	void reset(pointer ptr) throw(); 

	void swap(AllocUniquePtr &another) throw();

private:
	typedef typename Conditional<
			FOR_ANY, detail::AnyDeleter<deleter_type>,
			detail::InitializableDeleter<deleter_type> >::Type WrappedDeleter;

	AllocUniquePtr(const AllocUniquePtr&);
	AllocUniquePtr& operator=(const AllocUniquePtr&);

	static deleter_type createEmptyDeleter() throw();

	template<typename U> static U& deref(U *ptr) throw() {
		assert(ptr != NULL);
		return *ptr;
	}
	static Inaccessible deref(void*) throw() { return Inaccessible(); }

	detail::InitializableValue<pointer> ptr_;
	WrappedDeleter deleter_;
};
} 

#define ALLOC_UNIQUE(alloc, type, ...) \
		util::AllocUniquePtr<type>::of( \
				ALLOC_NEW(alloc) type(__VA_ARGS__), alloc)


namespace util {
template<typename T> class LocalUniquePtr;
namespace detail {
class LocalUniquePtrBuilder {
public:
	LocalUniquePtrBuilder(void *storage, size_t size, bool constructed);

	void* getStorage(size_t size) const;
	void* get() const throw();

	template<typename T>
	static LocalUniquePtr<T>& check(LocalUniquePtr<T> &src) { return src; }

private:
	void *storage_;
	size_t size_;
	bool constructed_;
};
} 

template<typename T>
class LocalUniquePtr {
public:
	class Builder;

	typedef T element_type;
	typedef element_type *pointer;

	LocalUniquePtr() throw() {}
	~LocalUniquePtr();

	pointer get() const throw() { return ptr_.get(); }
	element_type& operator*() const;
	pointer operator->() const throw();

	pointer release() throw();
	void reset() throw();

	LocalUniquePtr& operator=(const detail::LocalUniquePtrBuilder &builder);

	detail::LocalUniquePtrBuilder toBuilder(T *ptr);

private:
	LocalUniquePtr(const LocalUniquePtr&);
	LocalUniquePtr& operator=(const LocalUniquePtr&);

	detail::InitializableValue<pointer> ptr_;
	detail::ApproxAlignedStorage<sizeof(T)> storage_;
};
} 

#define UTIL_MAKE_LOCAL_UNIQUE(localUniqPtr, type, ...) \
		util::detail::LocalUniquePtrBuilder::check(localUniqPtr).toBuilder( \
				new (util::detail::LocalUniquePtrBuilder::check( \
						localUniqPtr).toBuilder(NULL).getStorage( \
								sizeof(type))) \
						type(__VA_ARGS__))

inline void* operator new(
		size_t size, const util::detail::LocalUniquePtrBuilder &builder) {
	return builder.getStorage(size);
}

inline void operator delete(
		void*, const util::detail::LocalUniquePtrBuilder&) throw() {
}

namespace util {

class AllocatorManager {
public:
	typedef AllocatorInfo::ManagerInside ManagerInside;

	typedef AllocatorGroupId GroupId;
	static const AllocatorGroupId GROUP_ID_ROOT = 0;

	enum LimitType {
		LIMIT_GROUP_TOTAL_SIZE,
		LIMIT_EACH_CACHE_SIZE,
		LIMIT_EACH_STABLE_SIZE,
		LIMIT_TYPE_END
	};

	~AllocatorManager();
	static AllocatorManager& getDefaultInstance();

	bool addGroup(GroupId parentId, GroupId id, const char8_t *nameLiteral);

	template<typename Alloc>
	bool addAllocator(GroupId id, Alloc &alloc, AllocatorLimitter *&limitter);

	template<typename Alloc>
	static bool addAllocator(
			const AllocatorInfo &info, Alloc &alloc,
			AllocatorLimitter *&limitter);

	template<typename Alloc>
	bool removeAllocator(GroupId id, Alloc &alloc) throw();

	template<typename Alloc>
	static bool removeAllocator(const AllocatorInfo &info, Alloc &alloc);

	bool getParentId(GroupId &id, const ManagerInside *inside = NULL);
	template<typename InsertIterator> void listSubGroup(
			GroupId id, InsertIterator it, bool recursive = false);

	const char8_t* getName(GroupId id, const ManagerInside *inside = NULL);

	void getGroupStats(
			const GroupId *idList, size_t idCount, AllocatorStats *statsList,
			AllocatorStats *directStats = NULL);

	template<typename InsertIterator>
	void getAllocatorStats(
			const GroupId *idList, size_t idCount, InsertIterator it);

	void setLimit(GroupId id, LimitType limitType, size_t limit);

	AllocatorLimitter* getGroupLimitter(GroupId id);
	void setSharingLimitterGroup(GroupId id, GroupId sharingId);

#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
	void saveReporterSnapshots(
			const GroupId *idList, size_t idCount, const char8_t *prefix,
			void *requester);
	void removeReporterSnapshots(
			const GroupId *idList, size_t idCount, const char8_t *prefix,
			void *requester);
	void compareReporterSnapshots(
			const GroupId *idList, size_t idCount, const char8_t *prefix,
			void *requester, std::ostream *out);
#endif

	static int64_t estimateHeapUsage(
			const AllocatorStats &rootStats, const AllocatorStats &directStats);

	struct Initializer;

private:
	enum Command {
		COMMAND_GET_STAT,
		COMMAND_SET_TOTAL_LIMITTER,
		COMMAND_SET_EACH_LIMIT,
		COMMAND_SAVE_REPORTER_SNAPSHOT,
		COMMAND_REMOVE_REPORTER_SNAPSHOT,
		COMMAND_COMPARE_REPORTER_SNAPSHOT
	};

	template<typename Alloc> struct Accessor;
	struct AccessorParams;
	struct AllocatorEntry;
	struct AllocatorMap;
	struct GroupEntry;

	typedef void (AccessorFunc)(void*, Command, const AccessorParams&);
	typedef void (GroupIdInsertFunc)(void*, const GroupId&);
	typedef void (StatsInsertFunc)(void*, const AllocatorStats&);

	template<typename T> struct TinyList {
	public:
		TinyList();
		~TinyList();
		TinyList(const TinyList &another);
		TinyList& operator=(const TinyList &another);
		void add(const T &value);
		void remove(T *it);

		T *begin_;
		T *end_;
		T *storageEnd_;
	};

	AllocatorManager();

	AllocatorManager(const AllocatorManager&);
	AllocatorManager& operator=(const AllocatorManager&);

	void clearGroupList();

	void applyAllocatorLimit(
			AllocatorEntry &entry, LimitType limitType,
			GroupEntry &groupEntry);

	bool addAllocatorDetail(
			GroupId id, const AllocatorEntry &allocEntry,
			AllocatorLimitter *&limitter);
	bool removeAllocatorDetail(GroupId id, void *alloc) throw();
	void listSubGroupDetail(
			GroupId id, void *insertIt, GroupIdInsertFunc insertFunc,
			bool recursive);
	void getAllocatorStatsDetail(
			const GroupId *idList, size_t idCount, void *insertIt,
			StatsInsertFunc insertFunc);

	static void getDirectAllocationStats(AllocatorStats &stats);
	static void getDirectAllocationStats(
			void *insertIt, StatsInsertFunc insertFunc);

	GroupEntry* getParentEntry(GroupId &id);
	bool isDescendantOrSelf(GroupId id, GroupId subId);

	static AllocatorEntry* findAllocatorEntry(
			GroupEntry &groupEntry, void *alloc);
	static void addAllocatorEntry(
			GroupEntry &groupEntry, const AllocatorEntry &allocEntry);
	static void removeAllocatorEntry(
			GroupEntry &groupEntry, const AllocatorEntry &allocEntry);

	template<typename InsertIterator>
	static void insertGroupId(void *insertIt, const GroupId &id);
	template<typename InsertIterator>
	static void insertStats(void *insertIt, const AllocatorStats &stats);

	static void mergeStatsAsInserter(
			void *insertIt, const AllocatorStats &stats);

#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
	void operateReporterSnapshots(
			const GroupId *idList, size_t idCount, const char8_t *prefix,
			void *requester, Command command, std::ostream *out);
#endif

	AllocatorLimitter& prepareTotalLimitter(GroupId id, bool &found);

	static AllocatorManager *defaultInstance_;

	util::Mutex mutex_;
	TinyList<GroupEntry> groupList_;
};

struct AllocatorManager::Initializer {
public:
	Initializer();
	~Initializer();

private:
	static size_t counter_;
};

template<typename Alloc>
struct AllocatorManager::Accessor {
	static void access(
			void* allocator, Command command, const AccessorParams &params);
};

struct AllocatorManager::AccessorParams {
	AccessorParams();

	size_t size_;
	AllocatorStats *stats_;
	AllocatorLimitter *limitter_;
	AllocatorStats::Type type_;
	std::ostream *out_;
};

struct AllocatorManager::AllocatorEntry {
	AllocatorEntry();

	void *allocator_;
	AccessorFunc *accessor_;
};

namespace detail {
static AllocatorManager::Initializer g_allocatorManagerInitializer;
}

class AllocatorLimitter {
public:
	class Scope;
	struct Stats;

	AllocatorLimitter(const AllocatorInfo &info, AllocatorLimitter *parent);
	~AllocatorLimitter();

	Stats getStats();

	void setLimit(size_t size);
	void setFailOnExcess(bool enabled);

	size_t acquire(
			size_t minimum, size_t desired, bool force = false,
			const AllocatorLimitter *requester = NULL);
	void release(size_t size);

	void growReservation(size_t size);
	void shrinkReservation(size_t size);

	size_t getAvailableSize();
	size_t getUsageSize();

	static AllocatorLimitter* moveSize(
			AllocatorLimitter *dest, AllocatorLimitter *src, size_t size,
			bool force);

	void setErrorHandler(AllocationErrorHandler *handler);

private:
	size_t acquireLocal(
			size_t minimum, size_t desired, bool force,
			const AllocatorLimitter *requester);
	void releaseLocal(size_t size);

	size_t getLocalAvailableSize();
	size_t getLocalUsageSize();
	void updatePeakUsageSize();

	const AllocatorLimitter& resolveRequester(const AllocatorLimitter *base);
	bool resolveFailOnExcess(
			const AllocatorLimitter *requester, bool acquiringForcibly);

	void errorNewLimit(const AllocatorInfo &info, size_t newLimit);
	size_t errorAcquisition(const AllocatorInfo &info, size_t required);

	const AllocatorInfo info_;
	AllocatorLimitter *parent_;
	size_t limit_;
	size_t acquired_;
	size_t reserved_;
	size_t peakUsage_;
	bool failOnExcess_;
	util::Mutex mutex_;
	util::AllocationErrorHandler *errorHandler_;
};

class AllocatorLimitter::Scope {
public:
	template<typename Alloc> Scope(AllocatorLimitter &limitter, Alloc &allocator);
	~Scope();

private:
	typedef void (*UnbindFunc)(void*, AllocatorLimitter*);

	template<typename Alloc>
	static void unbind(void *allocator, AllocatorLimitter *orgLimitter);

	AllocatorLimitter &limitter_;
	AllocatorLimitter *orgLimitter_;
	void *allocator_;
	UnbindFunc unbinder_;
};

struct AllocatorLimitter::Stats {
	Stats();

	size_t usage_;
	size_t peakUsage_;
	size_t limit_;
	bool failOnExcess_;
};

/*!
	@brief STL string template for using allocators with members.
*/
template<
		typename CharT,
		typename Traits = std::char_traits<CharT>,
		typename Alloc = std::allocator<CharT> >
class BasicString : public std::basic_string<CharT, Traits, Alloc> {
private:
	typedef BasicString<CharT, Traits, Alloc> ThisType;
	typedef std::basic_string<CharT, Traits, Alloc> BaseType;

	template<typename T>
	struct IntDetector {
		enum { VALUE = std::numeric_limits<T>::is_integer };
		typedef typename BoolType<VALUE>::Result Result;
	};

public:
	typedef typename BaseType::iterator iterator;
	typedef typename BaseType::size_type size_type;
	typedef typename BaseType::allocator_type allocator_type;

	explicit inline BasicString(const allocator_type& alloc = allocator_type()) :
			BaseType(alloc) {
	}

	inline BasicString(
			const BaseType& str, size_type pos, size_type len = ThisType::npos,
			const allocator_type& alloc = allocator_type()) :
			BaseType(str, pos, len, alloc) {
	}

	inline BasicString(
			const CharT *s, const allocator_type &alloc = allocator_type()) :
			BaseType(s, alloc) {
	}

	inline BasicString(
			const CharT *s, size_type n,
			const allocator_type& alloc = allocator_type()) :
			BaseType(s, n, alloc) {
	}

	inline BasicString(
			size_type n, CharT c,
			const allocator_type& alloc = allocator_type()) :
			BaseType(n, c, alloc) {
	}

	template<class InputIterator>
	inline BasicString(
			InputIterator first, InputIterator last,
			const allocator_type &alloc = allocator_type()) :
			BaseType(first, last, alloc) {
	}

	inline BasicString(const BaseType& str) : BaseType(str) {
	}

	inline ~BasicString() {
	}

	inline ThisType& operator=(const BaseType &str) {
		BaseType::operator=(str);
		return *this;
	}

	inline ThisType& operator=(const CharT *s) {
		BaseType::operator=(s);
		return *this;
	}

	inline ThisType& operator=(CharT c) {
		BaseType::operator=(c);
		return *this;
	}

#if UTIL_ALLOCATOR_BASIC_STRING_ALTER_MODIFIERS

	inline ThisType& operator+=(const BaseType &str) {
		BaseType::operator+=(str);
		return *this;
	}

	inline ThisType& operator+=(const CharT *s) {
		BaseType::operator+=(s);
		return *this;
	}

	inline ThisType& operator+=(CharT c) {
		BaseType::operator+=(c);
		return *this;
	}

	inline ThisType& append(const BaseType &str) {
		BaseType::append(str);
		return *this;
	}

	inline ThisType& append(const BaseType &str, size_type pos, size_type n) {
		BaseType::append(str, pos, n);
		return *this;
	}

	inline ThisType& append(const CharT *s) {
		BaseType::append(s);
		return *this;
	}

	inline ThisType& append(const CharT *s, size_type n) {
		BaseType::append(s, n);
		return *this;
	}

	inline ThisType& append(size_type n, CharT c) {
		BaseType::append(n, c);
		return *this;
	}

	inline ThisType& assign(const BaseType &str) {
		BaseType::assign(str);
		return *this;
	}

	inline ThisType& assign(const BaseType &str, size_type pos, size_type n) {
		BaseType::assign(str, pos, n);
		return *this;
	}

	inline ThisType& assign(const CharT *s) {
		BaseType::assign(s);
		return *this;
	}

	inline ThisType& assign(const CharT *s, size_type n) {
		BaseType::assign(s, n);
		return *this;
	}

	inline ThisType& assign(size_type n, CharT c) {
		BaseType::assign(n, c);
		return *this;
	}

	inline ThisType& insert(size_type pos, const BaseType &str) {
		BaseType::insert(pos, str);
		return *this;
	}

	inline ThisType& insert(
			size_type pos, const BaseType &str,
			size_type subPos, size_type n) {
		BaseType::insert(pos, str, subPos, n);
		return *this;
	}

	inline ThisType& insert(size_type pos, const CharT *s) {
		BaseType::insert(pos, s);
		return *this;
	}

	inline ThisType& insert(size_type pos, const CharT *s, size_type n) {
		BaseType::insert(pos, s, n);
		return *this;
	}

	inline ThisType& insert(size_type pos, size_type n, CharT c) {
		BaseType::insert(pos, n, c);
		return *this;
	}

	inline void insert(iterator p, size_type n, CharT c) {
		BaseType::insert(p, n, c);
	}

	inline iterator insert(iterator p, CharT c) {
		return BaseType::insert(p, c);
	}

	inline ThisType& replace(
			size_type pos, size_type len, const BaseType &str) {
		BaseType::replace(pos, len, str);
		return *this;
	}

	inline ThisType& replace(
			iterator i1, iterator i2, const BaseType &str) {
		BaseType::replace(i1, i2, str);
		return *this;
	}

	inline ThisType& replace(
			size_type pos, size_type len, const BaseType &str,
			size_type subPos, size_type n) {
		BaseType::replace(pos, len, str, subPos, n);
		return *this;
	}

	inline ThisType& replace(size_type pos, size_type len, const CharT *s) {
		BaseType::replace(pos, len, s);
		return *this;
	}

	inline ThisType& replace(iterator i1, iterator i2, const CharT *s) {
		BaseType::replace(i1, i2, s);
		return *this;
	}

	inline ThisType& replace(
			size_type pos, size_type len, const CharT *s, size_type n) {
		BaseType::replace(pos, len, s, n);
		return *this;
	}

	inline ThisType& replace(
			iterator i1, iterator i2, const CharT *s, size_type n) {
		BaseType::replace(i1, i2, s, n);
		return *this;
	}

	inline ThisType& replace(
			size_type pos, size_type len, size_type n, CharT c) {
		BaseType::replace(pos, len, n, c);
		return *this;
	}

	inline ThisType& replace(
			iterator i1, iterator i2, size_type n, CharT c) {
		BaseType::replace(i1, i2, n, c);
		return *this;
	}

	inline ThisType& erase(size_type pos = 0, size_type len = ThisType::npos) {
		BaseType::erase(pos, len);
		return *this;
	}

	inline iterator erase(iterator p) {
		return BaseType::erase(p);
	}

	inline iterator erase(iterator first, iterator last) {
		return BaseType::erase(first, last);
	}

	template<typename InputIterator>
	inline ThisType& append(InputIterator first, InputIterator last) {
		typedef typename IntDetector<InputIterator>::Result Tag;
		return replaceByRange(this->end(), this->end(), first, last, Tag());
	}

	template<typename InputIterator>
	inline ThisType& assign(InputIterator first, InputIterator last) {
		typedef typename IntDetector<InputIterator>::Result Tag;
		return replaceByRange(this->begin(), this->end(), first, last, Tag());
	}

	template<typename InputIterator>
	inline void insert(iterator p, InputIterator first, InputIterator last) {
		typedef typename IntDetector<InputIterator>::Result Tag;
		replaceByRange(p, p, first, last, Tag());
	}

	template<typename InputIterator>
	inline ThisType& replace(
			iterator i1, iterator i2, InputIterator first, InputIterator last) {
		typedef typename IntDetector<InputIterator>::Result Tag;
		return replaceByRange(i1, i2, first, last, Tag());
	}

#endif	

	inline ThisType substr(
			size_type pos = 0, size_type len = ThisType::npos) const {
#if UTIL_ALLOCATOR_BASIC_STRING_SUBSTRING_ENABLED
		return ThisType(*this, pos, len, this->get_allocator());
#else
		(void) pos;
		(void) len;
		UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_OPERATION,
				"BasicString::substr is disabled");
#endif
	}

private:
	template<typename InputIterator>
	inline ThisType& replaceByRange(
			iterator i1, iterator i2, InputIterator n, InputIterator c,
			const TrueType&) {
		BaseType::replace(i1, i2, n, c);
		return *this;
	}

	template<typename InputIterator>
	inline ThisType& replaceByRange(
			iterator i1, iterator i2, InputIterator first, InputIterator last,
			const FalseType&) {
		const ThisType str(first, last, this->get_allocator());
		BaseType::replace(i1, i2, str);
		return *this;
	}
};

template<typename CharT, typename Traits, typename Alloc>
inline BasicString<CharT, Traits, Alloc> operator+(
		const BasicString<CharT, Traits, Alloc> &lhs,
		const BasicString<CharT, Traits, Alloc> &rhs) {
	return BasicString<CharT, Traits, Alloc>(lhs) += rhs;
}

template<typename CharT, typename Traits, typename Alloc, typename R>
inline BasicString<CharT, Traits, Alloc> operator+(
		const BasicString<CharT, Traits, Alloc> &lhs, R rhs) {
	return BasicString<CharT, Traits, Alloc>(lhs) += rhs;
}

template<typename CharT, typename Traits, typename Alloc, typename L>
inline BasicString<CharT, Traits, Alloc> operator+(
		L lhs, const BasicString<CharT, Traits, Alloc> &rhs) {
	return (BasicString<CharT, Traits, Alloc>(rhs.get_allocator()) += lhs) += rhs;
}

typedef BasicString<
		char8_t,
		std::char_traits<char8_t>,
		StdAllocator<char8_t, StackAllocator> > String;

typedef BasicString<
		char8_t,
		std::char_traits<char8_t>,
		StdAllocator<char8_t, void> > AllocString;



namespace detail {
union AlignmentUnit {
	void *member1;
	double member2;
	int64_t member3;
};

struct AllocatorUtils {
	inline static size_t getAlignedSize(size_t size) {
		return (size + (sizeof(AlignmentUnit) - 1)) & ~(sizeof(AlignmentUnit) - 1);
	}
};

template<typename T>
struct AlignedSizeOf {
	enum Value {
		VALUE = (sizeof(T) +
				(sizeof(AlignmentUnit) - 1)) & ~(sizeof(AlignmentUnit) - 1)
	};
};
}	


template<typename Alloc>
AllocatorLimitter::Scope::Scope(AllocatorLimitter &limitter, Alloc &allocator) :
		limitter_(limitter),
		orgLimitter_(NULL),
		allocator_(&allocator),
		unbinder_(&unbind<Alloc>) {
	const bool force = false;
	orgLimitter_ = allocator.setLimit(
			AllocatorStats::STAT_GROUP_TOTAL_LIMIT, &limitter, force);
}

template<typename Alloc>
void AllocatorLimitter::Scope::unbind(
		void *allocator, AllocatorLimitter *orgLimitter) {
	try {
		const bool force = true;
		static_cast<Alloc*>(allocator)->setLimit(
				AllocatorStats::STAT_GROUP_TOTAL_LIMIT, orgLimitter, force);
	}
	catch (...) {
		assert(false);
	}
}


#if !UTIL_ALLOCATOR_FORCE_ALLOCATOR_INFO
template<typename Mutex>
inline FixedSizeAllocator<Mutex>::FixedSizeAllocator(
		size_t elementSize) :
		elementSize_(elementSize),
		totalElementLimit_(std::numeric_limits<size_t>::max()),
#if UTIL_ALLOCATOR_NO_CACHE_FIXED_ALLOCATOR
		freeElementLimit_(0),
		stableElementLimit_(0),
#else
		freeElementLimit_(std::numeric_limits<size_t>::max()),
		stableElementLimit_(std::numeric_limits<size_t>::max()),
#endif
		errorHandler_(NULL),
		freeLink_(NULL),
		totalElementCount_(0),
		freeElementCount_(0),
		cleanUpHandler_(NULL),
		stats_(AllocatorInfo()),
		limitter_(NULL) {
	assert(elementSize > 0);
	stats_.info_.setUnitSize(elementSize);
	util::AllocatorManager::addAllocator(stats_.info_, *this, limitter_);
}
#endif

template<typename Mutex>
inline FixedSizeAllocator<Mutex>::FixedSizeAllocator(
		const AllocatorInfo &info, size_t elementSize) :
		elementSize_(elementSize),
		totalElementLimit_(std::numeric_limits<size_t>::max()),
#if UTIL_ALLOCATOR_NO_CACHE_FIXED_ALLOCATOR
		freeElementLimit_(0),
		stableElementLimit_(0),
#else
		freeElementLimit_(std::numeric_limits<size_t>::max()),
		stableElementLimit_(std::numeric_limits<size_t>::max()),
#endif
		errorHandler_(NULL),
		freeLink_(NULL),
		totalElementCount_(0),
		freeElementCount_(0),
		cleanUpHandler_(NULL),
		stats_(info),
		limitter_(NULL) {
	assert(elementSize > 0);
	stats_.info_.setUnitSize(elementSize);
	util::AllocatorManager::addAllocator(stats_.info_, *this, limitter_);
}

template<typename Mutex>
inline FixedSizeAllocator<Mutex>::~FixedSizeAllocator() {
	AllocatorCleanUpHandler::cleanUpAll(cleanUpHandler_);

#if UTIL_ALLOCATOR_REPORTER_ENABLED2
	std::map<void*, std::string>::iterator it = stackTraceMap_.begin();
	for (;it != stackTraceMap_.end(); it++) {
		std::cout << elementSize_ << "," << it->first << "," << it->second << std::endl; 
	}
#endif

#if UTIL_ALLOCATOR_DEBUG_REPORTER_ENABLED
	detail::AllocatorDebugReporter::reportUnexpectedUsage(
			stats_, totalElementCount_, freeElementCount_);
#endif

	assert(totalElementCount_ == freeElementCount_);

	clear(0);

	assert(freeLink_ == NULL);
	assert(totalElementCount_ == 0);
	assert(freeElementCount_ == 0);

	util::AllocatorManager::removeAllocator(stats_.info_, *this);
}

template<typename Mutex>
inline void* FixedSizeAllocator<Mutex>::allocate() {
	return allocate(detail::EmptyAllocationRequester());
}

template<typename Mutex>
template<typename R>
inline void* FixedSizeAllocator<Mutex>::allocate(const R &requester) {
#if UTIL_ALLOCATOR_SUBSTITUTE_FIXED_ALLOCATOR
	void *element = UTIL_MALLOC(elementSize_);
	if (element == NULL) {
		try {
			UTIL_THROW_UTIL_ERROR_CODED(CODE_NO_MEMORY);
		}
		catch (util::Exception &e) {
			if (errorHandler_ != NULL) {
				(*errorHandler_)(e);
			}
			throw;
		}
	}
	return element;
#else
	LockGuard<Mutex> guard(mutex_);

	if (freeLink_ == NULL) {
		reserve();
	}

	if (R::REQUESTER_ENABLED) {
		acquireForRequester(requester);
	}

	FreeLink *element = freeLink_;
	freeLink_ = freeLink_->next_;

	assert(freeElementCount_ > 0);
	--freeElementCount_;

#if UTIL_ALLOCATOR_REPORTER_ENABLED2
	NormalOStringStream oss;
	oss << StackTraceUtils::getStackTrace;
	stackTraceMap_.insert(std::make_pair(element, oss.str()));
#endif

#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
	activationSaver_.onAllocated(element);
#endif

	return element;
#endif
}

template<typename Mutex>
inline void FixedSizeAllocator<Mutex>::deallocate(void *element) {
	deallocate(element, detail::EmptyAllocationRequester());
}

template<typename Mutex>
template<typename R>
inline void FixedSizeAllocator<Mutex>::deallocate(
		void *element, const R &requester) {
#if UTIL_ALLOCATOR_SUBSTITUTE_FIXED_ALLOCATOR
	UTIL_FREE(element);
#else
	LockGuard<Mutex> guard(mutex_);

	if (element == NULL) {
		return;
	}

#if UTIL_ALLOCATOR_REPORTER_ENABLED2
	assert(stackTraceMap_.find(element) != stackTraceMap_.end());
	stackTraceMap_.erase(element);
#endif

#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
	activationSaver_.onDeallocated(element);
#endif

	if (R::REQUESTER_ENABLED) {
		releaseForRequester(requester);
	}

	FreeLink *next = freeLink_;
	freeLink_ = static_cast<FreeLink*>(element);
	freeLink_->next_ = next;

	assert(freeElementCount_ < totalElementCount_);
	++freeElementCount_;
	if (freeElementCount_ > freeElementLimit_ ||
			totalElementCount_ > stableElementLimit_) {
		clear(freeElementLimit_);
	}
#endif
}

template<typename Mutex>
inline void FixedSizeAllocator<Mutex>::setTotalElementLimit(size_t limit) {
	UTIL_ALLOCATOR_DETAIL_STAT_GUARD(guard, mutex_);
	totalElementLimit_ = limit;
}

template<typename Mutex>
void FixedSizeAllocator<Mutex>::setFreeElementLimit(size_t limit) {
	UTIL_ALLOCATOR_DETAIL_STAT_GUARD(guard, mutex_);
#if UTIL_ALLOCATOR_NO_CACHE_FIXED_ALLOCATOR
	static_cast<void>(limit);
#else
	freeElementLimit_ = limit;
#endif
	clear(freeElementLimit_);
}

template<typename Mutex>
inline void FixedSizeAllocator<Mutex>::setErrorHandler(
		AllocationErrorHandler *errorHandler) {
	errorHandler_ = &errorHandler;
}

template<typename Mutex>
void FixedSizeAllocator<Mutex>::addCleanUpHandler(
		AllocatorCleanUpHandler &cleanUpHandler) {
	LockGuard<Mutex> guard(mutex_);
	cleanUpHandler.bind(cleanUpHandler_, &mutex_);
}

template<typename Mutex>
inline Mutex& FixedSizeAllocator<Mutex>::getLock() {
	return mutex_;
}

template<typename Mutex>
inline size_t FixedSizeAllocator<Mutex>::getTotalElementLimit() {
	UTIL_ALLOCATOR_DETAIL_STAT_GUARD(guard, mutex_);
	return totalElementLimit_;
}

template<typename Mutex>
inline size_t FixedSizeAllocator<Mutex>::getFreeElementLimit() {
	UTIL_ALLOCATOR_DETAIL_STAT_GUARD(guard, mutex_);
	return freeElementLimit_;
}

template<typename Mutex>
inline size_t FixedSizeAllocator<Mutex>::getElementSize() {
	return elementSize_;
}

template<typename Mutex>
inline size_t FixedSizeAllocator<Mutex>::getTotalElementCount() {
	UTIL_ALLOCATOR_DETAIL_STAT_GUARD(guard, mutex_);
	return totalElementCount_;
}

template<typename Mutex>
inline size_t FixedSizeAllocator<Mutex>::getFreeElementCount() {
	UTIL_ALLOCATOR_DETAIL_STAT_GUARD(guard, mutex_);
	return freeElementCount_;
}

template<typename Mutex>
void FixedSizeAllocator<Mutex>::getStats(AllocatorStats &stats) {
	UTIL_ALLOCATOR_DETAIL_STAT_GUARD(guard, mutex_);

	stats.merge(stats_);
	stats.values_[AllocatorStats::STAT_CACHE_SIZE] +=
			AllocatorStats::asStatValue(elementSize_ * freeElementCount_);
}

template<typename Mutex>
void FixedSizeAllocator<Mutex>::setLimit(
		AllocatorStats::Type type, size_t value) {
	UTIL_ALLOCATOR_DETAIL_STAT_GUARD(guard, mutex_);

	switch (type) {
#if UTIL_ALLOCATOR_NO_CACHE_FIXED_ALLOCATOR
	default:
		static_cast<void>(value);
		break;
#else
	case AllocatorStats::STAT_CACHE_LIMIT:
		freeElementLimit_ = value / elementSize_;
		break;
	case AllocatorStats::STAT_STABLE_LIMIT:
		stableElementLimit_ = value / elementSize_;
		break;
	default:
		break;
#endif
	}

	clear(freeElementLimit_);
}

template<typename Mutex>
AllocatorLimitter* FixedSizeAllocator<Mutex>::setLimit(
		AllocatorStats::Type type, AllocatorLimitter *limitter, bool force) {
	UTIL_ALLOCATOR_DETAIL_STAT_GUARD(guard, mutex_);

	AllocatorLimitter *orgLimitter = limitter_;
	switch (type) {
	case AllocatorStats::STAT_GROUP_TOTAL_LIMIT:
		limitter_ = AllocatorLimitter::moveSize(
				limitter, limitter_, elementSize_ * totalElementCount_, force);
		break;
	default:
		break;
	}
	return orgLimitter;
}

template<typename Mutex>
inline void FixedSizeAllocator<Mutex>::reserve() {
	UTIL_DETAIL_ALLOCATOR_REPORT_MISS_HIT(
			elementSize_, totalElementCount_, freeElementCount_);

	stats_.values_[AllocatorStats::STAT_CACHE_MISS_COUNT]++;

	const size_t rest = std::max(totalElementLimit_, totalElementCount_) -
			totalElementCount_;
	const size_t steadyRest =
			std::max(stableElementLimit_, totalElementCount_) -
			totalElementCount_;

#if UTIL_ALLOCATOR_FIXED_ALLOCATOR_RESERVE_UNIT_SMALL
	const size_t maxUnitSize = 1024;
#else
	const size_t maxUnitSize = 1024 * 1024;
#endif

	const size_t unitCount = std::min<size_t>(128, maxUnitSize / elementSize_);
	const size_t localCount = std::min(rest, std::max<size_t>(
			1, std::min(steadyRest, unitCount)));

	try {
		if (localCount == 0) {
			UTIL_THROW_UTIL_ERROR(CODE_MEMORY_LIMIT_EXCEEDED,
					"Memory limit exceeded ("
					"name=" << stats_.info_ <<
					", elementSize=" << elementSize_ <<
					", totalElementLimit=" << totalElementLimit_ <<
					", totalElementCount=" << totalElementCount_ <<
					", freeElementCount=" << freeElementCount_ << ")");
		}

		size_t count;
		if (limitter_ == NULL) {
			count = localCount;
		}
		else {
			const size_t acquired =
					limitter_->acquire(elementSize_, elementSize_ * localCount);
			if (acquired % elementSize_ != 0) {
				limitter_->release(acquired % elementSize_);
			}
			count = acquired / elementSize_;
		}
		assert(count > 0);

		const size_t allocSize = std::max(elementSize_, sizeof(FreeLink));
#if UTIL_MEMORY_POOL_AGGRESSIVE
		FreeLink *elementLink = detail::DirectAllocationUtils::allocateBulk(
				allocSize, true, true, count);
		if (count <= 0) {
			count = 1;
		}
#endif
		for (size_t i = count; i > 0; --i) {
#if UTIL_MEMORY_POOL_AGGRESSIVE
			void *element = elementLink;
#else
			void *element = UTIL_MALLOC_MONITORING(allocSize);
#endif

			if (element == NULL) {
				if (limitter_ != NULL) {
					limitter_->release(elementSize_ * count);
				}

				UTIL_THROW_UTIL_ERROR(CODE_NO_MEMORY,
						"Memory allocation failed ("
						"name=" << stats_.info_ <<
						", elementSize=" << elementSize_ <<
						", totalElementCount=" << totalElementCount_ <<
						", freeElementCount=" << freeElementCount_ << ")");
			}

#if UTIL_MEMORY_POOL_AGGRESSIVE
			elementLink = elementLink->next_;
#endif

			FreeLink *next = freeLink_;
			freeLink_ = static_cast<FreeLink*>(element);
			freeLink_->next_ = next;

			++totalElementCount_;
			++freeElementCount_;

#if UTIL_ALLOCATOR_REPORTER_ENABLED3
			if(totalElementCount_ > g_limitCount) {
				try {
					UTIL_THROW_UTIL_ERROR(0, "");
				}
				catch (util::Exception &e) {
					NormalOStringStream oss;
					e.format(oss);
					std::cout << g_limitCountCount++ << ":" << oss.str() << std::endl;
				}
			}
#endif
		}

		const int64_t lastTotalSize = AllocatorStats::asStatValue(
				elementSize_ * totalElementCount_);
		if (stats_.values_[AllocatorStats::STAT_PEAK_TOTAL_SIZE] < lastTotalSize) {
			stats_.values_[AllocatorStats::STAT_PEAK_TOTAL_SIZE] = lastTotalSize;
		}
		stats_.values_[AllocatorStats::STAT_TOTAL_SIZE] = lastTotalSize;
	}
	catch (util::Exception &e) {
		if (errorHandler_ != NULL) {
			(*errorHandler_)(e);
		}
		throw;
	}
}

template<typename Mutex>
inline void FixedSizeAllocator<Mutex>::clear(size_t preservedCount) {
	size_t releasedCount = 0;

#if UTIL_MEMORY_POOL_AGGRESSIVE
	FreeLink *elementLink = NULL;
#endif
	for (; freeLink_ != NULL; --freeElementCount_, --totalElementCount_) {
		if (freeElementCount_ <= preservedCount &&
				totalElementCount_ <= stableElementLimit_) {
			break;
		}

		FreeLink *next = freeLink_->next_;
#if UTIL_MEMORY_POOL_AGGRESSIVE
		freeLink_->next_ = elementLink;
		elementLink = freeLink_;
#else
		UTIL_FREE_MONITORING(freeLink_);
#endif
		freeLink_ = next;
		releasedCount++;
	}
#if UTIL_MEMORY_POOL_AGGRESSIVE
	const size_t allocSize = std::max(elementSize_, sizeof(FreeLink));
	detail::DirectAllocationUtils::deallocateBulk(
			elementLink, allocSize, true, true);
#endif

	if (releasedCount > 0) {
		stats_.values_[AllocatorStats::STAT_CACHE_ADJUST_COUNT]++;
		stats_.values_[AllocatorStats::STAT_TOTAL_SIZE] =
				AllocatorStats::asStatValue(
						elementSize_ * totalElementCount_);

		if (limitter_ != NULL) {
			limitter_->release(elementSize_ * releasedCount);
		}
	}
}

template<typename Mutex>
template<typename R>
inline void FixedSizeAllocator<Mutex>::acquireForRequester(
		const R &requester) {
	if (requester.acquire(elementSize_, limitter_)) {
		assert(totalElementCount_ > 0);
		totalElementCount_--;

		int64_t &statValue = stats_.values_[AllocatorStats::STAT_TOTAL_SIZE];
		const int64_t statSize = static_cast<int64_t>(elementSize_);

		assert(statValue >= statSize);
		statValue -= statSize;
	}
}

template<typename Mutex>
template<typename R>
inline void FixedSizeAllocator<Mutex>::releaseForRequester(
		const R &requester) {
	if (requester.release(elementSize_, limitter_)) {
		totalElementCount_++;

		int64_t &statValue = stats_.values_[AllocatorStats::STAT_TOTAL_SIZE];
		const int64_t statSize = static_cast<int64_t>(elementSize_);

		statValue += statSize;
	}
}


template<typename L>
inline void* FixedSizeCachedAllocator::allocate(L *mutex) {
	if (findLocal(mutex)) {
		return allocateLocal(mutex);
	}

	LockGuard<L> guard(*mutex);

	if (freeLink_ == NULL) {
		reserve();
	}

	FreeLink *element = freeLink_;
	freeLink_ = freeLink_->next_;

	assert(freeElementCount_ > 0);
	--freeElementCount_;

	return element;
}

template<typename L>
inline void FixedSizeCachedAllocator::deallocate(L *mutex, void *element) {
	if (element == NULL) {
		return;
	}

	if (findLocal(mutex)) {
		deallocateLocal(mutex, element);
		return;
	}

	LockGuard<L> guard(*mutex);

	FreeLink *next = freeLink_;
	freeLink_ = static_cast<FreeLink*>(element);
	freeLink_->next_ = next;

	++freeElementCount_;
	if (freeElementCount_ > freeElementLimit_) {
		shrink();
	}
}

inline bool FixedSizeCachedAllocator::findLocal(const NoopMutex*) {
	return (localAlloc_.get() != NULL);
}

inline bool FixedSizeCachedAllocator::findLocal(const Mutex*) {
	return (localLockedAlloc_.get() != NULL);
}

template<typename L>
void FixedSizeCachedAllocator::setLimit(
		 L *mutex, AllocatorStats::Type type, size_t value) {
	if (findLocal(mutex)) {
		setLimitLocal(mutex, type, value);
		return;
	}

	LockGuard<L> guard(*mutex);

	switch (type) {
	case AllocatorStats::STAT_CACHE_LIMIT:
		freeElementLimit_ = value / getElementSize();
		break;
	default:
		break;
	}
}


template<size_t SmallSize, size_t MiddleSize, size_t LargeSize>
inline size_t VariableSizeAllocatorTraits<
		SmallSize, MiddleSize, LargeSize>::getFixedSize(size_t index) {
	switch (index) {
	case 0:
		return SmallSize;
	case 1:
		return MiddleSize;
	case 2:
		return LargeSize;
	default:
		assert(false);
		return std::numeric_limits<size_t>::max();
	}
}

template<size_t SmallSize, size_t MiddleSize, size_t LargeSize>
inline size_t VariableSizeAllocatorTraits<
		SmallSize, MiddleSize, LargeSize>::selectFixedAllocator(size_t size) {
	if (size <= SmallSize) {
		return 0;
	}
	else if (size <= MiddleSize) {
		return 1;
	}
	else if (size <= LargeSize) {
		return 2;
	}
	else {
		return 3;
	}
}


#if !UTIL_ALLOCATOR_FORCE_ALLOCATOR_INFO
template<typename Mutex, typename Traits>
inline VariableSizeAllocator<Mutex, Traits>::VariableSizeAllocator(
		const Traits &traits) :
		errorHandler_(NULL),
		hugeElementCount_(0),
		hugeElementSize_(0),
		traits_(traits),
		stats_(AllocatorInfo()),
		limitter_(NULL) {
	initialize(traits);
}
#endif

template<typename Mutex, typename Traits>
inline VariableSizeAllocator<Mutex, Traits>::VariableSizeAllocator(
		const AllocatorInfo &info, const Traits &traits) :
		errorHandler_(NULL),
		hugeElementCount_(0),
		hugeElementSize_(0),
		traits_(traits),
		stats_(info),
		limitter_(NULL) {
	initialize(traits);
}

template<typename Mutex, typename Traits>
inline VariableSizeAllocator<Mutex, Traits>::~VariableSizeAllocator() {
	assert(hugeElementCount_ == 0);
	assert(hugeElementSize_ == 0);

#if UTIL_ALLOCATOR_REPORTER_ENABLED2
	std::map<void*, std::string>::iterator it = stackTraceMap_.begin();
	for (;it != stackTraceMap_.end(); it++) {
		std::cout << "hugeElement" << "," << it->first << "," << it->second << std::endl; 
	}
#endif

	try {
		clear();
	}
	catch (...) {
	}
}

template<typename Mutex, typename Traits>
inline void* VariableSizeAllocator<Mutex, Traits>::allocate(size_t size) {
#if UTIL_ALLOCATOR_SUBSTITUTE_VAR_ALLOCATOR
	void *element = UTIL_MALLOC(size);
	if (element == NULL) {
		try {
			UTIL_THROW_UTIL_ERROR_CODED(CODE_NO_MEMORY);
		}
		catch (util::Exception &e) {
			if (errorHandler_ != NULL) {
				(*errorHandler_)(e);
			}
			throw;
		}
	}
	return element;
#else
	const size_t offset = detail::AlignedSizeOf<size_t>::VALUE;
	const size_t totalSize = offset + size;
	const size_t index = traits_.selectFixedAllocator(totalSize);

	void *ptr;
	try {
		if (index < FIXED_ALLOCATOR_COUNT) {
#if UTIL_ALLOCATOR_VAR_ALLOCATOR_POOLING
			ptr = baseList_[index]->allocate(&mutex_);
#else
			ptr = baseList_[index]->allocate();
#endif
		}
		else {
			UTIL_DETAIL_ALLOCATOR_REPORT_MISS_HIT(
					totalSize, hugeElementSize_, 0);

			LockGuard<Mutex> guard(mutex_);
			stats_.values_[AllocatorStats::STAT_CACHE_MISS_COUNT]++;

			if (limitter_ != NULL) {
				limitter_->acquire(totalSize, totalSize);
			}

			ptr = UTIL_MALLOC_MONITORING(totalSize);
			if (ptr == NULL) {
				if (limitter_ != NULL) {
					limitter_->release(totalSize);
				}

				UTIL_THROW_UTIL_ERROR(CODE_NO_MEMORY,
						"Memory allocation failed ("
						"name=" << stats_.info_ <<
						", requestedSize=" << size <<
						", totalSize=" << totalSize << ")");
			}

			++hugeElementCount_;
			hugeElementSize_ += totalSize;

			stats_.values_[AllocatorStats::STAT_TOTAL_SIZE] =
					AllocatorStats::asStatValue(hugeElementSize_);
			stats_.values_[AllocatorStats::STAT_PEAK_TOTAL_SIZE] = std::max(
					stats_.values_[AllocatorStats::STAT_PEAK_TOTAL_SIZE],
					stats_.values_[AllocatorStats::STAT_TOTAL_SIZE]);

#if UTIL_ALLOCATOR_REPORTER_ENABLED2
			NormalOStringStream oss;
			oss << StackTraceUtils::getStackTrace;
			stackTraceMap_.insert(std::make_pair(ptr, oss.str()));
#endif

#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
			activationSaver_.onAllocated(ptr);
#endif
		}
	}
	catch (Exception &e) {
		if (errorHandler_ != NULL) {
			(*errorHandler_)(e);
		}
		throw;
	}

	*static_cast<size_t*>(ptr) = totalSize;
	return static_cast<uint8_t*>(ptr) + offset;
#endif
}

template<typename Mutex, typename Traits>
inline void VariableSizeAllocator<Mutex, Traits>::deallocate(void *element) {
#if UTIL_ALLOCATOR_SUBSTITUTE_VAR_ALLOCATOR
	UTIL_FREE(element);
#else
	if (element == NULL) {
		return;
	}

	const size_t offset = detail::AlignedSizeOf<size_t>::VALUE;
	void *ptr = static_cast<uint8_t*>(element) - offset;

	const size_t totalSize = *static_cast<size_t*>(ptr);
	const size_t index = traits_.selectFixedAllocator(totalSize);

	if (index < FIXED_ALLOCATOR_COUNT) {
#if UTIL_ALLOCATOR_VAR_ALLOCATOR_POOLING
		baseList_[index]->deallocate(&mutex_, ptr);
#else
		baseList_[index]->deallocate(ptr);
#endif
	}
	else {
		LockGuard<Mutex> guard(mutex_);

		stats_.values_[AllocatorStats::STAT_CACHE_ADJUST_COUNT]++;

		assert(hugeElementCount_ > 0);
		assert(hugeElementSize_ >= totalSize);

#if UTIL_ALLOCATOR_REPORTER_ENABLED2
		assert(stackTraceMap_.find(ptr) != stackTraceMap_.end());
		stackTraceMap_.erase(ptr);
#endif

#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
		activationSaver_.onDeallocated(ptr);
#endif

		UTIL_FREE_MONITORING(ptr);

		--hugeElementCount_;
		hugeElementSize_ -= totalSize;

		stats_.values_[AllocatorStats::STAT_TOTAL_SIZE] =
				AllocatorStats::asStatValue(hugeElementSize_);

		if (limitter_ != NULL) {
			limitter_->release(totalSize);
		}
	}
#endif
}

template<typename Mutex, typename Traits>
template<typename T>
inline void VariableSizeAllocator<Mutex, Traits>::deleteObject(T *object) {
	if (object != NULL) {
		object->~T();
		deallocate(object);
	}
}

template<typename Mutex, typename Traits>
inline void VariableSizeAllocator<Mutex, Traits>::setErrorHandler(
		AllocationErrorHandler *errorHandler) {
	for (size_t i = FIXED_ALLOCATOR_COUNT; i > 0; --i) {
		const size_t index = i - 1;
		baseList_[index]->setErrorHandler(errorHandler);
	}
	errorHandler_ = errorHandler;
}

template<typename Mutex, typename Traits>
inline typename VariableSizeAllocator<Mutex, Traits>::BaseAllocator*
VariableSizeAllocator<Mutex, Traits>::base(size_t index) {
	assert(index < FIXED_ALLOCATOR_COUNT);
#if UTIL_ALLOCATOR_VAR_ALLOCATOR_POOLING
	return baseList_[index]->base(&mutex_);
#else
	return baseList_[index];
#endif
}

template<typename Mutex, typename Traits>
size_t VariableSizeAllocator<Mutex, Traits>::getElementHeadSize() {
	return detail::AlignedSizeOf<size_t>::VALUE;
}

template<typename Mutex, typename Traits>
inline size_t VariableSizeAllocator<Mutex, Traits>::getTotalElementSize() {
	size_t size = getHugeElementSize();

	for (size_t i = FIXED_ALLOCATOR_COUNT; i > 0; --i) {
		const size_t index = i - 1;
		size += baseList_[index]->getElementSize() *
				baseList_[index]->getTotalElementCount();
	}

	return size;
}

template<typename Mutex, typename Traits>
inline size_t VariableSizeAllocator<Mutex, Traits>::getFreeElementSize() {
	size_t size = 0;

	for (size_t i = FIXED_ALLOCATOR_COUNT; i > 0; --i) {
		const size_t index = i - 1;
		size += baseList_[index]->getElementSize() *
				baseList_[index]->getFreeElementCount();
	}

	return size;
}

template<typename Mutex, typename Traits>
inline size_t VariableSizeAllocator<Mutex, Traits>::getHugeElementCount() {
	return static_cast<size_t>(hugeElementCount_);
}

template<typename Mutex, typename Traits>
inline size_t VariableSizeAllocator<Mutex, Traits>::getHugeElementSize() {
	return static_cast<size_t>(hugeElementSize_);
}

template<typename Mutex, typename Traits>
inline size_t VariableSizeAllocator<Mutex, Traits>::getElementCapacity(
		const void *element) {
	if (element == NULL) {
		return 0;
	}

	const size_t offset = detail::AlignedSizeOf<size_t>::VALUE;
	const void *ptr = static_cast<const uint8_t*>(element) - offset;

	const size_t totalSize = *static_cast<const size_t*>(ptr);
	const size_t index = traits_.selectFixedAllocator(totalSize);

	if (index < FIXED_ALLOCATOR_COUNT) {
		return traits_.getFixedSize(index) - offset;
	}
	else {
		return totalSize - offset;
	}
}

template<typename Mutex, typename Traits>
void VariableSizeAllocator<Mutex, Traits>::getStats(AllocatorStats &stats) {
	stats.merge(stats_);
}

template<typename Mutex, typename Traits>
void VariableSizeAllocator<Mutex, Traits>::setLimit(
		AllocatorStats::Type type, size_t value) {
	(void) type;
	(void) value;
}

template<typename Mutex, typename Traits>
AllocatorLimitter* VariableSizeAllocator<Mutex, Traits>::setLimit(
		AllocatorStats::Type type, AllocatorLimitter *limitter, bool force) {
	UTIL_ALLOCATOR_DETAIL_STAT_GUARD(guard, mutex_);

	AllocatorLimitter *orgLimitter = limitter_;
	switch (type) {
	case AllocatorStats::STAT_GROUP_TOTAL_LIMIT:
		limitter_ = AllocatorLimitter::moveSize(
				limitter, limitter_, hugeElementSize_, force);
		for (size_t i = 0; i < FIXED_ALLOCATOR_COUNT; i++) {
			baseList_[i]->setLimit(type, limitter, force);
		}
		break;
	default:
		break;
	}
	return orgLimitter;
}

template<typename Mutex, typename Traits>
void VariableSizeAllocator<Mutex, Traits>::setBaseLimit(
		size_t index, AllocatorStats::Type type, size_t value) {
	assert(index < FIXED_ALLOCATOR_COUNT);
#if UTIL_ALLOCATOR_VAR_ALLOCATOR_POOLING
	baseList_[index]->setLimit(&mutex_, type, value);
#else
	baseList_[index]->setLimit(type, value);
#endif
}

template<typename Mutex, typename Traits>
inline void VariableSizeAllocator<Mutex, Traits>::initialize(
		const Traits &traits) {
#if UTIL_ALLOCATOR_VAR_ALLOCATOR_POOLING
	try {
		const size_t count = FIXED_ALLOCATOR_COUNT;
		size_t elementSizeList[count];
		for (size_t i = count; i > 0; --i) {
			const size_t index = i - 1;
			elementSizeList[index] = traits.getFixedSize(index);
		}

		const bool locked = !(IsSame<Mutex, NoopMutex>::VALUE);
		VariableSizeAllocatorPool::initializeSubAllocators(
				stats_.info_, traits.getPool(), baseList_, elementSizeList,
				count, locked);
		util::AllocatorManager::addAllocator(stats_.info_, *this, limitter_);
	}
	catch (...) {
		clear();
		throw;
	}
#else
	static_cast<void>(traits);
	std::fill(baseList_, baseList_ + FIXED_ALLOCATOR_COUNT,
			static_cast<FixedSizeAllocator<Mutex>*>(NULL));

	try {
		for (size_t i = FIXED_ALLOCATOR_COUNT; i > 0; --i) {
			const size_t index = i - 1;
			baseList_[index] = UTIL_NEW BaseAllocator(
					stats_.info_, traits_.getFixedSize(index));
		}
		util::AllocatorManager::addAllocator(stats_.info_, *this, limitter_);
	}
	catch (...) {
		clear();
		throw;
	}
#endif
}

template<typename Mutex, typename Traits>
inline void VariableSizeAllocator<Mutex, Traits>::clear() {
#if !UTIL_ALLOCATOR_VAR_ALLOCATOR_POOLING
	for (size_t i = FIXED_ALLOCATOR_COUNT; i > 0; --i) {
		const size_t index = i - 1;

		delete baseList_[index];
		baseList_[index] = NULL;
	}
#endif

	util::AllocatorManager::removeAllocator(stats_.info_, *this);
}


inline void* StackAllocator::allocate(size_t size) {
	UTIL_ALLOCATOR_DETAIL_LOCK_GUARD_STACK_ALLOCATOR(this);

#if UTIL_FAILURE_SIMULATION_ENABLED
	AllocationFailureSimulator::checkOperation(
			AllocationFailureSimulator::TARGET_STACK_ALLOCATION, size);
#endif

	size = detail::AllocatorUtils::getAlignedSize(size);

#if UTIL_ALLOCATOR_REPORTER_ENABLED3
	if(size > g_limitSize) {
		try {
			UTIL_THROW_UTIL_ERROR(0, "");
		}
		catch (util::Exception &e) {
			NormalOStringStream oss;
			e.format(oss);
			std::cout << g_limitSizeCount++ << ":" << oss.str() << std::endl;
		}
	}
#endif

	if (size > restSize_) {
		return allocateOverBlock(size);
	}

	void *ptr = end_ - restSize_;
	restSize_ -= size;

	return ptr;
}

inline void StackAllocator::deallocate(void *ptr) {
	UTIL_ALLOCATOR_DETAIL_LOCK_GUARD_STACK_ALLOCATOR(this);
	(void) ptr;
}

template<typename T> void StackAllocator::deleteObject(T *object) {
	if (object != NULL) {
		object->~T();
		deallocate(object);
	}
}

inline void StackAllocator::setErrorHandler(
		AllocationErrorHandler *errorHandler) {
	UTIL_ALLOCATOR_DETAIL_LOCK_GUARD_STACK_ALLOCATOR(this);
	errorHandler_ = errorHandler;
}

inline void StackAllocator::setDefaultErrorHandler(
		AllocationErrorHandler *errorHandler) {
	defaultErrorHandler_ = errorHandler;
}

inline void StackAllocator::setTotalSizeLimit(size_t limit) {
	UTIL_ALLOCATOR_DETAIL_STAT_GUARD_STACK_ALLOCATOR(this);
	totalSizeLimit_ = limit;
}

inline void StackAllocator::setFreeSizeLimit(size_t limit) {
	UTIL_ALLOCATOR_DETAIL_STAT_GUARD_STACK_ALLOCATOR(this);

#if UTIL_ALLOCATOR_NO_CACHE_STACK_ALLOCATOR
	static_cast<void>(limit);
#else
	freeSizeLimit_ = limit;
#endif
}

inline size_t StackAllocator::getTotalSizeLimit() {
	UTIL_ALLOCATOR_DETAIL_STAT_GUARD_STACK_ALLOCATOR(this);
	return totalSizeLimit_;
}

inline size_t StackAllocator::getFreeSizeLimit() {
	UTIL_ALLOCATOR_DETAIL_STAT_GUARD_STACK_ALLOCATOR(this);
	return freeSizeLimit_;
}

inline size_t StackAllocator::getTotalSize() {
	UTIL_ALLOCATOR_DETAIL_STAT_GUARD_STACK_ALLOCATOR(this);
	return totalSize_;
}

inline size_t StackAllocator::getFreeSize() {
	UTIL_ALLOCATOR_DETAIL_STAT_GUARD_STACK_ALLOCATOR(this);
	return freeSize_;
}

inline size_t StackAllocator::getHugeCount() {
	UTIL_ALLOCATOR_DETAIL_STAT_GUARD_STACK_ALLOCATOR(this);
	return hugeCount_;
}

inline size_t StackAllocator::getHugeSize() {
	UTIL_ALLOCATOR_DETAIL_STAT_GUARD_STACK_ALLOCATOR(this);
	return hugeSize_;
}

inline StackAllocator::BaseAllocator& StackAllocator::base() {
	return base_;
}

inline void StackAllocator::push(BlockHead *&lastBlock, size_t &lastRestSize) {
	lastBlock = topBlock_;
	lastRestSize = restSize_;
}


inline uint8_t* StackAllocator::BlockHead::body() {
	return reinterpret_cast<uint8_t*>(this) +
			detail::AlignedSizeOf<BlockHead>::VALUE;
}

inline uint8_t* StackAllocator::BlockHead::end() {
	return reinterpret_cast<uint8_t*>(this) + blockSize_;
}

inline size_t StackAllocator::BlockHead::bodySize() {
	return blockSize_ - detail::AlignedSizeOf<BlockHead>::VALUE;
}


inline StackAllocator::Scope::Scope(StackAllocator &allocator) :
		allocator_(allocator) {
	UTIL_ALLOCATOR_DETAIL_LOCK_GUARD_STACK_ALLOCATOR(&allocator);
	allocator_.push(lastBlock_, lastRestSize_);
}

inline StackAllocator::Scope::~Scope() {
	UTIL_ALLOCATOR_DETAIL_LOCK_GUARD_STACK_ALLOCATOR(&allocator_);
	allocator_.pop(lastBlock_, lastRestSize_);
}


inline StackAllocator::ConfigScope::ConfigScope(StackAllocator &allocator) :
		allocator_(&allocator),
		totalSizeLimit_(allocator.getTotalSizeLimit()),
		freeSizeLimit_(allocator.getFreeSizeLimit()) {
}

inline StackAllocator::ConfigScope::~ConfigScope() {
	reset();
}

inline void StackAllocator::ConfigScope::reset() {
	if (allocator_ == NULL) {
		return;
	}

	allocator_->setTotalSizeLimit(totalSizeLimit_);
	allocator_->setFreeSizeLimit(freeSizeLimit_);
	allocator_ = NULL;
}

#if !UTIL_ALLOC_CXX_MOVE_ENABLED

namespace detail {

template<typename T>
Movable<T>::Movable(T &value) throw() : chain_(localChain_) {
	value.swap(chain_.value_);
}

template<typename T>
Movable<T>::~Movable() {
	chain_.remove();
}

template<typename T>
Movable<T>::Movable(const Movable &another) throw() : chain_(localChain_) {
	*this = another;
}

template<typename T>
Movable<T>& Movable<T>::operator=(const Movable &another) throw() {
	if (this != &another) {
		another.chain_.add(chain_);
	}
	return *this;
}

template<typename T>
void Movable<T>::release(T &value) const throw() {
	Chain &top = chain_.top();

	top.value_.swap(value);
	top.clearValue();

	chain_.remove();
}

template<typename T>
Movable<T>::Chain::Chain() :
		next_(NULL),
		prev_(NULL) {
}

template<typename T>
bool Movable<T>::Chain::isTop() const throw() {
	return (prev_ == NULL);
}

template<typename T>
typename Movable<T>::Chain& Movable<T>::Chain::top() throw() {
	Chain *chain = this;
	for (; !chain->isTop(); chain = chain->prev_) {
	}
	return *chain;
}

template<typename T>
void Movable<T>::Chain::add(Chain &chain) throw() {
	chain.remove();
	chain.clearValue();

	if (next_ != NULL) {
		chain.next_ = next_;
		next_->prev_ = &chain;
	}

	if (prev_ != NULL) {
		chain.prev_ = prev_;
		prev_->next_ = &chain;
	}
}

template<typename T>
void Movable<T>::Chain::remove() throw() {
	if (next_ != NULL) {
		if (isTop()) {
			next_->value_.swap(value_);
		}
		next_->prev_ = prev_;
		next_ = NULL;
	}

	if (prev_ != NULL) {
		prev_->next_ = next_;
		prev_ = NULL;
	}
}

template<typename T>
void Movable<T>::Chain::clearValue() throw() {
	T emptyValue;
	value_.swap(emptyValue);
}

} 

#endif 


template<typename T, typename Alloc>
inline AllocDefaultDelete<T, Alloc>::AllocDefaultDelete(
		const Alloc &alloc) throw() : alloc_(alloc) {
}

template<typename T, typename Alloc>
template<typename U, typename OtherAlloc>
inline AllocDefaultDelete<T, Alloc>::AllocDefaultDelete(
		const AllocDefaultDelete<U, OtherAlloc> &other) throw() :
		alloc_(detail::EmptyObjectTraits<Alloc>::create()) {
	other.copyTo(*this);
}

template<typename T, typename Alloc>
template<typename U, typename OtherAlloc>
inline void AllocDefaultDelete<T, Alloc>::copyTo(
		AllocDefaultDelete<U, OtherAlloc> &other) const throw() {
	const OtherAlloc &otherAlloc = alloc_;
	other = AllocDefaultDelete<U, OtherAlloc>(otherAlloc);
}

template<typename T, typename Alloc>
inline void AllocDefaultDelete<T, Alloc>::operator()(T *ptr) {
	if (ptr != NULL) {
		if (detail::EmptyObjectTraits<Alloc>::isEmpty(alloc_)) {
			assert(false);
			return;
		}
		alloc_.destroy(ptr);
		alloc_.deallocate(ptr, 1);
	}
}

template<typename T, typename Alloc>
inline AllocDefaultDelete<T, Alloc>
AllocDefaultDelete<T, Alloc>::EmptyConstructor::operator()() const throw() {
	return AllocDefaultDelete(
			detail::EmptyObjectTraits<Alloc>::create());
}

template<typename T, typename Alloc>
inline bool AllocDefaultDelete<T, Alloc>::EmptyConstructor::operator()(
		const AllocDefaultDelete &obj) const throw() {
	return detail::EmptyObjectTraits<Alloc>::isEmpty(obj);
}


template<typename T, typename D>
inline AllocUniquePtr<T, D>::~AllocUniquePtr() {
	reset();
}

template<typename T, typename D>
template<typename B>
inline AllocUniquePtr<T, D>::AllocUniquePtr(
		pointer ptr, B &deleterBase) throw() :
		ptr_(ptr),
		deleter_(deleterBase) {
}

template<typename T, typename D>
template<typename B>
inline typename AllocUniquePtr<T, D>::ReturnType AllocUniquePtr<T, D>::of(
		pointer ptr, B &deleterBase) throw() {
	AllocUniquePtr uniquePtr(ptr, deleterBase);
	return uniquePtr;
}

template<typename T, typename D>
AllocUniquePtr<T, D>::AllocUniquePtr(MovableRefType another) throw() {
	*this = another;
}

template<typename T, typename D>
AllocUniquePtr<T, D>& AllocUniquePtr<T, D>::operator=(
		MovableRefType another) throw() {
	another.release(*this);
	return *this;
}

template<typename T, typename D>
template<typename U, typename E>
AllocUniquePtr<T, D>::AllocUniquePtr(
		const detail::Movable< AllocUniquePtr<U, E> > &other) throw() {
	*this = other;
}

template<typename T, typename D>
template<typename U, typename E>
AllocUniquePtr<T, D>& AllocUniquePtr<T, D>::operator=(
		const detail::Movable< AllocUniquePtr<U, E> > &other) throw() {
	UTIL_STATIC_ASSERT(FOR_ANY);

	AllocUniquePtr<U, E> tmp;
	other.release(tmp);

	deleter_ = WrappedDeleter(tmp.get_deleter(), tmp.get());
	ptr_.get() = tmp.release();

	return *this;
}

template<typename T, typename D>
template<typename U>
U* AllocUniquePtr<T, D>::getAs() const throw() {
	UTIL_STATIC_ASSERT((IsSame<T, U>::VALUE || FOR_ANY));
	if (!IsSame<T, U>::VALUE && deleter_.template detectNonDeletable<U>()) {
		return NULL;
	}
	return static_cast<U*>(ptr_.get());
}

template<typename T, typename D>
template<typename U>
U& AllocUniquePtr<T, D>::resolveAs() const {
	U *ptr = getAs<U>();
	assert(ptr != NULL);
	return *ptr;
}

template<typename T, typename D>
inline typename AllocUniquePtr<T, D>::pointer
AllocUniquePtr<T, D>::release() throw() {
	pointer ptr = ptr_.get();
	ptr_.get() = pointer();
	return ptr;
}

template<typename T, typename D>
inline void AllocUniquePtr<T, D>::reset() throw() {
	try {
		deleter_(ptr_.get());
	}
	catch (...) {
		assert(false);
	}
	ptr_.get() = pointer();
}

template<typename T, typename D>
inline void AllocUniquePtr<T, D>::reset(pointer ptr) throw() {
	reset();
	ptr_.get() = ptr;
}

template<typename T, typename D>
inline void AllocUniquePtr<T, D>::swap(AllocUniquePtr &another) throw() {
	std::swap(deleter_, another.deleter_);
	std::swap(ptr_, another.ptr_);
}


namespace detail {
inline LocalUniquePtrBuilder::LocalUniquePtrBuilder(
		void *storage, size_t size, bool constructed) :
		storage_(storage),
		size_(size),
		constructed_(constructed) {
}

inline void* LocalUniquePtrBuilder::getStorage(size_t size) const {
	if (constructed_ || size != size_) {
		assert(false);
		return NULL;
	}
	return storage_;
}

inline void* LocalUniquePtrBuilder::get() const throw() {
	if (!constructed_) {
		return NULL;
	}
	return storage_;
}

} 

template<typename T>
inline LocalUniquePtr<T>::~LocalUniquePtr() {
	reset();
}

template<typename T>
inline typename LocalUniquePtr<T>::element_type&
LocalUniquePtr<T>::operator*() const {
	assert(ptr_.get() != pointer());
	return *ptr_.get();
}

template<typename T>
inline typename LocalUniquePtr<T>::pointer
LocalUniquePtr<T>::operator->() const throw() {
	assert(ptr_.get() != pointer());
	return ptr_.get();
}

template<typename T>
inline typename LocalUniquePtr<T>::pointer
LocalUniquePtr<T>::release() throw() {
	pointer ptr = ptr_.get();
	ptr_.get() = pointer();
	return ptr;
}

template<typename T>
inline void LocalUniquePtr<T>::reset() throw() {
	pointer ptr = release();
	if (ptr != pointer()) {
		ptr->~T();
	}
}

template<typename T>
inline LocalUniquePtr<T>& LocalUniquePtr<T>::operator=(
		const detail::LocalUniquePtrBuilder &builder) {
	if (ptr_.get() != NULL || storage_.address() != builder.get()) {
		assert(false);
		return *this;
	}

	ptr_.get() = static_cast<T*>(storage_.address());
	return *this;
}

template<typename T>
inline detail::LocalUniquePtrBuilder LocalUniquePtr<T>::toBuilder(T *ptr) {
	reset();

	void *storage = storage_.address();
	const bool constructed = (ptr != NULL);

	if (constructed && storage != ptr) {
		assert(false);
		storage = NULL;
	}

	return detail::LocalUniquePtrBuilder(storage, sizeof(T), constructed);
}


template<typename Alloc>
inline bool AllocatorManager::addAllocator(
		GroupId id, Alloc &alloc, AllocatorLimitter *&limitter) {
	AllocatorEntry entry;
	entry.allocator_ = &alloc;
	entry.accessor_ = &Accessor<Alloc>::access;
	return addAllocatorDetail(id, entry, limitter);
}

template<typename Alloc>
inline bool AllocatorManager::addAllocator(
		const AllocatorInfo &info, Alloc &alloc, AllocatorLimitter *&limitter) {
	return info.resolveManager().addAllocator(
			info.getGroupId(), alloc, limitter);
}

template<typename Alloc>
inline bool AllocatorManager::removeAllocator(
		GroupId id, Alloc &alloc) throw() {
	return removeAllocatorDetail(id, &alloc);
}

template<typename Alloc>
inline bool AllocatorManager::removeAllocator(
		const AllocatorInfo &info, Alloc &alloc) {
	return info.resolveManager().removeAllocator(info.getGroupId(), alloc);
}

template<typename InsertIterator>
inline void AllocatorManager::listSubGroup(
		GroupId id, InsertIterator it, bool recursive) {
	listSubGroupDetail(id, &it, &insertGroupId<InsertIterator>, recursive);
}

template<typename InsertIterator>
inline void AllocatorManager::getAllocatorStats(
		const GroupId *idList, size_t idCount, InsertIterator it) {
	getAllocatorStatsDetail(
			idList, idCount, &it, &insertStats<InsertIterator>);
}

template<typename InsertIterator>
void AllocatorManager::insertGroupId(void *insertIt, const GroupId &id) {
	*(*static_cast<InsertIterator*>(insertIt))++ = id;
}

template<typename InsertIterator>
void AllocatorManager::insertStats(
		void *insertIt, const AllocatorStats &stats) {
	*(*static_cast<InsertIterator*>(insertIt))++ = stats;
}

template<typename Alloc>
inline void AllocatorManager::Accessor<Alloc>::access(
		void* allocator, Command command, const AccessorParams &params) {
	assert(allocator != NULL);
	Alloc *target = static_cast<Alloc*>(allocator);

#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
	typedef typename Alloc::ManagerTool ManagerTool;
	typedef typename ManagerTool::MutexType MutexType;
	ManagerInside inside;
#endif

	switch (command) {
	case COMMAND_GET_STAT:
		assert(params.stats_ != NULL);
		target->getStats(*params.stats_);
		break;
	case COMMAND_SET_TOTAL_LIMITTER:
		assert(params.limitter_ != NULL);
		target->setLimit(params.type_, params.limitter_);
		break;
	case COMMAND_SET_EACH_LIMIT:
		target->setLimit(params.type_, params.size_);
		break;
#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
	case COMMAND_SAVE_REPORTER_SNAPSHOT: {
		LockGuard<MutexType> guard(ManagerTool::getLock(*target));
		ManagerTool::getActivationSaver(*target).saveSnapshot(params.size_);
		break;
	}
	case COMMAND_REMOVE_REPORTER_SNAPSHOT: {
		LockGuard<MutexType> guard(ManagerTool::getLock(*target));
		ManagerTool::getActivationSaver(*target).removeSnapshot(params.size_);
		break;
	}
	case COMMAND_COMPARE_REPORTER_SNAPSHOT: {
		assert(params.stats_ != NULL);
		LockGuard<MutexType> guard(ManagerTool::getLock(*target));
		ManagerTool::getActivationSaver(*target).compareSnapshot(
				params.size_, params.stats_->info_, params.out_, &inside);
		break;
	}
#endif
	default:
		assert(false);
		break;
	}
}

}	

#endif
