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
#if defined(_GLIBCXX_FULLY_DYNAMIC_STRING) && _GLIBCXX_FULLY_DYNAMIC_STRING
#define UTIL_ALLOCATOR_EMPTY_ALLOCATOR_CONSTRUCTOR_ALLOWED 0
#else
#define UTIL_ALLOCATOR_EMPTY_ALLOCATOR_CONSTRUCTOR_ALLOWED 1
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

#ifndef UTIL_ALLOCATOR_SUBSTITUTE_FIXED_ALLOCATOR
#define UTIL_ALLOCATOR_SUBSTITUTE_FIXED_ALLOCATOR 0
#endif

#ifndef UTIL_ALLOCATOR_SUBSTITUTE_VAR_ALLOCATOR
#define UTIL_ALLOCATOR_SUBSTITUTE_VAR_ALLOCATOR 0
#endif

#ifndef UTIL_ALLOCATOR_SUBSTITUTE_STACK_ALLOCATOR
#define UTIL_ALLOCATOR_SUBSTITUTE_STACK_ALLOCATOR 0
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

#if UTIL_ALLOCATOR_CHECK_CONFLICTION
#define UTIL_ALLOCATOR_DETAIL_STAT_GUARD(guard, mutex) \
	LockGuard<Mutex> &guard(*static_cast<LockGuard<Mutex>*>(NULL)); \
	static_cast<void>(mutex) \
	static_cast<void>(guard)
#else
#define UTIL_ALLOCATOR_DETAIL_STAT_GUARD(guard, mutex) \
	LockGuard<Mutex> guard(mutex)
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

#if UTIL_ALLOCATOR_CHECK_CONFLICTION

public:
	inline void lock(void) { detector_.enter(); }
	inline void unlock(void) { detector_.leave(); }
private:
	ConflictionDetector detector_;

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
	void deallocate(void *element);


	void setTotalElementLimit(size_t limit);
	void setFreeElementLimit(size_t limit);

	void setErrorHandler(AllocationErrorHandler *errorHandler);

	Mutex& getLock();


	size_t getTotalElementLimit();
	size_t getFreeElementLimit();

	size_t getElementSize();
	size_t getTotalElementCount();
	size_t getFreeElementCount();


	void getStats(AllocatorStats &stats);
	void setLimit(AllocatorStats::Type type, size_t value);
	void setLimit(AllocatorStats::Type type, AllocatorLimitter *limitter);

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
	struct FreeLink {
		FreeLink *next_;
	};

	FixedSizeAllocator(const FixedSizeAllocator&);
	FixedSizeAllocator& operator=(const FixedSizeAllocator&);

	void reserve();
	void clear(size_t preservedCount);

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

	AllocatorStats stats_;
	AllocatorLimitter *limitter_;
};

template<
		size_t SmallSize = 128,
		size_t MiddleSize = 1024 * 4,
		size_t LargeSize = 1024 * 1024>
struct VariableSizeAllocatorTraits {
	static const size_t FIXED_ALLOCATOR_COUNT = 3;
	static size_t getFixedSize(size_t index);
	static size_t selectFixedAllocator(size_t size);
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

	template<typename T> void destroy(T *object);


	void setErrorHandler(AllocationErrorHandler *errorHandler);


	BaseAllocator* base(size_t index);

	size_t getTotalElementSize();
	size_t getFreeElementSize();

	size_t getHugeElementCount();
	size_t getHugeElementSize();

	size_t getElementCapacity(const void *element);


	void getStats(AllocatorStats &stats);
	void setLimit(AllocatorStats::Type type, size_t value);
	void setLimit(AllocatorStats::Type type, AllocatorLimitter *limitter);

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
	VariableSizeAllocator(const VariableSizeAllocator&);
	VariableSizeAllocator& operator=(const VariableSizeAllocator&);

	void initialize();
	void clear();

	BaseAllocator *baseList_[Traits::FIXED_ALLOCATOR_COUNT];

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

namespace detail {
struct VariableSizeAllocatorUtils {
	template<typename Mutex, typename Traits>
	static VariableSizeAllocator<Mutex, Traits>& checkType(
			VariableSizeAllocator<Mutex, Traits> &allocator) { return allocator; }
};
}
}	

#define ALLOC_VAR_SIZE_NEW(allocator) \
		new (util::detail::VariableSizeAllocatorUtils::checkType(allocator))

#define ALLOC_VAR_SIZE_DELETE(allocator, object) \
		util::detail::VariableSizeAllocatorUtils::checkType( \
				allocator).destroy(object)

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

namespace util {

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

	typedef FixedSizeAllocator<Mutex> BaseAllocator;

#if !UTIL_ALLOCATOR_FORCE_ALLOCATOR_INFO
	explicit StackAllocator(BaseAllocator &base);
#endif

	StackAllocator(const AllocatorInfo &info, BaseAllocator *base);

	~StackAllocator();

	void* allocate(size_t size);

	void deallocate(void *ptr);

	template<typename T> void destroy(T *object);

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
	void setLimit(AllocatorStats::Type type, AllocatorLimitter *limitter);

	BaseAllocator& base();

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

	static void handleAllocationError(util::Exception &e);

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

	AllocatorStats stats_;
	AllocatorLimitter *limitter_;

#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
	AllocatorDiffReporter::ActivationSaver activationSaver_;
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

#define ALLOC_NEW(allocator) \
		new (static_cast<util::StackAllocator&>(allocator))

#define ALLOC_DELETE(allocator, object) \
		static_cast<util::StackAllocator&>(allocator).destroy(object)

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
#if UTIL_ALLOCATOR_EMPTY_ALLOCATOR_CONSTRUCTOR_ALLOWED
		assert(base_ != NULL);
#endif
		return static_cast<pointer>(base_->allocate(size * sizeof(T)));
	}

	inline void deallocate(pointer ptr, size_type) {
#if UTIL_ALLOCATOR_EMPTY_ALLOCATOR_CONSTRUCTOR_ALLOWED
		assert(base_ != NULL);
#endif
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

private:
	void *base_;
	WrapperFunc *wrapper_;
};

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

template< typename T, typename Alloc = StdAllocator<T, void> >
class AllocDefaultDelete {
public:
	AllocDefaultDelete(const Alloc &alloc) throw();
	void operator()(T *ptr);

private:
	Alloc alloc_;
};

template< typename T, typename D = AllocDefaultDelete<T> >
class AllocUniquePtr {
public:
	typedef T element_type;
	typedef D deleter_type;
	typedef element_type *pointer;

	template<typename B> AllocUniquePtr(pointer ptr, B &deleterBase);
	~AllocUniquePtr();

	pointer get() const throw();
	element_type& operator*() const;
	pointer operator->() const throw();

	pointer release() throw();
	void reset(pointer ptr = pointer()) throw();

private:
	AllocUniquePtr(const AllocUniquePtr&);
	AllocUniquePtr& operator=(const AllocUniquePtr&);

	pointer ptr_;
	deleter_type deleter_;
};

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
	bool addAllocator(GroupId id, Alloc &alloc);

	template<typename Alloc>
	static bool addAllocator(const AllocatorInfo &info, Alloc &alloc);

	template<typename Alloc>
	bool removeAllocator(GroupId id, Alloc &alloc) throw();

	template<typename Alloc>
	static bool removeAllocator(const AllocatorInfo &info, Alloc &alloc);

	bool getParentId(GroupId &id, const ManagerInside *inside = NULL);
	template<typename InsertIterator> void listSubGroup(
			GroupId id, InsertIterator it, bool recursive = false);

	const char8_t* getName(GroupId id, const ManagerInside *inside = NULL);

	void getGroupStats(
			const GroupId *idList, size_t idCount, AllocatorStats *statsList);

	template<typename InsertIterator>
	void getAllocatorStats(
			const GroupId *idList, size_t idCount, InsertIterator it);

	void setLimit(GroupId id, LimitType limitType, size_t limit);

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
	struct GroupEntry;

	typedef void (AccessorFunc)(void*, Command, const AccessorParams&);

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

	void applyAllocatorLimit(
			AllocatorEntry &entry, LimitType limitType,
			GroupEntry &groupEntry);

	GroupEntry* getParentEntry(GroupId &id);
	bool isDescendantOrSelf(GroupId id, GroupId subId);

	AllocatorEntry *findAllocatorEntry(void *alloc, GroupEntry &groupEntry);

#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
	void operateReporterSnapshots(
			const GroupId *idList, size_t idCount, const char8_t *prefix,
			void *requester, Command command, std::ostream *out);
#endif

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

struct AllocatorManager::GroupEntry {
	GroupEntry();
	~GroupEntry();

	GroupId parentId_;
	const char8_t *nameLiteral_;
	TinyList<AllocatorEntry> allocatorList_;
	AllocatorLimitter *totalLimitter_;
	size_t limitList_[LIMIT_TYPE_END];
};

namespace detail {
static AllocatorManager::Initializer g_allocatorManagerInitializer;
}

class AllocatorLimitter {
public:
	AllocatorLimitter(const AllocatorInfo &info);

	void setLimit(size_t size);

	size_t acquire(size_t minimum, size_t desired);
	void release(size_t size);

	static AllocatorLimitter* moveSize(
			AllocatorLimitter *dest, AllocatorLimitter *src, size_t size);

private:
	const AllocatorInfo info_;
	size_t limit_;
	size_t acquired_;
	util::Mutex mutex_;
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


#if !UTIL_ALLOCATOR_FORCE_ALLOCATOR_INFO
template<typename Mutex>
inline FixedSizeAllocator<Mutex>::FixedSizeAllocator(
		size_t elementSize) :
		elementSize_(elementSize),
		totalElementLimit_(std::numeric_limits<size_t>::max()),
		freeElementLimit_(std::numeric_limits<size_t>::max()),
		stableElementLimit_(std::numeric_limits<size_t>::max()),
		errorHandler_(NULL),
		freeLink_(NULL),
		totalElementCount_(0),
		freeElementCount_(0),
		stats_(AllocatorInfo()),
		limitter_(NULL) {
	assert(elementSize > 0);
	stats_.info_.setUnitSize(elementSize);
	util::AllocatorManager::addAllocator(stats_.info_, *this);
}
#endif

template<typename Mutex>
inline FixedSizeAllocator<Mutex>::FixedSizeAllocator(
		const AllocatorInfo &info, size_t elementSize) :
		elementSize_(elementSize),
		totalElementLimit_(std::numeric_limits<size_t>::max()),
		freeElementLimit_(std::numeric_limits<size_t>::max()),
		stableElementLimit_(std::numeric_limits<size_t>::max()),
		errorHandler_(NULL),
		freeLink_(NULL),
		totalElementCount_(0),
		freeElementCount_(0),
		stats_(info),
		limitter_(NULL) {
	assert(elementSize > 0);
	stats_.info_.setUnitSize(elementSize);
	util::AllocatorManager::addAllocator(stats_.info_, *this);
}

template<typename Mutex>
inline FixedSizeAllocator<Mutex>::~FixedSizeAllocator() {
#if UTIL_ALLOCATOR_REPORTER_ENABLED2
	std::map<void*, std::string>::iterator it = stackTraceMap_.begin();
	for (;it != stackTraceMap_.end(); it++) {
		std::cout << elementSize_ << "," << it->first << "," << it->second << std::endl; 
	}
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
	freeElementLimit_ = limit;
	clear(freeElementLimit_);
}

template<typename Mutex>
inline void FixedSizeAllocator<Mutex>::setErrorHandler(
		AllocationErrorHandler *errorHandler) {
	errorHandler_ = &errorHandler;
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
	case AllocatorStats::STAT_CACHE_LIMIT:
		freeElementLimit_ = value / elementSize_;
		break;
	case AllocatorStats::STAT_STABLE_LIMIT:
		stableElementLimit_ = value / elementSize_;
		break;
	default:
		break;
	}

	clear(freeElementLimit_);
}

template<typename Mutex>
void FixedSizeAllocator<Mutex>::setLimit(
		AllocatorStats::Type type, AllocatorLimitter *limitter) {
	UTIL_ALLOCATOR_DETAIL_STAT_GUARD(guard, mutex_);

	switch (type) {
	case AllocatorStats::STAT_GROUP_TOTAL_LIMIT:
		limitter_ = AllocatorLimitter::moveSize(
				limitter, limitter_, elementSize_ * totalElementCount_);
		break;
	default:
		break;
	}
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
		for (size_t i = count; i > 0; --i) {
			void *element = UTIL_MALLOC(allocSize);

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

	for (; freeLink_ != NULL; --freeElementCount_, --totalElementCount_) {
		if (freeElementCount_ <= preservedCount &&
				totalElementCount_ <= stableElementLimit_) {
			break;
		}

		FreeLink *next = freeLink_->next_;
		UTIL_FREE(freeLink_);
		freeLink_ = next;
		releasedCount++;
	}

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
	initialize();
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
	initialize();
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
			ptr = baseList_[index]->allocate();
		}
		else {
			UTIL_DETAIL_ALLOCATOR_REPORT_MISS_HIT(
					totalSize, hugeElementSize_, 0);

			LockGuard<Mutex> guard(mutex_);
			stats_.values_[AllocatorStats::STAT_CACHE_MISS_COUNT]++;

			if (limitter_ != NULL) {
				limitter_->acquire(totalSize, totalSize);
			}

			ptr = UTIL_MALLOC(totalSize);
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
		baseList_[index]->deallocate(ptr);
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

		UTIL_FREE(ptr);

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
inline void VariableSizeAllocator<Mutex, Traits>::destroy(T *object) {
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
	return baseList_[index];
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
void VariableSizeAllocator<Mutex, Traits>::setLimit(
		AllocatorStats::Type type, AllocatorLimitter *limitter) {
	UTIL_ALLOCATOR_DETAIL_STAT_GUARD(guard, mutex_);

	switch (type) {
	case AllocatorStats::STAT_GROUP_TOTAL_LIMIT:
		limitter_ = AllocatorLimitter::moveSize(
				limitter, limitter_, hugeElementSize_);
		break;
	default:
		break;
	}
}

template<typename Mutex, typename Traits>
inline void VariableSizeAllocator<Mutex, Traits>::initialize() {
	std::fill(baseList_, baseList_ + FIXED_ALLOCATOR_COUNT,
			static_cast<FixedSizeAllocator<Mutex>*>(NULL));

	try {
		for (size_t i = FIXED_ALLOCATOR_COUNT; i > 0; --i) {
			const size_t index = i - 1;
			baseList_[index] = UTIL_NEW BaseAllocator(
					stats_.info_, traits_.getFixedSize(index));
		}
		util::AllocatorManager::addAllocator(stats_.info_, *this);
	}
	catch (...) {
		clear();
		throw;
	}
}

template<typename Mutex, typename Traits>
inline void VariableSizeAllocator<Mutex, Traits>::clear() {
	for (size_t i = FIXED_ALLOCATOR_COUNT; i > 0; --i) {
		const size_t index = i - 1;

		delete baseList_[index];
		baseList_[index] = NULL;
	}

	util::AllocatorManager::removeAllocator(stats_.info_, *this);
}


inline void* StackAllocator::allocate(size_t size) {
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
	(void) ptr;
}

template<typename T> void StackAllocator::destroy(T *object) {
	if (object != NULL) {
		object->~T();
		deallocate(object);
	}
}

inline void StackAllocator::setErrorHandler(
		AllocationErrorHandler *errorHandler) {
	errorHandler_ = errorHandler;
}

inline void StackAllocator::setDefaultErrorHandler(
		AllocationErrorHandler *errorHandler) {
	defaultErrorHandler_ = errorHandler;
}

inline void StackAllocator::setTotalSizeLimit(size_t limit) {
	totalSizeLimit_ = limit;
}

inline void StackAllocator::setFreeSizeLimit(size_t limit) {
	freeSizeLimit_ = limit;
}

inline size_t StackAllocator::getTotalSizeLimit() {
	return totalSizeLimit_;
}

inline size_t StackAllocator::getFreeSizeLimit() {
	return freeSizeLimit_;
}

inline size_t StackAllocator::getTotalSize() {
	return totalSize_;
}

inline size_t StackAllocator::getFreeSize() {
	return freeSize_;
}

inline size_t StackAllocator::getHugeCount() {
	return hugeCount_;
}

inline size_t StackAllocator::getHugeSize() {
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
	allocator_.push(lastBlock_, lastRestSize_);
}

inline StackAllocator::Scope::~Scope() {
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


template<typename T, typename Alloc>
inline AllocDefaultDelete<T, Alloc>::AllocDefaultDelete(
		const Alloc &alloc) throw() : alloc_(alloc) {
}

template<typename T, typename Alloc>
inline void AllocDefaultDelete<T, Alloc>::operator()(T *ptr) {
	if (ptr != NULL) {
		alloc_.destroy(ptr);
		alloc_.deallocate(ptr, 1);
	}
}


template<typename T, typename D>
template<typename B>
inline AllocUniquePtr<T, D>::AllocUniquePtr(pointer ptr, B &deleterBase) :
		ptr_(ptr),
		deleter_(deleterBase) {
}

template<typename T, typename D>
inline AllocUniquePtr<T, D>::~AllocUniquePtr() {
	reset();
}

template<typename T, typename D>
inline typename AllocUniquePtr<T, D>::pointer
AllocUniquePtr<T, D>::get() const throw() {
	return ptr_;
}

template<typename T, typename D>
inline typename AllocUniquePtr<T, D>::element_type&
AllocUniquePtr<T, D>::operator*() const {
	assert(ptr_ != pointer());
	return *ptr_;
}

template<typename T, typename D>
inline typename AllocUniquePtr<T, D>::pointer
AllocUniquePtr<T, D>::operator->() const throw() {
	return &(**this);
}

template<typename T, typename D>
inline typename AllocUniquePtr<T, D>::pointer
AllocUniquePtr<T, D>::release() throw() {
	pointer ptr = ptr_;
	ptr_ = pointer();
	return ptr;
}

template<typename T, typename D>
inline void AllocUniquePtr<T, D>::reset(pointer ptr) throw() {
	deleter_(ptr_);
	ptr_ = ptr;
}


template<typename Alloc>
inline bool AllocatorManager::addAllocator(GroupId id, Alloc &alloc) {
	util::LockGuard<util::Mutex> guard(mutex_);

	while (id >= groupList_.end_ - groupList_.begin_) {
		groupList_.add(GroupEntry());
	}

	GroupEntry &groupEntry = groupList_.begin_[id];
	if (findAllocatorEntry(&alloc, groupEntry) != NULL) {
		assert(false);
		return false;
	}

	AllocatorEntry entry;
	entry.allocator_ = &alloc;
	entry.accessor_ = &Accessor<Alloc>::access;
	groupEntry.allocatorList_.add(entry);

	assert(findAllocatorEntry(&alloc, groupEntry) != NULL);

	return true;
}

template<typename Alloc>
inline bool AllocatorManager::addAllocator(
		const AllocatorInfo &info, Alloc &alloc) {
	return info.resolveManager().addAllocator(info.getGroupId(), alloc);
}

template<typename Alloc>
inline bool AllocatorManager::removeAllocator(
		GroupId id, Alloc &alloc) throw() {
	util::LockGuard<util::Mutex> guard(mutex_);

	if (id >= groupList_.end_ - groupList_.begin_) {
		assert(false);
		return false;
	}

	GroupEntry &groupEntry = groupList_.begin_[id];
	AllocatorEntry *entry = findAllocatorEntry(&alloc, groupEntry);
	if (entry == NULL) {
		assert(false);
		return false;
	}

	groupEntry.allocatorList_.remove(entry);
	return true;
}

template<typename Alloc>
inline bool AllocatorManager::removeAllocator(
		const AllocatorInfo &info, Alloc &alloc) {
	return info.resolveManager().removeAllocator(info.getGroupId(), alloc);
}

template<typename InsertIterator>
inline void AllocatorManager::listSubGroup(
		GroupId id, InsertIterator it, bool recursive) {
	util::LockGuard<util::Mutex> guard(mutex_);

	for (GroupEntry *groupIt = groupList_.begin_;
			groupIt != groupList_.end_; ++groupIt) {

		const GroupId subId =
				static_cast<GroupId>(groupIt - groupList_.begin_);
		if (subId == id || !isDescendantOrSelf(id, subId)) {
			continue;
		}
		else if (!recursive && groupList_.begin_[subId].parentId_ != id) {
			continue;
		}

		*it++ = subId;
	}
}

template<typename InsertIterator>
void AllocatorManager::getAllocatorStats(
		const GroupId *idList, size_t idCount, InsertIterator it) {
	util::LockGuard<util::Mutex> guard(mutex_);

	for (size_t i = 0; i < idCount; i++) {
		const GroupEntry &entry = groupList_.begin_[idList[i]];

		for (AllocatorEntry *entryIt = entry.allocatorList_.begin_;
				entryIt != entry.allocatorList_.end_; ++entryIt) {
			AllocatorStats stats;
			AccessorParams params;
			params.stats_ = &stats;
			(*entryIt->accessor_)(
					entryIt->allocator_, COMMAND_GET_STAT, params);
			*it++ = stats;
		}
	}
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
		target->setLimit(params.type_, params.size_);
		break;
	case COMMAND_SET_EACH_LIMIT:
		assert(params.limitter_ != NULL);
		target->setLimit(params.type_, params.limitter_);
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
