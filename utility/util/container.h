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
    @brief Definition of Utility for Containers
*/
#ifndef UTIL_CONTAINER_H_
#define UTIL_CONTAINER_H_

#include "util/allocator.h"
#include "util/code.h"

#include <vector>
#include <map>
#include <set>
#include <deque>
#include <limits>
#include <memory>
#include <algorithm>
#include <climits>

#if UTIL_CXX11_SUPPORTED
#include <unordered_map>
#include <unordered_set>
#endif

#define UTIL_DISABLE_CONTAINER_DEFAULT_CONSTRUCTOR 1

#ifndef UTIL_OBJECT_POOL_NO_CACHE
#define UTIL_OBJECT_POOL_NO_CACHE UTIL_ALLOCATOR_NO_CACHE
#endif

#if UTIL_DISABLE_CONTAINER_DEFAULT_CONSTRUCTOR
#define UTIL_CONTAINER_ALLOC_ARG_OPT(type)
#else
#define UTIL_CONTAINER_ALLOC_ARG_OPT(type) = type()
#endif

namespace util {

namespace detail {
struct ObjectPoolFreeLink {
	ObjectPoolFreeLink *next_;
};

struct ObjectPoolUtils;
template<typename Mutex, typename D> class ObjectPoolDirectAllocator;
template<typename T, typename Mutex> class ObjectPoolAllocator;
}

/*!
	@brief Manages same type object in the pool
*/
template<typename T, typename Mutex = NoopMutex>
class ObjectPool {
	friend struct detail::ObjectPoolUtils;

public:
	struct StdAllocatorResolver;

	typedef FixedSizeAllocator<Mutex> BaseAllocator;

#if UTIL_ALLOCATOR_FORCE_ALLOCATOR_INFO
	ObjectPool(const AllocatorInfo &info);
#else
	ObjectPool(const AllocatorInfo &info = AllocatorInfo());
#endif

	~ObjectPool();


	T* allocate();
	void deallocate(T *element);

	T* poll();


	void setTotalElementLimit(size_t limit);
	void setFreeElementLimit(size_t limit);


	size_t getTotalElementCount();
	size_t getFreeElementCount();


	void getStats(AllocatorStats &stats);
	void setLimit(AllocatorStats::Type type, size_t value);
	void setLimit(AllocatorStats::Type type, AllocatorLimitter *limitter);

	BaseAllocator& base();
	StdAllocator<T, void> getStdAllocator();

#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
	class ManagerTool {
		friend class AllocatorManager;
		typedef Mutex MutexType;
		static AllocatorDiffReporter::ActivationSaver& getActivationSaver(
				ObjectPool &pool) {
			return pool.activationSaver_;
		}
		static MutexType& getLock(ObjectPool &pool) {
			return pool.getLock();
		}
	};
#endif

private:
	friend struct ObjectPoolUtils;

	typedef detail::ObjectPoolFreeLink FreeLink;
	typedef detail::ObjectPoolUtils Utils;

	ObjectPool(const ObjectPool&);
	ObjectPool& operator=(const ObjectPool&);

	void* allocateDirect();
	void deallocateDirect(void *rawElement);

	void clear(size_t preservedCount);

	size_t mergeFreeElementCount(
			size_t baseCount, const util::LockGuard<Mutex>&);

	Mutex& getLock();

	size_t freeElementLimit_;

	FreeLink *freeLink_;

	size_t freeElementCount_;

#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
	AllocatorDiffReporter::ActivationSaver activationSaver_;
#endif

	BaseAllocator base_;
	AllocatorStats stats_;
};

template<typename T, typename Mutex>
struct ObjectPool<T, Mutex>::StdAllocatorResolver {
	typedef T ValueType;

	template<typename U> struct Rebind {
		typedef typename ObjectPool<U, Mutex>::StdAllocatorResolver Other;
	};

	StdAllocator<T, void> operator()(ObjectPool<T, Mutex> *pool) const {
		return pool->getStdAllocator();
	}
};

namespace detail {
struct ObjectPoolUtils {
	static const size_t ELEMENT_OFFSET =
			AlignedSizeOf<ObjectPoolFreeLink>::VALUE;

	template<typename T> struct TypedElementDestructor;
	struct ElementDestructor;

	template<typename T> struct ElementDestructorOf {
#if UTIL_HAS_TEMPLATE_PLACEMENT_NEW
		typedef TypedElementDestructor<T> Type;
#else
		typedef ElementDestructor Type;
#endif
	};

	template<typename T, typename Mutex>
	static ObjectPool<T, Mutex>& checkType(ObjectPool<T, Mutex> &pool) {
		return pool;
	}

	template<typename T, typename Mutex>
	static ObjectPoolDirectAllocator<
			Mutex, typename ElementDestructorOf<T>::Type> toDirectAllocator(
			ObjectPool<T, Mutex> &pool) {
		typedef typename ElementDestructorOf<T>::Type Destructor;
		return ObjectPoolDirectAllocator<Mutex, Destructor>(
				pool.freeLink_, pool.freeElementCount_, pool.base_,
				Destructor::template create<T>());
	}

	template<typename T>
	static size_t getBaseElementSize() {
		return ELEMENT_OFFSET + sizeof(T);
	}

	static size_t getElementSize(size_t baseSize) {
		return baseSize - ELEMENT_OFFSET;
	}

	static void* elementOf(ObjectPoolFreeLink *entry) {
		return reinterpret_cast<uint8_t*>(entry) + ELEMENT_OFFSET;
	}

	static ObjectPoolFreeLink* linkOf(void *entry) {
		return reinterpret_cast<ObjectPoolFreeLink*>(
				static_cast<uint8_t*>(entry) - ELEMENT_OFFSET);
	}

};

template<typename T>
struct ObjectPoolUtils::TypedElementDestructor {
public:
	template<typename> static TypedElementDestructor create() {
		return TypedElementDestructor();
	}

	void operator()(void *rawElement) const {
		static_cast<T*>(rawElement)->~T();
	}
};

struct ObjectPoolUtils::ElementDestructor {
public:
	template<typename T> static ElementDestructor create() {
		return ElementDestructor(&destruct<T>);
	}

	void operator()(void *rawElement) const {
		func_(rawElement);
	}

private:
	typedef void (*Func)(void*);

	explicit ElementDestructor(Func func) : func_(func) {
	}

	template<typename T> static void destruct(void *rawElement) {
		static_cast<T*>(rawElement)->~T();
	}

	Func func_;
};
}	

namespace detail {
template<typename Mutex, typename D>
class ObjectPoolDirectAllocator {
public:
	typedef ObjectPoolFreeLink FreeLink;
	typedef FixedSizeAllocator<Mutex> BaseAllocator;

	ObjectPoolDirectAllocator(
			FreeLink *&freeLink, size_t &freeElementCount, BaseAllocator &base,
			const D &elementDestructor);

	void* allocateDirect(size_t elementSize) const;
	void deallocateDirect(void *rawElement) const throw();

private:
	typedef detail::ObjectPoolUtils Utils;

	Mutex& getLock() const {
		return base_.getLock();
	}

	FreeLink *&freeLink_;
	size_t &freeElementCount_;
	BaseAllocator &base_;
	D elementDestructor_;
};
}	

namespace detail {
template<typename T, typename Mutex>
class ObjectPoolAllocator {
public:
	typedef ObjectPool<T, Mutex> BaseType;

	void* allocate(size_t size) {
		BaseType &pool = base();
		if (size != sizeof(T) ||
				pool.base().getElementSize() !=
						ObjectPoolUtils::getBaseElementSize<T>()) {
			UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_ARGUMENT, "");
		}

		uint8_t *baseAddr = static_cast<uint8_t*>(pool.base().allocate());
		return baseAddr + ObjectPoolUtils::getBaseElementSize<T>() -
				sizeof(T);
	}

	void deallocate(void *ptr) {
		if (ptr == NULL) {
			return;
		}

		BaseType &pool = base();
		uint8_t *baseAddr = static_cast<uint8_t*>(ptr) + sizeof(T) -
				ObjectPoolUtils::getBaseElementSize<T>();

		pool.base().deallocate(baseAddr);
	}

	static ObjectPoolAllocator& get(BaseType &pool) {
		void *addr = &pool;
		return *static_cast<ObjectPoolAllocator*>(addr);
	}

private:
	ObjectPoolAllocator();

	ObjectPoolAllocator(const ObjectPoolAllocator&);
	ObjectPoolAllocator& operator=(const ObjectPoolAllocator&);

	BaseType& base() {
		void *addr = this;
		return *static_cast<BaseType*>(addr);
	}
};
}	
}	

#define UTIL_OBJECT_POOL_NEW(pool) \
		new (util::detail::ObjectPoolUtils::toDirectAllocator(pool))

#define UTIL_OBJECT_POOL_DELETE(pool, element) \
		util::detail::ObjectPoolUtils::checkType(pool).deallocate(element)

#if UTIL_HAS_TEMPLATE_PLACEMENT_NEW
template<typename Mutex, typename D>
void* operator new(
		size_t size, const util::detail::ObjectPoolDirectAllocator<
				Mutex, D> &alloc) {
	return alloc.allocateDirect(size);
}
template<typename Mutex, typename D>
void operator delete(
		void *p, const util::detail::ObjectPoolDirectAllocator<
				Mutex, D> &alloc) throw() {
	alloc.deallocateDirect(p);
}
#else
inline void operator delete(
		void *p, const util::detail::ObjectPoolDirectAllocator<
				util::NoopMutex,
				util::detail::ObjectPoolUtils::ElementDestructor> &alloc) throw() {
	alloc.deallocateDirect(p);
}

inline void* operator new(
		size_t size, const util::detail::ObjectPoolDirectAllocator<
				util::NoopMutex,
				util::detail::ObjectPoolUtils::ElementDestructor> &alloc) {
	return alloc.allocateDirect(size);
}

inline void* operator new(
		size_t size, const util::detail::ObjectPoolDirectAllocator<
				util::Mutex,
				util::detail::ObjectPoolUtils::ElementDestructor> &alloc) {
	return alloc.allocateDirect(size);
}

inline void operator delete(
		void *p, const util::detail::ObjectPoolDirectAllocator<
				util::Mutex,
				util::detail::ObjectPoolUtils::ElementDestructor> &alloc) throw() {
	alloc.deallocateDirect(p);
}
#endif 

namespace util {

/*!
	@brief std::vector, using StackAllocator in default
*/
template< typename T, typename Alloc = StdAllocator<T, StackAllocator> >
class Vector : public std::vector<T, Alloc> {
private:
	typedef Vector<T, Alloc> ThisType;
	typedef std::vector<T, Alloc> BaseType;

public:
	explicit Vector(const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(alloc) {
	}

	explicit Vector(
			size_t size,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(size, T(), alloc) {
	}

	explicit Vector(
			size_t size, const T &value,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(size, value, alloc) {
	}

	template<typename Iter>
	Vector(
			Iter first, Iter last,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(first, last, alloc) {
	}

	Vector(const BaseType &another) : BaseType(another) {
	}

	ThisType& operator=(const BaseType &another) {
		BaseType::operator=(another);
		return *this;
	}
};

template<typename T, typename Alloc>
inline bool operator==(
		const Vector<T, Alloc> &lhs, const Vector<T, Alloc> &rhs) {
	typedef typename std::vector<T, Alloc> Base;
	return static_cast<const Base&>(lhs) == static_cast<const Base&>(rhs);
}

template<typename T, typename Alloc>
inline bool operator!=(
		const Vector<T, Alloc> &lhs, const Vector<T, Alloc> &rhs) {
	typedef typename std::vector<T, Alloc> Base;
	return static_cast<const Base&>(lhs) != static_cast<const Base&>(rhs);
}

template<typename T, typename Alloc>
inline bool operator<(
		const Vector<T, Alloc> &lhs, const Vector<T, Alloc> &rhs) {
	typedef typename std::vector<T, Alloc> Base;
	return static_cast<const Base&>(lhs) < static_cast<const Base&>(rhs);
}

template<typename T, typename Alloc>
inline bool operator<=(
		const Vector<T, Alloc> &lhs, const Vector<T, Alloc> &rhs) {
	typedef typename std::vector<T, Alloc> Base;
	return static_cast<const Base&>(lhs) <= static_cast<const Base&>(rhs);
}

template<typename T, typename Alloc>
inline bool operator>(
		const Vector<T, Alloc> &lhs, const Vector<T, Alloc> &rhs) {
	typedef typename std::vector<T, Alloc> Base;
	return static_cast<const Base&>(lhs) > static_cast<const Base&>(rhs);
}

template<typename T, typename Alloc>
inline bool operator>=(
		const Vector<T, Alloc> &lhs, const Vector<T, Alloc> &rhs) {
	typedef typename std::vector<T, Alloc> Base;
	return static_cast<const Base&>(lhs) >= static_cast<const Base&>(rhs);
}

template<typename T>
class AllocVector : public Vector< T, StdAllocator<T, void> > {
private:
	typedef StdAllocator<T, void> Alloc;
	typedef Vector<T, Alloc> BaseType;

public:
	explicit AllocVector(
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(alloc) {
	}

	template<typename Iter>
	AllocVector(
			Iter first, Iter last,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(first, last, alloc) {
	}
};

/*!
	@brief std::deque, using StackAllocator in default
*/
template< typename T, typename Alloc = StdAllocator<T, StackAllocator> >
class Deque : public std::deque<T, Alloc> {
private:
	typedef Deque<T, Alloc> ThisType;
	typedef std::deque<T, Alloc> BaseType;

public:
	explicit Deque(const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(alloc) {
	}

	explicit Deque(
			size_t size,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(size, T(), alloc) {
	}

	explicit Deque(
			size_t size, const T &value,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(size, value, alloc) {
	}

	template<typename Iter>
	Deque(
			Iter first, Iter last,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(first, last, alloc) {
	}

	Deque(const BaseType &another) : BaseType(another) {
	}

	ThisType& operator=(const BaseType &another) {
		BaseType::operator=(another);
		return *this;
	}
};

template<typename T, typename Alloc>
inline bool operator==(
		const Deque<T, Alloc> &lhs, const Deque<T, Alloc> &rhs) {
	typedef typename std::deque<T, Alloc> Base;
	return static_cast<const Base&>(lhs) == static_cast<const Base&>(rhs);
}

template<typename T, typename Alloc>
inline bool operator!=(
		const Deque<T, Alloc> &lhs, const Deque<T, Alloc> &rhs) {
	typedef typename std::deque<T, Alloc> Base;
	return static_cast<const Base&>(lhs) != static_cast<const Base&>(rhs);
}

template<typename T, typename Alloc>
inline bool operator<(
		const Deque<T, Alloc> &lhs, const Deque<T, Alloc> &rhs) {
	typedef typename std::deque<T, Alloc> Base;
	return static_cast<const Base&>(lhs) < static_cast<const Base&>(rhs);
}

template<typename T, typename Alloc>
inline bool operator<=(
		const Deque<T, Alloc> &lhs, const Deque<T, Alloc> &rhs) {
	typedef typename std::deque<T, Alloc> Base;
	return static_cast<const Base&>(lhs) <= static_cast<const Base&>(rhs);
}

template<typename T, typename Alloc>
inline bool operator>(
		const Deque<T, Alloc> &lhs, const Deque<T, Alloc> &rhs) {
	typedef typename std::deque<T, Alloc> Base;
	return static_cast<const Base&>(lhs) > static_cast<const Base&>(rhs);
}

template<typename T, typename Alloc>
inline bool operator>=(
		const Deque<T, Alloc> &lhs, const Deque<T, Alloc> &rhs) {
	typedef typename std::deque<T, Alloc> Base;
	return static_cast<const Base&>(lhs) >= static_cast<const Base&>(rhs);
}

template<typename T>
class AllocDeque : public Deque< T, StdAllocator<T, void> > {
private:
	typedef StdAllocator<T, void> Alloc;
	typedef Deque<T, Alloc> BaseType;

public:
	explicit AllocDeque(
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(alloc) {
	}

	template<typename Iter>
	AllocDeque(
			Iter first, Iter last,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(first, last, alloc) {
	}
};

/*!
	@brief std::map, using StackAllocator in default
*/
template<
		typename K, typename V,
		typename Comp = typename std::map<K, V>::key_compare,
		typename Alloc = StdAllocator<std::pair<const K, V>, StackAllocator> >
class Map : public std::map<K, V, Comp, Alloc> {
private:
	typedef Map<K, V, Comp, Alloc> ThisType;
	typedef std::map<K, V, Comp, Alloc> BaseType;

public:
	explicit Map(
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(Comp(), alloc) {
	}

	Map(
			const Comp &comp,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(comp, alloc) {
	}

	template<typename Iter>
	Map(
			Iter first, Iter last,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(first, last, Comp(), alloc) {
	}

	template<typename Iter>
	Map(
			Iter first, Iter last, const Comp &comp,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(first, last, comp, alloc) {
	}

	Map(const BaseType &another) : BaseType(another) {
	}

	ThisType& operator=(const BaseType &another) {
		BaseType::operator=(another);
		return *this;
	}
};

template<typename K, typename V, typename Comp, typename Alloc>
inline bool operator==(
		const Map<K, V, Comp, Alloc> &lhs, const Map<K, V, Comp, Alloc> &rhs) {
	typedef typename std::map<K, V, Comp, Alloc> Base;
	return static_cast<const Base&>(lhs) == static_cast<const Base&>(rhs);
}

template<typename K, typename V, typename Comp, typename Alloc>
inline bool operator!=(
		const Map<K, V, Comp, Alloc> &lhs, const Map<K, V, Comp, Alloc> &rhs) {
	typedef typename std::map<K, V, Comp, Alloc> Base;
	return static_cast<const Base&>(lhs) != static_cast<const Base&>(rhs);
}

template<typename K, typename V, typename Comp, typename Alloc>
inline bool operator<(
		const Map<K, V, Comp, Alloc> &lhs, const Map<K, V, Comp, Alloc> &rhs) {
	typedef typename std::map<K, V, Comp, Alloc> Base;
	return static_cast<const Base&>(lhs) < static_cast<const Base&>(rhs);
}

template<typename K, typename V, typename Comp, typename Alloc>
inline bool operator<=(
		const Map<K, V, Comp, Alloc> &lhs, const Map<K, V, Comp, Alloc> &rhs) {
	typedef typename std::map<K, V, Comp, Alloc> Base;
	return static_cast<const Base&>(lhs) <= static_cast<const Base&>(rhs);
}

template<typename K, typename V, typename Comp, typename Alloc>
inline bool operator>(
		const Map<K, V, Comp, Alloc> &lhs, const Map<K, V, Comp, Alloc> &rhs) {
	typedef typename std::map<K, V, Comp, Alloc> Base;
	return static_cast<const Base&>(lhs) > static_cast<const Base&>(rhs);
}

template<typename K, typename V, typename Comp, typename Alloc>
inline bool operator>=(
		const Map<K, V, Comp, Alloc> &lhs, const Map<K, V, Comp, Alloc> &rhs) {
	typedef typename std::map<K, V, Comp, Alloc> Base;
	return static_cast<const Base&>(lhs) >= static_cast<const Base&>(rhs);
}

template<
		typename K, typename V,
		typename Comp = typename Map<K, V>::key_compare>
class AllocMap :
		public Map<K, V, Comp, StdAllocator<std::pair<const K, V>, void> > {
private:
	typedef StdAllocator<std::pair<const K, V>, void> Alloc;
	typedef Map<K, V, Comp, Alloc> BaseType;

public:
	explicit AllocMap(
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(Comp(), alloc) {
	}

	AllocMap(
			const Comp &comp,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(comp, alloc) {
	}
};

/*!
	@brief std::multimup, using StackAllocator in default
*/
template<
		typename K, typename V,
		typename Comp = typename std::multimap<K, V>::key_compare,
		typename Alloc = StdAllocator<std::pair<const K, V>, StackAllocator> >
class MultiMap : public std::multimap<K, V, Comp, Alloc> {
private:
	typedef MultiMap<K, V, Comp, Alloc> ThisType;
	typedef std::multimap<K, V, Comp, Alloc> BaseType;

public:
	explicit MultiMap(
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(Comp(), alloc) {
	}

	MultiMap(
			const Comp &comp,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(comp, alloc) {
	}

	template<typename Iter>
	MultiMap(
			Iter first, Iter last,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(first, last, Comp(), alloc) {
	}

	template<typename Iter>
	MultiMap(
			Iter first, Iter last, const Comp &comp,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(first, last, comp, alloc) {
	}

	MultiMap(const BaseType &another) : BaseType(another) {
	}

	ThisType& operator=(const BaseType &another) {
		BaseType::operator=(another);
		return *this;
	}
};

template<typename K, typename V, typename Comp, typename Alloc>
inline bool operator==(
		const MultiMap<K, V, Comp, Alloc> &lhs,
		const MultiMap<K, V, Comp, Alloc> &rhs) {
	typedef typename std::multimap<K, V, Comp, Alloc> Base;
	return static_cast<const Base&>(lhs) == static_cast<const Base&>(rhs);
}

template<typename K, typename V, typename Comp, typename Alloc>
inline bool operator!=(
		const MultiMap<K, V, Comp, Alloc> &lhs,
		const MultiMap<K, V, Comp, Alloc> &rhs) {
	typedef typename std::multimap<K, V, Comp, Alloc> Base;
	return static_cast<const Base&>(lhs) != static_cast<const Base&>(rhs);
}

template<typename K, typename V, typename Comp, typename Alloc>
inline bool operator<(
		const MultiMap<K, V, Comp, Alloc> &lhs,
		const MultiMap<K, V, Comp, Alloc> &rhs) {
	typedef typename std::multimap<K, V, Comp, Alloc> Base;
	return static_cast<const Base&>(lhs) < static_cast<const Base&>(rhs);
}

template<typename K, typename V, typename Comp, typename Alloc>
inline bool operator<=(
		const MultiMap<K, V, Comp, Alloc> &lhs,
		const MultiMap<K, V, Comp, Alloc> &rhs) {
	typedef typename std::multimap<K, V, Comp, Alloc> Base;
	return static_cast<const Base&>(lhs) <= static_cast<const Base&>(rhs);
}

template<typename K, typename V, typename Comp, typename Alloc>
inline bool operator>(
		const MultiMap<K, V, Comp, Alloc> &lhs,
		const MultiMap<K, V, Comp, Alloc> &rhs) {
	typedef typename std::multimap<K, V, Comp, Alloc> Base;
	return static_cast<const Base&>(lhs) > static_cast<const Base&>(rhs);
}

template<typename K, typename V, typename Comp, typename Alloc>
inline bool operator>=(
		const MultiMap<K, V, Comp, Alloc> &lhs,
		const MultiMap<K, V, Comp, Alloc> &rhs) {
	typedef typename std::multimap<K, V, Comp, Alloc> Base;
	return static_cast<const Base&>(lhs) >= static_cast<const Base&>(rhs);
}

template<
		typename K, typename V,
		typename Comp = typename MultiMap<K, V>::key_compare>
class AllocMultiMap : public MultiMap<
		K, V, Comp, StdAllocator<std::pair<const K, V>, void> > {
private:
	typedef StdAllocator<std::pair<const K, V>, void> Alloc;
	typedef MultiMap<K, V, Comp, Alloc> BaseType;

public:
	explicit AllocMultiMap(
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(Comp(), alloc) {
	}

	AllocMultiMap(
			const Comp &comp,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(comp, alloc) {
	}
};

/*!
	@brief std::set, using StackAllocator in default
*/
template<
		typename T, typename Comp = typename std::set<T>::key_compare,
		typename Alloc = StdAllocator<T, StackAllocator> >
class Set : public std::set<T, Comp, Alloc> {
private:
	typedef Set<T, Comp, Alloc> ThisType;
	typedef std::set<T, Comp, Alloc> BaseType;

public:
	explicit Set(
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(Comp(), alloc) {
	}

	Set(
			const Comp &comp,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(comp, alloc) {
	}

	template<typename Iter>
	Set(
			Iter first, Iter last,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(first, last, Comp(), alloc) {
	}

	template<typename Iter>
	Set(
			Iter first, Iter last, const Comp &comp,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(first, last, comp, alloc) {
	}

	Set(const BaseType &another) : BaseType(another) {
	}

	ThisType& operator=(const BaseType &another) {
		BaseType::operator=(another);
		return *this;
	}
};

template<typename T, typename Comp, typename Alloc>
inline bool operator==(
		const Set<T, Comp, Alloc> &lhs, const Set<T, Comp, Alloc> &rhs) {
	typedef typename std::set<T, Comp, Alloc> Base;
	return static_cast<const Base&>(lhs) == static_cast<const Base&>(rhs);
}

template<typename T, typename Comp, typename Alloc>
inline bool operator!=(
		const Set<T, Comp, Alloc> &lhs, const Set<T, Comp, Alloc> &rhs) {
	typedef typename std::set<T, Comp, Alloc> Base;
	return static_cast<const Base&>(lhs) != static_cast<const Base&>(rhs);
}

template<typename T, typename Comp, typename Alloc>
inline bool operator<(
		const Set<T, Comp, Alloc> &lhs, const Set<T, Comp, Alloc> &rhs) {
	typedef typename std::set<T, Comp, Alloc> Base;
	return static_cast<const Base&>(lhs) < static_cast<const Base&>(rhs);
}

template<typename T, typename Comp, typename Alloc>
inline bool operator<=(
		const Set<T, Comp, Alloc> &lhs, const Set<T, Comp, Alloc> &rhs) {
	typedef typename std::set<T, Comp, Alloc> Base;
	return static_cast<const Base&>(lhs) <= static_cast<const Base&>(rhs);
}

template<typename T, typename Comp, typename Alloc>
inline bool operator>(
		const Set<T, Comp, Alloc> &lhs, const Set<T, Comp, Alloc> &rhs) {
	typedef typename std::set<T, Comp, Alloc> Base;
	return static_cast<const Base&>(lhs) > static_cast<const Base&>(rhs);
}

template<typename T, typename Comp, typename Alloc>
inline bool operator>=(
		const Set<T, Comp, Alloc> &lhs, const Set<T, Comp, Alloc> &rhs) {
	typedef typename std::set<T, Comp, Alloc> Base;
	return static_cast<const Base&>(lhs) >= static_cast<const Base&>(rhs);
}

template<typename T, typename Comp = typename Set<T>::key_compare>
class AllocSet : public Set< T, Comp, StdAllocator<T, void> > {
private:
	typedef StdAllocator<T, void> Alloc;
	typedef Set<T, Comp, Alloc> BaseType;

public:
	explicit AllocSet(
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(Comp(), alloc) {
	}

	AllocSet(
			const Comp &comp,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(comp, alloc) {
	}
};

/*!
	@brief std::multiset, using StackAllocator in default
*/
template<
		typename T, typename Comp = typename std::multiset<T>::key_compare,
		typename Alloc = StdAllocator<T, StackAllocator> >
class MultiSet : public std::multiset<T, Comp, Alloc> {
private:
	typedef MultiSet<T, Comp, Alloc> ThisType;
	typedef std::multiset<T, Comp, Alloc> BaseType;

public:
	explicit MultiSet(
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(Comp(), alloc) {
	}

	MultiSet(
			const Comp &comp,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(comp, alloc) {
	}

	template<typename Iter>
	MultiSet(
			Iter first, Iter last,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(first, last, Comp(), alloc) {
	}

	template<typename Iter>
	MultiSet(
			Iter first, Iter last, const Comp &comp,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(first, last, comp, alloc) {
	}

	MultiSet(const BaseType &another) : BaseType(another) {
	}

	ThisType& operator=(const BaseType &another) {
		BaseType::operator=(another);
		return *this;
	}
};

template<typename T, typename Comp, typename Alloc>
inline bool operator==(
		const MultiSet<T, Comp, Alloc> &lhs,
		const MultiSet<T, Comp, Alloc> &rhs) {
	typedef typename std::multiset<T, Comp, Alloc> Base;
	return static_cast<const Base&>(lhs) == static_cast<const Base&>(rhs);
}

template<typename T, typename Comp, typename Alloc>
inline bool operator!=(
		const MultiSet<T, Comp, Alloc> &lhs,
		const MultiSet<T, Comp, Alloc> &rhs) {
	typedef typename std::multiset<T, Comp, Alloc> Base;
	return static_cast<const Base&>(lhs) != static_cast<const Base&>(rhs);
}

template<typename T, typename Comp, typename Alloc>
inline bool operator<(
		const MultiSet<T, Comp, Alloc> &lhs,
		const MultiSet<T, Comp, Alloc> &rhs) {
	typedef typename std::multiset<T, Comp, Alloc> Base;
	return static_cast<const Base&>(lhs) < static_cast<const Base&>(rhs);
}

template<typename T, typename Comp, typename Alloc>
inline bool operator<=(
		const MultiSet<T, Comp, Alloc> &lhs,
		const MultiSet<T, Comp, Alloc> &rhs) {
	typedef typename std::multiset<T, Comp, Alloc> Base;
	return static_cast<const Base&>(lhs) <= static_cast<const Base&>(rhs);
}

template<typename T, typename Comp, typename Alloc>
inline bool operator>(
		const MultiSet<T, Comp, Alloc> &lhs,
		const MultiSet<T, Comp, Alloc> &rhs) {
	typedef typename std::multiset<T, Comp, Alloc> Base;
	return static_cast<const Base&>(lhs) > static_cast<const Base&>(rhs);
}

template<typename T, typename Comp, typename Alloc>
inline bool operator>=(
		const MultiSet<T, Comp, Alloc> &lhs,
		const MultiSet<T, Comp, Alloc> &rhs) {
	typedef typename std::multiset<T, Comp, Alloc> Base;
	return static_cast<const Base&>(lhs) >= static_cast<const Base&>(rhs);
}

template<typename T, typename Comp = typename MultiSet<T>::key_compare>
class AllocMultiSet : public MultiSet< T, Comp, StdAllocator<T, void> > {
private:
	typedef StdAllocator<T, void> Alloc;
	typedef MultiSet<T, Comp, Alloc> BaseType;

public:
	explicit AllocMultiSet(
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(Comp(), alloc) {
	}

	AllocMultiSet(
			const Comp &comp,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(comp, alloc) {
	}
};

namespace detail {
template<typename C>
class UnorderedContainer : public C {
public:
	template<typename Hash, typename Pred, typename Comp, typename Alloc>
	UnorderedContainer(
			size_t n, const Hash &hash, const Pred &pred, const Comp &comp,
			const Alloc &alloc) :
#if UTIL_CXX11_SUPPORTED
			C(n, hash, pred, alloc) {
		static_cast<void>(comp);
	}
#else
			C(comp, alloc){
		static_cast<void>(n);
		static_cast<void>(hash);
		static_cast<void>(pred);
	}
#endif 
};

template<
		typename K, typename V,
		typename Hash, typename Pred, typename Comp, typename Alloc>
struct UnorderedMapTaits {
#if UTIL_CXX11_SUPPORTED
	typedef std::unordered_map<K, V, Hash, Pred, Alloc> BaseType;
#else
	typedef std::map<K, V, Comp, Alloc> BaseType;
#endif
	typedef UnorderedContainer<BaseType> WrappedType;
};

template<
		typename T,
		typename Hash, typename Pred, typename Comp, typename Alloc>
struct UnorderedSetTaits {
#if UTIL_CXX11_SUPPORTED
	typedef std::unordered_set<T, Hash, Pred, Alloc> BaseType;
#else
	typedef std::set<T, Comp, Alloc> BaseType;
#endif
	typedef UnorderedContainer<BaseType> WrappedType;
};

template<typename K> struct UnorderedKeyTaits {
#if UTIL_CXX11_SUPPORTED
	typedef typename std::unordered_set<K>::hasher Hash;
	typedef typename std::unordered_set<K>::key_equal Pred;
#else
	typedef FalseType Hash;
	typedef FalseType Pred;
#endif
	typedef typename std::set<K>::key_compare Comp;
};
} 

template<
		typename K, typename V,
		typename Hash = typename detail::UnorderedKeyTaits<K>::Hash,
		typename Pred = typename detail::UnorderedKeyTaits<K>::Pred,
		typename Comp = typename detail::UnorderedKeyTaits<K>::Comp,
		typename Alloc = StdAllocator<std::pair<const K, V>, StackAllocator> >
class UnorderedMap : public detail::UnorderedMapTaits<
		K, V, Hash, Pred, Comp, Alloc>::WrappedType {
private:
	typedef typename detail::UnorderedMapTaits<
			K, V, Hash, Pred, Comp, Alloc>::WrappedType BaseType;

public:
	explicit UnorderedMap(
			size_t n,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(n, Hash(), Pred(), Comp(), alloc) {
	}

	UnorderedMap(
			size_t n, const Hash &hash, const Pred &pred, const Comp &comp,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(n, hash, pred, comp, alloc) {
	}
};

template<
		typename K, typename V,
		typename Hash = typename detail::UnorderedKeyTaits<K>::Hash,
		typename Pred = typename detail::UnorderedKeyTaits<K>::Pred,
		typename Comp = typename detail::UnorderedKeyTaits<K>::Comp>
class AllocUnorderedMap : public UnorderedMap<
		K, V, Hash, Pred, Comp, StdAllocator<std::pair<const K, V>, void> > {
private:
	typedef StdAllocator<std::pair<const K, V>, void> Alloc;
	typedef UnorderedMap<K, V, Hash, Pred, Comp, Alloc> BaseType;

public:
	explicit AllocUnorderedMap(
			size_t n,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(n, alloc) {
	}

	AllocUnorderedMap(
			size_t n, const Hash &hash, const Pred &pred, const Comp &comp,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(n, hash, pred, comp, alloc) {
	}
};

template<
		typename T,
		typename Hash = typename detail::UnorderedKeyTaits<T>::Hash,
		typename Pred = typename detail::UnorderedKeyTaits<T>::Pred,
		typename Comp = typename detail::UnorderedKeyTaits<T>::Comp,
		typename Alloc = StdAllocator<T, StackAllocator> >
class UnorderedSet : public detail::UnorderedSetTaits<
		T, Hash, Pred, Comp, Alloc>::WrappedType {
private:
	typedef typename detail::UnorderedSetTaits<
			T, Hash, Pred, Comp, Alloc>::WrappedType BaseType;

public:
	explicit UnorderedSet(
			size_t n,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(n, Hash(), Pred(), Comp(), alloc) {
	}

	UnorderedSet(
			size_t n, const Hash &hash, const Pred &pred, const Comp &comp,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(n, hash, pred, comp, alloc) {
	}
};

template<
		typename T,
		typename Hash = typename detail::UnorderedKeyTaits<T>::Hash,
		typename Pred = typename detail::UnorderedKeyTaits<T>::Pred,
		typename Comp = typename detail::UnorderedKeyTaits<T>::Comp>
class AllocUnorderedSet : public UnorderedSet<
		T, Hash, Pred, Comp, StdAllocator<T, void> > {
private:
	typedef StdAllocator<T, void> Alloc;
	typedef UnorderedSet<T, Hash, Pred, Comp, Alloc> BaseType;

public:
	explicit AllocUnorderedSet(
			size_t n,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(n, alloc) {
	}

	AllocUnorderedSet(
			size_t n, const Hash &hash, const Pred &pred, const Comp &comp,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(n, hash, pred, comp, alloc) {
	}
};

template<typename T, typename Alloc> class XArray;

/*!
	@brief Iterates for XArray
*/
template<typename V, typename T>
class XArrayIterator {
private:
	typedef XArrayIterator<V, T> ThisType;
	template<typename TF, typename AllocF> friend class XArray;
	template<typename VF, typename TF> friend class XArrayIterator;

	template<typename VA, typename TA>
	struct Traits {
		typedef const TA *Pointer;
		typedef const TA &Reference;
	};

	template<typename TA>
	struct Traits<TA, TA> {
		typedef TA *Pointer;
		typedef TA &Reference;
	};

public:
	typedef std::random_access_iterator_tag iterator_category;
	typedef V value_type;
	typedef ptrdiff_t difference_type;
	typedef ptrdiff_t distance_type;
	typedef typename Traits<T, V>::Pointer pointer;
	typedef typename Traits<T, V>::Reference reference;

	XArrayIterator();
	template<typename VA>
	XArrayIterator(const XArrayIterator<VA, T> &another);
	template<typename VA>
	ThisType& operator=(const XArrayIterator<VA, T> &another);

	reference operator*() const;
	reference operator[](difference_type off) const;
	pointer operator->() const;

	ThisType& operator++();
	ThisType operator++(int);
	ThisType& operator+=(difference_type off);
	ThisType& operator--();
	ThisType operator--(int);
	ThisType& operator-=(difference_type off);

private:
	XArrayIterator(pointer cur);
	pointer cur_;
};

/*!
	@brief Manages data in the array: Array Class Wrapper
	@note Only a simple types are available;
	      e.g)
			OK: int32_t, uint8_t, std::pair, std::string*, XArray*, File*
			NG: std::string, util::XArray, util::File
*/
template< typename T, typename Alloc = StdAllocator<T, StackAllocator> >
class XArray {
public:
	typedef Alloc allocator_type;
	typedef typename allocator_type::size_type size_type;
	typedef typename allocator_type::difference_type difference_type;
	typedef typename allocator_type::value_type value_type;
	typedef typename allocator_type::pointer pointer;
	typedef typename allocator_type::reference reference;
	typedef typename allocator_type::const_pointer const_pointer;
	typedef typename allocator_type::const_reference const_reference;

	typedef XArrayIterator<T, T> iterator;
	typedef XArrayIterator<const T, T> const_iterator;
	typedef std::reverse_iterator<iterator> reverse_iterator;
	typedef std::reverse_iterator<const_iterator> const_reverse_iterator;


	explicit XArray(const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc));


	template<typename Iter>
	XArray(Iter first, Iter last, const Alloc &alloc = Alloc());


	~XArray();


	Alloc get_allocator() const { return allocator_; }


	template<typename Iter>
	void assign(Iter first, Iter last);

	void assign(size_t count, const T &value);

	void clear();

	void swap(XArray<T, Alloc> &another);


	void resize(size_t count);

	void resize(size_t count, T value);

	size_t size() const;

	bool empty() const;


	void reserve(size_t requestedCapacity);

	size_t capacity() const;


	iterator begin() { return iterator(data_); }
	iterator end() { return iterator(tail_); }

	const_iterator begin() const { return const_iterator(data_); }
	const_iterator end() const { return const_iterator(tail_); }

	reverse_iterator rbegin() { return reverse_iterator( end() ); }
	reverse_iterator rend() { return reverse_iterator( begin() ); }

	const_reverse_iterator rbegin() const { return const_reverse_iterator( end() ); }
	const_reverse_iterator rend() const { return const_reverse_iterator( begin() ); }


	T& front();
	const T& front() const;
	T& back();
	const T& back() const;

	void push_back(const T &value);

	void push_back(const T *values, size_t count);

	void pop_back();


	T& operator[](size_t index);
	const T& operator[](size_t index) const;

	T* data();

	const T* data() const;


	iterator insert(iterator pos, const T &value);

	template<typename Iter>
	void insert(iterator pos, Iter first, Iter last);


	iterator erase(iterator pos);
	iterator erase(iterator first, iterator last);

protected:
	XArray(const Alloc &alloc, size_t capacityCount);

private:
	void reserveInternal(size_t requestedCapacity);

	XArray(const XArray&);
	XArray& operator=(const XArray&);

	Alloc allocator_;
	size_t restSize_;
	T *data_;		
	T *tail_;		
};

/*!
	@brief XArray using usual dynamic memory allocator.
*/
template<typename T>
class NormalXArray : public XArray< T, std::allocator<T> > {
private:
	typedef std::allocator<T> Alloc;
	typedef XArray<T, Alloc> BaseType;

public:
	explicit NormalXArray(const Alloc& alloc = Alloc()) :
			BaseType(alloc) {
	}

	template<typename Iter>
	NormalXArray(Iter first, Iter last, const Alloc& alloc = Alloc()) :
			BaseType(first, last, alloc) {
	}
};

template<typename T>
class AllocXArray : public XArray< T, StdAllocator<T, void> > {
private:
	typedef StdAllocator<T, void> Alloc;
	typedef XArray<T, Alloc> BaseType;

public:
	explicit AllocXArray(
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(alloc) {
	}

	template<typename Iter>
	AllocXArray(
			Iter first, Iter last,
			const Alloc &alloc UTIL_CONTAINER_ALLOC_ARG_OPT(Alloc)) :
			BaseType(first, last, alloc) {
	}
};

template<typename T, typename Alloc> class BArray;

/*!
	@brief Iterates for BArray.
*/
template<typename V, typename T>
class BArrayIterator {
private:
	typedef BArrayIterator<V, T> ThisType;
	typedef T *Block;
	template<typename TF, typename AllocF> friend class BArray;
	template<typename VF, typename TF> friend class BArrayIterator;

	template<typename VA, typename TA>
	struct Traits {
		typedef XArrayIterator<const Block, Block> BlockIterator;
		typedef const TA *Pointer;
		typedef const TA &Reference;
	};

	template<typename TA>
	struct Traits<TA, TA> {
		typedef XArrayIterator<Block, Block> BlockIterator;
		typedef TA *Pointer;
		typedef TA &Reference;
	};

	typedef typename Traits<V, T>::BlockIterator BlockIterator;

public:
	typedef std::random_access_iterator_tag iterator_category;
	typedef V value_type;
	typedef ptrdiff_t difference_type;
	typedef ptrdiff_t distance_type;
	typedef typename Traits<T, V>::Pointer pointer;
	typedef typename Traits<T, V>::Reference reference;

	BArrayIterator();
	template<typename VA>
	BArrayIterator(const BArrayIterator<VA, T> &another);
	template<typename VA>
	ThisType& operator=(const BArrayIterator<VA, T> &another);

	reference operator*() const;
	reference operator[](difference_type off) const;
	pointer operator->() const;

	ThisType& operator++();
	ThisType operator++(int);
	ThisType& operator+=(difference_type off);
	ThisType& operator--();
	ThisType operator--(int);
	ThisType& operator-=(difference_type off);

	difference_type getBlockSize() const {
		return blockEnd_ - blockBegin_;
	}

	pointer cur_;
	pointer blockBegin_;
	pointer blockEnd_;
	BlockIterator blockItr_;

private:
	template<typename BA>
	BArrayIterator(pointer cur, pointer blockBegin, pointer blockEnd,
		const XArrayIterator<BA, Block> &blockItr);
};

class BArrayOption {
public:
	static const size_t DEFAULT_BLOCK_BITS = 5;

	explicit BArrayOption(
			size_t blockBits = DEFAULT_BLOCK_BITS,
			size_t initialCapacity = 0) :
			initialCapacity_(initialCapacity),
			blockBits_(blockBits) {
	}

	void getBlockBits(size_t value) { blockBits_ = value; }
	size_t getBlockBits() const { return blockBits_; }

	void getInitialCapacity(size_t value) { initialCapacity_ = value; }
	size_t getInitialCapacity() const { return initialCapacity_; }

private:
	size_t initialCapacity_;
	size_t blockBits_;
};

/*!
	@brief Manages array, using a block allocator.
*/
template< typename T, typename Alloc = StdAllocator<T, StackAllocator> >
class BArray {
public:
	typedef Alloc allocator_type;
	typedef typename allocator_type::size_type size_type;
	typedef typename allocator_type::difference_type difference_type;
	typedef typename allocator_type::value_type value_type;
	typedef typename allocator_type::pointer pointer;
	typedef typename allocator_type::reference reference;
	typedef typename allocator_type::const_pointer const_pointer;
	typedef typename allocator_type::const_reference const_reference;

	typedef BArrayIterator<T, T> iterator;
	typedef BArrayIterator<const T, T> const_iterator;
	typedef std::reverse_iterator<iterator> reverse_iterator;
	typedef std::reverse_iterator<const_iterator> const_reverse_iterator;


#if UTIL_DISABLE_CONTAINER_DEFAULT_CONSTRUCTOR
	explicit BArray(const Alloc &alloc);
	explicit BArray(const BArrayOption &option, const Alloc &alloc);
#else
	explicit BArray(const Alloc &alloc = Alloc());
	explicit BArray(const BArrayOption &option, const Alloc &alloc = Alloc());
#endif


	template<typename Iter>
	BArray(Iter first, Iter last, const Alloc &alloc = Alloc());

	template<typename Iter>
	BArray(Iter first, Iter last,
		const BArrayOption &option, const Alloc &alloc = Alloc());


	~BArray();


	Alloc get_allocator() const {
		return allocator_type(blockTable_.get_allocator());
	}


	template<typename Iter>
	void assign(Iter first, Iter last);

	void assign(size_t count, const T &value);

	void clear();

	void swap(BArray<T, Alloc> &another);


	void resize(size_t count);

	void resize(size_t count, T value);

	size_t size() const;

	bool empty() const;


	void reserve(size_t count);

	size_t capacity() const;


	iterator begin();
	iterator end();

	const_iterator begin() const;
	const_iterator end() const;

	reverse_iterator rbegin() { return reverse_iterator( end() ); }
	reverse_iterator rend() { return reverse_iterator( begin() ); }

	const_reverse_iterator rbegin() const { return const_reverse_iterator( end() ); }
	const_reverse_iterator rend() const { return const_reverse_iterator( begin() ); }


	T& front();
	const T& front() const;
	T& back();
	const T& back() const;

	void push_back(const T &value);
	void push_back(const T *values, size_t count);

	void pop_back();



	T& operator[](size_t index);
	const T& operator[](size_t index) const;


	iterator insert(iterator pos, const T &value);

	template<typename Iter>
	void insert(iterator pos, Iter first, Iter last);


	iterator erase(iterator pos);
	iterator erase(iterator first, iterator last);

private:
	typedef T *Block;
	typedef typename Alloc::template rebind<Block>::other BlockTableAllocator;
	typedef XArray<Block, BlockTableAllocator> BlockTable;

	BArray(const BArray&);
	BArray& operator=(BArray&);

	void destroy();

	T *tail_;
	T *blockEnd_;

	size_t blockBits_;		

	BlockTable blockTable_;		
	typename BlockTable::iterator blockItr_;		
};

/*!
	@brief NormalBArray using usual dynamic memory allocator.
*/
template<typename T>
class NormalBArray : public BArray< T, std::allocator<T> > {
private:
	typedef BArray< T, std::allocator<T> > BaseType;

public:
	typedef typename BaseType::allocator_type allocator_type;

	explicit NormalBArray(
			const BArrayOption &option = BArrayOption(),
			const allocator_type &alloc = allocator_type()) :
			BaseType(option, alloc) {
	}

	template<typename Iter>
	NormalBArray(
			Iter first, Iter last, const allocator_type &alloc = allocator_type()) :
			BaseType(first, last, alloc) {
	}
};

/*!
	@brief Lists data in order.
*/
template<
		typename T, typename Comp = typename std::set<T>::key_compare,
		typename Base = XArray<T> >
class SortedList {
public:
	typedef Comp value_compare;
	typedef typename Base::allocator_type allocator_type;
	typedef typename allocator_type::size_type size_type;
	typedef typename allocator_type::difference_type difference_type;
	typedef typename allocator_type::value_type value_type;
	typedef typename allocator_type::pointer pointer;
	typedef typename allocator_type::reference reference;
	typedef typename allocator_type::const_pointer const_pointer;
	typedef typename allocator_type::const_reference const_reference;

	typedef typename Base::iterator iterator;
	typedef typename Base::const_iterator const_iterator;
	typedef typename Base::reverse_iterator reverse_iterator;
	typedef typename Base::const_reverse_iterator const_reverse_iterator;

#if UTIL_DISABLE_CONTAINER_DEFAULT_CONSTRUCTOR
	explicit SortedList(const allocator_type &alloc) : base_(alloc) {
	}
#else
	explicit SortedList(const allocator_type &alloc = allocator_type()) :
			base_(alloc) {
	}
#endif

	explicit SortedList(
			const Comp &comp, const allocator_type &alloc = allocator_type()) :
			base_(alloc), comp_(comp) {
	}

	const Base& base() const { return base_; };

	void clear() { base_.clear(); }
	void reserve(size_t count) { base_.reserve(count); }

	size_t size() const { return base_.size(); }
	bool empty() const { return base_.empty(); }

	iterator begin() { return base_.begin(); }
	iterator end() { return base_.end(); }
	const_iterator begin() const { return base_.begin(); }
	const_iterator end() const { return base_.end(); }

	iterator rbegin() { return base_.rbegin(); }
	iterator rend() { return base_.rend(); }
	const_iterator rbegin() const { return base_.rbegin(); }
	const_iterator rend() const { return base_.rend(); }

	void insert(const T &value) {
		base_.insert(upper_bound(value), value);
	}

	iterator find(const T &value) {
		iterator it = lower_bound(value);
		return (it == end() || comp_(value, *it) ? end() : it);
	}

	const_iterator find(const T &value) const {
		return lower_bound(value);
	}

	iterator lower_bound(const T &value) {
		return std::lower_bound(begin(), end(), value, comp_);
	}

	const_iterator lower_bound(const T &value) const {
		return std::lower_bound(begin(), end(), value, comp_);
	}

	iterator upper_bound(const T &value) {
		return std::upper_bound(begin(), end(), value, comp_);
	}

	const_iterator upper_bound(const T &value) const {
		return std::upper_bound(begin(), end(), value, comp_);
	}

	std::pair<iterator, iterator> equal_range(const T &value) {
		return std::equal_range(begin(), end(), value, comp_);
	}

	std::pair<const_iterator, const_iterator> equal_range(const T &value) const {
		return std::equal_range(begin(), end(), value, comp_);
	}

	iterator erase(iterator pos) { return base_.erase(pos); }
	iterator erase(iterator first, iterator last) { return base_.erase(first, last); }

private:
	Base base_;
	Comp comp_;
};

/*!
	@brief Lists data in order using usual dynamic memory allocator.
*/
template< typename T, typename Comp = typename std::set<T>::key_compare >
class NormalSortedList : public SortedList< T, Comp, NormalXArray<T> > {
private:
	typedef SortedList< T, Comp, NormalXArray<T> > BaseType;

public:
	typedef typename BaseType::allocator_type allocator_type;

	explicit NormalSortedList(const allocator_type &alloc) : BaseType(alloc) {
	}

	explicit NormalSortedList(
			const Comp &comp = Comp(),
			const allocator_type &alloc = allocator_type()) :
			BaseType(comp, alloc) {
	}
};

/*!
	@brief Maps entries only referred.
	@note Thread safe
*/
template<typename K, typename V>
class WeakMap {
public:
	WeakMap();
	~WeakMap();

	V& acquire(const K &key);
	void release(const K &key);
	bool isEmpty();

private:
	template<typename Ev>
	struct Entry {
		Entry() : referenceCount_(0) {};

		Ev value_;
		size_t referenceCount_;
	};

	WeakMap(const WeakMap&);
	WeakMap& operator=(const WeakMap&);

	typedef Entry<V> EntryType;
	typedef std::map<K, EntryType*> BaseType;

	BaseType base_;
	Mutex mutex_;
};

/*!
	@brief Refers WeakMap entries
*/
template<typename K, typename V>
class WeakMapReference {
public:
	WeakMapReference(WeakMap<K, V> &weakMap, const K &key);
	~WeakMapReference();
	V* get();
	void unmanage();

private:
	WeakMapReference(const WeakMapReference&);
	WeakMapReference& operator=(const WeakMapReference&);

	WeakMap<K, V> *weakMap_;
	const K key_;
	V *value_;
};


namespace detail {

struct ConcurrentQueueUtils {
	typedef void (*ErrorHandler)(std::exception&);
	static void errorQueueClosed(ErrorHandler customHandler);
};

template< typename T, typename Alloc = StdAllocator<void, void> >
class BlockingQueue {
public:
	typedef ConcurrentQueueUtils::ErrorHandler ErrorHandler;

	explicit BlockingQueue(const Alloc &alloc);
	~BlockingQueue();

	void push(AllocUniquePtr<T> &ptr);
	bool poll(AllocUniquePtr<T> &ptr);

	void wait();

	bool isClosed();
	void close();

	void setErrorHandler(ErrorHandler handler);

private:
	struct Entry {
		Entry(T *value, const AllocDefaultDelete<T> &deteter);

		T *value_;
		AllocDefaultDelete<T> deteter_;
	};

	typedef typename Alloc::template rebind<Entry>::other EntryAllocator;

	BlockingQueue(const BlockingQueue&);
	BlockingQueue& operator=(const BlockingQueue&);

	Condition cond_;
	Deque<Entry, EntryAllocator> base_;
	ErrorHandler errorHandler_;
	bool closed_;
};

class FuturePool {
public:
	FuturePool(
			const AllocatorInfo &info, const StdAllocator<void, void> &alloc);

	StdAllocator<void, void>& getAllocator() throw();
	ObjectPool<Condition, Mutex>& getConditionPool() throw();

private:
	FuturePool(const FuturePool&);
	FuturePool& operator=(const FuturePool&);

	StdAllocator<void, void> alloc_;
	ObjectPool<Condition, Mutex> condPool_;
};

template<typename T>
class FutureBase {
public:
	explicit FutureBase(FuturePool &pool);
	~FutureBase();

	FutureBase(const FutureBase &another);
	FutureBase& operator=(const FutureBase&);

	T* poll();
	void waitFor(uint32_t timeoutMillis);
	void setValue(const T &value);
	void setException(std::exception &e);

	bool isCancelled();
	void cancel() throw();

private:
	struct Data;

	void reset() throw();
	Data& resolveData();

	static void cancelInternal(
			Data &data, const LockGuard<Condition> &gurad) throw();
	static bool isResultAcceptable(
			Data &data, const LockGuard<Condition>&) throw();

	Data *data_;
	bool forConsumer_;
};

template<typename T>
struct FutureBase<T>::Data {
	explicit Data(FuturePool &pool);
	~Data();

	Atomic<uint64_t> refCount_;
	Atomic<uint64_t> consumerCount_;

	FuturePool &pool_;
	Condition *cond_;

	LocalUniquePtr<T> value_;
	LocalUniquePtr< std::pair<Exception, bool> > exception_;
	bool cancelled_;
	bool retrieved_;
};

struct FutureBaseUtils {
	typedef std::pair<Exception, bool> ExceptionInfo;
	static void errorNotAssigned();
	static void errorRetrieved();
	static void errorCancelled();
	static void errorServiceShutdown(std::exception &e);

	static void raiseExceptionResult(const ExceptionInfo &ex);
	static void assignExceptionResult(
			std::exception &e, LocalUniquePtr<ExceptionInfo> &dest) throw();
};

class TaskBase {
public:
	virtual ~TaskBase();
	virtual void operator()() = 0;

protected:
	TaskBase();

private:
	TaskBase(const TaskBase&);
	TaskBase& operator=(const TaskBase&);
};

template<typename C>
class BasicTask : public TaskBase {
public:
	typedef typename C::ResultType ResultType;
	typedef FutureBase<ResultType> FutureType;

	BasicTask(FuturePool &pool, const C &command);
	virtual ~BasicTask();

	virtual void operator()();
	FutureBase<ResultType> getFuture();

private:
	FutureBase<ResultType> future_;
	C command_;
};

} 

template<typename T>
class Future {
public:
	explicit Future(const detail::FutureBase<T> &base);

	T* poll();
	void waitFor(uint32_t timeoutMillis);

private:
	detail::FutureBase<T> base_;
};

class ExecutorService {
public:
	template<typename C>
	struct FutureOf {
		typedef Future<typename detail::BasicTask<C>::ResultType> Type;
	};

	virtual ~ExecutorService();

	template<typename C>
	typename FutureOf<C>::Type submit(const C &command);

	virtual void start() = 0;
	virtual void shutdown() = 0;
	virtual void waitForShutdown() = 0;

protected:
	ExecutorService();

	virtual void submitTask(AllocUniquePtr<detail::TaskBase> &task) = 0;
	virtual detail::FuturePool& getFuturePool() = 0;
};

class SingleThreadExecutor : public ExecutorService {
public:
	SingleThreadExecutor(
			const AllocatorInfo &info, const StdAllocator<void, void> &alloc);
	virtual ~SingleThreadExecutor();

	virtual void start();
	virtual void shutdown();
	virtual void waitForShutdown();

protected:
	virtual void submitTask(AllocUniquePtr<detail::TaskBase> &task);
	virtual detail::FuturePool& getFuturePool();

private:
	typedef detail::BlockingQueue<detail::TaskBase> TaskQueue;

	static SingleThreadExecutor *defaultInstance_;

	class Runner : public ThreadRunner {
	public:
		explicit Runner(TaskQueue &queueRef);
		virtual ~Runner();
		virtual void run();

	private:
		TaskQueue &queueRef_;
	};

	detail::FuturePool futurePool_;
	TaskQueue queue_;
	Runner runner_;
	Thread thread_;
};

template< typename C, typename Alloc = StdAllocator<void, void> >
class BatchCommand {
public:
	typedef typename detail::BasicTask<C>::ResultType SubResultType;
	typedef typename Alloc::template rebind<SubResultType>::other ResultAllocator;
	typedef typename Alloc::template rebind<C>::other CommnandAllocator;
	typedef Vector<SubResultType, ResultAllocator> ResultType;
	typedef Vector<C, CommnandAllocator> CommnandList;
	typedef typename CommnandList::const_iterator CommandIterator;

	explicit BatchCommand(const Alloc &alloc);

	void add(const C &command);
	template<typename It> void addAll(It begin, It end);
	void clear();

	CommandIterator begin() const;
	CommandIterator end() const;

	ResultType operator()();

private:
	CommnandList commandList_;
};


class InsertionResetter {
public:
	template<typename C>
	explicit InsertionResetter(C &container);

	template<typename C>
	InsertionResetter(C &container, typename C::const_iterator insertionPoint);

	~InsertionResetter();

	void release() throw();
	void reset() throw();

private:
	typedef void (*ResetterFunc)(void*, size_t);

	struct Entry {
		Entry();
		Entry(ResetterFunc func, void *container, size_t pos);

		ResetterFunc func_;
		void *container_;
		size_t pos_;
	};

	template<typename C>
	static Entry createEntry(
			C &container, typename C::const_iterator insertionPoint);

	template<typename C>
	static void resetSpecific(void *container, size_t pos);

	InsertionResetter(const InsertionResetter&);
	InsertionResetter& operator=(const InsertionResetter&);

	Entry entry_;
};


/*!
	@brief Manages byte stream for XArray.
*/
template< typename Alloc = StdAllocator<uint8_t, StackAllocator> >
class XArrayOutStream : public BasicByteStream< XArrayOutStream<Alloc> > {
public:
	inline explicit XArrayOutStream(XArray<uint8_t, Alloc> &buffer);

	size_t write(const void *buf, size_t length);

	void writeAll(const void *buf, size_t length);

	inline size_t position() const { return buffer_.size(); }

	inline void position(size_t pos) { buffer_.resize(pos); }

	inline void clear() { position(0); }

private:
	XArray<uint8_t, Alloc> &buffer_;
};

typedef ByteStream< XArrayOutStream<> > XArrayByteOutStream;

typedef XArrayOutStream< std::allocator<uint8_t> > NormalOutStream;
typedef ByteStream<NormalOutStream> NormalByteOutStream;

/*!
	@brief Manages byte stream for writable object, e.g. files.
*/
template<typename Base>
class BufferedOutStream : public BasicByteStream< BufferedOutStream<Base> > {
public:
	inline explicit BufferedOutStream(
		Base &base, NormalXArray<uint8_t> &buffer, size_t limitSize);

	inline ~BufferedOutStream();

	inline size_t write(const void *buf, size_t length);

	inline void flush();

private:
	Base base_;
	NormalXArray<uint8_t> &buffer_;
	size_t limit_;
};



#ifdef _MSC_VER
typedef class XArray<int32_t> Int32XArray;
typedef class XArray<uint32_t> UInt32XArray;
typedef class BArray<int32_t> Int32BArray;
typedef class BArray<uint32_t> UInt32BArray;
#endif



template<typename T, typename Mutex>
ObjectPool<T, Mutex>::ObjectPool(const AllocatorInfo &info) :
		freeElementLimit_(std::numeric_limits<size_t>::max()),
		freeLink_(NULL),
		freeElementCount_(0),
		base_(info, Utils::ELEMENT_OFFSET + sizeof(T)),
		stats_(info) {

	util::AllocatorManager::addAllocator(stats_.info_, *this);
}

template<typename T, typename Mutex>
ObjectPool<T, Mutex>::~ObjectPool() {
	try {
		clear(0);
	}
	catch (...) {
	}

	assert(freeLink_ == NULL);
	assert(freeElementCount_ == 0);

	util::AllocatorManager::removeAllocator(stats_.info_, *this);
}

template<typename T, typename Mutex>
T* ObjectPool<T, Mutex>::allocate() {
	T *element = poll();

	if (element == NULL) {
		FreeLink *entry = static_cast<FreeLink*>(base_.allocate());

		entry->next_ = NULL;
		return new (Utils::elementOf(entry)) T();
	}

	return element;
}

template<typename T, typename Mutex>
void ObjectPool<T, Mutex>::deallocate(T *element) {
	if (element == NULL) {
		return;
	}

	FreeLink *entry = Utils::linkOf(element);

	do {
		const size_t baseCount = base_.getFreeElementCount();

		LockGuard<Mutex> guard(getLock());

		if (mergeFreeElementCount(baseCount, guard) >= freeElementLimit_) {
			break;
		}

		entry->next_ = freeLink_;
		freeLink_ = entry;
		++freeElementCount_;

		return;
	}
	while (false);

	element->~T();
	base_.deallocate(entry);
}

template<typename T, typename Mutex>
T* ObjectPool<T, Mutex>::poll() {
	LockGuard<Mutex> guard(getLock());

	FreeLink *cur = freeLink_;

	if (cur == NULL) {
		return NULL;
	}

	freeLink_ = cur->next_;
	cur->next_ = NULL;

	assert(freeElementCount_ > 0);
	--freeElementCount_;

	return static_cast<T*>(Utils::elementOf(cur));
}

template<typename T, typename Mutex>
void ObjectPool<T, Mutex>::setTotalElementLimit(size_t limit) {
	base_.setTotalElementLimit(limit);
}

template<typename T, typename Mutex>
void ObjectPool<T, Mutex>::setFreeElementLimit(size_t limit) {
#if UTIL_OBJECT_POOL_NO_CACHE
	static_cast<void>(limit);
	clear(freeElementLimit_);
#else
	{
		UTIL_ALLOCATOR_DETAIL_STAT_GUARD(guard, getLock());
		freeElementLimit_ = limit;
	}

	clear(limit);
#endif
}

template<typename T, typename Mutex>
size_t ObjectPool<T, Mutex>::getTotalElementCount() {
	return base_.getTotalElementCount();
}

template<typename T, typename Mutex>
size_t ObjectPool<T, Mutex>::getFreeElementCount() {
	const size_t baseCount = base_.getFreeElementCount();
	UTIL_ALLOCATOR_DETAIL_STAT_GUARD(guard, getLock());

	return mergeFreeElementCount(baseCount, guard);
}

template<typename T, typename Mutex>
typename ObjectPool<T, Mutex>::BaseAllocator&
ObjectPool<T, Mutex>::base() {
	return base_;
}

template<typename T, typename Mutex>
StdAllocator<T, void> ObjectPool<T, Mutex>::getStdAllocator() {
	typedef detail::ObjectPoolAllocator<T, Mutex> Base;
	Base &base = Base::get(*this);
	return StdAllocator<T, void>(base);
}

template<typename T, typename Mutex>
void ObjectPool<T, Mutex>::getStats(AllocatorStats &stats) {
	size_t localCount;
	{
		UTIL_ALLOCATOR_DETAIL_STAT_GUARD(guard, getLock());
		localCount = freeElementCount_;
	}

	stats.merge(stats_);
	stats.values_[AllocatorStats::STAT_CACHE_SIZE] +=
			AllocatorStats::asStatValue(localCount * base_.getElementSize());
}

template<typename T, typename Mutex>
void ObjectPool<T, Mutex>::setLimit(AllocatorStats::Type type, size_t value) {
	switch (type) {
#if UTIL_OBJECT_POOL_NO_CACHE
	default:
		static_cast<void>(value);
		break;
#else
	case AllocatorStats::STAT_CACHE_LIMIT:
		setFreeElementLimit(value / base_.getElementSize());
		break;
	default:
		break;
#endif
	}
}

template<typename T, typename Mutex>
void ObjectPool<T, Mutex>::setLimit(
		AllocatorStats::Type type, AllocatorLimitter *limitter) {
	(void) type;
	(void) limitter;
}

template<typename T, typename Mutex>
void ObjectPool<T, Mutex>::clear(size_t preservedCount) {
	const size_t baseCount = base_.getFreeElementCount();
	size_t rest = 0;
	do {
		FreeLink *cur;
		{
			LockGuard<Mutex> guard(getLock());

			if (rest == 0) {
				const size_t count = mergeFreeElementCount(baseCount, guard);
				if (preservedCount >= count) {
					break;
				}
				rest = count - preservedCount;
			}

			cur = freeLink_;
			if (cur == NULL) {
				break;
			}

			FreeLink *next = cur->next_;
			freeLink_ = next;

			assert(freeElementCount_ > 0);
			freeElementCount_--;
		}

		static_cast<T*>(Utils::elementOf(cur))->~T();
		base_.deallocate(cur);
	}
	while (--rest > 0);
}

template<typename T, typename Mutex>
size_t ObjectPool<T, Mutex>::mergeFreeElementCount(
		size_t baseCount, const util::LockGuard<Mutex>&) {
	return freeElementCount_ + baseCount;
}

template<typename T, typename Mutex>
Mutex& ObjectPool<T, Mutex>::getLock() {
	return base_.getLock();
}


namespace detail {
template<typename Mutex, typename D>
ObjectPoolDirectAllocator<Mutex, D>::ObjectPoolDirectAllocator(
		FreeLink *&freeLink, size_t &freeElementCount, BaseAllocator &base,
		const D &elementDestructor) :
		freeLink_(freeLink),
		freeElementCount_(freeElementCount),
		base_(base),
		elementDestructor_(elementDestructor) {
}

template<typename Mutex, typename D>
void* ObjectPoolDirectAllocator<Mutex, D>::allocateDirect(
		size_t elementSize) const {
	assert(elementSize == Utils::getElementSize(base_.getElementSize()));
	static_cast<void>(elementSize);

	FreeLink *cur;
	do {
		LockGuard<Mutex> guard(getLock());

		cur = freeLink_;
		if (cur == NULL) {
			break;
		}

		freeLink_ = cur->next_;

		assert(freeElementCount_ > 0);
		--freeElementCount_;
	}
	while (false);

	if (cur == NULL) {
		FreeLink *entry = static_cast<FreeLink*>(base_.allocate());

		entry->next_ = NULL;
		return Utils::elementOf(entry);
	}

	cur->next_ = NULL;

	void *rawElement = Utils::elementOf(cur);
	elementDestructor_(rawElement);

	return rawElement;
}

template<typename Mutex, typename D>
void ObjectPoolDirectAllocator<Mutex, D>::deallocateDirect(
		void *rawElement) const throw() {
	if (rawElement == NULL) {
		return;
	}

	FreeLink *entry = Utils::linkOf(rawElement);
	assert(entry->next_ == NULL);
	try {
		base_.deallocate(entry);
	}
	catch (...) {
	}
}
}	


template<typename V, typename T>
XArrayIterator<V, T>::XArrayIterator() : cur_(NULL) {
}

template<typename V, typename T>
XArrayIterator<V, T>::XArrayIterator(pointer cur) : cur_(cur) {
}

template<typename V, typename T>
template<typename VA>
XArrayIterator<V, T>::XArrayIterator(const XArrayIterator<VA, T> &another) :
	cur_(another.cur_)
{
}

template<typename V, typename T>
template<typename VA>
XArrayIterator<V, T>&
XArrayIterator<V, T>::operator=(const XArrayIterator<VA, T> &another) {
	cur_ = another.cur_;
	return *this;
}

template<typename V, typename T>
typename XArrayIterator<V, T>::reference
XArrayIterator<V, T>::operator*() const {
	return *cur_;
}

template<typename V, typename T>
typename XArrayIterator<V, T>::reference
XArrayIterator<V, T>::operator[](difference_type off) const {
	return *(cur_ + off);
}

template<typename V, typename T>
typename XArrayIterator<V, T>::pointer
XArrayIterator<V, T>::operator->() const {
	return cur_;
}

template<typename V, typename T>
XArrayIterator<V, T>&
XArrayIterator<V, T>::operator++() {
	cur_++;
	return *this;
}

template<typename V, typename T>
XArrayIterator<V, T>
XArrayIterator<V, T>::operator++(int) {
	ThisType prevIterator = *this;
	cur_++;
	return prevIterator;
}

template<typename V, typename T>
XArrayIterator<V, T>&
XArrayIterator<V, T>::operator+=(difference_type off) {
	cur_ += off;
	return *this;
}

template<typename V, typename T>
XArrayIterator<V, T>&
XArrayIterator<V, T>::operator--() {
	cur_--;
	return *this;
}

template<typename V, typename T>
XArrayIterator<V, T>
XArrayIterator<V, T>::operator--(int) {
	XArrayIterator<V, T> prevIterator = *this;
	cur_--;
	return prevIterator;
}

template<typename V, typename T>
XArrayIterator<V, T>&
XArrayIterator<V, T>::operator-=(difference_type off) {
	cur_ -= off;
	return *this;
}

template<typename V1, typename V2, typename T>
inline bool operator==(
	const XArrayIterator<V1, T> &i1, const XArrayIterator<V2, T> &i2)
{
	return &(*i1) == &(*i2);
}

template<typename V1, typename V2, typename T>
inline bool operator!=(
	const XArrayIterator<V1, T> &i1, const XArrayIterator<V2, T> &i2)
{
	return &(*i1) != &(*i2);
}

template<typename V1, typename V2, typename T>
inline bool operator<(
	const XArrayIterator<V1, T> &i1, const XArrayIterator<V2, T> &i2)
{
	return &(*i1) < &(*i2);
}

template<typename V1, typename V2, typename T>
inline bool operator<=(
	const XArrayIterator<V1, T> &i1, const XArrayIterator<V2, T> &i2)
{
	return &(*i1) <= &(*i2);
}

template<typename V1, typename V2, typename T>
inline bool operator>(
	const XArrayIterator<V1, T> &i1, const XArrayIterator<V2, T> &i2)
{
	return &(*i1) > &(*i2);
}

template<typename V1, typename V2, typename T>
inline bool operator>=(
	const XArrayIterator<V1, T> &i1, const XArrayIterator<V2, T> &i2)
{
	return &(*i1) >= &(*i2);
}

template<typename V, typename T>
inline XArrayIterator<V, T> operator+(
	const XArrayIterator<V, T> &i,
	typename XArrayIterator<V, T>::difference_type off)
{
	XArrayIterator<V, T> iterator = i;
	iterator += off;
	return iterator;
}

template<typename V, typename T>
inline XArrayIterator<V, T> operator+(
	typename XArrayIterator<V, T>::difference_type off,
	const XArrayIterator<V, T> &i)
{
	XArrayIterator<V, T> iterator = i;
	iterator += off;
	return iterator;
}

template<typename V, typename T>
inline XArrayIterator<V, T> operator-(
	const XArrayIterator<V, T> &i,
	typename XArrayIterator<V, T>::difference_type off)
{
	XArrayIterator<V, T> iterator = i;
	iterator -= off;
	return iterator;
}

template<typename V1, typename V2, typename T>
inline typename XArrayIterator<V1, T>::difference_type operator-(
	const XArrayIterator<V1, T> &i1, const XArrayIterator<V2, T> &i2)
{
	return &(*i1) - &(*i2);
}


template<typename T, typename Alloc>
XArray<T, Alloc>::XArray(const Alloc &alloc) :
	allocator_(alloc), restSize_(0), data_(NULL), tail_(NULL)
{
}

template<typename T, typename Alloc>
XArray<T, Alloc>::XArray(const Alloc &alloc, size_t capacityCount) :
	allocator_(alloc), restSize_(0), data_(NULL), tail_(NULL)
{
	reserve(capacityCount);
}

template<typename T, typename Alloc>
template<typename Iter>
XArray<T, Alloc>::XArray(Iter first, Iter last, const Alloc &alloc) :
	allocator_(alloc), restSize_(0), data_(NULL), tail_(NULL)
{
	assign(first, last);
}

template<typename T, typename Alloc>
XArray<T, Alloc>::~XArray() {
	if (data_ != NULL) {
		allocator_.deallocate(data_, capacity());
		data_ = NULL;
		tail_ = NULL;
		restSize_ = 0;
	}
}

template<typename T, typename Alloc>
template<typename Iter>
void XArray<T, Alloc>::assign(Iter first, Iter last) {
	clear();
	for (; first != last; ++first) {
		push_back(*first);
	}
}

template<typename T, typename Alloc>
void XArray<T, Alloc>::assign(size_t count, const T &value) {
	clear();
	reserve(count);
	for (; count > 0; --count) {
		push_back(value);
	}
}

template<typename T, typename Alloc>
void XArray<T, Alloc>::clear() {
	restSize_ += size();
	tail_ = data_;
}

template<typename T, typename Alloc>
void XArray<T, Alloc>::swap(XArray<T, Alloc> &another) {
	if (allocator_ == another.allocator_) {
		std::swap(data_, another.data_);
		std::swap(tail_, another.tail_);
		std::swap(restSize_, another.restSize_);
	}
	else {
		XArray<T, Alloc> tmp(another.begin(), another.end(), allocator_);
		another.assign(begin(), end());
		assign(tmp.begin(), tmp.end());
	}
}

template<typename T, typename Alloc>
size_t XArray<T, Alloc>::size() const {
	return static_cast<size_t>(tail_ - data_);
}

template<typename T, typename Alloc>
bool XArray<T, Alloc>::empty() const {
	return (tail_ == data_);
}

template<typename T, typename Alloc>
void XArray<T, Alloc>::resize(size_t count) {
	reserve(count);
	restSize_ = capacity() - count;
	tail_ = data_ + count;
}

template<typename T, typename Alloc>
void XArray<T, Alloc>::resize(size_t count, T value) {
	const size_t orgCount = size();
	resize(count);

	if (orgCount < count) {
		std::fill(begin() + orgCount, end(), value);
	}
}

template<typename T, typename Alloc>
void XArray<T, Alloc>::reserve(size_t requestedCapacity) {
	if (capacity() < requestedCapacity) {
		reserveInternal(requestedCapacity);
	}
}

template<typename T, typename Alloc>
size_t XArray<T, Alloc>::capacity() const {
	return size() + restSize_;
}

template<typename T, typename Alloc>
T& XArray<T, Alloc>::front() {
	assert ( !empty() );
	return *begin();
}

template<typename T, typename Alloc>
const T& XArray<T, Alloc>::front() const {
	assert ( !empty() );
	return *begin();
}

template<typename T, typename Alloc>
T& XArray<T, Alloc>::back() {
	assert ( !empty() );
	return *( --end() );
}

template<typename T, typename Alloc>
const T& XArray<T, Alloc>::back() const {
	assert ( !empty() );
	return *( --end() );
}

template<typename T, typename Alloc>
void XArray<T, Alloc>::push_back(const T &value) {
	if (restSize_ == 0) {
		reserveInternal(size() + 1);
	}
	restSize_--;
	*(tail_++) = value;
}

template<typename T, typename Alloc>
void XArray<T, Alloc>::push_back(const T *values, size_t count) {
	if (restSize_ < count) {
		reserveInternal(size() + count);
	}

	restSize_ -= count;
	memcpy(tail_, values, count * sizeof(T));
	tail_ += count;
}

template<typename T, typename Alloc>
void XArray<T, Alloc>::pop_back() {
	assert ( !empty() );
	tail_--;
	restSize_++;
}

template<typename T, typename Alloc>
T& XArray<T, Alloc>::operator[](size_t index) {
	assert ( index < size() );
	return data_[index];
}

template<typename T, typename Alloc>
const T& XArray<T, Alloc>::operator[](size_t index) const {
	assert ( index < size() );
	return data_[index];
}

template<typename T, typename Alloc>
T* XArray<T, Alloc>::data() {
	return data_;
}

template<typename T, typename Alloc>
const T* XArray<T, Alloc>::data() const {
	return data_;
}

template<typename T, typename Alloc>
typename XArray<T, Alloc>::iterator XArray<T, Alloc>::insert(
	iterator pos, const T &value)
{
	assert(data_ <= pos.cur_ && pos.cur_ <= tail_);
	const ptrdiff_t posIndex = pos.cur_ - data_;

	if (restSize_ == 0) {
		reserveInternal(size() + 1);
	}

	T *newPos = data_ + posIndex;
	memmove(newPos + 1, newPos, (size() - posIndex) * sizeof(T));
	*newPos = value;
	tail_++;
	restSize_--;

	return iterator(newPos);
}

template<typename T, typename Alloc>
template<typename Iter>
void XArray<T, Alloc>::insert(iterator pos, Iter first, Iter last) {
	assert(data_ <= pos.cur_ && pos.cur_ <= tail_);
	const ptrdiff_t posIndex = pos.cur_ - data_;

	size_t count = 0;
	for (Iter i = first; i != last; i++) {
		count++;
	}

	if (restSize_ < count) {
		reserveInternal(size() + count);
	}

	T *dest = data_ + posIndex;
	memmove(dest + count, dest, (size() - posIndex) * sizeof(T));
	for (Iter src = first; src != last; ++dest, ++src) {
		*dest = *src;
	}
	tail_ += count;
	restSize_ -= count;
}

template<typename T, typename Alloc>
typename XArray<T, Alloc>::iterator XArray<T, Alloc>::erase(iterator pos) {
	assert(data_ <= pos.cur_ && pos.cur_ < tail_);

	memmove(pos.cur_, pos.cur_ + 1, (tail_ - pos.cur_) * sizeof(T));
	tail_--;
	restSize_++;

	return iterator(pos.cur_);
}

template<typename T, typename Alloc>
typename XArray<T, Alloc>::iterator XArray<T, Alloc>::erase(
	iterator first, iterator last)
{
	assert(first.cur_ <= last.cur_);
	assert(data_ <= first.cur_ && last.cur_ <= tail_);

	const ptrdiff_t count = last.cur_ - first.cur_;
	memmove(first.cur_, first.cur_ + count, sizeof(T) * (tail_ - last.cur_));
	tail_ -= count;
	restSize_ += count;

	return iterator(first.cur_);
}

template<typename T, typename Alloc>
void XArray<T, Alloc>::reserveInternal(size_t requestedCapacity) {
	assert (0 < requestedCapacity);
	assert (size() < requestedCapacity);

	const uint32_t MIN_CAPACITY_BIT = 4;	
	const size_t usedSize = this->size();
	size_t newCapacity = (1U << std::max<uint32_t>(
		MIN_CAPACITY_BIT,
		static_cast<uint32_t>(sizeof(uint32_t) * CHAR_BIT) -
		nlz(static_cast<uint32_t>(requestedCapacity - 1)) ));
	if (newCapacity < requestedCapacity) {
		UTIL_THROW_UTIL_ERROR(CODE_SIZE_LIMIT_EXCEEDED,
				"Too large array capacity requested (size=" <<
				requestedCapacity << ")");
	}

#if UTIL_MEMORY_POOL_AGGRESSIVE
	typedef detail::DirectAllocationUtils Utils;
	if (newCapacity >= (1U << (Utils::LARGE_ELEMENT_BITS - 1)) / sizeof(T)) {
		const size_t margin =
				(Utils::ELEMENT_MARGIN_SIZE + sizeof(T) - 1) / sizeof(T);
		if (margin <= newCapacity - requestedCapacity) {
			newCapacity -= margin;
		}
		else if (newCapacity < (1U << (sizeof(uint32_t) * CHAR_BIT - 1)) &&
				newCapacity > margin) {
			newCapacity += newCapacity - margin;
		}
	}
#endif

	T *newData;
	try {
		newData = allocator_.allocate(newCapacity);
		assert (newData != NULL);
	}
	catch (std::bad_alloc &e) {
		UTIL_RETHROW_UTIL_ERROR(CODE_NO_MEMORY, e, "Allocation failed");
	}

	if (data_ != NULL) {
		memcpy(newData, data_, sizeof(T) * usedSize);
		allocator_.deallocate(data_, capacity());
	}

	data_ = newData;
	tail_ = data_ + usedSize;
	restSize_ = newCapacity - usedSize;
}


template<typename V, typename T>
template<typename BA>
BArrayIterator<V, T>::BArrayIterator(
	pointer cur, pointer blockBegin, pointer blockEnd,
	const XArrayIterator<BA, Block> &blockItr) :
	cur_(cur),
	blockBegin_(blockBegin),
	blockEnd_(blockEnd),
	blockItr_(blockItr)
{
}

template<typename V, typename T>
BArrayIterator<V, T>::BArrayIterator() : cur_(NULL) {
	assert ( (blockBegin_ = NULL, true) );
	assert ( (blockEnd_ = NULL, true) );
}

template<typename V, typename T>
template<typename VA>
BArrayIterator<V, T>::BArrayIterator(
	const BArrayIterator<VA, T> &another) :
	cur_(another.cur_),
	blockBegin_(another.blockBegin_),
	blockEnd_(another.blockEnd_),
	blockItr_(another.blockItr_)
{
}

template<typename V, typename T>
template<typename VA>
BArrayIterator<V, T>&
BArrayIterator<V, T>::operator=(const BArrayIterator<VA, T> &another) {
	cur_ = another.cur_;
	blockBegin_ = another.blockBegin_;
	blockEnd_ = another.blockEnd_;
	blockItr_ = another.blockItr_;
	return *this;
}

template<typename V, typename T>
typename BArrayIterator<V, T>::reference
BArrayIterator<V, T>::operator*() const {
	assert (cur_ !=  NULL);
	return *cur_;
}

template<typename V, typename T>
typename BArrayIterator<V, T>::reference
BArrayIterator<V, T>::operator[](difference_type off) const {
	assert (cur_ !=  NULL);

	const difference_type blockSize = getBlockSize();
	off += cur_ - blockBegin_;

	if (off < 0) {
		const difference_type blockOff = -((-off + blockSize - 1) / blockSize);
		return *( blockItr_[blockOff] + (-blockOff * blockSize + off) );
	}
	else if (off >= blockSize) {
		const difference_type blockOff = off / blockSize;
		return *( blockItr_[blockOff] + (off - blockOff * blockSize) );
	}

	return *(blockBegin_ + off);
}

template<typename V, typename T>
typename BArrayIterator<V, T>::pointer
BArrayIterator<V, T>::operator->() const {
	assert (cur_ !=  NULL);
	return cur_;
}

template<typename V, typename T>
BArrayIterator<V, T>&
BArrayIterator<V, T>::operator++() {
	cur_++;
	if (cur_ >= blockEnd_) {
		const difference_type blockSize = getBlockSize();
		blockItr_++;
		blockBegin_ = *blockItr_;
		blockEnd_ = blockBegin_ + blockSize;
		cur_ = blockBegin_;
	}
	return *this;
}

template<typename V, typename T>
BArrayIterator<V, T>
BArrayIterator<V, T>::operator++(int) {
	ThisType prevIterator = *this;
	++(*this);
	return prevIterator;
}

template<typename V, typename T>
BArrayIterator<V, T>&
BArrayIterator<V, T>::operator+=(difference_type off) {
	const difference_type blockSize = getBlockSize();
	off += cur_ - blockBegin_;

	if (off < 0) {
		const difference_type blockOff = -((-off + blockSize - 1) / blockSize);
		blockItr_ += blockOff;
		blockBegin_ = *blockItr_;
		blockEnd_ = blockBegin_ + blockSize;
		cur_ = blockBegin_ + (-blockOff * blockSize + off);
		return *this;
	}
	else if (off >= blockSize) {
		const difference_type blockOff = off / blockSize;
		blockItr_ += blockOff;
		blockBegin_ = *blockItr_;
		blockEnd_ = blockBegin_ + blockSize;
		cur_ = blockBegin_ + (off - blockOff * blockSize);
		return *this;
	}

	cur_ = blockBegin_ + off;
	return *this;
}

template<typename V, typename T>
BArrayIterator<V, T>&
BArrayIterator<V, T>::operator--() {
	if (cur_ <= blockBegin_) {
		const difference_type blockSize = blockEnd_ - blockBegin_;
		blockItr_--;
		blockBegin_ = *blockItr_;
		blockEnd_ = blockBegin_ + blockSize;
		cur_ = blockEnd_ - 1;
		return *this;
	}
	cur_--;
	return *this;
}

template<typename V, typename T>
BArrayIterator<V, T>
BArrayIterator<V, T>::operator--(int) {
	ThisType prevIterator = *this;
	--(*this);
	return prevIterator;
}

template<typename V, typename T>
BArrayIterator<V, T>&
BArrayIterator<V, T>::operator-=(difference_type off) {
	assert ( off > std::numeric_limits<difference_type>::min() );
	(*this) += -off;
	return *this;
}

template<typename V1, typename V2, typename T>
inline bool operator==(
	const BArrayIterator<V1, T> &i1, const BArrayIterator<V2, T> &i2)
{
	assert ( i1.getBlockSize() == i2.getBlockSize() );
	return i1.cur_ == i2.cur_;
}

template<typename V1, typename V2, typename T>
inline bool operator!=(
	const BArrayIterator<V1, T> &i1, const BArrayIterator<V2, T> &i2)
{
	assert ( i1.getBlockSize() == i2.getBlockSize() );
	return i1.cur_ != i2.cur_;
}

template<typename V1, typename V2, typename T>
inline bool operator<(
	const BArrayIterator<V1, T> &i1, const BArrayIterator<V2, T> &i2)
{
	assert ( i1.getBlockSize() == i2.getBlockSize() );
	return i1.blockItr_ < i2.blockItr_ ||
		( i1.blockItr_ == i2.blockItr_ && i1.cur_ < i2.cur_ );
}

template<typename V1, typename V2, typename T>
inline bool operator<=(
	const BArrayIterator<V1, T> &i1, const BArrayIterator<V2, T> &i2)
{
	assert ( i1.getBlockSize() == i2.getBlockSize() );
	return i1.blockItr_ <= i2.blockItr_ ||
		( i1.blockItr_ == i2.blockItr_ && i1.cur_ <= i2.cur_ );
}

template<typename V1, typename V2, typename T>
inline bool operator>(
	const BArrayIterator<V1, T> &i1, const BArrayIterator<V2, T> &i2)
{
	assert ( i1.getBlockSize() == i2.getBlockSize() );
	return i1.blockItr_ > i2.blockItr_ ||
		( i1.blockItr_ == i2.blockItr_ && i1.cur_ > i2.cur_ );
}

template<typename V1, typename V2, typename T>
inline bool operator>=(
	const BArrayIterator<V1, T> &i1, const BArrayIterator<V2, T> &i2)
{
	assert ( i1.getBlockSize() == i2.getBlockSize() );
	return i1.blockItr_ >= i2.blockItr_ ||
		( i1.blockItr_ == i2.blockItr_ && i1.cur_ >= i2.cur_ );
}

template<typename V, typename T>
inline BArrayIterator<V, T> operator+(
	const BArrayIterator<V, T> &i,
	typename BArrayIterator<V, T>::difference_type off)
{
	return (BArrayIterator<V, T>(i) += off);
}

template<typename V, typename T>
inline BArrayIterator<V, T> operator+(
	typename BArrayIterator<V, T>::difference_type off,
	const BArrayIterator<V, T> &i)
{
	return (BArrayIterator<V, T>(i) += off);
}

template<typename V, typename T>
inline BArrayIterator<V, T> operator-(
	const BArrayIterator<V, T> &i,
	typename BArrayIterator<V, T>::difference_type off)
{
	return (BArrayIterator<V, T>(i) -= off);
}

template<typename V1, typename V2, typename T>
inline typename BArrayIterator<V1, T>::difference_type operator-(
	const BArrayIterator<V1, T> &i1, const BArrayIterator<V2, T> &i2)
{
	assert ( i1.getBlockSize() == i2.getBlockSize() );

	if (i1.blockItr_ != i2.blockItr_) {
		return (i1.blockItr_ - i2.blockItr_) *  i1.getBlockSize() +
			(i1.cur_ - i1.blockBegin_) - (i2.cur_ - i2.blockBegin_);
	}

	return i1.cur_ - i2.cur_;
}


template<typename T, typename Alloc>
BArray<T, Alloc>::BArray(const Alloc &alloc) :
	tail_(NULL),
	blockEnd_(NULL),
	blockBits_(BArrayOption::DEFAULT_BLOCK_BITS),
	blockTable_(alloc),
	blockItr_( blockTable_.begin() )
{
}

template<typename T, typename Alloc>
BArray<T, Alloc>::BArray(const BArrayOption &option, const Alloc &alloc) :
	tail_(NULL),
	blockEnd_(NULL),
	blockBits_(option.getBlockBits()),
	blockTable_(alloc),
	blockItr_( blockTable_.begin() )
{
	try {
		reserve(option.getInitialCapacity());
	}
	catch (...) {
		destroy();
	}
}

template<typename T, typename Alloc>
template<typename Iter>
BArray<T, Alloc>::BArray(Iter first, Iter last, const Alloc &alloc) :
	tail_(NULL),
	blockEnd_(NULL),
	blockBits_(BArrayOption::DEFAULT_BLOCK_BITS),
	blockTable_(alloc),
	blockItr_( blockTable_.begin() )
{
	try {
		assign(first, last);
	}
	catch (...) {
		destroy();
	}
}

template<typename T, typename Alloc>
template<typename Iter>
BArray<T, Alloc>::BArray(
	Iter first, Iter last, const BArrayOption &option, const Alloc &alloc) :
	tail_(NULL),
	blockEnd_(NULL),
	blockBits_(option.getBlockBits()),
	blockTable_(alloc),
	blockItr_( blockTable_.begin() )
{
	try {
		reserve(option.getInitialCapacity());
		assign(first, last);
	}
	catch (...) {
		destroy();
	}
}

template<typename T, typename Alloc>
BArray<T, Alloc>::~BArray() {
	destroy();
}

template<typename T, typename Alloc>
void BArray<T, Alloc>::destroy() {
	for (typename BlockTable::iterator it = blockTable_.begin();
		it != blockTable_.end(); ++it)
	{
		get_allocator().deallocate(*it, 1U << blockBits_);
	}
	blockTable_.clear();
}

template<typename T, typename Alloc>
template<typename Iter>
void BArray<T, Alloc>::assign(Iter first, Iter last) {
	clear();
	for (; first != last; ++first) {
		push_back(*first);
	}
}

template<typename T, typename Alloc>
void BArray<T, Alloc>::assign(size_t count, const T &value) {
	clear();
	reserve(count);
	for (; count > 0; --count) {
		push_back(value);
	}
}

template<typename T, typename Alloc>
void BArray<T, Alloc>::clear() {
	tail_ = NULL;
	blockEnd_ = NULL;
	blockItr_ = blockTable_.begin();
}

template<typename T, typename Alloc>
void BArray<T, Alloc>::swap(BArray<T, Alloc> &another) {
	if (this == &another) {
		return;
	}
	else if ( get_allocator() == another.get_allocator() ) {
		std::swap(tail_, another.tail_);
		std::swap(blockEnd_, another.blockEnd_);
		std::swap(blockBits_, another.blockBits_);

		typedef typename BlockTable::iterator::difference_type DiffType;
		const DiffType orgThisBlockPos = blockItr_ - blockTable_.begin();
		const DiffType orgAnotherBlockPos =
			another.blockItr_ - another.blockTable_.begin();

		blockTable_.swap(another.blockTable_);
		blockItr_ = blockTable_.begin() + orgAnotherBlockPos;
		another.blockItr_ = another.blockTable_.begin() + orgThisBlockPos;
	}
	else {
		BArray<T, Alloc> temp( begin(), end(), get_allocator() );
		assign( another.begin(), another.end() );
		assign( temp.begin(), temp.end() );
	}
}

template<typename T, typename Alloc>
void BArray<T, Alloc>::resize(size_t count) {
	reserve(count);
	if (count == 0) {
		blockItr_ = blockTable_.begin();
		tail_ = blockEnd_ = NULL;
		return;
	}
	blockItr_ = blockTable_.begin() + ((count - 1) >> blockBits_);
	tail_ = *blockItr_ + ((count - 1) & ((1 << blockBits_) - 1)) + 1;
	blockEnd_ = *blockItr_ + (static_cast<size_t>(1) << blockBits_);
}

template<typename T, typename Alloc>
void BArray<T, Alloc>::resize(size_t count, T value) {
	const size_t orgCount = size();
	resize(count);

	if (orgCount < count) {
		std::fill(begin() + orgCount, end(), value);
	}
}

template<typename T, typename Alloc>
size_t BArray<T, Alloc>::size() const {
	if ( empty() ) {
		return 0;
	}

	return ((&blockItr_[0] - &blockTable_[0]) << blockBits_) +
		(tail_ - *blockItr_);
}

template<typename T, typename Alloc>
bool BArray<T, Alloc>::empty() const {
	return (tail_ == NULL);
}

template<typename T, typename Alloc>
void BArray<T, Alloc>::reserve(size_t count) {
	const size_t blockSize = static_cast<size_t>(1) << blockBits_;
	const size_t requiredBlocks = (count + blockSize - 1) >> blockBits_;
	ptrdiff_t rest = static_cast<ptrdiff_t>(requiredBlocks) - blockTable_.size();
	if (rest <= 0) {
		return;
	}

	const ptrdiff_t blockIndex = blockItr_ - blockTable_.begin();
	blockTable_.reserve(requiredBlocks + 1);
	for (; rest > 0; rest--) {
		blockTable_.push_back(get_allocator().allocate(1U << blockBits_));
	}
	blockItr_ = blockTable_.begin() + blockIndex;
}

template<typename T, typename Alloc>
size_t BArray<T, Alloc>::capacity() const {
	return blockTable_.size() << blockBits_;
}

template<typename T, typename Alloc>
typename BArray<T, Alloc>::iterator BArray<T, Alloc>::begin() {
	if ( empty() ) {
		return iterator(NULL, NULL, NULL, blockItr_);
	}

	T *pos = *blockTable_.begin();
	return iterator( pos, pos,
			pos + (static_cast<size_t>(1) << blockBits_), blockTable_.begin() );
}

template<typename T, typename Alloc>
typename BArray<T, Alloc>::iterator BArray<T, Alloc>::end() {
	if (tail_ == blockEnd_) {
		if (tail_ == NULL) {
			return iterator( NULL, NULL, NULL, blockTable_.end() );
		}
		else {
			typename BlockTable::iterator it = blockItr_ + 1;
			return iterator(*it, *it, *it + (static_cast<size_t>(1) << blockBits_), it);
		}
	}

	return iterator(tail_, *blockItr_, blockEnd_, blockItr_);
}

template<typename T, typename Alloc>
typename BArray<T, Alloc>::const_iterator BArray<T, Alloc>::begin() const {
	if ( empty() ) {
		return const_iterator( NULL, NULL, NULL, blockTable_.begin() );
	}

	T *pos = *blockTable_.begin();
	return const_iterator(
		pos, pos, pos + (static_cast<size_t>(1) << blockBits_), blockTable_.begin() );
}

template<typename T, typename Alloc>
typename BArray<T, Alloc>::const_iterator BArray<T, Alloc>::end() const {
	if (tail_ == blockEnd_) {
		if (tail_ == NULL) {
			return const_iterator( NULL, NULL, NULL, blockTable_.end() );
		}
		else {
			typename BlockTable::const_iterator it = blockItr_ + 1;
			return iterator(*it, *it, *it + (static_cast<size_t>(1) << blockBits_), it);
		}
	}

	return const_iterator(tail_, *blockItr_, blockEnd_, blockItr_);
}

template<typename T, typename Alloc>
T& BArray<T, Alloc>::front() {
	assert ( !empty() );
	return **blockTable_.begin();
}

template<typename T, typename Alloc>
const T& BArray<T, Alloc>::front() const {
	assert ( !empty() );
	return **blockTable_.begin();
}

template<typename T, typename Alloc>
T& BArray<T, Alloc>::back() {
	assert ( !empty() );
	if (tail_ - 1 == blockEnd_ - (static_cast<size_t>(1) << blockBits_)) {
		return *(--end());
	}
	return *(tail_ - 1);
}

template<typename T, typename Alloc>
const T& BArray<T, Alloc>::back() const {
	assert ( !empty() );
	if (tail_ - 1 == blockEnd_ - (static_cast<size_t>(1) << blockBits_)) {
		return *(--end());
	}
	return *(tail_ - 1);
}

template<typename T, typename Alloc>
inline void BArray<T, Alloc>::push_back(const T &value) {
	if (tail_ == blockEnd_) {
		reserve(size() + 1);
		if ( !empty() ) {
			blockItr_++;
		}
		tail_ = *blockItr_;
		blockEnd_ = tail_ + (static_cast<size_t>(1) << blockBits_);
	}

	*tail_ = value;
	tail_++;
}

template<typename T, typename Alloc>
inline void BArray<T, Alloc>::push_back(const T *values, size_t count) {
	const T *endIt = values + count;
	for (const T *it = values; it != endIt; ++it) {
		push_back(*it);
	}
}

template<typename T, typename Alloc>
inline void BArray<T, Alloc>::pop_back() {
	assert ( !empty() );
	if ( --tail_ == blockEnd_ - (static_cast<size_t>(1) << blockBits_) ) {
		if ( blockItr_ == blockTable_.begin() ) {
			tail_ = blockEnd_ = NULL;
		}
		else {
			blockItr_--;
			tail_ = blockEnd_ = *blockItr_ + (static_cast<size_t>(1) << blockBits_);
		}
	}
}

template<typename T, typename Alloc>
inline T& BArray<T, Alloc>::operator[](size_t index) {
	assert ( index < size() );
	const size_t indexMask = (static_cast<size_t>(1) << blockBits_) - 1;
	return blockTable_[index >> blockBits_][index & indexMask];
}

template<typename T, typename Alloc>
inline const T& BArray<T, Alloc>::operator[](size_t index) const {
	assert ( index < size() );
	const size_t indexMask = (static_cast<size_t>(1) << blockBits_) - 1;
	return blockTable_[index >> blockBits_][index & indexMask];
}

template<typename T, typename Alloc>
typename BArray<T, Alloc>::iterator BArray<T, Alloc>::insert(
	iterator pos, const T &value)
{
	assert ( begin() <= pos && pos <= end() );

	const size_t orgSize = size();
	const ptrdiff_t index = pos - begin();
	if (tail_ == blockEnd_) {
		resize(orgSize + 1);
		pos = begin() + index;
	}
	else {
		resize(orgSize + 1);
	}

	iterator destIt = end() - 1;
	{
		size_t i = orgSize - index;
		if (i > 0) {
			iterator srcIt = destIt - 1;
			for (;;) {
				assert (srcIt >= pos);
				*destIt = *srcIt;
				--destIt;
				if (--i <= 0) {
					break;
				}
				--srcIt;
			}
		}
	}

	*destIt = value;

	return destIt;
}

template<typename T, typename Alloc>
template<typename Iter>
void BArray<T, Alloc>::insert(iterator pos, Iter first, Iter last) {
	assert ( begin() <= pos && pos <= end() );

	size_t count = 0;
	for (Iter it = first; it != last; ++it) {
		count++;
	}

	const size_t index = pos - begin();
	const size_t orgSize = size();
	resize(orgSize + count);
	pos = begin() + index;

	if (count > 0) {
		size_t i = orgSize - index;
		if (i > 0) {
			iterator destIt = end() - 1;
			iterator srcIt = destIt - count;
			for (;;) {
				assert (srcIt >= pos);
				*destIt = *srcIt;
				if (--i <= 0) {
					break;
				}
				--destIt;
				--srcIt;
			}
		}
	}

	for (Iter it = first; it != last; ++pos, ++it) {
		*pos = *it;
	}
}

template<typename T, typename Alloc>
typename BArray<T, Alloc>::iterator BArray<T, Alloc>::erase(
	iterator pos)
{
	assert ( !empty() );
	assert ( begin() <= pos && pos < end() );

	iterator destIt = pos;
	for (iterator srcIt = destIt; ++srcIt != end(); ++destIt) {
		*destIt = *srcIt;
	}

	resize(size() - 1);
	return (empty() ? end() : pos);
}

template<typename T, typename Alloc>
typename BArray<T, Alloc>::iterator BArray<T, Alloc>::erase(
	iterator first, iterator last)
{
	assert ( !empty() );
	assert (first <= last);
	assert ( begin() <= first && last <= end() );

	size_t count = 0;
	for (iterator it = first; it != last; ++it) {
		count++;
	}

	iterator destIt = first;
	for (iterator srcIt = destIt + count; srcIt != end(); ++destIt, ++srcIt) {
		*destIt = *srcIt;
	}

	const ptrdiff_t index = first - begin();
	resize(size() - count);
	return (begin() + index);
}

template<typename K, typename V>
WeakMap<K, V>::WeakMap() {
}

template<typename K, typename V>
WeakMap<K, V>::~WeakMap() {
	for (typename std::map<K, Entry<V>*>::iterator it = base_.begin();
			it != base_.end(); ++it) {
		delete it->second;
	}
	base_.clear();
}

template<typename K, typename V>
bool WeakMap<K, V>::isEmpty() {
	LockGuard<Mutex> lock(mutex_);
	return base_.empty();
}

template<typename K, typename V>
V& WeakMap<K, V>::acquire(const K &key) {
	LockGuard<Mutex> lock(mutex_);

	typename BaseType::iterator it = base_.find(key);
	EntryType *entry = NULL;
	if ( it == base_.end() ) {
		UTIL_UNIQUE_PTR<EntryType> entryPtr(UTIL_NEW EntryType);
		base_.insert(std::make_pair( key, entryPtr.get() ));
		entry = entryPtr.get();
		entryPtr.release();
	}
	else {
		entry = it->second;
	}
	entry->referenceCount_++;

	return entry->value_;
}

template<typename K, typename V>
void WeakMap<K, V>::release(const K &key) {
	LockGuard<Mutex> lock(mutex_);

	typename BaseType::iterator it = base_.find(key);
	assert ( it != base_.end() );

	EntryType *entry = it->second;
	assert (entry->referenceCount_ > 0);
	if (--entry->referenceCount_ == 0) {
		delete entry;
		base_.erase(it);
	}
}

template<typename K, typename V>
WeakMapReference<K, V>::WeakMapReference(
		WeakMap<K, V> &weakMap, const K &key ) :
		weakMap_(&weakMap),
		key_(key),
		value_( &weakMap.acquire(key) ) {
}

template<typename K, typename V>
WeakMapReference<K, V>::~WeakMapReference() {
	try {
		if (weakMap_ != NULL) {
			weakMap_->release(key_);
		}
	}
	catch (...) {
	}
}

template<typename K, typename V>
V* WeakMapReference<K, V>::get() {
	return value_;
}

template<typename K, typename V>
void WeakMapReference<K, V>::unmanage() {
	weakMap_ = NULL;
	value_ = NULL;
}


namespace detail {


template<typename T, typename Alloc>
BlockingQueue<T, Alloc>::BlockingQueue(const Alloc &alloc) :
		base_(alloc),
		errorHandler_(NULL),
		closed_(false) {
}

template<typename T, typename Alloc>
BlockingQueue<T, Alloc>::~BlockingQueue() {
	close();
}

template<typename T, typename Alloc>
void BlockingQueue<T, Alloc>::push(AllocUniquePtr<T> &ptr) {
	LockGuard<Condition> guard(cond_);
	if (closed_) {
		ConcurrentQueueUtils::errorQueueClosed(errorHandler_);
		return;
	}

	cond_.signal();
	base_.push_back(Entry(ptr.get(), ptr.get_deleter()));
	ptr.release();
}

template<typename T, typename Alloc>
bool BlockingQueue<T, Alloc>::poll(AllocUniquePtr<T> &ptr) {
	LockGuard<Condition> guard(cond_);
	if (base_.empty()) {
		return false;
	}

	const Entry &entry = base_.front();
	base_.pop_front();
	ptr = AllocUniquePtr<T>::of(entry.value_, entry.deteter_);
	return true;
}

template<typename T, typename Alloc>
void BlockingQueue<T, Alloc>::wait() {
	LockGuard<Condition> guard(cond_);
	while (base_.empty() && !closed_) {
		cond_.wait();
	}
}

template<typename T, typename Alloc>
bool BlockingQueue<T, Alloc>::isClosed() {
	LockGuard<Condition> guard(cond_);
	return closed_;
}

template<typename T, typename Alloc>
void BlockingQueue<T, Alloc>::close() {
	LockGuard<Condition> guard(cond_);
	cond_.signal();

	closed_ = true;
	while (!base_.empty()) {
		const Entry &entry = base_.front();

		AllocUniquePtr<T> ptr(entry.value_, entry.deteter_);
		ptr.reset();

		base_.pop_front();
	}
}

template<typename T, typename Alloc>
void BlockingQueue<T, Alloc>::setErrorHandler(ErrorHandler handler) {
	errorHandler_ = handler;
}


template<typename T, typename Alloc>
BlockingQueue<T, Alloc>::Entry::Entry(
		T *value, const AllocDefaultDelete<T> &deteter) :
		value_(value),
		deteter_(deteter) {
}


template<typename T>
FutureBase<T>::FutureBase(FuturePool &pool) :
		data_(ALLOC_NEW(pool.getAllocator()) Data(pool)),
		forConsumer_(false) {
}

template<typename T>
FutureBase<T>::~FutureBase() {
	reset();
}

template<typename T>
FutureBase<T>::FutureBase(const FutureBase &another) :
		data_(NULL),
		forConsumer_(false) {
	*this = another;
}

template<typename T>
FutureBase<T>& FutureBase<T>::operator=(const FutureBase &another) {
	if (this == &another) {
		return *this;
	}
	reset();

	if (another.data_ != NULL) {
		data_ = another.data_;
		forConsumer_ = true;
		++data_->refCount_;
		++data_->consumerCount_;
	}
	return *this;
}

template<typename T>
T* FutureBase<T>::poll() {
	Data &data = resolveData();

	LockGuard<Condition> guard(*data.cond_);
	if (data.retrieved_) {
		FutureBaseUtils::errorRetrieved();
	}
	else if (data.cancelled_) {
		FutureBaseUtils::errorCancelled();
	}
	else if (data.exception_.get() != NULL) {
		data.retrieved_ = true;
		FutureBaseUtils::raiseExceptionResult(*data.exception_);
	}
	else if (data.value_.get() != NULL) {
		data.retrieved_ = true;
		return data.value_.get();
	}
	return NULL;
}

template<typename T>
void FutureBase<T>::waitFor(uint32_t timeoutMillis) {
	Data &data = resolveData();

	Stopwatch watch;
	watch.start();

	LockGuard<Condition> guard(*data.cond_);
	while (isResultAcceptable(data, guard)) {
		const uint32_t elapsedMillis = watch.elapsedMillis();
		if (elapsedMillis >= timeoutMillis) {
			break;
		}
		data.cond_->wait(timeoutMillis - elapsedMillis);
	}
}

template<typename T>
void FutureBase<T>::setValue(const T &value) {
	Data &data = resolveData();

	LockGuard<Condition> guard(*data.cond_);
	if (!isResultAcceptable(data, guard)) {
		return;
	}

	data.cond_->signal();
	try {
		data.value_ = UTIL_MAKE_LOCAL_UNIQUE(data.value_, T, value);
	}
	catch (...) {
		std::exception e;
		FutureBaseUtils::assignExceptionResult(e, data.exception_);
	}
}

template<typename T>
void FutureBase<T>::setException(std::exception &e) {
	Data &data = resolveData();

	LockGuard<Condition> guard(*data.cond_);
	if (!isResultAcceptable(data, guard)) {
		return;
	}

	data.cond_->signal();
	FutureBaseUtils::assignExceptionResult(e, data.exception_);
}

template<typename T>
bool FutureBase<T>::isCancelled() {
	Data &data = resolveData();

	LockGuard<Condition> guard(*data.cond_);
	return data.cancelled_;
}

template<typename T>
void FutureBase<T>::cancel() throw() {
	if (data_ == NULL) {
		return;
	}

	LockGuard<Condition> guard(*data_->cond_);
	cancelInternal(*data_, guard);
}

template<typename T>
void FutureBase<T>::reset() throw() {
	if (data_ == NULL) {
		return;
	}

	Data *const data = data_;
	const bool forConsumer = forConsumer_;

	data_ = NULL;
	forConsumer_ = false;

	assert(data->refCount_ > 0);
	const bool noRef = (--data->refCount_ == 0);

	if (forConsumer) {
		assert(data->consumerCount_ > 0);
		if (--data->consumerCount_ == 0) {
			LockGuard<Condition> guard(*data->cond_);
			cancelInternal(*data, guard);
		}
	}

	if (noRef) {
		ALLOC_DELETE(data->pool_.getAllocator(), data);
	}
}

template<typename T>
typename FutureBase<T>::Data& FutureBase<T>::resolveData() {
	if (data_ == NULL) {
		FutureBaseUtils::errorNotAssigned();
	}

	return *data_;
}

template<typename T>
void FutureBase<T>::cancelInternal(
		Data &data, const LockGuard<Condition> &gurad) throw() {
	if (!isResultAcceptable(data, gurad)) {
		return;
	}
	data.cond_->signal();
	data.cancelled_ = true;
}

template<typename T>
bool FutureBase<T>::isResultAcceptable(
		Data &data, const LockGuard<Condition>&) throw() {
	return (!data.cancelled_ &&
			data.exception_.get() == NULL &&
			data.value_.get() == NULL);
}


template<typename T>
FutureBase<T>::Data::Data(FuturePool &pool) :
		refCount_(1),
		consumerCount_(0),
		pool_(pool),
		cond_(pool.getConditionPool().allocate()),
		cancelled_(false),
		retrieved_(false) {
}

template<typename T>
FutureBase<T>::Data::~Data() {
	assert(refCount_ == 0);
	pool_.getConditionPool().deallocate(cond_);
}


template<typename C>
BasicTask<C>::BasicTask(FuturePool &pool, const C &command) :
		future_(pool),
		command_(command) {
}

template<typename C>
BasicTask<C>::~BasicTask() {
	future_.cancel();
}

template<typename C>
void BasicTask<C>::operator()() {
	try {
		if (future_.isCancelled()) {
			return;
		}
		future_.setValue(command_());
	}
	catch (...) {
		std::exception e;
		future_.setException(e);
	}
}

template<typename C>
typename BasicTask<C>::FutureType BasicTask<C>::getFuture() {
	return future_;
}

} 


template<typename T>
Future<T>::Future(const detail::FutureBase<T> &base) :
		base_(base) {
}

template<typename T>
T* Future<T>::poll() {
	return base_.poll();
}

template<typename T>
void Future<T>::waitFor(uint32_t timeoutMillis) {
	return base_.waitFor(timeoutMillis);
}


template<typename C>
typename ExecutorService::template FutureOf<C>::Type
ExecutorService::submit(const C &command) {
	typedef detail::BasicTask<C> TaskType;

	detail::FuturePool &pool = getFuturePool();
	StdAllocator<void, void> &alloc = pool.getAllocator();

	AllocUniquePtr<TaskType> task(
			ALLOC_UNIQUE(alloc, TaskType, pool, command));
	typename FutureOf<C>::Type future(task->getFuture());

	AllocUniquePtr<detail::TaskBase> taskBase(task.release(), alloc);
	submitTask(taskBase);

	return future;
}


template<typename C, typename Alloc>
BatchCommand<C, Alloc>::BatchCommand(const Alloc &alloc) :
		commandList_(alloc) {
}

template<typename C, typename Alloc>
void BatchCommand<C, Alloc>::add(const C &command) {
	commandList_.push_back(command);
}

template<typename C, typename Alloc>
template<typename It>
void BatchCommand<C, Alloc>::addAll(It begin, It end) {
	commandList_.insert(commandList_.end(), begin, end);
}

template<typename C, typename Alloc>
void BatchCommand<C, Alloc>::clear() {
	commandList_.clear();
}

template<typename C, typename Alloc>
typename BatchCommand<C, Alloc>::CommandIterator
BatchCommand<C, Alloc>::begin() const {
	return commandList_.begin();
}

template<typename C, typename Alloc>
typename BatchCommand<C, Alloc>::CommandIterator
BatchCommand<C, Alloc>::end() const {
	return commandList_.end();
}

template<typename C, typename Alloc>
typename BatchCommand<C, Alloc>::ResultType
BatchCommand<C, Alloc>::operator()() {
	ResultType result(commandList_.get_allocator());

	for (CommandIterator it = commandList_.begin();
			it != commandList_.end(); ++it) {
		result.push_back((*it)());
	}

	return result;
}



template<typename C>
InsertionResetter::InsertionResetter(C &container) :
		entry_(createEntry(container, container.end())) {
}

template<typename C>
InsertionResetter::InsertionResetter(
		C &container, typename C::const_iterator insertionPoint) :
		entry_(createEntry(container, insertionPoint)) {
}

template<typename C>
InsertionResetter::Entry InsertionResetter::createEntry(
		C &container, typename C::const_iterator insertionPoint) {
	const ptrdiff_t distance = insertionPoint - container.begin();
	return Entry(&resetSpecific<C>, &container, static_cast<size_t>(distance));
}

template<typename C>
void InsertionResetter::resetSpecific(void *container, size_t pos) {
	C *containerObj = static_cast<C*>(container);

	if (pos >= containerObj->size()) {
		assert(false);
		return;
	}

	containerObj->erase(containerObj->begin() + pos);
}



template<typename Alloc>
inline XArrayOutStream<Alloc>::XArrayOutStream(
		XArray<uint8_t, Alloc> &buffer) : buffer_(buffer) {
}

template<typename Alloc>
inline size_t XArrayOutStream<Alloc>::write(const void *buf, size_t length) {
	writeAll(buf, length);
	return length;
}

template<typename Alloc>
inline void XArrayOutStream<Alloc>::writeAll(const void *buf, size_t length) {
	const size_t lastSize = buffer_.size();
	buffer_.resize(lastSize + length);
	memcpy(&buffer_[0] + lastSize, buf, length);
}


template<typename Base>
inline BufferedOutStream<Base>::BufferedOutStream(
		Base &base, NormalXArray<uint8_t> &buffer, size_t limitSize) :
		base_(base), buffer_(buffer), limit_(limitSize) {
	buffer_.reserve(limit_);
}

template<typename Base>
inline BufferedOutStream<Base>::~BufferedOutStream() {
	if ( !buffer_.empty() ) {
		try {
			flush();
		}
		catch (...) {
		}
	}
}

template<typename Base>
inline size_t BufferedOutStream<Base>::write(
		const void *buf, size_t length ) {
	size_t lastSize = buffer_.size();
	if (limit_ > 0 && lastSize >= limit_) {
		flush();
		lastSize = 0;
	}

	const size_t consumed =
		( limit_ == 0 ? length : std::min(length, limit_ - lastSize) );
	buffer_.resize(lastSize + consumed);
	memcpy(&buffer_[0] + lastSize, buf, consumed);

	return consumed;
}

template<typename Base>
inline void BufferedOutStream<Base>::flush() {
	try {
		base_.writeAll( buffer_.data(), buffer_.size() );
		base_.flush();
		buffer_.clear();
	}
	catch (...) {
		this->setError();
		throw;
	}
}

} 

#endif
