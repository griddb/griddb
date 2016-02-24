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



#define UTIL_DISABLE_CONTAINER_DEFAULT_CONSTRUCTOR 1

namespace util {

namespace detail {
struct ObjectPoolUtils;
}

/*!
	@brief Manages same type object in the pool
*/



template<typename T, typename Mutex = NoopMutex>
class ObjectPool {
	friend struct detail::ObjectPoolUtils;

public:
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

private:
	struct FreeLink {
		FreeLink *next_;
	};

	static const size_t ELEMENT_OFFSET =
			detail::AlignedSizeOf<FreeLink>::VALUE;

	ObjectPool(const ObjectPool&);
	ObjectPool& operator=(const ObjectPool&);

	void* allocateDirect();
	void deallocateDirect(void *rawElement);

	void clear(size_t preservedCount);

	inline static void* elementOf(FreeLink *entry) {
		return reinterpret_cast<uint8_t*>(entry) + ELEMENT_OFFSET;
	}

	inline static FreeLink* linkOf(void *entry) {
		return reinterpret_cast<FreeLink*>(
				static_cast<uint8_t*>(entry) - ELEMENT_OFFSET);
	}

	size_t freeElementLimit_;

	FreeLink *freeLink_;

	size_t freeElementCount_;

	BaseAllocator base_;
	AllocatorStats stats_;
};

namespace detail {
struct ObjectPoolUtils {
	template<typename T, typename Mutex>
	static ObjectPool<T, Mutex>& checkType(ObjectPool<T, Mutex> &pool) {
		return pool;
	}

	template<typename T, typename Mutex>
	static void* allocateDirect(ObjectPool<T, Mutex> &pool) {
		return pool.allocateDirect();
	}

	template<typename T, typename Mutex>
	static void deallocateDirect(
			ObjectPool<T, Mutex> &pool, void *rawElement) throw() {
		try {
			pool.deallocateDirect(rawElement);
		}
		catch (...) {
		}
	}
};
}	
}	

#define UTIL_OBJECT_POOL_NEW(pool) \
		new (util::detail::ObjectPoolUtils::checkType(pool))

#define UTIL_OBJECT_POOL_DELETE(pool, element) \
		util::detail::ObjectPoolUtils::checkType(pool).deallocate(element)

template<typename T, typename Mutex>
void* operator new(size_t size, util::ObjectPool<T, Mutex> &pool) {
	assert(size == sizeof(T));
	(void) size;

	return util::detail::ObjectPoolUtils::allocateDirect(pool);
}

template<typename T, typename Mutex>
void operator delete(void *p, util::ObjectPool<T, Mutex> &pool) throw() {
	return util::detail::ObjectPoolUtils::deallocateDirect(pool, p);
}

namespace util {

/*!
	@brief std::vector, using StackAllocator in default
*/



template< typename T, typename Alloc = StdAllocator<T, StackAllocator> >
class Vector : public std::vector<T, Alloc> {
private:
	typedef Vector<T, Alloc> ThisType;
	typedef typename std::vector<T, Alloc> BaseType;

public:
#if UTIL_DISABLE_CONTAINER_DEFAULT_CONSTRUCTOR
	explicit Vector(const Alloc &alloc) : BaseType(alloc) {
	}
#else
	explicit Vector(const Alloc &alloc = Alloc()) : BaseType(alloc) {
	}
#endif

	explicit Vector(size_t size, const T &value = T(), const Alloc &alloc = Alloc()) :
		BaseType(size, value, alloc)
	{
	}

	template<typename Iter>
	Vector(Iter first, Iter last, const Alloc &alloc = Alloc()) :
		BaseType(first, last, alloc)
	{
	}

	Vector(BaseType &another) : BaseType(another) {
	}

	ThisType& operator=(BaseType &another) {
		base() = another;
		return *this;
	}

private:
	const BaseType& base() const { return *this; };
	BaseType& base() { return *this; };
};

/*!
	@brief std::deque, using StackAllocator in default
*/



template< typename T, typename Alloc = StdAllocator<T, StackAllocator> >
class Deque : public std::deque<T, Alloc> {
private:
	typedef Deque<T, Alloc> ThisType;
	typedef typename std::deque<T, Alloc> BaseType;

public:
#if UTIL_DISABLE_CONTAINER_DEFAULT_CONSTRUCTOR
	explicit Deque(const Alloc &alloc) : BaseType(alloc) {
	}
#else
	explicit Deque(const Alloc &alloc = Alloc()) : BaseType(alloc) {
	}
#endif

	explicit Deque(size_t size, const T &value = T(), const Alloc &alloc = Alloc()) :
		BaseType(size, value, alloc)
	{
	}

	Deque(BaseType &another) : BaseType(another) {
	}

	ThisType& operator=(BaseType &another) {
		base() = another;
		return *this;
	}

private:
	const BaseType& base() const { return *this; };
	BaseType& base() { return *this; };
};

/*!
	@brief std::map, using StackAllocator in default
*/



template< typename K, typename V, typename Comp = std::less<K>,
	typename Alloc = StdAllocator<std::pair<K, V>, StackAllocator> >
class Map : public std::map<K, V, Comp, Alloc> {
private:
	typedef Map<K, V, Comp, Alloc> ThisType;
	typedef typename std::map<K, V, Comp, Alloc> BaseType;

public:
	explicit Map(const Alloc &alloc) : BaseType(Comp(), alloc) {
	}

#if UTIL_DISABLE_CONTAINER_DEFAULT_CONSTRUCTOR
	explicit Map(const Comp &comp, const Alloc &alloc) :
		BaseType(comp, alloc)
	{
	}
#else
	explicit Map(const Comp &comp = Comp(), const Alloc &alloc = Alloc()) :
		BaseType(comp, alloc)
	{
	}
#endif

	template<typename Iter>
	Map(Iter first, Iter last, const Alloc &alloc) :
		BaseType(first, last, Comp(), alloc)
	{
	}

	template<typename Iter>
	Map(Iter first, Iter last,
		const Comp &comp = Comp(), const Alloc &alloc = Alloc()) :
		BaseType(first, last, comp, alloc)
	{
	}

	Map(BaseType &another) : BaseType(another) {
	}

	ThisType& operator=(BaseType &another) {
		base() = another;
		return *this;
	}

private:
	const BaseType& base() const { return *this; };
	BaseType& base() { return *this; };
};

/*!
	@brief std::multimup, using StackAllocator in default
*/



template< typename K, typename V, typename Comp = std::less<K>,
	typename Alloc = StdAllocator<std::pair<K, V>, StackAllocator> >
class MultiMap : public std::multimap<K, V, Comp, Alloc> {
private:
	typedef MultiMap<K, V, Comp, Alloc> ThisType;
	typedef typename std::multimap<K, V, Comp, Alloc> BaseType;

public:
	explicit MultiMap(const Alloc &alloc) : BaseType(Comp(), alloc) {
	}

#if UTIL_DISABLE_CONTAINER_DEFAULT_CONSTRUCTOR
	explicit MultiMap(const Comp &comp, const Alloc &alloc) :
		BaseType(comp, alloc)
	{
	}
#else
	explicit MultiMap(const Comp &comp = Comp(), const Alloc &alloc = Alloc()) :
		BaseType(comp, alloc)
	{
	}
#endif

	template<typename Iter>
	MultiMap(Iter first, Iter last, const Alloc &alloc) :
		BaseType(first, last, Comp(), alloc)
	{
	}

	template<typename Iter>
	MultiMap(Iter first, Iter last,
		const Comp &comp = Comp(), const Alloc &alloc = Alloc()) :
		BaseType(first, last, comp, alloc)
	{
	}

	MultiMap(BaseType &another) : BaseType(another) {
	}

	ThisType& operator=(BaseType &another) {
		base() = another;
		return *this;
	}

private:
	const BaseType& base() const { return *this; };
	BaseType& base() { return *this; };
};

/*!
	@brief std::set, using StackAllocator in default
*/



template< typename T, typename Comp = std::less<T>,
	typename Alloc = StdAllocator<T, StackAllocator> >
class Set : public std::set<T, Comp, Alloc> {
private:
	typedef Set<T, Comp, Alloc> ThisType;
	typedef typename std::set<T, Comp, Alloc> BaseType;

public:
	explicit Set(const Alloc &alloc) : BaseType(Comp(), alloc) {
	}

#if UTIL_DISABLE_CONTAINER_DEFAULT_CONSTRUCTOR
	explicit Set(const Comp &comp, const Alloc &alloc) :
		BaseType(comp, alloc)
	{
	}
#else
	explicit Set(const Comp &comp = Comp(), const Alloc &alloc = Alloc()) :
		BaseType(comp, alloc)
	{
	}
#endif

	template<typename Iter>
	Set(Iter first, Iter last, const Alloc &alloc) :
		BaseType(first, last, Comp(), alloc)
	{
	}

	template<typename Iter>
	Set(Iter first, Iter last,
		const Comp &comp = Comp(), const Alloc &alloc = Alloc()) :
		BaseType(first, last, comp, alloc)
	{
	}

	Set(BaseType &another) : BaseType(another) {
	}

	ThisType& operator=(BaseType &another) {
		base() = another;
		return *this;
	}

private:
	const BaseType& base() const { return *this; };
	BaseType& base() { return *this; };
};

/*!
	@brief std::multiset, using StackAllocator in default
*/



template< typename T, typename Comp = std::less<T>,
	typename Alloc = StdAllocator<T, StackAllocator> >
class MultiSet : public std::multiset<T, Comp, Alloc> {
private:
	typedef MultiSet<T, Comp, Alloc> ThisType;
	typedef typename std::multiset<T, Comp, Alloc> BaseType;

public:
	explicit MultiSet(const Alloc &alloc) : BaseType(Comp(), alloc) {
	}

#if UTIL_DISABLE_CONTAINER_DEFAULT_CONSTRUCTOR
	explicit MultiSet(const Comp &comp, const Alloc &alloc) :
		BaseType(comp, alloc)
	{
	}
#else
	explicit MultiSet(const Comp &comp = Comp(), const Alloc &alloc = Alloc()) :
		BaseType(comp, alloc)
	{
	}
#endif

	template<typename Iter>
	MultiSet(Iter first, Iter last, const Alloc &alloc) :
		BaseType(first, last, Comp(), alloc)
	{
	}

	template<typename Iter>
	MultiSet(Iter first, Iter last,
		const Comp &comp = Comp(), const Alloc &alloc = Alloc()) :
		BaseType(first, last, comp, alloc)
	{
	}

	MultiSet(BaseType &another) : BaseType(another) {
	}

	ThisType& operator=(BaseType &another) {
		base() = another;
		return *this;
	}

private:
	const BaseType& base() const { return *this; };
	BaseType& base() { return *this; };
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

	
	
	

#if UTIL_DISABLE_CONTAINER_DEFAULT_CONSTRUCTOR
	explicit XArray(const Alloc &alloc);
#else
	explicit XArray(const Alloc &alloc = Alloc());
#endif

	
	
	

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
	typedef XArray< T, std::allocator<T> > BaseType;

public:
	typedef typename BaseType::allocator_type allocator_type;

	explicit NormalXArray(
			const allocator_type& alloc = allocator_type()) : BaseType(alloc) {
	}

	template<typename Iter>
	NormalXArray(
			Iter first, Iter last, const allocator_type& alloc = allocator_type()) :
			BaseType(first, last, alloc) {
	}
};

template<
		typename T,
		typename Mutex = typename VariableSizeAllocator<>::MutexType,
		typename Traits = typename VariableSizeAllocator<>::TraitsType>
class VarXArray : public XArray<
		T, StdAllocator< T, VariableSizeAllocator<Mutex, Traits> > > {
private:
	typedef XArray< T,
			StdAllocator< T, VariableSizeAllocator<Mutex, Traits> > > BaseType;

public:
	typedef typename BaseType::allocator_type allocator_type;

	explicit VarXArray(
			const allocator_type& alloc = allocator_type()) : BaseType(alloc) {
	}

	template<typename Iter>
	VarXArray(
			Iter first, Iter last, const allocator_type& alloc = allocator_type()) :
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




template< typename T, typename Comp = std::less<T>,
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



template< typename T, typename Comp = std::less<T> >
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
	struct Enrty {
		Enrty() : referenceCount_(0) {};

		Ev value_;
		size_t referenceCount_;
	};

	WeakMap(const WeakMap&);
	WeakMap& operator=(const WeakMap&);

	typedef Enrty<V> EntryType;
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


#if 0























template<typename T, typename Alloc>
class BlockingQueue {
public:






	explicit BlockingQueue(
		size_t initialCapacity = 0, size_t limit = 0, const Alloc &alloc = Alloc());
	~BlockingQueue();



	bool push(const T &src, bool tryPush = false, uint32_t timeoutMillisec = 0);



	bool pop(T &dest, bool tryPop = false, uint32_t timeoutMillisec = 0);

private:
	BlockingQueue(const BlockingQueue&);
	BlockingQueue& operator=(const BlockingQueue&);

	Deque<T, Alloc> base_;
	const size_t limit_;

	Mutex mutex_;
	Event popEvent_;
	Event pushEvent_;
};

template<typename V>
class LimitedFlatTrie {
public:
	LimitedFlatTrie(const std::set<V> &orgSet);
	~LimitedFlatTrie();
	bool find(const V value);

private:
	typedef int32_t Block;		
	static const size_t TABLE_BITS = 12;	
	static const size_t TABLE_SIZE = static_cast<size_t>(1) << TABLE_BITS;
	static const V TABLE_MASK = static_cast<V>(
			(static_cast<size_t>(1) << TABLE_BITS) - 1 );

	Block blockList_[TABLE_SIZE];
	bool conflict_;
	std::set<V> valueSet_;
};
#endif







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
		base_(info, ELEMENT_OFFSET + sizeof(T)),
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
		return new (elementOf(entry)) T();
	}

	return element;
}

template<typename T, typename Mutex>
void ObjectPool<T, Mutex>::deallocate(T *element) {
	if (element == NULL) {
		return;
	}

	FreeLink *entry = linkOf(element);

	if (getFreeElementCount() < freeElementLimit_) {
		entry->next_ = freeLink_;
		freeLink_ = entry;
		++freeElementCount_;
	}
	else {
		element->~T();
		base_.deallocate(entry);
	}
}

template<typename T, typename Mutex>
T* ObjectPool<T, Mutex>::poll() {
	FreeLink *cur = freeLink_;

	if (cur == NULL) {
		return NULL;
	}

	freeLink_ = cur->next_;
	cur->next_ = NULL;

	assert(freeElementCount_ > 0);
	--freeElementCount_;

	return static_cast<T*>(elementOf(cur));
}

template<typename T, typename Mutex>
void ObjectPool<T, Mutex>::setTotalElementLimit(size_t limit) {
	base_.setTotalElementLimit(limit);
}

template<typename T, typename Mutex>
void ObjectPool<T, Mutex>::setFreeElementLimit(size_t limit) {
	freeElementLimit_ = limit;
	clear(limit);
}

template<typename T, typename Mutex>
size_t ObjectPool<T, Mutex>::getTotalElementCount() {
	return base_.getTotalElementCount();
}

template<typename T, typename Mutex>
size_t ObjectPool<T, Mutex>::getFreeElementCount() {
	return freeElementCount_ + base_.getFreeElementCount();
}

template<typename T, typename Mutex>
typename ObjectPool<T, Mutex>::BaseAllocator&
ObjectPool<T, Mutex>::base() {
	return base_;
}

template<typename T, typename Mutex>
void ObjectPool<T, Mutex>::getStats(AllocatorStats &stats) {
	stats.merge(stats_);
	stats.values_[AllocatorStats::STAT_CACHE_SIZE] +=
			AllocatorStats::asStatValue(
					freeElementCount_ * base_.getTotalElementCount());
}

template<typename T, typename Mutex>
void ObjectPool<T, Mutex>::setLimit(AllocatorStats::Type type, size_t value) {
	switch (type) {
	case AllocatorStats::STAT_CACHE_LIMIT:
		setFreeElementLimit(value / base_.getTotalElementCount());
		break;
	default:
		break;
	}
}

template<typename T, typename Mutex>
void ObjectPool<T, Mutex>::setLimit(
		AllocatorStats::Type type, AllocatorLimitter *limitter) {
	(void) type;
	(void) limitter;
}

template<typename T, typename Mutex>
void* ObjectPool<T, Mutex>::allocateDirect() {
	FreeLink *cur = freeLink_;

	if (cur == NULL) {
		FreeLink *entry = static_cast<FreeLink*>(base_.allocate());

		entry->next_ = NULL;
		return elementOf(entry);
	}

	void *rawElement = elementOf(cur);
	static_cast<T*>(rawElement)->~T();

	freeLink_ = cur->next_;
	cur->next_ = NULL;

	assert(freeElementCount_ > 0);
	--freeElementCount_;

	return rawElement;
}

template<typename T, typename Mutex>
void ObjectPool<T, Mutex>::deallocateDirect(void *rawElement) {
	if (rawElement == NULL) {
		return;
	}

	FreeLink *entry = linkOf(rawElement);
	assert(entry->next_ == NULL);
	base_.deallocate(entry);
}

template<typename T, typename Mutex>
void ObjectPool<T, Mutex>::clear(size_t preservedCount) {
	for (FreeLink *cur = freeLink_; cur != NULL;) {
		if (preservedCount >= freeElementCount_) {
			break;
		}

		static_cast<T*>(elementOf(cur))->~T();

		FreeLink *next = cur->next_;
		base_.deallocate(cur);
		cur = next;

		freeLink_ = cur;
		freeElementCount_--;
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
	const size_t newCapacity = (1U << std::max<uint32_t>(
		MIN_CAPACITY_BIT,
		static_cast<uint32_t>(sizeof(uint32_t) * CHAR_BIT) -
		nlz(static_cast<uint32_t>(requestedCapacity - 1)) ));
	if (newCapacity < requestedCapacity) {
		UTIL_THROW_UTIL_ERROR(CODE_SIZE_LIMIT_EXCEEDED,
				"Too large array capacity requested (size=" <<
				requestedCapacity << ")");
	}

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
	for (typename std::map<K, Enrty<V>*>::iterator it = base_.begin();
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

#if 0






template<typename V>
LimitedFlatTrie<V>::LimitedFlatTrie(const std::set<V> &orgSet) :
		conflict_(false), valueSet_(orgSet) {

	std::fill( &blockList_[0], &blockList_[TABLE_SIZE],
		static_cast<Block>(false) );
	for ( typename std::set<V>::iterator it = valueSet_.begin();
			it != valueSet_.end(); ++it ) {
		const V value = *it;
		if ( (value & (~TABLE_MASK)) != 0 ) {
			conflict_ |= true;
		}

		Block &found = blockList_[value & TABLE_MASK];
		if (found) {
			conflict_ |= true;
		}
		else {
			found |= true;
		}
	}
}

template<typename V>
LimitedFlatTrie<V>::~LimitedFlatTrie() {
}

template<typename V>
inline bool LimitedFlatTrie<V>::find(const V value) {
	if (blockList_[value & TABLE_MASK]) {
		if (conflict_) {
			return ( valueSet_.find(value) != valueSet_.end() );
		}
		else {
			return true;
		}
	}
	return false;
}
#endif


#if 0






template<typename T, typename Alloc>
BlockingQueue<T, Alloc>::BlockingQueue(
	size_t initialCapacity, size_t limit, const Alloc &alloc) :
	base_(initialCapacity, T(), alloc),
	limit_(limit)
{
	base_.clear();
}

template<typename T, typename Alloc>
BlockingQueue<T, Alloc>::~BlockingQueue() {
}

template<typename T, typename Alloc>
bool BlockingQueue<T, Alloc>::push(
	const T &src, bool tryPush, uint32_t timeoutMillisec)
{
	WaitableTimer timer(tryPush, timeoutMillisec);

	for (;;) {
		
		{
			Lock lock(mutex_);

			if (limit_ == 0 || base_.size() < limit_) {		
				base_.push_back(src);		
				popEvent_.signal();		

				return true;
			}

			if (limit_ > 0) {
				pushEvent_.reset();
			}
		}

		
		if ( limit_ > 0 && !timer.wait(pushEvent_) ) {
			return false;
		}
	}
}

template<typename T, typename Alloc>
bool BlockingQueue<T, Alloc>::pop(
	T &dest, bool tryPop, uint32_t timeoutMillisec)
{
	WaitableTimer timer(tryPop, timeoutMillisec);

	for (;;) {
		
		{
			Lock lock(mutex_);

			if ( !base_.empty() ) {		
				dest = base_.front();		
				base_.pop_front();

				if (limit_ > 0) {
					pushEvent_.signal();		
				}

				return true;
			}

			popEvent_.reset();
		}

		
		if ( !timer.wait(popEvent_) ) {
			return false;
		}
	}
}
#endif











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
