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

#ifndef SQL_UTILS_ALGORITHM_H_
#define SQL_UTILS_ALGORITHM_H_

#include "util/container.h"

struct SQLAlgorithmUtils {
	struct SortConfig;

	template<typename It> class CombSorter;

	template<
			typename T, typename Pred, typename Alloc =
					util::StdAllocator<T, util::StackAllocator> > class Sorter;

	template<typename T> class HeapElement;
	template<typename T, typename Pred> class HeapPredicate;
	template<
			typename T, typename Pred, typename Alloc =
					util::StdAllocator<T, util::StackAllocator> > class HeapQueue;

	template<
			typename K, typename Hash, typename Pred,
			typename Alloc = util::StdAllocator<
					K, util::StackAllocator> > class TreeHashSet;
	template<
			typename K, typename T, typename Hash, typename Pred,
			typename Alloc = util::StdAllocator<
					std::pair<const K, T>, util::StackAllocator> > class TreeHashMap;
	template<
			typename K, typename T, typename Hash, typename Pred,
			typename Alloc = util::StdAllocator<
					std::pair<const K, T>, util::StackAllocator> > class TreeHashMultiMap;

	template<
			typename K, typename Hash, typename Pred,
			typename Alloc = util::StdAllocator<
					K, util::StackAllocator> > class HashSet;
	template<
			typename K, typename T, typename Hash, typename Pred,
			typename Alloc = util::StdAllocator<
					std::pair<const K, T>, util::StackAllocator> > class HashMap;
	template<
			typename K, typename T, typename Hash, typename Pred,
			typename Alloc = util::StdAllocator<
					std::pair<const K, T>, util::StackAllocator> > class HashMultiMap;

	template< typename Alloc = util::StdAllocator<int64_t, util::StackAllocator> >
	class DenseBitSet;
};

struct SQLAlgorithmUtils::SortConfig {
	SortConfig() :
			interval_(std::numeric_limits<ptrdiff_t>::max()),
			limit_(-1),
			unique_(false),
			orderedUnique_(false) {
	}

	ptrdiff_t interval_;
	int64_t limit_;
	bool unique_;
	bool orderedUnique_;
};

template<typename It>
class SQLAlgorithmUtils::CombSorter {
public:
	CombSorter(It begin, It end);

	void setConfig(const SortConfig &config);
	const SortConfig& getConfig() const;

	bool isSorted() const;

	template<typename Pred>
	bool run(const Pred &pred);

	template<typename Pred>
	bool runMain(const Pred &pred, It tmp, bool tmpAllowed, bool *resultOnTmp);

	template<typename Pred>
	ptrdiff_t runMainUnique(
			const Pred &pred, It tmp, bool tmpAllowed, bool *resultOnTmp);

	template<typename Pred, typename Action>
	ptrdiff_t runMainGroup(
			const Pred &pred, const Action &action,
			It tmp, bool tmpAllowed, bool *resultOnTmp);

	template<typename Pred>
	static void runSubSort(const Pred &pred, It begin, It end);

	template<typename Pred>
	static void runSubSortUnique(
			const Pred &pred, It begin, It end, uint32_t *dup);

	template<typename Pred>
	static void runSubMerge(
			const Pred &pred, It begin1, It end1, It begin2, It end2, It out);

	template<typename Pred>
	static It runSubMergeTopUnique(
			const Pred &pred, It begin1, It end1, It begin2, It end2,
			const uint32_t *dup1, const uint32_t *dup2, It out);

	template<typename Pred, typename Action>
	static It runSubMergeTopGroup(
			const Pred &pred, const Action &action,
			It begin1, It end1, It begin2, It end2,
			const uint32_t *dup1, const uint32_t *dup2, It out);

	template<typename Pred>
	static It runSubMergeMidUnique(
			const Pred &pred, It begin1, It end1, It begin2, It end2, It out);

	template<typename Pred, typename Action>
	static It runSubMergeMidGroup(
			const Pred &pred, const Action &action,
			It begin1, It end1, It begin2, It end2, It out);

	template<typename Pred>
	bool runLimited(const Pred &pred, int64_t &reduced);

private:
	enum Constants {
		SORT_SHRINK_RATE = 13,
		SORT_SHRINK_BASE = 10
	};

	It begin_;
	It end_;

	ptrdiff_t gap_;
	bool sorted_;

	It middle_;
	SortConfig config_;
};

template<typename T, typename Pred, typename Alloc>
class SQLAlgorithmUtils::Sorter {
public:
	class Sort {
	public:
		typedef bool RetType;

		template<typename U> class TypeAt {
		public:
			explicit TypeAt(const Sort &base) : base_(base) {}

			bool operator()() const {
				return base_.base_.template sortAt<U>();
			}

		private:
			const Sort &base_;
		};

		explicit Sort(Sorter &base) : base_(base) {}

	private:
		Sorter &base_;
	};

	class SortUnique {
	public:
		typedef bool RetType;

		template<typename U> class TypeAt {
		public:
			explicit TypeAt(const SortUnique &base) : base_(base) {}

			bool operator()() const {
				return base_.base_.template sortUniqueAt<U>();
			}

		private:
			const SortUnique &base_;
		};

		explicit SortUnique(Sorter &base) : base_(base) {}

	private:
		Sorter &base_;
	};

	class SortLimited {
	public:
		typedef bool RetType;

		template<typename U> class TypeAt {
		public:
			explicit TypeAt(const SortLimited &base) : base_(base) {}

			bool operator()() const {
				return base_.base_.template sortLimitedAt<U>();
			}

		private:
			const SortLimited &base_;
		};

		explicit SortLimited(Sorter &base) : base_(base) {}

	private:
		Sorter &base_;
	};

	template<typename Action>
	class SortGroup {
	public:
		typedef bool RetType;

		template<typename U> class TypeAt {
		public:
			explicit TypeAt(const SortGroup &base) : base_(base) {}

			bool operator()() const {
				return base_.base_.template sortGroupAt<U, Action>(base_.action_);
			}

		private:
			const SortGroup &base_;
		};

		SortGroup(Sorter &base, const Action &action) :
				base_(base), action_(action) {
		}

	private:
		Sorter &base_;
		const Action &action_;
	};

	typedef const T* Iterator;

	Sorter(
			size_t capacity, const Pred &pred1, const Pred &pred2,
			const std::pair<T*, T*> &buffer);

	void setConfig(const SortConfig &config);
	void setSecondaryPredicate(const Pred &pred);
	void setPredicateEmpty(bool primary, bool empty);

	void add(const T &elem);
	void addSecondary(const T &elem);

	void clear();

	bool isEmpty() const;
	bool isEmptyAt(bool primary) const;
	bool isEmptyPredicateAt(bool primary) const;

	bool isFilled() const;
	bool isSorted() const;

	size_t getRemaining() const;

	Iterator begin() const;
	Iterator end() const;

	Iterator beginAt(bool primary) const;
	Iterator endAt(bool primary) const;
	Iterator baseEndAt(bool primary) const;

	bool sort();
	template<typename U> bool sortOptional(const U&);
	template<typename Action> bool sortGroup(const Action &action);

	bool isOptional() const;

private:
	typedef CombSorter<T*> BaseSorter;
	typedef util::XArray<T, Alloc> ElemList;

	template<typename U> bool sortAt();
	template<typename U> bool sortUniqueAt();
	template<typename U> bool sortLimitedAt();

	template<typename U, typename Action>
	bool sortGroupAt(const Action &action);

	template<typename Action>
	bool prepareBaseSorter(bool primary, const Action &action);
	bool acceptResult(bool sorted);

	void flip(bool primary);
	T* toFlippedAddress(T *src) const;

	template<typename Action>
	void groupAll(bool primary, const Action &action);
	void groupAll(bool primary, const util::FalseType&);

	union Elem {
		uint64_t value_;
		uint8_t data_[sizeof(T)];
	};

	BaseSorter base_;
	std::pair<Pred, Pred> predPair_;
	std::pair<bool, bool> predEmpty_;

	T *begin_;
	T *tail_;
	T *end_;
	T *secondaryBegin_;
	std::pair<T*, T*> sortingBounds_;
	std::pair<int64_t, int64_t> reducedSizes_;

	T *tmp_;
};

template<typename T>
class SQLAlgorithmUtils::HeapElement {
public:
	HeapElement(const T &value, size_t ordinal);

	const T& getValue() const;
	size_t getOrdinal() const;

	bool next() const;

	template<typename U>
	bool nextAt() const;

private:
	T value_;
	size_t ordinal_;
};

template<typename T, typename Pred>
class SQLAlgorithmUtils::HeapPredicate {
public:
	explicit HeapPredicate(const Pred &base);

	bool operator()(
			const HeapElement<T> &e1, const HeapElement<T> &e2) const;

	const Pred& getBase() const { return base_; }

private:
	Pred base_;
};

template<typename T, typename Pred, typename Alloc>
class SQLAlgorithmUtils::HeapQueue {
public:
	typedef HeapElement<T> Element;

	class Push {
	public:
		typedef void RetType;

		template<typename U> class TypeAt {
		public:
			explicit TypeAt(const Push &base) : base_(base) {}

			void operator()() const {
				base_.base_.template pushAt<U>(base_.elem_);
			}

		private:
			const Push &base_;
		};

		Push(HeapQueue &base, const Element &elem) :
				base_(base), elem_(elem) {}

	private:
		HeapQueue &base_;
		const Element &elem_;
	};

	class Pop {
	public:
		typedef Element RetType;

		template<typename U> class TypeAt {
		public:
			explicit TypeAt(const Pop &base) : base_(base) {}

			Element operator()() const {
				return base_.base_.template popAt<U>();
			}

		private:
			const Pop &base_;
		};

		explicit Pop(HeapQueue &base) : base_(base) {}

	private:
		HeapQueue &base_;
	};

	template<typename A>
	class Merge {
	public:
		typedef bool RetType;
		typedef typename A::OptionsType OptionsType;

		template<typename U> class TypeAt {
		public:
			explicit TypeAt(const Merge &base) : base_(base) {}

			bool operator()() const {
				return base_.base_.mergeAt<U, A>(base_.action_);
			}

		private:
			const Merge &base_;
		};

		Merge(HeapQueue &base, A &action) :
				base_(base),
				action_(action) {
		}

	private:
		HeapQueue &base_;
		A &action_;
	};

	template<typename A>
	class MergeUnique {
	public:
		typedef bool RetType;
		typedef typename A::OptionsType OptionsType;

		template<typename U> class TypeAt {
		public:
			explicit TypeAt(const MergeUnique &base) : base_(base) {}

			bool operator()() const {
				return base_.base_.mergeUniqueAt<U, A>(base_.action_);
			}

		private:
			const MergeUnique &base_;
		};

		MergeUnique(HeapQueue &base, A &action) :
				base_(base),
				action_(action) {
		}

	private:
		HeapQueue &base_;
		A &action_;
	};

	template<typename A>
	class MergeLimited {
	public:
		typedef bool RetType;
		typedef typename A::OptionsType OptionsType;

		template<typename U> class TypeAt {
		public:
			explicit TypeAt(const MergeLimited &base) : base_(base) {}

			bool operator()() const {
				return base_.base_.mergeLimitedAt<U, A>(base_.action_);
			}

		private:
			const MergeLimited &base_;
		};

		MergeLimited(HeapQueue &base, A &action) :
				base_(base),
				action_(action) {
		}

	private:
		HeapQueue &base_;
		A &action_;
	};

	HeapQueue(const Pred &basePred, const Alloc &alloc);

	bool isEmpty() const;

	bool isPredicateEmpty() const;
	void setPredicateEmpty(bool empty);

	Element newElement(const T &value);

	void push(const Element &elem);
	Element pop();

	template<typename A>
	bool merge(A &action);

	template<typename A>
	bool mergeUnique(A &action);

	template<typename A>
	bool mergeOrderedUnique(A &action);

	template<typename A>
	bool mergeLimited(A &action);

	template<typename A>
	bool mergeUnchecked(A &action);

	template<typename A>
	bool mergeUncheckedUnique(A &action);

private:
	template<typename U> void pushAt(const Element &elem);
	template<typename U> Element popAt();

	template<typename P> void pushAtTmp(const P &typedPred);
	template<typename P> const Element& popAtTmp(const P &typedPred);

	template<typename U, typename A> bool mergeAt(A &action);
	template<typename U, typename A> bool mergeUniqueAt(A &action);
	template<typename U, typename A> bool mergeLimitedAt(A &action);

	typedef typename Alloc::template rebind<Element>::other ElementAlloc;
	typedef util::Vector<Element, ElementAlloc> ElementList;

	ElementList elemList_;
	HeapPredicate<T, Pred> pred_;
	bool predEmpty_;
};

template<typename K, typename Hash, typename Pred, typename Alloc>
class SQLAlgorithmUtils::TreeHashSet {
private:
	typedef uint64_t BaseKeyType;
	typedef K BaseMappedType;
	typedef std::pair<BaseKeyType, BaseMappedType> BaseValueType;
	typedef typename std::map<
			BaseKeyType, BaseMappedType>::key_compare BaseKeyComp;
	typedef util::MultiMap<
			BaseKeyType, BaseMappedType, BaseKeyComp, Alloc> BaseType;

	typedef typename BaseType::iterator BaseIterator;

	class Iterator;

public:
	typedef Iterator iterator;

	TreeHashSet(
			size_t, const Hash &hash, const Pred &pred, const Alloc &alloc) :
			base_(alloc) {
	}

	bool empty() const {
		return base_.empty();
	}

	std::pair<iterator, bool> insert(const K &key) {
		const BaseKeyType &baseKey = hash_(key);
		const std::pair<BaseIterator, BaseIterator> &range =
				base_.equal_range(baseKey);

		BaseIterator it = range.first;
		for (; it != range.second; ++it) {
			if (pred_(it->second, key)) {
				return std::make_pair(iterator(it), false);
			}
		}

		return std::make_pair(iterator(base_.insert(
				it, BaseValueType(baseKey, key))), true);
	}

	iterator find(const K &key) {
		const BaseKeyType &baseKey = hash_(key);
		const std::pair<BaseIterator, BaseIterator> &range =
				base_.equal_range(baseKey);

		for (BaseIterator it = range.first; it != range.second; ++it) {
			if (pred_(it->second, key)) {
				return iterator(it);
			}
		}

		return end();
	}

	void erase(iterator it) {
		base_.erase(it.baseIt_);
	}

	iterator begin() {
		return iterator(base_.begin());
	}

	iterator end() {
		return iterator(base_.end());
	}

private:
	BaseType base_;
	Hash hash_;
	Pred pred_;
};

template<typename K, typename Hash, typename Pred, typename Alloc>
class SQLAlgorithmUtils::TreeHashSet<K, Hash, Pred, Alloc>::Iterator {
public:
	explicit Iterator(const BaseIterator &baseIt) : baseIt_(baseIt) {
	}

	K& operator*() const {
		return baseIt_->second;
	}

	K* operator->() const {
		return &baseIt_->second;
	}

	bool operator==(const Iterator &another) const {
		return baseIt_ == another.baseIt_;
	}

	bool operator!=(const Iterator &another) const {
		return baseIt_ != another.baseIt_;
	}

	Iterator& operator++() {
		++baseIt_;
		return *this;
	}

	Iterator operator++(int) {
		const Iterator last = *this;
		++baseIt_;
		return last;
	}

private:
	friend class TreeHashSet;
	BaseIterator baseIt_;
};

template<
		typename K, typename T, typename Hash, typename Pred,
		typename Alloc>
class SQLAlgorithmUtils::TreeHashMap {
private:
	typedef uint64_t BaseKeyType;
	typedef std::pair<K, T> BaseMappedType;
	typedef std::pair<BaseKeyType, BaseMappedType> BaseValueType;
	typedef typename std::map<
			BaseKeyType, BaseMappedType>::key_compare BaseKeyComp;
	typedef util::MultiMap<
			BaseKeyType, BaseMappedType, BaseKeyComp, Alloc> BaseType;

	typedef typename BaseType::iterator BaseIterator;

	class Iterator;

public:
	typedef Iterator iterator;

	TreeHashMap(
			size_t, const Hash &hash, const Pred &pred, const Alloc &alloc) :
			base_(alloc) {
	}

	bool empty() const {
		return base_.empty();
	}

	std::pair<iterator, bool> insert(const std::pair<K, T> &value) {
		const BaseKeyType &baseKey = hash_(value.first);
		const std::pair<BaseIterator, BaseIterator> &range =
				base_.equal_range(baseKey);

		BaseIterator it = range.first;
		for (; it != range.second; ++it) {
			if (pred_(it->second.first, value.first)) {
				return std::make_pair(iterator(it), false);
			}
		}

		return std::make_pair(iterator(base_.insert(
				it, BaseValueType(baseKey, value))), true);
	}

	iterator find(const K &key) {
		const BaseKeyType &baseKey = hash_(key);
		const std::pair<BaseIterator, BaseIterator> &range =
				base_.equal_range(baseKey);

		for (BaseIterator it = range.first; it != range.second; ++it) {
			if (pred_(it->second.first, key)) {
				return iterator(it);
			}
		}

		return end();
	}

	T& operator[](const K &key) {
		return insert(std::pair<K, T>(key, T())).first->second;
	}

	void erase(iterator it) {
		base_.erase(it.baseIt_);
	}

	iterator begin() {
		return iterator(base_.begin());
	}

	iterator end() {
		return iterator(base_.end());
	}

private:
	BaseType base_;
	Hash hash_;
	Pred pred_;
};

template<
		typename K, typename T, typename Hash, typename Pred,
		typename Alloc>
class SQLAlgorithmUtils::TreeHashMap<K, T, Hash, Pred, Alloc>::Iterator {
public:
	explicit Iterator(const BaseIterator &baseIt) : baseIt_(baseIt) {
	}

	std::pair<K, T>& operator*() const {
		return baseIt_->second;
	}

	std::pair<K, T>* operator->() const {
		return &baseIt_->second;
	}

	bool operator==(const Iterator &another) const {
		return baseIt_ == another.baseIt_;
	}

	bool operator!=(const Iterator &another) const {
		return baseIt_ != another.baseIt_;
	}

	Iterator& operator++() {
		++baseIt_;
		return *this;
	}

	Iterator operator++(int) {
		const Iterator last = *this;
		++baseIt_;
		return last;
	}

private:
	friend class TreeHashMap;
	BaseIterator baseIt_;
};

template<
		typename K, typename T, typename Hash, typename Pred,
		typename Alloc>
class SQLAlgorithmUtils::TreeHashMultiMap {
private:
	typedef uint64_t BaseKeyType;
	typedef std::pair<K, T> BaseMappedType;
	typedef std::pair<BaseKeyType, BaseMappedType> BaseValueType;
	typedef typename std::map<
			BaseKeyType, BaseMappedType>::key_compare BaseKeyComp;
	typedef util::MultiMap<
			BaseKeyType, BaseMappedType, BaseKeyComp, Alloc> BaseType;

	typedef typename BaseType::iterator BaseIterator;

public:
	typedef typename TreeHashMap<K, T, Hash, Pred, Alloc>::iterator iterator;

	TreeHashMultiMap(
			size_t, const Hash &hash, const Pred &pred, const Alloc &alloc) :
			base_(alloc) {
	}

	bool empty() const {
		return base_.empty();
	}

	iterator insert(const std::pair<K, T> &value) {
		const BaseKeyType &baseKey = hash_(value.first);
		const std::pair<BaseIterator, BaseIterator> &range =
				base_.equal_range(baseKey);

		BaseIterator it = range.first;
		for (; it != range.second; ++it) {
			if (pred_(it->second.first, value.first)) {
				return iterator(base_.insert(it, BaseValueType(baseKey, value)));
			}
		}

		return iterator(base_.insert(it, BaseValueType(baseKey, value)));
	}

	iterator insert(iterator, const std::pair<K, T> &value) {
		return insert(value);
	}

	iterator find(const K &key) {
		const BaseKeyType &baseKey = hash_(key);
		const std::pair<BaseIterator, BaseIterator> &range =
				base_.equal_range(baseKey);

		for (BaseIterator it = range.first; it != range.second; ++it) {
			if (pred_(it->second.first, key)) {
				return iterator(it);
			}
		}

		return end();
	}

	std::pair<iterator, iterator> equal_range(const K &key) {
		const BaseKeyType &baseKey = hash_(key);
		std::pair<BaseIterator, BaseIterator> range =
				base_.equal_range(baseKey);

		for (BaseIterator it = range.first;; ++it) {
			if (it == range.second) {
				return std::make_pair(end(), end());
			}

			if (pred_(it->second.first, key)) {
				range.first = it;
				while (++it != range.second && pred_(it->second.first, key)) {
				}
				range.second = it;
				break;
			}
		}

		return std::make_pair(iterator(range.first), iterator(range.second));
	}

	void erase(iterator it) {
		base_.erase(it.baseIt_);
	}

	iterator begin() {
		return iterator(base_.begin());
	}

	iterator end() {
		return iterator(base_.end());
	}

private:
	BaseType base_;
	Hash hash_;
	Pred pred_;
};

template<typename K, typename Hash, typename Pred, typename Alloc>
class SQLAlgorithmUtils::HashSet : public
#if UTIL_CXX11_SUPPORTED
		std::unordered_set<K, Hash, Pred, Alloc>
#else
		SQLAlgorithmUtils::TreeHashSet<K, Hash, Pred, Alloc>
#endif
{
public:
	HashSet(
			size_t capacity, const Hash &hash, const Pred &pred,
			const Alloc &alloc) :
#if UTIL_CXX11_SUPPORTED
			std::unordered_set<K, Hash, Pred, Alloc>
#else
			SQLAlgorithmUtils::TreeHashSet<K, Hash, Pred, Alloc>
#endif
			(capacity, hash, pred, alloc) {
	}
};

template<
		typename K, typename T, typename Hash, typename Pred,
		typename Alloc>
class SQLAlgorithmUtils::HashMap : public
#if UTIL_CXX11_SUPPORTED
		std::unordered_map<K, T, Hash, Pred, Alloc>
#else
		SQLAlgorithmUtils::TreeHashMap<K, T, Hash, Pred, Alloc>
#endif
{
public:
	HashMap(
			size_t capacity, const Hash &hash, const Pred &pred,
			const Alloc &alloc) :
#if UTIL_CXX11_SUPPORTED
			std::unordered_map<K, T, Hash, Pred, Alloc>
#else
			SQLAlgorithmUtils::TreeHashMap<K, T, Hash, Pred, Alloc>
#endif
			(capacity, hash, pred, alloc) {
	}
};

template<
		typename K, typename T, typename Hash, typename Pred,
		typename Alloc>
class SQLAlgorithmUtils::HashMultiMap : public
#if UTIL_CXX11_SUPPORTED
		std::unordered_multimap<K, T, Hash, Pred, Alloc>
#else
		SQLAlgorithmUtils::TreeHashMultiMap<K, T, Hash, Pred, Alloc>
#endif
{
public:
	HashMultiMap(
			size_t capacity, const Hash &hash, const Pred &pred,
			const Alloc &alloc) :
#if UTIL_CXX11_SUPPORTED
			std::unordered_multimap<K, T, Hash, Pred, Alloc>
#else
			SQLAlgorithmUtils::TreeHashMultiMap<K, T, Hash, Pred, Alloc>
#endif
			(capacity, hash, pred, alloc) {
	}
};

template<typename Alloc>
class SQLAlgorithmUtils::DenseBitSet {
public:
	typedef std::pair<int64_t, int64_t> CodedEntry;

	class Iterator;
	class InputCursor;
	class OutputCursor;

	typedef Alloc allocator_type;
	typedef typename Alloc::value_type key_type;
	typedef typename Alloc::value_type value_type;
	typedef Iterator iterator;

	explicit DenseBitSet(const allocator_type &alloc);

	std::pair<iterator, bool> insert(const value_type &val);
	iterator find(const key_type &k);
	iterator end();

	size_t capacity() const;

private:
	typedef value_type BaseKey;
	typedef uint64_t BitsType;
	struct BaseHasher {
		size_t operator()(BaseKey key) const { return static_cast<size_t>(key); }
	};
	typedef std::equal_to<BaseKey> BasePred;
	typedef HashMap<BaseKey, BitsType, BaseHasher, BasePred> Base;

	template<size_t, int> struct Nlz;
	template<int C> struct Nlz<sizeof(uint64_t) * CHAR_BIT, C> {
		static const BitsType VALUE = (sizeof(uint64_t) * CHAR_BIT) - 6;
	};

	struct Constants {
		static const BitsType SET_BITS_SIZE = sizeof(BitsType) * CHAR_BIT;
		static const BitsType SET_BITS_BASE = 1U;
		static const BitsType SET_BITS_MASK = (SET_BITS_BASE <<
				(SET_BITS_SIZE - Nlz<SET_BITS_SIZE, 0>::VALUE)) - 1U;

		static const BaseKey SET_KEY_MASK =
				static_cast<BaseKey>(~SET_BITS_MASK);
	};

	Base base_;
};

template<typename Alloc>
class SQLAlgorithmUtils::DenseBitSet<Alloc>::Iterator {
public:
	explicit Iterator(bool found = false);
	bool operator!=(const Iterator &another) const;
	bool operator==(const Iterator &another) const;

private:
	bool found_;
};

template<typename Alloc>
class SQLAlgorithmUtils::DenseBitSet<Alloc>::InputCursor {
public:
	explicit InputCursor(DenseBitSet &bitSet);
	bool next(CodedEntry &entry);

private:
	typedef typename Base::iterator BaseIterator;

	DenseBitSet &bitSet_;
	BaseIterator baseIt_;
};

template<typename Alloc>
class SQLAlgorithmUtils::DenseBitSet<Alloc>::OutputCursor {
public:
	explicit OutputCursor(DenseBitSet &bitSet);
	void next(const CodedEntry &entry);

private:
	DenseBitSet &bitSet_;
};



template<typename It>
SQLAlgorithmUtils::CombSorter<It>::CombSorter(It begin, It end) :
		begin_(begin),
		end_(end),
		gap_(end - begin),
		sorted_(false),
		middle_(end) {
	assert(gap_ >= 0);
}

template<typename It>
void SQLAlgorithmUtils::CombSorter<It>::setConfig(const SortConfig &config) {
	config_ = config;
}

template<typename It>
const SQLAlgorithmUtils::SortConfig&
SQLAlgorithmUtils::CombSorter<It>::getConfig() const {
	return config_;
}

template<typename It>
bool SQLAlgorithmUtils::CombSorter<It>::isSorted() const {
	return (begin_ == end_ || sorted_);
}

template<typename It>
template<typename Pred>
bool SQLAlgorithmUtils::CombSorter<It>::run(const Pred &pred) {
	while (!sorted_ || middle_ != end_) {
		if (middle_ == end_) {
			gap_ = gap_ * SORT_SHRINK_BASE / SORT_SHRINK_RATE;
			if (gap_ <= 1) {
				gap_ = 1;
				sorted_ = true;
			}
			if (gap_ >= (end_ - begin_)) {
				gap_ = 0;
				sorted_ = true;
			}
		}

		It it = begin_;
		It gapIt = it + gap_;

		assert(config_.interval_ > 0);
		middle_ = gapIt + static_cast<ptrdiff_t>(
				std::min<int64_t>(config_.interval_, (end_ - gapIt)));
		while (gapIt != middle_) {
			if (pred(*gapIt, *it)) {
				std::swap(*it, *gapIt);
				sorted_ = false;
			}
			++it;
			++gapIt;
		}
		if (middle_ != end_) {
			return false;
		}
	}
	return sorted_;
}

template<typename It>
template<typename Pred>
bool SQLAlgorithmUtils::CombSorter<It>::runMain(
		const Pred &pred, It tmp, bool tmpAllowed, bool *resultOnTmp) {
	ptrdiff_t baseUnit = 128;

	typedef std::pair<It, It> Range;
	const size_t listCapacity= 32;
	Range list[listCapacity];
	Range *listIt = list;
	Range *listTail = listIt;
	Range prev(begin_, begin_);
	bool tailReached = false;
	for (;;) {
		const bool fromTmp = ((listIt - list) % 2 != 0);
		if (prev.first == prev.second) {
			if (listIt == list) {
				if (prev.first == NULL) {
					prev = Range(listIt->second, listIt->second);
				}
				ptrdiff_t unit = end_ - prev.second;
				if (unit > baseUnit) {
					unit = baseUnit;
				}
				else {
					tailReached = true;
				}
				prev.second += unit;
			}
			else if (tailReached) {
				if (listIt >= listTail && (!fromTmp || tmpAllowed)) {
					assert(resultOnTmp != NULL);
					*resultOnTmp = fromTmp;
					assert((end_ - begin_) == (listIt->second - listIt->first));
					return true;
				}
			}
			else {
				if (listIt > listTail) {
					listTail = listIt;
				}
				listIt = list;
				continue;
			}
		}

		if (listIt->first == listIt->second) {
			*listIt = prev;
			prev = Range();
			continue;
		}

		Range next((listIt + 1)->second, It());
		if (next.first == NULL) {
			next.first = (fromTmp ? begin_ : tmp);
		}

		if (listIt == list) {
			for (size_t i = 0; i < 2; i++) {
				runSubSort(
						pred,
						(i == 0 ? listIt->first : prev.first),
						(i == 0 ? listIt->second : prev.second));
			}
		}
		runSubMerge(
				pred,
				listIt->first, listIt->second,
				prev.first, prev.second,
				next.first);
		next.second = next.first +
				(listIt->second - listIt->first) +
				(prev.second - prev.first);

		*listIt = Range(prev.second, prev.second);
		prev = next;
		++listIt;
		assert(listIt - list < static_cast<ptrdiff_t>(listCapacity));
	}
}

template<typename It>
template<typename Pred>
ptrdiff_t SQLAlgorithmUtils::CombSorter<It>::runMainUnique(
		const Pred &pred, It tmp, bool tmpAllowed, bool *resultOnTmp) {
	const ptrdiff_t baseUnit = 32;

	typedef std::pair<It, It> Range;
	const size_t listCapacity= 32;
	Range list[listCapacity];
	Range *listIt = list;
	Range *listTail = listIt;
	Range prev(begin_, begin_);
	bool tailReached = false;
	for (;;) {
		const bool fromTmp = ((listIt - list) % 2 != 0);
		if (prev.first == prev.second) {
			if (listIt == list) {
				if (prev.first == NULL) {
					prev = Range(listIt->second, listIt->second);
				}
				ptrdiff_t unit = end_ - prev.second;
				if (unit > baseUnit) {
					unit = baseUnit;
				}
				else {
					tailReached = true;
				}
				prev.second += unit;
			}
			else if (tailReached) {
				if (listIt >= listTail && (!fromTmp || tmpAllowed)) {
					assert(resultOnTmp != NULL);
					*resultOnTmp = fromTmp;
					return (end_ - begin_) - (listIt->second - listIt->first);
				}
			}
			else {
				if (listIt > listTail) {
					listTail = listIt;
				}
				listIt = list;
				continue;
			}
		}

		if (listIt->first == listIt->second) {
			*listIt = prev;
			prev = Range();
			continue;
		}

		Range next((listIt + 1)->second, It());
		if (next.first == NULL) {
			next.first = (fromTmp ? begin_ : tmp);
		}

		if (listIt == list) {
			uint32_t dup1[baseUnit];
			uint32_t dup2[baseUnit];
			for (size_t i = 0; i < 2; i++) {
				runSubSortUnique(
						pred,
						(i == 0 ? listIt->first : prev.first),
						(i == 0 ? listIt->second : prev.second),
						(i == 0 ? dup1 : dup2));
			}
			next.second = runSubMergeTopUnique(
					pred,
					listIt->first, listIt->second,
					prev.first, prev.second,
					dup1, dup2, next.first);
		}
		else {
			next.second = runSubMergeMidUnique(
					pred,
					listIt->first, listIt->second,
					prev.first, prev.second,
					next.first);
		}
		*listIt = Range(prev.second, prev.second);
		prev = next;
		++listIt;
		assert(listIt - list < static_cast<ptrdiff_t>(listCapacity));
	}
}

template<typename It>
template<typename Pred, typename Action>
ptrdiff_t SQLAlgorithmUtils::CombSorter<It>::runMainGroup(
		const Pred &pred, const Action &action,
		It tmp, bool tmpAllowed, bool *resultOnTmp) {
	const ptrdiff_t baseUnit = 32;

	typedef std::pair<It, It> Range;
	const size_t listCapacity= 32;
	Range list[listCapacity];
	Range *listIt = list;
	Range *listTail = listIt;
	Range prev(begin_, begin_);
	bool tailReached = false;
	for (;;) {
		const bool fromTmp = ((listIt - list) % 2 != 0);
		if (prev.first == prev.second) {
			if (listIt == list) {
				if (prev.first == NULL) {
					prev = Range(listIt->second, listIt->second);
				}
				ptrdiff_t unit = end_ - prev.second;
				if (unit > baseUnit) {
					unit = baseUnit;
				}
				else {
					tailReached = true;
				}
				prev.second += unit;
			}
			else if (tailReached) {
				if (listIt >= listTail && (!fromTmp || tmpAllowed)) {
					assert(resultOnTmp != NULL);
					*resultOnTmp = fromTmp;
					return (end_ - begin_) - (listIt->second - listIt->first);
				}
			}
			else {
				if (listIt > listTail) {
					listTail = listIt;
				}
				listIt = list;
				continue;
			}
		}

		if (listIt->first == listIt->second) {
			*listIt = prev;
			prev = Range();
			continue;
		}

		Range next((listIt + 1)->second, It());
		if (next.first == NULL) {
			next.first = (fromTmp ? begin_ : tmp);
		}

		if (listIt == list) {
			uint32_t dup1[baseUnit];
			uint32_t dup2[baseUnit];
			for (size_t i = 0; i < 2; i++) {
				runSubSortUnique(
						pred,
						(i == 0 ? listIt->first : prev.first),
						(i == 0 ? listIt->second : prev.second),
						(i == 0 ? dup1 : dup2));
			}
			next.second = runSubMergeTopGroup(
					pred, action,
					listIt->first, listIt->second,
					prev.first, prev.second,
					dup1, dup2, next.first);
		}
		else {
			next.second = runSubMergeMidGroup(
					pred, action,
					listIt->first, listIt->second,
					prev.first, prev.second,
					next.first);
		}
		*listIt = Range(prev.second, prev.second);
		prev = next;
		++listIt;
		assert(listIt - list < static_cast<ptrdiff_t>(listCapacity));
	}
}

template<typename It>
template<typename Pred>
void SQLAlgorithmUtils::CombSorter<It>::runSubSort(
		const Pred &pred, It begin, It end) {
	bool sorted = false;
	ptrdiff_t gap = end - begin;
	while (!sorted) {
		gap = gap * SORT_SHRINK_BASE / SORT_SHRINK_RATE;
		if (gap <= 1) {
			gap = 1;
			sorted = true;
		}
		if (gap >= (end - begin)) {
			gap = 0;
			sorted = true;
		}

		It it = begin;
		It gapIt = it + gap;
		while (gapIt != end) {
			if (pred(*gapIt, *it)) {
				std::swap(*it, *gapIt);
				sorted = false;
			}
			++it;
			++gapIt;
		}
	}
}

template<typename It>
template<typename Pred>
void SQLAlgorithmUtils::CombSorter<It>::runSubSortUnique(
		const Pred &pred, It begin, It end, uint32_t *dup) {
	bool sorted = false;
	ptrdiff_t gap = end - begin;
	while (!sorted) {
		gap = gap * SORT_SHRINK_BASE / SORT_SHRINK_RATE;
		if (gap <= 1) {
			gap = 1;
			sorted = true;
		}
		if (gap >= (end - begin)) {
			gap = 0;
			sorted = true;
		}

		It it = begin;
		It gapIt = it + gap;
		if (gap <= 1) {
			uint32_t *dupIt = dup;
			*dupIt = (gap < 1 ? 0 : 1);
			while (gapIt != end) {
				const int32_t ret = pred.toThreeWay()(*it, *gapIt);
				if (ret > 0) {
					std::swap(*it, *gapIt);
					sorted = false;
				}
				else if (ret < 0) {
					*(++dupIt) = 1;
				}
				else {
					++(*dupIt);
				}
				++it;
				++gapIt;
			}
		}
		else {
			while (gapIt != end) {
				if (pred(*gapIt, *it)) {
					std::swap(*it, *gapIt);
					sorted = false;
				}
				++it;
				++gapIt;
			}
		}
	}
}

template<typename It>
template<typename Pred>
void SQLAlgorithmUtils::CombSorter<It>::runSubMerge(
		const Pred &pred, It begin1, It end1, It begin2, It end2, It out) {
	It in1 = begin1;
	It in2 = begin2;

	if (in1 != end1 && in2 != end2) {
		for (;;) {
			if (pred(*in2, *in1)) {
				*out = *in2;
				++out;
				if (++in2 == end2) {
					break;
				}
			}
			else {
				*out = *in1;
				++out;
				if (++in1 == end1) {
					break;
				}
			}
		}
	}

	for (; in1 != end1; ++in1) {
		*out = *in1;
		++out;
	}

	for (; in2 != end2; ++in2) {
		*out = *in2;
		++out;
	}
}

template<typename It>
template<typename Pred>
It SQLAlgorithmUtils::CombSorter<It>::runSubMergeTopUnique(
		const Pred &pred, It begin1, It end1, It begin2, It end2,
		const uint32_t *dup1, const uint32_t *dup2, It out) {
	It in1 = begin1;
	It in2 = begin2;

	if (in1 != end1 && in2 != end2) {
		for (;;) {
			const int32_t ret = pred.toThreeWay()(*in1, *in2);
			if (ret > 0) {
				*out = *in2;
				++out;
				if ((in2 += *(dup2++)) == end2) {
					break;
				}
			}
			else {
				*out = *in1;
				++out;
				if (ret < 0) {
					if ((in1 += *(dup1++)) == end1) {
						break;
					}
				}
				else {
					in1 += *(dup1++);
					in2 += *(dup2++);
					if (in1 == end1 || in2 == end2) {
						break;
					}
				}
			}
		}
	}

	for (; in1 != end1; in1 += *(dup1++)) {
		*out = *in1;
		++out;
	}

	for (; in2 != end2; in2 += *(dup2++)) {
		*out = *in2;
		++out;
	}

	return out;
}

template<typename It>
template<typename Pred, typename Action>
It SQLAlgorithmUtils::CombSorter<It>::runSubMergeTopGroup(
		const Pred &pred, const Action &action,
		It begin1, It end1, It begin2, It end2,
		const uint32_t *dup1, const uint32_t *dup2, It out) {
	It in1 = begin1;
	It in2 = begin2;

	if (in1 != end1 && in2 != end2) {
		for (;;) {
			const int32_t ret = pred.toThreeWay()(*in1, *in2);
			if (ret > 0) {
				*out = *in2;
				action(*out);
				for (uint32_t dup = *(dup2++); --dup > 0;) {
					action(*out, *(++in2));
				}
				++out;
				if (++in2 == end2) {
					break;
				}
			}
			else {
				*out = *in1;
				action(*out);
				for (uint32_t dup = *(dup1++); --dup > 0;) {
					action(*out, *(++in1));
				}
				++in1;
				if (ret < 0) {
					++out;
					if (in1 == end1) {
						break;
					}
				}
				else {
					{
						uint32_t dup = *(dup2++);
						do {
							action(*out, *(in2++));
						}
						while (--dup > 0);
					}
					++out;
					if (in1 == end1 || in2 == end2) {
						break;
					}
				}
			}
		}
	}

	for (; in1 != end1; ++in1) {
		*out = *in1;
		action(*out);
		for (uint32_t dup = *(dup1++); --dup > 0;) {
			action(*out, *(++in1));
		}
		++out;
	}

	for (; in2 != end2; ++in2) {
		*out = *in2;
		action(*out);
		for (uint32_t dup = *(dup2++); --dup > 0;) {
			action(*out, *(++in2));
		}
		++out;
	}

	return out;
}

template<typename It>
template<typename Pred>
It SQLAlgorithmUtils::CombSorter<It>::runSubMergeMidUnique(
		const Pred &pred, It begin1, It end1, It begin2, It end2, It out) {
	It in1 = begin1;
	It in2 = begin2;

	if (in1 != end1 && in2 != end2) {
		for (;;) {
			const int32_t ret = pred.toThreeWay()(*in1, *in2);
			if (ret > 0) {
				*out = *in2;
				++out;
				if (++in2 == end2) {
					break;
				}
			}
			else {
				*out = *in1;
				++out;
				if (ret < 0) {
					if (++in1 == end1) {
						break;
					}
				}
				else {
					++in1;
					++in2;
					if (in1 == end1 || in2 == end2) {
						break;
					}
				}
			}
		}
	}

	for (; in1 != end1; ++in1) {
		*out = *in1;
		++out;
	}

	for (; in2 != end2; ++in2) {
		*out = *in2;
		++out;
	}

	return out;
}

template<typename It>
template<typename Pred, typename Action>
It SQLAlgorithmUtils::CombSorter<It>::runSubMergeMidGroup(
		const Pred &pred, const Action &action,
		It begin1, It end1, It begin2, It end2, It out) {
	It in1 = begin1;
	It in2 = begin2;

	if (in1 != end1 && in2 != end2) {
		for (;;) {
			const int32_t ret = pred.toThreeWay()(*in1, *in2);
			if (ret > 0) {
				*out = *in2;
				++out;
				if (++in2 == end2) {
					break;
				}
			}
			else {
				*out = *in1;
				if (ret < 0) {
					++out;
					if (++in1 == end1) {
						break;
					}
				}
				else {
					action(*out, *in2, util::FalseType());
					++out;
					++in1;
					++in2;
					if (in1 == end1 || in2 == end2) {
						break;
					}
				}
			}
		}
	}

	for (; in1 != end1; ++in1) {
		*out = *in1;
		++out;
	}

	for (; in2 != end2; ++in2) {
		*out = *in2;
		++out;
	}

	return out;
}

template<typename It>
template<typename Pred>
bool SQLAlgorithmUtils::CombSorter<It>::runLimited(
		const Pred &pred, int64_t &reduced) {
	It tail = begin_ + static_cast<ptrdiff_t>(std::min<int64_t>(
			end_ - begin_, std::max<int64_t>(config_.limit_, 0)));
	reduced = (end_ - tail);

	for (It it = begin_; it != tail;) {
		std::push_heap(begin_, ++it, pred);
	}
	for (It it = tail; it != end_; ++it) {
		if (pred(*it, *begin_)) {
			*tail = *it;
			std::push_heap(begin_, tail + 1, pred);
			std::pop_heap(begin_, tail + 1, pred);
		}
	}
	for (It it = tail; it != begin_; --it) {
		std::pop_heap(begin_, it, pred);
	}
	return true;
}


template<typename T, typename Pred, typename Alloc>
SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::Sorter(
		size_t capacity, const Pred &pred1, const Pred &pred2,
		const std::pair<T*, T*> &buffer) :
		base_(NULL, NULL),
		predPair_(pred1, pred2),
		begin_(NULL),
		tail_(NULL),
		end_(NULL),
		secondaryBegin_(NULL),
		tmp_(NULL) {
	begin_ = buffer.first;
	tail_ = begin_;
	end_ = tail_ + capacity;
	secondaryBegin_ = end_;
	tmp_ = buffer.second;
}

template<typename T, typename Pred, typename Alloc>
void SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::setConfig(
		const SortConfig &config) {
	base_.setConfig(config);
}

template<typename T, typename Pred, typename Alloc>
void SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::setPredicateEmpty(
		bool primary, bool empty) {
	bool &dest = (primary ? predEmpty_.first : predEmpty_.second);
	dest = empty;
}

template<typename T, typename Pred, typename Alloc>
void SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::clear() {
	const SortConfig config = base_.getConfig();
	base_ = BaseSorter(NULL, NULL);
	base_.setConfig(config);
	tail_ = begin_;
	secondaryBegin_ = end_;
	sortingBounds_ = std::pair<T*, T*>();
	reducedSizes_ = std::pair<int64_t, int64_t>();
}

template<typename T, typename Pred, typename Alloc>
inline void SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::add(const T &elem) {
	assert(tail_ != secondaryBegin_);
	new (tail_) T(elem);
	++tail_;
}

template<typename T, typename Pred, typename Alloc>
inline void SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::addSecondary(
		const T &elem) {
	assert(secondaryBegin_ != tail_);
	new (--secondaryBegin_) T(elem);
}

template<typename T, typename Pred, typename Alloc>
bool SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::isEmpty() const {
	return (isEmptyAt(true) && isEmptyAt(false));
}

template<typename T, typename Pred, typename Alloc>
bool SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::isEmptyAt(bool primary) const {
	if (primary) {
		return (tail_ - begin_ == 0);
	}
	else {
		return (end_ - secondaryBegin_ == 0);
	}
}

template<typename T, typename Pred, typename Alloc>
bool SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::isEmptyPredicateAt(
		bool primary) const {
	return (primary ? predEmpty_.first : predEmpty_.second);
}

template<typename T, typename Pred, typename Alloc>
bool SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::isFilled() const {
	return (tail_ == secondaryBegin_);
}

template<typename T, typename Pred, typename Alloc>
bool SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::isSorted() const {
	return (sortingBounds_.first == tail_ &&
			sortingBounds_.second == secondaryBegin_ &&
			base_.isSorted());
}

template<typename T, typename Pred, typename Alloc>
size_t SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::getRemaining() const {
	assert(secondaryBegin_ >= tail_);
	return (secondaryBegin_ - tail_);
}

template<typename T, typename Pred, typename Alloc>
inline typename SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::Iterator
SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::begin() const {
	return beginAt(true);
}

template<typename T, typename Pred, typename Alloc>
inline typename SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::Iterator
SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::end() const {
	return endAt(true);
}

template<typename T, typename Pred, typename Alloc>
inline typename SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::Iterator
SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::beginAt(bool primary) const {
	return (primary ? begin_ : secondaryBegin_);
}

template<typename T, typename Pred, typename Alloc>
inline typename SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::Iterator
SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::endAt(bool primary) const {
	return (primary ? tail_ : end_) - static_cast<ptrdiff_t>(
			primary ? reducedSizes_.first : reducedSizes_.second);
}

template<typename T, typename Pred, typename Alloc>
inline typename SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::Iterator
SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::baseEndAt(bool primary) const {
	return (primary ? tail_ : end_);
}

template<typename T, typename Pred, typename Alloc>
bool SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::sort() {
	for (size_t i = 0; i < 2; i++) {
		const bool primary = (i == 0);
		if (!prepareBaseSorter(primary, util::FalseType())) {
			continue;
		}

		const Pred &pred = (primary ? predPair_.first : predPair_.second);
		Sort op(*this);
		if (!acceptResult(
				pred.getTypeSwitcher().template get<Sort>()(op))) {
			return false;
		}
	}

	return true;
}

template<typename T, typename Pred, typename Alloc>
template<typename U>
bool SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::sortOptional(const U&) {
	for (size_t i = 0; i < 2; i++) {
		const bool primary = (i == 0);
		if (!prepareBaseSorter(primary, util::FalseType())) {
			continue;
		}

		const SortConfig &config = base_.getConfig();
		const Pred &pred = (primary ? predPair_.first : predPair_.second);

		bool sorted;
		if (config.unique_) {
			SortUnique op(*this);
			if (config.orderedUnique_) {
				sorted = pred.getTypeSwitcher().template get<SortUnique>()(op);
			}
			else {
				sorted = pred.getTypeSwitcher().toUnordered().template get<
						SortUnique>()(op);
			}
		}
		else {
			assert(!config.orderedUnique_);
			SortLimited op(*this);
			sorted = pred.getTypeSwitcher().template get<SortLimited>()(op);
		}

		if (!acceptResult(sorted)) {
			return false;
		}
	}

	return true;
}

template<typename T, typename Pred, typename Alloc>
template<typename Action>
bool SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::sortGroup(
		const Action &action) {
	for (size_t i = 0; i < 2; i++) {
		const bool primary = (i == 0);
		action(primary);

		if (!prepareBaseSorter(primary, action)) {
			continue;
		}

		const Pred &pred = (primary ? predPair_.first : predPair_.second);

		const SortConfig &config = base_.getConfig();
		assert(!config.orderedUnique_);
		static_cast<void>(config);

		typedef SortGroup<Action> Op;
		Op op(*this, action);
		const bool sorted =
				pred.getTypeSwitcher().toUnordered().template get<Op>()(op);

		if (!acceptResult(sorted)) {
			return false;
		}
	}

	return true;
}

template<typename T, typename Pred, typename Alloc>
bool SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::isOptional() const {
	const SortConfig &config = base_.getConfig();
	if (config.unique_) {
		return true;
	}
	else if (config.limit_ >= 0 &&
			(config.limit_ < endAt(true) - beginAt(true) ||
			config.limit_ < endAt(false) - beginAt(false))) {
		return true;
	}
	assert(!config.orderedUnique_);

	return false;
}

template<typename T, typename Pred, typename Alloc>
template<typename U>
bool SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::sortAt() {
	typedef typename Pred::template TypeAt<U>::TypedOp TypedPred;
	const bool primary = (sortingBounds_.second == NULL);
	const Pred &pred = (primary ? predPair_.first : predPair_.second);
	bool resultOnTmp;
	if (base_.runMain(TypedPred(pred), tmp_, primary, &resultOnTmp)) {
		if (resultOnTmp) {
			flip(primary);
		}
		return true;
	}
	return false;
}

template<typename T, typename Pred, typename Alloc>
template<typename U>
bool SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::sortUniqueAt() {
	typedef typename Pred::template TypeAt<U>::TypedOp TypedPred;
	const bool primary = (sortingBounds_.second == NULL);
	const Pred &pred = (primary ? predPair_.first : predPair_.second);

	bool resultOnTmp;
	(primary ? reducedSizes_.first : reducedSizes_.second) =
			base_.runMainUnique(TypedPred(pred), tmp_, primary, &resultOnTmp);
	if (resultOnTmp) {
		flip(primary);
	}
	return true;
}

template<typename T, typename Pred, typename Alloc>
template<typename U>
bool SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::sortLimitedAt() {
	typedef typename Pred::template TypeAt<U>::TypedOp TypedPred;
	const bool primary = (sortingBounds_.second == NULL);
	const Pred &pred = (primary ? predPair_.first : predPair_.second);

	int64_t &reduced = (primary ? reducedSizes_.first : reducedSizes_.second);
	return base_.runLimited(TypedPred(pred), reduced);
}

template<typename T, typename Pred, typename Alloc>
template<typename U, typename Action>
bool SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::sortGroupAt(
		const Action &action) {
	typedef typename Pred::template TypeAt<U>::TypedOp TypedPred;
	const bool primary = (sortingBounds_.second == NULL);
	const Pred &pred = (primary ? predPair_.first : predPair_.second);

	bool resultOnTmp;
	(primary ? reducedSizes_.first : reducedSizes_.second) =
			base_.runMainGroup(TypedPred(pred), action, tmp_, primary, &resultOnTmp);
	if (resultOnTmp) {
		flip(primary);
	}
	return true;
}

template<typename T, typename Pred, typename Alloc>
template<typename Action>
bool SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::prepareBaseSorter(
		bool primary, const Action &action) {
	do {
		BaseSorter nextBase(NULL, NULL);
		const SortConfig config = base_.getConfig();

		if (primary) {
			if (sortingBounds_.first == tail_) {
				if (sortingBounds_.second != NULL) {
					return false;
				}
				break;
			}

			sortingBounds_.first = tail_;
			sortingBounds_.second = NULL;

			if (!predEmpty_.first) {
				nextBase = BaseSorter(begin_, tail_);
			}
		}
		else {
			if (sortingBounds_.second == secondaryBegin_) {
				assert(sortingBounds_.first == tail_);
				break;
			}

			sortingBounds_.second = secondaryBegin_;
			if (!predEmpty_.second) {
				nextBase = BaseSorter(secondaryBegin_, end_);
			}
		}

		if (nextBase.isSorted() && (config.unique_ || config.limit_ >= 0)) {
			int64_t &reduced =
					(primary ? reducedSizes_.first : reducedSizes_.second);
			reduced = 0;

			const int64_t limit = (config.unique_ ? 1 : config.limit_);
			const int64_t size = endAt(primary) - beginAt(primary);

			if (config.unique_) {
				groupAll(primary, action);
			}

			reduced = size - std::min(size, limit);
		}

		base_ = nextBase;
		base_.setConfig(config);
	}
	while (false);

	return !base_.isSorted();
}

template<typename T, typename Pred, typename Alloc>
bool SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::acceptResult(bool sorted) {
	if (sorted) {
		BaseSorter nextBase(NULL, NULL);
		nextBase.setConfig(base_.getConfig());

		base_ = nextBase;
	}
	return sorted;
}

template<typename T, typename Pred, typename Alloc>
void SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::flip(bool primary) {
	const T *anotherSrcBegin = (primary ? secondaryBegin_ : begin_);
	const T *anotherSrcEnd = (primary ? end_ : tail_);
	T *anotherDestBegin;
	{
		T *tmpTail = toFlippedAddress(tail_);
		T *tmpEnd = toFlippedAddress(end_);
		T *tmpSecondaryBegin = toFlippedAddress(secondaryBegin_);

		std::pair<T*, T*> tmpSortingBounds(
				toFlippedAddress(sortingBounds_.first),
				toFlippedAddress(sortingBounds_.second));

		anotherDestBegin = (primary ? tmpSecondaryBegin : tmp_);

		std::swap(tmp_, begin_);
		std::swap(tmpTail, tail_);
		std::swap(tmpEnd, end_);
		std::swap(tmpSecondaryBegin, secondaryBegin_);
		std::swap(tmpSortingBounds, sortingBounds_);
	}

	{
		const T *srcIt = anotherSrcBegin;
		T *destIt = anotherDestBegin;
		for (; srcIt != anotherSrcEnd; ++srcIt, ++destIt) {
			*destIt = *srcIt;
		}
	}
}

template<typename T, typename Pred, typename Alloc>
T* SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::toFlippedAddress(T *src) const {
	if (src == NULL) {
		return NULL;
	}
	return tmp_ + (src - begin_);
}

template<typename T, typename Pred, typename Alloc>
template<typename Action>
void SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::groupAll(
		bool primary, const Action &action) {
	const T *it = beginAt(primary);
	const T *endIt = endAt(primary);
	if (it == endIt) {
		return;
	}

	T value = *it;
	action(value);

	while (++it != endIt) {
		T subValue = *it;
		action(value, subValue);
	}
}

template<typename T, typename Pred, typename Alloc>
void SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::groupAll(
		bool primary, const util::FalseType&) {
	static_cast<void>(primary);
}


template<typename T>
SQLAlgorithmUtils::HeapElement<T>::HeapElement(
		const T &value, size_t ordinal) :
		value_(value),
		ordinal_(ordinal) {
}

template<typename T>
inline const T& SQLAlgorithmUtils::HeapElement<T>::getValue() const {
	return value_;
}

template<typename T>
inline size_t SQLAlgorithmUtils::HeapElement<T>::getOrdinal() const {
	return ordinal_;
}

template<typename T>
inline bool SQLAlgorithmUtils::HeapElement<T>::next() const {
	return value_.next();
}

template<typename T>
template<typename U>
inline bool SQLAlgorithmUtils::HeapElement<T>::nextAt() const {
	return value_.template nextAt<U>();
}


template<typename T, typename Pred>
SQLAlgorithmUtils::HeapPredicate<T, Pred>::HeapPredicate(const Pred &base) :
		base_(base) {
}

template<typename T, typename Pred>
inline bool SQLAlgorithmUtils::HeapPredicate<T, Pred>::operator()(
		const HeapElement<T> &e1, const HeapElement<T> &e2) const {
	return base_(e1.getValue(), e2.getValue());
}


template<typename T, typename Pred, typename Alloc>
SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::HeapQueue(
		const Pred &basePred, const Alloc &alloc) :
		elemList_(alloc),
		pred_(basePred),
		predEmpty_(false) {
}

template<typename T, typename Pred, typename Alloc>
bool SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::isEmpty() const {
	return elemList_.empty();
}

template<typename T, typename Pred, typename Alloc>
bool SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::isPredicateEmpty() const {
	return predEmpty_;
}

template<typename T, typename Pred, typename Alloc>
void SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::setPredicateEmpty(
		bool empty) {
	predEmpty_ = empty;
}

template<typename T, typename Pred, typename Alloc>
typename SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::Element
SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::newElement(const T &value) {
	return Element(value, elemList_.size());
}

template<typename T, typename Pred, typename Alloc>
void SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::push(const Element &elem) {

	Push op(*this, elem);
	pred_.getBase().getTypeSwitcher().template get<const Push>()(op);
}

template<typename T, typename Pred, typename Alloc>
typename SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::Element
SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::pop() {
	std::pop_heap(elemList_.begin(), elemList_.end(), pred_);
	const Element elem = elemList_.back();
	elemList_.pop_back();
	return elem;
}

template<typename T, typename Pred, typename Alloc>
template<typename A>
bool SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::merge(A &action) {
	if (isEmpty()) {
		return true;
	}
	typedef Merge<A> Op;
	Op op(*this, action);
	return pred_.getBase().getTypeSwitcher().template getWithOptions<Op>()(op);
}

template<typename T, typename Pred, typename Alloc>
template<typename A>
bool SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::mergeUnique(A &action) {
	if (isEmpty()) {
		return true;
	}
	typedef MergeUnique<A> Op;
	Op op(*this, action);
	return pred_.getBase().getTypeSwitcher().toUnordered(
			).template getWithOptions<Op>()(op);
}

template<typename T, typename Pred, typename Alloc>
template<typename A>
bool SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::mergeOrderedUnique(
		A &action) {
	if (isEmpty()) {
		return true;
	}
	typedef MergeUnique<A> Op;
	Op op(*this, action);
	return pred_.getBase().getTypeSwitcher().template getWithOptions<Op>()(op);
}

template<typename T, typename Pred, typename Alloc>
template<typename A>
bool SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::mergeLimited(A &action) {
	if (isEmpty()) {
		return true;
	}
	typedef MergeLimited<A> Op;
	Op op(*this, action);
	return pred_.getBase().getTypeSwitcher().template getWithOptions<Op>()(op);
}

template<typename T, typename Pred, typename Alloc>
template<typename A>
bool SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::mergeUnchecked(A &action) {
	assert(elemList_.size() == 1 || isPredicateEmpty());
	while (!elemList_.empty()) {
		const Element &elem = elemList_.back();
		for (;;) {
			const bool continuable = action(elem);
			const bool exists = elem.next();

			if (!continuable) {
				return false;
			}

			if (!exists) {
				break;
			}
		}
		elemList_.pop_back();
	}
	return true;
}

template<typename T, typename Pred, typename Alloc>
template<typename A>
bool SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::mergeUncheckedUnique(A &action) {
	assert(isPredicateEmpty());
	while (!elemList_.empty()) {
		const Element &elem = elemList_.back();

		if (elemList_.size() > 1) {
			action(elem, util::FalseType());
		}
		else {
			action(elem, util::FalseType());
			action(elem, util::TrueType());
		}

		if (!elem.next()) {
			elemList_.pop_back();
		}
		else {
			assert(false);
		}
	}
	return true;
}

template<typename T, typename Pred, typename Alloc>
template<typename U>
void SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::pushAt(
		const Element &elem) {
	typedef typename Pred::template TypeAt<U>::TypedOp TypedPred;
	HeapPredicate<T, TypedPred> pred(TypedPred(pred_.getBase()));
	elemList_.push_back(elem);
	std::push_heap(elemList_.begin(), elemList_.end(), pred);
}

template<typename T, typename Pred, typename Alloc>
template<typename U>
typename SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::Element
SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::popAt() {
	typedef typename Pred::template TypeAt<U>::TypedOp TypedPred;
	HeapPredicate<T, TypedPred> pred(TypedPred(pred_.getBase()));
	std::pop_heap(elemList_.begin(), elemList_.end(), pred);
	const Element elem = elemList_.back();
	elemList_.pop_back();
	return elem;
}

template<typename T, typename Pred, typename Alloc>
template<typename P>
inline void SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::pushAtTmp(
		const P &typedPred) {
	std::push_heap(elemList_.begin(), elemList_.end(), typedPred);
}

template<typename T, typename Pred, typename Alloc>
template<typename P>
inline const typename SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::Element&
SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::popAtTmp(const P &typedPred) {
	std::pop_heap(elemList_.begin(), elemList_.end(), typedPred);
	return elemList_.back();
}

template<typename T, typename Pred, typename Alloc>
template<typename U, typename A>
bool SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::mergeAt(A &action) {
	assert(!isEmpty());

	typedef typename Pred::template TypeAt<U>::TypedOp TypedPred;
	typedef typename A::template TypeAt<U>::TypedOp TypedAction;

	const HeapPredicate<T, TypedPred> typedPred(TypedPred(pred_.getBase()));

	typename ElementList::iterator it = elemList_.end();
	for (;;) {
		std::pop_heap(elemList_.begin(), it, typedPred);

		const bool continuable = (TypedAction(action))(*(--it));
		const bool exists = it->next();

		static_cast<void>(continuable);

		if (!exists) {
			if (it == elemList_.begin()) {
				break;
			}
			continue;
		}
		std::push_heap(elemList_.begin(), ++it, typedPred);
	}
	return true;
}

template<typename T, typename Pred, typename Alloc>
template<typename U, typename A>
bool SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::mergeUniqueAt(A &action) {
	assert(!isEmpty());

	typedef typename Pred::template TypeAt<U>::TypedOp TypedPred;
	typedef typename A::template TypeAt<U>::TypedOp TypedAction;

	const HeapPredicate<T, TypedPred> typedPred(TypedPred(pred_.getBase()));

	typename ElementList::iterator it = elemList_.end();
	if (it - elemList_.begin() > 1) {
		for (;;) {
			std::pop_heap(elemList_.begin(), it, typedPred);

			const bool continuable = (TypedAction(action))(*(--it), util::FalseType());

			if ((TypedAction(action))(*it, util::TrueType(), typedPred) &&
					typedPred(*elemList_.begin(), *it)) {
				(TypedAction(action))(*it, util::TrueType());
			}

			const bool exists = it->next();

			if (!continuable) {
				return false;
			}

			if (!exists) {
				if (it - elemList_.begin() <= 1) {
					break;
				}
				continue;
			}

			std::push_heap(elemList_.begin(), ++it, typedPred);
		}
	}

	assert(it - elemList_.begin() == 1);
	{
		const bool single =
				(TypedAction(action))(*(--it), util::TrueType(), util::TrueType());
		for (;;) {
			const bool continuable = (TypedAction(action))(*it, util::FalseType());

			if ((TypedAction(action))(*it, util::TrueType(), typedPred)) {
				(TypedAction(action))(*it, util::TrueType());
			}

			if (!it->next() || !single) {
				break;
			}

			if (!continuable) {
				return false;
			}
		}
	}

	return true;
}

template<typename T, typename Pred, typename Alloc>
template<typename U, typename A>
bool SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::mergeLimitedAt(A &action) {
	assert(!isEmpty());

	typedef typename Pred::template TypeAt<U>::TypedOp TypedPred;
	typedef typename A::template TypeAt<U>::TypedOp TypedAction;

	const HeapPredicate<T, TypedPred> typedPred(TypedPred(pred_.getBase()));

	typename ElementList::iterator it = elemList_.end();
	for (;;) {
		std::pop_heap(elemList_.begin(), it, typedPred);

		const bool continuable = (TypedAction(action))(*(--it));

		const bool exists = it->next();

		if (!continuable) {
			return false;
		}

		if (!exists) {
			if (it == elemList_.begin()) {
				break;
			}
			continue;
		}
		std::push_heap(elemList_.begin(), ++it, typedPred);
	}
	return true;
}


template<typename Alloc>
SQLAlgorithmUtils::DenseBitSet<Alloc>::DenseBitSet(const allocator_type &alloc) :
		base_(0, BaseHasher(), BasePred(), alloc) {
}

template<typename Alloc>
inline std::pair<typename SQLAlgorithmUtils::DenseBitSet<Alloc>::Iterator, bool>
SQLAlgorithmUtils::DenseBitSet<Alloc>::insert(const value_type &val) {
	BitsType &bits = base_[val & Constants::SET_KEY_MASK];
	const BitsType valueBit =
			(Constants::SET_BITS_BASE << (val & Constants::SET_BITS_MASK));

	if ((bits & valueBit) != 0) {
		return std::pair<Iterator, bool>(Iterator(true), false);
	}

	bits |= valueBit;
	return std::pair<Iterator, bool>(Iterator(true), true);
}

template<typename Alloc>
inline typename SQLAlgorithmUtils::DenseBitSet<Alloc>::Iterator
SQLAlgorithmUtils::DenseBitSet<Alloc>::find(const key_type &k) {
	typename Base::iterator it = base_.find(k & Constants::SET_KEY_MASK);

	if (it == base_.end()) {
		return Iterator(false);
	}

	const BitsType &bits = it->second;
	const BitsType valueBit =
			(Constants::SET_BITS_BASE << (k & Constants::SET_BITS_MASK));

	return Iterator((bits & valueBit) != 0);
}

template<typename Alloc>
inline typename SQLAlgorithmUtils::DenseBitSet<Alloc>::Iterator
SQLAlgorithmUtils::DenseBitSet<Alloc>::end() {
	return Iterator();
}

template<typename Alloc>
size_t SQLAlgorithmUtils::DenseBitSet<Alloc>::capacity() const {
	return base_.size();
}


template<typename Alloc>
inline SQLAlgorithmUtils::DenseBitSet<Alloc>::Iterator::Iterator(bool found) :
		found_(found) {
}

template<typename Alloc>
inline bool SQLAlgorithmUtils::DenseBitSet<Alloc>::Iterator::operator!=(
		const Iterator &another) const {
	return (!found_ != !another.found_);
}

template<typename Alloc>
inline bool SQLAlgorithmUtils::DenseBitSet<Alloc>::Iterator::operator==(
		const Iterator &another) const {
	return (!found_ == !another.found_);
}


template<typename Alloc>
SQLAlgorithmUtils::DenseBitSet<Alloc>::InputCursor::InputCursor(
		DenseBitSet &bitSet) :
		bitSet_(bitSet),
		baseIt_(bitSet_.base_.begin()) {
}

template<typename Alloc>
inline bool SQLAlgorithmUtils::DenseBitSet<Alloc>::InputCursor::next(
		CodedEntry &entry) {
	if (baseIt_ == bitSet_.base_.end()) {
		return false;
	}

	entry = CodedEntry(
			static_cast<CodedEntry::first_type>(baseIt_->first),
			static_cast<CodedEntry::second_type>(baseIt_->second));
	++baseIt_;

	return true;
}


template<typename Alloc>
SQLAlgorithmUtils::DenseBitSet<Alloc>::OutputCursor::OutputCursor(
		DenseBitSet &bitSet) :
		bitSet_(bitSet) {
}

template<typename Alloc>
inline void SQLAlgorithmUtils::DenseBitSet<Alloc>::OutputCursor::next(
		const CodedEntry &entry) {
	bitSet_.base_.insert(std::make_pair(
			static_cast<BaseKey>(entry.first),
			static_cast<BitsType>(entry.second)));
}

#endif
