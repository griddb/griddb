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
			typename K, typename V, typename Hasher, typename Pred,
			typename Alloc> class HashMap;
	template<
			typename K, typename V, typename Hasher, typename Pred,
			typename Alloc> class HashSet;
};

template<typename It>
class SQLAlgorithmUtils::CombSorter {
public:
	CombSorter(It begin, It end);

	void setInterval(ptrdiff_t interval);
	void setConfig(const CombSorter &src);

	bool isSorted() const;

	template<typename Pred>
	bool run(const Pred &pred);

private:
	enum Constants {
		SORT_SHRINK_RATE = 13,
		SORT_SHRINK_BASE = 10
	};

	It begin_;
	It end_;

	ptrdiff_t gap_;
	bool sorted_;

	ptrdiff_t interval_;
	It middle_;
};

template<typename T, typename Pred, typename Alloc>
class SQLAlgorithmUtils::Sorter {
public:
	typedef bool RetType;

	template<typename U> class TypeAt {
	public:
		explicit TypeAt(Sorter &base) : base_(base) {}

		bool operator()() const {
			return base_.template sortAt<U>();
		}

	private:
		Sorter &base_;
	};

	typedef const T* Iterator;

	Sorter(size_t capacity, const Pred &pred, const Alloc &alloc);

	void setInterval(ptrdiff_t interval);

	void add(const T &elem);
	void clear();

	bool isEmpty() const;
	bool isFilled() const;
	bool isSorted() const;

	Iterator begin() const;
	Iterator end() const;

	bool sort();
private:
	typedef CombSorter<T*> BaseSorter;

	template<typename U> bool sortAt();

	BaseSorter base_;
	Pred pred_;

	util::XArray<T, Alloc> elemList_;
	T *begin_;
	T *tail_;
	T *end_;
	T *sortingTail_;
};

template<typename T>
class SQLAlgorithmUtils::HeapElement {
public:
	HeapElement(const T &value, size_t ordinal);

	const T& getValue() const;
	size_t getOrdinal() const;

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

	const Pred& getBase() { return base_; }

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
	HeapQueue(const Pred &basePred, const Alloc &alloc);

	bool isEmpty() const;

	Element newElement(const T &value);

	void push(const Element &elem);
	Element pop();

private:
	template<typename U> void pushAt(const Element &elem);
	template<typename U> Element popAt();

	util::Vector<Element> elemList_;
	HeapPredicate<T, Pred> pred_;
};



template<typename It>
SQLAlgorithmUtils::CombSorter<It>::CombSorter(It begin, It end) :
		begin_(begin),
		end_(end),
		gap_(end - begin),
		sorted_(false),
		interval_(std::numeric_limits<ptrdiff_t>::max()),
		middle_(end) {
	assert(gap_ >= 0);
}

template<typename It>
void SQLAlgorithmUtils::CombSorter<It>::setInterval(ptrdiff_t interval) {
	assert(interval > 0);
	interval_ = interval;
}

template<typename It>
void SQLAlgorithmUtils::CombSorter<It>::setConfig(const CombSorter &src) {
	setInterval(src.interval_);
}

template<typename It>
bool SQLAlgorithmUtils::CombSorter<It>::isSorted() const {
	return sorted_;
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
		middle_ = gapIt + static_cast<ptrdiff_t>(
				std::min<int64_t>(interval_, (end_ - gapIt)));
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


template<typename T, typename Pred, typename Alloc>
SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::Sorter(
		size_t capacity, const Pred &pred, const Alloc &alloc) :
		base_(NULL, NULL),
		pred_(pred),
		elemList_(alloc),
		begin_(NULL),
		tail_(NULL),
		end_(NULL),
		sortingTail_(NULL) {
	elemList_.resize(capacity);
	begin_ = elemList_.data();
	tail_ = begin_;
	end_ = tail_ + elemList_.size();
}

template<typename T, typename Pred, typename Alloc>
void SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::setInterval(
		ptrdiff_t interval) {
	base_.setInterval(interval);
}

template<typename T, typename Pred, typename Alloc>
void SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::clear() {
	base_ = BaseSorter(NULL, NULL);
	tail_ = begin_;
	sortingTail_ = NULL;
}

template<typename T, typename Pred, typename Alloc>
void SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::add(const T &elem) {
	assert(tail_ != end_);
	new (tail_) T(elem);
	++tail_;
}

template<typename T, typename Pred, typename Alloc>
bool SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::isEmpty() const {
	return (begin_ == tail_);
}

template<typename T, typename Pred, typename Alloc>
bool SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::isFilled() const {
	return (tail_ == end_);
}

template<typename T, typename Pred, typename Alloc>
bool SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::isSorted() const {
	return (tail_ == sortingTail_ && base_.isSorted());
}

template<typename T, typename Pred, typename Alloc>
typename SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::Iterator
SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::begin() const {
	return begin_;
}

template<typename T, typename Pred, typename Alloc>
typename SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::Iterator
SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::end() const {
	return tail_;
}

template<typename T, typename Pred, typename Alloc>
bool SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::sort() {
	return pred_.getTypeSwitcher()(*this);
}

template<typename T, typename Pred, typename Alloc>
template<typename U>
bool SQLAlgorithmUtils::Sorter<T, Pred, Alloc>::sortAt() {
	typedef typename Pred::template TypeAt<U> TypedPred;
	if (tail_ != sortingTail_) {
		sortingTail_ = tail_;

		const BaseSorter org = base_;
		base_ = BaseSorter(begin_, tail_);
		base_.setConfig(org);
	}

	return base_.run(TypedPred(pred_));
}


template<typename T>
SQLAlgorithmUtils::HeapElement<T>::HeapElement(
		const T &value, size_t ordinal) :
		value_(value),
		ordinal_(ordinal) {
}

template<typename T>
const T& SQLAlgorithmUtils::HeapElement<T>::getValue() const {
	return value_;
}

template<typename T>
size_t SQLAlgorithmUtils::HeapElement<T>::getOrdinal() const {
	return ordinal_;
}


template<typename T, typename Pred>
SQLAlgorithmUtils::HeapPredicate<T, Pred>::HeapPredicate(const Pred &base) :
		base_(base) {
}

template<typename T, typename Pred>
bool SQLAlgorithmUtils::HeapPredicate<T, Pred>::operator()(
		const HeapElement<T> &e1, const HeapElement<T> &e2) const {
	return base_(e1.getValue(), e2.getValue());
}


template<typename T, typename Pred, typename Alloc>
SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::HeapQueue(
		const Pred &basePred, const Alloc &alloc) :
		elemList_(alloc),
		pred_(basePred) {
}

template<typename T, typename Pred, typename Alloc>
bool SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::isEmpty() const {
	return elemList_.empty();
}

template<typename T, typename Pred, typename Alloc>
typename SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::Element
SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::newElement(const T &value) {
	return Element(value, elemList_.size());
}

template<typename T, typename Pred, typename Alloc>
void SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::push(const Element &elem) {
	Push op(*this, elem);
	pred_.getBase().getTypeSwitcher()(op);
}

template<typename T, typename Pred, typename Alloc>
typename SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::Element
SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::pop() {
	Pop op(*this);
	return pred_.getBase().getTypeSwitcher()(op);
}

template<typename T, typename Pred, typename Alloc>
template<typename U>
void SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::pushAt(
		const Element &elem) {
	typedef typename Pred::template TypeAt<U> TypedPred;
	HeapPredicate<T, TypedPred> pred(TypedPred(pred_.getBase()));
	elemList_.push_back(elem);
	std::push_heap(elemList_.begin(), elemList_.end(), pred);
}

template<typename T, typename Pred, typename Alloc>
template<typename U>
typename SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::Element
SQLAlgorithmUtils::HeapQueue<T, Pred, Alloc>::popAt() {
	typedef typename Pred::template TypeAt<U> TypedPred;
	HeapPredicate<T, TypedPred> pred(TypedPred(pred_.getBase()));
	std::pop_heap(elemList_.begin(), elemList_.end(), pred);
	const Element elem = elemList_.back();
	elemList_.pop_back();
	return elem;
}

#endif
