/*
	Copyright (c) 2013 TOSHIBA CORPORATION.

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
	@brief Definition and Implementation of ExpirableMap
*/
#ifndef EXPIRABLE_MAP_H_
#define EXPIRABLE_MAP_H_

#include "util/allocator.h"
#include "util/time.h"
#include "gs_error.h"

#include <iostream>


#define EM_ASSERT(cond) assert(cond)

namespace util {
/*!
	@brief Represents ExpirableMap(map with timeout control)
*/
template <typename K, typename V, typename T, typename Hash>
class ExpirableMap {
public:
	class Manager;
	class Cursor;

	V &create(K key, const T &timeout);
	V &createNoExpire(K key);

	V *get(K key);

	V *update(K key, const T &timeout);

	void remove(K key);

	V *refresh(const T &now, K *&key);

	Cursor getCursor();

	size_t size();

private:
	struct Link {
		Link *next_;
		Link *prev_;

		Link() : next_(NULL), prev_(NULL) {}
		~Link() {}
	};

	static const int32_t HASH_LINK = 0;
	static const int32_t TIMER_LINK = 1;

	struct Entry {
		Link link_[2];
		T timeout_;
		K key_;
		V value_;

		bool connected(int32_t linkType) const {
			return (link_[linkType].prev_ != NULL);
		}
	};

	static Entry *getEntry(int32_t linkType, Link *link) {
		return reinterpret_cast<Entry *>(link - linkType);
	}

#define LINK(linkType) link_[linkType]
#define LINK_NEXT(linkType) link_[linkType].next_
#define LINK_PREV(linkType) link_[linkType].prev_

	ExpirableMap(util::ObjectPool<Entry> &pool, size_t hashSize, uint32_t max,
		uint32_t interval);
	~ExpirableMap();

	Entry *create();

	void remove(Entry *entry);

	void insertHash(Entry *entry);

	Entry *getHash(K key);

	Entry *removeHash(K key);

	void insertTimer(Entry *entry, const T &timeout);
	void removeTimer(Entry *entry);

	void erase(int32_t linkType, Entry *entry);

	Entry *find(K key, Link *top);

	void push_front(int32_t linkType, Entry *entry, Link *top);
	Entry *pop_front(int32_t linkType, Link *top);
	void insert(Entry *entry, Link *top);

	void push_front(int32_t linkType, Entry *entry, Link *top, Link *tail);
	Entry *pop_front(int32_t linkType, Link *top, Link *tail);
	void insert(Entry *entry, Link *top, Link *tail);
	void append(Link *list, Link *top, Link *tail);

	ObjectPool<Entry> &pool_;

	Hash hash_;

	const size_t hashSize_;
	const uint32_t max_;
	const uint32_t interval_;
	const size_t timerSize_;

	T currentTime_;

	Link *hashTable_;

	Link *timerRing_;
	size_t timerTop_;  
	size_t timerBottom_;

	Link overList_;  

	Link timeoutListBegin_;  
	Link timeoutListEnd_;  

};

/*!
	@brief Represents Manager of ExpirableMap
*/
template <typename K, typename V, typename T, typename Hash>
class ExpirableMap<K, V, T, Hash>::Manager {
public:
	Manager(const util::AllocatorInfo &info);
	~Manager();

	ExpirableMap *create(size_t hashSize, uint32_t max, uint32_t interval);

	void remove(ExpirableMap *&map);

	void setTotalElementLimit(size_t limit);
	void setFreeElementLimit(size_t limit);

	size_t getElementSize();
	size_t getElementCount();
	size_t getFreeElementCount();

private:
	util::ObjectPool<Entry> pool_;
};

template <typename K, typename V, typename T, typename Hash>
ExpirableMap<K, V, T, Hash>::Manager::Manager(const util::AllocatorInfo &info)
	: pool_(info) {
	pool_.setFreeElementLimit(0);
}

template <typename K, typename V, typename T, typename Hash>
ExpirableMap<K, V, T, Hash>::Manager::~Manager() {}

/*!
	@brief Creates ExpirableMap
*/
template <typename K, typename V, typename T, typename Hash>
ExpirableMap<K, V, T, Hash> *ExpirableMap<K, V, T, Hash>::Manager::create(
	size_t hashSize, uint32_t max, uint32_t interval) {
	return UTIL_NEW ExpirableMap(pool_, hashSize, max, interval);
}

/*!
	@brief Removes ExpirableMap
*/
template <typename K, typename V, typename T, typename Hash>
void ExpirableMap<K, V, T, Hash>::Manager::remove(ExpirableMap *&map) {
	delete map;
	map = NULL;
}

/*!
	@brief Sets maximum number of key-value pair in all map
*/
template <typename K, typename V, typename T, typename Hash>
void ExpirableMap<K, V, T, Hash>::Manager::setTotalElementLimit(size_t limit) {
	pool_.setTotalElementLimit(limit);
}

/*!
	@brief Sets maximum number of free key-value pair in all map
*/
template <typename K, typename V, typename T, typename Hash>
void ExpirableMap<K, V, T, Hash>::Manager::setFreeElementLimit(size_t limit) {
	pool_.setFreeElementLimit(limit);
}

/*!
	@brief Gets key-value pair size
*/
template <typename K, typename V, typename T, typename Hash>
size_t ExpirableMap<K, V, T, Hash>::Manager::getElementSize() {
	return pool_.base().getElementSize();
}

/*!
	@brief Gets the number of key-value pair in use in all map
*/
template <typename K, typename V, typename T, typename Hash>
size_t ExpirableMap<K, V, T, Hash>::Manager::getElementCount() {
	return pool_.getTotalElementCount() - pool_.getFreeElementCount();
}

/*!
	@brief Gets the number of free key-value pair in all map
*/
template <typename K, typename V, typename T, typename Hash>
size_t ExpirableMap<K, V, T, Hash>::Manager::getFreeElementCount() {
	return pool_.getFreeElementCount();
}

/*!
	@brief Represents cursor to scan ExpirableMap
*/
template <typename K, typename V, typename T, typename Hash>
class ExpirableMap<K, V, T, Hash>::Cursor {
	friend class ExpirableMap<K, V, T, Hash>;

public:
	~Cursor();

	Cursor(const Cursor &c);

	Cursor &operator=(const Cursor &c);

	V *next();

private:
	Cursor(ExpirableMap<K, V, T, Hash> &map);

	ExpirableMap<K, V, T, Hash> &map_;
	size_t pos_;
	Link *link_;
	int32_t linkType_;
};

template <typename K, typename V, typename T, typename Hash>
ExpirableMap<K, V, T, Hash>::Cursor::Cursor(ExpirableMap<K, V, T, Hash> &map)
	: map_(map),
	  pos_(0),
	  link_(map_.hashTable_[pos_].next_),
	  linkType_(HASH_LINK) {}

template <typename K, typename V, typename T, typename Hash>
ExpirableMap<K, V, T, Hash>::Cursor::Cursor(const Cursor &c)
	: map_(c.map_), pos_(c.pos_), link_(c.link_), linkType_(c.linkType_) {}

template <typename K, typename V, typename T, typename Hash>
typename ExpirableMap<K, V, T, Hash>::Cursor &
ExpirableMap<K, V, T, Hash>::Cursor::operator=(const Cursor &c) {
	if (this == &c) {
		return *this;
	}
	map_ = c.map_;
	pos_ = c.pos_;
	link_ = c.link_;
	linkType_ = c.linkType_;
	return *this;
}

template <typename K, typename V, typename T, typename Hash>
ExpirableMap<K, V, T, Hash>::Cursor::~Cursor() {}

/*!
	@brief Gets next value
*/
template <typename K, typename V, typename T, typename Hash>
V *ExpirableMap<K, V, T, Hash>::Cursor::next() {
	while (link_ == NULL && pos_ < map_.hashSize_) {
		pos_++;
		if (pos_ < map_.hashSize_) {
			link_ = map_.hashTable_[pos_].next_;
		}
		else {
			link_ = NULL;
		}
	}

	if (link_ != NULL) {
		Entry *entry = getEntry(linkType_, link_);
		link_ = link_->next_;
		return &(entry->value_);
	}
	else {
		return NULL;
	}
}

template <typename K, typename V, typename T, typename Hash>
ExpirableMap<K, V, T, Hash>::ExpirableMap(util::ObjectPool<Entry> &pool,
	size_t hashSize, uint32_t max, uint32_t interval)
	: pool_(pool),
	  hashSize_(hashSize),
	  max_(max),
	  interval_(interval),
	  timerSize_(max_ / interval_),
	  currentTime_(0),
	  hashTable_(UTIL_NEW Link[hashSize_]),
	  timerRing_(UTIL_NEW Link[timerSize_]),
	  timerTop_(0),
	  timerBottom_(timerSize_ - 1)
{
	timeoutListEnd_.prev_ = &timeoutListBegin_;
	timeoutListBegin_.next_ = &timeoutListEnd_;

}

template <typename K, typename V, typename T, typename Hash>
ExpirableMap<K, V, T, Hash>::~ExpirableMap() {
	for (size_t i = 0; i < hashSize_; i++) {
		Link *link = hashTable_[i].next_;
		while (link != NULL) {
			Entry *entry = getEntry(HASH_LINK, link);
			link = link->next_;
			removeHash(entry->key_);
			remove(entry);
		}


		hashTable_[i].next_ = NULL;
	}

	delete[] hashTable_;
	delete[] timerRing_;
}

/*!
	@brief Creates key-value pair with setting timeout
*/
template <typename K, typename V, typename T, typename Hash>
V &ExpirableMap<K, V, T, Hash>::create(K key, const T &timeout) {
	Entry *entry = NULL;

	try {
		entry = create();
		entry->key_ = key;
		insertTimer(entry, timeout);
		insertHash(entry);

		return entry->value_;
	}
	catch (std::exception &e) {
		removeHash(key);
		removeTimer(entry);
		remove(entry);

		GS_RETHROW_USER_ERROR(e, "");
	}
}

/*!
	@brief Creates key-value pair without setting timeout
*/
template <typename K, typename V, typename T, typename Hash>
V &ExpirableMap<K, V, T, Hash>::createNoExpire(K key) {
	Entry *entry = NULL;

	try {
		entry = create();
		entry->key_ = key;
		insertHash(entry);

		return entry->value_;
	}
	catch (std::exception &e) {
		removeHash(key);
		remove(entry);

		GS_RETHROW_USER_ERROR(e, "");
	}
}

/*!
	@brief Updates timeout of the value with a specified key
*/
template <typename K, typename V, typename T, typename Hash>
V *ExpirableMap<K, V, T, Hash>::update(K key, const T &timeout) {
	try {
		Entry *entry = getHash(key);

		if (entry != NULL) {
			removeTimer(entry);
			insertTimer(entry, timeout);

			return &(entry->value_);
		}
		else {
			return NULL;
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
}

/*!
	@brief Gets value with a specified key
*/
template <typename K, typename V, typename T, typename Hash>
V *ExpirableMap<K, V, T, Hash>::get(K key) {
	try {
		Entry *entry = getHash(key);

		if (entry != NULL) {
			return &(entry->value_);
		}
		else {
			return NULL;
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
}

/*!
	@brief Removes value with a specified key
*/
template <typename K, typename V, typename T, typename Hash>
void ExpirableMap<K, V, T, Hash>::remove(K key) {
	try {
		Entry *entry = removeHash(key);
		removeTimer(entry);
		remove(entry);
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
}

/*!
	@brief Gets key-value pair which is timed out before a specified time
*/
template <typename K, typename V, typename T, typename Hash>
V *ExpirableMap<K, V, T, Hash>::refresh(const T &now, K *&key) {
	try {
		const int32_t linkType = TIMER_LINK;

		const T timeDiff = now - currentTime_;

		if (timeDiff >= 0) {
			const size_t diff = static_cast<size_t>(timeDiff / interval_);
			const size_t nowPos = timerTop_ + diff;

			if (nowPos - timerTop_ < timerSize_) {
				for (size_t pos = timerTop_; pos <= nowPos; pos++) {
					Link *list = timerRing_[pos % timerSize_].next_;

					if (list == NULL) {
						continue;
					}


					timerRing_[pos % timerSize_].next_ = NULL;

					Link *top = &timeoutListBegin_;
					Link *tail = &timeoutListEnd_;


					append(list, top, tail);

				}

				currentTime_ = now;
				timerBottom_ = (timerBottom_ + diff) % timerSize_;
				timerTop_ = nowPos % timerSize_;
			}
			else {
				for (size_t pos = timerTop_; pos < timerTop_ + timerSize_;
					 pos++) {
					Link *list = timerRing_[pos % timerSize_].next_;

					if (list == NULL) {
						continue;
					}

					timerRing_[pos % timerSize_].next_ = NULL;

					Link *top = &timeoutListBegin_;
					Link *tail = &timeoutListEnd_;


					append(list, top, tail);

				}

				{
					Link *listTop = overList_.next_;
					Link *listTail = NULL;
					Link *target = overList_.next_;

					while (listTop != NULL && target != NULL) {
						Entry *entry = getEntry(linkType, target);
						if (entry->timeout_ > now) {
							break;
						}
						else {
							listTail = target;
							target = target->next_;
						}
					}

					if (listTop != NULL && listTail != NULL) {
						listTop->prev_ = NULL;
						listTail->next_ = NULL;

						overList_.next_ = target;
						if (target != NULL) {
							target->prev_ = &overList_;
						}

						Link *top = &timeoutListBegin_;
						Link *tail = &timeoutListEnd_;


						append(listTop, top, tail);

					}
				}

				currentTime_ = now;
				timerBottom_ = timerSize_ - 1;
				timerTop_ = 0;
			}

			while (overList_.next_ != NULL) {
				Entry *entry = getEntry(linkType, overList_.next_);

				const T timeDiff = entry->timeout_ - currentTime_;
				assert(timeDiff >= 0);
				const size_t diff = static_cast<size_t>(timeDiff / interval_);
				const size_t pos = timerTop_ + diff;

				if (pos - timerTop_ < timerSize_) {

					erase(linkType, entry);


					Link *top = &timerRing_[pos % timerSize_];


					push_front(linkType, entry, top);

				}
				else {
					break;
				}
			}
		}

		{
			Link *top = &timeoutListBegin_;
			Link *tail = &timeoutListEnd_;

			if (top->next_ != tail) {
				Entry *entry = getEntry(linkType, top->next_);

				if (entry->timeout_ <= now) {
					entry = pop_front(linkType, top, tail);
					key = &(entry->key_);
					return &(entry->value_);
				}
				else {
					return NULL;
				}
			}
			else {
				return NULL;
			}
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, "");
	}
}

/*!
	@brief Gets a Cursor to scan this map
*/
template <typename K, typename V, typename T, typename Hash>
typename ExpirableMap<K, V, T, Hash>::Cursor
ExpirableMap<K, V, T, Hash>::getCursor() {
	return Cursor(*this);
}

/*!
	@brief Gets the number of key-value pair in this map
*/
template <typename K, typename V, typename T, typename Hash>
size_t ExpirableMap<K, V, T, Hash>::size() {
	return pool_.getTotalElementCount() - pool_.getFreeElementCount();
}

template <typename K, typename V, typename T, typename Hash>
typename ExpirableMap<K, V, T, Hash>::Entry *
ExpirableMap<K, V, T, Hash>::create() {
	return UTIL_OBJECT_POOL_NEW(pool_) Entry;
}

template <typename K, typename V, typename T, typename Hash>
void ExpirableMap<K, V, T, Hash>::remove(Entry *entry) {
	UTIL_OBJECT_POOL_DELETE(pool_, entry);
}

template <typename K, typename V, typename T, typename Hash>
void ExpirableMap<K, V, T, Hash>::insertHash(Entry *entry) {

	if (entry == NULL) {
		return;
	}

	const int32_t linkType = HASH_LINK;

	if (getHash(entry->key_) == NULL) {
		const size_t hashValue = hash_(entry->key_);
		Link *link = &(hashTable_[hashValue % hashSize_]);

		push_front(linkType, entry, link);

	}
	else {
		UTIL_THROW_ERROR(0, "");
	}
}

template <typename K, typename V, typename T, typename Hash>
typename ExpirableMap<K, V, T, Hash>::Entry *
ExpirableMap<K, V, T, Hash>::getHash(K key) {

	const size_t hashValue = hash_(key);
	Link *top = &hashTable_[hashValue % hashSize_];

	return find(key, top);
}

template <typename K, typename V, typename T, typename Hash>
typename ExpirableMap<K, V, T, Hash>::Entry *
ExpirableMap<K, V, T, Hash>::removeHash(K key) {

	const int32_t linkType = HASH_LINK;

	Entry *entry = getHash(key);

	if (entry != NULL) {
		erase(linkType, entry);

	}

	return entry;
}

template <typename K, typename V, typename T, typename Hash>
void ExpirableMap<K, V, T, Hash>::insertTimer(Entry *entry, const T &timeout) {
	if (entry == NULL) {
		return;
	}

	const int32_t linkType = TIMER_LINK;

	const T timeDiff = timeout - currentTime_;

	entry->timeout_ = timeout;

	if (timeDiff >= 0) {

		const size_t diff = static_cast<size_t>(timeDiff / interval_);
		const size_t pos = timerTop_ + diff;

		if (pos - timerTop_ < timerSize_) {
			Link *top = &(timerRing_[pos % timerSize_]);


			push_front(linkType, entry, top);

		}
		else {
			Link *top = &overList_;


			insert(entry, top);

		}
	}
	else {

		Link *top = &timeoutListBegin_;
		Link *tail = &timeoutListEnd_;


		insert(entry, top, tail);

	}
}

template <typename K, typename V, typename T, typename Hash>
void ExpirableMap<K, V, T, Hash>::removeTimer(Entry *entry) {

	const int32_t linkType = TIMER_LINK;

	if (entry == NULL || !entry->connected(linkType)) {
		return;
	}

	erase(linkType, entry);

}

template <typename K, typename V, typename T, typename Hash>
void ExpirableMap<K, V, T, Hash>::erase(int32_t linkType, Entry *entry) {

	assert(entry != NULL);

	Link *prev = entry->LINK_PREV(linkType);
	Link *next = entry->LINK_NEXT(linkType);

	if (prev != NULL) {
		prev->next_ = next;
	}
	if (next != NULL) {
		next->prev_ = prev;
	}

	entry->LINK_PREV(linkType) = NULL;
	entry->LINK_NEXT(linkType) = NULL;
}

template <typename K, typename V, typename T, typename Hash>
typename ExpirableMap<K, V, T, Hash>::Entry *ExpirableMap<K, V, T, Hash>::find(
	K key, Link *top) {

	assert(top != NULL);

	Link *link = top->next_;

	while (link != NULL) {
		Entry *entry = getEntry(HASH_LINK, link);
		if (entry->key_ == key) {
			return entry;
		}
		else {
			link = link->next_;
		}
	}

	return NULL;
}

template <typename K, typename V, typename T, typename Hash>
void ExpirableMap<K, V, T, Hash>::push_front(
	int32_t linkType, Entry *entry, Link *top) {
	assert(entry != NULL);
	assert(top != NULL);

	Link *prev = top;
	Link *next = top->next_;

	if (next != NULL) {
		next->prev_ = &(entry->LINK(linkType));
	}
	entry->LINK_NEXT(linkType) = next;

	entry->LINK_PREV(linkType) = prev;
	prev->next_ = &(entry->LINK(linkType));
}

template <typename K, typename V, typename T, typename Hash>
typename ExpirableMap<K, V, T, Hash>::Entry *
ExpirableMap<K, V, T, Hash>::pop_front(int32_t linkType, Link *top) {
	assert(top != NULL);

	Link *prev = top;

	if (prev->next_ != NULL) {
		Entry *entry = getEntry(linkType, prev->next_);

		Link *next = entry->LINK_NEXT(linkType);


		if (next != NULL) {
			next->prev_ = prev;
		}
		prev->next_ = next;

		entry->LINK_PREV(linkType) = NULL;
		entry->LINK_NEXT(linkType) = NULL;

		return entry;
	}
	else {
		return NULL;
	}
}

template <typename K, typename V, typename T, typename Hash>
void ExpirableMap<K, V, T, Hash>::insert(Entry *entry, Link *top) {
	const int32_t linkType = TIMER_LINK;

	assert(entry != NULL);
	assert(top != NULL);

	Link *link = top->next_;
	Link *prev = top;

	while (link != NULL) {
		Entry *target = getEntry(linkType, link);

		if (target->timeout_ > entry->timeout_) {
			prev->next_ = &(entry->LINK(linkType));
			entry->LINK_PREV(linkType) = prev;

			entry->LINK_NEXT(linkType) = link;
			link->prev_ = &(entry->LINK(linkType));

			return;
		}
		else {
			prev = link;
			link = link->next_;
		}
	}


	assert(prev != NULL);
	assert(link == NULL);

	prev->next_ = &(entry->LINK(linkType));
	entry->LINK_PREV(linkType) = prev;

	entry->LINK_NEXT(linkType) = NULL;
}

template <typename K, typename V, typename T, typename Hash>
void ExpirableMap<K, V, T, Hash>::push_front(
	int32_t linkType, Entry *entry, Link *top, Link *tail) {
	assert(entry != NULL);
	assert(top != NULL);
	assert(tail != NULL);

	Link *prev = top;
	Link *next = top->next_;

	if (next != NULL) {
		next->prev_ = &(entry->LINK(linkType));
	}
	entry->LINK_NEXT(linkType) = next;

	entry->LINK_PREV(linkType) = prev;
	prev->next_ = &(entry->LINK(linkType));
}

template <typename K, typename V, typename T, typename Hash>
typename ExpirableMap<K, V, T, Hash>::Entry *ExpirableMap<K, V, T,
	Hash>::pop_front(int32_t linkType, Link *top, Link *tail) {
	assert(top != NULL);
	assert(tail != NULL);

	Link *prev = top;

	if (prev->next_ != tail) {
		Entry *entry = getEntry(linkType, prev->next_);

		Link *next = entry->LINK_NEXT(linkType);


		if (next != NULL) {
			next->prev_ = prev;
		}
		prev->next_ = next;

		entry->LINK_PREV(linkType) = NULL;
		entry->LINK_NEXT(linkType) = NULL;

		return entry;
	}
	else {
		return NULL;
	}
}

template <typename K, typename V, typename T, typename Hash>
void ExpirableMap<K, V, T, Hash>::insert(Entry *entry, Link *top, Link *tail) {
	const int32_t linkType = TIMER_LINK;

	assert(entry != NULL);
	assert(top != NULL);
	assert(tail != NULL);

	Link *link = top->next_;
	Link *prev = top;

	while (link != tail) {
		Entry *target = getEntry(linkType, link);

		if (target->timeout_ > entry->timeout_) {
			prev->next_ = &(entry->LINK(linkType));
			entry->LINK_PREV(linkType) = prev;

			entry->LINK_NEXT(linkType) = link;
			link->prev_ = &(entry->LINK(linkType));

			return;
		}
		else {
			prev = link;
			link = link->next_;
		}
	}


	assert(prev != NULL);
	assert(link == tail);

	prev->next_ = &(entry->LINK(linkType));
	entry->LINK_PREV(linkType) = prev;

	entry->LINK_NEXT(linkType) = tail;
	tail->prev_ = &(entry->LINK(linkType));
}

template <typename K, typename V, typename T, typename Hash>
void ExpirableMap<K, V, T, Hash>::append(Link *list, Link *top, Link *tail) {
	assert(top != NULL);
	UNUSED_VARIABLE(top);
	assert(tail != NULL);
	assert(list != NULL);

	Link *listTop = list;

	Link *listTail = listTop;
	while (listTail->next_ != NULL) {
		listTail = listTail->next_;
	}

	assert(listTail != NULL);

	tail->prev_->next_ = listTop;
	listTop->prev_ = tail->prev_;

	tail->prev_ = listTail;
	listTail->next_ = tail;
}


}  

#endif
