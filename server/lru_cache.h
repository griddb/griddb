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
#ifndef LRU_CACHE_H_
#define LRU_CACHE_H_

#include "data_type.h"
#include "gs_error.h"
#include "util/type.h"
#include "util/container.h"
#include <iostream>


/*!
	@brief Base Cache Node
*/
template <class Key,class Value, class Allocator>
struct CacheNodeBase {

	/*!
		@brief Constructor
	*/
	CacheNodeBase(Key &k,Value &v) :
			key_(k),
			value_(v),
			prev_(NULL),
			next_(NULL) {}
	
	/*!
		@brief Constructor(No argument)
	*/
	CacheNodeBase(Allocator& alloc) : 
			key_(alloc),
			prev_(NULL),
			next_(NULL) {}

	/*!
		@brief Clear value with Allocator
	*/
	virtual void deallocateValue(Allocator &alloc) {
		UNUSED_VARIABLE(alloc);
	}

	virtual void dump(util::NormalOStringStream &oss) {
		oss << "key:" << key_.c_str();
	}

	Key key_;
	Value value_;
	CacheNodeBase* prev_;
	CacheNodeBase* next_;
};

/*!
	@brief Base LRU Cache manager
*/
template <class Node, class Key, class Value, class Allocator>
class LruCacheBase {
public:

	/*!
		@brief Constructor
	*/
	LruCacheBase(int32_t capacity, Allocator &alloc) :
			alloc_(alloc),
			size_(0),
			capacity_(capacity),
			keymap_(typename TableMap::key_compare(), alloc),
			hitCount_(0),
			missHitCount_(0), swapOutCount_(0), swapInCount_(0) {

		head_ = allocateNode();
		tail_ = allocateNode();
		head_->next_ = tail_;
		head_->prev_ = NULL;
		tail_->next_ = NULL;
		tail_->prev_ = head_;
		keymap_.clear();	
	}

	/*!
		@brief Destructor
	*/
	virtual ~LruCacheBase() {
		while (head_) {
			Node* temp = static_cast<Node *>(head_->next_);
			deallocateNode(head_);
			head_ = temp;
		}
	}

	/*!
		@brief Allocate node, use default allocator new
	*/
	virtual Node *allocateNode() {
		swapInCount_++;
		return ALLOC_VAR_SIZE_NEW(alloc_) Node(alloc_);
	}

	/*!
		@brief Allocate node with argument, use default allocator new
	*/
	virtual Node *allocateNode(Key &key, Value &value) {
		swapOutCount_++;
		return ALLOC_VAR_SIZE_NEW(alloc_) Node(key, value, alloc_);
	}

	/*!
		@brief Deallocate node, use default allocator delete
	*/
	virtual void deallocateNode(Node *node) {
		ALLOC_VAR_SIZE_DELETE(alloc_, node);
		swapOutCount_++;
	}

	/*!
		@brief Deallocate node, use default allocator delete
	*/
	virtual bool releaseNode(Node *node) {
		deallocateNode(node);
		return true;
	}

	/*!
		@brief Put cache entry and return this node;
	*/
	Node* put(Key &key, Value &value) {
		typename TableMap::iterator it = keymap_.find(key);
		Node* node = NULL;
		if (it == keymap_.end()) {
			if (size_ != capacity_) { 
				node = allocateNode(key, value);
				attach(node);
				keymap_.insert(std::make_pair(key, node));
				size_++;
				return node;
			} 
			else {
				Node* temp = static_cast<Node*>(tail_->prev_);
				keymap_.erase(temp->key_);
				detach(temp);
				releaseNode(temp);
				node = allocateNode(key, value);
				attach(node);
				keymap_.insert(std::make_pair(key, node));
				return node;
			}
		} 
		else {
			node = keymap_[key];
			detach(node);
			releaseNode(node);
			node = allocateNode(key, value);
			keymap_[key] = node;
			attach(node);
			return node;
		}
	}

	/*!
		@brief Get cache entry
	*/
	Node *get(Key &key) {
		typename LruCacheBase::TableMap::iterator it = keymap_.find(key);
		if (it != keymap_.end()) {
			Node* node = keymap_[key];
			detach(node);
			attach(node);
			hitCount_++;
			return node;
		}
		else {
			missHitCount_++;
			return (Node*)NULL;
		}
	}

	void dump() {
		std::cout << "swapIn:" << this->swapInCount_
				<< ", swapOut:" << this->swapOutCount_ << std::endl;
	}

	void getStat(int64_t &swapIn, int64_t &swapOut) {
		swapIn = this->swapInCount_;
		swapOut = this->swapOutCount_;
	}


	/*!
		@brief Dump
	*/
	virtual void dump(util::NormalOStringStream &oss, bool isDetail = false) {
		if (isDetail) {
			oss << "[Linked entries]:" << std::endl;
			Node *current = head_;
			int32_t count = 0;
			while (current) {
				Node* temp = static_cast<Node *>(current->next_);
				if (temp != NULL) {
					if (temp == tail_) {
					}
					else {
						oss << "[" << count++ << "] " ;
						temp->dump(oss);
					}
					oss << std::endl;
				}
				current = temp;
			}
		}

		oss << "[Total]: ";
		oss << "hashSize:" << capacity_ << ", currentSize:" << size_;
		int64_t total = hitCount_ + missHitCount_;
		double hitRate = 0;
		if (total != 0) {
			hitRate = static_cast<double>(hitCount_) / static_cast<double>(total);
		}
		oss << ", hitRate:" << hitRate << std::endl;
	}

	Allocator &getAllocator() {
		return alloc_;
	}
protected:
	
	typedef std::pair<Key, Value> MapEntry;
	typedef std::map<Key, Node*, std::less<Key>,
			util::StdAllocator<MapEntry, Allocator> > TableMap;

	util::Mutex lock_;
	Allocator &alloc_;
	int32_t size_;
	int32_t capacity_;
	TableMap keymap_;
	Node* head_;
	Node* tail_;
	int64_t hitCount_;
	int64_t missHitCount_;
	int64_t swapOutCount_;
	int64_t swapInCount_;

	/*!
		@brief move to head linked chain
	*/
	void attach(Node* node) {
		Node* temp = static_cast<Node*>(head_->next_);
		head_->next_ = node;
		node->next_ = temp;
		node->prev_ = head_;
		temp->prev_ = node;
	}

	/*!
		@brief detach linked chain
	*/
	void detach(Node* node) {
		node->prev_->next_ = node->next_;
		node->next_->prev_ = node->prev_;
	}
};

/*!
	@brief CacheNode (use global allocator, thread safe, available remove entry)
*/
template <class Key, class Value, class Allocator>
struct CacheNode : public CacheNodeBase<Key, Value, Allocator> {

	/*!
		@brief Constructor(key, value)
	*/
	CacheNode(Key &k, Value &v, Allocator &alloc) :
			CacheNodeBase<Key,Value,Allocator>(k, v),
			alloc_(alloc),
			refCount_(0),
			removed_(false) {}

	/*!
		@brief Constructor
	*/
	CacheNode(Allocator &alloc) :
			CacheNodeBase<Key,Value,Allocator>(alloc),
			alloc_(alloc),
			refCount_(0),
			removed_(false) {}

	/*!
		@brief release value with global allocator
	*/
	void deallocateValue(Allocator &alloc) {
		UNUSED_VARIABLE(alloc);
		if (this->value_) {
			ALLOC_VAR_SIZE_DELETE(this->alloc_, this->value_);
			this->value_ = NULL;
		}
	}

	/*!
		@brief if latched element, do not release entry and value immediately
	*/
	bool isLatched() {
		return (refCount_ > 0);
	};

	/*!
		@brief if enable this flag, it may detach from cache element if it is unlatched element
	*/
	bool isRemoved() {
		return removed_;
	}

	/*!
		@brief if need to detatch latched node, use this flag and release when unlatched
	*/
	void setRemoved() {
		removed_ = true;
	}

	/*!
		@brief dump
	*/
	void dump(util::NormalOStringStream &oss) {
		oss << "key:" << this->key_.c_str()
				<<", refCont:" << refCount_ 
				<< ", removing:" << static_cast<int32_t>(removed_);
	}


	Value getValue() {
		return this->value_;
	}

	Allocator &alloc_;
	int64_t refCount_;
	bool removed_;
};

/*!
	@brief LruCacheConcurrency (use global allocator, thread safe, available remove entry, resize)
*/
template <class Node, class Key, class Value, class Allocator>
class LruCacheConcurrency :
		public LruCacheBase<Node, Key, Value, Allocator> {
public:

	typedef std::pair<Key, Value> MapEntry;
	typedef std::map<Key, Node*, std::less<Key>,
			util::StdAllocator<MapEntry, Allocator> > TableMap;
	static const int32_t CACHE_MAX_SIZE = 100000;

	/*!
		@brief Constructor
	*/
	LruCacheConcurrency(int32_t capacity, Allocator &alloc) :
			LruCacheBase<Node,Key,Value,Allocator>(capacity, alloc),
			pendingCount_(0),
			allocateCount_(0) {}

	/*!
		@brief Destructor
	*/
	~LruCacheConcurrency() {
		Node *current = this->head_;
		while (current) {
			Node* temp = static_cast<Node *>(current->next_);
			if (current && current != this->head_ && current != this->tail_) {
				current->deallocateValue(this->alloc_);
			}
			deallocateNode(current);
			current = temp;
		}
		this->head_ = NULL;
		this->tail_ = NULL;
	}

	/*!
		@brief allocate node with global allocator(for head or tail)
	*/
	Node *allocateNode() {
		return ALLOC_VAR_SIZE_NEW(this->alloc_) Node(this->alloc_);
	}

	/*!
		@brief allocate node with global allocator
	*/
	Node *allocateNode(Key &key, Value &value) {
		allocateCount_++;
		return ALLOC_VAR_SIZE_NEW(this->alloc_)
				Node(key, value, this->alloc_);
	}

	/*!
		@brief deallocate node with global allocator
	*/
	void deallocateNode(Node *node) {
		allocateCount_--;
		ALLOC_VAR_SIZE_DELETE(this->alloc_, node);
	}

	/*!
		@brief if node is unlatched release immediately, else pending
	*/
	bool releaseNode(Node *node) {
		if (node) {
			if (node->isLatched()) {
				pendingCount_++;
				node->setRemoved();
			}
			else {
				if (node->refCount_ < 0) {
					return false;
				}
				node->deallocateValue(this->alloc_);
				deallocateNode(node);
				return true;
			}
		}
		return false;
	}

	/*!
		@brief put key-value element
	*/
	Node* put(Key &key, Value &value, bool withGet = false) {
		util::LockGuard<util::Mutex> guard(lock_);
		Node* node = static_cast<Node*>(
				LruCacheBase<Node,Key,Value,Allocator>::put(key, value));
		if (withGet) {
			node->refCount_++;
		}
		return node;
	};

	/*!
		@brief get value from key
	*/
	Node *get(Key &key) {
		util::LockGuard<util::Mutex> guard(lock_);
		Node *node = static_cast<Node*>(
				LruCacheBase<Node,Key,Value,Allocator>::get(key));
		if (node) {
			node->refCount_++;
			return node;
		}
		else {
			return (Node*)NULL;
		}
	}

	/*!
		@brief remove key value entry, update chain
	*/
	void remove(Key &key) {
		util::LockGuard<util::Mutex> guard(lock_);
		typename TableMap::iterator it = this->keymap_.find(key);
		if (it != this->keymap_.end()) {
			Node* node = (*this->keymap_.find(key)).second;
			this->keymap_.erase(key);
			LruCacheBase<Node,Key,Value,Allocator>::detach(node);
			node->setRemoved();
			this->size_--;
			releaseNode(node);
		}
	}

	bool setDiffLoad(Key &key) {
		util::LockGuard<util::Mutex> guard(lock_);
		typename TableMap::iterator it = this->keymap_.find(key);
		if (it != this->keymap_.end()) {
			Node* node = (*it).second;
			node->value_->setDiffLoad();
			return true;
		}
		return false;
	}

	/*!
		@brief release key-value entry, should be used RAII(get/release)
	*/
	void release(Node*& node) {
		util::LockGuard<util::Mutex> guard(lock_);
		if (node == NULL) {
			return;
		}
		node->refCount_--;
		if (node->refCount_ < 0) {
			return;
		}
		if (node->isRemoved()) {
			if (releaseNode(node)) {
				node = NULL;
			}
		}
	}

	void refresh() {
		util::LockGuard<util::Mutex> guard(lock_);
		for (typename TableMap::iterator it = this->keymap_.begin();
				it != this->keymap_.end(); it++) {
			Node* node = (*it).second;			
			node->value_->disableCache_ = true;
		}
	}

	/*!
		@brief resize cache
	*/
	void resize(int32_t recacheSize) {
		if (recacheSize < 0 ||  recacheSize >= CACHE_MAX_SIZE) {
			return;
		}
		util::LockGuard<util::Mutex> guard(lock_);
		Node* current = static_cast<Node *>(this->head_->next_);
		int32_t tempCount = 0;
		int32_t availableCount = 0;
		while (current && current != this->tail_) {
			Node* temp = static_cast<Node*>(current->next_);
			tempCount++;
			if (recacheSize < tempCount) {
				this->keymap_.erase(current->key_);
				LruCacheBase<Node,Key,Value,Allocator>::detach(current);
				releaseNode(current);
			}
			else {
				availableCount++;
			}
			current = temp;
		}
		this->capacity_ = recacheSize;
		this->size_ = availableCount;
	}

	void dump(util::NormalOStringStream &oss,
			bool isDetail = false) {
		util::LockGuard<util::Mutex> guard(lock_);
		LruCacheBase<Node,Key,Value,Allocator>::dump(oss, isDetail);
		oss << "allocateCount:" << allocateCount_
				<< ", pendingCount:" << pendingCount_ << std::endl;
	}

	void dump() {
		LruCacheBase<Node,Key,Value,Allocator>::dump();
	}

	void getStat(int64_t &swapIn, int64_t &swapOut) {
		LruCacheBase<Node,Key,Value,Allocator>::getStat(swapIn, swapOut);
	}

protected:
	util::Mutex lock_;
private:
	int64_t pendingCount_;
	int64_t allocateCount_;
};

#include "picojson.h"

#define TEST_PRINT(s)
#define TEST_PRINT1(s, d)

template <class Node, class Key, class Value, class Allocator>
class LruCacheWithMonitor :
		public LruCacheConcurrency<Node, Key, Value, Allocator> {
public:
	typedef std::pair<Key, Value> MapEntry;
	typedef std::map<Key, Node*, std::less<Key>,
			util::StdAllocator<MapEntry, Allocator> > TableMap;

	LruCacheWithMonitor(int32_t capacity, Allocator &alloc, int32_t updateInterval) :
			LruCacheConcurrency<Node,Key,Value,Allocator>(capacity, alloc),
			updateInterval_(updateInterval) {}

	Node* put(Key &key, Value &value, bool withGet = false) {
		util::LockGuard<util::Mutex> guard(this->lock_);
		typename TableMap::iterator it = this->keymap_.find(key);
		Node* node = NULL;
		if (it == this->keymap_.end()) {
			if (this->size_ != this->capacity_) { 
				node = LruCacheBase<Node,Key,Value,Allocator>::allocateNode(key, value);
				LruCacheBase<Node,Key,Value,Allocator>::attach(node);
				this->keymap_.insert(std::make_pair(key, node));
				this->size_++;
			} 
			else {
				Node* temp = static_cast<Node*>(this->tail_->prev_);
				this->keymap_.erase(temp->key_);
				LruCacheBase<Node,Key,Value,Allocator>::detach(temp);
				temp->deallocateValue(this->alloc_);
				LruCacheBase<Node,Key,Value,Allocator>::releaseNode(temp);
				node = LruCacheBase<Node,Key,Value,Allocator>::allocateNode(key, value);
				LruCacheBase<Node,Key,Value,Allocator>::attach(node);
				this->keymap_.insert(std::make_pair(key, node));
			}
		} 
		else {
			node = this->keymap_[key];
			int refCount = node->refCount_;
			LruCacheBase<Node,Key,Value,Allocator>::detach(node);
			node->deallocateValue(this->alloc_);
			LruCacheBase<Node,Key,Value,Allocator>::releaseNode(node);
			node = LruCacheBase<Node,Key,Value,Allocator>::allocateNode(key, value);
			node->refCount_ = refCount; 
			this->keymap_[key] = node;
			LruCacheBase<Node,Key,Value,Allocator>::attach(node);
		}

		if (withGet) {
			TEST_PRINT("LruCacheWithMonitor put() increment\n");
			node->refCount_++;
		}
		return node;
	};

	int checkAndGet(Key &key, Value &value) {
		util::LockGuard<util::Mutex> guard(this->lock_);
		Node *node = static_cast<Node*>(
				LruCacheBase<Node,Key,Value,Allocator>::get(key));
		if (node) {
			int ret = node->value_->checkAndGet(value, updateInterval_);
			if (ret != 0) {
				if (node->isRemoved()) {
					if (LruCacheConcurrency<Node,Key,Value,Allocator>::releaseNode(node)) {
						node = NULL;
					}
				}
			} else {
				TEST_PRINT("checkAndGet() increment\n");
				node->refCount_++;
			}
			return ret;
		}
		else {
			return 2;
		}
	}

	void release(Node*& node) {
		LruCacheConcurrency<Node,Key,Value,Allocator>::release(node);
	}

	void release(Key &key) {
		util::LockGuard<util::Mutex> guard(this->lock_);
		Node *node = static_cast<Node*>(
				LruCacheBase<Node,Key,Value,Allocator>::get(key));
		if (node) {
			TEST_PRINT("LruCacheWithMonitor;;release() decrement\n");
			node->refCount_--;
			if (node->isRemoved()) {
				if (LruCacheConcurrency<Node,Key,Value,Allocator>::releaseNode(node)) {
					node = NULL;
				}
			}
		}
	}
	
	void clear(Key &name, bool isDatabase) {
		util::LockGuard<util::Mutex> guard(this->lock_);
		Node *temp = static_cast<Node*>(this->head_->next_);
		while (temp != this->tail_) {
			temp->value_->clear(name, isDatabase);
			temp = static_cast<Node*>(temp->next_);
		}
	}

	void scan(Key *name, bool isDatabase, picojson::value &result) {
		TEST_PRINT("Cache scan() S\n");
		TEST_PRINT1("size=%d\n", this->size_);
		util::LockGuard<util::Mutex> guard(this->lock_);
		Node *temp = static_cast<Node*>(this->head_->next_);

		picojson::object root;
		picojson::array dataList;
		while (temp != this->tail_) {
			picojson::object cacheInfo;
			if (temp->value_->scan(name, isDatabase, cacheInfo)) {
				cacheInfo["count"] = picojson::value(static_cast<double>(temp->refCount_));
				dataList.push_back(picojson::value(cacheInfo));
			}
			temp = static_cast<Node*>(temp->next_);
		}

		root.insert(std::make_pair("usercache", picojson::value(dataList)));
		result = picojson::value(root);

	}

private:
	int32_t updateInterval_;
};

#endif
