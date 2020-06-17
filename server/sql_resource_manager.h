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
	@file sql_resource_manager.h
	@brief Definition of sql resource manager
*/

#ifndef SQL_RESOURCE_MANAGER_H_
#define SQL_RESOURCE_MANAGER_H_

#include "util/container.h"
#include "gs_error.h"

/*!
	@brief Managed resource base class
*/
class ResourceBase {

public:

	/*!
		@brief Constructor
	*/
	ResourceBase() : refCount_(0), removed_(false) {}

	/*!
		@brief Increment reference count
	*/
	void inc() {
		refCount_++;
	}

	/*!
		@brief Decrement reference count
	*/
	bool decl() {
		refCount_--;
		return (refCount_ == 0);
	}

	/*!
		@brief Remove from resource manager
	*/
	void remove() {
		removed_ = true;
	}

	/*!
		@brief Check remove
	*/
	bool isRemoved() {
		return removed_;
	}

	/*!
		@brief Check reference
	*/
	bool isRefer() {
		return (refCount_ > 0);
	}

protected:

	int32_t refCount_;
	bool removed_;
};

/*!
	@brief Resource manager
*/
template <class Key, class Value, class Context, class Allocator>
class ResourceManager {
public:

	typedef std::pair<Key, Value*> ResourceEntry;
	typedef util::AllocMap<Key, Value*> ResourceMap;

	/*!
		@brief Constructor
	*/
	ResourceManager(Allocator &alloc) :
			alloc_(alloc),
			resourceMap_(alloc) {}

	~ResourceManager() {
		try {
			for (typename ResourceMap::iterator it = resourceMap_.begin();
					it != resourceMap_.end(); it++) {

				util::LockGuard<util::Mutex> guard(lock_);
				Value *value = (*it).second;
				if (value != NULL) {
					ALLOC_VAR_SIZE_DELETE(alloc_, value);
				}
			}
		}
		catch (...) {
		}
	}

	Value *put(Key &key, Context &context);

	Value *get(Key &key);

	void remove(Key &key);

	void release(Value *&value);

	void getKeyList(util::Vector<Key> &keyList);

	util::Mutex &getLock() {
		return lock_;
	}

	typename ResourceMap::iterator begin() {
		return resourceMap_.begin();
	}

	typename ResourceMap::iterator end() {
		return resourceMap_.end();
	}

private:

	util::Mutex lock_;
	Allocator &alloc_;
	ResourceMap resourceMap_;
};

template <class Key, class Value, class Context, class Allocator>
Value* ResourceManager<Key, Value, Context, Allocator>::put(
		Key &key, Context &context) {

	try {
		util::LockGuard<util::Mutex> guard(lock_);
		typename ResourceMap::iterator it = resourceMap_.find(key);
		if (it != resourceMap_.end()) {
			GS_THROW_USER_ERROR(
					0, "Key=" << key << " is already exists");
		}
		else {
			Value *value = ALLOC_VAR_SIZE_NEW(alloc_) Value(context);
			value->inc();
			resourceMap_.insert(std::make_pair(key, value));
			return value;
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

template <class Key, class Value, class Context, class Allocator>
Value *ResourceManager<
		Key, Value, Context, Allocator>::get(Key &key) {

	try {
		util::LockGuard<util::Mutex> guard(lock_);

		typename ResourceMap::iterator it = resourceMap_.find(key);
		if (it != resourceMap_.end()) {
			Value *value = (*it).second;
			value->inc();
			return value;
		}
		else {
			return NULL;
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

template <class Key, class Value, class Context, class Allocator>
void ResourceManager<
		Key, Value, Context, Allocator>::remove(Key &key) {

	try {
		util::LockGuard<util::Mutex> guard(lock_);

		typename ResourceMap::iterator it = resourceMap_.find(key);
		if (it != resourceMap_.end()) {
			Value *value = (*it).second;
			value->remove();
			if (!value->isRefer()) {
				ALLOC_VAR_SIZE_DELETE(alloc_, value);
				value = NULL;
			}
			resourceMap_.erase(key);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

template <class Key, class Value, class Context, class Allocator>
void ResourceManager<
		Key, Value, Context, Allocator>::release(Value *&value) {

	try {
		util::LockGuard<util::Mutex> guard(lock_);
		if (value->decl() && value->isRemoved()) {
			ALLOC_VAR_SIZE_DELETE(alloc_, value);
			value = NULL;
		}
	}	
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

template <class Key, class Value, class Context, class Allocator>
void ResourceManager<
		Key, Value, Context, Allocator>::getKeyList(util::Vector<Key> &keyList) {

	try {
		util::LockGuard<util::Mutex> guard(lock_);

		for (typename ResourceMap::iterator it = resourceMap_.begin();
				it != resourceMap_.end(); it++) {
			keyList.push_back((*it).first);
		}
	}	
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

template <class Key, class Value, class Context, class Manager>
class ResourceLatch {
	public:

	ResourceLatch(Key &key, Manager *manager, Context *context) :
			key_(key), manager_(manager) {

		if (context) {
			value_ = manager_->put(key, *context);
		}
		else {
			value_ = manager_->get(key);
		}
	}

	~ResourceLatch() {
		if (value_) {
			manager_->release(value_);
		}
	}

	Value *get() {
		return value_;
	}

private:

	ResourceLatch(const ResourceLatch&);
	ResourceLatch& operator=(const ResourceLatch&);
	
	Key &key_;
	Manager *manager_;
	Value *value_;
};

#endif
