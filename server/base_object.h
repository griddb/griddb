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
	@brief Definition of BaseObject base class
*/
#ifndef BASE_OBJECT_H_
#define BASE_OBJECT_H_

#include "data_type.h"
#include <float.h>
#include "object_manager_v4.h"  

class TransactionContext;

/*!
	@brief Auto_ptr by util::StackAllocator
*/
template <typename T>
class StackAllocAutoPtr {
public:
	StackAllocAutoPtr(util::StackAllocator &alloc)
		: ptr_(NULL), alloc_(alloc) {}
	StackAllocAutoPtr(util::StackAllocator &alloc, T *ptr)
		: ptr_(ptr), alloc_(alloc) {}
	~StackAllocAutoPtr() {
		ALLOC_DELETE((alloc_), ptr_);
	}
	void set(T *ptr) {
		ptr_ = ptr;
	}
	T *get() {
		return ptr_;
	}

private:
	T *ptr_;
	util::StackAllocator &alloc_;
};

/*!
	@brief Object base class
*/
class BaseObject {
public:
	explicit BaseObject(ObjectManagerV4 &objectManager) :
			isDirty_(false),
			baseOId_(UNDEF_OID),
			baseAddr_(NULL),
			cursor_(NULL),
			objectManager_(&objectManager),
			strategy_(UNDEF_DS_GROUPID, &objectManager) {
	}

	BaseObject(
			ObjectManagerV4 &objectManager, const AllocateStrategy &strategy) :
			isDirty_(false),
			baseOId_(UNDEF_OID),
			baseAddr_(NULL),
			cursor_(NULL),
			objectManager_(&objectManager),
			strategy_(strategy.getInfo()) {
	}

	BaseObject(
			ObjectManagerV4 &objectManager, const AllocateStrategy &strategy,
			OId oId) :
			isDirty_(false),
			baseOId_(oId),
			objectManager_(&objectManager),
			strategy_(strategy.getInfo()) {
		assert(baseOId_ != UNDEF_OID);
		baseAddr_ = cursor_ =
				strategy_.accessor_.get<uint8_t>(baseOId_);
	}

	~BaseObject() {
		if (baseOId_ != UNDEF_OID) {
			strategy_.accessor_.unpin(baseOId_, isDirty_);
			baseOId_ = UNDEF_OID;
		}
	}

	/*!
		@brief Get Object from Chunk for reading
	*/
	void load(OId oId, uint8_t isDirty) {
		if (baseOId_ != UNDEF_OID) {
			strategy_.accessor_.unpin(baseOId_, isDirty_);
			baseOId_ = UNDEF_OID;
		}
		assert(oId != UNDEF_OID);
		baseAddr_ = cursor_ =
			strategy_.accessor_.get<uint8_t>(oId, isDirty);
		baseOId_ = oId;
		isDirty_ = isDirty;
		if (isDirty) {
			strategy_.accessor_.update(oId);
		}
	}

	inline void loadFast(OId oId) {
		assert(oId != UNDEF_OID);
		baseAddr_ =
			strategy_.accessor_.get<uint8_t>(oId, &baseOId_, baseAddr_, isDirty_);
	}

	inline void loadNeighbor(OId oId, AccessMode mode) {
		assert(oId != UNDEF_OID);
		baseAddr_ = cursor_ = strategy_.accessor_.get<uint8_t>(oId, &baseOId_, baseAddr_, isDirty_);
		if (mode == OBJECT_FOR_UPDATE) {
			isDirty_ = true;
			strategy_.accessor_.update(oId);
		}
	}
	/*!
		@brief Allocate Object from Chunk
	*/
	template <class T>
	T *allocate(DSObjectSize requestSize,
		OId &oId, ObjectType objectType) {
		if (baseOId_ != UNDEF_OID) {
			strategy_.accessor_.unpin(baseOId_, isDirty_);
			baseOId_ = UNDEF_OID;
		}
		baseOId_ = strategy_.accessor_.allocate(requestSize, objectType);
		baseAddr_ = cursor_ = strategy_.accessor_.get<uint8_t>(baseOId_, true);
		oId = getBaseOId();
		isDirty_ = true;
		return reinterpret_cast<T *>(getBaseAddr());
	}

	/*!
		@brief Try to allocate Object from same Chunk of the specified neighbor
	   Object.
	*/
	template <class T>
	T *allocateNeighbor(DSObjectSize requestSize,
		OId &oId, OId neighborOId,
		ObjectType objectType) {
		if (baseOId_ != UNDEF_OID) {
			strategy_.accessor_.unpin(baseOId_, isDirty_);
			baseOId_ = UNDEF_OID;
		}
		baseOId_ = strategy_.accessor_.allocateNeighbor(requestSize, neighborOId, objectType);
		baseAddr_ = cursor_ = strategy_.accessor_.get<uint8_t>(baseOId_, true);
		oId = getBaseOId();
		isDirty_ = true;
		return reinterpret_cast<T *>(getBaseAddr());
	}

	/*!
		@brief Refer to Object
	*/
	void copyReference(OId oId, uint8_t *addr) {
		if (baseOId_ != UNDEF_OID) {
			strategy_.accessor_.unpin(baseOId_, isDirty_);
			baseOId_ = UNDEF_OID;
		}
		if (oId != UNDEF_OID) {
			strategy_.accessor_.pin(oId);
		}
		baseOId_ = oId;
		baseAddr_ = cursor_ = addr;
		isDirty_ = false;
	}

	/*!
		@brief Get Object from Chunk for reading
	*/
	void setDirty() {
		if (!isDirty_ && baseOId_ != UNDEF_OID) {
			strategy_.accessor_.update(baseOId_);
		}
		isDirty_ = true;
	}

	/*!
		@brief Refer to Object
	*/
	void copyReference(const BaseObject &srcBaseObject) {
		copyReference(srcBaseObject.getBaseOId(), srcBaseObject.getBaseAddr());
		isDirty_ = srcBaseObject.isDirty();
	}

	/*!
		@brief Release the reference to Object
	*/
	void reset() {
		if (baseOId_ != UNDEF_OID) {
			strategy_.accessor_.unpin(baseOId_, isDirty_);
			baseOId_ = UNDEF_OID;
			baseAddr_ = cursor_ = NULL;
			isDirty_ = false;
		}
	}

	void reset(
			ObjectManagerV4 &objectManager, const AllocateStrategy &strategy) {
		reset();
		objectManager_ = &objectManager;
		strategy_.set(strategy.getInfo());
	}

	/*!
		@brief Free Object
	*/
	void finalize() {
		if (baseOId_ != UNDEF_OID) {
			OId oId = baseOId_;
			baseOId_ = UNDEF_OID;
			baseAddr_ = cursor_ = NULL;
			strategy_.accessor_.unpin(oId, isDirty_);
			strategy_.accessor_.free(oId);
		}
	}

public:
	/*!
		@brief Get OId of Object
	*/
	const OId &getBaseOId() const {
		return baseOId_;
	}
	/*!
		@brief Get OId of Object
	*/
	OId &getBaseOId() {
		return baseOId_;
	}
	/*!
		@brief Get address of Object
	*/
	uint8_t *getBaseAddr() const {
		return baseAddr_;
	}
	/*!
		@brief Get address of Object
	*/
	template <class T>
	T getBaseAddr() const {
		return reinterpret_cast<T>(baseAddr_);
	}
	/*!
		@brief Get address of cursor
	*/
	template <typename V>
	V *getCursor() const {
		return reinterpret_cast<V *>(cursor_);
	}

	/*!
		@brief Move cursor
	*/
	void moveCursor(int64_t offset) {
		cursor_ += offset;
	}
	/*!
		@brief Reset cursor
	*/
	void resetCursor() {
		cursor_ = baseAddr_;
	}

	/*!
		@brief Set OId of Object
	*/
	void setBaseOId(OId oId) {
		baseOId_ = oId;
	}
	/*!
		@brief Set address of Object
	*/
	void setBaseAddr(uint8_t *addr) {
		baseAddr_ = cursor_ = addr;
	}
	/*!
		@brief Get ObjectManager
	*/
	ObjectManagerV4 *getObjectManager() const {
		return objectManager_;
	}
	const AllocateStrategy& getAllocateStrategy() const {
		return strategy_;
	}
	bool isDirty() const {
		return isDirty_;
	}
	DSObjectSize getSize() {
		return objectManager_->getSize(baseAddr_);
	}

private:
	bool isDirty_;
	OId baseOId_;
	uint8_t *baseAddr_;
	uint8_t *cursor_;

protected:
	explicit BaseObject(uint8_t *addr) :
			isDirty_(false),
			baseOId_(UNDEF_OID),
			baseAddr_(addr),
			cursor_(addr),
			objectManager_(NULL) {
	}

	ObjectManagerV4 *objectManager_;
	AllocateStrategy strategy_;

private:
	BaseObject(const BaseObject&);  
	BaseObject& operator=(const BaseObject&);  
};

/*!
	@brief Object base class (for updating)
*/
class UpdateBaseObject : public BaseObject {
public:
	UpdateBaseObject(ObjectManagerV4 &objectManager, AllocateStrategy& strategy)
		: BaseObject(objectManager, strategy) {}
	UpdateBaseObject(ObjectManagerV4 &objectManager, AllocateStrategy& strategy, OId oId)
		: BaseObject(objectManager, strategy) {
		assert(oId != UNDEF_OID);
		uint8_t *baseAddr =
			strategy_.accessor_.get<uint8_t>(oId, true); 
		setBaseOId(oId);
		setBaseAddr(baseAddr);
	}

	/*!
		@brief Get Object from Chunk for updating
	*/
	void load(OId oId) {
		if (getBaseOId() != UNDEF_OID) {
			strategy_.accessor_.unpin(getBaseOId(), isDirty());
			setBaseOId(UNDEF_OID);
		}
		assert(oId != UNDEF_OID);
		uint8_t* baseAddr =
			strategy_.accessor_.get<uint8_t>(oId, true); 
		setBaseOId(oId);
		setBaseAddr(baseAddr);
	}
private:
	void load(DSGroupId groupId, OId oId, uint8_t isDirty);
};

#endif
