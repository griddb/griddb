﻿/*
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
#include "object_manager.h"  

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
	BaseObject(PartitionId pId, ObjectManager &objectManager)
		: forUpdate_(false),
		  baseOId_(UNDEF_OID),
		  baseAddr_(NULL),
		  cursor_(NULL),
		  pId_(pId),
		  objectManager_(&objectManager) {}

	BaseObject(PartitionId pId, ObjectManager &objectManager, OId oId)
		: forUpdate_(false), baseOId_(oId), pId_(pId),
		  objectManager_(&objectManager) {
		baseAddr_ = cursor_ =
			objectManager_->getForRead<uint8_t>(pId_, baseOId_);
	}


	~BaseObject() {
		if (baseOId_ != UNDEF_OID) {
			objectManager_->unfix(pId_, baseOId_);
			baseOId_ = UNDEF_OID;
		}
	}

	/*!
		@brief Get Object from Chunk for reading
	*/
	void load(OId oId) {
		if (pId_ == UNDEF_PARTITIONID) {
			GS_THROW_SYSTEM_ERROR(
				GS_ERROR_CM_INTERNAL_ERROR, "invalid implementation");
		}
		if (baseOId_ != UNDEF_OID) {
			objectManager_->unfix(pId_, baseOId_);
			baseOId_ = UNDEF_OID;
		}
		baseAddr_ = cursor_ = objectManager_->getForRead<uint8_t>(pId_, oId);
		baseOId_ = oId;
	}


	inline void loadNeighbor(OId oId, AccessMode mode) {
		if (mode == OBJECT_READ_ONLY) {
			baseAddr_ = cursor_ = objectManager_->load<uint8_t, OBJECT_READ_ONLY>(pId_, oId, &baseOId_, baseAddr_);
		} else {
			baseAddr_ = cursor_ = objectManager_->load<uint8_t, OBJECT_FOR_UPDATE>(pId_, oId, &baseOId_, baseAddr_);
			forUpdate_ = true;
		}
	}
	/*!
		@brief Allocate Object from Chunk
	*/
	template <class T>
	T *allocate(Size_t requestSize, const AllocateStrategy &allocateStrategy,
		OId &oId, ObjectType objectType) {
		if (pId_ == UNDEF_PARTITIONID) {
			GS_THROW_SYSTEM_ERROR(
				GS_ERROR_CM_INTERNAL_ERROR, "invalid implementation");
		}
		if (baseOId_ != UNDEF_OID) {
			objectManager_->unfix(pId_, baseOId_);
			baseOId_ = UNDEF_OID;
		}
		baseAddr_ = cursor_ = objectManager_->allocate<uint8_t>(
			pId_, requestSize, allocateStrategy, baseOId_, objectType);
		oId = getBaseOId();
		forUpdate_ = true;
		return reinterpret_cast<T *>(getBaseAddr());
	}

	/*!
		@brief Try to allocate Object from same Chunk of the specified neighbor
	   Object.
	*/
	template <class T>
	T *allocateNeighbor(Size_t requestSize,
		const AllocateStrategy &allocateStrategy, OId &oId, OId neighborOId,
		ObjectType objectType) {
		if (pId_ == UNDEF_PARTITIONID) {
			GS_THROW_SYSTEM_ERROR(
				GS_ERROR_CM_INTERNAL_ERROR, "invalid implementation");
		}
		if (baseOId_ != UNDEF_OID) {
			objectManager_->unfix(pId_, baseOId_);
			baseOId_ = UNDEF_OID;
		}
		baseAddr_ = cursor_ = objectManager_->allocateNeighbor<uint8_t>(pId_,
			requestSize, allocateStrategy, baseOId_, neighborOId, objectType);
		oId = getBaseOId();
		forUpdate_ = true;
		return reinterpret_cast<T *>(getBaseAddr());
	}
	/*!
		@brief Refer to Object
	*/
	void copyReference(OId oId, uint8_t *addr) {
		if (baseOId_ != UNDEF_OID && pId_ != UNDEF_PARTITIONID) {
			objectManager_->unfix(pId_, baseOId_);
			baseOId_ = UNDEF_OID;
		}
		if (oId != UNDEF_OID && pId_ != UNDEF_PARTITIONID) {
			objectManager_->fix(pId_, oId);
		}
		baseOId_ = oId;
		baseAddr_ = cursor_ = addr;
	}

	/*!
		@brief Get Object from Chunk for reading
	*/
	void setDirty() {
		if (pId_ == UNDEF_PARTITIONID) {
			GS_THROW_SYSTEM_ERROR(
				GS_ERROR_CM_INTERNAL_ERROR, "invalid implementation");
		}
		objectManager_->setDirty(pId_, baseOId_);
		forUpdate_ = true;
	}

	/*!
		@brief Refer to Object
	*/
	void copyReference(const BaseObject &srcBaseObject) {
		copyReference(srcBaseObject.getBaseOId(), srcBaseObject.getBaseAddr());
		forUpdate_ = srcBaseObject.forUpdate();
	}

	/*!
		@brief Release the reference to Object
	*/
	void reset() {
		if (baseOId_ != UNDEF_OID && pId_ != UNDEF_PARTITIONID) {
			objectManager_->unfix(pId_, baseOId_);
			baseOId_ = UNDEF_OID;
			baseAddr_ = cursor_ = NULL;
			forUpdate_ = false;
		}
	}

	void reset(PartitionId pId, ObjectManager &objectManager) {
		reset();
		objectManager_ = &objectManager;
		pId_ = pId;
	}

	/*!
		@brief Free Object
	*/
	void finalize() {
		if (baseOId_ != UNDEF_OID && pId_ != UNDEF_PARTITIONID) {
			OId oId = baseOId_;
			baseOId_ = UNDEF_OID;
			baseAddr_ = cursor_ = NULL;
			objectManager_->unfix(pId_, oId);
			objectManager_->free(pId_, oId);
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
	ObjectManager *getObjectManager() const {
		return objectManager_;
	}
	/*!
		@brief Get Object from Chunk
	*/
	void load(OId oId, uint8_t forUpdate) {
		if (pId_ == UNDEF_PARTITIONID) {
			GS_THROW_SYSTEM_ERROR(
				GS_ERROR_CM_INTERNAL_ERROR, "invalid implementation");
		}
		if (baseOId_ != UNDEF_OID) {
			objectManager_->unfix(pId_, baseOId_);
			baseOId_ = UNDEF_OID;
		}
		if (forUpdate == OBJECT_READ_ONLY) {
			baseAddr_ = cursor_ =
				objectManager_->getForRead<uint8_t>(pId_, oId);
		}
		else {
			baseAddr_ = cursor_ =
				objectManager_->getForUpdate<uint8_t>(pId_, oId);
			forUpdate_ = true;
		}
		setBaseOId(oId);
	}
	PartitionId getPartitionId() const {
		return pId_;
	}
	bool forUpdate() const {
		return forUpdate_;
	}
private:
	bool forUpdate_;
	OId baseOId_;
	uint8_t *baseAddr_;
	uint8_t *cursor_;
	PartitionId pId_;

protected:
	BaseObject(uint8_t *addr)
		: baseOId_(UNDEF_OID),
		  baseAddr_(addr),
		  cursor_(addr),
		  pId_(UNDEF_PARTITIONID),
		  objectManager_(NULL) {}
	ObjectManager *objectManager_;

private:
	BaseObject(const BaseObject &);  
	BaseObject &operator=(const BaseObject &);  
};

/*!
	@brief Object base class (for updating)
*/
class UpdateBaseObject : public BaseObject {
public:
	UpdateBaseObject(PartitionId pId, ObjectManager &objectManager)
		: BaseObject(pId, objectManager) {}
	UpdateBaseObject(PartitionId pId, ObjectManager &objectManager, OId oId)
		: BaseObject(pId, objectManager) {
		uint8_t *baseAddr =
			objectManager_->getForUpdate<uint8_t>(getPartitionId(), oId);
		setBaseOId(oId);
		setBaseAddr(baseAddr);
	}

	/*!
		@brief Get Object from Chunk for updating
	*/
	void load(OId oId) {
		if (getPartitionId() == UNDEF_PARTITIONID) {
			GS_THROW_SYSTEM_ERROR(
				GS_ERROR_CM_INTERNAL_ERROR, "invalid implementation");
		}
		if (getBaseOId() != UNDEF_OID) {
			objectManager_->unfix(getPartitionId(), getBaseOId());
			setBaseOId(UNDEF_OID);
		}
		uint8_t *baseAddr = objectManager_->getForUpdate<uint8_t>(getPartitionId(), oId);
		setBaseOId(oId);
		setBaseAddr(baseAddr);
	}
};

#endif
