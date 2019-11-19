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
	@brief Definition of ObjectManager
*/
#ifndef OBJECT_MANAGER_H_
#define OBJECT_MANAGER_H_

#include "util/type.h"
#include "util/container.h"
#include "util/trace.h"
#include "util/trace.h"
#include "gs_error.h"
#include "data_type.h"
#include "chunk_manager.h"
#include "object_allocator.h"
#include "data_store_common.h"  


UTIL_TRACER_DECLARE(OBJECT_MANAGER);

template<AccessMode> struct ObjectAccessType {};
typedef ObjectAccessType<OBJECT_READ_ONLY> ObjectReadType;
typedef ObjectAccessType<OBJECT_FOR_UPDATE> ObjectWriteType;



#define ASSERT_ISVALID_CATEGORYID(categoryId)      \
	assert(CHUNK_CATEGORY_ID_BASE <= categoryId && \
		   categoryId < ChunkManager::CHUNK_CATEGORY_NUM);

#define ASSERT_ISVALID_CHUNKID(cId) \
	assert(0 <= cId && cId <= ObjectManager::OBJECT_MAX_CHUNK_ID_);

#define ASSERT_ISVALID_OFFSET(offset)                           \
	assert(0 <= offset && offset < (1L << MAX_CHUNK_EXP_SIZE)); \
	assert((offset - ObjectAllocator::BLOCK_HEADER_SIZE) %      \
			   (1L << UNIT_OFFSET_ROUND_BIT) ==                 \
		   0);

#define ASSERT_ISVALID_USER(user)                           \
	assert(0 <= user && user < (1L << MAX_USER_BIT));

struct AllocateStrategy;

/*!
	@brief Operates objects on the each Chunk.
*/
class ObjectManager {
	typedef ChunkManager::MetaChunk MetaChunk;
	typedef ChunkManager::DataAffinityInfo DataAffinityInfo;

public:
	/*!
		@brief Free mode of allocated Objects one each or a Chunk of Objects at
	   once.
	*/
	enum FreeMode {

		OBJECT_FREE_MODE = 0,

		BATCH_FREE_MODE = 1
	};

	/*!
		@brief Attribute of objects
	*/
	struct ObjectAttribute {
		ObjectAttribute() : freeMode_(OBJECT_FREE_MODE) {}
		FreeMode freeMode_;
	};

	static const uint32_t CHUNK_HEADER_BLOCK_SIZE =
			ChunkManager::CHUNK_HEADER_FULL_SIZE;
	static const ChunkCategoryId CHUNK_CATEGORY_ID_BASE = 0;

	ObjectManager(
			const ConfigTable& configTable, ChunkManager* newChunkManager);
	ObjectManager();  

	~ObjectManager();

	void dropPartition(PartitionId pId);
	bool existPartition(PartitionId pId);
	void fix(PartitionId pId, OId oId);
	void unfix(PartitionId pId, OId oId);
	void resetRefCounter(PartitionId pId);
	UTIL_FORCEINLINE
	void freeLastLatchPhaseMemory(PartitionId pId);

	void setSwapOutCounter(PartitionId pId, int64_t counter);
	int64_t getSwapOutCounter(PartitionId pId);
	void setStoreMemoryAgingSwapRate(PartitionId pId, double ratio);
	double getStoreMemoryAgingSwapRate(PartitionId pId);

	Size_t estimateAllocateSize(Size_t requestSize);
	Size_t getAllocateSize(uint32_t exponent);

	uint8_t* allocateObject(
			PartitionId pId, Size_t requestSize,
			const AllocateStrategy& allocateStrategy, OId& oId,
			ObjectType objectType);

	template <class T>
	T* allocate(
			PartitionId pId, Size_t requestSize,
			const AllocateStrategy& allocateStrategy, OId& oId,
			ObjectType objectType);


	uint8_t* allocateNeighborObject(
			PartitionId pId, Size_t requestSize,
			const AllocateStrategy& allocateStrategy, OId& oId, OId neighborOId,
			ObjectType objectType);

	template <class T>
	T* allocateNeighbor(
			PartitionId pId, Size_t requestSize,
			const AllocateStrategy& allocateStrategy, OId& oId, OId neighborOId,
			ObjectType objectType);

	void free(PartitionId pId, OId oId);

	uint32_t getChunkSize() const {
		return chunkManager_->getConfig().getChunkSize();
	}

	bool batchFree(
			PartitionId pId, ChunkKey chunkKey, uint64_t maxScanNum,
			uint64_t& scanNum, uint64_t& freeChunkNum,
			ChunkKey simulateChunkKey, uint64_t& simulateFreeNum);

	void purgeDataAffinityInfo(PartitionId pId, Timestamp baseTime);

	template <class T>
	UTIL_FORCEINLINE T* getForUpdate(PartitionId pId, OId oId);

	template <class T>
	UTIL_FORCEINLINE T* getForRead(PartitionId pId, OId oId);

	template<typename T, AccessMode mode>
	UTIL_FORCEINLINE T* load(
			PartitionId pId, OId oId, OId* lastOId, uint8_t* lastAddr);

	void* reload(PartitionId pId, OId oId, OId *lastOId, const ObjectReadType&); 
	void* reload(PartitionId pId, OId oId, OId *lastOId, const ObjectWriteType&); 

	UTIL_FORCEINLINE
	void setDirty(PartitionId pId, OId oId);

	Size_t getSize(uint8_t* objectAddr) const;

	ObjectType getObjectType(uint8_t* objectAddr) const;

	uint32_t getMaxObjectSize() const {
		return maxObjectSize_;
	}
	uint32_t getHalfOfMaxObjectSize() const {
		return halfOfMaxObjectSize_;
	}
	uint32_t getRecommendtLimitObjectSize() const {
		return recommendLimitObjectSize_;
	}

	void validateRefCounter(PartitionId pId);

	void dumpRefCounter(PartitionId pId);
	void dumpObject(PartitionId pId, int32_t level = 1);
	void dumpObjectDigest(PartitionId pId);

private:
	const ChunkId OBJECT_MAX_CHUNK_ID_;
	static const int32_t BATCH_FREE_INTERVAL_ = 100;

	const PartitionId PARTITION_NUM_;   
	const uint32_t CHUNK_EXP_SIZE_;		
	ChunkManager* chunkManager_;		
	ObjectAllocator* objectAllocator_;  
	uint32_t maxObjectSize_;
	uint32_t halfOfMaxObjectSize_;
	uint32_t recommendLimitObjectSize_;
	bool isZeroFill_; 

	inline static DataAffinityInfo makeDataAffinityInfo(const AllocateStrategy &strategy);

	uint8_t* allocateObject(
			MetaChunk& metaChunk, uint8_t powerSize,
			ObjectType objectType, uint32_t& offset, Size_t& size);

	Size_t freeObject(MetaChunk& metaChunk, Offset_t offset);

	void validateOId(PartitionId pId, OId oId);

	void validateObject(
			uint8_t* objectAddr,
			PartitionId pId = UNDEF_PARTITIONID, OId oId = UNDEF_OID,
			ChunkCategoryId categoryId = -1, ChunkId cId = UNDEF_CHUNKID,
			Offset_t offset = -1) const;




	static const uint64_t MASK_32BIT = 0xFFFFFFFFULL;
	static const uint64_t MASK_16BIT = 0x0000FFFFULL;
	static const uint64_t MASK_3BIT = 0x00000007ULL;

	static const int32_t MAGIC_NUMBER_EXP_SIZE = 3;
	static const uint64_t MASK_MAGIC = 0x000000000000e000ULL;
	static const uint64_t MAGIC_NUMBER = 0x000000000000a000ULL;

	static const uint64_t MASK_USER = 0x000003FFULL;

	static const uint32_t UNIT_OFFSET_ROUND_BIT = 4;

	static const uint32_t MAX_CHUNK_EXP_SIZE = 26;

	const bool LARGE_CHUNK_MODE_;
	const uint32_t UNIT_CHUNK_SHIFT_BIT;
	static const uint32_t UNIT_OFFSET_SHIFT_BIT = 16;
	static const uint32_t CATEGORY_ID_SHIFT_BIT = 10;
	static const uint32_t MAGIC_SHIFT_BIT = 1 + 12;


	static const uint32_t MAX_USER_BIT = 10;

	const uint32_t MAX_UNIT_OFFSET_BIT;
	const uint64_t MASK_UNIT_OFFSET;

	static inline uint32_t calcUnitChunkShiftBit(bool isLargeChunkMode) {
		return isLargeChunkMode ? 38 : 32;
	}
	static inline uint32_t calcMaxUnitOffsetBit(bool isLargeChunkMode) {
		return isLargeChunkMode ? 22 : 16;
	}

public:
	inline static OId getOId(
			uint32_t chunkExpSize, ChunkCategoryId categoryId,
			ChunkId cId, Offset_t offset);
	inline OId getOId(
			ChunkCategoryId categoryId, ChunkId cId, Offset_t offset);

	inline ChunkId getChunkId(OId oId);
	inline Offset_t getOffset(OId oId);
	inline Offset_t getRelativeOffset(OId oId);
	inline static ChunkCategoryId getChunkCategoryId(OId oId);
	inline static OId getUserArea(OId oId);
	inline static OId setUserArea(OId oId, OId userArea);
	inline static OId getBaseArea(OId oId);

private:

	inline static bool isValidOId(OId oId);
	inline bool notEqualChunk(OId oId1, OId oId2);
public:
	void checkDirtyFlag(PartitionId) {
		;
	}
};


/*!
  @brief Drops all objects of the Partition by dropping all using Chunks.
*/
inline void ObjectManager::dropPartition(PartitionId pId) {
	try {
		chunkManager_->dropPartition(pId);
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

/*!
	@brief Checks if the Partition is not initial state or dropped.
*/
inline bool ObjectManager::existPartition(PartitionId pId) {
	return chunkManager_->existPartition(pId);
}

/*!
	@brief Fixes the object memory.
*/
inline void ObjectManager::fix(PartitionId pId, OId oId) {
	ChunkCategoryId categoryId = getChunkCategoryId(oId);
	ChunkId cId = getChunkId(oId);
	chunkManager_->fix(pId, categoryId, cId);
}

/*!
	@brief Unfixes the object memory.
*/
inline void ObjectManager::unfix(PartitionId pId, OId oId) {
	ChunkCategoryId categoryId = getChunkCategoryId(oId);
	ChunkId cId = getChunkId(oId);
	chunkManager_->unfix(pId, categoryId, cId);
}

/*!
	@brief Unfixes all fixed object memory.
*/
inline void ObjectManager::resetRefCounter(PartitionId pId) {
	chunkManager_->resetRefCounter(pId);
}

/*!
	@brief Frees StoreMemory acording to StoreMemoryLimit of PartitionGroup.
*/
UTIL_FORCEINLINE
void ObjectManager::freeLastLatchPhaseMemory(PartitionId pId) {
	try {
		chunkManager_->adjustStoreMemory(pId);
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
};

inline void ObjectManager::setSwapOutCounter(PartitionId pId, int64_t counter) {
	chunkManager_->setSwapOutCounter(pId, counter);
}
inline int64_t ObjectManager::getSwapOutCounter(PartitionId pId) {
	return chunkManager_->getSwapOutCounter(pId);
}
inline void ObjectManager::setStoreMemoryAgingSwapRate(PartitionId pId, double ratio) {
	chunkManager_->setStoreMemoryAgingSwapRate(pId, ratio);
}
inline double ObjectManager::getStoreMemoryAgingSwapRate(PartitionId pId) {
	return chunkManager_->getStoreMemoryAgingSwapRate(pId);
}

/*!
	@brief Returns an estimated size of the Object for requested size.
*/
inline Size_t ObjectManager::estimateAllocateSize(Size_t requestSize) {
	return (1U << objectAllocator_->getObjectExpSize(requestSize)) -
		   ObjectAllocator::BLOCK_HEADER_SIZE;
}
inline Size_t ObjectManager::getAllocateSize(uint32_t exponent) {
	return (1 << (CHUNK_EXP_SIZE_ - exponent)) - ObjectAllocator::BLOCK_HEADER_SIZE;
}

/*!
	@brief Returns an allocated and fixed Object for requested size for
   updating.
*/
template <class T>
T* ObjectManager::allocate(
		PartitionId pId, Size_t requestSize,
		const AllocateStrategy& allocateStrategy, OId& oId,
		ObjectType objectType) {
	uint8_t* addr =
			allocateObject(pId, requestSize, allocateStrategy, oId, objectType);
	return reinterpret_cast<T*>(addr);
}

/*!
	@brief Returns an allocated and fixed Object for requested size for
   updating, tried to allocate on the same Chunk of the specified neighbor
   Object.
*/
template <class T>
T* ObjectManager::allocateNeighbor(
		PartitionId pId, Size_t requestSize,
		const AllocateStrategy& allocateStrategy, OId& oId, OId neighborOId,
		ObjectType objectType) {

	uint8_t* addr = allocateNeighborObject(
			pId, requestSize, allocateStrategy, oId, neighborOId, objectType);
	return reinterpret_cast<T*>(addr);
};

	/*!
		@brief Frees all Objects on the Chunks, older than timestamp of
	   ChunkKey.
	*/
inline bool ObjectManager::batchFree(
		PartitionId pId, ChunkKey chunkKey, uint64_t maxScanNum,
		uint64_t& scanNum, uint64_t& freeChunkNum,
		ChunkKey simulateChunkKey, uint64_t& simulateFreeNum) {
	try {
		return chunkManager_->batchFreeChunk(
				pId, chunkKey, maxScanNum, scanNum, freeChunkNum,
				simulateChunkKey, simulateFreeNum);
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
};

inline void ObjectManager::purgeDataAffinityInfo(PartitionId pId, Timestamp baseTime) {
	return chunkManager_->purgeDataAffinityInfo(pId, baseTime);
};

/*!
	@brief Returns the fixed Object for updating, or it's dirty flag raised.
*/
template <class T>
UTIL_FORCEINLINE T* ObjectManager::getForUpdate(PartitionId pId, OId oId) {
	assert(UNDEF_OID != oId);
	validateOId(pId, oId);
	ChunkCategoryId categoryId = getChunkCategoryId(oId);
	ChunkId cId = getChunkId(oId);
	Offset_t offset = getOffset(oId);

	try {
		MetaChunk* metaChunk = chunkManager_->getChunkForUpdate(
				pId, categoryId, cId, UNDEF_CHUNK_KEY, true);
		uint8_t* objectAddr = metaChunk->getPtr() + offset;
		validateObject(objectAddr, pId, oId, categoryId, cId, offset);
		return reinterpret_cast<T*>(objectAddr);
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(
				e, "pId," << pId << ",oId," << oId <<
				",categoryId," << (int32_t)categoryId <<
				",cId," << cId << ",offset," << offset <<
				GS_EXCEPTION_MESSAGE(e));
	}
}

/*!
	@brief Returns the fixed Object for reading.
*/
template <class T>
UTIL_FORCEINLINE T* ObjectManager::getForRead(PartitionId pId, OId oId) {
	assert(UNDEF_OID != oId);
	validateOId(pId, oId);
	ChunkCategoryId categoryId = getChunkCategoryId(oId);
	ChunkId cId = getChunkId(oId);
	Offset_t offset = getOffset(oId);

	try {
		MetaChunk* metaChunk =
				chunkManager_->getChunk(pId, categoryId, cId);

		uint8_t* objectAddr = metaChunk->getPtr() + offset;
		validateObject(objectAddr, pId, oId, categoryId, cId, offset);
		return reinterpret_cast<T*>(objectAddr);
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(
				e, "pId," << pId << ",oId," << oId <<
				",categoryId," << (int32_t)categoryId <<
				",cId," << cId << ",offset," << offset <<
				GS_EXCEPTION_MESSAGE(e));
	}
}

template<typename T, AccessMode mode>
UTIL_FORCEINLINE T* ObjectManager::load(
		PartitionId pId, OId oId, OId* lastOId, uint8_t* lastAddr) {
	assert(oId != UNDEF_OID);

	T* result;
	if (notEqualChunk(oId, *lastOId)) {
		typedef ObjectAccessType<mode> AccessModeType;
		result = static_cast<T*>(reload(pId, oId, lastOId, AccessModeType()));
	}
	else {
		assert(*lastOId != UNDEF_OID);
		assert(objectAllocator_->isValidObject(lastAddr));

		result =
				reinterpret_cast<T*>(lastAddr - getRelativeOffset(*lastOId) +
						getRelativeOffset(oId));

		assert(objectAllocator_->isValidObject(result));
	}

	*lastOId = oId;
	return result;
}

/*!
	@brief Raises dirty flag of the Object.
*/
UTIL_FORCEINLINE
void ObjectManager::setDirty(PartitionId pId, OId oId) {
	assert(UNDEF_OID != oId);
	validateOId(pId, oId);
	ChunkCategoryId categoryId = getChunkCategoryId(oId);
	ChunkId cId = getChunkId(oId);

	try {
		chunkManager_->getChunkForUpdate(
				pId, categoryId, cId, UNDEF_CHUNK_KEY, false);
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(
				e, "pId," << pId << ",oId," << oId <<
				",categoryId," << (int32_t)categoryId <<
				",cId," << cId << ",offset=" << getOffset(oId) <<
				GS_EXCEPTION_MESSAGE(e));
	}
}

/*!
	@brief Returns the size the Object.
*/
inline Size_t ObjectManager::getSize(uint8_t* objectAddr) const {
	validateObject(objectAddr);
	return objectAllocator_->getObjectSize(objectAddr);
};

/*!
	@brief Returns the type the Object.
*/
inline ObjectType ObjectManager::getObjectType(uint8_t* objectAddr) const {
	validateObject(objectAddr);
	return objectAllocator_->getObjectType(objectAddr);
};

/*!
	@brief Validates RefCounter of fixing and unfixing.
*/
inline void ObjectManager::validateRefCounter(PartitionId pId) {
	if (existPartition(pId)) {
		ChunkCategoryId categoryId = 0;
		ChunkId cId = 0;
		uint64_t chunkNum = chunkManager_->getScanSize(pId);
		ChunkKey* chunkKey;
		MetaChunk* metaChunk = chunkManager_->begin(pId, categoryId, cId, chunkKey);
		for (uint64_t i = 0; i < chunkNum; i++) {
			if (metaChunk && !metaChunk->isFree()) {
				int32_t refCount =
						chunkManager_->getRefCount(pId, categoryId, cId);
				assert(refCount == 0);
				if (refCount != 0) {
					GS_THROW_SYSTEM_ERROR(GS_ERROR_OM_INVALID_OID,
						"refCount_ is not zero. "
							<< "pId = " << pId
							<< ", categoryId = " << (int32_t)categoryId
							<< ", cId = " << i << ", ref = " << refCount);
				}
			}
			metaChunk = chunkManager_->next(pId, categoryId, cId, chunkKey);
		}
	}
}

inline void ObjectManager::dumpRefCounter(PartitionId pId) {
	if (existPartition(pId)) {
		std::cout << "=========== dumpRefCounter pId = " << pId
				  << " =======" << std::endl;
		ChunkCategoryId categoryId = 0;
		ChunkId cId = 0;
		uint64_t chunkNum = chunkManager_->getScanSize(pId);
		ChunkKey *chunkKey;
		MetaChunk* metaChunk = chunkManager_->begin(pId, categoryId, cId, chunkKey);
		for (uint64_t i = 0; i < chunkNum; i++) {
			if (metaChunk && !metaChunk->isFree()) {
				int32_t refCount =
					chunkManager_->getRefCount(pId, categoryId, cId);
				std::cout << "pId = " << pId
						  << ", categoryId = " << (int32_t)categoryId
						  << ", cId = " << i << ", ref = " << refCount
						  << std::endl;
			}
			ChunkKey *chunkKey;
			metaChunk = chunkManager_->next(pId, categoryId, cId, chunkKey);
		}
	}
}

inline void ObjectManager::dumpObject(PartitionId pId, int32_t level) {
	if (existPartition(pId)) {
		try {
			std::cout << "=====pId, " << pId << std::endl;
			ChunkCategoryId categoryId = 0;
			ChunkCategoryId prevCategoryId = -1;
			ChunkId cId = 0;
			uint64_t chunkNum = chunkManager_->getScanSize(pId);
			ChunkKey *chunkKey;
			MetaChunk* metaChunk =
					chunkManager_->begin(pId, categoryId, cId, chunkKey);
			for (uint64_t i = 0; i < chunkNum; i++) {
				if (categoryId != prevCategoryId) {
					std::cout << "<<<<<categoryId, " << (int32_t)categoryId
							  << std::endl;
				}
				if (metaChunk && !metaChunk->isFree()) {
					metaChunk =
						chunkManager_->getChunk(pId, categoryId, cId);
					std::string str =
						objectAllocator_->dump(metaChunk->getPtr(), 1);
					std::cout << "-----cId, " << cId << std::endl;
					std::cout << str << std::endl;
					chunkManager_->unfix(pId, categoryId, cId);
				}
				prevCategoryId = categoryId;
				metaChunk = chunkManager_->next(pId, categoryId, cId, chunkKey);
			}
		}
		catch (std::exception& e) {
			GS_RETHROW_SYSTEM_ERROR(e, "");
		}
	}
}

inline void ObjectManager::dumpObjectDigest(PartitionId pId) {
	if (existPartition(pId)) {
		try {
			ChunkCategoryId categoryId = 0;
			ChunkCategoryId prevCategoryId = -1;
			ChunkId cId = 0;
			uint64_t chunkNum = chunkManager_->getScanSize(pId);
			ChunkKey *chunkKey;
			MetaChunk* metaChunk =
					chunkManager_->begin(pId, categoryId, cId, chunkKey);
			for (uint64_t i = 0; i < chunkNum; i++) {
				if (categoryId != prevCategoryId) {
				}
				if (metaChunk && !metaChunk->isFree()) {
					metaChunk = chunkManager_->getChunk(pId, categoryId, cId);
					std::cout << "pId," << pId << ",categoryId," <<
							(int32_t)categoryId << ",cId," << cId;
					std::string str =
							objectAllocator_->dumpDigest(metaChunk->getPtr());
					std::cout << str;
					str = ChunkManager::ChunkHeader::dump(metaChunk->getPtr());
					std::cout << "," << str << std::endl;
					chunkManager_->unfix(pId, categoryId, cId);
				}
				prevCategoryId = categoryId;
				metaChunk = chunkManager_->next(pId, categoryId, cId, chunkKey);
			}
		}
		catch (std::exception& e) {
			GS_RETHROW_SYSTEM_ERROR(e, "");
		}
	}
}

inline ObjectManager::DataAffinityInfo ObjectManager::makeDataAffinityInfo(
		const AllocateStrategy &strategy) {
	assert(strategy.expireCategoryId_ < ChunkManager::EXPIRE_INTERVAL_CATEGORY_COUNT);
	DataAffinityInfo affinityInfo;
	affinityInfo.expireCategory_ =
			strategy.expireCategoryId_ % ChunkManager::EXPIRE_INTERVAL_CATEGORY_COUNT;
	affinityInfo.updateCategory_ =
			strategy.affinityGroupId_ % ChunkManager::UPDATE_INTERVAL_CATEGORY_COUNT;
	return affinityInfo;
}

inline uint8_t* ObjectManager::allocateObject(
		MetaChunk& metaChunk, uint8_t powerSize,
		ObjectType objectType, uint32_t& offset, Size_t& size) {

	uint8_t maxFreeExpSize;
	uint8_t* addr = objectAllocator_->allocate(
			metaChunk.getPtr(), powerSize, objectType, offset, maxFreeExpSize);

	metaChunk.setUnoccupiedSize(maxFreeExpSize);

	Size_t objectWholeSize = (1U << powerSize);
	size = objectWholeSize - ObjectAllocator::BLOCK_HEADER_SIZE;
	metaChunk.addOccupiedSize(size);

	assert(size <= (1U << (CHUNK_EXP_SIZE_ - 1)));

	return addr;
}

inline Size_t ObjectManager::freeObject(MetaChunk& metaChunk, Offset_t offset) {
	uint8_t maxFreeExpSize;
	uint8_t powerSize =
			objectAllocator_->free(metaChunk.getPtr(), offset, maxFreeExpSize);

	metaChunk.setUnoccupiedSize(maxFreeExpSize);

	Size_t objectWholeSize = (1U << powerSize);
	Size_t size = objectWholeSize - ObjectAllocator::BLOCK_HEADER_SIZE;
	metaChunk.subtractOccupiedSize(size);

	assert(size <= (1U << (CHUNK_EXP_SIZE_ - 1)));

	return size;
}

inline void ObjectManager::validateOId(PartitionId pId, OId oId) {
	if (!isValidOId(oId)) {
		ChunkCategoryId categoryId = getChunkCategoryId(oId);
		ChunkId cId = getChunkId(oId);
		Offset_t offset = getOffset(oId);
		GS_THROW_SYSTEM_ERROR(GS_ERROR_OM_INVALID_OID,
			"OID is Invalid. "
				<< "pId," << pId << ",oId," << oId << ",categoryId,"
				<< (int32_t)categoryId << ",cId," << cId
				<< ",offset=" << offset);
	}
}

inline void ObjectManager::validateObject(
		uint8_t* objectAddr, PartitionId pId, OId oId,
		ChunkCategoryId categoryId, ChunkId cId, Offset_t offset) const {

	if (!objectAllocator_->isValidObject(objectAddr)) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_OM_INVALID_OBJECT,
			"object is Invalid. "
				<< "pId," << pId << ",oId," << oId << ",categoryId,"
				<< (int32_t)categoryId << ",cId," << cId << ",offset="
				<< offset << ", " << util::StackTraceUtils::getStackTrace);
	}
}

inline OId ObjectManager::getOId(
		ChunkCategoryId categoryId, ChunkId cId, Offset_t offset) {

	ASSERT_ISVALID_CATEGORYID(categoryId);
	ASSERT_ISVALID_CHUNKID(cId);
	ASSERT_ISVALID_OFFSET(offset);
	OId chunkIdOId = ((OId)cId << UNIT_CHUNK_SHIFT_BIT);
	OId unitOffsetOId = (((OId)offset >> UNIT_OFFSET_ROUND_BIT)
						 << UNIT_OFFSET_SHIFT_BIT);  
	OId categoryIdOId = ((OId)categoryId << CATEGORY_ID_SHIFT_BIT);
	OId magicOId = MAGIC_NUMBER;
	return (magicOId | categoryIdOId | unitOffsetOId | chunkIdOId);
}

inline OId ObjectManager::getOId(
		uint32_t chunkExpSize, ChunkCategoryId categoryId,
		ChunkId cId, Offset_t offset) {

	ASSERT_ISVALID_CATEGORYID(categoryId);

	const bool largeChunkMode = OIdBitUtils::isLargeChunkMode(chunkExpSize);
	const uint32_t UNIT_CHUNK_SHIFT_BIT = calcUnitChunkShiftBit(largeChunkMode);
	OId chunkIdOId = ((OId)cId << UNIT_CHUNK_SHIFT_BIT);
	OId unitOffsetOId = (((OId)offset >> UNIT_OFFSET_ROUND_BIT)
						 << UNIT_OFFSET_SHIFT_BIT);  
	OId categoryIdOId = ((OId)categoryId << CATEGORY_ID_SHIFT_BIT);
	OId magicOId = MAGIC_NUMBER;
	return (magicOId | categoryIdOId | unitOffsetOId | chunkIdOId);
}

inline ChunkId ObjectManager::getChunkId(OId oId) {
	ChunkId cId = static_cast<ChunkId>(MASK_32BIT & (oId >> UNIT_CHUNK_SHIFT_BIT));
	ASSERT_ISVALID_CHUNKID(cId);
	return cId;
}

inline Offset_t ObjectManager::getOffset(OId oId) {
	Offset_t offset = getRelativeOffset(oId) + ObjectAllocator::BLOCK_HEADER_SIZE;
	ASSERT_ISVALID_OFFSET(offset);
	return offset;
}

inline Offset_t ObjectManager::getRelativeOffset(OId oId) {
	const OId offsetMaskBit =
			(static_cast<OId>(1) << static_cast<OId>(MAX_UNIT_OFFSET_BIT)) - 1;
	const OId mask = offsetMaskBit << UNIT_OFFSET_SHIFT_BIT;
	const uint32_t shiftBits =
			UNIT_OFFSET_SHIFT_BIT - UNIT_OFFSET_ROUND_BIT;
	const Offset_t offset =
			static_cast<Offset_t>((mask & oId) >> shiftBits);
	ASSERT_ISVALID_OFFSET(offset + ObjectAllocator::BLOCK_HEADER_SIZE);
	return offset;
}

inline ChunkCategoryId ObjectManager::getChunkCategoryId(OId oId) {
	ChunkCategoryId categoryId = static_cast<ChunkCategoryId>(
			MASK_3BIT & (oId >> CATEGORY_ID_SHIFT_BIT));
	ASSERT_ISVALID_CATEGORYID(categoryId);
	return categoryId;
}

inline OId ObjectManager::getUserArea(OId oId) {
	OId userArea = oId & MASK_USER;
	return userArea;
}

inline OId ObjectManager::setUserArea(OId oId, OId userArea) {
	ASSERT_ISVALID_USER(userArea);
	OId addedOId = getBaseArea(oId) | userArea;
	return addedOId;
}

inline OId ObjectManager::getBaseArea(OId oId) {
	OId baseArea = oId & (~MASK_USER);
	return baseArea;
}

inline bool ObjectManager::isValidOId(OId oId) {
	uint64_t magic = static_cast<uint64_t>(MASK_MAGIC & oId);
	return (magic == MAGIC_NUMBER);
}

inline bool ObjectManager::notEqualChunk(OId oId1, OId oId2) {
	return ((oId1 & ~MASK_UNIT_OFFSET) != (oId2 & ~MASK_UNIT_OFFSET));
}


#endif
