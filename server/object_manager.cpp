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
	@brief Implementation of ObjectManager
*/
#include "object_manager.h"
#include "util/trace.h"
#include "gs_error.h"
#include <algorithm>
#include <cassert>
#include <iostream>
#include <math.h>


#ifdef _MSC_VER
#pragma warning(disable : 4996)
#endif

/*!
	@brief Constructor of ObjectManager
*/
ObjectManager::ObjectManager(
	const ConfigTable &configTable, ChunkManager *chunkManager)
	: PARTITION_NUM_(configTable.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM)),
	  CHUNK_EXP_SIZE_(util::nextPowerBitsOf2(
		  configTable.getUInt32(CONFIG_TABLE_DS_STORE_BLOCK_SIZE))),
	  chunkManager_(chunkManager),
	  objectAllocator_(NULL),
	  maxObjectSize_(
		  (1 << (CHUNK_EXP_SIZE_ - 1)) - ObjectAllocator::BLOCK_HEADER_SIZE),
	  halfOfMaxObjectSize_(
		  (1 << (CHUNK_EXP_SIZE_ - 2)) - ObjectAllocator::BLOCK_HEADER_SIZE),
	  recommendLimitObjectSize_(
		  (1 << (CHUNK_EXP_SIZE_ - 4)) - ObjectAllocator::BLOCK_HEADER_SIZE)
	  ,
	  isZeroFill_(configTable.get<int32_t>(CONFIG_TABLE_DS_STORE_COMPRESSION_MODE) == 
				  0 ? false: true)
	{
	if (CHUNK_EXP_SIZE_ <= 0 || MAX_CHUNK_EXP_SIZE < CHUNK_EXP_SIZE_) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_OM_INVALID_CHUNK_EXP_SIZE, "");
	}
	if (!chunkManager_) {
		GS_THROW_SYSTEM_ERROR(
			GS_ERROR_OM_CONSTRUCTOR_FAILED, "invalid chunkManager");
	}

	try {
		objectAllocator_ =
			UTIL_NEW ObjectAllocator(CHUNK_EXP_SIZE_, CHUNK_HEADER_BLOCK_SIZE, isZeroFill_);
	}
	catch (std::exception &e) {
		delete objectAllocator_;
		objectAllocator_ = NULL;
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}

ObjectManager::ObjectManager()
	: PARTITION_NUM_(1),
	  CHUNK_EXP_SIZE_(MAX_CHUNK_EXP_SIZE),
	  chunkManager_(NULL),
	  objectAllocator_(NULL),
	  maxObjectSize_(
		  (1 << (CHUNK_EXP_SIZE_ - 1)) - ObjectAllocator::BLOCK_HEADER_SIZE),
	  halfOfMaxObjectSize_(
		  (1 << (CHUNK_EXP_SIZE_ - 2)) - ObjectAllocator::BLOCK_HEADER_SIZE)
	  ,
	  isZeroFill_(false)
{}

/*!
	@brief Destructor of ObjectManager
*/
ObjectManager::~ObjectManager() {
	delete objectAllocator_;
}

uint8_t *ObjectManager::allocateObject(PartitionId pId, Size_t requestSize,
	const AllocateStrategy &allocateStrategy, OId &oId, ObjectType objectType) {
	const ChunkCategoryId categoryId = allocateStrategy.categoryId_;
	const DataAffinityInfo affinityInfo = makeDataAffinityInfo(allocateStrategy);
	try {
		const uint8_t powerSize =
			objectAllocator_->getObjectExpSize(requestSize);

		ChunkId cId;
		MetaChunk *metaChunk = chunkManager_->searchChunk(pId, categoryId,
				affinityInfo, allocateStrategy.chunkKey_, powerSize, cId);
		assert(cId != UNDEF_CHUNKID);

		if (metaChunk == NULL) {
			metaChunk = chunkManager_->allocateChunk(pId, categoryId,
					affinityInfo, allocateStrategy.chunkKey_, cId);
			assert(cId <= OBJECT_MAX_CHUNK_ID_);  
			uint8_t maxFreeExpSize;
			objectAllocator_->initializeChunk(
				metaChunk->getPtr(), maxFreeExpSize);
			metaChunk->setUnoccupiedSize(maxFreeExpSize);
		}

		assert(metaChunk != NULL);
		assert(cId != UNDEF_CHUNKID);

		uint32_t offset;
		Size_t size;
		uint8_t *addr =
			allocateObject(*metaChunk, powerSize, objectType, offset, size);

		oId = getOId(categoryId, cId, offset);
		assert(isValidOId(oId));
		return addr;
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(
			e, "PID=" << pId << ",requestSize=" << requestSize
					  << ",categoryId=" << (int32_t)categoryId
					  << ",chunkKey=" << allocateStrategy.chunkKey_
					  << ",objectType=" << (int32_t)objectType
					  << GS_EXCEPTION_MESSAGE(e));
	}
	return NULL;
}
uint8_t *ObjectManager::allocateNeighborObject(PartitionId pId,
	Size_t requestSize, const AllocateStrategy &allocateStrategy, OId &oId,
	OId neighborOId, ObjectType objectType) {

	validateOId(pId, neighborOId);
	ChunkCategoryId categoryId = getChunkCategoryId(neighborOId);
	ChunkId cId = getChunkId(neighborOId);

	try {
		uint8_t powerSize = objectAllocator_->getObjectExpSize(requestSize);

		MetaChunk *metaChunk = chunkManager_->searchChunk(
			pId, categoryId, cId, allocateStrategy.chunkKey_, powerSize);

		if (metaChunk && !metaChunk->isFree()) {
			uint32_t offset;
			Size_t size;
			uint8_t *addr =
				allocateObject(*metaChunk, powerSize, objectType, offset, size);

			oId = getOId(categoryId, cId, offset);
			assert(isValidOId(oId));
			return addr;
		}
		else {
			return allocateObject(
				pId, requestSize, allocateStrategy, oId, objectType);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(
			e, "PID=" << pId << ",requestSize=" << requestSize
					  << ",categoryId=" << (int32_t)categoryId
					  << ",chunkKey=" << allocateStrategy.chunkKey_
					  << ",objectType=" << (int32_t)objectType << ",OID="
					  << neighborOId << ",CID=" << getChunkId(neighborOId)
					  << ",offset=" << getOffset(neighborOId)
					  << GS_EXCEPTION_MESSAGE(e));
	}
}

/*!
	@brief Frees the Object.
*/
void ObjectManager::free(PartitionId pId, OId oId) {
	validateOId(pId, oId);
	ChunkCategoryId categoryId = getChunkCategoryId(oId);
	ChunkId cId = getChunkId(oId);
	uint32_t offset = getOffset(oId);

	try {
		MetaChunk *metaChunk = chunkManager_->getChunkForUpdate(
			pId, categoryId, cId, UNDEF_CHUNK_KEY, false);
		freeObject(*metaChunk, offset);

		if (metaChunk->getOccupiedSize() == 0) {
			chunkManager_->freeChunk(pId, categoryId, cId);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(
			e, "pId," << pId << ",oId," << oId << ",categoryId,"
					  << (int32_t)categoryId << ",cId," << cId << ",offset,"
					  << offset << GS_EXCEPTION_MESSAGE(e));
	}
}

void* ObjectManager::reload(PartitionId pId, OId oId, OId *lastOId, const ObjectReadType&)
{
	if (*lastOId != UNDEF_OID) {
		unfix(pId, *lastOId);
		*lastOId = UNDEF_OID;
	}

	return getForRead<uint8_t>(pId, oId);
}

void* ObjectManager::reload(PartitionId pId, OId oId, OId *lastOId, const ObjectWriteType&)
{
	if (*lastOId != UNDEF_OID) {
		unfix(pId, *lastOId);
		*lastOId = UNDEF_OID;
	}

	return getForUpdate<uint8_t>(pId, oId);
}

