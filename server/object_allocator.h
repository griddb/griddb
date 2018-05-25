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
	@brief Definition of ObjectAllocator
*/
#ifndef OBJECT_ALLOCATOR_H_
#define OBJECT_ALLOCATOR_H_

#include "data_type.h"
#include <cassert>
#include <iostream>
#include <map>


typedef int8_t ObjectType;

/*!
	@brief Allocate object on the buffer.
*/
class ObjectAllocator {
	friend class ObjectManagerTest;

public:
	static const ObjectType OBJECT_TYPE_UNKNOWN = 0;
	static const ObjectType OBJECT_TYPE_CHUNK_HEADER = 1;
	static const uint8_t BLOCK_HEADER_SIZE =
		static_cast<uint8_t>(sizeof(uint32_t));

	ObjectAllocator(uint8_t chunkExpSize, uint32_t chunkHeaderSize, bool isZeroFill = false);
	~ObjectAllocator();

	void initializeChunk(uint8_t *chunkTop, uint8_t &maxFreeExpSize);

	/*!
		@brief Returns an exponent of the estimated object size in case of a
	   power base is 2.
	*/
	uint8_t getObjectExpSize(uint32_t requestSize) {
		assert(0 < requestSize);
		const uint32_t alignedSize =
			ObjectAllocator::align(requestSize + hSize_);
		uint8_t k = ObjectAllocator::log2(alignedSize);
		if (k < minPower_) {
			k = minPower_;
		}
		return k;
	}

	uint8_t *allocate(uint8_t *chunkTop, uint8_t expSize, ObjectType type,
		uint32_t &offset, uint8_t &maxFreeExpSize);

	uint8_t free(uint8_t *chunkTop, uint32_t offset, uint8_t &maxFreeExpSize);

	/*!
		@brief Returns the size of the Object.
	*/
	Size_t getObjectSize(uint8_t *objectAddr) {
		assert(isValidObject(objectAddr));
		HeaderBlock *header = getHeaderBlock(objectAddr, -hSize_);
		uint8_t k = header->getPower();
		uint32_t freedBlockSize = UINT32_C(1) << k;
		return freedBlockSize - hSize_;
	}

	/*!
		@brief Returns the type of the Object.
	*/
	ObjectType getObjectType(uint8_t *objectAddr) {
		assert(isValidObject(objectAddr));
		HeaderBlock *header = getHeaderBlock(objectAddr, -hSize_);
		return header->getType();
	}

	/*!
		@brief Validates header of the Object.
	*/
	bool isValidObject(uint8_t *objectAddr) {
		HeaderBlock *headerBlock =
			reinterpret_cast<HeaderBlock *>(objectAddr - hSize_);
		return headerBlock->isValid() && headerBlock->isUsed();
	}

private:
	/*!
		@file
		@brief Manages empty object chains.
	*/
	class HeaderBlock {
	private:
		uint8_t power_;  
		int8_t type_;  
		uint16_t check_;  

		static const uint16_t MAGIC_NUMBER = 0xc1ac;  

	public:
		int32_t prev_;
		int32_t next_;

		uint8_t getPower() const {
			return power_ & 0x7fu;
		}

		void setUsed(
			bool isUsed, uint32_t power, int8_t type = OBJECT_TYPE_UNKNOWN) {
			power_ = static_cast<uint8_t>(0x80u * isUsed + (power & 0x0000007fu));
			type_ = type;
			check_ = MAGIC_NUMBER;
		}

		bool isUsed() const {
			return 0 != (power_ & 0x80u);
		}

		bool isValid() const {
			return MAGIC_NUMBER == check_;
		}

		int8_t getType() const {
			return type_;
		}

		HeaderBlock()
			: power_(0),
			  type_(ObjectAllocator::OBJECT_TYPE_UNKNOWN),
			  check_(MAGIC_NUMBER),
			  prev_(0),
			  next_(0) {}
		~HeaderBlock() {}

	private:
		HeaderBlock(const HeaderBlock &);
		HeaderBlock &operator=(const HeaderBlock &);
	};  

	inline HeaderBlock *getHeaderBlock(uint8_t *base, int32_t offset) const {
		return reinterpret_cast<HeaderBlock *>(base + offset);
	}

	inline uint32_t *getFreeList(uint8_t *chunkTop) const {
		return reinterpret_cast<uint32_t *>(chunkTop + freeListOffset_);
	}

	inline uint8_t getMaxFreeExpSize(uint8_t *chunkTop) const {
		assert(*static_cast<uint8_t *>(chunkTop + maxFreeExpSizeOffset_) <=
			   maxPower_);
		return *static_cast<uint8_t *>(chunkTop + maxFreeExpSizeOffset_);
	}

	inline void setMaxFreeExpSize(uint8_t *chunkTop, uint8_t expSize) {
		assert(expSize <= maxPower_);
		*static_cast<uint8_t *>(chunkTop + maxFreeExpSizeOffset_) = expSize;
	}

	uint8_t findMaxFreeBlockSize(uint8_t *chunkTop, uint8_t startPower);


	const uint8_t CHUNK_EXP_SIZE_;
	const int32_t CHUNK_SIZE_;

	uint32_t chunkHeaderSize_;
	uint8_t minPower_;  
	uint8_t maxPower_;  
	uint32_t tableSize_;
	uint8_t
		hSize_;  
	int32_t
		freeListOffset_;  
	int32_t maxFreeExpSizeOffset_;
	bool isZeroFill_; 

	ObjectAllocator(const ObjectAllocator &);
	ObjectAllocator &operator=(const ObjectAllocator &);

public:
	void dumpFreeChain(uint8_t *top, int32_t offset, int32_t k) const;
	void dumpFreeChain(uint8_t *top, int32_t k) const;
	std::string dump(uint8_t *chunk, int32_t level = 0) const;
	void dumpSummary(uint8_t *top,
		std::map<int8_t, std::pair<uint32_t, uint32_t> > &typeStatMap,
		uint64_t &summaryFreeSize, uint64_t &summaryUseSize) const;


private:

	inline static uint32_t align(uint32_t x) {
		x = x - 1;
		x = x | (x >> 1);
		x = x | (x >> 2);
		x = x | (x >> 4);
		x = x | (x >> 8);
		x = x | (x >> 16);

		return x + 1;
	};

	static const uint8_t ZeroTable[256];

	inline static uint8_t log2(uint32_t x) {
		assert(x);

		uint32_t y = 0;
		uint32_t nlz = 32;

		y = x >> 16;
		if (0 != y) {
			nlz = nlz - 16;
			x = y;
		}
		y = x >> 8;
		if (0 != y) {
			nlz = nlz - 8;
			x = y;
		}
		nlz -= ZeroTable[x];

		return static_cast<uint8_t>(31 - nlz);
	};
};

#endif
