/*
	Copyright (c) 2012 TOSHIBA CORPORATION.

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
	@brief Implementation of ObjectAllocator
*/


#include "object_allocator.h"
#include "gs_error.h"
#include <algorithm>  
#include <cstring>	
#include <limits.h>
#include <stdio.h>

const uint8_t ObjectAllocator::ZeroTable[256] = {
	0, 1, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5, 5,
	5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
	6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
	8, 8, 8, 8, 8, 8,
};

/*!
	@brief Constructor of ObjectAllocator
*/
ObjectAllocator::ObjectAllocator(uint8_t chunkExpSize, uint32_t chunkHeaderSize)
	: CHUNK_EXP_SIZE_(chunkExpSize),
	  CHUNK_SIZE_(1L << CHUNK_EXP_SIZE_),
	  chunkHeaderSize_(chunkHeaderSize),
	  minPower_(0),
	  maxPower_(0),
	  tableSize_(0),
	  hSize_(0) {
	hSize_ = sizeof(uint32_t);  

	minPower_ = ObjectAllocator::log2(
		ObjectAllocator::align(sizeof(HeaderBlock)));  

	const uint32_t alignedSize = ObjectAllocator::align(CHUNK_SIZE_);  
	maxPower_ = ObjectAllocator::log2(alignedSize);  

	tableSize_ = sizeof(uint32_t) *
				 (maxPower_ + 1);  

	freeListOffset_ =
		chunkHeaderSize - tableSize_;  
	maxFreeExpSizeOffset_ = freeListOffset_ - sizeof(uint8_t);
	/*assert((freeListOffset_ + sizeof(uint32_t) * minPower_) >= (hSize_ +
	 * chunkInfoSize));*/
}

/*!
	@brief Destructor of ObjectAllocator
*/
ObjectAllocator::~ObjectAllocator() {}

/*!
	@brief Initialize header of the Chunk for allocating Objects.
*/
void ObjectAllocator::initializeChunk(
	uint8_t *chunkTop, uint8_t &maxFreeExpSize) {
	uint32_t *freeList = getFreeList(chunkTop);
	memset(freeList, 0xff, tableSize_ - 1);
	freeList[maxPower_] = 0;  
	setMaxFreeExpSize(chunkTop, maxPower_);

	HeaderBlock *header = getHeaderBlock(chunkTop, 0);
	header->setUsed(false, maxPower_);  
	header->next_ = header->prev_ = 0;  

	Size_t requestSize = chunkHeaderSize_ - hSize_;
	uint8_t k = getObjectExpSize(requestSize);
	ObjectType objectType = OBJECT_TYPE_CHUNK_HEADER;
	uint32_t offset;
	allocate(chunkTop, k, objectType, offset, maxFreeExpSize);
}

/*!
	@brief Returns an allocated Object.
*/
uint8_t *ObjectAllocator::allocate(uint8_t *chunkTop, uint8_t expSize,
	ObjectType type, uint32_t &offset, uint8_t &maxFreeExpSize) {
	assert(minPower_ <= expSize);
	uint32_t *freeList = getFreeList(chunkTop);
	maxFreeExpSize = getMaxFreeExpSize(chunkTop);
	uint8_t k = expSize;

	const uint8_t kmin = k;

	HeaderBlock *list = 0;
	for (; k <= maxFreeExpSize; ++k)  
	{
		if (freeList[k] != UINT32_MAX) {
			list = getHeaderBlock(chunkTop, freeList[k]);
			assert(list->isValid());
			break;
		}
	}

	if ((freeList[k] == UINT32_MAX) || k > maxPower_) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_OA_NO_ROOM, "");
	}

	bool isCheckMaxFreeExpSize = false;

	if (list->next_ ==
		static_cast<int32_t>(reinterpret_cast<uint8_t *>(list) - chunkTop)) {
		freeList[k] = UINT32_MAX;

		if (k == maxFreeExpSize) {
			maxFreeExpSize--;
			if (expSize == k) {
				isCheckMaxFreeExpSize = true;
			}
			else if (maxFreeExpSize < minPower_) {
				maxFreeExpSize = 0;
			}
			setMaxFreeExpSize(chunkTop, maxFreeExpSize);
		}
	}
	else {
		getHeaderBlock(chunkTop, list->next_)->prev_ = list->prev_;
		getHeaderBlock(chunkTop, list->prev_)->next_ = list->next_;
		freeList[k] = list->next_;  
	}

	HeaderBlock *header = list;
	header->setUsed(
		true, kmin, type);  

	while (kmin < k--) {
		HeaderBlock *buddy =
			getHeaderBlock(reinterpret_cast<uint8_t *>(header), (1ULL << k));
		buddy->setUsed(false, k);

		freeList[k] =
			static_cast<int32_t>(reinterpret_cast<uint8_t *>(buddy) - chunkTop);
		buddy->next_ = freeList[k];
		buddy->prev_ = freeList[k];
	}
	assert(kmin == k + 1);

	if (isCheckMaxFreeExpSize) {
		maxFreeExpSize = findMaxFreeBlockSize(chunkTop, maxFreeExpSize);
		setMaxFreeExpSize(chunkTop, maxFreeExpSize);
	}

	uint8_t *objectAddr = reinterpret_cast<uint8_t *>(header) + hSize_;
	assert(isValidObject(objectAddr));
	if (!objectAddr) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_OA_ALLOCATE_FAILED,
			"chunk*, " << chunkTop << ", expSize, " << expSize);
	}
	assert(objectAddr > chunkTop);
	offset = static_cast<uint32_t>(objectAddr - chunkTop);
	return objectAddr;
}

/*!
	@brief Frees an allocated Object.
*/
uint8_t ObjectAllocator::free(
	uint8_t *chunkTop, uint32_t offset, uint8_t &maxFreeExpSize) {
	uint8_t *objectAddr = chunkTop + offset;
	uint32_t *freeList = getFreeList(chunkTop);
	maxFreeExpSize = getMaxFreeExpSize(chunkTop);
	HeaderBlock *header = getHeaderBlock(objectAddr, -hSize_);
	int32_t headerOffset = static_cast<int32_t>(offset - hSize_);

	if (!header->isValid()) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_OA_BAD_ADDRESS, "");
	}
	if (!header->isUsed()) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_OA_DOUBLE_FREE, "");
	}

	uint8_t k = header->getPower();
	uint8_t freedBlockSize = k;
	int32_t mask = 1L << k;
	HeaderBlock *buddy = getHeaderBlock(chunkTop, (headerOffset ^ mask));
	for (;;) {
		assert(buddy->isValid());

		if (buddy->isUsed() ||		 
			k == maxPower_ ||		 
			buddy->getPower() != k)  
		{
			header->setUsed(false, header->getPower());

			if (freeList[k] != UINT32_MAX) {
				HeaderBlock *it = getHeaderBlock(chunkTop, freeList[k]);
				assert(it->isValid());
				getHeaderBlock(chunkTop, it->prev_)->next_ =
					static_cast<int32_t>(
						reinterpret_cast<uint8_t *>(header) - chunkTop);
				header->next_ = static_cast<int32_t>(
					reinterpret_cast<uint8_t *>(it) - chunkTop);
				header->prev_ = it->prev_;
				it->prev_ = static_cast<int32_t>(
					reinterpret_cast<uint8_t *>(header) - chunkTop);
			}
			else {
				freeList[k] = static_cast<int32_t>(
					reinterpret_cast<uint8_t *>(header) - chunkTop);
				header->next_ = freeList[k];
				header->prev_ = freeList[k];
			}

			if (maxFreeExpSize < k) {
				maxFreeExpSize = k;
				setMaxFreeExpSize(chunkTop, maxFreeExpSize);
			}
			return freedBlockSize;
		}

		if (buddy->next_ ==
			static_cast<int32_t>(
				reinterpret_cast<uint8_t *>(buddy) - chunkTop)) {
			freeList[k] = UINT32_MAX;
		}
		else {
			getHeaderBlock(chunkTop, buddy->prev_)->next_ = buddy->next_;
			assert(getHeaderBlock(chunkTop, buddy->prev_)->isValid());
			getHeaderBlock(chunkTop, buddy->next_)->prev_ = buddy->prev_;
			assert(getHeaderBlock(chunkTop, buddy->next_)->isValid());
			freeList[k] = buddy->next_;  
		}

		header = std::min(header, buddy);
		assert((uintptr_t)header >= (uintptr_t)chunkTop);
		headerOffset =
			static_cast<int32_t>(reinterpret_cast<uintptr_t>(header) -
								 reinterpret_cast<uintptr_t>(chunkTop));
		header->setUsed(false, ++k);
		mask <<= 1;
		buddy = getHeaderBlock(chunkTop, (headerOffset ^ mask));
	}

	if (maxFreeExpSize < k) {
		maxFreeExpSize = k;
		setMaxFreeExpSize(chunkTop, maxFreeExpSize);
	}
	return freedBlockSize;  
}

void ObjectAllocator::dumpFreeChain(
	uint8_t *top, int32_t offset, int32_t k) const {
	if (offset == INT_MAX) {
		printf("[%d]FreeChain is empty\n", k);
	}
	else {
		HeaderBlock *start = getHeaderBlock(top, offset);
		HeaderBlock *cursor = start;
		while (true) {
			printf("[%d][%p]self=0x%0x, ->prev_=0x%0x, ->next_=0x%0x\n", k,
				cursor,
				static_cast<int32_t>(reinterpret_cast<uint8_t *>(cursor) - top),
				cursor->prev_, cursor->next_);
			if (static_cast<int32_t>(reinterpret_cast<uint8_t *>(cursor) -
									 top) == cursor->prev_) {
				break;
			}
			cursor = getHeaderBlock(top, cursor->prev_);
			if (start == cursor) {
				break;
			}
		}
	}
}

void ObjectAllocator::dumpFreeChain(uint8_t *top, int32_t k) const {
	uint32_t *freeList = getFreeList(top);
	if (freeList[k] != UINT32_MAX) {
		HeaderBlock *start = getHeaderBlock(top, freeList[k]);
		HeaderBlock *cursor = start;
		while (true) {
			printf("[%d][x%p]self=x%x, ->prev_=x%x, ->next_=x%x\n", k, cursor,
				static_cast<int32_t>(reinterpret_cast<uint8_t *>(cursor) - top),
				cursor->prev_, cursor->next_);
			cursor = getHeaderBlock(top, cursor->prev_);
			if (start == cursor) {
				break;
			}
		}
	}
}

std::string ObjectAllocator::dump(uint8_t *top, int32_t level) const {
	util::NormalOStringStream stream;

	stream << "[ObjectArea]" << std::endl;
	uint32_t *freeList = getFreeList(top);
	HeaderBlock *list = 0;
	uint32_t totalFreeSize = 0;
	std::map<int32_t, uint32_t> freeOffsetMap;
	stream << "(FreeArea)" << std::endl;
	for (uint8_t k = minPower_; k <= maxPower_; ++k) {
		uint32_t powerFreeSize = 0;
		list = getHeaderBlock(top, freeList[k]);
		int32_t counter = 0;
		uint32_t offset = freeList[k];
		if (offset != UINT32_MAX) {
			HeaderBlock *start = getHeaderBlock(top, offset);
			HeaderBlock *cursor = start;
			while (true) {
				printf("[%d][x%p]self=x%x, ->prev_=x%x, ->next_=x%x\n", k,
					cursor, static_cast<int32_t>(
								reinterpret_cast<uint8_t *>(cursor) - top),
					cursor->prev_, cursor->next_);
				freeOffsetMap.insert(std::make_pair(
					static_cast<int32_t>(
						reinterpret_cast<uint8_t *>(cursor) - top),
					1UL << k));
				counter++;
				if (static_cast<int32_t>(reinterpret_cast<uint8_t *>(cursor) -
										 top) == cursor->prev_) {
					break;
				}
				cursor = getHeaderBlock(top, cursor->prev_);
				if (start == cursor) {
					break;
				}
			}
		}
		powerFreeSize = (1UL << k) * counter;
		totalFreeSize += powerFreeSize;
		stream << "[" << static_cast<uint32_t>(k) << "]" << counter << "("
			   << powerFreeSize << ")" << std::endl;
	}

	int32_t objOffset = 0;
	std::map<int32_t, uint32_t>::iterator offsetItr;
	std::map<int8_t, std::pair<uint32_t, uint32_t> > typeStatMap;
	std::map<int8_t, std::pair<uint32_t, uint32_t> >::iterator typeItr;
	if (level > 0) {
		stream << "(AllArea)" << std::endl;
	}
	while (objOffset < CHUNK_SIZE_) {
		offsetItr = freeOffsetMap.find(objOffset);
		if (offsetItr != freeOffsetMap.end()) {
			if (level > 0) {
				stream << "[" << objOffset << ", " << offsetItr->second
					   << "](Free Object)" << std::endl;
			}
			objOffset += offsetItr->second;
		}
		else {
			HeaderBlock *header = getHeaderBlock(top, objOffset);
			uint32_t objSize = (1UL << header->getPower());
			if (level > 0) {
				stream << "[" << objOffset << ", " << objSize << ", "
					   << static_cast<int>(header->getType()) << "]";
			}
			if (!header->isUsed()) {
				stream << "<Bad Status : not used>" << std::endl;
			}
			if (!header->isValid()) {
				stream << "<Bad Status : is not valid>" << std::endl;
			}
			if (level > 0) {
			}
			objOffset += objSize;

			typeItr = typeStatMap.find(header->getType());
			if (typeItr != typeStatMap.end()) {
				typeItr->second.first++;
				typeItr->second.second += objSize;
			}
			else {
				std::pair<uint32_t, uint32_t> typePair(1, objSize);
				typeStatMap.insert(std::make_pair(header->getType(), typePair));
			}
		}
	}

	stream << "(TypeUse)" << std::endl;
	for (typeItr = typeStatMap.begin(); typeItr != typeStatMap.end();
		 typeItr++) {
		stream
			<< "num = " << typeItr->second.first
			<< ", size = " << typeItr->second.second << std::endl;
	}

	const uint32_t chunkHeaderSize =
		ObjectAllocator::align(chunkHeaderSize_ - hSize_);
	stream << "totalFreeSize = " << totalFreeSize << std::endl;
	stream << "totalUseSize = " << CHUNK_SIZE_ - totalFreeSize - chunkHeaderSize
		   << std::endl;
	return stream.str();
}

void ObjectAllocator::dumpSummary(uint8_t *top,
	std::map<int8_t, std::pair<uint32_t, uint32_t> > &typeStatMap,
	uint64_t &summaryFreeSize, uint64_t &summaryUseSize) const {
	uint32_t *freeList = getFreeList(top);
	HeaderBlock *list = 0;
	uint32_t totalFreeSize = 0;
	std::map<int32_t, uint32_t> freeOffsetMap;
	for (uint8_t k = minPower_; k <= maxPower_; ++k) {
		uint32_t powerFreeSize = 0;
		list = getHeaderBlock(top, freeList[k]);
		int32_t counter = 0;
		int32_t offset = freeList[k];
		if (offset != INT32_MAX) {
			HeaderBlock *start = getHeaderBlock(top, offset);
			HeaderBlock *cursor = start;
			while (true) {
				freeOffsetMap.insert(std::make_pair(
					static_cast<int32_t>(
						reinterpret_cast<uint8_t *>(cursor) - top),
					1UL << k));
				counter++;
				if (static_cast<int32_t>(reinterpret_cast<uint8_t *>(cursor) -
										 top) == cursor->prev_) {
					break;
				}
				cursor = getHeaderBlock(top, cursor->prev_);
				if (start == cursor) {
					break;
				}
			}
		}
		powerFreeSize = (1UL << k) * counter;
		totalFreeSize += powerFreeSize;
	}

	int32_t objOffset = 0;
	std::map<int32_t, uint32_t>::iterator offsetItr;
	std::map<int8_t, std::pair<uint32_t, uint32_t> >::iterator typeItr;
	while (objOffset < CHUNK_SIZE_) {
		offsetItr = freeOffsetMap.find(objOffset);
		if (offsetItr != freeOffsetMap.end()) {
			objOffset += offsetItr->second;
		}
		else {
			HeaderBlock *header = getHeaderBlock(top, objOffset);
			uint32_t objSize = (1UL << header->getPower());
			if (!header->isUsed()) {
				std::cerr << "<Bad Status : not used>" << std::endl;
			}
			if (!header->isValid()) {
				std::cerr << "<Bad Status : is not valid>" << std::endl;
			}
			objOffset += objSize;

			typeItr = typeStatMap.find(header->getType());
			if (typeItr != typeStatMap.end()) {
				typeItr->second.first++;
				typeItr->second.second += objSize;
			}
			else {
				std::pair<uint32_t, uint32_t> typePair(0, 0);
				typeStatMap.insert(std::make_pair(header->getType(), typePair));
			}
		}
	}

	const uint32_t chunkHeaderSize =
		ObjectAllocator::align(chunkHeaderSize_ - hSize_);
	summaryFreeSize += totalFreeSize;
	summaryUseSize += CHUNK_SIZE_ - totalFreeSize - chunkHeaderSize;
}

uint8_t ObjectAllocator::findMaxFreeBlockSize(
	uint8_t *chunkTop, uint8_t startPower) {
	if (CHUNK_EXP_SIZE_ < startPower) {
		startPower = CHUNK_EXP_SIZE_;
	}
	uint32_t *freeList = getFreeList(chunkTop);
	uint8_t k = startPower;
	for (; k >= minPower_; --k) {
		if (freeList[k] != UINT32_MAX) {
			break;
		}
	}
	if (k >= minPower_) {
		return k;
	}
	else {
		return 0;
	}
}

