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
	@brief Implementation of Chunk
*/
#include "chunk.h"

#include <numeric>
#include <iostream>
#include <iomanip>
#include <algorithm>
#include <memory>
#include <numeric>
#ifndef _WIN32
#include <fcntl.h>
#include <linux/falloc.h>
#endif

int8_t ChunkBuddy::CHUNK_EXP_SIZE_ = -1;
uint8_t ChunkBuddy::minPower_ = 0;
uint8_t ChunkBuddy::maxPower_ = 0;
int32_t ChunkBuddy::tableSize_ = -1;
int32_t ChunkBuddy::freeListOffset_ = -1;

UTIL_TRACER_DECLARE(CHUNK_MANAGER);  

void Chunk::checkMagic(int64_t offset) const {
	if (getShort(OFFSET_MAGIC) != (int16_t)MAGIC) {
		assert(false);
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "checkMagic O" << offset <<
				" " << toString() << " " << (int32_t)getByte(OFFSET_MAGIC));
	}
}

void Chunk::checkSum(uint32_t checksum, int64_t offset) const {
	if (static_cast<uint32_t>(getInt(OFFSET_CHECKSUM)) != checksum) {
		assert(false);
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "checkSum O" <<
				offset << "," << static_cast<uint32_t>(getInt(OFFSET_CHECKSUM)) <<
				"," << checksum);
	}
}

std::string Chunk::getSignature(int32_t level) const {
	if (level == 0) {
		util::NormalOStringStream oss;
		for (int32_t i = Chunk::HEADER_SIZE; i < dataSize_; i++) {
			oss << "[" << data_[i] << "]";
		}
		return oss.str();
	} else {
		return std::to_string(
			UtilBytes::getCheckSum(data_, dataSize_, Chunk::HEADER_SIZE));
	}
}

std::string Chunk::toString() const {
	util::NormalOStringStream oss;
	oss << std::setw(3) << std::setfill('0');
	for (int32_t i = dataSize_ - 50; i < dataSize_; i++) {
		oss << std::abs(data_[i]) << ",";
	}
	util::NormalOStringStream oss2;
	oss2 << "<" << "Chunk" << ",MAGIC" << (int32_t)getByte(OFFSET_MAGIC) <<
			",CHK" << static_cast<uint32_t>(getInt(OFFSET_CHECKSUM)) <<
			",COM" << getInt(OFFSET_COMPRESSED_SIZE) <<
			":" << oss.str() << "[" << dataSize_ << "]>";
	return oss2.str();
}

std::string Chunk::toString2() const {
	util::NormalOStringStream oss2;
	oss2 << "MAGIC," << getShort(OFFSET_MAGIC) <<
			",CHK," << calcCheckSum() <<
			",COM," << getInt(OFFSET_COMPRESSED_SIZE);
	return oss2.str();
}


const int32_t ChunkSLT::OFFSET_CHUNK_TYPE = 8;
const int32_t ChunkSLT::OFFSET_NUM_SLOTS = 12;
const int32_t ChunkSLT::OFFSET_UNUSED_NUM_SLOTS = 16;
const int32_t ChunkSLT::OFFSET_LAST_POSITION = 20;
const int32_t ChunkSLT::OFFSET_UNUSED_SPACE_SIZE = 24;
const int32_t ChunkSLT::OFFSET_SLOT_0 = 28;
const float ChunkSLT::COMPACTION_RATIO = (float) 0.3;

ChunkSLT::ChunkSLT(ChunkMemoryPool& pool,int32_t chunkSize) : Chunk(pool, chunkSize) {
	init();
}

ChunkSLT::ChunkSLT(Chunk& chunk) : Chunk(chunk) {
}

ChunkSLT& ChunkSLT::init() {
	assert(data_ != NULL);
	Chunk::setInt(OFFSET_CHUNK_TYPE, 0);
	Chunk::setInt(OFFSET_NUM_SLOTS, 0);
	Chunk::setInt(OFFSET_UNUSED_NUM_SLOTS, 0);
	Chunk::setInt(OFFSET_LAST_POSITION, dataSize_);
	Chunk::setInt(OFFSET_UNUSED_SPACE_SIZE, 0);
	return *this;
}

int32_t ChunkSLT::allocateObject(uint8_t* record, int32_t recordLength) {
	int32_t lastOffset = Chunk::getInt(OFFSET_LAST_POSITION);
	int32_t spaceNeeded = recordLength + (sizeof(int32_t)*2);
	int32_t slotCount = Chunk::getInt(OFFSET_NUM_SLOTS);
	assert(spaceNeeded <= getAvailableSpace());
	if (getInt(OFFSET_UNUSED_NUM_SLOTS) > 0) {
		for (int32_t slotId = 0; slotId < slotCount; slotId++) {
			if (getSlotLength(slotId) < 0) {
				setSlotLength(slotId, recordLength);
				setSlotOffset(slotId, lastOffset - recordLength);
				Chunk::setInt(OFFSET_UNUSED_NUM_SLOTS,
					Chunk::getInt(OFFSET_UNUSED_NUM_SLOTS) - 1);
				Chunk::setInt(OFFSET_LAST_POSITION, lastOffset - recordLength);
				if (record) {
					memcpy(data_ + lastOffset - recordLength, record, recordLength);
				}
				return slotId;
			}
		}
	}
	setSlotLength(slotCount, recordLength);
	setSlotOffset(slotCount, lastOffset - recordLength);
	Chunk::setInt(OFFSET_NUM_SLOTS, slotCount+1);
	Chunk::setInt(OFFSET_LAST_POSITION, lastOffset - recordLength);
	if (record) {
		memcpy(data_ + lastOffset - recordLength, record, recordLength);
	}
	return slotCount;
}
void ChunkSLT::tryAllocateObject(int32_t slotId, uint8_t* record, int32_t recordLength) {
	int32_t lastOffset = getInt(OFFSET_LAST_POSITION);
	int32_t spaceNeeded = recordLength + (sizeof(int32_t)*2);
	int32_t slotCount = getInt(OFFSET_NUM_SLOTS);
	assert(spaceNeeded <= getAvailableSpace());
	if (slotId < slotCount) {
		assert(getSlotLength(slotId) < 0); 
		setSlotLength(slotId, recordLength);
		setSlotOffset(slotId, lastOffset - recordLength);
		setInt(OFFSET_UNUSED_NUM_SLOTS, getInt(OFFSET_UNUSED_NUM_SLOTS)-1);
		setInt(OFFSET_LAST_POSITION, lastOffset - recordLength);
		if (record) {
			memcpy(data_ + lastOffset - recordLength, record, recordLength);
		}
		return;
	}
	setSlotLength(slotCount, recordLength);
	setSlotOffset(slotCount, lastOffset - recordLength);
	setInt(OFFSET_NUM_SLOTS, slotCount+1);
	setInt(OFFSET_LAST_POSITION, lastOffset - recordLength);
	if (record) {
		memcpy(data_ + lastOffset - recordLength, record, recordLength);
	}
}

std::tuple<void*, size_t> ChunkSLT::getObject(int32_t slotId) {
	int32_t length = getSlotLength(slotId);
	int32_t offset = getSlotOffset(slotId);
	return std::forward_as_tuple(data_ + offset, length);
}

void ChunkSLT::overwriteObject(int32_t slotId, const uint8_t* record, int32_t recordLength) {
	int32_t length = getSlotLength(slotId);
	int32_t offset = getSlotOffset(slotId);
	if(recordLength != length) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Invalid record size");
	} else {
		memcpy(data_ + offset, record, length);
		return;
	}
}

void ChunkSLT::compact() {
	int32_t slotCount = Chunk::getInt(OFFSET_NUM_SLOTS);
	int32_t lastOffset = dataSize_;
	int32_t unusedSlotCount = 0;
	typedef std::pair<int32_t, int32_t> Pair;
	std::vector<Pair> slots;
	for (int32_t s = 0; s < slotCount; s++) {
		Pair pair(s, getSlotOffset(s));
		slots.push_back(pair);
	}
	std::sort(slots.begin(), slots.end(), 
		[](const Pair& lhs, const Pair& rhs) {
			return lhs.second > rhs.second;
		});
	for (auto& pair : slots) {
		int32_t s = pair.first;
		if (getSlotLength(s) < 0) {
			unusedSlotCount++;
		}
		else {
			lastOffset -= getSlotLength(s);
			if (getSlotOffset(s) != lastOffset) {
				memcpy(data_ + lastOffset, data_ + getSlotOffset(s),
					getSlotLength(s));
			}
			setSlotOffset(s, lastOffset);
		}
	}
	Chunk::setInt(OFFSET_NUM_SLOTS, slotCount);
	Chunk::setInt(OFFSET_UNUSED_NUM_SLOTS, unusedSlotCount);
	Chunk::setInt(OFFSET_LAST_POSITION, lastOffset);
	Chunk::setInt(OFFSET_UNUSED_SPACE_SIZE, 0);

}

void ChunkSLT::removeObject(int32_t slotId) {
	int32_t length = getSlotLength(slotId);
	assert(getSlotLength(slotId) >= 0);
	setSlotLength(slotId, -1);
	setInt(OFFSET_UNUSED_NUM_SLOTS, getInt(OFFSET_UNUSED_NUM_SLOTS)+1);
	setInt(OFFSET_UNUSED_SPACE_SIZE, getInt(OFFSET_UNUSED_SPACE_SIZE)+length);

}

std::string ChunkSLT::toString() const {
	util::NormalOStringStream oss;
	assert(Chunk::getInt(OFFSET_NUM_SLOTS) >= Chunk::getInt(OFFSET_UNUSED_NUM_SLOTS));
	oss << "[MAGIC" << Chunk::getInt(4) <<
			",NS" << Chunk::getInt(OFFSET_NUM_SLOTS) <<
			",UUS" << Chunk::getInt(OFFSET_UNUSED_NUM_SLOTS) <<
			",LF" << Chunk::getInt(OFFSET_LAST_POSITION) <<
			",UUF" << Chunk::getInt(OFFSET_UNUSED_SPACE_SIZE) <<
			"]";

	int32_t slotCount = Chunk::getInt(OFFSET_NUM_SLOTS);
	for(int32_t s = 0; s < slotCount; s++) {
		int32_t length = getSlotLength(s);
		if (length >= 0) {
			util::NormalXArray<uint8_t> record;
			uint8_t initVal = 0;
			record.assign(length + 1, initVal);
			memcpy(record.data(), data_ + getSlotOffset(s), length);
			oss << "[" << record.data() << "]";
		}
		else {
			oss << "[x]";
		}
	}
	return oss.str();
}


void ChunkBuddy::resetParameters(int32_t chunkSize) {
	CHUNK_EXP_SIZE_ = util::ilog2(chunkSize);
	minPower_ = util::nextPowerBitsOf2(sizeof(V4ObjectHeader));
	maxPower_ = CHUNK_EXP_SIZE_;
	tableSize_ = sizeof(uint32_t) * (maxPower_ + 1);
	freeListOffset_ = CHUNK_HEADER_FULL_SIZE - tableSize_;  
}

ChunkBuddy& ChunkBuddy::init() {
	assert(data_ != NULL);
	memset(data_, 0, CHUNK_HEADER_FULL_SIZE);

	setVersion(V4ChunkHeader::GS_FILE_VERSION);
	setMagicNum();
	setMaxFreeExpSize(0);
	setOccupiedSize(0);
	
	uint32_t *freeList = getFreeList();
	for (int32_t i = minPower_; i < maxPower_; ++i) {
		freeList[i] = UINT32_MAX;
	}
	freeList[maxPower_] = 0;  
	setMaxFreeExpSize(maxPower_);

	V4ObjectHeader *header = getV4ObjectHeader(data_, 0);
	header->setUsed(false, maxPower_);  
	header->next_ = header->prev_ = 0;  
	uint8_t k = util::nextPowerBitsOf2(CHUNK_HEADER_FULL_SIZE);
	allocateObject(data_, k, OBJECT_TYPE_CHUNK_HEADER);
	return *this;
}

int32_t ChunkBuddy::allocateObject(uint8_t* record, uint8_t requestExpSize, ObjectType type) {
	assert(requestExpSize < maxPower_);
	uint8_t expSize = std::max(minPower_, requestExpSize);
	uint32_t *freeList = getFreeList();
	uint8_t maxFreeExpSize = getMaxFreeExpSize();
	uint8_t k = expSize;
	uint8_t* chunkTop = data_;
#ifndef NDEBUG 
	{
		V4ObjectHeader* list = 0;
		for (uint8_t p = minPower_; p <= maxFreeExpSize; ++p) {
			if (freeList[p] != UINT32_MAX) {
				list = getV4ObjectHeader(chunkTop, freeList[p]);
				assert(list->isValid());
			}
		}
	}
#endif

	const uint8_t kmin = k;

	V4ObjectHeader *list = 0;
	for (; k <= maxFreeExpSize; ++k)
	{
		if (freeList[k] != UINT32_MAX) {
			list = getV4ObjectHeader(chunkTop, freeList[k]);
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
			setMaxFreeExpSize(maxFreeExpSize);
		}
	}
	else {
		getV4ObjectHeader(chunkTop, list->next_)->prev_ = list->prev_;
		getV4ObjectHeader(chunkTop, list->prev_)->next_ = list->next_;
		freeList[k] = list->next_;
	}

	V4ObjectHeader *header = list;
	header->setUsed(true, kmin, type);

	while (kmin < k--) {
		V4ObjectHeader *buddy =
			getV4ObjectHeader(reinterpret_cast<uint8_t *>(header), (1ULL << k));
		buddy->setUsed(false, k);

		freeList[k] =
			static_cast<int32_t>(reinterpret_cast<uint8_t *>(buddy) - chunkTop);
		buddy->next_ = freeList[k];
		buddy->prev_ = freeList[k];
	}
	assert(kmin == k + 1);

	if (isCheckMaxFreeExpSize) {
		maxFreeExpSize = findMaxFreeBlockSize(maxFreeExpSize);
		setMaxFreeExpSize(maxFreeExpSize);
	}

	addOccupiedSize(INT32_C(1) << expSize);
	uint8_t *objectAddr = reinterpret_cast<uint8_t *>(header) + hSize_;
	if (!objectAddr) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_OA_ALLOCATE_FAILED,
			"chunk*, " << chunkTop << ", expSize, " << expSize);
	}
	assert(objectAddr > chunkTop);
	return static_cast<uint32_t>(objectAddr - chunkTop);
}

int32_t ChunkBuddy::tryAllocateObject(uint8_t* record, uint8_t requestExpSize, ObjectType type, int32_t offset) {
	assert(requestExpSize < maxPower_);
	uint8_t expSize = std::max(minPower_, requestExpSize);
	uint32_t *freeList = getFreeList();
	uint8_t maxFreeExpSize = getMaxFreeExpSize();
	uint8_t k = expSize;
	uint8_t* chunkTop = data_;
#ifndef NDEBUG 
	{
		V4ObjectHeader* list = 0;
		for (uint8_t p = minPower_; p <= maxFreeExpSize; ++p) {
			if (freeList[p] != UINT32_MAX) {
				list = getV4ObjectHeader(chunkTop, freeList[p]);
				assert(list->isValid());
			}
		}
	}
#endif
	if (isValidObject(offset)) {
		return offset;
	}
	const uint8_t kmin = k;
	V4ObjectHeader *list = 0;
	for (; k <= maxFreeExpSize; ++k)
	{
		if (freeList[k] != UINT32_MAX) {
			list = getV4ObjectHeader(chunkTop, freeList[k]);
			assert(list->isValid());
			uint8_t *objectAddr = reinterpret_cast<uint8_t *>(list) + hSize_;
			assert(objectAddr > chunkTop);
			uint32_t allocated =  static_cast<uint32_t>(objectAddr - chunkTop);
			assert(allocated == offset);
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
			setMaxFreeExpSize(maxFreeExpSize);
		}
	}
	else {
		getV4ObjectHeader(chunkTop, list->next_)->prev_ = list->prev_;
		getV4ObjectHeader(chunkTop, list->prev_)->next_ = list->next_;
		freeList[k] = list->next_;
	}

	V4ObjectHeader *header = list;
	header->setUsed(true, kmin, type);
	addOccupiedSize(INT32_C(1) << expSize);

	while (kmin < k--) {
		V4ObjectHeader *buddy =
			getV4ObjectHeader(reinterpret_cast<uint8_t *>(header), (1ULL << k));
		buddy->setUsed(false, k);

		freeList[k] =
			static_cast<int32_t>(reinterpret_cast<uint8_t *>(buddy) - chunkTop);
		buddy->next_ = freeList[k];
		buddy->prev_ = freeList[k];
	}
	assert(kmin == k + 1);

	if (isCheckMaxFreeExpSize) {
		maxFreeExpSize = findMaxFreeBlockSize(maxFreeExpSize);
		setMaxFreeExpSize(maxFreeExpSize);
	}

	uint8_t *objectAddr = reinterpret_cast<uint8_t *>(header) + hSize_;
	if (!objectAddr) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_OA_ALLOCATE_FAILED,
			"chunk*, " << chunkTop << ", expSize, " << expSize);
	}
	assert(objectAddr > chunkTop);
	return static_cast<uint32_t>(objectAddr - chunkTop);
}

void ChunkBuddy::overwriteObject(int32_t offset, const uint8_t* record, uint8_t expSize) {
	uint8_t* addr = data_ + offset;
	int8_t allocatedExpSize = getObjectSize(offset, V4ObjectHeader::OBJECT_HEADER_SIZE);
	if(allocatedExpSize != expSize) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "Invalid record size");
	} else {
		memcpy(addr, record, 1 << expSize);
		return;
	}
}

void ChunkBuddy::removeObject(int32_t offset) {
	uint8_t* chunkTop = data_;
	uint8_t *objectAddr = chunkTop + offset;
	uint32_t *freeList = getFreeList();
	uint8_t maxFreeExpSize = getMaxFreeExpSize();
	V4ObjectHeader *header = getV4ObjectHeader(objectAddr, -hSize_);
	int32_t headerOffset = static_cast<int32_t>(offset - hSize_);

	assert(header->isValid() && header->isUsed());
	if (!header->isValid()) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_OA_BAD_ADDRESS, "");
	}
	if (!header->isUsed()) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_OA_DOUBLE_FREE, "");
	}
#ifndef NDEBUG 
	{
		V4ObjectHeader* list = 0;
		for (uint8_t p = minPower_; p <= maxFreeExpSize; ++p) {
			if (freeList[p] != UINT32_MAX) {
				list = getV4ObjectHeader(chunkTop, freeList[p]);
				assert(list->isValid());
			}
		}
	}
#endif

	uint8_t k = header->getPower();
	uint8_t freedBlockSize = k;
	subtractOccupiedSize(INT32_C(1) << k);



	int32_t mask = 1L << k;
	V4ObjectHeader *buddy = getV4ObjectHeader(chunkTop, (headerOffset ^ mask));
	for (;;) {
		assert(buddy->isValid());

		if (buddy->isUsed() ||		 
			k == maxPower_ ||		 
			buddy->getPower() != k)  
		{
			header->setUsed(false, header->getPower());

			if (freeList[k] != UINT32_MAX) {
				V4ObjectHeader *it = getV4ObjectHeader(chunkTop, freeList[k]);
				assert(it->isValid());
				getV4ObjectHeader(chunkTop, it->prev_)->next_ =
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
				setMaxFreeExpSize(maxFreeExpSize);
			}
			return;
		}

		if (buddy->next_ ==
			static_cast<int32_t>(
				reinterpret_cast<uint8_t *>(buddy) - chunkTop)) {
			freeList[k] = UINT32_MAX;
		}
		else {
			getV4ObjectHeader(chunkTop, buddy->prev_)->next_ = buddy->next_;
			assert(getV4ObjectHeader(chunkTop, buddy->prev_)->isValid());
			getV4ObjectHeader(chunkTop, buddy->next_)->prev_ = buddy->prev_;
			assert(getV4ObjectHeader(chunkTop, buddy->next_)->isValid());
			freeList[k] = buddy->next_;
		}

		header = std::min(header, buddy);
		assert((uintptr_t)header >= (uintptr_t)chunkTop);
		headerOffset =
			static_cast<int32_t>(reinterpret_cast<uintptr_t>(header) -
								 reinterpret_cast<uintptr_t>(chunkTop));
		header->setUsed(false, ++k);
		mask <<= 1;
		buddy = getV4ObjectHeader(chunkTop, (headerOffset ^ mask));
	}

	if (maxFreeExpSize < k) {
		maxFreeExpSize = k;
		setMaxFreeExpSize(maxFreeExpSize);
	}
}

uint8_t ChunkBuddy::findMaxFreeBlockSize(uint8_t startPower) const {
	if (CHUNK_EXP_SIZE_ < startPower) {
		startPower = CHUNK_EXP_SIZE_;
	}
	uint32_t *freeList = getFreeList();
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

std::string ChunkBuddy::toString() const {
	const int32_t HEADER_SIZE = Chunk::getHeaderSize();
	const int32_t bodySize = dataSize_ - HEADER_SIZE;
	uint64_t sig = UtilBytes::getSignature(data_ + HEADER_SIZE, bodySize);

	util::NormalOStringStream oss2;
	oss2 << ",pId," << getPartitionId() << ",groupId," << getGroupId() <<
		",chunkId," << getChunkId() << ",MAGIC," << getShort(OFFSET_MAGIC) <<
		",CHK," << calcCheckSum() <<",COM," << getInt(OFFSET_COMPRESSED_SIZE) <<
		",SIG," << sig;
	return oss2.str();
}
