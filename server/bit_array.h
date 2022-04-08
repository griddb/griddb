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
	@brief Definition of BitArray
*/
#ifndef BIT_ARRAY_H_
#define BIT_ARRAY_H_

#include "data_type.h"
#include "util/code.h"
#include "util/container.h"
#include "util/trace.h"
#include "util/allocator.h"
#include "gs_error.h"
#include <iostream>
#include <iomanip>

/*!
	@brief Represents a compact array of bit values
*/
template <uint32_t BLOCK_BITS>
class BitArray {
public:
	BitArray(uint64_t capacity);
	~BitArray();

	bool get(uint64_t pos) const;
	void set(uint64_t pos, bool value);

	uint64_t append(bool value);

	uint64_t length() const;

	uint64_t countNumOfBits() const;

	void reserve(uint64_t capacity);

	void reset();

	void clear();

	void assign(const uint8_t *buf, uint64_t bitCount);

	typedef util::ByteStream< util::XArrayOutStream<> > OutStream;
	void serialize(OutStream &out) const;



	std::string dumpUnit() const;  
	std::string dump() const;

private:
	BitArray(const BitArray &bitArray);

	uint32_t getIndex(uint64_t p) const;
	const uint8_t* getBlock(uint64_t p) const;
	uint8_t* getBlock(uint64_t p);
	void expand(uint32_t blockCount);

	static const uint64_t BLOCK_BYTES = (1 << (BLOCK_BITS - 3));
	std::vector<uint8_t*> blocks_;
	uint64_t bitCount_;
};

typedef BitArray<18> ChunkBitArray; 
typedef BitArray<13> ContainerKeyBitArray; 
typedef BitArray<13> ColumnBitArray; 


template <uint32_t BLOCK_BITS>
BitArray<BLOCK_BITS>::BitArray(uint64_t capacity) : bitCount_(0)
{
	UTIL_STATIC_ASSERT(BLOCK_BITS >= 5);
	uint8_t *dummy = getBlock(capacity);
	static_cast<void>(dummy);
}

template <uint32_t BLOCK_BITS>
BitArray<BLOCK_BITS>::~BitArray() {
	for (std::vector<uint8_t*>::iterator it = blocks_.begin();
		 it != blocks_.end(); it++) {
		delete [] *it;
	}
}

template <uint32_t BLOCK_BITS>
inline const uint8_t* BitArray<BLOCK_BITS>::getBlock(uint64_t p) const {
	uint32_t blockno = static_cast<uint32_t>(p >> BLOCK_BITS);
	if (blocks_.size() <= blockno) {
		assert(false); 
		return NULL;
	}
	const uint8_t* b = blocks_[blockno];
	return b;
}

template <uint32_t BLOCK_BITS>
inline uint8_t* BitArray<BLOCK_BITS>::getBlock(uint64_t p) {
	uint32_t blockNo = static_cast<uint32_t>(p >> BLOCK_BITS);
	if (blocks_.size() <= blockNo) {
		expand(blockNo);
	}
	uint8_t* b = blocks_[blockNo];
	return b;
}

template <uint32_t BLOCK_BITS>
void BitArray<BLOCK_BITS>::expand(uint32_t blockCount) {
	uint8_t *block = NULL;
	try {
		while (blocks_.size() <= blockCount) {
			block = UTIL_NEW uint8_t[BLOCK_BYTES];
			memset(block, 0, BLOCK_BYTES);
			blocks_.push_back(block);
			block = NULL;
		}
	}
	catch (std::exception &e) {
		delete [] block;
		GS_RETHROW_USER_ERROR(
			e, "Expand failed. (blockCount=" << blockCount <<
			", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

template <uint32_t BLOCK_BITS>
inline uint32_t BitArray<BLOCK_BITS>::getIndex(uint64_t p) const {
	return static_cast<uint32_t>(p & ((1 << BLOCK_BITS) - 1));
}

template <uint32_t BLOCK_BITS>
inline void BitArray<BLOCK_BITS>::set(uint64_t p, bool flag) {
	uint8_t* block = getBlock(p);
	uint32_t i = getIndex(p);
	if (flag) {
		block[i >> 3] |= static_cast<uint8_t>(1 << (i & 7));
	}
	else {
		block[i >> 3] &= static_cast<uint8_t>(~(1 << (i & 7)));
	}
	if (bitCount_ <= p) {
		bitCount_ = p + 1;
	}
}

template <uint32_t BLOCK_BITS>
inline bool BitArray<BLOCK_BITS>::get(uint64_t p) const {
	if (bitCount_ <= p) {
		return false; 
	}
	const uint8_t* block = getBlock(p);
	assert(block);
	uint32_t i = getIndex(p);
	return !!(block[i >> 3] & (1 << (i & 7)));
}

template <uint32_t BLOCK_BITS>
uint64_t BitArray<BLOCK_BITS>::append(bool value) {
	uint64_t newPos = bitCount_;
	set(newPos, value);  
	return newPos;
}

template <uint32_t BLOCK_BITS>
uint64_t BitArray<BLOCK_BITS>::length() const {
	return bitCount_;
}

template <uint32_t BLOCK_BITS>
uint64_t BitArray<BLOCK_BITS>::countNumOfBits() const {
	uint64_t numOfBits = 0;
	const size_t unitCount = BLOCK_BYTES / sizeof(uint32_t);
	assert(unitCount > 0);
	for (std::vector<uint8_t*>::const_iterator it = blocks_.begin();
			it != blocks_.end(); it++) {
		const uint32_t *addr = reinterpret_cast<const uint32_t *>(*it);
		for (size_t c = 0; c < unitCount; ++c) {
			numOfBits += util::countNumOfBits(*addr);
			addr++;
		}
	}
	return numOfBits;
}

template <uint32_t BLOCK_BITS>
void BitArray<BLOCK_BITS>::reserve(uint64_t capacity) {
	uint8_t *dummy = getBlock(capacity);
	static_cast<void>(dummy);
}


template <uint32_t BLOCK_BITS>
void BitArray<BLOCK_BITS>::reset() {
	clear();
}

template <uint32_t BLOCK_BITS>
void BitArray<BLOCK_BITS>::clear(){
	for (std::vector<uint8_t*>::iterator it = blocks_.begin();
		 it != blocks_.end(); it++) {
		uint8_t* blockTop = *it;
		memset(blockTop, 0, BLOCK_BYTES);
	}
	bitCount_ = 0;
}


template <uint32_t BLOCK_BITS>
void BitArray<BLOCK_BITS>::assign(const uint8_t *buf, uint64_t bitCount) {
	reserve(bitCount);
	const uint8_t *src = buf;
	uint64_t remainBytes = (bitCount + 7) >> 3;
	size_t blockPos = 0;
	while (remainBytes > 0) {
		assert(blockPos < blocks_.size());
		uint8_t *dest = blocks_[blockPos];
		uint64_t readSize = (remainBytes > BLOCK_BYTES) ? BLOCK_BYTES : remainBytes;
		memcpy(dest, src, readSize);
		src += readSize;
		remainBytes -= readSize;
		++blockPos;
	}
	assert(src == (buf + ((bitCount + 7) >> 3)));
	bitCount_ = bitCount;
}

template <uint32_t BLOCK_BITS>
void BitArray<BLOCK_BITS>::serialize(OutStream &out) const {
	uint64_t remainBytes = (bitCount_ + 7) >> 3;
	for (std::vector<uint8_t*>::const_iterator it = blocks_.begin();
			it != blocks_.end() && (remainBytes > 0); it++) {
		const uint8_t* blockTop = *it;
		uint64_t writeSize = (remainBytes > BLOCK_BYTES) ? BLOCK_BYTES : remainBytes;
		out.writeAll(blockTop, writeSize);
		remainBytes -= writeSize;
	}
	out.flush();
	assert(remainBytes == 0);
}


template <uint32_t BLOCK_BITS>
std::string BitArray<BLOCK_BITS>::dumpUnit() const {
	util::NormalOStringStream ss;
	uint64_t remainBytes = (bitCount_ + 7) >> 3;
	ss << "size," << bitCount_ << ",remainBytes," << remainBytes << std::showbase << std::hex << std::endl;
	for (std::vector<uint8_t*>::const_iterator it = blocks_.begin();
			it != blocks_.end() && (remainBytes > 0); it++) {
		const uint8_t* addr = *it;
		for (uint64_t i = 0; i < BLOCK_BYTES && remainBytes > 0;) {
			for (uint64_t j = 0; j < 8; ++j, ++addr) {
				ss << std::hex << std::setw(2) << std::setfill('0') <<
						(uint32_t)(*addr) << " ";
				--remainBytes;
				if (remainBytes == 0) {
					break;
				}
			}
			ss << std::endl;
			i+=8;
		}
	}
	ss << std::dec << std::endl;

	return ss.str();
}

template <uint32_t BLOCK_BITS>
std::string BitArray<BLOCK_BITS>::dump() const {
	util::NormalOStringStream ss;
	for (uint64_t i = 0; i < bitCount_; ++i) {
		if (get(i)) {
			ss << "1";
		}
		else {
			ss << "0";
		}
	}
	return ss.str();
}
#endif  
