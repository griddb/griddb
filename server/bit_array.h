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
#include <iostream>

/*!
	@brief Represents a compact array of bit values
*/
class BitArray {
public:
	static const uint32_t UNIT_BIT_SIZE =
		sizeof(uint64_t) * 8;  
	static const uint64_t UNDEFINED_POS = -1;

	BitArray(uint64_t capacity);
	~BitArray();

	inline bool get(uint64_t pos) const {
		if (pos == UNDEFINED_POS) {
			return false;
		}
		if (pos < bitNum_) {
			return (data_[unitNth(pos)] & (1ULL << unitOffset(pos))) != 0;
		}
		else {
			return false;
		}
	}

	void set(uint64_t pos, bool value);

	uint64_t append(bool value);

	inline uint8_t* data() const {
		return reinterpret_cast<uint8_t*>(data_);
	}

	inline uint64_t length() const {
		return bitNum_;
	};

	inline uint64_t byteLength() const {
		return (bitNum_+ CHAR_BIT - 1) / CHAR_BIT;
	}

	inline uint64_t countNumOfBits() {
		uint64_t numOfBits = 0;
		for (uint64_t i = 0; i < bitNum_ / 64 + 1; ++i) {
			uint32_t *addr = reinterpret_cast<uint32_t *>(&data_[i]);
			numOfBits += util::countNumOfBits(*addr);
			addr++;
			numOfBits += util::countNumOfBits(*addr);
		}
		return numOfBits;
	}

	void reserve(uint64_t capacity);

	void reset() {
		clear();
		bitNum_ = 0;
	}

	void clear() {
		for (uint64_t i = 0; i < reservedUnitNum_; ++i) {
			data_[i] = 0;
		}
		bitNum_ = 0;
	};  

	void copyAll(util::XArray<uint8_t> &buf) const;
	void copyAll(util::NormalXArray<uint8_t> &buf) const;

	void putAll(const uint8_t *buf, uint64_t num);

	std::string dumpUnit() const;  
	std::string dump() const;

private:
	BitArray(const BitArray &bitArray);

	inline uint64_t unitNth(int64_t pos) const {
		return pos / UNIT_BIT_SIZE;
	}
	inline uint64_t unitOffset(int64_t pos) const {
		return pos % UNIT_BIT_SIZE;
	}

	void realloc(uint64_t newSize);

	uint64_t *data_;
	uint64_t bitNum_;
	uint64_t capacity_;
	uint64_t reservedUnitNum_;
};

#endif  
