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
	aint64_t with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
/*!
	@file
	@brief Definition of V5Utility
*/
#ifndef UTILITY_V5_H_
#define UTILITY_V5_H_

#include "util/time.h"
#include "bit_array.h"
#include <vector>
#include <random>


typedef util::VariableSizeAllocator<> VariableSizeAllocator;
typedef util::XArrayOutStream< util::StdAllocator<
	uint8_t, VariableSizeAllocator> > EventOutStream;
typedef util::ArrayByteInStream EventByteInStream;
typedef util::ByteStream<EventOutStream> EventByteOutStream;
typedef util::ByteStream<util::ArrayInStream> InStream;
typedef util::ByteStream< util::XArrayOutStream<> > OutStream;
struct Serializable {
	Serializable(util::StackAllocator* alloc) : alloc_(alloc), data_(NULL), size_(0) {}
	Serializable(util::StackAllocator* alloc, const uint8_t*data, size_t dataSize) :
		alloc_(alloc), data_(data), size_(dataSize) {}
	virtual void encode(EventByteOutStream& out) = 0;
	virtual void decode(EventByteInStream& in) = 0;

	virtual void encode(OutStream& out) = 0;



	util::StackAllocator* alloc_;
	const uint8_t* data_;
	size_t size_;
};

struct SimpleMessage : public Serializable {
	SimpleMessage(util::StackAllocator* alloc) : Serializable(alloc) {}
	SimpleMessage(util::StackAllocator* alloc, const uint8_t* data, size_t dataSize) :
		Serializable(alloc, data, dataSize) {}
	void encode(EventByteOutStream& out) { static_cast<void>(out); };
	void decode(EventByteInStream& in) { static_cast<void>(in); };
	void encode(OutStream& out) { static_cast<void>(out); };
};

class Interchangeable {
  private:
	int32_t ref_;

  public:
	Interchangeable() {
		ref_ = 0;
	}

	virtual ~Interchangeable() {};

	void use() {
		ref_++;
	}

	void unuse() {
		ref_--;
	}

	bool free() {
		if (ref_ <= 0) return true;
		return false;
	}

	virtual double getActivity() const = 0;

	virtual void setMaxCapacity(int32_t maxcapacity) {  static_cast<void>(maxcapacity); };

	virtual bool resize(int32_t capacity) = 0;
};



class MimicLongArrayBase {
public:
	virtual ~MimicLongArrayBase() {}
	virtual MimicLongArrayBase& init(int16_t size) = 0;
	virtual int16_t size() const = 0;
	virtual int8_t getId() const = 0;

	virtual bool reducible() const { return false; }
	virtual int64_t get(int16_t pos) const = 0;
	virtual bool trySet(int16_t pos, int64_t value) = 0;
};

class MimicLongArray {
private:
	MimicLongArrayBase* mimicArray_;

public:
	MimicLongArray(int16_t size);

	int64_t get(int16_t pos) const;

	void set(int16_t pos, int64_t value);

	void resize();

  private:
};

class OverflowException {
private:
	static const int64_t serialVersionUID = 1L;
    const char* msg_;
public:
	OverflowException(const char *msg) : msg_(msg){
	}

    const char* what() { return msg_; }
};

class MimicLongArray8;
class MimicLongArray2 final : public MimicLongArrayBase {
private:
	int64_t base_;
	int16_t* array_;
	int16_t size_;

public:
	MimicLongArray2(int16_t size);
	MimicLongArray2(MimicLongArray8& mla);

	MimicLongArray2& init(int16_t size);

	virtual ~MimicLongArray2();

	inline int16_t size() const {
		return size_;
	}

	inline int8_t getId() const {
		return 2;
	}

	inline int64_t get(int16_t pos) const {
		return array_[pos] + base_;
	}

	bool trySet(int16_t pos, int64_t value);
};

class MimicLongArray8 final : public MimicLongArrayBase {
private:
	int64_t* array_;
	int16_t size_;

public:
	MimicLongArray8(int16_t size);

	MimicLongArray8(MimicLongArray2& mla);

	MimicLongArray8& init(int16_t size);

	virtual ~MimicLongArray8();

	inline int16_t size() const {
		return size_;
	}

	inline int8_t getId() const {
		return 8;
	}

	int64_t min() const;

	bool reducible() const;

	inline int64_t get(int16_t pos) const {
		assert(pos < size_);
		return array_[pos];
	}

	inline bool trySet(int16_t pos, int64_t value) {
		assert(pos < size_);
		array_[pos] = value;
		return true;
	}
};



inline MimicLongArray::MimicLongArray(int16_t size)
: mimicArray_(UTIL_NEW MimicLongArray2(size)){
}

inline int64_t MimicLongArray::get(int16_t pos) const {
	return mimicArray_->get(pos);
}

inline void MimicLongArray::set(int16_t pos, int64_t value) {
	if (! mimicArray_->trySet(pos, value)) {
		MimicLongArray8* array8 = UTIL_NEW MimicLongArray8(
				*reinterpret_cast<MimicLongArray2*>(mimicArray_));
		delete mimicArray_;
		mimicArray_ = array8;
		mimicArray_->trySet(pos, value);
	}
}

inline void MimicLongArray::resize() {
	if (mimicArray_->getId() == (int8_t)8 && mimicArray_->reducible()) {
		MimicLongArray2* array2 = UTIL_NEW MimicLongArray2(*reinterpret_cast<MimicLongArray8*>(mimicArray_));
		delete mimicArray_;
		mimicArray_ = array2;
	}
}

inline MimicLongArray2::MimicLongArray2(int16_t size)
: base_(0), array_(NULL), size_(size) {
	init(size);
}

inline MimicLongArray2::MimicLongArray2(MimicLongArray8& mla)
: base_(mla.min()), array_(NULL), size_(mla.size()) {
	init(mla.size());
	for (int16_t pos = 0; pos < mla.size(); pos++) {
		trySet(pos, mla.get(pos));
	}
}

inline MimicLongArray2& MimicLongArray2::init(int16_t size) {
	array_ = UTIL_NEW int16_t[size];
	size_ = size;
	return (*this);
}

inline MimicLongArray2::~MimicLongArray2() {
	delete [] array_;
}

inline bool MimicLongArray2::trySet(int16_t pos, int64_t value) {
	if (value - base_ > INT16_MAX) {
		return false;
	}
	array_[pos] = static_cast<int16_t>(value - base_);
	return true;
}

inline MimicLongArray8::MimicLongArray8(int16_t size)
: array_(NULL), size_(size) {
	init(size);
}

inline MimicLongArray8::MimicLongArray8(MimicLongArray2& mla)
: array_(NULL), size_(mla.size()) {
	init(mla.size());
	for (int16_t pos = 0; pos < mla.size(); pos++) {
		trySet(pos, mla.get(pos));
	}
}

inline MimicLongArray8& MimicLongArray8::init(int16_t size) {
	array_ = UTIL_NEW int64_t[size];
	size_ = size;
	return (*this);
}

inline MimicLongArray8::~MimicLongArray8() {
	delete [] array_;
}

inline int64_t MimicLongArray8::min() const {
	return *std::min_element(array_, array_ + size_);
}

inline bool MimicLongArray8::reducible() const {
	int64_t max = *std::max_element(array_, array_ + size_);
	if (max - min() < INT16_MAX) {
		return true;
	}
	return false;
}


class DLTable {
public:
	class Elem {
	public:
		bool use_;
		int32_t prev_;
		int32_t next_;

		Elem() : use_(false), prev_(-1), next_(-1) {}

		std::string toString() const {
			util::NormalOStringStream oss;
			oss << "<"
				<< "Elem"; 
			oss << ",N" << next_ << ",P" << prev_ << ">";
			return oss.str();
		}
	};
	class Chain;

private:
	std::vector<Elem> table_;

public:
	DLTable();

	void resize(int32_t numelems);

	std::string toString() const;

	Chain* createChain();
};

class DLTable::Chain {
public:
	std::set<int32_t> base_;

public:
	Chain(DLTable* dlt);

	void add(int32_t index);

	void remove(int32_t index);

	int32_t peek() const;

	int32_t next(int32_t index) const;

	std::string toString() const;
};


inline DLTable::Chain* DLTable::createChain() {
	Chain* chain = UTIL_NEW Chain(this);
	return chain;
}

inline void DLTable::resize(int32_t numelems) {
	for (size_t pos = table_.size(); pos < static_cast<size_t>(numelems); pos++) {
		Elem e;
		table_.push_back(e);
	}
}

inline DLTable::Chain::Chain(DLTable*) {
}
inline void DLTable::Chain::add(int32_t index) {
	base_.insert(index);
}
inline void DLTable::Chain::remove(int32_t index) {
	base_.erase(index);
}
inline int32_t DLTable::Chain::peek() const {
	if (base_.empty()) {
		return -1;
	}
	return *base_.begin();
}
inline int32_t DLTable::Chain::next(int32_t index) const {
	const auto &it = base_.upper_bound(index);
	if (it == base_.end()) {
		return -1;
	}
	return *it;
}
inline std::string DLTable::Chain::toString() const {
	return "";
}


class UtilBitmap {
	typedef BitArray<18> ChunkBitArray; 
private:
	static const int32_t UNIT_SIZE_EXP = 10;
	static const int32_t unitsize_ = INT32_C(1) << UNIT_SIZE_EXP;
	static const int32_t POS_MASK = (INT32_C(1) << UNIT_SIZE_EXP) - 1;
	bool initialvalue_;
	std::vector<ChunkBitArray*> bitsets_;
	int64_t maxkey_;

  public:
	UtilBitmap()
	: initialvalue_(false), maxkey_(-1){
		init(false);
	}

	UtilBitmap(bool initialvalue)
	: initialvalue_(initialvalue), maxkey_(-1) {
		init(initialvalue);
	}

	~UtilBitmap() {
		for (auto& e : bitsets_) {
			delete e;
		}
	}

	void init(bool initialvalue) {
		initialvalue_ = initialvalue;
	}

	int64_t size() {
		return bitsets_.size() << UNIT_SIZE_EXP;
	}

	int64_t length() {
		return maxkey_ + 1;
	}

	void set(int64_t key) {
		set(key, true);
	}

	void set(int64_t key, bool value);

	bool get(int64_t key) {
		int32_t index = (int32_t)(key >> UNIT_SIZE_EXP);
		int32_t pos = (int32_t)(key & POS_MASK);
		ChunkBitArray* ba = bitsets_.at(index);
		return ba->get(pos);
	}
};

inline void UtilBitmap::set(int64_t key, bool value) {
	size_t index = (size_t)(key >> UNIT_SIZE_EXP);
	size_t pos = (size_t)(key & POS_MASK);
	for (size_t i = bitsets_.size(); i <= index; i++) {
		ChunkBitArray* ba = new ChunkBitArray(unitsize_);
		for (int32_t pos = 0; pos < unitsize_; ++pos) {
			ba->set(pos, initialvalue_);
		}
		bitsets_.push_back(ba);
	}
	ChunkBitArray* ba = bitsets_.at(index);
	ba->set(pos, value);
	maxkey_ = std::max(key, maxkey_);
}

struct UtilBytes {
	static void bytes2Bytes(const char* src, size_t length, char* dest);

	static uint16_t getCheckSum(const void* bytes, int32_t length);
	static uint16_t getCheckSum(const void* bytes, int32_t length, int32_t offset);
	static uint64_t getSignature(const void* bytes, int32_t length);

	static uint8_t getByte(const void* data, int32_t dataLength, int32_t pos);
	static void setByte(void* data, int32_t dataLength, int32_t pos, uint8_t value);

	static char getChar(const void* data, int32_t dataLength, int32_t pos);
	static void setChar(void* data, int32_t dataLength, int32_t pos, char value);

	static int16_t getShort(const void* data, int32_t dataLength, int32_t pos);
	static void setShort(void* data, int32_t dataLength, int32_t pos, int16_t value);

	static int32_t getInt(const void* data, int32_t dataLength, int32_t pos);
	static void setInt(void* data, int32_t dataLength, int32_t pos, int32_t value);

	static int64_t getLong(const void* data, int32_t dataLength, int32_t pos);
	static void setLong(void* data, int32_t dataLength, int32_t pos, int64_t value);

	static float getFloat(const void* data, int32_t dataLength, int32_t pos);
	static void setFloat(void* data, int32_t dataLength, int32_t pos, float value);

	static std::string getString(const void* data, int32_t dataLength, int32_t pos, int32_t length);
	static void setString(void* data, int32_t dataLength, int32_t pos, const std::string& value);

	static std::string dump(const void* data, int32_t dataLength);
};

struct UtilNumber {
	static int64_t ceil8(int64_t x) {
		return ((x + (8-1)) & ~(8 - 1));
	}

	static int64_t floor8(int64_t x) {
		return ((x) & ~(8 - 1));
	}

	static int32_t ceil8(int32_t x) {
		return ((x + (8-1)) & ~(8 - 1));
	}

	static int32_t floor8(int32_t x) {
		return ((x) & ~(8 - 1));
	}

	static bool checkAlign8(int64_t x) {
		return ((x % 8) == 0);
	}
};


class Locker { 
  public:
	virtual void lock() = 0;
	virtual void unlock() = 0;
	virtual bool isLockable() const = 0;
};

class MutexLocker final : public Locker {
  private:
	util::Mutex lock_;

  public:
	void lock() {
		lock_.lock();
	}

	void unlock() {
		lock_.unlock();
	}

	bool isLockable() const {
		return true;
	}
};

class NoLocker final : public Locker {
public:
	void lock() {
	}

	void unlock() {
	}

	bool isLockable() const {
		return false;
	}
};


class UtilFile {
  public:
	static void getFiles(const char* path, const char* reg, const char* exclude, std::vector<std::string>& outList);

	static void getFileNumbers(const char* path, const char* reg, const char* exclude, std::vector<int32_t>& outList);

	static int64_t extractNumber(const std::string& reg, const std::string& exclude, const std::string& str);

	static void createDir(const char* path);
	static void removeDir(const char* path);

	static void createFile(const char* path);
	static void removeFile(const char* path);

	static void dump(const char* path);
};

class UtilDebug {
public:
	static void dumpBackTrace();
	static void dumpMemory(const void* addr, size_t len);
};

inline void UtilBytes::bytes2Bytes(const char* src, size_t length, char* dest) {
	for (size_t i = 0; i < length; i++) {
		dest[i] = src[i];
	}
}

inline uint16_t UtilBytes::getCheckSum(const void* bytes, int32_t length) {
	return getCheckSum(bytes, length, 0);
}

inline uint16_t UtilBytes::getCheckSum(const void* bytes, int32_t length, int32_t offset) {
	const uint8_t* u8data = static_cast<const uint8_t*>(bytes);
	uint16_t sum = 513;
	for (int32_t i = offset; i < length; i += 7) {
		sum = static_cast<uint16_t>(
				sum + *reinterpret_cast<const uint16_t*>(u8data + i) * (i + 1));
	}
	return sum;
}

inline uint64_t UtilBytes::getSignature(const void* bytes, int32_t length) {
	const uint8_t* u8data = static_cast<const uint8_t*>(bytes);
	uint64_t sum = 513;
	for (int32_t i = 0; i < length - 1; i++) {
		sum += (*reinterpret_cast<const uint16_t*>(u8data + i) * (i + 1));
	}
	return sum;
}

inline uint8_t UtilBytes::getByte(const void* data, int32_t dataLength, int32_t pos) {
	assert(pos < dataLength);
	static_cast<void>(dataLength);
	const uint8_t* u8data = static_cast<const uint8_t*>(data);
	return *(u8data + pos);
}

inline void UtilBytes::setByte(void* data, int32_t dataLength, int32_t pos, uint8_t value) {
	assert(pos < dataLength);
	static_cast<void>(dataLength);
	uint8_t* u8data = static_cast<uint8_t*>(data);
	u8data[pos] = value;
}

inline char UtilBytes::getChar(const void* data, int32_t dataLength, int32_t pos) {
	assert(pos < dataLength);
	static_cast<void>(dataLength);
	const uint8_t* u8data = static_cast<const uint8_t*>(data);
	return static_cast<char>(u8data[pos]);
}

inline void UtilBytes::setChar(void* data, int32_t dataLength, int32_t pos, char value) {
	assert(pos < dataLength);
	static_cast<void>(dataLength);
	uint8_t* u8data = static_cast<uint8_t*>(data);
	u8data[pos] = static_cast<uint8_t>(value);
}

inline int16_t UtilBytes::getShort(const void* data, int32_t dataLength, int32_t pos) {
	assert(pos < dataLength);
	static_cast<void>(dataLength);
	const uint8_t* u8data = static_cast<const uint8_t*>(data);
	return static_cast<int16_t>( ((u8data[pos] & 0xff) << 8) | (u8data[pos + 1] & 0xff) );
}

inline void UtilBytes::setShort(void* data, int32_t dataLength, int32_t pos, int16_t value) {
	assert(pos < dataLength);
	static_cast<void>(dataLength);
	uint8_t* u8data = static_cast<uint8_t*>(data);
	u8data[pos] = static_cast<uint8_t>((value >> 8) & 0xff);
	u8data[pos + 1] = static_cast<uint8_t>(value & 0xff);
}

inline int32_t UtilBytes::getInt(const void* data, int32_t dataLength, int32_t pos) {
	assert(pos < dataLength);
	static_cast<void>(dataLength);
	const uint8_t* u8data = static_cast<const uint8_t*>(data);
	return static_cast<int32_t>(
		(static_cast<uint32_t>(u8data[pos + 0] & 0xff) << 24)
	  | (static_cast<uint32_t>(u8data[pos + 1] & 0xff) << 16)
	  | (static_cast<uint32_t>(u8data[pos + 2] & 0xff) << 8)
	  | static_cast<uint32_t>(u8data[pos + 3] & 0xff));
}

inline void UtilBytes::setInt(void* data, int32_t dataLength, int32_t pos, int32_t value) {
	assert(pos < dataLength);
	static_cast<void>(dataLength);
	uint8_t* u8data = static_cast<uint8_t*>(data);
	u8data[pos + 0] = static_cast<uint8_t>((value >> 24) & 0xff);
	u8data[pos + 1] = static_cast<uint8_t>((value >> 16) & 0xff);
	u8data[pos + 2] = static_cast<uint8_t>((value >> 8) & 0xff);
	u8data[pos + 3] = static_cast<uint8_t>(value & 0xff);
}

inline int64_t UtilBytes::getLong(const void* data, int32_t dataLength, int32_t pos) {
	assert(pos < dataLength);
	static_cast<void>(dataLength);
	const uint8_t* u8data = static_cast<const uint8_t*>(data);
	return static_cast<int64_t>(
		(static_cast<uint64_t>(u8data[pos + 0] & 0xff) << 56)
		| (static_cast<uint64_t>(u8data[pos + 1] & 0xff) << 48)
			| (static_cast<uint64_t>(u8data[pos + 2] & 0xff) << 40)
			| (static_cast<uint64_t>(u8data[pos + 3] & 0xff) << 32)
			| (static_cast<uint64_t>(u8data[pos + 4] & 0xff) << 24)
			| (static_cast<uint64_t>(u8data[pos + 5] & 0xff) << 16)
			| (static_cast<uint64_t>(u8data[pos + 6] & 0xff) << 8)
			| static_cast<uint64_t>(u8data[pos + 7] & 0xff) );
}

inline void UtilBytes::setLong(void* data, int32_t dataLength, int32_t pos, int64_t value) {
	assert(pos < dataLength);
	static_cast<void>(dataLength);
	uint8_t* u8data = static_cast<uint8_t*>(data);
	u8data[pos + 0] = static_cast<uint8_t>((value >> 56) & 0xff);
	u8data[pos + 1] = static_cast<uint8_t>((value >> 48) & 0xff);
	u8data[pos + 2] = static_cast<uint8_t>((value >> 40) & 0xff);
	u8data[pos + 3] = static_cast<uint8_t>((value >> 32) & 0xff);
	u8data[pos + 4] = static_cast<uint8_t>((value >> 24) & 0xff);
	u8data[pos + 5] = static_cast<uint8_t>((value >> 16) & 0xff);
	u8data[pos + 6] = static_cast<uint8_t>((value >> 8) & 0xff);
	u8data[pos + 7] = static_cast<uint8_t>(value & 0xff);
}

inline std::string UtilBytes::getString(const void* data, int32_t dataLength, int32_t pos, int32_t length) {
	assert(pos < dataLength);
	int32_t buflen = dataLength - pos;
	if (buflen < length) {
		length = buflen;
	}
	const uint8_t* u8data = static_cast<const uint8_t*>(data);
	return std::string(reinterpret_cast<const char*>(u8data + pos), length);
}

inline void UtilBytes::setString(void* data, int32_t dataLength, int32_t pos, const std::string& value) {
	assert(pos + value.size() < dataLength);
	static_cast<void>(dataLength);
	uint8_t* u8data = static_cast<uint8_t*>(data);
	memcpy(u8data + pos, value.c_str(), value.size());
}
#endif 
