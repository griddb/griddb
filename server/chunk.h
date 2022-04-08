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
	@brief Definition of Chunk
*/
#ifndef CHUNK_H_
#define CHUNK_H_

#include "utility_v5.h"
#include "data_type.h"
#include "data_store_common.h"
#include "config_table.h"

class ObjectAccessor;
class GroupObjectAccessor;
/*!
	@brief チャンク(基底クラス)
*/
class Chunk {
	friend class ObjectAccessor;
	friend class GroupObjectAccessor;
	friend class StoreV5Impl;
private:
	static const uint16_t MAGIC = 0xcd14;

protected:
	static const int32_t CHUNK_HEADER_PADDING_ = 12;  
	static const int32_t OFFSET_CHECKSUM = CHUNK_HEADER_PADDING_;
	static const int32_t VERSION_OFFSET = OFFSET_CHECKSUM + sizeof(uint32_t);
	static const int32_t OFFSET_MAGIC = VERSION_OFFSET + sizeof(uint16_t);

	static const int32_t OFFSET_COMPRESSED_SIZE = OFFSET_MAGIC + sizeof(int32_t);  
	static const int32_t HEADER_SIZE = OFFSET_COMPRESSED_SIZE + sizeof(int32_t); 

	uint8_t* data_;
	int32_t dataSize_;

public:
	typedef util::FixedSizeAllocator<util::Mutex> ChunkMemoryPool; 
	Chunk() : data_(NULL), dataSize_(0) {}

	Chunk(ChunkMemoryPool& pool, int32_t chunkSize)
	: data_(NULL), dataSize_(chunkSize) {
		data_ = static_cast<uint8_t*>(pool.allocate());
	}

	inline Chunk(Chunk &chunk)
	: data_(chunk.data_), dataSize_(chunk.dataSize_) {
	}

	inline Chunk& setData(uint8_t* data, int32_t dataSize) {
		assert(data_ == NULL);
		data_ = data;
		dataSize_ = dataSize;
		return *this;
	}
	
	inline void reset() {
		data_ = NULL;
		dataSize_ = 0;
	}

	virtual ~Chunk() {}

	inline Chunk& copy(Chunk& chunk) {
		assert(data_ != NULL);
		assert(chunk.dataSize_ <= dataSize_);
		memcpy(data_, chunk.data_, chunk.dataSize_);
		dataSize_ = chunk.dataSize_;
		return *this;
	}

	inline uint8_t* getData() {
		return data_;
	}

	inline int32_t getDataSize() const {
		return dataSize_;
	}

	inline Chunk& setMagic() {
		setShort(OFFSET_MAGIC, (int16_t)MAGIC);
		return *this;
	}

	void checkMagic(int64_t offset) const;

	inline Chunk& setCheckSum(uint32_t checksum) {
		setInt(OFFSET_CHECKSUM, static_cast<int32_t>(checksum));
		return *this;
	}
	inline uint32_t getCheckSum() const {
		return static_cast<uint32_t>(getInt(OFFSET_CHECKSUM));
	}

	inline uint32_t calcCheckSum() const {
		return util::fletcher32(
				data_ + HEADER_SIZE, dataSize_ - HEADER_SIZE);
	}

	void checkSum(uint32_t checksum, int64_t offset) const;

	std::string getSignature(int32_t level) const;

	static inline int32_t getHeaderSize() {
		return HEADER_SIZE;
	}

	inline int32_t getCompressedSize() const {
		return getInt(OFFSET_COMPRESSED_SIZE);
	}

	inline Chunk& setCompressedSize(int32_t size) {
		setInt(OFFSET_COMPRESSED_SIZE, size);
		return *this;
	}

	inline int8_t getByte(int32_t offset) const {
		return UtilBytes::getByte(data_, dataSize_, offset);
	}

	inline Chunk& setByte(int32_t offset, uint8_t value) {
		UtilBytes::setByte(data_, dataSize_, offset, value);
		return *this;
	}

	inline void getBytes(int32_t offset, uint8_t* value, int32_t offset2, int32_t len) const {
		memcpy(value + offset2, data_ + offset, len);
	}

	inline Chunk& setBytes(int32_t offset, const uint8_t* value, int32_t offset2, int32_t len) {
		memcpy(data_ + offset, value + offset2, len);
		return *this;
	}

	inline char getChar(int32_t offset) const {
		return UtilBytes::getChar(data_, dataSize_, offset);
	}

	inline Chunk& setChar(int32_t offset, char value) {
		UtilBytes::setChar(data_, dataSize_, offset, value);
		return *this;
	}

	inline int16_t getShort(int32_t offset) const {
		return UtilBytes::getShort(data_, dataSize_, offset);
	}

	inline Chunk& setShort(int32_t offset, int16_t value) {
		UtilBytes::setShort(data_, dataSize_, offset, value);
		return *this;
	}

	inline int32_t getInt(int32_t offset) const {
		return UtilBytes::getInt(data_, dataSize_, offset);
	}

	inline Chunk& setInt(int32_t offset, int32_t value) {
		UtilBytes::setInt(data_, dataSize_, offset, value);
		return *this;
	}

	inline int64_t getLong(int32_t offset) const {
		return UtilBytes::getLong(data_, dataSize_, offset);
	}

	inline Chunk& setLong(int32_t offset, int64_t value) {
		UtilBytes::setLong(data_, dataSize_, offset, value);
		return *this;
	}
	inline std::string getString(int32_t offset, int32_t length) const {
		return UtilBytes::getString(data_, dataSize_, offset, length);
	}

	inline Chunk& setString(int32_t offset, const std::string& value) {
		UtilBytes::setString(data_, dataSize_, offset, value);
		return *this;
	}

	std::string toString() const;
	std::string toString2() const;
};


/*!
	@brief スロット付きチャンク
*/
class ChunkSLT final : public Chunk {
	friend class ObjectAccessor;
protected:
	static const int32_t OFFSET_CHUNK_TYPE;
	static const int32_t OFFSET_NUM_SLOTS;
	static const int32_t OFFSET_UNUSED_NUM_SLOTS;
	static const int32_t OFFSET_LAST_POSITION;
	static const int32_t OFFSET_UNUSED_SPACE_SIZE;
	static const int32_t OFFSET_SLOT_0;
	static const float COMPACTION_RATIO;

public:
	ChunkSLT(ChunkMemoryPool& pool, int32_t chunkSize);

	ChunkSLT(Chunk& chunk);

	virtual ~ChunkSLT() {}

protected:
	ChunkSLT& init();

	inline int32_t getSlotCount() const {
		return Chunk::getInt(OFFSET_NUM_SLOTS);
	}

	inline int32_t getFreeSpace() const {
		return Chunk::getInt(OFFSET_LAST_POSITION);
	}

	inline int32_t getType() const {
		return Chunk::getInt(OFFSET_CHUNK_TYPE);
	}

	inline void setType(int32_t type) {
		Chunk::setInt(OFFSET_CHUNK_TYPE, type);
	}

	inline int32_t getSlotLength(int32_t slotNo) const {
		return Chunk::getInt(OFFSET_SLOT_0 + slotNo
							 * static_cast<int32_t>(sizeof(int32_t) * 2));
	}

	inline void setSlotLength(int32_t slotNo, int32_t length) {
		Chunk::setInt(OFFSET_SLOT_0 + slotNo
					  * static_cast<int32_t>(sizeof(int32_t) * 2), length);
	}

	inline int32_t getSlotOffset(int32_t slotNo) const {
		return Chunk::getInt(OFFSET_SLOT_0 + static_cast<int32_t>(
				slotNo * (sizeof(int32_t) * 2) + sizeof(int32_t)) );
	}

	inline void setSlotOffset(int32_t slotNo, int32_t offset) {
		Chunk::setInt(OFFSET_SLOT_0 + static_cast<int32_t>(
				slotNo * (sizeof(int32_t) * 2) + sizeof(int32_t)), offset);
	}

	inline int32_t getAvailableSpace() const {
		int32_t lastOffset = Chunk::getInt(OFFSET_LAST_POSITION);
		int32_t slotCount = Chunk::getInt(OFFSET_NUM_SLOTS);
		return lastOffset - (OFFSET_SLOT_0 + static_cast<int32_t>(
			slotCount * (sizeof(int32_t) * 2)) );
	}

	inline static int32_t getNeededSpace(int32_t len) {
		return len + static_cast<int32_t>(sizeof(int32_t) * 2);
	}

	inline static int32_t vacancyToAvailableSpace(int8_t vacancy) {
		if (vacancy == 101) {
			return INT32_MAX;
		} else {
			return 100 * vacancy;
		}
	}

	inline static int8_t availableSpaceToVacancy(int32_t availableSpace) {
		if (availableSpace / 100 > 100) {
			return 101;
		} else {
			return static_cast<int8_t>(availableSpace / 100);
		}
	}

	int32_t allocateObject(uint8_t* record, int32_t recordLength);

	void tryAllocateObject(int32_t slotid, uint8_t* record, int32_t recordLength);

	std::tuple<void*, size_t> getObject(int32_t offset);

	template <typename T>
	T* getObject(int32_t slotId);

	void overwriteObject(int32_t slotId, const uint8_t* record, int32_t recordLength);

	void compact();

	void removeObject(int32_t slotId);

public:
	std::string toString() const;
};


template <typename T>
inline T* ChunkSLT::getObject(int32_t slotId) {
	std::tuple<void*, size_t> tuple = getObject(slotId);
	return static_cast<T*>(std::get<0>(tuple));
}

/*!
	@brief バディシステムによる割り当てチャンク
*/
class ChunkBuddy final : public Chunk {
	friend class ObjectManagerV4;
	friend class GroupObjectAccessor;
	friend class ChunkManager;
	friend class StoreV5Impl;
	friend class SystemService;
	friend class CheckpointService;
public:
	static const int32_t CHUNK_HEADER_FULL_SIZE = 256;  
	struct V4ChunkHeader;

	ChunkBuddy(ChunkMemoryPool &pool, int32_t chunkSize);

	ChunkBuddy(Chunk& chunk);

	virtual ~ChunkBuddy() {}

protected:
	ChunkBuddy& init();

	int32_t allocateObject(uint8_t* record, uint8_t expSize, ObjectType type);

	int32_t tryAllocateObject(
			uint8_t* record, uint8_t requestExpSize, ObjectType type, int32_t offset);

	void* getObject(int32_t offset);

	template <typename T>
	T* getObject(int32_t offset);

	void overwriteObject(int32_t offset, const uint8_t* record, uint8_t expSize);

	void removeObject(int32_t offset);

	size_t getObjectSize(int32_t offset, int32_t headerSize) const;

	bool isValidObject(int32_t offset) const;

	static int8_t CHUNK_EXP_SIZE_;
	static uint8_t minPower_;
	static uint8_t maxPower_;
	static const int32_t hSize_ = sizeof(uint32_t);
	static int32_t tableSize_;
	static int32_t freeListOffset_;

protected:
	static void resetParameters(int32_t chunkSize);

	bool check() const;

	uint32_t getVersion() const;
	void setVersion(uint16_t version);

	bool checkMagicNum() const;
	void setMagicNum();

	uint32_t getPartitionNum() const;
	void setPartitionNum(PartitionId partitionNum);

	PartitionGroupId getPartitionGroupId() const;
	void setPartitionGroupId(PartitionGroupId pgId);

	PartitionId getPartitionId() const;
	void setPartitionId(PartitionId pId);

	ChunkCategoryId getChunkCategoryId() const;
	void setChunkCategoryId(ChunkCategoryId categoryId);

	uint8_t getChunkExpSize() const;
	void setChunkExpSize(uint8_t chunkExpSize);

	uint8_t getMaxFreeExpSize() const;
	void setMaxFreeExpSize(uint8_t expSize);

	int32_t getOccupiedSize() const;
	void setOccupiedSize(int32_t occupiedSize);
	void addOccupiedSize(int32_t size);
	void subtractOccupiedSize(int32_t size);

	DSGroupId getGroupId() const;
	void setGroupId(DSGroupId groupId);

	int64_t getChunkId() const;
	void setChunkId(int64_t cId);

	ChunkKey getChunkKey() const;
	void setChunkKey(ChunkKey chunkKey);

	uint8_t getAttribute() const;
	void setAttribute(uint8_t attribute);

	static void validateHeader(const uint8_t* data,
			PartitionId expectedPartitionNum, uint8_t expectedChunkExpSize,
			uint32_t expectedSplitCount, uint32_t expectedSplitStripeSize,
			bool checkSplitInfo,
			bool checkCheckSum);

	static std::string dumpFieldName();
	static std::string dump(const uint8_t* chunk);

	static void dumpHeader(const uint8_t* data, size_t dumpSize = 32, bool hexDump = false);
public:
	std::string toString() const;

private:
	uint8_t findMaxFreeBlockSize(uint8_t startPower) const;
	/*!
		@brief オブジェクトのヘッダ/未使用の場合は空きチェーン情報も記憶するクラス
	*/
	class V4ObjectHeader {
	private:
		uint8_t power_;  
		int8_t type_;  
		uint16_t check_;  

		static const uint16_t MAGIC_NUMBER = 0xc1ac;  

	public:
		int32_t prev_;
		int32_t next_;

		static const uint8_t OBJECT_HEADER_SIZE =
				static_cast<uint8_t>(sizeof(uint32_t));

		/*!
			@brief オブジェクトサイズを指数表現で取得
			@return オブジェクトサイズ
		*/
		uint8_t getPower() const {
			return power_ & 0x7fu;
		}

		/*!
			@brief オブジェクトの使用/未使用、サイズ(指数表現)を設定
			@param [in] isUsed 使用/未使用フラグ
			@param [in] power 有効範囲は [0,32)
			@param [in] type オブジェクトタイプ
			@attention
				isUsed=trueの場合はtypeを与える。isUsed=falseの場合はtypeを略してデフォルト値を使う。
		*/
		void setUsed(
			bool isUsed, uint32_t power, int8_t type = OBJECT_TYPE_UNKNOWN) {
			power_ = static_cast<uint8_t>(0x80u * isUsed + (power & 0x0000007fu));
			type_ = type;
			check_ = MAGIC_NUMBER;
		}

		/*!
			@brief オブジェクトの使用/未使用を判定
		*/
		bool isUsed() const {
			return 0 != (power_ & 0x80u);
		}

		/*!
			@brief オブジェクト領域かの検証関数
		*/
		bool isValid() const {
			return MAGIC_NUMBER == check_;
		}

		/*!
			@brief オブジェクトタイプの取得
		*/
		int8_t getType() const {
			return type_;
		}

		/*!
			@brief コンストラクタ
		*/
		V4ObjectHeader()
			: power_(0),
			  type_(OBJECT_TYPE_UNKNOWN),
			  check_(MAGIC_NUMBER),
			  prev_(0),
			  next_(0) {}
		/*!
			@brief デストラクタ
		*/
		~V4ObjectHeader() {}

	private:
		V4ObjectHeader(const V4ObjectHeader &);
		V4ObjectHeader &operator=(const V4ObjectHeader &);
	};  
	/*!
		@brief 指定したオフセットのオブジェクトのヘッダを取得
		@param [in] base Chunkの先頭
		@param [in] offset オフセット
		@return オブジェクトヘッダ
	*/
	static inline V4ObjectHeader *getV4ObjectHeader(uint8_t *base, int32_t offset) {
		return reinterpret_cast<V4ObjectHeader *>(base + offset);
	}

	/*!
		@brief フリーリストを取得
		@return フリーリストの先頭ポインタ
	*/
	inline uint32_t *getFreeList() const {
		return reinterpret_cast<uint32_t *>(data_ + freeListOffset_);
	}
};

/*!
	@brief Operates header data of each Chunk.
 */
struct ChunkBuddy::V4ChunkHeader {


	static const uint16_t GS_FILE_VERSION = 2000;
	static const int32_t CHUNK_HEADER_PADDING_ = 12;  

	static const int32_t OBJECT_HEADER_OFFSET = 0;
	static const int32_t CHECKSUM_OFFSET = CHUNK_HEADER_PADDING_;
	static const int32_t VERSION_OFFSET =
		CHECKSUM_OFFSET + sizeof(uint32_t);
	static const int32_t MAGIC_OFFSET = VERSION_OFFSET + sizeof(uint16_t);

	static const int32_t OFFSET_COMPRESSED_SIZE0 = MAGIC_OFFSET + sizeof(int32_t);


	static const int32_t OCCUPIED_SIZE_OFFSET =
		OFFSET_COMPRESSED_SIZE0 + sizeof(int32_t);
	static const int32_t MAX_FREE_EXP_SIZE_OFFSET =
		OCCUPIED_SIZE_OFFSET + sizeof(int32_t);
	static const int32_t CHUNK_ATTRIBUTE_OFFSET =
		MAX_FREE_EXP_SIZE_OFFSET + sizeof(int8_t);
	static const int32_t CHUNK_POWER_SIZE_OFFSET =
		CHUNK_ATTRIBUTE_OFFSET + sizeof(int8_t);
	static const int32_t CHUNK_CATEGORY_ID_OFFSET =
		CHUNK_POWER_SIZE_OFFSET + sizeof(int8_t);

	static const int32_t PARTITION_NUM_OFFSET =
		CHUNK_CATEGORY_ID_OFFSET + sizeof(int8_t);
	static const int32_t PARTITION_ID_OFFSET =
		PARTITION_NUM_OFFSET + sizeof(int32_t);
	static const int32_t GROUP_ID_OFFSET =
		PARTITION_ID_OFFSET + sizeof(int32_t);
	static const int32_t CHUNK_ID_OFFSET =
		GROUP_ID_OFFSET + sizeof(int64_t);
	static const int32_t CHUNK_KEY_OFFSET =
		CHUNK_ID_OFFSET + sizeof(int64_t);

	static const int32_t CHUNK_HEADER_SIZE =
		CHUNK_KEY_OFFSET + sizeof(uint32_t);

	static const uint16_t MAGIC_NUMBER = 0xcd14;
	static const int32_t CHECK_SUM_START_OFFSET = VERSION_OFFSET;
};


inline void* ChunkBuddy::getObject(int32_t offset) {
	return static_cast<void*>(data_ + offset);
}

template <typename T>
inline T* ChunkBuddy::getObject(int32_t offset) {
	void* addr = getObject(offset);
	return static_cast<T*>(addr);
}

inline size_t ChunkBuddy::getObjectSize(int32_t offset, int32_t headerSize) const {
	assert(isValidObject(offset));
	uint8_t* objectAddr = data_ + offset;
	V4ObjectHeader *header = getV4ObjectHeader(objectAddr, -headerSize);
	uint8_t k = header->getPower();
	uint32_t freedBlockSize = UINT32_C(1) << k;
	return freedBlockSize - headerSize;
}

inline bool ChunkBuddy::isValidObject(int32_t offset) const {
	V4ObjectHeader *header =
		reinterpret_cast<V4ObjectHeader *>(data_ + offset - hSize_);
	return header->isValid() && header->isUsed();
}


inline ChunkBuddy::ChunkBuddy(ChunkMemoryPool &pool, int32_t chunkSize) : Chunk(pool, chunkSize){
	init();
}

inline ChunkBuddy::ChunkBuddy(Chunk& chunk) : Chunk(chunk) {
}

/*!
	@brief バージョン情報を取得
*/
UTIL_FORCEINLINE uint32_t ChunkBuddy::getVersion() const {
	return Chunk::getShort(V4ChunkHeader::VERSION_OFFSET);
}
/*!
	@brief バージョン情報を設定
*/
UTIL_FORCEINLINE void ChunkBuddy::setVersion(uint16_t version) {
	Chunk::setInt(V4ChunkHeader::VERSION_OFFSET, version);
}

/*!
	@brief マジックナンバーによる確認
*/
UTIL_FORCEINLINE bool ChunkBuddy::checkMagicNum() const {
	return (V4ChunkHeader::MAGIC_NUMBER
			== static_cast<uint16_t>(Chunk::getShort(V4ChunkHeader::MAGIC_OFFSET)) );
}
/*!
	@brief マジックナンバーの設定
*/
UTIL_FORCEINLINE void ChunkBuddy::setMagicNum() {
	Chunk::setShort(V4ChunkHeader::MAGIC_OFFSET,
					static_cast<int16_t>(V4ChunkHeader::MAGIC_NUMBER) );
}
/*!
	@brief Partition数を取得
*/
UTIL_FORCEINLINE uint32_t ChunkBuddy::getPartitionNum() const {
	return Chunk::getInt(V4ChunkHeader::PARTITION_NUM_OFFSET);
}
/*!
	@brief Partition数を設定
*/
UTIL_FORCEINLINE void ChunkBuddy::setPartitionNum(PartitionId partitionNum) {
	Chunk::setInt(V4ChunkHeader::PARTITION_NUM_OFFSET, partitionNum);
}
/*!
	@brief Partition番号の取得
*/
UTIL_FORCEINLINE PartitionId ChunkBuddy::getPartitionId() const {
	return static_cast<PartitionId>(
		Chunk::getInt(V4ChunkHeader::PARTITION_ID_OFFSET));
}
/*!
	@brief Partition番号の設定
*/
UTIL_FORCEINLINE void ChunkBuddy::setPartitionId(PartitionId pId) {
	Chunk::setInt(V4ChunkHeader::PARTITION_ID_OFFSET, pId);
}
/*!
	@brief ChunkCategoryIdを取得
*/
UTIL_FORCEINLINE ChunkCategoryId ChunkBuddy::getChunkCategoryId() const {
	return static_cast<ChunkCategoryId>(
		Chunk::getByte(V4ChunkHeader::CHUNK_CATEGORY_ID_OFFSET));
}
/*!
	@brief ChunkCategoryIdを設定
*/
UTIL_FORCEINLINE
void ChunkBuddy::setChunkCategoryId(ChunkCategoryId categoryId) {
	Chunk::setByte(V4ChunkHeader::CHUNK_CATEGORY_ID_OFFSET, categoryId);
}
/*!
	@brief Chunkサイズ(指数表現)を取得
*/
UTIL_FORCEINLINE uint8_t ChunkBuddy::getChunkExpSize() const {
	return Chunk::getByte(V4ChunkHeader::CHUNK_POWER_SIZE_OFFSET);
}
/*!
	@brief Chunkサイズ(指数表現)を設定
*/
UTIL_FORCEINLINE void ChunkBuddy::setChunkExpSize(uint8_t chunkExpSize) {
	Chunk::setByte(V4ChunkHeader::CHUNK_POWER_SIZE_OFFSET, chunkExpSize);
}
/*!
	@brief 未使用中サイズを取得
*/
UTIL_FORCEINLINE
uint8_t ChunkBuddy::getMaxFreeExpSize() const {
	return Chunk::getByte(V4ChunkHeader::MAX_FREE_EXP_SIZE_OFFSET);
}
/*!
	@brief 未使用中サイズの設定
*/
UTIL_FORCEINLINE
void ChunkBuddy::setMaxFreeExpSize(uint8_t unoccupiedSize) {
	Chunk::setByte(V4ChunkHeader::MAX_FREE_EXP_SIZE_OFFSET, unoccupiedSize);
}
/*!
	@brief 使用中サイズを取得
*/
UTIL_FORCEINLINE int32_t ChunkBuddy::getOccupiedSize() const {
	return Chunk::getInt(V4ChunkHeader::OCCUPIED_SIZE_OFFSET);
}
/*!
	@brief 使用中サイズの設定
*/
UTIL_FORCEINLINE void ChunkBuddy::setOccupiedSize(int32_t occupiedSize) {
	Chunk::setInt(V4ChunkHeader::OCCUPIED_SIZE_OFFSET, occupiedSize);
}
/*!
	@brief 使用中サイズを加算
*/
UTIL_FORCEINLINE void ChunkBuddy::addOccupiedSize(int32_t size) {
	int32_t current = getOccupiedSize();
	Chunk::setInt(V4ChunkHeader::OCCUPIED_SIZE_OFFSET, current + size);
}
/*!
	@brief 使用中サイズを減算
*/
UTIL_FORCEINLINE void ChunkBuddy::subtractOccupiedSize(int32_t size) {
	int32_t current = getOccupiedSize();
	if (current >= size) {
		Chunk::setInt(V4ChunkHeader::OCCUPIED_SIZE_OFFSET, current - size);
	} else {
		Chunk::setInt(V4ChunkHeader::OCCUPIED_SIZE_OFFSET, 0);
	}
}
/*!
	@brief GroupIdの取得
*/
UTIL_FORCEINLINE DSGroupId ChunkBuddy::getGroupId() const {
	return static_cast<ChunkId>(Chunk::getLong(V4ChunkHeader::GROUP_ID_OFFSET));
}
/*!
	@brief GroupIdの設定
*/
UTIL_FORCEINLINE void ChunkBuddy::setGroupId(DSGroupId groupId) {
	Chunk::setLong(V4ChunkHeader::GROUP_ID_OFFSET, groupId);
}
/*!
	@brief ChunkIdの取得
*/
UTIL_FORCEINLINE int64_t ChunkBuddy::getChunkId() const {
	return static_cast<int64_t>(Chunk::getLong(V4ChunkHeader::CHUNK_ID_OFFSET));
}
/*!
	@brief ChunkIdの設定
*/
UTIL_FORCEINLINE void ChunkBuddy::setChunkId(int64_t cId) {
	Chunk::setLong(V4ChunkHeader::CHUNK_ID_OFFSET, cId);
}
/*!
	@brief BatchFreeのKeyの取得
*/
UTIL_FORCEINLINE ChunkKey ChunkBuddy::getChunkKey() const {
	return static_cast<ChunkKey>(
		Chunk::getInt(V4ChunkHeader::CHUNK_KEY_OFFSET));
}
/*!
	@brief BatchFreeのKeyの設定
*/
UTIL_FORCEINLINE void ChunkBuddy::setChunkKey(ChunkKey chunkKey) {
	Chunk::setInt(V4ChunkHeader::CHUNK_KEY_OFFSET, chunkKey);
}
/*!
	@brief  Chunk属性を取得
*/
UTIL_FORCEINLINE uint8_t ChunkBuddy::getAttribute() const {
	return Chunk::getByte(V4ChunkHeader::CHUNK_ATTRIBUTE_OFFSET);
}
/*!
	@brief Chunk属性を設定
*/
UTIL_FORCEINLINE void ChunkBuddy::setAttribute(uint8_t attribute) {
	Chunk::setByte(V4ChunkHeader::CHUNK_ATTRIBUTE_OFFSET, attribute);
}

#endif 

