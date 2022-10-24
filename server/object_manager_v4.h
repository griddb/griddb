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
#ifndef OBJECT_MANAGER_V4_H_
#define OBJECT_MANAGER_V4_H_

#include "data_type.h"
#include "chunk_manager.h"
#include "chunk.h"
#include "config_table.h"


#include <unordered_map>


class GroupObjectAccessor;
class ObjectManagerStats;


struct OIdBitUtils {
	static inline bool isLargeChunkMode(uint32_t chunkExpSize) {
		return (chunkExpSize > 20) ? true : false;
	}
};

/*!
	@brief ロウ指向データストアオブジェクト操作クラス
*/
class ObjectManagerV4 {
	friend class GroupObjectAccessor;
public:
	static const uint8_t MAX_CHUNK_EXP_SIZE_ = 26;
	static const int32_t MIN_CHUNK_EXP_SIZE_ = 15;
	static const int32_t CHUNK_CATEGORY_NUM = 6;

	static const uint32_t CHUNK_HEADER_BLOCK_SIZE = 256;

	static const uint8_t OBJECT_HEADER_SIZE =
			static_cast<uint8_t>(sizeof(uint32_t));

	ObjectManagerV4(
			const ConfigTable& configTable, ChunkManager* chunkManager,
			ObjectManagerStats &stats);
	~ObjectManagerV4();


	void free(ChunkAccessor &chunkaccessor, DSGroupId groupId, OId oId);


	DSObjectSize getSize(uint8_t* objectAddr) const;

	void resetRefCounter();
	void freeLastLatchPhaseMemory();

	void setSwapOutCounter(int64_t counter);
	int64_t getSwapOutCounter();
	void setStoreMemoryAgingSwapRate(double ratio);
	double getStoreMemoryAgingSwapRate();

	DSObjectSize estimateAllocateSize(DSObjectSize requestSize);
	DSObjectSize getAllocateSize(uint32_t exponent);

	uint8_t getObjectExpSize(uint32_t requestSize);

	uint32_t getMaxObjectSize() const {
		return maxObjectSize_;
	}
	uint32_t getHalfOfMaxObjectSize() const {
		return halfOfMaxObjectSize_;
	}
	uint32_t getRecommendedLimitObjectSize() const {
		return recommendLimitObjectSize_;
	}

	bool isActive(DSGroupId groupId) const;
	bool isActive() const;

	ChunkId getHeadChunkId(DSGroupId groupId) const;

	PartitionId getPId() {
		return chunkManager_->getPId();
	}
	ChunkManager* chunkManager() {
		return chunkManager_;
	}

	static OId getOId(
			uint32_t chunkExpSize, ChunkCategoryId categoryId,
			ChunkId cId, DSObjectOffset offset);
	OId getOId(
			DSGroupId groupId, ChunkId cId, DSObjectOffset offset);

	ChunkId getChunkId(OId oId) const;
	DSObjectOffset getOffset(OId oId) const;
	DSObjectOffset getRelativeOffset(OId oId) const;
	static ChunkCategoryId getChunkCategoryId(OId oId);
	static OId getUserArea(OId oId);
	static OId setUserArea(OId oId, OId userArea);
	static OId getBaseArea(OId oId);
	static bool isV4OId(OId oId);
	int64_t getV5ChunkId(DSGroupId groupId, ChunkId v4ChunkId);
	void updateStoreObjectUseStats();
	const ChunkManagerStats& getChunkManagerStats();

	inline uint32_t getChunkSize() const {
		return 1UL << CHUNK_EXP_SIZE_;
	}
	void drop();

	ChunkCategoryId getChunkCategoryNum() const;
	uint32_t getAffinitySize();

	void initializeV5StartChunkIdList(int64_t maxV4GroupId, const std::deque<int64_t>& chunkIdList);

	ObjectType getObjectType(uint8_t* objectAddr) const;
	void validateRefCounter();
	void checkDirtyFlag();

private:
	class HeaderBlock;

	void validateObject(void* objectAddr, OId oId, DSGroupId groupId) const;
	static bool isValidObject(void* objectAddr);
	void errorOId(OId oId, DSGroupId groupId) const;

	void initV4ChunkHeader(ChunkBuddy& chunk, DSGroupId groupId, ChunkId cId);

	static uint32_t calcUnitChunkShiftBit(bool isLargeChunkMode) {
		return isLargeChunkMode ? 38 : 32;
	}
	static uint32_t calcMaxUnitOffsetBit(bool isLargeChunkMode) {
		return isLargeChunkMode ? 22 : 16;
	}

	static uint32_t calcMaxChunkId(uint32_t blockSize) {
		const uint32_t chunkExpSize = util::nextPowerBitsOf2(blockSize);
		const bool isLargeChunkMode = OIdBitUtils::isLargeChunkMode(chunkExpSize);
		return isLargeChunkMode ? ((static_cast<int32_t>(1) << 26) - 2): INT32_MAX - 1;
	}

	const ConfigTable& configTable_;

	ChunkManager* chunkManager_;

	const ChunkId OBJECT_MAX_CHUNK_ID_;
	const PartitionId PARTITION_NUM_;   
	const uint32_t CHUNK_EXP_SIZE_;		
	uint32_t maxObjectSize_;
	uint32_t halfOfMaxObjectSize_;
	uint32_t recommendLimitObjectSize_;
	const bool LARGE_CHUNK_MODE_;
	const uint32_t UNIT_CHUNK_SHIFT_BIT;
	static const uint32_t UNIT_OFFSET_SHIFT_BIT = 16;
	static const uint32_t CATEGORY_ID_SHIFT_BIT = 10;
	static const uint32_t MAGIC_SHIFT_BIT = 1 + 12;
	static const uint32_t MAX_USER_BIT = 10;
	static const uint32_t UNIT_OFFSET_ROUND_BIT = 4;

	const uint32_t MAX_UNIT_OFFSET_BIT;
	const uint64_t MASK_UNIT_OFFSET;

	const int32_t chunkHeaderSize_;
	const int32_t minPower_;
	const int32_t maxPower_;
	const int32_t tableSize_;
	const int32_t freeListOffset_;
						  
	int64_t maxV4GroupId_;
	std::deque<int64_t> v5StartChunkIdList_;
	int64_t swapOutCounter_;

	std::unordered_map< int64_t, MeshedChunkTable::Group::StableGroupChunkCursor* > free_;

	ObjectManagerStats &stats_;

	static const int32_t RANGE = 1024;

	inline static bool isValidOId(OId oId);
	inline bool notEqualChunk(OId oId1, OId oId2);

	static const uint64_t MASK_32BIT = 0xFFFFFFFFULL;
	static const uint64_t MASK_16BIT = 0x0000FFFFULL;
	static const uint64_t MASK_3BIT = 0x00000007ULL;

	static const int32_t MAGIC_NUMBER_EXP_SIZE = 3;
	static const uint64_t MASK_MAGIC = 0x000000000000e000ULL;
	static const uint64_t MAGIC_NUMBER = 0x000000000000a000ULL;

	static const uint64_t MASK_USER = 0x000003FFULL;

	/*!
		@brief Free mode of allocated Objects one each or a Chunk of Objects at
	   once.
	*/
	enum FreeMode {
		SELF_FREE_MODE = 0,  
		BATCH_FREE_MODE = 1  
	};

	enum SearchMode {
		SMALL_SIZE_SKIP_SEARCH_MODE = 0,  
		SMALL_SIZE_SEARCH_MODE = 1,  
	};
	/*!
		@brief Attribute of Chunk Category
	*/
	struct ChunkCategoryAttribute {
		ChunkCategoryAttribute() : freeMode_(SELF_FREE_MODE), searchMode_(SMALL_SIZE_SKIP_SEARCH_MODE){}
		FreeMode freeMode_;
		SearchMode searchMode_;
	};

	int64_t searchChunk(MeshedChunkTable::Group& group, uint8_t powerSize, ChunkAccessor &ca);
};

/*!
	@brief オブジェクトのヘッダ/未使用の場合は空きチェーン情報も記憶するクラス
 */
class ObjectManagerV4::HeaderBlock {
	private:
		uint8_t power_;  
		int8_t type_;  
		uint16_t check_;  

		static const uint16_t MAGIC_NUMBER = 0xc1ac;  

	public:
		int32_t prev_;
		int32_t next_;

	inline uint8_t getPower() const {
		return power_ & 0x7fu;
	}

	inline void setUsed(
			bool isUsed, uint32_t power, int8_t type = OBJECT_TYPE_UNKNOWN)
	{
		power_ = static_cast<uint8_t>(0x80u * isUsed + (power & 0x0000007fu));
		type_ = type;
		check_ = MAGIC_NUMBER;
	}

	inline bool isUsed() const {
		return 0 != (power_ & 0x80u);
	}

	inline bool isValid() const {
		return MAGIC_NUMBER == check_;
	}

	inline int8_t getType() const {
		return type_;
	}

	HeaderBlock()
	: power_(0),
	  type_(OBJECT_TYPE_UNKNOWN),
	  check_(MAGIC_NUMBER),
	  prev_(0),
	  next_(0) {}

	~HeaderBlock() {}

private:
	HeaderBlock(const HeaderBlock &);
	HeaderBlock &operator=(const HeaderBlock &);
};  

/*!
	@brief 特定グループに属するオブジェクトのアクセッサ
	@note グループ管理情報取得回数削減
*/
class GroupObjectAccessor {
public:
	typedef std::pair<ObjectManagerV4*, MeshedChunkTable::Group*> Info;

	GroupObjectAccessor() : objMgr_(NULL), group_(NULL) {
	}

	explicit GroupObjectAccessor(ObjectManagerV4* objMgr) :
			objMgr_(objMgr), group_(NULL) {
		assert(objMgr_);
	}

	explicit GroupObjectAccessor(const Info &info) :
			objMgr_(info.first), group_(info.second) { 
		assert(objMgr_);
		assert(group_);
	}

	GroupObjectAccessor(ObjectManagerV4 *objMgr, DSGroupId groupId) :
			objMgr_(NULL), group_(NULL) {
		reset(objMgr, groupId);
	}

	OId allocate(DSObjectSize requestSize, ObjectType objectType);

	OId allocateNeighbor(  
			DSObjectSize requestSize,
			OId neighborOId, ObjectType objectType);

	void free(OId oId);

	template <typename T> 
	T* get(OId oId, bool isDirty = false);

	void update(OId oId);

	template<typename T>
	T* get(OId oId, OId* lastOId, uint8_t* lastAddr, bool isDirty);

	DSObjectSize getSize(OId oId);
	DSObjectSize getSize(uint8_t* objectAddr) const;

	void pin(OId oId);
	void unpin(OId oId, bool isDirty);

	bool batchFree(
			ChunkKey chunkKey, uint64_t maxScanNum,
			uint64_t& scanNum, util::XArray<OId> &freeList,
			ChunkKey simulateChunkKey, uint64_t& simulateFreeNum);

	void reset() {
		reset(Info());
	}

	void reset(const Info &info) {
		objMgr_ = info.first;
		group_ = info.second;
		chunkAccessor_.reset();
	}

	void reset(ObjectManagerV4 *objMgr, DSGroupId groupId) {
		assert(objMgr);
		reset(Info(objMgr, objMgr->chunkManager_->putGroup(groupId)));
	}

	Info getInfo() const {
		return Info(objMgr_, group_);
	}

	MeshedChunkTable::Group* group() {
		return group_;
	}

	ChunkAccessor& chunkAccessor() {
		return chunkAccessor_;
	}

private:
	GroupObjectAccessor(const GroupObjectAccessor&);
	GroupObjectAccessor& operator=(const GroupObjectAccessor&);

	ObjectManagerV4* objMgr_;
	MeshedChunkTable::Group* group_;
	ChunkAccessor chunkAccessor_;
};

/*!
	@brief Strategy for allocating object
*/
struct AllocateStrategy {
	struct Info {
		Info() :
				groupId_(UNDEF_DS_GROUPID) {
		}

		Info(DSGroupId groupId, const GroupObjectAccessor::Info &baseInfo) :
				groupId_(groupId),
				baseInfo_(baseInfo) {
		}

		DSGroupId groupId_;
		GroupObjectAccessor::Info baseInfo_;
	};

	AllocateStrategy() : groupId_(Info().groupId_) {
	}

	explicit AllocateStrategy(const Info &info) :
			groupId_(info.groupId_),
			accessor_(info.baseInfo_) {
	}

	AllocateStrategy(DSGroupId groupId, ObjectManagerV4 *objMgr) :
			groupId_(groupId),
			accessor_(objMgr, groupId) {
	}

	void reset() {
		set(Info());
	}

	void set(const Info &info) {
		groupId_ = info.groupId_;
		accessor_.reset(info.baseInfo_);
	}

	void set(DSGroupId groupId, ObjectManagerV4 *objMgr) {
		groupId_ = groupId;
		accessor_.reset(objMgr, groupId);
	}

	Info getInfo() const {
		return Info(groupId_, accessor_.getInfo());
	}

	DSGroupId getGroupId() const {
		return groupId_;
	}

private:
	AllocateStrategy(const AllocateStrategy&);
	AllocateStrategy &operator=(const AllocateStrategy&);

public:
	DSGroupId groupId_;
	GroupObjectAccessor accessor_;
};

struct ObjectManagerStats {
	enum Param {
		OBJECT_STAT_ALLOCATE_SIZE,
		OBJECT_STAT_END
	};

	typedef LocalStatTable<Param, OBJECT_STAT_END> Table;
	typedef Table::Mapper Mapper;

	explicit ObjectManagerStats(const Mapper *mapper);

	static void setUpMapper(Mapper &mapper);

	Table table_;
};


/*!
	@brief 論理IDからOID(物理ID)計算
	@note DataStoreにてInitializePartition直後の最初のOIdの値を把握するためにpublic化
	@note offsetはObjectのBody部分のOffset
*/
inline OId ObjectManagerV4::getOId(
		DSGroupId groupId, ChunkId cId, DSObjectOffset offset) {

	ChunkCategoryId categoryId = static_cast<ChunkCategoryId>(7); 
	OId chunkIdOId = ((OId)cId << UNIT_CHUNK_SHIFT_BIT);
	OId unitOffsetOId = (((OId)offset >> UNIT_OFFSET_ROUND_BIT)
						 << UNIT_OFFSET_SHIFT_BIT);  
	OId categoryIdOId = ((OId)categoryId << CATEGORY_ID_SHIFT_BIT);
	OId magicOId = MAGIC_NUMBER;
	return (magicOId | categoryIdOId | unitOffsetOId | chunkIdOId);
}

inline OId ObjectManagerV4::getOId(
		uint32_t chunkExpSize, ChunkCategoryId categoryId,
		ChunkId cId, DSObjectOffset offset) {


	const bool largeChunkMode = OIdBitUtils::isLargeChunkMode(chunkExpSize);
	const uint32_t UNIT_CHUNK_SHIFT_BIT = calcUnitChunkShiftBit(largeChunkMode);
	OId chunkIdOId = ((OId)cId << UNIT_CHUNK_SHIFT_BIT);
	OId unitOffsetOId = (((OId)offset >> UNIT_OFFSET_ROUND_BIT)
						 << UNIT_OFFSET_SHIFT_BIT);  
	OId categoryIdOId = ((OId)7 << CATEGORY_ID_SHIFT_BIT); 
	OId magicOId = MAGIC_NUMBER;
	return (magicOId | categoryIdOId | unitOffsetOId | chunkIdOId);
}

inline ChunkId ObjectManagerV4::getChunkId(OId oId) const {
	ChunkId chunkId = static_cast<ChunkId>(MASK_32BIT & (oId >> UNIT_CHUNK_SHIFT_BIT));
	return chunkId;
}

inline DSObjectOffset ObjectManagerV4::getOffset(OId oId) const {
	DSObjectOffset offset = getRelativeOffset(oId) + OBJECT_HEADER_SIZE;
	return offset;
}

inline DSObjectOffset ObjectManagerV4::getRelativeOffset(OId oId) const {
	const OId offsetMaskBit =
			(static_cast<OId>(1) << static_cast<OId>(MAX_UNIT_OFFSET_BIT)) - 1;
	const OId mask = offsetMaskBit << UNIT_OFFSET_SHIFT_BIT;
	const uint32_t shiftBits =
			UNIT_OFFSET_SHIFT_BIT - UNIT_OFFSET_ROUND_BIT;
	const DSObjectOffset offset =
			static_cast<DSObjectOffset>((mask & oId) >> shiftBits);
	return offset;
}

inline ChunkCategoryId ObjectManagerV4::getChunkCategoryId(OId oId) {
	ChunkCategoryId categoryId = static_cast<ChunkCategoryId>(
			MASK_3BIT & (oId >> CATEGORY_ID_SHIFT_BIT));
	return categoryId;
}

inline OId ObjectManagerV4::getUserArea(OId oId) {
	OId userArea = oId & MASK_USER;
	return userArea;
}

inline OId ObjectManagerV4::setUserArea(OId oId, OId userArea) {
	OId addedOId = getBaseArea(oId) | userArea;
	return addedOId;
}

inline OId ObjectManagerV4::getBaseArea(OId oId) {
	OId baseArea = oId & (~MASK_USER);
	return baseArea;
}

inline bool ObjectManagerV4::isV4OId(OId oId) {
	return (getChunkCategoryId(oId) != 7);
}

inline int64_t ObjectManagerV4::getV5ChunkId(DSGroupId groupId, ChunkId v4ChunkId) {
	assert(groupId >= 0);
	int64_t v5ChunkId = v4ChunkId;
	if (groupId <= maxV4GroupId_) {
		v5ChunkId += v5StartChunkIdList_[groupId];
	}
	assert(v5ChunkId >= 0);
	return v5ChunkId;
}

inline bool ObjectManagerV4::isValidOId(OId oId) {
	uint64_t magic = static_cast<uint64_t>(MASK_MAGIC & oId);
	return (magic == MAGIC_NUMBER);
}

inline bool ObjectManagerV4::notEqualChunk(OId oId1, OId oId2) {
	return ((oId1 & ~MASK_UNIT_OFFSET) != (oId2 & ~MASK_UNIT_OFFSET));
}

/*!
	@brief 指定したサイズに合わせて確保するObjectの指数サイズを返す
	@param [in] requestSize 必要な割り当てサイズ
	@return 指定したサイズに合わせて確保するObjectの指数サイズ
	@note Power とフラグ情報を保護するために余計にメモリが必要(4byte)
*/
inline uint8_t ObjectManagerV4::getObjectExpSize(uint32_t requestSize) {
	assert(0 < requestSize);
	uint8_t k = util::nextPowerBitsOf2(requestSize + OBJECT_HEADER_SIZE);
	if (k < minPower_) {
		k = minPower_;
	}
	return k;
}
/*!
	@brief 要求サイズに対する、Objectの確保予定サイズを返す
	@param [in] requestSize 必要な割り当てサイズ
	@return 割当予定サイズ
*/
inline DSObjectSize ObjectManagerV4::estimateAllocateSize(DSObjectSize requestSize) {
	return (1U << getObjectExpSize(requestSize)) -
		   OBJECT_HEADER_SIZE;
}
inline DSObjectSize ObjectManagerV4::getAllocateSize(uint32_t exponent) {
	return (1 << (CHUNK_EXP_SIZE_ - exponent)) - OBJECT_HEADER_SIZE;
}

/*!
	@brief 指定Objectのサイズを返す
	@param [in] objectAddr Objectのポインタ
	@return Objectのサイズを返す
*/
inline DSObjectSize ObjectManagerV4::getSize(uint8_t* objectAddr) const {
	ChunkBuddy::V4ObjectHeader* header = ChunkBuddy::getV4ObjectHeader(objectAddr, -OBJECT_HEADER_SIZE);
	uint8_t k = header->getPower();
	uint32_t freedBlockSize = UINT32_C(1) << k;
	return freedBlockSize - OBJECT_HEADER_SIZE;
};

/*!
	@brief 指定ObjectのObjectTypeを返す
	@param [in] objectPtr Objectのポインタ
	@return ObjectTypeを返す
*/
inline ObjectType ObjectManagerV4::getObjectType(uint8_t* objectAddr) const {
	ChunkBuddy::V4ObjectHeader* header = ChunkBuddy::getV4ObjectHeader(objectAddr, -OBJECT_HEADER_SIZE);
	return header->getType();
};

inline void ObjectManagerV4::updateStoreObjectUseStats() {
	chunkManager_->updateStoreObjectUseStats();
}

inline ChunkCategoryId ObjectManagerV4::getChunkCategoryNum() const {
	return DS_CHUNK_CATEGORY_SIZE;
}

inline bool ObjectManagerV4::isActive(DSGroupId groupId) const {
	return chunkManager_->hasBlock(groupId);
}

inline bool ObjectManagerV4::isActive() const {
	return chunkManager_->hasBlock();
}

inline ChunkId ObjectManagerV4::getHeadChunkId(DSGroupId groupId) const {
	return chunkManager_->getHeadChunkId(groupId);
}

inline const ChunkManagerStats& ObjectManagerV4::getChunkManagerStats() {
	return chunkManager_->getChunkManagerStats();
}

inline void ObjectManagerV4::validateObject(
		void* objectAddr, OId oId, DSGroupId groupId) const {
	if (!isValidObject(objectAddr)) {
		errorOId(oId, groupId);
	}
}

inline bool ObjectManagerV4::isValidObject(void *objectAddr) {
	ChunkBuddy::V4ObjectHeader* header =
			ChunkBuddy::getV4ObjectHeader(
				static_cast<uint8_t*>(objectAddr), -OBJECT_HEADER_SIZE);
	return (header->isUsed() && header->isValid());
}

inline void ObjectManagerV4::initV4ChunkHeader(ChunkBuddy &chunk, DSGroupId groupId, ChunkId cId) {
	chunk.setPartitionNum(configTable_.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM));
	chunk.setPartitionId(chunkManager_->getPId());
	chunk.setGroupId(groupId);
	chunk.setChunkId(cId);

	chunk.setChunkExpSize(CHUNK_EXP_SIZE_);
	chunk.setChunkCategoryId(7); 
	chunk.setChunkKey(0);
	chunk.setAttribute(0);
	chunk.setOccupiedSize(0);
}



/*!
	@brief ロウ指向データストアオブジェクト取得
*/
template <typename T> 
T* GroupObjectAccessor::get(OId oId, bool isDirty) {
	assert(group_);
	int64_t chunkId = objMgr_->getChunkId(oId);
	if (objMgr_->isV4OId(oId)) {
		chunkId = objMgr_->getV5ChunkId(group_->getId(), chunkId);
	}
	objMgr_->chunkManager_->getChunk(*group_, chunkId, chunkAccessor_);
	if (isDirty) {
		objMgr_->chunkManager_->update(*group_, chunkAccessor_);
	}
	ChunkBuddy* chunk = reinterpret_cast<ChunkBuddy*>(chunkAccessor_.getChunk());
	int32_t offset = objMgr_->getOffset(oId);
	void* ptr = chunk->getObject(offset);
	objMgr_->validateObject(ptr, oId, group_->getId());
	return reinterpret_cast<T*>(ptr);
}

/*!
	@brief getしたオブジェクトの更新宣言
*/
inline void GroupObjectAccessor::update(OId oId) {
#ifndef NDEBUG
	int64_t chunkId = objMgr_->getChunkId(oId);
	if (objMgr_->isV4OId(oId)) {
		chunkId = objMgr_->getV5ChunkId(group_->getId(), chunkId);
	}
	assert(chunkAccessor_.getChunkId() == chunkId);
#endif 
	objMgr_->chunkManager_->update(*group_, chunkAccessor_);
}

/*!
	@brief オブジェクト取得(連続アクセス用)
	@note 前回アクセスしたオブジェクトと同一チャンクに存在しない場合にラッチを自動解除
*/
template <typename T>
T* GroupObjectAccessor::get(
		OId oId, OId* lastOId, uint8_t* lastAddr, bool isDirty) {

	assert(group_);
	assert(oId != UNDEF_OID);
	T* result;
	if (objMgr_->notEqualChunk(oId, *lastOId)) {
		if (*lastOId != UNDEF_OID) {
			unpin(*lastOId, isDirty);
			*lastOId = UNDEF_OID;
		}
		void* ptr = get<uint8_t>(oId);
		result = static_cast<T*>(ptr);
	}
	else {
		assert(*lastOId != UNDEF_OID);

		result =
				reinterpret_cast<T*>(lastAddr - objMgr_->getRelativeOffset(*lastOId) +
					objMgr_->getRelativeOffset(oId));

	}
	objMgr_->validateObject(result, oId, group_->getId());
	*lastOId = oId;
	return result;
}

/*!
	@brief メモリ上にピン留め
*/
inline void GroupObjectAccessor::pin(OId oId) {
	assert(group_);
	int64_t chunkId = objMgr_->getChunkId(oId);
	if (objMgr_->isV4OId(oId)) {
		chunkId = objMgr_->getV5ChunkId(group_->getId(), chunkId);
	}
	objMgr_->chunkManager_->getChunk(*group_, chunkId, chunkAccessor_);
}

/*!
	@brief ピン留め解除(griddb V4のunfix、sqliteのunpin)
*/
inline void GroupObjectAccessor::unpin(OId oId, bool isDirty) {
	assert(group_);
	if (!chunkAccessor_.isEmpty()) {
#ifndef NDEBUG
		int64_t chunkId = objMgr_->getChunkId(oId);
		if (objMgr_->isV4OId(oId)) {
			chunkId = objMgr_->getV5ChunkId(group_->getId(), chunkId);
		}
		assert(chunkAccessor_.getChunkId() == chunkId);
#endif 
		chunkAccessor_.unpin(isDirty);
	}
	else {
		assert(false);
	}
}

/*!
	@brief サイズ取得: ラッチは保持しない
*/
inline DSObjectSize GroupObjectAccessor::getSize(OId oId) {
	assert(group_);
	int64_t chunkId = objMgr_->getChunkId(oId);
	if (objMgr_->isV4OId(oId)) {
		chunkId = objMgr_->getV5ChunkId(group_->getId(), chunkId);
	}
	objMgr_->chunkManager_->getChunk(*group_, chunkId, chunkAccessor_);
	ChunkBuddy* chunk = reinterpret_cast<ChunkBuddy*>(chunkAccessor_.getChunk());

	int32_t offset = objMgr_->getOffset(oId);
	size_t size = chunk->getObjectSize(offset, ObjectManagerV4::OBJECT_HEADER_SIZE);
	chunkAccessor_.unpin();
	return size;
}

/*!
	@brief 指定Objectのサイズを返す
	@param [in] objectAddr Objectのポインタ
	@return Objectのサイズを返す
*/
inline DSObjectSize GroupObjectAccessor::getSize(uint8_t* objectAddr) const {
	ChunkBuddy::V4ObjectHeader* header =
			ChunkBuddy::getV4ObjectHeader(
				objectAddr, -ObjectManagerV4::OBJECT_HEADER_SIZE);
	uint8_t k = header->getPower();
	uint32_t freedBlockSize = UINT32_C(1) << k;
	return freedBlockSize - ObjectManagerV4::OBJECT_HEADER_SIZE;
};

#endif  
