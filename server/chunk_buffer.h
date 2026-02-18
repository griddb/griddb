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
	@brief Definition of ChunkBuffer
*/
#ifndef CHUNK_BUFFER_H_
#define CHUNK_BUFFER_H_

#include "utility_v5.h"
#include "data_type.h"
#include "data_store_common.h"
#include "config_table.h"
#include "log_manager.h"
#include "chunk.h"
#include <vector>
#include <string>
#include <memory>
#include <unordered_map>

class VirtualFileBase;
class Chunk;
struct LRUFrame;
/*!
	@brief チャンクバッファ用ハッシュマップのキー
*/
struct BufferMapKey {
	int32_t pId_;
	int64_t offset_;

	BufferMapKey() : pId_(-1), offset_(-1) {}
	BufferMapKey(int32_t pId, int32_t offset) : pId_(pId), offset_(offset) {}

	bool operator==(const BufferMapKey& other) const {
		return (offset_ == other.offset_ && pId_ == other.pId_);
	}
	bool operator!=(const BufferMapKey& other) const {
		return !(this->operator==(other));
	}

	struct Hash {
		uint64_t operator()(const BufferMapKey& k) const {
			return (static_cast<uint64_t>(k.pId_) << 48) | k.offset_;  
		}
	};
};

/*!
	@brief チャンクバッファ用ハッシュマップ
*/
class BufferHashMap {
	typedef std::unordered_map< BufferMapKey, int64_t, BufferMapKey::Hash > BufferMap;

public:
	BufferHashMap();
	~BufferHashMap();

	void insert(uint32_t id1, uint64_t id2, int32_t pos);
	int32_t find(uint32_t id1, uint64_t id2) const;
	bool remove(uint32_t id1, uint64_t id2);
	bool swapPosition(
			uint32_t pId, uint64_t bufferId1, uint64_t bufferId2,
			int32_t &pos1, int32_t &pos2);
	BufferMap& getMap() { return map_; }


private:
	BufferMap map_;

	BufferHashMap(const BufferHashMap&);
	BufferHashMap& operator=(const BufferHashMap&);
};

/*!
	@brief チャンクバッファの統計値
*/
struct ChunkBufferStats {
	enum Param {
		BUF_STAT_SWAP_READ_COUNT,
		BUF_STAT_SWAP_READ_SIZE,
		BUF_STAT_SWAP_READ_TIME,
		BUF_STAT_SWAP_WRITE_COUNT,
		BUF_STAT_SWAP_WRITE_SIZE,
		BUF_STAT_SWAP_WRITE_TIME,
		BUF_STAT_SWAP_WRITE_COMPRESS_TIME,
		BUF_STAT_CP_WRITE_COUNT,
		BUF_STAT_CP_WRITE_SIZE,
		BUF_STAT_CP_WRITE_TIME,
		BUF_STAT_CP_WRITE_COMPRESS_TIME,
		BUF_STAT_HIT_COUNT,
		BUF_STAT_MISS_HIT_COUNT,
		BUF_STAT_ALLOCATE_COUNT, 
		BUF_STAT_ACTIVE_COUNT, 
		BUF_STAT_SWAP_NORMAL_READ_COUNT,
		BUF_STAT_SWAP_COLD_READ_COUNT,
		BUF_STAT_PUSH_HOT_COUNT, 
		BUF_STAT_PUSH_COLD_COUNT, 
		BUF_STAT_USE_BUFFER_COUNT,
		BUF_STAT_UNCOMPRESS_TIME,

		BUF_STAT_USE_STORE_SIZE,
		BUF_STAT_USE_BUFFER_SIZE,
		BUF_STAT_BUFFER_LIMIT_SIZE,

		BUF_STAT_END
	};

	typedef TimeStatTable<Param, BUF_STAT_END> TimeTable;
	typedef TimeTable::BaseTable Table;
	typedef Table::Mapper Mapper;

	explicit ChunkBufferStats(const Mapper *mapper);

	static void setUpMapper(Mapper &mapper);
	static void setUpBasicMapper(Mapper &mapper, bool detail);
	static void setUpCPMapper(Mapper &mapper, bool detail);

	Table table_;
	TimeTable timeTable_;
};

/*!
	@brief 圧縮処理用パラメータ
*/
struct CompressorParam {
	static const int32_t DEFAULT_MIN_SPACE_SAVINGS = 10; 
	static const int32_t DEFAULT_ZSTD_COMPRESSION_LEVEL = 1; 

	ChunkCompressionTypes::Mode mode_; 

	int32_t fileSystemBlockSize_; 
	int32_t minSpaceSavings_; 

	CompressorParam();
};


class LRUTable;
class ChunkCompressorBase;
class ChunkCopyContext;
class PartitionList;
/*!
	@brief チャンクバッファの基本実装
		LRUアルゴリズム、スレッド安全性は選択可能
*/
template<class L>
class BasicChunkBuffer final : public Interchangeable {
	friend class LRUTable;
private:
	static const int64_t DISABLE_COLD = INT64_MAX;

	L* locker_;
	ChunkCompressorBase *chunkCompressor_;
	std::unique_ptr<ChunkCompressorBase> zlibCompressor_;
	std::unique_ptr<ChunkCompressorBase> zstdCompressor_;
	PartitionList* partitionList_;
	PartitionGroupId pgId_;
	PartitionId beginPId_;
	PartitionId endPId_;
	uint64_t timeCount_;

protected:
	static const uint32_t MAX_ONCE_SWAP_SIZE_BYTE_ = 64 * 1024 * 1024;  

	std::vector<VirtualFileBase*> dataFileList_;
	Chunk::ChunkMemoryPool* pool_;
	int32_t chunkSize_;
	uint64_t storeMemoryLimitBlockCount_;
	CompressorParam compressorParam_;

	LRUTable* bufferTable_;
	BufferHashMap bufferMap_;

	double storeMemoryAgingSwapRate_;  
	int64_t storeMemoryAgingSwapCount_;
	int64_t agingSwapCounter_;
	size_t chunkCategoryNum_;
	const uint32_t maxOnceSwapNum_;

	ChunkBufferStats &stats_;

	util::Mutex compressorMutex_;
	util::Mutex uncompressorMutex_;

public:
	BasicChunkBuffer(
			L *locker, PartitionGroupId pgId, int32_t chunkSize,
			int32_t numpages, const CompressorParam &compressorParam,
			PartitionId beginPartitionId, PartitionId endPartitionId,
			size_t bufferHashMapSize, size_t chunkCategoryNum,
			ChunkBufferStats &stats);

	virtual ~BasicChunkBuffer();

	BasicChunkBuffer& setCompression(
			ChunkCompressionTypes::Mode mode, size_t skipSize,
			size_t compressedSizeLimit);

	class ChunkAccessor;

	void flush(PartitionId pId);
	util::Mutex& compressorMutex() { return compressorMutex_; }
	util::Mutex& uncompressorMutex() { return uncompressorMutex_; }

	void fin();

	double getActivity() const;

	void updateProfileCounter();

	bool resize(int32_t capacity);

	int32_t getCurrentLimit();

	void getFileStatus(PartitionId pId, uint64_t &size, uint64_t& allocatedSize) const;

	void setMemoryPool(Chunk::ChunkMemoryPool* pool);

	void setPartitionList(PartitionList* ptList);

	void validatePinCount();

	uint64_t getTotalPinCount();

	void setSwapOutCounter(int64_t counter);

	int64_t getSwapOutCounter();

	void setStoreMemoryAgingSwapRate(double rate, int64_t totalLimitNum, int32_t partitionGroupNum);
	double getStoreMemoryAgingSwapRate();
	void updateStoreMemoryAgingParams(int64_t totalLimitNum, int32_t partitionGroupNum);
	void rebalance();

	void setStoreMemoryLimitBlockCount(uint64_t limit);
	uint64_t getStoreMemoryLimitBlockCount();

	std::string toString() const;

	std::string getProfile() const;

	class Job;

	PartitionGroupId getPgId() { return pgId_; }

	PartitionList* partitionList() { return partitionList_; }

	void setDataFile(PartitionId pId, VirtualFileBase& dataFile) {
		assert(beginPId_ <= pId && pId < endPId_);
		dataFileList_[pId - beginPId_] = &dataFile;
	}
	VirtualFileBase& getDataFile(PartitionId pId) {
		assert(beginPId_ <= pId && pId < endPId_);
		return *dataFileList_[pId - beginPId_];
	};
};

/*!
	@brief チャンクバッファ用チャンクアクセッサ
*/
template<class L>
class BasicChunkBuffer<L>::ChunkAccessor {
private:
	BasicChunkBuffer& bcBuffer_;
	VirtualFileBase* outFile_;

protected:
	ChunkCompressorBase* chunkCompressor_;

public:
	ChunkAccessor(BasicChunkBuffer& bcBuffer);
	virtual ~ChunkAccessor() {};

	void pinChunk(
			PartitionId pId, int64_t offset, bool dirty, bool alloc,
			DSGroupId groupId, LRUFrame *&fr, int32_t &pos);

	bool isDirty(LRUFrame *fr);

	void setDirty(LRUFrame *fr);

	void unpinChunk(LRUFrame *fr, int32_t pos, bool dirty);

	void purgeChunk(PartitionId pId, int64_t offset, bool lost);

	void flushChunk(PartitionId pId, int64_t offset);

	void dumpChunk(PartitionId pId, int64_t offset);

	void flush(PartitionId pId);

	void swapOffset(PartitionId pId, int64_t offset1, int64_t offset2);

	int64_t getPinCount(PartitionId pId, int64_t offset);

	int64_t copyChunk(ChunkCopyContext* cxt, PartitionId pId, int64_t offset, bool keepSrcOffset);

	void restoreChunk(
		PartitionId pId, int64_t offset, uint8_t* buff, size_t buffSize);

	void punchHoleChunk(PartitionId pId, int64_t offset);

	void setOutFile(VirtualFileBase* outFile);

	void seekAndRead(
			PartitionId pId, int64_t offset, uint8_t* buff, size_t buffSize,
			StatStopwatch &ioWatch);
	void seekAndWrite(
			PartitionId pId, int64_t offset, uint8_t* buff, size_t buffSize,
			StatStopwatch &ioWatch, StatStopwatch &compressWatch);

	void tmpFileRead(
			ChunkCopyContext* cxt, int64_t offset, uint8_t* buff, size_t buffSize);
	void tmpFileWrite(
			ChunkCopyContext* cxt, int64_t offset, uint8_t* buff, size_t buffSize);

	void adjustStoreMemory(bool doReserve);

private:
	void pinChunkMissHit(
			PartitionId pId, int64_t offset, bool dirty, bool alloc,
			DSGroupId groupId, LRUFrame *&fr, int32_t &pos);
	int32_t swapOut(int32_t limit, int32_t &pos);
};


/*!
	@brief スレッドアンセーフなチャンクバッファ

		パーティション混在型バッファではパーティションIDとオフセットとの組み合わせで管理する
*/
class ChunkBuffer final : public Interchangeable {
private:
	typedef BasicChunkBuffer<NoLocker> NoLockBasicChunkBuffer;

	static const uint64_t MINIMUM_BUFFER_INFO_LIST_SIZE =
			1ULL << 21;  

	NoLockBasicChunkBuffer* basicChunkBuffer_;
	NoLockBasicChunkBuffer::ChunkAccessor* chunkAccessor_;
	PartitionGroupId pgId_;
	PartitionId beginPId_;
	PartitionId endPId_;
	Chunk::ChunkMemoryPool* pool_;
	PartitionList* partitionList_;
	util::Mutex mutex_;

public:
	ChunkBuffer(
			PartitionGroupId pgId, int32_t chunkSize, int32_t numpages,
			const CompressorParam &compressorParam,
			PartitionId beginPId, PartitionId endPId,
			size_t bufferHashMapSize, size_t chunkCategoryNum,
			ChunkBufferStats &stats);

	virtual ~ChunkBuffer();

	void pinChunk(
			PartitionId pId, int64_t offset, bool dirty, bool alloc,
			DSGroupId groupId, ChunkBufferFrameRef &frameRef);

	bool isDirty(const ChunkBufferFrameRef &frameRef);

	void setDirty(const ChunkBufferFrameRef &frameRef);

	void flushChunk(PartitionId pId, int64_t offset);

	void dumpChunk(PartitionId pId, int64_t offset);

	void purgeChunk(PartitionId pId, int64_t offset, bool lost);

	void unpinChunk(const ChunkBufferFrameRef &frameRef, bool dirty);

	void swapOffset(PartitionId pId, int64_t offset1, int64_t offset2);

	int64_t getPinCount(PartitionId pId, int64_t offset);

	int64_t copyChunk(ChunkCopyContext* cxt, PartitionId pId, int64_t offset, bool keepSrcOffset);

	void restoreChunk(PartitionId pId, int64_t offset, uint8_t* buff, size_t buffSize);

	void punchHoleChunk(PartitionId pId, int64_t offset);

	double getActivity() const;

	void updateProfileCounter();

	bool resize(int32_t capacity);

	int32_t getCurrentLimit();

	void validatePinCount();

	uint64_t getTotalPinCount();

	PartitionGroupId getPgId() {
		return pgId_;
	}

	PartitionList* partitionList() { return partitionList_; }

	void setDataFile(PartitionId pId, VirtualFileBase& dataFile) {
		assert(beginPId_ <= pId && pId < endPId_);
		basicChunkBuffer_->setDataFile(pId, dataFile);
	}
	VirtualFileBase& getDataFile(PartitionId pId) {
		assert(beginPId_ <= pId && pId < endPId_);
		return basicChunkBuffer_->getDataFile(pId);
	};

	void setMemoryPool(Chunk::ChunkMemoryPool* pool);
	Chunk::ChunkMemoryPool* getMemoryPool() { return pool_; }
	void setVirtualFile(PartitionId pId, VirtualFileBase &file);
	void setPartitionList(PartitionList* ptList);

	void setSwapOutCounter(int64_t counter);

	int64_t getSwapOutCounter();

	void setStoreMemoryAgingSwapRate(
			double rate, int64_t totalLimitNum, int32_t partitionGroupNum);
	double getStoreMemoryAgingSwapRate();
	void updateStoreMemoryAgingParams(
			int64_t totalLimitNum, int32_t partitionGroupNum);
	void rebalance();

	void setStoreMemoryLimitBlockCount(uint64_t limit);

	static uint64_t calcHashTableSize(
			PartitionGroupId partitionGroupNum,
			uint64_t totalAtomicMemoryLimitNum,
			uint64_t chunkExpSize ,double hashTableSizeRate);

	util::Mutex& mutex() { return mutex_; }

	static Chunk* getPinnedChunk(const ChunkBufferFrameRef &frameRef);

private:
	static uint64_t calcHashTableSize1(
			PartitionGroupId partitionGroupNum,
			uint64_t totalAtomicMemoryLimitNum);

	static uint64_t calcHashTableSize2(
			PartitionGroupId partitionGroupNum,
			uint64_t totalAtomicMemoryLimitNum,
			uint64_t chunkExpSize ,double hashTableSizeRate);

	NoLockBasicChunkBuffer& bcBuffer() { return *basicChunkBuffer_; }
};


/*!
	@brief LRUテーブル要素
*/
struct LRUFrame {
	int64_t key_;
	void* value_;
	bool isdirty_;
	bool startHot_;
	bool inHot_;
	int64_t pincount_;
	int32_t next_;
	int32_t prev_;
	PartitionId pId_;
	int64_t groupId_; 
	uint64_t timeCount_;

	LRUFrame()
		: key_(-1), value_(NULL),
		  isdirty_(false), startHot_(true),
		  inHot_(true), pincount_(-1), next_(-1), prev_(-1),
		  pId_(-1), groupId_(-1), timeCount_(0) {};
};

/*!
	@brief LRUテーブル

		L0_HOT head.............tail
			□-□-□-□-□-□-□-□-□
		L0_COLD head.............tail
			□-□-□-□-□-□-□-□-□
		L1 free
			□-□
		L2 stock
			□-□
*/
class LRUTable {
private:
	int32_t maxNumElems_;
	std::vector<LRUFrame> table_;	
	int32_t head1_; 
	int32_t tail1_; 
	int32_t head2_; 
	int32_t tail2_; 
	int32_t free_;  

	int32_t currentNumElems_; 
	int32_t hotNumElems_; 
	int32_t coldNumElems_; 
	int32_t stock_;  
	double hotRate_;

public:
	class Cursor;

	LRUTable(int32_t maxNumElems);
	~LRUTable();

	void pushTail(int32_t pos);
	bool pushMiddle(int32_t pos); 

	int32_t peek(bool& inCold);
	int32_t peekHot();

	bool touch(int32_t pos, bool toTail = true); 

	int32_t getFree();
	void pushFree(int32_t pos); 
	int32_t getStock();

	void gotoL1FromL0(int32_t pos);

	void gotoL2FromL0(Chunk::ChunkMemoryPool* pool, int32_t pos);

	bool gotoL2FromL1(Chunk::ChunkMemoryPool* pool);

	bool gotoL1FromL2();

	int32_t getCurrentNumElems();

	int32_t getHotNumElems();

	int32_t getColdNumElems();

	LRUFrame* get(int32_t pos);

	int32_t getSize() const;

	void setHotRate(double rate);
	double getHotRate() const;

	std::string toString();
	std::string toString(BasicChunkBuffer<NoLocker> &bcBuffer);

	void releaseAll(Chunk::ChunkMemoryPool* pool);

	void rebalance();

	bool checkL0(int32_t target);

	void expand();

	Cursor createCursor();

	int32_t shiftHotToCold(int32_t count);	
	int32_t shiftColdToHot(int32_t count);	

private:
	void init(int32_t pos);

	int32_t popHead();

	int32_t popTail();

	void pushHead(int32_t pos);

	void pushHotHead(int32_t pos);
	int32_t popHotHead();
	void pushHotTail(int32_t pos);
	int32_t popHotTail();

	void pushColdHead(int32_t pos);
	int32_t popColdHead();
	void pushColdTail(int32_t pos);
	int32_t popColdTail();

	void removeChain(int32_t pos);

	bool validateLink();
};

class LRUTable::Cursor {
private:
	LRUTable& table_;
	int32_t count_;
	int32_t pos_;
	bool inCold_;

public:
	Cursor(LRUTable& table);
	~Cursor() {};

	LRUFrame* next(int32_t& pos);

	int32_t getCount() { return count_; }
};

inline LRUTable::Cursor LRUTable::createCursor() {
	return Cursor(*this);
}

inline LRUTable::Cursor::Cursor(LRUTable& table)
: table_(table), count_(0), pos_(-1), inCold_(false) {
}

inline LRUFrame* LRUTable::Cursor::next(int32_t &pos) {
	if (count_ == 0) {
		pos_ = table_.peek(inCold_);
	}
	if (pos_ < 0) {
		pos = pos_;
		return NULL;
	}
	LRUFrame *fr = table_.get(pos_);
	assert(fr);
	int32_t next = fr->next_;
	if (next < 0 && inCold_) {
		next = table_.peekHot();
		inCold_ = false;
	}
	pos = pos_;
	pos_ = next;
	++count_;
	return fr;
}


inline void BufferHashMap::insert(
		uint32_t pId, uint64_t bufferId, int32_t pos) {

	const BufferMapKey key(
			static_cast<int32_t>(pId), static_cast<int32_t>(bufferId));
#ifdef NDEBUG
	map_.emplace(key, pos);
#else
	bool result = map_.emplace(key, pos).second;
	assert(result); 
#endif
};


inline int32_t BufferHashMap::find(uint32_t pId, uint64_t bufferId) const {
	BufferMap::const_iterator itr = map_.find(BufferMapKey(
			static_cast<int32_t>(pId), static_cast<int32_t>(bufferId)));
	if (itr != map_.end()) {
		return static_cast<int32_t>(itr->second);
	}
	else {
		return -1;
	}
};

inline bool BufferHashMap::remove(uint32_t pId, uint64_t bufferId) {
	BufferMap::iterator itr = map_.find(BufferMapKey(
			static_cast<int32_t>(pId), static_cast<int32_t>(bufferId)));
	if (itr != map_.end()) {
		map_.erase(itr);
		return true;
	}
	else {
		return false;
	}
};

inline bool BufferHashMap::swapPosition(
		uint32_t pId, uint64_t bufferId1, uint64_t bufferId2,
		int32_t &pos1, int32_t &pos2) {
	BufferMap::iterator itr1 = map_.find(BufferMapKey(
			static_cast<int32_t>(pId), static_cast<int32_t>(bufferId1)));
	BufferMap::iterator itr2 = map_.find(BufferMapKey(
			static_cast<int32_t>(pId), static_cast<int32_t>(bufferId2)));
	if (itr1 == map_.end() || itr2 == map_.end()) {
		return false;
	}
	pos1 = static_cast<int32_t>(itr1->second);
	pos2 = static_cast<int32_t>(itr2->second);
	std::swap(itr1->second, itr2->second);
	return true;
}


class ChunkCopyContext {
public:
	uint64_t ssn_;
	VirtualFileBase* datafile_;
	LogManager<MutexLocker>* logManager_;
	Chunk* chunk_;
	uint8_t* tmpBuffer_;
	uint64_t offset_;

	ChunkCopyContext() : ssn_(0), datafile_(NULL), logManager_(NULL),
	chunk_(NULL), tmpBuffer_(NULL), offset_(0) {};
};

inline void ChunkBuffer::pinChunk(
		PartitionId pId, int64_t offset, bool dirty, bool alloc,
		DSGroupId groupId, ChunkBufferFrameRef &frameRef) {
	LRUFrame *frame;
	int32_t pos;
	chunkAccessor_->pinChunk(pId, offset, dirty, alloc, groupId, frame, pos);
	frameRef = ChunkBufferFrameRef(frame, pos);
}

inline bool ChunkBuffer::isDirty(const ChunkBufferFrameRef &frameRef) {
	return chunkAccessor_->isDirty(frameRef.get());
}

inline void ChunkBuffer::setDirty(const ChunkBufferFrameRef &frameRef) {
	chunkAccessor_->setDirty(frameRef.get());
}

inline int64_t ChunkBuffer::copyChunk(
		ChunkCopyContext* cxt, PartitionId pId, int64_t offset, bool keepSrcOffset) {
	return chunkAccessor_->copyChunk(cxt, pId, offset, keepSrcOffset);
}

inline void ChunkBuffer::restoreChunk(
		PartitionId pId, int64_t offset, uint8_t* buff, size_t buffSize) {
	chunkAccessor_->restoreChunk(pId, offset, buff, buffSize);
}

inline void ChunkBuffer::punchHoleChunk(PartitionId pId, int64_t offset) {
	chunkAccessor_->punchHoleChunk(pId, offset);
}

inline void ChunkBuffer::flushChunk(PartitionId pId, int64_t offset) {
	chunkAccessor_->flushChunk(pId, offset);
}

inline void ChunkBuffer::dumpChunk(PartitionId pId, int64_t offset) {
	chunkAccessor_->dumpChunk(pId, offset);
}

inline void ChunkBuffer::purgeChunk(PartitionId pId, int64_t offset, bool lost) {
	chunkAccessor_->purgeChunk(pId, offset, lost);
}

inline void ChunkBuffer::unpinChunk(
		const ChunkBufferFrameRef &frameRef, bool dirty) {
	chunkAccessor_->unpinChunk(frameRef.get(), frameRef.getPosition(), dirty);
}

inline void ChunkBuffer::swapOffset(
		PartitionId pId, int64_t offset1, int64_t offset2) {
	chunkAccessor_->swapOffset(pId, offset1, offset2);
}

inline int64_t ChunkBuffer::getPinCount(PartitionId pId, int64_t offset) {
	return chunkAccessor_->getPinCount(pId, offset);
}

inline double ChunkBuffer::getActivity() const {
	return basicChunkBuffer_->getActivity();
}

inline void ChunkBuffer::updateProfileCounter() {
	return basicChunkBuffer_->updateProfileCounter();
}

inline bool ChunkBuffer::resize(int32_t capacity) {
	return basicChunkBuffer_->resize(capacity);
}

inline int32_t ChunkBuffer::getCurrentLimit() {
	return basicChunkBuffer_->getCurrentLimit();
}

inline void ChunkBuffer::validatePinCount() {
	basicChunkBuffer_->validatePinCount();
}

inline uint64_t ChunkBuffer::getTotalPinCount() {
	return basicChunkBuffer_->getTotalPinCount();
}

inline void ChunkBuffer::setSwapOutCounter(int64_t counter) {
	basicChunkBuffer_->setSwapOutCounter(counter);
}

inline int64_t ChunkBuffer::getSwapOutCounter() {
	return basicChunkBuffer_->getSwapOutCounter();
}

inline void ChunkBuffer::setStoreMemoryAgingSwapRate(double rate, int64_t totalLimitNum, int32_t partitionGroupNum) {
	basicChunkBuffer_->setStoreMemoryAgingSwapRate(rate, totalLimitNum, partitionGroupNum);
}
inline double ChunkBuffer::getStoreMemoryAgingSwapRate() {
	return basicChunkBuffer_->getStoreMemoryAgingSwapRate();
}
inline void ChunkBuffer::updateStoreMemoryAgingParams(
		int64_t totalLimitNum, int32_t partitionGroupNum) {
	basicChunkBuffer_->updateStoreMemoryAgingParams(
			totalLimitNum, partitionGroupNum);
}
inline void ChunkBuffer::rebalance() {
	return basicChunkBuffer_->rebalance();
}

inline void ChunkBuffer::setStoreMemoryLimitBlockCount(uint64_t count) {
	basicChunkBuffer_->setStoreMemoryLimitBlockCount(count);
}

inline Chunk* ChunkBuffer::getPinnedChunk(
		const ChunkBufferFrameRef &frameRef) {
	LRUFrame *frame = frameRef.get();
	assert(frame != NULL);
	assert(frame->pincount_ > 0);
	assert(frame->value_ != NULL);
	return static_cast<Chunk*>(frame->value_);
}

template<class L>
inline void BasicChunkBuffer<L>::flush(PartitionId pId) {
	ChunkAccessor accessor(*this);
	accessor.flush(pId);
}

template<class L>
inline double BasicChunkBuffer<L>::getActivity() const {
	return static_cast<double>(
			stats_.table_(ChunkBufferStats::BUF_STAT_MISS_HIT_COUNT).get());
}

template<class L>
void BasicChunkBuffer<L>::updateProfileCounter() {
	timeCount_++;
}

template<class L>
inline void BasicChunkBuffer<L>::rebalance() {
	bufferTable_->rebalance();
}

template<class L>
inline void BasicChunkBuffer<L>::setStoreMemoryLimitBlockCount(uint64_t count) {
	storeMemoryLimitBlockCount_ = count;
}

template<class L>
inline uint64_t BasicChunkBuffer<L>::getStoreMemoryLimitBlockCount() {
	return storeMemoryLimitBlockCount_;
}

template<class L>
inline bool BasicChunkBuffer<L>::ChunkAccessor::isDirty(LRUFrame *fr) {
	assert(fr);
	return fr->isdirty_;
}

template<class L>
inline void BasicChunkBuffer<L>::ChunkAccessor::setDirty(LRUFrame *fr) {
	assert(fr);
	fr->isdirty_ = true;
}

template<class L>
inline void BasicChunkBuffer<L>::ChunkAccessor::pinChunk(
		PartitionId pId, int64_t offset, bool dirty, bool alloc,
		DSGroupId groupId, LRUFrame *&fr, int32_t &pos) {
	assert(bcBuffer_.beginPId_ <= pId && pId < bcBuffer_.endPId_);

	pos = bcBuffer_.bufferMap_.find(pId, offset);
	if (pos != -1) {
		fr = bcBuffer_.bufferTable_->get(pos);
		assert(fr);
		assert(fr->value_);
		assert(fr->pId_ == pId);
		assert(fr->groupId_ == groupId);

		ChunkBufferStats &stats = bcBuffer_.stats_;
		if (fr->pincount_ == 0 && fr->timeCount_ != bcBuffer_.timeCount_) {
			stats.table_(ChunkBufferStats::BUF_STAT_HIT_COUNT).increment();
			fr->timeCount_ = bcBuffer_.timeCount_;
		}
		++fr->pincount_; 
		assert(fr->pincount_ > 0);
		if (dirty) {
			fr->isdirty_ = true;
		}
		return;
	} else {
		pinChunkMissHit(pId, offset, dirty, alloc, groupId, fr, pos);
	}
}

template<class L>
inline void BasicChunkBuffer<L>::ChunkAccessor::unpinChunk(
		LRUFrame *fr, int32_t pos, bool dirty) {
	if (dirty) {
		fr->isdirty_ = true;
	}
	assert(fr->pincount_ > 0);
	if (fr->pincount_ > 0) {
		--fr->pincount_;
		if (fr->pincount_ == 0) {
			const bool toHotTail = (bcBuffer_.agingSwapCounter_ < bcBuffer_.storeMemoryAgingSwapCount_) ? true : fr->startHot_;
			bool toHot = bcBuffer_.bufferTable_->touch(pos, toHotTail);
			ChunkBufferStats &stats = bcBuffer_.stats_;
			stats.table_(toHot ?
					ChunkBufferStats::BUF_STAT_PUSH_HOT_COUNT :
					ChunkBufferStats::BUF_STAT_PUSH_COLD_COUNT).increment();
		}
	}
}

template<class L>
inline void BasicChunkBuffer<L>::ChunkAccessor::setOutFile(VirtualFileBase* outFile) {
	outFile_ = outFile;
}

template<class L>
inline void BasicChunkBuffer<L>::ChunkAccessor::swapOffset(
		PartitionId pId, int64_t offset1, int64_t offset2) {
	int32_t pos1;
	int32_t pos2;
	if (!bcBuffer_.bufferMap_.swapPosition(pId, offset1, offset2, pos1, pos2)) {
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "swapOffset");
	}

	LRUFrame *fr1 = bcBuffer_.bufferTable_->get(pos1);
	LRUFrame *fr2 = bcBuffer_.bufferTable_->get(pos2);
	assert(fr1 != NULL && fr1->pincount_ > 0 && fr1->value_ != NULL);
	assert(fr2 != NULL && fr2->pincount_ > 0 && fr2->value_ != NULL);

	assert(fr1->pId_ == fr2->pId_);
	assert(fr1->groupId_ == fr2->groupId_);

	bool &dirty1 = fr1->isdirty_;
	bool &dirty2 = fr2->isdirty_;
	std::swap(dirty1, dirty2);

	int64_t &frOffset1 = fr1->key_;
	int64_t &frOffset2 = fr2->key_;
	std::swap(frOffset1, frOffset2);
}

template<class L>
inline int64_t BasicChunkBuffer<L>::ChunkAccessor::getPinCount(PartitionId pId, int64_t offset) {
	assert(bcBuffer_.beginPId_ <= pId && pId < bcBuffer_.endPId_);
	int64_t pinCount = 0;
	int32_t pos = bcBuffer_.bufferMap_.find(pId, offset);
	if (pos != -1) {
		LRUFrame* fr = bcBuffer_.bufferTable_->get(pos);
		if (fr) {
			pinCount = fr->pincount_;
		}
	}
	return pinCount;
}


inline void LRUTable::init(int32_t pos) {
	table_[pos].prev_ = -1;
	table_[pos].next_ = -1;
	table_[pos].key_ = -1;
	table_[pos].isdirty_ = false;
	table_[pos].pincount_ = 0;
	table_[pos].startHot_ = true;
	table_[pos].inHot_ = true;
}

inline int32_t LRUTable::popHead() {
	if (head2_ < 0 && head1_ < 0) {
		assert(false);
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "No more popHead");
	}
	int32_t pos = popColdHead();
	if (pos == -1) {
		pos = popHotHead();
	}
	return pos;
}

inline int32_t LRUTable::popTail() {
	if (tail1_ < 0 && tail2_ < 0) {
		assert(false);
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "No more popTail");
	}
	int32_t pos = popHotTail();
	if (pos < 0) {
		pos = popColdTail();
	}
	return pos;
}

inline void LRUTable::pushHead(int32_t pos) {
	assert(pos >= 0);
	if (head1_ < 0) {
		pushHotHead(pos);
	} else {
		pushColdHead(pos);
	}
}

inline void LRUTable::pushTail(int32_t pos) {
	pushHotTail(pos);
}

inline bool LRUTable::pushMiddle(int32_t pos) {
	bool toHot = false;
	int32_t targetHotNumElems = static_cast<int32_t>(
			static_cast<double>(currentNumElems_) * hotRate_ + 0.5);
	if (hotNumElems_ < targetHotNumElems) {
		pushHotHead(pos);
		toHot = true;;
	} else {
		pushColdTail(pos);
		toHot = false;
	}
	return toHot;
}

inline void LRUTable::removeChain(int32_t pos) {
	if (pos < 0) return;
	if (pos == head2_) {
		popColdHead();
	} else if (pos == tail1_) {
		popHotTail();
	} else if (pos == head1_) {
		popHotHead();
	} else if (pos == tail2_) {
		popColdTail();
	} else {
		if (table_[pos].prev_ < 0 || table_[pos].next_ < 0) {
			assert(false);
			GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "no removable");
		}
		if (table_[pos].inHot_) {
			assert(hotNumElems_ > 0);
			--hotNumElems_;
		} else {
			assert(coldNumElems_ > 0);
			--coldNumElems_;
		}
		table_[table_[pos].prev_].next_ = table_[pos].next_;
		table_[table_[pos].next_].prev_ = table_[pos].prev_;
	}
}

inline int32_t LRUTable::peek(bool &inCold) {
	if (head2_ < 0) {
		inCold = false;
		return head1_;
	}
	else {
		inCold = true;
		return head2_;
	}
}

inline int32_t LRUTable::peekHot() {
	return head1_;
}

inline bool LRUTable::touch(int32_t pos, bool toHotTail) {
	bool toHot = toHotTail;
	removeChain(pos);
	if (toHotTail) {
		pushHotTail(pos);
	}
	else {
		toHot = pushMiddle(pos);
	}
	return toHot;
}


inline void LRUTable::pushHotHead(int32_t pos) {
	assert(pos >= 0);
	if (head1_ < 0) {
		table_[pos].next_ = -1;
		table_[pos].prev_ = -1;
		head1_ = pos;
		tail1_ = pos;
	} else {
		table_[head1_].prev_ = pos;
		table_[pos].next_ = head1_;
		table_[pos].prev_ = -1;
		head1_ = pos;
	}
	table_[pos].inHot_ = true;
	++hotNumElems_;
}
inline int32_t LRUTable::popHotHead() {
	if (head1_ < 0) {
		return -1;
	} else {
		int32_t pos = head1_;
		head1_ = table_[pos].next_;
		if (head1_ < 0) {
			tail1_ = -1;
		} else {
			table_[head1_].prev_ = -1;
		}
		assert(hotNumElems_ > 0);
		--hotNumElems_;
		return pos;
	}
}

inline void LRUTable::pushHotTail(int32_t pos) {
	assert(pos >= 0);
	if (tail1_ < 0) {
		table_[pos].next_ = -1;
		table_[pos].prev_ = -1;
		head1_ = pos;
		tail1_ = pos;
	} else {
		table_[tail1_].next_ = pos;
		table_[pos].next_ = -1;
		table_[pos].prev_ = tail1_;
		tail1_ = pos;
	}
	table_[pos].inHot_ = true;
	++hotNumElems_;
}

inline int32_t LRUTable::popHotTail() {
	if (tail1_ < 0) {
		return -1;
	} else {
		int32_t pos = tail1_;
		tail1_ = table_[pos].prev_;
		if (tail1_ < 0) {
			head1_ = -1;
		} else {
			table_[tail1_].next_ = -1;
		}
		assert(hotNumElems_ > 0);
		--hotNumElems_;
		return pos;
	}
}

inline void LRUTable::pushColdHead(int32_t pos) {
	assert(pos >= 0);
	if (head2_ < 0) {
		table_[pos].next_ = -1;
		table_[pos].prev_ = -1;
		head2_ = pos;
		tail2_ = pos;
	} else {
		table_[head2_].prev_ = pos;
		table_[pos].next_ = head2_;
		table_[pos].prev_ = -1;
		head2_ = pos;
	}
	table_[pos].inHot_ = false;
	++coldNumElems_;
}
inline int32_t LRUTable::popColdHead() {
	if (head2_ < 0) {
		return -1;
	} else {
		int32_t pos = head2_;
		head2_ = table_[pos].next_;
		if (head2_ < 0) {
			tail2_ = -1;
		} else {
			table_[head2_].prev_ = -1;
		}
		assert(coldNumElems_ > 0);
		--coldNumElems_;
		return pos;
	}
}

inline void LRUTable::pushColdTail(int32_t pos) {
	assert(pos >= 0);
	if (tail2_ < 0) {
		table_[pos].next_ = -1;
		table_[pos].prev_ = -1;
		head2_ = pos;
		tail2_ = pos;
	} else {
		table_[tail2_].next_ = pos;
		table_[pos].next_ = -1;
		table_[pos].prev_ = tail2_;
		tail2_ = pos;
	}
	table_[pos].inHot_ = false;
	++coldNumElems_;
}

inline int32_t LRUTable::popColdTail() {
	if (tail2_ < 0) {
		return -1;
	} else {
		int32_t pos = tail2_;
		tail2_ = table_[pos].prev_;
		if (tail2_ < 0) {
			head2_ = -1;
		} else {
			table_[tail2_].next_ = -1;
		}
		assert(coldNumElems_ > 0);
		--coldNumElems_;
		return pos;
	}
}

inline void LRUTable::rebalance() {
	int32_t hotCount = static_cast<int32_t>(
			static_cast<double>(currentNumElems_) * hotRate_ + 0.5);
	if (hotCount < hotNumElems_) {
		shiftHotToCold(hotNumElems_ - hotCount);
	} else if (hotCount > hotNumElems_) {
		shiftColdToHot(hotCount - hotNumElems_);
	}
}


inline int32_t LRUTable::getFree() {
	if (free_ >= 0) {
		int32_t pos = free_;
		free_ = table_[pos].next_;
		return pos;
	}
	return -1;
}

inline void LRUTable::pushFree(int32_t pos) {
	init(pos);
	table_[pos].next_ = free_;
	free_ = pos;
}

inline int32_t LRUTable::getStock() {
	if (stock_ >= 0) {
		int32_t pos = stock_;
		stock_ = table_[pos].next_;
		return pos;
	}
	return -1;
}

inline void LRUTable::gotoL1FromL0(int32_t pos) {
	removeChain(pos);
	init(pos);
	table_[pos].next_ = free_;
	free_ = pos;
}

inline void LRUTable::gotoL2FromL0(Chunk::ChunkMemoryPool* pool, int32_t pos) {
	removeChain(pos);
	init(pos);
	if (pool && table_[pos].value_) {
		Chunk* chunk = static_cast<Chunk*>(table_[pos].value_);
		if (chunk->getData()) {
			pool->deallocate(chunk->getData());
			chunk->reset();
		}
		delete chunk;
		table_[pos].value_ = NULL;
	}
	table_[pos].next_ = stock_;
	stock_ = pos;
	assert(currentNumElems_ > 0);
	currentNumElems_--;
}

inline bool LRUTable::gotoL2FromL1(Chunk::ChunkMemoryPool* pool) {
	int32_t pos = free_;
	if (pos >= 0) {
		free_ = table_[pos].next_;
		if (pool && table_[pos].value_) {
			Chunk* chunk = static_cast<Chunk*>(table_[pos].value_);
			if (chunk->getData()) {
				pool->deallocate(chunk->getData());
				chunk->reset();
			}
			delete chunk;
			table_[pos].value_ = NULL;
		}
		init(pos);
		table_[pos].next_ = stock_;
		stock_ = pos;
		assert(currentNumElems_ > 0);
		currentNumElems_--;
		return true;
	} else {
		return false;
	}
}

inline bool LRUTable::gotoL1FromL2() {
	int32_t pos = stock_;
	if (pos >= 0) {
		stock_ = table_[pos].next_;

		init(pos);
		table_[pos].next_ = free_;
		free_ = pos;
		currentNumElems_++;
		return true;
	} else {
		return false;
	}
}

inline int32_t LRUTable::getCurrentNumElems() {
	return currentNumElems_;
}

inline int32_t LRUTable::getHotNumElems() {
	return hotNumElems_;
}

inline int32_t LRUTable::getColdNumElems() {
	return coldNumElems_;
}

inline int32_t LRUTable::getSize() const {
	return static_cast<int32_t>(table_.size());
}

inline LRUFrame* LRUTable::get(int32_t pos) {
	return &table_[pos];
}

inline double LRUTable::getHotRate() const {
	return hotRate_;
}
inline void LRUTable::setHotRate(double rate) {
	hotRate_ = rate;
}


#endif 
