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
	@brief Implementation of ChunkBuffer
*/
#include "chunk_buffer.h"
#include "partition_file.h"
#include "chunk_manager.h"
#include "partition.h"

UTIL_TRACER_DECLARE(CHUNK_MANAGER);
UTIL_TRACER_DECLARE(SIZE_MONITOR);
UTIL_TRACER_DECLARE(IO_MONITOR);


ChunkBufferStats::ChunkBufferStats(const Table::Mapper *mapper) :
		table_(mapper),
		timeTable_(table_) {
}

void ChunkBufferStats::setUpMapper(Mapper &mapper) {
	for (size_t i = 0; i < 2; i++) {
		Mapper sub(mapper.getSource());
		setUpBasicMapper(sub, (i > 0));
		mapper.addSub(sub);
	}

	for (size_t i = 0; i < 2; i++) {
		Mapper sub(mapper.getSource());
		setUpCPMapper(sub, (i > 0));
		mapper.addSub(sub);
	}
}

void ChunkBufferStats::setUpBasicMapper(Mapper &mapper, bool detail) {
	const ConfigTable::ValueUnit timeUnit = ConfigTable::VALUE_UNIT_DURATION_MS;
	mapper.addFilter(STAT_TABLE_DISPLAY_SELECT_PERF);

	if (detail) {
		mapper.addFilter(STAT_TABLE_DISPLAY_WEB_ONLY);

		mapper.bind(
				BUF_STAT_SWAP_READ_SIZE, STAT_TABLE_PERF_DS_SWAP_READ_SIZE);
		mapper.bind(
				BUF_STAT_SWAP_READ_TIME, STAT_TABLE_PERF_DS_SWAP_READ_TIME)
				.setTimeUnit(timeUnit);
		mapper.bind(
				BUF_STAT_SWAP_WRITE_SIZE, STAT_TABLE_PERF_DS_SWAP_WRITE_SIZE);
		mapper.bind(
				BUF_STAT_SWAP_WRITE_TIME, STAT_TABLE_PERF_DS_SWAP_WRITE_TIME)
				.setTimeUnit(timeUnit);

		mapper.bind(
				BUF_STAT_UNCOMPRESS_TIME,
				STAT_TABLE_PERF_DS_SWAP_READ_UNCOMPRESS_TIME)
				.setTimeUnit(timeUnit);
		mapper.bind(
				BUF_STAT_SWAP_WRITE_COMPRESS_TIME,
				STAT_TABLE_PERF_DS_SWAP_WRITE_COMPRESS_TIME)
				.setTimeUnit(timeUnit);
	}
	else {
		mapper.bind(BUF_STAT_SWAP_READ_COUNT, STAT_TABLE_PERF_DS_SWAP_READ);
		mapper.bind(BUF_STAT_SWAP_WRITE_COUNT, STAT_TABLE_PERF_DS_SWAP_WRITE);
	}
}

void ChunkBufferStats::setUpCPMapper(Mapper &mapper, bool detail) {
	const ConfigTable::ValueUnit timeUnit = ConfigTable::VALUE_UNIT_DURATION_MS;
	mapper.addFilter(STAT_TABLE_DISPLAY_SELECT_CP);

	if (detail) {
		mapper.addFilter(STAT_TABLE_DISPLAY_WEB_ONLY);

		mapper.bind(
				BUF_STAT_CP_WRITE_TIME, STAT_TABLE_PERF_CHECKPOINT_WRITE_TIME)
				.setTimeUnit(timeUnit);
		mapper.bind(
				BUF_STAT_CP_WRITE_COMPRESS_TIME,
				STAT_TABLE_PERF_CHECKPOINT_WRITE_COMPRESS_TIME)
				.setTimeUnit(timeUnit);
	}
	else {
		mapper.bind(BUF_STAT_CP_WRITE_COUNT, STAT_TABLE_PERF_CHECKPOINT_WRITE);
		mapper.bind(
				BUF_STAT_CP_WRITE_SIZE, STAT_TABLE_PERF_CHECKPOINT_WRITE_SIZE);
	}
}


CompressorParam::CompressorParam() :
		mode_(ChunkCompressionTypes::NO_BLOCK_COMPRESSION),
		fileSystemBlockSize_(4096),
		minSpaceSavings_(DEFAULT_MIN_SPACE_SAVINGS) {
}

ChunkBuffer::ChunkBuffer(
		PartitionGroupId pgId, int32_t chunkSize, int32_t numpages,
		const CompressorParam &compressorParam,
		PartitionId beginPId, PartitionId endPId,
		size_t bufferHashMapSize, size_t chunkCategoryNum,
		ChunkBufferStats &stats) :
		basicChunkBuffer_(nullptr), chunkAccessor_(nullptr), pgId_(pgId),
		beginPId_(beginPId), endPId_(endPId), pool_(nullptr), partitionList_(nullptr) {

	basicChunkBuffer_ = UTIL_NEW NoLockBasicChunkBuffer(
			UTIL_NEW NoLocker(), pgId, chunkSize, numpages, compressorParam,
			beginPId, endPId, bufferHashMapSize, chunkCategoryNum, stats);

	chunkAccessor_ = UTIL_NEW NoLockBasicChunkBuffer::ChunkAccessor(*basicChunkBuffer_);
}

ChunkBuffer::~ChunkBuffer() {
	delete chunkAccessor_;
	chunkAccessor_ = NULL;
	delete basicChunkBuffer_;
	basicChunkBuffer_ = NULL;
}

void ChunkBuffer::setMemoryPool(Chunk::ChunkMemoryPool* pool) {
	assert(pool);
	assert(pool_ == nullptr);
	pool_ = pool;
	basicChunkBuffer_->setMemoryPool(pool);
}

void ChunkBuffer::setPartitionList(PartitionList* ptList) {
	partitionList_ = ptList;
	basicChunkBuffer_->setPartitionList(ptList);
}

uint64_t ChunkBuffer::calcHashTableSize(
		PartitionGroupId partitionGroupNum,
		uint64_t totalAtomicMemoryLimitNum,
		uint64_t chunkExpSize ,double hashTableSizeRate) {

	uint64_t hashTableSize1 = calcHashTableSize1(
			partitionGroupNum,
			totalAtomicMemoryLimitNum);

	uint64_t hashTableSize2 = calcHashTableSize2(
			partitionGroupNum,
			totalAtomicMemoryLimitNum,
			chunkExpSize, hashTableSizeRate);

	GS_TRACE_INFO(
			SIZE_MONITOR, GS_TRACE_CHM_CHUNK_HASH_TABLE_SIZE,
			"chunkExpSize," << chunkExpSize <<
			",pgCount," << partitionGroupNum <<
			",storeMemoryLimitCount," << totalAtomicMemoryLimitNum <<
			",rate," << hashTableSizeRate <<
			",oldHashTableSize," << hashTableSize1 <<
			",newHashTableSize," << hashTableSize2 <<
			",max," << ((hashTableSize1 < hashTableSize2) ? hashTableSize2 : hashTableSize1)
	);
	return ((hashTableSize1 < hashTableSize2) ? hashTableSize2 : hashTableSize1);
}

uint64_t ChunkBuffer::calcHashTableSize1(
			PartitionGroupId partitionGroupNum,
	uint64_t totalAtomicMemoryLimitNum) {
	assert(0 < partitionGroupNum && partitionGroupNum <= 128);
	assert(0 < totalAtomicMemoryLimitNum);

	if (partitionGroupNum == 1) {
		if (totalAtomicMemoryLimitNum * 4 <=
			MINIMUM_BUFFER_INFO_LIST_SIZE) {
			return MINIMUM_BUFFER_INFO_LIST_SIZE;
		}

		uint64_t hashTableBit = 0;
		if (totalAtomicMemoryLimitNum - 1 < UINT32_MAX) {
			assert(0 < totalAtomicMemoryLimitNum);
			hashTableBit = util::nextPowerBitsOf2(static_cast<uint32_t>(
				totalAtomicMemoryLimitNum - 1)) + 2;
		}
		else {
			assert((totalAtomicMemoryLimitNum - 1) >> 31);
			hashTableBit = util::nextPowerBitsOf2(static_cast<uint32_t>(
				(totalAtomicMemoryLimitNum - 1) >> 31)) + 32 + 2;
		}
		uint64_t hashTableSize = 1ULL << hashTableBit;
		return hashTableSize;
	}

	uint64_t exponentialPG =
	  util::nextPowerBitsOf2(partitionGroupNum) + 1;
	if ((1ULL << exponentialPG) == partitionGroupNum) {
		exponentialPG--;
	}
	assert(((2 * exponentialPG) / partitionGroupNum) <=
		   2);  

	uint64_t preHashTableSize =
		1 + (totalAtomicMemoryLimitNum - 1) *
		(2 * exponentialPG) / partitionGroupNum;

	uint64_t hashTableBit = 0;
	if (preHashTableSize < UINT32_MAX) {
		assert(0 < preHashTableSize);
		hashTableBit = util::nextPowerBitsOf2(
			static_cast<uint32_t>(preHashTableSize));
	}
	else {
		hashTableBit = util::nextPowerBitsOf2(static_cast<uint32_t>(
			preHashTableSize >> 31)) +
		  32;
	}
	uint64_t hashTableSize = 1ULL << hashTableBit;

	uint64_t totalHashTableSize = hashTableSize * partitionGroupNum;
	if (totalHashTableSize < MINIMUM_BUFFER_INFO_LIST_SIZE) {
		assert(0 < MINIMUM_BUFFER_INFO_LIST_SIZE / partitionGroupNum);
		hashTableBit = util::nextPowerBitsOf2(static_cast<uint32_t>(
				MINIMUM_BUFFER_INFO_LIST_SIZE / partitionGroupNum));
		hashTableSize = 1ULL << hashTableBit;
		assert(MINIMUM_BUFFER_INFO_LIST_SIZE <=
			   hashTableSize * partitionGroupNum);
	}

	return hashTableSize;
}

uint64_t ChunkBuffer::calcHashTableSize2(
		PartitionGroupId partitionGroupNum,
		uint64_t totalAtomicMemoryLimitNum,
		uint64_t chunkExpSize ,double hashTableSizeRate) {

	assert(0 < partitionGroupNum && partitionGroupNum <= 128);
	assert(0 < totalAtomicMemoryLimitNum);
	if (hashTableSizeRate == 0.0) {
		hashTableSizeRate = 0.01;
	}
	uint64_t memLimitByte = totalAtomicMemoryLimitNum << chunkExpSize;
	uint64_t base = static_cast<double>(memLimitByte) * hashTableSizeRate
			/ static_cast<double>(partitionGroupNum) / static_cast<double>(8);

	if (partitionGroupNum == 1 && base * 4 <= MINIMUM_BUFFER_INFO_LIST_SIZE) {
		return MINIMUM_BUFFER_INFO_LIST_SIZE;
	}
	uint64_t hashTableBit = util::nextPowerBitsOf2(base);
	uint64_t hashTableSize = UINT64_C(1) << hashTableBit;

	uint64_t totalHashTableSize = hashTableSize * partitionGroupNum;
	if (totalHashTableSize < MINIMUM_BUFFER_INFO_LIST_SIZE) {
		assert(0 < MINIMUM_BUFFER_INFO_LIST_SIZE / partitionGroupNum);
		hashTableBit = util::nextPowerBitsOf2(
				MINIMUM_BUFFER_INFO_LIST_SIZE / partitionGroupNum);
		hashTableSize = UINT64_C(1) << hashTableBit;
		assert(MINIMUM_BUFFER_INFO_LIST_SIZE <=
			   hashTableSize * partitionGroupNum);
	}

	return hashTableSize;
}




template<class L>
BasicChunkBuffer<L>::BasicChunkBuffer(
		L* locker, PartitionGroupId pgId,
		int32_t chunkSize, int32_t numpages,
		const CompressorParam &compressorParam,
		PartitionId beginPId, PartitionId endPId,
		size_t bufferHashMapSize, size_t chunkCategoryNum,
		ChunkBufferStats &stats) :
		locker_(locker), partitionList_(nullptr),
		pgId_(pgId), beginPId_(beginPId), endPId_(endPId),
		pool_(nullptr), chunkSize_(chunkSize),
		compressorParam_(compressorParam), bufferTable_(NULL),
		storeMemoryAgingSwapRate_(0.02), storeMemoryAgingSwapCount_(0),
		agingSwapCounter_(0), chunkCategoryNum_(chunkCategoryNum),
		stats_(stats) {

	bufferTable_ = UTIL_NEW LRUTable(numpages);
	if (chunkSize >= compressorParam_.fileSystemBlockSize_ * 2) {
		Chunk tmpChunk;
		int32_t skipSize = tmpChunk.getHeaderSize();
		double ratio = 1.0 - static_cast<double>(compressorParam_.minSpaceSavings_) / 100.0;
		int32_t compressedSizeLimit = chunkSize * ratio;
		int32_t maxCompressedSizeLimit = chunkSize - compressorParam_.fileSystemBlockSize_;
		if (compressedSizeLimit > maxCompressedSizeLimit) {
			compressedSizeLimit = maxCompressedSizeLimit;
		}
		setCompression(compressorParam_.mode_, skipSize, compressedSizeLimit);
	}
	else {
		if (compressorParam_.mode_ != ChunkCompressionTypes::NO_BLOCK_COMPRESSION) {
			GS_TRACE_ERROR(CHUNK_MANAGER,
				GS_TRACE_CHM_INVALID_PARAMETER,
				"Compression mode is inactive. "
				<< "StoreBlockSize must be at least twice of "
				<< "block size of file system. "
				<< "StoreBlockSize = " << chunkSize_
				<< ", block size of file system = "
				<< compressorParam_.fileSystemBlockSize_);
		}
	}
	dataFileList_.assign(endPId_ - beginPId_, nullptr);
	setMaxCapacity(numpages);

	UTIL_STATIC_ASSERT((util::IsSame<L, NoLocker>::VALUE));
}

template<class L>
BasicChunkBuffer<L>::~BasicChunkBuffer() {
	bufferTable_->releaseAll(pool_);
	delete locker_;
};

template<class L>
BasicChunkBuffer<L>& BasicChunkBuffer<L>::setCompression(
		ChunkCompressionTypes::Mode mode, size_t skipSize,
		size_t compressedSizeLimit) {
	switch (mode) {
	case ChunkCompressionTypes::BLOCK_COMPRESSION:
		chunkCompressor_.reset(UTIL_NEW ChunkCompressorZip(
				chunkSize_, skipSize, compressedSizeLimit));
		break;
	default:
		assert(mode == ChunkCompressionTypes::NO_BLOCK_COMPRESSION);
		chunkCompressor_.reset();
		break;
	}
	return *this;
}

template<class L>
void BasicChunkBuffer<L>::fin() {
}

template<class L>
void BasicChunkBuffer<L>::setMemoryPool(Chunk::ChunkMemoryPool* pool) {
	assert(pool);
	assert(pool_ == nullptr);
	pool_ = pool;
}

template<class L>
void BasicChunkBuffer<L>::setPartitionList(PartitionList* ptList) {
	partitionList_ = ptList;
}

template<class L>
void BasicChunkBuffer<L>::validatePinCount() {
	if (bufferTable_->getCurrentNumElems() > 0) {
		LRUTable::Cursor bufferTableCursor = bufferTable_->createCursor();
		LRUFrame *fr;
		int32_t pos;
		while (fr = bufferTableCursor.next(pos)) {
			assert(fr->pincount_ == 0);
			if (fr->pincount_ != 0) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_OM_INVALID_OID,
						"pinCount_ is not zero. "
							<< "pos," << pos << ",ref," << fr->pincount_);
			}
		}
	}
}

template<class L>
uint64_t BasicChunkBuffer<L>::getTotalPinCount() {
	int pinCount = 0;
	if (bufferTable_->getCurrentNumElems() > 0) {
		LRUTable::Cursor bufferTableCursor = bufferTable_->createCursor();
		LRUFrame* fr;
		int32_t pos;
		while (fr = bufferTableCursor.next(pos)) {
			pinCount += fr->pincount_;
		}
	}
	return pinCount;
}

template<class L>
bool BasicChunkBuffer<L>::resize(int32_t capacity) {
	ChunkAccessor accessor(*this);
	if (bufferTable_->getCurrentNumElems() - capacity > 0) {
		int32_t reduce = bufferTable_->getCurrentNumElems() - capacity;
		for (int32_t i = 0; i < reduce; i++) {
			if (!bufferTable_->gotoL2FromL1(pool_)) {
				break;
			}
		}
		if (bufferTable_->getCurrentNumElems() == 0) {
			return true;
		}
		LRUTable::Cursor bufferTableCursor = bufferTable_->createCursor();
		LRUFrame *fr;
		int32_t pos;
		while (fr = bufferTableCursor.next(pos)) {
			if ((bufferTable_->getCurrentNumElems() - capacity) <= 0) {
				break;
			}
			if (fr->pincount_ == 0) {
				PartitionId pId = fr->pId_;
				int64_t offset = fr->key_;
				MeshedChunkTable::Group* group = NULL;
				if (partitionList()) {
					group = partitionList()->partition(pId).chunkManager().putGroup(fr->groupId_);
				}
				if (fr->isdirty_) {
					StatStopwatch &watch = stats_.timeTable_(
							ChunkBufferStats::BUF_STAT_SWAP_WRITE_TIME);
					StatStopwatch &compressWatch = stats_.timeTable_(
							ChunkBufferStats::BUF_STAT_SWAP_WRITE_COMPRESS_TIME);

					Chunk* chunk = static_cast<Chunk*>(fr->value_);
					accessor.seekAndWrite(
							fr->pId_, fr->key_,
							chunk->getData(), chunk->getDataSize(),
							watch, compressWatch);
					fr->isdirty_ = false;

					stats_.table_(ChunkBufferStats::BUF_STAT_SWAP_WRITE_COUNT)
							.increment();
					stats_.table_(ChunkBufferStats::BUF_STAT_SWAP_WRITE_SIZE)
							.add(chunk->getDataSize());
					if (group) {
						group->stats()(
								ChunkGroupStats::GROUP_STAT_SWAP_WRITE_COUNT)
								.increment();
					}
				}
				bufferMap_.remove(pId, offset);
				bufferTable_->gotoL2FromL0(pool_, pos);

				stats_.table_(ChunkBufferStats::BUF_STAT_USE_BUFFER_COUNT)
						.decrement();
				if (group) {
					group->stats()(
							ChunkGroupStats::GROUP_STAT_USE_BUFFER_COUNT)
							.decrement();
				}
			}
		}
	}
	else if (bufferTable_->getCurrentNumElems() - capacity < 0) {
		int32_t increase;
		increase = capacity - bufferTable_->getCurrentNumElems();
		for (int32_t i = 0; i < increase; i++) {
			if (!bufferTable_->gotoL1FromL2()) {
				break;
			}
		}
	}
	return true;
}

template<class L>
std::string BasicChunkBuffer<L>::toString() const {
	util::NormalOStringStream oss;
	oss << "BasicChunkBuffer" <<
			",HIT" << stats_.table_(ChunkBufferStats::BUF_STAT_HIT_COUNT).get() <<
			",MISS" << stats_.table_(ChunkBufferStats::BUF_STAT_MISS_HIT_COUNT).get() <<
			",ALC" << stats_.table_(ChunkBufferStats::BUF_STAT_ALLOCATE_COUNT).get() <<
			"," << bufferTable_->toString() << ",";

	return oss.str();
}
template<class L>
std::string BasicChunkBuffer<L>::getProfile() const {
	util::NormalOStringStream oss;
	oss << "BasicChunkBuffer" <<
			",HIT" << stats_.table_(ChunkBufferStats::BUF_STAT_HIT_COUNT).get() <<
			",MISS" << stats_.table_(ChunkBufferStats::BUF_STAT_MISS_HIT_COUNT).get() <<
			",ALC" << stats_.table_(ChunkBufferStats::BUF_STAT_ALLOCATE_COUNT).get() <<
			",NUM" << bufferTable_->getCurrentNumElems();
	return oss.str();
}

template<class L>
void BasicChunkBuffer<L>::setSwapOutCounter(int64_t counter) {
	agingSwapCounter_ = counter;
}
template<class L>
int64_t BasicChunkBuffer<L>::getSwapOutCounter() {
	return agingSwapCounter_;
}


template<class L>
void BasicChunkBuffer<L>::setStoreMemoryAgingSwapRate(double rate, int64_t totalLimitNum, int32_t partitionGroupNum) {
	if (rate < 0.0) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED, "rate=" << rate);
	}
	storeMemoryAgingSwapRate_ = rate;

	updateStoreMemoryAgingParams(totalLimitNum, partitionGroupNum);
}
template<class L>
double BasicChunkBuffer<L>::getStoreMemoryAgingSwapRate() {
	return storeMemoryAgingSwapRate_;
}

template<class L>
void BasicChunkBuffer<L>::updateStoreMemoryAgingParams(
		int64_t totalLimitNum, int32_t partitionGroupNum) {

	const double avgNum =
			totalLimitNum / static_cast<double>(partitionGroupNum);

	const double rate = storeMemoryAgingSwapRate_;

	storeMemoryAgingSwapCount_ = (rate < 1.0) ?
			static_cast<uint64_t>(avgNum * rate) : DISABLE_COLD;
}


template<class L>
BasicChunkBuffer<L>::ChunkAccessor::ChunkAccessor(BasicChunkBuffer& bcBuffer) :
		bcBuffer_(bcBuffer),
		outFile_(nullptr),
		chunkCompressor_(bcBuffer.chunkCompressor_.get()) {
}

template<class L>
int32_t BasicChunkBuffer<L>::ChunkAccessor::swapOut(int32_t limit, int32_t &pos) {
	int32_t count = 0;
	LRUTable::Cursor bufferTableCursor = bcBuffer_.bufferTable_->createCursor();
	int32_t reusePos = -1;
	LRUFrame *fr;
	while (fr = bufferTableCursor.next(pos)) {
		if (fr->pincount_ == 0) {
			PartitionId pId1 = fr->pId_;
			int64_t offset1 = fr->key_; 
			MeshedChunkTable::Group* group = NULL;
			if (bcBuffer_.partitionList()) {
				group = bcBuffer_.partitionList()->partition(pId1).chunkManager().putGroup(fr->groupId_);
			}
			ChunkBufferStats &stats = bcBuffer_.stats_;
			if (fr->isdirty_) {
				StatStopwatch &watch = stats.timeTable_(
						ChunkBufferStats::BUF_STAT_SWAP_WRITE_TIME);
				StatStopwatch &compressWatch = stats.timeTable_(
						ChunkBufferStats::BUF_STAT_SWAP_WRITE_COMPRESS_TIME);

				Chunk* chunk = static_cast<Chunk*>(fr->value_);
				seekAndWrite(
						pId1, offset1, chunk->getData(), chunk->getDataSize(),
						watch, compressWatch);

				stats.table_(ChunkBufferStats::BUF_STAT_SWAP_WRITE_COUNT)
						.increment();
				stats.table_(ChunkBufferStats::BUF_STAT_SWAP_WRITE_SIZE)
						.add(chunk->getDataSize());
				if (group) {
					group->stats()(
							ChunkGroupStats::GROUP_STAT_SWAP_WRITE_COUNT)
							.increment();
				}
			}
			bcBuffer_.bufferMap_.remove(pId1, offset1);
			bcBuffer_.bufferTable_->gotoL1FromL0(pos); 
			++count;

			stats.table_(ChunkBufferStats::BUF_STAT_USE_BUFFER_COUNT)
					.decrement();
			if (group) {
				group->stats()(
						ChunkGroupStats::GROUP_STAT_USE_BUFFER_COUNT)
						.decrement();
			}
			reusePos = pos;
			if (count == limit) {
				break;
			}
		}
	}
	pos = reusePos;  
	return count;
}

template<class L>
void BasicChunkBuffer<L>::ChunkAccessor::pinChunkMissHit(
		PartitionId pId, int64_t offset, bool dirty, bool alloc,
		DSGroupId groupId, LRUFrame *&fr, int32_t &pos) {
	assert(bcBuffer_.beginPId_ <= pId && pId < bcBuffer_.endPId_);

	bcBuffer_.rebalance();
	for (int32_t trial = 0; trial < 2; trial++) {
		pos = bcBuffer_.bufferTable_->getFree();
		if (pos >= 0) {
			fr = bcBuffer_.bufferTable_->get(pos);

			ChunkBufferStats &stats = bcBuffer_.stats_;
			stats.table_(alloc ?
					ChunkBufferStats::BUF_STAT_ALLOCATE_COUNT :
					ChunkBufferStats::BUF_STAT_MISS_HIT_COUNT).increment();
			bcBuffer_.bufferMap_.insert(pId, offset, pos);
			stats.table_(ChunkBufferStats::BUF_STAT_SWAP_NORMAL_READ_COUNT)
				  .increment();
			if (bcBuffer_.agingSwapCounter_ < bcBuffer_.storeMemoryAgingSwapCount_) {
				fr->startHot_ = true;
				stats.table_(ChunkBufferStats::BUF_STAT_PUSH_HOT_COUNT)
						.increment();
				bcBuffer_.bufferTable_->pushTail(pos);
			}
			else {
				bool toHot = bcBuffer_.bufferTable_->pushMiddle(pos);
				fr->startHot_ = toHot;
				if (toHot) {
					stats.table_(ChunkBufferStats::BUF_STAT_PUSH_HOT_COUNT)
							.increment();
				}
				else {
					stats.table_(ChunkBufferStats::BUF_STAT_PUSH_COLD_COUNT)
							.increment();
				}
			}
			MeshedChunkTable::Group* group = NULL;
			if (bcBuffer_.partitionList()) {
				group = bcBuffer_.partitionList()->partition(pId).chunkManager().putGroup(groupId);
			}
			if (fr->pincount_ == 0) {
				stats.table_(ChunkBufferStats::BUF_STAT_USE_BUFFER_COUNT)
						.increment();
				if (group) {
					group->stats()(
							ChunkGroupStats::GROUP_STAT_USE_BUFFER_COUNT)
							.increment();
				}
			}
			fr->pincount_ += 1; 
			if (fr->value_ == NULL) {
				fr->value_ = UTIL_NEW Chunk(*bcBuffer_.pool_, bcBuffer_.chunkSize_);
			}
			Chunk* chunk = static_cast<Chunk*>(fr->value_);
			if (alloc) {
				chunk->setMagic();
			} else {
				StatStopwatch &watch = stats.timeTable_(
						ChunkBufferStats::BUF_STAT_SWAP_READ_TIME);
				seekAndRead(pId, offset, chunk->getData(), chunk->getDataSize(), watch);
				stats.table_(ChunkBufferStats::BUF_STAT_SWAP_READ_COUNT)
						.increment();
				stats.table_(ChunkBufferStats::BUF_STAT_SWAP_READ_SIZE)
						.add(chunk->getDataSize());
				if (group) {
					group->stats()(
							ChunkGroupStats::GROUP_STAT_SWAP_READ_COUNT)
							.increment();
				}
			}
			fr->pId_ = pId;
			fr->groupId_ = groupId;
			fr->key_ = offset;
			if (dirty) {
				fr->isdirty_ = true;
			}
			stats.table_(ChunkBufferStats::BUF_STAT_ACTIVE_COUNT).increment();
			return;
		} else {
			swapOut(1, pos);
			++bcBuffer_.agingSwapCounter_;
			if (pos < 0) {
				if (!bcBuffer_.bufferTable_->gotoL1FromL2()) {
					bcBuffer_.bufferTable_->expand(); 
					bool result = bcBuffer_.bufferTable_->gotoL1FromL2(); 
					assert(result);
				}
			}
		}
	}
	GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "getChunk");
}

template<class L>
void BasicChunkBuffer<L>::ChunkAccessor::purgeChunk(PartitionId pId, int64_t offset, bool lost) {
	assert(bcBuffer_.beginPId_ <= pId && pId < bcBuffer_.endPId_);
	int32_t pos = bcBuffer_.bufferMap_.find(pId, offset);
	if (pos != -1) {
		LRUFrame* fr = bcBuffer_.bufferTable_->get(pos);
		assert(fr);
		MeshedChunkTable::Group* group = NULL;
		if (bcBuffer_.partitionList()) {
			group = bcBuffer_.partitionList()->partition(pId).chunkManager().putGroup(fr->groupId_);
		}
		bcBuffer_.bufferMap_.remove(pId, offset);
		if (fr->isdirty_ && !lost) {
			ChunkBufferStats &stats = bcBuffer_.stats_;
			StatStopwatch &watch = stats.timeTable_(
					ChunkBufferStats::BUF_STAT_CP_WRITE_TIME);
			StatStopwatch &compressWatch = stats.timeTable_(
					ChunkBufferStats::BUF_STAT_CP_WRITE_COMPRESS_TIME);

			Chunk* chunk = static_cast<Chunk*>(fr->value_);
			seekAndWrite(
					pId, offset, chunk->getData(), chunk->getDataSize(),
					watch, compressWatch);

			stats.table_(ChunkBufferStats::BUF_STAT_CP_WRITE_COUNT)
					.increment();
			stats.table_(ChunkBufferStats::BUF_STAT_CP_WRITE_SIZE)
					.add(chunk->getDataSize());
		}
		fr->pincount_ = 0; 
		bcBuffer_.bufferTable_->gotoL1FromL0(pos);

		ChunkBufferStats &stats = bcBuffer_.stats_;
		stats.table_(ChunkBufferStats::BUF_STAT_ACTIVE_COUNT).decrement();

		stats.table_(ChunkBufferStats::BUF_STAT_USE_BUFFER_COUNT).decrement();
		if (group) {
			group->stats()(
					ChunkGroupStats::GROUP_STAT_USE_BUFFER_COUNT).decrement();
		}
	}
}

template<class L>
void BasicChunkBuffer<L>::ChunkAccessor::punchHoleChunk(PartitionId pId, int64_t offset) {
	StatStopwatch::NonStat ioWatch;
	assert(bcBuffer_.beginPId_ <= pId && pId < bcBuffer_.endPId_);
	const size_t nth = pId - bcBuffer_.beginPId_;
	VirtualFileBase* dataFile = bcBuffer_.dataFileList_[nth];
	dataFile->punchHoleBlock(offset, ioWatch.get());

}

template<class L>
void BasicChunkBuffer<L>::ChunkAccessor::flushChunk(PartitionId pId, int64_t offset) {
	int32_t pos = bcBuffer_.bufferMap_.find(pId, offset);
	if (pos != -1) {
		LRUFrame* fr = bcBuffer_.bufferTable_->get(pos);
		assert(fr);
		if (fr->isdirty_) {
			ChunkBufferStats &stats = bcBuffer_.stats_;
			StatStopwatch &watch = stats.timeTable_(
					ChunkBufferStats::BUF_STAT_CP_WRITE_TIME);
			StatStopwatch &compressWatch = stats.timeTable_(
					ChunkBufferStats::BUF_STAT_CP_WRITE_COMPRESS_TIME);

			Chunk* chunk = static_cast<Chunk*>(fr->value_);
			seekAndWrite(
					pId, offset, chunk->getData(), chunk->getDataSize(),
					watch, compressWatch);
			fr->isdirty_ = false;

			stats.table_(ChunkBufferStats::BUF_STAT_CP_WRITE_COUNT)
					.increment();
			stats.table_(ChunkBufferStats::BUF_STAT_CP_WRITE_SIZE)
					.add(chunk->getDataSize());
		}
	} else {
	}
}

template<class L>
void BasicChunkBuffer<L>::ChunkAccessor::dumpChunk(PartitionId pId, int64_t offset) {
	int32_t pos = bcBuffer_.bufferMap_.find(pId, offset);
	if (pos != -1) {
		LRUFrame* fr = bcBuffer_.bufferTable_->get(pos);
		assert(fr);
		Chunk* chunk = static_cast<Chunk*>(fr->value_);
		std::cerr << "dumpChunk,pId," << pId << ",offset," << offset <<
		  ",checkSum," << chunk->calcCheckSum() << ",isDirty," << (fr->isdirty_ ? "True" : "False") << std::endl;
	} else {
	}
}

template<class L>
void BasicChunkBuffer<L>::ChunkAccessor::flush(PartitionId pId) {
	assert(bcBuffer_.beginPId_ <= pId && pId < bcBuffer_.endPId_);
	for (auto& itr : bcBuffer_.bufferMap_.getMap()) {
		if (itr.second != -1) {
			LRUFrame* fr = bcBuffer_.bufferTable_->get(itr.second);
			if (fr) {
				assert(fr->pincount_ > 0);
				if (fr->isdirty_) {
					ChunkBufferStats &stats = bcBuffer_.stats_;
					StatStopwatch &watch = stats.timeTable_(
							ChunkBufferStats::BUF_STAT_CP_WRITE_TIME);
					StatStopwatch &compressWatch = stats.timeTable_(
							ChunkBufferStats::BUF_STAT_CP_WRITE_COMPRESS_TIME);

					Chunk* chunk = static_cast<Chunk*>(fr->value_);
					seekAndWrite(
							pId, itr.first.offset_, chunk->getData(),
							chunk->getDataSize(), watch, compressWatch);
					fr->isdirty_ = false;

					stats.table_(ChunkBufferStats::BUF_STAT_CP_WRITE_COUNT)
							.increment();
					stats.table_(ChunkBufferStats::BUF_STAT_CP_WRITE_SIZE)
							.add(chunk->getDataSize());
				}
			}
		}
	}
	bcBuffer_.flush(pId);
}

template<class L>
int64_t BasicChunkBuffer<L>::ChunkAccessor::copyChunk(
		ChunkCopyContext *cxt, PartitionId pId, int64_t offset, bool keepSrcOffset) {
	assert(bcBuffer_.beginPId_ <= pId && pId < bcBuffer_.endPId_);
	StatStopwatch::NonStat ioWatch;
	assert(cxt);
	Chunk* chunk = cxt->chunk_;
	seekAndRead(
			pId, offset, chunk->getData(), chunk->getDataSize(), ioWatch.get());
	int64_t destOffset = offset;
	if (keepSrcOffset) {
		if (cxt->datafile_) {
			tmpFileWrite(cxt, offset, chunk->getData(), chunk->getDataSize());
		}
	}
	else {
		destOffset = cxt->offset_;
		if (cxt->datafile_) {
			tmpFileWrite(cxt, destOffset, chunk->getData(), chunk->getDataSize());
		}
	}
	++cxt->offset_;
	return destOffset;
}

template<class L>
void BasicChunkBuffer<L>::ChunkAccessor::restoreChunk(
		PartitionId pId, int64_t offset, uint8_t* buff, size_t buffSize) {

	assert(bcBuffer_.beginPId_ <= pId && pId < bcBuffer_.endPId_);
	ChunkBufferStats &stats = bcBuffer_.stats_;
	StatStopwatch &watch = stats.timeTable_(
			ChunkBufferStats::BUF_STAT_CP_WRITE_TIME);
	StatStopwatch &compressWatch = stats.timeTable_(
			ChunkBufferStats::BUF_STAT_CP_WRITE_COMPRESS_TIME);

	seekAndWrite(pId, offset, buff, buffSize, watch, compressWatch);
	stats.table_(ChunkBufferStats::BUF_STAT_CP_WRITE_COUNT).increment();
	stats.table_(ChunkBufferStats::BUF_STAT_CP_WRITE_SIZE).add(buffSize);
}

template<class L>
void BasicChunkBuffer<L>::ChunkAccessor::seekAndRead(
		PartitionId pId, int64_t offset, uint8_t* buff, size_t buffSize,
		StatStopwatch &ioWatch) {
	assert(bcBuffer_.beginPId_ <= pId && pId < bcBuffer_.endPId_);
	const size_t nth = pId - bcBuffer_.beginPId_;
	VirtualFileBase* dataFile = bcBuffer_.dataFileList_[nth];
	Chunk chunk;
	if (chunkCompressor_ != nullptr) {
		dataFile->seekAndRead(offset, buff, buffSize, ioWatch);
		chunk.setData(buff, buffSize);
		util::LockGuard<util::Mutex> guard(bcBuffer_.uncompressorMutex());

		StatStopwatch &watch = bcBuffer_.stats_.timeTable_(
				ChunkBufferStats::BUF_STAT_UNCOMPRESS_TIME);
		watch.start();
		bool result = chunkCompressor_->uncompress(chunk.getData(), chunk.getCompressedSize());
		watch.stop();

		if (result) {
			chunk.setCompressedSize(bcBuffer_.chunkSize_ - chunk.getHeaderSize());
		}
	} else {
		dataFile->seekAndRead(offset, buff, buffSize, ioWatch);
		chunk.setData(buff, buffSize);
	}
	chunk.checkMagic(offset);
	chunk.checkSum(chunk.calcCheckSum(), offset);

}

template<class L>
void BasicChunkBuffer<L>::ChunkAccessor::seekAndWrite(
		PartitionId pId, int64_t offset, uint8_t* buff, size_t buffSize,
		StatStopwatch &ioWatch, StatStopwatch &compressWatch) {
	assert(bcBuffer_.beginPId_ <= pId && pId < bcBuffer_.endPId_);
	size_t nth = pId - bcBuffer_.beginPId_;
	VirtualFileBase* dataFile = outFile_ ? outFile_ : bcBuffer_.dataFileList_[nth];
	size_t targetWriteSize = 0;
	assert(dataFile);
	if (chunkCompressor_ != nullptr) {
		Chunk chunk;
		chunk.setData(buff, buffSize);
		const uint32_t chunkCheckSum = chunk.calcCheckSum();
		size_t compressedSize = 0;
		{
			void* compressedData = NULL;
			util::LockGuard<util::Mutex> guard(bcBuffer_.compressorMutex());

			compressWatch.start();
			compressedSize = chunkCompressor_->compress(chunk.getData(), compressedData);
			compressWatch.stop();

			if (compressedSize > 0) {
				assert(compressedData);
				Chunk outChunk;
				outChunk.setData(static_cast<uint8_t*>(compressedData), bcBuffer_.chunkSize_);
				outChunk.setCompressedSize(compressedSize);
				outChunk.setCheckSum(chunkCheckSum);
				const size_t unitSize = bcBuffer_.compressorParam_.fileSystemBlockSize_;
				size_t writeSize = (((outChunk.getHeaderSize() + compressedSize) + unitSize) / unitSize) * unitSize;
				assert(writeSize < bcBuffer_.chunkSize_);
				targetWriteSize = writeSize;
				dataFile->seekAndWrite(
						offset, outChunk.getData(), outChunk.getDataSize(),
						writeSize, ioWatch);
			} else {
				chunk.setCompressedSize(bcBuffer_.chunkSize_ - chunk.getHeaderSize());
				chunk.setCheckSum(chunkCheckSum);
				dataFile->seekAndWrite(
						offset, chunk.getData(), chunk.getDataSize(),
						bcBuffer_.chunkSize_, ioWatch);
				targetWriteSize = bcBuffer_.chunkSize_;
			}
		}
	}
	else {
		Chunk chunk;
		chunk.setData(buff, buffSize);
		chunk.setCompressedSize(
				bcBuffer_.chunkSize_ - chunk.getHeaderSize());
		chunk.setCheckSum(chunk.calcCheckSum());
		dataFile->seekAndWrite(
				offset, chunk.getData(), chunk.getDataSize(),
				bcBuffer_.chunkSize_, ioWatch);
		targetWriteSize = bcBuffer_.chunkSize_;
	}
}

template<class L>
void BasicChunkBuffer<L>::ChunkAccessor::tmpFileRead(
		ChunkCopyContext* cxt, int64_t offset, uint8_t* buff, size_t buffSize) {
	StatStopwatch::NonStat ioWatch;
	Chunk chunk;
	if (chunkCompressor_ != nullptr) {
		cxt->datafile_->seekAndRead(offset, buff, buffSize, ioWatch.get());
		chunk.setData(buff, buffSize);
		util::LockGuard<util::Mutex> guard(bcBuffer_.uncompressorMutex());
		StatStopwatch &watch = bcBuffer_.stats_.timeTable_(
				ChunkBufferStats::BUF_STAT_UNCOMPRESS_TIME);
		watch.start();
		chunkCompressor_->uncompress(chunk.getData(), chunk.getCompressedSize());
		watch.stop();
	} else {
		cxt->datafile_->seekAndRead(offset, buff, buffSize, ioWatch.get());
		chunk.setData(buff, buffSize);
	}
	chunk.checkMagic(offset);

	chunk.checkSum(chunk.calcCheckSum(), offset);
}

template<class L>
void BasicChunkBuffer<L>::ChunkAccessor::tmpFileWrite(
		ChunkCopyContext* cxt, int64_t offset, uint8_t* buff, size_t buffSize) {
	StatStopwatch::NonStat ioWatch;

	assert(cxt->datafile_);
	Chunk chunk;
	chunk.setData(buff, buffSize);
	chunk.setCompressedSize(
			bcBuffer_.chunkSize_ - chunk.getHeaderSize());
	chunk.setCheckSum(chunk.calcCheckSum());
	cxt->datafile_->seekAndWrite(
			offset, chunk.getData(), chunk.getDataSize(),
			bcBuffer_.chunkSize_, ioWatch.get());

}

template class BasicChunkBuffer<NoLocker>;


LRUTable::LRUTable(int32_t maxNumElems) {
	maxNumElems_ = maxNumElems;
	table_.assign(maxNumElems_, LRUFrame());
	head1_ = -1;
	tail1_ = -1;
	head2_ = -1;
	tail2_ = -1;
	for (int32_t pos = 0; pos < maxNumElems_; pos++) {
		init(pos);
		table_[pos].next_ = pos - 1;
	}
	free_ = maxNumElems_ - 1;

	currentNumElems_ = maxNumElems;
	hotNumElems_ = 0;
	coldNumElems_ = 0;
	stock_ = -1;
	hotRate_ = 5.0 / 8.0;
}

LRUTable::~LRUTable() {
	for (int32_t pos = 0; pos < maxNumElems_; pos++) {
		if (table_[pos].value_) {
			Chunk* chunk = static_cast<Chunk*>(table_[pos].value_);
			delete chunk;
			table_[pos].value_ = NULL;
		}
	}
}

void LRUTable::releaseAll(Chunk::ChunkMemoryPool* pool) {
	size_t count = 0;
	for (int32_t pos = 0; pos < maxNumElems_; pos++) {
		if (table_[pos].value_) {
			Chunk* chunk = static_cast<Chunk*>(table_[pos].value_);
			if (chunk->getData()) {
				pool->deallocate(chunk->getData());
				chunk->reset();
				++count;
			}
			delete chunk;
			table_[pos].value_ = NULL;
		}
	}
}

bool LRUTable::checkL0(int32_t target) {
	int32_t pos = head2_;
	while (pos >= 0) {
		if (pos == target) {
			return true;
		}
		pos = table_[pos].next_;
	}
	pos = head1_;
	while (pos >= 0) {
		if (pos == target) {
			return true;
		}
		pos = table_[pos].next_;
	}
	return false;
}

std::string LRUTable::toString() {
	util::NormalOStringStream oss;
	oss << "LRUTable";
	{
		oss << "<COLD<";
		int32_t pos = head2_;
		int32_t prev = -1;
		while (pos >= 0) {
			oss << pos << ",";
			if (table_[pos].inHot_) {
				assert(!table_[pos].inHot_);
			}
			prev = pos;
			pos = get(pos)->next_;
			if (pos >= 0) {
				if (get(pos)->prev_ != prev) {
					assert(get(pos)->prev_ == prev);
				}
			}
			if (pos == head1_) {
				break;
			}
		}
		pos = head1_;
		oss << ">HOT<";
		while (pos >= 0) {
			oss << pos << ",";
			assert(table_[pos].inHot_);
			prev = pos;
			pos = get(pos)->next_;
			if (pos >= 0) {
				if (get(pos)->prev_ != prev) {
					assert(get(pos)->prev_ == prev);
				}
			}
		}
		oss << ">>";
	}
	{
		oss << "<FREE<";
		int32_t pos = free_;
		while (pos >= 0) {
			oss << pos << ",";
			pos = get(pos)->next_;
		}
		oss << ">>";
	}
	{
		oss << "<STOCK<";
		int32_t pos = stock_;
		while (pos >= 0) {
			oss << pos << ",";
			pos = get(pos)->next_;
		}
		oss << ">>";
	}
	return oss.str();
}


std::string LRUTable::toString(BasicChunkBuffer<NoLocker> &bcBuffer) {
	util::NormalOStringStream oss;
	oss << "LRUTable";
	{
		oss << "<COLD<";
		int32_t pos = head2_;
		int32_t prev = -1;
		while (pos >= 0) {
			oss << std::endl << pos;
			LRUFrame* fr = bcBuffer.bufferTable_->get(pos);
			if (fr && fr->value_) {
				Chunk* chunk = reinterpret_cast<Chunk*>(fr->value_);
				oss << "(" << chunk->toString2().c_str() << ")";
			}
			else {
				oss << "(NULL)";
			}
			oss << ",";
			if (table_[pos].inHot_) {
				assert(!table_[pos].inHot_);
			}
			prev = pos;
			pos = get(pos)->next_;
			if (pos >= 0) {
				if (get(pos)->prev_ != prev) {
					assert(get(pos)->prev_ == prev);
				}
			}
			if (pos == head1_) {
				break;
			}
		}
		pos = head1_;
		oss << ">HOT<";
		while (pos >= 0) {
			oss << std::endl << pos;
			LRUFrame* fr = bcBuffer.bufferTable_->get(pos);
			if (fr && fr->value_) {
				Chunk* chunk = reinterpret_cast<Chunk*>(fr->value_);
				oss << "(" << chunk->toString2().c_str() << ")";
			}
			else {
				oss << "(NULL)";
			}
			oss << ",";
			assert(table_[pos].inHot_);
			prev = pos;
			pos = get(pos)->next_;
			if (pos >= 0) {
				if (get(pos)->prev_ != prev) {
					assert(get(pos)->prev_ == prev);
				}
			}
		}
		oss << ">>";
	}
	{
		oss << "<FREE<";
		int32_t pos = free_;
		while (pos >= 0) {
			oss << pos << ",";
			pos = get(pos)->next_;
		}
		oss << ">>";
	}
	{
		oss << "<STOCK<";
		int32_t pos = stock_;
		while (pos >= 0) {
			oss << pos << ",";
			pos = get(pos)->next_;
		}
		oss << ">>";
	}
	return oss.str();
}


void LRUTable::expand() {
	try {
		int32_t pos = maxNumElems_;
		table_.push_back(LRUFrame());
		++maxNumElems_;
		init(pos);
		assert(table_.size() == maxNumElems_);
		table_[pos].next_ = stock_;
		stock_ = pos;
	} catch(std::exception &e) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_NO_MEMORY, "table expand failed.");
	}
}

BufferHashMap::BufferHashMap()
{
	;
}

BufferHashMap::~BufferHashMap() {
	;
}
