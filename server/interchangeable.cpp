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
	@brief Implementation of InterchangeableStoreMemory
*/
#include "interchangeable.h"
#include "affinity_manager.h"
#include "chunk_buffer.h"
#include <cfloat>

const double InterchangeableStoreMemory::CALC_EMA_HALF_LIFE_CONSTANT = 2.8854;

/*!
	@brief コンストラクタ
*/
InterchangeableStoreMemory::InterchangeableStoreMemory(
		const ConfigTable &config, uint32_t chunkExpSize,
		int64_t storeMemoryLimit, int64_t walMemoryLimit, int64_t affinityMemoryLimit)
: config_(config),
  partitionNum_(config.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM)),
  partitionGroupNum_(PartitionGroupConfig(config).getPartitionGroupCount()),
  chunkExpSize_(chunkExpSize),
  storeMemoryLimit_(storeMemoryLimit), walMemoryLimit_(walMemoryLimit),
  affinityMemoryLimit_(affinityMemoryLimit),
  totalLoad_(0), avgLoad_(0.0),
  atomicShiftableMemRate_(config.get<int32_t>(
		CONFIG_TABLE_DS_STORE_MEMORY_REDISTRIBUTE_SHIFTABLE_MEMORY_RATE)),
  atomicEMAHalfLifePeriod_(config.get<int32_t>(
		CONFIG_TABLE_DS_STORE_MEMORY_REDISTRIBUTE_EMA_HALF_LIFE_PERIOD)),
  smoothingConstant_(0.003)
{
	chunkBuffers_.resize(partitionGroupNum_, NULL);
	walBuffers_.resize(partitionNum_, NULL);
	affinityManagers_.resize(partitionNum_, NULL);
	loadTable_.resize(partitionGroupNum_);
	walBufferSizeList_.resize(partitionNum_, walMemoryLimit);
	affinityManagerSizeList_.resize(partitionNum_, affinityMemoryLimit);
	updateEMAHalfLifePeriod(atomicEMAHalfLifePeriod_);
}

/*!
	@brief デストラクタ
*/
InterchangeableStoreMemory::~InterchangeableStoreMemory() {
	for (auto& itr : walBuffers_) {
		delete itr;
	}
	for (auto& itr : affinityManagers_) {
		delete itr;
	}
}

/*!
	@brief チャンクバッファ登録
*/
void InterchangeableStoreMemory::regist(int32_t pgId, ChunkBuffer* chunkBuffer, int64_t initialBufferSize) {
	assert(chunkBuffer);
	assert(0 <= pgId && static_cast<uint32_t>(pgId) < partitionGroupNum_);
	assert(!chunkBuffers_[pgId]);
	chunkBuffer->use();
	chunkBuffers_[pgId] = chunkBuffer;
	Load& load = loadTable_[pgId];
	load.limitNum_ = initialBufferSize / partitionGroupNum_;
	load.lastBaseLimit_ = initialBufferSize / partitionGroupNum_;
}

ChunkBuffer* InterchangeableStoreMemory::getChunkBuffer(PartitionGroupId pgId) {
	UTIL_STATIC_ASSERT(!std::numeric_limits<PartitionGroupId>::is_signed);
	assert(pgId < partitionGroupNum_);
	return chunkBuffers_[pgId];
}
/*!
	@brief WALバッファ登録
*/
void InterchangeableStoreMemory::regist(int32_t pId, WALBuffer* walBuffer) {
	assert(walBuffer);
	assert(0 <= pId && static_cast<uint32_t>(pId) < partitionNum_);
	if (walBuffers_[pId]) {
		delete walBuffers_[pId];
	}
	walBuffer->resize(static_cast<int32_t>(walMemoryLimit_));
	walBuffers_[pId] = walBuffer;
}

/*!
	@brief アフィニティマネージャ登録
*/
void InterchangeableStoreMemory::regist(int32_t pId, AffinityManager* affinityManager) {
	assert(affinityManager);
	assert(0 <= pId && static_cast<uint32_t>(pId) < partitionNum_);
	if (affinityManagers_[pId]) {
		delete affinityManagers_[pId];
	}
	affinityManager->use();
	affinityManager->resize(static_cast<int32_t>(affinityMemoryLimit_));
	affinityManagers_[pId] = affinityManager;
}

/*!
	@brief チャンクバッファ登録解除
*/
void InterchangeableStoreMemory::unregistChunkBuffer(int32_t pgId) {
	assert(0 <= pgId && static_cast<uint32_t>(pgId) < partitionGroupNum_);
	ChunkBuffer* chunkBuffer = chunkBuffers_[pgId];
	assert(chunkBuffer);
	chunkBuffer->unuse();
	chunkBuffers_[pgId] = NULL;
	delete chunkBuffer;
}

/*!
	@brief WALバッファ、アフィニティマネージャ登録解除
*/
void InterchangeableStoreMemory::unregist(int32_t pId) {
	assert(0 <= pId && static_cast<uint32_t>(pId) < partitionNum_);

	WALBuffer* walBuffer = walBuffers_[pId];
	assert(walBuffer);
	walBuffers_[pId] = NULL;
	delete walBuffer;

	AffinityManager* affinityManager = affinityManagers_[pId];
	assert(affinityManager);
	affinityManager->unuse();
	affinityManagers_[pId] = NULL;
	delete affinityManager;

	loadTable_[pId] = Load();
}

/*!
	@brief storeMemoryLimit更新に伴うバッファサイズ変更
*/
void InterchangeableStoreMemory::updateBufferSize(int64_t newStoreMemLimit) {
	storeMemoryLimit_ = newStoreMemLimit;
	uint64_t storeMemoryLimitMB = newStoreMemLimit / 1024 / 1024;
	uint64_t chunkBufferLimit = storeMemoryLimitMB * 1024 * 1024 / (UINT64_C(1) << chunkExpSize_);
	uint64_t bufferSize = chunkBufferLimit;
	if (bufferSize < partitionGroupNum_) {
		bufferSize = partitionGroupNum_;
		chunkBufferLimit = partitionGroupNum_;
	}
	for (int32_t pgId = 0;
			static_cast<uint32_t>(pgId) < partitionGroupNum_; ++pgId) {
		Load& load = loadTable_[pgId];
		load.limitNum_ = bufferSize / partitionGroupNum_;
		load.lastBaseLimit_ = bufferSize / partitionGroupNum_;
	}
	for (auto& itr : chunkBuffers_) {
		if (itr) {
			itr->setStoreMemoryLimitBlockCount(chunkBufferLimit);
		}
	}
}

void InterchangeableStoreMemory::updateShiftableMemRate(int32_t shiftableMemRate) {
	atomicShiftableMemRate_ = shiftableMemRate;
}

void InterchangeableStoreMemory::updateEMAHalfLifePeriod(int32_t emaHalfLifePeriod) {
	atomicEMAHalfLifePeriod_ = emaHalfLifePeriod;
	smoothingConstant_ =
			2.0 / (static_cast<double>(atomicEMAHalfLifePeriod_)
			   * CALC_EMA_HALF_LIFE_CONSTANT + 1.0);
}

void InterchangeableStoreMemory::collectLoad(int32_t &activePartitionCount) {

	assert(chunkBuffers_.size() == partitionGroupNum_);
	for (auto& itr : chunkBuffers_) {
		++activePartitionCount;
		ChunkBuffer& chunkBuffer = *itr;
		Load& load = loadTable_[chunkBuffer.getPgId()];

		double activity = chunkBuffer.getActivity();
		uint64_t lastReadCount = load.readCount_;
		load.readCount_ = static_cast<uint64_t>(activity);
		load.lastLoad_ = load.load_;
		load.load_ = load.readCount_ - lastReadCount;
		totalLoad_ += load.load_;

		load.lastEstimatedLoad_ = load.newEstimatedLoad_;
		load.newEstimatedLoad_ =
				smoothingConstant_ * static_cast<double>(load.load_) +
				(1.0 - smoothingConstant_) * load.lastEstimatedLoad_;

	}
	avgLoad_ = static_cast<double>(totalLoad_ / partitionGroupNum_);
}

void InterchangeableStoreMemory::calculateEMA(uint64_t minNum, uint64_t totalLimitNum) {
	uint64_t limitTotal = 0;
	double estimatedTotal = 0.0;
	uint64_t lastBaseLimitTotal = 0;
	size_t id = 0;
	assert(loadTable_.size() == partitionGroupNum_);
	for (auto& load : loadTable_) {
		limitTotal += load.limitNum_;
		estimatedTotal += load.newEstimatedLoad_;
		lastBaseLimitTotal += load.lastBaseLimit_;
		++id;
	}
	assert(totalLimitNum >= limitTotal);
	uint64_t targetLimit = totalLimitNum;
	if (estimatedTotal <= DBL_EPSILON) {
		return;
	}
	int32_t maxPgId = 0;
	uint64_t maxLimit = 0;
	uint64_t subTotal = 0;
	assert(targetLimit >= minNum * partitionGroupNum_);
	targetLimit -= minNum * partitionGroupNum_;
	for (PartitionGroupId pgId = 0; pgId < partitionGroupNum_; pgId++) {
		Load &load = loadTable_[pgId];
		load.lastBaseLimit_ = static_cast<uint64_t>(
				static_cast<double>(targetLimit)
				* load.newEstimatedLoad_ / estimatedTotal) + minNum;

		assert(load.lastBaseLimit_ >= minNum);
		subTotal += load.lastBaseLimit_;
		if (maxLimit < load.lastBaseLimit_) {
			maxLimit = load.lastBaseLimit_;
			maxPgId = pgId;
		}
		load.limitNum_ = load.lastBaseLimit_;
	}

	if (subTotal != totalLimitNum) {
		assert(subTotal < totalLimitNum);
		int64_t diff = static_cast<int64_t>(totalLimitNum)
				- static_cast<int64_t>(subTotal);
		if (diff >= partitionGroupNum_) {
			int64_t addition = diff / partitionGroupNum_;
			for (PartitionGroupId pgId = 0; pgId < partitionGroupNum_; pgId++) {
				Load &load = loadTable_[pgId];
				load.lastBaseLimit_ += addition;
				load.limitNum_ = load.lastBaseLimit_;
			}
			diff -= addition * partitionGroupNum_;
		}
		if (diff >= 0) {
			loadTable_[maxPgId].lastBaseLimit_ += diff;
			loadTable_[maxPgId].limitNum_ = loadTable_[maxPgId].lastBaseLimit_;
		}
		else {
			assert(false);
			diff = -diff;
			if (loadTable_[maxPgId].lastBaseLimit_ > diff + minNum) {
				loadTable_[maxPgId].lastBaseLimit_ -= diff;
			}
			else {
				loadTable_[maxPgId].lastBaseLimit_ = minNum;
			}
			loadTable_[maxPgId].limitNum_ = loadTable_[maxPgId].lastBaseLimit_;
	}
	}
	for (PartitionGroupId pgId = 0; pgId < partitionGroupNum_; pgId++) {
	}
}


void InterchangeableStoreMemory::calculateChunkBufferSize() {
	uint64_t limitTotal = 0;
	double estimatedTotal = 0.0;
	uint64_t totalLimitNum = storeMemoryLimit_;
	uint64_t minNum = static_cast<uint64_t>(
			static_cast<double>(totalLimitNum) * 0.2 / partitionGroupNum_);

	int32_t activePartitionCount = 0;

	collectLoad(activePartitionCount);

	calculateEMA(minNum, totalLimitNum);

	uint64_t targetLimit = totalLimitNum;
	UNUSED_VARIABLE(limitTotal);
	UNUSED_VARIABLE(estimatedTotal);
	UNUSED_VARIABLE(targetLimit);
}

void InterchangeableStoreMemory::calculateWALBufferSize() {
	return;
}

void InterchangeableStoreMemory::calculateAffinityManagerSize() {
	return;
}
/*!
	@brief 周期的な融通の計算
*/
void InterchangeableStoreMemory::calculate() {
	calculateChunkBufferSize();
	calculateWALBufferSize();
	calculateAffinityManagerSize();
}

/*!
	@brief 周期的な融通の指示
*/
void InterchangeableStoreMemory::resize() {
	for (auto& chunkBuffer : chunkBuffers_) {
		if (chunkBuffer) {
			chunkBuffer->resize(static_cast<int32_t>(
					loadTable_[chunkBuffer->getPgId()].limitNum_));
		}
	}
	for (auto& walBuffer : walBuffers_) {
		if (walBuffer) {
			walBuffer->resize(static_cast<int32_t>(
					walBufferSizeList_[walBuffer->getPId()]));
		}
	}
	for (auto& affinityManager : affinityManagers_) {
		if (affinityManager) {
			affinityManager->resize(static_cast<int32_t>(
					affinityManagerSizeList_[affinityManager->getPId()]));
		}
	}
}

uint64_t InterchangeableStoreMemory::getChunkBufferLimit(PartitionGroupId pgId) {
	return loadTable_[pgId].limitNum_;
}
