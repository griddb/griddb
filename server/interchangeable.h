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
	@brief Definition of InterchangeableStoreMemory
*/
#ifndef INTERCHANGEABLE_H_
#define INTERCHANGEABLE_H_

#include "utility_v5.h"
#include "data_type.h"
#include "chunk_manager.h"
#include "config_table.h"
#include "log_manager.h"
#include <vector>
#include <string>
#include <memory>

class ChunkBuffer;
class WALBuffer;
class AffinityManager;

/*!
	@brief 融通可能なメモリ管理
*/
class InterchangeableStoreMemory {
public:
	InterchangeableStoreMemory(
			const ConfigTable& config,
			uint32_t chunkExpSize,
			int64_t storeMemoryLimit,
			int64_t walMemoryLimit,
			int64_t affinityMemoryLimit);
	~InterchangeableStoreMemory();

	void regist(int32_t pgId, ChunkBuffer* chunkBuffer, int64_t initialBufferSize);

	void regist(int32_t pId, WALBuffer* walBuffer);

	void regist(int32_t pId, AffinityManager* affinityManager);

	void unregistChunkBuffer(int32_t pgId);

	void unregist(int32_t pId);

	void calculate();

	void resize();

	uint64_t getChunkBufferLimit(PartitionGroupId pgId);

	void updateBufferSize(int64_t newStoreMemLimit);

	void updateShiftableMemRate(int32_t shiftableMemRate);

	void updateEMAHalfLifePeriod(int32_t emaHalfLifePeriod);

	ChunkBuffer* getChunkBuffer(PartitionGroupId pgId);

private:
	static const double CALC_EMA_HALF_LIFE_CONSTANT;

	const ConfigTable& config_;
	const uint32_t partitionNum_;
	const uint32_t partitionGroupNum_;
	const uint32_t chunkExpSize_;
	int64_t storeMemoryLimit_;
	int64_t walMemoryLimit_;
	int64_t affinityMemoryLimit_;

	std::vector<ChunkBuffer*> chunkBuffers_;
	std::vector<WALBuffer*> walBuffers_;
	std::vector<AffinityManager*> affinityManagers_;

	uint64_t totalLoad_;
	double avgLoad_;
	util::Atomic<int32_t> atomicShiftableMemRate_;
	util::Atomic<int32_t> atomicEMAHalfLifePeriod_;
	double smoothingConstant_;

	struct Load {
		uint64_t readCount_;
		uint64_t load_;
		uint64_t lastLoad_;
		double lastEstimatedLoad_;
		double newEstimatedLoad_;
		util::Atomic<uint64_t> limitNum_;
		uint64_t lastBaseLimit_;

		Load()
		: readCount_(0), load_(0), lastLoad_(0),
		  lastEstimatedLoad_(0.0), newEstimatedLoad_(0.0),
		  limitNum_(1), lastBaseLimit_(1) {}
	};
	std::vector<Load> loadTable_;
	std::vector<int64_t> walBufferSizeList_;
	std::vector<int64_t> affinityManagerSizeList_;

	void calculateChunkBufferSize();
	void calculateWALBufferSize();
	void calculateAffinityManagerSize();

	void collectLoad(int32_t &activePartitionCount);
	void calculateEMA(uint64_t minNum, uint64_t totalLimitNum);
};
#endif 
