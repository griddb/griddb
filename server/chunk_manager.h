/*
	Copyright (c) 2012 TOSHIBA CORPORATION.

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
	@brief Definition of ChunkManager
*/
#ifndef CHUNK_MANAGER_H_
#define CHUNK_MANAGER_H_



#define TEST_DIRTY_CHECK_SET(pId, categoryId, cId, chunkPtr)
#define TEST_DIRTY_CHECK_RESET(pId, categoryId, cId)
#define TEST_DIRTY_CHECK(pId, categoryId, cId, bufferInfo, isForceCheck)

#include "util/system.h"
#include "util/trace.h"
#include "bit_array.h"
#include "checkpoint_file.h"
#include "config_table.h"
#include "data_type.h"
#include "gs_error.h"
#include "zlib.h"
#include <bitset>
#include <iomanip>
#include <queue>
#include <vector>

#define EXEC_FAILURE(errorNo)

UTIL_TRACER_DECLARE(CHUNK_MANAGER);  
UTIL_TRACER_DECLARE(CHUNK_MANAGER_DETAIL);  

/*!
	@brief Manages Chunks, or data blocks, of store data.
*/
class ChunkManager {
	class MemoryLimitManager;
	class MetaChunkManager;
	class CheckpointManager;
	class BufferManager;
	friend struct MemoryPoolStats;

	struct PartitionGroupData;
	typedef std::vector<PartitionGroupData*> PartitionGroupDataList;

	typedef util::FixedSizeAllocator<util::Mutex> MemoryPool;
	typedef util::VariableSizeAllocator<> ArrayAllocator;
	typedef util::VariableSizeAllocator<util::NoopMutex,
		util::VariableSizeAllocatorTraits<128, 1024, 1024 * 8> >
		PartitionGroupAllocator;

	class BaseMetaChunk;
	class BufferInfo;

public:

	/*!
		@brief Free mode of allocated Objects one each or a Chunk of Objects at
	   once.
	*/
	enum FreeMode {
		SELF_FREE_MODE = 0,  
		BATCH_FREE_MODE = 1  
	};

	/*!
		@brief Attribute of Chunk Category
	*/
	struct ChunkCategoryAttribute {
		ChunkCategoryAttribute() : freeMode_(SELF_FREE_MODE) {}
		FreeMode freeMode_;
	};

	/*!
		@brief Chunk compression mode
	*/
	enum CompressionMode {
		NO_BLOCK_COMPRESSION = 0,  
		BLOCK_COMPRESSION,		   
		BLOCK_COMPRESSION_MODE_NUM
	};
	/*!
		@brief Operates configuration of ChunkManager.
	*/
	class Config : public ConfigTable::ParamHandler {
		friend class ChunkManager;
	public:
		Config(PartitionGroupId partitionGroupNum, PartitionId partitionNum,
			ChunkCategoryId chunkCategoryNum, int32_t chunkSize,
			int32_t affinitySize, bool isWarmStart, uint64_t storeMemoryLimit,
			uint64_t checkpointMemoryLimit, uint32_t maxOnceSwapNum
			,
			ChunkManager::CompressionMode compressionMode
			);
		~Config();
		PartitionGroupId getPartitionGroupId(PartitionId pId) const {
			assert(pId < partitionNum_);
			return pgIdList_[pId];
		}
		PartitionGroupId getPartitionGroupNum() const {
			return partitionGroupNum_;
		}  
		PartitionId getPartitionNum() const {
			return partitionNum_;
		}
		ChunkCategoryId getChunkCategoryNum() const {
			return categoryNum_;
		}
		uint8_t getChunkExpSize() const {
			return chunkExpSize_;
		}
		uint32_t getChunkSize() const {
			return chunkSize_;
		}
		bool setAtomicStoreMemoryLimit(uint64_t memoryLimitByte) {
			assert((memoryLimitByte >> getChunkExpSize()) <= UINT32_MAX);
			atomicStorePoolLimit_ = memoryLimitByte;
			atomicStorePoolLimitNum_ =
				static_cast<uint32_t>(memoryLimitByte >> getChunkExpSize());
			return true;
		}
		uint64_t getAtomicStoreMemoryLimit() {
			return atomicStorePoolLimit_;
		}
		uint64_t getAtomicStoreMemoryLimitNum() {
			return atomicStorePoolLimitNum_;
		}
		bool setAtomicCheckpointMemoryLimit(uint64_t memoryLimitByte) {
			assert((memoryLimitByte >> getChunkExpSize()) <= UINT32_MAX);
			atomicCheckpointPoolLimit_ = memoryLimitByte;
			atomicCheckpointPoolLimitNum_ =
				static_cast<uint32_t>(memoryLimitByte >> getChunkExpSize());
			return true;
		}
		uint64_t getAtomicCheckpointMemoryLimit() {
			return atomicCheckpointPoolLimit_;
		}
		uint64_t getAtomicCheckpointMemoryLimitNum() {
			return atomicCheckpointPoolLimitNum_;
		}
		bool setAffinitySize(int32_t affinitySize) {
			atomicAffinitySize_ = affinitySize;
			return true;
		}
		int32_t getAffinitySize() {
			return atomicAffinitySize_;
		}
		CompressionMode getCompressionMode() const {
			return compressionMode_;
		}

		CompressionMode getPartitionCompressionMode(PartitionId pId) const {
			return partitionCompressionMode_[pId];
		}

		bool setPartitionCompressionMode(
			PartitionId pId, CompressionMode mode) {
			if (mode == NO_BLOCK_COMPRESSION) {
				partitionCompressionMode_[pId] = NO_BLOCK_COMPRESSION;
				GS_TRACE_ERROR(CHUNK_MANAGER, GS_TRACE_CHM_CONFIG,
					"partition " << pId << " compression mode is inactive. ");
			}
			else {
				if (compressionMode_ == NO_BLOCK_COMPRESSION) {
					partitionCompressionMode_[pId] = NO_BLOCK_COMPRESSION;
					GS_TRACE_ERROR(CHUNK_MANAGER, GS_TRACE_CHM_CONFIG,
						"partition " << pId
									 << " compression mode is inactive. ");
					return false;
				}
				else {
					partitionCompressionMode_[pId] = BLOCK_COMPRESSION;
					GS_TRACE_INFO(CHUNK_MANAGER, GS_TRACE_CHM_CONFIG,
						"partition " << pId << " compression mode is active. ");
				}
			}
			return true;  
		}
		std::string dump();

	private:
		const PartitionGroupId partitionGroupNum_;
		const PartitionId partitionNum_;
		const ChunkCategoryId categoryNum_;
		const uint8_t chunkExpSize_;
		const uint32_t chunkSize_;
		util::Atomic<uint32_t>
			atomicAffinitySize_;  
		const bool isWarmStart_;
		const uint32_t maxOnceSwapNum_;
		std::vector<CompressionMode> partitionCompressionMode_;  
		CompressionMode compressionMode_;
		std::vector<PartitionGroupId> pgIdList_;
		util::Atomic<uint64_t> atomicStorePoolLimit_;
		util::Atomic<uint32_t> atomicStorePoolLimitNum_;
		util::Atomic<uint64_t> atomicCheckpointPoolLimit_;
		util::Atomic<uint32_t> atomicCheckpointPoolLimitNum_;
		bool isWarmStart() const {
			return isWarmStart_;
		}
		uint32_t getMaxOnceSwapNum() const {
			return maxOnceSwapNum_;
		}
		void appendPartitionGroupId(PartitionGroupId pgId) {
			pgIdList_.push_back(pgId);
		}
		CompressionMode initializetCompressionMode(CompressionMode mode) {
			for (PartitionId pId = 0; pId < getPartitionNum(); pId++) {
				partitionCompressionMode_[pId] = mode;
			}
			return mode;
		}
		bool isValid();
		void setUpConfigHandler(ConfigTable& configTable);
		void operator()(ConfigTable::ParamId id, const ParamValue& value);
		Config(const Config&);
		Config& operator=(const Config&);
	};

	/*!
		@brief Statistics of ChunkManager.
	*/
	class ChunkManagerStats : public StatTable::StatUpdator {
		friend class ChunkManager;

	public:
		ChunkManagerStats();
		~ChunkManagerStats();
		uint64_t getUseStore(ChunkCategoryId categoryId) const;
		uint64_t getStoreMemory(ChunkCategoryId categoryId) const;
		uint64_t getSwapRead(ChunkCategoryId categoryId) const;
		uint64_t getSwapWrite(ChunkCategoryId categoryId) const;
		uint64_t getCheckpointFileSize(PartitionGroupId pgId) const;
		uint64_t getCheckpointFileAllocateSize(PartitionGroupId pgId) const;
		uint64_t getUseStore() const;
		uint64_t getBatchFree() const;
		int64_t getStoreAllocateData() const;
		uint64_t getCheckpointMemory() const;
		uint64_t getAllocatedCheckpointBufferCount() const;
		uint64_t getCheckpointFileSize() const;
		double getCheckpointFileUsageRate() const;
		const char8_t* getActualCompressionMode() const;
		uint64_t getCheckpointFileAllocateSize() const;
		uint64_t getCheckpointWriteBufferSize() const;
		uint64_t getCheckpointWriteSize() const;
		uint64_t getSyncReadSize() const;
		uint64_t getCheckpointWriteTime() const;
		uint64_t getSyncReadTime() const;
		uint64_t getStoreMemory() const;
		uint64_t getGlobalPoolMemory() const;
		uint64_t getGlobalCheckpointPoolMemory() const;

		uint64_t getGlobalPoolAllocateMemory() const;
		uint64_t getGlobalCheckpointPoolAllocateMemory() const;
		uint64_t getSwapRead() const;
		uint64_t getSwapWrite() const;
		uint64_t getSwapReadSize() const;
		uint64_t getSwapWriteSize() const;
		uint64_t getRecoveryReadSize() const;
		uint64_t getSwapReadTime() const;
		uint64_t getSwapWriteTime() const;
		uint64_t getRecoveryReadTime() const;

		uint64_t getPGStoreMemoryLimit(PartitionGroupId pgId) const;
		uint64_t getPGStoreMemory(PartitionGroupId pgId) const;
		uint64_t getPGSwapRead(PartitionGroupId pgId) const;

		static class StatSetUpHandler : public StatTable::SetUpHandler {
			virtual void operator()(StatTable& stat);
		} statSetUpHandler_;

		static StatTableParamId getStoreParamId(
			int32_t parentId, StatTableParamId id);

		virtual bool operator()(StatTable& stat);

	private:
		void setManager(ChunkManager& chunkManager) {
			manager_ = &chunkManager;
		}
		ChunkManager* manager_;
		Config& getConfig() const {
			return manager_->config_;
		}
		MemoryPool& getStoreMemoryPool() const {
			return manager_->storeMemoryPool_;
		}
		MemoryPool& getCheckpointMemoryPool() const {
			return manager_->checkpointMemoryPool_;
		}
		MemoryLimitManager& getMemoryLimitManager() const {
			return manager_->memoryLimitManager_;
		}
		MetaChunkManager& getMetaChunkManager(PartitionGroupId pgId) const {
			return manager_->partitionGroupData_[pgId]->metaChunkManager_;
		}
		CheckpointManager& getCheckpointManager(PartitionGroupId pgId) const {
			return manager_->partitionGroupData_[pgId]->checkpointManager_;
		}
		BufferManager& getBufferManager(PartitionGroupId pgId) const {
			return manager_->partitionGroupData_[pgId]->bufferManager_;
		}
		ChunkManagerStats(const ChunkManagerStats&);
		ChunkManagerStats& operator=(const ChunkManagerStats&);
	};

	static const uint8_t MAX_CHUNK_EXP_SIZE_ = 20;
	static const int32_t CHUNK_CATEGORY_NUM = 5;
	static const int32_t MAX_CHUNK_ID =
		INT32_MAX - 1;  
	static const int32_t CHUNK_HEADER_FULL_SIZE = 256;  

public:
	/*!
		@brief Manages basic information of all Chunks.
	*/
	class MetaChunk {
		friend class ChunkManager;
	public:
		MetaChunk();
		MetaChunk(MetaChunkManager* metaChunkManager)
			: base_(NULL),
			  buffer_(NULL),
			  bufferInfo_(NULL),
			  metaChunkManager_(metaChunkManager) {}
		~MetaChunk();
		bool isFree() const;
		uint8_t* getPtr() const;
		int64_t getCheckpointPos() const;
		ChunkKey getChunkKey() const;
		uint8_t getUnoccupiedSize() const;
		bool setUnoccupiedSize(uint8_t unoccupiedSize);
		void addOccupiedSize(uint64_t objectSize);
		void subtractOccupiedSize(uint64_t objectSize);
		uint64_t getOccupiedSize() const;
		bool isValid() const;
		std::string dump(bool isFull = true);
		UTIL_FORCEINLINE void setCumulativeDirtyFlag(bool flag) {
			base_->setCumulativeDirtyFlag(flag);
		}
		UTIL_FORCEINLINE bool getCumulativeDirtyFlag() {
			return base_->getCumulativeDirtyFlag();
		}
		UTIL_FORCEINLINE void setDifferentialDirtyFlag(bool flag) {
			base_->setDifferentialDirtyFlag(flag);
		}
		UTIL_FORCEINLINE bool getDifferentialDirtyFlag() {
			return base_->getDifferentialDirtyFlag();
		}

	private:
		BaseMetaChunk* base_;
		uint8_t* buffer_;
		BufferInfo* bufferInfo_;
		MetaChunkManager* metaChunkManager_;
		void setCheckpointPos(int64_t checkpointPos);
		void resetCheckpointPos();
		bool setChunkKey(ChunkKey chunkKey);
		void restoreUnoccupiedSize(uint8_t unoccupiedSize);
		void resetUnoccupiedSize();
		void setBuffer(uint8_t*& buffer);
		void resetBuffer();
		void setMetaChunkManager(MetaChunkManager& metaChunkManager);
		bool isAffinityUse() const;
		void setAffinityUsed();
		void setAffinityUnused();
		MetaChunk(const MetaChunk&);
		MetaChunk& operator=(const MetaChunk&);
	};

	/*!
		@brief Operates header data of each Chunk.
	*/
	class ChunkHeader {
		friend class ChunkManager;

	public:


		static const int32_t CHUNK_HEADER_PADDING_ = 12;  
		static const uint16_t GS_FILE_VERSION = 2000;

		static const Offset_t OBJECT_HEADER_OFFSET = 0;
		static const Offset_t CHECKSUM_OFFSET = CHUNK_HEADER_PADDING_;
		static const Offset_t VERSION_OFFSET =
			CHECKSUM_OFFSET + sizeof(uint32_t);
		static const Offset_t MAGIC_OFFSET = VERSION_OFFSET + sizeof(uint16_t);

		static const Offset_t PARTITION_GROUP_NUM_OFFSET =
			MAGIC_OFFSET + sizeof(uint16_t);
		static const Offset_t PARTITION_NUM_OFFSET =
			PARTITION_GROUP_NUM_OFFSET + sizeof(PartitionGroupId);
		static const Offset_t PARTITION_GROUP_ID_OFFSET =
			PARTITION_NUM_OFFSET + sizeof(PartitionId);
		static const Offset_t PARTITION_ID_OFFSET =
			PARTITION_GROUP_ID_OFFSET + sizeof(PartitionGroupId);

		static const Offset_t CHUNK_CATEGORY_NUM_OFFSET =
			PARTITION_ID_OFFSET + sizeof(PartitionId);
		static const Offset_t CHUNK_CATEGORY_ID_OFFSET =
			CHUNK_CATEGORY_NUM_OFFSET + sizeof(int8_t);
		static const Offset_t CHUNK_POWER_SIZE_OFFSET =
			CHUNK_CATEGORY_ID_OFFSET + sizeof(ChunkCategoryId);
		static const Offset_t UNOCCUPIED_SIZE_OFFSET =
			CHUNK_POWER_SIZE_OFFSET + sizeof(int8_t);
		static const Offset_t OCCUPIED_SIZE_OFFSET =
			UNOCCUPIED_SIZE_OFFSET + sizeof(uint8_t);
		static const Offset_t CHUNK_ID_OFFSET =
			OCCUPIED_SIZE_OFFSET + sizeof(int32_t);
		static const Offset_t CHUNK_KEY_OFFSET =
			CHUNK_ID_OFFSET + sizeof(ChunkId);

		static const Offset_t SWAP_CPID_OFFSET =
			CHUNK_KEY_OFFSET + sizeof(ChunkKey);
		static const Offset_t CP_CPID_OFFSET =
			SWAP_CPID_OFFSET + sizeof(CheckpointId);
		static const Offset_t CHUNK_ATTRIBUTE_OFFSET =
			CP_CPID_OFFSET + sizeof(CheckpointId);
		static const Offset_t CHUNK_PADDING_OFFSET =
			CHUNK_ATTRIBUTE_OFFSET + sizeof(uint8_t);

		static const Offset_t CHUNK_DATA_SIZE_OFFSET =
			CHUNK_PADDING_OFFSET + sizeof(uint8_t) * 3;
		static const Size_t CHUNK_HEADER_SIZE =
			CHUNK_DATA_SIZE_OFFSET + sizeof(uint32_t);

		static const uint16_t MAGIC_NUMBER = 0xcd14;
		static const Offset_t CHECK_SUM_START_OFFSET = VERSION_OFFSET;

		UTIL_FORCEINLINE static void initialize(uint8_t* data,
			uint8_t chunkExpSize, PartitionGroupId pgId, PartitionId pId,
			ChunkCategoryId categoryId, ChunkId cId,
			ChunkCategoryAttribute& attribute, ChunkKey chunkKey,
			PartitionGroupId partitionGroupNum, PartitionId partitionNum,
			ChunkCategoryId categoryNum) {
			memset(data, 0, CHUNK_HEADER_FULL_SIZE);

			setVersion(data, GS_FILE_VERSION);
			setMagicNum(data);

			setPartitionNum(data, partitionNum);
			setPartitionGroupNum(data, partitionGroupNum);
			setPartitionGroupId(data, pgId);
			setPartitionId(data, pId);

			setChunkCategoryNum(data, categoryNum);
			setChunkCategoryId(data, categoryId);
			setChunkExpSize(data, chunkExpSize);
			setUnoccupiedSize(data, 0);
			setOccupiedSize(data, 0);
			setChunkId(data, cId);
			setChunkKey(data, chunkKey);

			setSwapCpId(data, 0);
			setCPCpId(data, 0);
			setAttribute(data, attribute.freeMode_);
			setCompressedDataSize(data, 0);
		}

		UTIL_FORCEINLINE static uint32_t getCheckSum(const uint8_t* data) {
			return *(const uint32_t*)(data + CHECKSUM_OFFSET);
		}
		UTIL_FORCEINLINE static uint32_t calcCheckSum(const uint8_t* data) {
			return util::fletcher32(data + CHECK_SUM_START_OFFSET,
				(1ULL << getChunkExpSize(data)) - CHECK_SUM_START_OFFSET);
		}
		UTIL_FORCEINLINE static uint32_t updateCheckSum(uint8_t* data) {
			uint32_t checkSum = calcCheckSum(data);
			memcpy((data + CHECKSUM_OFFSET), &checkSum, sizeof(uint32_t));
			return checkSum;
		}
		UTIL_FORCEINLINE static bool check(const uint8_t* data) {
			uint32_t headerCheckSum = getCheckSum(data);
			uint32_t currentCheckSum = calcCheckSum(data);
			return (headerCheckSum == currentCheckSum);
		}
		UTIL_FORCEINLINE static uint32_t getVersion(const uint8_t* data) {
			return *(const uint16_t*)(data + VERSION_OFFSET);
		}
		UTIL_FORCEINLINE static void setVersion(
			uint8_t* data, uint16_t version) {
			memcpy((data + VERSION_OFFSET), &version, sizeof(uint16_t));
		}

		UTIL_FORCEINLINE static bool checkMagicNum(const uint8_t* data) {
			return (MAGIC_NUMBER ==
					*reinterpret_cast<uint16_t*>(
						const_cast<uint8_t*>(data + MAGIC_OFFSET)));
		}
		UTIL_FORCEINLINE static void setMagicNum(uint8_t* data) {
			*reinterpret_cast<uint16_t*>(data + MAGIC_OFFSET) = MAGIC_NUMBER;
		}
		UTIL_FORCEINLINE static PartitionGroupId getPartitionGroupNum(
			const uint8_t* data) {
			return *reinterpret_cast<const PartitionGroupId*>(
				data + PARTITION_GROUP_NUM_OFFSET);
		}
		UTIL_FORCEINLINE static void setPartitionGroupNum(
			uint8_t* data, PartitionGroupId partitionGroupNum) {
			memcpy((data + PARTITION_GROUP_NUM_OFFSET), &partitionGroupNum,
				sizeof(PartitionGroupId));
		}
		UTIL_FORCEINLINE static uint32_t getPartitionNum(const uint8_t* data) {
			return *reinterpret_cast<const PartitionGroupId*>(
				data + PARTITION_NUM_OFFSET);
		}
		UTIL_FORCEINLINE static void setPartitionNum(
			uint8_t* data, PartitionId partitionNum) {
			memcpy((data + PARTITION_NUM_OFFSET), &partitionNum,
				sizeof(PartitionId));
		}
		UTIL_FORCEINLINE static PartitionGroupId getPartitionGroupId(
			const uint8_t* data) {
			return *reinterpret_cast<const PartitionGroupId*>(
				data + PARTITION_GROUP_ID_OFFSET);
		}
		UTIL_FORCEINLINE static void setPartitionGroupId(
			uint8_t* data, PartitionGroupId pgId) {
			memcpy((data + PARTITION_GROUP_ID_OFFSET), &pgId,
				sizeof(PartitionGroupId));
		}
		UTIL_FORCEINLINE static PartitionId getPartitionId(
			const uint8_t* data) {
			return *reinterpret_cast<const PartitionGroupId*>(
				data + PARTITION_ID_OFFSET);
		}
		UTIL_FORCEINLINE static void setPartitionId(
			uint8_t* data, PartitionId pId) {
			memcpy((data + PARTITION_ID_OFFSET), &pId, sizeof(PartitionId));
		}
		UTIL_FORCEINLINE static ChunkCategoryId getChunkCategoryNum(
			const uint8_t* data) {
			return *reinterpret_cast<const ChunkCategoryId*>(
				data + CHUNK_CATEGORY_NUM_OFFSET);
		}
		UTIL_FORCEINLINE static void setChunkCategoryNum(
			uint8_t* data, ChunkCategoryId categoryId) {
			memcpy((data + CHUNK_CATEGORY_NUM_OFFSET), &categoryId,
				sizeof(ChunkCategoryId));
		}
		UTIL_FORCEINLINE static ChunkCategoryId getChunkCategoryId(
			const uint8_t* data) {
			return *reinterpret_cast<const ChunkCategoryId*>(
				data + CHUNK_CATEGORY_ID_OFFSET);
		}
		UTIL_FORCEINLINE static void setChunkCategoryId(
			uint8_t* data, ChunkCategoryId categoryId) {
			memcpy((data + CHUNK_CATEGORY_ID_OFFSET), &categoryId,
				sizeof(ChunkCategoryId));
		}
		UTIL_FORCEINLINE static uint8_t getChunkExpSize(const uint8_t* data) {
			return *(data + CHUNK_POWER_SIZE_OFFSET);
		}
		UTIL_FORCEINLINE static void setChunkExpSize(
			uint8_t* data, uint8_t chunkExpSize) {
			memcpy((data + CHUNK_POWER_SIZE_OFFSET), &chunkExpSize,
				sizeof(uint8_t));
		}
		UTIL_FORCEINLINE static uint8_t getUnoccupiedSize(const uint8_t* data) {
			return *(data + UNOCCUPIED_SIZE_OFFSET);
		}
		UTIL_FORCEINLINE static void setUnoccupiedSize(
			uint8_t* data, uint8_t unoccupiedSize) {
			memcpy((data + UNOCCUPIED_SIZE_OFFSET), &unoccupiedSize,
				sizeof(uint8_t));
		}
		UTIL_FORCEINLINE static int32_t getOccupiedSize(const uint8_t* data) {
			return *reinterpret_cast<int32_t*>(
				const_cast<uint8_t*>(data + OCCUPIED_SIZE_OFFSET));
		}
		UTIL_FORCEINLINE static void setOccupiedSize(
			uint8_t* data, int32_t occupiedSize) {
			memcpy(
				(data + OCCUPIED_SIZE_OFFSET), &occupiedSize, sizeof(int32_t));
		}
		UTIL_FORCEINLINE static ChunkId getChunkId(const uint8_t* data) {
			return *reinterpret_cast<const ChunkId*>(data + CHUNK_ID_OFFSET);
		}
		UTIL_FORCEINLINE static void setChunkId(uint8_t* data, ChunkId cId) {
			memcpy((data + CHUNK_ID_OFFSET), &cId, sizeof(ChunkId));
		}
		UTIL_FORCEINLINE static ChunkKey getChunkKey(const uint8_t* data) {
			return *reinterpret_cast<ChunkKey*>(
				const_cast<uint8_t*>(data + CHUNK_KEY_OFFSET));
		}
		UTIL_FORCEINLINE static void setChunkKey(
			uint8_t* data, ChunkKey chunkKey) {
			memcpy((data + CHUNK_KEY_OFFSET), &chunkKey, sizeof(ChunkKey));
		}
		UTIL_FORCEINLINE static CheckpointId getSwapCpId(const uint8_t* data) {
			return *reinterpret_cast<CheckpointId*>(
				const_cast<uint8_t*>(data + SWAP_CPID_OFFSET));
		}
		UTIL_FORCEINLINE static void setSwapCpId(
			uint8_t* data, CheckpointId cpId) {
			memcpy((data + SWAP_CPID_OFFSET), &cpId, sizeof(CheckpointId));
		}
		UTIL_FORCEINLINE static CheckpointId getCPCpId(const uint8_t* data) {
			return *reinterpret_cast<CheckpointId*>(
				const_cast<uint8_t*>(data + CP_CPID_OFFSET));
		}
		UTIL_FORCEINLINE static void setCPCpId(
			uint8_t* data, CheckpointId cpId) {
			memcpy((data + CP_CPID_OFFSET), &cpId, sizeof(CheckpointId));
		}
		UTIL_FORCEINLINE static uint8_t getAttribute(const uint8_t* data) {
			return *(data + CHUNK_ATTRIBUTE_OFFSET);
		}
		UTIL_FORCEINLINE static void setAttribute(
			uint8_t* data, uint8_t attribute) {
			memcpy(
				(data + CHUNK_ATTRIBUTE_OFFSET), &attribute, sizeof(uint8_t));
		}
		static void validateHeader(const uint8_t* data,
			PartitionId expectedPartitionNum, uint8_t expectedChunkExpSize,
			bool checkCheckSum) {
			uint32_t version = getVersion(data);
			if (version != GS_FILE_VERSION) {
				GS_THROW_USER_ERROR(GS_ERROR_CHM_INVALID_VERSION,
					"version=" << version << ", expected=" << GS_FILE_VERSION);
			}

			uint8_t chunkExpSize = getChunkExpSize(data);
			if (chunkExpSize != expectedChunkExpSize) {
				GS_THROW_USER_ERROR(GS_ERROR_CHM_INVALID_CHUNK_SIZE,
					"chunkExponentialSize=" << (int32_t)chunkExpSize
											<< ", expected="
											<< (int32_t)expectedChunkExpSize);
			}

			PartitionId partitionNum = getPartitionNum(data);
			if (expectedPartitionNum != partitionNum) {
				GS_THROW_USER_ERROR(GS_ERROR_CHM_INVALID_PARTITION_NUM,
					"partitionNum=" << partitionNum
									<< ", expected=" << expectedPartitionNum);
			}

			PartitionId partitionId = getPartitionId(data);
			if (partitionNum <= partitionId) {
				GS_THROW_USER_ERROR(GS_ERROR_CHM_INVALID_PARTITION_ID,
					"partitionId=" << partitionId
								   << ", partitionNum=" << partitionNum);
			}
		}


		inline static uint32_t getCompressedDataSize(const uint8_t* data) {
			return *(const uint32_t*)(data + CHUNK_DATA_SIZE_OFFSET);
		}
		inline static void setCompressedDataSize(
			uint8_t* data, uint32_t dataSize) {
			memcpy(
				(data + CHUNK_DATA_SIZE_OFFSET), &dataSize, sizeof(uint32_t));
		}

		static std::string dumpFieldName();
		static std::string dump(const uint8_t* chunk);

	private:
		uint8_t* data_;
		ChunkHeader();
		~ChunkHeader();
	};  

	ChunkManager(ConfigTable&, ChunkCategoryId chunkCategoryNum,
		const ChunkCategoryAttribute* chunkCategoryAttributeList,
		bool isReadOnlyModeFile, bool isCreateModeFile);
	~ChunkManager();
	bool isBatchFreeMode(ChunkCategoryId categoryId) {
		bool isBatchFree = (getChunkCategoryAttribute(categoryId).freeMode_ ==
							BATCH_FREE_MODE);
		return isBatchFree;
	}


	bool existPartition(PartitionId pId);
	void dropPartition(PartitionId pId);
	void resetRefCounter(PartitionId pId);
	void fix(PartitionId pId, ChunkCategoryId categoryId, ChunkId cId);
	void unfix(PartitionId pId, ChunkCategoryId categoryId, ChunkId cId);
	MetaChunk* allocateChunk(PartitionId pId, ChunkCategoryId categoryId,
		AffinityGroupId affinityValue, ChunkKey chunkKey, ChunkId cId);
	void freeChunk(PartitionId pId, ChunkCategoryId categoryId, ChunkId cId);
	MetaChunk* getChunkForUpdate(PartitionId pId, ChunkCategoryId categoryId,
		ChunkId cId, ChunkKey chunkKey, bool doFix);
	MetaChunk* getChunk(
		PartitionId pId, ChunkCategoryId categoryId, ChunkId cId);
	MetaChunk* searchChunk(PartitionId pId, ChunkCategoryId categoryId,
		AffinityGroupId affinityValue, ChunkKey chunkKey, uint8_t powerSize,
		ChunkId& cId);
	MetaChunk* searchChunk(PartitionId pId, ChunkCategoryId categoryId,
		ChunkId cId, ChunkKey chunkKey, uint8_t powerSize);
	void redistributeMemoryLimit(PartitionId pId);
	void adjustStoreMemory(PartitionId pId);
	void adjustStoreMemoryPool() {
		storeMemoryPool_.setLimit(util::AllocatorStats::STAT_STABLE_LIMIT,
			config_.getAtomicStoreMemoryLimit());
	}
	void adjustCheckpointMemoryPool() {
		checkpointMemoryPool_.setLimit(util::AllocatorStats::STAT_STABLE_LIMIT,
			config_.getAtomicCheckpointMemoryLimit());
	}
	void reconstructAffinityTable(PartitionId pId);
	bool batchFreeChunk(PartitionId pId, ChunkKey chunkKey,
		uint64_t scanChunkNum, uint64_t& scanCount, uint64_t& freeChunkNum);


	void isValidFileHeader(PartitionGroupId pgId);
	void setCheckpointBit(
		PartitionGroupId pgId, const uint8_t* bitList, uint64_t bitNum);
	void recoveryChunk(PartitionId pId, ChunkCategoryId categoryId, ChunkId cId,
		ChunkKey chunkKey, uint8_t unoccupiedSize, uint64_t filePos);
	void recoveryChunk(PartitionId pId, const uint8_t* chunk, uint32_t size);
	void adjustPGStoreMemory(PartitionGroupId pgId);


	void startCheckpoint(PartitionGroupId pgId, CheckpointId cpId);
	void endCheckpoint(PartitionGroupId pgId, CheckpointId cpId);
	void flush(PartitionGroupId pgId);
	BitArray& getCheckpointBit(PartitionGroupId pgId);

	void startCheckpoint(PartitionId pId);
	void endCheckpoint(PartitionId pId);
	uint64_t getScanSize(PartitionId pId);
	MetaChunk* begin(
		PartitionId pId, ChunkCategoryId& categoryId, ChunkId& cId);
	MetaChunk* next(PartitionId pId, ChunkCategoryId& categoryId, ChunkId& cId);
	bool isCopyLeft(PartitionId pId);
	bool copyChunk(PartitionId pId);
	uint64_t writeChunk(PartitionId pId);
	void cleanCheckpointData(PartitionGroupId pgId);
	uint64_t startSync(CheckpointId cpId, PartitionId pId);
	bool getCheckpointChunk(PartitionId pId, uint32_t size, uint8_t* buffer);


	Config& getConfig() {
		return config_;
	}
	ChunkManagerStats& getChunkManagerStats() {
		return chunkManagerStats_;
	}
	std::string dump(
		PartitionId pId, ChunkCategoryId categoryId = UNDEF_CHUNK_CATEGORY_ID);
	std::string dumpChunkCSVField();
	std::string dumpChunkCSV(PartitionGroupId pgId, int64_t pos);

	int32_t getRefCount(
		PartitionId pId, ChunkCategoryId categoryId, ChunkId cId) {
		assert(isValidId(pId, categoryId, cId));
		return getBufferManager(pId).getRefCounter(pId, categoryId, cId);
	}

private:
	static const uint8_t UNDEF_UNOCCUPIED_SIZE_ = UINT8_MAX;
	static const uint64_t MAX_EMPTY_CHUNK_SEARCH_NUM_ = 1024;
	static const uint64_t MAX_COPY_SCAN_NUM_ = 1024;
	static const uint32_t MAX_ONCE_SWAP_SIZE_BYTE_ = 16 * 1024 * 1024;  
	static const uint32_t INIT_CHUNK_RESERVE_NUM = 2;

	struct ChunkCursor {
		ChunkId cId_;
		ChunkCategoryId categoryId_;
		ChunkCursor(ChunkCategoryId categoryId = 0, ChunkId cId = 0)
			: cId_(cId), categoryId_(categoryId) {}
	};

	/*!
		@brief Manages information of Chunks for store.
	*/
	class BaseMetaChunk {
	public:
		int64_t checkpointPos_;   
		ChunkKey chunkKey_;		  
		uint8_t unoccupiedSize_;  
		bool isAffinityUse_;  

		bool cumulativeDirtyFlag_;	
		bool differentialDirtyFlag_;  
		BaseMetaChunk()
			: checkpointPos_(-1),
			  chunkKey_(MIN_CHUNK_KEY),
			  unoccupiedSize_(UNDEF_UNOCCUPIED_SIZE_),
			  isAffinityUse_(false),
			  cumulativeDirtyFlag_(false),
			  differentialDirtyFlag_(false) {}
		void reuse(uint8_t initialUnoccupiedSize) {
			assert(unoccupiedSize_ = UNDEF_UNOCCUPIED_SIZE_);
			chunkKey_ =
				MIN_CHUNK_KEY;  
			unoccupiedSize_ = initialUnoccupiedSize;
			isAffinityUse_ = false;
		}
		UTIL_FORCEINLINE bool isFree() const {
			return unoccupiedSize_ == UNDEF_UNOCCUPIED_SIZE_;
		}
		UTIL_FORCEINLINE int64_t getCheckpointPos() {
			return checkpointPos_;
		}
		UTIL_FORCEINLINE ChunkKey getChunkKey() {
			return chunkKey_;
		}
		UTIL_FORCEINLINE uint8_t getUnoccupiedSize() {
			return unoccupiedSize_;
		}
		UTIL_FORCEINLINE bool isAffinityUse() {
			return isAffinityUse_;
		}
		UTIL_FORCEINLINE void setCheckpointPos(int64_t checkpointPos) {
			checkpointPos_ = checkpointPos;
		}
		UTIL_FORCEINLINE void setChunkKey(ChunkKey chunkKey) {
			chunkKey_ = chunkKey;
		}
		UTIL_FORCEINLINE void setUnoccupiedSize(uint8_t unoccupiedSize) {
			unoccupiedSize_ = unoccupiedSize;
		}
		UTIL_FORCEINLINE void resetUnoccupiedSize() {
			unoccupiedSize_ = UNDEF_UNOCCUPIED_SIZE_;
		}
		UTIL_FORCEINLINE void setAffinityUsed() {
			isAffinityUse_ = true;
		}
		UTIL_FORCEINLINE void setAffinityUnused() {
			isAffinityUse_ = false;
		}
		UTIL_FORCEINLINE void setCumulativeDirtyFlag(bool flag) {
			cumulativeDirtyFlag_ = flag;
		}
		UTIL_FORCEINLINE bool getCumulativeDirtyFlag() {
			return cumulativeDirtyFlag_;
		}
		UTIL_FORCEINLINE void setDifferentialDirtyFlag(bool flag) {
			differentialDirtyFlag_ = flag;
		}
		UTIL_FORCEINLINE bool getDifferentialDirtyFlag() {
			return differentialDirtyFlag_;
		}
		std::string dump() {
			util::NormalOStringStream stream;
			stream << "BaseMetaChunk"
				   << ",checkpointPos," << checkpointPos_ << ",chunkKey,"
				   << chunkKey_ << ",unoccupiedSize,"
				   << (int32_t)unoccupiedSize_ << ",isAffinityUse,"
				   << isAffinityUse_ << ",cumulativeDirtyFlag,"
				   << cumulativeDirtyFlag_ << ",differentialDirtyFlag,"
				   << differentialDirtyFlag_;
			return stream.str();
		}
	};

	enum BufferInfoState {
		BUFFER_MANAGER_STATE_NONE,
		BUFFER_MANAGER_STATE_DIRTY_BUFFER,
		BUFFER_MANAGER_STATE_DIRTY_FILE,
		BUFFER_MANAGER_STATE_DIRTY_BOTH,
		BUFFER_MANAGER_STATE_RESERVED,
		BUFFER_MANAGER_STATE_CLEAN_BUFFER,
	};

	/*!
		@brief Manages information of Chunks on the buffer and dirty Chunks in
	   the file.
	*/
	class BufferInfo {
		friend class LRU;
		friend class BufferManager;
	private:
		BufferInfo* nextCollision_;
		PartitionId pId_;
		ChunkId cId_;
		uint8_t* buffer_;
		int32_t refCount_;  
		int64_t lastRefId_;  
		BufferInfo* lruNext_;
		BufferInfo* lruPrev_;
		int64_t latestPos_;			
		int64_t newCheckpointPos_;  
		bool isDirty_;				
		bool toCopy_;				
		ChunkCategoryId categoryId_;

	public:
		BufferInfo();
		BufferInfo(PartitionId pId, ChunkCategoryId categoryId, ChunkId cId);
		UTIL_FORCEINLINE bool fix(int64_t refId) {
			assert(0 <= refCount_);
			if (refId != lastRefId_ && 0 < refCount_) {
				GS_TRACE_ERROR(CHUNK_MANAGER, GS_TRACE_CHM_FORCE_UNFIX,
					"refCount forced to reset: refCount must be zero before "
					"fix, but "
						<< refCount_ << ",pId," << pId_ << ",categoryId,"
						<< (int32_t)categoryId_ << ",cId," << cId_);
				refCount_ = 1;
			}
			else {
				refCount_++;
			}
			lastRefId_ = refId;
			return (refCount_ == 1);
		}
		UTIL_FORCEINLINE void fixFirstTime(int64_t refId) {
			assert(0 == refCount_);
			refCount_ = 1;
			lastRefId_ = refId;
		}
		UTIL_FORCEINLINE void unfix() {
			refCount_--;
			assert(0 <= refCount_);
		}
		bool isFixed() const {
			assert(0 <= refCount_);
			return (0 < refCount_);
		}
		BufferInfoState getState();
		bool isValid(BufferInfoState type);
		std::string dump();
	};

	static const char* const ChunkOperationTypeString[];
	enum ChunkState {
		STATE_NONE = 0,
		STATE_DIRTY_BUFFER = 1,
		STATE_DIRTY_FILE = 2,
		STATE_DIRTY_BOTH = 3,
		STATE_CLEAN_FILE = 4,
		STATE_CLEAN_BOTH = 5,
		STATE_NONE_CP = 6,
		STATE_DIRTY_BUFFER_CP = 7,
		STATE_DIRTY_FILE_CP = 8,
		STATE_DIRTY_BOTH_CP = 9,
		STATE_NUM = 10
	};
	typedef std::deque<BaseMetaChunk,
		util::StdAllocator<BaseMetaChunk, PartitionGroupAllocator> >
		ChunkList;
	typedef ChunkList::iterator ChunkListItr;
	/*!
		@brief Manages information of Chunks for each Partition.
	*/
	class PartitionMetaChunk {
		friend class MetaChunkManager;
	private:
		struct MetaChunkStats {
			uint64_t useChunkNum_;
			uint64_t checkpointChunkNum_;
			MetaChunkStats() : useChunkNum_(0), checkpointChunkNum_(0) {}
		};
		const ChunkCategoryId chunkCategoryNum_;
		std::vector<MetaChunkStats> categoryStats_;
		std::vector<ChunkList*> chunkList_;
		std::vector<ChunkCursor> emptyScanCursor_;
		std::vector<ChunkCursor> nextTailCursor_;
		std::vector<ChunkCursor> checkpointTailCursor_;
		ChunkCursor batchFreeCursor_;

	public:
		PartitionMetaChunk(
			ChunkCategoryId chunkCategoryNum, PartitionGroupAllocator& alloc_);
		~PartitionMetaChunk();
	};

	struct PartitionInfo {
		CheckpointId lastCpId_;
		CheckpointId startCpId_;
		uint64_t checkpointNum_;
		uint64_t copyNum_;
		PartitionInfo()
			: lastCpId_(UNDEF_CHECKPOINT_ID),
			  startCpId_(UNDEF_CHECKPOINT_ID),
			  checkpointNum_(0),
			  copyNum_(0)
		{
		}
		~PartitionInfo() {
		}
	};

	/*!
		@brief Chunk ID Table for Affinity
	*/
	struct AffinityChunkIdTable {
		const uint32_t pId_;
		const uint32_t chunkCategoryNum_;
		uint64_t affinitySize_;
		ChunkId** affinityChunkId_;

		void initialize(uint64_t affinitySize, ChunkId**& affinityChunkId);
		void clear(ChunkId**& affinityChunkId);
		AffinityGroupId getIndex(
			uint32_t categoryId, AffinityGroupId affinityValue) const {
			return affinityValue % getAffinitySize(categoryId);
		}
		AffinityChunkIdTable(
			PartitionId pId, uint32_t categotyNum, uint64_t affinitySize);
		~AffinityChunkIdTable();
		void reconstruct(uint64_t newAffinitySize);
		uint64_t getAffinitySize(uint32_t categoryId) const {
			return (categoryId == 0) ? 1 : affinitySize_;
		}  
		ChunkId getAffinityChunkId(
			uint32_t categoryId, AffinityGroupId affinityValue) const {
			return affinityChunkId_[categoryId]
								   [getIndex(categoryId, affinityValue)];
		}
		void setAffinityChunkId(uint32_t categoryId,
			AffinityGroupId affinityValue, ChunkId chunkId) {
			affinityChunkId_[categoryId][getIndex(categoryId, affinityValue)] =
				chunkId;
		}
	};

	/*!
		@brief Data set of a Partition
	*/
	struct PartitionData {
		const PartitionGroupId pgId_;
		uint8_t partitionExistance_;
		PartitionMetaChunk partitionMetaChunk_;
		PartitionInfo partitionInfo_;
		AffinityChunkIdTable affinityChunkIdTable_;
		ChunkCursor syncCursor_;
		PartitionData(
			Config& config, PartitionId pId, PartitionGroupAllocator& allocator)
			: pgId_(config.getPartitionGroupId(pId)),
			  partitionExistance_(NO_PARTITION),
			  partitionMetaChunk_(config.getChunkCategoryNum(), allocator),
			  partitionInfo_(),
			  affinityChunkIdTable_(pId, config.getChunkCategoryNum(),
				  1)  
			  ,
			  syncCursor_() {}
	};
	typedef std::vector<PartitionData*> PartitionDataList;
	typedef PartitionDataList PartitionInfoList;

	/*!
		@brief Manages list of all Chunks of each Partition group.
	*/
	class MetaChunkManager {
		friend class ChunkManager;  
	private:
		/*!
			@brief Stats of MetaChunkManager
		*/
		struct PGStats {
			uint64_t useChunkNum_;
			uint64_t checkpointChunkNum_;
			uint64_t batchFreeCount_;
			uint64_t objectAllocateSize_;
			PGStats()
				: useChunkNum_(0),
				  checkpointChunkNum_(0),
				  batchFreeCount_(0),
				  objectAllocateSize_(0) {}
		};

		static const int32_t MAX_CHUNK_ID = ChunkManager::MAX_CHUNK_ID;
		Config& config_;
		PartitionGroupId pgId_;
		PGStats pgStats_;
		std::vector<PartitionId>& pIdList_;
		PartitionDataList& partitionList_;
		UTIL_FORCEINLINE PartitionMetaChunk& getPartition(PartitionId pId) {
			return partitionList_[pId]->partitionMetaChunk_;
		}
		UTIL_FORCEINLINE ChunkList& getChunkList(
			PartitionId pId, ChunkCategoryId categoryId) {
			return *getPartition(pId).chunkList_[categoryId];
		}
		void contract(PartitionId pId, ChunkCursor& nextTail);
		MetaChunkManager(const MetaChunkManager&);
		MetaChunkManager& operator=(const MetaChunkManager&);

	public:
		MetaChunkManager(PartitionGroupId pgId,
			std::vector<PartitionId>& pIdList, PartitionDataList& partitionList,
			Config& config);
		~MetaChunkManager();
		void expand(PartitionId pId, ChunkCursor& nextTail,
			ChunkId cId = UNDEF_CHUNKID);
		BaseMetaChunk* allocate(
			PartitionId pId, ChunkCategoryId categoryId, ChunkId cId);
		void free(PartitionId pId, ChunkCategoryId categoryId, ChunkId cId);
		BaseMetaChunk* reallocate(
			PartitionId pId, ChunkCategoryId categoryId, ChunkId cId);
		UTIL_FORCEINLINE BaseMetaChunk* getMetaChunk(
			PartitionId pId, ChunkCategoryId categoryId, ChunkId cId) {
			return &getChunkList(pId, categoryId)[cId];
		}
		UTIL_FORCEINLINE ChunkCursor& getEmptyScanCursor(
			PartitionId pId, ChunkCategoryId categoryId) {
			return getPartition(pId).emptyScanCursor_[categoryId];
		}
		UTIL_FORCEINLINE ChunkCursor& getNextTail(
			PartitionId pId, ChunkCategoryId categoryId) {
			return getPartition(pId).nextTailCursor_[categoryId];
		}
		uint64_t getMaxChunkId();
		uint64_t getPGUseChunkNum(ChunkCategoryId categoryId);
		uint64_t getPGUseChunkNum() const;
		uint64_t getPGCheckpointChunkNum() const;
		UTIL_FORCEINLINE void incrementPGBatchFreeCount() {
			pgStats_.batchFreeCount_++;
		}
		uint64_t getPGBatchFreeCount() const;
		void addPGAllocatedObjectSize(int64_t size);
		void subtractPGAllocatedObjectSize(int64_t size);
		uint64_t getPGAllocatedObjectSize() const;
		uint64_t getTotalListElementNum(PartitionId pId);
		ChunkCursor& getBatchFreeCursor(PartitionId pId);
		BaseMetaChunk* begin(PartitionId pId, ChunkCursor& cursor);
		BaseMetaChunk* next(PartitionId pId, ChunkCursor& cursor, bool& isHead);
		BaseMetaChunk* nextForCategory(
			PartitionId pId, ChunkCursor& cursor, bool& isHead);
		bool isValid(PartitionId pId, ChunkCategoryId categoryId);
	};

	/*!
		@brief Manages pool of memory for each Partition group.
	*/
	class SinglePool {
	private:
		uint8_t* pool_;
		MemoryPool& memoryPool_;
		SinglePool(const SinglePool&);
		SinglePool& operator=(const SinglePool&);

	public:
		explicit SinglePool(MemoryPool& memoryPool)
			: pool_(NULL), memoryPool_(memoryPool) {}
		~SinglePool() {
			if (pool_) {
				memoryPool_.deallocate(pool_);
				pool_ = NULL;
			}
		}
		uint8_t* allocate();
		void free(uint8_t* buffer, uint32_t limit);
		uint64_t getPoolNum() const;
	};

	class Compressor {
	public:
		Compressor() : isFirstTime_(true), compressionErrorCount_(0) {
			deflateStream_.zalloc = Z_NULL;
			deflateStream_.zfree = Z_NULL;
			deflateStream_.opaque = Z_NULL;

			inflateStream_.zalloc = Z_NULL;
			inflateStream_.zfree = Z_NULL;

			static const char* currentVersion = ZLIB_VERSION;
			EXEC_FAILURE(GS_ERROR_CHM_INCOMPATIBLE_ZLIB_VERSION);
			if (currentVersion[0] != zlibVersion()[0]) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_CHM_INCOMPATIBLE_ZLIB_VERSION,
					"the zlib library version (zlib_version) is incompatible"
						<< " with the version assumed, " << currentVersion
						<< ", but the library version, " << zlibVersion());
			}
		}
		~Compressor() {}

		uint64_t getCompressionErrorCount() const {
			return compressionErrorCount_;
		}

		void compressData(
			uint8_t* src, uint32_t srcSize, uint8_t* dest, uint32_t& destSize) {
			int32_t flush = Z_FINISH;

			deflateStream_.zalloc = Z_NULL;
			deflateStream_.zfree = Z_NULL;
			deflateStream_.opaque = Z_NULL;

			EXEC_FAILURE(GS_ERROR_CHM_COMPRESSION_FAILED);
			int32_t ret = deflateInit(&deflateStream_, COMPRESS_LEVEL);
			if (ret != Z_OK) {
				if (ret == Z_MEM_ERROR) {
					if (isFirstTime_) {
						GS_TRACE_ERROR(CHUNK_MANAGER,
							GS_TRACE_CHM_COMPRESSION_FAILED,
							"error occured in deflateInit.");
						isFirstTime_ = false;
					}
					destSize = srcSize;
					compressionErrorCount_++;
					return;
				}
				GS_THROW_SYSTEM_ERROR(
					GS_ERROR_CHM_COMPRESSION_FAILED, "deflateInit failed.");
			}

			deflateStream_.avail_in = srcSize;
			deflateStream_.avail_out = destSize;
			deflateStream_.next_in = (Bytef*)src;
			deflateStream_.next_out = (Bytef*)dest;

			do {
				ret = deflate(&deflateStream_, flush);
			} while (ret == Z_OK);

			if (ret != Z_STREAM_END) {
				if (isFirstTime_) {
					GS_TRACE_ERROR(CHUNK_MANAGER,
						GS_TRACE_CHM_COMPRESSION_FAILED,
						"error occured in deflate.");
					isFirstTime_ = false;
				}
				destSize = srcSize;
				compressionErrorCount_++;
				return;
			}
			else {
				destSize = static_cast<uint32_t>(deflateStream_.total_out);
				if (srcSize < destSize) {
					destSize = srcSize;
				}
			}

			ret = deflateEnd(&deflateStream_);
			if (ret != Z_OK) {
				if (isFirstTime_) {
					GS_TRACE_ERROR(CHUNK_MANAGER,
						GS_TRACE_CHM_COMPRESSION_FAILED,
						"error occured in deflateEnd.");
				}
				destSize = srcSize;
				compressionErrorCount_++;
				return;
			}
		}

		void uncompressData(
			uint8_t* src, uint32_t srcSize, uint8_t* dest, uint32_t& destSize) {
			int32_t flush = Z_FINISH;

			inflateStream_.zalloc = Z_NULL;
			inflateStream_.zfree = Z_NULL;
			inflateStream_.opaque = Z_NULL;

			EXEC_FAILURE(GS_ERROR_CHM_UNCOMPRESSION_FAILED);
			if (inflateInit(&inflateStream_) != Z_OK) {
				GS_THROW_SYSTEM_ERROR(
					GS_ERROR_CHM_UNCOMPRESSION_FAILED, "inflateInit failed.");
			}

			inflateStream_.avail_in = srcSize;
			inflateStream_.avail_out = destSize;
			inflateStream_.next_in = (Bytef*)src;
			inflateStream_.next_out = (Bytef*)dest;

			int ret = Z_OK;
			do {
				ret = inflate(&inflateStream_, flush);
			} while (ret == Z_OK);

			if (ret != Z_STREAM_END) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_CHM_UNCOMPRESSION_FAILED,
					" There was not enough memory "
						<< ", or there was not enough room in the output "
						   "buffer. "
						<< " srcSize = " << srcSize
						<< ", desSize = " << destSize << ", ret = " << ret);
			}

			destSize = static_cast<uint32_t>(inflateStream_.total_out);

			if (inflateEnd(&inflateStream_) != Z_OK) {
				GS_THROW_SYSTEM_ERROR(
					GS_ERROR_CHM_UNCOMPRESSION_FAILED, "inflateEnd failed.");
			}
		}

	private:
		static const int32_t COMPRESS_LEVEL = Z_BEST_SPEED;

		Compressor(const Compressor&);
		Compressor& operator=(const Compressor&);
		bool isFirstTime_;
		uint64_t compressionErrorCount_;
		z_stream deflateStream_;
		z_stream inflateStream_;
	};

	class FileManager {
		static util::Atomic<uint32_t> atomicEnableCompression_;
	public:
		FileManager(Config& config, CheckpointFile& checkpointFile)
			: config_(config),
			  checkpointFile_(checkpointFile),
			  compressor_(),
			  holeBlockExpSize_(config.getChunkExpSize()),
			  invalidCompressionCount_(TRACE_COUNTER_UNIT_),
			  invalidCompressionTraceCount_(1),
			  ioTimer_(),
			  chunkExpSize_(config_.getChunkExpSize()),
			  chunkSize_(1UL << config_.getChunkExpSize()),
			  destBuffer_(UTIL_NEW uint8_t[chunkSize_ * 2]) {

			uint64_t fileSystemBlockSize =
				checkpointFile_.getFileSystemBlockSize();

			if (!isValidHoleSize(chunkSize_, fileSystemBlockSize)) {
				GS_TRACE_ERROR(CHUNK_MANAGER, GS_TRACE_CHM_INVALID_PARAMETER,
					"Compression mode is inactive. "
						<< "StoreBlockSize must be at least twice of "
						<< "block size of file system. "
						<< "StoreBlockSize = " << chunkSize_
						<< ", block size of file system = "
						<< fileSystemBlockSize);
				atomicEnableCompression_ = 0;
			}
			else {
				holeBlockExpSize_ = util::nextPowerBitsOf2(
					static_cast<uint32_t>(fileSystemBlockSize));
				GS_TRACE_INFO(CHUNK_MANAGER, GS_TRACE_CHM_CONFIG,
					"Compression mode is active. ");
			}

			if (config_.getCompressionMode() == NO_BLOCK_COMPRESSION) {
				atomicEnableCompression_ = 0;
			}
		}
		~FileManager() {
			delete[] destBuffer_;
			destBuffer_ = NULL;
		}

		bool isValidHoleSize(uint32_t chunkSize, uint64_t fileSystemBlockSize) {
			if (fileSystemBlockSize == SUPPORTED_FILE_SYSTEM_BLOCK_SIZE_) {
				return true;
			}
			return false;
		}


		uint32_t getHoleOffset(uint32_t compressedSize, uint32_t& holeSize) {
			uint32_t unitHoleSize = (1UL << holeBlockExpSize_);
			assert(compressedSize < chunkSize_);

			uint32_t holeOffset = 0;
			if (compressedSize == 0) {
				holeOffset = chunkSize_;
			}
			else if (unitHoleSize <= chunkSize_ - compressedSize) {
				holeSize = (((chunkSize_ - compressedSize) >> holeBlockExpSize_)
							<< holeBlockExpSize_);
				assert(holeSize % unitHoleSize == 0);
				holeOffset = chunkSize_ - holeSize;
			}
			return holeOffset;
		}

		uint32_t compressChunk(uint8_t* buffer, CompressionMode mode) {
			if (atomicEnableCompression_ != 0 && mode == BLOCK_COMPRESSION) {
				uint32_t unitHoleSize = (1UL << holeBlockExpSize_);
				if (ChunkHeader::getCompressedDataSize(buffer) != 0) {
					uint32_t compressedSize =
						ChunkHeader::getCompressedDataSize(buffer);
					uint32_t holeSize = 0;
					return getHoleOffset(compressedSize, holeSize);
				}

				uint8_t* bodyPtr = buffer + CHUNK_HEADER_FULL_SIZE;
				uint32_t srcSize = chunkSize_ - CHUNK_HEADER_FULL_SIZE;
				uint32_t destSize = chunkSize_;  
				compressor_.compressData(
					bodyPtr, srcSize, destBuffer_, destSize);


				uint32_t compressedSize = CHUNK_HEADER_FULL_SIZE + destSize;

				if (unitHoleSize <= chunkSize_ - compressedSize) {
					ChunkHeader::setCompressedDataSize(buffer, compressedSize);
					memcpy(bodyPtr, destBuffer_, destSize);
					uint32_t holeSize = 0;
					uint32_t holeOffset =
						getHoleOffset(compressedSize, holeSize);
					memset(buffer + holeOffset, 0, holeSize);

					return holeOffset;
				}
				else {
					invalidCompressionCount_++;
					if (TRACE_COUNTER_UNIT_ < invalidCompressionCount_) {
						GS_TRACE_INFO(CHUNK_MANAGER,
							GS_TRACE_CHM_INVALID_COMPRESSION,
							"count of invalid compression is over "
								<< invalidCompressionTraceCount_
								<< "(error count = "
								<< compressor_.getCompressionErrorCount()
								<< "). ");
						invalidCompressionTraceCount_ += TRACE_COUNTER_UNIT_;
						invalidCompressionCount_ = 0;
					}
				}
			}
			return chunkSize_;
		}

		void uncompressChunk(uint8_t* buffer) {
			uint32_t compressedSize =
				ChunkHeader::getCompressedDataSize(buffer);
			if (chunkSize_ <= compressedSize) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_CHM_UNCOMPRESSION_FAILED,
					"Invalid compressed block data size.");
			}
			if (0 < compressedSize) {
				uint8_t* bodyPtr = buffer + CHUNK_HEADER_FULL_SIZE;
				uint32_t srcSize = compressedSize - CHUNK_HEADER_FULL_SIZE;
				uint32_t destSize =
					chunkSize_ - CHUNK_HEADER_FULL_SIZE;  
				compressor_.uncompressData(
					bodyPtr, srcSize, destBuffer_, destSize);
				EXEC_FAILURE(GS_ERROR_CHM_UNCOMPRESSION_FAILED);
				if (chunkSize_ != CHUNK_HEADER_FULL_SIZE + destSize) {
					GS_THROW_SYSTEM_ERROR(GS_ERROR_CHM_UNCOMPRESSION_FAILED,
						"Invalid uncompressed block data size.");
				}
				memcpy(buffer + CHUNK_HEADER_FULL_SIZE, destBuffer_, destSize);
				ChunkHeader::setCompressedDataSize(buffer, 0);
			}
		}
		int64_t writeHolePunchingChunk(
			uint8_t* buffer, int64_t writePos, uint32_t holeOffset) {
			uint32_t size = 1;
			iotrace(buffer, size, writePos);

			ioTimer_.reset();
			ioTimer_.start();

			EXEC_FAILURE(GS_ERROR_CHM_IO_FAILED);		  
			EXEC_FAILURE(GS_ERROR_CF_READ_CHUNK_FAILED);  
#ifdef _WIN32
			checkpointFile_.writePartialBlock(
				buffer, chunkSize_, (writePos << chunkExpSize_));
#else
			uint32_t holeSize = chunkSize_ - holeOffset;
			checkpointFile_.writePartialBlock(
				buffer, holeOffset, (writePos << chunkExpSize_));

			try {
				EXEC_FAILURE(GS_ERROR_CHM_IO_FAILED);  
				checkpointFile_.punchHoleBlock(holeSize,
					(writePos << chunkExpSize_) + holeOffset);  
			}
			catch (std::exception& e) {
				GS_TRACE_ERROR(CHUNK_MANAGER, GS_TRACE_CHM_INVALID_PARAMETER,
					"Compression mode is inactive. "
						<< "because file hole punching failed.");

				atomicEnableCompression_ = 0;
				EXEC_FAILURE(GS_ERROR_CHM_IO_FAILED);  
				EXEC_FAILURE(
					GS_ERROR_CF_READ_CHUNK_FAILED);  
				checkpointFile_.writePartialBlock(
					buffer, holeSize, (writePos << chunkExpSize_) + holeOffset);
			}
#endif

			int64_t ioTime = ioTimer_.elapsedNanos() / 1000;
			return ioTime;
		}

		int64_t writeChunk(uint8_t* buffer, uint32_t size, int64_t writePos) {
			iotrace(buffer, size, writePos);

			EXEC_FAILURE(GS_ERROR_CHM_IO_FAILED);

			ioTimer_.reset();
			ioTimer_.start();

			checkpointFile_.writeBlock(buffer, size, writePos);  

			int64_t ioTime = ioTimer_.elapsedNanos() / 1000;

			return ioTime;
		}

		int64_t readChunk(uint8_t* buffer, uint32_t size, int64_t readPos) {
			iotrace(buffer, size, readPos);

			ioTimer_.reset();
			ioTimer_.start();

			EXEC_FAILURE(GS_ERROR_CHM_IO_FAILED);
			checkpointFile_.readBlock(buffer, size, readPos);

			int64_t ioTime = ioTimer_.elapsedNanos() / 1000;

			return ioTime;
		}

		void updateCheckSum(uint8_t* buffer) {
			ChunkHeader::updateCheckSum(buffer);
		}

		void isValidCheckSum(uint8_t* buffer) {
			uint32_t chunkCheckSum = ChunkHeader::getCheckSum(buffer);
			uint32_t calcCheckSum = ChunkHeader::calcCheckSum(buffer);

			EXEC_FAILURE(GS_ERROR_CHM_INVALID_CHUNK_CHECKSUM);

			if (chunkCheckSum != calcCheckSum) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_CHM_INVALID_CHUNK_CHECKSUM,
					"checkSum, "
						<< chunkCheckSum << ", " << calcCheckSum << ","
						<< "pId = " << ChunkHeader::getPartitionId(buffer)
						<< ", categoryId = "
						<< (int32_t)ChunkHeader::getChunkCategoryId(buffer)
						<< ", cId = " << ChunkHeader::getChunkId(buffer)
						<< ", pgId = "
						<< ChunkHeader::getPartitionGroupId(buffer));
			}
		}

		static CompressionMode isEnableCompression() {
			if (atomicEnableCompression_ == 0) {
				return NO_BLOCK_COMPRESSION;
			}
			else {
				return BLOCK_COMPRESSION;
			}
		}

	private:
		static const uint32_t SUPPORTED_FILE_SYSTEM_BLOCK_SIZE_ = 4UL * 1024;
		static const uint64_t TRACE_COUNTER_UNIT_ = 1000;
		Config& config_;
		CheckpointFile& checkpointFile_;
		Compressor compressor_;
		uint32_t holeBlockExpSize_;
		uint64_t invalidCompressionCount_;
		uint64_t invalidCompressionTraceCount_;
		util::Stopwatch ioTimer_;
		const uint32_t chunkExpSize_;
		const uint32_t chunkSize_;
		uint8_t* destBuffer_;

		uint32_t getHoleBlockExpSize() const {
			return holeBlockExpSize_;
		}

		void iotrace(uint8_t* buffer, uint32_t size, int64_t pos) {
			GS_TRACE_INFO(CHUNK_MANAGER_DETAIL, GS_TRACE_CHM_INTERNAL_INFO,
				"pId = " << ChunkHeader::getPartitionId(buffer)
						 << ", categoryId = "
						 << (int32_t)ChunkHeader::getChunkCategoryId(buffer)
						 << ", cId = " << ChunkHeader::getChunkId(buffer)
						 << ", attribute = "
						 << (int32_t)ChunkHeader::getAttribute(buffer)
						 << ", chunkKey = " << ChunkHeader::getChunkKey(buffer)
						 << ", pgId = "
						 << ChunkHeader::getPartitionGroupId(buffer)
						 << ", pos = " << pos << ", size = " << size);
		}

		FileManager(const FileManager&);
		FileManager& operator=(const FileManager&);
	};

	/*!
		@brief Buffer for checkpoint
	*/
	struct CheckpointBuffer {
		uint8_t* buffer_;
		int64_t checkpointPos_;
		CheckpointBuffer() : buffer_(NULL), checkpointPos_(-1) {}
	};

	/*!
		@brief Manages store checkpoint for each Partition group.
	*/
	class CheckpointManager {
	private:
		typedef std::deque<ChunkId,
			util::StdAllocator<ChunkId, PartitionGroupAllocator> >
			CopyList;
		typedef CopyList::iterator CopyListItr;
		typedef std::deque<CheckpointBuffer,
			util::StdAllocator<CheckpointBuffer, PartitionGroupAllocator> >
			WriteList;
		typedef WriteList::iterator WriteListItr;
		typedef std::deque<int64_t,
			util::StdAllocator<int64_t, PartitionGroupAllocator> >
			CheckpointPosList;

		struct PGStats {
			uint64_t checkpointCount_;
			uint64_t checkpointWriteCount_;
			uint64_t checkpointWriteTime_;
			uint64_t chunkSyncReadCount_;
			uint64_t chunkSyncReadTime_;
			uint64_t recoveryReadCount_;
			uint64_t recoveryReadTime_;
			uint64_t backupReadCount_;
			uint64_t backupReadTime_;
			PGStats()
				: checkpointCount_(0),
				  checkpointWriteCount_(0),
				  checkpointWriteTime_(0),
				  chunkSyncReadCount_(0),
				  chunkSyncReadTime_(0),
				  recoveryReadCount_(0),
				  recoveryReadTime_(0),
				  backupReadCount_(0),
				  backupReadTime_(0) {}
		};

		/*!
			@brief Writes checkpoint Chunks of each Partition.
		*/
		class CPWriter {
		private:
			uint64_t checkpointBufferNum_;
			uint64_t
				ptLeftCopyNum_;  
			WriteList ptCheckpointBuffer_;  
			util::Mutex mutex_;

		public:
			explicit CPWriter(PartitionGroupAllocator& multiThreadAllocator);
			~CPWriter();
			void atomicIncrementCopySize();
			void atomicDecrementCopySize();
			uint64_t atomicGetSize();
			void atomicResetCopySize();
			void atomicPushQueue(CheckpointBuffer& checkpointBuffer);
			bool atomicFrontQueue(CheckpointBuffer& checkpointBuffer);
			bool atomicPopQueue(CheckpointBuffer& checkpointBuffer);
			uint64_t getCheckpointBufferNum() const;
		};

		const PartitionGroupId pgId_;
		PGStats pgStats_;
		SinglePool pgPool_;  
		util::Mutex
			pgPoolMutext_;  
		CheckpointId pgStartCpId_;
		CheckpointId pgEndCpId_;
		BitArray pgCheckpointBitArray_;
		BitArray pgFreeBitArray_;
		PartitionId pId_;
		PartitionInfoList& partitionInfo_;
		ChunkCategoryId
			categoryCursor_;  
		std::vector<CopyList*> ptCopyList_;
		CPWriter cpWriter_;  
		PartitionId syncPId_;
		CheckpointPosList checkpointPosList_;
		CheckpointFile& checkpointFile_;  
		Config& config_;
		FileManager fileManagerCPThread_;   
		FileManager fileManagerTxnThread_;  

		CopyList& getCopyList(ChunkCategoryId categoryId) const {
			return *ptCopyList_[categoryId];
		}
		PartitionInfo& getPartitionInfo(PartitionId pId) {
			assert(config_.getPartitionGroupId(pId) == pgId_);
			return partitionInfo_[pId]->partitionInfo_;
		}
		CheckpointFile& getCheckpointFile() {
			return checkpointFile_;
		}
		CheckpointManager(const CheckpointManager&);
		CheckpointManager& operator=(const CheckpointManager&);

	public:
		CheckpointManager(PartitionGroupId pgId,
			PartitionGroupAllocator& allocator,
			PartitionGroupAllocator& multiThreadAllocator,
			PartitionInfoList& partitionInfo, CheckpointFile& checkpointFile,
			MemoryPool& memoryPool, Config& config);
		~CheckpointManager();
		void setPGCheckpointId(CheckpointId cpId);
		CheckpointId getPGCompletedCheckpointId() const;
		void switchPGCheckpointBit(CheckpointId cpId);
		BitArray& getPGCheckpointBit();
		void setPGCheckpointBit(const uint8_t* bitList, const uint64_t bitNum);
		void flushPGFile();
		void clearPG();
		void appendNewCheckpointChunkId(PartitionId pId,
			ChunkCategoryId categoryId, ChunkId cId, int64_t oldPos,
			int64_t newPos, bool toWrite = false);
		void removeOldCheckpointChunkId(PartitionId pId, int64_t oldPos);
		bool isCopyLeft(PartitionId pId);
		bool getCopyChunkId(
			PartitionId pId, ChunkCategoryId& categoryId, ChunkId& cId);
		void copyChunk(PartitionId pId, ChunkCategoryId categoryId, ChunkId cId,
			uint8_t* buffer, uint64_t checkpointPos);
		bool writeChunk(PartitionId pId);
		void clearPartition();

		void readSyncChunk(int64_t checkpointPos, uint8_t* buffer);
		bool readRecoveryChunk(uint8_t* buffer);


		void clearCheckpointPosList();
		bool clearCheckpointPosList(PartitionId pId);
		void appendCheckpointPosList(PartitionId pId, int64_t checkpointPos);

		bool getCheckpointPosList(int64_t& checkpointPos);
		UTIL_FORCEINLINE PartitionId getSyncPId() {
			return syncPId_;
		}
		UTIL_FORCEINLINE bool isExecutingCheckpoint(PartitionId pId) {
			assert(pgId_ == config_.getPartitionGroupId(pId));

			if (pgStartCpId_ == pgEndCpId_) {
				return false;
			}
			else {
				assert(pgEndCpId_ == -1 || pgEndCpId_ < pgStartCpId_);

				CheckpointBuffer checkpointBuffer;
				if (getPartitionInfo(pId).startCpId_ == pgEndCpId_) {
					return false;
				}
				else {
					assert(pgEndCpId_ == -1 ||
						   pgEndCpId_ < getPartitionInfo(pId).startCpId_);
					return true;
				}
			}
		}
		UTIL_FORCEINLINE uint64_t getSyncChunkNum() {
			return checkpointPosList_.size();
		}
		UTIL_FORCEINLINE void incrementCheckpointNum(PartitionId pId) {
			getPartitionInfo(pId).checkpointNum_++;
		}
		UTIL_FORCEINLINE uint64_t getCheckpointNum(PartitionId pId) {
			assert(pId != syncPId_ ||
				   checkpointPosList_.size() ==
					   getPartitionInfo(pId).checkpointNum_);
			return getPartitionInfo(pId).checkpointNum_;
		}
		UTIL_FORCEINLINE uint64_t getWriteNum(PartitionId pId) {
			return getPartitionInfo(pId).copyNum_;
		}
		UTIL_FORCEINLINE uint64_t getPGCheckpointCount() const {
			return pgStats_.checkpointCount_;
		}
		UTIL_FORCEINLINE uint64_t getPGCheckpointBufferNum() const {
			return cpWriter_.getCheckpointBufferNum();
		}
		UTIL_FORCEINLINE uint64_t getPGCheckpointWriteCount() const {
			return pgStats_.checkpointWriteCount_;
		}
		UTIL_FORCEINLINE uint64_t getPGCheckpointWriteTime() const {
			return pgStats_.checkpointWriteTime_;
		}
		UTIL_FORCEINLINE uint64_t getPGChunkSyncReadCount() const {
			return pgStats_.chunkSyncReadCount_;
		}
		UTIL_FORCEINLINE uint64_t getPGChunkSyncReadTime() const {
			return pgStats_.chunkSyncReadTime_;
		}
		UTIL_FORCEINLINE uint64_t getPGRecoveryReadCount() const {
			return pgStats_.recoveryReadCount_;
		}
		UTIL_FORCEINLINE uint64_t getPGRecoveryReadTime() const {
			return pgStats_.recoveryReadTime_;
		}
		UTIL_FORCEINLINE uint64_t getPGFileNum() {
			const CheckpointFile& checkpointFile = getCheckpointFile();
			uint64_t fileNum = checkpointFile.getBlockNum();
			return fileNum;
		}
		UTIL_FORCEINLINE uint64_t getPGFileUseNum() {
			const CheckpointFile& checkpointFile = getCheckpointFile();
			uint64_t fileUseNum = checkpointFile.getUseBitNum();
			return fileUseNum;
		}
		UTIL_FORCEINLINE uint64_t getPGFileSize() {
			CheckpointFile& checkpointFile = getCheckpointFile();
			uint64_t fileSize = checkpointFile.getFileSize();
			return fileSize;
		}
		UTIL_FORCEINLINE uint64_t getPGFileAllocateSize() {
			CheckpointFile& checkpointFile = getCheckpointFile();
			uint64_t fileAllocateSize = checkpointFile.getFileAllocateSize();
			return fileAllocateSize;
		}
	};

	/*!
		@brief Manages buffers of Chunks for each Partition group.
	*/
	class BufferManager {
		friend class ChunkManager;  

		typedef util::BArray<BufferInfo*,
			util::StdAllocator<BufferInfo*, ArrayAllocator> >
			BufferInfoList;

	private:
		typedef uint64_t BufferId;
		static const uint32_t CATEGORY_ID_SHIFT_BIT = 32;
		static const uint32_t UNIT_PID_SHIFT_BIT = 32 + 3;
		static BufferId getBufferId(
			PartitionId pId, ChunkCategoryId categoryId, ChunkId cId) {
			assert(pId <= 10000);
			assert(categoryId < 5);
			assert(cId <= MAX_CHUNKID);
			BufferId chunkIdBufferId = ((BufferId)cId);
			BufferId categoryIdBufferId =
				((BufferId)categoryId << CATEGORY_ID_SHIFT_BIT);
			BufferId pIdBufferId = ((BufferId)pId << UNIT_PID_SHIFT_BIT);
			return (pIdBufferId | categoryIdBufferId | chunkIdBufferId);
		}
		static BufferId getBufferId(BufferInfo* bufferInfo) {
			return getBufferId(
				bufferInfo->pId_, bufferInfo->categoryId_, bufferInfo->cId_);
		}

		/*!
			@brief Stats of BufferManager for each Partition group
		*/
		struct PGStats {
			/*!
				@brief Stats of BufferManager for each category
			*/
			struct CategoryStats {
				uint64_t useBufferNum_;
				uint64_t swapWriteCount_;
				uint64_t swapReadCount_;
				CategoryStats()
					: useBufferNum_(0), swapWriteCount_(0), swapReadCount_(0) {}
			};
			CategoryStats* categoryStats_;
			uint64_t useBufferNum_;
			uint64_t swapWriteCount_;
			uint64_t swapWriteTime_;
			util::Atomic<uint64_t> atomicSwapReadCount_;
			uint64_t swapReadTime_;
			uint64_t enwarmReadCount_;
			uint64_t enwarmReadTime_;
			PGStats(ChunkCategoryId categoryNum);
			~PGStats();
		};

		/*!
			@brief Manages set of buffers in LRU.
		*/
		class LRU {
		private:
			uint64_t elementNum_;
			BufferInfo* head_;
			BufferInfo* tail_;
			BufferInfo* cursor_;  
			LRU(const LRU&);
			LRU& operator=(const LRU&);

		public:
			LRU();
			~LRU();
			BufferInfo* front() {
				assert(!head_ || head_->lruPrev_ == NULL);
				assert(!head_ || head_->lruNext_ != NULL || head_ == tail_);
				return head_;
			}
			void pop() {
				assert(head_->buffer_);  
				BufferInfo* popChunk = head_;
				BufferInfo* nextChunk = popChunk->lruNext_;

				if (tail_ == popChunk) {
					tail_ = NULL;
				}
				if (nextChunk) {
					assert(tail_ != NULL);
					nextChunk->lruPrev_ = NULL;
				}
				head_ = nextChunk;
				popChunk->lruNext_ = NULL;
				popChunk->lruPrev_ = NULL;
				cursor_ = NULL;
				assert(0 < elementNum_);
				elementNum_--;
				assert(isValid());
			}
			UTIL_FORCEINLINE void push_back(BufferInfo& bufferInfo) {
				assert(bufferInfo.buffer_);  
				assert(bufferInfo.lruPrev_ == NULL);
				assert(bufferInfo.lruNext_ == NULL);
				assert(bufferInfo.refCount_ == 0);

				assert((head_ != NULL) ||
					   (head_ == NULL && tail_ == NULL && elementNum_ == NULL));

				if (!head_) {
					assert(elementNum_ == 0);
					head_ = &bufferInfo;
				}
				bufferInfo.lruPrev_ = tail_;
				bufferInfo.lruNext_ = NULL;
				if (tail_) {
					tail_->lruNext_ = &bufferInfo;
				}
				tail_ = &bufferInfo;
				elementNum_++;
				cursor_ = NULL;
				assert(isValid());
				assert((&bufferInfo == head_ && &bufferInfo == tail_ &&
						   bufferInfo.lruPrev_ == NULL &&
						   bufferInfo.lruNext_ == NULL) ||
					   (&bufferInfo == head_ && &bufferInfo != tail_ &&
						   bufferInfo.lruPrev_ == NULL &&
						   bufferInfo.lruNext_ != NULL) ||
					   (&bufferInfo != head_ && &bufferInfo != tail_ &&
						   bufferInfo.lruPrev_ != NULL &&
						   bufferInfo.lruNext_ != NULL) ||
					   (&bufferInfo != head_ && &bufferInfo == tail_ &&
						   bufferInfo.lruPrev_ != NULL &&
						   bufferInfo.lruNext_ == NULL));
			}
			UTIL_FORCEINLINE void remove(BufferInfo& bufferInfo) {
				assert(bufferInfo.buffer_);  
				BufferInfo* prev = bufferInfo.lruPrev_;
				BufferInfo* next = bufferInfo.lruNext_;

				if (!prev && !next && (!head_ || (head_ != &bufferInfo))) {
					return;
				}

				assert(bufferInfo.lruPrev_ != NULL ||
					   bufferInfo.lruNext_ != NULL ||
					   (head_ == tail_ && head_ == &bufferInfo));

				if (prev) {
					prev->lruNext_ = next;
				}
				else {
					head_ = next;
				}
				if (next) {
					next->lruPrev_ = prev;
				}
				else {
					tail_ = prev;
				}
				bufferInfo.lruNext_ = NULL;
				bufferInfo.lruPrev_ = NULL;
				assert(0 < elementNum_);
				elementNum_--;
				cursor_ = NULL;
				assert((head_ == NULL && tail_ == NULL && elementNum_ == 0) ||
					   (head_ != NULL && tail_ != NULL && head_ == tail_ &&
						   head_->lruNext_ == NULL && head_->lruPrev_ == NULL &&
						   elementNum_ == 1) ||
					   (head_ != NULL && tail_ != NULL && head_ != tail_ &&
						   head_->lruNext_ == tail_ &&
						   tail_->lruPrev_ == head_ && elementNum_ == 2) ||
					   (head_ != NULL && tail_ != NULL && head_ != tail_ &&
						   head_->lruNext_ != tail_ &&
						   tail_->lruPrev_ != head_ && 3 <= elementNum_));
				assert(isValid());
			}
			uint64_t getElementNum() const {
				return elementNum_;
			}
			BufferInfo* beginCursor() {
				cursor_ = head_;
				return nextCursor();
			}
			BufferInfo* nextCursor() {
				if (cursor_) {
					BufferInfo* bufferInfo = cursor_;
					cursor_ = cursor_->lruNext_;
					return bufferInfo;
				}
				return NULL;
			}
			bool isValid();
		private:
		};

		static const uint64_t MINIMUM_BUFFER_INFO_LIST_SIZE =
			1ULL << 21;  
		static uint64_t getHashTableSize(PartitionGroupId partitionGroupNum,
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
					assert(
						0 <
						totalAtomicMemoryLimitNum);  
					hashTableBit =
						util::nextPowerBitsOf2(totalAtomicMemoryLimitNum - 1) +
						2;
				}
				else {
					assert(
						(totalAtomicMemoryLimitNum - 1) >>
						31);  
					hashTableBit = util::nextPowerBitsOf2(static_cast<uint32_t>(
									   (totalAtomicMemoryLimitNum - 1) >> 31)) +
								   32 + 2;
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

			uint64_t preHashTableSize = 1 +
										(totalAtomicMemoryLimitNum - 1) *
											(2 * exponentialPG) /
											partitionGroupNum;

			uint64_t hashTableBit = 0;
			if (preHashTableSize < UINT32_MAX) {
				assert(
					0 <
					preHashTableSize);  
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
				assert(
					0 <
					MINIMUM_BUFFER_INFO_LIST_SIZE /
						partitionGroupNum);  
				hashTableBit = util::nextPowerBitsOf2(
					MINIMUM_BUFFER_INFO_LIST_SIZE / partitionGroupNum);
				hashTableSize = 1ULL << hashTableBit;
				assert(MINIMUM_BUFFER_INFO_LIST_SIZE <=
					   hashTableSize * partitionGroupNum);
			}

			return hashTableSize;
		}

		class ChainingHashTableCursor;

		/*!
			@brief Manages set of buffers in hash.
		*/
		class ChainingHashTable {
			friend class ChainingHashTableCursor;
			typedef uint64_t HashValue;

		public:
			ChainingHashTable(size_t size, ArrayAllocator& allocator)
				: maxSize_(size),
				  tableMask_(maxSize_ - 1),
				  table_(allocator)
			{
				BufferInfo* initialBufferInfoPtr = NULL;
				table_.assign(maxSize_, initialBufferInfoPtr);
			}
			~ChainingHashTable() {
				table_.clear();
			}
			void append(BufferId bufferId, BufferInfo* bufferInfo) {
				assert(bufferInfo);
				HashValue hashValue = calcKey(bufferId);
				bufferInfo->nextCollision_ = table_[hashValue];
				table_[hashValue] = bufferInfo;
			};
			BufferInfo* get(BufferId bufferId) {
				HashValue hashValue = calcKey(bufferId);
				BufferInfo* bufferInfo = table_[hashValue];
				while (bufferInfo) {
					if (getBufferId(bufferInfo) == bufferId) {
						return bufferInfo;
					}
					bufferInfo = bufferInfo->nextCollision_;
				}
				return NULL;
			};
			BufferInfo* remove(BufferId bufferId) {
				HashValue hashValue = calcKey(bufferId);
				BufferInfo* prevBufferInfo = NULL;
				BufferInfo* bufferInfo = table_[hashValue];
				while (bufferInfo) {
					if (getBufferId(bufferInfo) == bufferId) {
						if (prevBufferInfo) {
							prevBufferInfo->nextCollision_ =
								bufferInfo->nextCollision_;
						}
						else {
							table_[hashValue] = bufferInfo->nextCollision_;
						}
						return bufferInfo;
					}
					prevBufferInfo = bufferInfo;
					bufferInfo = bufferInfo->nextCollision_;
				}
				return NULL;
			};
		private:
			static const uint32_t SEED_ = 0;
			const uint64_t maxSize_;
			const uint64_t tableMask_;
			BufferInfoList table_;
			ChainingHashTable(const ChainingHashTable&);
			ChainingHashTable& operator=(const ChainingHashTable&);

			static const uint64_t FNV_OFFSET_BASIS_64 = 14695981039346656037ULL;
			static const uint64_t FNV_PRIME_64 = 1099511628211ULL;
			UTIL_FORCEINLINE HashValue fnvHash(uint8_t* bytes, size_t length) {
				uint64_t hash = FNV_OFFSET_BASIS_64;
				for (size_t i = 0; i < length; ++i) {
					hash = (FNV_PRIME_64 * hash) ^ (bytes[i]);
				}

				return hash;
			}
			UTIL_FORCEINLINE HashValue calcKey(BufferId bufferId) {
				HashValue hashValue = fnvHash(
					reinterpret_cast<uint8_t*>(&bufferId), sizeof(BufferId));
				hashValue &= tableMask_;
				assert(hashValue < maxSize_);
				return hashValue;
			}
		};

		class ChainingHashTableCursor {
		public:
			explicit ChainingHashTableCursor(ChainingHashTable& hashTable)
				: hashTable_(hashTable),
				  maxSize_(static_cast<int64_t>(hashTable_.maxSize_)),
				  index_(-1),
				  last_(NULL) {}

			bool next(BufferInfo*& bufferInfo) {
				if (last_ && last_->nextCollision_) {
					bufferInfo = last_->nextCollision_;
					last_ = bufferInfo;
					return true;
				}

				while (++index_ < maxSize_) {
					bufferInfo = hashTable_.table_[index_];
					if (bufferInfo) {
						last_ = bufferInfo;
						return true;
					}
				}

				return false;
			}

			void remove(BufferInfo* bufferInfo) {
				if (last_) {
					bufferInfo = hashTable_.remove(getBufferId(last_));
					last_ = NULL;
					index_--;
				}
				else {
					bufferInfo = NULL;
				}
			}

		private:
			ChainingHashTable& hashTable_;
			const int64_t maxSize_;
			int64_t index_;
			BufferInfo* last_;
		};

		typedef ChainingHashTable HashTable;

		PartitionGroupId pgId_;
		Config& config_;
		util::Atomic<int32_t> atomicMemoryLimitNum_;  
		PartitionInfoList& partitionInfo_;
		PGStats pgStats_;
		SinglePool pgPool_;  
		ArrayAllocator bufferInfoAllocator_;
		HashTable bufferInfoList_;
		util::ObjectPool<BufferInfo> bufferInfoPool_;
		LRU swapList_;
		int64_t refId_;  
		PartitionId
			currentCPPId_;  
		CheckpointFile& checkpointFile_;  
		FileManager fileManager_;
		CheckpointFile& getCheckpointFile() {
			return checkpointFile_;
		}
		PartitionInfo& getPartitionInfo(PartitionId pId) {
			assert(config_.getPartitionGroupId(pId) == pgId_);
			return partitionInfo_[pId]->partitionInfo_;
		}
		UTIL_FORCEINLINE uint64_t getSwapNum(uint64_t limit, uint64_t use) {
			uint64_t swapNum = 0;
			if (limit < (use + 1)) {
				swapNum += ((use + 1) - limit);
			}
			return swapNum;
		}
		BufferInfo* getSwapChunk(
			PartitionId& pId, ChunkCategoryId& categoryId, ChunkId& cId);
		void swapOut(PartitionId pId, ChunkCategoryId categoryId, ChunkId cId,
			BufferInfo* bufferInfo);
		void updateChunkCheckpointId(
			CheckpointId last, CheckpointId current, uint8_t* buffer);
		BufferManager(const BufferManager&);
		BufferManager& operator=(const BufferManager&);

	public:
		BufferManager(PartitionGroupId pgId, PartitionInfoList& partitionInfo,
			CheckpointFile& checkpointFile, MemoryPool& memoryPool,
			Config& config);
		~BufferManager();
		void adjustStoreMemory();
		void resetRefCounter();
		int32_t getRefCounter(
			PartitionId pId, ChunkCategoryId categoryId, ChunkId cId);
		bool fix(PartitionId pId, ChunkCategoryId categoryId, ChunkId cId);
		bool fix(BufferInfo* bufferInfo);
		bool unfix(PartitionId pId, ChunkCategoryId categoryId, ChunkId cId);
		bool isFixed(PartitionId pId, ChunkCategoryId categoryId, ChunkId cId);
		uint8_t* allocate(PartitionId pId, ChunkCategoryId categoryId,
			ChunkId cId, BufferInfo*& bufferInfo);
		void free(PartitionId pId, ChunkCategoryId categoryId, ChunkId cId,
			int64_t checkpointPos, uint8_t*& checkpointBuffer);
		uint8_t* getForUpdate(PartitionId pId, ChunkCategoryId categoryId,
			ChunkId cId, int64_t checkpointPos, bool doFix,
			uint8_t*& checkpointBuffer, BufferInfo*& bufferInfo);
		uint8_t* get(PartitionId pId, ChunkCategoryId categoryId, ChunkId cId,
			int64_t checkpointPos, BufferInfo*& bufferInfo);
		void freeCheckpointBuffer(uint8_t* checkpointBuffer);
		bool startCheckpoint(PartitionId pId, ChunkCategoryId categoryId,
			ChunkId cId, int64_t& swapPos);
		void endCheckpoint();
		void getCheckpointBuffer(PartitionId pId, ChunkCategoryId categoryId,
			ChunkId cId, int64_t& checkpointPos, uint8_t*& checkpointBuffer);
		bool enwarm(PartitionId pId, ChunkCategoryId categoryId, ChunkId cId,
			int64_t checkpointPos);
		bool isOnBuffer(
			PartitionId pId, ChunkCategoryId categoryId, ChunkId cId);
		UTIL_FORCEINLINE uint64_t getAtomicPGMemoryLimitNum() const {
			assert(0 <= atomicMemoryLimitNum_);
			return atomicMemoryLimitNum_;
		}
		UTIL_FORCEINLINE void setAtomicPGMemoryLimitNum(uint64_t num) {
			atomicMemoryLimitNum_ = static_cast<int32_t>(num);
		}
		UTIL_FORCEINLINE uint64_t getPGUseBufferNum(
			ChunkCategoryId categoryId) const {
			return pgStats_.categoryStats_[categoryId].useBufferNum_;
		}
		UTIL_FORCEINLINE uint64_t getPGUseBufferNum() const {
			return pgStats_.useBufferNum_;
		}
		UTIL_FORCEINLINE uint64_t getPGPoolBufferNum() const {
			return pgPool_.getPoolNum();
		}
		UTIL_FORCEINLINE uint64_t getPGSwapWriteCount(
			ChunkCategoryId categoryId) const {
			return pgStats_.categoryStats_[categoryId].swapWriteCount_;
		}
		UTIL_FORCEINLINE uint64_t getPGSwapWriteCount() const {
			return pgStats_.swapWriteCount_;
		}
		UTIL_FORCEINLINE uint64_t getPGSwapWriteTime() const {
			return pgStats_.swapWriteTime_;
		}
		UTIL_FORCEINLINE uint64_t getPGSwapReadCount(
			ChunkCategoryId categoryId) const {
			return pgStats_.categoryStats_[categoryId].swapReadCount_;
		}
		UTIL_FORCEINLINE uint64_t getAtomicPGSwapReadCount() const {
			return pgStats_.atomicSwapReadCount_;
		}
		UTIL_FORCEINLINE uint64_t getPGSwapReadTime() const {
			return pgStats_.swapReadTime_;
		}
		UTIL_FORCEINLINE uint64_t getPGRecoveryReadCount() const {
			return pgStats_.enwarmReadCount_;
		}
		UTIL_FORCEINLINE uint64_t getPGRecoveryReadTime() const {
			return pgStats_.enwarmReadTime_;
		}
		UTIL_FORCEINLINE uint64_t getPGBackupReadCount() const;
		UTIL_FORCEINLINE uint64_t getPGBackupReadTime() const;
		std::string dumpDirtyState(PartitionId pId, ChunkCategoryId categoryId,
			ChunkId cId, BufferInfo* bufferInfo);


	};

	/*!
		@brief Balances StoreMemoryLimit of each Partition group, according to
	   SwapRead.
	*/
	class MemoryLimitManager {
		friend class MemoryLimitManagerTest;
		friend class ObjectManagerTest;

	public:
		static const PartitionGroupId DISTRIBUTOR_PARTITION_GROUP_ID_ = 0;

	private:
		/*!
			@brief Information of loads of each Partition group
		*/
		struct Load {
			uint64_t totalLoad_;
			uint64_t avgLoad_;
			uint64_t* lastLoad_;  
			uint64_t* load_;	  
			uint64_t*
				readCount_;  
			Load(PartitionGroupId partitionGroupNum)
				: totalLoad_(0),
				  avgLoad_(0),
				  lastLoad_(NULL),
				  load_(NULL),
				  readCount_(NULL) {
				try {
					lastLoad_ = UTIL_NEW uint64_t[partitionGroupNum];
					load_ = UTIL_NEW uint64_t[partitionGroupNum];
					readCount_ = UTIL_NEW uint64_t[partitionGroupNum];
					for (PartitionGroupId pgId = 0; pgId < partitionGroupNum;
						 pgId++) {
						lastLoad_[pgId] = 0;
						load_[pgId] = 0;
						readCount_[pgId] = 0;
					}
				}
				catch (std::exception& e) {
					GS_RETHROW_SYSTEM_ERROR(e, "partitionGroupNum,"
												   << partitionGroupNum
												   << GS_EXCEPTION_MESSAGE(e));
				}
			}
			~Load() {
				delete[] lastLoad_;
				lastLoad_ = NULL;
				delete[] load_;
				load_ = NULL;
				delete[] readCount_;
				readCount_ = NULL;
			}
		};
		MemoryLimitManager(const MemoryLimitManager&);
		MemoryLimitManager& operator=(const MemoryLimitManager&);
		static const uint32_t SHIFTABLE_MEMORY_RATIO =
			80;  
		static const uint32_t ONCE_SHIFTABLE_MEMORY_RATIO =
			1;  
		const PartitionGroupId partitionGroupNum_;
		const uint64_t minTotalLimitNum_;  
		const uint64_t maxShiftNum_;  
		bool isLastShifted_;
		uint64_t lastTotalLimitNum_;
		PartitionGroupId delPgIdCursor_;
		PartitionGroupId addPgIdCursor_;
		Load load_;
		uint64_t* limitNum_;  
		Config& config_;
		PartitionGroupDataList& partitionGroupData_;
		MemoryPool& memoryPool_;
		BufferManager& getPGBufferManager(PartitionGroupId pgId) {
			return partitionGroupData_[pgId]->bufferManager_;
		}
		void getCurrentLoad(Load& load);
		bool isLoadWorse(const Load& load, uint64_t avgNum,
			PartitionGroupId& delPgId, PartitionGroupId& addPgId);
		bool getShiftablePartitionGroup(const Load& load, uint64_t avgNum,
			uint64_t minNum, uint64_t shiftNum, PartitionGroupId& delPgId,
			PartitionGroupId& addPgId);

	public:
		MemoryLimitManager(Config& config,
			PartitionGroupDataList& partitionGroupData, MemoryPool& memoryPool);
		~MemoryLimitManager();
		void redistributeMemoryLimit(PartitionGroupId pgId);
		bool isValid() const;
		std::string dump() const;
	};

	/*!
		@brief Manages affinity configuration of selecting Chunks for data
	   appending.
	*/
	class AffinityManager {
	private:
		const uint32_t chunkCategoryNum_;
		const uint64_t maxAffinitySize_;  
		PartitionDataList& partitionData_;
		AffinityChunkIdTable& getAffinityChunkIdTable(PartitionId pId) const {
			return partitionData_[pId]->affinityChunkIdTable_;
		}
		uint64_t calculateAffinitySize(uint64_t storeLimitNum,
			uint32_t existsPartitonNum,
			uint64_t userSpecifiedAffinitySize) const;
		AffinityManager(const AffinityManager&);
		AffinityManager& operator=(const AffinityManager&);

	public:
		static const uint64_t MAX_AFFINITY_SIZE;  
		AffinityManager(int32_t chunkCategoryNum, uint64_t maxAffinitySize,
			PartitionDataList& partitionData);
		~AffinityManager();
		void reconstruct(PartitionId pId, uint64_t storeLimitNum,
			uint32_t existsPartitonNum, uint64_t userSpecifiedAffinitySize,
			bool forceUserSpecifiedSize = false);
		void drop(PartitionId pId);
		ChunkId getAffinityChunkId(PartitionId pId, ChunkCategoryId categoryId,
			AffinityGroupId affinityValue) const;
		void setAffinityChunkId(PartitionId pId, ChunkCategoryId categoryId,
			AffinityGroupId affinityValue, ChunkId cId,
			ChunkId& oldAffinityCId);
	};

	enum PartitionExistanceStatus { NO_PARTITION = 0, EXIST_PARTITION };

	typedef std::vector<PartitionId> ActivePartitionList;
	/*!
		@brief Data set of a Partition group
	*/
	struct PartitionGroupData {
		ActivePartitionList pIdList_;
		PartitionGroupAllocator allocator_;
		PartitionGroupAllocator multiThreadAllocator_;
		CheckpointFile checkpointFile_;
		MetaChunkManager metaChunkManager_;
		MetaChunk metaChunk_;
		CheckpointManager checkpointManager_;
		BufferManager bufferManager_;
		AffinityManager affinityManager_;
		PartitionGroupData(Config& config, PartitionGroupId pgId,
			const std::string& dir, MemoryPool& checkpointMemoryPool,
			MemoryPool& storeMemoryPool, PartitionDataList& partitionDataList);
	};

	const ConfigTable& configTable_;
	Config config_;					   
	MemoryPool storeMemoryPool_;	   
	MemoryPool checkpointMemoryPool_;  
	util::Atomic<uint32_t>
		atomicExistPartitionNum_;							 
	std::vector<ChunkCategoryAttribute> categoryAttribute_;  
	std::vector<ChunkCategoryId> batchFreeCategoryId_;		 
	PartitionDataList partitionList_;						 
	PartitionGroupDataList
		partitionGroupData_;  
	MemoryLimitManager
		memoryLimitManager_;  
	ChunkManagerStats
		chunkManagerStats_;  

	UTIL_FORCEINLINE PartitionGroupId getPartitionGroupId(PartitionId pId) {
		return getConfig().getPartitionGroupId(pId);
	}
	UTIL_FORCEINLINE ChunkCategoryAttribute& getChunkCategoryAttribute(
		ChunkCategoryId categoryId) {
		return categoryAttribute_[categoryId];
	}
	UTIL_FORCEINLINE PartitionData& getPartitionData(PartitionId pId) {
		return *partitionList_[pId];
	}
	UTIL_FORCEINLINE PartitionGroupData& getPartitionGroupData(
		PartitionId pId) {
		return *partitionGroupData_[getPartitionGroupId(pId)];
	}
	UTIL_FORCEINLINE MetaChunk& getMetaChunk(PartitionId pId) {
		return getPartitionGroupData(pId).metaChunk_;
	}
	UTIL_FORCEINLINE MetaChunkManager& getMetaChunkManager(PartitionId pId) {
		return getPartitionGroupData(pId).metaChunkManager_;
	}
	UTIL_FORCEINLINE CheckpointManager& getCheckpointManager(PartitionId pId) {
		return getPartitionGroupData(pId).checkpointManager_;
	}
	UTIL_FORCEINLINE BufferManager& getBufferManager(PartitionId pId) {
		return getPartitionGroupData(pId).bufferManager_;
	}
	UTIL_FORCEINLINE AffinityManager& getAffinityManager(PartitionId pId) {
		return getPartitionGroupData(pId).affinityManager_;
	}
	UTIL_FORCEINLINE ChunkCursor& getSyncCursor(PartitionId pId) {
		return getPartitionData(pId).syncCursor_;
	}

	UTIL_FORCEINLINE PartitionGroupData& getPGPartitionGroupData(
		PartitionGroupId pgId) {
		return *partitionGroupData_[pgId];
	}
	UTIL_FORCEINLINE ActivePartitionList& getPGActivePartitionList(
		PartitionGroupId pgId) {
		return getPGPartitionGroupData(pgId).pIdList_;
	}
	UTIL_FORCEINLINE MetaChunkManager& getPGMetaChunkManager(
		PartitionGroupId pgId) {
		return getPGPartitionGroupData(pgId).metaChunkManager_;
	}
	UTIL_FORCEINLINE CheckpointManager& getPGCheckpointManager(
		PartitionGroupId pgId) {
		return getPGPartitionGroupData(pgId).checkpointManager_;
	}
	UTIL_FORCEINLINE BufferManager& getPGBufferManager(PartitionGroupId pgId) {
		return getPGPartitionGroupData(pgId).bufferManager_;
	}
	UTIL_FORCEINLINE CheckpointFile& getPGCheckpointFile(
		PartitionGroupId pgId) {
		return getPGPartitionGroupData(pgId).checkpointFile_;
	}

	void drop(PartitionId pId);
	void initialize(PartitionId pId);
	bool exist(PartitionId pId);
	uint32_t getAtomicExistPartitionNum();

	ChunkId searchEmptyChunk(
		PartitionId pId, ChunkCategoryId categoryId, uint8_t powerSize);

	std::string getTraceInfo(PartitionId pId,
		ChunkCategoryId categoryId = UNDEF_CHUNK_CATEGORY_ID,
		ChunkId cId = UNDEF_CHUNKID, bool enableMetaChunk = false,
		bool enableMetaChunkManager = false,
		bool enableCheckpointManager = false,
		bool enableMemoryLimitManager = false, bool enableBufferManager = false,
		bool enableFileManager = false, bool enableAffinityManager = false);
	bool isValidId(PartitionId pId,
		ChunkCategoryId categoryId = UNDEF_CHUNK_CATEGORY_ID,
		ChunkId cId = UNDEF_CHUNKID);
	bool isValid(PartitionId pId,
		ChunkCategoryId categoryId = UNDEF_CHUNK_CATEGORY_ID,
		bool enableMetaChunkManager = false,
		bool enableMemoryLimitManager = false);






	ChunkManager(const ChunkManager&);
	ChunkManager& operator=(const ChunkManager&);

};

UTIL_FORCEINLINE ChunkManager::MetaChunk* ChunkManager::allocateChunk(
	PartitionId pId, ChunkCategoryId categoryId, AffinityGroupId affinityValue,
	ChunkKey chunkKey, ChunkId cId) {
	assert(isValidId(pId, categoryId));

	bool isValidChunkKey = (0 <= chunkKey && chunkKey <= UNDEF_CHUNK_KEY);
	if (!isValidChunkKey) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CHM_INVALID_CHUNK_KEY,
			"chunk Key must be 0 <= chunkKey <= UNDEF_CHUNK_KEY, chunkKey, "
				<< chunkKey);
	}

	try {

		MetaChunkManager& metaChunkManager = getMetaChunkManager(pId);
		BaseMetaChunk* baseMetaChunk =
			metaChunkManager.allocate(pId, categoryId, cId);

		AffinityManager& affinityManager = getAffinityManager(pId);
		if (!exist(pId)) {
			initialize(pId);
			affinityManager.reconstruct(pId,
				getConfig().getAtomicStoreMemoryLimitNum(),
				getAtomicExistPartitionNum(), getConfig().getAffinitySize());
		}

		assert(!baseMetaChunk->isAffinityUse());
		ChunkId oldAffinityCId;
		affinityManager.setAffinityChunkId(
			pId, categoryId, affinityValue, cId, oldAffinityCId);
		baseMetaChunk->setAffinityUsed();
		if (oldAffinityCId != UNDEF_CHUNKID) {
			BaseMetaChunk* oldBaseMetaChunk =
				metaChunkManager.getMetaChunk(pId, categoryId, oldAffinityCId);
			if (oldBaseMetaChunk) {
				oldBaseMetaChunk->setAffinityUnused();
			}
		}

		MetaChunk& metaChunk = getMetaChunk(pId);

		BufferManager& bufferManager = getBufferManager(pId);
		uint8_t* buffer =
			bufferManager.allocate(pId, categoryId, cId, metaChunk.bufferInfo_);

		assert(buffer);
		ChunkHeader::initialize(buffer, getConfig().getChunkExpSize(),
			getPartitionGroupId(pId), pId, categoryId, cId,
			getChunkCategoryAttribute(categoryId), chunkKey,
			getConfig().getPartitionGroupNum(), getConfig().getPartitionNum(),
			getConfig().getChunkCategoryNum());

		baseMetaChunk->setChunkKey(chunkKey);

		metaChunk.base_ = baseMetaChunk;
		metaChunk.buffer_ = buffer;
		assert(metaChunk.isValid());

		GS_TRACE_INFO(CHUNK_MANAGER_DETAIL, GS_TRACE_CHM_INTERNAL_INFO,
			getTraceInfo(pId, categoryId, cId).c_str()
				<< ",affinityGroupId," << affinityValue << ",chunkKey,"
				<< chunkKey);
		assert(metaChunkManager.isValid(pId, categoryId));
		assert(baseMetaChunk->getUnoccupiedSize() ==
			   getConfig().getChunkExpSize() - 1);
		assert(getMetaChunk(pId).getOccupiedSize() == 0);
		assert(buffer && ChunkHeader::getPartitionId(buffer) == pId &&
			   ChunkHeader::getChunkCategoryId(buffer) == categoryId &&
			   ChunkHeader::getChunkId(buffer) == cId);
		return &metaChunk;
	}
	catch (SystemException&) {
		throw;
	}
	catch (std::exception& e) {
		GS_RETHROW_SYSTEM_ERROR(e, "");
	}
}
UTIL_FORCEINLINE void ChunkManager::MetaChunk::setCheckpointPos(
	int64_t checkpointPos) {
	if (0 <= checkpointPos) {
		base_->checkpointPos_ = checkpointPos;
	}
}
UTIL_FORCEINLINE void ChunkManager::MetaChunk::resetCheckpointPos() {
	base_->checkpointPos_ = -1;
}
UTIL_FORCEINLINE bool ChunkManager::MetaChunk::setChunkKey(ChunkKey chunkKey) {
	if (0 <= chunkKey && base_->chunkKey_ < chunkKey) {
		base_->chunkKey_ = chunkKey;
		return true;
	}
	return false;
}
UTIL_FORCEINLINE void ChunkManager::MetaChunk::setBuffer(uint8_t*& buffer) {
	buffer_ = buffer;
}
UTIL_FORCEINLINE void ChunkManager::MetaChunk::resetBuffer() {
	buffer_ = NULL;
}
UTIL_FORCEINLINE void ChunkManager::MetaChunk::setMetaChunkManager(
	MetaChunkManager& metaChunkManager) {
	metaChunkManager_ = &metaChunkManager;
}
UTIL_FORCEINLINE bool ChunkManager::MetaChunk::isAffinityUse() const {
	return base_->isAffinityUse_;
}
UTIL_FORCEINLINE void ChunkManager::MetaChunk::setAffinityUsed() {
	base_->isAffinityUse_ = true;
}
UTIL_FORCEINLINE void ChunkManager::MetaChunk::setAffinityUnused() {
	base_->isAffinityUse_ = false;
}
UTIL_FORCEINLINE bool ChunkManager::MetaChunk::isFree() const {
	return (base_->unoccupiedSize_ == UNDEF_UNOCCUPIED_SIZE_);
}
UTIL_FORCEINLINE uint8_t* ChunkManager::MetaChunk::getPtr() const {
	assert(isValid());
	return buffer_;
}
UTIL_FORCEINLINE int64_t ChunkManager::MetaChunk::getCheckpointPos() const {
	return base_->checkpointPos_;
}
UTIL_FORCEINLINE ChunkKey ChunkManager::MetaChunk::getChunkKey() const {
	return base_->chunkKey_;
}
UTIL_FORCEINLINE uint8_t ChunkManager::MetaChunk::getUnoccupiedSize() const {
	return base_->unoccupiedSize_;
}
UTIL_FORCEINLINE void ChunkManager::MetaChunk::restoreUnoccupiedSize(
	uint8_t unoccupiedSize) {
	base_->unoccupiedSize_ = unoccupiedSize;
}
UTIL_FORCEINLINE void ChunkManager::MetaChunk::resetUnoccupiedSize() {
	base_->unoccupiedSize_ = UNDEF_UNOCCUPIED_SIZE_;
}
UTIL_FORCEINLINE bool ChunkManager::MetaChunk::setUnoccupiedSize(
	uint8_t unoccupiedSize) {
	assert(isValid());
	if (unoccupiedSize < MAX_CHUNK_EXP_SIZE_) {
		base_->unoccupiedSize_ = unoccupiedSize;
		ChunkHeader::setUnoccupiedSize(getPtr(), unoccupiedSize);
		return true;
	}
	else {
		return false;
	}
}
UTIL_FORCEINLINE void ChunkManager::MetaChunk::addOccupiedSize(
	uint64_t objectSize) {
	assert(isValid());
	assert(0 < objectSize);
	assert(objectSize <= (1ULL << ChunkHeader::getChunkExpSize(getPtr())));
	int32_t oldOccupiedSize = ChunkHeader::getOccupiedSize(getPtr());
	int32_t newOccupiedSize =
		static_cast<int32_t>(oldOccupiedSize + objectSize);
	assert(0 < newOccupiedSize);
	assert(newOccupiedSize <= (1 << ChunkHeader::getChunkExpSize(getPtr())));
	ChunkHeader::setOccupiedSize(getPtr(), newOccupiedSize);
	metaChunkManager_->addPGAllocatedObjectSize(objectSize);
}
UTIL_FORCEINLINE void ChunkManager::MetaChunk::subtractOccupiedSize(
	uint64_t objectSize) {
	assert(isValid());
	assert(0 < objectSize);
	assert(objectSize <= (1ULL << ChunkHeader::getChunkExpSize(getPtr())));
	int32_t oldOccupiedSize = ChunkHeader::getOccupiedSize(getPtr());
	assert(objectSize <= oldOccupiedSize);
	int32_t newOccupiedSize =
		static_cast<int32_t>(oldOccupiedSize - objectSize);
	assert(0 <= newOccupiedSize);
	assert(newOccupiedSize <= (1 << ChunkHeader::getChunkExpSize(getPtr())));
	ChunkHeader::setOccupiedSize(getPtr(), newOccupiedSize);
	metaChunkManager_->subtractPGAllocatedObjectSize(objectSize);
}
UTIL_FORCEINLINE uint64_t ChunkManager::MetaChunk::getOccupiedSize() const {
	assert(isValid());
	return ChunkHeader::getOccupiedSize(getPtr());
}

UTIL_FORCEINLINE uint8_t* ChunkManager::SinglePool::allocate() {
	if (pool_) {
		uint8_t* buffer = pool_;
		pool_ = NULL;
		return buffer;
	}
	else {
		uint8_t* buffer = static_cast<uint8_t*>(memoryPool_.allocate());
		return buffer;
	}
}
UTIL_FORCEINLINE void ChunkManager::SinglePool::free(
	uint8_t* buffer, uint32_t limit) {
	if (pool_ || limit == 0) {
		memoryPool_.deallocate(buffer);
	}
	else {
		pool_ = buffer;
	}
}
UTIL_FORCEINLINE uint64_t ChunkManager::SinglePool::getPoolNum() const {
	return (pool_) ? 1 : 0;
}
#endif  
