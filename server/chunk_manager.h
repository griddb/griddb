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


#ifdef WIN32
static inline uint32_t __builtin_clz(uint32_t x) {
	return 31 - util::nlz(x);
}
static inline uint32_t u32(uint64_t x) {
	return static_cast<uint32_t>(x >> 32);
}
static inline uint32_t l32(uint64_t x) {
	return static_cast<uint32_t>(x & 0xFFFFFFFF);
}
static inline uint64_t __builtin_clzll(uint64_t x) {
	return u32(x) ? __builtin_clz(u32(x)) : __builtin_clz(l32(x)) + 32;
}
#endif


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
	typedef util::VariableSizeAllocator<>  ArrayAllocator;
	typedef util::VariableSizeAllocator<util::NoopMutex,
		util::VariableSizeAllocatorTraits<128, 1024, 1024 * 8> >
		PartitionGroupAllocator;

	class BaseMetaChunk;
	class BufferInfo;
public:
	class DataAffinityUtils;

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
		@brief Data affinity
	*/
	struct DataAffinityInfo {
		DataAffinityInfo()
				: expireCategory_(0), updateCategory_(0) {};

		explicit DataAffinityInfo(ExpireIntervalCategoryId expireCategory)
				: expireCategory_(expireCategory), updateCategory_(0) {};

		DataAffinityInfo(
				ExpireIntervalCategoryId expireCategory,
				UpdateIntervalCategoryId updateCategory)
				: expireCategory_(expireCategory),
				  updateCategory_(updateCategory) {};

		ExpireIntervalCategoryId expireCategory_;
		UpdateIntervalCategoryId updateCategory_;
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
				, ChunkManager::CompressionMode compressionMode
				, int32_t shiftableMemRate = 80
				, int32_t emaHalfLifePeriod_ = 240
			   , double atomicStoreMemoryColdRate_ = 3.0 / 8.0
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
		bool setAtomicStoreMemoryLimit(uint64_t memoryLimitByte);
		uint64_t getAtomicStoreMemoryLimit() {
			return atomicStorePoolLimit_;
		}
		uint64_t getAtomicStoreMemoryLimitNum() {
			return atomicStorePoolLimitNum_;
		}

		bool setAtomicCheckpointMemoryLimit(uint64_t memoryLimitByte);
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

		int32_t getAtomicShiftableMemRate() {
			return atomicShiftableMemRate_;
		}
		bool setAtomicShiftableMemRate(int32_t rate) {
			atomicShiftableMemRate_ = rate;
			return true;
		}

		int32_t getAtomicEMAHalfLifePeriod() {
			return atomicEMAHalfLifePeriod_;
		}
		bool setAtomicEMAHalfLifePeriod(int32_t period) {
			atomicEMAHalfLifePeriod_ = period;
			return true;
		}

		double getAtomicStoreMemoryColdRate() {
			return static_cast<double>(atomicStoreMemoryColdRate_) / 1.0E6;
		}
		bool setAtomicStoreMemoryColdRate(double rate);

		int32_t getAffinitySize() {
			return atomicAffinitySize_;
		}

		CompressionMode getCompressionMode() const {
			return compressionMode_;
		}

		CompressionMode getPartitionCompressionMode(PartitionId pId) const {
			return partitionCompressionMode_[pId];
		}

		bool setPartitionCompressionMode(PartitionId pId, CompressionMode mode);

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

		util::Atomic<uint32_t> atomicShiftableMemRate_;
		util::Atomic<uint32_t> atomicEMAHalfLifePeriod_;
		util::Atomic<uint64_t> atomicStoreMemoryColdRate_;
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
		uint64_t getStoreUse(ChunkCategoryId categoryId) const;
		uint64_t getStoreMemory(ChunkCategoryId categoryId) const;
		uint64_t getSwapRead(ChunkCategoryId categoryId) const;
		uint64_t getSwapWrite(ChunkCategoryId categoryId) const;
		uint64_t getCheckpointFileSize(PartitionGroupId pgId) const;
		uint64_t getCheckpointFileAllocateSize(PartitionGroupId pgId) const;
		uint64_t getStoreUse() const;
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
		uint64_t getPGNormalSwapRead(PartitionGroupId pgId) const;
		uint64_t getPGColdBufferingSwapRead(PartitionGroupId pgId) const;
		uint64_t getPGStoreUse(PartitionGroupId pgId) const;
		uint64_t getPGCategoryStoreUse(
				PartitionGroupId pgId, ChunkCategoryId categoryId) const;
		uint64_t getPGCategoryStoreMemory(
				PartitionGroupId pgId, ChunkCategoryId categoryId) const;
		uint64_t getPGCategorySwapRead(
				PartitionGroupId pgId, ChunkCategoryId categoryId) const;
		uint64_t getPGCategorySwapWrite(
				PartitionGroupId pgId, ChunkCategoryId categoryId) const;

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
	static const int32_t MIN_CHUNK_EXP_SIZE_ = 15;
	static const int32_t CHUNK_CATEGORY_NUM = 5;
	static const int32_t MAX_CHUNK_ID =
		INT32_MAX - 1;  
	static const int32_t CHUNK_HEADER_FULL_SIZE = 256;  
	static const uint32_t MAX_CHUNK_EXPAND_COUNT = 4096;
	static const uint32_t LIMIT_CHUNK_NUM_LIST[6];

public:
	static const size_t EXPIRE_INTERVAL_CATEGORY_COUNT = 6;
	static const size_t UPDATE_INTERVAL_CATEGORY_COUNT = 7;

	static const size_t CHUNKKEY_BIT_NUM =
		22;  
	static const size_t CHUNKKEY_BITS =
		0x3FFFFF;  

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
		static const Offset_t CHUNK_DATA_AFFINITY_VALUE =
			CHUNK_DATA_SIZE_OFFSET + sizeof(uint32_t);
		static const Size_t CHUNK_HEADER_SIZE =
			CHUNK_DATA_AFFINITY_VALUE + sizeof(uint64_t);

		static const uint16_t MAGIC_NUMBER = 0xcd14;
		static const Offset_t CHECK_SUM_START_OFFSET = VERSION_OFFSET;

		static void initialize(uint8_t* data,
				uint8_t chunkExpSize, PartitionGroupId pgId, PartitionId pId,
				ChunkCategoryId categoryId, ChunkId cId,
				ChunkCategoryAttribute& attribute, ChunkKey chunkKey,
				PartitionGroupId partitionGroupNum, PartitionId partitionNum,
				ChunkCategoryId categoryNum);

		static uint32_t getCheckSum(const uint8_t* data);
		static uint32_t calcCheckSum(const uint8_t* data);
		static uint32_t updateCheckSum(uint8_t* data);

		static bool check(const uint8_t* data);

		static uint32_t getVersion(const uint8_t* data);
		static void setVersion(
				uint8_t* data, uint16_t version);

		static bool checkMagicNum(const uint8_t* data);
		static void setMagicNum(uint8_t* data);

		static PartitionGroupId getPartitionGroupNum(const uint8_t* data);
		static void setPartitionGroupNum(
				uint8_t* data, PartitionGroupId partitionGroupNum);

		static uint32_t getPartitionNum(const uint8_t* data);
		static void setPartitionNum(uint8_t* data, PartitionId partitionNum);

		static PartitionGroupId getPartitionGroupId(const uint8_t* data);
		static void setPartitionGroupId(uint8_t* data, PartitionGroupId pgId);

		static PartitionId getPartitionId(const uint8_t* data);
		static void setPartitionId(uint8_t* data, PartitionId pId);

		static ChunkCategoryId getChunkCategoryNum(const uint8_t* data);
		static void setChunkCategoryNum(
				uint8_t* data, ChunkCategoryId categoryId);

		static ChunkCategoryId getChunkCategoryId(const uint8_t* data);
		static void setChunkCategoryId(
				uint8_t* data, ChunkCategoryId categoryId);

		static uint8_t getChunkExpSize(const uint8_t* data);
		static void setChunkExpSize(uint8_t* data, uint8_t chunkExpSize);

		static uint8_t getUnoccupiedSize(const uint8_t* data);
		static void setUnoccupiedSize(uint8_t* data, uint8_t unoccupiedSize);

		static int32_t getOccupiedSize(const uint8_t* data);
		static void setOccupiedSize(uint8_t* data, int32_t occupiedSize);

		static ChunkId getChunkId(const uint8_t* data);
		static void setChunkId(uint8_t* data, ChunkId cId);

		static ChunkKey getChunkKey(const uint8_t* data);
		static void setChunkKey(uint8_t* data, ChunkKey chunkKey);

		static CheckpointId getSwapCpId(const uint8_t* data);
		static void setSwapCpId(uint8_t* data, CheckpointId cpId);

		static CheckpointId getCPCpId(const uint8_t* data);
		static void setCPCpId(uint8_t* data, CheckpointId cpId);

		static uint8_t getAttribute(const uint8_t* data);
		static void setAttribute(uint8_t* data, uint8_t attribute);

		static void validateHeader(const uint8_t* data,
				PartitionId expectedPartitionNum, uint8_t expectedChunkExpSize,
				bool checkCheckSum);

		static uint32_t getCompressedDataSize(const uint8_t* data);
		static void setCompressedDataSize(uint8_t* data, uint32_t dataSize);

		static uint64_t getChunkDataAffinity(const uint8_t* data);
		static void setChunkDataAffinity(uint8_t* data, uint64_t dataSize);

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
			const DataAffinityInfo &affinityInfo, ChunkKey chunkKey, ChunkId cId);
	void freeChunk(PartitionId pId, ChunkCategoryId categoryId, ChunkId cId);
	MetaChunk* getChunkForUpdate(PartitionId pId, ChunkCategoryId categoryId,
			ChunkId cId, ChunkKey chunkKey, bool doFix);
	MetaChunk* getChunk(
			PartitionId pId, ChunkCategoryId categoryId, ChunkId cId);
	MetaChunk* searchChunk(
			PartitionId pId, ChunkCategoryId chunkCategoryId,
			const DataAffinityInfo &affinityInfo, ChunkKey chunkKey,
			uint8_t powerSize, ChunkId& cId);
	MetaChunk* searchChunk(
			PartitionId pId, ChunkCategoryId categoryId,
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
			uint64_t scanChunkNum, uint64_t& scanCount, uint64_t& freeChunkNum,
			ChunkKey simulateChunkKey, uint64_t& simulateFreeNum);
	void purgeDataAffinityInfo(PartitionId pId, Timestamp ts);



	void isValidFileHeader(PartitionGroupId pgId);
	void resetCheckpointBit(PartitionGroupId pgId);
	void recoveryChunk(PartitionId pId, ChunkCategoryId categoryId, ChunkId cId,
			ChunkKey chunkKey, uint8_t unoccupiedSize, uint64_t filePos);
	void recoveryChunk(PartitionId pId, const uint8_t* chunk, uint32_t size,
			bool forIncrementalBackup);
	void adjustPGStoreMemory(PartitionGroupId pgId);


	void startCheckpoint(PartitionGroupId pgId, CheckpointId cpId);
	void endCheckpoint(PartitionGroupId pgId, CheckpointId cpId);
	void flush(PartitionGroupId pgId);
	BitArray& getCheckpointBit(PartitionGroupId pgId);

	void startCheckpoint(PartitionId pId);
	void endCheckpoint(PartitionId pId);
	uint64_t getScanSize(PartitionId pId);

	MetaChunk* begin(
			PartitionId pId, ChunkCategoryId& categoryId,
			ChunkId& cId, ChunkKey* &chunkKey);
	MetaChunk* next(
			PartitionId pId, ChunkCategoryId& categoryId,
			ChunkId& cId, ChunkKey* &chunkKey);

	bool isCopyLeft(PartitionId pId);
	bool copyChunk(PartitionId pId);
	uint64_t writeChunk(PartitionId pId);
	void cleanCheckpointData(PartitionGroupId pgId);
	uint64_t startSync(CheckpointId cpId, PartitionId pId);
	bool getCheckpointChunk(PartitionId pId, uint32_t size, uint8_t* buffer);
	uint64_t backupCheckpointFile(PartitionGroupId pgId, CheckpointId cpId,
			const std::string& backupPath);

	void getBackupChunk(
			PartitionGroupId pgId, uint64_t filePos, uint8_t* chunk);

	BitArray& getBackupBitArray(PartitionGroupId pgId);

	int64_t getCheckpointChunkPos(
			PartitionId pId, std::map<int64_t, int64_t> &oldOffsetMap);

	struct MakeSyncTempCpContext {
		MakeSyncTempCpContext(util::StackAllocator &alloc)
				: readBuffer_(alloc), writeBuffer_(alloc) {}
		PartitionGroupId pgId_;
		PartitionId pId_;
		CheckpointId cpId_;
		std::string syncTempPath_;
		CheckpointFile* syncTempFile_;
		CheckpointFile* srcCpFile_;
		util::XArray<uint8_t> readBuffer_;
		util::XArray<uint8_t> writeBuffer_;
		uint32_t chunkSize_;
		uint64_t chunkNum_;
		uint32_t copyNum_;
		static const uint32_t IO_SIZE = 1 * 1024 * 1024;
	};

	void prepareMakeSyncTempCpFile(
			PartitionGroupId pgId, PartitionId pId, CheckpointId cpId,
			const std::string& syncTempPath, CheckpointFile &syncTempFile,
			MakeSyncTempCpContext &context);

	bool makeSyncTempCpFile(
			MakeSyncTempCpContext &context,
			std::map<int64_t, int64_t> &oldOffsetMap,
			std::map<int64_t, int64_t> &newOffsetMap,
			uint64_t &srcFilePos, uint64_t &destFilePos,
			uint64_t &writeCount);

	void makeSyncTempCpFileInternal(
			MakeSyncTempCpContext &context,
			std::map<int64_t, int64_t> &oldOffsetMap,
			std::map<int64_t, int64_t> &newOffsetMap,
			uint64_t &srcReadPos, uint64_t readCount,
			uint64_t &destBlockNo, uint64_t &writeBufferPos,
			uint64_t &writeCount, bool isLast);

	void setSwapOutCounter(PartitionId pId, int64_t counter);

	int64_t getSwapOutCounter(PartitionId pId);

	void setStoreMemoryAgingSwapRate(PartitionId pId, double rate);
	double getStoreMemoryAgingSwapRate(PartitionId pId);



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
	static const uint32_t MAX_ONCE_SWAP_SIZE_BYTE_ = 64 * 1024 * 1024;  
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
	private:
		static const uint64_t CHECKPOINT_POS_SHIFT_BIT = 15;
		static const uint64_t CHECKPOINT_POS_MASK =
				~((UINT64_C(1) << CHECKPOINT_POS_SHIFT_BIT) - 1);
		static const uint64_t UNOCCUPIED_SIZE_MASK = (UINT64_C(1) << 8) - 1;
		static const uint64_t IS_AFFINITY_USE_BIT = UINT64_C(1) << 8;
		static const uint64_t CUMULATIVE_DIRTY_FLAG_BIT = UINT64_C(1) << 9;
		static const uint64_t DIFFERENTIAL_DIRTY_FLAG_BIT = UINT64_C(1) << 10;
	public:
		static const int64_t UNDEF_CHECKPOINT_POS =
				INT64_C(-1) & static_cast<int64_t>(CHECKPOINT_POS_MASK);

		BaseMetaChunk()
			: data_(UNDEF_CHECKPOINT_POS | UNDEF_UNOCCUPIED_SIZE_) {
			}

		void reuse(uint8_t initialUnoccupiedSize);

		bool isFree() const;

		int64_t getCheckpointPos() const;
		void setCheckpointPos(int64_t checkpointPos);

		uint8_t getUnoccupiedSize() const;
		void setUnoccupiedSize(uint8_t unoccupiedSize);
		void resetUnoccupiedSize();

		bool isAffinityUse() const;
		void setAffinityUsed();
		void setAffinityUnused();
		bool getCumulativeDirtyFlag() const;
		void setCumulativeDirtyFlag(bool flag);

		bool getDifferentialDirtyFlag() const;
		void setDifferentialDirtyFlag(bool flag);
		std::string dump();

	private:
		uint64_t data_;
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
		bool onHotList_;			
		ChunkCategoryId categoryId_;

	public:
		BufferInfo();
		BufferInfo(PartitionId pId, ChunkCategoryId categoryId, ChunkId cId);

		bool fix(int64_t refId);
		void fixFirstTime(int64_t refId);
		void unfix();
		bool isFixed() const;

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
	typedef std::deque<ChunkKey,
					   util::StdAllocator<ChunkKey, PartitionGroupAllocator> >
		ChunkKeyList;
	typedef ChunkKeyList::iterator ChunkKeyListItr;

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
		std::vector<ChunkKeyList*> chunkKeyList_;
		std::vector<ChunkCursor> emptyScanCursor_;
		std::vector<ChunkCursor> nextTailCursor_;
		std::vector<ChunkCursor> checkpointTailCursor_;
		ChunkCursor batchFreeCursor_;

	public:
		PartitionMetaChunk(
			ChunkCategoryId chunkCategoryNum, PartitionGroupAllocator& alloc_,
			ChunkManager *manager);

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

	typedef std::pair<ChunkKey, ChunkId> AffinityChunkInfo;
	typedef std::deque<AffinityChunkInfo,
					   util::StdAllocator<AffinityChunkInfo, PartitionGroupAllocator> >
		AffinityChunkInfoDeque;

	class BatchFreeChunkInfoList { 
	public:
		static const uint32_t KEEP_NUM = 11;

		BatchFreeChunkInfoList(
				PartitionGroupAllocator &alloc,
				ExpireIntervalCategoryId expireCategory,
				uint64_t startBaseTime);
		~BatchFreeChunkInfoList();

		ChunkId find(ChunkKey chunkKey);
		bool set(ChunkKey chunkKey, ChunkId chunkId);
		void purge(Timestamp baseTime);

		void clear();

		size_t size() const {
			return chunkInfoDeque_.size();
		}

	private:
		void expand(ChunkKey chunkKey);

		void initialize(Timestamp baseTime);

		AffinityChunkInfoDeque chunkInfoDeque_;
		ExpireIntervalCategoryId expireCategory_;
		uint64_t roundingBitNum_;
		ChunkKey roundingMask_;
		ChunkKey minChunkKey_;
	};

	class BatchFreeChunkInfoTable { 
	public:
		BatchFreeChunkInfoTable(
				PartitionGroupAllocator& allocator, PartitionId pId,
				ChunkCategoryId chunkCategoryId, uint32_t expireCategoryNum,
				uint32_t updateCategoryNum, uint64_t affinitySize);
		virtual ~BatchFreeChunkInfoTable();

		uint64_t getAffinitySize() const {
			return (chunkCategoryId_ == 0) ? 1 : affinitySize_;
		}  

		ChunkId getAffinityChunkId(
				const DataAffinityInfo &affinityInfo,
				ChunkKey chunkKey) const;

		bool setAffinityChunkId(
				const DataAffinityInfo &affinityInfo,
				ChunkKey chunkKey, ChunkId chunkId);

		void purge(Timestamp baseTime);

		void clear();

		size_t size() const;

	private:
		void initialize();

		typedef std::vector<BatchFreeChunkInfoList*,
			util::StdAllocator<BatchFreeChunkInfoList*, PartitionGroupAllocator> >
			BatchFreeChunkInfoTableList;

		PartitionGroupAllocator& alloc_;
		BatchFreeChunkInfoTableList chunkInfoTableList_;
		const uint32_t pId_;
		const ChunkCategoryId chunkCategoryId_;
		const uint32_t expireCategoryNum_;
		const uint32_t updateCategoryNum_;
		uint64_t affinitySize_;
	};

	class NormalChunkInfoTable { 
	public:
		NormalChunkInfoTable(
				PartitionGroupAllocator& allocator, PartitionId pId,
				ChunkCategoryId chunkCategoryId, uint32_t expireCategoryNum,
				uint32_t updateCategoryNum, uint64_t affinitySize);
		virtual ~NormalChunkInfoTable();

		uint64_t getAffinitySize() const {
			return (chunkCategoryId_ == 0) ? 1 : affinitySize_;
		}  

		ChunkId getAffinityChunkId(
				const DataAffinityInfo &affinityInfo,
				ChunkKey chunkKey) const;

		bool setAffinityChunkId(
				const DataAffinityInfo &affinityInfo,
				ChunkKey chunkKey, ChunkId chunkId);

		void purge(Timestamp baseTime);

		void clear();

		size_t size() const {
			return updateCategoryNum_;
		}

	private:
		void initialize();

		typedef std::vector<ChunkId,
			util::StdAllocator<ChunkId, PartitionGroupAllocator> >
			NormalChunkInfoTableList;

		PartitionGroupAllocator& alloc_;
		NormalChunkInfoTableList chunkInfoTableList_;
		const uint32_t pId_;
		const ChunkCategoryId chunkCategoryId_;
		const uint32_t expireCategoryNum_;
		const uint32_t updateCategoryNum_;
		uint64_t affinitySize_;
	};

	/*!
		@brief Chunk ID Table for Affinity
	*/
	class PartitionAffinityChunkInfoTable {
		typedef std::vector<void*,
			util::StdAllocator<void*, PartitionGroupAllocator> >
			AffinityChunkInfoTableList;

	public:
		PartitionAffinityChunkInfoTable(
				PartitionGroupAllocator& allocator, PartitionId pId,
				ChunkCategoryId chunkCategoryNum, uint32_t expireCategoryNum,
				uint32_t updateCategoryNum, uint64_t affinitySize,
				ChunkManager *chunkManager);

		~PartitionAffinityChunkInfoTable();

		void reconstruct(uint64_t newAffinitySize);
		uint64_t getAffinitySize(ChunkCategoryId categoryId) const {
			return (categoryId == 0) ? 1 : affinitySize_;
		}  

		ChunkId getAffinityChunkId(
				ChunkCategoryId categoryId,
				const DataAffinityInfo &affinityInfo,
				ChunkKey chunkKey) const;

		bool setAffinityChunkId(
				ChunkCategoryId categoryId,
				const DataAffinityInfo &affinityInfo,
				ChunkKey chunkKey, ChunkId chunkId);

		void purge(ChunkCategoryId categoryId, Timestamp baseTime);

		void clear();

		size_t size() const;

	private:
		void initialize(Timestamp baseTime);

		PartitionGroupAllocator& alloc_;
		ChunkManager* chunkManager_;
		const uint32_t pId_;
		const ChunkCategoryId chunkCategoryNum_;
		const uint32_t expireCategoryNum_;
		const uint32_t updateCategoryNum_;
		uint64_t affinitySize_;
		AffinityChunkInfoTableList infoTableList_;
	};

	/*!
		@brief Data set of a Partition
	*/
	struct PartitionData {
		const PartitionGroupId pgId_;
		uint8_t partitionExistance_;
		PartitionMetaChunk partitionMetaChunk_;
		PartitionInfo partitionInfo_;
		PartitionAffinityChunkInfoTable affinityChunkInfoTable_;
		ChunkCursor syncCursor_;
		PartitionData(
				Config& config, PartitionId pId,
				PartitionGroupAllocator& allocator,
				ChunkManager *chunkManager = NULL)
			: pgId_(config.getPartitionGroupId(pId)),
			  partitionExistance_(NO_PARTITION),
			  partitionMetaChunk_(
					config.getChunkCategoryNum(), allocator, chunkManager),
			  partitionInfo_(),
			  affinityChunkInfoTable_(
					allocator, pId, config.getChunkCategoryNum(),
					EXPIRE_INTERVAL_CATEGORY_COUNT, UPDATE_INTERVAL_CATEGORY_COUNT,
					1, chunkManager)  
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
			util::Atomic<uint64_t> atomicUseChunkNum_;
			uint64_t checkpointChunkNum_;
			uint64_t batchFreeCount_;
			uint64_t objectAllocateSize_;
			PGStats()
				: atomicUseChunkNum_(0),
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
		UTIL_FORCEINLINE ChunkKeyList& getChunkKeyList(
			PartitionId pId, ChunkCategoryId categoryId) {
			return *getPartition(pId).chunkKeyList_[categoryId];
		}
		void contract(PartitionId pId, ChunkCursor& nextTail);
		MetaChunkManager(const MetaChunkManager&);
		MetaChunkManager& operator=(const MetaChunkManager&);

	public:
		MetaChunkManager(PartitionGroupId pgId,
				std::vector<PartitionId>& pIdList,
				PartitionDataList& partitionList,
				Config& config);
		~MetaChunkManager();
		void expand(PartitionId pId, ChunkCursor& nextTail,
				ChunkId cId = UNDEF_CHUNKID);
		BaseMetaChunk* allocate(
				PartitionId pId, ChunkCategoryId categoryId, ChunkId cId,
				ChunkKey*& metaChunkKey);
		void free(PartitionId pId, ChunkCategoryId categoryId, ChunkId cId);
		BaseMetaChunk* reallocate(
				PartitionId pId, ChunkCategoryId categoryId, ChunkId cId,
				ChunkKey*& metaChunkKey);
		UTIL_FORCEINLINE BaseMetaChunk* getMetaChunk(
				PartitionId pId, ChunkCategoryId categoryId, ChunkId cId,
				ChunkKey *&metaChunkKey) {
			metaChunkKey = NULL;
			ChunkKeyList &chunkKeyList = getChunkKeyList(pId, categoryId);
			if (cId < static_cast<ChunkId>(chunkKeyList.size())) {
				metaChunkKey = &chunkKeyList[cId];
			}
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
		uint64_t getAtomicPGUseChunkNum() const;
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
		BaseMetaChunk* begin(
				PartitionId pId, ChunkCursor& cursor,
				ChunkKey*& metaChunkKey);
		BaseMetaChunk* next(
				PartitionId pId, ChunkCursor& cursor,
				bool& isHead, ChunkKey*& metaChunkKey);
		BaseMetaChunk* nextForCategory(
				PartitionId pId, ChunkCursor& cursor,
				bool& isHead, ChunkKey* &metaChunkKey);
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
		uint64_t getFreeElementCount() const;
		uint64_t getTotalElementCount() const;
	};

	class Compressor {
	public:

	Compressor();
	~Compressor() {}

	uint64_t getCompressionErrorCount() const;

	void compressData(
			uint8_t* src, uint32_t srcSize, uint8_t* dest,
			uint32_t &destSize);

	void uncompressData(
			uint8_t* src, uint32_t srcSize, uint8_t* dest,
			uint32_t &destSize);

	private:
		static const int32_t COMPRESS_LEVEL = Z_BEST_SPEED;

		Compressor(const Compressor&);
		Compressor& operator=(const Compressor&);
		bool isFirstTime_;
		uint64_t compressionErrorCount_;
		z_stream deflateStream_;
		z_stream inflateStream_;
	};

public:

	class FileManager {
		static util::Atomic<uint32_t> atomicEnableCompression_;
	public:
		FileManager(Config& config, CheckpointFile& checkpointFile);
		~FileManager();

		bool isValidHoleSize(uint32_t chunkSize, uint64_t fileSystemBlockSize);

		uint32_t getHoleOffset(uint32_t compressedSize, uint32_t &holeSize);

		uint32_t compressChunk(uint8_t* buffer, CompressionMode mode);

		void uncompressChunk(uint8_t* buffer);

		int64_t writeHolePunchingChunk(
				uint8_t* buffer, int64_t writePos, uint32_t holeOffset);

		int64_t writeChunk(uint8_t* buffer, uint32_t size, int64_t writePos);

		int64_t readChunk(uint8_t* buffer, uint32_t size, int64_t readPos);

		void updateCheckSum(uint8_t* buffer);

		void isValidCheckSum(uint8_t* buffer);

		static CompressionMode isEnableCompression();

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

		void iotrace(uint8_t* buffer, uint32_t size, int64_t pos);

		FileManager(const FileManager&);
		FileManager& operator=(const FileManager&);
	};

private:

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
			uint64_t backupWriteCount_;
			uint64_t backupWriteTime_;
			PGStats()
				: checkpointCount_(0),
				  checkpointWriteCount_(0),
				  checkpointWriteTime_(0),
				  chunkSyncReadCount_(0),
				  chunkSyncReadTime_(0),
				  recoveryReadCount_(0),
				  recoveryReadTime_(0),
				  backupReadCount_(0),
				  backupReadTime_(0)
				  ,
				  backupWriteCount_(0),
				  backupWriteTime_(0)
			
				   {}
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
		BitArray pgBackupBitArray_;  
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
		void initializePGCheckpointBit();
		void setPGCheckpointBit(const uint64_t pos);
		void flushPGFile();
		void clearPG();
		void releaseUnusedPGFileBlocks(const uint64_t bitNum);
		BitArray& getPGBackupBit();  
		void clearPGBackupBit();	 
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

		void readCheckpointChunk(uint64_t checkpointPos, uint8_t* buffer);

		void clearCheckpointPosList();
		bool clearCheckpointPosList(PartitionId pId);
		void appendCheckpointPosList(PartitionId pId, int64_t checkpointPos);

		bool getCheckpointPosList(int64_t& checkpointPos);

		UTIL_FORCEINLINE PartitionId getSyncPId();

		UTIL_FORCEINLINE bool isExecutingCheckpoint(PartitionId pId);
		UTIL_FORCEINLINE uint64_t getSyncChunkNum();
		UTIL_FORCEINLINE void incrementCheckpointNum(PartitionId pId);
		UTIL_FORCEINLINE uint64_t getCheckpointNum(PartitionId pId);
		UTIL_FORCEINLINE uint64_t getWriteNum(PartitionId pId);
		UTIL_FORCEINLINE uint64_t getPGCheckpointCount() const;
		UTIL_FORCEINLINE uint64_t getPGCheckpointBufferNum() const;
		UTIL_FORCEINLINE uint64_t getPGCheckpointWriteCount() const;
		UTIL_FORCEINLINE uint64_t getPGCheckpointWriteTime() const;
		UTIL_FORCEINLINE uint64_t getPGChunkSyncReadCount() const;
		UTIL_FORCEINLINE uint64_t getPGChunkSyncReadTime() const;
		UTIL_FORCEINLINE uint64_t getPGRecoveryReadCount() const;
		UTIL_FORCEINLINE uint64_t getPGRecoveryReadTime() const;
		UTIL_FORCEINLINE uint64_t getPGBackupReadCount() const;
		UTIL_FORCEINLINE uint64_t getPGBackupReadTime() const;
		UTIL_FORCEINLINE uint64_t getPGBackupWriteCount() const;
		UTIL_FORCEINLINE uint64_t getPGBackupWriteTime() const;
		UTIL_FORCEINLINE uint64_t getPGFileNum();
		UTIL_FORCEINLINE uint64_t getPGFileUseNum();
		UTIL_FORCEINLINE uint64_t getPGFileSize();
		UTIL_FORCEINLINE uint64_t getPGFileAllocateSize();

	};

	/*!
		@brief Manages buffers of Chunks for each Partition group.
	*/
	class BufferManager {
		friend class ChunkManager;  

		typedef BufferInfo** BufferInfoList;

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
					: useBufferNum_(0), swapWriteCount_(0), swapReadCount_(0)
					  {}
			};
			CategoryStats* categoryStats_;
			uint64_t useBufferNum_;
			uint64_t swapWriteCount_;
			uint64_t swapWriteTime_;
			util::Atomic<uint64_t> atomicSwapReadCount_;
			uint64_t swapReadTime_;
			uint64_t swapOutCount_;
			double storeMemoryAgingSwapRate_;
			uint64_t swapNormalReadCount_;
			uint64_t swapColdBufferingReadCount_;
			uint64_t enwarmReadCount_;
			uint64_t enwarmReadTime_;
			PGStats(ChunkCategoryId categoryNum);
			~PGStats();
		};

		class LinkedList {
		private:
			uint64_t elementCount_;
			BufferInfo* head_;
			BufferInfo* tail_;
			BufferInfo* cursor_;  

			LinkedList(const LinkedList&);
			LinkedList& operator=(const LinkedList&);

		public:
			LinkedList();
			~LinkedList();

			BufferInfo* back();
			void pop_back();
			void push_front(BufferInfo& bufferInfo);

			void remove(BufferInfo& bufferInfo);
			uint64_t getElementCount() const;

			void split(int64_t pos, LinkedList &result);

			void concat(LinkedList &list, bool toTail);

			void reset(BufferInfo* head, BufferInfo* tail, uint64_t elementCount);

			BufferInfo* beginCursor();
			BufferInfo* nextCursor();

			bool isValid();
		private:
		};

		class BufferList {
		public:
			BufferList();
			~BufferList();

			BufferInfo* back();  
			void push(BufferInfo& bufferInfo);     
			void pushCold(BufferInfo& bufferInfo);  

			void remove(BufferInfo& bufferInfo);

			void rebalance(uint64_t totalElementCount); 

			void setColdRate(double rate);
			double getColdRate() const;

			uint64_t getHotElementCount() const;
			uint64_t getColdElementCount() const;
			uint64_t getElementCount() const;

			BufferInfo* beginCursor();
			BufferInfo* nextCursor();

			bool isValid();

		private:
			BufferList(const BufferList&);
			BufferList& operator=(const BufferList&);

			void shiftHotToCold(size_t n);
			void shiftColdToHot(size_t n);

			void pop(size_t n);

			LinkedList hotList_;  
			LinkedList coldList_;   
			BufferInfo* cursor_;  
			bool cursorOnCold_;

			double coldRate_;
		};

		static const uint64_t MINIMUM_BUFFER_INFO_LIST_SIZE =
			1ULL << 21;  

		static uint64_t getHashTableSize(
				PartitionGroupId partitionGroupNum,
				uint64_t totalAtomicMemoryLimitNum);

		class ChainingHashTableCursor;
		/*!
			@brief Manages set of buffers in hash.
		*/
		class ChainingHashTable {
			friend class ChainingHashTableCursor;
			typedef uint64_t HashValue;

		public:
			ChainingHashTable(size_t size, ArrayAllocator& allocator);
			~ChainingHashTable();

			void append(BufferId bufferId, BufferInfo* bufferInfo);
			BufferInfo* get(BufferId bufferId);
			BufferInfo* remove(BufferId bufferId);
		private:
			static const uint32_t SEED_ = 0;
			const uint64_t maxSize_;
			const uint64_t tableMask_;
			BufferInfoList table_;
			ChainingHashTable(const ChainingHashTable&);
			ChainingHashTable& operator=(const ChainingHashTable&);

			static const uint64_t FNV_OFFSET_BASIS_64 = 14695981039346656037ULL;
			static const uint64_t FNV_PRIME_64 = 1099511628211ULL;
			HashValue fnvHash(uint8_t* bytes, size_t length);
			HashValue calcKey(BufferId bufferId);
		};

		class ChainingHashTableCursor {
		public:
			explicit ChainingHashTableCursor(ChainingHashTable& hashTable)
				: hashTable_(hashTable),
				  maxSize_(static_cast<int64_t>(hashTable_.maxSize_)),
				  index_(-1),
				  last_(NULL) {}

			bool next(BufferInfo*& bufferInfo);
			void remove(BufferInfo*& bufferInfo);

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
		BufferList swapList_;
		int64_t refId_;  
		PartitionId
			currentCPPId_;  
		uint64_t storeMemoryAgingSwapCount_;  

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

		void adjustStoreMemory(bool doReserve = false);

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
		UTIL_FORCEINLINE uint64_t getPGSwapNormalReadCount() const {
			return pgStats_.swapNormalReadCount_;
		}
		UTIL_FORCEINLINE uint64_t getPGSwapColdBufferingReadCount() const {
			return pgStats_.swapColdBufferingReadCount_;
		}
		UTIL_FORCEINLINE uint64_t getPGRecoveryReadCount() const {
			return pgStats_.enwarmReadCount_;
		}
		UTIL_FORCEINLINE uint64_t getPGRecoveryReadTime() const {
			return pgStats_.enwarmReadTime_;
		}
		UTIL_FORCEINLINE uint64_t getPGBackupReadCount() const;
		UTIL_FORCEINLINE uint64_t getPGBackupReadTime() const;

		UTIL_FORCEINLINE void setSwapOutCounter(uint64_t counter) {
			pgStats_.swapOutCount_ = counter;
		}

		UTIL_FORCEINLINE uint64_t getSwapOutCounter() {
			return pgStats_.swapOutCount_;
		}

		UTIL_FORCEINLINE double getStoreMemoryAgingSwapRate() {
			return pgStats_.storeMemoryAgingSwapRate_;
		}
		void setStoreMemoryAgingSwapRate(double rate);
		void updateStoreMemoryAgingParams();

		void setStoreMemoryColdRate(double rate) {
			swapList_.setColdRate(rate);
		}
		double getStoreMemoryColdRate() {
			return swapList_.getColdRate();
		}
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
		static const double CALC_EMA_HALF_LIFE_CONSTANT;

	private:
		/*!
			@brief Information of loads of each Partition group
		*/
		struct Load {
			uint64_t totalLoad_;
			uint64_t avgLoad_;
			std::vector< double > lastEstimatedLoad_;
			std::vector< double > newEstimatedLoad_;
			uint64_t* lastLoad_;  
			uint64_t* load_;	  
			uint64_t*
				readCount_;  

			Load(PartitionGroupId partitionGroupNum);
			~Load();
		};

		MemoryLimitManager(const MemoryLimitManager&);
		MemoryLimitManager& operator=(const MemoryLimitManager&);

		static const uint32_t ONCE_SHIFTABLE_MEMORY_RATE =
			3;  

		const PartitionGroupId partitionGroupNum_;
		const uint64_t minTotalLimitNum_;  
		const uint64_t maxShiftNum_;  
		uint64_t lastTotalLimitNum_;
		Load load_;
		uint64_t* limitNum_;  
		std::vector<uint64_t> lastBaseLimit_;
		int32_t shiftableMemRate_;
		int32_t lastEMAHalfLifePeriod_;
		double smoothingConstant_;
		Config& config_;
		PartitionGroupDataList& partitionGroupData_;
		MemoryPool& memoryPool_;

		BufferManager& getPGBufferManager(PartitionGroupId pgId) {
			return partitionGroupData_[pgId]->bufferManager_;
		}
		MetaChunkManager& getPGMetaChunkManager(PartitionGroupId pgId) {
			return partitionGroupData_[pgId]->metaChunkManager_;
		}
		void getCurrentLoad(Load& load);

		void redistributeByEMA(
				uint64_t avgNum, uint64_t minNum,
				uint64_t totalLimitNum);
	public:
		MemoryLimitManager(Config& config,
			PartitionGroupDataList& partitionGroupData, MemoryPool& memoryPool);
		~MemoryLimitManager();

		void redistributeMemoryLimit(PartitionGroupId pgId);
		bool isValid() const;

		std::string dump() const;
		void calcParameter(
				uint64_t &avgNum, uint64_t &minNum);
	};

	/*!
		@brief Manages affinity configuration of selecting Chunks for data
	   appending.
	*/
	class AffinityManager {
	private:
		const uint32_t chunkCategoryNum_;
		const uint32_t expireCategoryNum_;
		const uint32_t updateCategoryNum_;
		const uint64_t
			maxAffinitySize_;  
		PartitionDataList& partitionData_;
		PartitionAffinityChunkInfoTable& getAffinityChunkInfoTable(PartitionId pId) const {
			return partitionData_[pId]->affinityChunkInfoTable_;
		}
		uint64_t calculateAffinitySize(
				uint64_t storeLimitNum, uint32_t existsPartitonNum,
				uint64_t userSpecifiedAffinitySize) const;
		AffinityManager(const AffinityManager&);
		AffinityManager& operator=(const AffinityManager&);

	public:
		static const uint64_t MAX_AFFINITY_SIZE;  
		AffinityManager(
				int32_t chunkCategoryNum, uint64_t maxAffinitySize,
				PartitionDataList& partitionData);
		~AffinityManager();
		void drop(PartitionId pId);
		ChunkId getAffinityChunkId(
				PartitionId pId, ChunkCategoryId categoryId,
				const DataAffinityInfo &affinityInfo,
				ChunkKey chunkKey) const;
		bool setAffinityChunkId(
				PartitionId pId, ChunkCategoryId categoryId,
				const DataAffinityInfo &affinityInfo,
				ChunkKey chunkKey, ChunkId cId,
				ChunkId& oldAffinityCId);
		void purge(PartitionId pId, Timestamp baseTime);
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
	bool readOnly_;	

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
			PartitionId pId, ChunkCategoryId categoryId,
			uint8_t powerSize, ChunkKey chunkKey);
	std::string getTraceInfo(
			PartitionId pId,
			ChunkCategoryId categoryId = UNDEF_CHUNK_CATEGORY_ID,
			ChunkId cId = UNDEF_CHUNKID,
			bool enableMetaChunk = false,
			bool enableMetaChunkManager = false,
			bool enableCheckpointManager = false,
			bool enableMemoryLimitManager = false,
			bool enableBufferManager = false,
			bool enableFileManager = false,
			bool enableAffinityManager = false);
	bool isValidId(
			PartitionId pId,
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
		PartitionId pId, ChunkCategoryId categoryId,
		const DataAffinityInfo &affinityInfo,
	ChunkKey chunkKey, ChunkId cId) {
	assert(!readOnly_);
	assert(isValidId(pId, categoryId));

	bool isValidChunkKey = (0 <= chunkKey && chunkKey <= UNDEF_CHUNK_KEY);
	if (!isValidChunkKey) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_CHM_INVALID_CHUNK_KEY,
			"chunk Key must be 0 <= chunkKey <= UNDEF_CHUNK_KEY, chunkKey, "
				<< chunkKey);
	}

	try {

		MetaChunkManager& metaChunkManager = getMetaChunkManager(pId);
		ChunkKey *metaChunkKey = NULL;
		BaseMetaChunk* baseMetaChunk =
				metaChunkManager.allocate(pId, categoryId, cId, metaChunkKey);

		AffinityManager& affinityManager = getAffinityManager(pId);
		if (!exist(pId)) {
			initialize(pId);
		}

		assert(!baseMetaChunk->isAffinityUse());
		ChunkId oldAffinityCId;
		affinityManager.setAffinityChunkId(
				pId, categoryId, affinityInfo, chunkKey,
				cId, oldAffinityCId);
		baseMetaChunk->setAffinityUsed();
		if (oldAffinityCId != UNDEF_CHUNKID) {
			ChunkKey* chunkKey;
			BaseMetaChunk* oldBaseMetaChunk =
					metaChunkManager.getMetaChunk(
							pId, categoryId, oldAffinityCId, chunkKey);
			if (oldBaseMetaChunk) {
				oldBaseMetaChunk->setAffinityUnused();
			}
		}

		MetaChunk& metaChunk = getMetaChunk(pId);

		BufferManager& bufferManager = getBufferManager(pId);
		uint8_t* buffer =
				bufferManager.allocate(pId, categoryId, cId, metaChunk.bufferInfo_);

		assert(buffer);
		ChunkHeader::initialize(
				buffer, getConfig().getChunkExpSize(),
				getPartitionGroupId(pId), pId, categoryId, cId,
				getChunkCategoryAttribute(categoryId), chunkKey,
				getConfig().getPartitionGroupNum(), getConfig().getPartitionNum(),
				getConfig().getChunkCategoryNum());

		if (metaChunkKey != NULL) {
			*metaChunkKey = chunkKey;
		}
		metaChunk.base_ = baseMetaChunk;
		metaChunk.buffer_ = buffer;
		assert(metaChunk.isValid());

		GS_TRACE_INFO(CHUNK_MANAGER_DETAIL, GS_TRACE_CHM_INTERNAL_INFO,
				getTraceInfo(pId, categoryId, cId).c_str() <<
				",expireCategory," << affinityInfo.expireCategory_ <<
				",updateCategory," << affinityInfo.updateCategory_ <<
				",chunkKey," << chunkKey);
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
		base_->setCheckpointPos(checkpointPos);
	}
}
UTIL_FORCEINLINE void ChunkManager::MetaChunk::resetCheckpointPos() {
	base_->setCheckpointPos(-1);
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
	return base_->isAffinityUse();
}
UTIL_FORCEINLINE void ChunkManager::MetaChunk::setAffinityUsed() {
	base_->setAffinityUsed();
}
UTIL_FORCEINLINE void ChunkManager::MetaChunk::setAffinityUnused() {
	base_->setAffinityUnused() ;
}

UTIL_FORCEINLINE bool ChunkManager::MetaChunk::isFree() const {
	return (base_->getUnoccupiedSize() == UNDEF_UNOCCUPIED_SIZE_);
}

UTIL_FORCEINLINE uint8_t* ChunkManager::MetaChunk::getPtr() const {
	assert(isValid());
	return buffer_;
}

UTIL_FORCEINLINE int64_t ChunkManager::MetaChunk::getCheckpointPos() const {
	return base_->getCheckpointPos();
}

UTIL_FORCEINLINE uint8_t ChunkManager::MetaChunk::getUnoccupiedSize() const {
	return base_->getUnoccupiedSize();
}
UTIL_FORCEINLINE void ChunkManager::MetaChunk::restoreUnoccupiedSize(
		uint8_t unoccupiedSize) {
	base_->setUnoccupiedSize(unoccupiedSize);
}
UTIL_FORCEINLINE void ChunkManager::MetaChunk::resetUnoccupiedSize() {
	base_->setUnoccupiedSize(UNDEF_UNOCCUPIED_SIZE_);
}
UTIL_FORCEINLINE bool ChunkManager::MetaChunk::setUnoccupiedSize(
		uint8_t unoccupiedSize) {
	assert(isValid());
	if (unoccupiedSize < MAX_CHUNK_EXP_SIZE_) {
		base_->setUnoccupiedSize(unoccupiedSize);
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
	assert(objectSize <= static_cast<uint64_t>(oldOccupiedSize));
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
UTIL_FORCEINLINE uint64_t ChunkManager::SinglePool::getFreeElementCount() const {
	uint64_t count = (pool_) ? 1 : 0;
	return count + memoryPool_.getFreeElementCount();
}
UTIL_FORCEINLINE uint64_t ChunkManager::SinglePool::getTotalElementCount() const {
	return memoryPool_.getTotalElementCount();
}

/**
	@brief DataAffinity utility
 */
class ChunkManager::DataAffinityUtils {
public:
	static const uint64_t MIN_EXPIRE_INTERVAL_CATEGORY_ROUNDUP_BITS = 22;
	static const uint64_t MIN_EXPIRE_INTERVAL_CATEGORY_TERM_BITS = 28;
	static const uint64_t MAX_EXPIRE_INTERVAL_CATEGORY_TERM_BITS = 36;
	static const uint64_t MIN_UPDATE_INTERVAL_CATEGORY_TERM_BITS = 9;
	static const uint64_t MAX_UPDATE_INTERVAL_CATEGORY_TERM_BITS = 33;

	static UTIL_FORCEINLINE uint32_t ilog2(uint64_t x) {
		assert(x > 0);
		return (x > 0) ?
				static_cast<uint32_t>(8 * sizeof(uint64_t) - __builtin_clzll((x)) - 1)
				: static_cast<uint32_t>(8 * sizeof(uint64_t));
	}

	static ExpireIntervalCategoryId calcExpireIntervalCategoryId(
			uint64_t expireIntervalMillis);

	static UpdateIntervalCategoryId calcUpdateIntervalCategoryId(
			uint64_t updateIntervalMillis);

	static uint64_t getExpireTimeRoundingBitNum(
			ExpireIntervalCategoryId expireCategory);

	static ChunkKey convertTimestamp2ChunkKey(
			Timestamp time, uint64_t roundingBitNum, bool isRoundUp);
};


UTIL_FORCEINLINE
ChunkKey ChunkManager::DataAffinityUtils::convertTimestamp2ChunkKey(
		Timestamp time, uint64_t roundingBitNum, bool isRoundUp) {
	assert(roundingBitNum >= CHUNKKEY_BIT_NUM);
	if (isRoundUp) {
		assert(time > 0);
		ChunkKey chunkKey = static_cast<ChunkKey>((time - 1) >> roundingBitNum) + 1;
		return static_cast<ChunkKey>(chunkKey << (roundingBitNum - CHUNKKEY_BIT_NUM));
	}
	else {
		ChunkKey chunkKey = static_cast<ChunkKey>(time >> roundingBitNum);
		return static_cast<ChunkKey>(chunkKey << (roundingBitNum - CHUNKKEY_BIT_NUM));
	}
}

UTIL_FORCEINLINE
ExpireIntervalCategoryId ChunkManager::DataAffinityUtils::calcExpireIntervalCategoryId(
		uint64_t expireIntervalMillis) {
	if (expireIntervalMillis > 0) {
		uint64_t log2Value = ilog2(expireIntervalMillis);
		if (log2Value < MIN_EXPIRE_INTERVAL_CATEGORY_TERM_BITS) {
			return 0;
		}
		else {
			return static_cast<ExpireIntervalCategoryId>(
					(log2Value > MAX_EXPIRE_INTERVAL_CATEGORY_TERM_BITS) ?
							(EXPIRE_INTERVAL_CATEGORY_COUNT - 1)
							: ((log2Value - MIN_EXPIRE_INTERVAL_CATEGORY_TERM_BITS) / 2 + 1)
					);
		}
	}
	else {
		assert(false); 
		return 0;
	}
}

UTIL_FORCEINLINE
UpdateIntervalCategoryId ChunkManager::DataAffinityUtils::calcUpdateIntervalCategoryId(
		uint64_t updateIntervalMillis) {
	if (updateIntervalMillis > 0) {
		uint32_t log2Value = ilog2(updateIntervalMillis);
		if (log2Value < MIN_UPDATE_INTERVAL_CATEGORY_TERM_BITS) {
			return 1;
		}
		else {
			return static_cast<UpdateIntervalCategoryId>(
					(log2Value > MAX_UPDATE_INTERVAL_CATEGORY_TERM_BITS) ?
							(UPDATE_INTERVAL_CATEGORY_COUNT - 1)
							: ((log2Value - MIN_UPDATE_INTERVAL_CATEGORY_TERM_BITS) / 6 + 2)
					);
		}
	}
	else {
		return 0;
	}
}

UTIL_FORCEINLINE
uint64_t ChunkManager::DataAffinityUtils::getExpireTimeRoundingBitNum(
		ExpireIntervalCategoryId expireCategory) {
	assert (expireCategory < EXPIRE_INTERVAL_CATEGORY_COUNT);
	return expireCategory * 2 + MIN_EXPIRE_INTERVAL_CATEGORY_ROUNDUP_BITS;
}


UTIL_FORCEINLINE void ChunkManager::ChunkHeader::initialize(
		uint8_t* data, uint8_t chunkExpSize, PartitionGroupId pgId,
		PartitionId pId, ChunkCategoryId categoryId, ChunkId cId,
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
	setChunkDataAffinity(data, 0);
}

UTIL_FORCEINLINE
uint32_t ChunkManager::ChunkHeader::getCheckSum(const uint8_t* data) {
	return *(const uint32_t*)(data + CHECKSUM_OFFSET);
}
UTIL_FORCEINLINE
uint32_t ChunkManager::ChunkHeader::calcCheckSum(const uint8_t* data) {
	return util::fletcher32(
		data + CHECK_SUM_START_OFFSET,
		(1ULL << getChunkExpSize(data)) - CHECK_SUM_START_OFFSET);
}
UTIL_FORCEINLINE
uint32_t ChunkManager::ChunkHeader::updateCheckSum(uint8_t* data) {
	uint32_t checkSum = calcCheckSum(data);
	memcpy((data + CHECKSUM_OFFSET), &checkSum, sizeof(uint32_t));
	return checkSum;
}
UTIL_FORCEINLINE bool ChunkManager::ChunkHeader::check(const uint8_t* data) {
	uint32_t headerCheckSum = getCheckSum(data);
	uint32_t currentCheckSum = calcCheckSum(data);
	return (headerCheckSum == currentCheckSum);
}
UTIL_FORCEINLINE
uint32_t ChunkManager::ChunkHeader::getVersion(const uint8_t* data) {
	return *(const uint16_t*)(data + VERSION_OFFSET);
}
UTIL_FORCEINLINE void ChunkManager::ChunkHeader::setVersion(
	uint8_t* data, uint16_t version) {
	memcpy((data + VERSION_OFFSET), &version, sizeof(uint16_t));
}

UTIL_FORCEINLINE
bool ChunkManager::ChunkHeader::checkMagicNum(const uint8_t* data) {
	return (MAGIC_NUMBER ==
			*reinterpret_cast<uint16_t*>(
					const_cast<uint8_t*>(data + MAGIC_OFFSET)));
}
UTIL_FORCEINLINE void ChunkManager::ChunkHeader::setMagicNum(uint8_t* data) {
	*reinterpret_cast<uint16_t*>(data + MAGIC_OFFSET) = MAGIC_NUMBER;
}
UTIL_FORCEINLINE PartitionGroupId ChunkManager::ChunkHeader::getPartitionGroupNum(
		const uint8_t* data) {
	return *reinterpret_cast<const PartitionGroupId*>(
			data + PARTITION_GROUP_NUM_OFFSET);
}
UTIL_FORCEINLINE void ChunkManager::ChunkHeader::setPartitionGroupNum(
	uint8_t* data, PartitionGroupId partitionGroupNum) {
	memcpy((data + PARTITION_GROUP_NUM_OFFSET), &partitionGroupNum,
			sizeof(PartitionGroupId));
}
UTIL_FORCEINLINE
uint32_t ChunkManager::ChunkHeader::getPartitionNum(const uint8_t* data) {
	return *reinterpret_cast<const PartitionGroupId*>(
			data + PARTITION_NUM_OFFSET);
}
UTIL_FORCEINLINE void ChunkManager::ChunkHeader::setPartitionNum(
	uint8_t* data, PartitionId partitionNum) {
	memcpy((data + PARTITION_NUM_OFFSET), &partitionNum,
			sizeof(PartitionId));
}
UTIL_FORCEINLINE PartitionGroupId ChunkManager::ChunkHeader::getPartitionGroupId(
		const uint8_t* data) {
	return *reinterpret_cast<const PartitionGroupId*>(
			data + PARTITION_GROUP_ID_OFFSET);
}
UTIL_FORCEINLINE void ChunkManager::ChunkHeader::setPartitionGroupId(
		uint8_t* data, PartitionGroupId pgId) {
	memcpy((data + PARTITION_GROUP_ID_OFFSET), &pgId,
			sizeof(PartitionGroupId));
}
UTIL_FORCEINLINE PartitionId ChunkManager::ChunkHeader::getPartitionId(
		const uint8_t* data) {
	return *reinterpret_cast<const PartitionGroupId*>(
			data + PARTITION_ID_OFFSET);
}
UTIL_FORCEINLINE void ChunkManager::ChunkHeader::setPartitionId(
		uint8_t* data, PartitionId pId) {
	memcpy((data + PARTITION_ID_OFFSET), &pId, sizeof(PartitionId));
}
UTIL_FORCEINLINE ChunkCategoryId ChunkManager::ChunkHeader::getChunkCategoryNum(
		const uint8_t* data) {
	return *reinterpret_cast<const ChunkCategoryId*>(
			data + CHUNK_CATEGORY_NUM_OFFSET);
}
UTIL_FORCEINLINE void ChunkManager::ChunkHeader::setChunkCategoryNum(
		uint8_t* data, ChunkCategoryId categoryId) {
	memcpy((data + CHUNK_CATEGORY_NUM_OFFSET), &categoryId,
			sizeof(ChunkCategoryId));
}
UTIL_FORCEINLINE ChunkCategoryId ChunkManager::ChunkHeader::getChunkCategoryId(
		const uint8_t* data) {
	return *reinterpret_cast<const ChunkCategoryId*>(
			data + CHUNK_CATEGORY_ID_OFFSET);
}
UTIL_FORCEINLINE void ChunkManager::ChunkHeader::setChunkCategoryId(
		uint8_t* data, ChunkCategoryId categoryId) {
	memcpy((data + CHUNK_CATEGORY_ID_OFFSET), &categoryId,
			sizeof(ChunkCategoryId));
}
UTIL_FORCEINLINE
uint8_t ChunkManager::ChunkHeader::getChunkExpSize(const uint8_t* data) {
	return *(data + CHUNK_POWER_SIZE_OFFSET);
}
UTIL_FORCEINLINE void ChunkManager::ChunkHeader::setChunkExpSize(
		uint8_t* data, uint8_t chunkExpSize) {
	memcpy((data + CHUNK_POWER_SIZE_OFFSET), &chunkExpSize,
			sizeof(uint8_t));
}
UTIL_FORCEINLINE
uint8_t ChunkManager::ChunkHeader::getUnoccupiedSize(const uint8_t* data) {
	return *(data + UNOCCUPIED_SIZE_OFFSET);
}
UTIL_FORCEINLINE void ChunkManager::ChunkHeader::setUnoccupiedSize(
		uint8_t* data, uint8_t unoccupiedSize) {
	memcpy((data + UNOCCUPIED_SIZE_OFFSET), &unoccupiedSize,
			sizeof(uint8_t));
}
UTIL_FORCEINLINE
int32_t ChunkManager::ChunkHeader::getOccupiedSize(const uint8_t* data) {
	return *reinterpret_cast<int32_t*>(
			const_cast<uint8_t*>(data + OCCUPIED_SIZE_OFFSET));
}
UTIL_FORCEINLINE void ChunkManager::ChunkHeader::setOccupiedSize(
		uint8_t* data, int32_t occupiedSize) {
	memcpy((data + OCCUPIED_SIZE_OFFSET), &occupiedSize, sizeof(int32_t));
}
UTIL_FORCEINLINE
ChunkId ChunkManager::ChunkHeader::getChunkId(const uint8_t* data) {
	return *reinterpret_cast<const ChunkId*>(data + CHUNK_ID_OFFSET);
}
UTIL_FORCEINLINE
void ChunkManager::ChunkHeader::setChunkId(uint8_t* data, ChunkId cId) {
	memcpy((data + CHUNK_ID_OFFSET), &cId, sizeof(ChunkId));
}
UTIL_FORCEINLINE
ChunkKey ChunkManager::ChunkHeader::getChunkKey(const uint8_t* data) {
	return *reinterpret_cast<ChunkKey*>(
			const_cast<uint8_t*>(data + CHUNK_KEY_OFFSET));
}
UTIL_FORCEINLINE void ChunkManager::ChunkHeader::setChunkKey(
		uint8_t* data, ChunkKey chunkKey) {
	memcpy((data + CHUNK_KEY_OFFSET), &chunkKey, sizeof(ChunkKey));
}
UTIL_FORCEINLINE
CheckpointId ChunkManager::ChunkHeader::getSwapCpId(const uint8_t* data) {
	return *reinterpret_cast<CheckpointId*>(
			const_cast<uint8_t*>(data + SWAP_CPID_OFFSET));
}
UTIL_FORCEINLINE void ChunkManager::ChunkHeader::setSwapCpId(
		uint8_t* data, CheckpointId cpId) {
	memcpy((data + SWAP_CPID_OFFSET), &cpId, sizeof(CheckpointId));
}
UTIL_FORCEINLINE
CheckpointId ChunkManager::ChunkHeader::getCPCpId(const uint8_t* data) {
	return *reinterpret_cast<CheckpointId*>(
			const_cast<uint8_t*>(data + CP_CPID_OFFSET));
}
UTIL_FORCEINLINE void ChunkManager::ChunkHeader::setCPCpId(
		uint8_t* data, CheckpointId cpId) {
	memcpy((data + CP_CPID_OFFSET), &cpId, sizeof(CheckpointId));
}
UTIL_FORCEINLINE
uint8_t ChunkManager::ChunkHeader::getAttribute(const uint8_t* data) {
	return *(data + CHUNK_ATTRIBUTE_OFFSET);
}
UTIL_FORCEINLINE void ChunkManager::ChunkHeader::setAttribute(
	uint8_t* data, uint8_t attribute) {
	memcpy((data + CHUNK_ATTRIBUTE_OFFSET), &attribute, sizeof(uint8_t));
}
inline
uint32_t ChunkManager::ChunkHeader::getCompressedDataSize(const uint8_t* data) {
	return *(const uint32_t*)(data + CHUNK_DATA_SIZE_OFFSET);
}
inline void ChunkManager::ChunkHeader::setCompressedDataSize(
		uint8_t* data, uint32_t dataSize) {
	memcpy((data + CHUNK_DATA_SIZE_OFFSET), &dataSize, sizeof(uint32_t));
}
inline
uint64_t ChunkManager::ChunkHeader::getChunkDataAffinity(const uint8_t* data) {
	return *(const uint64_t*)(data + CHUNK_DATA_AFFINITY_VALUE);
}
inline void ChunkManager::ChunkHeader::setChunkDataAffinity(
		uint8_t* data, uint64_t dataSize) {
	memcpy((data + CHUNK_DATA_AFFINITY_VALUE), &dataSize, sizeof(uint64_t));
}

inline void ChunkManager::BaseMetaChunk::reuse(uint8_t initialUnoccupiedSize) {
	assert(getUnoccupiedSize() == UNDEF_UNOCCUPIED_SIZE_);
	setUnoccupiedSize(initialUnoccupiedSize);
	setAffinityUnused();
}

UTIL_FORCEINLINE bool ChunkManager::BaseMetaChunk::isFree() const {
	return getUnoccupiedSize() == UNDEF_UNOCCUPIED_SIZE_;
}

UTIL_FORCEINLINE int64_t ChunkManager::BaseMetaChunk::getCheckpointPos() const {
	int64_t pos = static_cast<int64_t>(data_ & CHECKPOINT_POS_MASK) >>
			CHECKPOINT_POS_SHIFT_BIT;
	return (pos == UNDEF_CHECKPOINT_POS) ? -1 : pos;
}

UTIL_FORCEINLINE uint8_t ChunkManager::BaseMetaChunk::getUnoccupiedSize() const {
	return static_cast<uint8_t>(data_ & UNOCCUPIED_SIZE_MASK);
}

UTIL_FORCEINLINE bool ChunkManager::BaseMetaChunk::isAffinityUse() const {
	return (data_ & IS_AFFINITY_USE_BIT) != 0;
}

UTIL_FORCEINLINE
void ChunkManager::BaseMetaChunk::setCheckpointPos(int64_t checkpointPos) {
	int64_t pos = (checkpointPos == -1) ?
			UNDEF_CHECKPOINT_POS : (checkpointPos << CHECKPOINT_POS_SHIFT_BIT);
	data_ &= ~CHECKPOINT_POS_MASK;
	data_ |= static_cast<uint64_t>(pos) & CHECKPOINT_POS_MASK;
}

UTIL_FORCEINLINE
void ChunkManager::BaseMetaChunk::setUnoccupiedSize(uint8_t unoccupiedSize) {
	data_ &= ~UNOCCUPIED_SIZE_MASK;
	data_ |= static_cast<uint64_t>(unoccupiedSize);
}

UTIL_FORCEINLINE void ChunkManager::BaseMetaChunk::resetUnoccupiedSize() {
	data_ &= ~UNOCCUPIED_SIZE_MASK;
	data_ |= static_cast<uint64_t>(UNDEF_UNOCCUPIED_SIZE_);
}

UTIL_FORCEINLINE void ChunkManager::BaseMetaChunk::setAffinityUsed() {
	data_ |= IS_AFFINITY_USE_BIT;
}

UTIL_FORCEINLINE void ChunkManager::BaseMetaChunk::setAffinityUnused() {
	data_ &= ~IS_AFFINITY_USE_BIT;
}
UTIL_FORCEINLINE
void ChunkManager::BaseMetaChunk::setCumulativeDirtyFlag(bool flag) {
	if (flag) {
		data_ |= CUMULATIVE_DIRTY_FLAG_BIT;
	}
	else {
		data_ &= ~CUMULATIVE_DIRTY_FLAG_BIT;
	}
}

UTIL_FORCEINLINE
bool ChunkManager::BaseMetaChunk::getCumulativeDirtyFlag() const {
	return (data_ & CUMULATIVE_DIRTY_FLAG_BIT) != 0;
}

UTIL_FORCEINLINE
void ChunkManager::BaseMetaChunk::setDifferentialDirtyFlag(bool flag) {
	if (flag) {
		data_ |= DIFFERENTIAL_DIRTY_FLAG_BIT;
	}
	else {
		data_ &= ~DIFFERENTIAL_DIRTY_FLAG_BIT;
	}
}

UTIL_FORCEINLINE
bool ChunkManager::BaseMetaChunk::getDifferentialDirtyFlag() const {
	return (data_ & DIFFERENTIAL_DIRTY_FLAG_BIT) != 0;
}


UTIL_FORCEINLINE bool ChunkManager::BufferInfo::fix(int64_t refId) {
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

UTIL_FORCEINLINE void ChunkManager::BufferInfo::fixFirstTime(int64_t refId) {
	assert(0 == refCount_);
	refCount_ = 1;
	lastRefId_ = refId;
}

UTIL_FORCEINLINE void ChunkManager::BufferInfo::unfix() {
	refCount_--;
	assert(0 <= refCount_);
}

UTIL_FORCEINLINE bool ChunkManager::BufferInfo::isFixed() const {
	assert(0 <= refCount_);
	return (0 < refCount_);
}


inline bool ChunkManager::FileManager::isValidHoleSize(
		uint32_t chunkSize, uint64_t fileSystemBlockSize) {
	static_cast<void>(chunkSize);
	if (fileSystemBlockSize == SUPPORTED_FILE_SYSTEM_BLOCK_SIZE_) {
		return true;
	}
	return false;
}

inline uint32_t ChunkManager::FileManager::getHoleOffset(
		uint32_t compressedSize, uint32_t &holeSize) {
	uint32_t unitHoleSize = (
			static_cast<uint32_t>(1UL << holeBlockExpSize_));
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

inline void ChunkManager::FileManager::updateCheckSum(uint8_t* buffer) {
	ChunkHeader::updateCheckSum(buffer);
}

inline
ChunkManager::CompressionMode ChunkManager::FileManager::isEnableCompression() {
	if (atomicEnableCompression_ == 0) {
		return NO_BLOCK_COMPRESSION;
	}
	else {
		return BLOCK_COMPRESSION;
	}
}


UTIL_FORCEINLINE PartitionId ChunkManager::CheckpointManager::getSyncPId() {
	return syncPId_;
}
UTIL_FORCEINLINE
bool ChunkManager::CheckpointManager::isExecutingCheckpoint(PartitionId pId) {
	assert(pgId_ == config_.getPartitionGroupId(pId));

	if (pgStartCpId_ == pgEndCpId_) {
		return false;
	}
	else {
		assert(pgEndCpId_ == UNDEF_CHECKPOINT_ID ||
				pgEndCpId_ < pgStartCpId_);

		CheckpointBuffer checkpointBuffer;
		if (getPartitionInfo(pId).startCpId_ == pgEndCpId_) {
			return false;
		}
		else {
			assert(pgEndCpId_ == UNDEF_CHECKPOINT_ID ||
					pgEndCpId_ < getPartitionInfo(pId).startCpId_);
			return true;
		}
	}
}
UTIL_FORCEINLINE uint64_t ChunkManager::CheckpointManager::getSyncChunkNum() {
	return checkpointPosList_.size();
}
UTIL_FORCEINLINE
void ChunkManager::CheckpointManager::incrementCheckpointNum(PartitionId pId) {
	getPartitionInfo(pId).checkpointNum_++;
}
UTIL_FORCEINLINE
uint64_t ChunkManager::CheckpointManager::getCheckpointNum(PartitionId pId) {
	assert(pId != syncPId_ ||
			checkpointPosList_.size() ==
			getPartitionInfo(pId).checkpointNum_);
	return getPartitionInfo(pId).checkpointNum_;
}
UTIL_FORCEINLINE
uint64_t ChunkManager::CheckpointManager::getWriteNum(PartitionId pId) {
	return getPartitionInfo(pId).copyNum_;
}
UTIL_FORCEINLINE
uint64_t ChunkManager::CheckpointManager::getPGCheckpointCount() const {
	return pgStats_.checkpointCount_;
}
UTIL_FORCEINLINE
uint64_t ChunkManager::CheckpointManager::getPGCheckpointBufferNum() const {
	return cpWriter_.getCheckpointBufferNum();
}
UTIL_FORCEINLINE
uint64_t ChunkManager::CheckpointManager::getPGCheckpointWriteCount() const {
	return pgStats_.checkpointWriteCount_;
}
UTIL_FORCEINLINE
uint64_t ChunkManager::CheckpointManager::getPGCheckpointWriteTime() const {
	return pgStats_.checkpointWriteTime_;
}
UTIL_FORCEINLINE
uint64_t ChunkManager::CheckpointManager::getPGChunkSyncReadCount() const {
	return pgStats_.chunkSyncReadCount_;
}
UTIL_FORCEINLINE
uint64_t ChunkManager::CheckpointManager::getPGChunkSyncReadTime() const {
	return pgStats_.chunkSyncReadTime_;
}
UTIL_FORCEINLINE
uint64_t ChunkManager::CheckpointManager::getPGRecoveryReadCount() const {
	return pgStats_.recoveryReadCount_;
}
UTIL_FORCEINLINE
uint64_t ChunkManager::CheckpointManager::getPGRecoveryReadTime() const {
	return pgStats_.recoveryReadTime_;
}
UTIL_FORCEINLINE
uint64_t ChunkManager::CheckpointManager::getPGBackupReadCount() const {
	return pgStats_.backupReadCount_;
}
UTIL_FORCEINLINE
uint64_t ChunkManager::CheckpointManager::getPGBackupReadTime() const {
	return pgStats_.backupReadTime_;
}
UTIL_FORCEINLINE
uint64_t ChunkManager::CheckpointManager::getPGBackupWriteCount() const {
	return pgStats_.backupWriteCount_;
}
UTIL_FORCEINLINE
uint64_t ChunkManager::CheckpointManager::getPGBackupWriteTime() const {
	return pgStats_.backupWriteTime_;
}
UTIL_FORCEINLINE uint64_t ChunkManager::CheckpointManager::getPGFileNum() {
	const CheckpointFile& checkpointFile = getCheckpointFile();
	uint64_t fileNum = checkpointFile.getBlockNum();
	return fileNum;
}
UTIL_FORCEINLINE uint64_t ChunkManager::CheckpointManager::getPGFileUseNum() {
	const CheckpointFile& checkpointFile = getCheckpointFile();
	uint64_t fileUseNum = checkpointFile.getUseBitNum();
	return fileUseNum;
}
UTIL_FORCEINLINE uint64_t ChunkManager::CheckpointManager::getPGFileSize() {
	CheckpointFile& checkpointFile = getCheckpointFile();
	uint64_t fileSize = checkpointFile.getFileSize();
	return fileSize;
}
UTIL_FORCEINLINE
uint64_t ChunkManager::CheckpointManager::getPGFileAllocateSize() {
	CheckpointFile& checkpointFile = getCheckpointFile();
	uint64_t fileAllocateSize = checkpointFile.getFileAllocateSize();
	return fileAllocateSize;
}



inline ChunkManager::BufferInfo*
ChunkManager::BufferManager::BufferList::back() {
	return (coldList_.back() ? coldList_.back() : hotList_.back());
}

inline void ChunkManager::BufferManager::BufferList::push(BufferInfo& bufferInfo) {
	bufferInfo.onHotList_ = true;
	hotList_.push_front(bufferInfo);
}

inline void ChunkManager::BufferManager::BufferList::pushCold(BufferInfo& bufferInfo) {
	bufferInfo.onHotList_ = false;
	coldList_.push_front(bufferInfo);
}

inline void ChunkManager::BufferManager::BufferList::remove(BufferInfo& bufferInfo) {
	if (bufferInfo.onHotList_) {
		hotList_.remove(bufferInfo);
	}
	else {
		coldList_.remove(bufferInfo);
	}
}

inline void ChunkManager::BufferManager::BufferList::rebalance(
		uint64_t totalElementCount) {
	uint64_t coldCount = coldList_.getElementCount();
	uint64_t goal = static_cast<uint64_t>(
			static_cast<double>(totalElementCount) * coldRate_ + 0.5);
	if (coldCount == goal) {
		return;
	}
	if (goal > coldCount) {
		shiftHotToCold(goal - coldCount);
	}
	else {
		shiftColdToHot(coldCount - goal);
	}
}

inline void ChunkManager::BufferManager::BufferList::shiftHotToCold(size_t n) {

	LinkedList tmpList;
	int64_t splitPos = 0 - static_cast<int64_t>(n);
	hotList_.split(splitPos, tmpList);

#ifndef NDEBUG
	BufferInfo *tmpCursor = tmpList.beginCursor();
	while (tmpCursor) {
		assert(!tmpCursor->onHotList_);
		tmpCursor = tmpCursor->lruNext_;
	}
#endif
	if (tmpList.getElementCount() > 0) {
		coldList_.concat(tmpList, false); 
	}
}

inline void ChunkManager::BufferManager::BufferList::shiftColdToHot(size_t n) {

	LinkedList tmpList;
	int64_t splitPos = static_cast<int64_t>(n);
	coldList_.split(splitPos, tmpList);

#ifndef NDEBUG
	BufferInfo *tmpCursor = tmpList.beginCursor();
	while (tmpCursor) {
		assert(tmpCursor->onHotList_);
		tmpCursor = tmpCursor->lruNext_;
	}
#endif
	if (tmpList.getElementCount() > 0) {
		hotList_.concat(tmpList, true); 
	}
}

inline ChunkManager::BufferInfo*
ChunkManager::BufferManager::BufferList::beginCursor() {
	cursorOnCold_ = false;
	cursor_ = hotList_.beginCursor();
	if (!cursor_) {
		cursorOnCold_ = true;
		cursor_ = coldList_.beginCursor();
	}
	return cursor_;
}

inline ChunkManager::BufferInfo*
ChunkManager::BufferManager::BufferList::nextCursor() {
	if (cursor_) {
		cursor_ = cursor_->lruNext_;
		if (!cursor_ && !cursorOnCold_) {
			cursorOnCold_ = true;
			cursor_ = coldList_.beginCursor();
		}
	}
	return cursor_;
}

inline bool ChunkManager::BufferManager::BufferList::isValid() {
	return (hotList_.isValid() && coldList_.isValid());
}

inline void ChunkManager::BufferManager::BufferList::setColdRate(double rate) {
	assert(0.0 <= rate && rate <= 1.0);
	coldRate_ = rate;
}

inline double ChunkManager::BufferManager::BufferList::getColdRate() const {
	return coldRate_;
}

inline uint64_t ChunkManager::BufferManager::BufferList::getHotElementCount() const {
	return hotList_.getElementCount();
}

inline uint64_t ChunkManager::BufferManager::BufferList::getColdElementCount() const {
	return coldList_.getElementCount();
}

inline uint64_t ChunkManager::BufferManager::BufferList::getElementCount() const {
	return hotList_.getElementCount() + coldList_.getElementCount();
}


inline ChunkManager::BufferManager::LinkedList::LinkedList()
: elementCount_(0), head_(NULL), tail_(NULL), cursor_(NULL)
{}

inline ChunkManager::BufferManager::LinkedList::~LinkedList()
{}

inline void ChunkManager::BufferManager::LinkedList::reset(
		BufferInfo* head, BufferInfo* tail, uint64_t elementCount) {
	elementCount_ = elementCount;
	head_ = head;
	tail_ = tail;
	cursor_ = NULL;

	assert(isValid());
}

inline ChunkManager::BufferInfo* ChunkManager::BufferManager::LinkedList::back() {
	assert(!tail_ || tail_->lruNext_ == NULL);
	assert(!tail_ || tail_->lruPrev_ != NULL || head_ == tail_);
	return tail_;
}

inline void ChunkManager::BufferManager::LinkedList::pop_back() {
	assert(tail_->buffer_);  
	BufferInfo* popChunk = tail_;
	BufferInfo* prevChunk = popChunk->lruPrev_;

	if (head_ == popChunk) {
		head_ = NULL;
	}
	if (prevChunk) {
		assert(tail_ != NULL);
		prevChunk->lruNext_ = NULL;
	}
	tail_ = prevChunk;
	popChunk->lruNext_ = NULL;
	popChunk->lruPrev_ = NULL;
	cursor_ = NULL;
	assert(0 < elementCount_);
	elementCount_--;
	assert(isValid());
}

inline void ChunkManager::BufferManager::LinkedList::push_front(BufferInfo& bufferInfo) {
	assert(bufferInfo.buffer_);  
	assert(bufferInfo.lruPrev_ == NULL);
	assert(bufferInfo.lruNext_ == NULL);
	assert(bufferInfo.refCount_ == 0);

	assert((head_ != NULL) ||
			(head_ == NULL && tail_ == NULL && elementCount_ == 0));

	if (!tail_) {
		assert(elementCount_ == 0);
		tail_ = &bufferInfo;
	}
	bufferInfo.lruPrev_ = NULL;
	bufferInfo.lruNext_ = head_;
	if (head_) {
		head_->lruPrev_ = &bufferInfo;
	}
	head_ = &bufferInfo;
	elementCount_++;
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

inline void ChunkManager::BufferManager::LinkedList::remove(BufferInfo& bufferInfo) {
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
	assert(0 < elementCount_);
	elementCount_--;
	cursor_ = NULL;
	assert((head_ == NULL && tail_ == NULL && elementCount_ == 0) ||
			(head_ != NULL && tail_ != NULL && head_ == tail_ &&
					head_->lruNext_ == NULL && head_->lruPrev_ == NULL &&
					elementCount_ == 1) ||
			(head_ != NULL && tail_ != NULL && head_ != tail_ &&
					head_->lruNext_ == tail_ &&
					tail_->lruPrev_ == head_ && elementCount_ == 2) ||
			(head_ != NULL && tail_ != NULL && head_ != tail_ &&
					head_->lruNext_ != tail_ &&
					tail_->lruPrev_ != head_ && 3 <= elementCount_));
	assert(isValid());
}

inline void ChunkManager::BufferManager::LinkedList::concat(
		LinkedList &list, bool toTail) {
	assert(list.isValid());
	assert(list.head_ && list.tail_);
	assert(isValid());
	if (!head_) {
		assert(elementCount_ == 0);
		head_ = list.head_;
		tail_ = list.tail_;
	}
	else {
		if (toTail) {
			assert(tail_);
			tail_->lruNext_ = list.head_;
			list.head_->lruPrev_ = tail_;
			tail_ = list.tail_;
		}
		else {
			assert(head_);
			list.tail_->lruNext_ = head_;
			head_->lruPrev_ = list.tail_;
			head_ = list.head_;
		}
	}
	elementCount_ += list.elementCount_;

	assert((head_ == NULL && tail_ == NULL && elementCount_ == 0) ||
			(head_ != NULL && tail_ != NULL && head_ == tail_ &&
					head_->lruNext_ == NULL && head_->lruPrev_ == NULL &&
					elementCount_ == 1) ||
			(head_ != NULL && tail_ != NULL && head_ != tail_ &&
					head_->lruNext_ == tail_ &&
					tail_->lruPrev_ == head_ && elementCount_ == 2) ||
			(head_ != NULL && tail_ != NULL && head_ != tail_ &&
					head_->lruNext_ != tail_ &&
					tail_->lruPrev_ != head_ && 3 <= elementCount_));
	assert(isValid());
}

inline uint64_t ChunkManager::BufferManager::LinkedList::getElementCount() const {
	return elementCount_;
}

inline
ChunkManager::BufferInfo* ChunkManager::BufferManager::LinkedList::beginCursor() {
	cursor_ = head_;
	return nextCursor();
}

inline
ChunkManager::BufferInfo* ChunkManager::BufferManager::LinkedList::nextCursor() {
	if (cursor_) {
		BufferInfo* bufferInfo = cursor_;
		cursor_ = cursor_->lruNext_;
		return bufferInfo;
	}
	return NULL;
}


inline void ChunkManager::BufferManager::ChainingHashTable::append(
		BufferId bufferId, BufferInfo* bufferInfo) {
	assert(bufferInfo);
	HashValue hashValue = calcKey(bufferId);
	bufferInfo->nextCollision_ = table_[hashValue];
	table_[hashValue] = bufferInfo;
};

inline ChunkManager::BufferInfo*
ChunkManager::BufferManager::ChainingHashTable::get(BufferId bufferId) {
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

inline ChunkManager::BufferInfo*
ChunkManager::BufferManager::ChainingHashTable::remove(BufferId bufferId) {
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


UTIL_FORCEINLINE ChunkManager::BufferManager::ChainingHashTable::HashValue
ChunkManager::BufferManager::ChainingHashTable::fnvHash(
		uint8_t* bytes, size_t length) {
	uint64_t hash = FNV_OFFSET_BASIS_64;
	for (size_t i = 0; i < length; ++i) {
		hash = (FNV_PRIME_64 * hash) ^ (bytes[i]);
	}

	return hash;
}

UTIL_FORCEINLINE ChunkManager::BufferManager::ChainingHashTable::HashValue
ChunkManager::BufferManager::ChainingHashTable::calcKey(
		BufferId bufferId) {
	HashValue hashValue = fnvHash(
		reinterpret_cast<uint8_t*>(&bufferId), sizeof(BufferId));
	hashValue &= tableMask_;
	assert(hashValue < maxSize_);
	return hashValue;
}


inline bool ChunkManager::BufferManager::ChainingHashTableCursor::next(
		BufferInfo*& bufferInfo) {
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

inline void ChunkManager::BufferManager::ChainingHashTableCursor::remove(
		BufferInfo*& bufferInfo) {
	if (last_) {
		bufferInfo = hashTable_.remove(getBufferId(last_));
		last_ = NULL;
		index_--;
	}
	else {
		bufferInfo = NULL;
	}
}

#endif  
